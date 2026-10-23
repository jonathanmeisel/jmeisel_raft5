//! A single Raft node, runnable as a library ([`serve_config`]) or as the
//! `main` binary (see `main.rs`, which only parses flags into a
//! [`ServerConfig`]). The split lets an integration test start a real
//! node in-process instead of running the binary.
//!
//! # Shutting a node down
//!
//! A node takes a `CancellationToken`, because a Raft node is not one
//! task: it is two gRPC servers, a state machine, and a changing set of
//! per-role and per-peer background tasks. Cancelling the token is the one
//! shutdown signal for all of them, and the node's future resolving
//! confirms that every one has finished: no detached task outlives the
//! node, and no `JoinHandle::abort` leaves a half-done storage call holding
//! the database lock. `raft::Task` enforces this.

pub mod auth;
pub mod grpc;
pub mod keyspace;
pub mod pb;
pub mod perms;
pub mod raft;
pub mod storage;
pub mod store;
pub mod txn;

use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tonic::transport::{Certificate, ClientTlsConfig, Identity, Server, ServerTlsConfig};
use tower_http::trace::{DefaultMakeSpan, DefaultOnFailure, DefaultOnResponse, TraceLayer};
use tower_http::validate_request::ValidateRequestHeaderLayer;
use tracing::{Level, info};

use auth::{AuthConfig, RequireToken};
use grpc::{KvService, RaftPeerService};
use pb::raft5::client::kv_server::KvServer;
use pb::raft5::cluster::ClusterMember;
use pb::raft5::raft::raft_peer_server::RaftPeerServer;
use raft::{Common, NodeHandles};
use storage::RaftLog;
use store::Store;

/// How a node's failure is reported. `Send + Sync` (unlike a plain
/// `Box<dyn Error>`), so the node's future can go straight to
/// `tokio::spawn`, which is how `main` and the integration tests run a
/// node.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// How a node is configured. See [`serve_config`].
///
/// A struct, not more parameters, so callers do not have to count commas,
/// and new settings do not change every call site.
pub struct ServerConfig {
    /// This node's stable identity in the cluster. Must not change across
    /// restarts: every other node's log uses it for this node.
    pub id: String,
    /// The directory with this node's database (log, state and membership).
    /// Created if it does not exist.
    pub db: PathBuf,
    /// The address to serve the client API on.
    pub client_addr: SocketAddr,
    /// The address to serve the peer Raft API on.
    pub peer_addr: SocketAddr,
    /// The URL other nodes dial to reach this node's peer API. Defaults to
    /// `peer_addr`, which is wrong for a wildcard bind (`0.0.0.0:...`) or
    /// behind NAT: set it explicitly then. Recorded in the cluster
    /// configuration at bootstrap.
    pub advertise: Option<String>,
    /// The same, for the client API: what clients get when they ask the
    /// cluster where its members are.
    pub advertise_client: Option<String>,
    /// How tokens are signed and checked.
    ///
    /// `None` leaves the client API open: no request needs a token, and the
    /// account RPCs all answer `AUTH_DISABLED`. See [`crate::auth`].
    pub auth: Option<AuthConfig>,
    /// mTLS for the peer API, both as served here and as this node dials
    /// other members. `None` is plaintext both ways. The whole cluster must
    /// agree, because a plaintext node and a TLS node cannot talk at all.
    pub peer_tls: Option<PeerTls>,
    /// TLS for the client API, or `None` to serve it in plaintext.
    pub client_tls: Option<ClientApiTls>,
}

/// How a node proves itself to its peers, and how it decides to trust
/// theirs.
///
/// One identity does both: the certificate a node serves the peer API with
/// is also the one it presents as a client when it dials another node. So
/// if it lists extended key usages, it needs both `serverAuth` and
/// `clientAuth`.
///
/// Any certificate that `ca` vouches for is a peer. Nothing ties a
/// certificate to a particular `server_id`: with one certificate shared by
/// the whole cluster, there is no per-node identity to tie it to.
#[derive(Clone)]
pub struct PeerTls {
    pub identity: Identity,
    /// What a peer's certificate must chain to. May be a self-signed leaf used
    /// as its own anchor; then it must not be marked `CA:TRUE`, because webpki
    /// refuses a CA certificate used as an end entity.
    pub ca: Certificate,
    /// The name to check every peer's certificate against, instead of the host
    /// in its address. This lets one certificate, with one name, serve every
    /// node however it is addressed.
    pub server_name: Option<String>,
}

/// How the client API is served over TLS.
#[derive(Clone)]
pub struct ClientApiTls {
    pub identity: Identity,
    /// If set, every client must present a certificate that chains to this
    /// (mTLS), or the handshake fails before any RPC is seen. That is all it
    /// does: the token, not the certificate, says who the caller is (see
    /// [`crate::auth`]).
    pub client_ca: Option<Certificate>,
}

/// Run a node with authentication and TLS off, until `cancel` is
/// cancelled. [`serve_config`] with a default config; a node that needs
/// tokens or TLS starts through that instead.
pub async fn serve(
    id: String,
    db: &Path,
    client_addr: SocketAddr,
    peer_addr: SocketAddr,
    advertise: Option<String>,
    advertise_client: Option<String>,
    cancel: CancellationToken,
) -> Result<(), Error> {
    serve_config(
        ServerConfig {
            id,
            db: db.to_path_buf(),
            client_addr,
            peer_addr,
            advertise,
            advertise_client,
            auth: None,
            peer_tls: None,
            client_tls: None,
        },
        cancel,
    )
    .await
}

/// Run one node, as `config` describes it, until a server fails or
/// `cancel` is cancelled.
///
/// Cancelling is the graceful path. `cancel` is the root of a tree of child
/// tokens that covers every task the node starts: the gRPC servers (which
/// stop accepting, then finish their in-flight requests), the state
/// machine, and each per-role and per-peer background task. This returns
/// only after all of them have returned, so it is safe to restart a node on
/// the same database directory and addresses at once: nothing still holds
/// the database lock or the sockets.
///
/// Dropping the future instead of cancelling is abrupt, and leaves
/// in-flight `spawn_blocking` storage work running with no owner. Prefer
/// `cancel()`, then `.await`.
///
/// A node started on a new `db` directory is in no cluster. It answers RPCs
/// and waits, never campaigning, until it is bootstrapped into a
/// single-node cluster (the `Bootstrap` client RPC) or added to an existing
/// one (`AddServer` on that cluster's leader, which replicates the
/// configuration here). Membership lives in the log, so a restart on an
/// existing `db` rejoins the cluster this node was in: there is no peer
/// list to keep in sync by hand.
pub async fn serve_config(config: ServerConfig, cancel: CancellationToken) -> Result<(), Error> {
    let ServerConfig {
        id,
        db,
        client_addr,
        peer_addr,
        advertise,
        advertise_client,
        auth,
        peer_tls,
        client_tls,
    } = config;
    let log = Arc::new(RaftLog::open(&db)?);
    let identity = ClusterMember {
        server_id: id.clone(),
        address: advertise.unwrap_or_else(|| {
            raft5_shared::tls::with_scheme(&peer_addr.to_string(), peer_tls.is_some())
        }),
        client_address: advertise_client.unwrap_or_else(|| {
            raft5_shared::tls::with_scheme(&client_addr.to_string(), client_tls.is_some())
        }),
        voting: true,
    };

    // Checked before the node starts, not from `role_rx`: the state machine
    // has not published a role yet.
    let uninitialized = log.current_config()?.is_none();

    let NodeHandles {
        mut common,
        peer_handle,
        ..
    } = Common::new(id.clone(), Arc::clone(&log), cancel.clone())?;
    // Built before either server starts, so a certificate rustls does not
    // accept fails the node at startup, not at its first dial.
    let mut client_builder = Server::builder();
    let mut peer_builder = Server::builder();
    let mut peer_dial = None;
    if let Some(tls) = client_tls {
        let mut config = ServerTlsConfig::new().identity(tls.identity);
        if let Some(ca) = tls.client_ca {
            // Required, not optional (tonic's default). An optional client certificate
            // would gate nothing, because nothing here reads who it names.
            config = config.client_ca_root(ca);
        }
        client_builder = client_builder.tls_config(config)?;
    }
    if let Some(tls) = peer_tls {
        peer_builder = peer_builder.tls_config(
            ServerTlsConfig::new()
                .identity(tls.identity.clone())
                .client_ca_root(tls.ca.clone()),
        )?;
        let mut dial = ClientTlsConfig::new()
            .identity(tls.identity)
            .ca_certificate(tls.ca);
        if let Some(name) = tls.server_name {
            dial = dial.domain_name(name);
        }
        common.set_peer_tls(dial.clone());
        peer_dial = Some(dial);
    }
    let client_handle = common.client_handle();

    // KvService appends client writes through `LeaderHandle`, not through the
    // event loop (see its module doc). `client_handle` is what `Bootstrap`
    // goes through.
    let mut kv_service = KvService::new(
        Store::new(Arc::clone(&log)),
        identity.clone(),
        client_handle,
        auth.clone(),
    );
    if let Some(dial) = peer_dial {
        kv_service = kv_service.with_peer_tls(dial);
    }
    spawn_transaction_sweeper(kv_service.locks(), cancel.clone());
    // A whole transaction may be one value, so one `Write` can be as large as
    // `MAX_TRANSACTION_BYTES`. Without this, tonic's 4 MB default would refuse
    // it in the transport, where the transaction size limit has no say and no
    // way to explain itself.
    let kv_server =
        KvServer::new(kv_service).max_decoding_message_size(raft5_shared::MAX_KV_MESSAGE_BYTES);
    let raft_peer_server = RaftPeerServer::new(
        RaftPeerService::new(peer_handle).identify_as(identity, Store::new(Arc::clone(&log))),
    );

    if uninitialized {
        info!(
            "node {id} is not part of a cluster yet -- send Bootstrap to form a single-node \
             cluster, or AddServer on an existing cluster's leader to join it"
        );
    }
    info!("client API listening on {client_addr}");
    info!("peer API listening on {peer_addr}");

    // `serve_with_shutdown`, not `serve`: on cancellation, tonic stops
    // accepting and lets in-flight requests finish instead of dropping them
    // mid-response. They can finish, even one waiting in
    // `LeaderHandle::append_writes` for a commit that will never come, because
    // `raft::run` below drops `Common` as it exits, which closes the channels
    // those handlers wait on.
    //
    // Each server also cancels the token on its way out, so a bind failure on
    // one port stops the whole node instead of leaving it half reachable.
    let client_server = async {
        let result = client_builder
            .layer(trace_layer())
            // Inside the trace layer, so a request refused for lack of a token is
            // still one of the spans this node logs, which is the only place an
            // operator would see it.
            .layer(ValidateRequestHeaderLayer::custom(RequireToken::new(auth)))
            .add_service(kv_server)
            .serve_with_shutdown(client_addr, cancel.cancelled())
            .await;
        cancel.cancel();
        result
    };
    let peer_server = async {
        let result = peer_builder
            .layer(trace_layer())
            // Matches the peer clients (see `raft::PeerConnections`): without it,
            // h2's default 65535-byte window caps replication throughput at
            // `window / round trip`, however the batch is chunked.
            .http2_adaptive_window(Some(true))
            // An `AppendEntries` stream buffers up to `MAX_APPEND_ENTRIES_BYTES` here
            // before any of it reaches the event loop, so the number in flight at once
            // is the other half of that limit, and this port is where an
            // unauthenticated peer would push on it.
            .max_concurrent_streams(Some(MAX_CONCURRENT_PEER_STREAMS))
            .add_service(raft_peer_server)
            .serve_with_shutdown(peer_addr, cancel.cancelled())
            .await;
        cancel.cancel();
        result
    };

    // `join!`, not `select!`: all three must run to completion, not only until
    // the first finishes. `raft::run` returns only after every task it owns is
    // signalled and joined (see `raft::Task`), and dropping it early would
    // abandon exactly the work (in-flight `spawn_blocking` storage calls that
    // hold the database lock) that an immediate restart would collide with.
    let (client_result, peer_result, ()) =
        tokio::join!(client_server, peer_server, raft::run(common));
    client_result?;
    peer_result?;
    Ok(())
}

/// One span per RPC, on both the client and peer services, with its method,
/// latency and outcome. Success or failure comes from the `grpc-status`
/// trailer, not the HTTP status (almost always 200 for gRPC, whatever the
/// outcome), so an application error is logged as an error, not as "ok".
///
/// Logged at `INFO`, and failures at `WARN` so they stand out, to match the
/// level the rest of this crate logs at by default (see `main`'s
/// `EnvFilter`).
fn trace_layer()
-> TraceLayer<tower_http::classify::SharedClassifier<tower_http::classify::GrpcErrorsAsFailures>> {
    TraceLayer::new_for_grpc()
        .make_span_with(DefaultMakeSpan::new().level(Level::TRACE))
        .on_response(DefaultOnResponse::new().level(Level::TRACE))
        .on_failure(DefaultOnFailure::new().level(Level::WARN))
}

/// The most peer RPCs in flight against this node at once.
///
/// Each concurrent `AppendEntries` can hold `MAX_APPEND_ENTRIES_BYTES` of
/// reassembly buffer, so this and that constant together are the real limit
/// on what inbound replication can make this node allocate. Well above
/// what a real cluster does (a peer runs one replication stream and one
/// lease probe against each other node), and far below unbounded.
const MAX_CONCURRENT_PEER_STREAMS: u32 = 64;

/// How often abandoned transactions are reclaimed, and how long an aborted
/// transaction's reason outlives it.
///
/// The interval is not a latency budget. The first request that collides
/// with an expired transaction's locks releases them, so nothing waits on
/// this tick to make progress. It only reclaims transactions nobody
/// collides with, which costs memory, not time. The grace period lets a
/// client that was quiet come back and hear `EXPIRED` instead of `UNKNOWN`.
const TRANSACTION_SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);
const TRANSACTION_TOMBSTONE_GRACE: std::time::Duration = std::time::Duration::from_secs(30);

/// Reclaim transactions nobody collided with, until the node shuts down.
///
/// Holds the lock table by a strong reference, not a weak one: it is a map
/// behind an `Arc`, not the service, so keeping it alive costs a few words
/// and makes no cycle. The cancellation token ends this task, the same
/// token that ends everything else here.
fn spawn_transaction_sweeper(locks: txn::Locks, cancel: CancellationToken) {
    tokio::spawn(async move {
        let mut ticks = tokio::time::interval(TRANSACTION_SWEEP_INTERVAL);
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                _ = ticks.tick() => {}
            }
            locks.sweep(std::time::Instant::now(), TRANSACTION_TOMBSTONE_GRACE);
        }
    });
}
