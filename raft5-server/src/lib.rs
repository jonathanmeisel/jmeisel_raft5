//! A single Raft node, runnable as a library (`run`) or as the `main`
//! binary (see `main.rs`, which just parses flags into `run`'s
//! arguments). Splitting it this way is what lets an integration test
//! spin up a real node in-process instead of shelling out to the binary.
//!
//! # Shutting a node down
//!
//! `run` hands back a future and a `CancellationToken` rather than just a
//! future, because a Raft node is not one task: it is two gRPC servers,
//! a state machine, and a shifting set of per-role and per-peer
//! background tasks underneath it. Cancelling the token is the one
//! shutdown signal for all of them, and the future resolving is the
//! confirmation that every last one has finished -- no detached tasks
//! outliving the node, no `JoinHandle::abort` leaving a half-finished
//! storage call still holding the database lock. `raft::Task` is where
//! that discipline is actually enforced.

pub mod auth;
pub mod grpc;
pub mod keyspace;
pub mod pb;
pub mod raft;
pub mod storage;
pub mod store;
pub mod txn;

use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tower_http::trace::{DefaultMakeSpan, DefaultOnFailure, DefaultOnResponse, TraceLayer};
use tower_http::validate_request::ValidateRequestHeaderLayer;
use tracing::{info, Level};

use auth::{AuthConfig, RequireToken};
use grpc::{KvService, RaftPeerService};
use pb::raft5::client::kv_server::KvServer;
use pb::raft5::cluster::ClusterMember;
use pb::raft5::raft::raft_peer_server::RaftPeerServer;
use raft::{Common, NodeHandles};
use storage::RaftLog;
use store::Store;

/// What a node's failure is reported as. `Send + Sync` (unlike a bare
/// `Box<dyn Error>`) so that `run`'s future can be handed straight to
/// `tokio::spawn`, which is how both `main` and the integration tests
/// drive a node.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Everything needed to run one Raft node: the future that *is* the node,
/// and the token that shuts it down.
///
/// Nothing starts until the future is polled, and the node keeps running
/// until either a server fails or the token is cancelled. Cancelling is
/// the graceful path, not an abrupt one: the token is the root of a tree
/// of child tokens covering every task the node spawns -- the gRPC
/// servers (which stop accepting and then drain their in-flight
/// requests), the state machine, and each of its per-role and per-peer
/// background tasks. The future resolves only once every one of them has
/// actually returned, which is what makes it safe to immediately restart
/// a node on the same database directory and the same addresses: by then
/// nothing is still holding the database lock or the sockets.
///
/// Dropping the future instead of cancelling the token is still abrupt,
/// and still leaves in-flight `spawn_blocking` storage work running
/// headless -- prefer `cancel()` then `.await`.
///
/// A node started against a fresh `db` directory belongs to no cluster:
/// it answers RPCs and waits, without ever campaigning, until it is
/// either bootstrapped into a single-node cluster (the `Bootstrap` client
/// RPC) or added to an existing one (`AddServer` on that cluster's
/// leader, which replicates the configuration here). Membership lives in
/// the log, so a restart against an existing `db` rejoins whatever
/// cluster this node was already part of -- there is no peer list to
/// keep in sync by hand.
///
/// - `id`: this node's stable identity within the cluster. Must not
///   change across restarts: it is what every other node's log calls
///   this one.
/// - `db`: directory holding this node's database (log, state, and
///   membership). Created if it doesn't exist.
/// - `client_addr`: address to serve the client API on.
/// - `peer_addr`: address to serve the peer-to-peer Raft API on.
/// - `advertise`: URL other nodes should dial to reach this node's peer
///   API. Defaults to `peer_addr`, which is wrong whenever that is a
///   wildcard bind (`0.0.0.0:...`) or behind NAT -- set it explicitly
///   then. Recorded in the cluster configuration at bootstrap.
/// - `advertise_client`: the same, for the client API -- what clients
///   are handed when they ask the cluster where its members are.
// pub fn run(
//     id: String,
//     db: &Path,
//     client_addr: SocketAddr,
//     peer_addr: SocketAddr,
//     advertise: Option<String>,
// ) -> (
//     impl Future<Output = Result<(), Error>> + Send + 'static,
//     CancellationToken,
// ) {
//     let cancel = CancellationToken::new();
//     // Owned, so the returned future borrows nothing and can be spawned.
//     let db = db.to_path_buf();
//     let token = cancel.clone();
//     let node = async move { serve(id, &db, client_addr, peer_addr, advertise, token).await };
//     (node, cancel)
// }

/// How a node is configured. See [`serve_config`].
///
/// A struct rather than more parameters because the list was already at
/// the length where a caller has to count commas, and because `auth`
/// will not be the last thing added to it.
pub struct ServerConfig {
    /// This node's stable identity within the cluster. Must not change
    /// across restarts: it is what every other node's log calls this
    /// one.
    pub id: String,
    /// Directory holding this node's database (log, state, and
    /// membership). Created if it doesn't exist.
    pub db: PathBuf,
    /// Address to serve the client API on.
    pub client_addr: SocketAddr,
    /// Address to serve the peer-to-peer Raft API on.
    pub peer_addr: SocketAddr,
    /// URL other nodes should dial to reach this node's peer API.
    /// Defaults to `peer_addr`, which is wrong whenever that is a
    /// wildcard bind (`0.0.0.0:...`) or behind NAT -- set it explicitly
    /// then. Recorded in the cluster configuration at bootstrap.
    pub advertise: Option<String>,
    /// The same, for the client API -- what clients are handed when they
    /// ask the cluster where its members are.
    pub advertise_client: Option<String>,
    /// How tokens are signed and checked.
    ///
    /// `None` leaves the client API open: no request needs a token and
    /// the account RPCs all answer `AUTH_DISABLED`, which is how this
    /// cluster behaved before authentication existed. See
    /// [`crate::auth`].
    pub auth: Option<AuthConfig>,
}

/// `run`, for a caller that already has a token to shut this node down
/// with -- one shared with other nodes in the same process, say, or a
/// child of some wider application-level token. `run` is this plus a
/// freshly minted root token.
///
/// This is [`serve_config`] with authentication off; a node that should
/// require tokens is started through that instead.
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
        },
        cancel,
    )
    .await
}

/// Runs one node to completion, as `config` describes it.
pub async fn serve_config(config: ServerConfig, cancel: CancellationToken) -> Result<(), Error> {
    let ServerConfig {
        id,
        db,
        client_addr,
        peer_addr,
        advertise,
        advertise_client,
        auth,
    } = config;
    let log = Arc::new(RaftLog::open(&db)?);
    let identity = ClusterMember {
        server_id: id.clone(),
        address: advertise.unwrap_or_else(|| format!("http://{peer_addr}")),
        client_address: advertise_client.unwrap_or_else(|| format!("http://{client_addr}")),
        voting: true,
    };

    // Checked before the node starts, not from `role_rx`: the state
    // machine hasn't published a role yet at this point.
    let uninitialized = log.current_config()?.is_none();

    let NodeHandles {
        common,
        peer_handle,
        ..
    } = Common::new(id.clone(), Arc::clone(&log), cancel.clone())?;
    let client_handle = common.client_handle();

    // KvService's read/write path still bypasses the state machine (see
    // its module doc); `client_handle` is what `Bootstrap` goes through.
    let kv_service = KvService::new(
        Store::new(Arc::clone(&log)),
        identity,
        client_handle,
        auth.clone(),
    );
    spawn_transaction_sweeper(kv_service.locks(), cancel.clone());
    // A whole transaction may be a single value, so one `BufferWrite`
    // can legitimately be as large as `MAX_TRANSACTION_BYTES`. Without
    // this, tonic's 4MB default would refuse it -- and refuse it in the
    // transport, where the transaction size limit has no say and no way
    // to explain itself.
    let kv_server = KvServer::new(kv_service)
        .max_decoding_message_size(raft5_shared::MAX_KV_MESSAGE_BYTES);
    let raft_peer_server = RaftPeerServer::new(RaftPeerService::new(peer_handle));

    if uninitialized {
        info!(
            "node {id} is not part of a cluster yet -- send Bootstrap to form a single-node \
             cluster, or AddServer on an existing cluster's leader to join it"
        );
    }
    info!("client API listening on {client_addr}");
    info!("peer API listening on {peer_addr}");

    // `serve_with_shutdown` rather than `serve`: on cancellation tonic
    // stops accepting and then lets the requests already in flight finish,
    // instead of dropping them mid-response. They can finish -- even one
    // parked in `LeaderHandle::append_writes` waiting on a commit that will
    // now never come -- because `raft::run` below drops `Common` as it
    // exits, which closes the channels those handlers are waiting on.
    //
    // Each server also cancels the token on its own way out, so a bind
    // failure on one port brings the whole node down rather than leaving
    // a half-reachable one behind.
    let client_server = async {
        let result = Server::builder()
            .layer(trace_layer())
            // Inside the trace layer, so a request turned away for want
            // of a token is still one of the spans this node logs --
            // which is the only place an operator would see that
            // happening at all.
            .layer(ValidateRequestHeaderLayer::custom(RequireToken::new(auth)))
            .add_service(kv_server)
            .serve_with_shutdown(client_addr, cancel.cancelled())
            .await;
        cancel.cancel();
        result
    };
    let peer_server = async {
        let result = Server::builder()
            .layer(trace_layer())
            // Matches the peer clients (see `raft::PeerConnections`):
            // without it, h2's default 65535-byte window caps
            // replication throughput at `window / round trip` however
            // the batch is chunked.
            .http2_adaptive_window(Some(true))
            // An `AppendEntries` stream buffers up to
            // `MAX_APPEND_ENTRIES_BYTES` here before any of it reaches
            // the event loop, so how many may be in flight at once is
            // the other half of that limit -- and this port is where an
            // unauthenticated peer would push on it.
            .max_concurrent_streams(Some(MAX_CONCURRENT_PEER_STREAMS))
            .add_service(raft_peer_server)
            .serve_with_shutdown(peer_addr, cancel.cancelled())
            .await;
        cancel.cancel();
        result
    };

    // `join!`, not `select!`: all three have to be driven to completion,
    // not just until the first one finishes. `raft::run` returns only
    // after every task it owns has been signalled *and joined* (see
    // `raft::Task`), and dropping it early instead would abandon exactly
    // the work -- in-flight `spawn_blocking` storage calls holding the
    // database lock -- that a restart right afterwards would collide with.
    let (client_result, peer_result, ()) =
        tokio::join!(client_server, peer_server, raft::run(common));
    client_result?;
    peer_result?;
    Ok(())
}

/// One span per RPC, on both the client and peer services, recording its
/// method, latency, and outcome. Success/failure is read from the
/// `grpc-status` trailer rather than the HTTP status (which for gRPC is
/// almost always 200 regardless of the RPC's outcome), so an application
/// error here is a real error, not a false "ok".
///
/// Logged at `INFO` -- and failures at `WARN`, so they stand out from
/// ordinary traffic -- to match the level the rest of this crate's
/// logging defaults to (see `main`'s `EnvFilter`).
fn trace_layer()
-> TraceLayer<tower_http::classify::SharedClassifier<tower_http::classify::GrpcErrorsAsFailures>>
{
    TraceLayer::new_for_grpc()
        .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
        .on_response(DefaultOnResponse::new().level(Level::INFO))
        .on_failure(DefaultOnFailure::new().level(Level::WARN))
}

/// How many peer RPCs may be in flight against this node at once.
///
/// Each concurrent `AppendEntries` can hold `MAX_APPEND_ENTRIES_BYTES`
/// of reassembly buffer, so this and that constant together are the real
/// ceiling on what inbound replication can make this node allocate.
/// Comfortably above anything a legitimate cluster does -- a peer runs
/// one replication stream and one lease probe against each other node --
/// and far below "unbounded".
const MAX_CONCURRENT_PEER_STREAMS: u32 = 64;

/// How often abandoned transactions are reclaimed, and how long an
/// aborted one's record of *why* it died outlives it.
///
/// The interval is not a latency budget. An expired transaction's locks
/// are released by the first request that collides with them, so nothing
/// waits on this tick to make progress -- it only reclaims transactions
/// nobody happens to want anything from, which costs memory rather than
/// time. The grace period is what lets a client that has been quiet come
/// back and be told `EXPIRED` instead of `UNKNOWN`.
const TRANSACTION_SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);
const TRANSACTION_TOMBSTONE_GRACE: std::time::Duration = std::time::Duration::from_secs(30);

/// Reclaims transactions nobody collided with, until the node shuts down.
///
/// Holds the lock table outright rather than weakly: it is a map behind
/// an `Arc`, not the service, so keeping it alive costs a few words and
/// creates no cycle. The cancellation token is what ends this task, the
/// same one that ends everything else here.
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
