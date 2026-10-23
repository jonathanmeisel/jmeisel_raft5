//! Shared setup for integration tests: start real `raft5_server` nodes
//! (`serve_config`) on real localhost sockets and drive them only through
//! the client gRPC API, retrying the temporary refusals that are normal
//! during startup, elections and replication (instead of fixed sleeps that
//! guess how long those take).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

use raft5_server::pb::raft5::client::kv_client::KvClient;
use raft5_server::pb::raft5::client::{
    AddServerRequest, BootstrapRequest, GetConfigRequest, ReadRequest, WriteRequest,
    config_change_response, read_response, write_response,
};
use raft5_server::pb::raft5::cluster::ClusterMember;

pub mod certs;

const OVERALL_TIMEOUT: Duration = Duration::from_secs(10);
const RETRY_DELAY: Duration = Duration::from_millis(20);

/// A connected client for a node's client API. `KvClient`'s RPC methods
/// take `&mut self`, but the `Channel` under it is cheap to clone and safe
/// to use concurrently. So every function below takes `&Arc<Client>` and
/// clones its own copy to make a call; the `Arc` lets many callers (and
/// `TestNode` itself) share one connection instead of each dialing its own.
pub type Client = KvClient<Channel>;

/// A node started for a test: its own new database directory and fixed
/// addresses, plus a background task running the real server loop
/// (`raft5_server::serve_config`), the token that stops it, and a client
/// connected to it. All `None` while the node is shut down on purpose with
/// `shut_down` (as opposed to removed for good by `Drop`).
pub struct TestNode {
    pub id: String,
    pub client_addr: SocketAddr,
    pub peer_addr: SocketAddr,
    pub db_dir: PathBuf,
    /// How this node signs and checks tokens, or `None` for a node that keeps
    /// no accounts, which is what most tests use.
    auth: Option<raft5_server::auth::AuthConfig>,
    tls: TestTls,
    running: Option<RunningNode>,
    /// Private: reached only through `client()`, so a test that uses it while
    /// the node is down gets a clear panic instead of a `None` to unwrap.
    client: Option<Arc<Client>>,
}

/// How a test node uses TLS. The default is no TLS at all, which is what
/// most tests use.
#[derive(Clone, Default)]
pub struct TestTls {
    pub peer: Option<raft5_server::PeerTls>,
    pub client: Option<raft5_server::ClientApiTls>,
    /// How this harness's own client dials the client API. Needed whenever
    /// `client` is set.
    pub dial: Option<ClientTlsConfig>,
    /// Advertise both APIs as `http://` even with TLS on, as a cluster
    /// configured without TLS records its addresses, so a test can check that
    /// they are still dialed over TLS.
    pub advertise_http: bool,
}

struct RunningNode {
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

impl TestNode {
    pub async fn new(id: &str) -> Self {
        Self::start(id, None, TestTls::default()).await
    }

    pub async fn with_tls(id: &str, tls: TestTls) -> Self {
        Self::start(id, None, tls).await
    }

    pub async fn with_auth_and_tls(
        id: &str,
        auth: raft5_server::auth::AuthConfig,
        tls: TestTls,
    ) -> Self {
        Self::start(id, Some(auth), tls).await
    }

    /// A node that requires a token on every RPC except `Login` and
    /// `GetConfig`.
    pub async fn with_auth(id: &str, auth: raft5_server::auth::AuthConfig) -> Self {
        Self::start(id, Some(auth), TestTls::default()).await
    }

    async fn start(
        id: &str,
        auth: Option<raft5_server::auth::AuthConfig>,
        tls: TestTls,
    ) -> Self {
        // `try_init`, not `init`: every test in the binary calls this, in the same
        // process (possibly at the same time, since tests run on their own threads
        // by default), and `init` panics on a second call. Ignoring the error is
        // fine: the first call that succeeds sets the filter and format.
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_test_writer()
            .try_init();

        let mut node = Self {
            id: id.to_string(),
            client_addr: free_local_addr(),
            peer_addr: free_local_addr(),
            db_dir: temp_db_dir(id),
            auth,
            tls,
            running: None,
            client: None,
        };
        node.start_up().await;
        node
    }

    /// Start (or restart) this node's server task with its existing identity,
    /// addresses and database, as a real restart does: membership and log
    /// persist in `db_dir`, so the node rejoins the cluster it was in instead
    /// of coming back uninitialized. Also (re)connects `client`, retrying while
    /// the listener comes up. Panics if the node is already running.
    ///
    /// No retry on the database lock: `shut_down` does not return until the
    /// previous instance has fully finished, in-flight storage work included,
    /// so nothing still holds it.
    pub async fn start_up(&mut self) {
        assert!(
            self.running.is_none(),
            "node {} is already running",
            self.id
        );
        let id = self.id.clone();
        let db_dir = self.db_dir.clone();
        let client_addr = self.client_addr;
        let peer_addr = self.peer_addr;
        let auth = self.auth.clone();
        let tls = self.tls.clone();
        let (advertise, advertise_client) = if tls.advertise_http {
            (
                Some(format!("http://{peer_addr}")),
                Some(format!("http://{client_addr}")),
            )
        } else {
            (None, None)
        };
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let task = tokio::spawn(async move {
            let node = raft5_server::serve_config(
                raft5_server::ServerConfig {
                    id: id.clone(),
                    db: db_dir,
                    client_addr,
                    peer_addr,
                    advertise,
                    advertise_client,
                    auth,
                    peer_tls: tls.peer,
                    client_tls: tls.client,
                },
                cancel_clone,
            );
            if let Err(e) = node.await {
                tracing::warn!("node {id} exited with an error: {e}");
            }
        });
        self.running = Some(RunningNode { cancel, task });
        self.client = Some(Arc::new(connect(self.client_addr, self.tls.dial.clone()).await));
    }

    /// Stop this node and wait until it is really gone (sockets released,
    /// database lock released, every background task joined) before returning,
    /// so a `start_up` right after gets a clean start. Drops `client` too, so
    /// nothing can keep using a connection to a node that is gone. Leaves the
    /// database alone, so the node comes back from `start_up` with everything
    /// it had. Does nothing if it is not running.
    pub async fn shut_down(&mut self) {
        self.client = None;
        if let Some(RunningNode { cancel, task }) = self.running.take() {
            cancel.cancel();
            task.await.expect("node task panicked");
        }
    }

    /// This node's client. Panics if it is not running: never started, or
    /// `shut_down` and not yet `start_up` again.
    pub fn client(&self) -> Arc<Client> {
        self.client
            .clone()
            .unwrap_or_else(|| panic!("node {} has no client -- is it running?", self.id))
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        // Best effort: `Drop` cannot `.await` like `shut_down` does, so this only
        // signals. That is fine: nothing after this needs the addresses or the
        // database back, and cancelling still lets the node shut itself down
        // properly instead of being abandoned midway, as an `abort` would leave it.
        if let Some(running) = self.running.take() {
            running.cancel.cancel();
        }
        let _ = std::fs::remove_dir_all(&self.db_dir);
    }
}

/// Bind an ephemeral local port and release it at once, to give a node a
/// real address before its server exists.
fn free_local_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().expect("local_addr")
}

fn temp_db_dir(label: &str) -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let counter = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut path = std::env::temp_dir();
    path.push(format!(
        "raft5_integration_test_{}_{label}_{nanos:x}_{counter:x}",
        std::process::id()
    ));
    path
}

fn deadline() -> Instant {
    Instant::now() + OVERALL_TIMEOUT
}

/// Connect to a node's client API, retrying while its server comes up (the
/// listener may not be bound right after spawning). Used only by
/// `TestNode::start_up`; other code gets a client through
/// `TestNode::client` instead of dialing its own.
async fn connect(addr: SocketAddr, tls: Option<ClientTlsConfig>) -> Client {
    let until = deadline();
    loop {
        let endpoint = match &tls {
            Some(tls) => Endpoint::from_shared(format!("https://{addr}"))
                .unwrap()
                .tls_config(tls.clone())
                .expect("the test's TLS config should build"),
            None => Endpoint::from_shared(format!("http://{addr}")).unwrap(),
        };
        match endpoint.connect().await.map(KvClient::new) {
            Ok(client) => return client,
            Err(e) if Instant::now() < until => {
                tokio::time::sleep(RETRY_DELAY).await;
                let _ = e;
            }
            Err(e) => panic!("failed to connect to {addr} within {OVERALL_TIMEOUT:?}: {e}"),
        }
    }
}

/// Bootstrap `server_id` into a single-node cluster, with retries: right
/// after the node starts, its gRPC service exists but the state machine may
/// not have reached `Uninitialized` yet, so an immediate `Bootstrap` can
/// briefly race that.
pub async fn bootstrap(client: &Arc<Client>, server_id: &str) {
    let mut client = (**client).clone();
    let until = deadline();
    loop {
        let resp = client
            .bootstrap(BootstrapRequest {
                server_id: server_id.to_string(),
                heartbeat_millis: 0,
            })
            .await;
        match resp {
            Ok(r) => match r.into_inner().result {
                Some(config_change_response::Result::Ok(true)) => return,
                other => assert!(Instant::now() < until, "bootstrap refused: {other:?}"),
            },
            Err(e) => assert!(Instant::now() < until, "bootstrap RPC failed: {e}"),
        }
        tokio::time::sleep(RETRY_DELAY).await;
    }
}

/// Add `node` to the cluster this client's node leads, retrying on
/// temporary refusals such as `NotLeader` or `ConfigChangeInProgress` (for
/// example, a membership change from a previous call still committing).
pub async fn add_server(client: &Arc<Client>, node: &TestNode) {
    let server_id = &node.id;
    let mut client = (**client).clone();
    let until = deadline();
    loop {
        let resp = client
            .add_server(AddServerRequest {
                peer_address: format!("http://{}", node.peer_addr),
            })
            .await;
        match resp {
            Ok(r) => match r.into_inner().result {
                Some(config_change_response::Result::Ok(true)) => return,
                other => assert!(
                    Instant::now() < until,
                    "add_server({server_id}) did not succeed in time: {other:?}"
                ),
            },
            Err(e) => assert!(Instant::now() < until, "add_server RPC failed: {e}"),
        }
        tokio::time::sleep(RETRY_DELAY).await;
    }
}

/// Write `key`/`value` through this client's node, retrying while it is
/// not (yet) the leader, for example right after `bootstrap`, before the
/// single-node election finishes. Returns the seq the write committed at.
///
/// A transaction is the only way to write, so this opens one around the
/// single key, as `raft5_client::Client::write` does. An attempt that loses
/// the key to another transaction is retried like everything else.
pub async fn write(client: &Arc<Client>, key: &[u8], value: Option<&[u8]>) -> u64 {
    let until = deadline();
    loop {
        let id = begin(client, Duration::from_secs(2)).await;
        txn_write(client, &id, key, value).await;
        match commit(client, &id).await {
            Ok(seq) => return seq,
            other => {
                assert!(
                    Instant::now() < until,
                    "write did not commit in time: {other:?}"
                );
                tokio::time::sleep(RETRY_DELAY).await;
            }
        }
    }
}

/// Open a transaction on `client`, retrying while the cluster settles.
/// Returns the transaction id.
pub async fn begin(client: &Arc<Client>, ttl: Duration) -> Vec<u8> {
    use raft5_shared::raft5::client::{CreateTransactionRequest, create_transaction_response};

    let mut client = (**client).clone();
    let until = deadline();
    loop {
        let resp = client
            .create_transaction(CreateTransactionRequest {
                ttl_ms: ttl.as_millis() as u64,
            })
            .await;
        match resp.map(|r| r.into_inner().result) {
            Ok(Some(create_transaction_response::Result::TransactionId(id))) => return id,
            other => assert!(
                Instant::now() < until,
                "no transaction could be opened: {other:?}"
            ),
        }
        tokio::time::sleep(RETRY_DELAY).await;
    }
}

/// Buffer one write into `id`, and assert it was accepted. Named after the
/// transaction, not the RPC, to keep it apart from `write` above, which is
/// a whole transaction of its own.
pub async fn txn_write(client: &Arc<Client>, id: &[u8], key: &[u8], value: Option<&[u8]>) {
    let mut client = (**client).clone();
    let resp = client
        .write(WriteRequest {
            transaction_id: id.to_vec(),
            key: key.to_vec(),
            value: value.map(<[u8]>::to_vec),
        })
        .await
        .expect("write should reach the node")
        .into_inner();
    assert!(
        matches!(resp.result, Some(write_response::Result::Ok(true))),
        "write was refused: {:?}",
        resp.result
    );
}

/// Commit `id`, and return the seq or the refusal.
pub async fn commit(
    client: &Arc<Client>,
    id: &[u8],
) -> Result<u64, raft5_shared::raft5::client::RaftError> {
    use raft5_shared::raft5::client::{CommitTransactionRequest, commit_transaction_response};

    let mut client = (**client).clone();
    let resp = client
        .commit_transaction(CommitTransactionRequest {
            transaction_id: id.to_vec(),
        })
        .await
        .expect("commit should reach the node")
        .into_inner();
    match resp.result {
        Some(commit_transaction_response::Result::Seq(seq)) => Ok(seq),
        Some(commit_transaction_response::Result::Error(e)) => Err(e),
        None => panic!("the node answered a commit without a result"),
    }
}

/// A (possibly stale) local read of `key`, or `None` if it has no value now.
pub async fn read(client: &Arc<Client>, key: &[u8]) -> Option<Vec<u8>> {
    let mut client = (**client).clone();
    let resp = client
        .read(ReadRequest {
            keys: vec![key.to_vec()],
            versions_only: false,
            consistency: None,
        })
        .await
        .expect("read RPC failed")
        .into_inner();
    match resp.result {
        Some(read_response::Result::Ok(ok)) => ok.entries.into_iter().next().and_then(|e| e.value),
        other => panic!("expected a successful read, got {other:?}"),
    }
}

/// This node's current cluster configuration, from its own log (Raft §4.1:
/// it shows a change as soon as it is appended, not only once committed).
/// Empty if the node is not yet bootstrapped or added to a cluster.
pub async fn config(client: &Arc<Client>) -> Vec<ClusterMember> {
    let mut client = (**client).clone();
    client
        .get_config(GetConfigRequest {})
        .await
        .expect("get_config RPC failed")
        .into_inner()
        .members
}

/// Wait until `server_id` shows up as a voting member in the configuration
/// of `client`'s node, for example after `add_server` joins it as a
/// learner, to wait for its automatic promotion (Raft §4.2.1). This matters
/// beyond membership: fewer voters than intended changes what a majority
/// is, so a caller that needs a specific voter count (for example, before
/// stopping one on purpose) must confirm it, not assume promotion is done.
pub async fn wait_until_voter(client: &Arc<Client>, server_id: &str) {
    let until = deadline();
    loop {
        let members = config(client).await;
        if members.iter().any(|m| m.server_id == server_id && m.voting) {
            return;
        }
        assert!(
            Instant::now() < until,
            "{server_id} never became a voter in the queried node's configuration"
        );
        tokio::time::sleep(RETRY_DELAY).await;
    }
}

/// Poll `client`'s node until its own local state shows `key == value` at
/// `min_seq` or later. A follower applies a commit asynchronously as
/// `AppendEntries` batches arrive, so this is not instant like the leader's
/// own read after a write.
pub async fn assert_eventually_reads(client: &Arc<Client>, key: &[u8], value: &[u8], min_seq: u64) {
    let mut client = (**client).clone();
    let until = deadline();
    loop {
        let resp = client
            .read(ReadRequest {
                keys: vec![key.to_vec()],
                versions_only: false,
                consistency: None,
            })
            .await
            .expect("read RPC failed")
            .into_inner();
        if let Some(read_response::Result::Ok(ok)) = &resp.result
            && let [entry] = ok.entries.as_slice()
            && entry.seq >= min_seq
            && entry.value.as_deref() == Some(value)
        {
            return;
        }
        assert!(
            Instant::now() < until,
            "replica never caught up to seq {min_seq} with the expected value: {resp:?}"
        );
        tokio::time::sleep(RETRY_DELAY).await;
    }
}

/// Find the cluster's current leader among `nodes`: ask each in turn for its
/// configuration, and retry the whole set until one says `is_leader`. Only a
/// node that has caught up on what it inherited and still holds a live read
/// quorum says so (see `grpc::KvService::get_config`), so this is leader
/// discovery backed by a real quorum check, not a guess.
///
/// Nodes that are down (`shut_down`, so their client is gone) are skipped,
/// not treated as a failure, like a node that lost the leadership race this
/// round.
pub async fn find_leader<'a>(nodes: &[&'a TestNode]) -> &'a TestNode {
    let until = deadline();
    loop {
        for &node in nodes {
            if let Some(client) = &node.client {
                let mut client = (**client).clone();
                if let Ok(resp) = client.get_config(GetConfigRequest {}).await
                    && resp.into_inner().is_leader
                {
                    return node;
                }
            }
        }
        assert!(
            Instant::now() < until,
            "no node among the given set answered as leader in time"
        );
        tokio::time::sleep(RETRY_DELAY).await;
    }
}
