//! Shared setup for integration tests: spinning up real
//! `raft5_server::run` nodes on real localhost sockets and driving them
//! purely through the client gRPC API, with retries for the transient
//! refusals that are a normal part of startup/election/replication
//! (rather than fixed sleeps guessing how long those take).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use log::warn;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

use raft5_server::pb::raft5::client::kv_client::KvClient;
use raft5_server::pb::raft5::client::{
    AddServerRequest, BootstrapRequest, GetConfigRequest, ReadRequest, WriteRequest,
    config_change_response, read_response, write_response,
};
use raft5_server::pb::raft5::cluster::ClusterMember;

const OVERALL_TIMEOUT: Duration = Duration::from_secs(10);
const RETRY_DELAY: Duration = Duration::from_millis(20);

/// A connected client for a node's client API. `KvClient`'s own RPC
/// methods take `&mut self`, but the `Channel` underneath is cheap to
/// clone and safe to use concurrently, so every function below takes
/// `&Arc<Client>` and clones a private, owned copy out of it to actually
/// make a call -- the `Arc` is what lets many callers (and `TestNode`
/// itself) share the one connection without each dialing their own.
pub type Client = KvClient<Channel>;

/// A node started for a test: its own fresh database directory and fixed
/// addresses, plus a background task running the real
/// `raft5_server::run` server loop, the token that stops it, and a
/// client already connected to it -- all `None` while deliberately shut
/// down via `shut_down` (as opposed to torn down for good via `Drop`).
pub struct TestNode {
    pub id: String,
    pub client_addr: SocketAddr,
    pub peer_addr: SocketAddr,
    pub db_dir: PathBuf,
    running: Option<RunningNode>,
    /// Private: reached only through the `client()` accessor, so a test
    /// that reaches for this while the node is down gets a clear panic
    /// instead of a `None` it has to unwrap itself.
    client: Option<Arc<Client>>,
}

struct RunningNode {
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

impl TestNode {
    pub async fn new(id: &str) -> Self {
        // `try_init` rather than `init`: every test in the binary calls
        // this, all in the same process (possibly concurrently, since
        // tests run on their own threads by default), and `init` panics
        // on a second call. Ignoring the error is fine either way -- the
        // first call to succeed is the one whose filter/format wins.
        let _ = env_logger::try_init();

        let mut node = Self {
            id: id.to_string(),
            client_addr: free_local_addr(),
            peer_addr: free_local_addr(),
            db_dir: temp_db_dir(id),
            running: None,
            client: None,
        };
        node.start_up().await;
        node
    }

    /// (Re-)starts this node's server task against its existing identity,
    /// addresses, and database -- what a real restart looks like:
    /// membership and log persist in `db_dir` across it, so the node
    /// rejoins whatever cluster it left off in rather than coming back
    /// uninitialized. Also (re-)connects `client`, retrying while the
    /// listener is still coming up. Panics if it's already running.
    ///
    /// No retry on the database lock: `shut_down` doesn't return until
    /// the previous incarnation has fully finished, in-flight storage
    /// work included, so there is nothing left holding it by this point.
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
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let task = tokio::spawn(async move {
            let node = raft5_server::serve(
                id.clone(),
                &db_dir,
                client_addr,
                peer_addr,
                None,
                None,
                cancel_clone,
            );
            if let Err(e) = node.await {
                warn!("node {id} exited with an error: {e}");
            }
        });
        self.running = Some(RunningNode { cancel, task });
        self.client = Some(Arc::new(connect(self.client_addr).await));
    }

    /// Stops this node and waits for it to actually be gone -- sockets
    /// released, database lock released, every background task joined --
    /// before returning, so a `start_up` right after gets a clean slate.
    /// Drops `client` too, so nothing can go on using a connection to a
    /// node that is no longer there. Leaves the database alone, so the
    /// node comes back from `start_up` with everything it had before. A
    /// no-op if it isn't running.
    pub async fn shut_down(&mut self) {
        self.client = None;
        if let Some(RunningNode { cancel, task }) = self.running.take() {
            cancel.cancel();
            task.await.expect("node task panicked");
        }
    }

    /// This node's client. Panics if it isn't currently running --
    /// either it was never started, or it's been `shut_down` and not yet
    /// `start_up` again.
    pub fn client(&self) -> Arc<Client> {
        self.client
            .clone()
            .unwrap_or_else(|| panic!("node {} has no client -- is it running?", self.id))
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        // Best-effort: `Drop` can't `.await` the way `shut_down` does, so
        // this only signals. That's fine here -- nothing after this needs
        // the addresses or the database back, and cancelling still means
        // the node winds itself down properly rather than being abandoned
        // mid-flight the way an `abort` would leave it.
        if let Some(running) = self.running.take() {
            running.cancel.cancel();
        }
        let _ = std::fs::remove_dir_all(&self.db_dir);
    }
}

/// Binds an ephemeral local port and immediately releases it, for handing
/// a real address to a node before its server exists yet.
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

/// Connects to a node's client API, retrying while its server is still
/// coming up (the listener may not have bound yet right after spawning).
/// Internal to `TestNode::start_up` -- callers elsewhere get a client via
/// `TestNode::client` instead of dialing their own.
async fn connect(addr: SocketAddr) -> Client {
    let until = deadline();
    loop {
        match KvClient::connect(format!("http://{addr}")).await {
            Ok(client) => return client,
            Err(e) if Instant::now() < until => {
                tokio::time::sleep(RETRY_DELAY).await;
                let _ = e;
            }
            Err(e) => panic!("failed to connect to {addr} within {OVERALL_TIMEOUT:?}: {e}"),
        }
    }
}

/// Bootstraps `server_id` into a single-node cluster, retrying: right
/// after `spawn_node`, the node's gRPC service exists but the state
/// machine hasn't necessarily reached `Uninitialized` internally, so an
/// immediate `Bootstrap` can transiently race that.
pub async fn bootstrap(client: &Arc<Client>, server_id: &str) {
    let mut client = (**client).clone();
    let until = deadline();
    loop {
        let resp = client
            .bootstrap(BootstrapRequest {
                server_id: server_id.to_string(),
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

/// Adds `node` to the cluster this client's node leads, retrying on
/// `NotLeader`/`ConfigChangeInProgress`-style transient refusals (e.g. a
/// membership change from a previous call still committing).
pub async fn add_server(client: &Arc<Client>, node: &TestNode) {
    let server_id = &node.id;
    let mut client = (**client).clone();
    let until = deadline();
    loop {
        let resp = client
            .add_server(AddServerRequest {
                server_id: server_id.to_string(),
                address: format!("http://{}", node.peer_addr),
                client_address: format!("http://{}", node.client_addr),
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

/// Writes `key`/`value` through this client's node, retrying while it
/// isn't (yet) the leader -- e.g. immediately after `bootstrap`, before
/// the resulting single-node election has finished. Returns the seq the
/// write committed at.
pub async fn write(client: &Arc<Client>, key: &[u8], value: Option<&[u8]>) -> u64 {
    let mut client = (**client).clone();
    let until = deadline();
    loop {
        let resp = client
            .write(WriteRequest {
                key: key.to_vec(),
                value: value.map(<[u8]>::to_vec),
            })
            .await
            .expect("write RPC failed");
        match resp.into_inner().result {
            Some(write_response::Result::Seq(seq)) => return seq,
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

/// A (possibly stale) local read of `key`, or `None` if it currently has
/// no value.
pub async fn read(client: &Arc<Client>, key: &[u8]) -> Option<Vec<u8>> {
    let mut client = (**client).clone();
    let resp = client
        .read(ReadRequest {
            key: key.to_vec(),
            linearizable: false,
        })
        .await
        .expect("read RPC failed")
        .into_inner();
    match resp.result {
        Some(read_response::Result::Ok(ok)) => ok.value,
        other => panic!("expected a successful read, got {other:?}"),
    }
}

/// This node's current cluster configuration, straight from its own log
/// (Raft §4.1 -- reflects a change as soon as it's appended, not only
/// once it commits). Empty if it hasn't been bootstrapped or added to a
/// cluster yet.
pub async fn config(client: &Arc<Client>) -> Vec<ClusterMember> {
    let mut client = (**client).clone();
    client
        .get_config(GetConfigRequest {})
        .await
        .expect("get_config RPC failed")
        .into_inner()
        .members
}

/// Waits until `server_id` shows up as a *voting* member of `client`'s
/// node's configuration -- e.g. after `add_server` joins it as a
/// learner, waiting here for its automatic promotion (Raft §4.2.1) to
/// land. This matters beyond just membership: a config with fewer voters
/// than intended changes what majority means, so a caller that needs a
/// specific voter count (e.g. before deliberately taking one down) has
/// to confirm it, not assume promotion already finished.
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

/// Polls `client`'s node until its own local state reflects `key ==
/// value` at (at least) `min_seq` -- a follower applies a commit
/// asynchronously as `AppendEntries` batches arrive, so this isn't
/// instantaneous the way the leader's own post-write read is.
pub async fn assert_eventually_reads(client: &Arc<Client>, key: &[u8], value: &[u8], min_seq: u64) {
    let mut client = (**client).clone();
    let until = deadline();
    loop {
        let resp = client
            .read(ReadRequest {
                key: key.to_vec(),
                linearizable: false,
            })
            .await
            .expect("read RPC failed")
            .into_inner();
        if let Some(read_response::Result::Ok(ok)) = &resp.result
            && ok.seq >= min_seq
            && ok.value.as_deref() == Some(value)
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

/// Finds the cluster's current leader among `nodes`, by attempting a
/// linearizable read against each of them in turn and retrying the whole
/// set until one succeeds. A linearizable read succeeding at all is
/// proof the answering node holds a live read quorum right now (see
/// `grpc::KvService::read`), not merely that it was first to answer --
/// so this doubles as leader discovery backed by an actual quorum check,
/// not just a guess at which node to send further requests to.
///
/// Nodes that are down (`shut_down`, so their client is gone) are
/// skipped rather than treated as a failure -- same as one that's merely
/// lost the leadership race this round.
pub async fn find_leader<'a>(nodes: &[&'a TestNode]) -> &'a TestNode {
    // The key itself is irrelevant -- linearizability, not this specific
    // key's data, is what proves the answering node is genuinely leading.
    const PROBE_KEY: &[u8] = b"__raft5_test_find_leader_probe__";

    let until = deadline();
    loop {
        for &node in nodes {
            if let Some(client) = &node.client {
                let mut client = (**client).clone();
                let resp = client
                    .read(ReadRequest {
                        key: PROBE_KEY.to_vec(),
                        linearizable: true,
                    })
                    .await;
                if let Ok(resp) = resp
                    && matches!(resp.into_inner().result, Some(read_response::Result::Ok(_)))
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
