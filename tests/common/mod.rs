//! Shared setup for integration tests: spinning up real
//! `jmeisel_raft5::run` nodes on real localhost sockets and driving them
//! purely through the client gRPC API, with retries for the transient
//! refusals that are a normal part of startup/election/replication
//! (rather than fixed sleeps guessing how long those take).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

use jmeisel_raft5::pb::raft5::client::kv_client::KvClient;
use jmeisel_raft5::pb::raft5::client::{
    AddServerRequest, BootstrapRequest, GetConfigRequest, ReadRequest, WriteRequest,
    config_change_response, read_response, write_response,
};
use jmeisel_raft5::pb::raft5::cluster::ClusterMember;

const OVERALL_TIMEOUT: Duration = Duration::from_secs(10);
const RETRY_DELAY: Duration = Duration::from_millis(20);

/// A node started for a test: its own fresh database directory and
/// fixed addresses, plus a background task running the real
/// `jmeisel_raft5::run` server loop and the token that stops it --
/// `None` while deliberately shut down via `shut_down` (as opposed to
/// torn down for good via `Drop`).
pub struct TestNode {
    pub id: String,
    pub client_addr: SocketAddr,
    pub peer_addr: SocketAddr,
    pub db_dir: PathBuf,
    running: Option<RunningNode>,
}

struct RunningNode {
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

impl TestNode {
    pub fn new(id: &str) -> Self {
        let mut node = Self {
            id: id.to_string(),
            client_addr: free_local_addr(),
            peer_addr: free_local_addr(),
            db_dir: temp_db_dir(id),
            running: None,
        };
        node.start_up();
        node
    }

    /// (Re-)starts this node's server task against its existing identity,
    /// addresses, and database -- what a real restart looks like:
    /// membership and log persist in `db_dir` across it, so the node
    /// rejoins whatever cluster it left off in rather than coming back
    /// uninitialized. Panics if it's already running.
    ///
    /// No retry on the database lock: `shut_down` doesn't return until
    /// the previous incarnation has fully finished, in-flight storage
    /// work included, so there is nothing left holding it by this point.
    pub fn start_up(&mut self) {
        assert!(
            self.running.is_none(),
            "node {} is already running",
            self.id
        );
        let id = self.id.clone();
        let (node, cancel) = jmeisel_raft5::run(
            id.clone(),
            &self.db_dir,
            self.client_addr,
            self.peer_addr,
            None,
        );
        let task = tokio::spawn(async move {
            if let Err(e) = node.await {
                eprintln!("node {id} exited with an error: {e}");
            }
        });
        self.running = Some(RunningNode { cancel, task });
    }

    /// Stops this node and waits for it to actually be gone -- sockets
    /// released, database lock released, every background task joined --
    /// before returning, so a `start_up` right after gets a clean slate.
    /// Leaves the database alone, so the node comes back from `start_up`
    /// with everything it had before. A no-op if it isn't running.
    pub async fn shut_down(&mut self) {
        if let Some(RunningNode { cancel, task }) = self.running.take() {
            cancel.cancel();
            task.await.expect("node task panicked");
        }
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
pub async fn connect(addr: SocketAddr) -> KvClient<Channel> {
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
pub async fn bootstrap(client: &mut KvClient<Channel>, server_id: &str) {
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

/// Adds `server_id` to the cluster this client's node leads, retrying on
/// `NotLeader`/`ConfigChangeInProgress`-style transient refusals (e.g. a
/// membership change from a previous call still committing).
pub async fn add_server(client: &mut KvClient<Channel>, server_id: &str, peer_addr: SocketAddr) {
    let until = deadline();
    loop {
        let resp = client
            .add_server(AddServerRequest {
                server_id: server_id.to_string(),
                address: format!("http://{peer_addr}"),
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
pub async fn write(client: &mut KvClient<Channel>, key: &[u8], value: Option<&[u8]>) -> u64 {
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
pub async fn read(client: &mut KvClient<Channel>, key: &[u8]) -> Option<Vec<u8>> {
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
pub async fn config(client: &mut KvClient<Channel>) -> Vec<ClusterMember> {
    client
        .get_config(GetConfigRequest {})
        .await
        .expect("get_config RPC failed")
        .into_inner()
        .members
}

/// Waits until `server_id` shows up as a *voting* member of `node`'s
/// configuration -- e.g. after `add_server` joins it as a learner,
/// waiting here for its automatic promotion (Raft §4.2.1) to land. This
/// matters beyond just membership: a config with fewer voters than
/// intended changes what majority means, so a caller that needs a
/// specific voter count (e.g. before deliberately taking one down) has
/// to confirm it, not assume promotion already finished.
pub async fn wait_until_voter(node: &TestNode, server_id: &str) {
    let until = deadline();
    let mut client = connect(node.client_addr).await;
    loop {
        let members = config(&mut client).await;
        if members.iter().any(|m| m.server_id == server_id && m.voting) {
            return;
        }
        assert!(
            Instant::now() < until,
            "{server_id} never became a voter in {node}'s configuration",
            node = node.id
        );
        tokio::time::sleep(RETRY_DELAY).await;
    }
}

/// Polls `client`'s node until its own local state reflects `key ==
/// value` at (at least) `min_seq` -- a follower applies a commit
/// asynchronously as `AppendEntries` batches arrive, so this isn't
/// instantaneous the way the leader's own post-write read is.
pub async fn assert_eventually_reads(
    client: &mut KvClient<Channel>,
    key: &[u8],
    value: &[u8],
    min_seq: u64,
) {
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

/// Tries `attempt` against each of `nodes` in turn, retrying the whole
/// set until one succeeds (returns `Some`) or the overall deadline
/// elapses. Shared by `write_to_leader`/`read_from_leader`: both are
/// "do this against whichever node is currently the leader," discovered
/// simply by trying -- a non-leader's refusal, or a node that's
/// unreachable (e.g. one deliberately shut down), just fails `attempt`
/// and moves on to the next candidate.
async fn try_each_node<T, Fut>(
    nodes: &[&TestNode],
    mut attempt: impl FnMut(SocketAddr) -> Fut,
) -> (SocketAddr, T)
where
    Fut: std::future::Future<Output = Option<T>>,
{
    let until = deadline();
    loop {
        for node in nodes {
            if let Some(result) = attempt(node.client_addr).await {
                return (node.client_addr, result);
            }
        }
        assert!(
            Instant::now() < until,
            "no node among the given set answered as leader in time"
        );
        tokio::time::sleep(RETRY_DELAY).await;
    }
}

/// Writes `key`/`value` to whichever of `nodes` is currently the
/// cluster's leader, discovering it by trying each in turn -- for use
/// once it isn't known in advance which node that is, e.g. right after a
/// leadership change. Returns the leader's address (so the caller can
/// keep talking to the same node afterward) and the seq the write
/// committed at.
pub async fn write_to_leader(
    nodes: &[&TestNode],
    key: &[u8],
    value: Option<&[u8]>,
) -> (SocketAddr, u64) {
    try_each_node(nodes, |addr| async move {
        let mut client = KvClient::connect(format!("http://{addr}")).await.ok()?;
        let resp = client
            .write(WriteRequest {
                key: key.to_vec(),
                value: value.map(<[u8]>::to_vec),
            })
            .await
            .ok()?
            .into_inner();
        match resp.result {
            Some(write_response::Result::Seq(seq)) => Some(seq),
            _ => None,
        }
    })
    .await
}

/// Reads `key` from whichever of `nodes` is currently the cluster's
/// leader -- the read counterpart to `write_to_leader`. With
/// `linearizable: true` this doubles as leader discovery *backed by a
/// live read quorum*: a follower, or a leader that has lost its lease
/// quorum, answers `NotLeader` rather than succeeding (see
/// `grpc::KvService::read`), so a successful result here is itself
/// evidence the answering node is genuinely leading right now, not just
/// the first one to answer.
pub async fn read_from_leader(
    nodes: &[&TestNode],
    key: &[u8],
    linearizable: bool,
) -> (SocketAddr, Option<Vec<u8>>) {
    try_each_node(nodes, |addr| async move {
        let mut client = KvClient::connect(format!("http://{addr}")).await.ok()?;
        let resp = client
            .read(ReadRequest {
                key: key.to_vec(),
                linearizable,
            })
            .await
            .ok()?
            .into_inner();
        match resp.result {
            Some(read_response::Result::Ok(ok)) => Some(ok.value),
            _ => None,
        }
    })
    .await
}
