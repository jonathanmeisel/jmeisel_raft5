use super::*;
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) fn open_temp_log() -> (Arc<RaftLog>, std::path::PathBuf) {
    // A nanosecond timestamp alone isn't a reliable unique suffix under
    // parallel test execution -- two tests starting at nearly the same
    // instant can land on the same value (clock resolution, scheduling)
    // and collide on the same directory, so a process-wide counter is
    // layered on top to guarantee uniqueness regardless.
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let mut path = std::env::temp_dir();
    path.push(format!("raft5_raft_test_{}", std::process::id()));
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let counter = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    path.push(format!("{suffix:x}_{counter:x}"));
    let log = Arc::new(RaftLog::open(&path).expect("open"));
    (log, path)
}

pub(crate) fn cleanup(path: &std::path::Path) {
    let _ = std::fs::remove_dir_all(path);
}

/// Spins up a real event loop (as `main` would) and hands back the two
/// external handles plus the temp dir to clean up afterward.
pub(crate) fn spawn_test_node(
    peers: Vec<PeerInfo>,
) -> (
    watch::Receiver<RoleHandle>,
    RaftPeerHandle,
    std::path::PathBuf,
) {
    let (log, path) = open_temp_log();
    let (common, role_rx, peer_handle, _commit_rx, _compact_rx) =
        Common::new("test-node".to_string(), peers, log).unwrap();
    tokio::spawn(run(common));
    (role_rx, peer_handle, path)
}

/// Like `spawn_test_node`, but also hands back the storage handle (to
/// check what actually landed in it) and `commit_rx`/`compact_rx` (to
/// observe replication/watermark progress) -- needed for the
/// log-matching and replication tests.
pub(crate) fn spawn_node_with_handles(
    peers: Vec<PeerInfo>,
) -> (
    Arc<RaftLog>,
    watch::Receiver<RoleHandle>,
    RaftPeerHandle,
    watch::Receiver<u64>,
    watch::Receiver<u64>,
    std::path::PathBuf,
) {
    let (log, path) = open_temp_log();
    let (common, role_rx, peer_handle, commit_rx, compact_rx) =
        Common::new("test-node".to_string(), peers, Arc::clone(&log)).unwrap();
    tokio::spawn(run(common));
    (log, role_rx, peer_handle, commit_rx, compact_rx, path)
}

pub(crate) async fn wait_for_role(
    role_rx: &mut watch::Receiver<RoleHandle>,
    pred: impl Fn(&RoleHandle) -> bool,
) {
    // Generous relative to the 150-300ms election timeout: real-network
    // tests (election + a vote RPC round trip, or a full election
    // followed by replication) need enough headroom to not flake under
    // parallel test execution's CPU contention.
    tokio::time::timeout(Duration::from_millis(1500), async {
        loop {
            if pred(&role_rx.borrow()) {
                return;
            }
            role_rx.changed().await.unwrap();
        }
    })
    .await
    .expect("expected role to match the predicate before the timeout")
}

pub(crate) fn blank_append_entries(term: u64, leader_id: &str) -> AppendEntriesRequest {
    AppendEntriesRequest {
        term,
        leader_id: leader_id.to_string(),
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![],
        leader_commit: 0,
        compact_seq: 0,
    }
}

pub(crate) fn request_vote(term: u64, candidate_id: &str) -> RequestVoteRequest {
    RequestVoteRequest {
        term,
        candidate_id: candidate_id.to_string(),
        last_log_index: 0,
        last_log_term: 0,
        dry_run: false,
    }
}

pub(crate) fn append_entries(
    term: u64,
    leader_id: &str,
    prev_log_index: u64,
    prev_log_term: u64,
    entries: Vec<LogEntry>,
    leader_commit: u64,
) -> AppendEntriesRequest {
    AppendEntriesRequest {
        term,
        leader_id: leader_id.to_string(),
        prev_log_index,
        prev_log_term,
        entries,
        leader_commit,
        compact_seq: 0,
    }
}

pub(crate) fn log_entry(seq: u64, term: u64, key: &[u8], value: Option<&[u8]>) -> LogEntry {
    LogEntry {
        seq,
        term,
        key: key.to_vec(),
        value: value.map(<[u8]>::to_vec),
    }
}

/// Binds an ephemeral local port and immediately releases it, for
/// handing a real address to a peer/server pair before either exists.
/// Small race (something else could grab the port before we rebind
/// it), but standard practice for this kind of test.
pub(crate) fn free_local_addr() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().expect("local_addr")
}

pub(crate) fn expect_leader_handle(role: &RoleHandle) -> LeaderHandle {
    match role {
        RoleHandle::Leader(h) => h.clone(),
        _ => panic!("expected Leader"),
    }
}
