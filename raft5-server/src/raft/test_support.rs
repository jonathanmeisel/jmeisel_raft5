use super::*;
use crate::pb::raft5::cluster::{ClusterConfig, ClusterMember};
use crate::pb::raft5::common::EntryKind;
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) fn open_temp_log() -> (Arc<RaftLog>, std::path::PathBuf) {
    // `try_init`, not `init`: every test that starts a node comes here, in the
    // same process, and `init` panics on a second call. Ignoring the error is
    // fine: the first call that succeeds sets the filter and format.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    // A nanosecond timestamp alone is not a reliable unique suffix when tests
    // run in parallel: two tests that start at nearly the same moment can get
    // the same value and collide on one directory. A process-wide counter on
    // top guarantees uniqueness.
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

/// A `ClusterConfig` from `(server_id, voting)` pairs. Addresses point at
/// ports nothing listens on: these tests are about what the configuration
/// means, so peers are expected never to answer.
pub(crate) fn cluster_config(members: &[(&str, bool)]) -> ClusterConfig {
    ClusterConfig {
        members: members
            .iter()
            .enumerate()
            .map(|(i, (id, voting))| ClusterMember {
                server_id: (*id).to_string(),
                address: format!("http://127.0.0.1:{}", 9000 + i),
                client_address: format!("http://127.0.0.1:{}", 8000 + i),
                voting: *voting,
            })
            .collect(),
        heartbeat_millis: DEFAULT_HEARTBEAT_MS,
    }
}

/// A temp log already bootstrapped with `config`, plus a `Common` that
/// reads its membership from it (not from a peer list).
pub(crate) fn open_temp_log_with_config(
    node_id: &str,
    config: &ClusterConfig,
) -> (Arc<RaftLog>, NodeHandles, std::path::PathBuf) {
    let (log, path) = open_temp_log();
    log.append_config(1, &config.to_bytes()).expect("bootstrap");
    let common = Common::new(
        node_id.to_string(),
        Arc::clone(&log),
        CancellationToken::new(),
    )
    .unwrap();
    (log, common, path)
}

pub(crate) fn cleanup(path: &std::path::Path) {
    let _ = std::fs::remove_dir_all(path);
}

/// Start a real event loop (as `main` would), and return the two external
/// handles plus the temp dir to clean up afterwards.
pub(crate) fn spawn_test_node(
    peers: Vec<PeerInfo>,
) -> (
    watch::Receiver<RoleHandle>,
    RaftPeerHandle,
    std::path::PathBuf,
) {
    let (log, path) = open_temp_log();
    let NodeHandles {
        common,
        role_rx,
        peer_handle,
        ..
    } = Common::new_seeded(
        "test-node".to_string(),
        peers,
        log,
        CancellationToken::new(),
    )
    .unwrap();
    tokio::spawn(run(common));
    (role_rx, peer_handle, path)
}

/// Like `spawn_test_node`, but also returns the storage handle (to check
/// what landed in it) and `commit_rx`/`compact_rx` (to watch replication
/// and watermark progress), for the log-matching and replication tests.
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
    let NodeHandles {
        common,
        role_rx,
        peer_handle,
        commit_rx,
        compact_rx,
        ..
    } = Common::new_seeded(
        "test-node".to_string(),
        peers,
        Arc::clone(&log),
        CancellationToken::new(),
    )
    .unwrap();
    tokio::spawn(run(common));
    (log, role_rx, peer_handle, commit_rx, compact_rx, path)
}

pub(crate) async fn wait_for_role(
    role_rx: &mut watch::Receiver<RoleHandle>,
    pred: impl Fn(&RoleHandle) -> bool,
) {
    // Generous compared with the 150-300ms election timeout: real-network
    // tests (an election plus a vote round trip, or an election followed by
    // replication) need room to not flake under the CPU load of parallel
    // tests.
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

/// A one-mutation `WRITE` entry, the shape almost every test wants.
/// `value = None` is a delete.
pub(crate) fn log_entry(seq: u64, term: u64, key: &[u8], value: Option<&[u8]>) -> LogEntry {
    write_entry(seq, term, vec![(key.to_vec(), value.map(<[u8]>::to_vec))])
}

/// A `WRITE` entry with any number of mutations: what a committed
/// transaction looks like on the wire.
pub(crate) fn write_entry(
    seq: u64,
    term: u64,
    writes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
) -> LogEntry {
    LogEntry {
        seq,
        term,
        kind: EntryKind::Write as i32,
        writes: writes
            .into_iter()
            .map(|(key, value)| crate::pb::raft5::common::KeyValue { key, value })
            .collect(),
        config: None,
    }
}

/// A `ChunkReader` that already holds `bytes`, for tests that have a whole
/// snapshot and just want to give it to `install_snapshot`.
///
/// The channel is sized to the number of chunks and the sender is dropped
/// at once, so nothing blocks. In production, a live gRPC stream fills this
/// channel.
pub(crate) fn reader_over(bytes: Vec<u8>) -> crate::store::ChunkReader {
    let chunks: Vec<Vec<u8>> = if bytes.is_empty() {
        Vec::new()
    } else {
        bytes.chunks(64 * 1024).map(<[u8]>::to_vec).collect()
    };
    let (tx, rx) = tokio::sync::mpsc::channel(chunks.len() + 1);
    for chunk in chunks {
        tx.try_send(chunk).expect("the channel was sized for these");
    }
    drop(tx);
    crate::store::ChunkReader::new(rx)
}

/// Bind an ephemeral local port and release it at once, to give a real
/// address to a peer and server pair before either exists. There is a small
/// race (something else could take the port first), but this is standard
/// for this kind of test.
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
