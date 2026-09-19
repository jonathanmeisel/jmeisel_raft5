use std::sync::Arc;

use super::*;
use crate::pb::raft5::client::{
    CommitTransactionRequest, CreateTransactionRequest, DropTransactionRequest,
    commit_transaction_response, create_transaction_response, raft_error, read_response,
    transaction_aborted, write_response,
};
use crate::raft::{self, RoleHandle};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

fn open_temp() -> (Arc<RaftLog>, std::path::PathBuf) {
    // A nanosecond timestamp alone isn't a reliable unique suffix under
    // parallel test execution -- two tests starting at nearly the same
    // instant can land on the same value (clock resolution, scheduling)
    // and collide on the same directory, so a process-wide counter is
    // layered on top (as a second path segment, not combined into the
    // same u64, since combining risks collisions of its own) to
    // guarantee uniqueness regardless.
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let mut path = std::env::temp_dir();
    path.push(format!("raft5_grpc_test_{}", std::process::id()));
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    path.push(format!("{:x}_{counter:x}", rand_suffix()));
    let log = Arc::new(RaftLog::open(&path).expect("open"));
    (log, path)
}

fn rand_suffix() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

fn cleanup(path: &std::path::Path) {
    let _ = std::fs::remove_dir_all(path);
}

/// Buffers one key into `txn`, as a client's `Write` does.
async fn buffer(
    svc: &KvService,
    txn: &[u8],
    key: &[u8],
    value: Option<&[u8]>,
) -> write_response::Result {
    svc.write(Request::new(WriteRequest {
        transaction_id: txn.to_vec(),
        key: key.to_vec(),
        value: value.map(<[u8]>::to_vec),
    }))
    .await
    .unwrap()
    .into_inner()
    .result
    .expect("a write response always carries a result")
}

/// A whole single-key write the way a client makes one: open a
/// transaction, buffer the key, commit. Returns the seq it landed at.
async fn write(svc: &KvService, key: &[u8], value: Option<&[u8]>) -> u64 {
    let txn = begin_txn(svc, 5000).await;
    assert!(
        matches!(buffer(svc, &txn, key, value).await, write_response::Result::Ok(true)),
        "buffering one key into a fresh transaction should be accepted"
    );
    commit(svc, txn).await
}

/// The seq a transaction that was expected to commit landed at.
async fn commit(svc: &KvService, txn: Vec<u8>) -> u64 {
    let result = svc
        .commit_transaction(Request::new(CommitTransactionRequest {
            transaction_id: txn,
        }))
        .await
        .unwrap()
        .into_inner()
        .result;
    match result {
        Some(commit_transaction_response::Result::Seq(seq)) => seq,
        other => panic!("the transaction should have committed, got {other:?}"),
    }
}

/// A (stale, so always answerable) read of `key`.
async fn read(svc: &KvService, key: &[u8]) -> ReadOk {
    let resp = svc
        .read(Request::new(ReadRequest {
            key: key.to_vec(),
            transaction_id: None,
        }))
        .await
        .unwrap()
        .into_inner();
    match resp.result {
        Some(read_response::Result::Ok(ok)) => ok,
        other => panic!("expected a successful read, got {other:?}"),
    }
}

/// A `KvService` on a node that has been bootstrapped into a
/// single-node cluster and has won its election -- i.e. one that can
/// actually serve a write.
async fn leader_kv_service(log: Arc<RaftLog>, node_id: &str) -> KvService {
    let (svc, mut role_rx) = kv_service(log, node_id);
    svc.bootstrap(Request::new(BootstrapRequest::default()))
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !matches!(&*role_rx.borrow(), RoleHandle::Leader(_)) {
            role_rx.changed().await.unwrap();
        }
    })
    .await
    .expect("a bootstrapped single-node cluster should elect itself");
    // The lock table learns which term it is serving from the first
    // request that needs a leader, and discards everything it held if
    // that term has changed. Doing it here means a test that takes a
    // lock directly -- rather than through an RPC -- isn't holding one
    // from before the node had a term at all, which any real caller
    // could not be.
    svc.leader().expect("this node just became the leader");
    svc
}

/// A `KvService` on top of a running state machine, plus the handles
/// a test needs to watch it.
fn kv_service(log: Arc<RaftLog>, node_id: &str) -> (KvService, watch::Receiver<RoleHandle>) {
    let raft::NodeHandles {
        common, role_rx, ..
    } = raft::Common::new(
        node_id.to_string(),
        Arc::clone(&log),
        CancellationToken::new(),
    )
    .unwrap();
    let client_handle = common.client_handle();
    tokio::spawn(raft::run(common));
    let identity = ClusterMember {
        server_id: node_id.to_string(),
        address: format!("http://127.0.0.1:1/{node_id}"),
        client_address: format!("http://127.0.0.1:2/{node_id}"),
        voting: true,
    };
    (
        KvService::new(Store::new(log), identity, client_handle),
        role_rx,
    )
}

/// A `KvService` for `node_id`, served on a real listener at an ephemeral
/// port -- for tests where another node's `add_server` has to reach it
/// for real (it calls `GetConfig` on whatever `client_address` it's
/// given). The peer address stays a fake, unreachable one, same as
/// `kv_service` -- nothing here needs this node reachable over the
/// *peer* API, only the client one. Torn down by cancelling the
/// returned token.
async fn spawn_reachable_kv_service(
    log: Arc<RaftLog>,
    node_id: &str,
) -> (std::net::SocketAddr, CancellationToken) {
    let raft::NodeHandles { common, .. } = raft::Common::new(
        node_id.to_string(),
        Arc::clone(&log),
        CancellationToken::new(),
    )
    .unwrap();
    let client_handle = common.client_handle();
    tokio::spawn(raft::run(common));

    let addr = free_local_addr();
    let identity = ClusterMember {
        server_id: node_id.to_string(),
        address: format!("http://127.0.0.1:1/{node_id}"),
        client_address: format!("http://{addr}"),
        voting: true,
    };
    let svc = KvService::new(Store::new(log), identity, client_handle);

    let cancel = CancellationToken::new();
    let shutdown = cancel.clone();
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(crate::pb::raft5::client::kv_server::KvServer::new(svc))
            .serve_with_shutdown(addr, shutdown.cancelled())
            .await;
    });

    (addr, cancel)
}

fn free_local_addr() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().expect("local_addr")
}

#[tokio::test]
async fn write_then_read_round_trips_through_kv_service() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;

    let seq = write(&svc, b"hello", Some(b"world")).await;
    assert_eq!(
        seq, 3,
        "seq 1 is the bootstrap config entry and seq 2 the leader's no-op"
    );

    let read_resp = svc
        .read(Request::new(ReadRequest {
            key: b"hello".to_vec(),
            transaction_id: None,
        }))
        .await
        .unwrap()
        .into_inner();
    match read_resp.result {
        Some(read_response::Result::Ok(ok)) => {
            assert_eq!(ok.seq, seq);
            assert_eq!(ok.value, Some(b"world".to_vec()));
        }
        other => panic!("expected Ok, got {other:?}"),
    }

    cleanup(&path);
}

#[tokio::test]
async fn write_with_no_value_deletes_and_read_reflects_it() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;

    write(&svc, b"k", Some(b"v")).await;
    write(&svc, b"k", None).await;

    assert_eq!(read(&svc, b"k").await.value, None);

    cleanup(&path);
}

#[tokio::test]
async fn read_of_never_written_key_returns_no_value() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;

    let read_resp = svc
        .read(Request::new(ReadRequest {
            key: b"missing".to_vec(),
            transaction_id: None,
        }))
        .await
        .unwrap()
        .into_inner();
    match read_resp.result {
        Some(read_response::Result::Ok(ok)) => {
            assert_eq!(ok.seq, 0);
            assert_eq!(ok.value, None);
        }
        other => panic!("expected Ok, got {other:?}"),
    }

    cleanup(&path);
}

/// A node that isn't the leader answers with a structured
/// `NotLeader` rather than forwarding (Raft §6.2), so the client
/// retries against the leader directly and exactly one node does the
/// work.
#[tokio::test]
async fn writes_to_a_non_leader_come_back_as_not_leader() {
    let (log, path) = open_temp();
    let (svc, _role_rx) = kv_service(log, "bystander");

    // A write is a transaction, so a bystander refuses at the first
    // step -- opening one -- rather than at the write itself.
    let create = svc
        .create_transaction(Request::new(CreateTransactionRequest { ttl_ms: 1000 }))
        .await
        .unwrap()
        .into_inner()
        .result;
    match create {
        Some(create_transaction_response::Result::Error(e)) => {
            assert!(matches!(e.kind, Some(raft_error::Kind::NotLeader(_))));
        }
        other => panic!("expected NotLeader, got {other:?}"),
    }

    // And so does a write naming a transaction it has never heard of,
    // rather than reporting it as unknown -- which node you are asking
    // is settled before which transaction you mean.
    match buffer(&svc, &[0u8; 16], b"k", Some(b"v")).await {
        write_response::Result::Error(e) => {
            assert!(matches!(e.kind, Some(raft_error::Kind::NotLeader(_))));
        }
        other => panic!("expected NotLeader, got {other:?}"),
    }

    // Membership changes take the same route. The leader check happens
    // before `add_server` ever dials `client_address`, so a bystander
    // answers `NotLeader` without needing it to be reachable.
    let add = svc
        .add_server(Request::new(AddServerRequest {
            client_address: "http://127.0.0.1:9998".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        add.result,
        Some(config_change_response::Result::Error(_))
    ));

    cleanup(&path);
}

/// A read outside a transaction is served by whoever receives it,
/// leader or not -- that is the whole of what it promises. A read
/// inside one is refused by the same node, since it has neither the
/// transaction nor a lock table to protect it.
#[tokio::test]
async fn a_stale_read_is_served_without_a_leader() {
    let (log, path) = open_temp();
    let (svc, _role_rx) = kv_service(log, "bystander");

    let resp = svc
        .read(Request::new(ReadRequest {
            key: b"k".to_vec(),
            transaction_id: None,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(resp.result, Some(read_response::Result::Ok(_))));

    let strict = svc
        .read(Request::new(ReadRequest {
            key: b"k".to_vec(),
            transaction_id: Some(vec![0u8; 16]),
        }))
        .await
        .unwrap()
        .into_inner();
    match strict.result {
        Some(read_response::Result::Error(e)) => {
            assert!(matches!(e.kind, Some(raft_error::Kind::NotLeader(_))));
        }
        other => panic!("expected NotLeader, got {other:?}"),
    }

    cleanup(&path);
}

/// How a client finds the leader: `GetConfig` says so, and only a node
/// that is leading *now* -- caught up on what it inherited and still
/// holding a quorum of leases -- says yes.
///
/// Note the window this polls through. Winning the election is not
/// enough: the answer stays false until this leader's own no-op has
/// been *applied*, because until then its state doesn't yet reflect
/// everything a previous leader committed. `get_config` reports that
/// rather than waiting for it, so a client scanning for the leader is
/// never held up by a node that isn't ready -- it just looks again.
#[tokio::test]
async fn get_config_reports_whether_this_node_is_leading() {
    async fn is_leader(svc: &KvService) -> bool {
        svc.get_config(Request::new(GetConfigRequest {}))
            .await
            .unwrap()
            .into_inner()
            .is_leader
    }

    let (log, path) = open_temp();
    let (bystander, _role_rx) = kv_service(Arc::clone(&log), "bystander");
    assert!(
        !is_leader(&bystander).await,
        "a node that has never led is not the leader"
    );
    cleanup(&path);

    let (log, path) = open_temp();
    let leader = leader_kv_service(log, "solo").await;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !is_leader(&leader).await {
        assert!(
            std::time::Instant::now() < deadline,
            "a bootstrapped single node never reported itself as leading"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    cleanup(&path);
}

/// The whole add path through the client API: one call, and the new
/// server is a member of the configuration in the leader's log.
#[tokio::test]
async fn add_server_then_remove_server_round_trip_through_the_kv_service() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(Arc::clone(&log), "solo").await;

    // A real, reachable "new" node -- `add_server` now calls `GetConfig`
    // on it to learn its identity, so it has to actually answer.
    let (new_log, new_path) = open_temp();
    let (new_addr, new_cancel) = spawn_reachable_kv_service(new_log, "new").await;

    let resp = svc
        .add_server(Request::new(AddServerRequest {
            client_address: format!("http://{new_addr}"),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        resp.result,
        Some(config_change_response::Result::Ok(true))
    ));

    let config = ClusterConfig::from_bytes(&log.current_config().unwrap().unwrap()).unwrap();
    assert!(config.contains("new"));
    assert!(!config.is_voter("new"), "added as a learner");

    // "new"'s peer address is unreachable, so it never catches up and
    // stays a learner -- which means the cluster still commits on "solo"
    // alone, and can therefore also remove it again.
    let removed = svc
        .remove_server(Request::new(RemoveServerRequest {
            server_id: "new".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        removed.result,
        Some(config_change_response::Result::Ok(true))
    ));
    let config = ClusterConfig::from_bytes(&log.current_config().unwrap().unwrap()).unwrap();
    assert!(!config.contains("new"));

    new_cancel.cancel();
    cleanup(&path);
    cleanup(&new_path);
}

/// The operator-facing half of bootstrapping: the node names *itself*
/// in the configuration it forms, so a client doesn't have to know
/// (or be able to lie about) what address this node advertises.
#[tokio::test]
async fn bootstrap_forms_a_cluster_of_this_node_alone() {
    let (log, path) = open_temp();
    let (svc, mut role_rx) = kv_service(Arc::clone(&log), "solo");

    let resp = svc
        .bootstrap(Request::new(BootstrapRequest::default()))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        resp.result,
        Some(config_change_response::Result::Ok(true))
    ));

    let config = ClusterConfig::from_bytes(&log.current_config().unwrap().unwrap()).unwrap();
    assert_eq!(config.members.len(), 1);
    assert_eq!(config.members[0].server_id, "solo");
    assert!(config.members[0].voting);

    // A single-node cluster has no one to lose an election to.
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !matches!(&*role_rx.borrow(), RoleHandle::Leader(_)) {
            role_rx.changed().await.unwrap();
        }
    })
    .await
    .expect("a bootstrapped single-node cluster should elect itself");

    cleanup(&path);
}

/// `GetConfig` reflects membership before it commits (Raft §4.1 --
/// activation on append), and reports an empty cluster rather than an
/// error for a node that hasn't been bootstrapped yet.
#[tokio::test]
async fn get_config_reports_empty_before_bootstrap_and_the_new_member_right_after_add() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(Arc::clone(&log), "solo").await;

    let members = svc
        .get_config(Request::new(GetConfigRequest {}))
        .await
        .unwrap()
        .into_inner()
        .members;
    assert_eq!(
        members
            .iter()
            .map(|m| m.server_id.as_str())
            .collect::<Vec<_>>(),
        vec!["solo"]
    );

    let (new_log, new_path) = open_temp();
    let (new_addr, new_cancel) = spawn_reachable_kv_service(new_log, "new").await;

    svc.add_server(Request::new(AddServerRequest {
        client_address: format!("http://{new_addr}"),
    }))
    .await
    .unwrap();

    let members = svc
        .get_config(Request::new(GetConfigRequest {}))
        .await
        .unwrap()
        .into_inner()
        .members;
    let new_member = members
        .iter()
        .find(|m| m.server_id == "new")
        .expect("new member should be visible");
    assert!(!new_member.voting, "added as a learner");

    new_cancel.cancel();
    cleanup(&path);
    cleanup(&new_path);
}

/// Bootstrapping is irreversible, so a client that names the wrong
/// node is told so rather than silently initializing the one it
/// happened to reach.
#[tokio::test]
async fn bootstrap_refuses_when_the_named_node_is_someone_else() {
    let (log, path) = open_temp();
    let (svc, _role_rx) = kv_service(Arc::clone(&log), "solo");

    let err = svc
        .bootstrap(Request::new(BootstrapRequest {
            server_id: "some-other-node".to_string(),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(log.current_config().unwrap().is_none());

    cleanup(&path);
}

/// End-to-end through the actual tonic service: confirms
/// `RaftPeerService` is wired to a *running* state machine, not just
/// that `raft::RaftPeerHandle` works in isolation (covered separately
/// in `raft`'s own unit tests).
///
/// Over a real socket, because `AppendEntries` is a streaming RPC and
/// `tonic::Streaming` has no public constructor -- the only way to hand
/// the service one is to have tonic build it from an actual connection.
/// That also makes this the one test that exercises the chunking and
/// reassembly together, end to end; `reassemble_append_entries`'s own
/// rules are tested directly below.
#[tokio::test]
async fn append_entries_and_request_vote_reach_the_running_state_machine() {
    use crate::pb::raft5::raft::raft_peer_client::RaftPeerClient;
    use crate::pb::raft5::raft::raft_peer_server::RaftPeerServer;

    let (log, path) = open_temp();
    let raft::NodeHandles {
        common,
        role_rx,
        peer_handle,
        ..
    } = raft::Common::new(
        "node-under-test".to_string(),
        Arc::clone(&log),
        CancellationToken::new(),
    )
    .unwrap();
    tokio::spawn(raft::run(common));

    let addr = crate::raft::test_support::free_local_addr();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(RaftPeerServer::new(RaftPeerService::new(
                peer_handle.clone(),
            )))
            .serve(addr),
    );
    let svc = RaftPeerService::new(peer_handle);

    let mut client = loop {
        match RaftPeerClient::connect(format!("http://{addr}")).await {
            Ok(client) => break client,
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(10)).await,
        }
    };

    let append_resp = client
        .append_entries(crate::raft::append_entries_stream(AppendEntriesRequest {
            term: 7,
            leader_id: "leader-1".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
            compact_seq: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(append_resp.term, 7);
    assert!(append_resp.success);

    // The AppendEntries above just renewed this follower's lease on
    // leader-1, so it'll refuse any vote request until that lease
    // naturally expires -- and since this node's own candidacy timer
    // shares that exact deadline, it may fire first instead (see
    // raft::follower's `wait_for_vote_grant_or_role_change` for the
    // full reasoning). Retry until either happens.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    let vote_resp = loop {
        if !matches!(&*role_rx.borrow(), raft::RoleHandle::Follower(_)) {
            break None; // moved on to campaigning -- also proves the point
        }
        let resp = svc
            .request_vote(Request::new(RequestVoteRequest {
                term: 7,
                candidate_id: "candidate-1".to_string(),
                last_log_index: 0,
                last_log_term: 0,
                dry_run: false,
            }))
            .await
            .unwrap()
            .into_inner();
        if resp.vote_granted {
            break Some(resp);
        }
        assert!(
            std::time::Instant::now() < deadline,
            "neither granted the vote nor moved on to campaigning within the deadline"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    };

    if let Some(vote_resp) = vote_resp {
        assert!(vote_resp.vote_granted);
        assert_eq!(vote_resp.term, 7);
    }

    cleanup(&path);
}

// --- transactions -----------------------------------------------------

async fn begin_txn(svc: &KvService, ttl_ms: u64) -> Vec<u8> {
    match svc
        .create_transaction(Request::new(CreateTransactionRequest { ttl_ms }))
        .await
        .unwrap()
        .into_inner()
        .result
    {
        Some(create_transaction_response::Result::TransactionId(id)) => id,
        other => panic!("expected a transaction id, got {other:?}"),
    }
}

/// A transaction id is fixed-width, so anything else is a malformed
/// request rather than a transaction that happens not to exist -- worth
/// distinguishing, since "no such transaction" is an ordinary answer a
/// client is expected to handle and this is a bug in its caller.
#[tokio::test]
async fn a_malformed_transaction_id_is_an_invalid_argument() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;

    let status = svc
        .commit_transaction(Request::new(CommitTransactionRequest {
            transaction_id: b"too short".to_vec(),
        }))
        .await
        .expect_err("a malformed id should be rejected");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);

    cleanup(&path);
}

/// Asking for a lifetime the server will not honour is refused rather
/// than quietly shortened: a client told "yes" would go on to act as
/// though it had the lease it asked for.
#[tokio::test]
async fn an_over_long_transaction_is_refused_rather_than_shortened() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;

    let too_long = crate::txn::MAX_TRANSACTION_TTL.as_millis() as u64 + 1;
    let status = svc
        .create_transaction(Request::new(CreateTransactionRequest { ttl_ms: too_long }))
        .await
        .expect_err("an over-long lifetime should be refused");
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);

    cleanup(&path);
}

/// Dropping is idempotent and content with an id it has never heard of:
/// committing and then dropping is the ordinary client path, and a drop
/// shouldn't have to know whether it raced its own commit.
#[tokio::test]
async fn dropping_a_transaction_twice_is_not_an_error() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;
    let txn = begin_txn(&svc, 1000).await;

    for _ in 0..2 {
        svc.drop_transaction(Request::new(DropTransactionRequest {
            transaction_id: txn.clone(),
        }))
        .await
        .expect("dropping should always be allowed");
    }
    svc.drop_transaction(Request::new(DropTransactionRequest {
        transaction_id: vec![0u8; 16],
    }))
    .await
    .expect("an unknown id is not an error either");

    cleanup(&path);
}

/// A transaction that was never opened -- or was dropped, or committed
/// already -- is `UNKNOWN` rather than a silent success.
#[tokio::test]
async fn committing_an_unknown_transaction_says_so() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;

    let result = svc
        .commit_transaction(Request::new(CommitTransactionRequest {
            transaction_id: vec![7u8; 16],
        }))
        .await
        .unwrap()
        .into_inner()
        .result;
    match result {
        Some(commit_transaction_response::Result::Error(e)) => match e.kind {
            Some(raft_error::Kind::Aborted(a)) => assert_eq!(
                a.reason,
                transaction_aborted::Reason::Unknown as i32,
                "an id nobody knows should say exactly that"
            ),
            other => panic!("expected an abort, got {other:?}"),
        },
        other => panic!("expected an error, got {other:?}"),
    }

    cleanup(&path);
}

/// The whole path through the service: read, buffer, commit, and every
/// key visible at the one sequence number the entry landed at.
#[tokio::test]
async fn a_committed_transaction_makes_every_key_visible_at_one_seq() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;
    write(&svc, b"a", Some(b"1")).await;

    let txn = begin_txn(&svc, 2000).await;
    let read_back = svc
        .read(Request::new(ReadRequest {
            key: b"a".to_vec(),
            transaction_id: Some(txn.clone()),
        }))
        .await
        .unwrap()
        .into_inner()
        .result;
    assert!(matches!(
        read_back,
        Some(read_response::Result::Ok(ReadOk { value: Some(v), .. })) if v == b"1"
    ));

    for (key, value) in [(&b"a"[..], &b"2"[..]), (b"b", b"3")] {
        assert!(matches!(
            buffer(&svc, &txn, key, Some(value)).await,
            write_response::Result::Ok(true)
        ));
    }

    let seq = commit(&svc, txn).await;

    let a = read(&svc, b"a").await;
    let b = read(&svc, b"b").await;
    assert_eq!((a.value.as_deref(), b.value.as_deref()), (Some(&b"2"[..]), Some(&b"3"[..])));
    assert_eq!((a.seq, b.seq), (seq, seq), "one entry, so one seq for both keys");

    cleanup(&path);
}

// --- append_entries stream reassembly ---------------------------------

use crate::pb::raft5::raft::{AppendEntriesChunk, AppendEntriesHeader, append_entries_chunk};

fn header_chunk(term: u64, entries_bytes: u64) -> AppendEntriesChunk {
    AppendEntriesChunk {
        part: Some(append_entries_chunk::Part::Header(AppendEntriesHeader {
            term,
            leader_id: "leader-1".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            leader_commit: 0,
            compact_seq: 0,
            entries_bytes,
        })),
    }
}

fn fragment(bytes: Vec<u8>) -> AppendEntriesChunk {
    AppendEntriesChunk {
        part: Some(append_entries_chunk::Part::Entries(bytes.into())),
    }
}

async fn reassemble(
    chunks: Vec<AppendEntriesChunk>,
    current_term: u64,
) -> Result<Reassembled, Status> {
    reassemble_append_entries(tokio_stream::iter(chunks.into_iter().map(Ok)), current_term).await
}

/// The round trip the wire depends on: whatever the leader chunked has
/// to come back as the request it chunked, mutations and all.
#[tokio::test]
async fn a_chunked_request_reassembles_into_exactly_what_was_sent() {
    let original = AppendEntriesRequest {
        term: 4,
        leader_id: "leader-1".to_string(),
        prev_log_index: 9,
        prev_log_term: 3,
        entries: vec![crate::raft::test_support::write_entry(
            10,
            4,
            vec![
                (b"a".to_vec(), Some(b"1".to_vec())),
                (b"b".to_vec(), None),
            ],
        )],
        leader_commit: 7,
        compact_seq: 2,
    };

    let chunks: Vec<_> = tokio_stream::StreamExt::collect::<Vec<_>>(
        crate::raft::append_entries_stream(original.clone()),
    )
    .await;

    match reassemble(chunks, 4).await.unwrap() {
        Reassembled::Request(req) => assert_eq!(*req, original),
        Reassembled::Refused(why) => panic!("should have reassembled, got {why:?}"),
    }
}

/// A batch large enough to span several chunks still arrives whole --
/// the case the whole change exists for.
#[tokio::test]
async fn a_batch_spanning_many_chunks_reassembles_whole() {
    let value = vec![7u8; 3 * 1024 * 1024];
    let original = AppendEntriesRequest {
        term: 1,
        leader_id: "leader-1".to_string(),
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![crate::raft::test_support::write_entry(
            1,
            1,
            vec![(b"big".to_vec(), Some(value.clone()))],
        )],
        leader_commit: 0,
        compact_seq: 0,
    };

    let chunks: Vec<_> = tokio_stream::StreamExt::collect::<Vec<_>>(
        crate::raft::append_entries_stream(original.clone()),
    )
    .await;
    assert!(
        chunks.len() > 3,
        "a 3MiB batch should have taken several chunks, got {}",
        chunks.len()
    );

    match reassemble(chunks, 1).await.unwrap() {
        Reassembled::Request(req) => {
            assert_eq!(req.entries[0].writes[0].value.as_deref(), Some(&value[..]));
        }
        Reassembled::Refused(why) => panic!("should have reassembled, got {why:?}"),
    }
}

/// The header carries the term, so it has to arrive before anything can
/// be decided about the body. A stream that opens with a fragment is
/// malformed, not merely unwelcome.
#[tokio::test]
async fn a_stream_must_open_with_a_header() {
    let status = reassemble(vec![fragment(vec![1, 2, 3])], 1)
        .await
        .expect_err("a body-first stream is malformed");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);

    let status = reassemble(Vec::new(), 1)
        .await
        .expect_err("an empty stream is malformed");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

/// A leader behind this node's term is refused without its body being
/// read -- but it is *refused*, with a normal response carrying the
/// term, not errored. An error would reach the leader as a transport
/// failure, and it would retry forever instead of stepping down.
#[tokio::test]
async fn a_stale_leaders_stream_is_refused_rather_than_errored() {
    let chunks = vec![header_chunk(3, 4), fragment(vec![0; 4])];
    match reassemble(chunks, 9).await.unwrap() {
        Reassembled::Refused(_) => {}
        Reassembled::Request(_) => panic!("a stale term must not be accepted"),
    }
}

/// The declared size is the sender's claim, and believing it enough to
/// size a buffer would make one header an out-of-memory button. Refused
/// on the claim alone, without allocating against it.
#[tokio::test]
async fn an_absurd_declared_size_is_refused_without_allocating_it() {
    let chunks = vec![header_chunk(1, u64::MAX)];
    match reassemble(chunks, 1).await.unwrap() {
        Reassembled::Refused(_) => {}
        Reassembled::Request(_) => panic!("an over-limit declaration must be refused"),
    }
}

/// ...and a sender that under-declares and then keeps sending is caught
/// on what actually arrives, which is the number that costs memory.
#[tokio::test]
async fn a_body_over_the_limit_is_refused_however_it_was_declared() {
    let mut chunks = vec![header_chunk(1, 1)];
    let chunk_bytes = vec![0u8; 1024 * 1024];
    for _ in 0..(MAX_APPEND_ENTRIES_BYTES / chunk_bytes.len() + 2) {
        chunks.push(fragment(chunk_bytes.clone()));
    }
    match reassemble(chunks, 1).await.unwrap() {
        Reassembled::Refused(_) => {}
        Reassembled::Request(_) => panic!("an over-limit body must be refused"),
    }
}

/// A body that disagrees with its header is a broken sender rather than
/// a policy refusal, so it is an error: there is nothing sensible to
/// answer, and retrying the same bytes cannot help.
#[tokio::test]
async fn a_body_that_disagrees_with_its_header_is_rejected() {
    let chunks = vec![header_chunk(1, 100), fragment(vec![0; 4])];
    let status = reassemble(chunks, 1)
        .await
        .expect_err("a short body should be rejected");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

/// Anything after the header must be a fragment. A second header would
/// mean two requests sharing one stream, which this protocol does not
/// have a meaning for.
#[tokio::test]
async fn a_second_header_mid_stream_is_rejected() {
    let chunks = vec![header_chunk(1, 0), header_chunk(1, 0)];
    let status = reassemble(chunks, 1)
        .await
        .expect_err("two headers on one stream is malformed");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}
