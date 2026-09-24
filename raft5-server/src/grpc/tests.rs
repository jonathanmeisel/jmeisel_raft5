use std::sync::Arc;

use super::*;
use crate::pb::raft5::client::{
    CommitTransactionRequest, CreateInviteRequest, CreateTransactionRequest, DropTransactionRequest,
    GetKeysRequest, ListInvitesRequest, RedeemInviteRequest, RevokeInviteRequest, auth_error,
    commit_transaction_response, config_change_response, create_invite_response,
    create_transaction_response, create_user_response, delete_user_response, get_keys_response,
    list_invites_response, login_response, raft_error, read_response, redeem_invite_response,
    revoke_invite_response, transaction_aborted, write_response,
};
use crate::auth::{AuthConfig, AuthContext, InitialAdmin};
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
            consistency: None,
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
    leader_kv_service_with_auth(log, node_id, None).await
}

/// `leader_kv_service`, for a node that keeps accounts.
async fn leader_kv_service_with_auth(
    log: Arc<RaftLog>,
    node_id: &str,
    auth: Option<AuthConfig>,
) -> KvService {
    let (svc, mut role_rx) = kv_service_with_auth(log, node_id, auth);
    // On a node that keeps accounts, bootstrapping is done by the
    // administrator it was started with -- exactly the identity the auth
    // layer would put here from that operator's token. Bootstrap then
    // writes that administrator into the log, so the cluster comes up
    // with one account already in it.
    let mut request = Request::new(BootstrapRequest::default());
    if let Some(admin) = svc
        .auth
        .as_ref()
        .and_then(|auth| auth.initial_admin.as_ref())
    {
        request.extensions_mut().insert(AuthContext {
            username: admin.username.clone(),
        });
    }
    svc.bootstrap(request).await.unwrap();
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
    kv_service_with_auth(log, node_id, None)
}

/// `kv_service`, for a node that keeps accounts.
fn kv_service_with_auth(
    log: Arc<RaftLog>,
    node_id: &str,
    auth: Option<AuthConfig>,
) -> (KvService, watch::Receiver<RoleHandle>) {
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
        KvService::new(Store::new(log), identity, client_handle, auth),
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
    let svc = KvService::new(Store::new(log), identity, client_handle, None);

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
            consistency: None,
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
            consistency: None,
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
            consistency: None,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(resp.result, Some(read_response::Result::Ok(_))));

    let strict = svc
        .read(Request::new(ReadRequest {
            key: b"k".to_vec(),
            consistency: Some(read_request::Consistency::TransactionId(vec![0u8; 16])),
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
            consistency: Some(read_request::Consistency::TransactionId(txn.clone())),
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

// --- key scans --------------------------------------------------------

/// A scan, the way a client makes one.
async fn get_keys(
    svc: &KvService,
    start: &[u8],
    max_keys: u32,
    txn: Option<&[u8]>,
) -> (Vec<Vec<u8>>, bool) {
    let result = svc
        .get_keys(Request::new(GetKeysRequest {
            start_key: start.to_vec(),
            max_keys,
            consistency: txn.map(|t| get_keys_request::Consistency::TransactionId(t.to_vec())),
        }))
        .await
        .unwrap()
        .into_inner()
        .result;
    match result {
        Some(get_keys_response::Result::Ok(ok)) => (ok.keys, ok.has_more),
        other => panic!("expected a successful scan, got {other:?}"),
    }
}

fn as_strings(keys: &[Vec<u8>]) -> Vec<String> {
    keys.iter()
        .map(|k| String::from_utf8_lossy(k).into_owned())
        .collect()
}

#[tokio::test]
async fn a_scan_walks_keys_in_order_from_where_it_is_told() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "scan").await;
    for key in [&b"b"[..], b"d", b"a", b"c"] {
        write(&svc, key, Some(b"v")).await;
    }

    let (keys, has_more) = get_keys(&svc, b"", 10, None).await;
    assert_eq!(as_strings(&keys), ["a", "b", "c", "d"]);
    assert!(!has_more, "four keys asked of a store holding four is the end");

    let (keys, _) = get_keys(&svc, b"c", 10, None).await;
    assert_eq!(
        as_strings(&keys),
        ["c", "d"],
        "start_key is inclusive, and nothing before it comes back"
    );

    cleanup(&path);
}

/// `has_more` is what says whether to ask again, and appending a zero
/// byte to the last key is how the next batch starts just past it --
/// the paging contract `raft5_client::KeyIterator` is built on.
#[tokio::test]
async fn a_scan_pages_through_every_key_without_skipping_or_repeating() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "scan").await;
    for i in 0..7 {
        write(&svc, format!("k{i}").as_bytes(), Some(b"v")).await;
    }

    let mut seen = Vec::new();
    let mut start = Vec::new();
    loop {
        let (keys, has_more) = get_keys(&svc, &start, 2, None).await;
        assert!(keys.len() <= 2, "a batch never exceeds max_keys");
        let last = keys.last().cloned();
        seen.extend(keys);
        match (has_more, last) {
            (true, Some(last)) => {
                start = last;
                start.push(0);
            }
            _ => break,
        }
    }

    assert_eq!(
        as_strings(&seen),
        ["k0", "k1", "k2", "k3", "k4", "k5", "k6"],
        "paging must cover the keyspace exactly once"
    );

    cleanup(&path);
}

/// Zero means the server's default rather than zero keys -- a proto3
/// scalar cannot tell "unset" from "0", so the default has to be what
/// the absent value means.
#[tokio::test]
async fn a_scan_with_no_max_uses_the_default() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "scan").await;
    for i in 0..15 {
        write(&svc, format!("k{i:02}").as_bytes(), Some(b"v")).await;
    }

    let (keys, has_more) = get_keys(&svc, b"", 0, None).await;
    assert_eq!(keys.len(), DEFAULT_MAX_KEYS as usize);
    assert!(has_more);

    cleanup(&path);
}

/// Refused rather than quietly shortened, for the same reason an
/// over-long transaction lifetime is: a caller paging in batches of N
/// should not be told yes and handed something else.
#[tokio::test]
async fn a_scan_of_absurdly_many_keys_is_refused_rather_than_shortened() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "scan").await;

    let status = svc
        .get_keys(Request::new(GetKeysRequest {
            start_key: Vec::new(),
            max_keys: MAX_KEYS_PER_SCAN + 1,
            consistency: None,
        }))
        .await
        .expect_err("an over-large batch should be refused");
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);

    cleanup(&path);
}

/// A deleted key is simply not there -- the same collapsing of "deleted"
/// and "never written" that a read does.
#[tokio::test]
async fn a_scan_does_not_return_a_deleted_key() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "scan").await;
    write(&svc, b"a", Some(b"v")).await;
    write(&svc, b"b", Some(b"v")).await;
    write(&svc, b"b", None).await;

    let (keys, _) = get_keys(&svc, b"", 10, None).await;
    assert_eq!(as_strings(&keys), ["a"]);

    cleanup(&path);
}

/// A transaction sees its own uncommitted changes, the same way a read
/// does: a key it has written is there even though the store has never
/// heard of it, and one it has deleted is gone even though the store
/// still has it.
#[tokio::test]
async fn a_transactional_scan_reflects_what_the_transaction_buffered() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "scan").await;
    write(&svc, b"a", Some(b"1")).await;
    write(&svc, b"c", Some(b"3")).await;

    let txn = begin_txn(&svc, 5000).await;
    buffer(&svc, &txn, b"b", Some(b"2")).await;
    buffer(&svc, &txn, b"c", None).await;

    let (keys, _) = get_keys(&svc, b"", 10, Some(&txn)).await;
    assert_eq!(
        as_strings(&keys),
        ["a", "b"],
        "the buffered write appears and the buffered delete does not"
    );

    // And nobody outside it sees any of that until it commits.
    let (keys, _) = get_keys(&svc, b"", 10, None).await;
    assert_eq!(as_strings(&keys), ["a", "c"]);

    cleanup(&path);
}

/// A buffered delete shortens the batch it falls in without that
/// meaning the walk is over -- which is exactly why `has_more` is
/// reported rather than inferred from `keys.len() < max_keys`.
#[tokio::test]
async fn a_batch_shortened_by_a_buffered_delete_still_says_there_is_more() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "scan").await;
    for key in [&b"a"[..], b"b", b"c", b"d"] {
        write(&svc, key, Some(b"v")).await;
    }

    let txn = begin_txn(&svc, 5000).await;
    buffer(&svc, &txn, b"a", None).await;

    let (keys, has_more) = get_keys(&svc, b"", 2, Some(&txn)).await;
    assert_eq!(as_strings(&keys), ["b", "c"]);
    assert!(has_more, "d is still out there");

    cleanup(&path);
}

/// The point of doing this inside a transaction: every key handed back
/// is locked, so nobody else can change it for as long as the
/// transaction lives.
#[tokio::test]
async fn a_transactional_scan_locks_every_key_it_returns() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "scan").await;
    write(&svc, b"a", Some(b"1")).await;
    write(&svc, b"b", Some(b"2")).await;

    let scanner = begin_txn(&svc, 5000).await;
    let (keys, _) = get_keys(&svc, b"", 10, Some(&scanner)).await;
    assert_eq!(as_strings(&keys), ["a", "b"]);

    // Younger, so it cannot wound its way past the read locks -- it is
    // refused, naming the key it wanted.
    let younger = begin_txn(&svc, 5000).await;
    match buffer(&svc, &younger, b"b", Some(b"mine")).await {
        write_response::Result::Error(PbRaftError {
            kind: Some(raft_error::Kind::Aborted(a)),
        }) => {
            assert_eq!(a.reason, transaction_aborted::Reason::LockConflict as i32);
            assert_eq!(a.key.as_deref(), Some(&b"b"[..]));
        }
        other => panic!("expected a lock conflict over a scanned key, got {other:?}"),
    }

    cleanup(&path);
}

/// A scan outside a transaction is served by whoever receives it, with
/// no leader check -- the same bargain the plain read makes.
#[tokio::test]
async fn a_scan_outside_a_transaction_is_served_without_a_leader() {
    let (log, path) = open_temp();
    let (svc, _role_rx) = kv_service(log, "bystander");

    let (keys, has_more) = get_keys(&svc, b"", 10, None).await;
    assert!(keys.is_empty() && !has_more);

    // One inside a transaction is not: this node has neither the
    // transaction nor a lock table to protect it.
    let result = svc
        .get_keys(Request::new(GetKeysRequest {
            start_key: Vec::new(),
            max_keys: 10,
            consistency: Some(get_keys_request::Consistency::TransactionId(vec![0u8; 16])),
        }))
        .await
        .unwrap()
        .into_inner()
        .result;
    match result {
        Some(get_keys_response::Result::Error(e)) => {
            assert!(matches!(e.kind, Some(raft_error::Kind::NotLeader(_))));
        }
        other => panic!("expected NotLeader, got {other:?}"),
    }

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

// --- the reserved keyspace --------------------------------------------
//
// `crate::keyspace` carves a prefix out of the key space for the
// cluster's own state -- user accounts, today. These cover the four
// points where the client API has to refuse to address it. The server's
// own writes must still reach it, so each test that needs a reserved key
// present seeds one through `append_directly`, which is the path
// `CreateUser` will take.

/// Puts a key into the store the way the server's own writes do: at the
/// leader handle, below the gRPC boundary where reserved keys are
/// refused.
async fn append_directly(svc: &KvService, key: &[u8], value: &[u8]) -> u64 {
    svc.leader()
        .expect("the test service leads")
        .append_writes(vec![Mutation::new(key, Some(value))])
        .await
        .expect("a direct append should commit")
}

/// The privilege-escalation guard. A client that could buffer this write
/// could write its own account record and set `is_admin` on it.
#[tokio::test]
async fn write_refuses_a_reserved_key() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;
    let txn = begin_txn(&svc, 5000).await;

    let status = svc
        .write(Request::new(WriteRequest {
            transaction_id: txn,
            key: crate::keyspace::user_key("alice"),
            value: Some(b"forged".to_vec()),
        }))
        .await
        .expect_err("a reserved key should be refused");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);

    cleanup(&path);
}

/// Both branches: refused rather than answered as absent, because a
/// password hash handed back is one to attack offline.
#[tokio::test]
async fn read_refuses_a_reserved_key_transactional_or_not() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;
    let key = crate::keyspace::user_key("alice");
    append_directly(&svc, &key, b"hash").await;

    let txn = begin_txn(&svc, 5000).await;
    for consistency in [None, Some(read_request::Consistency::TransactionId(txn))] {
        let status = svc
            .read(Request::new(ReadRequest {
                key: key.clone(),
                consistency,
            }))
            .await
            .expect_err("a reserved key should be refused");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    cleanup(&path);
}

#[tokio::test]
async fn get_keys_refuses_a_reserved_start_key() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;

    let status = svc
        .get_keys(Request::new(GetKeysRequest {
            start_key: vec![crate::keyspace::RESERVED],
            max_keys: 10,
            consistency: None,
        }))
        .await
        .expect_err("a reserved start key should be refused");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);

    cleanup(&path);
}

/// A walk from the very beginning starts at the empty key, which sorts
/// below the whole reserved range -- so this is the scan that would
/// reveal every account record if the filter were missing.
#[tokio::test]
async fn get_keys_does_not_reveal_reserved_keys() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;

    append_directly(&svc, &crate::keyspace::user_key("alice"), b"hash").await;
    append_directly(&svc, &crate::keyspace::user_key("bob"), b"hash").await;
    write(&svc, b"visible", Some(b"v")).await;

    let (keys, has_more) = get_keys(&svc, b"", 10, None).await;
    assert_eq!(as_strings(&keys), vec!["visible"]);
    assert!(!has_more);
    assert!(
        !keys.iter().any(|key| is_reserved(key)),
        "a scan must never hand back a reserved key"
    );

    cleanup(&path);
}

/// Reserved keys are skipped inside the scan rather than filtered out of
/// its result, so they must not consume any of the caller's batch. A
/// client asking for three keys gets three, however many account records
/// happen to sort before them.
#[tokio::test]
async fn reserved_keys_do_not_shorten_a_batch_or_confuse_has_more() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;

    for name in ["alice", "bob", "carol"] {
        append_directly(&svc, &crate::keyspace::user_key(name), b"hash").await;
    }
    for key in [b"k1", b"k2", b"k3"] {
        write(&svc, key, Some(b"v")).await;
    }

    // Exactly as many keys as there are: a full batch, and nothing after
    // it.
    let (keys, has_more) = get_keys(&svc, b"", 3, None).await;
    assert_eq!(as_strings(&keys), vec!["k1", "k2", "k3"]);
    assert!(
        !has_more,
        "three keys asked for and three that exist is the end of the walk"
    );

    // One fewer than there are, so `has_more` has to notice the third --
    // past three reserved keys that don't count towards it.
    let (keys, has_more) = get_keys(&svc, b"", 2, None).await;
    assert_eq!(as_strings(&keys), vec!["k1", "k2"]);
    assert!(has_more, "there is a third key to come");

    cleanup(&path);
}

// --- accounts and tokens ----------------------------------------------
//
// The auth layer itself is not in the picture here: these drive the
// handlers directly, supplying the caller's identity the way that layer
// will. What they cover is everything underneath it -- who may do what,
// what the store ends up holding, and the one window where a node has no
// accounts to consult.

const TEST_SECRET: &[u8] = b"a secret shared by every node in this test";

fn auth_config(initial_admin: Option<InitialAdmin>) -> Option<AuthConfig> {
    Some(AuthConfig::new(
        TEST_SECRET,
        std::time::Duration::from_secs(3600),
        initial_admin,
    ))
}

async fn startup_admin(username: &str, password: &str) -> InitialAdmin {
    InitialAdmin {
        username: username.to_string(),
        password_hash: crate::auth::hash_password(password.to_string())
            .await
            .expect("hashing should work"),
    }
}

/// A node that keeps accounts, bootstrapped by `root`, who is left in
/// the store as its administrator with the password `hunter42`.
async fn authenticated_service(log: Arc<RaftLog>, node_id: &str) -> KvService {
    let admin = startup_admin("root", "hunter42").await;
    leader_kv_service_with_auth(log, node_id, auth_config(Some(admin))).await
}

/// A request carrying a verified identity, as the auth layer leaves it.
fn as_user<T>(caller: &str, message: T) -> Request<T> {
    let mut request = Request::new(message);
    request.extensions_mut().insert(AuthContext {
        username: caller.to_string(),
    });
    request
}

async fn login(svc: &KvService, username: &str, password: &str) -> login_response::Result {
    svc.login(Request::new(LoginRequest {
        username: username.to_string(),
        password: password.to_string(),
    }))
    .await
    .unwrap()
    .into_inner()
    .result
    .expect("a login always answers")
}

async fn create_user(
    svc: &KvService,
    caller: &str,
    username: &str,
    password: &str,
    is_admin: bool,
) -> create_user_response::Result {
    svc.create_user(as_user(
        caller,
        CreateUserRequest {
            username: username.to_string(),
            password: password.to_string(),
            is_admin,
        },
    ))
    .await
    .unwrap()
    .into_inner()
    .result
    .expect("create_user always answers")
}

async fn delete_user(svc: &KvService, caller: &str, username: &str) -> delete_user_response::Result {
    svc.delete_user(as_user(
        caller,
        DeleteUserRequest {
            username: username.to_string(),
        },
    ))
    .await
    .unwrap()
    .into_inner()
    .result
    .expect("delete_user always answers")
}

fn auth_kind(error: &PbAuthError) -> auth_error::Kind {
    auth_error::Kind::try_from(error.kind).expect("a kind this build knows")
}

fn login_refusal(result: login_response::Result) -> auth_error::Kind {
    match result {
        login_response::Result::AuthError(e) => auth_kind(&e),
        other => panic!("expected the login to be refused, got {other:?}"),
    }
}

fn login_token(result: login_response::Result) -> String {
    match result {
        login_response::Result::Ok(ok) => ok.token,
        other => panic!("expected the login to succeed, got {other:?}"),
    }
}

fn create_refusal(result: create_user_response::Result) -> auth_error::Kind {
    match result {
        create_user_response::Result::AuthError(e) => auth_kind(&e),
        other => panic!("expected the account not to be created, got {other:?}"),
    }
}

fn delete_refusal(result: delete_user_response::Result) -> auth_error::Kind {
    match result {
        delete_user_response::Result::AuthError(e) => auth_kind(&e),
        other => panic!("expected the account not to be removed, got {other:?}"),
    }
}

fn created_seq(result: create_user_response::Result) -> u64 {
    match result {
        create_user_response::Result::Seq(seq) => seq,
        other => panic!("expected the account to be created, got {other:?}"),
    }
}

/// With no secret there is nothing to sign a token with and no accounts
/// to consult, so all three say so rather than half-working.
#[tokio::test]
async fn the_account_rpcs_report_themselves_disabled_without_a_secret() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;

    assert_eq!(
        login_refusal(login(&svc, "alice", "hunter42").await),
        auth_error::Kind::AuthDisabled
    );
    assert_eq!(
        create_refusal(create_user(&svc, "root", "alice", "hunter42", false).await),
        auth_error::Kind::AuthDisabled
    );
    assert_eq!(
        delete_refusal(delete_user(&svc, "root", "alice").await),
        auth_error::Kind::AuthDisabled
    );

    cleanup(&path);
}

/// The window the startup administrator exists for. A node that has
/// never been bootstrapped has no accounts and no leader to read them
/// from, yet `Bootstrap` needs a token like everything else -- so the
/// operator has to be able to authenticate against something.
#[tokio::test]
async fn the_startup_admin_can_log_in_before_the_node_is_bootstrapped() {
    let (log, path) = open_temp();
    let admin = startup_admin("root", "hunter42").await;
    let (svc, _role_rx) = kv_service_with_auth(log, "kv-test", auth_config(Some(admin)));

    assert!(
        !login_token(login(&svc, "root", "hunter42").await).is_empty(),
        "an unbootstrapped node has no leader but can still sign"
    );
    assert_eq!(
        login_refusal(login(&svc, "root", "the wrong one").await),
        auth_error::Kind::BadCredentials
    );
    assert_eq!(
        login_refusal(login(&svc, "someone-else", "hunter42").await),
        auth_error::Kind::BadCredentials,
        "the startup password belongs to one username only"
    );

    cleanup(&path);
}

/// Bootstrapping is what turns the startup administrator from a flag
/// into an account, so that the cluster comes up with someone who can
/// administer it.
#[tokio::test]
async fn bootstrapping_writes_the_startup_admin_into_the_store() {
    let (log, path) = open_temp();
    let svc = authenticated_service(log, "kv-test").await;

    assert!(
        !login_token(login(&svc, "root", "hunter42").await).is_empty(),
        "root should now be a stored account"
    );
    // And a stored one: it can do what only a stored administrator can.
    created_seq(create_user(&svc, "root", "alice", "correct horse", false).await);

    cleanup(&path);
}

/// And the flag itself goes inert. It is gated on the node having a
/// configuration rather than on the account being absent, so removing
/// the administrator cannot quietly re-arm the password this node was
/// started with.
///
/// The other face of that: this is also how a cluster can be locked out
/// of itself. Deleting the last administrator is unrecoverable.
#[tokio::test]
async fn the_startup_password_does_not_come_back_when_the_account_is_removed() {
    let (log, path) = open_temp();
    let svc = authenticated_service(log, "kv-test").await;
    created_seq(create_user(&svc, "root", "other", "hunter42", true).await);

    delete_user(&svc, "other", "root").await;
    assert_eq!(
        login_refusal(login(&svc, "root", "hunter42").await),
        auth_error::Kind::BadCredentials,
        "the startup password must not come back"
    );

    cleanup(&path);
}

#[tokio::test]
async fn an_administrator_can_create_an_account_that_can_then_log_in() {
    let (log, path) = open_temp();
    let svc = authenticated_service(log, "kv-test").await;

    assert!(created_seq(create_user(&svc, "root", "alice", "correct horse", false).await) > 0);
    assert!(!login_token(login(&svc, "alice", "correct horse").await).is_empty());
    assert_eq!(
        login_refusal(login(&svc, "alice", "wrong").await),
        auth_error::Kind::BadCredentials
    );

    cleanup(&path);
}

/// An account that does not exist and a password that is wrong get the
/// same answer: saying which would confirm that a username is real, and
/// a username is half of a credential.
#[tokio::test]
async fn an_unknown_account_and_a_wrong_password_are_refused_alike() {
    let (log, path) = open_temp();
    let svc = authenticated_service(log, "kv-test").await;

    assert_eq!(
        login_refusal(login(&svc, "root", "wrong").await),
        login_refusal(login(&svc, "nobody-at-all", "wrong").await)
    );

    cleanup(&path);
}

#[tokio::test]
async fn creating_an_account_that_already_exists_is_refused() {
    let (log, path) = open_temp();
    let svc = authenticated_service(log, "kv-test").await;

    created_seq(create_user(&svc, "root", "alice", "one", false).await);
    assert_eq!(
        create_refusal(create_user(&svc, "root", "alice", "two", false).await),
        auth_error::Kind::AlreadyExists
    );
    assert!(
        !login_token(login(&svc, "alice", "one").await).is_empty(),
        "the refused second attempt must not have overwritten the first"
    );

    cleanup(&path);
}

#[tokio::test]
async fn a_deleted_account_can_no_longer_log_in() {
    let (log, path) = open_temp();
    let svc = authenticated_service(log, "kv-test").await;
    created_seq(create_user(&svc, "root", "alice", "correct horse", false).await);

    match delete_user(&svc, "root", "alice").await {
        delete_user_response::Result::Seq(_) => {}
        other => panic!("expected the account to be removed, got {other:?}"),
    }
    assert_eq!(
        login_refusal(login(&svc, "alice", "correct horse").await),
        auth_error::Kind::BadCredentials
    );
    assert_eq!(
        delete_refusal(delete_user(&svc, "root", "alice").await),
        auth_error::Kind::NoSuchUser
    );

    cleanup(&path);
}

/// Authenticating says who you are, not what you may do.
#[tokio::test]
async fn an_ordinary_account_may_not_manage_accounts() {
    let (log, path) = open_temp();
    let svc = authenticated_service(log, "kv-test").await;
    created_seq(create_user(&svc, "root", "alice", "correct horse", false).await);

    assert_eq!(
        create_refusal(create_user(&svc, "alice", "mallory", "letmein", true).await),
        auth_error::Kind::NotAdmin
    );
    assert_eq!(
        delete_refusal(delete_user(&svc, "alice", "root").await),
        auth_error::Kind::NotAdmin
    );

    cleanup(&path);
}

/// A token for an account that has since been deleted still verifies --
/// nothing can un-issue one -- but it buys no administrative rights,
/// because those are looked up on each call rather than asserted by the
/// token.
#[tokio::test]
async fn a_deleted_administrator_no_longer_manages_accounts() {
    let (log, path) = open_temp();
    let svc = authenticated_service(log, "kv-test").await;
    created_seq(create_user(&svc, "root", "other", "hunter42", true).await);

    delete_user(&svc, "other", "root").await;
    assert_eq!(
        create_refusal(create_user(&svc, "root", "alice", "letmein", false).await),
        auth_error::Kind::NotAdmin
    );

    cleanup(&path);
}

/// The handlers never take the caller's word for who they are: with no
/// identity put there by the auth layer, there is no caller.
#[tokio::test]
async fn managing_accounts_without_a_verified_identity_is_refused() {
    let (log, path) = open_temp();
    let svc = authenticated_service(log, "kv-test").await;

    let status = svc
        .create_user(Request::new(CreateUserRequest {
            username: "alice".to_string(),
            password: "letmein".to_string(),
            is_admin: true,
        }))
        .await
        .expect_err("a request with no verified identity should be refused");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);

    cleanup(&path);
}

/// Bootstrapping an authenticated node is the one call that cannot be
/// authorized from the store, since there is nothing in it yet -- so it
/// is authorized against the administrator the node was started with.
#[tokio::test]
async fn only_the_startup_admin_may_bootstrap_an_authenticated_node() {
    let (log, path) = open_temp();
    let admin = startup_admin("root", "hunter42").await;
    let (svc, _role_rx) = kv_service_with_auth(log, "kv-test", auth_config(Some(admin)));

    let status = svc
        .bootstrap(as_user("mallory", BootstrapRequest::default()))
        .await
        .expect_err("someone else's token should not bootstrap this node");
    assert_eq!(status.code(), tonic::Code::PermissionDenied);

    let status = svc
        .bootstrap(Request::new(BootstrapRequest::default()))
        .await
        .expect_err("an unauthenticated bootstrap should be refused");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);

    cleanup(&path);
}

/// The two halves meeting: an account is a key/value pair, and the
/// client API cannot see it.
#[tokio::test]
async fn an_account_is_invisible_through_the_client_api() {
    let (log, path) = open_temp();
    let svc = authenticated_service(log, "kv-test").await;
    created_seq(create_user(&svc, "root", "alice", "correct horse", false).await);
    write(&svc, b"ordinary", Some(b"v")).await;

    let (keys, _) = get_keys(&svc, b"", 100, None).await;
    assert_eq!(as_strings(&keys), vec!["ordinary"]);

    let status = svc
        .read(Request::new(ReadRequest {
            key: crate::keyspace::user_key("alice"),
            consistency: None,
        }))
        .await
        .expect_err("an account record is not readable");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);

    cleanup(&path);
}

/// Membership changes are the most destructive calls this service has --
/// removing the wrong server can cost a cluster its quorum -- so holding
/// a token is not enough for them either.
#[tokio::test]
async fn an_ordinary_account_may_not_change_cluster_membership() {
    let (log, path) = open_temp();
    let svc = authenticated_service(log, "kv-test").await;
    created_seq(create_user(&svc, "root", "alice", "correct horse", false).await);

    let refused = svc
        .remove_server(as_user(
            "alice",
            RemoveServerRequest {
                server_id: "kv-test".to_string(),
            },
        ))
        .await
        .unwrap()
        .into_inner()
        .result
        .expect("remove_server always answers");
    match refused {
        config_change_response::Result::AuthError(e) => {
            assert_eq!(auth_kind(&e), auth_error::Kind::NotAdmin);
        }
        other => panic!("expected NOT_ADMIN, got {other:?}"),
    }

    let refused = svc
        .add_server(as_user(
            "alice",
            AddServerRequest {
                client_address: "http://127.0.0.1:1".to_string(),
            },
        ))
        .await
        .unwrap()
        .into_inner()
        .result
        .expect("add_server always answers");
    match refused {
        config_change_response::Result::AuthError(e) => {
            assert_eq!(auth_kind(&e), auth_error::Kind::NotAdmin);
        }
        other => panic!("expected NOT_ADMIN, got {other:?}"),
    }

    cleanup(&path);
}

/// And the administrator still can -- the check turns away the wrong
/// caller, not the call.
#[tokio::test]
async fn an_administrator_may_still_change_cluster_membership() {
    let (log, path) = open_temp();
    let svc = authenticated_service(log, "kv-test").await;

    // Removing a server that isn't a member is refused on its merits,
    // which is the point: it got past the admin check to be judged.
    let answered = svc
        .remove_server(as_user(
            "root",
            RemoveServerRequest {
                server_id: "not-a-member".to_string(),
            },
        ))
        .await
        .unwrap()
        .into_inner()
        .result
        .expect("remove_server always answers");
    assert!(
        !matches!(answered, config_change_response::Result::AuthError(_)),
        "an administrator should not be turned away, got {answered:?}"
    );

    cleanup(&path);
}

// --- GetVersion -------------------------------------------------------

/// The seq `GetVersion` reports for `key`, at whatever consistency.
async fn version_at(
    svc: &KvService,
    key: &[u8],
    consistency: Option<get_version_request::Consistency>,
) -> get_version_response::Result {
    svc.get_version(Request::new(GetVersionRequest {
        key: key.to_vec(),
        consistency,
    }))
    .await
    .unwrap()
    .into_inner()
    .result
    .expect("a get_version response always carries a result")
}

/// A stale `GetVersion`, which is always answerable.
async fn version(svc: &KvService, key: &[u8]) -> u64 {
    match version_at(svc, key, None).await {
        get_version_response::Result::Ok(ok) => ok.seq,
        other => panic!("expected a successful get_version, got {other:?}"),
    }
}

#[tokio::test]
async fn get_version_reports_the_seq_a_write_landed_at() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;

    let seq = write(&svc, b"k", Some(b"v")).await;
    assert_eq!(version(&svc, b"k").await, seq);
    // And agrees with what `Read` says about the same key, which is the
    // property that makes it a substitute for one.
    assert_eq!(read(&svc, b"k").await.seq, seq);

    cleanup(&path);
}

#[tokio::test]
async fn get_version_is_zero_for_a_key_with_no_value() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;

    // Never written.
    assert_eq!(version(&svc, b"missing").await, 0);

    // And deleted, which is the same answer -- `Read` does not
    // distinguish the two either.
    write(&svc, b"k", Some(b"v")).await;
    write(&svc, b"k", None).await;
    assert_eq!(version(&svc, b"k").await, 0);

    cleanup(&path);
}

#[tokio::test]
async fn get_version_refuses_a_reserved_key_transactional_or_not() {
    // As `read` does, and for a sharper reason: a seq is small enough to
    // feel harmless, but "this account changed at seq N" is still the
    // reserved range answering questions about itself.
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;
    let key = crate::keyspace::user_key("alice");
    append_directly(&svc, &key, b"hash").await;

    let txn = begin_txn(&svc, 5000).await;
    for consistency in [
        None,
        Some(get_version_request::Consistency::TransactionId(txn)),
        Some(get_version_request::Consistency::Linearizable(true)),
    ] {
        let status = svc
            .get_version(Request::new(GetVersionRequest {
                key: key.clone(),
                consistency,
            }))
            .await
            .expect_err("a reserved key should be refused");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    cleanup(&path);
}

#[tokio::test]
async fn a_stale_get_version_is_served_without_a_leader() {
    // The mirror of `a_stale_read_is_served_without_a_leader`: the
    // default asks for no guarantee, so any node answers, while both
    // guaranteed forms need a leader and this node is not one.
    let (log, path) = open_temp();
    let (svc, _role_rx) = kv_service(log, "bystander");

    assert!(matches!(
        version_at(&svc, b"k", None).await,
        get_version_response::Result::Ok(_)
    ));

    for consistency in [
        get_version_request::Consistency::TransactionId(vec![0u8; 16]),
        get_version_request::Consistency::Linearizable(true),
    ] {
        match version_at(&svc, b"k", Some(consistency)).await {
            get_version_response::Result::Error(e) => {
                assert!(matches!(e.kind, Some(raft_error::Kind::NotLeader(_))));
            }
            other => panic!("expected NotLeader, got {other:?}"),
        }
    }

    cleanup(&path);
}

#[tokio::test]
async fn a_transactional_get_version_holds_the_key() {
    // What proves the transactional path takes the lock rather than
    // reading storage behind the lock table's back: another transaction
    // must not be able to write the key afterwards.
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;
    write(&svc, b"k", Some(b"v")).await;

    let reader = begin_txn(&svc, 5000).await;
    let seq = match version_at(
        &svc,
        b"k",
        Some(get_version_request::Consistency::TransactionId(
            reader.clone(),
        )),
    )
    .await
    {
        get_version_response::Result::Ok(ok) => ok.seq,
        other => panic!("expected a successful get_version, got {other:?}"),
    };
    assert_ne!(seq, 0);

    let writer = begin_txn(&svc, 5000).await;
    match buffer(&svc, &writer, b"k", Some(b"other")).await {
        write_response::Result::Error(e) => {
            assert!(matches!(e.kind, Some(raft_error::Kind::Aborted(_))));
        }
        other => panic!("expected a lock conflict, got {other:?}"),
    }

    cleanup(&path);
}

#[tokio::test]
async fn a_transactional_get_version_reports_committed_state_over_its_own_write() {
    // A transaction asking about a key it has itself buffered gets the
    // *committed* seq: the buffered write has no sequence number until
    // the commit assigns one. It must also not deadlock on its own write
    // lock, which is why the read side skips a key it already holds.
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;
    let committed = write(&svc, b"k", Some(b"v")).await;

    let txn = begin_txn(&svc, 5000).await;
    assert!(matches!(
        buffer(&svc, &txn, b"k", Some(b"buffered")).await,
        write_response::Result::Ok(true)
    ));

    match version_at(
        &svc,
        b"k",
        Some(get_version_request::Consistency::TransactionId(txn)),
    )
    .await
    {
        get_version_response::Result::Ok(ok) => assert_eq!(ok.seq, committed),
        other => panic!("expected a successful get_version, got {other:?}"),
    }

    cleanup(&path);
}

#[tokio::test]
async fn a_linearizable_read_and_version_agree_with_the_stale_ones_on_a_leader() {
    // On a settled single-node leader all three consistencies describe
    // the same state, so this pins that the two new paths answer at all
    // and answer the same thing -- their guarantee is about *when*, not
    // about what.
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;
    let seq = write(&svc, b"k", Some(b"v")).await;

    match version_at(
        &svc,
        b"k",
        Some(get_version_request::Consistency::Linearizable(true)),
    )
    .await
    {
        get_version_response::Result::Ok(ok) => assert_eq!(ok.seq, seq),
        other => panic!("expected a successful get_version, got {other:?}"),
    }

    let resp = svc
        .read(Request::new(ReadRequest {
            key: b"k".to_vec(),
            consistency: Some(read_request::Consistency::Linearizable(true)),
        }))
        .await
        .unwrap()
        .into_inner();
    match resp.result {
        Some(read_response::Result::Ok(ok)) => {
            assert_eq!(ok.seq, seq);
            assert_eq!(ok.value.as_deref(), Some(b"v".as_slice()));
        }
        other => panic!("expected a successful read, got {other:?}"),
    }

    cleanup(&path);
}

/// `linearizable: false` is a present field saying no guarantee was
/// asked for, not a malformed request -- it must behave exactly as an
/// unset `consistency` does, including on a node that is not a leader.
#[tokio::test]
async fn linearizable_false_is_a_stale_read() {
    let (log, path) = open_temp();
    let (svc, _role_rx) = kv_service(log, "bystander");

    assert!(matches!(
        version_at(
            &svc,
            b"k",
            Some(get_version_request::Consistency::Linearizable(false)),
        )
        .await,
        get_version_response::Result::Ok(_)
    ));

    let resp = svc
        .read(Request::new(ReadRequest {
            key: b"k".to_vec(),
            consistency: Some(read_request::Consistency::Linearizable(false)),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(resp.result, Some(read_response::Result::Ok(_))));

    cleanup(&path);
}

// --- snapshots --------------------------------------------------------

/// Create a snapshot on a node that keeps no accounts, so `require_admin`
/// is satisfied by there being nobody to be an administrator.
async fn make_snapshot(svc: &KvService, id: &[u8]) -> PbSnapshot {
    let result = svc
        .create_snapshot(Request::new(CreateSnapshotRequest {
            snapshot_id: id.to_vec(),
            created_unix_ms: 1234,
        }))
        .await
        .unwrap()
        .into_inner()
        .result
        .expect("a create_snapshot response always carries a result");
    match result {
        create_snapshot_response::Result::Ok(s) => s,
        other => panic!("expected a snapshot, got {other:?}"),
    }
}

async fn read_at(svc: &KvService, id: &[u8], key: &[u8]) -> Result<ReadOk, Status> {
    let resp = svc
        .read(Request::new(ReadRequest {
            key: key.to_vec(),
            consistency: Some(read_request::Consistency::SnapshotId(id.to_vec())),
        }))
        .await?
        .into_inner();
    match resp.result {
        Some(read_response::Result::Ok(ok)) => Ok(ok),
        other => panic!("expected a successful read, got {other:?}"),
    }
}

#[tokio::test]
async fn a_snapshot_reads_the_store_as_it_was_when_it_was_taken() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;

    let before = write(&svc, b"k", Some(b"before")).await;
    write(&svc, b"doomed", Some(b"here")).await;
    let snap = make_snapshot(&svc, b"s1").await;

    // Everything after the snapshot is invisible through it.
    write(&svc, b"k", Some(b"after")).await;
    write(&svc, b"doomed", None).await;
    write(&svc, b"fresh", Some(b"new")).await;

    let at = read_at(&svc, b"s1", b"k").await.unwrap();
    assert_eq!(at.value.as_deref(), Some(b"before".as_slice()));
    assert_eq!(at.seq, before);
    assert_eq!(
        read_at(&svc, b"s1", b"doomed")
            .await
            .unwrap()
            .value
            .as_deref(),
        Some(b"here".as_slice()),
        "a key deleted after the snapshot is still visible through it"
    );
    assert_eq!(
        read_at(&svc, b"s1", b"fresh").await.unwrap().value,
        None,
        "a key created after the snapshot is not"
    );

    // Current reads are unaffected.
    assert_eq!(
        read(&svc, b"k").await.value.as_deref(),
        Some(b"after".as_slice())
    );

    assert!(snap.seq > before);
    cleanup(&path);
}

#[tokio::test]
async fn a_snapshot_walk_sees_the_key_set_as_of_the_snapshot() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;

    write(&svc, b"a", Some(b"1")).await;
    write(&svc, b"b", Some(b"2")).await;
    make_snapshot(&svc, b"s1").await;
    write(&svc, b"b", None).await;
    write(&svc, b"c", Some(b"3")).await;

    let (keys, _) = get_keys_at(&svc, b"s1", b"", 10).await;
    assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);

    let (now, _) = get_keys(&svc, b"", 10, None).await;
    assert_eq!(now, vec![b"a".to_vec(), b"c".to_vec()]);

    cleanup(&path);
}

async fn get_keys_at(
    svc: &KvService,
    id: &[u8],
    start: &[u8],
    max_keys: u32,
) -> (Vec<Vec<u8>>, bool) {
    let result = svc
        .get_keys(Request::new(GetKeysRequest {
            start_key: start.to_vec(),
            max_keys,
            consistency: Some(get_keys_request::Consistency::SnapshotId(id.to_vec())),
        }))
        .await
        .unwrap()
        .into_inner()
        .result;
    match result {
        Some(get_keys_response::Result::Ok(ok)) => (ok.keys, ok.has_more),
        other => panic!("expected a successful scan, got {other:?}"),
    }
}

#[tokio::test]
async fn get_version_at_a_snapshot_reports_the_seq_it_was_written_at() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;

    let first = write(&svc, b"k", Some(b"1")).await;
    make_snapshot(&svc, b"s1").await;
    let second = write(&svc, b"k", Some(b"2")).await;

    let at = snapshot_version(&svc, b"s1", b"k").await;
    assert_eq!(at, first);
    assert_eq!(version(&svc, b"k").await, second);

    cleanup(&path);
}

async fn snapshot_version(svc: &KvService, id: &[u8], key: &[u8]) -> u64 {
    match snapshot_version_result(svc, id, key).await.unwrap() {
        get_version_response::Result::Ok(ok) => ok.seq,
        other => panic!("expected a successful get_version, got {other:?}"),
    }
}

async fn snapshot_version_result(
    svc: &KvService,
    id: &[u8],
    key: &[u8],
) -> Result<get_version_response::Result, Status> {
    Ok(svc
        .get_version(Request::new(GetVersionRequest {
            key: key.to_vec(),
            consistency: Some(get_version_request::Consistency::SnapshotId(id.to_vec())),
        }))
        .await?
        .into_inner()
        .result
        .expect("a get_version response always carries a result"))
}

#[tokio::test]
async fn an_unknown_snapshot_is_refused_rather_than_answered_as_empty() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;
    write(&svc, b"k", Some(b"v")).await;

    // Answering an unknown id as "no value" would be indistinguishable
    // from a snapshot taken before the key existed, which is exactly the
    // confusion an explicit refusal avoids.
    let status = read_at(&svc, b"nope", b"k").await.expect_err("unknown id");
    assert_eq!(status.code(), tonic::Code::NotFound);
    let status = snapshot_version_result(&svc, b"nope", b"k")
        .await
        .expect_err("unknown id");
    assert_eq!(status.code(), tonic::Code::NotFound);

    cleanup(&path);
}

#[tokio::test]
async fn a_removed_snapshot_stops_being_readable() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;
    write(&svc, b"k", Some(b"v")).await;
    make_snapshot(&svc, b"s1").await;
    assert!(read_at(&svc, b"s1", b"k").await.is_ok());

    let result = svc
        .remove_snapshot(Request::new(RemoveSnapshotRequest {
            snapshot_id: b"s1".to_vec(),
        }))
        .await
        .unwrap()
        .into_inner()
        .result
        .expect("a remove_snapshot response always carries a result");
    assert!(matches!(result, remove_snapshot_response::Result::Seq(_)));

    let status = read_at(&svc, b"s1", b"k").await.expect_err("removed");
    assert_eq!(status.code(), tonic::Code::NotFound);

    cleanup(&path);
}

#[tokio::test]
async fn creating_a_snapshot_twice_is_refused_rather_than_moving_it() {
    // The invariant the whole feature rests on: a snapshot's seq is the
    // seq of its own record, so a second write would silently move it
    // forward in time.
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;
    let first = make_snapshot(&svc, b"s1").await;
    write(&svc, b"k", Some(b"later")).await;

    let result = svc
        .create_snapshot(Request::new(CreateSnapshotRequest {
            snapshot_id: b"s1".to_vec(),
            created_unix_ms: 9999,
        }))
        .await
        .expect_err("a duplicate id should be refused");
    assert_eq!(result.code(), tonic::Code::AlreadyExists);

    // And it still reads where it originally did.
    let listed = list_snapshots(&svc).await;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].seq, first.seq);

    cleanup(&path);
}

async fn list_snapshots(svc: &KvService) -> Vec<PbSnapshot> {
    let result = svc
        .list_snapshots(Request::new(ListSnapshotsRequest {}))
        .await
        .unwrap()
        .into_inner()
        .result
        .expect("a list_snapshots response always carries a result");
    match result {
        list_snapshots_response::Result::Ok(list) => list.snapshots,
        other => panic!("expected a listing, got {other:?}"),
    }
}

#[tokio::test]
async fn a_malformed_snapshot_id_is_refused() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;

    for id in [
        b"".as_slice(),
        b"has/slash".as_slice(),
        b"has space".as_slice(),
    ] {
        let status = svc
            .create_snapshot(Request::new(CreateSnapshotRequest {
                snapshot_id: id.to_vec(),
                created_unix_ms: 0,
            }))
            .await
            .expect_err("a malformed id should be refused");
        assert_eq!(status.code(), tonic::Code::InvalidArgument, "id {id:?}");
    }

    // An RFC-3339-ish timestamp, which is what the client defaults to,
    // must be legal.
    make_snapshot(&svc, b"2026-09-22T14:03:11.482Z").await;

    cleanup(&path);
}

#[tokio::test]
async fn snapshot_records_are_invisible_to_a_key_walk() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;
    write(&svc, b"visible", Some(b"v")).await;
    make_snapshot(&svc, b"s1").await;

    let (keys, _) = get_keys(&svc, b"", 10, None).await;
    assert_eq!(keys, vec![b"visible".to_vec()]);

    // And the record's key is unaddressable directly.
    let status = svc
        .read(Request::new(ReadRequest {
            key: b"\0snap/s1".to_vec(),
            consistency: None,
        }))
        .await
        .expect_err("the reserved range should be refused");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);

    cleanup(&path);
}

#[tokio::test]
async fn the_number_of_live_snapshots_is_capped() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;

    for i in 0..crate::storage::MAX_LIVE_CHECKPOINTS {
        make_snapshot(&svc, format!("s{i}").as_bytes()).await;
    }
    let status = svc
        .create_snapshot(Request::new(CreateSnapshotRequest {
            snapshot_id: b"one-too-many".to_vec(),
            created_unix_ms: 0,
        }))
        .await
        .expect_err("the cap should be enforced");
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);

    cleanup(&path);
}

/// Managing snapshots is administration, not reading: a snapshot pins
/// storage cluster-wide until someone releases it.
#[tokio::test]
async fn an_ordinary_account_may_not_manage_snapshots() {
    let (log, path) = open_temp();
    let svc = authenticated_service(log, "kv-test").await;
    created_seq(create_user(&svc, "root", "alice", "correct horse", false).await);

    let refused = svc
        .create_snapshot(as_user(
            "alice",
            CreateSnapshotRequest {
                snapshot_id: b"s1".to_vec(),
                created_unix_ms: 0,
            },
        ))
        .await
        .unwrap()
        .into_inner()
        .result;
    match refused {
        Some(create_snapshot_response::Result::AuthError(e)) => {
            assert_eq!(auth_kind(&e), auth_error::Kind::NotAdmin);
        }
        other => panic!("expected NOT_ADMIN, got {other:?}"),
    }

    // An admin may, and then an ordinary account may not release it.
    let made = svc
        .create_snapshot(as_user(
            "root",
            CreateSnapshotRequest {
                snapshot_id: b"s1".to_vec(),
                created_unix_ms: 0,
            },
        ))
        .await
        .unwrap()
        .into_inner()
        .result;
    assert!(matches!(
        made,
        Some(create_snapshot_response::Result::Ok(_))
    ));

    let refused = svc
        .remove_snapshot(as_user(
            "alice",
            RemoveSnapshotRequest {
                snapshot_id: b"s1".to_vec(),
            },
        ))
        .await
        .unwrap()
        .into_inner()
        .result;
    match refused {
        Some(remove_snapshot_response::Result::AuthError(e)) => {
            assert_eq!(auth_kind(&e), auth_error::Kind::NotAdmin);
        }
        other => panic!("expected NOT_ADMIN, got {other:?}"),
    }

    // But reading at it needs no privilege at all: it is a read.
    assert!(read_at(&svc, b"s1", b"anything").await.is_ok());

    cleanup(&path);
}

// --- Invitations ---------------------------------------------------------
//
// The workflow these cover has two halves that never meet: an
// administrator who decides an account may exist, and a person who
// decides what it is called and what its password is. The tests below
// are mostly about the seam between them -- what the invitation carries
// across it, and what it must not.

/// Mints an invitation and hands back `(token, invite_id)`.
async fn create_invite(
    svc: &KvService,
    caller: &str,
    is_admin: bool,
    username: Option<&str>,
    ttl_ms: u64,
) -> create_invite_response::Result {
    svc.create_invite(as_user(
        caller,
        CreateInviteRequest {
            is_admin,
            ttl_ms,
            username: username.map(str::to_string),
        },
    ))
    .await
    .unwrap()
    .into_inner()
    .result
    .expect("create_invite always answers")
}

/// The ordinary case: an invitation for anyone, with the default
/// lifetime, made by `root`.
async fn invite(svc: &KvService) -> (String, String) {
    minted(create_invite(svc, "root", false, None, 0).await)
}

fn minted(result: create_invite_response::Result) -> (String, String) {
    match result {
        create_invite_response::Result::Ok(ok) => {
            let invite = ok.invite.expect("a minted invitation carries its record");
            assert!(ok.seq > 0, "the invitation should have been written");
            (ok.token, invite.invite_id)
        }
        other => panic!("expected an invitation, got {other:?}"),
    }
}

fn invite_refusal(result: create_invite_response::Result) -> auth_error::Kind {
    match result {
        create_invite_response::Result::AuthError(e) => auth_kind(&e),
        other => panic!("expected the invitation to be refused, got {other:?}"),
    }
}

/// `RedeemInvite` carries no identity by design, so this is the one
/// account call built with a plain `Request`.
async fn redeem(
    svc: &KvService,
    token: &str,
    username: &str,
    password: &str,
) -> redeem_invite_response::Result {
    svc.redeem_invite(Request::new(RedeemInviteRequest {
        token: token.to_string(),
        username: username.to_string(),
        password: password.to_string(),
    }))
    .await
    .unwrap()
    .into_inner()
    .result
    .expect("redeem_invite always answers")
}

fn redeemed_seq(result: redeem_invite_response::Result) -> u64 {
    match result {
        redeem_invite_response::Result::Ok(ok) => ok.seq,
        other => panic!("expected the account to be created, got {other:?}"),
    }
}

fn redeem_refusal(result: redeem_invite_response::Result) -> auth_error::Kind {
    match result {
        redeem_invite_response::Result::AuthError(e) => auth_kind(&e),
        other => panic!("expected the redemption to be refused, got {other:?}"),
    }
}

async fn list_invites(svc: &KvService, caller: &str) -> Vec<crate::pb::raft5::client::Invite> {
    let result = svc
        .list_invites(as_user(caller, ListInvitesRequest {}))
        .await
        .unwrap()
        .into_inner()
        .result
        .expect("list_invites always answers");
    match result {
        list_invites_response::Result::Ok(list) => list.invites,
        other => panic!("expected a listing, got {other:?}"),
    }
}

async fn revoke(svc: &KvService, caller: &str, invite_id: &str) -> revoke_invite_response::Result {
    svc.revoke_invite(as_user(
        caller,
        RevokeInviteRequest {
            invite_id: invite_id.to_string(),
        },
    ))
    .await
    .unwrap()
    .into_inner()
    .result
    .expect("revoke_invite always answers")
}

/// The whole point of the feature, end to end: an administrator issues a
/// secret, hands it over, and someone else turns it into an account
/// whose password the administrator never knew.
#[tokio::test]
async fn an_invitation_becomes_an_account_its_issuer_cannot_log_in_as() {
    let (log, path) = open_temp();
    let svc = authenticated_service(log, "kv-test").await;

    let (token, _) = invite(&svc).await;
    assert!(redeemed_seq(redeem(&svc, &token, "alice", "correct horse").await) > 0);

    assert!(!login_token(login(&svc, "alice", "correct horse").await).is_empty());
    // `root` chose nothing about this account but that it could exist.
    assert_eq!(
        login_refusal(login(&svc, "alice", "hunter42").await),
        auth_error::Kind::BadCredentials
    );

    cleanup(&path);
}

/// An invitation is spent in the same transaction that writes the
/// account, which is the whole of "single use".
#[tokio::test]
async fn an_invitation_can_only_be_redeemed_once() {
    let (log, path) = open_temp();
    let svc = authenticated_service(log, "kv-test").await;

    let (token, _) = invite(&svc).await;
    redeemed_seq(redeem(&svc, &token, "alice", "one").await);

    assert_eq!(
        redeem_refusal(redeem(&svc, &token, "bob", "two").await),
        auth_error::Kind::BadInvite
    );
    assert_eq!(
        login_refusal(login(&svc, "bob", "two").await),
        auth_error::Kind::BadCredentials,
        "the second redemption must not have created anything"
    );

    cleanup(&path);
}

/// The rights of the new account come from the invitation, which the
/// redeemer cannot influence -- an invite whose holder could ask for
/// `is_admin` would be a way to promote oneself by accepting one.
#[tokio::test]
async fn the_invitation_decides_whether_the_new_account_is_an_administrator() {
    let (log, path) = open_temp();
    let svc = authenticated_service(log, "kv-test").await;

    let (ordinary, _) = minted(create_invite(&svc, "root", false, None, 0).await);
    let (privileged, _) = minted(create_invite(&svc, "root", true, None, 0).await);
    redeemed_seq(redeem(&svc, &ordinary, "alice", "one").await);
    redeemed_seq(redeem(&svc, &privileged, "bob", "two").await);

    // `create_user` is the admin-only call, so who can run it is the
    // question these two accounts answer differently.
    assert_eq!(
        create_refusal(create_user(&svc, "alice", "carol", "three", false).await),
        auth_error::Kind::NotAdmin
    );
    assert!(created_seq(create_user(&svc, "bob", "carol", "three", false).await) > 0);

    cleanup(&path);
}

/// Issuing one is an administrator's call. It grants an account, and
/// possibly an administrator's account, so it is exactly as privileged
/// as `CreateUser`.
#[tokio::test]
async fn only_an_administrator_may_issue_or_list_or_revoke_an_invitation() {
    let (log, path) = open_temp();
    let svc = authenticated_service(log, "kv-test").await;
    created_seq(create_user(&svc, "root", "alice", "correct horse", false).await);

    let (_, invite_id) = invite(&svc).await;

    assert_eq!(
        invite_refusal(create_invite(&svc, "alice", true, None, 0).await),
        auth_error::Kind::NotAdmin
    );
    match svc
        .list_invites(as_user("alice", ListInvitesRequest {}))
        .await
        .unwrap()
        .into_inner()
        .result
    {
        Some(list_invites_response::Result::AuthError(e)) => {
            assert_eq!(auth_kind(&e), auth_error::Kind::NotAdmin);
        }
        other => panic!("expected NOT_ADMIN, got {other:?}"),
    }
    match revoke(&svc, "alice", &invite_id).await {
        revoke_invite_response::Result::AuthError(e) => {
            assert_eq!(auth_kind(&e), auth_error::Kind::NotAdmin);
        }
        other => panic!("expected NOT_ADMIN, got {other:?}"),
    }

    cleanup(&path);
}

/// The remedy for an invitation sent to the wrong address: the secret
/// cannot be recalled, only made worthless.
#[tokio::test]
async fn a_revoked_invitation_cannot_be_redeemed() {
    let (log, path) = open_temp();
    let svc = authenticated_service(log, "kv-test").await;

    let (token, invite_id) = invite(&svc).await;
    match revoke(&svc, "root", &invite_id).await {
        revoke_invite_response::Result::Seq(seq) => assert!(seq > 0),
        other => panic!("expected the invitation to be withdrawn, got {other:?}"),
    }

    assert_eq!(
        redeem_refusal(redeem(&svc, &token, "alice", "one").await),
        auth_error::Kind::BadInvite
    );

    cleanup(&path);
}

/// An invitation outlives neither its ttl nor the leader's clock.
#[tokio::test]
async fn a_lapsed_invitation_cannot_be_redeemed() {
    let (log, path) = open_temp();
    let svc = authenticated_service(log, "kv-test").await;

    let (token, _) = minted(create_invite(&svc, "root", false, None, 1).await);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    assert_eq!(
        redeem_refusal(redeem(&svc, &token, "alice", "one").await),
        auth_error::Kind::BadInvite
    );
    // And the next issue sweeps it, so lapsed records do not pile up in
    // the reserved range with nothing to remove them.
    invite(&svc).await;
    assert_eq!(
        list_invites(&svc, "root").await.len(),
        1,
        "the lapsed invitation should have been swept by the new one"
    );

    cleanup(&path);
}

/// A pinned invitation is still a secret; the pin is what stops a
/// misdelivered one from quietly becoming an account under a name
/// nobody expected.
#[tokio::test]
async fn an_invitation_pinned_to_a_username_refuses_any_other() {
    let (log, path) = open_temp();
    let svc = authenticated_service(log, "kv-test").await;

    let (token, _) = minted(create_invite(&svc, "root", false, Some("alice"), 0).await);

    assert_eq!(
        redeem_refusal(redeem(&svc, &token, "mallory", "one").await),
        auth_error::Kind::BadInvite
    );
    assert!(redeemed_seq(redeem(&svc, &token, "alice", "one").await) > 0);

    cleanup(&path);
}

/// Choosing a name somebody already has is a thing the holder can fix by
/// choosing another -- so it must not cost them the invitation.
#[tokio::test]
async fn a_name_that_is_taken_refuses_the_redemption_without_spending_it() {
    let (log, path) = open_temp();
    let svc = authenticated_service(log, "kv-test").await;

    let (token, _) = invite(&svc).await;
    assert_eq!(
        redeem_refusal(redeem(&svc, &token, "root", "one").await),
        auth_error::Kind::AlreadyExists
    );
    assert!(
        redeemed_seq(redeem(&svc, &token, "alice", "one").await) > 0,
        "the invitation should have survived the name clash"
    );
    assert!(!login_token(login(&svc, "root", "hunter42").await).is_empty(),
        "the existing account must not have been overwritten");

    cleanup(&path);
}

/// What a listing says, and what it deliberately does not: the token is
/// gone, and a redeemed invitation is not an outstanding one.
#[tokio::test]
async fn the_listing_shows_outstanding_invitations_and_never_their_secrets() {
    let (log, path) = open_temp();
    let svc = authenticated_service(log, "kv-test").await;

    let (token, invite_id) = minted(create_invite(&svc, "root", true, Some("alice"), 0).await);

    let listed = list_invites(&svc, "root").await;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].invite_id, invite_id);
    assert_eq!(listed[0].issued_by, "root");
    assert!(listed[0].is_admin);
    assert_eq!(listed[0].username.as_deref(), Some("alice"));
    assert!(listed[0].expires_unix_ms > 0);
    // The id is a hash of the token, not the token: naming an invitation
    // is not the same as being able to spend one.
    assert_ne!(listed[0].invite_id, token);

    redeemed_seq(redeem(&svc, &token, "alice", "one").await);
    assert!(
        list_invites(&svc, "root").await.is_empty(),
        "a redeemed invitation is no longer outstanding"
    );

    cleanup(&path);
}

/// Nothing about a token that is not one this cluster issued
/// distinguishes it from one that was: both name no record.
#[tokio::test]
async fn a_token_this_cluster_never_issued_is_refused() {
    let (log, path) = open_temp();
    let svc = authenticated_service(log, "kv-test").await;

    for token in ["", "not-a-token", &"x".repeat(4096)] {
        assert_eq!(
            redeem_refusal(redeem(&svc, token, "alice", "one").await),
            auth_error::Kind::BadInvite,
            "{token:?} should not be redeemable"
        );
    }

    cleanup(&path);
}

/// The lifetime is the cluster's policy. One past what it grants is
/// refused rather than quietly shortened, so an issuer never hands out
/// an invitation believing it good for longer than it is.
#[tokio::test]
async fn an_invitation_may_not_outlast_what_the_cluster_grants() {
    let (log, path) = open_temp();
    let svc = authenticated_service(log, "kv-test").await;

    let status = svc
        .create_invite(as_user(
            "root",
            CreateInviteRequest {
                is_admin: false,
                ttl_ms: MAX_INVITE_TTL.as_millis() as u64 + 1,
                username: None,
            },
        ))
        .await
        .expect_err("an over-long lifetime is refused outright");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);

    cleanup(&path);
}

/// With no secret there are no accounts to invite anyone into, so these
/// say so rather than half-working -- the same answer `Login` and
/// `CreateUser` give.
#[tokio::test]
async fn the_invitation_rpcs_report_themselves_disabled_without_a_secret() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;

    assert_eq!(
        invite_refusal(create_invite(&svc, "root", false, None, 0).await),
        auth_error::Kind::AuthDisabled
    );
    assert_eq!(
        redeem_refusal(redeem(&svc, "whatever", "alice", "one").await),
        auth_error::Kind::AuthDisabled
    );

    cleanup(&path);
}
