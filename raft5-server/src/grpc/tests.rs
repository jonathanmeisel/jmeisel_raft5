use std::sync::Arc;

use super::*;
use crate::pb::raft5::client::{
    AclEntry as PbAclEntry, BackupRequest, CommitTransactionRequest, CreateGroupRequest, CreateInviteRequest,
    CreateTransactionRequest, DeleteAclRequest, DropTransactionRequest, GetAclRequest,
    GetGroupRequest, GetKeysRequest, Linearizable, ListAclsRequest, ListGroupsRequest, ListInvitesRequest,
    Principal as PbPrincipal, RedeemInviteRequest, RevokeInviteRequest, Right as PbRight,
    SetAclRequest, UpdateGroupRequest, create_group_response, delete_acl_response,
    get_acl_response, get_group_response, list_acls_response, list_groups_response, principal,
    set_acl_response, update_group_response, auth_error, backup_chunk,
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
    // A nanosecond timestamp alone is not a reliable unique suffix when tests
    // run in parallel: two tests that start at nearly the same moment can get
    // the same value and collide on one directory. So a process-wide counter
    // is added on top, as a second path segment (combining both into one u64
    // could collide too).
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

/// Buffer one key into `txn`, as a client's `Write` does.
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

/// A whole single-key write, as a client makes one: open a transaction,
/// buffer the key, commit. Returns the seq it landed at.
async fn write(svc: &KvService, key: &[u8], value: Option<&[u8]>) -> u64 {
    let txn = begin_txn(svc, 5000).await;
    assert!(
        matches!(buffer(svc, &txn, key, value).await, write_response::Result::Ok(true)),
        "buffering one key into a fresh transaction should be accepted"
    );
    commit(svc, txn).await
}

/// The seq of a transaction that is expected to commit.
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

/// The one entry of a one-key read. A one-key read is never cut short, so
/// there is always exactly one.
fn only(ok: ReadOk) -> KeyVersion {
    assert_eq!(ok.entries.len(), 1, "a one-key read answers one key: {ok:?}");
    ok.entries.into_iter().next().unwrap()
}

/// A stale read of `key` (so always answerable).
async fn read(svc: &KvService, key: &[u8]) -> KeyVersion {
    let resp = svc
        .read(Request::new(ReadRequest {
            keys: vec![key.to_vec()],
            versions_only: false,
            consistency: None,
        }))
        .await
        .unwrap()
        .into_inner();
    match resp.result {
        Some(read_response::Result::Ok(ok)) => only(ok),
        other => panic!("expected a successful read, got {other:?}"),
    }
}

/// A `KvService` on a node bootstrapped into a single-node cluster that has
/// won its election: one that can serve a write.
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
    // On a node with accounts, the administrator it was started with
    // bootstraps it: exactly the identity the auth layer would put here from
    // that operator's token. Bootstrap then writes that administrator into the
    // log, so the cluster starts with one account.
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
    // The lock table learns its term from the first request that needs a
    // leader, and discards everything if the term changed. Doing it here means
    // a test that takes a lock directly (not through an RPC) does not hold one
    // from before the node had a term, which no real caller could.
    svc.leader().expect("this node just became the leader");
    svc
}

/// A `KvService` on a running state machine, plus the handles a test needs
/// to watch it.
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

/// A `RaftPeerService` for `node_id`, served on a real listener at an
/// ephemeral port, for tests where another node's `add_server` must reach
/// it (it calls `Identify` on the `peer_address` it is given).
///
/// The address it reports is still fake and unreachable, as in
/// `kv_service`: that is what the leader records and replicates to, so the
/// new node never catches up and stays a learner. That lets a lone leader
/// keep committing, and remove it again, by itself. Stop it by cancelling
/// the returned token.
async fn spawn_reachable_peer_service(
    log: Arc<RaftLog>,
    node_id: &str,
) -> (std::net::SocketAddr, CancellationToken) {
    let raft::NodeHandles {
        common,
        peer_handle,
        ..
    } = raft::Common::new(
        node_id.to_string(),
        Arc::clone(&log),
        CancellationToken::new(),
    )
    .unwrap();
    tokio::spawn(raft::run(common));

    let addr = free_local_addr();
    let identity = ClusterMember {
        server_id: node_id.to_string(),
        address: format!("http://127.0.0.1:1/{node_id}"),
        client_address: format!("http://127.0.0.1:1/{node_id}-client"),
        voting: true,
    };
    let svc = RaftPeerService::new(peer_handle).identify_as(identity, Store::new(log));

    let cancel = CancellationToken::new();
    let shutdown = cancel.clone();
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(crate::pb::raft5::raft::raft_peer_server::RaftPeerServer::new(svc))
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
            keys: vec![b"hello".to_vec()],
            versions_only: false,
            consistency: None,
        }))
        .await
        .unwrap()
        .into_inner();
    match read_resp.result {
        Some(read_response::Result::Ok(ok)) => {
            let ok = only(ok);
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
            keys: vec![b"missing".to_vec()],
            versions_only: false,
            consistency: None,
        }))
        .await
        .unwrap()
        .into_inner();
    match read_resp.result {
        Some(read_response::Result::Ok(ok)) => {
            let ok = only(ok);
            assert_eq!(ok.seq, 0);
            assert_eq!(ok.value, None);
        }
        other => panic!("expected Ok, got {other:?}"),
    }

    cleanup(&path);
}

/// A non-leader answers with a structured `NotLeader` instead of
/// forwarding (Raft §6.2), so the client retries on the leader and only one
/// node does the work.
#[tokio::test]
async fn writes_to_a_non_leader_come_back_as_not_leader() {
    let (log, path) = open_temp();
    let (svc, _role_rx) = kv_service(log, "bystander");

    // A write is a transaction, so a bystander refuses at the first step
    // (opening one), not at the write itself.
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

    // It also refuses a write that names a transaction it never heard of,
    // instead of reporting it as unknown: which node you ask is settled before
    // which transaction you mean.
    match buffer(&svc, &[0u8; 16], b"k", Some(b"v")).await {
        write_response::Result::Error(e) => {
            assert!(matches!(e.kind, Some(raft_error::Kind::NotLeader(_))));
        }
        other => panic!("expected NotLeader, got {other:?}"),
    }

    // Membership changes go the same way. The leader check runs before
    // `add_server` dials `peer_address`, so a bystander answers `NotLeader`
    // without needing it to be reachable.
    let add = svc
        .add_server(Request::new(AddServerRequest {
            peer_address: "http://127.0.0.1:9998".to_string(),
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

/// A read outside a transaction is served by any node, leader or not:
/// that is all it promises. The same node refuses a read inside a
/// transaction, because it has neither the transaction nor a lock table to
/// protect it.
#[tokio::test]
async fn a_stale_read_is_served_without_a_leader() {
    let (log, path) = open_temp();
    let (svc, _role_rx) = kv_service(log, "bystander");

    let resp = svc
        .read(Request::new(ReadRequest {
            keys: vec![b"k".to_vec()],
            versions_only: false,
            consistency: None,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(resp.result, Some(read_response::Result::Ok(_))));

    let strict = svc
        .read(Request::new(ReadRequest {
            keys: vec![b"k".to_vec()],
            versions_only: false,
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
/// leading now (caught up on what it inherited, and holding a quorum of
/// leases) says yes.
///
/// Note the window this polls through. Winning the election is not enough:
/// the answer stays false until this leader's own no-op is applied,
/// because until then its state does not include everything an earlier
/// leader committed. `get_config` reports that without waiting, so a client
/// looking for the leader is never held up by a node that is not ready; it
/// just looks again.
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
/// server is a member in the leader's log.
#[tokio::test]
async fn add_server_then_remove_server_round_trip_through_the_kv_service() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(Arc::clone(&log), "solo").await;

    // A real, reachable "new" node: `add_server` calls `Identify` on it to
    // learn its identity, so it must answer.
    let (new_log, new_path) = open_temp();
    let (new_addr, new_cancel) = spawn_reachable_peer_service(new_log, "new").await;

    let resp = svc
        .add_server(Request::new(AddServerRequest {
            peer_address: format!("http://{new_addr}"),
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

    // "new"'s peer address is unreachable, so it never catches up and stays a
    // learner. So the cluster still commits on "solo" alone, and can remove it
    // again.
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

/// The operator side of bootstrap: the node names itself in the
/// configuration it forms, so a client does not have to know (or be able
/// to lie about) the address this node advertises.
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

    // A single-node cluster has nobody to lose an election to.
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !matches!(&*role_rx.borrow(), RoleHandle::Leader(_)) {
            role_rx.changed().await.unwrap();
        }
    })
    .await
    .expect("a bootstrapped single-node cluster should elect itself");

    cleanup(&path);
}

/// `GetConfig` shows membership before it commits (Raft §4.1: it takes
/// effect on append), and reports an empty cluster, not an error, for a
/// node not yet bootstrapped.
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
    let (new_addr, new_cancel) = spawn_reachable_peer_service(new_log, "new").await;

    svc.add_server(Request::new(AddServerRequest {
        peer_address: format!("http://{new_addr}"),
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

/// Bootstrap cannot be undone, so a client that names the wrong node is
/// told so, instead of silently initializing whichever node it reached.
#[tokio::test]
async fn bootstrap_refuses_when_the_named_node_is_someone_else() {
    let (log, path) = open_temp();
    let (svc, _role_rx) = kv_service(Arc::clone(&log), "solo");

    let err = svc
        .bootstrap(Request::new(BootstrapRequest {
            server_id: "some-other-node".to_string(),
            heartbeat_millis: 0,
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(log.current_config().unwrap().is_none());

    cleanup(&path);
}

/// End to end through the real tonic service: confirms `RaftPeerService` is
/// connected to a running state machine, not only that
/// `raft::RaftPeerHandle` works alone (that is covered in `raft`'s own unit
/// tests).
///
/// Over a real socket, because `AppendEntries` is a streaming RPC and
/// `tonic::Streaming` has no public constructor: the only way to give the
/// service one is to let tonic build it from a real connection. That also
/// makes this the one test of chunking and reassembly together, end to
/// end; the rules of `reassemble_append_entries` are tested directly below.
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

    // The AppendEntries above just renewed this follower's lease on leader-1,
    // so it refuses every vote request until that lease expires. This node's
    // own candidacy timer has the same deadline, so it may fire first (see
    // `wait_for_vote_grant_or_role_change` in raft::follower's tests for the
    // full reasoning). Retry until either happens.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    let vote_resp = loop {
        if !matches!(&*role_rx.borrow(), raft::RoleHandle::Follower(_)) {
            break None; // moved on to campaigning, which also proves the point
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

/// A transaction id has a fixed width, so any other length is a malformed
/// request, not a transaction that does not exist. Worth telling apart:
/// "no such transaction" is a normal answer a client must handle, while
/// this is a bug in the caller.
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

/// A lifetime the server will not grant is refused, not shortened: a
/// client told "yes" would act as if it had the lease it asked for.
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

/// Dropping is safe to repeat and fine with an unknown id: commit-then-drop
/// is the normal client path, and a drop should not need to know whether
/// it raced its own commit.
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

/// A transaction that was never opened (or was dropped, or already
/// committed) is `UNKNOWN`, not a silent success.
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

/// The whole path through the service: read, buffer, commit, and every key
/// visible at the one seq of the entry.
#[tokio::test]
async fn a_committed_transaction_makes_every_key_visible_at_one_seq() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;
    write(&svc, b"a", Some(b"1")).await;

    let txn = begin_txn(&svc, 2000).await;
    let read_back = svc
        .read(Request::new(ReadRequest {
            keys: vec![b"a".to_vec()],
            versions_only: false,
            consistency: Some(read_request::Consistency::TransactionId(txn.clone())),
        }))
        .await
        .unwrap()
        .into_inner()
        .result;
    assert!(matches!(
        read_back,
        Some(read_response::Result::Ok(ok)) if only(ok.clone()).value.as_deref() == Some(b"1")
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

/// A scan, as a client makes one.
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

/// `has_more` says whether to ask again, and the last key plus a zero byte
/// starts the next batch just past it: the paging contract that
/// `raft5_client::KeyIterator` relies on.
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

/// Zero means the server's default, not zero keys: a proto3 scalar cannot
/// tell "unset" from "0", so the default must be what the absent value
/// means.
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

/// Refused, not shortened, as for an over-long transaction lifetime: a
/// caller that pages in batches of N must not be told yes and given
/// something else.
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

/// A deleted key is simply not there: the same merging of "deleted" and
/// "never written" that a read does.
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

/// A transaction sees its own uncommitted changes, as a read does: a key it
/// wrote is there although the store has never seen it, and a key it
/// deleted is gone although the store still has it.
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

    // And nobody outside sees any of that until it commits.
    let (keys, _) = get_keys(&svc, b"", 10, None).await;
    assert_eq!(as_strings(&keys), ["a", "c"]);

    cleanup(&path);
}

/// A buffered delete shortens its batch without ending the walk, which is
/// exactly why `has_more` is reported, not inferred from
/// `keys.len() < max_keys`.
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

/// The point of scanning in a transaction: every returned key is locked, so
/// nobody else can change it while the transaction lives.
#[tokio::test]
async fn a_transactional_scan_locks_every_key_it_returns() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "scan").await;
    write(&svc, b"a", Some(b"1")).await;
    write(&svc, b"b", Some(b"2")).await;

    let scanner = begin_txn(&svc, 5000).await;
    let (keys, _) = get_keys(&svc, b"", 10, Some(&scanner)).await;
    assert_eq!(as_strings(&keys), ["a", "b"]);

    // Younger, so it cannot wound its way past the read locks: it is refused,
    // with the key it wanted.
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

/// A scan outside a transaction is served by any node, with no leader
/// check: the same terms as a plain read.
#[tokio::test]
async fn a_scan_outside_a_transaction_is_served_without_a_leader() {
    let (log, path) = open_temp();
    let (svc, _role_rx) = kv_service(log, "bystander");

    let (keys, has_more) = get_keys(&svc, b"", 10, None).await;
    assert!(keys.is_empty() && !has_more);

    // A scan inside a transaction is not: this node has neither the
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

/// The round trip the wire depends on: what the leader chunked must come
/// back as the request it chunked, mutations and all.
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

/// A batch large enough to span several chunks still arrives whole: the
/// main case chunking exists for.
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

/// The header carries the term, so it must arrive before anything can be
/// decided about the body. A stream that starts with a fragment is
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
/// read, but refused with a normal response that carries the term, not an
/// error. An error would reach the leader as a transport failure, and it
/// would retry forever instead of stepping down.
#[tokio::test]
async fn a_stale_leaders_stream_is_refused_rather_than_errored() {
    let chunks = vec![header_chunk(3, 4), fragment(vec![0; 4])];
    match reassemble(chunks, 9).await.unwrap() {
        Reassembled::Refused(_) => {}
        Reassembled::Request(_) => panic!("a stale term must not be accepted"),
    }
}

/// The declared size is the sender's claim, and trusting it to size a
/// buffer would let one header exhaust memory. Refused on the claim alone,
/// without allocating for it.
#[tokio::test]
async fn an_absurd_declared_size_is_refused_without_allocating_it() {
    let chunks = vec![header_chunk(1, u64::MAX)];
    match reassemble(chunks, 1).await.unwrap() {
        Reassembled::Refused(_) => {}
        Reassembled::Request(_) => panic!("an over-limit declaration must be refused"),
    }
}

/// ...and a sender that under-declares and keeps sending is caught on what
/// actually arrives, which is the number that costs memory.
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

/// A body that disagrees with its header is a broken sender, not a policy
/// refusal, so it is an error: there is nothing sensible to answer, and
/// retrying the same bytes cannot help.
#[tokio::test]
async fn a_body_that_disagrees_with_its_header_is_rejected() {
    let chunks = vec![header_chunk(1, 100), fragment(vec![0; 4])];
    let status = reassemble(chunks, 1)
        .await
        .expect_err("a short body should be rejected");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

/// Everything after the header must be a fragment. A second header would
/// mean two requests on one stream, which this protocol has no meaning for.
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
// `crate::keyspace` sets aside a key prefix for the cluster's own state
// (accounts, invitations, groups, ACLs, checkpoints). These tests cover the
// points where the client API must refuse to address it. The server's own
// writes must still reach it, so each test that needs a reserved key seeds
// one through `append_directly`, the path the server's own writes take.

/// Put a key into the store the way the server's own writes do: at the
/// leader handle, below the gRPC boundary where reserved keys are refused.
async fn append_directly(svc: &KvService, key: &[u8], value: &[u8]) -> u64 {
    svc.leader()
        .expect("the test service leads")
        .append_writes(vec![Mutation::new(key, Some(value))])
        .await
        .expect("a direct append should commit")
}

/// The guard against privilege escalation. A client that could buffer this
/// write could write its own account record and set `is_admin` on it.
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

/// Both branches: refused, not answered as absent, because a returned
/// password hash can be attacked offline.
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
                keys: vec![key.clone()],
                versions_only: false,
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

/// A walk from the very start begins at the empty key, which sorts below
/// the whole reserved range. So this is the scan that would reveal every
/// account record if the filter were missing.
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

/// Reserved keys are skipped inside the scan, not filtered from its result,
/// so they must not use up any of the caller's batch. A client that asks
/// for three keys gets three, however many account records sort before
/// them.
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

    // Exactly as many keys as there are: a full batch, and nothing after it.
    let (keys, has_more) = get_keys(&svc, b"", 3, None).await;
    assert_eq!(as_strings(&keys), vec!["k1", "k2", "k3"]);
    assert!(
        !has_more,
        "three keys asked for and three that exist is the end of the walk"
    );

    // One fewer than there are, so `has_more` must notice the third, past three
    // reserved keys that do not count toward it.
    let (keys, has_more) = get_keys(&svc, b"", 2, None).await;
    assert_eq!(as_strings(&keys), vec!["k1", "k2"]);
    assert!(has_more, "there is a third key to come");

    cleanup(&path);
}

// --- accounts and tokens ----------------------------------------------
//
// The auth layer itself is not involved here: these tests drive the
// handlers directly and supply the caller's identity as that layer does.
// They cover everything below it: who may do what, what the store ends up
// holding, and the one window where a node has no accounts to check.

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

/// A node with accounts, bootstrapped by `root`, who stays in the store as
/// its administrator with the password `hunter42`.
async fn authenticated_service(log: Arc<RaftLog>, node_id: &str) -> KvService {
    let admin = startup_admin("root", "hunter42").await;
    leader_kv_service_with_auth(log, node_id, auth_config(Some(admin))).await
}

/// A request with a verified identity, as the auth layer leaves it.
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

/// With no secret, there is nothing to sign a token with and no accounts to
/// check, so all three calls say so instead of half working.
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

/// The window the startup administrator exists for. A node that was never
/// bootstrapped has no accounts and no leader to read them from, yet
/// `Bootstrap` needs a token like every other call, so the operator must be
/// able to log in against something.
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

/// Bootstrap turns the startup administrator from a flag into an account,
/// so the cluster starts with someone who can administer it.
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

/// And the flag itself stops working. It depends on the node having a
/// configuration, not on the account being absent, so removing the
/// administrator cannot silently re-enable the startup password.
///
/// The other side of that: this is also how a cluster can lock itself out.
/// Deleting the last administrator cannot be undone.
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

/// An account that does not exist and a wrong password get the same
/// answer: saying which would confirm that a username exists, and a
/// username is half of a credential.
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

/// Logging in says who you are, not what you may do.
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

/// A token for an account that was deleted still verifies (nothing can
/// withdraw one), but it gives no administrative rights, because those are
/// looked up on each call, not stated in the token.
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
/// identity from the auth layer, there is no caller.
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
/// authorized from the store, because the store is empty. So it is
/// authorized against the administrator the node was started with.
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

/// Both sides together: an account is a key/value pair, and the client API
/// cannot see it.
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
            keys: vec![crate::keyspace::user_key("alice")],
            versions_only: false,
            consistency: None,
        }))
        .await
        .expect_err("an account record is not readable");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);

    cleanup(&path);
}

/// Membership changes are the most destructive calls on this service
/// (removing the wrong server can cost the cluster its quorum), so a token
/// alone is not enough for them either.
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
                peer_address: "http://127.0.0.1:1".to_string(),
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

/// And the administrator still can: the check turns away the wrong caller,
/// not the call.
#[tokio::test]
async fn an_administrator_may_still_change_cluster_membership() {
    let (log, path) = open_temp();
    let svc = authenticated_service(log, "kv-test").await;

    // Removing a non-member is refused on its merits, which is the point: it
    // passed the admin check and was judged.
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

// --- versions-only reads --------------------------------------------

/// What a versions-only read of `key` reports, at any consistency.
async fn version_at(
    svc: &KvService,
    key: &[u8],
    consistency: Option<read_request::Consistency>,
) -> read_response::Result {
    svc.read(Request::new(ReadRequest {
        keys: vec![key.to_vec()],
        versions_only: true,
        consistency,
    }))
    .await
    .unwrap()
    .into_inner()
    .result
    .expect("a read always answers")
}

/// A stale versions-only read, which is always answerable.
async fn version(svc: &KvService, key: &[u8]) -> u64 {
    match version_at(svc, key, None).await {
        read_response::Result::Ok(ok) => only(ok).seq,
        other => panic!("expected a successful versions-only read, got {other:?}"),
    }
}

#[tokio::test]
async fn version_read_reports_the_seq_a_write_landed_at() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;

    let seq = write(&svc, b"k", Some(b"v")).await;
    assert_eq!(version(&svc, b"k").await, seq);
    // It agrees with what a full read says about the same key, which is what
    // makes it a substitute for one.
    assert_eq!(read(&svc, b"k").await.seq, seq);

    cleanup(&path);
}

#[tokio::test]
async fn version_read_is_zero_for_a_key_with_no_value() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;

    // Never written.
    assert_eq!(version(&svc, b"missing").await, 0);

    // And deleted, which gives the same answer: a full read does not tell the
    // two apart either.
    write(&svc, b"k", Some(b"v")).await;
    write(&svc, b"k", None).await;
    assert_eq!(version(&svc, b"k").await, 0);

    cleanup(&path);
}

#[tokio::test]
async fn version_read_refuses_a_reserved_key_transactional_or_not() {
    // As for a full read, and for a sharper reason: a seq seems harmless, but
    // "this account changed at seq N" is still the reserved range answering
    // questions about itself.
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;
    let key = crate::keyspace::user_key("alice");
    append_directly(&svc, &key, b"hash").await;

    let txn = begin_txn(&svc, 5000).await;
    for consistency in [
        None,
        Some(read_request::Consistency::TransactionId(txn)),
        Some(read_request::Consistency::Linearizable(Linearizable {})),
    ] {
        let status = svc
            .read(Request::new(ReadRequest {
                keys: vec![key.clone()],
                versions_only: true,
                consistency,
            }))
            .await
            .expect_err("a reserved key should be refused");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    cleanup(&path);
}

#[tokio::test]
async fn a_stale_version_read_is_served_without_a_leader() {
    // The mirror of `a_stale_read_is_served_without_a_leader`: the default asks
    // for no guarantee, so any node answers, while both guaranteed modes need
    // a leader, and this node is not one.
    let (log, path) = open_temp();
    let (svc, _role_rx) = kv_service(log, "bystander");

    assert!(matches!(
        version_at(&svc, b"k", None).await,
        read_response::Result::Ok(_)
    ));

    for consistency in [
        read_request::Consistency::TransactionId(vec![0u8; 16]),
        read_request::Consistency::Linearizable(Linearizable {}),
    ] {
        match version_at(&svc, b"k", Some(consistency)).await {
            read_response::Result::Error(e) => {
                assert!(matches!(e.kind, Some(raft_error::Kind::NotLeader(_))));
            }
            other => panic!("expected NotLeader, got {other:?}"),
        }
    }

    cleanup(&path);
}

#[tokio::test]
async fn a_transactional_version_read_holds_the_key() {
    // What proves the transactional path takes the lock, instead of reading
    // storage behind the lock table's back: another transaction must not be
    // able to write the key afterwards.
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;
    write(&svc, b"k", Some(b"v")).await;

    let reader = begin_txn(&svc, 5000).await;
    let seq = match version_at(
        &svc,
        b"k",
        Some(read_request::Consistency::TransactionId(
            reader.clone(),
        )),
    )
    .await
    {
        read_response::Result::Ok(ok) => only(ok).seq,
        other => panic!("expected a successful versions-only read, got {other:?}"),
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
async fn a_transactional_version_read_reports_committed_state_over_its_own_write() {
    // A transaction that asks about a key it buffered gets the committed seq:
    // a buffered write has no seq until the commit gives it one. It must also
    // not deadlock on its own write lock, which is why the read side skips a
    // key it already holds.
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
        Some(read_request::Consistency::TransactionId(txn)),
    )
    .await
    {
        read_response::Result::Ok(ok) => assert_eq!(only(ok).seq, committed),
        other => panic!("expected a successful versions-only read, got {other:?}"),
    }

    cleanup(&path);
}

#[tokio::test]
async fn a_linearizable_read_and_version_agree_with_the_stale_ones_on_a_leader() {
    // On a settled single-node leader, all three consistencies describe the
    // same state, so this checks that the linearizable and transactional paths
    // answer at all, and answer the same. Their guarantee is about when, not
    // what.
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;
    let seq = write(&svc, b"k", Some(b"v")).await;

    match version_at(
        &svc,
        b"k",
        Some(read_request::Consistency::Linearizable(Linearizable {})),
    )
    .await
    {
        read_response::Result::Ok(ok) => assert_eq!(only(ok).seq, seq),
        other => panic!("expected a successful versions-only read, got {other:?}"),
    }

    let resp = svc
        .read(Request::new(ReadRequest {
            keys: vec![b"k".to_vec()],
            versions_only: false,
            consistency: Some(read_request::Consistency::Linearizable(Linearizable {})),
        }))
        .await
        .unwrap()
        .into_inner();
    match resp.result {
        Some(read_response::Result::Ok(ok)) => {
            let ok = only(ok);
            assert_eq!(ok.seq, seq);
            assert_eq!(ok.value.as_deref(), Some(b"v".as_slice()));
        }
        other => panic!("expected a successful read, got {other:?}"),
    }

    cleanup(&path);
}

// --- snapshots --------------------------------------------------------

/// Create a snapshot on a node with no accounts, where `require_admin`
/// passes because nobody can be an administrator.
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

async fn read_at(svc: &KvService, id: &[u8], key: &[u8]) -> Result<KeyVersion, Status> {
    let resp = svc
        .read(Request::new(ReadRequest {
            keys: vec![key.to_vec()],
            versions_only: false,
            consistency: Some(read_request::Consistency::SnapshotId(id.to_vec())),
        }))
        .await?
        .into_inner();
    match resp.result {
        Some(read_response::Result::Ok(ok)) => Ok(only(ok)),
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
async fn version_read_at_a_snapshot_reports_the_seq_it_was_written_at() {
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
        read_response::Result::Ok(ok) => only(ok).seq,
        other => panic!("expected a successful versions-only read, got {other:?}"),
    }
}

async fn snapshot_version_result(
    svc: &KvService,
    id: &[u8],
    key: &[u8],
) -> Result<read_response::Result, Status> {
    Ok(svc
        .read(Request::new(ReadRequest {
            keys: vec![key.to_vec()],
            versions_only: true,
            consistency: Some(read_request::Consistency::SnapshotId(id.to_vec())),
        }))
        .await?
        .into_inner()
        .result
        .expect("a read always answers"))
}

#[tokio::test]
async fn an_unknown_snapshot_is_refused_rather_than_answered_as_empty() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;
    write(&svc, b"k", Some(b"v")).await;

    // Answering an unknown id as "no value" would look the same as a snapshot
    // taken before the key existed, which is exactly the confusion an explicit
    // refusal avoids.
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
    // The rule the whole feature rests on: a snapshot's seq is the seq of its
    // own record, so a second write would silently move it forward in time.
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

    // And it still reads where it did before.
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

    // An RFC 3339-style timestamp (the client's default id) must be valid.
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

    // And the record's key cannot be addressed directly.
    let status = svc
        .read(Request::new(ReadRequest {
            keys: vec![b"\0snap/s1".to_vec()],
            versions_only: false,
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
/// storage across the cluster until someone releases it.
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

    // An admin may create one, and then an ordinary account may not release
    // it.
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

    // But reading at it needs no privilege: it is a read.
    assert!(read_at(&svc, b"s1", b"anything").await.is_ok());

    cleanup(&path);
}

// --- Invitations ---------------------------------------------------------
//
// The workflow has two sides that never meet: an administrator who
// decides an account may exist, and a person who decides its name and
// password. These tests are mostly about the boundary between them: what
// the invitation carries across, and what it must not.

/// Create an invitation and return `(token, invite_id)`.
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

/// The normal case: an invitation for anyone, with the default lifetime,
/// created by `root`.
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

/// `RedeemInvite` has no identity by design, so this is the one account
/// call built with a plain `Request`.
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
/// secret and hands it over, and someone else turns it into an account
/// whose password the administrator never knew.
#[tokio::test]
async fn an_invitation_becomes_an_account_its_issuer_cannot_log_in_as() {
    let (log, path) = open_temp();
    let svc = authenticated_service(log, "kv-test").await;

    let (token, _) = invite(&svc).await;
    assert!(redeemed_seq(redeem(&svc, &token, "alice", "correct horse").await) > 0);

    assert!(!login_token(login(&svc, "alice", "correct horse").await).is_empty());
    // `root` chose nothing about this account except that it could exist.
    assert_eq!(
        login_refusal(login(&svc, "alice", "hunter42").await),
        auth_error::Kind::BadCredentials
    );

    cleanup(&path);
}

/// An invitation is used up in the same transaction that writes the
/// account, which is all "single use" means.
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

/// The new account's rights come from the invitation, which the redeemer
/// cannot change: an invite whose holder could ask for `is_admin` would let
/// anyone promote themselves by accepting one.
#[tokio::test]
async fn the_invitation_decides_whether_the_new_account_is_an_administrator() {
    let (log, path) = open_temp();
    let svc = authenticated_service(log, "kv-test").await;

    let (ordinary, _) = minted(create_invite(&svc, "root", false, None, 0).await);
    let (privileged, _) = minted(create_invite(&svc, "root", true, None, 0).await);
    redeemed_seq(redeem(&svc, &ordinary, "alice", "one").await);
    redeemed_seq(redeem(&svc, &privileged, "bob", "two").await);

    // `create_user` is admin-only, so who may run it is the question these two
    // accounts answer differently.
    assert_eq!(
        create_refusal(create_user(&svc, "alice", "carol", "three", false).await),
        auth_error::Kind::NotAdmin
    );
    assert!(created_seq(create_user(&svc, "bob", "carol", "three", false).await) > 0);

    cleanup(&path);
}

/// Issuing one is an administrator's call. It grants an account, possibly
/// an administrator's account, so it is exactly as privileged as
/// `CreateUser`.
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

/// The fix for an invitation sent to the wrong address: the secret cannot
/// be recalled, only made worthless.
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
    // And the next issue sweeps it, so lapsed records do not pile up in the
    // reserved range with nothing to remove them.
    invite(&svc).await;
    assert_eq!(
        list_invites(&svc, "root").await.len(),
        1,
        "the lapsed invitation should have been swept by the new one"
    );

    cleanup(&path);
}

/// A pinned invitation is still a secret; the pin stops a misdelivered one
/// from silently becoming an account with a name nobody expected.
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

/// Choosing a name that is taken is something the holder can fix by
/// choosing another, so it must not cost them the invitation.
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

/// What a listing shows, and what it does not, on purpose: the token is
/// gone, and a redeemed invitation is not an open one.
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
    // The id is a hash of the token, not the token: naming an invitation does
    // not let you use it.
    assert_ne!(listed[0].invite_id, token);

    redeemed_seq(redeem(&svc, &token, "alice", "one").await);
    assert!(
        list_invites(&svc, "root").await.is_empty(),
        "a redeemed invitation is no longer outstanding"
    );

    cleanup(&path);
}

/// Nothing distinguishes a token this cluster did not issue from one it
/// did: neither names a record.
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

/// The lifetime is cluster policy. A lifetime past what it grants is
/// refused, not shortened, so an issuer never hands out an invitation
/// believing it lasts longer than it does.
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
/// calls say so instead of half working: the same answer `Login` and
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

// ---------------------------------------------------------------------
// Permissions: groups and prefix ACLs.
//
// The handler-level part. These give the service a verified identity
// directly, like the rest of this file; `tests/auth.rs` covers the same
// ground through the real tower layer and real tokens.
// ---------------------------------------------------------------------

fn pb_user(name: &str) -> PbPrincipal {
    PbPrincipal {
        kind: Some(principal::Kind::User(name.to_string())),
    }
}

fn pb_group(name: &str) -> PbPrincipal {
    PbPrincipal {
        kind: Some(principal::Kind::Group(name.to_string())),
    }
}

fn grant(principal: PbPrincipal, right: PbRight) -> PbAclEntry {
    PbAclEntry {
        principal: Some(principal),
        right: right as i32,
    }
}

async fn set_acl(
    svc: &KvService,
    caller: &str,
    prefix: &[u8],
    entries: Vec<PbAclEntry>,
) -> set_acl_response::Result {
    svc.set_acl(as_user(
        caller,
        SetAclRequest {
            prefix: prefix.to_vec(),
            entries,
        },
    ))
    .await
    .unwrap()
    .into_inner()
    .result
    .expect("set_acl always answers")
}

/// `set_acl`, for calls a test expects to succeed.
async fn must_set_acl(svc: &KvService, caller: &str, prefix: &[u8], entries: Vec<PbAclEntry>) -> u64 {
    match set_acl(svc, caller, prefix, entries).await {
        set_acl_response::Result::Seq(seq) => seq,
        other => panic!("expected the ACL to be written, got {other:?}"),
    }
}

async fn delete_acl(svc: &KvService, caller: &str, prefix: &[u8]) -> delete_acl_response::Result {
    svc.delete_acl(as_user(
        caller,
        DeleteAclRequest {
            prefix: prefix.to_vec(),
        },
    ))
    .await
    .unwrap()
    .into_inner()
    .result
    .expect("delete_acl always answers")
}

async fn create_group(
    svc: &KvService,
    caller: &str,
    name: &str,
    members: Vec<PbPrincipal>,
) -> create_group_response::Result {
    svc.create_group(as_user(
        caller,
        CreateGroupRequest {
            name: name.to_string(),
            members,
            admins: Vec::new(),
        },
    ))
    .await
    .unwrap()
    .into_inner()
    .result
    .expect("create_group always answers")
}

async fn must_create_group(svc: &KvService, caller: &str, name: &str, members: Vec<PbPrincipal>) {
    match create_group(svc, caller, name, members).await {
        create_group_response::Result::Seq(_) => {}
        other => panic!("expected the group to be created, got {other:?}"),
    }
}

/// `UpdateGroup` with only the two fields most tests need.
async fn update_group(
    svc: &KvService,
    caller: &str,
    name: &str,
    add_members: Vec<PbPrincipal>,
    remove_members: Vec<PbPrincipal>,
) -> update_group_response::Result {
    svc.update_group(as_user(
        caller,
        UpdateGroupRequest {
            name: name.to_string(),
            add_members,
            remove_members,
            add_admins: Vec::new(),
            remove_admins: Vec::new(),
        },
    ))
    .await
    .unwrap()
    .into_inner()
    .result
    .expect("update_group always answers")
}

/// `UpdateGroup` without unwrapping the transport error.
///
/// A malformed request (one that would put a group inside itself, or
/// leave it with no administrator) returns an `InvalidArgument` status, not
/// an in-band answer. That is the channel `reject_reserved` uses, which the
/// client treats as permanent, so a helper that can see it is useful.
async fn try_update_group(
    svc: &KvService,
    caller: &str,
    req: UpdateGroupRequest,
) -> Result<update_group_response::Result, Status> {
    Ok(svc
        .update_group(as_user(caller, req))
        .await?
        .into_inner()
        .result
        .expect("update_group always answers"))
}

fn members_only(name: &str, add: Vec<PbPrincipal>, remove: Vec<PbPrincipal>) -> UpdateGroupRequest {
    UpdateGroupRequest {
        name: name.to_string(),
        add_members: add,
        remove_members: remove,
        add_admins: Vec::new(),
        remove_admins: Vec::new(),
    }
}

async fn read_as(svc: &KvService, caller: &str, key: &[u8]) -> read_response::Result {
    svc.read(as_user(
        caller,
        ReadRequest {
            keys: vec![key.to_vec()],
            versions_only: false,
            consistency: None,
        },
    ))
    .await
    .unwrap()
    .into_inner()
    .result
    .expect("a read always answers")
}

/// A whole single-key write as `caller`. The transaction is opened and
/// cleaned up here, so a refusal leaves nothing behind.
async fn write_as(
    svc: &KvService,
    caller: &str,
    key: &[u8],
    value: Option<&[u8]>,
) -> write_response::Result {
    let txn = begin_txn(svc, 5000).await;
    let result = svc
        .write(as_user(
            caller,
            WriteRequest {
                transaction_id: txn.clone(),
                key: key.to_vec(),
                value: value.map(<[u8]>::to_vec),
            },
        ))
        .await
        .unwrap()
        .into_inner()
        .result
        .expect("a write always answers");

    match &result {
        write_response::Result::Ok(true) => {
            commit(svc, txn).await;
        }
        _ => {
            svc.drop_transaction(Request::new(DropTransactionRequest {
                transaction_id: txn,
            }))
            .await
            .unwrap();
        }
    }
    result
}

async fn get_keys_as(
    svc: &KvService,
    caller: &str,
    start: &[u8],
    max_keys: u32,
) -> crate::pb::raft5::client::GetKeysOk {
    let result = svc
        .get_keys(as_user(
            caller,
            GetKeysRequest {
                start_key: start.to_vec(),
                max_keys,
                consistency: None,
            },
        ))
        .await
        .unwrap()
        .into_inner()
        .result;
    match result {
        Some(get_keys_response::Result::Ok(ok)) => ok,
        other => panic!("expected a successful scan, got {other:?}"),
    }
}

fn read_refusal(result: read_response::Result) -> auth_error::Kind {
    match result {
        read_response::Result::AuthError(e) => auth_kind(&e),
        other => panic!("expected the read to be refused, got {other:?}"),
    }
}

fn write_refusal(result: write_response::Result) -> auth_error::Kind {
    match result {
        write_response::Result::AuthError(e) => auth_kind(&e),
        other => panic!("expected the write to be refused, got {other:?}"),
    }
}

fn acl_refusal(result: set_acl_response::Result) -> auth_error::Kind {
    match result {
        set_acl_response::Result::AuthError(e) => auth_kind(&e),
        other => panic!("expected the ACL not to be written, got {other:?}"),
    }
}

fn group_refusal(result: create_group_response::Result) -> auth_error::Kind {
    match result {
        create_group_response::Result::AuthError(e) => auth_kind(&e),
        other => panic!("expected the group not to be created, got {other:?}"),
    }
}

fn ok_read(result: read_response::Result) -> KeyVersion {
    match result {
        read_response::Result::Ok(ok) => only(ok),
        other => panic!("expected a successful read, got {other:?}"),
    }
}

/// A node with accounts, `root` as its administrator, and an ordinary
/// account `alice`: the setup almost every test below wants.
async fn service_with_alice(log: Arc<RaftLog>, node_id: &str) -> KvService {
    let svc = authenticated_service(log, node_id).await;
    created_seq(create_user(&svc, "root", "alice", "hunter42", false).await);
    svc
}

// -- the default -------------------------------------------------------

/// With no ACLs written, nothing is refused and nothing changes.
#[tokio::test]
async fn without_any_acl_every_account_reaches_every_key() {
    let (log, path) = open_temp();
    let svc = service_with_alice(log, "kv-test").await;

    assert!(matches!(
        write_as(&svc, "alice", b"app/config", Some(b"v")).await,
        write_response::Result::Ok(true)
    ));
    assert_eq!(
        ok_read(read_as(&svc, "alice", b"app/config").await).value,
        Some(b"v".to_vec())
    );
    assert_eq!(
        as_strings(&get_keys_as(&svc, "alice", b"", 10).await.keys),
        vec!["app/config"]
    );

    cleanup(&path);
}

/// A cluster with no secret keeps no accounts, so no ACL can be about
/// anyone. Decided in one place, as `require_admin` decides it.
#[tokio::test]
async fn a_node_without_a_secret_enforces_no_acls_and_has_none_to_manage() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;

    assert_eq!(
        acl_refusal(set_acl(&svc, "root", b"app/", vec![grant(pb_user("root"), PbRight::Write)]).await),
        auth_error::Kind::AuthDisabled
    );
    assert_eq!(
        group_refusal(create_group(&svc, "root", "ops", vec![]).await),
        auth_error::Kind::AuthDisabled
    );
    // And the data path is untouched: no identity is even looked for.
    assert!(write(&svc, b"app/config", Some(b"v")).await > 0);

    cleanup(&path);
}

// -- enforcement on the data path --------------------------------------

/// An ACL on `app/` that names only `root` shuts `alice` out of that
/// subtree, and leaves the rest of the store as open as it was.
#[tokio::test]
async fn an_acl_denies_the_subtree_it_governs_and_nothing_else() {
    let (log, path) = open_temp();
    let svc = service_with_alice(log, "kv-test").await;

    write(&svc, b"app/config", Some(b"v")).await;
    write(&svc, b"other/config", Some(b"v")).await;
    must_set_acl(&svc, "root", b"app/", vec![grant(pb_user("root"), PbRight::Write)]).await;

    assert_eq!(
        read_refusal(read_as(&svc, "alice", b"app/config").await),
        auth_error::Kind::NotPermitted
    );
    assert_eq!(
        write_refusal(write_as(&svc, "alice", b"app/config", Some(b"x")).await),
        auth_error::Kind::NotPermitted
    );
    // A delete is a write with no value, so the same check covers it.
    assert_eq!(
        write_refusal(write_as(&svc, "alice", b"app/config", None).await),
        auth_error::Kind::NotPermitted
    );
    // A key's seq is a fact about a key she may not read.
    let refused = svc
        .read(as_user(
            "alice",
            ReadRequest {
                keys: vec![b"app/config".to_vec()],
                versions_only: true,
                consistency: None,
            },
        ))
        .await
        .unwrap()
        .into_inner()
        .result;
    assert!(matches!(
        refused,
        Some(read_response::Result::AuthError(_))
    ));
    // An administrator gets an answer for the same key.
    assert!(matches!(
        svc.read(as_user(
            "root",
            ReadRequest {
                keys: vec![b"app/config".to_vec()],
                versions_only: true,
                consistency: None,
            },
        ))
        .await
        .unwrap()
        .into_inner()
        .result,
        Some(read_response::Result::Ok(_))
    ));

    // Nothing governs `other/`, so it stays open.
    assert_eq!(
        ok_read(read_as(&svc, "alice", b"other/config").await).value,
        Some(b"v".to_vec())
    );
    assert!(matches!(
        write_as(&svc, "alice", b"other/config", Some(b"x")).await,
        write_response::Result::Ok(true)
    ));

    cleanup(&path);
}

/// An ACL never shuts out an administrator: they could rewrite it anyway,
/// and locking one out would only lock out the cluster.
#[tokio::test]
async fn an_administrator_is_never_denied_by_an_acl() {
    let (log, path) = open_temp();
    let svc = service_with_alice(log, "kv-test").await;

    write(&svc, b"app/config", Some(b"v")).await;
    // An ACL that names `alice` and not `root` at all.
    must_set_acl(&svc, "root", b"app/", vec![grant(pb_user("alice"), PbRight::Read)]).await;

    assert_eq!(
        ok_read(read_as(&svc, "root", b"app/config").await).value,
        Some(b"v".to_vec())
    );
    assert!(matches!(
        write_as(&svc, "root", b"app/config", Some(b"x")).await,
        write_response::Result::Ok(true)
    ));
    // And `alice`'s own grant is read, not write.
    assert!(matches!(
        read_as(&svc, "alice", b"app/config").await,
        read_response::Result::Ok(_)
    ));
    assert_eq!(
        write_refusal(write_as(&svc, "alice", b"app/config", Some(b"y")).await),
        auth_error::Kind::NotPermitted
    );

    cleanup(&path);
}

/// The longest ACL that exists decides, not the longest that grants.
/// `alice` holds write on `app/` and is still shut out of `app/secret/`.
#[tokio::test]
async fn the_longest_prefix_with_an_acl_decides() {
    let (log, path) = open_temp();
    let svc = service_with_alice(log, "kv-test").await;

    write(&svc, b"app/config", Some(b"v")).await;
    write(&svc, b"app/secret/k", Some(b"v")).await;
    must_set_acl(&svc, "root", b"app/", vec![grant(pb_user("alice"), PbRight::Write)]).await;
    must_set_acl(
        &svc,
        "root",
        b"app/secret/",
        vec![grant(pb_user("root"), PbRight::Write)],
    )
    .await;

    assert!(matches!(
        read_as(&svc, "alice", b"app/config").await,
        read_response::Result::Ok(_)
    ));
    assert_eq!(
        read_refusal(read_as(&svc, "alice", b"app/secret/k").await),
        auth_error::Kind::NotPermitted
    );

    cleanup(&path);
}

/// An ACL on the empty prefix governs every key, which is how a cluster
/// closes everything by default.
#[tokio::test]
async fn an_acl_on_the_empty_prefix_closes_the_whole_store() {
    let (log, path) = open_temp();
    let svc = service_with_alice(log, "kv-test").await;

    write(&svc, b"anything", Some(b"v")).await;
    must_set_acl(&svc, "root", b"", vec![grant(pb_user("root"), PbRight::Write)]).await;

    assert_eq!(
        read_refusal(read_as(&svc, "alice", b"anything").await),
        auth_error::Kind::NotPermitted
    );
    assert_eq!(
        read_refusal(read_as(&svc, "alice", b"").await),
        auth_error::Kind::NotPermitted
    );

    cleanup(&path);
}

// -- groups ------------------------------------------------------------

/// Membership grants what the group holds, through nesting too. Removing
/// the member removes the access on the very next request, which tests that
/// the epoch key really invalidates the cache.
#[tokio::test]
async fn group_membership_grants_access_and_removing_it_revokes_at_once() {
    let (log, path) = open_temp();
    let svc = service_with_alice(log, "kv-test").await;

    write(&svc, b"app/config", Some(b"v")).await;
    must_create_group(&svc, "root", "interns", vec![pb_user("alice")]).await;
    must_create_group(&svc, "root", "staff", vec![pb_group("interns")]).await;
    // The ACL names only the outermost group.
    must_set_acl(&svc, "root", b"app/", vec![grant(pb_group("staff"), PbRight::Read)]).await;

    assert!(
        matches!(read_as(&svc, "alice", b"app/config").await, read_response::Result::Ok(_)),
        "membership through a nested group should grant what the outer group holds"
    );

    // Out of the inner group, so out of the outer one.
    assert!(matches!(
        update_group(&svc, "root", "interns", vec![], vec![pb_user("alice")]).await,
        update_group_response::Result::Seq(_)
    ));
    assert_eq!(
        read_refusal(read_as(&svc, "alice", b"app/config").await),
        auth_error::Kind::NotPermitted,
        "the next request after the change must already see it"
    );

    cleanup(&path);
}

/// Creating a group is not an administrator's privilege (it gives its
/// creator nothing new), but administering one is its own administrators'
/// business.
#[tokio::test]
async fn anyone_may_create_a_group_and_only_its_admins_may_change_it() {
    let (log, path) = open_temp();
    let svc = service_with_alice(log, "kv-test").await;
    created_seq(create_user(&svc, "root", "bob", "hunter42", false).await);

    must_create_group(&svc, "alice", "oncall", vec![]).await;

    // The creator administers it, whatever the request said.
    let group = match svc
        .get_group(as_user(
            "bob",
            GetGroupRequest {
                name: "oncall".to_string(),
            },
        ))
        .await
        .unwrap()
        .into_inner()
        .result
    {
        Some(get_group_response::Result::Ok(group)) => group,
        other => panic!("expected the group, got {other:?}"),
    };
    assert_eq!(group.admins, vec![pb_user("alice")]);
    assert_eq!(group.created_by, "alice");

    // Someone else's group is not bob's to change.
    let refused = update_group(&svc, "bob", "oncall", vec![pb_user("bob")], vec![]).await;
    assert!(
        matches!(&refused, update_group_response::Result::AuthError(e) if auth_kind(e) == auth_error::Kind::NotPermitted),
        "got {refused:?}"
    );
    assert!(matches!(
        update_group(&svc, "alice", "oncall", vec![pb_user("bob")], vec![]).await,
        update_group_response::Result::Seq(_)
    ));
    // A cluster administrator may always.
    assert!(matches!(
        update_group(&svc, "root", "oncall", vec![], vec![pb_user("bob")]).await,
        update_group_response::Result::Seq(_)
    ));

    cleanup(&path);
}

/// A cycle would make "who is in this group" unanswerable, so the edge
/// that would close one is refused where it is added.
#[tokio::test]
async fn a_group_cannot_be_put_inside_itself() {
    let (log, path) = open_temp();
    let svc = service_with_alice(log, "kv-test").await;

    must_create_group(&svc, "root", "a", vec![]).await;
    must_create_group(&svc, "root", "b", vec![pb_group("a")]).await;

    // `b` is already in `a`'s would-be ancestry: a contains nothing, b
    // contains a. Putting b into a closes the loop.
    assert!(
        try_update_group(&svc, "root", members_only("a", vec![pb_group("b")], vec![]))
            .await
            .is_err(),
        "a cycle should be refused"
    );
    // Directly, too.
    assert!(
        try_update_group(&svc, "root", members_only("a", vec![pb_group("a")], vec![]))
            .await
            .is_err()
    );
    // And at creation, against a group that already names the new one. Naming
    // a group that does not exist is allowed, so this is a cycle the new group
    // walks into, not one it could see coming.
    must_create_group(&svc, "root", "c", vec![pb_group("later")]).await;
    assert!(
        svc.create_group(as_user(
            "root",
            CreateGroupRequest {
                name: "later".to_string(),
                members: vec![pb_group("c")],
                admins: Vec::new(),
            },
        ))
        .await
        .is_err()
    );
    // An edge that closes no loop is fine.
    assert!(matches!(
        update_group(&svc, "root", "b", vec![pb_user("alice")], vec![]).await,
        update_group_response::Result::Seq(_)
    ));

    cleanup(&path);
}

/// A group with no administrators could never change again except by a
/// cluster administrator, so the call that would empty the list is refused,
/// instead of someone finding out later.
#[tokio::test]
async fn a_group_must_keep_at_least_one_administrator() {
    let (log, path) = open_temp();
    let svc = service_with_alice(log, "kv-test").await;

    must_create_group(&svc, "alice", "oncall", vec![]).await;
    assert!(
        try_update_group(
            &svc,
            "alice",
            UpdateGroupRequest {
                name: "oncall".to_string(),
                add_members: vec![],
                remove_members: vec![],
                add_admins: vec![],
                remove_admins: vec![pb_user("alice")],
            },
        )
        .await
        .is_err(),
        "emptying the administrator list should be refused"
    );

    // Handing it over is fine, because additions happen first.
    assert!(matches!(
        try_update_group(
            &svc,
            "alice",
            UpdateGroupRequest {
                name: "oncall".to_string(),
                add_members: vec![],
                remove_members: vec![],
                add_admins: vec![pb_user("root")],
                remove_admins: vec![pb_user("alice")],
            },
        )
        .await
        .unwrap(),
        update_group_response::Result::Seq(_)
    ));
    // And after handing it over, she is no longer the one who may.
    let refused = update_group(&svc, "alice", "oncall", vec![pb_user("alice")], vec![]).await;
    assert!(
        matches!(&refused, update_group_response::Result::AuthError(e) if auth_kind(e) == auth_error::Kind::NotPermitted),
        "got {refused:?}"
    );

    cleanup(&path);
}

/// A group that does not exist is `NO_SUCH_GROUP`, not `NO_SUCH_USER`: the
/// namespaces are separate, and a caller told the wrong one would look in
/// the wrong place.
#[tokio::test]
async fn a_missing_group_is_reported_as_a_missing_group() {
    let (log, path) = open_temp();
    let svc = service_with_alice(log, "kv-test").await;

    let result = svc
        .get_group(as_user(
            "alice",
            GetGroupRequest {
                name: "nope".to_string(),
            },
        ))
        .await
        .unwrap()
        .into_inner()
        .result;
    assert!(
        matches!(&result, Some(get_group_response::Result::AuthError(e)) if auth_kind(e) == auth_error::Kind::NoSuchGroup),
        "got {result:?}"
    );

    cleanup(&path);
}

// -- authority over the ACLs themselves --------------------------------

/// `write` on a prefix gives authority over that prefix's ACL and every
/// longer prefix's, and stops exactly there.
#[tokio::test]
async fn write_on_a_prefix_carries_authority_over_its_acls() {
    let (log, path) = open_temp();
    let svc = service_with_alice(log, "kv-test").await;

    must_set_acl(&svc, "root", b"app/", vec![grant(pb_user("alice"), PbRight::Write)]).await;

    // Longer: hers.
    must_set_acl(
        &svc,
        "alice",
        b"app/sub/",
        vec![grant(pb_user("alice"), PbRight::Write)],
    )
    .await;
    // The prefix itself: also hers, because her own ACL grants her write.
    must_set_acl(
        &svc,
        "alice",
        b"app/",
        vec![
            grant(pb_user("alice"), PbRight::Write),
            grant(pb_user("root"), PbRight::Write),
        ],
    )
    .await;
    // A sibling that nothing governs: not hers, and not because she lacks a
    // grant, but because nobody's grant reaches there.
    assert_eq!(
        acl_refusal(set_acl(&svc, "alice", b"app2/", vec![grant(pb_user("alice"), PbRight::Write)]).await),
        auth_error::Kind::NotPermitted
    );

    cleanup(&path);
}

/// `read` is not authority. Being able to see a subtree is not being able
/// to decide who else may.
#[tokio::test]
async fn read_on_a_prefix_is_not_authority_over_its_acls() {
    let (log, path) = open_temp();
    let svc = service_with_alice(log, "kv-test").await;

    must_set_acl(&svc, "root", b"app/", vec![grant(pb_user("alice"), PbRight::Read)]).await;
    assert_eq!(
        acl_refusal(set_acl(&svc, "alice", b"app/x", vec![grant(pb_user("alice"), PbRight::Write)]).await),
        auth_error::Kind::NotPermitted
    );

    cleanup(&path);
}

/// The hole this closes: a key nothing governs is open to everyone, and
/// without this rule any account could write an ACL on the empty prefix
/// and lock the cluster out of its own keyspace.
#[tokio::test]
async fn the_default_is_not_authority_to_write_an_acl() {
    let (log, path) = open_temp();
    let svc = service_with_alice(log, "kv-test").await;

    // Nothing governs anything yet, and `alice` can write every key, but she
    // still may not decide who else can.
    assert!(matches!(
        write_as(&svc, "alice", b"app/config", Some(b"v")).await,
        write_response::Result::Ok(true)
    ));
    assert_eq!(
        acl_refusal(set_acl(&svc, "alice", b"", vec![grant(pb_user("alice"), PbRight::Write)]).await),
        auth_error::Kind::NotPermitted
    );
    assert_eq!(
        acl_refusal(set_acl(&svc, "alice", b"app/", vec![grant(pb_user("alice"), PbRight::Write)]).await),
        auth_error::Kind::NotPermitted
    );
    // An administrator may, which is how the first ACL gets written.
    must_set_acl(&svc, "root", b"app/", vec![grant(pb_user("alice"), PbRight::Write)]).await;

    cleanup(&path);
}

/// The same rule from the other side: removing an ACL that no shorter one
/// would replace opens the subtree to everyone, so it is an administrator's
/// act, even for the account that holds write there.
#[tokio::test]
async fn removing_the_last_acl_over_a_prefix_is_an_administrators_act() {
    let (log, path) = open_temp();
    let svc = service_with_alice(log, "kv-test").await;

    must_set_acl(&svc, "root", b"app/", vec![grant(pb_user("alice"), PbRight::Write)]).await;
    must_set_acl(
        &svc,
        "root",
        b"app/sub/",
        vec![grant(pb_user("alice"), PbRight::Write)],
    )
    .await;

    // `app/sub/` falls back to `app/`, which still protects it: hers.
    assert!(matches!(
        delete_acl(&svc, "alice", b"app/sub/").await,
        delete_acl_response::Result::Seq(_)
    ));
    // `app/` falls back to nothing: not hers.
    let refused = delete_acl(&svc, "alice", b"app/").await;
    assert!(
        matches!(&refused, delete_acl_response::Result::AuthError(e) if auth_kind(e) == auth_error::Kind::NotPermitted),
        "got {refused:?}"
    );
    assert!(matches!(
        delete_acl(&svc, "root", b"app/").await,
        delete_acl_response::Result::Seq(_)
    ));
    // And once it is gone, the subtree is open again.
    assert!(matches!(
        read_as(&svc, "alice", b"app/config").await,
        read_response::Result::Ok(_)
    ));

    cleanup(&path);
}

/// A prefix with no ACL of its own is `NO_SUCH_ACL`, which is not the same
/// as "nothing governs the keys under it".
#[tokio::test]
async fn removing_an_acl_that_is_not_there_says_so() {
    let (log, path) = open_temp();
    let svc = service_with_alice(log, "kv-test").await;

    must_set_acl(&svc, "root", b"app/", vec![grant(pb_user("root"), PbRight::Write)]).await;
    let result = delete_acl(&svc, "root", b"app/sub/").await;
    assert!(
        matches!(&result, delete_acl_response::Result::AuthError(e) if auth_kind(e) == auth_error::Kind::NoSuchAcl),
        "got {result:?}"
    );

    cleanup(&path);
}

// -- scans --------------------------------------------------------------

/// Keys the caller may not read are left out of a walk instead of refusing
/// it, and because they are skipped inside the scan, not filtered after,
/// they use none of the batch.
#[tokio::test]
async fn a_scan_omits_what_the_caller_may_not_read_without_shortening_the_batch() {
    let (log, path) = open_temp();
    let svc = service_with_alice(log, "kv-test").await;

    for i in 0..10 {
        write(&svc, format!("hidden/{i:02}").as_bytes(), Some(b"v")).await;
        write(&svc, format!("open/{i:02}").as_bytes(), Some(b"v")).await;
    }
    must_set_acl(
        &svc,
        "root",
        b"hidden/",
        vec![grant(pb_user("root"), PbRight::Write)],
    )
    .await;

    // `hidden/` sorts before `open/`, so a naive filter applied afterwards
    // would return an empty first batch.
    let batch = get_keys_as(&svc, "alice", b"", 5).await;
    assert_eq!(
        as_strings(&batch.keys),
        vec!["open/00", "open/01", "open/02", "open/03", "open/04"]
    );
    assert!(batch.has_more);

    // An administrator sees everything, which also checks that the filter is
    // about the caller, not about the keys.
    let batch = get_keys_as(&svc, "root", b"", 5).await;
    assert_eq!(
        as_strings(&batch.keys),
        vec!["hidden/00", "hidden/01", "hidden/02", "hidden/03", "hidden/04"]
    );

    cleanup(&path);
}

/// A walk of a store the caller can see almost none of stops at its budget
/// and says where to resume, instead of becoming an O(keyspace) call. The
/// cursor is what matters: the batch can come back empty.
#[tokio::test]
async fn a_filtered_walk_pages_with_the_cursor_the_server_sends() {
    let (log, path) = open_temp();
    let svc = service_with_alice(log, "kv-test").await;

    for i in 0..20 {
        write(&svc, format!("hidden/{i:02}").as_bytes(), Some(b"v")).await;
    }
    write(&svc, b"zzz-visible", Some(b"v")).await;
    must_set_acl(
        &svc,
        "root",
        b"hidden/",
        vec![grant(pb_user("root"), PbRight::Write)],
    )
    .await;

    // Walk it as a client does, following the cursor. Without one, an empty
    // batch would end the walk, and `zzz-visible` would never be reached.
    let mut seen: Vec<Vec<u8>> = Vec::new();
    let mut start = Vec::new();
    loop {
        let batch = get_keys_as(&svc, "alice", &start, 5).await;
        seen.extend(batch.keys.iter().cloned());
        if !batch.has_more {
            break;
        }
        start = if batch.next_start_key.is_empty() {
            let mut next = batch.keys.last().expect("a full batch has a last key").clone();
            next.push(0);
            next
        } else {
            batch.next_start_key
        };
    }
    assert_eq!(as_strings(&seen), vec!["zzz-visible"]);

    cleanup(&path);
}

// -- reading the ACLs ---------------------------------------------------

/// The question a caller with a refusal has: which ACL decided, and what
/// does it give me?
#[tokio::test]
async fn get_acl_says_which_prefix_governs_and_what_it_grants() {
    let (log, path) = open_temp();
    let svc = service_with_alice(log, "kv-test").await;

    must_create_group(&svc, "root", "ops", vec![pb_user("alice")]).await;
    must_set_acl(
        &svc,
        "root",
        b"app/",
        vec![grant(pb_group("ops"), PbRight::Read)],
    )
    .await;

    let ok = match svc
        .get_acl(as_user(
            "alice",
            GetAclRequest {
                key: b"app/config".to_vec(),
            },
        ))
        .await
        .unwrap()
        .into_inner()
        .result
    {
        Some(get_acl_response::Result::Ok(ok)) => ok,
        other => panic!("expected an answer, got {other:?}"),
    };
    let acl = ok.acl.expect("an ACL governs that key");
    assert_eq!(acl.prefix, b"app/".to_vec());
    // Granted through the group, which `effective` saves a client from
    // working out itself.
    assert_eq!(ok.effective, Some(PbRight::Read as i32));

    // A key nothing governs: no ACL, and no grant to report.
    let ok = match svc
        .get_acl(as_user(
            "alice",
            GetAclRequest {
                key: b"other".to_vec(),
            },
        ))
        .await
        .unwrap()
        .into_inner()
        .result
    {
        Some(get_acl_response::Result::Ok(ok)) => ok,
        other => panic!("expected an answer, got {other:?}"),
    };
    assert_eq!(ok.acl, None);
    assert_eq!(ok.effective, None);

    cleanup(&path);
}

/// Anyone with an account can read ACLs and groups: they say who may reach
/// which keys, which is no secret from the people they are about, and a
/// caller who could not read them could not tell a refusal from a bug.
#[tokio::test]
async fn any_account_may_read_the_acls_and_the_groups() {
    let (log, path) = open_temp();
    let svc = service_with_alice(log, "kv-test").await;

    must_create_group(&svc, "root", "ops", vec![]).await;
    must_set_acl(&svc, "root", b"b/", vec![grant(pb_user("root"), PbRight::Write)]).await;
    must_set_acl(&svc, "root", b"a/", vec![grant(pb_user("root"), PbRight::Write)]).await;

    let acls = match svc
        .list_acls(as_user("alice", ListAclsRequest {}))
        .await
        .unwrap()
        .into_inner()
        .result
    {
        Some(list_acls_response::Result::Ok(list)) => list.acls,
        other => panic!("expected a listing, got {other:?}"),
    };
    // Shortest prefix first: the order they take effect.
    assert_eq!(
        acls.iter().map(|a| a.prefix.clone()).collect::<Vec<_>>(),
        vec![b"a/".to_vec(), b"b/".to_vec()]
    );

    let groups = match svc
        .list_groups(as_user("alice", ListGroupsRequest {}))
        .await
        .unwrap()
        .into_inner()
        .result
    {
        Some(list_groups_response::Result::Ok(list)) => list.groups,
        other => panic!("expected a listing, got {other:?}"),
    };
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].name, "ops");

    cleanup(&path);
}

// -- the reserved range -------------------------------------------------

/// Groups and ACLs live in the reserved range, so no client can reach them
/// through the data path: `reject_reserved` covers their prefixes too.
#[tokio::test]
async fn the_permission_records_are_not_reachable_as_keys() {
    let (log, path) = open_temp();
    let svc = service_with_alice(log, "kv-test").await;

    must_create_group(&svc, "root", "ops", vec![]).await;
    must_set_acl(&svc, "root", b"app/", vec![grant(pb_user("root"), PbRight::Write)]).await;

    for key in [
        crate::keyspace::group_key("ops"),
        crate::keyspace::acl_key(b"app/"),
        crate::keyspace::EPOCH_KEY.to_vec(),
    ] {
        assert!(
            svc.read(as_user("root", ReadRequest { keys: vec![key.clone()], versions_only: false, consistency: None }))
                .await
                .is_err(),
            "reading {key:?} should be refused outright"
        );
        let txn = begin_txn(&svc, 5000).await;
        assert!(
            svc.write(as_user(
                "root",
                WriteRequest {
                    transaction_id: txn,
                    key: key.clone(),
                    value: Some(b"x".to_vec()),
                },
            ))
            .await
            .is_err(),
            "writing {key:?} should be refused outright"
        );
    }
    // Nor do they appear in a walk.
    assert!(
        get_keys_as(&svc, "root", b"", 100)
            .await
            .keys
            .is_empty(),
        "the reserved range must not show up in a scan"
    );

    cleanup(&path);
}

// --- many-key reads -----------------------------------------------------

/// A read of `keys`, as `svc` answers it.
async fn read_keys(
    svc: &KvService,
    keys: &[&[u8]],
    versions_only: bool,
    consistency: Option<read_request::Consistency>,
) -> Result<read_response::Result, Status> {
    Ok(svc
        .read(Request::new(ReadRequest {
            keys: keys.iter().map(|key| key.to_vec()).collect(),
            versions_only,
            consistency,
        }))
        .await?
        .into_inner()
        .result
        .expect("a read always answers"))
}

/// `read_keys`, for reads a test expects to succeed.
async fn read_ok_keys(
    svc: &KvService,
    keys: &[&[u8]],
    versions_only: bool,
    consistency: Option<read_request::Consistency>,
) -> ReadOk {
    match read_keys(svc, keys, versions_only, consistency).await {
        Ok(read_response::Result::Ok(ok)) => ok,
        other => panic!("expected a successful read, got {other:?}"),
    }
}

fn linearizable() -> Option<read_request::Consistency> {
    Some(read_request::Consistency::Linearizable(Linearizable {}))
}

fn in_txn(txn: &[u8]) -> Option<read_request::Consistency> {
    Some(read_request::Consistency::TransactionId(txn.to_vec()))
}

fn kv(seq: u64, value: Option<&[u8]>) -> KeyVersion {
    KeyVersion {
        seq,
        value: value.map(<[u8]>::to_vec),
    }
}

/// By position: every key answered where it was named, a duplicate at each
/// of its positions, a missing key at seq 0, and `as_of` at least as new as
/// anything returned.
#[tokio::test]
async fn a_read_answers_every_key_in_the_order_it_was_named() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;
    let a = write(&svc, b"a", Some(b"1")).await;
    let b = write(&svc, b"b", Some(b"2")).await;

    for consistency in [None, linearizable()] {
        let ok = read_ok_keys(&svc, &[b"b", b"missing", b"a", b"b"], false, consistency).await;
        assert_eq!(
            ok.entries,
            vec![kv(b, Some(b"2")), kv(0, None), kv(a, Some(b"1")), kv(b, Some(b"2"))]
        );
        assert!(ok.as_of >= b, "as_of {} is older than what it read", ok.as_of);
    }

    cleanup(&path);
}

/// `versions_only` returns the same seqs as a full read, and no values.
#[tokio::test]
async fn a_versions_only_read_agrees_with_a_full_one_without_the_values() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;
    write(&svc, b"a", Some(b"1")).await;
    write(&svc, b"b", Some(b"2")).await;
    let keys: [&[u8]; 3] = [b"a", b"b", b"missing"];

    let full = read_ok_keys(&svc, &keys, false, None).await;
    let versions = read_ok_keys(&svc, &keys, true, None).await;
    assert_eq!(versions.as_of, full.as_of);
    let seqs = |ok: &ReadOk| ok.entries.iter().map(|e| e.seq).collect::<Vec<_>>();
    assert_eq!(seqs(&versions), seqs(&full));
    assert!(versions.entries.iter().all(|e| e.value.is_none()));

    cleanup(&path);
}

/// The guarantee the whole call exists for: keys written together are
/// never read apart, however the read interleaves with commits (stale or
/// linearizable), and `as_of` is never older than what was read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keys_committed_together_are_always_read_together() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;

    let writer = async {
        for round in 0..=40u32 {
            let txn = begin_txn(&svc, 5000).await;
            let value = round.to_string();
            for key in [&b"a"[..], b"b"] {
                assert!(matches!(
                    buffer(&svc, &txn, key, Some(value.as_bytes())).await,
                    write_response::Result::Ok(true)
                ));
            }
            commit(&svc, txn).await;
        }
    };
    let reader = |consistency: Option<read_request::Consistency>| {
        let svc = &svc;
        async move {
            for _ in 0..200 {
                let ok = read_ok_keys(svc, &[b"a", b"b"], false, consistency.clone()).await;
                // Before the first commit both are absent, which also counts as "the
                // same".
                let [a, b] = ok.entries.as_slice() else {
                    panic!("two keys, two entries: {ok:?}");
                };
                assert_eq!(a, b, "a and b are only ever written together");
                assert!(ok.as_of >= a.seq);
                tokio::task::yield_now().await;
            }
        }
    };
    tokio::join!(writer, reader(None), reader(linearizable()));

    cleanup(&path);
}

/// In a transaction: its own writes on top of committed state, at the
/// committed seq, and every named key held against a younger writer.
#[tokio::test]
async fn a_transactional_read_overlays_its_own_writes_and_locks_every_key() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;
    let a = write(&svc, b"a", Some(b"1")).await;
    let b = write(&svc, b"b", Some(b"2")).await;
    let c = write(&svc, b"c", Some(b"3")).await;

    let txn = begin_txn(&svc, 5000).await;
    buffer(&svc, &txn, b"a", Some(b"mine")).await;
    buffer(&svc, &txn, b"b", None).await;

    let keys: [&[u8]; 4] = [b"a", b"b", b"c", b"new"];
    let ok = read_ok_keys(&svc, &keys, false, in_txn(&txn)).await;
    assert_eq!(
        ok.entries,
        vec![kv(a, Some(b"mine")), kv(b, None), kv(c, Some(b"3")), kv(0, None)]
    );
    let versions = read_ok_keys(&svc, &keys, true, in_txn(&txn)).await;
    assert_eq!(
        versions.entries,
        vec![kv(a, None), kv(b, None), kv(c, None), kv(0, None)],
        "a buffered key reports the seq of what it replaces"
    );

    let younger = begin_txn(&svc, 5000).await;
    for key in [&b"c"[..], b"new"] {
        match buffer(&svc, &younger, key, Some(b"theirs")).await {
            write_response::Result::Error(PbRaftError {
                kind: Some(raft_error::Kind::Aborted(abort)),
            }) => assert_eq!(abort.reason, transaction_aborted::Reason::LockConflict as i32),
            other => panic!("expected a lock conflict on {key:?}, got {other:?}"),
        }
    }

    cleanup(&path);
}

/// At a snapshot: as of its seq, with `as_of` saying so. A snapshot nobody
/// created is not found; an empty id is malformed.
#[tokio::test]
async fn a_read_at_a_snapshot_reads_every_key_as_of_it() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;
    let a = write(&svc, b"a", Some(b"old")).await;
    let snap = make_snapshot(&svc, b"s1").await;
    write(&svc, b"a", Some(b"new")).await;
    write(&svc, b"b", Some(b"fresh")).await;

    let at = Some(read_request::Consistency::SnapshotId(b"s1".to_vec()));
    let ok = read_ok_keys(&svc, &[b"a", b"b"], false, at).await;
    assert_eq!(ok.as_of, snap.seq);
    assert_eq!(ok.entries, vec![kv(a, Some(b"old")), kv(0, None)]);

    let missing = Some(read_request::Consistency::SnapshotId(b"nope".to_vec()));
    let status = read_keys(&svc, &[b"a"], false, missing).await.unwrap_err();
    assert_eq!(status.code(), tonic::Code::NotFound);
    let empty = Some(read_request::Consistency::SnapshotId(Vec::new()));
    let status = read_keys(&svc, &[b"a"], false, empty).await.unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);

    cleanup(&path);
}

/// More keys than one read may name is refused, not shortened: the caller
/// chose the number.
#[tokio::test]
async fn a_read_of_too_many_keys_is_refused() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;
    let keys: Vec<Vec<u8>> = (0..=raft5_shared::MAX_KEYS_PER_READ)
        .map(|i| format!("k{i}").into_bytes())
        .collect();
    let keys: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
    let status = read_keys(&svc, &keys, true, None).await.unwrap_err();
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);

    cleanup(&path);
}

/// Past the value budget, the answer is a prefix of the keys, never empty,
/// so a reissue for the rest makes progress. Versions alone are never cut.
#[tokio::test]
async fn a_read_past_the_value_budget_answers_a_prefix() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;
    let big = vec![9u8; 6 * 1024 * 1024];
    for key in [&b"a"[..], b"b", b"c"] {
        write(&svc, key, Some(&big)).await;
    }

    let keys: [&[u8]; 3] = [b"a", b"b", b"c"];
    let mut rest = &keys[..];
    let mut calls = 0;
    while !rest.is_empty() {
        let ok = read_ok_keys(&svc, rest, false, None).await;
        assert_eq!(ok.entries.len(), 1, "two 6 MiB values pass the 10 MiB budget");
        assert_eq!(ok.entries[0].value.as_deref(), Some(big.as_slice()));
        rest = &rest[ok.entries.len()..];
        calls += 1;
    }
    assert_eq!(calls, 3);

    assert_eq!(read_ok_keys(&svc, &keys, true, None).await.entries.len(), 3);

    cleanup(&path);
}

/// In a transaction, a shortened read has still locked every key, so the
/// reissue sees the same state, and a buffered value counts toward the
/// budget like a committed one.
#[tokio::test]
async fn a_shortened_transactional_read_still_holds_the_keys_it_left_out() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;
    let big = vec![9u8; 6 * 1024 * 1024];
    write(&svc, b"b", Some(&big)).await;

    let txn = begin_txn(&svc, 10_000).await;
    buffer(&svc, &txn, b"a", Some(&big)).await;
    let ok = read_ok_keys(&svc, &[b"a", b"b"], false, in_txn(&txn)).await;
    assert_eq!(ok.entries.len(), 1, "the buffered 6 MiB counts toward the budget");

    let younger = begin_txn(&svc, 5000).await;
    assert!(matches!(
        buffer(&svc, &younger, b"b", Some(b"theirs")).await,
        write_response::Result::Error(_)
    ), "b was locked though it was not answered");

    let rest = read_ok_keys(&svc, &[b"b"], false, in_txn(&txn)).await;
    assert_eq!(rest.entries[0].value.as_deref(), Some(big.as_slice()));

    cleanup(&path);
}

/// A reserved key anywhere in the list refuses the whole read.
#[tokio::test]
async fn a_reserved_key_anywhere_refuses_the_whole_read() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;
    write(&svc, b"a", Some(b"1")).await;
    let reserved = crate::keyspace::user_key("alice");

    let status = read_keys(&svc, &[b"a", &reserved], true, None).await.unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);

    cleanup(&path);
}

/// One key the caller may not read refuses the whole read, and names the
/// key by position; an administrator is outside the ACL.
#[tokio::test]
async fn one_denied_key_refuses_the_whole_read_and_says_which() {
    let (log, path) = open_temp();
    let svc = service_with_alice(log, "kv-test").await;
    must_set_acl(&svc, "root", b"secret/", vec![grant(pb_user("root"), PbRight::Write)]).await;

    let request = |caller| {
        as_user(
            caller,
            ReadRequest {
                keys: vec![b"open".to_vec(), b"secret/x".to_vec()],
                versions_only: true,
                consistency: None,
            },
        )
    };
    let refused = svc.read(request("alice")).await.unwrap().into_inner().result;
    match refused {
        Some(read_response::Result::AuthError(e)) => {
            assert_eq!(auth_kind(&e), auth_error::Kind::NotPermitted);
            assert!(e.detail.contains("keys[1]"), "{}", e.detail);
        }
        other => panic!("expected the read to be refused, got {other:?}"),
    }
    let admin = svc.read(request("root")).await.unwrap().into_inner().result;
    assert!(matches!(admin, Some(read_response::Result::Ok(_))), "{admin:?}");

    cleanup(&path);
}

// --- backups ------------------------------------------------------------

/// What a `Backup` call streamed: the bytes, or the refusal it sent as its
/// only message.
async fn take_backup(
    svc: &KvService,
    request: Request<BackupRequest>,
) -> Result<Result<Vec<u8>, PbAuthError>, Status> {
    use tokio_stream::StreamExt;
    let mut stream = svc.backup(request).await?.into_inner();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        match chunk?.result {
            Some(backup_chunk::Result::Data(data)) => bytes.extend_from_slice(&data),
            Some(backup_chunk::Result::AuthError(e)) => return Ok(Err(e)),
            None => panic!("an empty backup chunk"),
        }
    }
    Ok(Ok(bytes))
}

/// A new store holding `backup`, as a restored node would.
fn restored(backup: &[u8]) -> (RaftLog, std::path::PathBuf) {
    let (_, path) = open_temp();
    let log = RaftLog::open(path.join("restored")).expect("open");
    match log.restore_backup(&mut &backup[..], b"cfg").unwrap() {
        crate::storage::RestoreOutcome::Restored { .. } => {}
        other => panic!("expected the backup to restore, got {other:?}"),
    }
    (log, path)
}

/// On a node with no accounts, anyone may take a backup. It holds the
/// client's keys and leaves out snapshots.
#[tokio::test]
async fn a_backup_holds_the_keys_and_not_the_snapshots() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;
    let a = write(&svc, b"a", Some(b"1")).await;
    make_snapshot(&svc, b"s1").await;
    let b = write(&svc, b"b", Some(b"2")).await;

    let backup = take_backup(&svc, Request::new(BackupRequest::default()))
        .await
        .unwrap()
        .expect("no accounts, so no refusal");
    let (copy, copy_path) = restored(&backup);
    assert_eq!(copy.get_latest(b"a").unwrap(), Some((a, b"1".to_vec())));
    assert_eq!(copy.get_latest(b"b").unwrap(), Some((b, b"2".to_vec())));
    assert_eq!(copy.checkpoint_seq(b"s1").unwrap(), None);

    cleanup(&path);
    cleanup(&copy_path);
}

/// A backup at a snapshot shows the store as it was then. A snapshot nobody
/// created is `not_found`, before any byte is sent.
#[tokio::test]
async fn a_backup_at_a_snapshot_reads_as_of_it() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;
    let old = write(&svc, b"k", Some(b"old")).await;
    make_snapshot(&svc, b"s1").await;
    write(&svc, b"k", Some(b"new")).await;

    let at = BackupRequest {
        snapshot_id: b"s1".to_vec(),
    };
    let backup = take_backup(&svc, Request::new(at)).await.unwrap().unwrap();
    let (copy, copy_path) = restored(&backup);
    assert_eq!(copy.get_latest(b"k").unwrap(), Some((old, b"old".to_vec())));

    let missing = BackupRequest {
        snapshot_id: b"nope".to_vec(),
    };
    let status = take_backup(&svc, Request::new(missing)).await.unwrap_err();
    assert_eq!(status.code(), tonic::Code::NotFound);

    cleanup(&path);
    cleanup(&copy_path);
}

/// An account that is neither an administrator nor a backup operator may
/// not take a backup: it holds every account's password hash. What it
/// carries from the reserved range is accounts, groups, ACLs and the
/// permission epoch, and not invitations.
#[tokio::test]
async fn a_backup_is_for_administrators_and_carries_permissions() {
    let (log, path) = open_temp();
    let svc = service_with_alice(log, "kv-test").await;
    must_create_group(&svc, "root", "ops", vec![pb_user("alice")]).await;
    must_set_acl(&svc, "root", b"app/", vec![grant(pb_user("root"), PbRight::Write)]).await;
    let (_, invite_id) = invite(&svc).await;

    let refused = take_backup(&svc, as_user("alice", BackupRequest::default()))
        .await
        .unwrap()
        .expect_err("alice is not an administrator");
    assert_eq!(auth_kind(&refused), auth_error::Kind::NotAdmin);

    let backup = take_backup(&svc, as_user("root", BackupRequest::default()))
        .await
        .unwrap()
        .expect("root is an administrator");
    let (copy, copy_path) = restored(&backup);
    for key in [
        crate::keyspace::user_key("alice"),
        crate::keyspace::user_key("root"),
        crate::keyspace::group_key("ops"),
        crate::keyspace::acl_key(b"app/"),
        crate::keyspace::EPOCH_KEY.to_vec(),
    ] {
        assert!(copy.get_latest(&key).unwrap().is_some(), "{key:?} is missing");
    }
    assert_eq!(copy.get_latest(&crate::keyspace::invite_key(&invite_id)).unwrap(), None);

    cleanup(&path);
    cleanup(&copy_path);
}

// --- backup operators -----------------------------------------------------

async fn set_backup_operators(
    svc: &KvService,
    caller: &str,
    principals: Vec<PbPrincipal>,
) -> set_backup_operators_response::Result {
    svc.set_backup_operators(as_user(caller, SetBackupOperatorsRequest { principals }))
        .await
        .unwrap()
        .into_inner()
        .result
        .expect("a set_backup_operators response always carries a result")
}

async fn snapshot_as(svc: &KvService, caller: &str, id: &[u8]) -> create_snapshot_response::Result {
    svc.create_snapshot(as_user(
        caller,
        CreateSnapshotRequest {
            snapshot_id: id.to_vec(),
            created_unix_ms: 0,
        },
    ))
    .await
    .unwrap()
    .into_inner()
    .result
    .expect("a create_snapshot response always carries a result")
}

async fn drop_snapshot_as(
    svc: &KvService,
    caller: &str,
    id: &[u8],
) -> remove_snapshot_response::Result {
    svc.remove_snapshot(as_user(
        caller,
        RemoveSnapshotRequest {
            snapshot_id: id.to_vec(),
        },
    ))
    .await
    .unwrap()
    .into_inner()
    .result
    .expect("a remove_snapshot response always carries a result")
}

/// A backup operator, named directly or through a group, may create and
/// remove snapshots and take backups. Anyone else who is not an
/// administrator may not. The list travels with the backup.
#[tokio::test]
async fn a_backup_operator_may_snapshot_and_back_up() {
    let (log, path) = open_temp();
    let svc = service_with_alice(log, "kv-test").await;
    created_seq(create_user(&svc, "root", "bob", "hunter42", false).await);
    created_seq(create_user(&svc, "root", "carol", "hunter42", false).await);
    must_create_group(&svc, "root", "backup", vec![pb_user("carol")]).await;
    assert!(matches!(
        set_backup_operators(&svc, "root", vec![pb_user("bob"), pb_group("backup")]).await,
        set_backup_operators_response::Result::Seq(_)
    ));

    match snapshot_as(&svc, "alice", b"s1").await {
        create_snapshot_response::Result::AuthError(e) => {
            assert_eq!(auth_kind(&e), auth_error::Kind::NotAdmin);
        }
        other => panic!("alice is not a backup operator, got {other:?}"),
    }
    assert!(matches!(
        snapshot_as(&svc, "bob", b"s1").await,
        create_snapshot_response::Result::Ok(_)
    ));

    let at_s1 = || BackupRequest {
        snapshot_id: b"s1".to_vec(),
    };
    let refused = take_backup(&svc, as_user("alice", at_s1()))
        .await
        .unwrap()
        .expect_err("alice is not a backup operator");
    assert_eq!(auth_kind(&refused), auth_error::Kind::NotAdmin);
    let backup = take_backup(&svc, as_user("carol", at_s1()))
        .await
        .unwrap()
        .expect("carol is a backup operator through her group");

    match drop_snapshot_as(&svc, "alice", b"s1").await {
        remove_snapshot_response::Result::AuthError(e) => {
            assert_eq!(auth_kind(&e), auth_error::Kind::NotAdmin);
        }
        other => panic!("alice is not a backup operator, got {other:?}"),
    }
    assert!(matches!(
        drop_snapshot_as(&svc, "carol", b"s1").await,
        remove_snapshot_response::Result::Seq(_)
    ));

    let (copy, copy_path) = restored(&backup);
    assert!(
        copy.get_latest(crate::keyspace::BACKUP_OPERATORS_KEY)
            .unwrap()
            .is_some()
    );

    cleanup(&path);
    cleanup(&copy_path);
}

/// Only an administrator may change the list, anyone signed in may read
/// it, and an empty list takes the right away.
#[tokio::test]
async fn only_an_administrator_chooses_the_backup_operators() {
    let (log, path) = open_temp();
    let svc = service_with_alice(log, "kv-test").await;

    match set_backup_operators(&svc, "alice", vec![pb_user("alice")]).await {
        set_backup_operators_response::Result::AuthError(e) => {
            assert_eq!(auth_kind(&e), auth_error::Kind::NotAdmin);
        }
        other => panic!("alice is not an administrator, got {other:?}"),
    }
    let seq = match set_backup_operators(&svc, "root", vec![pb_user("alice")]).await {
        set_backup_operators_response::Result::Seq(seq) => seq,
        other => panic!("root is an administrator, got {other:?}"),
    };

    let listed = svc
        .get_backup_operators(as_user("alice", GetBackupOperatorsRequest {}))
        .await
        .unwrap()
        .into_inner()
        .result;
    match listed {
        Some(get_backup_operators_response::Result::Ok(ok)) => {
            assert_eq!(ok.principals, vec![pb_user("alice")]);
            assert_eq!(ok.seq, seq);
        }
        other => panic!("expected the list, got {other:?}"),
    }
    assert!(matches!(
        snapshot_as(&svc, "alice", b"s1").await,
        create_snapshot_response::Result::Ok(_)
    ));

    assert!(matches!(
        set_backup_operators(&svc, "root", vec![]).await,
        set_backup_operators_response::Result::Seq(_)
    ));
    assert!(matches!(
        snapshot_as(&svc, "alice", b"s2").await,
        create_snapshot_response::Result::AuthError(_)
    ));

    cleanup(&path);
}
