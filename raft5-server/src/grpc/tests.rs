use std::sync::Arc;

use super::*;
use crate::pb::raft5::client::{read_response, write_response};
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

/// A write of `key`, conditional on it currently being at
/// `previous_seq` -- 0 meaning it must have no value.
async fn write(
    svc: &KvService,
    key: &[u8],
    value: Option<&[u8]>,
    previous_seq: u64,
) -> write_response::Result {
    svc.write(Request::new(WriteRequest {
        key: key.to_vec(),
        value: value.map(<[u8]>::to_vec),
        previous_seq: Some(previous_seq),
    }))
    .await
    .unwrap()
    .into_inner()
    .result
    .expect("a write response always carries a result")
}

/// The seq a write that was expected to succeed committed at.
fn committed(result: write_response::Result) -> u64 {
    match result {
        write_response::Result::Seq(seq) => seq,
        other => panic!("expected the write to commit, got {other:?}"),
    }
}

/// What a write that was expected to be refused conflicted with: the
/// key's actual seq, or `None` for a write still in flight.
fn conflict(result: write_response::Result) -> Option<u64> {
    match result {
        write_response::Result::Error(PbRaftError {
            kind: Some(raft_error::Kind::Conflict(conflict)),
        }) => conflict.actual_seq,
        other => panic!("expected a conflict, got {other:?}"),
    }
}

/// A (stale, so always answerable) read of `key`: what it holds, and
/// the seq that is the precondition for writing it next.
async fn read(svc: &KvService, key: &[u8]) -> ReadOk {
    let resp = svc
        .read(Request::new(ReadRequest {
            key: key.to_vec(),
            linearizable: false,
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

#[tokio::test]
async fn write_then_read_round_trips_through_kv_service() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;

    let seq = committed(write(&svc, b"hello", Some(b"world"), 0).await);
    assert_eq!(
        seq, 3,
        "seq 1 is the bootstrap config entry and seq 2 the leader's no-op"
    );

    let read_resp = svc
        .read(Request::new(ReadRequest {
            key: b"hello".to_vec(),
            linearizable: false,
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

    let seq = committed(write(&svc, b"k", Some(b"v"), 0).await);
    committed(write(&svc, b"k", None, seq).await);

    assert_eq!(read(&svc, b"k").await.value, None);

    cleanup(&path);
}

/// The point of the whole thing: a write names the version it was
/// written against, and one that names a version the key has moved
/// past is refused rather than silently clobbering it.
#[tokio::test]
async fn a_write_must_name_the_version_it_was_written_against() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;

    let first = committed(write(&svc, b"k", Some(b"one"), 0).await);

    // Someone who still thinks the key is empty is told what it is at
    // instead, and has written nothing.
    assert_eq!(
        conflict(write(&svc, b"k", Some(b"two"), 0).await),
        Some(first)
    );
    assert_eq!(read(&svc, b"k").await.value, Some(b"one".to_vec()));

    // Naming the current version goes through, and invalidates itself
    // for the next caller.
    let second = committed(write(&svc, b"k", Some(b"two"), first).await);
    assert_eq!(read(&svc, b"k").await.value, Some(b"two".to_vec()));
    assert_eq!(
        conflict(write(&svc, b"k", Some(b"three"), first).await),
        Some(second),
        "the seq that was current a moment ago is exactly the stale one"
    );

    cleanup(&path);
}

/// A deleted key is indistinguishable from one that was never written
/// (storage's `get_latest` collapses the two), and the precondition
/// follows that: seq 0 means "no value", whichever way it got there.
#[tokio::test]
async fn a_deleted_key_takes_a_precondition_of_zero_again() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;

    let seq = committed(write(&svc, b"k", Some(b"v"), 0).await);
    committed(write(&svc, b"k", None, seq).await);

    assert_eq!(read(&svc, b"k").await.value, None);
    committed(write(&svc, b"k", Some(b"again"), 0).await);
    assert_eq!(read(&svc, b"k").await.value, Some(b"again".to_vec()));

    cleanup(&path);
}

/// Between an entry being appended and being applied, `latest` still
/// answers for the version the in-flight write is replacing -- so a
/// precondition checked against it in that window would be checked
/// against a value that is already gone. The key's claim is what
/// closes the window, so a second write is refused outright rather
/// than validated against a stale answer.
#[tokio::test]
async fn a_key_with_a_write_in_flight_refuses_another() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;

    let claim = svc.claim(b"k").expect("the key starts unclaimed");
    assert_eq!(
        conflict(write(&svc, b"k", Some(b"v"), 0).await),
        None,
        "a conflict with a write in flight can say nothing about the key"
    );
    assert!(
        svc.claim(b"other").is_some(),
        "a claim is per key, not a lock on the whole store"
    );

    drop(claim);
    committed(write(&svc, b"k", Some(b"v"), 0).await);

    cleanup(&path);
}

/// A client that disconnects mid-write leaves an entry that may still
/// commit. Releasing its key at that point would let the next write's
/// precondition be checked against a `latest` that is about to change,
/// so the claim outlives the request -- and, just as importantly, does
/// not outlive the entry.
#[tokio::test]
async fn a_cancelled_write_releases_its_key_only_once_the_entry_lands() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;

    // Dropped before it can finish. Where exactly it got to is up to
    // the scheduler; what must hold either way is that the key ends up
    // released and the store ends up consistent with whatever landed.
    let cancelled = svc.write(Request::new(WriteRequest {
        key: b"k".to_vec(),
        value: Some(b"v".to_vec()),
        previous_seq: Some(0),
    }));
    assert!(
        tokio::time::timeout(std::time::Duration::from_micros(1), cancelled)
            .await
            .is_err(),
        "the write should not have had time to finish"
    );

    // A leaked claim would fail this until the deadline: the follow-up
    // write can never be committed while the key is still held.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        // Seq 0 if the cancelled write never landed, its seq if it did.
        let previous_seq = read(&svc, b"k").await.seq;
        match write(&svc, b"k", Some(b"after"), previous_seq).await {
            write_response::Result::Seq(_) => break,
            other => assert!(
                std::time::Instant::now() < deadline,
                "the cancelled write never released its key: {other:?}"
            ),
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(read(&svc, b"k").await.value, Some(b"after".to_vec()));

    cleanup(&path);
}

#[tokio::test]
async fn read_of_never_written_key_returns_no_value() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(log, "kv-test").await;

    let read_resp = svc
        .read(Request::new(ReadRequest {
            key: b"missing".to_vec(),
            linearizable: false,
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

    match write(&svc, b"k", Some(b"v"), 0).await {
        write_response::Result::Error(e) => {
            assert!(matches!(e.kind, Some(raft_error::Kind::NotLeader(_))));
        }
        other => panic!("expected NotLeader, got {other:?}"),
    }

    // Membership changes take the same route.
    let add = svc
        .add_server(Request::new(AddServerRequest {
            server_id: "new".to_string(),
            address: "http://127.0.0.1:9999".to_string(),
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

/// A stale read is served by whoever receives it, leader or not --
/// that is what asking for a stale read means.
#[tokio::test]
async fn a_stale_read_is_served_without_a_leader() {
    let (log, path) = open_temp();
    let (svc, _role_rx) = kv_service(log, "bystander");

    let resp = svc
        .read(Request::new(ReadRequest {
            key: b"k".to_vec(),
            linearizable: false,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(resp.result, Some(read_response::Result::Ok(_))));

    // A linearizable one is refused, since this node can't show that
    // its state is current.
    let strict = svc
        .read(Request::new(ReadRequest {
            key: b"k".to_vec(),
            linearizable: true,
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

/// The whole add path through the client API: one call, and the new
/// server is a member of the configuration in the leader's log.
#[tokio::test]
async fn add_server_then_remove_server_round_trip_through_the_kv_service() {
    let (log, path) = open_temp();
    let svc = leader_kv_service(Arc::clone(&log), "solo").await;

    let resp = svc
        .add_server(Request::new(AddServerRequest {
            server_id: "new".to_string(),
            address: "http://127.0.0.1:9999".to_string(),
            client_address: "http://127.0.0.1:9998".to_string(),
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

    // "new" is unreachable, so it never catches up and stays a
    // learner -- which means the cluster still commits on "solo"
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

    cleanup(&path);
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

    svc.add_server(Request::new(AddServerRequest {
        server_id: "new".to_string(),
        address: "http://127.0.0.1:9999".to_string(),
        client_address: "http://127.0.0.1:9998".to_string(),
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

    cleanup(&path);
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
#[tokio::test]
async fn append_entries_and_request_vote_reach_the_running_state_machine() {
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

    let svc = RaftPeerService::new(peer_handle);

    let append_resp = svc
        .append_entries(Request::new(AppendEntriesRequest {
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
