use super::*;
// Only the tests use the replication step builder directly; the leader
// itself only sees it from inside a replication task.
use super::replication::{ReplicationStep, build_replication_step_sync};
use crate::raft::test_support::*;

/// A cluster with no peers reaches a majority on its self-vote alone (see
/// the immediate-majority check in `candidate::Campaign::start`), and then
/// has no peers to wait on for commit either. `Event::LeaderTick` notices
/// the leader's growing log and advances `commit_tx` with no peer response
/// to trigger it.
#[tokio::test]
async fn leader_commits_a_write_immediately_in_a_single_node_cluster() {
    let (mut role_rx, _peer_handle, path) = spawn_test_node(vec![]);
    wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Leader(_))).await;
    let leader = expect_leader_handle(&role_rx.borrow());

    let seq = tokio::time::timeout(
        Duration::from_millis(500),
        leader.append_log(b"k", Some(b"v")),
    )
    .await
    .expect("append_log should not hang with no peers to wait on")
    .unwrap();
    // 2, not 1: seq 1 is the term-opening no-op that `Leader::new` appends.
    assert_eq!(seq, 2);

    cleanup(&path);
}

/// Raft §5.4.2: a leader can only advance its commit index over an entry
/// from its own term, so every term must start with one. Otherwise a leader
/// that never gets a client write could never commit its inherited tail.
#[tokio::test]
async fn a_new_leader_opens_its_term_with_a_noop() {
    let (log, mut role_rx, _peer_handle, _commit_rx, _compact_rx, path) =
        spawn_node_with_handles(vec![]);
    wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Leader(_))).await;
    let leader = expect_leader_handle(&role_rx.borrow());

    let stored = log.entries_in_range(1, 2).unwrap();
    assert_eq!(stored.len(), 1, "the first entry of the term is the no-op");
    assert_eq!(stored[0].payload, storage::EntryPayload::NoOp);
    assert_eq!(stored[0].term, leader.term);

    cleanup(&path);
}

/// A `Leader` with no tasks attached, for driving `recompute_*` directly.
/// Nothing here talks to a peer, so the states these tests need (a specific
/// pattern of `peer_match_index`) can just be set, instead of produced by a
/// real cluster. Returns the `read_quorum` receiver with the leader, because
/// a test that checks read quorum reads it there.
fn bare_leader(common: Common) -> (Leader, watch::Receiver<bool>) {
    let (read_quorum_tx, read_quorum_rx) = watch::channel(false);
    // A real `Task` (one that only waits to be stopped), not a bare handle, so
    // these leaders shut down through the same path as a live one.
    let tick_timer = Task::spawn(common.cancel_token(), |cancel| async move {
        cancel.cancelled().await;
    });
    let leader = Leader {
        common,
        replication_tasks: HashMap::new(),
        lease_tasks: HashMap::new(),
        tick_timer,
        peer_match_index: HashMap::new(),
        peer_lease_expiry: HashMap::new(),
        read_quorum_tx,
        // No no-op appended here, so the Figure 8 own-term gate is open from the
        // start: these tests set up the log state they want directly.
        noop_seq: 0,
        pending_config_seq: None,
        catch_up: HashMap::new(),
    };
    (leader, read_quorum_rx)
}

/// A leader set up as if it had already committed its term-opening no-op,
/// so `propose_config`'s guards are open.
async fn ready_leader(
    members: &[(&str, bool)],
) -> (
    Arc<RaftLog>,
    Leader,
    watch::Receiver<bool>,
    std::path::PathBuf,
) {
    let config = cluster_config(members);
    let (log, NodeHandles { mut common, .. }, path) = open_temp_log_with_config("n0", &config);
    common.current_term = 1;
    let noop_seq = log.append_noop(1).unwrap();
    common.advance_commit(noop_seq);

    let (mut leader, read_quorum_rx) = bare_leader(common);
    leader.noop_seq = noop_seq;
    leader.reconcile_peers().await;
    (log, leader, read_quorum_rx, path)
}

/// A server is added as a learner (§4.2.1): a member that gets replication
/// but has no vote. Adding it as a voter at once would let a server that is
/// far behind count toward (and so stall) every quorum from the moment it
/// joined.
#[tokio::test]
async fn add_server_joins_as_a_non_voting_learner() {
    let (_log, mut leader, _rq, path) = ready_leader(&[("n0", true), ("v1", true)]).await;
    assert_eq!(leader.common.majority(), 2);

    leader
        .propose_config(ConfigChange::AddServer {
            server_id: "new".to_string(),
            address: "http://127.0.0.1:9999".to_string(),
            client_address: String::new(),
        })
        .await
        .unwrap();

    assert!(leader.common.config.contains("new"));
    assert!(!leader.common.config.is_voter("new"));
    assert_eq!(
        leader.common.majority(),
        2,
        "a learner must not raise the quorum bar"
    );
    // In force on append: replication to it starts at once.
    assert!(leader.replication_tasks.contains_key("new"));
    assert!(!leader.lease_tasks.contains_key("new"));

    cleanup(&path);
}

/// Raft §4.1 allows only one membership change in flight at a time: two
/// overlapping single-server changes can produce configurations with no
/// common majority.
#[tokio::test]
async fn only_one_membership_change_may_be_in_flight() {
    let (_log, mut leader, _rq, path) = ready_leader(&[("n0", true), ("v1", true)]).await;

    let first = leader
        .propose_config(ConfigChange::AddServer {
            server_id: "a".to_string(),
            address: "http://127.0.0.1:9001".to_string(),
            client_address: String::new(),
        })
        .await
        .unwrap();
    let err = leader
        .propose_config(ConfigChange::AddServer {
            server_id: "b".to_string(),
            address: "http://127.0.0.1:9002".to_string(),
            client_address: String::new(),
        })
        .await
        .unwrap_err();
    assert!(matches!(err, RaftError::ConfigChangeInProgress));
    assert!(!leader.common.config.contains("b"));

    // The guard lifts as soon as the first change commits.
    leader.peer_match_index.insert("v1".to_string(), first);
    leader.recompute_watermarks().await;
    assert!(leader.pending_config_seq.is_none());
    leader
        .propose_config(ConfigChange::AddServer {
            server_id: "b".to_string(),
            address: "http://127.0.0.1:9002".to_string(),
            client_address: String::new(),
        })
        .await
        .unwrap();

    cleanup(&path);
}

/// Ongaro 2015: a leader that has not yet committed an entry from its own
/// term still reads a configuration that a later leader could roll back, so
/// it must not build a membership change on it.
#[tokio::test]
async fn a_leader_will_not_change_membership_before_committing_its_own_term() {
    let config = cluster_config(&[("n0", true), ("v1", true)]);
    let (log, NodeHandles { mut common, .. }, path) = open_temp_log_with_config("n0", &config);
    common.current_term = 1;
    let noop_seq = log.append_noop(1).unwrap(); // appended, not committed

    let (mut leader, _rq) = bare_leader(common);
    leader.noop_seq = noop_seq;

    let err = leader
        .propose_config(ConfigChange::RemoveServer {
            server_id: "v1".to_string(),
        })
        .await
        .unwrap_err();
    assert!(matches!(err, RaftError::NotCaughtUp));

    // Committing the no-op is exactly what opens the gate.
    leader.peer_match_index.insert("v1".to_string(), noop_seq);
    leader.recompute_watermarks().await;
    leader
        .propose_config(ConfigChange::RemoveServer {
            server_id: "v1".to_string(),
        })
        .await
        .unwrap();

    cleanup(&path);
}

/// Changes that cannot apply are refused, instead of producing a
/// configuration nobody could make progress under.
#[tokio::test]
async fn impossible_membership_changes_are_refused() {
    let (_log, mut leader, _rq, path) = ready_leader(&[("n0", true), ("v1", true)]).await;

    let already = leader
        .propose_config(ConfigChange::AddServer {
            server_id: "v1".to_string(),
            address: "http://127.0.0.1:9001".to_string(),
            client_address: String::new(),
        })
        .await
        .unwrap_err();
    assert!(matches!(already, RaftError::InvalidConfigChange(_)));

    let stranger = leader
        .propose_config(ConfigChange::RemoveServer {
            server_id: "nobody".to_string(),
        })
        .await
        .unwrap_err();
    assert!(matches!(stranger, RaftError::InvalidConfigChange(_)));

    // A refused change must not use up the one in-flight slot.
    assert!(leader.pending_config_seq.is_none());
    cleanup(&path);
}

/// The last voter cannot be removed: the entry that removes it could never
/// commit, because nobody would be left to form a quorum of the
/// configuration it creates.
#[tokio::test]
async fn the_last_voter_cannot_be_removed() {
    let (_log, mut leader, _rq, path) = ready_leader(&[("n0", true), ("l1", false)]).await;
    let err = leader
        .propose_config(ConfigChange::RemoveServer {
            server_id: "n0".to_string(),
        })
        .await
        .unwrap_err();
    assert!(matches!(err, RaftError::InvalidConfigChange(_)));
    cleanup(&path);
}

/// A heartbeat change is a configuration change that keeps the membership.
/// Every running task reads its timing again, so the change reaches them
/// without restarting any (which would cost each replication task its
/// hard-won `next_index`).
#[tokio::test]
async fn set_heartbeat_retimes_the_leader_without_touching_membership() {
    let members = [("n0", true), ("v1", true), ("v2", true)];
    let (_log, mut leader, _rq, path) = ready_leader(&members).await;
    let timing_rx = leader.common.timing_tx.subscribe();
    assert_eq!(*timing_rx.borrow(), Timing::default());
    let replication = leader.replication_tasks["v1"].id();
    let lease = leader.lease_tasks["v1"].id();

    leader
        .propose_config(ConfigChange::SetHeartbeat { millis: 200 })
        .await
        .unwrap();

    assert_eq!(*timing_rx.borrow(), Timing::from_millis(200));
    assert_eq!(leader.common.timing().lease(), Duration::from_millis(800));
    assert_eq!(
        leader.common.config.members,
        cluster_config(&members).members
    );
    assert!(
        leader.pending_config_seq.is_some(),
        "it is a config change like any other, one at a time"
    );
    assert_eq!(leader.replication_tasks["v1"].id(), replication);
    assert_eq!(leader.lease_tasks["v1"].id(), lease);
    cleanup(&path);
}

#[tokio::test]
async fn an_out_of_range_heartbeat_is_refused() {
    let (_log, mut leader, _rq, path) = ready_leader(&[("n0", true)]).await;
    for millis in [0, 9, 10_001, u64::MAX] {
        let err = leader
            .propose_config(ConfigChange::SetHeartbeat { millis })
            .await
            .unwrap_err();
        assert!(
            matches!(err, RaftError::InvalidConfigChange(_)),
            "{millis}ms should be refused"
        );
    }
    assert!(leader.pending_config_seq.is_none());
    assert_eq!(leader.common.timing(), Timing::default());
    cleanup(&path);
}

/// A grant counts until the deadline computed from when its probe was sent,
/// not from when the answer arrived: the follower started its promise on
/// receipt, so any later start would let the leader count a lease the
/// follower already gave up, by as much as a whole round trip on a slow
/// link.
#[tokio::test]
async fn a_lease_grant_counts_only_until_its_probes_deadline() {
    let config = cluster_config(&[("n0", true), ("v1", true), ("v2", true)]);
    let (_log, NodeHandles { common, .. }, path) = open_temp_log_with_config("n0", &config);
    let (mut leader, read_quorum_rx) = bare_leader(common);
    let granted = || GetLeaseResponse {
        lease_granted: true,
    };

    // Already over by the time it arrived.
    leader
        .handle_event(Event::GetLeaseResponse {
            from: "v1".to_string(),
            resp: granted(),
            expires_at: Instant::now() - Duration::from_millis(1),
        })
        .await;
    assert!(!*read_quorum_rx.borrow());

    leader
        .handle_event(Event::GetLeaseResponse {
            from: "v1".to_string(),
            resp: granted(),
            expires_at: Instant::now() + Timing::default().lease(),
        })
        .await;
    assert!(*read_quorum_rx.borrow());
    cleanup(&path);
}

/// Raft §4.2.2: a leader that removes itself is the only node that can
/// commit that entry, so it keeps serving until it does, and steps down the
/// moment it commits, not before.
#[tokio::test]
async fn a_leader_that_removes_itself_serves_until_the_change_commits() {
    let (_log, mut leader, _rq, path) =
        ready_leader(&[("n0", true), ("v1", true), ("v2", true)]).await;

    let seq = leader
        .propose_config(ConfigChange::RemoveServer {
            server_id: "n0".to_string(),
        })
        .await
        .unwrap();
    assert!(!leader.common.config.contains("n0"));
    assert!(
        matches!(leader.step_down_if_removed(), Transition::Continue),
        "stepping down now would leave nobody able to finish the change"
    );

    // A majority of the new configuration (v1 and v2, since we no longer
    // count) acknowledges it.
    leader.peer_match_index.insert("v1".to_string(), seq);
    leader.peer_match_index.insert("v2".to_string(), seq);
    leader.recompute_watermarks().await;
    assert!(matches!(
        leader.step_down_if_removed(),
        Transition::Become(Role::Follower)
    ));

    cleanup(&path);
}

/// Raft §4.2.1: a learner is promoted once it finishes a catch-up round
/// fast enough to show it can keep up in real time, automatically, with no
/// second client call.
#[tokio::test]
async fn a_learner_that_keeps_up_is_promoted_automatically() {
    let (log, mut leader, _rq, path) =
        ready_leader(&[("n0", true), ("v1", true), ("l1", false)]).await;

    // The first response only starts the round; there is nothing to judge it
    // by yet.
    leader.review_learner_progress().await;
    assert!(!leader.common.config.is_voter("l1"));
    assert!(leader.catch_up.contains_key("l1"));

    // It reaches the round's target well within an election timeout.
    let tail = log.last_entry().unwrap().unwrap().0;
    leader.peer_match_index.insert("l1".to_string(), tail);
    leader.review_learner_progress().await;

    assert!(leader.common.config.is_voter("l1"));
    assert_eq!(
        leader.common.majority(),
        2,
        "three voters now, so still two -- but l1's vote counts"
    );
    assert!(leader.lease_tasks.contains_key("l1"));
    assert!(
        !leader.catch_up.contains_key("l1"),
        "a promoted learner is done being measured"
    );

    cleanup(&path);
}

/// A learner that reaches its target only after an election timeout has
/// not shown it can keep up, so it starts another round instead of being
/// promoted. There is no failure path: it stays a learner, replicated to
/// but counted in no quorum.
#[tokio::test]
async fn a_learner_that_lags_starts_another_round_instead() {
    let (log, mut leader, _rq, path) =
        ready_leader(&[("n0", true), ("v1", true), ("l1", false)]).await;

    let tail = log.last_entry().unwrap().unwrap().0;
    leader.catch_up.insert(
        "l1".to_string(),
        CatchUpState {
            round_start_index: tail,
            round_started_at: Instant::now() - (Timing::default().lease() * 2),
        },
    );
    leader.peer_match_index.insert("l1".to_string(), tail);
    leader.review_learner_progress().await;

    assert!(!leader.common.config.is_voter("l1"));
    let state = &leader.catch_up["l1"];
    assert!(
        state.round_started_at.elapsed() < Timing::default().lease(),
        "a fresh round should have started"
    );

    cleanup(&path);
}

/// A membership change adds and removes exactly the tasks that changed, and
/// leaves the rest running: each replication task owns this leader's
/// hard-won `next_index` for its peer, so restarting them all on every
/// change would probe the whole cluster again because one server was
/// added.
#[tokio::test]
async fn reconcile_peers_adds_and_removes_only_what_changed() {
    let before = cluster_config(&[("n0", true), ("v1", true), ("l1", false)]);
    let (log, NodeHandles { common, .. }, path) = open_temp_log_with_config("n0", &before);

    let (mut leader, _read_quorum_rx) = bare_leader(common);
    leader.reconcile_peers().await;
    assert_eq!(
        sorted_keys(&leader.replication_tasks),
        vec!["l1", "v1"],
        "learners are replicated to"
    );
    assert_eq!(
        sorted_keys(&leader.lease_tasks),
        vec!["v1"],
        "but do not hold a lease"
    );
    let untouched = leader.replication_tasks["v1"].id();

    // Promote the learner and add a new one, in one new configuration.
    let after = cluster_config(&[("n0", true), ("v1", true), ("l1", true), ("l2", false)]);
    log.append_config(1, &after.to_bytes()).unwrap();
    leader.common.reload_config().await;
    leader.reconcile_peers().await;

    assert_eq!(
        sorted_keys(&leader.replication_tasks),
        vec!["l1", "l2", "v1"]
    );
    assert_eq!(
        sorted_keys(&leader.lease_tasks),
        vec!["l1", "v1"],
        "the promoted learner now holds a lease too"
    );
    assert_eq!(
        leader.replication_tasks["v1"].id(),
        untouched,
        "an unaffected peer's replication task must not be restarted"
    );

    cleanup(&path);
}

/// A removed peer must be forgotten, not only unsubscribed: a stale
/// `peer_match_index` for a former member would keep being counted, and
/// pin `compact_seq` at the index where that server stopped, forever.
#[tokio::test]
async fn removing_a_peer_stops_it_from_holding_back_compaction() {
    let before = cluster_config(&[("n0", true), ("v1", true), ("gone", true)]);
    let (
        log,
        NodeHandles {
            mut common,
            compact_rx,
            ..
        },
        path,
    ) = open_temp_log_with_config("n0", &before);
    common.current_term = 1;
    let tail = log.append_noop(1).unwrap();

    let (mut leader, _read_quorum_rx) = bare_leader(common);
    leader.reconcile_peers().await;
    leader.peer_match_index.insert("v1".to_string(), tail);
    leader.peer_match_index.insert("gone".to_string(), 0);
    leader.recompute_watermarks().await;
    assert_eq!(*compact_rx.borrow(), 0, "\"gone\" is still a member");

    let after = cluster_config(&[("n0", true), ("v1", true)]);
    log.append_config(1, &after.to_bytes()).unwrap();
    leader.common.reload_config().await;
    leader.reconcile_peers().await;

    assert!(!leader.replication_tasks.contains_key("gone"));
    assert!(!leader.peer_match_index.contains_key("gone"));
    // `reconcile_peers` recomputes by itself once a peer leaves, so nothing
    // else is needed.
    assert_eq!(*compact_rx.borrow(), tail);

    cleanup(&path);
}

fn sorted_keys(tasks: &HashMap<String, Task>) -> Vec<&str> {
    let mut ids: Vec<&str> = tasks.keys().map(String::as_str).collect();
    ids.sort_unstable();
    ids
}

/// Only voters decide commit (§4.2.1). A learner gets replication like
/// anyone else, but counting it toward the commit quorum would let an entry
/// commit because of a node that cannot vote in the election that must
/// preserve it.
#[tokio::test]
async fn a_learners_replication_does_not_commit_anything() {
    let config = cluster_config(&[("n0", true), ("v1", true), ("l1", false)]);
    let (
        log,
        NodeHandles {
            mut common,
            commit_rx,
            ..
        },
        path,
    ) = open_temp_log_with_config("n0", &config);
    common.current_term = 1;
    let tail = log.append_noop(1).unwrap();

    let (mut leader, _read_quorum_rx) = bare_leader(common);
    assert_eq!(leader.common.majority(), 2, "two voters, so two to decide");

    // Two of the three members hold the tail (us and the learner), but only
    // one of the two voters does.
    leader.peer_match_index.insert("l1".to_string(), tail);
    leader.peer_match_index.insert("v1".to_string(), 0);
    leader.recompute_watermarks().await;
    assert_eq!(*commit_rx.borrow(), 0);

    // The second voter is what decides it.
    leader.peer_match_index.insert("v1".to_string(), tail);
    leader.recompute_watermarks().await;
    assert_eq!(*commit_rx.borrow(), tail);

    cleanup(&path);
}

/// Compaction uses the opposite group: it deletes entries, so it must wait
/// for everyone who might still need them, including a learner that is
/// still catching up.
#[tokio::test]
async fn compaction_waits_for_a_learner_that_commitment_ignores() {
    let config = cluster_config(&[("n0", true), ("v1", true), ("l1", false)]);
    let (
        log,
        NodeHandles {
            mut common,
            commit_rx,
            compact_rx,
            ..
        },
        path,
    ) = open_temp_log_with_config("n0", &config);
    common.current_term = 1;
    let tail = log.append_noop(1).unwrap();

    let (mut leader, _read_quorum_rx) = bare_leader(common);

    // Both voters have everything, so it commits, but the learner is still
    // behind, so nothing may be deleted yet.
    leader.peer_match_index.insert("v1".to_string(), tail);
    leader.peer_match_index.insert("l1".to_string(), 0);
    leader.recompute_watermarks().await;
    assert_eq!(*commit_rx.borrow(), tail);
    assert_eq!(*compact_rx.borrow(), 0);

    leader.peer_match_index.insert("l1".to_string(), tail);
    leader.recompute_watermarks().await;
    assert_eq!(*compact_rx.borrow(), tail);

    cleanup(&path);
}

/// A read lease is a quorum too, so only voters count. Otherwise a leader
/// cut off from every voter could still claim read quorum through learners
/// alone.
#[tokio::test]
async fn read_quorum_counts_voters_only() {
    let config = cluster_config(&[("n0", true), ("v1", true), ("v2", true), ("l1", false)]);
    let (_log, NodeHandles { common, .. }, path) = open_temp_log_with_config("n0", &config);

    let (mut leader, read_quorum_rx) = bare_leader(common);
    let live = Instant::now() + Timing::default().lease();

    // Us plus a learner: two of four members, but only one of three voters,
    // short of the two a quorum needs.
    leader.peer_lease_expiry.insert("l1".to_string(), live);
    leader.recompute_lease_quorum();
    assert!(!*read_quorum_rx.borrow());

    leader.peer_lease_expiry.insert("v1".to_string(), live);
    leader.recompute_lease_quorum();
    assert!(*read_quorum_rx.borrow());

    cleanup(&path);
}

/// Raft §5.4.2 / Figure 8: replication to a majority does not commit an
/// entry from an earlier term, because a later leader can still overwrite
/// it. `recompute_watermarks` must hold `commit_seq` back until an entry
/// from the current term reaches a majority, then commit the whole prefix
/// at once.
///
/// Drives `recompute_watermarks` directly instead of through a real
/// cluster, because the point is a state a leader cannot be steered into
/// from outside: a majority that has the inherited tail but not yet this
/// term's no-op.
#[tokio::test]
async fn commit_waits_for_an_entry_from_the_leaders_own_term() {
    let (log, path) = open_temp_log();
    // A tail inherited from term 1, never committed by its leader.
    log.append(1, b"a", Some(b"1")).unwrap();
    let inherited = log.append(1, b"b", Some(b"2")).unwrap();

    let peers = vec![
        PeerInfo {
            id: "p1".to_string(),
            address: "http://127.0.0.1:1".to_string(),
        },
        PeerInfo {
            id: "p2".to_string(),
            address: "http://127.0.0.1:2".to_string(),
        },
    ];
    let NodeHandles {
        mut common,
        commit_rx,
        ..
    } = Common::new_seeded(
        "n0".to_string(),
        peers,
        Arc::clone(&log),
        CancellationToken::new(),
    )
    .unwrap();
    common.current_term = 7;
    // ... plus the no-op this leader opens term 7 with.
    let noop = log.append_noop(7).unwrap();

    let (mut leader, _read_quorum_rx) = bare_leader(common);

    // p1 has the inherited entries but not the no-op. That is a majority (p1
    // and us) holding seq 2, and it still must not commit, because seq 2 is
    // from term 1.
    leader.peer_match_index.insert("p1".to_string(), inherited);
    leader.recompute_watermarks().await;
    assert_eq!(
        *commit_rx.borrow(),
        0,
        "an entry from a previous term must not commit on majority replication alone"
    );

    // Once the term-7 no-op reaches p1, committing it commits everything
    // before it too (Log Matching).
    leader.peer_match_index.insert("p1".to_string(), noop);
    leader.recompute_watermarks().await;
    assert_eq!(*commit_rx.borrow(), noop);

    cleanup(&path);
}

/// `append_log` and `spawn_storage_maintenance_task` are separate `watch`
/// subscribers of the same commit signal, and `watch` gives no order between
/// subscribers that wake on the same update. Waiting on `commit_tx` could
/// return before the maintenance task ran `apply_committed` for that seq.
/// `append_log` waits on `applied_tx`, so when it returns, `get_latest` must
/// already show the write, with no polling or sleeping.
#[tokio::test]
async fn append_log_does_not_return_until_latest_actually_reflects_it() {
    let (log, mut role_rx, _peer_handle, _commit_rx, _compact_rx, path) =
        spawn_node_with_handles(vec![]);
    wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Leader(_))).await;
    let leader = expect_leader_handle(&role_rx.borrow());

    let seq = leader.append_log(b"k", Some(b"v")).await.unwrap();

    // No wait, no poll: if this raced the maintenance task instead of really
    // waiting for it, this would be flaky (sometimes seeing `None` here).
    assert_eq!(log.get_latest(b"k").unwrap(), Some((seq, b"v".to_vec())));

    cleanup(&path);
}

/// A leader must not grant a higher-term vote request without checking the
/// candidate's log per §5.4.1. A candidate whose log is behind must be
/// denied even though its term is higher.
#[tokio::test]
async fn leader_denies_a_higher_term_vote_if_the_candidates_log_is_behind() {
    let (mut role_rx, peer_handle, path) = spawn_test_node(vec![]);
    wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Leader(_))).await;
    let leader = expect_leader_handle(&role_rx.borrow());

    // Give the leader a log entry, so it has something to be ahead of.
    leader.append_log(b"k", Some(b"v")).await.unwrap();

    // The term is much higher, but the candidate's log is empty: behind the
    // leader's one entry.
    let resp = peer_handle
        .request_vote(RequestVoteRequest {
            term: 999,
            candidate_id: "candidate-a".to_string(),
            last_log_index: 0,
            last_log_term: 0,
            dry_run: false,
        })
        .await
        .unwrap();

    assert!(!resp.vote_granted);
    cleanup(&path);
}

/// A cluster with no peers always has a quorum of leases (just itself), as
/// it always has a commit majority.
#[tokio::test]
async fn leader_has_read_quorum_immediately_in_a_single_node_cluster() {
    let (mut role_rx, _peer_handle, path) = spawn_test_node(vec![]);
    wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Leader(_))).await;
    let leader = expect_leader_handle(&role_rx.borrow());

    // `RoleHandle::Leader` is published just before the first
    // `recompute_lease_quorum()` call finishes (there is an `.await` between
    // them, for the watermark read), so poll briefly instead of asserting on
    // the very first check.
    tokio::time::timeout(Duration::from_millis(500), async {
        while !leader.has_read_quorum() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("a single-node cluster should reach read quorum almost immediately");
    cleanup(&path);
}

/// End to end over real sockets: once a leader's lease-prober task reaches
/// a live peer and gets a grant, the grant counts toward read quorum (a
/// majority with one peer needs both).
#[tokio::test]
async fn leader_gains_read_quorum_once_a_real_peer_grants_its_lease() {
    use crate::grpc::RaftPeerService;
    use crate::pb::raft5::raft::raft_peer_server::RaftPeerServer;

    let addr_a = free_local_addr();
    let addr_b = free_local_addr();

    let (log_a, path_a) = open_temp_log();
    let (log_b, path_b) = open_temp_log();

    let NodeHandles {
        common: common_a,
        role_rx: mut role_rx_a,
        peer_handle: peer_handle_a,
        ..
    } = Common::new_seeded(
        "node-a".to_string(),
        vec![PeerInfo {
            id: "node-b".to_string(),
            address: format!("http://{addr_b}"),
        }],
        Arc::clone(&log_a),
        CancellationToken::new(),
    )
    .unwrap();
    let NodeHandles {
        common: common_b,
        role_rx: mut role_rx_b,
        peer_handle: peer_handle_b,
        ..
    } = Common::new_seeded(
        "node-b".to_string(),
        vec![PeerInfo {
            id: "node-a".to_string(),
            address: format!("http://{addr_a}"),
        }],
        Arc::clone(&log_b),
        CancellationToken::new(),
    )
    .unwrap();

    tokio::spawn(run(common_a));
    tokio::spawn(run(common_b));

    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(RaftPeerServer::new(RaftPeerService::new(peer_handle_a)))
            .serve(addr_a),
    );
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(RaftPeerServer::new(RaftPeerService::new(peer_handle_b)))
            .serve(addr_b),
    );

    tokio::select! {
        _ = wait_for_role(&mut role_rx_a, |r| matches!(r, RoleHandle::Leader(_))) => {}
        _ = wait_for_role(&mut role_rx_b, |r| matches!(r, RoleHandle::Leader(_))) => {}
    }
    let leader = if matches!(&*role_rx_a.borrow(), RoleHandle::Leader(_)) {
        expect_leader_handle(&role_rx_a.borrow())
    } else {
        expect_leader_handle(&role_rx_b.borrow())
    };

    tokio::time::timeout(Duration::from_millis(2000), async {
        while !leader.has_read_quorum() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("leader should gain read quorum once its peer's lease prober succeeds");

    cleanup(&path_a);
    cleanup(&path_b);
}

#[test]
fn build_replication_step_returns_snapshot_when_prev_log_index_is_compacted() {
    let (log, path) = open_temp_log();
    for i in 1..=5u64 {
        log.append(1, format!("k{i}").as_bytes(), Some(b"v"))
            .unwrap();
    }
    // `compact` clamps to what is applied, so apply everything first, or the
    // compact below would do nothing.
    log.apply_committed(5).unwrap();
    // Removes entries 1-3; the floor lands at seq 3 (its term still answerable
    // through the floor case in `term_at`).
    log.compact(4).unwrap();

    // next_index = 4 -> prev_log_index = 3, exactly the floor, which is still
    // answerable, so this is a normal AppendEntries.
    let step = step_for(&log, 4);
    assert!(matches!(step, ReplicationStep::AppendEntries { .. }));

    // next_index = 3 -> prev_log_index = 2, below the floor. `term_at` cannot
    // answer for it, so this must fall back to a snapshot instead of guessing.
    let step = step_for(&log, 3);
    assert!(matches!(step, ReplicationStep::Snapshot));

    cleanup(&path);
}

/// A gap that the `term_at` check alone misses: `prev_log_index == 0` (a
/// peer replaying from scratch) never calls `term_at`, so if the early
/// entries it needs are also compacted away, only the contiguity check
/// catches it.
#[test]
fn build_replication_step_returns_snapshot_when_replaying_from_scratch_hits_a_gap() {
    let (log, path) = open_temp_log();
    for i in 1..=5u64 {
        log.append(1, format!("k{i}").as_bytes(), Some(b"v"))
            .unwrap();
    }
    log.apply_committed(5).unwrap();
    log.compact(4).unwrap();

    let step = step_for(&log, 1);
    assert!(matches!(step, ReplicationStep::Snapshot));

    cleanup(&path);
}

/// A normal replication step, with the arguments a test never varies
/// filled in. `heartbeat_only` is false: these tests are about what the
/// builder decides to send, which a probe round skips.
fn step_for(log: &RaftLog, next_index: u64) -> ReplicationStep {
    build_replication_step_sync(log, next_index, "leader-1".to_string(), 1, 0, 0, false).unwrap()
}

/// The batch is bounded by bytes, not by how far behind a peer is. A short
/// batch must still be a contiguous prefix that starts at `next_index`, or
/// the follower would get a history with holes. And it must say there is
/// more, so the caller comes straight back instead of sleeping between
/// rounds.
#[test]
fn build_replication_step_stops_at_the_byte_budget_and_says_there_is_more() {
    let (log, path) = open_temp_log();
    // Each entry is well over a megabyte, so a few of them exceed any sane
    // budget without the test needing to know it.
    let big = vec![0u8; 2 * 1024 * 1024];
    for i in 1..=16u64 {
        log.append(1, format!("k{i}").as_bytes(), Some(&big)).unwrap();
    }

    let ReplicationStep::AppendEntries { req, more_to_send } = step_for(&log, 1) else {
        panic!("a log with no compaction should replicate, not snapshot");
    };
    assert!(
        more_to_send,
        "a batch cut short by the budget has to report that there is more"
    );
    assert!(
        req.entries.len() < 16,
        "the budget should have cut this batch short, got all {} entries",
        req.entries.len()
    );
    assert!(!req.entries.is_empty(), "a budget must never stall progress entirely");
    for (i, entry) in req.entries.iter().enumerate() {
        assert_eq!(
            entry.seq,
            i as u64 + 1,
            "a truncated batch must still be a contiguous prefix from next_index"
        );
    }

    // Continuing from there eventually drains the log, and the last batch says
    // so.
    let next = req.entries.len() as u64 + 1;
    let ReplicationStep::AppendEntries { req, .. } = step_for(&log, next) else {
        panic!("the rest of the log should replicate normally");
    };
    assert_eq!(req.entries[0].seq, next);

    cleanup(&path);
}

/// The escape hatch: one entry larger than the whole budget must still be
/// sent, or it could never replicate, never commit, and would block
/// everything behind it forever. That is why nothing larger than
/// `MAX_ENTRY_BYTES` is allowed into the log.
#[test]
fn build_replication_step_always_sends_at_least_one_entry() {
    let (log, path) = open_temp_log();
    let huge = vec![0u8; MAX_REPLICATION_BATCH_BYTES + 1];
    log.append(1, b"huge", Some(&huge)).unwrap();
    log.append(1, b"after", Some(b"v")).unwrap();

    let ReplicationStep::AppendEntries { req, more_to_send } = step_for(&log, 1) else {
        panic!("an uncompacted log should replicate, not snapshot");
    };
    assert_eq!(
        req.entries.len(),
        1,
        "an oversized entry goes on its own, but it does go"
    );
    assert_eq!(req.entries[0].seq, 1);
    assert!(more_to_send);

    cleanup(&path);
}

/// A case a naive contiguity check would miss: every entry in the range is
/// compacted away while `last_entry` still answers from the floor. Sent as
/// an empty batch, that reads as a heartbeat, so the follower replies
/// `success` at the index it already had, `next_index` never moves, and the
/// peer spins every tick with no progress and no snapshot.
#[test]
fn build_replication_step_returns_snapshot_when_the_whole_range_is_compacted() {
    let (log, path) = open_temp_log();
    for i in 1..=5u64 {
        log.append(1, format!("k{i}").as_bytes(), Some(b"v"))
            .unwrap();
    }
    log.apply_committed(5).unwrap();
    // Compacts everything: `log` is empty, but `last_entry` still reports seq 5
    // from the floor.
    log.compact(6).unwrap();
    assert_eq!(log.last_entry().unwrap().map(|(seq, _)| seq), Some(5));
    assert!(log.entries_in_range(1, 6).unwrap().is_empty());

    // next_index = 1 reaches this branch: prev_log_index is 0, so the `term_at`
    // check above is skipped, and `next_index <= last_index` holds because the
    // floor says 5. So it fetches, and gets nothing.
    let step = step_for(&log, 1);
    assert!(
        matches!(step, ReplicationStep::Snapshot),
        "a peer whose entries are all gone needs a snapshot, not a heartbeat"
    );

    cleanup(&path);
}

/// End to end over real sockets: forces the scenario
/// `ReplicationStep::Snapshot` exists for. It rolls a follower's log back
/// (as if it fell behind) below the point where the leader's log was
/// compacted (also done directly, not through the normal `compact_seq`
/// path; see the comment below). Then it checks that the leader notices and
/// sends a real `InstallSnapshot` RPC that restores the follower's missing
/// history.
#[tokio::test]
async fn leader_sends_a_real_snapshot_to_a_peer_that_has_fallen_behind_compaction() {
    use crate::grpc::RaftPeerService;
    use crate::pb::raft5::raft::raft_peer_server::RaftPeerServer;

    let addr_a = free_local_addr();
    let addr_b = free_local_addr();

    let (log_a, path_a) = open_temp_log();
    let (log_b, path_b) = open_temp_log();

    let NodeHandles {
        common: common_a,
        role_rx: mut role_rx_a,
        peer_handle: peer_handle_a,
        ..
    } = Common::new_seeded(
        "node-a".to_string(),
        vec![PeerInfo {
            id: "node-b".to_string(),
            address: format!("http://{addr_b}"),
        }],
        Arc::clone(&log_a),
        CancellationToken::new(),
    )
    .unwrap();
    let NodeHandles {
        common: common_b,
        role_rx: mut role_rx_b,
        peer_handle: peer_handle_b,
        ..
    } = Common::new_seeded(
        "node-b".to_string(),
        vec![PeerInfo {
            id: "node-a".to_string(),
            address: format!("http://{addr_a}"),
        }],
        Arc::clone(&log_b),
        CancellationToken::new(),
    )
    .unwrap();

    tokio::spawn(run(common_a));
    tokio::spawn(run(common_b));

    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(RaftPeerServer::new(RaftPeerService::new(peer_handle_a)))
            .serve(addr_a),
    );
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(RaftPeerServer::new(RaftPeerService::new(peer_handle_b)))
            .serve(addr_b),
    );

    tokio::select! {
        _ = wait_for_role(&mut role_rx_a, |r| matches!(r, RoleHandle::Leader(_))) => {}
        _ = wait_for_role(&mut role_rx_b, |r| matches!(r, RoleHandle::Leader(_))) => {}
    }

    // Leadership can still move right after an election, as in
    // `raft::tests::leader_replicates_a_write_to_a_real_peer_and_commits_it`:
    // retry against the current leader instead of assuming the first leader
    // seen stays leader, while appending 5 distinct keys.
    let (leader_log, follower_log) = loop {
        let a_is_leader = matches!(&*role_rx_a.borrow(), RoleHandle::Leader(_));
        let (leader, leader_log, follower_log) = if a_is_leader {
            (
                expect_leader_handle(&role_rx_a.borrow()),
                Arc::clone(&log_a),
                Arc::clone(&log_b),
            )
        } else {
            (
                expect_leader_handle(&role_rx_b.borrow()),
                Arc::clone(&log_b),
                Arc::clone(&log_a),
            )
        };

        let mut ok = true;
        for i in 0..5u64 {
            match tokio::time::timeout(
                Duration::from_millis(2000),
                leader.append_log(format!("k{i}").as_bytes(), Some(format!("v{i}").as_bytes())),
            )
            .await
            .expect("append_log should not hang")
            {
                Ok(_) => {}
                Err(RaftError::NotLeader { .. }) => {
                    ok = false;
                    break;
                }
                Err(e) => panic!("unexpected append_log error: {e:?}"),
            }
        }
        if ok {
            break (leader_log, follower_log);
        }
        tokio::select! {
            _ = wait_for_role(&mut role_rx_a, |r| matches!(r, RoleHandle::Leader(_))) => {}
            _ = wait_for_role(&mut role_rx_b, |r| matches!(r, RoleHandle::Leader(_))) => {}
        }
    };

    // Give the follower's AppendEntries handling a moment to finish persisting
    // the 5th entry before rolling it back.
    tokio::time::timeout(Duration::from_millis(1000), async {
        loop {
            if follower_log.get_latest(b"k4").unwrap().is_some() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("follower should have replicated all 5 entries normally first");

    // Simulate the follower falling behind: roll it back to entry 2, out of
    // band (the leader's replication task does not know this happened).
    follower_log.truncate_from(3).unwrap();

    // Compact the leader's log directly, past where the follower now is,
    // bypassing `compact_seq` and `Common`: `compact_seq` is the minimum over
    // every configured peer, which makes this precondition very hard to reach
    // in normal two-node operation. Forcing it tests the RPC mechanics this
    // test cares about. `compact` clamps to the applied watermark, so apply
    // first: in normal operation `spawn_storage_maintenance_task` does that,
    // but this test cannot rely on it having run yet.
    leader_log.apply_committed(5).unwrap();
    leader_log.compact(4).unwrap();

    // The leader's next heartbeat is rejected (the follower's log no longer
    // reaches where the leader thinks it does), which backs `next_index` down
    // to a point the leader's own log can no longer answer for, which triggers
    // a real `InstallSnapshot`. Wait for the follower to have the complete,
    // correct state again.
    tokio::time::timeout(Duration::from_millis(3000), async {
        loop {
            if (0..5u64).all(|i| {
                follower_log
                    .get_latest(format!("k{i}").as_bytes())
                    .unwrap()
                    .is_some_and(|(_, v)| v == format!("v{i}").into_bytes())
            }) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("follower should be caught up via a real InstallSnapshot RPC");

    cleanup(&path_a);
    cleanup(&path_b);
}

/// Replication is woken by the log and the commit watermark, not polled on
/// a `Timing::default().heartbeat()` timer: an entry appended at any moment
/// reaches the peer, and the resulting commit index reaches it too, without
/// waiting for a tick. On a polling loop, each of those would cost up to
/// `Timing::default().heartbeat()`, so 20 sequential writes could not finish
/// this fast.
#[tokio::test]
async fn replication_is_woken_by_new_entries_rather_than_polled() {
    use crate::grpc::RaftPeerService;
    use crate::pb::raft5::raft::raft_peer_server::RaftPeerServer;

    let addr_a = free_local_addr();
    let addr_b = free_local_addr();

    let (log_a, path_a) = open_temp_log();
    let (log_b, path_b) = open_temp_log();

    let NodeHandles {
        common: common_a,
        role_rx: mut role_rx_a,
        peer_handle: peer_handle_a,
        commit_rx: mut commit_rx_a,
        ..
    } = Common::new_seeded(
        "node-a".to_string(),
        vec![PeerInfo {
            id: "node-b".to_string(),
            address: format!("http://{addr_b}"),
        }],
        Arc::clone(&log_a),
        CancellationToken::new(),
    )
    .unwrap();
    let NodeHandles {
        common: common_b,
        role_rx: mut role_rx_b,
        peer_handle: peer_handle_b,
        commit_rx: mut commit_rx_b,
        ..
    } = Common::new_seeded(
        "node-b".to_string(),
        vec![PeerInfo {
            id: "node-a".to_string(),
            address: format!("http://{addr_a}"),
        }],
        Arc::clone(&log_b),
        CancellationToken::new(),
    )
    .unwrap();

    tokio::spawn(run(common_a));
    tokio::spawn(run(common_b));
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(RaftPeerServer::new(RaftPeerService::new(peer_handle_a)))
            .serve(addr_a),
    );
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(RaftPeerServer::new(RaftPeerService::new(peer_handle_b)))
            .serve(addr_b),
    );

    tokio::select! {
        _ = wait_for_role(&mut role_rx_a, |r| matches!(r, RoleHandle::Leader(_))) => {}
        _ = wait_for_role(&mut role_rx_b, |r| matches!(r, RoleHandle::Leader(_))) => {}
    }
    let leader = if matches!(&*role_rx_a.borrow(), RoleHandle::Leader(_)) {
        expect_leader_handle(&role_rx_a.borrow())
    } else {
        expect_leader_handle(&role_rx_b.borrow())
    };
    // Past the election's settling period: the no-op has committed everywhere
    // that matters, so what is measured below is the steady state.
    tokio::time::timeout(Duration::from_secs(2), async {
        while !leader.has_read_quorum() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("leader should gain read quorum");

    const WRITES: u32 = 20;
    let started = Instant::now();
    let mut last_seq = 0;
    for i in 0..WRITES {
        // Lands at a different phase of any timer each time.
        tokio::time::sleep(Duration::from_millis(u64::from(i % 7) * 3)).await;
        last_seq = leader
            .append_log(format!("k{i}").as_bytes(), Some(b"v"))
            .await
            .expect("append_log");
    }
    let sleeping: u64 = (0..WRITES).map(|i| u64::from(i % 7) * 3).sum();
    let in_writes = started.elapsed() - Duration::from_millis(sleeping);
    let heartbeat = Timing::default().heartbeat();
    assert!(
        in_writes < heartbeat * WRITES / 4,
        "{WRITES} writes took {in_writes:?} of commit wait; a loop that polls every \
         {heartbeat:?} would average at least {:?} per write",
        heartbeat / 2
    );

    // Both nodes learn the commit index quickly, the follower too: it only
    // hears it through `leader_commit` on an `AppendEntries`.
    for rx in [&mut commit_rx_a, &mut commit_rx_b] {
        tokio::time::timeout(Timing::default().heartbeat() / 2, async {
            while *rx.borrow_and_update() < last_seq {
                rx.changed().await.unwrap();
            }
        })
        .await
        .expect("the commit index should reach every node without waiting for a tick");
    }

    cleanup(&path_a);
    cleanup(&path_b);
}
