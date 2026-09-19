use super::*;
use crate::storage::Mutation;
use crate::pb::raft5::raft::GetLeaseRequest;
use crate::raft::test_support::*;

/// Retries `req` until it's granted or this node has moved on to
/// campaigning itself, whichever comes first -- both outcomes prove
/// a lease-based denial isn't permanent. The two are deliberately
/// racy against each other here: the same deadline that ends the
/// lease also starts this node's own candidacy timer (see the module
/// doc on why the two were merged), so on an isolated node (no real
/// peer refreshing anything) there's no way to guarantee which one
/// wins on any given run. Returns the granted response, or `None` if
/// it moved on to campaigning instead.
async fn wait_for_vote_grant_or_role_change(
    peer_handle: &RaftPeerHandle,
    role_rx: &mut watch::Receiver<RoleHandle>,
    req: &RequestVoteRequest,
) -> Option<RequestVoteResponse> {
    let deadline = std::time::Instant::now() + LEASE_DURATION * 10;
    loop {
        if !matches!(&*role_rx.borrow(), RoleHandle::Follower(_)) {
            return None;
        }
        let resp = peer_handle.request_vote(req.clone()).await.unwrap();
        if resp.vote_granted {
            return Some(resp);
        }
        assert!(
            std::time::Instant::now() < deadline,
            "neither granted the vote nor moved on to campaigning within the deadline"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn append_entries_with_higher_term_is_accepted_and_advances_term() {
    let (_role_rx, peer_handle, path) = spawn_test_node(vec![]);

    let resp = peer_handle
        .append_entries(blank_append_entries(5, "leader-1"))
        .await
        .unwrap();

    assert_eq!(resp.term, 5);
    assert!(resp.success);
    cleanup(&path);
}

#[tokio::test]
async fn heartbeat_on_an_empty_log_matches_at_index_zero() {
    let (_log, _role_rx, peer_handle, _commit_rx, _compact_rx, path) =
        spawn_node_with_handles(vec![]);

    let resp = peer_handle
        .append_entries(blank_append_entries(1, "leader-1"))
        .await
        .unwrap();

    assert!(resp.success);
    assert_eq!(resp.match_index, 0);
    cleanup(&path);
}

#[tokio::test]
async fn append_entries_appends_fresh_entries_to_an_empty_log() {
    let (log, _role_rx, peer_handle, _commit_rx, _compact_rx, path) =
        spawn_node_with_handles(vec![]);

    let entries = vec![
        log_entry(1, 1, b"a", Some(b"1")),
        log_entry(2, 1, b"b", Some(b"2")),
    ];
    let resp = peer_handle
        .append_entries(append_entries(1, "leader-1", 0, 0, entries, 0))
        .await
        .unwrap();

    assert!(resp.success);
    assert_eq!(resp.match_index, 2);
    // `leader_commit` is 0 here, so nothing has committed yet --
    // `get_latest` wouldn't reflect these entries even once applied
    // (see storage's module doc), so check `log` directly instead.
    let stored = log.entries_in_range(1, 3).unwrap();
    assert_eq!(stored.len(), 2);
    assert_eq!(stored[0].payload.writes(), [Mutation::new(*b"a", Some(*b"1"))]);
    assert_eq!(stored[1].payload.writes(), [Mutation::new(*b"b", Some(*b"2"))]);
    cleanup(&path);
}

/// A follower re-appends replicated entries into its own log, so the
/// entry's kind has to survive the round trip -- inferring it from
/// `value` would turn the leader's term-opening no-op into a delete
/// tombstone for the empty key, and the two logs would stop matching.
#[tokio::test]
async fn append_entries_preserves_a_replicated_noop_rather_than_flattening_it() {
    let (log, _role_rx, peer_handle, _commit_rx, _compact_rx, path) =
        spawn_node_with_handles(vec![]);

    let noop = LogEntry {
        seq: 1,
        term: 1,
        kind: crate::pb::raft5::common::EntryKind::Noop as i32,
        writes: Vec::new(),
        config: None,
    };
    let resp = peer_handle
        .append_entries(append_entries(
            1,
            "leader-1",
            0,
            0,
            vec![noop, log_entry(2, 1, b"a", Some(b"1"))],
            0,
        ))
        .await
        .unwrap();

    assert!(resp.success);
    assert_eq!(resp.match_index, 2);

    let stored = log.entries_in_range(1, 3).unwrap();
    assert_eq!(stored[0].payload, storage::EntryPayload::NoOp);
    assert_eq!(
        stored[1].payload,
        storage::EntryPayload::Write(vec![Mutation::new(*b"a", Some(*b"1"))])
    );
    cleanup(&path);
}

/// A configuration takes effect the moment it is *appended* (Raft
/// §4.1), not when it commits -- so a replicated `Config` entry has
/// to change the follower's membership immediately, with
/// `leader_commit` still at 0.
#[tokio::test]
async fn a_replicated_config_entry_takes_effect_on_append_not_on_commit() {
    let before = cluster_config(&[("test-node", true), ("v1", true)]);
    let (
        log,
        NodeHandles {
            common,
            mut role_rx,
            peer_handle,
            commit_rx,
            ..
        },
        path,
    ) = open_temp_log_with_config("test-node", &before);
    tokio::spawn(run(common));
    wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Follower(_))).await;

    let after = cluster_config(&[("test-node", true), ("v1", true), ("v2", true)]);
    let entry = LogEntry {
        seq: 2,
        term: 1,
        kind: crate::pb::raft5::common::EntryKind::Config as i32,
        writes: Vec::new(),
        config: Some(after.to_bytes()),
    };
    let resp = peer_handle
        .append_entries(append_entries(1, "v1", 1, 1, vec![entry], 0))
        .await
        .unwrap();
    assert!(resp.success);
    assert_eq!(*commit_rx.borrow(), 0, "nothing has committed");

    // Visible from storage straight away, uncommitted and unapplied.
    assert_eq!(
        ClusterConfig::from_bytes(&log.current_config().unwrap().unwrap()).unwrap(),
        after
    );
    cleanup(&path);
}

/// The flip side of activating on append: if the entry carrying a
/// configuration is truncated away by a conflicting leader, the
/// previous configuration is in force again.
#[tokio::test]
async fn a_truncated_config_entry_reverts_the_active_configuration() {
    let before = cluster_config(&[("test-node", true), ("v1", true)]);
    let (
        log,
        NodeHandles {
            common,
            mut role_rx,
            peer_handle,
            ..
        },
        path,
    ) = open_temp_log_with_config("test-node", &before);
    tokio::spawn(run(common));
    wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Follower(_))).await;

    let after = cluster_config(&[("test-node", true), ("v1", true), ("v2", true)]);
    let config_entry = LogEntry {
        seq: 2,
        term: 1,
        kind: crate::pb::raft5::common::EntryKind::Config as i32,
        writes: Vec::new(),
        config: Some(after.to_bytes()),
    };
    peer_handle
        .append_entries(append_entries(1, "v1", 1, 1, vec![config_entry], 0))
        .await
        .unwrap();

    // A leader from a later term overwrites seq 2 with something
    // else, discarding the configuration that lived there.
    let resp = peer_handle
        .append_entries(append_entries(
            2,
            "v2",
            1,
            1,
            vec![log_entry(2, 2, b"a", Some(b"1"))],
            0,
        ))
        .await
        .unwrap();
    assert!(resp.success);
    assert_eq!(
        ClusterConfig::from_bytes(&log.current_config().unwrap().unwrap()).unwrap(),
        before
    );
    cleanup(&path);
}

/// Promotion has to rebuild the election timer: `Follower::new` arms
/// one only for a voter, so a learner that is promoted while sitting
/// in `Follower` would otherwise never campaign again.
#[tokio::test]
async fn a_learner_promoted_by_a_replicated_config_starts_campaigning() {
    let before = cluster_config(&[("test-node", false), ("v1", true), ("v2", true)]);
    let (
        _log,
        NodeHandles {
            common,
            mut role_rx,
            peer_handle,
            ..
        },
        path,
    ) = open_temp_log_with_config("test-node", &before);
    tokio::spawn(run(common));
    wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Follower(_))).await;

    let after = cluster_config(&[("test-node", true), ("v1", true), ("v2", true)]);
    let entry = LogEntry {
        seq: 2,
        term: 1,
        kind: crate::pb::raft5::common::EntryKind::Config as i32,
        writes: Vec::new(),
        config: Some(after.to_bytes()),
    };
    peer_handle
        .append_entries(append_entries(1, "v1", 1, 1, vec![entry], 0))
        .await
        .unwrap();

    // Its peers are unreachable, so it can't win -- but it must at
    // least start trying, which it never did as a learner.
    wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Candidate)).await;
    cleanup(&path);
}

#[tokio::test]
async fn append_entries_rejects_and_hints_next_index_when_log_is_too_short() {
    let (_log, _role_rx, peer_handle, _commit_rx, _compact_rx, path) =
        spawn_node_with_handles(vec![]);

    let resp = peer_handle
        .append_entries(append_entries(1, "leader-1", 5, 1, vec![], 0))
        .await
        .unwrap();

    assert!(!resp.success);
    assert_eq!(resp.conflict_index, Some(1));
    assert_eq!(resp.conflict_term, None);
    cleanup(&path);
}

#[tokio::test]
async fn append_entries_rejects_and_hints_the_start_of_the_conflicting_term_run() {
    let (_log, _role_rx, peer_handle, _commit_rx, _compact_rx, path) =
        spawn_node_with_handles(vec![]);

    let seed = vec![
        log_entry(1, 1, b"a", Some(b"1")),
        log_entry(2, 1, b"b", Some(b"2")),
        log_entry(3, 1, b"c", Some(b"3")),
    ];
    peer_handle
        .append_entries(append_entries(1, "leader-1", 0, 0, seed, 0))
        .await
        .unwrap();

    // Claims prev_log_index 3 was written at term 2, but the follower
    // actually has term 1 there (and at 1, 2 too) -- the fast-backup
    // hint should point at the start of that whole term-1 run, not
    // just one index back.
    let resp = peer_handle
        .append_entries(append_entries(2, "leader-1", 3, 2, vec![], 0))
        .await
        .unwrap();

    assert!(!resp.success);
    assert_eq!(resp.conflict_term, Some(1));
    assert_eq!(resp.conflict_index, Some(1));
    cleanup(&path);
}

#[tokio::test]
async fn append_entries_truncates_a_conflicting_suffix_and_applies_the_leaders_entries() {
    let (log, _role_rx, peer_handle, _commit_rx, _compact_rx, path) =
        spawn_node_with_handles(vec![]);

    // Follower picks up a stale entry at seq 2 from an old leader.
    let stale = vec![
        log_entry(1, 1, b"a", Some(b"1")),
        log_entry(2, 1, b"stale", Some(b"old")),
    ];
    peer_handle
        .append_entries(append_entries(1, "leader-1", 0, 0, stale, 0))
        .await
        .unwrap();

    // A new leader (higher term) overwrites seq 2 onward.
    let fresh = vec![log_entry(2, 2, b"fresh", Some(b"new"))];
    let resp = peer_handle
        .append_entries(append_entries(2, "leader-2", 1, 1, fresh, 0))
        .await
        .unwrap();

    assert!(resp.success);
    assert_eq!(resp.match_index, 2);
    // `leader_commit` is 0 here, so `get_latest` wouldn't reflect any
    // of this regardless -- check `log` directly: the stale entry at
    // seq 2 is gone, replaced by the new leader's entry at the same
    // seq (a keyspace can only hold one value per exact seq, so this
    // alone proves the old one didn't survive).
    let stored = log.entries_in_range(2, 3).unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].payload.writes()[0].key, b"fresh");
    assert_eq!(stored[0].payload.writes()[0].value, Some(b"new".to_vec()));
    cleanup(&path);
}

#[tokio::test]
async fn append_entries_advances_commit_to_the_min_of_leader_commit_and_match_index() {
    let (_log, _role_rx, peer_handle, mut commit_rx, _compact_rx, path) =
        spawn_node_with_handles(vec![]);

    let entries = vec![
        log_entry(1, 1, b"a", Some(b"1")),
        log_entry(2, 1, b"b", Some(b"2")),
    ];
    // leader_commit (1) is lower than match_index (2), so commit should
    // land at 1, not 2.
    peer_handle
        .append_entries(append_entries(1, "leader-1", 0, 0, entries, 1))
        .await
        .unwrap();

    commit_rx.changed().await.unwrap();
    assert_eq!(*commit_rx.borrow(), 1);
    cleanup(&path);
}

#[tokio::test]
async fn request_vote_grants_first_vote_then_denies_a_second_in_the_same_term() {
    let (_role_rx, peer_handle, path) = spawn_test_node(vec![]);

    let first = peer_handle
        .request_vote(request_vote(3, "candidate-a"))
        .await
        .unwrap();
    assert!(first.vote_granted);

    let second = peer_handle
        .request_vote(request_vote(3, "candidate-b"))
        .await
        .unwrap();
    assert!(!second.vote_granted);
    assert_eq!(second.term, 3);

    cleanup(&path);
}

#[tokio::test]
async fn request_vote_with_a_higher_term_is_granted_even_after_voting_this_term() {
    let (mut role_rx, peer_handle, path) = spawn_test_node(vec![]);

    peer_handle
        .request_vote(request_vote(3, "candidate-a"))
        .await
        .unwrap();

    // Granting that vote just renewed this follower's lease (voting
    // for a prospective leader defers our own campaign, and the two
    // deadlines are deliberately merged into one -- see the module
    // doc), so a second, different candidate can't get a vote until
    // that lease naturally lapses, regardless of term. Once it does,
    // this node's own candidacy timer -- the very same deadline --
    // may also fire first; see `wait_for_vote_grant_or_role_change`.
    let resp = wait_for_vote_grant_or_role_change(
        &peer_handle,
        &mut role_rx,
        &request_vote(4, "candidate-b"),
    )
    .await;

    if let Some(resp) = resp {
        assert!(resp.vote_granted);
        assert_eq!(resp.term, 4);
    }
    cleanup(&path);
}

#[tokio::test]
async fn request_vote_denies_a_candidate_whose_log_ends_at_an_older_term() {
    let (_log, _role_rx, peer_handle, _commit_rx, _compact_rx, path) =
        spawn_node_with_handles(vec![]);

    // Follower's log ends at term 2.
    let entries = vec![log_entry(1, 2, b"a", Some(b"1"))];
    peer_handle
        .append_entries(append_entries(2, "leader-1", 0, 0, entries, 0))
        .await
        .unwrap();

    // Candidate's RPC term (3) is higher, but its own log ends at an
    // *older* term (1) than ours (2) -- must be denied regardless of
    // how much longer its log claims to be (index 5).
    let resp = peer_handle
        .request_vote(RequestVoteRequest {
            term: 3,
            candidate_id: "candidate-a".to_string(),
            last_log_index: 5,
            last_log_term: 1,
            dry_run: false,
        })
        .await
        .unwrap();

    assert!(!resp.vote_granted);
    cleanup(&path);
}

#[tokio::test]
async fn request_vote_grants_a_candidate_whose_log_is_at_least_as_up_to_date() {
    let (_log, mut role_rx, peer_handle, _commit_rx, _compact_rx, path) =
        spawn_node_with_handles(vec![]);

    let entries = vec![log_entry(1, 2, b"a", Some(b"1"))];
    peer_handle
        .append_entries(append_entries(2, "leader-1", 0, 0, entries, 0))
        .await
        .unwrap();

    // The AppendEntries above just renewed this follower's lease on
    // leader-1, so it'll refuse any vote request until that lease
    // naturally expires -- wait it out (or for this node's own
    // candidacy timer, the same deadline, to fire first; see
    // `wait_for_vote_grant_or_role_change`) before expecting a grant.
    //
    // Same term as our last entry (2) and the same index (1) --
    // "at least as up to date" per §5.4.1, so this should be granted.
    let req = RequestVoteRequest {
        term: 3,
        candidate_id: "candidate-a".to_string(),
        last_log_index: 1,
        last_log_term: 2,
        dry_run: false,
    };
    let resp = wait_for_vote_grant_or_role_change(&peer_handle, &mut role_rx, &req).await;

    if let Some(resp) = resp {
        assert!(resp.vote_granted);
    }
    cleanup(&path);
}

#[tokio::test]
async fn granting_a_vote_persists_it_to_storage() {
    // Complements storage.rs's own round-trip test: this one checks
    // that `Follower::handle_event` actually calls through to
    // persistence, not just that the storage layer works in isolation.
    let (log, path) = open_temp_log();
    let NodeHandles {
        common,
        peer_handle,
        ..
    } = Common::new_seeded(
        "test-node".to_string(),
        vec![],
        Arc::clone(&log),
        CancellationToken::new(),
    )
    .unwrap();
    tokio::spawn(run(common));

    peer_handle
        .request_vote(request_vote(9, "candidate-a"))
        .await
        .unwrap();

    assert_eq!(
        log.term_state().unwrap(),
        (9, Some("candidate-a".to_string()))
    );
    cleanup(&path);
}

#[tokio::test]
async fn dry_run_vote_grants_without_mutating_any_state() {
    let (log, _role_rx, peer_handle, _commit_rx, _compact_rx, path) =
        spawn_node_with_handles(vec![]);

    let resp = peer_handle
        .request_vote(RequestVoteRequest {
            term: 5,
            candidate_id: "candidate-a".to_string(),
            last_log_index: 0,
            last_log_term: 0,
            dry_run: true,
        })
        .await
        .unwrap();

    assert!(resp.vote_granted);
    // A dry run must not actually record anything -- current_term and
    // voted_for stay exactly as they were before this call.
    assert_eq!(log.term_state().unwrap(), (0, None));
    cleanup(&path);
}

#[tokio::test]
async fn dry_run_vote_denies_when_term_is_not_higher() {
    let (_log, _role_rx, peer_handle, _commit_rx, _compact_rx, path) =
        spawn_node_with_handles(vec![]);

    peer_handle
        .append_entries(blank_append_entries(5, "leader-1"))
        .await
        .unwrap();

    // A dry run at the SAME term (not strictly higher) must be denied
    // -- granting would mean "yes, you could win term 5", but this
    // follower already recognizes term 5 as current (and possibly
    // already voted in it for real).
    let resp = peer_handle
        .request_vote(RequestVoteRequest {
            term: 5,
            candidate_id: "candidate-a".to_string(),
            last_log_index: 0,
            last_log_term: 0,
            dry_run: true,
        })
        .await
        .unwrap();

    assert!(!resp.vote_granted);
    cleanup(&path);
}

#[tokio::test]
async fn dry_run_vote_denies_when_candidates_log_is_behind() {
    let (_log, _role_rx, peer_handle, _commit_rx, _compact_rx, path) =
        spawn_node_with_handles(vec![]);

    let entries = vec![log_entry(1, 2, b"a", Some(b"1"))];
    peer_handle
        .append_entries(append_entries(2, "leader-1", 0, 0, entries, 0))
        .await
        .unwrap();

    // Term is high enough (3 > 2), but the candidate's log ends at an
    // older term (1) than ours (2).
    let resp = peer_handle
        .request_vote(RequestVoteRequest {
            term: 3,
            candidate_id: "candidate-a".to_string(),
            last_log_index: 5,
            last_log_term: 1,
            dry_run: true,
        })
        .await
        .unwrap();

    assert!(!resp.vote_granted);
    cleanup(&path);
}

#[tokio::test]
async fn a_granted_dry_run_does_not_block_a_later_real_vote_for_someone_else() {
    let (_log, _role_rx, peer_handle, _commit_rx, _compact_rx, path) =
        spawn_node_with_handles(vec![]);

    // Since a dry run never records anything, a later REAL request at
    // the same term from a *different* candidate must still be
    // evaluated fresh, not implicitly denied because of who the dry
    // run was "for".
    let dry = peer_handle
        .request_vote(RequestVoteRequest {
            term: 5,
            candidate_id: "candidate-a".to_string(),
            last_log_index: 0,
            last_log_term: 0,
            dry_run: true,
        })
        .await
        .unwrap();
    assert!(dry.vote_granted);

    let real = peer_handle
        .request_vote(RequestVoteRequest {
            term: 5,
            candidate_id: "candidate-b".to_string(),
            last_log_index: 0,
            last_log_term: 0,
            dry_run: false,
        })
        .await
        .unwrap();
    assert!(real.vote_granted);
    cleanup(&path);
}

#[tokio::test]
async fn get_lease_grants_when_term_matches_and_denies_otherwise() {
    let (mut role_rx, peer_handle, path) = spawn_test_node(vec![]);
    // `get_lease` bypasses the event loop entirely (that's the whole
    // point), so unlike append_entries/request_vote it doesn't
    // naturally wait for the driver to actually start -- confirm the
    // follower role is really published first.
    wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Follower(_))).await;

    // Follower starts at term 0 -- a lease request for term 0 matches.
    let resp = peer_handle.get_lease(GetLeaseRequest {
        leader_id: "leader-1".to_string(),
        term: 0,
    });
    assert!(resp.lease_granted);

    // A term that doesn't match the follower's current term is denied.
    let resp = peer_handle.get_lease(GetLeaseRequest {
        leader_id: "leader-1".to_string(),
        term: 5,
    });
    assert!(!resp.lease_granted);

    cleanup(&path);
}

#[tokio::test]
async fn a_granted_lease_blocks_a_vote_request_for_someone_else() {
    let (mut role_rx, peer_handle, path) = spawn_test_node(vec![]);
    wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Follower(_))).await;

    let resp = peer_handle.get_lease(GetLeaseRequest {
        leader_id: "leader-1".to_string(),
        term: 0,
    });
    assert!(resp.lease_granted);

    // A vote request arriving while the lease is still active must be
    // refused outright -- even a real request with a higher term and
    // a perfectly fine log.
    let resp = peer_handle
        .request_vote(RequestVoteRequest {
            term: 99,
            candidate_id: "candidate-a".to_string(),
            last_log_index: 0,
            last_log_term: 0,
            dry_run: false,
        })
        .await
        .unwrap();
    assert!(!resp.vote_granted);

    // Same for a pre-vote.
    let resp = peer_handle
        .request_vote(RequestVoteRequest {
            term: 99,
            candidate_id: "candidate-a".to_string(),
            last_log_index: 0,
            last_log_term: 0,
            dry_run: true,
        })
        .await
        .unwrap();
    assert!(!resp.vote_granted);

    cleanup(&path);
}

#[tokio::test]
async fn a_vote_request_is_granted_normally_once_the_lease_expires() {
    let (mut role_rx, peer_handle, path) = spawn_test_node(vec![]);
    wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Follower(_))).await;

    peer_handle.get_lease(GetLeaseRequest {
        leader_id: "leader-1".to_string(),
        term: 0,
    });

    // Wait for the lease to naturally expire -- or, since it's the
    // same deadline, for this node's own candidacy timer to fire
    // first; see `wait_for_vote_grant_or_role_change`.
    let req = RequestVoteRequest {
        term: 1,
        candidate_id: "candidate-a".to_string(),
        last_log_index: 0,
        last_log_term: 0,
        dry_run: false,
    };
    let resp = wait_for_vote_grant_or_role_change(&peer_handle, &mut role_rx, &req).await;

    if let Some(resp) = resp {
        assert!(resp.vote_granted);
    }
    cleanup(&path);
}

/// A follower must not transition to candidate before its lease
/// expires -- repeated `GetLease` grants (standing in for the
/// leader's periodic probes) should keep deferring the election
/// timer indefinitely, well past when it would otherwise have fired.
#[tokio::test]
async fn periodic_lease_grants_prevent_becoming_a_candidate() {
    let (mut role_rx, peer_handle, path) = spawn_test_node(vec![]);
    wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Follower(_))).await;

    let deadline = std::time::Instant::now() + LEASE_DURATION * 3;
    while std::time::Instant::now() < deadline {
        let resp = peer_handle.get_lease(GetLeaseRequest {
            leader_id: "leader-1".to_string(),
            term: 0,
        });
        assert!(resp.lease_granted);
        tokio::time::sleep(HEARTBEAT).await;
    }

    assert!(matches!(&*role_rx.borrow(), RoleHandle::Follower(_)));
    cleanup(&path);
}

#[tokio::test]
async fn install_snapshot_applies_state_and_advances_watermarks() {
    let (source_log, source_path) = open_temp_log();
    source_log.append(3, b"a", Some(b"1")).unwrap();
    let s2 = source_log.append(3, b"b", Some(b"2")).unwrap();
    // `export_snapshot` only ever reflects committed state -- apply
    // first so there's actually something for it to export.
    source_log.apply_committed(s2).unwrap();
    let mut buf = Vec::new();
    let meta = source_log.export_snapshot(&mut buf).unwrap();

    let (log, mut role_rx, peer_handle, mut commit_rx, mut compact_rx, path) =
        spawn_node_with_handles(vec![]);
    wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Follower(_))).await;

    let resp = peer_handle
        .install_snapshot(
            SnapshotHeader {
                term: 5,
                leader_id: "leader-1".to_string(),
                last_included_index: meta.last_included_seq,
                last_included_term: meta.last_included_term,
            },
            reader_over(buf),
        )
        .await
        .unwrap();

    assert_eq!(resp.term, 5);
    // `install_snapshot` repopulates `latest` at `last_included_seq`
    // for every key, regardless of the seq each was originally
    // written at in the source log.
    assert_eq!(
        log.get_latest(b"a").unwrap(),
        Some((meta.last_included_seq, b"1".to_vec()))
    );
    assert_eq!(
        log.get_latest(b"b").unwrap(),
        Some((meta.last_included_seq, b"2".to_vec()))
    );

    commit_rx.changed().await.unwrap();
    assert_eq!(*commit_rx.borrow(), meta.last_included_seq);
    compact_rx.changed().await.unwrap();
    assert_eq!(*compact_rx.borrow(), meta.last_included_seq);

    cleanup(&path);
    cleanup(&source_path);
}

#[tokio::test]
async fn install_snapshot_rejects_a_stale_term_and_does_not_touch_storage() {
    let (log, mut role_rx, peer_handle, _commit_rx, _compact_rx, path) =
        spawn_node_with_handles(vec![]);
    wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Follower(_))).await;

    peer_handle
        .append_entries(blank_append_entries(5, "leader-1"))
        .await
        .unwrap();

    let resp = peer_handle
        .install_snapshot(
            SnapshotHeader {
                term: 2,
                leader_id: "leader-2".to_string(),
                last_included_index: 10,
                last_included_term: 1,
            },
            reader_over(Vec::new()),
        )
        .await
        .unwrap();

    assert_eq!(resp.term, 5);
    assert_eq!(log.last_entry().unwrap(), None);
    cleanup(&path);
}

/// A transaction is one entry carrying every key it wrote, so a follower
/// that replayed it with the right seq, term and kind but *fewer
/// mutations* would still satisfy log matching -- (seq, term) is all that
/// compares -- and the divergence would become the cluster's truth at the
/// next leader change. The mutations have to arrive too.
#[tokio::test]
async fn append_entries_replays_every_mutation_of_a_multi_key_entry() {
    let (log, _role_rx, peer_handle, _commit_rx, _compact_rx, path) =
        spawn_node_with_handles(vec![]);

    let txn = write_entry(
        1,
        1,
        vec![
            (b"a".to_vec(), Some(b"1".to_vec())),
            (b"b".to_vec(), Some(b"2".to_vec())),
            (b"gone".to_vec(), None),
        ],
    );
    let resp = peer_handle
        .append_entries(append_entries(1, "leader-1", 0, 0, vec![txn], 0))
        .await
        .unwrap();

    assert!(resp.success);
    let stored = log.entries_in_range(1, 2).unwrap();
    assert_eq!(
        stored[0].payload,
        storage::EntryPayload::Write(vec![
            Mutation::new(*b"a", Some(*b"1")),
            Mutation::new(*b"b", Some(*b"2")),
            Mutation::new(*b"gone", None::<Vec<u8>>),
        ]),
        "the follower's copy of a transaction must be the leader's, mutation for mutation"
    );
    cleanup(&path);
}

/// An entry whose kind this build doesn't recognize is refused, not
/// guessed at. Guessing used to be safe -- the unknown value defaulted to
/// a one-key write, which was wrong but visibly so. It isn't safe now:
/// the guess would produce a write with no mutations, an entry that
/// consumes a seq and satisfies log matching while silently dropping
/// every key it was carrying.
#[tokio::test]
async fn append_entries_refuses_an_entry_whose_kind_it_does_not_recognize() {
    let (log, _role_rx, peer_handle, _commit_rx, _compact_rx, path) =
        spawn_node_with_handles(vec![]);

    let alien = LogEntry {
        seq: 1,
        term: 1,
        kind: 9999,
        writes: Vec::new(),
        config: None,
    };
    let resp = peer_handle
        .append_entries(append_entries(1, "leader-1", 0, 0, vec![alien], 0))
        .await;

    assert!(
        resp.is_err(),
        "an unrecognized entry must fail the batch, not be appended as something else"
    );
    assert_eq!(
        log.entries_in_range(1, 2).unwrap().len(),
        0,
        "nothing should have reached the log"
    );
    cleanup(&path);
}
