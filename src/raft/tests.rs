use super::test_support::*;
use super::*;

#[tokio::test]
async fn fresh_node_starts_as_follower() {
    let (mut role_rx, _peer_handle, path) = spawn_test_node(vec![]);
    wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Follower(_))).await;
    cleanup(&path);
}

/// A node started against a fresh database belongs to no cluster: it
/// answers RPCs and waits, but never campaigns, however long it hears
/// nothing (Raft §4.4 -- a node that assumed it was its own cluster
/// would elect itself, and a second such node would be a second
/// cluster).
#[tokio::test]
async fn an_unbootstrapped_node_waits_instead_of_electing_itself() {
    let (log, path) = open_temp_log();
    let NodeHandles {
        common,
        mut role_rx,
        ..
    } = Common::new(
        "fresh".to_string(),
        Arc::clone(&log),
        CancellationToken::new(),
    )
    .unwrap();
    assert!(common.config.members.is_empty());
    tokio::spawn(run(common));

    wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Uninitialized)).await;
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert!(
        matches!(*role_rx.borrow(), RoleHandle::Uninitialized),
        "an unbootstrapped node must never campaign"
    );
    assert!(
        log.last_entry().unwrap().is_none(),
        "and must write nothing"
    );

    cleanup(&path);
}

/// Bootstrapping writes the configuration as the log's first entry,
/// after which the node is a single-node cluster and elects itself.
#[tokio::test]
async fn bootstrap_forms_a_single_node_cluster_that_then_elects_itself() {
    let (log, path) = open_temp_log();
    let NodeHandles {
        common,
        mut role_rx,
        ..
    } = Common::new(
        "solo".to_string(),
        Arc::clone(&log),
        CancellationToken::new(),
    )
    .unwrap();
    let client = common.client_handle();
    tokio::spawn(run(common));
    wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Uninitialized)).await;

    let config = cluster_config(&[("solo", true)]);
    client.bootstrap(config.clone()).await.unwrap();

    let stored = log.entries_in_range(1, 2).unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].kind, storage::EntryKind::Config);
    assert_eq!(
        ClusterConfig::from_bytes(stored[0].value.as_ref().unwrap()).unwrap(),
        config
    );

    wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Leader(_))).await;
    cleanup(&path);
}

/// Bootstrapping twice is how one cluster silently becomes two, so a
/// node that is already part of one refuses -- whether it learned its
/// membership from a bootstrap or, as here, from replication.
#[tokio::test]
async fn bootstrap_is_refused_once_the_node_belongs_to_a_cluster() {
    let (
        _log,
        NodeHandles {
            common,
            mut role_rx,
            ..
        },
        path,
    ) = open_temp_log_with_config("solo", &cluster_config(&[("solo", true)]));
    let client = common.client_handle();
    tokio::spawn(run(common));
    wait_for_role(&mut role_rx, |r| !matches!(r, RoleHandle::Starting)).await;

    let err = client
        .bootstrap(cluster_config(&[("solo", true), ("interloper", true)]))
        .await
        .unwrap_err();
    assert!(matches!(err, RaftError::AlreadyInitialized));

    cleanup(&path);
}

/// The other half of the guard: an empty configuration isn't enough
/// on its own. A node with log entries but no configuration has state
/// that a bootstrap would silently strand -- it belongs to some
/// cluster whose config entry it simply hasn't received yet.
#[tokio::test]
async fn bootstrap_is_refused_on_a_non_empty_log() {
    let (log, path) = open_temp_log();
    log.append(1, b"a", Some(b"1")).unwrap();
    let NodeHandles {
        common,
        mut role_rx,
        ..
    } = Common::new(
        "odd-one".to_string(),
        Arc::clone(&log),
        CancellationToken::new(),
    )
    .unwrap();
    let client = common.client_handle();
    tokio::spawn(run(common));
    wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Uninitialized)).await;

    let err = client
        .bootstrap(cluster_config(&[("odd-one", true)]))
        .await
        .unwrap_err();
    assert!(matches!(err, RaftError::AlreadyInitialized));
    assert_eq!(log.last_entry().unwrap().unwrap().0, 1, "nothing appended");

    cleanup(&path);
}

/// The path a server added to an existing cluster takes: it starts
/// uninitialized and is initialized purely by replication, with no
/// bootstrap anywhere.
#[tokio::test]
async fn a_replicated_config_entry_initializes_a_fresh_node() {
    let (log, path) = open_temp_log();
    let NodeHandles {
        common,
        mut role_rx,
        peer_handle,
        ..
    } = Common::new(
        "newcomer".to_string(),
        Arc::clone(&log),
        CancellationToken::new(),
    )
    .unwrap();
    tokio::spawn(run(common));
    wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Uninitialized)).await;

    // The leader adds it as a learner, so it is a member but still
    // has no vote -- and so still doesn't campaign.
    let config = cluster_config(&[("leader-1", true), ("newcomer", false)]);
    let entry = LogEntry {
        seq: 1,
        term: 1,
        kind: crate::pb::raft5::common::EntryKind::Config as i32,
        key: Vec::new(),
        value: Some(config.to_bytes()),
    };
    let resp = peer_handle
        .append_entries(append_entries(1, "leader-1", 0, 0, vec![entry], 0))
        .await
        .unwrap();
    assert!(resp.success);

    wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Follower(_))).await;
    assert_eq!(
        ClusterConfig::from_bytes(&log.current_config().unwrap().unwrap()).unwrap(),
        config
    );

    cleanup(&path);
}

/// Membership comes from the log, not from the flag the node was
/// started with: a store that has already been bootstrapped ignores
/// the peer list entirely.
#[tokio::test]
async fn common_takes_its_membership_from_the_log_not_the_peer_argument() {
    let (log, path) = open_temp_log();
    let config = cluster_config(&[("n0", true), ("v1", true), ("l1", false)]);
    log.append_config(1, &config.to_bytes()).unwrap();

    let stale = vec![PeerInfo {
        id: "someone-else".to_string(),
        address: "http://127.0.0.1:1".to_string(),
    }];
    let NodeHandles { common, .. } = Common::new_seeded(
        "n0".to_string(),
        stale,
        Arc::clone(&log),
        CancellationToken::new(),
    )
    .unwrap();

    assert_eq!(common.config, config);
    let mut peer_ids: Vec<_> = common.peers().into_iter().map(|p| p.id).collect();
    peer_ids.sort();
    assert_eq!(peer_ids, vec!["l1", "v1"], "learners are replicated to");
    assert_eq!(
        common
            .voting_peers()
            .into_iter()
            .map(|p| p.id)
            .collect::<Vec<_>>(),
        vec!["v1"],
        "but only voters are asked for votes"
    );
    assert_eq!(common.majority(), 2);
    assert!(common.is_voter());

    cleanup(&path);
}

/// A learner replicates and answers RPCs, but never starts an
/// election: it has no vote, so it could never win one, and a server
/// campaigning outside the voting configuration is exactly the
/// disruption §4.2.3 warns about.
#[tokio::test]
async fn a_learner_never_campaigns() {
    let config = cluster_config(&[("learner", false), ("v1", true), ("v2", true)]);
    let (
        _log,
        NodeHandles {
            common,
            mut role_rx,
            ..
        },
        path,
    ) = open_temp_log_with_config("learner", &config);
    tokio::spawn(run(common));
    wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Follower(_))).await;

    // Comfortably past the LEASE_DURATION + fuzz window (200-400ms)
    // in which a voter with unreachable peers would have campaigned.
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert!(
        matches!(*role_rx.borrow(), RoleHandle::Follower(_)),
        "a learner must stay a follower no matter how long it hears nothing"
    );

    cleanup(&path);
}

/// The control for `a_learner_never_campaigns`: the same unreachable
/// peers, the same silence -- the only difference is the `voting`
/// flag, and it is what decides whether an election happens.
#[tokio::test]
async fn a_voter_with_the_same_unreachable_peers_does_campaign() {
    let config = cluster_config(&[("v0", true), ("v1", true), ("v2", true)]);
    let (
        _log,
        NodeHandles {
            common,
            mut role_rx,
            ..
        },
        path,
    ) = open_temp_log_with_config("v0", &config);
    tokio::spawn(run(common));
    wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Candidate)).await;
    cleanup(&path);
}

#[tokio::test]
async fn follower_becomes_candidate_after_election_timeout() {
    let (mut role_rx, _peer_handle, path) = spawn_test_node(vec![]);
    wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Candidate)).await;
    cleanup(&path);
}

/// End-to-end over real sockets: two full nodes (state machine + a real
/// `RaftPeerService` bound to a real port), each configured with the
/// other as its only peer. Exercises the whole voting path --
/// `Candidate` actually dialing out via `RaftPeerClient`,
/// `PeerConnections` caching/reusing that connection, and the
/// follower-side §5.4.1 check granting the vote -- rather than just the
/// in-process pieces the other tests cover individually.
#[tokio::test]
async fn candidate_wins_election_against_a_real_peer_over_the_network() {
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

    // Whichever of the two wins its election first -- a real,
    // cross-process-style race, same as a real cluster.
    tokio::select! {
        _ = wait_for_role(&mut role_rx_a, |r| matches!(r, RoleHandle::Leader(_))) => {}
        _ = wait_for_role(&mut role_rx_b, |r| matches!(r, RoleHandle::Leader(_))) => {}
    }

    cleanup(&path_a);
    cleanup(&path_b);
}

/// End-to-end over real sockets, continuing where
/// `candidate_wins_election_against_a_real_peer_over_the_network`
/// leaves off: once one of the two nodes wins the election, a write
/// through its `LeaderHandle` should reach the other node's storage
/// too (real replication task, real connection, real §5.3 next_index
/// tracking) and commit (real majority -- both nodes -- computed by
/// `Leader::recompute_watermarks`) rather than hanging.
#[tokio::test]
async fn leader_replicates_a_write_to_a_real_peer_and_commits_it() {
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

    // Leadership can still shuffle right after the initial election
    // settles: e.g. the loser's own election timer, having already
    // been ticking, fires a competing (higher-term) candidacy before
    // it ever hears the winner's first heartbeat, and per Raft the
    // winner correctly steps down on seeing that higher term. That's
    // correct behavior, not a bug -- so retry against whoever's leader
    // now, same as a real client would on `NotLeader`, rather than
    // assuming the first leader observed stays leader.
    let (seq, follower_log) = loop {
        let a_is_leader = matches!(&*role_rx_a.borrow(), RoleHandle::Leader(_));
        let (leader, follower_log) = if a_is_leader {
            (
                expect_leader_handle(&role_rx_a.borrow()),
                Arc::clone(&log_b),
            )
        } else {
            (
                expect_leader_handle(&role_rx_b.borrow()),
                Arc::clone(&log_a),
            )
        };
        match tokio::time::timeout(
            Duration::from_millis(2000),
            leader.append_log(b"k", Some(b"v")),
        )
        .await
        .expect("append_log should commit once the peer replicates it")
        {
            Ok(seq) => break (seq, follower_log),
            Err(RaftError::NotLeader { .. }) => {
                tokio::select! {
                    _ = wait_for_role(&mut role_rx_a, |r| matches!(r, RoleHandle::Leader(_))) => {}
                    _ = wait_for_role(&mut role_rx_b, |r| matches!(r, RoleHandle::Leader(_))) => {}
                }
            }
            Err(e) => panic!("unexpected append_log error: {e:?}"),
        }
    };

    // `append_log` only waits for the leader's own `commit_tx`, which
    // can advance the instant a majority (both nodes, here) has the
    // entry -- give the follower's own AppendEntries handling a moment
    // to actually finish persisting it locally before checking.
    tokio::time::timeout(Duration::from_millis(500), async {
        loop {
            if follower_log.get_latest(b"k").unwrap().is_some() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("follower should eventually receive the replicated entry");

    assert_eq!(
        follower_log.get_latest(b"k").unwrap(),
        Some((seq, b"v".to_vec()))
    );

    cleanup(&path_a);
    cleanup(&path_b);
}

/// A zero-peer cluster's `compact_seq` trivially equals its own last
/// index (see `leader::Leader::recompute_watermarks`) -- confirms the
/// compaction task spawned by `run` actually reacts to that and does
/// real work, not just that the watermark itself advances correctly
/// (already covered elsewhere).
#[tokio::test]
async fn compaction_task_compacts_the_log_as_compact_seq_advances() {
    let (log, mut role_rx, _peer_handle, _commit_rx, mut compact_rx, path) =
        spawn_node_with_handles(vec![]);
    wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Leader(_))).await;
    let leader = expect_leader_handle(&role_rx.borrow());

    // Seqs are captured rather than assumed: seq 1 is the
    // term-opening no-op `Leader::new` appends, so these start at 2.
    let a = leader.append_log(b"a", Some(b"1")).await.unwrap();
    let b = leader.append_log(b"b", Some(b"2")).await.unwrap();
    let c = leader.append_log(b"c", Some(b"3")).await.unwrap();

    tokio::time::timeout(Duration::from_millis(1000), async {
        loop {
            if *compact_rx.borrow() >= c {
                return;
            }
            compact_rx.changed().await.unwrap();
        }
    })
    .await
    .expect("compact_seq should advance to the tail in a zero-peer cluster");

    // The compaction task reacts to that same watch channel -- give
    // it a moment to actually run its (tiny, but still real) blocking
    // I/O.
    tokio::time::timeout(Duration::from_millis(1000), async {
        loop {
            if log.entries_in_range(1, c).unwrap().is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("compaction should have removed the entries below compact_seq");

    // Compaction only removes superseded history -- current values
    // must still be intact.
    assert_eq!(log.get_latest(b"a").unwrap(), Some((a, b"1".to_vec())));
    assert_eq!(log.get_latest(b"b").unwrap(), Some((b, b"2".to_vec())));
    assert_eq!(log.get_latest(b"c").unwrap(), Some((c, b"3".to_vec())));

    cleanup(&path);
}

/// Runs `common` to completion under `cancel`, cancelling once it
/// reaches `role`, and hands back how long the shutdown itself took.
/// Panics if the node doesn't stop, so every caller's real assertion
/// is the one it makes about `log` afterwards.
async fn cancel_once(
    common: Common,
    mut role_rx: watch::Receiver<RoleHandle>,
    cancel: &CancellationToken,
    role: impl Fn(&RoleHandle) -> bool,
) -> Duration {
    let node = tokio::spawn(run(common));
    // Cancelling a node that never got as far as starting its tasks
    // would prove nothing, so wait until it is genuinely running.
    wait_for_role(&mut role_rx, role).await;

    let started = Instant::now();
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), node)
        .await
        .expect("a cancelled node should stop, not hang")
        .expect("the node task panicked");
    started.elapsed()
}

/// Cancellation is how a node is stopped, and `run` returning is the
/// proof that everything underneath it stopped too. The `Arc` count
/// is what actually checks that: a single abandoned task still
/// holding a clone of the log is exactly what leaves the database
/// locked against the next node to open the same directory, and it is
/// invisible from the outside until that next node fails.
///
/// A leader with an unreachable peer is the demanding version of it:
/// the self-tick, the storage maintenance task, and a replication
/// task looping on a connection that never succeeds -- which has to
/// be interrupted rather than merely caught between ticks.
#[tokio::test]
async fn cancelling_a_leader_stops_every_task_under_it() {
    let (log, path) = open_temp_log();
    // `n0` is the only voter, so it wins its own election without
    // anyone's help; `l1` is a learner at an address nothing is
    // listening on, which is what gets it replicated to forever
    // without ever answering. (It has to be a learner rather than a
    // second voter for both halves to hold at once: a second voter
    // would have to answer for `n0` to become leader at all.)
    let config = cluster_config(&[("n0", true), ("l1", false)]);
    log.append_config(1, &config.to_bytes()).unwrap();
    let cancel = CancellationToken::new();
    let NodeHandles {
        common, role_rx, ..
    } = Common::new("n0".to_string(), Arc::clone(&log), cancel.clone()).unwrap();

    let took = cancel_once(common, role_rx, &cancel, |r| {
        matches!(r, RoleHandle::Leader(_))
    })
    .await;

    assert_eq!(
        Arc::strong_count(&log),
        1,
        "something the node spawned still holds its log after it shut down"
    );
    // Well inside `LIGHTWEIGHT_RPC_TIMEOUT`, let alone the 30s
    // `AppendEntries` one: the peer-facing tasks are cut short rather
    // than waited out.
    assert!(
        took < LEASE_DURATION,
        "shutting down took {took:?}, which means something was waited out rather than cancelled"
    );

    cleanup(&path);
}

/// The same contract for a node that has no configuration at all: it
/// publishes `Uninitialized` and arms no election timer, so the path
/// out of it is different from a voter's, and a node left running
/// because nobody was watching the token would be just as invisible.
#[tokio::test]
async fn cancelling_an_uninitialized_node_stops_it_too() {
    let (log, path) = open_temp_log();
    let cancel = CancellationToken::new();
    let NodeHandles {
        common, role_rx, ..
    } = Common::new("fresh".to_string(), Arc::clone(&log), cancel.clone()).unwrap();

    cancel_once(common, role_rx, &cancel, |r| {
        matches!(r, RoleHandle::Uninitialized)
    })
    .await;

    assert_eq!(Arc::strong_count(&log), 1);

    cleanup(&path);
}

/// A node that is already cancelled before it is ever polled still
/// has to come back cleanly rather than, say, settling into being a
/// follower first -- `crate::serve` starts the engine and the servers
/// together, so a server that fails to bind cancels the token while
/// the state machine is still on its first step.
#[tokio::test]
async fn a_node_cancelled_before_it_starts_stops_immediately() {
    let (log, path) = open_temp_log();
    let cancel = CancellationToken::new();
    let NodeHandles { common, .. } =
        Common::new("stillborn".to_string(), Arc::clone(&log), cancel.clone()).unwrap();
    cancel.cancel();

    tokio::time::timeout(Duration::from_secs(5), run(common))
        .await
        .expect("an already-cancelled node should stop, not hang");

    assert_eq!(Arc::strong_count(&log), 1);

    cleanup(&path);
}
