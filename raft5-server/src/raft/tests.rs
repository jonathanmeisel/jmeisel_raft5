use super::test_support::*;
use super::*;

#[tokio::test]
async fn fresh_node_starts_as_follower() {
    let (mut role_rx, _peer_handle, path) = spawn_test_node(vec![]);
    wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Follower(_))).await;
    cleanup(&path);
}

/// A node started on a new database is in no cluster: it answers RPCs and
/// waits, but never campaigns, however long it hears nothing (Raft §4.4: a
/// node that assumed it was its own cluster would elect itself, and a
/// second such node would be a second cluster).
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

/// Bootstrap writes the configuration as the log's first entry, after
/// which the node is a single-node cluster and elects itself.
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
    let storage::EntryPayload::Config(stored_config) = &stored[0].payload else {
        panic!("bootstrap should append a config entry, got {:?}", stored[0].payload);
    };
    assert_eq!(ClusterConfig::from_bytes(stored_config).unwrap(), config);

    wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Leader(_))).await;
    cleanup(&path);
}

/// A second bootstrap is how one cluster silently becomes two, so a node
/// already in a cluster refuses, whether it learned its membership from a
/// bootstrap or, as here, from replication.
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

/// The other half of the guard: an empty configuration is not enough. A
/// node with log entries but no configuration has state that a bootstrap
/// would silently strand: it belongs to some cluster whose configuration
/// entry it has not received yet.
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
/// uninitialized and is initialized only by replication, with no bootstrap
/// anywhere.
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

    // The leader adds it as a learner, so it is a member but has no vote yet,
    // and so still does not campaign.
    let config = cluster_config(&[("leader-1", true), ("newcomer", false)]);
    let entry = LogEntry {
        seq: 1,
        term: 1,
        kind: crate::pb::raft5::common::EntryKind::Config as i32,
        writes: Vec::new(),
        config: Some(config.to_bytes()),
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

/// Membership comes from the log, not from the flag the node was started
/// with: a store already bootstrapped ignores the peer list.
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

/// A learner replicates and answers RPCs, but never starts an election: it
/// has no vote, so it could never win, and a server that campaigns outside
/// the voting configuration is the disruption §4.2.3 warns about.
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

    // Well past the Timing::default().lease() + fuzz window (200-400ms) in
    // which a voter with unreachable peers would have campaigned.
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert!(
        matches!(*role_rx.borrow(), RoleHandle::Follower(_)),
        "a learner must stay a follower no matter how long it hears nothing"
    );

    cleanup(&path);
}

/// The control for `a_learner_never_campaigns`: the same unreachable peers,
/// the same silence. Only the `voting` flag differs, and it decides whether
/// an election happens.
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

/// End to end over real sockets: two full nodes (state machine plus a real
/// `RaftPeerService` on a real port), each with the other as its only peer.
/// Exercises the whole voting path (`Candidate` dialing out through
/// `RaftPeerClient`, `PeerConnections` caching and reusing that connection,
/// and the follower's §5.4.1 check granting the vote), not only the
/// in-process pieces the other tests cover one by one.
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

    // Whichever of the two wins its election first: a real race, as in a real
    // cluster.
    tokio::select! {
        _ = wait_for_role(&mut role_rx_a, |r| matches!(r, RoleHandle::Leader(_))) => {}
        _ = wait_for_role(&mut role_rx_b, |r| matches!(r, RoleHandle::Leader(_))) => {}
    }

    cleanup(&path_a);
    cleanup(&path_b);
}

/// End to end over real sockets, continuing from
/// `candidate_wins_election_against_a_real_peer_over_the_network`: once one
/// node wins the election, a write through its `LeaderHandle` should reach
/// the other node's storage (real replication task, real connection, real
/// §5.3 next_index tracking) and commit (a real majority of both nodes,
/// computed by `Leader::recompute_watermarks`), not hang.
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

    // Leadership can still move right after the first election settles. For
    // example, the loser's election timer, already running, fires a competing
    // higher-term candidacy before it hears the winner's first heartbeat, and
    // per Raft the winner steps down on seeing that higher term. That is
    // correct behavior, not a bug, so retry against the current leader, as a
    // real client would on `NotLeader`, instead of assuming the first leader
    // seen stays leader.
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

    // `append_log` waits only for the leader's own `commit_tx`, which can
    // advance as soon as a majority (both nodes, here) has the entry. Give the
    // follower's AppendEntries handling a moment to finish persisting it before
    // checking.
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

/// A cluster with no peers has `compact_seq` equal to its own last index
/// (see `leader::Leader::recompute_watermarks`). Confirms the compaction
/// task that `run` starts reacts to that and does real work, not only that
/// the watermark advances (covered elsewhere).
#[tokio::test]
async fn compaction_task_compacts_the_log_as_compact_seq_advances() {
    let (log, mut role_rx, _peer_handle, _commit_rx, mut compact_rx, path) =
        spawn_node_with_handles(vec![]);
    wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Leader(_))).await;
    let leader = expect_leader_handle(&role_rx.borrow());

    // Seqs are captured, not assumed: seq 1 is the term-opening no-op that
    // `Leader::new` appends, so these start at 2.
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

    // The compaction task reacts to the same watch channel; give it a moment
    // to run its (small, but real) blocking I/O.
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

    // Compaction only removes superseded history; current values must still be
    // intact.
    assert_eq!(log.get_latest(b"a").unwrap(), Some((a, b"1".to_vec())));
    assert_eq!(log.get_latest(b"b").unwrap(), Some((b, b"2".to_vec())));
    assert_eq!(log.get_latest(b"c").unwrap(), Some((c, b"3".to_vec())));

    cleanup(&path);
}

/// Run `common` to completion under `cancel`, cancel once it reaches `role`,
/// and return how long the shutdown took. Panics if the node does not stop,
/// so each caller's real assertion is the one it makes about `log`
/// afterwards.
async fn cancel_once(
    common: Common,
    mut role_rx: watch::Receiver<RoleHandle>,
    cancel: &CancellationToken,
    role: impl Fn(&RoleHandle) -> bool,
) -> Duration {
    let node = tokio::spawn(run(common));
    // Cancelling a node that never started its tasks would prove nothing, so
    // wait until it is really running.
    wait_for_role(&mut role_rx, role).await;

    let started = Instant::now();
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), node)
        .await
        .expect("a cancelled node should stop, not hang")
        .expect("the node task panicked");
    started.elapsed()
}

/// Cancellation is how a node is stopped, and `run` returning proves that
/// everything below it stopped too. The `Arc` count is what checks that: one
/// abandoned task still holding a clone of the log is exactly what leaves
/// the database locked against the next node to open the same directory,
/// and it cannot be seen from outside until that next node fails.
///
/// A leader with an unreachable peer is the hard version: the self-tick,
/// the storage maintenance task, and a replication task looping on a
/// connection that never succeeds, which must be interrupted, not merely
/// caught between ticks.
#[tokio::test]
async fn cancelling_a_leader_stops_every_task_under_it() {
    let (log, path) = open_temp_log();
    // `n0` is the only voter, so it wins its own election without help. `l1`
    // is a learner at an address nothing listens on, so it gets replicated to
    // forever and never answers. (It must be a learner, not a second voter, for
    // both to hold at once: a second voter would have to answer for `n0` to
    // become leader at all.)
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
    // Well within `Timing::lightweight_rpc_timeout`, let alone the 30s
    // `AppendEntries` timeout: the peer-facing tasks are cut short, not waited
    // out.
    assert!(
        took < Timing::default().lease(),
        "shutting down took {took:?}, which means something was waited out rather than cancelled"
    );

    cleanup(&path);
}

/// The same contract for a node with no configuration at all: it publishes
/// `Uninitialized` and arms no election timer, so its way out differs from
/// a voter's, and a node left running because nobody watched the token
/// would be just as invisible.
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

/// A node cancelled before it is ever polled must still return cleanly,
/// not, for example, become a follower first: `crate::serve` starts the
/// engine and the servers together, so a server that fails to bind cancels
/// the token while the state machine is still on its first step.
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

#[test]
fn timing_derives_everything_from_the_heartbeat() {
    let t = Timing::from_millis(200);
    assert_eq!(t.heartbeat(), Duration::from_millis(200));
    assert_eq!(t.lease(), Duration::from_millis(800));
    assert_eq!(t.lightweight_rpc_timeout(), Duration::from_millis(400));
    assert_eq!(t.idle_replication_interval(), Duration::from_millis(2_000));

    // Unset (0) means the default; anything else is held to the allowed range.
    assert_eq!(Timing::from_millis(0), Timing::default());
    assert_eq!(Timing::default().heartbeat_ms(), DEFAULT_HEARTBEAT_MS);
    assert_eq!(Timing::from_millis(1).heartbeat_ms(), 10);
    assert_eq!(Timing::from_millis(u64::MAX).heartbeat_ms(), 10_000);
    assert_eq!(Timing::of(&ClusterConfig::default()), Timing::default());
}

#[test]
fn election_timeouts_keep_their_jitter_at_a_long_heartbeat() {
    for ms in [DEFAULT_HEARTBEAT_MS, 2_000, 10_000] {
        let t = Timing::from_millis(ms);
        let first = t.election_timeout_for_attempt(0);
        assert!(
            t.lease() <= first && first < t.lease() * 2,
            "{ms}ms: {first:?} is outside [lease, 2*lease)"
        );
        // Enough draws that all landing on one value would mean the window had
        // collapsed, which happens if the backoff cap is below a long heartbeat's
        // first window.
        let draws: std::collections::HashSet<_> =
            (0..32).map(|_| t.election_timeout_for_attempt(0)).collect();
        assert!(draws.len() > 1, "{ms}ms: no jitter in the first window");
    }
    // Backoff still has a ceiling.
    let cap = Duration::from_millis(MAX_ELECTION_BACKOFF_MS);
    assert_eq!(
        Timing::default().election_timeout_for_attempt(u32::MAX),
        cap
    );
}

#[test]
fn heartbeat_bounds_are_checked_where_one_is_asked_for() {
    assert!(check_heartbeat_ms(10).is_ok());
    assert!(check_heartbeat_ms(10_000).is_ok());
    assert!(check_heartbeat_ms(0).is_err());
    assert!(check_heartbeat_ms(9).is_err());
    assert!(check_heartbeat_ms(10_001).is_err());
}
