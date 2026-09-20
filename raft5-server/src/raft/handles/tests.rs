use std::time::Duration;

use super::*;
use crate::raft::test_support::{cleanup, open_temp_log};

/// Everything needed to drive one `LeaderHandle` by hand: the states
/// worth testing here -- a leader that has not caught up, a leader
/// deposed while something waits on it -- are otherwise only reachable
/// in the split second after an election.
struct TestLeader {
    handle: LeaderHandle,
    applied_tx: watch::Sender<u64>,
    role_tx: watch::Sender<RoleHandle>,
    path: std::path::PathBuf,
    /// Nothing reads these; holding them open is what stops the handle
    /// seeing a closed channel and concluding the node has shut down.
    _event_rx: mpsc::Receiver<Event>,
    _quorum_tx: watch::Sender<bool>,
}

fn test_leader(noop_seq: u64) -> TestLeader {
    let (log, path) = open_temp_log();
    let (applied_tx, applied_rx) = watch::channel(0);
    let (role_tx, role_rx) = watch::channel(RoleHandle::Stopped);
    let (event_tx, event_rx) = mpsc::channel(1);
    let (quorum_tx, read_quorum_rx) = watch::channel(true);

    let handle = LeaderHandle {
        store: Store::new(log),
        term: 1,
        event_tx,
        applied_rx,
        role_rx,
        read_quorum_rx,
        noop_seq,
        log_tx: Arc::new(watch::channel(0).0),
    };
    // Published over the very channel it holds a receiver for -- the
    // same self-reference a real leader has (see `RoleHandle::Stopped`
    // for what that costs), and what makes `still_leader` say yes.
    role_tx.send(RoleHandle::Leader(handle.clone())).unwrap();

    TestLeader {
        handle,
        applied_tx,
        role_tx,
        path,
        _event_rx: event_rx,
        _quorum_tx: quorum_tx,
    }
}

/// The gate a conditional write and a linearizable read both wait on:
/// until this leader's own no-op has been *applied*, `latest` may still
/// be missing writes a previous leader committed, and anything that
/// answers from it would answer from a gap.
#[tokio::test]
async fn catching_up_waits_for_this_leader_to_apply_its_own_noop() {
    let leader = test_leader(5);

    assert!(
        tokio::time::timeout(Duration::from_millis(50), leader.handle.await_caught_up())
            .await
            .is_err(),
        "a leader that has applied nothing cannot answer from `latest` yet"
    );

    // Replicated and committed, but the tail it inherited is not in
    // `latest` yet -- which is the whole distinction being drawn here.
    leader.applied_tx.send_replace(4);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), leader.handle.await_caught_up())
            .await
            .is_err(),
        "one entry short is still short"
    );

    leader.applied_tx.send_replace(5);
    tokio::time::timeout(Duration::from_millis(500), leader.handle.await_caught_up())
        .await
        .expect("applying the no-op should release the wait")
        .expect("this node is still the leader");

    cleanup(&leader.path);
}

/// Waiting to catch up is not waiting forever. A leader deposed while
/// someone waits on it will never apply that no-op, so the waiter is
/// sent to whoever replaced it instead of hanging.
#[tokio::test]
async fn a_deposed_leader_stops_waiting_to_catch_up() {
    let leader = test_leader(5);
    let handle = leader.handle.clone();
    let waiting = tokio::spawn(async move { handle.await_caught_up().await });

    leader
        .role_tx
        .send_replace(RoleHandle::Follower(FollowerHandle {
            leader_hint: Some("n2".to_string()),
        }));

    let err = tokio::time::timeout(Duration::from_secs(2), waiting)
        .await
        .expect("being deposed should end the wait")
        .expect("the waiting task should not panic")
        .expect_err("a deposed leader cannot report itself current");
    assert!(
        matches!(err, RaftError::NotLeader { leader_hint } if leader_hint.as_deref() == Some("n2")),
        "the waiter should be pointed at the new leader",
    );

    cleanup(&leader.path);
}
