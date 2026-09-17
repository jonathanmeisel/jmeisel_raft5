use super::*;
use crate::raft::test_support::*;

#[tokio::test]
async fn candidate_steps_down_to_follower_on_append_entries_from_a_leader() {
    let (mut role_rx, peer_handle, path) = spawn_test_node(vec![]);
    wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Candidate)).await;

    // A real leader's AppendEntries at any term at least as high as the
    // candidate's own (we don't know it exactly from out here, but
    // u64::MAX always qualifies) should make it concede the election.
    peer_handle
        .append_entries(blank_append_entries(u64::MAX, "leader-1"))
        .await
        .unwrap();

    wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Follower(_))).await;
    cleanup(&path);
}

#[tokio::test]
async fn candidate_restarts_the_election_after_its_own_timeout() {
    // A real peer that will never answer (nothing listens on this
    // address), so majority (2, with one peer) can never be reached on
    // the self-vote alone -- unlike a *zero*-peer cluster, which wins
    // immediately on its own vote (see
    // `leader::tests::leader_commits_a_write_immediately_in_a_single_node_cluster`).
    // With an unreachable peer, pre-vote itself never reaches
    // majority either, so this never even becomes a *real* candidate
    // -- it just keeps re-publishing `RoleHandle::Candidate` (which
    // covers both pre-vote and real candidacies) from repeated
    // `PreCandidate` attempts.
    let peers = vec![PeerInfo {
        id: "ghost".to_string(),
        address: "http://127.0.0.1:1".to_string(),
    }];
    let (mut role_rx, _peer_handle, path) = spawn_test_node(peers);
    wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Candidate)).await;

    // Without its own timer it would otherwise wait on `event_rx`
    // forever. Just check it keeps re-publishing `Candidate` rather
    // than hanging.
    for _ in 0..2 {
        role_rx.changed().await.unwrap();
        assert!(matches!(&*role_rx.borrow(), RoleHandle::Candidate));
    }
    cleanup(&path);
}

/// Backoff should make later retries (still failing, same unreachable
/// peer) wait noticeably longer than earlier ones, rather than
/// hammering away at the same 150-300ms interval forever.
#[tokio::test]
async fn repeated_election_failures_back_off_the_retry_interval() {
    async fn next_change(role_rx: &mut watch::Receiver<RoleHandle>) -> Duration {
        let start = std::time::Instant::now();
        // Generously above attempt 2's ~1200ms ceiling.
        tokio::time::timeout(Duration::from_millis(2500), role_rx.changed())
            .await
            .expect("expected another role change before the timeout")
            .unwrap();
        start.elapsed()
    }

    let peers = vec![PeerInfo {
        id: "ghost".to_string(),
        address: "http://127.0.0.1:1".to_string(),
    }];
    let (mut role_rx, _peer_handle, path) = spawn_test_node(peers);

    // Follower's own timeout (attempt 0's range) firing into the
    // first `PreCandidate` attempt -- just where we start counting
    // from, not one of the two gaps compared below.
    wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Candidate)).await;

    // Gap A: the first `PreCandidate` campaign's own timeout (attempt
    // 0: 150-300ms) firing and republishing on retry.
    let gap_a = next_change(&mut role_rx).await;
    // Skip one retry (attempt 1: 300-600ms) -- comparing against
    // attempt 2 instead of attempt 1 gives a much wider margin
    // (150-300ms vs. 600-1200ms, rather than adjacent ranges sharing
    // a 300ms boundary), so this assertion isn't sensitive to a bit
    // of scheduling noise under parallel test load.
    next_change(&mut role_rx).await;
    // Gap B: the third campaign's own timeout (attempt 2: 600-1200ms).
    let gap_b = next_change(&mut role_rx).await;

    assert!(
        gap_b > gap_a,
        "expected a later, backed-off retry ({gap_b:?}) to take longer \
         than an earlier one ({gap_a:?})"
    );
    cleanup(&path);
}
