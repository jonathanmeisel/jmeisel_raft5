//! End-to-end integration test: three real nodes (each a full
//! `jmeisel_raft5::run` instance, listening on real localhost sockets),
//! formed into a cluster via the client gRPC API alone -- bootstrap plus
//! two `AddServer` calls, no internal shortcuts. A write through the
//! leader is only reported back once committed and applied (see
//! `raft::LeaderHandle::append_log`), so awaiting the `Write` RPC *is*
//! waiting for commitment; the read that follows confirms the value is
//! actually there, both on the leader and, once replication catches up,
//! on the other two nodes.

mod common;

use common::{
    TestNode, add_server, assert_eventually_reads, bootstrap, connect, read, read_from_leader,
    wait_until_voter, write, write_to_leader,
};

/// Three fresh nodes (ids `{prefix}1`/`{prefix}2`/`{prefix}3`), formed
/// into a cluster purely through the client gRPC API: `Bootstrap` on the
/// first, then `AddServer` for the other two. Each joins as a
/// non-voting learner and is promoted automatically once it catches up
/// (Raft §4.2.1) -- waited for here, so callers get back a cluster of
/// three genuine voters, not one voter plus two learners still catching
/// up (which, notably, could never survive that one voter's death: a
/// learner never campaigns, so the other two would be stuck with no one
/// left able to hold an election).
async fn three_node_cluster(prefix: &str) -> (TestNode, TestNode, TestNode) {
    let n1 = TestNode::new(&format!("{prefix}1"));
    let n2 = TestNode::new(&format!("{prefix}2"));
    let n3 = TestNode::new(&format!("{prefix}3"));

    let mut c1 = connect(n1.client_addr).await;
    bootstrap(&mut c1, &n1.id).await;
    add_server(&mut c1, &n2.id, n2.peer_addr).await;
    add_server(&mut c1, &n3.id, n3.peer_addr).await;
    wait_until_voter(&n1, &n2.id).await;
    wait_until_voter(&n1, &n3.id).await;

    (n1, n2, n3)
}

#[tokio::test]
async fn write_through_a_three_node_cluster_commits_and_reads_back() {
    let (n1, n2, n3) = three_node_cluster("n").await;
    let mut c1 = connect(n1.client_addr).await;

    // `write` only returns once the entry has committed *and* been
    // applied (see `raft::LeaderHandle::append_log`) -- there is nothing
    // further to wait on here.
    let seq = write(&mut c1, b"hello", Some(b"world")).await;

    let value = read(&mut c1, b"hello").await;
    assert_eq!(value, Some(b"world".to_vec()));

    // The value is committed cluster-wide, not just locally on the
    // leader: it eventually reaches both other nodes too, once each has
    // replicated up to `seq`.
    let mut c2 = connect(n2.client_addr).await;
    let mut c3 = connect(n3.client_addr).await;
    assert_eventually_reads(&mut c2, b"hello", b"world", seq).await;
    assert_eventually_reads(&mut c3, b"hello", b"world", seq).await;
}

/// Covers a leadership change: killing the leader outright, confirming
/// the survivors elect a new one that still has everything committed
/// before the old leader disappeared, that the new leader keeps serving
/// writes, and that the old leader -- once restarted -- catches back up
/// on everything it missed while it was down, including an entry
/// (`"second"`) it never saw appended at all.
#[tokio::test]
async fn a_restarted_node_catches_up_after_a_leadership_change() {
    let (mut n1, n2, n3) = three_node_cluster("r").await;

    // n1 formed the cluster and has had no reason to step down, so it's
    // still leading -- the write/read round trip works there directly.
    let mut c1 = connect(n1.client_addr).await;
    let seq1 = write(&mut c1, b"first", Some(b"one")).await;
    assert_eq!(read(&mut c1, b"first").await, Some(b"one".to_vec()));

    // Kill the leader. n2 and n3 have to elect one of themselves before
    // either can serve another request.
    n1.shut_down().await;

    // A *linearizable* read succeeding at all is proof this landed on a
    // genuine new leader (one holding a live read quorum), not merely
    // the first surviving node to answer -- see `grpc::KvService::read`.
    // The value itself might not show up on this very first read: a
    // freshly elected leader's `latest` only catches up to its own
    // commit index asynchronously (`raft::spawn_storage_maintenance_task`),
    // so this confirms leadership here and waits for the value
    // separately, rather than asserting on this exact response.
    let (leader_addr, _) = read_from_leader(&[&n2, &n3], b"first", true).await;
    let mut leader_client = connect(leader_addr).await;
    assert_eventually_reads(&mut leader_client, b"first", b"one", seq1).await;

    // The new leader takes a fresh write just as well, and reads it back
    // immediately.
    let (leader_addr, seq2) = write_to_leader(&[&n2, &n3], b"second", Some(b"two")).await;
    let mut leader_client = connect(leader_addr).await;
    assert_eq!(
        read(&mut leader_client, b"second").await,
        Some(b"two".to_vec())
    );

    // Bring the old leader back. It rejoins with a stale log: it missed
    // the entire leadership change, plus "second" outright. Restarting
    // straight onto the same database directory and the same addresses
    // is only safe because `shut_down` waited for the previous
    // incarnation to release both.
    n1.start_up();

    // It eventually catches up on both, via ordinary replication from
    // whichever node is leading now.
    let mut c1 = connect(n1.client_addr).await;
    assert_eventually_reads(&mut c1, b"first", b"one", seq1).await;
    assert_eventually_reads(&mut c1, b"second", b"two", seq2).await;
}
