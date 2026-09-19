//! End-to-end integration test: three real nodes (each a full
//! `raft5_server::run` instance, listening on real localhost sockets),
//! formed into a cluster via the client gRPC API alone -- bootstrap plus
//! two `AddServer` calls, no internal shortcuts. A write through the
//! leader is only reported back once committed and applied (see
//! `raft::LeaderHandle::append_log`), so awaiting the `Write` RPC *is*
//! waiting for commitment; the read that follows confirms the value is
//! actually there, both on the leader and, once replication catches up,
//! on the other two nodes.

mod common;

use common::{
    TestNode, add_server, assert_eventually_reads, begin, bootstrap, buffer_write, commit,
    find_leader, read, wait_until_voter, write,
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
    let n1 = TestNode::new(&format!("{prefix}1")).await;
    let n2 = TestNode::new(&format!("{prefix}2")).await;
    let n3 = TestNode::new(&format!("{prefix}3")).await;

    let c1 = n1.client();
    bootstrap(&c1, &n1.id).await;
    add_server(&c1, &n2).await;
    add_server(&c1, &n3).await;
    wait_until_voter(&c1, &n2.id).await;
    wait_until_voter(&c1, &n3.id).await;

    (n1, n2, n3)
}

#[tokio::test]
async fn write_through_a_three_node_cluster_commits_and_reads_back() {
    let (n1, n2, n3) = three_node_cluster("n").await;
    let c1 = n1.client();

    // `write` only returns once the entry has committed *and* been
    // applied (see `raft::LeaderHandle::append_log`) -- there is nothing
    // further to wait on here.
    let seq = write(&c1, b"hello", Some(b"world")).await;

    let value = read(&c1, b"hello").await;
    assert_eq!(value, Some(b"world".to_vec()));

    // The value is committed cluster-wide, not just locally on the
    // leader: it eventually reaches both other nodes too, once each has
    // replicated up to `seq`.
    assert_eventually_reads(&n2.client(), b"hello", b"world", seq).await;
    assert_eventually_reads(&n3.client(), b"hello", b"world", seq).await;
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
    let c1 = n1.client();
    let seq1 = write(&c1, b"first", Some(b"one")).await;
    assert_eq!(read(&c1, b"first").await, Some(b"one".to_vec()));

    // Kill the leader. n2 and n3 have to elect one of themselves before
    // either can serve another request.
    n1.shut_down().await;

    // `find_leader` succeeding at all here is proof this landed on a
    // genuine new leader (one holding a live read quorum), not merely
    // the first surviving node to answer -- see `grpc::KvService::read`.
    // The value itself might not show up on the leader's very first
    // linearizable read: a freshly elected leader's `latest` only
    // catches up to its own commit index asynchronously
    // (`raft::spawn_storage_maintenance_task`), so this confirms
    // leadership here and waits for the value separately.
    let leader = find_leader(&[&n2, &n3]).await;
    let leader_client = leader.client();
    assert_eventually_reads(&leader_client, b"first", b"one", seq1).await;

    // The new leader takes a fresh write just as well, and reads it back
    // immediately.
    let seq2 = write(&leader_client, b"second", Some(b"two")).await;
    assert_eq!(read(&leader_client, b"second").await, Some(b"two".to_vec()));

    // Bring the old leader back. It rejoins with a stale log: it missed
    // the entire leadership change, plus "second" outright. Restarting
    // straight onto the same database directory and the same addresses
    // is only safe because `shut_down` waited for the previous
    // incarnation to release both.
    n1.start_up().await;

    // It eventually catches up on both, via ordinary replication from
    // whichever node is leading now.
    let c1 = n1.client();
    assert_eventually_reads(&c1, b"first", b"one", seq1).await;
    assert_eventually_reads(&c1, b"second", b"two", seq2).await;
}

/// A transaction across a real three-node cluster: every key it wrote
/// replicates to every node, and arrives at the same sequence number on
/// each -- one entry, so there is only one for them to arrive at.
#[tokio::test]
async fn a_transaction_replicates_all_of_its_keys_at_one_seq() {
    let (n1, n2, n3) = three_node_cluster("t").await;
    let c1 = n1.client();

    write(&c1, b"alice", Some(b"10")).await;
    write(&c1, b"bob", Some(b"10")).await;

    let txn = begin(&c1, std::time::Duration::from_secs(5)).await;
    buffer_write(&c1, &txn, b"alice", Some(b"9")).await;
    buffer_write(&c1, &txn, b"bob", Some(b"11")).await;
    let seq = commit(&c1, &txn).await.expect("the transaction should commit");

    // Committed *and* applied by the time commit returns, so the leader
    // already reads back both keys.
    assert_eq!(read(&c1, b"alice").await, Some(b"9".to_vec()));
    assert_eq!(read(&c1, b"bob").await, Some(b"11".to_vec()));

    // And the whole entry reaches the others, not half of it.
    for node in [&n2, &n3] {
        assert_eventually_reads(&node.client(), b"alice", b"9", seq).await;
        assert_eventually_reads(&node.client(), b"bob", b"11", seq).await;
    }
}

/// A transaction belongs to the leader that opened it. Kill that leader
/// mid-transaction and the commit fails rather than half-applying -- and
/// because nothing was appended, the keys are exactly as they were.
#[tokio::test]
async fn a_transaction_does_not_survive_the_death_of_its_leader() {
    let (mut n1, n2, n3) = three_node_cluster("d").await;
    let leader_is_n1 = find_leader(&[&n1, &n2, &n3]).await.id == n1.id;
    assert!(leader_is_n1, "the bootstrapped node should still be leading");

    let c1 = n1.client();
    write(&c1, b"k", Some(b"original")).await;

    let txn = begin(&c1, std::time::Duration::from_secs(5)).await;
    buffer_write(&c1, &txn, b"k", Some(b"never")).await;

    n1.shut_down().await;

    // Whoever leads now has never heard of this transaction, and the key
    // is untouched: nothing was appended, because nothing is appended
    // until commit.
    let survivor = find_leader(&[&n2, &n3]).await;
    assert_eq!(read(&survivor.client(), b"k").await, Some(b"original".to_vec()));
    // ...and it is writable again, so no lock outlived the leader.
    write(&survivor.client(), b"k", Some(b"after")).await;
}

/// The ceiling this whole change exists to raise. A batch past tonic's
/// 4 MB default decode limit used to be refused in the transport --
/// silently, and forever, because a leader retries an entry it has
/// already appended and nothing after it can commit until it lands.
///
/// Several large values rather than one, so chunk boundaries fall inside
/// a value as well as between them.
#[tokio::test]
async fn a_transaction_larger_than_one_grpc_message_replicates_to_every_node() {
    let (n1, n2, n3) = three_node_cluster("big").await;
    let c1 = n1.client();

    // Enough to be decisively past the 4 MB message ceiling and to span
    // several 1 MiB wire chunks, and no more: the payload is written
    // three times over (leader plus two followers), and a bigger one
    // starves the rest of the suite rather than proving anything more.
    const VALUES: usize = 5;
    const VALUE_BYTES: usize = 1024 * 1024;
    // Committed in the same transaction as the big values, so it lands
    // in the same entry at the same seq. Waiting on *it* is what lets
    // this test avoid re-reading megabytes every 20ms while a follower
    // catches up -- and it only works because of the property under
    // test: one transaction is one entry, so a node that has the
    // sentinel has all of it.
    const SENTINEL: &[u8] = b"big-sentinel";

    let txn = begin(&c1, std::time::Duration::from_secs(15)).await;
    for i in 0..VALUES {
        // Distinct bytes per key, so a value arriving at the wrong key
        // (or a chunk landing in the wrong place) is caught rather than
        // hidden by every value being identical.
        let value = vec![u8::try_from(i).unwrap(); VALUE_BYTES];
        buffer_write(&c1, &txn, format!("big{i}").as_bytes(), Some(&value)).await;
    }
    buffer_write(&c1, &txn, SENTINEL, Some(b"landed")).await;
    let seq = commit(&c1, &txn)
        .await
        .expect("a transaction past the old message ceiling should now commit");

    for node in [&n1, &n2, &n3] {
        let client = node.client();
        assert_eventually_reads(&client, SENTINEL, b"landed", seq).await;
        // The sentinel is there, so by construction the rest of the
        // entry is too -- read each value exactly once and check it.
        for i in 0..VALUES {
            let expected = vec![u8::try_from(i).unwrap(); VALUE_BYTES];
            let got = read(&client, format!("big{i}").as_bytes()).await;
            assert!(
                got.as_deref() == Some(&expected[..]),
                "node {} has {} bytes for key big{i}, expected {VALUE_BYTES}",
                node.id,
                got.map_or(0, |v| v.len())
            );
        }
    }
}
