//! End-to-end integration test: three real nodes (each a full
//! `raft5_server` instance on real localhost sockets), formed into a
//! cluster through the client gRPC API alone: bootstrap plus two
//! `AddServer` calls, no internal shortcuts. A write is a transaction, and
//! its commit is reported only after the entry is committed and applied
//! (see `raft::LeaderHandle::append_writes`), so awaiting
//! `CommitTransaction` waits for commit. The read that follows confirms the
//! value is there, on the leader and, once replication catches up, on the
//! other two nodes.

mod common;

use common::{
    TestNode, add_server, assert_eventually_reads, begin, bootstrap, txn_write, commit,
    find_leader, read, wait_until_voter, write,
};

/// Three new nodes (ids `{prefix}1`, `{prefix}2`, `{prefix}3`), formed into
/// a cluster only through the client gRPC API: `Bootstrap` on the first,
/// then `AddServer` for the other two. Each joins as a non-voting learner
/// and is promoted automatically once it catches up (Raft §4.2.1). This
/// waits for that, so callers get three real voters, not one voter plus two
/// learners still catching up (which could not survive that voter's death:
/// a learner never campaigns, so the other two would have nobody able to
/// hold an election).
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

    // `write` returns only after the entry is committed and applied (see
    // `raft::LeaderHandle::append_writes`), so there is nothing more to wait
    // for.
    let seq = write(&c1, b"hello", Some(b"world")).await;

    let value = read(&c1, b"hello").await;
    assert_eq!(value, Some(b"world".to_vec()));

    // The value is committed across the cluster, not only on the leader: it
    // reaches both other nodes once each has replicated up to `seq`.
    assert_eventually_reads(&n2.client(), b"hello", b"world", seq).await;
    assert_eventually_reads(&n3.client(), b"hello", b"world", seq).await;
}

/// A leadership change: kill the leader, and confirm that the survivors
/// elect a new one that still has everything committed before the old
/// leader went away, that the new leader keeps serving writes, and that the
/// old leader, once restarted, catches up on everything it missed while
/// down, including an entry (`"second"`) it never saw appended.
#[tokio::test]
async fn a_restarted_node_catches_up_after_a_leadership_change() {
    let (mut n1, n2, n3) = three_node_cluster("r").await;

    // n1 formed the cluster and has had no reason to step down, so it still
    // leads: the write and read work there directly.
    let c1 = n1.client();
    let seq1 = write(&c1, b"first", Some(b"one")).await;
    assert_eq!(read(&c1, b"first").await, Some(b"one".to_vec()));

    // Kill the leader. n2 and n3 must elect one of themselves before either
    // can serve another request.
    n1.shut_down().await;

    // `find_leader` succeeding proves this is a real new leader (one with a
    // live read quorum), not just the first surviving node to answer; see
    // `grpc::KvService::get_config`. The value may not show up on the leader's
    // very first read: a new leader applies up to its commit index
    // asynchronously (`raft::spawn_storage_maintenance_task`), so this confirms
    // leadership here and waits for the value separately.
    let leader = find_leader(&[&n2, &n3]).await;
    let leader_client = leader.client();
    assert_eventually_reads(&leader_client, b"first", b"one", seq1).await;

    // The new leader takes a new write just as well, and reads it back at once.
    let seq2 = write(&leader_client, b"second", Some(b"two")).await;
    assert_eq!(read(&leader_client, b"second").await, Some(b"two".to_vec()));

    // Bring the old leader back. It rejoins with a stale log: it missed the
    // whole leadership change, and "second" entirely. Restarting on the same
    // database directory and addresses is safe only because `shut_down` waited
    // for the previous instance to release both.
    n1.start_up().await;

    // It catches up on both, through normal replication from whichever node
    // leads now.
    let c1 = n1.client();
    assert_eventually_reads(&c1, b"first", b"one", seq1).await;
    assert_eventually_reads(&c1, b"second", b"two", seq2).await;
}

/// A transaction across a real three-node cluster: every key it wrote
/// replicates to every node, and arrives at the same seq on each. One
/// entry, so there is only one seq to arrive at.
#[tokio::test]
async fn a_transaction_replicates_all_of_its_keys_at_one_seq() {
    let (n1, n2, n3) = three_node_cluster("t").await;
    let c1 = n1.client();

    write(&c1, b"alice", Some(b"10")).await;
    write(&c1, b"bob", Some(b"10")).await;

    let txn = begin(&c1, std::time::Duration::from_secs(5)).await;
    txn_write(&c1, &txn, b"alice", Some(b"9")).await;
    txn_write(&c1, &txn, b"bob", Some(b"11")).await;
    let seq = commit(&c1, &txn).await.expect("the transaction should commit");

    // Committed and applied by the time commit returns, so the leader already
    // reads back both keys.
    assert_eq!(read(&c1, b"alice").await, Some(b"9".to_vec()));
    assert_eq!(read(&c1, b"bob").await, Some(b"11".to_vec()));

    // And the whole entry reaches the others, not half of it.
    for node in [&n2, &n3] {
        assert_eventually_reads(&node.client(), b"alice", b"9", seq).await;
        assert_eventually_reads(&node.client(), b"bob", b"11", seq).await;
    }
}

/// A transaction belongs to the leader that opened it. Kill that leader
/// mid-transaction, and the commit fails instead of half applying, and
/// because nothing was appended, the keys are as they were.
#[tokio::test]
async fn a_transaction_does_not_survive_the_death_of_its_leader() {
    let (mut n1, n2, n3) = three_node_cluster("d").await;
    let leader_is_n1 = find_leader(&[&n1, &n2, &n3]).await.id == n1.id;
    assert!(leader_is_n1, "the bootstrapped node should still be leading");

    let c1 = n1.client();
    write(&c1, b"k", Some(b"original")).await;

    let txn = begin(&c1, std::time::Duration::from_secs(5)).await;
    txn_write(&c1, &txn, b"k", Some(b"never")).await;

    n1.shut_down().await;

    // The current leader has never heard of this transaction, and the key is
    // untouched: nothing was appended, because nothing is appended until
    // commit.
    let survivor = find_leader(&[&n2, &n3]).await;
    assert_eq!(read(&survivor.client(), b"k").await, Some(b"original".to_vec()));
    // ...and it is writable again, so no lock outlived the leader.
    write(&survivor.client(), b"k", Some(b"after")).await;
}

/// A batch past tonic's 4 MB default decode limit must still replicate.
/// If the transport refused it, the refusal would be silent and permanent:
/// a leader keeps resending an entry it has appended, and nothing after it
/// can commit until it lands.
///
/// Several large values, not one, so chunk boundaries fall inside a value
/// as well as between values.
#[tokio::test]
async fn a_transaction_larger_than_one_grpc_message_replicates_to_every_node() {
    let (n1, n2, n3) = three_node_cluster("big").await;
    let c1 = n1.client();

    // Enough to be well past the 4 MB message limit and to span several 1 MiB
    // wire chunks, and no more: the payload is written three times (leader plus
    // two followers), and a bigger one slows the rest of the suite without
    // proving more.
    const VALUES: usize = 5;
    const VALUE_BYTES: usize = 1024 * 1024;
    // Committed in the same transaction as the big values, so it lands in the
    // same entry at the same seq. Waiting on it lets this test avoid reading
    // megabytes every 20ms while a follower catches up, and that works only
    // because of the property under test: one transaction is one entry, so a
    // node that has the sentinel has all of it.
    const SENTINEL: &[u8] = b"big-sentinel";

    let txn = begin(&c1, std::time::Duration::from_secs(15)).await;
    for i in 0..VALUES {
        // Different bytes per key, so a value at the wrong key (or a chunk in the
        // wrong place) is caught, not hidden by identical values.
        let value = vec![u8::try_from(i).unwrap(); VALUE_BYTES];
        txn_write(&c1, &txn, format!("big{i}").as_bytes(), Some(&value)).await;
    }
    txn_write(&c1, &txn, SENTINEL, Some(b"landed")).await;
    let seq = commit(&c1, &txn)
        .await
        .expect("a transaction past the old message ceiling should now commit");

    for node in [&n1, &n2, &n3] {
        let client = node.client();
        assert_eventually_reads(&client, SENTINEL, b"landed", seq).await;
        // The sentinel is there, so the rest of the entry is too: read each value
        // once and check it.
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

/// A stale read on a follower says how far that follower had applied. That
/// is how a caller gets read-your-writes there without paying for a
/// linearizable read: until `as_of` reaches the seq its own commit
/// returned, the answer may not include that commit, and once it has, it
/// must. Both keys of the one entry move together either way.
#[tokio::test]
async fn a_followers_stale_read_says_whether_it_has_caught_up() {
    use raft5_server::pb::raft5::client::{ReadRequest, read_response};

    let (n1, n2, _n3) = three_node_cluster("f").await;
    let c1 = n1.client();
    let id = begin(&c1, std::time::Duration::from_secs(5)).await;
    txn_write(&c1, &id, b"a", Some(b"1")).await;
    txn_write(&c1, &id, b"b", Some(b"1")).await;
    let seq = commit(&c1, &id).await.expect("the commit should land");

    let mut follower = (*n2.client()).clone();
    let until = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let resp = follower
            .read(ReadRequest {
                keys: vec![b"a".to_vec(), b"b".to_vec()],
                versions_only: false,
                consistency: None,
            })
            .await
            .expect("read RPC failed")
            .into_inner();
        let Some(read_response::Result::Ok(ok)) = resp.result else {
            panic!("a stale read is always served: {resp:?}");
        };
        let [a, b] = ok.entries.as_slice() else {
            panic!("two keys, two entries: {ok:?}");
        };
        assert_eq!(a, b, "one entry wrote both");
        if ok.as_of >= seq {
            assert_eq!(a.seq, seq);
            assert_eq!(a.value.as_deref(), Some(&b"1"[..]));
            return;
        }
        assert!(a.seq < seq, "as_of {} is behind a write it shows: {a:?}", ok.as_of);
        assert!(std::time::Instant::now() < until, "the follower never caught up");
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}
