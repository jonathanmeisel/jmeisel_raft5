//! The client driven against real `raft5_server` nodes on real
//! localhost sockets -- the parts of it that only exist because a
//! cluster moves under a client: redirects, discovery, and a membership
//! that changes while the client is using it.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio_util::sync::CancellationToken;

use raft5_client::{Client, Error, ReadResult};

const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(10);

/// A node started for a test: fresh database directory, ephemeral
/// ports, and the real server loop running in the background. Stopped
/// and cleaned up on drop.
struct TestNode {
    id: String,
    client_addr: SocketAddr,
    peer_addr: SocketAddr,
    db_dir: PathBuf,
    cancel: CancellationToken,
}

impl TestNode {
    fn start(id: &str) -> Self {
        Self::start_with(id, None)
    }

    /// A node that requires a token on every RPC but `Login` and
    /// `GetConfig`.
    fn start_with_auth(id: &str, auth: raft5_server::auth::AuthConfig) -> Self {
        Self::start_with(id, Some(auth))
    }

    fn start_with(id: &str, auth: Option<raft5_server::auth::AuthConfig>) -> Self {
        let _ = env_logger::try_init();
        let node = Self {
            id: id.to_string(),
            client_addr: free_local_addr(),
            peer_addr: free_local_addr(),
            db_dir: temp_db_dir(id),
            cancel: CancellationToken::new(),
        };

        let (server_id, db_dir) = (node.id.clone(), node.db_dir.clone());
        let (client_addr, peer_addr) = (node.client_addr, node.peer_addr);
        let cancel = node.cancel.clone();
        tokio::spawn(async move {
            let server = raft5_server::serve_config(
                raft5_server::ServerConfig {
                    id: server_id,
                    db: db_dir,
                    client_addr,
                    peer_addr,
                    advertise: None,
                    advertise_client: None,
                    auth,
                },
                cancel,
            );
            if let Err(e) = server.await {
                eprintln!("node exited with an error: {e}");
            }
        });

        node
    }

    fn url(&self) -> String {
        format!("http://{}", self.client_addr)
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        self.cancel.cancel();
        let _ = std::fs::remove_dir_all(&self.db_dir);
    }
}

fn free_local_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().expect("local_addr")
}

fn temp_db_dir(label: &str) -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let counter = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut path = std::env::temp_dir();
    path.push(format!(
        "raft5_client_test_{}_{label}_{nanos:x}_{counter:x}",
        std::process::id()
    ));
    path
}

/// `Client::bootstrap` deliberately doesn't retry (it is irreversible),
/// but a node that has only just started may not be ready to accept one
/// yet -- so a test that bootstraps the instant it spawns a node has to
/// wait for that itself.
async fn bootstrap(client: &Client, server_id: &str) {
    let until = Instant::now() + BOOTSTRAP_TIMEOUT;
    loop {
        match client.bootstrap(server_id).await {
            Ok(()) => return,
            Err(e) => {
                assert!(Instant::now() < until, "bootstrap never succeeded: {e}");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
}

#[tokio::test]
async fn write_read_and_delete_round_trip() {
    let n1 = TestNode::start("n1");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "n1").await;

    let seq = client.write(b"k", Some(b"v")).await.unwrap();
    let read = client.read(b"k").await.unwrap();
    assert_eq!(read.value.as_deref(), Some(&b"v"[..]));
    assert_eq!(read.seq, seq);

    client.write(b"k", None).await.unwrap();
    assert_eq!(client.read(b"k").await.unwrap().value, None);
}

/// The redirect path: a client that has only ever been told about a
/// node that cannot serve a write still serves one, by following the
/// `NotLeader` hint (and the configuration it fetches to make sense of
/// it) to whoever can.
#[tokio::test]
async fn a_client_seeded_with_a_follower_finds_the_leader() {
    let n1 = TestNode::start("n1");
    let n2 = TestNode::start("n2");
    let n3 = TestNode::start("n3");

    let admin = Client::new([n1.url()]).unwrap();
    bootstrap(&admin, "n1").await;
    admin.add_server(&n2.url()).await.unwrap();
    admin.add_server(&n3.url()).await.unwrap();

    let client = Client::new([n3.url()]).unwrap();
    client.write(b"k", Some(b"v")).await.unwrap();
    assert_eq!(
        client.read(b"k").await.unwrap().value.as_deref(),
        Some(&b"v"[..])
    );
    assert_eq!(
        client.endpoints().len(),
        3,
        "the client should have learned the whole cluster on its way to the leader: {:?}",
        client.endpoints()
    );
}

/// A seed is a starting point, not a fixed list: once the cluster says
/// a server is no longer a member, the client stops sending it
/// anything.
#[tokio::test]
async fn a_seed_that_leaves_the_cluster_is_dropped() {
    let n1 = TestNode::start("n1");
    let n2 = TestNode::start("n2");
    let n3 = TestNode::start("n3");

    let client = Client::new([n1.url(), n2.url(), n3.url()]).unwrap();
    bootstrap(&client, "n1").await;
    client.add_server(&n2.url()).await.unwrap();
    client.add_server(&n3.url()).await.unwrap();
    assert_eq!(client.endpoints().len(), 3);

    client.remove_server("n3").await.unwrap();

    let endpoints = client.endpoints();
    assert_eq!(endpoints.len(), 2, "{endpoints:?}");
    assert!(
        !endpoints
            .iter()
            .any(|e| e.contains(&n3.client_addr.to_string())),
        "a removed server should not still be in the endpoint list: {endpoints:?}"
    );
}

/// The read-modify-write the store cannot do on its own. A transaction
/// reads the counter, writes one more than that, and commits; nobody
/// else can write the key between the read and the commit, so no
/// increment is ever computed from a value that has already been
/// replaced.
#[tokio::test]
async fn a_transaction_reads_modifies_and_writes() {
    let n1 = TestNode::start("n1");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "n1").await;

    for expected in ["1", "2", "3"] {
        increment(&client).await.unwrap();
        assert_eq!(
            fresh(&client, b"hits").await.value.as_deref(),
            Some(expected.as_bytes())
        );
    }
}

/// The point of all of it. Every one of these increments reads a value
/// that another task is racing to replace; a single lost update -- one
/// write landing on top of a version it never saw -- and the total comes
/// up short.
#[tokio::test]
async fn concurrent_increments_do_not_lose_writes() {
    const TASKS: u64 = 4;
    const PER_TASK: u64 = 5;

    let n1 = TestNode::start("n1");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "n1").await;

    let mut tasks = Vec::new();
    for _ in 0..TASKS {
        let client = client.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..PER_TASK {
                // Deciding what to do about a lost race is the caller's
                // job, and with four writers on one key it happens
                // often: here, simply go round again. Safe because an
                // aborted transaction wrote nothing.
                loop {
                    match increment(&client).await {
                        Ok(_) => break,
                        Err(Error::Aborted { .. }) => continue,
                        Err(e) => panic!("increment failed: {e}"),
                    }
                }
            }
        }));
    }
    for task in tasks {
        task.await.expect("an incrementing task panicked");
    }

    let total = fresh(&client, b"hits").await;
    assert_eq!(
        total.value.as_deref(),
        Some((TASKS * PER_TASK).to_string().as_bytes()),
        "every increment must be there, or a write landed on a version it never read"
    );
}

/// A transaction that buffers nothing writes nothing, and commits at
/// seq 0 -- there was no entry, so there is no sequence number to name.
#[tokio::test]
async fn a_transaction_that_buffers_nothing_writes_nothing() {
    let n1 = TestNode::start("n1");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "n1").await;
    client.write(b"k", Some(b"before")).await.unwrap();
    let before = fresh(&client, b"k").await;

    let txn = client.transaction(Duration::from_secs(2)).await.unwrap();
    txn.read(b"k").await.unwrap();
    assert_eq!(txn.commit().await.unwrap(), 0);

    assert_eq!(fresh(&client, b"k").await, before);
}

/// Writing no value deletes the key, so the same transaction that
/// updates a value can also decide it should no longer exist.
#[tokio::test]
async fn a_transactional_write_of_nothing_deletes_the_key() {
    let n1 = TestNode::start("n1");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "n1").await;
    client.write(b"k", Some(b"v")).await.unwrap();

    let txn = client.transaction(Duration::from_secs(2)).await.unwrap();
    txn.read(b"k").await.unwrap();
    txn.write(b"k", None).await.unwrap();
    txn.commit().await.unwrap();

    assert_eq!(fresh(&client, b"k").await.value, None);
}

/// Reads the counter `key` holds (absent counts as zero) and commits one
/// more than that, in a single transaction.
async fn increment(client: &Client) -> Result<u64, Error> {
    let txn = client.transaction(Duration::from_secs(2)).await?;
    let current = txn.read(b"hits").await?;
    let n: u64 = current
        .value
        .map(|bytes| String::from_utf8(bytes).unwrap().parse().unwrap())
        .unwrap_or(0);
    txn.write(b"hits", Some((n + 1).to_string().as_bytes())).await?;
    txn.commit().await
}

/// A read guaranteed to reflect every write that has already returned.
///
/// `Client::read` is deliberately a stale local read, so asserting a
/// value straight after writing it has to go through a transaction --
/// which is the only linearizable read the protocol has. Aborted rather
/// than committed: it buffered nothing, so there is nothing to write,
/// and aborting hands the lock back now instead of at the deadline.
async fn fresh(client: &Client, key: &[u8]) -> ReadResult {
    let txn = client
        .transaction(Duration::from_secs(2))
        .await
        .expect("opening a transaction for a current read");
    let result = txn.read(key).await.expect("a transactional read");
    txn.abort().await.expect("aborting a read-only transaction");
    result
}

// --- transactions -----------------------------------------------------

/// Two keys changing at one moment. Both land at the same sequence
/// number, which is what "together" means here -- one log entry, one
/// apply.
#[tokio::test]
async fn a_transaction_commits_every_key_at_one_seq() {
    let n1 = TestNode::start("txn1");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "txn1").await;

    client.write(b"alice", Some(b"10")).await.unwrap();
    client.write(b"bob", Some(b"10")).await.unwrap();

    let txn = client.transaction(Duration::from_secs(2)).await.unwrap();
    assert_eq!(txn.read(b"alice").await.unwrap().value.as_deref(), Some(&b"10"[..]));
    assert_eq!(txn.read(b"bob").await.unwrap().value.as_deref(), Some(&b"10"[..]));
    txn.write(b"alice", Some(b"9")).await.unwrap();
    txn.write(b"bob", Some(b"11")).await.unwrap();
    let seq = txn.commit().await.unwrap();

    let alice = client.read(b"alice").await.unwrap();
    let bob = client.read(b"bob").await.unwrap();
    assert_eq!(alice.value.as_deref(), Some(&b"9"[..]));
    assert_eq!(bob.value.as_deref(), Some(&b"11"[..]));
    assert_eq!(
        (alice.seq, bob.seq),
        (seq, seq),
        "both keys must land at the transaction's one sequence number"
    );
}

/// Nothing is written until commit. Everything else a transaction can do
/// -- buffer, read, be dropped, run out of time -- leaves the store
/// exactly as it was.
#[tokio::test]
async fn a_dropped_transaction_writes_nothing_and_frees_its_keys() {
    let n1 = TestNode::start("txn2");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "txn2").await;

    client.write(b"k", Some(b"original")).await.unwrap();

    let txn = client.transaction(Duration::from_secs(2)).await.unwrap();
    txn.write(b"k", Some(b"never")).await.unwrap();
    txn.abort().await.unwrap();

    assert_eq!(
        client.read(b"k").await.unwrap().value.as_deref(),
        Some(&b"original"[..])
    );
    // And the key is writable again, which is the half a stale lock
    // would break.
    client.write(b"k", Some(b"after")).await.unwrap();
}

/// A transaction sees its own buffered writes. `seq` still describes
/// committed state even then -- the buffered write hasn't got one yet.
#[tokio::test]
async fn a_transaction_reads_back_its_own_buffered_writes() {
    let n1 = TestNode::start("txn3");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "txn3").await;

    let committed = client.write(b"k", Some(b"committed")).await.unwrap();

    let txn = client.transaction(Duration::from_secs(2)).await.unwrap();
    txn.write(b"k", Some(b"buffered")).await.unwrap();
    let read = txn.read(b"k").await.unwrap();
    assert_eq!(read.value.as_deref(), Some(&b"buffered"[..]));
    assert_eq!(read.seq, committed, "seq describes committed state, not the buffer");

    // A buffered delete reads back as absent, the same way.
    txn.write(b"k", None).await.unwrap();
    assert_eq!(txn.read(b"k").await.unwrap().value, None);

    // ...and none of that is visible to anyone else until it commits.
    assert_eq!(
        client.read(b"k").await.unwrap().value.as_deref(),
        Some(&b"committed"[..])
    );
    txn.commit().await.unwrap();
    assert_eq!(client.read(b"k").await.unwrap().value, None);
}

/// A transaction's read lock is what makes its read set mean something:
/// nobody else can change a key it has read out from under it, not even
/// through an ordinary single-key write.
#[tokio::test]
async fn a_plain_write_cannot_change_a_key_a_transaction_has_read() {
    let n1 = TestNode::start("txn4");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "txn4").await;

    client.write(b"k", Some(b"v")).await.unwrap();

    let txn = client.transaction(Duration::from_secs(5)).await.unwrap();
    txn.read(b"k").await.unwrap();

    // `write` retries a contended key until the client's own deadline,
    // so bound it here: what is being asserted is that it does *not*
    // succeed while the transaction holds the key.
    let racing = tokio::time::timeout(
        Duration::from_millis(300),
        client.write(b"k", Some(b"stolen")),
    )
    .await;
    assert!(racing.is_err(), "a plain write must not slip past a read lock");

    txn.abort().await.unwrap();
    // Once the lock is gone the same write goes through.
    client.write(b"k", Some(b"allowed")).await.unwrap();
    assert_eq!(
        client.read(b"k").await.unwrap().value.as_deref(),
        Some(&b"allowed"[..])
    );
}

/// Wound-wait, end to end. The older transaction takes the key rather
/// than backing off, and the younger one is told why -- `Wounded`, not
/// some generic failure, and not silence.
#[tokio::test]
async fn an_older_transaction_wounds_a_younger_one_holding_its_key() {
    let n1 = TestNode::start("txn5");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "txn5").await;

    let older = client.transaction(Duration::from_secs(5)).await.unwrap();
    let younger = client.transaction(Duration::from_secs(5)).await.unwrap();

    younger.write(b"k", Some(b"theirs")).await.unwrap();
    older
        .write(b"k", Some(b"mine"))
        .await
        .expect("the older transaction takes the key");

    match younger.write(b"other", Some(b"v")).await {
        Err(raft5_client::Error::Aborted { reason, .. }) => {
            assert_eq!(reason, raft5_client::AbortReason::Wounded);
        }
        other => panic!("the wounded transaction should learn why, got {other:?}"),
    }

    older.commit().await.unwrap();
    assert_eq!(
        client.read(b"k").await.unwrap().value.as_deref(),
        Some(&b"mine"[..])
    );
}

/// A transaction that outlives its deadline holds nothing. The cluster
/// refuses the commit rather than applying writes whose read set it has
/// long since stopped protecting.
#[tokio::test]
async fn a_transaction_past_its_deadline_cannot_commit() {
    let n1 = TestNode::start("txn6");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "txn6").await;

    client.write(b"k", Some(b"original")).await.unwrap();

    let txn = client.transaction(Duration::from_millis(50)).await.unwrap();
    txn.write(b"k", Some(b"too late")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    match txn.commit().await {
        Err(raft5_client::Error::Aborted { reason, .. }) => assert!(
            matches!(
                reason,
                raft5_client::AbortReason::Expired | raft5_client::AbortReason::Unknown
            ),
            "expected an expiry, got {reason:?}"
        ),
        other => panic!("an expired transaction must not commit, got {other:?}"),
    }
    assert_eq!(
        client.read(b"k").await.unwrap().value.as_deref(),
        Some(&b"original"[..])
    );
}

/// A transaction that buffered nothing still commits -- there is just no
/// entry, so no sequence number to name. Worth pinning because the
/// read-only path skips the raft round trip entirely and has to check
/// the leader's lease itself instead.
#[tokio::test]
async fn a_read_only_transaction_commits_at_no_sequence_number() {
    let n1 = TestNode::start("txn7");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "txn7").await;

    client.write(b"k", Some(b"v")).await.unwrap();

    let txn = client.transaction(Duration::from_secs(2)).await.unwrap();
    assert_eq!(txn.read(b"k").await.unwrap().value.as_deref(), Some(&b"v"[..]));
    assert_eq!(txn.commit().await.unwrap(), 0);

    // Its read lock is gone with it.
    client.write(b"k", Some(b"after")).await.unwrap();
}

/// The concurrency property the whole feature exists for: many
/// transactions doing read-modify-write over the *same pair* of keys,
/// with their sum checked at the end. Two independent single-key writes
/// could not make this safe: there would be no moment at which both had
/// happened and nothing else had come between.
#[tokio::test]
async fn concurrent_transactions_keep_two_keys_consistent() {
    let n1 = TestNode::start("txn8");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "txn8").await;

    client.write(b"a", Some(b"50")).await.unwrap();
    client.write(b"b", Some(b"50")).await.unwrap();

    const TRANSFERS: usize = 12;
    let mut handles = Vec::new();
    for _ in 0..TRANSFERS {
        let client = client.clone();
        handles.push(tokio::spawn(async move {
            // Retried by the caller, not the library: a transaction that
            // aborts wrote nothing, but replaying it means replaying the
            // reads too, and only this code knows what they were.
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                match transfer_one(&client).await {
                    Ok(()) => return,
                    Err(e) => assert!(
                        Instant::now() < deadline,
                        "a transfer never got through: {e}"
                    ),
                }
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let a: i64 = number(&client, b"a").await;
    let b: i64 = number(&client, b"b").await;
    assert_eq!(a + b, 100, "every transfer must conserve the total (a={a}, b={b})");
    assert_eq!(a, 50 - TRANSFERS as i64);
}

/// Move one unit from `a` to `b`, reading both and writing both inside
/// one transaction.
async fn transfer_one(client: &Client) -> Result<(), raft5_client::Error> {
    let txn = client.transaction(Duration::from_secs(5)).await?;
    let a: i64 = parse(txn.read(b"a").await?.value.as_deref());
    let b: i64 = parse(txn.read(b"b").await?.value.as_deref());
    txn.write(b"a", Some((a - 1).to_string().as_bytes()))
        .await?;
    txn.write(b"b", Some((b + 1).to_string().as_bytes()))
        .await?;
    txn.commit().await.map(|_| ())
}

async fn number(client: &Client, key: &[u8]) -> i64 {
    parse(client.read(key).await.unwrap().value.as_deref())
}

fn parse(bytes: Option<&[u8]>) -> i64 {
    bytes
        .and_then(|b| std::str::from_utf8(b).ok()?.parse().ok())
        .unwrap_or(0)
}

/// A refusal time cannot fix is reported as one, promptly. Retrying it
/// would spend the client's whole deadline arriving at the same answer
/// and then report it as though the cluster had merely been busy, which
/// hides the actual reason exactly when the caller most needs to read it.
#[tokio::test]
async fn a_request_the_cluster_will_never_serve_fails_fast_and_says_why() {
    let n1 = TestNode::start("txn9");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "txn9").await;

    let started = Instant::now();
    let refused = client
        .transaction(Duration::from_secs(60 * 60))
        .await
        .expect_err("an hour-long transaction should be refused");

    match &refused {
        raft5_client::Error::Rejected(why) => {
            assert!(why.contains("lifetime"), "the reason should survive: {why}");
        }
        other => panic!("expected an outright refusal, got {other:?}"),
    }
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "a permanent refusal must not be retried to the deadline (took {:?})",
        started.elapsed()
    );
}

/// Dropping the handle releases the transaction's locks, without an
/// explicit `abort`. Best-effort -- `Drop` cannot await -- so this waits
/// for the key to come free rather than assuming it already has.
#[tokio::test]
async fn dropping_the_handle_releases_the_transactions_locks() {
    let n1 = TestNode::start("txn10");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "txn10").await;

    client.write(b"k", Some(b"original")).await.unwrap();

    {
        // A deadline far longer than this test will wait, so that what
        // frees the key can only be the drop.
        let txn = client.transaction(Duration::from_secs(5)).await.unwrap();
        txn.read(b"k").await.unwrap();
    }

    let until = Instant::now() + Duration::from_secs(4);
    loop {
        match tokio::time::timeout(
            Duration::from_millis(200),
            client.write(b"k", Some(b"after")),
        )
        .await
        {
            Ok(Ok(_)) => break,
            _ => assert!(
                Instant::now() < until,
                "the dropped transaction never released its lock"
            ),
        }
    }
    assert_eq!(
        client.read(b"k").await.unwrap().value.as_deref(),
        Some(&b"after"[..])
    );
}

// --- key iteration ----------------------------------------------------

/// The iterator's whole job: hand back every key in order, fetching
/// batches as it goes, so the caller never sees the paging.
#[tokio::test]
async fn a_key_iterator_walks_every_key_in_order() {
    let n1 = TestNode::start("keys1");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "keys1").await;

    for i in 0..25 {
        client
            .write(format!("k{i:02}").as_bytes(), Some(b"v"))
            .await
            .unwrap();
    }

    // A batch size that divides the keyspace several times over, so the
    // walk genuinely spans batches rather than fitting in one.
    let keys = client
        .key_iterator(b"")
        .batch_size(4)
        .collect()
        .await
        .unwrap();

    let expected: Vec<Vec<u8>> = (0..25).map(|i| format!("k{i:02}").into_bytes()).collect();
    assert_eq!(keys, expected, "paging must cover the keyspace exactly once");
}

/// Starting mid-keyspace starts there, inclusively, and an empty stretch
/// simply ends immediately.
#[tokio::test]
async fn a_key_iterator_starts_where_it_is_told_and_stops_at_the_end() {
    let n1 = TestNode::start("keys2");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "keys2").await;

    for key in [&b"apple"[..], b"banana", b"cherry"] {
        client.write(key, Some(b"v")).await.unwrap();
    }

    let keys = client.key_iterator(b"banana").collect().await.unwrap();
    assert_eq!(keys, vec![b"banana".to_vec(), b"cherry".to_vec()]);

    let past_the_end = client.key_iterator(b"z").collect().await.unwrap();
    assert!(past_the_end.is_empty());
}

/// A deleted key is not a key. The iterator walks what has a value, so
/// a delete removes it from the walk like any other read.
#[tokio::test]
async fn a_key_iterator_skips_deleted_keys() {
    let n1 = TestNode::start("keys3");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "keys3").await;

    for key in [&b"a"[..], b"b", b"c"] {
        client.write(key, Some(b"v")).await.unwrap();
    }
    client.write(b"b", None).await.unwrap();

    let keys = client.key_iterator(b"").collect().await.unwrap();
    assert_eq!(keys, vec![b"a".to_vec(), b"c".to_vec()]);
}

/// Inside a transaction the walk sees the transaction's own buffered
/// changes, and every key it hands over is locked -- so a
/// read-modify-write over "whatever is in this range" is possible at
/// all.
#[tokio::test]
async fn a_transactional_key_iterator_sees_its_own_writes_and_holds_what_it_returns() {
    let n1 = TestNode::start("keys4");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "keys4").await;

    client.write(b"a", Some(b"1")).await.unwrap();
    client.write(b"c", Some(b"3")).await.unwrap();

    let txn = client.transaction(Duration::from_secs(5)).await.unwrap();
    txn.write(b"b", Some(b"2")).await.unwrap();
    txn.write(b"c", None).await.unwrap();

    let keys = txn.key_iterator(b"").batch_size(2).collect().await.unwrap();
    assert_eq!(
        keys,
        vec![b"a".to_vec(), b"b".to_vec()],
        "its own buffered write is there and its own buffered delete is gone"
    );

    // `a` came back from the walk, so the transaction holds it: a
    // younger writer is refused rather than slipping in underneath.
    let contender = client.transaction(Duration::from_secs(5)).await.unwrap();
    match contender.write(b"a", Some(b"stolen")).await {
        Err(Error::Aborted { reason, key }) => {
            assert_eq!(reason, raft5_client::AbortReason::LockConflict);
            assert_eq!(key.as_deref(), Some(&b"a"[..]));
        }
        other => panic!("expected the scan's lock to hold the key, got {other:?}"),
    }

    txn.abort().await.unwrap();
}

/// A walk that stops early stops fetching. Only the batches actually
/// needed are paid for -- which, inside a transaction, also means only
/// those keys are locked.
#[tokio::test]
async fn a_key_iterator_stopped_early_does_not_fetch_the_rest() {
    let n1 = TestNode::start("keys5");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "keys5").await;

    for i in 0..20 {
        client
            .write(format!("k{i:02}").as_bytes(), Some(b"v"))
            .await
            .unwrap();
    }

    let txn = client.transaction(Duration::from_secs(5)).await.unwrap();
    let mut keys = txn.key_iterator(b"").batch_size(2);
    assert_eq!(keys.next().await.unwrap(), Some(b"k00".to_vec()));
    assert_eq!(keys.next().await.unwrap(), Some(b"k01".to_vec()));
    drop(keys);

    // Nothing past the first batch was ever asked for, so nothing past
    // it is held: another transaction can still take those keys.
    let other = client.transaction(Duration::from_secs(5)).await.unwrap();
    other.write(b"k19", Some(b"free")).await.unwrap();
    other.commit().await.unwrap();

    txn.abort().await.unwrap();
}

/// The batch boundary, at the one shape of key that could break it.
/// Continuing a walk appends a zero byte to the last key seen, so a key
/// that *is* the previous one plus a zero byte -- or any other prefix
/// relationship -- has to land in the next batch rather than be skipped
/// or repeated.
#[tokio::test]
async fn a_key_iterator_pages_correctly_across_keys_that_prefix_each_other() {
    let n1 = TestNode::start("keys6");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "keys6").await;

    // In ascending byte order: "k" < "k\0" < "k\0\0" < "ka".
    let expected: Vec<Vec<u8>> = vec![
        b"k".to_vec(),
        b"k\x00".to_vec(),
        b"k\x00\x00".to_vec(),
        b"ka".to_vec(),
    ];
    for key in &expected {
        client.write(key, Some(b"v")).await.unwrap();
    }

    // One key per batch, so every one of those boundaries is crossed by
    // the zero-byte continuation rather than skipped over inside a
    // batch.
    let keys = client
        .key_iterator(b"")
        .batch_size(1)
        .collect()
        .await
        .unwrap();
    assert_eq!(keys, expected);
}

// --- authentication ---------------------------------------------------

const TEST_SECRET: &[u8] = b"thirty-two bytes at the very least, and this is";

async fn auth_config(admin: Option<(&str, &str)>) -> raft5_server::auth::AuthConfig {
    let initial_admin = match admin {
        Some((username, password)) => Some(raft5_server::auth::InitialAdmin {
            username: username.to_string(),
            password_hash: raft5_server::auth::hash_password(password.to_string())
                .await
                .expect("hashing should work"),
        }),
        None => None,
    };
    raft5_server::auth::AuthConfig::new(TEST_SECRET, Duration::from_secs(3600), initial_admin)
}

/// An authenticated cluster, from the client's side: log in before there
/// is anything to log in to, bootstrap with that token, then use the
/// account bootstrapping left behind.
#[tokio::test]
async fn a_client_can_log_in_bootstrap_and_then_work() {
    let n1 = TestNode::start_with_auth("auth-c1", auth_config(Some(("root", "hunter42"))).await);
    let client = Client::new([n1.url()]).unwrap();

    assert!(!client.is_logged_in());
    client.login("root", "hunter42").await.unwrap();
    assert!(client.is_logged_in());

    bootstrap(&client, "auth-c1").await;

    // The token from before the cluster existed is still good, and the
    // account bootstrapping wrote is what backs it from here on.
    client.write(b"k", Some(b"v")).await.unwrap();
    assert_eq!(client.read(b"k").await.unwrap().value, Some(b"v".to_vec()));

    client
        .create_user("alice", "correct horse", false)
        .await
        .unwrap();

    let alice = Client::new([n1.url()]).unwrap();
    alice.login("alice", "correct horse").await.unwrap();
    assert_eq!(alice.read(b"k").await.unwrap().value, Some(b"v".to_vec()));
}

/// The regression that matters most for how this *feels*: an
/// unauthenticated call has to fail at once. Retried to the ten-second
/// deadline it would be reported as "the cluster could not serve the
/// request", which is both wrong and the single most misleading thing
/// this client could say.
#[tokio::test]
async fn a_call_without_a_token_fails_immediately_rather_than_retrying() {
    let n1 = TestNode::start_with_auth("auth-c2", auth_config(Some(("root", "hunter42"))).await);
    let client = Client::new([n1.url()]).unwrap();
    // Bootstrap through a client that has logged in, so the cluster is
    // usable and the only thing wrong below is the missing token.
    let admin = Client::new([n1.url()]).unwrap();
    admin.login("root", "hunter42").await.unwrap();
    bootstrap(&admin, "auth-c2").await;

    let started = Instant::now();
    let error = client
        .read(b"k")
        .await
        .expect_err("a read with no token should be refused");
    assert!(
        matches!(error, Error::Unauthenticated(_)),
        "expected Unauthenticated, got {error:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "a refusal that cannot change should not be retried to the deadline; took {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn logging_in_with_the_wrong_password_is_refused_and_says_so() {
    let n1 = TestNode::start_with_auth("auth-c3", auth_config(Some(("root", "hunter42"))).await);
    let client = Client::new([n1.url()]).unwrap();

    let error = client
        .login("root", "not the password")
        .await
        .expect_err("the wrong password should be refused");
    assert!(
        matches!(
            error,
            Error::Denied {
                reason: raft5_client::Denial::BadCredentials,
                ..
            }
        ),
        "expected BadCredentials, got {error:?}"
    );
    assert!(!client.is_logged_in());
}

/// Authenticating says who you are; the store says what you may do.
#[tokio::test]
async fn an_ordinary_account_is_refused_account_management() {
    let n1 = TestNode::start_with_auth("auth-c4", auth_config(Some(("root", "hunter42"))).await);
    let admin = Client::new([n1.url()]).unwrap();
    admin.login("root", "hunter42").await.unwrap();
    bootstrap(&admin, "auth-c4").await;
    admin
        .create_user("alice", "correct horse", false)
        .await
        .unwrap();

    let alice = Client::new([n1.url()]).unwrap();
    alice.login("alice", "correct horse").await.unwrap();

    let error = alice
        .create_user("mallory", "letmein", true)
        .await
        .expect_err("an ordinary account may not create accounts");
    assert!(
        matches!(
            error,
            Error::Denied {
                reason: raft5_client::Denial::NotAdmin,
                ..
            }
        ),
        "expected NotAdmin, got {error:?}"
    );
}

/// A cluster with no secret keeps no accounts, and says so rather than
/// letting a caller believe they have logged in to something.
#[tokio::test]
async fn logging_in_to_a_cluster_without_a_secret_says_it_is_disabled() {
    let n1 = TestNode::start("auth-c5");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "auth-c5").await;

    let error = client
        .login("root", "hunter42")
        .await
        .expect_err("there is nothing to log in to");
    assert!(
        matches!(
            error,
            Error::Denied {
                reason: raft5_client::Denial::AuthDisabled,
                ..
            }
        ),
        "expected AuthDisabled, got {error:?}"
    );

    // And the cluster is perfectly usable without logging in.
    client.write(b"k", Some(b"v")).await.unwrap();
}

/// Logging out is not just local bookkeeping: the connections this
/// client already holds stop sending the token too.
#[tokio::test]
async fn logging_out_stops_existing_connections_using_the_token() {
    let n1 = TestNode::start_with_auth("auth-c6", auth_config(Some(("root", "hunter42"))).await);
    let client = Client::new([n1.url()]).unwrap();
    client.login("root", "hunter42").await.unwrap();
    bootstrap(&client, "auth-c6").await;
    client.write(b"k", Some(b"v")).await.unwrap();

    client.logout();
    assert!(!client.is_logged_in());

    let error = client
        .read(b"k")
        .await
        .expect_err("after logging out there is no token to send");
    assert!(
        matches!(error, Error::Unauthenticated(_)),
        "expected Unauthenticated, got {error:?}"
    );
}

/// Membership changes are administrators-only, and the refusal reaches
/// the client as the same `Denial::NotAdmin` a rejected `create_user`
/// gives -- one error type for "you may not do this", whichever call it
/// was.
#[tokio::test]
async fn an_ordinary_account_is_refused_membership_changes() {
    let n1 = TestNode::start_with_auth("auth-c7", auth_config(Some(("root", "hunter42"))).await);
    let admin = Client::new([n1.url()]).unwrap();
    admin.login("root", "hunter42").await.unwrap();
    bootstrap(&admin, "auth-c7").await;
    admin
        .create_user("alice", "correct horse", false)
        .await
        .unwrap();

    let alice = Client::new([n1.url()]).unwrap();
    alice.login("alice", "correct horse").await.unwrap();

    let error = alice
        .remove_server("auth-c7")
        .await
        .expect_err("an ordinary account may not change membership");
    assert!(
        matches!(
            error,
            Error::Denied {
                reason: raft5_client::Denial::NotAdmin,
                ..
            }
        ),
        "expected NotAdmin, got {error:?}"
    );
}
