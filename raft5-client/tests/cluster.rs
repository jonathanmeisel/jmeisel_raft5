//! The client driven against real `raft5_server` nodes on real
//! localhost sockets -- the parts of it that only exist because a
//! cluster moves under a client: redirects, discovery, and a membership
//! that changes while the client is using it.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio_util::sync::CancellationToken;

use raft5_client::{Client, Update};

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
            let server = raft5_server::serve(
                server_id,
                &db_dir,
                client_addr,
                peer_addr,
                None,
                None,
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

/// The read-modify-write the store cannot do on its own: the closure
/// sees what the key holds and what it returns is written back, so a
/// counter can be incremented without ever reading a value someone else
/// has already replaced.
#[tokio::test]
async fn update_reads_modifies_and_writes() {
    let n1 = TestNode::start("n1");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "n1").await;

    for expected in ["1", "2", "3"] {
        client.update(b"hits", increment).await.unwrap();
        assert_eq!(
            client.read(b"hits").await.unwrap().value.as_deref(),
            Some(expected.as_bytes())
        );
    }
}

/// The point of all of it. Every one of these increments reads a value
/// that another task is racing to replace; a single lost update -- one
/// write landing on top of a version it never saw -- and the total
/// comes up short.
#[tokio::test]
async fn concurrent_updates_do_not_lose_writes() {
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
                // often: here, simply go round again.
                loop {
                    match client.update(b"hits", increment).await {
                        Ok(_) => break,
                        Err(raft5_client::Error::Conflict { .. }) => continue,
                        Err(e) => panic!("increment failed: {e}"),
                    }
                }
            }
        }));
    }
    for task in tasks {
        task.await.expect("an updating task panicked");
    }

    let total = client.read(b"hits").await.unwrap();
    assert_eq!(
        total.value.as_deref(),
        Some((TASKS * PER_TASK).to_string().as_bytes()),
        "every increment must be there, or a write landed on a version it never read"
    );
}

/// A closure that declines to change anything writes nothing, and the
/// seq it gets back is the one the key was already at.
#[tokio::test]
async fn a_noop_update_writes_nothing() {
    let n1 = TestNode::start("n1");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "n1").await;
    client.write(b"k", Some(b"before")).await.unwrap();
    let before = client.read(b"k").await.unwrap();

    let seq = client.update(b"k", |_| Update::Noop).await.unwrap();
    assert_eq!(seq, before.seq);
    assert_eq!(client.read(b"k").await.unwrap(), before);
}

/// Writing no value deletes the key, so the same call that updates a
/// value can also decide it should no longer exist.
#[tokio::test]
async fn an_update_returning_nothing_deletes_the_key() {
    let n1 = TestNode::start("n1");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "n1").await;
    client.write(b"k", Some(b"v")).await.unwrap();

    client.update(b"k", |_| Update::Write(None)).await.unwrap();
    assert_eq!(client.read(b"k").await.unwrap().value, None);
}

/// A key that is rewritten under the client on every single attempt is
/// not a race it can win by trying harder, so it stops and says so
/// rather than looping forever.
// Multi-threaded so the closure can block on a write of its own: the
// update API is deliberately synchronous, and this is the one test that
// needs to do something asynchronous from inside it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_update_that_never_wins_gives_up() {
    let n1 = TestNode::start("n1");
    let client = Client::new([n1.url()]).unwrap();
    let interloper = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "n1").await;
    client.write(b"k", Some(b"0")).await.unwrap();

    let attempts = std::sync::atomic::AtomicU64::new(0);
    let err = client
        .update(b"k", |current| {
            // Runs *after* the read this attempt will be validated
            // against, so every attempt is doomed before it is sent.
            let n = attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current()
                    .block_on(interloper.write(b"k", Some(n.to_string().as_bytes())))
            })
            .unwrap();
            Update::Write(current.map(<[u8]>::to_vec))
        })
        .await
        .unwrap_err();

    assert!(
        matches!(err, raft5_client::Error::Conflict { .. }),
        "expected to give up with a conflict, got {err}"
    );
    assert_eq!(
        attempts.load(std::sync::atomic::Ordering::SeqCst),
        raft5_client::MAX_UPDATE_ATTEMPTS as u64,
        "it should have used every attempt before giving up"
    );
}

/// Parses the counter `key` currently holds (absent counts as zero) and
/// hands back one more than that.
fn increment(current: Option<&[u8]>) -> Update {
    let n: u64 = current
        .map(|bytes| std::str::from_utf8(bytes).unwrap().parse().unwrap())
        .unwrap_or(0);
    Update::Write(Some((n + 1).to_string().into_bytes()))
}

// --- transactions -----------------------------------------------------

/// The thing `update` cannot do: two keys changing at one moment. Both
/// land at the same sequence number, which is what "together" means here
/// -- one log entry, one apply.
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
    txn.buffer_write(b"alice", Some(b"9")).await.unwrap();
    txn.buffer_write(b"bob", Some(b"11")).await.unwrap();
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
    txn.buffer_write(b"k", Some(b"never")).await.unwrap();
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
    txn.buffer_write(b"k", Some(b"buffered")).await.unwrap();
    let read = txn.read(b"k").await.unwrap();
    assert_eq!(read.value.as_deref(), Some(&b"buffered"[..]));
    assert_eq!(read.seq, committed, "seq describes committed state, not the buffer");

    // A buffered delete reads back as absent, the same way.
    txn.buffer_write(b"k", None).await.unwrap();
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

    younger.buffer_write(b"k", Some(b"theirs")).await.unwrap();
    older
        .buffer_write(b"k", Some(b"mine"))
        .await
        .expect("the older transaction takes the key");

    match younger.buffer_write(b"other", Some(b"v")).await {
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
    txn.buffer_write(b"k", Some(b"too late")).await.unwrap();
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
/// with their sum checked at the end. Interleaving reads and writes on
/// two keys is exactly what `update` cannot make safe.
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
    txn.buffer_write(b"a", Some((a - 1).to_string().as_bytes()))
        .await?;
    txn.buffer_write(b"b", Some((b + 1).to_string().as_bytes()))
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
