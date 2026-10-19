//! The client driven against real `raft5_server` nodes on real
//! localhost sockets -- the parts of it that only exist because a
//! cluster moves under a client: redirects, discovery, and a membership
//! that changes while the client is using it.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio_util::sync::CancellationToken;

use raft5_client::{Client, Consistency, Entry, Error, Transaction};

const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(10);

/// The single-key reads most of these tests make, on the many-key API.
/// A one-key read is never cut short, so there is always one entry.
trait ReadOne {
    async fn read_one(&self, key: &[u8]) -> Result<Entry, Error>;
    async fn version_one(&self, key: &[u8]) -> Result<u64, Error>;
}

impl ReadOne for Client {
    async fn read_one(&self, key: &[u8]) -> Result<Entry, Error> {
        self.read_one_at(key, Consistency::Stale).await
    }

    async fn version_one(&self, key: &[u8]) -> Result<u64, Error> {
        self.version_one_at(key, Consistency::Stale).await
    }
}

impl ReadOne for Transaction {
    async fn read_one(&self, key: &[u8]) -> Result<Entry, Error> {
        Ok(self.read(&[key]).await?.entries.remove(0))
    }

    async fn version_one(&self, key: &[u8]) -> Result<u64, Error> {
        Ok(self.read_versions(&[key]).await?.entries[0].seq)
    }
}

/// `ReadOne` at a consistency other than stale.
trait ReadOneAt {
    async fn read_one_at(&self, key: &[u8], consistency: Consistency<'_>) -> Result<Entry, Error>;
    async fn version_one_at(&self, key: &[u8], consistency: Consistency<'_>) -> Result<u64, Error>;
}

impl ReadOneAt for Client {
    async fn read_one_at(&self, key: &[u8], consistency: Consistency<'_>) -> Result<Entry, Error> {
        Ok(self.read(&[key], consistency).await?.entries.remove(0))
    }

    async fn version_one_at(&self, key: &[u8], consistency: Consistency<'_>) -> Result<u64, Error> {
        Ok(self.read_versions(&[key], consistency).await?.entries[0].seq)
    }
}

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
        Self::start_full(id, auth, None, None)
    }

    /// A node serving both its APIs over TLS.
    fn start_with_tls(
        id: &str,
        peer_tls: raft5_server::PeerTls,
        client_tls: raft5_server::ClientApiTls,
    ) -> Self {
        Self::start_full(id, None, Some(peer_tls), Some(client_tls))
    }

    fn start_full(
        id: &str,
        auth: Option<raft5_server::auth::AuthConfig>,
        peer_tls: Option<raft5_server::PeerTls>,
        client_tls: Option<raft5_server::ClientApiTls>,
    ) -> Self {
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
                    peer_tls,
                    client_tls,
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

    /// What `add_server` is given: the peer API, not the client one.
    fn peer_url(&self) -> String {
        format!("http://{}", self.peer_addr)
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
    bootstrap_with_heartbeat(client, server_id, None).await;
}

async fn bootstrap_with_heartbeat(client: &Client, server_id: &str, heartbeat_ms: Option<u64>) {
    let until = Instant::now() + BOOTSTRAP_TIMEOUT;
    loop {
        match client.bootstrap(server_id, heartbeat_ms).await {
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
    let read = client.read_one(b"k").await.unwrap();
    assert_eq!(read.value.as_deref(), Some(&b"v"[..]));
    assert_eq!(read.seq, seq);

    client.write(b"k", None).await.unwrap();
    assert_eq!(client.read_one(b"k").await.unwrap().value, None);
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
    admin.add_server(&n2.peer_url()).await.unwrap();
    admin.add_server(&n3.peer_url()).await.unwrap();

    let client = Client::new([n3.url()]).unwrap();
    client.write(b"k", Some(b"v")).await.unwrap();
    assert_eq!(
        client.read_one(b"k").await.unwrap().value.as_deref(),
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
    client.add_server(&n2.peer_url()).await.unwrap();
    client.add_server(&n3.peer_url()).await.unwrap();
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

/// The heartbeat is chosen at bootstrap, replicated to every member
/// that joins, and can be changed on a running cluster without it
/// losing its leader or its ability to commit.
#[tokio::test]
async fn the_heartbeat_is_set_at_bootstrap_and_changeable_while_running() {
    let n1 = TestNode::start("n1");
    let n2 = TestNode::start("n2");
    let n3 = TestNode::start("n3");

    let client = Client::new([n1.url(), n2.url(), n3.url()]).unwrap();
    bootstrap_with_heartbeat(&client, "n1", Some(100)).await;
    client.add_server(&n2.peer_url()).await.unwrap();
    client.add_server(&n3.peer_url()).await.unwrap();
    assert_eq!(client.heartbeat_ms().await.unwrap(), 100);

    client.set_heartbeat(250).await.unwrap();
    assert_eq!(client.heartbeat_ms().await.unwrap(), 250);
    // Every member, not just whichever one the client asked: n3 joined
    // under the old value and learned the new one by replication.
    for node in [&n1, &n2, &n3] {
        let direct = Client::new([node.url()]).unwrap();
        assert_eq!(direct.heartbeat_ms().await.unwrap(), 250, "{}", node.url());
    }

    client.write(b"k", Some(b"v")).await.unwrap();
    assert_eq!(fresh(&client, b"k").await.value.as_deref(), Some(&b"v"[..]));

    // Refused up front, not retried until the deadline.
    let err = client.set_heartbeat(5).await.unwrap_err();
    assert!(
        matches!(&err, Error::Rejected(why) if why.contains("heartbeat")),
        "{err:?}"
    );
    assert_eq!(client.heartbeat_ms().await.unwrap(), 250);
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
    txn.read_one(b"k").await.unwrap();
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
    txn.read_one(b"k").await.unwrap();
    txn.write(b"k", None).await.unwrap();
    txn.commit().await.unwrap();

    assert_eq!(fresh(&client, b"k").await.value, None);
}

/// Reads the counter `key` holds (absent counts as zero) and commits one
/// more than that, in a single transaction.
async fn increment(client: &Client) -> Result<u64, Error> {
    let txn = client.transaction(Duration::from_secs(2)).await?;
    let current = txn.read_one(b"hits").await?;
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
/// value straight after writing it has to ask for more than that.
/// `read_linearizable` is the cheapest way to: no transaction, no lock
/// to hand back, just the leader proving it still leads before it
/// answers. Nothing here needs the key held afterwards -- these are
/// assertions, not read-modify-writes.
async fn fresh(client: &Client, key: &[u8]) -> Entry {
    client
        .read_one_at(key, Consistency::Linearizable)
        .await
        .expect("a linearizable read")
}

/// `fresh`, for the version alone.
async fn fresh_version(client: &Client, key: &[u8]) -> u64 {
    client
        .version_one_at(key, Consistency::Linearizable)
        .await
        .expect("a linearizable versions-only read")
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
    assert_eq!(txn.read_one(b"alice").await.unwrap().value.as_deref(), Some(&b"10"[..]));
    assert_eq!(txn.read_one(b"bob").await.unwrap().value.as_deref(), Some(&b"10"[..]));
    txn.write(b"alice", Some(b"9")).await.unwrap();
    txn.write(b"bob", Some(b"11")).await.unwrap();
    let seq = txn.commit().await.unwrap();

    let alice = client.read_one(b"alice").await.unwrap();
    let bob = client.read_one(b"bob").await.unwrap();
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
        client.read_one(b"k").await.unwrap().value.as_deref(),
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
    let read = txn.read_one(b"k").await.unwrap();
    assert_eq!(read.value.as_deref(), Some(&b"buffered"[..]));
    assert_eq!(read.seq, committed, "seq describes committed state, not the buffer");

    // A buffered delete reads back as absent, the same way.
    txn.write(b"k", None).await.unwrap();
    assert_eq!(txn.read_one(b"k").await.unwrap().value, None);

    // ...and none of that is visible to anyone else until it commits.
    assert_eq!(
        client.read_one(b"k").await.unwrap().value.as_deref(),
        Some(&b"committed"[..])
    );
    txn.commit().await.unwrap();
    assert_eq!(client.read_one(b"k").await.unwrap().value, None);
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
    txn.read_one(b"k").await.unwrap();

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
        client.read_one(b"k").await.unwrap().value.as_deref(),
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
        client.read_one(b"k").await.unwrap().value.as_deref(),
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
        client.read_one(b"k").await.unwrap().value.as_deref(),
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
    assert_eq!(txn.read_one(b"k").await.unwrap().value.as_deref(), Some(&b"v"[..]));
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
    let a: i64 = parse(txn.read_one(b"a").await?.value.as_deref());
    let b: i64 = parse(txn.read_one(b"b").await?.value.as_deref());
    txn.write(b"a", Some((a - 1).to_string().as_bytes()))
        .await?;
    txn.write(b"b", Some((b + 1).to_string().as_bytes()))
        .await?;
    txn.commit().await.map(|_| ())
}

async fn number(client: &Client, key: &[u8]) -> i64 {
    parse(client.read_one(key).await.unwrap().value.as_deref())
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
        txn.read_one(b"k").await.unwrap();
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
        client.read_one(b"k").await.unwrap().value.as_deref(),
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
    assert_eq!(client.read_one(b"k").await.unwrap().value, Some(b"v".to_vec()));

    client
        .create_user("alice", "correct horse", false)
        .await
        .unwrap();

    let alice = Client::new([n1.url()]).unwrap();
    alice.login("alice", "correct horse").await.unwrap();
    assert_eq!(alice.read_one(b"k").await.unwrap().value, Some(b"v".to_vec()));
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
        .read_one(b"k")
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
        .read_one(b"k")
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

// --- versions ---------------------------------------------------------

#[tokio::test]
async fn version_read_tracks_writes_and_deletes() {
    let n1 = TestNode::start("n1");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "n1").await;

    // Nothing there yet: 0, the "no current value" sentinel.
    assert_eq!(fresh_version(&client, b"k").await, 0);

    let first = client.write(b"k", Some(b"v")).await.unwrap();
    assert_eq!(fresh_version(&client, b"k").await, first);

    // A second write moves it, and the version is what changed even
    // though the caller never sees the value.
    let second = client.write(b"k", Some(b"v2")).await.unwrap();
    assert!(second > first);
    assert_eq!(fresh_version(&client, b"k").await, second);

    // A delete takes the version away with the value.
    client.write(b"k", None).await.unwrap();
    assert_eq!(fresh_version(&client, b"k").await, 0);
}

/// The version agrees with what a read of the same key reports, which is
/// what makes it a substitute for one. Asserted on a large value,
/// because that is the case the RPC exists for -- and the one where the
/// two answers come from physically different places.
#[tokio::test]
async fn version_read_agrees_with_read_on_a_large_value() {
    let n1 = TestNode::start("n1");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "n1").await;

    let big = vec![b'x'; 2 * 1024 * 1024];
    let seq = client.write(b"big", Some(&big)).await.unwrap();

    let read = fresh(&client, b"big").await;
    assert_eq!(read.seq, seq);
    assert_eq!(read.value.map(|v| v.len()), Some(big.len()));
    assert_eq!(fresh_version(&client, b"big").await, seq);
}

/// A linearizable read reflects a write that has already returned. The
/// stale one is allowed to lag and so is not asserted about -- this pins
/// the guarantee, not the absence of one.
#[tokio::test]
async fn a_linearizable_read_sees_a_write_that_has_returned() {
    let n1 = TestNode::start("n1");
    let n2 = TestNode::start("n2");
    let n3 = TestNode::start("n3");

    let admin = Client::new([n1.url()]).unwrap();
    bootstrap(&admin, "n1").await;
    admin.add_server(&n2.peer_url()).await.unwrap();
    admin.add_server(&n3.peer_url()).await.unwrap();

    let client = Client::new([n3.url()]).unwrap();
    let seq = client.write(b"k", Some(b"v")).await.unwrap();

    let read = client.read_one_at(b"k", Consistency::Linearizable).await.unwrap();
    assert_eq!(read.seq, seq);
    assert_eq!(read.value.as_deref(), Some(&b"v"[..]));
    assert_eq!(client.version_one_at(b"k", Consistency::Linearizable).await.unwrap(), seq);
}

/// The shape the RPC exists for: check the version, decide, write, and
/// have the commit fail if anyone touched the key in between. The read
/// lock a transactional versions-only read takes is what makes that safe, so
/// a later transaction must be refused the key rather than silently
/// winning it.
///
/// The conflict is observed through a second *transaction*, not through
/// `Client::write`: that retries an abort until its deadline, so against
/// a held key it would simply wait the holder out and then succeed,
/// which is the behaviour it is supposed to have and says nothing about
/// whether the lock was ever taken.
#[tokio::test]
async fn a_transactional_version_read_locks_out_a_later_write() {
    let n1 = TestNode::start("n1");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "n1").await;
    let written = client.write(b"k", Some(b"v")).await.unwrap();

    // The checker opens first, so it is the *older* transaction: a
    // younger one that wants its key dies rather than wounding it.
    let checker = client.transaction(Duration::from_secs(5)).await.unwrap();
    assert_eq!(checker.version_one(b"k").await.unwrap(), written);

    let later = client.transaction(Duration::from_secs(5)).await.unwrap();
    match later.write(b"k", Some(b"stolen")).await {
        Err(Error::Aborted { .. }) => {}
        other => panic!("a write against a held key should be refused, got {other:?}"),
    }

    // And the transaction that was holding it commits on the state it
    // checked.
    checker.write(b"k", Some(b"mine")).await.unwrap();
    checker.commit().await.unwrap();
    assert_eq!(
        fresh(&client, b"k").await.value.as_deref(),
        Some(&b"mine"[..])
    );
}

/// A transaction asking about a key it has itself buffered gets the
/// committed version, not a guess at the one its own write will land at
/// -- that number does not exist until the commit assigns it.
#[tokio::test]
async fn a_transactional_version_read_reports_committed_state() {
    let n1 = TestNode::start("n1");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "n1").await;
    let committed = client.write(b"k", Some(b"v")).await.unwrap();

    let txn = client.transaction(Duration::from_secs(2)).await.unwrap();
    txn.write(b"k", Some(b"buffered")).await.unwrap();
    assert_eq!(txn.version_one(b"k").await.unwrap(), committed);

    let landed = txn.commit().await.unwrap();
    assert!(landed > committed);
    assert_eq!(fresh_version(&client, b"k").await, landed);
}

// --- snapshots --------------------------------------------------------

#[tokio::test]
async fn a_snapshot_holds_the_store_as_it_was_taken() {
    let n1 = TestNode::start("n1");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "n1").await;

    client.write(b"kept", Some(b"v1")).await.unwrap();
    client.write(b"doomed", Some(b"here")).await.unwrap();
    let snap = client.create_snapshot(Some(b"s1")).await.unwrap();

    // Churn the store thoroughly after the snapshot.
    for i in 0..5 {
        client
            .write(b"kept", Some(format!("v{i}").as_bytes()))
            .await
            .unwrap();
    }
    client.write(b"doomed", None).await.unwrap();
    client.write(b"fresh", Some(b"new")).await.unwrap();

    // The snapshot is unmoved by any of it.
    assert_eq!(
        client
            .read_one_at(b"kept", Consistency::Snapshot(b"s1"))
            .await
            .unwrap()
            .value
            .as_deref(),
        Some(&b"v1"[..])
    );
    assert_eq!(
        client
            .read_one_at(b"doomed", Consistency::Snapshot(b"s1"))
            .await
            .unwrap()
            .value
            .as_deref(),
        Some(&b"here"[..]),
        "a key deleted after the snapshot is still visible through it"
    );
    assert_eq!(client.read_one_at(b"fresh", Consistency::Snapshot(b"s1")).await.unwrap().value, None);

    // The key set as of the snapshot, too.
    let keys = client.key_iterator_at(b"s1", b"").collect().await.unwrap();
    assert_eq!(keys, vec![b"doomed".to_vec(), b"kept".to_vec()]);

    // And it is listed, at the seq it was taken at.
    let listed = client.list_snapshots().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, b"s1".to_vec());
    assert_eq!(listed[0].seq, snap.seq);

    // Once released, reads at it fail rather than silently drifting.
    client.remove_snapshot(b"s1").await.unwrap();
    assert!(client.read_one_at(b"kept", Consistency::Snapshot(b"s1")).await.is_err());
    assert!(client.list_snapshots().await.unwrap().is_empty());
}

/// A snapshot is replicated state, so every node serves it -- and serves
/// the same answer. This is what would break if the registry lived
/// anywhere but the replicated keyspace.
#[tokio::test]
async fn every_node_serves_the_same_snapshot_read() {
    let n1 = TestNode::start("n1");
    let n2 = TestNode::start("n2");
    let n3 = TestNode::start("n3");

    let admin = Client::new([n1.url()]).unwrap();
    bootstrap(&admin, "n1").await;
    admin.add_server(&n2.peer_url()).await.unwrap();
    admin.add_server(&n3.peer_url()).await.unwrap();

    admin.write(b"k", Some(b"at-snapshot")).await.unwrap();
    let snap = admin.create_snapshot(Some(b"s1")).await.unwrap();
    admin.write(b"k", Some(b"after")).await.unwrap();

    // Each client is seeded with exactly one node, so each read is
    // answered by a different one -- and a snapshot read takes no
    // leader, so whichever node it lands on must answer it.
    for node in [&n1, &n2, &n3] {
        let client = Client::new([node.url()]).unwrap();
        // The snapshot has to have reached this node first; it arrives
        // as an ordinary replicated write, so this is the same wait any
        // read-after-write needs.
        let mut seen = None;
        let deadline = Instant::now() + BOOTSTRAP_TIMEOUT;
        while Instant::now() < deadline {
            if let Ok(r) = client.read_one_at(b"k", Consistency::Snapshot(b"s1")).await {
                seen = Some(r);
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let read = seen.expect("the snapshot should have replicated");
        assert_eq!(
            read.value.as_deref(),
            Some(&b"at-snapshot"[..]),
            "node {} disagreed about the snapshot",
            node.id
        );
        assert_eq!(client.list_snapshots().await.unwrap()[0].seq, snap.seq);
    }
}

/// Two snapshots at different points, each holding its own view, and
/// neither disturbed by the other being released.
#[tokio::test]
async fn snapshots_at_different_points_hold_different_views() {
    let n1 = TestNode::start("n1");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "n1").await;

    client.write(b"k", Some(b"one")).await.unwrap();
    client.create_snapshot(Some(b"first")).await.unwrap();
    client.write(b"k", Some(b"two")).await.unwrap();
    client.create_snapshot(Some(b"second")).await.unwrap();
    client.write(b"k", Some(b"three")).await.unwrap();

    let at = |id: &'static [u8]| {
        let client = client.clone();
        async move {
            client
                .read_one_at(b"k", Consistency::Snapshot(id))
                .await
                .unwrap()
                .value
                .map(|v| String::from_utf8(v).unwrap())
        }
    };
    assert_eq!(at(b"first").await.as_deref(), Some("one"));
    assert_eq!(at(b"second").await.as_deref(), Some("two"));
    assert_eq!(
        fresh(&client, b"k").await.value.as_deref(),
        Some(&b"three"[..])
    );

    // Releasing one leaves the other exactly where it was.
    client.remove_snapshot(b"first").await.unwrap();
    assert_eq!(at(b"second").await.as_deref(), Some("two"));
}

/// The id defaults to the client's own clock, and two in the same
/// millisecond disambiguate rather than colliding.
#[tokio::test]
async fn a_snapshot_id_defaults_to_a_timestamp() {
    let n1 = TestNode::start("n1");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "n1").await;
    client.write(b"k", Some(b"v")).await.unwrap();

    let a = client.create_snapshot(None).await.unwrap();
    let b = client.create_snapshot(None).await.unwrap();
    assert_ne!(a.id, b.id, "two defaulted ids collided");

    // Both are usable.
    for snap in [&a, &b] {
        assert_eq!(
            client
                .read_one_at(b"k", Consistency::Snapshot(&snap.id))
                .await
                .unwrap()
                .value
                .as_deref(),
            Some(&b"v"[..])
        );
    }
}

/// An id nobody created is reported as such straight away, rather than
/// retried until the deadline and surfaced as the cluster being busy.
#[tokio::test]
async fn a_read_at_a_missing_snapshot_fails_fast() {
    let n1 = TestNode::start("n1");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "n1").await;

    let started = std::time::Instant::now();
    let read = client.read_one_at(b"k", Consistency::Snapshot(b"no-such-snapshot")).await;
    assert!(matches!(read, Err(Error::NotFound(_))), "{read:?}");
    let version = client.version_one_at(b"k", Consistency::Snapshot(b"no-such-snapshot")).await;
    assert!(matches!(version, Err(Error::NotFound(_))), "{version:?}");
    let removed = client.remove_snapshot(b"no-such-snapshot").await;
    assert!(matches!(removed, Err(Error::NotFound(_))), "{removed:?}");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(2),
        "took {:?}",
        started.elapsed()
    );
}

/// A snapshot walk pages like any other, across a batch boundary, with
/// keys deleted after the snapshot still in the walk.
#[tokio::test]
async fn a_snapshot_walk_pages_across_batches() {
    let n1 = TestNode::start("n1");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "n1").await;

    for i in 0..25 {
        client
            .write(format!("k{i:02}").as_bytes(), Some(b"v"))
            .await
            .unwrap();
    }
    client.create_snapshot(Some(b"s1")).await.unwrap();
    // Delete every third key *after* the snapshot; the walk should
    // still see all 25.
    for i in (0..25).step_by(3) {
        client
            .write(format!("k{i:02}").as_bytes(), None)
            .await
            .unwrap();
    }

    let keys = client
        .key_iterator_at(b"s1", b"")
        .batch_size(4)
        .collect()
        .await
        .unwrap();
    assert_eq!(keys.len(), 25, "the snapshot walk lost keys across batches");
    let expected: Vec<Vec<u8>> = (0..25).map(|i| format!("k{i:02}").into_bytes()).collect();
    assert_eq!(keys, expected);

    // While the current walk sees only what survives.
    let now = client.key_iterator(b"").collect().await.unwrap();
    assert_eq!(now.len(), 25 - 9);
}

// --- permissions ------------------------------------------------------

/// The client's side of an ACL: written by one account, enforced against
/// another, and reported by `get_acl` in the terms the refusal used.
#[tokio::test]
async fn an_acl_written_by_an_admin_is_enforced_against_another_account() {
    use raft5_client::{Denial, Principal, Right};

    let n1 = TestNode::start_with_auth("perm-c1", auth_config(Some(("root", "hunter42"))).await);
    let admin = Client::new([n1.url()]).unwrap();
    admin.login("root", "hunter42").await.unwrap();
    bootstrap(&admin, "perm-c1").await;
    admin.write(b"app/config", Some(b"v")).await.unwrap();
    admin.write(b"other/config", Some(b"v")).await.unwrap();
    admin
        .create_user("alice", "correct horse", false)
        .await
        .unwrap();

    let alice = Client::new([n1.url()]).unwrap();
    alice.login("alice", "correct horse").await.unwrap();
    // Before the ACL, alice reaches everything -- which is the state
    // every cluster upgrades into.
    assert_eq!(
        alice.read_one(b"app/config").await.unwrap().value,
        Some(b"v".to_vec())
    );

    admin
        .set_acl(b"app/", &[(Principal::user("root"), Right::Write)])
        .await
        .unwrap();

    let error = alice
        .read_one(b"app/config")
        .await
        .expect_err("the ACL does not name alice");
    assert!(
        matches!(
            error,
            Error::Denied {
                reason: Denial::NotPermitted,
                ..
            }
        ),
        "expected NotPermitted, got {error:?}"
    );
    let error = alice
        .write(b"app/config", Some(b"x"))
        .await
        .expect_err("nor for writing");
    assert!(matches!(error, Error::Denied { reason: Denial::NotPermitted, .. }));

    // A key the ACL does not govern is untouched.
    assert_eq!(
        alice.read_one(b"other/config").await.unwrap().value,
        Some(b"v".to_vec())
    );

    // And she can find out why, which is what makes a refusal
    // actionable rather than a mystery.
    let governing = alice.get_acl(b"app/config").await.unwrap();
    let acl = governing.acl.expect("something governs that key");
    assert_eq!(acl.prefix, b"app/".to_vec());
    assert_eq!(acl.entries, vec![(Principal::user("root"), Right::Write)]);
    assert_eq!(governing.effective, None, "the ACL grants alice nothing");

    assert_eq!(alice.get_acl(b"other/config").await.unwrap().acl, None);
}

/// A group is a name for a set of accounts, and granting one is what
/// makes that name worth having. Nesting included: the ACL names only
/// the outer group.
#[tokio::test]
async fn a_nested_group_carries_a_grant_to_its_members() {
    use raft5_client::{Denial, Principal, Right};

    let n1 = TestNode::start_with_auth("perm-c2", auth_config(Some(("root", "hunter42"))).await);
    let admin = Client::new([n1.url()]).unwrap();
    admin.login("root", "hunter42").await.unwrap();
    bootstrap(&admin, "perm-c2").await;
    admin.write(b"app/config", Some(b"v")).await.unwrap();
    for name in ["alice", "bob"] {
        admin.create_user(name, "correct horse", false).await.unwrap();
    }

    admin
        .create_group("interns", &[Principal::user("alice")], &[])
        .await
        .unwrap();
    admin
        .create_group("staff", &[Principal::group("interns")], &[])
        .await
        .unwrap();
    admin
        .set_acl(
            b"app/",
            &[
                (Principal::user("root"), Right::Write),
                (Principal::group("staff"), Right::Read),
            ],
        )
        .await
        .unwrap();

    let alice = Client::new([n1.url()]).unwrap();
    alice.login("alice", "correct horse").await.unwrap();
    let bob = Client::new([n1.url()]).unwrap();
    bob.login("bob", "correct horse").await.unwrap();

    assert_eq!(
        alice.read_one(b"app/config").await.unwrap().value,
        Some(b"v".to_vec()),
        "membership of a nested group should carry the outer group's grant"
    );
    assert_eq!(
        alice.get_acl(b"app/config").await.unwrap().effective,
        Some(Right::Read)
    );
    // Read, not write.
    assert!(matches!(
        alice.write(b"app/config", Some(b"x")).await,
        Err(Error::Denied { reason: Denial::NotPermitted, .. })
    ));
    // Bob is in nothing.
    assert!(matches!(
        bob.read_one(b"app/config").await,
        Err(Error::Denied { reason: Denial::NotPermitted, .. })
    ));

    // Taking alice out of the inner group takes her out of the outer
    // one, and the very next call sees it.
    admin
        .update_group("interns", &[], &[Principal::user("alice")], &[], &[])
        .await
        .unwrap();
    assert!(matches!(
        alice.read_one(b"app/config").await,
        Err(Error::Denied { reason: Denial::NotPermitted, .. })
    ));

    // The listings are readable by anyone with an account: a caller who
    // could not read them could not tell a refusal from a bug.
    let groups = bob.list_groups().await.unwrap();
    assert_eq!(
        groups.iter().map(|g| g.name.as_str()).collect::<Vec<_>>(),
        vec!["interns", "staff"]
    );
    assert_eq!(bob.list_acls().await.unwrap().len(), 1);
}

/// The paging case the server-side cursor exists for: a walk over a
/// store the caller can see almost none of.
///
/// The iterator has to keep going through batches that come back empty,
/// which is the thing it could not do before -- an empty batch used to
/// mean the end of the keyspace, and here it means "still looking".
#[tokio::test]
async fn a_key_walk_pages_past_everything_the_caller_may_not_see() {
    use raft5_client::{Principal, Right};

    let n1 = TestNode::start_with_auth("perm-c3", auth_config(Some(("root", "hunter42"))).await);
    let admin = Client::new([n1.url()]).unwrap();
    admin.login("root", "hunter42").await.unwrap();
    bootstrap(&admin, "perm-c3").await;

    for i in 0..40 {
        admin
            .write(format!("hidden/{i:02}").as_bytes(), Some(b"v"))
            .await
            .unwrap();
    }
    // Sorts after every hidden key, so reaching it means the walk got
    // all the way through them.
    admin.write(b"zzz", Some(b"v")).await.unwrap();
    admin
        .create_user("alice", "correct horse", false)
        .await
        .unwrap();
    admin
        .set_acl(b"hidden/", &[(Principal::user("root"), Right::Write)])
        .await
        .unwrap();

    let alice = Client::new([n1.url()]).unwrap();
    alice.login("alice", "correct horse").await.unwrap();

    let keys = alice.key_iterator(b"").batch_size(5).collect().await.unwrap();
    assert_eq!(keys, vec![b"zzz".to_vec()]);

    // The administrator's walk is unaffected.
    let keys = admin.key_iterator(b"").batch_size(5).collect().await.unwrap();
    assert_eq!(keys.len(), 41);
}

/// An account may create a group -- it grants them nothing they did not
/// already have -- but writing an ACL where none yet decides is an
/// administrator's act. Without that rule any account could write an ACL
/// on the empty prefix and lock the cluster out of its own keyspace.
#[tokio::test]
async fn an_ordinary_account_may_make_a_group_but_not_claim_an_unowned_prefix() {
    use raft5_client::{Denial, Principal, Right};

    let n1 = TestNode::start_with_auth("perm-c4", auth_config(Some(("root", "hunter42"))).await);
    let admin = Client::new([n1.url()]).unwrap();
    admin.login("root", "hunter42").await.unwrap();
    bootstrap(&admin, "perm-c4").await;
    admin
        .create_user("alice", "correct horse", false)
        .await
        .unwrap();

    let alice = Client::new([n1.url()]).unwrap();
    alice.login("alice", "correct horse").await.unwrap();

    alice.create_group("oncall", &[], &[]).await.unwrap();
    let group = alice.get_group("oncall").await.unwrap();
    assert_eq!(group.created_by, "alice");
    assert_eq!(group.admins, vec![Principal::user("alice")]);

    // Nothing governs the empty prefix, so it is not hers to claim.
    let error = alice
        .set_acl(b"", &[(Principal::user("alice"), Right::Write)])
        .await
        .expect_err("the default confers no authority over the ACLs");
    assert!(
        matches!(error, Error::Denied { reason: Denial::NotPermitted, .. }),
        "got {error:?}"
    );

    // Once an administrator grants her group write on a prefix, that
    // prefix and everything under it are the group's to govern.
    admin
        .set_acl(b"app/", &[(Principal::group("oncall"), Right::Write)])
        .await
        .unwrap();

    // Administering a group is not being in one. Alice created
    // `oncall` and decides who is in it, and until she puts herself in
    // it the grant to the group does not reach her -- otherwise every
    // group's administrator would silently hold everything that group
    // was ever granted.
    assert!(matches!(
        alice.set_acl(b"app/sub/", &[(Principal::user("alice"), Right::Read)]).await,
        Err(Error::Denied { reason: Denial::NotPermitted, .. })
    ));
    alice
        .update_group("oncall", &[Principal::user("alice")], &[], &[], &[])
        .await
        .unwrap();
    alice
        .set_acl(b"app/sub/", &[(Principal::user("alice"), Right::Read)])
        .await
        .unwrap();
    // But not a sibling nothing reaches.
    assert!(matches!(
        alice.set_acl(b"app2/", &[(Principal::user("alice"), Right::Write)]).await,
        Err(Error::Denied { reason: Denial::NotPermitted, .. })
    ));

    // A group that does not exist is reported as such.
    assert!(matches!(
        alice.get_group("nope").await,
        Err(Error::Denied { reason: Denial::NoSuchGroup, .. })
    ));
}

/// Permission changes contend for one key -- the epoch, which every one
/// of them writes -- so two at once resolve the way any two conflicting
/// transactions do, and the loser is told to retry rather than quietly
/// deciding from a view that has already moved.
///
/// This is the cost of pinning those decisions, and it is worth knowing
/// exactly what a caller sees: both of these must end up applied, one of
/// them possibly after a retry.
#[tokio::test]
async fn two_permission_changes_at_once_both_land_one_may_retry() {
    use raft5_client::{Principal, Right};

    let n1 = TestNode::start_with_auth("perm-c5", auth_config(Some(("root", "hunter42"))).await);
    let admin = Client::new([n1.url()]).unwrap();
    admin.login("root", "hunter42").await.unwrap();
    bootstrap(&admin, "perm-c5").await;

    let a = admin.clone();
    let b = admin.clone();
    let one = tokio::spawn(async move {
        let grant = [(Principal::user("root"), Right::Write)];
        retry_on_abort(|| a.set_acl(b"one/", &grant)).await
    });
    let two = tokio::spawn(async move {
        let grant = [(Principal::user("root"), Right::Write)];
        retry_on_abort(|| b.set_acl(b"two/", &grant)).await
    });
    one.await.unwrap();
    two.await.unwrap();

    let acls = admin.list_acls().await.unwrap();
    assert_eq!(
        acls.iter().map(|a| a.prefix.clone()).collect::<Vec<_>>(),
        vec![b"one/".to_vec(), b"two/".to_vec()],
        "both changes should have landed"
    );
}

/// Replays a call that lost a lock race. An aborted transaction wrote
/// nothing, so replaying it is always safe -- the client does not do it
/// automatically because in general the retry is the whole transaction,
/// reads and all, and only the caller knows what those were.
async fn retry_on_abort<F, Fut>(mut call: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<u64, Error>>,
{
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        match call().await {
            Ok(_) => return,
            Err(e @ Error::Aborted { .. }) | Err(e @ Error::Unavailable(_)) => {
                assert!(Instant::now() < until, "never succeeded: {e}");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(e) => panic!("unexpected failure: {e}"),
        }
    }
}

// --- TLS ----------------------------------------------------------------

#[path = "../../raft5-server/tests/common/certs.rs"]
mod certs;

/// Waits until `n` voters show up in the configuration of the node at
/// `addr`, dialed with `tls` -- so a test can take one down knowing the
/// rest still make a majority.
async fn wait_for_voters(addr: SocketAddr, tls: tonic::transport::ClientTlsConfig, n: usize) {
    use raft5_shared::raft5::client::GetConfigRequest;
    use raft5_shared::raft5::client::kv_client::KvClient;

    let channel = tonic::transport::Endpoint::from_shared(format!("https://{addr}"))
        .unwrap()
        .tls_config(tls)
        .unwrap()
        .connect_lazy();
    let mut kv = KvClient::new(channel);
    let until = Instant::now() + BOOTSTRAP_TIMEOUT;
    loop {
        if let Ok(resp) = kv.get_config(GetConfigRequest {}).await
            && resp.into_inner().members.iter().filter(|m| m.voting).count() == n
        {
            return;
        }
        assert!(Instant::now() < until, "never saw {n} voters");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A client that only knows one seed -- spelled `http://`, as an
/// address from before TLS would be -- finds the rest of a TLS cluster
/// through its configuration, and still reaches them once the seed is
/// gone. Every node past the first is one it dialed from an address the
/// cluster handed it, so every one of those dials had to get TLS too.
#[tokio::test]
async fn a_tls_client_follows_the_cluster_through_losing_its_seed() {
    let ca = certs::TestCa::new();
    let shared = ca.leaf(&["localhost", "127.0.0.1"]);
    let client_cert = ca.leaf(&["some-client"]);
    let node = |id| {
        TestNode::start_with_tls(
            id,
            raft5_server::PeerTls {
                identity: shared.identity(),
                ca: ca.certificate(),
                server_name: None,
            },
            raft5_server::ClientApiTls {
                identity: shared.identity(),
                client_ca: Some(ca.certificate()),
            },
        )
    };
    let n1 = node("n1");
    let n2 = node("n2");
    let n3 = node("n3");

    let client = Client::with_tls(
        [n1.url()],
        raft5_client::ClientTls::new(ca.certificate()).identity(client_cert.identity()),
    )
    .unwrap();
    bootstrap(&client, "n1").await;
    client.add_server(&n2.peer_url()).await.unwrap();
    client.add_server(&n3.peer_url()).await.unwrap();
    wait_for_voters(
        n2.client_addr,
        tonic::transport::ClientTlsConfig::new()
            .ca_certificate(ca.certificate())
            .identity(client_cert.identity()),
        3,
    )
    .await;
    client.write(b"k", Some(b"before")).await.unwrap();

    drop(n1);

    client.write(b"k", Some(b"after")).await.unwrap();
    assert_eq!(
        client.read_one(b"k").await.unwrap().value.as_deref(),
        Some(&b"after"[..])
    );
}

#[test]
fn a_tls_client_with_an_unusable_identity_is_refused_up_front() {
    let ca = certs::TestCa::new();
    let garbage = raft5_client::Identity::from_pem("not a cert", "not a key");
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _guard = runtime.enter();
    let result = Client::with_tls(
        ["https://127.0.0.1:1"],
        raft5_client::ClientTls::new(ca.certificate()).identity(garbage),
    );
    assert!(matches!(result, Err(Error::Tls(_))), "{:?}", result.err());
}

// --- many-key reads -----------------------------------------------------

/// One read, many keys, every consistency: each answers in the order
/// asked, with a missing key at seq 0 and `as_of` no older than anything
/// it read.
#[tokio::test]
async fn a_many_key_read_works_at_every_consistency() {
    let n1 = TestNode::start("n1");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "n1").await;

    let a = client.write(b"a", Some(b"1")).await.unwrap();
    let snap = client.create_snapshot(Some(b"s1")).await.unwrap();
    let b = client.write(b"b", Some(b"2")).await.unwrap();
    let keys: [&[u8]; 3] = [b"b", b"missing", b"a"];

    for consistency in [Consistency::Stale, Consistency::Linearizable] {
        let read = client.read(&keys, consistency).await.unwrap();
        assert!(read.is_complete(keys.len()));
        let got: Vec<_> = read
            .entries
            .iter()
            .map(|e| (e.key.as_slice(), e.seq, e.value.as_deref()))
            .collect();
        assert_eq!(
            got,
            [
                (&b"b"[..], b, Some(&b"2"[..])),
                (b"missing", 0, None),
                (b"a", a, Some(&b"1"[..])),
            ]
        );
        assert!(read.as_of >= b);

        let versions = client.read_versions(&keys, consistency).await.unwrap();
        let seqs: Vec<_> = versions.entries.iter().map(|v| v.seq).collect();
        assert_eq!(seqs, [b, 0, a]);
    }

    let at = client.read(&keys, Consistency::Snapshot(b"s1")).await.unwrap();
    assert_eq!(at.as_of, snap.seq);
    let seqs: Vec<_> = at.entries.iter().map(|e| e.seq).collect();
    assert_eq!(seqs, [0, 0, a], "b did not exist yet at the snapshot");

    let txn = client.transaction(Duration::from_secs(5)).await.unwrap();
    txn.write(b"a", Some(b"mine")).await.unwrap();
    let mine = txn.read(&keys).await.unwrap();
    assert_eq!(mine.entries[2].value.as_deref(), Some(&b"mine"[..]));
    assert_eq!(mine.entries[2].seq, a, "a buffered write keeps the committed seq");
    let versions = txn.read_versions(&keys).await.unwrap();
    let seqs: Vec<_> = versions.entries.iter().map(|v| v.seq).collect();
    assert_eq!(seqs, [b, 0, a]);
    txn.abort().await.unwrap();
}

/// Past the value budget the client hands back the prefix the cluster
/// answered rather than stitching reissues together, and reissuing for
/// the rest finishes the job.
#[tokio::test]
async fn a_read_past_the_value_budget_is_returned_short_for_the_caller_to_finish() {
    let n1 = TestNode::start("n1");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "n1").await;

    let big = vec![5u8; 6 * 1024 * 1024];
    for key in [&b"a"[..], b"b"] {
        client.write(key, Some(&big)).await.unwrap();
    }
    let keys: [&[u8]; 2] = [b"a", b"b"];

    let first = client.read(&keys, Consistency::Linearizable).await.unwrap();
    assert!(!first.is_complete(keys.len()));
    assert_eq!(first.entries.len(), 1);
    assert_eq!(first.entries[0].key, b"a");

    let rest = client
        .read(&keys[first.entries.len()..], Consistency::Linearizable)
        .await
        .unwrap();
    assert!(rest.is_complete(1));
    assert_eq!(rest.entries[0].key, b"b");
    assert_eq!(rest.entries[0].value.as_deref(), Some(big.as_slice()));

    let versions = client.read_versions(&keys, Consistency::Stale).await.unwrap();
    assert!(versions.is_complete(keys.len()), "versions alone are never cut short");
}

/// Too many keys is refused here, before anything is sent.
#[tokio::test]
async fn a_read_of_too_many_keys_is_refused_by_the_client() {
    let n1 = TestNode::start("n1");
    let client = Client::new([n1.url()]).unwrap();
    let keys = vec![b"k".to_vec(); raft5_shared::MAX_KEYS_PER_READ + 1];
    let read = client.read(&keys, Consistency::Stale).await;
    assert!(matches!(read, Err(Error::InvalidArgument(_))), "{read:?}");
}
