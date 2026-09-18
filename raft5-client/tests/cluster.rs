//! The client driven against real `raft5_server` nodes on real
//! localhost sockets -- the parts of it that only exist because a
//! cluster moves under a client: redirects, discovery, and a membership
//! that changes while the client is using it.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio_util::sync::CancellationToken;

use raft5_client::Client;

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
    admin
        .add_server("n2", &n2.peer_url(), &n2.url())
        .await
        .unwrap();
    admin
        .add_server("n3", &n3.peer_url(), &n3.url())
        .await
        .unwrap();

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
    client
        .add_server("n2", &n2.peer_url(), &n2.url())
        .await
        .unwrap();
    client
        .add_server("n3", &n3.peer_url(), &n3.url())
        .await
        .unwrap();
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

/// A closure that gives up takes the whole update with it: there is
/// nothing sensible to write, so nothing is written.
#[tokio::test]
async fn a_closure_that_fails_writes_nothing() {
    let n1 = TestNode::start("n1");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "n1").await;
    client.write(b"k", Some(b"before")).await.unwrap();

    let err = client
        .update(b"k", |_| Err("not today".into()))
        .await
        .unwrap_err();
    assert!(
        matches!(&err, raft5_client::Error::Update(e) if e.to_string() == "not today"),
        "expected the closure's own error, got {err}"
    );
    assert_eq!(
        client.read(b"k").await.unwrap().value.as_deref(),
        Some(&b"before"[..])
    );
}

/// Returning no value deletes the key, so the same call that updates a
/// value can also decide it should no longer exist.
#[tokio::test]
async fn an_update_returning_nothing_deletes_the_key() {
    let n1 = TestNode::start("n1");
    let client = Client::new([n1.url()]).unwrap();
    bootstrap(&client, "n1").await;
    client.write(b"k", Some(b"v")).await.unwrap();

    client.update(b"k", |_| Ok(None)).await.unwrap();
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
            })?;
            Ok(current.map(<[u8]>::to_vec))
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
fn increment(current: Option<&[u8]>) -> Result<Option<Vec<u8>>, raft5_client::BoxError> {
    let n: u64 = match current {
        Some(bytes) => std::str::from_utf8(bytes)?.parse()?,
        None => 0,
    };
    Ok(Some((n + 1).to_string().into_bytes()))
}
