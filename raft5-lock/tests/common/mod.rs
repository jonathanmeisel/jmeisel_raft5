//! A real single-node cluster for the lock tests.
//!
//! Copied from `raft5-client/tests/cluster.rs` -- it lives under that
//! crate's `tests/`, so it cannot be imported across crates.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use raft5_client::Client;
use tokio_util::sync::CancellationToken;

const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(10);

/// A node started for a test: fresh database directory, ephemeral ports,
/// and the real server loop running in the background. Stopped and
/// cleaned up on drop.
pub struct TestNode {
    id: String,
    client_addr: SocketAddr,
    db_dir: PathBuf,
    cancel: CancellationToken,
}

impl TestNode {
    pub fn start(id: &str) -> Self {
        let _ = env_logger::try_init();
        let node = Self {
            id: id.to_string(),
            client_addr: free_local_addr(),
            db_dir: temp_db_dir(id),
            cancel: CancellationToken::new(),
        };

        let (server_id, db_dir) = (node.id.clone(), node.db_dir.clone());
        let (client_addr, peer_addr) = (node.client_addr, free_local_addr());
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

    pub fn url(&self) -> String {
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
        "raft5_lock_test_{}_{label}_{nanos:x}_{counter:x}",
        std::process::id()
    ));
    path
}

/// `Client::bootstrap` deliberately doesn't retry (it is irreversible),
/// but a node that has only just started may not be ready to accept one
/// yet.
pub async fn bootstrap(client: &Client, server_id: &str) {
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

/// A started, bootstrapped single-node cluster and a client for it.
pub async fn cluster(id: &str) -> (TestNode, Client) {
    let node = TestNode::start(id);
    let client = Client::new([node.url()]).unwrap();
    bootstrap(&client, id).await;
    (node, client)
}
