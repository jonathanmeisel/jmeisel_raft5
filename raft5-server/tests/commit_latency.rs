//! A reproduction for the burst-commit latency sawtooth: a real three-node
//! cluster taking ~1 MiB transactions, reporting each commit's latency in
//! order, so a ramp (and its reset) is visible, not averaged away.
//!
//! Two shapes, because they stress different things:
//! - `sequential_burst` waits for each commit before starting the next, so
//!   there is never a queue, and it measures one commit's cost.
//! - `concurrent_burst` uploads every transaction first and then sends all
//!   the commits at once, which builds a backlog.
//!
//! Ignored by default: these are measurements, not assertions. Run:
//!
//! ```text
//! cargo test -p raft5-server --test commit_latency -- --ignored --nocapture
//! ```
//!
//! **The database goes wherever `std::env::temp_dir()` points, which is
//! often `/tmp`, a tmpfs (RAM).** A run there measures CPU and protocol
//! only and cannot show anything I/O bound. To measure on a real disk,
//! point `TMPDIR` at one:
//!
//! ```text
//! TMPDIR=/var/tmp cargo test -p raft5-server --test commit_latency -- --ignored --nocapture
//! ```

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{Client, TestNode, add_server, begin, bootstrap, commit, txn_write, wait_until_voter};

/// The value size the latency was reported at. `VALUE_KIB` overrides it.
fn value_bytes() -> usize {
    env_usize("VALUE_KIB", 1024) * 1024
}

/// "Up to 50 in short succession", as reported. `BURST` overrides it. Worth
/// raising well past the 64 MiB memtable, because a short burst never
/// flushes and so never shows anything bound by flush or compaction.
fn burst() -> usize {
    env_usize("BURST", 50)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

async fn cluster(prefix: &str) -> (TestNode, TestNode, TestNode) {
    let n1 = TestNode::new(&format!("{prefix}1")).await;
    let n2 = TestNode::new(&format!("{prefix}2")).await;
    let n3 = TestNode::new(&format!("{prefix}3")).await;

    let c1 = n1.client();
    bootstrap(&c1, &n1.id).await;
    add_server(&c1, &n2).await;
    add_server(&c1, &n3).await;
    wait_until_voter(&c1, &n2.id).await;
    wait_until_voter(&c1, &n3.id).await;

    println!("db dir: {}", n1.db_dir.display());
    (n1, n2, n3)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "latency measurement; run explicitly with --ignored --nocapture"]
async fn sequential_burst() {
    let (burst, value_bytes) = (burst(), value_bytes());
    let (n1, n2, n3) = cluster("slat").await;
    let client = n1.client();

    // One buffer reused for every transaction: the point is to measure the
    // server, not to allocate a megabyte per loop iteration.
    let value = vec![b'x'; value_bytes];

    let mut upload_ms = Vec::with_capacity(burst);
    let mut commit_ms = Vec::with_capacity(burst);

    for i in 0..burst {
        // A different key per transaction, so nothing in the burst waits on
        // another's lock, and the commit path is measured, not contention.
        let key = format!("burst/{i:04}").into_bytes();
        let id = begin(&client, Duration::from_secs(10)).await;

        let upload_start = Instant::now();
        txn_write(&client, &id, &key, Some(&value)).await;
        upload_ms.push(ms_since(upload_start));

        let commit_start = Instant::now();
        commit(&client, &id)
            .await
            .unwrap_or_else(|e| panic!("commit {i} was refused: {e:?}"));
        commit_ms.push(ms_since(commit_start));
    }

    report("sequential: upload (BufferWrite)", &upload_ms);
    report("sequential: commit", &commit_ms);

    drop((n1, n2, n3));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "latency measurement; run explicitly with --ignored --nocapture"]
async fn concurrent_burst() {
    let (burst, value_bytes) = (burst(), value_bytes());
    let (n1, n2, n3) = cluster("clat").await;
    let client = n1.client();
    let value = vec![b'x'; value_bytes];

    // Upload everything first, so the measured burst is the commits alone, not
    // uploads racing them.
    let mut ids = Vec::with_capacity(burst);
    for i in 0..burst {
        let key = format!("cburst/{i:04}").into_bytes();
        let id = begin(&client, Duration::from_secs(10)).await;
        txn_write(&client, &id, &key, Some(&value)).await;
        ids.push(id);
    }

    let mut tasks = Vec::with_capacity(burst);
    for (i, id) in ids.into_iter().enumerate() {
        let client: Arc<Client> = Arc::clone(&client);
        tasks.push(tokio::spawn(async move {
            let started = Instant::now();
            let result = commit(&client, &id).await;
            (i, ms_since(started), result.is_ok())
        }));
    }

    let mut commit_ms = vec![0.0; burst];
    let mut refused = Vec::new();
    for task in tasks {
        let (i, ms, ok) = task.await.expect("commit task should not panic");
        commit_ms[i] = ms;
        if !ok {
            refused.push(i);
        }
    }
    assert!(refused.is_empty(), "commits were refused: {refused:?}");

    report("concurrent: commit (in issue order)", &commit_ms);

    drop((n1, n2, n3));
}

fn ms_since(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

/// Print the series in order, then its summary. The series is what matters:
/// a mean hides exactly the shape under investigation.
fn report(label: &str, samples: &[f64]) {
    println!("\n=== {label} -- {} samples, in order ===", samples.len());
    for (i, ms) in samples.iter().enumerate() {
        // A bar as well as the number, so a ramp is visible at a glance.
        let bars = (*ms / 5.0).round() as usize;
        println!("{i:3}  {ms:8.1} ms  {}", "#".repeat(bars.min(100)));
    }

    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).expect("no NaN timings"));
    let pct = |p: f64| sorted[((sorted.len() - 1) as f64 * p).round() as usize];
    println!(
        "min {:.1}  p50 {:.1}  p90 {:.1}  p99 {:.1}  max {:.1}  (ms)",
        sorted[0],
        pct(0.50),
        pct(0.90),
        pct(0.99),
        sorted[sorted.len() - 1],
    );
}
