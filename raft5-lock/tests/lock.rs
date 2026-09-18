//! Holding a lock, against a real cluster: acquiring, releasing,
//! contending, and the fencing token.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use raft5_lock::{Error, LockOptions};

use common::cluster;

/// A brisk interval, so tests that have to watch a refresh happen do not
/// have to wait around for it.
fn fast() -> LockOptions {
    LockOptions {
        update_interval: Duration::from_millis(100),
    }
}

#[tokio::test]
async fn acquire_and_release_round_trip() {
    let (_n1, client) = cluster("n1").await;

    let lock = raft5_lock::acquire(&client, "alice", "job", LockOptions::default())
        .await
        .unwrap();
    assert!(lock.is_held());
    assert_eq!(lock.owner(), "alice");
    assert_eq!(lock.name(), "job");
    assert!(lock.fence() > 0, "a held lock must have a fencing token");
    assert!(client.read(b"job").await.unwrap().value.is_some());

    lock.release().await.unwrap();
    assert_eq!(client.read(b"job").await.unwrap().value, None);
}

/// Pins the wire format the takeover fixtures hand-write.
#[tokio::test]
async fn the_stored_value_matches_the_documented_format() {
    let (_n1, client) = cluster("n1").await;
    let lock = raft5_lock::acquire(&client, "alice", "job", LockOptions::default())
        .await
        .unwrap();

    let raw = client.read(b"job").await.unwrap().value.unwrap();
    let text = String::from_utf8(raw).unwrap();
    let fields: Vec<&str> = text.splitn(5, ' ').collect();

    assert_eq!(fields.len(), 5, "{text:?}");
    assert_eq!(fields[0], "v1");
    assert_eq!(fields[1], "200");
    assert_eq!(fields[2], lock.epoch());
    assert!(fields[3].parse::<u64>().is_ok(), "counter: {:?}", fields[3]);
    assert_eq!(fields[4], "alice");
}

#[tokio::test]
async fn a_second_acquire_waits_for_the_first_to_release() {
    let (_n1, client) = cluster("n1").await;

    let first = raft5_lock::acquire(&client, "alice", "job", fast())
        .await
        .unwrap();
    let first_fence = first.fence();

    let waiter = {
        let client = client.clone();
        tokio::spawn(async move { raft5_lock::acquire(&client, "bob", "job", fast()).await })
    };

    // Long enough that a broken implementation would have taken it.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !waiter.is_finished(),
        "bob took a lock alice was still holding"
    );

    first.release().await.unwrap();
    let second = tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("bob should get the lock once alice releases it")
        .unwrap()
        .unwrap();

    assert_eq!(second.owner(), "bob");
    assert!(
        second.fence() > first_fence,
        "a later holder must outrank an earlier one: {} vs {first_fence}",
        second.fence()
    );
}

/// The headline test: if two holders ever overlap, the counter goes above
/// one and this trips.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn contenders_never_hold_at_once() {
    const TASKS: usize = 3;
    const ROUNDS: usize = 3;

    let (_n1, client) = cluster("n1").await;
    let holders = Arc::new(AtomicUsize::new(0));

    let mut tasks = Vec::new();
    for i in 0..TASKS {
        let (client, holders) = (client.clone(), Arc::clone(&holders));
        tasks.push(tokio::spawn(async move {
            for _ in 0..ROUNDS {
                let lock = raft5_lock::acquire(&client, &format!("w{i}"), "job", fast())
                    .await
                    .unwrap();

                let before = holders.fetch_add(1, Ordering::SeqCst);
                assert_eq!(before, 0, "two holders at once");
                tokio::time::sleep(Duration::from_millis(30)).await;
                let after = holders.fetch_sub(1, Ordering::SeqCst);
                assert_eq!(after, 1, "two holders at once");

                lock.release().await.unwrap();
            }
        }));
    }

    for task in tasks {
        task.await.expect("a contending task panicked");
    }
}

#[tokio::test]
async fn try_acquire_is_refused_while_the_lock_is_held() {
    let (_n1, client) = cluster("n1").await;
    let _held = raft5_lock::acquire(&client, "alice", "job", LockOptions::default())
        .await
        .unwrap();

    let started = Instant::now();
    let err = raft5_lock::try_acquire(&client, "bob", "job", LockOptions::default())
        .await
        .unwrap_err();

    assert!(
        matches!(&err, Error::Held { by, .. } if by == "alice"),
        "expected Held by alice, got {err}"
    );
    // Asserted so that a regression into waiting is caught rather than
    // just making the suite slower.
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "try_acquire waited {:?}; it must not",
        started.elapsed()
    );
}

#[tokio::test]
async fn try_acquire_succeeds_once_the_lock_is_released() {
    let (_n1, client) = cluster("n1").await;

    let held = raft5_lock::acquire(&client, "alice", "job", LockOptions::default())
        .await
        .unwrap();
    held.release().await.unwrap();

    let lock = raft5_lock::try_acquire(&client, "bob", "job", LockOptions::default())
        .await
        .unwrap();
    assert_eq!(lock.owner(), "bob");
}

/// Also the proof that the refresher is running at all.
#[tokio::test]
async fn the_fence_advances_with_each_refresh() {
    let (_n1, client) = cluster("n1").await;
    let lock = raft5_lock::acquire(&client, "alice", "job", fast())
        .await
        .unwrap();

    let first = lock.fence();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let later = lock.fence();

    assert!(
        lock.is_held(),
        "the lock should still be held: {:?}",
        lock.loss_reason()
    );
    assert!(
        later > first,
        "the fence did not advance: {first} -> {later}"
    );
}

/// `Drop` cannot await, so the delete lands shortly afterwards rather
/// than before drop returns -- assert it eventually, not immediately.
#[tokio::test]
async fn dropping_a_lock_deletes_the_key() {
    let (_n1, client) = cluster("n1").await;
    {
        let _lock = raft5_lock::acquire(&client, "alice", "job", LockOptions::default())
            .await
            .unwrap();
        assert!(client.read(b"job").await.unwrap().value.is_some());
    }

    let until = Instant::now() + Duration::from_secs(2);
    loop {
        if client.read(b"job").await.unwrap().value.is_none() {
            return;
        }
        assert!(Instant::now() < until, "the key outlived its dropped lock");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn an_out_of_range_interval_is_refused() {
    let (_n1, client) = cluster("n1").await;
    let options = LockOptions {
        update_interval: Duration::from_millis(1),
    };
    assert!(matches!(
        raft5_lock::acquire(&client, "alice", "job", options).await,
        Err(Error::InvalidOptions(_))
    ));
}
