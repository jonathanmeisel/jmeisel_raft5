//! Losing a lock and taking one over -- the parts that only exist because
//! a holder can stop without saying so.

mod common;

use std::time::{Duration, Instant};

use raft5_lock::{Error, LockOptions, LossReason};

use common::cluster;

fn fast() -> LockOptions {
    LockOptions {
        update_interval: Duration::from_millis(100),
    }
}

/// A hand-written value for a holder that never existed, in the format
/// `tests/lock.rs::the_stored_value_matches_the_documented_format` pins.
fn abandoned(update_ms: u64, epoch: &str, user: &str) -> Vec<u8> {
    format!("v1 {update_ms} {epoch} 7 {user}").into_bytes()
}

/// Taken over, but only after waiting the holder's full quiet period --
/// the elapsed-time assertion is what distinguishes reclaiming from
/// stealing.
#[tokio::test]
async fn an_abandoned_lock_is_taken_over() {
    let (_n1, client) = cluster("n1").await;
    client
        .write(b"job", Some(&abandoned(100, "dead1234", "ghost")))
        .await
        .unwrap();

    let started = Instant::now();
    let lock = raft5_lock::acquire(&client, "alice", "job", fast())
        .await
        .unwrap();
    let waited = started.elapsed();

    assert_eq!(lock.owner(), "alice");
    assert_ne!(lock.epoch(), "dead1234");
    // The threshold is 8x the *observed* interval of 100ms, which is the
    // ghost's published one -- not alice's own.
    assert!(
        waited >= Duration::from_millis(800),
        "took the lock after only {waited:?}; it must watch for the full quiet period"
    );
}

/// The one test that would catch a takeover threshold set below the
/// holder's own give-up deadline.
#[tokio::test]
async fn a_live_lock_is_not_taken_over() {
    let (_n1, client) = cluster("n1").await;
    let held = raft5_lock::acquire(&client, "alice", "job", LockOptions::default())
        .await
        .unwrap();

    let attempt = raft5_lock::acquire(&client, "bob", "job", LockOptions::default());
    assert!(
        tokio::time::timeout(Duration::from_secs(3), attempt)
            .await
            .is_err(),
        "bob took a lock alice was actively refreshing"
    );
    assert!(held.is_held(), "alice lost it: {:?}", held.loss_reason());
}

/// The end-to-end version: a real refresher that stops, rather than a
/// fabricated value.
#[tokio::test]
async fn a_holder_that_stops_refreshing_loses_the_lock_to_a_challenger() {
    let (_n1, client) = cluster("n1").await;

    let first = raft5_lock::acquire(&client, "alice", "job", fast())
        .await
        .unwrap();
    let alice_epoch = first.epoch();
    first.abandon();

    let second = tokio::time::timeout(
        Duration::from_secs(10),
        raft5_lock::acquire(&client, "bob", "job", fast()),
    )
    .await
    .expect("bob should reclaim an abandoned lock")
    .unwrap();

    assert_eq!(second.owner(), "bob");
    assert_ne!(second.epoch(), alice_epoch);
}

#[tokio::test]
async fn a_holder_that_is_stolen_from_finds_out() {
    let (_n1, client) = cluster("n1").await;
    let lock = raft5_lock::acquire(&client, "alice", "job", fast())
        .await
        .unwrap();

    client
        .write(b"job", Some(&abandoned(100, "beef", "thief")))
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(2), lock.lost())
        .await
        .expect("the holder should notice a theft within a refresh or two");
    assert!(!lock.is_held());
    assert_eq!(
        lock.loss_reason(),
        Some(LossReason::Stolen {
            by: "thief".to_string()
        })
    );
}

/// The conditional-delete safety property: a holder that was taken over
/// must not delete its successor's lock when it tidies up.
#[tokio::test]
async fn releasing_after_a_takeover_does_not_delete_the_successor() {
    let (_n1, client) = cluster("n1").await;
    let lock = raft5_lock::acquire(&client, "alice", "job", fast())
        .await
        .unwrap();

    let successor = abandoned(100, "beef", "thief");
    client.write(b"job", Some(&successor)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), lock.lost())
        .await
        .expect("alice should notice");

    lock.release().await.unwrap();

    assert_eq!(
        client.read(b"job").await.unwrap().value.as_deref(),
        Some(&successor[..]),
        "alice deleted the lock that had been taken from her"
    );
}

/// Self-demotion: the property that makes the lease a lease.
#[tokio::test]
async fn a_holder_that_loses_the_cluster_steps_down() {
    let (node, client) = cluster("n1").await;
    let lock = raft5_lock::acquire(&client, "alice", "job", fast())
        .await
        .unwrap();

    drop(node); // the only server in the cluster

    tokio::time::timeout(Duration::from_secs(5), lock.lost())
        .await
        .expect("a holder that cannot reach the cluster must give the lock up");
    assert!(matches!(
        lock.loss_reason(),
        Some(LossReason::Expired { .. })
    ));
}

#[tokio::test]
async fn a_key_that_is_not_a_lock_is_refused_not_stolen() {
    let (_n1, client) = cluster("n1").await;
    client.write(b"job", Some(b"hello")).await.unwrap();

    let err = tokio::time::timeout(
        Duration::from_secs(2),
        raft5_lock::acquire(&client, "alice", "job", LockOptions::default()),
    )
    .await
    .expect("a malformed key must fail promptly, not wait")
    .unwrap_err();

    assert!(matches!(err, Error::Malformed { .. }), "got {err}");
    assert_eq!(
        client.read(b"job").await.unwrap().value.as_deref(),
        Some(&b"hello"[..]),
        "application data was overwritten by a lock"
    );
}

/// Cross-version safety, and the reason the version tag is field zero.
#[tokio::test]
async fn a_newer_format_version_is_refused_not_stolen() {
    let (_n1, client) = cluster("n1").await;
    let future = b"v2 200 abcd 1 alice".to_vec();
    client.write(b"job", Some(&future)).await.unwrap();

    let err = raft5_lock::acquire(&client, "bob", "job", LockOptions::default())
        .await
        .unwrap_err();

    assert!(matches!(err, Error::Malformed { .. }), "got {err}");
    assert_eq!(
        client.read(b"job").await.unwrap().value.as_deref(),
        Some(&future[..]),
        "a v2 lock was stolen by a v1 build"
    );
}
