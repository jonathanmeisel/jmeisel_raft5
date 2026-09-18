//! Getting hold of a lock: creating it if nobody has it, and taking it
//! over if whoever does has stopped saying so.

use std::time::Instant;

use raft5_client::{Client, Update};

use crate::lock::Lock;
use crate::timing::{poll_gap, stepdown, takeover};
use crate::value::{Epoch, Malformed, Value};
use crate::{Error, LockOptions, MAX_UPDATE_INTERVAL, MIN_UPDATE_INTERVAL};

/// Whether a blocked attempt waits or gives up.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Block,
    Immediate,
}

/// A value a challenger is watching go quiet.
struct Watch {
    bytes: Vec<u8>,
    /// When these exact bytes were first seen. Reset only when the value
    /// changes, never on a mere re-look.
    first_seen: Instant,
    /// How long they must stay put, derived from the interval *this
    /// value* published.
    threshold: std::time::Duration,
}

/// What one compare-and-swap attempt found. Recorded by the closure
/// rather than inferred from `update`'s return value, because
/// [`Update::Noop`] makes `update` return the sequence number it read at
/// *without writing* -- so the return value alone cannot tell "I took it"
/// from "someone else holds it".
enum Outcome {
    /// The write landed: the lock is ours.
    Took,
    /// The key already holds our own epoch.
    AlreadyOurs,
    /// Someone else holds it. Carries the exact bytes seen, which is what
    /// the next attempt's compare-and-swap is conditioned on.
    Blocked(Vec<u8>, Value),
    /// The key is not a lock this build can read.
    Malformed(Malformed),
}

/// Acquires `lock_name`, waiting as long as it takes.
///
/// Must be called from within a Tokio runtime: the returned [`Lock`] owns
/// a spawned task that keeps it alive.
///
/// There is no built-in timeout -- wrap the call in
/// `tokio::time::timeout` for one. Cancelling it is safe, with one
/// caveat: a cancellation landing in the window between the acquiring
/// write committing and this function returning leaves the key held by
/// nobody until it expires.
pub async fn acquire(
    client: &Client,
    user_id: &str,
    lock_name: &str,
    options: LockOptions,
) -> Result<Lock, Error> {
    obtain(client, user_id, lock_name, options, Mode::Block).await
}

/// Acquires `lock_name` only if it is free right now.
///
/// Never waits on a holder: a key that is held sends it straight back
/// with [`Error::Held`]. (It may still write twice against a cluster slow
/// enough that its first write spent the lease it was establishing.)
///
/// It therefore *cannot* reclaim a lock whose
/// holder has died -- deciding a holder is dead means watching its value
/// sit unchanged for longer than it promised to go quiet, which takes
/// time this call does not spend. A caller polling `try_acquire` against
/// a crashed holder will be refused forever; use
/// `tokio::time::timeout(d, acquire(..))` for a bounded wait that can
/// still reclaim.
pub async fn try_acquire(
    client: &Client,
    user_id: &str,
    lock_name: &str,
    options: LockOptions,
) -> Result<Lock, Error> {
    obtain(client, user_id, lock_name, options, Mode::Immediate).await
}

async fn obtain(
    client: &Client,
    user_id: &str,
    lock_name: &str,
    options: LockOptions,
    mode: Mode,
) -> Result<Lock, Error> {
    if lock_name.is_empty() {
        return Err(Error::InvalidArgument("a lock needs a name"));
    }
    if !(MIN_UPDATE_INTERVAL..=MAX_UPDATE_INTERVAL).contains(&options.update_interval) {
        return Err(Error::InvalidOptions(
            "update_interval must be between MIN_UPDATE_INTERVAL and MAX_UPDATE_INTERVAL",
        ));
    }

    // One epoch for the whole call, however many attempts it takes: it is
    // what identifies *this* acquisition, and re-minting it per attempt
    // would break the `AlreadyOurs` arm below.
    let epoch = Epoch::new();
    let mine = Value::new(options.update_interval, epoch, user_id);
    let encoded = mine.encode();
    let key = lock_name.as_bytes();

    // The value being watched, and since when. Any change to the bytes
    // resets it -- which is what makes this a statement about a real
    // interval of quiet rather than about a number of samples, and so what
    // keeps it correct when the holder's published interval changes under
    // us.
    let mut watching: Option<Watch> = None;

    loop {
        // The bytes this attempt is allowed to overwrite, and only once
        // they have been quiet for the full period their holder published.
        // Keeping this separate from `watching` is load-bearing: a
        // challenger looks at the value several times before it may claim
        // it, and passing the watched bytes in before the period was up
        // would have every poll take the lock on its second look.
        let claim = watching
            .as_ref()
            .filter(|w| w.first_seen.elapsed() >= w.threshold)
            .map(|w| w.bytes.clone());

        let mut outcome = None;
        // Anchored before the attempt, so a lease that starts here starts
        // conservatively. See `timing::stepdown`.
        let started = Instant::now();

        let result = client
            .update(key, |current| match current {
                // Free. The compare-and-swap precondition is sequence
                // number 0 -- the cluster's own "this key has no value"
                // -- so two racing creators cannot both win.
                None => {
                    outcome = Some(Outcome::Took);
                    Update::Write(Some(encoded.clone()))
                }
                // Exactly the bytes we watched go quiet, still unchanged
                // at the instant of the swap. Comparing bytes is a sound
                // CAS only because the epoch makes a value string
                // unrepeatable; see `value`.
                Some(bytes) if claim.as_deref() == Some(bytes) => {
                    outcome = Some(Outcome::Took);
                    Update::Write(Some(encoded.clone()))
                }
                Some(bytes) => match Value::decode(bytes) {
                    // Our own epoch, on an attempt we did not think had
                    // landed: the write committed and its reply was lost,
                    // and `update`'s retry then re-read and re-ran this
                    // closure. Without this arm we would mistake our own
                    // value for a rival's and orphan the lock until it
                    // expired.
                    //
                    // Refreshed rather than left alone, because this arm
                    // is also how a lease that was spent proving itself
                    // gets a fresh one (see `Outcome::Took` below). It
                    // must be `next()`: rewriting identical bytes would
                    // look like no change at all to a challenger, which
                    // is the one thing a refresh must never look like.
                    Ok(v) if v.epoch == epoch => {
                        outcome = Some(Outcome::AlreadyOurs);
                        Update::Write(Some(v.next().encode()))
                    }
                    Ok(v) => {
                        outcome = Some(Outcome::Blocked(bytes.to_vec(), v));
                        Update::Noop
                    }
                    Err(m) => {
                        outcome = Some(Outcome::Malformed(m));
                        Update::Noop
                    }
                },
            })
            .await;

        match (result, outcome) {
            (Ok(seq), Some(Outcome::Took | Outcome::AlreadyOurs)) => {
                // The lease this write establishes runs from `started`,
                // the conservative end of it -- so a write that was itself
                // slow hands back a lock with most of its life already
                // gone, and one slow enough hands back a lock that is
                // already dead. (The first write after a bootstrap, or
                // one spanning an election, can easily take longer than
                // the whole deadline.)
                //
                // Rather than return that, write again: the key is ours
                // now, so the next pass takes the `AlreadyOurs` arm, and a
                // write that lands promptly anchors a full lease. Nobody
                // can take the lock away in the meantime -- a challenger
                // needs `takeover` of quiet and that is longer still.
                if started.elapsed() + options.update_interval >= stepdown(options.update_interval)
                {
                    log::debug!(
                        "acquiring {lock_name:?} took {}ms, most of a {}ms lease; re-anchoring",
                        started.elapsed().as_millis(),
                        stepdown(options.update_interval).as_millis()
                    );
                    watching = None;
                    continue;
                }
                return Ok(Lock::start(
                    client.clone(),
                    lock_name.to_string(),
                    mine,
                    seq,
                    started,
                ));
            }
            // Terminal, and deliberately so: overwriting a key we cannot
            // read would destroy whatever is actually there. See the
            // crate doc.
            (_, Some(Outcome::Malformed(Malformed(detail)))) => {
                return Err(Error::Malformed {
                    key: lock_name.to_string(),
                    detail,
                });
            }
            (Ok(_), Some(Outcome::Blocked(bytes, held))) => {
                // The holder's own published interval, not ours: how long
                // it may go quiet is its promise to keep, and only it
                // knows what it promised. Re-read every pass, so a
                // takeover that changes the interval is picked up rather
                // than assumed away.
                let threshold = takeover(held.update);

                if mode == Mode::Immediate {
                    return Err(Error::Held {
                        by: held.user_id,
                        retry_after: threshold,
                    });
                }

                match &watching {
                    // Still the same value: leave `first_seen` where it
                    // is, since the whole question is how long these exact
                    // bytes have been sitting there.
                    Some(w) if w.bytes == bytes => {}
                    _ => {
                        watching = Some(Watch {
                            bytes,
                            first_seen: Instant::now(),
                            threshold,
                        });
                    }
                }

                // Quiet for long enough: go straight round without
                // sleeping, and the next pass arms `claim` and swaps.
                if watching
                    .as_ref()
                    .is_some_and(|w| w.first_seen.elapsed() >= threshold)
                {
                    continue;
                }

                // Otherwise wait and look again -- the next iteration's
                // `update` does a linearizable read of its own, so it
                // *is* the next poll. Nothing here reads stale: a lagging
                // follower would manufacture a false staleness verdict.
                tokio::time::sleep(poll_gap(held.update)).await;
            }
            // Every attempt lost its race. That says the key is busy, not
            // who holds it, so stop watching and look again.
            (Err(raft5_client::Error::Conflict { .. }), _) => {
                watching = None;
            }
            (Err(e), _) => return Err(Error::Cluster(e)),
            (Ok(_), None) => unreachable!("the update closure always records an outcome"),
        }
    }
}
