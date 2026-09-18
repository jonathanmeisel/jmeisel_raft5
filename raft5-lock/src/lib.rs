//! Distributed locking on a raft5 cluster.
//!
//! A lock is one key in the store. [`acquire`] writes a value to it and
//! spawns a task that rewrites that value every `update_interval`,
//! bumping a counter; the returned [`Lock`] holds the task, and dropping
//! it stops the rewriting and deletes the key. A holder that stops
//! rewriting -- because it crashed, or because it could not reach the
//! cluster -- loses the lock to whoever is waiting for it.
//!
//! ```no_run
//! # async fn f(client: raft5_client::Client) -> Result<(), raft5_lock::Error> {
//! let lock = raft5_lock::acquire(&client, "worker-3", "nightly-job",
//!                                raft5_lock::LockOptions::default()).await?;
//!
//! tokio::select! {
//!     // The lease is gone; whatever we were doing, stop.
//!     _ = lock.lost() => return Err(raft5_lock::Error::InvalidArgument("lost the lock")),
//!     _ = do_the_work(lock.fence()) => {}
//! }
//!
//! lock.release().await?;
//! # Ok(()) }
//! # async fn do_the_work(_fence: u64) {}
//! ```
//!
//! Every mutation -- creating the key, refreshing it, taking it over,
//! deleting it -- goes through [`raft5_client::Client::update`], so every
//! mutation is a compare-and-swap and every race is settled by raft
//! rather than by timing.
//!
//! # This is a lease, not mutual exclusion
//!
//! Read this part. [`Lock::is_held`] returning `true` tells you what this
//! process last observed, not what is true now and certainly not what
//! will be true in the instant after you act on it. A holder can be
//! descheduled, paused by its garbage collector, stopped by a debugger,
//! or frozen with its VM between checking and acting; if that lasts past
//! its deadline, a challenger will take the lock while this process still
//! believes it holds one. No amount of tuning removes this. It is what
//! lease-based locking is.
//!
//! Two things are done about it. The holder **gives the lock up itself**
//! when it cannot refresh in time, on a deadline strictly tighter than
//! the one a challenger uses to decide it has died -- so in the common
//! case there is a window where nobody holds the lock rather than one
//! where two do (`timing` has the argument in full). And [`Lock::fence`]
//! hands out a **fencing token**, which is the only thing here that
//! actually protects a resource:
//!
//! ```no_run
//! # async fn f(lock: &raft5_lock::Lock) {
//! // Have the resource keep the highest token it has accepted and reject
//! // anything below it. A stalled holder's writes then arrive stale and
//! // bounce, whatever that holder believed about `is_held`.
//! write_to_resource(lock.fence(), b"payload").await;
//! # }
//! # async fn write_to_resource(_fence: u64, _data: &[u8]) {}
//! ```
//!
//! Raft sequence numbers come from one cluster-wide counter and are never
//! reused, so a later holder's token always exceeds an earlier one's.
//! Without a resource that checks the token, this crate is an
//! optimization -- it stops two workers doing the same job at the same
//! time *most* of the time -- not a guarantee. This is Kleppmann's
//! fencing argument, and it applies to every lease-based lock, not just
//! this one.
//!
//! # Other things worth knowing
//!
//! - **Clock rate, not clock time.** Nothing here compares timestamps
//!   between processes, so no NTP assumption is made. What is assumed is
//!   that two processes' monotonic clocks advance at roughly the same
//!   rate; see `timing` for how much divergence the margin absorbs.
//! - **Delete-on-drop is best effort.** A killed process never runs
//!   `Drop`, and even an orderly one may be racing its runtime's
//!   shutdown. The key is then reclaimed by expiry instead, which is the
//!   mechanism that actually makes this work. [`Lock::release`] is the
//!   only way to *know* the key is gone.
//! - **`update_interval` is a real decision.** Much below 150ms the
//!   holder's deadline drops under the server's worst-case election time
//!   and routine leader changes start dropping locks. Raising it makes
//!   locks ride out more turbulence and makes a dead holder's lock take
//!   proportionally longer to reclaim.
//! - **A takeover may change the timing.** The interval is published in
//!   the value and challengers obey whatever they find, so a holder
//!   configured at 5s makes every challenger wait 40s. This crate does
//!   not second-guess it.
//! - **A key that is not a lock is never overwritten.** If the name
//!   collides with application data, or with a format a future version
//!   writes, [`acquire`] fails with [`Error::Malformed`] and keeps
//!   failing until a human intervenes. That is deliberate: the
//!   alternative is silently destroying whatever was there.

use std::time::Duration;

mod acquire;
mod lock;
mod timing;
mod value;

pub use acquire::{acquire, try_acquire};
pub use lock::{Lock, LossReason};

/// How often a holder rewrites its lock key, unless told otherwise.
///
/// Chosen against the cluster's own timing rather than picked round: the
/// server heartbeats at 50ms and elections take 200-400ms, so 200ms puts
/// the derived give-up deadline (5x, so one second) comfortably clear of
/// a routine leader failover. A lock that dropped every time raft elected
/// would not be worth having.
pub const DEFAULT_UPDATE_INTERVAL: Duration = Duration::from_millis(200);

/// The floor, set by what a refresh actually costs.
///
/// A refresh is a write, and a write does not commit until the leader's
/// next heartbeat -- so one costs on the order of the server's 50ms
/// heartbeat however fast the network is. An interval near that would
/// spend the whole of itself on a single refresh, leaving a holder no
/// headroom at all: the first hiccup would take the lock. Twice that is
/// the point at which the derived deadline leaves room for several
/// attempts.
pub const MIN_UPDATE_INTERVAL: Duration = Duration::from_millis(100);

/// An hour of unchanged value would be reclaimed eight minutes after the
/// holder died, which is about as slow as a lock can be and still be one.
pub const MAX_UPDATE_INTERVAL: Duration = Duration::from_secs(60);

/// How a lock should be held.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LockOptions {
    /// How often the holder rewrites the key.
    ///
    /// Every other threshold -- how long this holder waits before
    /// deciding it has lost the lock, how long a challenger watches
    /// before deciding this holder has died -- is a fixed multiple of it.
    /// So this one number sets both how quickly a crashed holder's lock
    /// is reclaimed and how much cluster unavailability a live one
    /// survives, and the two cannot be traded off separately.
    pub update_interval: Duration,
}

impl Default for LockOptions {
    fn default() -> Self {
        Self {
            update_interval: DEFAULT_UPDATE_INTERVAL,
        }
    }
}

#[derive(Debug)]
pub enum Error {
    /// The cluster could not serve a read or a write.
    Cluster(raft5_client::Error),
    /// [`try_acquire`] only: the key is held.
    ///
    /// Liveness is deliberately *not* asserted. Proving a holder dead
    /// means watching its value sit unchanged for longer than it promised
    /// to go quiet, and `try_acquire` does not wait at all. `retry_after`
    /// is how long an unrefreshed lock would take [`acquire`] to reclaim.
    Held { by: String, retry_after: Duration },
    /// The key exists but does not hold a lock value this build
    /// understands. It is never overwritten -- see the crate doc.
    Malformed { key: String, detail: String },
    /// An `update_interval` outside
    /// [`MIN_UPDATE_INTERVAL`]..=[`MAX_UPDATE_INTERVAL`].
    InvalidOptions(&'static str),
    /// Rejected before anything was sent.
    InvalidArgument(&'static str),
}

impl From<raft5_client::Error> for Error {
    fn from(e: raft5_client::Error) -> Self {
        Self::Cluster(e)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cluster(e) => write!(f, "the cluster could not serve the lock: {e}"),
            Self::Held { by, retry_after } => write!(
                f,
                "the lock is held by {by:?}; an abandoned one would be claimable in {}ms",
                retry_after.as_millis()
            ),
            Self::Malformed { key, detail } => write!(
                f,
                "{key:?} exists but is not a lock this build can read ({detail}); \
                 refusing to overwrite it"
            ),
            Self::InvalidOptions(why) | Self::InvalidArgument(why) => write!(f, "{why}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Cluster(e) => Some(e),
            _ => None,
        }
    }
}
