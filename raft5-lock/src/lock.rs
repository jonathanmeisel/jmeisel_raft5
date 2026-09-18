//! A held lock, and the task that keeps it held.

use std::future::Future;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use raft5_client::{Client, Update};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::Error;
use crate::timing::{attempt_timeout, stepdown};
use crate::value::Value;

/// Why a lock stopped being held.
///
/// Cheap to clone and to compare, which is the point: [`Lock::lost`] may
/// be awaited by any number of tasks and each of them may want to know
/// why. `raft5_client::Error` is not `Clone`, and the only thing a caller
/// could do with a refresh failure is log it, so a cluster error is
/// rendered to a string at this boundary rather than propagated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LossReason {
    /// No refresh landed within the deadline. This holder gave the lock
    /// up on its own; it does not follow that anyone has taken it.
    Expired { detail: String },
    /// The key carries another epoch: someone completed a takeover.
    Stolen { by: String },
    /// The key no longer exists. We did not delete it, so we do not hold
    /// it -- usually an operator with the client shell, or a successor
    /// that took the lock and then released it.
    Vanished,
    /// [`Lock::release`], or `Drop`.
    Released,
}

/// The state a [`Lock`] and its refresher share.
pub(crate) struct LockInner {
    pub(crate) client: Client,
    pub(crate) name: String,
    pub(crate) value: Value,
    fence: AtomicU64,
    lost: CancellationToken,
    reason: OnceLock<LossReason>,
}

impl LockInner {
    /// Publishes the loss. The reason is set *before* the token is
    /// cancelled so that an observer woken by `lost()` always finds one;
    /// `OnceLock` keeps the first, which is what makes this idempotent
    /// and lets `release` call it without checking whether the refresher
    /// beat it to the punch.
    fn lose(&self, reason: LossReason) {
        let _ = self.reason.set(reason);
        self.lost.cancel();
    }

    /// Deletes the key, but only while it still carries our epoch.
    ///
    /// The condition is the whole safety property. A holder that was
    /// taken over 300ms ago still has a `Lock` in hand and will still run
    /// this on drop; an unconditional delete would quietly destroy its
    /// successor's lock while the successor believed it held one.
    async fn delete_if_ours(&self) -> Result<(), Error> {
        let epoch = self.value.epoch;
        self.client
            .update(self.name.as_bytes(), |current| match current {
                Some(bytes) => match Value::decode(bytes) {
                    Ok(v) if v.epoch == epoch => Update::Write(None),
                    _ => Update::Noop,
                },
                None => Update::Noop,
            })
            .await?;
        Ok(())
    }
}

/// A held lock.
///
/// `Send + Sync`, and deliberately not `Clone`: holding a lock is an
/// exclusive obligation, and two handles to one lock would race to delete
/// one key.
///
/// Dropping it stops the refreshing and makes a best-effort attempt to
/// delete the key -- see [`Lock::release`] for the version whose outcome
/// you can observe.
pub struct Lock {
    inner: Arc<LockInner>,
    /// Taken by whichever of `release`, `abandon` or `drop` runs.
    task: Option<JoinHandle<()>>,
}

impl Lock {
    /// Takes ownership of a key that has just been written, and starts
    /// refreshing it.
    ///
    /// `anchor` is the instant *before* the acquiring write was issued --
    /// see `timing::stepdown` for why the conservative end of that write
    /// is the one the lease hangs from.
    pub(crate) fn start(
        client: Client,
        name: String,
        value: Value,
        fence: u64,
        anchor: Instant,
    ) -> Self {
        let inner = Arc::new(LockInner {
            client,
            name,
            value,
            fence: AtomicU64::new(fence),
            lost: CancellationToken::new(),
            reason: OnceLock::new(),
        });
        let task = tokio::spawn(refresh(Arc::clone(&inner), anchor));
        Self {
            inner,
            task: Some(task),
        }
    }

    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub fn owner(&self) -> &str {
        &self.inner.value.user_id
    }

    /// This acquisition's epoch, as hex. For logs and for telling two
    /// tenures of the same lock apart.
    pub fn epoch(&self) -> String {
        self.inner.value.epoch.to_hex()
    }

    pub fn update_interval(&self) -> Duration {
        self.inner.value.update
    }

    /// The raft sequence number of this lock's most recent successful
    /// write.
    ///
    /// Raft sequence numbers are assigned from one cluster-wide counter
    /// and are never reused, so this is strictly increasing across every
    /// acquisition of this lock -- and across every other key in the
    /// store, which does not matter but is worth knowing. Pass it with
    /// each protected write and have the resource reject anything below
    /// the highest token it has seen.
    ///
    /// **That, not [`is_held`], is what makes the critical section safe.**
    /// See the crate doc.
    ///
    /// [`is_held`]: Lock::is_held
    pub fn fence(&self) -> u64 {
        self.inner.fence.load(Ordering::Relaxed)
    }

    /// Whether this holder has yet seen anything that ended its claim.
    ///
    /// A local belief about the past. `true` is never a promise about the
    /// next instant -- see the crate doc on why that distinction is the
    /// whole story.
    pub fn is_held(&self) -> bool {
        !self.inner.lost.is_cancelled()
    }

    /// Resolves once the lock is no longer held.
    ///
    /// Resolves immediately if it is already lost, and may be awaited any
    /// number of times from any number of tasks, which is what makes it
    /// usable as a `tokio::select!` arm guarding work that should stop
    /// the moment the lease goes.
    pub fn lost(&self) -> impl Future<Output = ()> + Send + '_ {
        self.inner.lost.cancelled()
    }

    /// Why the lock was lost, or `None` while it is still held.
    pub fn loss_reason(&self) -> Option<LossReason> {
        self.inner.reason.get().cloned()
    }

    /// Stops refreshing, waits for any refresh already in flight, and
    /// deletes the key if it still carries this lock's epoch.
    ///
    /// An error means the key was left behind to expire instead; the lock
    /// is released locally either way. This is the only way to *know* the
    /// key is gone -- `Drop` tries, but cannot report.
    pub async fn release(mut self) -> Result<(), Error> {
        self.inner.lose(LossReason::Released);
        // Waiting for the task, rather than aborting it, is what makes
        // this race-free: the refresher only checks for cancellation at
        // its sleep point, so a completed join proves no refresh is in
        // flight and nothing can resurrect the key after our delete. An
        // aborted mid-write would leave its RPC to commit behind us and
        // strand a value nobody refreshes until it expired.
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }

        // `Lock` has a `Drop` impl, so the fields have to come out before
        // `self` is disposed of; forgetting the husk then stops `Drop`
        // redoing work that is about to be done properly.
        let inner = Arc::clone(&self.inner);
        std::mem::forget(self);
        inner.delete_if_ours().await
    }

    /// Stops refreshing and leaves the key behind to expire, exactly as a
    /// crashed process would.
    ///
    /// For tests that need a holder which has stopped without releasing.
    /// `std::mem::forget` is not a substitute: it strands the refresher,
    /// which keeps writing, and so simulates a *healthy* holder.
    #[doc(hidden)]
    pub fn abandon(mut self) {
        self.inner.lose(LossReason::Released);
        // Dropping the handle detaches the task; it exits at its next
        // sleep point, which it reaches within one interval.
        self.task.take();
        std::mem::forget(self);
    }
}

impl std::fmt::Debug for Lock {
    /// Hand-written because `Client` is not `Debug`, and because the
    /// interesting state is the claim rather than the machinery: what
    /// lock, whose, which tenure, and whether we still think it is ours.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lock")
            .field("name", &self.inner.name)
            .field("owner", &self.inner.value.user_id)
            .field("epoch", &self.epoch())
            .field("fence", &self.fence())
            .field("held", &self.is_held())
            .field("loss_reason", &self.loss_reason())
            .finish()
    }
}

impl Drop for Lock {
    fn drop(&mut self) {
        self.inner.lose(LossReason::Released);

        // `Drop` cannot await, and must not block: `block_on` or
        // `block_in_place` here would deadlock a current-thread runtime
        // outright. So the delete is handed to the runtime and this
        // returns without it having happened.
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            log::debug!(
                "lock {:?} dropped outside a runtime; leaving the key to expire",
                self.inner.name
            );
            return;
        };

        let inner = Arc::clone(&self.inner);
        let task = self.task.take();
        handle.spawn(async move {
            if let Some(task) = task {
                let _ = task.await;
            }
            if let Err(e) = inner.delete_if_ours().await {
                log::debug!("could not delete lock {:?} on drop: {e}", inner.name);
            }
        });
    }
}

/// What one refresh attempt found.
enum Found {
    /// The key was ours and has been rewritten one counter on.
    Refreshed,
    /// The key carries someone else's epoch, or something unreadable.
    Stolen(String),
    /// The key is gone.
    Vanished,
}

/// Rewrites the key every interval for as long as it is ours.
///
/// Holds an `Arc<LockInner>`, but never outlives its [`Lock`]: every path
/// that disposes of one cancels the token first, and this loop checks it
/// on every pass.
async fn refresh(inner: Arc<LockInner>, mut anchor: Instant) {
    let update = inner.value.update;
    let epoch = inner.value.epoch;
    let key = inner.name.as_bytes().to_vec();
    let mut next_tick = anchor + update;

    loop {
        // Cancellation is observed here and only here. `release` depends
        // on it: a refresh that has started always runs to completion, so
        // joining this task proves nothing is in flight.
        tokio::select! {
            _ = inner.lost.cancelled() => return,
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(next_tick)) => {}
        }

        let deadline = anchor + stepdown(update);
        let now = Instant::now();
        if now >= deadline {
            inner.lose(LossReason::Expired {
                detail: format!(
                    "no refresh landed within {}ms",
                    stepdown(update).as_millis()
                ),
            });
            return;
        }

        // Clamped to the deadline, not just to the attempt budget: an
        // attempt started shortly before the deadline would otherwise run
        // past it and the holder would notice it had expired late. With
        // the clamp the lock is given up at exactly the deadline,
        // whatever the client is doing internally -- which is also how
        // the client's own 10s deadline is kept out of the safety chain.
        let budget = attempt_timeout(update).min(deadline - now);
        // The conservative anchor: before the attempt, never after it.
        let started = now;
        next_tick = started + update;

        let mut found = None;
        let attempt = tokio::time::timeout(
            budget,
            inner.client.update(&key, |current| match current {
                None => {
                    found = Some(Found::Vanished);
                    Update::Noop
                }
                Some(bytes) => match Value::decode(bytes) {
                    // Refreshed from what the store holds rather than
                    // from a local counter, so a refresh whose reply was
                    // lost resumes from what actually landed.
                    Ok(v) if v.epoch == epoch => {
                        found = Some(Found::Refreshed);
                        Update::Write(Some(v.next().encode()))
                    }
                    Ok(v) => {
                        found = Some(Found::Stolen(v.user_id));
                        Update::Noop
                    }
                    Err(_) => {
                        found = Some(Found::Stolen("<unreadable>".to_string()));
                        Update::Noop
                    }
                },
            }),
        )
        .await;

        match (attempt, found) {
            (Ok(Ok(seq)), Some(Found::Refreshed)) => {
                anchor = started;
                inner.fence.store(seq, Ordering::Relaxed);
            }
            // Someone completed a takeover. The lock is gone and this
            // task must not try to win it back: a lock that cannot be
            // lost is exactly the thing that would make the lease unsafe.
            (_, Some(Found::Stolen(by))) => {
                log::warn!("lock {:?} was taken over by {by:?}", inner.name);
                inner.lose(LossReason::Stolen { by });
                return;
            }
            (_, Some(Found::Vanished)) => {
                log::warn!("lock {:?} vanished from under its holder", inner.name);
                inner.lose(LossReason::Vanished);
                return;
            }
            // Not a loss, only a failure to prove otherwise: retried on
            // the next tick until the deadline turns it into `Expired`. A
            // lock quietly riding out a leader election is worth a line.
            (Ok(Err(e)), _) => log::warn!("could not refresh lock {:?}: {e}", inner.name),
            (Err(_elapsed), _) => log::warn!(
                "refreshing lock {:?} did not finish within {}ms",
                inner.name,
                budget.as_millis()
            ),
            (Ok(Ok(_)), None) => unreachable!("the update closure always records what it found"),
        }
    }
}
