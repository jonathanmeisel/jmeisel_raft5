//! Structured ownership of background tasks.
//!
//! Nothing Raft-specific lives here: `Task` is simply a spawned future
//! plus the token that stops it, with `stop` as the only way to get rid
//! of one. See the parent module's "Cancellation" section for why this
//! codebase refuses to hold a bare `JoinHandle`.

use std::future::Future;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::Event;

/// A spawned background task together with the token that stops it.
///
/// Every background task in this module is owned by one of these, and the
/// only way to get rid of one is `stop`, which signals *and then waits*.
/// That is the whole point: a `JoinHandle::abort` (or, worse, a handle
/// simply dropped) leaves the task's last poll wherever it happened to
/// be, so whatever it was holding -- an `Arc<RaftLog>`, and with it the
/// database lock -- can outlive the node that owned it. `stop` guarantees
/// the task has actually returned before the owner moves on.
pub(crate) struct Task {
    cancel: CancellationToken,
    handle: JoinHandle<()>,
}

impl Task {
    /// Spawns `body`, handing it a fresh child of `parent`. The child
    /// relationship is what makes cancellation propagate downward without
    /// any of these tasks knowing about each other: cancelling the node's
    /// root token stops every task at once, while `stop` on an individual
    /// `Task` (a leader dropping one peer's replication, say) cancels only
    /// that one.
    pub(super) fn spawn<F>(
        parent: &CancellationToken,
        body: impl FnOnce(CancellationToken) -> F,
    ) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let cancel = parent.child_token();
        let handle = tokio::spawn(body(cancel.clone()));
        Self { cancel, handle }
    }

    /// Signals the task to stop and waits until it actually has.
    ///
    /// A `JoinError` here is either a panic (already reported by the
    /// default panic hook, and not something this layer can do anything
    /// about) or an abort that nothing in this module performs -- either
    /// way the task is over, which is all the caller is waiting for.
    pub(super) async fn stop(self) {
        self.signal();
        let _ = self.handle.await;
    }

    /// Signals without waiting. Only useful as the first half of
    /// `stop_all`.
    pub(super) fn signal(&self) {
        self.cancel.cancel();
    }

    /// The runtime's id for the spawned task -- for tests asserting that
    /// a task was (or was not) restarted, since two `Task`s are otherwise
    /// indistinguishable from the outside.
    #[cfg(test)]
    pub(super) fn id(&self) -> tokio::task::Id {
        self.handle.id()
    }

    /// Stops a whole group of tasks, signalling every one of them before
    /// joining any -- so they wind down concurrently instead of serially,
    /// each waiting out the one ahead of it.
    pub(super) async fn stop_all(tasks: impl IntoIterator<Item = Task>) {
        let tasks: Vec<Task> = tasks.into_iter().collect();
        for task in &tasks {
            task.signal();
        }
        for task in tasks {
            task.stop().await;
        }
    }
}

/// Awaits `fut` unless `cancel` fires first; `None` means cancelled.
///
/// Deliberately *not* used around a `store::Store` call. Those run on the
/// blocking pool, and dropping the future awaiting one doesn't stop it --
/// it keeps running on its own worker thread, still holding whatever it
/// captured, typically an `Arc<RaftLog>` and with it the database lock.
/// Storage work is short and bounded, so every such await is left to
/// complete; only genuinely open-ended waits (a peer RPC, a heartbeat
/// sleep, a channel send into an event loop that may already have stopped
/// draining) are wrapped in this.
pub(super) async fn until_cancelled<T>(
    cancel: &CancellationToken,
    fut: impl Future<Output = T>,
) -> Option<T> {
    tokio::select! {
        biased;
        () = cancel.cancelled() => None,
        value = fut => Some(value),
    }
}

/// `until_cancelled` for the common "push an event and keep going unless
/// there's no one left to push to" step. `true` means the caller should
/// carry on; `false` means it should return, either because it was
/// cancelled or because the event loop is gone.
pub(super) async fn send_event(
    cancel: &CancellationToken,
    tx: &mpsc::Sender<Event>,
    event: Event,
) -> bool {
    matches!(until_cancelled(cancel, tx.send(event)).await, Some(Ok(())))
}
