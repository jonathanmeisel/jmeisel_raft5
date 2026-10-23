//! Structured ownership of background tasks.
//!
//! Nothing here is Raft-specific: a `Task` is a spawned future plus the
//! token that stops it, and `stop` is the only way to get rid of one. See
//! the parent module's "Cancellation" section for why this codebase never
//! holds a bare `JoinHandle`.

use std::future::Future;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::Event;

/// A spawned background task, with the token that stops it.
///
/// Every background task in this module is owned by one of these, and only
/// `stop` gets rid of one: it signals, then waits. That is the point. A
/// `JoinHandle::abort` (or a dropped handle) leaves the task wherever its
/// last poll stopped, so what it held (an `Arc<RaftLog>`, and with it the
/// database lock) can outlive the node. `stop` guarantees the task has
/// returned before the owner moves on.
pub(crate) struct Task {
    cancel: CancellationToken,
    handle: JoinHandle<()>,
}

impl Task {
    /// Spawn `body`, and give it a new child of `parent`. The child relation
    /// makes cancellation flow downward without these tasks knowing each other:
    /// cancelling the node's root token stops every task at once, while `stop`
    /// on one `Task` (a leader dropping one peer's replication, for example)
    /// cancels only that one.
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

    /// Signal the task to stop, and wait until it has.
    ///
    /// A `JoinError` here is a panic (the default panic hook already reported
    /// it, and this layer can do nothing about it) or an abort that nothing in
    /// this module does. Either way the task is over, which is all the caller
    /// waits for.
    pub(super) async fn stop(self) {
        self.signal();
        let _ = self.handle.await;
    }

    /// Signal without waiting. Only useful as the first half of `stop_all`.
    pub(super) fn signal(&self) {
        self.cancel.cancel();
    }

    /// The runtime's id for the spawned task, for tests that check whether a
    /// task was restarted: two `Task`s look the same from outside otherwise.
    #[cfg(test)]
    pub(super) fn id(&self) -> tokio::task::Id {
        self.handle.id()
    }

    /// Stop a group of tasks: signal all of them before joining any, so they
    /// wind down concurrently, not one after another.
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

/// Await `fut` unless `cancel` fires first; `None` means cancelled.
///
/// Never used around a `store::Store` call, on purpose. Those run on the
/// blocking pool, and dropping the future that awaits one does not stop
/// it: it keeps running on its worker thread, still holding what it
/// captured (usually an `Arc<RaftLog>`, and with it the database lock).
/// Storage work is short and bounded, so every such await runs to
/// completion. Only open-ended waits (a peer RPC, a heartbeat sleep, a
/// send into an event loop that may have stopped reading) use this.
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

/// `until_cancelled` for the common step "push an event and continue,
/// unless nobody is left to push to". `true` means the caller continues;
/// `false` means it returns, because it was cancelled or the event loop is
/// gone.
pub(super) async fn send_event(
    cancel: &CancellationToken,
    tx: &mpsc::Sender<Event>,
    event: Event,
) -> bool {
    matches!(until_cancelled(cancel, tx.send(event)).await, Some(Ok(())))
}
