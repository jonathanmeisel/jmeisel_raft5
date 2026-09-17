//! The async face of `storage`.
//!
//! `storage::RaftLog` is deliberately synchronous -- fjall's API is, and
//! keeping the storage layer free of an executor is what lets it be
//! reasoned about and tested without one. `Store` is the other half of
//! that bargain: the single place where a synchronous storage call is put
//! on the blocking pool, so that no async caller anywhere in the crate
//! writes `spawn_blocking` itself.
//!
//! That matters for more than tidiness. Blocking an executor thread on
//! disk I/O -- even briefly -- stalls every other task scheduled on it,
//! including the heartbeats and lease probes that decide whether this node
//! is still considered alive. Having exactly one place that dispatches
//! storage work means that rule cannot be forgotten at a new call site.

use std::sync::Arc;

use crate::storage::{self, RaftLog};

/// An async handle to this node's log. Cheap to clone -- it is an `Arc`
/// and nothing else -- so every component that needs storage holds its
/// own rather than borrowing one.
#[derive(Clone)]
pub struct Store {
    log: Arc<RaftLog>,
}

impl Store {
    pub fn new(log: Arc<RaftLog>) -> Self {
        Self { log }
    }

    /// Runs `f` against the log on the blocking pool.
    ///
    /// Takes a closure rather than mirroring `RaftLog`'s methods one for
    /// one, and deliberately: several callers need a *composite*
    /// operation -- match-then-truncate-then-append for an inbound
    /// `AppendEntries`, or read-tail-then-build-batch for replication --
    /// whose steps must not be interleaved with another writer's. One
    /// closure is one blocking task, so composing inside `f` is what
    /// keeps that property; composing out here, from several `run` calls,
    /// would silently lose it.
    ///
    /// A panic in `f` propagates. `spawn_blocking` only fails to join if
    /// its closure panicked, so unwrapping that here -- rather than at
    /// each of the callers that used to -- costs nothing and says the
    /// policy once: a storage panic is fail-stop (see `raft`'s module
    /// doc on why continuing past one is worse than not).
    pub async fn run<T, F>(&self, f: F) -> T
    where
        F: FnOnce(&RaftLog) -> T + Send + 'static,
        T: Send + 'static,
    {
        let log = Arc::clone(&self.log);
        tokio::task::spawn_blocking(move || f(&log))
            .await
            .expect("storage task panicked")
    }

    /// `run`, for the callers whose policy on a storage failure is to
    /// stop rather than carry on with state they could not read or write.
    /// `what` completes the sentence "failed to ...".
    ///
    /// This is most of the state machine: a role's event loop has no
    /// meaningful way to recover from, say, being unable to read the term
    /// at a log index -- proceeding on a guess is how a consensus
    /// implementation silently violates its own safety properties. The
    /// exceptions are the paths that genuinely can report the failure
    /// onward (a client write) or retry it (storage maintenance), and
    /// those use `run` and match on the `Result` themselves.
    pub async fn expect<T, F>(&self, what: &'static str, f: F) -> T
    where
        F: FnOnce(&RaftLog) -> storage::Result<T> + Send + 'static,
        T: Send + 'static,
    {
        self.run(f)
            .await
            .unwrap_or_else(|e| panic!("failed to {what}: {e}"))
    }
}
