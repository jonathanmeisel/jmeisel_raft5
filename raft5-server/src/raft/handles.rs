//! The handles that code outside `raft` holds: this module is the whole
//! public API surface of `raft`.
//!
//! Everything else in `raft` is reachable only through these.
//! `RoleHandle` (and the per-role handles in it) is what the gRPC layer
//! reads to decide what this node can do now. `RaftPeerHandle` and
//! `ClientHandle` are how requests get in. Their fields are `pub(super)`,
//! not private, because `raft` and the role modules build them; nothing
//! outside `raft` can touch them.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot, watch};
use tracing::instrument;

use crate::pb::raft5::cluster::ClusterConfig;
use crate::pb::raft5::raft::{
    AppendEntriesRequest, AppendEntriesResponse, GetLeaseRequest, GetLeaseResponse,
    InstallSnapshotResponse, RequestVoteRequest, RequestVoteResponse, SnapshotHeader,
};
use crate::storage::Mutation;
use crate::store::Store;

use super::{ConfigChange, Event, RaftError, Timing};

/// Why an append did not finish, split by the one fact a caller cannot do
/// without: whether the entry reached the log.
///
/// A transaction's commit needs this. Replaying a read-modify-write that
/// already applied applies it twice, so "nothing was written" and "I can no
/// longer tell what happened to it" cannot be one failure. A transaction is
/// the only way to write, so every real append returns through here.
#[derive(Debug)]
pub enum AppendError {
    /// Nothing was appended. Safe to retry, here or on the new leader.
    NotAppended(RaftError),
    /// The entry is in this node's log at `seq`, and this node can no longer
    /// say what happened to it. The next leader may still replicate and commit
    /// it, or truncate it, and nothing here can tell which. **Never retry.**
    InDoubt { seq: u64, source: RaftError },
}

impl fmt::Display for AppendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AppendError::NotAppended(e) => write!(f, "{e:?}"),
            AppendError::InDoubt { seq, source } => write!(
                f,
                "the entry at seq {seq} was appended, but this node stopped leading \
                 before it could confirm what became of it: {source:?}"
            ),
        }
    }
}

impl From<AppendError> for RaftError {
    /// Drop the distinction, for callers that truly have none: see the doc of
    /// `AppendError`.
    fn from(e: AppendError) -> Self {
        match e {
            AppendError::NotAppended(e) | AppendError::InDoubt { source: e, .. } => e,
        }
    }
}

/// Who to point a client at, given a role handle. `None` unless this node
/// knows a leader: a candidate, or a follower that has not heard from one
/// yet, has nowhere to send them.
pub fn leader_hint_of(role: &RoleHandle) -> Option<String> {
    match role {
        RoleHandle::Follower(f) => f.leader_hint.clone(),
        _ => None,
    }
}

/// A cheap, cloneable snapshot of "what can callers do now". The running
/// role publishes it and refreshes it on every transition. `KvService`
/// holds this, not the role structs.
#[derive(Clone)]
pub enum RoleHandle {
    Leader(LeaderHandle),
    Follower(FollowerHandle),
    Candidate,
    /// This node's log has no cluster configuration, so it is in no cluster
    /// yet (Raft §4.4). Inside, it is a follower with no vote: it answers every
    /// RPC and accepts replication. That is how a server added to an existing
    /// cluster leaves this state: the `Config` entry that names it arrives as a
    /// normal log entry. The other way out is `Bootstrap`, which makes it a
    /// single-node cluster.
    Uninitialized,
    /// The value the `watch` channel holds before the driver publishes anything
    /// real. No role ever sends it. It exists so callers (and tests) that wait
    /// for a specific role cannot be fooled by the channel's initial value.
    Starting,
    /// This node has shut down and will never publish another role. `run`
    /// publishes it on its way out, for two reasons.
    ///
    /// The visible one: a gRPC handler with a stale `RoleHandle` would still
    /// see `Leader` and try to serve a request on a state machine that no
    /// longer exists.
    ///
    /// The essential one: `LeaderHandle` holds a `role_rx` subscribed to the
    /// same channel it is published on, so a published `RoleHandle::Leader` is
    /// a reference cycle: the channel's value keeps a receiver alive, and that
    /// receiver keeps the channel (and its value) alive. Dropping every sender
    /// does not break it, so the whole handle, including its `Arc<RaftLog>`
    /// and with it the database lock, would outlive the node forever.
    /// Overwriting the channel with a variant that holds nothing breaks the
    /// cycle.
    Stopped,
}

impl fmt::Display for RoleHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RoleHandle::Leader(h) => write!(f, "Leader (term {})", h.term),
            RoleHandle::Follower(h) => match &h.leader_hint {
                Some(hint) => write!(f, "Follower (leader hint: {hint})"),
                None => write!(f, "Follower (no leader known)"),
            },
            RoleHandle::Candidate => write!(f, "Candidate"),
            RoleHandle::Uninitialized => write!(f, "Uninitialized"),
            RoleHandle::Starting => write!(f, "Starting"),
            RoleHandle::Stopped => write!(f, "Stopped"),
        }
    }
}

impl fmt::Debug for RoleHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[derive(Clone)]
pub struct LeaderHandle {
    pub(super) store: Store,
    pub(super) term: u64,
    /// For `propose_config`, which, unlike `append_writes`, cannot write to the
    /// log directly: the guards a membership change needs live on the event
    /// loop (see `leader::Leader::propose_config`).
    pub(super) event_tx: mpsc::Sender<Event>,
    /// How far `RaftLog::apply_committed` has brought `latest`, not
    /// `commit_tx`. `append_writes` needs to know that `latest` shows the
    /// entry, not only that a majority has it. `commit_tx` and this are two
    /// separate `watch` subscriptions, and `watch` gives no order between
    /// subscribers that wake on the same update. Waiting on `commit_tx` here
    /// would let `append_writes` return before the maintenance task ran
    /// `apply_committed` for this seq, so an immediate read could see stale or
    /// missing data.
    pub(super) applied_rx: watch::Receiver<u64>,
    /// Detects a step-down while `append_writes` waits, so a caller does not
    /// hang forever on an entry that will never commit (for example, one a new
    /// leader truncates).
    ///
    /// This is a receiver for the same channel this handle is published on,
    /// which makes a published `RoleHandle::Leader` refer to itself. See
    /// `RoleHandle::Stopped` for the cost, and why `run` overwrites it before
    /// the node goes away.
    pub(super) role_rx: watch::Receiver<RoleHandle>,
    /// Whether this leader holds a quorum of leases (see the module doc). A
    /// linearizable read checks this before it answers.
    pub(super) read_quorum_rx: watch::Receiver<bool>,
    /// This leader's own no-op entry (Raft §5.4.2). `applied_rx` passing it
    /// says that `latest` is complete: see `await_caught_up`.
    pub(super) noop_seq: u64,
    /// Told about every entry `append_writes` appends, because that path
    /// bypasses the event loop. See `Common::log_tx`.
    pub(super) log_tx: Arc<watch::Sender<u64>>,
}

impl LeaderHandle {
    /// Append a one-key entry, as `append_writes` does for many keys.
    ///
    /// Test-only: every real write is a transaction, so the service never
    /// appends one key at a time. It stays because most of what is worth
    /// testing about the append path (that it waits for applied, not only
    /// committed; that it gives up when the term changes) does not depend on
    /// the key count, and one key is simpler to express.
    ///
    /// Drops the `AppendError` distinction: a test that loses leadership
    /// mid-append has already failed, so in-doubt versus not-appended tells it
    /// nothing.
    #[cfg(test)]
    pub async fn append_log(&self, key: &[u8], value: Option<&[u8]>) -> Result<u64, RaftError> {
        self.append_writes(vec![Mutation::new(key.to_vec(), value.map(<[u8]>::to_vec))])
            .await
            .map_err(Into::into)
    }

    /// Append `writes` as one entry (one seq, committed and applied to
    /// `latest` together), and wait until it is applied. This is how a
    /// transaction commits, which means it is how anything is written.
    ///
    /// The result separates "nothing was written" from "something was written
    /// and I can no longer tell what happened to it". A transaction's caller
    /// must know which: the first is safe to replay, the second is not. See
    /// `AppendError`.
    ///
    /// Records two timings as span fields:
    /// - `log_append_ms`: the on-disk append.
    /// - `commit_wait_ms`: replication plus apply to `latest` (see
    ///   `await_applied`).
    ///
    /// Both are measured by hand, not by `tracing`'s busy/idle accounting,
    /// because both wait across a boundary (a `spawn_blocking` for the append,
    /// a `watch` channel for the wait) where the real work happens while this
    /// span looks idle. Busy/idle timing would show almost zero for exactly
    /// the durations these fields exist to show.
    #[instrument(skip(self, writes), fields(log_append_ms, commit_wait_ms))]
    pub async fn append_writes(&self, writes: Vec<Mutation>) -> Result<u64, AppendError> {
        let term = self.term;

        let append_start = Instant::now();
        let seq = self
            .store
            .run(move |log| log.append_writes(term, writes))
            .await
            .map_err(|e| AppendError::NotAppended(RaftError::Storage(e)))?;
        super::notify_appended(&self.log_tx, seq);
        let log_append_ms = append_start.elapsed().as_millis();
        tracing::Span::current().record("log_append_ms", log_append_ms);

        // From here the entry exists in the log, so every remaining failure is
        // in doubt, not a clean failure.
        let wait_start = Instant::now();
        let result = self.await_applied(seq).await;
        let commit_wait_ms = wait_start.elapsed().as_millis();
        tracing::Span::current().record("commit_wait_ms", commit_wait_ms);

        // `#[instrument]` alone prints nothing: `record` only attaches these to
        // the span, for consumers that read span fields (an OTLP exporter, for
        // example). An event makes them show up in this crate's text log, nested
        // under this span.
        tracing::info!(log_append_ms, commit_wait_ms, "entry committed");

        result.map_err(|source| AppendError::InDoubt { seq, source })
    }

    /// Propose a single-server membership change and wait until it commits.
    /// Returns the seq of the `Config` entry.
    ///
    /// The change takes effect on append (§4.1), so the return does not make
    /// it real; it makes it durable. The wait matters most for `RemoveServer`:
    /// this leader may be removing itself, and it must stay leader until the
    /// entry commits before it steps down (§4.2.2).
    pub async fn propose_config(&self, change: ConfigChange) -> Result<u64, RaftError> {
        let (respond_to, rx) = oneshot::channel();
        self.event_tx
            .send(Event::ProposeConfig { change, respond_to })
            .await
            .map_err(|_| RaftError::Disconnected)?;
        let seq = rx.await.map_err(|_| RaftError::Disconnected)??;
        self.await_applied(seq).await
    }

    /// Block until `seq` is committed and applied, or until this node stops
    /// leading in this term. In that case the entry may be truncated instead of
    /// committed, so waiting forever would hang.
    async fn await_applied(&self, seq: u64) -> Result<u64, RaftError> {
        let mut applied_rx = self.applied_rx.clone();
        let mut role_rx = self.role_rx.clone();
        loop {
            if *applied_rx.borrow() >= seq {
                return Ok(seq);
            }
            if !self.still_leader(&role_rx) {
                return Err(self.not_leader_error(&role_rx));
            }
            tokio::select! {
                r = applied_rx.changed() => { r.map_err(|_| RaftError::Disconnected)?; }
                r = role_rx.changed() => { r.map_err(|_| RaftError::Disconnected)?; }
            }
        }
    }

    fn still_leader(&self, role_rx: &watch::Receiver<RoleHandle>) -> bool {
        matches!(&*role_rx.borrow(), RoleHandle::Leader(h) if h.term == self.term)
    }

    fn not_leader_error(&self, role_rx: &watch::Receiver<RoleHandle>) -> RaftError {
        RaftError::NotLeader {
            leader_hint: leader_hint_of(&role_rx.borrow()),
        }
    }

    /// The term this node leads in.
    ///
    /// `grpc::KvService` reads it to notice a leader change: a transaction
    /// belongs to the term that created it, and nothing it holds may outlive
    /// that term.
    pub fn term(&self) -> u64 {
        self.term
    }

    /// Whether this leader may safely serve a linearizable read from local
    /// state, without the log: that is, whether it holds a quorum of leases.
    /// See the module doc.
    pub fn has_read_quorum(&self) -> bool {
        *self.read_quorum_rx.borrow()
    }

    /// What `await_caught_up` waits for, asked without waiting.
    ///
    /// For a caller that can use "not yet": `grpc::KvService::get_config`
    /// reports leadership to a client looking for the leader, and should
    /// answer either way instead of holding up the search. Code about to
    /// answer from `latest` wants `await_caught_up`: the wait usually ends
    /// within a heartbeat of the election, and a refusal would only make the
    /// caller sleep and ask again.
    pub fn is_caught_up(&self) -> bool {
        *self.applied_rx.borrow() >= self.noop_seq
    }

    /// Wait until `latest` shows every write that any earlier leader
    /// committed.
    ///
    /// A leader inherits a log tail it has not applied yet, and `latest` shows
    /// only applied entries (see `RaftLog::apply_committed`). Winning an
    /// election says nothing about having applied it: this leader may be
    /// moments from applying a write it cannot see yet. Its own no-op comes
    /// after everything it inherited, so once the no-op is applied, so is
    /// everything before it.
    ///
    /// Anything that answers from `latest` and claims to be current (a
    /// linearizable read, a transactional read) must wait here first, or it
    /// answers from a gap it does not know about. `has_read_quorum` asks a
    /// different question (is this node still leader now?), and neither
    /// implies the other.
    ///
    /// It waits instead of refusing because this resolves by itself, usually
    /// within a heartbeat of the election: a caller told "not yet" would only
    /// sleep and ask again. It still returns `NotLeader`, instead of waiting
    /// forever, if this node loses leadership first.
    pub async fn await_caught_up(&self) -> Result<(), RaftError> {
        self.await_applied(self.noop_seq).await.map(|_| ())
    }
}

#[derive(Clone)]
pub struct FollowerHandle {
    pub leader_hint: Option<String>,
}

/// The entry point for inbound peer RPCs. Cheap to clone, and valid for
/// the life of the node in every role. `RaftPeerService` holds one of these
/// instead of using `RoleHandle`, because every role must answer these
/// calls, not only the leader.
#[derive(Clone)]
pub struct RaftPeerHandle {
    pub(super) event_tx: mpsc::Sender<Event>,
    /// Read directly by `get_lease` below, never through the event loop, so a
    /// lease probe can never wait behind what the event loop is doing (for
    /// example, a slow `AppendEntries` in the middle of a disk write). See the
    /// module doc.
    pub(super) role_rx: watch::Receiver<RoleHandle>,
    pub(super) term_rx: watch::Receiver<u64>,
    /// Only for a lease request that names no duration: see `get_lease`.
    pub(super) timing_rx: watch::Receiver<Timing>,
    pub(super) lease_expires_at_tx: Arc<watch::Sender<Option<Instant>>>,
}

impl RaftPeerHandle {
    pub async fn append_entries(
        &self,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError> {
        let (respond_to, rx) = oneshot::channel();
        self.event_tx
            .send(Event::AppendEntries { req, respond_to })
            .await
            .map_err(|_| RaftError::Disconnected)?;
        rx.await.map_err(|_| RaftError::Disconnected)
    }

    pub async fn request_vote(
        &self,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, RaftError> {
        let (respond_to, rx) = oneshot::channel();
        self.event_tx
            .send(Event::RequestVote { req, respond_to })
            .await
            .map_err(|_| RaftError::Disconnected)?;
        rx.await.map_err(|_| RaftError::Disconnected)
    }

    /// Unlike `get_lease`, this goes through the event loop, like
    /// `append_entries` and `request_vote`: it needs the full term check and
    /// does real storage work (`RaftLog::install_snapshot`), so it cannot be
    /// independent of the running role the way the lease fast path is.
    pub async fn install_snapshot(
        &self,
        header: SnapshotHeader,
        body: crate::store::ChunkReader,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        let (respond_to, rx) = oneshot::channel();
        self.event_tx
            .send(Event::InstallSnapshot {
                header,
                body,
                respond_to,
            })
            .await
            .map_err(|_| RaftError::Disconnected)?;
        rx.await.map_err(|_| RaftError::Disconnected)
    }

    /// This node's current term, read from the watch channel, not through the
    /// event loop: the same fast path as `get_lease` below, for the same
    /// reason.
    ///
    /// Safe only for rejecting: the value is a lower bound on the real term
    /// (the event loop may be ahead of it, never behind). So anything this
    /// calls stale really is stale, but anything it accepts must still be
    /// checked on the event loop. `grpc::RaftPeerService` uses it to avoid
    /// reading a large `AppendEntries` body from a leader that is already
    /// superseded.
    pub fn current_term(&self) -> u64 {
        *self.term_rx.borrow()
    }

    /// Answer a lease probe from shared, watch-based state, not through
    /// `Event` and the sequential event loop: that loop is what would let a
    /// slow, unrelated `AppendEntries` stall a probe that must be fast.
    /// Infallible and synchronous on purpose: the signature shows that this
    /// can never block on the event loop.
    pub fn get_lease(&self, req: GetLeaseRequest) -> GetLeaseResponse {
        if !matches!(&*self.role_rx.borrow(), RoleHandle::Follower(_)) {
            // Only a follower recognizes a leader through a lease. Any other role
            // (including another leader) has nothing to grant.
            return GetLeaseResponse {
                lease_granted: false,
            };
        }
        let granted = req.term == *self.term_rx.borrow();
        if granted {
            // Exactly as long as the leader asked for, because that is how long it
            // counts this grant. Not this node's own configured lease, which can
            // briefly differ from the leader's while a heartbeat change replicates.
            // Clamped, so no request, however wrong, can block this node's elections
            // forever. Zero means the leader named no duration; use our own lease.
            let lease = if req.lease_millis == 0 {
                self.timing_rx.borrow().lease()
            } else {
                Duration::from_millis(req.lease_millis)
                    .clamp(Timing::min_lease(), Timing::max_lease())
            };
            self.lease_expires_at_tx
                .send_replace(Some(Instant::now() + lease));
        }
        GetLeaseResponse {
            lease_granted: granted,
        }
    }
}

/// What the client service (`grpc::KvService`) talks to: the current role,
/// plus the one client request that is not routed by role (`Bootstrap`,
/// which targets a node with no leader by definition).
///
/// Kept separate from `RaftPeerHandle` on purpose: the two are served on
/// different ports because their exposure differs, and nothing a client
/// sends must reach the peer protocol.
#[derive(Clone)]
pub struct ClientHandle {
    pub(super) event_tx: mpsc::Sender<Event>,
    pub(super) role_rx: watch::Receiver<RoleHandle>,
}

impl ClientHandle {
    /// What this node can do now: who to point a client at, or whether it needs
    /// a bootstrap.
    pub fn role(&self) -> RoleHandle {
        self.role_rx.borrow().clone()
    }

    /// Form a single-node cluster around this node (Raft §4.4). A node already
    /// in a cluster refuses with `AlreadyInitialized`; see
    /// `follower::Follower::bootstrap`.
    pub async fn bootstrap(&self, config: ClusterConfig) -> Result<(), RaftError> {
        self.send_bootstrap(config, None).await
    }

    /// `bootstrap`, after loading the backup that `backup` streams, so the new
    /// cluster holds its contents. See `storage::RaftLog::restore_backup`.
    pub async fn bootstrap_from_backup(
        &self,
        config: ClusterConfig,
        backup: crate::store::ChunkReader,
    ) -> Result<(), RaftError> {
        self.send_bootstrap(config, Some(backup)).await
    }

    async fn send_bootstrap(
        &self,
        config: ClusterConfig,
        restore: Option<crate::store::ChunkReader>,
    ) -> Result<(), RaftError> {
        let (respond_to, rx) = oneshot::channel();
        self.event_tx
            .send(Event::Bootstrap {
                config,
                restore,
                respond_to,
            })
            .await
            .map_err(|_| RaftError::Disconnected)?;
        rx.await.map_err(|_| RaftError::Disconnected)?
    }
}

#[cfg(test)]
mod tests;
