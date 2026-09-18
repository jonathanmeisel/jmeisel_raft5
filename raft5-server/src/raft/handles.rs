//! The handles the outside world holds: this module's entire public API
//! surface, in one place.
//!
//! Everything else in `raft` is reachable only through one of these.
//! `RoleHandle` (and the per-role handles it carries) is what the gRPC
//! layer reads to decide what this node can currently do; `RaftPeerHandle`
//! and `ClientHandle` are how requests get *in*. Their fields are
//! `pub(super)` rather than private because `raft` itself and the role
//! modules construct them -- nothing outside `raft` can touch them.

use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{mpsc, oneshot, watch};

use crate::pb::raft5::cluster::ClusterConfig;
use crate::pb::raft5::raft::{
    AppendEntriesRequest, AppendEntriesResponse, GetLeaseRequest, GetLeaseResponse,
    InstallSnapshotResponse, RequestVoteRequest, RequestVoteResponse, SnapshotHeader,
};
use crate::store::Store;

use super::{ConfigChange, Event, LEASE_DURATION, RaftError};

/// Who to point a client at, given a role handle. `None` unless this node
/// currently knows of a leader -- a candidate, or a follower that hasn't
/// heard from one yet, has nowhere to send them.
pub fn leader_hint_of(role: &RoleHandle) -> Option<String> {
    match role {
        RoleHandle::Follower(f) => f.leader_hint.clone(),
        _ => None,
    }
}

/// A cheap, cloneable snapshot of "what can the outside world do right
/// now", published by whichever role is currently running and refreshed on
/// every transition. This -- not the role structs themselves -- is what
/// `KvService` would hold to implement `Write`/`Read`.
#[derive(Clone)]
pub enum RoleHandle {
    Leader(LeaderHandle),
    Follower(FollowerHandle),
    Candidate,
    /// This node's log holds no cluster configuration at all, so it isn't
    /// part of any cluster yet (Raft §4.4). Internally it is just a
    /// follower with no vote: it answers every RPC and will happily be
    /// replicated to -- which is exactly how a server *added* to an
    /// existing cluster leaves this state, since the `Config` entry
    /// naming it arrives as an ordinary log entry. The other way out is
    /// `Bootstrap`, which makes it a single-node cluster.
    Uninitialized,
    /// The value the `watch` channel holds before the driver has published
    /// anything real. Never sent by any role -- exists so callers (and
    /// tests) waiting for a specific role can't be fooled by the channel's
    /// construction-time default.
    Starting,
    /// This node has shut down and will never publish another role.
    /// Published by `run` on its way out, for two reasons.
    ///
    /// The visible one: a gRPC handler holding a stale `RoleHandle` would
    /// otherwise still see `Leader` and try to serve a request against a
    /// state machine that no longer exists.
    ///
    /// The load-bearing one: `LeaderHandle` holds a `role_rx` subscribed
    /// to the very channel it is published on, so a published
    /// `RoleHandle::Leader` is a reference cycle -- the channel's value
    /// keeps a receiver alive, and that receiver keeps the channel (and
    /// its value) alive. Dropping every sender does not break it, so the
    /// whole handle -- including its `Arc<RaftLog>`, and with it the
    /// database lock -- would outlive the node forever. Overwriting the
    /// channel with a variant that holds nothing drops the cycle.
    Stopped,
}

#[derive(Clone)]
pub struct LeaderHandle {
    pub(super) store: Store,
    pub(super) term: u64,
    /// For `propose_config`, which -- unlike `append_log` -- can't just
    /// write to the log directly: the guards a membership change needs
    /// live on the event loop (see `leader::Leader::propose_config`).
    pub(super) event_tx: mpsc::Sender<Event>,
    /// How far `RaftLog::apply_committed` has actually brought `latest`
    /// up to date -- *not* `commit_tx` directly. `append_log` needs to
    /// know `latest` itself reflects the entry, not just that it's been
    /// replicated to a majority: `commit_tx` and this are two independent
    /// `watch` subscriptions off of `spawn_storage_maintenance_task`, and
    /// `watch` gives no ordering guarantee between separate subscribers
    /// waking up on the same update. Waiting on `commit_tx` directly here
    /// would let `append_log` return before the maintenance task has
    /// actually run `apply_committed` for this seq, so a caller that
    /// immediately reads afterward could see stale or missing data.
    pub(super) applied_rx: watch::Receiver<u64>,
    /// Used to detect a step-down while waiting in `append_log`, so a
    /// caller doesn't hang forever on an entry that will never commit
    /// (e.g. because it gets truncated by a new leader instead).
    ///
    /// Note that this is a receiver for the same channel this handle is
    /// itself published on, which makes a published `RoleHandle::Leader`
    /// self-referential -- see `RoleHandle::Stopped` for what that costs
    /// and why `run` publishes over it before the node goes away.
    pub(super) role_rx: watch::Receiver<RoleHandle>,
    /// Whether this leader currently holds a quorum of leases -- see the
    /// module doc. What a linearizable read checks before answering.
    pub(super) read_quorum_rx: watch::Receiver<bool>,
    /// This leader's own no-op entry (Raft §5.4.2), against which
    /// `applied_rx` says whether `latest` is complete -- see
    /// `await_caught_up`.
    pub(super) noop_seq: u64,
}

impl LeaderHandle {
    /// Append a new entry and wait until it's committed (replicated to a
    /// majority) *and* actually applied to `latest` before returning --
    /// see `applied_rx`'s doc on why waiting for commit alone isn't
    /// enough.
    pub async fn append_log(&self, key: &[u8], value: Option<&[u8]>) -> Result<u64, RaftError> {
        let term = self.term;
        let key = key.to_vec();
        let value = value.map(<[u8]>::to_vec);
        let seq = self
            .store
            .run(move |log| log.append(term, &key, value.as_deref()))
            .await
            .map_err(RaftError::Storage)?;

        self.await_applied(seq).await
    }

    /// Propose a single-server membership change and wait until it
    /// commits. Returns the seq of the `Config` entry.
    ///
    /// The change itself takes effect on *append* (§4.1), so this
    /// returning is not what makes it real -- it is what makes it
    /// durable. The wait matters for `RemoveServer` especially: this
    /// leader may be removing *itself*, and it has to stay leader long
    /// enough to get the entry committed before stepping down (§4.2.2).
    pub async fn propose_config(&self, change: ConfigChange) -> Result<u64, RaftError> {
        let (respond_to, rx) = oneshot::channel();
        self.event_tx
            .send(Event::ProposeConfig { change, respond_to })
            .await
            .map_err(|_| RaftError::Disconnected)?;
        let seq = rx.await.map_err(|_| RaftError::Disconnected)??;
        self.await_applied(seq).await
    }

    /// Blocks until `seq` has committed *and* been applied, or until this
    /// node stops being leader at this term -- in which case the entry
    /// may well be truncated rather than committed, so waiting on it
    /// forever would hang.
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

    /// Whether it's currently safe for this leader to serve a
    /// linearizable read directly from local state, without going
    /// through the log -- i.e. whether it holds a quorum of leases. See
    /// the module doc.
    pub fn has_read_quorum(&self) -> bool {
        *self.read_quorum_rx.borrow()
    }

    /// Waits until `latest` reflects every write that any previous
    /// leader committed.
    ///
    /// A leader inherits a log tail it has not applied yet, and
    /// `latest` only ever reflects *applied* entries (see
    /// `RaftLog::apply_committed`). Winning an election says nothing
    /// about having caught up on it: this leader may be moments away
    /// from applying a write it cannot yet see. Its own no-op sits
    /// after everything it inherited, so once that has been applied, so
    /// has everything before it.
    ///
    /// Anything that answers from `latest` and claims to be current --
    /// a linearizable read, the precondition on a conditional write --
    /// has to wait here first, or it will answer from a gap it doesn't
    /// know it has. `has_read_quorum` asks a different question (is
    /// this node still the leader *now*) and neither implies the other.
    ///
    /// Waiting rather than refusing because this resolves on its own,
    /// usually within a heartbeat of the election: a caller told "not
    /// yet" would only sleep and ask again. It still returns
    /// `NotLeader` rather than waiting forever if this node is deposed
    /// before it gets there.
    pub async fn await_caught_up(&self) -> Result<(), RaftError> {
        self.await_applied(self.noop_seq).await.map(|_| ())
    }
}

#[derive(Clone)]
pub struct FollowerHandle {
    pub leader_hint: Option<String>,
}

/// The entry point for inbound peer RPCs. Cheap to clone, valid for the
/// life of the node regardless of which role is currently running --
/// `RaftPeerService` holds one of these rather than reaching into
/// `RoleHandle`, since every role has to be able to answer these calls, not
/// just the leader.
#[derive(Clone)]
pub struct RaftPeerHandle {
    pub(super) event_tx: mpsc::Sender<Event>,
    /// Read directly (never through the event loop) by `get_lease` below,
    /// so answering a lease probe can never get stuck behind whatever the
    /// event loop is currently doing -- e.g. a slow `AppendEntries`
    /// mid-disk-write. See the module doc.
    pub(super) role_rx: watch::Receiver<RoleHandle>,
    pub(super) term_rx: watch::Receiver<u64>,
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

    /// Unlike `get_lease`, this goes through the event loop like
    /// `append_entries`/`request_vote`: it needs the full term check
    /// *and* actually has real storage work to do (`RaftLog::install_snapshot`),
    /// so there's no way to make it independent of whichever role is
    /// currently running the way the lease fast path is.
    pub async fn install_snapshot(
        &self,
        header: SnapshotHeader,
        data: Vec<u8>,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        let (respond_to, rx) = oneshot::channel();
        self.event_tx
            .send(Event::InstallSnapshot {
                header,
                data,
                respond_to,
            })
            .await
            .map_err(|_| RaftError::Disconnected)?;
        rx.await.map_err(|_| RaftError::Disconnected)
    }

    /// Answers a lease probe directly against shared, watch-based state --
    /// deliberately *not* through `Event`/the sequential event loop, since
    /// that's exactly what would let a slow, unrelated `AppendEntries`
    /// stall a probe that's supposed to be fast. Infallible (nothing here
    /// can actually fail the way a channel send could) and needs no
    /// `.await`, which is itself part of the point: the type signature
    /// makes "this can never block on the event loop" visible.
    pub fn get_lease(&self, req: GetLeaseRequest) -> GetLeaseResponse {
        if !matches!(&*self.role_rx.borrow(), RoleHandle::Follower(_)) {
            // Only a follower "recognizes" a leader via lease -- anyone
            // else (including another leader) has nothing to grant.
            return GetLeaseResponse {
                lease_granted: false,
            };
        }
        let granted = req.term == *self.term_rx.borrow();
        if granted {
            self.lease_expires_at_tx
                .send_replace(Some(Instant::now() + LEASE_DURATION));
        }
        GetLeaseResponse {
            lease_granted: granted,
        }
    }
}

/// What the client-facing service (`grpc::KvService`) talks to: the
/// current role, plus the one client request that isn't routed by role
/// (`Bootstrap`, which by definition targets a node that has no leader).
///
/// Deliberately separate from `RaftPeerHandle`: the two are served on
/// different ports precisely because they have different exposure, and
/// nothing a client sends should be able to reach the peer protocol.
#[derive(Clone)]
pub struct ClientHandle {
    pub(super) event_tx: mpsc::Sender<Event>,
    pub(super) role_rx: watch::Receiver<RoleHandle>,
}

impl ClientHandle {
    /// What this node can do right now -- who to point a client at, or
    /// whether it needs bootstrapping at all.
    pub fn role(&self) -> RoleHandle {
        self.role_rx.borrow().clone()
    }

    /// Form a single-node cluster around this node (Raft §4.4). Refused
    /// with `AlreadyInitialized` by any node that is already part of one;
    /// see `follower::Follower::bootstrap`.
    pub async fn bootstrap(&self, config: ClusterConfig) -> Result<(), RaftError> {
        let (respond_to, rx) = oneshot::channel();
        self.event_tx
            .send(Event::Bootstrap { config, respond_to })
            .await
            .map_err(|_| RaftError::Disconnected)?;
        rx.await.map_err(|_| RaftError::Disconnected)?
    }
}

#[cfg(test)]
mod tests;
