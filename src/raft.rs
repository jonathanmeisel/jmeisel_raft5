//! Raft consensus state machine.
//!
//! Only one role's event loop is ever running at a time. `run` drives a
//! `Common` through a sequence of roles: each role's `new` consumes
//! `Common`, starts whatever background tasks that role needs (replication
//! for the leader, vote-requesting for the candidate, an election timer for
//! the follower), publishes a `RoleHandle` so the outside world (the gRPC
//! layer) can see what's currently possible, then runs its own event loop
//! until it decides to change role. `shutdown` tears down that role's
//! background tasks and hands `Common` back, and the driver constructs the
//! next role from it. Because a role is fully torn down before the next one
//! starts, there's never more than one event loop reading `Common`'s state
//! at a time -- no locking needed around it.
//!
//! This file holds what's shared across every role -- `Common`, `Event`,
//! the handle types, `PeerConnections`, the driver -- and each role's own
//! implementation lives in its own submodule (`leader`, `candidate`,
//! `follower`), private to this module. A submodule sees everything here
//! (including private fields/methods) since it's a descendant of this
//! module, same as if the code hadn't moved; the split is purely about file
//! size, not encapsulation.
//!
//! `LeaderHandle::append_log` is the one piece of the public API that does
//! *not* go through the event loop: `storage::RaftLog::append` is already
//! safe to call concurrently, and waiting for a commit is just watching a
//! channel, so many concurrent client requests can sit in `append_log`
//! without serializing on (or blocking) the state machine's own loop.
//!
//! `RaftPeerHandle` is the mirror image for *inbound* RPCs: unlike
//! `LeaderHandle`, it isn't role-specific (every role has to be able to
//! answer AppendEntries/RequestVote), so it's handed out once, up front,
//! and stays valid across every role transition for the life of the node.
//! It does go through the event loop -- `RaftPeerService`'s gRPC handlers
//! call `append_entries`/`request_vote`, which push an `Event` carrying a
//! `oneshot` reply channel and await it, so whichever role is currently
//! running decides how to answer.
//!
//! `current_term`/`voted_for` are persisted to `storage`'s `meta` keyspace
//! (`RaftLog::term_state`/`persist_term_state`) through `Common::advance_term`
//! /`vote_for`, the only two places allowed to change them -- Raft's safety
//! property depends on a node never granting two votes in the same term,
//! even across a restart, so every change is durable before it's acted on.
//! A persist failure panics rather than continuing on an unpersisted vote
//! (see those methods).
//!
//! Every `storage::RaftLog` call made from async code goes through
//! `tokio::task::spawn_blocking` -- fjall's calls are synchronous, and
//! blocking an executor thread on disk I/O (even briefly) can stall
//! whatever else that thread was scheduled to run. Because of this,
//! `handle_event` is `async fn` on every role even though most events
//! don't touch storage, so the one that does (`AppendEntries`, via
//! `apply_append_entries`) can `.await` its `spawn_blocking` task like
//! anything else in the loop.
//!
//! `Common::peer_conns` caches one gRPC client per peer (keyed by
//! `PeerInfo::id`), shared across every role and every election so
//! `RequestVote`/`AppendEntries` calls don't pay a fresh connection setup
//! each time. A cached client is evicted after any failed call (`Channel`
//! doesn't auto-reconnect once its connection drops), so the next attempt
//! gets a fresh one instead of failing forever.
//!
//! Before a `Follower` (or a `Candidate` whose real election timed out)
//! commits to a real, term-incrementing candidacy, it first runs a
//! `PreCandidate` round: a non-binding pre-vote (Raft dissertation §9.6,
//! `RequestVoteRequest.dry_run`) that asks every peer "would you grant a
//! real vote at `current_term + 1`" without either side changing any
//! state. Only once a majority say yes does it become a real `Candidate`.
//! This keeps a node that's been partitioned (and so can't possibly win)
//! from inflating its term every 150-300ms and disrupting a healthy
//! cluster the moment it reconnects. `Common::election_attempt` counts
//! consecutive failed cycles (pre-vote or real) since this node last had
//! reason to believe a leader existed, and backs off the retry timeout
//! exponentially (`election_timeout_for_attempt`) so a persistently
//! undecided cluster doesn't hammer itself forever; it resets to 0 the
//! moment a fresh `Follower` starts (see `follower::Follower::new`).
//! `PreCandidate` and `Candidate` share nearly all of this machinery --
//! see `candidate::Campaign`.
//!
//! Every other timing constant derives from one `HEARTBEAT` (a future
//! user-configurable knob): `LEASE_DURATION` is `4 * HEARTBEAT`, and a
//! follower's own election timeout (the "am I sure there's no leader"
//! wait) is `LEASE_DURATION` plus an additional `fuzz(0, LEASE_DURATION)`
//! on top -- see `follower::spawn_lease_expiration_watcher` for why that's
//! two sequential phases (one deterministic, one randomized) rather than
//! one combined random draw. `PreCandidate`/`Candidate`'s own
//! campaign-retry timeout starts at that same `[LEASE_DURATION,
//! 2*LEASE_DURATION)` range for their first attempt and backs off
//! exponentially from there (`election_timeout_for_attempt`).
//!
//! Leader leasing (`GetLease`, deliberately a separate RPC from
//! `AppendEntries` since that one can be slow -- it does real log-append
//! work, a lease probe needs to stay fast) serves two purposes: a leader
//! that can't retain a quorum of leases can no longer prove it's safe to
//! serve a linearizable read locally (see `LeaderHandle::has_read_quorum`
//! -- not consumed by any read path yet, but the plumbing is there), and
//! a follower holding a live lease refuses *any* vote request (real or
//! pre-vote) for someone else, so a leader that a majority can still
//! reach can never be disrupted by a competing election. Losing lease
//! quorum does not, by itself, force a leader to step down -- only actual
//! contact revealing a higher term (a real `AppendEntries`/`RequestVote`)
//! does that; lease loss is a read-safety signal, not a health verdict on
//! its own, since replication (and therefore write-side commit safety)
//! has its own independent, majority-based quorum check.
//!
//! This is a skeleton: the leader-side §5.3 backoff optimization is a
//! simplification (see `leader::Leader::spawn_replication`), and there's no
//! cluster membership / snapshot-triggering yet. Log-matching (the follower
//! side), the candidate's voting (both sides), and leader replication
//! (majority commit + all-peers compaction watermark) are implemented.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{Mutex as AsyncMutex, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tonic::transport::Channel;

use crate::pb::raft5::common::LogEntry;
use crate::pb::raft5::raft::raft_peer_client::RaftPeerClient;
use crate::pb::raft5::raft::{
    AppendEntriesRequest, AppendEntriesResponse, GetLeaseRequest, GetLeaseResponse,
    InstallSnapshotResponse, RequestVoteRequest, RequestVoteResponse, SnapshotHeader,
};
use crate::storage::{self, RaftLog, StorageError};

mod candidate;
mod follower;
mod leader;

use candidate::{Candidate, PreCandidate};
use follower::Follower;
use leader::Leader;

/// The one base timing unit everything else derives from (see the module
/// doc). A `u64` millisecond count alongside the `Duration` form since a
/// couple of derived constants need to do integer arithmetic on it (in a
/// `const` context, before a `Duration` value could be decomposed back
/// out again). Will become a user-configurable, per-cluster setting;
/// hardcoded for now.
const HEARTBEAT_MS: u64 = 50;
const HEARTBEAT: Duration = Duration::from_millis(HEARTBEAT_MS);

/// How long a granted lease (see `RaftPeerHandle::get_lease`) stays valid,
/// on both sides: how long a follower keeps refusing other vote requests
/// after its last confirmed contact with the leader, and how long a
/// leader keeps counting a peer's last grant toward its own lease quorum.
const LEASE_DURATION_MS: u64 = HEARTBEAT_MS * 4;
const LEASE_DURATION: Duration = Duration::from_millis(LEASE_DURATION_MS);

/// Timeout for a cheap, no-disk-I/O peer RPC (`RequestVote`, `GetLease`) --
/// including connecting, if there's no cached client yet. Tight, since
/// these should always be fast; a slow reply is as good as no reply.
const LIGHTWEIGHT_RPC_TIMEOUT: Duration = Duration::from_millis(HEARTBEAT_MS * 2);

/// Timeout for `AppendEntries` specifically, which -- unlike the above --
/// does real log-append work on the follower and so can legitimately take
/// a while under load. Deliberately generous: retrying an already-slow
/// follower sooner doesn't help if the bottleneck is disk I/O, it just
/// adds more redundant work on top.
const APPEND_ENTRIES_RPC_TIMEOUT: Duration = Duration::from_secs(30);

/// Timeout for a full `InstallSnapshot` transfer -- more generous still
/// than `AppendEntries`: this moves the *entire* current state, not just a
/// batch of new entries, so it can take much longer on a large log or a
/// slow link.
const SNAPSHOT_RPC_TIMEOUT: Duration = Duration::from_secs(300);

/// How much of an exported snapshot's bytes go into each `SnapshotChunk`
/// sent over the wire. The whole snapshot is still buffered in memory on
/// both ends first (matching the receiving side's existing "get it
/// working" tradeoff -- see `grpc::RaftPeerService::install_snapshot`);
/// this only bounds the size of each individual gRPC message.
const SNAPSHOT_CHUNK_SIZE: usize = 1024 * 1024;

#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub id: String,
    pub address: String,
}

/// Inbound events a role's event loop reacts to. Requests carry their own
/// response channel so the loop can reply once it's decided how to handle
/// them -- every role has to be able to answer AppendEntries/RequestVote,
/// not just the leader.
pub enum Event {
    AppendEntries {
        req: AppendEntriesRequest,
        respond_to: oneshot::Sender<AppendEntriesResponse>,
    },
    AppendEntriesResponse {
        from: String,
        resp: AppendEntriesResponse,
    },
    RequestVote {
        req: RequestVoteRequest,
        respond_to: oneshot::Sender<RequestVoteResponse>,
    },
    RequestVoteResponse {
        from: String,
        resp: RequestVoteResponse,
    },
    /// Only ever the *leader's* own probe results coming back in --
    /// answering an inbound `GetLease` never goes through this event loop
    /// at all (see `RaftPeerHandle::get_lease`), specifically so it can
    /// never get stuck behind a slow in-progress `AppendEntries`.
    GetLeaseResponse {
        from: String,
        resp: GetLeaseResponse,
    },
    /// `data` is the full byte stream `RaftLog::export_snapshot` produced
    /// (magic/version/seq/term header included) -- `grpc::RaftPeerService`
    /// reassembles it from the wire but doesn't touch storage itself, so
    /// the term check and the actual install both happen here, same as
    /// every other inbound RPC.
    InstallSnapshot {
        header: SnapshotHeader,
        data: Vec<u8>,
        respond_to: oneshot::Sender<InstallSnapshotResponse>,
    },
    /// The leader's own snapshot-send task reporting back what happened,
    /// same pattern as `AppendEntriesResponse` -- `last_included_seq` is
    /// carried alongside the raw proto response (which only has `term`)
    /// so a successful install can advance `Leader::peer_match_index`
    /// the same way a successful `AppendEntries` does.
    InstallSnapshotResponse {
        from: String,
        resp: InstallSnapshotResponse,
        last_included_seq: u64,
    },
    ElectionTimeout,
    /// Self-notification, leader only: nudges the event loop to recompute
    /// `commit_seq`/`compact_seq` even when no peer response triggers it.
    /// Needed for a zero-peer cluster (nothing else would ever advance
    /// `commit_tx` after a client write there), and also keeps watermarks
    /// from stalling between replication rounds in a multi-peer one.
    LeaderTick,
}

/// Which role to construct next. A plain tag -- unlike the role structs
/// themselves, it carries no state, since only one role's state is ever
/// alive at a time.
pub(crate) enum Role {
    Follower,
    /// Non-binding pre-vote round -- see the module doc.
    PreCandidate,
    Candidate,
    Leader,
}

/// What a role's event loop decided after handling one event.
enum Transition {
    Continue,
    Become(Role),
}

/// A random value in `[0, max)`. Good enough for election-timeout jitter
/// without pulling in a `rand` dependency: `RandomState`'s whole purpose is
/// a fresh random seed per instance (it's what `HashMap` uses to resist
/// hash-flooding), so a fresh hasher's initial state is already
/// unpredictable without needing to write anything through it.
fn random_jitter_millis(max: u64) -> u64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    RandomState::new().build_hasher().finish() % max
}

/// How many consecutive failed election cycles it takes to reach this.
const MAX_ELECTION_BACKOFF_MS: u64 = 30_000;

/// The jitter window for the `attempt`-th consecutive *campaign* cycle
/// (`PreCandidate`/`Candidate` waiting on vote responses) since this node
/// last had reason to believe a leader existed -- see
/// `Common::election_attempt`. Attempt 0 is `[LEASE_DURATION,
/// 2*LEASE_DURATION)`, doubling per attempt from there, capped at
/// `MAX_ELECTION_BACKOFF_MS`, so a persistently undecided cluster (or a
/// node that genuinely can't reach anyone) backs off instead of retrying
/// forever at the same tight interval.
///
/// This is *not* what a `Follower` uses to decide it's time to stop
/// trusting its leader in the first place -- that's
/// `follower::spawn_lease_expiration_watcher`'s two-phase wait, which
/// happens to total the same `[LEASE_DURATION, 2*LEASE_DURATION)` range
/// for different reasons (a deterministic detection phase plus a
/// randomized settle phase, rather than a single random draw).
fn election_timeout_for_attempt(attempt: u32) -> Duration {
    // Cap the shift itself (not just its result) so this can't panic on a
    // pathologically large attempt count -- 2^20 already blows past the
    // millisecond cap by many orders of magnitude, so nothing above that
    // ever changes the (already-capped) outcome.
    let scale = 1u64 << attempt.min(20);
    let min = LEASE_DURATION_MS
        .saturating_mul(scale)
        .min(MAX_ELECTION_BACKOFF_MS);
    let max = LEASE_DURATION_MS
        .saturating_mul(2)
        .saturating_mul(scale)
        .min(MAX_ELECTION_BACKOFF_MS);
    let jitter_range = max.saturating_sub(min);
    let jitter = if jitter_range == 0 {
        0
    } else {
        random_jitter_millis(jitter_range)
    };
    Duration::from_millis(min + jitter)
}

/// Both `Follower` (waiting for a leader) and `PreCandidate`/`Candidate`
/// (waiting for votes) need this: if nothing interesting happens in time,
/// push `ElectionTimeout` so the event loop can start/restart an election.
///
/// Randomized per the Raft paper: two nodes with the exact same fixed
/// timeout tend to become candidates at the same instant, and then
/// perpetually split-vote against each other every single attempt
/// (neither's `RequestVote` looks newer than the other's own, so both
/// just deny each other and restart in lockstep) -- jitter is what breaks
/// the tie within a few rounds. `attempt` widens that jitter window for
/// repeated failures; a fresh `Follower` always passes 0.
fn spawn_election_timer(attempt: u32, event_tx: mpsc::Sender<Event>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let timeout = election_timeout_for_attempt(attempt);
        tokio::time::sleep(timeout).await;
        let _ = event_tx.send(Event::ElectionTimeout).await;
    })
}

/// This node's own `(last_log_index, last_log_term)`, needed both to fill
/// in a `RequestVoteRequest` we're sending and to evaluate the §5.4.1
/// up-to-date check on one we've received. `(0, 0)` for an empty log,
/// matching Raft's virtual "before the first entry" convention.
async fn last_log_index_and_term(log: &Arc<RaftLog>) -> (u64, u64) {
    let log = Arc::clone(log);
    tokio::task::spawn_blocking(move || log.last_entry())
        .await
        .expect("last_entry task panicked")
        .expect("failed to read last entry")
        .unwrap_or((0, 0))
}

/// Evaluates a pre-vote (`RequestVoteRequest.dry_run == true`) without
/// touching any state -- no term bump, no `voted_for`, no election timer
/// reset. Reports whether a *real* `RequestVote` at `req.term` would be
/// granted.
///
/// Deliberately doesn't check `voted_for`: it's always scoped to exactly
/// `current_term` (`Common::advance_term` resets it in the same step it
/// bumps the term, and `vote_for` is only ever called once `current_term`
/// already equals the term being voted for), so a real vote can only ever
/// exist at a term `<= common.current_term`. A dry run that passes the
/// `req.term > common.current_term` check below is by construction asking
/// about a term strictly beyond any term a real vote could have been cast
/// at, so there's no possible conflict to check for. (Two concurrent
/// candidates can still both pass pre-vote against the same peer this way
/// -- that's fine, since it's non-binding; the real vote phase's
/// `voted_for` check is what actually enforces "at most one winner per
/// term".)
async fn evaluate_dry_run_vote(common: &Common, req: &RequestVoteRequest) -> bool {
    if req.term <= common.current_term {
        return false;
    }
    let (our_index, our_term) = last_log_index_and_term(&common.log).await;
    (req.last_log_term, req.last_log_index) >= (our_term, our_index)
}

/// Evaluates *and applies* a real `RequestVote` (§5.4.1: term at least as
/// current, not already voted for someone else this term, candidate's log
/// at least as up to date as ours), advancing `common`'s term first if
/// `req.term` is higher and persisting the vote if granted. Returns
/// whether it granted.
///
/// Shared by every role: `Follower` and `PreCandidate` need the full
/// check since either can have an existing `voted_for` from earlier this
/// term (a `PreCandidate` never sets one, so it just inherits whatever
/// `Follower` had); `Candidate` and `Leader` always have `voted_for ==
/// Some(self)` at their own `current_term`, so for them this simplifies
/// to exactly their old behavior at `req.term == current_term` (deny) --
/// but unlike their old behavior, it also correctly withholds the vote at
/// `req.term > current_term` if the candidate's log isn't actually as up
/// to date, which the term check alone doesn't guarantee.
async fn evaluate_real_vote(common: &mut Common, req: &RequestVoteRequest) -> bool {
    if req.term > common.current_term {
        common.advance_term(req.term).await;
    }
    let grant = if req.term >= common.current_term
        && common
            .voted_for
            .as_deref()
            .is_none_or(|v| v == req.candidate_id)
    {
        let (our_index, our_term) = last_log_index_and_term(&common.log).await;
        (req.last_log_term, req.last_log_index) >= (our_term, our_index)
    } else {
        false
    };
    if grant {
        common.vote_for(&req.candidate_id).await;
    }
    grant
}

#[derive(Debug)]
pub enum RaftError {
    /// Not (or no longer) the leader. `leader_hint` lets a client retry in
    /// the right place, same as `RaftError`/`NotLeader` in client.proto.
    NotLeader {
        leader_hint: Option<String>,
    },
    Storage(StorageError),
    /// The event loop is gone (node shutting down) before it could reply.
    Disconnected,
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
    /// The value the `watch` channel holds before the driver has published
    /// anything real. Never sent by any role -- exists so callers (and
    /// tests) waiting for a specific role can't be fooled by the channel's
    /// construction-time default.
    Starting,
}

#[derive(Clone)]
pub struct LeaderHandle {
    log: Arc<RaftLog>,
    term: u64,
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
    applied_rx: watch::Receiver<u64>,
    /// Used to detect a step-down while waiting in `append_log`, so a
    /// caller doesn't hang forever on an entry that will never commit
    /// (e.g. because it gets truncated by a new leader instead).
    role_rx: watch::Receiver<RoleHandle>,
    /// Whether this leader currently holds a quorum of leases -- see the
    /// module doc. Not consumed by anything yet (no read path exists that
    /// would check it), but published either way so one can be added
    /// later without touching this plumbing.
    read_quorum_rx: watch::Receiver<bool>,
}

impl LeaderHandle {
    /// Append a new entry and wait until it's committed (replicated to a
    /// majority) *and* actually applied to `latest` before returning --
    /// see `applied_rx`'s doc on why waiting for commit alone isn't
    /// enough.
    pub async fn append_log(&self, key: &[u8], value: Option<&[u8]>) -> Result<u64, RaftError> {
        let log = Arc::clone(&self.log);
        let term = self.term;
        let key = key.to_vec();
        let value = value.map(<[u8]>::to_vec);
        let seq = tokio::task::spawn_blocking(move || log.append(term, &key, value.as_deref()))
            .await
            .map_err(|_| RaftError::Disconnected)?
            .map_err(RaftError::Storage)?;

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
        let leader_hint = match &*role_rx.borrow() {
            RoleHandle::Follower(f) => f.leader_hint.clone(),
            _ => None,
        };
        RaftError::NotLeader { leader_hint }
    }

    /// Whether it's currently safe for this leader to serve a
    /// linearizable read directly from local state, without going
    /// through the log -- i.e. whether it holds a quorum of leases. See
    /// the module doc.
    pub fn has_read_quorum(&self) -> bool {
        *self.read_quorum_rx.borrow()
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
    event_tx: mpsc::Sender<Event>,
    /// Read directly (never through the event loop) by `get_lease` below,
    /// so answering a lease probe can never get stuck behind whatever the
    /// event loop is currently doing -- e.g. a slow `AppendEntries`
    /// mid-disk-write. See the module doc.
    role_rx: watch::Receiver<RoleHandle>,
    term_rx: watch::Receiver<u64>,
    lease_expires_at_tx: Arc<watch::Sender<Option<Instant>>>,
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
            let _ = self
                .lease_expires_at_tx
                .send(Some(Instant::now() + LEASE_DURATION));
        }
        GetLeaseResponse {
            lease_granted: granted,
        }
    }
}

/// Caches one gRPC client per peer (see the module doc), shared across
/// every role and every election via `Common::peer_conns`.
#[derive(Default)]
struct PeerConnections {
    clients: AsyncMutex<HashMap<String, RaftPeerClient<Channel>>>,
}

impl PeerConnections {
    /// A cloned handle to a cached client for `peer`, connecting (and
    /// caching the result) if there isn't one yet. `RaftPeerClient` is a
    /// thin wrapper around a `Channel`, which is cheap to clone and safe
    /// to use concurrently from many tasks at once. Connect failures
    /// aren't cached, so the next attempt gets a fresh try.
    async fn get_or_connect(&self, peer: &PeerInfo) -> Option<RaftPeerClient<Channel>> {
        let mut clients = self.clients.lock().await;
        if let Some(client) = clients.get(&peer.id) {
            return Some(client.clone());
        }
        let client = RaftPeerClient::connect(peer.address.clone()).await.ok()?;
        clients.insert(peer.id.clone(), client.clone());
        Some(client)
    }

    /// Drops a peer's cached client, if any, so the next call reconnects
    /// instead of reusing a connection that's already failed once.
    /// `Channel` doesn't auto-reconnect on its own once its connection
    /// drops.
    async fn evict(&self, peer_id: &str) {
        self.clients.lock().await.remove(peer_id);
    }
}

/// State shared across every role, threaded from one role's `shutdown`
/// straight into the next role's `new`.
pub struct Common {
    pub node_id: String,
    pub peers: Vec<PeerInfo>,
    pub log: Arc<RaftLog>,
    peer_conns: Arc<PeerConnections>,

    /// Only ever changed via `advance_term`/`vote_for` below, which also
    /// persist the change -- see the module doc on why nothing should be
    /// able to touch these without going through storage first.
    current_term: u64,
    voted_for: Option<String>,
    /// Mirrors `current_term`, published on every `advance_term` call, so
    /// `RaftPeerHandle::get_lease`'s fast path can read it without going
    /// through the event loop.
    term_tx: watch::Sender<u64>,

    /// Consecutive failed election cycles (pre-vote or real) since this
    /// node last had reason to believe a leader existed -- see the module
    /// doc. Not persisted: it's just a backoff hint, not safety-critical
    /// state, so losing it across a restart is harmless.
    election_attempt: u32,

    /// When this node stops refusing vote requests from anyone but the
    /// leader it currently recognizes -- see the module doc on leader
    /// leasing. `None` means no lease has ever been established (distinct
    /// from "expired": see `follower::spawn_lease_expiration_watcher` and
    /// `follower::Follower::has_active_lease`, which each need to treat a
    /// never-established lease differently -- an already-passed `Instant`
    /// can't represent both "never granted" and "granted, then lapsed").
    /// `Arc`-wrapped because `RaftPeerHandle::get_lease` writes to it
    /// directly (bypassing the event loop entirely, on purpose -- see
    /// that method), not just whichever role's event loop is currently
    /// running.
    lease_expires_at_tx: Arc<watch::Sender<Option<Instant>>>,
    lease_expires_at_rx: watch::Receiver<Option<Instant>>,

    /// Highest seq known committed. Outlives any single role (a follower
    /// applies a leader's `leader_commit` here too), so it lives in
    /// `Common` rather than inside `Leader`.
    pub commit_tx: watch::Sender<u64>,

    /// Highest seq known replicated to *every* peer, not just a majority --
    /// safe to compact below regardless of role: a leader computes it from
    /// `Leader::peer_match_index`, a follower just adopts whatever value
    /// rides along on `AppendEntries.compact_seq`. Consumed by
    /// `spawn_storage_maintenance_task`.
    pub compact_tx: watch::Sender<u64>,

    /// How far `RaftLog::apply_committed` has actually brought `latest`
    /// up to date -- published by `spawn_storage_maintenance_task` only
    /// *after* that call returns, deliberately never touched anywhere
    /// else. This is what `LeaderHandle::append_log` waits on (see its
    /// doc): `commit_tx` alone only says an entry is safely replicated,
    /// not that `latest` reflects it yet, since the maintenance task is
    /// an independent `watch` subscriber with no ordering relationship to
    /// any other subscriber of `commit_tx`. `Arc`-wrapped so `Common` can
    /// keep handing out fresh `subscribe()`s (one per `Leader::new`, since
    /// a new `LeaderHandle` is built on every election) after the one
    /// sender has already been moved into the maintenance task.
    applied_tx: Arc<watch::Sender<u64>>,

    /// Where peer RPCs (relayed by `RaftPeerService`) and internal timers
    /// land. Shared across roles so anything already queued survives a
    /// transition.
    pub event_rx: mpsc::Receiver<Event>,
    pub event_tx: mpsc::Sender<Event>,

    pub role_tx: watch::Sender<RoleHandle>,
}

/// `(common, role_rx, peer_handle, commit_rx, compact_rx)` -- the state
/// machine plus the external handles a caller needs to observe/drive it.
type NewCommon = (
    Common,
    watch::Receiver<RoleHandle>,
    RaftPeerHandle,
    watch::Receiver<u64>,
    watch::Receiver<u64>,
);

impl Common {
    pub fn new(
        node_id: String,
        peers: Vec<PeerInfo>,
        log: Arc<RaftLog>,
    ) -> storage::Result<NewCommon> {
        let (current_term, voted_for) = log.term_state()?;

        let (commit_tx, commit_rx) = watch::channel(0);
        let (compact_tx, compact_rx) = watch::channel(0);
        // The initial receiver is discarded -- `Common` hands out fresh
        // ones later via `applied_tx.subscribe()` (once per `Leader::new`),
        // which works regardless of whether any earlier receiver is still
        // alive, as long as the sender itself is.
        let (applied_tx, _applied_rx) = watch::channel(0);
        let applied_tx = Arc::new(applied_tx);
        let (term_tx, term_rx) = watch::channel(current_term);
        // `None` -- no lease established yet. `has_active_lease` treats
        // this as "not blocked" (a brand new node that's never contacted
        // anyone shouldn't refuse votes), while
        // `spawn_lease_expiration_watcher` treats it as "give this node
        // the same full grace period a fresh grant would get" -- neither
        // of those is what an already-elapsed `Instant` would mean, which
        // is why this is `Option<Instant>` rather than just `Instant`.
        let (lease_expires_at_tx, lease_expires_at_rx) = watch::channel(None);
        let lease_expires_at_tx = Arc::new(lease_expires_at_tx);
        let (event_tx, event_rx) = mpsc::channel(256);
        let (role_tx, role_rx) = watch::channel(RoleHandle::Starting);

        let peer_handle = RaftPeerHandle {
            event_tx: event_tx.clone(),
            role_rx: role_tx.subscribe(),
            term_rx,
            lease_expires_at_tx: Arc::clone(&lease_expires_at_tx),
        };

        let common = Self {
            node_id,
            peers,
            log,
            peer_conns: Arc::new(PeerConnections::default()),
            current_term,
            voted_for,
            term_tx,
            election_attempt: 0,
            lease_expires_at_tx,
            lease_expires_at_rx,
            commit_tx,
            compact_tx,
            applied_tx,
            event_rx,
            event_tx,
            role_tx,
        };
        Ok((common, role_rx, peer_handle, commit_rx, compact_rx))
    }

    /// Advances `commit_tx` to `new_value` if that's higher than what's
    /// already published -- never regresses, in case responses/requests
    /// are ever processed out of order.
    fn advance_commit(&self, new_value: u64) {
        if new_value > *self.commit_tx.borrow() {
            let _ = self.commit_tx.send(new_value);
        }
    }

    /// Same as `advance_commit`, for the "replicated to every peer, safe
    /// to compact" watermark.
    fn advance_compact(&self, new_value: u64) {
        if new_value > *self.compact_tx.borrow() {
            let _ = self.compact_tx.send(new_value);
        }
    }

    fn publish_role(&self, handle: RoleHandle) {
        let _ = self.role_tx.send(handle);
    }

    fn majority(&self) -> usize {
        self.peers.len().div_ceil(2) + 1
    }

    /// Advance to a higher term, discarding any vote already cast (Raft
    /// scopes `voted_for` per-term -- a candidate campaigning in a brand
    /// new term must not be refused because of who we voted for last
    /// term). No-op if `term` isn't actually higher than what's already
    /// recorded -- callers should still check `term > current_term`
    /// themselves before calling where practical, to skip the
    /// persist-dispatching `.await` entirely rather than pay for it and
    /// have it no-op.
    async fn advance_term(&mut self, term: u64) {
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
            let _ = self.term_tx.send(term);
            self.persist_term_state().await;
        }
    }

    /// Record a vote for `candidate` in the current term.
    async fn vote_for(&mut self, candidate: &str) {
        self.voted_for = Some(candidate.to_string());
        self.persist_term_state().await;
    }

    /// Panics on a storage failure rather than continuing on an
    /// unpersisted term/vote: silently proceeding risks a double vote in
    /// the same term after a crash, which is a Raft safety violation, not
    /// just a lost write.
    async fn persist_term_state(&self) {
        let log = Arc::clone(&self.log);
        let term = self.current_term;
        let voted_for = self.voted_for.clone();
        tokio::task::spawn_blocking(move || log.persist_term_state(term, voted_for.as_deref()))
            .await
            .expect("persist_term_state task panicked")
            .expect("failed to persist raft term/vote state");
    }
}

// --- Driver ----------------------------------------------------------------

/// Storage maintenance: applies newly committed entries to `latest` (see
/// `RaftLog::apply_committed` -- `latest` must never reflect an
/// uncommitted write, since a leader change could still roll it back),
/// publishes `applied_tx` to say so, and then compacts `log` up to
/// `compact_seq`, every time either watermark advances.
///
/// The apply-then-compact ordering matters: `compact` physically removes
/// `log` entries, and doing that before they've been applied to `latest`
/// would lose that data permanently (`RaftLog::compact` also
/// independently clamps itself to `apply_committed`'s watermark as a
/// second line of defense, but doing both here, always in this order,
/// keeps the common case simple rather than relying on that clamp to
/// silently paper over bad ordering). `applied_tx` is published as its
/// own step, right after `apply_committed` returns and *before* `compact`
/// even runs -- `LeaderHandle::append_log` needs to know `latest` itself
/// is caught up, not merely that `compact` was also attempted, and
/// separating the two means a slow/failing `compact` call can never delay
/// that signal. Reacting to both `commit_tx` and `compact_tx` (rather
/// than just one) matters too: a follower's `compact_seq` can advance
/// independently of its own `commit_tx` (see `Follower::apply_append_entries`),
/// so watching only one channel could leave the other's progress stalled.
///
/// Spawned once per node, independent of role: this is storage
/// maintenance, not state-machine behavior, so unlike replication/lease/tick
/// tasks it doesn't need starting or stopping across role transitions.
///
/// A failure here is logged rather than panicking (unlike, say, a failed
/// term persist): falling behind on applying/compacting doesn't corrupt
/// anything or violate a safety property, it just means `latest` is
/// briefly stale and the log is larger on disk than it needs to be --
/// this is maintenance, not a correctness requirement, and it'll simply
/// retry on the next watermark change.
fn spawn_storage_maintenance_task(
    log: Arc<RaftLog>,
    mut commit_rx: watch::Receiver<u64>,
    mut compact_rx: watch::Receiver<u64>,
    applied_tx: Arc<watch::Sender<u64>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                r = commit_rx.changed() => { if r.is_err() { return; } }
                r = compact_rx.changed() => { if r.is_err() { return; } }
            }
            let commit_seq = *commit_rx.borrow();
            let compact_seq = *compact_rx.borrow();

            let log2 = Arc::clone(&log);
            match tokio::task::spawn_blocking(move || log2.apply_committed(commit_seq)).await {
                Ok(Ok(())) => {
                    // Only ever moves forward -- `commit_seq` is itself
                    // monotonic (`Common::advance_commit`), so an
                    // out-of-order wakeup can't regress this.
                    let _ = applied_tx.send(commit_seq);
                }
                Ok(Err(e)) => {
                    eprintln!("apply_committed to seq {commit_seq} failed: {e}");
                    continue; // don't attempt to compact past what didn't apply
                }
                Err(e) => {
                    eprintln!("apply_committed task panicked: {e}");
                    continue;
                }
            }

            let log3 = Arc::clone(&log);
            match tokio::task::spawn_blocking(move || log3.compact(compact_seq)).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => eprintln!("compact to seq {compact_seq} failed: {e}"),
                Err(e) => eprintln!("compact task panicked: {e}"),
            }
        }
    })
}

/// Runs forever, handing `Common` from one role to the next. Only one
/// role's event loop is ever active: the previous one has fully returned
/// (and torn down its background tasks) before the next one starts.
pub async fn run(mut common: Common) -> ! {
    let _maintenance_task = spawn_storage_maintenance_task(
        Arc::clone(&common.log),
        common.commit_tx.subscribe(),
        common.compact_tx.subscribe(),
        Arc::clone(&common.applied_tx),
    );
    let mut role = Role::Follower;
    loop {
        let (next_role, next_common) = match role {
            Role::Follower => Follower::new(common).await,
            Role::PreCandidate => PreCandidate::new(common).await,
            Role::Candidate => Candidate::new(common).await,
            Role::Leader => Leader::new(common).await,
        };
        role = next_role;
        common = next_common;
    }
}

/// Shared test infrastructure for this module and its `leader`/`candidate`/
/// `follower` submodules -- each submodule's own `#[cfg(test)] mod tests`
/// pulls these in with `use crate::raft::test_support::*;`.
#[cfg(test)]
pub(crate) mod test_support;

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;

    #[tokio::test]
    async fn fresh_node_starts_as_follower() {
        let (mut role_rx, _peer_handle, path) = spawn_test_node(vec![]);
        wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Follower(_))).await;
        cleanup(&path);
    }

    #[tokio::test]
    async fn follower_becomes_candidate_after_election_timeout() {
        let (mut role_rx, _peer_handle, path) = spawn_test_node(vec![]);
        wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Candidate)).await;
        cleanup(&path);
    }

    /// End-to-end over real sockets: two full nodes (state machine + a real
    /// `RaftPeerService` bound to a real port), each configured with the
    /// other as its only peer. Exercises the whole voting path --
    /// `Candidate` actually dialing out via `RaftPeerClient`,
    /// `PeerConnections` caching/reusing that connection, and the
    /// follower-side §5.4.1 check granting the vote -- rather than just the
    /// in-process pieces the other tests cover individually.
    #[tokio::test]
    async fn candidate_wins_election_against_a_real_peer_over_the_network() {
        use crate::grpc::RaftPeerService;
        use crate::pb::raft5::raft::raft_peer_server::RaftPeerServer;

        let addr_a = free_local_addr();
        let addr_b = free_local_addr();

        let (log_a, path_a) = open_temp_log();
        let (log_b, path_b) = open_temp_log();

        let (common_a, mut role_rx_a, peer_handle_a, _commit_rx_a, _compact_rx_a) = Common::new(
            "node-a".to_string(),
            vec![PeerInfo {
                id: "node-b".to_string(),
                address: format!("http://{addr_b}"),
            }],
            Arc::clone(&log_a),
        )
        .unwrap();
        let (common_b, mut role_rx_b, peer_handle_b, _commit_rx_b, _compact_rx_b) = Common::new(
            "node-b".to_string(),
            vec![PeerInfo {
                id: "node-a".to_string(),
                address: format!("http://{addr_a}"),
            }],
            Arc::clone(&log_b),
        )
        .unwrap();

        tokio::spawn(run(common_a));
        tokio::spawn(run(common_b));

        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(RaftPeerServer::new(RaftPeerService::new(peer_handle_a)))
                .serve(addr_a),
        );
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(RaftPeerServer::new(RaftPeerService::new(peer_handle_b)))
                .serve(addr_b),
        );

        // Whichever of the two wins its election first -- a real,
        // cross-process-style race, same as a real cluster.
        tokio::select! {
            _ = wait_for_role(&mut role_rx_a, |r| matches!(r, RoleHandle::Leader(_))) => {}
            _ = wait_for_role(&mut role_rx_b, |r| matches!(r, RoleHandle::Leader(_))) => {}
        }

        cleanup(&path_a);
        cleanup(&path_b);
    }

    /// End-to-end over real sockets, continuing where
    /// `candidate_wins_election_against_a_real_peer_over_the_network`
    /// leaves off: once one of the two nodes wins the election, a write
    /// through its `LeaderHandle` should reach the other node's storage
    /// too (real replication task, real connection, real §5.3 next_index
    /// tracking) and commit (real majority -- both nodes -- computed by
    /// `Leader::recompute_watermarks`) rather than hanging.
    #[tokio::test]
    async fn leader_replicates_a_write_to_a_real_peer_and_commits_it() {
        use crate::grpc::RaftPeerService;
        use crate::pb::raft5::raft::raft_peer_server::RaftPeerServer;

        let addr_a = free_local_addr();
        let addr_b = free_local_addr();

        let (log_a, path_a) = open_temp_log();
        let (log_b, path_b) = open_temp_log();

        let (common_a, mut role_rx_a, peer_handle_a, _commit_rx_a, _compact_rx_a) = Common::new(
            "node-a".to_string(),
            vec![PeerInfo {
                id: "node-b".to_string(),
                address: format!("http://{addr_b}"),
            }],
            Arc::clone(&log_a),
        )
        .unwrap();
        let (common_b, mut role_rx_b, peer_handle_b, _commit_rx_b, _compact_rx_b) = Common::new(
            "node-b".to_string(),
            vec![PeerInfo {
                id: "node-a".to_string(),
                address: format!("http://{addr_a}"),
            }],
            Arc::clone(&log_b),
        )
        .unwrap();

        tokio::spawn(run(common_a));
        tokio::spawn(run(common_b));

        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(RaftPeerServer::new(RaftPeerService::new(peer_handle_a)))
                .serve(addr_a),
        );
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(RaftPeerServer::new(RaftPeerService::new(peer_handle_b)))
                .serve(addr_b),
        );

        tokio::select! {
            _ = wait_for_role(&mut role_rx_a, |r| matches!(r, RoleHandle::Leader(_))) => {}
            _ = wait_for_role(&mut role_rx_b, |r| matches!(r, RoleHandle::Leader(_))) => {}
        }

        // Leadership can still shuffle right after the initial election
        // settles: e.g. the loser's own election timer, having already
        // been ticking, fires a competing (higher-term) candidacy before
        // it ever hears the winner's first heartbeat, and per Raft the
        // winner correctly steps down on seeing that higher term. That's
        // correct behavior, not a bug -- so retry against whoever's leader
        // now, same as a real client would on `NotLeader`, rather than
        // assuming the first leader observed stays leader.
        let (seq, follower_log) = loop {
            let a_is_leader = matches!(&*role_rx_a.borrow(), RoleHandle::Leader(_));
            let (leader, follower_log) = if a_is_leader {
                (
                    expect_leader_handle(&role_rx_a.borrow()),
                    Arc::clone(&log_b),
                )
            } else {
                (
                    expect_leader_handle(&role_rx_b.borrow()),
                    Arc::clone(&log_a),
                )
            };
            match tokio::time::timeout(
                Duration::from_millis(2000),
                leader.append_log(b"k", Some(b"v")),
            )
            .await
            .expect("append_log should commit once the peer replicates it")
            {
                Ok(seq) => break (seq, follower_log),
                Err(RaftError::NotLeader { .. }) => {
                    tokio::select! {
                        _ = wait_for_role(&mut role_rx_a, |r| matches!(r, RoleHandle::Leader(_))) => {}
                        _ = wait_for_role(&mut role_rx_b, |r| matches!(r, RoleHandle::Leader(_))) => {}
                    }
                }
                Err(e) => panic!("unexpected append_log error: {e:?}"),
            }
        };

        // `append_log` only waits for the leader's own `commit_tx`, which
        // can advance the instant a majority (both nodes, here) has the
        // entry -- give the follower's own AppendEntries handling a moment
        // to actually finish persisting it locally before checking.
        tokio::time::timeout(Duration::from_millis(500), async {
            loop {
                if follower_log.get_latest(b"k").unwrap().is_some() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("follower should eventually receive the replicated entry");

        assert_eq!(
            follower_log.get_latest(b"k").unwrap(),
            Some((seq, b"v".to_vec()))
        );

        cleanup(&path_a);
        cleanup(&path_b);
    }

    /// A zero-peer cluster's `compact_seq` trivially equals its own last
    /// index (see `leader::Leader::recompute_watermarks`) -- confirms the
    /// compaction task spawned by `run` actually reacts to that and does
    /// real work, not just that the watermark itself advances correctly
    /// (already covered elsewhere).
    #[tokio::test]
    async fn compaction_task_compacts_the_log_as_compact_seq_advances() {
        let (log, mut role_rx, _peer_handle, _commit_rx, mut compact_rx, path) =
            spawn_node_with_handles(vec![]);
        wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Leader(_))).await;
        let leader = expect_leader_handle(&role_rx.borrow());

        leader.append_log(b"a", Some(b"1")).await.unwrap();
        leader.append_log(b"b", Some(b"2")).await.unwrap();
        leader.append_log(b"c", Some(b"3")).await.unwrap();

        tokio::time::timeout(Duration::from_millis(1000), async {
            loop {
                if *compact_rx.borrow() >= 3 {
                    return;
                }
                compact_rx.changed().await.unwrap();
            }
        })
        .await
        .expect("compact_seq should advance to the tail in a zero-peer cluster");

        // The compaction task reacts to that same watch channel -- give
        // it a moment to actually run its (tiny, but still real) blocking
        // I/O.
        tokio::time::timeout(Duration::from_millis(1000), async {
            loop {
                if log.entries_in_range(1, 3).unwrap().is_empty() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("compaction should have removed the entries below compact_seq");

        // Compaction only removes superseded history -- current values
        // must still be intact.
        assert_eq!(log.get_latest(b"a").unwrap(), Some((1, b"1".to_vec())));
        assert_eq!(log.get_latest(b"b").unwrap(), Some((2, b"2".to_vec())));
        assert_eq!(log.get_latest(b"c").unwrap(), Some((3, b"3".to_vec())));

        cleanup(&path);
    }
}
