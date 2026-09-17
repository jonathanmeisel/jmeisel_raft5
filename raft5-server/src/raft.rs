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
//! Raft §5.1's term rule -- "if an RPC request *or response* contains term
//! T > currentTerm, set currentTerm = T and convert to follower" -- is
//! split by which half of it is uniform. For a **response** it is applied
//! once, for every role, in `Common::next_event` (see
//! `Event::response_term`): there is no reply to compose, so adopting the
//! term is the entire job, and centralising it means a new `Event` variant
//! cannot quietly skip it. For a **request** each role answers differently
//! before stepping down -- a leader rejects the append, a follower accepts
//! it, a candidate still evaluates the vote -- so that half stays in the
//! role event loops, next to the reply it has to build.
//!
//! Every `storage::RaftLog` call made from async code goes through
//! `store::Store`, which puts it on the blocking pool -- fjall's calls
//! are synchronous, and blocking an executor thread on disk I/O (even
//! briefly) can stall whatever else that thread was scheduled to run.
//! Nothing here calls `spawn_blocking` itself; `Store` is the one place
//! that does, so the rule cannot be forgotten at a new call site. Because
//! of this, `handle_event` is `async fn` on every role even though most
//! events don't touch storage, so the one that does (`AppendEntries`, via
//! `apply_append_entries`) can `.await` its storage call like anything
//! else in the loop.
//!
//! `Common::peer_conns` holds one gRPC client per peer (keyed by
//! `PeerInfo::id`), shared across every role and every election so
//! `RequestVote`/`AppendEntries` calls don't pay a fresh connection setup
//! each time. The clients are built with `Endpoint::connect_lazy`, which
//! dials on first use and re-dials by itself afterwards, so a failed call
//! needs no eviction and no bookkeeping -- the next call simply
//! reconnects. That is also what makes handing one out a pure in-memory
//! lookup rather than something that can block: see `PeerConnections`.
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
//! # Cluster membership
//!
//! Membership is not configuration a node is started with -- it lives in
//! the log, as `Config` entries replicated and log-matched like any other
//! (Raft §4). Three consequences run through this module:
//!
//! * A configuration takes effect on **append**, not on commit (§4.1).
//!   `Common::reload_config` is called wherever one may have appeared,
//!   and `storage::RaftLog::current_config` explains why waiting for
//!   commitment would deadlock.
//! * Members are **voters or learners** (§4.2.1). Everyone is replicated
//!   to; only voters count toward a quorum, campaign, or hold a lease.
//!   A server is added as a learner and promoted automatically once it
//!   can keep up -- see `leader::Leader::review_learner_progress`.
//! * A node whose log holds no configuration belongs to **no cluster**
//!   (`RoleHandle::Uninitialized`, §4.4). It answers RPCs and waits to be
//!   bootstrapped (`ClientHandle::bootstrap`) or replicated to; it never
//!   assumes it is its own cluster, since two nodes doing that would be
//!   two clusters.
//!
//! # Cancellation
//!
//! Every background task here -- a role's timers, a leader's per-peer
//! replication and lease probes, a candidate's vote requests, the storage
//! maintenance task -- is owned by a `Task`, and every one of those runs
//! under a child of the node's single root `CancellationToken` (supplied
//! by `crate::serve`). There are three rules, and `Task` exists to make
//! them hard to break:
//!
//! * **Nothing is detached.** A task is stopped by `Task::stop`, which
//!   cancels and *then joins*. `JoinHandle::abort` leaves the task
//!   wherever its last poll got to, which is how an `Arc<RaftLog>` -- and
//!   with it the database lock -- ends up outliving the node that owned
//!   it, invisibly, until the next node to open the same directory fails.
//! * **Long waits are cancellable, storage calls are not.** Peer RPCs,
//!   heartbeat sleeps and sends into the event loop go through
//!   `until_cancelled`; `Store` calls are always awaited to completion,
//!   since dropping a `spawn_blocking` doesn't stop it -- it keeps
//!   running on its own worker thread, still holding what it captured.
//! * **`run` returning means the node is gone.** Not "is stopping": every
//!   role has torn down and joined its own tasks, the maintenance task
//!   has been joined, and `Common` has been dropped, which is what
//!   releases the gRPC handlers still parked on its channels.
//!
//! Still simplified: the leader-side §5.3 backoff optimization (see
//! `leader::Leader::spawn_replication`), and nothing triggers snapshots
//! on its own yet -- a snapshot is sent only when a peer has fallen
//! behind the compaction floor.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::pb::raft5::cluster::ClusterConfig;
use crate::pb::raft5::common::LogEntry;
use crate::pb::raft5::raft::{
    AppendEntriesRequest, AppendEntriesResponse, GetLeaseResponse, InstallSnapshotResponse,
    RequestVoteRequest, RequestVoteResponse, SnapshotHeader,
};
use crate::storage::{self, RaftLog, StorageError};
use crate::store::Store;

mod candidate;
mod event;
mod follower;
mod handles;
mod leader;
mod peer;
mod task;

use candidate::{Candidate, PreCandidate};
use follower::Follower;
use leader::Leader;

// Re-exported at the module root rather than left behind a path, for two
// reasons: these names are `raft`'s public API (`grpc` and `lib` know
// them as `raft::LeaderHandle`, not `raft::handles::LeaderHandle`), and
// each role module's `use super::*` picks the whole vocabulary up from
// here, so splitting this file didn't touch a single role.
pub use event::{ConfigChange, Event, RaftError};
pub use handles::{
    ClientHandle, FollowerHandle, LeaderHandle, RaftPeerHandle, RoleHandle, leader_hint_of,
};
pub use peer::PeerInfo;

pub(crate) use event::Role;
use event::Transition;
use peer::PeerConnections;
pub(crate) use task::Task;
use task::{send_event, until_cancelled};

/// The one base timing unit everything else derives from (see the module
/// doc). A `u64` millisecond count alongside the `Duration` form since a
/// couple of derived constants need to do integer arithmetic on it (in a
/// `const` context, before a `Duration` value could be decomposed back
/// out again). Will become a user-configurable, per-cluster setting;
/// hardcoded for now.
pub(crate) const HEARTBEAT_MS: u64 = 50;
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
fn spawn_election_timer(
    cancel: &CancellationToken,
    attempt: u32,
    event_tx: mpsc::Sender<Event>,
) -> Task {
    Task::spawn(cancel, move |cancel| async move {
        let timeout = election_timeout_for_attempt(attempt);
        if until_cancelled(&cancel, tokio::time::sleep(timeout))
            .await
            .is_some()
        {
            send_event(&cancel, &event_tx, Event::ElectionTimeout).await;
        }
    })
}

/// This node's own `(last_log_index, last_log_term)`, needed both to fill
/// in a `RequestVoteRequest` we're sending and to evaluate the §5.4.1
/// up-to-date check on one we've received. `(0, 0)` for an empty log,
/// matching Raft's virtual "before the first entry" convention.
async fn last_log_index_and_term(store: &Store) -> (u64, u64) {
    store
        .expect("read the last log entry", RaftLog::last_entry)
        .await
        .unwrap_or((0, 0))
}

/// The term of the entry at `seq`, or `None` if we no longer have (or
/// never had) one there. Async wrapper over `RaftLog::term_at`, same
/// shape as `last_log_index_and_term`.
async fn term_at(store: &Store, seq: u64) -> Option<u64> {
    store
        .expect("read the term at a log index", move |log| log.term_at(seq))
        .await
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
    let (our_index, our_term) = last_log_index_and_term(&common.store).await;
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
        let (our_index, our_term) = last_log_index_and_term(&common.store).await;
        (req.last_log_term, req.last_log_index) >= (our_term, our_index)
    } else {
        false
    };
    if grant {
        common.vote_for(&req.candidate_id).await;
    }
    grant
}

/// The cluster configuration recorded in `log`, decoded. `None` for a
/// store that has never seen one.
fn load_config(log: &RaftLog) -> storage::Result<Option<ClusterConfig>> {
    match log.current_config()? {
        Some(bytes) => ClusterConfig::from_bytes(&bytes)
            .map(Some)
            .map_err(|_| StorageError::Corrupt("cluster config in the log failed to decode")),
        None => Ok(None),
    }
}

/// A configuration made of `node_id` plus `peers`, all voting -- an
/// implicit bootstrap.
///
/// Test-only, and deliberately so: implicit bootstrap is exactly what
/// Raft §4.4 warns against, since two nodes started with different peer
/// lists would each form a cluster believing itself to be the whole
/// thing. Real nodes start uninitialized and are bootstrapped explicitly.
#[cfg(test)]
fn seed_config(node_id: &str, peers: &[PeerInfo]) -> ClusterConfig {
    use crate::pb::raft5::cluster::ClusterMember;
    let member = |id: &str, address: &str| ClusterMember {
        server_id: id.to_string(),
        address: address.to_string(),
        voting: true,
    };
    ClusterConfig {
        members: std::iter::once(member(node_id, ""))
            .chain(peers.iter().map(|p| member(&p.id, &p.address)))
            .collect(),
        heartbeat_millis: HEARTBEAT_MS,
    }
}

/// State shared across every role, threaded from one role's `shutdown`
/// straight into the next role's `new`.
///
/// Every field is private. That isn't only tidiness: `raft` is a `pub`
/// module, so a `pub` field here would put this node's event channel,
/// watermark senders and role publisher on the crate's public API, where
/// anything could drive them out from under the state machine. The role
/// modules are descendants of `raft` and so see them all regardless --
/// which is the intended audience, and the only one.
pub struct Common {
    node_id: String,
    /// The cluster configuration this node is currently operating under:
    /// who its peers are, and which of them vote. Read from the log (see
    /// `storage::RaftLog::current_config`) rather than from a flag, and
    /// re-read whenever a `Config` entry is appended or rolled back --
    /// on *append*, not on commit, per Raft §4.1.
    config: ClusterConfig,
    store: Store,
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
    commit_tx: watch::Sender<u64>,

    /// Highest seq known replicated to *every* peer, not just a majority --
    /// safe to compact below regardless of role: a leader computes it from
    /// `Leader::peer_match_index`, a follower just adopts whatever value
    /// rides along on `AppendEntries.compact_seq`. Consumed by
    /// `spawn_storage_maintenance_task`.
    compact_tx: watch::Sender<u64>,

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
    event_rx: mpsc::Receiver<Event>,
    event_tx: mpsc::Sender<Event>,

    role_tx: watch::Sender<RoleHandle>,

    /// This node's root cancellation token, supplied by whoever started
    /// it (`crate::serve`). Every background task this node spawns runs
    /// under a child of it, so cancelling it here brings the whole node
    /// down -- see `Task` and the module doc.
    cancel: CancellationToken,
}

/// What `Common::new` hands back: the state machine itself, plus the
/// handles a caller needs to observe and drive it.
///
/// A struct rather than the tuple this used to be, and deliberately.
/// `commit_rx` and `compact_rx` are both `watch::Receiver<u64>` and mean
/// very different things, so as adjacent tuple elements they could be
/// transposed at any call site and still compile -- yielding a node that
/// compacted at its commit watermark, which is exactly the bug
/// `RaftLog::compact`'s clamp exists to survive. Named fields make the
/// mistake unrepresentable instead of merely survivable.
pub struct NodeHandles {
    pub common: Common,
    pub role_rx: watch::Receiver<RoleHandle>,
    pub peer_handle: RaftPeerHandle,
    pub commit_rx: watch::Receiver<u64>,
    pub compact_rx: watch::Receiver<u64>,
}

impl Common {
    /// Membership comes from `log` alone. A store that has never seen a
    /// configuration yields an empty one, which is how a node starts out
    /// uninitialized: it has no vote, so it never campaigns, and it waits
    /// to be either bootstrapped or replicated to. See
    /// `RoleHandle::Uninitialized`.
    // Same reason the role constructors carry this: what a caller needs
    // back is not just the `Common` but the handles for reaching it, and
    // splitting the two into `new` plus a second call would let a caller
    // hold a `Common` that nothing can observe.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        node_id: String,
        log: Arc<RaftLog>,
        cancel: CancellationToken,
    ) -> storage::Result<NodeHandles> {
        Self::build(node_id, log, ClusterConfig::default(), cancel)
    }

    /// `new`, but with a configuration supplied directly rather than read
    /// from the log -- for tests that want a given cluster shape without
    /// bootstrapping one. Still loses to a configuration in the log, if
    /// there is one.
    #[cfg(test)]
    pub fn new_seeded(
        node_id: String,
        peers: Vec<PeerInfo>,
        log: Arc<RaftLog>,
        cancel: CancellationToken,
    ) -> storage::Result<NodeHandles> {
        let seed = seed_config(&node_id, &peers);
        Self::build(node_id, log, seed, cancel)
    }

    fn build(
        node_id: String,
        log: Arc<RaftLog>,
        fallback: ClusterConfig,
        cancel: CancellationToken,
    ) -> storage::Result<NodeHandles> {
        let (current_term, voted_for) = log.term_state()?;
        let config = load_config(&log)?.unwrap_or(fallback);

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
            config,
            store: Store::new(log),
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
            cancel,
        };
        Ok(NodeHandles {
            common,
            role_rx,
            peer_handle,
            commit_rx,
            compact_rx,
        })
    }

    /// Advances `commit_tx` to `new_value` if that's higher than what's
    /// already published -- never regresses, in case responses/requests
    /// are ever processed out of order.
    ///
    /// `send_replace`, not `send`, and that matters: every `watch` in
    /// `Common` is *state* that is read back through `borrow()` (this
    /// method's own comparison, `Leader::propose_config`'s catch-up gate,
    /// `step_down_if_removed`), not merely a notification. `send` fails
    /// and leaves the value untouched once the last receiver is dropped,
    /// so a watermark could silently stop advancing while the code that
    /// reads it went on believing it. `send_replace` always stores.
    fn advance_commit(&self, new_value: u64) {
        if new_value > *self.commit_tx.borrow() {
            self.commit_tx.send_replace(new_value);
        }
    }

    /// Same as `advance_commit`, for the "replicated to every peer, safe
    /// to compact" watermark.
    fn advance_compact(&self, new_value: u64) {
        if new_value > *self.compact_tx.borrow() {
            self.compact_tx.send_replace(new_value);
        }
    }

    /// A handle for the client-facing service. Taken before `run`
    /// consumes `Common`.
    pub fn client_handle(&self) -> ClientHandle {
        ClientHandle {
            event_tx: self.event_tx.clone(),
            role_rx: self.role_tx.subscribe(),
        }
    }

    fn publish_role(&self, handle: RoleHandle) {
        self.role_tx.send_replace(handle);
    }

    /// The next thing this node's event loop should do.
    ///
    /// Every role funnels through here, which buys two things. Cancelling
    /// the node stops it at once rather than whenever its next timer
    /// happens to fire; and Raft §5.1's term rule is applied once, for all
    /// roles, rather than being rewritten in each of them (see
    /// `Event::response_term`).
    async fn next_event(&mut self) -> Next {
        let Self {
            cancel, event_rx, ..
        } = self;
        let Some(Some(event)) = until_cancelled(cancel, event_rx.recv()).await else {
            return Next::Stop;
        };
        if let Some(term) = event.response_term()
            && term > self.current_term
        {
            self.advance_term(term).await;
            return Next::StepDown;
        }
        Next::Handle(event)
    }

    /// A parent for the background tasks a role owns, so that cancelling
    /// this node cancels them too -- see `Task::spawn`.
    fn cancel_token(&self) -> &CancellationToken {
        &self.cancel
    }

    /// Every other member of the current configuration -- everyone this
    /// node replicates to and probes, learners included.
    fn peers(&self) -> Vec<PeerInfo> {
        self.config
            .others(&self.node_id)
            .map(PeerInfo::from)
            .collect()
    }

    /// The other *voting* members: who a candidate solicits votes from,
    /// and whose leases a leader counts toward read quorum. A learner is
    /// replicated to but is invisible to every quorum (§4.2.1).
    fn voting_peers(&self) -> Vec<PeerInfo> {
        self.config
            .others(&self.node_id)
            .filter(|m| m.voting)
            .map(PeerInfo::from)
            .collect()
    }

    /// How many votes make a quorum, counting voting members only.
    fn majority(&self) -> usize {
        self.config.majority()
    }

    /// Whether this node is itself a voter -- false while it is a learner
    /// catching up, and false once it has been removed from the cluster.
    /// Such a node replicates and serves reads but must never campaign.
    fn is_voter(&self) -> bool {
        self.config.is_voter(&self.node_id)
    }

    /// Re-read the active configuration from the log, and report whether
    /// this node's own right to vote changed as a result. Called whenever
    /// a `Config` entry may have been appended, truncated away, or
    /// brought in by a snapshot -- note *appended*, not committed, per
    /// Raft §4.1 (see `storage::RaftLog::current_config`).
    ///
    /// Panics on a storage failure, same as every other storage read on
    /// the event loop -- see the module doc.
    async fn reload_config(&mut self) -> bool {
        let was_voter = self.is_voter();
        let loaded = self
            .store
            .expect("read the cluster config", load_config)
            .await;
        // Nothing in the log (or in the compacted fallback) means no
        // configuration has ever been established here, so there's
        // nothing to switch to -- keep operating under what we have.
        if let Some(config) = loaded {
            self.config = config;
        }
        was_voter != self.is_voter()
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
            self.term_tx.send_replace(term);
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
        let term = self.current_term;
        let voted_for = self.voted_for.clone();
        self.store
            .expect("persist the raft term/vote state", move |log| {
                log.persist_term_state(term, voted_for.as_deref())
            })
            .await;
    }
}

/// What `Common::next_event` decided, before any role-specific handling.
enum Next {
    Handle(Event),
    /// A response carried a term higher than ours. `next_event` has
    /// already adopted it (Raft §5.1); whatever role asked has only to
    /// stop being that role.
    StepDown,
    /// The node is shutting down.
    Stop,
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
    cancel: &CancellationToken,
    store: Store,
    mut commit_rx: watch::Receiver<u64>,
    mut compact_rx: watch::Receiver<u64>,
    applied_tx: Arc<watch::Sender<u64>>,
) -> Task {
    Task::spawn(cancel, move |cancel| async move {
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                r = commit_rx.changed() => { if r.is_err() { return; } }
                r = compact_rx.changed() => { if r.is_err() { return; } }
            }
            let commit_seq = *commit_rx.borrow();
            let compact_seq = *compact_rx.borrow();

            match store.run(move |log| log.apply_committed(commit_seq)).await {
                Ok(()) => {
                    // Only ever moves forward -- `commit_seq` is itself
                    // monotonic (`Common::advance_commit`), so an
                    // out-of-order wakeup can't regress this.
                    applied_tx.send_replace(commit_seq);
                }
                Err(e) => {
                    eprintln!("apply_committed to seq {commit_seq} failed: {e}");
                    continue; // don't attempt to compact past what didn't apply
                }
            }

            if let Err(e) = store.run(move |log| log.compact(compact_seq)).await {
                eprintln!("compact to seq {compact_seq} failed: {e}");
            }
        }
    })
}

/// Runs forever, handing `Common` from one role to the next. Only one
/// role's event loop is ever active: the previous one has fully returned
/// (and torn down its background tasks) before the next one starts.
pub async fn run(mut common: Common) {
    let maintenance_task = spawn_storage_maintenance_task(
        common.cancel_token(),
        common.store.clone(),
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
            // Each role's own `shutdown` has already stopped and joined
            // that role's tasks by the time it hands `common` back, so
            // there is nothing left here but the node-wide maintenance
            // task.
            Role::Stopped => break,
        };
        role = next_role;
        common = next_common;
    }
    maintenance_task.stop().await;
    // Both a courtesy to anyone still holding a `RoleHandle` and a
    // necessity: it is what breaks the `LeaderHandle` reference cycle
    // that would otherwise pin this node's `Arc<RaftLog>` (and its
    // database lock) for the life of the process. See
    // `RoleHandle::Stopped`.
    common.publish_role(RoleHandle::Stopped);
    // Explicit rather than incidental: dropping `Common` is what closes
    // `event_tx`/`role_tx`/`commit_tx` and so releases any gRPC handler
    // still parked on one of them (`LeaderHandle::await_applied`, say),
    // letting the servers finish their own graceful shutdown. It is also
    // the last `Arc<RaftLog>` this module holds, so the database lock is
    // free the moment this function returns.
    drop(common);
}

/// Shared test infrastructure for this module and its `leader`/`candidate`/
/// `follower` submodules -- each submodule's own `#[cfg(test)] mod tests`
/// pulls these in with `use crate::raft::test_support::*;`.
#[cfg(test)]
pub(crate) mod test_support;

#[cfg(test)]
mod tests;
