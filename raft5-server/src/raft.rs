//! The Raft consensus state machine.
//!
//! # Roles
//!
//! Only one role's event loop runs at a time. `run` passes a `Common`
//! through a sequence of roles. Each role:
//! - takes `Common` in its `new`,
//! - starts its background tasks (replication for the leader, vote
//!   requests for the candidate, an election timer for the follower),
//! - publishes a `RoleHandle`, so the gRPC layer can see what is possible,
//! - runs its event loop until it decides to change role,
//! - in `shutdown`, stops its tasks and gives `Common` back.
//!
//! A role is fully stopped before the next one starts, so only one loop
//! reads `Common` at a time, and `Common` needs no lock.
//!
//! This file holds what all roles share: `Common`, `Event`, the handle
//! types, `PeerConnections` and the driver. Each role lives in its own
//! private submodule (`leader`, `candidate`, `follower`). A submodule sees
//! private items here, so the split is about file size, not
//! encapsulation.
//!
//! # Handles
//!
//! `LeaderHandle::append_writes` does not go through the event loop:
//! `storage::RaftLog::append` is safe to call concurrently, and waiting for
//! a commit is only watching a channel. So many client requests can wait
//! in `append_writes` without blocking the state machine's loop.
//!
//! `RaftPeerHandle` serves inbound RPCs. Every role must answer
//! AppendEntries and RequestVote, so this handle is not role-specific: it
//! is created once and stays valid for the life of the node. It does go
//! through the event loop: the gRPC handlers push an `Event` with a
//! `oneshot` reply channel and wait, so the current role decides the
//! answer.
//!
//! # Term and vote
//!
//! `current_term` and `voted_for` are persisted (`RaftLog::term_state`,
//! `persist_term_state`) through `Common::advance_term` and `vote_for`,
//! the only two places that change them. Raft is safe only if a node never
//! votes twice in one term, even across a restart, so every change is
//! durable before the node acts on it. A failed persist panics.
//!
//! Raft §5.1 says: "if an RPC request or response contains term T >
//! currentTerm, set currentTerm = T and convert to follower". This is
//! split in two:
//! - For a response, `Common::next_event` applies it once for all roles
//!   (see `Event::response_term`). Adopting the term is the whole job, and
//!   one central place means a new `Event` variant cannot skip it.
//! - For a request, each role answers differently before it steps down (a
//!   leader rejects the append, a follower accepts it, a candidate still
//!   evaluates the vote). So that half stays in each role's loop, next to
//!   the reply it builds.
//!
//! # Storage calls
//!
//! Every `storage::RaftLog` call from async code goes through
//! `store::Store`, which runs it on the blocking pool: fjall is
//! synchronous, and disk I/O on an executor thread can stall other tasks
//! on that thread. Nothing here calls `spawn_blocking` directly. For the
//! same reason, `handle_event` is `async fn` on every role, so that the one
//! event that touches storage (`AppendEntries`, via `apply_append_entries`)
//! can `.await` it.
//!
//! # Peer connections
//!
//! `Common::peer_conns` holds one gRPC client per peer (keyed by
//! `PeerInfo::id`), shared by all roles and elections, so RPCs do not pay
//! for a new connection each time. The clients use
//! `Endpoint::connect_lazy`: they dial on first use and re-dial by
//! themselves, so a failed call needs no cleanup, and getting a client is
//! an in-memory lookup that never blocks. See `PeerConnections`.
//!
//! # Elections
//!
//! Before a `Follower` (or a `Candidate` whose election timed out) starts
//! a real candidacy, which increments the term, it runs a `PreCandidate`
//! round: a non-binding pre-vote (Raft dissertation §9.6,
//! `RequestVoteRequest.dry_run`). It asks every peer "would you vote for me
//! at `current_term + 1`?", and neither side changes state. Only after a
//! majority says yes does it become a real `Candidate`. This stops a
//! partitioned node, which cannot win, from raising its term on every
//! timeout and disrupting a healthy cluster when it reconnects.
//!
//! `Common::election_attempt` counts failed cycles (pre-vote or real)
//! since this node last believed a leader existed. The retry timeout backs
//! off exponentially with it (`Timing::election_timeout_for_attempt`), so an
//! undecided cluster does not overload itself. It resets to 0 when a new
//! `Follower` starts (see `follower::Follower::new`). `PreCandidate` and
//! `Candidate` share most of this logic: see `candidate::Campaign`.
//!
//! # Timing
//!
//! All timing values come from one heartbeat, a per-cluster setting in the
//! replicated configuration (see `Timing`):
//! - The lease is `4 * heartbeat`.
//! - A follower's election timeout (the "is there really no leader?" wait)
//!   is one lease plus `fuzz(0, lease)`. See
//!   `follower::spawn_lease_expiration_watcher` for why these are two
//!   phases (one fixed, one random) and not one random draw.
//! - A campaign's retry timeout starts in the same `[lease, 2*lease)`
//!   range and backs off from there.
//!
//! # Leases
//!
//! Leader leases use `GetLease`, a separate RPC from `AppendEntries`,
//! because `AppendEntries` does real log work and can be slow, while a
//! lease probe must stay fast. Leases have two purposes:
//! - A leader that cannot keep a quorum of leases cannot prove it is safe
//!   to serve a linearizable read locally (see
//!   `LeaderHandle::has_read_quorum`, which linearizable and
//!   transactional reads check).
//! - A follower with a live lease refuses every vote request (real or
//!   pre-vote) for another node, so a leader that a majority can reach is
//!   never disrupted by an election.
//!
//! Losing the lease quorum does not make a leader step down. Only contact
//! that shows a higher term does that. Lease loss is a read-safety signal:
//! commit safety for writes has its own majority check in replication.
//!
//! # Cluster membership
//!
//! Membership is not start-up configuration. It lives in the log, as
//! `Config` entries that are replicated and log-matched like any other
//! (Raft §4). This has three effects:
//!
//! * A configuration takes effect on **append**, not on commit (§4.1).
//!   `Common::reload_config` runs wherever one may have appeared, and
//!   `storage::RaftLog::current_config` explains why waiting for commit
//!   would deadlock.
//! * Members are **voters or learners** (§4.2.1). All members get
//!   replication; only voters count toward a quorum, campaign, or hold a
//!   lease. A server joins as a learner and is promoted automatically when
//!   it keeps up: see `leader::Leader::review_learner_progress`.
//! * A node whose log has no configuration is in **no cluster**
//!   (`RoleHandle::Uninitialized`, §4.4). It answers RPCs and waits to be
//!   bootstrapped (`ClientHandle::bootstrap`) or replicated to. It never
//!   assumes it is its own cluster, because two nodes doing that would make
//!   two clusters.
//!
//! # Cancellation
//!
//! Every background task (role timers, leader replication and lease
//! probes, candidate vote requests, storage maintenance) is owned by a
//! `Task`, and each runs under a child of the node's root
//! `CancellationToken` (from `crate::serve`). `Task` enforces three rules:
//!
//! * **Nothing is detached.** `Task::stop` cancels and then joins.
//!   `JoinHandle::abort` leaves a task wherever its last poll stopped, so
//!   an `Arc<RaftLog>`, and with it the database lock, can outlive the
//!   node. The next node to open the same directory then fails.
//! * **Long waits are cancellable; storage calls are not.** Peer RPCs,
//!   heartbeat sleeps and sends into the event loop use
//!   `until_cancelled`. `Store` calls always run to completion: dropping a
//!   `spawn_blocking` does not stop it, and it keeps holding what it
//!   captured.
//! * **When `run` returns, the node is gone.** Every role has stopped and
//!   joined its tasks, the maintenance task is joined, and `Common` is
//!   dropped, which releases gRPC handlers waiting on its channels.
//!
//! # Limits
//!
//! - The leader-side §5.3 backoff is simplified (see
//!   `leader::Leader::spawn_replication`).
//! - Nothing triggers a snapshot by itself: a snapshot is sent only when a
//!   peer is behind the compaction floor.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

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

// Re-exported at the module root, for two reasons: these names are the
// public API of `raft` (`grpc` and `lib` use `raft::LeaderHandle`, not
// `raft::handles::LeaderHandle`), and each role module gets the whole set
// through `use super::*`.
pub use event::{ConfigChange, Event, RaftError};
pub use handles::{
    AppendError, ClientHandle, FollowerHandle, LeaderHandle, RaftPeerHandle, RoleHandle,
    leader_hint_of,
};
pub use peer::PeerInfo;

pub(crate) use event::Role;
use event::Transition;
/// The wire form of an `AppendEntries`, for the one test that sends it
/// over a real socket: see `grpc::tests`.
#[cfg(test)]
pub(crate) use leader::replication::append_entries_stream;
use peer::PeerConnections;
pub(crate) use peer::peer_endpoint;
pub(crate) use task::Task;
use task::{send_event, until_cancelled};

/// The heartbeat of a cluster that was not bootstrapped with (or later set
/// to) another value, and of a node with no configuration yet. Good on a
/// LAN; see `Timing` for when to raise it.
pub(crate) const DEFAULT_HEARTBEAT_MS: u64 = 50;

/// The allowed heartbeat range. The floor stops a typo from making every
/// peer a busy loop of probes. The ceiling stops one from making a dead
/// leader take most of a minute to detect (a follower waits up to
/// `2 * lease`, which is 8 heartbeats).
pub(crate) const HEARTBEAT_MS_RANGE: std::ops::RangeInclusive<u64> = 10..=10_000;

/// Whether a cluster may use `ms` as its heartbeat. Checked where a value
/// is requested (bootstrap, `ConfigChange::SetHeartbeat`), so an operator
/// learns about a bad value instead of having it silently clamped.
pub(crate) fn check_heartbeat_ms(ms: u64) -> Result<(), &'static str> {
    if HEARTBEAT_MS_RANGE.contains(&ms) {
        Ok(())
    } else {
        Err("heartbeat must be between 10ms and 10000ms")
    }
}

/// Every timeout and interval of the consensus core, all derived from one
/// heartbeat (see the module doc).
///
/// The heartbeat is a per-cluster setting in the replicated
/// `ClusterConfig` (`heartbeat_millis`), so it takes effect on every node
/// when the entry that sets it is appended, like a membership change.
/// Raise it when nodes are far apart: the light RPC timeout is only
/// `2 * heartbeat`, so a heartbeat well below the peer round trip (plus a
/// TLS handshake on a cold connection) makes lease probes and votes time
/// out, leases lapse, and elections repeat.
///
/// `Copy` and cheap: the event loop publishes the current value through a
/// `watch` (see `Common::timing_tx`). Long-lived tasks read it again on
/// each iteration, so a change applies on the next tick without
/// restarting anything.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Timing {
    heartbeat_ms: u64,
}

impl Default for Timing {
    fn default() -> Self {
        Self::from_millis(DEFAULT_HEARTBEAT_MS)
    }
}

impl Timing {
    /// `ms`, clamped into `HEARTBEAT_MS_RANGE`. 0 is an unset proto field and
    /// means the default.
    pub(crate) fn from_millis(ms: u64) -> Self {
        let heartbeat_ms = if ms == 0 {
            DEFAULT_HEARTBEAT_MS
        } else {
            ms.clamp(*HEARTBEAT_MS_RANGE.start(), *HEARTBEAT_MS_RANGE.end())
        };
        Self { heartbeat_ms }
    }

    /// The timing that `config` asks for.
    pub(crate) fn of(config: &ClusterConfig) -> Self {
        Self::from_millis(config.heartbeat_millis)
    }

    pub(crate) fn heartbeat_ms(self) -> u64 {
        self.heartbeat_ms
    }

    /// How often the leader ticks, probes leases, and retries an unreachable
    /// peer.
    fn heartbeat(self) -> Duration {
        Duration::from_millis(self.heartbeat_ms)
    }

    /// How long an idle, caught-up replication task waits before it sends its
    /// peer an empty `AppendEntries`. This is not a liveness heartbeat (leases
    /// do that) but a safety net: replication otherwise runs only when the log
    /// or a watermark changes, so a follower that silently restarted would go
    /// unnoticed until the next write.
    fn idle_replication_interval(self) -> Duration {
        self.heartbeat() * 10
    }

    /// The lease duration a request asks for (see
    /// `RaftPeerHandle::get_lease`). A follower refuses other vote requests
    /// for this long after its last confirmed contact with the leader, and a
    /// leader counts a peer's grant toward its lease quorum for this long. The
    /// two agree because the leader names the duration in each request: see
    /// `GetLeaseRequest`.
    fn lease_ms(self) -> u64 {
        self.heartbeat_ms * 4
    }

    fn lease(self) -> Duration {
        Duration::from_millis(self.lease_ms())
    }

    /// The timeout for a cheap peer RPC with no disk I/O (`RequestVote`,
    /// `GetLease`), including the connect if no client is cached. Tight,
    /// because these should be fast: a slow reply is as good as none.
    fn lightweight_rpc_timeout(self) -> Duration {
        self.heartbeat() * 2
    }

    /// The longest lease a request may ask for: the lease of a cluster at the
    /// top of `HEARTBEAT_MS_RANGE`. A follower clamps to this, so no leader,
    /// however misconfigured, can block its elections for longer.
    fn max_lease() -> Duration {
        Timing::from_millis(*HEARTBEAT_MS_RANGE.end()).lease()
    }

    /// The shortest lease a request may ask for.
    fn min_lease() -> Duration {
        Timing::from_millis(*HEARTBEAT_MS_RANGE.start()).lease()
    }
}

/// The timeout for `AppendEntries`. Unlike the RPCs above, it does real
/// log work on the follower and can take a while under load. Generous on
/// purpose: an earlier retry does not help a follower whose disk is the
/// bottleneck; it only adds more work.
///
/// Sized against `MAX_REPLICATION_BATCH_BYTES`: a full batch must fit in
/// this time on any link worth using. At 16 MiB that needs about
/// 550 KiB/s. Keep this relation if either number changes: a batch that
/// cannot finish in time never commits, and nothing after it commits
/// either.
const APPEND_ENTRIES_RPC_TIMEOUT: Duration = Duration::from_secs(30);

/// How long replication to one peer may keep failing before it is logged
/// as a stall, not a retry. After this long the likely cause is no longer
/// "the peer is restarting" but "this will never succeed", most often a
/// batch too large for the link, which blocks the commit index with no
/// other symptom.
const REPLICATION_STALL_WARNING: Duration = Duration::from_secs(60);

// --- the replication size cap chain -----------------------------------
//
// These limits depend on each other. If the leader ever sends an entry the
// follower refuses, the entry is already in the log, the leader sends it
// forever, and the cluster never commits again. `txn::MAX_TRANSACTION_BYTES`
// exists to prevent this. So the limits are derived from each other, and
// their order is asserted below.
//
// Budgets are in encoded bytes, and the layers encode differently: `txn`
// counts raw key and value bytes, a `log` row is postcard, the wire is
// protobuf. The gap between `txn::MAX_TRANSACTION_BYTES` and
// `MAX_ENTRY_BYTES` absorbs the difference.

/// The largest log entry that may exist. `txn` enforces it, before
/// anything reaches the log, which is the only place that can. Everything
/// below is sized so an entry this large always replicates.
pub const MAX_ENTRY_BYTES: usize = 16 * 1024 * 1024;

/// How many bytes of log entries one replication round may carry.
///
/// A soft budget: `RaftLog::entries_in_range_capped` always returns at
/// least one entry, even if that entry alone is over the budget, so a
/// large entry makes progress. That is why this must be at least
/// `MAX_ENTRY_BYTES`: the "at least one" rule is safe only if no entry is
/// larger than the budget.
pub const MAX_REPLICATION_BATCH_BYTES: usize = MAX_ENTRY_BYTES;

/// The receiver's hard limit on one `AppendEntries` body. Above
/// `MAX_REPLICATION_BATCH_BYTES`, so protobuf framing around a full batch
/// never puts an honest leader over the limit.
pub const MAX_APPEND_ENTRIES_BYTES: usize = MAX_REPLICATION_BATCH_BYTES + 4 * 1024 * 1024;

// A compile-time check, not a test: breaking this order should fail the
// build. The chain is 10 MiB < 16 MiB <= 16 MiB < 20 MiB.
const _: () = assert!(
    crate::txn::MAX_TRANSACTION_BYTES < MAX_ENTRY_BYTES,
    "a transaction must leave room for the framing its log entry adds, or \
     the leader can append an entry it can never replicate"
);
const _: () = assert!(
    MAX_ENTRY_BYTES <= MAX_REPLICATION_BATCH_BYTES,
    "a single entry must always fit in one replication round, or the \
     'always send at least one entry' rule emits batches over the budget"
);
const _: () = assert!(
    MAX_REPLICATION_BATCH_BYTES < MAX_APPEND_ENTRIES_BYTES,
    "the receiver must accept anything the leader is willing to send, or \
     an entry is refused forever and the cluster stops committing"
);

/// The timeout for a full `InstallSnapshot` transfer. Longer than for
/// `AppendEntries`, because it moves the whole current state, not a batch
/// of new entries.
const SNAPSHOT_RPC_TIMEOUT: Duration = Duration::from_secs(7200);

/// How much of an encoded entry batch goes into each
/// `AppendEntriesChunk`.
///
/// This bounds one gRPC message only; `MAX_REPLICATION_BATCH_BYTES` bounds
/// the batch. Small messages stay well under any decode limit, so the
/// batch limit is set by the cap chain on purpose, not inherited from
/// transport defaults.
const APPEND_ENTRIES_CHUNK_SIZE: usize = 1024 * 1024;

/// A random value in `[0, max)`, for election-timeout jitter, without a
/// `rand` dependency. Each `RandomState` gets a random seed (that is how
/// `HashMap` resists hash flooding), so a new hasher's output is already
/// unpredictable.
fn random_jitter_millis(max: u64) -> u64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    RandomState::new().build_hasher().finish() % max
}

/// How long the storage maintenance task waits before it retries a failed
/// apply. Long enough that a failing disk does not spin the blocking pool,
/// short enough that a brief failure costs a commit a short pause, not the
/// rest of the term.
const APPLY_RETRY_DELAY: Duration = Duration::from_millis(100);

/// How often the version GC wakes when it has no work.
///
/// Short, because it reclaims every superseded version of every
/// overwritten key: under heavy writes, the store grows until it runs. A
/// pass that stops on its budget does not wait: it starts again at once.
const VERSION_GC_INTERVAL: Duration = Duration::from_secs(60);

/// How many version rows one GC pass visits before it yields.
///
/// Bounds how long a blocking-pool worker is held and how large the write
/// batch gets, like `WRITE_CHUNK`. A pass always finishes the key it is in,
/// so the limit is approximate.
const VERSION_GC_BUDGET: usize = 50_000;

/// The longest backoff for a campaign that keeps failing, or `4 * lease`
/// if that is longer, so a cluster with a long heartbeat still gets a
/// jitter window on its first attempts instead of all nodes retrying at
/// exactly the cap.
const MAX_ELECTION_BACKOFF_MS: u64 = 30_000;

impl Timing {
    /// The jitter window for the `attempt`-th failed campaign cycle in a row
    /// (`PreCandidate` or `Candidate` waiting for votes) since this node last
    /// believed a leader existed (see `Common::election_attempt`).
    /// - Attempt 0 is `[lease, 2*lease)`.
    /// - Each attempt doubles it, up to `MAX_ELECTION_BACKOFF_MS`.
    ///
    /// So an undecided cluster, or a node that cannot reach anyone, backs off
    /// instead of retrying at the same short interval forever.
    ///
    /// A `Follower` does not use this to decide its leader is gone. It uses
    /// the two-phase wait in `follower::spawn_lease_expiration_watcher`, which
    /// also totals `[lease, 2*lease)` but for different reasons (a fixed
    /// detection phase plus a random settle phase).
    fn election_timeout_for_attempt(self, attempt: u32) -> Duration {
        let lease_ms = self.lease_ms();
        let cap = MAX_ELECTION_BACKOFF_MS.max(lease_ms.saturating_mul(4));
        // Cap the shift itself, not only the result, so a huge attempt count
        // cannot panic. 2^20 is already far past the millisecond cap, so higher
        // values change nothing.
        let scale = 1u64 << attempt.min(20);
        let min = lease_ms.saturating_mul(scale).min(cap);
        let max = lease_ms.saturating_mul(2).saturating_mul(scale).min(cap);
        let jitter_range = max.saturating_sub(min);
        let jitter = if jitter_range == 0 {
            0
        } else {
            random_jitter_millis(jitter_range)
        };
        Duration::from_millis(min + jitter)
    }
}

/// Push `ElectionTimeout` if nothing happens in time, so the event loop
/// can start or restart an election. `Follower` (waiting for a leader)
/// and `PreCandidate`/`Candidate` (waiting for votes) both use this.
///
/// Randomized, as in the Raft paper. Two nodes with the same fixed timeout
/// tend to become candidates at the same moment and then split the vote on
/// every attempt: neither's `RequestVote` looks newer than its own, so both
/// refuse and restart together. Jitter breaks the tie within a few rounds.
/// `attempt` widens the window after repeated failures; a new `Follower`
/// passes 0.
fn spawn_election_timer(
    cancel: &CancellationToken,
    timing: Timing,
    attempt: u32,
    event_tx: mpsc::Sender<Event>,
) -> Task {
    Task::spawn(cancel, move |cancel| async move {
        let timeout = timing.election_timeout_for_attempt(attempt);
        if until_cancelled(&cancel, tokio::time::sleep(timeout))
            .await
            .is_some()
        {
            send_event(&cancel, &event_tx, Event::ElectionTimeout).await;
        }
    })
}

/// See `Common::notify_appended`. A free function so `LeaderHandle` can
/// call it with only the sender.
fn notify_appended(log_tx: &watch::Sender<u64>, seq: u64) {
    log_tx.send_if_modified(|latest| {
        let advanced = seq > *latest;
        if advanced {
            *latest = seq;
        }
        advanced
    });
}

/// This node's `(last_log_index, last_log_term)`. Used to fill in an
/// outgoing `RequestVoteRequest` and to run the §5.4.1 up-to-date check on
/// an incoming one. `(0, 0)` for an empty log, as in Raft's "before the
/// first entry" convention.
async fn last_log_index_and_term(store: &Store) -> (u64, u64) {
    store
        .expect("read the last log entry", RaftLog::last_entry)
        .await
        .unwrap_or((0, 0))
}

/// The term of the entry at `seq`, or `None` if there is no entry there
/// (any more). An async wrapper over `RaftLog::term_at`.
async fn term_at(store: &Store, seq: u64) -> Option<u64> {
    store
        .expect("read the term at a log index", move |log| log.term_at(seq))
        .await
}

/// Evaluate a pre-vote (`RequestVoteRequest.dry_run == true`) without
/// changing any state: no term change, no `voted_for`, no timer reset.
/// Returns whether a real `RequestVote` at `req.term` would be granted.
///
/// It does not check `voted_for`, because a vote is always scoped to
/// `current_term`: `Common::advance_term` resets it when it raises the
/// term, and `vote_for` runs only when `current_term` equals the voted
/// term. So a real vote exists only at a term `<= common.current_term`. A
/// dry run that passes the `req.term > common.current_term` check asks
/// about a later term, so no vote can conflict. Two candidates can both
/// pass pre-vote against the same peer, which is fine because pre-vote is
/// not binding: the `voted_for` check in the real vote enforces "at most
/// one winner per term".
async fn evaluate_dry_run_vote(common: &Common, req: &RequestVoteRequest) -> bool {
    if req.term <= common.current_term {
        return false;
    }
    let (our_index, our_term) = last_log_index_and_term(&common.store).await;
    (req.last_log_term, req.last_log_index) >= (our_term, our_index)
}

/// Evaluate and apply a real `RequestVote` (§5.4.1). The vote is granted
/// if:
/// - the term is at least the current term,
/// - this node has not voted for someone else in this term,
/// - the candidate's log is at least as up to date as ours.
///
/// If `req.term` is higher, `common`'s term advances first, and a granted
/// vote is persisted. Returns whether the vote was granted.
///
/// All roles share this. `Follower` and `PreCandidate` need the full check,
/// because either can already have a `voted_for` in this term (a
/// `PreCandidate` never sets one; it keeps the `Follower`'s). `Candidate`
/// and `Leader` always have `voted_for == Some(self)` in their own term,
/// so they deny at `req.term == current_term`. At `req.term >
/// current_term`, the log check still withholds the vote from a candidate
/// whose log is behind, which the term check alone does not guarantee.
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

/// The cluster configuration in `log`, decoded. `None` for a store that
/// has never seen one.
fn load_config(log: &RaftLog) -> storage::Result<Option<ClusterConfig>> {
    match log.current_config()? {
        Some(bytes) => ClusterConfig::from_bytes(&bytes)
            .map(Some)
            .map_err(|_| StorageError::Corrupt("cluster config in the log failed to decode")),
        None => Ok(None),
    }
}

/// A configuration of `node_id` plus `peers`, all voting: an implicit
/// bootstrap.
///
/// Test-only, on purpose: Raft §4.4 warns against implicit bootstrap,
/// because two nodes started with different peer lists would each form a
/// cluster and believe it is the whole one. Real nodes start uninitialized
/// and are bootstrapped explicitly.
#[cfg(test)]
fn seed_config(node_id: &str, peers: &[PeerInfo]) -> ClusterConfig {
    use crate::pb::raft5::cluster::ClusterMember;
    let member = |id: &str, address: &str| ClusterMember {
        server_id: id.to_string(),
        address: address.to_string(),
        client_address: String::new(),
        voting: true,
    };
    ClusterConfig {
        members: std::iter::once(member(node_id, ""))
            .chain(peers.iter().map(|p| member(&p.id, &p.address)))
            .collect(),
        heartbeat_millis: DEFAULT_HEARTBEAT_MS,
    }
}

/// State shared by all roles, passed from one role's `shutdown` into the
/// next role's `new`.
///
/// Every field is private. `raft` is a `pub` module, so a `pub` field
/// would put this node's event channel, watermark senders and role
/// publisher on the crate's public API, where anything could drive them
/// behind the state machine's back. The role modules are inside `raft`, so
/// they see every field; they are the intended users.
pub struct Common {
    node_id: String,
    /// The cluster configuration this node runs under: its peers and which of
    /// them vote. Read from the log (see `storage::RaftLog::current_config`),
    /// not from a flag, and read again when a `Config` entry is appended or
    /// rolled back. On append, not on commit, per Raft §4.1.
    config: ClusterConfig,
    /// `Timing::of(&config)`, published when `config` changes it. Long-lived
    /// tasks read it on every iteration, so a heartbeat change reaches a
    /// running leader's probes and replication without a restart.
    /// `RaftPeerHandle` reads it for a lease request from an old leader that
    /// names no duration.
    timing_tx: watch::Sender<Timing>,
    store: Store,
    peer_conns: Arc<PeerConnections>,

    /// Changed only through `advance_term` and `vote_for`, which also persist
    /// the change. See the module doc for why storage must come first.
    current_term: u64,
    voted_for: Option<String>,
    /// A copy of `current_term`, published on every `advance_term`, so the
    /// fast path of `RaftPeerHandle::get_lease` can read it without the event
    /// loop.
    term_tx: watch::Sender<u64>,

    /// Failed election cycles in a row (pre-vote or real) since this node
    /// last believed a leader existed. See the module doc. Not persisted: it is
    /// only a backoff hint, so losing it on restart is harmless.
    election_attempt: u32,

    /// When this node stops refusing vote requests from nodes other than its
    /// current leader. See the module doc on leases.
    ///
    /// `None` means no lease was ever granted, which differs from "expired":
    /// `follower::spawn_lease_expiration_watcher` and
    /// `follower::Follower::has_active_lease` treat a never-granted lease
    /// differently, and a past `Instant` cannot mean both.
    ///
    /// `Arc`-wrapped because `RaftPeerHandle::get_lease` writes to it directly,
    /// outside the event loop on purpose (see that method).
    lease_expires_at_tx: Arc<watch::Sender<Option<Instant>>>,
    lease_expires_at_rx: watch::Receiver<Option<Instant>>,

    /// The highest seq known to be committed. It outlives a single role (a
    /// follower also applies the leader's `leader_commit` here), so it lives
    /// in `Common`, not in `Leader`.
    commit_tx: watch::Sender<u64>,

    /// The highest seq this leader has appended to its own log. Replication
    /// tasks wait on it, so a new entry goes out at once instead of on the
    /// next tick. Only leader-side append sites publish here. `Arc`-wrapped so
    /// `LeaderHandle`, which appends client writes outside the event loop, can
    /// publish too.
    log_tx: Arc<watch::Sender<u64>>,

    /// The highest seq known to be replicated to every peer (not only a
    /// majority), so it is safe to compact below it in any role. A leader
    /// computes it from `Leader::peer_match_index`; a follower takes the value
    /// from `AppendEntries.compact_seq`. Used by
    /// `spawn_storage_maintenance_task`.
    compact_tx: watch::Sender<u64>,

    /// How far `RaftLog::apply_committed` has brought `latest`. Only
    /// `spawn_storage_maintenance_task` publishes it, after that call returns.
    ///
    /// `LeaderHandle::append_writes` waits on this (see its doc).
    /// `commit_tx` only says an entry is safely replicated, not that `latest`
    /// shows it: the maintenance task is a separate `watch` subscriber with no
    /// ordering against other subscribers of `commit_tx`.
    ///
    /// `Arc`-wrapped so `Common` can hand out new `subscribe()`s (one per
    /// `Leader::new`, because every election builds a new `LeaderHandle`)
    /// after the sender has moved into the maintenance task.
    applied_tx: Arc<watch::Sender<u64>>,

    /// Where peer RPCs (from `RaftPeerService`) and internal timers arrive.
    /// Shared by all roles, so queued events survive a role change.
    event_rx: mpsc::Receiver<Event>,
    event_tx: mpsc::Sender<Event>,

    role_tx: watch::Sender<RoleHandle>,

    /// This node's root cancellation token, from whoever started it
    /// (`crate::serve`). Every background task runs under a child of it, so
    /// cancelling it stops the whole node. See `Task` and the module doc.
    cancel: CancellationToken,
}

/// What `Common::new` returns: the state machine, and the handles a
/// caller needs to observe and drive it.
///
/// A struct, not a tuple, on purpose. `commit_rx` and `compact_rx` are
/// both `watch::Receiver<u64>` with very different meanings. As tuple
/// elements they could be swapped at a call site and still compile, giving
/// a node that compacts at its commit watermark: the exact bug that
/// `RaftLog::compact`'s clamp exists to survive. Named fields make the
/// mistake impossible.
pub struct NodeHandles {
    pub common: Common,
    pub role_rx: watch::Receiver<RoleHandle>,
    pub peer_handle: RaftPeerHandle,
    pub commit_rx: watch::Receiver<u64>,
    pub compact_rx: watch::Receiver<u64>,
}

impl Common {
    /// Membership comes only from `log`. A store with no configuration gives
    /// an empty one, so the node starts uninitialized: it has no vote, never
    /// campaigns, and waits to be bootstrapped or replicated to. See
    /// `RoleHandle::Uninitialized`.
    // For the same reason as the role constructors: the caller needs the
    // handles as well as `Common`. A separate second call would let a caller
    // hold a `Common` that nothing can observe.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        node_id: String,
        log: Arc<RaftLog>,
        cancel: CancellationToken,
    ) -> storage::Result<NodeHandles> {
        Self::build(node_id, log, ClusterConfig::default(), cancel)
    }

    /// `new`, with a configuration given directly instead of read from the
    /// log, for tests that want a cluster shape without a bootstrap. A
    /// configuration in the log still wins.
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
        let (log_tx, _log_rx) = watch::channel(0);
        let log_tx = Arc::new(log_tx);
        // Drop the first receiver. `Common` hands out new ones later through
        // `applied_tx.subscribe()` (once per `Leader::new`), which works as long as
        // the sender is alive.
        let (applied_tx, _applied_rx) = watch::channel(0);
        let applied_tx = Arc::new(applied_tx);
        let (term_tx, term_rx) = watch::channel(current_term);
        // `None`: no lease yet. `has_active_lease` treats this as "not blocked"
        // (a new node that has contacted nobody must not refuse votes), and
        // `spawn_lease_expiration_watcher` treats it as "give the same full grace
        // period as a new grant". A past `Instant` would mean neither, which is
        // why this is `Option<Instant>`.
        let (lease_expires_at_tx, lease_expires_at_rx) = watch::channel(None);
        let lease_expires_at_tx = Arc::new(lease_expires_at_tx);
        let (event_tx, event_rx) = mpsc::channel(256);
        let (role_tx, role_rx) = watch::channel(RoleHandle::Starting);
        let (timing_tx, timing_rx) = watch::channel(Timing::of(&config));

        let peer_handle = RaftPeerHandle {
            event_tx: event_tx.clone(),
            role_rx: role_tx.subscribe(),
            term_rx,
            timing_rx,
            lease_expires_at_tx: Arc::clone(&lease_expires_at_tx),
        };

        let common = Self {
            node_id,
            config,
            timing_tx,
            store: Store::new(log),
            peer_conns: Arc::new(PeerConnections::default()),
            current_term,
            voted_for,
            term_tx,
            election_attempt: 0,
            lease_expires_at_tx,
            lease_expires_at_rx,
            commit_tx,
            log_tx,
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

    /// Publish that entries up to `seq` exist in this leader's log, to wake
    /// replication. Keeps the maximum, because concurrent client appends can
    /// finish in any order.
    fn notify_appended(&self, seq: u64) {
        notify_appended(&self.log_tx, seq);
    }

    /// Raise `commit_tx` to `new_value` if that is higher. Never lowers it, in
    /// case messages are processed out of order.
    ///
    /// `send_replace`, not `send`, on purpose. Every `watch` in `Common` is
    /// state that code reads back with `borrow()` (this comparison, the
    /// catch-up gate in `Leader::propose_config`, `step_down_if_removed`), not
    /// only a notification. `send` fails and keeps the old value when the last
    /// receiver is gone, so a watermark could silently stop moving while code
    /// still trusts it. `send_replace` always stores.
    fn advance_commit(&self, new_value: u64) {
        if new_value > *self.commit_tx.borrow() {
            self.commit_tx.send_replace(new_value);
        }
    }

    /// Like `advance_commit`, for the "replicated to every peer, safe to
    /// compact" watermark.
    fn advance_compact(&self, new_value: u64) {
        if new_value > *self.compact_tx.borrow() {
            self.compact_tx.send_replace(new_value);
        }
    }

    /// A handle for the client-facing service. Take it before `run` consumes
    /// `Common`.
    pub fn client_handle(&self) -> ClientHandle {
        ClientHandle {
            event_tx: self.event_tx.clone(),
            role_rx: self.role_tx.subscribe(),
        }
    }

    /// Dial every peer over mTLS instead of plaintext. Set it before `run`
    /// consumes `Common`, so no plaintext clients are cached yet.
    pub fn set_peer_tls(&mut self, tls: tonic::transport::ClientTlsConfig) {
        self.peer_conns = Arc::new(PeerConnections::new(Some(tls)));
    }

    fn publish_role(&self, handle: RoleHandle) {
        let previous_handle = self.role_tx.send_replace(handle);
        if previous_handle.to_string() != self.role_tx.borrow().to_string() {
            info!("role changed: {}", *self.role_tx.borrow());
        }
    }

    /// The next thing this node's event loop should do.
    ///
    /// Every role comes through here, which gives two benefits:
    /// - Cancelling the node stops it at once, not at its next timer.
    /// - Raft §5.1's term rule is applied once for all roles, not in each one
    ///   (see `Event::response_term`).
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

    /// A parent for a role's background tasks, so cancelling this node
    /// cancels them too. See `Task::spawn`.
    fn cancel_token(&self) -> &CancellationToken {
        &self.cancel
    }

    /// Every other member of the current configuration: everyone this node
    /// replicates to and probes, learners included.
    fn peers(&self) -> Vec<PeerInfo> {
        self.config
            .others(&self.node_id)
            .map(PeerInfo::from)
            .collect()
    }

    /// The other voting members: who a candidate asks for votes, and whose
    /// leases a leader counts toward read quorum. A learner gets replication
    /// but counts in no quorum (§4.2.1).
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

    /// Whether this node is a voter. False while it is a learner that is
    /// catching up, and false once it is removed from the cluster. Such a node
    /// replicates and serves reads but must never campaign.
    fn is_voter(&self) -> bool {
        self.config.is_voter(&self.node_id)
    }

    /// Read the active configuration from the log again, and report whether
    /// this node's own right to vote changed. Called wherever a `Config` entry
    /// may have been appended, truncated, or brought in by a snapshot.
    /// Appended, not committed, per Raft §4.1 (see
    /// `storage::RaftLog::current_config`).
    ///
    /// Panics on a storage failure, like every storage read on the event loop:
    /// see the module doc.
    async fn reload_config(&mut self) -> bool {
        let was_voter = self.is_voter();
        let loaded = self
            .store
            .expect("read the cluster config", load_config)
            .await;
        // No configuration in the log (or in the compacted fallback) means none
        // was ever set here, so there is nothing to switch to. Keep the current
        // one.
        if let Some(config) = loaded {
            let timing = Timing::of(&config);
            self.config = config;
            if *self.timing_tx.borrow() != timing {
                info!("heartbeat is now {}ms", timing.heartbeat_ms());
                self.timing_tx.send_replace(timing);
            }
        }
        was_voter != self.is_voter()
    }

    /// The timing this node runs at. See `Timing`.
    fn timing(&self) -> Timing {
        *self.timing_tx.borrow()
    }

    /// Advance to a higher term and discard any vote already cast. Raft scopes
    /// `voted_for` per term: a candidate in a new term must not be refused
    /// because of last term's vote. Does nothing if `term` is not higher.
    /// Where practical, callers check `term > current_term` first, to skip the
    /// persist `.await` entirely.
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

    /// Panics on a storage failure instead of continuing with an unpersisted
    /// term or vote: after a crash, that could mean two votes in one term,
    /// which breaks Raft safety, not only a lost write.
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

/// What `Common::next_event` decided, before any role handles it.
enum Next {
    Handle(Event),
    /// A response carried a higher term. `next_event` has already adopted it
    /// (Raft §5.1); the role only has to stop being that role.
    StepDown,
    /// The node is shutting down.
    Stop,
}

// --- Driver ----------------------------------------------------------------

/// Collect, on a timer, versions that no read can select.
///
/// A separate task, not another branch of
/// `spawn_storage_maintenance_task`, for three reasons:
/// - That loop is driven by changes to the commit and compaction
///   watermarks. A tick there would make an idle cluster run
///   `apply_committed` and `compact` for no reason.
/// - Its failure policy (retry the same seq, skip that cycle's
///   compaction) has nothing to do with GC, and neither should be able to
///   delay the other.
/// - GC has its own budget, cursor and log line, which are easier to read
///   apart.
///
/// Runs on every node in every role, with no coordination: it removes only
/// rows no read could select, so two nodes may hold different amounts of
/// garbage with no visible difference. See `storage::gc`.
fn spawn_version_gc_task(cancel: &CancellationToken, store: Store) -> Task {
    Task::spawn(cancel, move |cancel| async move {
        loop {
            let started = Instant::now();
            let stats = match store.run(|log| log.gc_versions(VERSION_GC_BUDGET)).await {
                Ok(stats) => stats,
                Err(e) => {
                    // Logged, never fatal, as in the maintenance task: uncollected garbage
                    // costs space, and nothing depends on this having run.
                    warn!("version gc failed, retrying on the next tick: {e}");
                    tokio::select! {
                        biased;
                        () = cancel.cancelled() => return,
                        () = tokio::time::sleep(VERSION_GC_INTERVAL) => continue,
                    }
                }
            };

            if stats.rows_removed > 0 || !stats.done {
                info!(
                    rows_visited = stats.rows_visited,
                    rows_removed = stats.rows_removed,
                    done = stats.done,
                    gc_ms = started.elapsed().as_millis(),
                    "version gc pass"
                );
            }

            // A pass that stopped on its budget has more work, so it starts again at
            // once instead of sleeping.
            if !stats.done {
                if cancel.is_cancelled() {
                    return;
                }
                continue;
            }

            tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                () = tokio::time::sleep(VERSION_GC_INTERVAL) => {}
            }
        }
    })
}

/// Storage maintenance. Each time a watermark advances:
/// - apply newly committed entries to `latest` (see
///   `RaftLog::apply_committed`; `latest` must never show an uncommitted
///   write, because a leader change could still roll it back),
/// - publish `applied_tx`,
/// - compact `log` up to `compact_seq`.
///
/// The order matters. `compact` removes `log` entries, and removing them
/// before they are applied would lose the data. (`RaftLog::compact` also
/// clamps itself to the applied watermark as a second line of defense.)
/// `applied_tx` is published before `compact` runs, because
/// `LeaderHandle::append_writes` needs to know that `latest` is current,
/// and a slow or failing `compact` must not delay that signal.
///
/// It reacts to both `commit_tx` and `compact_tx`: a follower's
/// `compact_seq` can advance without its `commit_tx` (see
/// `Follower::apply_append_entries`), so watching one channel could stall
/// the other's progress.
///
/// Started once per node, for every role: this is storage upkeep, not
/// state-machine behavior, so it does not start and stop with roles.
///
/// A failure is logged, not a panic (unlike a failed term persist). Falling
/// behind does not corrupt anything or break safety: `latest` is briefly
/// stale and the log is larger than needed.
///
/// It must keep trying, though, so a failed apply sleeps and retries the
/// same seq instead of waiting for the watermarks again. A quiet cluster
/// changes no watermark, so waiting would mean no retry at all, and every
/// write and transaction commit waits on `applied_tx`. A commit that never
/// hears back keeps its locks for the rest of the term.
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

            let apply_started = Instant::now();
            match store.run(move |log| log.apply_committed(commit_seq)).await {
                Ok(()) => {
                    // Only moves forward: `commit_seq` itself never goes back
                    // (`Common::advance_commit`), so a wakeup out of order cannot lower this.
                    applied_tx.send_replace(commit_seq);
                }
                Err(e) => {
                    warn!(
                        "apply_committed to seq {commit_seq} failed, retrying in \
                         {APPLY_RETRY_DELAY:?}: {e}"
                    );
                    // Do not compact past what did not apply, and do not go back to waiting on
                    // a watermark that may never move again. `select!`, not a plain sleep, so
                    // the backoff does not delay a shutdown.
                    tokio::select! {
                        biased;
                        () = cancel.cancelled() => return,
                        () = tokio::time::sleep(APPLY_RETRY_DELAY) => {}
                    }
                    continue;
                }
            }

            let apply_ms = apply_started.elapsed().as_millis();

            let compact_started = Instant::now();
            if let Err(e) = store.run(move |log| log.compact(compact_seq)).await {
                warn!("compact to seq {compact_seq} failed: {e}");
            }

            // Log both halves of the cycle: a commit that waits to be applied is
            // queued behind the previous cycle's work, including its compaction,
            // which nothing else waits for. A large `compact_ms` next to a small
            // `apply_ms` is what "compaction delays commits" looks like.
            info!(
                applied_to = commit_seq,
                apply_ms,
                compact_ms = compact_started.elapsed().as_millis(),
                "storage maintenance cycle"
            );
        }
    })
}

/// Run forever, passing `Common` from one role to the next. Only one
/// role's event loop is active: the previous one has fully returned, and
/// stopped its background tasks, before the next one starts.
pub async fn run(mut common: Common) {
    let maintenance_task = spawn_storage_maintenance_task(
        common.cancel_token(),
        common.store.clone(),
        common.commit_tx.subscribe(),
        common.compact_tx.subscribe(),
        Arc::clone(&common.applied_tx),
    );
    let version_gc_task = spawn_version_gc_task(common.cancel_token(), common.store.clone());
    let mut role = Role::Follower;
    loop {
        let (next_role, next_common) = match role {
            Role::Follower => Follower::new(common).await,
            Role::PreCandidate => PreCandidate::new(common).await,
            Role::Candidate => Candidate::new(common).await,
            Role::Leader => Leader::new(common).await,
            // Each role's `shutdown` has already stopped and joined its tasks before
            // it gives `common` back, so only the node-wide maintenance task is left.
            Role::Stopped => break,
        };
        role = next_role;
        common = next_common;
    }
    maintenance_task.stop().await;
    version_gc_task.stop().await;
    // A courtesy to anyone who still holds a `RoleHandle`, and a necessity:
    // it breaks the `LeaderHandle` reference cycle that would otherwise keep
    // this node's `Arc<RaftLog>` (and its database lock) alive for the life of
    // the process. See `RoleHandle::Stopped`.
    common.publish_role(RoleHandle::Stopped);
    // Explicit on purpose: dropping `Common` closes `event_tx`, `role_tx` and
    // `commit_tx`, which releases any gRPC handler waiting on one of them (for
    // example `LeaderHandle::await_applied`), so the servers can finish their
    // graceful shutdown. It is also the last `Arc<RaftLog>` this module holds,
    // so the database lock is free when this function returns.
    drop(common);
}

/// Shared test helpers for this module and its `leader`, `candidate` and
/// `follower` submodules. Each submodule's tests use
/// `use crate::raft::test_support::*;`.
#[cfg(test)]
pub(crate) mod test_support;

#[cfg(test)]
mod tests;
