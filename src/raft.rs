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
//! This is a skeleton: replication logic (the leader side of AppendEntries)
//! is still simplified and marked with TODOs. Log-matching (the follower
//! side, in `Follower::apply_append_entries`) and the candidate's voting
//! (both sides) are implemented.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, watch, Mutex as AsyncMutex};
use tokio::task::JoinHandle;
use tonic::transport::Channel;

use crate::pb::raft5::common::LogEntry;
use crate::pb::raft5::raft::raft_peer_client::RaftPeerClient;
use crate::pb::raft5::raft::{
    AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
};
use crate::storage::{self, RaftLog, StorageError};

/// How long to wait for a peer RPC (including connecting, if there's no
/// cached client yet) before giving up. Shorter than the election timeout,
/// so a candidate has a chance to hear back (or know it won't) before its
/// own timer fires again.
const PEER_RPC_TIMEOUT: Duration = Duration::from_millis(100);

/// How often the leader retries/heartbeats each peer, and how often it
/// re-checks its own watermarks (`Event::LeaderTick`). Comfortably shorter
/// than the 150-300ms election timeout, so followers hear from a live
/// leader well before they'd time out and start a needless election.
const REPLICATION_INTERVAL: Duration = Duration::from_millis(50);

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
    ElectionTimeout,
    /// Self-notification, leader only: nudges the event loop to recompute
    /// `commit_seq`/`compact_seq` even when no peer response triggers it.
    /// Needed for a zero-peer cluster (nothing else would ever advance
    /// `commit_tx` after a client write there), and also keeps watermarks
    /// from stalling between replication rounds in a multi-peer one.
    LeaderTick,
}

/// Which role to construct next. A plain tag -- unlike the three role
/// structs, it carries no state, since only one role's state is ever alive
/// at a time.
pub(crate) enum Role {
    Follower,
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

/// Both `Follower` (waiting for a leader) and `Candidate` (waiting for
/// votes) need this: if nothing interesting happens in time, push
/// `ElectionTimeout` so the event loop can start/restart an election.
///
/// Randomized (150-300ms) per the Raft paper: two nodes with the exact
/// same fixed timeout tend to become candidates at the same instant, and
/// then perpetually split-vote against each other every single term
/// (neither's `RequestVote` looks newer than the other's own, so both
/// just deny each other and restart in lockstep) -- jitter is what breaks
/// the tie within a few rounds.
fn spawn_election_timer(event_tx: mpsc::Sender<Event>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let timeout = Duration::from_millis(150 + random_jitter_millis(150));
        tokio::time::sleep(timeout).await;
        let _ = event_tx.send(Event::ElectionTimeout).await;
    })
}

/// Periodically pushes `Event::LeaderTick` -- see the variant's doc for why
/// the leader needs this in addition to reacting to peer responses.
fn spawn_leader_tick(event_tx: mpsc::Sender<Event>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(REPLICATION_INTERVAL).await;
            if event_tx.send(Event::LeaderTick).await.is_err() {
                return;
            }
        }
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

/// Builds the next `AppendEntries` request for a peer whose next expected
/// entry is `next_index`, batching everything from there through the
/// current tail (empty `entries` == a plain heartbeat). Synchronous
/// storage core, run inside `spawn_blocking` by `Leader::spawn_replication`.
fn build_append_entries_sync(
    log: &RaftLog,
    next_index: u64,
    leader_id: String,
    term: u64,
    leader_commit: u64,
    compact_seq: u64,
) -> storage::Result<AppendEntriesRequest> {
    let prev_log_index = next_index - 1;
    // TODO: `None` here means this entry has been compacted away, i.e. the
    // peer has fallen far enough behind that it needs a snapshot rather
    // than more AppendEntries -- not implemented, so this just falls back
    // to 0 and lets the usual conflict/backoff path keep retrying (it
    // won't make progress past this point until entries are available,
    // but it won't corrupt anything either).
    let prev_log_term = if prev_log_index == 0 {
        0
    } else {
        log.term_at(prev_log_index)?.unwrap_or(0)
    };
    let (last_index, _) = log.last_entry()?.unwrap_or((0, 0));
    let entries = if next_index <= last_index {
        // `storage::LogEntry` and `pb::raft5::common::LogEntry` are
        // structurally identical (the proto mirrors the storage type) but
        // are distinct Rust types, so each entry needs a field-by-field copy.
        log.entries_in_range(next_index, last_index + 1)?
            .into_iter()
            .map(|e| LogEntry {
                seq: e.seq,
                term: e.term,
                key: e.key,
                value: e.value,
            })
            .collect()
    } else {
        Vec::new()
    };
    Ok(AppendEntriesRequest {
        term,
        leader_id,
        prev_log_index,
        prev_log_term,
        entries,
        leader_commit,
        compact_seq,
    })
}

/// Requests a vote from `peer`, connecting (or reusing a cached
/// connection) through `conns`, bounded by `PEER_RPC_TIMEOUT`. `None` on
/// any failure -- connect error, RPC error, or timeout -- which is always
/// safe to just ignore: the candidate's own election timer is Raft's
/// built-in retry mechanism, so there's nothing else to do with a peer
/// that didn't answer in time.
async fn request_vote_from_peer(
    peer: &PeerInfo,
    req: RequestVoteRequest,
    conns: &PeerConnections,
) -> Option<RequestVoteResponse> {
    let attempt = async {
        let mut client = conns.get_or_connect(peer).await?;
        client.request_vote(req).await.ok()
    };
    let resp = match tokio::time::timeout(PEER_RPC_TIMEOUT, attempt).await {
        Ok(Some(resp)) => Some(resp.into_inner()),
        _ => None,
    };
    if resp.is_none() {
        conns.evict(&peer.id).await;
    }
    resp
}

/// Sends one `AppendEntries` to `peer`, same connect/timeout/evict
/// handling as `request_vote_from_peer`. `None` on any failure -- the
/// caller (a replication task, running continuously) just retries on its
/// next tick.
async fn append_entries_to_peer(
    peer: &PeerInfo,
    req: AppendEntriesRequest,
    conns: &PeerConnections,
) -> Option<AppendEntriesResponse> {
    let attempt = async {
        let mut client = conns.get_or_connect(peer).await?;
        client.append_entries(req).await.ok()
    };
    let resp = match tokio::time::timeout(PEER_RPC_TIMEOUT, attempt).await {
        Ok(Some(resp)) => Some(resp.into_inner()),
        _ => None,
    };
    if resp.is_none() {
        conns.evict(&peer.id).await;
    }
    resp
}

#[derive(Debug)]
pub enum RaftError {
    /// Not (or no longer) the leader. `leader_hint` lets a client retry in
    /// the right place, same as `RaftError`/`NotLeader` in client.proto.
    NotLeader { leader_hint: Option<String> },
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
    commit_rx: watch::Receiver<u64>,
    /// Used to detect a step-down while waiting in `append_log`, so a
    /// caller doesn't hang forever on an entry that will never commit
    /// (e.g. because it gets truncated by a new leader instead).
    role_rx: watch::Receiver<RoleHandle>,
}

impl LeaderHandle {
    /// Append a new entry and wait until it's committed (replicated to a
    /// majority) before returning.
    pub async fn append_log(&self, key: &[u8], value: Option<&[u8]>) -> Result<u64, RaftError> {
        let log = Arc::clone(&self.log);
        let term = self.term;
        let key = key.to_vec();
        let value = value.map(<[u8]>::to_vec);
        let seq = tokio::task::spawn_blocking(move || log.append(term, &key, value.as_deref()))
            .await
            .map_err(|_| RaftError::Disconnected)?
            .map_err(RaftError::Storage)?;

        let mut commit_rx = self.commit_rx.clone();
        let mut role_rx = self.role_rx.clone();

        loop {
            if *commit_rx.borrow() >= seq {
                return Ok(seq);
            }
            if !self.still_leader(&role_rx) {
                return Err(self.not_leader_error(&role_rx));
            }
            tokio::select! {
                r = commit_rx.changed() => { r.map_err(|_| RaftError::Disconnected)?; }
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

    /// Highest seq known committed. Outlives any single role (a follower
    /// applies a leader's `leader_commit` here too), so it lives in
    /// `Common` rather than inside `Leader`.
    pub commit_tx: watch::Sender<u64>,

    /// Highest seq known replicated to *every* peer, not just a majority --
    /// safe to compact below regardless of role: a leader computes it from
    /// `Leader::peer_match_index`, a follower just adopts whatever value
    /// rides along on `AppendEntries.compact_seq`. Not consumed by
    /// anything yet (no compaction task wired up), but published either
    /// way so one can be added later without touching this plumbing.
    pub compact_tx: watch::Sender<u64>,

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
        let (event_tx, event_rx) = mpsc::channel(256);
        let (role_tx, role_rx) = watch::channel(RoleHandle::Starting);

        let peer_handle = RaftPeerHandle {
            event_tx: event_tx.clone(),
        };

        let common = Self {
            node_id,
            peers,
            log,
            peer_conns: Arc::new(PeerConnections::default()),
            current_term,
            voted_for,
            commit_tx,
            compact_tx,
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

// --- Leader ----------------------------------------------------------------

pub struct Leader {
    common: Common,
    replication_tasks: Vec<JoinHandle<()>>,
    tick_timer: JoinHandle<()>,
    /// Highest seq each peer is known to have replicated. Missing entry
    /// (rather than 0) for a peer we haven't heard back from yet -- same
    /// thing numerically, `recompute_watermarks` just treats an absent
    /// peer as 0 rather than requiring this to be pre-seeded.
    peer_match_index: HashMap<String, u64>,
}

impl Leader {
    /// Starts one replication task per peer plus a self-tick, publishes a
    /// `LeaderHandle`, then runs the event loop until a role change is
    /// needed.
    // Named `new` (paired with `shutdown`) rather than returning `Self`,
    // since this runs the whole role lifecycle and hands back `Common` for
    // the driver to build the *next* role from, not just constructs one.
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(common: Common) -> (Role, Common) {
        let commit_rx = common.commit_tx.subscribe();
        let role_rx = common.role_tx.subscribe();
        common.publish_role(RoleHandle::Leader(LeaderHandle {
            log: Arc::clone(&common.log),
            term: common.current_term,
            commit_rx,
            role_rx,
        }));

        let replication_tasks = common
            .peers
            .iter()
            .cloned()
            .map(|peer| {
                Self::spawn_replication(
                    peer,
                    common.node_id.clone(),
                    common.current_term,
                    Arc::clone(&common.log),
                    Arc::clone(&common.peer_conns),
                    common.commit_tx.subscribe(),
                    common.compact_tx.subscribe(),
                    common.event_tx.clone(),
                )
            })
            .collect();
        let tick_timer = spawn_leader_tick(common.event_tx.clone());

        let mut leader = Leader {
            common,
            replication_tasks,
            tick_timer,
            peer_match_index: HashMap::new(),
        };
        // A zero-peer cluster starts out already at majority (just itself)
        // -- get commit/compact caught up to whatever's already in the log
        // without waiting for the first tick.
        leader.recompute_watermarks().await;
        let next_role = leader.run().await;
        (next_role, leader.shutdown())
    }

    /// Repeatedly sends `AppendEntries` to `peer` (a heartbeat if there's
    /// nothing new to send), tracking `next_index` locally: advances past
    /// whatever the peer just accepted, or backs it up using the
    /// follower's §5.3 conflict hints on rejection. Every response is
    /// forwarded as `Event::AppendEntriesResponse` so match_index
    /// aggregation and term step-downs happen on the event loop, not in
    /// this task -- a connect/RPC failure or timeout just gets silently
    /// retried next tick, same policy as `Candidate`'s vote requests.
    // Each parameter is an independent piece of context this task owns for
    // its whole lifetime (or a channel it reads/writes) -- there's no
    // natural subgroup to bundle into a struct that wouldn't just move the
    // same list into a literal at the one call site.
    #[allow(clippy::too_many_arguments)]
    fn spawn_replication(
        peer: PeerInfo,
        leader_id: String,
        term: u64,
        log: Arc<RaftLog>,
        conns: Arc<PeerConnections>,
        commit_rx: watch::Receiver<u64>,
        compact_rx: watch::Receiver<u64>,
        event_tx: mpsc::Sender<Event>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let (last_index, _) = last_log_index_and_term(&log).await;
            let mut next_index = last_index + 1;

            loop {
                let leader_commit = *commit_rx.borrow();
                let compact_seq = *compact_rx.borrow();
                let req = {
                    let log = Arc::clone(&log);
                    let leader_id = leader_id.clone();
                    tokio::task::spawn_blocking(move || {
                        build_append_entries_sync(
                            &log,
                            next_index,
                            leader_id,
                            term,
                            leader_commit,
                            compact_seq,
                        )
                    })
                    .await
                    .expect("build_append_entries task panicked")
                    .expect("failed to read log for replication")
                };

                if let Some(resp) = append_entries_to_peer(&peer, req, &conns).await {
                    if resp.success {
                        next_index = resp.match_index + 1;
                    } else if let Some(conflict_index) = resp.conflict_index {
                        // TODO (§5.3 leader-side optimization): search our
                        // own log for `conflict_term` and, if found, skip
                        // to just past its last entry instead of always
                        // trusting the follower's `conflict_index`
                        // directly. Still correct as-is, just occasionally
                        // one extra round trip than strictly necessary.
                        next_index = conflict_index.max(1);
                    }
                    // A term bump is handled on the event loop (which
                    // steps this whole role down, aborting this task) --
                    // nothing else to do with the response here either way.
                    if event_tx
                        .send(Event::AppendEntriesResponse {
                            from: peer.id.clone(),
                            resp,
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                }

                tokio::time::sleep(REPLICATION_INTERVAL).await;
            }
        })
    }

    async fn run(&mut self) -> Role {
        loop {
            let Some(event) = self.common.event_rx.recv().await else {
                return Role::Follower;
            };
            match self.handle_event(event).await {
                Transition::Continue => {}
                Transition::Become(role) => return role,
            }
        }
    }

    async fn handle_event(&mut self, event: Event) -> Transition {
        match event {
            Event::AppendEntries { req, respond_to } => {
                // A higher term means someone else won an election; step
                // down regardless of who they are.
                if req.term > self.common.current_term {
                    self.common.advance_term(req.term).await;
                    let _ = respond_to.send(self.reject_append(req.term));
                    return Transition::Become(Role::Follower);
                }
                // Otherwise it's a stale/duplicate leader -- reject, stay
                // leader (should be unreachable if term is the sole
                // leadership marker, but harmless to guard).
                let _ = respond_to.send(self.reject_append(self.common.current_term));
                Transition::Continue
            }
            Event::AppendEntriesResponse { from, resp } => {
                if resp.term > self.common.current_term {
                    self.common.advance_term(resp.term).await;
                    return Transition::Become(Role::Follower);
                }
                // A failure's `match_index` is meaningless (0) -- only a
                // successful response tells us anything about the peer's
                // actual progress.
                if resp.success {
                    let entry = self.peer_match_index.entry(from).or_insert(0);
                    *entry = (*entry).max(resp.match_index);
                    self.recompute_watermarks().await;
                }
                Transition::Continue
            }
            Event::RequestVote { req, respond_to } => {
                if req.term > self.common.current_term {
                    self.common.advance_term(req.term).await;
                    self.common.vote_for(&req.candidate_id).await;
                    let _ = respond_to.send(RequestVoteResponse {
                        term: self.common.current_term,
                        vote_granted: true,
                    });
                    return Transition::Become(Role::Follower);
                }
                let _ = respond_to.send(RequestVoteResponse {
                    term: self.common.current_term,
                    vote_granted: false,
                });
                Transition::Continue
            }
            Event::RequestVoteResponse { .. } => Transition::Continue, // stray, ignore
            Event::ElectionTimeout => Transition::Continue,            // leaders don't run one
            Event::LeaderTick => {
                self.recompute_watermarks().await;
                Transition::Continue
            }
        }
    }

    /// Recomputes `commit_seq` (highest seq replicated to a majority,
    /// including this node's own log) and `compact_seq` (lowest seq
    /// replicated to *every* peer) from `peer_match_index`, and publishes
    /// both via `Common` -- monotonically, so a stale/out-of-order
    /// response can never make either regress.
    async fn recompute_watermarks(&mut self) {
        let (own_last_index, _) = last_log_index_and_term(&self.common.log).await;

        let mut matched: Vec<u64> = self
            .common
            .peers
            .iter()
            .map(|p| *self.peer_match_index.get(&p.id).unwrap_or(&0))
            .collect();
        matched.push(own_last_index);
        matched.sort_unstable_by(|a, b| b.cmp(a)); // descending

        // The value at position `majority - 1` is the majority-th largest
        // -- i.e. the highest value at least `majority` entries (including
        // our own, always the max) are `>=` to.
        let commit_seq = matched[self.common.majority() - 1];
        // The last (smallest) entry once sorted descending -- the one
        // value every single node, without exception, has reached.
        let compact_seq = *matched.last().expect("always has our own entry");

        self.common.advance_commit(commit_seq);
        self.common.advance_compact(compact_seq);
    }

    fn reject_append(&self, term: u64) -> AppendEntriesResponse {
        AppendEntriesResponse {
            term,
            success: false,
            match_index: 0,
            conflict_index: None,
            conflict_term: None,
        }
    }

    /// Cancels replication tasks and hands `Common` back. Aborting rather
    /// than signaling+joining is the simplest thing that works; a graceful
    /// drain (finish in-flight RPCs first) would need a cancellation token
    /// instead.
    fn shutdown(self) -> Common {
        for task in self.replication_tasks {
            task.abort();
        }
        self.tick_timer.abort();
        self.common
    }
}

// --- Candidate ---------------------------------------------------------

pub struct Candidate {
    common: Common,
    vote_tasks: Vec<JoinHandle<()>>,
    election_timer: JoinHandle<()>,
    votes_received: usize,
}

impl Candidate {
    // See the comment on `Leader::new` re: this not returning `Self`.
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(mut common: Common) -> (Role, Common) {
        common.current_term += 1;
        common.voted_for = Some(common.node_id.clone());
        common.persist_term_state().await;
        common.publish_role(RoleHandle::Candidate);

        let (last_log_index, last_log_term) = last_log_index_and_term(&common.log).await;
        let vote_tasks = common
            .peers
            .iter()
            .cloned()
            .map(|peer| {
                Self::spawn_request_vote(
                    peer,
                    common.current_term,
                    common.node_id.clone(),
                    last_log_index,
                    last_log_term,
                    Arc::clone(&common.peer_conns),
                    common.event_tx.clone(),
                )
            })
            .collect();
        // Without this, a split vote (or a cluster too small to reach
        // majority with the peers that actually answer) would leave the
        // candidate stuck forever with nothing left to wait on.
        let election_timer = spawn_election_timer(common.event_tx.clone());

        let mut candidate = Candidate {
            common,
            vote_tasks,
            election_timer,
            votes_received: 1, // vote for self
        };
        // A zero-peer cluster reaches majority on the self-vote alone --
        // nothing will ever arrive on the event loop to notice that, so
        // check up front rather than sitting in `run` forever restarting
        // the election every timeout (see
        // `candidate_restarts_the_election_after_its_own_timeout`, which
        // covers the *intended* version of that behavior: peers exist but
        // aren't answering).
        let next_role = if candidate.votes_received >= candidate.common.majority() {
            Role::Leader
        } else {
            candidate.run().await
        };
        (next_role, candidate.shutdown())
    }

    /// Calls `peer`'s RequestVote RPC and, if it answers in time, pushes
    /// the result back as `Event::RequestVoteResponse` so vote counting
    /// happens on the event loop rather than racing with it.
    fn spawn_request_vote(
        peer: PeerInfo,
        term: u64,
        candidate_id: String,
        last_log_index: u64,
        last_log_term: u64,
        conns: Arc<PeerConnections>,
        event_tx: mpsc::Sender<Event>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let req = RequestVoteRequest {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            };
            let Some(resp) = request_vote_from_peer(&peer, req, &conns).await else {
                return;
            };
            let _ = event_tx
                .send(Event::RequestVoteResponse { from: peer.id, resp })
                .await;
        })
    }

    async fn run(&mut self) -> Role {
        loop {
            let Some(event) = self.common.event_rx.recv().await else {
                return Role::Follower;
            };
            match self.handle_event(event).await {
                Transition::Continue => {}
                Transition::Become(role) => return role,
            }
        }
    }

    async fn handle_event(&mut self, event: Event) -> Transition {
        match event {
            Event::RequestVoteResponse { resp, .. } => {
                if resp.term > self.common.current_term {
                    self.common.advance_term(resp.term).await;
                    return Transition::Become(Role::Follower);
                }
                if resp.term == self.common.current_term && resp.vote_granted {
                    self.votes_received += 1;
                    if self.votes_received >= self.common.majority() {
                        return Transition::Become(Role::Leader);
                    }
                }
                Transition::Continue
            }
            Event::AppendEntries { req, respond_to } => {
                // A valid leader for this (or a later) term means the
                // election is over; concede. Only actually advance the
                // term (and persist) if it's strictly higher -- conceding
                // to a leader already at our own term needs no term change.
                if req.term >= self.common.current_term {
                    if req.term > self.common.current_term {
                        self.common.advance_term(req.term).await;
                    }
                    let _ = respond_to.send(AppendEntriesResponse {
                        term: self.common.current_term,
                        success: false, // re-sent once we're a follower
                        match_index: 0,
                        conflict_index: None,
                        conflict_term: None,
                    });
                    return Transition::Become(Role::Follower);
                }
                let _ = respond_to.send(AppendEntriesResponse {
                    term: self.common.current_term,
                    success: false,
                    match_index: 0,
                    conflict_index: None,
                    conflict_term: None,
                });
                Transition::Continue
            }
            Event::RequestVote { req, respond_to } => {
                if req.term > self.common.current_term {
                    self.common.advance_term(req.term).await;
                    self.common.vote_for(&req.candidate_id).await;
                    let _ = respond_to.send(RequestVoteResponse {
                        term: self.common.current_term,
                        vote_granted: true,
                    });
                    return Transition::Become(Role::Follower);
                }
                // Already voted for ourselves this term.
                let _ = respond_to.send(RequestVoteResponse {
                    term: self.common.current_term,
                    vote_granted: false,
                });
                Transition::Continue
            }
            Event::ElectionTimeout => Transition::Become(Role::Candidate), // restart the election
            Event::AppendEntriesResponse { .. } | Event::LeaderTick => Transition::Continue,
        }
    }

    fn shutdown(self) -> Common {
        for task in self.vote_tasks {
            task.abort();
        }
        self.election_timer.abort();
        self.common
    }
}

// --- Follower ------------------------------------------------------------

/// The outcome of matching+applying one `AppendEntries` call's `entries`
/// against storage. Doesn't carry `term` -- that's `Common`'s concern, not
/// storage's.
struct AppendOutcome {
    success: bool,
    match_index: u64,
    conflict_index: Option<u64>,
    conflict_term: Option<u64>,
}

impl AppendOutcome {
    fn reject(conflict_index: u64, conflict_term: Option<u64>) -> Self {
        Self {
            success: false,
            match_index: 0,
            conflict_index: Some(conflict_index),
            conflict_term,
        }
    }
}

/// The synchronous core of Raft's AppendEntries receiver logic (Raft paper
/// §5.3), run inside `spawn_blocking` by `Follower::apply_append_entries`.
/// Checks `prev_log_index`/`prev_log_term`, then walks `entries` against
/// what's already in `log`: entries already present with a matching term
/// are left alone, the first mismatch truncates the divergent suffix, and
/// everything past the current tail is appended. `entries` is expected in
/// ascending, gap-free `seq` order starting at `prev_log_index + 1` --
/// guaranteed by the leader's own log being gap-free.
fn apply_append_entries_sync(
    log: &RaftLog,
    prev_log_index: u64,
    prev_log_term: u64,
    entries: &[LogEntry],
) -> storage::Result<AppendOutcome> {
    if prev_log_index > 0 {
        match log.term_at(prev_log_index)? {
            Some(term) if term == prev_log_term => {}
            Some(conflict_term) => {
                // Fast-backup (§5.3): point the leader at the first index
                // of the conflicting term, so it can skip the whole run in
                // one round trip instead of backing up one index at a time.
                let mut conflict_index = prev_log_index;
                while conflict_index > 1 {
                    match log.term_at(conflict_index - 1)? {
                        Some(term) if term == conflict_term => conflict_index -= 1,
                        _ => break,
                    }
                }
                return Ok(AppendOutcome::reject(conflict_index, Some(conflict_term)));
            }
            None => {
                // Our log doesn't even reach prev_log_index -- point the
                // leader at right after whatever we actually have.
                let next_expected = match log.last_entry()? {
                    Some((seq, _)) => seq + 1,
                    None => 1,
                };
                return Ok(AppendOutcome::reject(next_expected, None));
            }
        }
    }

    let mut match_index = prev_log_index;
    for (i, entry) in entries.iter().enumerate() {
        match log.term_at(entry.seq)? {
            // Already present with the same term -- by the Log Matching
            // Property the rest of our log up to here already matches
            // too, so there's nothing to do.
            Some(term) if term == entry.term => {
                match_index = entry.seq;
            }
            // Either a conflicting entry (wrong term) or nothing here yet
            // -- either way, this entry and everything after it in the
            // batch needs to be (re-)written.
            other => {
                if other.is_some() {
                    log.truncate_from(entry.seq)?;
                }
                for fresh in &entries[i..] {
                    let seq = log.append(fresh.term, &fresh.key, fresh.value.as_deref())?;
                    debug_assert_eq!(
                        seq, fresh.seq,
                        "follower's log position drifted from the leader's indexing"
                    );
                    match_index = seq;
                }
                break;
            }
        }
    }

    Ok(AppendOutcome {
        success: true,
        match_index,
        conflict_index: None,
        conflict_term: None,
    })
}

pub struct Follower {
    common: Common,
    election_timer: JoinHandle<()>,
    leader_hint: Option<String>,
}

impl Follower {
    // See the comment on `Leader::new` re: this not returning `Self`.
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(common: Common) -> (Role, Common) {
        common.publish_role(RoleHandle::Follower(FollowerHandle { leader_hint: None }));

        let election_timer = spawn_election_timer(common.event_tx.clone());

        let mut follower = Follower {
            common,
            election_timer,
            leader_hint: None,
        };
        let next_role = follower.run().await;
        (next_role, follower.shutdown())
    }

    /// Called whenever a valid AppendEntries/RequestVote is processed, so
    /// a live leader/candidate keeps this follower from timing out.
    fn reset_election_timer(&mut self) {
        self.election_timer.abort();
        self.election_timer = spawn_election_timer(self.common.event_tx.clone());
    }

    /// Runs `apply_append_entries_sync` on the blocking pool, then advances
    /// `commit_tx` to `min(leader_commit, match_index)` (only if the
    /// entries themselves matched) and `compact_tx` to `req.compact_seq`
    /// (regardless -- it's independent of whether this particular batch of
    /// entries matched, and by this point the caller has already confirmed
    /// `req.term` is at least as current as ours, so the hint is trusted).
    /// Panics on a storage failure -- see the module doc.
    async fn apply_append_entries(&mut self, req: &AppendEntriesRequest) -> AppendOutcome {
        let log = Arc::clone(&self.common.log);
        let prev_log_index = req.prev_log_index;
        let prev_log_term = req.prev_log_term;
        let entries = req.entries.clone();
        let outcome = tokio::task::spawn_blocking(move || {
            apply_append_entries_sync(&log, prev_log_index, prev_log_term, &entries)
        })
        .await
        .expect("apply_append_entries task panicked")
        .expect("failed to apply append entries to storage");

        if outcome.success {
            self.common
                .advance_commit(req.leader_commit.min(outcome.match_index));
        }
        self.common.advance_compact(req.compact_seq);
        outcome
    }

    async fn run(&mut self) -> Role {
        loop {
            let Some(event) = self.common.event_rx.recv().await else {
                return Role::Follower;
            };
            match self.handle_event(event).await {
                Transition::Continue => {}
                Transition::Become(role) => return role,
            }
        }
    }

    async fn handle_event(&mut self, event: Event) -> Transition {
        match event {
            Event::AppendEntries { req, respond_to } => {
                if req.term < self.common.current_term {
                    let _ = respond_to.send(AppendEntriesResponse {
                        term: self.common.current_term,
                        success: false,
                        match_index: 0,
                        conflict_index: None,
                        conflict_term: None,
                    });
                    return Transition::Continue;
                }
                if req.term > self.common.current_term {
                    self.common.advance_term(req.term).await;
                }
                self.leader_hint = Some(req.leader_id.clone());
                self.common
                    .publish_role(RoleHandle::Follower(FollowerHandle {
                        leader_hint: self.leader_hint.clone(),
                    }));
                self.reset_election_timer();

                let outcome = self.apply_append_entries(&req).await;
                let _ = respond_to.send(AppendEntriesResponse {
                    term: self.common.current_term,
                    success: outcome.success,
                    match_index: outcome.match_index,
                    conflict_index: outcome.conflict_index,
                    conflict_term: outcome.conflict_term,
                });
                Transition::Continue
            }
            Event::RequestVote { req, respond_to } => {
                // A strictly higher term means `voted_for` is left over
                // from an earlier term and no longer applies -- `advance_term`
                // resets it before the check below, or a candidate in a
                // brand new term could be denied because of who we voted
                // for last time. No-op (and no need to call it) if the
                // term hasn't actually advanced.
                if req.term > self.common.current_term {
                    self.common.advance_term(req.term).await;
                }
                // Only bother reading our own log (the one check here that
                // touches storage) if the vote would actually be grantable
                // otherwise -- no point paying for it on a request that's
                // going to be denied on term/prior-vote grounds anyway.
                let grant = if req.term >= self.common.current_term
                    && self
                        .common
                        .voted_for
                        .as_deref()
                        .is_none_or(|v| v == req.candidate_id)
                {
                    // §5.4.1: only vote for a candidate whose log is at
                    // least as up to date as ours -- later term wins; if
                    // equal, the longer (higher-index) log wins. Tuple
                    // ordering expresses exactly this comparison.
                    let (our_index, our_term) = last_log_index_and_term(&self.common.log).await;
                    (req.last_log_term, req.last_log_index) >= (our_term, our_index)
                } else {
                    false
                };
                if grant {
                    self.common.vote_for(&req.candidate_id).await;
                    self.reset_election_timer();
                }
                let _ = respond_to.send(RequestVoteResponse {
                    term: self.common.current_term,
                    vote_granted: grant,
                });
                Transition::Continue
            }
            Event::ElectionTimeout => Transition::Become(Role::Candidate),
            Event::AppendEntriesResponse { .. }
            | Event::RequestVoteResponse { .. }
            | Event::LeaderTick => {
                Transition::Continue // stray messages from a role we no longer hold
            }
        }
    }

    fn shutdown(self) -> Common {
        self.election_timer.abort();
        self.common
    }
}

// --- Driver ----------------------------------------------------------------

/// Runs forever, handing `Common` from one role to the next. Only one
/// role's event loop is ever active: the previous one has fully returned
/// (and torn down its background tasks) before the next one starts.
pub async fn run(mut common: Common) -> ! {
    let mut role = Role::Follower;
    loop {
        let (next_role, next_common) = match role {
            Role::Follower => Follower::new(common).await,
            Role::Candidate => Candidate::new(common).await,
            Role::Leader => Leader::new(common).await,
        };
        role = next_role;
        common = next_common;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn open_temp_log() -> (Arc<RaftLog>, std::path::PathBuf) {
        // A nanosecond timestamp alone isn't a reliable unique suffix under
        // parallel test execution -- two tests starting at nearly the same
        // instant can land on the same value (clock resolution, scheduling)
        // and collide on the same directory, so a process-wide counter is
        // layered on top to guarantee uniqueness regardless.
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let mut path = std::env::temp_dir();
        path.push(format!("raft5_raft_test_{}", std::process::id()));
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let counter = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        path.push(format!("{suffix:x}_{counter:x}"));
        let log = Arc::new(RaftLog::open(&path).expect("open"));
        (log, path)
    }

    fn cleanup(path: &std::path::Path) {
        let _ = std::fs::remove_dir_all(path);
    }

    /// Spins up a real event loop (as `main` would) and hands back the two
    /// external handles plus the temp dir to clean up afterward.
    fn spawn_test_node(
        peers: Vec<PeerInfo>,
    ) -> (watch::Receiver<RoleHandle>, RaftPeerHandle, std::path::PathBuf) {
        let (log, path) = open_temp_log();
        let (common, role_rx, peer_handle, _commit_rx, _compact_rx) =
            Common::new("test-node".to_string(), peers, log).unwrap();
        tokio::spawn(run(common));
        (role_rx, peer_handle, path)
    }

    /// Like `spawn_test_node`, but also hands back the storage handle (to
    /// check what actually landed in it) and `commit_rx`/`compact_rx` (to
    /// observe replication/watermark progress) -- needed for the
    /// log-matching and replication tests below.
    fn spawn_node_with_handles(
        peers: Vec<PeerInfo>,
    ) -> (
        Arc<RaftLog>,
        watch::Receiver<RoleHandle>,
        RaftPeerHandle,
        watch::Receiver<u64>,
        watch::Receiver<u64>,
        std::path::PathBuf,
    ) {
        let (log, path) = open_temp_log();
        let (common, role_rx, peer_handle, commit_rx, compact_rx) =
            Common::new("test-node".to_string(), peers, Arc::clone(&log)).unwrap();
        tokio::spawn(run(common));
        (log, role_rx, peer_handle, commit_rx, compact_rx, path)
    }

    async fn wait_for_role(
        role_rx: &mut watch::Receiver<RoleHandle>,
        pred: impl Fn(&RoleHandle) -> bool,
    ) {
        // Generous relative to the 150-300ms election timeout: real-network
        // tests (election + a vote RPC round trip, or a full election
        // followed by replication) need enough headroom to not flake under
        // parallel test execution's CPU contention.
        tokio::time::timeout(Duration::from_millis(1500), async {
            loop {
                if pred(&role_rx.borrow()) {
                    return;
                }
                role_rx.changed().await.unwrap();
            }
        })
        .await
        .expect("expected role to match the predicate before the timeout")
    }

    fn blank_append_entries(term: u64, leader_id: &str) -> AppendEntriesRequest {
        AppendEntriesRequest {
            term,
            leader_id: leader_id.to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
            compact_seq: 0,
        }
    }

    fn request_vote(term: u64, candidate_id: &str) -> RequestVoteRequest {
        RequestVoteRequest {
            term,
            candidate_id: candidate_id.to_string(),
            last_log_index: 0,
            last_log_term: 0,
        }
    }

    fn append_entries(
        term: u64,
        leader_id: &str,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    ) -> AppendEntriesRequest {
        AppendEntriesRequest {
            term,
            leader_id: leader_id.to_string(),
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
            compact_seq: 0,
        }
    }

    fn log_entry(seq: u64, term: u64, key: &[u8], value: Option<&[u8]>) -> LogEntry {
        LogEntry {
            seq,
            term,
            key: key.to_vec(),
            value: value.map(<[u8]>::to_vec),
        }
    }

    #[tokio::test]
    async fn fresh_node_starts_as_follower() {
        let (mut role_rx, _peer_handle, path) = spawn_test_node(vec![]);
        wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Follower(_))).await;
        cleanup(&path);
    }

    #[tokio::test]
    async fn append_entries_with_higher_term_is_accepted_and_advances_term() {
        let (_role_rx, peer_handle, path) = spawn_test_node(vec![]);

        let resp = peer_handle
            .append_entries(blank_append_entries(5, "leader-1"))
            .await
            .unwrap();

        assert_eq!(resp.term, 5);
        assert!(resp.success);
        cleanup(&path);
    }

    #[tokio::test]
    async fn heartbeat_on_an_empty_log_matches_at_index_zero() {
        let (_log, _role_rx, peer_handle, _commit_rx, _compact_rx, path) = spawn_node_with_handles(vec![]);

        let resp = peer_handle
            .append_entries(blank_append_entries(1, "leader-1"))
            .await
            .unwrap();

        assert!(resp.success);
        assert_eq!(resp.match_index, 0);
        cleanup(&path);
    }

    #[tokio::test]
    async fn append_entries_appends_fresh_entries_to_an_empty_log() {
        let (log, _role_rx, peer_handle, _commit_rx, _compact_rx, path) = spawn_node_with_handles(vec![]);

        let entries = vec![
            log_entry(1, 1, b"a", Some(b"1")),
            log_entry(2, 1, b"b", Some(b"2")),
        ];
        let resp = peer_handle
            .append_entries(append_entries(1, "leader-1", 0, 0, entries, 0))
            .await
            .unwrap();

        assert!(resp.success);
        assert_eq!(resp.match_index, 2);
        assert_eq!(
            log.get_latest(b"a").unwrap(),
            Some((1, Some(b"1".to_vec())))
        );
        assert_eq!(
            log.get_latest(b"b").unwrap(),
            Some((2, Some(b"2".to_vec())))
        );
        cleanup(&path);
    }

    #[tokio::test]
    async fn append_entries_rejects_and_hints_next_index_when_log_is_too_short() {
        let (_log, _role_rx, peer_handle, _commit_rx, _compact_rx, path) = spawn_node_with_handles(vec![]);

        let resp = peer_handle
            .append_entries(append_entries(1, "leader-1", 5, 1, vec![], 0))
            .await
            .unwrap();

        assert!(!resp.success);
        assert_eq!(resp.conflict_index, Some(1));
        assert_eq!(resp.conflict_term, None);
        cleanup(&path);
    }

    #[tokio::test]
    async fn append_entries_rejects_and_hints_the_start_of_the_conflicting_term_run() {
        let (_log, _role_rx, peer_handle, _commit_rx, _compact_rx, path) = spawn_node_with_handles(vec![]);

        let seed = vec![
            log_entry(1, 1, b"a", Some(b"1")),
            log_entry(2, 1, b"b", Some(b"2")),
            log_entry(3, 1, b"c", Some(b"3")),
        ];
        peer_handle
            .append_entries(append_entries(1, "leader-1", 0, 0, seed, 0))
            .await
            .unwrap();

        // Claims prev_log_index 3 was written at term 2, but the follower
        // actually has term 1 there (and at 1, 2 too) -- the fast-backup
        // hint should point at the start of that whole term-1 run, not
        // just one index back.
        let resp = peer_handle
            .append_entries(append_entries(2, "leader-1", 3, 2, vec![], 0))
            .await
            .unwrap();

        assert!(!resp.success);
        assert_eq!(resp.conflict_term, Some(1));
        assert_eq!(resp.conflict_index, Some(1));
        cleanup(&path);
    }

    #[tokio::test]
    async fn append_entries_truncates_a_conflicting_suffix_and_applies_the_leaders_entries() {
        let (log, _role_rx, peer_handle, _commit_rx, _compact_rx, path) = spawn_node_with_handles(vec![]);

        // Follower picks up a stale entry at seq 2 from an old leader.
        let stale = vec![
            log_entry(1, 1, b"a", Some(b"1")),
            log_entry(2, 1, b"stale", Some(b"old")),
        ];
        peer_handle
            .append_entries(append_entries(1, "leader-1", 0, 0, stale, 0))
            .await
            .unwrap();

        // A new leader (higher term) overwrites seq 2 onward.
        let fresh = vec![log_entry(2, 2, b"fresh", Some(b"new"))];
        let resp = peer_handle
            .append_entries(append_entries(2, "leader-2", 1, 1, fresh, 0))
            .await
            .unwrap();

        assert!(resp.success);
        assert_eq!(resp.match_index, 2);
        // The stale entry's key is gone from `latest`...
        assert_eq!(log.get_latest(b"stale").unwrap(), None);
        // ...replaced by the new leader's entry at the same seq.
        assert_eq!(
            log.get_latest(b"fresh").unwrap(),
            Some((2, Some(b"new".to_vec())))
        );
        cleanup(&path);
    }

    #[tokio::test]
    async fn append_entries_advances_commit_to_the_min_of_leader_commit_and_match_index() {
        let (_log, _role_rx, peer_handle, mut commit_rx, _compact_rx, path) =
            spawn_node_with_handles(vec![]);

        let entries = vec![
            log_entry(1, 1, b"a", Some(b"1")),
            log_entry(2, 1, b"b", Some(b"2")),
        ];
        // leader_commit (1) is lower than match_index (2), so commit should
        // land at 1, not 2.
        peer_handle
            .append_entries(append_entries(1, "leader-1", 0, 0, entries, 1))
            .await
            .unwrap();

        commit_rx.changed().await.unwrap();
        assert_eq!(*commit_rx.borrow(), 1);
        cleanup(&path);
    }

    #[tokio::test]
    async fn request_vote_grants_first_vote_then_denies_a_second_in_the_same_term() {
        let (_role_rx, peer_handle, path) = spawn_test_node(vec![]);

        let first = peer_handle
            .request_vote(request_vote(3, "candidate-a"))
            .await
            .unwrap();
        assert!(first.vote_granted);

        let second = peer_handle
            .request_vote(request_vote(3, "candidate-b"))
            .await
            .unwrap();
        assert!(!second.vote_granted);
        assert_eq!(second.term, 3);

        cleanup(&path);
    }

    #[tokio::test]
    async fn request_vote_with_a_higher_term_is_granted_even_after_voting_this_term() {
        let (_role_rx, peer_handle, path) = spawn_test_node(vec![]);

        peer_handle
            .request_vote(request_vote(3, "candidate-a"))
            .await
            .unwrap();

        let resp = peer_handle
            .request_vote(request_vote(4, "candidate-b"))
            .await
            .unwrap();

        assert!(resp.vote_granted);
        assert_eq!(resp.term, 4);
        cleanup(&path);
    }

    #[tokio::test]
    async fn request_vote_denies_a_candidate_whose_log_ends_at_an_older_term() {
        let (_log, _role_rx, peer_handle, _commit_rx, _compact_rx, path) = spawn_node_with_handles(vec![]);

        // Follower's log ends at term 2.
        let entries = vec![log_entry(1, 2, b"a", Some(b"1"))];
        peer_handle
            .append_entries(append_entries(2, "leader-1", 0, 0, entries, 0))
            .await
            .unwrap();

        // Candidate's RPC term (3) is higher, but its own log ends at an
        // *older* term (1) than ours (2) -- must be denied regardless of
        // how much longer its log claims to be (index 5).
        let resp = peer_handle
            .request_vote(RequestVoteRequest {
                term: 3,
                candidate_id: "candidate-a".to_string(),
                last_log_index: 5,
                last_log_term: 1,
            })
            .await
            .unwrap();

        assert!(!resp.vote_granted);
        cleanup(&path);
    }

    #[tokio::test]
    async fn request_vote_grants_a_candidate_whose_log_is_at_least_as_up_to_date() {
        let (_log, _role_rx, peer_handle, _commit_rx, _compact_rx, path) = spawn_node_with_handles(vec![]);

        let entries = vec![log_entry(1, 2, b"a", Some(b"1"))];
        peer_handle
            .append_entries(append_entries(2, "leader-1", 0, 0, entries, 0))
            .await
            .unwrap();

        // Same term as our last entry (2) and the same index (1) --
        // "at least as up to date" per §5.4.1, so this should be granted.
        let resp = peer_handle
            .request_vote(RequestVoteRequest {
                term: 3,
                candidate_id: "candidate-a".to_string(),
                last_log_index: 1,
                last_log_term: 2,
            })
            .await
            .unwrap();

        assert!(resp.vote_granted);
        cleanup(&path);
    }

    #[tokio::test]
    async fn granting_a_vote_persists_it_to_storage() {
        // Complements storage.rs's own round-trip test: this one checks
        // that `Follower::handle_event` actually calls through to
        // persistence, not just that the storage layer works in isolation.
        let (log, path) = open_temp_log();
        let (common, _role_rx, peer_handle, _commit_rx, _compact_rx) =
            Common::new("test-node".to_string(), vec![], Arc::clone(&log)).unwrap();
        tokio::spawn(run(common));

        peer_handle
            .request_vote(request_vote(9, "candidate-a"))
            .await
            .unwrap();

        assert_eq!(
            log.term_state().unwrap(),
            (9, Some("candidate-a".to_string()))
        );
        cleanup(&path);
    }

    #[tokio::test]
    async fn follower_becomes_candidate_after_election_timeout() {
        let (mut role_rx, _peer_handle, path) = spawn_test_node(vec![]);
        wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Candidate)).await;
        cleanup(&path);
    }

    #[tokio::test]
    async fn candidate_steps_down_to_follower_on_append_entries_from_a_leader() {
        let (mut role_rx, peer_handle, path) = spawn_test_node(vec![]);
        wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Candidate)).await;

        // A real leader's AppendEntries at any term at least as high as the
        // candidate's own (we don't know it exactly from out here, but
        // u64::MAX always qualifies) should make it concede the election.
        peer_handle
            .append_entries(blank_append_entries(u64::MAX, "leader-1"))
            .await
            .unwrap();

        wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Follower(_))).await;
        cleanup(&path);
    }

    #[tokio::test]
    async fn candidate_restarts_the_election_after_its_own_timeout() {
        // A real peer that will never answer (nothing listens on this
        // address), so majority (2, with one peer) can never be reached on
        // the self-vote alone -- unlike a *zero*-peer cluster, which wins
        // immediately on its own vote (see
        // `leader_commits_a_write_immediately_in_a_single_node_cluster`).
        let peers = vec![PeerInfo {
            id: "ghost".to_string(),
            address: "http://127.0.0.1:1".to_string(),
        }];
        let (mut role_rx, _peer_handle, path) = spawn_test_node(peers);
        wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Candidate)).await;

        // Without its own timer it would otherwise wait on `event_rx`
        // forever. Just check it keeps re-publishing `Candidate` rather
        // than hanging.
        for _ in 0..2 {
            role_rx.changed().await.unwrap();
            assert!(matches!(&*role_rx.borrow(), RoleHandle::Candidate));
        }
        cleanup(&path);
    }

    /// Binds an ephemeral local port and immediately releases it, for
    /// handing a real address to a peer/server pair before either exists.
    /// Small race (something else could grab the port before we rebind
    /// it), but standard practice for this kind of test.
    fn free_local_addr() -> std::net::SocketAddr {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        listener.local_addr().expect("local_addr")
    }

    /// End-to-end over real sockets: two full nodes (state machine + a real
    /// `RaftPeerService` bound to a real port), each configured with the
    /// other as its only peer. Exercises the whole voting path this task
    /// added -- `Candidate` actually dialing out via `RaftPeerClient`,
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
                .add_service(RaftPeerServer::new(RaftPeerService::new(
                    log_a,
                    peer_handle_a,
                )))
                .serve(addr_a),
        );
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(RaftPeerServer::new(RaftPeerService::new(
                    log_b,
                    peer_handle_b,
                )))
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

    fn expect_leader_handle(role: &RoleHandle) -> LeaderHandle {
        match role {
            RoleHandle::Leader(h) => h.clone(),
            _ => panic!("expected Leader"),
        }
    }

    /// A zero-peer "cluster" reaches majority on its own self-vote alone
    /// (see the fix in `Candidate::new`), and then has no peers to wait on
    /// for commit either -- `Event::LeaderTick` is what's responsible for
    /// noticing the leader's own growing log and advancing `commit_tx`
    /// without any peer response ever arriving to trigger it.
    #[tokio::test]
    async fn leader_commits_a_write_immediately_in_a_single_node_cluster() {
        let (mut role_rx, _peer_handle, path) = spawn_test_node(vec![]);
        wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Leader(_))).await;
        let leader = expect_leader_handle(&role_rx.borrow());

        let seq = tokio::time::timeout(Duration::from_millis(500), leader.append_log(b"k", Some(b"v")))
            .await
            .expect("append_log should not hang with no peers to wait on")
            .unwrap();
        assert_eq!(seq, 1);

        cleanup(&path);
    }

    /// End-to-end over real sockets, continuing where
    /// `candidate_wins_election_against_a_real_peer_over_the_network`
    /// leaves off: once one of the two nodes wins the election, a write
    /// through its `LeaderHandle` should reach the other node's storage
    /// too (real replication task, real connection, real §5.3 next_index
    /// tracking) and commit (real majority -- both nodes -- computed by
    /// `recompute_watermarks`) rather than hanging.
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
                .add_service(RaftPeerServer::new(RaftPeerService::new(
                    Arc::clone(&log_a),
                    peer_handle_a,
                )))
                .serve(addr_a),
        );
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(RaftPeerServer::new(RaftPeerService::new(
                    Arc::clone(&log_b),
                    peer_handle_b,
                )))
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
                (expect_leader_handle(&role_rx_a.borrow()), Arc::clone(&log_b))
            } else {
                (expect_leader_handle(&role_rx_b.borrow()), Arc::clone(&log_a))
            };
            match tokio::time::timeout(Duration::from_millis(2000), leader.append_log(b"k", Some(b"v")))
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
            Some((seq, Some(b"v".to_vec())))
        );

        cleanup(&path_a);
        cleanup(&path_b);
    }
}
