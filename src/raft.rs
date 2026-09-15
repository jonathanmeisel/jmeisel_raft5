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
//! This is a skeleton: the leader-side §5.3 backoff optimization is a
//! simplification (see `leader::Leader::spawn_replication`), and there's no
//! cluster membership / snapshot-triggering yet. Log-matching (the follower
//! side), the candidate's voting (both sides), and leader replication
//! (majority commit + all-peers compaction watermark) are implemented.

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

mod candidate;
mod follower;
mod leader;

use candidate::Candidate;
use follower::Follower;
use leader::Leader;

/// How long to wait for a peer RPC (including connecting, if there's no
/// cached client yet) before giving up. Shorter than the election timeout,
/// so a candidate has a chance to hear back (or know it won't) before its
/// own timer fires again.
const PEER_RPC_TIMEOUT: Duration = Duration::from_millis(100);

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

/// Shared test infrastructure for this module and its `leader`/`candidate`/
/// `follower` submodules -- each submodule's own `#[cfg(test)] mod tests`
/// pulls these in with `use crate::raft::test_support::*;`.
#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    pub(crate) fn open_temp_log() -> (Arc<RaftLog>, std::path::PathBuf) {
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

    pub(crate) fn cleanup(path: &std::path::Path) {
        let _ = std::fs::remove_dir_all(path);
    }

    /// Spins up a real event loop (as `main` would) and hands back the two
    /// external handles plus the temp dir to clean up afterward.
    pub(crate) fn spawn_test_node(
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
    /// log-matching and replication tests.
    pub(crate) fn spawn_node_with_handles(
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

    pub(crate) async fn wait_for_role(
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

    pub(crate) fn blank_append_entries(term: u64, leader_id: &str) -> AppendEntriesRequest {
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

    pub(crate) fn request_vote(term: u64, candidate_id: &str) -> RequestVoteRequest {
        RequestVoteRequest {
            term,
            candidate_id: candidate_id.to_string(),
            last_log_index: 0,
            last_log_term: 0,
        }
    }

    pub(crate) fn append_entries(
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

    pub(crate) fn log_entry(seq: u64, term: u64, key: &[u8], value: Option<&[u8]>) -> LogEntry {
        LogEntry {
            seq,
            term,
            key: key.to_vec(),
            value: value.map(<[u8]>::to_vec),
        }
    }

    /// Binds an ephemeral local port and immediately releases it, for
    /// handing a real address to a peer/server pair before either exists.
    /// Small race (something else could grab the port before we rebind
    /// it), but standard practice for this kind of test.
    pub(crate) fn free_local_addr() -> std::net::SocketAddr {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        listener.local_addr().expect("local_addr")
    }

    pub(crate) fn expect_leader_handle(role: &RoleHandle) -> LeaderHandle {
        match role {
            RoleHandle::Leader(h) => h.clone(),
            _ => panic!("expected Leader"),
        }
    }
}

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
