//! The leader role: replicates its log to every peer and advances the
//! commit/compaction watermarks as they catch up. See the parent module's
//! doc for how roles fit together generally.

use super::*;
use crate::pb::raft5::raft::{SnapshotChunk, snapshot_chunk};

/// What `build_replication_step_sync` decides to send a peer next.
enum ReplicationStep {
    AppendEntries(AppendEntriesRequest),
    /// The peer's next expected entry has already been compacted away --
    /// nothing short of a full state transfer can bring it up to date.
    Snapshot,
}

/// Builds the next replication step for a peer whose next expected entry
/// is `next_index`: an `AppendEntries` batching everything from there
/// through the current tail (empty `entries` == a plain heartbeat), or a
/// `Snapshot` step if `next_index`'s predecessor has already been
/// compacted away (so `term_at` can no longer answer for it -- there's no
/// valid `prev_log_term` to send). Synchronous storage core, run inside
/// `spawn_blocking` by `Leader::spawn_replication`.
fn build_replication_step_sync(
    log: &RaftLog,
    next_index: u64,
    leader_id: String,
    term: u64,
    leader_commit: u64,
    compact_seq: u64,
) -> storage::Result<ReplicationStep> {
    let prev_log_index = next_index - 1;
    let prev_log_term = if prev_log_index == 0 {
        0
    } else {
        match log.term_at(prev_log_index)? {
            Some(term) => term,
            None => return Ok(ReplicationStep::Snapshot),
        }
    };
    let (last_index, _) = log.last_entry()?.unwrap_or((0, 0));
    let entries = if next_index <= last_index {
        let fetched = log.entries_in_range(next_index, last_index + 1)?;
        // `entries_in_range` silently skips any seq that's been compacted
        // away rather than erroring -- fine for its own documented
        // contract, but if that happened *here* it would replay this
        // peer with silent gaps in its history instead of catching it up
        // correctly. This only catches compaction below `next_index`
        // itself; `prev_log_index > 0` going missing is already caught
        // above via `term_at`, but `prev_log_index == 0` (a peer
        // replaying from scratch) skips that check entirely, so a log
        // whose *earliest* surviving entry is already past `next_index`
        // needs this second check to be caught too.
        let expected = last_index - next_index + 1;
        if (fetched.len() as u64) < expected {
            return Ok(ReplicationStep::Snapshot);
        }
        fetched.into_iter().map(LogEntry::from).collect()
    } else {
        Vec::new()
    };
    Ok(ReplicationStep::AppendEntries(AppendEntriesRequest {
        term,
        leader_id,
        prev_log_index,
        prev_log_term,
        entries,
        leader_commit,
        compact_seq,
    }))
}

/// Sends one `AppendEntries` to `peer`, same connect/evict handling as
/// `candidate::request_vote_from_peer` but with the more generous
/// `APPEND_ENTRIES_RPC_TIMEOUT` (this one can legitimately take a while --
/// see that constant's doc). `None` on any failure -- the caller (a
/// replication task, running continuously) just retries on its next tick.
async fn append_entries_to_peer(
    peer: &PeerInfo,
    req: AppendEntriesRequest,
    conns: &PeerConnections,
) -> Option<AppendEntriesResponse> {
    let attempt = async {
        let mut client = conns.get_or_connect(peer).await?;
        client.append_entries(req).await.ok()
    };
    let resp = match tokio::time::timeout(APPEND_ENTRIES_RPC_TIMEOUT, attempt).await {
        Ok(Some(resp)) => Some(resp.into_inner()),
        _ => None,
    };
    if resp.is_none() {
        conns.evict(&peer.id).await;
    }
    resp
}

/// Sends one `GetLease` to `peer`, same connect/evict handling as the
/// other peer RPCs, bounded by the tight `LIGHTWEIGHT_RPC_TIMEOUT` (this
/// one does no disk I/O, so it should always be fast -- that's the whole
/// point of keeping it separate from `AppendEntries`). `None` on any
/// failure -- the caller (a lease-probing task, running continuously)
/// just retries on its next probe.
async fn get_lease_from_peer(
    peer: &PeerInfo,
    req: GetLeaseRequest,
    conns: &PeerConnections,
) -> Option<GetLeaseResponse> {
    let attempt = async {
        let mut client = conns.get_or_connect(peer).await?;
        client.get_lease(req).await.ok()
    };
    let resp = match tokio::time::timeout(LIGHTWEIGHT_RPC_TIMEOUT, attempt).await {
        Ok(Some(resp)) => Some(resp.into_inner()),
        _ => None,
    };
    if resp.is_none() {
        conns.evict(&peer.id).await;
    }
    resp
}

/// Exports the current state (on the blocking pool -- fjall's export is
/// synchronous, same discipline as every other storage call) and streams
/// it to `peer` as a `SnapshotHeader` followed by the exported bytes
/// chunked into `SNAPSHOT_CHUNK_SIZE` pieces -- the header carries the
/// Raft-protocol metadata (term/leader_id) the follower's term check
/// needs, while the chunked bytes are exactly what `RaftLog::install_snapshot`
/// expects to parse back out (it has its own embedded magic/version/seq/term,
/// redundant with the header but at a different layer). Bounded by the
/// generous `SNAPSHOT_RPC_TIMEOUT`, since this can dwarf a normal
/// `AppendEntries` batch. `None` on an RPC failure -- the caller just
/// tries again next tick, same policy as everything else -- but a storage
/// read failure panics, same as `build_replication_step_sync`.
async fn send_snapshot_to_peer(
    peer: &PeerInfo,
    leader_id: &str,
    term: u64,
    log: &Arc<RaftLog>,
    conns: &PeerConnections,
) -> Option<(InstallSnapshotResponse, u64)> {
    let log = Arc::clone(log);
    let (buf, meta) = tokio::task::spawn_blocking(move || -> storage::Result<_> {
        let mut buf = Vec::new();
        let meta = log.export_snapshot(&mut buf)?;
        Ok((buf, meta))
    })
    .await
    .expect("export_snapshot task panicked")
    .expect("failed to export snapshot for replication");

    let header = SnapshotChunk {
        part: Some(snapshot_chunk::Part::Header(SnapshotHeader {
            term,
            leader_id: leader_id.to_string(),
            last_included_index: meta.last_included_seq,
            last_included_term: meta.last_included_term,
        })),
    };
    // Collected eagerly (rather than left as a lazy iterator borrowing
    // `buf`) since the stream handed to `install_snapshot` needs its
    // items to be `'static`.
    let chunks: Vec<SnapshotChunk> = buf
        .chunks(SNAPSHOT_CHUNK_SIZE)
        .map(|c| SnapshotChunk {
            part: Some(snapshot_chunk::Part::Data(c.to_vec())),
        })
        .collect();
    let stream = tokio_stream::iter(std::iter::once(header).chain(chunks));

    let attempt = async {
        let mut client = conns.get_or_connect(peer).await?;
        let resp: Option<tonic::Response<InstallSnapshotResponse>> =
            client.install_snapshot(stream).await.ok();
        resp
    };
    let resp = match tokio::time::timeout(SNAPSHOT_RPC_TIMEOUT, attempt).await {
        Ok(Some(resp)) => Some(resp.into_inner()),
        _ => None,
    };
    match resp {
        Some(resp) => Some((resp, meta.last_included_seq)),
        None => {
            conns.evict(&peer.id).await;
            None
        }
    }
}

/// Periodically pushes `Event::LeaderTick` -- see the variant's doc for why
/// the leader needs this in addition to reacting to peer responses.
fn spawn_leader_tick(event_tx: mpsc::Sender<Event>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(HEARTBEAT).await;
            if event_tx.send(Event::LeaderTick).await.is_err() {
                return;
            }
        }
    })
}

/// Repeatedly probes `peer` for a lease renewal at `HEARTBEAT` --
/// independent of, and much cheaper than, `spawn_replication`'s
/// `AppendEntries`, so a follower busy appending a large batch doesn't
/// prevent the leader from promptly learning whether it's still trusted.
/// Every response is forwarded as `Event::GetLeaseResponse`, same pattern
/// as replication and voting.
fn spawn_lease_prober(
    peer: PeerInfo,
    leader_id: String,
    term: u64,
    conns: Arc<PeerConnections>,
    event_tx: mpsc::Sender<Event>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let req = GetLeaseRequest {
                leader_id: leader_id.clone(),
                term,
            };
            if let Some(resp) = get_lease_from_peer(&peer, req, &conns).await
                && event_tx
                    .send(Event::GetLeaseResponse {
                        from: peer.id.clone(),
                        resp,
                    })
                    .await
                    .is_err()
            {
                return;
            }
            tokio::time::sleep(HEARTBEAT).await;
        }
    })
}

pub struct Leader {
    common: Common,
    /// One replication task per peer, keyed by node id so a membership
    /// change can spawn or abort exactly the ones that changed rather
    /// than tearing every peer's replication down and starting over --
    /// each task holds per-peer `next_index` state that would be lost.
    /// Every member except us, learners included.
    replication_tasks: HashMap<String, JoinHandle<()>>,
    /// Same, for lease probes -- but voting members only.
    lease_tasks: HashMap<String, JoinHandle<()>>,
    tick_timer: JoinHandle<()>,
    /// Highest seq each peer is known to have replicated. Missing entry
    /// (rather than 0) for a peer we haven't heard back from yet -- same
    /// thing numerically, `recompute_watermarks` just treats an absent
    /// peer as 0 rather than requiring this to be pre-seeded.
    peer_match_index: HashMap<String, u64>,
    /// When each peer's last granted lease stops counting toward quorum
    /// (`Instant::now() + LEASE_DURATION` at the time it was granted).
    /// Missing entry means "never granted (or already known-stale)" --
    /// treated as expired, same as an entry whose deadline has passed.
    peer_lease_expiry: HashMap<String, Instant>,
    read_quorum_tx: watch::Sender<bool>,
    /// The seq of this leader's term-opening no-op. Raft §5.4.2 means
    /// commitment can't pass this point until it is itself replicated to
    /// a majority, so `commit_seq >= noop_seq` is exactly "this leader
    /// has committed an entry from its own term" -- the precondition for
    /// proposing a membership change (see `propose_config`).
    noop_seq: u64,
    /// The seq of a `Config` entry this leader has appended but not yet
    /// seen commit, if any. Raft allows only one membership change in
    /// flight at a time (§4.1).
    pending_config_seq: Option<u64>,
    /// Per-learner catch-up progress, for automatic promotion. Keyed by
    /// node id; entries exist only while a member is a non-voting
    /// learner.
    catch_up: HashMap<String, CatchUpState>,
}

/// One round of a learner catching up (Raft §4.2.1). A round starts at
/// whatever the leader's log tail is right then; the learner has caught
/// up when its `match_index` reaches that point. What matters is not
/// that it finished, but *how fast*: a round that completes inside an
/// election timeout means this server can keep up with the cluster in
/// real time, which is the actual precondition for giving it a vote.
struct CatchUpState {
    round_start_index: u64,
    round_started_at: Instant,
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
        // Raft §5.4.2 (Figure 8): a leader may only advance `commit_seq`
        // over an entry from its *own* term. Appending a no-op first
        // means there is always such an entry to reach -- without it, a
        // leader that never receives a client write could never commit
        // the inherited tail of its log at all, and `recompute_watermarks`
        // would stall indefinitely. Done before the `RoleHandle` is
        // published and before any replication task starts, so the no-op
        // is part of the log from the very first `AppendEntries` we send.
        let noop_seq = {
            let log = Arc::clone(&common.log);
            let term = common.current_term;
            tokio::task::spawn_blocking(move || log.append_noop(term))
                .await
                .expect("append_noop task panicked")
                .expect("failed to append leader no-op")
        };

        let applied_rx = common.applied_tx.subscribe();
        let role_rx = common.role_tx.subscribe();
        let (read_quorum_tx, read_quorum_rx) = watch::channel(false);
        common.publish_role(RoleHandle::Leader(LeaderHandle {
            log: Arc::clone(&common.log),
            term: common.current_term,
            event_tx: common.event_tx.clone(),
            applied_rx,
            role_rx,
            read_quorum_rx,
        }));

        let tick_timer = spawn_leader_tick(common.event_tx.clone());

        let mut leader = Leader {
            common,
            replication_tasks: HashMap::new(),
            lease_tasks: HashMap::new(),
            tick_timer,
            peer_match_index: HashMap::new(),
            peer_lease_expiry: HashMap::new(),
            read_quorum_tx,
            noop_seq,
            pending_config_seq: None,
            catch_up: HashMap::new(),
        };
        // Starting from empty maps, this spawns the full initial set --
        // the same code path a later membership change goes through.
        leader.reconcile_peers().await;
        // A zero-peer cluster starts out already at majority (just itself)
        // -- get commit/compact and read-quorum caught up without waiting
        // for the first tick.
        leader.recompute_watermarks().await;
        leader.recompute_lease_quorum();
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
                let step = {
                    let log = Arc::clone(&log);
                    let leader_id = leader_id.clone();
                    tokio::task::spawn_blocking(move || {
                        build_replication_step_sync(
                            &log,
                            next_index,
                            leader_id,
                            term,
                            leader_commit,
                            compact_seq,
                        )
                    })
                    .await
                    .expect("build_replication_step task panicked")
                    .expect("failed to read log for replication")
                };

                match step {
                    ReplicationStep::AppendEntries(req) => {
                        if let Some(resp) = append_entries_to_peer(&peer, req, &conns).await {
                            if resp.success {
                                next_index = resp.match_index + 1;
                            } else if let Some(conflict_index) = resp.conflict_index {
                                // TODO (§5.3 leader-side optimization): search
                                // our own log for `conflict_term` and, if
                                // found, skip to just past its last entry
                                // instead of always trusting the follower's
                                // `conflict_index` directly. Still correct
                                // as-is, just occasionally one extra round
                                // trip than strictly necessary.
                                next_index = conflict_index.max(1);
                            }
                            // A term bump is handled on the event loop
                            // (which steps this whole role down, aborting
                            // this task) -- nothing else to do with the
                            // response here either way.
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
                    }
                    ReplicationStep::Snapshot => {
                        if let Some((resp, last_included_seq)) =
                            send_snapshot_to_peer(&peer, &leader_id, term, &log, &conns).await
                        {
                            // Whatever the peer's log looked like before,
                            // it now starts fresh right after the
                            // snapshot -- resume normal AppendEntries from
                            // there next tick.
                            next_index = last_included_seq + 1;
                            if event_tx
                                .send(Event::InstallSnapshotResponse {
                                    from: peer.id.clone(),
                                    resp,
                                    last_included_seq,
                                })
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                    }
                }

                tokio::time::sleep(HEARTBEAT).await;
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
                    self.review_learner_progress().await;
                }
                self.step_down_if_removed()
            }
            Event::InstallSnapshot {
                header, respond_to, ..
            } => {
                // Same shape as the AppendEntries arm above: a higher term
                // means someone else is leading now, step down; otherwise
                // it's stale, reject and stay leader. `data` is unused --
                // a leader never applies an inbound snapshot itself.
                if header.term > self.common.current_term {
                    self.common.advance_term(header.term).await;
                    let _ = respond_to.send(InstallSnapshotResponse {
                        term: self.common.current_term,
                    });
                    return Transition::Become(Role::Follower);
                }
                let _ = respond_to.send(InstallSnapshotResponse {
                    term: self.common.current_term,
                });
                Transition::Continue
            }
            Event::InstallSnapshotResponse {
                from,
                resp,
                last_included_seq,
            } => {
                if resp.term > self.common.current_term {
                    self.common.advance_term(resp.term).await;
                    return Transition::Become(Role::Follower);
                }
                let entry = self.peer_match_index.entry(from).or_insert(0);
                *entry = (*entry).max(last_included_seq);
                self.recompute_watermarks().await;
                // A learner far enough behind to need a snapshot makes
                // its progress here rather than through AppendEntries, so
                // its catch-up round has to be judged here too.
                self.review_learner_progress().await;
                self.step_down_if_removed()
            }
            Event::RequestVote { req, respond_to } => {
                if req.dry_run {
                    let grant = evaluate_dry_run_vote(&self.common, &req).await;
                    let _ = respond_to.send(RequestVoteResponse {
                        term: self.common.current_term,
                        vote_granted: grant,
                    });
                    return Transition::Continue;
                }
                // `voted_for == Some(self)` at our own `current_term`
                // always denies a same-term request, same as before this
                // used the shared check -- what's new is that a
                // higher-term request is no longer granted unconditionally:
                // it's now also subject to the §5.4.1 log check.
                let is_new_term = req.term > self.common.current_term;
                let grant = evaluate_real_vote(&mut self.common, &req).await;
                let _ = respond_to.send(RequestVoteResponse {
                    term: self.common.current_term,
                    vote_granted: grant,
                });
                if is_new_term {
                    return Transition::Become(Role::Follower);
                }
                Transition::Continue
            }
            Event::RequestVoteResponse { .. } => Transition::Continue, // stray, ignore
            Event::GetLeaseResponse { from, resp } => {
                if resp.lease_granted {
                    self.peer_lease_expiry
                        .insert(from, Instant::now() + LEASE_DURATION);
                } else {
                    // An explicit denial is stronger evidence than mere
                    // silence -- this peer no longer trusts us right now,
                    // so don't keep counting a stale prior grant.
                    self.peer_lease_expiry.remove(&from);
                }
                self.recompute_lease_quorum();
                Transition::Continue
            }
            Event::ElectionTimeout => Transition::Continue, // leaders don't run one
            Event::LeaderTick => {
                self.recompute_watermarks().await;
                self.recompute_lease_quorum();
                self.step_down_if_removed()
            }
            Event::ProposeConfig { change, respond_to } => {
                let outcome = self.propose_config(change).await;
                let _ = respond_to.send(outcome);
                self.step_down_if_removed()
            }
            // A leader is by definition already part of a cluster.
            Event::Bootstrap { respond_to, .. } => {
                let _ = respond_to.send(Err(RaftError::AlreadyInitialized));
                Transition::Continue
            }
        }
    }

    /// Appends a membership change to the log, where it takes effect
    /// immediately (§4.1 -- on append, not on commit). Returns the seq
    /// the `Config` entry landed at, so the caller can wait for it to
    /// commit.
    ///
    /// Two guards, both about not building on ground that can still move:
    ///
    /// * **One change at a time** (§4.1). Two overlapping single-server
    ///   changes can produce configurations with no majority in common,
    ///   which is precisely the disjoint-quorum split that makes
    ///   single-server changes safe in the first place. Enforced by
    ///   refusing while a `Config` entry we appended is still uncommitted.
    /// * **A committed entry from our own term** (Ongaro 2015). A new
    ///   leader's inherited tail isn't committed yet, so the
    ///   configuration it is reading could still be rolled back by a
    ///   future leader -- and a membership change built on a
    ///   configuration that never existed can produce two disjoint
    ///   quorums just as surely. `noop_seq` is the entry that clears this.
    async fn propose_config(&mut self, change: ConfigChange) -> Result<u64, RaftError> {
        if self.pending_config_seq.is_some() {
            return Err(RaftError::ConfigChangeInProgress);
        }
        if *self.common.commit_tx.borrow() < self.noop_seq {
            return Err(RaftError::NotCaughtUp);
        }

        let next = change.apply_to(&self.common.config)?;
        let log = Arc::clone(&self.common.log);
        let term = self.common.current_term;
        let bytes = next.to_bytes();
        let seq = tokio::task::spawn_blocking(move || log.append_config(term, &bytes))
            .await
            .map_err(|_| RaftError::Disconnected)?
            .map_err(RaftError::Storage)?;

        self.pending_config_seq = Some(seq);
        // In force as of now -- so start replicating to (or stop
        // replicating to) the affected server straight away.
        self.common.reload_config().await;
        self.reconcile_peers().await;
        Ok(seq)
    }

    /// Raft §4.2.2: a leader that removes itself keeps serving until the
    /// entry removing it *commits* -- it is the only node that can get
    /// that entry committed -- and steps down the moment it does.
    /// Stepping down any earlier would leave the cluster with no one able
    /// to finish the change.
    fn step_down_if_removed(&self) -> Transition {
        let committed = match self.pending_config_seq {
            Some(seq) => *self.common.commit_tx.borrow() >= seq,
            None => true,
        };
        if committed && !self.common.config.contains(&self.common.node_id) {
            Transition::Become(Role::Follower)
        } else {
            Transition::Continue
        }
    }

    /// Raft §4.2.1: promotes a learner that has demonstrated it can keep
    /// up with the cluster in real time.
    ///
    /// A learner is measured in rounds. Each round fixes a target -- the
    /// leader's log tail at the moment the round starts -- and ends when
    /// the learner's `match_index` reaches it. The round's *duration* is
    /// the actual test: finishing within an election timeout means this
    /// server can replicate as fast as the cluster produces entries, so
    /// giving it a vote won't stall commitment. A round that takes longer
    /// simply starts another one; there is no failure path, and a learner
    /// that never converges just stays a learner, replicated to but
    /// invisible to every quorum.
    async fn review_learner_progress(&mut self) {
        let learners: Vec<String> = self
            .common
            .config
            .others(&self.common.node_id)
            .filter(|m| !m.voting)
            .map(|m| m.server_id.clone())
            .collect();
        if learners.is_empty() {
            return;
        }

        let (own_last_index, _) = last_log_index_and_term(&self.common.log).await;
        let now = Instant::now();
        let mut promote = None;

        for id in learners {
            let matched = self.peer_match_index.get(&id).copied().unwrap_or(0);
            match self.catch_up.get_mut(&id) {
                None => {
                    self.catch_up.insert(
                        id,
                        CatchUpState {
                            round_start_index: own_last_index,
                            round_started_at: now,
                        },
                    );
                }
                Some(state) if matched >= state.round_start_index => {
                    if now.duration_since(state.round_started_at) <= LEASE_DURATION {
                        promote = Some(id);
                        break;
                    }
                    // Caught up, but too slowly to be trusted with a vote
                    // yet. Try again against a fresh target.
                    state.round_start_index = own_last_index;
                    state.round_started_at = now;
                }
                Some(_) => {} // still working through the current round
            }
        }

        if let Some(server_id) = promote {
            // Best-effort: the guards in `propose_config` can legitimately
            // refuse right now (another change in flight, say). The
            // learner stays a learner and is reconsidered on the next
            // response, so there is nothing to recover from here.
            let _ = self
                .propose_config(ConfigChange::Promote { server_id })
                .await;
        }
    }

    /// Brings the running replication and lease-probe tasks in line with
    /// the current configuration: spawns one for each member that doesn't
    /// have one, and aborts (plus forgets) the ones for members that are
    /// no longer there.
    ///
    /// Incremental on purpose. A replication task owns this leader's
    /// `next_index` for its peer, discovered over potentially many
    /// round trips of §5.3 backup; restarting every task on every
    /// membership change would throw all of that away and re-probe the
    /// whole cluster just because one server was added.
    ///
    /// Dropping a departed peer's `peer_match_index`/`peer_lease_expiry`
    /// isn't only tidiness: a stale entry for a server that is no longer
    /// a member would otherwise keep being counted -- holding
    /// `compact_seq` down forever at whatever index a removed node
    /// happened to stop at.
    async fn reconcile_peers(&mut self) {
        let wanted_replication = self.common.peers();
        let wanted_leases = self.common.voting_peers();

        for peer in &wanted_replication {
            if !self.replication_tasks.contains_key(&peer.id) {
                self.replication_tasks.insert(
                    peer.id.clone(),
                    Self::spawn_replication(
                        peer.clone(),
                        self.common.node_id.clone(),
                        self.common.current_term,
                        Arc::clone(&self.common.log),
                        Arc::clone(&self.common.peer_conns),
                        self.common.commit_tx.subscribe(),
                        self.common.compact_tx.subscribe(),
                        self.common.event_tx.clone(),
                    ),
                );
            }
        }
        for peer in &wanted_leases {
            if !self.lease_tasks.contains_key(&peer.id) {
                self.lease_tasks.insert(
                    peer.id.clone(),
                    spawn_lease_prober(
                        peer.clone(),
                        self.common.node_id.clone(),
                        self.common.current_term,
                        Arc::clone(&self.common.peer_conns),
                        self.common.event_tx.clone(),
                    ),
                );
            }
        }

        let keep_replication: std::collections::HashSet<&str> =
            wanted_replication.iter().map(|p| p.id.as_str()).collect();
        let keep_leases: std::collections::HashSet<&str> =
            wanted_leases.iter().map(|p| p.id.as_str()).collect();

        let dropped: Vec<String> = self
            .replication_tasks
            .keys()
            .filter(|id| !keep_replication.contains(id.as_str()))
            .cloned()
            .collect();
        let demoted: Vec<String> = self
            .lease_tasks
            .keys()
            .filter(|id| !keep_leases.contains(id.as_str()))
            .cloned()
            .collect();

        for id in &demoted {
            if let Some(task) = self.lease_tasks.remove(id) {
                task.abort();
            }
            self.peer_lease_expiry.remove(id);
        }
        // A promoted learner is done being measured.
        self.catch_up
            .retain(|id, _| self.common.config.member(id).is_some_and(|m| !m.voting));
        for id in &dropped {
            if let Some(task) = self.replication_tasks.remove(id) {
                task.abort();
            }
            self.peer_match_index.remove(id);
            self.catch_up.remove(id);
            // Nothing will dial this peer again, so don't hold its
            // connection open.
            self.common.peer_conns.evict(id).await;
        }

        if !dropped.is_empty() || !demoted.is_empty() {
            // Both quorums just got smaller, which can mean an entry is
            // already committed, or read quorum already held, on the
            // strength of responses we have in hand.
            self.recompute_watermarks().await;
            self.recompute_lease_quorum();
        }
    }

    /// Recomputes `commit_seq` (highest seq replicated to a majority of
    /// *voters*) and `compact_seq` (lowest seq replicated to *every*
    /// member, learners included) from `peer_match_index`, and publishes
    /// both via `Common` -- monotonically, so a stale/out-of-order
    /// response can never make either regress.
    ///
    /// The two watermarks deliberately draw from different populations.
    /// Commitment is a safety property and belongs to the voters alone:
    /// counting a learner would let an entry commit on the strength of a
    /// node that can't vote in the election that must preserve it.
    /// Compaction is only about what is safe to physically discard, so it
    /// has to wait for everyone who might still need to be sent it --
    /// which very much includes a learner that is still catching up.
    async fn recompute_watermarks(&mut self) {
        let (own_last_index, _) = last_log_index_and_term(&self.common.log).await;
        let matched_for = |peer: &PeerInfo| *self.peer_match_index.get(&peer.id).unwrap_or(&0);

        let mut voter_matched: Vec<u64> = self
            .common
            .voting_peers()
            .iter()
            .map(&matched_for)
            .collect();
        // Our own log counts only if we are ourselves a voter -- a leader
        // that has been removed from the configuration goes on serving
        // until the entry removing it commits (§4.2.2), but it is not
        // part of the quorum deciding that.
        if self.common.is_voter() {
            voter_matched.push(own_last_index);
        }
        voter_matched.sort_unstable_by(|a, b| b.cmp(a)); // descending

        let mut all_matched: Vec<u64> = self.common.peers().iter().map(&matched_for).collect();
        all_matched.push(own_last_index);

        // The value at position `majority - 1` is the majority-th largest
        // -- i.e. the highest value at least `majority` voters are `>=`
        // to. A configuration with no voters at all can't decide
        // anything; nothing commits until one exists.
        let majority = self.common.majority();
        if voter_matched.len() >= majority {
            let commit_seq = voter_matched[majority - 1];

            // Raft §5.4.2 (Figure 8): replication to a majority is *not*
            // on its own enough to commit an entry from an earlier term
            // -- such an entry can still be overwritten by a future
            // leader. Only an entry from our own term is safe to count
            // directly, and committing it commits everything before it by
            // the Log Matching Property. One check on `commit_seq` covers
            // the whole prefix: terms are non-decreasing along the log, so
            // if the entry there isn't from our term, nothing below it is
            // either.
            //
            // `Leader::new` appends a no-op at the start of every term, so
            // this gate always has something reachable to open on -- it
            // delays commitment until that no-op replicates, never blocks
            // it forever.
            if term_at(&self.common.log, commit_seq).await == Some(self.common.current_term) {
                self.common.advance_commit(commit_seq);
                // The one-change-at-a-time guard lifts the moment the
                // change we are holding it for commits.
                if self.pending_config_seq.is_some_and(|seq| commit_seq >= seq) {
                    self.pending_config_seq = None;
                }
            }
        }

        // The smallest entry -- the one value every single member,
        // without exception, has reached.
        let compact_seq = all_matched.into_iter().min().expect("always has our own");
        self.common.advance_compact(compact_seq);
    }

    /// Recomputes whether this leader currently holds a quorum of live
    /// leases (itself, which trivially always trusts itself, plus every
    /// peer whose last granted lease hasn't yet expired) and publishes
    /// the result. Purely in-memory (no storage read needed, unlike
    /// `recompute_watermarks`), so this can be a plain sync fn.
    fn recompute_lease_quorum(&mut self) {
        let now = Instant::now();
        let live = self
            .common
            .voting_peers()
            .iter()
            .filter(|p| {
                self.peer_lease_expiry
                    .get(&p.id)
                    .is_some_and(|exp| *exp > now)
            })
            .count()
            // We always trust ourselves -- but, as with commitment, only
            // if we are a voter at all.
            + usize::from(self.common.is_voter());
        let has_quorum = live >= self.common.majority();
        let _ = self.read_quorum_tx.send(has_quorum);
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
        for task in self.replication_tasks.into_values() {
            task.abort();
        }
        for task in self.lease_tasks.into_values() {
            task.abort();
        }
        self.tick_timer.abort();
        self.common
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::test_support::*;

    /// A zero-peer "cluster" reaches majority on its own self-vote alone
    /// (see the immediate-majority check in `candidate::Campaign::start`),
    /// and then has no peers to wait on for commit either --
    /// `Event::LeaderTick` is what's responsible for noticing the leader's
    /// own growing log and advancing `commit_tx` without any peer
    /// response ever arriving to trigger it.
    #[tokio::test]
    async fn leader_commits_a_write_immediately_in_a_single_node_cluster() {
        let (mut role_rx, _peer_handle, path) = spawn_test_node(vec![]);
        wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Leader(_))).await;
        let leader = expect_leader_handle(&role_rx.borrow());

        let seq = tokio::time::timeout(
            Duration::from_millis(500),
            leader.append_log(b"k", Some(b"v")),
        )
        .await
        .expect("append_log should not hang with no peers to wait on")
        .unwrap();
        // 2, not 1: seq 1 is the term-opening no-op `Leader::new` appends.
        assert_eq!(seq, 2);

        cleanup(&path);
    }

    /// Raft §5.4.2: a leader can only advance its commit index over an
    /// entry from its *own* term, so every term has to start with one --
    /// otherwise a leader that never gets a client write could never
    /// commit the tail it inherited.
    #[tokio::test]
    async fn a_new_leader_opens_its_term_with_a_noop() {
        let (log, mut role_rx, _peer_handle, _commit_rx, _compact_rx, path) =
            spawn_node_with_handles(vec![]);
        wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Leader(_))).await;
        let leader = expect_leader_handle(&role_rx.borrow());

        let stored = log.entries_in_range(1, 2).unwrap();
        assert_eq!(stored.len(), 1, "the first entry of the term is the no-op");
        assert_eq!(stored[0].kind, storage::EntryKind::NoOp);
        assert_eq!(stored[0].term, leader.term);
        assert!(stored[0].key.is_empty());
        assert_eq!(stored[0].value, None);

        cleanup(&path);
    }

    /// A `Leader` with no tasks attached, for driving `recompute_*`
    /// directly. Nothing here talks to a peer, so the states these tests
    /// need -- a specific pattern of `peer_match_index` -- can just be
    /// set, rather than coaxed out of a real cluster.
    /// Hands back the `read_quorum` receiver alongside the leader: a
    /// `watch::Sender` silently drops its value when no receiver is left
    /// alive, so a test that checks read quorum has to hold one.
    fn bare_leader(common: Common) -> (Leader, watch::Receiver<bool>) {
        let (read_quorum_tx, read_quorum_rx) = watch::channel(false);
        let leader = Leader {
            common,
            replication_tasks: HashMap::new(),
            lease_tasks: HashMap::new(),
            tick_timer: tokio::spawn(std::future::pending()),
            peer_match_index: HashMap::new(),
            peer_lease_expiry: HashMap::new(),
            read_quorum_tx,
            // No no-op appended here, so the Figure 8 / own-term gate is
            // open from the start -- these tests set up the log state
            // they want directly.
            noop_seq: 0,
            pending_config_seq: None,
            catch_up: HashMap::new(),
        };
        (leader, read_quorum_rx)
    }

    /// A leader set up as if it had already committed its own
    /// term-opening no-op, so `propose_config`'s guards are open.
    async fn ready_leader(
        members: &[(&str, bool)],
    ) -> (
        Arc<RaftLog>,
        Leader,
        watch::Receiver<bool>,
        std::path::PathBuf,
    ) {
        let config = cluster_config(members);
        let (log, (mut common, _role_rx, _peer_handle, _c, _cc), path) =
            open_temp_log_with_config("n0", &config);
        common.current_term = 1;
        let noop_seq = log.append_noop(1).unwrap();
        common.advance_commit(noop_seq);

        let (mut leader, read_quorum_rx) = bare_leader(common);
        leader.noop_seq = noop_seq;
        leader.reconcile_peers().await;
        (log, leader, read_quorum_rx, path)
    }

    /// A server is added as a *learner* (§4.2.1): a member, replicated
    /// to, but with no vote. Adding it as a voter outright would let a
    /// server that is far behind count toward -- and so stall -- every
    /// quorum from the moment it joined.
    #[tokio::test]
    async fn add_server_joins_as_a_non_voting_learner() {
        let (_log, mut leader, _rq, path) = ready_leader(&[("n0", true), ("v1", true)]).await;
        assert_eq!(leader.common.majority(), 2);

        leader
            .propose_config(ConfigChange::AddServer {
                server_id: "new".to_string(),
                address: "http://127.0.0.1:9999".to_string(),
            })
            .await
            .unwrap();

        assert!(leader.common.config.contains("new"));
        assert!(!leader.common.config.is_voter("new"));
        assert_eq!(
            leader.common.majority(),
            2,
            "a learner must not raise the quorum bar"
        );
        // In force on append: replication to it starts at once.
        assert!(leader.replication_tasks.contains_key("new"));
        assert!(!leader.lease_tasks.contains_key("new"));

        cleanup(&path);
    }

    /// Raft §4.1 allows only one membership change in flight at a time:
    /// two overlapping single-server changes can produce configurations
    /// with no majority in common.
    #[tokio::test]
    async fn only_one_membership_change_may_be_in_flight() {
        let (_log, mut leader, _rq, path) = ready_leader(&[("n0", true), ("v1", true)]).await;

        let first = leader
            .propose_config(ConfigChange::AddServer {
                server_id: "a".to_string(),
                address: "http://127.0.0.1:9001".to_string(),
            })
            .await
            .unwrap();
        let err = leader
            .propose_config(ConfigChange::AddServer {
                server_id: "b".to_string(),
                address: "http://127.0.0.1:9002".to_string(),
            })
            .await
            .unwrap_err();
        assert!(matches!(err, RaftError::ConfigChangeInProgress));
        assert!(!leader.common.config.contains("b"));

        // The guard lifts as soon as the first change commits.
        leader.peer_match_index.insert("v1".to_string(), first);
        leader.recompute_watermarks().await;
        assert!(leader.pending_config_seq.is_none());
        leader
            .propose_config(ConfigChange::AddServer {
                server_id: "b".to_string(),
                address: "http://127.0.0.1:9002".to_string(),
            })
            .await
            .unwrap();

        cleanup(&path);
    }

    /// Ongaro 2015: a leader that hasn't yet committed an entry from its
    /// own term is still reading a configuration that a future leader
    /// could roll back, so it must not build a membership change on it.
    #[tokio::test]
    async fn a_leader_will_not_change_membership_before_committing_its_own_term() {
        let config = cluster_config(&[("n0", true), ("v1", true)]);
        let (log, (mut common, _role_rx, _peer_handle, _c, _cc), path) =
            open_temp_log_with_config("n0", &config);
        common.current_term = 1;
        let noop_seq = log.append_noop(1).unwrap(); // appended, not committed

        let (mut leader, _rq) = bare_leader(common);
        leader.noop_seq = noop_seq;

        let err = leader
            .propose_config(ConfigChange::RemoveServer {
                server_id: "v1".to_string(),
            })
            .await
            .unwrap_err();
        assert!(matches!(err, RaftError::NotCaughtUp));

        // Committing the no-op is exactly what opens the gate.
        leader.peer_match_index.insert("v1".to_string(), noop_seq);
        leader.recompute_watermarks().await;
        leader
            .propose_config(ConfigChange::RemoveServer {
                server_id: "v1".to_string(),
            })
            .await
            .unwrap();

        cleanup(&path);
    }

    /// Changes that can't apply are refused rather than producing a
    /// configuration nobody could make progress under.
    #[tokio::test]
    async fn impossible_membership_changes_are_refused() {
        let (_log, mut leader, _rq, path) = ready_leader(&[("n0", true), ("v1", true)]).await;

        let already = leader
            .propose_config(ConfigChange::AddServer {
                server_id: "v1".to_string(),
                address: "http://127.0.0.1:9001".to_string(),
            })
            .await
            .unwrap_err();
        assert!(matches!(already, RaftError::InvalidConfigChange(_)));

        let stranger = leader
            .propose_config(ConfigChange::RemoveServer {
                server_id: "nobody".to_string(),
            })
            .await
            .unwrap_err();
        assert!(matches!(stranger, RaftError::InvalidConfigChange(_)));

        // A refused change must not have consumed the one in-flight slot.
        assert!(leader.pending_config_seq.is_none());
        cleanup(&path);
    }

    /// The last voter can't be removed: the entry removing it could never
    /// commit, since there would be no one left to form a quorum of the
    /// configuration it creates.
    #[tokio::test]
    async fn the_last_voter_cannot_be_removed() {
        let (_log, mut leader, _rq, path) = ready_leader(&[("n0", true), ("l1", false)]).await;
        let err = leader
            .propose_config(ConfigChange::RemoveServer {
                server_id: "n0".to_string(),
            })
            .await
            .unwrap_err();
        assert!(matches!(err, RaftError::InvalidConfigChange(_)));
        cleanup(&path);
    }

    /// Raft §4.2.2: a leader removing *itself* is the only node that can
    /// get that entry committed, so it keeps serving until it does --
    /// and steps down the instant it commits, not before.
    #[tokio::test]
    async fn a_leader_that_removes_itself_serves_until_the_change_commits() {
        let (_log, mut leader, _rq, path) =
            ready_leader(&[("n0", true), ("v1", true), ("v2", true)]).await;

        let seq = leader
            .propose_config(ConfigChange::RemoveServer {
                server_id: "n0".to_string(),
            })
            .await
            .unwrap();
        assert!(!leader.common.config.contains("n0"));
        assert!(
            matches!(leader.step_down_if_removed(), Transition::Continue),
            "stepping down now would leave nobody able to finish the change"
        );

        // A majority of the *new* configuration (v1 and v2, since we no
        // longer count) acknowledges it.
        leader.peer_match_index.insert("v1".to_string(), seq);
        leader.peer_match_index.insert("v2".to_string(), seq);
        leader.recompute_watermarks().await;
        assert!(matches!(
            leader.step_down_if_removed(),
            Transition::Become(Role::Follower)
        ));

        cleanup(&path);
    }

    /// Raft §4.2.1: a learner is promoted once it finishes a catch-up
    /// round quickly enough to show it can keep up with the cluster in
    /// real time -- automatically, with no second client call.
    #[tokio::test]
    async fn a_learner_that_keeps_up_is_promoted_automatically() {
        let (log, mut leader, _rq, path) =
            ready_leader(&[("n0", true), ("v1", true), ("l1", false)]).await;

        // First response only starts the round; there is nothing yet to
        // judge it by.
        leader.review_learner_progress().await;
        assert!(!leader.common.config.is_voter("l1"));
        assert!(leader.catch_up.contains_key("l1"));

        // It reaches the round's target well within an election timeout.
        let tail = log.last_entry().unwrap().unwrap().0;
        leader.peer_match_index.insert("l1".to_string(), tail);
        leader.review_learner_progress().await;

        assert!(leader.common.config.is_voter("l1"));
        assert_eq!(
            leader.common.majority(),
            2,
            "three voters now, so still two -- but l1's vote counts"
        );
        assert!(leader.lease_tasks.contains_key("l1"));
        assert!(
            !leader.catch_up.contains_key("l1"),
            "a promoted learner is done being measured"
        );

        cleanup(&path);
    }

    /// A learner that reaches its target only after an election timeout
    /// has elapsed hasn't shown it can keep up -- it starts another
    /// round rather than being promoted. There is no failure path: it
    /// stays a learner, replicated to but invisible to every quorum.
    #[tokio::test]
    async fn a_learner_that_lags_starts_another_round_instead() {
        let (log, mut leader, _rq, path) =
            ready_leader(&[("n0", true), ("v1", true), ("l1", false)]).await;

        let tail = log.last_entry().unwrap().unwrap().0;
        leader.catch_up.insert(
            "l1".to_string(),
            CatchUpState {
                round_start_index: tail,
                round_started_at: Instant::now() - (LEASE_DURATION * 2),
            },
        );
        leader.peer_match_index.insert("l1".to_string(), tail);
        leader.review_learner_progress().await;

        assert!(!leader.common.config.is_voter("l1"));
        let state = &leader.catch_up["l1"];
        assert!(
            state.round_started_at.elapsed() < LEASE_DURATION,
            "a fresh round should have started"
        );

        cleanup(&path);
    }

    /// A membership change adds and removes exactly the tasks that
    /// changed, and leaves the rest running: each replication task owns
    /// this leader's hard-won `next_index` for its peer, so restarting
    /// them all on every change would re-probe the whole cluster just
    /// because one server was added.
    #[tokio::test]
    async fn reconcile_peers_adds_and_removes_only_what_changed() {
        let before = cluster_config(&[("n0", true), ("v1", true), ("l1", false)]);
        let (log, (common, _role_rx, _peer_handle, _c, _cc), path) =
            open_temp_log_with_config("n0", &before);

        let (mut leader, _read_quorum_rx) = bare_leader(common);
        leader.reconcile_peers().await;
        assert_eq!(
            sorted_keys(&leader.replication_tasks),
            vec!["l1", "v1"],
            "learners are replicated to"
        );
        assert_eq!(
            sorted_keys(&leader.lease_tasks),
            vec!["v1"],
            "but do not hold a lease"
        );
        let untouched = leader.replication_tasks["v1"].id();

        // Promote the learner and add a new one, in one new config.
        let after = cluster_config(&[("n0", true), ("v1", true), ("l1", true), ("l2", false)]);
        log.append_config(1, &after.to_bytes()).unwrap();
        leader.common.reload_config().await;
        leader.reconcile_peers().await;

        assert_eq!(
            sorted_keys(&leader.replication_tasks),
            vec!["l1", "l2", "v1"]
        );
        assert_eq!(
            sorted_keys(&leader.lease_tasks),
            vec!["l1", "v1"],
            "the promoted learner now holds a lease too"
        );
        assert_eq!(
            leader.replication_tasks["v1"].id(),
            untouched,
            "an unaffected peer's replication task must not be restarted"
        );

        cleanup(&path);
    }

    /// A removed peer has to be forgotten, not just unsubscribed: a stale
    /// `peer_match_index` for a server that is no longer a member would
    /// go on being counted, pinning `compact_seq` at whatever index that
    /// server happened to stop at, forever.
    #[tokio::test]
    async fn removing_a_peer_stops_it_from_holding_back_compaction() {
        let before = cluster_config(&[("n0", true), ("v1", true), ("gone", true)]);
        let (log, (mut common, _role_rx, _peer_handle, _c, compact_rx), path) =
            open_temp_log_with_config("n0", &before);
        common.current_term = 1;
        let tail = log.append_noop(1).unwrap();

        let (mut leader, _read_quorum_rx) = bare_leader(common);
        leader.reconcile_peers().await;
        leader.peer_match_index.insert("v1".to_string(), tail);
        leader.peer_match_index.insert("gone".to_string(), 0);
        leader.recompute_watermarks().await;
        assert_eq!(*compact_rx.borrow(), 0, "\"gone\" is still a member");

        let after = cluster_config(&[("n0", true), ("v1", true)]);
        log.append_config(1, &after.to_bytes()).unwrap();
        leader.common.reload_config().await;
        leader.reconcile_peers().await;

        assert!(!leader.replication_tasks.contains_key("gone"));
        assert!(!leader.peer_match_index.contains_key("gone"));
        // `reconcile_peers` recomputes on its own once a peer goes away,
        // so this needs no further prompting.
        assert_eq!(*compact_rx.borrow(), tail);

        cleanup(&path);
    }

    fn sorted_keys(tasks: &HashMap<String, JoinHandle<()>>) -> Vec<&str> {
        let mut ids: Vec<&str> = tasks.keys().map(String::as_str).collect();
        ids.sort_unstable();
        ids
    }

    /// Commitment is decided by the voters alone (§4.2.1). A learner is
    /// replicated to like anyone else, but counting it toward the commit
    /// quorum would let an entry commit on the strength of a node that
    /// can't vote in the election that has to preserve it.
    #[tokio::test]
    async fn a_learners_replication_does_not_commit_anything() {
        let config = cluster_config(&[("n0", true), ("v1", true), ("l1", false)]);
        let (log, (mut common, _role_rx, _peer_handle, commit_rx, _compact_rx), path) =
            open_temp_log_with_config("n0", &config);
        common.current_term = 1;
        let tail = log.append_noop(1).unwrap();

        let (mut leader, _read_quorum_rx) = bare_leader(common);
        assert_eq!(leader.common.majority(), 2, "two voters, so two to decide");

        // Two of the three *members* hold the tail (us and the learner)
        // -- but only one of the two voters does.
        leader.peer_match_index.insert("l1".to_string(), tail);
        leader.peer_match_index.insert("v1".to_string(), 0);
        leader.recompute_watermarks().await;
        assert_eq!(*commit_rx.borrow(), 0);

        // The second voter is what actually decides it.
        leader.peer_match_index.insert("v1".to_string(), tail);
        leader.recompute_watermarks().await;
        assert_eq!(*commit_rx.borrow(), tail);

        cleanup(&path);
    }

    /// Compaction draws on the opposite population: it physically
    /// discards entries, so it has to wait for everyone who might still
    /// need to be sent them -- very much including a learner that is
    /// still catching up.
    #[tokio::test]
    async fn compaction_waits_for_a_learner_that_commitment_ignores() {
        let config = cluster_config(&[("n0", true), ("v1", true), ("l1", false)]);
        let (log, (mut common, _role_rx, _peer_handle, commit_rx, compact_rx), path) =
            open_temp_log_with_config("n0", &config);
        common.current_term = 1;
        let tail = log.append_noop(1).unwrap();

        let (mut leader, _read_quorum_rx) = bare_leader(common);

        // Both voters have everything, so it commits -- but the learner
        // is still behind, so nothing may be discarded yet.
        leader.peer_match_index.insert("v1".to_string(), tail);
        leader.peer_match_index.insert("l1".to_string(), 0);
        leader.recompute_watermarks().await;
        assert_eq!(*commit_rx.borrow(), tail);
        assert_eq!(*compact_rx.borrow(), 0);

        leader.peer_match_index.insert("l1".to_string(), tail);
        leader.recompute_watermarks().await;
        assert_eq!(*compact_rx.borrow(), tail);

        cleanup(&path);
    }

    /// A read lease is a quorum too, so it is counted over voters only --
    /// otherwise a leader partitioned from every voter could still claim
    /// read quorum on the strength of learners alone.
    #[tokio::test]
    async fn read_quorum_counts_voters_only() {
        let config = cluster_config(&[("n0", true), ("v1", true), ("v2", true), ("l1", false)]);
        let (_log, (common, _role_rx, _peer_handle, _commit_rx, _compact_rx), path) =
            open_temp_log_with_config("n0", &config);

        let (mut leader, read_quorum_rx) = bare_leader(common);
        let live = Instant::now() + LEASE_DURATION;

        // Us plus a learner: two of four members, but only one of three
        // voters -- short of the two a quorum needs.
        leader.peer_lease_expiry.insert("l1".to_string(), live);
        leader.recompute_lease_quorum();
        assert!(!*read_quorum_rx.borrow());

        leader.peer_lease_expiry.insert("v1".to_string(), live);
        leader.recompute_lease_quorum();
        assert!(*read_quorum_rx.borrow());

        cleanup(&path);
    }

    /// Raft §5.4.2 / Figure 8: replication to a majority does *not*
    /// commit an entry from an earlier term -- a later leader can still
    /// overwrite it. `recompute_watermarks` has to hold `commit_seq` back
    /// until an entry from the current term reaches a majority, and only
    /// then commit the whole prefix at once.
    ///
    /// Drives `recompute_watermarks` directly rather than through a real
    /// cluster, since the point is precisely the state a leader can't be
    /// steered into from outside: a majority that has the inherited tail
    /// but not yet this term's no-op.
    #[tokio::test]
    async fn commit_waits_for_an_entry_from_the_leaders_own_term() {
        let (log, path) = open_temp_log();
        // A tail inherited from term 1 and never committed by its leader.
        log.append(1, b"a", Some(b"1")).unwrap();
        let inherited = log.append(1, b"b", Some(b"2")).unwrap();

        let peers = vec![
            PeerInfo {
                id: "p1".to_string(),
                address: "http://127.0.0.1:1".to_string(),
            },
            PeerInfo {
                id: "p2".to_string(),
                address: "http://127.0.0.1:2".to_string(),
            },
        ];
        let (mut common, _role_rx, _peer_handle, commit_rx, _compact_rx) =
            Common::new_seeded("n0".to_string(), peers, Arc::clone(&log)).unwrap();
        common.current_term = 7;
        // ... plus the no-op this leader opens term 7 with.
        let noop = log.append_noop(7).unwrap();

        let (mut leader, _read_quorum_rx) = bare_leader(common);

        // p1 has the inherited entries but not the no-op. That's a
        // majority (p1 and us) holding seq 2 -- and it still must not
        // commit, because seq 2 is from term 1.
        leader.peer_match_index.insert("p1".to_string(), inherited);
        leader.recompute_watermarks().await;
        assert_eq!(
            *commit_rx.borrow(),
            0,
            "an entry from a previous term must not commit on majority replication alone"
        );

        // Once the term-7 no-op reaches p1, committing it commits
        // everything before it too (Log Matching).
        leader.peer_match_index.insert("p1".to_string(), noop);
        leader.recompute_watermarks().await;
        assert_eq!(*commit_rx.borrow(), noop);

        cleanup(&path);
    }

    /// Regression test for a race between `append_log` and
    /// `spawn_storage_maintenance_task`: both are independent `watch`
    /// subscribers of the same commit signal, and `watch` gives no
    /// ordering guarantee between separate subscribers waking up on the
    /// same update. Waiting on `commit_tx` directly (as `append_log` used
    /// to) could let it return *before* the maintenance task had actually
    /// run `apply_committed` for that seq -- `append_log` now waits on
    /// `applied_tx` instead, so the moment it returns, `get_latest` must
    /// already reflect the write with no polling or sleeping needed to
    /// observe it.
    #[tokio::test]
    async fn append_log_does_not_return_until_latest_actually_reflects_it() {
        let (log, mut role_rx, _peer_handle, _commit_rx, _compact_rx, path) =
            spawn_node_with_handles(vec![]);
        wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Leader(_))).await;
        let leader = expect_leader_handle(&role_rx.borrow());

        let seq = leader.append_log(b"k", Some(b"v")).await.unwrap();

        // No wait, no poll: if this were racing the maintenance task
        // instead of genuinely waiting for it, this would be flaky
        // (occasionally observing `None` here).
        assert_eq!(log.get_latest(b"k").unwrap(), Some((seq, b"v".to_vec())));

        cleanup(&path);
    }

    /// Regression test for a real, pre-existing bug found while adding the
    /// shared `evaluate_real_vote` helper: a leader used to grant *any*
    /// higher-term vote request unconditionally, without checking the
    /// candidate's log per §5.4.1 (only `Follower` did). A candidate whose
    /// log is behind must still be denied even though its term is higher.
    #[tokio::test]
    async fn leader_denies_a_higher_term_vote_if_the_candidates_log_is_behind() {
        let (mut role_rx, peer_handle, path) = spawn_test_node(vec![]);
        wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Leader(_))).await;
        let leader = expect_leader_handle(&role_rx.borrow());

        // Give the leader a log entry so it has something to be ahead of.
        leader.append_log(b"k", Some(b"v")).await.unwrap();

        // Term is way higher, but the candidate's log is entirely empty --
        // strictly behind the leader's one entry.
        let resp = peer_handle
            .request_vote(RequestVoteRequest {
                term: 999,
                candidate_id: "candidate-a".to_string(),
                last_log_index: 0,
                last_log_term: 0,
                dry_run: false,
            })
            .await
            .unwrap();

        assert!(!resp.vote_granted);
        cleanup(&path);
    }

    /// A zero-peer cluster trivially always has a quorum of leases (just
    /// itself), same as it trivially always has commit majority.
    #[tokio::test]
    async fn leader_has_read_quorum_immediately_in_a_single_node_cluster() {
        let (mut role_rx, _peer_handle, path) = spawn_test_node(vec![]);
        wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Leader(_))).await;
        let leader = expect_leader_handle(&role_rx.borrow());

        // `RoleHandle::Leader` is published slightly before the first
        // `recompute_lease_quorum()` call completes (there's an `.await`
        // in between, for the commit/compact watermark read) -- poll
        // briefly rather than asserting on the very first check.
        tokio::time::timeout(Duration::from_millis(500), async {
            while !leader.has_read_quorum() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("a single-node cluster should reach read quorum almost immediately");
        cleanup(&path);
    }

    /// End-to-end over real sockets: once a leader's lease-prober task
    /// actually reaches a live peer and gets granted, it should count
    /// toward read quorum (majority with one peer needs both).
    #[tokio::test]
    async fn leader_gains_read_quorum_once_a_real_peer_grants_its_lease() {
        use crate::grpc::RaftPeerService;
        use crate::pb::raft5::raft::raft_peer_server::RaftPeerServer;

        let addr_a = free_local_addr();
        let addr_b = free_local_addr();

        let (log_a, path_a) = open_temp_log();
        let (log_b, path_b) = open_temp_log();

        let (common_a, mut role_rx_a, peer_handle_a, _commit_rx_a, _compact_rx_a) =
            Common::new_seeded(
                "node-a".to_string(),
                vec![PeerInfo {
                    id: "node-b".to_string(),
                    address: format!("http://{addr_b}"),
                }],
                Arc::clone(&log_a),
            )
            .unwrap();
        let (common_b, mut role_rx_b, peer_handle_b, _commit_rx_b, _compact_rx_b) =
            Common::new_seeded(
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
        let leader = if matches!(&*role_rx_a.borrow(), RoleHandle::Leader(_)) {
            expect_leader_handle(&role_rx_a.borrow())
        } else {
            expect_leader_handle(&role_rx_b.borrow())
        };

        tokio::time::timeout(Duration::from_millis(2000), async {
            while !leader.has_read_quorum() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("leader should gain read quorum once its peer's lease prober succeeds");

        cleanup(&path_a);
        cleanup(&path_b);
    }

    #[test]
    fn build_replication_step_returns_snapshot_when_prev_log_index_is_compacted() {
        let (log, path) = open_temp_log();
        for i in 1..=5u64 {
            log.append(1, format!("k{i}").as_bytes(), Some(b"v"))
                .unwrap();
        }
        // `compact` clamps to what's been applied -- apply everything
        // first so the compact below isn't a no-op.
        log.apply_committed(5).unwrap();
        // Removes entries 1-3; the floor lands at seq 3 (its term still
        // answerable via the floor special-case in `term_at`).
        log.compact(4).unwrap();

        // next_index = 4 -> prev_log_index = 3, exactly the floor -- still
        // answerable, so this is an ordinary AppendEntries.
        let step = build_replication_step_sync(&log, 4, "leader-1".to_string(), 1, 0, 0).unwrap();
        assert!(matches!(step, ReplicationStep::AppendEntries(_)));

        // next_index = 3 -> prev_log_index = 2, strictly below the floor --
        // `term_at` can't answer for it, so this must fall back to a
        // snapshot instead of guessing.
        let step = build_replication_step_sync(&log, 3, "leader-1".to_string(), 1, 0, 0).unwrap();
        assert!(matches!(step, ReplicationStep::Snapshot));

        cleanup(&path);
    }

    /// Regression test for a gap the `term_at`-based check alone misses:
    /// `prev_log_index == 0` (a peer replaying from scratch) never calls
    /// `term_at` at all, so if the early entries it would need have
    /// *also* been compacted away, only the `entries_in_range` length
    /// check catches it.
    #[test]
    fn build_replication_step_returns_snapshot_when_replaying_from_scratch_hits_a_gap() {
        let (log, path) = open_temp_log();
        for i in 1..=5u64 {
            log.append(1, format!("k{i}").as_bytes(), Some(b"v"))
                .unwrap();
        }
        log.apply_committed(5).unwrap();
        log.compact(4).unwrap();

        let step = build_replication_step_sync(&log, 1, "leader-1".to_string(), 1, 0, 0).unwrap();
        assert!(matches!(step, ReplicationStep::Snapshot));

        cleanup(&path);
    }

    /// End-to-end over real sockets: forces the exact scenario
    /// `ReplicationStep::Snapshot` exists for by directly rolling a
    /// follower's log back (simulating it falling behind) below where the
    /// leader's log has (also directly, bypassing the normal
    /// `compact_seq` machinery -- see the comment below) been compacted,
    /// then confirms the leader detects it and sends a real
    /// `InstallSnapshot` RPC that correctly restores the follower's
    /// missing history.
    #[tokio::test]
    async fn leader_sends_a_real_snapshot_to_a_peer_that_has_fallen_behind_compaction() {
        use crate::grpc::RaftPeerService;
        use crate::pb::raft5::raft::raft_peer_server::RaftPeerServer;

        let addr_a = free_local_addr();
        let addr_b = free_local_addr();

        let (log_a, path_a) = open_temp_log();
        let (log_b, path_b) = open_temp_log();

        let (common_a, mut role_rx_a, peer_handle_a, _commit_rx_a, _compact_rx_a) =
            Common::new_seeded(
                "node-a".to_string(),
                vec![PeerInfo {
                    id: "node-b".to_string(),
                    address: format!("http://{addr_b}"),
                }],
                Arc::clone(&log_a),
            )
            .unwrap();
        let (common_b, mut role_rx_b, peer_handle_b, _commit_rx_b, _compact_rx_b) =
            Common::new_seeded(
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

        // Same leadership-can-still-shuffle-right-after-election tolerance
        // as `raft::tests::leader_replicates_a_write_to_a_real_peer_and_commits_it`:
        // retry against whoever's leader now rather than assuming the
        // first leader observed stays leader, appending 5 distinct keys.
        let (leader_log, follower_log) = loop {
            let a_is_leader = matches!(&*role_rx_a.borrow(), RoleHandle::Leader(_));
            let (leader, leader_log, follower_log) = if a_is_leader {
                (
                    expect_leader_handle(&role_rx_a.borrow()),
                    Arc::clone(&log_a),
                    Arc::clone(&log_b),
                )
            } else {
                (
                    expect_leader_handle(&role_rx_b.borrow()),
                    Arc::clone(&log_b),
                    Arc::clone(&log_a),
                )
            };

            let mut ok = true;
            for i in 0..5u64 {
                match tokio::time::timeout(
                    Duration::from_millis(2000),
                    leader.append_log(format!("k{i}").as_bytes(), Some(format!("v{i}").as_bytes())),
                )
                .await
                .expect("append_log should not hang")
                {
                    Ok(_) => {}
                    Err(RaftError::NotLeader { .. }) => {
                        ok = false;
                        break;
                    }
                    Err(e) => panic!("unexpected append_log error: {e:?}"),
                }
            }
            if ok {
                break (leader_log, follower_log);
            }
            tokio::select! {
                _ = wait_for_role(&mut role_rx_a, |r| matches!(r, RoleHandle::Leader(_))) => {}
                _ = wait_for_role(&mut role_rx_b, |r| matches!(r, RoleHandle::Leader(_))) => {}
            }
        };

        // Give the follower's own AppendEntries handling a moment to
        // actually finish persisting the 5th entry before rolling it back.
        tokio::time::timeout(Duration::from_millis(1000), async {
            loop {
                if follower_log.get_latest(b"k4").unwrap().is_some() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("follower should have replicated all 5 entries normally first");

        // Simulate the follower having fallen behind: roll it back to
        // just entry 2, out of band (the leader's replication task has no
        // idea this happened).
        follower_log.truncate_from(3).unwrap();

        // Directly compact the *leader's* log past where the follower now
        // is -- bypassing `compact_seq`/`Common` entirely, since (as
        // discussed while implementing this) `compact_seq`'s strict
        // min-across-every-configured-peer semantics make it exceedingly
        // hard to reach this precondition through normal two-node
        // operation. Forcing it directly tests the RPC mechanics this
        // test actually cares about, without relitigating that separate
        // design question. `compact` clamps to `apply_committed`'s
        // watermark, so that has to be caught up first too -- in normal
        // operation `spawn_storage_maintenance_task` always does this,
        // but this test can't rely on that having raced ahead by now.
        leader_log.apply_committed(5).unwrap();
        leader_log.compact(4).unwrap();

        // The leader's next heartbeat attempt will be rejected (the
        // follower's log no longer reaches where the leader still thinks
        // it does), backing `next_index` down to a point the leader's own
        // log can no longer answer for -- triggering a real
        // `InstallSnapshot`. Wait for the follower to end up with the
        // complete, correct state again.
        tokio::time::timeout(Duration::from_millis(3000), async {
            loop {
                if (0..5u64).all(|i| {
                    follower_log
                        .get_latest(format!("k{i}").as_bytes())
                        .unwrap()
                        .is_some_and(|(_, v)| v == format!("v{i}").into_bytes())
                }) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("follower should be caught up via a real InstallSnapshot RPC");

        cleanup(&path_a);
        cleanup(&path_b);
    }
}
