//! The leader role: replicates its log to every peer and advances the
//! commit/compaction watermarks as they catch up. See the parent module's
//! doc for how roles fit together generally.

use super::*;
// `GetLeaseRequest` is only ever *sent*, and only by a leader, so unlike
// the rest of the peer protocol it isn't part of `Event` and doesn't come
// in through `super::*`.
use crate::pb::raft5::raft::{
    AppendEntriesChunk, AppendEntriesHeader, EntryBatch, GetLeaseRequest, SnapshotChunk,
    append_entries_chunk, snapshot_chunk,
};

mod lease;
pub(super) mod replication;

use lease::spawn_lease_prober;
use tracing::info;
use replication::spawn_replication;

/// Periodically pushes `Event::LeaderTick` -- see the variant's doc for why
/// the leader needs this in addition to reacting to peer responses.
fn spawn_leader_tick(
    cancel: &CancellationToken,
    timing_rx: watch::Receiver<Timing>,
    event_tx: mpsc::Sender<Event>,
) -> Task {
    Task::spawn(cancel, move |cancel| async move {
        loop {
            let heartbeat = timing_rx.borrow().heartbeat();
            if until_cancelled(&cancel, tokio::time::sleep(heartbeat))
                .await
                .is_none()
                || !send_event(&cancel, &event_tx, Event::LeaderTick).await
            {
                return;
            }
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
    replication_tasks: HashMap<String, Task>,
    /// Same, for lease probes -- but voting members only.
    lease_tasks: HashMap<String, Task>,
    tick_timer: Task,
    /// Highest seq each peer is known to have replicated. Missing entry
    /// (rather than 0) for a peer we haven't heard back from yet -- same
    /// thing numerically, `recompute_watermarks` just treats an absent
    /// peer as 0 rather than requiring this to be pre-seeded.
    peer_match_index: HashMap<String, u64>,
    /// When each peer's last granted lease stops counting toward quorum
    /// (the probe's send time plus the lease it asked for -- see
    /// `Event::GetLeaseResponse`).
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
            let term = common.current_term;
            common
                .store
                .expect("append the leader no-op", move |log| log.append_noop(term))
                .await
        };
        common.notify_appended(noop_seq);

        let applied_rx = common.applied_tx.subscribe();
        let role_rx = common.role_tx.subscribe();
        let (read_quorum_tx, read_quorum_rx) = watch::channel(false);
        common.publish_role(RoleHandle::Leader(LeaderHandle {
            store: common.store.clone(),
            term: common.current_term,
            event_tx: common.event_tx.clone(),
            applied_rx,
            role_rx,
            read_quorum_rx,
            noop_seq,
            log_tx: Arc::clone(&common.log_tx),
        }));

        let tick_timer = spawn_leader_tick(
            common.cancel_token(),
            common.timing_tx.subscribe(),
            common.event_tx.clone(),
        );

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
        (next_role, leader.shutdown().await)
    }

    async fn run(&mut self) -> Role {
        loop {
            let event = match self.common.next_event().await {
                Next::Handle(event) => event,
                Next::StepDown => return Role::Follower,
                Next::Stop => return Role::Stopped,
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
                // No term check: a response from a newer term never
                // reaches a role's event loop at all (see
                // `Common::next_event`).
                //
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
                last_included_seq,
                // The only thing the response itself carried was a term,
                // and a newer one never reaches here (see
                // `Common::next_event`).
                resp: _,
            } => {
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
            Event::GetLeaseResponse {
                from,
                resp,
                expires_at,
            } => {
                if resp.lease_granted {
                    // Kept, not maxed against the old deadline: probes
                    // go out one at a time per peer, so a later response
                    // is always for a later send.
                    self.peer_lease_expiry.insert(from, expires_at);
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
        let term = self.common.current_term;
        let bytes = next.to_bytes();
        let seq = self
            .common
            .store
            .run(move |log| log.append_config(term, &bytes))
            .await
            .map_err(RaftError::Storage)?;
        self.common.notify_appended(seq);

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

        let (own_last_index, _) = last_log_index_and_term(&self.common.store).await;
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
                    if now.duration_since(state.round_started_at) <= self.common.timing().lease() {
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
                    spawn_replication(
                        self.common.cancel_token(),
                        peer.clone(),
                        self.common.node_id.clone(),
                        self.common.current_term,
                        self.common.store.clone(),
                        Arc::clone(&self.common.peer_conns),
                        self.common.timing_tx.subscribe(),
                        self.common.log_tx.subscribe(),
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
                        self.common.cancel_token(),
                        peer.clone(),
                        self.common.node_id.clone(),
                        self.common.current_term,
                        Arc::clone(&self.common.peer_conns),
                        self.common.timing_tx.subscribe(),
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

        // Signalled in one pass and joined in a second, so several peers
        // leaving at once wind down concurrently -- and signalled *before*
        // any is awaited, since a task already parked on `event_tx.send`
        // can only finish once it hears it is no longer wanted (this same
        // event loop is what would otherwise be draining it).
        for id in &demoted {
            if let Some(task) = self.lease_tasks.get(id) {
                task.signal();
            }
        }
        for id in &dropped {
            if let Some(task) = self.replication_tasks.get(id) {
                task.signal();
            }
        }
        for id in &demoted {
            if let Some(task) = self.lease_tasks.remove(id) {
                task.stop().await;
            }
            self.peer_lease_expiry.remove(id);
        }
        // A promoted learner is done being measured.
        self.catch_up
            .retain(|id, _| self.common.config.member(id).is_some_and(|m| !m.voting));
        for id in &dropped {
            if let Some(task) = self.replication_tasks.remove(id) {
                task.stop().await;
            }
            self.peer_match_index.remove(id);
            self.catch_up.remove(id);
            // Nothing will dial this peer again, so don't hold its
            // connection open.
            self.common.peer_conns.forget(id);
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
        let (own_last_index, _) = last_log_index_and_term(&self.common.store).await;
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
            if term_at(&self.common.store, commit_seq).await == Some(self.common.current_term) {
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
        self.read_quorum_tx.send_replace(has_quorum);
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

    /// Stops every task this leader owns -- replication, lease probes and
    /// the self-tick -- and hands `Common` back only once they have all
    /// actually returned. Whatever they were holding (peer connections,
    /// an `Arc<RaftLog>`) is therefore gone by the time the next role, or
    /// the shutting-down node, gets `Common` back.
    async fn shutdown(self) -> Common {
        Task::stop_all(
            self.replication_tasks
                .into_values()
                .chain(self.lease_tasks.into_values())
                .chain(std::iter::once(self.tick_timer)),
        )
        .await;
        self.common
    }
}

#[cfg(test)]
mod tests;
