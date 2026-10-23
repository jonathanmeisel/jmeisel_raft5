//! The leader role: replicates its log to every peer and advances the
//! commit and compaction watermarks as peers catch up. See the parent
//! module's doc for how roles fit together.

use super::*;
// Only a leader sends `GetLeaseRequest`, and nobody receives it through
// `Event`, so unlike the rest of the peer protocol it does not come in
// through `super::*`.
use crate::pb::raft5::raft::{
    AppendEntriesChunk, AppendEntriesHeader, EntryBatch, GetLeaseRequest, SnapshotChunk,
    append_entries_chunk, snapshot_chunk,
};

mod lease;
pub(super) mod replication;

use lease::spawn_lease_prober;
use tracing::info;
use replication::spawn_replication;

/// Push `Event::LeaderTick` at intervals. See that variant's doc for why
/// the leader needs this as well as reacting to peer responses.
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
    /// One replication task per peer, keyed by node id, so a membership change
    /// starts or stops only the tasks that changed. Restarting every task would
    /// lose each task's per-peer `next_index`. Every member except us, learners
    /// included.
    replication_tasks: HashMap<String, Task>,
    /// Same, for lease probes, but voting members only.
    lease_tasks: HashMap<String, Task>,
    tick_timer: Task,
    /// The highest seq each peer is known to have replicated. A peer we have
    /// not heard from yet has no entry; `recompute_watermarks` treats a missing
    /// peer as 0, so the map needs no pre-seeding.
    peer_match_index: HashMap<String, u64>,
    /// When each peer's last granted lease stops counting toward quorum (the
    /// probe's send time plus the lease it asked for; see
    /// `Event::GetLeaseResponse`). No entry means never granted, or known
    /// stale, and is treated as expired.
    peer_lease_expiry: HashMap<String, Instant>,
    read_quorum_tx: watch::Sender<bool>,
    /// The seq of this leader's term-opening no-op. By Raft §5.4.2, commit
    /// cannot pass this point until the no-op itself reaches a majority, so
    /// `commit_seq >= noop_seq` means "this leader has committed an entry from
    /// its own term", which a membership change needs first (see
    /// `propose_config`).
    noop_seq: u64,
    /// The seq of a `Config` entry this leader appended that has not
    /// committed yet, if any. Raft allows only one membership change in flight
    /// at a time (§4.1).
    pending_config_seq: Option<u64>,
    /// Catch-up progress per learner, for automatic promotion. Keyed by node
    /// id; an entry exists only while a member is a non-voting learner.
    catch_up: HashMap<String, CatchUpState>,
}

/// One round of a learner catching up (Raft §4.2.1). A round starts at the
/// leader's log tail at that moment, and the learner has caught up when its
/// `match_index` reaches that point. What matters is how fast, not only
/// that it finished: a round that ends within an election timeout means
/// this server can keep up in real time, which is the real condition for
/// giving it a vote.
struct CatchUpState {
    round_start_index: u64,
    round_started_at: Instant,
}

impl Leader {
    /// Start one replication task per peer plus a self-tick, publish a
    /// `LeaderHandle`, then run the event loop until a role change is needed.
    // Named `new` (paired with `shutdown`) but does not return `Self`: it runs
    // the whole role lifecycle and returns `Common`, from which the driver
    // builds the next role.
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(common: Common) -> (Role, Common) {
        // Raft §5.4.2 (Figure 8): a leader may only advance `commit_seq` over an
        // entry from its own term. A no-op first means such an entry always
        // exists. Without it, a leader that never gets a client write could never
        // commit its inherited tail, and `recompute_watermarks` would stall
        // forever. Done before the `RoleHandle` is published and before any
        // replication task starts, so the no-op is in the log from the first
        // `AppendEntries` we send.
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
        // Starting from empty maps, this starts the full initial set, through the
        // same code path a later membership change uses.
        leader.reconcile_peers().await;
        // A cluster with no peers is already at majority (just itself), so bring
        // commit, compaction and read quorum up to date without waiting for the
        // first tick.
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
                // A higher term means someone else won an election; step down, whoever
                // they are.
                if req.term > self.common.current_term {
                    self.common.advance_term(req.term).await;
                    let _ = respond_to.send(self.reject_append(req.term));
                    return Transition::Become(Role::Follower);
                }
                // Otherwise it is a stale or duplicate leader: reject and stay leader.
                // This should not happen if the term is the only leadership marker, but
                // the guard is harmless.
                let _ = respond_to.send(self.reject_append(self.common.current_term));
                Transition::Continue
            }
            Event::AppendEntriesResponse { from, resp } => {
                // No term check: a response from a newer term never reaches a role's
                // event loop (see `Common::next_event`).
                //
                // A failure's `match_index` means nothing (0); only a success tells us
                // the peer's real progress.
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
                // Same shape as the AppendEntries arm above: a higher term means someone
                // else leads now, so step down; otherwise it is stale, so reject and stay
                // leader. `data` is unused: a leader never applies an inbound snapshot.
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
                // The response carried only a term, and a newer term never reaches here
                // (see `Common::next_event`).
                resp: _,
            } => {
                let entry = self.peer_match_index.entry(from).or_insert(0);
                *entry = (*entry).max(last_included_seq);
                self.recompute_watermarks().await;
                // A learner far enough behind to need a snapshot makes progress here,
                // not through AppendEntries, so its catch-up round must be judged here
                // too.
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
                // `voted_for == Some(self)` in our own `current_term` always denies a
                // same-term request. A higher-term request is also subject to the
                // §5.4.1 log check, so it is not granted just for its term.
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
                    // Replace, do not take the max with the old deadline: probes go out one
                    // at a time per peer, so a later response is always for a later send.
                    self.peer_lease_expiry.insert(from, expires_at);
                } else {
                    // An explicit denial is stronger evidence than silence: this peer does
                    // not trust us now, so stop counting an older grant.
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
            // A leader is in a cluster by definition.
            Event::Bootstrap { respond_to, .. } => {
                let _ = respond_to.send(Err(RaftError::AlreadyInitialized));
                Transition::Continue
            }
        }
    }

    /// Append a membership change to the log, where it takes effect at once
    /// (§4.1: on append, not on commit). Returns the seq of the `Config`
    /// entry, so the caller can wait for its commit.
    ///
    /// Two guards, both against building on ground that can still move:
    ///
    /// * **One change at a time** (§4.1). Two overlapping single-server
    ///   changes can produce configurations with no majority in common: the
    ///   exact disjoint-quorum split that single-server changes exist to
    ///   avoid. Enforced by refusing while a `Config` entry we appended is
    ///   uncommitted.
    /// * **A committed entry from our own term** (Ongaro 2015). A new leader's
    ///   inherited tail is not committed yet, so the configuration it reads
    ///   could still be rolled back by a later leader, and a change built on a
    ///   configuration that never existed can also produce two disjoint
    ///   quorums. `noop_seq` is the entry that clears this.
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
        // In force now, so start (or stop) replicating to the affected server at
        // once.
        self.common.reload_config().await;
        self.reconcile_peers().await;
        Ok(seq)
    }

    /// Raft §4.2.2: a leader that removes itself keeps serving until the entry
    /// that removes it commits (it is the only node that can commit that
    /// entry), and steps down as soon as it does. An earlier step-down would
    /// leave nobody able to finish the change.
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

    /// Raft §4.2.1: promote a learner that has shown it can keep up with the
    /// cluster in real time.
    ///
    /// A learner is measured in rounds. Each round fixes a target (the
    /// leader's log tail when the round starts) and ends when the learner's
    /// `match_index` reaches it. The round's duration is the test: finishing
    /// within an election timeout means this server replicates as fast as the
    /// cluster produces entries, so a vote for it will not stall commit. A
    /// slower round just starts another one. There is no failure path: a
    /// learner that never converges stays a learner, replicated to but counted
    /// in no quorum.
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
                    // Caught up, but too slowly to get a vote yet. Try again with a new
                    // target.
                    state.round_start_index = own_last_index;
                    state.round_started_at = now;
                }
                Some(_) => {} // still working through the current round
            }
        }

        if let Some(server_id) = promote {
            // Best effort: the guards in `propose_config` can refuse now for valid
            // reasons (another change in flight, for example). The learner stays a
            // learner and is checked again on the next response, so there is nothing
            // to recover from.
            let _ = self
                .propose_config(ConfigChange::Promote { server_id })
                .await;
        }
    }

    /// Bring the replication and lease-probe tasks in line with the current
    /// configuration: start one for each member that has none, and stop (and
    /// forget) the ones for members that left.
    ///
    /// Incremental on purpose. A replication task owns this leader's
    /// `next_index` for its peer, found over possibly many round trips of
    /// §5.3 backup. Restarting every task on every membership change would
    /// throw that away and probe the whole cluster again because one server
    /// was added.
    ///
    /// Removing a departed peer's `peer_match_index` and `peer_lease_expiry`
    /// entries is required: a stale entry for a former member would keep being
    /// counted, and hold `compact_seq` down forever at the index where the
    /// removed node stopped.
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

        // Signal in one pass and join in a second, so several leaving peers wind
        // down concurrently. Signal before awaiting any, because a task already
        // blocked on `event_tx.send` can only finish once it learns it is not
        // wanted (this same event loop would otherwise drain it).
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
        // A promoted learner no longer needs measuring.
        self.catch_up
            .retain(|id, _| self.common.config.member(id).is_some_and(|m| !m.voting));
        for id in &dropped {
            if let Some(task) = self.replication_tasks.remove(id) {
                task.stop().await;
            }
            self.peer_match_index.remove(id);
            self.catch_up.remove(id);
            // Nothing will dial this peer again, so close its connection.
            self.common.peer_conns.forget(id);
        }

        if !dropped.is_empty() || !demoted.is_empty() {
            // Both quorums just got smaller, so an entry may already be committed,
            // or read quorum already held, from responses we already have.
            self.recompute_watermarks().await;
            self.recompute_lease_quorum();
        }
    }

    /// Recompute `commit_seq` (highest seq replicated to a majority of voters)
    /// and `compact_seq` (lowest seq replicated to every member, learners
    /// included) from `peer_match_index`, and publish both through `Common`.
    /// Both only move forward, so a stale or out-of-order response can never
    /// lower either.
    ///
    /// The two watermarks use different groups on purpose:
    /// - Commit is a safety property and belongs to voters alone: counting a
    ///   learner would let an entry commit because of a node that cannot vote
    ///   in the election that must preserve it.
    /// - Compaction is only about what is safe to delete, so it must wait for
    ///   everyone who might still need an entry, including a learner that is
    ///   still catching up.
    async fn recompute_watermarks(&mut self) {
        let (own_last_index, _) = last_log_index_and_term(&self.common.store).await;
        let matched_for = |peer: &PeerInfo| *self.peer_match_index.get(&peer.id).unwrap_or(&0);

        let mut voter_matched: Vec<u64> = self
            .common
            .voting_peers()
            .iter()
            .map(&matched_for)
            .collect();
        // Our own log counts only if we are a voter. A leader removed from the
        // configuration keeps serving until the entry that removes it commits
        // (§4.2.2), but it is not part of the quorum that decides that.
        if self.common.is_voter() {
            voter_matched.push(own_last_index);
        }
        voter_matched.sort_unstable_by(|a, b| b.cmp(a)); // descending

        let mut all_matched: Vec<u64> = self.common.peers().iter().map(&matched_for).collect();
        all_matched.push(own_last_index);

        // The value at position `majority - 1` is the majority-th largest: the
        // highest value that at least `majority` voters have reached. A
        // configuration with no voters decides nothing; nothing commits until one
        // exists.
        let majority = self.common.majority();
        if voter_matched.len() >= majority {
            let commit_seq = voter_matched[majority - 1];

            // Raft §5.4.2 (Figure 8): a majority alone is not enough to commit an
            // entry from an earlier term, because a later leader can still overwrite
            // it. Only an entry from our own term can be counted directly, and
            // committing it commits everything before it (Log Matching Property).
            // One check at `commit_seq` covers the whole prefix: terms never decrease
            // along the log, so if that entry is not from our term, nothing below it
            // is.
            //
            // `Leader::new` appends a no-op at the start of every term, so this gate
            // always has something it can open on: it delays commit until that no-op
            // replicates, and never blocks it forever.
            if term_at(&self.common.store, commit_seq).await == Some(self.common.current_term) {
                self.common.advance_commit(commit_seq);
                // The one-change-at-a-time guard lifts as soon as the change it waits for
                // commits.
                if self.pending_config_seq.is_some_and(|seq| commit_seq >= seq) {
                    self.pending_config_seq = None;
                }
            }
        }

        // The smallest value: the one that every member, without exception, has
        // reached.
        let compact_seq = all_matched.into_iter().min().expect("always has our own");
        self.common.advance_compact(compact_seq);
    }

    /// Recompute whether this leader holds a quorum of live leases (itself,
    /// plus every peer whose last grant has not expired), and publish the
    /// result. In memory only (no storage read, unlike
    /// `recompute_watermarks`), so this is a plain sync fn.
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
            // We always trust ourselves, but, as with commit, only if we are a voter.
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

    /// Stop every task this leader owns (replication, lease probes, the
    /// self-tick), and return `Common` only after all have returned. So
    /// whatever they held (peer connections, an `Arc<RaftLog>`) is gone before
    /// the next role, or the shutting-down node, gets `Common` back.
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
