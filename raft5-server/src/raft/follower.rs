//! The follower role: answers AppendEntries (log-matching, §5.3 fast
//! backup) and RequestVote (§5.4.1 up-to-date check), and starts an
//! election if it hasn't heard from a leader in time. See the parent
//! module's doc for how roles fit together generally.

use log::info;

use super::*;

/// The outcome of matching+applying one `AppendEntries` call's `entries`
/// against storage. Doesn't carry `term` -- that's `Common`'s concern, not
/// storage's.
struct AppendOutcome {
    success: bool,
    match_index: u64,
    conflict_index: Option<u64>,
    conflict_term: Option<u64>,
    /// Whether this call may have changed the active cluster
    /// configuration -- true if it appended a `Config` entry, and also
    /// true if it truncated anything, since a rolled-back `Config` entry
    /// reverts membership just as surely as a new one changes it.
    ///
    /// Necessary because a configuration takes effect on *append* (Raft
    /// §4.1), so the follower has to react to it here rather than waiting
    /// for it to commit. See `storage::RaftLog::current_config`.
    config_may_have_changed: bool,
}

impl AppendOutcome {
    fn reject(conflict_index: u64, conflict_term: Option<u64>) -> Self {
        Self {
            success: false,
            match_index: 0,
            conflict_index: Some(conflict_index),
            conflict_term,
            config_may_have_changed: false,
        }
    }
}

/// The synchronous core of Raft's AppendEntries receiver logic (Raft paper
/// §5.3), run on the blocking pool by `Follower::apply_append_entries`.
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
    let mut config_may_have_changed = false;
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
                    // Conservative: we don't check whether the discarded
                    // suffix actually held a `Config` entry, only that
                    // something was discarded. Truncation is rare, and
                    // reloading when nothing changed is free.
                    config_may_have_changed = true;
                }
                config_may_have_changed |= entries[i..]
                    .iter()
                    .any(|e| e.entry_kind() == storage::EntryKind::Config);
                for fresh in &entries[i..] {
                    // `append_entry`, not `append`: the entry's kind comes
                    // from the leader and has to be preserved verbatim, or
                    // our copy of the log stops matching theirs.
                    let seq = log.append_entry(
                        fresh.term,
                        fresh.entry_kind(),
                        &fresh.key,
                        fresh.value.as_deref(),
                    )?;
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
        config_may_have_changed,
    })
}

/// Watches `lease_expires_at_rx` and fires `Event::ElectionTimeout` once
/// the lease it tracks has genuinely expired -- no fresh deadline pushed
/// (by `Follower::renew_lease`, from *either* the event loop or
/// `RaftPeerHandle::get_lease`'s fully independent fast path -- see the
/// module doc on why that one specifically must never be blockable) for a
/// full `LEASE_DURATION` -- *and* an additional random `fuzz(0,
/// LEASE_DURATION)` on top of that.
///
/// Two sequential phases rather than one combined random draw,
/// deliberately: the first (`LEASE_DURATION`, no jitter) is a clean,
/// deterministic detection that contact was actually lost, not muddied by
/// randomness; only the second phase is randomized, to stagger multiple
/// followers' actual campaign start times and avoid a synchronized
/// thundering herd.
///
/// `None` (no lease ever established, e.g. right at node startup) gets
/// the same full grace period as a fresh grant would, timed from when
/// this watcher itself started (i.e. this `Follower` period began) rather
/// than recomputed on every loop iteration -- recomputing against
/// `Instant::now()` each time would make the deadline a perpetually
/// moving target that never actually arrives.
///
/// Reactive rather than an abort-and-respawn timer, on purpose: a
/// `watch::Receiver` wakes up on `changed()` regardless of *which* writer
/// pushed the update, so this correctly restarts from scratch on a fresh
/// deadline from either source without either one needing to reach into
/// the other's task.
fn spawn_lease_expiration_watcher(
    cancel: &CancellationToken,
    mut lease_rx: watch::Receiver<Option<Instant>>,
    event_tx: mpsc::Sender<Event>,
) -> Task {
    Task::spawn(cancel, move |cancel| async move {
        let started_at = Instant::now();
        loop {
            let deadline = (*lease_rx.borrow()).unwrap_or(started_at + LEASE_DURATION);
            let now = Instant::now();
            if now < deadline {
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => return,
                    _ = tokio::time::sleep(deadline - now) => {}
                    changed = lease_rx.changed() => {
                        if changed.is_err() {
                            return; // node shutting down (Common dropped)
                        }
                    }
                }
                continue;
            }

            let fuzz = Duration::from_millis(random_jitter_millis(LEASE_DURATION_MS));
            tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                _ = tokio::time::sleep(fuzz) => {}
                changed = lease_rx.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    continue; // renewed mid-fuzz -- restart from the top
                }
            }
            // The deadline could have moved again right as the fuzz
            // elapsed (a race between the two `select!` arms) -- double
            // check before actually declaring the lease dead.
            let still_expired = match *lease_rx.borrow() {
                Some(deadline) => Instant::now() >= deadline,
                None => true,
            };
            if !still_expired {
                continue;
            }
            send_event(&cancel, &event_tx, Event::ElectionTimeout).await;
            return;
        }
    })
}

/// What a follower publishes as its role: `Uninitialized` while it has no
/// cluster configuration at all, `Follower` once it has one. The two are
/// the same role internally -- an uninitialized node answers every RPC
/// and is replicated to exactly like a follower, which is how a server
/// added to an existing cluster receives the very `Config` entry that
/// initializes it. The distinction is for clients: only one of the two
/// can be bootstrapped.
fn follower_handle(common: &Common, leader_hint: Option<String>) -> RoleHandle {
    if common.config.members.is_empty() {
        RoleHandle::Uninitialized
    } else {
        RoleHandle::Follower(FollowerHandle { leader_hint })
    }
}

pub struct Follower {
    common: Common,
    election_timer: Task,
    leader_hint: Option<String>,
}

impl Follower {
    // See the comment on `Leader::new` re: this not returning `Self`.
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(mut common: Common) -> (Role, Common) {
        // A fresh `Follower` period means this node has (or just got) a
        // reason to believe a leader exists -- any backoff accumulated
        // from a past run of failed election attempts no longer applies,
        // so the next one starts fresh at the base timeout again.
        common.election_attempt = 0;
        common.publish_role(follower_handle(&common, None));
        info!("Transitioned to follower: term {}", common.current_term);

        // A learner -- or a server that has been removed from the
        // configuration -- never campaigns, so it gets no election timer
        // at all rather than one whose every firing has to be discarded.
        // Being promoted to voter re-enters `Follower`, which is what
        // arms one.
        let election_timer = if common.is_voter() {
            spawn_lease_expiration_watcher(
                common.cancel_token(),
                common.lease_expires_at_rx.clone(),
                common.event_tx.clone(),
            )
        } else {
            // Nothing to watch for, but still a real `Task` rather than a
            // bare handle: `shutdown` treats every role's timer the same
            // way, and a `pending` future left to be dropped is exactly
            // the loose end this is all meant to avoid.
            Task::spawn(common.cancel_token(), |cancel| async move {
                cancel.cancelled().await;
            })
        };

        let mut follower = Follower {
            common,
            election_timer,
            leader_hint: None,
        };
        let next_role = follower.run().await;
        (next_role, follower.shutdown().await)
    }

    fn not_leader_error(&self) -> RaftError {
        RaftError::NotLeader {
            leader_hint: self.leader_hint.clone(),
        }
    }

    /// Forms a single-node cluster around this node by appending
    /// `config` as the first entry in the log (Raft §4.4).
    ///
    /// Refused unless this node has neither a configuration nor a single
    /// log entry. Bootstrapping a node that already has either is how a
    /// cluster gets silently split in two -- both halves would believe
    /// they are the whole thing, and both could elect a leader. A server
    /// meant to *join* an existing cluster is added from that cluster's
    /// leader instead, and receives its configuration by replication.
    async fn bootstrap(&mut self, config: ClusterConfig) -> Result<(), RaftError> {
        if !self.common.config.members.is_empty() {
            return Err(RaftError::AlreadyInitialized);
        }
        let bytes = config.to_bytes();
        let appended = self
            .common
            .store
            .run(move |log| {
                if log.last_entry()?.is_some() {
                    return Ok(false);
                }
                // Term 1, not 0: 0 is the "before any term" sentinel that an
                // empty log reports, and this entry has to be reachable by
                // the §5.4.1 up-to-date check like any other.
                log.append_config(1, &bytes)?;
                storage::Result::Ok(true)
            })
            .await
            .map_err(RaftError::Storage)?;

        if !appended {
            return Err(RaftError::AlreadyInitialized);
        }
        self.common.reload_config().await;
        Ok(())
    }

    /// Re-reads the cluster configuration, republishes this node's role
    /// handle to match, and -- if its own right to vote changed --
    /// re-enters `Follower` so the election timer is rebuilt: a learner
    /// has none at all, and a node just promoted to voter needs one armed
    /// (see `Follower::new`).
    async fn reload_config(&mut self) -> Transition {
        let voting_changed = self.common.reload_config().await;
        // Republished unconditionally: gaining (or losing) membership
        // changes which handle we publish even when our right to vote is
        // unaffected -- a server added as a learner goes from
        // `Uninitialized` to `Follower` while staying a non-voter
        // throughout.
        self.common
            .publish_role(follower_handle(&self.common, self.leader_hint.clone()));
        if voting_changed {
            Transition::Become(Role::Follower)
        } else {
            Transition::Continue
        }
    }

    /// Called on any sign that campaigning right now would be premature:
    /// a live leader's `AppendEntries`, a live leader's lease probe, or
    /// granting our own vote to someone else (give them a chance to win
    /// before also jumping in ourselves) -- these three cases are folded
    /// into one shared deadline, so granting a vote also means refusing
    /// *other* vote requests for one `LEASE_DURATION`, same as genuine
    /// leader contact would. Just publishes a fresh deadline; the
    /// reactive watcher task picks it up on its own.
    fn renew_lease(&mut self) {
        self.common
            .lease_expires_at_tx
            .send_replace(Some(Instant::now() + LEASE_DURATION));
    }

    /// Whether we should refuse *any* vote request (real or pre-vote) for
    /// someone other than our current leader, regardless of term or log
    /// -- see the module doc on why a leader a majority can still reach
    /// should never be disrupted by a competing election. `None` (no
    /// lease ever established) is *not* active -- a node that's never
    /// actually confirmed a leader has no one to protect.
    fn has_active_lease(&self) -> bool {
        match *self.common.lease_expires_at_rx.borrow() {
            Some(deadline) => Instant::now() < deadline,
            None => false,
        }
    }

    /// Runs `apply_append_entries_sync` on the blocking pool, then advances
    /// `commit_tx` to `min(leader_commit, match_index)` (only if the
    /// entries themselves matched) and `compact_tx` to `req.compact_seq`
    /// (regardless -- it's independent of whether this particular batch of
    /// entries matched, and by this point the caller has already confirmed
    /// `req.term` is at least as current as ours, so the hint is trusted).
    /// Panics on a storage failure -- see the module doc.
    async fn apply_append_entries(&mut self, req: &AppendEntriesRequest) -> AppendOutcome {
        let prev_log_index = req.prev_log_index;
        let prev_log_term = req.prev_log_term;
        let entries = req.entries.clone();
        let outcome = self
            .common
            .store
            .expect("apply append entries to storage", move |log| {
                apply_append_entries_sync(log, prev_log_index, prev_log_term, &entries)
            })
            .await;

        if outcome.success {
            self.common
                .advance_commit(req.leader_commit.min(outcome.match_index));
        }
        self.common.advance_compact(req.compact_seq);
        outcome
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
                    .publish_role(follower_handle(&self.common, self.leader_hint.clone()));
                self.renew_lease();

                let outcome = self.apply_append_entries(&req).await;
                let _ = respond_to.send(AppendEntriesResponse {
                    term: self.common.current_term,
                    success: outcome.success,
                    match_index: outcome.match_index,
                    conflict_index: outcome.conflict_index,
                    conflict_term: outcome.conflict_term,
                });
                // Answer first, then react: the leader is waiting on this
                // response, and reloading is our own bookkeeping.
                if outcome.config_may_have_changed {
                    self.reload_config().await
                } else {
                    Transition::Continue
                }
            }
            Event::InstallSnapshot {
                header,
                data,
                respond_to,
            } => {
                if header.term < self.common.current_term {
                    let _ = respond_to.send(InstallSnapshotResponse {
                        term: self.common.current_term,
                    });
                    return Transition::Continue;
                }
                if header.term > self.common.current_term {
                    self.common.advance_term(header.term).await;
                }
                self.leader_hint = Some(header.leader_id.clone());
                self.common
                    .publish_role(follower_handle(&self.common, self.leader_hint.clone()));
                self.renew_lease();

                self.common
                    .store
                    .expect("install snapshot", move |log| {
                        log.install_snapshot(&mut data.as_slice())
                    })
                    .await;

                // A snapshot only ever contains already-committed state
                // (Leader Completeness) -- both watermarks can jump
                // straight to it, same as AppendEntries's
                // leader_commit/compact_seq hints do.
                self.common.advance_commit(header.last_included_index);
                self.common.advance_compact(header.last_included_index);

                let _ = respond_to.send(InstallSnapshotResponse {
                    term: self.common.current_term,
                });
                // An install wipes `log`, taking any `Config` entry with
                // it; the snapshot's own header is where membership comes
                // from now (see `storage::RaftLog::install_snapshot`), so
                // this is never optional.
                self.reload_config().await
            }
            Event::RequestVote { req, respond_to } => {
                if self.has_active_lease() {
                    // Refuse to disrupt a leader we've recently confirmed
                    // is alive, regardless of term, log, or whether this
                    // is a real vote or just a pre-vote -- this is what
                    // stops a competing election from ever succeeding
                    // against a leader a majority can still reach.
                    let _ = respond_to.send(RequestVoteResponse {
                        term: self.common.current_term,
                        vote_granted: false,
                    });
                    return Transition::Continue;
                }
                if req.dry_run {
                    let grant = evaluate_dry_run_vote(&self.common, &req).await;
                    let _ = respond_to.send(RequestVoteResponse {
                        term: self.common.current_term,
                        vote_granted: grant,
                    });
                    return Transition::Continue;
                }
                let grant = evaluate_real_vote(&mut self.common, &req).await;
                if grant {
                    self.renew_lease();
                }
                let _ = respond_to.send(RequestVoteResponse {
                    term: self.common.current_term,
                    vote_granted: grant,
                });
                Transition::Continue
            }
            Event::Bootstrap { config, respond_to } => {
                let outcome = self.bootstrap(config).await;
                let accepted = outcome.is_ok();
                let _ = respond_to.send(outcome);
                if accepted {
                    // We just became a voting member of a (single-node)
                    // cluster, so re-enter `Follower` to get an election
                    // timer -- there was none while uninitialized.
                    Transition::Become(Role::Follower)
                } else {
                    Transition::Continue
                }
            }
            Event::ElectionTimeout => {
                // A learner (or a server that has been removed from the
                // configuration) must never campaign: it has no vote, so
                // it could never win, and a removed server campaigning is
                // exactly the disruption §4.2.3 warns about. It still
                // replicates and still answers RPCs -- it just waits.
                //
                // `Follower::new` doesn't arm a timer for such a node at
                // all; this is here for the window where the
                // configuration changes under an already-in-flight one.
                if self.common.is_voter() {
                    Transition::Become(Role::PreCandidate)
                } else {
                    Transition::Continue
                }
            }
            Event::AppendEntriesResponse { .. }
            | Event::RequestVoteResponse { .. }
            | Event::GetLeaseResponse { .. }
            | Event::InstallSnapshotResponse { .. }
            | Event::LeaderTick => {
                Transition::Continue // stray messages from a role we no longer hold
            }
            Event::ProposeConfig { respond_to, .. } => {
                let _ = respond_to.send(Err(self.not_leader_error()));
                Transition::Continue
            }
        }
    }

    async fn shutdown(self) -> Common {
        self.election_timer.stop().await;
        self.common
    }
}

#[cfg(test)]
mod tests;
