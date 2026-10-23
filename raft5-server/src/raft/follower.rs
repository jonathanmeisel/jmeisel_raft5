//! The follower role: answers AppendEntries (log matching, §5.3 fast
//! backup) and RequestVote (§5.4.1 up-to-date check), and starts an
//! election if it does not hear from a leader in time. See the parent
//! module's doc for how roles fit together.

use tracing::info;

use super::*;
use crate::storage::{BootstrapRefusal, RestoreOutcome, StorageError};

/// What the storage half of `Follower::bootstrap` did.
enum Booted {
    /// A plain bootstrap appended the configuration.
    Plain,
    /// A bootstrap from a backup, and what the restore did.
    Restore(RestoreOutcome),
    /// A plain bootstrap was refused.
    Refused(BootstrapRefusal),
}

/// The result of matching and applying one `AppendEntries` call's
/// `entries` against storage. Has no `term`: that is `Common`'s concern,
/// not storage's.
struct AppendOutcome {
    success: bool,
    match_index: u64,
    conflict_index: Option<u64>,
    conflict_term: Option<u64>,
    /// Whether this call may have changed the active cluster configuration:
    /// true if it appended a `Config` entry, and also true if it truncated
    /// anything, because a rolled-back `Config` entry changes membership as
    /// surely as a new one.
    ///
    /// Needed because a configuration takes effect on append (Raft §4.1), so
    /// the follower must react here, not wait for commit. See
    /// `storage::RaftLog::current_config`.
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

/// The synchronous core of Raft's AppendEntries receiver (Raft paper §5.3),
/// run on the blocking pool by `Follower::apply_append_entries`.
///
/// - Check `prev_log_index` and `prev_log_term`.
/// - Walk `entries` against `log`: leave entries already present with a
///   matching term, truncate the divergent suffix at the first mismatch,
///   and append everything past the current tail.
///
/// `entries` must be in ascending `seq` order with no gaps, starting at
/// `prev_log_index + 1`. The leader's log has no gaps, so this holds.
fn apply_append_entries_sync(
    log: &RaftLog,
    prev_log_index: u64,
    prev_log_term: u64,
    entries: &mut Vec<LogEntry>,
) -> storage::Result<AppendOutcome> {
    if prev_log_index > 0 {
        match log.term_at(prev_log_index)? {
            Some(term) if term == prev_log_term => {}
            Some(conflict_term) => {
                // Fast backup (§5.3): point the leader at the first index of the
                // conflicting term, so it can skip the whole run in one round trip
                // instead of backing up one index at a time.
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
                // Our log does not reach prev_log_index: point the leader just past what
                // we have.
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
    let mut appended_from = None;
    for (i, entry) in entries.iter().enumerate() {
        match log.term_at(entry.seq)? {
            // Already present with the same term. By the Log Matching Property, the
            // rest of our log up to here matches too, so there is nothing to do.
            Some(term) if term == entry.term => {
                match_index = entry.seq;
            }
            // A conflicting entry (wrong term) or no entry yet: either way, this entry
            // and everything after it in the batch must be written.
            other => {
                if other.is_some() {
                    log.truncate_from(entry.seq)?;
                    // Conservative: we do not check whether the discarded suffix held a
                    // `Config` entry, only that something was discarded. Truncation is rare,
                    // and a reload with no change costs nothing.
                    config_may_have_changed = true;
                }
                config_may_have_changed |= entries[i..].iter().any(LogEntry::is_config);
                // Where the rest of the batch starts. Appended below, not here, so the
                // entries can be moved into the log: an entry with a whole transaction is
                // large, and `payload()` would clone every mutation of it just before the
                // original is dropped.
                appended_from = Some(i);
                break;
            }
        }
    }

    if let Some(from) = appended_from {
        for fresh in entries.drain(from..) {
            // `append_payload`, not `append`: the payload comes from the leader and
            // must be kept exactly (its kind and every mutation), or our log stops
            // matching the leader's. An entry we cannot decode is refused, not
            // guessed at; see `pb::UnknownEntry`.
            let (expected_seq, expected_term, expected_writes) =
                (fresh.seq, fresh.term, fresh.writes.len());
            let payload = fresh.into_payload().map_err(|_| {
                StorageError::Corrupt("replicated log entry has an unrecognized kind")
            })?;
            debug_assert_eq!(
                payload.mutation_count(),
                expected_writes,
                "a replicated entry lost mutations on the way into the log"
            );
            let seq = log.append_payload(expected_term, &payload)?;
            debug_assert_eq!(
                seq, expected_seq,
                "follower's log position drifted from the leader's indexing"
            );
            match_index = seq;
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

/// Watch `lease_expires_at_rx`, and push `Event::ElectionTimeout` once the
/// tracked lease has really expired: no new deadline for a full lease,
/// plus a random `fuzz(0, lease)` on top.
///
/// New deadlines come from `Follower::renew_lease`, either on the event
/// loop or on the independent fast path in `RaftPeerHandle::get_lease`
/// (see the module doc for why that path must never block). The lease
/// length is read again on every pass, so a heartbeat change applies from
/// the next deadline on.
///
/// Two phases in sequence, not one random draw, on purpose:
/// - The first (one lease, no jitter) is a clean, fixed detection that
///   contact was really lost.
/// - Only the second is random, to spread out the followers' campaign
///   start times and avoid a synchronized stampede.
///
/// `None` (no lease yet, for example at node start) gets the same full
/// grace period as a new grant, timed from when this watcher started (when
/// this `Follower` period began). Recomputing it from `Instant::now()` on
/// each loop would make a deadline that keeps moving and never arrives.
///
/// Reactive, not an abort-and-respawn timer, on purpose: a
/// `watch::Receiver` wakes on `changed()` whoever pushed the update, so a
/// new deadline from either source restarts the wait, and neither source
/// needs to reach into the other's task.
fn spawn_lease_expiration_watcher(
    cancel: &CancellationToken,
    mut lease_rx: watch::Receiver<Option<Instant>>,
    timing_rx: watch::Receiver<Timing>,
    event_tx: mpsc::Sender<Event>,
) -> Task {
    Task::spawn(cancel, move |cancel| async move {
        let started_at = Instant::now();
        loop {
            let timing = *timing_rx.borrow();
            let deadline = (*lease_rx.borrow()).unwrap_or(started_at + timing.lease());
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

            let fuzz = Duration::from_millis(random_jitter_millis(timing.lease_ms()));
            tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                _ = tokio::time::sleep(fuzz) => {}
                changed = lease_rx.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    continue; // renewed mid-fuzz: restart from the top
                }
            }
            // The deadline could have moved again just as the fuzz elapsed (a race
            // between the two `select!` arms), so check again before declaring the
            // lease dead.
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
/// cluster configuration, `Follower` once it has one. Internally both are
/// the same role: an uninitialized node answers every RPC and accepts
/// replication like a follower, which is how a server added to a cluster
/// receives the `Config` entry that initializes it. The difference is for
/// clients: only an uninitialized node can be bootstrapped.
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
    // See the comment on `Leader::new` about this not returning `Self`.
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(mut common: Common) -> (Role, Common) {
        // A new `Follower` period means this node has (or just got) a reason to
        // believe a leader exists. Backoff from earlier failed elections no longer
        // applies, so the next one starts at the base timeout.
        common.election_attempt = 0;
        common.publish_role(follower_handle(&common, None));

        // A learner, or a server removed from the configuration, never campaigns,
        // so it gets no election timer at all, instead of one whose every firing
        // must be ignored. Promotion to voter re-enters `Follower`, which arms one.
        let election_timer = if common.is_voter() {
            spawn_lease_expiration_watcher(
                common.cancel_token(),
                common.lease_expires_at_rx.clone(),
                common.timing_tx.subscribe(),
                common.event_tx.clone(),
            )
        } else {
            // Nothing to watch, but still a real `Task`, not a bare handle:
            // `shutdown` treats every role's timer the same way, and a `pending`
            // future left to be dropped is exactly the loose end this design avoids.
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

    /// Form a single-node cluster around this node by appending `config` as
    /// the first log entry (Raft §4.4). With `restore`, load that backup
    /// first, so the cluster holds its contents (see
    /// `storage::RaftLog::restore_backup`).
    ///
    /// Refused unless this node is untouched: no configuration, no log entry,
    /// and no data (see `storage::RaftLog::bootstrap_refusal`). A bootstrap of
    /// a node in a cluster is how a cluster silently splits in two: both
    /// halves would believe they are the whole cluster, and both could elect a
    /// leader. A server meant to join an existing cluster is added from that
    /// cluster's leader instead, and gets its configuration by replication.
    ///
    /// A restore holds this event loop until the backup is loaded. That is
    /// fine: the node is in no cluster, so nothing else needs the loop yet.
    async fn bootstrap(
        &mut self,
        config: ClusterConfig,
        restore: Option<crate::store::ChunkReader>,
    ) -> Result<(), RaftError> {
        if !self.common.config.members.is_empty() {
            return Err(RaftError::AlreadyInitialized);
        }
        let bytes = config.to_bytes();
        let outcome = self
            .common
            .store
            .run(move |log| match restore {
                Some(mut reader) => log.restore_backup(&mut reader, &bytes).map(Booted::Restore),
                None => {
                    if let Some(refusal) = log.bootstrap_refusal()? {
                        return Ok(Booted::Refused(refusal));
                    }
                    // Term 1, not 0: 0 is the "before any term" value an empty
                    // log reports, and this entry must be reachable by the
                    // §5.4.1 up-to-date check like any other.
                    log.append_config(1, &bytes)?;
                    Ok(Booted::Plain)
                }
            })
            .await
            .map_err(RaftError::Storage)?;

        let refusal = match outcome {
            Booted::Plain => None,
            Booted::Restore(RestoreOutcome::Restored {
                as_of,
                record_count,
                source_node_id,
            }) => {
                info!(as_of, record_count, source_node_id, "restored a backup");
                None
            }
            Booted::Restore(RestoreOutcome::Refused(refusal)) | Booted::Refused(refusal) => {
                Some(refusal)
            }
            Booted::Restore(RestoreOutcome::BadBackup(why)) => {
                return Err(RaftError::BadBackup(why));
            }
        };
        match refusal {
            None => {}
            Some(BootstrapRefusal::AlreadyInitialized) => return Err(RaftError::AlreadyInitialized),
            Some(BootstrapRefusal::Dirty(why)) => return Err(RaftError::BootstrapRefused(why)),
        }
        self.common.reload_config().await;
        Ok(())
    }

    /// Read the cluster configuration again, republish this node's role
    /// handle to match, and, if its own right to vote changed, re-enter
    /// `Follower` so the election timer is rebuilt: a learner has none, and a
    /// newly promoted voter needs one (see `Follower::new`).
    async fn reload_config(&mut self) -> Transition {
        let voting_changed = self.common.reload_config().await;
        // Always republish: gaining or losing membership changes which handle we
        // publish, even when our right to vote does not change. A server added as
        // a learner goes from `Uninitialized` to `Follower` while it stays a
        // non-voter.
        self.common
            .publish_role(follower_handle(&self.common, self.leader_hint.clone()));
        if voting_changed {
            Transition::Become(Role::Follower)
        } else {
            Transition::Continue
        }
    }

    /// Called on any sign that campaigning now would be too early: a live
    /// leader's `AppendEntries`, a live leader's lease probe, or granting our
    /// own vote to someone else (give them a chance to win first). All three
    /// share one deadline, so granting a vote also refuses other vote requests
    /// for one lease, as real leader contact does. Only publishes a new
    /// deadline; the reactive watcher task picks it up.
    fn renew_lease(&mut self) {
        self.common
            .lease_expires_at_tx
            .send_replace(Some(Instant::now() + self.common.timing().lease()));
    }

    /// Whether to refuse every vote request (real or pre-vote) for a node other
    /// than our current leader, whatever its term or log. See the module doc
    /// for why a leader that a majority can reach must never be disrupted by an
    /// election. `None` (no lease yet) is not active: a node that never
    /// confirmed a leader has nobody to protect.
    fn has_active_lease(&self) -> bool {
        match *self.common.lease_expires_at_rx.borrow() {
            Some(deadline) => Instant::now() < deadline,
            None => false,
        }
    }

    /// Run `apply_append_entries_sync` on the blocking pool, then:
    /// - raise `commit_tx` to `min(leader_commit, match_index)`, only if the
    ///   entries matched;
    /// - raise `compact_tx` to `req.compact_seq` in all cases. It does not
    ///   depend on this batch matching, and the caller has already confirmed
    ///   that `req.term` is at least as current as ours, so the hint is
    ///   trusted.
    ///
    /// Panics on a storage failure: see the module doc.
    async fn apply_append_entries(&mut self, req: &mut AppendEntriesRequest) -> AppendOutcome {
        let prev_log_index = req.prev_log_index;
        let prev_log_term = req.prev_log_term;
        // Taken, not cloned. The entries must move to the blocking pool either
        // way, and for a batch with a transaction a clone is a second full copy for
        // nothing: the request does not use them again. `req` keeps the scalars the
        // caller reads below.
        let mut entries = std::mem::take(&mut req.entries);
        // What this batch costs to apply, and how big it is: the follower's share
        // of the round trip the leader waits on, measured where it is paid.
        let entry_count = entries.len();
        let entry_bytes: usize = entries.iter().map(prost::Message::encoded_len).sum();
        let started = Instant::now();
        let outcome = self
            .common
            .store
            .expect("apply append entries to storage", move |log| {
                apply_append_entries_sync(log, prev_log_index, prev_log_term, &mut entries)
            })
            .await;
        if entry_count > 0 {
            info!(
                entry_count,
                entry_bytes,
                append_ms = started.elapsed().as_millis(),
                "follower appended a batch"
            );
        }

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
            Event::AppendEntries { mut req, respond_to } => {
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

                let outcome = self.apply_append_entries(&mut req).await;
                let _ = respond_to.send(AppendEntriesResponse {
                    term: self.common.current_term,
                    success: outcome.success,
                    match_index: outcome.match_index,
                    conflict_index: outcome.conflict_index,
                    conflict_term: outcome.conflict_term,
                });
                // Answer first, then react: the leader waits for this response, and the
                // reload is our own bookkeeping.
                if outcome.config_may_have_changed {
                    self.reload_config().await
                } else {
                    Transition::Continue
                }
            }
            Event::InstallSnapshot {
                header,
                mut body,
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

                // `body` is read here, on the blocking pool, as chunks arrive: the sender
                // is still pumping them in while this runs. A rejection above drops it
                // unread instead, which closes the channel and stops the pump.
                self.common
                    .store
                    .expect("install snapshot", move |log| log.install_snapshot(&mut body))
                    .await;

                // A snapshot only contains committed state (Leader Completeness), so both
                // watermarks can jump straight to it, as with AppendEntries's
                // leader_commit and compact_seq hints.
                self.common.advance_commit(header.last_included_index);
                self.common.advance_compact(header.last_included_index);

                let _ = respond_to.send(InstallSnapshotResponse {
                    term: self.common.current_term,
                });
                // An install wipes `log`, including any `Config` entry; membership now
                // comes from the snapshot's header (see
                // `storage::RaftLog::install_snapshot`), so this is never optional.
                self.reload_config().await
            }
            Event::RequestVote { req, respond_to } => {
                if self.has_active_lease() {
                    // Do not disrupt a leader we recently confirmed is alive, whatever the
                    // term, log, or kind of vote (real or pre-vote). This is what stops a
                    // competing election from ever winning against a leader that a majority
                    // can still reach.
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
            Event::Bootstrap {
                config,
                restore,
                respond_to,
            } => {
                let outcome = self.bootstrap(config, restore).await;
                let accepted = outcome.is_ok();
                let _ = respond_to.send(outcome);
                if accepted {
                    // We just became a voting member of a single-node cluster, so re-enter
                    // `Follower` to get an election timer; there was none while
                    // uninitialized.
                    Transition::Become(Role::Follower)
                } else {
                    Transition::Continue
                }
            }
            Event::ElectionTimeout => {
                // A learner (or a removed server) must never campaign: it has no vote, so
                // it could never win, and a removed server that campaigns is the
                // disruption §4.2.3 warns about. It still replicates and answers RPCs; it
                // only waits.
                //
                // `Follower::new` arms no timer for such a node. This check covers the
                // window where the configuration changes while a timer is already
                // running.
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
