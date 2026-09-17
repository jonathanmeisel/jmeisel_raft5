//! The follower role: answers AppendEntries (log-matching, §5.3 fast
//! backup) and RequestVote (§5.4.1 up-to-date check), and starts an
//! election if it hasn't heard from a leader in time. See the parent
//! module's doc for how roles fit together generally.

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
    mut lease_rx: watch::Receiver<Option<Instant>>,
    event_tx: mpsc::Sender<Event>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let started_at = Instant::now();
        loop {
            let deadline = (*lease_rx.borrow()).unwrap_or(started_at + LEASE_DURATION);
            let now = Instant::now();
            if now < deadline {
                tokio::select! {
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
            let _ = event_tx.send(Event::ElectionTimeout).await;
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
    election_timer: JoinHandle<()>,
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

        // A learner -- or a server that has been removed from the
        // configuration -- never campaigns, so it gets no election timer
        // at all rather than one whose every firing has to be discarded.
        // Being promoted to voter re-enters `Follower`, which is what
        // arms one.
        let election_timer = if common.is_voter() {
            spawn_lease_expiration_watcher(
                common.lease_expires_at_rx.clone(),
                common.event_tx.clone(),
            )
        } else {
            tokio::spawn(std::future::pending())
        };

        let mut follower = Follower {
            common,
            election_timer,
            leader_hint: None,
        };
        let next_role = follower.run().await;
        (next_role, follower.shutdown())
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
        let log = Arc::clone(&self.common.log);
        let bytes = config.to_bytes();
        let appended = tokio::task::spawn_blocking(move || {
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
        .expect("bootstrap append task panicked")
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
        let _ = self
            .common
            .lease_expires_at_tx
            .send(Some(Instant::now() + LEASE_DURATION));
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

                let log = Arc::clone(&self.common.log);
                tokio::task::spawn_blocking(move || log.install_snapshot(&mut data.as_slice()))
                    .await
                    .expect("install_snapshot task panicked")
                    .expect("failed to install snapshot");

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

    fn shutdown(self) -> Common {
        self.election_timer.abort();
        self.common
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::test_support::*;

    /// Retries `req` until it's granted or this node has moved on to
    /// campaigning itself, whichever comes first -- both outcomes prove
    /// a lease-based denial isn't permanent. The two are deliberately
    /// racy against each other here: the same deadline that ends the
    /// lease also starts this node's own candidacy timer (see the module
    /// doc on why the two were merged), so on an isolated node (no real
    /// peer refreshing anything) there's no way to guarantee which one
    /// wins on any given run. Returns the granted response, or `None` if
    /// it moved on to campaigning instead.
    async fn wait_for_vote_grant_or_role_change(
        peer_handle: &RaftPeerHandle,
        role_rx: &mut watch::Receiver<RoleHandle>,
        req: &RequestVoteRequest,
    ) -> Option<RequestVoteResponse> {
        let deadline = std::time::Instant::now() + LEASE_DURATION * 10;
        loop {
            if !matches!(&*role_rx.borrow(), RoleHandle::Follower(_)) {
                return None;
            }
            let resp = peer_handle.request_vote(req.clone()).await.unwrap();
            if resp.vote_granted {
                return Some(resp);
            }
            assert!(
                std::time::Instant::now() < deadline,
                "neither granted the vote nor moved on to campaigning within the deadline"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
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
        let (_log, _role_rx, peer_handle, _commit_rx, _compact_rx, path) =
            spawn_node_with_handles(vec![]);

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
        let (log, _role_rx, peer_handle, _commit_rx, _compact_rx, path) =
            spawn_node_with_handles(vec![]);

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
        // `leader_commit` is 0 here, so nothing has committed yet --
        // `get_latest` wouldn't reflect these entries even once applied
        // (see storage's module doc), so check `log` directly instead.
        let stored = log.entries_in_range(1, 3).unwrap();
        assert_eq!(stored.len(), 2);
        assert_eq!(stored[0].key, b"a");
        assert_eq!(stored[0].value, Some(b"1".to_vec()));
        assert_eq!(stored[1].key, b"b");
        assert_eq!(stored[1].value, Some(b"2".to_vec()));
        cleanup(&path);
    }

    /// A follower re-appends replicated entries into its own log, so the
    /// entry's kind has to survive the round trip -- inferring it from
    /// `value` would turn the leader's term-opening no-op into a delete
    /// tombstone for the empty key, and the two logs would stop matching.
    #[tokio::test]
    async fn append_entries_preserves_a_replicated_noop_rather_than_flattening_it() {
        let (log, _role_rx, peer_handle, _commit_rx, _compact_rx, path) =
            spawn_node_with_handles(vec![]);

        let noop = LogEntry {
            seq: 1,
            term: 1,
            kind: crate::pb::raft5::common::EntryKind::Noop as i32,
            key: Vec::new(),
            value: None,
        };
        let resp = peer_handle
            .append_entries(append_entries(
                1,
                "leader-1",
                0,
                0,
                vec![noop, log_entry(2, 1, b"a", Some(b"1"))],
                0,
            ))
            .await
            .unwrap();

        assert!(resp.success);
        assert_eq!(resp.match_index, 2);

        let stored = log.entries_in_range(1, 3).unwrap();
        assert_eq!(stored[0].kind, storage::EntryKind::NoOp);
        assert_eq!(stored[1].kind, storage::EntryKind::Put);
        cleanup(&path);
    }

    /// A configuration takes effect the moment it is *appended* (Raft
    /// §4.1), not when it commits -- so a replicated `Config` entry has
    /// to change the follower's membership immediately, with
    /// `leader_commit` still at 0.
    #[tokio::test]
    async fn a_replicated_config_entry_takes_effect_on_append_not_on_commit() {
        let before = cluster_config(&[("test-node", true), ("v1", true)]);
        let (log, (common, mut role_rx, peer_handle, commit_rx, _cc), path) =
            open_temp_log_with_config("test-node", &before);
        tokio::spawn(run(common));
        wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Follower(_))).await;

        let after = cluster_config(&[("test-node", true), ("v1", true), ("v2", true)]);
        let entry = LogEntry {
            seq: 2,
            term: 1,
            kind: crate::pb::raft5::common::EntryKind::Config as i32,
            key: Vec::new(),
            value: Some(after.to_bytes()),
        };
        let resp = peer_handle
            .append_entries(append_entries(1, "v1", 1, 1, vec![entry], 0))
            .await
            .unwrap();
        assert!(resp.success);
        assert_eq!(*commit_rx.borrow(), 0, "nothing has committed");

        // Visible from storage straight away, uncommitted and unapplied.
        assert_eq!(
            ClusterConfig::from_bytes(&log.current_config().unwrap().unwrap()).unwrap(),
            after
        );
        cleanup(&path);
    }

    /// The flip side of activating on append: if the entry carrying a
    /// configuration is truncated away by a conflicting leader, the
    /// previous configuration is in force again.
    #[tokio::test]
    async fn a_truncated_config_entry_reverts_the_active_configuration() {
        let before = cluster_config(&[("test-node", true), ("v1", true)]);
        let (log, (common, mut role_rx, peer_handle, _c, _cc), path) =
            open_temp_log_with_config("test-node", &before);
        tokio::spawn(run(common));
        wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Follower(_))).await;

        let after = cluster_config(&[("test-node", true), ("v1", true), ("v2", true)]);
        let config_entry = LogEntry {
            seq: 2,
            term: 1,
            kind: crate::pb::raft5::common::EntryKind::Config as i32,
            key: Vec::new(),
            value: Some(after.to_bytes()),
        };
        peer_handle
            .append_entries(append_entries(1, "v1", 1, 1, vec![config_entry], 0))
            .await
            .unwrap();

        // A leader from a later term overwrites seq 2 with something
        // else, discarding the configuration that lived there.
        let resp = peer_handle
            .append_entries(append_entries(
                2,
                "v2",
                1,
                1,
                vec![log_entry(2, 2, b"a", Some(b"1"))],
                0,
            ))
            .await
            .unwrap();
        assert!(resp.success);
        assert_eq!(
            ClusterConfig::from_bytes(&log.current_config().unwrap().unwrap()).unwrap(),
            before
        );
        cleanup(&path);
    }

    /// Promotion has to rebuild the election timer: `Follower::new` arms
    /// one only for a voter, so a learner that is promoted while sitting
    /// in `Follower` would otherwise never campaign again.
    #[tokio::test]
    async fn a_learner_promoted_by_a_replicated_config_starts_campaigning() {
        let before = cluster_config(&[("test-node", false), ("v1", true), ("v2", true)]);
        let (_log, (common, mut role_rx, peer_handle, _c, _cc), path) =
            open_temp_log_with_config("test-node", &before);
        tokio::spawn(run(common));
        wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Follower(_))).await;

        let after = cluster_config(&[("test-node", true), ("v1", true), ("v2", true)]);
        let entry = LogEntry {
            seq: 2,
            term: 1,
            kind: crate::pb::raft5::common::EntryKind::Config as i32,
            key: Vec::new(),
            value: Some(after.to_bytes()),
        };
        peer_handle
            .append_entries(append_entries(1, "v1", 1, 1, vec![entry], 0))
            .await
            .unwrap();

        // Its peers are unreachable, so it can't win -- but it must at
        // least start trying, which it never did as a learner.
        wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Candidate)).await;
        cleanup(&path);
    }

    #[tokio::test]
    async fn append_entries_rejects_and_hints_next_index_when_log_is_too_short() {
        let (_log, _role_rx, peer_handle, _commit_rx, _compact_rx, path) =
            spawn_node_with_handles(vec![]);

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
        let (_log, _role_rx, peer_handle, _commit_rx, _compact_rx, path) =
            spawn_node_with_handles(vec![]);

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
        let (log, _role_rx, peer_handle, _commit_rx, _compact_rx, path) =
            spawn_node_with_handles(vec![]);

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
        // `leader_commit` is 0 here, so `get_latest` wouldn't reflect any
        // of this regardless -- check `log` directly: the stale entry at
        // seq 2 is gone, replaced by the new leader's entry at the same
        // seq (a keyspace can only hold one value per exact seq, so this
        // alone proves the old one didn't survive).
        let stored = log.entries_in_range(2, 3).unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].key, b"fresh");
        assert_eq!(stored[0].value, Some(b"new".to_vec()));
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
        let (mut role_rx, peer_handle, path) = spawn_test_node(vec![]);

        peer_handle
            .request_vote(request_vote(3, "candidate-a"))
            .await
            .unwrap();

        // Granting that vote just renewed this follower's lease (voting
        // for a prospective leader defers our own campaign, and the two
        // deadlines are deliberately merged into one -- see the module
        // doc), so a second, different candidate can't get a vote until
        // that lease naturally lapses, regardless of term. Once it does,
        // this node's own candidacy timer -- the very same deadline --
        // may also fire first; see `wait_for_vote_grant_or_role_change`.
        let resp = wait_for_vote_grant_or_role_change(
            &peer_handle,
            &mut role_rx,
            &request_vote(4, "candidate-b"),
        )
        .await;

        if let Some(resp) = resp {
            assert!(resp.vote_granted);
            assert_eq!(resp.term, 4);
        }
        cleanup(&path);
    }

    #[tokio::test]
    async fn request_vote_denies_a_candidate_whose_log_ends_at_an_older_term() {
        let (_log, _role_rx, peer_handle, _commit_rx, _compact_rx, path) =
            spawn_node_with_handles(vec![]);

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
                dry_run: false,
            })
            .await
            .unwrap();

        assert!(!resp.vote_granted);
        cleanup(&path);
    }

    #[tokio::test]
    async fn request_vote_grants_a_candidate_whose_log_is_at_least_as_up_to_date() {
        let (_log, mut role_rx, peer_handle, _commit_rx, _compact_rx, path) =
            spawn_node_with_handles(vec![]);

        let entries = vec![log_entry(1, 2, b"a", Some(b"1"))];
        peer_handle
            .append_entries(append_entries(2, "leader-1", 0, 0, entries, 0))
            .await
            .unwrap();

        // The AppendEntries above just renewed this follower's lease on
        // leader-1, so it'll refuse any vote request until that lease
        // naturally expires -- wait it out (or for this node's own
        // candidacy timer, the same deadline, to fire first; see
        // `wait_for_vote_grant_or_role_change`) before expecting a grant.
        //
        // Same term as our last entry (2) and the same index (1) --
        // "at least as up to date" per §5.4.1, so this should be granted.
        let req = RequestVoteRequest {
            term: 3,
            candidate_id: "candidate-a".to_string(),
            last_log_index: 1,
            last_log_term: 2,
            dry_run: false,
        };
        let resp = wait_for_vote_grant_or_role_change(&peer_handle, &mut role_rx, &req).await;

        if let Some(resp) = resp {
            assert!(resp.vote_granted);
        }
        cleanup(&path);
    }

    #[tokio::test]
    async fn granting_a_vote_persists_it_to_storage() {
        // Complements storage.rs's own round-trip test: this one checks
        // that `Follower::handle_event` actually calls through to
        // persistence, not just that the storage layer works in isolation.
        let (log, path) = open_temp_log();
        let (common, _role_rx, peer_handle, _commit_rx, _compact_rx) =
            Common::new_seeded("test-node".to_string(), vec![], Arc::clone(&log)).unwrap();
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
    async fn dry_run_vote_grants_without_mutating_any_state() {
        let (log, _role_rx, peer_handle, _commit_rx, _compact_rx, path) =
            spawn_node_with_handles(vec![]);

        let resp = peer_handle
            .request_vote(RequestVoteRequest {
                term: 5,
                candidate_id: "candidate-a".to_string(),
                last_log_index: 0,
                last_log_term: 0,
                dry_run: true,
            })
            .await
            .unwrap();

        assert!(resp.vote_granted);
        // A dry run must not actually record anything -- current_term and
        // voted_for stay exactly as they were before this call.
        assert_eq!(log.term_state().unwrap(), (0, None));
        cleanup(&path);
    }

    #[tokio::test]
    async fn dry_run_vote_denies_when_term_is_not_higher() {
        let (_log, _role_rx, peer_handle, _commit_rx, _compact_rx, path) =
            spawn_node_with_handles(vec![]);

        peer_handle
            .append_entries(blank_append_entries(5, "leader-1"))
            .await
            .unwrap();

        // A dry run at the SAME term (not strictly higher) must be denied
        // -- granting would mean "yes, you could win term 5", but this
        // follower already recognizes term 5 as current (and possibly
        // already voted in it for real).
        let resp = peer_handle
            .request_vote(RequestVoteRequest {
                term: 5,
                candidate_id: "candidate-a".to_string(),
                last_log_index: 0,
                last_log_term: 0,
                dry_run: true,
            })
            .await
            .unwrap();

        assert!(!resp.vote_granted);
        cleanup(&path);
    }

    #[tokio::test]
    async fn dry_run_vote_denies_when_candidates_log_is_behind() {
        let (_log, _role_rx, peer_handle, _commit_rx, _compact_rx, path) =
            spawn_node_with_handles(vec![]);

        let entries = vec![log_entry(1, 2, b"a", Some(b"1"))];
        peer_handle
            .append_entries(append_entries(2, "leader-1", 0, 0, entries, 0))
            .await
            .unwrap();

        // Term is high enough (3 > 2), but the candidate's log ends at an
        // older term (1) than ours (2).
        let resp = peer_handle
            .request_vote(RequestVoteRequest {
                term: 3,
                candidate_id: "candidate-a".to_string(),
                last_log_index: 5,
                last_log_term: 1,
                dry_run: true,
            })
            .await
            .unwrap();

        assert!(!resp.vote_granted);
        cleanup(&path);
    }

    #[tokio::test]
    async fn a_granted_dry_run_does_not_block_a_later_real_vote_for_someone_else() {
        let (_log, _role_rx, peer_handle, _commit_rx, _compact_rx, path) =
            spawn_node_with_handles(vec![]);

        // Since a dry run never records anything, a later REAL request at
        // the same term from a *different* candidate must still be
        // evaluated fresh, not implicitly denied because of who the dry
        // run was "for".
        let dry = peer_handle
            .request_vote(RequestVoteRequest {
                term: 5,
                candidate_id: "candidate-a".to_string(),
                last_log_index: 0,
                last_log_term: 0,
                dry_run: true,
            })
            .await
            .unwrap();
        assert!(dry.vote_granted);

        let real = peer_handle
            .request_vote(RequestVoteRequest {
                term: 5,
                candidate_id: "candidate-b".to_string(),
                last_log_index: 0,
                last_log_term: 0,
                dry_run: false,
            })
            .await
            .unwrap();
        assert!(real.vote_granted);
        cleanup(&path);
    }

    #[tokio::test]
    async fn get_lease_grants_when_term_matches_and_denies_otherwise() {
        let (mut role_rx, peer_handle, path) = spawn_test_node(vec![]);
        // `get_lease` bypasses the event loop entirely (that's the whole
        // point), so unlike append_entries/request_vote it doesn't
        // naturally wait for the driver to actually start -- confirm the
        // follower role is really published first.
        wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Follower(_))).await;

        // Follower starts at term 0 -- a lease request for term 0 matches.
        let resp = peer_handle.get_lease(GetLeaseRequest {
            leader_id: "leader-1".to_string(),
            term: 0,
        });
        assert!(resp.lease_granted);

        // A term that doesn't match the follower's current term is denied.
        let resp = peer_handle.get_lease(GetLeaseRequest {
            leader_id: "leader-1".to_string(),
            term: 5,
        });
        assert!(!resp.lease_granted);

        cleanup(&path);
    }

    #[tokio::test]
    async fn a_granted_lease_blocks_a_vote_request_for_someone_else() {
        let (mut role_rx, peer_handle, path) = spawn_test_node(vec![]);
        wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Follower(_))).await;

        let resp = peer_handle.get_lease(GetLeaseRequest {
            leader_id: "leader-1".to_string(),
            term: 0,
        });
        assert!(resp.lease_granted);

        // A vote request arriving while the lease is still active must be
        // refused outright -- even a real request with a higher term and
        // a perfectly fine log.
        let resp = peer_handle
            .request_vote(RequestVoteRequest {
                term: 99,
                candidate_id: "candidate-a".to_string(),
                last_log_index: 0,
                last_log_term: 0,
                dry_run: false,
            })
            .await
            .unwrap();
        assert!(!resp.vote_granted);

        // Same for a pre-vote.
        let resp = peer_handle
            .request_vote(RequestVoteRequest {
                term: 99,
                candidate_id: "candidate-a".to_string(),
                last_log_index: 0,
                last_log_term: 0,
                dry_run: true,
            })
            .await
            .unwrap();
        assert!(!resp.vote_granted);

        cleanup(&path);
    }

    #[tokio::test]
    async fn a_vote_request_is_granted_normally_once_the_lease_expires() {
        let (mut role_rx, peer_handle, path) = spawn_test_node(vec![]);
        wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Follower(_))).await;

        peer_handle.get_lease(GetLeaseRequest {
            leader_id: "leader-1".to_string(),
            term: 0,
        });

        // Wait for the lease to naturally expire -- or, since it's the
        // same deadline, for this node's own candidacy timer to fire
        // first; see `wait_for_vote_grant_or_role_change`.
        let req = RequestVoteRequest {
            term: 1,
            candidate_id: "candidate-a".to_string(),
            last_log_index: 0,
            last_log_term: 0,
            dry_run: false,
        };
        let resp = wait_for_vote_grant_or_role_change(&peer_handle, &mut role_rx, &req).await;

        if let Some(resp) = resp {
            assert!(resp.vote_granted);
        }
        cleanup(&path);
    }

    /// A follower must not transition to candidate before its lease
    /// expires -- repeated `GetLease` grants (standing in for the
    /// leader's periodic probes) should keep deferring the election
    /// timer indefinitely, well past when it would otherwise have fired.
    #[tokio::test]
    async fn periodic_lease_grants_prevent_becoming_a_candidate() {
        let (mut role_rx, peer_handle, path) = spawn_test_node(vec![]);
        wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Follower(_))).await;

        let deadline = std::time::Instant::now() + LEASE_DURATION * 3;
        while std::time::Instant::now() < deadline {
            let resp = peer_handle.get_lease(GetLeaseRequest {
                leader_id: "leader-1".to_string(),
                term: 0,
            });
            assert!(resp.lease_granted);
            tokio::time::sleep(HEARTBEAT).await;
        }

        assert!(matches!(&*role_rx.borrow(), RoleHandle::Follower(_)));
        cleanup(&path);
    }

    #[tokio::test]
    async fn install_snapshot_applies_state_and_advances_watermarks() {
        let (source_log, source_path) = open_temp_log();
        source_log.append(3, b"a", Some(b"1")).unwrap();
        let s2 = source_log.append(3, b"b", Some(b"2")).unwrap();
        // `export_snapshot` only ever reflects committed state -- apply
        // first so there's actually something for it to export.
        source_log.apply_committed(s2).unwrap();
        let mut buf = Vec::new();
        let meta = source_log.export_snapshot(&mut buf).unwrap();

        let (log, mut role_rx, peer_handle, mut commit_rx, mut compact_rx, path) =
            spawn_node_with_handles(vec![]);
        wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Follower(_))).await;

        let resp = peer_handle
            .install_snapshot(
                SnapshotHeader {
                    term: 5,
                    leader_id: "leader-1".to_string(),
                    last_included_index: meta.last_included_seq,
                    last_included_term: meta.last_included_term,
                },
                buf,
            )
            .await
            .unwrap();

        assert_eq!(resp.term, 5);
        // `install_snapshot` repopulates `latest` at `last_included_seq`
        // for every key, regardless of the seq each was originally
        // written at in the source log.
        assert_eq!(
            log.get_latest(b"a").unwrap(),
            Some((meta.last_included_seq, b"1".to_vec()))
        );
        assert_eq!(
            log.get_latest(b"b").unwrap(),
            Some((meta.last_included_seq, b"2".to_vec()))
        );

        commit_rx.changed().await.unwrap();
        assert_eq!(*commit_rx.borrow(), meta.last_included_seq);
        compact_rx.changed().await.unwrap();
        assert_eq!(*compact_rx.borrow(), meta.last_included_seq);

        cleanup(&path);
        cleanup(&source_path);
    }

    #[tokio::test]
    async fn install_snapshot_rejects_a_stale_term_and_does_not_touch_storage() {
        let (log, mut role_rx, peer_handle, _commit_rx, _compact_rx, path) =
            spawn_node_with_handles(vec![]);
        wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Follower(_))).await;

        peer_handle
            .append_entries(blank_append_entries(5, "leader-1"))
            .await
            .unwrap();

        let resp = peer_handle
            .install_snapshot(
                SnapshotHeader {
                    term: 2,
                    leader_id: "leader-2".to_string(),
                    last_included_index: 10,
                    last_included_term: 1,
                },
                vec![],
            )
            .await
            .unwrap();

        assert_eq!(resp.term, 5);
        assert_eq!(log.last_entry().unwrap(), None);
        cleanup(&path);
    }
}
