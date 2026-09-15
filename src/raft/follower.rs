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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::test_support::*;

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
            })
            .await
            .unwrap();

        assert!(!resp.vote_granted);
        cleanup(&path);
    }

    #[tokio::test]
    async fn request_vote_grants_a_candidate_whose_log_is_at_least_as_up_to_date() {
        let (_log, _role_rx, peer_handle, _commit_rx, _compact_rx, path) =
            spawn_node_with_handles(vec![]);

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
}
