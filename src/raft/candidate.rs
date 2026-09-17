//! `PreCandidate` (pre-vote) and `Candidate` (real, term-incrementing
//! election): both campaign for votes from every peer, count responses,
//! and become something else on majority or on timeout -- close enough in
//! shape that they share one implementation (`Campaign`) parameterized by
//! `CampaignKind`, rather than maintaining two near-identical copies. See
//! the parent module's doc for how pre-vote fits into the bigger picture.

use super::*;

/// Requests a vote (real or pre-vote, per `req.dry_run`) from `peer`,
/// connecting (or reusing a cached connection) through `conns`, bounded by
/// `LIGHTWEIGHT_RPC_TIMEOUT`. `None` on any failure -- connect error, RPC
/// error, or timeout -- which is always safe to just ignore: the
/// campaign's own election timer is Raft's built-in retry mechanism, so
/// there's nothing else to do with a peer that didn't answer in time.
async fn request_vote_from_peer(
    peer: &PeerInfo,
    req: RequestVoteRequest,
    conns: &PeerConnections,
) -> Option<RequestVoteResponse> {
    let attempt = async {
        let mut client = conns.get_or_connect(peer).await?;
        client.request_vote(req).await.ok()
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

/// Which kind of campaign a `Campaign` is running.
#[derive(Clone, Copy, PartialEq, Eq)]
enum CampaignKind {
    /// Non-binding pre-vote: doesn't touch `current_term`/`voted_for`.
    /// Winning promotes to a real `Candidate`.
    PreVote,
    /// A real, term-incrementing candidacy. Winning promotes to `Leader`.
    RealVote,
}

impl CampaignKind {
    fn promotion(self) -> Role {
        match self {
            CampaignKind::PreVote => Role::Candidate,
            CampaignKind::RealVote => Role::Leader,
        }
    }
}

/// The event loop and state shared by `PreCandidate` and `Candidate` --
/// see the module doc. `kind` is the only thing that changes what this
/// actually does at the handful of points where the two genuinely differ:
/// whether `start` bumps the term, what a granted majority promotes to,
/// and how a `RequestVoteResponse` is matched to *this* campaign (see the
/// comment in `handle_event`).
struct Campaign {
    common: Common,
    kind: CampaignKind,
    vote_tasks: Vec<JoinHandle<()>>,
    election_timer: JoinHandle<()>,
    votes_received: usize,
}

impl Campaign {
    async fn start(mut common: Common, kind: CampaignKind) -> (Role, Common) {
        // A real candidacy commits to the new term up front (and persists
        // it, so a crash mid-election can't lead to a double vote later);
        // a pre-vote only asks a hypothetical "what if" without changing
        // anything, so it just computes what that term *would* be.
        let term = match kind {
            CampaignKind::RealVote => {
                common.current_term += 1;
                common.voted_for = Some(common.node_id.clone());
                common.persist_term_state().await;
                common.current_term
            }
            CampaignKind::PreVote => common.current_term + 1,
        };
        // Pre-vote is externally indistinguishable from a real candidacy
        // -- a client can't act on the difference either way, so there's
        // no separate `RoleHandle` variant for it.
        common.publish_role(RoleHandle::Candidate);

        let (last_log_index, last_log_term) = last_log_index_and_term(&common.log).await;
        let dry_run = kind == CampaignKind::PreVote;
        // Voting peers only: a learner has no vote to give, so asking it
        // would just be a wasted RPC that can never move `votes_received`.
        let vote_tasks = common
            .voting_peers()
            .into_iter()
            .map(|peer| {
                Self::spawn_request_vote(
                    peer,
                    term,
                    common.node_id.clone(),
                    last_log_index,
                    last_log_term,
                    dry_run,
                    Arc::clone(&common.peer_conns),
                    common.event_tx.clone(),
                )
            })
            .collect();
        // Without this, a split vote (or a cluster too small to reach
        // majority with the peers that actually answer) would leave the
        // campaign stuck forever with nothing left to wait on.
        let election_timer = spawn_election_timer(common.election_attempt, common.event_tx.clone());

        let campaign_common_is_voter = common.is_voter();
        let mut campaign = Campaign {
            common,
            kind,
            vote_tasks,
            election_timer,
            // A vote/pre-vote for ourselves -- but only if we actually
            // have one. A learner, or a server that has been removed
            // from the configuration, counts for nothing. (It shouldn't
            // be campaigning at all; see `Follower::handle_event`. This
            // is the same rule enforced where it is actually counted.)
            votes_received: usize::from(campaign_common_is_voter),
        };
        // A zero-peer cluster reaches majority on the self-vote alone --
        // nothing will ever arrive on the event loop to notice that, so
        // check up front rather than sitting in `run` forever restarting
        // every timeout (see `candidate_restarts_the_election_after_its_own_timeout`,
        // which covers the *intended* version of that behavior: peers
        // exist but aren't answering).
        let next_role = if campaign.votes_received >= campaign.common.majority() {
            kind.promotion()
        } else {
            campaign.run().await
        };
        (next_role, campaign.shutdown())
    }

    /// Calls `peer`'s RequestVote RPC and, if it answers in time, pushes
    /// the result back as `Event::RequestVoteResponse` so vote counting
    /// happens on the event loop rather than racing with it.
    #[allow(clippy::too_many_arguments)]
    fn spawn_request_vote(
        peer: PeerInfo,
        term: u64,
        candidate_id: String,
        last_log_index: u64,
        last_log_term: u64,
        dry_run: bool,
        conns: Arc<PeerConnections>,
        event_tx: mpsc::Sender<Event>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let req = RequestVoteRequest {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
                dry_run,
            };
            let Some(resp) = request_vote_from_peer(&peer, req, &conns).await else {
                return;
            };
            let _ = event_tx
                .send(Event::RequestVoteResponse {
                    from: peer.id,
                    resp,
                })
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
                // A real vote is only meaningful if it's for the exact
                // term we're running at (an equal-or-lower term response
                // could be a stale reply from an abandoned attempt). A
                // pre-vote response has no such term to match: it always
                // carries the *responder's own* current term, which for a
                // granted pre-vote is guaranteed `<= self.common.current_term`
                // (never `==` it in general -- our dry-run term is
                // `current_term + 1`, strictly ahead of anything a
                // responder would report) but isn't stale in the same
                // sense (`current_term` doesn't change across pre-vote
                // retries the way it does between real candidacies), so
                // `vote_granted` alone is the right thing to check.
                let counts = match self.kind {
                    CampaignKind::RealVote => {
                        resp.term == self.common.current_term && resp.vote_granted
                    }
                    CampaignKind::PreVote => resp.vote_granted,
                };
                if counts {
                    self.votes_received += 1;
                    if self.votes_received >= self.common.majority() {
                        return Transition::Become(self.kind.promotion());
                    }
                }
                Transition::Continue
            }
            Event::AppendEntries { req, respond_to } => {
                // A valid leader for this (or a later) term means the
                // election is over; concede. Only actually advance the
                // term (and persist) if it's strictly higher -- conceding
                // to a leader already at our own term needs no term change.
                // For a `PreVote` campaign, `current_term` is still the
                // *old*, un-bumped term, so this concedes to any leader at
                // least that current -- correctly, since a `PreCandidate`
                // hasn't actually committed to anything yet.
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
            Event::InstallSnapshot {
                header, respond_to, ..
            } => {
                // Same concede-or-reject shape as the AppendEntries arm
                // above, and for the same reason: a campaigning node
                // never applies the snapshot itself (`data` is unused
                // here) -- it just concedes if the sender's term is
                // legitimate, and the leader's own retry will resend once
                // this node is back to being a Follower able to actually
                // install it.
                if header.term >= self.common.current_term {
                    if header.term > self.common.current_term {
                        self.common.advance_term(header.term).await;
                    }
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
            Event::RequestVote { req, respond_to } => {
                if req.dry_run {
                    let grant = evaluate_dry_run_vote(&self.common, &req).await;
                    let _ = respond_to.send(RequestVoteResponse {
                        term: self.common.current_term,
                        vote_granted: grant,
                    });
                    return Transition::Continue;
                }
                // A `PreVote` campaign hasn't cast any real vote of its
                // own (unlike `RealVote`, which always has `voted_for ==
                // Some(self)` already) -- it just inherits whatever
                // `voted_for` its preceding `Follower` had, so this needs
                // the full check, not "deny, we already voted for
                // ourselves this term".
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
            Event::ElectionTimeout => {
                // Every retry -- pre-vote or real -- goes back through a
                // fresh pre-vote round rather than straight into another
                // real candidacy, so a genuinely hopeless attempt (e.g. a
                // still-partitioned node) never bumps its term just to
                // fail again; `election_attempt` backs off how long the
                // *next* attempt waits before giving up in turn.
                self.common.election_attempt = self.common.election_attempt.saturating_add(1);
                Transition::Become(Role::PreCandidate)
            }
            Event::AppendEntriesResponse { .. }
            | Event::GetLeaseResponse { .. }
            | Event::InstallSnapshotResponse { .. }
            | Event::LeaderTick => Transition::Continue,
            // No leader to forward to yet -- an election is in progress.
            Event::ProposeConfig { respond_to, .. } => {
                let _ = respond_to.send(Err(RaftError::NotLeader { leader_hint: None }));
                Transition::Continue
            }
            // Only an uninitialized node can be bootstrapped, and an
            // uninitialized node never campaigns.
            Event::Bootstrap { respond_to, .. } => {
                let _ = respond_to.send(Err(RaftError::AlreadyInitialized));
                Transition::Continue
            }
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

/// Non-binding pre-vote round. See the parent module's doc.
pub struct PreCandidate;

impl PreCandidate {
    // See the comment on `Leader::new` re: this not returning `Self`.
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(common: Common) -> (Role, Common) {
        Campaign::start(common, CampaignKind::PreVote).await
    }
}

/// A real, term-incrementing candidacy -- only ever entered after
/// `PreCandidate` has already confirmed a majority would plausibly grant
/// it a real vote.
pub struct Candidate;

impl Candidate {
    // See the comment on `Leader::new` re: this not returning `Self`.
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(common: Common) -> (Role, Common) {
        Campaign::start(common, CampaignKind::RealVote).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::test_support::*;

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
        // `leader::tests::leader_commits_a_write_immediately_in_a_single_node_cluster`).
        // With an unreachable peer, pre-vote itself never reaches
        // majority either, so this never even becomes a *real* candidate
        // -- it just keeps re-publishing `RoleHandle::Candidate` (which
        // covers both pre-vote and real candidacies) from repeated
        // `PreCandidate` attempts.
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

    /// Backoff should make later retries (still failing, same unreachable
    /// peer) wait noticeably longer than earlier ones, rather than
    /// hammering away at the same 150-300ms interval forever.
    #[tokio::test]
    async fn repeated_election_failures_back_off_the_retry_interval() {
        async fn next_change(role_rx: &mut watch::Receiver<RoleHandle>) -> Duration {
            let start = std::time::Instant::now();
            // Generously above attempt 2's ~1200ms ceiling.
            tokio::time::timeout(Duration::from_millis(2500), role_rx.changed())
                .await
                .expect("expected another role change before the timeout")
                .unwrap();
            start.elapsed()
        }

        let peers = vec![PeerInfo {
            id: "ghost".to_string(),
            address: "http://127.0.0.1:1".to_string(),
        }];
        let (mut role_rx, _peer_handle, path) = spawn_test_node(peers);

        // Follower's own timeout (attempt 0's range) firing into the
        // first `PreCandidate` attempt -- just where we start counting
        // from, not one of the two gaps compared below.
        wait_for_role(&mut role_rx, |r| matches!(r, RoleHandle::Candidate)).await;

        // Gap A: the first `PreCandidate` campaign's own timeout (attempt
        // 0: 150-300ms) firing and republishing on retry.
        let gap_a = next_change(&mut role_rx).await;
        // Skip one retry (attempt 1: 300-600ms) -- comparing against
        // attempt 2 instead of attempt 1 gives a much wider margin
        // (150-300ms vs. 600-1200ms, rather than adjacent ranges sharing
        // a 300ms boundary), so this assertion isn't sensitive to a bit
        // of scheduling noise under parallel test load.
        next_change(&mut role_rx).await;
        // Gap B: the third campaign's own timeout (attempt 2: 600-1200ms).
        let gap_b = next_change(&mut role_rx).await;

        assert!(
            gap_b > gap_a,
            "expected a later, backed-off retry ({gap_b:?}) to take longer \
             than an earlier one ({gap_a:?})"
        );
        cleanup(&path);
    }
}
