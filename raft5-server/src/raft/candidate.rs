//! `PreCandidate` (pre-vote) and `Candidate` (a real election that raises
//! the term). Both ask every peer for votes, count responses, and change
//! role on a majority or a timeout. They are close enough in shape to share
//! one implementation (`Campaign`), parameterized by `CampaignKind`, instead
//! of two near-identical copies. See the parent module's doc for how
//! pre-vote fits in.

use tracing::info;

use super::*;

/// Request a vote (real or pre-vote, as `req.dry_run` says) from `peer`,
/// through `conns`' shared client, within `timeout`
/// (`Timing::lightweight_rpc_timeout`). `None` on any failure (connect
/// error, RPC error, timeout). Always safe to ignore: the campaign's own
/// election timer is Raft's retry mechanism, so there is nothing else to do
/// with a peer that did not answer in time.
async fn request_vote_from_peer(
    peer: &PeerInfo,
    req: RequestVoteRequest,
    conns: &PeerConnections,
    timeout: Duration,
) -> Option<RequestVoteResponse> {
    let mut client = conns.client(peer)?;
    let attempt = client.request_vote(req);
    match tokio::time::timeout(timeout, attempt).await {
        Ok(Ok(resp)) => Some(resp.into_inner()),
        _ => None,
    }
}

/// Which kind of campaign a `Campaign` runs.
#[derive(Clone, Copy, PartialEq, Eq)]
enum CampaignKind {
    /// Non-binding pre-vote: does not touch `current_term` or `voted_for`.
    /// Winning leads to a real `Candidate`.
    PreVote,
    /// A real candidacy that raises the term. Winning leads to `Leader`.
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

/// The event loop and state shared by `PreCandidate` and `Candidate` (see
/// the module doc). `kind` changes behavior only where the two really
/// differ:
/// - whether `start` raises the term,
/// - what a majority leads to,
/// - how a `RequestVoteResponse` is matched to this campaign (see the
///   comment in `handle_event`).
struct Campaign {
    common: Common,
    kind: CampaignKind,
    vote_tasks: Vec<Task>,
    election_timer: Task,
    votes_received: usize,
}

impl Campaign {
    async fn start(mut common: Common, kind: CampaignKind) -> (Role, Common) {
        // A real candidacy commits to the new term up front, and persists it, so
        // a crash mid-election cannot lead to a double vote later. A pre-vote
        // only asks "what if" without changing anything, so it only computes what
        // the term would be.
        let term = match kind {
            CampaignKind::RealVote => {
                common.current_term += 1;
                common.voted_for = Some(common.node_id.clone());
                common.persist_term_state().await;
                common.current_term
            }
            CampaignKind::PreVote => common.current_term + 1,
        };
        // From outside, pre-vote looks the same as a real candidacy (a client
        // cannot act on the difference), so it has no separate `RoleHandle`
        // variant.
        common.publish_role(RoleHandle::Candidate);

        let (last_log_index, last_log_term) = last_log_index_and_term(&common.store).await;
        let dry_run = kind == CampaignKind::PreVote;
        // Voting peers only: a learner has no vote, so asking it would be a
        // wasted RPC that can never change `votes_received`.
        let vote_tasks = common
            .voting_peers()
            .into_iter()
            .map(|peer| {
                Self::spawn_request_vote(
                    common.cancel_token(),
                    peer,
                    term,
                    common.node_id.clone(),
                    last_log_index,
                    last_log_term,
                    dry_run,
                    Arc::clone(&common.peer_conns),
                    common.timing().lightweight_rpc_timeout(),
                    common.event_tx.clone(),
                )
            })
            .collect();
        // Without this, a split vote (or a cluster too small to reach a majority
        // with the peers that answer) would leave the campaign stuck forever with
        // nothing to wait for.
        let election_timer = spawn_election_timer(
            common.cancel_token(),
            common.timing(),
            common.election_attempt,
            common.event_tx.clone(),
        );

        let campaign_common_is_voter = common.is_voter();
        let mut campaign = Campaign {
            common,
            kind,
            vote_tasks,
            election_timer,
            // A vote (or pre-vote) for ourselves, but only if we have one. A learner,
            // or a server removed from the configuration, counts for nothing. (It
            // should not campaign at all; see `Follower::handle_event`. This applies
            // the same rule where votes are counted.)
            votes_received: usize::from(campaign_common_is_voter),
        };
        // A cluster with no peers reaches a majority on the self-vote alone, and
        // nothing will arrive on the event loop to notice. So check now, instead of
        // waiting in `run` forever and restarting on every timeout (see
        // `candidate_restarts_the_election_after_its_own_timeout`, which covers the
        // intended version of that: peers exist but do not answer).
        let next_role = if campaign.votes_received >= campaign.common.majority() {
            kind.promotion()
        } else {
            campaign.run().await
        };
        (next_role, campaign.shutdown().await)
    }

    /// Call `peer`'s RequestVote RPC and, if it answers in time, push the
    /// result back as `Event::RequestVoteResponse`, so votes are counted on the
    /// event loop, not in a race with it.
    #[allow(clippy::too_many_arguments)]
    fn spawn_request_vote(
        cancel: &CancellationToken,
        peer: PeerInfo,
        term: u64,
        candidate_id: String,
        last_log_index: u64,
        last_log_term: u64,
        dry_run: bool,
        conns: Arc<PeerConnections>,
        timeout: Duration,
        event_tx: mpsc::Sender<Event>,
    ) -> Task {
        Task::spawn(cancel, move |cancel| async move {
            let req = RequestVoteRequest {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
                dry_run,
            };
            // Wrapped, not just awaited: `request_vote_from_peer` is bounded by
            // `timeout`, but an abandoned campaign should not have to wait even that
            // long for this task to end.
            let resp =
                until_cancelled(&cancel, request_vote_from_peer(&peer, req, &conns, timeout)).await;
            let Some(Some(resp)) = resp else {
                return;
            };
            send_event(
                &cancel,
                &event_tx,
                Event::RequestVoteResponse {
                    from: peer.id,
                    resp,
                },
            )
            .await;
        })
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
            Event::RequestVoteResponse { resp, .. } => {
                // A response from a newer term never gets here (see
                // `Common::next_event`).
                //
                // A real vote counts only for the exact term we run at: a response for an
                // equal or lower term could be a stale reply from an abandoned attempt.
                // A pre-vote response has no such term to match. It always carries the
                // responder's own current term, which for a granted pre-vote is at most
                // `self.common.current_term` (our dry-run term is `current_term + 1`, ahead
                // of anything a responder reports). That is not stale in the same sense,
                // because `current_term` does not change between pre-vote retries as it
                // does between real candidacies. So `vote_granted` alone is the right
                // check.
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
                // A valid leader for this (or a later) term means the election is over:
                // give in. Advance (and persist) the term only if it is strictly higher;
                // giving in to a leader at our own term needs no change. In a `PreVote`
                // campaign, `current_term` is still the old term, so this gives in to any
                // leader at least that current, which is correct: a `PreCandidate` has not
                // committed to anything yet.
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
                // Same give-in-or-reject shape as the AppendEntries arm above, for the
                // same reason: a campaigning node never applies the snapshot itself
                // (`data` is unused here). It gives in if the sender's term is valid, and
                // the leader's retry resends the snapshot once this node is a Follower
                // that can install it.
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
                // A `PreVote` campaign has cast no real vote of its own (unlike `RealVote`,
                // which always has `voted_for == Some(self)`). It keeps whatever
                // `voted_for` its `Follower` had, so it needs the full check, not "deny,
                // we already voted for ourselves this term".
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
                // Every retry, pre-vote or real, goes back through a new pre-vote round,
                // not straight into another real candidacy. So a hopeless attempt (for
                // example, a node still partitioned) never raises its term only to fail
                // again. `election_attempt` makes the next attempt wait longer.
                self.common.election_attempt = self.common.election_attempt.saturating_add(1);
                Transition::Become(Role::PreCandidate)
            }
            Event::AppendEntriesResponse { .. }
            | Event::GetLeaseResponse { .. }
            | Event::InstallSnapshotResponse { .. }
            | Event::LeaderTick => Transition::Continue,
            // No leader to forward to yet: an election is in progress.
            Event::ProposeConfig { respond_to, .. } => {
                let _ = respond_to.send(Err(RaftError::NotLeader { leader_hint: None }));
                Transition::Continue
            }
            // Only an uninitialized node can be bootstrapped, and an uninitialized
            // node never campaigns.
            Event::Bootstrap { respond_to, .. } => {
                let _ = respond_to.send(Err(RaftError::AlreadyInitialized));
                Transition::Continue
            }
        }
    }

    /// Stop this campaign's tasks and return `Common`. All are signalled first
    /// and only then joined, so the open vote requests wind down in parallel,
    /// not one RPC timeout at a time.
    async fn shutdown(self) -> Common {
        Task::stop_all(self.vote_tasks).await;
        self.election_timer.stop().await;
        self.common
    }
}

/// A non-binding pre-vote round. See the parent module's doc.
pub struct PreCandidate;

impl PreCandidate {
    // See the comment on `Leader::new` about this not returning `Self`.
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(common: Common) -> (Role, Common) {
        Campaign::start(common, CampaignKind::PreVote).await
    }
}

/// A real candidacy that raises the term. Entered only after
/// `PreCandidate` has confirmed that a majority would likely grant a real
/// vote.
pub struct Candidate;

impl Candidate {
    // See the comment on `Leader::new` about this not returning `Self`.
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(common: Common) -> (Role, Common) {
        Campaign::start(common, CampaignKind::RealVote).await
    }
}

#[cfg(test)]
mod tests;
