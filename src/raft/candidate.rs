//! The candidate role: campaigns for votes from every peer and either wins
//! (becomes leader), loses/times out (restarts the election), or concedes
//! to a legitimate leader/higher term (steps down to follower). See the
//! parent module's doc for how roles fit together generally.

use super::*;

/// Requests a vote from `peer`, connecting (or reusing a cached
/// connection) through `conns`, bounded by `PEER_RPC_TIMEOUT`. `None` on
/// any failure -- connect error, RPC error, or timeout -- which is always
/// safe to just ignore: the candidate's own election timer is Raft's
/// built-in retry mechanism, so there's nothing else to do with a peer
/// that didn't answer in time.
async fn request_vote_from_peer(
    peer: &PeerInfo,
    req: RequestVoteRequest,
    conns: &PeerConnections,
) -> Option<RequestVoteResponse> {
    let attempt = async {
        let mut client = conns.get_or_connect(peer).await?;
        client.request_vote(req).await.ok()
    };
    let resp = match tokio::time::timeout(PEER_RPC_TIMEOUT, attempt).await {
        Ok(Some(resp)) => Some(resp.into_inner()),
        _ => None,
    };
    if resp.is_none() {
        conns.evict(&peer.id).await;
    }
    resp
}

pub struct Candidate {
    common: Common,
    vote_tasks: Vec<JoinHandle<()>>,
    election_timer: JoinHandle<()>,
    votes_received: usize,
}

impl Candidate {
    // See the comment on `Leader::new` re: this not returning `Self`.
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(mut common: Common) -> (Role, Common) {
        common.current_term += 1;
        common.voted_for = Some(common.node_id.clone());
        common.persist_term_state().await;
        common.publish_role(RoleHandle::Candidate);

        let (last_log_index, last_log_term) = last_log_index_and_term(&common.log).await;
        let vote_tasks = common
            .peers
            .iter()
            .cloned()
            .map(|peer| {
                Self::spawn_request_vote(
                    peer,
                    common.current_term,
                    common.node_id.clone(),
                    last_log_index,
                    last_log_term,
                    Arc::clone(&common.peer_conns),
                    common.event_tx.clone(),
                )
            })
            .collect();
        // Without this, a split vote (or a cluster too small to reach
        // majority with the peers that actually answer) would leave the
        // candidate stuck forever with nothing left to wait on.
        let election_timer = spawn_election_timer(common.event_tx.clone());

        let mut candidate = Candidate {
            common,
            vote_tasks,
            election_timer,
            votes_received: 1, // vote for self
        };
        // A zero-peer cluster reaches majority on the self-vote alone --
        // nothing will ever arrive on the event loop to notice that, so
        // check up front rather than sitting in `run` forever restarting
        // the election every timeout (see
        // `candidate_restarts_the_election_after_its_own_timeout`, which
        // covers the *intended* version of that behavior: peers exist but
        // aren't answering).
        let next_role = if candidate.votes_received >= candidate.common.majority() {
            Role::Leader
        } else {
            candidate.run().await
        };
        (next_role, candidate.shutdown())
    }

    /// Calls `peer`'s RequestVote RPC and, if it answers in time, pushes
    /// the result back as `Event::RequestVoteResponse` so vote counting
    /// happens on the event loop rather than racing with it.
    fn spawn_request_vote(
        peer: PeerInfo,
        term: u64,
        candidate_id: String,
        last_log_index: u64,
        last_log_term: u64,
        conns: Arc<PeerConnections>,
        event_tx: mpsc::Sender<Event>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let req = RequestVoteRequest {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            };
            let Some(resp) = request_vote_from_peer(&peer, req, &conns).await else {
                return;
            };
            let _ = event_tx
                .send(Event::RequestVoteResponse { from: peer.id, resp })
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
                if resp.term == self.common.current_term && resp.vote_granted {
                    self.votes_received += 1;
                    if self.votes_received >= self.common.majority() {
                        return Transition::Become(Role::Leader);
                    }
                }
                Transition::Continue
            }
            Event::AppendEntries { req, respond_to } => {
                // A valid leader for this (or a later) term means the
                // election is over; concede. Only actually advance the
                // term (and persist) if it's strictly higher -- conceding
                // to a leader already at our own term needs no term change.
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
            Event::RequestVote { req, respond_to } => {
                if req.term > self.common.current_term {
                    self.common.advance_term(req.term).await;
                    self.common.vote_for(&req.candidate_id).await;
                    let _ = respond_to.send(RequestVoteResponse {
                        term: self.common.current_term,
                        vote_granted: true,
                    });
                    return Transition::Become(Role::Follower);
                }
                // Already voted for ourselves this term.
                let _ = respond_to.send(RequestVoteResponse {
                    term: self.common.current_term,
                    vote_granted: false,
                });
                Transition::Continue
            }
            Event::ElectionTimeout => Transition::Become(Role::Candidate), // restart the election
            Event::AppendEntriesResponse { .. } | Event::LeaderTick => Transition::Continue,
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
}
