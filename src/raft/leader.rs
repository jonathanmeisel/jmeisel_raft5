//! The leader role: replicates its log to every peer and advances the
//! commit/compaction watermarks as they catch up. See the parent module's
//! doc for how roles fit together generally.

use super::*;

/// Builds the next `AppendEntries` request for a peer whose next expected
/// entry is `next_index`, batching everything from there through the
/// current tail (empty `entries` == a plain heartbeat). Synchronous
/// storage core, run inside `spawn_blocking` by `Leader::spawn_replication`.
fn build_append_entries_sync(
    log: &RaftLog,
    next_index: u64,
    leader_id: String,
    term: u64,
    leader_commit: u64,
    compact_seq: u64,
) -> storage::Result<AppendEntriesRequest> {
    let prev_log_index = next_index - 1;
    // TODO: `None` here means this entry has been compacted away, i.e. the
    // peer has fallen far enough behind that it needs a snapshot rather
    // than more AppendEntries -- not implemented, so this just falls back
    // to 0 and lets the usual conflict/backoff path keep retrying (it
    // won't make progress past this point until entries are available,
    // but it won't corrupt anything either).
    let prev_log_term = if prev_log_index == 0 {
        0
    } else {
        log.term_at(prev_log_index)?.unwrap_or(0)
    };
    let (last_index, _) = log.last_entry()?.unwrap_or((0, 0));
    let entries = if next_index <= last_index {
        // `storage::LogEntry` and `pb::raft5::common::LogEntry` are
        // structurally identical (the proto mirrors the storage type) but
        // are distinct Rust types, so each entry needs a field-by-field copy.
        log.entries_in_range(next_index, last_index + 1)?
            .into_iter()
            .map(|e| LogEntry {
                seq: e.seq,
                term: e.term,
                key: e.key,
                value: e.value,
            })
            .collect()
    } else {
        Vec::new()
    };
    Ok(AppendEntriesRequest {
        term,
        leader_id,
        prev_log_index,
        prev_log_term,
        entries,
        leader_commit,
        compact_seq,
    })
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
    replication_tasks: Vec<JoinHandle<()>>,
    lease_tasks: Vec<JoinHandle<()>>,
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
        let commit_rx = common.commit_tx.subscribe();
        let role_rx = common.role_tx.subscribe();
        let (read_quorum_tx, read_quorum_rx) = watch::channel(false);
        common.publish_role(RoleHandle::Leader(LeaderHandle {
            log: Arc::clone(&common.log),
            term: common.current_term,
            commit_rx,
            role_rx,
            read_quorum_rx,
        }));

        let replication_tasks = common
            .peers
            .iter()
            .cloned()
            .map(|peer| {
                Self::spawn_replication(
                    peer,
                    common.node_id.clone(),
                    common.current_term,
                    Arc::clone(&common.log),
                    Arc::clone(&common.peer_conns),
                    common.commit_tx.subscribe(),
                    common.compact_tx.subscribe(),
                    common.event_tx.clone(),
                )
            })
            .collect();
        let lease_tasks = common
            .peers
            .iter()
            .cloned()
            .map(|peer| {
                spawn_lease_prober(
                    peer,
                    common.node_id.clone(),
                    common.current_term,
                    Arc::clone(&common.peer_conns),
                    common.event_tx.clone(),
                )
            })
            .collect();
        let tick_timer = spawn_leader_tick(common.event_tx.clone());

        let mut leader = Leader {
            common,
            replication_tasks,
            lease_tasks,
            tick_timer,
            peer_match_index: HashMap::new(),
            peer_lease_expiry: HashMap::new(),
            read_quorum_tx,
        };
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
                let req = {
                    let log = Arc::clone(&log);
                    let leader_id = leader_id.clone();
                    tokio::task::spawn_blocking(move || {
                        build_append_entries_sync(
                            &log,
                            next_index,
                            leader_id,
                            term,
                            leader_commit,
                            compact_seq,
                        )
                    })
                    .await
                    .expect("build_append_entries task panicked")
                    .expect("failed to read log for replication")
                };

                if let Some(resp) = append_entries_to_peer(&peer, req, &conns).await {
                    if resp.success {
                        next_index = resp.match_index + 1;
                    } else if let Some(conflict_index) = resp.conflict_index {
                        // TODO (§5.3 leader-side optimization): search our
                        // own log for `conflict_term` and, if found, skip
                        // to just past its last entry instead of always
                        // trusting the follower's `conflict_index`
                        // directly. Still correct as-is, just occasionally
                        // one extra round trip than strictly necessary.
                        next_index = conflict_index.max(1);
                    }
                    // A term bump is handled on the event loop (which
                    // steps this whole role down, aborting this task) --
                    // nothing else to do with the response here either way.
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
                }
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
                Transition::Continue
            }
        }
    }

    /// Recomputes `commit_seq` (highest seq replicated to a majority,
    /// including this node's own log) and `compact_seq` (lowest seq
    /// replicated to *every* peer) from `peer_match_index`, and publishes
    /// both via `Common` -- monotonically, so a stale/out-of-order
    /// response can never make either regress.
    async fn recompute_watermarks(&mut self) {
        let (own_last_index, _) = last_log_index_and_term(&self.common.log).await;

        let mut matched: Vec<u64> = self
            .common
            .peers
            .iter()
            .map(|p| *self.peer_match_index.get(&p.id).unwrap_or(&0))
            .collect();
        matched.push(own_last_index);
        matched.sort_unstable_by(|a, b| b.cmp(a)); // descending

        // The value at position `majority - 1` is the majority-th largest
        // -- i.e. the highest value at least `majority` entries (including
        // our own, always the max) are `>=` to.
        let commit_seq = matched[self.common.majority() - 1];
        // The last (smallest) entry once sorted descending -- the one
        // value every single node, without exception, has reached.
        let compact_seq = *matched.last().expect("always has our own entry");

        self.common.advance_commit(commit_seq);
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
            .peers
            .iter()
            .filter(|p| {
                self.peer_lease_expiry
                    .get(&p.id)
                    .is_some_and(|exp| *exp > now)
            })
            .count()
            + 1; // always trust ourselves
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
        for task in self.replication_tasks {
            task.abort();
        }
        for task in self.lease_tasks {
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
        assert_eq!(seq, 1);

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

        let (common_a, mut role_rx_a, peer_handle_a, _commit_rx_a, _compact_rx_a) = Common::new(
            "node-a".to_string(),
            vec![PeerInfo {
                id: "node-b".to_string(),
                address: format!("http://{addr_b}"),
            }],
            Arc::clone(&log_a),
        )
        .unwrap();
        let (common_b, mut role_rx_b, peer_handle_b, _commit_rx_b, _compact_rx_b) = Common::new(
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
                .add_service(RaftPeerServer::new(RaftPeerService::new(
                    log_a,
                    peer_handle_a,
                )))
                .serve(addr_a),
        );
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(RaftPeerServer::new(RaftPeerService::new(
                    log_b,
                    peer_handle_b,
                )))
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
}
