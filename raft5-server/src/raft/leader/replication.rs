//! Getting this leader's log into its peers: what to send one next, the
//! RPCs that send it, and the per-peer task that keeps doing so.
//!
//! All free functions. A replication task owns its peer's `next_index`
//! and nothing else of the leader's, so none of this needs -- or gets --
//! access to `Leader` itself; results come back as `Event`s and are
//! aggregated on the event loop like any other inbound message.

use super::*;

/// What `build_replication_step_sync` decides to send a peer next.
pub(super) enum ReplicationStep {
    AppendEntries(AppendEntriesRequest),
    /// The peer's next expected entry has already been compacted away --
    /// nothing short of a full state transfer can bring it up to date.
    Snapshot,
}

/// Builds the next replication step for a peer whose next expected entry
/// is `next_index`: an `AppendEntries` batching everything from there
/// through the current tail (empty `entries` == a plain heartbeat), or a
/// `Snapshot` step if `next_index`'s predecessor has already been
/// compacted away (so `term_at` can no longer answer for it -- there's no
/// valid `prev_log_term` to send). Synchronous storage core, run inside
/// `Store::expect` by `spawn_replication`.
pub(super) fn build_replication_step_sync(
    log: &RaftLog,
    next_index: u64,
    leader_id: String,
    term: u64,
    leader_commit: u64,
    compact_seq: u64,
) -> storage::Result<ReplicationStep> {
    let prev_log_index = next_index - 1;
    let prev_log_term = if prev_log_index == 0 {
        0
    } else {
        match log.term_at(prev_log_index)? {
            Some(term) => term,
            None => return Ok(ReplicationStep::Snapshot),
        }
    };
    let (last_index, _) = log.last_entry()?.unwrap_or((0, 0));
    let entries = if next_index <= last_index {
        let fetched = log.entries_in_range(next_index, last_index + 1)?;
        // `entries_in_range` silently skips any seq that's been compacted
        // away rather than erroring -- fine for its own documented
        // contract, but if that happened *here* it would replay this
        // peer with silent gaps in its history instead of catching it up
        // correctly. This only catches compaction below `next_index`
        // itself; `prev_log_index > 0` going missing is already caught
        // above via `term_at`, but `prev_log_index == 0` (a peer
        // replaying from scratch) skips that check entirely, so a log
        // whose *earliest* surviving entry is already past `next_index`
        // needs this second check to be caught too.
        let expected = last_index - next_index + 1;
        if (fetched.len() as u64) < expected {
            return Ok(ReplicationStep::Snapshot);
        }
        fetched.into_iter().map(LogEntry::from).collect()
    } else {
        Vec::new()
    };
    Ok(ReplicationStep::AppendEntries(AppendEntriesRequest {
        term,
        leader_id,
        prev_log_index,
        prev_log_term,
        entries,
        leader_commit,
        compact_seq,
    }))
}

/// Sends one `AppendEntries` to `peer`, same shape as
/// `candidate::request_vote_from_peer` but with the more generous
/// `APPEND_ENTRIES_RPC_TIMEOUT` (this one can legitimately take a while --
/// see that constant's doc). `None` on any failure -- the caller (a
/// replication task, running continuously) just retries on its next tick.
async fn append_entries_to_peer(
    peer: &PeerInfo,
    req: AppendEntriesRequest,
    conns: &PeerConnections,
) -> Option<AppendEntriesResponse> {
    let mut client = conns.client(peer)?;
    let attempt = client.append_entries(req);
    match tokio::time::timeout(APPEND_ENTRIES_RPC_TIMEOUT, attempt).await {
        Ok(Ok(resp)) => Some(resp.into_inner()),
        _ => None,
    }
}

/// Exports the current state (on the blocking pool -- fjall's export is
/// synchronous, same discipline as every other storage call) and streams
/// it to `peer` as a `SnapshotHeader` followed by the exported bytes
/// chunked into `SNAPSHOT_CHUNK_SIZE` pieces -- the header carries the
/// Raft-protocol metadata (term/leader_id) the follower's term check
/// needs, while the chunked bytes are exactly what `RaftLog::install_snapshot`
/// expects to parse back out (it has its own embedded magic/version/seq/term,
/// redundant with the header but at a different layer). Bounded by the
/// generous `SNAPSHOT_RPC_TIMEOUT`, since this can dwarf a normal
/// `AppendEntries` batch. `None` on an RPC failure -- the caller just
/// tries again next tick, same policy as everything else -- but a storage
/// read failure panics, same as `build_replication_step_sync`.
async fn send_snapshot_to_peer(
    cancel: &CancellationToken,
    peer: &PeerInfo,
    leader_id: &str,
    term: u64,
    store: &Store,
    conns: &PeerConnections,
) -> Option<(InstallSnapshotResponse, u64)> {
    // Deliberately awaited outright rather than through
    // `until_cancelled`: see that function's doc on why a storage call
    // holding an `Arc<RaftLog>` must never be abandoned mid-flight.
    let (buf, meta) = store
        .expect("export the snapshot for replication", |log| {
            let mut buf = Vec::new();
            let meta = log.export_snapshot(&mut buf)?;
            storage::Result::Ok((buf, meta))
        })
        .await;

    let header = SnapshotChunk {
        part: Some(snapshot_chunk::Part::Header(SnapshotHeader {
            term,
            leader_id: leader_id.to_string(),
            last_included_index: meta.last_included_seq,
            last_included_term: meta.last_included_term,
        })),
    };
    // Collected eagerly (rather than left as a lazy iterator borrowing
    // `buf`) since the stream handed to `install_snapshot` needs its
    // items to be `'static`.
    let chunks: Vec<SnapshotChunk> = buf
        .chunks(SNAPSHOT_CHUNK_SIZE)
        .map(|c| SnapshotChunk {
            part: Some(snapshot_chunk::Part::Data(c.to_vec())),
        })
        .collect();
    let stream = tokio_stream::iter(std::iter::once(header).chain(chunks));

    let mut client = conns.client(peer)?;
    let attempt = client.install_snapshot(stream);
    // The transfer itself is bounded by `SNAPSHOT_RPC_TIMEOUT`, which is
    // five minutes -- far too long for a shutting-down node to sit
    // through, so cancellation cuts it short.
    match until_cancelled(cancel, tokio::time::timeout(SNAPSHOT_RPC_TIMEOUT, attempt)).await {
        Some(Ok(Ok(resp))) => Some((resp.into_inner(), meta.last_included_seq)),
        _ => None,
    }
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
pub(super) fn spawn_replication(
    cancel: &CancellationToken,
    peer: PeerInfo,
    leader_id: String,
    term: u64,
    store: Store,
    conns: Arc<PeerConnections>,
    commit_rx: watch::Receiver<u64>,
    compact_rx: watch::Receiver<u64>,
    event_tx: mpsc::Sender<Event>,
) -> Task {
    Task::spawn(cancel, move |cancel| async move {
        let (last_index, _) = last_log_index_and_term(&store).await;
        let mut next_index = last_index + 1;

        while !cancel.is_cancelled() {
            let leader_commit = *commit_rx.borrow();
            let compact_seq = *compact_rx.borrow();
            let leader_id_for_step = leader_id.clone();
            let step = store
                .expect("read the log for replication", move |log| {
                    build_replication_step_sync(
                        log,
                        next_index,
                        leader_id_for_step,
                        term,
                        leader_commit,
                        compact_seq,
                    )
                })
                .await;

            match step {
                ReplicationStep::AppendEntries(req) => {
                    let Some(answered) =
                        until_cancelled(&cancel, append_entries_to_peer(&peer, req, &conns)).await
                    else {
                        return;
                    };
                    if let Some(resp) = answered {
                        if resp.success {
                            next_index = resp.match_index + 1;
                        } else if let Some(conflict_index) = resp.conflict_index {
                            // TODO (§5.3 leader-side optimization): search
                            // our own log for `conflict_term` and, if
                            // found, skip to just past its last entry
                            // instead of always trusting the follower's
                            // `conflict_index` directly. Still correct
                            // as-is, just occasionally one extra round
                            // trip than strictly necessary.
                            next_index = conflict_index.max(1);
                        }
                        // A term bump is handled on the event loop
                        // (which steps this whole role down, stopping
                        // this task) -- nothing else to do with the
                        // response here either way.
                        if !send_event(
                            &cancel,
                            &event_tx,
                            Event::AppendEntriesResponse {
                                from: peer.id.clone(),
                                resp,
                            },
                        )
                        .await
                        {
                            return;
                        }
                    }
                }
                ReplicationStep::Snapshot => {
                    if let Some((resp, last_included_seq)) =
                        send_snapshot_to_peer(&cancel, &peer, &leader_id, term, &store, &conns)
                            .await
                    {
                        // Whatever the peer's log looked like before,
                        // it now starts fresh right after the
                        // snapshot -- resume normal AppendEntries from
                        // there next tick.
                        next_index = last_included_seq + 1;
                        if !send_event(
                            &cancel,
                            &event_tx,
                            Event::InstallSnapshotResponse {
                                from: peer.id.clone(),
                                resp,
                                last_included_seq,
                            },
                        )
                        .await
                        {
                            return;
                        }
                    }
                }
            }

            if until_cancelled(&cancel, tokio::time::sleep(HEARTBEAT))
                .await
                .is_none()
            {
                return;
            }
        }
    })
}
