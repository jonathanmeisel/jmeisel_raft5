//! Getting the leader's log to its peers: what to send a peer next, the
//! RPCs that send it, and the per-peer task that keeps sending.
//!
//! All free functions. A replication task owns its peer's `next_index`
//! and nothing else of the leader's, so none of this needs access to
//! `Leader`. Results come back as `Event`s and are combined on the event
//! loop like any other inbound message.

use bytes::Bytes;
use tracing::error;
use prost::Message;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};

use std::io::Write;

use crate::store::ChunkWriter;

use super::*;

/// What `build_replication_step_sync` decides to send a peer next.
pub(super) enum ReplicationStep {
    AppendEntries {
        req: AppendEntriesRequest,
        /// The byte budget cut this batch short: there is more for this peer
        /// now, so the caller should come straight back instead of waiting a
        /// heartbeat.
        more_to_send: bool,
    },
    /// The peer's next expected entry is already compacted away. Only a full
    /// state transfer can bring it up to date.
    Snapshot,
}

/// Whether `entries` is a run with no gaps that starts exactly at
/// `next_index`.
///
/// `entries_in_range` skips a compacted seq instead of failing. That fits
/// its own contract, but a caller that replicated the result anyway would
/// give this peer a history with silent holes. So the caller checks, and
/// falls back to a snapshot.
fn contiguous_from(entries: &[storage::LogEntry], next_index: u64) -> bool {
    entries.first().is_some_and(|first| first.seq == next_index)
        && entries.windows(2).all(|w| w[1].seq == w[0].seq + 1)
}

/// Build the next replication step for a peer whose next expected entry is
/// `next_index`:
/// - an `AppendEntries` with entries from there toward the tail (empty
///   `entries` is a plain heartbeat), or
/// - a `Snapshot` step, if the entry before `next_index` is compacted away
///   (then `term_at` cannot answer for it, so there is no valid
///   `prev_log_term` to send).
///
/// The synchronous storage part; `spawn_replication` runs it inside
/// `Store::expect`.
///
/// The batch is bounded by `MAX_REPLICATION_BATCH_BYTES`, not run to the
/// tail. Without a bound, how much one round holds in memory and pushes
/// through one RPC would depend on how far behind the slowest peer is,
/// which has no limit and is a property of the cluster's history, not of
/// any choice this node made. A cut-short batch is reported, so the caller
/// can send the next one at once.
///
/// `heartbeat_only` skips reading entries, for a peer whose `next_index`
/// is not yet confirmed: see `spawn_replication`.
pub(super) fn build_replication_step_sync(
    log: &RaftLog,
    next_index: u64,
    leader_id: String,
    term: u64,
    leader_commit: u64,
    compact_seq: u64,
    heartbeat_only: bool,
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

    let (entries, more_to_send) = if heartbeat_only || next_index > last_index {
        (Vec::new(), false)
    } else {
        let fetched = log.entries_in_range_capped(
            next_index,
            last_index + 1,
            MAX_REPLICATION_BATCH_BYTES,
        )?;
        // With a byte budget, a short read usually means "that is all that
        // fits", so the check is about which entries came back, not how many. An
        // empty result is its own case: everything in the range is gone while
        // `last_entry` still answers from the floor. Sending it as a heartbeat
        // would leave `next_index` stuck, spinning every tick with no progress and
        // no snapshot.
        //
        // This catches only compaction at or above `next_index`. A missing
        // `prev_log_index > 0` is caught above through `term_at`, but
        // `prev_log_index == 0` (a peer replaying from scratch) skips that check,
        // so a log whose earliest surviving entry is already past `next_index`
        // needs this check too.
        if !contiguous_from(&fetched.entries, next_index) {
            return Ok(ReplicationStep::Snapshot);
        }
        (
            fetched.entries.into_iter().map(LogEntry::from).collect(),
            fetched.truncated,
        )
    };

    Ok(ReplicationStep::AppendEntries {
        req: AppendEntriesRequest {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
            compact_seq,
        },
        more_to_send,
    })
}

/// Split `req` into the stream the wire expects: the header, then its
/// entries, encoded once and handed out in `APPEND_ENTRIES_CHUNK_SIZE`
/// slices.
///
/// The slices are `Bytes`, so each one is a refcount on the single encoded
/// buffer, not a copy. The iterator is lazy, so slices are made as the
/// connection is ready for them, not all at once.
pub(crate) fn append_entries_stream(req: AppendEntriesRequest) -> impl Stream<Item = AppendEntriesChunk> {
    let encoded = Bytes::from(
        EntryBatch {
            entries: req.entries,
        }
        .encode_to_vec(),
    );
    let header = AppendEntriesChunk {
        part: Some(append_entries_chunk::Part::Header(AppendEntriesHeader {
            term: req.term,
            leader_id: req.leader_id,
            prev_log_index: req.prev_log_index,
            prev_log_term: req.prev_log_term,
            leader_commit: req.leader_commit,
            compact_seq: req.compact_seq,
            entries_bytes: encoded.len() as u64,
        })),
    };
    let body = (0..).map_while(move |i: usize| {
        let start = i * APPEND_ENTRIES_CHUNK_SIZE;
        (start < encoded.len()).then(|| {
            let end = (start + APPEND_ENTRIES_CHUNK_SIZE).min(encoded.len());
            AppendEntriesChunk {
                part: Some(append_entries_chunk::Part::Entries(encoded.slice(start..end))),
            }
        })
    });
    tokio_stream::iter(std::iter::once(header).chain(body))
}

/// Read the log and build the next step for `next_index`, on the blocking
/// pool.
///
/// Reports how long that took, which is worth knowing apart from the round
/// trip that follows: it is the purely leader-side part of a replication
/// round, so it is the part that overlapping rounds could hope to hide.
#[allow(clippy::too_many_arguments)]
async fn prepare_step(
    store: &Store,
    next_index: u64,
    leader_id: &str,
    term: u64,
    leader_commit: u64,
    compact_seq: u64,
    heartbeat_only: bool,
) -> ReplicationStep {
    let leader_id = leader_id.to_string();
    let started = Instant::now();
    let step = store
        .expect("read the log for replication", move |log| {
            build_replication_step_sync(
                log,
                next_index,
                leader_id,
                term,
                leader_commit,
                compact_seq,
                heartbeat_only,
            )
        })
        .await;
    if let ReplicationStep::AppendEntries { req, .. } = &step
        && !req.entries.is_empty()
    {
        tracing::info!(
            entry_count = req.entries.len(),
            prepare_ms = started.elapsed().as_millis(),
            "prepared a batch"
        );
    }
    step
}

/// Send one `AppendEntries` to `peer` as a chunked stream, with the
/// generous `APPEND_ENTRIES_RPC_TIMEOUT` covering the whole transfer (it
/// can take a while; see that constant's doc).
///
/// `None` on any failure. The caller retries next tick, like everything
/// else here, but this also logs the failure: see
/// `report_replication_failure` for why a silent retry is not enough.
async fn append_entries_to_peer(
    peer: &PeerInfo,
    req: AppendEntriesRequest,
    conns: &PeerConnections,
) -> Option<AppendEntriesResponse> {
    let mut client = conns.client(peer)?;
    // How big a batch this round carries, next to what the round trip costs.
    // A low `entry_count` under load means entries reach this task one at a
    // time, so each pays a full round trip and a separate append on the far
    // side.
    let entry_count = req.entries.len();
    let entry_bytes: usize = req.entries.iter().map(prost::Message::encoded_len).sum();
    // Measured apart: `append_entries_stream` encodes the whole batch into one
    // buffer up front, so this is leader CPU, once per peer. Pipelining would
    // hide waiting, not this; knowing which half dominates says whether
    // pipelining is worth building.
    let encode_started = Instant::now();
    let stream = append_entries_stream(req);
    let encode_ms = encode_started.elapsed().as_millis();

    let started = Instant::now();
    let attempt = client.append_entries(stream);
    let answer = match tokio::time::timeout(APPEND_ENTRIES_RPC_TIMEOUT, attempt).await {
        Ok(Ok(resp)) => Some(resp.into_inner()),
        _ => None,
    };
    if entry_count > 0 {
        tracing::info!(
            peer = %peer.id,
            entry_count,
            entry_bytes,
            encode_ms,
            rpc_ms = started.elapsed().as_millis(),
            ok = answer.is_some(),
            "replicated a batch"
        );
    }
    answer
}

/// Export the current state and stream it to `peer`: a `SnapshotHeader`,
/// then the exported bytes in chunks.
///
/// - The header carries the Raft metadata (term, leader_id) that the
///   follower's term check needs.
/// - The chunks are exactly what `RaftLog::install_snapshot` parses. They
///   have their own magic, version, seq and term, which repeat the header
///   at a different layer.
///
/// The export runs on the blocking pool (fjall's export is synchronous,
/// as with every storage call) and feeds the stream through a
/// `store::ChunkWriter`. Bounded by the generous `SNAPSHOT_RPC_TIMEOUT`,
/// because this can be far larger than an `AppendEntries` batch. `None` on
/// an RPC failure (the caller tries again next tick, as everywhere here),
/// but a storage read failure panics, as in `build_replication_step_sync`.
async fn send_snapshot_to_peer(
    cancel: &CancellationToken,
    peer: &PeerInfo,
    leader_id: &str,
    term: u64,
    store: &Store,
    conns: &PeerConnections,
) -> Option<(InstallSnapshotResponse, u64)> {
    // Read the export's boundary before it starts: the header must go out
    // before the body, and waiting for the export to finish to fill in the
    // header is the buffer-everything behavior this avoids. See
    // `RaftLog::snapshot_boundary` for why describing it slightly early is
    // safe.
    //
    // Awaited directly, not through `until_cancelled`: see that function's
    // doc for why a storage call that holds an `Arc<RaftLog>` must never be
    // abandoned midway.
    let (last_included_seq, last_included_term) = store
        .expect("read the snapshot boundary for replication", RaftLog::snapshot_boundary)
        .await;

    let header = SnapshotChunk {
        part: Some(snapshot_chunk::Part::Header(SnapshotHeader {
            term,
            leader_id: leader_id.to_string(),
            last_included_index: last_included_seq,
            last_included_term,
        })),
    };

    // The export runs on the blocking pool and passes its bytes through a
    // bounded channel as it makes them, so only a few chunks are in memory at
    // once, not the whole applied state. If the RPC fails, the task outlives
    // this function: dropping the receiver makes its next write fail with a
    // broken pipe, which stops it from walking the keyspace for nobody.
    let (tx, rx) = tokio::sync::mpsc::channel(crate::store::CHUNK_BACKLOG);
    let exporting = store.spawn(move |log| {
        let mut writer = ChunkWriter::new(tx);
        let result = log.export_snapshot(&mut writer);
        // `export_snapshot` never flushes (it does not know its writer buffers),
        // so the tail of the snapshot is still held here.
        result.and_then(|meta| {
            writer.flush()?;
            Ok(meta)
        })
    });

    let body = ReceiverStream::new(rx).map(|data| SnapshotChunk {
        part: Some(snapshot_chunk::Part::Data(data)),
    });
    let stream = tokio_stream::iter(std::iter::once(header)).chain(body);

    let mut client = conns.client(peer)?;
    let attempt = client.install_snapshot(stream);
    // `SNAPSHOT_RPC_TIMEOUT` bounds the transfer, at two hours: far too long
    // for a shutting-down node to wait, so cancellation cuts it short.
    let answered = until_cancelled(cancel, tokio::time::timeout(SNAPSHOT_RPC_TIMEOUT, attempt)).await;

    // Join the exporter either way: on success, to surface a storage failure
    // that the response alone would hide; on failure, so the blocking task is
    // done with the log before this returns.
    match exporting.await {
        Ok(Ok(_meta)) => {}
        Ok(Err(e)) => {
            warn!("exporting a snapshot for {} failed: {e}", peer.id);
            return None;
        }
        Err(e) => panic!("the snapshot export task failed: {e}"),
    }

    match answered {
        Some(Ok(Ok(resp))) => Some((resp.into_inner(), last_included_seq)),
        _ => {
            warn!("install_snapshot to {} did not complete; retrying", peer.id);
            None
        }
    }
}

/// Send `AppendEntries` to `peer` whenever there is something to tell it,
/// and track `next_index` locally: move past what the peer accepted, or
/// back up with the follower's §5.3 conflict hints on a rejection. Every
/// response is forwarded as `Event::AppendEntriesResponse`, so match_index
/// aggregation and term step-downs happen on the event loop, not here.
///
/// "Something to tell it" is a new log entry (`log_rx`), or a moved commit
/// or compaction watermark (`commit_rx`, `compact_rx`, which travel on
/// `AppendEntries`). The task sleeps until one of them changes. This is not
/// a heartbeat, on purpose: leader liveness is the lease prober's job.
/// - A connect or RPC failure, or a timeout, is retried every heartbeat.
/// - A caught-up peer is still contacted every
///   `Timing::idle_replication_interval`, so a silently restarted one is
///   noticed.
///
/// Both intervals are read from `timing_rx` on every wait.
// Each parameter is separate context this task owns for its whole life,
// or a channel it uses. No natural group would make a struct that does
// more than move the same list to the one call site.
#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_replication(
    cancel: &CancellationToken,
    peer: PeerInfo,
    leader_id: String,
    term: u64,
    store: Store,
    conns: Arc<PeerConnections>,
    timing_rx: watch::Receiver<Timing>,
    mut log_rx: watch::Receiver<u64>,
    mut commit_rx: watch::Receiver<u64>,
    mut compact_rx: watch::Receiver<u64>,
    event_tx: mpsc::Sender<Event>,
) -> Task {
    Task::spawn(cancel, move |cancel| async move {
        let (last_index, _) = last_log_index_and_term(&store).await;
        let mut next_index = last_index + 1;
        // A new leader's `next_index` for a peer is a guess (its own tail) until
        // the peer accepts something. Until then, probe with heartbeats instead
        // of reading and encoding a whole batch that a `conflict_index` will
        // discard. Otherwise an unreachable peer would cost a full
        // `MAX_REPLICATION_BATCH_BYTES` of log reads and encoding every
        // heartbeat, forever, per peer.
        let mut confirmed = false;
        let mut consecutive_failures = 0u32;
        // When the current run of failures started, so the escalation below
        // depends on elapsed time, not on the attempt count.
        let mut failing_since = Instant::now();

        while !cancel.is_cancelled() {
            // Marked seen before the log is read, so a change that lands while this
            // round is in flight wakes the wait below instead of being lost.
            let leader_commit = *commit_rx.borrow_and_update();
            let compact_seq = *compact_rx.borrow_and_update();
            log_rx.borrow_and_update();
            let next_index_before = next_index;
            let heartbeat_only = !confirmed;

            let step = prepare_step(
                &store,
                next_index,
                &leader_id,
                term,
                leader_commit,
                compact_seq,
                heartbeat_only,
            )
            .await;

            // Set when a round did all it could for now, so the wait below is skipped
            // while a backlog drains.
            let mut more_to_send = false;
            // Set when the peer could not be reached or made no progress, so the wait
            // below is a short retry timer, not a wait for something new: nothing new
            // will fix this.
            let mut retry_soon = false;

            match step {
                ReplicationStep::AppendEntries { req, more_to_send: more } => {
                    let entry_count = req.entries.len();
                    let Some(answered) =
                        until_cancelled(&cancel, append_entries_to_peer(&peer, req, &conns)).await
                    else {
                        return;
                    };
                    match &answered {
                        Some(resp) if resp.success => {
                            confirmed = true;
                            consecutive_failures = 0;
                            more_to_send = more;
                        }
                        // A rejection is an answer, not a failure: the peer is reachable and told
                        // us something. But `next_index` is a guess again until it accepts.
                        Some(_) => {
                            confirmed = false;
                            consecutive_failures = 0;
                        }
                        None => {
                            retry_soon = true;
                            if consecutive_failures == 0 {
                                failing_since = Instant::now();
                            }
                            consecutive_failures += 1;
                            confirmed = false;
                            report_replication_failure(
                                &peer,
                                next_index,
                                entry_count,
                                consecutive_failures,
                                failing_since.elapsed(),
                            );
                        }
                    }
                    if let Some(resp) = answered {
                        if resp.success {
                            next_index = resp.match_index + 1;
                        } else if let Some(conflict_index) = resp.conflict_index {
                            // TODO (§5.3 leader-side optimization): search our own log for
                            // `conflict_term` and, if found, skip to just past its last entry instead
                            // of always trusting the follower's `conflict_index`. Correct as is; it
                            // only costs an extra round trip now and then.
                            next_index = conflict_index.max(1);
                        }
                        // A rejection that moved `next_index` is worth retrying at once; one that
                        // did not would just be rejected again.
                        if !resp.success {
                            if next_index != next_index_before {
                                more_to_send = true;
                            } else {
                                retry_soon = true;
                            }
                        }
                        // The event loop handles a term bump (it steps this whole role down, which
                        // stops this task), so there is nothing else to do with the response here.
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
                        // Whatever the peer's log was before, it now starts right after the
                        // snapshot, so resume normal AppendEntries from there next tick. A
                        // successful install confirms `next_index` as well as an accepted append
                        // does: we set the peer's log ourselves, so we know where it is.
                        next_index = last_included_seq + 1;
                        confirmed = true;
                        consecutive_failures = 0;
                        more_to_send = true;
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
                    } else {
                        retry_soon = true;
                    }
                }
            }

            // A batch cut short by the byte budget means there is more for this peer
            // now. Waiting a heartbeat between rounds would make catching up take
            // `backlog / batch × heartbeat` of pure sleep, which decides whether a
            // learner catches up within an election timeout.
            if more_to_send {
                continue;
            }
            let timing = *timing_rx.borrow();
            let woke = until_cancelled(&cancel, async {
                if retry_soon {
                    tokio::time::sleep(timing.heartbeat()).await;
                    return true;
                }
                // A closed channel means the node is shutting down.
                tokio::select! {
                    r = log_rx.changed() => r.is_ok(),
                    r = commit_rx.changed() => r.is_ok(),
                    r = compact_rx.changed() => r.is_ok(),
                    _ = tokio::time::sleep(timing.idle_replication_interval()) => true,
                }
            })
            .await;
            if woke != Some(true) {
                return;
            }
        }
    })
}

/// Log that replication to a peer is not getting through.
///
/// `append_entries_to_peer` reports every failure the same way, because
/// the retry is the same. But that includes a batch too large to move in
/// the timeout, which never resolves by itself and blocks the commit index
/// with no other symptom, so it deserves a log line.
///
/// Logged on the 1st, 2nd, 4th, 8th, ... failure in a row, so a peer that
/// is simply down does not fill the log.
///
/// Escalated by how long the streak has really lasted, not how long it
/// could have: a peer that refuses connections fails at once, and counting
/// each failure as a full `APPEND_ENTRIES_RPC_TIMEOUT` would call an
/// unreachable node a stalled transfer within a few ticks. Only a batch
/// with entries can be too large; a failing heartbeat only says the peer is
/// unreachable.
fn report_replication_failure(
    peer: &PeerInfo,
    next_index: u64,
    entry_count: usize,
    consecutive: u32,
    failing_for: Duration,
) {
    if !consecutive.is_power_of_two() {
        return;
    }
    if entry_count > 0 && failing_for >= REPLICATION_STALL_WARNING {
        error!(
            "replication to {} has been failing for {failing_for:?} ({consecutive} \
             attempts, next_index {next_index}, {entry_count} entries): nothing \
             after these entries can commit while they cannot be delivered -- \
             they may be too large for this link",
            peer.id
        );
    } else {
        warn!(
            "append_entries to {} failed ({consecutive} in a row over \
             {failing_for:?}, next_index {next_index}, {entry_count} entries); \
             retrying",
            peer.id
        );
    }
}
