//! Getting this leader's log into its peers: what to send one next, the
//! RPCs that send it, and the per-peer task that keeps doing so.
//!
//! All free functions. A replication task owns its peer's `next_index`
//! and nothing else of the leader's, so none of this needs -- or gets --
//! access to `Leader` itself; results come back as `Event`s and are
//! aggregated on the event loop like any other inbound message.

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
        /// The byte budget cut this batch short: there is more for this
        /// peer *right now*, so the caller should come straight back
        /// rather than waiting out a heartbeat interval first.
        more_to_send: bool,
    },
    /// The peer's next expected entry has already been compacted away --
    /// nothing short of a full state transfer can bring it up to date.
    Snapshot,
}

/// Whether `entries` is a gap-free run starting exactly at `next_index`.
///
/// `entries_in_range` silently skips a seq that's been compacted away
/// rather than erroring -- fine for its own contract, but a caller that
/// replicated the result anyway would replay this peer with silent holes
/// in its history. So the caller checks, and falls back to a snapshot.
fn contiguous_from(entries: &[storage::LogEntry], next_index: u64) -> bool {
    entries.first().is_some_and(|first| first.seq == next_index)
        && entries.windows(2).all(|w| w[1].seq == w[0].seq + 1)
}

/// Builds the next replication step for a peer whose next expected entry
/// is `next_index`: an `AppendEntries` batching entries from there toward
/// the current tail (empty `entries` == a plain heartbeat), or a
/// `Snapshot` step if `next_index`'s predecessor has already been
/// compacted away (so `term_at` can no longer answer for it -- there's no
/// valid `prev_log_term` to send). Synchronous storage core, run inside
/// `Store::expect` by `spawn_replication`.
///
/// The batch is bounded by `MAX_REPLICATION_BATCH_BYTES` rather than
/// running to the tail. Without a bound, how much a single round has to
/// hold in memory and push through one RPC is decided by how far behind
/// the slowest peer happens to be -- which is unbounded, and is a
/// property of the cluster's history rather than of anything this node
/// chose. A truncated batch is reported back so the caller can send the
/// next one immediately.
///
/// `heartbeat_only` skips reading entries altogether, for a peer whose
/// `next_index` hasn't been confirmed yet -- see `spawn_replication`.
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
        // A short read used to mean "something was compacted away"; with
        // a byte budget it usually just means "that's all that fits", so
        // the check has to be about *which* entries came back rather
        // than how many. An empty result here is its own case: it means
        // everything in the range is gone while `last_entry` is still
        // answering from the floor, and sending it as a heartbeat would
        // leave `next_index` stuck, spinning every tick with no progress
        // and no snapshot.
        //
        // This only catches compaction at or above `next_index`;
        // `prev_log_index > 0` going missing is already caught above via
        // `term_at`, but `prev_log_index == 0` (a peer replaying from
        // scratch) skips that check entirely, so a log whose *earliest*
        // surviving entry is already past `next_index` needs this to be
        // caught too.
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

/// Splits `req` into the stream the wire expects: the header, then its
/// entries encoded once and handed out in `APPEND_ENTRIES_CHUNK_SIZE`
/// slices.
///
/// The slices are `Bytes`, so each one is a refcount on the single
/// encoded buffer rather than a copy of it -- and the iterator is lazy,
/// so they are produced as the connection is ready for them instead of
/// all existing at once. (`send_snapshot_to_peer` collects eagerly with
/// a note about needing `'static` items; a captured `Bytes` is `'static`
/// *and* lazy, which is what makes this cheaper than that.)
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

/// Reads the log and builds the next step for `next_index`, on the
/// blocking pool.
///
/// Reports how long that took, which is worth knowing separately from
/// the round trip that follows: it is the part of a replication round
/// that is pure leader-side work, and so the part any scheme that
/// overlaps rounds could hope to hide.
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

/// Sends one `AppendEntries` to `peer` as a chunked stream, with the
/// generous `APPEND_ENTRIES_RPC_TIMEOUT` covering the whole transfer
/// (this one can legitimately take a while -- see that constant's doc).
///
/// `None` on any failure. The caller retries next tick, same policy as
/// everything else here, but unlike everything else here it also says so
/// out loud -- see `report_replication_failure` for why a silent retry
/// is no longer good enough.
async fn append_entries_to_peer(
    peer: &PeerInfo,
    req: AppendEntriesRequest,
    conns: &PeerConnections,
) -> Option<AppendEntriesResponse> {
    let mut client = conns.client(peer)?;
    // How big a batch this round actually carries, alongside what the
    // round trip costs. A persistently low `entry_count` under load says
    // entries are reaching this task one at a time, so every one of them
    // pays a full round trip and a separate append on the far side.
    let entry_count = req.entries.len();
    let entry_bytes: usize = req.entries.iter().map(prost::Message::encoded_len).sum();
    // Split deliberately: `append_entries_stream` protobuf-encodes the
    // whole batch into one buffer up front, so this is CPU on the
    // leader, once per peer. Pipelining would hide waiting, not this --
    // knowing which half dominates is what says whether pipelining is
    // worth building.
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
    // What this export will be bounded by, read before it starts: the
    // header has to go out ahead of the body, and waiting for the export
    // to finish in order to fill the header in is exactly the
    // buffer-the-whole-thing behaviour being avoided here. See
    // `RaftLog::snapshot_boundary` for why describing it slightly early
    // is safe.
    //
    // Deliberately awaited outright rather than through
    // `until_cancelled`: see that function's doc on why a storage call
    // holding an `Arc<RaftLog>` must never be abandoned mid-flight.
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

    // The export runs on the blocking pool and hands its bytes over a
    // bounded channel as it produces them, so what is in memory at any
    // moment is a few chunks rather than the entire applied state. The
    // task outlives this function if the RPC fails -- dropping the
    // receiver makes its next write fail with a broken pipe, which
    // stops it walking the keyspace for nobody.
    let (tx, rx) = tokio::sync::mpsc::channel(crate::store::CHUNK_BACKLOG);
    let exporting = store.spawn(move |log| {
        let mut writer = ChunkWriter::new(tx);
        let result = log.export_snapshot(&mut writer);
        // `export_snapshot` never flushes -- it doesn't know it is
        // writing to anything that buffers -- so the tail of the
        // snapshot is still held here.
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
    // The transfer itself is bounded by `SNAPSHOT_RPC_TIMEOUT`, which is
    // five minutes -- far too long for a shutting-down node to sit
    // through, so cancellation cuts it short.
    let answered = until_cancelled(cancel, tokio::time::timeout(SNAPSHOT_RPC_TIMEOUT, attempt)).await;

    // Join the exporter either way: on success to surface a storage
    // failure that the response alone would hide, and on failure so the
    // blocking task is finished with the log before this returns.
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

/// Sends `AppendEntries` to `peer` whenever there is something to tell
/// it, tracking `next_index` locally: advances past whatever the peer
/// just accepted, or backs it up using the follower's §5.3 conflict
/// hints on rejection. Every response is forwarded as
/// `Event::AppendEntriesResponse` so match_index aggregation and term
/// step-downs happen on the event loop, not in this task.
///
/// "Something to tell it" is a new log entry (`log_rx`) or a moved
/// commit/compact watermark (`commit_rx`/`compact_rx`, which ride along
/// on `AppendEntries`) -- the task sleeps until one of them changes. It
/// is deliberately *not* a heartbeat: leader liveness is the lease
/// prober's job. A connect/RPC failure or timeout is retried every
/// heartbeat, and a caught-up peer is still contacted every
/// `Timing::idle_replication_interval` so a silently restarted one is
/// noticed. Both are re-read from `timing_rx` on every wait.
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
    timing_rx: watch::Receiver<Timing>,
    mut log_rx: watch::Receiver<u64>,
    mut commit_rx: watch::Receiver<u64>,
    mut compact_rx: watch::Receiver<u64>,
    event_tx: mpsc::Sender<Event>,
) -> Task {
    Task::spawn(cancel, move |cancel| async move {
        let (last_index, _) = last_log_index_and_term(&store).await;
        let mut next_index = last_index + 1;
        // A new leader's `next_index` for a peer is a guess -- its own
        // tail -- and stays a guess until the peer accepts something.
        // Until then, probe with heartbeats instead of reading and
        // encoding a whole batch that a `conflict_index` is about to
        // throw away. Without this, an unreachable peer costs a full
        // `MAX_REPLICATION_BATCH_BYTES` of log reads and encoding every
        // heartbeat, forever, per peer.
        let mut confirmed = false;
        let mut consecutive_failures = 0u32;
        // When the current run of failures started, so the escalation
        // below is about elapsed time rather than attempt count.
        let mut failing_since = Instant::now();

        while !cancel.is_cancelled() {
            // Marked seen *before* the log is read, so a change landing
            // while this round is in flight wakes the wait below rather
            // than being lost.
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

            // Set when a round did all it could for now, so the wait
            // below can be skipped while a backlog is still draining.
            let mut more_to_send = false;
            // Set when the peer couldn't be reached or made no progress,
            // so the wait below is a short retry timer rather than a wait
            // for something new -- nothing new is coming to fix this.
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
                        // A rejection is an answer, not a failure: the
                        // peer is reachable and told us something. But
                        // `next_index` is a guess again until it accepts.
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
                            // TODO (§5.3 leader-side optimization): search
                            // our own log for `conflict_term` and, if
                            // found, skip to just past its last entry
                            // instead of always trusting the follower's
                            // `conflict_index` directly. Still correct
                            // as-is, just occasionally one extra round
                            // trip than strictly necessary.
                            next_index = conflict_index.max(1);
                        }
                        // A rejection that moved `next_index` is worth
                        // retrying at once; one that didn't would just
                        // be rejected again.
                        if !resp.success {
                            if next_index != next_index_before {
                                more_to_send = true;
                            } else {
                                retry_soon = true;
                            }
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
                        // there next tick. A successful install is as
                        // good a confirmation of `next_index` as an
                        // accepted append: we set the peer's log
                        // ourselves, so we know where it is.
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

            // A batch cut short by the byte budget means there is more
            // for this peer right now. Waiting out a heartbeat between
            // every round would make catching up take
            // `backlog / batch × heartbeat` of pure sleeping, which is
            // the difference between a learner catching up inside an
            // election timeout and not.
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

/// Says out loud that replication to a peer isn't getting through.
///
/// `append_entries_to_peer` deliberately reports every failure the same
/// way, because the retry is the same either way -- but "the same" also
/// covers a batch too large to move inside the timeout, which never
/// resolves on its own. That case sticks the commit index with no
/// symptom anywhere, so it is worth a line.
///
/// Logged on the 1st, 2nd, 4th, 8th... consecutive failure, so a peer
/// that is simply down doesn't fill the log.
///
/// Escalated on how long the streak has actually lasted, not on how long
/// it might have: a peer that is refusing connections fails instantly,
/// and counting each failure as a full `APPEND_ENTRIES_RPC_TIMEOUT`
/// would call an unreachable node a stalled transfer within a couple of
/// ticks. And only a batch with entries in it can be too large -- a
/// failing *heartbeat* says the peer is unreachable and nothing else.
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
