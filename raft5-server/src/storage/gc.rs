//! Collecting versions nothing can read any more.
//!
//! `apply_committed` only ever adds rows, so without this the `versions`
//! keyspace grows with every overwrite forever. That makes this task
//! mandatory on any workload that writes the same key twice -- it is not
//! only about checkpoints, and a cluster that never creates one still
//! needs it running.
//!
//! # What is kept
//!
//! For each key: the newest version, plus -- for each live checkpoint
//! seq `S` -- the newest version with `seq <= S`. Then, if the newest
//! version is a tombstone *and* nothing older survived, that too, so a
//! deleted key's rows disappear entirely.
//!
//! The last clause is the subtle one. A head tombstone may only be
//! dropped when nothing below it is retained: with versions
//! `5 (value), 9 (tombstone)` and a checkpoint at `S = 7`, dropping the
//! tombstone would leave `v5` at the head and a *current* read would see
//! a value that was deleted. When nothing else survives, dropping it is
//! invisible -- `get_as_of` answers `None` for a tombstone head and for
//! an absent key alike -- and that is what reproduces the behaviour of a
//! store where a delete simply removed the row.
//!
//! # Why this can run locally
//!
//! Every row it removes is one that no read -- current, linearizable,
//! transactional or checkpointed -- could have selected, because
//! selection is exactly the rule above. It touches neither `log` nor any
//! watermark Raft depends on, and does not move `applied_seq`. Two nodes
//! may therefore hold different amounts of uncollected garbage with
//! nothing observable differing between them, so this needs no
//! replication and no coordination.
//!
//! The two concurrency cases both fall the safe way. A checkpoint
//! *created* while a pass runs has a seq above everything the pass read,
//! so the version it selects for any key is that key's newest -- which
//! is always retained. A checkpoint *removed* while a pass runs only
//! makes the pass keep more than it had to; the next one collects it.

use super::codec::{Kind, row_chain, row_kind, row_seq};
use super::{META_GC_CURSOR, RaftLog, Result};

/// What one pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GcStats {
    pub rows_visited: u64,
    pub rows_removed: u64,
    /// Whether the pass reached the end of the keyspace. When false it
    /// stopped on its budget and left a cursor to resume from, and the
    /// caller should come straight back rather than wait out its
    /// interval.
    pub done: bool,
}

impl RaftLog {
    /// Collect versions no read can select, visiting at most roughly
    /// `budget` rows before stopping at a key boundary.
    ///
    /// Stopping only at a boundary is what makes the head-tombstone rule
    /// decidable: it needs to know whether anything below the head
    /// survived, which is not known until the key's rows are exhausted.
    pub fn gc_versions(&self, budget: usize) -> Result<GcStats> {
        // Descending, so the walk can take checkpoints in the same order
        // the rows arrive.
        let mut snaps = self.live_checkpoint_seqs()?;
        snaps.sort_unstable_by(|a, b| b.cmp(a));

        let cursor = self.meta.get(META_GC_CURSOR)?;
        let start = cursor.as_deref().map(<[u8]>::to_vec).unwrap_or_default();

        let mut stats = GcStats::default();
        let mut batch = self.db.batch();
        let mut pending = 0usize;

        // Per-key state, reset at each boundary.
        let mut chain: Option<Vec<u8>> = None;
        let mut head: Option<(Vec<u8>, Kind)> = None;
        let mut head_pinned = false;
        let mut kept_below_head = false;
        // How far into `snaps` the walk has got for this key. Rows
        // arrive newest-first and `snaps` is descending, so each
        // checkpoint is passed exactly once per key.
        let mut snap_at = 0usize;
        // Where to resume: the first row of the key after the last one
        // finished. Recorded only at a boundary.
        let mut resume: Option<Vec<u8>> = None;

        for item in self.versions.range(start..) {
            let row = item.key()?;
            let row_chain_bytes = row_chain(&row)?;

            if chain.as_deref() != Some(row_chain_bytes) {
                // Finish the key just left.
                if let Some((head_row, Kind::Tombstone)) = &head
                    && !head_pinned
                    && !kept_below_head
                {
                    batch.remove(&self.versions, head_row.as_slice());
                    pending += 1;
                    stats.rows_removed += 1;
                }
                self.flush_if_full(&mut batch, &mut pending)?;

                if stats.rows_visited >= budget as u64 {
                    resume = Some(row.to_vec());
                    break;
                }

                chain = Some(row_chain_bytes.to_vec());
                head = None;
                head_pinned = false;
                kept_below_head = false;
                snap_at = 0;
            }

            stats.rows_visited += 1;
            let seq = row_seq(&row)?;

            // Any checkpoint at or above this row's seq, and not already
            // satisfied by a newer row, selects this one.
            let mut pinned = false;
            while snap_at < snaps.len() && snaps[snap_at] >= seq {
                snap_at += 1;
                pinned = true;
            }

            if head.is_none() {
                head = Some((row.to_vec(), row_kind(&row)?));
                head_pinned = pinned;
            } else if pinned {
                kept_below_head = true;
            } else {
                batch.remove(&self.versions, &*row);
                pending += 1;
                stats.rows_removed += 1;
                // Safe to flush mid-key, unlike `apply_committed`: every
                // row dropped here is strictly older than the head and
                // already unselected, so no reader can tell, and a crash
                // leaves a key whose head-tombstone decision the next
                // pass makes identically.
                self.flush_if_full(&mut batch, &mut pending)?;
            }
        }

        if resume.is_none() {
            // Ran off the end: finish the last key too.
            if let Some((head_row, Kind::Tombstone)) = &head
                && !head_pinned
                && !kept_below_head
            {
                batch.remove(&self.versions, head_row.as_slice());
                stats.rows_removed += 1;
            }
            stats.done = true;
        }

        match &resume {
            Some(at) => batch.insert(&self.meta, META_GC_CURSOR, at.as_slice()),
            None => batch.remove(&self.meta, META_GC_CURSOR),
        }
        batch.commit()?;
        Ok(stats)
    }
}
