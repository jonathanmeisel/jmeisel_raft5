//! Checkpoints: named, point-in-time read views over `versions`.
//!
//! A checkpoint is what the client API calls a snapshot. That word already
//! means Raft's InstallSnapshot in this crate (see `storage::snapshot`), a
//! different thing (a full state transfer to a follower behind the
//! compaction floor), so below the RPC boundary everything says
//! "checkpoint".
//!
//! # A checkpoint's seq is its own record's seq
//!
//! The whole feature rests on this rule, and it is not obvious.
//!
//! A checkpoint must name the seq it reads at. That number cannot be stored
//! in the record, because the append that writes the record assigns it:
//! when the value is built, nobody knows it yet. So it is not stored at
//! all. The record is a normal key in the reserved range, its row in
//! `versions` carries the seq it was written at like every other row, and
//! that seq is the checkpoint's seq.
//!
//! Two consequences, and both matter:
//!
//! - **The record must be written once and never updated.** A second write
//!   would add a newer row, and the checkpoint would silently move forward
//!   in time. `grpc::KvService` enforces this by refusing to create one
//!   that exists.
//! - **Seeing the record proves the node can serve it.** The record lives
//!   at seq `S`, and `apply_committed` applies in strict seq order, so a
//!   node that can read the record has applied everything through `S`:
//!   every version the checkpoint could select is here. "Can this follower
//!   serve this read?" becomes a point lookup, with nothing to coordinate.
//!
//! # Why the reserved range
//!
//! Records are normal key/value pairs under `\0snap/`, as user accounts
//! are under `\0auth/user/` (see `crate::keyspace` for the argument): they
//! replicate, snapshot and survive restarts through the existing machinery,
//! with no second database to keep in sync. Creating a checkpoint is a
//! plain transactional write, so it needs no new log entry kind, and no new
//! failure mode on a peer that would not understand one.

use serde::{Deserialize, Serialize};

use super::{RaftLog, Result};

/// The first bytes of every checkpoint record's key.
///
/// Inside `keyspace::RESERVED`, so the client API refuses to address one
/// directly (`grpc::reject_reserved`) and key scans hide it. The only way
/// to see a checkpoint is `ListSnapshots`.
pub(super) const PREFIX: &[u8] = b"\0snap/";

/// The most checkpoints that may exist at once.
///
/// Each live checkpoint can pin one extra version of every key overwritten
/// since it was taken, so an unbounded count means an unbounded store. Same
/// reasoning, and the same kind of refusal, as
/// `txn::MAX_OPEN_TRANSACTIONS`.
pub const MAX_LIVE_CHECKPOINTS: usize = 32;

/// What a checkpoint record's value holds.
///
/// Metadata only. The seq is not here, on purpose: see the module doc.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct CheckpointRecord {
    /// When the creating client said it created it. For humans reading a
    /// listing; nothing depends on it, and nothing should: it is one client's
    /// clock, stored as given.
    pub(super) created_unix_ms: u64,
}

/// A checkpoint, as callers see it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    pub id: Vec<u8>,
    /// The seq this checkpoint reads at.
    pub seq: u64,
    pub created_unix_ms: u64,
}

/// The record key for `id`.
pub fn checkpoint_key(id: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(PREFIX.len() + id.len());
    key.extend_from_slice(PREFIX);
    key.extend_from_slice(id);
    key
}

pub fn encode_record(created_unix_ms: u64) -> Result<Vec<u8>> {
    Ok(postcard::to_allocvec(&CheckpointRecord {
        created_unix_ms,
    })?)
}

impl RaftLog {
    /// The seq that checkpoint `id` reads at, or `None` if this node has no
    /// such checkpoint.
    ///
    /// Answered from a row key, so the cost does not depend on the record.
    /// `None` covers "never created", "removed", and "created, but this node
    /// has not applied that entry yet". The last is a temporary condition
    /// reported like a permanent one: the same staleness as any read of a key
    /// just written.
    pub fn checkpoint_seq(&self, id: &[u8]) -> Result<Option<u64>> {
        self.version_as_of(&checkpoint_key(id), u64::MAX)
    }

    /// The seq of every live checkpoint, ascending.
    ///
    /// Row keys only: no record value is read, because the GC does not care
    /// what is in them. The GC calls this once per pass, and it is not cached
    /// on purpose: the live set is tens of rows under one prefix, and the real
    /// cost of a cache would be keeping it correct on a follower, where the
    /// only hook is inside `apply_committed`, which would then have to know
    /// what a checkpoint is.
    pub fn live_checkpoint_seqs(&self) -> Result<Vec<u64>> {
        let mut seqs = Vec::new();
        self.for_each_live_record(PREFIX, |_, seq, _| {
            seqs.push(seq);
            Ok(())
        })?;
        seqs.sort_unstable();
        Ok(seqs)
    }

    /// Every live checkpoint, ascending by id, with its metadata.
    ///
    /// Unlike `live_checkpoint_seqs`, this reads each record's value, which
    /// `ListSnapshots` needs and the GC does not.
    pub fn list_checkpoints(&self) -> Result<Vec<Checkpoint>> {
        let mut out = Vec::new();
        self.for_each_live_record(PREFIX, |id, seq, row| {
            let created_unix_ms = match self.versions.get(row)? {
                Some(bytes) => {
                    let record: CheckpointRecord = postcard::from_bytes(&bytes)?;
                    record.created_unix_ms
                }
                // Between listing the row and reading it, the GC could have collected a
                // record that a concurrent remove turned into a tombstone. Report 0, do
                // not fail the whole listing: the checkpoint is going away anyway.
                None => 0,
            };
            out.push(Checkpoint {
                id,
                seq,
                created_unix_ms,
            });
            Ok(())
        })?;
        Ok(out)
    }

    /// How many checkpoints are live. For the cap.
    pub fn checkpoint_count(&self) -> Result<usize> {
        let mut n = 0;
        self.for_each_live_record(PREFIX, |_, _, _| {
            n += 1;
            Ok(())
        })?;
        Ok(n)
    }
}
