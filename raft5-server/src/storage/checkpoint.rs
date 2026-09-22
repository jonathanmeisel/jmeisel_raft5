//! Checkpoints: named, point-in-time read views over `versions`.
//!
//! A checkpoint is what the client API calls a *snapshot*. The word is
//! already taken in this crate by Raft's InstallSnapshot machinery (see
//! `storage::snapshot`), which is an entirely different thing -- a full
//! state transfer to a follower that has fallen behind the compaction
//! floor -- so everything below the RPC boundary says "checkpoint".
//!
//! # A checkpoint's seq is its own record's seq
//!
//! This is the invariant the whole feature rests on, and it is not
//! obvious.
//!
//! A checkpoint has to name the sequence number it reads at. That number
//! cannot be stored *in* the record, because it is assigned by the append
//! that writes the record -- at the moment the value is built, nobody
//! knows it yet. So it is not stored at all: the record is an ordinary
//! key in the reserved range, its row in `versions` carries the seq it
//! was written at like every other row, and *that* is the checkpoint's
//! seq.
//!
//! Two consequences follow, and both matter:
//!
//! - **The record must be written exactly once and never updated.** A
//!   second write would add a newer row, and the checkpoint would
//!   silently move forward in time. `grpc::KvService` enforces this by
//!   refusing to create one that already exists.
//! - **Seeing the record is proof the node can serve it.** The record
//!   lives at seq `S` and `apply_committed` applies in strict seq order,
//!   so a node that can read the record has necessarily applied
//!   everything through `S` -- every version the checkpoint could
//!   select is already here. "Can this follower serve this read?"
//!   collapses into a point lookup, with nothing to coordinate.
//!
//! # Why the reserved range
//!
//! Records are ordinary key/value pairs under `\0snap/`, exactly as user
//! accounts are under `\0auth/user/` -- see `crate::keyspace`, which
//! makes the argument: they replicate, snapshot and survive restarts
//! through the machinery that already exists, and there is no second
//! database to keep in sync with this one. Creating a checkpoint is a
//! plain transactional write, so it needs no new log entry kind and no
//! new failure mode on a peer that does not understand one.

use serde::{Deserialize, Serialize};

use super::codec::{Kind, chain_start, row_kind, row_seq, row_user_key};
use super::{RaftLog, Result, StorageError};

/// First bytes of every checkpoint record's key.
///
/// Inside `keyspace::RESERVED`, so the client API refuses to address one
/// directly (`grpc::reject_reserved`) and key scans hide it -- the only
/// way to see a checkpoint is `ListSnapshots`.
pub(super) const PREFIX: &[u8] = b"\0snap/";

/// How many checkpoints may exist at once.
///
/// Each live checkpoint can pin one extra version of every key that has
/// been overwritten since it was taken, so an unbounded count is an
/// unbounded store -- the same reasoning, and the same kind of refusal,
/// as `txn::MAX_OPEN_TRANSACTIONS`.
pub const MAX_LIVE_CHECKPOINTS: usize = 32;

/// What a checkpoint record's value holds.
///
/// Metadata only. The seq is deliberately absent -- see the module doc.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct CheckpointRecord {
    /// When the client that created it said it was doing so. For humans
    /// reading a listing; nothing depends on it, and nothing should --
    /// it is one client's clock, recorded verbatim.
    pub(super) created_unix_ms: u64,
}

/// A checkpoint as the outside world sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    pub id: Vec<u8>,
    /// The sequence number this checkpoint reads at.
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
    /// The seq checkpoint `id` reads at, or `None` if there is no such
    /// checkpoint *on this node*.
    ///
    /// Answered from a row key, so it costs the same whatever the record
    /// holds. `None` covers both "never created" and "removed", and also
    /// "created, but this node has not applied that entry yet" -- which
    /// is a transient condition reported like a permanent one, and is
    /// the same staleness any read of a just-written key has.
    pub fn checkpoint_seq(&self, id: &[u8]) -> Result<Option<u64>> {
        self.version_as_of(&checkpoint_key(id), u64::MAX)
    }

    /// The seq of every live checkpoint, ascending.
    ///
    /// Row keys only -- no record value is read, because the GC does not
    /// care what is in them. This is on the GC's path once per pass and
    /// is deliberately not cached: the live set is tens of rows under
    /// one prefix, and a cache's real cost would be keeping it correct
    /// on a follower, where the only hook is inside `apply_committed`,
    /// which would then have to know what a checkpoint is.
    pub fn live_checkpoint_seqs(&self) -> Result<Vec<u64>> {
        let mut seqs = Vec::new();
        self.for_each_live_checkpoint(|_, seq, _| {
            seqs.push(seq);
            Ok(())
        })?;
        seqs.sort_unstable();
        Ok(seqs)
    }

    /// Every live checkpoint, ascending by id, with its metadata.
    ///
    /// Unlike `live_checkpoint_seqs` this resolves each record's value,
    /// which is what `ListSnapshots` needs and the GC does not.
    pub fn list_checkpoints(&self) -> Result<Vec<Checkpoint>> {
        let mut out = Vec::new();
        self.for_each_live_checkpoint(|id, seq, row| {
            let created_unix_ms = match self.versions.get(row)? {
                Some(bytes) => {
                    let record: CheckpointRecord = postcard::from_bytes(&bytes)?;
                    record.created_unix_ms
                }
                // Between listing the row and reading it the GC could
                // have collected a record a concurrent remove
                // tombstoned. Reporting 0 rather than failing the whole
                // listing: the checkpoint is on its way out anyway.
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
        self.for_each_live_checkpoint(|_, _, _| {
            n += 1;
            Ok(())
        })?;
        Ok(n)
    }

    /// Walk the head row of every checkpoint record that currently
    /// exists, in key order, skipping ones whose head is a tombstone --
    /// those are removed checkpoints whose rows the GC has not collected
    /// yet.
    ///
    /// One iterator over the prefix rather than a seek per record, for
    /// the reason `scan_as_of` gives: a single pinned nonce, so the walk
    /// sees one point in time.
    fn for_each_live_checkpoint(
        &self,
        mut f: impl FnMut(Vec<u8>, u64, &[u8]) -> Result<()>,
    ) -> Result<()> {
        let mut settled: Option<Vec<u8>> = None;
        for item in self.versions.range(chain_start(PREFIX)..) {
            let row = item.key()?;
            let key = row_user_key(&row)?;
            if !key.starts_with(PREFIX) {
                break;
            }
            let chain = super::codec::row_chain(&row)?;
            if settled.as_deref() == Some(chain) {
                continue;
            }
            settled = Some(chain.to_vec());
            if row_kind(&row)? != Kind::Value {
                continue;
            }
            let id = key
                .get(PREFIX.len()..)
                .ok_or(StorageError::Corrupt(
                    "checkpoint key is shorter than its prefix",
                ))?
                .to_vec();
            f(id, row_seq(&row)?, &row)?;
        }
        Ok(())
    }
}
