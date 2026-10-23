//! The `meta` keyspace: every fixed key stored there, and its readers.
//!
//! These are small scalars that outlive the rows they describe:
//! - the watermarks for how far `compact` and `apply_committed` have got,
//! - the floor left when entries leave `log`,
//! - the term and vote pair that Raft's safety depends on,
//! - the configuration fallback for when the `Config` entry itself is
//!   compacted away.
//!
//! The key names and encodings are kept together, so one file answers
//! "what is in `meta`?".

use fjall::Keyspace;

use super::Result;
use super::codec::from_be_bytes;

pub(super) const META_FLOOR_SEQ: &[u8] = b"floor_seq";

pub(super) const META_FLOOR_TERM: &[u8] = b"floor_term";

pub(super) const META_COMPACT_SEQ: &[u8] = b"compact_last_seq";

pub(super) const META_APPLIED_SEQ: &[u8] = b"applied_seq";

pub(super) const META_CURRENT_TERM: &[u8] = b"current_term";

pub(super) const META_VOTED_FOR: &[u8] = b"voted_for";

/// Present while `restore_backup` runs. A store that still has it after a
/// restart had a restore interrupted, and holds partial data that neither
/// bootstrap path may build on.
pub(super) const META_RESTORE_IN_PROGRESS: &[u8] = b"restore_in_progress";

/// The newest `Config` entry that was compacted (or snapshotted) out of
/// `log`. `current_config` falls back to this once the entry with the
/// active configuration no longer exists. A cluster that has not changed
/// membership for a while has its configuration entry far below the
/// compaction floor, and losing it would leave the node not knowing its
/// peers.
pub(super) const META_BASE_CONFIG: &[u8] = b"base_config";

/// Where the next version-GC pass resumes.
///
/// Not a seq like the watermarks nearby, but a `versions` row key: GC's
/// unit of progress is one key's run of versions, not one seq, so it must
/// remember a position in the keyspace. Absent means "start from the
/// beginning", for a new store and after a pass that ran to the end. See
/// `gc_versions`.
pub(super) const META_GC_CURSOR: &[u8] = b"gc_cursor";

/// The seq and term of the highest entry that existed but is no longer in
/// `log`. `install_snapshot` sets it to the snapshot boundary, and
/// `compact` to the top of the range it deleted. `last_entry` and
/// `term_at` fall back to it when `log` cannot answer.
pub(super) fn read_floor(meta: &Keyspace) -> Result<(u64, u64)> {
    Ok((
        read_u64(meta, META_FLOOR_SEQ)?,
        read_u64(meta, META_FLOOR_TERM)?,
    ))
}

/// The `below_seq` of the latest `compact` call (or the snapshot
/// watermark, if `install_snapshot` ran since). Entries below it are
/// already handled, so a later `compact` never needs to look at them
/// again.
pub(super) fn read_compact_watermark(meta: &Keyspace) -> Result<u64> {
    read_u64(meta, META_COMPACT_SEQ)
}

/// How far `apply_committed` has brought `versions`: entries up to and
/// including this seq are there, nothing past it is (and an uncommitted
/// entry must not be until it commits). `compact` clamps itself to this,
/// so it never removes a `log` entry before `apply_committed` has copied
/// it into `versions`.
pub(super) fn read_applied_seq(meta: &Keyspace) -> Result<u64> {
    read_u64(meta, META_APPLIED_SEQ)
}

/// Every value in this keyspace that is not a configuration blob or a
/// node id is a big-endian `u64`, and each one is 0 when never written: a
/// new store has no floor, no watermarks, and term 0.
fn read_u64(meta: &Keyspace, key: &[u8]) -> Result<u64> {
    meta.get(key)?.map_or(Ok(0), |bytes| from_be_bytes(&bytes))
}
