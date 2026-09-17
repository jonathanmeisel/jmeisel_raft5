//! The `meta` keyspace: every fixed key stored there, and the readers for
//! them.
//!
//! All of these are small scalars that outlive the rows they describe --
//! watermarks that say how far `compact` and `apply_committed` have got,
//! the floor left behind when entries leave `log`, the term/vote pair
//! Raft's safety argument depends on, and the configuration fallback for
//! when the `Config` entry itself has been compacted away. Keeping the key
//! names and their encodings together means "what is in `meta`?" is
//! answered by one file.

use fjall::Keyspace;

use super::Result;
use super::codec::from_be_bytes;

pub(super) const META_FLOOR_SEQ: &[u8] = b"floor_seq";

pub(super) const META_FLOOR_TERM: &[u8] = b"floor_term";

pub(super) const META_COMPACT_SEQ: &[u8] = b"compact_last_seq";

pub(super) const META_APPLIED_SEQ: &[u8] = b"applied_seq";

pub(super) const META_CURRENT_TERM: &[u8] = b"current_term";

pub(super) const META_VOTED_FOR: &[u8] = b"voted_for";

/// The most recent `Config` entry that has been compacted (or snapshotted)
/// out of `log`. `current_config` falls back to this once the entry
/// carrying the active configuration no longer physically exists -- a
/// cluster that hasn't changed membership in a while has its config entry
/// far below the compaction floor, and losing it would leave the node with
/// no idea who its peers are.
pub(super) const META_BASE_CONFIG: &[u8] = b"base_config";

/// The seq/term of the highest entry known to have existed but no
/// longer physically present in `log`, updated by both
/// `install_snapshot` (to the snapshot boundary) and `compact` (to the
/// top of whatever range it just deleted). `last_entry`/`term_at` fall
/// back to this when `log` doesn't have the answer itself.
pub(super) fn read_floor(meta: &Keyspace) -> Result<(u64, u64)> {
    Ok((
        read_u64(meta, META_FLOOR_SEQ)?,
        read_u64(meta, META_FLOOR_TERM)?,
    ))
}

/// The `below_seq` used by the most recent `compact` call (or the
/// snapshot watermark, if `install_snapshot` has run since): entries
/// below this are already reduced to one surviving entry per key, so
/// a future `compact` never needs to look at them again.
pub(super) fn read_compact_watermark(meta: &Keyspace) -> Result<u64> {
    read_u64(meta, META_COMPACT_SEQ)
}

/// How far `latest` has been brought up to date by `apply_committed`
/// -- entries up to and including this seq are already reflected
/// there; nothing past it is (or, for an uncommitted entry, ever
/// should be until it's confirmed committed). `compact` clamps itself
/// to this so it can never physically remove a `log` entry before
/// `apply_committed` has had a chance to copy it into `latest`.
pub(super) fn read_applied_seq(meta: &Keyspace) -> Result<u64> {
    read_u64(meta, META_APPLIED_SEQ)
}

/// Every value in this keyspace that isn't a config blob or a node id is
/// a big-endian `u64`, and every one of them defaults to 0 when it has
/// never been written -- a fresh store has no floor, no watermarks and
/// term 0.
fn read_u64(meta: &Keyspace, key: &[u8]) -> Result<u64> {
    meta.get(key)?.map_or(Ok(0), |bytes| from_be_bytes(&bytes))
}
