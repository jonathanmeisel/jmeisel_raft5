//! The on-disk byte formats, and nothing else.
//!
//! Pure functions with no `RaftLog`, no keyspaces and no I/O, so the
//! formats can be reasoned about (and tested) on their own.
//!
//! Row *values* are serde (postcard). Row *keys* are not, and must never
//! become so: `RaftLog` scans `log` with `range(be_bytes(a)..be_bytes(b))`,
//! which is only correct because a fixed-width big-endian `u64` orders
//! lexicographically the same way it orders numerically. Postcard encodes
//! integers as varints, so a key written that way would silently break
//! every range scan in the module. `be_bytes`/`from_be_bytes` below are
//! for keys (and the fixed-width `meta` values) and stay hand-rolled for
//! that reason.
//!
//! The value formats are:
//!
//! - a `log` row: a postcard-encoded [`StoredEntry`] -- the term plus the
//!   entry's payload. Its seq isn't in there because it is the row's key.
//! - a `latest` row: a postcard-encoded `(seq, value)`. `latest` never
//!   stores a delete marker (see the parent module's doc), so unlike a
//!   `log` row there is no "kind" to discriminate on.
//!
//! Changing either of those types changes the on-disk format. In
//! particular postcard writes an enum's discriminant by *position*, so
//! [`EntryPayload`]'s variants may be appended to but never reordered or
//! removed.

use serde::{Deserialize, Serialize};

use super::{EntryPayload, Result, StorageError};

pub(super) fn be_bytes(v: u64) -> [u8; 8] {
    v.to_be_bytes()
}

pub(super) fn from_be_bytes(bytes: &[u8]) -> Result<u64> {
    let arr: [u8; 8] = bytes
        .try_into()
        .map_err(|_| StorageError::Corrupt("expected 8-byte big-endian u64"))?;
    Ok(u64::from_be_bytes(arr))
}

/// One `log` row's value: everything about an entry except its seq, which
/// is the row's key and so is already in the caller's hand.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct StoredEntry {
    pub(super) term: u64,
    pub(super) payload: EntryPayload,
}

pub(super) fn encode_log_entry(term: u64, payload: &EntryPayload) -> Result<Vec<u8>> {
    // `StoredEntry` borrows nothing, so encode from a reference pair via a
    // shim rather than cloning the payload just to serialize it.
    Ok(postcard::to_allocvec(&StoredEntryRef { term, payload })?)
}

/// The serialize-only twin of `StoredEntry`. Same field order, so it
/// produces byte-identical output, and it lets `encode_log_entry` take the
/// payload by reference.
#[derive(Serialize)]
struct StoredEntryRef<'a> {
    term: u64,
    payload: &'a EntryPayload,
}

pub(super) fn decode_log_entry(bytes: &[u8]) -> Result<StoredEntry> {
    Ok(postcard::from_bytes(bytes)?)
}

/// A `latest` row, for writing. Borrows its value, and carries the
/// `serde_bytes` attribute that keeps it a memcpy rather than a
/// per-byte loop -- see `storage::Mutation` for why that matters.
#[derive(Serialize)]
struct LatestRef<'a> {
    seq: u64,
    #[serde(with = "serde_bytes")]
    value: &'a [u8],
}

/// The same row, for reading. Same field order, so the two agree.
#[derive(Deserialize)]
struct Latest {
    seq: u64,
    #[serde(with = "serde_bytes")]
    value: Vec<u8>,
}

pub(super) fn encode_latest_value(seq: u64, value: &[u8]) -> Result<Vec<u8>> {
    Ok(postcard::to_allocvec(&LatestRef { seq, value })?)
}

pub(super) fn decode_latest_value(bytes: &[u8]) -> Result<(u64, Vec<u8>)> {
    let row: Latest = postcard::from_bytes(bytes)?;
    Ok((row.seq, row.value))
}
