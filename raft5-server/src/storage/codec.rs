//! The on-disk byte formats, and nothing else.
//!
//! Pure functions with no `RaftLog`, no keyspaces and no I/O, so the
//! formats can be reasoned about (and tested) on their own.
//!
//! Row *keys* are never postcard, and must never become so: every range
//! scan in this module depends on a row key ordering lexicographically
//! the same way it orders semantically. A fixed-width big-endian `u64`
//! does; a postcard varint does not, so a `log` key written that way
//! would silently break every scan over it. `be_bytes`/`from_be_bytes`
//! stay hand-rolled for that reason.
//!
//! A `versions` row key is variable-width, which is admissible for the
//! same *reason* rather than in spite of it: the escaping below is
//! order-preserving, which is exactly the property big-endian buys for
//! integers. See [`row_key`].
//!
//! The formats are:
//!
//! - a `log` row: key is the seq (u64 BE), value is a postcard-encoded
//!   [`StoredEntry`] -- the term plus the entry's payload. Its seq isn't
//!   in the value because it is the key.
//! - a `versions` row: key is `escape(user key) || 00 00 || !seq || kind`
//!   ([`row_key`]), value is the raw value bytes with no framing at all
//!   (empty for a tombstone). Everything that describes the row -- which
//!   key, which seq, whether it is a delete -- lives in the *key*, so
//!   those questions are answerable without resolving the value. That is
//!   what keeps `get_version`, `scan_latest` and the version GC off the
//!   blob files that `versions`' key-value separation puts large values
//!   in.
//!
//! Changing `StoredEntry` changes the on-disk format. In particular
//! postcard writes an enum's discriminant by *position*, so
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

// --- `versions` row keys ----------------------------------------------

/// The fixed-width tail every `versions` row key ends with: `!seq`
/// (8 bytes) then `kind` (1).
const TAIL: usize = 9;

/// What a `versions` row says happened to its key at its seq.
///
/// In the row *key* rather than the value, which is the whole reason a
/// tombstone costs nothing to detect: "does this key exist as of S?" is
/// answered by a row key, and row keys never live in a blob file. The
/// obvious alternative -- an empty value meaning "deleted" -- is simply
/// wrong, because the empty value is one a client may legitimately
/// write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    /// The key was deleted at this seq.
    Tombstone = 0,
    /// The key was set to this row's value at this seq.
    Value = 1,
}

impl Kind {
    fn from_byte(b: u8) -> Result<Self> {
        match b {
            0 => Ok(Kind::Tombstone),
            1 => Ok(Kind::Value),
            _ => Err(StorageError::Corrupt("unknown version row kind")),
        }
    }
}

/// `key` with every `0x00` byte doubled to `0x00 0x01`, so that the
/// `0x00 0x00` terminator [`chain_start`] appends cannot occur inside
/// it.
///
/// Order-preserving: comparing two escaped keys gives the same answer as
/// comparing the originals. Where one key is a prefix of the other, the
/// shorter one's terminator (`00 00`) meets either a real byte -- which
/// is `> 0x00`, or is itself a `00` that escaped to `00 01` -- and loses
/// either way, which is the ordering the unescaped keys already have.
fn escape(key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(key.len() + 2);
    for &b in key {
        out.push(b);
        if b == 0 {
            out.push(1);
        }
    }
    out
}

fn unescape(escaped: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(escaped.len());
    let mut i = 0;
    while i < escaped.len() {
        let b = escaped[i];
        out.push(b);
        // A `00` in an escaped key is always followed by its `01`.
        if b == 0 {
            match escaped.get(i + 1) {
                Some(1) => i += 2,
                _ => {
                    return Err(StorageError::Corrupt(
                        "malformed escape in a version row key",
                    ));
                }
            }
        } else {
            i += 1;
        }
    }
    Ok(out)
}

/// One `versions` row key: `escape(key) || 00 00 || !seq || kind`.
///
/// Three properties, each load-bearing:
///
/// 1. **Order preservation.** `escape` keeps user-key order (see there),
///    so rows sort by user key first -- which is what lets `GetKeys`
///    keep scanning user-key ranges unchanged -- and every row of one
///    key is contiguous. The naive `key || seq` does *not* have this:
///    `"ab" || seq` sorts inside the range `["a" || 00.., "a" || ff..]`,
///    so a per-key scan would pick up another key's rows and a key's own
///    versions would not even be adjacent.
///
/// 2. **Descending seq**, via the bitwise complement. The newest version
///    is therefore the *first* row of a key's run, so "the current
///    value" and "the newest version at or before S" are one forward
///    seek differing only in where it starts. `!u64::MAX == 0`, so
///    [`seek_as_of`] at `u64::MAX` is exactly [`chain_start`] -- which
///    is why reading the current value needs no special case anywhere in
///    this crate.
///
/// 3. **`kind` last**, so it is a fixed offset from the end and costs
///    nothing to read. See [`Kind`].
pub(super) fn row_key(key: &[u8], seq: u64, kind: Kind) -> Vec<u8> {
    let mut out = chain_start(key);
    out.extend_from_slice(&(!seq).to_be_bytes());
    out.push(kind as u8);
    out
}

/// The prefix every row of `key` begins with: `escape(key) || 00 00`.
///
/// Sorts strictly below all of them (they carry eight more bytes), so it
/// doubles as the inclusive lower bound of a scan that should begin at
/// this key.
pub(super) fn chain_start(key: &[u8]) -> Vec<u8> {
    let mut out = escape(key);
    out.push(0);
    out.push(0);
    out
}

/// The exclusive upper bound of `key`'s rows: `escape(key) || 00 01`.
///
/// Above every row of `key`, which carries `00 00` at that position, and
/// no higher than the first row of any key above it -- an escaped key
/// that continues past this point does so with either a byte `>= 01` or
/// the `00 01` of an escaped zero, and both are `>= 00 01`.
pub(super) fn chain_end(key: &[u8]) -> Vec<u8> {
    let mut out = escape(key);
    out.push(0);
    out.push(1);
    out
}

/// Inclusive lower bound for "the newest version of `key` at or before
/// `as_of`": `chain_start(key) || !as_of`.
///
/// Rows newer than `as_of` have a larger seq, so a smaller complement,
/// so they sort *before* this bound and are skipped. The first row at or
/// after it is the one wanted -- if it is still within
/// [`chain_end`]`(key)`, and not some later key's.
pub(super) fn seek_as_of(key: &[u8], as_of: u64) -> Vec<u8> {
    let mut out = chain_start(key);
    out.extend_from_slice(&(!as_of).to_be_bytes());
    out
}

/// The seq a row key carries. Fixed offset from the end, no unescaping.
pub(super) fn row_seq(row: &[u8]) -> Result<u64> {
    let tail = row
        .len()
        .checked_sub(TAIL)
        .ok_or(StorageError::Corrupt("version row key is too short"))?;
    Ok(!from_be_bytes(&row[tail..tail + 8])?)
}

/// The kind a row key carries. Fixed offset from the end.
pub(super) fn row_kind(row: &[u8]) -> Result<Kind> {
    let last = row
        .last()
        .ok_or(StorageError::Corrupt("version row key is empty"))?;
    Kind::from_byte(*last)
}

/// The escaped-and-terminated prefix identifying which key this row
/// belongs to -- i.e. [`chain_start`] of it, without unescaping.
///
/// Two rows belong to the same key exactly when these are equal, so a
/// scan can tell it has crossed into the next key with a `memcmp` and no
/// allocation. That is what lets `scan_as_of` skip the rest of a key's
/// versions without leaving the iterator.
pub(super) fn row_chain(row: &[u8]) -> Result<&[u8]> {
    let tail = row
        .len()
        .checked_sub(TAIL)
        .ok_or(StorageError::Corrupt("version row key is too short"))?;
    Ok(&row[..tail])
}

/// The user key a row key belongs to.
///
/// Allocates and unescapes, so it is only for the paths that actually
/// hand a key back to someone -- a scan's results, a snapshot export.
/// Everything else compares [`row_chain`] instead.
pub(super) fn row_user_key(row: &[u8]) -> Result<Vec<u8>> {
    let chain = row_chain(row)?;
    let escaped = chain
        .len()
        .checked_sub(2)
        .ok_or(StorageError::Corrupt("version row key has no terminator"))?;
    if chain[escaped..] != [0, 0] {
        return Err(StorageError::Corrupt("version row key has no terminator"));
    }
    unescape(&chain[..escaped])
}

#[cfg(test)]
mod tests;
