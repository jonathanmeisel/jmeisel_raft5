//! The on-disk byte formats, and nothing else.
//!
//! Pure functions: no `RaftLog`, no keyspaces, no I/O, so the formats can
//! be reasoned about and tested on their own.
//!
//! Row keys are never postcard, and must never become postcard: every
//! range scan here depends on a row key sorting byte-wise the same way it
//! sorts by meaning. A fixed-width big-endian `u64` does; a postcard varint
//! does not, so a `log` key written that way would silently break every
//! scan over it. That is why `be_bytes` and `from_be_bytes` are hand-written.
//!
//! A `versions` row key has variable width, which is allowed for the same
//! reason: the escaping below keeps order, the same property big-endian
//! gives integers. See [`row_key`].
//!
//! The formats:
//!
//! - A `log` row: the key is the seq (u64 BE), the value is a
//!   postcard-encoded [`StoredEntry`] (the term plus the entry's payload).
//!   The seq is not in the value because it is the key.
//! - A `versions` row: the key is `escape(user key) || 00 00 || !seq ||
//!   kind` ([`row_key`]), the value is the raw value bytes with no framing
//!   (empty for a tombstone). Every fact about the row (which key, which
//!   seq, whether it is a delete) is in the key, so those questions are
//!   answered without reading the value. That keeps `get_version`,
//!   `scan_latest` and the version GC off the blob files where key-value
//!   separation puts large values.
//!
//! Changing `StoredEntry` changes the on-disk format. In particular,
//! postcard writes an enum discriminant by position, so you may add
//! [`EntryPayload`] variants at the end but never reorder or remove them.

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
/// is the row's key, so the caller already has it.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct StoredEntry {
    pub(super) term: u64,
    pub(super) payload: EntryPayload,
}

pub(super) fn encode_log_entry(term: u64, payload: &EntryPayload) -> Result<Vec<u8>> {
    // `StoredEntry` owns its fields, so encode through a borrowing twin
    // instead of cloning the payload just to serialize it.
    Ok(postcard::to_allocvec(&StoredEntryRef { term, payload })?)
}

/// The serialize-only twin of `StoredEntry`. Same field order, so the
/// output is byte-identical, and `encode_log_entry` can take the payload by
/// reference.
#[derive(Serialize)]
struct StoredEntryRef<'a> {
    term: u64,
    payload: &'a EntryPayload,
}

pub(super) fn decode_log_entry(bytes: &[u8]) -> Result<StoredEntry> {
    Ok(postcard::from_bytes(bytes)?)
}

// --- `versions` row keys ----------------------------------------------

/// The fixed-width tail of every `versions` row key: `!seq` (8 bytes),
/// then `kind` (1).
const TAIL: usize = 9;

/// What a `versions` row says happened to its key at its seq.
///
/// In the row key, not the value, which is why a tombstone costs nothing
/// to detect: "does this key exist as of S?" is answered by a row key, and
/// row keys are never in a blob file. The obvious alternative, an empty
/// value meaning "deleted", is wrong, because a client may write an empty
/// value.
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

/// `key` with every `0x00` byte doubled to `0x00 0x01`, so the `0x00 0x00`
/// terminator that [`chain_start`] appends cannot occur inside it.
///
/// Keeps order: comparing two escaped keys gives the same result as
/// comparing the originals. Where one key is a prefix of the other, the
/// shorter one's terminator (`00 00`) meets either a real byte (`> 0x00`)
/// or an escaped `00 01`, and sorts first either way, which is the order
/// the unescaped keys already have.
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
        // In an escaped key, a `00` is always followed by its `01`.
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
/// Three properties, all essential:
///
/// 1. **Order is kept.** `escape` keeps user-key order (see there), so rows
///    sort by user key first, which lets `GetKeys` scan user-key ranges as
///    before, and all rows of one key are contiguous. A naive `key || seq`
///    does not have this: `"ab" || seq` sorts inside the range
///    `["a" || 00.., "a" || ff..]`, so a per-key scan would pick up another
///    key's rows, and one key's versions would not even be adjacent.
///
/// 2. **Seq descends**, through the bitwise complement. So the newest
///    version is the first row of a key's run, and "the current value" and
///    "the newest version at or before S" are the same forward seek with a
///    different start. `!u64::MAX == 0`, so [`seek_as_of`] at `u64::MAX` is
///    exactly [`chain_start`], which is why reading the current value needs
///    no special case anywhere in this crate.
///
/// 3. **`kind` comes last**, so it is at a fixed offset from the end and
///    costs nothing to read. See [`Kind`].
pub(super) fn row_key(key: &[u8], seq: u64, kind: Kind) -> Vec<u8> {
    let mut out = chain_start(key);
    out.extend_from_slice(&(!seq).to_be_bytes());
    out.push(kind as u8);
    out
}

/// The prefix of every row of `key`: `escape(key) || 00 00`.
///
/// Sorts below all of them (they have eight more bytes), so it is also the
/// inclusive lower bound of a scan that starts at this key.
pub(super) fn chain_start(key: &[u8]) -> Vec<u8> {
    let mut out = escape(key);
    out.push(0);
    out.push(0);
    out
}

/// The exclusive upper bound of `key`'s rows: `escape(key) || 00 01`.
///
/// Above every row of `key` (they have `00 00` at that position), and not
/// above the first row of any later key: an escaped key that continues
/// past this point does so with a byte `>= 01` or with the `00 01` of an
/// escaped zero, and both are `>= 00 01`.
pub(super) fn chain_end(key: &[u8]) -> Vec<u8> {
    let mut out = escape(key);
    out.push(0);
    out.push(1);
    out
}

/// The inclusive lower bound for "the newest version of `key` at or before
/// `as_of`": `chain_start(key) || !as_of`.
///
/// Rows newer than `as_of` have a larger seq, so a smaller complement, so
/// they sort before this bound and are skipped. The first row at or after
/// it is the one wanted, if it is still before [`chain_end`]`(key)` and not
/// a later key's row.
pub(super) fn seek_as_of(key: &[u8], as_of: u64) -> Vec<u8> {
    let mut out = chain_start(key);
    out.extend_from_slice(&(!as_of).to_be_bytes());
    out
}

/// The seq in a row key. A fixed offset from the end, no unescaping.
pub(super) fn row_seq(row: &[u8]) -> Result<u64> {
    let tail = row
        .len()
        .checked_sub(TAIL)
        .ok_or(StorageError::Corrupt("version row key is too short"))?;
    Ok(!from_be_bytes(&row[tail..tail + 8])?)
}

/// The kind in a row key. A fixed offset from the end.
pub(super) fn row_kind(row: &[u8]) -> Result<Kind> {
    let last = row
        .last()
        .ok_or(StorageError::Corrupt("version row key is empty"))?;
    Kind::from_byte(*last)
}

/// The escaped, terminated prefix that says which key this row belongs to
/// (its [`chain_start`]), without unescaping.
///
/// Two rows belong to the same key exactly when these are equal, so a scan
/// can tell it has crossed into the next key with a `memcmp` and no
/// allocation. That lets `scan_as_of` skip the rest of a key's versions
/// without leaving the iterator.
pub(super) fn row_chain(row: &[u8]) -> Result<&[u8]> {
    let tail = row
        .len()
        .checked_sub(TAIL)
        .ok_or(StorageError::Corrupt("version row key is too short"))?;
    Ok(&row[..tail])
}

/// The user key a row key belongs to.
///
/// Allocates and unescapes, so only for code that returns a key to someone
/// (a scan's results, a snapshot export). Everything else compares
/// [`row_chain`].
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
