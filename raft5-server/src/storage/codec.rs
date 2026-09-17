//! The on-disk byte formats, and nothing else.
//!
//! Pure functions over `&[u8]` with no `RaftLog`, no keyspaces and no I/O,
//! so the wire format can be reasoned about (and tested) on its own. Two
//! encodings live here:
//!
//! - a `log` row: `[term: u64 BE][tag: u8][key_len: u32 BE][key][value?]`
//! - a `latest` row: `[seq: u64 BE][value]`, with no tag byte, because
//!   `latest` never stores a delete marker (see the parent module's doc).

use super::{EntryKind, Result, StorageError};

pub(super) const TAG_PUT: u8 = 0;

pub(super) const TAG_DELETE: u8 = 1;

pub(super) const TAG_NOOP: u8 = 2;

pub(super) const TAG_CONFIG: u8 = 3;

pub(super) fn be_bytes(v: u64) -> [u8; 8] {
    v.to_be_bytes()
}

pub(super) fn from_be_bytes(bytes: &[u8]) -> Result<u64> {
    let arr: [u8; 8] = bytes
        .try_into()
        .map_err(|_| StorageError::Corrupt("expected 8-byte big-endian u64"))?;
    Ok(u64::from_be_bytes(arr))
}

pub(super) fn encode_log_entry(
    term: u64,
    kind: EntryKind,
    key: &[u8],
    value: Option<&[u8]>,
) -> Vec<u8> {
    let value_len = value.map_or(0, <[u8]>::len);
    let mut buf = Vec::with_capacity(8 + 1 + 4 + key.len() + value_len);
    buf.extend_from_slice(&term.to_be_bytes());
    buf.push(match kind {
        EntryKind::Put => TAG_PUT,
        EntryKind::Delete => TAG_DELETE,
        EntryKind::NoOp => TAG_NOOP,
        EntryKind::Config => TAG_CONFIG,
    });
    buf.extend_from_slice(&(key.len() as u32).to_be_bytes());
    buf.extend_from_slice(key);
    if let Some(v) = value {
        buf.extend_from_slice(v);
    }
    buf
}

/// One decoded `log` row. Its seq isn't in here because it is the row's
/// *key*, not part of the encoded value -- the caller already has it.
pub(super) struct Decoded {
    pub(super) term: u64,
    pub(super) kind: EntryKind,
    pub(super) key: Vec<u8>,
    pub(super) value: Option<Vec<u8>>,
}

pub(super) fn decode_log_entry(bytes: &[u8]) -> Result<Decoded> {
    if bytes.len() < 13 {
        return Err(StorageError::Corrupt("log entry too short"));
    }
    let term = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
    let tag = bytes[8];
    let key_len = u32::from_be_bytes(bytes[9..13].try_into().unwrap()) as usize;
    if bytes.len() < 13 + key_len {
        return Err(StorageError::Corrupt("log entry key_len out of bounds"));
    }
    let key = bytes[13..13 + key_len].to_vec();
    let (kind, value) = match tag {
        TAG_PUT => (EntryKind::Put, Some(bytes[13 + key_len..].to_vec())),
        TAG_DELETE => (EntryKind::Delete, None),
        TAG_NOOP => (EntryKind::NoOp, None),
        TAG_CONFIG => (EntryKind::Config, Some(bytes[13 + key_len..].to_vec())),
        _ => return Err(StorageError::Corrupt("log entry has unknown tag")),
    };
    Ok(Decoded {
        term,
        kind,
        key,
        value,
    })
}

pub(super) fn encode_latest_value(seq: u64, value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8 + value.len());
    buf.extend_from_slice(&seq.to_be_bytes());
    buf.extend_from_slice(value);
    buf
}

pub(super) fn decode_latest_value(bytes: &[u8]) -> Result<(u64, Vec<u8>)> {
    if bytes.len() < 8 {
        return Err(StorageError::Corrupt("latest value too short"));
    }
    let seq = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
    Ok((seq, bytes[8..].to_vec()))
}
