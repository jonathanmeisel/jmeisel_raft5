//! Raft log storage backed by `fjall`.
//!
//! Layout (three keyspaces):
//! - `log`:    seq (u64 BE)                -> encoded (term, key, value|tombstone) entry
//! - `latest`: key                          -> encoded (seq, value|tombstone) current state
//! - `meta`:   small set of fixed keys: the floor (`floor_seq`,
//!   `floor_term`) — the seq/term of the highest entry no longer physically
//!   in `log`, updated by both `install_snapshot` and `compact` — and the
//!   compaction watermark (`compact_last_seq`) recorded by `compact` alone,
//!   for resuming an incremental compaction.
//!
//! A single append writes `log` and `latest` in one atomic `WriteBatch`, so
//! they never diverge. Sequence numbers are assigned under a mutex held for
//! the full duration of the write, so a failed write never leaves a gap or
//! is double-assigned.
//!
//! Deletes are appended entries with no value (a tombstone), not row
//! removal: `append(term, key, None)` records that the key was deleted at
//! that seq, same as any other entry, and participates in `compact`
//! normally. `latest` keeps a tombstone marker too, so `get_latest` can
//! distinguish "never written" (`None`) from "deleted, most recently at seq
//! N" (`Some((n, None))`) — but only until `compact` passes that seq: once
//! a tombstone's seq drops below the compaction threshold, `compact` drops
//! it from `latest` too, since at that point "deleted a while ago" and
//! "never written" are indistinguishable and there's nothing to gain from
//! keeping the marker. `export_snapshot` omits tombstoned keys, since a
//! snapshot represents only current state.
//!
//! Note on compaction and term lookups: `compact` unconditionally deletes
//! every `log` entry below its threshold, so a term_at/entries_in_range
//! query can find gaps there even when they're not below any snapshot
//! boundary. This is safe for Raft's log-matching purposes: entries below a
//! node's own compaction/snapshot point are already committed and can never
//! be a source of conflict again (Leader Completeness), so callers should
//! never need term information that far back — comparisons only ever happen
//! in the live tail. Live entries in `latest` — puts, and tombstones not
//! yet passed by compaction — are never touched.
//!
//! Note on memory: `compact` and `install_snapshot` operate on fjall
//! iterators, which pin a consistent point-in-time view as of iterator
//! creation — so it's safe to interleave reading from one with issuing
//! writes/deletes against the live `Database` from the same loop. Both use
//! this to process the keyspaces in bounded chunks rather than buffering an
//! entire range into memory before acting on it.

use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Mutex;

use fjall::{Database, Keyspace, KeyspaceCreateOptions, OwnedWriteBatch};

const CF_LOG: &str = "log";
const CF_LATEST: &str = "latest";
const CF_META: &str = "meta";

const META_FLOOR_SEQ: &[u8] = b"floor_seq";
const META_FLOOR_TERM: &[u8] = b"floor_term";
const META_COMPACT_SEQ: &[u8] = b"compact_last_seq";
const META_CURRENT_TERM: &[u8] = b"current_term";
const META_VOTED_FOR: &[u8] = b"voted_for";

const SNAPSHOT_MAGIC: &[u8; 8] = b"RLOGSNAP";
const SNAPSHOT_VERSION: u32 = 1;

/// Max pending writes buffered in a `WriteBatch` before it's flushed, used
/// everywhere we process a potentially large range in bounded chunks.
const WRITE_CHUNK: usize = 1000;

const TAG_PUT: u8 = 0;
const TAG_DELETE: u8 = 1;

#[derive(Debug)]
pub enum StorageError {
    Db(fjall::Error),
    Io(std::io::Error),
    Corrupt(&'static str),
}

impl From<fjall::Error> for StorageError {
    fn from(e: fjall::Error) -> Self {
        StorageError::Db(e)
    }
}

impl From<std::io::Error> for StorageError {
    fn from(e: std::io::Error) -> Self {
        StorageError::Io(e)
    }
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::Db(e) => write!(f, "fjall error: {e}"),
            StorageError::Io(e) => write!(f, "io error: {e}"),
            StorageError::Corrupt(msg) => write!(f, "corrupt storage: {msg}"),
        }
    }
}

impl std::error::Error for StorageError {}

pub type Result<T> = std::result::Result<T, StorageError>;

pub struct SnapshotMeta {
    pub last_included_seq: u64,
    pub last_included_term: u64,
    pub entry_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub seq: u64,
    pub term: u64,
    pub key: Vec<u8>,
    /// `None` if this entry is a delete tombstone.
    pub value: Option<Vec<u8>>,
}

/// Append-only, gap-free, per-key-compactable log on top of `fjall`.
pub struct RaftLog {
    db: Database,
    log: Keyspace,
    latest: Keyspace,
    meta: Keyspace,
    next_seq: Mutex<u64>,
}

// --- key/value encoding helpers -------------------------------------------

fn be_bytes(v: u64) -> [u8; 8] {
    v.to_be_bytes()
}

fn from_be_bytes(bytes: &[u8]) -> Result<u64> {
    let arr: [u8; 8] = bytes
        .try_into()
        .map_err(|_| StorageError::Corrupt("expected 8-byte big-endian u64"))?;
    Ok(u64::from_be_bytes(arr))
}

// Log entry encoding: [term (8 bytes BE)][tag PUT|DELETE (1 byte)][key_len (4 bytes BE)][key][value?]
fn encode_log_entry(term: u64, key: &[u8], value: Option<&[u8]>) -> Vec<u8> {
    let value_len = value.map_or(0, <[u8]>::len);
    let mut buf = Vec::with_capacity(8 + 1 + 4 + key.len() + value_len);
    buf.extend_from_slice(&term.to_be_bytes());
    buf.push(if value.is_some() { TAG_PUT } else { TAG_DELETE });
    buf.extend_from_slice(&(key.len() as u32).to_be_bytes());
    buf.extend_from_slice(key);
    if let Some(v) = value {
        buf.extend_from_slice(v);
    }
    buf
}

fn decode_log_entry(bytes: &[u8]) -> Result<(u64, Vec<u8>, Option<Vec<u8>>)> {
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
    let value = match tag {
        TAG_PUT => Some(bytes[13 + key_len..].to_vec()),
        TAG_DELETE => None,
        _ => return Err(StorageError::Corrupt("log entry has unknown tag")),
    };
    Ok((term, key, value))
}

// Latest value encoding: [seq (8 bytes BE)][tag PUT|DELETE (1 byte)][value?]
fn encode_latest_value(seq: u64, value: Option<&[u8]>) -> Vec<u8> {
    let value_len = value.map_or(0, <[u8]>::len);
    let mut buf = Vec::with_capacity(8 + 1 + value_len);
    buf.extend_from_slice(&seq.to_be_bytes());
    buf.push(if value.is_some() { TAG_PUT } else { TAG_DELETE });
    if let Some(v) = value {
        buf.extend_from_slice(v);
    }
    buf
}

fn decode_latest_value(bytes: &[u8]) -> Result<(u64, Option<Vec<u8>>)> {
    if bytes.len() < 9 {
        return Err(StorageError::Corrupt("latest value too short"));
    }
    let seq = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
    let tag = bytes[8];
    let value = match tag {
        TAG_PUT => Some(bytes[9..].to_vec()),
        TAG_DELETE => None,
        _ => return Err(StorageError::Corrupt("latest value has unknown tag")),
    };
    Ok((seq, value))
}

// --- implementation ---------------------------------------------------

impl RaftLog {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = Database::builder(path).open()?;
        let log = db.keyspace(CF_LOG, KeyspaceCreateOptions::default)?;
        let latest = db.keyspace(CF_LATEST, KeyspaceCreateOptions::default)?;
        let meta = db.keyspace(CF_META, KeyspaceCreateOptions::default)?;
        let next_seq = Self::recover_next_seq(&log, &meta)?;

        Ok(Self {
            db,
            log,
            latest,
            meta,
            next_seq: Mutex::new(next_seq),
        })
    }

    /// `next_seq` must account for both the physical log tail and the
    /// floor: after `install_snapshot`, or after `compact` empties `log`
    /// down to its physical tail, `log` can be empty even though sequence
    /// numbers up to the floor are already spoken for.
    fn recover_next_seq(log: &Keyspace, meta: &Keyspace) -> Result<u64> {
        let log_tail = match log.last_key_value() {
            Some(guard) => from_be_bytes(&guard.key()?)?,
            None => 0,
        };
        let (floor_seq, _) = Self::read_floor(meta)?;
        Ok(log_tail.max(floor_seq) + 1)
    }

    /// The seq/term of the highest entry known to have existed but no
    /// longer physically present in `log`, updated by both
    /// `install_snapshot` (to the snapshot boundary) and `compact` (to the
    /// top of whatever range it just deleted). `last_entry`/`term_at` fall
    /// back to this when `log` doesn't have the answer itself.
    fn read_floor(meta: &Keyspace) -> Result<(u64, u64)> {
        let seq = match meta.get(META_FLOOR_SEQ)? {
            Some(bytes) => from_be_bytes(&bytes)?,
            None => 0,
        };
        let term = match meta.get(META_FLOOR_TERM)? {
            Some(bytes) => from_be_bytes(&bytes)?,
            None => 0,
        };
        Ok((seq, term))
    }

    /// The `below_seq` used by the most recent `compact` call (or the
    /// snapshot watermark, if `install_snapshot` has run since): entries
    /// below this are already reduced to one surviving entry per key, so
    /// a future `compact` never needs to look at them again.
    fn read_compact_watermark(meta: &Keyspace) -> Result<u64> {
        match meta.get(META_COMPACT_SEQ)? {
            Some(bytes) => from_be_bytes(&bytes),
            None => Ok(0),
        }
    }

    /// The persisted `(current_term, voted_for)` pair Raft's safety
    /// property depends on: a node must never grant two votes in the same
    /// term, even across a restart. Defaults to `(0, None)` if never
    /// persisted (a brand new node). Loaded once at startup by
    /// `raft::Common::new`.
    pub fn term_state(&self) -> Result<(u64, Option<String>)> {
        let term = match self.meta.get(META_CURRENT_TERM)? {
            Some(bytes) => from_be_bytes(&bytes)?,
            None => 0,
        };
        let voted_for = match self.meta.get(META_VOTED_FOR)? {
            Some(bytes) => Some(
                String::from_utf8(bytes.to_vec())
                    .map_err(|_| StorageError::Corrupt("voted_for is not valid utf8"))?,
            ),
            None => None,
        };
        Ok((term, voted_for))
    }

    /// Atomically persist a new `(current_term, voted_for)` pair.
    /// `voted_for = None` clears any previously recorded vote (e.g. when
    /// advancing to a term nothing has been voted in yet) rather than
    /// leaving a stale vote from an earlier term on disk.
    pub fn persist_term_state(&self, term: u64, voted_for: Option<&str>) -> Result<()> {
        let mut batch = self.db.batch();
        batch.insert(&self.meta, META_CURRENT_TERM, be_bytes(term));
        match voted_for {
            Some(candidate) => batch.insert(&self.meta, META_VOTED_FOR, candidate.as_bytes()),
            None => batch.remove(&self.meta, META_VOTED_FOR),
        }
        batch.commit()?;
        Ok(())
    }

    /// Atomically assign the next sequence number and write the entry at
    /// the given Raft term. `value = None` records a delete tombstone for
    /// `key` rather than removing any row. Returns the assigned sequence
    /// number.
    pub fn append(&self, term: u64, key: &[u8], value: Option<&[u8]>) -> Result<u64> {
        let mut guard = self.next_seq.lock().unwrap();
        let seq = *guard;

        let mut batch = self.db.batch();
        batch.insert(&self.log, be_bytes(seq), encode_log_entry(term, key, value));
        batch.insert(&self.latest, key, encode_latest_value(seq, value));
        batch.commit()?;

        // Only advance now that the write has durably succeeded, so a
        // failure never leaves a gap or lets another append reuse `seq`.
        *guard = seq + 1;
        drop(guard);

        Ok(seq)
    }

    /// Delete every `log` entry with `seq >= new_tail_seq` and roll
    /// sequencing back to resume from there. For Raft log-matching: a
    /// follower discovering a conflicting entry truncates its divergent
    /// suffix before accepting the leader's replacement entries at the
    /// same seqs.
    ///
    /// Only ever expected to run on the *uncommitted* tail: entries below
    /// the compact/floor watermark are already committed (Leader
    /// Completeness) and should never be a truncation target. This isn't
    /// enforced here -- it's on the caller (the log-matching algorithm
    /// itself should never construct a truncation point that low).
    ///
    /// Unlike `compact`, this commits as a single atomic batch rather than
    /// chunking: a truncation that only deleted part of the divergent
    /// suffix, or fixed up `latest` for only some of the affected keys,
    /// would leave `latest` inconsistent with `log`. A follower's
    /// divergent suffix is expected to be small (bounded by how long since
    /// its last successful `AppendEntries`), so buffering it all is fine.
    pub fn truncate_from(&self, new_tail_seq: u64) -> Result<()> {
        let mut guard = self.next_seq.lock().unwrap();
        if new_tail_seq >= *guard {
            return Ok(()); // nothing at or above this seq to remove
        }

        let mut batch = self.db.batch();
        let mut affected_keys: HashSet<Vec<u8>> = HashSet::new();

        for item in self.log.range(be_bytes(new_tail_seq)..) {
            let (k, v) = item.into_inner()?;
            let (_, key, _) = decode_log_entry(&v)?;
            affected_keys.insert(key);
            batch.remove(&self.log, k);
        }

        // Resolve each affected key's most recent *surviving* entry with a
        // single reverse scan below the truncation point, rather than one
        // scan per key -- stopping as soon as every key is resolved (or
        // the scan runs out, meaning no surviving entry exists).
        let mut unresolved = affected_keys.clone();
        let mut survivors: HashMap<Vec<u8>, (u64, Option<Vec<u8>>)> = HashMap::new();
        if new_tail_seq > 0 {
            for item in self.log.range(..be_bytes(new_tail_seq)).rev() {
                if unresolved.is_empty() {
                    break;
                }
                let (k, v) = item.into_inner()?;
                let seq = from_be_bytes(&k)?;
                let (_, key, value) = decode_log_entry(&v)?;
                if unresolved.remove(&key) {
                    survivors.insert(key, (seq, value));
                }
            }
        }

        for key in &affected_keys {
            match survivors.get(key) {
                Some((seq, value)) => {
                    batch.insert(&self.latest, key, encode_latest_value(*seq, value.as_deref()));
                }
                None => batch.remove(&self.latest, key),
            }
        }

        batch.commit()?;
        *guard = new_tail_seq;
        Ok(())
    }

    /// The current state and the sequence number that produced it, if
    /// known. `None` if the key was never written, or if its most recent
    /// write was a delete that `compact` has since dropped from `latest`
    /// (an old tombstone and "never written" are indistinguishable once
    /// compaction has passed it). The inner `Option` is `None` when the
    /// key's most recent surviving entry was a delete.
    pub fn get_latest(&self, key: &[u8]) -> Result<Option<(u64, Option<Vec<u8>>)>> {
        match self.latest.get(key)? {
            Some(bytes) => Ok(Some(decode_latest_value(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Entries with `start <= seq < end`, in ascending seq order. Any seq in
    /// the range that isn't present (compacted away) is silently skipped —
    /// see the module-level note on why that's safe for log-matching.
    pub fn entries_in_range(&self, start: u64, end: u64) -> Result<Vec<LogEntry>> {
        let mut out = Vec::new();
        for item in self.log.range(be_bytes(start)..be_bytes(end)) {
            let (k, v) = item.into_inner()?;
            let seq = from_be_bytes(&k)?;
            let (term, key, value) = decode_log_entry(&v)?;
            out.push(LogEntry {
                seq,
                term,
                key,
                value,
            });
        }
        Ok(out)
    }

    /// The seq and term of the most recent log entry, falling back to the
    /// floor if no entries remain in `log` (e.g. right after
    /// `install_snapshot`, or after `compact` has deleted everything up to
    /// the physical tail). `None` only for a log that has never had an
    /// append or snapshot install.
    pub fn last_entry(&self) -> Result<Option<(u64, u64)>> {
        if let Some(guard) = self.log.last_key_value() {
            let (k, v) = guard.into_inner()?;
            let seq = from_be_bytes(&k)?;
            let (term, _, _) = decode_log_entry(&v)?;
            return Ok(Some((seq, term)));
        }
        let (floor_seq, floor_term) = Self::read_floor(&self.meta)?;
        if floor_seq > 0 {
            Ok(Some((floor_seq, floor_term)))
        } else {
            Ok(None)
        }
    }

    /// The term recorded at `seq`, if known. Consults the floor when `seq`
    /// is exactly the floor boundary and `log` has no entry there (it never
    /// does, once a snapshot or compaction has advanced past it).
    pub fn term_at(&self, seq: u64) -> Result<Option<u64>> {
        if let Some(bytes) = self.log.get(be_bytes(seq))? {
            let (term, _, _) = decode_log_entry(&bytes)?;
            return Ok(Some(term));
        }
        let (floor_seq, floor_term) = Self::read_floor(&self.meta)?;
        if floor_seq > 0 && seq == floor_seq {
            return Ok(Some(floor_term));
        }
        Ok(None)
    }

    /// Flush `batch` (replacing it with a fresh one) once it reaches
    /// `WRITE_CHUNK` pending writes, bounding a batch's memory regardless of
    /// how large the overall operation is.
    fn flush_if_full(&self, batch: &mut OwnedWriteBatch, pending: &mut usize) -> Result<()> {
        if *pending >= WRITE_CHUNK {
            std::mem::replace(batch, self.db.batch()).commit()?;
            *pending = 0;
        }
        Ok(())
    }

    /// Compact away every `log` entry with `seq < below_seq`. Also drops
    /// the `latest` tombstone for any key whose most recent write is one of
    /// the deletes being compacted away: once that's true, "deleted a
    /// while ago" and "never written" are indistinguishable, so there's
    /// nothing to gain from keeping the marker. Live values, and
    /// tombstones more recent than `below_seq`, are never touched.
    ///
    /// Resumes from the previous call's threshold (persisted in `meta`)
    /// rather than rescanning from 0 every time, so each call only ever
    /// touches entries added since the last one. Writes are issued in
    /// chunks of `WRITE_CHUNK` rather than accumulated into one giant batch.
    ///
    /// Also records the term of the entry at `below_seq - 1` (the last one
    /// visited, since the log is gap-free) as the new floor: if this call
    /// empties `log` all the way down to its physical tail, `last_entry`/
    /// `term_at` still need an answer for that boundary, the same way they
    /// do after `install_snapshot`.
    pub fn compact(&self, below_seq: u64) -> Result<()> {
        let from = Self::read_compact_watermark(&self.meta)?;
        if below_seq <= from {
            return Ok(());
        }

        let mut batch = self.db.batch();
        let mut pending = 0usize;
        let mut floor_term = None;

        for item in self.log.range(be_bytes(from)..be_bytes(below_seq)) {
            let (k, v) = item.into_inner()?;
            let seq = from_be_bytes(&k)?;
            let (term, key, value) = decode_log_entry(&v)?;
            floor_term = Some(term);

            batch.remove(&self.log, k);
            pending += 1;

            if value.is_none() {
                // Only drop `latest` if nothing has written `key` since:
                // otherwise this old tombstone has already been
                // superseded and `latest` is correctly reflecting
                // something newer.
                if let Some((latest_seq, _)) = self.get_latest(&key)?
                    && latest_seq == seq
                {
                    batch.remove(&self.latest, key);
                    pending += 1;
                }
            }

            self.flush_if_full(&mut batch, &mut pending)?;
        }

        if let Some(term) = floor_term {
            batch.insert(&self.meta, META_FLOOR_SEQ, be_bytes(below_seq - 1));
            batch.insert(&self.meta, META_FLOOR_TERM, be_bytes(term));
        }
        batch.insert(&self.meta, META_COMPACT_SEQ, be_bytes(below_seq));
        batch.commit()?;
        Ok(())
    }

    /// Stream the current (post-compaction) state to `writer` as a portable
    /// snapshot: a header (magic, version, last_included_seq,
    /// last_included_term) followed by length-prefixed (key, value)
    /// records, read until EOF. Intended for Raft's InstallSnapshot path —
    /// engine-agnostic and chunkable, unlike an `fjall`-native checkpoint.
    /// Tombstoned keys are omitted, since they carry no current value.
    /// Already bounded to O(1) extra memory beyond one record at a time:
    /// entries are written straight to `writer` as they're read from
    /// `fjall`, never accumulated.
    pub fn export_snapshot<W: Write>(&self, writer: &mut W) -> Result<SnapshotMeta> {
        let (last_included_seq, last_included_term) = match self.last_entry()? {
            Some((seq, term)) => (seq, term),
            None => (0, 0),
        };

        writer.write_all(SNAPSHOT_MAGIC)?;
        writer.write_all(&SNAPSHOT_VERSION.to_be_bytes())?;
        writer.write_all(&last_included_seq.to_be_bytes())?;
        writer.write_all(&last_included_term.to_be_bytes())?;

        let mut entry_count: u64 = 0;
        for item in self.latest.iter() {
            let (key, value_bytes) = item.into_inner()?;
            let (_, value) = decode_latest_value(&value_bytes)?;
            let Some(value) = value else {
                continue; // tombstoned: not part of current state
            };

            writer.write_all(&(key.len() as u32).to_be_bytes())?;
            writer.write_all(&key)?;
            writer.write_all(&(value.len() as u32).to_be_bytes())?;
            writer.write_all(&value)?;
            entry_count += 1;
        }

        Ok(SnapshotMeta {
            last_included_seq,
            last_included_term,
            entry_count,
        })
    }

    /// Install a snapshot produced by `export_snapshot`, replacing all
    /// current log/latest state. This is a full wipe (not the etcd-style
    /// "retain the suffix if it doesn't conflict" optimization): every
    /// existing `log` and `latest` row is dropped (each `clear()` is an
    /// O(1) keyspace-level operation), then the incoming (key, value)
    /// records repopulate `latest` at `last_included_seq`, and the snapshot
    /// watermark in `meta` is set so `last_entry`/`term_at` keep working
    /// even though `log` now has nothing at or below that point.
    /// Subsequent `append` calls continue from `last_included_seq + 1`.
    ///
    /// Bounded memory: the incoming record stream is processed and
    /// committed in chunks of `WRITE_CHUNK`, rather than buffering the whole
    /// incoming snapshot in memory first. This means the install is not one
    /// single atomic commit — a crash partway through can leave the store
    /// with only some chunks applied. That's an accepted trade-off here
    /// (installing a snapshot is a heavyweight, comparatively rare
    /// operation, and Raft's InstallSnapshot is naturally retryable — a
    /// follower that crashes mid-install simply reports needing a snapshot
    /// again).
    pub fn install_snapshot<R: Read>(&self, reader: &mut R) -> Result<SnapshotMeta> {
        let mut magic = [0u8; 8];
        reader.read_exact(&mut magic)?;
        if &magic != SNAPSHOT_MAGIC {
            return Err(StorageError::Corrupt("bad snapshot magic"));
        }
        let mut version_bytes = [0u8; 4];
        reader.read_exact(&mut version_bytes)?;
        if u32::from_be_bytes(version_bytes) != SNAPSHOT_VERSION {
            return Err(StorageError::Corrupt("unsupported snapshot version"));
        }
        let mut seq_bytes = [0u8; 8];
        reader.read_exact(&mut seq_bytes)?;
        let last_included_seq = u64::from_be_bytes(seq_bytes);
        let mut term_bytes = [0u8; 8];
        reader.read_exact(&mut term_bytes)?;
        let last_included_term = u64::from_be_bytes(term_bytes);

        // Serialize against concurrent appends for the duration of the
        // install, same discipline as `append`.
        let mut guard = self.next_seq.lock().unwrap();

        self.log.clear()?;
        self.latest.clear()?;

        // Stream incoming records straight into chunked write batches
        // instead of buffering the whole snapshot in memory.
        let mut batch = self.db.batch();
        let mut pending = 0usize;
        let mut entry_count: u64 = 0;

        loop {
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let key_len = u32::from_be_bytes(len_buf) as usize;
            let mut key = vec![0u8; key_len];
            reader.read_exact(&mut key)?;

            let mut vlen_buf = [0u8; 4];
            reader.read_exact(&mut vlen_buf)?;
            let value_len = u32::from_be_bytes(vlen_buf) as usize;
            let mut value = vec![0u8; value_len];
            reader.read_exact(&mut value)?;

            batch.insert(
                &self.latest,
                &key,
                encode_latest_value(last_included_seq, Some(&value)),
            );
            entry_count += 1;
            pending += 1;
            self.flush_if_full(&mut batch, &mut pending)?;
        }

        batch.insert(&self.meta, META_FLOOR_SEQ, be_bytes(last_included_seq));
        batch.insert(&self.meta, META_FLOOR_TERM, be_bytes(last_included_term));
        // `log` was just wiped, so compaction has nothing to do below
        // `last_included_seq` — reset the watermark to match, whatever it
        // was before.
        batch.insert(&self.meta, META_COMPACT_SEQ, be_bytes(last_included_seq));
        batch.commit()?;

        *guard = last_included_seq + 1;
        drop(guard);

        Ok(SnapshotMeta {
            last_included_seq,
            last_included_term,
            entry_count,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_temp() -> (RaftLog, std::path::PathBuf) {
        let mut path = std::env::temp_dir();
        path.push(format!("raftlog_test_{}", std::process::id()));
        path.push(format!("{:x}", rand_suffix()));
        let log = RaftLog::open(&path).expect("open");
        (log, path)
    }

    fn rand_suffix() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }

    fn cleanup(path: &std::path::Path) {
        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn append_assigns_gap_free_increasing_seqs() {
        let (log, path) = open_temp();
        let s1 = log.append(1, b"a", Some(b"1")).unwrap();
        let s2 = log.append(1, b"b", Some(b"2")).unwrap();
        let s3 = log.append(2, b"a", Some(b"3")).unwrap();
        assert_eq!((s1, s2, s3), (1, 2, 3));
        cleanup(&path);
    }

    #[test]
    fn get_latest_returns_newest_value() {
        let (log, path) = open_temp();
        log.append(1, b"k", Some(b"v1")).unwrap();
        let seq2 = log.append(1, b"k", Some(b"v2")).unwrap();
        let (seq, val) = log.get_latest(b"k").unwrap().unwrap();
        assert_eq!(seq, seq2);
        assert_eq!(val, Some(b"v2".to_vec()));
        cleanup(&path);
    }

    #[test]
    fn delete_records_tombstone_and_clears_current_value() {
        let (log, path) = open_temp();
        log.append(1, b"k", Some(b"v1")).unwrap();
        let del_seq = log.append(1, b"k", None).unwrap();

        let (seq, val) = log.get_latest(b"k").unwrap().unwrap();
        assert_eq!(seq, del_seq);
        assert_eq!(val, None);

        // The tombstone is a normal log entry like any other.
        let entries = log.entries_in_range(del_seq, del_seq + 1).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].value, None);

        // A never-written key is still indistinguishable from nothing.
        assert_eq!(log.get_latest(b"never").unwrap(), None);
        cleanup(&path);
    }

    #[test]
    fn export_snapshot_omits_tombstoned_keys() {
        let (log, path) = open_temp();
        log.append(1, b"a", Some(b"1")).unwrap();
        log.append(1, b"b", Some(b"2")).unwrap();
        log.append(1, b"a", None).unwrap();

        let mut buf = Vec::new();
        let meta = log.export_snapshot(&mut buf).unwrap();
        assert_eq!(meta.entry_count, 1); // only "b" survives

        let (target, target_path) = open_temp();
        target.install_snapshot(&mut buf.as_slice()).unwrap();
        assert_eq!(target.get_latest(b"a").unwrap(), None);
        assert_eq!(
            target.get_latest(b"b").unwrap(),
            Some((meta.last_included_seq, Some(b"2".to_vec())))
        );

        cleanup(&path);
        cleanup(&target_path);
    }

    #[test]
    fn compact_removes_entries_below_threshold_but_preserves_current_value() {
        let (log, path) = open_temp();
        log.append(1, b"k", Some(b"v1")).unwrap();
        log.append(1, b"k", Some(b"v2")).unwrap();
        let s3 = log.append(1, b"k", Some(b"v3")).unwrap();
        let above = log.append(1, b"k", Some(b"v4")).unwrap();

        log.compact(above).unwrap();

        // Everything below the threshold is gone from `log`...
        assert!(log.entries_in_range(0, above).unwrap().is_empty());
        assert_eq!(log.term_at(1).unwrap(), None);
        // ...except the floor boundary itself (`below_seq - 1`), which
        // `last_entry`/`term_at` still need an answer for even once `log`
        // has nothing left at or below it.
        assert_eq!(log.term_at(s3).unwrap(), Some(1));

        // ...but `get_latest` still works, since `latest` is untouched.
        let (seq, val) = log.get_latest(b"k").unwrap().unwrap();
        assert_eq!(seq, above);
        assert_eq!(val, Some(b"v4".to_vec()));
        cleanup(&path);
    }

    #[test]
    fn compact_to_the_tail_leaves_last_entry_and_term_at_answerable() {
        // Compacting all the way through the current tail empties `log`
        // entirely (unlike the old per-key compaction, which always left
        // at least the newest entry behind). `last_entry`/`term_at` must
        // still be able to answer for that boundary, the same way they do
        // after `install_snapshot`.
        let (log, path) = open_temp();
        log.append(1, b"a", Some(b"1")).unwrap();
        log.append(1, b"a", Some(b"2")).unwrap();
        let last = log.append(2, b"b", Some(b"x")).unwrap();
        log.compact(last + 1).unwrap();

        assert!(log.entries_in_range(0, last + 1).unwrap().is_empty());
        assert_eq!(log.last_entry().unwrap(), Some((last, 2)));
        assert_eq!(log.term_at(last).unwrap(), Some(2));

        // Sequencing continues correctly even though `log` is empty.
        let next = log.append(2, b"c", Some(b"new")).unwrap();
        assert_eq!(next, last + 1);

        cleanup(&path);
    }

    #[test]
    fn compact_resumes_from_the_previous_watermark_across_calls() {
        // Two compaction passes should each only need to touch entries
        // added since the last one — the second call must not need to
        // rescan what the first one already deleted.
        let (log, path) = open_temp();
        log.append(1, b"k", Some(b"v1")).unwrap();
        let s2 = log.append(1, b"k", Some(b"v2")).unwrap();
        log.compact(s2 + 1).unwrap();
        assert!(log.entries_in_range(0, s2 + 1).unwrap().is_empty());

        log.append(1, b"k", Some(b"v3")).unwrap();
        let s4 = log.append(1, b"k", Some(b"v4")).unwrap();
        log.compact(s4 + 1).unwrap();

        assert!(log.entries_in_range(0, s4 + 1).unwrap().is_empty());
        let (seq, val) = log.get_latest(b"k").unwrap().unwrap();
        assert_eq!(seq, s4);
        assert_eq!(val, Some(b"v4".to_vec()));

        // A second call with a threshold at or below the previous one is a
        // no-op rather than an error or a rescan.
        log.compact(s2 + 1).unwrap();
        assert_eq!(log.get_latest(b"k").unwrap().unwrap().0, s4);

        cleanup(&path);
    }

    #[test]
    fn compact_drops_the_latest_tombstone_once_the_delete_is_compacted_away() {
        let (log, path) = open_temp();
        log.append(1, b"k", Some(b"v1")).unwrap();
        let del_seq = log.append(1, b"k", None).unwrap();
        let above = log.append(1, b"other", Some(b"x")).unwrap();

        // Before compaction, the tombstone is still visible.
        assert_eq!(log.get_latest(b"k").unwrap(), Some((del_seq, None)));

        log.compact(above).unwrap();

        // Once the delete itself has been compacted away, the tombstone in
        // `latest` is dropped too — "deleted a while ago" and "never
        // written" are now indistinguishable.
        assert_eq!(log.get_latest(b"k").unwrap(), None);
        cleanup(&path);
    }

    #[test]
    fn compact_preserves_a_tombstone_still_above_the_threshold() {
        let (log, path) = open_temp();
        log.append(1, b"k", Some(b"v1")).unwrap();
        let del_seq = log.append(1, b"k", None).unwrap();

        // Compacting only up to (not including) the delete leaves it alone.
        log.compact(del_seq).unwrap();
        assert_eq!(log.get_latest(b"k").unwrap(), Some((del_seq, None)));
        cleanup(&path);
    }

    #[test]
    fn compact_does_not_drop_a_tombstone_superseded_by_a_later_write() {
        let (log, path) = open_temp();
        log.append(1, b"k", None).unwrap();
        let put_seq = log.append(1, b"k", Some(b"v2")).unwrap();
        let above = log.append(1, b"other", Some(b"x")).unwrap();

        log.compact(above).unwrap();

        // `latest` reflects the later write, not the compacted-away
        // delete, so it must be untouched.
        assert_eq!(
            log.get_latest(b"k").unwrap(),
            Some((put_seq, Some(b"v2".to_vec())))
        );
        cleanup(&path);
    }

    #[test]
    fn compact_handles_many_entries_across_chunks() {
        // Exercises the chunked-flush path in `compact` (WRITE_CHUNK is
        // 1000) with more entries below the threshold than fit in one
        // batch.
        let (log, path) = open_temp();
        let mut last_seq = 0;
        for i in 0..2500u32 {
            last_seq = log.append(1, b"hot", Some(&i.to_be_bytes())).unwrap();
        }
        log.compact(last_seq + 1).unwrap();
        assert!(log.entries_in_range(0, last_seq + 1).unwrap().is_empty());
        let (seq, _) = log.get_latest(b"hot").unwrap().unwrap();
        assert_eq!(seq, last_seq);
        cleanup(&path);
    }

    #[test]
    fn truncate_from_removes_the_suffix_and_resumes_sequencing_there() {
        let (log, path) = open_temp();
        log.append(1, b"a", Some(b"1")).unwrap();
        let keep = log.append(1, b"b", Some(b"2")).unwrap();
        log.append(1, b"c", Some(b"3")).unwrap();
        log.append(1, b"d", Some(b"4")).unwrap();

        log.truncate_from(keep + 1).unwrap();

        assert!(log.entries_in_range(keep + 1, 100).unwrap().is_empty());
        assert_eq!(log.last_entry().unwrap(), Some((keep, 1)));

        // Sequencing resumes right after the truncation point.
        let next = log.append(2, b"e", Some(b"5")).unwrap();
        assert_eq!(next, keep + 1);
        cleanup(&path);
    }

    #[test]
    fn truncate_from_reverts_latest_to_the_surviving_entry_for_each_key() {
        let (log, path) = open_temp();
        log.append(1, b"k", Some(b"v1")).unwrap();
        let cut = log.append(1, b"k", Some(b"v2")).unwrap();
        log.append(1, b"k", Some(b"v3")).unwrap(); // truncated away

        log.truncate_from(cut + 1).unwrap();

        // `latest` reverts to the last surviving write for "k"...
        assert_eq!(
            log.get_latest(b"k").unwrap(),
            Some((cut, Some(b"v2".to_vec())))
        );
        cleanup(&path);
    }

    #[test]
    fn truncate_from_clears_latest_when_a_key_has_no_surviving_entry() {
        let (log, path) = open_temp();
        let above = log.append(1, b"other", Some(b"x")).unwrap();
        log.append(1, b"only-in-suffix", Some(b"y")).unwrap();

        log.truncate_from(above + 1).unwrap();

        // "only-in-suffix" was never written below the truncation point,
        // so it must disappear from `latest` entirely, not linger.
        assert_eq!(log.get_latest(b"only-in-suffix").unwrap(), None);
        assert_eq!(
            log.get_latest(b"other").unwrap(),
            Some((above, Some(b"x".to_vec())))
        );
        cleanup(&path);
    }

    #[test]
    fn truncate_from_at_or_above_the_tail_is_a_no_op() {
        let (log, path) = open_temp();
        let last = log.append(1, b"a", Some(b"1")).unwrap();

        log.truncate_from(last + 1).unwrap();
        assert_eq!(log.last_entry().unwrap(), Some((last, 1)));
        cleanup(&path);
    }

    #[test]
    fn entries_in_range_returns_terms_in_order() {
        let (log, path) = open_temp();
        log.append(1, b"a", Some(b"1")).unwrap();
        log.append(1, b"b", Some(b"2")).unwrap();
        log.append(2, b"c", Some(b"3")).unwrap();
        log.append(2, b"d", Some(b"4")).unwrap();

        let entries = log.entries_in_range(2, 4).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].seq, 2);
        assert_eq!(entries[0].term, 1);
        assert_eq!(entries[0].key, b"b");
        assert_eq!(entries[1].seq, 3);
        assert_eq!(entries[1].term, 2);
        assert_eq!(entries[1].key, b"c");
        cleanup(&path);
    }

    #[test]
    fn last_entry_and_term_at_track_the_log_tail() {
        let (log, path) = open_temp();
        assert_eq!(log.last_entry().unwrap(), None);

        log.append(1, b"a", Some(b"1")).unwrap();
        let last_seq = log.append(3, b"b", Some(b"2")).unwrap();

        assert_eq!(log.last_entry().unwrap(), Some((last_seq, 3)));
        assert_eq!(log.term_at(1).unwrap(), Some(1));
        assert_eq!(log.term_at(last_seq).unwrap(), Some(3));
        assert_eq!(log.term_at(last_seq + 1).unwrap(), None);
        cleanup(&path);
    }

    #[test]
    fn export_snapshot_reflects_compacted_state() {
        let (log, path) = open_temp();
        log.append(1, b"a", Some(b"1")).unwrap();
        log.append(1, b"a", Some(b"2")).unwrap();
        let last = log.append(2, b"b", Some(b"x")).unwrap();
        log.compact(last + 1).unwrap();

        let mut buf = Vec::new();
        let meta = log.export_snapshot(&mut buf).unwrap();
        assert_eq!(meta.entry_count, 2);
        assert_eq!(meta.last_included_seq, last);
        assert_eq!(meta.last_included_term, 2);
        assert!(buf.starts_with(SNAPSHOT_MAGIC));
        cleanup(&path);
    }

    #[test]
    fn install_snapshot_replaces_state_and_continues_sequencing() {
        let (log, path) = open_temp();
        log.append(1, b"a", Some(b"1")).unwrap();
        log.append(1, b"a", Some(b"2")).unwrap();
        let last = log.append(2, b"b", Some(b"x")).unwrap();

        let mut buf = Vec::new();
        log.export_snapshot(&mut buf).unwrap();

        let (target, target_path) = open_temp();
        let meta = target.install_snapshot(&mut buf.as_slice()).unwrap();
        assert_eq!(meta.last_included_seq, last);
        assert_eq!(meta.last_included_term, 2);
        assert_eq!(meta.entry_count, 2);

        // State came across correctly.
        let (seq, val) = target.get_latest(b"a").unwrap().unwrap();
        assert_eq!((seq, val), (last, Some(b"2".to_vec())));
        assert_eq!(target.last_entry().unwrap(), Some((last, 2)));

        // Sequencing continues from where the snapshot left off, with no
        // gap, even though `log` has nothing before this new append.
        let next = target.append(2, b"c", Some(b"new")).unwrap();
        assert_eq!(next, last + 1);

        cleanup(&path);
        cleanup(&target_path);
    }

    #[test]
    fn install_snapshot_wipes_prior_state_on_the_target() {
        // The target already has its own unrelated history; installing a
        // snapshot must fully replace it, not merge with it.
        let (target, target_path) = open_temp();
        target.append(1, b"stale", Some(b"old")).unwrap();

        let (source, source_path) = open_temp();
        let last = source.append(1, b"fresh", Some(b"new")).unwrap();
        let mut buf = Vec::new();
        source.export_snapshot(&mut buf).unwrap();

        target.install_snapshot(&mut buf.as_slice()).unwrap();

        assert_eq!(target.get_latest(b"stale").unwrap(), None);
        assert_eq!(
            target.get_latest(b"fresh").unwrap(),
            Some((last, Some(b"new".to_vec())))
        );

        cleanup(&target_path);
        cleanup(&source_path);
    }

    #[test]
    fn next_seq_recovers_from_watermark_with_no_log_entries() {
        let (log, path) = open_temp();
        log.append(1, b"a", Some(b"1")).unwrap();
        let last = log.append(2, b"b", Some(b"2")).unwrap();

        let mut buf = Vec::new();
        log.export_snapshot(&mut buf).unwrap();

        let (target, target_path) = open_temp();
        target.install_snapshot(&mut buf.as_slice()).unwrap();
        drop(target);

        // Reopen with no further appends: log CF is empty, so next_seq
        // must come from the meta watermark alone.
        let reopened = RaftLog::open(&target_path).unwrap();
        assert_eq!(reopened.last_entry().unwrap(), Some((last, 2)));
        let next = reopened.append(2, b"c", Some(b"3")).unwrap();
        assert_eq!(next, last + 1);

        cleanup(&path);
        cleanup(&target_path);
    }

    #[test]
    fn term_state_defaults_when_never_persisted() {
        let (log, path) = open_temp();
        assert_eq!(log.term_state().unwrap(), (0, None));
        cleanup(&path);
    }

    #[test]
    fn persist_term_state_round_trips_after_reopen() {
        let (log, path) = open_temp();
        log.persist_term_state(5, Some("node-b")).unwrap();
        drop(log);

        let reopened = RaftLog::open(&path).unwrap();
        assert_eq!(
            reopened.term_state().unwrap(),
            (5, Some("node-b".to_string()))
        );
        cleanup(&path);
    }

    #[test]
    fn persist_term_state_clears_a_previous_vote() {
        let (log, path) = open_temp();
        log.persist_term_state(5, Some("node-b")).unwrap();
        log.persist_term_state(6, None).unwrap();
        assert_eq!(log.term_state().unwrap(), (6, None));
        cleanup(&path);
    }

    #[test]
    fn recovers_next_seq_after_reopen() {
        let (log, path) = open_temp();
        log.append(1, b"a", Some(b"1")).unwrap();
        log.append(1, b"b", Some(b"2")).unwrap();
        drop(log);

        let reopened = RaftLog::open(&path).unwrap();
        let seq = reopened.append(1, b"c", Some(b"3")).unwrap();
        assert_eq!(seq, 3);
        cleanup(&path);
    }
}
