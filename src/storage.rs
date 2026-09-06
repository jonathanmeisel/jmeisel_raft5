//! Raft log storage backed by RocksDB.
//!
//! Layout (four column families):
//! - `log`:    seq (u64 BE)                -> encoded (term, key, value|tombstone) entry
//! - `latest`: key                          -> encoded (seq, value|tombstone) current state
//! - `index`:  encoded (key_len, key, seq)  -> empty marker, for history-by-key lookups
//! - `meta`:   small set of fixed keys, currently the snapshot watermark
//!             (`last_included_seq`, `last_included_term`) recorded by
//!             `install_snapshot`.
//!
//! A single append writes `log`, `latest` and `index` in one `WriteBatch`, so
//! they never diverge. Sequence numbers are assigned under a mutex held for
//! the full duration of the write, so a failed write never leaves a gap or
//! is double-assigned.
//!
//! Deletes are appended entries with no value (a tombstone), not row
//! removal: `append(term, key, None)` records that the key was deleted at
//! that seq, same as any other entry — it appears in `seqs_for_key` history
//! and participates in `compact` normally. `latest` keeps a tombstone marker
//! too, so `get_latest` can distinguish "never written" (`None`) from
//! "deleted, most recently at seq N" (`Some((n, None))`). `export_snapshot`
//! omits tombstoned keys, since a snapshot represents only current state.
//!
//! Note on compaction and term lookups: `compact` removes entries per-key,
//! not as a full index-prefix cut, so a term_at/entries_in_range query can
//! find gaps at seq values below a compaction threshold even when they're
//! not below any snapshot boundary. This is safe for Raft's log-matching
//! purposes: entries below a node's own compaction/snapshot point are
//! already committed and can never be a source of conflict again (Leader
//! Completeness), so callers should never need term information that far
//! back — comparisons only ever happen in the live tail.
//!
//! Note on memory: `compact` and `install_snapshot` operate on RocksDB
//! iterators, which pin a consistent point-in-time view as of iterator
//! creation — so it's safe to interleave reading from one with issuing
//! writes/deletes against the live `DB` from the same loop. Both use this to
//! process the CFs in bounded chunks rather than buffering an entire range
//! into memory before acting on it.

use std::collections::HashSet;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Mutex;

use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, Direction, IteratorMode, Options, WriteBatch, DB};

const CF_LOG: &str = "log";
const CF_LATEST: &str = "latest";
const CF_INDEX: &str = "index";
const CF_META: &str = "meta";

const META_SNAP_SEQ: &[u8] = b"snap_last_seq";
const META_SNAP_TERM: &[u8] = b"snap_last_term";

const SNAPSHOT_MAGIC: &[u8; 8] = b"RLOGSNAP";
const SNAPSHOT_VERSION: u32 = 1;

/// Max pending writes buffered in a `WriteBatch` before it's flushed, used
/// everywhere we process a potentially large range in bounded chunks.
const WRITE_CHUNK: usize = 1000;

const TAG_PUT: u8 = 0;
const TAG_DELETE: u8 = 1;

#[derive(Debug)]
pub enum StorageError {
    Db(rocksdb::Error),
    Io(std::io::Error),
    Corrupt(&'static str),
}

impl From<rocksdb::Error> for StorageError {
    fn from(e: rocksdb::Error) -> Self {
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
            StorageError::Db(e) => write!(f, "rocksdb error: {e}"),
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

/// Append-only, gap-free, per-key-compactable log on top of RocksDB.
pub struct RaftLog {
    db: DB,
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

/// Prefix shared by every index entry for `key`: `[key_len][key]`.
/// Length-prefixing the key keeps this prefix unambiguous, so a scan for
/// key "ab" can never match entries belonging to key "abc".
fn index_key_prefix(key: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + key.len());
    buf.extend_from_slice(&(key.len() as u32).to_be_bytes());
    buf.extend_from_slice(key);
    buf
}

fn index_key(key: &[u8], seq: u64) -> Vec<u8> {
    let mut buf = index_key_prefix(key);
    buf.extend_from_slice(&seq.to_be_bytes());
    buf
}

fn decode_index_key_seq(bytes: &[u8], prefix_len: usize) -> Result<u64> {
    if bytes.len() != prefix_len + 8 {
        return Err(StorageError::Corrupt("index key has unexpected length"));
    }
    from_be_bytes(&bytes[prefix_len..])
}

// --- implementation ---------------------------------------------------

impl RaftLog {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        let cfs = vec![
            ColumnFamilyDescriptor::new(CF_LOG, Options::default()),
            ColumnFamilyDescriptor::new(CF_LATEST, Options::default()),
            ColumnFamilyDescriptor::new(CF_INDEX, Options::default()),
            ColumnFamilyDescriptor::new(CF_META, Options::default()),
        ];

        let db = DB::open_cf_descriptors(&db_opts, path, cfs)?;
        let next_seq = Self::recover_next_seq(&db)?;

        Ok(Self {
            db,
            next_seq: Mutex::new(next_seq),
        })
    }

    /// `next_seq` must account for both the physical log tail and any
    /// snapshot watermark: after `install_snapshot` with no further
    /// appends, `log` can be empty even though sequence numbers up to
    /// `last_included_seq` are already spoken for.
    fn recover_next_seq(db: &DB) -> Result<u64> {
        let cf_log = Self::cf(db, CF_LOG)?;
        let mut iter = db.iterator_cf(cf_log, IteratorMode::End);
        let log_tail = match iter.next() {
            Some(Ok((k, _))) => from_be_bytes(&k)?,
            Some(Err(e)) => return Err(e.into()),
            None => 0,
        };
        let (watermark_seq, _) = Self::read_watermark(db)?;
        Ok(log_tail.max(watermark_seq) + 1)
    }

    fn read_watermark(db: &DB) -> Result<(u64, u64)> {
        let cf_meta = Self::cf(db, CF_META)?;
        let seq = match db.get_cf(cf_meta, META_SNAP_SEQ)? {
            Some(bytes) => from_be_bytes(&bytes)?,
            None => 0,
        };
        let term = match db.get_cf(cf_meta, META_SNAP_TERM)? {
            Some(bytes) => from_be_bytes(&bytes)?,
            None => 0,
        };
        Ok((seq, term))
    }

    fn cf<'a>(db: &'a DB, name: &str) -> Result<&'a ColumnFamily> {
        db.cf_handle(name)
            .ok_or(StorageError::Corrupt("missing expected column family"))
    }

    /// Atomically assign the next sequence number and write the entry at
    /// the given Raft term. `value = None` records a delete tombstone for
    /// `key` rather than removing any row. Returns the assigned sequence
    /// number.
    pub fn append(&self, term: u64, key: &[u8], value: Option<&[u8]>) -> Result<u64> {
        let cf_log = Self::cf(&self.db, CF_LOG)?;
        let cf_latest = Self::cf(&self.db, CF_LATEST)?;
        let cf_index = Self::cf(&self.db, CF_INDEX)?;

        let mut guard = self.next_seq.lock().unwrap();
        let seq = *guard;

        let mut batch = WriteBatch::default();
        batch.put_cf(cf_log, be_bytes(seq), encode_log_entry(term, key, value));
        batch.put_cf(cf_latest, key, encode_latest_value(seq, value));
        batch.put_cf(cf_index, index_key(key, seq), []);

        self.db.write(batch)?;

        // Only advance now that the write has durably succeeded, so a
        // failure never leaves a gap or lets another append reuse `seq`.
        *guard = seq + 1;
        drop(guard);

        Ok(seq)
    }

    /// The current state and the sequence number that produced it, if the
    /// key has ever been written and hasn't been compacted away entirely
    /// (compaction never removes the latest entry for a key, so this is
    /// only `None` if the key was never written). The inner `Option` is
    /// `None` when the key's most recent entry was a delete.
    pub fn get_latest(&self, key: &[u8]) -> Result<Option<(u64, Option<Vec<u8>>)>> {
        let cf_latest = Self::cf(&self.db, CF_LATEST)?;
        match self.db.get_cf(cf_latest, key)? {
            Some(bytes) => Ok(Some(decode_latest_value(&bytes)?)),
            None => Ok(None),
        }
    }

    /// All sequence numbers at which `key` currently has a retained entry
    /// (put or delete), in ascending order. Sequence numbers compacted away
    /// won't appear.
    pub fn seqs_for_key(&self, key: &[u8]) -> Result<Vec<u64>> {
        let cf_index = Self::cf(&self.db, CF_INDEX)?;
        let prefix = index_key_prefix(key);

        let iter = self
            .db
            .iterator_cf(cf_index, IteratorMode::From(&prefix, Direction::Forward));

        let mut seqs = Vec::new();
        for item in iter {
            let (k, _) = item?;
            if !k.starts_with(&prefix) {
                break;
            }
            seqs.push(decode_index_key_seq(&k, prefix.len())?);
        }
        Ok(seqs)
    }

    /// Entries with `start <= seq < end`, in ascending seq order. Any seq in
    /// the range that isn't present (compacted away) is silently skipped —
    /// see the module-level note on why that's safe for log-matching.
    pub fn entries_in_range(&self, start: u64, end: u64) -> Result<Vec<LogEntry>> {
        let cf_log = Self::cf(&self.db, CF_LOG)?;
        let from = be_bytes(start);
        let iter = self
            .db
            .iterator_cf(cf_log, IteratorMode::From(&from, Direction::Forward));

        let mut out = Vec::new();
        for item in iter {
            let (k, v) = item?;
            let seq = from_be_bytes(&k)?;
            if seq >= end {
                break;
            }
            let (term, key, value) = decode_log_entry(&v)?;
            out.push(LogEntry { seq, term, key, value });
        }
        Ok(out)
    }

    /// The seq and term of the most recent log entry, falling back to the
    /// snapshot watermark if no entries remain in `log` (e.g. right after
    /// `install_snapshot`, or an entirely fresh log with a snapshot but no
    /// entries above it). `None` only for a log that has never had an
    /// append or snapshot install.
    pub fn last_entry(&self) -> Result<Option<(u64, u64)>> {
        let cf_log = Self::cf(&self.db, CF_LOG)?;
        let mut iter = self.db.iterator_cf(cf_log, IteratorMode::End);
        if let Some(item) = iter.next() {
            let (k, v) = item?;
            let seq = from_be_bytes(&k)?;
            let (term, _, _) = decode_log_entry(&v)?;
            return Ok(Some((seq, term)));
        }
        let (watermark_seq, watermark_term) = Self::read_watermark(&self.db)?;
        if watermark_seq > 0 {
            Ok(Some((watermark_seq, watermark_term)))
        } else {
            Ok(None)
        }
    }

    /// The term recorded at `seq`, if known. Consults the snapshot
    /// watermark when `seq` is exactly the snapshot boundary and `log` has
    /// no entry there (it never does, by construction of
    /// `install_snapshot`).
    pub fn term_at(&self, seq: u64) -> Result<Option<u64>> {
        let cf_log = Self::cf(&self.db, CF_LOG)?;
        if let Some(bytes) = self.db.get_cf(cf_log, be_bytes(seq))? {
            let (term, _, _) = decode_log_entry(&bytes)?;
            return Ok(Some(term));
        }
        let (watermark_seq, watermark_term) = Self::read_watermark(&self.db)?;
        if watermark_seq > 0 && seq == watermark_seq {
            return Ok(Some(watermark_term));
        }
        Ok(None)
    }

    /// Flush `batch` (replacing it with a fresh one) once it reaches
    /// `WRITE_CHUNK` pending writes, bounding a batch's memory regardless of
    /// how large the overall operation is.
    fn flush_if_full(&self, batch: &mut WriteBatch, pending: &mut usize) -> Result<()> {
        if *pending >= WRITE_CHUNK {
            self.db.write(std::mem::take(batch))?;
            *pending = 0;
        }
        Ok(())
    }

    /// Compact entries with `seq < below_seq`: for each key, keep only its
    /// highest surviving sequence number below the threshold (whether a put
    /// or a delete tombstone) and drop the rest (from both `log` and
    /// `index`). Entries at or above `below_seq` are untouched. `latest` is
    /// never touched, since it's already exactly the compacted view.
    ///
    /// Processes the range in descending order so the decision "is this the
    /// newest entry for this key below the threshold" only requires
    /// remembering which *distinct keys* have already been seen — not every
    /// entry — bounding peak memory to O(distinct keys below the
    /// threshold), which is the minimum information the task requires.
    /// Superseded entries are deleted in chunks of `WRITE_CHUNK` rather than
    /// accumulated into one giant batch.
    pub fn compact(&self, below_seq: u64) -> Result<()> {
        if below_seq == 0 {
            return Ok(());
        }
        let cf_log = Self::cf(&self.db, CF_LOG)?;
        let cf_index = Self::cf(&self.db, CF_INDEX)?;

        let start = be_bytes(below_seq - 1);
        let iter = self
            .db
            .iterator_cf(cf_log, IteratorMode::From(&start, Direction::Reverse));

        let mut seen: HashSet<Vec<u8>> = HashSet::new();
        let mut batch = WriteBatch::default();
        let mut pending = 0usize;

        for item in iter {
            let (k, v) = item?;
            let seq = from_be_bytes(&k)?;
            debug_assert!(seq < below_seq);
            let (_, entry_key, _) = decode_log_entry(&v)?;

            if seen.contains(&entry_key) {
                batch.delete_cf(cf_log, be_bytes(seq));
                batch.delete_cf(cf_index, index_key(&entry_key, seq));
                pending += 1;
                self.flush_if_full(&mut batch, &mut pending)?;
            } else {
                seen.insert(entry_key);
            }
        }

        if pending > 0 {
            self.db.write(batch)?;
        }
        Ok(())
    }

    /// Stream the current (post-compaction) state to `writer` as a portable
    /// snapshot: a header (magic, version, last_included_seq,
    /// last_included_term) followed by length-prefixed (key, value)
    /// records, read until EOF. Intended for Raft's InstallSnapshot path —
    /// engine-agnostic and chunkable, unlike a RocksDB-native checkpoint.
    /// Tombstoned keys are omitted, since they carry no current value.
    /// Already bounded to O(1) extra memory beyond one record at a time:
    /// entries are written straight to `writer` as they're read from
    /// RocksDB, never accumulated.
    pub fn export_snapshot<W: Write>(&self, writer: &mut W) -> Result<SnapshotMeta> {
        let cf_latest = Self::cf(&self.db, CF_LATEST)?;
        let (last_included_seq, last_included_term) = match self.last_entry()? {
            Some((seq, term)) => (seq, term),
            None => (0, 0),
        };

        writer.write_all(SNAPSHOT_MAGIC)?;
        writer.write_all(&SNAPSHOT_VERSION.to_be_bytes())?;
        writer.write_all(&last_included_seq.to_be_bytes())?;
        writer.write_all(&last_included_term.to_be_bytes())?;

        let mut entry_count: u64 = 0;
        for item in self.db.iterator_cf(cf_latest, IteratorMode::Start) {
            let (key, value_bytes) = item?;
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

    /// Delete every row in `cf`, in chunks, without ever holding more than
    /// `WRITE_CHUNK` keys in memory at once.
    fn wipe_cf(&self, cf: &ColumnFamily) -> Result<()> {
        let mut batch = WriteBatch::default();
        let mut pending = 0usize;
        for item in self.db.iterator_cf(cf, IteratorMode::Start) {
            let (k, _) = item?;
            batch.delete_cf(cf, k);
            pending += 1;
            self.flush_if_full(&mut batch, &mut pending)?;
        }
        if pending > 0 {
            self.db.write(batch)?;
        }
        Ok(())
    }

    /// Install a snapshot produced by `export_snapshot`, replacing all
    /// current log/index/latest state. This is a full wipe (not the
    /// etcd-style "retain the suffix if it doesn't conflict" optimization):
    /// every existing `log`, `latest` and `index` row is dropped, then the
    /// incoming (key, value) records repopulate `latest`/`index` at
    /// `last_included_seq`, and the snapshot watermark in `meta` is set so
    /// `last_entry`/`term_at` keep working even though `log` now has
    /// nothing at or below that point. Subsequent `append` calls continue
    /// from `last_included_seq + 1`.
    ///
    /// Bounded memory: the wipe and the incoming record stream are both
    /// processed and committed in chunks of `WRITE_CHUNK`, rather than
    /// buffering the existing keyspace or the whole incoming snapshot in
    /// memory first. This means the install is not one single atomic
    /// commit — a crash partway through can leave the store with only some
    /// chunks applied. That's an accepted trade-off here (installing a
    /// snapshot is a heavyweight, comparatively rare operation, and Raft's
    /// InstallSnapshot is naturally retryable — a follower that crashes
    /// mid-install simply reports needing a snapshot again).
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

        let cf_log = Self::cf(&self.db, CF_LOG)?;
        let cf_latest = Self::cf(&self.db, CF_LATEST)?;
        let cf_index = Self::cf(&self.db, CF_INDEX)?;
        let cf_meta = Self::cf(&self.db, CF_META)?;

        // Serialize against concurrent appends for the duration of the
        // install, same discipline as `append`.
        let mut guard = self.next_seq.lock().unwrap();

        self.wipe_cf(cf_log)?;
        self.wipe_cf(cf_index)?;
        self.wipe_cf(cf_latest)?;

        // Stream incoming records straight into chunked write batches
        // instead of buffering the whole snapshot in memory.
        let mut batch = WriteBatch::default();
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

            batch.put_cf(
                cf_latest,
                &key,
                encode_latest_value(last_included_seq, Some(&value)),
            );
            batch.put_cf(cf_index, index_key(&key, last_included_seq), []);
            entry_count += 1;
            pending += 1;
            self.flush_if_full(&mut batch, &mut pending)?;
        }

        batch.put_cf(cf_meta, META_SNAP_SEQ, be_bytes(last_included_seq));
        batch.put_cf(cf_meta, META_SNAP_TERM, be_bytes(last_included_term));
        self.db.write(batch)?;

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

        // The delete itself is part of history.
        assert!(log.seqs_for_key(b"k").unwrap().contains(&del_seq));

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
    fn seqs_for_key_tracks_full_history_until_compacted() {
        let (log, path) = open_temp();
        let s1 = log.append(1, b"k", Some(b"v1")).unwrap();
        let s2 = log.append(1, b"k", Some(b"v2")).unwrap();
        log.append(1, b"other", Some(b"x")).unwrap();
        let s3 = log.append(1, b"k", Some(b"v3")).unwrap();

        assert_eq!(log.seqs_for_key(b"k").unwrap(), vec![s1, s2, s3]);

        // Compacting below s3 + 1 (i.e. including s3 itself in the
        // compacted range) should collapse "k"'s history to just s3, its
        // newest entry. "other" has only ever had one entry, so it's
        // untouched.
        log.compact(s3 + 1).unwrap();
        assert_eq!(log.seqs_for_key(b"k").unwrap(), vec![s3]);
        assert_eq!(log.seqs_for_key(b"other").unwrap().len(), 1);

        cleanup(&path);
    }

    #[test]
    fn compact_keeps_latest_per_key_and_preserves_current_value() {
        let (log, path) = open_temp();
        log.append(1, b"k", Some(b"v1")).unwrap();
        log.append(1, b"k", Some(b"v2")).unwrap();
        let s3 = log.append(1, b"k", Some(b"v3")).unwrap();
        let above = log.append(1, b"k", Some(b"v4")).unwrap();

        log.compact(above).unwrap();

        assert_eq!(log.seqs_for_key(b"k").unwrap(), vec![s3, above]);
        let (seq, val) = log.get_latest(b"k").unwrap().unwrap();
        assert_eq!(seq, above);
        assert_eq!(val, Some(b"v4".to_vec()));
        cleanup(&path);
    }

    #[test]
    fn compact_keeps_latest_tombstone_for_a_deleted_key() {
        let (log, path) = open_temp();
        log.append(1, b"k", Some(b"v1")).unwrap();
        let del_seq = log.append(1, b"k", None).unwrap();
        let above = log.append(1, b"other", Some(b"x")).unwrap();

        log.compact(above).unwrap();

        assert_eq!(log.seqs_for_key(b"k").unwrap(), vec![del_seq]);
        assert_eq!(log.get_latest(b"k").unwrap(), Some((del_seq, None)));
        cleanup(&path);
    }

    #[test]
    fn compact_handles_many_entries_for_the_same_key_across_chunks() {
        // Exercises the chunked-flush path in `compact` (WRITE_CHUNK is
        // 1000): every entry below the threshold is for the same key, so
        // all but the last must be deleted.
        let (log, path) = open_temp();
        let mut last_seq = 0;
        for i in 0..2500u32 {
            last_seq = log.append(1, b"hot", Some(&i.to_be_bytes())).unwrap();
        }
        log.compact(last_seq + 1).unwrap();
        assert_eq!(log.seqs_for_key(b"hot").unwrap(), vec![last_seq]);
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
        assert_eq!(target.seqs_for_key(b"a").unwrap(), vec![last]);
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
