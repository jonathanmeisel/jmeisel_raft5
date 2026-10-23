//! Backup files: a portable copy of the key/value state as of one seq, and
//! the restore that turns one into the state of a new cluster.
//!
//! A backup is not a Raft snapshot (`super::snapshot`). The snapshot format
//! is internal: peers on one release exchange it, and any other version is
//! refused. A backup must stay readable by later releases, so it has its own
//! magic and version, and holds only what a new cluster needs: one version
//! per key, values only, at the source seqs.
//!
//! # Format
//!
//! ```text
//! header:  magic "RAFT5BAK" | format_version u32 | as_of u64
//!          | created_unix_ms u64 | source_node_id (u32 len + bytes)
//!          | snapshot_id (u32 len + bytes, empty = current state)
//! records: key_len u32 | key | seq u64 | value_len u32 | value
//!          (ascending by key, values only, 1 <= seq <= as_of)
//! end:     u32::MAX
//! trailer: record_count u64 | sha256 of every byte before the trailer
//! ```
//!
//! All integers are big-endian. The end marker means a truncated file fails
//! as "no trailer", not as a short backup that looks valid.

use std::io::{Read, Write};

use fjall::Readable;
use sha2::{Digest, Sha256};

use super::codec::{Kind, be_bytes, encode_log_entry, row_chain, row_key, row_kind, row_seq, row_user_key};
use super::meta::{
    META_APPLIED_SEQ, META_COMPACT_SEQ, META_FLOOR_SEQ, META_FLOOR_TERM, META_RESTORE_IN_PROGRESS,
};
use super::{EntryPayload, RaftLog, ReadPoint, Result, StorageError};

/// The first bytes of every backup file.
pub const BACKUP_MAGIC: &[u8; 8] = b"RAFT5BAK";

/// The format version this build writes. Append-only: a new release must
/// still read every older version.
pub const BACKUP_FORMAT_VERSION: u32 = 1;

/// The key length that marks the end of the records.
const END_OF_RECORDS: u32 = u32::MAX;

/// The longest header string (node id, snapshot id) a restore accepts.
const MAX_HEADER_FIELD: usize = 4096;

/// The longest key or value a restore accepts. Well above any value the
/// cluster can hold, so it only stops a corrupt length from allocating
/// gigabytes.
const MAX_RECORD_FIELD: usize = 64 * 1024 * 1024;

/// Where a backup came from, for its header.
pub struct BackupSource<'a> {
    pub node_id: &'a str,
    /// Empty for a backup of the current state.
    pub snapshot_id: &'a [u8],
    pub created_unix_ms: u64,
}

/// What `export_backup` did.
#[derive(Debug, PartialEq, Eq)]
pub enum BackupExport {
    Done { as_of: u64, record_count: u64 },
    /// No checkpoint has the requested id on this node. Nothing was written.
    NoSuchCheckpoint,
}

/// Why a bootstrap (plain or from a backup) is refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootstrapRefusal {
    /// The node already has a log or a configuration: it is in a cluster.
    AlreadyInitialized,
    /// The node is in no cluster but holds data that a bootstrap would
    /// silently build on, for example the remains of an interrupted restore.
    Dirty(&'static str),
}

/// What `restore_backup` did.
#[derive(Debug, PartialEq, Eq)]
pub enum RestoreOutcome {
    Restored {
        as_of: u64,
        record_count: u64,
        source_node_id: String,
    },
    Refused(BootstrapRefusal),
    /// The backup could not be read. The store is pristine again.
    BadBackup(String),
}

/// A failure while loading a backup: the backup's fault, or storage's.
enum LoadError {
    Bad(String),
    Storage(StorageError),
}

impl From<StorageError> for LoadError {
    fn from(e: StorageError) -> Self {
        LoadError::Storage(e)
    }
}

impl From<fjall::Error> for LoadError {
    fn from(e: fjall::Error) -> Self {
        LoadError::Storage(e.into())
    }
}

/// A `Write` that hashes everything written through it.
struct HashingWriter<'a, W: Write> {
    inner: &'a mut W,
    hasher: Sha256,
}

impl<W: Write> Write for HashingWriter<'_, W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hasher.update(&buf[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// A `Read` that hashes everything read through it.
struct HashingReader<'a, R: Read> {
    inner: &'a mut R,
    hasher: Sha256,
}

impl<R: Read> Read for HashingReader<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.hasher.update(&buf[..n]);
        Ok(n)
    }
}

fn read_exact(r: &mut impl Read, buf: &mut [u8]) -> std::result::Result<(), LoadError> {
    r.read_exact(buf).map_err(|e| match e.kind() {
        std::io::ErrorKind::UnexpectedEof => {
            LoadError::Bad("the backup is truncated: it ends before its trailer".to_string())
        }
        _ => LoadError::Bad(format!("could not read the backup: {e}")),
    })
}

fn read_u32(r: &mut impl Read) -> std::result::Result<u32, LoadError> {
    let mut buf = [0u8; 4];
    read_exact(r, &mut buf)?;
    Ok(u32::from_be_bytes(buf))
}

fn read_u64(r: &mut impl Read) -> std::result::Result<u64, LoadError> {
    let mut buf = [0u8; 8];
    read_exact(r, &mut buf)?;
    Ok(u64::from_be_bytes(buf))
}

fn read_field(
    r: &mut impl Read,
    len: u32,
    max: usize,
    what: &str,
) -> std::result::Result<Vec<u8>, LoadError> {
    let len = len as usize;
    if len > max {
        return Err(LoadError::Bad(format!("the backup has a {what} of {len} bytes, more than {max}")));
    }
    let mut buf = vec![0u8; len];
    read_exact(r, &mut buf)?;
    Ok(buf)
}

fn write_field(w: &mut impl Write, bytes: &[u8]) -> std::io::Result<()> {
    w.write_all(&(bytes.len() as u32).to_be_bytes())?;
    w.write_all(bytes)
}

/// What the export walk does with one `versions` row.
enum Pick {
    /// A newer version of a key, or a row of a key already decided.
    Skip,
    /// The head of its key as of the backup's seq. `Some` if it is a value
    /// the backup includes.
    Settle { emit: Option<Vec<u8>> },
}

/// Decide about `row` from its key alone. `settled` is the chain of the key
/// already decided, and `seek` the seq to read heads at.
fn pick(
    row: &[u8],
    settled: Option<&[u8]>,
    seek: u64,
    include: &impl Fn(&[u8]) -> bool,
) -> Result<Pick> {
    if settled == Some(row_chain(row)?) || row_seq(row)? > seek {
        return Ok(Pick::Skip);
    }
    if row_kind(row)? != Kind::Value {
        return Ok(Pick::Settle { emit: None });
    }
    let key = row_user_key(row)?;
    Ok(Pick::Settle {
        emit: include(&key).then_some(key),
    })
}

impl RaftLog {
    /// Write a backup of the keys `include` accepts, as of `point`, to
    /// `writer`.
    ///
    /// - The walk runs inside one `fjall::Snapshot`, so it sees one point in
    ///   time while `apply_committed` keeps writing. For `ReadPoint::Latest`
    ///   the snapshot pins fjall's old data until the walk ends, so disk can
    ///   grow during a long backup. For a checkpoint, the versions it reads
    ///   are already pinned by the checkpoint.
    /// - Each key contributes its newest version at or before the backup's
    ///   seq, and only if that version is a value: a deleted key is absent.
    /// - A value is read only for a row that is written to the backup.
    ///
    /// Flushes `writer` at the end, which a `ChunkWriter` needs to send its
    /// last chunk.
    pub fn export_backup<W: Write>(
        &self,
        writer: &mut W,
        point: &ReadPoint,
        source: &BackupSource,
        include: impl Fn(&[u8]) -> bool,
    ) -> Result<BackupExport> {
        let snap = self.db.snapshot();
        let Some((as_of, seek)) = self.resolve_point(&snap, point)? else {
            return Ok(BackupExport::NoSuchCheckpoint);
        };

        let mut out = HashingWriter {
            inner: writer,
            hasher: Sha256::new(),
        };
        out.write_all(BACKUP_MAGIC)?;
        out.write_all(&BACKUP_FORMAT_VERSION.to_be_bytes())?;
        out.write_all(&as_of.to_be_bytes())?;
        out.write_all(&source.created_unix_ms.to_be_bytes())?;
        write_field(&mut out, source.node_id.as_bytes())?;
        write_field(&mut out, source.snapshot_id)?;

        let mut settled: Option<Vec<u8>> = None;
        let mut record_count = 0u64;
        for item in snap.iter(&self.versions) {
            // `into_inner_if` reads the value only when the predicate says so,
            // which keeps skipped versions' blob files unread. The predicate
            // cannot return errors or change state, so the same decision is
            // made again below with errors and state.
            let decided = settled.as_deref();
            let (row, value) = item.into_inner_if(|row| {
                matches!(
                    pick(row, decided, seek, &include),
                    Ok(Pick::Settle { emit: Some(_) })
                )
            })?;
            match pick(&row, decided, seek, &include)? {
                Pick::Skip => continue,
                Pick::Settle { emit } => {
                    settled = Some(row_chain(&row)?.to_vec());
                    let Some(key) = emit else { continue };
                    let Some(value) = value else {
                        return Err(StorageError::Corrupt("a value row resolved to no value"));
                    };
                    write_field(&mut out, &key)?;
                    out.write_all(&row_seq(&row)?.to_be_bytes())?;
                    write_field(&mut out, &value)?;
                    record_count += 1;
                }
            }
        }

        out.write_all(&END_OF_RECORDS.to_be_bytes())?;
        let digest = out.hasher.finalize();
        let inner = out.inner;
        inner.write_all(&record_count.to_be_bytes())?;
        inner.write_all(&digest)?;
        inner.flush()?;
        Ok(BackupExport::Done { as_of, record_count })
    }

    /// Why this store may not be bootstrapped, or `None` if it may.
    ///
    /// A store may be bootstrapped only if it is untouched: no log entry, no
    /// configuration, no version rows, and no interrupted restore. The last
    /// two matter because a bootstrap would build a cluster on top of data
    /// nobody chose to keep.
    pub fn bootstrap_refusal(&self) -> Result<Option<BootstrapRefusal>> {
        if self.meta.get(META_RESTORE_IN_PROGRESS)?.is_some() {
            return Ok(Some(BootstrapRefusal::Dirty(
                "an interrupted restore left partial data on this node; \
                 delete its database directory and start again",
            )));
        }
        if self.last_entry()?.is_some() || self.current_config()?.is_some() {
            return Ok(Some(BootstrapRefusal::AlreadyInitialized));
        }
        if self.versions.iter().next().is_some() {
            return Ok(Some(BootstrapRefusal::Dirty(
                "this node holds data but no log; \
                 delete its database directory and start again",
            )));
        }
        Ok(None)
    }

    /// Load a backup into this untouched store, and append `config` as the
    /// first log entry after it, so the node becomes a cluster that holds
    /// the backup's contents.
    ///
    /// - Records go into `versions` at their source seqs, in `WRITE_CHUNK`
    ///   batches, so memory stays bounded.
    /// - The checksum is checked before anything makes the store
    ///   bootstrapped. Success is one batch: the floor, applied and
    ///   compaction watermarks set to the backup's seq, and `config` at the
    ///   next seq, term 1 (as for a plain bootstrap).
    /// - On a bad backup or a storage error, the version rows are cleared and
    ///   the store is untouched again, so the restore can be retried.
    /// - A marker in `meta` covers a crash during the load: a store that
    ///   still has it refuses every bootstrap (see `bootstrap_refusal`).
    ///
    /// The floor's term is 0, below the configuration entry's term 1. A peer
    /// added later has nothing at or below the floor, so the leader sends it
    /// a Raft snapshot.
    pub fn restore_backup<R: Read>(&self, reader: &mut R, config: &[u8]) -> Result<RestoreOutcome> {
        let encoded_config = encode_log_entry(1, &EntryPayload::Config(config.to_vec()))?;

        // Block appends for the whole restore, as `append` does.
        let mut guard = self.next_seq.lock().unwrap();

        if let Some(refusal) = self.bootstrap_refusal()? {
            return Ok(RestoreOutcome::Refused(refusal));
        }
        self.meta.insert(META_RESTORE_IN_PROGRESS, [1u8])?;

        let loaded = self.load_backup(reader);
        let (as_of, record_count, source_node_id) = match loaded {
            Ok(header) => header,
            Err(e) => {
                self.versions.clear()?;
                self.meta.remove(META_RESTORE_IN_PROGRESS)?;
                return match e {
                    LoadError::Bad(why) => Ok(RestoreOutcome::BadBackup(why)),
                    LoadError::Storage(e) => Err(e),
                };
            }
        };

        let config_seq = as_of + 1;
        let mut batch = self.db.batch();
        batch.insert(&self.meta, META_FLOOR_SEQ, be_bytes(as_of));
        batch.insert(&self.meta, META_FLOOR_TERM, be_bytes(0));
        batch.insert(&self.meta, META_APPLIED_SEQ, be_bytes(as_of));
        batch.insert(&self.meta, META_COMPACT_SEQ, be_bytes(as_of));
        batch.remove(&self.meta, META_RESTORE_IN_PROGRESS);
        batch.insert(&self.log, be_bytes(config_seq), encoded_config);
        batch.commit()?;

        *guard = config_seq + 1;
        Ok(RestoreOutcome::Restored {
            as_of,
            record_count,
            source_node_id,
        })
    }

    /// Read and check a whole backup, writing its records into `versions`.
    /// Returns the backup's seq, its record count and its source node id.
    fn load_backup<R: Read>(&self, reader: &mut R) -> std::result::Result<(u64, u64, String), LoadError> {
        let mut r = HashingReader {
            inner: reader,
            hasher: Sha256::new(),
        };

        let mut magic = [0u8; 8];
        read_exact(&mut r, &mut magic)?;
        if &magic != BACKUP_MAGIC {
            return Err(LoadError::Bad("this is not a raft5 backup file".to_string()));
        }
        let version = read_u32(&mut r)?;
        if version != BACKUP_FORMAT_VERSION {
            return Err(LoadError::Bad(format!(
                "backup format version {version} is not one this build reads \
                 (it reads {BACKUP_FORMAT_VERSION})"
            )));
        }
        let as_of = read_u64(&mut r)?;
        let _created_unix_ms = read_u64(&mut r)?;
        let len = read_u32(&mut r)?;
        let source_node_id = read_field(&mut r, len, MAX_HEADER_FIELD, "source node id")?;
        let source_node_id = String::from_utf8_lossy(&source_node_id).into_owned();
        let len = read_u32(&mut r)?;
        let _snapshot_id = read_field(&mut r, len, MAX_HEADER_FIELD, "snapshot id")?;

        let mut batch = self.db.batch();
        let mut pending = 0usize;
        let mut record_count = 0u64;
        let mut previous: Option<Vec<u8>> = None;
        loop {
            let key_len = read_u32(&mut r)?;
            if key_len == END_OF_RECORDS {
                break;
            }
            let key = read_field(&mut r, key_len, MAX_RECORD_FIELD, "key")?;
            let seq = read_u64(&mut r)?;
            let len = read_u32(&mut r)?;
            let value = read_field(&mut r, len, MAX_RECORD_FIELD, "value")?;

            if seq == 0 || seq > as_of {
                return Err(LoadError::Bad(format!(
                    "the backup has a record at seq {seq}, outside 1..={as_of}"
                )));
            }
            if previous.as_ref().is_some_and(|p| *p >= key) {
                return Err(LoadError::Bad(
                    "the backup's keys are not in strictly ascending order".to_string(),
                ));
            }

            batch.insert(&self.versions, row_key(&key, seq, Kind::Value), value);
            previous = Some(key);
            record_count += 1;
            pending += 1;
            self.flush_if_full(&mut batch, &mut pending)?;
        }
        batch.commit()?;

        let digest = r.hasher.finalize();
        let reader = r.inner;
        let declared_count = read_u64(reader)?;
        let mut declared_digest = [0u8; 32];
        read_exact(reader, &mut declared_digest)?;
        if declared_count != record_count {
            return Err(LoadError::Bad(format!(
                "the backup's trailer says {declared_count} records, but it holds {record_count}"
            )));
        }
        if declared_digest[..] != digest[..] {
            return Err(LoadError::Bad("the backup's checksum does not match its contents".to_string()));
        }
        let mut extra = [0u8; 1];
        match reader.read(&mut extra) {
            Ok(0) => {}
            Ok(_) => return Err(LoadError::Bad("the backup has data after its trailer".to_string())),
            Err(e) => return Err(LoadError::Bad(format!("could not read the backup: {e}"))),
        }

        Ok((as_of, record_count, source_node_id))
    }
}

#[cfg(test)]
mod tests;
