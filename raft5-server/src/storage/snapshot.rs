//! Export and install of the portable snapshot format.
//!
//! This is Raft's InstallSnapshot: a full state transfer to a follower
//! that is behind the compaction floor. It is not the client-visible
//! snapshot feature, which lives in `super::checkpoint` and is called a
//! checkpoint everywhere below the RPC boundary.
//!
//! Engine-agnostic on purpose, not an `fjall`-native checkpoint: a
//! snapshot is a header followed by length-prefixed version records, so it
//! can be chunked over the wire and read back by a node whose storage
//! layout is not byte-identical. Both directions stream; neither buffers
//! the whole snapshot in memory (see each function's doc for what that
//! costs on install).

use std::io::{Read, Write};

use super::codec::{Kind, be_bytes, row_key, row_kind, row_seq, row_user_key};
use super::meta::{
    META_APPLIED_SEQ, META_BASE_CONFIG, META_COMPACT_SEQ, META_FLOOR_SEQ, META_FLOOR_TERM,
    read_applied_seq,
};
use super::{RaftLog, Result, StorageError};

pub(super) const SNAPSHOT_MAGIC: &[u8; 8] = b"RLOGSNAP";

/// The snapshot format version. Each record carries one version of a key,
/// `[key][seq][kind][value]`, because `versions` keeps many versions: a
/// recipient that got one flattened value per key could not serve any
/// checkpoint read, and would fail silently with an empty result, not an
/// error. The header ends with the cluster configuration.
///
/// A snapshot of any other version is refused: a half-understood snapshot
/// is worse than a refused one.
pub(super) const SNAPSHOT_VERSION: u32 = 3;

pub struct SnapshotMeta {
    pub last_included_seq: u64,
    pub last_included_term: u64,
    pub entry_count: u64,
}

impl RaftLog {
    /// Stream the current (post-compaction) state to `writer` as a portable
    /// snapshot: a header (magic, version, last_included_seq,
    /// last_included_term, cluster configuration), then length-prefixed
    /// `[key][seq][kind][value]` records until EOF. For Raft's
    /// InstallSnapshot: engine-agnostic and chunkable, unlike an
    /// `fjall`-native checkpoint. Uses O(1) extra memory beyond one record:
    /// records go straight to `writer` as they are read, never collected.
    ///
    /// **Every kept version is sent, tombstones included.** A tombstone is not
    /// absence here: only it can tell the recipient that a key did not exist
    /// as of a checkpoint taken before the delete. Without it, that checkpoint
    /// would report a value the sender knows was deleted. The checkpoint
    /// records are normal rows under `\0snap/`, so they travel with everything
    /// they pin, and sender and recipient agree by construction.
    ///
    /// So an export is O(kept versions), not O(live keys). The two are equal
    /// in the steady state with no checkpoints and a recent `gc_versions`
    /// pass, and the export is at most `(live checkpoints + 1) x live keys`
    /// plus kept tombstones.
    ///
    /// `last_included_seq` and `last_included_term` describe `applied_seq`,
    /// not the log's physical tail: `versions` (what is exported) shows state
    /// only through `applied_seq`. There can be appended but uncommitted
    /// entries past it that are not in `versions` yet, so naming the tail as
    /// the boundary would describe state the export does not contain.
    ///
    /// The header also carries the configuration in force as of
    /// `last_included_seq`. An install wipes `log`, including the recipient's
    /// `Config` entries, so without this the recipient would end the install
    /// unable to name a single peer. Uncommitted configurations past
    /// `last_included_seq` are left out on purpose: the snapshot describes
    /// state through that point only, and the leader replicates any later
    /// configuration entry as a normal entry right after.
    pub fn export_snapshot<W: Write>(&self, writer: &mut W) -> Result<SnapshotMeta> {
        let (last_included_seq, last_included_term) = self.snapshot_boundary()?;
        let config = self.config_at(last_included_seq)?.unwrap_or_default();

        writer.write_all(SNAPSHOT_MAGIC)?;
        writer.write_all(&SNAPSHOT_VERSION.to_be_bytes())?;
        writer.write_all(&last_included_seq.to_be_bytes())?;
        writer.write_all(&last_included_term.to_be_bytes())?;
        writer.write_all(&(config.len() as u32).to_be_bytes())?;
        writer.write_all(&config)?;

        // `Keyspace::iter` pins a snapshot nonce, so this walks one point in time
        // even while `apply_committed` writes: it cannot see an entry half
        // applied.
        let mut entry_count: u64 = 0;
        for item in self.versions.iter() {
            let (row, value) = item.into_inner()?;
            let key = row_user_key(&row)?;
            let seq = row_seq(&row)?;
            let kind = row_kind(&row)?;

            writer.write_all(&(key.len() as u32).to_be_bytes())?;
            writer.write_all(&key)?;
            writer.write_all(&seq.to_be_bytes())?;
            writer.write_all(&[kind as u8])?;
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

    /// The seq and term that bound an export taken now: what
    /// `apply_committed` has reached, and the term at that point.
    ///
    /// Separate, so a caller that streams an export can describe it to its
    /// peer before producing it: the Raft header must go first, and waiting
    /// for the export to finish to fill it in is the buffer-everything behavior
    /// that streaming avoids.
    ///
    /// An export started a moment later can contain slightly more than this
    /// describes, if `apply_committed` advanced in between. That is safe: the
    /// `versions` iteration is pinned when it starts, not when the header is
    /// written, and the boundary only ever understates the body. The receiver
    /// sets its watermarks low, and the leader replicates the difference as
    /// normal entries.
    pub fn snapshot_boundary(&self) -> Result<(u64, u64)> {
        let last_included_seq = read_applied_seq(&self.meta)?;
        let last_included_term = if last_included_seq == 0 {
            0
        } else {
            self.term_at(last_included_seq)?.unwrap_or(0)
        };
        Ok((last_included_seq, last_included_term))
    }

    /// Install a snapshot made by `export_snapshot`, replacing all current log
    /// and version state.
    ///
    /// A full wipe (not the etcd-style "keep the suffix if it does not
    /// conflict" optimization):
    /// - every `log` and `versions` row is dropped (each `clear()` is an O(1)
    ///   keyspace operation),
    /// - the incoming records refill `versions`, **each at its own seq**,
    /// - the snapshot watermark in `meta` is set, so `last_entry` and `term_at`
    ///   keep working although `log` has nothing at or below that point.
    ///
    /// Later `append` calls continue from `last_included_seq + 1`.
    ///
    /// Restoring each row at its own seq, not stamping all at
    /// `last_included_seq`, keeps checkpoints working across an install. A
    /// checkpoint record's seq is its row's seq (see `super::checkpoint`), so
    /// re-stamping would make every checkpoint claim a point it was never
    /// taken at, and answer with wrong data instead of refusing.
    ///
    /// Bounded memory: the incoming records are processed and committed in
    /// chunks of `WRITE_CHUNK`, not buffered whole first. So the install is not
    /// one atomic commit: a crash partway can leave only some chunks applied.
    /// We accept that: an install is heavy and rare, and Raft's InstallSnapshot
    /// can be retried (a follower that crashes mid-install just reports that
    /// it needs a snapshot again).
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
        let mut config_len_bytes = [0u8; 4];
        reader.read_exact(&mut config_len_bytes)?;
        let mut config = vec![0u8; u32::from_be_bytes(config_len_bytes) as usize];
        reader.read_exact(&mut config)?;

        // Block concurrent appends for the whole install, as `append` does.
        let mut guard = self.next_seq.lock().unwrap();

        self.log.clear()?;
        self.versions.clear()?;
        // The old store's GC position describes a keyspace that no longer exists.
        let mut cursor = self.db.batch();
        cursor.remove(&self.meta, super::META_GC_CURSOR);
        cursor.commit()?;

        // Stream incoming records straight into chunked write batches, instead of
        // buffering the whole snapshot in memory.
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

            let mut seq_buf = [0u8; 8];
            reader.read_exact(&mut seq_buf)?;
            let seq = u64::from_be_bytes(seq_buf);

            let mut kind_buf = [0u8; 1];
            reader.read_exact(&mut kind_buf)?;
            let kind = match kind_buf[0] {
                0 => Kind::Tombstone,
                1 => Kind::Value,
                _ => return Err(StorageError::Corrupt("unknown version row kind")),
            };

            let mut vlen_buf = [0u8; 4];
            reader.read_exact(&mut vlen_buf)?;
            let value_len = u32::from_be_bytes(vlen_buf) as usize;
            let mut value = vec![0u8; value_len];
            reader.read_exact(&mut value)?;

            // At its own seq, not the boundary: see the doc above.
            batch.insert(&self.versions, row_key(&key, seq, kind), value);
            entry_count += 1;
            pending += 1;
            self.flush_if_full(&mut batch, &mut pending)?;
        }

        batch.insert(&self.meta, META_FLOOR_SEQ, be_bytes(last_included_seq));
        batch.insert(&self.meta, META_FLOOR_TERM, be_bytes(last_included_term));
        // `log` was just wiped, so this node's `Config` entry is gone. The
        // sender's configuration from the header becomes the fallback for
        // `current_config`.
        if !config.is_empty() {
            batch.insert(&self.meta, META_BASE_CONFIG, config);
        }
        // `log` was just wiped, so compaction has nothing to do below
        // `last_included_seq`. Reset the watermark to match.
        batch.insert(&self.meta, META_COMPACT_SEQ, be_bytes(last_included_seq));
        // `versions` was just refilled from the snapshot, which only ever holds
        // committed state (the sender's `export_snapshot` reads the same
        // commit-only `versions`), so it is already applied through this point.
        batch.insert(&self.meta, META_APPLIED_SEQ, be_bytes(last_included_seq));
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
