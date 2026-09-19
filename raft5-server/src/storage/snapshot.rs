//! Exporting and installing the portable snapshot format.
//!
//! Deliberately engine-agnostic rather than an `fjall`-native checkpoint:
//! a snapshot is a header followed by length-prefixed `(key, value)`
//! records, so it can be chunked over the wire by Raft's InstallSnapshot
//! and read back by a node that isn't byte-identical in its storage
//! layout. Both directions stream -- neither buffers the whole snapshot
//! in memory (see each function's doc for what that costs on install).

use std::io::{Read, Write};

use super::codec::{be_bytes, decode_latest_value, encode_latest_value};
use super::meta::{
    META_APPLIED_SEQ, META_BASE_CONFIG, META_COMPACT_SEQ, META_FLOOR_SEQ, META_FLOOR_TERM,
    read_applied_seq,
};
use super::{RaftLog, Result, StorageError};

pub(super) const SNAPSHOT_MAGIC: &[u8; 8] = b"RLOGSNAP";

/// v2 added the trailing `[config_len][config_bytes]` header field. There's
/// no v1 compatibility path -- nothing has shipped, and a snapshot without
/// a configuration would leave its recipient unable to find its peers.
pub(super) const SNAPSHOT_VERSION: u32 = 2;

pub struct SnapshotMeta {
    pub last_included_seq: u64,
    pub last_included_term: u64,
    pub entry_count: u64,
}

impl RaftLog {
    /// Stream the current (post-compaction) state to `writer` as a portable
    /// snapshot: a header (magic, version, last_included_seq,
    /// last_included_term, cluster configuration) followed by
    /// length-prefixed (key, value) records, read until EOF. Intended for Raft's InstallSnapshot path —
    /// engine-agnostic and chunkable, unlike an `fjall`-native checkpoint.
    /// Every row in `latest` is current state -- there's no tombstone case
    /// to skip (see the module doc). Already bounded to O(1) extra memory
    /// beyond one record at a time: entries are written straight to
    /// `writer` as they're read from `fjall`, never accumulated.
    ///
    /// `last_included_seq`/`last_included_term` describe `applied_seq`,
    /// *not* the log's raw physical tail: `latest` (what's actually being
    /// exported here) only ever reflects state through `applied_seq` --
    /// there can be appended-but-uncommitted entries past that point that
    /// aren't in `latest` at all yet, so claiming the tail as this
    /// snapshot's boundary would describe state the export doesn't
    /// actually contain.
    ///
    /// The header also carries the configuration in force as of
    /// `last_included_seq`. Installing a snapshot wipes `log`, taking the
    /// recipient's `Config` entries with it, so without this the receiver
    /// would come out of the install unable to name a single peer.
    /// Uncommitted configurations past `last_included_seq` are
    /// deliberately excluded: the snapshot describes state through that
    /// point and nothing beyond it, and the leader will replicate any
    /// later config entry as an ordinary entry right after.
    pub fn export_snapshot<W: Write>(&self, writer: &mut W) -> Result<SnapshotMeta> {
        let (last_included_seq, last_included_term) = self.snapshot_boundary()?;
        let config = self.config_at(last_included_seq)?.unwrap_or_default();

        writer.write_all(SNAPSHOT_MAGIC)?;
        writer.write_all(&SNAPSHOT_VERSION.to_be_bytes())?;
        writer.write_all(&last_included_seq.to_be_bytes())?;
        writer.write_all(&last_included_term.to_be_bytes())?;
        writer.write_all(&(config.len() as u32).to_be_bytes())?;
        writer.write_all(&config)?;

        // `Keyspace::iter` pins a snapshot nonce, so this walks one
        // point in time even while `apply_committed` is writing: an
        // entry's mutations can't be caught half-applied here.
        let mut entry_count: u64 = 0;
        for item in self.latest.iter() {
            let (key, value_bytes) = item.into_inner()?;
            let (_, value) = decode_latest_value(&value_bytes)?;

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

    /// The seq and term an export taken right now would be bounded by:
    /// what `apply_committed` has reached, and the term at that point.
    ///
    /// Split out so a caller streaming an export can describe it to its
    /// peer *before* producing it -- the Raft-level header has to go out
    /// first, and waiting for the export to finish in order to fill it
    /// in is precisely the buffer-it-all behaviour streaming removes.
    ///
    /// An export started moments later can therefore contain slightly
    /// *more* than this describes, if `apply_committed` advanced in
    /// between. That is safe and was already true (the `latest`
    /// iteration is pinned when it starts, not when the header is
    /// written): the boundary only ever understates the body, so the
    /// receiver sets its watermarks low and the leader replicates the
    /// difference as ordinary entries.
    pub fn snapshot_boundary(&self) -> Result<(u64, u64)> {
        let last_included_seq = read_applied_seq(&self.meta)?;
        let last_included_term = if last_included_seq == 0 {
            0
        } else {
            self.term_at(last_included_seq)?.unwrap_or(0)
        };
        Ok((last_included_seq, last_included_term))
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
        let mut config_len_bytes = [0u8; 4];
        reader.read_exact(&mut config_len_bytes)?;
        let mut config = vec![0u8; u32::from_be_bytes(config_len_bytes) as usize];
        reader.read_exact(&mut config)?;

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
                encode_latest_value(last_included_seq, &value)?,
            );
            entry_count += 1;
            pending += 1;
            self.flush_if_full(&mut batch, &mut pending)?;
        }

        batch.insert(&self.meta, META_FLOOR_SEQ, be_bytes(last_included_seq));
        batch.insert(&self.meta, META_FLOOR_TERM, be_bytes(last_included_term));
        // `log` was just wiped, so whatever `Config` entry this node was
        // running on is gone -- the sender's configuration in the header
        // becomes the new fallback for `current_config`.
        if !config.is_empty() {
            batch.insert(&self.meta, META_BASE_CONFIG, config);
        }
        // `log` was just wiped, so compaction has nothing to do below
        // `last_included_seq` — reset the watermark to match, whatever it
        // was before.
        batch.insert(&self.meta, META_COMPACT_SEQ, be_bytes(last_included_seq));
        // `latest` was just fully repopulated from the snapshot, which by
        // construction only ever represents committed state (the source
        // node's own `export_snapshot` reads the same, commit-only
        // `latest`) -- so it's already "applied" through this point.
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
