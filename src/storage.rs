//! Raft log storage backed by `fjall`.
//!
//! Layout (three keyspaces):
//! - `log`:    seq (u64 BE)                -> encoded (term, kind, key, value) entry
//! - `latest`: key                          -> encoded (seq, value) *committed* current state
//! - `meta`:   small set of fixed keys: the floor (`floor_seq`,
//!   `floor_term`) — the seq/term of the highest entry no longer physically
//!   in `log`, updated by both `install_snapshot` and `compact` — the
//!   compaction watermark (`compact_last_seq`, recorded by `compact` alone,
//!   for resuming an incremental compaction) — and the applied watermark
//!   (`applied_seq`, recorded by `apply_committed` alone, for the same
//!   reason) — plus `base_config`, the last cluster configuration to fall
//!   out of `log` via `compact` or `install_snapshot` (see `config_at`).
//!
//! Not every log entry is key/value data: `EntryKind` also covers a
//! leader's term-opening no-op and the cluster configuration. Those
//! occupy a seq and take part in log matching and commitment like
//! anything else, but `apply_committed` skips them, so they never reach
//! `latest`.
//!
//! `append` only ever writes to `log`; `apply_committed` is the *only*
//! thing that ever writes to `latest`, and it's driven by Raft's own commit
//! index (`raft::Common::commit_tx`), not by appends. This separation
//! matters: an appended-but-uncommitted entry can still be rolled back
//! (`truncate_from`) if a leader change happens before it commits, so
//! serving it out of `latest` (e.g. for a client read) before that's
//! settled would risk exposing a write that never durably happened.
//! Sequence numbers are assigned under a mutex held for the full duration
//! of an `append`/`truncate_from`/`install_snapshot` write, so a failed
//! write never leaves a gap or is double-assigned.
//!
//! Because `latest` is only ever driven by confirmed, permanent history, a
//! delete simply *removes* the key from `latest` (`apply_committed`) rather
//! than leaving a tombstone there the way `log` does: there's no "deleted a
//! while ago vs. never written" ambiguity left to preserve, since `latest`
//! was never ahead of what's actually final in the first place.
//! `export_snapshot` therefore never needs to skip anything -- every row in
//! `latest` is already current state.
//!
//! Because `compact` physically removes `log` entries, it must never run
//! ahead of `apply_committed` for the same range -- otherwise an entry
//! could be deleted from `log` before ever reaching `latest`, losing that
//! key's data outright. `compact` enforces this itself (clamping its own
//! `below_seq` to whatever `apply_committed` has reached), and
//! `raft::spawn_storage_maintenance_task` additionally always calls
//! `apply_committed` immediately before `compact` in the same task, so the
//! clamp is normally a no-op rather than something callers need to reason
//! about.
//!
//! Deletes are still appended entries with no value (a tombstone) in `log`
//! itself, not row removal: `append(term, key, None)` records that the key
//! was deleted at that seq, same as any other entry, and participates in
//! `compact` normally.
//!
//! Note on compaction and term lookups: `compact` unconditionally deletes
//! every `log` entry below its threshold, so a term_at/entries_in_range
//! query can find gaps there even when they're not below any snapshot
//! boundary. This is safe for Raft's log-matching purposes: entries below a
//! node's own compaction/snapshot point are already committed and can never
//! be a source of conflict again (Leader Completeness), so callers should
//! never need term information that far back — comparisons only ever happen
//! in the live tail.
//!
//! Note on memory: `compact`, `apply_committed`, and `install_snapshot`
//! operate on fjall iterators, which pin a consistent point-in-time view as
//! of iterator creation — so it's safe to interleave reading from one with
//! issuing writes/deletes against the live `Database` from the same loop.
//! All three use this to process the keyspaces in bounded chunks rather
//! than buffering an entire range into memory before acting on it.

use std::path::Path;
use std::sync::Mutex;

use fjall::{Database, Keyspace, KeyspaceCreateOptions, OwnedWriteBatch};

mod codec;
mod meta;
mod snapshot;

use codec::{
    be_bytes, decode_latest_value, decode_log_entry, encode_latest_value, encode_log_entry,
    from_be_bytes,
};
use meta::{
    META_APPLIED_SEQ, META_BASE_CONFIG, META_COMPACT_SEQ, META_CURRENT_TERM, META_FLOOR_SEQ,
    META_FLOOR_TERM, META_VOTED_FOR, read_applied_seq, read_compact_watermark, read_floor,
};

pub use snapshot::SnapshotMeta;

const CF_LOG: &str = "log";
const CF_LATEST: &str = "latest";
const CF_META: &str = "meta";

/// Max pending writes buffered in a `WriteBatch` before it's flushed, used
/// everywhere we process a potentially large range in bounded chunks.
const WRITE_CHUNK: usize = 1000;

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

/// What a log entry does. Only `Put` and `Delete` carry key/value state
/// machine data; the rest occupy a sequence number -- so they participate
/// in log matching and commitment like anything else -- but are skipped by
/// `apply_committed` and so never reach `latest`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    /// A write: `key` and `value` both set.
    Put,
    /// A delete tombstone: `key` set, `value` unset.
    Delete,
    /// A new leader's term-opening no-op: neither `key` nor `value` set.
    /// Exists purely so a leader can commit an entry from its *own* term
    /// before advancing the commit index over anything inherited from a
    /// previous one (Raft §5.4.2, Figure 8).
    NoOp,
    /// A cluster configuration: `value` holds the encoded membership,
    /// `key` unset. Unlike every other kind, this one takes effect when
    /// it is *appended* rather than when it commits -- see
    /// `current_config`.
    Config,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub seq: u64,
    pub term: u64,
    pub kind: EntryKind,
    /// Empty unless `kind` is `Put` or `Delete`.
    pub key: Vec<u8>,
    /// `Some` only when `kind` is `Put`.
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

// Log entry encoding: [term (8 bytes BE)][tag (1 byte)][key_len (4 bytes BE)][key][value?]

// Latest value encoding: [seq (8 bytes BE)][value] -- always a real value:
// `latest` never stores a delete marker (see the module doc), so unlike
// `log`'s entry encoding there's no tag byte to distinguish.

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
        let (floor_seq, _) = read_floor(meta)?;
        Ok(log_tail.max(floor_seq) + 1)
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
    ///
    /// Only ever touches `log` -- an appended entry isn't durable in the
    /// Raft sense yet (it can still be rolled back by `truncate_from` if a
    /// leader change happens before it commits), so it must not be
    /// visible via `get_latest` until `apply_committed` confirms it. See
    /// the module doc.
    pub fn append(&self, term: u64, key: &[u8], value: Option<&[u8]>) -> Result<u64> {
        let kind = if value.is_some() {
            EntryKind::Put
        } else {
            EntryKind::Delete
        };
        self.append_entry(term, kind, key, value)
    }

    /// Append a leader's term-opening no-op (see `EntryKind::NoOp`).
    pub fn append_noop(&self, term: u64) -> Result<u64> {
        self.append_entry(term, EntryKind::NoOp, &[], None)
    }

    /// Append a new cluster configuration (see `EntryKind::Config`).
    /// `config` is opaque here -- storage neither parses nor validates
    /// it, it only guarantees the bytes come back out of `current_config`
    /// once this entry is the newest `Config` in the log.
    pub fn append_config(&self, term: u64, config: &[u8]) -> Result<u64> {
        self.append_entry(term, EntryKind::Config, &[], Some(config))
    }

    /// `append`, but with the entry's kind given explicitly rather than
    /// inferred from `value`. A follower replaying entries from a leader
    /// must use this: inferring would flatten every replicated non-state
    /// entry (a `NoOp`, say) into a `Put` or `Delete`, so the follower's
    /// copy of the log would no longer be byte-identical to the leader's.
    pub fn append_entry(
        &self,
        term: u64,
        kind: EntryKind,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<u64> {
        let mut guard = self.next_seq.lock().unwrap();
        let seq = *guard;

        let mut batch = self.db.batch();
        batch.insert(
            &self.log,
            be_bytes(seq),
            encode_log_entry(term, kind, key, value),
        );
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
    /// the compact/floor watermark (or, now, `applied_seq`) are already
    /// committed (Leader Completeness) and should never be a truncation
    /// target. This isn't enforced here -- it's on the caller (the
    /// log-matching algorithm itself should never construct a truncation
    /// point that low).
    ///
    /// A pure `log`-keyspace operation: `latest` only ever reflects
    /// *committed* entries (see the module doc and `apply_committed`), and
    /// this never touches anything committed, so there's nothing in
    /// `latest` that could need fixing up here.
    pub fn truncate_from(&self, new_tail_seq: u64) -> Result<()> {
        let mut guard = self.next_seq.lock().unwrap();
        if new_tail_seq >= *guard {
            return Ok(()); // nothing at or above this seq to remove
        }

        let mut batch = self.db.batch();
        for item in self.log.range(be_bytes(new_tail_seq)..) {
            let (k, _v) = item.into_inner()?;
            batch.remove(&self.log, k);
        }

        batch.commit()?;
        *guard = new_tail_seq;
        Ok(())
    }

    /// The current *committed* value for `key` and the seq it was written
    /// at, if it currently has one. `None` if the key was never written,
    /// or if its most recent committed write was a delete -- the two are
    /// indistinguishable, and always have been, since `latest` never
    /// stores a delete marker (see the module doc). Never reflects an
    /// appended-but-not-yet-committed write; see `apply_committed`.
    pub fn get_latest(&self, key: &[u8]) -> Result<Option<(u64, Vec<u8>)>> {
        match self.latest.get(key)? {
            Some(bytes) => Ok(Some(decode_latest_value(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Applies every committed log entry not yet reflected in `latest`, up
    /// through and including `up_to_seq`, so `latest` always represents
    /// state that's permanent -- never a write that could still be rolled
    /// back by a leader change (`truncate_from`) before it commits. See
    /// the module doc.
    ///
    /// Entries are applied in ascending seq order, so a later write to the
    /// same key always wins; a delete removes the key from `latest`
    /// entirely rather than leaving a tombstone, since a *committed*
    /// delete is permanent -- there's nothing left for anything to ever
    /// need to distinguish it from "never written".
    ///
    /// Resumes from the previously persisted watermark rather than
    /// rescanning from 0 every time, same discipline as `compact`; a
    /// no-op if `up_to_seq` doesn't advance past it.
    pub fn apply_committed(&self, up_to_seq: u64) -> Result<()> {
        let applied = read_applied_seq(&self.meta)?;
        if up_to_seq <= applied {
            return Ok(());
        }

        let mut batch = self.db.batch();
        let mut pending = 0usize;

        for item in self
            .log
            .range(be_bytes(applied + 1)..be_bytes(up_to_seq + 1))
        {
            let (k, v) = item.into_inner()?;
            let seq = from_be_bytes(&k)?;
            let entry = decode_log_entry(&v)?;
            match entry.kind {
                EntryKind::Put => batch.insert(
                    &self.latest,
                    &entry.key,
                    encode_latest_value(seq, &entry.value.unwrap_or_default()),
                ),
                EntryKind::Delete => batch.remove(&self.latest, &entry.key),
                // Not state machine data -- these still consume a seq and
                // move the applied watermark, they just have no effect
                // here. A `Config` in particular is deliberately *not*
                // mirrored into `latest`: it takes effect on append, so
                // `current_config` reads it straight out of `log`, where
                // it is visible before it commits.
                EntryKind::NoOp | EntryKind::Config => continue,
            }
            pending += 1;
            self.flush_if_full(&mut batch, &mut pending)?;
        }

        batch.insert(&self.meta, META_APPLIED_SEQ, be_bytes(up_to_seq));
        batch.commit()?;
        Ok(())
    }

    /// Entries with `start <= seq < end`, in ascending seq order. Any seq in
    /// the range that isn't present (compacted away) is silently skipped —
    /// see the module-level note on why that's safe for log-matching.
    pub fn entries_in_range(&self, start: u64, end: u64) -> Result<Vec<LogEntry>> {
        let mut out = Vec::new();
        for item in self.log.range(be_bytes(start)..be_bytes(end)) {
            let (k, v) = item.into_inner()?;
            let seq = from_be_bytes(&k)?;
            let entry = decode_log_entry(&v)?;
            out.push(LogEntry {
                seq,
                term: entry.term,
                kind: entry.kind,
                key: entry.key,
                value: entry.value,
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
            let term = decode_log_entry(&v)?.term;
            return Ok(Some((seq, term)));
        }
        let (floor_seq, floor_term) = read_floor(&self.meta)?;
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
            let term = decode_log_entry(&bytes)?.term;
            return Ok(Some(term));
        }
        let (floor_seq, floor_term) = read_floor(&self.meta)?;
        if floor_seq > 0 && seq == floor_seq {
            return Ok(Some(floor_term));
        }
        Ok(None)
    }

    /// The cluster configuration in force as of `as_of_seq`: the newest
    /// `Config` entry at or below it, or -- if no such entry survives in
    /// `log` -- whatever `compact`/`install_snapshot` last promoted into
    /// `meta`. `None` only for a node that has never seen a
    /// configuration at all (i.e. one that hasn't been bootstrapped).
    ///
    /// Deliberately keyed off the *log*, not `latest`: a configuration
    /// takes effect the moment it is appended, before it commits, so it
    /// has to be readable before `apply_committed` would ever have seen
    /// it. (Raft §4.1: a server always uses the latest configuration in
    /// its log, committed or not. Waiting for commitment instead would
    /// deadlock a cluster the moment it changed membership, since
    /// committing the new configuration requires a quorum *of* the new
    /// configuration.)
    ///
    /// Scans backwards from `as_of_seq`, so in the common case it stops
    /// at the first entry it looks at.
    pub fn config_at(&self, as_of_seq: u64) -> Result<Option<Vec<u8>>> {
        for item in self.log.range(..=be_bytes(as_of_seq)).rev() {
            let (_k, v) = item.into_inner()?;
            let entry = decode_log_entry(&v)?;
            if entry.kind == EntryKind::Config {
                return Ok(Some(entry.value.unwrap_or_default()));
            }
        }
        self.base_config()
    }

    /// `config_at` for the log's tail -- the configuration this node is
    /// actually operating under right now.
    pub fn current_config(&self) -> Result<Option<Vec<u8>>> {
        self.config_at(u64::MAX)
    }

    /// The last configuration compacted or snapshotted out of `log`.
    fn base_config(&self) -> Result<Option<Vec<u8>>> {
        Ok(self.meta.get(META_BASE_CONFIG)?.map(|b| b.to_vec()))
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

    /// Compact away every `log` entry with `seq < below_seq`. `below_seq`
    /// is first clamped to `apply_committed`'s watermark: `compact`
    /// physically removes `log` rows, so it must never remove one that
    /// `apply_committed` hasn't copied into `latest` yet, or that key's
    /// data would disappear outright rather than surviving in `latest`
    /// the way compaction is supposed to guarantee. In normal operation
    /// this clamp is a no-op -- see the module doc and
    /// `raft::spawn_storage_maintenance_task`, which always applies
    /// before compacting -- it's here as a second line of defense, not
    /// the primary mechanism.
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
        // `apply_committed(applied)` has already copied everything through
        // (and including) `applied` into `latest`, so it's safe to remove
        // up to (and including) `applied` too -- i.e. `below_seq` may be
        // as high as `applied + 1` (this function's `seq < below_seq`
        // convention), just not any higher.
        let applied = read_applied_seq(&self.meta)?;
        let below_seq = below_seq.min(applied + 1);
        let from = read_compact_watermark(&self.meta)?;
        if below_seq <= from {
            return Ok(());
        }

        let mut batch = self.db.batch();
        let mut pending = 0usize;
        let mut floor_term = None;
        let mut dropped_config = None;

        for item in self.log.range(be_bytes(from)..be_bytes(below_seq)) {
            let (k, v) = item.into_inner()?;
            let entry = decode_log_entry(&v)?;
            floor_term = Some(entry.term);
            if entry.kind == EntryKind::Config {
                // About to delete the entry holding this configuration --
                // preserve it in `meta` so `current_config` can still
                // answer once nothing in `log` carries it. Ascending
                // order, so the last one seen is the newest.
                dropped_config = Some(entry.value.unwrap_or_default());
            }

            batch.remove(&self.log, k);
            pending += 1;
            self.flush_if_full(&mut batch, &mut pending)?;
        }

        if let Some(term) = floor_term {
            batch.insert(&self.meta, META_FLOOR_SEQ, be_bytes(below_seq - 1));
            batch.insert(&self.meta, META_FLOOR_TERM, be_bytes(term));
        }
        if let Some(config) = dropped_config {
            batch.insert(&self.meta, META_BASE_CONFIG, config);
        }
        batch.insert(&self.meta, META_COMPACT_SEQ, be_bytes(below_seq));
        batch.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
