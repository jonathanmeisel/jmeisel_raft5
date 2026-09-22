//! Raft log storage backed by `fjall`.
//!
//! Layout (three keyspaces):
//! - `log`:      seq (u64 BE)  -> encoded (term, payload) entry
//! - `versions`: one row per *version* of a key -- see below
//! - `meta`:   small set of fixed keys: the floor (`floor_seq`,
//!   `floor_term`) — the seq/term of the highest entry no longer physically
//!   in `log`, updated by both `install_snapshot` and `compact` — the
//!   compaction watermark (`compact_last_seq`, recorded by `compact` alone,
//!   for resuming an incremental compaction) — the applied watermark
//!   (`applied_seq`, recorded by `apply_committed` alone, for the same
//!   reason) — the version-GC cursor (`gc_cursor`, see `gc_versions`) —
//!   plus `base_config`, the last cluster configuration to fall
//!   out of `log` via `compact` or `install_snapshot` (see `config_at`).
//!
//! # Versions
//!
//! `versions` is multi-version: a write does not replace what a key held,
//! it *adds* a row, and a delete adds a tombstone row rather than removing
//! anything. A row key is
//! `escape(user key) || 00 00 || !seq || kind` (`codec::row_key`), which
//! sorts by user key and then newest-seq-first, so every version of one
//! key is a contiguous run with the current value at its head.
//!
//! That is what makes a *checkpoint* -- a client-visible, named,
//! point-in-time read view (`checkpoint.rs`) -- possible: reading "as of
//! seq S" is one seek into a key's run, and costs the same whether the key
//! has one version or a thousand. `get_latest` is literally
//! `get_as_of(key, u64::MAX)`, so there is no separate current-value path
//! to keep in step.
//!
//! Everything describing a row lives in its *key*, never its value, which
//! is why `get_version`, `scan_latest` and `gc_versions` can work entirely
//! from `Guard::key()` and never touch the blob files that this keyspace's
//! key-value separation puts large values in.
//!
//! The cost is that an overwritten version stays until something collects
//! it. `gc_versions` is that something, and it is not optional: without it
//! this keyspace grows without bound on any workload that overwrites, whether
//! or not a checkpoint is ever created.
//!
//! Not every log entry is key/value data: `EntryPayload` also covers a
//! leader's term-opening no-op and the cluster configuration. Those
//! occupy a seq and take part in log matching and commitment like
//! anything else, but `apply_committed` skips them, so they never reach
//! `versions`.
//!
//! A `Write` payload carries *any number* of mutations, and they apply to
//! `versions` together: one entry, one seq, one batch. That is what makes a
//! multi-key transaction possible -- see `apply_committed`.
//!
//! `append` only ever writes to `log`; `apply_committed` is the *only*
//! thing that ever writes to `versions`, and it's driven by Raft's own commit
//! index (`raft::Common::commit_tx`), not by appends. This separation
//! matters: an appended-but-uncommitted entry can still be rolled back
//! (`truncate_from`) if a leader change happens before it commits, so
//! serving it out of `versions` (e.g. for a client read) before that's
//! settled would risk exposing a write that never durably happened.
//! Sequence numbers are assigned under a mutex held for the full duration
//! of an `append`/`truncate_from`/`install_snapshot` write, so a failed
//! write never leaves a gap or is double-assigned.
//!
//! A delete writes a *tombstone* row (`apply_committed`) rather than
//! removing anything. At the head of a key's run a tombstone reads exactly
//! as "never written" does -- `get_as_of` answers `None` for both, and no
//! caller can tell them apart -- but further down the run it is the only
//! thing that can say "this key did not exist as of S" to a checkpoint
//! taken before the delete. `gc_versions` drops a head tombstone once
//! nothing older survives, which is what makes a deleted key's rows
//! actually disappear on a store with no checkpoints.
//!
//! `export_snapshot` therefore *does* have to carry tombstones: one that a
//! checkpoint selects is state, not absence, and a recipient without it
//! would answer that checkpoint's reads with a value the sender knows was
//! deleted.
//!
//! Because `compact` physically removes `log` entries, it must never run
//! ahead of `apply_committed` for the same range -- otherwise an entry
//! could be deleted from `log` before ever reaching `versions`, losing that
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
//! Note on visibility, since a multi-key entry invites the question: a
//! `fjall` batch is atomic in that all of it lands or none of it does,
//! but *not* in that readers see it all at once. `Batch::commit` takes
//! its sequence number before it walks the batch inserting into
//! memtables, and a point read runs at `SeqNo::MAX`, so a reader on
//! another thread can catch a batch halfway through.
//!
//! That is fine here, and no barrier is needed. `get_latest` reads a
//! single key, which has no "half" to see. `export_snapshot` and
//! `scan_latest` walk many, but `Keyspace::iter`/`range` open a snapshot
//! nonce and read at that instant, so both are already pinned to one
//! point in time.
//!
//! What this does mean is that two *separate* reads straddling a commit
//! see two different points in time -- read `a`, then read `b`, and the
//! entry that wrote both may have landed in between. That is not a torn
//! batch, it is two reads at two times, and no storage-level lock can fix
//! it. A caller that needs several keys as of one instant takes read
//! locks on them, which is what the transactions in `grpc::KvService` are
//! for.
//!
//! Note on memory: `compact`, `apply_committed`, and `install_snapshot`
//! operate on fjall iterators, which pin a consistent point-in-time view as
//! of iterator creation — so it's safe to interleave reading from one with
//! issuing writes/deletes against the live `Database` from the same loop.
//! All three use this to process the keyspaces in bounded chunks rather
//! than buffering an entire range into memory before acting on it.

use std::path::Path;
use std::sync::Mutex;

use fjall::{Database, Keyspace, KeyspaceCreateOptions, KvSeparationOptions, OwnedWriteBatch};
use serde::{Deserialize, Serialize};

mod checkpoint;
mod codec;
mod gc;
mod meta;
mod snapshot;

use codec::{
    Kind, be_bytes, chain_end, chain_start, decode_log_entry, encode_log_entry, from_be_bytes,
    row_key, row_seq, row_user_key, seek_as_of,
};
use meta::{
    META_APPLIED_SEQ, META_BASE_CONFIG, META_COMPACT_SEQ, META_CURRENT_TERM, META_FLOOR_SEQ,
    META_FLOOR_TERM, META_GC_CURSOR, META_VOTED_FOR, read_applied_seq, read_compact_watermark,
    read_floor,
};

pub use checkpoint::{Checkpoint, MAX_LIVE_CHECKPOINTS, checkpoint_key, encode_record};
pub use gc::GcStats;
pub use snapshot::SnapshotMeta;

const CF_LOG: &str = "log";
const CF_VERSIONS: &str = "versions";
const CF_META: &str = "meta";

/// Max pending writes buffered in a `WriteBatch` before it's flushed, used
/// everywhere we process a potentially large range in bounded chunks.
const WRITE_CHUNK: usize = 1000;

#[derive(Debug)]
pub enum StorageError {
    Db(fjall::Error),
    Io(std::io::Error),
    Corrupt(&'static str),
    /// A row would not decode, or would not encode. Decoding is where
    /// this is actually interesting: it is how a corrupt, truncated or
    /// foreign row is refused rather than half-read. Bounds checking used
    /// to be this module's own job and is now the decoder's.
    Codec(postcard::Error),
}

impl From<postcard::Error> for StorageError {
    fn from(e: postcard::Error) -> Self {
        StorageError::Codec(e)
    }
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
            StorageError::Codec(e) => write!(f, "corrupt storage: {e}"),
        }
    }
}

impl std::error::Error for StorageError {}

pub type Result<T> = std::result::Result<T, StorageError>;

/// `writes` reduced to at most one mutation per key, the last for each
/// winning, in their original order.
///
/// **One entry may contribute at most one row per key**, and that is a
/// requirement of the row encoding rather than a tidiness. Two mutations
/// for one key used to collapse on their own, because both produced the
/// same `latest` row key and the second overwrote the first. They no
/// longer do: a value and a tombstone at one seq are two *different* row
/// keys, and the tombstone sorts first (`Kind::Tombstone` is 0), so a
/// delete-then-write within one entry would leave the write buried under
/// its own delete and the key would read as absent.
///
/// `txn::Locks` already dedups by key, so a well-formed entry from this
/// cluster never contains a duplicate; this is for a hand-built
/// `append_writes` and for a replicated entry from a peer that did not.
fn last_write_per_key(writes: Vec<Mutation>) -> Vec<Mutation> {
    use std::collections::{HashMap, HashSet};

    // The index of the last mutation for each key. Built from borrows,
    // which is why it is scoped: `writes` is consumed below.
    let keep: HashSet<usize> = {
        let mut last: HashMap<&[u8], usize> = HashMap::with_capacity(writes.len());
        for (i, m) in writes.iter().enumerate() {
            last.insert(&m.key, i);
        }
        // No duplicates is the overwhelmingly common case, and it needs
        // no filtering pass at all.
        if last.len() == writes.len() {
            return writes;
        }
        last.into_values().collect()
    };

    writes
        .into_iter()
        .enumerate()
        .filter_map(|(i, m)| keep.contains(&i).then_some(m))
        .collect()
}

/// One key's change within a `Write` entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mutation {
    // `serde_bytes`, on every byte-string field in this module, and not
    // optional. Serde's stock `Vec<u8>` impl is a *sequence of u8*, so
    // it makes one serializer call per byte: correct, compact under
    // postcard, and about 50 MB/s. `serde_bytes` makes it one
    // `serialize_bytes` call and a memcpy instead. The difference does
    // not show up on small values and dominates completely on large
    // ones -- a few megabytes of transaction was spending entire
    // CPU-seconds here.
    #[serde(with = "serde_bytes")]
    pub key: Vec<u8>,
    /// `None` is a delete: a tombstone in `log`, and a tombstone row in
    /// `versions` when it applies.
    #[serde(with = "serde_bytes")]
    pub value: Option<Vec<u8>>,
}

impl Mutation {
    pub fn new(key: impl Into<Vec<u8>>, value: Option<impl Into<Vec<u8>>>) -> Self {
        Self {
            key: key.into(),
            value: value.map(Into::into),
        }
    }
}

/// What a log entry does. Only `Write` carries key/value state machine
/// data; the rest occupy a sequence number -- so they participate in log
/// matching and commitment like anything else -- but are skipped by
/// `apply_committed` and so never reach `latest`.
///
/// This is an enum with its data inside it rather than a kind tag beside
/// an always-present key and value, because the two are not independent:
/// there is no such thing as a `NoOp` with a key, and a `Write` without
/// mutations is not a write. Saying so in the type means no caller has to
/// remember it.
///
/// **The variant order is the on-disk format.** Postcard writes an enum's
/// discriminant by position, so variants may be appended but never
/// reordered or removed -- see `codec`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryPayload {
    /// One or more mutations that apply to `latest` as a single unit.
    /// A transaction is exactly this: every key it wrote, in one entry, at
    /// one seq.
    Write(Vec<Mutation>),
    /// A new leader's term-opening no-op. Exists purely so a leader can
    /// commit an entry from its *own* term before advancing the commit
    /// index over anything inherited from a previous one (Raft §5.4.2,
    /// Figure 8).
    NoOp,
    /// A cluster configuration, encoded. Unlike every other payload, this
    /// one takes effect when it is *appended* rather than when it
    /// commits -- see `current_config`.
    Config(#[serde(with = "serde_bytes")] Vec<u8>),
}

impl EntryPayload {
    /// How many mutations this payload carries -- zero for anything that
    /// isn't a write. Exists for the follower's check that a replicated
    /// entry arrived with all of them; see `raft::follower`.
    /// The mutations this payload carries, empty for anything that isn't
    /// a write.
    pub fn writes(&self) -> &[Mutation] {
        match self {
            EntryPayload::Write(writes) => writes,
            EntryPayload::NoOp | EntryPayload::Config(_) => &[],
        }
    }

    pub fn mutation_count(&self) -> usize {
        match self {
            EntryPayload::Write(writes) => writes.len(),
            EntryPayload::NoOp | EntryPayload::Config(_) => 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub seq: u64,
    pub term: u64,
    pub payload: EntryPayload,
}

/// What `entries_in_range_capped` read, and whether it stopped early.
///
/// `truncated` is not an error: it says "there is more, right now", which
/// is what lets a caller replicating a backlog send the next batch
/// immediately instead of waiting out a heartbeat interval first.
#[derive(Debug)]
pub struct CappedEntries {
    pub entries: Vec<LogEntry>,
    pub truncated: bool,
}

/// Append-only, gap-free, per-key-compactable log on top of `fjall`.
pub struct RaftLog {
    db: Database,
    log: Keyspace,
    versions: Keyspace,
    meta: Keyspace,
    next_seq: Mutex<u64>,
}

// --- implementation ---------------------------------------------------

impl RaftLog {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = Database::builder(path).open()?;
        // Default `KeyspaceCreateOptions`, including its 64 MiB
        // `max_memtable_size`, which is comfortably above the largest
        // entry that can exist (`raft::MAX_ENTRY_BYTES`). That is worth
        // knowing rather than assuming: an entry larger than the
        // memtable would force a rotation by itself, and enough of those
        // in a row reach fjall's write-halt loop, which sleeps the
        // calling thread -- here, a blocking-pool worker the event loop
        // is awaiting. Raising `MAX_ENTRY_BYTES` past this means sizing
        // these keyspaces deliberately.
        //
        // `log` and `versions` hold the large values, so they use KV
        // separation: values past the threshold live in blob files
        // instead of being rewritten by every compaction. `meta` holds
        // a handful of tiny values and has nothing to separate.
        //
        // Separation is why a `versions` row key carries everything that
        // describes the row: a row *key* is never in a blob file, so the
        // paths that only need to know which key, which seq and whether
        // it is a tombstone stay off the blobs entirely. See `codec`.
        let separated = || {
            KeyspaceCreateOptions::default()
                .with_kv_separation(Some(KvSeparationOptions::default()))
        };
        let log = db.keyspace(CF_LOG, separated)?;
        let versions = db.keyspace(CF_VERSIONS, separated)?;
        let meta = db.keyspace(CF_META, KeyspaceCreateOptions::default)?;
        let next_seq = Self::recover_next_seq(&log, &meta)?;

        Ok(Self {
            db,
            log,
            versions,
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

    /// Atomically assign the next sequence number and write a one-key
    /// write at the given Raft term. `value = None` records a delete
    /// tombstone for `key` rather than removing any row. Returns the
    /// assigned sequence number.
    ///
    /// The degenerate case of `append_writes`, kept because most callers
    /// -- and every single-key client write -- genuinely have one pair.
    pub fn append(&self, term: u64, key: &[u8], value: Option<&[u8]>) -> Result<u64> {
        self.append_writes(term, vec![Mutation::new(key.to_vec(), value.map(<[u8]>::to_vec))])
    }

    /// Append `writes` as *one* entry at one sequence number, so they
    /// commit together and apply together. This is what a transaction
    /// commits through.
    ///
    /// Only ever touches `log` -- an appended entry isn't durable in the
    /// Raft sense yet (it can still be rolled back by `truncate_from` if a
    /// leader change happens before it commits), so it must not be
    /// visible via `get_latest` until `apply_committed` confirms it. See
    /// the module doc.
    pub fn append_writes(&self, term: u64, writes: Vec<Mutation>) -> Result<u64> {
        self.append_payload(term, &EntryPayload::Write(writes))
    }

    /// Append a leader's term-opening no-op (see `EntryPayload::NoOp`).
    pub fn append_noop(&self, term: u64) -> Result<u64> {
        self.append_payload(term, &EntryPayload::NoOp)
    }

    /// Append a new cluster configuration (see `EntryPayload::Config`).
    /// `config` is opaque here -- storage neither parses nor validates
    /// it, it only guarantees the bytes come back out of `current_config`
    /// once this entry is the newest `Config` in the log.
    pub fn append_config(&self, term: u64, config: &[u8]) -> Result<u64> {
        self.append_payload(term, &EntryPayload::Config(config.to_vec()))
    }

    /// Append an entry whose payload is given whole. A follower replaying
    /// entries from a leader must use this: anything that reconstructed
    /// the payload from parts would risk flattening a replicated entry
    /// into something else -- a `NoOp` into a write, or a multi-key write
    /// into a single one -- and the follower's copy of the log would stop
    /// matching the leader's.
    pub fn append_payload(&self, term: u64, payload: &EntryPayload) -> Result<u64> {
        // Encoded before the lock is taken: it can fail, and failing
        // while holding `next_seq` would be a needlessly wide blast
        // radius for something that never touches sequencing.
        let encoded = encode_log_entry(term, payload)?;

        let mut guard = self.next_seq.lock().unwrap();
        let seq = *guard;

        let mut batch = self.db.batch();
        batch.insert(&self.log, be_bytes(seq), encoded);
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

    /// The row at the head of `key`'s run as of `as_of`: the newest
    /// version with `seq <= as_of`, whatever kind it is. `None` when the
    /// key had no version at all by then.
    ///
    /// One seek, whatever the run's length: `seek_as_of` lands past every
    /// version newer than `as_of` (they sort before it, having a larger
    /// seq and so a smaller complement), and `chain_end` stops the range
    /// before the next key's rows.
    ///
    /// The value comes back only when `want_value` is set *and* the head
    /// is a value row. `Guard::into_inner_if` decides from the row key
    /// whether to resolve the value at all, and under key-value
    /// separation declining is what keeps a large value's blob file
    /// unread -- so `version_as_of` asks for no value and pays for none,
    /// and nobody ever pays to read a tombstone's (empty) one.
    fn head_as_of(
        &self,
        key: &[u8],
        as_of: u64,
        want_value: bool,
    ) -> Result<Option<(u64, Kind, Option<fjall::UserValue>)>> {
        let range = seek_as_of(key, as_of)..chain_end(key);
        match self.versions.range(range).next() {
            Some(guard) => {
                let (row, value) = guard.into_inner_if(|row| {
                    want_value && matches!(codec::row_kind(row), Ok(Kind::Value))
                })?;
                Ok(Some((row_seq(&row)?, codec::row_kind(&row)?, value)))
            }
            None => Ok(None),
        }
    }

    /// The committed value for `key` as of `as_of`, and the seq it was
    /// written at. `None` if the key had never been written by then, or
    /// if its newest write at or before `as_of` was a delete -- the two
    /// are indistinguishable to a caller, as they always have been.
    ///
    /// Never reflects an appended-but-not-yet-committed write; see
    /// `apply_committed`.
    pub fn get_as_of(&self, key: &[u8], as_of: u64) -> Result<Option<(u64, Vec<u8>)>> {
        match self.head_as_of(key, as_of, true)? {
            Some((seq, Kind::Value, Some(value))) => Ok(Some((seq, value.to_vec()))),
            // A value row whose value did not come back cannot happen:
            // `head_as_of` was asked for one and the predicate admits
            // exactly this case.
            Some((_, Kind::Value, None)) => {
                Err(StorageError::Corrupt("a value row resolved to no value"))
            }
            Some((_, Kind::Tombstone, _)) | None => Ok(None),
        }
    }

    /// The current committed value for `key`, and the seq it was written
    /// at.
    ///
    /// Literally `get_as_of` at the largest seq there can be: `!u64::MAX`
    /// is zero, so the seek lands at the very head of the key's run,
    /// which is the newest version. There is deliberately no separate
    /// current-value path to keep in step with the historical one.
    pub fn get_latest(&self, key: &[u8]) -> Result<Option<(u64, Vec<u8>)>> {
        self.get_as_of(key, u64::MAX)
    }

    /// The seq `key`'s value as of `as_of` was written at, without the
    /// value. `None` on exactly the terms `get_as_of` returns `None`.
    ///
    /// Costs the same whether the value is one byte or sixteen megabytes:
    /// the seq is part of the row *key*, and `Guard::key` resolves it
    /// without touching the blob file the value may live in.
    pub fn version_as_of(&self, key: &[u8], as_of: u64) -> Result<Option<u64>> {
        match self.head_as_of(key, as_of, false)? {
            Some((seq, Kind::Value, _)) => Ok(Some(seq)),
            Some((_, Kind::Tombstone, _)) | None => Ok(None),
        }
    }

    /// The seq `key`'s current committed value was written at.
    pub fn get_version(&self, key: &[u8]) -> Result<Option<u64>> {
        self.version_as_of(key, u64::MAX)
    }

    /// Up to `limit` keys at or after `start` that had a value as of
    /// `as_of`, in ascending order, skipping any key `skip` rejects.
    ///
    /// `skip` is how a transaction's buffered deletes are honoured
    /// without teaching storage what a transaction is: a key the caller
    /// has already deleted doesn't exist as far as it is concerned, and
    /// skipping it here rather than filtering afterwards is what keeps a
    /// full batch full. A key whose head as of `as_of` is a tombstone is
    /// hidden on the same terms and for the same reason.
    ///
    /// Keys only. The values are the caller's business to fetch (or
    /// not), and a scan that carried them would make the size of a batch
    /// a property of the data rather than of `limit`. `Guard::key` is
    /// what makes that true in fact and not just in intent:
    /// `Guard::into_inner` resolves the value, which under KV separation
    /// is a blob file read per key, to be dropped on the next line.
    ///
    /// **One iterator, not a seek per key.** `Keyspace::range` pins a
    /// snapshot nonce, so this walks one point in time even while
    /// `apply_committed` is writing -- the same guarantee
    /// `export_snapshot` relies on, and the reason a scan can't catch a
    /// multi-key entry half-applied. Re-seeking per key would take a
    /// fresh nonce each time and quietly give that up. The cost is that
    /// this steps over the versions a key has that are newer than
    /// `as_of`, which is bounded by how promptly `gc_versions` runs.
    pub fn scan_as_of(
        &self,
        start: &[u8],
        as_of: u64,
        limit: usize,
        skip: impl Fn(&[u8]) -> bool,
    ) -> Result<Vec<Vec<u8>>> {
        let mut keys = Vec::new();
        // The run this scan has already decided about. Rows of one key
        // arrive together and newest first, so the first row at or below
        // `as_of` settles the key and the rest of its run is stepped
        // over -- compared as `row_chain`, which is a memcmp on bytes
        // already in hand rather than an unescape per row.
        let mut settled: Option<Vec<u8>> = None;

        for item in self.versions.range(chain_start(start)..) {
            if keys.len() == limit {
                break;
            }
            let row = item.key()?;
            let chain = codec::row_chain(&row)?;
            if settled.as_deref() == Some(chain) {
                continue;
            }
            if row_seq(&row)? > as_of {
                // Newer than the caller asked for; the next row down
                // this run may still qualify.
                continue;
            }
            settled = Some(chain.to_vec());
            if codec::row_kind(&row)? != Kind::Value {
                continue;
            }
            let key = row_user_key(&row)?;
            if !skip(&key) {
                keys.push(key);
            }
        }
        Ok(keys)
    }

    /// `scan_as_of` over current state.
    pub fn scan_latest(
        &self,
        start: &[u8],
        limit: usize,
        skip: impl Fn(&[u8]) -> bool,
    ) -> Result<Vec<Vec<u8>>> {
        self.scan_as_of(start, u64::MAX, limit, skip)
    }

    /// Applies every committed log entry not yet reflected in `versions`,
    /// up through and including `up_to_seq`, so `versions` always
    /// represents state that's permanent -- never a write that could still
    /// be rolled back by a leader change (`truncate_from`) before it
    /// commits. See the module doc.
    ///
    /// Every mutation *adds* a row: a write adds a value row at this
    /// entry's seq, a delete adds a tombstone at it. Nothing is ever
    /// overwritten or removed here, which is what leaves earlier versions
    /// readable by a checkpoint taken before them, and is why
    /// `gc_versions` exists to collect the ones no checkpoint can reach.
    ///
    /// An entry's mutations all go into the same batch, and the batch is
    /// only ever flushed *between* entries, so a `Write` is applied whole
    /// or not at all -- which is what lets a transaction's keys share one
    /// seq and one moment of becoming durable.
    ///
    /// The cost is that one entry with more mutations than `WRITE_CHUNK`
    /// makes a batch larger than the chunking is meant to allow. That is
    /// the right way round -- splitting it would break the guarantee the
    /// entry exists to provide -- and the size is bounded before anything
    /// reaches the log, by the cap the client-facing service puts on a
    /// transaction.
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
            match entry.payload {
                EntryPayload::Write(writes) => {
                    for m in last_write_per_key(writes) {
                        match m.value {
                            Some(value) => {
                                batch.insert(
                                    &self.versions,
                                    row_key(&m.key, seq, Kind::Value),
                                    value,
                                );
                            }
                            None => {
                                batch.insert(
                                    &self.versions,
                                    row_key(&m.key, seq, Kind::Tombstone),
                                    [],
                                );
                            }
                        }
                        pending += 1;
                    }
                }
                // Not state machine data -- these still consume a seq and
                // move the applied watermark, they just have no effect
                // here. A `Config` in particular is deliberately *not*
                // mirrored into `versions`: it takes effect on append, so
                // `current_config` reads it straight out of `log`, where
                // it is visible before it commits.
                EntryPayload::NoOp | EntryPayload::Config(_) => continue,
            }
            // Deliberately outside the mutation loop: flushing between
            // two mutations of one entry is exactly the tear this is
            // meant to prevent.
            self.flush_if_full(&mut batch, &mut pending)?;
        }

        batch.insert(&self.meta, META_APPLIED_SEQ, be_bytes(up_to_seq));
        batch.commit()?;
        Ok(())
    }

    /// Whether any version row exists for `key` at all, at any seq --
    /// including a tombstone. For the GC's benefit and for tests; a
    /// caller asking whether a key *has a value* wants `get_as_of`.
    #[cfg(test)]
    pub(crate) fn version_count(&self, key: &[u8]) -> Result<usize> {
        Ok(self
            .versions
            .range(chain_start(key)..chain_end(key))
            .count())
    }

    /// Entries with `start <= seq < end`, in ascending seq order. Any seq in
    /// the range that isn't present (compacted away) is silently skipped —
    /// see the module-level note on why that's safe for log-matching.
    pub fn entries_in_range(&self, start: u64, end: u64) -> Result<Vec<LogEntry>> {
        Ok(self.entries_in_range_capped(start, end, usize::MAX)?.entries)
    }

    /// `entries_in_range`, stopping once the entries read total more than
    /// `max_bytes` — measured in stored row bytes, which is what the
    /// caller is really budgeting (a row is within framing overhead of
    /// what it costs to send).
    ///
    /// **Always returns at least one entry**, however large it is, so that
    /// a single oversized entry can still make progress rather than
    /// wedging replication behind a budget it can never fit into. That
    /// makes the budget a soft one, and puts the hard ceiling somewhere
    /// else: nothing that exceeds `MAX_ENTRY_BYTES` may reach the log in
    /// the first place. See the cap chain in `raft`.
    pub fn entries_in_range_capped(
        &self,
        start: u64,
        end: u64,
        max_bytes: usize,
    ) -> Result<CappedEntries> {
        let mut entries = Vec::new();
        let mut bytes = 0usize;
        let mut truncated = false;

        for item in self.log.range(be_bytes(start)..be_bytes(end)) {
            let (k, v) = item.into_inner()?;
            // Would this entry take us past the budget? Stop before
            // decoding it -- unless nothing has been read yet, in which
            // case take it however large it is.
            if !entries.is_empty() && bytes.saturating_add(v.len()) > max_bytes {
                truncated = true;
                break;
            }
            let seq = from_be_bytes(&k)?;
            bytes = bytes.saturating_add(v.len());
            let entry = decode_log_entry(&v)?;
            entries.push(LogEntry {
                seq,
                term: entry.term,
                payload: entry.payload,
            });
        }
        Ok(CappedEntries { entries, truncated })
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
            if let EntryPayload::Config(config) = decode_log_entry(&v)?.payload {
                return Ok(Some(config));
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
    ///
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
            if let EntryPayload::Config(config) = entry.payload {
                // About to delete the entry holding this configuration --
                // preserve it in `meta` so `current_config` can still
                // answer once nothing in `log` carries it. Ascending
                // order, so the last one seen is the newest.
                dropped_config = Some(config);
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
