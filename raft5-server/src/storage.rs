//! Raft log storage on top of `fjall`.
//!
//! # Layout
//!
//! Three keyspaces:
//! - `log`: seq (u64 BE) -> encoded (term, payload) entry.
//! - `versions`: one row per version of a key (see below).
//! - `meta`: a small set of fixed keys:
//!   - the floor (`floor_seq`, `floor_term`): the seq and term of the
//!     highest entry no longer in `log`, set by `install_snapshot` and
//!     `compact`;
//!   - the compaction watermark (`compact_last_seq`), set only by
//!     `compact`, to resume an incremental compaction;
//!   - the applied watermark (`applied_seq`), set only by
//!     `apply_committed`, for the same reason;
//!   - the version-GC cursor (`gc_cursor`, see `gc_versions`);
//!   - `base_config`: the last cluster configuration removed from `log` by
//!     `compact` or `install_snapshot` (see `config_at`).
//!
//! # Versions
//!
//! `versions` keeps every version. A write adds a row instead of replacing
//! the old one, and a delete adds a tombstone row instead of removing
//! anything. A row key is `escape(user key) || 00 00 || !seq || kind`
//! (`codec::row_key`). It sorts by user key, then newest seq first, so all
//! versions of a key form one run with the current value at its head.
//!
//! This makes checkpoints possible (named, point-in-time read views, see
//! `checkpoint.rs`): a read "as of seq S" is one seek into a key's run,
//! whatever the run's length. `get_latest` is `get_as_of(key, u64::MAX)`,
//! so there is no separate current-value path to keep in step.
//!
//! All facts about a row live in its key, never its value. So
//! `get_version`, `scan_latest` and `gc_versions` work from `Guard::key()`
//! alone and never touch the blob files where key-value separation puts
//! large values.
//!
//! The cost: an overwritten version stays until something collects it.
//! `gc_versions` does that, and it is required: without it this keyspace
//! grows without limit under any workload that overwrites, with or without
//! checkpoints.
//!
//! # Entries
//!
//! Not every entry is key/value data: `EntryPayload` also has a leader's
//! term-opening no-op and the cluster configuration. These take a seq and
//! take part in log matching and commit, but `apply_committed` skips them,
//! so they never reach `versions`.
//!
//! A `Write` payload carries any number of mutations, and they apply to
//! `versions` together: one entry, one seq, one batch. That is what makes
//! a multi-key transaction possible (see `apply_committed`).
//!
//! `append` writes only to `log`. Only `apply_committed` writes to
//! `versions`, driven by Raft's commit index (`raft::Common::commit_tx`),
//! not by appends. This matters: a leader change can still roll back an
//! uncommitted entry (`truncate_from`), so a read from `versions` before
//! commit could expose a write that never durably happened.
//!
//! Sequence numbers are assigned under a mutex held for the whole
//! `append`, `truncate_from` or `install_snapshot` write, so a failed
//! write never leaves a gap or reuses a seq.
//!
//! # Deletes
//!
//! A delete adds a tombstone row (`apply_committed`). At the head of a run,
//! a tombstone reads like "never written": `get_as_of` returns `None` for
//! both. Further down the run, only a tombstone can tell a checkpoint taken
//! before the delete that "this key did not exist as of S". `gc_versions`
//! drops a head tombstone once nothing older survives, so a deleted key's
//! rows disappear on a store with no checkpoints.
//!
//! So `export_snapshot` must carry tombstones: one that a checkpoint
//! selects is state, not absence, and a recipient without it would answer
//! that checkpoint with a value the sender knows was deleted.
//!
//! In `log` itself, a delete is an entry with no value:
//! `append(term, key, None)` records the delete at that seq, and `compact`
//! treats it like any other entry.
//!
//! # Compaction
//!
//! `compact` removes `log` entries, so it must never get ahead of
//! `apply_committed`: an entry removed before it reaches `versions` loses
//! its data. `compact` clamps `below_seq` to the applied watermark, and
//! `raft::spawn_storage_maintenance_task` always runs `apply_committed`
//! just before `compact`, so the clamp normally does nothing.
//!
//! `compact` deletes every `log` entry below its threshold, so
//! `term_at` and `entries_in_range` can find gaps there, even above any
//! snapshot boundary. That is safe for log matching: entries below a
//! node's compaction or snapshot point are committed and can never
//! conflict again (Leader Completeness), so comparisons only happen in the
//! live tail.
//!
//! # Visibility
//!
//! A `fjall` batch is atomic (all of it lands or none of it), but readers
//! do not see it all at once. `Batch::commit` takes its seqno before it
//! inserts into memtables, and a point read runs at `SeqNo::MAX`, so a
//! reader on another thread can see half a batch.
//!
//! This needs no barrier here:
//! - `get_latest` reads one key, which has no half.
//! - `export_snapshot` and `scan_latest` read many keys, but
//!   `Keyspace::iter`/`range` pin a snapshot nonce, so each reads one point
//!   in time.
//! - `read_many` reads through one `fjall::Snapshot`, which reads at the
//!   seqno fjall publishes after a batch finishes.
//!
//! Two separate reads can still see two points in time: read `a`, then
//! read `b`, and the entry that wrote both may land in between. That is not
//! a torn batch, and no storage lock can fix it. A caller that needs many
//! keys at one moment uses `read_many`, or takes read locks in a
//! transaction (`grpc::KvService`).
//!
//! # Memory
//!
//! `compact`, `apply_committed` and `install_snapshot` read through fjall
//! iterators, which pin a point-in-time view when created. So each can
//! write to the live `Database` from the same loop it reads in, and
//! process the keyspaces in bounded chunks instead of buffering a whole
//! range in memory.

use std::path::Path;
use std::sync::Mutex;

use fjall::{
    Database, Keyspace, KeyspaceCreateOptions, KvSeparationOptions, OwnedWriteBatch, Readable,
    Snapshot,
};
use serde::{Deserialize, Serialize};

mod checkpoint;
mod codec;
mod gc;
mod meta;
mod backup;
mod reserved;
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

pub use backup::{BackupExport, BackupSource, BootstrapRefusal, RestoreOutcome};
pub use checkpoint::{Checkpoint, MAX_LIVE_CHECKPOINTS, checkpoint_key, encode_record};
pub use gc::GcStats;
pub use snapshot::SnapshotMeta;

const CF_LOG: &str = "log";
const CF_VERSIONS: &str = "versions";
const CF_META: &str = "meta";

/// The most pending writes in a `WriteBatch` before it is flushed. Used
/// wherever a large range is processed in bounded chunks.
const WRITE_CHUNK: usize = 1000;

#[derive(Debug)]
pub enum StorageError {
    Db(fjall::Error),
    Io(std::io::Error),
    Corrupt(&'static str),
    /// A row would not decode, or would not encode. Decoding is the case
    /// that matters: it is how a corrupt, truncated or foreign row is refused
    /// instead of half-read.
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

/// `writes` reduced to at most one mutation per key, the last one for
/// each key, in the original order.
///
/// **One entry may add at most one row per key.** The row encoding needs
/// this. A value and a tombstone at one seq are two different row keys,
/// and the tombstone sorts first (`Kind::Tombstone` is 0). So a
/// delete-then-write in one entry would leave the write under its own
/// delete, and the key would read as absent.
///
/// `txn::Locks` already removes duplicate keys, so entries from this
/// cluster have none. This is for a hand-built `append_writes`, and for a
/// replicated entry from a peer that did not dedupe.
fn last_write_per_key(writes: Vec<Mutation>) -> Vec<Mutation> {
    use std::collections::{HashMap, HashSet};

    // The index of the last mutation for each key. Built from borrows, so it
    // is in its own scope: `writes` is consumed below.
    let keep: HashSet<usize> = {
        let mut last: HashMap<&[u8], usize> = HashMap::with_capacity(writes.len());
        for (i, m) in writes.iter().enumerate() {
            last.insert(&m.key, i);
        }
        // No duplicates is by far the most common case, and it needs no filter
        // pass.
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

/// One key's change in a `Write` entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mutation {
    // `serde_bytes` on every byte-string field in this module, and it is
    // required. Serde's default `Vec<u8>` is a sequence of u8: one serializer
    // call per byte, about 50 MB/s. `serde_bytes` makes it one
    // `serialize_bytes` call and a memcpy. Small values show no difference;
    // for large ones it dominates. A few megabytes of transaction cost whole
    // CPU-seconds here without it.
    #[serde(with = "serde_bytes")]
    pub key: Vec<u8>,
    /// `None` is a delete: a tombstone in `log`, and a tombstone row in
    /// `versions` once applied.
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

/// What a log entry does. Only `Write` carries key/value data. The others
/// take a seq, so they take part in log matching and commit, but
/// `apply_committed` skips them, so they never reach `versions`.
///
/// An enum with the data inside, not a kind tag next to a key and value
/// that are always present: a `NoOp` has no key, and a `Write` with no
/// mutations is not a write. The type says so, so no caller has to
/// remember it.
///
/// **The variant order is the on-disk format.** Postcard writes an enum
/// discriminant by position, so you may add variants at the end, but never
/// reorder or remove them. See `codec`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryPayload {
    /// One or more mutations that apply to `versions` as one unit. A
    /// transaction is exactly this: all its keys, in one entry, at one seq.
    Write(Vec<Mutation>),
    /// A new leader's term-opening no-op. It lets a leader commit an entry
    /// from its own term before it moves the commit index over entries from
    /// an earlier term (Raft §5.4.2, Figure 8).
    NoOp,
    /// An encoded cluster configuration. Unlike every other payload, it takes
    /// effect when appended, not when committed: see `current_config`.
    Config(#[serde(with = "serde_bytes")] Vec<u8>),
}

impl EntryPayload {
    /// The mutations this payload carries. Empty for anything that is not a
    /// write.
    pub fn writes(&self) -> &[Mutation] {
        match self {
            EntryPayload::Write(writes) => writes,
            EntryPayload::NoOp | EntryPayload::Config(_) => &[],
        }
    }

    /// How many mutations this payload carries: zero for anything that is
    /// not a write. A follower uses it to check that a replicated entry
    /// arrived whole: see `raft::follower`.
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
/// `truncated` is not an error. It means "there is more now", so a caller
/// replicating a backlog can send the next batch at once instead of
/// waiting a heartbeat.
#[derive(Debug)]
pub struct CappedEntries {
    pub entries: Vec<LogEntry>,
    pub truncated: bool,
}

/// What a scan returns: the keys it kept, and where to start again if it
/// stopped before the batch was full. See [`RaftLog::scan_as_of`].
pub type ScanPage = (Vec<Vec<u8>>, Option<Vec<u8>>);

/// The moment [`RaftLog::read_many`] reads at.
#[derive(Debug, Clone)]
pub enum ReadPoint {
    /// Everything this node has applied.
    Latest,
    /// As of the checkpoint with this id.
    Checkpoint(Vec<u8>),
}

/// What [`RaftLog::read_many`] does with one key's value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fetch {
    /// Read it, and count its length against the budget.
    Value,
    /// Do not read it (under key-value separation, its blob file stays
    /// untouched), and count nothing.
    VersionOnly,
    /// Do not read it, but count this many bytes against the budget. The
    /// caller has its own value for this position (a transaction's buffered
    /// write), and the budget is about the response, not about what storage
    /// read.
    Charge(usize),
}

/// One key as [`RaftLog::read_many`] found it: the seq its value was
/// written at, and the value if it was fetched. `None` for a key with no
/// value.
pub type ReadRow = Option<(u64, Option<Vec<u8>>)>;

/// What [`RaftLog::read_many`] found.
#[derive(Debug)]
pub enum MultiRead {
    Ok {
        /// The seq that every row was read as of.
        as_of: u64,
        /// One per key, in order. A prefix of the keys if the budget ran out,
        /// but never empty when there were keys. `None` is a key with no value
        /// as of `as_of`. A value is present only for `Fetch::Value`.
        rows: Vec<ReadRow>,
    },
    /// No checkpoint has this id on this node.
    NoSuchCheckpoint,
}

/// An append-only, gap-free Raft log with versioned key state, on top of
/// `fjall`. See the module doc.
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
        // Default `KeyspaceCreateOptions`. Its 64 MiB `max_memtable_size` is
        // well above the largest possible entry (`raft::MAX_ENTRY_BYTES`). That
        // matters: an entry larger than the memtable forces a rotation by
        // itself, and enough of those in a row reach fjall's write-halt loop,
        // which sleeps the calling thread (here, a blocking-pool worker that the
        // event loop waits on). If you raise `MAX_ENTRY_BYTES` past this, size
        // these keyspaces on purpose.
        //
        // `log` and `versions` hold the large values, so they use key-value
        // separation: large values live in blob files and are not rewritten by
        // every compaction. `meta` holds a few tiny values and needs none.
        //
        // Separation is why a `versions` row key holds every fact about the row:
        // a row key is never in a blob file, so code that needs only the key,
        // the seq and the tombstone flag never touches blobs. See `codec`.
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

    /// `next_seq` must take both the physical log tail and the floor into
    /// account. After `install_snapshot`, or after `compact` empties `log` to
    /// its tail, `log` can be empty while seqs up to the floor are already
    /// used.
    fn recover_next_seq(log: &Keyspace, meta: &Keyspace) -> Result<u64> {
        let log_tail = match log.last_key_value() {
            Some(guard) => from_be_bytes(&guard.key()?)?,
            None => 0,
        };
        let (floor_seq, _) = read_floor(meta)?;
        Ok(log_tail.max(floor_seq) + 1)
    }

    /// The persisted `(current_term, voted_for)` pair. Raft safety depends on
    /// it: a node must never vote twice in one term, even across a restart.
    /// `(0, None)` if never persisted (a new node). `raft::Common::new` loads
    /// it once at startup.
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

    /// Persist a new `(current_term, voted_for)` pair atomically.
    /// `voted_for = None` clears any earlier vote (for example, when moving to
    /// a term with no vote yet), so no stale vote stays on disk.
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

    /// Assign the next seq atomically and write a one-key write at the given
    /// Raft term. `value = None` records a delete tombstone for `key`; it does
    /// not remove any row. Returns the assigned seq.
    ///
    /// The one-pair case of `append_writes`, kept because most callers have
    /// exactly one pair.
    pub fn append(&self, term: u64, key: &[u8], value: Option<&[u8]>) -> Result<u64> {
        self.append_writes(term, vec![Mutation::new(key.to_vec(), value.map(<[u8]>::to_vec))])
    }

    /// Append `writes` as one entry at one seq, so they commit and apply
    /// together. A transaction commits through this.
    ///
    /// Touches only `log`. An appended entry is not yet durable in the Raft
    /// sense (`truncate_from` can still roll it back after a leader change),
    /// so `get_latest` must not show it until `apply_committed` confirms it.
    /// See the module doc.
    pub fn append_writes(&self, term: u64, writes: Vec<Mutation>) -> Result<u64> {
        self.append_payload(term, &EntryPayload::Write(writes))
    }

    /// Append a leader's term-opening no-op (see `EntryPayload::NoOp`).
    pub fn append_noop(&self, term: u64) -> Result<u64> {
        self.append_payload(term, &EntryPayload::NoOp)
    }

    /// Append a new cluster configuration (see `EntryPayload::Config`).
    /// `config` is opaque here: storage does not parse or check it, it only
    /// makes sure the bytes come back from `current_config` while this is the
    /// newest `Config` in the log.
    pub fn append_config(&self, term: u64, config: &[u8]) -> Result<u64> {
        self.append_payload(term, &EntryPayload::Config(config.to_vec()))
    }

    /// Append an entry with the payload given whole. A follower that replays
    /// a leader's entries must use this: rebuilding the payload from parts
    /// could change a replicated entry (a `NoOp` into a write, a multi-key
    /// write into a single one), and the follower's log would stop matching
    /// the leader's.
    pub fn append_payload(&self, term: u64, payload: &EntryPayload) -> Result<u64> {
        // Encode before taking the lock: encoding can fail, and failing while
        // holding `next_seq` would block sequencing for no reason.
        let encoded = encode_log_entry(term, payload)?;

        let mut guard = self.next_seq.lock().unwrap();
        let seq = *guard;

        let mut batch = self.db.batch();
        batch.insert(&self.log, be_bytes(seq), encoded);
        batch.commit()?;

        // Advance only after the write succeeds, so a failure never leaves a gap
        // or lets another append reuse `seq`.
        *guard = seq + 1;
        drop(guard);

        Ok(seq)
    }

    /// Delete every `log` entry with `seq >= new_tail_seq` and resume
    /// sequencing from there. For Raft log matching: a follower that finds a
    /// conflicting entry truncates its divergent suffix, then accepts the
    /// leader's entries at the same seqs.
    ///
    /// Expected only on the uncommitted tail. Entries below the compaction
    /// floor or `applied_seq` are committed (Leader Completeness) and must
    /// never be truncated. This is not checked here: log matching must never
    /// choose a truncation point that low.
    ///
    /// Touches only `log`: `versions` shows only committed entries (see the
    /// module doc and `apply_committed`), and this never touches a committed
    /// entry, so nothing in `versions` needs fixing.
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

    /// The row at the head of `key`'s run as of `as_of`: the newest version
    /// with `seq <= as_of`, of any kind. `None` if the key had no version by
    /// then.
    ///
    /// One seek, whatever the run's length: `seek_as_of` lands past every
    /// version newer than `as_of` (they sort before it, with a larger seq and
    /// so a smaller complement), and `chain_end` stops before the next key's
    /// rows.
    ///
    /// The value comes back only if `want_value` is set and the head is a
    /// value row. `Guard::into_inner_if` decides from the row key whether to
    /// read the value. Under key-value separation, skipping it keeps a large
    /// value's blob file unread, so `version_as_of` pays for no value, and
    /// nobody reads a tombstone's empty value.
    ///
    /// Reads through `snap`, not the live keyspace, so `read_many` can answer
    /// many keys from one point in time. A single-key caller opens its own
    /// snapshot, as the live keyspace would. So there is one head lookup, not
    /// two to keep in step.
    fn head_as_of(
        &self,
        snap: &Snapshot,
        key: &[u8],
        as_of: u64,
        want_value: bool,
    ) -> Result<Option<(u64, Kind, Option<fjall::UserValue>)>> {
        let range = seek_as_of(key, as_of)..chain_end(key);
        match snap.range(&self.versions, range).next() {
            Some(guard) => {
                let (row, value) = guard.into_inner_if(|row| {
                    want_value && matches!(codec::row_kind(row), Ok(Kind::Value))
                })?;
                Ok(Some((row_seq(&row)?, codec::row_kind(&row)?, value)))
            }
            None => Ok(None),
        }
    }

    /// The committed value of `key` as of `as_of`, and the seq it was written
    /// at. `None` if the key was never written by then, or if its newest write
    /// at or before `as_of` was a delete. A caller cannot tell these two
    /// apart.
    ///
    /// Never shows an appended but uncommitted write: see
    /// `apply_committed`.
    pub fn get_as_of(&self, key: &[u8], as_of: u64) -> Result<Option<(u64, Vec<u8>)>> {
        match self.head_as_of(&self.db.snapshot(), key, as_of, true)? {
            Some((seq, Kind::Value, Some(value))) => Ok(Some((seq, value.to_vec()))),
            // Cannot happen: `head_as_of` was asked for a value, and the predicate
            // reads the value in exactly this case.
            Some((_, Kind::Value, None)) => {
                Err(StorageError::Corrupt("a value row resolved to no value"))
            }
            Some((_, Kind::Tombstone, _)) | None => Ok(None),
        }
    }

    /// The current committed value of `key`, and the seq it was written at.
    ///
    /// This is `get_as_of` at the largest seq: `!u64::MAX` is zero, so the
    /// seek lands at the head of the key's run, which is the newest version.
    /// There is no separate current-value path, on purpose.
    pub fn get_latest(&self, key: &[u8]) -> Result<Option<(u64, Vec<u8>)>> {
        self.get_as_of(key, u64::MAX)
    }

    /// The seq at which `key`'s value as of `as_of` was written, without the
    /// value. `None` in the same cases as `get_as_of`.
    ///
    /// Costs the same for a one-byte or a 16 MiB value: the seq is part of
    /// the row key, and `Guard::key` reads it without touching the value's
    /// blob file.
    pub fn version_as_of(&self, key: &[u8], as_of: u64) -> Result<Option<u64>> {
        match self.head_as_of(&self.db.snapshot(), key, as_of, false)? {
            Some((seq, Kind::Value, _)) => Ok(Some(seq)),
            Some((_, Kind::Tombstone, _)) | None => Ok(None),
        }
    }

    /// The seq at which `key`'s current committed value was written.
    pub fn get_version(&self, key: &[u8]) -> Result<Option<u64>> {
        self.version_as_of(key, u64::MAX)
    }

    /// Read many keys, all as of the same moment, and return that moment.
    ///
    /// **One snapshot for all keys.** Reading the keys one `get_as_of` at a
    /// time takes a new point in time per key, and an entry that wrote two of
    /// them could land in between (see "Visibility" in the module doc). A
    /// `fjall::Snapshot` reads at the seqno fjall publishes after a batch
    /// finishes, so it never sees part of a batch, and `apply_committed` never
    /// splits an entry across batches.
    ///
    /// The moment is found inside the same snapshot as the keys:
    /// - `Latest` reads the applied watermark. `apply_committed` writes it
    ///   into every batch it flushes, so it names exactly the entries whose
    ///   rows the snapshot sees. The keys are read at their heads, not by
    ///   seeking to the watermark: the result is the same, and the version GC
    ///   cannot have removed a row the head read needs.
    /// - `Checkpoint` finds the record in the snapshot too, so a
    ///   `RemoveSnapshot`, and the GC it allows, cannot land between finding
    ///   the checkpoint and reading through it.
    ///
    /// `value_budget` limits the value bytes returned, counted in key order.
    /// The key that would go past it is not returned, nor is any key after
    /// it, so `rows` is then a prefix of `keys`. The first key is always
    /// returned, so a caller that reissues for the rest always makes progress.
    /// A value's length is known only after it is read, so the value that
    /// goes over is read and dropped: at most one per call.
    pub fn read_many(
        &self,
        keys: &[(Vec<u8>, Fetch)],
        point: ReadPoint,
        value_budget: usize,
    ) -> Result<MultiRead> {
        let snap = self.db.snapshot();
        let Some((as_of, seek)) = self.resolve_point(&snap, &point)? else {
            return Ok(MultiRead::NoSuchCheckpoint);
        };

        let mut rows = Vec::with_capacity(keys.len());
        let mut spent = 0usize;
        for (key, fetch) in keys {
            let want_value = matches!(fetch, Fetch::Value);
            let row = match self.head_as_of(&snap, key, seek, want_value)? {
                Some((seq, Kind::Value, value)) => {
                    if want_value && value.is_none() {
                        // As in `get_as_of`: a value was asked for, and this is a value row.
                        return Err(StorageError::Corrupt("a value row resolved to no value"));
                    }
                    Some((seq, value.map(|v| v.to_vec())))
                }
                Some((_, Kind::Tombstone, _)) | None => None,
            };
            let cost = match fetch {
                Fetch::Value => row
                    .as_ref()
                    .and_then(|(_, value)| value.as_ref())
                    .map_or(0, Vec::len),
                Fetch::VersionOnly => 0,
                Fetch::Charge(bytes) => *bytes,
            };
            spent = spent.saturating_add(cost);
            if spent > value_budget && !rows.is_empty() {
                break;
            }
            rows.push(row);
        }
        Ok(MultiRead::Ok { as_of, rows })
    }

    /// The moment `point` names, inside `snap`: `(as_of, seek)`, where
    /// `as_of` is the seq to report and `seek` the seq to read heads at.
    /// `None` if `point` names a checkpoint this node does not have.
    ///
    /// - `Latest` reads the applied watermark, which names exactly the
    ///   entries `snap` can see. Heads are read at `u64::MAX`, which gives the
    ///   same rows, and the version GC cannot have removed one of them.
    /// - `Checkpoint` finds the record in `snap` too, so a `RemoveSnapshot`,
    ///   and the GC it allows, cannot land between finding the checkpoint and
    ///   reading through it.
    fn resolve_point(&self, snap: &Snapshot, point: &ReadPoint) -> Result<Option<(u64, u64)>> {
        match point {
            ReadPoint::Latest => {
                let applied = match snap.get(&self.meta, META_APPLIED_SEQ)? {
                    Some(bytes) => from_be_bytes(&bytes)?,
                    None => 0,
                };
                Ok(Some((applied, u64::MAX)))
            }
            ReadPoint::Checkpoint(id) => {
                match self.head_as_of(snap, &checkpoint_key(id), u64::MAX, false)? {
                    Some((seq, Kind::Value, _)) => Ok(Some((seq, seq))),
                    Some((_, Kind::Tombstone, _)) | None => Ok(None),
                }
            }
        }
    }

    /// Up to `limit` keys at or after `start` that had a value as of `as_of`,
    /// in ascending order, skipping keys that `skip` rejects. Also returns
    /// where to start again if the scan stopped early.
    ///
    /// `budget` limits how many keys the scan decides about (kept or skipped)
    /// before it gives up on filling the batch. Without it, a `skip` that
    /// rejects most keys makes one call walk the whole keyspace. Two callers
    /// can do that: a transaction with many buffered deletes, and a caller
    /// whose ACL access is a small part of a large store. The second return
    /// value is `Some(key)` only when the budget ran out. That key has not
    /// been decided, so a caller that starts its next batch there misses
    /// nothing.
    ///
    /// The budget limits work, not results: a batch cut short this way can be
    /// shorter than `limit`, or empty, with many keys after it. So the cursor
    /// is returned; the caller cannot derive it from the last key, because
    /// there may be none.
    ///
    /// `skip` lets a transaction's buffered deletes count without storage
    /// knowing what a transaction is. A key the caller deleted does not exist
    /// for it, and skipping it here, not filtering after, keeps a full batch
    /// full. A key whose head as of `as_of` is a tombstone is hidden the same
    /// way, for the same reason.
    ///
    /// Keys only. The caller fetches values if it wants them; a scan that
    /// carried values would make the batch size depend on the data, not on
    /// `limit`. `Guard::key` makes this true in practice: `Guard::into_inner`
    /// reads the value, which under key-value separation is a blob file read
    /// per key, only to drop it.
    ///
    /// **One iterator, not a seek per key.** `Keyspace::range` pins a snapshot
    /// nonce, so the walk sees one point in time even while
    /// `apply_committed` writes. `export_snapshot` relies on the same
    /// guarantee, and it is why a scan cannot see a multi-key entry half
    /// applied. A seek per key would take a new nonce each time and lose this.
    /// The cost: the walk steps over versions newer than `as_of`, which is
    /// bounded by how often `gc_versions` runs.
    pub fn scan_as_of(
        &self,
        start: &[u8],
        as_of: u64,
        limit: usize,
        budget: usize,
        skip: impl Fn(&[u8]) -> bool,
    ) -> Result<ScanPage> {
        let mut keys = Vec::new();
        // The key this scan has already decided. A key's rows arrive together,
        // newest first, so the first row at or below `as_of` decides the key and
        // the rest of its run is skipped. Compared with `row_chain`, a memcmp on
        // bytes we already have, not an unescape per row.
        let mut settled: Option<Vec<u8>> = None;
        // How many keys this scan has decided, skipped or kept.
        let mut examined = 0usize;
        let mut resume = None;

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
                // Newer than the caller asked for. The next row of this run may still
                // qualify.
                continue;
            }
            if examined == budget {
                // Out of budget before the batch is full, which means `skip` rejected
                // most of what we walked. Stop and say where to start again: at this
                // key, which is not yet decided, so nothing between the two batches is
                // missed. The unescape happens only here, which is why the budget is
                // checked before the key is decided.
                resume = Some(row_user_key(&row)?);
                break;
            }
            settled = Some(chain.to_vec());
            examined += 1;
            if codec::row_kind(&row)? != Kind::Value {
                continue;
            }
            let key = row_user_key(&row)?;
            if !skip(&key) {
                keys.push(key);
            }
        }
        Ok((keys, resume))
    }

    /// `scan_as_of` over current state.
    pub fn scan_latest(
        &self,
        start: &[u8],
        limit: usize,
        budget: usize,
        skip: impl Fn(&[u8]) -> bool,
    ) -> Result<ScanPage> {
        self.scan_as_of(start, u64::MAX, limit, budget, skip)
    }

    /// Apply every committed log entry not yet in `versions`, up to and
    /// including `up_to_seq`. So `versions` shows only permanent state, never
    /// a write that a leader change (`truncate_from`) could still roll back.
    /// See the module doc.
    ///
    /// Every mutation adds a row: a write adds a value row at this entry's
    /// seq, a delete adds a tombstone. Nothing is overwritten or removed
    /// here, so earlier versions stay readable by a checkpoint taken before
    /// them. That is why `gc_versions` exists: to collect versions no
    /// checkpoint can reach.
    ///
    /// All mutations of one entry go into the same batch, and a batch is
    /// flushed only between entries. So a `Write` applies whole or not at all,
    /// and a transaction's keys share one seq and become durable at one moment.
    ///
    /// The cost: one entry with more than `WRITE_CHUNK` mutations makes a
    /// batch larger than the chunking intends. That is the right trade-off
    /// (splitting the entry would break its guarantee), and the client service
    /// caps the entry size before it reaches the log.
    ///
    /// Resumes from the persisted watermark instead of from 0, like `compact`.
    /// Does nothing if `up_to_seq` is not past it.
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
                // Not state machine data. These still use a seq and move the applied
                // watermark, but have no effect here. A `Config` is not copied into
                // `versions`, on purpose: it takes effect on append, so `current_config`
                // reads it from `log`, where it is visible before commit.
                EntryPayload::NoOp | EntryPayload::Config(_) => continue,
            }
            // Outside the mutation loop on purpose: a flush between two mutations of
            // one entry is the tear this prevents.
            //
            // Every batch carries the watermark of its last entry, not only the
            // final batch. So a reader's snapshot always finds a watermark that names
            // exactly the rows it sees, which lets `read_many` report the moment it
            // read at. Only at a flush, never per entry: the same key written twice
            // in one batch shares a seqno, and which write wins is not reliable.
            if pending >= WRITE_CHUNK {
                batch.insert(&self.meta, META_APPLIED_SEQ, be_bytes(seq));
            }
            self.flush_if_full(&mut batch, &mut pending)?;
        }

        batch.insert(&self.meta, META_APPLIED_SEQ, be_bytes(up_to_seq));
        batch.commit()?;
        Ok(())
    }

    /// Whether any version row exists for `key`, at any seq, including a
    /// tombstone. For the GC and for tests. To ask whether a key has a value,
    /// use `get_as_of`.
    #[cfg(test)]
    pub(crate) fn version_count(&self, key: &[u8]) -> Result<usize> {
        Ok(self
            .versions
            .range(chain_start(key)..chain_end(key))
            .count())
    }

    /// Entries with `start <= seq < end`, in ascending seq order. A seq in the
    /// range that is not present (compacted away) is skipped. See the module
    /// doc for why that is safe for log matching.
    pub fn entries_in_range(&self, start: u64, end: u64) -> Result<Vec<LogEntry>> {
        Ok(self.entries_in_range_capped(start, end, usize::MAX)?.entries)
    }

    /// `entries_in_range`, but it stops once the entries read total more than
    /// `max_bytes`, measured in stored row bytes (a row costs about what it
    /// costs to send, minus framing).
    ///
    /// **Always returns at least one entry**, however large, so one oversized
    /// entry still makes progress and does not block replication behind a
    /// budget it can never fit. So the budget is soft, and the hard limit is
    /// elsewhere: nothing larger than `MAX_ENTRY_BYTES` may reach the log. See
    /// the cap chain in `raft`.
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
            // Would this entry go over the budget? Stop before decoding it, unless
            // nothing has been read yet: then take it, however large.
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

    /// The seq and term of the newest log entry, or the floor if `log` has no
    /// entries (for example right after `install_snapshot`, or after `compact`
    /// removed everything up to the tail). `None` only for a log that never
    /// had an append or a snapshot install.
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

    /// The term recorded at `seq`, if known. Uses the floor when `seq` is the
    /// floor boundary and `log` has no entry there (it never does once a
    /// snapshot or compaction has moved past it).
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
    /// `Config` entry at or below it, or, if no such entry is left in `log`,
    /// what `compact` or `install_snapshot` last saved in `meta`. `None` only
    /// for a node that has never seen a configuration (not bootstrapped).
    ///
    /// Reads the log, not `versions`, on purpose. A configuration takes effect
    /// when it is appended, before commit, so it must be readable before
    /// `apply_committed` sees it. Raft §4.1: a server always uses the latest
    /// configuration in its log, committed or not. Waiting for commit would
    /// deadlock a membership change, because committing the new configuration
    /// needs a quorum of the new configuration.
    ///
    /// Scans backwards from `as_of_seq`, so it usually stops at the first
    /// entry it reads.
    pub fn config_at(&self, as_of_seq: u64) -> Result<Option<Vec<u8>>> {
        for item in self.log.range(..=be_bytes(as_of_seq)).rev() {
            let (_k, v) = item.into_inner()?;
            if let EntryPayload::Config(config) = decode_log_entry(&v)?.payload {
                return Ok(Some(config));
            }
        }
        self.base_config()
    }

    /// `config_at` for the log's tail: the configuration this node runs under
    /// now.
    pub fn current_config(&self) -> Result<Option<Vec<u8>>> {
        self.config_at(u64::MAX)
    }

    /// The last configuration removed from `log` by compaction or a
    /// snapshot.
    fn base_config(&self) -> Result<Option<Vec<u8>>> {
        Ok(self.meta.get(META_BASE_CONFIG)?.map(|b| b.to_vec()))
    }

    /// Flush `batch` (and start a new one) once it holds `WRITE_CHUNK` pending
    /// writes. This bounds a batch's memory, however large the whole operation
    /// is.
    fn flush_if_full(&self, batch: &mut OwnedWriteBatch, pending: &mut usize) -> Result<()> {
        if *pending >= WRITE_CHUNK {
            std::mem::replace(batch, self.db.batch()).commit()?;
            *pending = 0;
        }
        Ok(())
    }

    /// Remove every `log` entry with `seq < below_seq`.
    ///
    /// `below_seq` is first clamped to the applied watermark. `compact` removes
    /// `log` rows, so it must never remove one that `apply_committed` has not
    /// yet copied to `versions`, or that data would be lost. Normally the clamp
    /// does nothing (`raft::spawn_storage_maintenance_task` always applies
    /// before it compacts); it is a second line of defense.
    ///
    /// Resumes from the previous call's threshold (persisted in `meta`), so
    /// each call only touches entries added since the last one. Writes go out
    /// in chunks of `WRITE_CHUNK`, not one huge batch.
    ///
    /// Also records the term of the entry at `below_seq - 1` (the last one
    /// visited; the log has no gaps) as the new floor. If this call empties
    /// `log` to its tail, `last_entry` and `term_at` still need an answer for
    /// that boundary, as after `install_snapshot`.
    pub fn compact(&self, below_seq: u64) -> Result<()> {
        // `apply_committed(applied)` has copied everything up to and including
        // `applied` into `versions`, so we may remove up to and including
        // `applied`: `below_seq` may be as high as `applied + 1` (this function
        // removes `seq < below_seq`), but no higher.
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
                // We are about to delete the entry that holds this configuration. Save it
                // in `meta`, so `current_config` can still answer when no `log` entry has
                // it. The order is ascending, so the last one seen is the newest.
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
