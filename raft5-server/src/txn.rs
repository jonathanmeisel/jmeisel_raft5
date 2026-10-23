//! Transaction state and the lock table, for `grpc::KvService`.
//!
//! This state lives in memory on the leader only. That is what makes the
//! design correct: only a leader can append to the log, so a transaction
//! whose leader is gone can never commit. Nothing here is replicated,
//! because replicated state could outlive the leader it belongs to.
//!
//! # Isolation
//!
//! Strict two-phase locking:
//! - A transactional read takes a shared lock and holds it to the end.
//! - A buffered write takes an exclusive lock, or upgrades the shared
//!   lock the same transaction holds.
//! - All locks are released together, after the commit is applied or
//!   the transaction ends.
//!
//! Two transactions that conflict on a key cannot interleave, so the
//! result is serializable. A transaction is the only way to write (see
//! `client.proto`), so every lock holder is a transaction, and a read
//! set always describes data that is still true.
//!
//! # Conflicts: nothing waits
//!
//! A request that cannot get a lock is refused at once. Nobody holds a
//! lock while it waits for another, so deadlock cannot occur.
//!
//! Fail-fast alone can starve: two transactions that take the same two
//! keys in opposite orders can abort each other forever. So conflicts
//! are settled by age (wound-wait). A transaction older than every
//! holder in its way *wounds* them (aborts them and takes the lock); a
//! younger one is refused. `Txn::priority` is a counter, so ages have no
//! ties.
//!
//! A transaction that has started to commit is never wounded: its
//! entry may already be in the log, and an appended entry cannot be
//! recalled.
//!
//! Result: the oldest open transaction is never refused by another open
//! one. A committing transaction can still refuse it, but a commit ends
//! in bounded time (one append and its apply), so a retry makes
//! progress. Every other transaction gets its turn by becoming the
//! oldest.
//!
//! # Deadlines
//!
//! A transaction past its deadline holds nothing. The next request that
//! collides with one of its locks releases them on the spot (see
//! `acquire`). If release waited for the sweeper, a dead transaction
//! would cause unexplained stalls on a busy key. The sweeper only
//! reclaims memory for transactions that nobody collides with.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{Duration, Instant};

/// The longest time a transaction may hold its locks.
/// A request for more is refused, not clamped: the caller would act
/// as if it had the longer lease.
///
/// Two limits pull on this value:
/// - Every write is a transaction, so a long cap lets a stuck client
///   block a hot key for that long.
/// - A max-size transaction (`MAX_TRANSACTION_BYTES`) is sent as many
///   `Write` calls that must all land before it expires. 15s needs
///   about 700 KB/s; 5s would need 2 MB/s.
///
/// If you raise `MAX_TRANSACTION_BYTES`, raise this too, or large
/// transactions time out mid-upload with an error that hides the cause.
pub const MAX_TRANSACTION_TTL: Duration = Duration::from_secs(15);

/// The most transactions open on this node at once. Each one holds
/// memory and locks, so there must be a limit. Without it, a client
/// that retries in a loop leaks one transaction per attempt until each
/// TTL runs out.
pub const MAX_OPEN_TRANSACTIONS: usize = 1024;

/// The most data one transaction may buffer.
///
/// This protects the log, not memory. A transaction commits as one log
/// entry. An entry too large to replicate never commits, and because
/// the leader keeps sending it, no later entry commits either. So the
/// only safe place to refuse it is here, before it reaches the log.
/// `raft` sizes everything downstream so that an entry this large
/// always replicates, and a static assert there checks it.
///
/// Counted in raw key and value bytes. The real entry is a little
/// larger; the gap to `raft::MAX_ENTRY_BYTES` covers that.
///
/// There is no separate key-count limit. `entry_size` charges each key
/// an overhead, so many tiny keys still add up.
pub const MAX_TRANSACTION_BYTES: usize = raft5_shared::MAX_TRANSACTION_BYTES;

/// The most data all open transactions on this node may buffer together.
///
/// The per-transaction cap times `MAX_OPEN_TRANSACTIONS` is gigabytes,
/// all held on the leader before anything is proposed. This is the
/// number that limits how much memory clients can make this node hold.
///
/// It does not count everything: a leader also buffers replication in
/// flight (about `MAX_REPLICATION_BATCH_BYTES` per peer, twice over),
/// and a follower buffers reassembly. Budget for this plus a few
/// hundred megabytes.
pub const MAX_BUFFERED_BYTES_NODE_WIDE: usize = 256 * 1024 * 1024;

/// An extra charge per buffered key, so that a million empty keys are
/// not free. About what a key costs in map overhead plus entry framing.
const PER_KEY_OVERHEAD: usize = 64;

/// Server-generated and random, not sequential, so a client with a
/// stale id cannot reach another client's transaction.
pub type TxnId = [u8; 16];

/// What a transaction buffered, in key order. `None` is a delete.
pub type BufferedWrites = Vec<(Vec<u8>, Option<Vec<u8>>)>;

/// The keys a transaction buffered, in order, and whether each is a
/// delete. No values: see `Locks::buffered_from`.
pub type BufferedKeys = Vec<(Vec<u8>, bool)>;

/// The access a request wants, and the access a lock grants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Read,
    Write,
}

/// The lock on one key.
#[derive(Debug)]
enum LockState {
    /// Shared, held by every transaction in the set.
    ///
    /// A set, not a reference count. With a count, a transaction that reads
    /// a key twice counts as two readers, so "I am the only reader, so I
    /// may upgrade" fails for read-then-write, the most common shape. With
    /// a set, a second acquire does nothing, "only me" is
    /// `holders == {me}`, and a release cannot leave a phantom holder.
    Read { holders: HashSet<TxnId> },
    Write { owner: TxnId },
}

/// Why a transaction can no longer be used. Kept on the transaction, so
/// that its owner learns the reason on the next request instead of
/// hearing that the transaction never existed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbortReason {
    /// Its deadline passed.
    Expired,
    /// An older transaction took one of its locks. Nothing was written, so
    /// a replay is safe.
    Wounded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnState {
    Open,
    /// Its writes are going to the log. From here it cannot be wounded or
    /// expired, because the entry may already exist.
    Committing,
    Aborted(AbortReason),
}

#[derive(Debug)]
pub struct Txn {
    /// Lower is older, and older wins. A counter, not a timestamp: it gives
    /// a total order with no ties and does not depend on clock resolution.
    priority: u64,
    deadline: Instant,
    /// The leader term this transaction was created under. A transaction
    /// does not survive a leader change: see `Locks::observe_term`.
    pub term: u64,
    reads: HashSet<Vec<u8>>,
    /// Buffered only: nothing here goes to raft until the commit. `None`
    /// is a delete.
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    buffered_bytes: usize,
    state: TxnState,
}

/// Why a request did not get the lock it asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refused {
    /// A holder this requester may not wound. The key is the only part a
    /// caller can act on.
    Conflict { key: Vec<u8> },
    /// The transaction can no longer be used.
    Aborted(AbortReason),
    /// No transaction has this id: it was never created, or its tombstone
    /// is already reclaimed.
    Unknown,
    /// The leader changed. The reads were checked against the old leader,
    /// so the transaction cannot finish anywhere else.
    LeaderChanged,
    /// Refused before any attempt: too many transactions are open, or this
    /// one has buffered too much.
    TooMuch(&'static str),
}

/// All shared state, behind one lock. A synchronous `Mutex` that is
/// never held across `.await`, as for other small shared maps in this
/// crate.
#[derive(Debug, Default)]
struct Shared {
    next_priority: u64,
    /// The leader term the table reflects, so that a change is noticed.
    /// `None` before this node has been leader.
    term: Option<u64>,
    txns: HashMap<TxnId, Txn>,
    locks: HashMap<Vec<u8>, LockState>,
    /// The sum of `buffered_bytes` over all transactions that still hold a
    /// buffer, checked against `MAX_BUFFERED_BYTES_NODE_WIDE`. Only
    /// `buffer_write` and `forget_buffer` change it.
    buffered_total: usize,
}

impl Shared {
    /// Give `owner` `mode` on `key`, or say why not.
    ///
    /// The only place that inserts into `locks`, and the only place that
    /// holds the conflict rules. Reads and writes both come here, so there
    /// is one definition of a conflict.
    fn acquire(&mut self, key: &[u8], owner: TxnId, mode: Mode, now: Instant) -> Result<(), Refused> {
        loop {
            let blockers = self.blockers(key, owner, mode);
            if blockers.is_empty() {
                self.install(key, owner, mode);
                return Ok(());
            }

            // Release expired holders first. It is cheaper than wounding, and the
            // result does not depend on who asks.
            let mut released_any = false;
            for blocker in &blockers {
                if self.is_expired_open(*blocker, now) {
                    self.abort(*blocker, AbortReason::Expired);
                    released_any = true;
                }
            }
            if released_any {
                continue;
            }

            // Wound only when this transaction is older than every holder still in
            // its way.
            if let Some(mine) = self.txns.get(&owner).map(|t| t.priority)
                && blockers.iter().all(|b| self.is_woundable(*b, mine))
            {
                for blocker in &blockers {
                    self.abort(*blocker, AbortReason::Wounded);
                }
                continue;
            }

            return Err(Refused::Conflict { key: key.to_vec() });
        }
    }

    /// Who holds `key` in a way that conflicts with `owner` wanting `mode`.
    /// The requester never blocks itself: re-reading or rewriting its own
    /// key is not a conflict.
    fn blockers(&self, key: &[u8], owner: TxnId, mode: Mode) -> Vec<TxnId> {
        match (self.locks.get(key), mode) {
            (None, _) => Vec::new(),
            // Shared with shared: no conflict.
            (Some(LockState::Read { .. }), Mode::Read) => Vec::new(),
            // Shared with exclusive: everyone but the requester is in the way.
            // An empty result is the upgrade case.
            (Some(LockState::Read { holders }), Mode::Write) => {
                holders.iter().copied().filter(|h| *h != owner).collect()
            }
            (Some(LockState::Write { owner: held }), _) => {
                if *held == owner {
                    Vec::new()
                } else {
                    vec![*held]
                }
            }
        }
    }

    /// Record the grant. Called only when there are no blockers, so
    /// replacing a shared lock with an exclusive one here is the upgrade:
    /// the requester is then the only holder.
    fn install(&mut self, key: &[u8], owner: TxnId, mode: Mode) {
        match mode {
            Mode::Read => match self.locks.get_mut(key) {
                Some(LockState::Read { holders }) => {
                    holders.insert(owner);
                }
                // The requester already holds this key exclusively. A shared lock on
                // top adds nothing, and a downgrade would silently lose exclusion.
                Some(LockState::Write { .. }) => {}
                None => {
                    self.locks.insert(
                        key.to_vec(),
                        LockState::Read {
                            holders: HashSet::from([owner]),
                        },
                    );
                }
            },
            Mode::Write => {
                self.locks
                    .insert(key.to_vec(), LockState::Write { owner });
            }
        }
    }

    /// Drop `owner`'s hold on `key`, if it still has one.
    ///
    /// The only place that removes from `locks`. Every release (commit,
    /// abort, sweeper) comes here, because the owner check makes a late
    /// release safe: a plain remove could delete a lock that someone else
    /// has taken since.
    fn release(&mut self, key: &[u8], owner: TxnId) {
        let Some(state) = self.locks.get_mut(key) else {
            return;
        };
        let now_empty = match state {
            LockState::Read { holders } => {
                holders.remove(&owner);
                holders.is_empty()
            }
            LockState::Write { owner: held } => *held == owner,
        };
        if now_empty {
            self.locks.remove(key);
        }
    }

    fn is_expired_open(&self, id: TxnId, now: Instant) -> bool {
        self.txns
            .get(&id)
            .is_some_and(|t| t.state == TxnState::Open && t.deadline <= now)
    }

    /// Whether a transaction of age `mine` may push `blocker` aside. See
    /// the module doc for why `Committing` is immune.
    fn is_woundable(&self, blocker: TxnId, mine: u64) -> bool {
        self.txns
            .get(&blocker)
            .is_some_and(|t| t.state == TxnState::Open && t.priority > mine)
    }

    /// Mark a transaction as unusable and release all it holds.
    ///
    /// The transaction stays in the map as a tombstone, so that its owner's
    /// next request learns why. `sweep` reclaims it later.
    fn abort(&mut self, id: TxnId, reason: AbortReason) {
        let Some(txn) = self.txns.get_mut(&id) else {
            return;
        };
        if txn.state != TxnState::Open {
            return;
        }
        txn.state = TxnState::Aborted(reason);
        let held: Vec<Vec<u8>> = txn.reads.iter().chain(txn.writes.keys()).cloned().collect();
        for key in held {
            self.release(&key, id);
        }
        // Its buffer will never be written, so it stops counting against the
        // node budget now. The tombstone keeps only the reason.
        self.forget_buffer(id);
    }

    /// Drop a transaction's buffered writes and the budget they held.
    ///
    /// Safe to call twice. This and `buffer_write` are the only code that
    /// changes `buffered_total`, so the two cannot drift apart.
    fn forget_buffer(&mut self, id: TxnId) {
        let Some(txn) = self.txns.get_mut(&id) else {
            return;
        };
        self.buffered_total = self.buffered_total.saturating_sub(txn.buffered_bytes);
        txn.buffered_bytes = 0;
        txn.writes.clear();
    }

    /// Release all that `id` holds without changing its state, for a
    /// transaction that ends normally.
    fn release_all(&mut self, id: TxnId) {
        let Some(txn) = self.txns.get(&id) else {
            return;
        };
        let held: Vec<Vec<u8>> = txn.reads.iter().chain(txn.writes.keys()).cloned().collect();
        for key in held {
            self.release(&key, id);
        }
        self.forget_buffer(id);
    }
}

/// The handle the service holds. Cheap to clone; all clones share one
/// table.
#[derive(Clone, Debug, Default)]
pub struct Locks {
    shared: std::sync::Arc<std::sync::Mutex<Shared>>,
}

impl Locks {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Shared> {
        self.shared.lock().unwrap()
    }

    /// Record the leader term this node serves under. If it changed,
    /// discard every transaction and every lock.
    ///
    /// A transaction belongs to the leader that created it: its reads were
    /// checked against that leader, and only that leader can commit it.
    /// After a term change, its locks would only block keys for
    /// transactions that can never commit.
    ///
    /// Safe while a commit is in flight: its `CommitGuard` releases through
    /// `discard`, which finds the transaction gone and does nothing. It
    /// does not remove locks that now belong to someone else.
    pub fn observe_term(&self, term: Option<u64>) {
        let mut shared = self.lock();
        if shared.term == term {
            return;
        }
        shared.term = term;
        shared.txns.clear();
        shared.locks.clear();
        shared.buffered_total = 0;
    }

    /// Reclaim expired transactions and the locks they still hold.
    ///
    /// A backstop only: `acquire` releases an expired transaction's locks
    /// as soon as someone collides with them. This handles transactions
    /// that nobody collides with, which cost memory but not progress.
    /// Tombstones stay for a grace period past their deadline, so their
    /// owners can still learn why.
    pub fn sweep(&self, now: Instant, tombstone_grace: Duration) {
        let mut shared = self.lock();
        let expired: Vec<TxnId> = shared
            .txns
            .iter()
            .filter(|(_, t)| t.state == TxnState::Open && t.deadline <= now)
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            shared.abort(id, AbortReason::Expired);
        }
        shared
            .txns
            .retain(|_, t| t.state == TxnState::Open || t.deadline + tombstone_grace > now);
    }

    /// Open a transaction under leader term `term`, and return its id.
    pub fn create(&self, term: u64, ttl: Duration, id: TxnId) -> Result<TxnId, Refused> {
        if ttl > MAX_TRANSACTION_TTL {
            return Err(Refused::TooMuch("the requested transaction lifetime is too long"));
        }
        let mut shared = self.lock();
        if shared.txns.len() >= MAX_OPEN_TRANSACTIONS {
            return Err(Refused::TooMuch("too many transactions are open on this node"));
        }
        let priority = shared.next_priority;
        shared.next_priority += 1;
        shared.txns.insert(
            id,
            Txn {
                priority,
                deadline: Instant::now() + ttl,
                term,
                reads: HashSet::new(),
                writes: BTreeMap::new(),
                buffered_bytes: 0,
                state: TxnState::Open,
            },
        );
        Ok(id)
    }

    /// Run `f` on a usable transaction, or say why there is none. Every
    /// transactional request starts here, so the owner of an aborted
    /// transaction learns the reason instead of "unknown id".
    fn with_open<T>(
        &self,
        id: TxnId,
        f: impl FnOnce(&mut Shared) -> Result<T, Refused>,
    ) -> Result<T, Refused> {
        let mut shared = self.lock();
        match shared.txns.get(&id).map(|t| t.state) {
            None => Err(Refused::Unknown),
            Some(TxnState::Aborted(reason)) => Err(Refused::Aborted(reason)),
            // A second commit of a committing transaction is a caller error, not a
            // state to report.
            Some(TxnState::Committing) => Err(Refused::Unknown),
            Some(TxnState::Open) => f(&mut shared),
        }
    }

    /// Take read locks on all of `keys` for `id`, and add them to the read
    /// set.
    ///
    /// Returns, for each position, what this transaction has buffered for
    /// that key (read-your-own-writes): `Some(None)` is a buffered delete,
    /// `None` is a key it has not written.
    ///
    /// If `want_values` is false, every position is `None` and nothing is
    /// cloned. A versions-only read or a key scan cannot use a buffered
    /// value, and a clone could cost megabytes.
    ///
    /// Not atomic, by design: if a key is refused part-way, the locks
    /// already taken stay. They are part of the read set and are released
    /// when the transaction ends. Undoing them would not help: this
    /// transaction may have wounded another one to get them.
    ///
    /// A key this transaction already wrote takes no read lock: it holds
    /// the stronger exclusive lock, and a shared lock on top would be an
    /// upgrade against itself.
    pub fn read_many(
        &self,
        id: TxnId,
        keys: &[Vec<u8>],
        want_values: bool,
    ) -> Result<Vec<Option<Option<Vec<u8>>>>, Refused> {
        let now = Instant::now();
        self.with_open(id, |shared| {
            let mut buffered = Vec::with_capacity(keys.len());
            for key in keys {
                if let Some(value) = shared.txns[&id].writes.get(key) {
                    buffered.push(want_values.then(|| value.clone()));
                    continue;
                }
                shared.acquire(key, id, Mode::Read, now)?;
                // `acquire` wounds this transaction only if it is younger than a
                // holder, and then it returns an error above. So the entry is still
                // here and still open.
                shared
                    .txns
                    .get_mut(&id)
                    .expect("an open transaction cannot vanish under its own read")
                    .reads
                    .insert(key.clone());
                buffered.push(None);
            }
            Ok(buffered)
        })
    }

    /// The keys at or after `start` that this transaction buffered, and
    /// whether each is a delete. A key scan uses this to merge its own
    /// uncommitted changes into what the store says.
    ///
    /// Keys only, so the cost scales with the key count, not the value
    /// bytes. Returns every buffered key from `start` on, not a bounded
    /// prefix: a scan must know every delete it might pass, or it returns a
    /// key this transaction has removed.
    pub fn buffered_from(&self, id: TxnId, start: &[u8]) -> Result<BufferedKeys, Refused> {
        self.with_open(id, |shared| {
            Ok(shared.txns[&id]
                .writes
                .range(start.to_vec()..)
                .map(|(key, value)| (key.clone(), value.is_none()))
                .collect())
        })
    }

    /// Take (or upgrade to) a write lock on `key` for `id`, and buffer
    /// `value` for it. `None` buffers a delete.
    pub fn buffer_write(
        &self,
        id: TxnId,
        key: &[u8],
        value: Option<Vec<u8>>,
    ) -> Result<(), Refused> {
        self.with_open(id, |shared| {
            let txn = &shared.txns[&id];
            // A rewrite of a buffered key replaces the old value, so the old size
            // comes out of both budgets.
            let previous = txn.writes.get(key).map(|v| entry_size(key, v.as_deref()));
            let size = entry_size(key, value.as_deref());
            let delta = size as isize - previous.unwrap_or(0) as isize;
            let projected = txn.buffered_bytes.saturating_add_signed(delta);
            let projected_node_wide = shared.buffered_total.saturating_add_signed(delta);

            if projected > MAX_TRANSACTION_BYTES {
                return Err(Refused::TooMuch("the transaction has buffered too much"));
            }
            // A separate check and message: this limit is not about the caller's
            // own transaction, and a smaller transaction may not help.
            if projected_node_wide > MAX_BUFFERED_BYTES_NODE_WIDE {
                return Err(Refused::TooMuch(
                    "this node is already holding as much buffered transaction data as it will",
                ));
            }

            shared.acquire(key, id, Mode::Write, Instant::now())?;

            shared.buffered_total = projected_node_wide;
            let txn = shared
                .txns
                .get_mut(&id)
                .expect("an open transaction cannot vanish under its own write");
            txn.writes.insert(key.to_vec(), value);
            txn.buffered_bytes = projected;
            Ok(())
        })
    }

    /// Move a transaction to `Committing`, and return its buffered writes
    /// in key order.
    ///
    /// From here it cannot be wounded or expired: its writes are about to
    /// go into the log, and nothing here can take them back. The caller
    /// must release it afterwards: see `CommitGuard`.
    pub fn begin_commit(
        &self,
        id: TxnId,
        term: u64,
    ) -> Result<(BufferedWrites, CommitGuard), Refused> {
        let now = Instant::now();
        let writes = self.with_open(id, |shared| {
            let txn = &shared.txns[&id];
            if txn.term != term {
                // The leader that checked this transaction's reads is not the leader
                // asked to commit it. `Locks::observe_term` normally discards it first,
                // so this rarely fires. It stays because a wrong guess at commit cannot
                // be undone.
                shared.abort(id, AbortReason::Expired);
                return Err(Refused::LeaderChanged);
            }
            if txn.deadline <= now {
                shared.abort(id, AbortReason::Expired);
                return Err(Refused::Aborted(AbortReason::Expired));
            }
            let writes = txn
                .writes
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            shared
                .txns
                .get_mut(&id)
                .expect("checked just above")
                .state = TxnState::Committing;
            Ok(writes)
        })?;
        Ok((
            writes,
            CommitGuard {
                locks: self.clone(),
                id,
            },
        ))
    }

    /// Give up a transaction and all it holds. Safe to call twice, and
    /// quiet about an unknown id: commit-then-drop is the normal client
    /// path, and the drop should not need to know if it lost a race with
    /// its own commit.
    pub fn discard(&self, id: TxnId) {
        let mut shared = self.lock();
        shared.release_all(id);
        shared.txns.remove(&id);
    }

    /// The leader term this transaction was created under, if it is still
    /// usable.
    pub fn term_of(&self, id: TxnId) -> Result<u64, Refused> {
        self.with_open(id, |shared| Ok(shared.txns[&id].term))
    }

    #[cfg(test)]
    pub(crate) fn state_of(&self, id: TxnId) -> Option<TxnState> {
        self.lock().txns.get(&id).map(|t| t.state)
    }

    #[cfg(test)]
    pub(crate) fn is_locked(&self, key: &[u8]) -> bool {
        self.lock().locks.contains_key(key)
    }

    /// What all open transactions buffer together, against
    /// `MAX_BUFFERED_BYTES_NODE_WIDE`.
    #[cfg(test)]
    pub(crate) fn buffered_total(&self) -> usize {
        self.lock().buffered_total
    }

    #[cfg(test)]
    pub(crate) fn open_count(&self) -> usize {
        self.lock()
            .txns
            .values()
            .filter(|t| t.state == TxnState::Open)
            .count()
    }
}

/// The approximate cost of one buffered mutation, for the size cap. An
/// estimate on purpose: it guards the encoded entry size, and being
/// roughly right well below the real limit is better than being exact
/// at it.
fn entry_size(key: &[u8], value: Option<&[u8]>) -> usize {
    key.len() + value.map_or(0, <[u8]>::len) + PER_KEY_OVERHEAD
}

/// The locks of a committing transaction, released when the commit
/// ends, however it ends.
///
/// A `Drop` impl, not a line at the end of the commit path, so that an
/// error, a panic, or a dropped task also releases them. A transaction
/// in `Committing` never expires, so a leaked lock would last until the
/// term ends.
pub struct CommitGuard {
    locks: Locks,
    id: TxnId,
}

impl Drop for CommitGuard {
    fn drop(&mut self) {
        self.locks.discard(self.id);
    }
}

/// A new random transaction id.
///
/// Each `RandomState` gets a random seed, so a new hasher's output is
/// already random. `raft::random_jitter_millis` uses the same trick, to
/// avoid a `rand` dependency.
pub fn fresh_id() -> TxnId {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    let mut id = [0u8; 16];
    id[..8].copy_from_slice(&RandomState::new().build_hasher().finish().to_ne_bytes());
    id[8..].copy_from_slice(&RandomState::new().build_hasher().finish().to_ne_bytes());
    id
}

#[cfg(test)]
mod tests;
