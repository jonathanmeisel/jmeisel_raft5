//! Transaction state and the lock table, for `grpc::KvService`.
//!
//! In memory, on the leader, and nowhere else. That is not a shortcut --
//! it is what makes the scheme correct. A transaction can only commit by
//! appending to the log, which only a leader can do, so a transaction
//! whose leader is gone cannot commit no matter what any other node
//! believes. Nothing here needs to be replicated, and replicating it
//! would only create state that could outlive the thing it describes.
//!
//! # Isolation
//!
//! Strict two-phase locking. A transactional read takes a shared lock on
//! the key and holds it to the end; a buffered write takes an exclusive
//! one, or upgrades the shared lock the same transaction already holds;
//! every lock is released together, after the commit has been applied (or
//! the transaction has gone away). Two transactions that touch a key in
//! conflicting ways therefore cannot interleave, which is what makes the
//! result serializable.
//!
//! An ordinary single-key `Write` takes an exclusive lock here too, as
//! `Owner::Bare`. It has to: a write that bypassed the table could change
//! a key out from under a transaction that had read it, and the read set
//! would be a record of something that was no longer true.
//!
//! # Conflicts: nothing ever waits
//!
//! A request that cannot have a lock is refused immediately. No caller
//! holds a lock while waiting for another, so there is no wait-for graph
//! and deadlock is not possible -- not detected and resolved, but absent.
//!
//! Fail-fast alone would be deadlock-free and still starve: two
//! transactions grabbing the same two keys in opposite orders can abort
//! each other forever, each retry as doomed as the last. So conflicts are
//! settled by age (wound-wait): a transaction older than everything
//! blocking it *wounds* those holders -- aborts them and takes the lock --
//! while a younger one is refused. Ages come from `Txn::priority`, a
//! counter, so they are a total order with no ties.
//!
//! Some holders are never wounded, all for one reason: their write may
//! already be in the raft log, and an appended entry cannot be recalled.
//! A transaction that has begun committing, and any `Owner::Bare` holder,
//! are immune. A bare write never wounds anything either -- it keeps the
//! fail-fast behaviour it has always had, and its client already retries.
//!
//! What that guarantees is worth stating exactly, because it is less than
//! "no transaction ever starves": the *oldest* open transaction is never
//! refused by another transaction, since it wounds them all. It can still
//! be refused by a bare write or by a committing transaction, but both of
//! those finish in bounded time -- one append plus its apply -- so a
//! retry makes progress. Every other transaction gets there by becoming
//! the oldest, which it does simply by outliving the ones ahead of it.
//!
//! # Deadlines
//!
//! Every transaction carries one, and a transaction past it holds
//! nothing: the next request to collide with one of its locks releases
//! them on the spot (`acquire` below), rather than waiting for a sweeper
//! tick. That matters more than it looks -- `raft5_lock`'s refresh gives
//! an attempt 400ms before it counts as failed, so lock-release latency
//! measured in sweeper periods would be a correctness problem for the
//! crate built on top of this one. The sweeper is only a reclaimer of
//! memory for transactions nobody happens to collide with.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{Duration, Instant};

/// The longest a transaction may hold its locks. A cap rather than a
/// clamp: a caller that asked for longer has made an assumption this
/// server will not honour, and silently giving it a shorter lease than it
/// asked for would let it act on that assumption anyway.
///
/// Short, because a transaction's locks block ordinary writes to the same
/// keys for as long as they are held, and `raft5_lock` -- built on those
/// ordinary writes -- gives up a lock lease after a second of failed
/// refreshes.
pub const MAX_TRANSACTION_TTL: Duration = Duration::from_secs(5);

/// How many transactions may be open on this node at once. Creating one
/// is a client asking the server to allocate memory and hold locks, so
/// there has to be a point at which the answer is no; without it a
/// client that retries in a loop leaks one per attempt until each TTL
/// runs out.
pub const MAX_OPEN_TRANSACTIONS: usize = 1024;

/// How many keys one transaction may buffer, and how many bytes.
///
/// These are not tidiness. A transaction commits as a single log entry,
/// which replicates inside a single `AppendEntries` message, which tonic
/// will refuse to decode past its 4MB default. An entry too large to
/// replicate never commits -- and, because it is already in the log and
/// the leader keeps re-proposing it, nothing after it ever commits
/// either. The only safe place to refuse it is here, before anything
/// reaches the log at all.
pub const MAX_TRANSACTION_KEYS: usize = 256;
pub const MAX_TRANSACTION_BYTES: usize = 1024 * 1024;

/// Opaque, server-generated, and random rather than sequential so that a
/// client holding a stale id cannot address a transaction that isn't
/// theirs.
pub type TxnId = [u8; 16];

/// What a transaction buffered, in key order. `None` is a delete.
pub type BufferedWrites = Vec<(Vec<u8>, Option<Vec<u8>>)>;

/// Who holds a lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Owner {
    Txn(TxnId),
    /// A plain single-key `Write` RPC. Has no age and is never wounded --
    /// see the module doc.
    Bare(u64),
}

/// What kind of access a request wants, and what a lock grants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Read,
    Write,
}

/// One key's lock.
#[derive(Debug)]
enum LockState {
    /// Shared, held by everyone in the set.
    ///
    /// A set of owners rather than a reference count, which is the
    /// obvious encoding and the wrong one: a transaction that reads the
    /// same key twice would bump a count to two while its read set stayed
    /// at one entry, and then "the only reader is me, so I may upgrade"
    /// would be false for a transaction that *is* the only reader.
    /// Read-then-write is the commonest shape a transaction has. With a
    /// set, acquiring twice is idempotent, "only me" is exactly
    /// `holders == {me}`, and releasing cannot leak a phantom holder.
    Read { holders: HashSet<Owner> },
    Write { owner: Owner },
}

/// Why a transaction is no longer usable. Kept on the transaction rather
/// than deleting it, so its owner finds out *why* on their next request
/// instead of being told it never existed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbortReason {
    /// Its deadline passed.
    Expired,
    /// An older transaction took one of its locks. Nothing was written,
    /// so replaying it is safe.
    Wounded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnState {
    Open,
    /// Its writes are on their way to the log. Past this point it cannot
    /// be wounded or expired: the entry may already exist.
    Committing,
    Aborted(AbortReason),
}

#[derive(Debug)]
pub struct Txn {
    /// Lower is older, and older wins. A counter rather than a timestamp:
    /// a total order with no ties and no dependence on clock resolution,
    /// and "older" here only ever means "created earlier on this leader".
    priority: u64,
    deadline: Instant,
    /// The leader term this was created under. A transaction does not
    /// survive a change of leader -- see `Locks::wipe`.
    pub term: u64,
    reads: HashSet<Vec<u8>>,
    /// Buffered, not written: nothing here has been proposed to raft
    /// until the transaction commits. `None` is a delete.
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    buffered_bytes: usize,
    state: TxnState,
}

/// Why a request could not have the lock it asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refused {
    /// A holder this requester is not allowed to wound. Naming the key is
    /// the only part of this a caller can act on.
    Conflict { key: Vec<u8> },
    /// The transaction is no longer usable.
    Aborted(AbortReason),
    /// No transaction by that id -- never created, or reclaimed long
    /// enough ago that even the tombstone is gone.
    Unknown,
    /// The leader changed under it. Its reads were validated against the
    /// leader that served them, so it cannot be finished anywhere else.
    LeaderChanged,
    /// The request was refused before it could be attempted: too many
    /// transactions open, or too much buffered in this one.
    TooMuch(&'static str),
}

/// Everything under one lock. A synchronous `Mutex` with no `.await`
/// beneath it, the same discipline the rest of this crate uses for
/// small shared maps.
#[derive(Debug, Default)]
struct Shared {
    /// Bumped whenever this node stops being the leader it was. Every
    /// lock release is checked against the epoch it was taken in, so a
    /// release arriving late from a previous term cannot remove a lock
    /// that now belongs to someone else.
    epoch: u64,
    next_priority: u64,
    next_bare_id: u64,
    /// The leader term the table currently reflects, so a change can be
    /// noticed. `None` before this node has led at all.
    term: Option<u64>,
    txns: HashMap<TxnId, Txn>,
    locks: HashMap<Vec<u8>, LockState>,
}

impl Shared {
    /// Grant `owner` `mode` on `key`, or say why not.
    ///
    /// The single place `locks` is ever inserted into, and the only place
    /// the conflict rules live. Every caller -- transactional read,
    /// buffered write, plain write -- comes through here, so there is one
    /// description of what conflicts with what rather than three that
    /// have to agree.
    fn acquire(&mut self, key: &[u8], owner: Owner, mode: Mode, now: Instant) -> Result<(), Refused> {
        loop {
            let blockers = self.blockers(key, owner, mode);
            if blockers.is_empty() {
                self.install(key, owner, mode);
                return Ok(());
            }

            // Expired holders first: cheaper than wounding, and unlike
            // wounding it doesn't depend on who is asking.
            let mut released_any = false;
            for blocker in &blockers {
                if let Owner::Txn(id) = blocker
                    && self.is_expired_open(*id, now)
                {
                    self.abort(*id, AbortReason::Expired);
                    released_any = true;
                }
            }
            if released_any {
                continue;
            }

            // Wound: only a transaction wounds, and only one older than
            // every holder still in its way.
            if let Owner::Txn(me) = owner
                && let Some(mine) = self.txns.get(&me).map(|t| t.priority)
                && blockers.iter().all(|b| self.is_woundable(*b, mine))
            {
                for blocker in &blockers {
                    if let Owner::Txn(id) = blocker {
                        self.abort(*id, AbortReason::Wounded);
                    }
                }
                continue;
            }

            return Err(Refused::Conflict { key: key.to_vec() });
        }
    }

    /// Who currently holds `key` in a way that conflicts with `owner`
    /// wanting `mode`. The requester is never its own blocker: a
    /// transaction re-reading a key it has read, or rewriting a key it
    /// has written, is not in conflict with itself.
    fn blockers(&self, key: &[u8], owner: Owner, mode: Mode) -> Vec<Owner> {
        match (self.locks.get(key), mode) {
            (None, _) => Vec::new(),
            // Shared against shared: no conflict, whoever else holds it.
            (Some(LockState::Read { .. }), Mode::Read) => Vec::new(),
            // Shared against exclusive: everyone but the requester is in
            // the way. An empty result here is the upgrade case.
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

    /// Record the grant. Only ever called with no blockers, so replacing
    /// a shared lock with an exclusive one here is the upgrade -- by then
    /// the requester is provably its only holder.
    fn install(&mut self, key: &[u8], owner: Owner, mode: Mode) {
        match mode {
            Mode::Read => match self.locks.get_mut(key) {
                Some(LockState::Read { holders }) => {
                    holders.insert(owner);
                }
                // A key this requester already holds exclusively needs no
                // shared lock on top: the exclusive one is stronger, and
                // downgrading it here would be a silent loss of exclusion.
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
    /// The only place `locks` is ever removed from. Every release goes
    /// through here -- a guard's drop, a commit, an abort, the sweeper --
    /// because the owner check is what makes a *late* release safe: a
    /// release that just removed the key would happily delete a lock
    /// somebody else has since taken.
    fn release(&mut self, key: &[u8], owner: Owner) {
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

    /// Whether `blocker` can be pushed out of the way by a transaction of
    /// age `mine`. See the module doc for why `Committing` and `Bare` are
    /// immune.
    fn is_woundable(&self, blocker: Owner, mine: u64) -> bool {
        match blocker {
            Owner::Bare(_) => false,
            Owner::Txn(id) => self
                .txns
                .get(&id)
                .is_some_and(|t| t.state == TxnState::Open && t.priority > mine),
        }
    }

    /// Mark a transaction unusable and give up everything it held.
    ///
    /// The transaction itself stays in the map as a tombstone so its
    /// owner's next request can be told *why* rather than "no such
    /// transaction"; `sweep` reclaims it later.
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
            self.release(&key, Owner::Txn(id));
        }
    }

    /// Release everything `id` holds without changing its state -- for a
    /// transaction that is finishing normally rather than being aborted.
    fn release_all(&mut self, id: TxnId) {
        let Some(txn) = self.txns.get(&id) else {
            return;
        };
        let held: Vec<Vec<u8>> = txn.reads.iter().chain(txn.writes.keys()).cloned().collect();
        for key in held {
            self.release(&key, Owner::Txn(id));
        }
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

    /// Note the leader term this node is serving under, discarding every
    /// transaction and every transactional lock if it has changed.
    ///
    /// A transaction belongs to the leader that created it: its reads
    /// were validated against that leader's state and its commit can only
    /// happen there. A new term means none of that holds any more, and
    /// keeping the locks would only make keys unwritable on behalf of
    /// transactions that can never commit.
    ///
    /// Bumping `epoch` is what makes this safe to do while work is still
    /// in flight: a lock released after the wipe is checked against the
    /// epoch it was taken in and quietly does nothing, instead of
    /// removing a lock that now belongs to somebody else.
    pub fn observe_term(&self, term: Option<u64>) {
        let mut shared = self.lock();
        if shared.term == term {
            return;
        }
        shared.term = term;
        shared.epoch += 1;
        shared.txns.clear();
        shared.locks.clear();
    }

    /// Reclaim expired transactions and the locks they still hold.
    ///
    /// A backstop, not the mechanism: `acquire` already releases an
    /// expired transaction's locks the moment anyone collides with them,
    /// so this only matters for a transaction nobody happens to want
    /// anything from -- which costs memory rather than progress.
    /// Tombstones are reclaimed a grace period past their deadline, late
    /// enough that their owner has had a chance to be told why.
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

    /// Claim `key` for one plain single-key write, or say why not.
    ///
    /// The guard releases the claim when it is dropped, which the caller
    /// arranges to happen when the *entry* resolves rather than when the
    /// request does -- see `grpc::KvService::conditional_write`.
    pub fn claim(&self, key: &[u8]) -> Result<BareGuard, Refused> {
        let mut shared = self.lock();
        let id = shared.next_bare_id;
        shared.next_bare_id += 1;
        let owner = Owner::Bare(id);
        shared.acquire(key, owner, Mode::Write, Instant::now())?;
        Ok(BareGuard {
            locks: self.clone(),
            epoch: shared.epoch,
            key: key.to_vec(),
            owner,
        })
    }

    /// Open a transaction under leader term `term`, returning its id.
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

    /// Run `f` against a usable transaction, or say why there isn't one.
    /// Every transactional request starts here, which is how an aborted
    /// transaction's owner learns the reason rather than being told the
    /// id is unknown.
    fn with_open<T>(
        &self,
        id: TxnId,
        f: impl FnOnce(&mut Shared) -> Result<T, Refused>,
    ) -> Result<T, Refused> {
        let mut shared = self.lock();
        match shared.txns.get(&id).map(|t| t.state) {
            None => Err(Refused::Unknown),
            Some(TxnState::Aborted(reason)) => Err(Refused::Aborted(reason)),
            // A second commit on a transaction already committing is the
            // caller's mistake, not a state to be shared.
            Some(TxnState::Committing) => Err(Refused::Unknown),
            Some(TxnState::Open) => f(&mut shared),
        }
    }

    /// Take a read lock on `key` for `id` and record it in the read set.
    ///
    /// Returns the value this transaction has already buffered for the
    /// key, if any -- read-your-own-writes, answered from the buffer
    /// because the log knows nothing about it yet.
    pub fn read(&self, id: TxnId, key: &[u8]) -> Result<Option<Option<Vec<u8>>>, Refused> {
        self.with_open(id, |shared| {
            if let Some(buffered) = shared.txns[&id].writes.get(key) {
                return Ok(Some(buffered.clone()));
            }
            shared.acquire(key, Owner::Txn(id), Mode::Read, Instant::now())?;
            // `acquire` can wound *this* transaction only if it is
            // younger than a holder, in which case it returned an error
            // above; so the entry is still here and still open.
            shared
                .txns
                .get_mut(&id)
                .expect("an open transaction cannot vanish under its own read")
                .reads
                .insert(key.to_vec());
            Ok(None)
        })
    }

    /// Take (or upgrade to) a write lock on `key` for `id` and buffer
    /// `value` against it. `None` buffers a delete.
    pub fn buffer_write(
        &self,
        id: TxnId,
        key: &[u8],
        value: Option<Vec<u8>>,
    ) -> Result<(), Refused> {
        self.with_open(id, |shared| {
            let txn = &shared.txns[&id];
            let previous = txn.writes.get(key).map(|v| entry_size(key, v.as_deref()));
            let size = entry_size(key, value.as_deref());
            let projected = txn.buffered_bytes + size - previous.unwrap_or(0);
            let keys = txn.writes.len() + usize::from(previous.is_none());
            if keys > MAX_TRANSACTION_KEYS {
                return Err(Refused::TooMuch("the transaction has too many keys"));
            }
            if projected > MAX_TRANSACTION_BYTES {
                return Err(Refused::TooMuch("the transaction has buffered too much"));
            }

            shared.acquire(key, Owner::Txn(id), Mode::Write, Instant::now())?;

            let txn = shared
                .txns
                .get_mut(&id)
                .expect("an open transaction cannot vanish under its own write");
            txn.writes.insert(key.to_vec(), value);
            txn.buffered_bytes = projected;
            Ok(())
        })
    }

    /// Move a transaction to `Committing` and hand back what it buffered,
    /// in key order.
    ///
    /// Past this point it can no longer be wounded or expired, which is
    /// the point: its writes are about to exist in the log, and nothing
    /// here could take them back. The caller owns releasing it afterwards
    /// -- see `CommitGuard`.
    pub fn begin_commit(
        &self,
        id: TxnId,
        term: u64,
    ) -> Result<(BufferedWrites, CommitGuard), Refused> {
        let now = Instant::now();
        let writes = self.with_open(id, |shared| {
            let txn = &shared.txns[&id];
            if txn.term != term {
                // The leader that validated this transaction's reads is
                // not the leader being asked to commit it. (Normally
                // `Locks::observe_term` has already discarded it and
                // this never fires; it is here because a commit is the
                // one request where guessing wrong is unrecoverable.)
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

    /// Give up a transaction and everything it holds. Idempotent, and
    /// silent about an id it doesn't know: committing and then dropping
    /// is the ordinary client path, and the drop shouldn't have to know
    /// whether it lost the race with its own commit.
    pub fn discard(&self, id: TxnId) {
        let mut shared = self.lock();
        shared.release_all(id);
        shared.txns.remove(&id);
    }

    /// The leader term this transaction was created under, and whether
    /// it is still usable at all.
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

    #[cfg(test)]
    pub(crate) fn open_count(&self) -> usize {
        self.lock()
            .txns
            .values()
            .filter(|t| t.state == TxnState::Open)
            .count()
    }
}

/// Roughly what one buffered mutation costs, for the size cap. An
/// estimate on purpose: what it is guarding is the encoded size of the
/// log entry, and being approximately right well below the real ceiling
/// is worth more than being exactly right at it.
fn entry_size(key: &[u8], value: Option<&[u8]>) -> usize {
    key.len() + value.map_or(0, <[u8]>::len)
}

/// One plain write's exclusive claim on a key, released on drop.
///
/// Held from before the write's precondition is read until its entry has
/// been applied -- see `grpc`'s module doc for why that whole span
/// matters.
pub struct BareGuard {
    locks: Locks,
    /// The epoch the claim was taken in. A release from a previous epoch
    /// does nothing: the table it belonged to is gone, and the key may
    /// since have been claimed by somebody else.
    epoch: u64,
    key: Vec<u8>,
    owner: Owner,
}

impl Drop for BareGuard {
    fn drop(&mut self) {
        let mut shared = self.locks.lock();
        if shared.epoch != self.epoch {
            return;
        }
        shared.release(&self.key, self.owner);
    }
}

/// A committing transaction's locks, released when the commit resolves
/// however it resolves.
///
/// A `Drop` impl rather than a line at the end of the commit path, so
/// that an error -- or a panic, or the task being dropped outright --
/// releases them too. A transaction stuck in `Committing` is exempt from
/// expiry by design, so locks leaked here would be held until the term
/// ended.
pub struct CommitGuard {
    locks: Locks,
    id: TxnId,
}

impl Drop for CommitGuard {
    fn drop(&mut self) {
        self.locks.discard(self.id);
    }
}

/// A fresh transaction id.
///
/// `RandomState` exists to give `HashMap` an unpredictable seed per
/// instance, so a fresh hasher's initial state is already random without
/// writing anything through it -- the same trick `raft::random_jitter_millis`
/// uses, and for the same reason: this does not need a `rand` dependency.
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
