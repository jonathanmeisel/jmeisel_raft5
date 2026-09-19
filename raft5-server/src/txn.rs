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
//! Every holder is a transaction, because a transaction is the only way
//! to write anything (see `client.proto`). Nothing can change a key out
//! from under a transaction that has read it, so a read set is always a
//! record of something still true.
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
//! One kind of holder is never wounded, for one reason: its writes may
//! already be in the raft log, and an appended entry cannot be recalled.
//! A transaction that has begun committing is therefore immune.
//!
//! What that guarantees: the *oldest* open transaction is never refused
//! by another open transaction, since it wounds them all. It can still be
//! refused by one already committing, but that finishes in bounded time
//! -- one append plus its apply -- so a retry makes progress. Every other
//! transaction gets there by becoming the oldest, which it does simply by
//! outliving the ones ahead of it.
//!
//! # Deadlines
//!
//! Every transaction carries one, and a transaction past it holds
//! nothing: the next request to collide with one of its locks releases
//! them on the spot (`acquire` below), rather than waiting for a sweeper
//! tick. Lock-release latency measured in sweeper periods would show up
//! as unexplained stalls on a contended key, when what is actually
//! holding it is a transaction that died some time ago. The sweeper is
//! only a reclaimer of memory for transactions nobody happens to collide
//! with.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{Duration, Instant};

/// The longest a transaction may hold its locks. A cap rather than a
/// clamp: a caller that asked for longer has made an assumption this
/// server will not honour, and silently giving it a shorter lease than it
/// asked for would let it act on that assumption anyway.
///
/// Pulled in two directions. A transaction's locks block everyone else
/// from the same keys for as long as they are held, and every write in
/// the system is a transaction, so a long cap means a stuck client can
/// make a hot key unwritable for that long. But a
/// `MAX_TRANSACTION_BYTES` transaction is uploaded as many `Write`
/// calls that must *all* land before this expires, or the locks go and
/// the work is wasted; at 10 MiB, five seconds would demand a sustained
/// 2 MB/s with nothing to spare. Fifteen sets that floor around
/// 700 KB/s, which is a bar any usable link clears.
///
/// So the two constants are related: raising `MAX_TRANSACTION_BYTES`
/// without raising this makes large transactions fail by timing out
/// mid-upload, for reasons that look nothing like their actual cause.
pub const MAX_TRANSACTION_TTL: Duration = Duration::from_secs(15);

/// How many transactions may be open on this node at once. Creating one
/// is a client asking the server to allocate memory and hold locks, so
/// there has to be a point at which the answer is no; without it a
/// client that retries in a loop leaks one per attempt until each TTL
/// runs out.
pub const MAX_OPEN_TRANSACTIONS: usize = 1024;

/// How much one transaction may buffer.
///
/// This is not tidiness, and it is not really about memory. A
/// transaction commits as a single log entry, and an entry too large to
/// replicate never commits -- and, because it is already in the log and
/// the leader keeps re-proposing it, nothing after it ever commits
/// either. The only safe place to refuse it is here, before anything
/// reaches the log at all. `raft`'s cap chain is the other end of that
/// argument: everything downstream is sized so that a transaction this
/// large always replicates, and a static assert there enforces it.
///
/// Counted in raw key and value bytes, which is a little under what the
/// entry actually costs on disk and on the wire; the headroom between
/// this and `raft::MAX_ENTRY_BYTES` is what covers the difference.
///
/// There is deliberately no separate limit on the number of keys. Bytes
/// are what cost memory and wire time, `entry_size` charges every key an
/// overhead so a great many tiny ones still add up, and a second limit
/// would only be another number to explain without bounding anything
/// this one doesn't.
pub const MAX_TRANSACTION_BYTES: usize = 10 * 1024 * 1024;

/// How much *every* open transaction on this node may buffer between
/// them.
///
/// The per-transaction cap alone bounds nothing useful: multiplied by
/// `MAX_OPEN_TRANSACTIONS` it permits gigabytes, all of it held on the
/// leader before a single byte has been proposed. This is the number
/// that actually decides how much memory a client can make this node
/// hold.
///
/// It is not the whole story: a leader is also buffering in-flight
/// replication (roughly `MAX_REPLICATION_BATCH_BYTES` per peer, twice
/// over between the encoded batch and the entries it came from) and a
/// follower its reassembly buffer, none of which is counted here.
/// Budget for something like this plus a few hundred megabytes, not for
/// this alone.
pub const MAX_BUFFERED_BYTES_NODE_WIDE: usize = 256 * 1024 * 1024;

/// Charged per buffered key on top of its bytes, so that a transaction
/// of a million empty keys is not free. Roughly what a key costs in map
/// overhead here plus framing in the entry it becomes.
const PER_KEY_OVERHEAD: usize = 64;

/// Opaque, server-generated, and random rather than sequential so that a
/// client holding a stale id cannot address a transaction that isn't
/// theirs.
pub type TxnId = [u8; 16];

/// What a transaction buffered, in key order. `None` is a delete.
pub type BufferedWrites = Vec<(Vec<u8>, Option<Vec<u8>>)>;

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
    Read { holders: HashSet<TxnId> },
    Write { owner: TxnId },
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
    next_priority: u64,
    /// The leader term the table currently reflects, so a change can be
    /// noticed. `None` before this node has led at all.
    term: Option<u64>,
    txns: HashMap<TxnId, Txn>,
    locks: HashMap<Vec<u8>, LockState>,
    /// The sum of `buffered_bytes` over every transaction still holding
    /// its buffer, against `MAX_BUFFERED_BYTES_NODE_WIDE`. Maintained by
    /// `buffer_write` and `forget_buffer`, which are the only two places
    /// a transaction's buffer grows or goes away.
    buffered_total: usize,
}

impl Shared {
    /// Grant `owner` `mode` on `key`, or say why not.
    ///
    /// The single place `locks` is ever inserted into, and the only place
    /// the conflict rules live. Both callers -- transactional read and
    /// buffered write -- come through here, so there is one description
    /// of what conflicts with what rather than two that have to agree.
    fn acquire(&mut self, key: &[u8], owner: TxnId, mode: Mode, now: Instant) -> Result<(), Refused> {
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
                if self.is_expired_open(*blocker, now) {
                    self.abort(*blocker, AbortReason::Expired);
                    released_any = true;
                }
            }
            if released_any {
                continue;
            }

            // Wound: only a transaction older than every holder still in
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

    /// Who currently holds `key` in a way that conflicts with `owner`
    /// wanting `mode`. The requester is never its own blocker: a
    /// transaction re-reading a key it has read, or rewriting a key it
    /// has written, is not in conflict with itself.
    fn blockers(&self, key: &[u8], owner: TxnId, mode: Mode) -> Vec<TxnId> {
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
    fn install(&mut self, key: &[u8], owner: TxnId, mode: Mode) {
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
    /// through here -- a commit, an abort, the sweeper -- because the
    /// owner check is what makes a *late* release safe: a release that
    /// just removed the key would happily delete a lock somebody else
    /// has since taken.
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

    /// Whether `blocker` can be pushed out of the way by a transaction of
    /// age `mine`. See the module doc for why `Committing` is immune.
    fn is_woundable(&self, blocker: TxnId, mine: u64) -> bool {
        self.txns
            .get(&blocker)
            .is_some_and(|t| t.state == TxnState::Open && t.priority > mine)
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
            self.release(&key, id);
        }
        // Whatever it buffered will never be written now, so it stops
        // counting against the node's budget immediately -- the
        // tombstone left behind is a reason, not a payload.
        self.forget_buffer(id);
    }

    /// Give up a transaction's buffered writes and the budget they held.
    ///
    /// Idempotent, and the only place besides `buffer_write` that moves
    /// `buffered_total`, so the two stay in step by there being nowhere
    /// else for them to drift.
    fn forget_buffer(&mut self, id: TxnId) {
        let Some(txn) = self.txns.get_mut(&id) else {
            return;
        };
        self.buffered_total = self.buffered_total.saturating_sub(txn.buffered_bytes);
        txn.buffered_bytes = 0;
        txn.writes.clear();
    }

    /// Release everything `id` holds without changing its state -- for a
    /// transaction that is finishing normally rather than being aborted.
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

    /// Note the leader term this node is serving under, discarding every
    /// transaction and every transactional lock if it has changed.
    ///
    /// A transaction belongs to the leader that created it: its reads
    /// were validated against that leader's state and its commit can only
    /// happen there. A new term means none of that holds any more, and
    /// keeping the locks would only make keys unwritable on behalf of
    /// transactions that can never commit.
    ///
    /// Safe to do while a commit is still in flight: its `CommitGuard`
    /// releases through `discard`, which finds the transaction already
    /// gone from `txns` and does nothing, rather than removing locks
    /// that now belong to somebody else.
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
            shared.acquire(key, id, Mode::Read, Instant::now())?;
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
            // Rewriting a key this transaction already buffered replaces
            // that value rather than adding to it, so the old one comes
            // back out of both budgets.
            let previous = txn.writes.get(key).map(|v| entry_size(key, v.as_deref()));
            let size = entry_size(key, value.as_deref());
            let delta = size as isize - previous.unwrap_or(0) as isize;
            let projected = txn.buffered_bytes.saturating_add_signed(delta);
            let projected_node_wide = shared.buffered_total.saturating_add_signed(delta);

            if projected > MAX_TRANSACTION_BYTES {
                return Err(Refused::TooMuch("the transaction has buffered too much"));
            }
            // Checked separately, and worth a distinct message: this one
            // is not about the caller's own transaction being too big,
            // and shrinking it may not help.
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

    /// What every open transaction is holding between them, against
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

/// Roughly what one buffered mutation costs, for the size cap. An
/// estimate on purpose: what it is guarding is the encoded size of the
/// log entry, and being approximately right well below the real ceiling
/// is worth more than being exactly right at it.
fn entry_size(key: &[u8], value: Option<&[u8]>) -> usize {
    key.len() + value.map_or(0, <[u8]>::len) + PER_KEY_OVERHEAD
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
