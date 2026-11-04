//! Explicit transactions: reads that lock, buffered writes, one commit.

use std::future::Future;
use std::hash::{BuildHasher, Hasher, RandomState};
use std::time::{Duration, Instant};

use raft5_shared::raft5::client::{
    CommitTransactionRequest, CommitTransactionResponse, CreateTransactionRequest,
    CreateTransactionResponse, DropTransactionRequest, WriteRequest, commit_transaction_response,
    create_transaction_response, get_keys_request, read_request,
};

use crate::data::{Entry, KeyIterator, Read, Version, entries, versions, write_reply};
use crate::routing::Kv;
use crate::{AbortReason, Client, Error, Reply};

/// How long [`Client::run_transaction`] may keep starting new attempts.
/// Long enough to outlast an election and a burst of contention. An attempt
/// already running may finish after it.
const RUN_LIMIT: Duration = Duration::from_secs(30);

/// How many times an attempt that ran out of time is tried again. The same
/// work usually runs out of time again, so waiting out the whole limit would
/// only hide why it failed.
const EXPIRED_RETRIES: u32 = 2;

/// The wait after the first abort. Most locks are held for a commit or less.
const FIRST_RETRY_DELAY: Duration = Duration::from_millis(2);

/// The longest wait between attempts.
const MAX_RETRY_DELAY: Duration = Duration::from_millis(250);

/// Decides whether to try a transaction again, and how long to wait first.
struct Retries {
    deadline: Instant,
    delay: Duration,
    expired: u32,
}

impl Retries {
    fn new(now: Instant, limit: Duration) -> Self {
        Self {
            deadline: now + limit,
            delay: FIRST_RETRY_DELAY,
            expired: 0,
        }
    }

    /// The wait before the next attempt after `error`, or `None` to give up
    /// and report it.
    ///
    /// - Only [`Error::Aborted`] is replayed. [`Error::InDoubt`] must not be,
    ///   and nothing else is a store abort.
    /// - An attempt that would start past the deadline never starts.
    /// - The wait is random between half of the current delay and all of it,
    ///   so writers that collided don't collide again in step.
    fn next_delay(&mut self, error: &Error, now: Instant) -> Option<Duration> {
        let Error::Aborted { reason, .. } = error else {
            return None;
        };
        if *reason == AbortReason::Expired {
            self.expired += 1;
            if self.expired > EXPIRED_RETRIES {
                return None;
            }
        }
        let half = self.delay / 2;
        let wait = half + half.mul_f64(random_fraction());
        self.delay = (self.delay * 2).min(MAX_RETRY_DELAY);
        (now + wait < self.deadline).then_some(wait)
    }
}

/// A number from 0 up to 1. It only spreads retries out, so it need not be
/// strong, and `RandomState` is seeded randomly for each use.
fn random_fraction() -> f64 {
    RandomState::new().build_hasher().finish() as f64 / u64::MAX as f64
}

impl Client {
    /// Run a whole transaction, opening a new one and running `attempt` again
    /// each time the store aborts it. This is the usual way to write.
    ///
    /// `attempt` gets the open transaction, and commits it unless it decides
    /// to write nothing (dropping it gives up its locks):
    ///
    /// ```no_run
    /// # async fn f(client: raft5_client::Client) -> Result<(), raft5_client::Error> {
    /// # use std::time::Duration;
    /// let seq = client
    ///     .run_transaction(Duration::from_secs(1), |txn| async move {
    ///         let balance = txn.read(&["alice"]).await?;
    ///         txn.write(b"alice", Some(b"9")).await?;
    ///         # let _ = balance;
    ///         txn.commit().await
    ///     })
    ///     .await?;
    /// # let _ = seq;
    /// # Ok(()) }
    /// ```
    ///
    /// `attempt` can run more than once, so it must do its reads again each
    /// time and leave nothing behind that a second run would repeat: build
    /// what it returns from what that attempt read.
    ///
    /// It stops and returns the error:
    /// - after [`Error::InDoubt`], or any error that isn't [`Error::Aborted`],
    ///   each at once;
    /// - after [`AbortReason::Expired`] a few times, because the same work
    ///   usually runs out of `ttl` again;
    /// - after about 30 seconds of other aborts. It waits a little longer
    ///   between attempts each time, so it doesn't add to the contention.
    ///
    /// A replayed attempt is a new transaction, and so the youngest: under
    /// heavy contention, a long transaction can lose to short ones until it
    /// gives up.
    pub async fn run_transaction<T, F, Fut>(&self, ttl: Duration, attempt: F) -> Result<T, Error>
    where
        F: FnMut(Transaction) -> Fut,
        Fut: Future<Output = Result<T, Error>>,
    {
        self.run_transaction_within(ttl, RUN_LIMIT, attempt).await
    }

    /// [`run_transaction`](Client::run_transaction), starting no attempt after
    /// `limit`.
    pub(crate) async fn run_transaction_within<T, F, Fut>(
        &self,
        ttl: Duration,
        limit: Duration,
        mut attempt: F,
    ) -> Result<T, Error>
    where
        F: FnMut(Transaction) -> Fut,
        Fut: Future<Output = Result<T, Error>>,
    {
        let mut retries = Retries::new(Instant::now(), limit);
        loop {
            let outcome = match self.transaction(ttl).await {
                Ok(txn) => attempt(txn).await,
                Err(e) => Err(e),
            };
            let error = match outcome {
                Ok(value) => return Ok(value),
                Err(error) => error,
            };
            match retries.next_delay(&error, Instant::now()) {
                Some(wait) => tokio::time::sleep(wait).await,
                None => return Err(error),
            }
        }
    }

    /// Open a transaction: reads and writes across any number of keys that
    /// commit together, or not at all.
    ///
    /// This is the only way to write, and a way to read data that stays
    /// current. A transaction reads what it needs, buffers changes, and
    /// commits them as one raft entry:
    /// - every key lands at one seq,
    /// - nobody else can write a key it has touched meanwhile,
    /// - every read it made is still true when it commits.
    ///
    /// That makes a read-modify-write possible.
    ///
    /// `ttl` is how long the cluster holds its locks. Choose it by how long the
    /// work between opening and committing really takes, not generously: until
    /// the transaction ends, nobody else can write the keys it read.
    ///
    /// A failure is normal and usually means "try again": see
    /// [`Error::Aborted`], which always means nothing was written. The one
    /// exception is [`Error::InDoubt`], which you must not replay.
    /// [`run_transaction`] does the trying again; this is one attempt.
    ///
    /// ```no_run
    /// # async fn f(client: raft5_client::Client) -> Result<(), raft5_client::Error> {
    /// # use std::time::Duration;
    /// let txn = client.transaction(Duration::from_secs(1)).await?;
    /// let balances = txn.read(&["alice", "bob"]).await?;
    /// txn.write(b"alice", Some(b"9")).await?;
    /// txn.write(b"bob", Some(b"11")).await?;
    /// txn.commit().await?;
    /// # let _ = balances;
    /// # Ok(()) }
    /// ```
    ///
    /// [`read`]: Client::read
    /// [`run_transaction`]: Client::run_transaction
    pub async fn transaction(&self, ttl: Duration) -> Result<Transaction, Error> {
        let request = CreateTransactionRequest {
            ttl_ms: u64::try_from(ttl.as_millis())
                .map_err(|_| Error::InvalidArgument("that transaction lifetime is absurd"))?,
        };
        let id = self
            .call(|mut kv: Kv| async move {
                kv.create_transaction(request)
                    .await
                    .map(create_transaction_reply)
            })
            .await?;

        Ok(Transaction {
            client: self.clone(),
            id,
            finished: false,
        })
    }
}

reply! {
    fn create_transaction_reply(
        CreateTransactionResponse,
        create_transaction_response,
        without_auth
    ) -> Vec<u8> {
        TransactionId(id) => Reply::Ok(id),
    }

    fn commit_reply(CommitTransactionResponse, commit_transaction_response, without_auth) -> u64 {
        Seq(seq) => Reply::Ok(seq),
    }
}

/// An open transaction: reads and buffered writes across any number of
/// keys, which commit together or not at all.
///
/// The state lives on the leader; this is a handle to it. That is why it
/// has a deadline, and why losing the leader ends it. Nothing it buffers
/// is written until [`commit`]. Dropping it, losing the connection, or
/// taking too long all leave the store as it was.
///
/// Every failure except [`Error::InDoubt`] means nothing was written, so
/// the usual response is to open a new transaction and do it all again,
/// reads included. [`Client::run_transaction`] does that, because its caller
/// gives it the reads to do again.
///
/// [`commit`]: Transaction::commit
pub struct Transaction {
    client: Client,
    id: Vec<u8>,
    /// Whether the cluster already knows this transaction is over, so `Drop`
    /// does not send a second request after a commit or an explicit abort.
    finished: bool,
}

/// Only the id: the client behind it is shared machinery, not part of
/// what identifies the transaction.
impl std::fmt::Debug for Transaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Transaction({})", hex(&self.id))
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

impl Transaction {
    /// Read `keys` as part of this transaction, and hold every one of them
    /// against conflicting writes until the transaction ends.
    ///
    /// A key this transaction already wrote reads back as that write: a
    /// transaction sees its own changes. `seq` still describes committed
    /// state, because a buffered write has no seq yet.
    ///
    /// Every key is locked before any is read, including keys the answer
    /// leaves out when it is cut short (see [`Read`]). So a reissue for the
    /// rest sees exactly the state this call would have.
    pub async fn read(&self, keys: &[impl AsRef<[u8]>]) -> Result<Read<Entry>, Error> {
        let (keys, ok) = self
            .client
            .read_inner(keys, false, self.consistency())
            .await?;
        Ok(entries(keys, ok))
    }

    /// The seqs of `keys`' values, without the values: 0 for a key with no
    /// value.
    ///
    /// Holds the keys against conflicting writes for the rest of the
    /// transaction, like [`read`]. So this is the cheap half of a
    /// check-then-write: get the versions, decide, write, commit, and nobody
    /// else can have touched the keys in between.
    ///
    /// The answer describes committed state, even for a key this transaction
    /// wrote: a buffered write has no seq until the commit gives it one.
    ///
    /// [`read`]: Transaction::read
    pub async fn read_versions(&self, keys: &[impl AsRef<[u8]>]) -> Result<Read<Version>, Error> {
        let (keys, ok) = self
            .client
            .read_inner(keys, true, self.consistency())
            .await?;
        Ok(versions(keys, ok))
    }

    fn consistency(&self) -> Option<read_request::Consistency> {
        Some(read_request::Consistency::TransactionId(self.id.clone()))
    }

    /// Walk every key that has a value now, in order, from `start_key` on, as
    /// part of this transaction.
    ///
    /// Every key it returns joins the read set and is held against
    /// conflicting writes until the transaction ends, like [`read`]. The walk
    /// includes this transaction's buffered changes: a key it wrote appears,
    /// and a key it deleted does not.
    ///
    /// Only the keys are locked, not the range. The locks are per key, so
    /// someone else can still create a key at a position the walk already
    /// passed (a phantom). A transaction that must not miss such a key cannot
    /// get that guarantee here.
    ///
    /// [`read`]: Transaction::read
    pub fn key_iterator(&self, start_key: &[u8]) -> KeyIterator {
        KeyIterator::new(
            self.client.clone(),
            Some(get_keys_request::Consistency::TransactionId(
                self.id.clone(),
            )),
            start_key,
        )
    }

    /// Buffer a write to `key` (or a delete, if `value` is `None`) and take
    /// the key exclusively.
    ///
    /// Nothing is written here, so there is no seq to return. The value waits
    /// on the leader until [`commit`], which is what lets every key in the
    /// transaction land at the same moment, under one seq.
    ///
    /// [`commit`]: Transaction::commit
    pub async fn write(&self, key: &[u8], value: Option<&[u8]>) -> Result<(), Error> {
        let request = WriteRequest {
            transaction_id: self.id.clone(),
            key: key.to_vec(),
            value: value.map(<[u8]>::to_vec),
        };
        self.client
            .call(|mut kv: Kv| {
                let request = request.clone();
                async move { kv.write(request).await.map(write_reply) }
            })
            .await
    }

    /// Commit everything this transaction buffered, as one entry.
    ///
    /// Returns the seq that all its keys were written at: one entry, one seq.
    /// A transaction that buffered no writes returns 0: nothing was appended,
    /// so there is no seq.
    ///
    /// Returns only after the entry is committed and applied on the leader, so
    /// a later transactional or linearizable read sees all its keys. A stale
    /// [`Client::read`] may not: a node the entry has not reached yet can
    /// answer it.
    pub async fn commit(mut self) -> Result<u64, Error> {
        let request = CommitTransactionRequest {
            transaction_id: self.id.clone(),
        };
        // Set before the call, not after: success or failure, the cluster is done
        // with this transaction, and `Drop` saying so again would only be noise.
        self.finished = true;
        self.client
            .call(|mut kv: Kv| {
                let request = request.clone();
                async move { kv.commit_transaction(request).await.map(commit_reply) }
            })
            .await
    }

    /// Give up the transaction and release its locks now, not at its
    /// deadline. Nothing it buffered is written.
    ///
    /// The same as dropping it, except that this waits for the cluster to
    /// confirm. Use it when your next step needs those keys.
    pub async fn abort(mut self) -> Result<(), Error> {
        self.finished = true;
        Self::send_drop(&self.client, self.id.clone()).await
    }

    async fn send_drop(client: &Client, id: Vec<u8>) -> Result<(), Error> {
        let request = DropTransactionRequest { transaction_id: id };
        client
            .call(|mut kv: Kv| {
                let request = request.clone();
                async move { kv.drop_transaction(request).await.map(|_| Reply::Ok(())) }
            })
            .await
    }
}

impl Drop for Transaction {
    /// Tell the cluster to release the locks.
    ///
    /// Best effort: `Drop` cannot await, so this can only spawn the request.
    /// That is fine: the transaction's deadline releases the same locks
    /// anyway, and this only makes it sooner. Use [`abort`] if you need to
    /// know it happened.
    ///
    /// Outside a Tokio runtime there is nothing to spawn on, so this does
    /// nothing and the deadline does the work.
    ///
    /// [`abort`]: Transaction::abort
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let (client, id) = (self.client.clone(), std::mem::take(&mut self.id));
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let _ = Self::send_drop(&client, id).await;
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMIT: Duration = Duration::from_secs(30);

    fn aborted(reason: AbortReason) -> Error {
        Error::Aborted { reason, key: None }
    }

    #[test]
    fn waits_grow_up_to_the_cap_and_stay_in_range() {
        let now = Instant::now();
        let mut retries = Retries::new(now, LIMIT);
        let mut delay = FIRST_RETRY_DELAY;
        for _ in 0..20 {
            let wait = retries
                .next_delay(&aborted(AbortReason::LockConflict), now)
                .expect("a conflict inside the limit is retried");
            assert!(wait >= delay / 2 && wait <= delay, "{wait:?} for {delay:?}");
            delay = (delay * 2).min(MAX_RETRY_DELAY);
        }
        assert_eq!(delay, MAX_RETRY_DELAY);
    }

    #[test]
    fn every_abort_but_expiry_is_retried_until_the_deadline() {
        let now = Instant::now();
        for reason in [
            AbortReason::LockConflict,
            AbortReason::Wounded,
            AbortReason::LeaderChanged,
            AbortReason::Unknown,
        ] {
            let mut retries = Retries::new(now, LIMIT);
            for _ in 0..100 {
                assert!(retries.next_delay(&aborted(reason), now).is_some());
            }
            assert!(retries.next_delay(&aborted(reason), now + LIMIT).is_none());
        }
    }

    #[test]
    fn no_attempt_is_planned_to_start_past_the_deadline() {
        let now = Instant::now();
        let mut retries = Retries::new(now, MAX_RETRY_DELAY / 4);
        // Even the shortest wait, half the delay, is past the deadline.
        retries.delay = MAX_RETRY_DELAY;
        assert!(
            retries
                .next_delay(&aborted(AbortReason::Wounded), now)
                .is_none()
        );
    }

    #[test]
    fn expiry_is_retried_only_a_few_times() {
        let now = Instant::now();
        let mut retries = Retries::new(now, LIMIT);
        for _ in 0..EXPIRED_RETRIES {
            assert!(
                retries
                    .next_delay(&aborted(AbortReason::Expired), now)
                    .is_some()
            );
        }
        assert!(
            retries
                .next_delay(&aborted(AbortReason::Expired), now)
                .is_none()
        );
    }

    #[test]
    fn in_doubt_and_other_errors_are_never_replayed() {
        let now = Instant::now();
        let mut retries = Retries::new(now, LIMIT);
        assert!(
            retries
                .next_delay(&Error::InDoubt { seq: 7 }, now)
                .is_none()
        );
        assert!(
            retries
                .next_delay(&Error::Unavailable("down".into()), now)
                .is_none()
        );
    }

    #[test]
    fn random_fractions_stay_between_zero_and_one() {
        for _ in 0..1000 {
            assert!((0.0..=1.0).contains(&random_fraction()));
        }
    }
}
