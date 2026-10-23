//! Explicit transactions: reads that lock, buffered writes, one commit.

use std::time::Duration;

use raft5_shared::raft5::client::{
    CommitTransactionRequest, CommitTransactionResponse, CreateTransactionRequest,
    CreateTransactionResponse, DropTransactionRequest, WriteRequest, commit_transaction_response,
    create_transaction_response, get_keys_request, read_request,
};

use crate::data::{Entry, KeyIterator, Read, Version, entries, versions, write_reply};
use crate::routing::Kv;
use crate::{Client, Error, Reply};

impl Client {
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
/// reads included. That is why this library does not retry for you: it
/// does not know what the reads were.
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
