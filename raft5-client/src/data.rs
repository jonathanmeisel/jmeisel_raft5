//! Reads, writes and key walks outside a transaction.

use std::time::{Duration, Instant};

use raft5_shared::raft5::client::{
    GetKeysRequest, GetKeysResponse, Linearizable, ReadOk, ReadRequest, ReadResponse, WriteResponse,
    get_keys_request, get_keys_response, read_request, read_response, write_response,
};

use crate::{Client, DEADLINE, Error, RETRY_DELAY, Reply};
use crate::routing::Kv;

/// The lifetime of the transaction that [`Client::write`] opens for the
/// caller. Short, because that transaction never waits on the caller: it
/// is three round trips end to end. A longer hold would only block the
/// next writer if this client stalls mid-write.
const IMPLICIT_TXN_TTL: Duration = Duration::from_secs(2);

/// How many keys a [`KeyIterator`] fetches per round trip by default.
/// Small enough that a short walk does not fetch a full page, large
/// enough that a long walk does not cost a round trip per key.
pub const DEFAULT_KEY_BATCH: u32 = 64;

/// One batch of a key walk, as the cluster sent it.
///
/// Internal: [`KeyIterator`] is the public way to walk keys. This is a
/// separate type because `has_more` and `next_start_key` are separate
/// facts: a batch can be short or empty and still have keys after it.
struct KeyBatch {
    keys: Vec<Vec<u8>>,
    has_more: bool,
    /// Where the server says the next batch starts. Unset by a server that
    /// does not send it, and on the last batch.
    next_start_key: Option<Vec<u8>>,
}

/// Which moment a read or key walk outside a transaction describes, and
/// what was proved about it. For a read inside one, see
/// [`Transaction::read`].
///
/// [`Transaction::read`]: crate::Transaction::read
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Consistency<'a> {
    /// From whichever node the client talks to, without checking that it
    /// still leads. Cheap (one round trip, no read-quorum round), but it may
    /// miss recent writes, including this client's own. [`Read::as_of`] says
    /// how far behind it is: a value below the seq one of your commits
    /// returned means this node has not applied that commit yet.
    Stale,
    /// As of the moment the call is answered, including everything committed
    /// anywhere in the cluster. The node first proves it still leads and holds
    /// a quorum of leases, which costs a round of lease checks that
    /// [`Consistency::Stale`] does not.
    ///
    /// Nothing is locked, so the answer describes a moment already past: the
    /// keys may change right after. A read in a [`Transaction`] differs: it
    /// holds the keys against conflicting writes until the transaction ends.
    ///
    /// [`Transaction`]: crate::Transaction
    Linearizable,
    /// As the keys were when the snapshot with this id was created, whatever
    /// happened to them since, including deletes.
    ///
    /// Stale like [`Consistency::Stale`], on purpose: it describes a past
    /// moment, so any node can serve it without proving it leads. It does
    /// guarantee stability: the same read of the same snapshot gives the same
    /// answer for as long as the snapshot exists.
    Snapshot(&'a [u8]),
}

impl Consistency<'_> {
    fn to_pb(self) -> Option<read_request::Consistency> {
        match self {
            Consistency::Stale => None,
            Consistency::Linearizable => {
                Some(read_request::Consistency::Linearizable(Linearizable {}))
            }
            Consistency::Snapshot(id) => Some(read_request::Consistency::SnapshotId(id.to_vec())),
        }
    }

    /// The same choice, for `GetKeys`, whose oneof is a different type.
    fn to_get_keys_pb(self) -> Option<get_keys_request::Consistency> {
        match self {
            Consistency::Stale => None,
            Consistency::Linearizable => {
                Some(get_keys_request::Consistency::Linearizable(Linearizable {}))
            }
            Consistency::Snapshot(id) => {
                Some(get_keys_request::Consistency::SnapshotId(id.to_vec()))
            }
        }
    }
}

/// What a read found: one entry per key, in the order asked, all as of
/// the same moment.
///
/// **There may be fewer entries than keys.** When the values in one
/// response would pass [`raft5_shared::MAX_READ_VALUE_BYTES`], the cluster
/// stops, and `entries` answers a prefix of the keys. The prefix is never
/// empty, so a reissue for `&keys[entries.len()..]` always makes progress.
/// A versions-only read is never cut short.
///
/// The reissue is a separate read with its own `as_of`. In a
/// [`Transaction`] or at a [`Consistency::Snapshot`] that changes nothing:
/// the keys are locked, or the moment is fixed. A stale or linearizable
/// reissue describes a later moment, and comparing the two `as_of` values
/// shows whether anything was applied in between. That is why this
/// library never reissues for you: joining the halves would claim one
/// moment for two.
///
/// [`Transaction`]: crate::Transaction
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Read<T> {
    /// The seq that every entry describes the store as of.
    ///
    /// In a transaction: committed state as of this point, with the
    /// transaction's own buffered writes on top.
    pub as_of: u64,
    pub entries: Vec<T>,
}

/// One key's value. `seq` is the seq the value was written at. A key that
/// was never written, or was deleted, reads as `None` at seq 0 (seqs start
/// at 1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub key: Vec<u8>,
    pub seq: u64,
    pub value: Option<Vec<u8>>,
}

/// One key's version only: the seq its value was written at, or 0 if it
/// has no value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Version {
    pub key: Vec<u8>,
    pub seq: u64,
}

impl<T> Read<T> {
    /// Whether every key asked for was answered. False when the value budget
    /// cut the response short. `asked` is how many keys the read named.
    pub fn is_complete(&self, asked: usize) -> bool {
        self.entries.len() == asked
    }
}

pub(crate) fn entries(keys: Vec<Vec<u8>>, ok: ReadOk) -> Read<Entry> {
    Read {
        as_of: ok.as_of,
        entries: keys
            .into_iter()
            .zip(ok.entries)
            .map(|(key, found)| Entry {
                key,
                seq: found.seq,
                value: found.value,
            })
            .collect(),
    }
}

pub(crate) fn versions(keys: Vec<Vec<u8>>, ok: ReadOk) -> Read<Version> {
    Read {
        as_of: ok.as_of,
        entries: keys
            .into_iter()
            .zip(ok.entries)
            .map(|(key, found)| Version {
                key,
                seq: found.seq,
            })
            .collect(),
    }
}

impl Client {
    /// Write `value` to `key`, or delete `key` if `value` is `None`, whatever
    /// it holds now. Returns the seq of the commit. Returns only after the
    /// write is committed and applied, so a transactional read after it sees
    /// it.
    ///
    /// A transaction is the cluster's only way to write, so this opens one,
    /// buffers the key, and commits: three round trips, not one.
    /// [`transaction`] is the same mechanism with the steps left to you; use
    /// it when more than one key is involved or the value depends on a read.
    ///
    /// Losing a key to another transaction is not a failure here: an aborted
    /// transaction wrote nothing, so this opens another and tries again until
    /// [`DEADLINE`]. [`Error::InDoubt`] is the exception and is never retried.
    ///
    /// A write cut off by a lost connection may be retried after it already
    /// committed, because the client cannot tell which happened. The retry
    /// writes the same value to the same key, so the only trace is a second
    /// seq.
    ///
    /// [`transaction`]: Client::transaction
    pub async fn write(&self, key: &[u8], value: Option<&[u8]>) -> Result<u64, Error> {
        let deadline = Instant::now() + DEADLINE;
        loop {
            match self.write_once(key, value).await {
                // The value does not depend on what the key held, so a lost key just
                // means trying again, within the same deadline as the rest of the
                // client.
                Err(Error::Aborted { .. }) if Instant::now() < deadline => {
                    tokio::time::sleep(RETRY_DELAY).await;
                }
                other => return other,
            }
        }
    }

    /// One attempt at [`write`](Client::write): open a transaction, buffer the
    /// key, commit. On any failure, `Transaction`'s `Drop` gives the locks
    /// back instead of waiting for the deadline.
    async fn write_once(&self, key: &[u8], value: Option<&[u8]>) -> Result<u64, Error> {
        let txn = self.transaction(IMPLICIT_TXN_TTL).await?;
        txn.write(key, value).await?;
        txn.commit().await
    }

    /// Read `keys`, all as of the same moment, at `consistency`.
    ///
    /// Duplicate keys are fine and are answered at each position. At most
    /// [`raft5_shared::MAX_KEYS_PER_READ`] keys; more is refused before
    /// anything is sent. The whole read is refused if this account may not
    /// read any one of the keys.
    ///
    /// The answer may cover only a prefix of `keys`: see [`Read`] for when,
    /// and why the reissue is left to you. If the keys must also stay as
    /// read, use [`Transaction::read`].
    ///
    /// [`Transaction::read`]: crate::Transaction::read
    pub async fn read(
        &self,
        keys: &[impl AsRef<[u8]>],
        consistency: Consistency<'_>,
    ) -> Result<Read<Entry>, Error> {
        let (keys, ok) = self.read_inner(keys, false, consistency.to_pb()).await?;
        Ok(entries(keys, ok))
    }

    /// [`read`], for the seqs only: 0 for a key with no value.
    ///
    /// Answered from the seq index without reading any value, so the cost
    /// does not depend on value size, and the answer is never cut short. Use
    /// it to ask "has this changed since I last looked?": [`read`] can answer
    /// that only by sending back the whole value for you to discard.
    ///
    /// [`read`]: Client::read
    pub async fn read_versions(
        &self,
        keys: &[impl AsRef<[u8]>],
        consistency: Consistency<'_>,
    ) -> Result<Read<Version>, Error> {
        let (keys, ok) = self.read_inner(keys, true, consistency.to_pb()).await?;
        Ok(versions(keys, ok))
    }

    /// Walk the keys that have a value, in order, from `start_key` on, at
    /// `consistency`. Pass an empty slice to start at the beginning.
    ///
    /// Each batch is one call, and `consistency` applies to each call:
    /// - [`Consistency::Stale`]: each batch is a local read on whichever node
    ///   answered, and keys written since may be missing.
    /// - [`Consistency::Linearizable`]: each batch includes everything
    ///   committed when it was answered. Batches are separate moments, so a
    ///   key written mid-walk may or may not appear.
    /// - [`Consistency::Snapshot`]: the keys that had a value when the
    ///   snapshot was created, including keys deleted since. The only mode in
    ///   which the whole walk describes one moment.
    ///
    /// To walk and also keep the keys from changing, use
    /// [`Transaction::key_iterator`].
    ///
    /// ```no_run
    /// # async fn f(client: raft5_client::Client) -> Result<(), raft5_client::Error> {
    /// use raft5_client::Consistency;
    /// let mut keys = client.key_iterator(b"user:", Consistency::Stale);
    /// while let Some(key) = keys.next().await? {
    ///     println!("{}", String::from_utf8_lossy(&key));
    /// }
    /// # Ok(()) }
    /// ```
    ///
    /// [`Transaction::key_iterator`]: crate::Transaction::key_iterator
    pub fn key_iterator(&self, start_key: &[u8], consistency: Consistency<'_>) -> KeyIterator {
        KeyIterator::new(self.clone(), consistency.to_get_keys_pb(), start_key)
    }

    async fn get_keys_batch(
        &self,
        start_key: Vec<u8>,
        max_keys: u32,
        consistency: Option<get_keys_request::Consistency>,
    ) -> Result<KeyBatch, Error> {
        let request = GetKeysRequest {
            start_key,
            max_keys,
            consistency,
        };
        self.call(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.get_keys(request).await.map(get_keys_reply) }
        })
        .await
    }

    /// The one `Read` call behind every read method. Returns the keys it was
    /// asked about with the answer, so the caller can pair them.
    ///
    /// Checks the shape of the answer: it may cover fewer keys than asked,
    /// but never more, and never none of a non-empty list (the cluster always
    /// answers at least the first key).
    pub(crate) async fn read_inner(
        &self,
        keys: &[impl AsRef<[u8]>],
        versions_only: bool,
        consistency: Option<read_request::Consistency>,
    ) -> Result<(Vec<Vec<u8>>, ReadOk), Error> {
        if keys.len() > raft5_shared::MAX_KEYS_PER_READ {
            return Err(Error::InvalidArgument(
                "that is more keys than one read may name",
            ));
        }
        let request = ReadRequest {
            keys: keys.iter().map(|key| key.as_ref().to_vec()).collect(),
            versions_only,
            consistency,
        };
        let ok = self
            .call(|mut kv: Kv| {
                let request = request.clone();
                async move { kv.read(request).await.map(read_reply) }
            })
            .await?;
        if ok.entries.len() > request.keys.len()
            || (ok.entries.is_empty() && !request.keys.is_empty())
        {
            return Err(Error::Unavailable(format!(
                "malformed read response: {} entries for {} keys",
                ok.entries.len(),
                request.keys.len()
            )));
        }
        Ok((request.keys, ok))
    }
}

reply! {
    pub(crate) fn write_reply(WriteResponse, write_response) -> () {
        Ok(_) => Reply::Ok(()),
    }

    fn get_keys_reply(GetKeysResponse, get_keys_response, without_auth) -> KeyBatch {
        Ok(ok) => Reply::Ok(KeyBatch {
            keys: ok.keys,
            has_more: ok.has_more,
            next_start_key: (!ok.next_start_key.is_empty()).then_some(ok.next_start_key),
        }),
    }

    fn read_reply(ReadResponse, read_response) -> ReadOk {
        Ok(ok) => Reply::Ok(ok),
    }
}

/// Walk a cluster's keys in order, one batch at a time.
///
/// Built by [`Client::key_iterator`] (at a [`Consistency`]) or
/// [`Transaction::key_iterator`] (keys locked as they are returned). Not a
/// [`std::iter::Iterator`]: each batch is a round trip, so each step must
/// be awaited.
///
/// The cost is one RPC per [`batch_size`] keys, not one per key. Fetched
/// keys come from memory, and the next batch is requested only when the
/// current one runs out, so a caller that stops early never pays for the
/// rest.
///
/// [`batch_size`]: KeyIterator::batch_size
/// [`Transaction::key_iterator`]: crate::Transaction::key_iterator
pub struct KeyIterator {
    client: Client,
    /// What the walk asks for: a transaction, a snapshot, linearizable, or
    /// nothing (stale). Fixed
    /// when the iterator is made, so every batch uses the same terms. A walk
    /// that changed consistency midway would not describe any one state of
    /// the store.
    consistency: Option<get_keys_request::Consistency>,
    /// Where the next batch starts, inclusive. `None` once the cluster has
    /// said there is nothing after what we have.
    next_start: Option<Vec<u8>>,
    batch_size: u32,
    /// The current batch, reversed, so the next key is a `pop` from the end,
    /// not a shift from the front.
    buffer: Vec<Vec<u8>>,
}

impl KeyIterator {
    pub(crate) fn new(
        client: Client,
        consistency: Option<get_keys_request::Consistency>,
        start_key: &[u8],
    ) -> Self {
        Self {
            client,
            consistency,
            next_start: Some(start_key.to_vec()),
            batch_size: DEFAULT_KEY_BATCH,
            buffer: Vec::new(),
        }
    }

    /// How many keys to fetch per round trip. Defaults to
    /// [`DEFAULT_KEY_BATCH`]. A server that will not scan this many in one call
    /// refuses instead of sending fewer, so the error shows up here, not as a
    /// silently smaller batch.
    #[must_use]
    pub fn batch_size(mut self, keys: u32) -> Self {
        self.batch_size = keys;
        self
    }

    /// The next key, or `None` at the end of the keyspace.
    ///
    /// Answered from the buffer when possible; otherwise this makes the round
    /// trip. In a transaction, that round trip also locks the next batch's
    /// keys, so the transaction can abort here: see [`Error::Aborted`].
    pub async fn next(&mut self) -> Result<Option<Vec<u8>>, Error> {
        loop {
            if let Some(key) = self.buffer.pop() {
                return Ok(Some(key));
            }
            let Some(start) = self.next_start.take() else {
                return Ok(None);
            };

            let batch = self
                .client
                .get_keys_batch(start, self.batch_size, self.consistency.clone())
                .await?;
            let mut keys = batch.keys;

            // Where to go next. Use the server's cursor when it sends one, because
            // only the server knows why the batch ended: a walk can examine many
            // more keys than it keeps (hidden by an ACL, or deleted by the
            // transaction), and then there is no last key to derive a cursor from.
            //
            // Otherwise, use the batch's last key with one zero byte appended. That
            // is the smallest key that sorts after it, so nothing can be skipped. A
            // server that sends no cursor needs this too.
            self.next_start = match batch.next_start_key {
                Some(next) => Some(next),
                None if batch.has_more => keys.last().map(|last| {
                    let mut next = last.clone();
                    next.push(0);
                    next
                }),
                None => None,
            };

            // An empty batch can still have more: a walk that used its whole budget
            // without finding a key this caller may see. So continue on the cursor,
            // and stop only when there is no cursor left.
            if keys.is_empty() {
                if self.next_start.is_some() {
                    continue;
                }
                return Ok(None);
            }
            keys.reverse();
            self.buffer = keys;
        }
    }

    /// Every key from here to the end of the keyspace, in order.
    ///
    /// Useful for a keyspace known to be small. For a large one, prefer
    /// [`next`] and stop when you have what you need: this keeps going, and in
    /// a transaction it keeps locking.
    ///
    /// [`next`]: KeyIterator::next
    pub async fn collect(mut self) -> Result<Vec<Vec<u8>>, Error> {
        let mut all = Vec::new();
        while let Some(key) = self.next().await? {
            all.push(key);
        }
        Ok(all)
    }
}
