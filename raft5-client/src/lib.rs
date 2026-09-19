//! A client for a raft5 cluster.
//!
//! [`Client`] wraps the `Kv` service (`client.proto`) so that a caller
//! writes `client.write(key, value)` and gets an answer, rather than
//! having to know which of the cluster's nodes is currently able to give
//! one -- or that the write is a transaction underneath, since the
//! cluster has no other way to write. Three things stand between those
//! two:
//!
//! * **Only the leader can serve most requests**, and a node that isn't
//!   the leader says so rather than forwarding (Raft §6.2). Every such
//!   refusal carries a `leader_hint` naming whoever it last heard from,
//!   and the client follows it -- immediately, without backing off,
//!   since a redirect is not a failure.
//! * **The leader changes.** With no hint to follow (a cluster mid
//!   election, a configuration older than the current leader), the
//!   client finds it by asking each node it knows of for the cluster
//!   configuration and seeing which one says `is_leader` -- which only a
//!   caught-up leader still holding a read quorum does, so it is proof
//!   rather than a guess.
//! * **The membership changes.** A client is constructed from seed
//!   addresses, but the cluster is the authority on who its members are,
//!   so the client periodically re-reads the configuration from whoever
//!   it believes is leading and keeps its endpoint list in step --
//!   picking up servers it was never seeded with, and dropping seeds
//!   that have left.
//!
//! The connection to the leader is kept open between calls, so the
//! common case -- same leader as last time -- dials nothing and costs one
//! round trip.

mod routing;

use std::future::Future;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use tonic::Status;

use raft5_shared::raft5::client::{
    AddServerRequest, BootstrapRequest, CommitTransactionRequest, CommitTransactionResponse,
    ConfigChangeResponse, CreateTransactionRequest, CreateTransactionResponse,
    DropTransactionRequest, GetKeysRequest, GetKeysResponse, RaftError as PbRaftError,
    ReadRequest, ReadResponse, RemoveServerRequest, WriteRequest, WriteResponse,
    commit_transaction_response, config_change_response, create_transaction_response,
    get_keys_response, raft_error, read_response, transaction_aborted, write_response,
};

use routing::{Kv, Router};

/// How long a call keeps retrying before giving up. Everything a call is
/// retried for -- a redirect, an election in progress, a membership
/// change still committing -- resolves well inside this on a healthy
/// cluster; past it, something is actually wrong and a caller waiting on
/// a key/value store deserves to hear about it.
///
/// Note what this costs a large transaction's commit, which has to
/// replicate to a majority and be applied inside it: a commit that runs
/// out of deadline comes back as [`Error::InDoubt`], the one outcome a
/// caller must never replay. "Too slow" therefore becomes
/// "unrecoverable", not "try again" -- so if the cluster's transaction
/// size limit ever grows much past its current few megabytes, this
/// wants growing with it.
const DEADLINE: Duration = Duration::from_secs(10);

/// How long to wait before re-sending a request that was refused for a
/// reason only time fixes (no leader elected yet, say). A redirect
/// carrying a leader hint doesn't wait at all -- there is somewhere
/// better to ask right now.
const RETRY_DELAY: Duration = Duration::from_millis(20);

/// How often to re-read the cluster's membership from the node believed
/// to be leading.
const CONFIG_REFRESH_INTERVAL: Duration = Duration::from_secs(10);

/// The lifetime [`Client::write`] asks for on the transaction it opens
/// behind the caller's back. Short, because that transaction is never
/// waiting on anything a caller does: it is three round trips end to
/// end, and holding the key any longer than that would only block
/// whoever wants it next if this client stalls mid-write.
const IMPLICIT_TXN_TTL: Duration = Duration::from_secs(2);

/// How many keys a [`KeyIterator`] fetches per round trip unless told
/// otherwise. Small enough that walking a handful of keys doesn't drag
/// back a page of them, large enough that walking a lot doesn't cost a
/// round trip each.
pub const DEFAULT_KEY_BATCH: u32 = 64;

#[derive(Debug)]
pub enum Error {
    /// A seed address (or one the cluster advertised) that isn't a
    /// usable URI. Addresses look like `http://host:port`.
    InvalidAddress(String),
    /// A client was constructed with no seeds, so it has nowhere to send
    /// anything.
    NoSeeds,
    /// Rejected before it was sent.
    InvalidArgument(&'static str),
    /// The cluster refused outright. Unlike [`Unavailable`], this is not
    /// about timing: the request is one no node will serve at any point,
    /// so it was not retried. Asking for more than a limit allows, or
    /// sending something malformed, lands here.
    ///
    /// [`Unavailable`]: Error::Unavailable
    Rejected(String),
    /// Nobody served the request: either no member managed to before the
    /// deadline, or (for `Bootstrap`, which doesn't retry) the node
    /// refused outright. The text is the last refusal or transport
    /// failure seen.
    Unavailable(String),
    /// The transaction is over and nothing it buffered was written.
    ///
    /// Not retried by this library, because the retry isn't the request
    /// -- it is the whole transaction, reads and all, and only the
    /// caller knows what those were. Replaying is always safe: an
    /// aborted transaction wrote nothing.
    Aborted {
        reason: AbortReason,
        /// Which key was contended, for [`AbortReason::LockConflict`].
        key: Option<Vec<u8>>,
    },
    /// The commit reached the log and then the leader changed, so
    /// nothing anywhere can say whether it committed or was rolled back.
    /// `seq` is where the entry went.
    ///
    /// **Do not replay the transaction.** Every other failure here means
    /// nothing was written; this one means nobody knows. Re-running a
    /// read-modify-write that already applied applies it twice. Read the
    /// keys back and decide from what is actually there.
    InDoubt { seq: u64 },
}

/// Why a transaction ended without committing. Each of these means
/// nothing was written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AbortReason {
    /// A key was held by someone this transaction couldn't displace: an
    /// older transaction, or one already committing. Worth retrying --
    /// the holder will finish.
    LockConflict,
    /// Its deadline passed before it committed. A longer one, or less
    /// work between opening it and committing, would help.
    Expired,
    /// An older transaction took one of its keys, so that the older one
    /// could make progress rather than the two aborting each other
    /// forever. Retrying is expected: this transaction is now the older
    /// one relative to whatever starts next.
    Wounded,
    /// The leader changed. A transaction lives on the leader that
    /// created it and cannot be finished anywhere else.
    LeaderChanged,
    /// The cluster has no transaction by this id -- it was dropped,
    /// committed already, or died long enough ago that even the record
    /// of why is gone.
    Unknown,
}

impl AbortReason {
    fn from_wire(reason: transaction_aborted::Reason) -> Self {
        match reason {
            transaction_aborted::Reason::LockConflict => Self::LockConflict,
            transaction_aborted::Reason::Expired => Self::Expired,
            transaction_aborted::Reason::Wounded => Self::Wounded,
            transaction_aborted::Reason::LeaderChanged => Self::LeaderChanged,
            // `InDoubt` never reaches here -- it becomes `Error::InDoubt`
            // before this, precisely so it cannot be mistaken for one of
            // the reasons that are safe to replay.
            transaction_aborted::Reason::Unknown | transaction_aborted::Reason::InDoubt => {
                Self::Unknown
            }
        }
    }
}

impl std::fmt::Display for AbortReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::LockConflict => "a key it needed was held by someone it could not displace",
            Self::Expired => "its deadline passed",
            Self::Wounded => "an older transaction took one of its keys",
            Self::LeaderChanged => "the leader changed",
            Self::Unknown => "the cluster has no such transaction",
        })
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidAddress(address) => write!(f, "not a usable address: {address:?}"),
            Self::NoSeeds => write!(f, "a client needs at least one seed address"),
            Self::InvalidArgument(why) => write!(f, "{why}"),
            Self::Rejected(why) => write!(f, "the cluster refused the request: {why}"),
            Self::Unavailable(why) => write!(f, "the cluster could not serve the request: {why}"),
            Self::Aborted { reason, key: Some(key) } => write!(
                f,
                "the transaction was aborted: {reason} ({})",
                String::from_utf8_lossy(key)
            ),
            Self::Aborted { reason, key: None } => {
                write!(f, "the transaction was aborted: {reason}")
            }
            Self::InDoubt { seq } => write!(
                f,
                "the commit reached the log at seq {seq} and then the leader changed; \
                 whether it committed is unknown, so it must not be replayed"
            ),
        }
    }
}

impl std::error::Error for Error {}

/// What a key currently holds. `seq` is the sequence number the value
/// was written at; a key that has never been written (or has been
/// deleted) reads as `None` at seq 0, since sequencing starts at 1.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadResult {
    pub seq: u64,
    pub value: Option<Vec<u8>>,
}

/// How one attempt came back, with the transport and protocol-level
/// differences flattened into the only distinction the retry loop cares
/// about: done, redirected, or try again.
enum Reply<T> {
    Ok(T),
    /// Not the leader; `Option<String>` is the hinted leader's
    /// `server_id`, absent if it doesn't know one either.
    NotLeader(Option<String>),
    /// Refused for a reason that may pass on its own: no leader elected
    /// yet, a membership change still in flight, a leader that hasn't
    /// yet committed an entry from its own term.
    Unavailable(String),
    /// The transaction is over. Not retried here: what would have to be
    /// retried is the whole transaction, which only its caller can
    /// replay.
    Aborted { reason: AbortReason, key: Option<Vec<u8>> },
    /// The commit is in doubt. Retried by nobody, ever.
    InDoubt { seq: u64 },
}

impl<T> Reply<T> {
    fn from_error(error: PbRaftError) -> Self {
        match error.kind {
            Some(raft_error::Kind::NotLeader(not_leader)) => {
                Self::NotLeader(not_leader.leader_hint)
            }
            Some(raft_error::Kind::Unavailable(why)) => Self::Unavailable(why),
            Some(raft_error::Kind::Aborted(aborted)) => {
                match transaction_aborted::Reason::try_from(aborted.reason) {
                    // Split out before anything else can touch it: this
                    // is the one outcome that must never be retried, so
                    // it must never be reachable through a variant that
                    // is.
                    Ok(transaction_aborted::Reason::InDoubt) => Self::InDoubt {
                        seq: aborted.seq.unwrap_or_default(),
                    },
                    Ok(reason) => Self::Aborted {
                        reason: AbortReason::from_wire(reason),
                        key: aborted.key,
                    },
                    // A reason this build doesn't know. Reporting it as
                    // in-doubt is the conservative reading: it refuses to
                    // replay something it cannot show was not written.
                    Err(_) => Self::InDoubt {
                        seq: aborted.seq.unwrap_or_default(),
                    },
                }
            }
            None => Self::Unavailable("the node refused without saying why".to_string()),
        }
    }
}

/// A handle on a raft5 cluster.
///
/// Cheap to clone -- clones share one connection to the leader and one
/// view of the membership -- and safe to use from many tasks at once.
#[derive(Clone)]
pub struct Client {
    router: Arc<Router>,
}

impl Client {
    /// A client that starts out knowing `seeds`: client API addresses
    /// (`http://host:port`) of nodes that are, or are about to become,
    /// members. They are a starting point rather than a fixed list --
    /// the cluster's own configuration takes over as soon as any one of
    /// them answers.
    ///
    /// Must be called from within a Tokio runtime: it spawns the task
    /// that keeps that configuration up to date.
    pub fn new(seeds: impl IntoIterator<Item = impl AsRef<str>>) -> Result<Self, Error> {
        let router = Arc::new(Router::new(seeds)?);
        spawn_config_refresh(Arc::downgrade(&router));
        Ok(Self { router })
    }

    /// The client API addresses this client currently knows of: the
    /// seeds until the cluster's configuration has been read, its
    /// membership afterwards.
    pub fn endpoints(&self) -> Vec<String> {
        self.router.endpoints()
    }

    /// Writes `value` to `key`, or deletes `key` if `value` is `None`,
    /// whatever it currently holds, returning the sequence number it
    /// committed at. Returns only once the write has been committed
    /// *and* applied, so a transactional read that follows it reflects
    /// it.
    ///
    /// A transaction is the cluster's only way to write, so this opens
    /// one of its own, buffers the key, and commits -- three round trips
    /// rather than one. [`transaction`] is the same machinery with the
    /// steps left to the caller, and is what to reach for when more than
    /// one key is involved or the value depends on what was read.
    ///
    /// Losing a key to a contending transaction is not a failure here:
    /// an aborted transaction wrote nothing, so this simply opens
    /// another and tries again until [`DEADLINE`]. [`Error::InDoubt`] is
    /// the exception and is never retried.
    ///
    /// A write interrupted by a lost connection may be retried after it
    /// has already committed, since nothing tells the client which of the
    /// two happened. The retry rewrites the same key with the same value,
    /// so the only trace of it is a second sequence number.
    ///
    /// [`transaction`]: Client::transaction
    pub async fn write(&self, key: &[u8], value: Option<&[u8]>) -> Result<u64, Error> {
        let deadline = Instant::now() + DEADLINE;
        loop {
            match self.write_once(key, value).await {
                // Nothing here needs reconsidering -- the value doesn't
                // depend on what the key held -- so a lost key just
                // means going round again, to the same deadline the rest
                // of the client works to.
                Err(Error::Aborted { .. }) if Instant::now() < deadline => {
                    tokio::time::sleep(RETRY_DELAY).await;
                }
                other => return other,
            }
        }
    }

    /// One attempt at [`write`](Client::write): open a transaction, buffer the
    /// one key, commit. A failure anywhere leaves `Transaction`'s `Drop`
    /// to give the locks back rather than waiting for the deadline.
    async fn write_once(&self, key: &[u8], value: Option<&[u8]>) -> Result<u64, Error> {
        let txn = self.transaction(IMPLICIT_TXN_TTL).await?;
        txn.write(key, value).await?;
        txn.commit().await
    }

    /// Opens a transaction: reads and writes across any number of keys
    /// that commit together, or not at all.
    ///
    /// This is the only way to write, and the only way to read something
    /// guaranteed current -- [`read`] outside a transaction may be
    /// stale. A transaction reads what it needs, buffers the changes,
    /// and commits them as a single raft entry: every key lands at one
    /// sequence number, nobody else can write a key it has touched in
    /// the meantime, and a read it made is still true when it commits.
    /// That is what makes a read-modify-write possible.
    ///
    /// `ttl` is how long the cluster will hold its locks. Choose it by
    /// how long the work between opening and committing actually takes,
    /// not generously: until the transaction finishes, nobody else can
    /// write the keys it has read.
    ///
    /// Failing is normal and usually means try again -- see
    /// [`Error::Aborted`], which always means nothing was written. The
    /// one exception is [`Error::InDoubt`], which must not be replayed.
    ///
    /// ```no_run
    /// # async fn f(client: raft5_client::Client) -> Result<(), raft5_client::Error> {
    /// # use std::time::Duration;
    /// let txn = client.transaction(Duration::from_secs(1)).await?;
    /// let alice = txn.read(b"alice").await?;
    /// let bob = txn.read(b"bob").await?;
    /// txn.write(b"alice", Some(b"9")).await?;
    /// txn.write(b"bob", Some(b"11")).await?;
    /// txn.commit().await?;
    /// # let _ = (alice, bob);
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

    /// Reads `key` from whichever node the client is currently talking
    /// to, without checking that it still leads. Cheap -- one round trip
    /// and no read-quorum round -- at the price of possibly missing
    /// recent writes, including ones this client itself just made.
    ///
    /// For a read that is guaranteed current, open a [`transaction`] and
    /// read inside it: that is what takes the lock and proves the node
    /// answering is still the leader.
    ///
    /// [`transaction`]: Client::transaction
    pub async fn read(&self, key: &[u8]) -> Result<ReadResult, Error> {
        self.read_inner(key, None).await
    }

    /// Walks every key that currently has a value, in order, from
    /// `start_key` on. Pass an empty slice to start at the beginning.
    ///
    /// Outside a transaction, like [`read`], this is stale: each batch
    /// is a local read of whichever node answered, and keys written
    /// since may be missing. [`Transaction::key_iterator`] is the
    /// current one.
    ///
    /// ```no_run
    /// # async fn f(client: raft5_client::Client) -> Result<(), raft5_client::Error> {
    /// let mut keys = client.key_iterator(b"user:");
    /// while let Some(key) = keys.next().await? {
    ///     println!("{}", String::from_utf8_lossy(&key));
    /// }
    /// # Ok(()) }
    /// ```
    ///
    /// [`read`]: Client::read
    pub fn key_iterator(&self, start_key: &[u8]) -> KeyIterator {
        KeyIterator::new(self.clone(), None, start_key)
    }

    async fn get_keys_batch(
        &self,
        start_key: Vec<u8>,
        max_keys: u32,
        transaction_id: Option<Vec<u8>>,
    ) -> Result<(Vec<Vec<u8>>, bool), Error> {
        let request = GetKeysRequest {
            start_key,
            max_keys,
            transaction_id,
        };
        self.call(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.get_keys(request).await.map(get_keys_reply) }
        })
        .await
    }

    async fn read_inner(
        &self,
        key: &[u8],
        transaction_id: Option<Vec<u8>>,
    ) -> Result<ReadResult, Error> {
        let request = ReadRequest {
            key: key.to_vec(),
            transaction_id,
        };
        self.call(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.read(request).await.map(read_reply) }
        })
        .await
    }

    /// Adds a server to the cluster, at the address its client API is
    /// reachable on. The leader dials it for its own `GetConfig` to learn
    /// its server id and peer address, so the caller only has to know
    /// where its client API is. It joins as a non-voting learner and is
    /// promoted automatically once it has caught up (Raft §4.2.1), so
    /// this one call is the whole operation.
    ///
    /// Returns once the change has committed. Only one membership change
    /// may be in flight at a time; a call that collides with another is
    /// retried like any other transient refusal.
    pub async fn add_server(&self, client_address: &str) -> Result<(), Error> {
        let request = AddServerRequest {
            client_address: client_address.to_string(),
        };
        self.change_config(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.add_server(request).await.map(config_reply) }
        })
        .await
    }

    /// Removes a server from the cluster. Returns once the change has
    /// committed.
    pub async fn remove_server(&self, server_id: &str) -> Result<(), Error> {
        let request = RemoveServerRequest {
            server_id: server_id.to_string(),
        };
        self.change_config(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.remove_server(request).await.map(config_reply) }
        })
        .await
    }

    /// Forms a single-node cluster around the node called `server_id`
    /// (Raft §4.4), which must be one of this client's endpoints. Every
    /// other node joins that cluster through [`add_server`] instead.
    ///
    /// Sent to each endpoint in turn: a node refuses a `server_id` that
    /// isn't its own, so the guard decides which one acts on it, and a
    /// node that already belongs to a cluster refuses outright. Unlike
    /// every other call this one does not retry -- bootstrapping is
    /// irreversible, and a node that has only just started may need a
    /// moment before it can accept one.
    ///
    /// [`add_server`]: Client::add_server
    pub async fn bootstrap(&self, server_id: &str) -> Result<(), Error> {
        if server_id.is_empty() {
            return Err(Error::InvalidArgument(
                "bootstrap needs the id of the node to bootstrap",
            ));
        }

        let request = BootstrapRequest {
            server_id: server_id.to_string(),
        };
        let mut last: Option<String> = None;
        for node in self.router.nodes() {
            let mut kv = node.connect();
            match kv.bootstrap(request.clone()).await {
                Ok(resp) => match config_reply(resp) {
                    Reply::Ok(()) => return Ok(()),
                    // A membership change has no key and so no
                    // precondition, and it is not a transaction; every
                    // other variant is unreachable here, and each says
                    // the same thing anyway -- this node said no.
                    _ => last = Some(format!("{} refused to bootstrap", node.key)),
                },
                Err(status) => last = Some(format!("{}: {status}", node.key)),
            }
        }

        Err(Error::Unavailable(last.unwrap_or_else(|| {
            format!("no endpoint of this client is called {server_id:?}")
        })))
    }

    /// `call`, plus a re-read of the membership on success: the client
    /// has just changed who the cluster's members are, so its own view
    /// of them is known to be out of date.
    async fn change_config<F, Fut>(&self, op: F) -> Result<(), Error>
    where
        F: Fn(Kv) -> Fut,
        Fut: Future<Output = Result<Reply<()>, Status>>,
    {
        self.call(op).await?;
        if let Some(leader) = self.router.cached() {
            self.router.refresh_config(&leader.kv).await;
        }
        Ok(())
    }

    /// Runs one RPC against the leader, following redirects and retrying
    /// transient refusals until [`DEADLINE`].
    async fn call<T, F, Fut>(&self, op: F) -> Result<T, Error>
    where
        F: Fn(Kv) -> Fut,
        Fut: Future<Output = Result<Reply<T>, Status>>,
    {
        let deadline = Instant::now() + DEADLINE;
        let mut hint: Option<String> = None;
        let mut last: Option<String> = None;

        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::Unavailable(last.unwrap_or_else(|| {
                    "no cluster member answered in time".to_string()
                })));
            }

            match self.router.leader(hint.take()).await {
                Ok(leader) => {
                    // Bounded by whatever is left of the deadline rather
                    // than by a timeout of its own: a write blocks until
                    // it commits, and how long that may legitimately take
                    // is exactly the question the deadline answers.
                    match tokio::time::timeout(remaining, op(leader.kv)).await {
                        Ok(Ok(Reply::Ok(value))) => return Ok(value),
                        Ok(Ok(Reply::NotLeader(new_hint))) => {
                            self.router.forget(&leader.key);
                            last = Some(format!("{} is not the leader", leader.key));
                            hint = new_hint;
                            // A hint is somewhere better to ask, right
                            // now -- waiting first would only add
                            // latency to a request that can be served.
                            if hint.is_some() {
                                continue;
                            }
                        }
                        // The leader is the right node; the cluster just
                        // isn't ready to answer yet. Keep the connection.
                        Ok(Ok(Reply::Unavailable(why))) => last = Some(why),
                        // Neither of these is this loop's to retry. An
                        // aborted transaction has to be replayed whole,
                        // reads included, which only its caller can do;
                        // an in-doubt commit must not be replayed at
                        // all.
                        Ok(Ok(Reply::Aborted { reason, key })) => {
                            return Err(Error::Aborted { reason, key });
                        }
                        Ok(Ok(Reply::InDoubt { seq })) => return Err(Error::InDoubt { seq }),
                        // A refusal that time cannot fix. Retrying it
                        // would only spend the whole deadline arriving at
                        // the same answer, and then report it as though
                        // the cluster had been too busy -- which would
                        // hide the actual reason in the one case where
                        // the caller most needs to read it.
                        Ok(Err(status)) if is_permanent(&status) => {
                            return Err(Error::Rejected(status.message().to_string()));
                        }
                        Ok(Err(status)) => {
                            self.router.forget(&leader.key);
                            last = Some(format!("{}: {status}", leader.key));
                        }
                        Err(_) => {
                            self.router.forget(&leader.key);
                            last = Some(format!("{} did not answer in time", leader.key));
                        }
                    }
                }
                // Nobody could be shown to be leading this round -- an
                // election, most likely. Worth another look shortly.
                Err(Error::Unavailable(why)) => last = Some(why),
                Err(e) => return Err(e),
            }

            tokio::time::sleep(RETRY_DELAY).await;
        }
    }
}

/// Whether a `Status` says the request itself is wrong, rather than that
/// this node happened not to be able to serve it. Deliberately a short
/// list: anything not named here is retried, because treating a
/// transient failure as permanent is the worse mistake of the two.
fn is_permanent(status: &Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::InvalidArgument | tonic::Code::ResourceExhausted
    )
}

/// Re-reads the cluster's membership from the node the client believes
/// is leading, so that servers added since the client started are
/// reachable and ones removed since are forgotten.
///
/// Holds only a `Weak`, so the task ends when the last [`Client`] is
/// dropped rather than keeping it (and its connections) alive forever.
/// There is nothing to refresh on a tick before any call has found a
/// leader, so a tick with none is skipped.
///
/// Before the first tick, the seeds are tried in turn until one hands
/// over a configuration, so the client's endpoints are the cluster's own
/// as soon as any seed is reachable rather than after the first request.
fn spawn_config_refresh(router: Weak<Router>) {
    tokio::spawn(async move {
        if let Some(router) = router.upgrade() {
            for node in router.nodes() {
                if router.refresh_config(&node.connect()).await {
                    break;
                }
            }
        }
        let mut ticker = tokio::time::interval(CONFIG_REFRESH_INTERVAL);
        ticker.tick().await; // the first tick completes immediately
        loop {
            ticker.tick().await;
            let Some(router) = router.upgrade() else {
                return;
            };
            if let Some(leader) = router.cached() {
                router.refresh_config(&leader.kv).await;
            }
        }
    });
}

fn write_reply(resp: tonic::Response<WriteResponse>) -> Reply<()> {
    match resp.into_inner().result {
        Some(write_response::Result::Ok(_)) => Reply::Ok(()),
        Some(write_response::Result::Error(e)) => Reply::from_error(e),
        None => empty_response(),
    }
}

fn get_keys_reply(resp: tonic::Response<GetKeysResponse>) -> Reply<(Vec<Vec<u8>>, bool)> {
    match resp.into_inner().result {
        Some(get_keys_response::Result::Ok(ok)) => Reply::Ok((ok.keys, ok.has_more)),
        Some(get_keys_response::Result::Error(e)) => Reply::from_error(e),
        None => empty_response(),
    }
}

fn read_reply(resp: tonic::Response<ReadResponse>) -> Reply<ReadResult> {
    match resp.into_inner().result {
        Some(read_response::Result::Ok(ok)) => Reply::Ok(ReadResult {
            seq: ok.seq,
            value: ok.value,
        }),
        Some(read_response::Result::Error(e)) => Reply::from_error(e),
        None => empty_response(),
    }
}

fn create_transaction_reply(resp: tonic::Response<CreateTransactionResponse>) -> Reply<Vec<u8>> {
    match resp.into_inner().result {
        Some(create_transaction_response::Result::TransactionId(id)) => Reply::Ok(id),
        Some(create_transaction_response::Result::Error(e)) => Reply::from_error(e),
        None => Reply::Unavailable("the node answered without a result".to_string()),
    }
}

fn commit_reply(resp: tonic::Response<CommitTransactionResponse>) -> Reply<u64> {
    match resp.into_inner().result {
        Some(commit_transaction_response::Result::Seq(seq)) => Reply::Ok(seq),
        Some(commit_transaction_response::Result::Error(e)) => Reply::from_error(e),
        None => Reply::Unavailable("the node answered without a result".to_string()),
    }
}

fn config_reply(resp: tonic::Response<ConfigChangeResponse>) -> Reply<()> {
    match resp.into_inner().result {
        Some(config_change_response::Result::Ok(true)) => Reply::Ok(()),
        Some(config_change_response::Result::Ok(false)) => {
            Reply::Unavailable("the leader did not apply the change".to_string())
        }
        Some(config_change_response::Result::Error(e)) => Reply::from_error(e),
        None => empty_response(),
    }
}

fn empty_response<T>() -> Reply<T> {
    Reply::Unavailable("the node sent a response with nothing in it".to_string())
}

/// An open transaction: reads and buffered writes across any number of
/// keys, which commit together or not at all.
///
/// Held on the leader, not here -- this is a handle to state that lives
/// there, which is why it has a deadline and why losing the leader ends
/// it. Nothing it buffers is written until [`commit`]; dropping it,
/// losing the connection, or simply taking too long all leave the store
/// exactly as it was.
///
/// Every failure short of [`Error::InDoubt`] means nothing was written,
/// so the usual response is to open another transaction and do it all
/// again -- including the reads, which is why this library doesn't retry
/// for you: it doesn't know what they were.
///
/// [`commit`]: Transaction::commit
pub struct Transaction {
    client: Client,
    id: Vec<u8>,
    /// Whether the cluster has already been told this transaction is
    /// over, so `Drop` doesn't send a redundant request after a commit
    /// or an explicit abort.
    finished: bool,
}

/// Just the id -- the client behind it is shared machinery, not part of
/// what identifies this transaction.
impl std::fmt::Debug for Transaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Transaction({})", hex(&self.id))
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

impl Transaction {
    /// Reads `key` as part of this transaction, holding it against
    /// conflicting writes until the transaction finishes.
    ///
    /// A key this transaction has already buffered a write for reads
    /// back as that write -- a transaction sees its own changes. `seq`
    /// always describes *committed* state even then, since a buffered
    /// write has no sequence number yet.
    pub async fn read(&self, key: &[u8]) -> Result<ReadResult, Error> {
        self.client.read_inner(key, Some(self.id.clone())).await
    }

    /// Walks every key that currently has a value, in order, from
    /// `start_key` on, as part of this transaction.
    ///
    /// Every key it hands back joins the read set and is held against
    /// conflicting writes until the transaction finishes, exactly as
    /// [`read`] does -- and what this transaction has buffered is
    /// reflected, so a key it wrote appears and one it deleted does not.
    ///
    /// The *range* is not locked, only the keys. These are per-key
    /// locks, so a key that didn't exist when the walk passed its
    /// position can still be created by someone else -- a phantom. A
    /// transaction that must not miss such a key cannot get that
    /// guarantee here.
    ///
    /// [`read`]: Transaction::read
    pub fn key_iterator(&self, start_key: &[u8]) -> KeyIterator {
        KeyIterator::new(self.client.clone(), Some(self.id.clone()), start_key)
    }

    /// Buffers a write to `key` (or a delete, if `value` is `None`) and
    /// takes the key exclusively.
    ///
    /// Nothing is written here, which is why there is no sequence number
    /// to return. The value is held on the leader until [`commit`],
    /// which is what lets every key in the transaction land at the same
    /// moment -- and is where the one sequence number for all of them
    /// comes from.
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

    /// Commits everything this transaction buffered, as one entry.
    ///
    /// Returns the sequence number all of its keys were written at --
    /// one entry, so one seq for the lot. A transaction that buffered no
    /// writes commits at 0: there was nothing to append, so there is no
    /// sequence number to name.
    ///
    /// Returns only once the entry has committed *and* been applied on
    /// the leader, so a transactional read that follows reflects every
    /// key of it. A plain [`Client::read`] may not: it can be answered
    /// by a node this entry hasn't reached yet.
    pub async fn commit(mut self) -> Result<u64, Error> {
        let request = CommitTransactionRequest {
            transaction_id: self.id.clone(),
        };
        // Set before the call, not after: whether it succeeds or fails,
        // the cluster is done with this transaction, and a `Drop` that
        // then went and said so again would only be noise.
        self.finished = true;
        self.client
            .call(|mut kv: Kv| {
                let request = request.clone();
                async move { kv.commit_transaction(request).await.map(commit_reply) }
            })
            .await
    }

    /// Gives up the transaction, releasing its locks now rather than
    /// when its deadline passes. Nothing it buffered is written.
    ///
    /// The same thing dropping it does, except that this waits for the
    /// cluster to confirm -- worth it when the next thing you do needs
    /// those keys.
    pub async fn abort(mut self) -> Result<(), Error> {
        self.finished = true;
        Self::send_drop(&self.client, self.id.clone()).await
    }

    async fn send_drop(client: &Client, id: Vec<u8>) -> Result<(), Error> {
        let request = DropTransactionRequest {
            transaction_id: id,
        };
        client
            .call(|mut kv: Kv| {
                let request = request.clone();
                async move { kv.drop_transaction(request).await.map(|_| Reply::Ok(())) }
            })
            .await
    }
}

impl Drop for Transaction {
    /// Tells the cluster to let the locks go.
    ///
    /// Best-effort by necessity -- `Drop` cannot await, so this can only
    /// spawn the request and hope. That is fine: the transaction's
    /// deadline releases the same locks regardless, and this only makes
    /// it happen sooner. Use [`abort`] instead when you need to know it
    /// landed.
    ///
    /// Outside a Tokio runtime there is nothing to spawn onto, so this
    /// does nothing at all and the deadline does the work.
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

/// Walks a cluster's keys in order, a batch at a time.
///
/// Built by [`Client::key_iterator`] (a stale walk) or
/// [`Transaction::key_iterator`] (one whose keys are locked as they are
/// handed over). Not a [`std::iter::Iterator`]: every batch is a round
/// trip, so advancing has to be awaited.
///
/// The cost is one RPC per [`batch_size`] keys rather than one per key.
/// Keys already fetched are handed out from memory, and the next batch
/// is asked for only when the last one runs out -- so a caller that
/// stops early never pays for the rest.
///
/// [`batch_size`]: KeyIterator::batch_size
pub struct KeyIterator {
    client: Client,
    transaction_id: Option<Vec<u8>>,
    /// Where the next batch starts, inclusive. `None` once the cluster
    /// has said there is nothing after what we have.
    next_start: Option<Vec<u8>>,
    batch_size: u32,
    /// The current batch, reversed so that taking the next key is a
    /// `pop` off the end rather than a shift off the front.
    buffer: Vec<Vec<u8>>,
}

impl KeyIterator {
    fn new(client: Client, transaction_id: Option<Vec<u8>>, start_key: &[u8]) -> Self {
        Self {
            client,
            transaction_id,
            next_start: Some(start_key.to_vec()),
            batch_size: DEFAULT_KEY_BATCH,
            buffer: Vec::new(),
        }
    }

    /// How many keys to fetch per round trip. Defaults to
    /// [`DEFAULT_KEY_BATCH`]; a server that won't scan this many in one
    /// call refuses rather than shortening it, so the error surfaces
    /// here rather than as a quietly smaller batch.
    #[must_use]
    pub fn batch_size(mut self, keys: u32) -> Self {
        self.batch_size = keys;
        self
    }

    /// The next key, or `None` at the end of the keyspace.
    ///
    /// Answered from the buffer when it can be; otherwise this is where
    /// the round trip happens. Inside a transaction, that round trip is
    /// also where the next batch's keys are locked, so this is a point
    /// at which the transaction can be aborted -- see [`Error::Aborted`].
    pub async fn next(&mut self) -> Result<Option<Vec<u8>>, Error> {
        loop {
            if let Some(key) = self.buffer.pop() {
                return Ok(Some(key));
            }
            let Some(start) = self.next_start.take() else {
                return Ok(None);
            };

            let (mut keys, has_more) = self
                .client
                .get_keys_batch(start, self.batch_size, self.transaction_id.clone())
                .await?;

            // The next batch starts just past the last key of this one.
            // A single zero byte appended is the smallest key that can
            // sort after it, so nothing between the two can be skipped
            // -- there is nothing between them.
            if has_more && let Some(last) = keys.last() {
                let mut next = last.clone();
                next.push(0);
                self.next_start = Some(next);
            }

            // An empty batch with `has_more` set can't happen, but if it
            // did, `next_start` would be unchanged and this would spin.
            // The loop only goes round again on a non-empty batch.
            if keys.is_empty() {
                return Ok(None);
            }
            keys.reverse();
            self.buffer = keys;
        }
    }

    /// Everything from here to the end of the keyspace, in order.
    ///
    /// Convenient for a keyspace known to be small; for one that isn't,
    /// prefer [`next`] and stop when you have what you need, since this
    /// keeps going -- and, inside a transaction, keeps locking.
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
