//! A client for a raft5 cluster.
//!
//! [`Client`] wraps the `Kv` service (`client.proto`) so that a caller
//! writes `client.write(key, value)` and gets an answer, rather than
//! having to know which of the cluster's nodes is currently able to give
//! one. Three things stand between those two:
//!
//! * **Only the leader can serve most requests**, and a node that isn't
//!   the leader says so rather than forwarding (Raft §6.2). Every such
//!   refusal carries a `leader_hint` naming whoever it last heard from,
//!   and the client follows it -- immediately, without backing off,
//!   since a redirect is not a failure.
//! * **The leader changes.** With no hint to follow (a cluster mid
//!   election, a configuration older than the current leader), the
//!   client finds it by asking each node it knows of for a *linearizable*
//!   read: only a leader still holding a read quorum will answer one, so
//!   an answer is proof rather than a guess.
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
    AddServerRequest, BootstrapRequest, ConfigChangeResponse, RaftError as PbRaftError,
    ReadRequest, ReadResponse, RemoveServerRequest, WriteRequest, WriteResponse,
    config_change_response, raft_error, read_response, write_response,
};

use routing::{Kv, Router};

/// How long a call keeps retrying before giving up. Everything a call is
/// retried for -- a redirect, an election in progress, a membership
/// change still committing -- resolves well inside this on a healthy
/// cluster; past it, something is actually wrong and a caller waiting on
/// a key/value store deserves to hear about it.
const DEADLINE: Duration = Duration::from_secs(10);

/// How long to wait before re-sending a request that was refused for a
/// reason only time fixes (no leader elected yet, say). A redirect
/// carrying a leader hint doesn't wait at all -- there is somewhere
/// better to ask right now.
const RETRY_DELAY: Duration = Duration::from_millis(20);

/// How often to re-read the cluster's membership from the node believed
/// to be leading.
const CONFIG_REFRESH_INTERVAL: Duration = Duration::from_secs(10);

/// How many times [`Client::update`] re-reads and re-runs its closure
/// after losing a race for the key. Each attempt is a fresh read of a
/// value someone else just wrote, so losing several in a row means real
/// contention on that one key rather than bad luck, and the caller is
/// better placed than this library to decide what to do about it.
pub const MAX_UPDATE_ATTEMPTS: usize = 5;

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
    /// Nobody served the request: either no member managed to before the
    /// deadline, or (for `Bootstrap`, which doesn't retry) the node
    /// refused outright. The text is the last refusal or transport
    /// failure seen.
    Unavailable(String),
    /// The key was written by someone else between this client reading
    /// it and writing it back, so nothing was written. `actual_seq` is
    /// what the key is at instead (0 if it has no value). From
    /// [`Client::update`] this means the key stayed contended for every
    /// attempt.
    Conflict { actual_seq: u64 },
    /// The closure passed to [`Client::update`] failed; nothing was
    /// written.
    Update(BoxError),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidAddress(address) => write!(f, "not a usable address: {address:?}"),
            Self::NoSeeds => write!(f, "a client needs at least one seed address"),
            Self::InvalidArgument(why) => write!(f, "{why}"),
            Self::Unavailable(why) => write!(f, "the cluster could not serve the request: {why}"),
            Self::Conflict { actual_seq } => {
                write!(
                    f,
                    "the key changed under this write; it is at seq {actual_seq}"
                )
            }
            Self::Update(e) => write!(f, "the update closure failed: {e}"),
        }
    }
}

impl std::error::Error for Error {}

/// Whatever the caller's own [`Client::update`] closure failed with --
/// the client has no idea what it was trying to do, only that it
/// couldn't.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

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
    /// A write whose precondition didn't hold. `Some(seq)` is what the
    /// key is actually at, which no amount of retrying will change --
    /// only re-reading and deciding again will. `None` means another
    /// write to the key is still in flight, which is transient.
    Conflict(Option<u64>),
}

impl<T> Reply<T> {
    fn from_error(error: PbRaftError) -> Self {
        match error.kind {
            Some(raft_error::Kind::NotLeader(not_leader)) => {
                Self::NotLeader(not_leader.leader_hint)
            }
            Some(raft_error::Kind::Unavailable(why) | raft_error::Kind::Timeout(why)) => {
                Self::Unavailable(why)
            }
            Some(raft_error::Kind::Conflict(conflict)) => Self::Conflict(conflict.actual_seq),
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
    /// *and* applied, so a read that follows it reflects it.
    ///
    /// Every write the cluster accepts is conditional (see
    /// [`update`], which this is the degenerate case of), so an
    /// unconditional overwrite is a read and a write: under contention
    /// it may lose the race and try again.
    ///
    /// A write interrupted by a lost connection may be retried after it
    /// has already committed, since nothing tells the client which of the
    /// two happened. The retry rewrites the same key with the same value,
    /// so the only trace of it is a second sequence number.
    ///
    /// [`update`]: Client::update
    pub async fn write(&self, key: &[u8], value: Option<&[u8]>) -> Result<u64, Error> {
        let deadline = Instant::now() + DEADLINE;
        loop {
            match self.update(key, |_| Ok(value.map(<[u8]>::to_vec))).await {
                // Unlike a real update there is nothing here to
                // reconsider -- the value doesn't depend on what the key
                // held -- so losing the race just means going round
                // again, to the same deadline the rest of the client
                // works to.
                Err(Error::Conflict { .. }) if Instant::now() < deadline => {}
                other => return other,
            }
        }
    }

    /// Reads `key`, hands what it holds to `f`, and writes back what `f`
    /// returns (`None` deletes the key) -- but only if nothing else
    /// wrote the key in between. This is the read-modify-write the store
    /// itself cannot do: a counter increment, an entry appended to a
    /// list, a flag flipped only if it was where the caller last saw it.
    ///
    /// Losing that race is not a failure: the value is re-read, `f` runs
    /// again on what is there now, and the write is retried, up to
    /// [`MAX_UPDATE_ATTEMPTS`] times. So `f` may run more than once and
    /// should compute rather than act -- anything it does besides
    /// returning a value will happen again on the next attempt. Its
    /// argument is `None` when the key currently has no value, and
    /// returning `Err` abandons the update without writing anything.
    ///
    /// ```no_run
    /// # async fn f(client: raft5_client::Client) -> Result<(), raft5_client::Error> {
    /// client
    ///     .update(b"hits", |current| {
    ///         let n: u64 = match current {
    ///             Some(bytes) => std::str::from_utf8(bytes)?.parse()?,
    ///             None => 0,
    ///         };
    ///         Ok(Some((n + 1).to_string().into_bytes()))
    ///     })
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub async fn update<F>(&self, key: &[u8], mut f: F) -> Result<u64, Error>
    where
        F: FnMut(Option<&[u8]>) -> Result<Option<Vec<u8>>, BoxError>,
    {
        let mut last_conflict = 0;
        for _ in 0..MAX_UPDATE_ATTEMPTS {
            let current = self.read(key).await?;
            let next = f(current.value.as_deref()).map_err(Error::Update)?;
            // Seq 0 is the cluster's own "this key has no value", so the
            // seq a read hands back is the precondition, unchanged,
            // whether or not the key was there.
            match self.write_at(key, next.as_deref(), current.seq).await {
                Err(Error::Conflict { actual_seq }) => last_conflict = actual_seq,
                other => return other,
            }
        }
        Err(Error::Conflict {
            actual_seq: last_conflict,
        })
    }

    /// Writes `value` to `key` only if it is still at `previous_seq` (0
    /// meaning it has no value), returning the sequence number the write
    /// committed at.
    ///
    /// Unlike [`write`], which retries an unconditional overwrite until it
    /// lands, this fails outright -- as [`Error::Conflict`] -- the moment
    /// someone else gets there first, since only the caller knows whether
    /// the new value is still right in that case. Use [`update`] instead
    /// when the new value depends on the old one rather than on a seq the
    /// caller already has in hand.
    ///
    /// [`write`]: Client::write
    /// [`update`]: Client::update
    pub async fn write_if(
        &self,
        key: &[u8],
        value: Option<&[u8]>,
        previous_seq: u64,
    ) -> Result<u64, Error> {
        self.write_at(key, value, previous_seq).await
    }

    /// One conditional write: applied only if `key` is still at
    /// `previous_seq` (0 meaning it has no value).
    async fn write_at(
        &self,
        key: &[u8],
        value: Option<&[u8]>,
        previous_seq: u64,
    ) -> Result<u64, Error> {
        let request = WriteRequest {
            key: key.to_vec(),
            value: value.map(<[u8]>::to_vec),
            previous_seq: Some(previous_seq),
        };
        self.call(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.write(request).await.map(write_reply) }
        })
        .await
    }

    /// Reads `key` linearizably: the answer reflects every write that
    /// completed before this call began. Served by the leader, and only
    /// while it can still prove it is one.
    pub async fn read(&self, key: &[u8]) -> Result<ReadResult, Error> {
        self.read_at(key, true).await
    }

    /// Reads `key` from whichever node the client is currently talking
    /// to, without checking that it still leads. Cheaper -- no
    /// read-quorum round -- at the price of possibly missing recent
    /// writes.
    pub async fn read_stale(&self, key: &[u8]) -> Result<ReadResult, Error> {
        self.read_at(key, false).await
    }

    async fn read_at(&self, key: &[u8], linearizable: bool) -> Result<ReadResult, Error> {
        let request = ReadRequest {
            key: key.to_vec(),
            linearizable,
        };
        self.call(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.read(request).await.map(read_reply) }
        })
        .await
    }

    /// Adds a server to the cluster, at the addresses its peer and
    /// client APIs are reachable on. It joins as a non-voting learner
    /// and is promoted automatically once it has caught up (Raft
    /// §4.2.1), so this one call is the whole operation.
    ///
    /// Returns once the change has committed. Only one membership change
    /// may be in flight at a time; a call that collides with another is
    /// retried like any other transient refusal.
    pub async fn add_server(
        &self,
        server_id: &str,
        peer_address: &str,
        client_address: &str,
    ) -> Result<(), Error> {
        let request = AddServerRequest {
            server_id: server_id.to_string(),
            address: peer_address.to_string(),
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
                    // precondition; `Conflict` is unreachable here.
                    Reply::NotLeader(_) | Reply::Unavailable(_) | Reply::Conflict(_) => {
                        last = Some(format!("{} refused to bootstrap", node.key));
                    }
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
                        // Nothing to decide: the key is at a version this
                        // write wasn't written against, and re-sending it
                        // unchanged can only be refused again. Whoever
                        // built the request has to look again.
                        Ok(Ok(Reply::Conflict(Some(actual_seq)))) => {
                            return Err(Error::Conflict { actual_seq });
                        }
                        // A conflict with a write still in flight, on the
                        // other hand, says nothing about the key yet --
                        // it resolves on its own once that one lands.
                        Ok(Ok(Reply::Conflict(None))) => {
                            last = Some(format!("{} has this key mid-write", leader.key));
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

/// Re-reads the cluster's membership from the node the client believes
/// is leading, so that servers added since the client started are
/// reachable and ones removed since are forgotten.
///
/// Holds only a `Weak`, so the task ends when the last [`Client`] is
/// dropped rather than keeping it (and its connections) alive forever.
/// There is nothing to refresh before the first call finds a leader, so
/// a tick with none is skipped.
fn spawn_config_refresh(router: Weak<Router>) {
    tokio::spawn(async move {
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

fn write_reply(resp: tonic::Response<WriteResponse>) -> Reply<u64> {
    match resp.into_inner().result {
        Some(write_response::Result::Seq(seq)) => Reply::Ok(seq),
        Some(write_response::Result::Error(e)) => Reply::from_error(e),
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
