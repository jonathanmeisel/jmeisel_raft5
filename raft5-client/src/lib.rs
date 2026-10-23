//! A client for a raft5 cluster.
//!
//! [`Client`] wraps the `Kv` service (`client.proto`). A caller writes
//! `client.write(key, value)` and gets an answer, without knowing which
//! node can serve it, or that the write is a transaction (the cluster's
//! only way to write). The client handles three things for the caller:
//!
//! * **Only the leader serves most requests.** A follower says so and does
//!   not forward (Raft §6.2). Each refusal carries a `leader_hint`, and the
//!   client follows it at once, without backoff: a redirect is not a
//!   failure.
//! * **The leader changes.** With no hint (an election is in progress, or
//!   the configuration is older than the leader), the client asks each
//!   known node for the cluster configuration and picks the one that says
//!   `is_leader`. Only a caught-up leader with a read quorum says that, so
//!   it is proof, not a guess.
//! * **Membership changes.** The client starts from seed addresses, but
//!   the cluster decides who its members are. So the client re-reads the
//!   configuration from the leader from time to time, adds servers it was
//!   never seeded with, and drops seeds that have left.
//!
//! The connection to the leader stays open between calls, so the common
//! case (same leader as last time) dials nothing and costs one round trip.

/// Define functions that turn one RPC's response into a [`Reply`].
///
/// Nearly every response is a `oneof` of a success, a `RaftError` and an
/// `AuthError`, and all but the success are handled the same way. So each
/// function names only its success arms; the macro adds the rest:
/// - `Error(e)` becomes [`Reply::from_error`];
/// - `AuthError(e)` becomes [`denied`];
/// - an empty `oneof` becomes [`empty_response`].
///
/// Write `without_auth` after the response module for a response with no
/// `AuthError` variant.
///
/// ```ignore
/// reply! {
///     fn delete_acl_reply(DeleteAclResponse, delete_acl_response) -> u64 {
///         Seq(seq) => Reply::Ok(seq),
///     }
/// }
/// ```
macro_rules! reply {
    () => {};
    (
        $(#[$meta:meta])*
        $vis:vis fn $name:ident($response:ident, $module:ident) -> $out:ty {
            $($variant:ident($($pat:tt)*) => $body:expr),+ $(,)?
        }
        $($rest:tt)*
    ) => {
        $(#[$meta])*
        $vis fn $name(resp: tonic::Response<$response>) -> $crate::Reply<$out> {
            match resp.into_inner().result {
                $(Some($module::Result::$variant($($pat)*)) => $body,)+
                Some($module::Result::Error(e)) => $crate::Reply::from_error(e),
                Some($module::Result::AuthError(e)) => $crate::denied(e),
                None => $crate::empty_response(),
            }
        }
        reply!($($rest)*);
    };
    (
        $(#[$meta:meta])*
        $vis:vis fn $name:ident($response:ident, $module:ident, without_auth) -> $out:ty {
            $($variant:ident($($pat:tt)*) => $body:expr),+ $(,)?
        }
        $($rest:tt)*
    ) => {
        $(#[$meta])*
        $vis fn $name(resp: tonic::Response<$response>) -> $crate::Reply<$out> {
            match resp.into_inner().result {
                $(Some($module::Result::$variant($($pat)*)) => $body,)+
                Some($module::Result::Error(e)) => $crate::Reply::from_error(e),
                None => $crate::empty_response(),
            }
        }
        reply!($($rest)*);
    };
}

mod accounts;
mod cluster;
mod data;
mod permissions;
mod routing;
mod snapshots;
mod transaction;

use std::future::Future;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use tonic::Status;
use tonic::transport::ClientTlsConfig;

pub use raft5_shared::{ADMINS_GROUP, BACKUP_OPERATORS_GROUP};
/// Re-exported so a caller can build a [`ClientTls`] without a direct
/// tonic dependency.
pub use tonic::transport::{Certificate, Identity};

pub use accounts::{Invite, NewInvite};
pub use cluster::{Member, Membership};
pub use data::{Consistency, DEFAULT_KEY_BATCH, Entry, KeyIterator, Read, Version};
pub use permissions::{Acl, AclFor, Group, Principal, Right};
pub use snapshots::{BackupInfo, SnapshotInfo};
pub use transaction::Transaction;

use raft5_shared::raft5::client::{
    RaftError as PbRaftError, auth_error, raft_error, transaction_aborted,
};
use routing::{Kv, Router};

/// How long a call keeps retrying before it gives up. On a healthy
/// cluster, everything a call retries for (a redirect, an election, a
/// membership change still committing) resolves well within this. After
/// it, something is wrong and the caller should hear about it.
///
/// This also limits a large transaction's commit, which must replicate to
/// a majority and apply within it. A commit that runs out of time returns
/// [`Error::InDoubt`], which a caller must never replay: "too slow"
/// becomes "unrecoverable", not "try again". If the cluster's transaction
/// size limit grows far past a few megabytes, raise this too.
const DEADLINE: Duration = Duration::from_secs(10);

/// How long to wait before resending a request refused for a reason only
/// time fixes (for example, no leader elected yet). A redirect with a
/// leader hint does not wait: there is a better node to ask now.
const RETRY_DELAY: Duration = Duration::from_millis(20);

/// How often to re-read the cluster's membership from the leader.
const CONFIG_REFRESH_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub enum Error {
    /// A seed address (or an address the cluster advertised) that is not a
    /// valid URI. Addresses look like `http://host:port`.
    InvalidAddress(String),
    /// The TLS configuration was refused, for example a certificate or key
    /// that is not valid PEM. Not a handshake failure: those happen per
    /// connection and are reported as [`Unavailable`].
    ///
    /// [`Unavailable`]: Error::Unavailable
    Tls(String),
    /// The client was built with no seeds, so it has nowhere to send
    /// anything.
    NoSeeds,
    /// Rejected before it was sent.
    InvalidArgument(&'static str),
    /// The cluster refused outright. Unlike [`Unavailable`], this is not
    /// about timing: no node will ever serve this request, so it was not
    /// retried. Examples: asking for more than a limit allows, or a
    /// malformed request.
    ///
    /// [`Unavailable`]: Error::Unavailable
    Rejected(String),
    /// Nobody served the request: no member did before the deadline, or (for
    /// `Bootstrap`, which does not retry) the node refused outright. The text
    /// is the last refusal or transport failure seen.
    Unavailable(String),
    /// The transaction is over, and nothing it buffered was written.
    ///
    /// This library does not retry it, because the retry is the whole
    /// transaction, reads included, and only the caller knows what those
    /// were. A replay is always safe: an aborted transaction wrote nothing.
    Aborted {
        reason: AbortReason,
        /// The contended key, for [`AbortReason::LockConflict`].
        key: Option<Vec<u8>>,
    },
    /// The commit reached the log, and then the leader changed, so nobody can
    /// say if it committed or was rolled back. `seq` is where the entry went.
    ///
    /// **Do not replay the transaction.** Every other failure here means
    /// nothing was written; this one means nobody knows. Running a
    /// read-modify-write again after it already applied applies it twice.
    /// Read the keys back and decide from what is there.
    InDoubt { seq: u64 },
    /// The cluster did not accept this client's identity: no token, a token
    /// it did not sign, or an expired one.
    ///
    /// Never retried. If the client has credentials from an earlier
    /// [`login`](Client::login), it logs in again once and retries before it
    /// reports this. So this error means that failed too, or that there were
    /// no credentials.
    Unauthenticated(String),
    /// The cluster knows who this client is and does not allow this. Never
    /// retried: time does not grant permissions.
    PermissionDenied(String),
    /// The thing the request names does not exist: a snapshot id nobody
    /// created (or that was removed), an invitation already redeemed. Never
    /// retried: the leader answered, and asking again gets the same answer.
    NotFound(String),
    /// An account request that the cluster understood and refused. Never
    /// retried, for the same reason: a wrong password does not become right
    /// on the second try.
    Denied {
        reason: Denial,
        /// What the cluster said, for a human.
        detail: String,
    },
}

/// Why the cluster refused an account request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Denial {
    /// The username or the password was wrong. The cluster does not say
    /// which, so this is also the answer for an account that does not exist.
    BadCredentials,
    /// An account with that name already exists.
    AlreadyExists,
    /// No account has that name.
    NoSuchUser,
    /// The caller is not an administrator.
    NotAdmin,
    /// The cluster was started without a signing secret, so it keeps no
    /// accounts and issues no tokens.
    AuthDisabled,
    /// The invitation cannot be used: unknown, already redeemed, withdrawn,
    /// lapsed, or issued for another username. The cluster does not say
    /// which, because the fix is the same for all: ask for another.
    BadInvite,
    /// An ACL governs the key or prefix and does not grant this account what
    /// the call needs. `Client::get_acl` shows which ACL decided and what it
    /// grants.
    NotPermitted,
    /// No group has that name.
    NoSuchGroup,
    /// No ACL is set on that exact prefix. A longer or shorter prefix may
    /// still govern the keys under it.
    NoSuchAcl,
    /// A reason this client version does not know.
    Unknown,
}

impl std::fmt::Display for Denial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let text = match self {
            Self::BadCredentials => "the username or password was wrong",
            Self::AlreadyExists => "that account already exists",
            Self::NoSuchUser => "there is no such account",
            Self::NotAdmin => "that account is not an administrator",
            Self::AuthDisabled => "this cluster keeps no accounts",
            Self::BadInvite => "that invitation cannot be used",
            Self::NotPermitted => "an ACL does not grant this account that",
            Self::NoSuchGroup => "there is no such group",
            Self::NoSuchAcl => "there is no ACL on that prefix",
            Self::Unknown => "the cluster refused",
        };
        f.write_str(text)
    }
}

impl From<auth_error::Kind> for Denial {
    fn from(kind: auth_error::Kind) -> Self {
        match kind {
            auth_error::Kind::BadCredentials => Self::BadCredentials,
            auth_error::Kind::AlreadyExists => Self::AlreadyExists,
            auth_error::Kind::NoSuchUser => Self::NoSuchUser,
            auth_error::Kind::NotAdmin => Self::NotAdmin,
            auth_error::Kind::AuthDisabled => Self::AuthDisabled,
            auth_error::Kind::BadInvite => Self::BadInvite,
            auth_error::Kind::NotPermitted => Self::NotPermitted,
            auth_error::Kind::NoSuchGroup => Self::NoSuchGroup,
            auth_error::Kind::NoSuchAcl => Self::NoSuchAcl,
            auth_error::Kind::Unspecified => Self::Unknown,
        }
    }
}

/// Why a transaction ended without a commit. Each of these means nothing
/// was written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AbortReason {
    /// A key was held by a transaction this one could not displace: an older
    /// one, or one already committing. Worth a retry: the holder will finish.
    LockConflict,
    /// The deadline passed before the commit. A longer deadline, or less
    /// work between opening and committing, would help.
    Expired,
    /// An older transaction took one of its keys, so that the older one makes
    /// progress instead of the two aborting each other forever. A retry is
    /// expected: this transaction is older than whatever starts next.
    Wounded,
    /// The leader changed. A transaction lives on the leader that created it
    /// and cannot finish anywhere else.
    LeaderChanged,
    /// The cluster has no transaction with this id: it was dropped, already
    /// committed, or died so long ago that its record is gone.
    Unknown,
}

impl AbortReason {
    fn from_wire(reason: transaction_aborted::Reason) -> Self {
        match reason {
            transaction_aborted::Reason::LockConflict => Self::LockConflict,
            transaction_aborted::Reason::Expired => Self::Expired,
            transaction_aborted::Reason::Wounded => Self::Wounded,
            transaction_aborted::Reason::LeaderChanged => Self::LeaderChanged,
            // `InDoubt` never gets here: it becomes `Error::InDoubt` first, so it
            // cannot be mistaken for a reason that is safe to replay.
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
            Self::Tls(why) => write!(f, "unusable TLS configuration: {why}"),
            Self::NoSeeds => write!(f, "a client needs at least one seed address"),
            Self::InvalidArgument(why) => write!(f, "{why}"),
            Self::Rejected(why) => write!(f, "the cluster refused the request: {why}"),
            Self::Unavailable(why) => write!(f, "the cluster could not serve the request: {why}"),
            Self::Aborted {
                reason,
                key: Some(key),
            } => write!(
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
            Self::Unauthenticated(why) => {
                write!(
                    f,
                    "the cluster did not accept this client's identity: {why}"
                )
            }
            Self::PermissionDenied(why) => write!(f, "not permitted: {why}"),
            Self::NotFound(why) => write!(f, "not found: {why}"),
            Self::Denied { reason, detail } if detail.is_empty() => write!(f, "{reason}"),
            Self::Denied { detail, .. } => f.write_str(detail),
        }
    }
}

impl std::error::Error for Error {}

/// The result of one attempt, reduced to what the retry loop cares about:
/// done, redirected, or try again.
enum Reply<T> {
    Ok(T),
    /// Not the leader. The `Option<String>` is the hinted leader's
    /// `server_id`, or `None` if it knows no leader either.
    NotLeader(Option<String>),
    /// Refused for a reason that may pass by itself: no leader yet, a
    /// membership change in flight, a leader that has not yet committed an
    /// entry from its own term.
    Unavailable(String),
    /// The transaction is over. Not retried here: the retry is the whole
    /// transaction, which only its caller can replay.
    Aborted {
        reason: AbortReason,
        key: Option<Vec<u8>>,
    },
    /// The commit is in doubt. Never retried by anyone.
    InDoubt {
        seq: u64,
    },
    /// An account request the cluster refused on its merits. Never retried:
    /// the answer will not change.
    Denied {
        reason: Denial,
        detail: String,
    },
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
                    // Handle this first: it is the one outcome that must never be retried,
                    // so it must never reach a variant that is.
                    Ok(transaction_aborted::Reason::InDoubt) => Self::InDoubt {
                        seq: aborted.seq.unwrap_or_default(),
                    },
                    Ok(reason) => Self::Aborted {
                        reason: AbortReason::from_wire(reason),
                        key: aborted.key,
                    },
                    // A reason this build does not know. In-doubt is the safe reading: it
                    // refuses to replay something that may have been written.
                    Err(_) => Self::InDoubt {
                        seq: aborted.seq.unwrap_or_default(),
                    },
                }
            }
            None => Self::Unavailable("the node refused without saying why".to_string()),
        }
    }
}

/// How a [`Client`] dials the cluster over TLS.
///
/// There is no default trust: only the certificates in `ca` are trusted,
/// not the system roots. So you trust a cluster with a self-signed
/// certificate by passing that certificate here, and a client built for
/// one cluster cannot be pointed at another server that merely has a
/// publicly valid certificate.
#[derive(Clone)]
pub struct ClientTls {
    ca: Certificate,
    identity: Option<Identity>,
    server_name: Option<String>,
}

impl ClientTls {
    /// Trust servers whose certificate chains to `ca`. `ca` may be a
    /// self-signed leaf used as its own anchor, if it is not marked
    /// `CA:TRUE`.
    pub fn new(ca: Certificate) -> Self {
        Self {
            ca,
            identity: None,
            server_name: None,
        }
    }

    /// Present `identity` to the server, for a cluster whose client API
    /// requires mTLS. If it lists extended key usages, it needs
    /// `clientAuth`.
    pub fn identity(mut self, identity: Identity) -> Self {
        self.identity = Some(identity);
        self
    }

    /// Check every server's certificate against `name` instead of the host in
    /// its address. For one certificate, with one name, shared by all nodes.
    pub fn server_name(mut self, name: impl Into<String>) -> Self {
        self.server_name = Some(name.into());
        self
    }

    fn into_config(self) -> ClientTlsConfig {
        let mut config = ClientTlsConfig::new().ca_certificate(self.ca);
        if let Some(identity) = self.identity {
            config = config.identity(identity);
        }
        if let Some(name) = self.server_name {
            config = config.domain_name(name);
        }
        config
    }
}

/// A handle to a raft5 cluster.
///
/// Cheap to clone (clones share one connection to the leader and one view
/// of the membership) and safe to use from many tasks at once.
#[derive(Clone)]
pub struct Client {
    router: Arc<Router>,
}

impl Client {
    /// A client that starts from `seeds`: client API addresses
    /// (`http://host:port`) of nodes that are, or will become, members. They
    /// are a starting point, not a fixed list: the cluster's own
    /// configuration takes over when any one of them answers.
    ///
    /// Call it inside a Tokio runtime: it starts the task that keeps the
    /// configuration current.
    pub fn new(seeds: impl IntoIterator<Item = impl AsRef<str>>) -> Result<Self, Error> {
        Self::build(seeds, None)
    }

    /// [`Client::new`], for a cluster whose client API uses TLS. Every node
    /// is dialed over TLS, whatever scheme its address names, including
    /// `http://`, which a cluster advertises when its configuration was made
    /// without TLS.
    pub fn with_tls(
        seeds: impl IntoIterator<Item = impl AsRef<str>>,
        tls: ClientTls,
    ) -> Result<Self, Error> {
        Self::build(seeds, Some(tls.into_config()))
    }

    fn build(
        seeds: impl IntoIterator<Item = impl AsRef<str>>,
        tls: Option<ClientTlsConfig>,
    ) -> Result<Self, Error> {
        let router = Arc::new(Router::new(seeds, tls)?);
        spawn_config_refresh(Arc::downgrade(&router));
        Ok(Self { router })
    }

    /// The client API addresses this client knows: the seeds until it reads
    /// the cluster's configuration, the membership after that.
    pub fn endpoints(&self) -> Vec<String> {
        self.router.endpoints()
    }

    /// Run one RPC on the leader, following redirects and retrying temporary
    /// refusals until [`DEADLINE`].
    async fn call<T, F, Fut>(&self, op: F) -> Result<T, Error>
    where
        F: Fn(Kv) -> Fut,
        Fut: Future<Output = Result<Reply<T>, Status>>,
    {
        self.call_inner(op, true).await
    }

    /// `call`, with a flag that says whether an expired token may be replaced
    /// by a new login. Only `Login` passes `false`, so handling an expiry
    /// cannot recurse into handling an expiry.
    async fn call_inner<T, F, Fut>(&self, op: F, allow_relogin: bool) -> Result<T, Error>
    where
        F: Fn(Kv) -> Fut,
        Fut: Future<Output = Result<Reply<T>, Status>>,
    {
        let deadline = Instant::now() + DEADLINE;
        let mut hint: Option<String> = None;
        let mut last: Option<String> = None;
        let mut relogged_in = false;

        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::Unavailable(last.unwrap_or_else(|| {
                    "no cluster member answered in time".to_string()
                })));
            }

            match self.router.leader(hint.take()).await {
                Ok(leader) => {
                    // Limited by what is left of the deadline, not by its own timeout: a
                    // write blocks until it commits, and the deadline is exactly how long
                    // that may take.
                    match tokio::time::timeout(remaining, op(leader.kv)).await {
                        Ok(Ok(Reply::Ok(value))) => return Ok(value),
                        Ok(Ok(Reply::NotLeader(new_hint))) => {
                            self.router.forget(&leader.key);
                            last = Some(format!("{} is not the leader", leader.key));
                            hint = new_hint;
                            // A hint names a better node to ask now. Waiting would only add delay
                            // to a request that can be served.
                            if hint.is_some() {
                                continue;
                            }
                        }
                        // The leader is the right node; the cluster is not ready to answer yet.
                        // Keep the connection.
                        Ok(Ok(Reply::Unavailable(why))) => last = Some(why),
                        // Neither is this loop's to retry. An aborted transaction must be
                        // replayed whole, reads included, which only its caller can do. An
                        // in-doubt commit must not be replayed at all.
                        Ok(Ok(Reply::Aborted { reason, key })) => {
                            return Err(Error::Aborted { reason, key });
                        }
                        Ok(Ok(Reply::InDoubt { seq })) => return Err(Error::InDoubt { seq }),
                        Ok(Ok(Reply::Denied { reason, detail })) => {
                            return Err(Error::Denied { reason, detail });
                        }
                        // Before the `is_permanent` arm below, which would turn these into
                        // `Rejected` and lose the difference.
                        //
                        // An expired token is the common case, and a client that knows its
                        // login can get a new token. Only once: an identity the cluster really
                        // rejects must fail, not loop.
                        Ok(Err(status)) if status.code() == tonic::Code::Unauthenticated => {
                            if allow_relogin
                                && !relogged_in
                                && let Some((username, password)) = self.router.credentials()
                            {
                                relogged_in = true;
                                // Boxed to break a type-level cycle: this calls `refresh_token`, which
                                // calls `call_inner`. It never recurses at runtime (that call passes
                                // `allow_relogin: false`), but without indirection the future's size
                                // would be infinite.
                                let again =
                                    Box::pin(self.refresh_token(&username, &password)).await;
                                if again.is_ok() {
                                    continue;
                                }
                            }
                            return Err(Error::Unauthenticated(status.message().to_string()));
                        }
                        Ok(Err(status)) if status.code() == tonic::Code::PermissionDenied => {
                            return Err(Error::PermissionDenied(status.message().to_string()));
                        }
                        Ok(Err(status)) if status.code() == tonic::Code::NotFound => {
                            return Err(Error::NotFound(status.message().to_string()));
                        }
                        // A refusal that time cannot fix. A retry would only spend the whole
                        // deadline to reach the same answer, then report it as if the cluster
                        // was busy, which hides the real reason when the caller most needs it.
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
                // No node could be shown to be leading this round, probably because of an
                // election. Look again soon.
                Err(Error::Unavailable(why)) => last = Some(why),
                Err(e) => return Err(e),
            }

            tokio::time::sleep(RETRY_DELAY).await;
        }
    }
}

/// A `Status` from a call that is not retried, as the error `call_inner`
/// would report for it.
fn status_error(status: Status) -> Error {
    match status.code() {
        tonic::Code::Unauthenticated => Error::Unauthenticated(status.message().to_string()),
        tonic::Code::PermissionDenied => Error::PermissionDenied(status.message().to_string()),
        tonic::Code::NotFound => Error::NotFound(status.message().to_string()),
        _ if is_permanent(&status) => Error::Rejected(status.message().to_string()),
        _ => Error::Unavailable(status.to_string()),
    }
}

/// Whether a `Status` says the request itself is wrong, not that this node
/// could not serve it. A short list on purpose: anything not named here is
/// retried, because treating a temporary failure as permanent is the worse
/// mistake.
///
/// `Unauthenticated`, `PermissionDenied` and `NotFound` also belong on the
/// list. They are missing only because `call_inner` already returns on all
/// three, with error types that say more than `Rejected`.
fn is_permanent(status: &Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::InvalidArgument | tonic::Code::ResourceExhausted
    )
}

/// Re-read the cluster's membership from the node the client believes is
/// leading, so servers added after the client started are reachable and
/// removed ones are forgotten.
///
/// Holds only a `Weak`, so the task ends when the last [`Client`] is
/// dropped, instead of keeping it and its connections alive forever. A
/// tick before any call has found a leader has nothing to refresh, so it is
/// skipped.
///
/// Before the first tick, it tries the seeds in turn until one returns a
/// configuration. So the client's endpoints become the cluster's own as
/// soon as any seed is reachable, not after the first request.
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

/// An `AuthError` from the wire. An unknown kind is still a refusal, not
/// retried: a cluster that names a reason means it.
fn denied<T>(error: raft5_shared::raft5::client::AuthError) -> Reply<T> {
    Reply::Denied {
        reason: auth_error::Kind::try_from(error.kind).map_or(Denial::Unknown, Denial::from),
        detail: error.detail,
    }
}

fn empty_response<T>() -> Reply<T> {
    Reply::Unavailable("the node sent a response with nothing in it".to_string())
}
