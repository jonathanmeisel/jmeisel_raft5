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
    ConfigChangeResponse, CreateSnapshotRequest, CreateSnapshotResponse, CreateTransactionRequest,
    CreateInviteRequest, CreateInviteResponse, CreateTransactionResponse, CreateUserRequest,
    CreateUserResponse, DeleteUserRequest,
    DeleteUserResponse, DropTransactionRequest, GetKeysRequest, GetKeysResponse, GetVersionRequest,
    GetVersionResponse, ListInvitesRequest, ListInvitesResponse, ListSnapshotsRequest,
    ListSnapshotsResponse, LoginRequest, LoginResponse, RedeemInviteRequest, RedeemInviteResponse,
    RevokeInviteRequest, RevokeInviteResponse,
    RaftError as PbRaftError, ReadRequest, ReadResponse, RemoveServerRequest,
    RemoveSnapshotRequest, RemoveSnapshotResponse, WriteRequest, WriteResponse, auth_error,
    commit_transaction_response, config_change_response, create_snapshot_response,
    create_invite_response, create_transaction_response, create_user_response,
    delete_user_response, get_keys_request, get_keys_response, get_version_request,
    get_version_response, list_invites_response, list_snapshots_response, login_response,
    raft_error, read_request, read_response, redeem_invite_response, remove_snapshot_response,
    revoke_invite_response, transaction_aborted, write_response,
};
use raft5_shared::raft5::client::{
    Acl as PbAcl, AclEntry as PbAclEntry, CreateGroupRequest, CreateGroupResponse,
    DeleteAclRequest, DeleteAclResponse, DeleteGroupRequest, DeleteGroupResponse, GetAclRequest,
    GetAclResponse, GetGroupRequest, GetGroupResponse, Group as PbGroup, ListAclsRequest,
    ListAclsResponse, ListGroupsRequest, ListGroupsResponse, Principal as PbPrincipal,
    Right as PbRight, SetAclRequest, SetAclResponse, UpdateGroupRequest, UpdateGroupResponse,
    create_group_response, delete_acl_response, delete_group_response, get_acl_response,
    get_group_response, list_acls_response, list_groups_response, principal, set_acl_response,
    update_group_response,
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
    /// The cluster would not accept who this client is: no token, or one
    /// it did not sign, or one that has expired.
    ///
    /// Never retried. If the client has credentials from an earlier
    /// [`login`](Client::login) it logs in again once and retries by
    /// itself before reporting this, so seeing it means that failed too
    /// -- or that there were no credentials to try.
    Unauthenticated(String),
    /// The cluster knows who this client is and will not let them do
    /// this. Never retried: time does not grant permissions.
    PermissionDenied(String),
    /// An account request the cluster understood and refused. Never
    /// retried, for the same reason -- a wrong password does not come
    /// right on the second attempt.
    Denied {
        reason: Denial,
        /// What the cluster said, for a human to read.
        detail: String,
    },
}

/// One batch of a key walk, as the cluster answered it.
///
/// Internal: [`KeyIterator`] is the public way to walk keys, and the
/// only reason this is a type of its own is that `has_more` and
/// `next_start_key` are two different facts -- a batch can be short, or
/// empty, and still have keys after it.
struct KeyBatch {
    keys: Vec<Vec<u8>>,
    has_more: bool,
    /// Where the server says the next batch starts. Absent from an
    /// older server, and from a batch that is the last one.
    next_start_key: Option<Vec<u8>>,
}

/// Someone an ACL or a group can name.
///
/// Users and groups are separate namespaces: `Principal::user("ops")`
/// and `Principal::group("ops")` are two different principals, and
/// nothing resolves one to the other.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Principal {
    User(String),
    Group(String),
}

impl Principal {
    pub fn user(name: impl Into<String>) -> Self {
        Self::User(name.into())
    }

    pub fn group(name: impl Into<String>) -> Self {
        Self::Group(name.into())
    }

    fn to_pb(&self) -> PbPrincipal {
        PbPrincipal {
            kind: Some(match self {
                Self::User(name) => principal::Kind::User(name.clone()),
                Self::Group(name) => principal::Kind::Group(name.clone()),
            }),
        }
    }

    /// A principal off the wire. A kind this client does not know about
    /// is left out of the list it was in rather than failing the call:
    /// an ACL from a newer cluster is still worth most of what it says.
    fn from_pb(pb: &PbPrincipal) -> Option<Self> {
        match &pb.kind {
            Some(principal::Kind::User(name)) => Some(Self::User(name.clone())),
            Some(principal::Kind::Group(name)) => Some(Self::Group(name.clone())),
            None => None,
        }
    }
}

impl std::fmt::Display for Principal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::User(name) => write!(f, "user:{name}"),
            Self::Group(name) => write!(f, "group:{name}"),
        }
    }
}

/// What an ACL grants. `Write` includes everything `Read` does.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Right {
    Read,
    Write,
}

impl Right {
    fn to_pb(self) -> i32 {
        match self {
            Self::Read => PbRight::Read as i32,
            Self::Write => PbRight::Write as i32,
        }
    }

    fn from_pb(right: i32) -> Option<Self> {
        match PbRight::try_from(right) {
            Ok(PbRight::Read) => Some(Self::Read),
            Ok(PbRight::Write) => Some(Self::Write),
            _ => None,
        }
    }
}

impl std::fmt::Display for Right {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Read => "read",
            Self::Write => "write",
        })
    }
}

/// One access control list: the key prefix it governs and what it grants.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Acl {
    /// The prefix. Empty governs every key there is.
    pub prefix: Vec<u8>,
    pub entries: Vec<(Principal, Right)>,
    /// The seq the record was last written at.
    pub seq: u64,
}

impl Acl {
    fn from_pb(pb: PbAcl) -> Self {
        Self {
            prefix: pb.prefix,
            entries: pb
                .entries
                .iter()
                .filter_map(|entry| {
                    let principal = Principal::from_pb(entry.principal.as_ref()?)?;
                    Some((principal, Right::from_pb(entry.right)?))
                })
                .collect(),
            seq: pb.seq,
        }
    }
}

/// What governs one key, and what it grants the account that asked.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AclFor {
    /// The ACL that governs the key, or `None` when nothing does --
    /// which means the key is open to every authenticated account.
    pub acl: Option<Acl>,
    /// What that ACL grants this account, or `None` when it grants it
    /// nothing, and when nothing governs the key.
    ///
    /// Note that an administrator sees what the ACL says about them,
    /// not their bypass: the question is about the ACL.
    pub effective: Option<Right>,
}

/// One group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Group {
    pub name: String,
    /// The account that created it, which need not still administer it.
    pub created_by: String,
    /// Who may change it.
    pub admins: Vec<Principal>,
    /// Who is in it. A group here is a nested group: everything in it
    /// is in this one too, transitively.
    pub members: Vec<Principal>,
    /// The seq the record was last written at.
    pub seq: u64,
}

impl Group {
    fn from_pb(pb: PbGroup) -> Self {
        Self {
            name: pb.name,
            created_by: pb.created_by,
            admins: pb.admins.iter().filter_map(Principal::from_pb).collect(),
            members: pb.members.iter().filter_map(Principal::from_pb).collect(),
            seq: pb.seq,
        }
    }
}

fn entries_to_pb(entries: &[(Principal, Right)]) -> Vec<PbAclEntry> {
    entries
        .iter()
        .map(|(principal, right)| PbAclEntry {
            principal: Some(principal.to_pb()),
            right: right.to_pb(),
        })
        .collect()
}

/// Why the cluster refused an account request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Denial {
    /// The username or the password was wrong. Which of the two is not
    /// disclosed, so this is the answer for an account that does not
    /// exist as well.
    BadCredentials,
    /// An account by that name already exists.
    AlreadyExists,
    /// There is no account by that name to remove.
    NoSuchUser,
    /// The caller is not an administrator.
    NotAdmin,
    /// The cluster was started without a signing secret, so it keeps no
    /// accounts and issues no tokens.
    AuthDisabled,
    /// The invitation offered cannot be spent: unknown, already
    /// redeemed, withdrawn, lapsed, or issued for a different username.
    /// Which of those is not distinguished -- there is one thing to do
    /// about any of them, which is to ask for another.
    BadInvite,
    /// An ACL governs the key or prefix asked about, and does not grant
    /// this account what the call needed. `Client::get_acl` says which
    /// ACL decided and what it grants.
    NotPermitted,
    /// There is no group by that name.
    NoSuchGroup,
    /// There is no ACL on that exact prefix. A longer or a shorter one
    /// may still govern the keys under it.
    NoSuchAcl,
    /// A reason this version of the client does not know about.
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
            Self::Unauthenticated(why) => {
                write!(f, "the cluster did not accept this client's identity: {why}")
            }
            Self::PermissionDenied(why) => write!(f, "not permitted: {why}"),
            Self::Denied { reason, detail } if detail.is_empty() => write!(f, "{reason}"),
            Self::Denied { detail, .. } => f.write_str(detail),
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

/// A point-in-time view of the store, pinned until it is removed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotInfo {
    pub id: Vec<u8>,
    /// The sequence number this snapshot reads at. Every write with a
    /// seq at or below this one is visible through it; nothing above it
    /// is, however long the snapshot lives.
    pub seq: u64,
    /// What the creating client's clock said at the time. Informational
    /// only -- it is one machine's wall clock, recorded verbatim, and
    /// nothing in the cluster depends on it.
    pub created_unix_ms: u64,
}

/// One outstanding invitation, as an administrator sees it.
///
/// Everything about an invitation except the secret, which exists only
/// in the reply to the [`create_invite`] that minted it.
///
/// [`create_invite`]: Client::create_invite
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Invite {
    /// What to name this invitation in a [`revoke_invite`] -- the hash
    /// of the token rather than the token, so it can be written down and
    /// passed around without being worth anything.
    ///
    /// [`revoke_invite`]: Client::revoke_invite
    pub id: String,
    /// The administrator who issued it.
    pub issued_by: String,
    /// Whether the account it creates will be an administrator. Settled
    /// when the invitation was issued; the recipient has no say.
    pub is_admin: bool,
    /// The one username it may create, if it was issued for a particular
    /// person.
    pub username: Option<String>,
    /// When it stops being redeemable, by the cluster's clock.
    pub expires_unix_ms: u64,
}

/// A newly minted invitation: the secret to pass on, and the public
/// record of it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewInvite {
    /// **The only time this is ever available.** The cluster keeps a
    /// hash of it and cannot produce it again; an issuer who loses it
    /// has to withdraw the invitation and mint another.
    pub token: String,
    pub invite: Invite,
    /// The sequence number the invitation was written at.
    pub seq: u64,
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
    /// An account request the cluster refused on its merits. Never
    /// retried: the answer will not change.
    Denied { reason: Denial, detail: String },
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
    /// For a read that is guaranteed current, use
    /// [`read_linearizable`], or open a [`transaction`] and read inside
    /// it if the key must also *stay* as it was read.
    ///
    /// [`read_linearizable`]: Client::read_linearizable
    /// [`transaction`]: Client::transaction
    pub async fn read(&self, key: &[u8]) -> Result<ReadResult, Error> {
        self.read_inner(key, None).await
    }

    /// Reads `key` as of the moment the call is answered, reflecting
    /// everything committed anywhere in the cluster. The node answering
    /// proves it still leads and still holds a quorum of leases first,
    /// which costs a round of lease checks that [`read`] does not.
    ///
    /// Nothing is locked, so the answer describes an instant that has
    /// already passed: the key may change immediately after. That is the
    /// difference from reading inside a [`transaction`], which holds the
    /// key against conflicting writes until the transaction ends.
    ///
    /// [`read`]: Client::read
    /// [`transaction`]: Client::transaction
    pub async fn read_linearizable(&self, key: &[u8]) -> Result<ReadResult, Error> {
        self.read_inner(key, Some(read_request::Consistency::Linearizable(true)))
            .await
    }

    /// The sequence number `key`'s current value was written at, without
    /// fetching the value -- 0 if it has no current value.
    ///
    /// Answered from an index of seqs alone, so it costs the same
    /// whatever the value weighs. This is the call for "has this changed
    /// since I last looked?"; [`read`] can answer that only by sending
    /// the whole value back for you to discard.
    ///
    /// Stale on exactly the terms [`read`] is stale. See
    /// [`get_version_linearizable`] and [`Transaction::get_version`].
    ///
    /// [`read`]: Client::read
    /// [`get_version_linearizable`]: Client::get_version_linearizable
    pub async fn get_version(&self, key: &[u8]) -> Result<u64, Error> {
        self.get_version_inner(key, None).await
    }

    /// [`get_version`], as of the moment the call is answered, on the
    /// same terms (and at the same cost) as [`read_linearizable`].
    ///
    /// [`get_version`]: Client::get_version
    /// [`read_linearizable`]: Client::read_linearizable
    pub async fn get_version_linearizable(&self, key: &[u8]) -> Result<u64, Error> {
        self.get_version_inner(
            key,
            Some(get_version_request::Consistency::Linearizable(true)),
        )
        .await
    }

    /// Reads `key` as it was when snapshot `snapshot_id` was created --
    /// whatever has happened to it since, including being deleted.
    ///
    /// Stale in the same sense [`read`] is, and deliberately so: it
    /// describes a moment that has already passed, so any node can
    /// serve it without proving it still leads. What it *does*
    /// guarantee is stability: the same call against the same snapshot
    /// gives the same answer for as long as that snapshot exists.
    ///
    /// [`read`]: Client::read
    pub async fn read_at(&self, snapshot_id: &[u8], key: &[u8]) -> Result<ReadResult, Error> {
        self.read_inner(
            key,
            Some(read_request::Consistency::SnapshotId(snapshot_id.to_vec())),
        )
        .await
    }

    /// [`get_version`] as of a snapshot. See [`read_at`].
    ///
    /// [`get_version`]: Client::get_version
    /// [`read_at`]: Client::read_at
    pub async fn get_version_at(&self, snapshot_id: &[u8], key: &[u8]) -> Result<u64, Error> {
        self.get_version_inner(
            key,
            Some(get_version_request::Consistency::SnapshotId(
                snapshot_id.to_vec(),
            )),
        )
        .await
    }

    /// Walks the keys that had a value when `snapshot_id` was created --
    /// including ones deleted since, and excluding ones created since.
    pub fn key_iterator_at(&self, snapshot_id: &[u8], start_key: &[u8]) -> KeyIterator {
        KeyIterator::new(
            self.clone(),
            Some(get_keys_request::Consistency::SnapshotId(
                snapshot_id.to_vec(),
            )),
            start_key,
        )
    }

    /// Pins the current sequence number as a snapshot, so that
    /// [`read_at`] and friends can later see the store exactly as it is
    /// now.
    ///
    /// `id` must be unique; creating one that already exists is refused
    /// rather than moving the existing snapshot forward. Passing `None`
    /// uses this machine's clock, formatted as a timestamp -- which is
    /// where a clock belongs, since the cluster has no wall clock of its
    /// own and a leader-assigned one would be a replicated
    /// non-determinism. A collision at the same millisecond comes back
    /// as an ordinary already-exists, which [`create_snapshot`] retries
    /// past with a suffix.
    ///
    /// Administrators only: until it is removed, a snapshot holds every
    /// version of every key that has changed since it was taken.
    ///
    /// [`read_at`]: Client::read_at
    /// [`create_snapshot`]: Client::create_snapshot
    pub async fn create_snapshot(&self, id: Option<&[u8]>) -> Result<SnapshotInfo, Error> {
        let now = unix_millis();
        match id {
            Some(id) => self.create_snapshot_once(id, now).await,
            None => {
                let base = timestamp_id(now);
                match self.create_snapshot_once(base.as_bytes(), now).await {
                    // Two snapshots defaulted in the same millisecond.
                    // Disambiguating here rather than making the caller
                    // do it: they asked for "a snapshot of now", and
                    // which suffix it gets is not their problem.
                    Err(Error::Rejected(_)) => {
                        let mut last = Err(Error::Rejected("snapshot id taken".into()));
                        for n in 1..16u32 {
                            let id = format!("{base}-{n}");
                            last = self.create_snapshot_once(id.as_bytes(), now).await;
                            if !matches!(last, Err(Error::Rejected(_))) {
                                break;
                            }
                        }
                        last
                    }
                    other => other,
                }
            }
        }
    }

    async fn create_snapshot_once(
        &self,
        id: &[u8],
        created_unix_ms: u64,
    ) -> Result<SnapshotInfo, Error> {
        let request = CreateSnapshotRequest {
            snapshot_id: id.to_vec(),
            created_unix_ms,
        };
        self.call(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.create_snapshot(request).await.map(create_snapshot_reply) }
        })
        .await
    }

    /// Releases a snapshot. The versions it was holding become
    /// collectable, and reads at it start failing. Administrators only.
    pub async fn remove_snapshot(&self, id: &[u8]) -> Result<u64, Error> {
        let request = RemoveSnapshotRequest {
            snapshot_id: id.to_vec(),
        };
        self.call(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.remove_snapshot(request).await.map(remove_snapshot_reply) }
        })
        .await
    }

    /// Every snapshot that currently exists.
    ///
    /// The only way to discover ids: snapshot records live in the
    /// reserved range, which [`key_iterator`] hides. Possibly stale, on
    /// the same terms as any read outside a transaction.
    ///
    /// [`key_iterator`]: Client::key_iterator
    pub async fn list_snapshots(&self) -> Result<Vec<SnapshotInfo>, Error> {
        self.call(|mut kv: Kv| async move {
            kv.list_snapshots(ListSnapshotsRequest {})
                .await
                .map(list_snapshots_reply)
        })
        .await
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

    async fn read_inner(
        &self,
        key: &[u8],
        consistency: Option<read_request::Consistency>,
    ) -> Result<ReadResult, Error> {
        let request = ReadRequest {
            key: key.to_vec(),
            consistency,
        };
        self.call(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.read(request).await.map(read_reply) }
        })
        .await
    }

    async fn get_version_inner(
        &self,
        key: &[u8],
        consistency: Option<get_version_request::Consistency>,
    ) -> Result<u64, Error> {
        let request = GetVersionRequest {
            key: key.to_vec(),
            consistency,
        };
        self.call(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.get_version(request).await.map(get_version_reply) }
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
                    // The cluster's answer, not this node's mood, so
                    // asking the next node would collect the same one.
                    Reply::Denied { reason, detail } => {
                        return Err(Error::Denied { reason, detail });
                    }
                    // A membership change has no key and so no
                    // precondition, and it is not a transaction; every
                    // other variant is unreachable here, and each says
                    // the same thing anyway -- this node said no.
                    _ => last = Some(format!("{} refused to bootstrap", node.key)),
                },
                // Reported rather than folded into the "nobody could
                // serve it" below: an operator whose token is missing or
                // whose identity is not the one this node was started
                // with needs to be told that, not told the cluster was
                // busy. Same reasoning as in `call_inner`.
                Err(status) if status.code() == tonic::Code::Unauthenticated => {
                    return Err(Error::Unauthenticated(status.message().to_string()));
                }
                Err(status) if status.code() == tonic::Code::PermissionDenied => {
                    return Err(Error::PermissionDenied(status.message().to_string()));
                }
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

    /// Logs in, and uses the resulting token for everything this client
    /// does from here on -- including connections it already holds, and
    /// every clone of it.
    ///
    /// Against a cluster started without a signing secret this fails
    /// with [`Denial::AuthDisabled`]: there is nothing to log in to.
    ///
    /// The credentials are kept in memory so that a token expiring
    /// mid-session can be replaced without the caller noticing. For a
    /// caller unwilling to hold a password for the life of the client,
    /// [`login_with_token`] does the same job without that -- at the
    /// cost of surfacing the expiry.
    ///
    /// [`login_with_token`]: Client::login_with_token
    pub async fn login(&self, username: &str, password: &str) -> Result<(), Error> {
        self.refresh_token(username, password).await?;
        self.router
            .set_credentials(Some((username.to_string(), password.to_string())));
        Ok(())
    }

    /// Uses a token obtained elsewhere.
    ///
    /// Nothing is validated here -- the first request to carry it is
    /// what finds out whether the cluster agrees. No credentials are
    /// stored, so an expiry is reported as
    /// [`Error::Unauthenticated`] rather than quietly replaced.
    pub fn login_with_token(&self, token: String) {
        self.router.set_token(Some(token));
        self.router.set_credentials(None);
    }

    /// Forgets the token and any credentials held with it.
    pub fn logout(&self) {
        self.router.set_token(None);
        self.router.set_credentials(None);
    }

    /// Whether this client currently holds a token. Says nothing about
    /// whether the cluster still accepts it.
    pub fn is_logged_in(&self) -> bool {
        self.router.has_token()
    }

    /// Who this client logged in as, if it logged in.
    ///
    /// `None` for a client holding a token it was handed rather than one
    /// it asked for: a token names its holder, but this client does not
    /// read the tokens it carries.
    pub fn username(&self) -> Option<String> {
        self.router.username()
    }

    /// Creates an account. Administrators only.
    ///
    /// `is_admin` decides whether the new account may in turn manage
    /// accounts. Returns the sequence number the account was written at.
    pub async fn create_user(
        &self,
        username: &str,
        password: &str,
        is_admin: bool,
    ) -> Result<u64, Error> {
        let request = CreateUserRequest {
            username: username.to_string(),
            password: password.to_string(),
            is_admin,
        };
        self.call(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.create_user(request).await.map(create_user_reply) }
        })
        .await
    }

    /// Removes an account. Administrators only.
    ///
    /// A token already issued to that account goes on being accepted
    /// until it expires -- nothing can un-issue one -- but it buys no
    /// administrative rights from the moment the account is gone.
    pub async fn delete_user(&self, username: &str) -> Result<u64, Error> {
        let request = DeleteUserRequest {
            username: username.to_string(),
        };
        self.call(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.delete_user(request).await.map(delete_user_reply) }
        })
        .await
    }

    /// Mints an invitation, so that someone without an account can
    /// create one for themselves. Administrators only.
    ///
    /// The returned [`NewInvite::token`] is the thing to pass on, and
    /// this is the only moment it exists: the cluster stores a hash of
    /// it and cannot produce it again.
    ///
    /// `is_admin` fixes what the resulting account will be -- an
    /// administrator's decision, made here, not the recipient's.
    /// `username`, if given, pins the invitation to that one name, so a
    /// misdelivered invitation cannot become an account under an
    /// unexpected one. `ttl` is refused outright if it is longer than
    /// the cluster is willing to grant, rather than quietly shortened;
    /// `Duration::ZERO` asks for the cluster's default.
    pub async fn create_invite(
        &self,
        is_admin: bool,
        ttl: Duration,
        username: Option<&str>,
    ) -> Result<NewInvite, Error> {
        let request = CreateInviteRequest {
            is_admin,
            ttl_ms: ttl.as_millis() as u64,
            username: username.map(str::to_string),
        };
        self.call(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.create_invite(request).await.map(create_invite_reply) }
        })
        .await
    }

    /// Spends an invitation on a new account, choosing its password.
    ///
    /// The one call on this client that needs no login -- a caller
    /// holding an invitation is by definition someone with no account
    /// yet. It does not log the client in either: the account exists
    /// afterwards, and [`login`](Client::login) is what gets a token for
    /// it.
    ///
    /// The invitation is spent in the same transaction that writes the
    /// account, so it works exactly once. Two failures leave it
    /// unspent and are worth telling apart:
    /// [`Denial::AlreadyExists`] means the username is taken and
    /// another can be tried with the same token, while
    /// [`Denial::BadInvite`] means the token itself is no good and a new
    /// one has to come from whoever issued it.
    pub async fn redeem_invite(
        &self,
        token: &str,
        username: &str,
        password: &str,
    ) -> Result<u64, Error> {
        let request = RedeemInviteRequest {
            token: token.to_string(),
            username: username.to_string(),
            password: password.to_string(),
        };
        // `call_inner(.., false)`: there are no credentials to fall back
        // on here and nothing to fall back to. A client redeeming an
        // invitation has no account to log in to -- that is what it is
        // in the middle of creating.
        self.call_inner(
            |mut kv: Kv| {
                let request = request.clone();
                async move { kv.redeem_invite(request).await.map(redeem_invite_reply) }
            },
            false,
        )
        .await
    }

    /// Every invitation that is still outstanding. Administrators only.
    ///
    /// Never the secrets -- those are gone. Lapsed invitations are not
    /// listed: they cannot be spent, and the cluster sweeps them itself.
    pub async fn list_invites(&self) -> Result<Vec<Invite>, Error> {
        self.call(|mut kv: Kv| async move {
            kv.list_invites(ListInvitesRequest {})
                .await
                .map(list_invites_reply)
        })
        .await
    }

    /// Withdraws an outstanding invitation, by its
    /// [`id`](Invite::id). Administrators only.
    ///
    /// The remedy for one sent to the wrong person: the secret cannot be
    /// recalled, only made worthless. An invitation that has already
    /// been redeemed is [`Error::Rejected`] -- which is the answer worth
    /// having, since it means someone got there first.
    pub async fn revoke_invite(&self, invite_id: &str) -> Result<u64, Error> {
        let request = RevokeInviteRequest {
            invite_id: invite_id.to_string(),
        };
        self.call(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.revoke_invite(request).await.map(revoke_invite_reply) }
        })
        .await
    }

    // -- groups and ACLs ----------------------------------------------
    //
    // Every call here that *changes* something contends for one key --
    // the permission epoch, which all of them write -- so two at once
    // resolve the way any two conflicting transactions do and one may
    // come back [`Error::Aborted`]. That is deliberate: it is what lets
    // the cluster decide each of these against a view that cannot move
    // underneath it, so that (say) a write grant being revoked cannot be
    // spent in its last moment on an ACL granting it back. Replaying an
    // aborted one is always safe, since it wrote nothing.
    //
    // Reading them contends for nothing.

    /// Creates a group. Any authenticated account may, up to a
    /// per-account quota; this client's account becomes its sole
    /// administrator whether or not `admins` says so.
    ///
    /// A group is a name for a set of principals and is worth something
    /// only when an ACL names it, so creating one grants its creator
    /// nothing they did not already have.
    pub async fn create_group(
        &self,
        name: &str,
        members: &[Principal],
        admins: &[Principal],
    ) -> Result<u64, Error> {
        let request = CreateGroupRequest {
            name: name.to_string(),
            members: members.iter().map(Principal::to_pb).collect(),
            admins: admins.iter().map(Principal::to_pb).collect(),
        };
        self.call(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.create_group(request).await.map(create_group_reply) }
        })
        .await
    }

    /// Deletes a group. Its administrators, or a cluster administrator.
    ///
    /// ACLs that named it are left alone: a grant to a group that no
    /// longer exists matches nobody.
    pub async fn delete_group(&self, name: &str) -> Result<u64, Error> {
        let request = DeleteGroupRequest {
            name: name.to_string(),
        };
        self.call(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.delete_group(request).await.map(delete_group_reply) }
        })
        .await
    }

    /// Adds and removes members and administrators, in one transaction.
    /// The group's administrators, or a cluster administrator.
    ///
    /// Additions are applied before removals, so a principal in both
    /// lists ends up removed -- which is what makes "these exactly" a
    /// single call that cannot land halfway.
    pub async fn update_group(
        &self,
        name: &str,
        add_members: &[Principal],
        remove_members: &[Principal],
        add_admins: &[Principal],
        remove_admins: &[Principal],
    ) -> Result<u64, Error> {
        let request = UpdateGroupRequest {
            name: name.to_string(),
            add_members: add_members.iter().map(Principal::to_pb).collect(),
            remove_members: remove_members.iter().map(Principal::to_pb).collect(),
            add_admins: add_admins.iter().map(Principal::to_pb).collect(),
            remove_admins: remove_admins.iter().map(Principal::to_pb).collect(),
        };
        self.call(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.update_group(request).await.map(update_group_reply) }
        })
        .await
    }

    /// One group, as stored -- members and admins are not expanded, so a
    /// nested group appears as itself.
    pub async fn get_group(&self, name: &str) -> Result<Group, Error> {
        let request = GetGroupRequest {
            name: name.to_string(),
        };
        self.call(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.get_group(request).await.map(get_group_reply) }
        })
        .await
    }

    /// Every group, in name order.
    pub async fn list_groups(&self) -> Result<Vec<Group>, Error> {
        self.call(|mut kv: Kv| async move {
            kv.list_groups(ListGroupsRequest {})
                .await
                .map(list_groups_reply)
        })
        .await
    }

    /// Writes the ACL governing a key prefix, replacing whatever it
    /// held. An empty prefix governs every key there is.
    ///
    /// Requires `write` on that prefix -- which, by the longest-prefix
    /// rule, means write on it or on a shorter prefix that governs it.
    /// A cluster administrator may always.
    ///
    /// What it will not accept is the *default*: creating an ACL where
    /// no ACL currently decides is an administrator's act, because a key
    /// nothing governs is open to everyone and that is permission to use
    /// the key rather than authority over who else may.
    pub async fn set_acl(
        &self,
        prefix: &[u8],
        entries: &[(Principal, Right)],
    ) -> Result<u64, Error> {
        let request = SetAclRequest {
            prefix: prefix.to_vec(),
            entries: entries_to_pb(entries),
        };
        self.call(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.set_acl(request).await.map(set_acl_reply) }
        })
        .await
    }

    /// Removes the ACL on a key prefix, so the next shorter one that has
    /// one governs instead.
    ///
    /// Same authority as writing it, with the same exception from the
    /// other side: a removal that would leave the prefix governed by
    /// nothing, and so open to everyone, is an administrator's act.
    pub async fn delete_acl(&self, prefix: &[u8]) -> Result<u64, Error> {
        let request = DeleteAclRequest {
            prefix: prefix.to_vec(),
        };
        self.call(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.delete_acl(request).await.map(delete_acl_reply) }
        })
        .await
    }

    /// Which ACL governs `key`, and what it grants this client's
    /// account.
    ///
    /// The question someone holding a [`Denial::NotPermitted`] actually
    /// has, answered in one round trip rather than by reimplementing
    /// group expansion against [`list_acls`](Client::list_acls).
    pub async fn get_acl(&self, key: &[u8]) -> Result<AclFor, Error> {
        let request = GetAclRequest { key: key.to_vec() };
        self.call(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.get_acl(request).await.map(get_acl_reply) }
        })
        .await
    }

    /// Every ACL, shortest prefix first -- which is the order they take
    /// effect in, least specific to most.
    pub async fn list_acls(&self) -> Result<Vec<Acl>, Error> {
        self.call(|mut kv: Kv| async move {
            kv.list_acls(ListAclsRequest {}).await.map(list_acls_reply)
        })
        .await
    }

    /// Gets a token and starts using it, without touching whatever
    /// credentials are stored.
    ///
    /// `call_inner(.., false)`: `Login` is the one call that must not
    /// answer an expired token by logging in again, which is what it is
    /// in the middle of doing.
    async fn refresh_token(&self, username: &str, password: &str) -> Result<(), Error> {
        let request = LoginRequest {
            username: username.to_string(),
            password: password.to_string(),
        };
        let routed = self
            .call_inner(
                |mut kv: Kv| {
                    let request = request.clone();
                    async move { kv.login(request).await.map(login_reply) }
                },
                false,
            )
            .await;

        let token = match routed {
            Ok(token) => token,
            // Nobody could be shown to be leading. For a cluster that
            // has not been bootstrapped yet that is not a failure, it is
            // the normal state -- and logging in is precisely what an
            // operator has to do there, to get the token `Bootstrap`
            // will ask for. A node with no configuration answers a login
            // from the credentials it was started with and needs no
            // leader to do it, so ask the nodes directly.
            Err(Error::Unavailable(why)) => match self.login_directly(&request).await {
                Ok(token) => token,
                // Report the original complaint rather than the second
                // one: "no leader" is the more useful of the two, and
                // the direct attempt was the speculative one.
                Err(Error::Unavailable(_)) => return Err(Error::Unavailable(why)),
                Err(e) => return Err(e),
            },
            Err(e) => return Err(e),
        };
        self.router.set_token(Some(token));
        Ok(())
    }

    /// One `Login` per known endpoint, in turn, with no leader routing.
    ///
    /// The fallback for a cluster with no leader to route to. Sent to
    /// each node rather than one because this client's seeds may include
    /// nodes that are not the one holding the startup credentials.
    async fn login_directly(&self, request: &LoginRequest) -> Result<String, Error> {
        let mut last: Option<String> = None;
        for node in self.router.nodes() {
            let mut kv = node.connect();
            match kv.login(request.clone()).await {
                Ok(resp) => match login_reply(resp) {
                    Reply::Ok(token) => return Ok(token),
                    // A refusal on the merits is the cluster's answer,
                    // not this node's, so asking the next one would only
                    // collect the same refusal again.
                    Reply::Denied { reason, detail } => {
                        return Err(Error::Denied { reason, detail });
                    }
                    _ => last = Some(format!("{} could not answer a login", node.key)),
                },
                Err(status) => last = Some(format!("{}: {status}", node.key)),
            }
        }
        Err(Error::Unavailable(last.unwrap_or_else(|| {
            "no cluster member answered a login".to_string()
        })))
    }

    /// Runs one RPC against the leader, following redirects and retrying
    /// transient refusals until [`DEADLINE`].
    async fn call<T, F, Fut>(&self, op: F) -> Result<T, Error>
    where
        F: Fn(Kv) -> Fut,
        Fut: Future<Output = Result<Reply<T>, Status>>,
    {
        self.call_inner(op, true).await
    }

    /// `call`, saying whether an expired token may be replaced by
    /// logging in again. Only `Login` itself passes `false`, so that
    /// answering an expiry cannot recurse into answering an expiry.
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
                        Ok(Ok(Reply::Denied { reason, detail })) => {
                            return Err(Error::Denied { reason, detail });
                        }
                        // Before the `is_permanent` arm below, which
                        // would otherwise turn these into `Rejected` and
                        // lose the distinction.
                        //
                        // An expired token is the common case here, and
                        // a client that knows what it logged in with can
                        // simply get another. Once only: an identity the
                        // cluster genuinely will not accept has to fail
                        // rather than spin.
                        Ok(Err(status)) if status.code() == tonic::Code::Unauthenticated => {
                            if allow_relogin
                                && !relogged_in
                                && let Some((username, password)) = self.router.credentials()
                            {
                                relogged_in = true;
                                // Boxed to break the type-level cycle:
                                // this calls `refresh_token`, which
                                // calls back into `call_inner`. It can
                                // never recurse at runtime -- that call
                                // passes `allow_relogin: false` -- but
                                // the future's size would be infinite
                                // without an indirection here.
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
///
/// `Unauthenticated` and `PermissionDenied` belong on that list too and
/// are absent only because `call_inner` has already returned on both,
/// with error types that say more than `Rejected` would.
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
        Some(write_response::Result::AuthError(e)) => denied(e),
        None => empty_response(),
    }
}

fn get_keys_reply(resp: tonic::Response<GetKeysResponse>) -> Reply<KeyBatch> {
    match resp.into_inner().result {
        Some(get_keys_response::Result::Ok(ok)) => Reply::Ok(KeyBatch {
            keys: ok.keys,
            has_more: ok.has_more,
            next_start_key: (!ok.next_start_key.is_empty()).then_some(ok.next_start_key),
        }),
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
        Some(read_response::Result::AuthError(e)) => denied(e),
        None => empty_response(),
    }
}

fn snapshot_info(s: raft5_shared::raft5::client::Snapshot) -> SnapshotInfo {
    SnapshotInfo {
        id: s.snapshot_id,
        seq: s.seq,
        created_unix_ms: s.created_unix_ms,
    }
}

fn create_snapshot_reply(resp: tonic::Response<CreateSnapshotResponse>) -> Reply<SnapshotInfo> {
    match resp.into_inner().result {
        Some(create_snapshot_response::Result::Ok(s)) => Reply::Ok(snapshot_info(s)),
        Some(create_snapshot_response::Result::Error(e)) => Reply::from_error(e),
        Some(create_snapshot_response::Result::AuthError(e)) => denied(e),
        None => empty_response(),
    }
}

fn remove_snapshot_reply(resp: tonic::Response<RemoveSnapshotResponse>) -> Reply<u64> {
    match resp.into_inner().result {
        Some(remove_snapshot_response::Result::Seq(seq)) => Reply::Ok(seq),
        Some(remove_snapshot_response::Result::Error(e)) => Reply::from_error(e),
        Some(remove_snapshot_response::Result::AuthError(e)) => denied(e),
        None => empty_response(),
    }
}

fn list_snapshots_reply(resp: tonic::Response<ListSnapshotsResponse>) -> Reply<Vec<SnapshotInfo>> {
    match resp.into_inner().result {
        Some(list_snapshots_response::Result::Ok(list)) => {
            Reply::Ok(list.snapshots.into_iter().map(snapshot_info).collect())
        }
        Some(list_snapshots_response::Result::Error(e)) => Reply::from_error(e),
        None => empty_response(),
    }
}

/// Milliseconds since the Unix epoch, by this machine's clock.
///
/// The cluster has no wall clock, so a timestamp only ever means "what
/// the client that asked believed the time to be".
fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// A default snapshot id: the timestamp, in a form the server's id
/// character set accepts and that sorts chronologically.
fn timestamp_id(unix_ms: u64) -> String {
    format!("t{unix_ms}")
}

fn get_version_reply(resp: tonic::Response<GetVersionResponse>) -> Reply<u64> {
    match resp.into_inner().result {
        Some(get_version_response::Result::Ok(ok)) => Reply::Ok(ok.seq),
        Some(get_version_response::Result::Error(e)) => Reply::from_error(e),
        Some(get_version_response::Result::AuthError(e)) => denied(e),
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

fn create_group_reply(resp: tonic::Response<CreateGroupResponse>) -> Reply<u64> {
    match resp.into_inner().result {
        Some(create_group_response::Result::Seq(seq)) => Reply::Ok(seq),
        Some(create_group_response::Result::Error(e)) => Reply::from_error(e),
        Some(create_group_response::Result::AuthError(e)) => denied(e),
        None => empty_response(),
    }
}

fn delete_group_reply(resp: tonic::Response<DeleteGroupResponse>) -> Reply<u64> {
    match resp.into_inner().result {
        Some(delete_group_response::Result::Seq(seq)) => Reply::Ok(seq),
        Some(delete_group_response::Result::Error(e)) => Reply::from_error(e),
        Some(delete_group_response::Result::AuthError(e)) => denied(e),
        None => empty_response(),
    }
}

fn update_group_reply(resp: tonic::Response<UpdateGroupResponse>) -> Reply<u64> {
    match resp.into_inner().result {
        Some(update_group_response::Result::Seq(seq)) => Reply::Ok(seq),
        Some(update_group_response::Result::Error(e)) => Reply::from_error(e),
        Some(update_group_response::Result::AuthError(e)) => denied(e),
        None => empty_response(),
    }
}

fn get_group_reply(resp: tonic::Response<GetGroupResponse>) -> Reply<Group> {
    match resp.into_inner().result {
        Some(get_group_response::Result::Ok(group)) => Reply::Ok(Group::from_pb(group)),
        Some(get_group_response::Result::Error(e)) => Reply::from_error(e),
        Some(get_group_response::Result::AuthError(e)) => denied(e),
        None => empty_response(),
    }
}

fn list_groups_reply(resp: tonic::Response<ListGroupsResponse>) -> Reply<Vec<Group>> {
    match resp.into_inner().result {
        Some(list_groups_response::Result::Ok(list)) => {
            Reply::Ok(list.groups.into_iter().map(Group::from_pb).collect())
        }
        Some(list_groups_response::Result::Error(e)) => Reply::from_error(e),
        Some(list_groups_response::Result::AuthError(e)) => denied(e),
        None => empty_response(),
    }
}

fn set_acl_reply(resp: tonic::Response<SetAclResponse>) -> Reply<u64> {
    match resp.into_inner().result {
        Some(set_acl_response::Result::Seq(seq)) => Reply::Ok(seq),
        Some(set_acl_response::Result::Error(e)) => Reply::from_error(e),
        Some(set_acl_response::Result::AuthError(e)) => denied(e),
        None => empty_response(),
    }
}

fn delete_acl_reply(resp: tonic::Response<DeleteAclResponse>) -> Reply<u64> {
    match resp.into_inner().result {
        Some(delete_acl_response::Result::Seq(seq)) => Reply::Ok(seq),
        Some(delete_acl_response::Result::Error(e)) => Reply::from_error(e),
        Some(delete_acl_response::Result::AuthError(e)) => denied(e),
        None => empty_response(),
    }
}

fn get_acl_reply(resp: tonic::Response<GetAclResponse>) -> Reply<AclFor> {
    match resp.into_inner().result {
        Some(get_acl_response::Result::Ok(ok)) => Reply::Ok(AclFor {
            acl: ok.acl.map(Acl::from_pb),
            effective: ok.effective.and_then(Right::from_pb),
        }),
        Some(get_acl_response::Result::Error(e)) => Reply::from_error(e),
        Some(get_acl_response::Result::AuthError(e)) => denied(e),
        None => empty_response(),
    }
}

fn list_acls_reply(resp: tonic::Response<ListAclsResponse>) -> Reply<Vec<Acl>> {
    match resp.into_inner().result {
        Some(list_acls_response::Result::Ok(list)) => {
            Reply::Ok(list.acls.into_iter().map(Acl::from_pb).collect())
        }
        Some(list_acls_response::Result::Error(e)) => Reply::from_error(e),
        Some(list_acls_response::Result::AuthError(e)) => denied(e),
        None => empty_response(),
    }
}

fn login_reply(resp: tonic::Response<LoginResponse>) -> Reply<String> {
    match resp.into_inner().result {
        Some(login_response::Result::Ok(ok)) => Reply::Ok(ok.token),
        Some(login_response::Result::Error(e)) => Reply::from_error(e),
        Some(login_response::Result::AuthError(e)) => denied(e),
        None => empty_response(),
    }
}

/// An `Invite` off the wire.
fn invite_from_wire(invite: raft5_shared::raft5::client::Invite) -> Invite {
    Invite {
        id: invite.invite_id,
        issued_by: invite.issued_by,
        is_admin: invite.is_admin,
        username: invite.username,
        expires_unix_ms: invite.expires_unix_ms,
    }
}

fn create_invite_reply(resp: tonic::Response<CreateInviteResponse>) -> Reply<NewInvite> {
    match resp.into_inner().result {
        // A reply with the token but no record is treated like any other
        // half-filled one. It is not papered over with a default,
        // because an invitation shown with no expiry would read as one
        // that never lapses.
        Some(create_invite_response::Result::Ok(ok)) => match ok.invite.map(invite_from_wire) {
            Some(invite) => Reply::Ok(NewInvite {
                token: ok.token,
                invite,
                seq: ok.seq,
            }),
            None => empty_response(),
        },
        Some(create_invite_response::Result::Error(e)) => Reply::from_error(e),
        Some(create_invite_response::Result::AuthError(e)) => denied(e),
        None => empty_response(),
    }
}

fn redeem_invite_reply(resp: tonic::Response<RedeemInviteResponse>) -> Reply<u64> {
    match resp.into_inner().result {
        Some(redeem_invite_response::Result::Ok(ok)) => Reply::Ok(ok.seq),
        Some(redeem_invite_response::Result::Error(e)) => Reply::from_error(e),
        Some(redeem_invite_response::Result::AuthError(e)) => denied(e),
        None => empty_response(),
    }
}

fn list_invites_reply(resp: tonic::Response<ListInvitesResponse>) -> Reply<Vec<Invite>> {
    match resp.into_inner().result {
        Some(list_invites_response::Result::Ok(list)) => {
            Reply::Ok(list.invites.into_iter().map(invite_from_wire).collect())
        }
        Some(list_invites_response::Result::Error(e)) => Reply::from_error(e),
        Some(list_invites_response::Result::AuthError(e)) => denied(e),
        None => empty_response(),
    }
}

fn revoke_invite_reply(resp: tonic::Response<RevokeInviteResponse>) -> Reply<u64> {
    match resp.into_inner().result {
        Some(revoke_invite_response::Result::Seq(seq)) => Reply::Ok(seq),
        Some(revoke_invite_response::Result::Error(e)) => Reply::from_error(e),
        Some(revoke_invite_response::Result::AuthError(e)) => denied(e),
        None => empty_response(),
    }
}

fn create_user_reply(resp: tonic::Response<CreateUserResponse>) -> Reply<u64> {
    match resp.into_inner().result {
        Some(create_user_response::Result::Seq(seq)) => Reply::Ok(seq),
        Some(create_user_response::Result::Error(e)) => Reply::from_error(e),
        Some(create_user_response::Result::AuthError(e)) => denied(e),
        None => empty_response(),
    }
}

fn delete_user_reply(resp: tonic::Response<DeleteUserResponse>) -> Reply<u64> {
    match resp.into_inner().result {
        Some(delete_user_response::Result::Seq(seq)) => Reply::Ok(seq),
        Some(delete_user_response::Result::Error(e)) => Reply::from_error(e),
        Some(delete_user_response::Result::AuthError(e)) => denied(e),
        None => empty_response(),
    }
}

/// An `AuthError` off the wire. An unrecognized kind is still a refusal
/// -- reported as such rather than retried, since a cluster that named a
/// reason meant it.
fn denied<T>(error: raft5_shared::raft5::client::AuthError) -> Reply<T> {
    Reply::Denied {
        reason: auth_error::Kind::try_from(error.kind)
            .map_or(Denial::Unknown, Denial::from),
        detail: error.detail,
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
        Some(config_change_response::Result::AuthError(e)) => denied(e),
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
        self.client
            .read_inner(
                key,
                Some(read_request::Consistency::TransactionId(self.id.clone())),
            )
            .await
    }

    /// The sequence number `key`'s current value was written at, without
    /// fetching the value -- 0 if it has no current value.
    ///
    /// Holds `key` against conflicting writes for the rest of the
    /// transaction exactly as [`read`] does, so this is the cheap half of
    /// a check-then-write: take the version, decide, write, commit, and
    /// the commit fails if anyone else touched the key in between.
    ///
    /// The answer describes *committed* state even for a key this
    /// transaction has itself buffered a write for -- a buffered write
    /// has no sequence number until the commit assigns one.
    ///
    /// [`read`]: Transaction::read
    pub async fn get_version(&self, key: &[u8]) -> Result<u64, Error> {
        self.client
            .get_version_inner(
                key,
                Some(get_version_request::Consistency::TransactionId(
                    self.id.clone(),
                )),
            )
            .await
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
        KeyIterator::new(
            self.client.clone(),
            Some(get_keys_request::Consistency::TransactionId(
                self.id.clone(),
            )),
            start_key,
        )
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
    /// What the walk asks for: a transaction, a snapshot, or nothing at
    /// all. Fixed when the iterator is made, so every batch is asked
    /// for on the same terms -- a walk that changed consistency
    /// half-way would not describe any one state of the store.
    consistency: Option<get_keys_request::Consistency>,
    /// Where the next batch starts, inclusive. `None` once the cluster
    /// has said there is nothing after what we have.
    next_start: Option<Vec<u8>>,
    batch_size: u32,
    /// The current batch, reversed so that taking the next key is a
    /// `pop` off the end rather than a shift off the front.
    buffer: Vec<Vec<u8>>,
}

impl KeyIterator {
    fn new(
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

            let batch = self
                .client
                .get_keys_batch(start, self.batch_size, self.consistency.clone())
                .await?;
            let mut keys = batch.keys;

            // Where to go next. The server's own cursor when it sent
            // one, because only it knows why the batch ended: a walk
            // can stop having examined far more keys than it kept --
            // hidden by an ACL, or deleted by the transaction -- and
            // then there is no last key to derive a cursor from.
            //
            // Otherwise the last key of this batch with a single zero
            // byte appended, which is the smallest key that can sort
            // after it, so nothing between the two can be skipped --
            // there is nothing between them. That is also what an older
            // server, which sends no cursor, needs.
            self.next_start = match batch.next_start_key {
                Some(next) => Some(next),
                None if batch.has_more => keys.last().map(|last| {
                    let mut next = last.clone();
                    next.push(0);
                    next
                }),
                None => None,
            };

            // An empty batch used not to be able to carry `has_more`.
            // It can now -- a walk that examined its whole budget
            // without finding a key this caller may see is exactly
            // that -- so the loop goes round on the cursor rather than
            // stopping, and stops only when there is no cursor left.
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
