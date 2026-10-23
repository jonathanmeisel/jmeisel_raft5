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

mod routing;

use std::future::Future;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use tonic::Status;
use tonic::transport::ClientTlsConfig;
/// Re-exported so a caller can build a [`ClientTls`] without a direct
/// tonic dependency.
pub use tonic::transport::{Certificate, Identity};

use raft5_shared::raft5::client::{
    AddServerRequest, BackupRequest, BootstrapFromBackupChunk, BootstrapRequest,
    CommitTransactionRequest, CommitTransactionResponse,
    ConfigChangeResponse, CreateSnapshotRequest, CreateSnapshotResponse, CreateTransactionRequest,
    CreateInviteRequest, CreateInviteResponse, CreateTransactionResponse, CreateUserRequest,
    CreateUserResponse, DeleteUserRequest,
    DeleteUserResponse, DropTransactionRequest, GetConfigRequest, GetKeysRequest, GetKeysResponse,
    Linearizable, ListInvitesRequest, ListInvitesResponse, ListSnapshotsRequest,
    ListSnapshotsResponse, LoginRequest, LoginResponse, RedeemInviteRequest, RedeemInviteResponse,
    RevokeInviteRequest, RevokeInviteResponse,
    RaftError as PbRaftError, ReadOk, ReadRequest, ReadResponse, RemoveServerRequest,
    RemoveSnapshotRequest, RemoveSnapshotResponse, SetHeartbeatRequest, WriteRequest,
    WriteResponse, auth_error,
    commit_transaction_response, config_change_response, create_snapshot_response,
    create_invite_response, create_transaction_response, create_user_response,
    delete_user_response, get_keys_request, get_keys_response, list_invites_response, list_snapshots_response, login_response,
    backup_chunk, bootstrap_from_backup_chunk, raft_error, read_request, read_response,
    redeem_invite_response, remove_snapshot_response,
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

/// The lifetime of the transaction that [`Client::write`] opens for the
/// caller. Short, because that transaction never waits on the caller: it
/// is three round trips end to end. A longer hold would only block the
/// next writer if this client stalls mid-write.
const IMPLICIT_TXN_TTL: Duration = Duration::from_secs(2);

/// How many keys a [`KeyIterator`] fetches per round trip by default.
/// Small enough that a short walk does not fetch a full page, large
/// enough that a long walk does not cost a round trip per key.
pub const DEFAULT_KEY_BATCH: u32 = 64;

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

/// Someone an ACL or a group can name.
///
/// Users and groups are separate namespaces: `Principal::user("ops")` and
/// `Principal::group("ops")` are two different principals, and neither
/// resolves to the other.
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

    /// A principal from the wire. A kind this client does not know is left
    /// out of its list instead of failing the call: an ACL from a newer
    /// cluster is still mostly useful.
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
    /// The prefix. An empty prefix governs every key.
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
    /// The ACL that governs the key, or `None` if none does. Then the key is
    /// open to every authenticated account.
    pub acl: Option<Acl>,
    /// What that ACL grants this account. `None` if it grants nothing, and
    /// when nothing governs the key.
    ///
    /// An administrator sees what the ACL says about them, not their bypass:
    /// the question is about the ACL.
    pub effective: Option<Right>,
}

/// One group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Group {
    pub name: String,
    /// The account that created it. It need not still administer the group.
    pub created_by: String,
    /// Who may change it.
    pub admins: Vec<Principal>,
    /// Who is in it. A group in this list is nested: all its members are
    /// members of this group too, transitively.
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
            Self::NotFound(why) => write!(f, "not found: {why}"),
            Self::Denied { reason, detail } if detail.is_empty() => write!(f, "{reason}"),
            Self::Denied { detail, .. } => f.write_str(detail),
        }
    }
}

impl std::error::Error for Error {}

/// Which moment a read outside a transaction describes, and what was
/// proved about it. For a read inside one, see [`Transaction::read`].
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

fn entries(keys: Vec<Vec<u8>>, ok: ReadOk) -> Read<Entry> {
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

fn versions(keys: Vec<Vec<u8>>, ok: ReadOk) -> Read<Version> {
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

/// What [`Client::backup`] wrote.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackupInfo {
    /// The size of the backup, in bytes.
    pub bytes: u64,
}

/// A point-in-time view of the store, kept until it is removed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotInfo {
    pub id: Vec<u8>,
    /// The seq this snapshot reads at. Every write at or below this seq is
    /// visible through it; nothing above it is, however long it lives.
    pub seq: u64,
    /// What the creating client's clock said. For information only: it is
    /// one machine's wall clock, stored as given, and nothing in the cluster
    /// depends on it.
    pub created_unix_ms: u64,
}

/// The cluster's membership, as [`Client::members`] reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Membership {
    /// The id of the member that answered, if it was leading when it
    /// answered. Through a [`Client`] it nearly always is, because the client
    /// asks the leader. `None` from a leader not yet ready to say so (see
    /// `GetConfigResponse.is_leader`).
    pub leader: Option<String>,
    pub members: Vec<Member>,
}

/// One member of the cluster.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Member {
    pub server_id: String,
    /// Where the other servers replicate to it: its peer API, the address
    /// given to [`Client::add_server`].
    pub peer_address: String,
    /// Where clients reach it.
    pub client_address: String,
    /// Whether it votes and counts toward the commit quorum. A server added
    /// with [`Client::add_server`] starts as a non-voting learner, and the
    /// leader promotes it once it keeps up in real time. So `true` for a new
    /// server means "it has caught up".
    pub voting: bool,
}

/// One open invitation, as an administrator sees it.
///
/// All facts about an invitation except the secret, which exists only in
/// the reply to the [`create_invite`] that created it.
///
/// [`create_invite`]: Client::create_invite
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Invite {
    /// The name to give [`revoke_invite`]: the hash of the token, not the
    /// token, so it can be written down and shared without being worth
    /// anything.
    ///
    /// [`revoke_invite`]: Client::revoke_invite
    pub id: String,
    /// The administrator who issued it.
    pub issued_by: String,
    /// Whether the account it creates will be an administrator. Set when it
    /// was issued; the recipient has no say.
    pub is_admin: bool,
    /// The only username it may create, if it was issued for one person.
    pub username: Option<String>,
    /// When it can no longer be redeemed, by the cluster's clock.
    pub expires_unix_ms: u64,
}

/// A new invitation: the secret to pass on, and its public record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewInvite {
    /// **The only time this is available.** The cluster keeps a hash and
    /// cannot produce it again. An issuer who loses it must withdraw the
    /// invitation and create another.
    pub token: String,
    pub invite: Invite,
    /// The seq the invitation was written at.
    pub seq: u64,
}

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
    Aborted { reason: AbortReason, key: Option<Vec<u8>> },
    /// The commit is in doubt. Never retried by anyone.
    InDoubt { seq: u64 },
    /// An account request the cluster refused on its merits. Never retried:
    /// the answer will not change.
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

    /// Walk the keys that had a value when `snapshot_id` was created,
    /// including keys deleted since and excluding keys created since.
    pub fn key_iterator_at(&self, snapshot_id: &[u8], start_key: &[u8]) -> KeyIterator {
        KeyIterator::new(
            self.clone(),
            Some(get_keys_request::Consistency::SnapshotId(
                snapshot_id.to_vec(),
            )),
            start_key,
        )
    }

    /// Save the current seq as a snapshot, so that reads at
    /// [`Consistency::Snapshot`] and [`key_iterator_at`] can see the store as
    /// it is now.
    ///
    /// `id` must be unique: creating one that exists is refused, not used to
    /// move the snapshot forward. `None` uses this machine's clock as a
    /// timestamp. The clock belongs on the client: the cluster has no wall
    /// clock, and a leader-assigned time would be replicated
    /// non-determinism. Two defaults in the same millisecond collide as a
    /// normal "already exists", which [`create_snapshot`] resolves with a
    /// suffix.
    ///
    /// Administrators only: until it is removed, a snapshot keeps every
    /// version of every key that changed after it.
    ///
    /// [`key_iterator_at`]: Client::key_iterator_at
    /// [`create_snapshot`]: Client::create_snapshot
    pub async fn create_snapshot(&self, id: Option<&[u8]>) -> Result<SnapshotInfo, Error> {
        let now = unix_millis();
        match id {
            Some(id) => self.create_snapshot_once(id, now).await,
            None => {
                let base = timestamp_id(now);
                match self.create_snapshot_once(base.as_bytes(), now).await {
                    // Two snapshots defaulted in the same millisecond. Resolve it here: the
                    // caller asked for "a snapshot of now", and the suffix is not its
                    // problem.
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

    /// Release a snapshot. The versions it kept can be collected, and reads
    /// at it start to fail. Administrators only.
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

    /// Every snapshot that exists.
    ///
    /// The only way to find ids: snapshot records live in the reserved range,
    /// which [`key_iterator`] hides. Possibly stale, like any read outside a
    /// transaction.
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

    /// Walk every key that has a value now, in order, from `start_key` on.
    /// Pass an empty slice to start at the beginning.
    ///
    /// Outside a transaction, like a [`read`] at [`Consistency::Stale`], this
    /// is stale: each batch is a local read on whichever node answered, and
    /// keys written since may be missing. For a current walk, use
    /// [`Transaction::key_iterator`].
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

    /// The one `Read` call behind every read method. Returns the keys it was
    /// asked about with the answer, so the caller can pair them.
    ///
    /// Checks the shape of the answer: it may cover fewer keys than asked,
    /// but never more, and never none of a non-empty list (the cluster always
    /// answers at least the first key).
    async fn read_inner(
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
        if ok.entries.len() > request.keys.len() || (ok.entries.is_empty() && !request.keys.is_empty())
        {
            return Err(Error::Unavailable(format!(
                "malformed read response: {} entries for {} keys",
                ok.entries.len(),
                request.keys.len()
            )));
        }
        Ok((request.keys, ok))
    }

    /// Add a server to the cluster, at the address of its peer API (the one
    /// other nodes replicate to). The leader dials it there (over the
    /// cluster's peer mTLS, if any) to ask the server its id and client
    /// address. It joins as a non-voting learner and is promoted
    /// automatically when it catches up (Raft §4.2.1), so this one call is the
    /// whole operation.
    ///
    /// The scheme of `peer_address` does not matter: the leader uses TLS or
    /// not, as its own configuration says.
    ///
    /// Returns once the change commits. Only one membership change can be in
    /// flight at a time; a call that collides with another is retried like
    /// any other temporary refusal.
    pub async fn add_server(&self, peer_address: &str) -> Result<(), Error> {
        let request = AddServerRequest {
            peer_address: peer_address.to_string(),
        };
        self.change_config(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.add_server(request).await.map(config_reply) }
        })
        .await
    }

    /// Remove a server from the cluster. Returns once the change commits.
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

    /// Change the cluster's heartbeat interval, from which every election and
    /// lease timeout is derived. Returns once the change commits. Raise it for
    /// nodes far apart: it should be well above the round trip between any two
    /// servers.
    pub async fn set_heartbeat(&self, heartbeat_ms: u64) -> Result<(), Error> {
        let request = SetHeartbeatRequest {
            heartbeat_millis: heartbeat_ms,
        };
        self.change_config(
            |mut kv: Kv| async move { kv.set_heartbeat(request).await.map(config_reply) },
        )
        .await
    }

    /// The cluster's heartbeat interval, as the node this client talks to
    /// sees it. Like a stale [`read`], it may briefly lag a change just made.
    ///
    /// [`read`]: Client::read
    pub async fn heartbeat_ms(&self) -> Result<u64, Error> {
        self.call(|mut kv: Kv| async move {
            kv.get_config(GetConfigRequest {})
                .await
                .map(|resp| Reply::Ok(resp.into_inner().heartbeat_millis))
        })
        .await
    }

    /// Every member of the cluster, and whether each votes yet, from the
    /// leader's configuration (a follower's may briefly lag). A server added
    /// with [`add_server`] shows as non-voting until it catches up.
    ///
    /// [`add_server`]: Client::add_server
    pub async fn members(&self) -> Result<Membership, Error> {
        self.call(|mut kv: Kv| async move {
            kv.get_config(GetConfigRequest {}).await.map(|resp| {
                let resp = resp.into_inner();
                let leader = resp
                    .is_leader
                    .then(|| resp.identity.map(|me| me.server_id))
                    .flatten();
                Reply::Ok(Membership {
                    leader,
                    members: resp
                        .members
                        .into_iter()
                        .map(|m| Member {
                            server_id: m.server_id,
                            peer_address: m.address,
                            client_address: m.client_address,
                            voting: m.voting,
                        })
                        .collect(),
                })
            })
        })
        .await
    }

    /// Form a single-node cluster around the node named `server_id` (Raft
    /// §4.4), which must be one of this client's endpoints. Every other node
    /// joins that cluster through [`add_server`].
    ///
    /// Sent to each endpoint in turn. A node refuses a `server_id` that is not
    /// its own, so the guard picks which node acts, and a node already in a
    /// cluster refuses outright. Unlike every other call, this one does not
    /// retry: bootstrap cannot be undone, and a node that just started may need
    /// a moment before it accepts one.
    ///
    /// `heartbeat_ms` is the new cluster's heartbeat interval, or `None` for
    /// the server's default. See [`set_heartbeat`].
    ///
    /// [`add_server`]: Client::add_server
    /// [`set_heartbeat`]: Client::set_heartbeat
    pub async fn bootstrap(&self, server_id: &str, heartbeat_ms: Option<u64>) -> Result<(), Error> {
        if server_id.is_empty() {
            return Err(Error::InvalidArgument(
                "bootstrap needs the id of the node to bootstrap",
            ));
        }

        let request = BootstrapRequest {
            server_id: server_id.to_string(),
            heartbeat_millis: heartbeat_ms.unwrap_or(0),
        };
        let mut last: Option<String> = None;
        for node in self.router.nodes() {
            let mut kv = node.connect();
            match kv.bootstrap(request.clone()).await {
                Ok(resp) => match config_reply(resp) {
                    Reply::Ok(()) => return Ok(()),
                    // The cluster's answer, not this node's state, so the next node would
                    // give the same one.
                    Reply::Denied { reason, detail } => {
                        return Err(Error::Denied { reason, detail });
                    }
                    // A membership change has no key, no precondition, and no transaction.
                    // Every other variant cannot happen here, and each means the same thing
                    // anyway: this node said no.
                    _ => last = Some(format!("{} refused to bootstrap", node.key)),
                },
                // Report this, do not fold it into "nobody could serve it" below: an
                // operator whose token is missing, or whose identity is not the one this
                // node was started with, must be told so, not told the cluster was busy.
                // Same reasoning as in `call_inner`.
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

    /// Stream a backup of the cluster's key/value state into `out`, as of now
    /// or as of the snapshot `snapshot_id`. Administrators only. Returns how
    /// many bytes were written.
    ///
    /// The backup holds every key a client wrote, with its newest value, plus
    /// accounts, groups and ACLs. It leaves out invitations, snapshots and
    /// the token signing secret: a cluster restored from it signs tokens with
    /// whatever secret it is started with, so old tokens work only if that is
    /// the same secret.
    ///
    /// Not retried once data has started to arrive: a backup that fails
    /// partway leaves `out` with a partial file, which a restore refuses.
    /// Write to a temporary file and rename it on success.
    ///
    /// See [`bootstrap_from_backup`] for the restore.
    ///
    /// [`bootstrap_from_backup`]: Client::bootstrap_from_backup
    pub async fn backup<W>(&self, snapshot_id: Option<&[u8]>, out: &mut W) -> Result<BackupInfo, Error>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        use tokio::io::AsyncWriteExt;

        let request = BackupRequest {
            snapshot_id: snapshot_id.unwrap_or_default().to_vec(),
        };
        let mut relogged_in = false;
        let mut stream = loop {
            let leader = self.router.leader(None).await?;
            match leader.kv.clone().backup(request.clone()).await {
                Ok(response) => break response.into_inner(),
                // An expired token is the common case, as in `call_inner`: log in
                // again once, if this client has credentials.
                Err(status) if status.code() == tonic::Code::Unauthenticated && !relogged_in => {
                    relogged_in = true;
                    match self.router.credentials() {
                        Some((username, password)) => {
                            self.refresh_token(&username, &password).await?;
                        }
                        None => return Err(status_error(status)),
                    }
                }
                Err(status) => return Err(status_error(status)),
            }
        };

        let mut bytes = 0u64;
        while let Some(chunk) = stream.message().await.map_err(status_error)? {
            match chunk.result {
                Some(backup_chunk::Result::Data(data)) => {
                    out.write_all(&data)
                        .await
                        .map_err(|e| Error::Rejected(format!("could not write the backup: {e}")))?;
                    bytes += data.len() as u64;
                }
                Some(backup_chunk::Result::AuthError(e)) => match denied::<()>(e) {
                    Reply::Denied { reason, detail } => return Err(Error::Denied { reason, detail }),
                    _ => unreachable!("`denied` only returns `Reply::Denied`"),
                },
                None => return Err(Error::Unavailable("the node sent an empty backup chunk".to_string())),
            }
        }
        out.flush()
            .await
            .map_err(|e| Error::Rejected(format!("could not write the backup: {e}")))?;
        Ok(BackupInfo { bytes })
    }

    /// Form a single-node cluster around the node named `server_id`, as
    /// [`bootstrap`] does, holding the contents of the backup that `backup`
    /// reads. Every other node then joins through [`add_server`], and gets the
    /// data by snapshot transfer.
    ///
    /// - The node must be in no cluster and hold no data.
    /// - Keys keep the seqs they had in the source cluster.
    /// - Accounts come from the backup, passwords included. The administrator
    ///   the node was started with is created only if the backup has no
    ///   account with that username; when it has one, the backed-up password
    ///   stays. This client must be logged in as that startup administrator,
    ///   as for [`bootstrap`].
    /// - The token signing secret is not in the backup: the new cluster uses
    ///   whatever secret it was started with.
    ///
    /// The node checks the backup before it becomes a cluster. A bad backup
    /// is [`Error::Rejected`] with the reason, and leaves the node as it was,
    /// so the call can be repeated. Not retried: the backup is read once.
    ///
    /// [`bootstrap`]: Client::bootstrap
    /// [`add_server`]: Client::add_server
    pub async fn bootstrap_from_backup<R>(
        &self,
        server_id: &str,
        heartbeat_ms: Option<u64>,
        mut backup: R,
    ) -> Result<(), Error>
    where
        R: tokio::io::AsyncRead + Unpin + Send + 'static,
    {
        use tokio::io::AsyncReadExt;

        if server_id.is_empty() {
            return Err(Error::InvalidArgument(
                "bootstrap needs the id of the node to bootstrap",
            ));
        }

        // Find the node by its own identity first: the backup can be read only
        // once, so it must go to the right node on the first try.
        let mut target = None;
        for node in self.router.nodes() {
            let mut kv = node.connect();
            if let Ok(config) = kv.get_config(GetConfigRequest {}).await
                && config.get_ref().identity.as_ref().is_some_and(|m| m.server_id == server_id)
            {
                target = Some(kv);
                break;
            }
        }
        let Some(mut kv) = target else {
            return Err(Error::Unavailable(format!(
                "no endpoint of this client is called {server_id:?}"
            )));
        };

        // The reader runs in its own task and feeds a bounded channel, so only
        // a few chunks are in memory however large the backup is.
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let header = BootstrapFromBackupChunk {
            part: Some(bootstrap_from_backup_chunk::Part::Header(BootstrapRequest {
                server_id: server_id.to_string(),
                heartbeat_millis: heartbeat_ms.unwrap_or(0),
            })),
        };
        tx.send(header)
            .await
            .expect("the receiver is still held here");
        let reading = tokio::spawn(async move {
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                let n = backup.read(&mut buf).await?;
                if n == 0 {
                    return Ok::<(), std::io::Error>(());
                }
                let chunk = BootstrapFromBackupChunk {
                    part: Some(bootstrap_from_backup_chunk::Part::Data(buf[..n].to_vec())),
                };
                // The node stopped reading: its answer says why.
                if tx.send(chunk).await.is_err() {
                    return Ok(());
                }
            }
        });

        let answered = kv
            .bootstrap_from_backup(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await;
        // A local read error ends the stream early, so the node reports a
        // truncated backup. The local error says more.
        match reading.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(Error::Rejected(format!("could not read the backup: {e}"))),
            Err(e) => return Err(Error::Rejected(format!("the backup reader failed: {e}"))),
        }
        match answered {
            Ok(response) => match config_reply(response) {
                Reply::Ok(()) => {
                    self.router.refresh_config(&kv).await;
                    Ok(())
                }
                Reply::Denied { reason, detail } => Err(Error::Denied { reason, detail }),
                _ => Err(Error::Unavailable(format!("{server_id} refused to bootstrap"))),
            },
            // Already in a cluster, or holding data: no retry will change it.
            Err(status) if status.code() == tonic::Code::FailedPrecondition => {
                Err(Error::Rejected(status.message().to_string()))
            }
            Err(status) => Err(status_error(status)),
        }
    }

    /// `call`, and then a re-read of the membership on success: the client
    /// just changed the cluster's members, so its own view is out of date.
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

    /// Log in, and use the resulting token for everything this client does
    /// from now on, including connections it already holds and all of its
    /// clones.
    ///
    /// Against a cluster started without a signing secret, this fails with
    /// [`Denial::AuthDisabled`]: there is nothing to log in to.
    ///
    /// The credentials stay in memory, so a token that expires mid-session is
    /// replaced without the caller noticing. A caller that does not want to
    /// hold a password for the client's lifetime can use [`login_with_token`]
    /// instead, at the cost of seeing the expiry.
    ///
    /// [`login_with_token`]: Client::login_with_token
    pub async fn login(&self, username: &str, password: &str) -> Result<(), Error> {
        self.refresh_token(username, password).await?;
        self.router
            .set_credentials(Some((username.to_string(), password.to_string())));
        Ok(())
    }

    /// Use a token obtained elsewhere.
    ///
    /// Nothing is checked here: the first request that carries it finds out
    /// whether the cluster accepts it. No credentials are stored, so an expiry
    /// is reported as [`Error::Unauthenticated`], not silently replaced.
    pub fn login_with_token(&self, token: String) {
        self.router.set_token(Some(token));
        self.router.set_credentials(None);
    }

    /// Forget the token and any credentials held with it.
    pub fn logout(&self) {
        self.router.set_token(None);
        self.router.set_credentials(None);
    }

    /// Whether this client holds a token. Says nothing about whether the
    /// cluster still accepts it.
    pub fn is_logged_in(&self) -> bool {
        self.router.has_token()
    }

    /// Who this client logged in as, if it logged in.
    ///
    /// `None` for a client that was given a token: a token names its holder,
    /// but this client does not read the tokens it carries.
    pub fn username(&self) -> Option<String> {
        self.router.username()
    }

    /// Create an account. Administrators only.
    ///
    /// `is_admin` decides whether the new account may manage accounts in
    /// turn. Returns the seq the account was written at.
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

    /// Remove an account. Administrators only.
    ///
    /// A token already issued to that account stays valid until it expires
    /// (nothing can withdraw one), but it gives no administrative rights once
    /// the account is gone.
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

    /// Create an invitation, so someone without an account can create one
    /// for themselves. Administrators only.
    ///
    /// Pass on the returned [`NewInvite::token`]. This is the only moment it
    /// exists: the cluster stores a hash and cannot produce it again.
    ///
    /// - `is_admin` sets what the new account will be: the administrator
    ///   decides here, not the recipient.
    /// - `username`, if given, pins the invitation to that name, so a
    ///   misdelivered invitation cannot become an account with another name.
    /// - `ttl` is refused if it is longer than the cluster allows, not
    ///   shortened. `Duration::ZERO` asks for the cluster's default.
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

    /// Use an invitation to create an account, and choose its password.
    ///
    /// The one call on this client that needs no login: a caller with an
    /// invitation has no account yet. It does not log the client in either:
    /// the account exists afterwards, and [`login`](Client::login) gets a
    /// token for it.
    ///
    /// The invitation is used up in the same transaction that writes the
    /// account, so it works exactly once. Two failures leave it unused and are
    /// worth telling apart:
    /// - [`Denial::AlreadyExists`]: the username is taken; try another with
    ///   the same token.
    /// - [`Denial::BadInvite`]: the token itself is bad; get a new one from
    ///   the issuer.
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
        // `call_inner(.., false)`: there are no credentials to fall back on. A
        // client redeeming an invitation has no account to log in to; it is
        // creating one.
        self.call_inner(
            |mut kv: Kv| {
                let request = request.clone();
                async move { kv.redeem_invite(request).await.map(redeem_invite_reply) }
            },
            false,
        )
        .await
    }

    /// Every open invitation. Administrators only.
    ///
    /// Never the secrets: those are gone. Lapsed invitations are not listed:
    /// nobody can use them, and the cluster sweeps them itself.
    pub async fn list_invites(&self) -> Result<Vec<Invite>, Error> {
        self.call(|mut kv: Kv| async move {
            kv.list_invites(ListInvitesRequest {})
                .await
                .map(list_invites_reply)
        })
        .await
    }

    /// Withdraw an open invitation, by its [`id`](Invite::id). Administrators
    /// only.
    ///
    /// The fix for one sent to the wrong person: the secret cannot be
    /// recalled, only made worthless. An invitation already redeemed gives
    /// [`Error::Rejected`], which is the answer you need: someone used it
    /// first.
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
    // Every call here that changes something writes one shared key, the
    // permission epoch. So two at once resolve like any two conflicting
    // transactions, and one may return [`Error::Aborted`]. This is on
    // purpose: the cluster decides each change against a view that cannot
    // move under it, so (for example) a write grant being revoked cannot be
    // used in its last moment to write an ACL that grants it back. A replay
    // of an aborted call is always safe: it wrote nothing.
    //
    // Reads contend for nothing.

    /// Create a group. Any authenticated account may, up to a per-account
    /// quota. This client's account becomes its only administrator, whatever
    /// `admins` says.
    ///
    /// A group is a name for a set of principals and is useful only when an
    /// ACL names it, so creating one gives its creator nothing new.
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

    /// Delete a group. Its administrators, or a cluster administrator.
    ///
    /// ACLs that name it are left alone: a grant to a group that no longer
    /// exists matches nobody.
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

    /// Add and remove members and administrators, in one transaction. The
    /// group's administrators, or a cluster administrator.
    ///
    /// Additions apply before removals, so a principal in both lists ends up
    /// removed. That makes "exactly these" one call that cannot land half
    /// done.
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

    /// One group, as stored: members and admins are not expanded, so a nested
    /// group appears as itself.
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

    /// Write the ACL that governs a key prefix, replacing what it held. An
    /// empty prefix governs every key.
    ///
    /// Requires `write` on that prefix, which, by the longest-prefix rule,
    /// means write on it or on a shorter prefix that governs it. A cluster
    /// administrator may always.
    ///
    /// It does not accept the default: creating an ACL where no ACL decides is
    /// an administrator's act. A key nothing governs is open to everyone, and
    /// that gives permission to use the key, not authority over who else may.
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

    /// Remove the ACL on a key prefix, so the next shorter prefix with an ACL
    /// governs instead.
    ///
    /// Same authority as writing it, with the same exception from the other
    /// side: a removal that leaves the prefix governed by nothing, and so open
    /// to everyone, is an administrator's act.
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

    /// Which ACL governs `key`, and what it grants this client's account.
    ///
    /// The question a holder of [`Denial::NotPermitted`] has, answered in one
    /// round trip, without reimplementing group expansion on top of
    /// [`list_acls`](Client::list_acls).
    pub async fn get_acl(&self, key: &[u8]) -> Result<AclFor, Error> {
        let request = GetAclRequest { key: key.to_vec() };
        self.call(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.get_acl(request).await.map(get_acl_reply) }
        })
        .await
    }

    /// Every ACL, shortest prefix first: the order they take effect, least
    /// specific to most.
    pub async fn list_acls(&self) -> Result<Vec<Acl>, Error> {
        self.call(|mut kv: Kv| async move {
            kv.list_acls(ListAclsRequest {}).await.map(list_acls_reply)
        })
        .await
    }

    /// Get a token and start to use it, without touching the stored
    /// credentials.
    ///
    /// `call_inner(.., false)`: `Login` is the one call that must not answer
    /// an expired token by logging in again, because that is what it is
    /// doing.
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
            // No node could be shown to be leading. For a cluster not yet
            // bootstrapped, that is normal, not a failure, and logging in is what an
            // operator must do there to get the token `Bootstrap` asks for. A node
            // with no configuration answers a login from its startup credentials
            // and needs no leader, so ask the nodes directly.
            Err(Error::Unavailable(why)) => match self.login_directly(&request).await {
                Ok(token) => token,
                // Report the first complaint, not the second: "no leader" is more
                // useful, and the direct attempt was a guess.
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
    /// The fallback for a cluster with no leader. Sent to each node, not one,
    /// because this client's seeds may include nodes that do not hold the
    /// startup credentials.
    async fn login_directly(&self, request: &LoginRequest) -> Result<String, Error> {
        let mut last: Option<String> = None;
        for node in self.router.nodes() {
            let mut kv = node.connect();
            match kv.login(request.clone()).await {
                Ok(resp) => match login_reply(resp) {
                    Reply::Ok(token) => return Ok(token),
                    // A refusal on the merits is the cluster's answer, not this node's, so
                    // the next node would give the same refusal.
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

fn read_reply(resp: tonic::Response<ReadResponse>) -> Reply<ReadOk> {
    match resp.into_inner().result {
        Some(read_response::Result::Ok(ok)) => Reply::Ok(ok),
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
/// The cluster has no wall clock, so a timestamp only means "what the
/// client that asked believed the time was".
fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// A default snapshot id: the timestamp, in a form the server's id
/// character set accepts, and that sorts by time.
fn timestamp_id(unix_ms: u64) -> String {
    format!("t{unix_ms}")
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

/// An `Invite` from the wire.
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
        // incomplete reply. It is not filled with a default, because an
        // invitation shown with no expiry would read as one that never lapses.
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

/// An `AuthError` from the wire. An unknown kind is still a refusal, not
/// retried: a cluster that names a reason means it.
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

/// Walk a cluster's keys in order, one batch at a time.
///
/// Built by [`Client::key_iterator`] (a stale walk) or
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
pub struct KeyIterator {
    client: Client,
    /// What the walk asks for: a transaction, a snapshot, or nothing. Fixed
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
