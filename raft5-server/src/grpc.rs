//! gRPC service implementations wiring the generated proto types (`pb`)
//! into `storage::RaftLog`.
//!
//! `KvService` is the client-facing half. Everything that changes state
//! -- writes, membership changes, and a transactional read -- requires
//! this node to be the leader, and a node that isn't answers `NotLeader`
//! with whatever leader hint it has rather than forwarding (Raft §6.2),
//! so the client retries against the leader directly and exactly one node
//! does the work. A *stale* read is the one exception: it asks for no
//! guarantee, so it is served locally by whoever receives it. `Bootstrap`
//! is the other: by definition it targets a node with no leader at all.
//!
//! Every write goes through a transaction. `write` buffers a key into
//! one and appends nothing; `commit` turns everything a transaction
//! buffered into a single log entry. There is no second write path with
//! its own locking and its own failure modes to keep in agreement with
//! this one -- a single-key write is a transaction of one key. What
//! protects a transaction's reads is `crate::txn`'s lock table rather
//! than a precondition on a sequence number, which is what lets the
//! whole of a read-modify-write be one atomic thing instead of two round
//! trips with a race between them.
//!
//! Both `commit` and the transactional reads (`transactional_read` for
//! one key, `transactional_scan` for a range of them) establish the same
//! two things before answering: this leader's `latest` must be complete
//! (`LeaderHandle::await_caught_up`), since `latest` reflects only
//! *applied* entries and a fresh leader may still be catching up on a
//! log tail it inherited; and it must still hold a quorum of leases
//! (`LeaderHandle::has_read_quorum`), or it may have been deposed
//! without knowing it yet.
//!
//! `RaftPeerService` is the peer-facing half, served on a separate port
//! since the two have different network exposure. It forwards every call
//! straight into a live `raft::RaftPeerHandle` -- whichever role the
//! state machine is currently in decides how to answer. `install_snapshot`
//! reassembles the incoming stream (transport-level work) but, like every
//! other call here, never touches `storage::RaftLog` directly -- the term
//! check and the actual install both happen on the far side of
//! `RaftPeerHandle`, same as everything else.

use prost::Message;
use tonic::transport::ClientTlsConfig;
use tonic::{Request, Response, Status, Streaming};
use tracing::Instrument;

use crate::auth::{AuthConfig, AuthContext, InviteRecord, InviteSecret, UserRecord};
use crate::keyspace::is_reserved;
use crate::pb::raft5::client::kv_server::Kv;
use crate::pb::raft5::client::{
    AddServerRequest, AuthError as PbAuthError, BootstrapRequest, CommitTransactionRequest,
    CommitTransactionResponse, ConfigChangeResponse, CreateInviteOk, CreateInviteRequest,
    CreateInviteResponse, CreateSnapshotRequest, CreateSnapshotResponse, CreateTransactionRequest,
    CreateTransactionResponse, CreateUserRequest, CreateUserResponse, DeleteUserRequest,
    DeleteUserResponse, DropTransactionRequest, DropTransactionResponse, GetConfigRequest,
    GetConfigResponse, GetKeysOk, GetKeysRequest, GetKeysResponse, GetVersionOk, GetVersionRequest,
    GetVersionResponse, Invite as PbInvite, InviteList, ListInvitesRequest, ListInvitesResponse,
    ListSnapshotsRequest, ListSnapshotsResponse, LoginOk, LoginRequest, LoginResponse, NotLeader,
    RaftError as PbRaftError, ReadOk, ReadRequest, ReadResponse, RedeemInviteOk,
    RedeemInviteRequest, RedeemInviteResponse, RemoveServerRequest, RemoveSnapshotRequest,
    SetHeartbeatRequest,
    RemoveSnapshotResponse, RevokeInviteRequest, RevokeInviteResponse, Snapshot as PbSnapshot,
    SnapshotList, TransactionAborted, WriteRequest, WriteResponse, auth_error,
    commit_transaction_response, config_change_response, create_invite_response,
    create_snapshot_response, create_transaction_response, create_user_response,
    delete_user_response, drop_transaction_response, get_keys_request, get_keys_response,
    get_version_request, get_version_response, list_invites_response, list_snapshots_response,
    login_response, raft_error, read_request, read_response, redeem_invite_response,
    remove_snapshot_response, revoke_invite_response, transaction_aborted, write_response,
};
use crate::pb::raft5::client::{
    CreateGroupRequest, CreateGroupResponse, DeleteAclRequest, DeleteAclResponse,
    DeleteGroupRequest, DeleteGroupResponse, GetAclRequest, GetAclResponse, GetGroupRequest,
    GetGroupResponse, ListAclsRequest, ListAclsResponse, ListGroupsRequest, ListGroupsResponse,
    SetAclRequest, SetAclResponse, UpdateGroupRequest, UpdateGroupResponse, create_group_response,
    delete_acl_response, delete_group_response, get_acl_response, get_group_response,
    list_acls_response, list_groups_response, set_acl_response, update_group_response,
};
use crate::pb::raft5::cluster::{ClusterConfig, ClusterMember};
use crate::pb::raft5::raft::raft_peer_server::RaftPeer;
use crate::pb::raft5::raft::{
    AppendEntriesChunk, AppendEntriesRequest, AppendEntriesResponse, EntryBatch, GetLeaseRequest,
    GetLeaseResponse, IdentifyRequest, IdentifyResponse, InstallSnapshotResponse,
    RequestVoteRequest, RequestVoteResponse, SnapshotChunk, append_entries_chunk, snapshot_chunk,
};
use crate::pb::raft5::raft::raft_peer_client::RaftPeerClient;
use crate::perms::Right;
use crate::raft::{
    self, AppendError, ClientHandle, ConfigChange, LeaderHandle, MAX_APPEND_ENTRIES_BYTES,
    RaftError, RaftPeerHandle, RoleHandle,
};
use crate::storage::{Checkpoint, MAX_LIVE_CHECKPOINTS, Mutation, checkpoint_key};
use crate::storage::{RaftLog, StorageError};
use crate::store::ChunkReader;
use crate::store::Store;
use crate::txn::{AbortReason, BufferedKeys, Locks, Refused, TxnId};

/// What storage found, as a `ReadOk`.
///
/// A never-written key and a deleted key both read as "no value"
/// (storage's `get_latest` already collapses the two -- see its doc); seq
/// 0 is never assigned, since sequencing starts at 1, so it doubles as
/// the "never written" sentinel here.
fn read_ok(found: Option<(u64, Vec<u8>)>) -> ReadOk {
    match found {
        Some((seq, value)) => ReadOk {
            seq,
            value: Some(value),
        },
        None => ReadOk {
            seq: 0,
            value: None,
        },
    }
}

/// A scan's batch as the wire carries it.
fn get_keys_ok(batch: ScanBatch) -> GetKeysOk {
    GetKeysOk {
        keys: batch.keys,
        has_more: batch.has_more,
        next_start_key: batch.next_start_key.unwrap_or_default(),
    }
}

/// How many keys `GetKeys` returns when the caller doesn't say.
const DEFAULT_MAX_KEYS: u32 = 10;

/// The most keys one `GetKeys` will return.
///
/// Inside a transaction this is also a cap on how many read locks one
/// call can take, which is the reason it isn't larger: every key a scan
/// hands back is held until that transaction ends, so an unbounded scan
/// would let one caller make an arbitrary stretch of the keyspace
/// unwritable. Outside a transaction it is just a bound on the work and
/// the response size.
const MAX_KEYS_PER_SCAN: u32 = 1024;

/// How long an account write may hold its key.
///
/// Short, because nothing human is in the loop: the whole
/// read-modify-write happens server side within one raft round trip, so
/// this only bounds how long a wedged admin call leaves an account
/// unwritable.
const ADMIN_TXN_TTL: std::time::Duration = std::time::Duration::from_secs(5);

/// The lifetime an invitation gets when its issuer does not name one.
///
/// A day: long enough to survive being sent to someone in another
/// timezone and read the next morning, short enough that an invite
/// forgotten in an inbox is not still spendable a month later.
const DEFAULT_INVITE_TTL: std::time::Duration = std::time::Duration::from_secs(24 * 60 * 60);

/// The longest lifetime this cluster will grant one.
///
/// An invitation is a credential that creates a credential, and the only
/// thing bounding how long a leaked one stays dangerous is this. A week
/// covers "I am on holiday until Monday"; anything past that is an
/// account, not an invitation, and should be created as one.
const MAX_INVITE_TTL: std::time::Duration = std::time::Duration::from_secs(7 * 24 * 60 * 60);

/// How many invitations may be outstanding at once.
///
/// Unlike `MAX_LIVE_CHECKPOINTS` this does not protect the store from
/// anything -- an invite record is a hundred bytes, and an administrator
/// who wanted to fill the disk has `CreateUser` for that. What it bounds
/// is the *listing*: `ListInvites` reads every outstanding record in one
/// unpaged call, and this is the number that keeps that call small
/// enough to be answered that way. Lapsed invitations are swept by the
/// next `CreateInvite` rather than counted against it, so a cluster that
/// issues a steady trickle never meets this at all.
const MAX_LIVE_INVITES: usize = 64;

/// How long `Bootstrap` will wait for this node to start leading before
/// giving up on writing the administrator it was started with.
///
/// Generous: a single-node cluster elects itself unopposed, so this is
/// the cost of one election timeout and one no-op commit, not of
/// agreement with anybody.
const BOOTSTRAP_ADMIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
const BOOTSTRAP_ADMIN_RETRY: std::time::Duration = std::time::Duration::from_millis(20);

/// A transaction's refusal as the client protocol carries it.
///
/// Every variant but `TooMuch` is an ordinary answer rather than an
/// error, the same way a write conflict is: it says something specific
/// about the transaction that the caller can act on. `TooMuch` is a
/// genuine refusal to serve -- the request was never going to work at any
/// time, against any state -- so it stays a `Status`.
fn txn_err(refused: Refused) -> Result<PbRaftError, Status> {
    let (reason, key) = match refused {
        Refused::Conflict { key } => (transaction_aborted::Reason::LockConflict, Some(key)),
        Refused::Aborted(AbortReason::Expired) => (transaction_aborted::Reason::Expired, None),
        Refused::Aborted(AbortReason::Wounded) => (transaction_aborted::Reason::Wounded, None),
        Refused::Unknown => (transaction_aborted::Reason::Unknown, None),
        Refused::LeaderChanged => (transaction_aborted::Reason::LeaderChanged, None),
        Refused::TooMuch(why) => return Err(Status::resource_exhausted(why)),
    };
    Ok(aborted(reason, key, None))
}

fn aborted(
    reason: transaction_aborted::Reason,
    key: Option<Vec<u8>>,
    seq: Option<u64>,
) -> PbRaftError {
    PbRaftError {
        kind: Some(raft_error::Kind::Aborted(TransactionAborted {
            reason: reason as i32,
            key,
            seq,
        })),
    }
}

/// Why a transactional request didn't happen: the transaction's own
/// reasons, plus everything the state machine can refuse.
enum TxnError {
    Refused(Refused),
    Raft(RaftError),
    /// The commit reached the log and this node then stopped leading.
    /// Carried separately from every other failure because it is the one
    /// a client must not retry -- see `raft::AppendError`.
    InDoubt(u64),
    Internal(Status),
}

impl From<Refused> for TxnError {
    fn from(r: Refused) -> Self {
        Self::Refused(r)
    }
}

impl From<RaftError> for TxnError {
    fn from(e: RaftError) -> Self {
        Self::Raft(e)
    }
}

impl From<AppendError> for TxnError {
    fn from(e: AppendError) -> Self {
        match e {
            AppendError::NotAppended(e) => Self::Raft(e),
            AppendError::InDoubt { seq, .. } => Self::InDoubt(seq),
        }
    }
}

fn txn_error(e: TxnError) -> Result<PbRaftError, Status> {
    match e {
        TxnError::Refused(r) => txn_err(r),
        TxnError::Raft(e) => client_err(e),
        TxnError::InDoubt(seq) => Ok(aborted(
            transaction_aborted::Reason::InDoubt,
            None,
            Some(seq),
        )),
        TxnError::Internal(status) => Err(status),
    }
}

/// Why an authentication RPC did not do what it was asked.
enum AuthFailure {
    /// Something about the request or the account. Reported to the
    /// caller in band, as an `AuthError`.
    Refused(auth_error::Kind, String),
    /// The cluster could not serve the request at all.
    Cluster(TxnError),
}

impl From<TxnError> for AuthFailure {
    fn from(e: TxnError) -> Self {
        Self::Cluster(e)
    }
}

impl From<RaftError> for AuthFailure {
    fn from(e: RaftError) -> Self {
        Self::Cluster(TxnError::Raft(e))
    }
}

impl From<Refused> for AuthFailure {
    fn from(r: Refused) -> Self {
        Self::Cluster(TxnError::Refused(r))
    }
}

impl From<Status> for AuthFailure {
    fn from(s: Status) -> Self {
        Self::Cluster(TxnError::Internal(s))
    }
}

impl AuthFailure {
    fn refused(kind: auth_error::Kind, detail: impl Into<String>) -> Self {
        Self::Refused(kind, detail.into())
    }

    fn disabled() -> Self {
        Self::refused(auth_error::Kind::AuthDisabled, AUTH_DISABLED)
    }

    /// One answer for "no such account" and "wrong password" alike.
    /// Distinguishing them would confirm which usernames are real, and a
    /// username is half of a credential.
    fn bad_credentials() -> Self {
        Self::refused(
            auth_error::Kind::BadCredentials,
            "the username or password was wrong",
        )
    }

    /// An invitation that cannot be spent. `why` is for a human to read
    /// -- an invite holder is someone an administrator deliberately gave
    /// a secret to, so telling them "that lapsed on Tuesday" rather than
    /// a flat no costs nothing and saves a support conversation.
    fn bad_invite(why: &'static str) -> Self {
        Self::refused(auth_error::Kind::BadInvite, why)
    }
}

/// Which of the two error shapes a response should carry.
enum AuthReply {
    Raft(PbRaftError),
    Auth(PbAuthError),
}

/// An `AuthFailure` as the answer to a membership change.
fn config_change_refusal(e: AuthFailure) -> Result<Response<ConfigChangeResponse>, Status> {
    Ok(Response::new(ConfigChangeResponse {
        result: Some(match auth_failure(e)? {
            AuthReply::Raft(e) => config_change_response::Result::Error(e),
            AuthReply::Auth(e) => config_change_response::Result::AuthError(e),
        }),
    }))
}

fn auth_failure(e: AuthFailure) -> Result<AuthReply, Status> {
    Ok(match e {
        AuthFailure::Refused(kind, detail) => AuthReply::Auth(auth_err(kind, detail)),
        AuthFailure::Cluster(e) => AuthReply::Raft(txn_error(e)?),
    })
}

/// A transaction id off the wire. Fixed-width, so anything else is a
/// malformed request rather than a transaction that happens not to
/// exist.
fn txn_id(bytes: &[u8]) -> Result<TxnId, Status> {
    bytes
        .try_into()
        .map_err(|_| Status::invalid_argument("transaction_id must be 16 bytes"))
}

/// Refuses a client's attempt to address a key the cluster reserves for
/// itself (`crate::keyspace`).
///
/// `invalid_argument` rather than `permission_denied`: no credential
/// reaches these keys through this API, so the honest answer is that the
/// key is not addressable here at all rather than that this caller may
/// not have it. It is also already on the client's short list of
/// permanent failures, so a caller learns this at once instead of after
/// spending its whole retry deadline on an answer that will not change.
fn reject_reserved(key: &[u8]) -> Result<(), Status> {
    if is_reserved(key) {
        return Err(Status::invalid_argument(
            "that key is reserved for the cluster's own use",
        ));
    }
    Ok(())
}

/// An invitation as the wire carries it: everything but the secret,
/// which no longer exists anywhere by the time anyone can ask for this.
fn pb_invite(invite_id: String, record: InviteRecord) -> PbInvite {
    PbInvite {
        invite_id,
        issued_by: record.issued_by,
        is_admin: record.is_admin,
        username: record.username,
        expires_unix_ms: record.expires_unix_ms,
    }
}

/// How long an invitation asked for with `ttl_ms` should last.
///
/// Over the limit is refused rather than quietly shortened, exactly as
/// an over-long transaction lifetime is and for the same reason: an
/// issuer told "yes" would hand out an invitation believing it good for
/// a month and hear about it when the recipient could not use it.
fn invite_ttl(ttl_ms: u64) -> Result<std::time::Duration, Status> {
    if ttl_ms == 0 {
        return Ok(DEFAULT_INVITE_TTL);
    }
    let ttl = std::time::Duration::from_millis(ttl_ms);
    if ttl > MAX_INVITE_TTL {
        return Err(Status::invalid_argument(format!(
            "an invitation may last at most {} hours",
            MAX_INVITE_TTL.as_secs() / 3600
        )));
    }
    Ok(ttl)
}

/// Whether `record` may be spent, right now, on `username`.
///
/// Both halves answer `BAD_INVITE` rather than saying anything more
/// structured: from the holder's side an invitation that has lapsed and
/// one that was meant for someone else call for the same thing, which is
/// to go back to whoever issued it.
fn check_invite(record: &InviteRecord, username: &str) -> Result<(), AuthFailure> {
    if record.has_expired(crate::auth::now_unix_ms()) {
        return Err(AuthFailure::bad_invite(
            "that invitation has lapsed; ask whoever issued it for another",
        ));
    }
    match record.username.as_deref() {
        Some(pinned) if pinned != username => Err(AuthFailure::bad_invite(
            "that invitation was issued for a different username",
        )),
        _ => Ok(()),
    }
}

/// Who a request is from, for the records that want to say so.
///
/// Only sound after `require_admin` has run on a node that keeps
/// accounts: it is that call which establishes there is an identity here
/// at all. The error is the same one it would have raised, so a caller
/// that forgets the ordering fails closed.
fn caller(extensions: &tonic::Extensions) -> Result<String, AuthFailure> {
    extensions
        .get::<AuthContext>()
        .map(|context| context.username.clone())
        .ok_or_else(|| Status::unauthenticated("this request carried no verified identity").into())
}

/// A checkpoint as the wire carries it.
fn pb_snapshot(cp: Checkpoint) -> PbSnapshot {
    PbSnapshot {
        snapshot_id: cp.id,
        seq: cp.seq,
        created_unix_ms: cp.created_unix_ms,
    }
}

/// How long a snapshot id may be.
const MAX_SNAPSHOT_ID_LEN: usize = 128;

/// Checks that `id` is one `checkpoint_key` can safely build a key from.
///
/// The character set is the point, not the length, and the reasoning is
/// `keyspace::validate_username`'s exactly: the id is appended to a
/// fixed prefix with no separator or escaping, so any byte that could
/// make two ids collide -- or let one climb back out of the prefix --
/// has to be excluded here rather than handled there. The set is wider
/// than a username's only by `:` and `+`, so that an RFC-3339 timestamp
/// is a legal id, which is what the client defaults to.
fn validate_snapshot_id(id: &[u8]) -> Result<(), Status> {
    if id.is_empty() {
        return Err(Status::invalid_argument("a snapshot id cannot be empty"));
    }
    if id.len() > MAX_SNAPSHOT_ID_LEN {
        return Err(Status::invalid_argument(format!(
            "a snapshot id may be at most {MAX_SNAPSHOT_ID_LEN} bytes"
        )));
    }
    if !id
        .iter()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-' | b':' | b'+'))
    {
        return Err(Status::invalid_argument(
            "a snapshot id may contain only letters, digits, '.', '_', '-', ':' and '+'",
        ));
    }
    Ok(())
}

/// An `AuthError` as the client protocol carries it.
///
/// In-band beside `RaftError` rather than a `Status`, for the same
/// reason a lock conflict is: every one of these is an answer, and a
/// client that retried it would spend its whole deadline arriving back
/// at the same one. A bad *token* is the opposite case and stays a
/// `Status` -- see `crate::auth`.
fn auth_err(kind: auth_error::Kind, detail: impl Into<String>) -> PbAuthError {
    PbAuthError {
        kind: kind as i32,
        detail: detail.into(),
    }
}

/// What every auth RPC answers on a node started without a secret.
const AUTH_DISABLED: &str =
    "this node was started without a signing secret, so it keeps no accounts";

fn storage_err(e: StorageError) -> Status {
    Status::internal(e.to_string())
}

fn raft_err(e: RaftError) -> Status {
    match e {
        // Only reachable on the peer-facing service, where a structured
        // client error would have nowhere to go.
        RaftError::NotLeader { .. } => Status::failed_precondition("not leader"),
        RaftError::Storage(e) => storage_err(e),
        RaftError::Disconnected => Status::unavailable("raft event loop is not running"),
        RaftError::AlreadyInitialized => Status::failed_precondition(
            "this node is already part of a cluster; to add it to a different one, \
             use AddServer on that cluster's leader",
        ),
        RaftError::ConfigChangeInProgress => {
            Status::unavailable("another membership change is still in flight")
        }
        RaftError::NotCaughtUp => Status::unavailable(
            "this leader has not yet committed an entry from its own term; retry shortly",
        ),
        RaftError::InvalidConfigChange(why) => Status::failed_precondition(why),
    }
}

/// `RaftError` as the structured error the client protocol carries, so a
/// client can find the real leader from the response itself rather than
/// needing a separate discovery RPC. Only `NotLeader` has somewhere
/// useful to point; everything else is reported as a plain string, and
/// genuine internal failures (storage, a dead event loop) stay `Status`
/// errors rather than being dressed up as an ordinary answer.
fn client_err(e: RaftError) -> Result<PbRaftError, Status> {
    Ok(match e {
        RaftError::NotLeader { leader_hint } => PbRaftError {
            kind: Some(raft_error::Kind::NotLeader(NotLeader { leader_hint })),
        },
        RaftError::ConfigChangeInProgress
        | RaftError::NotCaughtUp
        | RaftError::AlreadyInitialized
        | RaftError::InvalidConfigChange(_) => PbRaftError {
            kind: Some(raft_error::Kind::Unavailable(
                raft_err(e).message().to_string(),
            )),
        },
        e => return Err(raft_err(e)),
    })
}

/// A refusal wrapped the same way `propose` wraps one -- for callers
/// (`add_server`'s leader check) that need to answer this way before
/// ever reaching `propose` itself.
fn not_leader_response(e: RaftError) -> Result<Response<ConfigChangeResponse>, Status> {
    Ok(Response::new(ConfigChangeResponse {
        result: Some(config_change_response::Result::Error(client_err(e)?)),
    }))
}

/// The leader handle for this node, or the error to hand back to the
/// client instead. Every write and every membership change goes through
/// here: a follower answers `NotLeader` with whatever hint it has rather
/// than forwarding, so a client retries against the leader directly
/// (Raft §6.2) and there is exactly one node doing the work.
fn require_leader(role: &RoleHandle) -> Result<LeaderHandle, RaftError> {
    match role {
        RoleHandle::Leader(h) => Ok(h.clone()),
        other => Err(RaftError::NotLeader {
            leader_hint: raft::leader_hint_of(other),
        }),
    }
}

/// One batch of a key scan, and where the next one starts.
///
/// A struct rather than a tuple because `has_more` and `next_start_key`
/// are two different facts now: a batch can be short, or empty, and
/// still have keys after it -- see `RaftLog::scan_as_of`'s budget.
struct ScanBatch {
    keys: Vec<Vec<u8>>,
    has_more: bool,
    /// Where to resume, set whenever `has_more` is.
    next_start_key: Option<Vec<u8>>,
}

pub struct KvService {
    store: Store,
    /// How this node's own identity reaches `Bootstrap` -- the
    /// configuration it forms names itself, and only this node knows what
    /// it is called and where its peers should dial it.
    identity: ClusterMember,
    client_handle: ClientHandle,
    /// Who holds what, and every open transaction. See `crate::txn`.
    locks: Locks,
    /// How tokens are signed and checked, or `None` on a node started
    /// without a secret -- which is what leaves this API open. Every
    /// authorization check reads as "allowed" when this is `None`, so
    /// that the open case lives in one place rather than in each
    /// handler. See `crate::auth`.
    auth: Option<AuthConfig>,
    /// Every group and ACL, as of the last permission change this node
    /// has applied. Shared across the clones tonic makes of this
    /// service, and rebuilt only when the permission epoch moves. See
    /// `grpc::permissions`.
    perms: permissions::PermsCache,
    /// How `AddServer` dials the server it is asked to add: over the
    /// same mTLS as every other peer, or `None` for plaintext.
    peer_tls: Option<ClientTlsConfig>,
}

impl KvService {
    pub fn new(
        store: Store,
        identity: ClusterMember,
        client_handle: ClientHandle,
        auth: Option<AuthConfig>,
    ) -> Self {
        Self {
            store,
            identity,
            client_handle,
            locks: Locks::new(),
            auth,
            perms: permissions::new_cache(),
            peer_tls: None,
        }
    }

    /// Dials the servers `AddServer` is asked to add over mTLS, the way
    /// the cluster's peers dial each other. Must match how this node's
    /// own peer connections are configured: a server this node can't
    /// reach the way it reaches peers would never get past being added.
    pub fn with_peer_tls(mut self, tls: ClientTlsConfig) -> Self {
        self.peer_tls = Some(tls);
        self
    }

    /// The lock table, for the shutdown-scoped sweeper `serve` spawns.
    pub fn locks(&self) -> Locks {
        self.locks.clone()
    }

    /// This node's leader handle, having first told the lock table which
    /// term it is answering in.
    ///
    /// Every request that needs a leader goes through here rather than
    /// `require_leader` directly, so the table notices a change of term
    /// at the first request after it rather than at the next sweep. A
    /// transaction cannot outlive the leader that created it, and the
    /// sooner that is acted on, the sooner its keys are writable again.
    fn leader(&self) -> Result<LeaderHandle, RaftError> {
        let role = self.client_handle.role();
        let leader = require_leader(&role);
        self.locks
            .observe_term(leader.as_ref().ok().map(LeaderHandle::term));
        leader
    }

    /// A transaction's leader: this node, and only if it is still the
    /// one the transaction was created under.
    ///
    /// The term check is belt to `observe_term`'s braces. `leader()`
    /// already discards every transaction when the term changes, so a
    /// transaction from a previous term is normally gone by the time
    /// anyone asks for it; this catches the ordering where the same
    /// request observes the change.
    fn transaction_leader(&self, id: TxnId) -> Result<LeaderHandle, TxnError> {
        let leader = self.leader()?;
        if self.locks.term_of(id)? != leader.term() {
            return Err(Refused::LeaderChanged.into());
        }
        Ok(leader)
    }

    /// A transactional read: take the shared lock, then answer.
    ///
    /// `require_leader` and `await_caught_up` come *before* the lock: a
    /// leader still catching up should not be holding keys nobody can
    /// write while it does it.
    async fn transactional_read(&self, id: TxnId, key: &[u8]) -> Result<ReadOk, TxnError> {
        self.transaction_read_quorum(id).await?;

        let buffered = self.locks.read(id, key)?;

        let key = key.to_vec();
        let committed = self
            .store
            .run(move |log| log.get_latest(&key))
            .await
            .map_err(|e| TxnError::Raft(RaftError::Storage(e)))?;

        // `seq` always describes committed state, even when the value
        // doesn't: a buffered write has no sequence number yet, because
        // nothing has been appended. Reading back one's own buffered
        // write therefore gives the new value at the old key's seq,
        // which is the honest answer -- the seq this write will land at
        // does not exist until the commit assigns one.
        let seq = committed.as_ref().map_or(0, |(seq, _)| *seq);
        let value = match buffered {
            Some(buffered) => buffered,
            None => committed.map(|(_, value)| value),
        };
        Ok(ReadOk { seq, value })
    }

    /// What a transaction must establish before it may answer a read:
    /// this node leads the transaction's own term, its `latest` is
    /// complete, and it still holds a quorum of leases.
    ///
    /// `transaction_leader` and `await_caught_up` come *before* the
    /// caller takes any lock: a leader still catching up should not be
    /// holding keys nobody can write while it does it.
    async fn transaction_read_quorum(&self, id: TxnId) -> Result<(), TxnError> {
        let leader = self.transaction_leader(id)?;
        leader.await_caught_up().await?;
        if !leader.has_read_quorum() {
            return Err(RaftError::NotLeader { leader_hint: None }.into());
        }
        Ok(())
    }

    /// A transactional `GetVersion`: take the shared lock, then answer
    /// from the index.
    ///
    /// `read_many` of a single key rather than `read`, which is otherwise
    /// the obvious call. Both take the lock and record the read; `read`
    /// additionally clones whatever this transaction has buffered for the
    /// key, which is megabytes to answer an eight-byte question -- and a
    /// question about committed state at that, which the buffered value
    /// could not answer even if it were free. `read_many` also skips a
    /// key the transaction already holds a write lock on, which is what
    /// makes asking about one's own buffered write work rather than
    /// deadlock on the upgrade.
    async fn transactional_version(&self, id: TxnId, key: &[u8]) -> Result<GetVersionOk, TxnError> {
        self.transaction_read_quorum(id).await?;

        let keys = [key.to_vec()];
        self.locks.read_many(id, &keys)?;

        let [key] = keys;
        let seq = self
            .store
            .run(move |log| log.get_version(&key))
            .await
            .map_err(|e| TxnError::Raft(RaftError::Storage(e)))?;
        Ok(GetVersionOk {
            seq: seq.unwrap_or(0),
        })
    }

    /// One pass of a key scan: up to `limit` keys at or after `start`,
    /// with `buffered` -- this transaction's own uncommitted changes, or
    /// empty outside one -- folded in.
    ///
    /// Returns the keys and whether the scan stopped early because it
    /// filled up, which is what tells a caller there is another batch.
    async fn scan(
        &self,
        start: Vec<u8>,
        as_of: u64,
        limit: usize,
        buffered: &BufferedKeys,
        filter: &Option<permissions::ScanFilter>,
    ) -> Result<ScanBatch, StorageError> {
        // A key this transaction has deleted doesn't exist as far as it
        // is concerned, so the scan skips it rather than returning it
        // and filtering after -- which would shorten the batch for a
        // reason the caller can't see.
        let deleted: std::collections::HashSet<Vec<u8>> = buffered
            .iter()
            .filter(|(_, is_delete)| *is_delete)
            .map(|(key, _)| key.clone())
            .collect();

        // One more than asked for, so "there is another batch" is
        // something this saw rather than something the caller infers
        // from a count -- see `GetKeysOk.has_more`.
        let want = limit.saturating_add(1);
        let (committed, budget_cursor) = {
            let start = start.clone();
            let filter = filter.clone();
            self.store
                .run(move |log| {
                    // Folded into the skip closure rather than filtered
                    // out of the result: a skipped key doesn't count
                    // against `want`, so hiding the reserved range costs
                    // the caller no batch length. Both callers -- the
                    // transactional scan and the plain one -- come
                    // through here, so this is the whole of `GetKeys`.
                    //
                    // Keys an ACL hides join it on exactly those terms.
                    // The filter is resolved before the walk, because
                    // this closure runs on the blocking pool and cannot
                    // read the store -- which is also why a walk that
                    // hides nearly everything needs the budget below to
                    // stop it walking the whole keyspace.
                    log.scan_as_of(&start, as_of, want, permissions::SCAN_ROW_BUDGET, |key| {
                        deleted.contains(key)
                            || is_reserved(key)
                            || filter.as_ref().is_some_and(|f| f.hides(key))
                    })
                })
                .await?
        };

        // Buffered writes make keys appear that the store has never
        // heard of. Merging them in can only add, so taking the first
        // `limit` of the union is the same answer a merge of the two
        // full streams would give.
        let mut keys: Vec<Vec<u8>> = committed;
        keys.extend(
            buffered
                .iter()
                .filter(|(key, is_delete)| {
                    // `is_reserved` is belt to `write`'s braces: a client
                    // transaction is refused a reserved key at the RPC
                    // boundary, so it should have none buffered. The
                    // server's own transactions do, and none of them
                    // scans -- but the cost of being sure is one
                    // comparison.
                    !*is_delete && key.as_slice() >= start.as_slice() && !is_reserved(key)
                })
                .map(|(key, _)| key.clone()),
        );
        keys.sort_unstable();
        keys.dedup();

        // Two reasons to have more: the batch filled, or the walk ran
        // out of budget before it could. The first derives its cursor
        // from the last key kept; the second cannot -- there may be no
        // key at all -- so it uses the one the walk stopped at.
        let filled = keys.len() > limit;
        keys.truncate(limit);
        let next_start_key = match &budget_cursor {
            Some(cursor) => Some(cursor.clone()),
            None if filled => keys.last().map(|last| {
                let mut next = last.clone();
                next.push(0);
                next
            }),
            None => None,
        };
        Ok(ScanBatch {
            keys,
            has_more: filled || budget_cursor.is_some(),
            next_start_key,
        })
    }

    /// A transactional key scan: find the keys, lock them, then look
    /// again.
    ///
    /// The second pass is what makes this a read rather than a guess.
    /// A single-key read locks first and reads after, which a scan
    /// cannot do -- it doesn't know which keys to lock until it has
    /// looked. So it looks, locks what it found, and looks again: by
    /// then nobody else can change any of those keys, and the second
    /// answer is one this transaction can hold to. A key that vanished
    /// in between is simply gone (we hold its lock and it isn't there);
    /// one that appeared is a phantom this returns nothing about, since
    /// these are per-key locks and the range itself is not held.
    async fn transactional_scan(
        &self,
        id: TxnId,
        start: Vec<u8>,
        limit: usize,
        filter: &Option<permissions::ScanFilter>,
    ) -> Result<ScanBatch, TxnError> {
        let leader = self.transaction_leader(id)?;
        leader.await_caught_up().await?;
        if !leader.has_read_quorum() {
            return Err(RaftError::NotLeader { leader_hint: None }.into());
        }

        let buffered = self.locks.buffered_from(id, &start)?;

        let candidates = self
            .scan(start.clone(), u64::MAX, limit, &buffered, filter)
            .await
            .map_err(|e| TxnError::Raft(RaftError::Storage(e)))?
            .keys;
        self.locks.read_many(id, &candidates)?;

        let batch = self
            .scan(start, u64::MAX, limit, &buffered, filter)
            .await
            .map_err(|e| TxnError::Raft(RaftError::Storage(e)))?;

        // Only what the locks above actually cover. Anything else the
        // second pass turned up arrived after this transaction looked,
        // and handing it back unlocked would be exactly the unprotected
        // data under a transaction's name that the lock table exists to
        // prevent.
        let locked: std::collections::HashSet<&[u8]> =
            candidates.iter().map(Vec::as_slice).collect();
        let keys = batch
            .keys
            .into_iter()
            .filter(|key| locked.contains(key.as_slice()))
            .collect();
        // The cursor comes from the second pass, which is the one whose
        // answer this is. Filtering to what the locks cover can shorten
        // the batch, which is one more reason the cursor is carried
        // rather than derived from the last key returned.
        Ok(ScanBatch { keys, ..batch })
    }

    /// Commit everything a transaction buffered, as one entry.
    ///
    /// The append runs in a task of its own owning the transaction's
    /// locks, so they are released when the *entry* resolves rather than
    /// when this call does. A client that disconnects mid-commit would
    /// otherwise leave the transaction stuck part-way, and a part-way
    /// transaction is deliberately exempt from expiry: its keys would
    /// stay unwritable for the rest of the term.
    async fn commit(&self, id: TxnId) -> Result<u64, TxnError> {
        let leader = self.transaction_leader(id)?;
        let (writes, guard) = self.locks.begin_commit(id, leader.term())?;

        if writes.is_empty() {
            // Nothing to append, so nothing re-validates the point in
            // time this transaction read at -- the raft round trip is
            // what normally does that. Term equality alone isn't enough,
            // since a partitioned leader still believes it holds its
            // term; a lease quorum is. (Re-acquiring one in term T is
            // impossible once a majority has moved to T', which is what
            // makes this sound rather than merely reassuring.)
            leader.await_caught_up().await?;
            if !leader.has_read_quorum() {
                return Err(RaftError::NotLeader { leader_hint: None }.into());
            }
            drop(guard);
            return Ok(0);
        }

        let mutations = writes
            .into_iter()
            .map(|(key, value)| Mutation { key, value })
            .collect();

        // `.instrument(..)`: a span doesn't cross `tokio::spawn` on its
        // own any more than it crosses `spawn_blocking` -- without this,
        // `append_writes`'s span would show up as a disconnected root
        // instead of nested under this RPC's.
        let span = tracing::Span::current();
        tokio::spawn(
            async move {
                let _guard = guard;
                leader.append_writes(mutations).await
            }
            .instrument(span),
        )
        .await
        .map_err(|e| TxnError::Internal(Status::internal(format!("the commit task failed: {e}"))))?
        .map_err(Into::into)
    }

    /// Shared body of `add_server`/`remove_server`: both are the same
    /// operation with a different `ConfigChange`, and both answer
    /// `NotLeader` rather than forwarding (Raft §6.2).
    async fn propose(
        &self,
        change: ConfigChange,
    ) -> Result<Response<ConfigChangeResponse>, Status> {
        let result = async {
            let leader = self.leader()?;
            leader.propose_config(change).await
        }
        .await;

        Ok(Response::new(ConfigChangeResponse {
            result: Some(match result {
                Ok(_) => config_change_response::Result::Ok(true),
                Err(e) => config_change_response::Result::Error(client_err(e)?),
            }),
        }))
    }

    /// What a lock-free linearizable read must establish: this node
    /// leads, its `latest` is complete, and it still holds a quorum of
    /// leases.
    ///
    /// The same proof `transaction_read_quorum` rests on, minus the
    /// transaction -- nothing here belongs to one that would have to go
    /// on holding the key afterwards, so nothing is locked and nothing
    /// is held after the answer is given.
    async fn read_quorum(&self) -> Result<(), RaftError> {
        let leader = self.leader()?;
        leader.await_caught_up().await?;
        if !leader.has_read_quorum() {
            return Err(RaftError::NotLeader { leader_hint: None });
        }
        Ok(())
    }

    /// A read that reflects everything committed anywhere in the
    /// cluster, without taking a lock.
    async fn linearizable_read(&self, key: Vec<u8>) -> Result<Option<(u64, Vec<u8>)>, RaftError> {
        self.read_quorum().await?;
        self.store
            .run(move |log| log.get_latest(&key))
            .await
            .map_err(RaftError::Storage)
    }

    /// `linearizable_read`, for a caller that wants the value and not the
    /// seq it came at.
    async fn linearizable_get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, RaftError> {
        Ok(self.linearizable_read(key).await?.map(|(_, value)| value))
    }

    /// The seq alone, linearizably -- `linearizable_read` without the
    /// read of the value, which is the whole saving.
    async fn linearizable_version(&self, key: Vec<u8>) -> Result<u64, RaftError> {
        self.read_quorum().await?;
        Ok(self
            .store
            .run(move |log| log.get_version(&key))
            .await
            .map_err(RaftError::Storage)?
            .unwrap_or(0))
    }

    /// Resolve a snapshot id to the seq it reads at.
    ///
    /// A local read, deliberately: the record's existence on this node
    /// is itself the proof that this node has applied everything the
    /// snapshot could need (see `storage::checkpoint`), so there is
    /// nothing a leader could add.
    async fn checkpoint_seq(&self, id: &[u8]) -> Result<u64, Status> {
        if id.is_empty() {
            return Err(Status::invalid_argument(
                "a snapshot id cannot be empty; leave `consistency` unset for a current read",
            ));
        }
        let owned = id.to_vec();
        let found = self
            .store
            .run(move |log| log.checkpoint_seq(&owned))
            .await
            .map_err(storage_err)?;
        found.ok_or_else(|| {
            Status::not_found(
                "no such snapshot; it may have been removed, or not reached this node yet",
            )
        })
    }

    /// Create a checkpoint: write its record, once, and report the seq
    /// that write landed at -- which *is* the snapshot's seq.
    ///
    /// The `put_user` shape verbatim, and for the same reason: the
    /// read-and-then-write has to be one transaction or two callers
    /// could both find the id absent and both create it, and the second
    /// record would silently move the snapshot forward in time.
    async fn create_checkpoint_inner(
        &self,
        extensions: &tonic::Extensions,
        req: CreateSnapshotRequest,
    ) -> Result<Checkpoint, AuthFailure> {
        self.require_admin(extensions).await?;
        validate_snapshot_id(&req.snapshot_id)?;

        let live = self
            .store
            .run(|log| log.checkpoint_count())
            .await
            .map_err(|e| TxnError::Raft(RaftError::Storage(e)))?;
        if live >= MAX_LIVE_CHECKPOINTS {
            return Err(Status::resource_exhausted(format!(
                "this cluster already holds {MAX_LIVE_CHECKPOINTS} snapshots; \
                 remove one before creating another"
            ))
            .into());
        }

        let key = checkpoint_key(&req.snapshot_id);
        let seq = self
            .admin_txn(async |id| {
                self.write_checkpoint(id, &key, Some(req.created_unix_ms))
                    .await
            })
            .await?;
        Ok(Checkpoint {
            id: req.snapshot_id,
            seq,
            created_unix_ms: req.created_unix_ms,
        })
    }

    async fn remove_checkpoint_inner(
        &self,
        extensions: &tonic::Extensions,
        req: RemoveSnapshotRequest,
    ) -> Result<u64, AuthFailure> {
        self.require_admin(extensions).await?;
        validate_snapshot_id(&req.snapshot_id)?;

        let key = checkpoint_key(&req.snapshot_id);
        self.admin_txn(async |id| self.write_checkpoint(id, &key, None).await)
            .await
    }

    /// `write_user` for checkpoints: `Some(created)` creates, `None`
    /// removes, and either is refused if the record is not in the state
    /// that implies.
    async fn write_checkpoint(
        &self,
        id: TxnId,
        key: &[u8],
        created_unix_ms: Option<u64>,
    ) -> Result<u64, AuthFailure> {
        let existing = self.transactional_read(id, key).await?.value;
        match (&created_unix_ms, &existing) {
            (Some(_), Some(_)) => {
                return Err(
                    Status::already_exists("a snapshot with that id already exists").into(),
                );
            }
            (None, None) => {
                return Err(Status::not_found("there is no snapshot with that id").into());
            }
            _ => {}
        }

        let value = created_unix_ms
            .map(crate::storage::encode_record)
            .transpose()
            .map_err(|e| TxnError::Raft(RaftError::Storage(e)))?;
        self.locks.buffer_write(id, key, value)?;
        Ok(self.commit(id).await?)
    }

    /// The account record for `username`, read currently.
    async fn load_user(&self, username: &str) -> Result<Option<UserRecord>, AuthFailure> {
        match self
            .linearizable_get(crate::keyspace::user_key(username))
            .await?
        {
            Some(bytes) => Ok(Some(UserRecord::from_bytes(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Writes an account record, or removes it when `record` is `None`,
    /// in a transaction of its own -- see [`admin_txn`] for why that is
    /// what this takes rather than an append.
    ///
    /// [`admin_txn`]: KvService::admin_txn
    async fn put_user(
        &self,
        username: &str,
        record: Option<UserRecord>,
    ) -> Result<u64, AuthFailure> {
        let key = crate::keyspace::user_key(username);
        self.admin_txn(async |id| self.write_user(id, &key, record).await)
            .await
    }

    /// Runs `f` inside a transaction of the server's own, on the leader,
    /// and releases what it held however it ends.
    ///
    /// Every administrative write is a read-modify-write -- whether the
    /// record already exists is what decides if the call is allowed at
    /// all -- so the check and the write have to be atomic against
    /// another administrator doing the same thing at the same moment.
    /// That is why these drive an ordinary transaction instead of
    /// appending directly, and in turn why reserved keys are refused at
    /// the RPC boundary rather than in `Locks`: the lock table cannot
    /// tell this write from a client's, and must not.
    ///
    /// Nothing a client does can contend for these keys, since `write`
    /// refuses them, so the only competition is another administrative
    /// call. Two racing ones resolve the way any two transactions do,
    /// and the loser is told its transaction aborted.
    ///
    /// Clients and the server do share one thing, though: the open
    /// transaction table. Enough concurrent client transactions to fill
    /// `MAX_OPEN_TRANSACTIONS` will refuse this one too, surfacing as
    /// `resource_exhausted`. Left as it is rather than given reserved
    /// headroom, because an administrator retrying is a far smaller
    /// problem than a second allocation rule to keep consistent with the
    /// first.
    ///
    /// The discard is explicit rather than a `Drop` guard: discarding a
    /// transaction that already committed is a no-op, and the commit
    /// path itself hands its locks to the task doing the append, so
    /// there is nothing here that releasing early could cut short.
    async fn admin_txn<T>(
        &self,
        f: impl AsyncFnOnce(TxnId) -> Result<T, AuthFailure>,
    ) -> Result<T, AuthFailure> {
        let leader = self.leader()?;
        leader.await_caught_up().await?;
        let id = self
            .locks
            .create(leader.term(), ADMIN_TXN_TTL, crate::txn::fresh_id())?;
        let result = f(id).await;
        self.locks.discard(id);
        result
    }

    /// The body of `put_user`, inside its transaction.
    async fn write_user(
        &self,
        id: TxnId,
        key: &[u8],
        record: Option<UserRecord>,
    ) -> Result<u64, AuthFailure> {
        let existing = self.transactional_read(id, key).await?.value;
        match (&record, &existing) {
            (Some(_), Some(_)) => {
                return Err(AuthFailure::refused(
                    auth_error::Kind::AlreadyExists,
                    "that account already exists",
                ));
            }
            (None, None) => {
                return Err(AuthFailure::refused(
                    auth_error::Kind::NoSuchUser,
                    "there is no such account",
                ));
            }
            _ => {}
        }

        let value = record.map(|record| record.to_bytes()).transpose()?;
        self.locks.buffer_write(id, key, value)?;
        Ok(self.commit(id).await?)
    }

    /// The body of `write`: the two checks, then the buffer.
    ///
    /// The ACL is consulted where the key is named rather than at
    /// commit, so a caller learns which key it may not write, at the
    /// moment it names one, instead of losing a whole transaction later
    /// to an error that could not say. The cost is that a permission
    /// withdrawn between here and the commit does not stop this write --
    /// the same window a linearizable read has, and for the same reason:
    /// nothing is held after an answer is given.
    async fn write_inner(
        &self,
        extensions: &tonic::Extensions,
        req: WriteRequest,
    ) -> Result<(), AuthFailure> {
        let id = txn_id(&req.transaction_id)?;
        // The privilege-escalation guard. A client able to buffer a
        // write to a reserved key could write its own account record,
        // with `is_admin` set to whatever it liked.
        reject_reserved(&req.key)?;
        // A delete is a write of no value, so this covers both.
        self.require_access(extensions, &req.key, Right::Write)
            .await?;

        // Purely in-memory: nothing reaches the log until `commit`. The
        // leader handle is fetched only for the term check inside
        // `transaction_leader` -- a transaction cannot be added to from
        // anywhere but the leader that created it.
        self.transaction_leader(id)?;
        self.locks.buffer_write(id, &req.key, req.value)?;
        Ok(())
    }

    /// Checks that whoever sent this request is an administrator.
    ///
    /// The token says who they are; the store says what they may do.
    /// Read fresh on every call rather than carried as a claim, because
    /// a right baked into a token cannot be withdrawn until that token
    /// expires. These calls are rare and already pay a raft round trip,
    /// so the read costs nothing worth saving.
    async fn require_admin(&self, extensions: &tonic::Extensions) -> Result<(), AuthFailure> {
        // A node with no secret keeps no accounts, so there is nobody to
        // be an administrator and nothing to check against. This is the
        // one place the open case is decided: every caller of this is
        // spared having to remember it, and there is a single line to
        // read when asking what an unauthenticated cluster permits.
        if self.auth.is_none() {
            return Ok(());
        }
        // Missing only if the auth layer did not run. That is a
        // misconfiguration rather than a forged request, but the safe
        // reading of the two is the same.
        let Some(context) = extensions.get::<AuthContext>() else {
            return Err(
                Status::unauthenticated("this request carried no verified identity").into(),
            );
        };
        match self.load_user(&context.username).await? {
            Some(user) if user.is_admin => Ok(()),
            // Either not an admin, or an account deleted since its token
            // was issued. Neither may proceed, and neither is told which
            // it was.
            // Deliberately not naming the operation: this guards
            // account management and membership changes alike, and a
            // message that named one of them would be wrong for the
            // other.
            _ => Err(AuthFailure::refused(
                auth_error::Kind::NotAdmin,
                "this account is not an administrator",
            )),
        }
    }

    async fn login_inner(&self, req: LoginRequest) -> Result<LoginOk, AuthFailure> {
        let Some(auth) = self.auth.as_ref() else {
            return Err(AuthFailure::disabled());
        };
        // A name that could never have been stored cannot match an
        // account, and refusing it here keeps `user_key` to the shape it
        // is sound for.
        crate::keyspace::validate_username(&req.username)?;

        let stored = self.password_hash_for(auth, &req.username).await?;
        if !crate::auth::verify_password_or_dummy(req.password, stored).await? {
            return Err(AuthFailure::bad_credentials());
        }

        let (token, expires_unix) = auth.issue(&req.username)?;
        Ok(LoginOk {
            token,
            expires_unix,
        })
    }

    /// What to check a login against: the stored account's hash, or --
    /// on a node that is not yet part of any cluster -- the one this
    /// node was started with.
    ///
    /// That fallback is what breaks the circle at the very beginning. A
    /// fresh node has no accounts, and no leader to read them from, yet
    /// `Bootstrap` needs a token like everything else; so the operator
    /// has to be able to authenticate against something. It is gated on
    /// the node having no configuration rather than on no account
    /// existing, because the latter would quietly re-arm the startup
    /// password the moment the last account was deleted. Once this node
    /// has been bootstrapped or added to a cluster, the flag is inert
    /// for good.
    async fn password_hash_for(
        &self,
        auth: &AuthConfig,
        username: &str,
    ) -> Result<Option<String>, AuthFailure> {
        let uninitialized = self
            .store
            .run(RaftLog::current_config)
            .await
            .map_err(|e| AuthFailure::from(RaftError::Storage(e)))?
            .is_none();

        if uninitialized {
            return Ok(auth
                .initial_admin
                .as_ref()
                .filter(|admin| admin.username == username)
                .map(|admin| admin.password_hash.clone()));
        }
        Ok(self
            .load_user(username)
            .await?
            .map(|user| user.password_hash))
    }

    /// Checks that whoever is bootstrapping this node is the
    /// administrator it was started expecting.
    ///
    /// `require_admin` cannot be used here. It reads the store, and a
    /// node with no cluster has no leader to read from -- which is the
    /// very reason the startup administrator exists. So the check is
    /// against that configured identity instead, which is also what
    /// stops a token minted for some other cluster sharing this secret
    /// from bootstrapping this node.
    fn require_startup_admin(&self, extensions: &tonic::Extensions) -> Result<(), Status> {
        let Some(auth) = self.auth.as_ref() else {
            return Ok(());
        };
        let Some(context) = extensions.get::<AuthContext>() else {
            return Err(Status::unauthenticated(
                "this request carried no verified identity",
            ));
        };
        match auth.initial_admin.as_ref() {
            Some(admin) if admin.username == context.username => Ok(()),
            _ => Err(Status::permission_denied(
                "only the administrator this node was started with may bootstrap it",
            )),
        }
    }

    /// Writes that administrator into the log, once this node is
    /// actually leading.
    ///
    /// `client_handle.bootstrap` only installs the configuration; the
    /// node still has to win its (unopposed) election and commit its own
    /// no-op before anything can be appended. Hence the wait rather than
    /// an immediate append -- and hence `Bootstrap` answering only once
    /// this has happened, so that "ok" means there is an administrator
    /// rather than that there will shortly be one.
    ///
    /// Idempotent: a record already there is the end state this wants.
    async fn write_startup_admin(&self) -> Result<(), Status> {
        let Some(admin) = self
            .auth
            .as_ref()
            .and_then(|auth| auth.initial_admin.as_ref())
        else {
            return Ok(());
        };
        let record = UserRecord::new(admin.username.clone(), admin.password_hash.clone(), true);

        let deadline = std::time::Instant::now() + BOOTSTRAP_ADMIN_TIMEOUT;
        loop {
            match self.put_user(&admin.username, Some(record.clone())).await {
                Ok(_) => return Ok(()),
                Err(AuthFailure::Refused(auth_error::Kind::AlreadyExists, _)) => return Ok(()),
                Err(_) if std::time::Instant::now() < deadline => {
                    tokio::time::sleep(BOOTSTRAP_ADMIN_RETRY).await;
                }
                // The configuration is installed and this node is a
                // cluster, but nobody can administer it: the startup
                // password stops working the moment a configuration
                // exists. Said plainly, because the way out is to
                // discard this node's directory and bootstrap again.
                Err(_) => {
                    return Err(Status::internal(
                        "this node was bootstrapped but its administrator could not be written, \
                         so the cluster has no account that can manage it; discard this node's \
                         database directory and bootstrap again",
                    ));
                }
            }
        }
    }

    async fn create_user_inner(
        &self,
        extensions: &tonic::Extensions,
        req: CreateUserRequest,
    ) -> Result<u64, AuthFailure> {
        if self.auth.is_none() {
            return Err(AuthFailure::disabled());
        }
        crate::keyspace::validate_username(&req.username)?;
        self.require_admin(extensions).await?;

        let password_hash = crate::auth::hash_password(req.password).await?;
        let record = UserRecord::new(req.username.clone(), password_hash, req.is_admin);
        self.put_user(&req.username, Some(record)).await
    }

    async fn delete_user_inner(
        &self,
        extensions: &tonic::Extensions,
        req: DeleteUserRequest,
    ) -> Result<u64, AuthFailure> {
        if self.auth.is_none() {
            return Err(AuthFailure::disabled());
        }
        crate::keyspace::validate_username(&req.username)?;
        self.require_admin(extensions).await?;
        self.put_user(&req.username, None).await
    }

    /// Every outstanding invitation, split into the ones that can still
    /// be spent and the ids of the ones that cannot.
    ///
    /// A local read, not a linearizable one. Both callers can take that:
    /// `ListInvites` is a listing, stale on the same terms as any read
    /// outside a transaction, and `CreateInvite` uses it only to count
    /// and to sweep -- an invitation this node has not heard of yet is
    /// one more that goes uncounted and unswept, which costs a slot and
    /// a sweep, not correctness. Nothing here decides whether an invite
    /// may be *spent*; that happens under a lock in `redeem_invite`.
    async fn read_invites(
        &self,
    ) -> Result<(Vec<(String, InviteRecord)>, Vec<String>), AuthFailure> {
        let rows = self
            .store
            .run(|log| log.list_records_under(crate::keyspace::INVITE_PREFIX))
            .await
            .map_err(|e| AuthFailure::from(RaftError::Storage(e)))?;

        // One `now` for the whole partition, so a listing cannot report
        // two invitations expiring in the same millisecond differently.
        let now = crate::auth::now_unix_ms();
        let mut live = Vec::new();
        let mut lapsed = Vec::new();
        for (id, value) in rows {
            let id = String::from_utf8(id)
                .map_err(|_| Status::internal("a stored invitation's id is not text"))?;
            let record = InviteRecord::from_bytes(&value)?;
            if record.has_expired(now) {
                lapsed.push(id);
            } else {
                live.push((id, record));
            }
        }
        Ok((live, lapsed))
    }

    /// Mints an invitation and writes the cluster's half of it.
    ///
    /// The secret exists in this process, in the response, and nowhere
    /// else: what goes into the log is its SHA-256, which is also the
    /// key the record is written under. So this is the only moment the
    /// token is knowable, and an issuer who loses it has to revoke and
    /// mint again -- there is nothing to look it up from.
    async fn create_invite_inner(
        &self,
        extensions: &tonic::Extensions,
        req: CreateInviteRequest,
    ) -> Result<CreateInviteOk, AuthFailure> {
        if self.auth.is_none() {
            return Err(AuthFailure::disabled());
        }
        self.require_admin(extensions).await?;
        let issued_by = caller(extensions)?;

        // A name that could never be stored cannot be pinned to, and
        // refusing it here means the pin is checked once, now, rather
        // than by the recipient discovering their invitation is
        // unusable.
        if let Some(username) = req.username.as_deref() {
            crate::keyspace::validate_username(username)?;
        }
        let ttl = invite_ttl(req.ttl_ms)?;

        let (live, lapsed) = self.read_invites().await?;
        if live.len() >= MAX_LIVE_INVITES {
            return Err(Status::resource_exhausted(format!(
                "this cluster already holds {MAX_LIVE_INVITES} outstanding invitations; \
                 withdraw one before issuing another"
            ))
            .into());
        }

        let secret = InviteSecret::mint()?;
        let record = InviteRecord::new(
            issued_by,
            req.is_admin,
            req.username,
            crate::auth::now_unix_ms().saturating_add(ttl.as_millis() as u64),
        );
        let key = crate::keyspace::invite_key(&secret.id);
        let value = record.to_bytes()?;

        let seq = self
            .admin_txn(async |id| {
                // Two distinct 32-byte secrets hashing alike is not a
                // thing that happens, so this is not a race being
                // guarded against -- it is the invariant that a record
                // is written once, stated where it could be violated.
                if self.transactional_read(id, &key).await?.value.is_some() {
                    return Err(
                        Status::internal("the invitation drawn already exists; try again").into(),
                    );
                }
                // Lapsed invitations are swept here rather than by
                // anything of their own. There is no timer in this
                // cluster to hang an expiry on and no state machine hook
                // to run one from, so the work is folded into the call
                // that has a reason to care how many there are -- which
                // keeps the reserved range from accumulating records
                // that can never be spent again.
                for id_of_lapsed in &lapsed {
                    self.locks.buffer_write(
                        id,
                        &crate::keyspace::invite_key(id_of_lapsed),
                        None,
                    )?;
                }
                self.locks.buffer_write(id, &key, Some(value))?;
                Ok(self.commit(id).await?)
            })
            .await?;

        Ok(CreateInviteOk {
            token: secret.token,
            invite: Some(pb_invite(secret.id, record)),
            seq,
        })
    }

    /// Spends an invitation on a new account.
    ///
    /// Answered for anyone: this is the one write on the service that
    /// runs without a token (see `auth::EXEMPT`), because its callers
    /// are by definition people who have no account yet. The invitation
    /// stands in for the token, and everything below turns on it.
    ///
    /// The checking happens twice on purpose, and the two are not the
    /// same check.
    ///
    /// The first is a plain read, before any work: an anonymous caller
    /// can ask this node to do whatever it does before it decides, so
    /// what it does before it decides had better be cheap. Hashing a
    /// password with Argon2 costs 19 MiB and several milliseconds --
    /// worth paying for a real redemption, and not worth paying for a
    /// stranger sending in garbage.
    ///
    /// The second is inside the transaction, under a lock, and is the
    /// one that counts. Between the two the invitation could have been
    /// revoked, spent by someone else, or lapsed; the account name could
    /// have been taken. That is why the record is read again rather than
    /// carried over, and why `is_admin` is taken from the *locked* read
    /// -- the rights the account gets must come from the invitation as
    /// it stood when it was spent, not as it stood when it was glanced
    /// at.
    ///
    /// The account and the invitation's removal are buffered into one
    /// transaction, so they land at one sequence number or not at all.
    /// That is the whole of "single use": two callers racing with the
    /// same token contend for the same key, and one of them loses.
    async fn redeem_invite_inner(&self, req: RedeemInviteRequest) -> Result<u64, AuthFailure> {
        if self.auth.is_none() {
            return Err(AuthFailure::disabled());
        }
        // Bounded before it is hashed. Everything else about the token
        // is answered by whether its hash names a record, which is the
        // point of keying records by that hash -- but the hashing itself
        // is work, and an anonymous caller does not get to choose how
        // much of it to ask for.
        if req.token.is_empty() || req.token.len() > crate::auth::MAX_INVITE_TOKEN_LEN {
            return Err(AuthFailure::bad_invite(
                "that is not an invitation this cluster issued",
            ));
        }
        crate::keyspace::validate_username(&req.username)?;

        let invite_key = crate::keyspace::invite_key(&crate::auth::invite_id(&req.token));
        let user_key = crate::keyspace::user_key(&req.username);

        let Some(bytes) = self.linearizable_get(invite_key.clone()).await? else {
            return Err(AuthFailure::bad_invite(
                "this cluster is not holding that invitation; it may already have been \
                 used, or withdrawn",
            ));
        };
        check_invite(&InviteRecord::from_bytes(&bytes)?, &req.username)?;

        // Outside the transaction: Argon2 is deliberately slow, and
        // holding a lock on the account name for the length of it would
        // make every redemption wait on every other one.
        let password_hash = crate::auth::hash_password(req.password).await?;

        self.admin_txn(async |id| {
            let Some(bytes) = self.transactional_read(id, &invite_key).await?.value else {
                return Err(AuthFailure::bad_invite(
                    "this cluster is not holding that invitation; it may already have been \
                     used, or withdrawn",
                ));
            };
            let record = InviteRecord::from_bytes(&bytes)?;
            check_invite(&record, &req.username)?;

            if self
                .transactional_read(id, &user_key)
                .await?
                .value
                .is_some()
            {
                // Deliberately not spent. The holder chose a name
                // somebody already has, which is a thing they can fix by
                // choosing another -- and burning their invitation for
                // it would leave them unable to.
                return Err(AuthFailure::refused(
                    auth_error::Kind::AlreadyExists,
                    "that account already exists; the invitation has not been spent, so \
                     another name can be tried with it",
                ));
            }

            let user = UserRecord::new(req.username.clone(), password_hash, record.is_admin);
            self.locks
                .buffer_write(id, &user_key, Some(user.to_bytes()?))?;
            self.locks.buffer_write(id, &invite_key, None)?;
            Ok(self.commit(id).await?)
        })
        .await
    }

    async fn list_invites_inner(
        &self,
        extensions: &tonic::Extensions,
    ) -> Result<Vec<PbInvite>, AuthFailure> {
        if self.auth.is_none() {
            return Err(AuthFailure::disabled());
        }
        self.require_admin(extensions).await?;

        // Lapsed ones are left out rather than listed as expired. They
        // cannot be spent and the next `CreateInvite` will sweep them,
        // so reporting them would be reporting the cluster's own
        // housekeeping as something to act on.
        let (live, _) = self.read_invites().await?;
        Ok(live
            .into_iter()
            .map(|(id, record)| pb_invite(id, record))
            .collect())
    }

    async fn revoke_invite_inner(
        &self,
        extensions: &tonic::Extensions,
        req: RevokeInviteRequest,
    ) -> Result<u64, AuthFailure> {
        if self.auth.is_none() {
            return Err(AuthFailure::disabled());
        }
        self.require_admin(extensions).await?;
        crate::keyspace::validate_invite_id(&req.invite_id)?;

        let key = crate::keyspace::invite_key(&req.invite_id);
        self.admin_txn(async |id| {
            // An id that is not here is `not_found` rather than a quiet
            // success. Withdrawing an invitation is something an
            // administrator does because they believe a live one is out
            // there, and "already gone" is the answer they most need to
            // hear -- it means someone spent it first.
            if self.transactional_read(id, &key).await?.value.is_none() {
                return Err(Status::not_found(
                    "there is no such invitation; it may have been redeemed already",
                )
                .into());
            }
            self.locks.buffer_write(id, &key, None)?;
            Ok(self.commit(id).await?)
        })
        .await
    }

    async fn get_config_internal(&self) -> Result<ClusterConfig, StorageError> {
        let bytes = self.store.run(RaftLog::current_config).await?;

        match bytes {
            Some(bytes) => ClusterConfig::from_bytes(&bytes)
                .map_err(|_| StorageError::Corrupt("stored cluster config was corrupt")),
            // Not bootstrapped or added to a cluster yet -- not an error,
            // just an empty configuration.
            None => Ok(ClusterConfig::default()),
        }
    }
}

#[tonic::async_trait]
impl Kv for KvService {
    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        // `into_parts` rather than `into_inner`: who the caller is lives
        // in the extensions, and an ACL is about them.
        let (_, extensions, req) = request.into_parts();
        let result = self.write_inner(&extensions, req).await;

        Ok(Response::new(WriteResponse {
            result: Some(match result {
                Ok(()) => write_response::Result::Ok(true),
                Err(e) => match auth_failure(e)? {
                    AuthReply::Raft(e) => write_response::Result::Error(e),
                    AuthReply::Auth(e) => write_response::Result::AuthError(e),
                },
            }),
        }))
    }

    async fn read(&self, request: Request<ReadRequest>) -> Result<Response<ReadResponse>, Status> {
        let (_, extensions, req) = request.into_parts();
        // Before the branch, so it covers the transactional read too.
        // Refused rather than answered as absent: a password hash handed
        // back is one to attack offline, at leisure.
        reject_reserved(&req.key)?;

        // Likewise before the branch: every consistency reads the same
        // key, and which one was asked for has no bearing on whether
        // this caller may have it.
        if let Err(e) = self
            .require_access(&extensions, &req.key, Right::Read)
            .await
        {
            return Ok(Response::new(ReadResponse {
                result: Some(match auth_failure(e)? {
                    AuthReply::Raft(e) => read_response::Result::Error(e),
                    AuthReply::Auth(e) => read_response::Result::AuthError(e),
                }),
            }));
        }

        // A transactional read is a different thing again: it takes a
        // lock, and it can be answered from what the transaction itself
        // has buffered -- only this transaction's own leader, still
        // holding a read quorum, will serve one.
        if let Some(read_request::Consistency::TransactionId(id)) = &req.consistency {
            let id = txn_id(id)?;
            let result = self.transactional_read(id, &req.key).await;
            return Ok(Response::new(ReadResponse {
                result: Some(match result {
                    Ok(ok) => read_response::Result::Ok(ok),
                    Err(e) => read_response::Result::Error(txn_error(e)?),
                }),
            }));
        }

        // Linearizable without a transaction: this node proves it still
        // leads and still holds a read quorum, and then answers. No lock,
        // so nothing is held once the answer is given -- the key is free
        // to change the moment after.
        if let Some(read_request::Consistency::Linearizable(true)) = req.consistency {
            return Ok(Response::new(ReadResponse {
                result: Some(match self.linearizable_read(req.key).await {
                    Ok(found) => read_response::Result::Ok(read_ok(found)),
                    Err(e) => read_response::Result::Error(client_err(e)?),
                }),
            }));
        }

        // A snapshot read. Stale by construction -- it describes a
        // moment that has already passed -- so any node serves it, with
        // no leader check and no quorum round.
        if let Some(read_request::Consistency::SnapshotId(id)) = &req.consistency {
            let as_of = self.checkpoint_seq(id).await?;
            let key = req.key;
            let found = self
                .store
                .run(move |log| log.get_as_of(&key, as_of))
                .await
                .map_err(storage_err)?;
            return Ok(Response::new(ReadResponse {
                result: Some(read_response::Result::Ok(read_ok(found))),
            }));
        }

        // Everything else: `consistency` unset, or `linearizable: false`,
        // which says the caller asked for no guarantee rather than failed
        // to ask. Answered from local state by whoever received it,
        // leader or not, with no read-quorum round and no wait to catch
        // up. So it can be behind -- by a write still replicating on the
        // leader, or by however far this node has fallen behind.
        let found = self
            .store
            .run(move |log| log.get_latest(&req.key))
            .await
            .map_err(storage_err)?;

        Ok(Response::new(ReadResponse {
            result: Some(read_response::Result::Ok(read_ok(found))),
        }))
    }

    async fn get_version(
        &self,
        request: Request<GetVersionRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        let (_, extensions, req) = request.into_parts();
        // Before the branch, as in `read`, and for the same reason. The
        // seq of an account record is still a fact about the reserved
        // range, and "this key changed at seq N" is enough to watch the
        // administration of a cluster one has no business watching.
        reject_reserved(&req.key)?;

        // The same reasoning carries to an ACL: "this key changed at
        // seq N" is a fact about a key, and one the caller may not read
        // is one they may not learn that about either.
        if let Err(e) = self
            .require_access(&extensions, &req.key, Right::Read)
            .await
        {
            return Ok(Response::new(GetVersionResponse {
                result: Some(match auth_failure(e)? {
                    AuthReply::Raft(e) => get_version_response::Result::Error(e),
                    AuthReply::Auth(e) => get_version_response::Result::AuthError(e),
                }),
            }));
        }

        if let Some(get_version_request::Consistency::TransactionId(id)) = &req.consistency {
            let id = txn_id(id)?;
            let result = self.transactional_version(id, &req.key).await;
            return Ok(Response::new(GetVersionResponse {
                result: Some(match result {
                    Ok(ok) => get_version_response::Result::Ok(ok),
                    Err(e) => get_version_response::Result::Error(txn_error(e)?),
                }),
            }));
        }

        if let Some(get_version_request::Consistency::Linearizable(true)) = req.consistency {
            return Ok(Response::new(GetVersionResponse {
                result: Some(match self.linearizable_version(req.key).await {
                    Ok(seq) => get_version_response::Result::Ok(GetVersionOk { seq }),
                    Err(e) => get_version_response::Result::Error(client_err(e)?),
                }),
            }));
        }

        if let Some(get_version_request::Consistency::SnapshotId(id)) = &req.consistency {
            let as_of = self.checkpoint_seq(id).await?;
            let key = req.key;
            let seq = self
                .store
                .run(move |log| log.version_as_of(&key, as_of))
                .await
                .map_err(storage_err)?;
            return Ok(Response::new(GetVersionResponse {
                result: Some(get_version_response::Result::Ok(GetVersionOk {
                    seq: seq.unwrap_or(0),
                })),
            }));
        }

        // Stale, as `read` is stale, and cheap in the way this RPC exists
        // to be: the seq is part of the row key, so the value behind it
        // is never read whatever it weighs.
        let seq = self
            .store
            .run(move |log| log.get_version(&req.key))
            .await
            .map_err(storage_err)?;

        Ok(Response::new(GetVersionResponse {
            result: Some(get_version_response::Result::Ok(GetVersionOk {
                seq: seq.unwrap_or(0),
            })),
        }))
    }

    async fn get_keys(
        &self,
        request: Request<GetKeysRequest>,
    ) -> Result<Response<GetKeysResponse>, Status> {
        let (_, extensions, req) = request.into_parts();
        // `scan` would skip the whole reserved range anyway, so this
        // refusal protects nothing the filter doesn't. It is here to say
        // where the boundary is: a caller starting a walk at the very
        // beginning passes an empty key, which is not reserved, so
        // nothing legitimate asks for this.
        reject_reserved(&req.start_key)?;

        // What this caller may not read, resolved once for the whole
        // call. Unlike `read`, a walk is never refused for it: keys the
        // caller may not see are left out, so a caller whose access is
        // one subtree of a large store can still walk the store.
        let filter = self.scan_filter(&extensions).await?;

        // Refused rather than shortened, so a caller paging in batches
        // of N finds out it isn't getting N instead of silently getting
        // a different shape of answer than it asked for.
        if req.max_keys > MAX_KEYS_PER_SCAN {
            return Err(Status::resource_exhausted(
                "that is more keys than this server will scan in one call",
            ));
        }
        let limit = match req.max_keys {
            0 => DEFAULT_MAX_KEYS,
            n => n,
        } as usize;

        if let Some(get_keys_request::Consistency::TransactionId(id)) = &req.consistency {
            let id = txn_id(id)?;
            let result = self
                .transactional_scan(id, req.start_key, limit, &filter)
                .await;
            return Ok(Response::new(GetKeysResponse {
                result: Some(match result {
                    Ok(batch) => get_keys_response::Result::Ok(get_keys_ok(batch)),
                    Err(e) => get_keys_response::Result::Error(txn_error(e)?),
                }),
            }));
        }

        // A snapshot walk: the keys that had a value as of that seq,
        // including ones deleted since and excluding ones created
        // since. Stale by construction, so any node serves it.
        let as_of = match &req.consistency {
            Some(get_keys_request::Consistency::SnapshotId(id)) => self.checkpoint_seq(id).await?,
            _ => u64::MAX,
        };

        // Outside a transaction, the same terms as `read`: local state,
        // no locks, whoever received it. There is nothing to merge --
        // no transaction means no buffered writes -- and no second pass,
        // since there are no locks for one to be taken under.
        let batch = self
            .scan(req.start_key, as_of, limit, &BufferedKeys::new(), &filter)
            .await
            .map_err(storage_err)?;

        Ok(Response::new(GetKeysResponse {
            result: Some(get_keys_response::Result::Ok(get_keys_ok(batch))),
        }))
    }

    async fn create_transaction(
        &self,
        request: Request<CreateTransactionRequest>,
    ) -> Result<Response<CreateTransactionResponse>, Status> {
        let ttl = std::time::Duration::from_millis(request.into_inner().ttl_ms);

        let result = async {
            let leader = self.leader()?;
            // The transaction's reads will be answered from `latest`, so
            // a leader that may still be missing entries has no business
            // opening one -- same precondition as a conditional write.
            leader.await_caught_up().await?;
            self.locks
                .create(leader.term(), ttl, crate::txn::fresh_id())
                .map_err(TxnError::from)
        }
        .await;

        Ok(Response::new(CreateTransactionResponse {
            result: Some(match result {
                Ok(id) => create_transaction_response::Result::TransactionId(id.to_vec()),
                Err(e) => create_transaction_response::Result::Error(txn_error(e)?),
            }),
        }))
    }

    async fn commit_transaction(
        &self,
        request: Request<CommitTransactionRequest>,
    ) -> Result<Response<CommitTransactionResponse>, Status> {
        let id = txn_id(&request.into_inner().transaction_id)?;
        let result = self.commit(id).await;

        Ok(Response::new(CommitTransactionResponse {
            result: Some(match result {
                // `append_writes` returns only once the entry has
                // committed *and* been applied, so by the time a client
                // sees this seq, every key the transaction wrote reads
                // back at it.
                Ok(seq) => commit_transaction_response::Result::Seq(seq),
                Err(e) => commit_transaction_response::Result::Error(txn_error(e)?),
            }),
        }))
    }

    async fn drop_transaction(
        &self,
        request: Request<DropTransactionRequest>,
    ) -> Result<Response<DropTransactionResponse>, Status> {
        let id = txn_id(&request.into_inner().transaction_id)?;
        // Unconditional, and deliberately not leader-checked: giving up
        // locks should never be refused, and a node that isn't the
        // leader has none of this transaction's state to give up anyway.
        self.locks.discard(id);

        Ok(Response::new(DropTransactionResponse {
            result: Some(drop_transaction_response::Result::Ok(true)),
        }))
    }

    async fn add_server(
        &self,
        request: Request<AddServerRequest>,
    ) -> Result<Response<ConfigChangeResponse>, Status> {
        let (_, extensions, req) = request.into_parts();

        // Checked before the round trip below, not just inside `propose`:
        // a follower has no business dialing the new server only to find
        // out afterwards that it should have answered `NotLeader` and
        // sent the caller elsewhere. `propose` still re-checks at the
        // point of committing, since leadership can change during the
        // round trip.
        if let Err(e) = require_leader(&self.client_handle.role()) {
            return not_leader_response(e);
        }
        // And before it too: dialing the server named by a caller who
        // may not do this is a side effect on that caller's say-so.
        if let Err(e) = self.require_admin(&extensions).await {
            return config_change_refusal(e);
        }

        // First, ask the new server who it is -- over its peer API,
        // dialed exactly as it will be once it is a peer, so a server
        // this node can't replicate to fails here rather than after
        // it has been added.
        let mut client = RaftPeerClient::new(
            raft::peer_endpoint(&req.peer_address, self.peer_tls.as_ref())
                .map_err(|e| Status::invalid_argument(format!("invalid address: {e}")))?
                .connect_lazy(),
        );
        let response = client
            .identify(Request::new(IdentifyRequest {}))
            .await
            .map_err(|e| {
                Status::unavailable(format!(
                    "failed to reach the new server at {}: {e}",
                    req.peer_address
                ))
            })?
            .into_inner();

        if response.in_cluster {
            return Err(Status::invalid_argument(
                "the new server is already part of a cluster",
            ));
        }
        let identity = ClusterMember {
            server_id: response.server_id,
            address: response.address,
            client_address: response.client_address,
            voting: true,
        };

        // Check that the new server hasn't already been added to the configuration.
        let config = self.get_config_internal().await.map_err(storage_err)?;
        for member in &config.members {
            if member.server_id == identity.server_id
                || member.address == identity.address
                || member.client_address == identity.client_address
            {
                return Err(Status::invalid_argument(format!(
                    "the newly added server: {:?} conflicts with an existing server in the configuration: {:?}",
                    member, identity
                )));
            }
        }

        // Added as a learner, not a voter: see `ConfigChange::AddServer`.
        // It is promoted automatically once it has caught up, so from a
        // client's point of view this one call is the whole operation.
        self.propose(ConfigChange::AddServer {
            server_id: identity.server_id,
            address: identity.address,
            client_address: identity.client_address,
        })
        .await
    }

    async fn remove_server(
        &self,
        request: Request<RemoveServerRequest>,
    ) -> Result<Response<ConfigChangeResponse>, Status> {
        let (_, extensions, req) = request.into_parts();
        if let Err(e) = self.require_admin(&extensions).await {
            return config_change_refusal(e);
        }
        self.propose(ConfigChange::RemoveServer {
            server_id: req.server_id,
        })
        .await
    }

    async fn set_heartbeat(
        &self,
        request: Request<SetHeartbeatRequest>,
    ) -> Result<Response<ConfigChangeResponse>, Status> {
        let (_, extensions, req) = request.into_parts();
        if let Err(e) = self.require_admin(&extensions).await {
            return config_change_refusal(e);
        }
        // Checked here as well as by the leader, so an operator who typed
        // a bad value is told so plainly rather than as a refused change.
        raft::check_heartbeat_ms(req.heartbeat_millis).map_err(Status::invalid_argument)?;
        self.propose(ConfigChange::SetHeartbeat {
            millis: req.heartbeat_millis,
        })
        .await
    }

    async fn bootstrap(
        &self,
        request: Request<BootstrapRequest>,
    ) -> Result<Response<ConfigChangeResponse>, Status> {
        let (_, extensions, req) = request.into_parts();
        if !req.server_id.is_empty() && req.server_id != self.identity.server_id {
            return Err(Status::failed_precondition(format!(
                "this node is {}, not {}",
                self.identity.server_id, req.server_id
            )));
        }
        let heartbeat_millis = match req.heartbeat_millis {
            0 => raft::DEFAULT_HEARTBEAT_MS,
            ms => {
                raft::check_heartbeat_ms(ms).map_err(Status::invalid_argument)?;
                ms
            }
        };
        self.require_startup_admin(&extensions)?;

        // A cluster of exactly one: the node is its own sole voter, so it
        // wins the next election unopposed and can then serve AddServer
        // to grow from there.
        let config = ClusterConfig {
            members: vec![self.identity.clone()],
            heartbeat_millis,
        };
        self.client_handle
            .bootstrap(config)
            .await
            .map_err(raft_err)?;
        // Before answering, not after: the startup password stops
        // working the instant a configuration exists, so an operator
        // told "ok" needs the replacement to already be in the log.
        self.write_startup_admin().await?;

        Ok(Response::new(ConfigChangeResponse {
            result: Some(config_change_response::Result::Ok(true)),
        }))
    }

    async fn get_config(
        &self,
        _request: Request<GetConfigRequest>,
    ) -> Result<Response<GetConfigResponse>, Status> {
        let config = self.get_config_internal().await.map_err(storage_err)?;

        // Both checks are non-blocking on purpose. `is_caught_up` is the
        // same question `await_caught_up` waits for an answer to, asked
        // rather than waited on: a client scanning for the leader wants
        // this node's membership either way, and a leader still applying
        // the tail it inherited should say "not yet" now instead of
        // holding the scan up until it is.
        let is_leader = self
            .leader()
            .is_ok_and(|leader| leader.is_caught_up() && leader.has_read_quorum());

        Ok(Response::new(GetConfigResponse {
            identity: Some(self.identity.clone()),
            members: config.members,
            is_leader,
            heartbeat_millis: config.heartbeat_millis,
        }))
    }

    async fn login(
        &self,
        request: Request<LoginRequest>,
    ) -> Result<Response<LoginResponse>, Status> {
        let result = self.login_inner(request.into_inner()).await;
        Ok(Response::new(LoginResponse {
            result: Some(match result {
                Ok(ok) => login_response::Result::Ok(ok),
                Err(e) => match auth_failure(e)? {
                    AuthReply::Raft(e) => login_response::Result::Error(e),
                    AuthReply::Auth(e) => login_response::Result::AuthError(e),
                },
            }),
        }))
    }

    async fn create_snapshot(
        &self,
        request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        // `into_parts` rather than `into_inner`: who the caller is lives
        // in the extensions, put there by the auth layer.
        let (_, extensions, req) = request.into_parts();
        let result = self.create_checkpoint_inner(&extensions, req).await;
        Ok(Response::new(CreateSnapshotResponse {
            result: Some(match result {
                Ok(cp) => create_snapshot_response::Result::Ok(pb_snapshot(cp)),
                Err(e) => match auth_failure(e)? {
                    AuthReply::Raft(e) => create_snapshot_response::Result::Error(e),
                    AuthReply::Auth(e) => create_snapshot_response::Result::AuthError(e),
                },
            }),
        }))
    }

    async fn remove_snapshot(
        &self,
        request: Request<RemoveSnapshotRequest>,
    ) -> Result<Response<RemoveSnapshotResponse>, Status> {
        let (_, extensions, req) = request.into_parts();
        let result = self.remove_checkpoint_inner(&extensions, req).await;
        Ok(Response::new(RemoveSnapshotResponse {
            result: Some(match result {
                Ok(seq) => remove_snapshot_response::Result::Seq(seq),
                Err(e) => match auth_failure(e)? {
                    AuthReply::Raft(e) => remove_snapshot_response::Result::Error(e),
                    AuthReply::Auth(e) => remove_snapshot_response::Result::AuthError(e),
                },
            }),
        }))
    }

    async fn list_snapshots(
        &self,
        _request: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        // A local read, answerable by any node and stale on the same
        // terms as any read outside a transaction: a snapshot created
        // moments ago may not have reached this one yet. Not
        // admin-gated -- knowing which snapshots exist is what a client
        // needs in order to read at one, and the ids are its own.
        let listed = self
            .store
            .run(|log| log.list_checkpoints())
            .await
            .map_err(storage_err)?;

        Ok(Response::new(ListSnapshotsResponse {
            result: Some(list_snapshots_response::Result::Ok(SnapshotList {
                snapshots: listed.into_iter().map(pb_snapshot).collect(),
            })),
        }))
    }

    async fn create_user(
        &self,
        request: Request<CreateUserRequest>,
    ) -> Result<Response<CreateUserResponse>, Status> {
        // `into_parts` rather than `into_inner`: who the caller is lives
        // in the extensions, put there by the auth layer.
        let (_, extensions, req) = request.into_parts();
        let result = self.create_user_inner(&extensions, req).await;
        Ok(Response::new(CreateUserResponse {
            result: Some(match result {
                Ok(seq) => create_user_response::Result::Seq(seq),
                Err(e) => match auth_failure(e)? {
                    AuthReply::Raft(e) => create_user_response::Result::Error(e),
                    AuthReply::Auth(e) => create_user_response::Result::AuthError(e),
                },
            }),
        }))
    }

    async fn delete_user(
        &self,
        request: Request<DeleteUserRequest>,
    ) -> Result<Response<DeleteUserResponse>, Status> {
        let (_, extensions, req) = request.into_parts();
        let result = self.delete_user_inner(&extensions, req).await;
        Ok(Response::new(DeleteUserResponse {
            result: Some(match result {
                Ok(seq) => delete_user_response::Result::Seq(seq),
                Err(e) => match auth_failure(e)? {
                    AuthReply::Raft(e) => delete_user_response::Result::Error(e),
                    AuthReply::Auth(e) => delete_user_response::Result::AuthError(e),
                },
            }),
        }))
    }

    async fn create_invite(
        &self,
        request: Request<CreateInviteRequest>,
    ) -> Result<Response<CreateInviteResponse>, Status> {
        let (_, extensions, req) = request.into_parts();
        let result = self.create_invite_inner(&extensions, req).await;
        Ok(Response::new(CreateInviteResponse {
            result: Some(match result {
                Ok(ok) => create_invite_response::Result::Ok(ok),
                Err(e) => match auth_failure(e)? {
                    AuthReply::Raft(e) => create_invite_response::Result::Error(e),
                    AuthReply::Auth(e) => create_invite_response::Result::AuthError(e),
                },
            }),
        }))
    }

    async fn redeem_invite(
        &self,
        request: Request<RedeemInviteRequest>,
    ) -> Result<Response<RedeemInviteResponse>, Status> {
        // `into_inner`, not `into_parts`: there is no verified identity
        // to look at here and there is not meant to be. Whatever the
        // caller may do comes from the invitation they present, which is
        // in the message.
        let result = self.redeem_invite_inner(request.into_inner()).await;
        Ok(Response::new(RedeemInviteResponse {
            result: Some(match result {
                Ok(seq) => redeem_invite_response::Result::Ok(RedeemInviteOk { seq }),
                Err(e) => match auth_failure(e)? {
                    AuthReply::Raft(e) => redeem_invite_response::Result::Error(e),
                    AuthReply::Auth(e) => redeem_invite_response::Result::AuthError(e),
                },
            }),
        }))
    }

    async fn list_invites(
        &self,
        request: Request<ListInvitesRequest>,
    ) -> Result<Response<ListInvitesResponse>, Status> {
        let (_, extensions, _) = request.into_parts();
        let result = self.list_invites_inner(&extensions).await;
        Ok(Response::new(ListInvitesResponse {
            result: Some(match result {
                Ok(invites) => list_invites_response::Result::Ok(InviteList { invites }),
                Err(e) => match auth_failure(e)? {
                    AuthReply::Raft(e) => list_invites_response::Result::Error(e),
                    AuthReply::Auth(e) => list_invites_response::Result::AuthError(e),
                },
            }),
        }))
    }

    async fn revoke_invite(
        &self,
        request: Request<RevokeInviteRequest>,
    ) -> Result<Response<RevokeInviteResponse>, Status> {
        let (_, extensions, req) = request.into_parts();
        let result = self.revoke_invite_inner(&extensions, req).await;
        Ok(Response::new(RevokeInviteResponse {
            result: Some(match result {
                Ok(seq) => revoke_invite_response::Result::Seq(seq),
                Err(e) => match auth_failure(e)? {
                    AuthReply::Raft(e) => revoke_invite_response::Result::Error(e),
                    AuthReply::Auth(e) => revoke_invite_response::Result::AuthError(e),
                },
            }),
        }))
    }

    async fn create_group(
        &self,
        request: Request<CreateGroupRequest>,
    ) -> Result<Response<CreateGroupResponse>, Status> {
        let (_, extensions, req) = request.into_parts();
        let result = self.create_group_inner(&extensions, req).await;
        Ok(Response::new(CreateGroupResponse {
            result: Some(match result {
                Ok(seq) => create_group_response::Result::Seq(seq),
                Err(e) => match auth_failure(e)? {
                    AuthReply::Raft(e) => create_group_response::Result::Error(e),
                    AuthReply::Auth(e) => create_group_response::Result::AuthError(e),
                },
            }),
        }))
    }

    async fn delete_group(
        &self,
        request: Request<DeleteGroupRequest>,
    ) -> Result<Response<DeleteGroupResponse>, Status> {
        let (_, extensions, req) = request.into_parts();
        let result = self.delete_group_inner(&extensions, req).await;
        Ok(Response::new(DeleteGroupResponse {
            result: Some(match result {
                Ok(seq) => delete_group_response::Result::Seq(seq),
                Err(e) => match auth_failure(e)? {
                    AuthReply::Raft(e) => delete_group_response::Result::Error(e),
                    AuthReply::Auth(e) => delete_group_response::Result::AuthError(e),
                },
            }),
        }))
    }

    async fn update_group(
        &self,
        request: Request<UpdateGroupRequest>,
    ) -> Result<Response<UpdateGroupResponse>, Status> {
        let (_, extensions, req) = request.into_parts();
        let result = self.update_group_inner(&extensions, req).await;
        Ok(Response::new(UpdateGroupResponse {
            result: Some(match result {
                Ok(seq) => update_group_response::Result::Seq(seq),
                Err(e) => match auth_failure(e)? {
                    AuthReply::Raft(e) => update_group_response::Result::Error(e),
                    AuthReply::Auth(e) => update_group_response::Result::AuthError(e),
                },
            }),
        }))
    }

    async fn get_group(
        &self,
        request: Request<GetGroupRequest>,
    ) -> Result<Response<GetGroupResponse>, Status> {
        let (_, extensions, req) = request.into_parts();
        let result = self.get_group_inner(&extensions, req).await;
        Ok(Response::new(GetGroupResponse {
            result: Some(match result {
                Ok(ok) => get_group_response::Result::Ok(ok),
                Err(e) => match auth_failure(e)? {
                    AuthReply::Raft(e) => get_group_response::Result::Error(e),
                    AuthReply::Auth(e) => get_group_response::Result::AuthError(e),
                },
            }),
        }))
    }

    async fn list_groups(
        &self,
        request: Request<ListGroupsRequest>,
    ) -> Result<Response<ListGroupsResponse>, Status> {
        let (_, extensions, _) = request.into_parts();
        let result = self.list_groups_inner(&extensions).await;
        Ok(Response::new(ListGroupsResponse {
            result: Some(match result {
                Ok(ok) => list_groups_response::Result::Ok(ok),
                Err(e) => match auth_failure(e)? {
                    AuthReply::Raft(e) => list_groups_response::Result::Error(e),
                    AuthReply::Auth(e) => list_groups_response::Result::AuthError(e),
                },
            }),
        }))
    }

    async fn set_acl(
        &self,
        request: Request<SetAclRequest>,
    ) -> Result<Response<SetAclResponse>, Status> {
        let (_, extensions, req) = request.into_parts();
        let result = self.set_acl_inner(&extensions, req).await;
        Ok(Response::new(SetAclResponse {
            result: Some(match result {
                Ok(seq) => set_acl_response::Result::Seq(seq),
                Err(e) => match auth_failure(e)? {
                    AuthReply::Raft(e) => set_acl_response::Result::Error(e),
                    AuthReply::Auth(e) => set_acl_response::Result::AuthError(e),
                },
            }),
        }))
    }

    async fn delete_acl(
        &self,
        request: Request<DeleteAclRequest>,
    ) -> Result<Response<DeleteAclResponse>, Status> {
        let (_, extensions, req) = request.into_parts();
        let result = self.delete_acl_inner(&extensions, req).await;
        Ok(Response::new(DeleteAclResponse {
            result: Some(match result {
                Ok(seq) => delete_acl_response::Result::Seq(seq),
                Err(e) => match auth_failure(e)? {
                    AuthReply::Raft(e) => delete_acl_response::Result::Error(e),
                    AuthReply::Auth(e) => delete_acl_response::Result::AuthError(e),
                },
            }),
        }))
    }

    async fn get_acl(
        &self,
        request: Request<GetAclRequest>,
    ) -> Result<Response<GetAclResponse>, Status> {
        let (_, extensions, req) = request.into_parts();
        let result = self.get_acl_inner(&extensions, req).await;
        Ok(Response::new(GetAclResponse {
            result: Some(match result {
                Ok(ok) => get_acl_response::Result::Ok(ok),
                Err(e) => match auth_failure(e)? {
                    AuthReply::Raft(e) => get_acl_response::Result::Error(e),
                    AuthReply::Auth(e) => get_acl_response::Result::AuthError(e),
                },
            }),
        }))
    }

    async fn list_acls(
        &self,
        request: Request<ListAclsRequest>,
    ) -> Result<Response<ListAclsResponse>, Status> {
        let (_, extensions, _) = request.into_parts();
        let result = self.list_acls_inner(&extensions).await;
        Ok(Response::new(ListAclsResponse {
            result: Some(match result {
                Ok(ok) => list_acls_response::Result::Ok(ok),
                Err(e) => match auth_failure(e)? {
                    AuthReply::Raft(e) => list_acls_response::Result::Error(e),
                    AuthReply::Auth(e) => list_acls_response::Result::AuthError(e),
                },
            }),
        }))
    }
}

pub struct RaftPeerService {
    peer_handle: RaftPeerHandle,
    /// What `Identify` answers with: this node's identity, and the store
    /// that says whether it has a configuration yet. `None` on a service
    /// built without them, which then can't be added to a cluster -- only
    /// tests that drive the Raft RPCs directly build one that way.
    identity: Option<(ClusterMember, Store)>,
}

impl RaftPeerService {
    pub fn new(peer_handle: RaftPeerHandle) -> Self {
        Self {
            peer_handle,
            identity: None,
        }
    }

    /// Answers `Identify` as `identity`, reporting whether `store` holds
    /// a configuration yet.
    pub fn identify_as(mut self, identity: ClusterMember, store: Store) -> Self {
        self.identity = Some((identity, store));
        self
    }
}

/// What `reassemble_append_entries` decided about a stream.
#[derive(Debug)]
enum Reassembled {
    /// A complete request, ready for the event loop to rule on properly.
    Request(Box<AppendEntriesRequest>),
    /// Refused here, before the event loop, and why. Still answered with
    /// an ordinary response rather than an error -- see below.
    Refused(&'static str),
}

/// Reads one `AppendEntries` stream back into the request it was split
/// from.
///
/// A free function rather than a method for a practical reason:
/// `tonic::Streaming` has no public constructor, so anything taking a
/// `Request<Streaming<_>>` cannot be tested at all -- and the rules
/// below (header first, the size ceiling, the declared-versus-actual
/// byte count, the stale-term fast path) are exactly the part worth
/// testing.
///
/// `current_term` is a lower bound on this node's term, used only to
/// refuse early. Everything that gets past it is still checked on the
/// event loop, which is where §5.1 actually lives.
async fn reassemble_append_entries<S>(
    mut stream: S,
    current_term: u64,
) -> Result<Reassembled, Status>
where
    S: futures_core::Stream<Item = Result<AppendEntriesChunk, Status>> + Unpin,
{
    use futures_util::StreamExt;

    let first = stream
        .next()
        .await
        .transpose()?
        .ok_or_else(|| Status::invalid_argument("empty append_entries stream"))?;
    let header = match first.part {
        Some(append_entries_chunk::Part::Header(header)) => header,
        _ => return Err(Status::invalid_argument("first chunk must be a header")),
    };

    // Two reasons to want nothing to do with the body. Both still read
    // it to the end -- see `drain` below.
    let refusal = if header.term < current_term {
        Some("the leader's term is behind this node's")
    } else if header.entries_bytes > MAX_APPEND_ENTRIES_BYTES as u64 {
        Some("the declared batch size is over this node's limit")
    } else {
        None
    };

    // Trusting the declared size to size the buffer would make a single
    // header an out-of-memory button, so the allocation is bounded by
    // what this node was willing to accept in the first place.
    let mut body = if refusal.is_some() {
        Vec::new()
    } else {
        Vec::with_capacity(header.entries_bytes.min(MAX_APPEND_ENTRIES_BYTES as u64) as usize)
    };
    let mut refusal = refusal;

    while let Some(chunk) = stream.next().await.transpose()? {
        let Some(append_entries_chunk::Part::Entries(fragment)) = chunk.part else {
            return Err(Status::invalid_argument(
                "every chunk after the header must be an entries fragment",
            ));
        };
        if refusal.is_some() {
            // Draining, not collecting: the rest of the stream costs
            // bandwidth but nothing else.
            continue;
        }
        // The header's byte count is a claim; this is the fact.
        if body.len() + fragment.len() > MAX_APPEND_ENTRIES_BYTES {
            refusal = Some("the batch is over this node's size limit");
            body = Vec::new();
            continue;
        }
        body.extend_from_slice(&fragment);
    }

    if let Some(why) = refusal {
        return Ok(Reassembled::Refused(why));
    }
    if body.len() as u64 != header.entries_bytes {
        return Err(Status::invalid_argument(
            "the stream carried a different number of bytes than its header declared",
        ));
    }

    let entries = EntryBatch::decode(&body[..])
        .map_err(|e| Status::invalid_argument(format!("undecodable entry batch: {e}")))?
        .entries;

    Ok(Reassembled::Request(Box::new(AppendEntriesRequest {
        term: header.term,
        leader_id: header.leader_id,
        prev_log_index: header.prev_log_index,
        prev_log_term: header.prev_log_term,
        entries,
        leader_commit: header.leader_commit,
        compact_seq: header.compact_seq,
    })))
}

#[tonic::async_trait]
impl RaftPeer for RaftPeerService {
    /// Reassembles the stream and hands the result inward unchanged --
    /// the term check, the log matching and the append all still happen
    /// on the event loop, exactly as they did when this was unary.
    ///
    /// A refusal is answered with an ordinary `AppendEntriesResponse`
    /// carrying this node's term, never a `Status`. That is load-bearing
    /// rather than stylistic: a response is how a leader on a stale term
    /// learns to step down (`Event::response_term`), and an error
    /// returned while the leader is still writing its request body
    /// surfaces to it as a transport failure, which it would retry
    /// forever without ever seeing the term that should have stopped it.
    async fn append_entries(
        &self,
        request: Request<tonic::Streaming<AppendEntriesChunk>>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let current_term = self.peer_handle.current_term();
        match reassemble_append_entries(request.into_inner(), current_term).await? {
            Reassembled::Request(req) => {
                let resp = self
                    .peer_handle
                    .append_entries(*req)
                    .await
                    .map_err(raft_err)?;
                Ok(Response::new(resp))
            }
            Reassembled::Refused(why) => {
                tracing::warn!("refused an append_entries stream: {why}");
                Ok(Response::new(AppendEntriesResponse {
                    // Read again rather than reusing `current_term`: the
                    // event loop may have moved on while the body was
                    // being drained, and a leader deserves the freshest
                    // term this node can give it.
                    term: self.peer_handle.current_term(),
                    success: false,
                    match_index: 0,
                    conflict_index: None,
                    conflict_term: None,
                }))
            }
        }
    }

    async fn request_vote(
        &self,
        request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        let resp = self
            .peer_handle
            .request_vote(request.into_inner())
            .await
            .map_err(raft_err)?;
        Ok(Response::new(resp))
    }

    async fn get_lease(
        &self,
        request: Request<GetLeaseRequest>,
    ) -> Result<Response<GetLeaseResponse>, Status> {
        // Deliberately synchronous and infallible -- see
        // `RaftPeerHandle::get_lease`'s doc for why this must never be
        // able to get stuck behind, e.g., a slow in-progress AppendEntries.
        Ok(Response::new(
            self.peer_handle.get_lease(request.into_inner()),
        ))
    }

    async fn identify(
        &self,
        _request: Request<IdentifyRequest>,
    ) -> Result<Response<IdentifyResponse>, Status> {
        let Some((identity, store)) = &self.identity else {
            return Err(Status::unimplemented("this node has no identity to report"));
        };
        let in_cluster = store
            .run(RaftLog::current_config)
            .await
            .map_err(storage_err)?
            .is_some();
        Ok(Response::new(IdentifyResponse {
            server_id: identity.server_id.clone(),
            address: identity.address.clone(),
            client_address: identity.client_address.clone(),
            in_cluster,
        }))
    }

    async fn install_snapshot(
        &self,
        request: Request<Streaming<SnapshotChunk>>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        let mut stream = request.into_inner();

        // First message must be the header: Raft-protocol metadata
        // (term/leader_id) for this RPC, distinct from the
        // storage-level header embedded in the reassembled bytes below
        // (which `RaftLog::install_snapshot` parses back out itself).
        let first = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("empty snapshot stream"))?;
        let header = match first.part {
            Some(snapshot_chunk::Part::Header(header)) => header,
            _ => return Err(Status::invalid_argument("first chunk must be a header")),
        };

        // The chunks go straight through to `install_snapshot` as they
        // arrive, rather than being assembled first. A snapshot is the
        // cluster's entire applied state, so "buffer it, then install
        // it" has no size at which it stays reasonable -- what is held
        // here is `CHUNK_BACKLOG` chunks, whatever the snapshot weighs.
        //
        // The install and the pump have to run at the same time: the
        // install blocks waiting for chunks that only the pump can
        // deliver, and the pump finishes only once the stream is done.
        let (tx, rx) = tokio::sync::mpsc::channel(crate::store::CHUNK_BACKLOG);
        let install = self
            .peer_handle
            .install_snapshot(header, ChunkReader::new(rx));

        let pump = async move {
            let mut giving_up = false;
            while let Some(chunk) = stream.message().await? {
                let Some(snapshot_chunk::Part::Data(data)) = chunk.part else {
                    return Err(Status::invalid_argument("expected a data chunk"));
                };
                if giving_up {
                    continue;
                }
                // A closed channel means the state machine refused this
                // snapshot -- a stale term, most likely -- and dropped
                // the reader. Keep reading the stream to its end anyway:
                // answering while the sender is still writing its
                // request body reaches it as a transport error rather
                // than the response carrying the term that should make
                // it stand down.
                if tx.send(data.to_vec()).await.is_err() {
                    giving_up = true;
                }
            }
            Ok::<(), Status>(())
        };

        let (installed, pumped) = tokio::join!(install, pump);
        pumped?;
        Ok(Response::new(installed.map_err(raft_err)?))
    }
}

mod permissions;

#[cfg(test)]
mod tests;
