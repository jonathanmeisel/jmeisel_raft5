//! gRPC services that connect the generated proto types (`pb`) to
//! `storage::RaftLog`.
//!
//! `KvService` serves clients:
//! - Anything that changes state (writes, membership changes,
//!   transactional reads) needs the leader. A follower answers
//!   `NotLeader` with its leader hint and does not forward (Raft §6.2),
//!   so the client retries on the leader and only one node does the
//!   work.
//! - A stale read asks for no guarantee, so any node serves it.
//! - `Bootstrap` targets a node that has no leader at all.
//!
//! Every write goes through a transaction. `write` buffers a key and
//! appends nothing; `commit` turns the whole buffer into one log entry.
//! A single-key write is a transaction of one key, so there is only one
//! write path with one set of locking and failure rules. The lock table
//! in `crate::txn` protects a transaction's reads, which makes a whole
//! read-modify-write atomic, with no race between two round trips.
//!
//! `commit` and the transactional reads (`transactional_multi_read`,
//! `transactional_scan`) first make sure of two things:
//! - The leader's `latest` is complete (`LeaderHandle::await_caught_up`).
//!   `latest` shows only applied entries, and a new leader may still be
//!   applying the log tail it inherited.
//! - The leader still holds a quorum of leases
//!   (`LeaderHandle::has_read_quorum`). Without it, the node may have
//!   lost leadership without knowing.
//!
//! `RaftPeerService` serves the other nodes, on a separate port because
//! its network exposure is different. It forwards every call to the live
//! `raft::RaftPeerHandle`, and the current role decides the answer.
//! `install_snapshot` reassembles the stream, but like every call here it
//! never touches `storage::RaftLog` directly: the term check and the
//! install happen behind `RaftPeerHandle`.

use prost::Message;
use tonic::transport::ClientTlsConfig;
use tonic::{Request, Response, Status, Streaming};
use tracing::Instrument;

use crate::auth::{AuthConfig, AuthContext, InviteRecord, InviteSecret, UserRecord};
use crate::keyspace::is_reserved;
use crate::pb::raft5::client::kv_server::Kv;
use crate::pb::raft5::client::{
    AddServerRequest, AuthError as PbAuthError, BackupChunk, BackupRequest,
    BootstrapFromBackupChunk, BootstrapRequest, CommitTransactionRequest,
    CommitTransactionResponse, ConfigChangeResponse, CreateInviteOk, CreateInviteRequest,
    CreateInviteResponse, CreateSnapshotRequest, CreateSnapshotResponse, CreateTransactionRequest,
    CreateTransactionResponse, CreateUserRequest, CreateUserResponse, DeleteUserRequest,
    DeleteUserResponse, DropTransactionRequest, DropTransactionResponse, GetConfigRequest,
    GetConfigResponse, GetKeysOk, GetKeysRequest, GetKeysResponse, Invite as PbInvite, KeyVersion, InviteList, ListInvitesRequest, ListInvitesResponse,
    ListSnapshotsRequest, ListSnapshotsResponse, LoginOk, LoginRequest, LoginResponse, NotLeader,
    RaftError as PbRaftError, ReadOk, ReadRequest, ReadResponse, RedeemInviteOk,
    RedeemInviteRequest, RedeemInviteResponse, RemoveServerRequest, RemoveSnapshotRequest,
    SetHeartbeatRequest,
    RemoveSnapshotResponse, RevokeInviteRequest, RevokeInviteResponse, Snapshot as PbSnapshot,
    SnapshotList, TransactionAborted, WriteRequest, WriteResponse, auth_error,
    commit_transaction_response, config_change_response, create_invite_response,
    create_snapshot_response, create_transaction_response, create_user_response,
    delete_user_response, drop_transaction_response, get_keys_request, get_keys_response,
    list_invites_response, list_snapshots_response,
    backup_chunk, bootstrap_from_backup_chunk, login_response, raft_error, read_request,
    read_response, redeem_invite_response,
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
use crate::pb::raft5::client::{
    GetBackupOperatorsRequest, GetBackupOperatorsResponse, SetBackupOperatorsRequest,
    SetBackupOperatorsResponse, get_backup_operators_response, set_backup_operators_response,
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
use crate::storage::{
    BackupExport, BackupSource, Checkpoint, Fetch, MAX_LIVE_CHECKPOINTS, MultiRead, Mutation,
    ReadPoint, ReadRow, checkpoint_key,
};
use crate::storage::{RaftLog, StorageError};
use crate::store::{ChunkReader, ChunkWriter};
use crate::store::Store;
use crate::txn::{AbortReason, BufferedKeys, Locks, Refused, TxnId};

/// What `RaftLog::read_many` found, in wire form.
///
/// A key that was never written and a deleted key both read as "no
/// value". Seq 0 is never assigned (sequencing starts at 1), so it marks
/// "no value".
///
/// `buffered` holds a transaction's own writes, by position, laid over
/// what storage found. It is empty outside a transaction. A buffered
/// value replaces the committed value but not its seq: a buffered write
/// gets a seq only at commit.
fn read_ok(
    as_of: u64,
    rows: Vec<ReadRow>,
    buffered: Vec<Option<Option<Vec<u8>>>>,
) -> ReadOk {
    let mut buffered = buffered.into_iter();
    let entries = rows
        .into_iter()
        .map(|row| {
            let (seq, committed) = row.unwrap_or((0, None));
            let value = match buffered.next().flatten() {
                Some(mine) => mine,
                None => committed,
            };
            KeyVersion { seq, value }
        })
        .collect();
    ReadOk { as_of, entries }
}

/// One scan batch, in wire form.
fn get_keys_ok(batch: ScanBatch) -> GetKeysOk {
    GetKeysOk {
        keys: batch.keys,
        has_more: batch.has_more,
        next_start_key: batch.next_start_key.unwrap_or_default(),
    }
}

/// How many keys `GetKeys` returns when the caller does not say.
const DEFAULT_MAX_KEYS: u32 = 10;

/// The most keys one `GetKeys` returns.
///
/// In a transaction, this also caps how many read locks one call takes.
/// Every key a scan returns stays locked until the transaction ends, so
/// an unbounded scan would let one caller block any part of the
/// keyspace. Outside a transaction, it only bounds the work and the
/// response size.
const MAX_KEYS_PER_SCAN: u32 = 1024;

/// How long an account write may hold its key.
///
/// Short, because no human is involved: the whole read-modify-write runs
/// on the server within one raft round trip. This only limits how long a
/// stuck admin call can block an account.
const ADMIN_TXN_TTL: std::time::Duration = std::time::Duration::from_secs(5);

/// The lifetime of an invitation when the issuer does not give one.
///
/// One day: long enough to reach someone in another time zone and be
/// read the next morning, short enough that a forgotten invite cannot be
/// used a month later.
const DEFAULT_INVITE_TTL: std::time::Duration = std::time::Duration::from_secs(24 * 60 * 60);

/// The longest lifetime this cluster gives an invitation.
///
/// An invitation is a credential that creates a credential, and this is
/// the only limit on how long a leaked one stays dangerous. A week
/// covers "I am on holiday until Monday". For longer, create an account.
const MAX_INVITE_TTL: std::time::Duration = std::time::Duration::from_secs(7 * 24 * 60 * 60);

/// The most invitations that may be open at once.
///
/// This does not protect the store (a record is about 100 bytes, and an
/// admin who wants to fill the disk can use `CreateUser`). It bounds the
/// listing: `ListInvites` reads every open record in one unpaged call.
/// The next `CreateInvite` sweeps lapsed invitations instead of counting
/// them, so a cluster with a steady trickle never reaches this limit.
const MAX_LIVE_INVITES: usize = 64;

/// How long `Bootstrap` waits for this node to become leader before it
/// gives up on writing the startup administrator.
///
/// Generous: a single-node cluster elects itself without opposition, so
/// this covers one election timeout and one no-op commit.
const BOOTSTRAP_ADMIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
const BOOTSTRAP_ADMIN_RETRY: std::time::Duration = std::time::Duration::from_millis(20);

/// A transaction refusal, in client-protocol form.
///
/// Every variant except `TooMuch` is a normal answer, like a write
/// conflict: it tells the caller something it can act on. `TooMuch` is a
/// real refusal to serve (the request can never work), so it stays a
/// `Status`.
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

/// Why a transactional request did not happen: the transaction's own
/// reasons, plus all the state machine can refuse.
enum TxnError {
    Refused(Refused),
    Raft(RaftError),
    /// The commit reached the log, and then this node stopped leading. Kept
    /// separate because a client must not retry it: see
    /// `raft::AppendError`.
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
    /// A problem with the request or the account. Sent to the caller in
    /// band, as an `AuthError`.
    Refused(auth_error::Kind, String),
    /// The cluster could not serve the request.
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

    /// The same answer for "no such account" and "wrong password". Two
    /// answers would confirm which usernames exist, and a username is half
    /// of a credential.
    fn bad_credentials() -> Self {
        Self::refused(
            auth_error::Kind::BadCredentials,
            "the username or password was wrong",
        )
    }

    /// An invitation that cannot be used. `why` is for a human. The holder
    /// got the secret from an admin on purpose, so a clear reason ("it
    /// lapsed on Tuesday") costs nothing and saves a support call.
    fn bad_invite(why: &'static str) -> Self {
        Self::refused(auth_error::Kind::BadInvite, why)
    }
}

/// Which of the two error forms a response carries.
enum AuthReply {
    Raft(PbRaftError),
    Auth(PbAuthError),
}

/// An `AuthFailure`, as the answer to a membership change.
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

/// A transaction id from the wire. Ids have a fixed width, so any other
/// length is a malformed request, not a missing transaction.
fn txn_id(bytes: &[u8]) -> Result<TxnId, Status> {
    bytes
        .try_into()
        .map_err(|_| Status::invalid_argument("transaction_id must be 16 bytes"))
}

/// Refuse a client request for a key the cluster reserves for itself
/// (`crate::keyspace`).
///
/// `invalid_argument`, not `permission_denied`: no credential reaches
/// these keys through this API, so the key is not addressable at all.
/// The client also treats `invalid_argument` as permanent, so the caller
/// learns at once instead of retrying until its deadline.
fn reject_reserved(key: &[u8]) -> Result<(), Status> {
    if is_reserved(key) {
        return Err(Status::invalid_argument(
            "that key is reserved for the cluster's own use",
        ));
    }
    Ok(())
}

/// An invitation in wire form: all but the secret, which no longer
/// exists anywhere once anyone can ask for this.
fn pb_invite(invite_id: String, record: InviteRecord) -> PbInvite {
    PbInvite {
        invite_id,
        issued_by: record.issued_by,
        is_admin: record.is_admin,
        username: record.username,
        expires_unix_ms: record.expires_unix_ms,
    }
}

/// How long an invitation with `ttl_ms` should last.
///
/// A value over the limit is refused, not shortened, as for transaction
/// lifetimes: an issuer told "yes" would believe the invite lasts a
/// month and find out only when the recipient cannot use it.
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

/// Whether `record` can be used now, for `username`.
///
/// Both failures answer `BAD_INVITE` with no more detail. To the holder,
/// a lapsed invite and one meant for someone else need the same action:
/// ask the issuer again.
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

/// Who sent a request, for records that store it.
///
/// Sound only after `require_admin` has run on a node with accounts:
/// that call makes sure an identity is present. The error is the same as
/// that call's, so a caller that gets the order wrong fails closed.
fn caller(extensions: &tonic::Extensions) -> Result<String, AuthFailure> {
    extensions
        .get::<AuthContext>()
        .map(|context| context.username.clone())
        .ok_or_else(|| Status::unauthenticated("this request carried no verified identity").into())
}

/// A checkpoint in wire form.
fn pb_snapshot(cp: Checkpoint) -> PbSnapshot {
    PbSnapshot {
        snapshot_id: cp.id,
        seq: cp.seq,
        created_unix_ms: cp.created_unix_ms,
    }
}

/// The longest allowed snapshot id.
const MAX_SNAPSHOT_ID_LEN: usize = 128;

/// Refuse an empty snapshot id in a read. An empty id means "no
/// snapshot", which is an unset `consistency`, not a snapshot read.
fn require_snapshot_id(id: &[u8]) -> Result<(), Status> {
    if id.is_empty() {
        return Err(Status::invalid_argument(
            "a snapshot id cannot be empty; leave `consistency` unset for a current read",
        ));
    }
    Ok(())
}

/// A read that names a snapshot this node does not have.
fn no_such_snapshot() -> Status {
    Status::not_found("no such snapshot; it may have been removed, or not reached this node yet")
}

/// Check that `checkpoint_key` can safely build a key from `id`.
///
/// The character set matters more than the length, for the reason given
/// in `keyspace::validate_username`: the id is appended to a fixed prefix
/// with no separator or escaping, so any byte that could make two ids
/// collide, or escape the prefix, must be refused here. The set adds `:`
/// and `+` to a username's, so that an RFC 3339 timestamp (the client's
/// default id) is valid.
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

/// An `AuthError` in client-protocol form.
///
/// Sent in band next to `RaftError`, not as a `Status`, like a lock
/// conflict: each is an answer, and a client that retried it would get
/// the same answer until its deadline. A bad token is the opposite case
/// and stays a `Status`: see `crate::auth`.
fn auth_err(kind: auth_error::Kind, detail: impl Into<String>) -> PbAuthError {
    PbAuthError {
        kind: kind as i32,
        detail: detail.into(),
    }
}

/// The answer to every auth RPC on a node started without a secret.
const AUTH_DISABLED: &str =
    "this node was started without a signing secret, so it keeps no accounts";

fn storage_err(e: StorageError) -> Status {
    Status::internal(e.to_string())
}

fn raft_err(e: RaftError) -> Status {
    match e {
        // Only the peer service reaches this, where a structured client error
        // has nowhere to go.
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
        RaftError::BootstrapRefused(why) => Status::failed_precondition(why),
        RaftError::BadBackup(why) => Status::invalid_argument(why),
    }
}

/// `RaftError` as the structured error of the client protocol, so that a
/// client finds the real leader from the response, with no separate
/// discovery RPC. Only `NotLeader` has something useful to point at;
/// other errors are plain strings. Real internal failures (storage, a
/// dead event loop) stay `Status` errors, not normal answers.
fn client_err(e: RaftError) -> Result<PbRaftError, Status> {
    Ok(match e {
        RaftError::NotLeader { leader_hint } => PbRaftError {
            kind: Some(raft_error::Kind::NotLeader(NotLeader { leader_hint })),
        },
        RaftError::ConfigChangeInProgress
        | RaftError::NotCaughtUp
        | RaftError::AlreadyInitialized
        | RaftError::BootstrapRefused(_)
        | RaftError::BadBackup(_)
        | RaftError::InvalidConfigChange(_) => PbRaftError {
            kind: Some(raft_error::Kind::Unavailable(
                raft_err(e).message().to_string(),
            )),
        },
        e => return Err(raft_err(e)),
    })
}

/// A refusal wrapped as `propose` wraps one, for callers (the leader
/// check in `add_server`) that must answer before they reach `propose`.
fn not_leader_response(e: RaftError) -> Result<Response<ConfigChangeResponse>, Status> {
    Ok(Response::new(ConfigChangeResponse {
        result: Some(config_change_response::Result::Error(client_err(e)?)),
    }))
}

/// This node's leader handle, or the error to send to the client. Every
/// write and membership change goes through here. A follower answers
/// `NotLeader` with its hint and does not forward, so the client retries
/// on the leader (Raft §6.2) and only one node does the work.
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
/// A struct, not a tuple, because `has_more` and `next_start_key` are
/// separate facts: a batch can be short or empty and still have keys
/// after it. See the budget in `RaftLog::scan_as_of`.
struct ScanBatch {
    keys: Vec<Vec<u8>>,
    has_more: bool,
    /// Where to resume. Set whenever `has_more` is set.
    next_start_key: Option<Vec<u8>>,
}

pub struct KvService {
    store: Store,
    /// This node's own identity, for `Bootstrap`. The new configuration
    /// names this node, and only this node knows its name and peer address.
    identity: ClusterMember,
    client_handle: ClientHandle,
    /// Who holds which locks, and every open transaction. See `crate::txn`.
    locks: Locks,
    /// How tokens are signed and checked, or `None` on a node started
    /// without a secret, which leaves this API open. Every authorization
    /// check reads "allowed" when this is `None`, so the open case is
    /// decided in one place, not in each handler. See `crate::auth`.
    auth: Option<AuthConfig>,
    /// Every group and ACL, as of the last permission change this node
    /// applied. Shared by the clones tonic makes of this service, and
    /// rebuilt only when the permission epoch moves. See
    /// `grpc::permissions`.
    perms: permissions::PermsCache,
    /// How `AddServer` dials the server it adds: over the peer mTLS, or
    /// `None` for plaintext.
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

    /// Dial servers that `AddServer` adds over mTLS, as peers dial each
    /// other. This must match this node's own peer connections: a server
    /// this node cannot reach as a peer would never finish being added.
    pub fn with_peer_tls(mut self, tls: ClientTlsConfig) -> Self {
        self.peer_tls = Some(tls);
        self
    }

    /// The lock table, for the sweeper that `serve` starts.
    pub fn locks(&self) -> Locks {
        self.locks.clone()
    }

    /// This node's leader handle, after telling the lock table which term
    /// it answers in.
    ///
    /// Every request that needs a leader comes here, not to
    /// `require_leader`, so the table sees a term change at the first
    /// request after it, not at the next sweep. A transaction cannot outlive
    /// its leader, and the sooner the table acts on that, the sooner its
    /// keys are writable again.
    fn leader(&self) -> Result<LeaderHandle, RaftError> {
        let role = self.client_handle.role();
        let leader = require_leader(&role);
        self.locks
            .observe_term(leader.as_ref().ok().map(LeaderHandle::term));
        leader
    }

    /// A transaction's leader: this node, but only if it is still the leader
    /// the transaction was created under.
    ///
    /// A second line of defense. `leader()` already discards all
    /// transactions when the term changes, so an old transaction is normally
    /// gone. This check catches the case where this same request is the one
    /// that sees the change.
    fn transaction_leader(&self, id: TxnId) -> Result<LeaderHandle, TxnError> {
        let leader = self.leader()?;
        if self.locks.term_of(id)? != leader.term() {
            return Err(Refused::LeaderChanged.into());
        }
        Ok(leader)
    }

    /// A transactional read of many keys: lock all of them, then answer all
    /// from one storage snapshot.
    ///
    /// Every key is locked before any is read, including keys that the value
    /// budget leaves out of the answer. So a reissue for the rest sees
    /// exactly what this call would have. Nothing it names can change until
    /// the transaction ends.
    ///
    /// The locks, not the snapshot, make the keys agree. The snapshot costs
    /// nothing and makes `as_of` a point at which every committed value here
    /// is true.
    async fn transactional_multi_read(
        &self,
        id: TxnId,
        keys: Vec<Vec<u8>>,
        versions_only: bool,
    ) -> Result<ReadOk, TxnError> {
        self.transaction_read_quorum(id).await?;

        let buffered = self.locks.read_many(id, &keys, !versions_only)?;

        // The transaction supplies its buffered values, so storage does not
        // read the committed values they replace. They still count toward the
        // budget, because the budget is about the response.
        let fetches = keys
            .into_iter()
            .zip(&buffered)
            .map(|(key, mine)| {
                let fetch = match (versions_only, mine) {
                    (true, _) => Fetch::VersionOnly,
                    (false, Some(value)) => Fetch::Charge(value.as_ref().map_or(0, Vec::len)),
                    (false, None) => Fetch::Value,
                };
                (key, fetch)
            })
            .collect();
        match self.read_store(fetches, ReadPoint::Latest).await {
            Ok(MultiRead::Ok { as_of, rows }) => Ok(read_ok(as_of, rows, buffered)),
            // `Latest` names no checkpoint, so none can be missing.
            Ok(MultiRead::NoSuchCheckpoint) => {
                Err(TxnError::Internal(Status::internal("a latest read found no checkpoint")))
            }
            Err(e) => Err(TxnError::Raft(RaftError::Storage(e))),
        }
    }

    /// One key in a transaction, with its value. For the server's own
    /// read-modify-writes, which read one record at a time.
    async fn transactional_read(&self, id: TxnId, key: &[u8]) -> Result<KeyVersion, TxnError> {
        let mut ok = self
            .transactional_multi_read(id, vec![key.to_vec()], false)
            .await?;
        // One key always fits: the budget always keeps the first.
        Ok(ok.entries.remove(0))
    }

    /// `RaftLog::read_many` on the blocking pool, with the response value
    /// budget.
    async fn read_store(
        &self,
        keys: Vec<(Vec<u8>, Fetch)>,
        point: ReadPoint,
    ) -> Result<MultiRead, StorageError> {
        self.store
            .run(move |log| log.read_many(&keys, point, raft5_shared::MAX_READ_VALUE_BYTES))
            .await
    }

    /// A read of many keys outside a transaction, at `point`. This is the
    /// storage step of every non-transactional mode, after the caller has
    /// proved what its mode needs.
    async fn multi_read(
        &self,
        keys: Vec<Vec<u8>>,
        versions_only: bool,
        point: ReadPoint,
    ) -> Result<ReadOk, Status> {
        let fetch = if versions_only {
            Fetch::VersionOnly
        } else {
            Fetch::Value
        };
        let keys = keys.into_iter().map(|key| (key, fetch)).collect();
        match self.read_store(keys, point).await.map_err(storage_err)? {
            MultiRead::Ok { as_of, rows } => Ok(read_ok(as_of, rows, Vec::new())),
            MultiRead::NoSuchCheckpoint => Err(no_such_snapshot()),
        }
    }

    /// What a transaction must prove before it answers a read: this node
    /// leads the transaction's term, its `latest` is complete, and it still
    /// holds a quorum of leases.
    ///
    /// These checks come before the caller takes any lock: a leader that is
    /// still catching up must not hold keys that nobody can write meanwhile.
    async fn transaction_read_quorum(&self, id: TxnId) -> Result<(), TxnError> {
        let leader = self.transaction_leader(id)?;
        leader.await_caught_up().await?;
        if !leader.has_read_quorum() {
            return Err(RaftError::NotLeader { leader_hint: None }.into());
        }
        Ok(())
    }

    /// One pass of a key scan: up to `limit` keys at or after `start`, with
    /// `buffered` (the transaction's own uncommitted changes, or empty)
    /// merged in.
    ///
    /// Returns the keys and whether the scan stopped because the batch was
    /// full, which tells the caller that another batch exists.
    async fn scan(
        &self,
        start: Vec<u8>,
        as_of: u64,
        limit: usize,
        buffered: &BufferedKeys,
        filter: &Option<permissions::ScanFilter>,
    ) -> Result<ScanBatch, StorageError> {
        // A key this transaction deleted does not exist for it, so the scan
        // skips it. Filtering after the scan would shorten the batch for a
        // reason the caller cannot see.
        let deleted: std::collections::HashSet<Vec<u8>> = buffered
            .iter()
            .filter(|(_, is_delete)| *is_delete)
            .map(|(key, _)| key.clone())
            .collect();

        // Ask for one more than needed, so "another batch exists" is something
        // the scan saw, not something the caller guesses from a count. See
        // `GetKeysOk.has_more`.
        let want = limit.saturating_add(1);
        let (committed, budget_cursor) = {
            let start = start.clone();
            let filter = filter.clone();
            self.store
                .run(move |log| {
                    // Reserved keys are skipped inside the walk, not filtered from the
                    // result: a skipped key does not count against `want`, so hiding the
                    // reserved range does not shorten the batch. Both the transactional
                    // and the plain scan come here, so this covers all of `GetKeys`.
                    //
                    // Keys hidden by an ACL are skipped the same way. The filter is built
                    // before the walk, because this closure runs on the blocking pool and
                    // cannot read the store. That is also why a walk that hides almost
                    // everything needs the budget, or it would walk the whole keyspace.
                    log.scan_as_of(&start, as_of, want, permissions::SCAN_ROW_BUDGET, |key| {
                        deleted.contains(key)
                            || is_reserved(key)
                            || filter.as_ref().is_some_and(|f| f.hides(key))
                    })
                })
                .await?
        };

        // Buffered writes can add keys that the store does not have. Merging
        // can only add keys, so the first `limit` of the union equals the
        // result of merging the two full streams.
        let mut keys: Vec<Vec<u8>> = committed;
        keys.extend(
            buffered
                .iter()
                .filter(|(key, is_delete)| {
                    // A second line of defense: `write` refuses reserved keys for client
                    // transactions, so none should be buffered here. The server's own
                    // transactions do buffer them, but never scan. The check costs one
                    // comparison.
                    !*is_delete && key.as_slice() >= start.as_slice() && !is_reserved(key)
                })
                .map(|(key, _)| key.clone()),
        );
        keys.sort_unstable();
        keys.dedup();

        // Two reasons to have more: the batch filled, or the walk ran out of
        // budget first. A full batch takes its cursor from the last key kept.
        // A budget stop may have no key at all, so it uses the key where the
        // walk stopped.
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

    /// A transactional key scan: find the keys, lock them, then look again.
    ///
    /// A single-key read locks first and reads after. A scan cannot: it does
    /// not know which keys to lock until it looks. So it:
    /// - looks,
    /// - locks what it found,
    /// - looks again.
    ///
    /// After the locks, nobody else can change those keys, so the second
    /// answer is one the transaction can rely on. A key that disappeared in
    /// between is gone (we hold its lock and it is not there). A key that
    /// appeared is a phantom and is not returned: the locks are per key, and
    /// the range is not locked.
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
        self.locks.read_many(id, &candidates, false)?;

        let batch = self
            .scan(start, u64::MAX, limit, &buffered, filter)
            .await
            .map_err(|e| TxnError::Raft(RaftError::Storage(e)))?;

        // Return only keys the locks cover. Other keys from the second pass
        // appeared after this transaction looked, and returning them unlocked
        // is exactly what the lock table prevents.
        let locked: std::collections::HashSet<&[u8]> =
            candidates.iter().map(Vec::as_slice).collect();
        let keys = batch
            .keys
            .into_iter()
            .filter(|key| locked.contains(key.as_slice()))
            .collect();
        // The cursor comes from the second pass, because that pass gives the
        // answer. Filtering to locked keys can shorten the batch, which is
        // another reason to send the cursor instead of deriving it from the
        // last key.
        Ok(ScanBatch { keys, ..batch })
    }

    /// Commit all that a transaction buffered, as one entry.
    ///
    /// The append runs in its own task, which owns the transaction's locks.
    /// So the locks are released when the entry resolves, not when this call
    /// returns. Otherwise a client that disconnects mid-commit would leave
    /// the transaction half done, and a committing transaction never
    /// expires: its keys would stay blocked for the rest of the term.
    async fn commit(&self, id: TxnId) -> Result<u64, TxnError> {
        let leader = self.transaction_leader(id)?;
        let (writes, guard) = self.locks.begin_commit(id, leader.term())?;

        if writes.is_empty() {
            // There is nothing to append, so no raft round trip confirms the point
            // in time this transaction read at. Term equality alone is not enough:
            // a partitioned leader still believes it holds its term. A lease quorum
            // is enough: nobody can get one in term T after a majority has moved to
            // a later term.
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

        // A span does not cross `tokio::spawn` (or `spawn_blocking`) by
        // itself. Without `.instrument(..)`, the `append_writes` span would
        // show up as a separate root, not under this RPC.
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

    /// The shared body of `add_server` and `remove_server`: the same
    /// operation with a different `ConfigChange`. Both answer `NotLeader`
    /// instead of forwarding (Raft §6.2).
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

    /// What a linearizable read without locks must prove: this node leads,
    /// its `latest` is complete, and it still holds a quorum of leases.
    ///
    /// The same proof as `transaction_read_quorum`, without a transaction.
    /// Nothing must stay held after the answer, so nothing is locked.
    async fn read_quorum(&self) -> Result<(), RaftError> {
        let leader = self.leader()?;
        leader.await_caught_up().await?;
        if !leader.has_read_quorum() {
            return Err(RaftError::NotLeader { leader_hint: None });
        }
        Ok(())
    }

    /// A read that includes everything committed anywhere in the cluster,
    /// with no lock.
    async fn linearizable_read(&self, key: Vec<u8>) -> Result<Option<(u64, Vec<u8>)>, RaftError> {
        self.read_quorum().await?;
        self.store
            .run(move |log| log.get_latest(&key))
            .await
            .map_err(RaftError::Storage)
    }

    /// `linearizable_read`, for a caller that wants only the value.
    async fn linearizable_get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, RaftError> {
        Ok(self.linearizable_read(key).await?.map(|(_, value)| value))
    }

    /// Find the seq that a snapshot id reads at.
    ///
    /// A local read on purpose: if this node has the record, it has applied
    /// everything the snapshot needs (see `storage::checkpoint`). A leader
    /// could add nothing.
    async fn checkpoint_seq(&self, id: &[u8]) -> Result<u64, Status> {
        require_snapshot_id(id)?;
        let owned = id.to_vec();
        let found = self
            .store
            .run(move |log| log.checkpoint_seq(&owned))
            .await
            .map_err(storage_err)?;
        found.ok_or_else(no_such_snapshot)
    }

    /// Create a checkpoint: write its record once, and return the seq of
    /// that write, which is the snapshot's seq.
    ///
    /// Same shape as `put_user`, for the same reason: the read and the write
    /// must be one transaction. Otherwise two callers could both find the id
    /// absent and both create it, and the second record would silently move
    /// the snapshot forward in time.
    async fn create_checkpoint_inner(
        &self,
        extensions: &tonic::Extensions,
        req: CreateSnapshotRequest,
    ) -> Result<Checkpoint, AuthFailure> {
        self.require_snapshot_rights(extensions).await?;
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
        self.require_snapshot_rights(extensions).await?;
        validate_snapshot_id(&req.snapshot_id)?;

        let key = checkpoint_key(&req.snapshot_id);
        self.admin_txn(async |id| self.write_checkpoint(id, &key, None).await)
            .await
    }

    /// `write_user` for checkpoints: `Some(created)` creates, `None`
    /// removes. Each is refused if the record is not in the state it needs.
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

    /// The account record for `username`, read linearizably.
    async fn load_user(&self, username: &str) -> Result<Option<UserRecord>, AuthFailure> {
        match self
            .linearizable_get(crate::keyspace::user_key(username))
            .await?
        {
            Some(bytes) => Ok(Some(UserRecord::from_bytes(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Write an account record, or remove it when `record` is `None`, in its
    /// own transaction. See [`admin_txn`] for why it uses a transaction and
    /// not a plain append.
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

    /// Run `f` in a server-owned transaction on the leader, and release what
    /// it held however it ends.
    ///
    /// Every admin write is a read-modify-write: whether the record exists
    /// decides if the call is allowed. So the check and the write must be
    /// atomic against another admin doing the same thing. That is why these
    /// use a normal transaction, and why reserved keys are refused at the
    /// RPC boundary and not in `Locks`: the lock table cannot tell this
    /// write from a client's, and must not.
    ///
    /// Clients cannot contend for these keys, because `write` refuses them.
    /// Two admin calls that race resolve like any two transactions, and the
    /// loser learns that its transaction aborted.
    ///
    /// Clients and the server do share the open-transaction table. Enough
    /// client transactions to fill `MAX_OPEN_TRANSACTIONS` also refuse this
    /// one, as `resource_exhausted`. We accept that: an admin retry is a
    /// smaller problem than a second allocation rule.
    ///
    /// The discard is explicit, not a `Drop` guard. Discarding a committed
    /// transaction does nothing, and the commit path gives its locks to the
    /// append task, so an early release cannot cut anything short.
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

    /// The body of `write`: two checks, then the buffer.
    ///
    /// The ACL check happens when the key is named, not at commit. So the
    /// caller learns which key it may not write, at the moment it names it,
    /// and does not lose a whole transaction later to an error that cannot
    /// say which key. The cost: a permission removed between here and the
    /// commit does not stop this write. A linearizable read has the same
    /// window, because nothing is held after an answer.
    async fn write_inner(
        &self,
        extensions: &tonic::Extensions,
        req: WriteRequest,
    ) -> Result<(), AuthFailure> {
        let id = txn_id(&req.transaction_id)?;
        // The guard against privilege escalation. A client that could buffer a
        // write to a reserved key could write its own account record with any
        // `is_admin` value.
        reject_reserved(&req.key)?;
        // A delete is a write with no value, so this covers both.
        self.require_access(extensions, &req.key, Right::Write)
            .await?;

        // In memory only: nothing reaches the log until `commit`. The leader
        // handle is fetched only for the term check in `transaction_leader`:
        // only the leader that created a transaction can add to it.
        self.transaction_leader(id)?;
        self.locks.buffer_write(id, &req.key, req.value)?;
        Ok(())
    }

    /// Check that the sender of this request is an administrator.
    ///
    /// The token says who they are; the store says what they may do. The
    /// store is read on every call, not trusted from a token claim, because
    /// a right inside a token cannot be withdrawn until the token expires.
    /// These calls are rare and already pay a raft round trip, so the read
    /// is cheap by comparison.
    async fn require_admin(&self, extensions: &tonic::Extensions) -> Result<(), AuthFailure> {
        // A node with no secret keeps no accounts, so there is no admin and
        // nothing to check. This is the only place that decides the open case,
        // so no caller has to remember it, and one line says what an
        // unauthenticated cluster allows.
        if self.auth.is_none() {
            return Ok(());
        }
        // Missing only if the auth layer did not run. That is a configuration
        // error, not a forged request, but the safe response is the same.
        let Some(context) = extensions.get::<AuthContext>() else {
            return Err(
                Status::unauthenticated("this request carried no verified identity").into(),
            );
        };
        match self.load_user(&context.username).await? {
            Some(user) if user.is_admin => Ok(()),
            // Either not an admin, or an account deleted after its token was
            // issued. Neither may proceed, and the caller is not told which.
            // The message does not name the operation: this guards account
            // management and membership changes, and a name would be wrong for one
            // of them.
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
        // A name that can never be stored cannot match an account. Refusing it
        // here keeps `user_key` to the inputs it is safe for.
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

    /// The hash to check a login against: the stored account's hash, or, on
    /// a node that is not yet in a cluster, the startup password's hash.
    ///
    /// The fallback breaks the circle at the start. A new node has no
    /// accounts and no leader to read them from, but `Bootstrap` needs a
    /// token like every other call, so the operator must be able to log in
    /// against something. The fallback depends on the node having no
    /// configuration, not on there being no accounts: otherwise deleting the
    /// last account would silently re-enable the startup password. Once the
    /// node is in a cluster, the fallback never applies again.
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

    /// Check that the caller bootstrapping this node is the administrator it
    /// was started with.
    ///
    /// `require_admin` cannot work here: it reads the store, and a node with
    /// no cluster has no leader to read from. That is why the startup
    /// administrator exists. This check against the configured identity
    /// also stops a token from another cluster with the same secret from
    /// bootstrapping this node.
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

    /// Check a bootstrap request, and build the single-node configuration it
    /// asks for. Shared by `Bootstrap` and `BootstrapFromBackup`.
    ///
    /// - `server_id`, if set, must name this node.
    /// - The heartbeat must be in range; 0 means the default.
    /// - The caller must be the startup administrator.
    fn bootstrap_config(
        &self,
        extensions: &tonic::Extensions,
        req: &BootstrapRequest,
    ) -> Result<ClusterConfig, Status> {
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
        self.require_startup_admin(extensions)?;

        // A cluster of one: the node is its only voter, so it wins the next
        // election unopposed and can then serve AddServer to grow.
        Ok(ClusterConfig {
            members: vec![self.identity.clone()],
            heartbeat_millis,
        })
    }

    /// Check that the caller may take a backup: an administrator or a backup
    /// operator, by this node's own records.
    ///
    /// A local check, like the data path's, so a follower can serve backups.
    /// The cost: a caller whose right was withdrawn a moment ago can still take
    /// a backup from a follower that has not applied the change yet. A node
    /// with no secret keeps no accounts, so it lets anyone back up.
    async fn require_backup_rights(&self, extensions: &tonic::Extensions) -> Result<(), AuthFailure> {
        if self.auth.is_none() {
            return Ok(());
        }
        let who = caller(extensions)?;
        if self.is_admin_locally(&who).await? || self.is_backup_operator(extensions).await? {
            return Ok(());
        }
        Err(AuthFailure::refused(
            auth_error::Kind::NotAdmin,
            "only an administrator or a backup operator may take a backup",
        ))
    }

    /// Check that the caller may create or remove a snapshot: an
    /// administrator or a backup operator.
    ///
    /// The same people who may take a backup, because a snapshot exists to
    /// back up from. The admin check is the authoritative one `require_admin`
    /// makes; these calls write through the leader anyway.
    async fn require_snapshot_rights(
        &self,
        extensions: &tonic::Extensions,
    ) -> Result<(), AuthFailure> {
        if self.is_cluster_admin(extensions).await? || self.is_backup_operator(extensions).await? {
            return Ok(());
        }
        Err(AuthFailure::refused(
            auth_error::Kind::NotAdmin,
            "only an administrator or a backup operator may manage snapshots",
        ))
    }

    /// Write the startup administrator into the log, once this node is
    /// leader.
    ///
    /// `client_handle.bootstrap` only installs the configuration. The node
    /// must still win its election (unopposed) and commit its no-op before
    /// it can append. So this waits, and `Bootstrap` answers only after it:
    /// "ok" means an administrator exists, not that one will exist soon.
    ///
    /// Safe to repeat: a record that already exists is the goal.
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
                // The configuration is installed and this node is a cluster, but nobody
                // can administer it: the startup password stops working once a
                // configuration exists. Say so plainly, because the fix is to delete
                // this node's directory and bootstrap again.
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

    /// Every open invitation, split into the ones that can still be used and
    /// the ids of the ones that cannot.
    ///
    /// A local read, not a linearizable one. Both callers accept that:
    /// - `ListInvites` is a listing, stale like any read outside a
    ///   transaction.
    /// - `CreateInvite` only counts and sweeps. An invite this node has not
    ///   seen yet costs a slot and a sweep, not correctness.
    ///
    /// Whether an invite may be used is decided under a lock, in
    /// `redeem_invite`.
    async fn read_invites(
        &self,
    ) -> Result<(Vec<(String, InviteRecord)>, Vec<String>), AuthFailure> {
        let rows = self
            .store
            .run(|log| log.list_records_under(crate::keyspace::INVITE_PREFIX))
            .await
            .map_err(|e| AuthFailure::from(RaftError::Storage(e)))?;

        // One `now` for the whole split, so two invitations that expire in the
        // same millisecond cannot be classed differently.
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

    /// Create an invitation and write the cluster's half of it.
    ///
    /// The secret exists only in this process and in the response. The log
    /// gets its SHA-256, which is also the record's key. So this is the only
    /// moment the token is known, and an issuer who loses it must revoke and
    /// create another: nothing can look it up.
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

        // A name that can never be stored cannot be pinned to. Checking it now
        // means the pin is checked once, here, and the recipient does not
        // discover later that the invite is unusable.
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
                // Two different 32-byte secrets do not hash alike, so this is not a race
                // guard. It states the rule that a record is written once, at the place
                // that rule could break.
                if self.transactional_read(id, &key).await?.value.is_some() {
                    return Err(
                        Status::internal("the invitation drawn already exists; try again").into(),
                    );
                }
                // Lapsed invitations are swept here. The cluster has no timer for
                // expiry and no state machine hook to run one, so the work goes into
                // the call that cares how many there are. This stops the reserved range
                // from filling with records that can never be used.
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

    /// Use an invitation to create an account.
    ///
    /// Answered for anyone: this is the only write on the service that runs
    /// without a token (see `auth::EXEMPT`), because its callers have no
    /// account yet. The invitation takes the place of the token.
    ///
    /// The checks run twice on purpose, and they are not the same check:
    /// - First, a plain read before any work. An anonymous caller can make
    ///   this node do whatever it does before it decides, so that part must
    ///   be cheap. Argon2 hashing costs 19 MiB and several milliseconds:
    ///   worth it for a real redemption, not for random input.
    /// - Second, inside the transaction, under a lock. This one counts.
    ///   Between the two, the invite could be revoked, used by someone
    ///   else, or lapse, and the account name could be taken. So the record
    ///   is read again, and `is_admin` comes from the locked read: the
    ///   account's rights must come from the invite as it was when it was
    ///   used.
    ///
    /// The account and the invite's removal are buffered into one
    /// transaction, so they land at one seq or not at all. That is what
    /// makes an invite single-use: two callers with the same token contend
    /// for the same key, and one of them loses.
    async fn redeem_invite_inner(&self, req: RedeemInviteRequest) -> Result<u64, AuthFailure> {
        if self.auth.is_none() {
            return Err(AuthFailure::disabled());
        }
        // Limit the token size before hashing. The rest of the check is whether
        // its hash names a record, but hashing is work, and an anonymous caller
        // does not get to choose how much.
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

        // Outside the transaction: Argon2 is slow on purpose, and holding a lock
        // on the account name for that long would make each redemption wait for
        // the others.
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
                // Do not use up the invite. The holder chose a name that is taken, which
                // they can fix by choosing another. Burning the invite would leave them
                // unable to.
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

        // Lapsed invites are not listed. Nobody can use them and the next
        // `CreateInvite` sweeps them, so listing them would show the cluster's
        // own housekeeping as something to act on.
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
            // A missing id is `not_found`, not a silent success. An admin revokes an
            // invite because they believe a live one exists, and "already gone" is
            // what they most need to hear: someone used it first.
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
            // Not yet bootstrapped or added to a cluster: not an error, just an
            // empty configuration.
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
        // `into_parts`, not `into_inner`: the caller's identity is in the
        // extensions, and the ACL check needs it.
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
        // Refused, not shortened: unlike the value budget, the key count is the
        // caller's choice and it can know the limit.
        if req.keys.len() > raft5_shared::MAX_KEYS_PER_READ {
            return Err(Status::resource_exhausted(
                "that is more keys than this server will read in one call",
            ));
        }

        // Before the branch, so it also covers the transactional read. Refused,
        // not answered as absent: a returned password hash can be attacked
        // offline, and even the seq of an account record lets someone watch
        // the cluster's administration.
        for key in &req.keys {
            reject_reserved(key)?;
        }

        // Also before the branch: every mode reads the same keys, and the mode
        // does not change whether this caller may read them. All or nothing,
        // because a positional answer cannot leave out a key without saying
        // which.
        if let Err(e) = self
            .require_access_all(&extensions, &req.keys, Right::Read)
            .await
        {
            return Ok(Response::new(ReadResponse {
                result: Some(match auth_failure(e)? {
                    AuthReply::Raft(e) => read_response::Result::Error(e),
                    AuthReply::Auth(e) => read_response::Result::AuthError(e),
                }),
            }));
        }

        let result = match req.consistency {
            // A transactional read takes locks and can answer from the
            // transaction's buffer. Only the transaction's own leader, with a read
            // quorum, serves it.
            Some(read_request::Consistency::TransactionId(id)) => {
                let id = txn_id(&id)?;
                match self
                    .transactional_multi_read(id, req.keys, req.versions_only)
                    .await
                {
                    Ok(ok) => Ok(ok),
                    Err(e) => Err(txn_error(e)?),
                }
            }

            // Linearizable without a transaction: this node proves it still leads
            // and holds a read quorum, then answers. Nothing is locked, so the keys
            // can change right after the answer.
            Some(read_request::Consistency::Linearizable(_)) => match self.read_quorum().await {
                Ok(()) => Ok(self
                    .multi_read(req.keys, req.versions_only, ReadPoint::Latest)
                    .await?),
                Err(e) => Err(client_err(e)?),
            },

            // A snapshot read is stale by design (it describes a past moment), so
            // any node serves it, with no leader check and no quorum round.
            Some(read_request::Consistency::SnapshotId(id)) => {
                require_snapshot_id(&id)?;
                Ok(self
                    .multi_read(req.keys, req.versions_only, ReadPoint::Checkpoint(id))
                    .await?)
            }

            // `consistency` unset: answered from local state by any node, with no
            // read-quorum round and no wait to catch up. So it can be behind, by a
            // write still replicating or by however far this node lags. `as_of`
            // shows the caller how far.
            None => Ok(self
                .multi_read(req.keys, req.versions_only, ReadPoint::Latest)
                .await?),
        };

        Ok(Response::new(ReadResponse {
            result: Some(match result {
                Ok(ok) => read_response::Result::Ok(ok),
                Err(e) => read_response::Result::Error(e),
            }),
        }))
    }

    async fn get_keys(
        &self,
        request: Request<GetKeysRequest>,
    ) -> Result<Response<GetKeysResponse>, Status> {
        let (_, extensions, req) = request.into_parts();
        // `scan` skips the reserved range anyway, so this refusal adds no
        // protection. It marks the boundary: a walk from the start uses an
        // empty key, which is not reserved, so no valid caller asks for this.
        reject_reserved(&req.start_key)?;

        // What this caller may not read, built once for the whole call. Unlike
        // `read`, a walk is never refused for it: hidden keys are left out, so a
        // caller with access to one subtree can still walk the store.
        let filter = self.scan_filter(&extensions).await?;

        // Refused, not shortened, so a caller that pages in batches of N learns
        // that it is not getting N, instead of getting a different shape of
        // answer.
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

        // A snapshot walk: the keys that had a value at that seq, including keys
        // deleted since and excluding keys created since. Stale by design, so
        // any node serves it.
        let as_of = match &req.consistency {
            Some(get_keys_request::Consistency::SnapshotId(id)) => self.checkpoint_seq(id).await?,
            _ => u64::MAX,
        };

        // Outside a transaction, the same terms as `read`: local state, no
        // locks, any node. There is nothing to merge (no buffered writes) and
        // no second pass (no locks to take).
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
            // The transaction's reads come from `latest`, so a leader that may
            // still miss entries must not open one.
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
                // `append_writes` returns only after the entry is committed and
                // applied. So when a client sees this seq, every key the transaction
                // wrote reads back at it.
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
        // Always runs, with no leader check: giving up locks must never be
        // refused, and a non-leader holds no state for this transaction anyway.
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

        // Check leadership before the round trip below, not only in `propose`:
        // a follower must not dial the new server only to learn that it should
        // have answered `NotLeader`. `propose` checks again at commit time,
        // because leadership can change during the round trip.
        if let Err(e) = require_leader(&self.client_handle.role()) {
            return not_leader_response(e);
        }
        // Also check admin rights first: dialing a server that a caller names
        // is a side effect, and only an admin may cause it.
        if let Err(e) = self.require_admin(&extensions).await {
            return config_change_refusal(e);
        }

        // Ask the new server who it is, over its peer API, dialed exactly as it
        // will be as a peer. A server this node cannot replicate to fails here,
        // not after it is added.
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

        // Check that the configuration does not already include the new server.
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

        // Added as a learner, not a voter: see `ConfigChange::AddServer`. It is
        // promoted automatically when it catches up, so for a client this one
        // call is the whole operation.
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
        // Checked here as well as on the leader, so an operator who typed a bad
        // value gets a plain error, not a refused change.
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
        let config = self.bootstrap_config(&extensions, &req)?;
        self.client_handle
            .bootstrap(config)
            .await
            .map_err(raft_err)?;
        // Before the answer, not after: the startup password stops working as
        // soon as a configuration exists, so an operator told "ok" needs the
        // administrator to be in the log already.
        self.write_startup_admin().await?;

        Ok(Response::new(ConfigChangeResponse {
            result: Some(config_change_response::Result::Ok(true)),
        }))
    }

    type BackupStream = tokio_stream::wrappers::ReceiverStream<Result<BackupChunk, Status>>;

    async fn backup(
        &self,
        request: Request<BackupRequest>,
    ) -> Result<Response<Self::BackupStream>, Status> {
        let (_, extensions, req) = request.into_parts();
        let (out_tx, out_rx) = tokio::sync::mpsc::channel(crate::store::CHUNK_BACKLOG);

        if let Err(e) = self.require_backup_rights(&extensions).await {
            let refusal = match auth_failure(e)? {
                AuthReply::Auth(e) => Ok(BackupChunk {
                    result: Some(backup_chunk::Result::AuthError(e)),
                }),
                AuthReply::Raft(_) => Err(Status::unavailable("this node cannot check accounts now")),
            };
            let _ = out_tx.send(refusal).await;
            return Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(out_rx)));
        }

        // Check the snapshot exists before any byte is sent, so the caller gets
        // `not_found` instead of a stream that fails at the end. If it is
        // removed after this check, the export reports that at the end.
        let point = if req.snapshot_id.is_empty() {
            ReadPoint::Latest
        } else {
            self.checkpoint_seq(&req.snapshot_id).await?;
            ReadPoint::Checkpoint(req.snapshot_id.clone())
        };

        // The export runs on the blocking pool and writes through a bounded
        // channel, so only a few chunks are in memory at once. If the caller
        // goes away, the forwarding task stops, the export's next write fails,
        // and the export stops walking the keyspace.
        let (chunk_tx, mut chunk_rx) = tokio::sync::mpsc::channel(crate::store::CHUNK_BACKLOG);
        let node_id = self.identity.server_id.clone();
        let snapshot_id = req.snapshot_id;
        let exporting = self.store.spawn(move |log| {
            let source = BackupSource {
                node_id: &node_id,
                snapshot_id: &snapshot_id,
                created_unix_ms: crate::auth::now_unix_ms(),
            };
            log.export_backup(
                &mut ChunkWriter::new(chunk_tx),
                &point,
                &source,
                crate::keyspace::is_backed_up,
            )
        });
        tokio::spawn(
            async move {
                while let Some(data) = chunk_rx.recv().await {
                    let chunk = BackupChunk {
                        result: Some(backup_chunk::Result::Data(data)),
                    };
                    if out_tx.send(Ok(chunk)).await.is_err() {
                        return;
                    }
                }
                let failure = match exporting.await {
                    Ok(Ok(BackupExport::Done { .. })) => return,
                    Ok(Ok(BackupExport::NoSuchCheckpoint)) => no_such_snapshot(),
                    Ok(Err(e)) => storage_err(e),
                    Err(e) => Status::internal(format!("the backup task failed: {e}")),
                };
                let _ = out_tx.send(Err(failure)).await;
            }
            .instrument(tracing::Span::current()),
        );

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(out_rx)))
    }

    async fn bootstrap_from_backup(
        &self,
        request: Request<Streaming<BootstrapFromBackupChunk>>,
    ) -> Result<Response<ConfigChangeResponse>, Status> {
        let (_, extensions, mut stream) = request.into_parts();
        let first = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("empty backup stream"))?;
        let header = match first.part {
            Some(bootstrap_from_backup_chunk::Part::Header(header)) => header,
            _ => return Err(Status::invalid_argument("the first message must be the header")),
        };
        let config = self.bootstrap_config(&extensions, &header)?;

        // The restore and the pump run together, as for InstallSnapshot: the
        // restore waits for chunks that only the pump delivers. When the pump
        // ends, for any reason, it drops `tx`, the restore sees the end of the
        // stream, and a backup cut short fails its trailer check and is
        // discarded.
        let (tx, rx) = tokio::sync::mpsc::channel(crate::store::CHUNK_BACKLOG);
        let restore = self
            .client_handle
            .bootstrap_from_backup(config, ChunkReader::new(rx));
        let pump = async move {
            while let Some(chunk) = stream.message().await? {
                let Some(bootstrap_from_backup_chunk::Part::Data(data)) = chunk.part else {
                    return Err(Status::invalid_argument("only data may follow the header"));
                };
                // A closed channel means the restore stopped early (it refused
                // the node or the backup). Its answer is the one to report.
                if tx.send(data.to_vec()).await.is_err() {
                    break;
                }
            }
            Ok::<(), Status>(())
        };
        // A restore that succeeded read a whole, checked backup, so it stands.
        // A failed one may have failed only because the stream broke, and then
        // the stream's error says more than "the backup is truncated".
        let (restored, pumped) = tokio::join!(restore, pump);
        if let Err(e) = restored {
            pumped?;
            return Err(raft_err(e));
        }

        // As for Bootstrap. The startup administrator is written only if the
        // backup has no account with that username: `write_startup_admin`
        // treats "already exists" as done, so a backed-up password stays.
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

        // Both checks do not block, on purpose. `is_caught_up` asks the
        // question that `await_caught_up` waits on. A client looking for the
        // leader wants this node's membership either way, and a leader still
        // applying its inherited tail should say "not yet" now, not hold up
        // the search.
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
        // `into_parts`, not `into_inner`: the auth layer puts the caller's
        // identity in the extensions.
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
        // A local read: any node answers, stale like any read outside a
        // transaction, so a snapshot created a moment ago may not be here yet.
        // Not admin-only: a client must know which snapshots exist to read at
        // one, and the ids are its own.
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
        // `into_parts`, not `into_inner`: the auth layer puts the caller's
        // identity in the extensions.
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
        // `into_inner`, not `into_parts`: there is no verified identity here,
        // by design. The caller's rights come from the invitation in the
        // message.
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

    async fn set_backup_operators(
        &self,
        request: Request<SetBackupOperatorsRequest>,
    ) -> Result<Response<SetBackupOperatorsResponse>, Status> {
        let (_, extensions, req) = request.into_parts();
        let result = self.set_backup_operators_inner(&extensions, req).await;
        Ok(Response::new(SetBackupOperatorsResponse {
            result: Some(match result {
                Ok(seq) => set_backup_operators_response::Result::Seq(seq),
                Err(e) => match auth_failure(e)? {
                    AuthReply::Raft(e) => set_backup_operators_response::Result::Error(e),
                    AuthReply::Auth(e) => set_backup_operators_response::Result::AuthError(e),
                },
            }),
        }))
    }

    async fn get_backup_operators(
        &self,
        request: Request<GetBackupOperatorsRequest>,
    ) -> Result<Response<GetBackupOperatorsResponse>, Status> {
        let (_, extensions, _) = request.into_parts();
        let result = self.get_backup_operators_inner(&extensions).await;
        Ok(Response::new(GetBackupOperatorsResponse {
            result: Some(match result {
                Ok(ok) => get_backup_operators_response::Result::Ok(ok),
                Err(e) => match auth_failure(e)? {
                    AuthReply::Raft(e) => get_backup_operators_response::Result::Error(e),
                    AuthReply::Auth(e) => get_backup_operators_response::Result::AuthError(e),
                },
            }),
        }))
    }
}

pub struct RaftPeerService {
    peer_handle: RaftPeerHandle,
    /// What `Identify` answers with: this node's identity, and the store
    /// that says if it has a configuration yet. `None` on a service built
    /// without them, which cannot be added to a cluster. Only tests that
    /// drive the Raft RPCs directly build one like that.
    identity: Option<(ClusterMember, Store)>,
}

impl RaftPeerService {
    pub fn new(peer_handle: RaftPeerHandle) -> Self {
        Self {
            peer_handle,
            identity: None,
        }
    }

    /// Answer `Identify` as `identity`, and report whether `store` holds a
    /// configuration yet.
    pub fn identify_as(mut self, identity: ClusterMember, store: Store) -> Self {
        self.identity = Some((identity, store));
        self
    }
}

/// What `reassemble_append_entries` decided about a stream.
#[derive(Debug)]
enum Reassembled {
    /// A complete request, for the event loop to rule on.
    Request(Box<AppendEntriesRequest>),
    /// Refused here, before the event loop, with the reason. Still answered
    /// with a normal response, not an error: see below.
    Refused(&'static str),
}

/// Read one `AppendEntries` stream back into the request it came from.
///
/// A free function, not a method, so that it can be tested:
/// `tonic::Streaming` has no public constructor, so code that takes a
/// `Request<Streaming<_>>` cannot be tested. The rules here are the part
/// worth testing:
/// - the header comes first,
/// - the size limit,
/// - the declared byte count matches the actual one,
/// - the fast path for a stale term.
///
/// `current_term` is a lower bound on this node's term, used only to
/// refuse early. The event loop checks everything that gets past it;
/// that is where §5.1 is enforced.
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

    // Two reasons to ignore the body. In both cases we still read it to the
    // end: see `drain` below.
    let refusal = if header.term < current_term {
        Some("the leader's term is behind this node's")
    } else if header.entries_bytes > MAX_APPEND_ENTRIES_BYTES as u64 {
        Some("the declared batch size is over this node's limit")
    } else {
        None
    };

    // If the declared size set the buffer size, one header could exhaust
    // memory. So the allocation is bounded by what this node accepts.
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
            // Drain, do not collect: the rest of the stream costs bandwidth only.
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
    /// Reassemble the stream and pass the result in unchanged. The term
    /// check, log matching and append all happen on the event loop.
    ///
    /// A refusal is a normal `AppendEntriesResponse` with this node's term,
    /// never a `Status`. This matters: the response is how a leader with a
    /// stale term learns to step down (`Event::response_term`). An error
    /// sent while the leader is still writing its request reaches it as a
    /// transport failure, which it retries forever without seeing the term
    /// that should stop it.
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
                    // Read the term again: the event loop may have moved on while the body
                    // drained, and the leader should get the newest term this node has.
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
        // Synchronous and infallible on purpose: see the doc of
        // `RaftPeerHandle::get_lease` for why this must never wait behind, for
        // example, a slow AppendEntries.
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

        // The first message must be the header. It carries Raft metadata (term,
        // leader_id) for this RPC, separate from the storage header inside the
        // reassembled bytes, which `RaftLog::install_snapshot` parses itself.
        let first = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("empty snapshot stream"))?;
        let header = match first.part {
            Some(snapshot_chunk::Part::Header(header)) => header,
            _ => return Err(Status::invalid_argument("first chunk must be a header")),
        };

        // Chunks go to `install_snapshot` as they arrive, not after assembly. A
        // snapshot is the cluster's whole applied state, so "buffer it, then
        // install it" fails at some size. Only `CHUNK_BACKLOG` chunks are held
        // here, whatever the snapshot size.
        //
        // The install and the pump must run at the same time: the install waits
        // for chunks that only the pump delivers, and the pump finishes only
        // when the stream ends.
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
                // A closed channel means the state machine refused this snapshot
                // (probably a stale term) and dropped the reader. Read the stream to its
                // end anyway: an answer sent while the sender still writes its request
                // reaches it as a transport error, not as the response whose term should
                // make it step down.
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
