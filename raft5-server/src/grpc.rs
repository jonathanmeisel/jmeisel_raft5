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
use raft5_shared::raft5::client::kv_client::KvClient;
use tonic::transport::Endpoint;
use tonic::{Request, Response, Status, Streaming};
use tracing::Instrument;

use crate::auth::{AuthConfig, AuthContext, UserRecord};
use crate::keyspace::is_reserved;
use crate::pb::raft5::client::kv_server::Kv;
use crate::pb::raft5::client::{
    AddServerRequest, AuthError as PbAuthError, BootstrapRequest, CommitTransactionRequest,
    CommitTransactionResponse, ConfigChangeResponse, CreateTransactionRequest,
    CreateTransactionResponse, CreateUserRequest, CreateUserResponse, DeleteUserRequest,
    DeleteUserResponse, DropTransactionRequest, DropTransactionResponse, GetConfigRequest,
    GetConfigResponse, GetKeysOk, GetKeysRequest, GetKeysResponse, GetVersionOk, GetVersionRequest,
    GetVersionResponse, LoginOk, LoginRequest, LoginResponse, NotLeader, RaftError as PbRaftError,
    ReadOk, ReadRequest, ReadResponse, RemoveServerRequest, TransactionAborted, WriteRequest,
    WriteResponse, auth_error, commit_transaction_response, config_change_response,
    create_transaction_response, create_user_response, delete_user_response,
    drop_transaction_response, get_keys_response, get_version_request, get_version_response,
    login_response, raft_error, read_request, read_response, transaction_aborted, write_response,
};
use crate::pb::raft5::cluster::{ClusterConfig, ClusterMember};
use crate::pb::raft5::raft::raft_peer_server::RaftPeer;
use crate::pb::raft5::raft::{
    AppendEntriesChunk, AppendEntriesRequest, AppendEntriesResponse, EntryBatch, GetLeaseRequest,
    GetLeaseResponse, InstallSnapshotResponse, RequestVoteRequest, RequestVoteResponse,
    SnapshotChunk, append_entries_chunk, snapshot_chunk,
};
use crate::raft::{
    self, AppendError, ClientHandle, ConfigChange, LeaderHandle, MAX_APPEND_ENTRIES_BYTES,
    RaftError, RaftPeerHandle, RoleHandle,
};
use crate::storage::Mutation;
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
        }
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
        limit: usize,
        buffered: &BufferedKeys,
    ) -> Result<(Vec<Vec<u8>>, bool), StorageError> {
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
        let committed = {
            let start = start.clone();
            self.store
                .run(move |log| {
                    // Folded into the skip closure rather than filtered
                    // out of the result: a skipped key doesn't count
                    // against `want`, so hiding the reserved range costs
                    // the caller no batch length. Both callers -- the
                    // transactional scan and the plain one -- come
                    // through here, so this is the whole of `GetKeys`.
                    log.scan_latest(&start, want, |key| {
                        deleted.contains(key) || is_reserved(key)
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

        let has_more = keys.len() > limit;
        keys.truncate(limit);
        Ok((keys, has_more))
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
    ) -> Result<(Vec<Vec<u8>>, bool), TxnError> {
        let leader = self.transaction_leader(id)?;
        leader.await_caught_up().await?;
        if !leader.has_read_quorum() {
            return Err(RaftError::NotLeader { leader_hint: None }.into());
        }

        let buffered = self.locks.buffered_from(id, &start)?;

        let (candidates, _) = self
            .scan(start.clone(), limit, &buffered)
            .await
            .map_err(|e| TxnError::Raft(RaftError::Storage(e)))?;
        self.locks.read_many(id, &candidates)?;

        let (keys, has_more) = self
            .scan(start, limit, &buffered)
            .await
            .map_err(|e| TxnError::Raft(RaftError::Storage(e)))?;

        // Only what the locks above actually cover. Anything else the
        // second pass turned up arrived after this transaction looked,
        // and handing it back unlocked would be exactly the unprotected
        // data under a transaction's name that the lock table exists to
        // prevent.
        let locked: std::collections::HashSet<&[u8]> =
            candidates.iter().map(Vec::as_slice).collect();
        let keys = keys
            .into_iter()
            .filter(|key| locked.contains(key.as_slice()))
            .collect();
        Ok((keys, has_more))
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

    /// Writes an account record, or removes it when `record` is `None`.
    ///
    /// A read-modify-write -- whether the account already exists decides
    /// whether the call is allowed at all -- so the check and the write
    /// have to be atomic against another admin doing the same thing at
    /// the same moment. That is why this drives an ordinary transaction
    /// instead of appending directly, and in turn why reserved keys are
    /// refused at the RPC boundary rather than in `Locks`: the lock
    /// table cannot tell this write from a client's, and must not.
    ///
    /// Nothing a client does can contend for these keys, since `write`
    /// refuses them, so the only competition here is another admin call.
    /// Two racing creations of one name resolve the way any two
    /// transactions do, and the loser is told its transaction aborted.
    ///
    /// Clients and the server do share one thing, though: the open
    /// transaction table. Enough concurrent client transactions to fill
    /// `MAX_OPEN_TRANSACTIONS` will refuse this one too, surfacing as
    /// `resource_exhausted`. Left as it is rather than given reserved
    /// headroom, because an administrator retrying is a far smaller
    /// problem than a second allocation rule to keep consistent with the
    /// first.
    async fn put_user(
        &self,
        username: &str,
        record: Option<UserRecord>,
    ) -> Result<u64, AuthFailure> {
        let key = crate::keyspace::user_key(username);
        let leader = self.leader()?;
        leader.await_caught_up().await?;
        let id = self
            .locks
            .create(leader.term(), ADMIN_TXN_TTL, crate::txn::fresh_id())?;

        // Explicit rather than a `Drop` guard: discarding a transaction
        // that already committed is a no-op, and the commit path itself
        // hands its locks to the task doing the append, so there is
        // nothing here that releasing early could cut short.
        let result = self.write_user(id, &key, record).await;
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
            return Err(Status::unauthenticated("this request carried no verified identity").into());
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
        Ok(self.load_user(username).await?.map(|user| user.password_hash))
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

    async fn get_config_internal(&self) -> Result<Vec<ClusterMember>, StorageError> {
        let bytes = self.store.run(RaftLog::current_config).await?;

        let members = match bytes {
            Some(bytes) => {
                ClusterConfig::from_bytes(&bytes)
                    .map_err(|_| StorageError::Corrupt("stored cluster config was corrupt"))?
                    .members
            }
            // Not bootstrapped or added to a cluster yet -- not an error,
            // just an empty membership list.
            None => Vec::new(),
        };
        Ok(members)
    }
}

#[tonic::async_trait]
impl Kv for KvService {
    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let req = request.into_inner();
        let id = txn_id(&req.transaction_id)?;
        // The privilege-escalation guard. A client able to buffer a
        // write to a reserved key could write its own account record,
        // with `is_admin` set to whatever it liked.
        reject_reserved(&req.key)?;

        // Purely in-memory: nothing reaches the log until `commit`. The
        // leader handle is fetched only for the term check inside
        // `transaction_leader` -- a transaction cannot be added to from
        // anywhere but the leader that created it.
        let result = self.transaction_leader(id).and_then(|_| {
            self.locks
                .buffer_write(id, &req.key, req.value)
                .map_err(Into::into)
        });

        Ok(Response::new(WriteResponse {
            result: Some(match result {
                Ok(()) => write_response::Result::Ok(true),
                Err(e) => write_response::Result::Error(txn_error(e)?),
            }),
        }))
    }

    async fn read(&self, request: Request<ReadRequest>) -> Result<Response<ReadResponse>, Status> {
        let req = request.into_inner();
        // Before the branch, so it covers the transactional read too.
        // Refused rather than answered as absent: a password hash handed
        // back is one to attack offline, at leisure.
        reject_reserved(&req.key)?;

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
        let req = request.into_inner();
        // Before the branch, as in `read`, and for the same reason. The
        // seq of an account record is still a fact about the reserved
        // range, and "this key changed at seq N" is enough to watch the
        // administration of a cluster one has no business watching.
        reject_reserved(&req.key)?;

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

        // Stale, as `read` is stale, and cheap in the way this RPC exists
        // to be: one point lookup on `latest_seq`, whatever the value
        // behind it weighs.
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
        let req = request.into_inner();
        // `scan` would skip the whole reserved range anyway, so this
        // refusal protects nothing the filter doesn't. It is here to say
        // where the boundary is: a caller starting a walk at the very
        // beginning passes an empty key, which is not reserved, so
        // nothing legitimate asks for this.
        reject_reserved(&req.start_key)?;

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

        if let Some(id) = &req.transaction_id {
            let id = txn_id(id)?;
            let result = self.transactional_scan(id, req.start_key, limit).await;
            return Ok(Response::new(GetKeysResponse {
                result: Some(match result {
                    Ok((keys, has_more)) => get_keys_response::Result::Ok(GetKeysOk {
                        keys,
                        has_more,
                    }),
                    Err(e) => get_keys_response::Result::Error(txn_error(e)?),
                }),
            }));
        }

        // Outside a transaction, the same terms as `read`: local state,
        // no locks, whoever received it. There is nothing to merge --
        // no transaction means no buffered writes -- and no second pass,
        // since there are no locks for one to be taken under.
        let (keys, has_more) = self
            .scan(req.start_key, limit, &BufferedKeys::new())
            .await
            .map_err(storage_err)?;

        Ok(Response::new(GetKeysResponse {
            result: Some(get_keys_response::Result::Ok(GetKeysOk { keys, has_more })),
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

        // First, send an RPC to the new server to get its configuration.
        let mut client = KvClient::new(
            Endpoint::from_shared(req.client_address.clone())
                .map_err(|e| Status::invalid_argument(format!("invalid address: {e}")))?
                .connect_lazy(),
        );
        let response = client
            .get_config(Request::new(GetConfigRequest {}))
            .await
            .map_err(|e| {
                Status::unavailable(format!(
                    "failed to reach the new server at {}: {e}",
                    req.client_address
                ))
            })?
            .into_inner();

        let identity = response.identity.ok_or_else(|| {
            Status::invalid_argument("the new server did not report its identity")
        })?;
        if !response.members.is_empty() {
            return Err(Status::invalid_argument(
                "the new server is already part of a cluster",
            ));
        }

        // Check that the new server hasn't already been added to the configuration.
        let members = self.get_config_internal().await.map_err(storage_err)?;
        for member in &members {
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
        self.require_startup_admin(&extensions)?;

        // A cluster of exactly one: the node is its own sole voter, so it
        // wins the next election unopposed and can then serve AddServer
        // to grow from there.
        let config = ClusterConfig {
            members: vec![self.identity.clone()],
            heartbeat_millis: raft::HEARTBEAT_MS,
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
        let members = self.get_config_internal().await.map_err(storage_err)?;

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
            members,
            is_leader,
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
}

pub struct RaftPeerService {
    peer_handle: RaftPeerHandle,
}

impl RaftPeerService {
    pub fn new(peer_handle: RaftPeerHandle) -> Self {
        Self { peer_handle }
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

#[cfg(test)]
mod tests;
