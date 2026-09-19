//! gRPC service implementations wiring the generated proto types (`pb`)
//! into `storage::RaftLog`.
//!
//! `KvService` is the client-facing half. Everything that changes state
//! -- writes, membership changes, and a linearizable read -- requires
//! this node to be the leader, and a node that isn't answers `NotLeader`
//! with whatever leader hint it has rather than forwarding (Raft §6.2),
//! so the client retries against the leader directly and exactly one node
//! does the work. A *stale* read is the one exception: it asks for no
//! guarantee, so it is served locally by whoever receives it. `Bootstrap`
//! is the other: by definition it targets a node with no leader at all.
//!
//! Every write is conditional: it names the sequence number the key must
//! currently be at, and is refused with `WriteConflict` if it isn't.
//! That is what makes a read-modify-write possible over a protocol where
//! a client's read and its write are two separate round trips. Two
//! things have to hold for that check to mean anything, and
//! `conditional_write` below establishes both before it appends: this
//! leader's `latest` must be complete (`LeaderHandle::await_caught_up`),
//! and no other write to the same key may be in flight
//! (`KvService::claim`). Between an entry being appended and being
//! applied, `latest` still answers for the version that entry replaces,
//! and a precondition checked against it then would be checked against
//! a value that is already gone.
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

use crate::pb::raft5::client::kv_server::Kv;
use crate::pb::raft5::client::{
    AddServerRequest, BootstrapRequest, BufferWriteRequest, BufferWriteResponse,
    CommitTransactionRequest, CommitTransactionResponse, ConfigChangeResponse,
    CreateTransactionRequest, CreateTransactionResponse, DropTransactionRequest,
    DropTransactionResponse, GetConfigRequest, GetConfigResponse, NotLeader,
    RaftError as PbRaftError, ReadOk, ReadRequest, ReadResponse, RemoveServerRequest,
    TransactionAborted, WriteConflict, WriteRequest, WriteResponse, buffer_write_response,
    commit_transaction_response, config_change_response, create_transaction_response,
    drop_transaction_response, raft_error, read_response, transaction_aborted, write_response,
};
use crate::pb::raft5::cluster::{ClusterConfig, ClusterMember};
use crate::pb::raft5::raft::raft_peer_server::RaftPeer;
use crate::pb::raft5::raft::{
    AppendEntriesChunk, AppendEntriesResponse, AppendEntriesRequest, EntryBatch, GetLeaseRequest,
    GetLeaseResponse, InstallSnapshotResponse, RequestVoteRequest, RequestVoteResponse,
    SnapshotChunk, append_entries_chunk, snapshot_chunk,
};
use crate::raft::{
    self, AppendError, ClientHandle, ConfigChange, LeaderHandle, MAX_APPEND_ENTRIES_BYTES,
    RaftError, RaftPeerHandle, RoleHandle,
};
use crate::storage::Mutation;
use crate::storage::{RaftLog, StorageError};
use crate::store::Store;
use crate::store::ChunkReader;
use crate::txn::{AbortReason, BareGuard, Locks, Refused, TxnId};

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

/// A transaction id off the wire. Fixed-width, so anything else is a
/// malformed request rather than a transaction that happens not to
/// exist.
fn txn_id(bytes: &[u8]) -> Result<TxnId, Status> {
    bytes
        .try_into()
        .map_err(|_| Status::invalid_argument("transaction_id must be 16 bytes"))
}

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

/// Why a write didn't happen. `RaftError` covers everything the state
/// machine can refuse; the rest is specific to a conditional write and
/// never reaches the state machine at all.
enum WriteError {
    Raft(RaftError),
    /// The precondition didn't hold. `Some(seq)` is what the key is
    /// actually at (0 for no value); `None` means another write to the
    /// same key is still in flight, so there is nothing settled to
    /// report yet.
    Conflict(Option<u64>),
    Internal(Status),
}

impl From<RaftError> for WriteError {
    fn from(e: RaftError) -> Self {
        Self::Raft(e)
    }
}

/// `WriteError` as the client protocol carries it -- `client_err` for
/// the state machine's half, and a conflict is an ordinary answer rather
/// than an error, since it says something specific about the key.
fn write_err(e: WriteError) -> Result<PbRaftError, Status> {
    match e {
        WriteError::Raft(e) => client_err(e),
        WriteError::Conflict(actual_seq) => Ok(PbRaftError {
            kind: Some(raft_error::Kind::Conflict(WriteConflict { actual_seq })),
        }),
        WriteError::Internal(status) => Err(status),
    }
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
    /// Who holds what, and every open transaction. See `crate::txn`: a
    /// plain write's claim on a key and a transaction's locks are the
    /// same table, because a write that could bypass a transaction's
    /// locks would make its read set a record of something no longer
    /// true.
    locks: Locks,
}

impl KvService {
    pub fn new(store: Store, identity: ClusterMember, client_handle: ClientHandle) -> Self {
        Self {
            store,
            identity,
            client_handle,
            locks: Locks::new(),
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

    /// Claims `key` for one plain write, or reports the conflict.
    fn claim(&self, key: &[u8]) -> Result<BareGuard, WriteError> {
        self.locks.claim(key).map_err(|_| {
            // Nothing settled can be said about a key somebody else is
            // holding: their write may land, or their transaction may
            // abort. Re-sending shortly may well succeed, which is
            // exactly what `Conflict(None)` means.
            WriteError::Conflict(None)
        })
    }

    /// Checks a write's precondition and, if it holds, appends it.
    ///
    /// The append runs in a task of its own that owns the key's claim,
    /// so that the claim is released when the *entry* resolves rather
    /// than when this call does. A client that disconnects mid-write
    /// leaves an entry that may still commit, and releasing its key at
    /// that point would let the next write's precondition be checked
    /// against a `latest` that is about to change.
    async fn conditional_write(&self, req: WriteRequest) -> Result<u64, WriteError> {
        let leader = self.leader()?;
        // The precondition is checked against `latest`, so a leader that
        // may still be missing entries has no business checking it.
        // Before the claim rather than after, so that a leader still
        // catching up isn't holding keys no one can write while it does.
        leader.await_caught_up().await?;

        // Claimed before the precondition is read, so that two writes
        // racing here cannot both find the key at the version each is
        // about to supersede.
        let guard = self.claim(&req.key)?;

        let key = req.key.clone();
        let actual = self
            .store
            .run(move |log| log.get_latest(&key))
            .await
            .map_err(|e| WriteError::Raft(RaftError::Storage(e)))?
            // Seq 0 is never assigned, so it doubles as "this key has no
            // value" -- the same sentinel `read` hands back, which is
            // what lets a client echo a seq it read straight back here.
            .map_or(0, |(seq, _)| seq);
        if actual != req.previous_seq.unwrap_or(0) {
            return Err(WriteError::Conflict(Some(actual)));
        }

        tokio::spawn(async move {
            let _guard = guard;
            leader.append_log(&req.key, req.value.as_deref()).await
        })
        .await
        .map_err(|e| WriteError::Internal(Status::internal(format!("the write task failed: {e}"))))?
        .map_err(WriteError::Raft)
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
    /// `require_leader` and `await_caught_up` come *before* the lock, the
    /// same ordering and for the same reason as `conditional_write` --
    /// a leader still catching up should not be holding keys nobody can
    /// write while it does it.
    async fn transactional_read(&self, id: TxnId, key: &[u8]) -> Result<ReadOk, TxnError> {
        let leader = self.transaction_leader(id)?;
        leader.await_caught_up().await?;
        if !leader.has_read_quorum() {
            return Err(RaftError::NotLeader { leader_hint: None }.into());
        }

        let buffered = self.locks.read(id, key)?;

        let key = key.to_vec();
        let committed = self
            .store
            .run(move |log| log.get_latest(&key))
            .await
            .map_err(|e| TxnError::Raft(RaftError::Storage(e)))?;

        // `seq` always describes committed state, even when the value
        // doesn't: a buffered write has no sequence number yet, because
        // nothing has been appended. So a buffered read's seq is not a
        // valid `previous_seq` for a write outside this transaction --
        // though inside one it is moot, since the lock is what a
        // transaction uses instead of a precondition.
        let seq = committed.as_ref().map_or(0, |(seq, _)| *seq);
        let value = match buffered {
            Some(buffered) => buffered,
            None => committed.map(|(_, value)| value),
        };
        Ok(ReadOk { seq, value })
    }

    /// Commit everything a transaction buffered, as one entry.
    ///
    /// The append runs in a task of its own owning the transaction's
    /// locks, so they are released when the *entry* resolves rather than
    /// when this call does -- the same discipline as `conditional_write`,
    /// and more load-bearing here. A client that disconnects mid-commit
    /// would otherwise leave the transaction stuck part-way, and a
    /// part-way transaction is deliberately exempt from expiry: its
    /// keys would stay unwritable for the rest of the term.
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

        tokio::spawn(async move {
            let _guard = guard;
            leader.append_writes(mutations).await
        })
        .await
        .map_err(|e| {
            TxnError::Internal(Status::internal(format!("the commit task failed: {e}")))
        })?
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
        let result = self.conditional_write(request.into_inner()).await;

        Ok(Response::new(WriteResponse {
            result: Some(match result {
                // `append_log` returns only once the entry has committed
                // *and* been applied, so the moment a client sees this
                // seq, a read on this node reflects it.
                Ok(seq) => write_response::Result::Seq(seq),
                Err(e) => write_response::Result::Error(write_err(e)?),
            }),
        }))
    }

    async fn read(&self, request: Request<ReadRequest>) -> Result<Response<ReadResponse>, Status> {
        let req = request.into_inner();

        // A transactional read is a different thing: it takes a lock,
        // and it can be answered from what the transaction itself has
        // buffered. It also cannot be stale -- a stale read is served by
        // whichever node received it, and a node that isn't the leader
        // has neither this transaction nor a lock table to protect it,
        // so it would hand back unprotected data under a transaction's
        // name. That is a malformed request rather than a refusal.
        if let Some(id) = &req.transaction_id {
            let id = txn_id(id)?;
            if !req.linearizable {
                return Err(Status::invalid_argument(
                    "a transactional read cannot be stale; set linearizable",
                ));
            }
            let result = self.transactional_read(id, &req.key).await;
            return Ok(Response::new(ReadResponse {
                result: Some(match result {
                    Ok(ok) => read_response::Result::Ok(ok),
                    Err(e) => read_response::Result::Error(txn_error(e)?),
                }),
            }));
        }

        // A linearizable read has to come from a leader that still holds
        // a quorum of leases: without one it may have been deposed
        // already and simply not know it yet, and would answer from state
        // a newer leader has moved past. It also has to come from one
        // that has caught up on the log it inherited, or it would answer
        // from a gap in the other direction -- a write it has committed
        // but not yet applied. A stale read asks for none of that and is
        // served locally by whoever received it.
        if req.linearizable {
            let held = match self.leader() {
                Ok(leader) => match leader.await_caught_up().await {
                    Ok(()) if leader.has_read_quorum() => Ok(()),
                    Ok(()) => Err(RaftError::NotLeader { leader_hint: None }),
                    Err(e) => Err(e),
                },
                Err(e) => Err(e),
            };
            if let Err(e) = held {
                return Ok(Response::new(ReadResponse {
                    result: Some(read_response::Result::Error(client_err(e)?)),
                }));
            }
        }

        let found = self
            .store
            .run(move |log| log.get_latest(&req.key))
            .await
            .map_err(storage_err)?;

        // A never-written key and a deleted key both read as "no value"
        // (storage's `get_latest` already collapses the two -- see its
        // doc); seq 0 is never assigned (sequencing starts at 1), so it
        // doubles as the "never written" sentinel here.
        let ok = match found {
            Some((seq, value)) => ReadOk {
                seq,
                value: Some(value),
            },
            None => ReadOk {
                seq: 0,
                value: None,
            },
        };

        Ok(Response::new(ReadResponse {
            result: Some(read_response::Result::Ok(ok)),
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

    async fn buffer_write(
        &self,
        request: Request<BufferWriteRequest>,
    ) -> Result<Response<BufferWriteResponse>, Status> {
        let req = request.into_inner();
        let id = txn_id(&req.transaction_id)?;

        let result = self
            .transaction_leader(id)
            .and_then(|_| self.locks.buffer_write(id, &req.key, req.value).map_err(Into::into));

        Ok(Response::new(BufferWriteResponse {
            result: Some(match result {
                Ok(()) => buffer_write_response::Result::Ok(true),
                Err(e) => buffer_write_response::Result::Error(txn_error(e)?),
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
        let req = request.into_inner();

        // Checked before the round trip below, not just inside `propose`:
        // a follower has no business dialing the new server only to find
        // out afterwards that it should have answered `NotLeader` and
        // sent the caller elsewhere. `propose` still re-checks at the
        // point of committing, since leadership can change during the
        // round trip.
        if let Err(e) = require_leader(&self.client_handle.role()) {
            return not_leader_response(e);
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
        let req = request.into_inner();
        self.propose(ConfigChange::RemoveServer {
            server_id: req.server_id,
        })
        .await
    }

    async fn bootstrap(
        &self,
        request: Request<BootstrapRequest>,
    ) -> Result<Response<ConfigChangeResponse>, Status> {
        let req = request.into_inner();
        if !req.server_id.is_empty() && req.server_id != self.identity.server_id {
            return Err(Status::failed_precondition(format!(
                "this node is {}, not {}",
                self.identity.server_id, req.server_id
            )));
        }

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

        Ok(Response::new(ConfigChangeResponse {
            result: Some(config_change_response::Result::Ok(true)),
        }))
    }

    async fn get_config(
        &self,
        _request: Request<GetConfigRequest>,
    ) -> Result<Response<GetConfigResponse>, Status> {
        let members = self.get_config_internal().await.map_err(storage_err)?;

        Ok(Response::new(GetConfigResponse {
            identity: Some(self.identity.clone()),
            members,
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
async fn reassemble_append_entries<S>(mut stream: S, current_term: u64) -> Result<Reassembled, Status>
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
                log::warn!("refused an append_entries stream: {why}");
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
        let install = self.peer_handle.install_snapshot(header, ChunkReader::new(rx));

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
