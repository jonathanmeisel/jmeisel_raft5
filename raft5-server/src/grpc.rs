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

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use raft5_shared::raft5::client::kv_client::KvClient;
use tonic::transport::Endpoint;
use tonic::{Request, Response, Status, Streaming};

use crate::pb::raft5::client::kv_server::Kv;
use crate::pb::raft5::client::{
    AddServerRequest, BootstrapRequest, ConfigChangeResponse, GetConfigRequest, GetConfigResponse,
    NotLeader, RaftError as PbRaftError, ReadOk, ReadRequest, ReadResponse, RemoveServerRequest,
    WriteConflict, WriteRequest, WriteResponse, config_change_response, raft_error, read_response,
    write_response,
};
use crate::pb::raft5::cluster::{ClusterConfig, ClusterMember};
use crate::pb::raft5::raft::raft_peer_server::RaftPeer;
use crate::pb::raft5::raft::{
    AppendEntriesRequest, AppendEntriesResponse, GetLeaseRequest, GetLeaseResponse,
    InstallSnapshotResponse, RequestVoteRequest, RequestVoteResponse, SnapshotChunk,
    snapshot_chunk,
};
use crate::raft::{
    self, ClientHandle, ConfigChange, LeaderHandle, RaftError, RaftPeerHandle, RoleHandle,
};
use crate::storage::{RaftLog, StorageError};
use crate::store::Store;

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

/// One write's exclusive claim on a key, released on drop. Held from
/// before the write's precondition is checked until its entry has been
/// applied -- see the module doc for why that whole span matters.
struct KeyGuard {
    writing: Arc<Mutex<HashSet<Vec<u8>>>>,
    key: Vec<u8>,
}

impl Drop for KeyGuard {
    fn drop(&mut self) {
        self.writing.lock().unwrap().remove(&self.key);
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
    /// Keys with a write in flight on this node. A synchronous mutex
    /// with no `.await` under it, same discipline as
    /// `raft::peer::PeerConnections`.
    writing: Arc<Mutex<HashSet<Vec<u8>>>>,
}

impl KvService {
    pub fn new(store: Store, identity: ClusterMember, client_handle: ClientHandle) -> Self {
        Self {
            store,
            identity,
            client_handle,
            writing: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Claims `key` for one write, or `None` if another write already
    /// holds it. `HashSet::insert` reporting whether it added anything
    /// is the test-and-set: two writes racing here cannot both win.
    fn claim(&self, key: &[u8]) -> Option<KeyGuard> {
        self.writing
            .lock()
            .unwrap()
            .insert(key.to_vec())
            .then(|| KeyGuard {
                writing: Arc::clone(&self.writing),
                key: key.to_vec(),
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
        let leader = require_leader(&self.client_handle.role())?;
        // The precondition is checked against `latest`, so a leader that
        // may still be missing entries has no business checking it.
        // Before the claim rather than after, so that a leader still
        // catching up isn't holding keys no one can write while it does.
        leader.await_caught_up().await?;

        // Claimed before the precondition is read, so that two writes
        // racing here cannot both find the key at the version each is
        // about to supersede.
        let guard = self.claim(&req.key).ok_or(WriteError::Conflict(None))?;

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

    /// Shared body of `add_server`/`remove_server`: both are the same
    /// operation with a different `ConfigChange`, and both answer
    /// `NotLeader` rather than forwarding (Raft §6.2).
    async fn propose(
        &self,
        change: ConfigChange,
    ) -> Result<Response<ConfigChangeResponse>, Status> {
        let result = async {
            let leader = require_leader(&self.client_handle.role())?;
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

        // A linearizable read has to come from a leader that still holds
        // a quorum of leases: without one it may have been deposed
        // already and simply not know it yet, and would answer from state
        // a newer leader has moved past. It also has to come from one
        // that has caught up on the log it inherited, or it would answer
        // from a gap in the other direction -- a write it has committed
        // but not yet applied. A stale read asks for none of that and is
        // served locally by whoever received it.
        if req.linearizable {
            let held = match require_leader(&self.client_handle.role()) {
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

#[tonic::async_trait]
impl RaftPeer for RaftPeerService {
    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let resp = self
            .peer_handle
            .append_entries(request.into_inner())
            .await
            .map_err(raft_err)?;
        Ok(Response::new(resp))
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

        // Reassemble the raw byte stream produced by `export_snapshot`.
        // This buffers the whole snapshot in memory before handing it
        // onward -- fine for getting the plumbing working, but a fully
        // streaming version (a sync `Read` fed by this async loop through
        // a channel, run inside `spawn_blocking`) would avoid that if
        // snapshots get large enough for it to matter.
        let mut buf = Vec::new();
        while let Some(chunk) = stream.message().await? {
            match chunk.part {
                Some(snapshot_chunk::Part::Data(data)) => buf.extend_from_slice(&data),
                _ => return Err(Status::invalid_argument("expected a data chunk")),
            }
        }

        let resp = self
            .peer_handle
            .install_snapshot(header, buf)
            .await
            .map_err(raft_err)?;
        Ok(Response::new(resp))
    }
}

#[cfg(test)]
mod tests;
