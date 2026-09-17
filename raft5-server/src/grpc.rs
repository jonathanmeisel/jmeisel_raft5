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
//! `RaftPeerService` is the peer-facing half, served on a separate port
//! since the two have different network exposure. It forwards every call
//! straight into a live `raft::RaftPeerHandle` -- whichever role the
//! state machine is currently in decides how to answer. `install_snapshot`
//! reassembles the incoming stream (transport-level work) but, like every
//! other call here, never touches `storage::RaftLog` directly -- the term
//! check and the actual install both happen on the far side of
//! `RaftPeerHandle`, same as everything else.

use tonic::{Request, Response, Status, Streaming};

use crate::pb::raft5::client::kv_server::Kv;
use crate::pb::raft5::client::{
    AddServerRequest, BootstrapRequest, ConfigChangeResponse, GetConfigRequest, GetConfigResponse,
    NotLeader, RaftError as PbRaftError, ReadOk, ReadRequest, ReadResponse, RemoveServerRequest,
    WriteRequest, WriteResponse, config_change_response, raft_error, read_response, write_response,
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
}

impl KvService {
    pub fn new(store: Store, identity: ClusterMember, client_handle: ClientHandle) -> Self {
        Self {
            store,
            identity,
            client_handle,
        }
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
}

#[tonic::async_trait]
impl Kv for KvService {
    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let req = request.into_inner();

        let result = async {
            let leader = require_leader(&self.client_handle.role())?;
            leader.append_log(&req.key, req.value.as_deref()).await
        }
        .await;

        Ok(Response::new(WriteResponse {
            result: Some(match result {
                // `append_log` returns only once the entry has committed
                // *and* been applied, so the moment a client sees this
                // seq, a read on this node reflects it.
                Ok(seq) => write_response::Result::Seq(seq),
                Err(e) => write_response::Result::Error(client_err(e)?),
            }),
        }))
    }

    async fn read(&self, request: Request<ReadRequest>) -> Result<Response<ReadResponse>, Status> {
        let req = request.into_inner();

        // A linearizable read has to come from a leader that still holds
        // a quorum of leases: without one it may have been deposed
        // already and simply not know it yet, and would answer from state
        // a newer leader has moved past. A stale read asks for none of
        // that and is served locally by whoever received it.
        if req.linearizable {
            let held = match require_leader(&self.client_handle.role()) {
                Ok(leader) if leader.has_read_quorum() => Ok(()),
                Ok(_) => Err(RaftError::NotLeader { leader_hint: None }),
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
        // Added as a learner, not a voter: see `ConfigChange::AddServer`.
        // It is promoted automatically once it has caught up, so from a
        // client's point of view this one call is the whole operation.
        self.propose(ConfigChange::AddServer {
            server_id: req.server_id,
            address: req.address,
            client_address: req.client_address,
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
        let bytes = self
            .store
            .run(RaftLog::current_config)
            .await
            .map_err(storage_err)?;

        let members = match bytes {
            Some(bytes) => {
                ClusterConfig::from_bytes(&bytes)
                    .map_err(|e| {
                        Status::internal(format!("stored cluster config was corrupt: {e}"))
                    })?
                    .members
            }
            // Not bootstrapped or added to a cluster yet -- not an error,
            // just an empty membership list.
            None => Vec::new(),
        };

        Ok(Response::new(GetConfigResponse { members }))
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
