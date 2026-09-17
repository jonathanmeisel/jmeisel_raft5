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

use std::sync::Arc;

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

fn join_err(e: tokio::task::JoinError) -> Status {
    Status::internal(format!("blocking task panicked: {e}"))
}

pub struct KvService {
    log: Arc<RaftLog>,
    /// How this node's own identity reaches `Bootstrap` -- the
    /// configuration it forms names itself, and only this node knows what
    /// it is called and where its peers should dial it.
    identity: ClusterMember,
    client_handle: ClientHandle,
}

impl KvService {
    pub fn new(log: Arc<RaftLog>, identity: ClusterMember, client_handle: ClientHandle) -> Self {
        Self {
            log,
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

        let log = Arc::clone(&self.log);

        let found = tokio::task::spawn_blocking(move || log.get_latest(&req.key))
            .await
            .map_err(join_err)?
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
        let log = Arc::clone(&self.log);
        let bytes = tokio::task::spawn_blocking(move || log.current_config())
            .await
            .map_err(join_err)?
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
mod tests {
    use super::*;
    use crate::pb::raft5::client::{read_response, write_response};
    use crate::raft::{self, RoleHandle};
    use tokio::sync::watch;
    use tokio_util::sync::CancellationToken;

    fn open_temp() -> (Arc<RaftLog>, std::path::PathBuf) {
        // A nanosecond timestamp alone isn't a reliable unique suffix under
        // parallel test execution -- two tests starting at nearly the same
        // instant can land on the same value (clock resolution, scheduling)
        // and collide on the same directory, so a process-wide counter is
        // layered on top (as a second path segment, not combined into the
        // same u64, since combining risks collisions of its own) to
        // guarantee uniqueness regardless.
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let mut path = std::env::temp_dir();
        path.push(format!("raft5_grpc_test_{}", std::process::id()));
        let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
        path.push(format!("{:x}_{counter:x}", rand_suffix()));
        let log = Arc::new(RaftLog::open(&path).expect("open"));
        (log, path)
    }

    fn rand_suffix() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }

    fn cleanup(path: &std::path::Path) {
        let _ = std::fs::remove_dir_all(path);
    }

    /// A `KvService` on a node that has been bootstrapped into a
    /// single-node cluster and has won its election -- i.e. one that can
    /// actually serve a write.
    async fn leader_kv_service(log: Arc<RaftLog>, node_id: &str) -> KvService {
        let (svc, mut role_rx) = kv_service(log, node_id);
        svc.bootstrap(Request::new(BootstrapRequest::default()))
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !matches!(&*role_rx.borrow(), RoleHandle::Leader(_)) {
                role_rx.changed().await.unwrap();
            }
        })
        .await
        .expect("a bootstrapped single-node cluster should elect itself");
        svc
    }

    /// A `KvService` on top of a running state machine, plus the handles
    /// a test needs to watch it.
    fn kv_service(log: Arc<RaftLog>, node_id: &str) -> (KvService, watch::Receiver<RoleHandle>) {
        let (common, role_rx, _peer_handle, _commit_rx, _compact_rx) = raft::Common::new(
            node_id.to_string(),
            Arc::clone(&log),
            CancellationToken::new(),
        )
        .unwrap();
        let client_handle = common.client_handle();
        tokio::spawn(raft::run(common));
        let identity = ClusterMember {
            server_id: node_id.to_string(),
            address: format!("http://127.0.0.1:1/{node_id}"),
            voting: true,
        };
        (KvService::new(log, identity, client_handle), role_rx)
    }

    #[tokio::test]
    async fn write_then_read_round_trips_through_kv_service() {
        let (log, path) = open_temp();
        let svc = leader_kv_service(log, "kv-test").await;

        let write_resp = svc
            .write(Request::new(WriteRequest {
                key: b"hello".to_vec(),
                value: Some(b"world".to_vec()),
            }))
            .await
            .unwrap()
            .into_inner();
        let seq = match write_resp.result {
            Some(write_response::Result::Seq(seq)) => seq,
            other => panic!("expected Seq, got {other:?}"),
        };
        assert_eq!(
            seq, 3,
            "seq 1 is the bootstrap config entry and seq 2 the leader's no-op"
        );

        let read_resp = svc
            .read(Request::new(ReadRequest {
                key: b"hello".to_vec(),
                linearizable: false,
            }))
            .await
            .unwrap()
            .into_inner();
        match read_resp.result {
            Some(read_response::Result::Ok(ok)) => {
                assert_eq!(ok.seq, seq);
                assert_eq!(ok.value, Some(b"world".to_vec()));
            }
            other => panic!("expected Ok, got {other:?}"),
        }

        cleanup(&path);
    }

    #[tokio::test]
    async fn write_with_no_value_deletes_and_read_reflects_it() {
        let (log, path) = open_temp();
        let svc = leader_kv_service(log, "kv-test").await;

        svc.write(Request::new(WriteRequest {
            key: b"k".to_vec(),
            value: Some(b"v".to_vec()),
        }))
        .await
        .unwrap();

        svc.write(Request::new(WriteRequest {
            key: b"k".to_vec(),
            value: None,
        }))
        .await
        .unwrap();

        let read_resp = svc
            .read(Request::new(ReadRequest {
                key: b"k".to_vec(),
                linearizable: false,
            }))
            .await
            .unwrap()
            .into_inner();
        match read_resp.result {
            Some(read_response::Result::Ok(ok)) => assert_eq!(ok.value, None),
            other => panic!("expected Ok, got {other:?}"),
        }

        cleanup(&path);
    }

    #[tokio::test]
    async fn read_of_never_written_key_returns_no_value() {
        let (log, path) = open_temp();
        let svc = leader_kv_service(log, "kv-test").await;

        let read_resp = svc
            .read(Request::new(ReadRequest {
                key: b"missing".to_vec(),
                linearizable: false,
            }))
            .await
            .unwrap()
            .into_inner();
        match read_resp.result {
            Some(read_response::Result::Ok(ok)) => {
                assert_eq!(ok.seq, 0);
                assert_eq!(ok.value, None);
            }
            other => panic!("expected Ok, got {other:?}"),
        }

        cleanup(&path);
    }

    /// A node that isn't the leader answers with a structured
    /// `NotLeader` rather than forwarding (Raft §6.2), so the client
    /// retries against the leader directly and exactly one node does the
    /// work.
    #[tokio::test]
    async fn writes_to_a_non_leader_come_back_as_not_leader() {
        let (log, path) = open_temp();
        let (svc, _role_rx) = kv_service(log, "bystander");

        let resp = svc
            .write(Request::new(WriteRequest {
                key: b"k".to_vec(),
                value: Some(b"v".to_vec()),
            }))
            .await
            .unwrap()
            .into_inner();
        match resp.result {
            Some(write_response::Result::Error(e)) => {
                assert!(matches!(e.kind, Some(raft_error::Kind::NotLeader(_))));
            }
            other => panic!("expected NotLeader, got {other:?}"),
        }

        // Membership changes take the same route.
        let add = svc
            .add_server(Request::new(AddServerRequest {
                server_id: "new".to_string(),
                address: "http://127.0.0.1:9999".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(matches!(
            add.result,
            Some(config_change_response::Result::Error(_))
        ));

        cleanup(&path);
    }

    /// A stale read is served by whoever receives it, leader or not --
    /// that is what asking for a stale read means.
    #[tokio::test]
    async fn a_stale_read_is_served_without_a_leader() {
        let (log, path) = open_temp();
        let (svc, _role_rx) = kv_service(log, "bystander");

        let resp = svc
            .read(Request::new(ReadRequest {
                key: b"k".to_vec(),
                linearizable: false,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(matches!(resp.result, Some(read_response::Result::Ok(_))));

        // A linearizable one is refused, since this node can't show that
        // its state is current.
        let strict = svc
            .read(Request::new(ReadRequest {
                key: b"k".to_vec(),
                linearizable: true,
            }))
            .await
            .unwrap()
            .into_inner();
        match strict.result {
            Some(read_response::Result::Error(e)) => {
                assert!(matches!(e.kind, Some(raft_error::Kind::NotLeader(_))));
            }
            other => panic!("expected NotLeader, got {other:?}"),
        }

        cleanup(&path);
    }

    /// The whole add path through the client API: one call, and the new
    /// server is a member of the configuration in the leader's log.
    #[tokio::test]
    async fn add_server_then_remove_server_round_trip_through_the_kv_service() {
        let (log, path) = open_temp();
        let svc = leader_kv_service(Arc::clone(&log), "solo").await;

        let resp = svc
            .add_server(Request::new(AddServerRequest {
                server_id: "new".to_string(),
                address: "http://127.0.0.1:9999".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(matches!(
            resp.result,
            Some(config_change_response::Result::Ok(true))
        ));

        let config = ClusterConfig::from_bytes(&log.current_config().unwrap().unwrap()).unwrap();
        assert!(config.contains("new"));
        assert!(!config.is_voter("new"), "added as a learner");

        // "new" is unreachable, so it never catches up and stays a
        // learner -- which means the cluster still commits on "solo"
        // alone, and can therefore also remove it again.
        let removed = svc
            .remove_server(Request::new(RemoveServerRequest {
                server_id: "new".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(matches!(
            removed.result,
            Some(config_change_response::Result::Ok(true))
        ));
        let config = ClusterConfig::from_bytes(&log.current_config().unwrap().unwrap()).unwrap();
        assert!(!config.contains("new"));

        cleanup(&path);
    }

    /// The operator-facing half of bootstrapping: the node names *itself*
    /// in the configuration it forms, so a client doesn't have to know
    /// (or be able to lie about) what address this node advertises.
    #[tokio::test]
    async fn bootstrap_forms_a_cluster_of_this_node_alone() {
        let (log, path) = open_temp();
        let (svc, mut role_rx) = kv_service(Arc::clone(&log), "solo");

        let resp = svc
            .bootstrap(Request::new(BootstrapRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert!(matches!(
            resp.result,
            Some(config_change_response::Result::Ok(true))
        ));

        let config = ClusterConfig::from_bytes(&log.current_config().unwrap().unwrap()).unwrap();
        assert_eq!(config.members.len(), 1);
        assert_eq!(config.members[0].server_id, "solo");
        assert!(config.members[0].voting);

        // A single-node cluster has no one to lose an election to.
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !matches!(&*role_rx.borrow(), RoleHandle::Leader(_)) {
                role_rx.changed().await.unwrap();
            }
        })
        .await
        .expect("a bootstrapped single-node cluster should elect itself");

        cleanup(&path);
    }

    /// `GetConfig` reflects membership before it commits (Raft §4.1 --
    /// activation on append), and reports an empty cluster rather than an
    /// error for a node that hasn't been bootstrapped yet.
    #[tokio::test]
    async fn get_config_reports_empty_before_bootstrap_and_the_new_member_right_after_add() {
        let (log, path) = open_temp();
        let svc = leader_kv_service(Arc::clone(&log), "solo").await;

        let members = svc
            .get_config(Request::new(GetConfigRequest {}))
            .await
            .unwrap()
            .into_inner()
            .members;
        assert_eq!(
            members
                .iter()
                .map(|m| m.server_id.as_str())
                .collect::<Vec<_>>(),
            vec!["solo"]
        );

        svc.add_server(Request::new(AddServerRequest {
            server_id: "new".to_string(),
            address: "http://127.0.0.1:9999".to_string(),
        }))
        .await
        .unwrap();

        let members = svc
            .get_config(Request::new(GetConfigRequest {}))
            .await
            .unwrap()
            .into_inner()
            .members;
        let new_member = members
            .iter()
            .find(|m| m.server_id == "new")
            .expect("new member should be visible");
        assert!(!new_member.voting, "added as a learner");

        cleanup(&path);
    }

    /// Bootstrapping is irreversible, so a client that names the wrong
    /// node is told so rather than silently initializing the one it
    /// happened to reach.
    #[tokio::test]
    async fn bootstrap_refuses_when_the_named_node_is_someone_else() {
        let (log, path) = open_temp();
        let (svc, _role_rx) = kv_service(Arc::clone(&log), "solo");

        let err = svc
            .bootstrap(Request::new(BootstrapRequest {
                server_id: "some-other-node".to_string(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(log.current_config().unwrap().is_none());

        cleanup(&path);
    }

    /// End-to-end through the actual tonic service: confirms
    /// `RaftPeerService` is wired to a *running* state machine, not just
    /// that `raft::RaftPeerHandle` works in isolation (covered separately
    /// in `raft`'s own unit tests).
    #[tokio::test]
    async fn append_entries_and_request_vote_reach_the_running_state_machine() {
        let (log, path) = open_temp();
        let (common, role_rx, peer_handle, _commit_rx, _compact_rx) = raft::Common::new(
            "node-under-test".to_string(),
            Arc::clone(&log),
            CancellationToken::new(),
        )
        .unwrap();
        tokio::spawn(raft::run(common));

        let svc = RaftPeerService::new(peer_handle);

        let append_resp = svc
            .append_entries(Request::new(AppendEntriesRequest {
                term: 7,
                leader_id: "leader-1".to_string(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
                compact_seq: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(append_resp.term, 7);
        assert!(append_resp.success);

        // The AppendEntries above just renewed this follower's lease on
        // leader-1, so it'll refuse any vote request until that lease
        // naturally expires -- and since this node's own candidacy timer
        // shares that exact deadline, it may fire first instead (see
        // raft::follower's `wait_for_vote_grant_or_role_change` for the
        // full reasoning). Retry until either happens.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        let vote_resp = loop {
            if !matches!(&*role_rx.borrow(), raft::RoleHandle::Follower(_)) {
                break None; // moved on to campaigning -- also proves the point
            }
            let resp = svc
                .request_vote(Request::new(RequestVoteRequest {
                    term: 7,
                    candidate_id: "candidate-1".to_string(),
                    last_log_index: 0,
                    last_log_term: 0,
                    dry_run: false,
                }))
                .await
                .unwrap()
                .into_inner();
            if resp.vote_granted {
                break Some(resp);
            }
            assert!(
                std::time::Instant::now() < deadline,
                "neither granted the vote nor moved on to campaigning within the deadline"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        };

        if let Some(vote_resp) = vote_resp {
            assert!(vote_resp.vote_granted);
            assert_eq!(vote_resp.term, 7);
        }

        cleanup(&path);
    }
}
