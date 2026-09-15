//! gRPC service implementations wiring the generated proto types (`pb`)
//! into `storage::RaftLog`.
//!
//! `KvService` is still a stand-in for a real Raft node: there's no leader
//! election, term tracking, or log replication in its write/read path, so
//! it just appends/reads locally as if this node were always its own sole
//! leader at term 0. `RaftPeerService`, on the other hand, forwards
//! `append_entries`/`request_vote` straight into a live `raft::RaftPeerHandle`
//! -- whichever role the state machine is currently in decides how to
//! answer. `install_snapshot` is wired up for real too, since it only needs
//! `storage::RaftLog`.

use std::sync::Arc;

use tonic::{Request, Response, Status, Streaming};

use crate::pb::raft5::client::kv_server::Kv;
use crate::pb::raft5::client::{
    AddServerRequest, ConfigChangeResponse, ReadOk, ReadRequest, ReadResponse, RemoveServerRequest,
    WriteRequest, WriteResponse, read_response, write_response,
};
use crate::pb::raft5::raft::raft_peer_server::RaftPeer;
use crate::pb::raft5::raft::{
    AppendEntriesRequest, AppendEntriesResponse, GetLeaseRequest, GetLeaseResponse,
    InstallSnapshotResponse, RequestVoteRequest, RequestVoteResponse, SnapshotChunk,
    snapshot_chunk,
};
use crate::raft::{RaftError, RaftPeerHandle};
use crate::storage::{RaftLog, StorageError};

fn storage_err(e: StorageError) -> Status {
    Status::internal(e.to_string())
}

fn raft_err(e: RaftError) -> Status {
    match e {
        // Neither RaftPeerHandle method can actually produce this today --
        // only LeaderHandle::append_log does -- but RaftError is shared, so
        // this match has to be total.
        RaftError::NotLeader { .. } => Status::failed_precondition("not leader"),
        RaftError::Storage(e) => storage_err(e),
        RaftError::Disconnected => Status::unavailable("raft event loop is not running"),
    }
}

fn join_err(e: tokio::task::JoinError) -> Status {
    Status::internal(format!("blocking task panicked: {e}"))
}

pub struct KvService {
    log: Arc<RaftLog>,
}

impl KvService {
    pub fn new(log: Arc<RaftLog>) -> Self {
        Self { log }
    }
}

#[tonic::async_trait]
impl Kv for KvService {
    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let req = request.into_inner();
        let log = Arc::clone(&self.log);

        let seq = tokio::task::spawn_blocking(move || {
            // Placeholder term: there's no leader election/term tracking
            // yet, so every node just appends locally as if it were sole
            // leader at term 0. Replace once real consensus exists.
            log.append(0, &req.key, req.value.as_deref())
        })
        .await
        .map_err(join_err)?
        .map_err(storage_err)?;

        Ok(Response::new(WriteResponse {
            result: Some(write_response::Result::Seq(seq)),
        }))
    }

    async fn read(&self, request: Request<ReadRequest>) -> Result<Response<ReadResponse>, Status> {
        let req = request.into_inner();
        let log = Arc::clone(&self.log);

        let found = tokio::task::spawn_blocking(move || log.get_latest(&req.key))
            .await
            .map_err(join_err)?
            .map_err(storage_err)?;

        // A never-written key and a deleted key both read as "no value";
        // seq 0 is never assigned (sequencing starts at 1), so it doubles
        // as the "never written" sentinel here.
        let ok = match found {
            Some((seq, value)) => ReadOk { seq, value },
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
        _request: Request<AddServerRequest>,
    ) -> Result<Response<ConfigChangeResponse>, Status> {
        Err(Status::unimplemented(
            "cluster membership changes require the raft consensus engine, not yet implemented",
        ))
    }

    async fn remove_server(
        &self,
        _request: Request<RemoveServerRequest>,
    ) -> Result<Response<ConfigChangeResponse>, Status> {
        Err(Status::unimplemented(
            "cluster membership changes require the raft consensus engine, not yet implemented",
        ))
    }
}

pub struct RaftPeerService {
    log: Arc<RaftLog>,
    peer_handle: RaftPeerHandle,
}

impl RaftPeerService {
    pub fn new(log: Arc<RaftLog>, peer_handle: RaftPeerHandle) -> Self {
        Self { log, peer_handle }
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

        // First message must be the header: leader/term metadata for this
        // RPC, distinct from the storage-level header that
        // `RaftLog::install_snapshot` parses out of the reassembled bytes
        // below.
        let first = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("empty snapshot stream"))?;
        match first.part {
            Some(snapshot_chunk::Part::Header(_)) => {}
            _ => return Err(Status::invalid_argument("first chunk must be a header")),
        }

        // Reassemble the raw byte stream produced by `export_snapshot`.
        // This buffers the whole snapshot in memory before handing it to
        // `install_snapshot` -- fine for getting the plumbing working, but
        // a fully streaming version (a sync `Read` fed by this async loop
        // through a channel, run inside `spawn_blocking`) would avoid that
        // if snapshots get large enough for it to matter.
        let mut buf = Vec::new();
        while let Some(chunk) = stream.message().await? {
            match chunk.part {
                Some(snapshot_chunk::Part::Data(data)) => buf.extend_from_slice(&data),
                _ => return Err(Status::invalid_argument("expected a data chunk")),
            }
        }

        let log = Arc::clone(&self.log);
        tokio::task::spawn_blocking(move || log.install_snapshot(&mut buf.as_slice()))
            .await
            .map_err(join_err)?
            .map_err(storage_err)?;

        Ok(Response::new(InstallSnapshotResponse { term: 0 }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::raft5::client::{read_response, write_response};
    use crate::raft;

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

    #[tokio::test]
    async fn write_then_read_round_trips_through_kv_service() {
        let (log, path) = open_temp();
        let svc = KvService::new(log);

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
        assert_eq!(seq, 1);

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
        let svc = KvService::new(log);

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
        let svc = KvService::new(log);

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

    /// End-to-end through the actual tonic service: confirms
    /// `RaftPeerService` is wired to a *running* state machine, not just
    /// that `raft::RaftPeerHandle` works in isolation (covered separately
    /// in `raft`'s own unit tests).
    #[tokio::test]
    async fn append_entries_and_request_vote_reach_the_running_state_machine() {
        let (log, path) = open_temp();
        let (common, role_rx, peer_handle, _commit_rx, _compact_rx) =
            raft::Common::new("node-under-test".to_string(), vec![], Arc::clone(&log)).unwrap();
        tokio::spawn(raft::run(common));

        let svc = RaftPeerService::new(log, peer_handle);

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
