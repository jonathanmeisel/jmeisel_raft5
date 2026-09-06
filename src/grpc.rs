//! gRPC service implementations wiring the generated proto types (`pb`)
//! into `storage::RaftLog`.
//!
//! Both services are stand-ins for a real Raft node: there's no leader
//! election, term tracking, or log replication yet, so `KvService` just
//! appends/reads locally (as if this node were always its own sole leader
//! at term 0) and `RaftPeerService::step_messages` is unimplemented since
//! there's no consensus engine to route messages to. `install_snapshot` is
//! wired up for real, since it only needs `storage::RaftLog`.

use std::sync::Arc;

use tonic::{Request, Response, Status, Streaming};

use crate::pb::raft5::client::kv_server::Kv;
use crate::pb::raft5::client::{
    write_response, read_response, AddServerRequest, ConfigChangeResponse, ReadOk, ReadRequest,
    ReadResponse, RemoveServerRequest, WriteRequest, WriteResponse,
};
use crate::pb::raft5::raft::raft_peer_server::RaftPeer;
use crate::pb::raft5::raft::{snapshot_chunk, Ack, InstallSnapshotResponse, RaftMessage, SnapshotChunk};
use crate::storage::{RaftLog, StorageError};

fn storage_err(e: StorageError) -> Status {
    Status::internal(e.to_string())
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
    async fn write(&self, request: Request<WriteRequest>) -> Result<Response<WriteResponse>, Status> {
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
            None => ReadOk { seq: 0, value: None },
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
}

impl RaftPeerService {
    pub fn new(log: Arc<RaftLog>) -> Self {
        Self { log }
    }
}

#[tonic::async_trait]
impl RaftPeer for RaftPeerService {
    async fn step_messages(
        &self,
        _request: Request<Streaming<RaftMessage>>,
    ) -> Result<Response<Ack>, Status> {
        Err(Status::unimplemented(
            "raft consensus engine not yet implemented",
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

    fn open_temp() -> (Arc<RaftLog>, std::path::PathBuf) {
        let mut path = std::env::temp_dir();
        path.push(format!("raft5_grpc_test_{}", std::process::id()));
        path.push(format!("{:x}", rand_suffix()));
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
}
