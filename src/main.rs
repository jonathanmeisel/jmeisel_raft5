mod grpc;
mod pb;
mod raft;
mod storage;

use std::net::SocketAddr;
use std::sync::Arc;

use tonic::transport::Server;

use grpc::{KvService, RaftPeerService};
use pb::raft5::client::kv_server::KvServer;
use pb::raft5::raft::raft_peer_server::RaftPeerServer;
use raft::Common;
use storage::RaftLog;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let log = Arc::new(RaftLog::open("./data")?);

    let client_addr: SocketAddr = "127.0.0.1:50051".parse()?;
    let peer_addr: SocketAddr = "127.0.0.1:50052".parse()?;

    // TODO: real cluster config (node id + peer addresses) instead of a
    // single hardcoded node with no peers -- see raft::PeerInfo /
    // pb::raft5::cluster::ClusterConfig.
    let node_id = "node-1".to_string();
    let (common, _role_rx, peer_handle, _commit_rx, _compact_rx) =
        Common::new(node_id, Vec::new(), Arc::clone(&log))?;
    tokio::spawn(raft::run(common));

    // KvService still bypasses the state machine entirely (see its module
    // doc) -- `_role_rx` isn't threaded in yet.
    let kv_server = KvServer::new(KvService::new(Arc::clone(&log)));
    let raft_peer_server = RaftPeerServer::new(RaftPeerService::new(Arc::clone(&log), peer_handle));

    println!("client API listening on {client_addr}");
    println!("peer API listening on {peer_addr}");

    let client_fut = Server::builder().add_service(kv_server).serve(client_addr);
    let peer_fut = Server::builder()
        .add_service(raft_peer_server)
        .serve(peer_addr);

    tokio::try_join!(client_fut, peer_fut)?;

    Ok(())
}
