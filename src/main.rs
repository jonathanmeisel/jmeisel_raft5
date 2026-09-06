mod grpc;
mod pb;
mod storage;

use std::net::SocketAddr;
use std::sync::Arc;

use tonic::transport::Server;

use grpc::{KvService, RaftPeerService};
use pb::raft5::client::kv_server::KvServer;
use pb::raft5::raft::raft_peer_server::RaftPeerServer;
use storage::RaftLog;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let log = Arc::new(RaftLog::open("./data")?);

    let client_addr: SocketAddr = "127.0.0.1:50051".parse()?;
    let peer_addr: SocketAddr = "127.0.0.1:50052".parse()?;

    let kv_server = KvServer::new(KvService::new(Arc::clone(&log)));
    let raft_peer_server = RaftPeerServer::new(RaftPeerService::new(Arc::clone(&log)));

    println!("client API listening on {client_addr}");
    println!("peer API listening on {peer_addr}");

    let client_fut = Server::builder().add_service(kv_server).serve(client_addr);
    let peer_fut = Server::builder()
        .add_service(raft_peer_server)
        .serve(peer_addr);

    tokio::try_join!(client_fut, peer_fut)?;

    Ok(())
}
