mod config;
mod grpc;
mod pb;
mod raft;
mod storage;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tonic::transport::Server;

use grpc::{KvService, RaftPeerService};
use pb::raft5::client::kv_server::KvServer;
use pb::raft5::cluster::ClusterMember;
use pb::raft5::raft::raft_peer_server::RaftPeerServer;
use raft::Common;
use storage::RaftLog;

/// A single Raft node.
///
/// A node started against a fresh database directory belongs to no
/// cluster: it answers RPCs and waits, without ever campaigning, until it
/// is either bootstrapped into a single-node cluster (the `Bootstrap`
/// client RPC) or added to an existing one (`AddServer` on that cluster's
/// leader, which replicates the configuration here). Membership lives in
/// the log, so a restart against an existing directory rejoins whatever
/// cluster this node was already part of -- there is no peer list to keep
/// in sync by hand.
#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// This node's stable identity within the cluster. Must not change
    /// across restarts: it is what every other node's log calls this one.
    #[arg(long)]
    id: String,

    /// Directory holding this node's database (log, state, and
    /// membership). Created if it doesn't exist.
    #[arg(long, default_value = "./data")]
    db: PathBuf,

    /// Address to serve the client API on.
    #[arg(long, default_value = "127.0.0.1:50051")]
    client_addr: SocketAddr,

    /// Address to serve the peer-to-peer Raft API on.
    #[arg(long, default_value = "127.0.0.1:50052")]
    peer_addr: SocketAddr,

    /// URL other nodes should dial to reach this node's peer API.
    /// Defaults to `peer_addr`, which is wrong whenever that is a
    /// wildcard bind (`0.0.0.0:...`) or behind NAT -- set it explicitly
    /// then. Recorded in the cluster configuration at bootstrap.
    #[arg(long)]
    advertise: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let log = Arc::new(RaftLog::open(&args.db)?);
    let identity = ClusterMember {
        server_id: args.id.clone(),
        address: args
            .advertise
            .clone()
            .unwrap_or_else(|| format!("http://{}", args.peer_addr)),
        voting: true,
    };

    // Checked before the node starts, not from `role_rx`: the state
    // machine hasn't published a role yet at this point.
    let uninitialized = log.current_config()?.is_none();

    let (common, _role_rx, peer_handle, _commit_rx, _compact_rx) =
        Common::new(args.id.clone(), Arc::clone(&log))?;
    let client_handle = common.client_handle();
    tokio::spawn(raft::run(common));

    // KvService's read/write path still bypasses the state machine (see
    // its module doc); `client_handle` is what `Bootstrap` goes through.
    let kv_server = KvServer::new(KvService::new(Arc::clone(&log), identity, client_handle));
    let raft_peer_server = RaftPeerServer::new(RaftPeerService::new(peer_handle));

    if uninitialized {
        println!(
            "node {} is not part of a cluster yet -- send Bootstrap to form a single-node \
             cluster, or AddServer on an existing cluster's leader to join it",
            args.id
        );
    }
    println!("client API listening on {}", args.client_addr);
    println!("peer API listening on {}", args.peer_addr);

    let client_fut = Server::builder()
        .add_service(kv_server)
        .serve(args.client_addr);
    let peer_fut = Server::builder()
        .add_service(raft_peer_server)
        .serve(args.peer_addr);

    tokio::try_join!(client_fut, peer_fut)?;

    Ok(())
}
