//! A single Raft node, runnable as a library (`run`) or as the `main`
//! binary (see `main.rs`, which just parses flags into `run`'s
//! arguments). Splitting it this way is what lets an integration test
//! spin up a real node in-process instead of shelling out to the binary.

pub mod config;
pub mod grpc;
pub mod pb;
pub mod raft;
pub mod storage;

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use tonic::transport::Server;

use grpc::{KvService, RaftPeerService};
use pb::raft5::client::kv_server::KvServer;
use pb::raft5::cluster::ClusterMember;
use pb::raft5::raft::raft_peer_server::RaftPeerServer;
use raft::Common;
use storage::RaftLog;

/// Runs one Raft node until its servers exit (normally only on error --
/// there's no graceful shutdown path yet).
///
/// A node started against a fresh `db` directory belongs to no cluster:
/// it answers RPCs and waits, without ever campaigning, until it is
/// either bootstrapped into a single-node cluster (the `Bootstrap` client
/// RPC) or added to an existing one (`AddServer` on that cluster's
/// leader, which replicates the configuration here). Membership lives in
/// the log, so a restart against an existing `db` rejoins whatever
/// cluster this node was already part of -- there is no peer list to
/// keep in sync by hand.
///
/// - `id`: this node's stable identity within the cluster. Must not
///   change across restarts: it is what every other node's log calls
///   this one.
/// - `db`: directory holding this node's database (log, state, and
///   membership). Created if it doesn't exist.
/// - `client_addr`: address to serve the client API on.
/// - `peer_addr`: address to serve the peer-to-peer Raft API on.
/// - `advertise`: URL other nodes should dial to reach this node's peer
///   API. Defaults to `peer_addr`, which is wrong whenever that is a
///   wildcard bind (`0.0.0.0:...`) or behind NAT -- set it explicitly
///   then. Recorded in the cluster configuration at bootstrap.
pub async fn run(
    id: String,
    db: &Path,
    client_addr: SocketAddr,
    peer_addr: SocketAddr,
    advertise: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let log = Arc::new(RaftLog::open(db)?);
    let identity = ClusterMember {
        server_id: id.clone(),
        address: advertise.unwrap_or_else(|| format!("http://{peer_addr}")),
        voting: true,
    };

    // Checked before the node starts, not from `role_rx`: the state
    // machine hasn't published a role yet at this point.
    let uninitialized = log.current_config()?.is_none();

    let (common, _role_rx, peer_handle, _commit_rx, _compact_rx) =
        Common::new(id.clone(), Arc::clone(&log))?;
    let client_handle = common.client_handle();
    tokio::spawn(raft::run(common));

    // KvService's read/write path still bypasses the state machine (see
    // its module doc); `client_handle` is what `Bootstrap` goes through.
    let kv_server = KvServer::new(KvService::new(Arc::clone(&log), identity, client_handle));
    let raft_peer_server = RaftPeerServer::new(RaftPeerService::new(peer_handle));

    if uninitialized {
        println!(
            "node {id} is not part of a cluster yet -- send Bootstrap to form a single-node \
             cluster, or AddServer on an existing cluster's leader to join it"
        );
    }
    println!("client API listening on {client_addr}");
    println!("peer API listening on {peer_addr}");

    let client_fut = Server::builder().add_service(kv_server).serve(client_addr);
    let peer_fut = Server::builder()
        .add_service(raft_peer_server)
        .serve(peer_addr);

    tokio::try_join!(client_fut, peer_fut)?;

    Ok(())
}
