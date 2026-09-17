use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;

use jmeisel_raft5::run;

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
    run(
        args.id,
        &args.db,
        args.client_addr,
        args.peer_addr,
        args.advertise,
    )
    .await
}
