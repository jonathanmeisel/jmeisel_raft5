use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use log::info;

use raft5_server::serve;
use tokio_util::sync::CancellationToken;

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

    /// URL clients should dial to reach this node's client API. Defaults
    /// to `client_addr`, with the same wildcard/NAT caveat as
    /// `advertise`. Recorded in the cluster configuration at bootstrap,
    /// and how a client that knows any one member can find the others.
    #[arg(long)]
    advertise_client: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), raft5_server::Error> {
    env_logger::init();
    let args = Args::parse();
    let cancel = CancellationToken::new();
    let node = serve(
        args.id,
        &args.db,
        args.client_addr,
        args.peer_addr,
        args.advertise,
        args.advertise_client,
        cancel.clone(),
    );

    // Ctrl-C cancels rather than kills: the node then stops accepting
    // new requests, finishes the ones in flight, and tears down its
    // state machine and every background task under it before `node`
    // finally resolves. Awaiting it afterwards is what makes the process
    // outlive its own shutdown long enough for that to happen.
    tokio::pin!(node);
    tokio::select! {
        result = &mut node => return result,
        _ = tokio::signal::ctrl_c() => {}
    }
    info!("shutting down...");
    cancel.cancel();
    node.await
}
