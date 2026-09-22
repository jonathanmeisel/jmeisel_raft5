use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use tracing::info;

use raft5_server::auth::{AuthConfig, InitialAdmin};
use raft5_server::{ServerConfig, serve_config};
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

    /// File holding the secret this cluster signs tokens with.
    ///
    /// Every node must be given the same bytes: a token is checked by
    /// whichever node receives the request, which is not generally the
    /// one that issued it. Leading and trailing whitespace is stripped,
    /// so a secret written with `echo` and one written with `printf`
    /// agree.
    ///
    /// Without this the client API requires no token from anyone and
    /// keeps no accounts, which is how this server behaved before
    /// authentication existed.
    #[arg(long)]
    jwt_secret_file: Option<PathBuf>,

    /// How long an issued token stays valid, in seconds.
    ///
    /// Also how long a deleted account's existing token goes on working
    /// for ordinary requests -- nothing can un-issue one -- so it is the
    /// bound on that, not just a convenience.
    #[arg(long, default_value_t = 3600)]
    token_ttl_secs: u64,

    /// The administrator to create when this node is bootstrapped.
    ///
    /// Also the only account that can log in *before* that happens,
    /// which is what makes bootstrapping an authenticated cluster
    /// possible at all: `Bootstrap` needs a token like every other call,
    /// and until it succeeds there are no stored accounts to get one
    /// from. Once this node has a configuration, this password stops
    /// working and the stored account takes over.
    #[arg(long, requires = "initial_admin_password_file")]
    initial_admin_user: Option<String>,

    /// File holding that administrator's initial password. Read once, at
    /// startup, and kept only as a hash.
    #[arg(long, requires = "initial_admin_user")]
    initial_admin_password_file: Option<PathBuf>,
}

/// Builds this node's authentication configuration from the flags, or
/// `None` for a node that was given no secret.
async fn auth_config(args: &Args) -> Result<Option<AuthConfig>, raft5_server::Error> {
    let Some(path) = args.jwt_secret_file.as_ref() else {
        if args.initial_admin_user.is_some() {
            return Err(
                "--initial-admin-user needs --jwt-secret-file: without a secret this \
                        node keeps no accounts and can sign nothing"
                    .into(),
            );
        }
        return Ok(None);
    };

    let raw = std::fs::read(path).map_err(|e| format!("could not read {}: {e}", path.display()))?;
    let secret = trimmed(&raw);
    // Short of this an HMAC key is the weakest part of the scheme, and
    // a weak key is not something the cluster can notice later.
    if secret.len() < MIN_SECRET_BYTES {
        return Err(format!(
            "the signing secret in {} is {} bytes; at least {MIN_SECRET_BYTES} are needed",
            path.display(),
            secret.len()
        )
        .into());
    }

    let initial_admin = match (&args.initial_admin_user, &args.initial_admin_password_file) {
        (Some(username), Some(path)) => {
            let password = std::fs::read_to_string(path)
                .map_err(|e| format!("could not read {}: {e}", path.display()))?;
            Some(InitialAdmin {
                username: username.clone(),
                password_hash: raft5_server::auth::hash_password(
                    password.trim_end_matches(['\n', '\r']).to_string(),
                )
                .await?,
            })
        }
        _ => None,
    };

    Ok(Some(AuthConfig::new(
        secret,
        Duration::from_secs(args.token_ttl_secs),
        initial_admin,
    )))
}

/// The shortest signing secret this server will accept, matching the
/// output width of the hash HS256 is built on.
const MIN_SECRET_BYTES: usize = 32;

/// `bytes` without leading or trailing ASCII whitespace.
///
/// So that a secret file written with `echo` and one written with
/// `printf` are the same secret -- a trailing newline that made one node
/// disagree with the others would show up only as tokens mysteriously
/// not verifying.
fn trimmed(bytes: &[u8]) -> &[u8] {
    let start = bytes
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    let end = bytes
        .iter()
        .rposition(|b| !b.is_ascii_whitespace())
        .map_or(start, |i| i + 1);
    &bytes[start..end]
}

#[tokio::main]
async fn main() -> Result<(), raft5_server::Error> {
    // `tower_http` gets its own directive: its RPC spans are the whole
    // point of this crate's tracing setup, but they log under its own
    // target rather than `raft5_server`, so a bare `raft5_server=info`
    // here would silently drop every one of them.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "raft5_server=info,tower_http=info".into()),
        )
        .init();
    let args = Args::parse();
    let auth = auth_config(&args).await?;
    if auth.is_none() {
        info!(
            "no --jwt-secret-file: this node's client API requires no token from anyone -- \
             every request it can serve, it will serve"
        );
    }
    let cancel = CancellationToken::new();
    let node = serve_config(
        ServerConfig {
            id: args.id,
            db: args.db,
            client_addr: args.client_addr,
            peer_addr: args.peer_addr,
            advertise: args.advertise,
            advertise_client: args.advertise_client,
            auth,
        },
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
