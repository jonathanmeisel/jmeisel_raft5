use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use tracing::info;

use raft5_server::auth::{AuthConfig, InitialAdmin};
use raft5_server::{ClientApiTls, PeerTls, ServerConfig, serve_config};
use tokio_util::sync::CancellationToken;

/// A single Raft node.
///
/// A node started on a new database directory is in no cluster. It answers
/// RPCs and waits, never campaigning, until it is bootstrapped into a
/// single-node cluster (the `Bootstrap` client RPC) or added to an existing
/// one (`AddServer` on that cluster's leader, which replicates the
/// configuration here). Membership lives in the log, so a restart on an
/// existing directory rejoins the cluster this node was in: there is no
/// peer list to keep in sync by hand.
#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// This node's stable identity in the cluster. Must not change across
    /// restarts: every other node's log uses it for this node.
    #[arg(long)]
    id: String,

    /// The directory with this node's database (log, state and membership).
    /// Created if it does not exist.
    #[arg(long, default_value = "./data")]
    db: PathBuf,

    /// The address to serve the client API on.
    #[arg(long, default_value = "127.0.0.1:50051")]
    client_addr: SocketAddr,

    /// The address to serve the peer Raft API on.
    #[arg(long, default_value = "127.0.0.1:50052")]
    peer_addr: SocketAddr,

    /// The URL other nodes dial to reach this node's peer API. Defaults to
    /// `peer_addr`, which is wrong for a wildcard bind (`0.0.0.0:...`) or
    /// behind NAT: set it explicitly then. Recorded in the cluster
    /// configuration at bootstrap.
    #[arg(long)]
    advertise: Option<String>,

    /// The URL clients dial to reach this node's client API. Defaults to
    /// `client_addr`, with the same wildcard and NAT caveat as `advertise`.
    /// Recorded in the cluster configuration at bootstrap; it is how a client
    /// that knows one member finds the others.
    #[arg(long)]
    advertise_client: Option<String>,

    /// A file with the secret this cluster signs tokens with.
    ///
    /// Every node must get the same bytes: the node that receives a request
    /// checks its token, and that is usually not the node that issued it.
    /// Leading and trailing whitespace is removed, so a secret written with
    /// `echo` and one written with `printf` match.
    ///
    /// Without this, the client API asks nobody for a token and keeps no
    /// accounts.
    #[arg(long)]
    jwt_secret_file: Option<PathBuf>,

    /// How long an issued token stays valid, in seconds.
    ///
    /// Also how long a deleted account's existing token keeps working for
    /// normal requests (nothing can withdraw a token), so this is a security
    /// bound, not only a convenience.
    #[arg(long, default_value_t = 3600)]
    token_ttl_secs: u64,

    /// The administrator to create when this node is bootstrapped.
    ///
    /// Also the only account that can log in before the bootstrap, which is
    /// what makes bootstrapping an authenticated cluster possible: `Bootstrap`
    /// needs a token like every other call, and until it succeeds there are no
    /// stored accounts to get one from. Once this node has a configuration,
    /// this password stops working and the stored account takes over.
    #[arg(long, requires = "initial_admin_password_file")]
    initial_admin_user: Option<String>,

    /// A file with that administrator's initial password. Read once, at
    /// startup, and kept only as a hash.
    #[arg(long, requires = "initial_admin_user")]
    initial_admin_password_file: Option<PathBuf>,

    /// The PEM certificate (chain) this node serves the peer API with, and
    /// presents when it dials other nodes. Turns on mTLS for the peer API,
    /// which every node in the cluster must then also use.
    ///
    /// It is both a server and a client certificate, so if it lists extended
    /// key usages it needs `serverAuth` and `clientAuth`, and it needs a
    /// subjectAltName that matches how peers address this node (or
    /// `--peer-tls-server-name`). The common name is ignored.
    ///
    /// Without this, the peer API is plaintext, and anyone who can reach the
    /// port can speak Raft to this node.
    #[arg(long, requires_all = ["peer_tls_key", "peer_tls_ca"])]
    peer_tls_cert: Option<PathBuf>,

    /// The PEM private key for `--peer-tls-cert`.
    #[arg(long, requires = "peer_tls_cert")]
    peer_tls_key: Option<PathBuf>,

    /// The PEM certificate(s) a peer's certificate must chain to. Anything it
    /// vouches for is accepted as a peer.
    ///
    /// For a self-signed certificate shared by the whole cluster, this is that
    /// certificate itself. It must then not be marked `CA:TRUE`, or it is
    /// refused as a CA used as an end entity.
    #[arg(long, requires = "peer_tls_cert")]
    peer_tls_ca: Option<PathBuf>,

    /// Check every peer's certificate against this name instead of the host in
    /// its address. Lets one certificate with one name serve every node,
    /// however each is addressed.
    #[arg(long, requires = "peer_tls_cert")]
    peer_tls_server_name: Option<String>,

    /// The PEM certificate (chain) to serve the client API with. Turns on TLS
    /// for the client API; clients then dial `https://`.
    #[arg(long, requires = "client_tls_key")]
    client_tls_cert: Option<PathBuf>,

    /// The PEM private key for `--client-tls-cert`.
    #[arg(long, requires = "client_tls_cert")]
    client_tls_key: Option<PathBuf>,

    /// The PEM certificate(s) a client's certificate must chain to. Turns TLS
    /// into mTLS: a client without such a certificate cannot connect.
    ///
    /// Only a gate on connecting. A client's token still decides who it is and
    /// what it may do (see `--jwt-secret-file`).
    #[arg(long, requires = "client_tls_cert")]
    client_tls_ca: Option<PathBuf>,
}

/// Build this node's authentication configuration from the flags, or
/// `None` for a node with no secret.
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
    // A shorter HMAC key would be the weakest part of the scheme, and the
    // cluster cannot detect a weak key later.
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

/// This node's peer and client TLS configuration, from the flags.
///
/// Read at startup, so a missing or unreadable file stops the node with
/// its path in the error, instead of showing up later as a handshake every
/// peer refuses.
fn tls_config(args: &Args) -> Result<(Option<PeerTls>, Option<ClientApiTls>), String> {
    use raft5_shared::tls::{load_ca, load_identity};

    let peer = match (&args.peer_tls_cert, &args.peer_tls_key, &args.peer_tls_ca) {
        (Some(cert), Some(key), Some(ca)) => Some(PeerTls {
            identity: load_identity(cert, key)?,
            ca: load_ca(ca)?,
            server_name: args.peer_tls_server_name.clone(),
        }),
        _ => None,
    };
    let client = match (&args.client_tls_cert, &args.client_tls_key) {
        (Some(cert), Some(key)) => Some(ClientApiTls {
            identity: load_identity(cert, key)?,
            client_ca: args.client_tls_ca.as_deref().map(load_ca).transpose()?,
        }),
        _ => None,
    };
    Ok((peer, client))
}

/// The shortest signing secret this server accepts: the output width of
/// the hash that HS256 uses.
const MIN_SECRET_BYTES: usize = 32;

/// `bytes` without leading or trailing ASCII whitespace.
///
/// So a secret file written with `echo` and one written with `printf` are
/// the same secret. A trailing newline that made one node disagree with
/// the others would show up only as tokens that fail to verify for no
/// visible reason.
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
    // `tower_http` gets its own directive: its RPC spans are the main point of
    // this crate's tracing, but they log under their own target, not
    // `raft5_server`, so `raft5_server=info` alone would silently drop all of
    // them.
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
    let (peer_tls, client_tls) = tls_config(&args)?;
    if peer_tls.is_none() {
        info!(
            "no --peer-tls-cert: this node's peer API is plaintext -- anyone who can reach \
             {} can speak Raft to it",
            args.peer_addr
        );
    }
    if client_tls.is_none() {
        info!("no --client-tls-cert: this node's client API is plaintext");
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
            peer_tls,
            client_tls,
        },
        cancel.clone(),
    );

    // Ctrl-C cancels, it does not kill: the node stops accepting requests,
    // finishes the ones in flight, and stops its state machine and every
    // background task before `node` resolves. Awaiting it afterwards keeps the
    // process alive long enough for that.
    tokio::pin!(node);
    tokio::select! {
        result = &mut node => return result,
        _ = tokio::signal::ctrl_c() => {}
    }
    info!("shutting down...");
    cancel.cancel();
    node.await
}
