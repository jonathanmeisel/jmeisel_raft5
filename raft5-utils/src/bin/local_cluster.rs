//! Runs a whole raft5 cluster on this machine, in one process.
//!
//! Everything the cluster needs lives under one directory: each node's
//! database, the certificates, the signing secret, the administrator's
//! password, and `cluster.json`, which records the ports. Pointed at an
//! empty or missing directory this bootstraps a new cluster there;
//! pointed at one it bootstrapped before, it starts that cluster again.
//!
//! The ports are chosen once, at bootstrap, and reused on every restart
//! after: each node's peer address is recorded in the cluster
//! configuration in the Raft log, so a node that came back on a different
//! port would be unreachable to the others.
//!
//! The cluster runs with mTLS between peers, TLS on the client API, and
//! token authentication, all from material minted at bootstrap.

use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use clap::Parser;
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::info;

use raft5_client::{Client, ClientTls};
use raft5_server::auth::{AuthConfig, InitialAdmin, hash_password};
use raft5_server::{ClientApiTls, PeerTls, ServerConfig, serve_config};
use raft5_shared::tls::{load_ca, load_identity};

type Error = Box<dyn std::error::Error + Send + Sync>;

/// Starts a local raft5 cluster, bootstrapping it first if need be.
#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// Directory holding the cluster's databases, certificates and
    /// secrets. A missing or empty one gets a new cluster; one this
    /// tool bootstrapped before has that cluster started again.
    dir: PathBuf,
    /// Heartbeat interval for a new cluster, in milliseconds (default:
    /// the server's, 50). Ignored when starting an existing cluster,
    /// whose heartbeat is part of its replicated configuration --
    /// change that one with the CLI's `heartbeat <ms>`.
    #[arg(long)]
    heartbeat_ms: Option<u64>,
}

/// How many nodes a new cluster gets: the fewest that survive losing one.
const NODES: usize = 3;

/// The administrator created at bootstrap.
const ADMIN_USER: &str = "admin";

/// How long issued tokens last.
const TOKEN_TTL: Duration = Duration::from_secs(3600);

/// How long bootstrapping waits for the first node to start answering.
const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(10);

/// Names the node certificate covers. Every node is on loopback, so one
/// certificate serves them all.
const CERT_NAMES: &[&str] = &["localhost", "127.0.0.1"];

const MANIFEST: &str = "cluster.json";
const CA_CERT: &str = "certs/ca.pem";
const NODE_CERT: &str = "certs/node.pem";
const NODE_KEY: &str = "certs/node.key";
const JWT_SECRET: &str = "jwt_secret";
const ADMIN_PASSWORD: &str = "admin_password";

/// What has to stay the same across restarts, written to `cluster.json`.
///
/// Written only once bootstrapping has fully succeeded, so its presence
/// is what marks a directory as holding a working cluster -- one with
/// files in it but no manifest is a bootstrap that was interrupted.
#[derive(Serialize, Deserialize)]
struct Manifest {
    admin_user: String,
    nodes: Vec<NodeSpec>,
}

#[derive(Serialize, Deserialize)]
struct NodeSpec {
    id: String,
    client_port: u16,
    peer_port: u16,
}

impl NodeSpec {
    fn client_addr(&self) -> SocketAddr {
        (Ipv4Addr::LOCALHOST, self.client_port).into()
    }

    fn peer_addr(&self) -> SocketAddr {
        (Ipv4Addr::LOCALHOST, self.peer_port).into()
    }

    fn db(&self, dir: &Path) -> PathBuf {
        dir.join("nodes").join(&self.id)
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "raft5_server=info,local_cluster=info".into()),
        )
        .init();
    let args = Args::parse();
    let dir = args.dir;

    let fresh = is_empty_or_missing(&dir)?;
    if !fresh && args.heartbeat_ms.is_some() {
        tracing::warn!(
            "--heartbeat-ms only applies to a new cluster; {} already has one",
            dir.display()
        );
    }
    let manifest = if fresh {
        info!(
            "{} is empty -- bootstrapping a new cluster there",
            dir.display()
        );
        create_cluster_files(&dir)?
    } else {
        read_manifest(&dir)?
    };

    let cancel = CancellationToken::new();
    let mut nodes = tokio::task::JoinSet::new();
    let auth = auth_config(&dir, &manifest.admin_user).await?;
    let (peer_tls, client_tls) = tls_config(&dir)?;
    for node in &manifest.nodes {
        let config = ServerConfig {
            id: node.id.clone(),
            db: node.db(&dir),
            client_addr: node.client_addr(),
            peer_addr: node.peer_addr(),
            advertise: None,
            advertise_client: None,
            auth: Some(auth.clone()),
            peer_tls: Some(peer_tls.clone()),
            client_tls: Some(client_tls.clone()),
        };
        let id = node.id.clone();
        let cancel = cancel.clone();
        nodes.spawn(async move {
            serve_config(config, cancel)
                .await
                .map_err(|e| format!("node {id}: {e}"))
        });
    }

    if fresh {
        let formed = tokio::select! {
            result = form_cluster(&dir, &manifest, &auth, args.heartbeat_ms) => result,
            // A node that fails to start (a port taken in the meantime,
            // say) would otherwise leave bootstrapping to time out.
            Some(Ok(Err(e))) = nodes.join_next() => Err(e.into()),
        };
        if let Err(e) = formed {
            cancel.cancel();
            while nodes.join_next().await.is_some() {}
            return Err(format!(
                "bootstrapping failed: {e}\n{} is left half-made; delete it before trying again",
                dir.display()
            )
            .into());
        }
        write_manifest(&dir, &manifest)?;
    }
    print_connection_info(&dir, &manifest);

    // Ctrl-C cancels rather than kills, so each node drains and releases
    // its database before the process exits -- see `serve_config`.
    let mut failure = None;
    tokio::select! {
        Some(result) = nodes.join_next() => failure = node_failure(result),
        _ = tokio::signal::ctrl_c() => info!("shutting down..."),
    }
    cancel.cancel();
    while let Some(result) = nodes.join_next().await {
        failure = failure.or(node_failure(result));
    }
    match failure {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

fn node_failure(result: Result<Result<(), String>, tokio::task::JoinError>) -> Option<Error> {
    match result {
        Ok(Ok(())) => None,
        Ok(Err(e)) => Some(e.into()),
        Err(e) => Some(e.into()),
    }
}

fn is_empty_or_missing(dir: &Path) -> Result<bool, Error> {
    match std::fs::read_dir(dir) {
        Ok(mut entries) => Ok(entries.next().is_none()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(e) => Err(format!("could not read {}: {e}", dir.display()).into()),
    }
}

fn read_manifest(dir: &Path) -> Result<Manifest, Error> {
    let path = dir.join(MANIFEST);
    let bytes = std::fs::read(&path).map_err(|e| {
        format!(
            "{} is not empty but has no readable {MANIFEST} ({e}) -- either it is not a \
             cluster directory, or bootstrapping it was interrupted and it should be deleted",
            dir.display()
        )
    })?;
    Ok(serde_json::from_slice(&bytes)
        .map_err(|e| format!("could not parse {}: {e}", path.display()))?)
}

/// Written to a temporary file and renamed into place, so an interrupted
/// write can't leave a manifest that parses as something it isn't.
fn write_manifest(dir: &Path, manifest: &Manifest) -> Result<(), Error> {
    let tmp = dir.join(format!("{MANIFEST}.tmp"));
    std::fs::write(&tmp, serde_json::to_vec_pretty(manifest)?)?;
    std::fs::rename(&tmp, dir.join(MANIFEST))?;
    Ok(())
}

/// Picks the ports and mints the certificates and secrets for a new
/// cluster in `dir`. The manifest itself is left for after the cluster
/// has formed.
fn create_cluster_files(dir: &Path) -> Result<Manifest, Error> {
    std::fs::create_dir_all(dir.join("certs"))?;

    let ports = free_ports(NODES * 2)?;
    let nodes = (0..NODES)
        .map(|i| NodeSpec {
            id: format!("n{}", i + 1),
            client_port: ports[2 * i],
            peer_port: ports[2 * i + 1],
        })
        .collect();

    let (ca, cert, key) = mint_certificates()?;
    write_private(&dir.join(CA_CERT), ca.as_bytes())?;
    write_private(&dir.join(NODE_CERT), cert.as_bytes())?;
    write_private(&dir.join(NODE_KEY), key.as_bytes())?;
    write_private(&dir.join(JWT_SECRET), random_string()?.as_bytes())?;
    write_private(&dir.join(ADMIN_PASSWORD), random_string()?.as_bytes())?;

    Ok(Manifest {
        admin_user: ADMIN_USER.to_string(),
        nodes,
    })
}

/// `count` distinct ports the OS considers free right now.
///
/// Every listener is held until all have been picked, so the OS can't
/// hand the same port out twice. They are released before the nodes
/// bind them, so something else could still take one in between -- in
/// which case that node fails to start and bootstrapping reports it.
fn free_ports(count: usize) -> Result<Vec<u16>, Error> {
    let listeners = (0..count)
        .map(|_| TcpListener::bind((Ipv4Addr::LOCALHOST, 0)))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(listeners
        .iter()
        .map(|l| l.local_addr().map(|a| a.port()))
        .collect::<Result<_, _>>()?)
}

/// A private CA, and one certificate it signs for every node to share:
/// `(ca_pem, node_cert_pem, node_key_pem)`.
///
/// The node certificate is both a server and a client certificate, since
/// a peer is both. The CA's key is thrown away once it has signed.
fn mint_certificates() -> Result<(String, String, String), Error> {
    let ca_key = KeyPair::generate()?;
    let mut ca_params = CertificateParams::new(Vec::<String>::new())?;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca_pem = ca_params.self_signed(&ca_key)?.pem();
    let issuer = Issuer::new(ca_params, ca_key);

    let node_key = KeyPair::generate()?;
    let mut node_params =
        CertificateParams::new(CERT_NAMES.iter().map(|n| n.to_string()).collect::<Vec<_>>())?;
    node_params.is_ca = IsCa::ExplicitNoCa;
    node_params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let node_cert = node_params.signed_by(&node_key, &issuer)?;
    Ok((ca_pem, node_cert.pem(), node_key.serialize_pem()))
}

/// 32 random bytes, as URL-safe base64: long enough for a signing
/// secret, and pasteable as a password.
fn random_string() -> Result<String, Error> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).map_err(|e| format!("could not get random bytes: {e}"))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

/// Writes a file only its owner can read: everything here is either a
/// secret or sits next to one.
fn write_private(path: &Path, contents: &[u8]) -> Result<(), Error> {
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    options
        .open(path)
        .and_then(|mut f| f.write_all(contents))
        .map_err(|e| format!("could not write {}: {e}", path.display()).into())
}

fn read_trimmed(path: &Path) -> Result<String, Error> {
    std::fs::read_to_string(path)
        .map(|s| s.trim().to_string())
        .map_err(|e| format!("could not read {}: {e}", path.display()).into())
}

/// The same for every node: they must all share the signing secret.
///
/// The initial administrator is passed on every start, not just the
/// first. It only matters until the cluster has a configuration, after
/// which the account stored at bootstrap is the one that counts.
async fn auth_config(dir: &Path, admin_user: &str) -> Result<AuthConfig, Error> {
    let secret = read_trimmed(&dir.join(JWT_SECRET))?;
    let password = read_trimmed(&dir.join(ADMIN_PASSWORD))?;
    Ok(AuthConfig::new(
        secret.as_bytes(),
        TOKEN_TTL,
        Some(InitialAdmin {
            username: admin_user.to_string(),
            password_hash: hash_password(password).await?,
        }),
    ))
}

fn tls_config(dir: &Path) -> Result<(PeerTls, ClientApiTls), Error> {
    let identity = load_identity(&dir.join(NODE_CERT), &dir.join(NODE_KEY))?;
    let ca = load_ca(&dir.join(CA_CERT))?;
    Ok((
        PeerTls {
            identity: identity.clone(),
            ca,
            server_name: None,
        },
        ClientApiTls {
            identity,
            client_ca: None,
        },
    ))
}

fn client_url(node: &NodeSpec) -> String {
    format!("https://{}", node.client_addr())
}

/// Bootstraps the first node into a single-node cluster, then adds the
/// rest to it.
async fn form_cluster(
    dir: &Path,
    manifest: &Manifest,
    auth: &AuthConfig,
    heartbeat_ms: Option<u64>,
) -> Result<(), Error> {
    let first = &manifest.nodes[0];
    let client = Client::with_tls(
        [client_url(first)],
        ClientTls::new(load_ca(&dir.join(CA_CERT))?),
    )?;
    // Signed here rather than fetched with `login`, which this process
    // could do only slowly: against a cluster with no leader yet, the
    // client spends its whole retry deadline looking for one before
    // falling back to asking the node directly.
    let (token, _) = auth.issue(&manifest.admin_user)?;
    client.login_with_token(token);

    // `Bootstrap` doesn't retry, and the node may not be listening yet,
    // so keep asking until it answers.
    let deadline = Instant::now() + BOOTSTRAP_TIMEOUT;
    loop {
        match client.bootstrap(&first.id, heartbeat_ms).await {
            Ok(()) => break,
            Err(raft5_client::Error::Unavailable(e)) if Instant::now() < deadline => {
                tracing::debug!("{} not ready to bootstrap yet: {e}", first.id);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => return Err(format!("could not bootstrap {}: {e}", first.id).into()),
        }
    }
    info!("bootstrapped {}", first.id);

    for node in &manifest.nodes[1..] {
        client
            .add_server(&format!("https://{}", node.peer_addr()))
            .await
            .map_err(|e| format!("could not add {}: {e}", node.id))?;
        info!("added {}", node.id);
    }
    Ok(())
}

fn print_connection_info(dir: &Path, manifest: &Manifest) {
    let dir = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());
    let seeds = manifest
        .nodes
        .iter()
        .map(|n| n.client_addr().to_string())
        .collect::<Vec<_>>()
        .join(",");
    println!();
    println!("raft5 cluster running from {}", dir.display());
    for node in &manifest.nodes {
        println!(
            "  {}: client {}  peer {}",
            node.id,
            client_url(node),
            node.peer_addr()
        );
    }
    println!("  admin user:     {}", manifest.admin_user);
    println!("  admin password: {}", dir.join(ADMIN_PASSWORD).display());
    println!("  CA certificate: {}", dir.join(CA_CERT).display());
    println!();
    println!("connect with:");
    println!(
        "  raft --nodes {seeds} --ca-cert {} --user {}",
        dir.join(CA_CERT).display(),
        manifest.admin_user
    );
    println!();
    println!("Ctrl-C to stop.");
}
