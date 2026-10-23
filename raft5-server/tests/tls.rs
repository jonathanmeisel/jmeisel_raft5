//! TLS on both ports, through real sockets: mTLS between peers, and TLS or
//! mTLS on the client API, with a private CA and with a bare self-signed
//! certificate used as its own anchor.
//!
//! The cluster tests prove that all peer RPCs run over mTLS by needing
//! every one of them: a learner cannot be promoted without replication, and
//! a follower cannot read a write that was never replicated to it.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};

use raft5_server::auth::AuthConfig;
use raft5_server::pb::raft5::client::kv_client::KvClient;
use raft5_server::pb::raft5::client::{GetConfigRequest, ReadRequest};
use raft5_server::pb::raft5::raft::IdentifyRequest;
use raft5_server::pb::raft5::raft::raft_peer_client::RaftPeerClient;
use raft5_server::{ClientApiTls, PeerTls};

use common::certs::{self, TestCa};
use common::{
    TestNode, TestTls, add_server, assert_eventually_reads, bootstrap, config, find_leader,
    wait_until_voter, write,
};

/// The names a test certificate covers: every node here is on loopback.
const LOCAL: &[&str] = &["localhost", "127.0.0.1"];

fn peer_tls(identity: Identity, ca: Certificate, server_name: Option<&str>) -> PeerTls {
    PeerTls {
        identity,
        ca,
        server_name: server_name.map(str::to_string),
    }
}

/// Bootstrap `nodes[0]`, add the rest, and check that a write made through
/// the leader reaches every one of them.
async fn form_cluster_and_replicate(nodes: &[&TestNode]) {
    let first = nodes[0];
    bootstrap(&first.client(), &first.id).await;
    for node in &nodes[1..] {
        add_server(&first.client(), node).await;
        wait_until_voter(&first.client(), &node.id).await;
    }
    let leader = find_leader(nodes).await;
    let seq = write(&leader.client(), b"k", Some(b"v")).await;
    for node in nodes {
        assert_eventually_reads(&node.client(), b"k", b"v", seq).await;
    }
}

#[tokio::test]
async fn peers_replicate_over_mtls_with_a_private_ca() {
    let ca = TestCa::new();
    // One certificate shared by every node, as in the intended deployment.
    let shared = ca.leaf(LOCAL);
    let tls = TestTls {
        peer: Some(peer_tls(shared.identity(), ca.certificate(), None)),
        ..TestTls::default()
    };
    let n1 = TestNode::with_tls("n1", tls.clone()).await;
    let n2 = TestNode::with_tls("n2", tls.clone()).await;
    let n3 = TestNode::with_tls("n3", tls).await;

    form_cluster_and_replicate(&[&n1, &n2, &n3]).await;

    let members = config(&n1.client()).await;
    assert!(
        members.iter().all(|m| m.address.starts_with("https://")),
        "a TLS node should advertise its peer API as https: {members:?}"
    );
}

/// A bare self-signed certificate, used as its own anchor, that names none
/// of the addresses it is reached at, only the fixed name every peer checks
/// it against instead.
#[tokio::test]
async fn peers_replicate_with_a_self_signed_cert_and_a_fixed_server_name() {
    let shared = certs::self_signed(&["raft5-peer"]);
    let tls = TestTls {
        peer: Some(peer_tls(
            shared.identity(),
            shared.as_ca(),
            Some("raft5-peer"),
        )),
        ..TestTls::default()
    };
    let n1 = TestNode::with_tls("n1", tls.clone()).await;
    let n2 = TestNode::with_tls("n2", tls).await;

    form_cluster_and_replicate(&[&n1, &n2]).await;
}

/// Addresses recorded as `http://` (as in a cluster configured without TLS)
/// are still dialed over TLS once TLS is on. tonic would dial them in
/// plaintext as written, and the TLS-only peer port would refuse that, so
/// replication working at all is the proof.
#[tokio::test]
async fn http_addresses_in_the_log_are_still_dialed_over_tls() {
    let ca = TestCa::new();
    let shared = ca.leaf(LOCAL);
    let tls = TestTls {
        peer: Some(peer_tls(shared.identity(), ca.certificate(), None)),
        advertise_http: true,
        ..TestTls::default()
    };
    let n1 = TestNode::with_tls("n1", tls.clone()).await;
    let n2 = TestNode::with_tls("n2", tls).await;

    form_cluster_and_replicate(&[&n1, &n2]).await;

    let members = config(&n1.client()).await;
    assert!(
        members.iter().all(|m| m.address.starts_with("http://")),
        "the scenario needs plaintext-looking addresses: {members:?}"
    );
}

/// Call `Identify` on a peer API, dialed as `endpoint` says.
async fn identify(endpoint: Endpoint) -> Result<(), String> {
    let channel = endpoint.connect().await.map_err(|e| e.to_string())?;
    RaftPeerClient::new(channel)
        .identify(IdentifyRequest {})
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

fn https(addr: SocketAddr, tls: ClientTlsConfig) -> Endpoint {
    Endpoint::from_shared(format!("https://{addr}"))
        .unwrap()
        .tls_config(tls)
        .unwrap()
        .connect_timeout(Duration::from_secs(2))
}

#[tokio::test]
async fn the_peer_port_refuses_anyone_without_a_trusted_certificate() {
    let ca = TestCa::new();
    let shared = ca.leaf(LOCAL);
    let node = TestNode::with_tls(
        "n1",
        TestTls {
            peer: Some(peer_tls(shared.identity(), ca.certificate(), None)),
            ..TestTls::default()
        },
    )
    .await;
    let addr = node.peer_addr;

    // The control: a peer's own credentials get an answer.
    identify(https(
        addr,
        ClientTlsConfig::new()
            .ca_certificate(ca.certificate())
            .identity(shared.identity()),
    ))
    .await
    .expect("a peer with the cluster's certificate should be answered");

    // Plaintext, to a port that only speaks TLS.
    let plaintext = Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect_timeout(Duration::from_secs(2));
    assert!(identify(plaintext).await.is_err(), "plaintext was answered");

    // TLS, but with no client certificate at all.
    let anonymous = ClientTlsConfig::new().ca_certificate(ca.certificate());
    assert!(
        identify(https(addr, anonymous)).await.is_err(),
        "a caller with no certificate was answered"
    );

    // A certificate from another CA.
    let stranger = TestCa::new().leaf(LOCAL);
    let impostor = ClientTlsConfig::new()
        .ca_certificate(ca.certificate())
        .identity(stranger.identity());
    assert!(
        identify(https(addr, impostor)).await.is_err(),
        "a certificate from another CA was answered"
    );
}

/// Call `GetConfig` on a client API, dialed as `endpoint` says.
async fn get_config(endpoint: Endpoint) -> Result<(), String> {
    let channel = endpoint.connect().await.map_err(|e| e.to_string())?;
    KvClient::new(channel)
        .get_config(GetConfigRequest {})
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

#[tokio::test]
async fn the_client_api_serves_tls_to_anyone_who_trusts_it() {
    let ca = TestCa::new();
    let server = ca.leaf(LOCAL);
    let dial = ClientTlsConfig::new().ca_certificate(ca.certificate());
    let node = TestNode::with_tls(
        "n1",
        TestTls {
            client: Some(ClientApiTls {
                identity: server.identity(),
                client_ca: None,
            }),
            dial: Some(dial.clone()),
            ..TestTls::default()
        },
    )
    .await;
    let addr = node.client_addr;

    get_config(https(addr, dial))
        .await
        .expect("a client that trusts the CA should be answered");

    let plaintext = Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect_timeout(Duration::from_secs(2));
    assert!(get_config(plaintext).await.is_err(), "plaintext was answered");

    let wrong_ca = ClientTlsConfig::new().ca_certificate(TestCa::new().certificate());
    assert!(
        get_config(https(addr, wrong_ca)).await.is_err(),
        "a client trusting some other CA still connected"
    );
}

#[tokio::test]
async fn the_client_api_requires_a_certificate_once_given_a_client_ca() {
    let server_ca = TestCa::new();
    let client_ca = TestCa::new();
    let server = server_ca.leaf(LOCAL);
    let client = client_ca.leaf(&["some-client"]);
    let dial = ClientTlsConfig::new()
        .ca_certificate(server_ca.certificate())
        .identity(client.identity());
    let node = TestNode::with_tls(
        "n1",
        TestTls {
            client: Some(ClientApiTls {
                identity: server.identity(),
                client_ca: Some(client_ca.certificate()),
            }),
            dial: Some(dial.clone()),
            ..TestTls::default()
        },
    )
    .await;
    let addr = node.client_addr;

    get_config(https(addr, dial))
        .await
        .expect("a client with a certificate from the client CA should be answered");

    let anonymous = ClientTlsConfig::new().ca_certificate(server_ca.certificate());
    assert!(
        get_config(https(addr, anonymous)).await.is_err(),
        "a client with no certificate was answered"
    );

    // Signed by the CA the server uses, not the one clients are checked
    // against.
    let wrong_issuer = ClientTlsConfig::new()
        .ca_certificate(server_ca.certificate())
        .identity(server_ca.leaf(&["some-client"]).identity());
    assert!(
        get_config(https(addr, wrong_issuer)).await.is_err(),
        "a client certificate from the wrong CA was answered"
    );
}

/// A client certificate is a gate on connecting, not a login: the token is
/// still required on everything that requires one.
#[tokio::test]
async fn mtls_does_not_stand_in_for_a_token() {
    let ca = TestCa::new();
    let server = ca.leaf(LOCAL);
    let client = ca.leaf(&["some-client"]);
    let dial = ClientTlsConfig::new()
        .ca_certificate(ca.certificate())
        .identity(client.identity());
    let node = TestNode::with_auth_and_tls(
        "n1",
        AuthConfig::new(
            b"thirty-two bytes at the very least, and this is",
            Duration::from_secs(3600),
            None,
        ),
        TestTls {
            client: Some(ClientApiTls {
                identity: server.identity(),
                client_ca: Some(ca.certificate()),
            }),
            dial: Some(dial),
            ..TestTls::default()
        },
    )
    .await;

    let status = node
        .client()
        .as_ref()
        .clone()
        .read(ReadRequest {
            keys: vec![b"k".to_vec()],
            versions_only: false,
            consistency: None,
        })
        .await
        .expect_err("a read with no token should be refused");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);
}
