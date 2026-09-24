//! End-to-end authentication, through a real socket and therefore
//! through the layer that enforces it.
//!
//! The unit tests in `grpc::tests` drive the handlers directly and hand
//! them an already-verified identity, which is exactly the part this
//! file does not take on trust: here the only thing a caller has is a
//! token in a header, and everything else follows from whether the layer
//! accepts it.

mod common;

use std::time::Duration;

use tonic::Request;

use raft5_server::auth::{AuthConfig, InitialAdmin};
use raft5_server::pb::raft5::client::{
    BootstrapRequest, CreateInviteRequest, CreateTransactionRequest, CreateUserRequest,
    GetConfigRequest, ListInvitesRequest, LoginRequest, ReadRequest, RedeemInviteRequest,
    create_invite_response, create_user_response, login_response, redeem_invite_response,
};

use common::TestNode;

const SECRET: &[u8] = b"thirty-two bytes at the very least, and this is";

async fn auth_config(admin: Option<(&str, &str)>) -> AuthConfig {
    let initial_admin = match admin {
        Some((username, password)) => Some(InitialAdmin {
            username: username.to_string(),
            password_hash: raft5_server::auth::hash_password(password.to_string())
                .await
                .expect("hashing should work"),
        }),
        None => None,
    };
    AuthConfig::new(SECRET, Duration::from_secs(3600), initial_admin)
}

/// A request carrying a bearer token, the way a client sends one.
fn with_token<T>(token: &str, message: T) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}")
            .parse()
            .expect("a token is a valid header value"),
    );
    request
}

async fn log_in(node: &TestNode, username: &str, password: &str) -> String {
    let result = node
        .client()
        .as_ref()
        .clone()
        .login(Request::new(LoginRequest {
            username: username.to_string(),
            password: password.to_string(),
        }))
        .await
        .expect("Login needs no token, so it should be answered")
        .into_inner()
        .result
        .expect("a login always answers");
    match result {
        login_response::Result::Ok(ok) => {
            assert!(ok.expires_unix > 0, "a token should say when it expires");
            ok.token
        }
        other => panic!("expected a token, got {other:?}"),
    }
}

/// The whole startup sequence for an authenticated cluster, which is
/// circular until the startup administrator breaks it: `Bootstrap` needs
/// a token, and until it has run there are no accounts to get one from.
#[tokio::test]
async fn an_authenticated_cluster_can_be_bootstrapped_and_then_used() {
    let node = TestNode::with_auth("auth1", auth_config(Some(("root", "hunter42"))).await).await;

    // Nothing is stored yet, so this token can only have come from the
    // password the node was started with.
    let token = log_in(&node, "root", "hunter42").await;

    node.client()
        .as_ref()
        .clone()
        .bootstrap(with_token(
            &token,
            BootstrapRequest {
                server_id: node.id.clone(),
            },
        ))
        .await
        .expect("the startup administrator may bootstrap")
        .into_inner();

    // Bootstrap wrote `root` into the log on its way out, so the same
    // credentials now work against a stored account rather than a flag.
    let token = log_in(&node, "root", "hunter42").await;
    let created = node
        .client()
        .as_ref()
        .clone()
        .create_user(with_token(
            &token,
            CreateUserRequest {
                username: "alice".to_string(),
                password: "correct horse".to_string(),
                is_admin: false,
            },
        ))
        .await
        .expect("an administrator may create accounts")
        .into_inner()
        .result
        .expect("create_user always answers");
    assert!(
        matches!(created, create_user_response::Result::Seq(seq) if seq > 0),
        "expected the account to be created, got {created:?}"
    );

    // And the new account can get a token of its own.
    assert!(!log_in(&node, "alice", "correct horse").await.is_empty());
}

/// The exempt list, from the outside. These two have to work without a
/// token or nothing else can: `Login` is what issues one, and
/// `GetConfig` is how a client finds the node to send a login to.
#[tokio::test]
async fn get_config_and_login_are_answered_without_a_token() {
    let node = TestNode::with_auth("auth2", auth_config(Some(("root", "hunter42"))).await).await;

    node.client()
        .as_ref()
        .clone()
        .get_config(Request::new(GetConfigRequest {}))
        .await
        .expect("GetConfig is how a client finds the leader, before it can log in");

    // Answered, and refused on its merits rather than for want of a
    // token -- the distinction the exemption exists to make.
    let refused = node
        .client()
        .as_ref()
        .clone()
        .login(Request::new(LoginRequest {
            username: "root".to_string(),
            password: "not the password".to_string(),
        }))
        .await
        .expect("Login itself never needs a token")
        .into_inner()
        .result
        .expect("a login always answers");
    assert!(
        matches!(refused, login_response::Result::AuthError(_)),
        "expected a refusal, got {refused:?}"
    );
}

/// Everything not on that list. An allowlist rather than a denylist is
/// what makes this the default.
#[tokio::test]
async fn every_other_rpc_is_refused_without_a_token() {
    let node = TestNode::with_auth("auth3", auth_config(Some(("root", "hunter42"))).await).await;
    let client = node.client();

    let status = client
        .as_ref()
        .clone()
        .read(Request::new(ReadRequest {
            key: b"k".to_vec(),
            consistency: None,
        }))
        .await
        .expect_err("a read needs a token");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);

    let status = client
        .as_ref()
        .clone()
        .create_transaction(Request::new(CreateTransactionRequest { ttl_ms: 1000 }))
        .await
        .expect_err("opening a transaction needs a token");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);

    let status = client
        .as_ref()
        .clone()
        .bootstrap(Request::new(BootstrapRequest::default()))
        .await
        .expect_err("even bootstrapping needs a token");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);

    let status = client
        .as_ref()
        .clone()
        .create_user(Request::new(CreateUserRequest {
            username: "mallory".to_string(),
            password: "letmein".to_string(),
            is_admin: true,
        }))
        .await
        .expect_err("creating an account needs a token");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);
}

/// A token is only as good as the secret behind it. This is what stops a
/// node from trusting a token minted anywhere outside its own cluster.
#[tokio::test]
async fn a_token_signed_with_another_secret_is_refused() {
    let node = TestNode::with_auth("auth4", auth_config(Some(("root", "hunter42"))).await).await;

    let (foreign, _) = AuthConfig::new(
        b"an entirely different secret, also long enough",
        Duration::from_secs(3600),
        None,
    )
    .issue("root")
    .expect("issuing should work");

    let status = node
        .client()
        .as_ref()
        .clone()
        .read(with_token(
            &foreign,
            ReadRequest {
                key: b"k".to_vec(),
                consistency: None,
            },
        ))
        .await
        .expect_err("a foreign token should be refused");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);
}

#[tokio::test]
async fn a_malformed_authorization_header_is_refused() {
    let node = TestNode::with_auth("auth5", auth_config(Some(("root", "hunter42"))).await).await;

    for header in ["", "Bearer", "Basic abc123", "Bearer not.a.token"] {
        let mut request = Request::new(ReadRequest {
            key: b"k".to_vec(),
            consistency: None,
        });
        request
            .metadata_mut()
            .insert("authorization", header.parse().unwrap());

        let status = node
            .client()
            .as_ref()
            .clone()
            .read(request)
            .await
            .unwrap_err();
        assert_eq!(
            status.code(),
            tonic::Code::Unauthenticated,
            "{header:?} should not authenticate anyone"
        );
    }
}

/// The opt-in half, tested rather than assumed: a node started without a
/// secret behaves exactly as this server did before any of this existed,
/// and is not confused by a header it never asked for.
#[tokio::test]
async fn a_node_without_a_secret_requires_and_ignores_tokens_alike() {
    let node = TestNode::new("auth6").await;

    node.client()
        .as_ref()
        .clone()
        .read(Request::new(ReadRequest {
            key: b"k".to_vec(),
            consistency: None,
        }))
        .await
        .expect("an open node serves a read with no token at all");

    node.client()
        .as_ref()
        .clone()
        .read(with_token(
            "not.a.real.token",
            ReadRequest {
                key: b"k".to_vec(),
                consistency: None,
            },
        ))
        .await
        .expect("and is not troubled by a token it never asked for");
}

/// The third exemption, and the only one that writes.
///
/// `RedeemInvite` is answered without a bearer token because its callers
/// are people who have no account yet. What it is not is unprotected:
/// the layer lets the request through, and the handler then refuses
/// everything that does not carry an invitation this cluster issued.
/// This test is about that distinction -- the request is *answered*, on
/// its merits, rather than turned away for want of a token.
#[tokio::test]
async fn an_invitation_creates_an_account_with_no_token_anywhere() {
    let node = TestNode::with_auth("auth6", auth_config(Some(("root", "hunter42"))).await).await;
    let client = node.client();

    let token = log_in(&node, "root", "hunter42").await;
    client
        .as_ref()
        .clone()
        .bootstrap(with_token(
            &token,
            BootstrapRequest {
                server_id: node.id.clone(),
            },
        ))
        .await
        .expect("the startup administrator may bootstrap");
    let token = log_in(&node, "root", "hunter42").await;

    // Issuing one needs the administrator's token, like any other
    // account management.
    let status = client
        .as_ref()
        .clone()
        .create_invite(Request::new(CreateInviteRequest::default()))
        .await
        .expect_err("issuing an invitation needs a token");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);

    let status = client
        .as_ref()
        .clone()
        .list_invites(Request::new(ListInvitesRequest {}))
        .await
        .expect_err("listing invitations needs a token");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);

    let issued = client
        .as_ref()
        .clone()
        .create_invite(with_token(
            &token,
            CreateInviteRequest {
                is_admin: false,
                ttl_ms: 0,
                username: None,
            },
        ))
        .await
        .expect("an administrator may issue an invitation")
        .into_inner()
        .result
        .expect("create_invite always answers");
    let invite_token = match issued {
        create_invite_response::Result::Ok(ok) => ok.token,
        other => panic!("expected an invitation, got {other:?}"),
    };

    // No `with_token` anywhere here: this is someone who has nothing but
    // the secret they were handed.
    let redeemed = client
        .as_ref()
        .clone()
        .redeem_invite(Request::new(RedeemInviteRequest {
            token: invite_token,
            username: "alice".to_string(),
            password: "correct horse".to_string(),
        }))
        .await
        .expect("redeeming an invitation never needs a token")
        .into_inner()
        .result
        .expect("redeem_invite always answers");
    assert!(
        matches!(redeemed, redeem_invite_response::Result::Ok(ref ok) if ok.seq > 0),
        "expected the account to be created, got {redeemed:?}"
    );

    // And the account it made is an ordinary one, which can now get a
    // token the ordinary way.
    assert!(!log_in(&node, "alice", "correct horse").await.is_empty());
}

/// The exemption is for the RPC, not for whoever calls it. A redemption
/// with no usable invitation is answered and refused -- which is the
/// whole of what keeps an open write on this service from being an open
/// cluster.
#[tokio::test]
async fn a_redemption_without_a_real_invitation_creates_nothing() {
    let node = TestNode::with_auth("auth7", auth_config(Some(("root", "hunter42"))).await).await;
    let client = node.client();

    let token = log_in(&node, "root", "hunter42").await;
    client
        .as_ref()
        .clone()
        .bootstrap(with_token(
            &token,
            BootstrapRequest {
                server_id: node.id.clone(),
            },
        ))
        .await
        .expect("the startup administrator may bootstrap");

    let refused = client
        .as_ref()
        .clone()
        .redeem_invite(Request::new(RedeemInviteRequest {
            token: "a token nobody issued".to_string(),
            username: "mallory".to_string(),
            password: "letmein".to_string(),
        }))
        .await
        .expect("the request is answered, not turned away")
        .into_inner()
        .result
        .expect("redeem_invite always answers");
    assert!(
        matches!(refused, redeem_invite_response::Result::AuthError(_)),
        "expected a refusal, got {refused:?}"
    );

    assert!(
        matches!(
            node.client()
                .as_ref()
                .clone()
                .login(Request::new(LoginRequest {
                    username: "mallory".to_string(),
                    password: "letmein".to_string(),
                }))
                .await
                .expect("Login needs no token")
                .into_inner()
                .result,
            Some(login_response::Result::AuthError(_))
        ),
        "no account should have been created"
    );
}
