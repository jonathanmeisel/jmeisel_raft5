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
    AclEntry, AddServerRequest, BootstrapRequest, CommitTransactionRequest, CreateGroupRequest,
    CreateInviteRequest, config_change_response,
    CreateTransactionRequest, CreateUserRequest, GetConfigRequest, ListAclsRequest,
    ListInvitesRequest, LoginRequest, Principal, ReadRequest, RedeemInviteRequest, Right,
    SetAclRequest, WriteRequest, auth_error, create_invite_response, create_user_response,
    login_response, principal, read_response, redeem_invite_response, set_acl_response,
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
                heartbeat_millis: 0,
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
            keys: vec![b"k".to_vec()],
            versions_only: false,
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

    // The permission RPCs are on the list by construction -- `EXEMPT`
    // is an allowlist and nothing was added to it -- but a new RPC that
    // forgot to be is exactly what this test exists to catch.
    let status = client
        .as_ref()
        .clone()
        .create_group(Request::new(CreateGroupRequest {
            name: "mallorys".to_string(),
            members: vec![],
            admins: vec![],
        }))
        .await
        .expect_err("creating a group needs a token");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);

    let status = client
        .as_ref()
        .clone()
        .set_acl(Request::new(SetAclRequest {
            prefix: vec![],
            entries: vec![],
        }))
        .await
        .expect_err("writing an ACL needs a token");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);

    let status = client
        .as_ref()
        .clone()
        .list_acls(Request::new(ListAclsRequest {}))
        .await
        .expect_err("reading the ACLs needs a token");
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
                keys: vec![b"k".to_vec()],
                versions_only: false,
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
            keys: vec![b"k".to_vec()],
            versions_only: false,
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
            keys: vec![b"k".to_vec()],
            versions_only: false,
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
                keys: vec![b"k".to_vec()],
                versions_only: false,
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
                heartbeat_millis: 0,
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
                heartbeat_millis: 0,
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

fn user(name: &str) -> Principal {
    Principal {
        kind: Some(principal::Kind::User(name.to_string())),
    }
}

/// A whole single-key write as whoever holds `token`.
async fn write_with(
    node: &TestNode,
    token: &str,
    key: &[u8],
    value: &[u8],
) -> Result<(), tonic::Status> {
    let mut client = node.client().as_ref().clone();
    let txn = match client
        .create_transaction(with_token(token, CreateTransactionRequest { ttl_ms: 5000 }))
        .await?
        .into_inner()
        .result
    {
        Some(raft5_server::pb::raft5::client::create_transaction_response::Result::TransactionId(
            id,
        )) => id,
        other => panic!("expected a transaction, got {other:?}"),
    };

    let result = client
        .write(with_token(
            token,
            WriteRequest {
                transaction_id: txn.clone(),
                key: key.to_vec(),
                value: Some(value.to_vec()),
            },
        ))
        .await?
        .into_inner()
        .result
        .expect("a write always answers");

    match result {
        raft5_server::pb::raft5::client::write_response::Result::Ok(true) => {
            client
                .commit_transaction(with_token(token, CommitTransactionRequest {
                    transaction_id: txn,
                }))
                .await?;
            Ok(())
        }
        raft5_server::pb::raft5::client::write_response::Result::AuthError(e) => Err(
            tonic::Status::permission_denied(format!("refused: {}", e.detail)),
        ),
        other => panic!("unexpected write result {other:?}"),
    }
}

/// Permissions end to end: two real accounts, two real tokens, and an
/// ACL that has to be enforced against whoever the layer says is calling
/// rather than against an identity a test put there.
///
/// The refusal arriving *in band* is half the point. A token that is
/// missing or forged is an `UNAUTHENTICATED` status raised before any
/// handler runs; an ACL saying no is an answer, and a client that
/// retried it would spend its whole deadline arriving back here.
#[tokio::test]
async fn an_acl_is_enforced_against_the_identity_the_token_carries() {
    let node = TestNode::with_auth("auth9", auth_config(Some(("root", "hunter42"))).await).await;
    let mut client = node.client().as_ref().clone();

    // The startup administrator breaks the circle, exactly as in
    // `an_authenticated_cluster_can_be_bootstrapped_and_then_used`.
    let startup = log_in(&node, "root", "hunter42").await;
    client
        .bootstrap(with_token(&startup, BootstrapRequest {
            server_id: node.id.clone(),
            heartbeat_millis: 0,
        }))
        .await
        .expect("the startup administrator may bootstrap");
    let root = log_in(&node, "root", "hunter42").await;

    // An ordinary account, and a token for it.
    let created = client
        .create_user(with_token(&root, CreateUserRequest {
            username: "alice".to_string(),
            password: "correct horse".to_string(),
            is_admin: false,
        }))
        .await
        .expect("creating an account should be answered")
        .into_inner()
        .result
        .expect("create_user always answers");
    assert!(matches!(created, create_user_response::Result::Seq(_)));
    let alice = log_in(&node, "alice", "correct horse").await;

    // Before any ACL: alice reaches everything, as she always could.
    write_with(&node, &alice, b"app/config", b"v")
        .await
        .expect("with no ACL, every account reaches every key");

    let set = client
        .set_acl(with_token(&root, SetAclRequest {
            prefix: b"app/".to_vec(),
            entries: vec![AclEntry {
                principal: Some(user("root")),
                right: Right::Write as i32,
            }],
        }))
        .await
        .expect("setting an ACL should be answered")
        .into_inner()
        .result
        .expect("set_acl always answers");
    assert!(matches!(set, set_acl_response::Result::Seq(_)), "got {set:?}");

    // Now the same call is refused, and refused in band.
    let refused = client
        .read(with_token(&alice, ReadRequest {
            keys: vec![b"app/config".to_vec()],
            versions_only: false,
            consistency: None,
        }))
        .await
        .expect("an ACL refusal is an answer, not a transport failure")
        .into_inner()
        .result
        .expect("a read always answers");
    match refused {
        read_response::Result::AuthError(e) => assert_eq!(
            auth_error::Kind::try_from(e.kind).expect("a kind this build knows"),
            auth_error::Kind::NotPermitted
        ),
        other => panic!("expected the read to be refused, got {other:?}"),
    }
    assert!(write_with(&node, &alice, b"app/config", b"x").await.is_err());

    // And root, whose token names an administrator, still gets through.
    let allowed = client
        .read(with_token(&root, ReadRequest {
            keys: vec![b"app/config".to_vec()],
            versions_only: false,
            consistency: None,
        }))
        .await
        .expect("root should be answered")
        .into_inner()
        .result
        .expect("a read always answers");
    assert!(matches!(allowed, read_response::Result::Ok(_)), "got {allowed:?}");

    // A key the ACL does not govern is untouched for either of them.
    write_with(&node, &alice, b"other/config", b"v")
        .await
        .expect("nothing governs `other/`");
}

/// A follower enforces the ACLs, which is what proves the permission
/// snapshot is fed by locally applied state rather than by a read of
/// the leader.
///
/// This is the case the obvious implementation gets wrong. The account
/// check beside it, `require_admin`, reads the store *linearizably*: it
/// needs a leader and a read quorum, and it can have both because it
/// guards a handful of rare RPCs that all run on the leader anyway. A
/// stale read does not run on the leader, so a permission check built
/// the same way would not merely be slow here -- there would be nothing
/// for it to ask.
///
/// So the check reads what this node has applied, which is the same
/// state the read it is authorizing is answered from. The cost is the
/// honest one: a change reaches a follower when replication does, which
/// is why this polls rather than asserting straight away.
#[tokio::test]
async fn a_follower_enforces_an_acl_written_on_the_leader() {
    let leader = TestNode::with_auth("auth10", auth_config(Some(("root", "hunter42"))).await).await;
    // The same secret, so a token minted by one is good at the other --
    // which is the whole of what makes them one cluster to a client.
    let follower = TestNode::with_auth("auth11", auth_config(None).await).await;

    let startup = log_in(&leader, "root", "hunter42").await;
    let mut lead = leader.client().as_ref().clone();
    lead.bootstrap(with_token(&startup, BootstrapRequest {
        server_id: leader.id.clone(),
        heartbeat_millis: 0,
    }))
    .await
    .expect("the startup administrator may bootstrap");

    let root = log_in(&leader, "root", "hunter42").await;
    retry("adding the follower", || {
        let mut lead = lead.clone();
        let root = root.clone();
        let address = format!("http://{}", follower.peer_addr);
        async move {
            matches!(
                lead.add_server(with_token(&root, AddServerRequest {
                    peer_address: address,
                }))
                .await
                .map(|r| r.into_inner().result),
                Ok(Some(config_change_response::Result::Ok(true)))
            )
        }
    })
    .await;

    lead.create_user(with_token(&root, CreateUserRequest {
        username: "alice".to_string(),
        password: "correct horse".to_string(),
        is_admin: false,
    }))
    .await
    .expect("an administrator may create accounts");
    let alice = log_in(&leader, "alice", "correct horse").await;

    write_with(&leader, &root, b"app/config", b"v")
        .await
        .expect("nothing governs this key yet");

    // Until replication catches up the follower knows nothing about
    // either. Waiting for the *value* is what makes the next assertion
    // meaningful: it says the follower could have answered, and then
    // stopped.
    let mut follow = follower.client().as_ref().clone();
    retry("the write reaching the follower", || {
        let mut follow = follow.clone();
        let alice = alice.clone();
        async move {
            matches!(
                follow
                    .read(with_token(&alice, ReadRequest {
                        keys: vec![b"app/config".to_vec()],
                        versions_only: false,
                        consistency: None,
                    }))
                    .await
                    .map(|r| r.into_inner().result),
                Ok(Some(read_response::Result::Ok(_)))
            )
        }
    })
    .await;

    lead.set_acl(with_token(&root, SetAclRequest {
        prefix: b"app/".to_vec(),
        entries: vec![AclEntry {
            principal: Some(user("root")),
            right: Right::Write as i32,
        }],
    }))
    .await
    .expect("setting an ACL should be answered")
    .into_inner();

    // And now the follower -- which is not the leader, holds no quorum,
    // and answers this read entirely from its own state -- refuses it.
    retry("the ACL reaching the follower", || {
        let mut follow = follow.clone();
        let alice = alice.clone();
        async move {
            matches!(
                follow
                    .read(with_token(&alice, ReadRequest {
                        keys: vec![b"app/config".to_vec()],
                        versions_only: false,
                        consistency: None,
                    }))
                    .await
                    .map(|r| r.into_inner().result),
                Ok(Some(read_response::Result::AuthError(_)))
            )
        }
    })
    .await;

    // The administrator's bypass is resolved from the follower's own
    // account records, so it works there too.
    let answered = follow
        .read(with_token(&root, ReadRequest {
            keys: vec![b"app/config".to_vec()],
            versions_only: false,
            consistency: None,
        }))
        .await
        .expect("root should be answered")
        .into_inner()
        .result;
    assert!(
        matches!(answered, Some(read_response::Result::Ok(_))),
        "got {answered:?}"
    );
}

/// Polls `check` until it holds, or fails the test after ten seconds.
///
/// Replication and apply are both asynchronous, so everything a
/// follower is asked about here is "eventually" rather than "now".
/// Sleeping a fixed amount instead would be either flaky or slow.
async fn retry<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let until = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if check().await {
            return;
        }
        assert!(std::time::Instant::now() < until, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
