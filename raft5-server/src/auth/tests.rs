use super::*;

const SECRET: &[u8] = b"a shared secret, the same on every node";

fn config() -> AuthConfig {
    AuthConfig::new(SECRET, Duration::from_secs(3600), None)
}

/// A token this node issued, checked by a node with the same secret.
#[tokio::test]
async fn a_token_round_trips() {
    let auth = config();
    let (token, expires) = auth.issue("alice").expect("issuing should work");

    let claims = auth.verify(&token).expect("a fresh token should verify");
    assert_eq!(claims.sub, "alice");
    assert_eq!(claims.exp as i64, expires);
    assert!(
        claims.iat <= claims.exp,
        "a token cannot expire before it was issued"
    );
}

/// The point of the shared secret: a node started with a different one is
/// outside this cluster's trust, and its tokens are worth nothing here.
#[tokio::test]
async fn a_token_signed_with_another_secret_is_refused() {
    let (token, _) = AuthConfig::new(b"some other secret", Duration::from_secs(3600), None)
        .issue("alice")
        .unwrap();

    let status = config()
        .verify(&token)
        .expect_err("a foreign token should be refused");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);
}

/// Well past the leeway `verify` allows for clock skew between the issuing
/// node and the checking node.
#[tokio::test]
async fn an_expired_token_is_refused() {
    let auth = config();
    let now = jsonwebtoken::get_current_timestamp();
    let claims = Claims {
        sub: "alice".to_string(),
        iat: now - 7200,
        exp: now - 3600,
    };
    let token = encode(&Header::new(ALGORITHM), &claims, &auth.encoding).unwrap();

    let status = auth
        .verify(&token)
        .expect_err("an hour-old expiry should be refused");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);
}

#[tokio::test]
async fn a_token_that_is_not_a_token_is_refused() {
    for token in ["", "not.a.token", "aaa", "Bearer something"] {
        assert!(
            config().verify(token).is_err(),
            "{token:?} should not verify"
        );
    }
}

#[tokio::test]
async fn a_password_verifies_against_its_own_hash() {
    let hash = hash_password("hunter42".to_string()).await.unwrap();
    assert!(
        verify_password("hunter42".to_string(), hash).await.unwrap(),
        "the password it was made from should verify"
    );
}

#[tokio::test]
async fn the_wrong_password_does_not_verify() {
    let hash = hash_password("hunter42".to_string()).await.unwrap();
    assert!(
        !verify_password("hunter43".to_string(), hash).await.unwrap(),
        "a different password must not verify"
    );
}

/// Salted, so two accounts with the same password do not share a hash. That
/// stops one cracked hash from exposing every account that reused it.
#[tokio::test]
async fn the_same_password_hashes_differently_every_time() {
    let one = hash_password("hunter42".to_string()).await.unwrap();
    let two = hash_password("hunter42".to_string()).await.unwrap();
    assert_ne!(one, two);

    // Both still verify: the salt is in the PHC string, not next to it.
    for hash in [one, two] {
        assert!(verify_password("hunter42".to_string(), hash).await.unwrap());
    }
}

/// A corrupt stored hash is an error, not a "wrong password". Reporting it
/// as one would tell a user their password was wrong when their account
/// record is broken.
#[tokio::test]
async fn an_unreadable_stored_hash_is_an_error_rather_than_a_refusal() {
    let status = verify_password("hunter42".to_string(), "not a PHC string".to_string())
        .await
        .expect_err("a corrupt hash should not be reported as a wrong password");
    assert_eq!(status.code(), tonic::Code::Internal);
}

#[tokio::test]
async fn an_account_record_round_trips() {
    let record = UserRecord::new("alice".to_string(), "$argon2id$fake".to_string());
    let decoded = UserRecord::from_bytes(&record.to_bytes().unwrap()).unwrap();
    assert_eq!(decoded, record);
    assert_eq!(decoded.version, RECORD_VERSION);
}

/// A record written by a node that knows a newer format is refused, not
/// read as this format: misreading a field is the failure to avoid here.
#[tokio::test]
async fn an_account_record_from_a_future_version_is_refused() {
    let mut record = UserRecord::new("alice".to_string(), "$argon2id$fake".to_string());
    record.version = RECORD_VERSION + 1;

    let status = UserRecord::from_bytes(&record.to_bytes().unwrap())
        .expect_err("a newer record version should be refused");
    assert_eq!(status.code(), tonic::Code::Internal);
}
