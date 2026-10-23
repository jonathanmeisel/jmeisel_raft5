//! Who sent a request, and how that is established.
//!
//! Accounts are normal key/value pairs in the store, under the prefix that
//! `crate::keyspace` hides from the client API, so they replicate and
//! snapshot like everything else. A password is never stored, only its
//! Argon2id hash. `Login` checks that hash and returns a signed token;
//! every later request carries the token instead of the password.
//!
//! # The signing key
//!
//! One symmetric secret (HS256), the same bytes on every node. A token must
//! verify on any node (a stale read is answered by whichever node got it,
//! not always the issuer), so the key cannot be per node. The operator
//! supplies it; it is not generated. That keeps key material out of the log
//! and snapshots, and lets it be mounted the same way as TLS material.
//!
//! # What a token does not say
//!
//! [`Claims`] holds the username and nothing about what that user may do.
//! Admin rights come from the `admins` group, checked on each request that
//! needs them, because a claim inside a token cannot be withdrawn: a
//! demoted admin's token would keep full power until it expired.
//!
//! The opposite gap is real and not solved here: a deleted user's token
//! still verifies, for non-admin requests, until it expires. The token
//! lifetime bounds that.
//!
//! # Invitations
//!
//! An account can also be created without an administrator choosing its
//! password. [`InviteSecret::mint`] draws 32 bytes from the OS and gives
//! them to the issuing administrator once. The store keeps their SHA-256,
//! under a key derived from that same hash. So the record is looked up by
//! the value being checked: there is no comparison to get wrong, and no
//! secret in the log to leak.
//!
//! Argon2 is not used here, on purpose. Its cost makes guessing a
//! human-chosen password infeasible. A 256-bit secret from `getrandom`
//! cannot be guessed anyway, so a slow hash would only give a slow
//! unauthenticated RPC (see `grpc::KvService::redeem_invite`, which anyone
//! can call).
//!
//! # Plaintext secrets
//!
//! `Login` takes a password in the message, and `RedeemInvite` takes a
//! password and an invitation. Without TLS on the client API, anyone who
//! can see the traffic can see passwords, replay tokens, and use an
//! invitation before its recipient does.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use argon2::Argon2;
use argon2::password_hash::{PasswordHasher, PasswordVerifier, phc::PasswordHash};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as BASE64;
use sha2::{Digest, Sha256};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use tonic::Status;

/// The only algorithm this cluster signs or accepts.
///
/// Pinned on the verify side too: a verifier that accepted whatever the
/// token's header asked for would accept `none`, and would let an attacker
/// with only the public half of another scheme present it as an HMAC
/// secret.
const ALGORITHM: Algorithm = Algorithm::HS256;

/// What a token asserts. See the module doc for what it does not.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Claims {
    /// The username this token was issued to.
    pub sub: String,
    /// Issued at, and expires at, in seconds since the epoch.
    pub iat: u64,
    pub exp: u64,
}

/// Who the auth layer decided a request is from.
///
/// The auth layer puts it in the request's extensions, and the handlers
/// that need it read it back (see `crate::grpc`). Its presence means the
/// token verified. It says nothing about what the user may do; that comes
/// from the store.
#[derive(Clone, Debug)]
pub struct AuthContext {
    pub username: String,
}

/// The version stamped into every record this build writes.
const RECORD_VERSION: u8 = 1;

/// One account, as stored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserRecord {
    /// The first field on purpose. `postcard` is not self-describing, so the
    /// encoding does not say what shape it is. With the version first, a
    /// future reader decodes the version before any field whose layout may
    /// have changed. That gives a clear answer for a record from a newer node;
    /// it does not guarantee such a record decodes.
    pub version: u8,
    pub username: String,
    pub password_hash: String,
}

impl UserRecord {
    pub fn new(username: String, password_hash: String) -> Self {
        Self {
            version: RECORD_VERSION,
            username,
            password_hash,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, Status> {
        postcard::to_allocvec(self)
            .map_err(|e| Status::internal(format!("could not encode an account record: {e}")))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Status> {
        let record: Self = postcard::from_bytes(bytes)
            .map_err(|_| Status::internal("a stored account record could not be decoded"))?;
        if record.version != RECORD_VERSION {
            return Err(Status::internal(format!(
                "a stored account record is version {}, and this node understands {RECORD_VERSION}",
                record.version
            )));
        }
        Ok(record)
    }
}

/// One open invitation, as stored.
///
/// The secret is never in it: the record is found by hashing the secret,
/// so the record shows what the invite is for without showing how to use
/// it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InviteRecord {
    /// First, for the reason [`UserRecord::version`] is first.
    pub version: u8,
    /// The administrator who issued it. Kept so a listing can say where an
    /// invitation came from. When accounts create accounts, "who let this
    /// person in?" has no other answer.
    pub issued_by: String,
    /// Whether the resulting account joins the `admins` group. An
    /// administrator sets it at issue time; the redeemer has no say.
    pub is_admin: bool,
    /// The only username this invite may create, if it was pinned to one.
    pub username: Option<String>,
    /// When it can no longer be redeemed, by the clock of the leader that wrote
    /// it.
    pub expires_unix_ms: u64,
}

impl InviteRecord {
    pub fn new(
        issued_by: String,
        is_admin: bool,
        username: Option<String>,
        expires_unix_ms: u64,
    ) -> Self {
        Self {
            version: RECORD_VERSION,
            issued_by,
            is_admin,
            username,
            expires_unix_ms,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, Status> {
        postcard::to_allocvec(self)
            .map_err(|e| Status::internal(format!("could not encode an invitation: {e}")))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Status> {
        let record: Self = postcard::from_bytes(bytes)
            .map_err(|_| Status::internal("a stored invitation could not be decoded"))?;
        if record.version != RECORD_VERSION {
            return Err(Status::internal(format!(
                "a stored invitation is version {}, and this node understands {RECORD_VERSION}",
                record.version
            )));
        }
        Ok(record)
    }

    /// Whether this invitation has lapsed, as of `now_unix_ms`.
    pub fn has_expired(&self, now_unix_ms: u64) -> bool {
        now_unix_ms >= self.expires_unix_ms
    }
}

/// How many bytes of entropy an invitation has.
///
/// 32, so nobody attempts to guess one, and the cluster does not need to
/// make guessing expensive to be safe.
const INVITE_SECRET_BYTES: usize = 32;

/// The longest token this node will hash at all.
///
/// `RedeemInvite` answers callers with no account, so an anonymous caller
/// can ask for any work it does before it decides. A token longer than any
/// this cluster creates is refused without hashing.
pub const MAX_INVITE_TOKEN_LEN: usize = 128;

/// A new invitation: the secret to hand over, and the id the cluster knows
/// it by.
pub struct InviteSecret {
    /// Given out exactly once, in the response to `CreateInvite`.
    pub token: String,
    /// `invite_id(&token)`, carried with it so the caller does not compute
    /// again what minting already knows.
    pub id: String,
}

impl InviteSecret {
    /// A new invitation, from the operating system's randomness.
    ///
    /// Not `txn::fresh_id`: that is a `RandomState` hash, fine for naming a
    /// transaction but useless for anything an attacker would like to predict.
    /// This is a credential, so it comes from `getrandom` only.
    pub fn mint() -> Result<Self, Status> {
        let mut bytes = [0u8; INVITE_SECRET_BYTES];
        getrandom::fill(&mut bytes)
            .map_err(|e| Status::internal(format!("could not draw a secret: {e}")))?;
        let token = BASE64.encode(bytes);
        let id = invite_id(&token);
        Ok(Self { token, id })
    }
}

/// The id the cluster knows `token` by: its SHA-256, in URL-safe base64.
///
/// This is the whole lookup and the whole check. The record's key is this
/// hash, so a token either names a record or does not. No stored secret is
/// compared, so no comparison timing can leak one.
///
/// Hashes the token's characters, not decoded bytes, so a token that is
/// not base64 at all simply names no record. There is no parse to fail,
/// and no malformed-input path that differs from a wrong secret.
pub fn invite_id(token: &str) -> String {
    BASE64.encode(Sha256::digest(token.as_bytes()))
}

/// Wall-clock milliseconds since the epoch.
///
/// The leader measures an invitation's lifetime: the issuer's and the
/// recipient's clocks are outside this cluster, and neither can be trusted
/// with a credential's expiry. A time before the epoch counts as the epoch:
/// a node whose clock says 1969 has a problem this cannot fix, and
/// saturating keeps it from creating an invitation that never lapses.
pub fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// The administrator a node was started with, for the one period when
/// there is no stored account to log in against.
///
/// Hashed once at startup, not kept in clear text, so the operator's
/// password does not stay in this process's memory, and the hash is ready
/// when `Bootstrap` writes the real record.
#[derive(Clone)]
pub struct InitialAdmin {
    pub username: String,
    pub password_hash: String,
}

/// What a node needs to issue and check tokens. Absent when the node was
/// started without a secret, which leaves the cluster open.
#[derive(Clone)]
pub struct AuthConfig {
    encoding: Arc<EncodingKey>,
    decoding: Arc<DecodingKey>,
    pub token_ttl: Duration,
    pub initial_admin: Option<InitialAdmin>,
}

impl AuthConfig {
    pub fn new(secret: &[u8], token_ttl: Duration, initial_admin: Option<InitialAdmin>) -> Self {
        Self {
            encoding: Arc::new(EncodingKey::from_secret(secret)),
            decoding: Arc::new(DecodingKey::from_secret(secret)),
            token_ttl,
            initial_admin,
        }
    }

    /// A token for `username`, and when it stops being valid.
    ///
    /// The expiry is returned, not left in the token, so a client never has to
    /// parse a JWT to know when to log in again.
    pub fn issue(&self, username: &str) -> Result<(String, i64), Status> {
        let now = jsonwebtoken::get_current_timestamp();
        let exp = now.saturating_add(self.token_ttl.as_secs());
        let claims = Claims {
            sub: username.to_string(),
            iat: now,
            exp,
        };
        let token = encode(&Header::new(ALGORITHM), &claims, &self.encoding)
            .map_err(|e| Status::internal(format!("could not issue a token: {e}")))?;
        Ok((token, exp as i64))
    }

    /// The claims of a token this cluster signed and that has not expired, or
    /// why not.
    ///
    /// The reason is reported, not reduced to "no": the holder of an expired
    /// token learns nothing new from it, and knowing which failure it was lets
    /// a client log in again instead of giving up.
    pub fn verify(&self, token: &str) -> Result<Claims, Status> {
        // `Validation::new` pins the algorithm (see `ALGORITHM`) and requires an
        // `exp` claim, so a token without one is rejected, not treated as eternal.
        // The default leeway (60s) stays: the issuing node is usually not the
        // checking node, and their clocks are only as close as the operator keeps
        // them.
        let validation = Validation::new(ALGORITHM);
        decode::<Claims>(token, &self.decoding, &validation)
            .map(|data| data.claims)
            .map_err(|e| Status::unauthenticated(format!("the token was not accepted: {e}")))
    }
}

/// The RPCs that may be called with no token.
///
/// An allowlist, not a denylist, so an RPC added to the service and
/// forgotten here is protected by default, not exposed.
///
/// - `Login` issues tokens, so it cannot require one.
/// - `GetConfig` is how a client finds the leader, which it must do before
///   it can send a login. It shows the members' addresses, which the
///   client was configured with anyway.
/// - `RedeemInvite` is the only exempt RPC that writes. Its caller has no
///   account by definition (that is what they are fixing), and requiring a
///   token to get a token is the same circle that `Login` breaks. The
///   invitation takes the place of a token: 32 bytes an administrator drew
///   and handed over, which the handler checks before anything else. An
///   unknown, used, revoked or lapsed invite gets exactly what no token
///   gets.
const EXEMPT: [&str; 3] = [
    "/raft5.client.Kv/Login",
    "/raft5.client.Kv/GetConfig",
    "/raft5.client.Kv/RedeemInvite",
];

/// Turn away any client API request that does not carry a token this
/// cluster signed.
///
/// A `tower` layer, not a tonic interceptor, because an interceptor cannot
/// see which method is called (tonic drops the URI first), and without the
/// method there is no way to exempt `Login` from needing what `Login`
/// issues.
///
/// It only authenticates. What a caller may do is decided in the handlers,
/// from the store, because that can change while a token is still valid.
#[derive(Clone)]
pub struct RequireToken {
    /// `None` on a node started without a secret, which makes this layer a
    /// pass-through. A value, not a missing layer, so the enabled and disabled
    /// cases are one code path with one branch.
    auth: Option<AuthConfig>,
}

impl RequireToken {
    pub fn new(auth: Option<AuthConfig>) -> Self {
        Self { auth }
    }
}

impl<B> tower_http::validate_request::ValidateRequest<B> for RequireToken {
    type ResponseBody = tonic::body::Body;

    fn validate(
        &mut self,
        request: &mut http::Request<B>,
    ) -> Result<(), http::Response<Self::ResponseBody>> {
        let Some(auth) = self.auth.as_ref() else {
            return Ok(());
        };
        if EXEMPT.contains(&request.uri().path()) {
            return Ok(());
        }

        let claims = bearer_token(request.headers())
            .and_then(|token| auth.verify(token))
            .map_err(Status::into_http)?;

        // What the handlers read back. Verification happened here, so a handler
        // never sees a token and never decides whether to trust one.
        request.extensions_mut().insert(AuthContext {
            username: claims.sub,
        });
        Ok(())
    }
}

/// The token from an `Authorization: Bearer ...` header.
///
/// The scheme is compared case-insensitively because HTTP says it is, and
/// a client that sends `bearer` is not wrong.
fn bearer_token(headers: &http::HeaderMap) -> Result<&str, Status> {
    let value = headers
        .get(http::header::AUTHORIZATION)
        .and_then(|header| header.to_str().ok())
        .ok_or_else(|| Status::unauthenticated("this request carried no authorization header"))?;
    let (scheme, token) = value.split_once(' ').ok_or_else(not_bearer)?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return Err(not_bearer());
    }
    Ok(token.trim())
}

fn not_bearer() -> Status {
    Status::unauthenticated("the authorization header is not a bearer token")
}

/// An Argon2id hash of `password`, as a PHC string.
///
/// Uses the `argon2` crate's default cost (19 MiB, two passes, one lane),
/// which OWASP recommends. It is expensive on purpose, which is also why
/// `Login` deserves rate limiting: each attempt costs the node 19 MiB and a
/// few milliseconds. The PHC string records its parameters, so a hash made
/// with older parameters still verifies after they change.
pub async fn hash_password(password: String) -> Result<String, Status> {
    blocking("hashing", move || {
        Argon2::default()
            .hash_password(password.as_bytes())
            .map(|hash| hash.to_string())
            .map_err(|e| Status::internal(format!("could not hash a password: {e}")))
    })
    .await
}

/// Whether `password` is the one `hash` was made from.
///
/// A malformed stored hash is an error, not `false`: it means the record is
/// corrupt, which is not a wrong password and must not be reported as one.
pub async fn verify_password(password: String, hash: String) -> Result<bool, Status> {
    blocking("verifying", move || {
        let parsed = PasswordHash::new(&hash)
            .map_err(|e| Status::internal(format!("a stored password hash was unreadable: {e}")))?;
        Ok(Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok())
    })
    .await
}

/// `verify_password`, for a login where there may be no stored hash.
///
/// A missing account is checked against a throwaway hash instead of
/// returning early, so a login as a user who does not exist takes as long
/// as one who does. Otherwise the response time would show which usernames
/// exist, and a username is half of a credential.
pub async fn verify_password_or_dummy(
    password: String,
    hash: Option<String>,
) -> Result<bool, Status> {
    match hash {
        Some(hash) => verify_password(password, hash).await,
        None => {
            verify_password(password, dummy_hash().to_string()).await?;
            Ok(false)
        }
    }
}

/// A hash of nothing in particular, for [`verify_password_or_dummy`].
/// Computed once: it never changes, and it is expensive to compute on
/// purpose.
fn dummy_hash() -> &'static str {
    static DUMMY: OnceLock<String> = OnceLock::new();
    DUMMY.get_or_init(|| {
        Argon2::default()
            .hash_password(b"no such account")
            .expect("hashing with the default parameters cannot fail")
            .to_string()
    })
}

/// Run `f` on the blocking pool.
///
/// Argon2 is slow and memory-hungry on purpose, so it must not run on a
/// runtime worker: a few concurrent logins would stall every other request
/// on the node, replication included.
async fn blocking<T, F>(what: &'static str, f: F) -> Result<T, Status>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, Status> + Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| Status::internal(format!("the task {what} a password failed: {e}")))?
}

#[cfg(test)]
mod tests;
