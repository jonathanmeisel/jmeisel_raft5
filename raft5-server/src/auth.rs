//! Who a request is from, and how that is established.
//!
//! Accounts are ordinary key/value pairs in the store, under the prefix
//! `crate::keyspace` hides from the client API, so they replicate and
//! snapshot like everything else. A password is never stored, only an
//! Argon2id hash of it. `Login` checks that hash and hands back a signed
//! token; every later request carries the token instead of the password.
//!
//! # The signing key
//!
//! One symmetric secret, HS256, the same bytes on every node. A token
//! has to verify wherever it lands -- a stale read is answered by
//! whichever node received it, not necessarily the one that issued the
//! token -- so the key cannot be per-node. It is supplied by the
//! operator rather than generated, which keeps key material out of the
//! log and out of snapshots, and lets it be mounted exactly the way the
//! TLS material will be.
//!
//! # What a token does not say
//!
//! [`Claims`] carries the username and nothing about what that user may
//! do. Admin rights are re-read from the store on each request that
//! needs them, because a claim baked into a token cannot be taken back:
//! demoting an admin would leave their existing token omnipotent until
//! it expired. The admin RPCs are rare and already pay a raft round
//! trip, so the extra read costs nothing worth having.
//!
//! The converse gap is real and not solved here: a *deleted* user's
//! token still passes verification, for non-admin requests, until it
//! expires. Bounding that is what the token lifetime is for.
//!
//! # Invitations
//!
//! An account can also come into being without an administrator ever
//! choosing its password. [`InviteSecret::mint`] produces 32 bytes from
//! the OS, handed to the issuing administrator once; what is stored is
//! the SHA-256 of them, under a key derived from that same hash. So the
//! record is looked up *by* the thing being checked, and there is no
//! comparison to get wrong and no secret in the log to leak.
//!
//! Argon2 is deliberately not used here, and the difference is worth
//! being explicit about. Argon2's cost exists to make guessing a
//! human-chosen password infeasible; a 256-bit secret from `getrandom`
//! is already past guessing, so the only thing a slow hash would buy is
//! a slow unauthenticated RPC -- see `grpc::KvService::redeem_invite`,
//! which is open to anyone holding a token and therefore to anyone at
//! all.
//!
//! # Not yet TLS
//!
//! `Login` takes a plaintext password over the wire, and `RedeemInvite`
//! takes both a password and an invitation. Until the client API is
//! behind TLS, anyone who can see the traffic can see passwords, can
//! replay tokens, and can spend an invitation before its recipient
//! does.

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

/// The one algorithm this cluster will sign or accept.
///
/// Pinned on the verifying side as well as the signing side: a verifier
/// that accepted whatever the token's own header asked for would accept
/// `none`, and would let an attacker holding only the public half of
/// some other scheme present it as an HMAC secret.
const ALGORITHM: Algorithm = Algorithm::HS256;

/// What a token asserts. See the module doc for what it deliberately
/// does not.
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
/// Inserted into the request's extensions by that layer and read back by
/// the handlers that need to know -- see `crate::grpc`. Its presence
/// means the token verified; it says nothing about what the user may do,
/// which is read from the store.
#[derive(Clone, Debug)]
pub struct AuthContext {
    pub username: String,
}

/// The version stamped into every record this build writes.
const RECORD_VERSION: u8 = 1;

/// One account, as stored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserRecord {
    /// First field on purpose. `postcard` is not self-describing, so
    /// nothing in the encoding says what shape it is; putting the
    /// version first at least means a future reader decodes the version
    /// before anything whose layout may have moved. It is a way to give
    /// a clear answer to a record from a newer node, not a guarantee
    /// that any such record decodes at all.
    pub version: u8,
    pub username: String,
    pub password_hash: String,
    pub is_admin: bool,
}

impl UserRecord {
    pub fn new(username: String, password_hash: String, is_admin: bool) -> Self {
        Self {
            version: RECORD_VERSION,
            username,
            password_hash,
            is_admin,
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

/// One outstanding invitation, as stored.
///
/// The secret itself is absent by construction: this record is found by
/// hashing the secret, so holding the record tells you what the invite
/// is *for* without telling you how to spend it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InviteRecord {
    /// First, for the reason [`UserRecord::version`] is first.
    pub version: u8,
    /// The administrator who issued it. Kept so that a listing can say
    /// where an invitation came from -- with accounts creating accounts,
    /// "who let this person in" is otherwise unanswerable.
    pub issued_by: String,
    /// What the account made from this will be. Fixed at issue time by
    /// an administrator; the redeemer never gets a say.
    pub is_admin: bool,
    /// The one username this invite may create, if it was pinned to one.
    pub username: Option<String>,
    /// When it stops being redeemable, by the clock of the leader that
    /// wrote it.
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

/// How many bytes of entropy an invitation is worth.
///
/// 32, so that guessing one is not a thing anyone attempts and the
/// cluster does not have to make guessing expensive to be safe from it.
const INVITE_SECRET_BYTES: usize = 32;

/// The longest token this node will even hash.
///
/// `RedeemInvite` answers callers with no account, so the work it does
/// before deciding anything is work an anonymous caller can ask for. A
/// token longer than one this cluster mints is refused on sight rather
/// than hashed.
pub const MAX_INVITE_TOKEN_LEN: usize = 128;

/// A freshly minted invitation: the secret to hand over, and the id the
/// cluster will know it by.
pub struct InviteSecret {
    /// Given out exactly once, by the response to `CreateInvite`.
    pub token: String,
    /// `invite_id(&token)`, carried alongside so the caller does not
    /// recompute what minting already knows.
    pub id: String,
}

impl InviteSecret {
    /// A new invitation, from the operating system's randomness.
    ///
    /// Not `txn::fresh_id`, which is a `RandomState` hash and is fine
    /// for naming a transaction and useless for anything an attacker
    /// would like to predict. This is a credential, so it comes from
    /// `getrandom` and nothing else.
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
/// This is the whole of the lookup and the whole of the check. Because
/// the record's key *is* this hash, a presented token either names a
/// record or it does not; there is no stored secret to compare against,
/// so there is no comparison whose timing could leak one.
///
/// Hashes the token's characters rather than the bytes they decode to,
/// so a token that is not base64 at all is simply a token that names no
/// record -- there is no parse here to fail, and no malformed-input path
/// distinguishable from a wrong-secret one.
pub fn invite_id(token: &str) -> String {
    BASE64.encode(Sha256::digest(token.as_bytes()))
}

/// Wall-clock milliseconds since the epoch.
///
/// An invitation's lifetime is the leader's to measure: the issuer's
/// clock and the recipient's are both outside this cluster, and a
/// credential's expiry cannot be left to either. Before the epoch is
/// treated as the epoch -- a node whose clock says 1969 has a problem
/// this function cannot fix, and saturating keeps it from being an
/// invitation that never lapses.
pub fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// The administrator a node was started with, for the one window where
/// there is no stored account to authenticate against.
///
/// Hashed once at startup rather than kept in the clear, so the
/// operator's password does not sit in this process's memory for its
/// whole life -- and so that the hash is already in hand when
/// `Bootstrap` writes the real record.
#[derive(Clone)]
pub struct InitialAdmin {
    pub username: String,
    pub password_hash: String,
}

/// Everything a node needs to issue and check tokens. Absent when the
/// node was started without a secret, which is what leaves the cluster
/// open.
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

    /// A token for `username`, and the instant it stops being valid.
    ///
    /// The expiry is handed back rather than left for the caller to dig
    /// out of the token, so a client never has to parse a JWT to know
    /// when to log in again.
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

    /// The claims of a token this cluster signed and that has not
    /// expired, or why not.
    ///
    /// The reason is reported rather than flattened to "no": the holder
    /// of an expired token already knows everything this tells them, and
    /// being told which of the two it was is what lets a client log in
    /// again instead of giving up.
    pub fn verify(&self, token: &str) -> Result<Claims, Status> {
        // `Validation::new` pins the accepted algorithm to this one --
        // see `ALGORITHM` -- and requires an `exp` claim to be present,
        // so a token without one is rejected rather than treated as
        // eternal. The default leeway (60s) is left alone: the node that
        // issued a token is usually not the node checking it, and their
        // clocks are only as close as the operator has made them.
        let validation = Validation::new(ALGORITHM);
        decode::<Claims>(token, &self.decoding, &validation)
            .map(|data| data.claims)
            .map_err(|e| Status::unauthenticated(format!("the token was not accepted: {e}")))
    }
}

/// The RPCs that may be called with no token at all.
///
/// An allowlist rather than a denylist, so that an RPC added to the
/// service and forgotten about here is protected by default rather than
/// exposed by default.
///
/// `Login` is what issues a token, so it cannot require one. `GetConfig`
/// is how a client finds the leader, which it has to do before it has
/// anywhere to send a login; it discloses the cluster's membership
/// addresses, which is what a client was configured with to begin with.
///
/// `RedeemInvite` is the third and the only one that writes. It is
/// exempt because the caller is by definition someone with no account --
/// that is what they are here to fix -- and requiring a token to get a
/// token is the same circle `Login` is exempt to break. What stands in
/// place of a token is the invitation: 32 bytes an administrator drew
/// and handed over, which the handler checks before it does anything
/// else. An invite that is unknown, spent, revoked or lapsed buys
/// exactly what no token at all buys.
const EXEMPT: [&str; 3] = [
    "/raft5.client.Kv/Login",
    "/raft5.client.Kv/GetConfig",
    "/raft5.client.Kv/RedeemInvite",
];

/// Turns away any request to the client API that does not carry a token
/// this cluster signed.
///
/// A `tower` layer rather than a tonic interceptor because an
/// interceptor cannot see which method is being called -- tonic drops
/// the URI before handing the request over -- and without that there is
/// no way to exempt `Login` from needing the thing `Login` hands out.
///
/// It only ever authenticates. What a caller may *do* is decided in the
/// handlers, from the store, because that answer can change while a
/// token is still valid.
#[derive(Clone)]
pub struct RequireToken {
    /// `None` on a node started without a secret, which makes this layer
    /// a pass-through. Held as a value rather than expressed by leaving
    /// the layer off, so that the enabled and disabled cases are the
    /// same code path with one branch between them.
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

        // What the handlers read back out. Verifying happened here, so a
        // handler never sees a token and never has to decide whether to
        // trust one.
        request.extensions_mut().insert(AuthContext {
            username: claims.sub,
        });
        Ok(())
    }
}

/// The token out of an `Authorization: Bearer ...` header.
///
/// The scheme is compared case-insensitively because HTTP says it is
/// case-insensitive, and a client that sends `bearer` is not wrong.
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
/// The cost parameters are the `argon2` crate's defaults -- 19 MiB,
/// two passes, one lane -- which are what OWASP currently recommends.
/// They are deliberately expensive, which is the point of them and also
/// the reason `Login` is worth rate limiting: each attempt costs the
/// node 19 MiB and a few milliseconds. The PHC string records the
/// parameters it was made with, so raising them later re-hashes records
/// one at a time as their owners log in, rather than invalidating them.
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
/// A malformed stored hash is an error rather than a `false`: it means
/// the record is corrupt, which is not the same as the password being
/// wrong and should not be reported to the caller as though it were.
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
/// returning early, so that logging in as a user who does not exist
/// costs the same as logging in as one who does. Without this, the time
/// a refusal takes says which usernames are real, and a username is half
/// of a credential.
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
/// Computed once: it is a constant, and computing it is deliberately
/// expensive.
fn dummy_hash() -> &'static str {
    static DUMMY: OnceLock<String> = OnceLock::new();
    DUMMY.get_or_init(|| {
        Argon2::default()
            .hash_password(b"no such account")
            .expect("hashing with the default parameters cannot fail")
            .to_string()
    })
}

/// Runs `f` on the blocking pool.
///
/// Argon2 is meant to be slow and memory-hungry, so it must not run on a
/// runtime worker: a handful of concurrent logins would otherwise stall
/// every other request on the node, replication included.
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
