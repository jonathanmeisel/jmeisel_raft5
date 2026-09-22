//! The reserved keyspace: keys the cluster keeps for itself.
//!
//! User accounts live in the store as ordinary key/value pairs, so they
//! replicate, snapshot, and survive restarts through exactly the
//! machinery that already exists -- there is no second database to keep
//! in sync with this one. What they must not be is *reachable* from the
//! client API: a caller who could write `\0auth/user/alice` could grant
//! itself admin, and one who could read it would have the password hash
//! to attack offline, at its leisure.
//!
//! So one prefix byte is carved out of the key space and made invisible
//! to that API. `0` rather than `0xff` because it is never the first
//! byte of a key anyone would choose, and because it gives the reserved
//! range a clean upper bound -- everything below `\x01` -- for an
//! internal scan.
//!
//! # Where this is enforced
//!
//! At the RPC boundary (`crate::grpc`), never here and never in
//! storage. Storage has to replicate and snapshot these keys like any
//! others, and the server's own writes -- creating a user, say -- go
//! through the same `Locks` and `append_writes` path a client's write
//! does. The only thing separating the two is which side of the gRPC
//! handler they entered from, so that is the only place the distinction
//! can be drawn.

use tonic::Status;

/// First byte of every key the cluster reserves for itself.
pub const RESERVED: u8 = 0;

/// Everything under `RESERVED`, as a prefix to build keys from. Kept
/// together with [`is_reserved`] so that the test that they agree is
/// looking at both halves of the same invariant.
const USER_PREFIX: &[u8] = b"\0auth/user/";

/// Whether `key` belongs to the cluster rather than to a client.
///
/// The empty key is *not* reserved -- it has no first byte to be
/// `RESERVED` -- which leaves it as writable as it is today. That is
/// deliberate: this predicate exists to carve out a new region, not to
/// quietly retire a key clients can currently use.
pub fn is_reserved(key: &[u8]) -> bool {
    key.first() == Some(&RESERVED)
}

/// Where `username`'s account record is stored.
///
/// Sound only for a username [`validate_username`] has accepted: the
/// guarantee that two distinct usernames cannot produce the same key,
/// and that none can produce a key outside the reserved range, is
/// entirely that function's doing.
pub fn user_key(username: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(USER_PREFIX.len() + username.len());
    key.extend_from_slice(USER_PREFIX);
    key.extend_from_slice(username.as_bytes());
    key
}

/// How long a username may be. Bounded so that a username cannot be
/// used to write an arbitrarily large key.
const MAX_USERNAME_LEN: usize = 64;

/// Checks that `username` is one [`user_key`] can safely build a key
/// from, returning the `Status` to refuse the request with if not.
///
/// The character set is the point, not the length. `user_key` appends
/// the name to a fixed prefix with no separator or escaping after it,
/// so any byte that could make two different names collide -- or could
/// let a name climb back out of the prefix -- has to be excluded here
/// rather than handled there. Restricting the set to characters that
/// cannot do either is what makes that append safe, and is cheaper to
/// be confident in than an escaping scheme would be.
pub fn validate_username(username: &str) -> Result<(), Status> {
    if username.is_empty() {
        return Err(Status::invalid_argument("a username cannot be empty"));
    }
    if username.len() > MAX_USERNAME_LEN {
        return Err(Status::invalid_argument(format!(
            "a username may be at most {MAX_USERNAME_LEN} bytes"
        )));
    }
    if !username
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    {
        return Err(Status::invalid_argument(
            "a username may contain only letters, digits, '.', '_' and '-'",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
