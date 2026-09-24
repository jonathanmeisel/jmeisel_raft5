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

/// Where outstanding invitations live, under the same reservation and
/// for the same reasons -- and with one of its own. An invite record is
/// a credential's hash and the rights that credential buys; a client who
/// could read one would learn which accounts are about to exist and
/// which of them will be administrators, and one who could write one
/// would mint themselves an invitation.
pub const INVITE_PREFIX: &[u8] = b"\0auth/invite/";

/// Where groups live. Public because listing them is a client-facing
/// operation -- unlike accounts, a group is meant to be discoverable:
/// naming one in an ACL is useless if nobody can find out it exists.
pub const GROUP_PREFIX: &[u8] = b"\0auth/group/";

/// Where the access control lists live, one record per key prefix they
/// govern. Under the reservation for the plainest of the usual reasons:
/// a client who could write one would grant itself whatever it liked.
///
/// Note the separate top-level name rather than a fourth `\0auth/`
/// range. What lives here is keyed by a *key prefix*, not by the name of
/// a principal, so it shares neither the character set nor the encoding
/// of the three that are.
pub const ACL_PREFIX: &[u8] = b"\0acl/";

/// The one key whose sequence number is the permission epoch.
///
/// Every transaction that changes a group or an ACL also writes here, so
/// asking "has anything about permissions changed since I last looked?"
/// is a single seek that never reads a value -- the seq lives in the row
/// key. `crate::perms` explains what that buys.
///
/// Deliberately not under `\0auth/user/`, `\0auth/invite/` or
/// `\0auth/group/`: it has no trailing separator and could not be
/// confused with a record in any of them, which
/// `no_two_reserved_prefixes_can_ever_meet` checks rather than assumes.
pub const EPOCH_KEY: &[u8] = b"\0auth/epoch";

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

/// Where the invitation with this id is stored.
///
/// Sound for an id [`validate_invite_id`] has accepted, on exactly the
/// terms [`user_key`] is sound for a validated username.
pub fn invite_key(invite_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(INVITE_PREFIX.len() + invite_id.len());
    key.extend_from_slice(INVITE_PREFIX);
    key.extend_from_slice(invite_id.as_bytes());
    key
}

/// Where `name`'s group record is stored.
///
/// Sound for a name [`validate_group_name`] has accepted, on exactly the
/// terms [`user_key`] is sound for a validated username.
pub fn group_key(name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(GROUP_PREFIX.len() + name.len());
    key.extend_from_slice(GROUP_PREFIX);
    key.extend_from_slice(name.as_bytes());
    key
}

/// Where the access control list governing `prefix` is stored.
///
/// The one reserved key built from something the *client* chose the
/// bytes of. A username or an invite id can be restricted to a character
/// set, which is what makes appending it raw safe; a key prefix cannot
/// be -- it is an arbitrary byte string, and one that is allowed to
/// contain the separator this prefix ends with, or a NUL, or nothing at
/// all. So it is hex-encoded instead: injective by construction, fixed
/// two bytes per input byte, and using an alphabet that cannot contain
/// anything structural. Order-preserving too, which nothing yet relies
/// on but which costs nothing to keep.
///
/// Sound for a prefix [`validate_acl_prefix`] has accepted, which is
/// only a length bound -- every byte string of that length or less has a
/// key here.
pub fn acl_key(prefix: &[u8]) -> Vec<u8> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut key = Vec::with_capacity(ACL_PREFIX.len() + prefix.len() * 2);
    key.extend_from_slice(ACL_PREFIX);
    for byte in prefix {
        key.push(HEX[usize::from(byte >> 4)]);
        key.push(HEX[usize::from(byte & 0x0f)]);
    }
    key
}

/// The prefix an [`acl_key`] governs, recovered from the part of that
/// key after [`ACL_PREFIX`] -- which is what a listing over that range
/// is handed.
///
/// Returns `None` for a suffix this module could not have written, which
/// is a corrupt record rather than a request to refuse.
pub fn acl_prefix_from_key_suffix(suffix: &[u8]) -> Option<Vec<u8>> {
    fn nibble(b: u8) -> Option<u8> {
        match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'a'..=b'f' => Some(b - b'a' + 10),
            _ => None,
        }
    }
    if !suffix.len().is_multiple_of(2) {
        return None;
    }
    suffix
        .chunks_exact(2)
        .map(|pair| Some((nibble(pair[0])? << 4) | nibble(pair[1])?))
        .collect()
}

/// How long a key prefix an ACL may govern.
///
/// Not a correctness bound -- `acl_key` is injective for any length --
/// but a bound on how large a reserved key one call can create, in the
/// spirit of [`MAX_USERNAME_LEN`]. Generous enough that a prefix anybody
/// would organise a keyspace around fits.
pub const MAX_ACL_PREFIX_LEN: usize = 256;

/// Checks that `prefix` is one an ACL may be written for.
///
/// The empty prefix is explicitly allowed: it is the one that matches
/// every key, and so is how a cluster is locked down.
pub fn validate_acl_prefix(prefix: &[u8]) -> Result<(), Status> {
    if prefix.len() > MAX_ACL_PREFIX_LEN {
        return Err(Status::invalid_argument(format!(
            "an ACL may govern a key prefix of at most {MAX_ACL_PREFIX_LEN} bytes"
        )));
    }
    Ok(())
}

/// How long an invite id is: a SHA-256 in URL-safe base64 without
/// padding, which is always 43 characters.
const INVITE_ID_LEN: usize = 43;

/// Checks that `invite_id` is one [`invite_key`] can safely build a key
/// from.
///
/// Unlike a username, this is not a name anybody chose -- every id this
/// cluster ever issues is the base64 of a hash, so the check is for
/// exactly that shape rather than for a permissive set of characters
/// someone might reasonably want. An id that is not one the cluster
/// could have minted is refused before it reaches the store, which keeps
/// the append in `invite_key` as safe as the one in `user_key`.
pub fn validate_invite_id(invite_id: &str) -> Result<(), Status> {
    if invite_id.len() != INVITE_ID_LEN
        || !invite_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
    {
        return Err(Status::invalid_argument(
            "that is not an invite id; an id is 43 characters of URL-safe base64, \
             as CreateInvite and ListInvites give them",
        ));
    }
    Ok(())
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
    validate_name(username, "username").map_err(Status::invalid_argument)
}

/// Checks that `name` is one [`group_key`] can safely build a key from.
///
/// The same rule as a username, for the same reason -- `group_key` and
/// `user_key` are the same append -- and deliberately the same *set*,
/// rather than merely a compatible one. A group and a user are both
/// principals an ACL can name, and a character legal in one but not the
/// other would be a difference nobody reading an ACL could guess at.
pub fn validate_group_name(name: &str) -> Result<(), Status> {
    validate_name(name, "group name").map_err(Status::invalid_argument)
}

/// The shared body of the two above: the character set and the bound
/// that make appending a name to a fixed prefix injective and
/// escape-proof. `what` completes "a ... may contain only".
fn validate_name(name: &str, what: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err(format!("a {what} cannot be empty"));
    }
    if name.len() > MAX_USERNAME_LEN {
        return Err(format!("a {what} may be at most {MAX_USERNAME_LEN} bytes"));
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    {
        return Err(format!(
            "a {what} may contain only letters, digits, '.', '_' and '-'"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
