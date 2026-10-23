//! The reserved keyspace: keys the cluster keeps for itself.
//!
//! User accounts are normal key/value pairs in the store, so they
//! replicate, snapshot and survive restarts through the existing
//! machinery, with no second database to keep in sync. But the client API
//! must not reach them: a caller who could write `\0auth/user/alice` could
//! make itself admin, and one who could read it would get the password hash
//! to attack offline.
//!
//! So one prefix byte is set aside and hidden from that API. `0`, not
//! `0xff`, because nobody would choose it as a key's first byte, and
//! because it gives the reserved range a clean upper bound (everything
//! below `\x01`) for an internal scan.
//!
//! # Where this is enforced
//!
//! At the RPC boundary (`crate::grpc`), never here and never in storage.
//! Storage must replicate and snapshot these keys like any others, and the
//! server's own writes (creating a user, for example) use the same `Locks`
//! and `append_writes` path as a client's write. The only difference is
//! which gRPC handler they came through, so that is the only place to draw
//! the line.

use tonic::Status;

/// The first byte of every key the cluster reserves for itself.
pub const RESERVED: u8 = 0;

/// The prefix for account keys, under `RESERVED`. Kept next to
/// [`is_reserved`], so the test that they agree checks both halves of one
/// invariant.
const USER_PREFIX: &[u8] = b"\0auth/user/";

/// Where open invitations live, under the same reservation, for the same
/// reasons and one more. An invite record is a credential's hash and the
/// rights it gives: a client who could read one would learn which accounts
/// are about to exist and which will be administrators, and one who could
/// write one would create its own invitation.
pub const INVITE_PREFIX: &[u8] = b"\0auth/invite/";

/// Where groups live. Public because listing them is a client operation:
/// unlike accounts, groups are meant to be discoverable, because naming one
/// in an ACL is useless if nobody can find out it exists.
pub const GROUP_PREFIX: &[u8] = b"\0auth/group/";

/// Where the access control lists live, one record per governed key prefix.
/// Reserved for the plainest reason: a client who could write one would
/// grant itself anything.
///
/// A separate top-level name, not a fourth `\0auth/` range: these records
/// are keyed by a key prefix, not by a principal's name, so they share
/// neither the character set nor the encoding of the other three.
pub const ACL_PREFIX: &[u8] = b"\0acl/";

/// The one key whose seq is the permission epoch.
///
/// Every transaction that changes a group or an ACL also writes here, so
/// "has anything about permissions changed since I last looked?" is one
/// seek that never reads a value (the seq is in the row key).
/// `crate::perms` explains what that gives.
///
/// Not under `\0auth/user/`, `\0auth/invite/` or `\0auth/group/` on
/// purpose: it has no trailing separator, so it cannot be mistaken for a
/// record in any of them. `no_two_reserved_prefixes_can_ever_meet` checks
/// this.
pub const EPOCH_KEY: &[u8] = b"\0auth/epoch";

/// Whether `key` belongs to the cluster, not to a client.
///
/// The empty key is not reserved (it has no first byte to be `RESERVED`),
/// so clients can still write it. This predicate only marks the reserved
/// region; it takes no key away from clients.
pub fn is_reserved(key: &[u8]) -> bool {
    key.first() == Some(&RESERVED)
}

/// Whether a backup carries `key`.
///
/// Every key a client wrote, and from the reserved range only accounts,
/// groups, ACLs and the permission epoch. An allowlist, so a reserved range
/// added later stays out of backups until someone decides it belongs.
///
/// - Invitations stay out: they are short-lived credentials for the old
///   cluster.
/// - Checkpoints stay out: a backup holds one version per key, so no
///   checkpoint could read through it.
/// - The epoch must come along. Without it, the restored node's permission
///   cache sees epoch 0, the value of an empty cache, and never loads the
///   restored ACLs.
pub fn is_backed_up(key: &[u8]) -> bool {
    !is_reserved(key)
        || key.starts_with(USER_PREFIX)
        || key.starts_with(GROUP_PREFIX)
        || key.starts_with(ACL_PREFIX)
        || key == EPOCH_KEY
}

/// Where `username`'s account record is stored.
///
/// Safe only for a username that [`validate_username`] accepted: that
/// function alone guarantees that two different usernames cannot make the
/// same key, and that none can make a key outside the reserved range.
pub fn user_key(username: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(USER_PREFIX.len() + username.len());
    key.extend_from_slice(USER_PREFIX);
    key.extend_from_slice(username.as_bytes());
    key
}

/// Where the invitation with this id is stored.
///
/// Safe for an id that [`validate_invite_id`] accepted, on the same terms
/// as [`user_key`] for a validated username.
pub fn invite_key(invite_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(INVITE_PREFIX.len() + invite_id.len());
    key.extend_from_slice(INVITE_PREFIX);
    key.extend_from_slice(invite_id.as_bytes());
    key
}

/// Where `name`'s group record is stored.
///
/// Safe for a name that [`validate_group_name`] accepted, on the same terms
/// as [`user_key`] for a validated username.
pub fn group_key(name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(GROUP_PREFIX.len() + name.len());
    key.extend_from_slice(GROUP_PREFIX);
    key.extend_from_slice(name.as_bytes());
    key
}

/// Where the access control list that governs `prefix` is stored.
///
/// The one reserved key built from bytes the client chose. A username or
/// an invite id can be limited to a character set, which makes a raw append
/// safe. A key prefix cannot: it is any byte string, including the
/// separator this prefix ends with, a NUL, or nothing at all. So it is hex
/// encoded: injective, exactly two bytes per input byte, and from an
/// alphabet with nothing structural in it. It also keeps order, which
/// nothing relies on but costs nothing.
///
/// Safe for a prefix that [`validate_acl_prefix`] accepted, which is only a
/// length bound: every byte string up to that length has a key here.
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

/// The prefix an [`acl_key`] governs, recovered from the part of the key
/// after [`ACL_PREFIX`] (what a listing over that range returns).
///
/// Returns `None` for a suffix this module could not have written: a
/// corrupt record, not a request to refuse.
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

/// The longest key prefix an ACL may govern.
///
/// Not needed for correctness (`acl_key` is injective at any length), but
/// it bounds how large a reserved key one call can create, like
/// [`MAX_USERNAME_LEN`]. Large enough for any prefix a keyspace would be
/// organized around.
pub const MAX_ACL_PREFIX_LEN: usize = 256;

/// Check that an ACL may be written for `prefix`.
///
/// The empty prefix is allowed: it matches every key, so it is how a
/// cluster is locked down.
pub fn validate_acl_prefix(prefix: &[u8]) -> Result<(), Status> {
    if prefix.len() > MAX_ACL_PREFIX_LEN {
        return Err(Status::invalid_argument(format!(
            "an ACL may govern a key prefix of at most {MAX_ACL_PREFIX_LEN} bytes"
        )));
    }
    Ok(())
}

/// The length of an invite id: a SHA-256 in URL-safe base64 without
/// padding, always 43 characters.
const INVITE_ID_LEN: usize = 43;

/// Check that [`invite_key`] can safely build a key from `invite_id`.
///
/// Unlike a username, nobody chose this name: every id this cluster issues
/// is the base64 of a hash, so the check is for exactly that shape, not for
/// a permissive character set. An id the cluster could not have created is
/// refused before it reaches the store, which keeps the append in
/// `invite_key` as safe as the one in `user_key`.
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

/// The longest allowed username. Bounded so a username cannot be used to
/// write a huge key.
const MAX_USERNAME_LEN: usize = 64;

/// Check that [`user_key`] can safely build a key from `username`, and
/// return the `Status` to refuse the request with if not.
///
/// The character set matters more than the length. `user_key` appends the
/// name to a fixed prefix with no separator or escaping, so any byte that
/// could make two names collide, or let a name escape the prefix, must be
/// refused here. A character set that can do neither makes the append safe,
/// and is easier to trust than an escaping scheme.
pub fn validate_username(username: &str) -> Result<(), Status> {
    validate_name(username, "username").map_err(Status::invalid_argument)
}

/// Check that [`group_key`] can safely build a key from `name`.
///
/// The same rule as for usernames, for the same reason (`group_key` and
/// `user_key` are the same append), and exactly the same set, not only a
/// compatible one. Groups and users are both principals an ACL can name,
/// and a character valid in one but not the other would be a difference
/// nobody reading an ACL could guess.
pub fn validate_group_name(name: &str) -> Result<(), Status> {
    validate_name(name, "group name").map_err(Status::invalid_argument)
}

/// The shared body of the two above: the character set and length limit
/// that make appending a name to a fixed prefix injective and unable to
/// escape it. `what` completes "a ... may contain only".
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
