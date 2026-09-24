use super::*;

fn valid(username: &str) -> bool {
    validate_username(username).is_ok()
}

/// The empty key has no first byte, so it is not reserved -- clients can
/// write it today and this must not change that.
#[test]
fn the_empty_key_is_not_reserved() {
    assert!(!is_reserved(b""));
}

#[test]
fn only_keys_starting_with_the_reserved_byte_are_reserved() {
    assert!(is_reserved(&[RESERVED]));
    assert!(is_reserved(b"\0anything at all"));

    assert!(!is_reserved(&[1]));
    assert!(!is_reserved(b"alice"));
    // The reserved byte anywhere but first is an ordinary key: the
    // prefix is what is carved out, not the byte.
    assert!(!is_reserved(b"alice\0"));
}

#[test]
fn a_user_key_is_reserved() {
    assert!(is_reserved(&user_key("alice")));
}

/// The property `user_key` actually has to have: every name
/// `validate_username` accepts lands inside the reserved range, so no
/// account record can be written somewhere a client could reach it.
#[test]
fn every_valid_username_produces_a_reserved_key() {
    for username in ["a", "alice", "A.B_c-9", &"u".repeat(MAX_USERNAME_LEN)] {
        assert!(valid(username), "{username:?} should be a valid username");
        assert!(
            is_reserved(&user_key(username)),
            "{username:?} should produce a reserved key"
        );
    }
}

/// `user_key` appends the name to its prefix with no separator after it,
/// so this is the property that makes that append safe.
#[test]
fn distinct_usernames_produce_distinct_keys() {
    let names = ["a", "ab", "b", "alice", "alice.bob", "alice_bob", "a-b"];
    for (i, one) in names.iter().enumerate() {
        for other in &names[i + 1..] {
            assert_ne!(
                user_key(one),
                user_key(other),
                "{one:?} and {other:?} collided"
            );
        }
    }
}

#[test]
fn an_empty_username_is_refused() {
    assert!(!valid(""));
}

#[test]
fn an_over_long_username_is_refused() {
    assert!(valid(&"u".repeat(MAX_USERNAME_LEN)));
    assert!(!valid(&"u".repeat(MAX_USERNAME_LEN + 1)));
}

/// The separator `user_key` builds its prefix from, a NUL that would put
/// the name back at the top of the reserved range, and anything else
/// that could make two names meet in one key.
#[test]
fn a_username_cannot_contain_anything_that_would_let_it_escape_its_prefix() {
    for username in [
        "alice/bob",
        "alice\0bob",
        "../etc",
        "alice bob",
        "alice\nbob",
        "ali\u{00e9}",
    ] {
        assert!(!valid(username), "{username:?} should be refused");
    }
}

#[test]
fn an_invite_key_is_reserved() {
    assert!(is_reserved(&invite_key(&"a".repeat(INVITE_ID_LEN))));
}

/// The same property `user_key` needs, for the same reason: the id is
/// appended to a fixed prefix with nothing after it, so the character
/// set has to be what stops a key climbing back out.
#[test]
fn every_valid_invite_id_produces_a_reserved_key() {
    for id in [
        "a".repeat(INVITE_ID_LEN),
        "-".repeat(INVITE_ID_LEN),
        "_".repeat(INVITE_ID_LEN),
        // The shape a real one has: `invite_id` is base64 of a SHA-256.
        crate::auth::invite_id("some token"),
    ] {
        assert!(
            validate_invite_id(&id).is_ok(),
            "{id:?} should be a valid invite id"
        );
        assert!(is_reserved(&invite_key(&id)));
    }
}

/// An id is never a name anybody picked -- every one this cluster issues
/// is the base64 of a hash -- so anything that is not exactly that shape
/// is refused rather than accommodated.
#[test]
fn an_invite_id_that_is_not_a_hash_is_refused() {
    for id in [
        "",
        "short",
        &"a".repeat(INVITE_ID_LEN - 1),
        &"a".repeat(INVITE_ID_LEN + 1),
        // Right length, wrong alphabet: base64 padding, the separator
        // the prefix is built from, and a NUL that would put the id back
        // at the top of the reserved range.
        &format!("{}=", "a".repeat(INVITE_ID_LEN - 1)),
        &format!("{}/", "a".repeat(INVITE_ID_LEN - 1)),
        &format!("{}\0", "a".repeat(INVITE_ID_LEN - 1)),
    ] {
        assert!(
            validate_invite_id(id).is_err(),
            "{id:?} should be refused as an invite id"
        );
    }
}

/// Accounts and invitations share the `\0auth/` prefix, so this is worth
/// stating: no username can produce the key of an invitation, or the
/// other way round.
#[test]
fn a_user_key_and_an_invite_key_can_never_meet() {
    let id = crate::auth::invite_id("some token");
    assert_ne!(user_key(&id), invite_key(&id));
    assert!(!user_key(&id).starts_with(INVITE_PREFIX));
}
