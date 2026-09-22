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
