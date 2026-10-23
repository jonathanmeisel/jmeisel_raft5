use super::*;

fn valid(username: &str) -> bool {
    validate_username(username).is_ok()
}

/// The empty key has no first byte, so it is not reserved: clients can
/// write it, and this must not change that.
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
    // The reserved byte anywhere but first is a normal key: the prefix is set
    // aside, not the byte.
    assert!(!is_reserved(b"alice\0"));
}

#[test]
fn a_user_key_is_reserved() {
    assert!(is_reserved(&user_key("alice")));
}

/// The property `user_key` must have: every name `validate_username`
/// accepts lands inside the reserved range, so no account record can be
/// written where a client could reach it.
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

/// `user_key` appends the name to its prefix with no separator after it, so
/// this property makes that append safe.
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
/// the name back at the top of the reserved range, and anything else that
/// could make two names meet in one key.
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
/// appended to a fixed prefix with nothing after it, so the character set
/// must stop a key from escaping.
#[test]
fn every_valid_invite_id_produces_a_reserved_key() {
    for id in [
        "a".repeat(INVITE_ID_LEN),
        "-".repeat(INVITE_ID_LEN),
        "_".repeat(INVITE_ID_LEN),
        // The shape a real id has: `invite_id` is base64 of a SHA-256.
        crate::auth::invite_id("some token"),
    ] {
        assert!(
            validate_invite_id(&id).is_ok(),
            "{id:?} should be a valid invite id"
        );
        assert!(is_reserved(&invite_key(&id)));
    }
}

/// Nobody picks an id (every id this cluster issues is base64 of a hash),
/// so anything not exactly that shape is refused, not accepted.
#[test]
fn an_invite_id_that_is_not_a_hash_is_refused() {
    for id in [
        "",
        "short",
        &"a".repeat(INVITE_ID_LEN - 1),
        &"a".repeat(INVITE_ID_LEN + 1),
        // Right length, wrong alphabet: base64 padding, the separator the prefix is
        // built from, and a NUL that would put the id back at the top of the
        // reserved range.
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
/// stating: no username can produce an invitation's key, or the reverse.
#[test]
fn a_user_key_and_an_invite_key_can_never_meet() {
    let id = crate::auth::invite_id("some token");
    assert_ne!(user_key(&id), invite_key(&id));
    assert!(!user_key(&id).starts_with(INVITE_PREFIX));
}

/// The general form of the test above, for every range in the reserved
/// space.
///
/// Every record range must be disjoint from every other, and from the one
/// key that stands alone. A prefix of another prefix would be worse than a
/// collision: a listing over the shorter one would return the longer one's
/// records as its own.
#[test]
fn no_two_reserved_prefixes_can_ever_meet() {
    let ranges = [
        ("user", USER_PREFIX),
        ("invite", INVITE_PREFIX),
        ("group", GROUP_PREFIX),
        ("acl", ACL_PREFIX),
        // Not a range: the whole key, treated as one so the "nothing under
        // another" check covers it too.
        ("epoch", EPOCH_KEY),
    ];
    for (i, (one, a)) in ranges.iter().enumerate() {
        assert!(is_reserved(a), "{one} prefix should be reserved");
        for (other, b) in &ranges[i + 1..] {
            assert!(
                !a.starts_with(b) && !b.starts_with(a),
                "the {one} and {other} ranges overlap"
            );
        }
    }
}

/// A username and a group name have the same shape, so what matters is that
/// they land in different places, and that neither can reach the epoch key,
/// which has no separator to hide behind.
#[test]
fn a_group_key_is_reserved_and_meets_nothing_else() {
    for name in ["a", "oncall", "A.B_c-9", &"g".repeat(MAX_USERNAME_LEN)] {
        assert!(validate_group_name(name).is_ok(), "{name:?} should be valid");
        let key = group_key(name);
        assert!(is_reserved(&key));
        assert_ne!(key, user_key(name));
        assert_eq!(key, [GROUP_PREFIX, name.as_bytes()].concat());
        assert_ne!(key, EPOCH_KEY.to_vec());
    }
}

#[test]
fn distinct_group_names_produce_distinct_keys() {
    let names = ["a", "ab", "b", "oncall", "on.call", "on_call", "on-call"];
    for (i, one) in names.iter().enumerate() {
        for other in &names[i + 1..] {
            assert_ne!(group_key(one), group_key(other), "{one:?} and {other:?}");
        }
    }
}

#[test]
fn a_group_name_is_held_to_the_same_rule_as_a_username() {
    for name in ["", "on/call", "on\0call", "../etc", "on call", "ali\u{00e9}"] {
        assert!(validate_group_name(name).is_err(), "{name:?}");
        assert!(validate_username(name).is_err(), "{name:?}");
    }
    assert!(validate_group_name(&"g".repeat(MAX_USERNAME_LEN)).is_ok());
    assert!(validate_group_name(&"g".repeat(MAX_USERNAME_LEN + 1)).is_err());
}

/// The property that makes `acl_key` safe for bytes no character set
/// limits: any prefix, including ones that would escape an unencoded append,
/// lands inside the ACL range and comes back out unchanged.
#[test]
fn any_key_prefix_at_all_round_trips_through_an_acl_key() {
    let prefixes: [&[u8]; 9] = [
        b"",
        b"a",
        b"app/",
        b"\0",
        b"\0auth/user/root",
        b"/",
        b"../etc",
        &[0xff, 0x00, 0xff],
        &[0u8; MAX_ACL_PREFIX_LEN],
    ];
    for prefix in prefixes {
        assert!(validate_acl_prefix(prefix).is_ok(), "{prefix:?}");
        let key = acl_key(prefix);
        assert!(is_reserved(&key), "{prefix:?} should produce a reserved key");
        assert!(key.starts_with(ACL_PREFIX), "{prefix:?} escaped its range");
        assert_eq!(
            acl_prefix_from_key_suffix(&key[ACL_PREFIX.len()..]).as_deref(),
            Some(prefix),
            "{prefix:?} did not round trip"
        );
    }
}

/// Injectivity, over the pairs most likely to break an encoding: one prefix
/// of another, and the empty one against everything.
#[test]
fn distinct_key_prefixes_produce_distinct_acl_keys() {
    let prefixes: [&[u8]; 6] = [b"", b"a", b"ab", b"b", b"app/", b"app/sub/"];
    for (i, one) in prefixes.iter().enumerate() {
        for other in &prefixes[i + 1..] {
            assert_ne!(acl_key(one), acl_key(other), "{one:?} and {other:?}");
        }
    }
}

#[test]
fn an_over_long_acl_prefix_is_refused() {
    assert!(validate_acl_prefix(&[0u8; MAX_ACL_PREFIX_LEN]).is_ok());
    assert!(validate_acl_prefix(&[0u8; MAX_ACL_PREFIX_LEN + 1]).is_err());
}

/// A suffix this module could not have written is a corrupt record, not a
/// request to refuse, so it reports instead of panicking.
#[test]
fn a_suffix_that_is_not_hex_decodes_to_nothing() {
    for suffix in [b"abc".as_slice(), b"zz", b"0g", b"AB"] {
        assert_eq!(acl_prefix_from_key_suffix(suffix), None, "{suffix:?}");
    }
}
