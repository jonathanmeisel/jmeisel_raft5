use super::*;

fn user(name: &str) -> Principal {
    Principal::User(name.to_string())
}

fn group(name: &str) -> Principal {
    Principal::Group(name.to_string())
}

/// A snapshot built as the server builds one: from encoded records keyed
/// as a listing of each reserved range returns them.
fn snapshot(
    groups: &[(&str, GroupRecord)],
    acls: &[(&[u8], AclRecord)],
) -> Permissions {
    // Seqs ascend, so the assertions can tell records apart; the evaluation
    // does not depend on them.
    let groups = groups
        .iter()
        .enumerate()
        .map(|(i, (name, record))| {
            (name.as_bytes().to_vec(), 100 + i as u64, record.to_bytes().unwrap())
        })
        .collect();
    let acls = acls
        .iter()
        .enumerate()
        .map(|(i, (prefix, record))| {
            let key = crate::keyspace::acl_key(prefix);
            let suffix = key[crate::keyspace::ACL_PREFIX.len()..].to_vec();
            (suffix, 200 + i as u64, record.to_bytes().unwrap())
        })
        .collect();
    Permissions::build(7, groups, acls).expect("these records should decode")
}

fn acl(entries: &[(Principal, Right)]) -> AclRecord {
    AclRecord::new(entries.to_vec())
}

fn group_of(members: &[Principal]) -> GroupRecord {
    GroupRecord::new("root".to_string(), vec![user("root")], members.to_vec())
}

// -- records ---------------------------------------------------------

#[test]
fn a_group_record_round_trips() {
    let record = GroupRecord::new(
        "alice".to_string(),
        vec![user("alice"), group("leads")],
        vec![user("bob"), group("interns")],
    );
    let bytes = record.to_bytes().unwrap();
    assert_eq!(GroupRecord::from_bytes(&bytes).unwrap(), record);
}

#[test]
fn an_acl_record_round_trips() {
    let record = acl(&[(user("alice"), Right::Write), (group("ops"), Right::Read)]);
    let bytes = record.to_bytes().unwrap();
    assert_eq!(AclRecord::from_bytes(&bytes).unwrap(), record);
}

/// postcard is not self-describing, so this node cannot make sense of a
/// record from a newer node, and saying so is what the version byte being
/// first is for.
#[test]
fn a_record_from_a_future_version_is_refused() {
    let mut record = group_of(&[user("alice")]);
    record.version = RECORD_VERSION + 1;
    assert!(GroupRecord::from_bytes(&record.to_bytes().unwrap()).is_err());

    let mut record = acl(&[(user("alice"), Right::Read)]);
    record.version = RECORD_VERSION + 1;
    assert!(AclRecord::from_bytes(&record.to_bytes().unwrap()).is_err());
}

/// An ACL that names a principal whose name could never be stored is a
/// grant that could never match, so it is refused when written.
#[test]
fn a_principal_must_be_nameable() {
    assert!(user("alice").validate().is_ok());
    assert!(group("on-call").validate().is_ok());
    for bad in ["", "a/b", "a\0b", "ali\u{00e9}"] {
        assert!(user(bad).validate().is_err(), "user {bad:?}");
        assert!(group(bad).validate().is_err(), "group {bad:?}");
    }
}

// -- the default ------------------------------------------------------

/// With no ACLs, nobody is denied anything.
#[test]
fn a_cluster_with_no_acls_allows_everything() {
    let perms = snapshot(&[], &[]);
    assert!(!perms.has_acls());
    for key in [b"".as_slice(), b"app/config", b"\xff"] {
        assert_eq!(perms.decide("alice", key), Decision::Default);
        assert!(perms.decide("alice", key).permits(Right::Write));
    }
}

/// `Default` is permission to use the key, not authority over who else may.
/// Without this, any account could write an ACL on the empty prefix and
/// lock out the cluster.
#[test]
fn the_default_allows_writes_but_confers_no_authority_over_acls() {
    assert!(Decision::Default.permits(Right::Write));
    assert!(!Decision::Default.permits_acl_change());
    assert!(Decision::Granted(Right::Write).permits_acl_change());
    assert!(!Decision::Granted(Right::Read).permits_acl_change());
    assert!(!Decision::Denied.permits_acl_change());
}

/// An ACL on the empty prefix governs every key, which is how a cluster
/// closes everything by default.
#[test]
fn an_acl_on_the_empty_prefix_governs_everything() {
    let perms = snapshot(&[], &[(b"", acl(&[(user("alice"), Right::Read)]))]);
    assert_eq!(perms.decide("alice", b"anything"), Decision::Granted(Right::Read));
    assert_eq!(perms.decide("bob", b"anything"), Decision::Denied);
    assert_eq!(perms.decide("bob", b""), Decision::Denied);
}

// -- longest prefix ---------------------------------------------------

/// The rule, and the part most likely to be wrong: the deciding ACL is the
/// one on the longest prefix that exists, not the longest that mentions the
/// caller. `bob` is named at `app/` and is still denied `app/secret/x`,
/// because a shorter grant does not survive a longer ACL that leaves him
/// out.
#[test]
fn the_longest_acl_that_exists_decides_not_the_longest_that_grants() {
    let perms = snapshot(
        &[],
        &[
            (b"app/", acl(&[(user("bob"), Right::Write)])),
            (b"app/secret/", acl(&[(user("alice"), Right::Read)])),
        ],
    );

    assert_eq!(perms.decide("bob", b"app/config"), Decision::Granted(Right::Write));
    assert_eq!(perms.decide("bob", b"app/secret/x"), Decision::Denied);
    assert_eq!(perms.decide("alice", b"app/secret/x"), Decision::Granted(Right::Read));
    assert_eq!(perms.decide("alice", b"app/config"), Decision::Denied);
    // Governed by neither.
    assert_eq!(perms.decide("bob", b"other"), Decision::Default);
}

/// An ACL's prefix matches the key it is written for exactly, not only keys
/// strictly under it.
#[test]
fn an_acl_governs_the_key_equal_to_its_own_prefix() {
    let perms = snapshot(&[], &[(b"app", acl(&[(user("alice"), Right::Read)]))]);
    assert_eq!(perms.decide("bob", b"app"), Decision::Denied);
    // A byte prefix is a byte prefix: `apple` is under `app`, whether or not a
    // separator was intended.
    assert_eq!(perms.decide("bob", b"apple"), Decision::Denied);
    assert_eq!(perms.decide("bob", b"ap"), Decision::Default);
}

#[test]
fn governing_reports_which_prefix_decided() {
    let perms = snapshot(
        &[],
        &[
            (b"", acl(&[(user("root"), Right::Write)])),
            (b"app/", acl(&[(user("alice"), Right::Write)])),
        ],
    );
    assert_eq!(perms.governing(b"app/x").map(|(p, _)| p), Some(b"app/".as_slice()));
    assert_eq!(perms.governing(b"other").map(|(p, _)| p), Some(b"".as_slice()));
    assert!(perms.acl_at(b"app/").is_some());
    assert!(perms.acl_at(b"app").is_none());
}

// -- rights -----------------------------------------------------------

#[test]
fn write_satisfies_a_requirement_for_read() {
    assert!(Right::Write > Right::Read);
    assert!(Decision::Granted(Right::Write).permits(Right::Read));
    assert!(Decision::Granted(Right::Write).permits(Right::Write));
    assert!(Decision::Granted(Right::Read).permits(Right::Read));
    assert!(!Decision::Granted(Right::Read).permits(Right::Write));
}

/// A principal reachable twice gets the higher grant, so adding someone to
/// a group can only widen what they hold, never narrow it.
#[test]
fn several_matching_entries_give_the_highest_right() {
    let perms = snapshot(
        &[("ops", group_of(&[user("alice")]))],
        &[(
            b"app/",
            acl(&[(user("alice"), Right::Read), (group("ops"), Right::Write)]),
        )],
    );
    assert_eq!(perms.decide("alice", b"app/x"), Decision::Granted(Right::Write));
}

// -- groups -----------------------------------------------------------

#[test]
fn membership_of_a_named_group_grants_what_the_group_holds() {
    let perms = snapshot(
        &[("ops", group_of(&[user("alice")]))],
        &[(b"app/", acl(&[(group("ops"), Right::Write)]))],
    );
    assert_eq!(perms.decide("alice", b"app/x"), Decision::Granted(Right::Write));
    assert_eq!(perms.decide("bob", b"app/x"), Decision::Denied);
}

/// Why a group can contain a group: `alice` is in `interns`, `interns` is in
/// `staff`, `staff` is in `everyone`, and the ACL names only `everyone`.
#[test]
fn membership_is_transitive_through_nested_groups() {
    let perms = snapshot(
        &[
            ("interns", group_of(&[user("alice")])),
            ("staff", group_of(&[group("interns")])),
            ("everyone", group_of(&[group("staff")])),
        ],
        &[(b"app/", acl(&[(group("everyone"), Right::Read)]))],
    );

    let inside = perms.groups_of("alice");
    assert!(inside.contains("interns"));
    assert!(inside.contains("staff"));
    assert!(inside.contains("everyone"));
    assert_eq!(perms.decide("alice", b"app/x"), Decision::Granted(Right::Read));
    assert!(perms.groups_of("bob").is_empty());
}

/// Nesting deeper than the walk goes is not a hang and not a panic: it is
/// simply not membership, which is the safe way to run out.
#[test]
fn nesting_beyond_the_depth_bound_stops_rather_than_granting() {
    let deep = MAX_GROUP_DEPTH + 3;
    let mut groups: Vec<(String, GroupRecord)> = vec![("g0".to_string(), group_of(&[user("alice")]))];
    for i in 1..deep {
        groups.push((format!("g{i}"), group_of(&[group(&format!("g{}", i - 1))])));
    }
    let groups: Vec<(&str, GroupRecord)> =
        groups.iter().map(|(n, g)| (n.as_str(), g.clone())).collect();
    let perms = snapshot(&groups, &[]);

    let inside = perms.groups_of("alice");
    assert!(inside.contains("g0"));
    assert!(inside.contains(&format!("g{}", MAX_GROUP_DEPTH - 1)));
    assert!(!inside.contains(&format!("g{}", deep - 1)));
}

/// Cycles are refused when edges are added, so one here means a record this
/// node did not write. The walk must still end.
#[test]
fn a_cycle_in_the_group_graph_terminates() {
    let perms = snapshot(
        &[
            ("a", group_of(&[user("alice"), group("b")])),
            ("b", group_of(&[group("a")])),
        ],
        &[(b"k", acl(&[(group("b"), Right::Read)]))],
    );
    let inside = perms.groups_of("alice");
    assert!(inside.contains("a"));
    assert!(inside.contains("b"));
    assert_eq!(perms.decide("alice", b"k"), Decision::Granted(Right::Read));
}

/// An ACL may name a group that does not exist: a grant that matches
/// nobody, which is how a dangling reference should fail.
#[test]
fn an_acl_naming_a_group_that_does_not_exist_grants_nothing() {
    let perms = snapshot(&[], &[(b"app/", acl(&[(group("ghosts"), Right::Write)]))]);
    assert_eq!(perms.decide("alice", b"app/x"), Decision::Denied);
}

#[test]
fn is_any_answers_for_a_list_of_principals() {
    let perms = snapshot(&[("ops", group_of(&[user("alice")]))], &[]);
    assert!(perms.is_any("alice", &[group("ops")]));
    assert!(perms.is_any("bob", &[user("bob"), group("ops")]));
    assert!(!perms.is_any("bob", &[group("ops")]));
    assert!(!perms.is_any("alice", &[]));
}

// -- cycle detection --------------------------------------------------

#[test]
fn a_group_cannot_be_made_its_own_ancestor() {
    let perms = snapshot(
        &[
            ("a", group_of(&[group("b")])),
            ("b", group_of(&[group("c")])),
            ("c", group_of(&[user("alice")])),
        ],
        &[],
    );

    // `c` is already below `a`, so putting `a` into `c` closes the loop.
    assert!(perms.would_create_a_cycle("c", "a"));
    assert!(perms.would_create_a_cycle("a", "a"));
    assert!(perms.would_create_a_cycle("b", "a"));
    // Downwards is fine: nothing reaches `d`.
    assert!(!perms.would_create_a_cycle("a", "d"));
    assert!(!perms.would_create_a_cycle("d", "a"));
}

// -- bookkeeping ------------------------------------------------------

#[test]
fn the_snapshot_reports_what_it_holds() {
    let perms = snapshot(
        &[
            ("ops", GroupRecord::new("alice".into(), vec![user("alice")], vec![])),
            ("leads", GroupRecord::new("alice".into(), vec![user("alice")], vec![])),
            ("all", GroupRecord::new("root".into(), vec![user("root")], vec![])),
        ],
        &[(b"app/", acl(&[(user("alice"), Right::Read)]))],
    );
    assert_eq!(perms.epoch(), 7);
    assert_eq!(perms.group_count(), 3);
    assert_eq!(perms.acl_count(), 1);
    assert_eq!(perms.groups_created_by("alice"), 2);
    assert_eq!(perms.groups_created_by("root"), 1);
    assert_eq!(perms.groups_created_by("bob"), 0);
    assert!(perms.group("ops").is_some());
    assert!(perms.group("nope").is_none());
    assert_eq!(perms.groups().count(), 3);
    assert_eq!(perms.acls().count(), 1);
    // Seqs come from where each record was stored, not from inside it.
    assert_ne!(perms.group_seq("ops"), 0);
    assert_ne!(perms.acl_seq(b"app/"), 0);
    assert_eq!(perms.group_seq("nope"), 0);
    assert_eq!(perms.acl_seq(b"nope/"), 0);
}

/// What `DeleteAcl` must ask before it allows a removal: with this ACL
/// gone, what would govern the prefix?
#[test]
fn governing_within_answers_what_removal_would_fall_back_to() {
    let perms = snapshot(
        &[],
        &[
            (b"app/", acl(&[(user("root"), Right::Write)])),
            (b"app/sub/", acl(&[(user("alice"), Right::Write)])),
        ],
    );

    let prefix = b"app/sub/";
    assert_eq!(perms.governing(prefix).map(|(p, _)| p), Some(prefix.as_slice()));
    // Strictly shorter: removing `app/sub/` leaves `app/` in charge.
    assert_eq!(
        perms.governing_within(prefix, prefix.len() - 1).map(|(p, _)| p),
        Some(b"app/".as_slice())
    );
    // Removing `app/` leaves nothing, which is the default, and so an
    // administrator's act.
    assert_eq!(perms.governing_within(b"app/", 3), None);
}

#[test]
fn an_empty_snapshot_has_never_seen_a_permission_change() {
    let perms = Permissions::empty();
    assert_eq!(perms.epoch(), 0);
    assert!(!perms.has_acls());
    assert_eq!(perms.decide("alice", b"anything"), Decision::Default);
}

/// A record that does not decode fails the build instead of silently
/// dropping out: a dropped ACL is a subtree that silently opens.
#[test]
fn a_record_that_will_not_decode_fails_the_build() {
    let junk = vec![0xff, 0xff];
    assert!(Permissions::build(1, vec![(b"ops".to_vec(), 1, junk.clone())], vec![]).is_err());
    assert!(Permissions::build(1, vec![], vec![(b"6161".to_vec(), 1, junk)]).is_err());
    // A key this module could not have written is corrupt, not empty.
    let acl = acl(&[(user("alice"), Right::Read)]).to_bytes().unwrap();
    assert!(Permissions::build(1, vec![], vec![(b"zz".to_vec(), 1, acl)]).is_err());
}

#[test]
fn roles_are_membership_of_the_built_in_groups() {
    let record = |members: Vec<Principal>| {
        GroupRecord::new("root".into(), vec![group(ADMINS_GROUP)], members)
            .to_bytes()
            .unwrap()
    };
    let groups = vec![
        (ADMINS_GROUP.as_bytes().to_vec(), 1, record(vec![user("root")])),
        (
            BACKUP_OPERATORS_GROUP.as_bytes().to_vec(),
            1,
            record(vec![user("bob"), group("nightly")]),
        ),
        (b"nightly".to_vec(), 2, record(vec![user("carol")])),
    ];
    let perms = Permissions::build(3, groups, vec![]).unwrap();
    assert!(perms.is_admin("root"));
    assert!(!perms.is_admin("bob"));
    // An administrator may back up without being named a backup operator.
    for who in ["root", "bob", "carol"] {
        assert!(perms.may_back_up(who), "{who} should be able to back up");
    }
    assert!(!perms.may_back_up("alice"));
    assert!(!Permissions::empty().is_admin("root"));
}
