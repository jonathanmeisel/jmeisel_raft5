use super::*;

fn member(id: &str, voting: bool) -> ClusterMember {
    ClusterMember {
        server_id: id.to_string(),
        address: format!("http://{id}:1"),
        voting,
    }
}

fn config(members: &[(&str, bool)]) -> ClusterConfig {
    ClusterConfig {
        members: members.iter().map(|(id, v)| member(id, *v)).collect(),
        heartbeat_millis: 50,
    }
}

#[test]
fn round_trips_through_bytes() {
    let c = config(&[("a", true), ("b", false)]);
    assert_eq!(ClusterConfig::from_bytes(&c.to_bytes()).unwrap(), c);
}

#[test]
fn majority_counts_voters_only() {
    // Three voters need two, and adding learners must not change
    // that -- otherwise adding a server would immediately raise the
    // bar for committing the very entry that added it.
    assert_eq!(
        config(&[("a", true), ("b", true), ("c", true)]).majority(),
        2
    );
    assert_eq!(
        config(&[("a", true), ("b", true), ("c", true), ("d", false)]).majority(),
        2
    );
    assert_eq!(config(&[("a", true)]).majority(), 1);
    // Degenerate, but must not underflow a `majority() - 1` index.
    assert_eq!(config(&[("a", false)]).majority(), 1);
    assert_eq!(config(&[]).majority(), 1);
}

#[test]
fn membership_predicates_distinguish_learners_from_strangers() {
    let c = config(&[("a", true), ("b", false)]);
    assert!(c.is_voter("a"));
    assert!(!c.is_voter("b")); // a learner: a member, but no vote
    assert!(!c.is_voter("zz")); // not a member at all
    assert!(c.contains("b"));
    assert!(!c.contains("zz"));
    assert_eq!(
        c.others("a")
            .map(|m| m.server_id.as_str())
            .collect::<Vec<_>>(),
        vec!["b"],
        "learners are still replicated to"
    );
}

#[test]
fn edits_produce_a_new_configuration_without_mutating_the_old() {
    let c = config(&[("a", true)]);

    let added = c.with_member(member("b", false));
    assert!(added.contains("b") && !added.is_voter("b"));
    assert!(!c.contains("b"), "the original is untouched");

    let promoted = added.with_voter("b");
    assert!(promoted.is_voter("b"));
    assert_eq!(promoted.majority(), 2, "promotion raises the quorum bar");

    // Re-adding an existing id updates it in place rather than
    // duplicating -- two entries for one server would corrupt every
    // count derived from `members`.
    let readded = promoted.with_member(member("b", false));
    assert_eq!(readded.members.len(), 2);
    assert!(!readded.is_voter("b"));

    assert!(!promoted.without_member("b").contains("b"));
    assert_eq!(
        promoted.without_member("zz"),
        promoted,
        "removing a non-member changes nothing"
    );
}
