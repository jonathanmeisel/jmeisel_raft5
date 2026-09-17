//! Cluster configuration: who is in the cluster, and which of them vote.
//!
//! The configuration is just a `ClusterConfig` message (see
//! `proto/cluster.proto`) stored in the Raft log as an
//! `EntryKind::Config` entry, replicated and log-matched exactly like a
//! key/value write. The two things that make it special both live
//! elsewhere:
//!
//! * It takes effect on **append**, not on commit (Raft §4.1) --
//!   `storage::RaftLog::current_config` reads it straight out of `log`.
//!   See that function for why waiting for commitment would deadlock.
//! * It survives compaction and snapshotting via `meta[base_config]` and
//!   the snapshot header, so a node can always name its peers even when
//!   the entry that introduced them is long gone.
//!
//! This module is the pure, in-memory half: decoding, the quorum
//! arithmetic every caller needs, and the (non-mutating) edits that
//! produce the *next* configuration to propose.

use prost::Message;

use crate::pb::raft5::cluster::{ClusterConfig, ClusterMember};

impl ClusterConfig {
    /// Decode a configuration as stored in a log entry's `value`.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, prost::DecodeError> {
        Self::decode(bytes)
    }

    /// Encode for storage in a log entry's `value`.
    pub fn to_bytes(&self) -> Vec<u8> {
        self.encode_to_vec()
    }

    pub fn member(&self, server_id: &str) -> Option<&ClusterMember> {
        self.members.iter().find(|m| m.server_id == server_id)
    }

    pub fn contains(&self, server_id: &str) -> bool {
        self.member(server_id).is_some()
    }

    /// Whether `server_id` votes in elections and counts toward the
    /// commit quorum. False for a server that isn't a member at all, and
    /// false for a learner (§4.2.1).
    pub fn is_voter(&self, server_id: &str) -> bool {
        self.member(server_id).is_some_and(|m| m.voting)
    }

    pub fn voters(&self) -> impl Iterator<Item = &ClusterMember> {
        self.members.iter().filter(|m| m.voting)
    }

    /// Every member except `self_id` -- i.e. everyone this node
    /// replicates to and probes, learners included.
    pub fn others(&self, self_id: &str) -> impl Iterator<Item = &ClusterMember> {
        self.members.iter().filter(move |m| m.server_id != self_id)
    }

    /// How many votes (or matching `match_index`es) make a quorum.
    /// Counts *voting* members only: learners are replicated to but are
    /// invisible to both quorums, which is the whole point of adding a
    /// server as one first.
    ///
    /// Zero voters is degenerate (nothing can ever be decided); it
    /// returns 1 rather than 0 so callers indexing a sorted quorum vector
    /// at `majority() - 1` can't underflow.
    pub fn majority(&self) -> usize {
        (self.voters().count() / 2 + 1).max(1)
    }

    /// This configuration plus `member`, replacing any existing entry for
    /// the same `server_id`. Returns the new configuration rather than
    /// mutating: a configuration change is proposed by appending the
    /// *result* as a new log entry, never by editing one in place.
    pub fn with_member(&self, member: ClusterMember) -> Self {
        let mut next = self.clone();
        match next
            .members
            .iter_mut()
            .find(|m| m.server_id == member.server_id)
        {
            Some(existing) => *existing = member,
            None => next.members.push(member),
        }
        next
    }

    /// This configuration without `server_id`. A no-op (returning an
    /// equal configuration) if it wasn't a member.
    pub fn without_member(&self, server_id: &str) -> Self {
        let mut next = self.clone();
        next.members.retain(|m| m.server_id != server_id);
        next
    }

    /// This configuration with `server_id` promoted from learner to
    /// voter. A no-op if it isn't a member, or is already a voter.
    pub fn with_voter(&self, server_id: &str) -> Self {
        let mut next = self.clone();
        if let Some(m) = next.members.iter_mut().find(|m| m.server_id == server_id) {
            m.voting = true;
        }
        next
    }
}

#[cfg(test)]
mod tests {
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
}
