//! Cluster configuration: who is in the cluster, and which members vote.
//!
//! The configuration is a `ClusterConfig` message (see
//! `proto/cluster.proto`) stored in the Raft log as an
//! `EntryPayload::Config` entry, replicated and log-matched like a
//! key/value write. The two things that make it special live elsewhere:
//!
//! * It takes effect on **append**, not on commit (Raft §4.1):
//!   `storage::RaftLog::current_config` reads it straight from `log`. See
//!   that function for why waiting for commit would deadlock.
//! * It survives compaction and snapshots through `meta[base_config]` and
//!   the snapshot header, so a node can always name its peers, even long
//!   after the entry that added them is gone.
//!
//! This module is the pure, in-memory part: decoding, the quorum
//! arithmetic every caller needs, and the edits (which return a new value)
//! that build the next configuration to propose.

use prost::Message;

use crate::raft5::cluster::{ClusterConfig, ClusterMember};

impl ClusterConfig {
    /// Decode a configuration as stored in a log entry.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, prost::DecodeError> {
        Self::decode(bytes)
    }

    /// Encode for storage in a log entry.
    pub fn to_bytes(&self) -> Vec<u8> {
        self.encode_to_vec()
    }

    pub fn member(&self, server_id: &str) -> Option<&ClusterMember> {
        self.members.iter().find(|m| m.server_id == server_id)
    }

    pub fn contains(&self, server_id: &str) -> bool {
        self.member(server_id).is_some()
    }

    /// Whether `server_id` votes in elections and counts toward the commit
    /// quorum. False for a non-member, and false for a learner (§4.2.1).
    pub fn is_voter(&self, server_id: &str) -> bool {
        self.member(server_id).is_some_and(|m| m.voting)
    }

    pub fn voters(&self) -> impl Iterator<Item = &ClusterMember> {
        self.members.iter().filter(|m| m.voting)
    }

    /// Every member except `self_id`: everyone this node replicates to and
    /// probes, learners included.
    pub fn others(&self, self_id: &str) -> impl Iterator<Item = &ClusterMember> {
        self.members.iter().filter(move |m| m.server_id != self_id)
    }

    /// How many votes (or matching `match_index` values) make a quorum.
    /// Counts voting members only: learners get replication but count in no
    /// quorum, which is why a server is added as one first.
    ///
    /// Zero voters is degenerate (nothing can be decided). It returns 1, not
    /// 0, so callers that index a sorted quorum vector at `majority() - 1`
    /// cannot underflow.
    pub fn majority(&self) -> usize {
        (self.voters().count() / 2 + 1).max(1)
    }

    /// This configuration plus `member`, replacing any entry with the same
    /// `server_id`. Returns a new configuration instead of changing this one:
    /// a change is proposed by appending the result as a new log entry, never
    /// by editing one in place.
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

    /// This configuration without `server_id`. Returns an equal configuration
    /// if it was not a member.
    pub fn without_member(&self, server_id: &str) -> Self {
        let mut next = self.clone();
        next.members.retain(|m| m.server_id != server_id);
        next
    }

    /// This configuration with `server_id` promoted from learner to voter.
    /// Returns an equal configuration if it is not a member, or already votes.
    pub fn with_voter(&self, server_id: &str) -> Self {
        let mut next = self.clone();
        if let Some(m) = next.members.iter_mut().find(|m| m.server_id == server_id) {
            m.voting = true;
        }
        next
    }
}

#[cfg(test)]
mod tests;
