//! Generated protobuf/gRPC types from `proto/*.proto` (see `build.rs`).
//!
//! `client` and `cluster` are generated in the `raft5-shared` crate (also
//! used by the client library) and re-exported here so the rest of this
//! crate can keep referring to them as `pb::raft5::client` /
//! `pb::raft5::cluster`, alongside the `common`/`raft` types generated
//! locally.

pub mod raft5 {
    pub mod common {
        include!(concat!(env!("OUT_DIR"), "/raft5.common.rs"));
    }
    pub mod raft {
        include!(concat!(env!("OUT_DIR"), "/raft5.raft.rs"));
    }
    pub use raft5_shared::raft5::client;
    pub use raft5_shared::raft5::cluster;
}

// --- storage <-> wire conversions -----------------------------------------
//
// `storage::EntryPayload` and the `kind`/`writes`/`config` triple on
// `raft5::common::LogEntry` describe the same thing in the two shapes
// their formats allow: an enum on the Rust side, where illegal
// combinations can simply not exist, and a tag beside optional fields on
// the wire, where proto3 has no sum type. Crossing the boundary is where
// that difference gets checked, so it gets checked in exactly one place.
//
// A follower re-appends replicated entries verbatim, so anything lost
// here is lost from its copy of the log: not just the kind, but every
// mutation a write carries.

use crate::storage::{EntryPayload, Mutation};

impl From<Mutation> for raft5::common::KeyValue {
    fn from(m: Mutation) -> Self {
        Self {
            key: m.key,
            value: m.value,
        }
    }
}

impl From<raft5::common::KeyValue> for Mutation {
    fn from(kv: raft5::common::KeyValue) -> Self {
        Self {
            key: kv.key,
            value: kv.value,
        }
    }
}

impl From<crate::storage::LogEntry> for raft5::common::LogEntry {
    fn from(e: crate::storage::LogEntry) -> Self {
        let (kind, writes, config) = match e.payload {
            EntryPayload::Write(writes) => (
                raft5::common::EntryKind::Write,
                writes.into_iter().map(Into::into).collect(),
                None,
            ),
            EntryPayload::NoOp => (raft5::common::EntryKind::Noop, Vec::new(), None),
            EntryPayload::Config(config) => (
                raft5::common::EntryKind::Config,
                Vec::new(),
                Some(config),
            ),
        };
        Self {
            seq: e.seq,
            term: e.term,
            kind: kind as i32,
            writes,
            config,
        }
    }
}

/// Why a replicated entry could not be understood. A follower that gets
/// one of these rejects the whole `AppendEntries` batch rather than
/// appending something it has had to guess at: an entry it guessed wrong
/// about would still match on (seq, term), so the divergence would never
/// be detected by log matching and would become the cluster's truth at
/// the next leader change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnknownEntry;

impl std::fmt::Display for UnknownEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("log entry has an unrecognized kind")
    }
}

impl raft5::common::LogEntry {
    /// This entry's payload, or `UnknownEntry` if its kind is one this
    /// build does not know.
    ///
    /// Deliberately *not* defaulting an unrecognized kind to a write. It
    /// used to, back when the field's proto3 default (`PUT`) and a
    /// one-key entry made that a harmless guess. It is not harmless now:
    /// the guess would produce a write with no mutations -- an entry that
    /// consumes a seq, satisfies log matching, and silently drops every
    /// key a transaction was carrying.
    pub fn payload(&self) -> Result<EntryPayload, UnknownEntry> {
        match raft5::common::EntryKind::try_from(self.kind) {
            Ok(raft5::common::EntryKind::Write) => Ok(EntryPayload::Write(
                self.writes.iter().cloned().map(Into::into).collect(),
            )),
            Ok(raft5::common::EntryKind::Noop) => Ok(EntryPayload::NoOp),
            Ok(raft5::common::EntryKind::Config) => Ok(EntryPayload::Config(
                self.config.clone().unwrap_or_default(),
            )),
            Ok(raft5::common::EntryKind::Unspecified) | Err(_) => Err(UnknownEntry),
        }
    }

    /// Whether this entry carries a cluster configuration, without
    /// decoding the rest of it. An unrecognized kind is not a config --
    /// the caller is about to reject the batch over it anyway.
    pub fn is_config(&self) -> bool {
        raft5::common::EntryKind::try_from(self.kind) == Ok(raft5::common::EntryKind::Config)
    }
}
