//! Generated protobuf and gRPC types from `proto/*.proto` (see `build.rs`).
//!
//! `client` and `cluster` are generated in the `raft5-shared` crate (which
//! the client library also uses) and re-exported here, so this crate can
//! refer to them as `pb::raft5::client` and `pb::raft5::cluster`, next to
//! the locally generated `common` and `raft` types.

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
// `storage::EntryPayload` and the `kind`/`writes`/`config` fields of
// `raft5::common::LogEntry` describe the same thing in the shapes their
// formats allow: an enum in Rust, where illegal combinations cannot exist,
// and a tag next to optional fields on the wire, where proto3 has no sum
// type. The conversion is where the difference is checked, so it is
// checked in one place.
//
// A follower re-appends replicated entries exactly, so anything lost here
// is lost from its log: not only the kind, but every mutation a write
// carries.

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
/// one rejects the whole `AppendEntries` batch, instead of appending
/// something it had to guess. A wrong guess would still match on (seq,
/// term), so log matching would never detect the divergence, and it would
/// become the cluster's truth at the next leader change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnknownEntry;

impl std::fmt::Display for UnknownEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("log entry has an unrecognized kind")
    }
}

impl raft5::common::LogEntry {
    /// This entry's payload, or `UnknownEntry` if this build does not know its
    /// kind.
    ///
    /// An unknown kind is not treated as a write, on purpose: the guess would
    /// produce a write with no mutations, an entry that uses a seq, passes log
    /// matching, and silently drops every key a transaction carried.
    pub fn payload(&self) -> Result<EntryPayload, UnknownEntry> {
        self.clone().into_payload()
    }

    /// `payload`, consuming the entry.
    ///
    /// Worth having next to the borrowing version: it runs once per entry in
    /// the follower's append loop, and `payload` must clone every `KeyValue`
    /// to build its result. For an entry with a whole transaction, that clone
    /// is the entry again, on the hot path, for nothing: the caller is done
    /// with the original anyway.
    pub fn into_payload(self) -> Result<EntryPayload, UnknownEntry> {
        match raft5::common::EntryKind::try_from(self.kind) {
            Ok(raft5::common::EntryKind::Write) => Ok(EntryPayload::Write(
                self.writes.into_iter().map(Into::into).collect(),
            )),
            Ok(raft5::common::EntryKind::Noop) => Ok(EntryPayload::NoOp),
            Ok(raft5::common::EntryKind::Config) => Ok(EntryPayload::Config(
                self.config.unwrap_or_default(),
            )),
            Ok(raft5::common::EntryKind::Unspecified) | Err(_) => Err(UnknownEntry),
        }
    }

    /// Whether this entry carries a cluster configuration, without decoding
    /// the rest of it. An unknown kind is not a configuration: the caller will
    /// reject the batch for it anyway.
    pub fn is_config(&self) -> bool {
        raft5::common::EntryKind::try_from(self.kind) == Ok(raft5::common::EntryKind::Config)
    }
}
