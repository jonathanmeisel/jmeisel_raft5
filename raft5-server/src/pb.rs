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
// `storage::LogEntry` and `raft5::common::LogEntry` are structurally
// identical (the proto deliberately mirrors the storage type) but are
// distinct Rust types, so crossing the boundary needs an explicit copy.
// `kind` is the part that matters: a follower re-appends replicated
// entries verbatim, so losing it would flatten every non-state entry into
// a write.

impl From<crate::storage::EntryKind> for raft5::common::EntryKind {
    fn from(kind: crate::storage::EntryKind) -> Self {
        match kind {
            crate::storage::EntryKind::Put => Self::Put,
            crate::storage::EntryKind::Delete => Self::Delete,
            crate::storage::EntryKind::NoOp => Self::Noop,
            crate::storage::EntryKind::Config => Self::Config,
        }
    }
}

impl From<raft5::common::EntryKind> for crate::storage::EntryKind {
    fn from(kind: raft5::common::EntryKind) -> Self {
        match kind {
            raft5::common::EntryKind::Put => Self::Put,
            raft5::common::EntryKind::Delete => Self::Delete,
            raft5::common::EntryKind::Noop => Self::NoOp,
            raft5::common::EntryKind::Config => Self::Config,
        }
    }
}

impl From<crate::storage::LogEntry> for raft5::common::LogEntry {
    fn from(e: crate::storage::LogEntry) -> Self {
        Self {
            seq: e.seq,
            term: e.term,
            key: e.key,
            value: e.value,
            kind: raft5::common::EntryKind::from(e.kind) as i32,
        }
    }
}

impl raft5::common::LogEntry {
    /// This entry's kind, treating an unrecognized wire value as a `Put`
    /// for backward compatibility with the field's proto3 default.
    pub fn entry_kind(&self) -> crate::storage::EntryKind {
        raft5::common::EntryKind::try_from(self.kind)
            .unwrap_or(raft5::common::EntryKind::Put)
            .into()
    }
}
