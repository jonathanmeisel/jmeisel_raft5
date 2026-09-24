//! Walking the records the cluster keeps for itself.
//!
//! Several things now live as ordinary key/value pairs under a fixed
//! prefix in the reserved range -- checkpoints under `\0snap/`, accounts
//! under `\0auth/user/`, invitations under `\0auth/invite/` -- for the
//! reasons `crate::keyspace` gives. Each of them needs the same
//! question answered at some point: which of these exist right now?
//!
//! That question is not a plain range scan, because `versions` holds a
//! chain of rows per key rather than one row. This module is the one
//! place that knows how to collapse those chains back into "the records
//! that currently exist", so a new reserved range gets listings without
//! bringing its own copy of the walk.

use super::codec::{Kind, chain_start, row_kind, row_seq, row_user_key};
use super::{RaftLog, Result, StorageError};

/// One record from a versioned listing: the part of its key after the
/// prefix, the seq it was last written at, and its value.
pub type VersionedRecord = (Vec<u8>, u64, Vec<u8>);

impl RaftLog {
    /// Walk the head row of every record under `prefix` that currently
    /// exists, in key order, skipping ones whose head is a tombstone --
    /// those are removed records whose rows the GC has not collected
    /// yet.
    ///
    /// `f` is handed the part of the key after `prefix`, the seq the
    /// record was written at, and the row itself -- which is what a
    /// caller needs in order to go and read the value, and what lets a
    /// caller that does not care about the value avoid reading one.
    ///
    /// One iterator over the prefix rather than a seek per record, for
    /// the reason `scan_as_of` gives: a single pinned nonce, so the walk
    /// sees one point in time.
    pub(super) fn for_each_live_record(
        &self,
        prefix: &[u8],
        mut f: impl FnMut(Vec<u8>, u64, &[u8]) -> Result<()>,
    ) -> Result<()> {
        let mut settled: Option<Vec<u8>> = None;
        for item in self.versions.range(chain_start(prefix)..) {
            let row = item.key()?;
            let key = row_user_key(&row)?;
            if !key.starts_with(prefix) {
                break;
            }
            let chain = super::codec::row_chain(&row)?;
            if settled.as_deref() == Some(chain) {
                continue;
            }
            settled = Some(chain.to_vec());
            if row_kind(&row)? != Kind::Value {
                continue;
            }
            let id = key
                .get(prefix.len()..)
                .ok_or(StorageError::Corrupt(
                    "a reserved record's key is shorter than its prefix",
                ))?
                .to_vec();
            f(id, row_seq(&row)?, &row)?;
        }
        Ok(())
    }

    /// Every live record under `prefix`, as (key suffix, seq, value)
    /// triples in key order.
    ///
    /// [`list_records_under`] without the seq, for the listings that do
    /// not care; everything said there about bounds and about a record
    /// vanishing mid-listing applies here.
    ///
    /// The seq is the one the record was last written at, which is what
    /// lets a caller tell a record it has already seen from one that has
    /// changed since -- and, for the permission snapshot, what it
    /// reports so a client can do the same.
    ///
    /// [`list_records_under`]: RaftLog::list_records_under
    pub fn list_versioned_records_under(&self, prefix: &[u8]) -> Result<Vec<VersionedRecord>> {
        let mut rows = Vec::new();
        self.for_each_live_record(prefix, |id, seq, row| {
            rows.push((id, seq, row.to_vec()));
            Ok(())
        })?;

        let mut out = Vec::with_capacity(rows.len());
        for (id, seq, row) in rows {
            if let Some(value) = self.versions.get(&row)? {
                out.push((id, seq, value.to_vec()));
            }
        }
        Ok(out)
    }

    /// Every live record under `prefix`, as (key suffix, value) pairs in
    /// key order.
    ///
    /// For the listings that need what is *in* each record. A record
    /// whose row is gone by the time it is read is left out rather than
    /// failing the listing: between the walk naming it and this reading
    /// it, the GC can have collected one a concurrent removal
    /// tombstoned, which is a record on its way out either way.
    ///
    /// Deliberately unbounded, because the ranges it is used on are: a
    /// caller that expects a range to grow without limit wants paging
    /// and should not be using this.
    pub fn list_records_under(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut rows = Vec::new();
        self.for_each_live_record(prefix, |id, _, row| {
            rows.push((id, row.to_vec()));
            Ok(())
        })?;

        let mut out = Vec::with_capacity(rows.len());
        for (id, row) in rows {
            if let Some(value) = self.versions.get(&row)? {
                out.push((id, value.to_vec()));
            }
        }
        Ok(out)
    }
}
