use super::checkpoint::{checkpoint_key, encode_record};
use super::snapshot::SNAPSHOT_MAGIC;
use super::*;

/// A budget no test here can reach, for the tests that are about
/// something other than the budget.
const NO_BUDGET: usize = usize::MAX;

fn open_temp() -> (RaftLog, std::path::PathBuf) {
    let mut path = std::env::temp_dir();
    path.push(format!("raftlog_test_{}", std::process::id()));
    path.push(format!("{:x}", rand_suffix()));
    let log = RaftLog::open(&path).expect("open");
    (log, path)
}

/// A unique directory suffix.
///
/// A nanosecond timestamp alone isn't reliable under parallel test
/// execution -- two tests starting at nearly the same instant can land
/// on the same value (clock resolution, scheduling) and then collide on
/// the same directory, which surfaces as `Db(Locked)` from whichever
/// one opens second. A process-wide counter on top guarantees
/// uniqueness regardless. Same reasoning, and the same fix, as
/// `raft::test_support::open_temp_log`.
fn rand_suffix() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    nanos.wrapping_mul(1024) ^ COUNTER.fetch_add(1, Ordering::Relaxed)
}

fn cleanup(path: &std::path::Path) {
    let _ = std::fs::remove_dir_all(path);
}

#[test]
fn append_assigns_gap_free_increasing_seqs() {
    let (log, path) = open_temp();
    let s1 = log.append(1, b"a", Some(b"1")).unwrap();
    let s2 = log.append(1, b"b", Some(b"2")).unwrap();
    let s3 = log.append(2, b"a", Some(b"3")).unwrap();
    assert_eq!((s1, s2, s3), (1, 2, 3));
    cleanup(&path);
}

#[test]
fn append_does_not_touch_latest() {
    // `append` only ever writes `log` -- `latest` must stay exactly as
    // it was (nothing, for a brand new key) until `apply_committed`
    // says otherwise. This is the whole point of the split: an
    // appended-but-uncommitted write must never be visible via
    // `get_latest`.
    let (log, path) = open_temp();
    log.append(1, b"k", Some(b"v1")).unwrap();
    assert_eq!(log.get_latest(b"k").unwrap(), None);
    cleanup(&path);
}

#[test]
fn apply_committed_advances_latest_to_the_newest_applied_write() {
    let (log, path) = open_temp();
    log.append(1, b"k", Some(b"v1")).unwrap();
    let seq2 = log.append(1, b"k", Some(b"v2")).unwrap();

    log.apply_committed(seq2).unwrap();

    let (seq, val) = log.get_latest(b"k").unwrap().unwrap();
    assert_eq!(seq, seq2);
    assert_eq!(val, b"v2".to_vec());
    cleanup(&path);
}

#[test]
fn apply_committed_only_applies_up_to_the_requested_seq() {
    // A write past `up_to_seq` must not be visible yet -- it hasn't
    // been confirmed committed.
    let (log, path) = open_temp();
    let seq1 = log.append(1, b"k", Some(b"v1")).unwrap();
    log.append(1, b"k", Some(b"v2")).unwrap();

    log.apply_committed(seq1).unwrap();

    let (seq, val) = log.get_latest(b"k").unwrap().unwrap();
    assert_eq!(seq, seq1);
    assert_eq!(val, b"v1".to_vec());
    cleanup(&path);
}

#[test]
fn apply_committed_removes_a_deleted_key_from_latest_entirely() {
    let (log, path) = open_temp();
    log.append(1, b"k", Some(b"v1")).unwrap();
    let del_seq = log.append(1, b"k", None).unwrap();

    log.apply_committed(del_seq).unwrap();

    // A committed delete is permanent -- there's nothing left to
    // distinguish it from "never written" for, unlike `log` itself
    // (which still records the tombstone as a normal entry).
    assert_eq!(log.get_latest(b"k").unwrap(), None);
    let entries = log.entries_in_range(del_seq, del_seq + 1).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].payload.writes(),
        [Mutation::new(*b"k", None::<Vec<u8>>)]
    );

    assert_eq!(log.get_latest(b"never").unwrap(), None);
    cleanup(&path);
}

#[test]
fn apply_committed_resumes_from_the_previous_watermark_across_calls() {
    let (log, path) = open_temp();
    let seq1 = log.append(1, b"k", Some(b"v1")).unwrap();
    log.apply_committed(seq1).unwrap();

    let seq2 = log.append(1, b"k", Some(b"v2")).unwrap();
    log.apply_committed(seq2).unwrap();

    let (seq, val) = log.get_latest(b"k").unwrap().unwrap();
    assert_eq!(seq, seq2);
    assert_eq!(val, b"v2".to_vec());

    // A second call at or below the previous watermark is a no-op.
    log.apply_committed(seq1).unwrap();
    assert_eq!(log.get_latest(b"k").unwrap().unwrap().0, seq2);

    cleanup(&path);
}

#[test]
fn append_noop_takes_a_seq_and_round_trips_its_kind() {
    let (log, path) = open_temp();
    let s1 = log.append(1, b"a", Some(b"1")).unwrap();
    let noop = log.append_noop(2).unwrap();
    let s3 = log.append(2, b"b", Some(b"2")).unwrap();

    // A no-op is a real log entry: it consumes a sequence number, so
    // the writes around it aren't contiguous.
    assert_eq!((s1, noop, s3), (1, 2, 3));

    let stored = log.entries_in_range(1, 4).unwrap();
    assert_eq!(stored[1].payload, EntryPayload::NoOp);
    assert_eq!(stored[1].term, 2);
    // The neighbours still decode as ordinary writes.
    assert_eq!(
        stored[0].payload.writes(),
        [Mutation::new(*b"a", Some(*b"1"))]
    );
    assert_eq!(
        stored[2].payload.writes(),
        [Mutation::new(*b"b", Some(*b"2"))]
    );
    // And the term is still readable at the no-op's index, which is
    // what the Figure 8 commit check consults.
    assert_eq!(log.term_at(noop).unwrap(), Some(2));

    cleanup(&path);
}

#[test]
fn a_committed_noop_contributes_nothing_to_latest() {
    let (log, path) = open_temp();
    log.append(1, b"a", Some(b"1")).unwrap();
    let noop = log.append_noop(2).unwrap();
    log.apply_committed(noop).unwrap();

    // The no-op moved the applied watermark without writing a row --
    // in particular it did not insert an empty key.
    assert_eq!(log.get_latest(b"a").unwrap(), Some((1, b"1".to_vec())));
    assert_eq!(log.get_latest(b"").unwrap(), None);

    let mut buf = Vec::new();
    let meta = log.export_snapshot(&mut buf).unwrap();
    assert_eq!(meta.entry_count, 1); // just "a"
    assert_eq!(meta.last_included_seq, noop);
    assert_eq!(meta.last_included_term, 2);

    cleanup(&path);
}

#[test]
fn current_config_reads_the_newest_config_entry_without_waiting_for_commitment() {
    let (log, path) = open_temp();
    assert_eq!(log.current_config().unwrap(), None); // never bootstrapped

    let c1 = log.append_config(1, b"cfg-1").unwrap();
    log.append(1, b"a", Some(b"1")).unwrap();
    // Nothing has been applied, so `latest` is still empty -- the
    // configuration has to be visible anyway (it takes effect on
    // append), which is exactly why it is read from `log`.
    assert_eq!(log.get_latest(b"a").unwrap(), None);
    assert_eq!(log.current_config().unwrap(), Some(b"cfg-1".to_vec()));

    let c2 = log.append_config(1, b"cfg-2").unwrap();
    assert_eq!(log.current_config().unwrap(), Some(b"cfg-2".to_vec()));

    // `config_at` sees the history the tail has moved past.
    assert_eq!(log.config_at(c2 - 1).unwrap(), Some(b"cfg-1".to_vec()));
    assert_eq!(log.config_at(c1).unwrap(), Some(b"cfg-1".to_vec()));
    assert_eq!(log.config_at(c1 - 1).unwrap(), None);

    // A rolled-back config entry reverts the active configuration,
    // which is the flip side of activating on append.
    log.truncate_from(c2).unwrap();
    assert_eq!(log.current_config().unwrap(), Some(b"cfg-1".to_vec()));

    cleanup(&path);
}

#[test]
fn a_config_entry_survives_being_compacted_away() {
    let (log, path) = open_temp();
    log.append_config(1, b"cfg-1").unwrap();
    log.append(1, b"a", Some(b"1")).unwrap();
    let tail = log.append(1, b"b", Some(b"2")).unwrap();
    log.apply_committed(tail).unwrap();

    // Compact past the config entry entirely: it no longer exists in
    // `log`, but a node still has to know who its peers are.
    log.compact(tail + 1).unwrap();
    assert!(log.entries_in_range(1, tail + 1).unwrap().is_empty());
    assert_eq!(log.current_config().unwrap(), Some(b"cfg-1".to_vec()));

    // A newer config entry still wins over the compacted fallback.
    log.append_config(2, b"cfg-2").unwrap();
    assert_eq!(log.current_config().unwrap(), Some(b"cfg-2".to_vec()));

    cleanup(&path);
}

#[test]
fn a_config_entry_is_not_state_machine_data() {
    let (log, path) = open_temp();
    log.append(1, b"a", Some(b"1")).unwrap();
    let cfg = log.append_config(1, b"cfg-1").unwrap();
    log.apply_committed(cfg).unwrap();

    // Applied, but it wrote no row: in particular not an empty key
    // holding the encoded config, which would then leak into every
    // snapshot as if it were user data.
    assert_eq!(log.get_latest(b"").unwrap(), None);
    let mut buf = Vec::new();
    assert_eq!(log.export_snapshot(&mut buf).unwrap().entry_count, 1);

    cleanup(&path);
}

#[test]
fn a_snapshot_carries_the_configuration_to_its_recipient() {
    let (log, path) = open_temp();
    log.append_config(1, b"cfg-1").unwrap();
    let tail = log.append(1, b"a", Some(b"1")).unwrap();
    log.apply_committed(tail).unwrap();

    let mut buf = Vec::new();
    log.export_snapshot(&mut buf).unwrap();

    // The target's own config entry is wiped along with its log, so
    // without the header it would come out of the install with no
    // idea who is in the cluster.
    let (target, target_path) = open_temp();
    target.append_config(1, b"stale-cfg").unwrap();
    target.install_snapshot(&mut buf.as_slice()).unwrap();
    assert_eq!(target.current_config().unwrap(), Some(b"cfg-1".to_vec()));
    assert_eq!(
        target.get_latest(b"a").unwrap(),
        Some((tail, b"1".to_vec()))
    );

    cleanup(&path);
    cleanup(&target_path);
}

#[test]
fn a_snapshot_excludes_a_configuration_past_its_boundary() {
    let (log, path) = open_temp();
    log.append_config(1, b"cfg-1").unwrap();
    let tail = log.append(1, b"a", Some(b"1")).unwrap();
    log.apply_committed(tail).unwrap();
    // Appended after the applied watermark, so it is outside what
    // this snapshot describes -- the leader replicates it as an
    // ordinary entry right after the install instead.
    log.append_config(1, b"cfg-2").unwrap();

    let mut buf = Vec::new();
    log.export_snapshot(&mut buf).unwrap();

    let (target, target_path) = open_temp();
    target.install_snapshot(&mut buf.as_slice()).unwrap();
    assert_eq!(target.current_config().unwrap(), Some(b"cfg-1".to_vec()));

    cleanup(&path);
    cleanup(&target_path);
}

/// A committed delete *does* travel, as a tombstone.
///
/// This inverts what it asserted before versioning: a tombstone used to
/// be absence, and absence needs no representation. Now it is the only
/// thing that can tell a recipient the key did not exist as of a
/// checkpoint taken after the delete, so dropping it would be losing
/// state rather than saving space.
#[test]
fn export_snapshot_carries_a_committed_delete_as_a_tombstone() {
    let (log, path) = open_temp();
    let first = log.append(1, b"a", Some(b"1")).unwrap();
    let b_seq = log.append(1, b"b", Some(b"2")).unwrap();
    let del = log.append(1, b"a", None).unwrap();
    log.apply_committed(del).unwrap();

    let mut buf = Vec::new();
    let meta = log.export_snapshot(&mut buf).unwrap();
    // "a"'s value, "a"'s tombstone, and "b" -- every retained row.
    assert_eq!(meta.entry_count, 3);

    let (target, target_path) = open_temp();
    target.install_snapshot(&mut buf.as_slice()).unwrap();

    // Currently absent, as on the source.
    assert_eq!(target.get_latest(b"a").unwrap(), None);
    // And each row came back at its *own* seq, not the boundary --
    // which is what lets a checkpoint below the delete still see it.
    assert_eq!(
        target.get_as_of(b"a", first).unwrap(),
        Some((first, b"1".to_vec()))
    );
    assert_eq!(target.get_as_of(b"a", del).unwrap(), None);
    assert_eq!(
        target.get_latest(b"b").unwrap(),
        Some((b_seq, b"2".to_vec()))
    );

    cleanup(&path);
    cleanup(&target_path);
}

#[test]
fn compact_removes_entries_below_threshold_but_preserves_current_value() {
    let (log, path) = open_temp();
    log.append(1, b"k", Some(b"v1")).unwrap();
    log.append(1, b"k", Some(b"v2")).unwrap();
    let s3 = log.append(1, b"k", Some(b"v3")).unwrap();
    let above = log.append(1, b"k", Some(b"v4")).unwrap();
    log.apply_committed(above).unwrap();

    log.compact(above).unwrap();

    // Everything below the threshold is gone from `log`...
    assert!(log.entries_in_range(0, above).unwrap().is_empty());
    assert_eq!(log.term_at(1).unwrap(), None);
    // ...except the floor boundary itself (`below_seq - 1`), which
    // `last_entry`/`term_at` still need an answer for even once `log`
    // has nothing left at or below it.
    assert_eq!(log.term_at(s3).unwrap(), Some(1));

    // ...but `get_latest` still works, since `latest` is untouched by
    // `compact` (it was already brought up to date by `apply_committed`
    // above).
    let (seq, val) = log.get_latest(b"k").unwrap().unwrap();
    assert_eq!(seq, above);
    assert_eq!(val, b"v4".to_vec());
    cleanup(&path);
}

#[test]
fn compact_clamps_to_the_applied_watermark_rather_than_losing_data() {
    // The key new safety invariant: `compact` must never physically
    // remove a `log` entry that `apply_committed` hasn't copied into
    // `latest` yet, since `log` is the only place that data exists
    // until it has been. Asking to compact past what's been applied
    // must silently do *less*, never lose data.
    let (log, path) = open_temp();
    log.append(1, b"k", Some(b"v1")).unwrap();
    let s2 = log.append(1, b"k", Some(b"v2")).unwrap();
    // Deliberately no `apply_committed` call before compacting.

    log.compact(s2 + 1).unwrap();

    // Nothing was removed -- clamped to applied_seq (0), which is
    // `<=` the existing compact watermark (also 0), a no-op.
    assert_eq!(log.entries_in_range(0, s2 + 1).unwrap().len(), 2);

    // Once applied, a later compact call can now proceed for real.
    log.apply_committed(s2).unwrap();
    log.compact(s2 + 1).unwrap();
    assert!(log.entries_in_range(0, s2 + 1).unwrap().is_empty());
    assert_eq!(log.get_latest(b"k").unwrap(), Some((s2, b"v2".to_vec())));

    cleanup(&path);
}

#[test]
fn compact_to_the_tail_leaves_last_entry_and_term_at_answerable() {
    // Compacting all the way through the current tail empties `log`
    // entirely (unlike the old per-key compaction, which always left
    // at least the newest entry behind). `last_entry`/`term_at` must
    // still be able to answer for that boundary, the same way they do
    // after `install_snapshot`.
    let (log, path) = open_temp();
    log.append(1, b"a", Some(b"1")).unwrap();
    log.append(1, b"a", Some(b"2")).unwrap();
    let last = log.append(2, b"b", Some(b"x")).unwrap();
    log.apply_committed(last).unwrap();
    log.compact(last + 1).unwrap();

    assert!(log.entries_in_range(0, last + 1).unwrap().is_empty());
    assert_eq!(log.last_entry().unwrap(), Some((last, 2)));
    assert_eq!(log.term_at(last).unwrap(), Some(2));

    // Sequencing continues correctly even though `log` is empty.
    let next = log.append(2, b"c", Some(b"new")).unwrap();
    assert_eq!(next, last + 1);

    cleanup(&path);
}

#[test]
fn compact_resumes_from_the_previous_watermark_across_calls() {
    // Two compaction passes should each only need to touch entries
    // added since the last one — the second call must not need to
    // rescan what the first one already deleted.
    let (log, path) = open_temp();
    log.append(1, b"k", Some(b"v1")).unwrap();
    let s2 = log.append(1, b"k", Some(b"v2")).unwrap();
    log.apply_committed(s2).unwrap();
    log.compact(s2 + 1).unwrap();
    assert!(log.entries_in_range(0, s2 + 1).unwrap().is_empty());

    log.append(1, b"k", Some(b"v3")).unwrap();
    let s4 = log.append(1, b"k", Some(b"v4")).unwrap();
    log.apply_committed(s4).unwrap();
    log.compact(s4 + 1).unwrap();

    assert!(log.entries_in_range(0, s4 + 1).unwrap().is_empty());
    let (seq, val) = log.get_latest(b"k").unwrap().unwrap();
    assert_eq!(seq, s4);
    assert_eq!(val, b"v4".to_vec());

    // A second call with a threshold at or below the previous one is a
    // no-op rather than an error or a rescan.
    log.compact(s2 + 1).unwrap();
    assert_eq!(log.get_latest(b"k").unwrap().unwrap().0, s4);

    cleanup(&path);
}

#[test]
fn compact_handles_many_entries_across_chunks() {
    // Exercises the chunked-flush path in `compact` (WRITE_CHUNK is
    // 1000) with more entries below the threshold than fit in one
    // batch.
    let (log, path) = open_temp();
    let mut last_seq = 0;
    for i in 0..2500u32 {
        last_seq = log.append(1, b"hot", Some(&i.to_be_bytes())).unwrap();
    }
    log.apply_committed(last_seq).unwrap();
    log.compact(last_seq + 1).unwrap();
    assert!(log.entries_in_range(0, last_seq + 1).unwrap().is_empty());
    let (seq, _) = log.get_latest(b"hot").unwrap().unwrap();
    assert_eq!(seq, last_seq);
    cleanup(&path);
}

#[test]
fn truncate_from_removes_the_suffix_and_resumes_sequencing_there() {
    let (log, path) = open_temp();
    log.append(1, b"a", Some(b"1")).unwrap();
    let keep = log.append(1, b"b", Some(b"2")).unwrap();
    log.append(1, b"c", Some(b"3")).unwrap();
    log.append(1, b"d", Some(b"4")).unwrap();

    log.truncate_from(keep + 1).unwrap();

    assert!(log.entries_in_range(keep + 1, 100).unwrap().is_empty());
    assert_eq!(log.last_entry().unwrap(), Some((keep, 1)));

    // Sequencing resumes right after the truncation point.
    let next = log.append(2, b"e", Some(b"5")).unwrap();
    assert_eq!(next, keep + 1);
    cleanup(&path);
}

#[test]
fn truncate_from_never_touches_latest() {
    // `truncate_from` only ever removes *uncommitted* entries (see its
    // doc), and `latest` only ever reflects *committed* ones -- so a
    // truncation should never have anything to fix up there, unlike
    // the old design where `latest` could be ahead of what's
    // committed and truncation had to revert it.
    let (log, path) = open_temp();
    let keep = log.append(1, b"k", Some(b"v1")).unwrap();
    log.apply_committed(keep).unwrap();
    log.append(1, b"k", Some(b"v2")).unwrap(); // truncated away below

    log.truncate_from(keep + 1).unwrap();

    // The committed value is completely unaffected by truncating the
    // uncommitted write that came after it.
    assert_eq!(log.get_latest(b"k").unwrap(), Some((keep, b"v1".to_vec())));
    cleanup(&path);
}

#[test]
fn truncate_from_at_or_above_the_tail_is_a_no_op() {
    let (log, path) = open_temp();
    let last = log.append(1, b"a", Some(b"1")).unwrap();

    log.truncate_from(last + 1).unwrap();
    assert_eq!(log.last_entry().unwrap(), Some((last, 1)));
    cleanup(&path);
}

#[test]
fn entries_in_range_returns_terms_in_order() {
    let (log, path) = open_temp();
    log.append(1, b"a", Some(b"1")).unwrap();
    log.append(1, b"b", Some(b"2")).unwrap();
    log.append(2, b"c", Some(b"3")).unwrap();
    log.append(2, b"d", Some(b"4")).unwrap();

    let entries = log.entries_in_range(2, 4).unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].seq, 2);
    assert_eq!(entries[0].term, 1);
    assert_eq!(entries[0].payload.writes()[0].key, b"b");
    assert_eq!(entries[1].seq, 3);
    assert_eq!(entries[1].term, 2);
    assert_eq!(entries[1].payload.writes()[0].key, b"c");
    cleanup(&path);
}

#[test]
fn last_entry_and_term_at_track_the_log_tail() {
    let (log, path) = open_temp();
    assert_eq!(log.last_entry().unwrap(), None);

    log.append(1, b"a", Some(b"1")).unwrap();
    let last_seq = log.append(3, b"b", Some(b"2")).unwrap();

    assert_eq!(log.last_entry().unwrap(), Some((last_seq, 3)));
    assert_eq!(log.term_at(1).unwrap(), Some(1));
    assert_eq!(log.term_at(last_seq).unwrap(), Some(3));
    assert_eq!(log.term_at(last_seq + 1).unwrap(), None);
    cleanup(&path);
}

#[test]
fn export_snapshot_reflects_compacted_state() {
    let (log, path) = open_temp();
    log.append(1, b"a", Some(b"1")).unwrap();
    log.append(1, b"a", Some(b"2")).unwrap();
    let last = log.append(2, b"b", Some(b"x")).unwrap();
    log.apply_committed(last).unwrap();
    log.compact(last + 1).unwrap();

    let mut buf = Vec::new();
    let meta = log.export_snapshot(&mut buf).unwrap();
    // Every *retained version*: "a" at 1, "a" at 2, "b" at 3. An export
    // is no longer one record per live key, because a key is no longer
    // one row -- compacting `log` says nothing about how many versions
    // `versions` is still holding.
    assert_eq!(meta.entry_count, 3);
    assert_eq!(meta.last_included_seq, last);
    assert_eq!(meta.last_included_term, 2);
    assert!(buf.starts_with(SNAPSHOT_MAGIC));

    // Collecting the superseded version is what brings an export back
    // down to one record per key.
    log.gc_versions(usize::MAX).unwrap();
    let mut buf = Vec::new();
    assert_eq!(log.export_snapshot(&mut buf).unwrap().entry_count, 2);

    cleanup(&path);
}

#[test]
fn export_snapshot_reflects_applied_seq_not_the_raw_log_tail() {
    // An appended-but-uncommitted entry must not be described by the
    // snapshot's own metadata, even though it's physically the log's
    // tail -- `latest` (what's actually being exported) doesn't
    // reflect it yet either.
    let (log, path) = open_temp();
    let applied = log.append(1, b"a", Some(b"1")).unwrap();
    log.apply_committed(applied).unwrap();
    log.append(1, b"b", Some(b"2")).unwrap(); // uncommitted

    let mut buf = Vec::new();
    let meta = log.export_snapshot(&mut buf).unwrap();
    assert_eq!(meta.last_included_seq, applied);
    assert_eq!(meta.last_included_term, 1);
    assert_eq!(meta.entry_count, 1); // only "a"

    cleanup(&path);
}

#[test]
fn install_snapshot_replaces_state_and_continues_sequencing() {
    let (log, path) = open_temp();
    log.append(1, b"a", Some(b"1")).unwrap();
    log.append(1, b"a", Some(b"2")).unwrap();
    let last = log.append(2, b"b", Some(b"x")).unwrap();
    log.apply_committed(last).unwrap();

    let mut buf = Vec::new();
    log.export_snapshot(&mut buf).unwrap();

    let (target, target_path) = open_temp();
    let meta = target.install_snapshot(&mut buf.as_slice()).unwrap();
    assert_eq!(meta.last_included_seq, last);
    assert_eq!(meta.last_included_term, 2);
    // Three retained versions, not two live keys: "a" was written
    // twice and nothing has collected the superseded row yet.
    assert_eq!(meta.entry_count, 3);

    // State came across correctly -- and "a" kept the seq it was
    // actually written at (2), rather than being re-stamped at the
    // snapshot boundary (3). Re-stamping would not merely lose history:
    // a checkpoint record's seq *is* its row's seq, so it would leave
    // every checkpoint claiming a point it was never taken at.
    let (seq, val) = target.get_latest(b"a").unwrap().unwrap();
    assert_eq!((seq, val), (2, b"2".to_vec()));
    assert_eq!(target.last_entry().unwrap(), Some((last, 2)));

    // Sequencing continues from where the snapshot left off, with no
    // gap, even though `log` has nothing before this new append.
    let next = target.append(2, b"c", Some(b"new")).unwrap();
    assert_eq!(next, last + 1);

    cleanup(&path);
    cleanup(&target_path);
}

#[test]
fn install_snapshot_wipes_prior_state_on_the_target() {
    // The target already has its own unrelated (committed) history;
    // installing a snapshot must fully replace it, not merge with it.
    let (target, target_path) = open_temp();
    let stale = target.append(1, b"stale", Some(b"old")).unwrap();
    target.apply_committed(stale).unwrap();

    let (source, source_path) = open_temp();
    let last = source.append(1, b"fresh", Some(b"new")).unwrap();
    source.apply_committed(last).unwrap();
    let mut buf = Vec::new();
    source.export_snapshot(&mut buf).unwrap();

    target.install_snapshot(&mut buf.as_slice()).unwrap();

    assert_eq!(target.get_latest(b"stale").unwrap(), None);
    assert_eq!(
        target.get_latest(b"fresh").unwrap(),
        Some((last, b"new".to_vec()))
    );

    cleanup(&target_path);
    cleanup(&source_path);
}

#[test]
fn next_seq_recovers_from_watermark_with_no_log_entries() {
    let (log, path) = open_temp();
    log.append(1, b"a", Some(b"1")).unwrap();
    let last = log.append(2, b"b", Some(b"2")).unwrap();
    log.apply_committed(last).unwrap();

    let mut buf = Vec::new();
    log.export_snapshot(&mut buf).unwrap();

    let (target, target_path) = open_temp();
    target.install_snapshot(&mut buf.as_slice()).unwrap();
    drop(target);

    // Reopen with no further appends: log CF is empty, so next_seq
    // must come from the meta watermark alone.
    let reopened = RaftLog::open(&target_path).unwrap();
    assert_eq!(reopened.last_entry().unwrap(), Some((last, 2)));
    let next = reopened.append(2, b"c", Some(b"3")).unwrap();
    assert_eq!(next, last + 1);

    cleanup(&path);
    cleanup(&target_path);
}

#[test]
fn term_state_defaults_when_never_persisted() {
    let (log, path) = open_temp();
    assert_eq!(log.term_state().unwrap(), (0, None));
    cleanup(&path);
}

#[test]
fn persist_term_state_round_trips_after_reopen() {
    let (log, path) = open_temp();
    log.persist_term_state(5, Some("node-b")).unwrap();
    drop(log);

    let reopened = RaftLog::open(&path).unwrap();
    assert_eq!(
        reopened.term_state().unwrap(),
        (5, Some("node-b".to_string()))
    );
    cleanup(&path);
}

#[test]
fn persist_term_state_clears_a_previous_vote() {
    let (log, path) = open_temp();
    log.persist_term_state(5, Some("node-b")).unwrap();
    log.persist_term_state(6, None).unwrap();
    assert_eq!(log.term_state().unwrap(), (6, None));
    cleanup(&path);
}

#[test]
fn recovers_next_seq_after_reopen() {
    let (log, path) = open_temp();
    log.append(1, b"a", Some(b"1")).unwrap();
    log.append(1, b"b", Some(b"2")).unwrap();
    drop(log);

    let reopened = RaftLog::open(&path).unwrap();
    let seq = reopened.append(1, b"c", Some(b"3")).unwrap();
    assert_eq!(seq, 3);
    cleanup(&path);
}

// --- multi-key entries ------------------------------------------------

/// Every payload shape has to survive a round trip through the row
/// encoding, because a `log` row is the only record of what an entry was.
/// Postcard writes an enum's discriminant positionally, so this is also
/// the test that notices if `EntryPayload`'s variants are ever reordered.
#[test]
fn every_payload_round_trips_through_a_log_row() {
    let (log, path) = open_temp();

    let payloads = [
        EntryPayload::NoOp,
        EntryPayload::Config(b"an encoded config".to_vec()),
        EntryPayload::Write(vec![Mutation::new(*b"k", Some(*b"v"))]),
        EntryPayload::Write(vec![Mutation::new(*b"gone", None::<Vec<u8>>)]),
        EntryPayload::Write(vec![
            Mutation::new(*b"a", Some(*b"1")),
            Mutation::new(*b"b", None::<Vec<u8>>),
            Mutation::new(Vec::new(), Some(Vec::new())),
        ]),
    ];
    for payload in &payloads {
        log.append_payload(7, payload).unwrap();
    }

    let stored = log.entries_in_range(1, payloads.len() as u64 + 1).unwrap();
    let round_tripped: Vec<_> = stored.into_iter().map(|e| e.payload).collect();
    assert_eq!(round_tripped, payloads);

    cleanup(&path);
}

/// Bounds checking used to be this module's own job, one hand-written
/// length guard per field. It is the decoder's now, and a corrupt row has
/// to still come back as an error rather than a panic or a wrong answer --
/// that property is the whole reason the hand-rolled framing was safe to
/// delete.
#[test]
fn a_corrupt_log_row_is_refused_rather_than_half_read() {
    let good = encode_log_entry(
        1,
        &EntryPayload::Write(vec![Mutation::new(*b"key", Some(*b"value"))]),
    )
    .unwrap();

    // Every truncation of a valid row.
    for cut in 0..good.len() {
        assert!(
            decode_log_entry(&good[..cut]).is_err(),
            "a row truncated to {cut} of {} bytes decoded anyway",
            good.len()
        );
    }

    // And a row claiming a payload variant that doesn't exist.
    let mut unknown_variant = good.clone();
    unknown_variant[1] = 0x7f;
    assert!(decode_log_entry(&unknown_variant).is_err());

    assert!(
        decode_log_entry(&good).is_ok(),
        "the fixture itself is valid"
    );
}

/// The point of putting a transaction's writes in one entry: they reach
/// `latest` together, at one seq, or not at all. Applying only up to the
/// entry *before* it must show none of them.
#[test]
fn a_multi_key_entry_applies_all_of_its_writes_at_one_seq() {
    let (log, path) = open_temp();

    log.append(1, b"untouched", Some(b"before")).unwrap();
    let txn = log
        .append_writes(
            1,
            vec![
                Mutation::new(*b"a", Some(*b"1")),
                Mutation::new(*b"b", Some(*b"2")),
                Mutation::new(*b"untouched", None::<Vec<u8>>),
            ],
        )
        .unwrap();

    // Committed only as far as the entry before it: none of it is visible.
    log.apply_committed(txn - 1).unwrap();
    assert_eq!(log.get_latest(b"a").unwrap(), None);
    assert_eq!(log.get_latest(b"b").unwrap(), None);
    assert!(log.get_latest(b"untouched").unwrap().is_some());

    log.apply_committed(txn).unwrap();
    assert_eq!(log.get_latest(b"a").unwrap(), Some((txn, b"1".to_vec())));
    assert_eq!(log.get_latest(b"b").unwrap(), Some((txn, b"2".to_vec())));
    // A delete inside the entry lands with the rest of it.
    assert_eq!(log.get_latest(b"untouched").unwrap(), None);

    cleanup(&path);
}

/// An entry with more mutations than `WRITE_CHUNK` is deliberately *not*
/// split across batches -- splitting it is exactly the tear the entry
/// exists to prevent -- so it has to apply whole anyway.
#[test]
fn a_multi_key_entry_larger_than_a_write_chunk_still_applies_whole() {
    let (log, path) = open_temp();

    let n = WRITE_CHUNK + 7;
    let writes: Vec<_> = (0..n)
        .map(|i| Mutation::new(format!("k{i}"), Some(format!("v{i}"))))
        .collect();
    let seq = log.append_writes(1, writes).unwrap();
    log.apply_committed(seq).unwrap();

    for i in [0, 1, n / 2, n - 1] {
        assert_eq!(
            log.get_latest(format!("k{i}").as_bytes()).unwrap(),
            Some((seq, format!("v{i}").into_bytes())),
            "key {i} of {n} did not land"
        );
    }

    cleanup(&path);
}

/// Two *separate* point reads straddling a commit see two points in
/// time -- that is not a torn batch, and no storage-level lock could make
/// it otherwise. Pinned here because it is the property that decides
/// where multi-key consistency has to come from: the lock table in
/// `grpc::KvService`, not `storage`. If this ever stops holding, the
/// reasoning in the module doc needs revisiting.
#[test]
fn two_separate_reads_can_straddle_a_commit() {
    let (log, path) = open_temp();

    let first = log
        .append_writes(
            1,
            vec![
                Mutation::new(*b"a", Some(*b"0")),
                Mutation::new(*b"b", Some(*b"0")),
            ],
        )
        .unwrap();
    log.apply_committed(first).unwrap();

    // A reader that reads `a`, is interrupted by a whole second entry,
    // then reads `b`, sees the two keys at different seqs. Spelled out
    // sequentially rather than raced, so it is a statement about the
    // interleaving rather than a flaky one about timing.
    let a = log.get_latest(b"a").unwrap();

    let second = log
        .append_writes(
            1,
            vec![
                Mutation::new(*b"a", Some(*b"1")),
                Mutation::new(*b"b", Some(*b"1")),
            ],
        )
        .unwrap();
    log.apply_committed(second).unwrap();

    let b = log.get_latest(b"b").unwrap();

    assert_eq!(a, Some((first, b"0".to_vec())));
    assert_eq!(b, Some((second, b"1".to_vec())));

    cleanup(&path);
}

/// `export_snapshot` is the one thing that reads many keys of `latest` in
/// a single operation, and `Keyspace::iter` pins a snapshot nonce so it
/// reads them all at one instant. That is what keeps a snapshot from
/// capturing an entry half-applied and handing its recipient a state no
/// node ever had -- a property this module leans on rather than provides,
/// so it is worth a test that would notice it changing.
#[test]
fn a_snapshot_never_captures_an_entry_half_applied() {
    let (log, path) = open_temp();

    let seq = log
        .append_writes(
            1,
            vec![
                Mutation::new(*b"a", Some(*b"same")),
                Mutation::new(*b"b", Some(*b"same")),
            ],
        )
        .unwrap();
    log.apply_committed(seq).unwrap();

    let mut buf = Vec::new();
    log.export_snapshot(&mut buf).unwrap();

    let (dest, dest_path) = open_temp();
    dest.install_snapshot(&mut buf.as_slice()).unwrap();
    assert_eq!(
        dest.get_latest(b"a").unwrap().map(|(_, v)| v),
        dest.get_latest(b"b").unwrap().map(|(_, v)| v),
        "both keys of one entry must survive a snapshot together"
    );

    cleanup(&path);
    cleanup(&dest_path);
}

/// Guards the `serde_bytes` attributes on this module's byte-string
/// fields, which nothing else can catch: without them serde encodes a
/// `Vec<u8>` as a sequence of `u8` and makes one serializer call per
/// byte, which is ~50 MB/s instead of ~2 GB/s -- and produces *byte-for-
/// byte identical output*, so no round-trip or format test notices.
///
/// A coarse threshold on purpose. 32 MiB inside 300ms is a floor of
/// about 110 MB/s, which is comfortably under what the fixed code does
/// on any machine that can run this suite (a few GB/s here, so ~20x
/// headroom) and comfortably over what the per-byte path manages.
#[test]
fn large_values_are_encoded_as_bytes_rather_than_byte_by_byte() {
    const SIZE: usize = 32 * 1024 * 1024;
    const BUDGET: std::time::Duration = std::time::Duration::from_millis(300);

    let payload = EntryPayload::Write(vec![Mutation::new(*b"k", Some(vec![0xABu8; SIZE]))]);

    let started = std::time::Instant::now();
    let encoded = encode_log_entry(1, &payload).unwrap();
    let encode_took = started.elapsed();

    let started = std::time::Instant::now();
    let decoded = decode_log_entry(&encoded).unwrap();
    let decode_took = started.elapsed();

    assert_eq!(
        decoded.payload, payload,
        "the fast path must still round-trip"
    );
    assert!(
        encode_took < BUDGET && decode_took < BUDGET,
        "encoding {SIZE} bytes took {encode_took:?} and decoding took {decode_took:?}, \
         both budgeted at {BUDGET:?} -- has a `#[serde(with = \"serde_bytes\")]` gone \
         missing from `Mutation` or `EntryPayload`?"
    );
}

/// A snapshot moves through a *bounded* channel, so neither end ever
/// holds all of it.
///
/// This is the property, and it is the test: the channel holds
/// `CHUNK_BACKLOG` chunks and the snapshot here is several times that.
/// An implementation that collected the export before installing it --
/// which is what this used to do, on both ends -- cannot get past a full
/// channel, so it would hang here rather than fail an assertion.
#[test]
fn a_snapshot_streams_through_a_bounded_channel_rather_than_being_buffered() {
    use crate::store::{CHUNK_BACKLOG, ChunkReader, ChunkWriter};
    use std::io::Write;

    let (source, source_path) = open_temp();
    // Comfortably more than the channel can hold at once.
    const KEYS: usize = 64;
    const VALUE_BYTES: usize = 64 * 1024;
    for i in 0..KEYS {
        source
            .append(
                1,
                format!("k{i}").as_bytes(),
                Some(&vec![i as u8; VALUE_BYTES]),
            )
            .unwrap();
    }
    source.apply_committed(KEYS as u64).unwrap();

    let (tx, rx) = tokio::sync::mpsc::channel(CHUNK_BACKLOG);
    let source = std::sync::Arc::new(source);
    let exporting = std::thread::spawn({
        let source = std::sync::Arc::clone(&source);
        move || {
            let mut writer = ChunkWriter::new(tx);
            let meta = source.export_snapshot(&mut writer).unwrap();
            writer.flush().unwrap();
            meta
        }
    });

    // Installing here, on this thread, while the export runs on that
    // one. Neither can finish without the other making room.
    let (dest, dest_path) = open_temp();
    let installed = dest.install_snapshot(&mut ChunkReader::new(rx)).unwrap();
    let exported = exporting
        .join()
        .expect("the exporter should not have panicked");

    assert_eq!(exported.entry_count, KEYS as u64);
    assert_eq!(installed.entry_count, exported.entry_count);
    assert_eq!(installed.last_included_seq, exported.last_included_seq);
    for i in 0..KEYS {
        // Each at the seq its own append took, not the boundary.
        assert_eq!(
            dest.get_latest(format!("k{i}").as_bytes()).unwrap(),
            Some((i as u64 + 1, vec![i as u8; VALUE_BYTES])),
            "key {i} did not survive the streamed transfer"
        );
    }

    cleanup(&source_path);
    cleanup(&dest_path);
}

/// The producer giving up has to reach the consumer as end-of-stream,
/// not as a silent truncation: a snapshot cut short must fail to parse
/// rather than install as a smaller, plausible-looking one.
#[test]
fn a_snapshot_cut_short_is_refused_rather_than_half_installed() {
    use crate::store::ChunkReader;

    let (source, source_path) = open_temp();
    source.append(1, b"k", Some(b"v")).unwrap();
    source.apply_committed(1).unwrap();
    let mut whole = Vec::new();
    source.export_snapshot(&mut whole).unwrap();

    // Everything but the last few bytes: a key/value record that stops
    // partway through.
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tx.try_send(whole[..whole.len() - 3].to_vec()).unwrap();
    drop(tx);

    let (dest, dest_path) = open_temp();
    assert!(
        dest.install_snapshot(&mut ChunkReader::new(rx)).is_err(),
        "a truncated snapshot must not install"
    );

    cleanup(&source_path);
    cleanup(&dest_path);
}

// --- versions ---------------------------------------------------------

/// A delete leaves a tombstone row rather than removing anything, and a
/// tombstone at the head reads exactly as "never written" does.
#[test]
fn a_committed_delete_leaves_a_tombstone_that_reads_as_absent() {
    let (log, path) = open_temp();
    let written = log.append(1, b"k", Some(b"v")).unwrap();
    log.apply_committed(written).unwrap();
    let deleted = log.append(1, b"k", None).unwrap();
    log.apply_committed(deleted).unwrap();

    assert_eq!(log.get_latest(b"k").unwrap(), None);
    assert_eq!(log.get_version(b"k").unwrap(), None);
    // But the rows are both still there -- that is the difference from
    // the single-version layout, and what a checkpoint below the delete
    // would read.
    assert_eq!(log.version_count(b"k").unwrap(), 2);
    assert_eq!(
        log.get_as_of(b"k", written).unwrap(),
        Some((written, b"v".to_vec()))
    );

    cleanup(&path);
}

#[test]
fn every_write_to_a_key_leaves_a_readable_version() {
    let (log, path) = open_temp();
    let a = log.append(1, b"k", Some(b"1")).unwrap();
    let b = log.append(1, b"k", Some(b"2")).unwrap();
    let c = log.append(1, b"k", Some(b"3")).unwrap();
    log.apply_committed(c).unwrap();

    assert_eq!(log.version_count(b"k").unwrap(), 3);
    assert_eq!(log.get_latest(b"k").unwrap(), Some((c, b"3".to_vec())));

    // Exactly on each version, and between them.
    assert_eq!(log.get_as_of(b"k", a).unwrap(), Some((a, b"1".to_vec())));
    assert_eq!(log.get_as_of(b"k", b).unwrap(), Some((b, b"2".to_vec())));
    assert_eq!(log.get_as_of(b"k", c).unwrap(), Some((c, b"3".to_vec())));
    assert_eq!(log.get_as_of(b"k", a - 1).unwrap(), None);

    // And the seq-only path agrees with the value-carrying one.
    for as_of in [a, b, c, a - 1] {
        assert_eq!(
            log.version_as_of(b"k", as_of).unwrap(),
            log.get_as_of(b"k", as_of).unwrap().map(|(seq, _)| seq),
        );
    }

    cleanup(&path);
}

/// A read as of some seq is settled: nothing written afterwards can
/// change what it says, however much of it there is.
#[test]
fn a_historical_read_is_unaffected_by_later_writes_and_deletes() {
    let (log, path) = open_temp();
    let pinned = log.append(1, b"k", Some(b"pinned")).unwrap();
    log.apply_committed(pinned).unwrap();

    for i in 0..20 {
        let seq = log
            .append(
                1,
                b"k",
                (i % 3 != 0)
                    .then(|| format!("v{i}").into_bytes())
                    .as_deref(),
            )
            .unwrap();
        log.apply_committed(seq).unwrap();
        assert_eq!(
            log.get_as_of(b"k", pinned).unwrap(),
            Some((pinned, b"pinned".to_vec())),
            "the pinned view changed after write {i}"
        );
    }

    cleanup(&path);
}

/// Both directions across a boundary: a key deleted after it, and a key
/// created after it.
#[test]
fn a_boundary_sees_what_existed_at_it_and_nothing_since() {
    let (log, path) = open_temp();
    let before = log.append(1, b"deleted-later", Some(b"v")).unwrap();
    log.apply_committed(before).unwrap();

    let boundary = before;

    let gone = log.append(1, b"deleted-later", None).unwrap();
    let made = log.append(1, b"made-later", Some(b"new")).unwrap();
    log.apply_committed(made.max(gone)).unwrap();

    // As of the boundary: the first key still had its value, and the
    // second did not exist.
    assert_eq!(
        log.get_as_of(b"deleted-later", boundary).unwrap(),
        Some((before, b"v".to_vec()))
    );
    assert_eq!(log.get_as_of(b"made-later", boundary).unwrap(), None);

    // Currently: the reverse.
    assert_eq!(log.get_latest(b"deleted-later").unwrap(), None);
    assert_eq!(
        log.get_latest(b"made-later").unwrap(),
        Some((made, b"new".to_vec()))
    );

    // And the same through a scan.
    let (keys, _) = log.scan_as_of(b"", boundary, 10, NO_BUDGET, |_| false).unwrap();
    assert_eq!(keys, vec![b"deleted-later".to_vec()]);
    let (keys, _) = log.scan_latest(b"", 10, NO_BUDGET, |_| false).unwrap();
    assert_eq!(keys, vec![b"made-later".to_vec()]);

    cleanup(&path);
}

/// A scan returns each key once however many versions it has, and a
/// tombstoned key not at all -- without either costing the caller batch
/// length.
#[test]
fn a_scan_dedups_versions_and_hides_tombstones_without_shortening_a_batch() {
    let (log, path) = open_temp();

    // 20 keys, each written three times, then every fourth deleted.
    for round in 0..3 {
        let writes: Vec<_> = (0..20)
            .map(|i| Mutation::new(format!("k{i:02}"), Some(format!("v{i}-{round}"))))
            .collect();
        let seq = log.append_writes(1, writes).unwrap();
        log.apply_committed(seq).unwrap();
    }
    let deletes: Vec<_> = (0..20)
        .step_by(4)
        .map(|i| Mutation::new(format!("k{i:02}"), None::<Vec<u8>>))
        .collect();
    let seq = log.append_writes(1, deletes).unwrap();
    log.apply_committed(seq).unwrap();

    // 15 keys survive; a batch of 10 is full, not shortened by the
    // tombstones or the superseded versions it stepped over.
    let (batch, resume) = log.scan_latest(b"", 10, NO_BUDGET, |_| false).unwrap();
    assert_eq!(batch.len(), 10);
    assert_eq!(resume, None, "a full batch did not run out of budget");
    assert_eq!(batch[0], b"k01".to_vec(), "k00 was deleted");
    let mut deduped = batch.clone();
    deduped.dedup();
    assert_eq!(deduped, batch, "a key came back more than once");

    let (all, _) = log.scan_latest(b"", 100, NO_BUDGET, |_| false).unwrap();
    assert_eq!(all.len(), 15);

    cleanup(&path);
}

/// The prefix-collision case, end to end through the store rather than
/// the encoding: `"a"` and `"ab"` must not see each other's versions.
#[test]
fn a_key_that_prefixes_another_keeps_its_own_versions() {
    let (log, path) = open_temp();
    let a1 = log.append(1, b"a", Some(b"a1")).unwrap();
    let ab1 = log.append(1, b"ab", Some(b"ab1")).unwrap();
    let a2 = log.append(1, b"a", Some(b"a2")).unwrap();
    let ab2 = log.append(1, b"ab", Some(b"ab2")).unwrap();
    log.apply_committed(ab2).unwrap();

    assert_eq!(log.version_count(b"a").unwrap(), 2);
    assert_eq!(log.version_count(b"ab").unwrap(), 2);

    assert_eq!(log.get_latest(b"a").unwrap(), Some((a2, b"a2".to_vec())));
    assert_eq!(log.get_latest(b"ab").unwrap(), Some((ab2, b"ab2".to_vec())));
    assert_eq!(log.get_as_of(b"a", a1).unwrap(), Some((a1, b"a1".to_vec())));
    assert_eq!(
        log.get_as_of(b"ab", ab1).unwrap(),
        Some((ab1, b"ab1".to_vec()))
    );
    // As of `a1`, "ab" did not exist yet -- a naive encoding would have
    // answered with one of "ab"'s rows here.
    assert_eq!(log.get_as_of(b"ab", a1).unwrap(), None);

    cleanup(&path);
}

/// One entry may contribute at most one row per key. Two mutations for
/// one key used to collapse on their own; now a value and a tombstone at
/// one seq would be two rows with the tombstone sorting first, so the
/// write would be buried under its own delete.
#[test]
fn duplicate_mutations_in_one_entry_collapse_to_the_last() {
    let (log, path) = open_temp();
    let seq = log
        .append_writes(
            1,
            vec![
                Mutation::new(b"k".to_vec(), Some(b"first".to_vec())),
                Mutation::new(b"other".to_vec(), Some(b"x".to_vec())),
                Mutation::new(b"k".to_vec(), None::<Vec<u8>>),
                Mutation::new(b"k".to_vec(), Some(b"last".to_vec())),
            ],
        )
        .unwrap();
    log.apply_committed(seq).unwrap();

    assert_eq!(log.version_count(b"k").unwrap(), 1);
    assert_eq!(log.get_latest(b"k").unwrap(), Some((seq, b"last".to_vec())));
    assert_eq!(
        log.get_latest(b"other").unwrap(),
        Some((seq, b"x".to_vec()))
    );

    cleanup(&path);
}

// --- version GC -------------------------------------------------------

/// Write `key` once per entry, at the seqs returned.
fn write_each(log: &RaftLog, key: &[u8], values: &[&[u8]]) -> Vec<u64> {
    values
        .iter()
        .map(|v| {
            let seq = log.append(1, key, Some(*v)).unwrap();
            log.apply_committed(seq).unwrap();
            seq
        })
        .collect()
}

/// With nothing pinning history, GC keeps exactly the current value.
#[test]
fn gc_keeps_only_the_newest_version_when_nothing_is_pinned() {
    let (log, path) = open_temp();
    let seqs = write_each(&log, b"k", &[b"1", b"2", b"3", b"4"]);
    assert_eq!(log.version_count(b"k").unwrap(), 4);

    let stats = log.gc_versions(usize::MAX).unwrap();
    assert!(stats.done);
    assert_eq!(stats.rows_removed, 3);
    assert_eq!(log.version_count(b"k").unwrap(), 1);
    assert_eq!(
        log.get_latest(b"k").unwrap(),
        Some((seqs[3], b"4".to_vec()))
    );

    cleanup(&path);
}

/// A checkpoint pins the newest version at or before its seq, and
/// nothing else.
#[test]
fn gc_retains_exactly_what_each_checkpoint_selects() {
    let (log, path) = open_temp();
    // Two writes, then the checkpoint, then two more -- so the
    // checkpoint genuinely falls between versions rather than after
    // all of them.
    let early = write_each(&log, b"k", &[b"1", b"2"]);
    let cp = log
        .append(1, &checkpoint_key(b"mid"), Some(&encode_record(0).unwrap()))
        .unwrap();
    log.apply_committed(cp).unwrap();
    let late = write_each(&log, b"k", &[b"3", b"4"]);
    assert!(cp > early[1] && cp < late[0]);
    assert_eq!(log.version_count(b"k").unwrap(), 4);

    log.gc_versions(usize::MAX).unwrap();

    // The checkpoint selects "2", so that survives; "1" and "3" are
    // reachable from nothing and go.
    assert_eq!(log.version_count(b"k").unwrap(), 2);
    assert_eq!(
        log.get_as_of(b"k", cp).unwrap(),
        Some((early[1], b"2".to_vec()))
    );
    assert_eq!(
        log.get_latest(b"k").unwrap(),
        Some((late[1], b"4".to_vec()))
    );

    cleanup(&path);
}

/// The head-tombstone rule, both ways round. Dropping a head tombstone
/// while something older survives would make a *current* read see a
/// value that was deleted.
#[test]
fn gc_drops_a_head_tombstone_only_when_nothing_older_survives() {
    // Pinned: the tombstone must stay, or the current read resurrects.
    let (log, path) = open_temp();
    let written = log.append(1, b"k", Some(b"v")).unwrap();
    log.apply_committed(written).unwrap();
    let cp = log
        .append(
            1,
            &checkpoint_key(b"before"),
            Some(&encode_record(0).unwrap()),
        )
        .unwrap();
    log.apply_committed(cp).unwrap();
    let deleted = log.append(1, b"k", None).unwrap();
    log.apply_committed(deleted).unwrap();

    log.gc_versions(usize::MAX).unwrap();
    assert_eq!(log.version_count(b"k").unwrap(), 2, "both rows are needed");
    assert_eq!(log.get_latest(b"k").unwrap(), None, "still deleted");
    assert_eq!(
        log.get_as_of(b"k", cp).unwrap(),
        Some((written, b"v".to_vec())),
        "the checkpoint still sees the value"
    );
    cleanup(&path);

    // Unpinned: nothing below survives, so the whole run goes.
    let (log, path) = open_temp();
    let written = log.append(1, b"k", Some(b"v")).unwrap();
    log.apply_committed(written).unwrap();
    let deleted = log.append(1, b"k", None).unwrap();
    log.apply_committed(deleted).unwrap();

    log.gc_versions(usize::MAX).unwrap();
    assert_eq!(
        log.version_count(b"k").unwrap(),
        0,
        "the run should be gone"
    );
    assert_eq!(log.get_latest(b"k").unwrap(), None);
    cleanup(&path);
}

/// A removed checkpoint stops pinning, and its own record is collected
/// along with the versions it was holding.
#[test]
fn removing_a_checkpoint_releases_what_it_pinned() {
    let (log, path) = open_temp();
    let seqs = write_each(&log, b"k", &[b"1", b"2"]);
    let cp = log
        .append(1, &checkpoint_key(b"c"), Some(&encode_record(0).unwrap()))
        .unwrap();
    log.apply_committed(cp).unwrap();
    let last = log.append(1, b"k", Some(b"3")).unwrap();
    log.apply_committed(last).unwrap();

    log.gc_versions(usize::MAX).unwrap();
    assert_eq!(
        log.version_count(b"k").unwrap(),
        2,
        "the checkpoint pins v2"
    );
    assert_eq!(log.live_checkpoint_seqs().unwrap(), vec![cp]);

    // Remove it, and the pin goes with it.
    let removed = log.append(1, &checkpoint_key(b"c"), None).unwrap();
    log.apply_committed(removed).unwrap();
    assert_eq!(log.live_checkpoint_seqs().unwrap(), Vec::<u64>::new());

    log.gc_versions(usize::MAX).unwrap();
    assert_eq!(log.version_count(b"k").unwrap(), 1);
    assert_eq!(log.get_latest(b"k").unwrap(), Some((last, b"3".to_vec())));
    assert_eq!(
        log.version_count(&checkpoint_key(b"c")).unwrap(),
        0,
        "the record's own rows should be collected too"
    );
    assert_eq!(seqs.len(), 2);

    cleanup(&path);
}

/// A budgeted pass stops at a key boundary, leaves a cursor, and the
/// next pass carries on from there -- across a reopen.
#[test]
fn gc_resumes_from_its_cursor_across_passes_and_reopens() {
    let (log, path) = open_temp();
    for i in 0..20 {
        write_each(&log, format!("k{i:02}").as_bytes(), &[b"1", b"2"]);
    }
    let total: usize = (0..20)
        .map(|i| log.version_count(format!("k{i:02}").as_bytes()).unwrap())
        .sum();
    assert_eq!(total, 40);

    // A budget far below the work available stops early.
    let first = log.gc_versions(4).unwrap();
    assert!(!first.done, "a budgeted pass should not claim to be done");
    assert!(first.rows_removed > 0 && first.rows_removed < 20);
    drop(log);

    // The cursor is in `meta`, so it survives a reopen.
    let log = RaftLog::open(&path).expect("reopen");
    let mut guard = 0;
    loop {
        let stats = log.gc_versions(4).unwrap();
        guard += 1;
        assert!(guard < 100, "GC made no progress");
        if stats.done {
            break;
        }
    }

    let remaining: usize = (0..20)
        .map(|i| log.version_count(format!("k{i:02}").as_bytes()).unwrap())
        .sum();
    assert_eq!(remaining, 20, "one version per key should survive");

    // A completed pass clears the cursor, so the next starts over.
    let again = log.gc_versions(usize::MAX).unwrap();
    assert!(again.done);
    assert_eq!(again.rows_removed, 0, "nothing left to collect");

    cleanup(&path);
}

/// A checkpoint created while a pass is in flight is above everything
/// that pass read, so the version it selects is each key's newest --
/// which GC always retains. Simulated by running the pass against the
/// live set as it was *before* the checkpoint existed.
#[test]
fn a_checkpoint_created_during_a_pass_is_still_readable_after_it() {
    let (log, path) = open_temp();
    write_each(&log, b"k", &[b"1", b"2"]);

    // The pass reads an empty live set here.
    let snaps = log.live_checkpoint_seqs().unwrap();
    assert!(snaps.is_empty());

    let cp = log
        .append(
            1,
            &checkpoint_key(b"racing"),
            Some(&encode_record(0).unwrap()),
        )
        .unwrap();
    log.apply_committed(cp).unwrap();

    // ...and only now collects, with the stale (empty) set.
    log.gc_versions(usize::MAX).unwrap();

    // The checkpoint reads the newest version, which survived.
    assert_eq!(log.checkpoint_seq(b"racing").unwrap(), Some(cp));
    assert_eq!(
        log.get_as_of(b"k", cp).unwrap(),
        Some((log.get_version(b"k").unwrap().unwrap(), b"2".to_vec()))
    );

    cleanup(&path);
}

// --- the checkpoint registry ------------------------------------------

#[test]
fn checkpoints_are_listed_with_the_seq_their_record_landed_at() {
    let (log, path) = open_temp();
    let a = log
        .append(
            1,
            &checkpoint_key(b"alpha"),
            Some(&encode_record(111).unwrap()),
        )
        .unwrap();
    let b = log
        .append(
            1,
            &checkpoint_key(b"beta"),
            Some(&encode_record(222).unwrap()),
        )
        .unwrap();
    log.apply_committed(b).unwrap();

    let listed = log.list_checkpoints().unwrap();
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[0].id, b"alpha".to_vec());
    assert_eq!(listed[0].seq, a);
    assert_eq!(listed[0].created_unix_ms, 111);
    assert_eq!(listed[1].id, b"beta".to_vec());
    assert_eq!(listed[1].seq, b);

    assert_eq!(log.checkpoint_seq(b"alpha").unwrap(), Some(a));
    assert_eq!(log.checkpoint_seq(b"nope").unwrap(), None);
    assert_eq!(log.checkpoint_count().unwrap(), 2);
    assert_eq!(log.live_checkpoint_seqs().unwrap(), vec![a, b]);

    cleanup(&path);
}

/// A removed checkpoint stops being live immediately, before the GC has
/// collected its rows.
#[test]
fn a_removed_checkpoint_stops_being_listed() {
    let (log, path) = open_temp();
    let made = log
        .append(1, &checkpoint_key(b"c"), Some(&encode_record(1).unwrap()))
        .unwrap();
    log.apply_committed(made).unwrap();
    assert_eq!(log.checkpoint_count().unwrap(), 1);

    let removed = log.append(1, &checkpoint_key(b"c"), None).unwrap();
    log.apply_committed(removed).unwrap();

    assert_eq!(log.checkpoint_seq(b"c").unwrap(), None);
    assert_eq!(log.checkpoint_count().unwrap(), 0);
    assert!(log.list_checkpoints().unwrap().is_empty());

    cleanup(&path);
}

/// Checkpoint records are ordinary rows, so they travel in a Raft
/// snapshot with the versions they pin -- and arrive still naming the
/// seq they were taken at.
#[test]
fn a_checkpoint_and_what_it_pins_survive_a_raft_snapshot() {
    let (source, source_path) = open_temp();
    let first = source.append(1, b"k", Some(b"old")).unwrap();
    source.apply_committed(first).unwrap();
    let cp = source
        .append(
            1,
            &checkpoint_key(b"nightly"),
            Some(&encode_record(7).unwrap()),
        )
        .unwrap();
    source.apply_committed(cp).unwrap();
    let second = source.append(1, b"k", Some(b"new")).unwrap();
    source.apply_committed(second).unwrap();

    let mut buf = Vec::new();
    source.export_snapshot(&mut buf).unwrap();

    let (target, target_path) = open_temp();
    target.install_snapshot(&mut buf.as_slice()).unwrap();

    // The checkpoint still names the seq it was taken at, not the
    // snapshot boundary...
    assert_eq!(target.checkpoint_seq(b"nightly").unwrap(), Some(cp));
    // ...and reading at it gives what it gave on the source.
    assert_eq!(
        target.get_as_of(b"k", cp).unwrap(),
        Some((first, b"old".to_vec()))
    );
    assert_eq!(
        target.get_latest(b"k").unwrap(),
        Some((second, b"new".to_vec()))
    );

    cleanup(&source_path);
    cleanup(&target_path);
}

/// A key scan must not expose the reserved range, so a checkpoint is
/// invisible to anything walking keys -- the caller's `skip` closure is
/// what enforces it, exactly as it does for accounts.
#[test]
fn checkpoint_records_sit_in_the_reserved_range() {
    let (log, path) = open_temp();
    let cp = log
        .append(1, &checkpoint_key(b"c"), Some(&encode_record(0).unwrap()))
        .unwrap();
    let k = log.append(1, b"visible", Some(b"v")).unwrap();
    log.apply_committed(k.max(cp)).unwrap();

    assert!(crate::keyspace::is_reserved(&checkpoint_key(b"c")));
    let (keys, _) = log
        .scan_latest(b"", 10, NO_BUDGET, crate::keyspace::is_reserved)
        .unwrap();
    assert_eq!(keys, vec![b"visible".to_vec()]);

    cleanup(&path);
}

/// The bound a filtered scan needs: `skip` rejecting nearly everything
/// must not turn one call into a walk of the whole keyspace.
///
/// The batch comes back short -- empty, here -- with a cursor, which is
/// the case the old "append a zero byte to the last key" rule had no
/// answer for.
#[test]
fn a_scan_that_skips_everything_stops_at_its_budget_and_says_where() {
    let (log, path) = open_temp();

    let writes: Vec<_> = (0..50)
        .map(|i| Mutation::new(format!("k{i:02}"), Some("v")))
        .collect();
    let seq = log.append_writes(1, writes).unwrap();
    log.apply_committed(seq).unwrap();

    // Nothing is acceptable, so the batch can never fill; the budget is
    // the only thing that ends the walk.
    let (keys, resume) = log.scan_latest(b"", 10, 20, |_| true).unwrap();
    assert!(keys.is_empty());
    assert_eq!(resume, Some(b"k20".to_vec()), "should resume where it stopped");

    // Resuming there sees the rest, and the walk as a whole misses
    // nothing: the cursor is a key the first pass did not decide about.
    let (keys, resume) = log
        .scan_latest(&resume.unwrap(), 10, 20, |key| key != b"k25")
        .unwrap();
    assert_eq!(keys, vec![b"k25".to_vec()]);
    assert_eq!(resume, Some(b"k40".to_vec()));

    // A budget the scan never reaches leaves no cursor behind.
    let (keys, resume) = log.scan_latest(b"", 10, NO_BUDGET, |_| false).unwrap();
    assert_eq!(keys.len(), 10);
    assert_eq!(resume, None);

    cleanup(&path);
}

/// Skipped keys count against the budget but not against `limit` -- the
/// two bounds are about different things, and conflating them is what
/// would make a batch silently short for a reason the caller cannot see.
#[test]
fn the_budget_counts_keys_examined_and_the_limit_counts_keys_kept() {
    let (log, path) = open_temp();

    let writes: Vec<_> = (0..30)
        .map(|i| Mutation::new(format!("k{i:02}"), Some("v")))
        .collect();
    let seq = log.append_writes(1, writes).unwrap();
    log.apply_committed(seq).unwrap();

    // Every third key is acceptable. A budget of 30 examines them all,
    // so the batch fills normally and the budget never bites.
    let (keys, resume) = log
        .scan_latest(b"", 5, 30, |key| {
            let n: usize = std::str::from_utf8(&key[1..]).unwrap().parse().unwrap();
            !n.is_multiple_of(3)
        })
        .unwrap();
    assert_eq!(keys.len(), 5, "skipped keys must not shorten the batch");
    assert_eq!(keys[4], b"k12".to_vec());
    assert_eq!(resume, None);

    cleanup(&path);
}

// --- read_many -----------------------------------------------------------

fn rows(read: MultiRead) -> (u64, Vec<ReadRow>) {
    match read {
        MultiRead::Ok { as_of, rows } => (as_of, rows),
        MultiRead::NoSuchCheckpoint => panic!("expected rows, got NoSuchCheckpoint"),
    }
}

fn fetch_all(keys: &[&[u8]], fetch: Fetch) -> Vec<(Vec<u8>, Fetch)> {
    keys.iter().map(|key| (key.to_vec(), fetch)).collect()
}

/// Positional, duplicates and all, with a missing or deleted key as an
/// empty position -- and `as_of` the applied watermark.
#[test]
fn read_many_answers_each_key_in_the_order_asked() {
    let (log, path) = open_temp();
    let a = log.append(1, b"a", Some(b"1")).unwrap();
    let b = log.append(1, b"b", Some(b"2")).unwrap();
    log.append(1, b"gone", Some(b"x")).unwrap();
    let last = log.append(1, b"gone", None).unwrap();
    log.apply_committed(last).unwrap();

    let keys: [&[u8]; 5] = [b"b", b"missing", b"a", b"gone", b"b"];
    let (as_of, found) = rows(
        log.read_many(&fetch_all(&keys, Fetch::Value), ReadPoint::Latest, NO_BUDGET)
            .unwrap(),
    );
    assert_eq!(as_of, last);
    assert_eq!(
        found,
        vec![
            Some((b, Some(b"2".to_vec()))),
            None,
            Some((a, Some(b"1".to_vec()))),
            None,
            Some((b, Some(b"2".to_vec()))),
        ]
    );

    let (_, versions) = rows(
        log.read_many(&fetch_all(&keys, Fetch::VersionOnly), ReadPoint::Latest, NO_BUDGET)
            .unwrap(),
    );
    assert_eq!(versions, vec![Some((b, None)), None, Some((a, None)), None, Some((b, None))]);
    cleanup(&path);
}

/// A checkpoint read is as of the checkpoint, and reports its seq; an
/// unknown or removed one is said to be missing rather than read as
/// empty.
#[test]
fn read_many_at_a_checkpoint_reads_as_of_it() {
    let (log, path) = open_temp();
    let old = log.append(1, b"k", Some(b"old")).unwrap();
    let cp = log
        .append(1, &checkpoint_key(b"cp"), Some(&encode_record(0).unwrap()))
        .unwrap();
    let new = log.append(1, b"k", Some(b"new")).unwrap();
    log.apply_committed(new).unwrap();

    let keys = fetch_all(&[b"k"], Fetch::Value);
    let (as_of, found) = rows(
        log.read_many(&keys, ReadPoint::Checkpoint(b"cp".to_vec()), NO_BUDGET)
            .unwrap(),
    );
    assert_eq!(as_of, cp);
    assert_eq!(found, vec![Some((old, Some(b"old".to_vec())))]);

    assert!(matches!(
        log.read_many(&keys, ReadPoint::Checkpoint(b"nope".to_vec()), NO_BUDGET)
            .unwrap(),
        MultiRead::NoSuchCheckpoint
    ));

    let removed = log.append(1, &checkpoint_key(b"cp"), None).unwrap();
    log.apply_committed(removed).unwrap();
    assert!(matches!(
        log.read_many(&keys, ReadPoint::Checkpoint(b"cp".to_vec()), NO_BUDGET)
            .unwrap(),
        MultiRead::NoSuchCheckpoint
    ));
    cleanup(&path);
}

/// The budget cuts the answer to a prefix, never to nothing; a charged
/// position counts though nothing was read for it; versions alone are
/// never cut.
#[test]
fn read_many_stops_at_the_value_budget_but_always_answers_the_first_key() {
    let (log, path) = open_temp();
    for key in [&b"a"[..], b"b", b"c"] {
        log.append(1, key, Some(&[7u8; 10])).unwrap();
    }
    log.apply_committed(3).unwrap();
    let keys: [&[u8]; 3] = [b"a", b"b", b"c"];

    let (_, found) = rows(
        log.read_many(&fetch_all(&keys, Fetch::Value), ReadPoint::Latest, 25)
            .unwrap(),
    );
    assert_eq!(found.len(), 2, "20 bytes fit in 25; the third would make 30");

    let (_, found) = rows(
        log.read_many(&fetch_all(&keys, Fetch::Value), ReadPoint::Latest, 5)
            .unwrap(),
    );
    assert_eq!(found.len(), 1, "the first key comes back even alone over budget");
    assert_eq!(found[0], Some((1, Some(vec![7u8; 10]))));

    let charged = vec![
        (b"a".to_vec(), Fetch::Charge(20)),
        (b"b".to_vec(), Fetch::Value),
    ];
    let (_, found) = rows(log.read_many(&charged, ReadPoint::Latest, 25).unwrap());
    assert_eq!(found, vec![Some((1, None))], "a charge counts, and reads nothing");

    let (_, found) = rows(
        log.read_many(&fetch_all(&keys, Fetch::VersionOnly), ReadPoint::Latest, 0)
            .unwrap(),
    );
    assert_eq!(found.len(), 3);
    cleanup(&path);
}

/// What `read_many` is for: keys written by one entry are never seen at
/// two different entries, even while `apply_committed` is mid-way
/// through a run of entries -- and `as_of` names exactly the last entry
/// the snapshot can see.
///
/// Every entry here writes `x` and `y` together, plus enough filler to
/// pass `WRITE_CHUNK`, so `apply_committed` flushes after each one. A
/// reader racing it must find `x` and `y` at the same seq, and that seq
/// must be `as_of`: every entry writes `x`, so `x`'s head is the last
/// entry applied. A watermark written only with the final batch would
/// let a mid-way snapshot see `x` past `as_of`.
#[test]
fn read_many_never_sees_an_entry_or_the_watermark_half_applied() {
    let (log, path) = open_temp();
    const ENTRIES: usize = 12;
    let mut last = 0;
    for round in 0..ENTRIES {
        let value = round.to_be_bytes().to_vec();
        let mut writes = vec![
            Mutation::new(b"x".to_vec(), Some(value.clone())),
            Mutation::new(b"y".to_vec(), Some(value)),
        ];
        for filler in 0..WRITE_CHUNK {
            writes.push(Mutation::new(
                format!("filler/{filler:05}").into_bytes(),
                Some(vec![round as u8]),
            ));
        }
        last = log.append_writes(1, writes).unwrap();
    }

    let done = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|scope| {
        scope.spawn(|| {
            log.apply_committed(last).unwrap();
            done.store(true, std::sync::atomic::Ordering::Release);
        });
        let keys = fetch_all(&[b"x", b"y"], Fetch::Value);
        let mut observed = std::collections::BTreeSet::new();
        loop {
            let finished = done.load(std::sync::atomic::Ordering::Acquire);
            let (as_of, found) = rows(log.read_many(&keys, ReadPoint::Latest, NO_BUDGET).unwrap());
            match (&found[0], &found[1]) {
                (None, None) => assert_eq!(as_of, 0, "nothing applied, so nothing seen"),
                (Some(x), Some(y)) => {
                    assert_eq!(x, y, "x and y were written by one entry");
                    assert_eq!(x.0, as_of, "as_of names the last entry the snapshot sees");
                    observed.insert(as_of);
                }
                torn => panic!("one entry's keys seen apart: {torn:?}"),
            }
            if finished {
                break;
            }
        }
        assert!(observed.contains(&last));
    });
    cleanup(&path);
}
