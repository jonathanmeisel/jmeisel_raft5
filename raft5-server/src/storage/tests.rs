use super::snapshot::SNAPSHOT_MAGIC;
use super::*;

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
    assert_eq!(entries[0].payload.writes(), [Mutation::new(*b"k", None::<Vec<u8>>)]);

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
    assert_eq!(stored[0].payload.writes(), [Mutation::new(*b"a", Some(*b"1"))]);
    assert_eq!(stored[2].payload.writes(), [Mutation::new(*b"b", Some(*b"2"))]);
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

#[test]
fn export_snapshot_omits_a_committed_delete() {
    let (log, path) = open_temp();
    log.append(1, b"a", Some(b"1")).unwrap();
    log.append(1, b"b", Some(b"2")).unwrap();
    let del = log.append(1, b"a", None).unwrap();
    log.apply_committed(del).unwrap();

    let mut buf = Vec::new();
    let meta = log.export_snapshot(&mut buf).unwrap();
    assert_eq!(meta.entry_count, 1); // only "b" survives

    let (target, target_path) = open_temp();
    target.install_snapshot(&mut buf.as_slice()).unwrap();
    assert_eq!(target.get_latest(b"a").unwrap(), None);
    assert_eq!(
        target.get_latest(b"b").unwrap(),
        Some((meta.last_included_seq, b"2".to_vec()))
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
    assert_eq!(meta.entry_count, 2);
    assert_eq!(meta.last_included_seq, last);
    assert_eq!(meta.last_included_term, 2);
    assert!(buf.starts_with(SNAPSHOT_MAGIC));
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
    assert_eq!(meta.entry_count, 2);

    // State came across correctly.
    let (seq, val) = target.get_latest(b"a").unwrap().unwrap();
    assert_eq!((seq, val), (last, b"2".to_vec()));
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

    assert!(decode_log_entry(&good).is_ok(), "the fixture itself is valid");
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

    assert_eq!(decoded.payload, payload, "the fast path must still round-trip");
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
            .append(1, format!("k{i}").as_bytes(), Some(&vec![i as u8; VALUE_BYTES]))
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
    let exported = exporting.join().expect("the exporter should not have panicked");

    assert_eq!(exported.entry_count, KEYS as u64);
    assert_eq!(installed.entry_count, exported.entry_count);
    assert_eq!(installed.last_included_seq, exported.last_included_seq);
    for i in 0..KEYS {
        assert_eq!(
            dest.get_latest(format!("k{i}").as_bytes()).unwrap(),
            Some((KEYS as u64, vec![i as u8; VALUE_BYTES])),
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

// --- the `latest_seq` index -------------------------------------------

/// The invariant `latest_seq` exists to maintain: it holds a row for
/// exactly the keys `latest` does, carrying exactly the seq encoded
/// inside that key's `latest` row.
///
/// Checked in both directions on purpose. A missing index row makes
/// `get_version` answer "no value" for a key that has one; a *stale* one
/// left behind by a delete makes it answer with a seq for a key that no
/// longer exists. Only walking both keyspaces catches both.
fn assert_index_agrees(log: &RaftLog) {
    for item in log.latest.iter() {
        let (key, value) = item.into_inner().unwrap();
        let expected = seq_of_latest_value(&value).unwrap();
        let indexed = log
            .get_version(&key)
            .unwrap()
            .unwrap_or_else(|| panic!("no index row for {:?}, which `latest` has", key));
        assert_eq!(
            indexed, expected,
            "index disagrees with `latest` for {key:?}"
        );
    }

    for item in log.latest_seq.iter() {
        let key = item.key().unwrap();
        assert!(
            log.latest.contains_key(&key).unwrap(),
            "index has a row for {key:?}, which `latest` does not"
        );
    }
}

#[test]
fn get_version_reports_what_get_latest_reports() {
    let (log, path) = open_temp();
    log.append(1, b"a", Some(b"1")).unwrap();
    let last = log.append(1, b"b", Some(b"2")).unwrap();
    log.apply_committed(last).unwrap();

    for key in [b"a".as_slice(), b"b".as_slice()] {
        assert_eq!(
            log.get_version(key).unwrap(),
            log.get_latest(key).unwrap().map(|(seq, _)| seq)
        );
    }
    assert_eq!(log.get_version(b"missing").unwrap(), None);

    assert_index_agrees(&log);
    cleanup(&path);
}

#[test]
fn get_version_ignores_an_appended_but_uncommitted_write() {
    // The same guarantee `append_does_not_touch_latest` pins, for the
    // index: a write that could still be rolled back has no version.
    let (log, path) = open_temp();
    let seq = log.append(1, b"k", Some(b"v")).unwrap();
    assert_eq!(log.get_version(b"k").unwrap(), None);

    log.apply_committed(seq).unwrap();
    assert_eq!(log.get_version(b"k").unwrap(), Some(seq));

    assert_index_agrees(&log);
    cleanup(&path);
}

#[test]
fn a_committed_delete_removes_the_index_row_too() {
    let (log, path) = open_temp();
    let written = log.append(1, b"k", Some(b"v")).unwrap();
    log.apply_committed(written).unwrap();
    assert_eq!(log.get_version(b"k").unwrap(), Some(written));

    let deleted = log.append(1, b"k", None).unwrap();
    log.apply_committed(deleted).unwrap();

    // Gone from both, not merely from `latest`. A row surviving here
    // would report a version for a key that no longer has a value.
    assert_eq!(log.get_version(b"k").unwrap(), None);
    assert!(!log.latest_seq.contains_key(b"k").unwrap());

    assert_index_agrees(&log);
    cleanup(&path);
}

#[test]
fn the_index_keeps_up_with_an_entry_larger_than_a_write_chunk() {
    // The index doubles the batch operations per mutation, so the
    // chunking boundary is the place it could plausibly come apart.
    let (log, path) = open_temp();

    let n = WRITE_CHUNK + 7;
    let writes: Vec<_> = (0..n)
        .map(|i| Mutation::new(format!("k{i}"), Some(format!("v{i}"))))
        .collect();
    let seq = log.append_writes(1, writes).unwrap();
    log.apply_committed(seq).unwrap();

    // Delete a scattering of them, again across a chunk boundary.
    let deletes: Vec<_> = (0..n)
        .step_by(3)
        .map(|i| Mutation::new(format!("k{i}"), None::<Vec<u8>>))
        .collect();
    let deleted_seq = log.append_writes(1, deletes).unwrap();
    log.apply_committed(deleted_seq).unwrap();

    for i in [0, 1, n / 2, n - 1] {
        let key = format!("k{i}");
        assert_eq!(
            log.get_version(key.as_bytes()).unwrap(),
            log.get_latest(key.as_bytes()).unwrap().map(|(seq, _)| seq),
            "key {i} of {n}"
        );
    }

    assert_index_agrees(&log);
    cleanup(&path);
}

#[test]
fn install_snapshot_replaces_the_index_wholesale() {
    let (source, source_path) = open_temp();
    let last = source.append(1, b"x", Some(b"from-snapshot")).unwrap();
    source.apply_committed(last).unwrap();
    let mut buf = Vec::new();
    source.export_snapshot(&mut buf).unwrap();

    // The target has its own unrelated history, index rows and all.
    let (target, target_path) = open_temp();
    let stale = target.append(1, b"b", Some(b"old")).unwrap();
    target.apply_committed(stale).unwrap();
    assert_eq!(target.get_version(b"b").unwrap(), Some(stale));

    let meta = target.install_snapshot(&mut buf.as_slice()).unwrap();

    assert_eq!(target.get_version(b"b").unwrap(), None);
    assert_eq!(
        target.get_version(b"x").unwrap(),
        Some(meta.last_included_seq)
    );

    assert_index_agrees(&target);
    cleanup(&source_path);
    cleanup(&target_path);
}

#[test]
fn a_store_written_before_the_index_existed_backfills_it_on_open() {
    let (log, path) = open_temp();
    let a_seq = log.append(1, b"a", Some(b"1")).unwrap();
    let b_seq = log.append(1, b"b", Some(b"2")).unwrap();
    log.apply_committed(b_seq).unwrap();

    // What a store written before `latest_seq` existed looks like: rows
    // in `latest`, nothing in the index, and no marker claiming
    // otherwise.
    log.latest_seq.clear().unwrap();
    let mut batch = log.db.batch();
    batch.remove(&log.meta, META_LATEST_SEQ_BUILT);
    batch.commit().unwrap();
    assert_eq!(log.get_version(b"a").unwrap(), None);
    drop(log);

    // Each key comes back at the seq of the entry that wrote it, not at
    // some single watermark -- a rebuild that stamped one seq across the
    // store would pass a weaker assertion than this.
    let reopened = RaftLog::open(&path).expect("reopen");
    assert_eq!(reopened.get_version(b"a").unwrap(), Some(a_seq));
    assert_eq!(reopened.get_version(b"b").unwrap(), Some(b_seq));
    assert_ne!(a_seq, b_seq);
    assert_index_agrees(&reopened);

    cleanup(&path);
}

#[test]
fn an_index_left_stale_by_a_half_finished_install_is_rebuilt() {
    // The other direction of drift, and the one a merge-style rebuild
    // would miss: a row for a key `latest` does not have. A crash
    // between `install_snapshot`'s clears and its final commit leaves
    // exactly this, which is why the marker goes down before the wipe.
    let (log, path) = open_temp();
    let seq = log.append(1, b"real", Some(b"v")).unwrap();
    log.apply_committed(seq).unwrap();

    let mut batch = log.db.batch();
    batch.insert(&log.latest_seq, b"ghost".as_slice(), be_bytes(99));
    batch.remove(&log.meta, META_LATEST_SEQ_BUILT);
    batch.commit().unwrap();
    assert_eq!(log.get_version(b"ghost").unwrap(), Some(99));
    drop(log);

    let reopened = RaftLog::open(&path).expect("reopen");
    assert_eq!(reopened.get_version(b"ghost").unwrap(), None);
    assert_eq!(reopened.get_version(b"real").unwrap(), Some(seq));
    assert_index_agrees(&reopened);

    cleanup(&path);
}

/// The efficiency claim, asserted structurally rather than by timing.
///
/// A value well past fjall's 1 KiB separation threshold lives in a blob
/// file, and reading it back is what `get_version` exists to avoid. The
/// index row is eight bytes, which is too small to hold a copy of that
/// value by any encoding -- so answering from it cannot be reading it.
/// `size_of` reports the stored size without fetching the blob, which is
/// what makes this assertable at all.
#[test]
fn a_version_is_answered_from_a_row_too_small_to_hold_the_value() {
    let (log, path) = open_temp();

    let big = vec![b'x'; 4 * 1024 * 1024];
    let seq = log.append(1, b"big", Some(&big)).unwrap();
    log.apply_committed(seq).unwrap();

    assert!(
        log.latest.size_of(b"big").unwrap().unwrap() as usize >= big.len(),
        "the value should be stored whole in `latest`"
    );
    assert_eq!(log.latest_seq.size_of(b"big").unwrap(), Some(8));
    assert_eq!(log.get_version(b"big").unwrap(), Some(seq));

    cleanup(&path);
}
