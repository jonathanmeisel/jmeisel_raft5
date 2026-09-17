use super::snapshot::SNAPSHOT_MAGIC;
use super::*;

fn open_temp() -> (RaftLog, std::path::PathBuf) {
    let mut path = std::env::temp_dir();
    path.push(format!("raftlog_test_{}", std::process::id()));
    path.push(format!("{:x}", rand_suffix()));
    let log = RaftLog::open(&path).expect("open");
    (log, path)
}

fn rand_suffix() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
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
    assert_eq!(entries[0].value, None);

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
    assert_eq!(stored[1].kind, EntryKind::NoOp);
    assert_eq!(stored[1].term, 2);
    assert!(stored[1].key.is_empty());
    assert_eq!(stored[1].value, None);
    // The neighbours still decode as ordinary writes.
    assert_eq!(stored[0].kind, EntryKind::Put);
    assert_eq!(stored[2].kind, EntryKind::Put);
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
    assert_eq!(entries[0].key, b"b");
    assert_eq!(entries[1].seq, 3);
    assert_eq!(entries[1].term, 2);
    assert_eq!(entries[1].key, b"c");
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
