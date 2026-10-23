use super::*;
use crate::storage::checkpoint::{checkpoint_key, encode_record};

fn open_temp() -> (RaftLog, std::path::PathBuf) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let mut path = std::env::temp_dir();
    path.push(format!("raftlog_backup_test_{}", std::process::id()));
    path.push(COUNTER.fetch_add(1, Ordering::Relaxed).to_string());
    let _ = std::fs::remove_dir_all(&path);
    let log = RaftLog::open(&path).expect("open");
    (log, path)
}

fn cleanup(path: &std::path::Path) {
    let _ = std::fs::remove_dir_all(path);
}

const SOURCE: BackupSource<'static> = BackupSource {
    node_id: "n1",
    snapshot_id: b"",
    created_unix_ms: 1234,
};

/// A backup of `log` as of `point`, with every key included.
fn export(log: &RaftLog, point: &ReadPoint) -> (Vec<u8>, BackupExport) {
    let mut out = Vec::new();
    let result = log.export_backup(&mut out, point, &SOURCE, |_| true).unwrap();
    (out, result)
}

fn restore(log: &RaftLog, backup: &[u8]) -> RestoreOutcome {
    log.restore_backup(&mut &backup[..], b"new-config").unwrap()
}

/// A backup built by hand, for records `export_backup` would never write.
fn handmade(as_of: u64, records: &[(&[u8], u64, &[u8])]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(BACKUP_MAGIC);
    body.extend_from_slice(&BACKUP_FORMAT_VERSION.to_be_bytes());
    body.extend_from_slice(&as_of.to_be_bytes());
    body.extend_from_slice(&0u64.to_be_bytes());
    write_field(&mut body, b"n1").unwrap();
    write_field(&mut body, b"").unwrap();
    for (key, seq, value) in records {
        write_field(&mut body, key).unwrap();
        body.extend_from_slice(&seq.to_be_bytes());
        write_field(&mut body, value).unwrap();
    }
    body.extend_from_slice(&END_OF_RECORDS.to_be_bytes());
    let digest = Sha256::digest(&body);
    body.extend_from_slice(&(records.len() as u64).to_be_bytes());
    body.extend_from_slice(&digest);
    body
}

/// Each key's newest value comes across at its own seq; deleted keys and
/// older versions do not. The restored node's log continues after the
/// backup, with the new configuration as its first entry.
#[test]
fn a_backup_round_trips_into_a_new_store() {
    let (source, source_path) = open_temp();
    source.append(1, b"a", Some(b"old")).unwrap();
    let b = source.append(1, b"b", Some(b"2")).unwrap();
    let a = source.append(1, b"a", Some(b"new")).unwrap();
    source.append(1, b"c", Some(b"gone")).unwrap();
    let last = source.append(1, b"c", None).unwrap();
    source.apply_committed(last).unwrap();

    let (backup, result) = export(&source, &ReadPoint::Latest);
    assert_eq!(result, BackupExport::Done { as_of: last, record_count: 2 });

    let (target, target_path) = open_temp();
    match restore(&target, &backup) {
        RestoreOutcome::Restored { as_of, record_count, source_node_id } => {
            assert_eq!((as_of, record_count), (last, 2));
            assert_eq!(source_node_id, "n1");
        }
        other => panic!("expected a restore, got {other:?}"),
    }
    assert_eq!(target.get_latest(b"a").unwrap(), Some((a, b"new".to_vec())));
    assert_eq!(target.get_latest(b"b").unwrap(), Some((b, b"2".to_vec())));
    assert_eq!(target.get_latest(b"c").unwrap(), None);
    assert_eq!(target.version_count(b"a").unwrap(), 1, "one version per key");

    assert_eq!(target.current_config().unwrap(), Some(b"new-config".to_vec()));
    assert_eq!(target.last_entry().unwrap(), Some((last + 1, 1)));
    assert_eq!(target.append(1, b"d", Some(b"4")).unwrap(), last + 2);
    assert_eq!(target.bootstrap_refusal().unwrap(), Some(BootstrapRefusal::AlreadyInitialized));

    cleanup(&source_path);
    cleanup(&target_path);
}

/// A backup at a checkpoint shows the store as it was then, and leaves
/// out whatever the caller's filter rejects.
#[test]
fn a_backup_at_a_checkpoint_reads_as_of_it() {
    let (log, path) = open_temp();
    let old = log.append(1, b"k", Some(b"old")).unwrap();
    let cp = log
        .append(1, &checkpoint_key(b"cp"), Some(&encode_record(0).unwrap()))
        .unwrap();
    let new = log.append(1, b"k", Some(b"new")).unwrap();
    log.append(1, b"later", Some(b"x")).unwrap();
    log.apply_committed(new + 1).unwrap();

    let mut backup = Vec::new();
    let result = log
        .export_backup(&mut backup, &ReadPoint::Checkpoint(b"cp".to_vec()), &SOURCE, |key| {
            !key.starts_with(b"\0snap/")
        })
        .unwrap();
    assert_eq!(result, BackupExport::Done { as_of: cp, record_count: 1 });

    let (target, target_path) = open_temp();
    assert!(matches!(restore(&target, &backup), RestoreOutcome::Restored { as_of, .. } if as_of == cp));
    assert_eq!(target.get_latest(b"k").unwrap(), Some((old, b"old".to_vec())));
    assert_eq!(target.get_latest(b"later").unwrap(), None);
    assert_eq!(target.checkpoint_seq(b"cp").unwrap(), None);

    let mut nothing = Vec::new();
    let missing = log
        .export_backup(&mut nothing, &ReadPoint::Checkpoint(b"nope".to_vec()), &SOURCE, |_| true)
        .unwrap();
    assert_eq!(missing, BackupExport::NoSuchCheckpoint);
    assert!(nothing.is_empty());

    cleanup(&path);
    cleanup(&target_path);
}

/// Every way a backup can be damaged is refused, and leaves the store
/// untouched, so the restore can be retried.
#[test]
fn a_damaged_backup_is_refused_and_leaves_the_store_untouched() {
    let (source, source_path) = open_temp();
    let last = source.append(1, b"k", Some(b"value")).unwrap();
    source.apply_committed(last).unwrap();
    let (good, _) = export(&source, &ReadPoint::Latest);

    let mut flipped = good.clone();
    let middle = flipped.len() / 2;
    flipped[middle] ^= 0xff;
    let truncated = good[..good.len() - 10].to_vec();
    let mut extra = good.clone();
    extra.push(0);
    let mut magic = good.clone();
    magic[0] = b'X';
    let out_of_order = handmade(5, &[(b"b", 1, b"1"), (b"a", 2, b"2")]);
    let too_new = handmade(5, &[(b"a", 9, b"1")]);

    for (what, backup) in [
        ("a flipped byte", flipped),
        ("a truncated file", truncated),
        ("trailing data", extra),
        ("a bad magic", magic),
        ("keys out of order", out_of_order),
        ("a seq past as_of", too_new),
    ] {
        let (target, target_path) = open_temp();
        assert!(
            matches!(restore(&target, &backup), RestoreOutcome::BadBackup(_)),
            "{what} should be refused"
        );
        assert_eq!(target.bootstrap_refusal().unwrap(), None, "{what} left data behind");
        assert!(matches!(restore(&target, &good), RestoreOutcome::Restored { .. }), "{what}: retry");
        cleanup(&target_path);
    }
    cleanup(&source_path);
}

/// Only an untouched store may be restored into. A store with a log is in
/// a cluster; a store with a leftover restore marker had a restore
/// interrupted, and plain bootstrap must refuse it too.
#[test]
fn a_restore_needs_an_untouched_store() {
    let (log, path) = open_temp();
    log.append(1, b"k", Some(b"v")).unwrap();
    let backup = handmade(0, &[]);
    assert_eq!(
        restore(&log, &backup),
        RestoreOutcome::Refused(BootstrapRefusal::AlreadyInitialized)
    );
    cleanup(&path);

    let (log, path) = open_temp();
    log.meta.insert(META_RESTORE_IN_PROGRESS, [1u8]).unwrap();
    assert!(matches!(log.bootstrap_refusal().unwrap(), Some(BootstrapRefusal::Dirty(_))));
    assert!(matches!(
        restore(&log, &backup),
        RestoreOutcome::Refused(BootstrapRefusal::Dirty(_))
    ));
    cleanup(&path);
}

/// A backup of an empty store restores like a plain bootstrap: the
/// configuration is the first entry.
#[test]
fn an_empty_backup_restores_like_a_plain_bootstrap() {
    let (source, source_path) = open_temp();
    let (backup, result) = export(&source, &ReadPoint::Latest);
    assert_eq!(result, BackupExport::Done { as_of: 0, record_count: 0 });

    let (target, target_path) = open_temp();
    assert!(matches!(restore(&target, &backup), RestoreOutcome::Restored { as_of: 0, .. }));
    assert_eq!(target.last_entry().unwrap(), Some((1, 1)));
    assert_eq!(target.current_config().unwrap(), Some(b"new-config".to_vec()));

    cleanup(&source_path);
    cleanup(&target_path);
}
