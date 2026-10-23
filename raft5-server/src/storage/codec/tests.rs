//! The `versions` row-key encoding, alone.
//!
//! The subtle correctness of the whole multi-version layout lives here, so
//! it is tested without a `RaftLog`, a keyspace or any I/O: these are
//! properties of the bytes.

use super::*;

/// Every row key for `key`, newest first, in keyspace order.
fn rows(key: &[u8], seqs: &[u64]) -> Vec<Vec<u8>> {
    let mut rows: Vec<_> = seqs
        .iter()
        .map(|&seq| row_key(key, seq, Kind::Value))
        .collect();
    rows.sort();
    rows
}

/// The case the naive `key || seq` encoding gets wrong.
///
/// `"ab" || seq` sorts inside the range `["a" || 00.., "a" || ff..]`, so
/// without escaping, a scan of `"a"`'s versions would return `"ab"`'s rows,
/// and `"a"`'s own versions would not even be adjacent. Both halves are
/// asserted, because they fail independently.
#[test]
fn one_keys_rows_never_stray_into_a_longer_keys_range() {
    let a = rows(b"a", &[1, 2, 3]);
    let ab = rows(b"ab", &[1, 2, 3]);

    let start = chain_start(b"a");
    let end = chain_end(b"a");

    for row in &a {
        assert!(
            row.as_slice() >= start.as_slice() && row.as_slice() < end.as_slice(),
            "a row of \"a\" fell outside its own range"
        );
    }
    for row in &ab {
        assert!(
            row.as_slice() < start.as_slice() || row.as_slice() >= end.as_slice(),
            "a row of \"ab\" fell inside \"a\"'s range"
        );
    }

    // And contiguity: sorting all rows of both together leaves "a"'s three in
    // one unbroken run.
    let mut all: Vec<_> = a.iter().chain(ab.iter()).cloned().collect();
    all.sort();
    let positions: Vec<_> = all
        .iter()
        .enumerate()
        .filter(|(_, row)| a.contains(row))
        .map(|(i, _)| i)
        .collect();
    assert_eq!(positions, [0, 1, 2], "\"a\"'s rows are not contiguous");
}

/// Row keys sort by user key first, then by descending seq.
#[test]
fn rows_sort_by_key_then_newest_first() {
    let mut rows = [
        row_key(b"b", 1, Kind::Value),
        row_key(b"a", 1, Kind::Value),
        row_key(b"a", 9, Kind::Value),
        row_key(b"a", 5, Kind::Value),
    ];
    rows.sort();

    let decoded: Vec<_> = rows
        .iter()
        .map(|r| (row_user_key(r).unwrap(), row_seq(r).unwrap()))
        .collect();
    assert_eq!(
        decoded,
        vec![
            (b"a".to_vec(), 9),
            (b"a".to_vec(), 5),
            (b"a".to_vec(), 1),
            (b"b".to_vec(), 1),
        ]
    );
}

/// The ordering property in general, over the awkward cases, not the easy
/// ones.
#[test]
fn row_key_order_matches_key_then_descending_seq() {
    let keys: [&[u8]; 9] = [
        b"",
        b"\x00",
        b"\x00\x00",
        b"\x00\x01",
        b"a",
        b"a\x00",
        b"ab",
        b"b",
        b"\xff",
    ];
    let seqs = [1u64, 2, 255, 256, u64::MAX];

    for ka in keys {
        for &sa in &seqs {
            for kb in keys {
                for &sb in &seqs {
                    let left = row_key(ka, sa, Kind::Value);
                    let right = row_key(kb, sb, Kind::Value);
                    // Seq descends within a key, so the comparison that should match is on
                    // the complement.
                    let want = (ka, !sa).cmp(&(kb, !sb));
                    assert_eq!(left.cmp(&right), want, "({ka:?}, {sa}) vs ({kb:?}, {sb})");
                }
            }
        }
    }
}

#[test]
fn a_row_key_round_trips_through_its_accessors() {
    let keys: [&[u8]; 7] = [
        b"",
        b"\x00",
        b"\x00\x00",
        b"trailing\x00",
        b"\x00leading",
        b"ordinary",
        b"\xff\xfe\xfd",
    ];

    for key in keys {
        for seq in [0u64, 1, 42, u64::MAX] {
            for kind in [Kind::Value, Kind::Tombstone] {
                let row = row_key(key, seq, kind);
                assert_eq!(row_user_key(&row).unwrap(), key, "key {key:?}");
                assert_eq!(row_seq(&row).unwrap(), seq, "key {key:?} seq {seq}");
                assert_eq!(row_kind(&row).unwrap(), kind, "key {key:?}");
                assert_eq!(row_chain(&row).unwrap(), chain_start(key), "key {key:?}");
            }
        }
    }
}

/// `chain_end` must be above every row of its own key and not above the
/// first row of any later key. Otherwise a bounded scan either cuts a key's
/// versions short or runs into the next key.
#[test]
fn chain_end_bounds_exactly_one_key() {
    let keys: [&[u8]; 6] = [b"", b"\x00", b"a", b"a\x00", b"ab", b"b"];

    for key in keys {
        let end = chain_end(key);
        for seq in [0u64, 1, u64::MAX] {
            let row = row_key(key, seq, Kind::Value);
            assert!(row < end, "{key:?} row at {seq} is not below its chain_end");
        }

        for other in keys {
            if other <= key {
                continue;
            }
            for seq in [0u64, 1, u64::MAX] {
                let row = row_key(other, seq, Kind::Value);
                assert!(
                    row >= end,
                    "{other:?} (above {key:?}) sorts below {key:?}'s chain_end"
                );
            }
        }
    }
}

/// The identity that makes "the current value" and "the value as of S" one
/// code path: seeking at `u64::MAX` seeks to the very start of the key's
/// rows.
#[test]
fn seeking_at_max_is_the_head_of_the_chain() {
    for key in [b"".as_slice(), b"k".as_slice(), b"\x00".as_slice()] {
        let mut want = chain_start(key);
        want.extend_from_slice(&[0u8; 8]);
        assert_eq!(seek_as_of(key, u64::MAX), want);
    }
}

/// A seek lands on the newest row at or before `as_of`, including when
/// `as_of` is exactly an existing seq and when it is one below.
#[test]
fn a_seek_selects_the_newest_row_at_or_before_it() {
    let key = b"k";
    let rows = rows(key, &[2, 4, 6]);
    let end = chain_end(key);

    let selected = |as_of: u64| -> Option<u64> {
        let from = seek_as_of(key, as_of);
        rows.iter()
            .find(|row| row.as_slice() >= from.as_slice() && row.as_slice() < end.as_slice())
            .map(|row| row_seq(row).unwrap())
    };

    assert_eq!(selected(1), None, "nothing had been written yet");
    assert_eq!(selected(2), Some(2), "exactly on a version");
    assert_eq!(selected(3), Some(2), "between two versions");
    assert_eq!(selected(5), Some(4));
    assert_eq!(selected(6), Some(6));
    assert_eq!(selected(u64::MAX), Some(6), "the current value");
}

/// A tombstone and a value at the same seq would be two different rows,
/// and the tombstone sorts first, which is why `apply_committed` must never
/// write both. Kept here so the encoding's part of that rule is visible
/// next to the encoding.
#[test]
fn a_tombstone_sorts_before_a_value_at_the_same_seq() {
    let tomb = row_key(b"k", 7, Kind::Tombstone);
    let value = row_key(b"k", 7, Kind::Value);
    assert!(tomb < value);
}

#[test]
fn a_malformed_row_key_is_refused_rather_than_misread() {
    // Too short to hold the fixed-width tail.
    assert!(row_seq(b"short").is_err());
    assert!(row_chain(b"short").is_err());
    assert!(row_user_key(b"short").is_err());

    // A kind byte that is neither of the two kinds.
    let mut row = row_key(b"k", 1, Kind::Value);
    *row.last_mut().unwrap() = 9;
    assert!(row_kind(&row).is_err());

    // A chain with no terminator where one must be.
    let mut row = row_key(b"k", 1, Kind::Value);
    let chain_len = row_chain(&row).unwrap().len();
    row[chain_len - 1] = b'x';
    assert!(row_user_key(&row).is_err());
}
