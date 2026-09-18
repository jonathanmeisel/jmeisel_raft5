use std::collections::HashSet;

use super::*;

fn sample() -> Value {
    Value::new(Duration::from_millis(200), Epoch::new(), "alice")
}

#[test]
fn a_value_round_trips() {
    let value = sample();
    let decoded = Value::decode(&value.encode()).expect("a value we wrote must be readable");
    assert_eq!(decoded, value);
}

#[test]
fn a_user_id_may_contain_spaces() {
    let mut value = sample();
    value.user_id = "host-7 worker 2".to_string();
    let decoded = Value::decode(&value.encode()).unwrap();
    assert_eq!(decoded.user_id, "host-7 worker 2");
}

/// The trailing separator is always written, so the field count does not
/// depend on the user id being non-empty.
#[test]
fn an_empty_user_id_round_trips() {
    let mut value = sample();
    value.user_id = String::new();
    let decoded = Value::decode(&value.encode()).unwrap();
    assert_eq!(decoded.user_id, "");
}

/// A key holding application data must be refused, not stolen.
#[test]
fn a_value_that_is_not_a_lock_is_rejected() {
    for bytes in [
        &b"hello"[..],
        b"",
        b"v1 200 abcd 1", // four fields
        b"{\"json\": true}",
        &[0xff, 0xfe][..], // not UTF-8
    ] {
        assert!(
            Value::decode(bytes).is_err(),
            "{:?} must not decode as a lock",
            String::from_utf8_lossy(bytes)
        );
    }
}

/// The reason the version tag is field zero: a `v1` parser must refuse a
/// `v2` value rather than treat it as garbage it may overwrite.
#[test]
fn an_unknown_version_is_rejected() {
    let value = sample();
    let bumped = String::from_utf8(value.encode())
        .unwrap()
        .replacen("v1 ", "v2 ", 1);
    assert!(Value::decode(bumped.as_bytes()).is_err());
}

#[test]
fn a_nonsense_epoch_or_counter_is_rejected() {
    for bad in [
        "v1 200 zzzz 1 alice",                              // not hex
        "v1 200  1 alice",                                  // empty epoch
        "v1 200 000000000000000000000000000000000 1 alice", // 33 digits
        "v1 200 abcd notanumber alice",                     // counter
    ] {
        assert!(
            Value::decode(bad.as_bytes()).is_err(),
            "{bad:?} must be rejected"
        );
    }
}

#[test]
fn an_out_of_range_update_ms_is_rejected() {
    for bad in ["v1 0 abcd 1 alice", "v1 999999999 abcd 1 alice"] {
        assert!(
            Value::decode(bad.as_bytes()).is_err(),
            "{bad:?} must be rejected"
        );
    }
}

/// A hand-written short epoch stays usable: this crate only ever compares
/// epochs, so an operator unsticking a lock by hand should not have to
/// type 32 digits.
#[test]
fn a_short_epoch_is_accepted_and_compares_equal() {
    let a = Value::decode(b"v1 200 ab 1 alice").unwrap();
    let b = Value::decode(b"v1 200 00000000000000000000000000000ab 2 alice").unwrap();
    assert_eq!(a.epoch, b.epoch);
}

/// The assumption the whole takeover compare-and-swap rests on: if two
/// acquisitions could share an epoch, each could mistake the other's
/// value for its own.
#[test]
fn epochs_do_not_repeat() {
    let epochs: HashSet<[u8; 16]> = (0..10_000).map(|_| Epoch::new().0).collect();
    assert_eq!(epochs.len(), 10_000, "an epoch repeated within one process");
}

#[test]
fn refreshing_only_moves_the_counter() {
    let value = sample();
    let next = value.next();
    assert_eq!(next.counter, value.counter + 1);
    assert_eq!(next.epoch, value.epoch);
    assert_eq!(next.update, value.update);
    assert_eq!(next.user_id, value.user_id);
}
