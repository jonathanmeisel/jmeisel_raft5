use std::collections::HashSet;

use super::*;
use crate::{MAX_UPDATE_INTERVAL, MIN_UPDATE_INTERVAL};

fn intervals() -> [Duration; 4] {
    [
        MIN_UPDATE_INTERVAL,
        Duration::from_millis(200),
        Duration::from_secs(1),
        MAX_UPDATE_INTERVAL,
    ]
}

/// `U < T < S < K` for every interval a caller may legally ask for, plus
/// the margin the clock-rate argument spends.
#[test]
fn the_threshold_chain_holds_for_every_legal_interval() {
    for u in intervals() {
        let (t, s, k) = (attempt_timeout(u), stepdown(u), takeover(u));
        assert!(u < t, "{u:?}: interval must be under the attempt budget");
        assert!(t < s, "{u:?}: an attempt must fit inside the deadline");
        assert!(
            s < k,
            "{u:?}: the holder must give up before a challenger takes over"
        );
        assert!(
            k - s >= u * 2,
            "{u:?}: the safety margin must be at least two intervals"
        );
    }
}

#[test]
fn the_poll_gap_stays_inside_its_window() {
    for u in intervals() {
        let (low, high) = (u * POLL_MIN_MULT, u * POLL_MAX_MULT);
        for _ in 0..1_000 {
            let gap = poll_gap(u);
            assert!(
                gap >= low && gap < high,
                "{u:?}: {gap:?} outside [{low:?}, {high:?})"
            );
        }
    }
}

/// Catches a `RandomState` misuse that silently returns one value -- the
/// fuzz would still be "in range" but would no longer decorrelate
/// anything.
#[test]
fn the_poll_gap_is_actually_fuzzed() {
    let gaps: HashSet<Duration> = (0..1_000)
        .map(|_| poll_gap(Duration::from_millis(200)))
        .collect();
    assert!(
        gaps.len() > 1,
        "the poll gap never varied across 1000 samples"
    );
}

/// Guarantees at least two looks inside every staleness window, so a
/// challenger cannot step over one.
#[test]
fn a_poll_gap_never_exceeds_the_takeover_threshold() {
    for u in intervals() {
        for _ in 0..1_000 {
            assert!(poll_gap(u) <= takeover(u));
        }
    }
}
