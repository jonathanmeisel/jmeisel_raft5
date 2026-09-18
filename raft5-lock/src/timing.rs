//! Every threshold in the crate, and the chain between them.
//!
//! All four are fixed multiples of one number -- the holder's
//! `update_interval`, written `U` below -- so a caller tunes one knob and
//! the relationships that make the lock safe are preserved by
//! construction rather than by arithmetic done at each call site.
//!
//! At the default `U` of 200ms:
//!
//! | symbol | value    | meaning                                   |
//! |--------|----------|-------------------------------------------|
//! | `U`    | 200ms    | how often the holder rewrites the key     |
//! | `T`    | 400ms    | budget for one refresh attempt            |
//! | `S`    | 1000ms   | holder declares itself lost               |
//! | poll   | 600-800ms| challenger's gap between looks, fuzzed    |
//! | `K`    | 1600ms   | unchanged this long means the holder died |
//!
//! # Why the chain is safe
//!
//! Anchor the holder's lease at `a`, the instant *immediately before* it
//! issued its last successful refresh; that write commits at some
//! `c >= a`. The holder stops considering itself the holder at `a + S`,
//! which is no later than `c + S`.
//!
//! A challenger's first observation of the unchanged value linearizes at
//! some `l1 >= c` -- it saw the bytes that commit produced -- and its
//! takeover write commits no earlier than `l1 + K`. That write is
//! conditional on a read that still showed those same bytes, so nothing
//! of the holder's committed in between.
//!
//! So the holder has given up by `c + S` and the challenger cannot have
//! taken over before `c + K`. Safety is exactly `S < K`, and the gap
//! `K - S` is the margin absorbing the difference between two processes
//! measuring the same interval with their own clocks.
//!
//! Note what is *not* in that condition: `T`, the per-attempt timeout.
//! Keeping it out is why the anchor is the instant before the attempt
//! rather than the instant it returned -- see [`stepdown`].
//!
//! # What is assumed
//!
//! Clock *rate*, never clock *time*. Everything here is an `Instant`
//! delta measured within one process; no two processes ever compare
//! timestamps, so nothing depends on NTP or on any agreement about what
//! time it is. What the margin buys is tolerance for the holder's and the
//! challenger's monotonic clocks advancing at different rates: at the
//! default multipliers, `K - S` is `3U` against an `S` of `5U`, so the
//! two may differ by around 37% over the observation window before the
//! margin is spent.

use std::time::Duration;

use crate::value::random_u64;

/// `T`: how long one refresh attempt may take before it is abandoned and
/// left for the next tick.
///
/// Two intervals covers a whole attempt-0 election window in the server
/// (`raft5_server::raft` jitters the first campaign over
/// `[200ms, 400ms)`), so a refresh issued just as a leader dies usually
/// still lands within its own attempt. It also has to sit far below
/// `raft5_client`'s internal 10s deadline, or the client's retry loop
/// rather than this one would decide when a refresh gives up.
const UPDATE_TIMEOUT_MULT: u32 = 2;

/// `S`: how long the holder goes without a successful refresh before it
/// gives up the lock.
///
/// This is the number that decides how much cluster turbulence a holder
/// survives. One second absorbs a 200-400ms election, plus the attempt
/// that failed discovering it, plus a retry. Tighter, and every routine
/// raft leader change would drop every lock in the cluster -- which is
/// why this is 5x rather than the 3x the ordering alone would allow.
const STEPDOWN_MULT: u32 = 5;

/// The challenger's gap between looks, as `[POLL_MIN_MULT, POLL_MAX_MULT)`.
const POLL_MIN_MULT: u32 = 3;
const POLL_MAX_MULT: u32 = 4;

/// `K`: a value unchanged for this long means its holder is gone.
///
/// The margin over `S` is `3U`, which is what the clock-rate argument in
/// the module doc spends.
const TAKEOVER_MULT: u32 = 8;

// The chain, checked where it cannot rot: `U < T < S < K`.
const _: () = assert!(
    UPDATE_TIMEOUT_MULT > 1,
    "an attempt gets more than one interval"
);
const _: () = assert!(
    UPDATE_TIMEOUT_MULT < STEPDOWN_MULT,
    "an attempt must fit inside the deadline it is racing"
);
const _: () = assert!(
    STEPDOWN_MULT < TAKEOVER_MULT,
    "the holder must give up before a challenger may take over -- this is the safety property"
);
const _: () = assert!(
    POLL_MIN_MULT < POLL_MAX_MULT,
    "the fuzz window must be non-empty"
);
const _: () = assert!(
    POLL_MAX_MULT <= TAKEOVER_MULT,
    "at least two polls have to land inside every staleness window"
);

/// `T`, for an interval of `update`.
pub(crate) fn attempt_timeout(update: Duration) -> Duration {
    update * UPDATE_TIMEOUT_MULT
}

/// `S`, for an interval of `update`.
///
/// Measured from the instant *before* the holder's last successful
/// refresh was issued, never from when it returned. Anchoring at the
/// return would let the lease run to `commit + T + S` and the safety
/// condition would become `T + S < K` instead of `S < K`. Anchoring at
/// the start only ever shortens our own lease, which costs nothing and
/// keeps the attempt timeout out of the safety argument entirely.
pub(crate) fn stepdown(update: Duration) -> Duration {
    update * STEPDOWN_MULT
}

/// `K`, for an interval of `update`.
///
/// Callers pass the interval *the observed value published*, not their
/// own configured one: how long a holder may go quiet is the holder's
/// promise to keep, and only the holder knows what it promised.
pub(crate) fn takeover(update: Duration) -> Duration {
    update * TAKEOVER_MULT
}

/// How long a challenger waits before looking again: uniform over
/// `[3U, 4U)`.
///
/// The fuzz is not a correctness device, and should not be mistaken for
/// one -- "this value has not changed in `K`" is a claim about a real
/// interval, and holds whatever phase the polls happen to be in. It is
/// here for two practical reasons. Several challengers on one lock would
/// otherwise poll in lockstep and fire their takeover writes in the same
/// instant, where all but one must lose and go round again; spreading
/// them out means the losers usually just *observe* the winner on their
/// next look. And every poll is a linearizable read, which costs the
/// leader a read-quorum round, so decorrelating them smooths that load.
pub(crate) fn poll_gap(update: Duration) -> Duration {
    let window = update * (POLL_MAX_MULT - POLL_MIN_MULT);
    let jitter = match u64::try_from(window.as_millis()) {
        Ok(ms) if ms > 0 => random_u64() % ms,
        _ => 0,
    };
    update * POLL_MIN_MULT + Duration::from_millis(jitter)
}

#[cfg(test)]
mod tests;
