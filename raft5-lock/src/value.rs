//! What a lock key holds, and how it is written down.
//!
//! One lock is one KV key, and everything a challenger needs to reason
//! about that lock has to be in the value -- there is nowhere else to put
//! it. The format is deliberately plain text so that `raft read <name>`
//! in the client shell shows a human what is going on:
//!
//! ```text
//! v1 200 3f7a1c9e08b4d2610000000000000000 41 alice
//! ```
//!
//! The field that does the real work is the epoch. Takeover is decided by
//! comparing the bytes a challenger watched go stale against the bytes
//! the store holds at the moment of the compare-and-swap (see
//! `acquire`), and a byte comparison is only a sound CAS if a value
//! string can never recur. A fresh random epoch per acquisition is what
//! guarantees that: a holder only ever moves its counter forward, and any
//! new holder arrives with an epoch nobody has used before, so a value
//! that has been replaced can never come back.

use std::time::Duration;

use crate::{MAX_UPDATE_INTERVAL, MIN_UPDATE_INTERVAL};

/// The only format this version writes, and the only one it will touch.
/// A value tagged anything else is left strictly alone -- see
/// [`Value::decode`].
const VERSION: &str = "v1";

/// The number of fields, and so the `splitn` limit: everything after the
/// fourth separator is the user id, spaces and all.
const FIELDS: usize = 5;

/// A 128-bit per-acquisition nonce, rendered as lowercase hex.
///
/// 128 bits because a collision would silently defeat the one mechanism
/// making byte equality a valid compare-and-swap -- two holders sharing
/// an epoch could each mistake the other's value for their own. That is
/// not a failure worth economizing on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Epoch([u8; 16]);

impl Epoch {
    /// A fresh epoch. Uses the process's own entropy sources rather than
    /// `rand`, following `raft5_server::raft::random_jitter_millis`:
    /// `RandomState`'s entire purpose is an unpredictable seed per
    /// instance, so a fresh hasher's initial state is already random
    /// without writing anything through it.
    ///
    /// The low word is additionally mixed with the pid and the clock.
    /// That costs nothing and means the epoch stays unique even between
    /// two processes whose hasher seeds somehow correlate -- which,
    /// unlike election jitter, is a case where a coincidence would cost
    /// correctness rather than a round of latency.
    pub(crate) fn new() -> Self {
        let hi = random_u64();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let lo = random_u64() ^ u64::from(std::process::id()) ^ nanos;

        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&hi.to_be_bytes());
        bytes[8..].copy_from_slice(&lo.to_be_bytes());
        Self(bytes)
    }

    pub(crate) fn to_hex(self) -> String {
        use std::fmt::Write;
        self.0.iter().fold(String::with_capacity(32), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
    }

    /// Parses an epoch that another process wrote. Deliberately accepts
    /// any length from 1 to 32 hex digits, right-aligned: this crate only
    /// ever compares epochs for equality, never interprets them, so a
    /// shorter one written by hand (a test fixture, an operator unsticking
    /// a lock) is perfectly usable.
    fn from_hex(s: &str) -> Option<Self> {
        if s.is_empty() || s.len() > 32 || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
            return None;
        }
        let padded = format!("{s:0>32}");
        let mut bytes = [0u8; 16];
        for (i, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&padded[i * 2..i * 2 + 2], 16).ok()?;
        }
        Some(Self(bytes))
    }
}

/// A lock key's contents, parsed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Value {
    /// How often the holder promises to rewrite this key.
    ///
    /// Published rather than assumed because a challenger derives its
    /// staleness threshold from *this*, not from its own options. Two
    /// processes configured differently would otherwise disagree about
    /// how long "unchanged" has to last before it means "dead", and the
    /// safety margin between a holder stepping down and a challenger
    /// taking over would stop holding.
    pub(crate) update: Duration,
    pub(crate) epoch: Epoch,
    /// The liveness signal: the only field a refresh changes.
    pub(crate) counter: u64,
    pub(crate) user_id: String,
}

/// Why a value could not be read as a lock. The detail reaches
/// [`crate::Error::Malformed`], and from there a human who has to decide
/// what to do about the key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Malformed(pub(crate) String);

impl Value {
    pub(crate) fn new(update: Duration, epoch: Epoch, user_id: &str) -> Self {
        Self {
            update,
            epoch,
            counter: 0,
            user_id: user_id.to_string(),
        }
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        // The trailing separator is always written, even for an empty
        // user id, so that a value always has exactly FIELDS fields and
        // `decode` can treat a shorter one as malformed rather than
        // having to special-case the tail.
        format!(
            "{VERSION} {} {} {} {}",
            self.update.as_millis(),
            self.epoch.to_hex(),
            self.counter,
            self.user_id,
        )
        .into_bytes()
    }

    /// Reads a value another process wrote.
    ///
    /// Every rejection here is load-bearing: a value this function cannot
    /// read is never overwritten (see [`crate::acquire`]), because the
    /// alternative -- treating anything unparseable as up for grabs --
    /// would destroy application data on a key-name collision, and would
    /// have a `v1` and a `v2` node in a rolling upgrade mutually stealing
    /// one lock while each believed it held it. Refusing is what makes
    /// the version tag worth having.
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, Malformed> {
        let text = std::str::from_utf8(bytes).map_err(|_| Malformed("not UTF-8".to_string()))?;

        let fields: Vec<&str> = text.splitn(FIELDS, ' ').collect();
        if fields.len() != FIELDS {
            return Err(Malformed(format!(
                "expected {FIELDS} space-separated fields, found {}",
                fields.len()
            )));
        }
        if fields[0] != VERSION {
            return Err(Malformed(format!(
                "unknown lock format {:?} (this build writes {VERSION})",
                fields[0]
            )));
        }

        let millis: u64 = fields[1]
            .parse()
            .map_err(|_| Malformed(format!("update interval {:?} is not a number", fields[1])))?;
        let update = Duration::from_millis(millis);
        if !(MIN_UPDATE_INTERVAL..=MAX_UPDATE_INTERVAL).contains(&update) {
            return Err(Malformed(format!(
                "update interval {millis}ms is outside the supported range \
                 {}ms..={}ms",
                MIN_UPDATE_INTERVAL.as_millis(),
                MAX_UPDATE_INTERVAL.as_millis(),
            )));
        }

        let epoch = Epoch::from_hex(fields[2])
            .ok_or_else(|| Malformed(format!("epoch {:?} is not 1-32 hex digits", fields[2])))?;

        let counter: u64 = fields[3]
            .parse()
            .map_err(|_| Malformed(format!("counter {:?} is not a number", fields[3])))?;

        Ok(Self {
            update,
            epoch,
            counter,
            user_id: fields[4].to_string(),
        })
    }

    /// The same lock, one refresh on.
    ///
    /// Built from the stored value rather than from a counter the holder
    /// keeps locally, so that a refresh whose response was lost -- the
    /// write committed, the reply did not arrive -- resumes from what
    /// actually landed instead of rewriting the same number forever.
    ///
    /// `wrapping_add` because a panic in a debug build would be a far
    /// worse outcome than a wrapped counter, and the counter wrapping
    /// would take on the order of 10^11 years at the default interval.
    pub(crate) fn next(&self) -> Self {
        Self {
            counter: self.counter.wrapping_add(1),
            ..self.clone()
        }
    }
}

/// A random `u64`, without a `rand` dependency -- see [`Epoch::new`] and
/// `raft5_server::raft::random_jitter_millis`, which does the same thing
/// for election jitter.
pub(crate) fn random_u64() -> u64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    RandomState::new().build_hasher().finish()
}

#[cfg(test)]
mod tests;
