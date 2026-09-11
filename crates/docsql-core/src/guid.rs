//! Time-ordered GUID generation (UUIDv7, RFC 9562) for auto-generated
//! primary keys.
//!
//! Layout: 48-bit big-endian Unix-millisecond timestamp, version 7, a
//! 12-bit intra-millisecond counter (randomly reseeded each new millisecond,
//! incremented otherwise), and 62 random bits. The canonical lowercase
//! string form therefore sorts lexicographically in generation order, which
//! keeps B+ tree index inserts append-heavy, and ids generated on different
//! nodes collide only with random-bit probability.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

/// Last emitted state packed as `(unix_ms << 12) | seq`. Compare-and-swap
/// makes values strictly increasing even under clock regression (the
/// timestamp never moves backwards; the counter absorbs the slack).
static LAST_STATE: AtomicU64 = AtomicU64::new(0);

/// Extra entropy for the random half: a process-wide tally of ids issued.
static ISSUE_COUNT: AtomicU64 = AtomicU64::new(0);

fn unix_ms() -> u64 {
    crate::now_ms()
}

/// 64 bits of randomness without a rand dependency: two independently
/// seeded SipHash instances (std randomizes `RandomState` seeds per
/// process/instance) mixed with the issue counter.
fn rand64() -> u64 {
    let mut a = RandomState::new().build_hasher();
    a.write_u64(ISSUE_COUNT.fetch_add(1, Ordering::Relaxed));
    let mut b = RandomState::new().build_hasher();
    b.write(&ISSUE_COUNT.load(Ordering::Relaxed).to_le_bytes());
    a.finish() ^ b.finish().rotate_left(29)
}

/// Reserve the next (timestamp, counter) pair, strictly increasing.
fn next_time_seq() -> (u64, u16) {
    loop {
        let cur = LAST_STATE.load(Ordering::Relaxed);
        let (cur_ms, cur_seq) = (cur >> 12, (cur & 0x0fff) as u16);
        let now = unix_ms();
        let next = if now > cur_ms {
            // New millisecond: reseed the counter randomly.
            (now << 12) | (rand64() & 0x0fff)
        } else {
            // Same millisecond (or regressed clock): bump the counter,
            // spilling into the next millisecond on overflow.
            let seq = cur_seq.wrapping_add(1) & 0x0fff;
            let ms = if seq == 0 { cur_ms + 1 } else { cur_ms };
            (ms << 12) | seq as u64
        };
        if LAST_STATE
            .compare_exchange_weak(cur, next, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            return (next >> 12, (next & 0x0fff) as u16);
        }
    }
}

/// Generate one time-ordered GUID in canonical lowercase form
/// (`xxxxxxxx-xxxx-7xxx-<8/9/a/b>xxx-xxxxxxxxxxxx`).
pub fn uuidv7() -> String {
    let (ms, seq) = next_time_seq();
    let rnd = rand64().to_be_bytes();
    let mut b = [0u8; 16];
    b[0] = (ms >> 40) as u8;
    b[1] = (ms >> 32) as u8;
    b[2] = (ms >> 24) as u8;
    b[3] = (ms >> 16) as u8;
    b[4] = (ms >> 8) as u8;
    b[5] = ms as u8;
    b[6] = 0x70 | ((seq >> 8) as u8 & 0x0f);
    b[7] = (seq & 0xff) as u8;
    b[8] = 0x80 | (rnd[0] & 0x3f);
    b[9..16].copy_from_slice(&rnd[1..8]);

    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(36);
    for (i, &byte) in b.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            s.push('-');
        }
        s.push(HEX[(byte >> 4) as usize] as char);
        s.push(HEX[(byte & 0x0f) as usize] as char);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn format_is_canonical_uuidv7() {
        let id = uuidv7();
        assert_eq!(id.len(), 36);
        let parts: Vec<&str> = id.split('-').collect();
        assert_eq!(
            parts.iter().map(|p| p.len()).collect::<Vec<_>>(),
            vec![8, 4, 4, 4, 12]
        );
        assert!(id.chars().all(|c| c.is_ascii_hexdigit() || c == '-'));
        assert_eq!(&id[14..15], "7");
        assert!(matches!(&id[19..20], "8" | "9" | "a" | "b"));
        // Timestamp prefix decodes back to a plausible Unix time.
        let ms = u64::from_str_radix(&id[0..13].replace('-', ""), 16).unwrap();
        assert!(ms > 1_600_000_000_000);
    }

    #[test]
    fn values_are_unique_and_monotonic() {
        let mut last = String::new();
        let mut seen = BTreeSet::new();
        for _ in 0..10_000 {
            let id = uuidv7();
            assert!(seen.insert(id.clone()), "duplicate guid {id}");
            if !last.is_empty() {
                assert!(id > last, "{id} not after {last}");
            }
            last = id;
        }
    }
}
