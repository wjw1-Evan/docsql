//! Transport encryption for the DSQ1 wire protocol.
//!
//! Pre-shared-key mode (`DOCSQL_KEY`, 32 bytes hex): every frame payload is
//! sealed with AES-256-GCM using a structured nonce — a per-process random
//! 4-byte prefix plus a process-wide monotonic 64-bit counter —
//! `payload' = nonce[12] || ciphertext+tag[16]`. The frame header (type,
//! length, flags) stays plaintext so routing stays cheap; everything
//! application-visible (SQL text, KV args, AUTH token, rows) is encrypted.
//! The header fields are bound into the GCM tag as associated data: a MITM
//! flipping a flag (clearing FLAG_ENCRYPTED, setting FLAG_REPLICATION)
//! invalidates the frame instead of re-routing it.
//!
//! Connection binding: the server opens every keyed connection with a
//! random 16-byte challenge (RESP_HELLO) that both directions fold into
//! the GCM associated data — a sealed frame replayed onto a DIFFERENT
//! connection fails the tag, so recording a whole session no longer
//! resurrects it (the per-connection ReplayGuard still catches in-stream
//! reordering and single-frame replays).
//!
//! Nonce construction: a fully random 96-bit nonce per frame carries the
//! NIST SP 800-38D birthday bound of 2^32 encryptions per key — a busy
//! fan-out cluster (one short-lived connection per write per target)
//! passes that in days. The structured form is an 8-byte per-process
//! random prefix plus a per-process 32-bit counter: the prefix collision
//! space is 2^64 (birthday bound ~2^32 PROCESS births — beyond any
//! realistic restart count), and inside a process the counter never
//! repeats; past 2^32 seals the process refuses to seal further (loud
//! restart-the-node error) instead of wrapping into reuse. The structure
//! also gives receivers a replay guard ([`ReplayGuard`]): frames from one
//! peer share its prefix and carry strictly increasing counters (TCP
//! ordering), so a captured frame replayed into the stream fails the
//! monotonic check even though it would decrypt fine.

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};

pub type TransportKey = [u8; 32];

pub const FLAG_ENCRYPTED: u16 = 0x0004;

/// Parse a 64-char hex string into a 32-byte transport key.
pub fn parse_key_hex(s: &str) -> Result<TransportKey, String> {
    let bytes = hex_decode(s)?;
    let n = bytes.len();
    bytes
        .try_into()
        .map_err(|_| format!("DOCSQL_KEY must be 32 bytes (64 hex chars), got {n} bytes"))
}

/// Constant-time equality for secrets (AUTH tokens). The length check only
/// leaks the length; byte comparison short-circuits nowhere. Shared with
/// the credential-hashing path in core's `kdf` module.
pub use docsql_core::kdf::constant_time_eq;

fn hex_decode(s: &str) -> Result<Vec<u8>, String> {
    let s = s.trim();
    // Strict ASCII hex, byte-wise: the old char-index slicing panicked on a
    // multibyte character whose byte length is even (`&s[0..2]` inside a
    // code point), and `from_str_radix` quietly accepted `+`/`-` signs —
    // both turning a hostile env into a crash (or a key that disagrees
    // with the .NET client's strict parser) instead of a clean refusal.
    if !s.is_ascii() {
        return Err("hex must be ASCII".into());
    }
    if !s.len().is_multiple_of(2) {
        return Err("odd-length hex".into());
    }
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len() / 2);
    for pair in b.as_chunks::<2>().0 {
        let hi = (pair[0] as char).to_digit(16).ok_or("bad hex digit")?;
        let lo = (pair[1] as char).to_digit(16).ok_or("bad hex digit")?;
        out.push(((hi << 4) | lo) as u8);
    }
    Ok(out)
}

/// Associated data for a frame's tag: the transmitted header fields a
/// receiver routes on (type + flags, little-endian like the wire form)
/// followed by the connection challenge (see RESP_HELLO). Binding the
/// per-connection challenge is what kills cross-connection replay: a
/// frame captured on one TCP connection carries a different AAD on any
/// other and fails the GCM tag before its payload is ever trusted.
pub fn aad(frame_type: u16, flags: u16, conn: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + conn.len());
    out.extend_from_slice(&frame_type.to_le_bytes());
    out.extend_from_slice(&flags.to_le_bytes());
    out.extend_from_slice(conn);
    out
}

/// Per-process nonce material: one random 8-byte prefix for the process
/// lifetime, one shared strictly-monotonic 32-bit counter. A colliding
/// prefix needs two of ~2^64 draws — the birthday bound lands on process
/// births, not frames; the counter cap below keeps a single process from
/// ever wrapping into nonce reuse.
static NONCE_PREFIX: std::sync::OnceLock<[u8; 8]> = std::sync::OnceLock::new();
static NONCE_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

const NONCE_COUNTER_CAP: u64 = u32::MAX as u64;

fn next_nonce() -> Result<[u8; 12], String> {
    let counter = NONCE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    // Hard cap: wrapping the 32-bit counter field would reuse exact nonces
    // (keystream XOR leak). Past the cap the process must be restarted —
    // fresh prefix, fresh counter.
    if counter > NONCE_COUNTER_CAP {
        return Err(
            "transport nonce budget exhausted (>2^32 sealed frames from this \
             process); restart the node"
                .into(),
        );
    }
    let prefix = NONCE_PREFIX.get_or_init(rand::random);
    let mut nonce = [0u8; 12];
    nonce[..8].copy_from_slice(prefix);
    nonce[8..].copy_from_slice(&(counter as u32).to_le_bytes());
    Ok(nonce)
}

/// Inbound replay guard for one connection direction. Sealed frames from
/// one peer share that peer's process prefix and carry strictly increasing
/// counters (TCP delivers in order), so anything non-increasing — or a
/// prefix change mid-connection — is a replay or an injection and is
/// rejected before decryption.
#[derive(Default)]
pub struct ReplayGuard {
    prefix: Option<[u8; 8]>,
    last: u64,
}

impl ReplayGuard {
    /// Validate the nonce of an inbound sealed payload (12-byte prefix).
    pub fn check(&mut self, sealed: &[u8]) -> Result<(), String> {
        if sealed.len() < 12 {
            return Err("sealed payload too short".into());
        }
        let mut prefix = [0u8; 8];
        prefix.copy_from_slice(&sealed[..8]);
        let counter = u32::from_le_bytes(sealed[8..12].try_into().expect("4-byte slice")) as u64;
        match self.prefix {
            None => {
                self.prefix = Some(prefix);
                self.last = counter;
                Ok(())
            }
            Some(p) if p == prefix => {
                if counter > self.last {
                    self.last = counter;
                    Ok(())
                } else {
                    Err("replayed or reordered encrypted frame rejected".into())
                }
            }
            Some(_) => Err("encrypted frame nonce prefix changed mid-connection".into()),
        }
    }
}

/// Seal one frame payload: nonce ‖ ciphertext+tag. `flags` must be the
/// flags as transmitted (i.e. with [`FLAG_ENCRYPTED`] already set). Fails
/// only when the process's nonce budget is exhausted (>2^32 seals): the
/// caller must not send the frame — restart the node.
pub fn seal(
    key: &TransportKey,
    frame_type: u16,
    flags: u16,
    plaintext: &[u8],
    conn: &[u8],
) -> Result<Vec<u8>, String> {
    let nonce = next_nonce()?;
    let cipher = Aes256Gcm::new(key.into());
    let ct = cipher
        .encrypt(
            Nonce::from_slice(&nonce),
            aes_gcm::aead::Payload {
                msg: plaintext,
                aad: &aad(frame_type, flags, conn),
            },
        )
        .expect("aes-gcm encrypt cannot fail with valid key/nonce");
    let mut out = Vec::with_capacity(12 + ct.len());
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ct);
    Ok(out)
}

/// Open a sealed frame payload. Fails (wrong key / tampered header or
/// payload) with a message.
pub fn open(
    key: &TransportKey,
    frame_type: u16,
    flags: u16,
    sealed: &[u8],
    conn: &[u8],
) -> Result<Vec<u8>, String> {
    if sealed.len() < 12 + 16 {
        return Err("sealed payload too short".into());
    }
    let (nonce, ct) = sealed.split_at(12);
    let cipher = Aes256Gcm::new(key.into());
    cipher
        .decrypt(
            Nonce::from_slice(nonce),
            aes_gcm::aead::Payload {
                msg: ct,
                aad: &aad(frame_type, flags, conn),
            },
        )
        .map_err(|_| "decrypt failed (wrong key or tampered frame)".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY_HEX: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

    fn key() -> TransportKey {
        parse_key_hex(KEY_HEX).unwrap()
    }

    #[test]
    fn parse_key_hex_roundtrip() {
        let k = key();
        assert_eq!(k[0], 0);
        assert_eq!(k[31], 0x1f);
        // surrounding whitespace is trimmed
        assert_eq!(parse_key_hex(&format!(" {KEY_HEX}\n")).unwrap(), k);
    }

    #[test]
    fn parse_key_hex_rejects_bad_input() {
        assert!(parse_key_hex("").is_err());
        assert!(parse_key_hex("zz").is_err());
        // odd length
        assert!(parse_key_hex("abc").is_err());
        // wrong size (16 bytes)
        assert!(parse_key_hex("000102030405060708090a0b0c0d0e0f").is_err());
        let e = parse_key_hex("00").unwrap_err();
        assert!(e.contains("32 bytes"), "{e}");
    }

    #[test]
    fn seal_open_roundtrip() {
        let k = key();
        let pt = b"SELECT 1 FROM t";
        let sealed = seal(&k, 0x0102, 0x0004, pt, b"conn").unwrap();
        // nonce(12) + tag(16) + plaintext
        assert_eq!(sealed.len(), 12 + 16 + pt.len());
        assert_ne!(&sealed[12..], &pt[..]);
        assert_eq!(
            open(&k, 0x0102, 0x0004, &sealed, b"conn").unwrap(),
            pt.to_vec()
        );
        // fresh nonce each seal
        assert_ne!(
            seal(&k, 0x0102, 0x0004, pt, b"conn").unwrap(),
            seal(&k, 0x0102, 0x0004, pt, b"conn").unwrap()
        );
    }

    #[test]
    fn replay_guard_rejects_replayed_and_reordered_frames() {
        let k = key();
        let first = seal(&k, 0x0102, 0x0004, b"one", b"conn").unwrap();
        let second = seal(&k, 0x0102, 0x0004, b"two", b"conn").unwrap();
        let mut g = ReplayGuard::default();
        assert!(g.check(&first).is_ok());
        assert!(g.check(&second).is_ok());
        // replay of the first frame: same (or lower) counter
        assert!(g.check(&first).is_err());
        assert!(g.check(&second).is_err());
        // prefix change mid-connection is an injection
        let mut foreign = first.clone();
        foreign[0] ^= 0xff;
        assert!(g.check(&foreign).is_err());
        assert!(g.check(b"short").is_err());
    }

    #[test]
    fn frames_bound_to_one_connection_challenge_do_not_open_on_another() {
        let k = key();
        let sealed = seal(&k, 0x0101, FLAG_ENCRYPTED, b"data", b"connection-a").unwrap();
        assert_eq!(
            open(&k, 0x0101, FLAG_ENCRYPTED, &sealed, b"connection-a").unwrap(),
            b"data".to_vec()
        );
        // Same key, different challenge (another connection's hello): the
        // tag fails — the recorded frame cannot be replayed across
        // connections.
        assert!(open(&k, 0x0101, FLAG_ENCRYPTED, &sealed, b"connection-b").is_err());
        // An empty challenge (pre-hello frames) is its own binding domain.
        assert!(open(&k, 0x0101, FLAG_ENCRYPTED, &sealed, b"").is_err());
    }

    #[test]
    fn nonces_share_prefix_and_increase() {
        let a = seal(&key(), 1, FLAG_ENCRYPTED, b"x", b"conn").unwrap();
        let b = seal(&key(), 1, FLAG_ENCRYPTED, b"x", b"conn").unwrap();
        assert_eq!(a[..8], b[..8]);
        let ca = u32::from_le_bytes(a[8..12].try_into().unwrap());
        let cb = u32::from_le_bytes(b[8..12].try_into().unwrap());
        assert!(cb > ca);
    }

    #[test]
    fn open_rejects_wrong_key_and_tampering() {
        let k = key();
        let sealed = seal(&k, 0x0102, 0x0004, b"payload", b"conn").unwrap();
        let other = parse_key_hex(&"f".repeat(64)).unwrap();
        assert!(open(&other, 0x0102, 0x0004, &sealed, b"conn").is_err());
        let mut tampered = sealed.clone();
        tampered[13] ^= 0xff;
        assert!(open(&k, 0x0102, 0x0004, &tampered, b"conn").is_err());
        assert!(open(&k, 0x0102, 0x0004, b"short", b"conn").is_err());
        // Header fields are authenticated: flipping type or flags fails.
        assert!(open(&k, 0x0103, 0x0004, &sealed, b"conn").is_err());
        assert!(open(&k, 0x0102, 0x0006, &sealed, b"conn").is_err());
        assert!(open(&k, 0x0102, 0x0000, &sealed, b"conn").is_err());
    }
}
