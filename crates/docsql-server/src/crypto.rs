//! Transport encryption for the DSQ1 wire protocol.
//!
//! Pre-shared-key mode (`DOCSQL_KEY`, 32 bytes hex): every frame payload is
//! sealed with AES-256-GCM using a fresh random 96-bit nonce —
//! `payload' = nonce[12] || ciphertext+tag[16]`. The frame header (type,
//! length, flags) stays plaintext so routing stays cheap; everything
//! application-visible (SQL text, KV args, AUTH token, rows) is encrypted.
//! The header fields are bound into the GCM tag as associated data: a MITM
//! flipping a flag (clearing FLAG_ENCRYPTED, setting FLAG_REPLICATION)
//! invalidates the frame instead of re-routing it.

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
    if !s.len().is_multiple_of(2) {
        return Err("odd-length hex".into());
    }
    (0..s.len() / 2)
        .map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).map_err(|e| format!("bad hex: {e}")))
        .collect()
}

/// Associated data for a frame's tag: the transmitted header fields a
/// receiver routes on (type + flags), little-endian like the wire form.
pub fn aad(frame_type: u16, flags: u16) -> [u8; 4] {
    let mut out = [0u8; 4];
    out[..2].copy_from_slice(&frame_type.to_le_bytes());
    out[2..].copy_from_slice(&flags.to_le_bytes());
    out
}

/// Seal one frame payload: nonce ‖ ciphertext+tag. `flags` must be the
/// flags as transmitted (i.e. with [`FLAG_ENCRYPTED`] already set).
pub fn seal(key: &TransportKey, frame_type: u16, flags: u16, plaintext: &[u8]) -> Vec<u8> {
    let nonce: [u8; 12] = rand::random();
    let cipher = Aes256Gcm::new(key.into());
    let ct = cipher
        .encrypt(
            Nonce::from_slice(&nonce),
            aes_gcm::aead::Payload {
                msg: plaintext,
                aad: &aad(frame_type, flags),
            },
        )
        .expect("aes-gcm encrypt cannot fail with valid key/nonce");
    let mut out = Vec::with_capacity(12 + ct.len());
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ct);
    out
}

/// Open a sealed frame payload. Fails (wrong key / tampered header or
/// payload) with a message.
pub fn open(
    key: &TransportKey,
    frame_type: u16,
    flags: u16,
    sealed: &[u8],
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
                aad: &aad(frame_type, flags),
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
        let sealed = seal(&k, 0x0102, 0x0004, pt);
        // nonce(12) + tag(16) + plaintext
        assert_eq!(sealed.len(), 12 + 16 + pt.len());
        assert_ne!(&sealed[12..], &pt[..]);
        assert_eq!(open(&k, 0x0102, 0x0004, &sealed).unwrap(), pt.to_vec());
        // fresh nonce each seal
        assert_ne!(seal(&k, 0x0102, 0x0004, pt), seal(&k, 0x0102, 0x0004, pt));
    }

    #[test]
    fn open_rejects_wrong_key_and_tampering() {
        let k = key();
        let sealed = seal(&k, 0x0102, 0x0004, b"payload");
        let other = parse_key_hex(&"f".repeat(64)).unwrap();
        assert!(open(&other, 0x0102, 0x0004, &sealed).is_err());
        let mut tampered = sealed.clone();
        tampered[13] ^= 0xff;
        assert!(open(&k, 0x0102, 0x0004, &tampered).is_err());
        assert!(open(&k, 0x0102, 0x0004, b"short").is_err());
        // Header fields are authenticated: flipping type or flags fails.
        assert!(open(&k, 0x0103, 0x0004, &sealed).is_err());
        assert!(open(&k, 0x0102, 0x0006, &sealed).is_err());
        assert!(open(&k, 0x0102, 0x0000, &sealed).is_err());
    }
}
