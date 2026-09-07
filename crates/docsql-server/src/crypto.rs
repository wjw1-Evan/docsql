//! Transport encryption for the DSQ1 wire protocol.
//!
//! Pre-shared-key mode (`DOCSQL_KEY`, 32 bytes hex): every frame payload is
//! sealed with AES-256-GCM using a fresh random 96-bit nonce —
//! `payload' = nonce[12] || ciphertext+tag[16]`. The frame header (type,
//! length, flags) stays plaintext so routing stays cheap; everything
//! application-visible (SQL text, KV args, AUTH token, rows) is encrypted.

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

fn hex_decode(s: &str) -> Result<Vec<u8>, String> {
    let s = s.trim();
    if !s.len().is_multiple_of(2) {
        return Err("odd-length hex".into());
    }
    (0..s.len() / 2)
        .map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).map_err(|e| format!("bad hex: {e}")))
        .collect()
}

/// Seal one frame payload: nonce ‖ ciphertext+tag.
pub fn seal(key: &TransportKey, plaintext: &[u8]) -> Vec<u8> {
    let nonce: [u8; 12] = rand::random();
    let cipher = Aes256Gcm::new(key.into());
    let ct = cipher
        .encrypt(Nonce::from_slice(&nonce), plaintext)
        .expect("aes-gcm encrypt cannot fail with valid key/nonce");
    let mut out = Vec::with_capacity(12 + ct.len());
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ct);
    out
}

/// Open a sealed frame payload. Fails (wrong key / tampered) with a message.
pub fn open(key: &TransportKey, sealed: &[u8]) -> Result<Vec<u8>, String> {
    if sealed.len() < 12 + 16 {
        return Err("sealed payload too short".into());
    }
    let (nonce, ct) = sealed.split_at(12);
    let cipher = Aes256Gcm::new(key.into());
    cipher
        .decrypt(Nonce::from_slice(nonce), ct)
        .map_err(|_| "decrypt failed (wrong key or tampered frame)".to_string())
}
