//! Password-hashing primitives for database-user credentials
//! (FIPS 180-4 SHA-256 / RFC 2104 HMAC / RFC 8018 PBKDF2).
//!
//! Hand-rolled with known-answer tests, mirroring the web console's
//! `auth.rs` implementation — no external crypto crate (project rule).
//! Kept in core so both the engine (hashing at CREATE/ALTER USER) and the
//! server (verification at REQ_AUTH_USER) share one implementation.

/// PBKDF2 iteration count for newly hashed passwords.
pub const PBKDF2_ITERATIONS: u32 = 60_000;

/// Stored-credential prefix: `$pbkdf2-sha256$<iterations>$<salt-hex>$<hash-hex>`.
pub const HASH_PREFIX: &str = "$pbkdf2-sha256";

const K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

struct Sha256 {
    state: [u32; 8],
    buf: [u8; 64],
    len: usize,
    total: u64,
}

impl Sha256 {
    fn new() -> Self {
        Sha256 {
            state: [
                0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
                0x5be0cd19,
            ],
            buf: [0; 64],
            len: 0,
            total: 0,
        }
    }

    fn update(&mut self, mut data: &[u8]) {
        self.total = self.total.wrapping_add(data.len() as u64);
        while !data.is_empty() {
            let take = (64 - self.len).min(data.len());
            self.buf[self.len..self.len + take].copy_from_slice(&data[..take]);
            self.len += take;
            data = &data[take..];
            if self.len == 64 {
                let block = self.buf;
                compress(&mut self.state, &block);
                self.len = 0;
            }
        }
    }

    fn finish(mut self) -> [u8; 32] {
        let bit_len = self.total.wrapping_mul(8);
        self.update(&[0x80]);
        while self.len != 56 {
            self.update(&[0]);
        }
        self.update(&bit_len.to_be_bytes());
        let mut out = [0u8; 32];
        for (i, w) in self.state.iter().enumerate() {
            out[i * 4..i * 4 + 4].copy_from_slice(&w.to_be_bytes());
        }
        out
    }
}

pub fn sha256(data: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(data);
    h.finish()
}

fn compress(state: &mut [u32; 8], block: &[u8; 64]) {
    let mut w = [0u32; 64];
    for (i, chunk) in block.as_chunks::<4>().0.iter().enumerate() {
        w[i] = u32::from_be_bytes(*chunk);
    }
    for i in 16..64 {
        let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
        let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
        w[i] = w[i - 16]
            .wrapping_add(s0)
            .wrapping_add(w[i - 7])
            .wrapping_add(s1);
    }
    let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = *state;
    for i in 0..64 {
        let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
        let ch = (e & f) ^ ((!e) & g);
        let t1 = h
            .wrapping_add(s1)
            .wrapping_add(ch)
            .wrapping_add(K[i])
            .wrapping_add(w[i]);
        let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
        let maj = (a & b) ^ (a & c) ^ (b & c);
        let t2 = s0.wrapping_add(maj);
        h = g;
        g = f;
        f = e;
        e = d.wrapping_add(t1);
        d = c;
        c = b;
        b = a;
        a = t1.wrapping_add(t2);
    }
    state[0] = state[0].wrapping_add(a);
    state[1] = state[1].wrapping_add(b);
    state[2] = state[2].wrapping_add(c);
    state[3] = state[3].wrapping_add(d);
    state[4] = state[4].wrapping_add(e);
    state[5] = state[5].wrapping_add(f);
    state[6] = state[6].wrapping_add(g);
    state[7] = state[7].wrapping_add(h);
}

pub fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut k = [0u8; 64];
    if key.len() > 64 {
        k[..32].copy_from_slice(&sha256(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0x36u8; 64];
    let mut opad = [0x5cu8; 64];
    for i in 0..64 {
        ipad[i] ^= k[i];
        opad[i] ^= k[i];
    }
    let mut inner = Sha256::new();
    inner.update(&ipad);
    inner.update(data);
    let ih = inner.finish();
    let mut outer = Sha256::new();
    outer.update(&opad);
    outer.update(&ih);
    outer.finish()
}

pub fn pbkdf2_hmac_sha256(password: &[u8], salt: &[u8], iterations: u32, out: &mut [u8]) {
    assert!(iterations >= 1, "PBKDF2 needs at least one iteration");
    let mut block_index: u32 = 1;
    let mut filled = 0;
    while filled < out.len() {
        let mut salted = salt.to_vec();
        salted.extend_from_slice(&block_index.to_be_bytes());
        let mut u = hmac_sha256(password, &salted);
        let mut t = u;
        for _ in 1..iterations {
            u = hmac_sha256(password, &u);
            for (tb, ub) in t.iter_mut().zip(u.iter()) {
                *tb ^= ub;
            }
        }
        let take = (out.len() - filled).min(32);
        out[filled..filled + take].copy_from_slice(&t[..take]);
        filled += take;
        block_index = block_index
            .checked_add(1)
            .expect("pbkdf2 block counter overflow");
    }
}

pub fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

pub fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let b = s.as_bytes();
    for i in (0..b.len()).step_by(2) {
        let hi = (b[i] as char).to_digit(16)?;
        let lo = (b[i + 1] as char).to_digit(16)?;
        out.push((hi * 16 + lo) as u8);
    }
    Some(out)
}

/// Length-guarded XOR comparison (constant-time for equal lengths).
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// A parsed stored-credential string (`$pbkdf2-sha256$iters$salt$hash`).
#[derive(Debug, Clone, PartialEq)]
pub struct StoredPw {
    pub iterations: u32,
    pub salt: Vec<u8>,
    pub hash: [u8; 32],
}

/// Hash a plaintext password into the stored form.
pub fn hash_password(password: &str, salt_bytes: &[u8]) -> String {
    let mut hash = [0u8; 32];
    pbkdf2_hmac_sha256(
        password.as_bytes(),
        salt_bytes,
        PBKDF2_ITERATIONS,
        &mut hash,
    );
    format!(
        "{HASH_PREFIX}${}${}${}",
        PBKDF2_ITERATIONS,
        hex(salt_bytes),
        hex(&hash)
    )
}

impl StoredPw {
    pub fn parse(s: &str) -> Option<StoredPw> {
        let rest = s.strip_prefix(HASH_PREFIX)?.strip_prefix('$')?;
        let mut parts = rest.split('$');
        let iterations = parts.next()?.parse::<u32>().ok()?;
        if iterations < 1 {
            return None;
        }
        let salt = unhex(parts.next()?)?;
        let hash_arr: [u8; 32] = unhex(parts.next()?)?.try_into().ok()?;
        if parts.next().is_some() {
            return None;
        }
        Some(StoredPw {
            iterations,
            salt,
            hash: hash_arr,
        })
    }

    /// Constant-time password check against this stored credential.
    pub fn verify(&self, password: &str) -> bool {
        let mut got = [0u8; 32];
        pbkdf2_hmac_sha256(password.as_bytes(), &self.salt, self.iterations, &mut got);
        constant_time_eq(&got, &self.hash)
    }
}

/// True when the string is already in stored (hashed) form — replication
/// replays carry hashes, never plaintext.
pub fn is_stored_form(s: &str) -> bool {
    s.starts_with(HASH_PREFIX) && StoredPw::parse(s).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex_of(bytes: [u8; 32]) -> String {
        hex(&bytes)
    }

    #[test]
    fn sha256_known_answers() {
        assert_eq!(
            hex_of(sha256(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            hex_of(sha256(b"")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            hex_of(sha256(
                b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"
            )),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );
        // > 64 bytes: crosses block boundaries.
        assert_eq!(
            hex_of(sha256(&[b'a'; 1000])),
            "41edece42d63e8d9bf515a9ba6932e1c20cbc9f5a5d134645adb5db1b9737ea3"
        );
    }

    #[test]
    fn hmac_sha256_rfc4231_case2() {
        // RFC 4231 test vector 2 (key longer than block? no — case 2 is the
        // "Jefe" one): HMAC-SHA256 over "what do ya want for nothing?"
        let out = hmac_sha256(b"Jefe", b"what do ya want for nothing?");
        assert_eq!(
            hex_of(out),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    #[test]
    fn hmac_sha256_rfc4231_large_key_cases() {
        // RFC 4231 vectors 6/7: keys over the 64-byte block size must be
        // hashed before use (exercises the key.len() > 64 branch).
        let key = [0xaa_u8; 131];
        let out = hmac_sha256(
            &key,
            b"Test Using Larger Than Block-Size Key - Hash Key First",
        );
        assert_eq!(
            hex_of(out),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
        let out = hmac_sha256(
            &key,
            b"This is a test using a larger than block-size key and a larger than block-size data. The key needs to be hashed before being used by the HMAC algorithm.",
        );
        assert_eq!(
            hex_of(out),
            "9b09ffa71b942fcb27635fbcd5b0e944bfdc63644f0713938a7f51535c3a35e2"
        );
    }

    #[test]
    fn constant_time_eq_and_unhex_edges() {
        assert!(constant_time_eq(b"", b""));
        assert!(constant_time_eq(b"same", b"same"));
        assert!(!constant_time_eq(b"samf", b"same"));
        assert!(!constant_time_eq(b"short", b"different length"));
        assert_eq!(unhex("a"), None);
        assert_eq!(unhex("zz"), None);
        assert_eq!(unhex("0a0B"), Some(vec![0x0a, 0x0b]));
        assert_eq!(unhex(""), Some(Vec::new()));
    }

    #[test]
    fn stored_pw_parse_rejects_malformed_inputs() {
        let ok_hash = "ab".repeat(32);
        let good = format!("{HASH_PREFIX}$60000$aa${ok_hash}");
        assert!(StoredPw::parse(&good).is_some());
        // wrong prefix, non-numeric or zero iterations
        assert!(StoredPw::parse(&format!("$sha256$60000$aa${ok_hash}")).is_none());
        assert!(StoredPw::parse(&format!("{HASH_PREFIX}$abc$aa${ok_hash}")).is_none());
        assert!(StoredPw::parse(&format!("{HASH_PREFIX}$0$aa${ok_hash}")).is_none());
        // salt/hash hex problems, missing or extra segments
        assert!(StoredPw::parse(&format!("{HASH_PREFIX}$60000$zz${ok_hash}")).is_none());
        assert!(StoredPw::parse(&format!("{HASH_PREFIX}$60000$aa$cd")).is_none());
        assert!(StoredPw::parse(&format!("{HASH_PREFIX}$60000$aa${ok_hash}$trailing")).is_none());
        assert!(StoredPw::parse(&format!("{HASH_PREFIX}$60000$aa")).is_none());
    }

    #[test]
    fn pbkdf2_hmac_sha256_known_answers() {
        // RFC 7914 / draft-josefsson-pbkdf2 test vectors (SHA-256).
        let mut out = [0u8; 16];
        pbkdf2_hmac_sha256(b"password", b"salt", 1, &mut out);
        assert_eq!(hex(&out), "120fb6cffcf8b32c43e7225256c4f837");
        pbkdf2_hmac_sha256(b"password", b"salt", 2, &mut out);
        assert_eq!(hex(&out), "ae4d0c95af6b46d32d0adff928f06dd0");
        pbkdf2_hmac_sha256(b"password", b"salt", 4096, &mut out);
        assert_eq!(hex(&out), "c5e478d59288c841aa530db6845c4c8d");
        let mut long = [0u8; 40];
        pbkdf2_hmac_sha256(
            b"passwordPASSWORDpassword",
            b"saltSALTsaltSALTsaltSALTsaltSALTsalt",
            4096,
            &mut long,
        );
        assert_eq!(
            hex(&long),
            "348c89dbcbd32b2f32d814b8116e84cf2b17347ebc1800181c4e2a1fb8dd53e1c635518c7dac47e9"
        );
    }

    #[test]
    fn password_hash_round_trip() {
        let stored = hash_password("correct horse", &[1u8; 16]);
        let parsed = StoredPw::parse(&stored).expect("parses own output");
        assert!(parsed.verify("correct horse"));
        assert!(!parsed.verify("wrong horse"));
        assert!(!StoredPw::parse("$pbkdf2-sha256$0$aa$bb").is_some());
        assert!(is_stored_form(&stored));
        assert!(!is_stored_form("plaintext"));
    }
}
