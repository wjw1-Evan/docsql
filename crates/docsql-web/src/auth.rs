//! Console's own authentication: first-use username/password setup, login
//! sessions, and login-failure lockout.
//!
//! The console is still storage-free as a *database tool*: the only state is
//! one credential file (path from `DOCSQL_WEB_AUTH_FILE`) holding a salted
//! PBKDF2-HMAC-SHA256 hash of the console account, plus in-memory sessions.
//! With the env unset the console API is open (node connections always use
//! the process's `DOCSQL_TOKEN`, never a browser-supplied value).
//!
//! The hash construction (SHA-256 / HMAC / PBKDF2, with known-answer tests)
//! lives in `docsql-core`'s `kdf` module and is shared with the database
//! user-credential path — no external crypto crate (project rule).

use docsql_core::kdf::{constant_time_eq, hex, pbkdf2_hmac_sha256, unhex};
use rand::rngs::OsRng;
use rand::RngCore;
use serde_json::json;
use std::collections::HashMap;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub const MIN_PASSWORD_LEN: usize = 8;
/// PBKDF2 iterations for NEW credentials. The env-tunable value (default
/// `docsql_core::kdf::PBKDF2_ITERATIONS`, lowerable via
/// `DOCSQL_PBKDF2_ITERATIONS`) — test suites set it low so the auth path
/// stays fast and the lockout window stays meaningful under
/// instrumentation. Stored credentials carry their own count.
pub fn pbkdf2_iterations() -> u32 {
    docsql_core::kdf::iterations_for_new_credentials()
}
/// Login lockout, mirroring the server's REQ_AUTH behavior.
pub const LOCK_THRESHOLD: usize = 10;
pub const LOCK_WINDOW: Duration = Duration::from_secs(60);
pub const LOCKOUT: Duration = Duration::from_secs(60);
/// Session lifetime; sliding (renewed on each successful check).
pub const SESSION_TTL: Duration = Duration::from_secs(12 * 3600);
pub const SESSION_COOKIE: &str = "docsql_session";

// ---- credential store ----

#[derive(Debug, Clone, PartialEq)]
pub struct Creds {
    pub username: String,
    pub salt: [u8; 16],
    pub iterations: u32,
    pub hash: [u8; 32],
}

pub enum AuthMode {
    /// Env set, no credential file yet: first use, force setup.
    Setup,
    /// Credential file exists: login required.
    Login,
}

pub struct AuthStore {
    path: PathBuf,
    creds: Option<Creds>,
}

impl AuthStore {
    /// Load the credential file. A corrupt file is an error (refuse to
    /// start): silently treating it as absent would let anyone re-run
    /// setup on a gated console.
    pub fn open(path: &Path) -> Result<Self, String> {
        let creds = match std::fs::read(path) {
            Ok(bytes) => Some(parse_creds(&bytes)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(format!("cannot read {}: {e}", path.display())),
        };
        Ok(AuthStore {
            path: path.to_path_buf(),
            creds,
        })
    }

    pub fn mode(&self) -> AuthMode {
        if self.creds.is_some() {
            AuthMode::Login
        } else {
            AuthMode::Setup
        }
    }

    pub fn username(&self) -> Option<&str> {
        self.creds.as_ref().map(|c| c.username.as_str())
    }

    /// First-use account creation. Fails if the account already exists
    /// (the file is the source of truth) or the input violates policy.
    /// Single-call convenience over the two-phase form below (tests and
    /// other single-threaded callers); the HTTP handlers use the split so
    /// the PBKDF2 derivation runs without the store mutex.
    pub fn setup(&mut self, username: &str, password: &str) -> Result<Creds, SetupError> {
        if self.creds.is_some() {
            return Err(SetupError::Exists);
        }
        let creds = Self::prepare_setup(username, password)?;
        self.install_setup(creds)
    }

    /// Setup phase 1 (off-lock): validate input and derive the new
    /// credentials. Pure CPU work — no store state is touched, so the
    /// caller runs it outside the mutex (login's snapshot discipline).
    pub fn prepare_setup(username: &str, password: &str) -> Result<Creds, SetupError> {
        let username = username.trim();
        let password_ok = validate_password(password);
        if let Err(msg) = validate_username(username) {
            return Err(SetupError::Invalid(msg));
        }
        if let Err(msg) = password_ok {
            return Err(SetupError::Invalid(msg));
        }
        let salt = random_salt();
        let iterations = pbkdf2_iterations();
        Ok(Creds {
            username: username.to_string(),
            salt,
            iterations,
            hash: derive_hash(password, &salt, iterations),
        })
    }

    /// Setup phase 2 (short lock): install derived credentials. The
    /// existence check runs again under the lock right before the write,
    /// so two concurrent setups both deriving off-lock still land exactly
    /// one account (the loser gets `Exists`) — the one-shot claim
    /// semantics do not depend on phase 1's snapshot.
    pub fn install_setup(&mut self, creds: Creds) -> Result<Creds, SetupError> {
        if self.creds.is_some() {
            return Err(SetupError::Exists);
        }
        self.write(&creds).map_err(SetupError::Io)?;
        self.creds = Some(creds.clone());
        Ok(creds)
    }

    /// Snapshot the stored credentials so callers can verify off-lock (the
    /// PBKDF2 derivation must not run under the store mutex).
    pub fn creds_snapshot(&self) -> Option<Creds> {
        self.creds.clone()
    }

    pub fn verify(&self, username: &str, password: &str) -> bool {
        let Some(creds) = &self.creds else {
            return false;
        };
        verify_creds(creds, username, password)
    }

    /// Change the account's username and/or password. The caller must prove
    /// the current password even though the endpoint is session-gated: the
    /// session keeps drive-by attackers out, the current password keeps a
    /// hijacked tab from silently taking over the credential file. An absent
    /// new password keeps the current one (username-only rename). The file
    /// is rewritten before the in-memory copy moves, so a failed write
    /// leaves the old credentials authoritative.
    ///
    /// Single-call convenience over the two-phase form below; the HTTP
    /// handler uses the split so both PBKDF2 derivations (verify current +
    /// derive new) run without the store mutex.
    pub fn change_credentials(
        &mut self,
        current_password: &str,
        new_username: &str,
        new_password: Option<&str>,
    ) -> Result<Creds, ChangeError> {
        let Some(creds) = &self.creds else {
            return Err(ChangeError::Auth);
        };
        let seen = creds.clone();
        let next = Self::prepare_change(&seen, current_password, new_username, new_password)?;
        self.install_change(&seen, next)
    }

    /// Change phase 1 (off-lock): verify the current password against a
    /// snapshot and build the next credentials. Pure CPU work on `seen`
    /// (from `creds_snapshot`) — no store state is touched.
    pub fn prepare_change(
        seen: &Creds,
        current_password: &str,
        new_username: &str,
        new_password: Option<&str>,
    ) -> Result<Creds, ChangeError> {
        let candidate = derive_hash(current_password, &seen.salt, seen.iterations);
        if !constant_time_eq(&candidate, &seen.hash) {
            return Err(ChangeError::Auth);
        }
        let new_username = new_username.trim();
        if let Err(msg) = validate_username(new_username) {
            return Err(ChangeError::Invalid(msg));
        }
        Ok(match new_password {
            Some(pw) => {
                if let Err(msg) = validate_password(pw) {
                    return Err(ChangeError::Invalid(msg));
                }
                // Fresh salt on re-key: the stored hash never rests on a
                // (password, salt) pair an attacker may already hold.
                let salt = random_salt();
                let iterations = pbkdf2_iterations();
                Creds {
                    username: new_username.to_string(),
                    salt,
                    iterations,
                    hash: derive_hash(pw, &salt, iterations),
                }
            }
            None => Creds {
                username: new_username.to_string(),
                ..seen.clone()
            },
        })
    }

    /// Change phase 2 (short lock): install the derived credentials, but
    /// only if the store still holds exactly the snapshot the derivation
    /// verified against. A concurrent change (or a completed setup racing
    /// the snapshot) surfaces as `Conflict` instead of being overwritten —
    /// last-writer-wins would silently discard the other rotation.
    pub fn install_change(&mut self, seen: &Creds, next: Creds) -> Result<Creds, ChangeError> {
        if self.creds.as_ref() != Some(seen) {
            return Err(ChangeError::Conflict);
        }
        self.write(&next).map_err(ChangeError::Io)?;
        self.creds = Some(next.clone());
        Ok(next)
    }

    fn write(&self, creds: &Creds) -> Result<(), String> {
        let doc = json!({
            "version": 1,
            "username": creds.username,
            "salt_hex": hex(&creds.salt),
            "iterations": creds.iterations,
            "hash_hex": hex(&creds.hash),
        });
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("cannot create {}: {e}", parent.display()))?;
        }
        // Atomic replace: write a sibling temp file with 0600 from the
        // start, fsync it, then rename over the target. The old truncate-in-
        // place write could leave an empty/partial file after a crash
        // (AuthStore::open refuses to start on a corrupt file), bricking the
        // console until an operator repairs the volume. Write-then-chmod is
        // also avoided: it leaves a world-readable window on the hash.
        let tmp = self.path.with_extension("tmp");
        let bytes = serde_json::to_vec(&doc).map_err(|e| format!("encode creds: {e}"))?;
        #[cfg(unix)]
        {
            use std::io::Write;
            use std::os::unix::fs::OpenOptionsExt;
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(&tmp)
                .map_err(|e| format!("cannot open {}: {e}", tmp.display()))?;
            f.write_all(&bytes)
                .map_err(|e| format!("cannot write {}: {e}", tmp.display()))?;
            f.sync_all()
                .map_err(|e| format!("cannot sync {}: {e}", tmp.display()))?;
        }
        #[cfg(not(unix))]
        {
            std::fs::write(&tmp, &bytes)
                .map_err(|e| format!("cannot write {}: {e}", tmp.display()))?;
        }
        std::fs::rename(&tmp, &self.path).map_err(|e| {
            format!(
                "cannot replace {} with {}: {e}",
                self.path.display(),
                tmp.display()
            )
        })?;
        // Make the rename itself durable (best effort: some volumes refuse
        // directory fsync; the rename is still atomic without it).
        if let Some(parent) = self.path.parent() {
            if let Ok(dir) = std::fs::File::open(parent) {
                let _ = dir.sync_all();
            }
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            // The volume may not honor chmod (some network mounts); say so
            // instead of letting the file keep its mount default.
            if let Err(e) =
                std::fs::set_permissions(&self.path, std::fs::Permissions::from_mode(0o600))
            {
                eprintln!(
                    "warning: cannot chmod 600 {}: {e} (credential file may be over-readable)",
                    self.path.display()
                );
            }
        }
        Ok(())
    }
}

fn parse_creds(bytes: &[u8]) -> Result<Creds, String> {
    let v: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|e| format!("corrupt credential file: {e}"))?;
    let username = v["username"]
        .as_str()
        .ok_or("corrupt credential file: username")?
        .to_string();
    let salt = unhex(
        v["salt_hex"]
            .as_str()
            .ok_or("corrupt credential file: salt")?,
    )
    .ok_or("corrupt credential file: salt hex")?;
    let hash = unhex(
        v["hash_hex"]
            .as_str()
            .ok_or("corrupt credential file: hash")?,
    )
    .ok_or("corrupt credential file: hash hex")?;
    let salt: [u8; 16] = salt
        .try_into()
        .map_err(|_| "corrupt credential file: salt len")?;
    let hash: [u8; 32] = hash
        .try_into()
        .map_err(|_| "corrupt credential file: hash len")?;
    // Missing count (hand-written file) defaults to the built-in default,
    // not the env override: a stored entry's count is the one it was
    // hashed with.
    let iterations = v["iterations"]
        .as_u64()
        .unwrap_or(docsql_core::kdf::PBKDF2_ITERATIONS as u64) as u32;
    // A hand-edited 0 would hit pbkdf2_hmac_sha256's assert and panic the
    // login handler on every attempt; an astronomic value would turn each
    // login into a CPU burn — both are corrupt like any other tampering.
    if !(1..=docsql_core::kdf::MAX_PBKDF2_ITERATIONS).contains(&iterations) {
        return Err("corrupt credential file: iterations".into());
    }
    Ok(Creds {
        username,
        salt,
        iterations,
        hash,
    })
}

fn validate_username(username: &str) -> Result<(), String> {
    let len = username.chars().count();
    if len == 0 || len > 64 {
        return Err("用户名需为 1-64 个字符".into());
    }
    Ok(())
}

fn validate_password(password: &str) -> Result<(), String> {
    if password.chars().count() < MIN_PASSWORD_LEN {
        return Err(format!("密码至少 {} 个字符", MIN_PASSWORD_LEN));
    }
    let mut chars = password.chars();
    let first = chars.next().unwrap();
    if password.chars().all(|c| c == first) {
        return Err("密码不能为单一字符重复".into());
    }
    Ok(())
}

/// Why a setup attempt failed. `Exists` counts toward the login lockout
/// (it signals a hostile attempt to re-run first-use); plain validation
/// errors do not — a real user may fumble the password rules.
#[derive(Debug, Clone, PartialEq)]
pub enum SetupError {
    Exists,
    Invalid(String),
    Io(String),
}

/// Why a credential-change attempt failed. Same lockout split as
/// `SetupError`: `Auth` is a guess at the current password (possibly
/// hostile, counts toward the lockout); validation errors are honest
/// fumbling and do not.
#[derive(Debug, Clone, PartialEq)]
pub enum ChangeError {
    /// Current password did not verify (or no account exists yet).
    Auth,
    Invalid(String),
    /// The stored credentials changed between the off-lock derivation and
    /// the locked install (concurrent change/setup won the race). Nothing
    /// was written; the caller retries against the fresh snapshot.
    Conflict,
    Io(String),
}

pub fn derive_hash(password: &str, salt: &[u8], iterations: u32) -> [u8; 32] {
    let mut out = [0u8; 32];
    pbkdf2_hmac_sha256(password.as_bytes(), salt, iterations, &mut out);
    out
}

/// Verify a password against a credential snapshot. Constant-time compare
/// via the same helper as the server's token check; a username mismatch
/// still burns a derivation so timing does not reveal which part was wrong.
pub fn verify_creds(creds: &Creds, username: &str, password: &str) -> bool {
    if !constant_time_eq(username.trim().as_bytes(), creds.username.as_bytes()) {
        derive_hash(password, &creds.salt, creds.iterations);
        return false;
    }
    let candidate = derive_hash(password, &creds.salt, creds.iterations);
    constant_time_eq(&candidate, &creds.hash)
}

fn random_salt() -> [u8; 16] {
    let mut s = [0u8; 16];
    OsRng.fill_bytes(&mut s);
    s
}

pub fn random_token() -> String {
    let mut b = [0u8; 32];
    OsRng.fill_bytes(&mut b);
    hex(&b)
}

// ---- sessions ----

/// Live sessions kept at once. The console has one account; every entry
/// beyond a handful is either a stale tab or someone hammering the login
/// form with stolen credentials — evicting the soonest-to-expire keeps
/// the table bounded without ever evicting an active session (active
/// sessions renew and sit at the full TTL).
pub const SESSION_MAX_LIVE: usize = 64;
/// Absolute lifetime. The sliding 12h TTL alone lets a continuously-used
/// session live forever; a stolen cookie then never expires. 24h walls
/// that off without touching interactive use.
pub const SESSION_MAX_AGE: Duration = Duration::from_secs(24 * 3600);

struct SessionEntry {
    created: Instant,
    expires: Instant,
}

pub struct Sessions {
    map: Mutex<HashMap<String, SessionEntry>>,
}

impl Default for Sessions {
    fn default() -> Self {
        Self::new()
    }
}

impl Sessions {
    pub fn new() -> Self {
        Sessions {
            map: Mutex::new(HashMap::new()),
        }
    }

    pub fn create(&self) -> String {
        let token = random_token();
        let mut map = self.map.lock().unwrap();
        let now = Instant::now();
        while map.len() >= SESSION_MAX_LIVE {
            // Evict the soonest-to-expire session: the least valuable
            // under both sliding and absolute rules.
            let victim = map
                .iter()
                .min_by_key(|(_, e)| e.expires)
                .map(|(t, _)| t.clone());
            match victim {
                Some(t) => {
                    map.remove(&t);
                }
                None => break,
            }
        }
        map.insert(
            token.clone(),
            SessionEntry {
                created: now,
                expires: now + SESSION_TTL,
            },
        );
        token
    }

    /// Sliding expiry within an absolute lifetime: every accepted request
    /// extends the session, but never past SESSION_MAX_AGE since creation.
    pub fn verify(&self, token: &str) -> bool {
        let mut map = self.map.lock().unwrap();
        let now = Instant::now();
        map.retain(|_, e| e.expires > now);
        match map.get_mut(token) {
            Some(e) if e.expires > now && now.duration_since(e.created) < SESSION_MAX_AGE => {
                e.expires = now + SESSION_TTL;
                true
            }
            _ => false,
        }
    }

    pub fn drop_session(&self, token: &str) {
        self.map.lock().unwrap().remove(token);
    }

    /// Drop every session except `keep` (`None` = drop all). Credential
    /// rotation must not leave other logged-in holders live; the caller's
    /// own session (or a token-bypass caller, which holds none) survives.
    pub fn keep_only(&self, keep: Option<&str>) {
        self.map
            .lock()
            .unwrap()
            .retain(|token, _| Some(token.as_str()) == keep);
    }
}

// ---- login lockout (mirrors the server's REQ_AUTH lockout) ----

pub struct Lockout {
    failures: HashMap<IpAddr, (Vec<Instant>, Option<Instant>)>,
}

impl Default for Lockout {
    fn default() -> Self {
        Self::new()
    }
}

impl Lockout {
    pub fn new() -> Self {
        Lockout {
            failures: HashMap::new(),
        }
    }

    pub fn check(&mut self, ip: IpAddr) -> bool {
        let now = Instant::now();
        let Some(entry) = self.failures.get_mut(&ip) else {
            return false;
        };
        entry.0.retain(|f| now.duration_since(*f) < LOCK_WINDOW);
        if let Some(until) = entry.1 {
            if until > now {
                return true;
            }
            entry.1 = None;
        }
        false
    }

    pub fn record_failure(&mut self, ip: IpAddr) {
        let now = Instant::now();
        let entry = self.failures.entry(ip).or_default();
        entry.0.retain(|f| now.duration_since(*f) < LOCK_WINDOW);
        entry.0.push(now);
        if entry.0.len() >= LOCK_THRESHOLD {
            entry.1 = Some(now + LOCKOUT);
            entry.0.clear();
        }
        // Drop idle buckets along the way: a scanner rotating source IPs
        // would otherwise grow the map forever (buckets are otherwise only
        // removed on that IP's successful login).
        self.failures.retain(|_, e| {
            e.0.retain(|f| now.duration_since(*f) < LOCK_WINDOW);
            !e.0.is_empty() || e.1.is_some_and(|until| until > now)
        });
    }

    pub fn reset(&mut self, ip: IpAddr) {
        self.failures.remove(&ip);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Crypto known-answer coverage lives with the shared construction in
    // `docsql-core`'s kdf module; the tests here cover the credential store.

    #[test]
    fn store_setup_verify_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("console-auth.json");
        let mut store = AuthStore::open(&path).unwrap();
        assert!(matches!(store.mode(), AuthMode::Setup));
        assert!(!store.verify("admin", "whatever"));
        store.setup("admin", "s3cret-pw").unwrap();
        assert!(matches!(store.mode(), AuthMode::Login));
        assert!(store.verify("admin", "s3cret-pw"));
        assert!(!store.verify("admin", "wrong-pw"));
        assert!(!store.verify("root", "s3cret-pw"));
        // Setup is one-shot.
        assert!(store.setup("other", "another-pw").is_err());
        // Reopen: mode is Login and verification still passes.
        let store = AuthStore::open(&path).unwrap();
        assert!(matches!(store.mode(), AuthMode::Login));
        assert!(store.verify("admin", "s3cret-pw"));
    }

    #[test]
    fn setup_rejects_weak_input() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = AuthStore::open(&dir.path().join("a.json")).unwrap();
        assert!(store.setup("", "longenough1").is_err());
        assert!(store.setup("admin", "short").is_err());
        assert!(store.setup("admin", "aaaaaaaa").is_err());
    }

    #[test]
    fn corrupt_file_refuses_to_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.json");
        std::fs::write(&path, b"not json").unwrap();
        assert!(AuthStore::open(&path).is_err());
    }

    #[test]
    fn parse_creds_rejects_tampered_fields() {
        let salt_hex = hex(&[0xab_u8; 16]);
        let hash_hex = hex(&[0xcd_u8; 32]);
        let creds = |user: &str, salt: &str, hash: &str, iters: serde_json::Value| -> Vec<u8> {
            json!({"username": user, "salt_hex": salt, "hash_hex": hash, "iterations": iters})
                .to_string()
                .into_bytes()
        };
        // Well-formed file parses; a missing iterations key falls back to
        // the default (older credential files predate the field).
        let c = parse_creds(&creds("admin", &salt_hex, &hash_hex, json!(1000))).unwrap();
        assert_eq!(c.iterations, 1000);
        let no_iters = json!({"username": "admin", "salt_hex": salt_hex, "hash_hex": hash_hex})
            .to_string()
            .into_bytes();
        assert_eq!(
            parse_creds(&no_iters).unwrap().iterations,
            docsql_core::kdf::PBKDF2_ITERATIONS
        );
        // A hand-edited 0 would panic the login handler downstream — refused.
        assert!(parse_creds(&creds("admin", &salt_hex, &hash_hex, json!(0))).is_err());
        // Missing or non-string fields, broken hex, wrong lengths.
        // (An empty username string still parses here — name policy is
        // setup's job; parse only guards the file's structural integrity.)
        assert!(parse_creds(b"{}").is_err());
        assert!(parse_creds(&creds("admin", "zz", &hash_hex, json!(1000))).is_err());
        assert!(parse_creds(&creds("admin", &hex(&[1_u8; 8]), &hash_hex, json!(1000))).is_err());
        assert!(parse_creds(&creds("admin", &salt_hex, "nothex", json!(1000))).is_err());
        assert!(parse_creds(&creds("admin", &salt_hex, "cd", json!(1000))).is_err());
    }

    #[test]
    fn validate_username_length_boundaries() {
        assert!(validate_username("").is_err());
        assert!(validate_username(&"a".repeat(64)).is_ok());
        assert!(validate_username(&"a".repeat(65)).is_err());
        // The limit counts characters, not bytes.
        assert!(validate_username(&"好".repeat(65)).is_err());
    }

    #[test]
    fn lockout_locks_after_threshold_and_resets() {
        let mut lock = Lockout::new();
        let ip: IpAddr = "10.0.0.9".parse().unwrap();
        for _ in 0..LOCK_THRESHOLD {
            assert!(!lock.check(ip));
            lock.record_failure(ip);
        }
        assert!(lock.check(ip), "locked after {LOCK_THRESHOLD} failures");
        lock.reset(ip);
        assert!(!lock.check(ip));
    }

    #[test]
    fn sessions_slide_and_expire() {
        let s = Sessions::new();
        let t = s.create();
        assert!(s.verify(t.as_str()));
        s.drop_session(t.as_str());
        assert!(!s.verify(t.as_str()));
    }

    #[test]
    fn change_credentials_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("console-auth.json");
        let mut store = AuthStore::open(&path).unwrap();
        store.setup("admin", "s3cret-pw").unwrap();

        // Rename + re-key in one call: old identity is gone in memory.
        let creds = store
            .change_credentials("s3cret-pw", "root", Some("n3w-password"))
            .unwrap();
        assert_eq!(creds.username, "root");
        assert!(store.verify("root", "n3w-password"));
        assert!(!store.verify("admin", "s3cret-pw"));
        assert!(!store.verify("root", "s3cret-pw"));

        // The rewrite is durable: a fresh store sees the new identity only.
        let store = AuthStore::open(&path).unwrap();
        assert_eq!(store.username(), Some("root"));
        assert!(store.verify("root", "n3w-password"));
        assert!(!store.verify("admin", "s3cret-pw"));
    }

    #[test]
    fn change_requires_current_password_and_valid_input() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("console-auth.json");
        let mut store = AuthStore::open(&path).unwrap();
        // No account yet: nothing to authenticate against.
        assert_eq!(
            store
                .change_credentials("whatever1", "root", None)
                .unwrap_err(),
            ChangeError::Auth
        );
        store.setup("admin", "s3cret-pw").unwrap();

        // Wrong current password: refused, identity unchanged.
        assert_eq!(
            store
                .change_credentials("wrong-pass", "root", None)
                .unwrap_err(),
            ChangeError::Auth
        );
        assert!(store.verify("admin", "s3cret-pw"));

        // Policy violations on the new values: refused, identity unchanged.
        assert_eq!(
            store.change_credentials("s3cret-pw", "", None).unwrap_err(),
            ChangeError::Invalid("用户名需为 1-64 个字符".into())
        );
        assert_eq!(
            store
                .change_credentials("s3cret-pw", "root", Some("short"))
                .unwrap_err(),
            ChangeError::Invalid("密码至少 8 个字符".into())
        );
        assert_eq!(
            store
                .change_credentials("s3cret-pw", "root", Some("aaaaaaaa"))
                .unwrap_err(),
            ChangeError::Invalid("密码不能为单一字符重复".into())
        );
        assert_eq!(store.username(), Some("admin"));
        assert!(store.verify("admin", "s3cret-pw"));
    }

    #[test]
    fn change_username_only_keeps_password() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("console-auth.json");
        let mut store = AuthStore::open(&path).unwrap();
        store.setup("admin", "s3cret-pw").unwrap();
        store.change_credentials("s3cret-pw", "ops", None).unwrap();
        assert!(store.verify("ops", "s3cret-pw"));
        assert!(!store.verify("admin", "s3cret-pw"));
        let reopened = AuthStore::open(&path).unwrap();
        assert_eq!(reopened.username(), Some("ops"));
        assert!(reopened.verify("ops", "s3cret-pw"));
    }

    /// 两阶段 setup 的并发兜底:锁外派生完成后再 install,若期间凭据
    /// 已被落盘,第二个安装者拿到 Exists 而不是覆盖。
    #[test]
    fn install_setup_refuses_when_account_appeared_off_lock() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("console-auth.json");
        let mut store = AuthStore::open(&path).unwrap();
        let first = AuthStore::prepare_setup("admin", "s3cret-pw").unwrap();
        store.install_setup(first).unwrap();
        // 并发派生的第二个凭据到达写回阶段:被拒,原账号不被覆盖。
        let second = AuthStore::prepare_setup("root", "n3w-password").unwrap();
        assert_eq!(store.install_setup(second).unwrap_err(), SetupError::Exists);
        assert_eq!(store.username(), Some("admin"));
        assert!(store.verify("admin", "s3cret-pw"));
        assert!(!store.verify("root", "n3w-password"));
    }

    /// 两阶段 change 的 TOCTOU 防护:凭据在锁外派生期间被并发修改时,
    /// install 以 Conflict 拒绝且不落盘 —— 对方的轮换不被覆盖。
    #[test]
    fn install_change_refuses_when_credentials_moved_off_lock() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("console-auth.json");
        let mut store = AuthStore::open(&path).unwrap();
        store.setup("admin", "s3cret-pw").unwrap();
        let seen = store.creds_snapshot().unwrap();
        // 锁外派生期间另一个会话先完成了改密。
        store.change_credentials("s3cret-pw", "ops", None).unwrap();
        let next =
            AuthStore::prepare_change(&seen, "s3cret-pw", "attacker", Some("stolen-pw")).unwrap();
        assert_eq!(
            store.install_change(&seen, next).unwrap_err(),
            ChangeError::Conflict
        );
        // 落盘与内存都还是赢家的形态。
        assert_eq!(store.username(), Some("ops"));
        assert!(!store.verify("attacker", "stolen-pw"));
        let reopened = AuthStore::open(&path).unwrap();
        assert_eq!(reopened.username(), Some("ops"));
        assert!(reopened.verify("ops", "s3cret-pw"));
    }

    /// 凭据仍与快照一致时,install 正常落盘(两阶段路径的直通形态)。
    #[test]
    fn install_change_writes_when_snapshot_still_current() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("console-auth.json");
        let mut store = AuthStore::open(&path).unwrap();
        store.setup("admin", "s3cret-pw").unwrap();
        let seen = store.creds_snapshot().unwrap();
        let next = AuthStore::prepare_change(&seen, "s3cret-pw", "ops", Some("n3w-password"))
            .expect("current password verified");
        store.install_change(&seen, next).unwrap();
        assert!(store.verify("ops", "n3w-password"));
        assert!(!store.verify("admin", "s3cret-pw"));
    }

    #[test]
    fn sessions_keep_only_survivor() {
        let s = Sessions::new();
        let a = s.create();
        let b = s.create();
        s.keep_only(Some(a.as_str()));
        assert!(s.verify(a.as_str()));
        assert!(!s.verify(b.as_str()));
        s.keep_only(None);
        assert!(!s.verify(a.as_str()));
    }

    #[test]
    fn sessions_evict_soonest_expiry_at_capacity() {
        // Default + 容量上限:第 SESSION_MAX_LIVE+1 个会话挤掉最快过期者,
        // 存活令牌恰好保持在上限内。
        let s = Sessions::default();
        let mut tokens = Vec::new();
        for _ in 0..(SESSION_MAX_LIVE + 2) {
            tokens.push(s.create());
        }
        let live = tokens.iter().filter(|t| s.verify(t)).count();
        assert!(
            (SESSION_MAX_LIVE - 1..=SESSION_MAX_LIVE).contains(&live),
            "live={live}"
        );
        // 显式下线单个会话。
        let gone = tokens[0].clone();
        s.drop_session(&gone);
        assert!(!s.verify(&gone));
    }

    #[test]
    fn lockout_threshold_and_reset() {
        let mut l = Lockout::default();
        let ip: std::net::IpAddr = "127.0.0.1".parse().unwrap();
        // 无记录 / 阈值内:不锁。
        assert!(!l.check(ip));
        for _ in 0..(LOCK_THRESHOLD - 1) {
            l.record_failure(ip);
        }
        assert!(!l.check(ip));
        // 过阈值:锁定并持续到 reset。
        l.record_failure(ip);
        assert!(l.check(ip));
        assert!(l.check(ip), "lock persists until reset");
        l.reset(ip);
        assert!(!l.check(ip));
    }

    #[test]
    fn store_open_rejects_unreadable_paths() {
        let dir = tempfile::tempdir().unwrap();
        // 目录路径:读取报错(非 NotFound)→ 拒绝启动而不是当成未初始化。
        let e = match AuthStore::open(dir.path()) {
            Err(e) => e,
            Ok(_) => panic!("opening a directory must not succeed"),
        };
        assert!(e.contains("cannot read"), "{e}");
    }
}
