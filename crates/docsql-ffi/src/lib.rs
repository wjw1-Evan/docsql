//! `docsql-ffi` — the C ABI that lets non-Rust processes (starting with the
//! .NET client's `Data Source=…` embedded mode) run the DocSQL engine
//! in-process: no server, no port, no daemon.
//!
//! Design contract (docs/design/006-dotnet-embedded-mode.md):
//!
//! - **No process spawning, no network.** Every call runs the same
//!   `docsql-core` engine the server uses; the library is a thin, panicking-safe
//!   boundary over it.
//! - **One `Database` per file per process.** A process-wide registry keyed by
//!   the canonicalized path hands every session the same `SharedDb` — the
//!   engine's single-writer discipline is then enforced by the embedded gate
//!   below (a second *process* opening the same file is still outside the
//!   guard, exactly like the documented storage boundary).
//! - **Reads never block writes** (MVCC stage B): a read builds an owned
//!   `ReadView` under a microsecond-scale read lock and executes entirely
//!   lock-free, exactly like the server's read tier.
//! - **Transactions keep server parity**: while any session holds an open
//!   transaction, the read tier yields and *all* statements go through the
//!   gated write path (a long `SELECT` inside a transaction sees its own
//!   uncommitted writes; other sessions wait up to the caller's timeout, then
//!   get a loud error instead of interleaving into someone else's unit).
//! - **Every entry point is panic-safe**: panics are caught at the boundary
//!   and reported as an error code — a panic crossing the FFI is UB.
//!
//! Error codes (return values):
//! `0` ok · `1` SQL/engine error · `2` usage/bad argument or parameter JSON ·
//! `3` internal panic · `4` transaction-ownership wait timed out ·
//! `5` open/IO error.
//!
//! Result payloads (UTF-8 JSON, allocated; free with
//! [`docsql_embedded_free`]): `{"affected":n}` or
//! `{"columns":[…],"rows":[[…],…]}` — byte-for-byte the server's RESP_ROWS
//! payload (same `json::to_string` scalar markers: `$dec`/`$bytes`/`$ts`/
//! `$float`), so the .NET client parses embedded results with the same code
//! path that parses wire responses.

use docsql_core::bind::{bind_params, json_param_to_value};
use docsql_core::engine::{Database, ExecOutcome};
use docsql_core::value::{Object, Value};
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, RwLock, Weak};
use std::time::{Duration, Instant};

/// Default wait for a transaction held by another connection (matches the
/// server's 30 s BEGIN queue bound; the .NET caller can shorten it through
/// its command/read timeout).
const DEFAULT_WAIT_MS: u64 = 30_000;

/// Opaque session handle handed to C callers.
pub struct Session {
    shared: Arc<SharedDb>,
    /// Stable ownership token for the transaction gate (`BEGIN` attributes
    /// the open transaction to the session that ran it).
    token: usize,
    /// The registry key, when this session owns a file-backed database (kept
    /// so a panic message can name the file). `:memory:` sessions skip it.
    path: Option<std::path::PathBuf>,
}

/// One engine + gate per data file, shared by every session in the process.
struct SharedDb {
    db: RwLock<Database>,
    /// Transaction owner token while a unit is open (`None` = no open
    /// transaction). Guarded by a non-blocking mutex so waiters can apply a
    /// deadline instead of queueing forever behind a hung transaction.
    gate: Mutex<Option<usize>>,
}

fn registry() -> &'static Mutex<HashMap<std::path::PathBuf, Weak<SharedDb>>> {
    static REG: OnceLock<Mutex<HashMap<std::path::PathBuf, Weak<SharedDb>>>> = OnceLock::new();
    REG.get_or_init(|| Mutex::new(HashMap::new()))
}

fn lock_or_recover<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

/// Canonical registry key. New files cannot be canonicalized directly, so
/// canonicalize the parent directory and rejoin the file name: two spellings
/// of the same future byte path then share one engine (the same trick SQLite
/// uses for its file identity).
fn registry_key(path: &std::path::Path) -> std::path::PathBuf {
    if let Ok(c) = std::fs::canonicalize(path) {
        return c;
    }
    match (path.parent(), path.file_name()) {
        (Some(dir), Some(name)) => std::fs::canonicalize(dir)
            .map(|d| d.join(name))
            .unwrap_or_else(|_| path.to_path_buf()),
        _ => path.to_path_buf(),
    }
}

fn open_shared(path: &str) -> Result<Arc<SharedDb>, (i32, String)> {
    if path == ":memory:" {
        let db = Database::in_memory().map_err(|e| (5, format!("open :memory:: {e}")))?;
        return Ok(Arc::new(SharedDb {
            db: RwLock::new(db),
            gate: Mutex::new(None),
        }));
    }
    let raw = std::path::Path::new(path);
    let key = registry_key(raw);
    let mut reg = lock_or_recover(registry());
    if let Some(existing) = reg.get(&key).and_then(Weak::upgrade) {
        return Ok(existing);
    }
    let db = Database::open(&key).map_err(|e| (5, format!("open {}: {e}", key.display())))?;
    let shared = Arc::new(SharedDb {
        db: RwLock::new(db),
        gate: Mutex::new(None),
    });
    reg.insert(key, Arc::downgrade(&shared));
    Ok(shared)
}

impl SharedDb {
    /// Take the write gate for `token`, waiting at most `wait` for another
    /// session's open transaction to finish. The returned guard is the ONLY
    /// path to `db.write()` — gate-before-database lock ordering keeps readers
    /// (which never touch the gate) deadlock-free.
    fn write_gate(
        &self,
        token: usize,
        wait: Duration,
    ) -> Result<MutexGuard<'_, Option<usize>>, (i32, String)> {
        let deadline = Instant::now() + wait;
        loop {
            match self.gate.try_lock() {
                Ok(g) => {
                    if g.is_none() || *g == Some(token) {
                        return Ok(g);
                    }
                }
                Err(std::sync::TryLockError::Poisoned(e)) => {
                    let g = e.into_inner();
                    if g.is_none() || *g == Some(token) {
                        return Ok(g);
                    }
                }
                Err(std::sync::TryLockError::WouldBlock) => {}
            }
            if Instant::now() >= deadline {
                return Err((
                    4,
                    format!(
                        "another connection holds the open transaction; waited {} ms",
                        wait.as_millis()
                    ),
                ));
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

fn deadline_of(timeout_ms: i32) -> Option<Instant> {
    (timeout_ms > 0).then(|| Instant::now() + Duration::from_millis(timeout_ms as u64))
}

/// The server's RESP_ROWS / RESP_AFFECTED payload shapes, produced by the
/// same scalar serializer (`json::to_string`) the wire frames use.
fn outcome_json(outcome: ExecOutcome) -> String {
    match outcome {
        ExecOutcome::Affected(n) => format!("{{\"affected\":{n}}}"),
        ExecOutcome::Rows(r) => {
            let mut obj = Object::new();
            obj.insert(
                "columns".into(),
                Value::Array(r.columns.into_iter().map(Value::Str).collect()),
            );
            obj.insert(
                "rows".into(),
                Value::Array(r.rows.into_iter().map(Value::Array).collect()),
            );
            docsql_core::json::to_string(&Value::Object(obj))
        }
    }
}

fn wait_duration(timeout_ms: i32) -> Duration {
    Duration::from_millis(if timeout_ms > 0 {
        timeout_ms as u64
    } else {
        DEFAULT_WAIT_MS
    })
}

/// One statement through the engine under embedding rules (see module docs).
fn execute_in_shared(
    shared: &SharedDb,
    token: usize,
    sql: &str,
    params_json: Option<&str>,
    timeout_ms: i32,
) -> Result<String, (i32, String)> {
    let params: Option<Vec<Value>> = match params_json {
        None => None,
        Some(text) => {
            let v: serde_json::Value =
                serde_json::from_str(text).map_err(|e| (2, format!("bad params JSON: {e}")))?;
            let arr = v
                .as_array()
                .ok_or((2, "params must be a JSON array".to_string()))?;
            Some(arr.iter().map(json_param_to_value).collect())
        }
    };
    let bound = match &params {
        None => sql.to_string(),
        Some(ps) => bind_params(sql, ps).map_err(|e| (1, e))?,
    };
    let parsed = Database::parse_classified(&bound).map_err(|e| (1, e.to_string()))?;
    let is_write = parsed.is_write || parsed.tx != docsql_core::engine::TxControl::None;

    // Read tier: only while no transaction is open, mirroring the server's
    // `classify_read_eligible` (owner SELECTs inside a transaction must see
    // their own uncommitted writes, so they go through the gated path).
    if !is_write {
        let gate_free = lock_or_recover(&shared.gate).is_none();
        if gate_free {
            let view = {
                let db = shared.db.read().unwrap_or_else(|p| p.into_inner());
                db.read_view()
            };
            if let Some(deadline) = deadline_of(timeout_ms) {
                view.set_statement_deadline(Some(deadline));
            }
            let out = view.execute(&bound).map_err(|e| (1, e.to_string()))?;
            return Ok(outcome_json(out));
        }
    }

    let mut gate = shared.write_gate(token, wait_duration(timeout_ms))?;
    let out = {
        let mut db = shared.db.write().unwrap_or_else(|p| p.into_inner());
        let out = db.execute_parsed(parsed).map_err(|e| (1, e.to_string()))?;
        // Engine truth decides ownership: BEGIN opened a unit (this session
        // owns it), COMMIT/ROLLBACK closed it, everything else leaves the
        // state unchanged. Only the gate holder can write, so `Some(token)`
        // here is always this session's unit.
        *gate = db.in_transaction().then_some(token);
        out
    };
    Ok(outcome_json(out))
}

/// Copy a Rust string into a C-owned allocation (freed by
/// [`docsql_embedded_free`]). NUL bytes cannot appear: SQL values travel
/// JSON-escaped, and error strings are diagnostics.
unsafe fn into_ptr(text: String) -> *mut c_char {
    match CString::new(text) {
        Ok(c) => c.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

/// Write an owned string through an out-pointer, tolerating callers that pass
/// a null out-param (they cannot read the message, but the call must not be
/// undefined behavior).
unsafe fn set_out(out: *mut *mut c_char, text: String) {
    if !out.is_null() {
        *out = into_ptr(text);
    }
}

/// Panic guard for every exported function. A panic payload is flattened into
/// the caller's error text; the process stays alive (the alternative — letting
/// it unwind across `extern "C"` — is undefined behavior).
fn guard(out_error: *mut *mut c_char, f: impl FnOnce() -> i32) -> i32 {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
    match result {
        Ok(code) => code,
        Err(payload) => {
            let msg = payload
                .downcast_ref::<&str>()
                .map(|s| (*s).to_string())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown panic".into());
            if !out_error.is_null() {
                unsafe {
                    *out_error =
                        into_ptr(format!("internal panic in docsql-embedded (bug): {msg}"));
                }
            }
            3
        }
    }
}

/// ABI version of the C surface. The .NET client refuses a library whose
/// major contract it does not know.
#[no_mangle]
pub extern "C" fn docsql_embedded_abi_version() -> i32 {
    1
}

/// Open (or join, for a path already open in this process) a database.
///
/// `path` is a UTF-8 file path or `:memory:` (fresh engine per open, not
/// shared). On success writes a session handle to `out_handle`; on failure
/// returns a non-zero code and, when `out_error` is non-null, an allocated
/// message (free with [`docsql_embedded_free`]).
///
/// # Safety
/// `path` must be a valid NUL-terminated UTF-8 string; `out_handle` and
/// `out_error` must be valid writable pointers.
#[no_mangle]
pub unsafe extern "C" fn docsql_embedded_open(
    path: *const c_char,
    out_handle: *mut *mut Session,
    out_error: *mut *mut c_char,
) -> i32 {
    guard(out_error, || {
        let Some(path) = (!path.is_null()).then(|| CStr::from_ptr(path)) else {
            unsafe { set_out(out_error, "open: null path".into()) };
            return 2;
        };
        let path = match path.to_str() {
            Ok(p) if !p.is_empty() => p,
            Ok(_) => {
                unsafe { set_out(out_error, "open: empty path".into()) };
                return 2;
            }
            Err(_) => {
                unsafe { set_out(out_error, "open: path is not valid UTF-8".into()) };
                return 2;
            }
        };
        match open_shared(path) {
            Ok(shared) => {
                let session = Box::new(Session {
                    shared,
                    token: 0,
                    path: (path != ":memory:").then(|| registry_key(std::path::Path::new(path))),
                });
                let raw = Box::into_raw(session);
                // The session's stable identity is its heap address: it is the
                // transaction-ownership token.
                unsafe { (*raw).token = raw as usize };
                unsafe { *out_handle = raw };
                0
            }
            Err((code, msg)) => {
                unsafe { set_out(out_error, msg) };
                code
            }
        }
    })
}

/// Close a session (dropping the last session of a file closes its engine;
/// the WAL remains for the next open to replay).
///
/// # Safety
/// `handle` must come from [`docsql_embedded_open`] and not be used again.
#[no_mangle]
pub unsafe extern "C" fn docsql_embedded_close(handle: *mut Session) {
    if !handle.is_null() {
        drop(unsafe { Box::from_raw(handle) });
    }
}

/// Execute one statement (parameters rendered through the shared
/// `docsql_core::bind` surface). `params_json` is a JSON array (nullable for
/// no placeholders); marker objects (`$dec`/`$bytes`/`$ts`/`$float`) decode
/// exactly like the wire protocol. `timeout_ms` bounds the transaction-owner
/// wait and the read deadline (`<= 0` = 30 s wait, no read deadline).
///
/// On success `out_json` receives the owned payload (`{"affected":n}` or the
/// RESP_ROWS-shaped JSON); on error it receives the owned message. The caller
/// must free it with [`docsql_embedded_free`].
///
/// # Safety
/// `handle` must be live; `sql`/`params_json` must be valid NUL-terminated
/// UTF-8; `out_json` must be a valid writable pointer (non-null).
#[no_mangle]
pub unsafe extern "C" fn docsql_embedded_execute(
    handle: *mut Session,
    sql: *const c_char,
    params_json: *const c_char,
    timeout_ms: i32,
    out_json: *mut *mut c_char,
) -> i32 {
    guard(out_json, || {
        let Some(session) = handle.as_ref() else {
            if !out_json.is_null() {
                unsafe { set_out(out_json, "execute: null session handle".into()) };
            }
            return 2;
        };
        let Some(sql) = (!sql.is_null()).then(|| CStr::from_ptr(sql)) else {
            unsafe { set_out(out_json, "execute: null SQL".into()) };
            return 2;
        };
        let sql = match sql.to_str() {
            Ok(s) => s,
            Err(_) => {
                unsafe { set_out(out_json, "execute: SQL is not valid UTF-8".into()) };
                return 2;
            }
        };
        let params = if params_json.is_null() {
            None
        } else {
            match CStr::from_ptr(params_json).to_str() {
                Ok(p) => Some(p),
                Err(_) => {
                    unsafe { set_out(out_json, "execute: params JSON is not UTF-8".into()) };
                    return 2;
                }
            }
        };
        match execute_in_shared(&session.shared, session.token, sql, params, timeout_ms) {
            Ok(payload) => {
                if !out_json.is_null() {
                    unsafe { set_out(out_json, payload) };
                }
                0
            }
            Err((code, msg)) => {
                let msg = match &session.path {
                    Some(p) if code == 5 => format!("{msg} (database: {})", p.display()),
                    _ => msg,
                };
                if !out_json.is_null() {
                    unsafe { set_out(out_json, msg) };
                }
                code
            }
        }
    })
}

/// Free a string returned through an out-pointer by this library.
///
/// # Safety
/// `text` must be a pointer produced by this library (or null).
#[no_mangle]
pub unsafe extern "C" fn docsql_embedded_free(text: *mut c_char) {
    if !text.is_null() {
        drop(unsafe { CString::from_raw(text) });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session_for(path: &str) -> Box<Session> {
        let shared = open_shared(path).expect("open");
        let mut s = Box::new(Session {
            shared,
            token: 0,
            path: Some(std::path::PathBuf::from(path)),
        });
        s.token = &*s as *const Session as usize;
        s
    }

    fn run(s: &Session, sql: &str) -> Result<String, (i32, String)> {
        execute_in_shared(&s.shared, s.token, sql, None, 500)
    }

    fn run_params(s: &Session, sql: &str, params: &str) -> Result<String, (i32, String)> {
        execute_in_shared(&s.shared, s.token, sql, Some(params), 500)
    }

    #[test]
    fn crud_survives_close_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.db");
        let p = path.to_str().unwrap();
        {
            let s = session_for(p);
            run(
                &s,
                "CREATE TABLE t (id INT AUTOINCREMENT PRIMARY KEY, name VARCHAR)",
            )
            .unwrap();
            run(&s, "INSERT INTO t (name) VALUES ('a')").unwrap();
            let rows = run(&s, "SELECT id, name FROM t").unwrap();
            assert!(rows.contains("\"name\""), "{rows}");
            assert!(rows.contains("\"a\""), "{rows}");
        }
        // Reopen from scratch: WAL replay must recover the committed writes.
        let s = session_for(p);
        let rows = run(&s, "SELECT COUNT(*) AS n FROM t").unwrap();
        assert!(rows.contains("[1]"), "{rows}");
    }

    #[test]
    fn two_sessions_share_one_engine_per_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shared.db");
        let p = path.to_str().unwrap();
        let a = session_for(p);
        let b = session_for(p);
        run(
            &a,
            "CREATE TABLE t (id INT AUTOINCREMENT PRIMARY KEY, v INT)",
        )
        .unwrap();
        run(&a, "INSERT INTO t (v) VALUES (7)").unwrap();
        let rows = run(&b, "SELECT v FROM t").unwrap();
        assert!(rows.contains("[7]"), "{rows}");
    }

    #[test]
    fn memory_sessions_are_isolated() {
        let a = session_for(":memory:");
        let b = session_for(":memory:");
        run(&a, "CREATE TABLE only_a (id INT)").unwrap();
        let err = run(&b, "SELECT * FROM only_a").unwrap_err();
        assert_eq!(err.0, 1, "{err:?}");
    }

    #[test]
    fn bound_values_are_data_only() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("params.db");
        let p = path.to_str().unwrap();
        let s = session_for(p);
        run(
            &s,
            "CREATE TABLE t (id INT AUTOINCREMENT PRIMARY KEY, v VARCHAR)",
        )
        .unwrap();
        run_params(
            &s,
            "INSERT INTO t (v) VALUES (?)",
            r#"["x'; DROP TABLE t; --"]"#,
        )
        .unwrap();
        let rows = run(&s, "SELECT v FROM t").unwrap();
        assert!(rows.contains("DROP TABLE t"), "{rows}");
        let n = run(&s, "SELECT COUNT(*) AS n FROM t").unwrap();
        assert!(n.contains("[1]"), "{n}");
    }

    #[test]
    fn transaction_owner_sees_own_writes_and_others_wait() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tx.db");
        let p = path.to_str().unwrap();
        let a = session_for(p);
        let b = session_for(p);
        run(
            &a,
            "CREATE TABLE t (id INT AUTOINCREMENT PRIMARY KEY, v INT)",
        )
        .unwrap();
        run(&a, "BEGIN").unwrap();
        run(&a, "INSERT INTO t (v) VALUES (1)").unwrap();
        // Owner's SELECT inside the transaction sees the uncommitted row.
        let own = run(&a, "SELECT COUNT(*) AS n FROM t").unwrap();
        assert!(own.contains("[1]"), "{own}");
        // A second session cannot interleave a write into the open unit.
        let err = execute_in_shared(
            &b.shared,
            b.token,
            "INSERT INTO t (v) VALUES (2)",
            None,
            100,
        )
        .unwrap_err();
        assert_eq!(err.0, 4, "{err:?}");
        run(&a, "COMMIT").unwrap();
        let visible = run(&b, "SELECT COUNT(*) AS n FROM t").unwrap();
        assert!(visible.contains("[1]"), "{visible}");
        run(&b, "INSERT INTO t (v) VALUES (2)").unwrap();
    }

    #[test]
    fn rollback_hides_writes_from_the_owner_afterwards() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rollback.db");
        let p = path.to_str().unwrap();
        let s = session_for(p);
        run(
            &s,
            "CREATE TABLE t (id INT AUTOINCREMENT PRIMARY KEY, v INT)",
        )
        .unwrap();
        run(&s, "BEGIN").unwrap();
        run(&s, "INSERT INTO t (v) VALUES (9)").unwrap();
        run(&s, "ROLLBACK").unwrap();
        let n = run(&s, "SELECT COUNT(*) AS n FROM t").unwrap();
        assert!(n.contains("[0]"), "{n}");
    }

    #[test]
    fn result_payload_matches_the_wire_shape() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shape.db");
        let p = path.to_str().unwrap();
        let s = session_for(p);
        run(&s, "CREATE TABLE t (d DECIMAL, b BLOB)").unwrap();
        run(
            &s,
            "INSERT INTO t (d, b) VALUES (CAST('1.25' AS DECIMAL), x'0aff')",
        )
        .unwrap();
        let rows = run(&s, "SELECT d, b FROM t").unwrap();
        // Exact scalar markers, identical to the server's RESP_ROWS payload.
        assert!(rows.contains(r#"{"$dec":"1.25"}"#), "{rows}");
        assert!(rows.contains(r#"{"$bytes":[10,255]}"#), "{rows}");
    }

    #[test]
    fn c_abi_roundtrip_error_codes_and_abi_version() {
        let dir = tempfile::tempdir().unwrap();
        let path = CString::new(dir.path().join("capi.db").to_str().unwrap()).unwrap();
        let mut handle: *mut Session = std::ptr::null_mut();
        let mut err: *mut c_char = std::ptr::null_mut();
        assert_eq!(
            unsafe { docsql_embedded_open(path.as_ptr(), &mut handle, &mut err) },
            0
        );
        assert!(!handle.is_null());
        assert_eq!(docsql_embedded_abi_version(), 1);

        // Usage errors are loud and never touch a null handle.
        let mut h2: *mut Session = std::ptr::null_mut();
        let mut e2: *mut c_char = std::ptr::null_mut();
        assert_eq!(
            unsafe { docsql_embedded_open(std::ptr::null(), &mut h2, &mut e2) },
            2
        );
        assert_eq!(
            unsafe { CStr::from_ptr(e2) }.to_str().unwrap(),
            "open: null path"
        );
        unsafe { docsql_embedded_free(e2) };

        let mut out: *mut c_char = std::ptr::null_mut();
        assert_eq!(
            unsafe {
                docsql_embedded_execute(
                    std::ptr::null_mut(),
                    path.as_ptr(),
                    std::ptr::null(),
                    0,
                    &mut out,
                )
            },
            2
        );
        assert!(unsafe { CStr::from_ptr(out) }
            .to_str()
            .unwrap()
            .contains("null session"));
        unsafe { docsql_embedded_free(out) };

        let create =
            CString::new("CREATE TABLE t (id INT AUTOINCREMENT PRIMARY KEY, v VARCHAR)").unwrap();
        let mut out: *mut c_char = std::ptr::null_mut();
        assert_eq!(
            unsafe {
                docsql_embedded_execute(handle, create.as_ptr(), std::ptr::null(), 100, &mut out)
            },
            0
        );
        assert_eq!(
            unsafe { CStr::from_ptr(out) }.to_str().unwrap(),
            "{\"affected\":0}"
        );
        unsafe { docsql_embedded_free(out) };

        let bad = CString::new("SELECT * FROM nope").unwrap();
        let mut out: *mut c_char = std::ptr::null_mut();
        assert_eq!(
            unsafe {
                docsql_embedded_execute(handle, bad.as_ptr(), std::ptr::null(), 100, &mut out)
            },
            1
        );
        assert!(unsafe { CStr::from_ptr(out) }
            .to_str()
            .unwrap()
            .contains("nope"));
        unsafe { docsql_embedded_free(out) };

        unsafe { docsql_embedded_close(handle) };
        unsafe { docsql_embedded_close(std::ptr::null_mut()) };
    }
}
