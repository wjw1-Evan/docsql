//! Query log: server-side statement audit trail.
//!
//! Every executed SQL statement is recorded (timestamp, client, latency,
//! affected rows, error) into an in-memory ring buffer, queryable as the
//! `docsql_log` system view over the normal protocol. Slow statements
//! (>= DOCSQL_SLOW_MS, default 100ms) are logged to stderr; setting
//! DOCSQL_LOG_FILE additionally appends every entry as JSONL.
//!
//! The [`SyncLog`] next door records replication/sync events (write
//! fan-out per target, pub/sub fan-out, PROMOTE) the same way. Both rings
//! feed the REQ_LOGS frame — the data source of the web console's logs
//! page — via [`logs_payload`].

use crate::ServerState;
use docsql_core::json;
use docsql_core::proto;
use docsql_core::value::{Object, Value};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct LogEntry {
    pub ts_ms: u64,
    pub peer: String,
    pub sql: String,
    pub ms: f64,
    pub affected: Option<i64>,
    pub error: Option<String>,
    pub replicated: bool,
}

pub struct QueryLog {
    ring: Mutex<VecDeque<LogEntry>>,
    /// JSONL audit sink when DOCSQL_LOG_FILE is set. Opened lazily and held
    /// for the process lifetime — the old open/append/close per entry put a
    /// full file-open syscall on every statement's critical path.
    sink: Mutex<Option<std::fs::File>>,
    /// Set once a sink problem has been reported; a broken audit sink must
    /// at least be visible, not silently produce no trail.
    sink_warned: std::sync::atomic::AtomicBool,
    /// Earliest next attempt to (re)open a failed sink: a bad path or a
    /// full disk must not turn every statement into an open() syscall, but
    /// the audit trail also must not stay latched off forever after one
    /// transient error.
    sink_retry_after: Mutex<Option<std::time::Instant>>,
    pub capacity: usize,
    pub slow_ms: f64,
    pub log_file: Option<String>,
}

impl Default for QueryLog {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryLog {
    pub fn new() -> QueryLog {
        QueryLog {
            ring: Mutex::new(VecDeque::new()),
            sink: Mutex::new(None),
            sink_warned: std::sync::atomic::AtomicBool::new(false),
            sink_retry_after: Mutex::new(None),
            capacity: 1000,
            // Fail fast on a malformed threshold (consistent with the
            // server binary's numeric env validation): a typo would
            // otherwise silently disable slow-query logging.
            slow_ms: match std::env::var("DOCSQL_SLOW_MS") {
                Ok(v) if !v.trim().is_empty() => match v.trim().parse() {
                    Ok(x) => x,
                    Err(_) => {
                        eprintln!("refusing to start: DOCSQL_SLOW_MS: invalid number {v:?}");
                        std::process::exit(2);
                    }
                },
                _ => 100.0,
            },
            log_file: std::env::var("DOCSQL_LOG_FILE")
                .ok()
                .filter(|v| !v.is_empty()),
        }
    }

    pub fn push(&self, e: LogEntry) {
        if e.ms >= self.slow_ms {
            eprintln!("slow query ({:.1} ms): {}", e.ms, e.sql);
        }
        if let Some(path) = &self.log_file {
            if let Ok(line) = serde_json::to_string(&serde_json::json!({
                "ts_ms": e.ts_ms, "peer": e.peer, "sql": e.sql,
                "ms": e.ms, "affected": e.affected,
                "error": e.error, "replicated": e.replicated,
            })) {
                let mut sink = self.sink.lock().unwrap_or_else(|p| p.into_inner());
                if sink.is_none() {
                    let now = std::time::Instant::now();
                    let retry_ok = self
                        .sink_retry_after
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .is_none_or(|t| now >= t);
                    if retry_ok {
                        *sink = std::fs::OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open(path)
                            .ok();
                        if sink.is_none() {
                            *self
                                .sink_retry_after
                                .lock()
                                .unwrap_or_else(|p| p.into_inner()) =
                                Some(now + std::time::Duration::from_secs(10));
                            if !self
                                .sink_warned
                                .swap(true, std::sync::atomic::Ordering::Relaxed)
                            {
                                eprintln!("query log file {path}: cannot open for append");
                            }
                        }
                    }
                }
                if let Some(f) = sink.as_mut() {
                    use std::io::Write;
                    if let Err(err) = writeln!(f, "{line}") {
                        // Drop the handle so the next push reopens; retry
                        // slowly and keep the failure visible.
                        *sink = None;
                        *self
                            .sink_retry_after
                            .lock()
                            .unwrap_or_else(|p| p.into_inner()) =
                            Some(std::time::Instant::now() + std::time::Duration::from_secs(10));
                        if !self
                            .sink_warned
                            .swap(true, std::sync::atomic::Ordering::Relaxed)
                        {
                            eprintln!("query log file {path}: {err}");
                        }
                    } else {
                        // Healthy again: re-arm the one-shot warning.
                        self.sink_warned
                            .store(false, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }
        }
        let mut ring = self.ring.lock().unwrap_or_else(|p| p.into_inner());
        if ring.len() == self.capacity {
            ring.pop_front();
        }
        ring.push_back(e);
    }

    pub fn snapshot(&self) -> Vec<LogEntry> {
        self.ring
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .cloned()
            .collect()
    }
}

/// Log one executed statement from its request/response frames.
pub fn record(
    state: &Arc<ServerState>,
    peer: &str,
    sql: &str,
    ms: f64,
    resp: &crate::Frame,
    is_replication: bool,
) {
    let affected = if resp.frame_type == proto::RESP_AFFECTED && resp.payload.len() == 8 {
        Some(i64::from_le_bytes(resp.payload[..8].try_into().unwrap()))
    } else {
        None
    };
    let error = if resp.frame_type == proto::RESP_ERROR {
        Some(String::from_utf8_lossy(&resp.payload).to_string())
    } else {
        None
    };
    // Plaintext passwords (CREATE/ALTER USER) must never land in the audit
    // trail; pre-hashed replays are left as-is.
    let redacted = docsql_core::useradmin::redact_sql(sql);
    state.query_log.push(LogEntry {
        ts_ms: docsql_core::now_ms(),
        peer: peer.to_string(),
        sql: redacted.chars().take(512).collect(),
        ms,
        affected,
        error,
        replicated: is_replication,
    });
}

/// Word tokens (identifier-ish runs) outside string literals and comments,
/// as byte spans into the lowercased SQL.
fn word_tokens(lower: &str) -> Vec<(usize, usize)> {
    let b = lower.as_bytes();
    let is_word = |c: u8| c.is_ascii_alphanumeric() || c == b'_' || c == b'$';
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < b.len() {
        let c = b[i];
        if c == b'\'' {
            let (end, _) = docsql_core::stmt::sql_literal_end(lower, i);
            i = end;
        } else if c == b'-' && b.get(i + 1) == Some(&b'-') {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
        } else if c == b'/' && b.get(i + 1) == Some(&b'*') {
            i += 2;
            while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(b.len());
        } else if is_word(c) {
            let start = i;
            while i < b.len() && is_word(b[i]) {
                i += 1;
            }
            out.push((start, i));
        } else {
            i += 1;
        }
    }
    out
}

/// Serve `SELECT ... FROM docsql_log` from the ring buffer.
/// Returns None when the SQL does not target the log view.
pub(crate) fn try_serve_log_view(
    sql: &str,
    state: &Arc<ServerState>,
    user: Option<&crate::UserAuth>,
) -> Option<crate::Frame> {
    // The audit log carries other users' statement text (only PASSWORD
    // literals are redacted), so a non-admin login must not harvest it.
    // Returning None drops the query into the normal path, where
    // docsql_log simply does not exist for them.
    if user.is_some_and(|u| !u.grants.admin) {
        return None;
    }
    let lower = sql.to_lowercase();
    if !lower.trim_start().starts_with("select") {
        return None;
    }
    // The view is served only when docsql_log is the FROM target: a query
    // like `WHERE note = 'docsql_log'` (or a same-named column of another
    // table) must reach the engine untouched, not swap in log rows.
    let toks = word_tokens(&lower);
    let words: Vec<&str> = toks.iter().map(|(a, b)| &lower[*a..*b]).collect();
    if !words
        .windows(2)
        .any(|w| w[0] == "from" && w[1] == "docsql_log")
    {
        return None;
    }
    // Best-effort LIMIT n support; default 100 most-recent rows.
    let limit = parse_limit(&words).unwrap_or(100);

    let mut docs: Vec<Object> = Vec::new();
    for e in state.query_log.snapshot().iter().rev().take(limit) {
        let mut o = Object::new();
        o.insert("ts_ms".into(), Value::Int(e.ts_ms as i64));
        o.insert("peer".into(), Value::Str(e.peer.clone()));
        o.insert("sql".into(), Value::Str(e.sql.clone()));
        o.insert("ms".into(), Value::Float(e.ms));
        o.insert(
            "affected".into(),
            e.affected.map(Value::Int).unwrap_or(Value::Null),
        );
        o.insert(
            "error".into(),
            e.error.clone().map(Value::Str).unwrap_or(Value::Null),
        );
        o.insert("replicated".into(), Value::Bool(e.replicated));
        docs.push(o);
    }
    const COLS: [&str; 7] = [
        "ts_ms",
        "peer",
        "sql",
        "ms",
        "affected",
        "error",
        "replicated",
    ];
    let mut obj = Object::new();
    obj.insert(
        "columns".into(),
        Value::Array(COLS.iter().map(|c| Value::Str((*c).into())).collect()),
    );
    let rows: Vec<Value> = docs
        .iter()
        .map(|d| {
            Value::Array(
                COLS.iter()
                    .map(|c| d.get(*c).cloned().unwrap_or(Value::Null))
                    .collect(),
            )
        })
        .collect();
    obj.insert("rows".into(), Value::Array(rows));
    Some(crate::Frame::new(
        proto::RESP_ROWS,
        json::to_string(&Value::Object(obj)).into_bytes(),
    ))
}

fn parse_limit(words: &[&str]) -> Option<usize> {
    // Token-based: a literal containing "limit" must not supply the value.
    let idx = words.iter().rposition(|w| *w == "limit")?;
    words.get(idx + 1)?.parse().ok()
}

// ---------------------------------------------------------------------------
// Sync log: replication/sync event trail.
// ---------------------------------------------------------------------------

/// One replication/sync event: a write fanned out to one target, a
/// pub/sub frame forwarded, a PROMOTE, ... Inbound replication applies
/// are not here — they are ordinary statements and show up in the query
/// log with `replicated = true`.
#[derive(Clone)]
pub struct SyncEntry {
    pub ts_ms: u64,
    /// Event kind: "forward" (SQL write fan-out), "publish"/"trim"
    /// (pub/sub fan-out), "promote".
    pub event: String,
    /// Fan-out target (host:port); empty for node-local events.
    pub target: String,
    /// Statement excerpt for SQL fan-out events.
    pub sql: Option<String>,
    pub ok: bool,
    /// Failure detail (error text) when `ok` is false.
    pub detail: Option<String>,
}

pub struct SyncLog {
    ring: Mutex<VecDeque<SyncEntry>>,
    pub capacity: usize,
}

impl SyncLog {
    pub fn new(capacity: usize) -> SyncLog {
        SyncLog {
            ring: Mutex::new(VecDeque::new()),
            capacity,
        }
    }

    pub fn push(&self, e: SyncEntry) {
        let mut ring = self.ring.lock().unwrap_or_else(|p| p.into_inner());
        if ring.len() == self.capacity {
            ring.pop_front();
        }
        ring.push_back(e);
    }

    pub fn snapshot(&self) -> Vec<SyncEntry> {
        self.ring
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .cloned()
            .collect()
    }
}

/// Log one sync event with the shared timestamp source.
pub fn sync_event(
    log: &SyncLog,
    event: &str,
    target: &str,
    sql: Option<&str>,
    ok: bool,
    detail: Option<String>,
) {
    log.push(SyncEntry {
        ts_ms: docsql_core::now_ms(),
        event: event.to_string(),
        target: target.to_string(),
        sql: sql.map(|s| s.chars().take(200).collect()),
        ok,
        detail,
    });
}

/// Default / maximum entries per section served by REQ_LOGS.
pub const LOGS_DEFAULT_LIMIT: usize = 200;
pub const LOGS_MAX_LIMIT: usize = 1000;

/// Parse the optional REQ_LOGS payload `{"limit": n}`; empty or malformed
/// payloads fall back to the default.
pub fn parse_logs_limit(payload: &[u8]) -> usize {
    serde_json::from_slice::<serde_json::Value>(payload)
        .ok()
        .and_then(|v| v["limit"].as_u64())
        .map(|n| n as usize)
        .unwrap_or(LOGS_DEFAULT_LIMIT)
        .clamp(1, LOGS_MAX_LIMIT)
}

/// Assemble the REQ_LOGS payload: the newest `limit` entries of each
/// ring, newest first. The web console's embedded engine reuses this for
/// its local section (its sync ring is always empty).
pub fn logs_payload(query: &QueryLog, sync: &SyncLog, limit: usize) -> Vec<u8> {
    let query: Vec<serde_json::Value> = query
        .snapshot()
        .iter()
        .rev()
        .take(limit)
        .map(|e| {
            serde_json::json!({
                "ts_ms": e.ts_ms, "peer": e.peer, "sql": e.sql,
                "ms": e.ms, "affected": e.affected,
                "error": e.error, "replicated": e.replicated,
            })
        })
        .collect();
    let sync: Vec<serde_json::Value> = sync
        .snapshot()
        .iter()
        .rev()
        .take(limit)
        .map(|e| {
            serde_json::json!({
                "ts_ms": e.ts_ms, "event": e.event, "target": e.target,
                "sql": e.sql, "ok": e.ok, "detail": e.detail,
            })
        })
        .collect();
    serde_json::to_vec(&serde_json::json!({"query": query, "sync": sync})).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(ts_ms: u64, sql: &str) -> LogEntry {
        LogEntry {
            ts_ms,
            peer: "a".into(),
            sql: sql.into(),
            ms: 1.0,
            affected: Some(1),
            error: None,
            replicated: false,
        }
    }

    #[test]
    fn sync_ring_evicts_oldest_beyond_capacity() {
        let log = SyncLog::new(2);
        for i in 0..3 {
            sync_event(
                &log,
                "forward",
                &format!("node-{i}"),
                Some("INSERT"),
                true,
                None,
            );
        }
        let snap = log.snapshot();
        assert_eq!(snap.len(), 2);
        assert_eq!(snap[0].target, "node-1");
        assert_eq!(snap[1].target, "node-2");
    }

    #[test]
    fn sync_event_truncates_sql_and_carries_detail() {
        let log = SyncLog::new(4);
        let long = "X".repeat(500);
        sync_event(
            &log,
            "forward",
            "peer:7600",
            Some(&long),
            false,
            Some("refused".into()),
        );
        let e = &log.snapshot()[0];
        assert_eq!(e.sql.as_ref().unwrap().len(), 200);
        assert!(!e.ok);
        assert_eq!(e.detail.as_deref(), Some("refused"));
    }

    #[test]
    fn logs_payload_newest_first_and_limited() {
        let q = QueryLog::new();
        q.push(entry(1, "first"));
        q.push(entry(2, "second"));
        let s = SyncLog::new(4);
        sync_event(&s, "promote", "", None, true, None);
        let v: serde_json::Value = serde_json::from_slice(&logs_payload(&q, &s, 1)).unwrap();
        let query = v["query"].as_array().unwrap();
        assert_eq!(query.len(), 1);
        assert_eq!(query[0]["sql"], "second"); // newest first
        assert_eq!(v["sync"][0]["event"], "promote");
        assert_eq!(v["sync"][0]["ok"], true);
    }

    #[test]
    fn logs_limit_defaults_and_clamps() {
        assert_eq!(parse_logs_limit(b""), LOGS_DEFAULT_LIMIT);
        assert_eq!(parse_logs_limit(b"garbage"), LOGS_DEFAULT_LIMIT);
        assert_eq!(parse_logs_limit(br#"{"limit": 5}"#), 5);
        assert_eq!(parse_logs_limit(br#"{"limit": 999999}"#), LOGS_MAX_LIMIT);
        assert_eq!(parse_logs_limit(br#"{"limit": 0}"#), 1);
    }

    fn log_with_sink(log_file: Option<String>) -> QueryLog {
        QueryLog {
            ring: Mutex::new(VecDeque::new()),
            sink: Mutex::new(None),
            sink_warned: std::sync::atomic::AtomicBool::new(false),
            sink_retry_after: Mutex::new(None),
            capacity: 10,
            slow_ms: 1000.0,
            log_file,
        }
    }

    #[test]
    fn jsonl_sink_appends_every_entry_and_keeps_the_handle_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");
        let q = log_with_sink(Some(path.to_string_lossy().to_string()));
        q.push(entry(7, "SELECT 1"));
        q.push(entry(8, "SELECT 2"));
        let text = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        // The lazy-open sink stays open across pushes: two appends, no truncate.
        assert_eq!(lines.len(), 2);
        let v: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(v["ts_ms"], 7);
        assert_eq!(v["peer"], "a");
        assert_eq!(v["sql"], "SELECT 1");
        assert_eq!(v["ms"], 1.0);
        assert_eq!(v["affected"], 1);
        assert_eq!(v["error"], serde_json::Value::Null);
        assert_eq!(v["replicated"], false);
        assert!(text.contains("SELECT 2"), "{text}");
    }

    #[test]
    fn jsonl_sink_open_failure_keeps_the_ring_and_warns() {
        let dir = tempfile::tempdir().unwrap();
        // A directory can never be opened as an append sink.
        let q = log_with_sink(Some(dir.path().to_string_lossy().to_string()));
        q.push(entry(1, "SELECT 1"));
        q.push(entry(2, "SELECT 2"));
        // A broken audit sink must not lose the in-memory trail, and the
        // failure is latched so it is reported once, not per statement.
        assert_eq!(q.snapshot().len(), 2);
        assert!(q.sink_warned.load(std::sync::atomic::Ordering::Relaxed));
    }

    #[test]
    fn word_tokens_skip_literals_comments_and_unterminated_blocks() {
        let sql = "select 'a b -- not a comment' from /* drop table x */ t -- tail";
        let toks = word_tokens(sql);
        let words: Vec<&str> = toks.iter().map(|(s, e)| &sql[*s..*e]).collect();
        assert_eq!(words, vec!["select", "from", "t"]);
        // An unterminated block comment consumes the remainder safely.
        let sql = "select /* never closed";
        let toks = word_tokens(sql);
        let words: Vec<&str> = toks.iter().map(|(s, e)| &sql[*s..*e]).collect();
        assert_eq!(words, vec!["select"]);
    }

    #[test]
    fn querylog_new_reads_env_and_defaults() {
        // 默认形状:1000 容量 / 100ms 慢查询阈值 / 无审计文件。
        std::env::remove_var("DOCSQL_SLOW_MS");
        std::env::remove_var("DOCSQL_LOG_FILE");
        let q = QueryLog::new();
        assert_eq!(q.capacity, 1000);
        assert_eq!(q.slow_ms, 100.0);
        assert!(q.log_file.is_none());
        assert_eq!(QueryLog::default().capacity, 1000);
        // 合法环境注入覆盖两处 env 分支(非法值分支是 exit(2),不在测试面)。
        std::env::set_var("DOCSQL_SLOW_MS", "250");
        std::env::set_var("DOCSQL_LOG_FILE", "/tmp/docsql-test-audit.jsonl");
        let q = QueryLog::new();
        assert_eq!(q.slow_ms, 250.0);
        assert_eq!(q.log_file.as_deref(), Some("/tmp/docsql-test-audit.jsonl"));
        std::env::remove_var("DOCSQL_SLOW_MS");
        std::env::remove_var("DOCSQL_LOG_FILE");
    }

    #[test]
    fn slow_entries_report_to_stderr_and_ring_evicts() {
        // 超阈值语句走慢查询 stderr 分支;环形缓冲按容量驱逐最旧。
        let q = QueryLog {
            ring: Mutex::new(VecDeque::new()),
            sink: Mutex::new(None),
            sink_warned: std::sync::atomic::AtomicBool::new(false),
            sink_retry_after: Mutex::new(None),
            capacity: 2,
            slow_ms: 0.5,
            log_file: None,
        };
        q.push(entry(1, "slow one"));
        q.push(entry(2, "slow two"));
        q.push(entry(3, "slow three"));
        let snap = q.snapshot();
        assert_eq!(snap.len(), 2);
        assert_eq!(snap[0].sql, "slow two");
        assert_eq!(snap[1].sql, "slow three");
    }
}
