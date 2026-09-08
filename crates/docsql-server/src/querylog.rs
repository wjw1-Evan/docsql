//! Query log: server-side statement audit trail.
//!
//! Every executed SQL statement is recorded (timestamp, client, latency,
//! affected rows, error) into an in-memory ring buffer, queryable as the
//! `docsql_log` system view over the normal protocol. Slow statements
//! (>= DOCSQL_SLOW_MS, default 100ms) are logged to stderr; setting
//! DOCSQL_LOG_FILE additionally appends every entry as JSONL.

use crate::ServerState;
use docsql_core::json;
use docsql_core::proto;
use docsql_core::value::{Object, Value};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

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
            capacity: 1000,
            slow_ms: std::env::var("DOCSQL_SLOW_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(100.0),
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
                use std::io::Write;
                if let Ok(mut f) = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)
                {
                    let _ = writeln!(f, "{line}");
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

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
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
    state.query_log.push(LogEntry {
        ts_ms: now_ms(),
        peer: peer.to_string(),
        sql: sql.chars().take(512).collect(),
        ms,
        affected,
        error,
        replicated: is_replication,
    });
}

/// Serve `SELECT ... FROM docsql_log` from the ring buffer.
/// Returns None when the SQL does not target the log view.
pub fn try_serve_log_view(sql: &str, state: &Arc<ServerState>) -> Option<crate::Frame> {
    let lower = sql.to_lowercase();
    if !lower.contains("docsql_log") || !lower.trim_start().starts_with("select") {
        return None;
    }
    // Best-effort LIMIT n support; default 100 most-recent rows.
    let limit = parse_limit(&lower).unwrap_or(100);

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
    let mut obj = Object::new();
    obj.insert(
        "columns".into(),
        Value::Array(
            [
                "ts_ms",
                "peer",
                "sql",
                "ms",
                "affected",
                "error",
                "replicated",
            ]
            .iter()
            .map(|c| Value::Str((*c).into()))
            .collect(),
        ),
    );
    let cols = [
        "ts_ms",
        "peer",
        "sql",
        "ms",
        "affected",
        "error",
        "replicated",
    ];
    let rows: Vec<Value> = docs
        .iter()
        .map(|d| {
            Value::Array(
                cols.iter()
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

fn parse_limit(lower: &str) -> Option<usize> {
    let idx = lower.rfind("limit")?;
    let rest: String = lower[idx + 5..]
        .chars()
        .skip_while(|c| c.is_whitespace())
        .take_while(|c| c.is_ascii_digit())
        .collect();
    rest.parse().ok()
}
