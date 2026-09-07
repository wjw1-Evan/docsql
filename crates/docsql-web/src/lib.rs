//! docsql Studio — web console backend (SSMS-style management UI).
//!
//! REST API over the shared engine:
//! - `GET  /`                embedded single-page console
//! - `POST /api/sql`         {sql} batch → results / affected / error
//!   (single-statement responses keep the legacy shape)
//! - `POST /api/parse`       {sql} parse-check without executing
//! - `GET  /api/meta`        object-explorer metadata (tables/columns/keys/
//!   indexes/row counts + KV summary + storage stats)
//! - `GET  /api/keys`        KV key browser (type / like / limit filters)
//! - `GET  /api/kvkey`       ?key= one KV entry, value decoded per type
//! - `POST /api/kv`          {command, args} KV command dispatch
//! - `GET  /api/stats`       server + storage + data counters
//! - `POST /api/publish`     {channel, message} console-side publish monitor
//! - `GET  /api/events`      ?after=seq recent publish events (polling)
//!
//! Auth v1: requests must send `X-Docsql-Token` when the server token is set.

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Html;
use axum::routing::{get, post};
use axum::{Json, Router};
use docsql_core::engine::{Database, ExecOutcome};
use docsql_core::json;
use docsql_core::value::Value;
use docsql_kv::{Kv, SetOpts};
use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

pub const CONSOLE_VERSION: &str = "1.0";

/// Keep at most this many publish events for the console monitor.
const EVENT_LOG_CAP: usize = 200;

pub struct WebState {
    pub kv: Mutex<Kv>,
    pub token: Option<String>,
    pub db_path: PathBuf,
    pub started: Instant,
    pub events: Mutex<EventLog>,
}

pub struct WebConfig {
    pub db_path: PathBuf,
    pub token: Option<String>,
}

/// Ring buffer of console-published messages (the "publish monitor" feed).
#[derive(Default)]
pub struct EventLog {
    seq: u64,
    items: VecDeque<PubEvent>,
}

#[derive(Clone)]
pub struct PubEvent {
    pub seq: u64,
    pub channel: String,
    pub payload: String,
    pub at_ms: i64,
}

impl EventLog {
    fn push(&mut self, channel: &str, payload: &str) -> u64 {
        self.seq += 1;
        self.items.push_back(PubEvent {
            seq: self.seq,
            channel: channel.into(),
            payload: payload.into(),
            at_ms: docsql_kv::now_ms(),
        });
        while self.items.len() > EVENT_LOG_CAP {
            self.items.pop_front();
        }
        self.seq
    }

    fn since(&self, after: u64) -> Vec<&PubEvent> {
        self.items.iter().filter(|e| e.seq > after).collect()
    }
}

pub async fn run(cfg: WebConfig, listen: &str) -> std::io::Result<()> {
    let kv = Kv::open(&cfg.db_path).map_err(|e| std::io::Error::other(format!("open db: {e}")))?;
    let state = Arc::new(WebState {
        kv: Mutex::new(kv),
        token: cfg.token,
        db_path: cfg.db_path,
        started: Instant::now(),
        events: Mutex::new(EventLog::default()),
    });
    let app = Router::new()
        .route("/", get(index))
        .route("/api/sql", post(api_sql))
        .route("/api/parse", post(api_parse))
        .route("/api/meta", get(api_meta))
        .route("/api/keys", get(api_keys))
        .route("/api/kvkey", get(api_kvkey))
        .route("/api/kv", post(api_kv))
        .route("/api/stats", get(api_stats))
        .route("/api/publish", post(api_publish))
        .route("/api/events", get(api_events))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(listen).await?;
    axum::serve(listener, app)
        .await
        .map_err(std::io::Error::other)
}

fn check_auth(state: &WebState, headers: &HeaderMap) -> Option<StatusCode> {
    let token = headers.get("X-Docsql-Token").and_then(|v| v.to_str().ok());
    match &state.token {
        None => None,
        Some(expect) if token == Some(expect.as_str()) => None,
        Some(_) => Some(StatusCode::UNAUTHORIZED),
    }
}

async fn index() -> Html<&'static str> {
    Html(include_str!("console.html"))
}

#[derive(serde::Deserialize)]
struct SqlBody {
    sql: String,
}

/// Run a batch and shape the JSON response. Single-statement batches keep the
/// legacy one-result shape; multi-statement batches return `kind:"batch"`.
pub fn run_sql(kv: &mut Kv, sql: &str) -> serde_json::Value {
    let batch = kv.db.execute_batch(sql);
    // Single-statement batches (including failures) keep the legacy shape.
    if batch.statements <= 1 {
        if let Some(e) = &batch.error {
            return serde_json::json!({"kind": "error", "message": e.message});
        }
        if let Some(o) = batch.outcomes.first() {
            return outcome_json(o);
        }
    }
    serde_json::json!({
        "kind": "batch",
        "results": batch.outcomes.iter().map(outcome_json).collect::<Vec<_>>(),
        "error": batch.error.as_ref().map(|e| serde_json::json!({
            "statement": e.statement,
            "message": e.message,
        })),
    })
}

fn outcome_json(o: &ExecOutcome) -> serde_json::Value {
    match o {
        ExecOutcome::Rows(r) => serde_json::json!({
            "kind": "rows",
            "columns": r.columns,
            "rows": r.rows.iter().map(|row| row.iter().map(value_json).collect::<Vec<_>>()).collect::<Vec<_>>(),
        }),
        ExecOutcome::Affected(n) => {
            serde_json::json!({"kind": "affected", "count": n})
        }
    }
}

pub fn value_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::json!(i),
        Value::Float(f) => serde_json::json!(f),
        Value::Str(s) => serde_json::json!(s),
        other => serde_json::json!(other.to_string()),
    }
}

async fn api_sql(
    State(state): State<Arc<WebState>>,
    headers: HeaderMap,
    Json(body): Json<SqlBody>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if let Some(code) = check_auth(&state, &headers) {
        return Err(code);
    }
    let mut kv = state.kv.lock().unwrap();
    Ok(Json(run_sql(&mut kv, &body.sql)))
}

async fn api_parse(
    State(state): State<Arc<WebState>>,
    headers: HeaderMap,
    Json(body): Json<SqlBody>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if let Some(code) = check_auth(&state, &headers) {
        return Err(code);
    }
    match Database::parse_check(&body.sql) {
        Ok(()) => Ok(Json(serde_json::json!({"ok": true}))),
        Err(m) => Ok(Json(serde_json::json!({"ok": false, "message": m}))),
    }
}

/// Assemble the object-explorer payload (server / storage / tables / KV).
pub fn build_meta(kv: &mut Kv, db_path: &Path, started: Instant) -> serde_json::Value {
    // The _kv system table is surfaced in the KV section, not the user tables.
    let catalog: Vec<_> = kv
        .db
        .catalog()
        .into_iter()
        .filter(|t| t.name != docsql_kv::KV_TABLE)
        .collect();
    let mut tables = Vec::new();
    let mut total_rows = 0u64;
    for t in &catalog {
        let row_count = match kv
            .db
            .execute(&format!("SELECT COUNT(*) FROM \"{}\"", t.name))
        {
            Ok(ExecOutcome::Rows(r)) => {
                r.rows.first().and_then(|row| row[0].as_i64()).unwrap_or(0) as u64
            }
            _ => 0,
        };
        total_rows += row_count;
        tables.push(serde_json::json!({
            "name": t.name,
            "row_count": row_count,
            "pages": t.pages,
            "keys": t.keys,
            "indexes": t.indexes,
            "columns": t.columns.iter().map(|c| serde_json::json!({
                "name": c.name,
                "nullable": c.nullable,
                "primary_key": c.primary_key,
                "unique": c.unique,
                "autoinc": c.autoinc,
            })).collect::<Vec<_>>(),
        }));
    }
    let (kv_total, by_type) = kv_summary(kv);
    serde_json::json!({
        "server": {
            "name": "docsql",
            "version": CONSOLE_VERSION,
            "uptime_ms": started.elapsed().as_millis() as u64,
        },
        "storage": {
            "page_size": kv.db.page_size(),
            "num_pages": kv.db.num_pages(),
            "db_bytes": file_bytes(db_path),
            "wal_bytes": file_bytes(&wal_path(db_path)),
        },
        "totals": {"tables": tables.len(), "rows": total_rows},
        "tables": tables,
        "kv": {"total": kv_total, "by_type": by_type},
    })
}

fn kv_summary(kv: &mut Kv) -> (u64, serde_json::Value) {
    let mut by_type: BTreeMap<String, u64> = BTreeMap::new();
    let mut total = 0u64;
    if let Ok(ExecOutcome::Rows(r)) = kv.db.execute("SELECT type FROM _kv") {
        for row in &r.rows {
            // Mirror KV read semantics: expired keys do not exist.
            let t = row[0].as_str().unwrap_or("string").to_string();
            by_type.entry(t).and_modify(|n| *n += 1).or_insert(1);
            total += 1;
        }
    }
    (total, serde_json::json!(by_type))
}

fn file_bytes(p: &Path) -> u64 {
    std::fs::metadata(p).map(|m| m.len()).unwrap_or(0)
}

fn wal_path(db: &Path) -> PathBuf {
    let mut s = db.as_os_str().to_os_string();
    s.push(".wal");
    PathBuf::from(s)
}

async fn api_meta(
    State(state): State<Arc<WebState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if let Some(code) = check_auth(&state, &headers) {
        return Err(code);
    }
    let mut kv = state.kv.lock().unwrap();
    Ok(Json(build_meta(&mut kv, &state.db_path, state.started)))
}

#[derive(serde::Deserialize, Default)]
struct KeysParams {
    #[serde(default)]
    r#type: Option<String>,
    #[serde(default)]
    like: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

/// One `_kv` row decoded for the browser.
pub struct KeyEntry {
    pub key: String,
    pub kind: String,
    pub ttl_ms: i64,
    pub raw_value: String,
}

pub fn list_keys(kv: &mut Kv, filter_type: Option<&str>, like: Option<&str>) -> Vec<KeyEntry> {
    let now = docsql_kv::now_ms();
    let mut out = Vec::new();
    if let Ok(ExecOutcome::Rows(r)) = kv
        .db
        .execute("SELECT \"key\", type, value, expire_at FROM _kv ORDER BY \"key\"")
    {
        for row in r.rows {
            let (Some(k), t, v, exp) = (
                row[0].as_str(),
                row[1].as_str().unwrap_or("string"),
                row[2].as_str().unwrap_or(""),
                row[3].as_i64().unwrap_or(0),
            ) else {
                continue;
            };
            if exp > 0 && exp <= now {
                continue; // expired keys do not exist
            }
            if let Some(ty) = filter_type {
                if t != ty {
                    continue;
                }
            }
            if let Some(pat) = like {
                if !pat.is_empty() && !k.contains(pat) {
                    continue;
                }
            }
            out.push(KeyEntry {
                key: k.to_string(),
                kind: t.to_string(),
                ttl_ms: if exp > 0 { exp - now } else { -1 },
                raw_value: v.to_string(),
            });
        }
    }
    out
}

async fn api_keys(
    State(state): State<Arc<WebState>>,
    headers: HeaderMap,
    Query(params): Query<KeysParams>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if let Some(code) = check_auth(&state, &headers) {
        return Err(code);
    }
    let mut kv = state.kv.lock().unwrap();
    let limit = params.limit.unwrap_or(500).min(5000);
    let all = list_keys(&mut kv, params.r#type.as_deref(), params.like.as_deref());
    let total = all.len();
    let keys: Vec<serde_json::Value> = all
        .into_iter()
        .take(limit)
        .map(|e| {
            let preview: String = e.raw_value.chars().take(120).collect();
            serde_json::json!({
                "key": e.key,
                "type": e.kind,
                "ttl_ms": e.ttl_ms,
                "preview": preview,
            })
        })
        .collect();
    Ok(Json(serde_json::json!({ "keys": keys, "total": total })))
}

/// Decode one KV entry's stored value per collection type.
pub fn decode_kv_value(kind: &str, raw: &str) -> serde_json::Value {
    let parsed = json::from_str(raw).unwrap_or(Value::Str(raw.to_string()));
    match kind {
        "string" => serde_json::json!({ "kind": "string", "text": raw }),
        "list" => match &parsed {
            Value::Array(a) => serde_json::json!({
                "kind": "list",
                "items": a.iter().map(value_json).collect::<Vec<_>>(),
            }),
            _ => serde_json::json!({ "kind": "list", "items": [] }),
        },
        "hash" => match &parsed {
            Value::Object(o) => serde_json::json!({
                "kind": "hash",
                "fields": o.iter().map(|(f, v)| serde_json::json!({
                    "field": f, "value": value_json(v),
                })).collect::<Vec<_>>(),
            }),
            _ => serde_json::json!({ "kind": "hash", "fields": [] }),
        },
        "set" => match &parsed {
            Value::Array(a) => serde_json::json!({
                "kind": "set",
                "members": a.iter().map(value_json).collect::<Vec<_>>(),
            }),
            _ => serde_json::json!({ "kind": "set", "members": [] }),
        },
        // ZSET is stored as {"member": score}; sort by (score, member).
        "zset" => match &parsed {
            Value::Object(o) => {
                let mut pairs: Vec<(String, f64)> = o
                    .iter()
                    .filter_map(|(m, v)| match v {
                        Value::Float(f) => Some((m.clone(), *f)),
                        Value::Int(i) => Some((m.clone(), *i as f64)),
                        _ => None,
                    })
                    .collect();
                pairs.sort_by(|a, b| {
                    a.1.partial_cmp(&b.1)
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then(a.0.cmp(&b.0))
                });
                serde_json::json!({
                    "kind": "zset",
                    "pairs": pairs.into_iter()
                        .map(|(m, sc)| serde_json::json!([m, sc]))
                        .collect::<Vec<_>>(),
                })
            }
            _ => serde_json::json!({ "kind": "zset", "pairs": [] }),
        },
        other => serde_json::json!({ "kind": other, "text": raw }),
    }
}

async fn api_kvkey(
    State(state): State<Arc<WebState>>,
    headers: HeaderMap,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if let Some(code) = check_auth(&state, &headers) {
        return Err(code);
    }
    let Some(key) = params.get("key") else {
        return Ok(Json(serde_json::json!({"error": "missing ?key="})));
    };
    let mut kv = state.kv.lock().unwrap();
    let now = docsql_kv::now_ms();
    let found = list_keys(&mut kv, None, None)
        .into_iter()
        .find(|e| e.key == *key);
    match found {
        Some(e) => Ok(Json(serde_json::json!({
            "key": e.key,
            "type": e.kind,
            "ttl_ms": if e.ttl_ms >= 0 { e.ttl_ms } else { -1 },
            "expires_at_ms": if e.ttl_ms >= 0 { now + e.ttl_ms } else { 0 },
            "value": decode_kv_value(&e.kind, &e.raw_value),
        }))),
        None => Ok(Json(
            serde_json::json!({"error": format!("no such key: {key}")}),
        )),
    }
}

#[derive(serde::Deserialize)]
struct KvBody {
    command: String,
    args: Vec<String>,
}

/// Dispatch one KV command (pure helper so semantics are unit-testable).
/// `PUBLISH` additionally records to the console event log.
pub fn kv_dispatch(
    kv: &mut Kv,
    log: Option<&mut EventLog>,
    command: &str,
    args: &[String],
) -> serde_json::Value {
    use serde_json::json;
    let a: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    let arg = |i: usize| a.get(i).copied().unwrap_or("");
    let int = |i: usize| a.get(i).and_then(|s| s.parse::<i64>().ok());
    let float = |i: usize| a.get(i).and_then(|s| s.parse::<f64>().ok());
    match command.to_uppercase().as_str() {
        "PING" => json!({"ok": true, "value": "PONG"}),
        "GET" => match kv.get(arg(0)) {
            Ok(v) => json!({"ok": true, "value": v}),
            Err(e) => json!({"ok": false, "error": e.to_string()}),
        },
        "SET" => {
            let mut opts = SetOpts::default();
            for f in a.iter().skip(2) {
                let f = f.to_uppercase();
                if f == "NX" {
                    opts.nx = true;
                } else if f == "XX" {
                    opts.xx = true;
                } else if let Some(secs) = f.strip_prefix("EX=") {
                    opts.ttl_ms = secs.parse::<i64>().ok().map(|s| s * 1000);
                } else if let Some(ms) = f.strip_prefix("PX=") {
                    opts.ttl_ms = ms.parse::<i64>().ok();
                }
            }
            match kv.set(arg(0), arg(1), opts) {
                Ok(done) => json!({"ok": true, "value": if done { "OK" } else { "SKIP" }}),
                Err(e) => json!({"ok": false, "error": e.to_string()}),
            }
        }
        "EXISTS" => match kv.exists(arg(0)) {
            Ok(done) => json!({"ok": true, "value": if done { 1 } else { 0 }}),
            Err(e) => json!({"ok": false, "error": e.to_string()}),
        },
        "DEL" => match kv.del(arg(0)) {
            Ok(done) => json!({"ok": true, "value": if done { 1 } else { 0 }}),
            Err(e) => json!({"ok": false, "error": e.to_string()}),
        },
        "EXPIRE" => match kv.expire(arg(0), int(1).unwrap_or(0)) {
            Ok(done) => json!({"ok": true, "value": if done { 1 } else { 0 }}),
            Err(e) => json!({"ok": false, "error": e.to_string()}),
        },
        "PERSIST" => match kv.persist(arg(0)) {
            Ok(done) => json!({"ok": true, "value": if done { 1 } else { 0 }}),
            Err(e) => json!({"ok": false, "error": e.to_string()}),
        },
        "TTL" => match kv.ttl_ms(arg(0)) {
            Ok(Some(ms)) => json!({"ok": true, "value": ms}),
            Ok(None) => json!({"ok": true, "value": -1}),
            Err(_) => json!({"ok": true, "value": -2}),
        },
        "TYPE" => match kv.type_of(arg(0)) {
            Ok(Some(t)) => json!({"ok": true, "value": t}),
            Ok(None) => json!({"ok": true, "value": serde_json::Value::Null}),
            Err(e) => json!({"ok": false, "error": e.to_string()}),
        },
        "INCR" | "INCRBY" | "DECR" | "DECRBY" => {
            let delta = match command.to_uppercase().as_str() {
                "INCR" => 1,
                "DECR" => -1,
                "INCRBY" => int(1).unwrap_or(1),
                _ => -(int(1).unwrap_or(1)),
            };
            match kv.incr_by(arg(0), delta) {
                Ok(n) => json!({"ok": true, "value": n}),
                Err(e) => json!({"ok": false, "error": e.to_string()}),
            }
        }
        "LPUSH" | "RPUSH" => {
            let vals = &a[1.min(a.len())..];
            let r = if command.eq_ignore_ascii_case("LPUSH") {
                kv.lpush(arg(0), vals)
            } else {
                kv.rpush(arg(0), vals)
            };
            match r {
                Ok(n) => json!({"ok": true, "value": n}),
                Err(e) => json!({"ok": false, "error": e.to_string()}),
            }
        }
        "LPOP" | "RPOP" => {
            let r = if command == "LPOP" {
                kv.lpop(arg(0))
            } else {
                kv.rpop(arg(0))
            };
            match r {
                Ok(v) => json!({"ok": true, "value": v}),
                Err(e) => json!({"ok": false, "error": e.to_string()}),
            }
        }
        "LRANGE" => match kv.lrange(arg(0), int(1).unwrap_or(0), int(2).unwrap_or(-1)) {
            Ok(items) => json!({"ok": true, "value": items}),
            Err(e) => json!({"ok": false, "error": e.to_string()}),
        },
        "LLEN" => match kv.llen(arg(0)) {
            Ok(n) => json!({"ok": true, "value": n}),
            Err(_) => json!({"ok": true, "value": 0}),
        },
        "HSET" => match kv.hset(arg(0), arg(1), arg(2)) {
            Ok(done) => json!({"ok": true, "value": done}),
            Err(e) => json!({"ok": false, "error": e.to_string()}),
        },
        "HGET" => match kv.hget(arg(0), arg(1)) {
            Ok(v) => json!({"ok": true, "value": v}),
            Err(e) => json!({"ok": false, "error": e.to_string()}),
        },
        "HGETALL" => match kv.hgetall(arg(0)) {
            Ok(pairs) => json!({"ok": true, "value": pairs}),
            Err(e) => json!({"ok": false, "error": e.to_string()}),
        },
        "SADD" => match kv.sadd(arg(0), &a[1.min(a.len())..]) {
            Ok(n) => json!({"ok": true, "value": n}),
            Err(e) => json!({"ok": false, "error": e.to_string()}),
        },
        "SMEMBERS" => match kv.smembers(arg(0)) {
            Ok(items) => json!({"ok": true, "value": items}),
            Err(e) => json!({"ok": false, "error": e.to_string()}),
        },
        "SISMEMBER" => match kv.sismember(arg(0), arg(1)) {
            Ok(b) => json!({"ok": true, "value": if b { 1 } else { 0 }}),
            Err(e) => json!({"ok": false, "error": e.to_string()}),
        },
        "ZADD" => match kv.zadd(arg(0), float(1).unwrap_or(0.0), arg(2)) {
            Ok(n) => json!({"ok": true, "value": n}),
            Err(e) => json!({"ok": false, "error": e.to_string()}),
        },
        "ZSCORE" => match kv.zscore(arg(0), arg(1)) {
            Ok(v) => json!({"ok": true, "value": v}),
            Err(e) => json!({"ok": false, "error": e.to_string()}),
        },
        "ZRANGE" => match kv.zrange(arg(0), float(1).unwrap_or(0.0), float(2).unwrap_or(0.0)) {
            Ok(pairs) => json!({"ok": true,
                "value": pairs.into_iter().map(|(m, s)| serde_json::json!([m, s])).collect::<Vec<_>>()}),
            Err(e) => json!({"ok": false, "error": e.to_string()}),
        },
        "ZRANK" => match kv.zrank(arg(0), arg(1)) {
            Ok(v) => json!({"ok": true, "value": v}),
            Err(e) => json!({"ok": false, "error": e.to_string()}),
        },
        "MULTI" => match kv.multi() {
            Ok(_) => json!({"ok": true, "value": "OK"}),
            Err(e) => json!({"ok": false, "error": e.to_string()}),
        },
        "EXEC" => match kv.exec() {
            Ok(_) => json!({"ok": true, "value": "OK"}),
            Err(e) => json!({"ok": false, "error": e.to_string()}),
        },
        "DISCARD" => match kv.discard() {
            Ok(_) => json!({"ok": true, "value": "OK"}),
            Err(e) => json!({"ok": false, "error": e.to_string()}),
        },
        "PUBLISH" => {
            let seq = match log {
                Some(l) => l.push(arg(0), arg(1)),
                None => 0,
            };
            json!({"ok": true, "value": "OK", "seq": seq,
                   "note": "recorded in the web console monitor; TCP clients subscribe on the protocol port"})
        }
        other => json!({"ok": false, "error": format!("unknown command: {other}")}),
    }
}

async fn api_kv(
    State(state): State<Arc<WebState>>,
    headers: HeaderMap,
    Json(body): Json<KvBody>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if let Some(code) = check_auth(&state, &headers) {
        return Err(code);
    }
    let mut kv = state.kv.lock().unwrap();
    let mut log = state.events.lock().unwrap();
    Ok(Json(kv_dispatch(
        &mut kv,
        Some(&mut log),
        &body.command,
        &body.args,
    )))
}

async fn api_stats(
    State(state): State<Arc<WebState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if let Some(code) = check_auth(&state, &headers) {
        return Err(code);
    }
    let mut kv = state.kv.lock().unwrap();
    let (kv_total, by_type) = kv_summary(&mut kv);
    let user_tables = kv
        .db
        .catalog()
        .into_iter()
        .filter(|t| t.name != docsql_kv::KV_TABLE)
        .count();
    let num_pages = kv.db.num_pages();
    let page_size = kv.db.page_size();
    Ok(Json(serde_json::json!({
        // legacy field kept for existing clients / deploy checks
        "kv_keys": kv_total,
        "kv_by_type": by_type,
        "tables": user_tables,
        "pages": num_pages,
        "page_size": page_size,
        "db_bytes": file_bytes(&state.db_path),
        "wal_bytes": file_bytes(&wal_path(&state.db_path)),
        "uptime_ms": state.started.elapsed().as_millis() as u64,
    })))
}

#[derive(serde::Deserialize)]
struct PublishBody {
    channel: String,
    message: String,
}

async fn api_publish(
    State(state): State<Arc<WebState>>,
    headers: HeaderMap,
    Json(body): Json<PublishBody>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if let Some(code) = check_auth(&state, &headers) {
        return Err(code);
    }
    let mut log = state.events.lock().unwrap();
    let seq = log.push(&body.channel, &body.message);
    Ok(Json(serde_json::json!({
        "ok": true,
        "seq": seq,
        "note": "web-console monitor only; TCP clients subscribe on the protocol port",
    })))
}

async fn api_events(
    State(state): State<Arc<WebState>>,
    headers: HeaderMap,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if let Some(code) = check_auth(&state, &headers) {
        return Err(code);
    }
    let after: u64 = params
        .get("after")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let log = state.events.lock().unwrap();
    let latest = log.seq;
    let events: Vec<serde_json::Value> = log
        .since(after)
        .into_iter()
        .map(|e| {
            serde_json::json!({
                "seq": e.seq, "channel": e.channel, "message": e.payload, "at_ms": e.at_ms,
            })
        })
        .collect();
    Ok(Json(
        serde_json::json!({ "events": events, "latest": latest }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kv() -> Kv {
        Kv::in_memory().unwrap()
    }

    #[tokio::test]
    async fn console_page_served() {
        let html = include_str!("console.html");
        assert!(html.contains("docsql")); // console markup present
        assert!(html.contains("docsql console")); // deploy-test marker
        assert!(html.contains("对象资源管理器")); // SSMS-style explorer present
    }

    #[test]
    fn value_json_mapping() {
        assert_eq!(value_json(&Value::Int(3)), serde_json::json!(3));
        assert_eq!(value_json(&Value::Str("x".into())), serde_json::json!("x"));
        assert_eq!(value_json(&Value::Null), serde_json::Value::Null);
    }

    #[test]
    fn run_sql_single_statement_keeps_legacy_shape() {
        let mut k = kv();
        let r = run_sql(&mut k, "CREATE TABLE t (id INT PRIMARY KEY)");
        assert_eq!(r, serde_json::json!({"kind": "affected", "count": 0}));
        let r = run_sql(&mut k, "INSERT INTO t VALUES (1)");
        assert_eq!(r, serde_json::json!({"kind": "affected", "count": 1}));
        let r = run_sql(&mut k, "SELECT id FROM t");
        assert_eq!(r["kind"], "rows");
        assert_eq!(r["rows"][0][0], 1);
        let r = run_sql(&mut k, "SELECT * FROM missing");
        assert_eq!(r["kind"], "error");
        assert!(r["message"].as_str().unwrap().contains("missing"));
    }

    #[test]
    fn run_sql_batch_reports_each_statement() {
        let mut k = kv();
        let r = run_sql(
            &mut k,
            "CREATE TABLE b (id INT); INSERT INTO b VALUES (1), (2); SELECT id FROM b ORDER BY id",
        );
        assert_eq!(r["kind"], "batch");
        let results = r["results"].as_array().unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[2]["rows"].as_array().unwrap().len(), 2);
        assert!(r["error"].is_null());

        // Error mid-batch: partial results + statement index.
        let r = run_sql(&mut k, "INSERT INTO b VALUES (3); SELECT * FROM nope");
        assert_eq!(r["kind"], "batch");
        assert_eq!(r["error"]["statement"], 1);
        assert_eq!(r["results"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn kv_dispatch_covers_core_and_collection_commands() {
        let mut k = kv();
        let d = |k: &mut Kv, c: &str, a: &[&str]| {
            let args: Vec<String> = a.iter().map(|s| s.to_string()).collect();
            kv_dispatch(k, None, c, &args)
        };
        assert_eq!(d(&mut k, "SET", &["greet", "hi"])["value"], "OK");
        assert_eq!(d(&mut k, "GET", &["greet"])["value"], "hi");
        assert_eq!(d(&mut k, "TYPE", &["greet"])["value"], "string");
        assert_eq!(d(&mut k, "INCR", &["hits"])["value"], 1);
        assert_eq!(d(&mut k, "INCRBY", &["hits", "9"])["value"], 10);
        assert_eq!(d(&mut k, "DECR", &["hits"])["value"], 9);
        assert_eq!(d(&mut k, "EXPIRE", &["greet", "60000"])["value"], 1);
        assert!(d(&mut k, "TTL", &["greet"])["value"].as_i64().unwrap() > 0);
        assert_eq!(d(&mut k, "PERSIST", &["greet"])["value"], 1);
        assert_eq!(d(&mut k, "TTL", &["greet"])["value"], -1);
        assert_eq!(d(&mut k, "TTL", &["missing"])["value"], -2);
        assert_eq!(d(&mut k, "DEL", &["greet"])["value"], 1);

        assert_eq!(d(&mut k, "RPUSH", &["q", "a", "b"])["value"], 2);
        assert_eq!(d(&mut k, "LPUSH", &["q", "z"])["value"], 3);
        assert_eq!(d(&mut k, "LLEN", &["q"])["value"], 3);
        assert_eq!(
            d(&mut k, "LRANGE", &["q", "0", "-1"])["value"],
            serde_json::json!(["z", "a", "b"])
        );
        assert_eq!(d(&mut k, "LPOP", &["q"])["value"], "z");

        assert_eq!(d(&mut k, "HSET", &["h", "f1", "v1"])["value"], 1);
        assert_eq!(d(&mut k, "HGET", &["h", "f1"])["value"], "v1");
        assert_eq!(
            d(&mut k, "HGETALL", &["h"])["value"],
            serde_json::json!([["f1", "v1"]])
        );

        assert_eq!(d(&mut k, "SADD", &["s", "m1", "m2"])["value"], 2);
        assert_eq!(d(&mut k, "SISMEMBER", &["s", "m1"])["value"], 1);
        assert_eq!(
            d(&mut k, "SMEMBERS", &["s"])["value"],
            serde_json::json!(["m1", "m2"])
        );

        assert_eq!(d(&mut k, "ZADD", &["z", "9.5", "one"])["value"], 1);
        assert_eq!(d(&mut k, "ZSCORE", &["z", "one"])["value"], 9.5);
        assert_eq!(d(&mut k, "ZRANK", &["z", "one"])["value"], 0);
        assert_eq!(
            d(&mut k, "ZRANGE", &["z", "0", "10"])["value"],
            serde_json::json!([["one", 9.5]])
        );

        assert!(!d(&mut k, "NOPE", &[])["ok"].as_bool().unwrap());
    }

    #[test]
    fn set_flags_nx_xx_ex() {
        let mut k = kv();
        let d = |k: &mut Kv, c: &str, a: &[&str]| {
            let args: Vec<String> = a.iter().map(|s| s.to_string()).collect();
            kv_dispatch(k, None, c, &args)
        };
        assert_eq!(d(&mut k, "SET", &["k", "1", "NX"])["value"], "OK");
        assert_eq!(d(&mut k, "SET", &["k", "2", "NX"])["value"], "SKIP");
        assert_eq!(d(&mut k, "SET", &["k", "3", "XX"])["value"], "OK");
        assert_eq!(d(&mut k, "GET", &["k"])["value"], "3");
        assert_eq!(d(&mut k, "SET", &["ttl", "x", "PX=60000"])["value"], "OK");
        assert!(d(&mut k, "TTL", &["ttl"])["value"].as_i64().unwrap() > 0);
    }

    #[test]
    fn list_keys_filters_by_type_and_substring() {
        let mut k = kv();
        let d = |k: &mut Kv, c: &str, a: &[&str]| {
            let args: Vec<String> = a.iter().map(|s| s.to_string()).collect();
            kv_dispatch(k, None, c, &args)
        };
        d(&mut k, "SET", &["user:1", "ann"]);
        d(&mut k, "SET", &["user:2", "bob"]);
        d(&mut k, "RPUSH", &["jobs", "a"]);
        let all = list_keys(&mut k, None, None);
        assert_eq!(all.len(), 3);
        let users = list_keys(&mut k, Some("string"), Some("user:"));
        assert_eq!(users.len(), 2);
        assert!(users.iter().all(|e| e.kind == "string"));
        assert_eq!(users[0].raw_value, "ann");
    }

    #[test]
    fn decode_kv_value_by_type() {
        assert_eq!(
            decode_kv_value("string", "hello"),
            serde_json::json!({"kind": "string", "text": "hello"})
        );
        assert_eq!(
            decode_kv_value("list", r#"["a","b"]"#),
            serde_json::json!({"kind": "list", "items": ["a", "b"]})
        );
        assert_eq!(
            decode_kv_value("hash", r#"{"f":"v"}"#),
            serde_json::json!({"kind": "hash", "fields": [{"field": "f", "value": "v"}]})
        );
        assert_eq!(
            decode_kv_value("set", r#"["m1","m2"]"#),
            serde_json::json!({"kind": "set", "members": ["m1", "m2"]})
        );
        // zset is stored as {"member": score} and decoded sorted by score
        assert_eq!(
            decode_kv_value("zset", r#"{"bob":72,"ann":98.5}"#),
            serde_json::json!({"kind": "zset", "pairs": [["bob", 72.0], ["ann", 98.5]]})
        );
    }

    #[test]
    fn build_meta_reports_tables_and_kv() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.db");
        let mut k = Kv::open(&path).unwrap();
        k.db.execute("CREATE TABLE m (id INT PRIMARY KEY, name TEXT)")
            .unwrap();
        k.db.execute("INSERT INTO m VALUES (1, 'a'), (2, 'b')")
            .unwrap();
        k.set("kk", "vv", SetOpts::default()).unwrap();
        let meta = build_meta(&mut k, &path, Instant::now());
        assert_eq!(meta["totals"]["tables"], 1);
        assert_eq!(meta["tables"][0]["row_count"], 2);
        assert_eq!(meta["tables"][0]["columns"].as_array().unwrap().len(), 2);
        assert_eq!(meta["tables"][0]["keys"][0], "id");
        assert_eq!(meta["kv"]["total"], 1);
        assert_eq!(meta["kv"]["by_type"]["string"], 1);
        assert_eq!(meta["storage"]["page_size"], 4096);
        assert!(meta["storage"]["num_pages"].as_u64().unwrap() >= 2);
    }

    #[test]
    fn event_log_ring_and_since() {
        let mut log = EventLog::default();
        assert!(log.since(0).is_empty());
        let s1 = log.push("ch", "m1");
        let s2 = log.push("ch", "m2");
        assert_eq!((s1, s2), (1, 2));
        let since: Vec<&PubEvent> = log.since(1);
        assert_eq!(since.len(), 1);
        assert_eq!(since[0].payload, "m2");
        for i in 0..(EVENT_LOG_CAP + 20) {
            log.push("ch", &format!("m{i}"));
        }
        assert_eq!(log.since(0).len(), EVENT_LOG_CAP);
    }

    // ---- Router-level integration tests (one-shot requests) ----

    use axum::body::Body;
    use http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn test_app(token: Option<String>) -> (Router, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let kv = Kv::open(&dir.path().join("t.db")).unwrap();
        let state = Arc::new(WebState {
            kv: Mutex::new(kv),
            token,
            db_path: dir.path().join("t.db"),
            started: Instant::now(),
            events: Mutex::new(EventLog::default()),
        });
        let app = Router::new()
            .route("/", get(index))
            .route("/api/sql", post(api_sql))
            .route("/api/parse", post(api_parse))
            .route("/api/meta", get(api_meta))
            .route("/api/keys", get(api_keys))
            .route("/api/kvkey", get(api_kvkey))
            .route("/api/kv", post(api_kv))
            .route("/api/stats", get(api_stats))
            .route("/api/publish", post(api_publish))
            .route("/api/events", get(api_events))
            .with_state(state);
        (app, dir)
    }

    async fn json_req(
        app: &Router,
        method: &str,
        uri: &str,
        token: Option<&str>,
        body: serde_json::Value,
    ) -> (StatusCode, serde_json::Value) {
        let mut b = Request::builder().method(method).uri(uri);
        if let Some(t) = token {
            b = b.header("X-Docsql-Token", t);
        }
        let resp = app
            .clone()
            .oneshot(
                b.header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let code = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let v = if bytes.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
        };
        (code, v)
    }

    #[tokio::test]
    async fn index_serves_console_html() {
        let (app, _d) = test_app(None);
        let resp = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(text.contains("docsql console"));
    }

    #[tokio::test]
    async fn sql_parse_meta_stats_endpoints_work() {
        let (app, _d) = test_app(None);
        // SQL: DDL, DML, rows, error
        let (c, v) = json_req(
            &app,
            "POST",
            "/api/sql",
            None,
            serde_json::json!({"sql": "CREATE TABLE t (id INT PRIMARY KEY)"}),
        )
        .await;
        assert_eq!(c, StatusCode::OK);
        assert_eq!(v["kind"], "affected");
        let (_, v) = json_req(
            &app,
            "POST",
            "/api/sql",
            None,
            serde_json::json!({"sql": "INSERT INTO t VALUES (1),(2)"}),
        )
        .await;
        assert_eq!(v["count"], 2);
        let (_, v) = json_req(
            &app,
            "POST",
            "/api/sql",
            None,
            serde_json::json!({"sql": "SELECT id FROM t ORDER BY id"}),
        )
        .await;
        assert_eq!(v["rows"].as_array().unwrap().len(), 2);
        let (_, v) = json_req(
            &app,
            "POST",
            "/api/sql",
            None,
            serde_json::json!({"sql": "SELECT * FROM nope"}),
        )
        .await;
        assert_eq!(v["kind"], "error");

        // Parse check
        let (_, v) = json_req(
            &app,
            "POST",
            "/api/parse",
            None,
            serde_json::json!({"sql": "SELECT 1"}),
        )
        .await;
        assert_eq!(v["ok"], true);
        let (_, v) = json_req(
            &app,
            "POST",
            "/api/parse",
            None,
            serde_json::json!({"sql": "SELEC 1"}),
        )
        .await;
        assert_eq!(v["ok"], false);

        // Meta
        let (c, v) = json_req(&app, "GET", "/api/meta", None, serde_json::Value::Null).await;
        assert_eq!(c, StatusCode::OK);
        assert_eq!(v["totals"]["tables"], 1);
        assert_eq!(v["totals"]["rows"], 2);

        // Stats
        let (c, v) = json_req(&app, "GET", "/api/stats", None, serde_json::Value::Null).await;
        assert_eq!(c, StatusCode::OK);
        assert_eq!(v["tables"], 1);
        assert_eq!(v["page_size"], 4096);
        assert!(v["uptime_ms"].as_u64().is_some());
    }

    #[tokio::test]
    async fn token_required_when_configured() {
        let (app, _d) = test_app(Some("secret".into()));
        let (c, _) = json_req(
            &app,
            "POST",
            "/api/sql",
            None,
            serde_json::json!({"sql": "SELECT 1"}),
        )
        .await;
        assert_eq!(c, StatusCode::UNAUTHORIZED);
        let (c, _) = json_req(
            &app,
            "POST",
            "/api/sql",
            Some("wrong"),
            serde_json::json!({"sql": "SELECT 1"}),
        )
        .await;
        assert_eq!(c, StatusCode::UNAUTHORIZED);
        for (m, uri) in [
            ("GET", "/api/meta"),
            ("GET", "/api/keys"),
            ("GET", "/api/kvkey?key=x"),
            ("GET", "/api/stats"),
            ("GET", "/api/events"),
        ] {
            let (c, _) = json_req(&app, m, uri, None, serde_json::Value::Null).await;
            assert_eq!(c, StatusCode::UNAUTHORIZED, "{uri}");
        }
        let (c, _) = json_req(
            &app,
            "POST",
            "/api/publish",
            None,
            serde_json::json!({"channel":"c","message":"m"}),
        )
        .await;
        assert_eq!(c, StatusCode::UNAUTHORIZED);
        let (c, _) = json_req(
            &app,
            "POST",
            "/api/kv",
            None,
            serde_json::json!({"command":"PING","args":[]}),
        )
        .await;
        assert_eq!(c, StatusCode::UNAUTHORIZED);
        // Correct token passes
        let (c, _) = json_req(
            &app,
            "POST",
            "/api/sql",
            Some("secret"),
            serde_json::json!({"sql": "SELECT 1"}),
        )
        .await;
        assert_eq!(c, StatusCode::OK);
    }

    #[tokio::test]
    async fn keys_browser_filters_and_limits() {
        let (app, _d) = test_app(None);
        for i in 0..5 {
            json_req(
                &app,
                "POST",
                "/api/kv",
                None,
                serde_json::json!({"command":"SET","args":[format!("user:{i}"), "v"]}),
            )
            .await;
        }
        json_req(
            &app,
            "POST",
            "/api/kv",
            None,
            serde_json::json!({"command":"RPUSH","args":["jobs","a"]}),
        )
        .await;
        let (_, v) = json_req(&app, "GET", "/api/keys", None, serde_json::Value::Null).await;
        assert_eq!(v["total"], 6);
        let (_, v) = json_req(
            &app,
            "GET",
            "/api/keys?type=string&like=user:",
            None,
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(v["total"], 5);
        let (_, v) = json_req(
            &app,
            "GET",
            "/api/keys?limit=2",
            None,
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(v["total"], 6);
        assert_eq!(v["keys"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn kvkey_endpoint_lookup() {
        let (app, _d) = test_app(None);
        json_req(
            &app,
            "POST",
            "/api/kv",
            None,
            serde_json::json!({"command":"SET","args":["greet","hi"]}),
        )
        .await;
        json_req(
            &app,
            "POST",
            "/api/kv",
            None,
            serde_json::json!({"command":"RPUSH","args":["q","a","b"]}),
        )
        .await;
        let (_, v) = json_req(&app, "GET", "/api/kvkey", None, serde_json::Value::Null).await;
        assert!(v["error"].as_str().unwrap().contains("missing"));
        let (_, v) = json_req(
            &app,
            "GET",
            "/api/kvkey?key=absent",
            None,
            serde_json::Value::Null,
        )
        .await;
        assert!(v["error"].as_str().unwrap().contains("no such key"));
        let (_, v) = json_req(
            &app,
            "GET",
            "/api/kvkey?key=greet",
            None,
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(v["type"], "string");
        assert_eq!(v["ttl_ms"], -1);
        assert_eq!(v["value"]["text"], "hi");
        let (_, v) = json_req(
            &app,
            "GET",
            "/api/kvkey?key=q",
            None,
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(v["value"]["kind"], "list");
        assert_eq!(v["value"]["items"].as_array().unwrap().len(), 2);
        // TTL-bearing key reports remaining ttl
        json_req(
            &app,
            "POST",
            "/api/kv",
            None,
            serde_json::json!({"command":"SET","args":["tmp","x","PX=60000"]}),
        )
        .await;
        let (_, v) = json_req(
            &app,
            "GET",
            "/api/kvkey?key=tmp",
            None,
            serde_json::Value::Null,
        )
        .await;
        assert!(v["ttl_ms"].as_i64().unwrap() > 0);
        assert!(v["expires_at_ms"].as_i64().unwrap() > 0);
    }

    #[tokio::test]
    async fn kv_dispatch_endpoint_and_publish_monitor() {
        let (app, _d) = test_app(None);
        let (c, v) = json_req(
            &app,
            "POST",
            "/api/kv",
            None,
            serde_json::json!({"command":"PING","args":[]}),
        )
        .await;
        assert_eq!(c, StatusCode::OK);
        assert_eq!(v["value"], "PONG");
        json_req(
            &app,
            "POST",
            "/api/kv",
            None,
            serde_json::json!({"command":"SET","args":["k","v"]}),
        )
        .await;
        let (_, v) = json_req(
            &app,
            "POST",
            "/api/kv",
            None,
            serde_json::json!({"command":"GET","args":["k"]}),
        )
        .await;
        assert_eq!(v["value"], "v");
        let (_, v) = json_req(
            &app,
            "POST",
            "/api/kv",
            None,
            serde_json::json!({"command":"GET","args":["missing"]}),
        )
        .await;
        assert_eq!(v["value"], serde_json::Value::Null);
        // PUBLISH lands in the event log and shows via /api/events
        let (_, v) = json_req(
            &app,
            "POST",
            "/api/kv",
            None,
            serde_json::json!({"command":"PUBLISH","args":["news","hello"]}),
        )
        .await;
        assert_eq!(v["ok"], true);
        assert!(v["seq"].as_u64().unwrap() >= 1);
        let (_, v) = json_req(
            &app,
            "POST",
            "/api/publish",
            None,
            serde_json::json!({"channel":"news","message":"again"}),
        )
        .await;
        assert_eq!(v["ok"], true);
        let (_, v) = json_req(&app, "GET", "/api/events", None, serde_json::Value::Null).await;
        assert_eq!(v["latest"], 2);
        assert_eq!(v["events"].as_array().unwrap().len(), 2);
        let (_, v) = json_req(
            &app,
            "GET",
            "/api/events?after=1",
            None,
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(v["events"].as_array().unwrap().len(), 1);
        assert_eq!(v["events"][0]["message"], "again");
        // MULTI/EXEC via the API
        let (_, v) = json_req(
            &app,
            "POST",
            "/api/kv",
            None,
            serde_json::json!({"command":"MULTI","args":[]}),
        )
        .await;
        assert_eq!(v["value"], "OK");
        let (_, v) = json_req(
            &app,
            "POST",
            "/api/kv",
            None,
            serde_json::json!({"command":"EXEC","args":[]}),
        )
        .await;
        assert_eq!(v["value"], "OK");
    }

    #[test]
    fn decode_kv_value_malformed_and_other_types() {
        // malformed collection payloads degrade to empty
        assert_eq!(
            decode_kv_value("list", "not json"),
            serde_json::json!({"kind": "list", "items": []})
        );
        assert_eq!(
            decode_kv_value("hash", "[1]"),
            serde_json::json!({"kind": "hash", "fields": []})
        );
        assert_eq!(
            decode_kv_value("set", "{}"),
            serde_json::json!({"kind": "set", "members": []})
        );
        assert_eq!(
            decode_kv_value("zset", "[1]"),
            serde_json::json!({"kind": "zset", "pairs": []})
        );
        // non-numeric zset scores are skipped
        assert_eq!(
            decode_kv_value("zset", r#"{"a":"x"}"#),
            serde_json::json!({"kind": "zset", "pairs": []})
        );
        // int scores accepted
        assert_eq!(
            decode_kv_value("zset", r#"{"a":1}"#),
            serde_json::json!({"kind": "zset", "pairs": [["a", 1.0]]})
        );
        assert_eq!(
            decode_kv_value("hyper", "blob"),
            serde_json::json!({"kind": "hyper", "text": "blob"})
        );
    }

    #[test]
    fn kv_dispatch_error_paths() {
        let mut k = kv();
        let d = |k: &mut Kv, c: &str, a: &[&str]| {
            let args: Vec<String> = a.iter().map(|s| s.to_string()).collect();
            kv_dispatch(k, None, c, &args)
        };
        // incr on a non-numeric string
        d(&mut k, "SET", &["s", "abc"]);
        assert!(!d(&mut k, "INCR", &["s"])["ok"].as_bool().unwrap());
        // rpop empty / missing key
        assert_eq!(
            d(&mut k, "RPOP", &["none"])["value"],
            serde_json::Value::Null
        );
        // LLEN on missing key
        assert_eq!(d(&mut k, "LLEN", &["none"])["value"], 0);
        // hget missing field
        assert_eq!(
            d(&mut k, "HGET", &["none", "f"])["value"],
            serde_json::Value::Null
        );
        // sismember false
        assert_eq!(d(&mut k, "SISMEMBER", &["none", "m"])["value"], 0);
        // type of missing key
        assert_eq!(
            d(&mut k, "TYPE", &["none"])["value"],
            serde_json::Value::Null
        );
        // LPUSH/RPUSH with key only
        assert_eq!(d(&mut k, "LPUSH", &["solo"])["value"], 0);
        // EXPIRE missing key / persist missing key
        assert_eq!(d(&mut k, "EXPIRE", &["none", "1000"])["value"], 0);
        assert_eq!(d(&mut k, "PERSIST", &["none"])["value"], 0);
        assert_eq!(d(&mut k, "EXISTS", &["none"])["value"], 0);
        assert_eq!(d(&mut k, "DEL", &["none"])["value"], 0);
        // INCRBY/DECRBY arg parsing fallbacks
        assert_eq!(d(&mut k, "INCRBY", &["n", "x"])["value"], 1);
        assert_eq!(d(&mut k, "DECRBY", &["n"])["value"], 0);
        // zrank/zscore of missing key
        assert_eq!(
            d(&mut k, "ZSCORE", &["none", "m"])["value"],
            serde_json::Value::Null
        );
        assert_eq!(
            d(&mut k, "ZRANK", &["none", "m"])["value"],
            serde_json::Value::Null
        );
        // ZRANGE defaults
        assert_eq!(
            d(&mut k, "ZRANGE", &["none"])
                .as_array()
                .unwrap_or(&vec![])
                .len(),
            0
        );
        // SET with unparsable EX flag is ignored
        assert_eq!(d(&mut k, "SET", &["kf", "1", "EX=bogus"])["value"], "OK");
    }
    #[test]
    fn value_json_all_variants() {
        assert_eq!(value_json(&Value::Bool(true)), serde_json::json!(true));
        assert_eq!(value_json(&Value::Float(1.25)), serde_json::json!(1.25));
        assert_eq!(
            value_json(&Value::Bytes(vec![1, 2])),
            serde_json::json!("x'0102'")
        );
        assert_eq!(
            value_json(&Value::Array(vec![Value::Int(1)])),
            serde_json::json!("[1]")
        );
    }

    #[test]
    fn kv_dispatch_wrongtype_matrix() {
        let mut k = kv();
        let d = |k: &mut Kv, c: &str, a: &[&str]| {
            let args: Vec<String> = a.iter().map(|s| s.to_string()).collect();
            kv_dispatch(k, None, c, &args)
        };
        d(&mut k, "SET", &["s", "text"]);
        for cmd in [
            &["INCR", "s"][..],
            &["LPUSH", "s", "x"],
            &["LPOP", "s"],
            &["RPOP", "s"],
            &["LRANGE", "s", "0", "-1"],
            &["HSET", "s", "f", "v"],
            &["HGET", "s", "f"],
            &["HGETALL", "s"],
            &["SADD", "s", "m"],
            &["SMEMBERS", "s"],
            &["ZADD", "s", "1.0", "m"],
            &["ZSCORE", "s", "m"],
            &["ZRANGE", "s", "0", "1"],
            &["ZRANK", "s", "m"],
        ] {
            let r = d(&mut k, cmd[0], &cmd[1..]);
            assert!(!r["ok"].as_bool().unwrap(), "{cmd:?} 应报错: {r}");
        }
        // LLEN 错误吞掉返回 0;TTL 错误返回 -2
        assert_eq!(d(&mut k, "LLEN", &["s"])["value"], 0);
        assert_eq!(d(&mut k, "TTL", &["s"])["value"], -1);
        // MULTI/EXEC/DISCARD 正常路径
        assert_eq!(d(&mut k, "MULTI", &[])["value"], "OK");
        assert_eq!(d(&mut k, "DISCARD", &[])["value"], "OK");
        assert!(!d(&mut k, "EXEC", &[])["ok"].as_bool().unwrap());
    }

    #[tokio::test]
    async fn live_server_serves_http() {
        // 用一次性端口起真实服务器(run),再打原生 HTTP 请求
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("live.db");
        let listen = format!("127.0.0.1:{port}");
        let cfg = WebConfig {
            db_path: db_path.clone(),
            token: None,
        };
        let srv = tokio::spawn(async move { run(cfg, &listen).await });
        // 轮询等待端口就绪
        let mut ok = false;
        for _ in 0..50 {
            if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
                ok = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(ok, "服务器未启动");
        async fn http(port: u16, method: &str, uri: &str, body: Option<&str>) -> String {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .unwrap();
            let (head, payload) = match (method, body) {
                ("POST", Some(b)) => (
                    format!(
                        "POST {uri} HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        b.len()
                    ),
                    b.to_string(),
                ),
                _ => (
                    format!("GET {uri} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"),
                    String::new(),
                ),
            };
            s.write_all(head.as_bytes()).await.unwrap();
            s.write_all(payload.as_bytes()).await.unwrap();
            let mut buf = Vec::new();
            s.read_to_end(&mut buf).await.unwrap();
            String::from_utf8_lossy(&buf).to_string()
        }
        let get = |uri: &'static str| http(port, "GET", uri, None);
        let post = |uri: &'static str, body: &'static str| http(port, "POST", uri, Some(body));
        // 首页 + meta + stats
        assert!(get("/").await.contains("docsql console"));
        assert!(get("/api/meta").await.contains("\"tables\":[]"));
        assert!(get("/api/stats").await.contains("kv_keys"));
        assert!(get("/api/keys").await.contains("\"total\":0"));
        assert!(get("/api/events").await.contains("\"latest\":0"));
        assert!(get("/api/kvkey?key=nope").await.contains("no such key"));
        // SQL 与 KV 写入
        assert!(post(
            "/api/sql",
            r#"{"sql":"CREATE TABLE lt (id INT PRIMARY KEY)"}"#
        )
        .await
        .contains("affected"));
        assert!(post("/api/kv", r#"{"command":"SET","args":["lk","lv"]}"#)
            .await
            .contains("\"ok\":true"));
        assert!(get("/api/keys?type=string").await.contains("lk"));
        assert!(get("/api/kvkey?key=lk").await.contains("lv"));
        assert!(post("/api/parse", r#"{"sql":"SELECT 1"}"#)
            .await
            .contains("\"ok\":true"));
        assert!(post("/api/publish", r#"{"channel":"c","message":"m"}"#)
            .await
            .contains("\"seq\":1"));
        srv.abort();
        let _ = srv.await;
    }
    #[test]
    fn list_keys_skips_expired() {
        let mut k = kv();
        let d = |k: &mut Kv, c: &str, a: &[&str]| {
            let args: Vec<String> = a.iter().map(|s| s.to_string()).collect();
            kv_dispatch(k, None, c, &args)
        };
        d(&mut k, "SET", &["gone", "1", "PX=1"]);
        d(&mut k, "SET", &["stay", "2"]);
        std::thread::sleep(std::time::Duration::from_millis(30));
        let keys = list_keys(&mut k, None, None);
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].key, "stay");
        assert_eq!(keys[0].ttl_ms, -1);
    }
}
