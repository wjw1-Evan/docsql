//! docsql Studio — web console backend (SSMS-style management UI).
//!
//! REST API over the shared engine:
//! - `GET  /`                embedded single-page console
//! - `POST /api/sql`         {sql} batch → results / affected / error
//!   (single-statement responses keep the legacy shape)
//! - `POST /api/parse`       {sql} parse-check without executing
//! - `GET  /api/meta`        object-explorer metadata (tables/columns/keys/
//!   indexes/row counts + storage stats)
//! - `GET  /api/stats`       server + storage + data counters
//!
//! Auth v1: requests must send `X-Docsql-Token` when the server token is set.

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Html;
use axum::routing::{get, post};
use axum::{Json, Router};
use docsql_core::engine::{Database, ExecOutcome};
use docsql_core::value::Value;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

pub const CONSOLE_VERSION: &str = "1.0";

pub struct WebState {
    pub db: Mutex<Database>,
    pub token: Option<String>,
    pub db_path: PathBuf,
    pub started: Instant,
}

pub struct WebConfig {
    pub db_path: PathBuf,
    pub token: Option<String>,
}

pub async fn run(cfg: WebConfig, listen: &str) -> std::io::Result<()> {
    let db =
        Database::open(&cfg.db_path).map_err(|e| std::io::Error::other(format!("open db: {e}")))?;
    let state = Arc::new(WebState {
        db: Mutex::new(db),
        token: cfg.token,
        db_path: cfg.db_path,
        started: Instant::now(),
    });
    let app = Router::new()
        .route("/", get(index))
        .route("/api/sql", post(api_sql))
        .route("/api/parse", post(api_parse))
        .route("/api/meta", get(api_meta))
        .route("/api/stats", get(api_stats))
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
        // Constant-time compare: a plain == short-circuits on the first
        // differing byte and leaks a (noisy but real) timing oracle.
        Some(expect)
            if token.is_some_and(|t| constant_time_eq(t.as_bytes(), expect.as_bytes())) =>
        {
            None
        }
        Some(_) => Some(StatusCode::UNAUTHORIZED),
    }
}

/// Length-guarded XOR fold (mirrors docsql-server's crypto helper).
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
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
pub fn run_sql(db: &mut Database, sql: &str) -> serde_json::Value {
    let batch = db.execute_batch(sql);
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
    let mut db = state.db.lock().unwrap_or_else(|p| p.into_inner());
    Ok(Json(run_sql(&mut db, &body.sql)))
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

/// Assemble the object-explorer payload (server / storage / tables).
pub fn build_meta(db: &mut Database, db_path: &Path, started: Instant) -> serde_json::Value {
    let catalog: Vec<_> = db.catalog().into_iter().collect();
    let mut tables = Vec::new();
    let mut total_rows = 0u64;
    for t in &catalog {
        let row_count = match db.execute(&format!(
            "SELECT COUNT(*) FROM \"{}\"",
            t.name.replace('"', "\"\"")
        )) {
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
    serde_json::json!({
        "server": {
            "name": "docsql",
            "version": CONSOLE_VERSION,
            "uptime_ms": started.elapsed().as_millis() as u64,
        },
        "storage": {
            "page_size": db.page_size(),
            "num_pages": db.num_pages(),
            "db_bytes": file_bytes(db_path),
            "wal_bytes": file_bytes(&wal_path(db_path)),
        },
        "totals": {"tables": tables.len(), "rows": total_rows},
        "tables": tables,
    })
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
    let mut db = state.db.lock().unwrap_or_else(|p| p.into_inner());
    Ok(Json(build_meta(&mut db, &state.db_path, state.started)))
}

async fn api_stats(
    State(state): State<Arc<WebState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if let Some(code) = check_auth(&state, &headers) {
        return Err(code);
    }
    let db = state.db.lock().unwrap_or_else(|p| p.into_inner());
    let user_tables = db.catalog().len();
    let num_pages = db.num_pages();
    let page_size = db.page_size();
    Ok(Json(serde_json::json!({
        "tables": user_tables,
        "pages": num_pages,
        "page_size": page_size,
        "db_bytes": file_bytes(&state.db_path),
        "wal_bytes": file_bytes(&wal_path(&state.db_path)),
        "uptime_ms": state.started.elapsed().as_millis() as u64,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn db() -> Database {
        Database::in_memory().unwrap()
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
        let mut d = db();
        let r = run_sql(&mut d, "CREATE TABLE t (id INT PRIMARY KEY)");
        assert_eq!(r, serde_json::json!({"kind": "affected", "count": 0}));
        let r = run_sql(&mut d, "INSERT INTO t VALUES (1)");
        assert_eq!(r, serde_json::json!({"kind": "affected", "count": 1}));
        let r = run_sql(&mut d, "SELECT id FROM t");
        assert_eq!(r["kind"], "rows");
        assert_eq!(r["rows"][0][0], 1);
        let r = run_sql(&mut d, "SELECT * FROM missing");
        assert_eq!(r["kind"], "error");
        assert!(r["message"].as_str().unwrap().contains("missing"));
    }

    #[test]
    fn run_sql_batch_reports_each_statement() {
        let mut d = db();
        let r = run_sql(
            &mut d,
            "CREATE TABLE b (id INT); INSERT INTO b VALUES (1), (2); SELECT id FROM b ORDER BY id",
        );
        assert_eq!(r["kind"], "batch");
        let results = r["results"].as_array().unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[2]["rows"].as_array().unwrap().len(), 2);
        assert!(r["error"].is_null());

        // Error mid-batch: partial results + statement index.
        let r = run_sql(&mut d, "INSERT INTO b VALUES (3); SELECT * FROM nope");
        assert_eq!(r["kind"], "batch");
        assert_eq!(r["error"]["statement"], 1);
        assert_eq!(r["results"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn build_meta_reports_tables() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.db");
        let mut d = Database::open(&path).unwrap();
        d.execute("CREATE TABLE m (id INT PRIMARY KEY, name TEXT)")
            .unwrap();
        d.execute("INSERT INTO m VALUES (1, 'a'), (2, 'b')")
            .unwrap();
        let meta = build_meta(&mut d, &path, Instant::now());
        assert_eq!(meta["totals"]["tables"], 1);
        assert_eq!(meta["tables"][0]["row_count"], 2);
        assert_eq!(meta["tables"][0]["columns"].as_array().unwrap().len(), 2);
        assert_eq!(meta["tables"][0]["keys"][0], "id");
        assert_eq!(meta["storage"]["page_size"], 4096);
        assert!(meta["storage"]["num_pages"].as_u64().unwrap() >= 2);
    }
}
