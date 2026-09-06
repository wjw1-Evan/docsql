//! Web console for docsql.
//!
//! REST API over the shared engine:
//! - `GET  /`                embedded single-page console
//! - `POST /api/sql`         {sql} → rows / affected / error
//! - `GET  /api/keys`        KV key browser (type + TTL)
//! - `POST /api/kv`          {command, args} → command dispatch
//! - `GET  /api/stats`       table/key counts
//!
//! Auth v1: requests must send `X-Docsql-Token` when the server token is set.

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Html;
use axum::routing::{get, post};
use axum::{Json, Router};
use docsql_core::engine::ExecOutcome;
use docsql_core::value::Value;
use docsql_kv::Kv;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

pub struct WebState {
    pub kv: Mutex<Kv>,
    pub token: Option<String>,
}

pub struct WebConfig {
    pub db_path: PathBuf,
    pub token: Option<String>,
}

pub async fn run(cfg: WebConfig, listen: &str) -> std::io::Result<()> {
    let kv = Kv::open(&cfg.db_path).map_err(|e| std::io::Error::other(format!("open db: {e}")))?;
    let state = Arc::new(WebState {
        kv: Mutex::new(kv),
        token: cfg.token,
    });
    let app = Router::new()
        .route("/", get(index))
        .route("/api/sql", post(api_sql))
        .route("/api/keys", get(api_keys))
        .route("/api/kv", post(api_kv))
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

async fn api_sql(
    State(state): State<Arc<WebState>>,
    headers: HeaderMap,
    Json(body): Json<SqlBody>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if let Some(code) = check_auth(&state, &headers) {
        return Err(code);
    }
    let mut kv = state.kv.lock().unwrap();
    match kv.db.execute(&body.sql) {
        Ok(ExecOutcome::Rows(r)) => Ok(Json(serde_json::json!({
            "kind": "rows",
            "columns": r.columns,
            "rows": r.rows.iter().map(|row| row.iter().map(value_json).collect::<Vec<_>>()).collect::<Vec<_>>(),
        }))),
        Ok(ExecOutcome::Affected(n)) => {
            Ok(Json(serde_json::json!({"kind": "affected", "count": n})))
        }
        Err(e) => Ok(Json(
            serde_json::json!({"kind": "error", "message": e.to_string()}),
        )),
    }
}

fn value_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::json!(i),
        Value::Float(f) => serde_json::json!(f),
        Value::Str(s) => serde_json::json!(s),
        other => serde_json::json!(other.to_string()),
    }
}

async fn api_keys(
    State(state): State<Arc<WebState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if let Some(code) = check_auth(&state, &headers) {
        return Err(code);
    }
    let mut kv = state.kv.lock().unwrap();
    let result = kv
        .db
        .execute("SELECT \"key\", type, expire_at FROM _kv WHERE expire_at = 0 OR expire_at > 0");
    let mut keys = Vec::new();
    if let Ok(ExecOutcome::Rows(r)) = result {
        let now = docsql_kv::now_ms();
        for row in r.rows {
            let (Some(k), t, exp) = (
                row[0].as_str(),
                row[1].as_str().unwrap_or(""),
                row[2].as_i64().unwrap_or(0),
            ) else {
                continue;
            };
            if exp > 0 && exp <= now {
                continue; // expired
            }
            keys.push(serde_json::json!({
                "key": k,
                "type": t,
                "ttl_ms": if exp > 0 { exp - now } else { -1 },
            }));
        }
    }
    Ok(Json(serde_json::json!({ "keys": keys })))
}

#[derive(serde::Deserialize)]
struct KvBody {
    command: String,
    #[serde(default)]
    args: Vec<String>,
}

async fn api_kv(
    State(state): State<Arc<WebState>>,
    headers: HeaderMap,
    Json(body): Json<KvBody>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if let Some(code) = check_auth(&state, &headers) {
        return Err(code);
    }
    use docsql_kv::SetOpts;
    let mut kv = state.kv.lock().unwrap();
    let args: Vec<&str> = body.args.iter().map(|s| s.as_str()).collect();
    let out = match body.command.to_uppercase().as_str() {
        "GET" => match kv.get(args.first().copied().unwrap_or("")) {
            Ok(Some(v)) => serde_json::json!({"ok": true, "value": v}),
            Ok(None) => serde_json::json!({"ok": true, "value": null}),
            Err(e) => serde_json::json!({"ok": false, "error": e.to_string()}),
        },
        "SET" => {
            let opts = SetOpts::default();
            match kv.set(
                args.first().copied().unwrap_or(""),
                args.get(1).copied().unwrap_or(""),
                opts,
            ) {
                Ok(done) => serde_json::json!({"ok": done}),
                Err(e) => serde_json::json!({"ok": false, "error": e.to_string()}),
            }
        }
        "DEL" => match kv.del(args.first().copied().unwrap_or("")) {
            Ok(done) => serde_json::json!({"ok": done}),
            Err(e) => serde_json::json!({"ok": false, "error": e.to_string()}),
        },
        "PUBLISH" => {
            serde_json::json!({"ok": true, "note": "pub/sub is served on the TCP protocol port"})
        }
        other => serde_json::json!({"ok": false, "error": format!("unknown command: {other}")}),
    };
    Ok(Json(out))
}

async fn api_stats(
    State(state): State<Arc<WebState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if let Some(code) = check_auth(&state, &headers) {
        return Err(code);
    }
    let mut kv = state.kv.lock().unwrap();
    let mut key_count = 0usize;
    if let Ok(ExecOutcome::Rows(r)) = kv.db.execute("SELECT COUNT(type) FROM _kv") {
        if let Some(row) = r.rows.first() {
            key_count = row[0].as_i64().unwrap_or(0) as usize;
        }
    }
    Ok(Json(serde_json::json!({ "kv_keys": key_count })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn console_page_served() {
        assert!(include_str!("console.html").contains("docsql"));
    }

    #[test]
    fn value_json_mapping() {
        assert_eq!(value_json(&Value::Int(3)), serde_json::json!(3));
        assert_eq!(value_json(&Value::Str("x".into())), serde_json::json!("x"));
        assert_eq!(value_json(&Value::Null), serde_json::Value::Null);
    }
}
