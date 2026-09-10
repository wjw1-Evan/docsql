//! DocSQL Studio — web console backend (SSMS-style management tool).
//!
//! The console keeps no data of its own: it is a management client for
//! DocSQL nodes, and every data operation runs on a real node over the
//! wire protocol:
//!
//! - `GET  /`                embedded single-page console
//! - `POST /api/sql`         {sql, node?} batch → results / affected / error
//!   (single-statement responses keep the legacy shape)
//! - `POST /api/parse`       {sql} parse-check without executing (pure
//!   syntax check, stays local — no storage involved)
//! - `GET  /api/meta`        object-explorer metadata (tables/columns/keys/
//!   indexes/row counts + storage stats; `observed` per table lists the
//!   data-derived field union next to the declared columns)
//! - `GET  /api/stats`       server + storage + data counters
//! - `GET  /api/cluster`     probe every DOCSQL_PEERS node over the wire
//!   protocol (PING liveness + REQ_STATUS report) for the cluster page,
//!   plus `default` — the console's default managed node
//! - `GET  /api/logs`        logs page data: the console's own statement
//!   audit ring + every DOCSQL_PEERS node's REQ_LOGS report (statement
//!   audit + replication/sync events)
//!
//! Managed nodes: data endpoints accept an optional `node` (`?node=` query
//! / JSON field) naming one of the configured DOCSQL_PEERS addresses;
//! without it the call lands on the default managed node (startup argument
//! / DOCSQL_UPSTREAM, else the first peer). The console connects to nodes
//! as a database client — it stores nothing itself, so all data lives in
//! the cluster. Targets are restricted to operator configuration (SSRF
//! guard); node connections authenticate with the server's own
//! DOCSQL_TOKEN, never with a browser-supplied value.
//!
//! Auth v1: requests must send `X-Docsql-Token` when the server token is set.

use axum::extract::{ConnectInfo, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use docsql_core::engine::Database;
use docsql_core::proto::{self, Frame};
use docsql_server::querylog::{self, LogEntry};
use serde_json::json;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub mod auth;

pub struct WebState {
    /// Default managed node: where data calls without an explicit `node`
    /// land. A client connection target, not local storage — the console
    /// holds no database of its own.
    pub upstream: Option<String>,
    pub token: Option<String>,
    /// Cluster nodes to monitor and switch between (DOCSQL_PEERS).
    pub peers: Vec<String>,
    /// Console-side statement audit (the logs page's 控制台 source).
    /// Same ring type as the server's docsql_log so one payload builder
    /// serves both.
    pub query_log: querylog::QueryLog,
    /// Always empty here — the console never replicates — but the shared
    /// logs payload expects the pair.
    pub sync_log: querylog::SyncLog,
    /// The console's own username/password account (first-use setup).
    /// `None` when `DOCSQL_WEB_AUTH_FILE` is unset: legacy behavior, token
    /// header gate only.
    pub auth: Option<AuthShared>,
}

/// Shared auth surface: the credential file store, in-memory sessions, and
/// per-source login-failure lockout.
pub struct AuthShared {
    store: Mutex<auth::AuthStore>,
    sessions: auth::Sessions,
    lockout: Mutex<auth::Lockout>,
}

impl AuthShared {
    fn open(path: &str) -> Result<Self, String> {
        Ok(AuthShared {
            store: Mutex::new(auth::AuthStore::open(std::path::Path::new(path))?),
            sessions: auth::Sessions::new(),
            lockout: Mutex::new(auth::Lockout::new()),
        })
    }

    fn mode(&self) -> auth::AuthMode {
        self.store.lock().unwrap().mode()
    }

    fn username(&self) -> Option<String> {
        self.store.lock().unwrap().username().map(String::from)
    }
}

pub struct WebConfig {
    /// Default managed node (see `WebState::upstream`).
    pub upstream: Option<String>,
    pub token: Option<String>,
    /// Cluster nodes to monitor for the cluster status page.
    pub peers: Vec<String>,
    /// Path of the console credential file. `None` (or empty) disables the
    /// username/password gate entirely (legacy behavior).
    pub auth_file: Option<String>,
}

pub async fn run(cfg: WebConfig, listen: &str) -> std::io::Result<()> {
    let auth = match cfg.auth_file.as_deref().filter(|p| !p.is_empty()) {
        Some(path) => Some(AuthShared::open(path).map_err(std::io::Error::other)?),
        None => None,
    };
    let state = Arc::new(WebState {
        upstream: cfg.upstream,
        token: cfg.token,
        peers: cfg.peers,
        query_log: querylog::QueryLog::new(),
        sync_log: querylog::SyncLog::new(1),
        auth,
    });
    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind(listen).await?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .map_err(std::io::Error::other)
}

fn build_router(state: Arc<WebState>) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/api/sql", post(api_sql))
        .route("/api/parse", post(api_parse))
        .route("/api/meta", get(api_meta))
        .route("/api/stats", get(api_stats))
        .route("/api/cluster", get(api_cluster))
        .route("/api/logs", get(api_logs))
        .route("/api/auth/status", get(auth_status))
        .route("/api/auth/setup", post(auth_setup))
        .route("/api/auth/login", post(auth_login))
        .route("/api/auth/logout", post(auth_logout))
        .with_state(state)
}

/// The API gate. Legacy behavior: `X-Docsql-Token` must match the server
/// token when one is configured, nothing otherwise. With the console
/// account active, a valid session cookie also passes; the token keeps
/// working as the programmatic bypass; and until the account is set up,
/// data endpoints stay reachable when no token is configured (the setup
/// happens on first use and closes that window).
fn check_auth(state: &WebState, headers: &HeaderMap) -> Option<StatusCode> {
    let token = headers.get("X-Docsql-Token").and_then(|v| v.to_str().ok());
    let token_ok = match &state.token {
        None => true,
        // Constant-time compare: a plain == short-circuits on the first
        // differing byte and leaks a (noisy but real) timing oracle.
        Some(expect) => token.is_some_and(|t| constant_time_eq(t.as_bytes(), expect.as_bytes())),
    };
    let Some(auth) = &state.auth else {
        return if token_ok {
            None
        } else {
            Some(StatusCode::UNAUTHORIZED)
        };
    };
    if session_from(headers).is_some_and(|t| auth.sessions.verify(&t)) {
        return None;
    }
    if token_ok && state.token.is_some() {
        return None;
    }
    if matches!(auth.mode(), auth::AuthMode::Setup) && state.token.is_none() {
        return None;
    }
    Some(StatusCode::UNAUTHORIZED)
}

fn session_from(headers: &HeaderMap) -> Option<String> {
    let raw = headers.get(header::COOKIE)?.to_str().ok()?;
    raw.split(';').find_map(|pair| {
        let pair = pair.trim();
        pair.strip_prefix("docsql_session=").map(String::from)
    })
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

// ---- console account (first-use setup + login) ----

#[derive(serde::Deserialize)]
struct CredentialsBody {
    username: String,
    password: String,
}

fn session_cookie(token: &str) -> String {
    format!(
        "{}={}; HttpOnly; Path=/; SameSite=Lax; Max-Age={}",
        auth::SESSION_COOKIE,
        token,
        auth::SESSION_TTL.as_secs()
    )
}

fn with_session_cookie(resp: Response, token: &str) -> Response {
    let mut resp = resp;
    let value = HeaderValue::from_str(&session_cookie(token)).expect("cookie header value");
    resp.headers_mut().append(header::SET_COOKIE, value);
    resp
}

fn json_response(status: StatusCode, body: serde_json::Value) -> Response {
    (status, Json(body)).into_response()
}

async fn auth_status(State(state): State<Arc<WebState>>) -> Response {
    let Some(a) = &state.auth else {
        return json_response(StatusCode::OK, json!({"mode": "off"}));
    };
    match a.mode() {
        auth::AuthMode::Setup => json_response(StatusCode::OK, json!({"mode": "setup"})),
        auth::AuthMode::Login => json_response(
            StatusCode::OK,
            json!({"mode": "login", "username": a.username().unwrap_or_default()}),
        ),
    }
}

/// First use: create the console account, then land the caller in a
/// session. One-shot — once the credential file exists this endpoint only
/// ever returns 409.
async fn auth_setup(
    State(state): State<Arc<WebState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Json(body): Json<CredentialsBody>,
) -> Response {
    let Some(a) = &state.auth else {
        return json_response(
            StatusCode::NOT_FOUND,
            json!({"error": "console account is not enabled"}),
        );
    };
    if a.lockout.lock().unwrap().check(peer.ip()) {
        return json_response(
            StatusCode::TOO_MANY_REQUESTS,
            json!({"error": "尝试次数过多,请一分钟后再试"}),
        );
    }
    let result = a
        .store
        .lock()
        .unwrap()
        .setup(&body.username, &body.password);
    match result {
        Ok(creds) => {
            a.lockout.lock().unwrap().reset(peer.ip());
            let token = a.sessions.create();
            with_session_cookie(
                json_response(
                    StatusCode::OK,
                    json!({"ok": true, "username": creds.username}),
                ),
                &token,
            )
        }
        Err(auth::SetupError::Exists) => {
            a.lockout.lock().unwrap().record_failure(peer.ip());
            json_response(
                StatusCode::CONFLICT,
                json!({"error": "账号已存在,请直接登录"}),
            )
        }
        Err(auth::SetupError::Invalid(msg)) => {
            json_response(StatusCode::BAD_REQUEST, json!({"error": msg}))
        }
        Err(auth::SetupError::Io(msg)) => json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error": format!("凭据写入失败:{msg}")}),
        ),
    }
}

async fn auth_login(
    State(state): State<Arc<WebState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Json(body): Json<CredentialsBody>,
) -> Response {
    let Some(a) = &state.auth else {
        return json_response(
            StatusCode::NOT_FOUND,
            json!({"error": "console account is not enabled"}),
        );
    };
    if a.lockout.lock().unwrap().check(peer.ip()) {
        return json_response(
            StatusCode::TOO_MANY_REQUESTS,
            json!({"error": "尝试次数过多,请一分钟后再试"}),
        );
    }
    let ok = a
        .store
        .lock()
        .unwrap()
        .verify(&body.username, &body.password);
    if !ok {
        // Same generic error for wrong username and wrong password —
        // verify() burns a derivation either way, and the text must not
        // narrow the guess.
        a.lockout.lock().unwrap().record_failure(peer.ip());
        return json_response(
            StatusCode::UNAUTHORIZED,
            json!({"error": "用户名或密码不正确"}),
        );
    }
    a.lockout.lock().unwrap().reset(peer.ip());
    let username = a.username().unwrap_or_default();
    let token = a.sessions.create();
    with_session_cookie(
        json_response(StatusCode::OK, json!({"ok": true, "username": username})),
        &token,
    )
}

async fn auth_logout(State(state): State<Arc<WebState>>, headers: HeaderMap) -> Response {
    if let Some(a) = &state.auth {
        if let Some(token) = session_from(&headers) {
            a.sessions.drop_session(&token);
        }
    }
    let clear = format!(
        "{}=; HttpOnly; Path=/; SameSite=Lax; Max-Age=0",
        auth::SESSION_COOKIE
    );
    let mut resp = json_response(StatusCode::OK, json!({"ok": true}));
    resp.headers_mut().append(
        header::SET_COOKIE,
        HeaderValue::from_str(&clear).expect("cookie header value"),
    );
    resp
}

#[derive(serde::Deserialize)]
struct SqlBody {
    sql: String,
    /// Managed-node override (node switching): must be one of DOCSQL_PEERS.
    node: Option<String>,
}

/// `?node=` query parameter for the GET endpoints.
#[derive(serde::Deserialize)]
struct NodeParams {
    node: Option<String>,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// One console SQL call → one audit entry. `peer` is the node the statement
/// actually ran on (default managed node or the explicit `node` target).
/// Affected/error are read back from the response shape the client already
/// sees.
fn record_console_sql(
    log: &querylog::QueryLog,
    peer: &str,
    sql: &str,
    ms: f64,
    out: &serde_json::Value,
) {
    let affected = if out["kind"] == "affected" {
        out["count"].as_i64()
    } else if out["kind"] == "batch" {
        let counts: Vec<i64> = out["results"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter(|o| o["kind"] == "affected")
                    .filter_map(|o| o["count"].as_i64())
                    .collect()
            })
            .unwrap_or_default();
        (!counts.is_empty()).then(|| counts.iter().sum())
    } else {
        None
    };
    let error = if out["kind"] == "error" {
        out["message"].as_str().map(String::from)
    } else {
        out["error"]["message"].as_str().map(String::from)
    };
    log.push(LogEntry {
        ts_ms: now_ms(),
        peer: peer.into(),
        sql: sql.chars().take(512).collect(),
        ms,
        affected,
        error,
        replicated: false,
    });
}

async fn api_sql(
    State(state): State<Arc<WebState>>,
    headers: HeaderMap,
    Json(body): Json<SqlBody>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if let Some(code) = check_auth(&state, &headers) {
        return Err(code);
    }
    let target = match target_for(&state, &body.node) {
        Ok(t) => t,
        Err(e) => return Ok(Json(e)),
    };
    let started = Instant::now();
    let out = remote_sql(&target, state.token.as_deref(), &body.sql).await;
    record_console_sql(
        &state.query_log,
        &target,
        &body.sql,
        started.elapsed().as_secs_f64() * 1000.0,
        &out,
    );
    Ok(Json(out))
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

async fn api_meta(
    State(state): State<Arc<WebState>>,
    headers: HeaderMap,
    Query(params): Query<NodeParams>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if let Some(code) = check_auth(&state, &headers) {
        return Err(code);
    }
    match target_for(&state, &params.node) {
        Ok(addr) => Ok(Json(remote_meta(&addr, state.token.as_deref()).await)),
        Err(e) => Ok(Json(e)),
    }
}

async fn api_stats(
    State(state): State<Arc<WebState>>,
    headers: HeaderMap,
    Query(params): Query<NodeParams>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if let Some(code) = check_auth(&state, &headers) {
        return Err(code);
    }
    match target_for(&state, &params.node) {
        Ok(addr) => Ok(Json(remote_stats(&addr, state.token.as_deref()).await)),
        Err(e) => Ok(Json(e)),
    }
}

/// Per-node probe budget: a wedged node must not stall the cluster page.
const PROBE_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
const PROBE_IO_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);
/// A status report is a small JSON object; cap the allocation.
const PROBE_RECV_CAP: usize = 1024 * 1024;
/// A logs report carries up to 2 × 1000 entries with statement texts —
/// bigger than a status report, still bounded by the requested limit.
const LOGS_RECV_CAP: usize = 4 * 1024 * 1024;

async fn write_frame(stream: &mut TcpStream, frame: &Frame) -> std::io::Result<()> {
    let bytes = frame.encode().map_err(std::io::Error::other)?;
    tokio::time::timeout(PROBE_IO_TIMEOUT, stream.write_all(&bytes)).await??;
    tokio::time::timeout(PROBE_IO_TIMEOUT, stream.flush()).await??;
    Ok(())
}

async fn read_response_frame(stream: &mut TcpStream, cap: usize) -> std::io::Result<Frame> {
    let mut header = [0u8; proto::HEADER_LEN];
    tokio::time::timeout(PROBE_IO_TIMEOUT, stream.read_exact(&mut header)).await??;
    let len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
    if len > cap {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "node response too large",
        ));
    }
    let mut buf = header.to_vec();
    let mut payload = vec![0u8; len];
    tokio::time::timeout(PROBE_IO_TIMEOUT, stream.read_exact(&mut payload)).await??;
    buf.extend_from_slice(&payload);
    let (f, _) = Frame::decode(&buf).map_err(std::io::Error::other)?;
    Ok(f)
}

/// Probe one cluster node: REQ_PING for liveness + latency, then (after AUTH
/// when a token is configured) REQ_STATUS for the full report. The probe
/// never writes to the node. Transport-encrypted nodes reject plaintext
/// frames and surface as reachable with an explanatory error.
pub async fn probe_node(addr: &str, token: Option<&str>) -> serde_json::Value {
    match probe_node_inner(addr, token).await {
        Ok((latency_ms, status)) => serde_json::json!({
            "addr": addr,
            "reachable": true,
            "latency_ms": latency_ms,
            "error": serde_json::Value::Null,
            "status": status,
        }),
        Err((reachable, message)) => serde_json::json!({
            "addr": addr,
            "reachable": reachable,
            "latency_ms": serde_json::Value::Null,
            "error": message,
            "status": serde_json::Value::Null,
        }),
    }
}

/// `reachable` in the error says whether PING got a PONG before the failure
/// (a node can be alive yet refuse STATUS, e.g. wrong console token).
async fn probe_node_inner(
    addr: &str,
    token: Option<&str>,
) -> Result<(f64, serde_json::Value), (bool, String)> {
    let mut stream = tokio::time::timeout(PROBE_CONNECT_TIMEOUT, TcpStream::connect(addr))
        .await
        .map_err(|e| (false, e.to_string()))?
        .map_err(|e| (false, e.to_string()))?;
    // Liveness: PING needs no authenticated session.
    let started = Instant::now();
    write_frame(&mut stream, &Frame::new(proto::REQ_PING, vec![]))
        .await
        .map_err(|e| (false, e.to_string()))?;
    let pong = read_response_frame(&mut stream, PROBE_RECV_CAP)
        .await
        .map_err(|e| (false, e.to_string()))?;
    let latency_ms = started.elapsed().as_secs_f64() * 1000.0;
    if pong.frame_type != proto::RESP_PONG {
        return Err((
            true,
            format!("unexpected response to PING: {:#06x}", pong.frame_type),
        ));
    }
    if let Some(t) = token {
        write_frame(
            &mut stream,
            &Frame::new(proto::REQ_AUTH, t.as_bytes().to_vec()),
        )
        .await
        .map_err(|e| (true, e.to_string()))?;
        let auth = read_response_frame(&mut stream, PROBE_RECV_CAP)
            .await
            .map_err(|e| (true, e.to_string()))?;
        if auth.frame_type == proto::RESP_ERROR {
            return Err((
                true,
                format!(
                    "node rejected AUTH: {}",
                    String::from_utf8_lossy(&auth.payload)
                ),
            ));
        }
    }
    write_frame(&mut stream, &Frame::new(proto::REQ_STATUS, vec![]))
        .await
        .map_err(|e| (true, e.to_string()))?;
    let resp = read_response_frame(&mut stream, PROBE_RECV_CAP)
        .await
        .map_err(|e| (true, e.to_string()))?;
    if resp.frame_type != proto::RESP_STATUS {
        return Err((
            true,
            format!(
                "unexpected response to STATUS: {}",
                String::from_utf8_lossy(&resp.payload)
            ),
        ));
    }
    let status = serde_json::from_slice(&resp.payload)
        .map_err(|e| (true, format!("bad status payload: {e}")))?;
    Ok((latency_ms, status))
}

async fn api_cluster(
    State(state): State<Arc<WebState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if let Some(code) = check_auth(&state, &headers) {
        return Err(code);
    }
    let token = state.token.clone();
    // Probes run concurrently, collected in configured order.
    let handles: Vec<_> = state
        .peers
        .iter()
        .map(|addr| {
            let addr = addr.clone();
            let token = token.clone();
            tokio::spawn(async move { probe_node(&addr, token.as_deref()).await })
        })
        .collect();
    let mut nodes = Vec::with_capacity(handles.len());
    for h in handles {
        let node = h.await.unwrap_or_else(|e| {
            serde_json::json!({
                "addr": "unknown",
                "reachable": false,
                "latency_ms": serde_json::Value::Null,
                "error": format!("probe task failed: {e}"),
                "status": serde_json::Value::Null,
            })
        });
        nodes.push(node);
    }
    Ok(Json(serde_json::json!({
        "nodes": nodes,
        "default": state.upstream,
    })))
}

/// Fetch one node's logs report (REQ_LOGS, read-only like the status
/// probe). `reachable` distinguishes "cannot connect" from "connected but
/// refused AUTH / bad response".
pub async fn fetch_node_logs(addr: &str, token: Option<&str>, limit: usize) -> serde_json::Value {
    match fetch_node_logs_inner(addr, token, limit).await {
        Ok(logs) => serde_json::json!({
            "addr": addr,
            "reachable": true,
            "error": serde_json::Value::Null,
            "logs": logs,
        }),
        Err((reachable, message)) => serde_json::json!({
            "addr": addr,
            "reachable": reachable,
            "error": message,
            "logs": serde_json::Value::Null,
        }),
    }
}

async fn fetch_node_logs_inner(
    addr: &str,
    token: Option<&str>,
    limit: usize,
) -> Result<serde_json::Value, (bool, String)> {
    let mut stream = tokio::time::timeout(PROBE_CONNECT_TIMEOUT, TcpStream::connect(addr))
        .await
        .map_err(|e| (false, e.to_string()))?
        .map_err(|e| (false, e.to_string()))?;
    if let Some(t) = token {
        write_frame(
            &mut stream,
            &Frame::new(proto::REQ_AUTH, t.as_bytes().to_vec()),
        )
        .await
        .map_err(|e| (true, e.to_string()))?;
        let auth = read_response_frame(&mut stream, LOGS_RECV_CAP)
            .await
            .map_err(|e| (true, e.to_string()))?;
        if auth.frame_type == proto::RESP_ERROR {
            return Err((
                true,
                format!(
                    "node rejected AUTH: {}",
                    String::from_utf8_lossy(&auth.payload)
                ),
            ));
        }
    }
    let body = serde_json::to_vec(&serde_json::json!({"limit": limit})).unwrap_or_default();
    write_frame(&mut stream, &Frame::new(proto::REQ_LOGS, body))
        .await
        .map_err(|e| (true, e.to_string()))?;
    let resp = read_response_frame(&mut stream, LOGS_RECV_CAP)
        .await
        .map_err(|e| (true, e.to_string()))?;
    if resp.frame_type != proto::RESP_LOGS {
        return Err((
            true,
            format!("unexpected response to LOGS: {:#06x}", resp.frame_type),
        ));
    }
    serde_json::from_slice(&resp.payload).map_err(|e| (true, format!("bad logs payload: {e}")))
}

#[derive(serde::Deserialize)]
struct LogsParams {
    limit: Option<usize>,
}

/// Logs page data: the console's own statement audit plus every peer
/// node's REQ_LOGS report (statement audit + sync events), newest entries
/// within each section.
async fn api_logs(
    State(state): State<Arc<WebState>>,
    headers: HeaderMap,
    Query(params): Query<LogsParams>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if let Some(code) = check_auth(&state, &headers) {
        return Err(code);
    }
    let limit = params
        .limit
        .unwrap_or(querylog::LOGS_DEFAULT_LIMIT)
        .clamp(1, querylog::LOGS_MAX_LIMIT);
    let local: serde_json::Value = serde_json::from_slice(&querylog::logs_payload(
        &state.query_log,
        &state.sync_log,
        limit,
    ))
    .unwrap_or_else(|_| serde_json::json!({"query": [], "sync": []}));
    let token = state.token.clone();
    // Node fetches run concurrently, collected in configured order — same
    // pattern as the cluster probes.
    let handles: Vec<_> = state
        .peers
        .iter()
        .map(|addr| {
            let addr = addr.clone();
            let token = token.clone();
            tokio::spawn(async move { fetch_node_logs(&addr, token.as_deref(), limit).await })
        })
        .collect();
    let mut nodes = Vec::with_capacity(handles.len());
    for h in handles {
        let node = h.await.unwrap_or_else(|e| {
            serde_json::json!({
                "addr": "unknown",
                "reachable": false,
                "error": format!("logs task failed: {e}"),
                "logs": serde_json::Value::Null,
            })
        });
        nodes.push(node);
    }
    Ok(Json(serde_json::json!({ "local": local, "nodes": nodes })))
}

/* ================= Node switching (managed-node selection) ================= */

/// Validate a requested managed node: only addresses configured in
/// DOCSQL_PEERS are dialable from the console backend. This is both an
/// authorization check and an SSRF guard — the browser never picks
/// arbitrary hosts for the backend to connect to.
fn resolve_node(
    state: &WebState,
    node: &Option<String>,
) -> Result<Option<String>, serde_json::Value> {
    match node.as_deref().map(str::trim).filter(|n| !n.is_empty()) {
        None => Ok(None),
        Some(addr) if state.peers.iter().any(|p| p == addr) => Ok(Some(addr.to_string())),
        Some(addr) => Err(serde_json::json!({
            "error": format!("未知节点 {addr}:不在 DOCSQL_PEERS 中(可用节点见 /api/cluster)"),
        })),
    }
}

/// Resolve the effective target for a data call: an explicit `node` (one of
/// DOCSQL_PEERS) wins, otherwise the default managed node. Without either,
/// the console has nowhere to send the call — a configuration gap reported
/// in-band.
fn target_for(state: &WebState, node: &Option<String>) -> Result<String, serde_json::Value> {
    match resolve_node(state, node)? {
        Some(addr) => Ok(addr),
        None => state.upstream.clone().ok_or_else(|| {
            serde_json::json!({
                "error": "未配置管理目标节点:启动参数或 DOCSQL_UPSTREAM 指定要管理的节点地址后才能执行数据操作",
            })
        }),
    }
}

/// Remote-node budget: SQL can be heavier than a status probe (scans, big
/// result sets), so the budget is wider than the probe's.
const NODE_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);
const NODE_IO_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);
/// Mirrors the server's inbound frame cap (64 MiB) so large result sets
/// round-trip untruncated.
const NODE_RECV_CAP: usize = 64 * 1024 * 1024;

async fn node_write_frame(stream: &mut TcpStream, frame: &Frame) -> std::io::Result<()> {
    let bytes = frame.encode().map_err(std::io::Error::other)?;
    tokio::time::timeout(NODE_IO_TIMEOUT, stream.write_all(&bytes)).await??;
    tokio::time::timeout(NODE_IO_TIMEOUT, stream.flush()).await??;
    Ok(())
}

async fn node_read_frame(stream: &mut TcpStream) -> std::io::Result<Frame> {
    let mut header = [0u8; proto::HEADER_LEN];
    tokio::time::timeout(NODE_IO_TIMEOUT, stream.read_exact(&mut header)).await??;
    let len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
    if len > NODE_RECV_CAP {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "node response too large",
        ));
    }
    let mut buf = header.to_vec();
    let mut payload = vec![0u8; len];
    tokio::time::timeout(NODE_IO_TIMEOUT, stream.read_exact(&mut payload)).await??;
    buf.extend_from_slice(&payload);
    let (f, _) = Frame::decode(&buf).map_err(std::io::Error::other)?;
    Ok(f)
}

/// Connect to a managed node and authenticate with the server's own
/// DOCSQL_TOKEN (never a browser-supplied value). A missing token on an
/// unauthenticated node is fine; a mismatch surfaces as an error.
async fn node_connect(addr: &str, token: Option<&str>) -> Result<TcpStream, String> {
    let mut stream = tokio::time::timeout(NODE_CONNECT_TIMEOUT, TcpStream::connect(addr))
        .await
        .map_err(|e| format!("节点 {addr} 不可达: {e}"))?
        .map_err(|e| format!("节点 {addr} 不可达: {e}"))?;
    if let Some(t) = token {
        node_write_frame(
            &mut stream,
            &Frame::new(proto::REQ_AUTH, t.as_bytes().to_vec()),
        )
        .await
        .map_err(|e| format!("节点 {addr} 认证请求失败: {e}"))?;
        let auth = node_read_frame(&mut stream)
            .await
            .map_err(|e| format!("节点 {addr} 认证无响应: {e}"))?;
        if auth.frame_type == proto::RESP_ERROR {
            return Err(format!(
                "节点 {addr} 拒绝认证(节点与控制台的 DOCSQL_TOKEN 不一致): {}",
                String::from_utf8_lossy(&auth.payload)
            ));
        }
    }
    Ok(stream)
}

/// Execute a batch on a managed node over the wire protocol and shape the
/// response: single statements keep the legacy shape, batches report
/// per-statement results plus the first-error index. All statements run in
/// order on one connection, so BEGIN/COMMIT spans the batch; separate calls
/// get separate connections (the server rolls an abandoned transaction back
/// when its connection closes).
/// One remote multi-statement batch: per-statement outcomes plus the first
/// statement error, if any — transport failures abort the whole call as a
/// single error object instead.
type BatchRun = Result<(Vec<serde_json::Value>, Option<(usize, String)>), String>;

pub async fn remote_sql(addr: &str, token: Option<&str>, sql: &str) -> serde_json::Value {
    let stmts = match docsql_core::stmt::split_statements(sql) {
        Ok(s) => s,
        // Same parser + dialect as the engine, so the message matches what
        // execution would have produced — no need to hit the wire.
        Err(m) => return serde_json::json!({"kind": "error", "message": m}),
    };
    // (per-statement outcomes, first statement error) — transport failures
    // abort the whole call as a single error object.
    let run: BatchRun = async {
        let mut stream = node_connect(addr, token).await?;
        let mut outcomes = Vec::new();
        for (i, stmt) in stmts.iter().enumerate() {
            let payload = proto::encode_sql(stmt).map_err(|e| e.to_string())?;
            node_write_frame(&mut stream, &Frame::new(proto::REQ_SQL, payload))
                .await
                .map_err(|e| format!("节点 {addr} 发送失败: {e}"))?;
            let f = node_read_frame(&mut stream)
                .await
                .map_err(|e| format!("节点 {addr} 无响应: {e}"))?;
            match f.frame_type {
                proto::RESP_ROWS => {
                    let v: serde_json::Value = serde_json::from_slice(&f.payload)
                        .map_err(|e| format!("节点 {addr} 返回了无法解析的结果: {e}"))?;
                    outcomes.push(serde_json::json!({
                        "kind": "rows",
                        "columns": v.get("columns").cloned().unwrap_or_else(|| serde_json::json!([])),
                        "rows": v.get("rows").cloned().unwrap_or_else(|| serde_json::json!([])),
                    }));
                }
                proto::RESP_AFFECTED => {
                    let n = f
                        .payload
                        .get(..8)
                        .and_then(|s| s.try_into().ok())
                        .map_or(0, u64::from_le_bytes);
                    outcomes.push(serde_json::json!({"kind": "affected", "count": n}));
                }
                proto::RESP_ERROR => {
                    return Ok((
                        outcomes,
                        Some((i, String::from_utf8_lossy(&f.payload).into_owned())),
                    ))
                }
                other => return Err(format!("节点 {addr} 返回了意外帧: {other:#06x}",)),
            }
        }
        Ok((outcomes, None))
    }
    .await;
    match run {
        Err(transport) => serde_json::json!({"kind": "error", "message": transport}),
        Ok((outcomes, None)) if stmts.len() <= 1 => outcomes
            .into_iter()
            .next()
            .unwrap_or_else(|| serde_json::json!({"kind": "error", "message": "empty statement"})),
        Ok((outcomes, None)) => serde_json::json!({
            "kind": "batch",
            "results": outcomes,
            "error": serde_json::Value::Null,
        }),
        Ok((_, Some((_, msg)))) if stmts.len() == 1 => {
            serde_json::json!({"kind": "error", "message": msg})
        }
        Ok((outcomes, Some((i, msg)))) => serde_json::json!({
            "kind": "batch",
            "results": outcomes,
            "error": {"statement": i, "message": msg},
        }),
    }
}

/// Fetch a managed node's object-explorer metadata (REQ_META). The server
/// assembles it with the same `core::meta::build_meta` walk the console
/// UI renders, so the payload is shape-stable for the frontend surface.
pub async fn remote_meta(addr: &str, token: Option<&str>) -> serde_json::Value {
    let run: Result<serde_json::Value, String> = async {
        let mut stream = node_connect(addr, token).await?;
        node_write_frame(&mut stream, &Frame::new(proto::REQ_META, vec![]))
            .await
            .map_err(|e| format!("节点 {addr} 请求失败: {e}"))?;
        let f = node_read_frame(&mut stream)
            .await
            .map_err(|e| format!("节点 {addr} 无响应: {e}"))?;
        if f.frame_type != proto::RESP_META {
            return Err(format!("节点 {addr} 返回了意外帧: {:#06x}", f.frame_type));
        }
        serde_json::from_slice(&f.payload)
            .map_err(|e| format!("节点 {addr} 的 meta 载荷无法解析: {e}"))
    }
    .await;
    match run {
        Ok(v) => v,
        Err(m) => serde_json::json!({"error": m}),
    }
}

/// Fetch a managed node's counters (REQ_STATUS) and map them onto the
/// `/api/stats` shape, so the dashboard card builder works unchanged.
pub async fn remote_stats(addr: &str, token: Option<&str>) -> serde_json::Value {
    let run: Result<serde_json::Value, String> = async {
        let mut stream = node_connect(addr, token).await?;
        node_write_frame(&mut stream, &Frame::new(proto::REQ_STATUS, vec![]))
            .await
            .map_err(|e| format!("节点 {addr} 请求失败: {e}"))?;
        let f = node_read_frame(&mut stream)
            .await
            .map_err(|e| format!("节点 {addr} 无响应: {e}"))?;
        if f.frame_type != proto::RESP_STATUS {
            return Err(format!("节点 {addr} 返回了意外帧: {:#06x}", f.frame_type));
        }
        let s: serde_json::Value = serde_json::from_slice(&f.payload)
            .map_err(|e| format!("节点 {addr} 的 status 载荷无法解析: {e}"))?;
        Ok(serde_json::json!({
            "tables": s.get("totals").and_then(|t| t.get("tables")).cloned().unwrap_or_else(|| serde_json::json!(0)),
            "pages": s.pointer("/storage/num_pages").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "page_size": s.pointer("/storage/page_size").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "db_bytes": s.pointer("/storage/db_bytes").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "wal_bytes": s.pointer("/storage/wal_bytes").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "uptime_ms": s.get("uptime_ms").cloned().unwrap_or_else(|| serde_json::json!(0)),
        }))
    }
    .await;
    match run {
        Ok(v) => v,
        Err(m) => serde_json::json!({"error": m}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::ServiceExt;

    #[tokio::test]
    async fn console_page_served() {
        let html = include_str!("console.html");
        assert!(html.contains("DocSQL")); // console markup present
        assert!(html.contains("DocSQL console")); // deploy-test marker
        assert!(html.contains("对象资源管理器")); // SSMS-style explorer present
                                                  // Write surface (mongo-express style): new-table & insert-document.
        assert!(html.contains("newTableDialog"));
        assert!(html.contains("insertDocDialog"));
        assert!(html.contains("插入文档"));
        // Index management surface: create/edit/drop from the explorer.
        assert!(html.contains("newIndexDialog"));
        assert!(html.contains("editIndexDialog"));
        assert!(html.contains("dropIndex"));
        assert!(html.contains("新建索引"));
        assert!(html.contains("修改索引"));
        // Logs surface: data/sync log viewer over /api/logs.
        assert!(html.contains("openLogsTab"));
        assert!(html.contains("数据日志"));
        assert!(html.contains("同步日志"));
        // No embedded engine: the selector's fallback is the default managed
        // node, never a local database.
        assert!(!html.contains("内嵌引擎"));
    }

    #[test]
    fn resolve_node_only_allows_configured_peers() {
        let state = WebState {
            upstream: Some("node-a:7600".into()),
            token: None,
            peers: vec!["node-a:7600".into()],
            query_log: querylog::QueryLog::new(),
            sync_log: querylog::SyncLog::new(1),
            auth: None,
        };
        assert_eq!(resolve_node(&state, &None).unwrap(), None);
        assert_eq!(resolve_node(&state, &Some(String::new())).unwrap(), None);
        assert_eq!(
            resolve_node(&state, &Some("node-a:7600".into())).unwrap(),
            Some("node-a:7600".into())
        );
        // Trimmed input still matches.
        assert_eq!(
            resolve_node(&state, &Some(" node-a:7600 ".into())).unwrap(),
            Some("node-a:7600".into())
        );
        // Anything outside DOCSQL_PEERS is refused (SSRF guard).
        assert!(resolve_node(&state, &Some("127.0.0.1:1".into())).is_err());
        assert!(resolve_node(&state, &Some("evil.example:7600".into())).is_err());
    }

    /// Data calls land on the explicit node when given, else on the default
    /// managed node; with neither configured there is nothing to manage.
    #[test]
    fn target_for_prefers_explicit_node_then_upstream() {
        let state = WebState {
            upstream: Some("node-a:7600".into()),
            token: None,
            peers: vec!["node-a:7600".into(), "node-b:7600".into()],
            query_log: querylog::QueryLog::new(),
            sync_log: querylog::SyncLog::new(1),
            auth: None,
        };
        assert_eq!(target_for(&state, &None).unwrap(), "node-a:7600");
        assert_eq!(
            target_for(&state, &Some("node-b:7600".into())).unwrap(),
            "node-b:7600"
        );
        // Explicit targets stay validated even when an upstream exists.
        assert!(target_for(&state, &Some("evil.example:7600".into())).is_err());

        let unconfigured = WebState {
            upstream: None,
            peers: Vec::new(),
            ..state
        };
        let e = target_for(&unconfigured, &None).unwrap_err();
        assert!(e["error"].as_str().unwrap().contains("未配置管理目标节点"));
    }

    #[tokio::test]
    async fn cluster_endpoint_reports_default_node() {
        let state = Arc::new(WebState {
            upstream: Some("node-a:7600".into()),
            token: None,
            peers: Vec::new(),
            query_log: querylog::QueryLog::new(),
            sync_log: querylog::SyncLog::new(1),
            auth: None,
        });
        let res = build_router(state)
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/cluster")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        let body = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["nodes"], serde_json::json!([]));
        assert_eq!(v["default"], "node-a:7600");
    }

    #[tokio::test]
    async fn probe_unreachable_node_reports_offline() {
        // Loopback port 1 refuses immediately.
        let v = probe_node("127.0.0.1:1", None).await;
        assert_eq!(v["reachable"], false);
        assert!(v["status"].is_null());
        assert!(!v["error"].as_str().unwrap().is_empty());
    }

    #[tokio::test]
    async fn probe_live_server_fetches_status_report() {
        let dir = tempfile::tempdir().unwrap();
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        drop(l);
        let addr = format!("127.0.0.1:{port}");
        tokio::spawn(docsql_server::run(docsql_server::ServerConfig {
            db_path: dir.path().join("probe.db"),
            listen: addr.clone(),
            auth_token: None,
            read_token: None,
            max_conn: 0,
            idle_timeout_secs: 0,
            auth_lock_threshold: 10,
            cluster_token: None,
            replicate_to: None,
            peers: Vec::new(),
            advertise: None,
            read_only: false,
            transport_key: None,
            async_commit: false,
            catchup_window: 0,
        }));
        for _ in 0..100 {
            if tokio::net::TcpStream::connect(&addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let v = probe_node(&addr, None).await;
        assert_eq!(v["reachable"], true, "{v}");
        assert!(v["error"].is_null());
        assert_eq!(v["status"]["name"], "docsql");
        assert_eq!(v["status"]["totals"]["tables"], 0);
        assert!(v["status"]["durable_lsn"].as_u64().is_some());
        assert!(v["latency_ms"].as_f64().is_some());
    }

    /// Start one real server node on a free port; returns its address.
    async fn spawn_node(dir: &tempfile::TempDir, token: Option<&str>) -> String {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        drop(l);
        let addr = format!("127.0.0.1:{port}");
        let cfg = docsql_server::ServerConfig {
            db_path: dir.path().join("node.db"),
            listen: addr.clone(),
            auth_token: token.map(String::from),
            read_token: None,
            max_conn: 0,
            idle_timeout_secs: 0,
            auth_lock_threshold: 10,
            cluster_token: None,
            replicate_to: None,
            peers: Vec::new(),
            advertise: None,
            read_only: false,
            transport_key: None,
            async_commit: false,
            catchup_window: 0,
        };
        tokio::spawn(docsql_server::run(cfg));
        for _ in 0..100 {
            if tokio::net::TcpStream::connect(&addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        addr
    }

    /// The remote proxy reproduces one response shape per statement count,
    /// including >512-char SQL (the REQ_SQL text cap regression).
    #[tokio::test]
    async fn remote_sql_proxies_batch_shapes() {
        let dir = tempfile::tempdir().unwrap();
        let addr = spawn_node(&dir, None).await;

        // Single statement keeps the legacy shape.
        let r = remote_sql(&addr, None, "CREATE TABLE rt (id INT PRIMARY KEY, v TEXT)").await;
        assert_eq!(r["kind"], "affected", "{r}");
        let r = remote_sql(&addr, None, "INSERT INTO rt VALUES (1, 'a'), (2, 'b')").await;
        assert_eq!(r["kind"], "affected");
        assert_eq!(r["count"], 2);

        // Multi-statement batch: per-statement results.
        let r = remote_sql(
            &addr,
            None,
            "INSERT INTO rt VALUES (3, 'c'); INSERT INTO rt VALUES (4, 'd'); SELECT id FROM rt ORDER BY id",
        )
        .await;
        assert_eq!(r["kind"], "batch", "{r}");
        let results = r["results"].as_array().unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(r["error"], serde_json::Value::Null);
        let rows = results[2]["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0][0], 1);

        // Error mid-batch: partial results + 0-based statement index.
        let r = remote_sql(
            &addr,
            None,
            "INSERT INTO rt VALUES (5, 'e'); SELECT * FROM nope",
        )
        .await;
        assert_eq!(r["kind"], "batch");
        assert_eq!(r["error"]["statement"], 1);
        assert_eq!(r["results"].as_array().unwrap().len(), 1);

        // Single-statement error keeps the legacy error shape.
        let r = remote_sql(&addr, None, "SELECT * FROM nope").await;
        assert_eq!(r["kind"], "error");

        // A parse error is caught before the wire (same parser).
        let r = remote_sql(&addr, None, "SELECT FROM").await;
        assert_eq!(r["kind"], "error");

        // Long statements must arrive untruncated (old REQ_SQL cap: 512).
        let long = "x".repeat(600);
        let sql = format!("INSERT INTO rt VALUES (9, '{long}')");
        assert!(sql.chars().count() > 512);
        let r = remote_sql(&addr, None, &sql).await;
        assert_eq!(r["kind"], "affected", "{r}");
        let r = remote_sql(&addr, None, "SELECT v FROM rt WHERE id = 9").await;
        assert_eq!(r["rows"][0][0].as_str().unwrap().len(), 600, "{r}");
    }

    /// Auth against token-protected nodes plus the offline-node error path.
    #[tokio::test]
    async fn remote_sql_auth_and_offline_errors() {
        let dir = tempfile::tempdir().unwrap();
        let addr = spawn_node(&dir, Some("sec")).await;
        let r = remote_sql(&addr, Some("wrong"), "SELECT 1").await;
        assert_eq!(r["kind"], "error");
        assert!(r["message"].as_str().unwrap().contains("拒绝认证"), "{r}");
        let r = remote_sql(&addr, Some("sec"), "SELECT 1").await;
        assert_eq!(r["kind"], "rows", "{r}");

        let r = remote_sql("127.0.0.1:1", None, "SELECT 1").await;
        assert_eq!(r["kind"], "error");
        assert!(r["message"].as_str().unwrap().contains("不可达"), "{r}");
    }

    /// Remote meta/stats mirror the /api/meta + /api/stats shapes
    /// (dashboard and object explorer render managed nodes unchanged).
    #[tokio::test]
    async fn remote_meta_and_stats_mirror_local_shapes() {
        let dir = tempfile::tempdir().unwrap();
        let addr = spawn_node(&dir, None).await;
        remote_sql(&addr, None, "CREATE TABLE rm (id INT PRIMARY KEY)").await;
        remote_sql(&addr, None, "INSERT INTO rm VALUES (1)").await;

        let m = remote_meta(&addr, None).await;
        assert!(m.get("error").is_none(), "{m}");
        assert_eq!(m["totals"]["tables"], 1);
        assert_eq!(m["tables"][0]["name"], "rm");
        assert_eq!(m["tables"][0]["row_count"], 1);
        assert!(!m["tables"][0]["columns"].as_array().unwrap().is_empty());
        assert!(!m["tables"][0]["index_defs"].as_array().unwrap().is_empty());
        assert!(m["storage"]["page_size"].as_u64().is_some());

        let s = remote_stats(&addr, None).await;
        assert!(s.get("error").is_none(), "{s}");
        assert_eq!(s["tables"], 1);
        assert!(s["page_size"].as_u64().is_some());
        assert!(s["uptime_ms"].as_u64().is_some());
    }
}
