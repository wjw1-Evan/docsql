//! End-to-end tests: real web console on a random port managing a real
//! wire-protocol node, plain HTTP/1.1 client over TcpStream (no external
//! HTTP dependency, mirroring the wire-protocol e2e suite in docsql-server).
//! The console stores nothing — every data call must land on the managed
//! node, which these tests prove by asserting against the node's own state.

use serde_json::json;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Start the console alone (no managed node): pure-UI surface tests.
async fn start_web(token: Option<&str>, peers: Vec<String>, upstream: Option<String>) -> String {
    start_web_auth(token, peers, upstream, None).await
}

/// Same with the console account gate pointed at `auth_file` (None = off).
async fn start_web_auth(
    token: Option<&str>,
    peers: Vec<String>,
    upstream: Option<String>,
    auth_file: Option<String>,
) -> String {
    // Pick a free port by binding a listener first.
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    let addr = format!("127.0.0.1:{port}");
    let cfg = docsql_web::WebConfig {
        upstream,
        token: token.map(String::from),
        peers,
        auth_file,
    };
    let listen = addr.clone();
    tokio::spawn(async move { docsql_web::run(cfg, &listen).await });
    for _ in 0..100 {
        if TcpStream::connect(&addr).await.is_ok() {
            return addr;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("web console did not come up");
}

/// One real node behind `token` + the console managing it as its default
/// target. Returns (node data dir, web addr, node addr).
async fn start_stack(
    token: Option<&str>,
    peers: Vec<String>,
) -> (tempfile::TempDir, String, String) {
    let dir = tempfile::tempdir().unwrap();
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    let node_addr = format!("127.0.0.1:{port}");
    tokio::spawn(docsql_server::run(docsql_server::ServerConfig {
        db_path: dir.path().join("node.db"),
        listen: node_addr.clone(),
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
        backup_interval_secs: 0,
        backup_keep: 7,
        backup_dir: None,
    }));
    for _ in 0..100 {
        if TcpStream::connect(&node_addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let web = start_web(token, peers, Some(node_addr.clone())).await;
    (dir, web, node_addr)
}

struct HttpResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl HttpResponse {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    fn json(self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap()
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

/// One-shot HTTP/1.1 request on a fresh connection (`Connection: close`), so
/// the response ends at EOF and no keep-alive bookkeeping is needed.
async fn http(
    addr: &str,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: Option<&str>,
) -> HttpResponse {
    http_full(addr, method, path, token, None, body).await
}

/// Same with a session cookie (the console account gate).
async fn http_cookie(
    addr: &str,
    method: &str,
    path: &str,
    cookie: &str,
    body: Option<&str>,
) -> HttpResponse {
    http_full(addr, method, path, None, Some(cookie), body).await
}

async fn http_full(
    addr: &str,
    method: &str,
    path: &str,
    token: Option<&str>,
    cookie: Option<&str>,
    body: Option<&str>,
) -> HttpResponse {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n");
    if let Some(t) = token {
        req.push_str(&format!("X-Docsql-Token: {t}\r\n"));
    }
    if let Some(c) = cookie {
        req.push_str(&format!("Cookie: {c}\r\n"));
    }
    if let Some(b) = body {
        req.push_str("Content-Type: application/json\r\n");
        req.push_str(&format!("Content-Length: {}\r\n", b.len()));
    }
    req.push_str("\r\n");
    stream.write_all(req.as_bytes()).await.unwrap();
    if let Some(b) = body {
        stream.write_all(b.as_bytes()).await.unwrap();
    }
    stream.flush().await.unwrap();

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();

    let sep = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("no header terminator");
    let head = String::from_utf8_lossy(&raw[..sep]).into_owned();
    let mut lines = head.split("\r\n");
    let status: u16 = lines
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect();
    let chunked = headers
        .iter()
        .any(|(k, v)| k.eq_ignore_ascii_case("transfer-encoding") && v.contains("chunked"));
    let body = raw[sep + 4..].to_vec();
    HttpResponse {
        status,
        headers,
        body: if chunked { dechunk(body) } else { body },
    }
}

/// Decode chunked framing (hex size lines + CRLF around each chunk, 0 ends).
fn dechunk(mut body: Vec<u8>) -> Vec<u8> {
    let mut out = Vec::new();
    while let Some(line_end) = body.windows(2).position(|w| w == b"\r\n") {
        let size_str = String::from_utf8_lossy(&body[..line_end]).into_owned();
        let size = usize::from_str_radix(size_str.trim().split(';').next().unwrap_or("0"), 16)
            .unwrap_or(0);
        body.drain(..line_end + 2);
        if size == 0 || body.len() < size + 2 {
            break;
        }
        out.extend_from_slice(&body[..size]);
        body.drain(..size + 2);
    }
    out
}

async fn sql(addr: &str, token: Option<&str>, sql: &str) -> serde_json::Value {
    let body = serde_json::to_string(&json!({ "sql": sql })).unwrap();
    http(addr, "POST", "/api/sql", token, Some(&body))
        .await
        .json()
}

#[tokio::test]
async fn console_page_served_over_http() {
    let addr = start_web(None, Vec::new(), None).await;
    let res = http(&addr, "GET", "/", None, None).await;
    assert_eq!(res.status, 200);
    assert!(res.header("content-type").unwrap().starts_with("text/html"));
    let html = res.text();
    assert!(html.contains("DocSQL console")); // deploy-test marker
    assert!(html.contains("对象资源管理器")); // SSMS-style explorer
    assert!(html.contains("insertDocDialog")); // write surface present
    assert!(html.contains("newIndexDialog")); // index management present
    assert!(html.contains("editIndexDialog"));
    assert!(html.contains("autoIndexScript")); // read-only constraint autoindexes
                                               // The API surface is JSON-only: a bare GET on it is rejected.
    let res = http(&addr, "GET", "/api/sql", None, None).await;
    assert_eq!(res.status, 405);
}

#[tokio::test]
async fn sql_roundtrip_over_http() {
    let (_dir, addr, _node) = start_stack(None, Vec::new()).await;
    // Single statements keep the legacy one-result shape.
    assert_eq!(
        sql(
            &addr,
            None,
            "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)"
        )
        .await,
        json!({"kind": "affected", "count": 0})
    );
    assert_eq!(
        sql(&addr, None, "INSERT INTO t VALUES (1, 'a'), (2, 'b')").await,
        json!({"kind": "affected", "count": 2})
    );
    let r = sql(&addr, None, "SELECT id, name FROM t ORDER BY id").await;
    assert_eq!(r["kind"], "rows");
    assert_eq!(r["columns"], json!(["id", "name"]));
    assert_eq!(r["rows"], json!([[1, "a"], [2, "b"]]));

    // Multi-statement batches return per-statement results + error index.
    let r = sql(
        &addr,
        None,
        "INSERT INTO t VALUES (3, 'c'); SELECT COUNT(*) AS n FROM t; DROP TABLE nope",
    )
    .await;
    assert_eq!(r["kind"], "batch");
    assert_eq!(r["results"].as_array().unwrap().len(), 2);
    assert_eq!(r["error"]["statement"], 2);

    // SQL failures surface as an error JSON body, not a bare status code.
    let r = sql(&addr, None, "SELECT * FROM missing").await;
    assert_eq!(r["kind"], "error");
    assert!(r["message"].as_str().unwrap().contains("missing"));

    // Malformed JSON body is a client error.
    let res = http(&addr, "POST", "/api/sql", None, Some("{not json")).await;
    assert_eq!(res.status, 400);
}

#[tokio::test]
async fn parse_endpoint_validates_without_executing() {
    let (_dir, addr, _node) = start_stack(None, Vec::new()).await;
    let ok = serde_json::to_string(&json!({"sql": "CREATE TABLE p (id INT)"})).unwrap();
    let res = http(&addr, "POST", "/api/parse", None, Some(&ok)).await;
    assert_eq!(res.status, 200);
    assert_eq!(res.json(), json!({"ok": true}));

    let bad = serde_json::to_string(&json!({"sql": "SELEC nope"})).unwrap();
    let res = http(&addr, "POST", "/api/parse", None, Some(&bad)).await;
    assert_eq!(res.status, 200);
    let v = res.json();
    assert_eq!(v["ok"], false);
    assert!(!v["message"].as_str().unwrap().is_empty());

    // Parse-check must not execute: the table never comes into existence
    // on the managed node.
    let meta = http(&addr, "GET", "/api/meta", None, None).await.json();
    assert_eq!(meta["totals"]["tables"], 0);
}

#[tokio::test]
async fn token_gates_api_surface_but_not_console_page() {
    let (_dir, addr, _node) = start_stack(Some("sekrit"), Vec::new()).await;
    // The console page must load without a token (the UI collects it).
    assert_eq!(http(&addr, "GET", "/", None, None).await.status, 200);

    for (method, path, body) in [
        ("POST", "/api/sql", Some(r#"{"sql":"SELECT 1"}"#)),
        ("POST", "/api/parse", Some(r#"{"sql":"SELECT 1"}"#)),
        ("GET", "/api/meta", None),
        ("GET", "/api/stats", None),
        ("GET", "/api/cluster", None),
    ] {
        let res = http(&addr, method, path, None, body).await;
        assert_eq!(res.status, 401, "{path} without token");
        let res = http(&addr, method, path, Some("wrong"), body).await;
        assert_eq!(res.status, 401, "{path} wrong token");
        let res = http(&addr, method, path, Some("sekrit"), body).await;
        assert_eq!(res.status, 200, "{path} correct token");
    }
}

#[tokio::test]
async fn meta_and_stats_report_live_catalog() {
    let (_dir, addr, _node) = start_stack(None, Vec::new()).await;
    sql(
        &addr,
        None,
        "CREATE TABLE m (id INT PRIMARY KEY, name TEXT DEFAULT 'anon'); INSERT INTO m VALUES (1, 'a'), (2, 'b')",
    )
    .await;

    let meta = http(&addr, "GET", "/api/meta", None, None).await.json();
    assert_eq!(meta["totals"], json!({"tables": 1, "rows": 2}));
    let t = &meta["tables"][0];
    assert_eq!(t["name"], "m");
    assert_eq!(t["row_count"], 2);
    assert_eq!(t["keys"], json!(["id"]));
    let cols = t["columns"].as_array().unwrap();
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0]["primary_key"], true);
    // Declared DEFAULT round-trips as SQL text (edit-table grid reads it);
    // columns without one report null.
    assert_eq!(cols[0]["default"], json!(null));
    assert_eq!(cols[1]["default"], "'anon'");
    assert_eq!(meta["storage"]["page_size"], 4096);
    // The engine's system tables surface under their own key (the console's
    // read-only 系统表 branch), never inside the user tables array.
    let sys: Vec<&str> = meta["system_tables"]
        .as_array()
        .expect("system_tables array")
        .iter()
        .map(|t| t["name"].as_str().expect("name"))
        .collect();
    assert!(sys.contains(&"_pubsub_messages"), "{sys:?}");
    assert!(!meta["tables"]
        .as_array()
        .unwrap()
        .iter()
        .any(|t| t["name"] == "_pubsub_messages"));
    // Read-only queries on a system table pass through the proxy (what the
    // 系统表 branch double-click issues); writes stay rejected.
    let r = sql(&addr, None, "SELECT COUNT(*) FROM _pubsub_messages").await;
    assert_eq!(r["kind"], "rows");
    let r = sql(
        &addr,
        None,
        "INSERT INTO _pubsub_messages (channel, payload) VALUES ('x', 'y')",
    )
    .await;
    assert_eq!(r["kind"], "error");
    assert!(r["message"].as_str().unwrap().contains("internal"));

    // Schemaless write: a top-level field no column declares. Meta must
    // surface it as observed without touching the declared column list.
    sql(
        &addr,
        None,
        "INSERT INTO m (id, extra) VALUES (3, 'undeclared')",
    )
    .await;
    let meta = http(&addr, "GET", "/api/meta", None, None).await.json();
    let t = &meta["tables"][0];
    assert_eq!(t["observed"], json!(["extra", "id", "name"]));
    assert_eq!(t["columns"].as_array().unwrap().len(), 2);

    let stats = http(&addr, "GET", "/api/stats", None, None).await.json();
    assert_eq!(stats["tables"], 1);
    assert_eq!(stats["page_size"], 4096);
    assert!(stats["db_bytes"].as_u64().unwrap() > 0);
    assert!(stats["uptime_ms"].as_u64().is_some());
}

/// GUID auto-generated primary keys through the console API: create, insert
/// without the id, and /api/meta surfacing data_type "GUID" (the console's
/// script generation round-trips the DDL from it).
#[tokio::test]
async fn guid_autogen_pk_over_http() {
    let (_dir, addr, _node) = start_stack(None, Vec::new()).await;
    assert_eq!(
        sql(
            &addr,
            None,
            "CREATE TABLE g (id GUID PRIMARY KEY AUTOINCREMENT, v TEXT)"
        )
        .await,
        json!({"kind": "affected", "count": 0})
    );
    assert_eq!(
        sql(&addr, None, "INSERT INTO g (v) VALUES ('a'), ('b')").await,
        json!({"kind": "affected", "count": 2})
    );
    let r = sql(&addr, None, "SELECT id, v FROM g ORDER BY id").await;
    assert_eq!(r["kind"], "rows");
    let rows = r["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    for row in rows {
        let id = row[0].as_str().unwrap();
        assert_eq!(id.len(), 36);
        assert_eq!(&id[14..15], "7", "uuidv7 version nibble in {id}");
    }
    // String order == generation order (time-ordered ids).
    assert!(rows[0][0].as_str().unwrap() < rows[1][0].as_str().unwrap());

    let meta = http(&addr, "GET", "/api/meta", None, None).await.json();
    let t = &meta["tables"][0];
    assert_eq!(t["name"], "g");
    let cols = t["columns"].as_array().unwrap();
    assert_eq!(cols[0]["name"], "id");
    assert_eq!(cols[0]["data_type"], "GUID");
    assert_eq!(cols[0]["autoinc"], true);
    assert_eq!(cols[1]["data_type"], "ANY");
}

/// The edit-table dialog's server contract: a rename → drop → add batch of
/// ALTER statements through /api/sql, and the engine's explicit rejections
/// (no PK drop, no constraint options on ADD COLUMN) surfacing to the UI.
#[tokio::test]
async fn edit_table_alter_batch_over_http() {
    let (_dir, addr, _node) = start_stack(None, Vec::new()).await;
    sql(
        &addr,
        None,
        "CREATE TABLE ed (id GUID PRIMARY KEY AUTOINCREMENT, a TEXT, b TEXT); \
         INSERT INTO ed (a, b) VALUES ('x', 'y')",
    )
    .await;

    // Rename a, drop b, add c (NOT NULL + DEFAULT, as the dialog emits for
    // a defaulted new column) — in the dialog's order.
    let r = sql(
        &addr,
        None,
        "ALTER TABLE ed RENAME COLUMN a TO a2;\n\
         ALTER TABLE ed DROP COLUMN b;\n\
         ALTER TABLE ed ADD COLUMN c INT NOT NULL DEFAULT 9;",
    )
    .await;
    assert_eq!(r["kind"], "batch");
    assert!(r["error"].is_null(), "{}", r);

    let meta = http(&addr, "GET", "/api/meta", None, None).await.json();
    let t = &meta["tables"][0];
    let names: Vec<&str> = t["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["id", "a2", "c"]);
    // The GUID auto PK survives structural edits untouched.
    assert_eq!(t["columns"][0]["data_type"], "GUID");
    assert_eq!(t["columns"][0]["autoinc"], true);
    // The defaulted new column reports its DEFAULT and NOT NULL, and the
    // pre-existing row was backfilled with the default.
    assert_eq!(t["columns"][2]["default"], "9");
    assert_eq!(t["columns"][2]["nullable"], false);
    sql(&addr, None, "INSERT INTO ed (a2) VALUES ('still works')").await;
    let r = sql(&addr, None, "SELECT c FROM ed ORDER BY id").await;
    assert_eq!(r["kind"], "rows");
    for row in r["rows"].as_array().unwrap() {
        assert_eq!(row[0].as_i64(), Some(9), "DEFAULT backfill + insert fill");
    }
    let r = sql(&addr, None, "SELECT id FROM ed ORDER BY id DESC LIMIT 1").await;
    assert_eq!(r["kind"], "rows");
    assert_eq!(&r["rows"][0][0].as_str().unwrap()[14..15], "7");

    // Engine-side guards the UI relies on (it disables the controls, the
    // server still enforces): PK columns cannot be dropped, ADD COLUMN
    // rejects constraint options.
    let r = sql(&addr, None, "ALTER TABLE ed DROP COLUMN id").await;
    assert_eq!(r["kind"], "error");
    assert!(r["message"].as_str().unwrap().contains("PRIMARY KEY"));
    let r = sql(&addr, None, "ALTER TABLE ed ADD COLUMN bad INT PRIMARY KEY").await;
    assert_eq!(r["kind"], "error");
    assert!(r["message"].as_str().unwrap().contains("ADD COLUMN"));
}

/// Index management over the console API: create (plain + UNIQUE), the
/// duplicate-data guard on UNIQUE (NULLs never block, matching the engine),
/// and the console's two modify paths — rename = create-new + drop-old,
/// same-name rebuild = drop + create in one batch. /api/meta must surface
/// the full definitions (name/column/unique) the edit dialog works against.
#[tokio::test]
async fn index_management_over_http() {
    let (_dir, addr, _node) = start_stack(None, Vec::new()).await;
    sql(
        &addr,
        None,
        "CREATE TABLE ix (id INT PRIMARY KEY, tag TEXT, email TEXT); \
         INSERT INTO ix VALUES (1, 'a', 'x@y'), (2, 'a', 'y@y'), (3, 'b', NULL)",
    )
    .await;

    assert_eq!(
        sql(&addr, None, "CREATE INDEX ix_tag ON ix (tag)").await,
        json!({"kind": "affected", "count": 0})
    );
    // UNIQUE on a column with duplicates is rejected (tag has 'a' twice)…
    let r = sql(&addr, None, "CREATE UNIQUE INDEX bad ON ix (tag)").await;
    assert_eq!(r["kind"], "error");
    assert!(r["message"].as_str().unwrap().contains("UNIQUE"));
    // …while NULLs never block a UNIQUE index (engine skips them).
    assert_eq!(
        sql(&addr, None, "CREATE UNIQUE INDEX ux_email ON ix (email)").await,
        json!({"kind": "affected", "count": 0})
    );

    let meta = http(&addr, "GET", "/api/meta", None, None).await.json();
    let t = &meta["tables"][0];
    // The PK constraint index leads the list; UNIQUE via CREATE INDEX is a
    // named definition and gets no second autoindex entry.
    assert_eq!(
        t["indexes"],
        json!(["sqlite_autoindex_ix_1", "ix_tag", "ux_email"])
    );
    assert_eq!(
        t["index_defs"],
        json!([
            {"name": "sqlite_autoindex_ix_1", "column": "id", "unique": true, "auto": true},
            {"name": "ix_tag", "column": "tag", "unique": false, "auto": false},
            {"name": "ux_email", "column": "email", "unique": true, "auto": false},
        ])
    );
    // Constraint indexes reject DROP/CREATE like the engine does.
    let r = sql(&addr, None, "DROP INDEX sqlite_autoindex_ix_1").await;
    assert_eq!(r["kind"], "error");
    assert!(r["message"].as_str().unwrap().contains("cannot be dropped"));

    // Rename path: create the new definition first, then drop the old name.
    let r = sql(
        &addr,
        None,
        "CREATE UNIQUE INDEX ux_mail ON ix (email); DROP INDEX ux_email;",
    )
    .await;
    assert_eq!(r["kind"], "batch");
    assert!(r["error"].is_null());
    // Same-name rebuild with a new definition (drop + create in one batch).
    let r = sql(
        &addr,
        None,
        "DROP INDEX ix_tag; CREATE UNIQUE INDEX ix_tag ON ix (id);",
    )
    .await;
    assert_eq!(r["kind"], "batch");
    assert!(r["error"].is_null());

    let meta = http(&addr, "GET", "/api/meta", None, None).await.json();
    let t = &meta["tables"][0];
    let defs = t["index_defs"].as_array().unwrap();
    // One derived PK autoindex + the two user indexes.
    assert_eq!(defs.len(), 3);
    assert_eq!(defs[0]["name"], "sqlite_autoindex_ix_1");
    assert_eq!(defs[0]["auto"], true);
    assert!(defs
        .iter()
        .any(|d| d["name"] == "ux_mail" && d["column"] == "email" && d["unique"] == true));
    assert!(defs
        .iter()
        .any(|d| d["name"] == "ix_tag" && d["column"] == "id" && d["unique"] == true));
}

#[tokio::test]
async fn cluster_page_probes_live_and_dead_nodes() {
    // A real wire-protocol node behind the same token + a dead address.
    let node_dir = tempfile::tempdir().unwrap();
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    let node_addr = format!("127.0.0.1:{port}");
    tokio::spawn(docsql_server::run(docsql_server::ServerConfig {
        db_path: node_dir.path().join("node.db"),
        listen: node_addr.clone(),
        auth_token: Some("sekrit".into()),
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
        backup_interval_secs: 0,
        backup_keep: 7,
        backup_dir: None,
    }));
    for _ in 0..100 {
        if TcpStream::connect(&node_addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let addr = start_web(
        Some("sekrit"),
        vec![node_addr.clone(), "127.0.0.1:1".into()],
        Some(node_addr.clone()),
    )
    .await;
    let res = http(&addr, "GET", "/api/cluster", Some("sekrit"), None).await;
    assert_eq!(res.status, 200);
    let v = res.json();
    // The default managed node is surfaced so the UI can name the fallback.
    assert_eq!(v["default"], node_addr);
    let nodes = v["nodes"].as_array().unwrap().clone();

    // Live node: full status report (AUTH + REQ_STATUS over the wire).
    assert_eq!(nodes[0]["addr"], node_addr);
    assert_eq!(nodes[0]["reachable"], true);
    assert_eq!(nodes[0]["status"]["name"], "docsql");
    assert!(nodes[0]["latency_ms"].as_f64().is_some());

    // Dead node: refused connection, explanatory error, no status.
    assert_eq!(nodes[1]["addr"], "127.0.0.1:1");
    assert_eq!(nodes[1]["reachable"], false);
    assert!(!nodes[1]["error"].as_str().unwrap().is_empty());
    assert!(nodes[1]["status"].is_null());
}

/// /api/logs: token-gated; serves the console's own statement audit as the
/// local section and fetches each peer's REQ_LOGS report over the wire.
#[tokio::test]
async fn logs_endpoint_serves_local_and_node_reports() {
    // Token gate first.
    let daddr = start_web(Some("sekrit"), Vec::new(), None).await;
    let res = http(&daddr, "GET", "/api/logs", None, None).await;
    assert_eq!(res.status, 401);

    // A real node behind the same token + a dead address.
    let node_dir = tempfile::tempdir().unwrap();
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    let node_addr = format!("127.0.0.1:{port}");
    tokio::spawn(docsql_server::run(docsql_server::ServerConfig {
        db_path: node_dir.path().join("node.db"),
        listen: node_addr.clone(),
        auth_token: Some("sekrit".into()),
        read_token: None,
        max_conn: 0,
        idle_timeout_secs: 0,
        auth_lock_threshold: 10,
        cluster_token: None,
        advertise: None,
        replicate_to: None,
        peers: Vec::new(),
        read_only: false,
        transport_key: None,
        async_commit: false,
        catchup_window: 0,
        backup_interval_secs: 0,
        backup_keep: 7,
        backup_dir: None,
    }));
    for _ in 0..100 {
        if TcpStream::connect(&node_addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let addr = start_web(
        Some("sekrit"),
        vec![node_addr.clone(), "127.0.0.1:1".into()],
        Some(node_addr.clone()),
    )
    .await;

    // Console statement → local audit ring (tagged with the node it ran
    // on); a node-side write → its query log.
    sql(&addr, Some("sekrit"), "CREATE TABLE wl (id INT)").await;
    {
        let mut s = TcpStream::connect(&node_addr).await.unwrap();
        let auth = docsql_core::proto::Frame::new(docsql_core::proto::REQ_AUTH, b"sekrit".to_vec());
        s.write_all(&auth.encode().unwrap()).await.unwrap();
        let stmt = docsql_core::proto::Frame::new(
            docsql_core::proto::REQ_SQL,
            docsql_core::proto::encode_sql("CREATE TABLE nl (id INT)").unwrap(),
        );
        s.write_all(&stmt.encode().unwrap()).await.unwrap();
        // Drain AUTH ack + SQL response — decode frames from a buffer
        // (both answers can arrive coalesced in one TCP segment, so a
        // fixed number of raw reads would block forever) and bound the
        // wait so a silent server fails the test instead of hanging it.
        let mut buf: Vec<u8> = Vec::new();
        let mut seen = 0;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while seen < 2 && tokio::time::Instant::now() < deadline {
            let mut chunk = [0u8; 4096];
            let n = tokio::time::timeout(Duration::from_secs(5), s.read(&mut chunk))
                .await
                .expect("timed out draining node responses")
                .unwrap();
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            let mut consumed = 0usize;
            while let Ok((_f, used)) = docsql_core::proto::Frame::decode(&buf[consumed..]) {
                consumed += used;
                seen += 1;
            }
            buf.drain(..consumed);
        }
        assert_eq!(seen, 2, "node did not answer AUTH + SQL");
    }

    let res = http(&addr, "GET", "/api/logs?limit=50", Some("sekrit"), None).await;
    assert_eq!(res.status, 200);
    let v = res.json();
    let local_query = v["local"]["query"].as_array().unwrap();
    let console_entry = local_query
        .iter()
        .find(|e| e["sql"].as_str() == Some("CREATE TABLE wl (id INT)"))
        .expect("local section missing console statement");
    // The console audit names the node the statement actually ran on.
    assert_eq!(console_entry["peer"], node_addr, "{v}");
    assert!(v["local"]["sync"].as_array().unwrap().is_empty());
    let nodes = v["nodes"].as_array().unwrap();
    assert_eq!(nodes[0]["addr"], node_addr);
    assert_eq!(nodes[0]["reachable"], true);
    let query = nodes[0]["logs"]["query"].as_array().unwrap();
    assert!(
        query
            .iter()
            .any(|e| e["sql"].as_str() == Some("CREATE TABLE nl (id INT)")),
        "node section missing its statement: {nodes:?}"
    );
    // The console's AUTH to this node is audited in its sync ring; only
    // auth events may be there, no data-plane fan-out.
    assert!(
        nodes[0]["logs"]["sync"]
            .as_array()
            .unwrap()
            .iter()
            .all(|e| e["event"] == "auth"),
        "unexpected non-auth sync events: {nodes:?}"
    );
    // Dead node surfaces as unreachable with an explanatory error.
    assert_eq!(nodes[1]["addr"], "127.0.0.1:1");
    assert_eq!(nodes[1]["reachable"], false);
    assert!(nodes[1]["logs"].is_null());
    assert!(!nodes[1]["error"].as_str().unwrap().is_empty());
}

/// Managed-node selection over HTTP: without a `node` the call lands on
/// the default managed node; an explicit `node` routes to that DOCSQL_PEERS
/// peer; unconfigured targets are refused; an offline peer errors in-band.
#[tokio::test]
async fn node_selection_routes_sql_meta_stats() {
    // The managed node behind the token + a dead configured address.
    let node_dir = tempfile::tempdir().unwrap();
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    let node_addr = format!("127.0.0.1:{port}");
    tokio::spawn(docsql_server::run(docsql_server::ServerConfig {
        db_path: node_dir.path().join("node.db"),
        listen: node_addr.clone(),
        auth_token: Some("sekrit".into()),
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
        backup_interval_secs: 0,
        backup_keep: 7,
        backup_dir: None,
    }));
    for _ in 0..100 {
        if TcpStream::connect(&node_addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let addr = start_web(
        Some("sekrit"),
        vec![node_addr.clone(), "127.0.0.1:1".into()],
        Some(node_addr.clone()),
    )
    .await;

    // Default target: statements without a node land on the managed node.
    let r = sql(
        &addr,
        Some("sekrit"),
        "CREATE TABLE sw (id INT PRIMARY KEY, v TEXT)",
    )
    .await;
    assert_eq!(r["kind"], "affected", "{r}");
    let r = sql(
        &addr,
        Some("sekrit"),
        "INSERT INTO sw VALUES (1, 'from-console'); INSERT INTO sw VALUES (2, 'also')",
    )
    .await;
    assert_eq!(r["kind"], "batch", "{r}");
    assert_eq!(r["results"].as_array().unwrap().len(), 2);
    let r = sql(&addr, Some("sekrit"), "SELECT v FROM sw ORDER BY id").await;
    assert_eq!(r["rows"][0][0], "from-console");

    // Explicit node selection routes to that peer (same node here).
    let sql_node = |s: &str| {
        let body = serde_json::to_string(&json!({ "sql": s, "node": node_addr })).unwrap();
        let addr = addr.clone();
        async move {
            http(&addr, "POST", "/api/sql", Some("sekrit"), Some(&body))
                .await
                .json()
        }
    };
    let r = sql_node("SELECT COUNT(*) AS n FROM sw").await;
    assert_eq!(r["rows"][0][0], 2, "{r}");

    // Default meta/stats render the managed node's state.
    let m = http(&addr, "GET", "/api/meta", Some("sekrit"), None)
        .await
        .json();
    assert!(m.get("error").is_none(), "{m}");
    let tables = m["tables"].as_array().unwrap();
    let sw = tables.iter().find(|t| t["name"] == "sw").unwrap();
    assert_eq!(sw["row_count"], 2);
    assert!(!sw["index_defs"].as_array().unwrap().is_empty());
    assert!(m["storage"]["page_size"].as_u64().is_some());
    let s = http(&addr, "GET", "/api/stats", Some("sekrit"), None)
        .await
        .json();
    assert_eq!(s["tables"], 1, "{s}");
    assert!(s["uptime_ms"].as_u64().is_some());

    // Unconfigured targets are refused (allow-list = DOCSQL_PEERS).
    let body = serde_json::to_string(&json!({"sql": "SELECT 1", "node": "127.0.0.1:9"})).unwrap();
    let r = http(&addr, "POST", "/api/sql", Some("sekrit"), Some(&body))
        .await
        .json();
    assert!(r.get("error").is_some(), "{r}");
    let res = http(
        &addr,
        "GET",
        "/api/meta?node=evil.example:7600",
        Some("sekrit"),
        None,
    )
    .await;
    assert!(res.json().get("error").is_some());

    // A configured but offline node surfaces as an in-band error.
    let body = serde_json::to_string(&json!({"sql": "SELECT 1", "node": "127.0.0.1:1"})).unwrap();
    let r = http(&addr, "POST", "/api/sql", Some("sekrit"), Some(&body))
        .await
        .json();
    assert_eq!(r["kind"], "error", "{r}");
    assert!(r["message"].as_str().unwrap().contains("不可达"), "{r}");

    // The console token gate applies before any node connection.
    let body = serde_json::to_string(&json!({"sql": "SELECT 1", "node": node_addr})).unwrap();
    let res = http(&addr, "POST", "/api/sql", None, Some(&body)).await;
    assert_eq!(res.status, 401);
}

/// No managed node configured: data endpoints report the configuration gap
/// in-band instead of failing silently.
#[tokio::test]
async fn data_endpoints_without_upstream_report_config_error() {
    let addr = start_web(None, Vec::new(), None).await;
    let r = sql(&addr, None, "SELECT 1").await;
    assert!(r.get("error").is_some(), "{r}");
    assert!(
        r["error"].as_str().unwrap().contains("未配置管理目标节点"),
        "{r}"
    );
    let m = http(&addr, "GET", "/api/meta", None, None).await.json();
    assert!(m.get("error").is_some(), "{m}");
    let s = http(&addr, "GET", "/api/stats", None, None).await.json();
    assert!(s.get("error").is_some(), "{s}");
    // Pure syntax check still works — it needs no node.
    let body = serde_json::to_string(&json!({"sql": "SELECT 1"})).unwrap();
    let res = http(&addr, "POST", "/api/parse", None, Some(&body)).await;
    assert_eq!(res.json(), json!({"ok": true}));
}

/// Console account gate: first-use setup creates the account and a session,
/// data endpoints lock afterwards, and login/logout manage the session.
/// Every other test in this file runs without the auth file (legacy mode)
/// and must keep working unchanged — that is the regression guard.
#[tokio::test]
async fn console_account_setup_login_and_gate() {
    let dir = tempfile::tempdir().unwrap();
    let auth_file = dir.path().join("console-auth.json");
    let addr = start_web_auth(
        None,
        Vec::new(),
        None,
        Some(auth_file.to_string_lossy().into_owned()),
    )
    .await;

    // Setup mode. Data endpoints stay locked until the account exists and
    // the operator is logged in — the anonymous pre-setup window used to
    // expose the whole-database restore to anyone who could reach the port.
    let st = http(&addr, "GET", "/api/auth/status", None, None)
        .await
        .json();
    assert_eq!(st["mode"], "setup");
    assert_eq!(
        http(&addr, "GET", "/api/meta", None, None).await.status,
        401,
        "pre-setup data endpoints must not be anonymous"
    );

    // Weak password rejected; the account does not exist yet.
    let r = http(
        &addr,
        "POST",
        "/api/auth/setup",
        None,
        Some(r#"{"username":"admin","password":"short"}"#),
    )
    .await;
    assert_eq!(r.status, 400);
    let st = http(&addr, "GET", "/api/auth/status", None, None)
        .await
        .json();
    assert_eq!(st["mode"], "setup");

    // Create the account → session cookie; the file is written.
    let r = http(
        &addr,
        "POST",
        "/api/auth/setup",
        None,
        Some(r#"{"username":"admin","password":"s3cret-pw"}"#),
    )
    .await;
    assert_eq!(r.status, 200);
    let cookie = r.header("set-cookie").expect("session cookie").to_string();
    assert!(cookie.starts_with("docsql_session="), "{cookie}");
    assert!(auth_file.exists(), "credential file written");

    // One-shot: a second setup attempt is a conflict; anonymous data is now
    // rejected; status flipped to login mode with the username.
    let r = http(
        &addr,
        "POST",
        "/api/auth/setup",
        None,
        Some(r#"{"username":"eve","password":"s3cret-pw"}"#),
    )
    .await;
    assert_eq!(r.status, 409);
    assert_eq!(
        http(&addr, "GET", "/api/meta", None, None).await.status,
        401
    );
    let st = http(&addr, "GET", "/api/auth/status", None, None)
        .await
        .json();
    assert_eq!(st["mode"], "login");
    assert_eq!(st["username"], "admin");

    // Wrong password → 401 (generic message); right one → a new session.
    let r = http(
        &addr,
        "POST",
        "/api/auth/login",
        None,
        Some(r#"{"username":"admin","password":"nope-nope"}"#),
    )
    .await;
    assert_eq!(r.status, 401);
    let r = http(
        &addr,
        "POST",
        "/api/auth/login",
        None,
        Some(r#"{"username":"admin","password":"s3cret-pw"}"#),
    )
    .await;
    assert_eq!(r.status, 200);
    let cookie2 = r.header("set-cookie").unwrap().to_string();

    // A session cookie opens data endpoints; logging out closes that
    // session while the independent one keeps working.
    assert_eq!(
        http_cookie(&addr, "GET", "/api/meta", &cookie, None)
            .await
            .status,
        200
    );
    let r = http_cookie(&addr, "POST", "/api/auth/logout", &cookie, None).await;
    assert_eq!(r.status, 200);
    assert_eq!(
        http_cookie(&addr, "GET", "/api/meta", &cookie, None)
            .await
            .status,
        401
    );
    assert_eq!(
        http_cookie(&addr, "GET", "/api/meta", &cookie2, None)
            .await
            .status,
        200
    );
}

/// With both DOCSQL_TOKEN and the console account configured, token-carrying
/// API clients keep working without any session (the programmatic bypass),
/// and anonymous calls are rejected even before setup.
#[tokio::test]
async fn console_account_token_bypass() {
    let dir = tempfile::tempdir().unwrap();
    let auth_file = dir.path().join("console-auth.json");
    let addr = start_web_auth(
        Some("node-secret"),
        Vec::new(),
        None,
        Some(auth_file.to_string_lossy().into_owned()),
    )
    .await;

    assert_eq!(
        http(&addr, "GET", "/api/meta", None, None).await.status,
        401
    );
    assert_eq!(
        http(&addr, "GET", "/api/meta", Some("wrong"), None)
            .await
            .status,
        401
    );
    assert_eq!(
        http(&addr, "GET", "/api/meta", Some("node-secret"), None)
            .await
            .status,
        200
    );
    let r = http(
        &addr,
        "POST",
        "/api/auth/setup",
        None,
        Some(r#"{"username":"admin","password":"s3cret-pw"}"#),
    )
    .await;
    assert_eq!(r.status, 200);
    assert_eq!(
        http(&addr, "GET", "/api/meta", None, None).await.status,
        401
    );
    assert_eq!(
        http(&addr, "GET", "/api/meta", Some("node-secret"), None)
            .await
            .status,
        200
    );
}

/// The credential file persists: a restarted console with the same file is
/// in login mode and accepts the same credentials (sessions do not persist —
/// a fresh login is required).
#[tokio::test]
async fn console_account_persists_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("console-auth.json");
    let addr = start_web_auth(
        None,
        Vec::new(),
        None,
        Some(path.to_string_lossy().into_owned()),
    )
    .await;
    let r = http(
        &addr,
        "POST",
        "/api/auth/setup",
        None,
        Some(r#"{"username":"admin","password":"s3cret-pw"}"#),
    )
    .await;
    assert_eq!(r.status, 200);

    let addr2 = start_web_auth(
        None,
        Vec::new(),
        None,
        Some(path.to_string_lossy().into_owned()),
    )
    .await;
    let st = http(&addr2, "GET", "/api/auth/status", None, None)
        .await
        .json();
    assert_eq!(st["mode"], "login");
    assert_eq!(st["username"], "admin");
    let r = http(
        &addr2,
        "POST",
        "/api/auth/login",
        None,
        Some(r#"{"username":"admin","password":"s3cret-pw"}"#),
    )
    .await;
    assert_eq!(r.status, 200);
    let cookie = r.header("set-cookie").unwrap().to_string();
    assert_eq!(
        http_cookie(&addr2, "GET", "/api/meta", &cookie, None)
            .await
            .status,
        200
    );
}

/// /api/backup: token-gated; GET lists the managed node's backup state
/// (REQ_BACKUP over the wire) and POST triggers one backup, whose file then
/// shows up in the next poll. `?node=` targets an explicit whitelisted peer.
#[tokio::test]
async fn backup_endpoint_lists_and_triggers() {
    // Token gate first.
    let daddr = start_web(Some("sekrit"), Vec::new(), None).await;
    let res = http(&daddr, "GET", "/api/backup", None, None).await;
    assert_eq!(res.status, 401);

    // Real node as default managed target AND whitelisted peer; some data.
    let node_dir = tempfile::tempdir().unwrap();
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    let node_addr = format!("127.0.0.1:{port}");
    tokio::spawn(docsql_server::run(docsql_server::ServerConfig {
        db_path: node_dir.path().join("node.db"),
        listen: node_addr.clone(),
        auth_token: Some("sekrit".into()),
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
        backup_interval_secs: 0,
        backup_keep: 7,
        backup_dir: None,
    }));
    for _ in 0..100 {
        if TcpStream::connect(&node_addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let web = start_web(
        Some("sekrit"),
        vec![node_addr.clone()],
        Some(node_addr.clone()),
    )
    .await;
    sql(
        &web,
        Some("sekrit"),
        "CREATE TABLE s (id INT PRIMARY KEY, v TEXT)",
    )
    .await;
    sql(&web, Some("sekrit"), "INSERT INTO s VALUES (1, 'bkp')").await;

    // GET: no backups yet (the interval is off in tests).
    let res = http(&web, "GET", "/api/backup", Some("sekrit"), None).await;
    assert_eq!(res.status, 200);
    let v = res.json();
    assert!(v["error"].is_null(), "{v}");
    assert_eq!(v["count"], 0);

    // POST: trigger; the node acknowledges and runs the backup async.
    let res = http(&web, "POST", "/api/backup", Some("sekrit"), Some("{}")).await;
    assert_eq!(res.status, 200);
    assert_eq!(res.json()["ok"], true);

    let mut ok = false;
    for _ in 0..250 {
        let v = http(&web, "GET", "/api/backup", Some("sekrit"), None)
            .await
            .json();
        if v["count"] == 1 && v["last"]["ok"] == true {
            assert!(v["files"][0]["name"]
                .as_str()
                .unwrap()
                .starts_with("backup-"));
            assert!(v["files"][0]["bytes"].as_u64().unwrap() > 0);
            ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(ok, "triggered backup never showed up in the list");

    // Explicit node override lands on the whitelisted peer itself.
    let res = http(
        &web,
        "GET",
        &format!("/api/backup?node={node_addr}"),
        Some("sekrit"),
        None,
    )
    .await;
    assert_eq!(res.status, 200);
    let v = res.json();
    assert!(v["error"].is_null(), "{v}");
    assert_eq!(v["count"], 1);

    // Unlisted nodes stay refused (SSRF guard).
    let res = http(
        &web,
        "GET",
        "/api/backup?node=evil.example:7600",
        Some("sekrit"),
        None,
    )
    .await;
    assert_eq!(res.status, 200);
    assert!(res.json()["error"].as_str().unwrap().contains("未知节点"));
}

/// /api/backup/restore: the named backup replays through the node's write
/// path — a table dropped after the backup comes back with its rows, and
/// invalid file names surface the node's refusal.
#[tokio::test]
async fn backup_restore_endpoint_round_trip() {
    // Real node as default managed target AND whitelisted peer.
    let node_dir = tempfile::tempdir().unwrap();
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    let node_addr = format!("127.0.0.1:{port}");
    tokio::spawn(docsql_server::run(docsql_server::ServerConfig {
        db_path: node_dir.path().join("node.db"),
        listen: node_addr.clone(),
        auth_token: Some("sekrit".into()),
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
        backup_interval_secs: 0,
        backup_keep: 7,
        backup_dir: None,
    }));
    for _ in 0..100 {
        if TcpStream::connect(&node_addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let web = start_web(
        Some("sekrit"),
        vec![node_addr.clone()],
        Some(node_addr.clone()),
    )
    .await;
    sql(
        &web,
        Some("sekrit"),
        "CREATE TABLE s (id INT PRIMARY KEY, v TEXT)",
    )
    .await;
    sql(
        &web,
        Some("sekrit"),
        "INSERT INTO s VALUES (1, 'rt-1'), (2, 'rt-2')",
    )
    .await;

    // Backup, wait for it to land.
    let res = http(&web, "POST", "/api/backup", Some("sekrit"), Some("{}")).await;
    assert_eq!(res.json()["ok"], true);
    let mut name = String::new();
    for _ in 0..250 {
        let v = http(&web, "GET", "/api/backup", Some("sekrit"), None)
            .await
            .json();
        if v["last"]["ok"] == true {
            name = v["files"][0]["name"].as_str().unwrap().to_string();
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(!name.is_empty(), "backup never appeared");

    // Drop the table, then restore the backup — explicitly targeting the
    // peer via ?node= (the restore endpoint honors the query param too).
    sql(&web, Some("sekrit"), "DROP TABLE s").await;
    // The confirm field must repeat the file name exactly: a restore is a
    // whole-database replacement, the request itself carries the intent.
    let body = serde_json::to_string(&json!({ "file": name })).unwrap();
    let res = http(
        &web,
        "POST",
        &format!("/api/backup/restore?node={node_addr}"),
        Some("sekrit"),
        Some(&body),
    )
    .await;
    assert_eq!(res.status, 400, "missing confirm must be refused");

    let body = serde_json::to_string(&json!({ "file": name, "confirm": name })).unwrap();
    let res = http(
        &web,
        "POST",
        &format!("/api/backup/restore?node={node_addr}"),
        Some("sekrit"),
        Some(&body),
    )
    .await;
    assert_eq!(res.status, 200);
    assert_eq!(res.json()["ok"], true);

    let mut done = false;
    for _ in 0..250 {
        let v = http(&web, "GET", "/api/backup", Some("sekrit"), None)
            .await
            .json();
        if v["restore"]["running"] == false && v["restore"]["ok"] == true {
            done = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(done, "restore never completed");

    let out = sql(&web, Some("sekrit"), "SELECT v FROM s WHERE id = 2").await;
    assert!(out.to_string().contains("rt-2"), "rows not restored: {out}");

    // Invalid names surface the node's refusal in-band (the confirm field
    // repeats the name; the node's own validation refuses the path).
    let body = serde_json::to_string(&json!({ "file": "../docsql.db", "confirm": "../docsql.db" }))
        .unwrap();
    let res = http(
        &web,
        "POST",
        "/api/backup/restore",
        Some("sekrit"),
        Some(&body),
    )
    .await;
    assert_eq!(res.status, 200);
    assert!(res.json()["error"].as_str().unwrap().contains("restore"));
}
