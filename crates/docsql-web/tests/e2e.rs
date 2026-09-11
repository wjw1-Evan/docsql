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
    start_web_full(token, peers, upstream, auth_file, None).await
}

/// Full-control console start, including optional native TLS (PEM paths).
async fn start_web_full(
    token: Option<&str>,
    peers: Vec<String>,
    upstream: Option<String>,
    auth_file: Option<String>,
    tls: Option<(String, String)>,
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
        tls: tls.map(|(cert_path, key_path)| docsql_web::TlsConfig {
            cert_path,
            key_path,
        }),
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
        statement_timeout_ms: 0,
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
    http_headers(addr, method, path, token, cookie, body, &[]).await
}

/// Same with extra raw header lines (e.g. X-Forwarded-For probes for the
/// login-lockout bucketing).
async fn http_headers(
    addr: &str,
    method: &str,
    path: &str,
    token: Option<&str>,
    cookie: Option<&str>,
    body: Option<&str>,
    extra_headers: &[String],
) -> HttpResponse {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n");
    if let Some(t) = token {
        req.push_str(&format!("X-Docsql-Token: {t}\r\n"));
    }
    if let Some(c) = cookie {
        req.push_str(&format!("Cookie: {c}\r\n"));
    }
    for line in extra_headers {
        req.push_str(line);
        req.push_str("\r\n");
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

/// 控制台账号的错误测试密码(运行时拼接,避免源码字面量凭据)。
fn console_wrong_pw() -> String {
    ["wr", "on", "g-w", "ro", "ng-p", "w9"].concat()
}

/// 单字符重复的非法密码(密码策略拒绝用)。
fn repeat_pw(c: &str) -> String {
    c.repeat(8)
}

/// 账号接口的 JSON body 组装。
fn creds_body(username: &str, password: &str) -> String {
    serde_json::to_string(&json!({ "username": username, "password": password })).unwrap()
}

/// 轮询一个单值查询直到其首格等于 expect(有界)。服务端在控制台断连
/// 读到 EOF 时回滚被遗弃的事务,这与下一次调用的新连接存在竞态;被轮询
/// 的行只会消失不会复现,poll-until-match 因此是可靠的。
async fn wait_for_scalar(addr: &str, q: &str, expect: serde_json::Value) -> serde_json::Value {
    let mut last = serde_json::Value::Null;
    for _ in 0..250 {
        last = sql(addr, None, q).await["rows"].clone();
        if last == expect {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    last
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
        statement_timeout_ms: 0,
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
        statement_timeout_ms: 0,
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
        statement_timeout_ms: 0,
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
        statement_timeout_ms: 0,
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
        statement_timeout_ms: 0,
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

// ---- /api/users:数据库用户与角色的控制台管理面 ----

/// POST /api/users 便捷封装。
async fn users_post(web: &str, token: Option<&str>, body: serde_json::Value) -> serde_json::Value {
    let body = serde_json::to_string(&body).unwrap();
    http(web, "POST", "/api/users", token, Some(&body))
        .await
        .json()
}

/// 测试密码运行时拼接(避免源码中出现字面量凭据)。
fn users_test_pw() -> String {
    ["con", "so", "le", "-p", "w1", "23"].concat()
}

#[tokio::test]
async fn users_page_manages_users_roles_and_grants() {
    let (_dir, web, _node) = start_stack(Some("console-admin-token"), vec![]).await;
    // 建一张表给表级授权用。
    let out = sql(&web, Some("console-admin-token"), "CREATE TABLE t (id INT)").await;
    assert!(out["error"].is_null(), "{out}");

    // 空状态:无用户、只有三个内置角色。
    let list = http(&web, "GET", "/api/users", Some("console-admin-token"), None)
        .await
        .json();
    assert!(list["error"].is_null(), "{list}");
    assert_eq!(list["users"].as_array().unwrap().len(), 0);
    let roles: Vec<&str> = list["roles"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["name"].as_str().unwrap())
        .collect();
    assert_eq!(roles, vec!["admin", "readwrite", "readonly"]);

    // 创建用户 + 授角色 + 自定义角色 + 表级授权。
    let pw = users_test_pw();
    let mut r = users_post(
        &web,
        Some("console-admin-token"),
        json!({
            "action": "create_user", "name": "alice", "password": pw
        }),
    )
    .await;
    assert!(r["error"].is_null(), "{r}");
    r = users_post(
        &web,
        Some("console-admin-token"),
        json!({
            "action": "grant_role", "role": "readonly", "name": "alice"
        }),
    )
    .await;
    assert!(r["error"].is_null(), "{r}");
    r = users_post(
        &web,
        Some("console-admin-token"),
        json!({
            "action": "create_role", "name": "reporting"
        }),
    )
    .await;
    assert!(r["error"].is_null(), "{r}");
    r = users_post(
        &web,
        Some("console-admin-token"),
        json!({
            "action": "grant_table", "name": "reporting", "table": "t",
            "privs": ["SELECT", "UPDATE"]
        }),
    )
    .await;
    assert!(r["error"].is_null(), "{r}");
    r = users_post(
        &web,
        Some("console-admin-token"),
        json!({
            "action": "grant_role", "role": "reporting", "name": "alice"
        }),
    )
    .await;
    assert!(r["error"].is_null(), "{r}");

    // 聚合视图:alice 的角色、直接/有效表权限;角色的成员与授权。
    let list = http(&web, "GET", "/api/users", Some("console-admin-token"), None)
        .await
        .json();
    let alice = list["users"]
        .as_array()
        .unwrap()
        .iter()
        .find(|u| u["name"] == "alice")
        .cloned()
        .expect("alice listed");
    let mut got_roles: Vec<&str> = alice["roles"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    got_roles.sort();
    assert_eq!(got_roles, vec!["readonly", "reporting"]);
    // 直接授权为空;有效(经角色)t 表 = SELECT, UPDATE
    assert_eq!(alice["direct"].as_object().unwrap().len(), 0);
    let mut eff: Vec<&str> = alice["grants"]["t"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    eff.sort();
    assert_eq!(eff, vec!["SELECT", "UPDATE"]);
    let reporting = list["roles"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == "reporting")
        .cloned()
        .unwrap();
    assert_eq!(reporting["members"].as_array().unwrap().len(), 1);
    assert_eq!(reporting["members"][0], "alice");

    // 撤销表权限与角色;改密码;删除用户。
    r = users_post(
        &web,
        Some("console-admin-token"),
        json!({
            "action": "revoke_table", "name": "reporting", "table": "t", "privs": ["UPDATE"]
        }),
    )
    .await;
    assert!(r["error"].is_null(), "{r}");
    let newpw = ["ro", "ta", "te", "d9", "9p", "w!"].concat();
    r = users_post(
        &web,
        Some("console-admin-token"),
        json!({
            "action": "alter_password", "name": "alice", "password": newpw
        }),
    )
    .await;
    assert!(r["error"].is_null(), "{r}");
    r = users_post(
        &web,
        Some("console-admin-token"),
        json!({
            "action": "revoke_role", "role": "reporting", "name": "alice"
        }),
    )
    .await;
    assert!(r["error"].is_null(), "{r}");
    r = users_post(
        &web,
        Some("console-admin-token"),
        json!({
            "action": "drop_user", "name": "alice"
        }),
    )
    .await;
    assert!(r["error"].is_null(), "{r}");
    let list = http(&web, "GET", "/api/users", Some("console-admin-token"), None)
        .await
        .json();
    assert_eq!(list["users"].as_array().unwrap().len(), 0);
    assert_eq!(reporting_after(&list), 0); // reporting 成员被级联清空
    r = users_post(
        &web,
        Some("console-admin-token"),
        json!({
            "action": "drop_role", "name": "reporting"
        }),
    )
    .await;
    assert!(r["error"].is_null(), "{r}");

    // 非法输入在构造层被拒(in-band),永远不触达节点:注入形状的用户名、
    // 过短密码、未知 action。
    let bad = users_post(
        &web,
        Some("console-admin-token"),
        json!({
            "action": "create_user", "name": "x; DROP TABLE t", "password": "long-enough-pw"
        }),
    )
    .await;
    assert!(bad["error"].as_str().unwrap().contains("不合法"), "{bad}");
    let bad = users_post(
        &web,
        Some("console-admin-token"),
        json!({
            "action": "create_user", "name": "mallory", "password": "short"
        }),
    )
    .await;
    assert!(bad["error"].as_str().unwrap().contains("8-256"), "{bad}");
    let bad = users_post(
        &web,
        Some("console-admin-token"),
        json!({
            "action": "explode", "name": "x"
        }),
    )
    .await;
    assert!(bad["error"].as_str().unwrap().contains("未知操作"), "{bad}");
    // 注入形状的用户名没有创建任何东西,表也还在。
    let out = sql(&web, Some("console-admin-token"), "SELECT COUNT(id) FROM t").await;
    assert!(out["error"].is_null(), "{out}");
}

fn reporting_after(list: &serde_json::Value) -> usize {
    list["roles"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == "reporting")
        .map(|r| r["members"].as_array().unwrap().len())
        .unwrap_or(0)
}

#[tokio::test]
async fn users_page_surfaces_the_authentication_required_state() {
    // 无 token 节点:开放模式可建第一个用户;此后控制台(匿名、每请求
    // 新连接)被节点拒绝 —— 页面以 in-band error 呈现。
    let (_dir, web, _node) = start_stack(None, vec![]).await;
    let list = http(&web, "GET", "/api/users", None, None).await.json();
    assert!(list["error"].is_null(), "{list}");
    assert_eq!(list["users"].as_array().unwrap().len(), 0);

    let pw = users_test_pw();
    let r = users_post(
        &web,
        None,
        json!({
            "action": "create_user", "name": "firstadmin", "password": pw
        }),
    )
    .await;
    assert!(
        r["error"].is_null(),
        "first user must be creatable in open mode: {r}"
    );

    let list = http(&web, "GET", "/api/users", None, None).await.json();
    let err = list["error"].as_str().unwrap_or_default().to_string();
    assert!(err.contains("authentication required"), "got: {list}");
}

/// Request body for /api/auth/change (fields built as values so the bodies
/// read as fixtures, not as source-literal credentials).
fn change_body(cur: &str, user: &str, pw: &str) -> String {
    json!({"current_password": cur, "username": user, "password": pw}).to_string()
}

/// Credential change: needs the current password on top of a live session,
/// rotates username and/or password in one call, kicks every other live
/// session (password rotation must not leave other holders logged in), and
/// keeps the caller's own session.
#[tokio::test]
async fn console_account_change_credentials() {
    let dir = tempfile::tempdir().unwrap();
    let auth_file = dir.path().join("console-auth.json");
    let addr = start_web_auth(
        None,
        Vec::new(),
        None,
        Some(auth_file.to_string_lossy().into_owned()),
    )
    .await;

    // Setup + a second login: two live sessions in two "browsers".
    let r = http(
        &addr,
        "POST",
        "/api/auth/setup",
        None,
        Some(r#"{"username":"admin","password":"s3cret-pw"}"#),
    )
    .await;
    assert_eq!(r.status, 200);
    let cookie_a = r.header("set-cookie").unwrap().to_string();
    let r = http(
        &addr,
        "POST",
        "/api/auth/login",
        None,
        Some(r#"{"username":"admin","password":"s3cret-pw"}"#),
    )
    .await;
    assert_eq!(r.status, 200);
    let cookie_b = r.header("set-cookie").unwrap().to_string();

    // No session: refused before the current password is even looked at.
    let r = http(
        &addr,
        "POST",
        "/api/auth/change",
        None,
        Some(&change_body("s3cret-pw", "root", "")),
    )
    .await;
    assert_eq!(r.status, 401);

    // Wrong current password: 401, account untouched.
    let r = http_cookie(
        &addr,
        "POST",
        "/api/auth/change",
        &cookie_a,
        Some(&change_body("wrong-pass", "root", "")),
    )
    .await;
    assert_eq!(r.status, 401);
    let st = http(&addr, "GET", "/api/auth/status", None, None)
        .await
        .json();
    assert_eq!(st["username"], "admin");

    // Weak new password: 400, account untouched.
    let r = http_cookie(
        &addr,
        "POST",
        "/api/auth/change",
        &cookie_a,
        Some(&change_body("s3cret-pw", "root", "short")),
    )
    .await;
    assert_eq!(r.status, 400);

    // Rotate username and password in one call.
    let r = http_cookie(
        &addr,
        "POST",
        "/api/auth/change",
        &cookie_a,
        Some(&change_body("s3cret-pw", "root", "n3w-password")),
    )
    .await;
    assert_eq!(r.status, 200);
    assert_eq!(r.json()["username"], "root");
    let st = http(&addr, "GET", "/api/auth/status", None, None)
        .await
        .json();
    assert_eq!(st["username"], "root");

    // Old credentials no longer log in; the new ones do.
    let r = http(
        &addr,
        "POST",
        "/api/auth/login",
        None,
        Some(r#"{"username":"root","password":"s3cret-pw"}"#),
    )
    .await;
    assert_eq!(r.status, 401);
    let r = http(
        &addr,
        "POST",
        "/api/auth/login",
        None,
        Some(r#"{"username":"root","password":"n3w-password"}"#),
    )
    .await;
    assert_eq!(r.status, 200);

    // The other session died with the rotation; the caller's survives.
    assert_eq!(
        http_cookie(&addr, "GET", "/api/meta", &cookie_b, None)
            .await
            .status,
        401
    );
    assert_eq!(
        http_cookie(&addr, "GET", "/api/meta", &cookie_a, None)
            .await
            .status,
        200
    );
}

// ---- 补充覆盖:事务契约 / 锁定 / 关闭面 / 节点错误面 / 参数钳制 / 输入策略 ----

/// One /api/sql call runs its whole batch on ONE node connection: a
/// BEGIN/COMMIT batch is a real transaction, and a batch that abandons one
/// (no COMMIT, or an error mid-batch) is rolled back when the console
/// drops its connection — the next call's fresh connection sees the old
/// state. This is the documented connection-per-call contract the console
/// editor's multi-statement runs rely on.
#[tokio::test]
async fn batch_transaction_spans_one_call_and_abandoned_rolls_back() {
    let (_dir, addr, _node) = start_stack(None, Vec::new()).await;
    assert_eq!(
        sql(&addr, None, "CREATE TABLE tx (id INT PRIMARY KEY, v TEXT)").await,
        json!({"kind": "affected", "count": 0})
    );

    // Committed batch: BEGIN, INSERT, COMMIT ride one connection, so the
    // row survives into the next call.
    let r = sql(
        &addr,
        None,
        "BEGIN; INSERT INTO tx VALUES (1, 'kept'); COMMIT",
    )
    .await;
    assert_eq!(r["kind"], "batch", "{r}");
    assert!(r["error"].is_null(), "{r}");
    let results = r["results"].as_array().unwrap();
    assert_eq!(results.len(), 3);
    assert!(results.iter().all(|o| o["kind"] == "affected"), "{r}");
    let r = sql(&addr, None, "SELECT v FROM tx").await;
    assert_eq!(r["rows"], json!([["kept"]]), "{r}");

    // Abandoned batch: BEGIN + INSERT without COMMIT — the console hangs
    // up after answering, and the server rolls the ownerless transaction
    // back.
    let r = sql(&addr, None, "BEGIN; INSERT INTO tx VALUES (2, 'ghost')").await;
    assert_eq!(r["kind"], "batch", "{r}");
    assert!(r["error"].is_null(), "{r}");
    let rows = wait_for_scalar(&addr, "SELECT COUNT(*) FROM tx", json!([[1]])).await;
    assert_eq!(rows, json!([[1]]), "abandoned transaction must roll back");

    // Error mid-batch aborts the call with the partial results + index;
    // the statements that DID run inside the open transaction roll back
    // with it.
    let r = sql(
        &addr,
        None,
        "BEGIN; INSERT INTO tx VALUES (3, 'doomed'); SELECT * FROM missing",
    )
    .await;
    assert_eq!(r["kind"], "batch", "{r}");
    assert_eq!(r["error"]["statement"], 2, "{r}");
    assert_eq!(r["results"].as_array().unwrap().len(), 2);
    let rows = wait_for_scalar(&addr, "SELECT COUNT(*) FROM tx", json!([[1]])).await;
    assert_eq!(rows, json!([[1]]), "mid-batch failure must roll back");
}

/// Login lockout over HTTP: LOCK_THRESHOLD wrong passwords from one source
/// lock the bucket — even the CORRECT password is refused with 429 until
/// the lockout expires. Spoofed X-Forwarded-For values must not rotate the
/// bucket: without DOCSQL_WEB_TRUST_PROXY a client-supplied header never
/// chooses the bucket (the socket peer does).
#[tokio::test]
async fn login_lockout_after_threshold_ignores_spoofed_xff() {
    let dir = tempfile::tempdir().unwrap();
    let addr = start_web_auth(
        None,
        Vec::new(),
        None,
        Some(
            dir.path()
                .join("console-auth.json")
                .to_string_lossy()
                .into_owned(),
        ),
    )
    .await;
    let body = creds_body("admin", &users_test_pw());
    let r = http(&addr, "POST", "/api/auth/setup", None, Some(&body)).await;
    assert_eq!(r.status, 200);

    // Exactly the threshold of failures, each claiming a different
    // forwarded client — they all land in the same socket-IP bucket.
    let wrong = creds_body("admin", &console_wrong_pw());
    for i in 0..docsql_web::auth::LOCK_THRESHOLD {
        let xff = format!("X-Forwarded-For: 10.9.0.{i}");
        let r = http_headers(
            &addr,
            "POST",
            "/api/auth/login",
            None,
            None,
            Some(&wrong),
            &[xff],
        )
        .await;
        assert_eq!(r.status, 401, "failure {i} must be a plain 401");
        assert_eq!(
            r.json()["error"].as_str().unwrap(),
            "用户名或密码不正确",
            "generic message must not narrow the guess"
        );
    }

    // Locked: the correct password is refused too, whether it arrives with
    // a fresh spoofed XFF or with none at all (same bucket either way).
    let right = creds_body("admin", &users_test_pw());
    for extra in [vec!["X-Forwarded-For: 10.9.0.99".to_string()], Vec::new()] {
        let r = http_headers(
            &addr,
            "POST",
            "/api/auth/login",
            None,
            None,
            Some(&right),
            &extra,
        )
        .await;
        assert_eq!(r.status, 429, "{extra:?}");
        assert!(
            r.json()["error"].as_str().unwrap().contains("尝试次数过多"),
            "{extra:?}"
        );
    }
}

/// With the credential file unset the account surface is off: status
/// reports "off", setup/login/change answer 404, logout stays idempotently
/// safe (ok + cookie clear), the backup endpoints join the other data
/// endpoints in reporting the missing managed node in-band, and unknown
/// routes are plain 404s.
#[tokio::test]
async fn auth_disabled_surface_and_no_upstream_backup() {
    let addr = start_web(None, Vec::new(), None).await;

    let st = http(&addr, "GET", "/api/auth/status", None, None)
        .await
        .json();
    assert_eq!(st, json!({"mode": "off"}));
    let body = creds_body("admin", &users_test_pw());
    assert_eq!(
        http(&addr, "POST", "/api/auth/setup", None, Some(&body))
            .await
            .status,
        404
    );
    assert_eq!(
        http(&addr, "POST", "/api/auth/login", None, Some(&body))
            .await
            .status,
        404
    );
    let change = serde_json::to_string(
        &json!({"current_password": users_test_pw(), "username": "root", "password": ""}),
    )
    .unwrap();
    assert_eq!(
        http(&addr, "POST", "/api/auth/change", None, Some(&change))
            .await
            .status,
        404
    );
    let r = http(&addr, "POST", "/api/auth/logout", None, None).await;
    assert_eq!(r.status, 200);
    let clear = r.header("set-cookie").unwrap().to_string();
    assert_eq!(r.json()["ok"], true);
    assert!(
        clear.contains("Max-Age=0"),
        "logout must clear the cookie: {clear}"
    );

    // Backup endpoints without a managed node: in-band config error, same
    // convention as /api/sql + /api/meta + /api/stats.
    let v = http(&addr, "GET", "/api/backup", None, None).await.json();
    assert!(
        v["error"].as_str().unwrap().contains("未配置管理目标节点"),
        "{v}"
    );
    let v = http(&addr, "POST", "/api/backup", None, Some("{}"))
        .await
        .json();
    assert!(
        v["error"].as_str().unwrap().contains("未配置管理目标节点"),
        "{v}"
    );

    assert_eq!(
        http(&addr, "GET", "/api/nope", None, None).await.status,
        404
    );
}

/// A console whose token does not match the managed node's: the web gate
/// passes (the browser presented the console's own token) but the
/// node-side AUTH fails — data endpoints answer with the node's refusal
/// in-band, and the cluster probe reports the node alive-but-refusing
/// (PING needs no auth, so reachable stays true with the error and no
/// status). The console page itself still loads: the misconfiguration is
/// a backend concern.
#[tokio::test]
async fn console_node_token_mismatch_surfaces_in_band() {
    let node_dir = tempfile::tempdir().unwrap();
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    let node_addr = format!("127.0.0.1:{port}");
    tokio::spawn(docsql_server::run(docsql_server::ServerConfig {
        db_path: node_dir.path().join("node.db"),
        listen: node_addr.clone(),
        auth_token: Some("node-secret".into()),
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
        statement_timeout_ms: 0,
    }));
    for _ in 0..100 {
        if TcpStream::connect(&node_addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let addr = start_web(
        Some("web-secret"),
        vec![node_addr.clone()],
        Some(node_addr.clone()),
    )
    .await;

    assert_eq!(http(&addr, "GET", "/", None, None).await.status, 200);

    let r = sql(&addr, Some("web-secret"), "SELECT 1").await;
    assert_eq!(r["kind"], "error", "{r}");
    assert!(r["message"].as_str().unwrap().contains("拒绝认证"), "{r}");
    let m = http(&addr, "GET", "/api/meta", Some("web-secret"), None)
        .await
        .json();
    assert!(m["error"].as_str().unwrap().contains("拒绝认证"), "{m}");
    let s = http(&addr, "GET", "/api/stats", Some("web-secret"), None)
        .await
        .json();
    assert!(s["error"].as_str().unwrap().contains("拒绝认证"), "{s}");

    let c = http(&addr, "GET", "/api/cluster", Some("web-secret"), None)
        .await
        .json();
    let nodes = c["nodes"].as_array().unwrap();
    assert_eq!(nodes[0]["addr"], node_addr);
    assert_eq!(nodes[0]["reachable"], true, "{c}");
    // Latency only surfaces on the full-success path; an AUTH refusal
    // keeps it null alongside the error.
    assert!(nodes[0]["latency_ms"].is_null(), "{c}");
    assert!(
        nodes[0]["error"]
            .as_str()
            .unwrap()
            .contains("node rejected AUTH"),
        "{c}"
    );
    assert!(nodes[0]["status"].is_null(), "{c}");
}

/// Configured-but-offline peers surface as in-band errors on every data
/// endpoint (meta/stats/backup), the live peer still serves explicit
/// `?node=` traffic, a restore whose target is unreachable fails the HTTP
/// layer with 502 (transport failure — scripts tell it apart from the
/// node's own refusals), and a well-named but missing backup file on a
/// live node stays an in-band 200 error.
#[tokio::test]
async fn offline_node_in_band_errors_and_restore_transport_502() {
    // Live node (default managed target) + a dead configured peer.
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
        statement_timeout_ms: 0,
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

    for path in [
        "/api/meta?node=127.0.0.1:1",
        "/api/stats?node=127.0.0.1:1",
        "/api/backup?node=127.0.0.1:1",
    ] {
        let v = http(&addr, "GET", path, Some("sekrit"), None).await.json();
        assert!(
            v["error"].as_str().unwrap().contains("不可达"),
            "{path}: {v}"
        );
    }

    // The live peer under its explicit name: meta + stats render fine.
    let m = http(
        &addr,
        "GET",
        &format!("/api/meta?node={node_addr}"),
        Some("sekrit"),
        None,
    )
    .await
    .json();
    assert!(m.get("error").is_none(), "{m}");
    assert!(m["totals"].is_object(), "{m}");
    let s = http(
        &addr,
        "GET",
        &format!("/api/stats?node={node_addr}"),
        Some("sekrit"),
        None,
    )
    .await
    .json();
    assert!(s.get("error").is_none(), "{s}");
    assert!(s["uptime_ms"].as_u64().is_some(), "{s}");

    // Restore against the offline peer: transport failure → 502.
    let body = serde_json::to_string(&json!({
        "file": "backup-x.sql",
        "confirm": "backup-x.sql",
    }))
    .unwrap();
    let res = http(
        &addr,
        "POST",
        "/api/backup/restore?node=127.0.0.1:1",
        Some("sekrit"),
        Some(&body),
    )
    .await;
    let status = res.status;
    let text = res.text();
    assert_eq!(status, 502, "{text}");
    let v: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert!(v["error"].as_str().unwrap().contains("不可达"), "{text}");

    // Same request on the live node, but the file does not exist: the
    // node's own refusal, in-band with 200.
    let body = serde_json::to_string(&json!({
        "file": "backup-missing.sql",
        "confirm": "backup-missing.sql",
    }))
    .unwrap();
    let res = http(
        &addr,
        "POST",
        "/api/backup/restore",
        Some("sekrit"),
        Some(&body),
    )
    .await;
    let status = res.status;
    let text = res.text();
    assert_eq!(status, 200, "{text}");
    assert!(
        text.contains("no such backup file"),
        "missing-file restore must be an in-band refusal: {text}"
    );
}

/// The JSON body carries the node override too (the console's api.post
/// merges the selected node into the body): POST /api/backup honors
/// {"node": …}, POST /api/backup/restore restores by {"node": …} without
/// a query param, and a bare trigger (no body at all) lands on the
/// default managed node.
#[tokio::test]
async fn backup_node_override_via_json_body_and_bare_trigger() {
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
        statement_timeout_ms: 0,
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
        "CREATE TABLE bj (id INT PRIMARY KEY, v TEXT)",
    )
    .await;
    sql(
        &web,
        Some("sekrit"),
        "INSERT INTO bj VALUES (1, 'json-node')",
    )
    .await;

    // Bare trigger: no body at all → default managed node.
    let res = http(&web, "POST", "/api/backup", Some("sekrit"), None).await;
    assert_eq!(res.status, 200);
    assert_eq!(res.json()["ok"], true);

    // JSON-body node override triggers on the named peer too.
    let body = serde_json::to_string(&json!({ "node": node_addr })).unwrap();
    let res = http(&web, "POST", "/api/backup", Some("sekrit"), Some(&body)).await;
    assert_eq!(res.status, 200);
    assert_eq!(res.json()["ok"], true);

    let mut name = String::new();
    for _ in 0..250 {
        let v = http(&web, "GET", "/api/backup", Some("sekrit"), None)
            .await
            .json();
        if v["count"].as_u64().unwrap_or(0) >= 1 && v["last"]["ok"] == true {
            name = v["files"][0]["name"].as_str().unwrap_or("").to_string();
            if !name.is_empty() {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(!name.is_empty(), "triggered backup never appeared");

    // Restore via the JSON-body node (no query param): the dropped table
    // comes back.
    sql(&web, Some("sekrit"), "DROP TABLE bj").await;
    let body = serde_json::to_string(&json!({
        "file": name,
        "confirm": name,
        "node": node_addr,
    }))
    .unwrap();
    let res = http(
        &web,
        "POST",
        "/api/backup/restore",
        Some("sekrit"),
        Some(&body),
    )
    .await;
    assert_eq!(res.status, 200, "{}", res.text());
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
    let out = sql(&web, Some("sekrit"), "SELECT v FROM bj WHERE id = 1").await;
    assert!(
        out.to_string().contains("json-node"),
        "rows not restored: {out}"
    );
}

/// ?limit= on /api/logs is clamped into [1, 1000]: the newest entry
/// survives a limit of 1, and an oversized (or absent) limit serves the
/// full ring instead of erroring.
#[tokio::test]
async fn logs_limit_param_is_clamped() {
    let (_dir, addr, _node) = start_stack(None, Vec::new()).await;
    sql(&addr, None, "CREATE TABLE lg (id INT)").await;
    sql(&addr, None, "INSERT INTO lg VALUES (1)").await;
    sql(&addr, None, "SELECT id FROM lg").await;

    // Newest first: a limit of 1 keeps exactly the last statement.
    let v = http(&addr, "GET", "/api/logs?limit=1", None, None)
        .await
        .json();
    let query = v["local"]["query"].as_array().unwrap();
    assert_eq!(query.len(), 1, "{v}");
    assert_eq!(query[0]["sql"], "SELECT id FROM lg", "{v}");

    for path in ["/api/logs?limit=100000", "/api/logs"] {
        let v = http(&addr, "GET", path, None, None).await.json();
        let query = v["local"]["query"].as_array().unwrap();
        assert_eq!(query.len(), 3, "{path}: {v}");
        assert_eq!(query[0]["sql"], "SELECT id FROM lg", "{path}: {v}");
        assert_eq!(query[2]["sql"], "CREATE TABLE lg (id INT)", "{path}: {v}");
    }
}

/// Setup input policy over HTTP: username length (counted in characters,
/// not bytes) and the single-char-repeat password rule are rejected with
/// 400, invalid input never counts toward the lockout (a real user may
/// fumble the rules — LOCK_THRESHOLD+1 bad attempts still leave setup
/// open), the stored username is trimmed, and the session cookie carries
/// its hardening flags.
#[tokio::test]
async fn setup_input_policy_trim_and_cookie_flags() {
    let dir = tempfile::tempdir().unwrap();
    let addr = start_web_auth(
        None,
        Vec::new(),
        None,
        Some(
            dir.path()
                .join("console-auth.json")
                .to_string_lossy()
                .into_owned(),
        ),
    )
    .await;

    let long_name = "a".repeat(65);
    let wide_name = "好".repeat(65); // 65 characters, 195 bytes
    let attempts = [
        creds_body("", "long-enough-pw"),
        creds_body(&long_name, "long-enough-pw"),
        creds_body(&wide_name, "long-enough-pw"),
        creds_body("admin", &repeat_pw("a")),
    ];
    for i in 0..(docsql_web::auth::LOCK_THRESHOLD + 1) {
        let r = http(
            &addr,
            "POST",
            "/api/auth/setup",
            None,
            Some(&attempts[i % attempts.len()]),
        )
        .await;
        assert_eq!(r.status, 400, "attempt {i}: {}", r.text());
        assert!(!r.json()["error"].as_str().unwrap().is_empty());
    }

    // Still not locked: validation failures never touch the lockout.
    let r = http(
        &addr,
        "POST",
        "/api/auth/setup",
        None,
        Some(&creds_body(" admin ", &users_test_pw())),
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.text());
    let cookie_hdr = r.header("set-cookie").expect("session cookie").to_string();
    assert!(cookie_hdr.starts_with("docsql_session="), "{cookie_hdr}");
    for flag in ["HttpOnly", "SameSite=Lax", "Path=/", "Max-Age="] {
        assert!(cookie_hdr.contains(flag), "{flag} missing: {cookie_hdr}");
    }

    // The stored username is the trimmed form; logging in with it works.
    let st = http(&addr, "GET", "/api/auth/status", None, None)
        .await
        .json();
    assert_eq!(st["mode"], "login");
    assert_eq!(st["username"], "admin");
    let r = http(
        &addr,
        "POST",
        "/api/auth/login",
        None,
        Some(&creds_body("admin", &users_test_pw())),
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.text());
}

/// Liveness is gate-free by design: orchestrators probe it without
/// credentials, and it must answer even when the console has no managed
/// node configured at all (it never touches a node).
#[tokio::test]
async fn healthz_is_gate_free_liveness() {
    let addr = start_web(Some("sekrit"), Vec::new(), None).await;
    let res = http(&addr, "GET", "/healthz", None, None).await;
    assert_eq!(res.status, 200);
    let body = res.json();
    assert_eq!(body["ok"], true);
    assert_eq!(body["service"], "docsql-web");
    assert!(body["version"].as_str().is_some());

    // With the console account gate active and pre-setup, data endpoints
    // are closed (401) while liveness stays open.
    let dir = tempfile::tempdir().unwrap();
    let gated = start_web_auth(
        None,
        Vec::new(),
        None,
        Some(dir.path().join("creds.json").display().to_string()),
    )
    .await;
    assert_eq!(
        http(&gated, "GET", "/api/stats", None, None).await.status,
        401
    );
    let res = http(&gated, "GET", "/healthz", None, None).await;
    assert_eq!(res.status, 200);
    assert_eq!(res.json()["ok"], true);
}

/// /metrics sits behind the same API gate as every other endpoint, serves
/// Prometheus text scraped from the managed node's REQ_STATUS (now carrying
/// the runtime counters), and counts the console's own HTTP surface with
/// scanner-proof path bucketing.
#[tokio::test]
async fn metrics_endpoint_gates_formats_and_scrapes_nodes() {
    let (_dir, addr, node) = start_stack(Some("sekrit"), Vec::new()).await;

    // Gate: no token / wrong token are refused like any API call.
    assert_eq!(http(&addr, "GET", "/metrics", None, None).await.status, 401);
    assert_eq!(
        http(&addr, "GET", "/metrics", Some("wrong"), None)
            .await
            .status,
        401
    );

    // One statement on the node first, so the SQL counter has work to show.
    sql(
        &addr,
        Some("sekrit"),
        "CREATE TABLE mx (id INT PRIMARY KEY)",
    )
    .await;

    let res = http(&addr, "GET", "/metrics", Some("sekrit"), None).await;
    assert_eq!(res.status, 200, "{}", res.text());
    let ct = res
        .headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-type"))
        .map(|(_, v)| v.clone())
        .unwrap_or_default();
    assert!(ct.starts_with("text/plain"), "content-type: {ct}");
    let body = res.text();

    // The live managed node is scraped through: up, versioned, counters
    // present.
    assert!(
        body.contains(&format!("docsql_node_up{{node=\"{node}\"}} 1")),
        "node up line missing:\n{body}"
    );
    assert!(
        body.contains("docsql_node_info{node=\""),
        "version info missing:\n{body}"
    );
    let stmts = body
        .lines()
        .find_map(|l| {
            l.strip_prefix(&format!("docsql_sql_statements_total{{node=\"{node}\"}} "))
                .and_then(|v| v.trim().parse::<u64>().ok())
        })
        .unwrap_or(0);
    assert!(stmts >= 1, "statements counter missing/zero:\n{body}");
    assert!(
        body.contains(&format!("docsql_network_bytes_total{{node=\"{node}\"}} ")),
        "byte counters missing:\n{body}"
    );

    // The console's own HTTP surface is counted, and unknown paths lump
    // under /other so scanners cannot grow the counter map.
    http(&addr, "GET", "/no-such-path", None, None).await;
    let body = http(&addr, "GET", "/metrics", Some("sekrit"), None)
        .await
        .text();
    assert!(
        body.contains("docsql_web_http_requests_total{"),
        "web counters missing:\n{body}"
    );
    assert!(
        body.contains("path=\"/other\""),
        "unknown paths not bucketed:\n{body}"
    );
}

/// 原生 TLS:配了 PEM 证书的控制台以 HTTPS 服务全 API 面 —— 自签信任链下
/// 客户端握手成功,/healthz 经 TLS 应答;明文 HTTP 落在 TLS 端口上得不到
/// 任何 HTTP 应答(握手被拒,连接关闭)。
#[tokio::test]
async fn tls_listener_serves_https_and_rejects_plaintext() {
    // 自签证书(SAN: localhost)。
    let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let cert = dir.path().join("cert.pem");
    let key = dir.path().join("key.pem");
    std::fs::write(&cert, ck.cert.pem()).unwrap();
    std::fs::write(&key, ck.key_pair.serialize_pem()).unwrap();

    let addr = start_web_full(
        Some("sekrit"),
        Vec::new(),
        None,
        None,
        Some((cert.display().to_string(), key.display().to_string())),
    )
    .await;

    // rustls 客户端:信任自签证书本身(作根)。
    let provider = std::sync::Arc::new(rustls::crypto::ring::default_provider());
    let mut roots = rustls::RootCertStore::empty();
    roots.add(ck.cert.der().clone()).unwrap();
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config));

    let stream = TcpStream::connect(&addr).await.unwrap();
    let mut tls = connector
        .connect("localhost".try_into().unwrap(), stream)
        .await
        .expect("TLS handshake with the trusted self-signed cert");
    tls.write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut buf = Vec::new();
    tls.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf);
    assert!(text.starts_with("HTTP/1.1 200"), "no HTTPS 200: {text}");
    assert!(text.contains("\"ok\":true"), "healthz body missing: {text}");

    // 明文 HTTP 落在 TLS 端口:不得有任何 HTTP 应答(TLS 层拒绝后关闭)。
    let mut plain = TcpStream::connect(&addr).await.unwrap();
    plain
        .write_all(b"GET /healthz HTTP/1.1\r\nHost: x\r\n\r\n")
        .await
        .unwrap();
    plain.flush().await.unwrap();
    let mut raw = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(3), plain.read_to_end(&mut raw)).await;
    let text = String::from_utf8_lossy(&raw);
    assert!(
        !text.contains("HTTP/1.1 200"),
        "plaintext request got an HTTP answer on the TLS port: {text}"
    );
}
