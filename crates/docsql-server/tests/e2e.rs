//! End-to-end tests: real server on a random port, raw TCP client speaking
//! the v1 protocol.

use docsql_core::proto::{self, Frame};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

async fn start_server(token: Option<&str>) -> (tempfile::TempDir, String) {
    start_server_tokens(token, None).await
}

/// Like [`start_server`], with an optional cluster (inter-node) token.
async fn start_server_tokens(
    token: Option<&str>,
    cluster: Option<&str>,
) -> (tempfile::TempDir, String) {
    start_server_sec(token, None, cluster, 0, 0, 10).await
}

/// Full-control server start for the security-surface tests: optional
/// client / read-only / cluster tokens, connection cap, idle timeout, and
/// auth-lockout threshold (0 disables lockout).
async fn start_server_sec(
    token: Option<&str>,
    read: Option<&str>,
    cluster: Option<&str>,
    max_conn: usize,
    idle_timeout_secs: u64,
    auth_lock_threshold: u32,
) -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("e2e.db");
    // Pick a free port by binding a listener first.
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    let addr = format!("127.0.0.1:{port}");
    let cfg = docsql_server::ServerConfig {
        db_path: db,
        listen: addr.clone(),
        auth_token: token.map(String::from),
        read_token: read.map(String::from),
        max_conn,
        idle_timeout_secs,
        auth_lock_threshold,
        cluster_token: cluster.map(String::from),
        replicate_to: None,
        peers: Vec::new(),
        advertise: None,
        read_only: false,
        transport_key: None,
        async_commit: false,
    };
    tokio::spawn(docsql_server::run(cfg));
    // Wait for the port to accept.
    for _ in 0..100 {
        if TcpStream::connect(&addr).await.is_ok() {
            return (dir, addr);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("server did not come up");
}

/// [`start_server`] with group commit enabled (DOCSQL_ASYNC_COMMIT=1): the
/// background flusher owns WAL fsyncs, so every statement commit is deferred.
async fn start_server_async_commit() -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("e2e.db");
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    let addr = format!("127.0.0.1:{port}");
    let cfg = docsql_server::ServerConfig {
        db_path: db,
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
        async_commit: true,
    };
    tokio::spawn(docsql_server::run(cfg));
    for _ in 0..100 {
        if TcpStream::connect(&addr).await.is_ok() {
            return (dir, addr);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("server did not come up");
}

struct Client {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Client {
    async fn connect(addr: &str) -> Client {
        Client {
            stream: TcpStream::connect(addr).await.unwrap(),
            buf: Vec::new(),
        }
    }

    async fn send(&mut self, f: &Frame) {
        let bytes = f.encode().unwrap();
        self.stream.write_all(&bytes).await.unwrap();
        self.stream.flush().await.unwrap();
    }

    async fn recv(&mut self) -> Frame {
        loop {
            if let Ok((f, n)) = Frame::decode(&self.buf) {
                self.buf.drain(..n);
                return f;
            }
            let mut chunk = [0u8; 8192];
            let n = self.stream.read(&mut chunk).await.unwrap();
            assert!(n > 0, "connection closed");
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }

    async fn sql(&mut self, sql: &str) -> Frame {
        self.send(&Frame::new(proto::REQ_SQL, proto::encode_sql(sql).unwrap()))
            .await;
        self.recv().await
    }

    async fn auth(&mut self, token: &str) -> Frame {
        self.send(&Frame::new(proto::REQ_AUTH, token.as_bytes().to_vec()))
            .await;
        self.recv().await
    }

    async fn promote(&mut self) -> Frame {
        self.send(&Frame::new(proto::REQ_PROMOTE, vec![])).await;
        self.recv().await
    }

    async fn ping(&mut self) -> Frame {
        self.send(&Frame::new(proto::REQ_PING, vec![])).await;
        self.recv().await
    }

    async fn subscribe(&mut self, channel: &str, from: &str) -> Frame {
        let payload = format!(r#"{{"channel":"{channel}","from":"{from}"}}"#);
        self.send(&Frame::new(proto::REQ_SUBSCRIBE, payload.into_bytes()))
            .await;
        self.recv().await
    }

    async fn psubscribe(&mut self, pattern: &str, from: &str) -> Frame {
        let payload = format!(r#"{{"pattern":"{pattern}","from":"{from}"}}"#);
        self.send(&Frame::new(proto::REQ_PSUBSCRIBE, payload.into_bytes()))
            .await;
        self.recv().await
    }

    async fn unsubscribe(&mut self, names: &str) -> Frame {
        self.send(&Frame::new(
            proto::REQ_UNSUBSCRIBE,
            names.as_bytes().to_vec(),
        ))
        .await;
        self.recv().await
    }

    async fn punsubscribe(&mut self, patterns: &str) -> Frame {
        self.send(&Frame::new(
            proto::REQ_PUNSUBSCRIBE,
            patterns.as_bytes().to_vec(),
        ))
        .await;
        self.recv().await
    }

    async fn publish(&mut self, channel: &str, payload: &str) -> Frame {
        let body = format!(r#"{{"channel":"{channel}","payload":"{payload}"}}"#);
        self.send(&Frame::new(proto::REQ_PUBLISH, body.into_bytes()))
            .await;
        self.recv().await
    }

    async fn pubsub_cmd(&mut self, body: &str) -> Frame {
        self.send(&Frame::new(proto::REQ_PUBSUB, body.as_bytes().to_vec()))
            .await;
        self.recv().await
    }
}

fn affected_u64(f: &Frame) -> u64 {
    f.payload
        .get(..8)
        .and_then(|s| s.try_into().ok())
        .map_or(0, u64::from_le_bytes)
}

/// Receive one frame, or None after `ms` (asserting push absence).
async fn recv_timeout(c: &mut Client, ms: u64) -> Option<Frame> {
    tokio::time::timeout(Duration::from_millis(ms), c.recv())
        .await
        .ok()
}

fn payload_str(f: &Frame) -> String {
    String::from_utf8_lossy(&f.payload).into_owned()
}

#[tokio::test]
async fn sql_roundtrip_over_wire() {
    let (_dir, addr) = start_server(None).await;
    let mut c = Client::connect(&addr).await;
    let r = c.sql("CREATE TABLE t (a INT, b TEXT)").await;
    assert_eq!(r.frame_type, proto::RESP_AFFECTED);
    c.sql("INSERT INTO t VALUES (1, 'x'), (2, 'y')").await;
    let r = c.sql("SELECT a, b FROM t ORDER BY a").await;
    assert_eq!(r.frame_type, proto::RESP_ROWS);
    let v = docsql_core::json::from_str(&payload_str(&r)).unwrap();
    let expected = docsql_core::value::Value::Object(docsql_core::value::Object::from([
        (
            "columns".into(),
            docsql_core::value::Value::Array(
                vec!["a".into(), "b".into()]
                    .into_iter()
                    .map(docsql_core::value::Value::Str)
                    .collect(),
            ),
        ),
        (
            "rows".into(),
            docsql_core::value::Value::Array(vec![
                docsql_core::value::Value::Array(vec![
                    docsql_core::Value::Int(1),
                    docsql_core::Value::Str("x".into()),
                ]),
                docsql_core::value::Value::Array(vec![
                    docsql_core::Value::Int(2),
                    docsql_core::Value::Str("y".into()),
                ]),
            ]),
        ),
    ]));
    assert_eq!(v, expected);
}

#[tokio::test]
async fn sql_join_groupby_over_wire() {
    let (_dir, addr) = start_server(None).await;
    let mut c = Client::connect(&addr).await;
    c.sql("CREATE TABLE users (id INT, name TEXT)").await;
    c.sql("CREATE TABLE orders (oid INT, uid INT, amount INT)")
        .await;
    c.sql("INSERT INTO users VALUES (1, 'ann'), (2, 'bob'), (3, 'zed')")
        .await;
    c.sql("INSERT INTO orders VALUES (10, 1, 5), (11, 1, 7), (12, 2, 3)")
        .await;
    // LEFT JOIN + GROUP BY + aggregate + ORDER BY, end to end.
    let r = c
        .sql(
            "SELECT u.name, COUNT(o.oid) AS n, SUM(o.amount) AS total \
             FROM users u LEFT JOIN orders o ON u.id = o.uid \
             GROUP BY u.name ORDER BY u.name",
        )
        .await;
    assert_eq!(r.frame_type, proto::RESP_ROWS, "{}", payload_str(&r));
    let body = payload_str(&r);
    assert!(body.contains("[\"u.name\",\"n\",\"total\"]"), "{body}");
    assert!(body.contains("[\"ann\",2,12]"), "{body}");
    assert!(body.contains("[\"bob\",1,3]"), "{body}");
    assert!(body.contains("[\"zed\",0,null]"), "{body}");
}

#[tokio::test]
async fn auth_gate() {
    let (_dir, addr) = start_server(Some("s3cret")).await;
    let mut c = Client::connect(&addr).await;
    // SQL before AUTH is rejected.
    let r = c.sql("SELECT 1").await;
    assert_eq!(r.frame_type, proto::RESP_ERROR);
    // Wrong token rejected.
    let r = c.auth("wrong").await;
    assert_eq!(r.frame_type, proto::RESP_ERROR);
    // Right token unlocks the session (SQL included).
    let r = c.auth("s3cret").await;
    assert_eq!(payload_str(&r), "ok");
    let r = c.sql("SELECT 1").await;
    assert_eq!(r.frame_type, proto::RESP_ROWS);
}

/// Identity-authentication failure handling: repeated wrong tokens from one
/// source lock that source out — even the correct token is refused until
/// the lockout elapses, and the lockout event lands in the audit trail.
#[tokio::test]
async fn auth_failures_lock_the_source() {
    let (_dir, addr) = start_server_sec(Some("s3cret-long"), None, None, 0, 0, 3).await;
    // Three wrong attempts exhaust the threshold.
    for _ in 0..3 {
        let mut c = Client::connect(&addr).await;
        assert_eq!(c.auth("wrong").await.frame_type, proto::RESP_ERROR);
    }
    // Locked: even the correct token is rejected unread.
    let mut c = Client::connect(&addr).await;
    let r = c.auth("s3cret-long").await;
    assert_eq!(r.frame_type, proto::RESP_ERROR);
    assert!(payload_str(&r).contains("locked"), "{}", payload_str(&r));
    // Other frames are unaffected (the lockout gates AUTH, not the port).
    let r = c.ping().await;
    assert_eq!(r.frame_type, proto::RESP_PONG);
}

/// Least-privilege access control: a read-only-token connection may read
/// and subscribe; every durable write is refused. A full client token on
/// the same server still writes.
#[tokio::test]
async fn read_only_token_least_privilege() {
    let (_dir, addr) =
        start_server_sec(Some("writer-token"), Some("reader-token"), None, 0, 0, 10).await;
    // Schema arrives via the privileged connection.
    let mut w = Client::connect(&addr).await;
    assert_eq!(
        w.auth("writer-token").await.frame_type,
        proto::RESP_AFFECTED
    );
    assert_eq!(
        w.sql("CREATE TABLE ro (id INT, v TEXT)").await.frame_type,
        proto::RESP_AFFECTED
    );

    let mut r = Client::connect(&addr).await;
    let ack = r.auth("reader-token").await;
    assert_eq!(ack.frame_type, proto::RESP_AFFECTED);
    assert_eq!(payload_str(&ack), "ok(read-only)");
    // Reads pass...
    assert_eq!(r.sql("SELECT 1").await.frame_type, proto::RESP_ROWS);
    assert_eq!(r.sql("SELECT * FROM ro").await.frame_type, proto::RESP_ROWS);
    // ...subscriptions pass (session state, not durable writes)...
    assert_eq!(
        r.subscribe("events", "latest").await.frame_type,
        proto::RESP_AFFECTED
    );
    // ...and every write is refused with one clear error.
    for sql in [
        "CREATE TABLE t2 (a INT)",
        "INSERT INTO ro VALUES (1, 'x')",
        "UPDATE ro SET v = 'y'",
        "DELETE FROM ro",
        "DROP TABLE ro",
    ] {
        let resp = r.sql(sql).await;
        assert_eq!(resp.frame_type, proto::RESP_ERROR, "{sql}");
        assert!(
            payload_str(&resp).contains("read-only"),
            "{sql}: {}",
            payload_str(&resp)
        );
    }
    let body = r#"{"channel":"events","payload":"x"}"#;
    r.send(&Frame::new(proto::REQ_PUBLISH, body.as_bytes().to_vec()))
        .await;
    let resp = r.recv().await;
    assert_eq!(resp.frame_type, proto::RESP_ERROR);
    assert!(payload_str(&resp).contains("read-only"));
    // The privileged client still publishes and writes.
    assert_eq!(
        w.publish("events", "hello").await.frame_type,
        proto::RESP_ROWS
    );
    assert_eq!(
        w.sql("INSERT INTO ro VALUES (1, 'x')").await.frame_type,
        proto::RESP_AFFECTED
    );
}

/// Resource control: past DOCSQL_MAX_CONN the server answers new
/// connections with an error instead of admitting them.
#[tokio::test]
async fn max_conn_limits_concurrent_connections() {
    let (_dir, addr) = start_server_sec(Some("s3cret-long"), None, None, 1, 0, 10).await;
    // The start helper's port probe briefly holds the single slot; retry
    // until the probe's connection has drained.
    let mut first = loop {
        let mut c = Client::connect(&addr).await;
        if c.auth("s3cret-long").await.frame_type == proto::RESP_AFFECTED {
            break c;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    // The next connection is refused at the door.
    let mut second = Client::connect(&addr).await;
    let r = second.recv().await;
    assert_eq!(r.frame_type, proto::RESP_ERROR);
    assert!(
        payload_str(&r).contains("too many connections"),
        "{}",
        payload_str(&r)
    );
    // The first connection is unaffected.
    assert_eq!(first.ping().await.frame_type, proto::RESP_PONG);
}

/// Session timeout: a connection silent past DOCSQL_IDLE_TIMEOUT is closed
/// by the server with a explanatory error frame.
#[tokio::test]
async fn idle_timeout_closes_silent_connections() {
    let (_dir, addr) = start_server_sec(Some("s3cret-long"), None, None, 0, 1, 10).await;
    let mut c = Client::connect(&addr).await;
    assert_eq!(c.auth("s3cret-long").await.frame_type, proto::RESP_AFFECTED);
    // Stay silent past the 1s window; the server must terminate the idle
    // session (bounded wait: either the error frame arrives or the socket
    // closes with data still buffered).
    let got = recv_timeout(&mut c, 5000).await;
    let r = got.expect("server did not close the idle connection");
    assert_eq!(r.frame_type, proto::RESP_ERROR);
    assert!(payload_str(&r).contains("idle"), "{}", payload_str(&r));
}

/// Node identity: with a cluster token configured, FLAG_REPLICATION frames
/// are node-to-node traffic — a client-token connection cannot forge them
/// (forged frames could otherwise bypass read-only replicas and suppress
/// fan-out).
#[tokio::test]
async fn cluster_token_gates_replication_frames() {
    let (_dir, addr) = start_server_tokens(Some("client-tok"), Some("cluster-tok")).await;
    let mut c = Client::connect(&addr).await;
    assert_eq!(c.auth("client-tok").await.frame_type, proto::RESP_AFFECTED);
    let mut f = Frame::new(
        proto::REQ_SQL,
        proto::encode_sql("CREATE TABLE t (a INT)").unwrap(),
    );
    f.flags = docsql_server::FLAG_REPLICATION;
    c.send(&f).await;
    let r = c.recv().await;
    assert_eq!(r.frame_type, proto::RESP_ERROR);
    assert!(
        payload_str(&r).contains("cluster auth"),
        "{}",
        payload_str(&r)
    );
    // Without the flag the same statement is an ordinary client write.
    let r = c.sql("CREATE TABLE t (a INT)").await;
    assert_eq!(r.frame_type, proto::RESP_AFFECTED);
}

/// A cluster-token connection authenticates as a peer node: replication
/// frames apply, ordinary client traffic is refused (nodes talk to nodes,
/// not to the SQL surface).
#[tokio::test]
async fn cluster_token_connection_is_peer_only() {
    let (_dir, addr) = start_server_tokens(Some("client-tok"), Some("cluster-tok")).await;
    let mut c = Client::connect(&addr).await;
    let r = c.auth("cluster-tok").await;
    assert_eq!(payload_str(&r), "ok");
    // PING stays available (liveness, no data).
    assert_eq!(c.ping().await.frame_type, proto::RESP_PONG);
    // Client statements are not peer traffic.
    let r = c.sql("SELECT 1").await;
    assert_eq!(r.frame_type, proto::RESP_ERROR);
    assert!(
        payload_str(&r).contains("only carry replication"),
        "{}",
        payload_str(&r)
    );
    // Replication-flagged SQL applies (the fan-out shape).
    let mut f = Frame::new(
        proto::REQ_SQL,
        proto::encode_sql("CREATE TABLE t (a INT)").unwrap(),
    );
    f.flags = docsql_server::FLAG_REPLICATION;
    c.send(&f).await;
    assert_eq!(c.recv().await.frame_type, proto::RESP_AFFECTED);
}

/// Without a cluster token the historical behavior holds: any
/// authenticated connection may send replication frames.
#[tokio::test]
async fn replication_frames_open_without_cluster_token() {
    let (_dir, addr) = start_server_tokens(Some("client-tok"), None).await;
    let mut c = Client::connect(&addr).await;
    assert_eq!(c.auth("client-tok").await.frame_type, proto::RESP_AFFECTED);
    let mut f = Frame::new(
        proto::REQ_SQL,
        proto::encode_sql("CREATE TABLE t (a INT)").unwrap(),
    );
    f.flags = docsql_server::FLAG_REPLICATION;
    c.send(&f).await;
    assert_eq!(c.recv().await.frame_type, proto::RESP_AFFECTED);
}

/// Cluster-token-only deployment (no client token): clients connect
/// freely, nodes authenticate to each other with the cluster token,
/// replication converges both ways — and the replication gate is armed
/// even though client auth is off.
#[tokio::test]
async fn fanout_authenticates_with_cluster_token() {
    let dir = tempfile::tempdir().unwrap();
    let free = || {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        format!("127.0.0.1:{p}")
    };
    let a_addr = free();
    let b_addr = free();
    let cfg_for = |listen: &str, peers: Vec<String>, db: &str| docsql_server::ServerConfig {
        db_path: dir.path().join(db),
        listen: listen.to_string(),
        auth_token: None,
        read_token: None,
        max_conn: 0,
        idle_timeout_secs: 0,
        auth_lock_threshold: 10,
        cluster_token: Some("cluster-tok".into()),
        advertise: None,
        replicate_to: None,
        peers,
        read_only: false,
        transport_key: None,
        async_commit: false,
    };
    tokio::spawn(docsql_server::run(cfg_for(
        &a_addr,
        vec![b_addr.clone()],
        "fa.db",
    )));
    tokio::spawn(docsql_server::run(cfg_for(
        &b_addr,
        vec![a_addr.clone()],
        "fb.db",
    )));
    for addr in [&a_addr, &b_addr] {
        for _ in 0..100 {
            if TcpStream::connect(addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    // A client write on a fans out to b under the cluster token.
    let mut a = Client::connect(&a_addr).await;
    a.sql("CREATE TABLE fan (id INT PRIMARY KEY)").await;
    a.sql("INSERT INTO fan VALUES (1)").await;
    assert!(
        wait_seen(&b_addr, "SELECT id FROM fan WHERE id = 1", "[[1]]").await,
        "cluster-token fan-out did not converge"
    );

    // b's own write reaches a the same way.
    let mut b = Client::connect(&b_addr).await;
    b.sql("INSERT INTO fan VALUES (2)").await;
    assert!(
        wait_seen(&a_addr, "SELECT id FROM fan WHERE id = 2", "[[2]]").await,
        "reverse fan-out did not converge"
    );

    // Even with client auth off, clients cannot forge replication frames.
    let mut f = Frame::new(
        proto::REQ_SQL,
        proto::encode_sql("INSERT INTO fan VALUES (3)").unwrap(),
    );
    f.flags = docsql_server::FLAG_REPLICATION;
    b.send(&f).await;
    let r = b.recv().await;
    assert_eq!(r.frame_type, proto::RESP_ERROR);
    assert!(
        payload_str(&r).contains("cluster auth"),
        "{}",
        payload_str(&r)
    );
}

#[tokio::test]
async fn default_fill_converges_across_peers() {
    // Red line 11, default flavor: peers replay the fanout INSERT text, so
    // DEFAULT fills must be deterministic. Plain defaults are re-filled
    // identically on every peer; auto-GUID inserts ride the canonical
    // resolved-INSERT rewrite that pins the default-filled columns too.
    let dir = tempfile::tempdir().unwrap();
    let free = || {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        format!("127.0.0.1:{p}")
    };
    let a_addr = free();
    let b_addr = free();
    let cfg_for = |listen: &str, peers: Vec<String>, db: &str| docsql_server::ServerConfig {
        db_path: dir.path().join(db),
        listen: listen.to_string(),
        auth_token: None,
        read_token: None,
        max_conn: 0,
        idle_timeout_secs: 0,
        auth_lock_threshold: 10,
        cluster_token: Some("cluster-tok".into()),
        advertise: None,
        replicate_to: None,
        peers,
        read_only: false,
        transport_key: None,
        async_commit: false,
    };
    tokio::spawn(docsql_server::run(cfg_for(
        &a_addr,
        vec![b_addr.clone()],
        "dfla.db",
    )));
    tokio::spawn(docsql_server::run(cfg_for(
        &b_addr,
        vec![a_addr.clone()],
        "dflb.db",
    )));
    for addr in [&a_addr, &b_addr] {
        for _ in 0..100 {
            if TcpStream::connect(addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    let mut a = Client::connect(&a_addr).await;
    a.sql("CREATE TABLE pl (id INT PRIMARY KEY, tag TEXT DEFAULT 'seed')")
        .await;
    a.sql(
        "CREATE TABLE g (id GUID PRIMARY KEY AUTOINCREMENT, tag TEXT DEFAULT 'seed', n INT NOT NULL DEFAULT 3)",
    )
    .await;
    // Plain default: both nodes fill the same constant.
    a.sql("INSERT INTO pl (id) VALUES (1)").await;
    assert!(
        wait_seen(&b_addr, "SELECT tag FROM pl WHERE id = 1", "[[\"seed\"]]").await,
        "plain default fill did not converge"
    );
    // GUID + defaults: the source's resolved INSERT must reach b with the
    // generated id and filled defaults pinned explicitly.
    a.sql("INSERT INTO g (id) VALUES (NULL)").await;
    assert!(
        wait_seen(&b_addr, "SELECT tag, n FROM g", "[[\"seed\",3]]").await,
        "GUID + default fill did not converge"
    );
    assert!(
        wait_seen(&a_addr, "SELECT COUNT(*) FROM g", "[[1]]").await,
        "guid insert did not land once on a"
    );
}

#[tokio::test]
async fn sql_error_reaches_client() {
    let (_dir, addr) = start_server(None).await;
    let mut c = Client::connect(&addr).await;
    let r = c.sql("SELECT * FROM missing").await;
    assert_eq!(r.frame_type, proto::RESP_ERROR);
    assert!(payload_str(&r).contains("does not exist"));
    // Connection stays usable afterwards (protocol-level ping).
    let r = c.ping().await;
    assert_eq!(r.frame_type, proto::RESP_PONG);
}

/// Statements longer than 512 chars must execute untruncated. The REQ_SQL
/// handler used to cut the text to 512 chars before execution (the query
/// log's display cap leaking into the execute path), silently garbling
/// document INSERTs — exactly what the web console's remote node switching
/// sends. The log's own copy stays capped (see query_log tests).
#[tokio::test]
async fn long_statements_execute_untruncated() {
    let (_dir, addr) = start_server(None).await;
    let mut c = Client::connect(&addr).await;
    let r = c
        .sql("CREATE TABLE longs (id INT PRIMARY KEY, v TEXT)")
        .await;
    assert_eq!(r.frame_type, proto::RESP_AFFECTED);

    let long = "x".repeat(600);
    let r = c
        .sql(&format!("INSERT INTO longs VALUES (1, '{long}')"))
        .await;
    assert_eq!(r.frame_type, proto::RESP_AFFECTED);

    let r = c.sql("SELECT v FROM longs WHERE id = 1").await;
    assert_eq!(r.frame_type, proto::RESP_ROWS);
    let rows = docsql_core::json::from_str(&payload_str(&r)).unwrap();
    if let docsql_core::Value::Object(o) = rows {
        if let Some(docsql_core::Value::Array(rs)) = o.get("rows") {
            assert_eq!(rs.len(), 1);
            let expected = docsql_core::Value::Str(long);
            assert_eq!(rs[0], docsql_core::Value::Array(vec![expected]));
            return;
        }
    }
    panic!("unexpected rows payload");
}

/// REQ_META: the object-explorer payload the web console's node switching
/// fetches — same walk (core::meta) as the console's local /api/meta, so
/// the shapes match field for field.
#[tokio::test]
async fn meta_frame_reports_catalog_for_console() {
    let (_dir, addr) = start_server(None).await;
    let mut c = Client::connect(&addr).await;
    let r = c.sql("CREATE TABLE mt (id INT PRIMARY KEY, v TEXT)").await;
    assert_eq!(r.frame_type, proto::RESP_AFFECTED);
    let r = c.sql("INSERT INTO mt VALUES (1, 'a')").await;
    assert_eq!(r.frame_type, proto::RESP_AFFECTED);

    c.send(&Frame::new(proto::REQ_META, vec![])).await;
    let r = c.recv().await;
    assert_eq!(r.frame_type, proto::RESP_META);
    let v: serde_json::Value = serde_json::from_slice(&r.payload).unwrap();
    assert_eq!(v["totals"]["tables"], 1);
    assert_eq!(v["tables"][0]["name"], "mt");
    assert_eq!(v["tables"][0]["row_count"], 1);
    assert!(!v["tables"][0]["columns"].as_array().unwrap().is_empty());
    assert!(!v["tables"][0]["index_defs"].as_array().unwrap().is_empty());
    assert!(v["storage"]["page_size"].as_u64().is_some());
    assert!(v["server"]["version"].as_str().is_some());
}

/// The engine allows one global transaction: a BEGIN from a second
/// connection queues behind the first connection's open transaction instead
/// of erroring (concurrent EF SaveChanges, one connection per context,
/// relies on this).
#[tokio::test]
async fn begin_on_second_connection_queues_behind_open_transaction() {
    let (_dir, addr) = start_server(None).await;
    let mut a = Client::connect(&addr).await;
    let mut b = Client::connect(&addr).await;
    let r = a.sql("CREATE TABLE q (id INT PRIMARY KEY)").await;
    assert_eq!(r.frame_type, proto::RESP_AFFECTED);
    let r = a.sql("BEGIN").await;
    assert_eq!(r.frame_type, proto::RESP_AFFECTED);
    a.sql("INSERT INTO q VALUES (1)").await;

    // B's BEGIN queues while A's transaction is open; the response must only
    // arrive (successfully) once A has committed.
    b.send(&Frame::new(
        proto::REQ_SQL,
        proto::encode_sql("BEGIN").unwrap(),
    ))
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let r = a.sql("COMMIT").await;
    assert_eq!(r.frame_type, proto::RESP_AFFECTED);
    let r = b.recv().await;
    assert_eq!(
        r.frame_type,
        proto::RESP_AFFECTED,
        "queued BEGIN must succeed once the blocking transaction commits"
    );

    b.sql("INSERT INTO q VALUES (2)").await;
    let r = b.sql("COMMIT").await;
    assert_eq!(r.frame_type, proto::RESP_AFFECTED);
    let r = a.sql("SELECT COUNT(*) FROM q").await;
    assert_eq!(r.frame_type, proto::RESP_ROWS);
    assert!(payload_str(&r).contains("[[2]]"), "{}", payload_str(&r));
}

/// A write from a connection that does not own the open transaction must
/// queue behind it in autocommit, not merge into it — the owner's ROLLBACK
/// used to silently drop writes the other connection had seen succeed.
#[tokio::test]
async fn foreign_write_survives_owners_rollback() {
    let (_dir, addr) = start_server(None).await;
    let mut a = Client::connect(&addr).await;
    let mut b = Client::connect(&addr).await;
    let r = a.sql("CREATE TABLE fw (id INT PRIMARY KEY)").await;
    assert_eq!(r.frame_type, proto::RESP_AFFECTED);
    let r = a.sql("BEGIN").await;
    assert_eq!(r.frame_type, proto::RESP_AFFECTED);
    a.sql("INSERT INTO fw VALUES (1)").await; // owner's write: rolled back

    // B's autocommit write queues while A's transaction is open...
    b.send(&Frame::new(
        proto::REQ_SQL,
        proto::encode_sql("INSERT INTO fw VALUES (2)").unwrap(),
    ))
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let r = a.sql("ROLLBACK").await;
    assert_eq!(r.frame_type, proto::RESP_AFFECTED);
    // ...and lands once the transaction closes: only A's own write is gone.
    let r = b.recv().await;
    assert_eq!(
        r.frame_type,
        proto::RESP_AFFECTED,
        "queued autocommit write must apply after the owner rolls back"
    );
    let r = a.sql("SELECT COUNT(*) FROM fw").await;
    assert!(payload_str(&r).contains("[[1]]"), "{}", payload_str(&r));
}

/// A replicated write arriving while the target's client transaction is
/// open must wait for it to end instead of joining it: the client's
/// ROLLBACK must not undo a write the origin already acknowledged.
#[tokio::test]
async fn replicated_write_waits_out_open_client_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let free = || {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        format!("127.0.0.1:{p}")
    };
    let (a, b) = (free(), free());
    // a fans every write out to b; b never fans back (one-way is enough).
    spawn_node(&dir, "rwa", &a, vec![b.clone()], None).await;
    spawn_node(&dir, "rwb", &b, Vec::new(), None).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut ca = Client::connect(&a).await;
    let mut cb = Client::connect(&b).await;
    ca.sql("CREATE TABLE rw (id INT PRIMARY KEY)").await;
    assert!(
        wait_seen_n(&b, "SELECT COUNT(id) FROM rw", "[[0]]", 500).await,
        "DDL did not replicate before the test proper"
    );

    // b's client opens a transaction; a's write replicates in behind it.
    let r = cb.sql("BEGIN").await;
    assert_eq!(r.frame_type, proto::RESP_AFFECTED);
    ca.send(&Frame::new(
        proto::REQ_SQL,
        proto::encode_sql("INSERT INTO rw VALUES (7)").unwrap(),
    ))
    .await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let r = cb.sql("ROLLBACK").await;
    assert_eq!(r.frame_type, proto::RESP_AFFECTED);
    let r = ca.recv().await;
    assert_eq!(
        r.frame_type,
        proto::RESP_AFFECTED,
        "origin's write must be acknowledged once the target's transaction closes"
    );
    assert!(
        wait_seen_n(&b, "SELECT COUNT(id) FROM rw", "[[1]]", 500).await,
        "replicated write was lost to the client's open transaction"
    );
}

/// Replication + failover: writes on the primary appear on the replica;
/// killing the primary and promoting the replica restores write capability.
#[tokio::test]
async fn replication_and_failover() {
    let dir = tempfile::tempdir().unwrap();
    let free = || {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        format!("127.0.0.1:{p}")
    };
    let replica_addr = free();
    let primary_addr = free();

    // Replica: read-only, no upstream.
    tokio::spawn(docsql_server::run(docsql_server::ServerConfig {
        db_path: dir.path().join("replica.db"),
        listen: replica_addr.clone(),
        auth_token: None,
        read_token: None,
        max_conn: 0,
        idle_timeout_secs: 0,
        auth_lock_threshold: 10,
        cluster_token: None,
        replicate_to: None,
        peers: Vec::new(),
        advertise: None,
        read_only: true,
        transport_key: None,
        async_commit: false,
    }));
    // Primary: forwards writes to the replica.
    tokio::spawn(docsql_server::run(docsql_server::ServerConfig {
        db_path: dir.path().join("primary.db"),
        listen: primary_addr.clone(),
        auth_token: None,
        read_token: None,
        max_conn: 0,
        idle_timeout_secs: 0,
        auth_lock_threshold: 10,
        cluster_token: None,
        replicate_to: Some(replica_addr.clone()),
        peers: Vec::new(),
        advertise: None,
        read_only: false,
        transport_key: None,
        async_commit: false,
    }));
    for addr in [&primary_addr, &replica_addr] {
        for _ in 0..100 {
            if TcpStream::connect(addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    // Write on the primary.
    let mut p = Client::connect(&primary_addr).await;
    p.sql("CREATE TABLE fail (id INT)").await;
    p.sql("INSERT INTO fail VALUES (7)").await;

    // The replica sees it (async forwarding — poll briefly).
    let mut r = Client::connect(&replica_addr).await;
    let mut seen = false;
    for _ in 0..50 {
        let resp = r.sql("SELECT id FROM fail").await;
        if resp.frame_type == proto::RESP_ROWS
            && String::from_utf8_lossy(&resp.payload).contains("\"rows\":[[7]]")
        {
            seen = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(seen, "replica did not observe the replicated write");

    // Replica rejects client writes before promotion.
    let resp = r.sql("INSERT INTO fail VALUES (8)").await;
    assert_eq!(resp.frame_type, proto::RESP_ERROR);
    assert!(payload_str(&resp).contains("read-only"));

    // Failover: promote the replica (REQ_PROMOTE); writes now succeed.
    let resp = r.promote().await;
    assert_ne!(resp.frame_type, proto::RESP_ERROR, "PROMOTE failed");
    let resp = r.sql("INSERT INTO fail VALUES (8)").await;
    assert_eq!(
        resp.frame_type,
        proto::RESP_AFFECTED,
        "promoted insert failed: {}",
        payload_str(&resp)
    );
    let resp = r.sql("SELECT COUNT(id) FROM fail").await;
    assert!(
        payload_str(&resp).contains("[[2]]"),
        "got {}",
        payload_str(&resp)
    );
}

/// Poll a node until the probe response payload contains `needle`.
async fn wait_seen(addr: &str, probe: &str, needle: &str) -> bool {
    for _ in 0..50 {
        let mut c = Client::connect(addr).await;
        let resp = c.sql(probe).await;
        let text = String::from_utf8_lossy(&resp.payload).to_string();
        if resp.frame_type != proto::RESP_ERROR && text.contains(needle) {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    false
}

/// Symmetric three-node cluster: no primary/replica roles — any node accepts
/// writes and fans them out to its peers.
#[tokio::test]
async fn symmetric_cluster_writes_on_any_node_visible_everywhere() {
    let dir = tempfile::tempdir().unwrap();
    let free = || {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        format!("127.0.0.1:{p}")
    };
    let addrs = vec![free(), free(), free()];
    for (i, addr) in addrs.iter().enumerate() {
        let peers = addrs
            .iter()
            .enumerate()
            .filter(|(j, _)| *j != i)
            .map(|(_, a)| a.clone())
            .collect::<Vec<_>>()
            .join(",");
        tokio::spawn(docsql_server::run(docsql_server::ServerConfig {
            db_path: dir.path().join(format!("peer{i}.db")),
            listen: addr.clone(),
            auth_token: None,
            read_token: None,
            max_conn: 0,
            idle_timeout_secs: 0,
            auth_lock_threshold: 10,
            cluster_token: None,
            replicate_to: None,
            peers: peers.split(',').map(String::from).collect(),
            advertise: None,
            read_only: false,
            transport_key: None,
            async_commit: false,
        }));
    }
    for addr in &addrs {
        for _ in 0..100 {
            if TcpStream::connect(addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    // Write through node 0 and node 1; read from node 2.
    let mut a = Client::connect(&addrs[0]).await;
    a.sql("CREATE TABLE sym (id INT, src INT)").await;
    a.sql("INSERT INTO sym VALUES (1, 0)").await;
    let mut b = Client::connect(&addrs[1]).await;
    b.sql("INSERT INTO sym VALUES (2, 1)").await;

    assert!(
        wait_seen(&addrs[2], "SELECT src FROM sym ORDER BY src", "[[0],[1]]").await,
        "node2 did not see both SQL writes"
    );

    // Any node also accepts writes (no read-only role anywhere).
    let mut c = Client::connect(&addrs[2]).await;
    let resp = c.sql("INSERT INTO sym VALUES (3, 2)").await;
    assert_eq!(resp.frame_type, proto::RESP_AFFECTED);
    assert!(
        wait_seen(&addrs[0], "SELECT COUNT(id) FROM sym", "[[3]]").await,
        "node0 did not see node2's write"
    );
}

/// Two-node symmetric cluster: transaction semantics must hold across
/// replication — a rolled-back write never reaches the peer, committed
/// transaction writes replay in order, and savepoint rollbacks trim exactly
/// the buffered tail.
#[tokio::test]
async fn symmetric_cluster_transaction_writes_replicate_only_on_commit() {
    let dir = tempfile::tempdir().unwrap();
    let free = || {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        format!("127.0.0.1:{p}")
    };
    let addrs = vec![free(), free()];
    for (i, addr) in addrs.iter().enumerate() {
        let peers = addrs
            .iter()
            .enumerate()
            .filter(|(j, _)| *j != i)
            .map(|(_, a)| a.clone())
            .collect::<Vec<_>>()
            .join(",");
        tokio::spawn(docsql_server::run(docsql_server::ServerConfig {
            db_path: dir.path().join(format!("txpeer{i}.db")),
            listen: addr.clone(),
            auth_token: None,
            read_token: None,
            max_conn: 0,
            idle_timeout_secs: 0,
            auth_lock_threshold: 10,
            cluster_token: None,
            replicate_to: None,
            peers: peers.split(',').map(String::from).collect(),
            advertise: None,
            read_only: false,
            transport_key: None,
            async_commit: false,
        }));
    }
    for addr in &addrs {
        for _ in 0..100 {
            if TcpStream::connect(addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    let mut a = Client::connect(&addrs[0]).await;
    a.sql("CREATE TABLE txr (id INT)").await;
    a.sql("INSERT INTO txr VALUES (1)").await;
    assert!(
        wait_seen(&addrs[1], "SELECT id FROM txr", "[[1]]").await,
        "autocommit write did not replicate"
    );

    // Rolled-back transaction: the peer must never observe id=2.
    a.sql("BEGIN").await;
    a.sql("INSERT INTO txr VALUES (2)").await;
    a.sql("ROLLBACK").await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let mut b = Client::connect(&addrs[1]).await;
    let resp = b.sql("SELECT id FROM txr WHERE id = 2").await;
    assert_eq!(resp.frame_type, proto::RESP_ROWS);
    assert!(
        payload_str(&resp).contains("\"rows\":[]"),
        "peer observed a rolled-back write: {}",
        payload_str(&resp)
    );

    // Savepoints: writes past ROLLBACK TO SAVEPOINT stay local-only; the
    // rest replays in order on commit.
    a.sql("BEGIN").await;
    a.sql("INSERT INTO txr VALUES (3)").await;
    a.sql("SAVEPOINT sp").await;
    a.sql("INSERT INTO txr VALUES (4)").await;
    a.sql("ROLLBACK TO SAVEPOINT sp").await;
    a.sql("INSERT INTO txr VALUES (5)").await;
    let resp = a.sql("COMMIT").await;
    assert_ne!(resp.frame_type, proto::RESP_ERROR);
    assert!(
        wait_seen(&addrs[1], "SELECT id FROM txr ORDER BY id", "[[1],[3],[5]]").await,
        "committed transaction did not replicate (or savepoint leaked)"
    );
}

/// Peer offline → writes on the surviving node → peer back online. The
/// fire-and-forget fan-out must neither fail nor wedge local writes while the
/// peer is down; the rebooted peer keeps its pre-outage data and receives new
/// writes again — but the writes that happened during the outage are lost for
/// it (no anti-entropy catch-up; documented cluster limitation).
#[tokio::test]
async fn peer_offline_then_online_resumes_new_writes_without_catchup() {
    let dir = tempfile::tempdir().unwrap();
    let free = || {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        format!("127.0.0.1:{p}")
    };
    let a_addr = free();
    let b_addr = free();
    let cfg_for = |listen: &str, peers: Vec<String>, db: &str| docsql_server::ServerConfig {
        db_path: dir.path().join(db),
        listen: listen.to_string(),
        auth_token: None,
        read_token: None,
        max_conn: 0,
        idle_timeout_secs: 0,
        auth_lock_threshold: 10,
        cluster_token: None,
        advertise: None,
        replicate_to: None,
        peers,
        read_only: false,
        transport_key: None,
        async_commit: false,
    };

    // Two-node symmetric cluster; keep b's handle so the test can take it down.
    tokio::spawn(docsql_server::run(cfg_for(
        &a_addr,
        vec![b_addr.clone()],
        "offa.db",
    )));
    let b = tokio::spawn(docsql_server::run(cfg_for(
        &b_addr,
        vec![a_addr.clone()],
        "offb.db",
    )));
    for addr in [&a_addr, &b_addr] {
        for _ in 0..100 {
            if TcpStream::connect(addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    // Baseline with both nodes up: the table and one row replicate to b.
    let mut a = Client::connect(&a_addr).await;
    a.sql("CREATE TABLE off (id INT PRIMARY KEY, note TEXT)")
        .await;
    a.sql("INSERT INTO off VALUES (1, 'both-up')").await;
    assert!(
        wait_seen(&b_addr, "SELECT id FROM off WHERE id = 1", "[[1]]").await,
        "baseline write did not replicate while both nodes were up"
    );

    // b goes down: aborting the task drops the listener (connection refused
    // from here on); its committed state stays on disk. wait_seen's poll
    // connections are already closed, so nothing holds the old port open.
    b.abort();
    let _ = b.await;

    // Writes during the outage — autocommit and a committed transaction.
    // Fan-out to the dead peer fails inline, but write_order is held across
    // it, so this also proves a dead peer neither fails nor wedges the write.
    let resp = a.sql("INSERT INTO off VALUES (2, 'outage')").await;
    assert_eq!(
        resp.frame_type,
        proto::RESP_AFFECTED,
        "write failed while peer was offline: {}",
        payload_str(&resp)
    );
    a.sql("BEGIN").await;
    a.sql("INSERT INTO off VALUES (3, 'outage-tx')").await;
    let resp = a.sql("COMMIT").await;
    assert_ne!(resp.frame_type, proto::RESP_ERROR);

    // b rejoins on the same port with the same data dir.
    tokio::spawn(docsql_server::run(cfg_for(
        &b_addr,
        vec![a_addr.clone()],
        "offb.db",
    )));
    for _ in 0..100 {
        if TcpStream::connect(&b_addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let mut b = Client::connect(&b_addr).await;

    // b kept its pre-outage row, and the outage writes are gone for it:
    // nothing re-sends them. Deterministic — fan-out runs before the write
    // responds, so the failed deliveries for ids 2/3 already completed while
    // b was still down.
    let resp = b.sql("SELECT id FROM off ORDER BY id").await;
    let text = payload_str(&resp);
    assert!(text.contains("[[1]]"), "b lost its pre-outage data: {text}");
    assert!(
        !text.contains("[[2]]") && !text.contains("[[3]]"),
        "outage writes unexpectedly reached b: {text}"
    );

    // New writes flow to the rejoined peer again (fresh connection per write).
    a.sql("INSERT INTO off VALUES (4, 'after-rejoin')").await;
    assert!(
        wait_seen(&b_addr, "SELECT id FROM off WHERE id = 4", "[[4]]").await,
        "write after rejoin did not reach b"
    );

    // The rejoined node is a full peer again: its write fans out to a.
    b.sql("INSERT INTO off VALUES (5, 'b-after-rejoin')").await;
    assert!(
        wait_seen(&a_addr, "SELECT id FROM off WHERE id = 5", "[[5]]").await,
        "rejoined peer's write did not reach a"
    );

    // Final state: a saw everything; b is missing exactly the outage writes.
    let resp = a.sql("SELECT COUNT(id) FROM off").await;
    assert!(
        payload_str(&resp).contains("[[5]]"),
        "a: {}",
        payload_str(&resp)
    );
    let resp = b.sql("SELECT COUNT(id) FROM off").await;
    assert!(
        payload_str(&resp).contains("[[3]]"),
        "b should hold exactly the three non-outage rows: {}",
        payload_str(&resp)
    );
}

/// REQ_STATUS: node status report for cluster monitoring. Gated behind AUTH
/// like SQL; the payload reports uptime / read-only / peers / storage /
/// durable LSN / table totals. PING needs no auth (the web console's probe
/// relies on that for liveness).
#[tokio::test]
async fn status_frame_over_wire() {
    let (_dir, addr) = start_server(Some("s3cret")).await;
    let mut c = Client::connect(&addr).await;

    c.send(&Frame::new(proto::REQ_STATUS, vec![])).await;
    let r = c.recv().await;
    assert_eq!(r.frame_type, proto::RESP_ERROR);
    assert_eq!(payload_str(&r), "unauthorized");

    let r = c.ping().await;
    assert_eq!(r.frame_type, proto::RESP_PONG);

    c.auth("s3cret").await;
    c.sql("CREATE TABLE s (id INT)").await;
    c.sql("INSERT INTO s VALUES (1), (2)").await;

    c.send(&Frame::new(proto::REQ_STATUS, vec![])).await;
    let r = c.recv().await;
    assert_eq!(r.frame_type, proto::RESP_STATUS, "{}", payload_str(&r));
    let v: serde_json::Value = serde_json::from_slice(&r.payload).unwrap();
    assert_eq!(v["name"], "docsql");
    assert_eq!(v["read_only"], false);
    assert_eq!(v["in_transaction"], false);
    assert_eq!(v["peers"], serde_json::json!([]));
    assert_eq!(v["totals"]["tables"], 1);
    assert_eq!(v["totals"]["rows"], 2);
    assert!(v["uptime_ms"].as_u64().unwrap() > 0);
    assert!(v["durable_lsn"].as_u64().unwrap() > 0);
    assert_eq!(v["storage"]["page_size"], 4096);
}

/// Query log: executed statements appear in the docsql_log view with
/// latency and affected counts; the view query itself is not logged.
#[tokio::test]
async fn query_log_records_statements() {
    let dir = tempfile::tempdir().unwrap();
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = format!("127.0.0.1:{}", l.local_addr().unwrap().port());
    drop(l);
    let db = dir.path().join("qlog.db");
    let db_str = db.clone();
    tokio::spawn(docsql_server::run(docsql_server::ServerConfig {
        db_path: db_str,
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
    }));
    for _ in 0..100 {
        if TcpStream::connect(&addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    let mut c = Client::connect(&addr).await;
    c.sql("CREATE TABLE q (id INT)").await;
    c.sql("INSERT INTO q VALUES (1), (2)").await;
    c.sql("SELECT nope FROM missing").await; // 错误也要记录

    let resp = c
        .sql("SELECT sql, affected, error FROM docsql_log ORDER BY ts_ms")
        .await;
    let text = String::from_utf8_lossy(&resp.payload).to_string();
    assert!(resp.frame_type == proto::RESP_ROWS, "got {text}");
    assert!(text.contains("CREATE TABLE q"), "missing create: {text}");
    assert!(text.contains("INSERT INTO q"), "missing insert: {text}");
    assert!(text.contains("missing"), "missing error entry: {text}");
    // affected:插入 2 行应记录 2
    assert!(text.contains("2"), "affected count missing: {text}");
    // 视图查询本身不写日志:每行含一次 peer,数 peer 出现次数
    let resp2 = c.sql("SELECT * FROM docsql_log").await;
    let t2 = String::from_utf8_lossy(&resp2.payload).to_string();
    assert_eq!(t2.matches("127.0.0.1").count(), 3, "log rows: {t2}");
}

// ---------------------------------------------------------------------------
// Pub/sub: persistent messages, replay cursors, patterns, trim, cluster.
// ---------------------------------------------------------------------------

/// Live delivery between two connections; PUBLISH reports the persisted id
/// and receiver count; the push frame carries the Redis message shape.
#[tokio::test]
async fn pubsub_live_delivery_between_connections() {
    let (_dir, addr) = start_server(None).await;
    let mut sub = Client::connect(&addr).await;
    let r = sub.subscribe("news", "latest").await;
    assert_eq!(r.frame_type, proto::RESP_AFFECTED);
    assert_eq!(affected_u64(&r), 1, "one active subscription");

    let mut publisher = Client::connect(&addr).await;
    let r = publisher.publish("news", "hello world").await;
    assert_eq!(r.frame_type, proto::RESP_ROWS, "{}", payload_str(&r));
    let v: serde_json::Value = serde_json::from_slice(&r.payload).unwrap();
    let id = v["rows"][0][0].as_i64().unwrap();
    assert!(id >= 1, "persisted id in the reply");
    assert_eq!(v["rows"][0][1].as_i64(), Some(1), "one live receiver");

    let f = sub.recv().await;
    assert_eq!(f.frame_type, proto::RESP_PUSH);
    let m: serde_json::Value = serde_json::from_slice(&f.payload).unwrap();
    assert_eq!(m["kind"], "message");
    assert_eq!(m["channel"], "news");
    assert_eq!(m["payload"], "hello world");
    assert_eq!(m["id"].as_i64(), Some(id));
    assert!(m["ts"].as_i64().unwrap() > 0);
}

/// Group-commit mode (async_commit = true): statement commits are deferred to
/// the background flusher, yet acked writes are immediately visible and the
/// pub/sub push still happens only after the forced durability flush.
#[tokio::test]
async fn async_commit_mode_serves_writes_and_pubsub() {
    let (_dir, addr) = start_server_async_commit().await;
    let mut c = Client::connect(&addr).await;
    assert_eq!(
        c.sql("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
            .await
            .frame_type,
        proto::RESP_AFFECTED
    );
    for i in 0..50 {
        let r = c.sql(&format!("INSERT INTO t VALUES ({i}, 'v{i}')")).await;
        assert_eq!(r.frame_type, proto::RESP_AFFECTED, "row {i}");
    }
    // Acked (deferred-committed) rows are visible through the buffer pool.
    let r = c.sql("SELECT COUNT(*) FROM t").await;
    let v: serde_json::Value = serde_json::from_slice(&r.payload).unwrap();
    assert_eq!(v["rows"][0][0].as_i64(), Some(50));

    // Pub/sub keeps its persist-before-push contract in async mode: the
    // subscriber gets the live push, and the store lists the message.
    let mut sub = Client::connect(&addr).await;
    assert_eq!(
        sub.subscribe("news", "latest").await.frame_type,
        proto::RESP_AFFECTED
    );
    let r = c.publish("news", "under group commit").await;
    assert_eq!(r.frame_type, proto::RESP_ROWS, "{}", payload_str(&r));
    let f = sub.recv().await;
    assert_eq!(f.frame_type, proto::RESP_PUSH);
    let r = c.pubsub_cmd(r#"{"sub":"channels"}"#).await;
    assert_eq!(r.frame_type, proto::RESP_ROWS, "{}", payload_str(&r));
    assert!(payload_str(&r).contains("\"news\""));
    // The message row itself is in the persistent store.
    let r = c
        .sql("SELECT payload FROM docsql_pubsub WHERE channel = 'news'")
        .await;
    assert_eq!(r.frame_type, proto::RESP_ROWS, "{}", payload_str(&r));
    assert!(payload_str(&r).contains("under group commit"));
    // Reads keep working after the flusher has had its ticks.
    tokio::time::sleep(Duration::from_millis(10)).await;
    let r = c.sql("SELECT v FROM t WHERE id = 7").await;
    let v: serde_json::Value = serde_json::from_slice(&r.payload).unwrap();
    assert_eq!(v["rows"][0][0], "v7");
}

/// PSUBSCRIBE glob: matching channels deliver `pmessage` frames carrying
/// the pattern; non-matching channels deliver nothing.
#[tokio::test]
async fn pubsub_pattern_delivery() {
    let (_dir, addr) = start_server(None).await;
    let mut sub = Client::connect(&addr).await;
    let r = sub.psubscribe("news.*", "latest").await;
    assert_eq!(affected_u64(&r), 1);

    let mut publisher = Client::connect(&addr).await;
    publisher.publish("news.tech", "deep dive").await;
    publisher.publish("sports", "ignored").await;

    let f = sub.recv().await;
    let m: serde_json::Value = serde_json::from_slice(&f.payload).unwrap();
    assert_eq!(m["kind"], "pmessage");
    assert_eq!(m["pattern"], "news.*");
    assert_eq!(m["channel"], "news.tech");
    assert_eq!(m["payload"], "deep dive");
    assert!(
        recv_timeout(&mut sub, 300).await.is_none(),
        "sports must not deliver"
    );
}

/// Messages persist even with no subscribers; `from=earliest` replays the
/// full history in id order after the subscription confirmation.
#[tokio::test]
async fn pubsub_persist_and_replay_earliest() {
    let (_dir, addr) = start_server(None).await;
    let mut publisher = Client::connect(&addr).await;
    for i in 1..=3 {
        let r = publisher.publish("news", &format!("m{i}")).await;
        assert_eq!(r.frame_type, proto::RESP_ROWS);
        let v: serde_json::Value = serde_json::from_slice(&r.payload).unwrap();
        assert_eq!(v["rows"][0][1].as_i64(), Some(0), "no live subscribers");
    }

    let mut sub = Client::connect(&addr).await;
    let r = sub.subscribe("news", "earliest").await;
    assert_eq!(affected_u64(&r), 1);
    let mut last = 0i64;
    for i in 1..=3 {
        let f = sub.recv().await;
        let m: serde_json::Value = serde_json::from_slice(&f.payload).unwrap();
        assert_eq!(m["channel"], "news");
        assert_eq!(m["payload"], format!("m{i}"));
        let id = m["id"].as_i64().unwrap();
        assert!(id > last, "replay must be id-ordered");
        last = id;
    }
    assert!(
        recv_timeout(&mut sub, 300).await.is_none(),
        "replay stops at the watermark"
    );
}

/// Cursor resume: disconnect, miss a publish, re-subscribe from the last
/// seen id and receive exactly the missed message (at-least-once).
#[tokio::test]
async fn pubsub_resume_from_id_after_reconnect() {
    let (_dir, addr) = start_server(None).await;
    let mut sub = Client::connect(&addr).await;
    sub.subscribe("news", "latest").await;

    let mut publisher = Client::connect(&addr).await;
    publisher.publish("news", "kept-1").await;
    publisher.publish("news", "kept-2").await;
    let mut last = 0i64;
    for _ in 0..2 {
        let f = sub.recv().await;
        let m: serde_json::Value = serde_json::from_slice(&f.payload).unwrap();
        last = last.max(m["id"].as_i64().unwrap());
    }
    drop(sub); // cursor held client-side; this publish is missed live
    publisher.publish("news", "missed").await;

    let mut sub2 = Client::connect(&addr).await;
    let r = sub2.subscribe("news", &last.to_string()).await;
    assert_eq!(affected_u64(&r), 1);
    let f = sub2.recv().await;
    let m: serde_json::Value = serde_json::from_slice(&f.payload).unwrap();
    assert_eq!(m["payload"], "missed");
    assert!(m["id"].as_i64().unwrap() > last);
    assert!(
        recv_timeout(&mut sub2, 300).await.is_none(),
        "older messages must not replay again"
    );
}

/// PUBSUB introspection and unsubscribe bookkeeping.
#[tokio::test]
async fn pubsub_unsubscribe_and_introspection() {
    let (_dir, addr) = start_server(None).await;
    let mut a = Client::connect(&addr).await;
    let mut b = Client::connect(&addr).await;
    a.subscribe("news", "latest").await;
    a.psubscribe("blog.*", "latest").await;
    b.subscribe("news", "latest").await;

    let r = a.pubsub_cmd(r#"{"sub":"channels"}"#).await;
    assert_eq!(r.frame_type, proto::RESP_ROWS, "{}", payload_str(&r));
    assert!(payload_str(&r).contains("\"news\""));

    let r = a
        .pubsub_cmd(r#"{"sub":"numsub","channels":["news","ghost"]}"#)
        .await;
    let text = payload_str(&r);
    assert!(
        text.contains("[\"news\",2]") && text.contains("[\"ghost\",0]"),
        "{text}"
    );

    let r = a.pubsub_cmd(r#"{"sub":"numpat"}"#).await;
    assert!(payload_str(&r).contains("[[1]]"), "{}", payload_str(&r));

    // Unsubscribe by name keeps the other subscription; empty array clears
    // the rest.
    let r = a.unsubscribe(r#"["news"]"#).await;
    assert_eq!(affected_u64(&r), 1, "blog.* pattern remains");
    let r = a.punsubscribe("[]").await;
    assert_eq!(affected_u64(&r), 0);

    let mut publisher = Client::connect(&addr).await;
    let r = publisher.publish("news", "x").await;
    let v: serde_json::Value = serde_json::from_slice(&r.payload).unwrap();
    assert_eq!(v["rows"][0][1].as_i64(), Some(1), "only b remains");
    let f = b.recv().await;
    let m: serde_json::Value = serde_json::from_slice(&f.payload).unwrap();
    assert_eq!(m["channel"], "news");
}

/// Retention (PUBSUB TRIM), the docsql_pubsub view, the system-table guard
/// and the status census hiding the backing table.
#[tokio::test]
async fn pubsub_trim_view_and_system_guard() {
    let (_dir, addr) = start_server(None).await;
    let mut c = Client::connect(&addr).await;
    for i in 1..=5 {
        c.publish("ch", &format!("m{i}")).await;
    }
    c.publish("other", "keepme").await;

    // The backing table is invisible to SQL clients...
    let r = c.sql("SELECT * FROM _pubsub_messages").await;
    assert_eq!(r.frame_type, proto::RESP_ERROR);
    assert!(payload_str(&r).contains("internal"), "{}", payload_str(&r));
    // ...but the docsql_pubsub view reads it with full SQL.
    let r = c.sql("SELECT COUNT(*) FROM docsql_pubsub").await;
    assert_eq!(r.frame_type, proto::RESP_ROWS, "{}", payload_str(&r));
    assert!(payload_str(&r).contains("[[6]]"), "{}", payload_str(&r));

    // Trim keeps the two newest "ch" messages; "other" is untouched.
    let r = c
        .pubsub_cmd(r#"{"sub":"trim","channel":"ch","keep":2}"#)
        .await;
    assert_eq!(r.frame_type, proto::RESP_AFFECTED, "{}", payload_str(&r));
    assert_eq!(affected_u64(&r), 3);
    let r = c.sql("SELECT COUNT(*) FROM docsql_pubsub").await;
    assert!(payload_str(&r).contains("[[3]]"), "{}", payload_str(&r));

    // The surviving history replays for a late subscriber.
    let mut sub = Client::connect(&addr).await;
    sub.subscribe("ch", "earliest").await;
    for expect in ["m4", "m5"] {
        let f = sub.recv().await;
        let m: serde_json::Value = serde_json::from_slice(&f.payload).unwrap();
        assert_eq!(m["payload"], expect);
    }
    assert!(recv_timeout(&mut sub, 300).await.is_none());

    // keep=0 is rejected: emptying the channel would reset id monotonicity.
    let r = c
        .pubsub_cmd(r#"{"sub":"trim","channel":"ch","keep":0}"#)
        .await;
    assert_eq!(r.frame_type, proto::RESP_ERROR);

    // The system table stays out of user-facing status totals.
    c.sql("CREATE TABLE t (id INT)").await;
    c.send(&Frame::new(proto::REQ_STATUS, vec![])).await;
    let r = c.recv().await;
    let v: serde_json::Value = serde_json::from_slice(&r.payload).unwrap();
    assert_eq!(v["totals"]["tables"], 1);
}

/// Pub/sub frames are gated behind AUTH like SQL.
#[tokio::test]
async fn pubsub_frames_require_auth() {
    let (_dir, addr) = start_server(Some("s3cret")).await;
    let mut c = Client::connect(&addr).await;
    let r = c.subscribe("news", "latest").await;
    assert_eq!(payload_str(&r), "unauthorized");
    let r = c.publish("news", "x").await;
    assert_eq!(payload_str(&r), "unauthorized");
    c.auth("s3cret").await;
    let r = c.subscribe("news", "latest").await;
    assert_eq!(r.frame_type, proto::RESP_AFFECTED);
}

/// Symmetric cluster: a publish on node 1 reaches node 0's subscriber and
/// persists on both nodes.
#[tokio::test]
async fn pubsub_cross_node_delivery() {
    let dir = tempfile::tempdir().unwrap();
    let free = || {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        format!("127.0.0.1:{p}")
    };
    let addrs = vec![free(), free()];
    for (i, addr) in addrs.iter().enumerate() {
        let peers = addrs
            .iter()
            .enumerate()
            .filter(|(j, _)| *j != i)
            .map(|(_, a)| a.clone())
            .collect::<Vec<_>>()
            .join(",");
        tokio::spawn(docsql_server::run(docsql_server::ServerConfig {
            db_path: dir.path().join(format!("pubsub_peer{i}.db")),
            listen: addr.clone(),
            auth_token: None,
            read_token: None,
            max_conn: 0,
            idle_timeout_secs: 0,
            auth_lock_threshold: 10,
            cluster_token: None,
            replicate_to: None,
            peers: peers.split(',').map(String::from).collect(),
            advertise: None,
            read_only: false,
            transport_key: None,
            async_commit: false,
        }));
    }
    for addr in &addrs {
        for _ in 0..100 {
            if TcpStream::connect(addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    let mut sub = Client::connect(&addrs[0]).await;
    let r = sub.subscribe("cluster", "latest").await;
    assert_eq!(affected_u64(&r), 1);

    let mut publisher = Client::connect(&addrs[1]).await;
    let r = publisher.publish("cluster", "from-b").await;
    assert_eq!(r.frame_type, proto::RESP_ROWS, "{}", payload_str(&r));
    let v: serde_json::Value = serde_json::from_slice(&r.payload).unwrap();
    assert_eq!(
        v["rows"][0][1].as_i64(),
        Some(1),
        "node0 subscriber counted"
    );

    let f = sub.recv().await;
    assert_eq!(f.frame_type, proto::RESP_PUSH);
    let m: serde_json::Value = serde_json::from_slice(&f.payload).unwrap();
    assert_eq!(m["channel"], "cluster");
    assert_eq!(m["payload"], "from-b");

    // The replicated publish also persisted on node 0.
    assert!(
        wait_seen(&addrs[0], "SELECT COUNT(*) FROM docsql_pubsub", "[[1]]").await,
        "replicated message not persisted on node0"
    );
}

/// Auto-generated GUID primary keys must converge across the symmetric
/// cluster: the origin fills UUIDv7 values and replicates the explicit-value
/// rewrite, so peers apply the exact generated ids (random values cannot be
/// re-derived the way AUTOINCREMENT's max+1 can). Autocommit and committed
/// transaction writes both count.
#[tokio::test]
async fn symmetric_cluster_guid_autogen_converges() {
    let dir = tempfile::tempdir().unwrap();
    let free = || {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        format!("127.0.0.1:{p}")
    };
    let addrs = vec![free(), free()];
    for (i, addr) in addrs.iter().enumerate() {
        let peers = addrs
            .iter()
            .enumerate()
            .filter(|(j, _)| *j != i)
            .map(|(_, a)| a.clone())
            .collect::<Vec<_>>()
            .join(",");
        tokio::spawn(docsql_server::run(docsql_server::ServerConfig {
            db_path: dir.path().join(format!("guidpeer{i}.db")),
            listen: addr.clone(),
            auth_token: None,
            read_token: None,
            max_conn: 0,
            idle_timeout_secs: 0,
            auth_lock_threshold: 10,
            cluster_token: None,
            replicate_to: None,
            peers: peers.split(',').map(String::from).collect(),
            advertise: None,
            read_only: false,
            transport_key: None,
            async_commit: false,
        }));
    }
    for addr in &addrs {
        for _ in 0..100 {
            if TcpStream::connect(addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    let mut a = Client::connect(&addrs[0]).await;
    a.sql("CREATE TABLE gdoc (id GUID PRIMARY KEY AUTOINCREMENT, v TEXT)")
        .await;
    // Autocommit insert without the guid column.
    let resp = a.sql("INSERT INTO gdoc (v) VALUES ('origin')").await;
    assert_eq!(
        resp.frame_type,
        proto::RESP_AFFECTED,
        "{}",
        payload_str(&resp)
    );
    let mut b = Client::connect(&addrs[1]).await;
    // Both nodes were born fresh with a mesh, so b may still be inside its
    // bootstrap window when a's fan-out lands (the write queues until the
    // dump replay finishes). Poll briefly instead of assuming synchronous
    // fan-out.
    let mut resp = None;
    for _ in 0..500 {
        let r = b.sql("SELECT id FROM gdoc WHERE v = 'origin'").await;
        let text = String::from_utf8_lossy(&r.payload).to_string();
        // The projection is the id column only — non-empty rows is the
        // arrival signal.
        if r.frame_type == proto::RESP_ROWS && text.contains("\"rows\":[[") {
            resp = Some(r);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let resp = resp.expect("guid row never reached the peer");
    let v: serde_json::Value = serde_json::from_slice(&resp.payload).unwrap();
    let id_on_b = v["rows"][0][0].as_str().expect("guid on peer").to_string();
    assert_eq!(&id_on_b[14..15], "7", "uuidv7 reached the peer: {id_on_b}");
    // The origin stored the very same id.
    let resp = a.sql("SELECT id FROM gdoc WHERE v = 'origin'").await;
    let v: serde_json::Value = serde_json::from_slice(&resp.payload).unwrap();
    let id_on_a = v["rows"][0][0]
        .as_str()
        .expect("guid on origin")
        .to_string();
    assert_eq!(
        id_on_a, id_on_b,
        "origin and peer diverged on the generated guid"
    );

    // Transactional write from the OTHER node converges back the same way.
    b.sql("BEGIN").await;
    let resp = b.sql("INSERT INTO gdoc (id, v) VALUES (NULL, 'tx')").await;
    assert_eq!(
        resp.frame_type,
        proto::RESP_AFFECTED,
        "{}",
        payload_str(&resp)
    );
    b.sql("COMMIT").await;
    assert!(
        wait_seen(&addrs[0], "SELECT COUNT(*) FROM gdoc", "[[2]]").await,
        "node0 did not receive the guid transaction write"
    );
    // Both nodes hold exactly the two generated guids — no per-node re-generation.
    for addr in &addrs {
        let mut c = Client::connect(addr).await;
        let resp = c.sql("SELECT COUNT(*) FROM gdoc").await;
        let v: serde_json::Value = serde_json::from_slice(&resp.payload).unwrap();
        assert_eq!(
            v["rows"][0][0].as_i64(),
            Some(2),
            "node {addr} guid rows diverged"
        );
    }
}

/// REQ_LOGS: statement-audit + sync events for the console's logs page.
/// Gated behind AUTH like SQL; query entries carry the executed statements,
/// sync entries record the fan-out trail (ok to the live peer, error to a
/// dead one), and the peer's query log marks replicated applies.
#[tokio::test]
async fn logs_frame_over_wire() {
    // AUTH gate first (mirrors REQ_STATUS).
    let (_tdir, taddr) = start_server(Some("s3cret")).await;
    let mut t = Client::connect(&taddr).await;
    t.send(&Frame::new(proto::REQ_LOGS, vec![])).await;
    let r = t.recv().await;
    assert_eq!(r.frame_type, proto::RESP_ERROR);
    assert_eq!(payload_str(&r), "unauthorized");
    t.auth("s3cret").await;
    t.send(&Frame::new(proto::REQ_LOGS, vec![])).await;
    let r = t.recv().await;
    assert_eq!(r.frame_type, proto::RESP_LOGS, "{}", payload_str(&r));
    let v: serde_json::Value = serde_json::from_slice(&r.payload).unwrap();
    assert!(v["query"].as_array().unwrap().is_empty());
    // The successful AUTH above is audited to the sync ring; no data-plane
    // (forward/publish/trim/...) events may exist on a fresh node.
    assert!(
        v["sync"]
            .as_array()
            .unwrap()
            .iter()
            .all(|e| e["event"] == "auth"),
        "unexpected non-auth sync events: {}",
        payload_str(&r)
    );

    let dir = tempfile::tempdir().unwrap();
    let free = || {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        format!("127.0.0.1:{p}")
    };
    let a_addr = free();
    let b_addr = free();
    let cfg_for = |listen: &str, peers: Vec<String>, db: &str| docsql_server::ServerConfig {
        db_path: dir.path().join(db),
        listen: listen.to_string(),
        auth_token: None,
        read_token: None,
        max_conn: 0,
        idle_timeout_secs: 0,
        auth_lock_threshold: 10,
        cluster_token: None,
        advertise: None,
        replicate_to: None,
        peers,
        read_only: false,
        transport_key: None,
        async_commit: false,
    };
    // a fans out to the live peer b and a dead address: both attempts must
    // show up in the sync log (ok and error respectively).
    tokio::spawn(docsql_server::run(cfg_for(
        &a_addr,
        vec![b_addr.clone(), "127.0.0.1:1".into()],
        "la.db",
    )));
    tokio::spawn(docsql_server::run(cfg_for(&b_addr, Vec::new(), "lb.db")));
    for addr in [&a_addr, &b_addr] {
        for _ in 0..100 {
            if TcpStream::connect(addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    // Data log: executed statements appear; sync log: per-target fan-out.
    let mut c = Client::connect(&a_addr).await;
    c.sql("CREATE TABLE lg (id INT)").await;
    c.sql("INSERT INTO lg VALUES (7)").await;
    assert!(
        wait_seen(&b_addr, "SELECT id FROM lg WHERE id = 7", "[[7]]").await,
        "fan-out to the live peer did not converge"
    );

    c.send(&Frame::new(proto::REQ_LOGS, br#"{"limit": 50}"#.to_vec()))
        .await;
    let r = c.recv().await;
    assert_eq!(r.frame_type, proto::RESP_LOGS, "{}", payload_str(&r));
    let v: serde_json::Value = serde_json::from_slice(&r.payload).unwrap();
    let query = v["query"].as_array().unwrap();
    assert!(
        query
            .iter()
            .any(|e| e["sql"].as_str() == Some("INSERT INTO lg VALUES (7)")),
        "query entries missing the insert: {v}"
    );
    let sync = v["sync"].as_array().unwrap();
    let fwd = |target: &str| {
        sync.iter().find(|e| {
            e["event"] == "forward"
                && e["target"] == target
                && e["sql"] == "INSERT INTO lg VALUES (7)"
        })
    };
    let live = fwd(&b_addr).expect("no forward entry for the live peer");
    assert_eq!(live["ok"], true);
    assert!(live["detail"].is_null());
    let dead = fwd("127.0.0.1:1").expect("no forward entry for the dead peer");
    assert_eq!(dead["ok"], false);
    assert!(!dead["detail"].as_str().unwrap().is_empty());
    // Newest first within each section.
    assert!(
        query[0]["ts_ms"].as_u64().unwrap() >= query.last().unwrap()["ts_ms"].as_u64().unwrap()
    );

    // The peer's own log shows the replicated apply (replicated = true).
    let mut b = Client::connect(&b_addr).await;
    b.send(&Frame::new(proto::REQ_LOGS, vec![])).await;
    let r = b.recv().await;
    assert_eq!(r.frame_type, proto::RESP_LOGS, "{}", payload_str(&r));
    let v: serde_json::Value = serde_json::from_slice(&r.payload).unwrap();
    assert!(
        v["query"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| { e["sql"] == "INSERT INTO lg VALUES (7)" && e["replicated"] == true }),
        "peer missing the replicated apply: {v}"
    );
}

// ---- cluster join: a fresh node automatically syncs the cluster state ----

/// One cluster node bound to `addr`, with the given static peer mesh and
/// optional advertised address (DOCSQL_ADVERTISE). Waits for the port.
async fn spawn_node(
    dir: &tempfile::TempDir,
    name: &str,
    addr: &str,
    peers: Vec<String>,
    advertise: Option<&str>,
) {
    tokio::spawn(docsql_server::run(docsql_server::ServerConfig {
        db_path: dir.path().join(format!("{name}.db")),
        listen: addr.to_string(),
        auth_token: None,
        read_token: None,
        max_conn: 0,
        idle_timeout_secs: 0,
        auth_lock_threshold: 10,
        cluster_token: None,
        replicate_to: None,
        peers,
        advertise: advertise.map(String::from),
        read_only: false,
        transport_key: None,
        async_commit: false,
    }));
    for _ in 0..100 {
        if TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("node {name} did not come up");
}

/// wait_seen with a caller-chosen patience (a join takes a dump round trip).
async fn wait_seen_n(addr: &str, probe: &str, needle: &str, tries: usize) -> bool {
    for _ in 0..tries {
        let mut c = Client::connect(addr).await;
        let resp = c.sql(probe).await;
        let text = String::from_utf8_lossy(&resp.payload).to_string();
        if resp.frame_type != proto::RESP_ERROR && text.contains(needle) {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    false
}

/// The headline join scenario: a three-node cluster with history (schema
/// shapes, constraints, GUID ids, schemaless fields, tricky strings), then
/// a fourth node comes up pointing at the cluster — it must pull the full
/// state on its own, enforce the same constraints, register itself, and
/// take part in fan-out in both directions.
#[tokio::test]
async fn new_node_joins_cluster_and_syncs_full_history() {
    let dir = tempfile::tempdir().unwrap();
    let free = || {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        format!("127.0.0.1:{p}")
    };
    let (a, b, c, d) = (free(), free(), free(), free());
    let mesh = |me: &str| {
        [&a, &b, &c]
            .iter()
            .filter(|x| x.as_str() != me)
            .map(|x| x.to_string())
            .collect::<Vec<_>>()
    };
    spawn_node(&dir, "ja", &a, mesh(&a), None).await;
    spawn_node(&dir, "jb", &b, mesh(&b), None).await;
    spawn_node(&dir, "jc", &c, mesh(&c), None).await;
    // Capture addr strings before client handles shadow them below.
    let b_addr = b.clone();
    let joiner_peers = mesh(&d);

    // History: constraint-rich schema and data with tricky strings.
    let mut b = Client::connect(&b).await;
    b.sql("CREATE TABLE parent (pid INT PRIMARY KEY, tag TEXT UNIQUE NOT NULL)")
        .await;
    b.sql(
        "CREATE TABLE child (id INT PRIMARY KEY AUTOINCREMENT, gid GUID AUTOINCREMENT, \
         pid INT, note TEXT DEFAULT ('n/a'), CHECK (id >= 0), \
         FOREIGN KEY (pid) REFERENCES parent (pid))",
    )
    .await;
    b.sql("CREATE INDEX ix_child_note ON child (note)").await;
    b.sql("INSERT INTO parent VALUES (1, 'alpha'), (2, '中文''tag')")
        .await;
    b.sql("INSERT INTO child (pid, note, extra) VALUES (1, 'n1', 'schemaless')")
        .await;
    b.sql("INSERT INTO child (pid) VALUES (2)").await;
    let mut c = Client::connect(&c).await;
    c.sql("INSERT INTO parent VALUES (3, 'from-c')").await;
    // All three nodes started fresh with a full mesh, so their bootstraps
    // race the first writes; give the mesh a few seconds to settle.
    assert!(
        wait_seen_n(&a, "SELECT COUNT(pid) FROM parent", "[[3]]", 500).await,
        "cluster did not settle before the join"
    );

    // Node d comes up pointing at the cluster, advertising itself.
    spawn_node(&dir, "jd", &d, joiner_peers, Some(&d)).await;

    // The dump must land on d without any manual step.
    assert!(
        wait_seen_n(&d, "SELECT COUNT(pid) FROM parent", "[[3]]", 500).await,
        "joining node did not sync parent rows"
    );
    assert!(
        wait_seen_n(&d, "SELECT COUNT(id) FROM child", "[[2]]", 100).await,
        "joining node did not sync child rows"
    );
    // Constraint shapes came along: UNIQUE, NOT NULL, FK.
    let mut dc = Client::connect(&d).await;
    let r = dc.sql("INSERT INTO parent VALUES (9, 'alpha')").await;
    assert_eq!(r.frame_type, proto::RESP_ERROR, "UNIQUE lost after join");
    let r = dc.sql("INSERT INTO child (pid) VALUES (99)").await;
    assert_eq!(
        r.frame_type,
        proto::RESP_ERROR,
        "FOREIGN KEY lost after join"
    );
    let r = dc.sql("INSERT INTO parent (pid) VALUES (10)").await;
    assert_eq!(r.frame_type, proto::RESP_ERROR, "NOT NULL lost after join");
    // Auto-GUID ids are the exact cluster values (not re-generated).
    let r = dc.sql("SELECT gid FROM child WHERE pid = 1").await;
    let v: serde_json::Value = serde_json::from_slice(&r.payload).unwrap();
    let gid_d = v["rows"][0][0]
        .as_str()
        .expect("guid on joiner")
        .to_string();
    let r = b.sql("SELECT gid FROM child WHERE pid = 1").await;
    let v: serde_json::Value = serde_json::from_slice(&r.payload).unwrap();
    let gid_b = v["rows"][0][0]
        .as_str()
        .expect("guid on origin")
        .to_string();
    assert_eq!(gid_d, gid_b, "GUID diverged between origin and joiner");
    // The schemaless field and the tricky string survived the dump.
    assert!(
        wait_seen_n(
            &d,
            "SELECT extra FROM child WHERE pid = 1",
            "schemaless",
            100
        )
        .await,
        "schemaless field lost in the dump"
    );
    assert!(
        wait_seen_n(&d, "SELECT tag FROM parent WHERE pid = 2", "中文'tag", 100).await,
        "tricky string lost in the dump"
    );

    // Dynamic registration: the original nodes list d as a peer now.
    let mut a3 = Client::connect(&a).await;
    a3.send(&Frame::new(proto::REQ_STATUS, vec![])).await;
    let r = a3.recv().await;
    assert_eq!(r.frame_type, proto::RESP_STATUS);
    let v: serde_json::Value = serde_json::from_slice(&r.payload).unwrap();
    assert!(
        v["peers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p.as_str() == Some(d.as_str())),
        "joiner not registered on node a: {}",
        v["peers"]
    );

    // Writes fan both ways across the join. History is 3 parent rows; the
    // constraint probes above all errored, so the joiner's insert makes 4.
    let r = dc.sql("INSERT INTO parent VALUES (11, 'from-d')").await;
    assert_eq!(r.frame_type, proto::RESP_AFFECTED, "{}", payload_str(&r));
    assert!(
        wait_seen_n(&b_addr, "SELECT COUNT(pid) FROM parent", "[[4]]", 100).await,
        "write on the joiner did not fan out"
    );
    b.sql("INSERT INTO parent VALUES (12, 'from-b-after-join')")
        .await;
    assert!(
        wait_seen_n(&d, "SELECT COUNT(pid) FROM parent", "[[5]]", 100).await,
        "write on an original node did not reach the joiner"
    );

    // Cluster-join frames are node-internal: a plain client cannot sync.
    let mut plain = Client::connect(&a).await;
    plain
        .send(&Frame::new(proto::REQ_SYNC, b"127.0.0.1:1".to_vec()))
        .await;
    let r = plain.recv().await;
    assert_eq!(
        r.frame_type,
        proto::RESP_ERROR,
        "plain client could REQ_SYNC"
    );
}

/// Writes racing the join must converge everywhere: the quiesce holds make
/// the snapshot exact, and writes committing while the joiner replays are
/// gated until the dump lands.
#[tokio::test]
async fn join_with_concurrent_writes_converges() {
    let dir = tempfile::tempdir().unwrap();
    let free = || {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        format!("127.0.0.1:{p}")
    };
    let (a, b, d) = (free(), free(), free());
    spawn_node(&dir, "cja", &a, vec![b.clone()], None).await;
    spawn_node(&dir, "cjb", &b, vec![a.clone()], None).await;
    let mut bc = Client::connect(&b).await;
    bc.sql("CREATE TABLE racing (id INT PRIMARY KEY, v TEXT)")
        .await;
    for i in 0..50 {
        bc.sql(&format!("INSERT INTO racing VALUES ({i}, 'v{i}')"))
            .await;
    }
    assert!(
        wait_seen_n(&a, "SELECT COUNT(id) FROM racing", "[[50]]", 500).await,
        "cluster did not settle before the join"
    );
    // d joins from the mesh while b keeps writing.
    spawn_node(&dir, "cjd", &d, vec![a.clone(), b.clone()], Some(&d)).await;
    for i in 50..70 {
        bc.sql(&format!("INSERT INTO racing VALUES ({i}, 'late{i}')"))
            .await;
    }
    for addr in [&a, &b, &d] {
        assert!(
            wait_seen_n(addr, "SELECT COUNT(id) FROM racing", "[[70]]", 500).await,
            "node {addr} did not converge across the join"
        );
    }
}
