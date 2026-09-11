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
        catchup_window: 0,
        backup_interval_secs: 0,
        backup_keep: 7,
        backup_dir: None,
        statement_timeout_ms: 0,
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
        catchup_window: 0,
        backup_interval_secs: 0,
        backup_keep: 7,
        backup_dir: None,
        statement_timeout_ms: 0,
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
        catchup_window: 0,
        backup_interval_secs: 0,
        backup_keep: 7,
        backup_dir: None,
        statement_timeout_ms: 0,
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
        catchup_window: 0,
        backup_interval_secs: 0,
        backup_keep: 7,
        backup_dir: None,
        statement_timeout_ms: 0,
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
    // System tables are reported under their own key (the console's
    // read-only 系统表 branch), never mixed into the user catalog.
    let sys_names: Vec<&str> = v["system_tables"]
        .as_array()
        .expect("system_tables array")
        .iter()
        .map(|t| t["name"].as_str().expect("name"))
        .collect();
    assert!(sys_names.contains(&"_pubsub_messages"), "{sys_names:?}");
    assert!(!sys_names.contains(&"mt"), "{sys_names:?}");
    assert_eq!(
        v["tables"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|t| t["name"] == "_pubsub_messages")
            .count(),
        0
    );
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
        catchup_window: 0,
        backup_interval_secs: 0,
        backup_keep: 7,
        backup_dir: None,
        statement_timeout_ms: 0,
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
        catchup_window: 0,
        backup_interval_secs: 0,
        backup_keep: 7,
        backup_dir: None,
        statement_timeout_ms: 0,
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
            catchup_window: 0,
            backup_interval_secs: 0,
            backup_keep: 7,
            backup_dir: None,
            statement_timeout_ms: 0,
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
            catchup_window: 0,
            backup_interval_secs: 0,
            backup_keep: 7,
            backup_dir: None,
            statement_timeout_ms: 0,
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
/// peer is down; the rebooted peer keeps its pre-outage data, receives new
/// writes again, and — since the rejoin repair (anti-entropy at restart) —
/// catches up the writes it missed during the outage.
#[tokio::test]
async fn peer_offline_then_online_catches_up_missed_writes() {
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
        catchup_window: 0,
        backup_interval_secs: 0,
        backup_keep: 7,
        backup_dir: None,
        statement_timeout_ms: 0,
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

    // b kept its pre-outage row, and the rejoin repair caught it up with
    // everything it missed while down: inserts from autocommit and from a
    // committed transaction alike. Deterministic — the failed fan-outs for
    // ids 2/3 completed while b was still down, so only the repair can
    // bring them in.
    assert!(
        wait_seen(&b_addr, "SELECT id FROM off WHERE id = 2", "[[2]]").await,
        "rejoin repair did not catch up the outage insert (id=2)"
    );
    assert!(
        wait_seen(&b_addr, "SELECT id FROM off WHERE id = 3", "[[3]]").await,
        "rejoin repair did not catch up the outage transaction (id=3)"
    );
    let resp = b.sql("SELECT id FROM off WHERE id = 1").await;
    assert!(
        payload_str(&resp).contains("[[1]]"),
        "b lost its pre-outage data: {}",
        payload_str(&resp)
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

    // Final state: the repair made b converge, so both nodes hold all
    // five rows — base, outage, and post-rejoin writes alike.
    let resp = a.sql("SELECT COUNT(id) FROM off").await;
    assert!(
        payload_str(&resp).contains("[[5]]"),
        "a: {}",
        payload_str(&resp)
    );
    let resp = b.sql("SELECT COUNT(id) FROM off").await;
    assert!(
        payload_str(&resp).contains("[[5]]"),
        "b should hold all five rows after the repair: {}",
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
        catchup_window: 0,
        backup_interval_secs: 0,
        backup_keep: 7,
        backup_dir: None,
        statement_timeout_ms: 0,
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

    // The backing table accepts no mutations...
    let r = c
        .sql("INSERT INTO _pubsub_messages (channel, payload) VALUES ('x', 'y')")
        .await;
    assert_eq!(r.frame_type, proto::RESP_ERROR);
    assert!(payload_str(&r).contains("internal"), "{}", payload_str(&r));
    // ...while read-only queries go through (the console's 系统表 branch).
    let r = c.sql("SELECT COUNT(*) FROM _pubsub_messages").await;
    assert_eq!(r.frame_type, proto::RESP_ROWS, "{}", payload_str(&r));
    assert!(payload_str(&r).contains("[[6]]"), "{}", payload_str(&r));
    // Catch-up journal: same policy, own message.
    let r = c
        .sql("INSERT INTO _cluster_log (seq, sql) VALUES (1, 'x')")
        .await;
    assert_eq!(r.frame_type, proto::RESP_ERROR);
    assert!(payload_str(&r).contains("internal"), "{}", payload_str(&r));
    // But a user-table write whose LITERAL merely mentions a system table
    // is the user's own data: the gate classifies on the statement's write
    // targets, not on a substring scan over the whole text.
    c.sql("CREATE TABLE audit_t (note TEXT)").await;
    let r = c
        .sql("INSERT INTO audit_t VALUES ('switched to _cluster_pos today')")
        .await;
    assert_eq!(r.frame_type, proto::RESP_AFFECTED, "{}", payload_str(&r));
    let r = c.sql("SELECT COUNT(*) FROM audit_t").await;
    assert!(payload_str(&r).contains("[[1]]"), "{}", payload_str(&r));
    // ...and the docsql_pubsub view reads it with full SQL.
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

    // The system tables stay out of user-facing status totals (audit_t
    // from the gate check above and t are the only user tables).
    c.sql("CREATE TABLE t (id INT)").await;
    c.send(&Frame::new(proto::REQ_STATUS, vec![])).await;
    let r = c.recv().await;
    let v: serde_json::Value = serde_json::from_slice(&r.payload).unwrap();
    assert_eq!(v["totals"]["tables"], 2);
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
            catchup_window: 0,
            backup_interval_secs: 0,
            backup_keep: 7,
            backup_dir: None,
            statement_timeout_ms: 0,
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
            catchup_window: 0,
            backup_interval_secs: 0,
            backup_keep: 7,
            backup_dir: None,
            statement_timeout_ms: 0,
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
        catchup_window: 0,
        backup_interval_secs: 0,
        backup_keep: 7,
        backup_dir: None,
        statement_timeout_ms: 0,
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
        catchup_window: 0,
        backup_interval_secs: 0,
        backup_keep: 7,
        backup_dir: None,
        statement_timeout_ms: 0,
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

// ---- cluster rejoin repair (anti-entropy at restart) ----

/// Like [`spawn_node`], but returns the server task handle so a test can
/// abort it to simulate a node going down: memory state is lost, the
/// on-disk database survives — exactly a crash. Callers must not hold
/// client connections across the abort (a live connection task would keep
/// the old engine instance alive over the same file).
async fn spawn_node_handle(
    dir: &tempfile::TempDir,
    name: &str,
    addr: &str,
    peers: Vec<String>,
) -> tokio::task::JoinHandle<std::io::Result<()>> {
    spawn_node_window(dir, name, addr, peers, 0).await
}

/// [`spawn_node_handle`] with a catch-up journal window (entries): used
/// to exercise the snapshot fallback when a rejoining peer's position is
/// older than the origin's retained window.
async fn spawn_node_window(
    dir: &tempfile::TempDir,
    name: &str,
    addr: &str,
    peers: Vec<String>,
    window: u64,
) -> tokio::task::JoinHandle<std::io::Result<()>> {
    let handle = tokio::spawn(docsql_server::run(docsql_server::ServerConfig {
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
        advertise: None,
        read_only: false,
        transport_key: None,
        async_commit: false,
        catchup_window: window,
        backup_interval_secs: 0,
        backup_keep: 7,
        backup_dir: None,
        statement_timeout_ms: 0,
    }));
    for _ in 0..200 {
        if TcpStream::connect(addr).await.is_ok() {
            return handle;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("node {name} did not come up");
}

/// The node's sync-log entries (REQ_LOGS), as raw JSON text.
async fn sync_log_text(addr: &str) -> String {
    let mut c = Client::connect(addr).await;
    c.send(&Frame::new(proto::REQ_LOGS, vec![])).await;
    let r = c.recv().await;
    String::from_utf8_lossy(&r.payload).to_string()
}

async fn wait_port_down(addr: &str) {
    for _ in 0..200 {
        if TcpStream::connect(addr).await.is_err() {
            // Give a lingering connection task one tick to finish exiting
            // so the respawn opens the database file unshared.
            tokio::time::sleep(Duration::from_millis(100)).await;
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("node at {addr} still accepts connections");
}

/// [`spawn_node`] with async-commit mode on: statement fsyncs defer to the
/// background flusher, and the fused write units are disabled (their
/// immediate `end` sync would re-impose one fsync per clustered write).
async fn spawn_node_async(dir: &tempfile::TempDir, name: &str, addr: &str, peers: Vec<String>) {
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
        advertise: None,
        read_only: false,
        transport_key: None,
        async_commit: true,
        catchup_window: 0,
        backup_interval_secs: 0,
        backup_keep: 7,
        backup_dir: None,
        statement_timeout_ms: 0,
    }));
    for _ in 0..200 {
        if TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("node {name} did not come up");
}

/// Async-commit mode must not weaken the clustered write path: with the
/// fused units disabled, the journal append, the sequenced fan-out and the
/// receiver's position tracking all take the plain deferred path — writes
/// converge, the origin's `_cluster_log` records them, the peer's
/// `_cluster_pos` tracks the origin, and a transaction's drain lands its
/// buffered writes with journal entries.
#[tokio::test]
async fn async_commit_cluster_journals_and_converges() {
    let dir = tempfile::tempdir().unwrap();
    let free = || {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        format!("127.0.0.1:{p}")
    };
    let (a, b) = (free(), free());
    spawn_node_async(&dir, "aa", &a, vec![b.clone()]).await;
    spawn_node_async(&dir, "ab", &b, vec![a.clone()]).await;

    let mut ca = Client::connect(&a).await;
    ca.sql("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").await;
    ca.sql("INSERT INTO t VALUES (1, 'x')").await;
    assert!(
        wait_seen_n(&b, "SELECT COUNT(id) FROM t", "[[1]]", 200).await,
        "write did not converge to the async peer"
    );
    // DDL and DML both journaled on the origin (flusher-owned durability).
    assert!(
        wait_seen_n(&a, "SELECT COUNT(*) FROM _cluster_log", "[[2]]", 200).await,
        "journal entries missing in async mode"
    );
    // The receiver tracked the origin's position through REQ_SQL_SEQ.
    assert!(
        wait_seen_n(&b, "SELECT COUNT(*) FROM _cluster_pos", "[[1]]", 200).await,
        "position row missing in async mode"
    );
    // Transactional drain: buffered write lands on the peer with journal.
    ca.sql("BEGIN").await;
    ca.sql("INSERT INTO t VALUES (2, 'y')").await;
    ca.sql("COMMIT").await;
    assert!(
        wait_seen_n(&b, "SELECT COUNT(id) FROM t", "[[2]]", 200).await,
        "transactional write did not converge in async mode"
    );
    assert!(
        wait_seen_n(&a, "SELECT COUNT(*) FROM _cluster_log", "[[3]]", 200).await,
        "drain did not journal the buffered write in async mode"
    );
}

/// The headline anti-entropy scenario: a node that was down while its peer
/// wrote must catch up automatically on restart — inserts, updates, and
/// deletes that happened during the outage all land, and the mesh fans out
/// both ways afterwards. Fan-out never back-fills, so this only works
/// because the restarting node compares digests and adopts the snapshot.
#[tokio::test]
async fn rejoin_repair_catches_up_writes_missed_while_offline() {
    let dir = tempfile::tempdir().unwrap();
    let free = || {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        format!("127.0.0.1:{p}")
    };
    let (a_addr, b_addr) = (free(), free());
    let a_peers = vec![b_addr.clone()];
    let b_peers = vec![a_addr.clone()];
    let a = spawn_node_handle(&dir, "ra", &a_addr, a_peers).await;
    let b = spawn_node_handle(&dir, "rb", &b_addr, b_peers.clone()).await;

    // Base state on both nodes.
    let mut ca = Client::connect(&a_addr).await;
    ca.sql("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").await;
    ca.sql("INSERT INTO t VALUES (1, 'one')").await;
    ca.sql("INSERT INTO t VALUES (2, 'two')").await;
    assert!(
        wait_seen_n(&b_addr, "SELECT COUNT(id) FROM t", "[[2]]", 200).await,
        "base rows did not reach b"
    );
    drop(ca);

    // b goes down; a keeps writing (fan-out to the dead peer logs, never
    // blocks).
    b.abort();
    wait_port_down(&b_addr).await;
    let mut ca = Client::connect(&a_addr).await;
    ca.sql("INSERT INTO t VALUES (3, 'three')").await;
    ca.sql("UPDATE t SET v = 'uno' WHERE id = 1").await;
    ca.sql("DELETE FROM t WHERE id = 2").await;
    ca.sql("INSERT INTO t VALUES (4, 'four')").await;
    drop(ca);

    // b rejoins: the startup digest compare must adopt a's snapshot with
    // every missed write applied. The first rounds can overlap a's own
    // startup-sync window (it refuses to serve a snapshot while its gate
    // is open), so the repair may need several seconds — poll patiently.
    let b = spawn_node_handle(&dir, "rb", &b_addr, b_peers).await;
    assert!(
        wait_seen_n(&b_addr, "SELECT v FROM t WHERE id = 1", "uno", 1000).await,
        "missed UPDATE not repaired on rejoin"
    );
    assert!(
        wait_seen_n(&b_addr, "SELECT COUNT(id) FROM t", "[[3]]", 400).await,
        "missed INSERT/DELETE not repaired on rejoin"
    );
    assert!(
        wait_seen_n(&b_addr, "SELECT v FROM t WHERE id = 4", "four", 100).await,
        "missed INSERT id=4 not repaired on rejoin"
    );

    // The repaired mesh fans out both ways.
    let mut cb = Client::connect(&b_addr).await;
    let r = cb.sql("INSERT INTO t VALUES (5, 'five')").await;
    assert_eq!(r.frame_type, proto::RESP_AFFECTED, "{}", payload_str(&r));
    assert!(
        wait_seen_n(&a_addr, "SELECT v FROM t WHERE id = 5", "five", 200).await,
        "write on the repaired node did not fan out"
    );
    drop(cb);
    let mut ca = Client::connect(&a_addr).await;
    let r = ca.sql("INSERT INTO t VALUES (6, 'six')").await;
    assert_eq!(r.frame_type, proto::RESP_AFFECTED, "{}", payload_str(&r));
    drop(ca);
    assert!(
        wait_seen_n(&b_addr, "SELECT v FROM t WHERE id = 6", "six", 200).await,
        "post-repair write on a did not reach b"
    );
    // The repair took the incremental journal path, not a snapshot.
    let logs = sync_log_text(&b_addr).await;
    assert!(
        logs.contains("\"event\":\"catchup\""),
        "expected a catchup sync event on the rejoined node: {logs}"
    );
    assert!(
        !logs.contains("\"event\":\"repair\""),
        "snapshot adoption should not have run: {logs}"
    );
    a.abort();
    b.abort();
}

/// A restart without divergence must be a no-op: digests agree, the node
/// keeps its data (no wipe, no resync) and rejoins the fan-out mesh.
#[tokio::test]
async fn restart_without_divergence_keeps_data() {
    let dir = tempfile::tempdir().unwrap();
    let free = || {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        format!("127.0.0.1:{p}")
    };
    let (a_addr, b_addr) = (free(), free());
    let a_peers = vec![b_addr.clone()];
    let b_peers = vec![a_addr.clone()];
    let a = spawn_node_handle(&dir, "ra", &a_addr, a_peers).await;
    let b = spawn_node_handle(&dir, "rb", &b_addr, b_peers.clone()).await;

    let mut ca = Client::connect(&a_addr).await;
    ca.sql("CREATE TABLE keep (id INT PRIMARY KEY, v TEXT)")
        .await;
    ca.sql("INSERT INTO keep VALUES (1, 'a'), (2, 'b')").await;
    assert!(
        wait_seen_n(&b_addr, "SELECT COUNT(id) FROM keep", "[[2]]", 200).await,
        "base rows did not reach b"
    );
    drop(ca);

    b.abort();
    wait_port_down(&b_addr).await;
    let b = spawn_node_handle(&dir, "rb", &b_addr, b_peers).await;

    // Data survived the restart untouched.
    let mut cb = Client::connect(&b_addr).await;
    let r = cb.sql("SELECT v FROM keep WHERE id = 1").await;
    assert!(
        String::from_utf8_lossy(&r.payload).contains("\"a\""),
        "restart lost data: {}",
        payload_str(&r)
    );
    // And the mesh is intact in both directions.
    let r = cb.sql("INSERT INTO keep VALUES (3, 'c')").await;
    assert_eq!(r.frame_type, proto::RESP_AFFECTED, "{}", payload_str(&r));
    drop(cb);
    assert!(
        wait_seen_n(&a_addr, "SELECT v FROM keep WHERE id = 3", "\"c\"", 200).await,
        "write after clean restart did not fan out"
    );
    let mut ca = Client::connect(&a_addr).await;
    ca.sql("INSERT INTO keep VALUES (4, 'd')").await;
    drop(ca);
    assert!(
        wait_seen_n(&b_addr, "SELECT v FROM keep WHERE id = 4", "\"d\"", 200).await,
        "post-restart write on a did not reach b"
    );
    a.abort();
    b.abort();
}

/// When the rejoining peer's position is older than the origin's retained
/// journal window (the window trimmed mid-flight), incremental catch-up
/// is impossible — the repair must fall back to snapshot adoption and
/// still converge.
#[tokio::test]
async fn catchup_falls_back_to_snapshot_after_window_trim() {
    let dir = tempfile::tempdir().unwrap();
    let free = || {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        format!("127.0.0.1:{p}")
    };
    let (a_addr, b_addr) = (free(), free());
    let a_peers = vec![b_addr.clone()];
    let b_peers = vec![a_addr.clone()];
    // a keeps only a 4-entry journal window (trimmed as it passes each
    // 512-entry mark); b unlimited.
    let a = spawn_node_window(&dir, "ra", &a_addr, a_peers, 4).await;
    let b = spawn_node_handle(&dir, "rb", &b_addr, b_peers.clone()).await;

    let mut ca = Client::connect(&a_addr).await;
    ca.sql("CREATE TABLE big (id INT PRIMARY KEY)").await;
    ca.sql("INSERT INTO big VALUES (1)").await;
    assert!(
        wait_seen_n(&b_addr, "SELECT COUNT(id) FROM big", "[[1]]", 200).await,
        "base row did not reach b"
    );
    drop(ca);

    // b goes down; a writes far past the window (512-entry trim mark).
    b.abort();
    wait_port_down(&b_addr).await;
    let mut ca = Client::connect(&a_addr).await;
    for i in 2..=540 {
        let r = ca.sql(&format!("INSERT INTO big VALUES ({i})")).await;
        assert_eq!(r.frame_type, proto::RESP_AFFECTED, "insert {i} failed");
    }
    drop(ca);

    // b rejoins: its position (1) is far below a's oldest retained seq —
    // snapshot adoption is the only way out, and it must converge.
    let b = spawn_node_handle(&dir, "rb", &b_addr, b_peers).await;
    assert!(
        wait_seen_n(&b_addr, "SELECT COUNT(id) FROM big", "[[540]]", 1000).await,
        "b did not converge via the snapshot fallback"
    );
    let logs = sync_log_text(&b_addr).await;
    assert!(
        logs.contains("\"event\":\"repair\""),
        "expected a repair (snapshot adoption) event: {logs}"
    );
    a.abort();
    b.abort();
}

/// A sequenced fan-out arriving while the joiner's sync gate is open must
/// be queued and acknowledged — not applied directly over the
/// still-replaying snapshot, where it would error ("no such table") and
/// be lost despite the ack. After the join the write must have landed
/// exactly once and its origin's position must have advanced with it.
/// (Regression: REQ_SQL_SEQ used to bypass the gate entirely.)
#[tokio::test]
async fn sequenced_write_during_join_queues_instead_of_applying() {
    let dir = tempfile::tempdir().unwrap();
    let free = || {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        format!("127.0.0.1:{p}")
    };
    let (a_addr, d_addr) = (free(), free());
    spawn_node(&dir, "sja", &a_addr, vec![], None).await;
    let mut ca = Client::connect(&a_addr).await;
    ca.sql("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").await;
    for i in 0..50 {
        ca.sql(&format!("INSERT INTO t VALUES ({i}, 'v{i}')")).await;
    }
    drop(ca);

    // The joiner comes up with its gate open; the frame below is sent
    // before its snapshot can have landed, so a direct apply would hit a
    // database without the table at all.
    spawn_node(&dir, "sjd", &d_addr, vec![a_addr.clone()], None).await;
    let mut cd = Client::connect(&d_addr).await;
    let mut sql = Vec::with_capacity(64);
    sql.extend_from_slice(&7u64.to_le_bytes());
    sql.extend_from_slice(&("test-origin".len() as u32).to_le_bytes());
    sql.extend_from_slice(b"test-origin");
    sql.extend_from_slice(&proto::encode_sql("INSERT INTO t VALUES (999, 'late')").unwrap());
    let mut f = Frame::new(proto::REQ_SQL_SEQ, sql);
    f.flags = docsql_server::FLAG_REPLICATION;
    cd.send(&f).await;
    let r = cd.recv().await;
    assert_eq!(r.frame_type, proto::RESP_AFFECTED, "{}", payload_str(&r));
    drop(cd);

    // The queued write replays after the snapshot and lands once.
    assert!(
        wait_seen_n(&d_addr, "SELECT v FROM t WHERE id = 999", "late", 400).await,
        "queued sequenced write never replayed after the join"
    );
    assert!(
        wait_seen_n(&d_addr, "SELECT COUNT(id) FROM t", "[[51]]", 200).await,
        "joiner did not converge to snapshot + queued write"
    );
    // The drain records the origin's position (apply-then-record, same as
    // the live path).
    assert!(
        wait_seen_n(
            &d_addr,
            "SELECT seq FROM _cluster_pos WHERE node_id = 'test-origin'",
            "[[7]]",
            200
        )
        .await,
        "drained sequenced write did not advance its origin's position"
    );
}

/// A BEGIN arriving on a replication frame used to open the global
/// transaction with no owner connection: every later replicated write then
/// spun in the 30s busy-wait — nothing could COMMIT or roll it back, and
/// the owner-disconnect cleanup only fires for owner connections.
/// Transaction control must be rejected on replication connections.
#[tokio::test]
async fn replication_frames_reject_transaction_control() {
    let dir = tempfile::tempdir().unwrap();
    let free = || {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        format!("127.0.0.1:{p}")
    };
    let addr = free();
    spawn_node(&dir, "rtc", &addr, vec![], None).await;
    let mut c = Client::connect(&addr).await;
    c.sql("CREATE TABLE t (id INT PRIMARY KEY)").await;
    for sql in ["BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT s"] {
        let mut f = Frame::new(proto::REQ_SQL, proto::encode_sql(sql).unwrap());
        f.flags = docsql_server::FLAG_REPLICATION;
        c.send(&f).await;
        let r = c.recv().await;
        assert_eq!(
            r.frame_type,
            proto::RESP_ERROR,
            "{sql}: {}",
            payload_str(&r)
        );
        assert!(
            payload_str(&r).contains("replication"),
            "{sql}: {}",
            payload_str(&r)
        );
    }
    // No transaction was opened: later writes proceed immediately.
    c.sql("INSERT INTO t VALUES (1)").await;
    let r = c.sql("SELECT COUNT(id) FROM t").await;
    assert!(payload_str(&r).contains("[[1]]"), "{}", payload_str(&r));
}

/// After a join the joiner's catch-up positions must equal the origins'
/// current journal heads. The probe-round heads are stale by the whole
/// snapshot transfer; leaving them low made the next rejoin replay
/// snapshot-covered ops (duplicate-key errors) and degrade to snapshot
/// adoption even when a cheap incremental pull would have sufficed.
#[tokio::test]
async fn joined_node_tracks_origin_head_and_rejoin_pulls_incrementally() {
    let dir = tempfile::tempdir().unwrap();
    let free = || {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        format!("127.0.0.1:{p}")
    };
    let (a_addr, b_addr, d_addr) = (free(), free(), free());
    let a = spawn_node_handle(&dir, "jta", &a_addr, vec![b_addr.clone()]).await;
    let b = spawn_node_handle(&dir, "jtb", &b_addr, vec![a_addr.clone()]).await;

    let mut ca = Client::connect(&a_addr).await;
    ca.sql("CREATE TABLE t (id INT PRIMARY KEY)").await;
    for i in 1..=30 {
        ca.sql(&format!("INSERT INTO t VALUES ({i})")).await;
    }
    assert!(
        wait_seen_n(&b_addr, "SELECT COUNT(id) FROM t", "[[30]]", 400).await,
        "base rows did not reach b"
    );

    // d joins, then the mesh goes quiet: a's journal head freezes at 30.
    let d = spawn_node_handle(&dir, "jtd", &d_addr, vec![a_addr.clone()]).await;
    assert!(
        wait_seen_n(&d_addr, "SELECT COUNT(id) FROM t", "[[30]]", 500).await,
        "joiner did not converge"
    );
    let mut ca = Client::connect(&a_addr).await;
    ca.send(&Frame::new(proto::REQ_STATUS, vec![])).await;
    let r = ca.recv().await;
    let v: serde_json::Value = serde_json::from_slice(&r.payload).unwrap();
    let a_id = v["cluster_id"].as_str().expect("a reported cluster_id");
    // The origin's journal head covers every write incl. the CREATE TABLE.
    let rh = ca.sql("SELECT MAX(seq) FROM _cluster_log").await;
    assert_eq!(rh.frame_type, proto::RESP_ROWS, "{}", payload_str(&rh));
    let head = payload_str(&rh);
    let head = head.trim_matches(|c: char| !c.is_ascii_digit()).to_string();
    assert!(!head.is_empty(), "origin journal head unread: {head}");
    let mut ok = false;
    for _ in 0..200 {
        let mut cd = Client::connect(&d_addr).await;
        let r = cd
            .sql(&format!(
                "SELECT seq FROM _cluster_pos WHERE node_id = '{a_id}'"
            ))
            .await;
        drop(cd);
        if r.frame_type == proto::RESP_ROWS && payload_str(&r).contains(&format!("[[{head}]]")) {
            ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        ok,
        "joiner's position ({head} expected) did not reach the origin's journal head"
    );
    drop(ca);

    // d goes down; a writes a small gap on top.
    d.abort();
    wait_port_down(&d_addr).await;
    let mut ca = Client::connect(&a_addr).await;
    for i in 31..=33 {
        ca.sql(&format!("INSERT INTO t VALUES ({i})")).await;
    }
    drop(ca);

    // d rejoins: exactly that gap must pull incrementally (catchup), not
    // via snapshot adoption (repair).
    let d = spawn_node_handle(&dir, "jtd", &d_addr, vec![a_addr.clone()]).await;
    assert!(
        wait_seen_n(&d_addr, "SELECT COUNT(id) FROM t", "[[33]]", 400).await,
        "rejoined node did not converge to the gap"
    );
    let logs = sync_log_text(&d_addr).await;
    assert!(
        logs.contains("\"event\":\"catchup\""),
        "expected the rejoin to pull the gap incrementally: {logs}"
    );
    assert!(
        !logs.contains("\"event\":\"repair\""),
        "rejoin degraded to snapshot adoption despite fresh positions: {logs}"
    );
    a.abort();
    b.abort();
    d.abort();
}

/// A node with no fan-out target can never have its journal pulled, and
/// appending is an engine write of its own: journaling there doubled
/// every write's fsync cost for nothing. Regression: single-node writes
/// must leave the catch-up journal empty.
#[tokio::test]
async fn single_node_does_not_journal_writes() {
    let dir = tempfile::tempdir().unwrap();
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = format!("127.0.0.1:{}", l.local_addr().unwrap().port());
    drop(l);
    spawn_node(&dir, "solo", &addr, vec![], None).await;

    let mut c = Client::connect(&addr).await;
    let r = c.sql("CREATE TABLE t (id INT PRIMARY KEY)").await;
    assert_eq!(r.frame_type, proto::RESP_AFFECTED, "{}", payload_str(&r));
    for i in 1..=3 {
        let r = c.sql(&format!("INSERT INTO t VALUES ({i})")).await;
        assert_eq!(r.frame_type, proto::RESP_AFFECTED, "{}", payload_str(&r));
    }
    let r = c.sql("SELECT COUNT(*) FROM _cluster_log").await;
    assert_eq!(r.frame_type, proto::RESP_ROWS, "{}", payload_str(&r));
    assert!(
        payload_str(&r).contains("[[0]]"),
        "single-node writes must not journal: {}",
        payload_str(&r)
    );
}

/// Server with automatic backups enabled on a 1s cadence. Returns
/// (data dir, default backup dir, addr) — the backup dir is derived from
/// the db path exactly like production (`<db dir>/backups`).
async fn start_server_backup(keep: usize) -> (tempfile::TempDir, std::path::PathBuf, String) {
    let dir = tempfile::tempdir().unwrap();
    let backups = dir.path().join("backups");
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
        async_commit: false,
        catchup_window: 0,
        backup_interval_secs: 1,
        backup_keep: keep,
        backup_dir: None,
        statement_timeout_ms: 0,
    };
    tokio::spawn(docsql_server::run(cfg));
    for _ in 0..100 {
        if TcpStream::connect(&addr).await.is_ok() {
            return (dir, backups, addr);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("server did not come up");
}

/// Backup file names in `dir`, newest first (the UTC stamp is fixed-width,
/// so name order is time order).
fn backup_names(dir: &std::path::Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|n| n.starts_with("backup-") && n.ends_with(".sql"))
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names.reverse();
    names
}

async fn backup_list(addr: &str, token: Option<&str>) -> serde_json::Value {
    let mut c = Client::connect(addr).await;
    if let Some(t) = token {
        c.auth(t).await;
    }
    c.send(&Frame::new(
        proto::REQ_BACKUP,
        br#"{"action":"list"}"#.to_vec(),
    ))
    .await;
    let r = c.recv().await;
    assert_eq!(r.frame_type, proto::RESP_BACKUP, "{}", payload_str(&r));
    serde_json::from_slice(&r.payload).unwrap()
}

/// Automatic backups: the timer dumps the full logical state on a cadence
/// (the first tick fires immediately, so the file materializes without any
/// client traffic), retention prunes to `keep`, REQ_STATUS reports the
/// state, and the attempt lands in the sync log for the console's logs page.
#[tokio::test]
async fn backup_periodic_with_retention_and_status() {
    let (_dir, backups, addr) = start_server_backup(2).await;
    let mut c = Client::connect(&addr).await;
    c.sql("CREATE TABLE s (id INT PRIMARY KEY, v TEXT)").await;
    c.sql("INSERT INTO s VALUES (1, 'hello')").await;

    // The earliest tick can land before the writes; a later one must carry
    // them. Retention keeps exactly `keep` files once the third backup exists.
    let mut newest = String::new();
    let mut names: Vec<String> = Vec::new();
    for _ in 0..250 {
        names = backup_names(&backups);
        if let Some(n) = names.first() {
            newest = std::fs::read_to_string(backups.join(n)).unwrap_or_default();
            if newest.contains("hello") && names.len() == 2 {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        newest.contains("CREATE TABLE") && newest.contains("hello"),
        "no backup carries the data; files: {names:?}"
    );
    assert_eq!(
        names.len(),
        2,
        "retention did not prune to keep=2: {names:?}"
    );
    // No temp residue: `.tmp` side files never count as backups. The only
    // legitimate contents are backups and their .sha256 sidecars.
    assert!(
        std::fs::read_dir(&backups)
            .unwrap()
            .filter_map(|e| e.ok())
            .all(|e| {
                let n = e.file_name().to_string_lossy().into_owned();
                n.ends_with(".sql") || n.ends_with(".sql.sha256")
            }),
        "stray files in backup dir"
    );

    let v = backup_list(&addr, None).await;
    assert_eq!(v["interval_secs"], 1);
    assert_eq!(v["keep"], 2);
    assert_eq!(v["count"], 2);
    assert_eq!(v["last"]["ok"], true);

    c.send(&Frame::new(proto::REQ_STATUS, vec![])).await;
    let r = c.recv().await;
    let v: serde_json::Value = serde_json::from_slice(&r.payload).unwrap();
    assert_eq!(v["backup"]["interval_secs"], 1);
    assert_eq!(v["backup"]["count"], 2);
    assert_eq!(v["backup"]["last"]["ok"], true);

    let logs = sync_log_text(&addr).await;
    assert!(logs.contains("\"backup\""), "no backup event: {logs}");
}

/// REQ_BACKUP trigger: runs one backup immediately on demand (interval off),
/// and read-only tokens may list but not trigger.
#[tokio::test]
async fn backup_trigger_over_wire() {
    let (_dir, addr) = start_server_sec(Some("s3cret"), Some("readonly1"), None, 0, 0, 10).await;
    let mut c = Client::connect(&addr).await;
    c.auth("s3cret").await;
    c.sql("CREATE TABLE s (id INT PRIMARY KEY, v TEXT)").await;
    c.sql("INSERT INTO s VALUES (1, 'snap')").await;

    // Read-only: list allowed, trigger refused.
    let mut ro = Client::connect(&addr).await;
    ro.auth("readonly1").await;
    ro.send(&Frame::new(
        proto::REQ_BACKUP,
        br#"{"action":"list"}"#.to_vec(),
    ))
    .await;
    let r = ro.recv().await;
    assert_eq!(r.frame_type, proto::RESP_BACKUP);
    ro.send(&Frame::new(
        proto::REQ_BACKUP,
        br#"{"action":"trigger"}"#.to_vec(),
    ))
    .await;
    let r = ro.recv().await;
    assert_eq!(r.frame_type, proto::RESP_ERROR);
    assert!(
        payload_str(&r).contains("read-only"),
        "unexpected error: {}",
        payload_str(&r)
    );
    ro.send(&Frame::new(
        proto::REQ_BACKUP,
        br#"{"action":"restore","file":"backup-1.sql"}"#.to_vec(),
    ))
    .await;
    let r = ro.recv().await;
    assert_eq!(r.frame_type, proto::RESP_ERROR);
    assert!(
        payload_str(&r).contains("read-only"),
        "unexpected error: {}",
        payload_str(&r)
    );

    // Trigger: acknowledged at once, the file lands asynchronously.
    c.send(&Frame::new(
        proto::REQ_BACKUP,
        br#"{"action":"trigger"}"#.to_vec(),
    ))
    .await;
    let r = c.recv().await;
    assert_eq!(r.frame_type, proto::RESP_AFFECTED, "{}", payload_str(&r));

    let dir = backup_list(&addr, Some("s3cret")).await["dir"]
        .as_str()
        .unwrap()
        .to_string();
    let dir = std::path::PathBuf::from(dir);
    let mut done = false;
    for _ in 0..250 {
        let names = backup_names(&dir);
        if let Some(n) = names.first() {
            if std::fs::read_to_string(dir.join(n))
                .unwrap_or_default()
                .contains("snap")
            {
                done = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(done, "triggered backup never appeared");
    let v = backup_list(&addr, Some("s3cret")).await;
    assert_eq!(v["count"], 1);
    assert_eq!(v["last"]["ok"], true);
    assert_eq!(v["interval_secs"], 0);

    // Unknown action and malformed payloads are explicit errors.
    let mut c2 = Client::connect(&addr).await;
    c2.auth("s3cret").await;
    c2.send(&Frame::new(
        proto::REQ_BACKUP,
        br#"{"action":"snapshot"}"#.to_vec(),
    ))
    .await;
    assert_eq!(c2.recv().await.frame_type, proto::RESP_ERROR);
    c2.send(&Frame::new(proto::REQ_BACKUP, b"not json".to_vec()))
        .await;
    assert_eq!(c2.recv().await.frame_type, proto::RESP_ERROR);
}

/// REQ_BACKUP restore: replays a backup file through the write path — a
/// table dropped after the backup comes back with its rows; traversal and
/// missing-file names are refused.
#[tokio::test]
async fn backup_restore_round_trip() {
    let (_dir, addr) = start_server(Some("s3cret")).await;
    let mut c = Client::connect(&addr).await;
    c.auth("s3cret").await;
    c.sql("CREATE TABLE s (id INT PRIMARY KEY, v TEXT)").await;
    c.sql("INSERT INTO s VALUES (1, 'snap-1'), (2, 'snap-2')")
        .await;
    // FK pair with a default: the restore replays over LIVE data, and the
    // recreated child used to block the old parent's DROP (FK guard) —
    // aborting every FK-bearing restore halfway.
    c.sql("CREATE TABLE users (id INT PRIMARY KEY, name TEXT DEFAULT 'anon')")
        .await;
    c.sql("CREATE TABLE orders (oid INT PRIMARY KEY, uid INT, FOREIGN KEY (uid) REFERENCES users (id))")
        .await;
    c.sql("INSERT INTO users VALUES (1, 'u1')").await;
    c.sql("INSERT INTO orders VALUES (100, 1)").await;

    // Take a backup carrying the rows.
    c.send(&Frame::new(
        proto::REQ_BACKUP,
        br#"{"action":"trigger"}"#.to_vec(),
    ))
    .await;
    assert_eq!(c.recv().await.frame_type, proto::RESP_AFFECTED);
    let dir = backup_list(&addr, Some("s3cret")).await["dir"]
        .as_str()
        .unwrap()
        .to_string();
    let dir = std::path::PathBuf::from(dir);
    let mut name = String::new();
    for _ in 0..250 {
        let names = backup_names(&dir);
        if let Some(n) = names.first() {
            if std::fs::read_to_string(dir.join(n))
                .unwrap_or_default()
                .contains("snap-2")
            {
                name = n.clone();
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(!name.is_empty(), "backup with data never appeared");

    // Damage the state: drop the backed-up table, add a post-backup one.
    // The FK pair stays LIVE on purpose: the restore must replace both
    // tables through their live FK references without tripping the guard.
    c.sql("DROP TABLE s").await;
    c.sql("CREATE TABLE after_bk (id INT PRIMARY KEY)").await;
    c.sql("INSERT INTO after_bk VALUES (7)").await;

    // Refusals: traversal name, wrong shape, missing file.
    for bad in [
        r#"{"action":"restore","file":"../docsql.db"}"#,
        r#"{"action":"restore"}"#,
        r#"{"action":"restore","file":"backup-nope.sql"}"#,
    ] {
        c.send(&Frame::new(proto::REQ_BACKUP, bad.as_bytes().to_vec()))
            .await;
        let r = c.recv().await;
        assert_eq!(r.frame_type, proto::RESP_ERROR, "expected refusal: {bad}");
    }

    // Restore: acknowledged at once, outcome polled via list. The status
    // reports progress: every statement of the script (DROP + CREATE +
    // one INSERT per row) applied exactly once.
    let payload = format!(r#"{{"action":"restore","file":"{name}"}}"#);
    c.send(&Frame::new(proto::REQ_BACKUP, payload.into_bytes()))
        .await;
    let r = c.recv().await;
    assert_eq!(r.frame_type, proto::RESP_AFFECTED, "{}", payload_str(&r));

    let mut applied = 0u64;
    let mut total = 0u64;
    let mut converged = String::from("missing");
    for _ in 0..250 {
        let v = backup_list(&addr, Some("s3cret")).await;
        if let Some(rs) = v["restore"].as_object() {
            if rs["running"] == false && rs["ok"] == true && rs["file"] == name {
                applied = rs["applied"].as_u64().unwrap_or(0);
                total = rs["total"].as_u64().unwrap_or(0);
                converged = rs["converged"].to_string();
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(applied > 0, "restore never completed");
    assert_eq!(applied, total, "progress != script size");
    assert!(total >= 3, "suspiciously small script: {total} statements");
    assert_eq!(
        converged, "true",
        "single-node restore must verify converged"
    );

    // The dropped table is back with its backup rows...
    let r = c.sql("SELECT v FROM s WHERE id = 2").await;
    assert_eq!(r.frame_type, proto::RESP_ROWS, "{}", payload_str(&r));
    assert!(payload_str(&r).contains("snap-2"));
    let r = c.sql("SELECT COUNT(id) FROM s").await;
    assert!(payload_str(&r).contains("[[2]]"), "{}", payload_str(&r));
    // The FK pair was replaced through live references, rows intact.
    let r = c.sql("SELECT oid FROM orders").await;
    assert!(payload_str(&r).contains("100"), "{}", payload_str(&r));
    let r = c.sql("SELECT name FROM users WHERE id = 1").await;
    assert!(payload_str(&r).contains("u1"), "{}", payload_str(&r));
    // ...while the post-backup table survived (documented semantics).
    let r = c.sql("SELECT COUNT(id) FROM after_bk").await;
    assert!(payload_str(&r).contains("[[1]]"), "{}", payload_str(&r));

    // The restore attempt is audited in the sync log.
    let mut cl = Client::connect(&addr).await;
    cl.auth("s3cret").await;
    cl.send(&Frame::new(proto::REQ_LOGS, vec![])).await;
    let logs = String::from_utf8_lossy(&cl.recv().await.payload).to_string();
    assert!(logs.contains("\"restore\""), "no restore event: {logs}");
}

/// Integrity gate: a dump whose bytes no longer match its `.sha256`
/// sidecar is refused before any replay — the restore status reports the
/// failure, the sync log carries it, and the live database keeps whatever
/// state it had (no half-applied snapshot). A sidecar-less legacy file
/// still restores.
#[tokio::test]
async fn backup_restore_refuses_corrupted_file() {
    let (_dir, backups, addr) = start_server_backup(3).await;
    let mut c = Client::connect(&addr).await;
    c.sql("CREATE TABLE s (id INT PRIMARY KEY, v TEXT)").await;
    c.sql("INSERT INTO s VALUES (1, 'keep-me')").await;

    // Trigger a backup and wait for the file together with its sidecar
    // (the pair is renamed in sequence; restore polls need both).
    c.send(&Frame::new(
        proto::REQ_BACKUP,
        br#"{"action":"trigger"}"#.to_vec(),
    ))
    .await;
    assert_eq!(c.recv().await.frame_type, proto::RESP_AFFECTED);
    let mut name = String::new();
    for _ in 0..250 {
        if let Some(n) = backup_names(&backups).first() {
            if backups.join(format!("{n}.sha256")).is_file()
                && std::fs::read_to_string(backups.join(n))
                    .unwrap_or_default()
                    .contains("keep-me")
            {
                name = n.clone();
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(!name.is_empty(), "backup never appeared");

    // The list marks the fresh backup as checksum-covered.
    let v = backup_list(&addr, None).await;
    let entry = v["files"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["name"] == name.as_str())
        .unwrap_or_else(|| panic!("backup {name} missing from list: {v}"));
    assert_eq!(entry["checksum"], serde_json::Value::Bool(true));

    // Corrupt the dump behind the sidecar's back; remember the original
    // bytes for the legacy-tolerance leg below.
    let original = std::fs::read_to_string(backups.join(&name)).unwrap();
    std::fs::write(backups.join(&name), "-- tampered\n".to_string() + &original).unwrap();

    // Damage the live state: a refused restore must leave this drop in
    // place — the checksum check fires before any statement is replayed.
    c.sql("DROP TABLE s").await;

    let payload = format!(r#"{{"action":"restore","file":"{name}"}}"#);
    c.send(&Frame::new(proto::REQ_BACKUP, payload.clone().into_bytes()))
        .await;
    assert_eq!(c.recv().await.frame_type, proto::RESP_AFFECTED);

    let mut err = String::new();
    for _ in 0..250 {
        let v = backup_list(&addr, None).await;
        if let Some(rs) = v["restore"].as_object() {
            if rs["running"] == false && rs["file"] == name {
                err = rs["error"].as_str().unwrap_or_default().to_string();
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(err.contains("failed checksum"), "restore error: {err}");
    // Nothing was replayed: the drop stands.
    let r = c.sql("SELECT COUNT(*) FROM s").await;
    assert_eq!(r.frame_type, proto::RESP_ERROR, "{}", payload_str(&r));

    // The refusal is audited like any restore outcome.
    let mut cl = Client::connect(&addr).await;
    cl.send(&Frame::new(proto::REQ_LOGS, vec![])).await;
    let logs = String::from_utf8_lossy(&cl.recv().await.payload).to_string();
    assert!(logs.contains("failed checksum"), "no refusal event: {logs}");

    // Legacy tolerance: the same file without a sidecar restores fine.
    std::fs::write(backups.join(&name), original).unwrap();
    std::fs::remove_file(backups.join(format!("{name}.sha256"))).unwrap();
    c.send(&Frame::new(proto::REQ_BACKUP, payload.into_bytes()))
        .await;
    assert_eq!(c.recv().await.frame_type, proto::RESP_AFFECTED);
    let mut ok = false;
    for _ in 0..250 {
        let v = backup_list(&addr, None).await;
        if let Some(rs) = v["restore"].as_object() {
            if rs["running"] == false && rs["file"] == name && rs["ok"] == true {
                ok = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(ok, "sidecar-less restore never completed");
    let r = c.sql("SELECT COUNT(*) FROM s").await;
    assert!(payload_str(&r).contains("[[1]]"), "{}", payload_str(&r));
}

/// Restore's headline property is cluster convergence: replaying the
/// snapshot on one node fans every statement out to the peers, so writes
/// made everywhere after the backup are replaced by the backup's state
/// cluster-wide.
#[tokio::test]
async fn backup_restore_converges_the_cluster() {
    let dir = tempfile::tempdir().unwrap();
    let free = || {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        format!("127.0.0.1:{p}")
    };
    let (a_addr, b_addr) = (free(), free());
    let a = spawn_node_handle(&dir, "ra", &a_addr, vec![b_addr.clone()]).await;
    let b = spawn_node_handle(&dir, "rb", &b_addr, vec![a_addr.clone()]).await;

    // Base state, replicated.
    let mut ca = Client::connect(&a_addr).await;
    ca.sql("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").await;
    ca.sql("INSERT INTO t VALUES (1, 'base')").await;
    assert!(
        wait_seen_n(&b_addr, "SELECT COUNT(id) FROM t", "[[1]]", 200).await,
        "base row did not reach b"
    );

    // Backup on a, then diverge from it on both nodes. The trigger is
    // refused while the node's startup sync gate is open ("retry later") —
    // in release timing the bootstrap probe loop can still be running here,
    // so honor the refusal and retry instead of failing.
    let mut triggered = false;
    for _ in 0..250 {
        ca.send(&Frame::new(
            proto::REQ_BACKUP,
            br#"{"action":"trigger"}"#.to_vec(),
        ))
        .await;
        let resp = ca.recv().await;
        if resp.frame_type == proto::RESP_AFFECTED {
            triggered = true;
            break;
        }
        let msg = String::from_utf8_lossy(&resp.payload).into_owned();
        assert!(
            msg.contains("startup sync"),
            "backup trigger refused unexpectedly: {msg}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        triggered,
        "backup trigger never accepted (startup sync never closed)"
    );
    let backups = dir.path().join("backups");
    let mut name = String::new();
    for _ in 0..250 {
        let names = backup_names(&backups);
        if let Some(n) = names.first() {
            if std::fs::read_to_string(backups.join(n))
                .unwrap_or_default()
                .contains("base")
            {
                name = n.clone();
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(!name.is_empty(), "backup never appeared");
    ca.sql("INSERT INTO t VALUES (2, 'post-a')").await;
    let mut cb = Client::connect(&b_addr).await;
    cb.sql("INSERT INTO t VALUES (3, 'post-b')").await;
    assert!(
        wait_seen_n(&a_addr, "SELECT COUNT(id) FROM t", "[[3]]", 200).await,
        "post-backup writes did not replicate both ways"
    );

    // Restore on a: both nodes end up at the backup's state.
    let payload = format!(r#"{{"action":"restore","file":"{name}"}}"#);
    ca.send(&Frame::new(proto::REQ_BACKUP, payload.clone().into_bytes()))
        .await;
    assert_eq!(
        ca.recv().await.frame_type,
        proto::RESP_AFFECTED,
        "{}",
        payload
    );
    assert!(
        wait_seen_n(&a_addr, "SELECT v FROM t WHERE id = 1", "base", 400).await,
        "restored row missing on a"
    );
    assert!(
        wait_seen_n(&b_addr, "SELECT v FROM t WHERE id = 1", "base", 400).await,
        "restore did not reach b"
    );
    let ra = ca.sql("SELECT COUNT(id) FROM t").await;
    assert!(
        payload_str(&ra).contains("[[1]]"),
        "a: {}",
        payload_str(&ra)
    );
    let rb = cb.sql("SELECT COUNT(id) FROM t").await;
    assert!(
        payload_str(&rb).contains("[[1]]"),
        "b: {}",
        payload_str(&rb)
    );
    a.abort();
    b.abort();
}

/// Backup and restore contend for the same write path: while a restore
/// replays, a backup trigger (timer or manual) and a second restore are
/// refused; afterwards both work again.
#[tokio::test]
async fn backup_and_restore_are_mutually_exclusive() {
    let (_dir, addr) = start_server(Some("s3cret")).await;
    let mut c = Client::connect(&addr).await;
    c.auth("s3cret").await;
    c.sql("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").await;
    // One fat statement keeps the test fast; the dump still yields one
    // INSERT per row, so replaying takes long enough to make the busy
    // window deterministic.
    let mut values = String::new();
    for i in 1..=3000 {
        values.push_str(&format!("({i}, 'v{i}'),"));
    }
    values.pop(); // trailing comma
    c.sql(&format!("INSERT INTO t VALUES {values}")).await;

    c.send(&Frame::new(
        proto::REQ_BACKUP,
        br#"{"action":"trigger"}"#.to_vec(),
    ))
    .await;
    assert_eq!(c.recv().await.frame_type, proto::RESP_AFFECTED);
    let dir = backup_list(&addr, Some("s3cret")).await["dir"]
        .as_str()
        .unwrap()
        .to_string();
    let dir = std::path::PathBuf::from(dir);
    let mut name = String::new();
    for _ in 0..500 {
        let names = backup_names(&dir);
        if let Some(n) = names.first() {
            if std::fs::read_to_string(dir.join(n))
                .unwrap_or_default()
                .contains("v3000")
            {
                name = n.clone();
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(!name.is_empty(), "backup never appeared");

    // Start the restore; while it replays, both a backup trigger and a
    // second restore are refused (thousands of fsync'd statements give a
    // wide busy window).
    let payload = format!(r#"{{"action":"restore","file":"{name}"}}"#);
    c.send(&Frame::new(proto::REQ_BACKUP, payload.clone().into_bytes()))
        .await;
    assert_eq!(c.recv().await.frame_type, proto::RESP_AFFECTED);
    c.send(&Frame::new(
        proto::REQ_BACKUP,
        br#"{"action":"trigger"}"#.to_vec(),
    ))
    .await;
    let r = c.recv().await;
    assert_eq!(r.frame_type, proto::RESP_ERROR, "backup during restore");
    assert!(
        payload_str(&r).contains("already in progress"),
        "{}",
        payload_str(&r)
    );
    c.send(&Frame::new(proto::REQ_BACKUP, payload.into_bytes()))
        .await;
    let r = c.recv().await;
    assert_eq!(r.frame_type, proto::RESP_ERROR, "double restore");

    // The restore completes and the node is functional again.
    let mut ok = false;
    for _ in 0..2000 {
        let v = backup_list(&addr, Some("s3cret")).await;
        if let Some(rs) = v["restore"].as_object() {
            if rs["running"] == false && rs["ok"] == true {
                ok = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(ok, "restore never completed");
    let r = c.sql("SELECT COUNT(id) FROM t").await;
    assert!(
        payload_str(&r).contains("[[3000]]"),
        "restored rows missing: {}",
        payload_str(&r)
    );
    c.send(&Frame::new(
        proto::REQ_BACKUP,
        br#"{"action":"trigger"}"#.to_vec(),
    ))
    .await;
    assert_eq!(c.recv().await.frame_type, proto::RESP_AFFECTED);
}

/// A backup file whose script fails partway: a parse-broken script is
/// rejected wholesale before anything runs, and an execution failure
/// (duplicate primary key) stops the replay at that statement — the
/// status and sync log attribute the failure, statements before it are
/// applied (partial replay is documented), and the node keeps serving.
#[tokio::test]
async fn backup_restore_failure_is_reported_and_audited() {
    let (_dir, addr) = start_server(Some("s3cret")).await;
    let dir = backup_list(&addr, Some("s3cret")).await["dir"]
        .as_str()
        .unwrap()
        .to_string();
    let dir = std::path::PathBuf::from(dir);
    std::fs::create_dir_all(&dir).unwrap();

    let mut c = Client::connect(&addr).await;
    c.auth("s3cret").await;

    // Parse-broken: nothing runs at all (the whole script is pre-parsed).
    std::fs::write(
        dir.join("backup-broken.sql"),
        "CREATE TABLE ok1 (id INT PRIMARY KEY);\nTHIS IS NOT SQL;\n",
    )
    .unwrap();
    let payload = br#"{"action":"restore","file":"backup-broken.sql"}"#.to_vec();
    c.send(&Frame::new(proto::REQ_BACKUP, payload.clone()))
        .await;
    assert_eq!(c.recv().await.frame_type, proto::RESP_AFFECTED);
    for _ in 0..250 {
        let v = backup_list(&addr, Some("s3cret")).await;
        if let Some(rs) = v["restore"].as_object() {
            if rs["running"] == false {
                assert_eq!(rs["ok"], false);
                assert!(
                    rs["error"].as_str().unwrap().contains("restore parse"),
                    "{}",
                    rs["error"].as_str().unwrap()
                );
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let r = c.sql("SELECT COUNT(id) FROM ok1").await;
    assert_eq!(r.frame_type, proto::RESP_ERROR, "nothing must be applied");

    // Execution-broken: the replay stops at the failing statement.
    std::fs::write(
        dir.join("backup-half.sql"),
        "CREATE TABLE ok1 (id INT PRIMARY KEY);\n\
         INSERT INTO ok1 VALUES (1);\n\
         INSERT INTO ok1 VALUES (1);\n",
    )
    .unwrap();
    let payload = br#"{"action":"restore","file":"backup-half.sql"}"#.to_vec();
    c.send(&Frame::new(proto::REQ_BACKUP, payload)).await;
    assert_eq!(c.recv().await.frame_type, proto::RESP_AFFECTED);

    let mut err = String::new();
    let mut applied = 0u64;
    for _ in 0..500 {
        let v = backup_list(&addr, Some("s3cret")).await;
        if let Some(rs) = v["restore"].as_object() {
            if rs["running"] == false {
                err = rs["error"].as_str().unwrap_or_default().to_string();
                applied = rs["applied"].as_u64().unwrap_or(0);
                assert_eq!(rs["ok"], false, "broken script must not restore ok");
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        err.contains("statement 3"),
        "failure not attributed to the broken statement: {err}"
    );
    assert_eq!(applied, 2, "progress lost on failure");

    // Statements before the failure are applied; the audit trail records
    // the failed restore with its reason.
    let r = c.sql("SELECT COUNT(id) FROM ok1").await;
    assert!(payload_str(&r).contains("[[1]]"), "{}", payload_str(&r));
    c.send(&Frame::new(proto::REQ_LOGS, vec![])).await;
    let logs = String::from_utf8_lossy(&c.recv().await.payload).to_string();
    assert!(
        logs.contains("\"restore\"") && logs.contains("statement 3"),
        "failed restore not audited: {logs}"
    );
}

/// A read-only replica refuses restores (every replayed statement would be
/// refused anyway) while backups stay allowed — a dump is read-only.
#[tokio::test]
async fn backup_restore_refused_on_read_only_replica() {
    let dir = tempfile::tempdir().unwrap();
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    let addr = format!("127.0.0.1:{port}");
    tokio::spawn(docsql_server::run(docsql_server::ServerConfig {
        db_path: dir.path().join("replica.db"),
        listen: addr.clone(),
        auth_token: Some("s3cret".into()),
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
        catchup_window: 0,
        backup_interval_secs: 0,
        backup_keep: 7,
        backup_dir: None,
        statement_timeout_ms: 0,
    }));
    for _ in 0..100 {
        if TcpStream::connect(&addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mut c = Client::connect(&addr).await;
    c.auth("s3cret").await;

    c.send(&Frame::new(
        proto::REQ_BACKUP,
        br#"{"action":"restore","file":"backup-1.sql"}"#.to_vec(),
    ))
    .await;
    let r = c.recv().await;
    assert_eq!(r.frame_type, proto::RESP_ERROR);
    assert!(
        payload_str(&r).contains("read-only replica"),
        "{}",
        payload_str(&r)
    );

    // Backups remain allowed on a replica.
    c.send(&Frame::new(
        proto::REQ_BACKUP,
        br#"{"action":"trigger"}"#.to_vec(),
    ))
    .await;
    assert_eq!(c.recv().await.frame_type, proto::RESP_AFFECTED);
}

/// Restore statements queue behind an open client transaction like every
/// other write: the replay waits it out and proceeds once it closes — it
/// never interleaves into a transaction that could still roll back.
#[tokio::test]
async fn backup_restore_waits_for_open_transaction() {
    let (_dir, addr) = start_server(Some("s3cret")).await;
    let mut c = Client::connect(&addr).await;
    c.auth("s3cret").await;
    c.sql("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").await;
    c.sql("INSERT INTO t VALUES (1, 'snap')").await;

    // Backup first (with no transaction open), then open a transaction —
    // its writes are buffered and only commit at COMMIT.
    c.send(&Frame::new(
        proto::REQ_BACKUP,
        br#"{"action":"trigger"}"#.to_vec(),
    ))
    .await;
    assert_eq!(c.recv().await.frame_type, proto::RESP_AFFECTED);
    let dir = backup_list(&addr, Some("s3cret")).await["dir"]
        .as_str()
        .unwrap()
        .to_string();
    let dir = std::path::PathBuf::from(dir);
    let mut name = String::new();
    for _ in 0..250 {
        let names = backup_names(&dir);
        if let Some(n) = names.first() {
            if std::fs::read_to_string(dir.join(n))
                .unwrap_or_default()
                .contains("snap")
            {
                name = n.clone();
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(!name.is_empty(), "backup never appeared");

    let mut tx = Client::connect(&addr).await;
    tx.auth("s3cret").await;
    tx.sql("BEGIN").await;
    tx.sql("INSERT INTO t VALUES (2, 'uncommitted')").await;

    // The restore's first statement queues behind the open transaction.
    let payload = format!(r#"{{"action":"restore","file":"{name}"}}"#);
    c.send(&Frame::new(proto::REQ_BACKUP, payload.into_bytes()))
        .await;
    assert_eq!(c.recv().await.frame_type, proto::RESP_AFFECTED);
    tokio::time::sleep(Duration::from_millis(300)).await;
    // While the transaction is open the restore cannot have finished.
    let v = backup_list(&addr, Some("s3cret")).await;
    if let Some(rs) = v["restore"].as_object() {
        if rs["running"] == false && rs["ok"] == true {
            panic!("restore ran inside someone else's open transaction");
        }
    }

    // Closing the transaction unblocks the replay.
    tx.sql("ROLLBACK").await;
    let mut done = false;
    for _ in 0..500 {
        let v = backup_list(&addr, Some("s3cret")).await;
        if let Some(rs) = v["restore"].as_object() {
            if rs["running"] == false && rs["ok"] == true {
                done = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(done, "restore never proceeded after ROLLBACK");
    // The rolled-back write stays gone; the backup state is intact.
    let r = c.sql("SELECT COUNT(id) FROM t").await;
    assert!(payload_str(&r).contains("[[1]]"), "{}", payload_str(&r));
}

/// `DOCSQL_BACKUP_DIR` moves the backups out of the default `<db
/// dir>/backups` location; the node reports and uses exactly that path.
#[tokio::test]
async fn backup_dir_override_is_honored() {
    let dir = tempfile::tempdir().unwrap();
    let snaps = dir.path().join("snaps");
    let default_backups = dir.path().join("backups");
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    let addr = format!("127.0.0.1:{port}");
    tokio::spawn(docsql_server::run(docsql_server::ServerConfig {
        db_path: dir.path().join("e2e.db"),
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
        backup_interval_secs: 0,
        backup_keep: 7,
        backup_dir: Some(snaps.clone()),
        statement_timeout_ms: 0,
    }));
    for _ in 0..100 {
        if TcpStream::connect(&addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mut c = Client::connect(&addr).await;
    c.sql("CREATE TABLE t (id INT PRIMARY KEY)").await;

    c.send(&Frame::new(
        proto::REQ_BACKUP,
        br#"{"action":"trigger"}"#.to_vec(),
    ))
    .await;
    assert_eq!(c.recv().await.frame_type, proto::RESP_AFFECTED);
    let mut listed_dir = String::new();
    let mut ok = false;
    for _ in 0..250 {
        let v = backup_list(&addr, None).await;
        if v["last"]["ok"] == true {
            listed_dir = v["dir"].as_str().unwrap().to_string();
            ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(ok, "backup never completed");
    assert_eq!(std::path::Path::new(&listed_dir), snaps.as_path());
    assert_eq!(backup_names(&snaps).len(), 1);
    // The default location is never created behind the override's back.
    assert!(!default_backups.exists(), "default backups dir appeared");
}

// ---- database users, roles and privileges ----

/// REQ_AUTH_USER login helper (JSON payload, like the dotnet client sends).
async fn user_login(c: &mut Client, user: &str, password: &str) -> Frame {
    let body = serde_json::json!({"user": user, "password": password});
    c.send(&Frame::new(
        proto::REQ_AUTH_USER,
        body.to_string().into_bytes(),
    ))
    .await;
    c.recv().await
}

#[tokio::test]
async fn users_roles_and_the_privilege_matrix() {
    let (_dir, addr) = start_server(None).await;
    let mut admin = Client::connect(&addr).await;
    // No client token + no users = legacy open (admin) access.
    admin
        .sql("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .await;
    for i in 0..3 {
        admin
            .sql(&format!("INSERT INTO t VALUES ({i}, 'v{i}')"))
            .await;
    }
    let pw = ["ro", "le", "pw", "12", "34"].concat();
    let pw2 = ["rw", "pw", "12", "34", "56"].concat();
    admin
        .sql(&format!("CREATE USER robyn PASSWORD '{pw}'"))
        .await;
    admin
        .sql(&format!("CREATE USER wally PASSWORD '{pw2}'"))
        .await;
    admin.sql("GRANT readonly TO robyn").await;
    admin.sql("GRANT readwrite TO wally").await;

    // readonly: SELECT yes, writes/DDL/user management no.
    let mut ro = Client::connect(&addr).await;
    let f = user_login(&mut ro, "robyn", &pw).await;
    assert_eq!(f.frame_type, proto::RESP_AFFECTED, "{}", payload_str(&f));
    let f = ro.sql("SELECT COUNT(id) FROM t").await;
    assert!(payload_str(&f).contains("[[3]]"), "{}", payload_str(&f));
    let f = ro.sql("INSERT INTO t VALUES (9, 'x')").await;
    assert_eq!(f.frame_type, proto::RESP_ERROR, "readonly INSERT must fail");
    let f = ro.sql("CREATE TABLE nope (a INT)").await;
    assert_eq!(f.frame_type, proto::RESP_ERROR, "readonly DDL must fail");
    let f = ro.sql("CREATE USER intruder PASSWORD 'whatever12'").await;
    assert_eq!(f.frame_type, proto::RESP_ERROR, "user mgmt must fail");
    // user/role tables are admin-only even for SELECT
    let f = ro.sql("SELECT name FROM docsql_users").await;
    assert_eq!(f.frame_type, proto::RESP_ERROR);

    // readwrite: DML yes, DDL/user mgmt no, PUBLISH yes.
    let mut rw = Client::connect(&addr).await;
    let f = user_login(&mut rw, "wally", &pw2).await;
    assert_eq!(f.frame_type, proto::RESP_AFFECTED);
    let f = rw.sql("INSERT INTO t VALUES (9, 'x')").await;
    assert_eq!(f.frame_type, proto::RESP_AFFECTED, "{}", payload_str(&f));
    let f = rw.sql("UPDATE t SET v = 'y' WHERE id = 9").await;
    assert_eq!(f.frame_type, proto::RESP_AFFECTED);
    let f = rw.sql("DROP TABLE t").await;
    assert_eq!(f.frame_type, proto::RESP_ERROR, "readwrite DDL must fail");
    let f = rw.sql("GRANT admin TO wally").await;
    assert_eq!(f.frame_type, proto::RESP_ERROR);
    // Even a readwrite user cannot touch the internal user tables with
    // plain DML — the statement family is the only door.
    let f = rw.sql("INSERT INTO docsql_users VALUES ('x', 'y')").await;
    assert_eq!(f.frame_type, proto::RESP_ERROR, "user-table DML");
    let f = rw.sql("DELETE FROM docsql_users").await;
    assert_eq!(f.frame_type, proto::RESP_ERROR, "user-table DML");

    // PUBLISH is a durable write: readonly refused, readwrite allowed.
    let f = ro.publish("ch", "nope").await;
    assert_eq!(f.frame_type, proto::RESP_ERROR, "readonly PUBLISH");
    assert!(payload_str(&f).contains("readwrite"), "{}", payload_str(&f));
    let f = rw.publish("ch", "from-wally").await;
    assert_eq!(f.frame_type, proto::RESP_ROWS, "{}", payload_str(&f));
    // TRIM deletes persisted messages: readonly refused, readwrite allowed.
    let f = ro
        .pubsub_cmd(r#"{"sub":"trim","channel":"ch","keep":1}"#)
        .await;
    assert_eq!(f.frame_type, proto::RESP_ERROR, "readonly TRIM");
    let f = rw
        .pubsub_cmd(r#"{"sub":"trim","channel":"ch","keep":1}"#)
        .await;
    assert_eq!(f.frame_type, proto::RESP_AFFECTED, "{}", payload_str(&f));

    // wrong password / unknown user are indistinguishable
    let mut bad = Client::connect(&addr).await;
    let wrong = ["wr", "on", "gp", "w!"].concat();
    let f = user_login(&mut bad, "robyn", &wrong).await;
    assert_eq!(f.frame_type, proto::RESP_ERROR);
    let f = user_login(&mut bad, "ghost", &pw).await;
    assert_eq!(f.frame_type, proto::RESP_ERROR);

    // Once a user exists, anonymous access closes.
    let mut anon = Client::connect(&addr).await;
    let f = anon.sql("SELECT 1").await;
    assert_eq!(f.frame_type, proto::RESP_ERROR, "anonymous must close");
    assert!(payload_str(&f).contains("authentication required"));

    // Table grants via a custom role.
    admin.sql("CREATE ROLE clerk").await;
    admin.sql("CREATE USER cara PASSWORD 'carapw99'").await;
    admin.sql("GRANT SELECT, UPDATE ON t TO clerk").await;
    admin.sql("GRANT clerk TO cara").await;
    let mut cu = Client::connect(&addr).await;
    let f = user_login(&mut cu, "cara", "carapw99").await;
    assert_eq!(f.frame_type, proto::RESP_AFFECTED);
    let f = cu.sql("SELECT id FROM t WHERE id = 1").await;
    assert_eq!(f.frame_type, proto::RESP_ROWS);
    let f = cu.sql("UPDATE t SET v = 'c' WHERE id = 1").await;
    assert_eq!(f.frame_type, proto::RESP_AFFECTED);
    let f = cu.sql("INSERT INTO t VALUES (50, 'no')").await;
    assert_eq!(f.frame_type, proto::RESP_ERROR, "ungranted INSERT");
    // a subquery against an ungranted table is still denied
    admin.sql("CREATE TABLE secret (x INT)").await;
    admin.sql("INSERT INTO secret VALUES (1)").await;
    let f = cu.sql("SELECT (SELECT MAX(x) FROM secret) AS leak").await;
    assert_eq!(f.frame_type, proto::RESP_ERROR, "subquery leak");

    // Grants land on an already-open connection at its NEXT statement
    // (epoch refresh is synchronous — no delay to wait out).
    admin.sql("GRANT INSERT ON t TO clerk").await;
    let f = cu.sql("INSERT INTO t VALUES (51, 'yes')").await;
    assert_eq!(f.frame_type, proto::RESP_AFFECTED, "{}", payload_str(&f));
    admin.sql("REVOKE INSERT ON t FROM clerk").await;
    let f = cu.sql("INSERT INTO t VALUES (52, 'no')").await;
    assert_eq!(f.frame_type, proto::RESP_ERROR, "table-level revocation");

    // Revoke of the membership itself, same connection again.
    admin.sql("REVOKE clerk FROM cara").await;
    let f = cu.sql("SELECT id FROM t WHERE id = 1").await;
    assert_eq!(f.frame_type, proto::RESP_ERROR, "revocation must apply");

    // DROP USER cascades: cara can no longer log in — and her still-open
    // connection loses access at its next statement.
    admin.sql("DROP USER cara").await;
    let f = cu.sql("SELECT 1").await;
    assert_eq!(f.frame_type, proto::RESP_ERROR, "dropped user's session");
    let mut gone = Client::connect(&addr).await;
    let f = user_login(&mut gone, "cara", "carapw99").await;
    assert_eq!(f.frame_type, proto::RESP_ERROR);

    // ALTER USER password.
    let newpw = ["ne", "wp", "w!", "89"].concat();
    admin
        .sql(&format!("ALTER USER robyn PASSWORD '{newpw}'"))
        .await;
    let mut ro2 = Client::connect(&addr).await;
    let f = user_login(&mut ro2, "robyn", &pw).await;
    assert_eq!(f.frame_type, proto::RESP_ERROR, "old password must die");
    let f = user_login(&mut ro2, "robyn", &newpw).await;
    assert_eq!(f.frame_type, proto::RESP_AFFECTED);
}

#[tokio::test]
async fn user_accounts_replicate_across_the_cluster() {
    let dir = tempfile::tempdir().unwrap();
    let free = || {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        format!("127.0.0.1:{p}")
    };
    let (a_addr, b_addr) = (free(), free());
    spawn_node(&dir, "ua", &a_addr, vec![b_addr.clone()], None).await;
    let mut ca = Client::connect(&a_addr).await;
    ca.sql("CREATE TABLE t (id INT PRIMARY KEY)").await;
    let pw = ["cl", "us", "te", "r1"].concat();
    ca.sql(&format!("CREATE USER dana PASSWORD '{pw}'")).await;
    ca.sql("GRANT readwrite TO dana").await;
    // the fan-out carries the resolved (hashed) statement
    drop(ca);

    spawn_node(&dir, "ub", &b_addr, vec![a_addr.clone()], None).await;
    // B synced the user (join snapshot embeds the user statements) and
    // accepts the login with the SAME password.
    for _ in 0..100 {
        let mut probe = Client::connect(&b_addr).await;
        let f = user_login(&mut probe, "dana", &pw).await;
        if f.frame_type == proto::RESP_AFFECTED {
            // dana's grants replicated too: DML works on B.
            let f = probe.sql("INSERT INTO t VALUES (7)").await;
            assert_eq!(f.frame_type, proto::RESP_AFFECTED, "{}", payload_str(&f));
            drop(probe);
            return;
        }
        drop(probe);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("user never replicated to the peer");
}

#[tokio::test]
async fn user_login_lockout_mirrors_token_auth() {
    let (_dir, addr) = start_server(None).await;
    let mut admin = Client::connect(&addr).await;
    let pw = ["lo", "ck", "me", "12"].concat();
    admin
        .sql(&format!("CREATE USER olga PASSWORD '{pw}'"))
        .await;
    // start_server uses the default threshold (10): 10 failures lock out.
    for _ in 0..10 {
        let mut c = Client::connect(&addr).await;
        let f = user_login(&mut c, "olga", "definitely-wrong").await;
        assert_eq!(f.frame_type, proto::RESP_ERROR);
        drop(c);
    }
    let mut c = Client::connect(&addr).await;
    let f = user_login(&mut c, "olga", &pw).await;
    assert_eq!(
        f.frame_type,
        proto::RESP_ERROR,
        "correct password must be locked out"
    );
    assert!(payload_str(&f).contains("locked"), "{}", payload_str(&f));
}

/// Protocol-level identity and admin guards: a peer connection never
/// authenticates as a database user, one connection holds one identity,
/// and non-admin users are refused backup trigger/restore and PROMOTE.
#[tokio::test]
async fn user_identity_and_admin_protocol_guards() {
    let (_dir, addr) = start_server_tokens(Some("client-tok"), Some("cluster-tok")).await;
    let mut admin = Client::connect(&addr).await;
    assert_eq!(
        admin.auth("client-tok").await.frame_type,
        proto::RESP_AFFECTED
    );
    let pw = ["gu", "ar", "d1", "23"].concat();
    admin
        .sql(&format!("CREATE USER gina PASSWORD '{pw}'"))
        .await;
    admin.sql("GRANT readonly TO gina").await;
    admin.sql("CREATE TABLE t (id INT PRIMARY KEY)").await;

    // A cluster-token (peer) connection cannot log in as a user.
    let mut peer = Client::connect(&addr).await;
    assert_eq!(payload_str(&peer.auth("cluster-tok").await), "ok");
    let f = user_login(&mut peer, "gina", &pw).await;
    assert_eq!(f.frame_type, proto::RESP_ERROR, "peer user login");

    // One identity per connection: token first, then user login → refused.
    let mut mixed = Client::connect(&addr).await;
    assert_eq!(
        mixed.auth("client-tok").await.frame_type,
        proto::RESP_AFFECTED
    );
    let f = user_login(&mut mixed, "gina", &pw).await;
    assert_eq!(f.frame_type, proto::RESP_ERROR, "double authentication");
    assert!(
        payload_str(&f).contains("already authenticated"),
        "{}",
        payload_str(&f)
    );

    // A readonly user session: PUBLISH/TRIM were covered by the matrix
    // test; here the admin-only protocol surface — backup trigger,
    // restore and PROMOTE.
    let mut gina = Client::connect(&addr).await;
    assert_eq!(
        user_login(&mut gina, "gina", &pw).await.frame_type,
        proto::RESP_AFFECTED
    );
    gina.send(&Frame::new(
        proto::REQ_BACKUP,
        br#"{"action":"trigger"}"#.to_vec(),
    ))
    .await;
    let f = gina.recv().await;
    assert_eq!(f.frame_type, proto::RESP_ERROR, "readonly backup trigger");
    assert!(payload_str(&f).contains("admin"), "{}", payload_str(&f));
    gina.send(&Frame::new(
        proto::REQ_BACKUP,
        br#"{"action":"restore","file":"backup-1.sql"}"#.to_vec(),
    ))
    .await;
    let f = gina.recv().await;
    assert_eq!(f.frame_type, proto::RESP_ERROR, "readonly backup restore");
    assert!(payload_str(&f).contains("admin"), "{}", payload_str(&f));
    let f = gina.promote().await;
    assert_eq!(f.frame_type, proto::RESP_ERROR, "readonly PROMOTE");
    assert!(payload_str(&f).contains("admin"), "{}", payload_str(&f));
    // Her SELECT surface still works (identity intact, not locked out).
    let f = gina.sql("SELECT COUNT(*) FROM t").await;
    assert_eq!(f.frame_type, proto::RESP_ROWS, "{}", payload_str(&f));
}

/// DOCSQL_STATEMENT_TIMEOUT_MS wiring: a client statement over the wall-
/// clock budget fails with the timeout error while the node keeps serving —
/// the deadline is armed per statement, not a poisoned state. INSERT carries
/// no row-loop deadline checks (nothing loop-shaped to preempt there), so
/// setup data lands fine even under a 1ms budget; the nested-loop join of
/// two tables is the deterministic runaway.
#[tokio::test]
async fn statement_timeout_kills_runaway_query_only() {
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
        async_commit: false,
        catchup_window: 0,
        backup_interval_secs: 0,
        backup_keep: 7,
        backup_dir: None,
        statement_timeout_ms: 1,
    };
    tokio::spawn(docsql_server::run(cfg));
    for _ in 0..100 {
        if TcpStream::connect(&addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mut c = Client::connect(&addr).await;
    let f = c.sql("CREATE TABLE a (id INT PRIMARY KEY)").await;
    assert_ne!(f.frame_type, proto::RESP_ERROR, "{}", payload_str(&f));
    let f = c.sql("CREATE TABLE b (id INT)").await;
    assert_ne!(f.frame_type, proto::RESP_ERROR, "{}", payload_str(&f));
    for (table, start) in [("a", 0i64), ("b", 10_000)] {
        let mut vals = format!("INSERT INTO {table} VALUES ({start})");
        for i in 1..1000 {
            vals.push_str(&format!(", ({})", start + i));
        }
        let f = c.sql(&vals).await;
        assert_ne!(f.frame_type, proto::RESP_ERROR, "{}", payload_str(&f));
    }

    // Under the budget: an indexed point query answers normally.
    let f = c.sql("SELECT id FROM a WHERE id = 5").await;
    assert_eq!(f.frame_type, proto::RESP_ROWS, "{}", payload_str(&f));

    // Runaway: 1000x1000 nested-loop join with a never-true predicate
    // (nothing materializes, a million ON evaluations) exceeds 1ms.
    let f = c
        .sql("SELECT COUNT(*) FROM a x, b y WHERE x.id + y.id < 0")
        .await;
    assert_eq!(f.frame_type, proto::RESP_ERROR, "join must time out");
    assert!(
        payload_str(&f).contains("statement timeout"),
        "{}",
        payload_str(&f)
    );

    // The deadline was per statement: the node is healthy and fast
    // statements still answer.
    let f = c.sql("SELECT 1").await;
    assert_eq!(f.frame_type, proto::RESP_ROWS, "{}", payload_str(&f));
}

/// Server-side prepared statements (REQ_PREPARE/REQ_EXECUTE/REQ_CLOSE_STMT):
/// the server renders typed literals itself, so a hostile string value can
/// never break out of the literal — the classic `' OR '1'='1` payload binds
/// as data. Handles are per connection; the query log shows the rendered
/// statement.
#[tokio::test]
async fn server_side_prepared_statements_bind_safely() {
    let (_dir, addr) = start_server(None).await;
    let mut c = Client::connect(&addr).await;
    c.sql("CREATE TABLE p (id INT PRIMARY KEY, v TEXT)").await;
    c.sql("INSERT INTO p VALUES (1, 'one'), (2, 'two')").await;

    // Prepare returns a numeric handle.
    c.send(&Frame::new(
        proto::REQ_PREPARE,
        proto::encode_sql("SELECT v FROM p WHERE id = ?").unwrap(),
    ))
    .await;
    let f = c.recv().await;
    assert_eq!(f.frame_type, proto::RESP_PREPARED, "{}", payload_str(&f));
    let body: serde_json::Value = serde_json::from_slice(&f.payload).unwrap();
    let h = body["handle"].as_u64().expect("handle");

    // Bound execution answers like REQ_SQL.
    c.send(&Frame::new(
        proto::REQ_EXECUTE,
        format!(r#"{{"handle":{h},"params":[2]}}"#).into_bytes(),
    ))
    .await;
    let f = c.recv().await;
    assert_eq!(f.frame_type, proto::RESP_ROWS, "{}", payload_str(&f));
    assert!(payload_str(&f).contains("two"), "{}", payload_str(&f));

    // Injection attempt as a string param: bound as the literal text —
    // the row set stays empty instead of leaking the table.
    c.send(&Frame::new(
        proto::REQ_EXECUTE,
        format!(r#"{{"handle":{h},"params":["2 OR 1=1"]}}"#).into_bytes(),
    ))
    .await;
    let f = c.recv().await;
    assert_eq!(f.frame_type, proto::RESP_ROWS, "{}", payload_str(&f));
    assert!(
        !payload_str(&f).contains("one") && !payload_str(&f).contains("two"),
        "injection payload leaked rows: {}",
        payload_str(&f)
    );

    // Quotes inside a bound string survive round-trip ('' escaping).
    c.send(&Frame::new(
        proto::REQ_PREPARE,
        proto::encode_sql("INSERT INTO p VALUES (?, ?)").unwrap(),
    ))
    .await;
    let f = c.recv().await;
    assert_eq!(f.frame_type, proto::RESP_PREPARED, "{}", payload_str(&f));
    let body: serde_json::Value = serde_json::from_slice(&f.payload).unwrap();
    let hi = body["handle"].as_u64().unwrap();
    c.send(&Frame::new(
        proto::REQ_EXECUTE,
        format!(r#"{{"handle":{hi},"params":[3,"it's fine"]}}"#).into_bytes(),
    ))
    .await;
    let f = c.recv().await;
    assert_eq!(f.frame_type, proto::RESP_AFFECTED, "{}", payload_str(&f));
    let f = c.sql("SELECT v FROM p WHERE id = 3").await;
    assert!(payload_str(&f).contains("it's fine"), "{}", payload_str(&f));

    // Arity mismatch and unknown handles are errors, never partial binds.
    c.send(&Frame::new(
        proto::REQ_EXECUTE,
        format!(r#"{{"handle":{hi},"params":[]}}"#).into_bytes(),
    ))
    .await;
    let f = c.recv().await;
    assert_eq!(f.frame_type, proto::RESP_ERROR, "{}", payload_str(&f));
    c.send(&Frame::new(
        proto::REQ_EXECUTE,
        br#"{"handle":9999,"params":[]}"#.to_vec(),
    ))
    .await;
    let f = c.recv().await;
    assert!(payload_str(&f).contains("unknown statement handle"));

    // CLOSE drops the handle for good.
    c.send(&Frame::new(
        proto::REQ_CLOSE_STMT,
        format!(r#"{{"handle":{h}}}"#).into_bytes(),
    ))
    .await;
    let f = c.recv().await;
    assert_eq!(f.frame_type, proto::RESP_AFFECTED, "{}", payload_str(&f));
    c.send(&Frame::new(
        proto::REQ_EXECUTE,
        format!(r#"{{"handle":{h},"params":[1]}}"#).into_bytes(),
    ))
    .await;
    let f = c.recv().await;
    assert!(payload_str(&f).contains("unknown statement handle"));
}

/// MVCC stage A (concurrent readers): many read-only SELECTs on separate
/// connections all succeed while a writer commits between them — the read
/// tier shares the engine and never observes a torn or missing row.
#[tokio::test]
async fn concurrent_readers_never_observe_partial_writes() {
    let (_dir, addr) = start_server(None).await;
    let mut c = Client::connect(&addr).await;
    c.sql("CREATE TABLE cr (id INT PRIMARY KEY, v TEXT)").await;
    c.sql("INSERT INTO cr VALUES (1, 'a'), (2, 'b')").await;

    // 8 concurrent reader connections: each repeatedly counts rows and
    // reads both rows by PK while a writer inserts row after row. Every
    // count must be the full previous size or the final size — never a
    // torn intermediate.
    let mut readers = Vec::new();
    for _ in 0..8 {
        let addr = addr.clone();
        readers.push(tokio::spawn(async move {
            let mut c = Client::connect(&addr).await;
            for _ in 0..25 {
                let f = c.sql("SELECT COUNT(id) FROM cr").await;
                assert_eq!(f.frame_type, proto::RESP_ROWS, "{}", payload_str(&f));
                let n: i64 = serde_json::from_slice::<serde_json::Value>(&f.payload).unwrap()
                    ["rows"][0][0]
                    .as_i64()
                    .unwrap();
                if n != 2 && n != 3 {
                    panic!("torn read: count {n} is neither the pre- nor post-write size");
                }
                let f = c.sql("SELECT v FROM cr WHERE id = 1").await;
                assert_eq!(f.frame_type, proto::RESP_ROWS, "{}", payload_str(&f));
            }
        }));
    }
    // One writer: a single new row, committed while readers are probing.
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(5)).await;
        let mut w = Client::connect(&addr).await;
        let f = w.sql("INSERT INTO cr VALUES (3, 'c')").await;
        assert_eq!(f.frame_type, proto::RESP_AFFECTED, "{}", payload_str(&f));
    });
    for r in readers {
        r.await.unwrap();
    }
}
