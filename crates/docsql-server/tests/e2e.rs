//! End-to-end tests: real server on a random port, raw TCP client speaking
//! the v1 protocol.

use docsql_core::proto::{self, Frame};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

async fn start_server(token: Option<&str>) -> (tempfile::TempDir, String) {
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
        replicate_to: None,
        peers: Vec::new(),
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

    async fn kv(&mut self, args: &[&str]) -> Frame {
        self.send(&Frame::new(proto::REQ_KV, args.join("\x00").into_bytes()))
            .await;
        self.recv().await
    }
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
async fn kv_commands_over_wire() {
    let (_dir, addr) = start_server(None).await;
    let mut c = Client::connect(&addr).await;
    let r = c.kv(&["SET", "greeting", "hello"]).await;
    assert_eq!(payload_str(&r), "ok");
    let r = c.kv(&["GET", "greeting"]).await;
    assert_eq!(payload_str(&r), "hello");
    let r = c.kv(&["INCR", "hits"]).await;
    assert_eq!(u64::from_le_bytes(r.payload[..8].try_into().unwrap()), 1);
    let r = c.kv(&["RPUSH", "L", "a", "b"]).await;
    assert_eq!(u64::from_le_bytes(r.payload[..8].try_into().unwrap()), 2);
    let r = c.kv(&["LRANGE", "L", "0", "-1"]).await;
    assert_eq!(payload_str(&r), "a\x00b");
    let r = c.kv(&["NOPE"]).await;
    assert_eq!(r.frame_type, proto::RESP_ERROR);
}

#[tokio::test]
async fn kv_full_surface_over_wire() {
    let (_dir, addr) = start_server(None).await;
    let mut c = Client::connect(&addr).await;
    let int = |f: &Frame| u64::from_le_bytes(f.payload[..8].try_into().unwrap());

    // DEL + EXISTS round trip
    c.kv(&["SET", "k", "v"]).await;
    let r = c.kv(&["EXISTS", "k"]).await;
    assert_eq!(int(&r), 1);
    let r = c.kv(&["EXISTS", "missing"]).await;
    assert_eq!(int(&r), 0);
    let r = c.kv(&["DEL", "k"]).await;
    assert_eq!(payload_str(&r), "1");
    let r = c.kv(&["DEL", "k"]).await;
    assert_eq!(payload_str(&r), "0");
    let r = c.kv(&["EXISTS", "k"]).await;
    assert_eq!(int(&r), 0);

    // SET flags: NX on existing key skips, XX on missing key skips
    c.kv(&["SET", "f", "a"]).await;
    let r = c.kv(&["SET", "f", "b", "NX"]).await;
    assert_eq!(payload_str(&r), "skip");
    let r = c.kv(&["SET", "f2", "b", "XX"]).await;
    assert_eq!(payload_str(&r), "skip");
    let r = c.kv(&["SET", "f", "b", "XX"]).await;
    assert_eq!(payload_str(&r), "ok");

    // EXPIRE / TTL / PERSIST
    let r = c.kv(&["EXPIRE", "f", "60000"]).await;
    assert_eq!(payload_str(&r), "1");
    let r = c.kv(&["TTL", "f"]).await;
    let ttl = int(&r);
    assert!(ttl > 0 && ttl <= 60_000, "unexpected ttl {ttl}");
    let r = c.kv(&["PERSIST", "f"]).await;
    assert_eq!(payload_str(&r), "1");
    let r = c.kv(&["TTL", "f"]).await;
    assert_eq!(payload_str(&r), "-1");

    // hash / set / zset over the wire
    let r = c.kv(&["HSET", "H", "field", "val"]).await;
    assert_eq!(int(&r), 1);
    let r = c.kv(&["HGET", "H", "field"]).await;
    assert_eq!(payload_str(&r), "val");
    let r = c.kv(&["HGET", "H", "nope"]).await;
    assert_eq!(r.payload.len(), 0); // nil
    let r = c.kv(&["SADD", "S", "x", "y", "x"]).await;
    assert_eq!(int(&r), 2);
    let r = c.kv(&["SMEMBERS", "S"]).await;
    let members = payload_str(&r);
    assert!(members.contains("x") && members.contains("y"), "{members}");
    let r = c.kv(&["ZADD", "Z", "2.5", "m2"]).await;
    assert_eq!(int(&r), 1);
    let r = c.kv(&["ZADD", "Z", "1.5", "m1"]).await;
    assert_eq!(int(&r), 1);
    let r = c.kv(&["ZRANGE", "Z", "-inf", "inf"]).await;
    assert_eq!(payload_str(&r), "m1\x001.5\x00m2\x002.5");

    // MULTI / EXEC commit
    c.kv(&["MULTI"]).await;
    c.kv(&["SET", "txa", "1"]).await;
    c.kv(&["RPUSH", "txl", "e"]).await;
    let r = c.kv(&["EXEC"]).await;
    assert_eq!(payload_str(&r), "ok");
    let r = c.kv(&["GET", "txa"]).await;
    assert_eq!(payload_str(&r), "1");
    let r = c.kv(&["LRANGE", "txl", "0", "-1"]).await;
    assert_eq!(payload_str(&r), "e");

    // MULTI / DISCARD rolls back
    c.kv(&["MULTI"]).await;
    c.kv(&["SET", "txa", "999"]).await;
    let r = c.kv(&["DISCARD"]).await;
    assert_eq!(payload_str(&r), "ok");
    let r = c.kv(&["GET", "txa"]).await;
    assert_eq!(payload_str(&r), "1");
}

#[tokio::test]
async fn auth_gate() {
    let (_dir, addr) = start_server(Some("s3cret")).await;
    let mut c = Client::connect(&addr).await;
    // SQL before AUTH is rejected.
    let r = c.sql("SELECT 1").await;
    assert_eq!(r.frame_type, proto::RESP_ERROR);
    // KV GET before AUTH rejected too.
    let r = c.kv(&["GET", "k"]).await;
    assert_eq!(r.frame_type, proto::RESP_ERROR);
    // Wrong token rejected.
    let r = c.kv(&["AUTH", "wrong"]).await;
    assert_eq!(r.frame_type, proto::RESP_ERROR);
    // Right token unlocks the session.
    let r = c.kv(&["AUTH", "s3cret"]).await;
    assert_eq!(payload_str(&r), "ok");
    let r = c.kv(&["SET", "k", "v"]).await;
    assert_eq!(payload_str(&r), "ok");
}

#[tokio::test]
async fn pubsub_push_between_connections() {
    let (_dir, addr) = start_server(None).await;
    let mut sub = Client::connect(&addr).await;
    let r = sub.kv(&["SUBSCRIBE", "news"]).await;
    assert!(payload_str(&r).starts_with("subscribed"));
    let mut pubber = Client::connect(&addr).await;
    let r = pubber.kv(&["PUBLISH", "news", "hello world"]).await;
    assert_eq!(u64::from_le_bytes(r.payload[..8].try_into().unwrap()), 1);
    // The subscriber connection receives a RESP_PUSH frame.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let f = tokio::time::timeout_at(deadline.into(), sub.recv()).await;
        match f {
            Ok(frame) if frame.frame_type == proto::RESP_PUSH => {
                assert_eq!(payload_str(&frame), "message\x00news\x00hello world");
                break;
            }
            Ok(_) => continue,
            Err(_) => panic!("no push received within timeout"),
        }
    }
}

#[tokio::test]
async fn sql_error_reaches_client() {
    let (_dir, addr) = start_server(None).await;
    let mut c = Client::connect(&addr).await;
    let r = c.sql("SELECT * FROM missing").await;
    assert_eq!(r.frame_type, proto::RESP_ERROR);
    assert!(payload_str(&r).contains("does not exist"));
    // Connection stays usable afterwards.
    let r = c.kv(&["PING"]).await;
    assert_eq!(payload_str(&r), "pong");
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
        replicate_to: None,
        peers: Vec::new(),
        read_only: true,
        transport_key: None,
        async_commit: false,
    }));
    // Primary: forwards writes to the replica.
    tokio::spawn(docsql_server::run(docsql_server::ServerConfig {
        db_path: dir.path().join("primary.db"),
        listen: primary_addr.clone(),
        auth_token: None,
        replicate_to: Some(replica_addr.clone()),
        peers: Vec::new(),
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

    // KV writes replicate too.
    p.kv(&["SET", "replkey", "replval"]).await;
    let mut kv_seen = false;
    for _ in 0..50 {
        let resp = r.kv(&["GET", "replkey"]).await;
        if resp.frame_type == proto::RESP_AFFECTED && payload_str(&resp) == "replval" {
            kv_seen = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(kv_seen, "KV write did not replicate");

    // Replica rejects client writes before promotion.
    let resp = r.sql("INSERT INTO fail VALUES (8)").await;
    assert_eq!(resp.frame_type, proto::RESP_ERROR);
    assert!(payload_str(&resp).contains("read-only"));

    // Failover: promote the replica; writes now succeed.
    r.kv(&["PROMOTE"]).await;
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
        let resp = if let Some(kv) = probe.strip_prefix("KV ") {
            c.kv(&kv.split_whitespace().collect::<Vec<_>>()).await
        } else {
            c.sql(probe).await
        };
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
            replicate_to: None,
            peers: peers.split(',').map(String::from).collect(),
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

    // Write through node 0 (SQL), node 1 (KV); read from node 2.
    let mut a = Client::connect(&addrs[0]).await;
    a.sql("CREATE TABLE sym (id INT, src INT)").await;
    a.sql("INSERT INTO sym VALUES (1, 0)").await;
    let mut b = Client::connect(&addrs[1]).await;
    b.sql("INSERT INTO sym VALUES (2, 1)").await;
    b.kv(&["SET", "symkey", "from1"]).await;

    assert!(
        wait_seen(&addrs[2], "SELECT src FROM sym ORDER BY src", "[[0],[1]]").await,
        "node2 did not see both SQL writes"
    );
    assert!(
        wait_seen(&addrs[2], "KV GET symkey", "from1").await,
        "node2 did not see the KV write"
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
/// the buffered tail (SQL and KV alike).
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
            replicate_to: None,
            peers: peers.split(',').map(String::from).collect(),
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

    // KV MULTI/DISCARD: buffered frames never reach the peer.
    a.kv(&["MULTI"]).await;
    a.kv(&["SET", "txk", "discarded"]).await;
    a.kv(&["DISCARD"]).await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let resp = b.kv(&["GET", "txk"]).await;
    assert_ne!(
        payload_str(&resp),
        "discarded",
        "peer observed a KV write from a discarded MULTI"
    );

    // KV MULTI/EXEC: the buffered write replays at commit time.
    a.kv(&["MULTI"]).await;
    a.kv(&["SET", "txk2", "committed"]).await;
    let resp = a.kv(&["EXEC"]).await;
    assert_ne!(resp.frame_type, proto::RESP_ERROR, "EXEC failed");
    assert!(
        wait_seen(&addrs[1], "KV GET txk2", "committed").await,
        "KV transaction write did not replicate on EXEC"
    );
}

/// Two real shards: KV keys route by hash slot and land deterministically.
#[tokio::test]
async fn shard_routing_distributes_kv_keys() {
    let dir = tempfile::tempdir().unwrap();
    let mut addrs = Vec::new();
    for i in 0..2 {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let a = format!("127.0.0.1:{}", l.local_addr().unwrap().port());
        drop(l);
        tokio::spawn(docsql_server::run(docsql_server::ServerConfig {
            db_path: dir.path().join(format!("shard{i}.db")),
            listen: a.clone(),
            auth_token: None,
            replicate_to: None,
            peers: Vec::new(),
            read_only: false,
            transport_key: None,
            async_commit: false,
        }));
        addrs.push(a);
    }
    for a in &addrs {
        for _ in 0..100 {
            if TcpStream::connect(a).await.is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    let router = docsql_server::shard::ShardRouter::new(addrs.clone());
    assert_eq!(router.shard_count(), 2);

    // Write through the router, read directly from each shard.
    let keys = ["shard-key-a", "shard-key-b", "shard-key-c", "shard-key-d"];
    for k in keys {
        let resp = router.kv(&["SET", k, "routed"]).await.unwrap();
        assert_ne!(resp.frame_type, proto::RESP_ERROR, "SET {k} failed");
    }
    for k in keys {
        let target = router.shard_for(k);
        let mut c = Client::connect(target).await;
        let resp = c.kv(&["GET", k]).await;
        // The value travels with the routed command.
        assert_eq!(payload_str(&resp), "routed", "GET {k} on {target}");
    }

    // Same key always routes to the same shard (read-your-writes).
    assert_eq!(router.shard_for("stable"), router.shard_for("stable"));
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
        replicate_to: None,
        peers: Vec::new(),
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
