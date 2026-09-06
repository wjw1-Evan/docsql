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
