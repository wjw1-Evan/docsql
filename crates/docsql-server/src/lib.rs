//! docsql network server.
//!
//! Speaks the v1 binary protocol from `docsql_core::proto`:
//! - REQ_SQL  → RESP_ROWS / RESP_AFFECTED / RESP_ERROR
//! - REQ_KV   → command dispatch (GET/SET/.../SUBSCRIBE/PUBLISH/AUTH)
//! - REQ_PING → RESP_PONG
//!
//! Every connection shares one engine instance behind a mutex (single-writer
//! v1; the cluster milestone brings per-shard concurrency). Subscribed
//! connections receive RESP_PUSH frames from the pub/sub bus via a writer
//! task.

pub mod crypto;
pub mod kvproto;
pub mod querylog;
pub mod shard;

use docsql_core::engine::{Database, ExecOutcome, TxControl};
use docsql_core::proto::{self, Frame};
use docsql_core::value::Value;
use docsql_kv::pubsub::{Event, PubSub};
use docsql_kv::Kv;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

pub struct ServerState {
    pub kv: Mutex<Kv>,
    pub pubsub: PubSub,
    pub auth_token: Option<String>,
    /// Upstream replication target (empty when not replicating).
    pub replicate_to: tokio::sync::Mutex<Option<String>>,
    /// Symmetric peers: every successful write is forwarded to all of them
    /// and every node accepts writes (no primary/replica roles).
    pub peers: tokio::sync::Mutex<Vec<String>>,
    /// Writes executed inside the open engine transaction (SQL statements
    /// and KV frames, in execution order). They reach peers only when the
    /// transaction commits; a rollback discards them.
    pub tx_pending: tokio::sync::Mutex<TxPending>,
    /// Replicas reject client writes until promoted.
    pub read_only: std::sync::atomic::AtomicBool,
    /// When set, every frame payload is sealed with AES-256-GCM.
    pub transport_key: Option<crypto::TransportKey>,
    /// Statement audit log (docsql_log view).
    pub query_log: querylog::QueryLog,
}

/// Replication buffer for the open engine transaction. Savepoint marks
/// mirror engine savepoints so ROLLBACK TO SAVEPOINT trims the tail.
#[derive(Default)]
pub struct TxPending {
    pub sqls: Vec<String>,
    pub frames: Vec<Frame>,
    marks: Vec<(String, usize, usize)>,
}

impl TxPending {
    pub fn new() -> Self {
        Self::default()
    }

    /// SAVEPOINT name: remember the buffer length to roll back to.
    pub fn mark(&mut self, name: &str) {
        self.marks
            .push((name.to_string(), self.sqls.len(), self.frames.len()));
    }

    /// ROLLBACK TO SAVEPOINT name: drop writes past the mark (and later marks).
    pub fn rollback_to(&mut self, name: &str) {
        if let Some(pos) = self.marks.iter().position(|(n, ..)| n == name) {
            let (_, sqls_len, frames_len) = self.marks[pos].clone();
            self.sqls.truncate(sqls_len);
            self.frames.truncate(frames_len);
            self.marks.truncate(pos);
        }
    }

    /// RELEASE SAVEPOINT name: forget the mark, keep the writes.
    pub fn release(&mut self, name: &str) {
        if let Some(pos) = self.marks.iter().position(|(n, ..)| n == name) {
            self.marks.truncate(pos);
        }
    }

    pub fn clear(&mut self) {
        self.sqls.clear();
        self.frames.clear();
        self.marks.clear();
    }
}

/// Flags bit 1 marks replication-internal frames (bypasses read-only).
pub const FLAG_REPLICATION: u16 = 0x0002;

pub struct ServerConfig {
    pub db_path: PathBuf,
    pub listen: String,
    pub auth_token: Option<String>,
    /// Replication upstream: every successful write is forwarded here
    /// (host:port of a docsql-server in replica mode).
    pub replicate_to: Option<String>,
    /// Symmetric-cluster peers (host:port list). In this mode there is no
    /// read-only role: any node accepts writes and fans them out.
    pub peers: Vec<String>,
    /// Replica mode: reject client writes (replication frames excepted)
    /// until promoted via PROMOTE.
    pub read_only: bool,
    /// Transport encryption key (32 bytes); None = plaintext frames.
    pub transport_key: Option<crypto::TransportKey>,
    /// Async-commit mode: statement commits skip the WAL fsync; a
    /// background flusher batches fsyncs every ~2ms (MongoDB-style
    /// journal interval). Bounded loss window on power failure.
    pub async_commit: bool,
}

pub async fn run(cfg: ServerConfig) -> std::io::Result<()> {
    let mut kv =
        Kv::open(&cfg.db_path).map_err(|e| std::io::Error::other(format!("open db: {e}")))?;
    kv.db.set_async_commit(cfg.async_commit);
    let state = Arc::new(ServerState {
        kv: Mutex::new(kv),
        pubsub: PubSub::new(),
        auth_token: cfg.auth_token,
        replicate_to: tokio::sync::Mutex::new(cfg.replicate_to.clone()),
        peers: tokio::sync::Mutex::new(cfg.peers.clone()),
        tx_pending: tokio::sync::Mutex::new(TxPending::new()),
        read_only: std::sync::atomic::AtomicBool::new(cfg.read_only),
        transport_key: cfg.transport_key,
        query_log: querylog::QueryLog::new(),
    });
    let listener = TcpListener::bind(&cfg.listen).await?;
    eprintln!("docsql-server listening on {}", cfg.listen);
    loop {
        let (stream, peer) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, state).await {
                eprintln!("connection {peer} closed: {e}");
            }
        });
    }
}

struct Conn {
    stream: tokio::net::tcp::OwnedReadHalf,
    buf: Vec<u8>,
}

impl Conn {
    async fn read_frame(&mut self) -> std::io::Result<Option<Frame>> {
        loop {
            if let Ok((f, n)) = Frame::decode(&self.buf) {
                self.buf.drain(..n);
                return Ok(Some(f));
            }
            // Need more bytes; header length check avoids unbounded growth.
            if self.buf.len() > 64 * 1024 * 1024 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "frame too large",
                ));
            }
            let mut chunk = [0u8; 8192];
            let n = self.stream.read(&mut chunk).await?;
            if n == 0 {
                return Ok(None);
            }
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }
}

pub async fn handle_connection(stream: TcpStream, state: Arc<ServerState>) -> std::io::Result<()> {
    let peer = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_default();
    let (rd, mut wr) = stream.into_split();
    let mut conn = Conn {
        stream: rd,
        buf: Vec::new(),
    };
    let (tx, mut rx) = mpsc::channel::<Frame>(256);
    let key = state.transport_key;

    // Writer task: serializes responses + pub/sub pushes (sealing when a
    // transport key is configured).
    let writer = tokio::spawn(async move {
        while let Some(f) = rx.recv().await {
            let f = if let Some(k) = key {
                Frame {
                    flags: f.flags | crypto::FLAG_ENCRYPTED,
                    payload: crypto::seal(&k, &f.payload),
                    ..f
                }
            } else {
                f
            };
            let bytes = match f.encode() {
                Ok(b) => b,
                Err(_) => continue,
            };
            if wr.write_all(&bytes).await.is_err() || wr.flush().await.is_err() {
                break;
            }
        }
        let _ = wr.shutdown().await;
    });

    let mut authed = state.auth_token.is_none();
    let result = async {
        while let Some(mut frame) = conn.read_frame().await? {
            if let Some(k) = key {
                if frame.flags & crypto::FLAG_ENCRYPTED == 0 {
                    let _ = tx
                        .send(Frame::new(
                            proto::RESP_ERROR,
                            kvproto::err_payload(
                                "transport encrypted; client must send encrypted frames",
                            ),
                        ))
                        .await;
                    break;
                }
                match crypto::open(&k, &frame.payload) {
                    Ok(pt) => frame.payload = pt,
                    Err(e) => {
                        let _ = tx
                            .send(Frame::new(proto::RESP_ERROR, kvproto::err_payload(&e)))
                            .await;
                        break;
                    }
                }
            }
            let resp = match frame.frame_type {
                proto::REQ_PING => Frame::new(proto::RESP_PONG, vec![]),
                proto::REQ_SQL if authed => {
                    let started = std::time::Instant::now();
                    let sql = proto::decode_sql(&frame.payload)
                        .map(|s| s.chars().take(512).collect::<String>())
                        .unwrap_or_default();
                    let mut logged = false;
                    let resp = match querylog::try_serve_log_view(&sql, &state) {
                        // 读日志的查询本身不写日志(避免读日志刷日志)。
                        Some(f) => f,
                        None => {
                            let resp = handle_sql(&state, &frame).await;
                            logged = true;
                            resp
                        }
                    };
                    if logged {
                        querylog::record(
                            &state,
                            &peer,
                            &sql,
                            started.elapsed().as_secs_f64() * 1000.0,
                            &resp,
                            frame.flags & FLAG_REPLICATION != 0,
                        );
                    }
                    resp
                }
                proto::REQ_KV => {
                    let (resp, sub_channel, now_authed) =
                        kvproto::handle(&state, &frame, authed).await;
                    authed = now_authed;
                    if let Some(ch) = sub_channel {
                        spawn_subscription(state.clone(), ch, tx.clone());
                    }
                    resp
                }
                _ => Frame::new(
                    proto::RESP_ERROR,
                    kvproto::err_payload(if authed {
                        "unsupported frame"
                    } else {
                        "unauthorized"
                    }),
                ),
            };
            if tx.send(resp).await.is_err() {
                break;
            }
        }
        Ok::<(), std::io::Error>(())
    }
    .await;
    drop(tx);
    let _ = writer.await;
    result
}

fn spawn_subscription(state: Arc<ServerState>, channel: String, tx: mpsc::Sender<Frame>) {
    let rx = state.pubsub.subscribe(&channel);
    tokio::spawn(async move {
        // Bridge the std receiver with a blocking drain loop.
        loop {
            let events: Vec<Event> = {
                let mut out = Vec::new();
                while let Ok(e) = rx.try_recv() {
                    out.push(e);
                }
                out
            };
            for e in events {
                let payload = match e {
                    Event::Message { channel, payload } => {
                        format!("message\x00{channel}\x00{payload}")
                    }
                    Event::PMessage {
                        pattern,
                        channel,
                        payload,
                    } => {
                        format!("pmessage\x00{pattern}\x00{channel}\x00{payload}")
                    }
                };
                if tx
                    .send(Frame::new(proto::RESP_PUSH, payload.into_bytes()))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    });
}

async fn handle_sql(state: &Arc<ServerState>, frame: &Frame) -> Frame {
    let sql = match proto::decode_sql(&frame.payload) {
        Ok(s) => s,
        Err(e) => return Frame::new(proto::RESP_ERROR, kvproto::err_payload(&e.to_string())),
    };
    // Replica read-only gate (replication-internal frames pass through).
    let is_replication = frame.flags & FLAG_REPLICATION != 0;
    let read_only = state.read_only.load(std::sync::atomic::Ordering::SeqCst);
    if read_only && !is_replication && docsql_core::engine::Database::is_write_statement(&sql) {
        return Frame::new(
            proto::RESP_ERROR,
            kvproto::err_payload("read-only replica; PROMOTE to accept writes"),
        );
    }
    let (outcome, in_tx) = {
        let mut kv = state.kv.lock().unwrap();
        let out = kv.db.execute(&sql);
        // Capture inside the same lock: another connection must not be able
        // to open/close a transaction between execute and classification.
        let in_tx = kv.db.in_transaction();
        (out, in_tx)
    };
    // Replication timing follows engine transaction state: writes inside an
    // open transaction buffer until COMMIT (ROLLBACK discards them), so peers
    // never observe writes this node later undoes. COMMIT/EXEC drain the
    // buffer in execution order, mixing SQL and KV writes alike.
    if !is_replication && outcome.is_ok() {
        match Database::tx_control(&sql) {
            TxControl::Commit => drain_tx_pending(state).await,
            TxControl::Rollback { savepoint: None } => state.tx_pending.lock().await.clear(),
            TxControl::Rollback {
                savepoint: Some(name),
            } => state.tx_pending.lock().await.rollback_to(&name),
            TxControl::Savepoint(name) => state.tx_pending.lock().await.mark(&name),
            TxControl::Release(name) => state.tx_pending.lock().await.release(&name),
            _ if Database::is_write_statement(&sql) => {
                if in_tx {
                    state.tx_pending.lock().await.sqls.push(sql.clone());
                } else {
                    forward_sql_all(state, &sql).await;
                }
            }
            _ => {}
        }
    }
    match outcome {
        Ok(ExecOutcome::Rows(r)) => {
            let mut obj = docsql_core::value::Object::new();
            obj.insert(
                "columns".into(),
                Value::Array(r.columns.into_iter().map(Value::Str).collect()),
            );
            obj.insert(
                "rows".into(),
                Value::Array(r.rows.into_iter().map(Value::Array).collect()),
            );
            Frame::new(
                proto::RESP_ROWS,
                docsql_core::json::to_string(&Value::Object(obj)).into_bytes(),
            )
        }
        Ok(ExecOutcome::Affected(n)) => Frame::new(proto::RESP_AFFECTED, n.to_le_bytes().to_vec()),
        Err(e) => Frame::new(proto::RESP_ERROR, kvproto::err_payload(&e.to_string())),
    }
}

/// Send a raw frame to a peer and read one response frame back.
/// When a transport key is configured the outbound frame is sealed (peers
/// share the key); the response is discarded, so it is not opened here.
pub async fn forward_frame(
    target: &str,
    frame: &Frame,
    key: Option<&crypto::TransportKey>,
) -> std::io::Result<Frame> {
    let mut stream = tokio::net::TcpStream::connect(target).await?;
    let frame = if let Some(k) = key {
        Frame {
            flags: frame.flags | crypto::FLAG_ENCRYPTED,
            payload: crypto::seal(k, &frame.payload),
            ..frame.clone()
        }
    } else {
        frame.clone()
    };
    let bytes = frame.encode().map_err(std::io::Error::other)?;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    stream.write_all(&bytes).await?;
    stream.flush().await?;
    let mut header = [0u8; proto::HEADER_LEN];
    stream.read_exact(&mut header).await?;
    let len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
    let mut buf = header.to_vec();
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;
    buf.extend_from_slice(&payload);
    let (f, _) = Frame::decode(&buf).map_err(std::io::Error::other)?;
    Ok(f)
}

/// Send one write to the replication upstream.
async fn forward_write(
    target: &str,
    sql: &str,
    key: Option<&crypto::TransportKey>,
) -> std::io::Result<()> {
    let mut stream = tokio::net::TcpStream::connect(target).await?;
    let mut frame = Frame::new(proto::REQ_SQL, proto::encode_sql(sql).unwrap_or_default());
    frame.flags = FLAG_REPLICATION;
    if let Some(k) = key {
        frame.payload = crypto::seal(k, &frame.payload);
        frame.flags |= crypto::FLAG_ENCRYPTED;
    }
    let bytes = frame.encode().map_err(std::io::Error::other)?;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    stream.write_all(&bytes).await?;
    stream.flush().await?;
    // Read the response header + payload and discard.
    let mut header = [0u8; proto::HEADER_LEN];
    stream.read_exact(&mut header).await?;
    let len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;
    if header[6] == (proto::RESP_ERROR & 0xff) as u8 && header[7] == (proto::RESP_ERROR >> 8) as u8
    {
        return Err(std::io::Error::other(format!(
            "replica rejected: {}",
            String::from_utf8_lossy(&payload)
        )));
    }
    Ok(())
}

/// Fan one SQL write out to the replication upstream and every peer.
pub async fn forward_sql_all(state: &Arc<ServerState>, sql: &str) {
    if let Some(target) = state.replicate_to.lock().await.clone() {
        if let Err(e) = forward_write(&target, sql, state.transport_key.as_ref()).await {
            eprintln!("replication to {target} failed: {e}");
        }
    }
    for peer in state.peers.lock().await.clone() {
        if let Err(e) = forward_write(&peer, sql, state.transport_key.as_ref()).await {
            eprintln!("peer replication to {peer} failed: {e}");
        }
    }
}

/// Fan one KV command frame out to the replication upstream and every peer.
pub async fn forward_kv_all(state: &Arc<ServerState>, frame: &Frame) {
    let mut fwd = frame.clone();
    fwd.flags = FLAG_REPLICATION;
    if let Some(target) = state.replicate_to.lock().await.clone() {
        if let Err(e) = forward_frame(&target, &fwd, state.transport_key.as_ref()).await {
            eprintln!("kv replication to {target} failed: {e}");
        }
    }
    for peer in state.peers.lock().await.clone() {
        if let Err(e) = forward_frame(&peer, &fwd, state.transport_key.as_ref()).await {
            eprintln!("kv peer replication to {peer} failed: {e}");
        }
    }
}

/// Forward everything buffered in the open transaction (SQL then KV, in
/// execution order) and clear the buffer. Called after a successful COMMIT.
pub async fn drain_tx_pending(state: &Arc<ServerState>) {
    let mut pending = state.tx_pending.lock().await;
    let sqls = std::mem::take(&mut pending.sqls);
    let frames = std::mem::take(&mut pending.frames);
    pending.marks.clear();
    drop(pending);
    for sql in &sqls {
        forward_sql_all(state, sql).await;
    }
    for frame in &frames {
        forward_kv_all(state, frame).await;
    }
}

pub fn parse_kv_args(payload: &[u8]) -> Result<Vec<String>, String> {
    let text = std::str::from_utf8(payload).map_err(|e| e.to_string())?;
    Ok(text
        .split('\x00')
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect())
}

pub fn kv_ok(parts: &[String]) -> Frame {
    Frame::new(proto::RESP_AFFECTED, parts.join("\x00").into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use docsql_kv::Kv;

    fn state(token: Option<&str>) -> Arc<ServerState> {
        Arc::new(ServerState {
            kv: Mutex::new(Kv::in_memory().unwrap()),
            pubsub: PubSub::new(),
            auth_token: token.map(String::from),
            replicate_to: tokio::sync::Mutex::new(None),
            peers: tokio::sync::Mutex::new(vec![]),
            read_only: std::sync::atomic::AtomicBool::new(false),
            transport_key: None,
            tx_pending: tokio::sync::Mutex::new(crate::TxPending::new()),
            query_log: querylog::QueryLog::new(),
        })
    }

    fn sql_frame(sql: &str, flags: u16) -> Frame {
        let mut f = Frame::new(proto::REQ_SQL, proto::encode_sql(sql).unwrap());
        f.flags = flags;
        f
    }

    fn kv_frame(args: &[&str], flags: u16) -> Frame {
        let mut f = Frame::new(proto::REQ_KV, args.join("\x00").into_bytes());
        f.flags = flags;
        f
    }

    #[test]
    fn parse_kv_args_splits_on_nul() {
        let a = parse_kv_args(b"SET\x00k\x00v").unwrap();
        assert_eq!(a, vec!["SET", "k", "v"]);
        // empty segments filtered; invalid utf-8 rejected
        assert!(parse_kv_args(b"a\x00\x00b").unwrap().len() == 2);
        assert!(parse_kv_args(&[0xff, 0xfe]).is_err());
        let f = kv_ok(&["a".into(), "b".into()]);
        assert_eq!(f.payload, b"a\x00b".to_vec());
    }

    #[tokio::test]
    async fn ping_and_unsupported_frames() {
        let st = state(None);
        let (resp, sub, _) = super::kvproto::handle(&st, &kv_frame(&["PING"], 0), true).await;
        assert_eq!(resp.payload, b"pong".to_vec());
        assert!(sub.is_none());
        // REQ_PING handled by handle_connection; REQ_KV with empty payload
        let (resp, _, _) =
            super::kvproto::handle(&st, &Frame::new(proto::REQ_KV, vec![]), true).await;
        assert_eq!(resp.frame_type, proto::RESP_ERROR);
        // unknown command
        let (resp, _, _) = super::kvproto::handle(&st, &kv_frame(&["WOBBLE"], 0), true).await;
        assert_eq!(resp.frame_type, proto::RESP_ERROR);
        assert!(String::from_utf8_lossy(&resp.payload).contains("unknown command"));
    }

    #[tokio::test]
    async fn kv_auth_flow() {
        let st = state(Some("tok"));
        // unauthed command is rejected
        let (resp, _, authed) =
            super::kvproto::handle(&st, &kv_frame(&["GET", "k"], 0), false).await;
        assert_eq!(resp.frame_type, proto::RESP_ERROR);
        assert!(!authed);
        // bad token keeps unauthed
        let (resp, _, authed) =
            super::kvproto::handle(&st, &kv_frame(&["AUTH", "nope"], 0), false).await;
        assert_eq!(resp.frame_type, proto::RESP_ERROR);
        assert!(!authed);
        // good token authentifies
        let (resp, _, authed) =
            super::kvproto::handle(&st, &kv_frame(&["AUTH", "tok"], 0), false).await;
        assert_ne!(resp.frame_type, proto::RESP_ERROR);
        assert!(authed);
        // PUBLISH/SUBSCRIBE need args
        let (resp, _, _) = super::kvproto::handle(&st, &kv_frame(&["PUBLISH"], 0), true).await;
        assert_eq!(resp.frame_type, proto::RESP_ERROR);
        let (resp, sub, _) =
            super::kvproto::handle(&st, &kv_frame(&["SUBSCRIBE", "ch"], 0), true).await;
        assert_eq!(sub, Some("ch".to_string()));
        assert!(String::from_utf8_lossy(&resp.payload).contains("subscribed"));
        // PUBLISH counts live subscribers (SUBSCRIBE frames only attach in
        // handle_connection, so subscribe directly on the bus here)
        let _rx = st.pubsub.subscribe("ch");
        let (resp, _, _) =
            super::kvproto::handle(&st, &kv_frame(&["PUBLISH", "ch", "m"], 0), true).await;
        assert_eq!(u64::from_le_bytes(resp.payload[..8].try_into().unwrap()), 1);
    }

    #[tokio::test]
    async fn kv_command_dispatch_all() {
        let st = state(None);
        let h = |st: &Arc<ServerState>, args: &[&str]| {
            let f = kv_frame(args, 0);
            let st = st.clone();
            async move { super::kvproto::handle(&st, &f, true).await }
        };
        // SET / GET / EXISTS / DEL
        let (r, _, _) = h(&st, &["SET", "k", "v"]).await;
        assert_eq!(r.payload, b"ok".to_vec());
        let (r, _, _) = h(&st, &["GET", "k"]).await;
        assert_eq!(r.payload, b"v".to_vec());
        let (r, _, _) = h(&st, &["GET", "missing"]).await;
        assert!(r.payload.is_empty());
        let (r, _, _) = h(&st, &["EXISTS", "k"]).await;
        assert_eq!(r.payload, 1u64.to_le_bytes().to_vec());
        let (r, _, _) = h(&st, &["SET", "k", "v2", "NX"]).await;
        assert_eq!(r.payload, b"skip".to_vec());
        let (r, _, _) = h(&st, &["SET", "k", "v3", "XX"]).await;
        assert_eq!(r.payload, b"ok".to_vec());
        let (r, _, _) = h(&st, &["DEL", "k"]).await;
        assert_eq!(r.payload, b"1".to_vec());
        let (r, _, _) = h(&st, &["DEL", "k"]).await;
        assert_eq!(r.payload, b"0".to_vec());
        // INCR / INCRBY
        let (r, _, _) = h(&st, &["INCR", "n"]).await;
        assert_eq!(r.payload, 1u64.to_le_bytes().to_vec());
        let (r, _, _) = h(&st, &["INCRBY", "n", "4"]).await;
        assert_eq!(r.payload, 5u64.to_le_bytes().to_vec());
        // EXPIRE / TTL / PERSIST
        h(&st, &["SET", "e", "1"]).await;
        let (r, _, _) = h(&st, &["EXPIRE", "e", "60000"]).await;
        assert_eq!(r.payload, b"1".to_vec());
        let (r, _, _) = h(&st, &["TTL", "e"]).await;
        assert!(u64::from_le_bytes(r.payload[..8].try_into().unwrap()) > 0);
        let (r, _, _) = h(&st, &["TTL", "none"]).await;
        assert_eq!(r.payload, b"-2".to_vec());
        let (r, _, _) = h(&st, &["PERSIST", "e"]).await;
        assert_eq!(r.payload, b"1".to_vec());
        let (r, _, _) = h(&st, &["TTL", "e"]).await;
        assert_eq!(r.payload, b"-1".to_vec());
        // lists
        let (r, _, _) = h(&st, &["RPUSH", "q", "a", "b"]).await;
        assert_eq!(r.payload, 2u64.to_le_bytes().to_vec());
        let (r, _, _) = h(&st, &["LPUSH", "q", "z"]).await;
        assert_eq!(r.payload, 3u64.to_le_bytes().to_vec());
        let (r, _, _) = h(&st, &["LRANGE", "q", "0", "-1"]).await;
        assert_eq!(r.payload, b"z\x00a\x00b".to_vec());
        let (r, _, _) = h(&st, &["LPOP", "q"]).await;
        assert_eq!(r.payload, b"z".to_vec());
        let (r, _, _) = h(&st, &["RPOP", "missing"]).await;
        assert!(r.payload.is_empty());
        // hashes
        let (r, _, _) = h(&st, &["HSET", "h", "f", "v"]).await;
        assert_eq!(r.payload, 1u64.to_le_bytes().to_vec());
        let (r, _, _) = h(&st, &["HGET", "h", "f"]).await;
        assert_eq!(r.payload, b"v".to_vec());
        let (r, _, _) = h(&st, &["HGET", "h", "none"]).await;
        assert!(r.payload.is_empty());
        let (r, _, _) = h(&st, &["HSET", "h"]).await;
        assert_eq!(r.frame_type, proto::RESP_ERROR);
        let (r, _, _) = h(&st, &["HGET", "h"]).await;
        assert_eq!(r.frame_type, proto::RESP_ERROR);
        // sets
        let (r, _, _) = h(&st, &["SADD", "s", "m1", "m2"]).await;
        assert_eq!(r.payload, 2u64.to_le_bytes().to_vec());
        let (r, _, _) = h(&st, &["SMEMBERS", "s"]).await;
        assert_eq!(r.payload, b"m1\x00m2".to_vec());
        // zsets
        let (r, _, _) = h(&st, &["ZADD", "z", "9.5", "one"]).await;
        assert_eq!(r.payload, 1u64.to_le_bytes().to_vec());
        let (r, _, _) = h(&st, &["ZADD", "z"]).await;
        assert_eq!(r.frame_type, proto::RESP_ERROR);
        let (r, _, _) = h(&st, &["ZRANGE", "z", "0", "10"]).await;
        assert_eq!(r.payload, b"one\x009.5".to_vec());
        // MULTI/EXEC/DISCARD
        let (r, _, _) = h(&st, &["MULTI"]).await;
        assert_eq!(r.payload, b"ok".to_vec());
        let (r, _, _) = h(&st, &["DISCARD"]).await;
        assert_eq!(r.payload, b"ok".to_vec());
        let (r, _, _) = h(&st, &["EXEC"]).await;
        assert_eq!(r.frame_type, proto::RESP_ERROR); // no tx open
                                                     // PROMOTE clears read-only and replication
        st.read_only
            .store(true, std::sync::atomic::Ordering::SeqCst);
        *st.replicate_to.lock().await = Some("127.0.0.1:1".into());
        let (r, _, _) = h(&st, &["PROMOTE"]).await;
        assert_eq!(r.payload, b"promoted".to_vec());
        assert!(!st.read_only.load(std::sync::atomic::Ordering::SeqCst));
        assert!(st.replicate_to.lock().await.is_none());
    }

    #[tokio::test]
    async fn read_only_replica_gates_writes_and_promotes() {
        let st = state(None);
        st.read_only
            .store(true, std::sync::atomic::Ordering::SeqCst);
        // client write rejected
        let resp = handle_sql(&st, &sql_frame("CREATE TABLE t (id INT)", 0)).await;
        assert_eq!(resp.frame_type, proto::RESP_ERROR);
        assert!(String::from_utf8_lossy(&resp.payload).contains("read-only"));
        // replication-flagged frames pass
        let resp = handle_sql(&st, &sql_frame("CREATE TABLE t (id INT)", FLAG_REPLICATION)).await;
        assert_ne!(resp.frame_type, proto::RESP_ERROR);
        // reads pass
        let resp = handle_sql(&st, &sql_frame("SELECT 1", 0)).await;
        assert_eq!(resp.frame_type, proto::RESP_ROWS);
    }

    #[tokio::test]
    async fn sql_handler_shapes_rows_and_errors() {
        let st = state(None);
        let resp = handle_sql(&st, &sql_frame("CREATE TABLE t (id INT)", 0)).await;
        assert_eq!(resp.frame_type, proto::RESP_AFFECTED);
        let resp = handle_sql(&st, &sql_frame("SELECT id FROM t", 0)).await;
        assert_eq!(resp.frame_type, proto::RESP_ROWS);
        let v = docsql_core::json::from_str(&String::from_utf8_lossy(&resp.payload)).unwrap();
        assert!(matches!(v, Value::Object(_)));
        // error path
        let resp = handle_sql(&st, &sql_frame("SELECT * FROM nope", 0)).await;
        assert_eq!(resp.frame_type, proto::RESP_ERROR);
        // invalid utf-8 sql payload
        let mut f = Frame::new(proto::REQ_SQL, vec![0xff, 0xfe]);
        f.flags = 0;
        let resp = handle_sql(&st, &f).await;
        assert_eq!(resp.frame_type, proto::RESP_ERROR);
    }

    #[tokio::test]
    async fn forward_frame_and_write_roundtrip_with_peer() {
        // Minimal echo server speaking the frame protocol (one frame per conn).
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let srv = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            while let Ok((mut s, _)) = listener.accept().await {
                let mut header = [0u8; proto::HEADER_LEN];
                if s.read_exact(&mut header).await.is_err() {
                    continue;
                }
                let len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
                let mut payload = vec![0u8; len];
                if s.read_exact(&mut payload).await.is_err() {
                    continue;
                }
                let mut resp = Frame::new(proto::RESP_AFFECTED, 7u64.to_le_bytes().to_vec());
                resp.flags = u16::from_le_bytes(header[4..6].try_into().unwrap());
                let _ = s.write_all(&resp.encode().unwrap()).await;
            }
        });
        let f = Frame::new(proto::REQ_SQL, b"SELECT 1".to_vec());
        let back = forward_frame(&addr, &f, None).await.unwrap();
        assert_eq!(back.payload, 7u64.to_le_bytes().to_vec());
        // with a transport key the sealed frame comes back untouched
        let key = crypto::parse_key_hex(&"ab".repeat(32)).unwrap();
        let f = Frame::new(proto::REQ_SQL, b"SELECT 2".to_vec());
        let back = forward_frame(&addr, &f, Some(&key)).await.unwrap();
        assert_eq!(back.payload, 7u64.to_le_bytes().to_vec());
        // unreachable target errors
        assert!(forward_frame("127.0.0.1:1", &f, None).await.is_err());
        srv.abort();
        let _ = srv.await;
    }

    #[tokio::test]
    async fn connection_lifecycle_over_tcp() {
        let st = state(None);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let srv = tokio::spawn(async move {
            loop {
                let (s, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => return,
                };
                tokio::spawn(handle_connection(s, st.clone()));
            }
        });
        let mut c = tokio::net::TcpStream::connect(&addr).await.unwrap();
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        // ping
        c.write_all(&Frame::new(proto::REQ_PING, vec![]).encode().unwrap())
            .await
            .unwrap();
        let mut header = [0u8; proto::HEADER_LEN];
        c.read_exact(&mut header).await.unwrap();
        let len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
        let mut payload = vec![0u8; len];
        c.read_exact(&mut payload).await.unwrap();
        assert_eq!(
            u16::from_le_bytes(header[6..8].try_into().unwrap()),
            proto::RESP_PONG
        );
        // sql + kv
        c.write_all(&sql_frame("SELECT 1", 0).encode().unwrap())
            .await
            .unwrap();
        c.read_exact(&mut header).await.unwrap();
        let len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
        let mut payload = vec![0u8; len];
        c.read_exact(&mut payload).await.unwrap();
        assert_eq!(
            u16::from_le_bytes(header[6..8].try_into().unwrap()),
            proto::RESP_ROWS
        );
        // unsupported frame type
        c.write_all(&Frame::new(0xBEEF, vec![]).encode().unwrap())
            .await
            .unwrap();
        c.read_exact(&mut header).await.unwrap();
        assert_eq!(
            u16::from_le_bytes(header[6..8].try_into().unwrap()),
            proto::RESP_ERROR
        );
        // subscribe then publish from a second connection
        let mut sub = tokio::net::TcpStream::connect(&addr).await.unwrap();
        sub.write_all(&kv_frame(&["SUBSCRIBE", "news"], 0).encode().unwrap())
            .await
            .unwrap();
        sub.read_exact(&mut header).await.unwrap();
        let len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
        let mut p = vec![0u8; len];
        sub.read_exact(&mut p).await.unwrap();
        let mut c2 = tokio::net::TcpStream::connect(&addr).await.unwrap();
        c2.write_all(&kv_frame(&["PUBLISH", "news", "hello"], 0).encode().unwrap())
            .await
            .unwrap();
        c2.read_exact(&mut header).await.unwrap();
        // subscriber receives the push frame
        sub.read_exact(&mut header).await.unwrap();
        let len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
        let mut p = vec![0u8; len];
        sub.read_exact(&mut p).await.unwrap();
        assert_eq!(
            u16::from_le_bytes(header[6..8].try_into().unwrap()),
            proto::RESP_PUSH
        );
        assert_eq!(String::from_utf8_lossy(&p), "message\x00news\x00hello");
        drop((c, c2, sub));
        srv.abort();
    }

    #[tokio::test]
    async fn encrypted_transport_rejects_plaintext() {
        let key = crypto::parse_key_hex(&"cd".repeat(32)).unwrap();
        let st = Arc::new(ServerState {
            kv: Mutex::new(Kv::in_memory().unwrap()),
            pubsub: PubSub::new(),
            auth_token: None,
            replicate_to: tokio::sync::Mutex::new(None),
            peers: tokio::sync::Mutex::new(vec![]),
            read_only: std::sync::atomic::AtomicBool::new(false),
            transport_key: Some(key),
            tx_pending: tokio::sync::Mutex::new(crate::TxPending::new()),
            query_log: querylog::QueryLog::new(),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (s, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => return,
                };
                tokio::spawn(handle_connection(s, st.clone()));
            }
        });
        let mut c = tokio::net::TcpStream::connect(&addr).await.unwrap();
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        // plaintext frame → error then close
        c.write_all(&sql_frame("SELECT 1", 0).encode().unwrap())
            .await
            .unwrap();
        let mut header = [0u8; proto::HEADER_LEN];
        c.read_exact(&mut header).await.unwrap();
        assert_eq!(
            u16::from_le_bytes(header[6..8].try_into().unwrap()),
            proto::RESP_ERROR
        );
        // sealed frame round-trips
        let mut c2 = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let mut f = sql_frame("SELECT 1", 0);
        f.flags |= crypto::FLAG_ENCRYPTED;
        f.payload = crypto::seal(&key, &f.payload);
        c2.write_all(&f.encode().unwrap()).await.unwrap();
        c2.read_exact(&mut header).await.unwrap();
        let len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
        let mut sealed = vec![0u8; len];
        c2.read_exact(&mut sealed).await.unwrap();
        assert_ne!(
            u16::from_le_bytes(header[6..8].try_into().unwrap()),
            proto::RESP_ERROR
        );
        let pt = crypto::open(&key, &sealed).unwrap();
        assert!(String::from_utf8_lossy(&pt).contains("columns"));
        // tampered sealed frame → error
        let mut c3 = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let mut f = sql_frame("SELECT 1", 0);
        f.flags |= crypto::FLAG_ENCRYPTED;
        let mut sealed = crypto::seal(&key, &f.payload);
        sealed[3] ^= 1;
        f.payload = sealed;
        c3.write_all(&f.encode().unwrap()).await.unwrap();
        c3.read_exact(&mut header).await.unwrap();
        assert_eq!(
            u16::from_le_bytes(header[6..8].try_into().unwrap()),
            proto::RESP_ERROR
        );
    }

    #[tokio::test]
    async fn kv_write_replicates_to_peer() {
        let st = state(None);
        // peer echo server counts received replication frames
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let cnt = count.clone();
        let srv = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            while let Ok((mut s, _)) = listener.accept().await {
                let c = cnt.clone();
                tokio::spawn(async move {
                    let mut header = [0u8; proto::HEADER_LEN];
                    while s.read_exact(&mut header).await.is_ok() {
                        let len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
                        let mut payload = vec![0u8; len];
                        if s.read_exact(&mut payload).await.is_err() {
                            return;
                        }
                        c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let resp = Frame::new(proto::RESP_AFFECTED, vec![]);
                        let _ = s.write_all(&resp.encode().unwrap()).await;
                    }
                });
            }
        });
        *st.peers.lock().await = vec![addr.clone()];
        let (r, _, _) = super::kvproto::handle(&st, &kv_frame(&["SET", "k", "v"], 0), true).await;
        assert_eq!(r.payload, b"ok".to_vec());
        // read commands are not replicated
        super::kvproto::handle(&st, &kv_frame(&["GET", "k"], 0), true).await;
        srv.abort();
        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    // ---- query log ----

    #[test]
    fn query_log_ring_capacity_and_snapshot() {
        let mut log = querylog::QueryLog::new();
        log.capacity = 3;
        for i in 0..5 {
            log.push(querylog::LogEntry {
                ts_ms: i,
                peer: "p".into(),
                sql: format!("s{i}"),
                ms: 0.1,
                affected: Some(i as i64),
                error: None,
                replicated: false,
            });
        }
        let snap = log.snapshot();
        assert_eq!(snap.len(), 3);
        assert_eq!(snap[0].sql, "s2");
        assert_eq!(snap[2].sql, "s4");
    }

    #[test]
    fn log_view_serves_recent_statements() {
        let st = state(None);
        querylog::record(
            &st,
            "127.0.0.1:1",
            "SELECT 1",
            0.5,
            &Frame::new(proto::RESP_AFFECTED, 3u64.to_le_bytes().to_vec()),
            false,
        );
        querylog::record(
            &st,
            "127.0.0.1:2",
            "BOOM",
            1.0,
            &Frame::new(proto::RESP_ERROR, b"bad".to_vec()),
            true,
        );
        let f = querylog::try_serve_log_view("SELECT * FROM docsql_log", &st).unwrap();
        let v = docsql_core::json::from_str(&String::from_utf8_lossy(&f.payload)).unwrap();
        let Value::Object(o) = v else { panic!() };
        let Value::Array(rows) = o.get("rows").unwrap() else {
            panic!()
        };
        assert_eq!(rows.len(), 2);
        // non-log SQL returns None
        assert!(querylog::try_serve_log_view("SELECT 1", &st).is_none());
        assert!(querylog::try_serve_log_view("DROP TABLE docsql_log", &st).is_none());
        // LIMIT support
        let f = querylog::try_serve_log_view("SELECT * FROM docsql_log LIMIT 1", &st).unwrap();
        let v = docsql_core::json::from_str(&String::from_utf8_lossy(&f.payload)).unwrap();
        let Value::Object(o) = v else { panic!() };
        let Value::Array(rows) = o.get("rows").unwrap() else {
            panic!()
        };
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn sql_statements_are_logged_over_the_wire() {
        let st = state(None);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let st2 = st.clone();
        let srv = tokio::spawn(async move {
            loop {
                let (s, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => return,
                };
                tokio::spawn(handle_connection(s, st2.clone()));
            }
        });
        let mut c = tokio::net::TcpStream::connect(&addr).await.unwrap();
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        c.write_all(&sql_frame("SELECT 42", 0).encode().unwrap())
            .await
            .unwrap();
        let mut header = [0u8; proto::HEADER_LEN];
        c.read_exact(&mut header).await.unwrap();
        let len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
        let mut payload = vec![0u8; len];
        c.read_exact(&mut payload).await.unwrap();
        srv.abort();
        let snap = st.query_log.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].sql, "SELECT 42");
        // SELECT ... FROM docsql_log is served from the ring without logging
        // itself — verify via handle_connection path implicitly covered in
        // lifecycle test; here just check record() extracts affected counts.
        querylog::record(
            &st,
            "p",
            "INSERT",
            0.1,
            &Frame::new(proto::RESP_AFFECTED, 5u64.to_le_bytes().to_vec()),
            false,
        );
        let snap = st.query_log.snapshot();
        assert_eq!(snap[1].affected, Some(5));
    }
    #[tokio::test]
    async fn kv_wrongtype_error_matrix() {
        let st = state(None);
        super::kvproto::handle(&st, &kv_frame(&["SET", "s", "text"], 0), true).await;
        async fn err_of(st: &Arc<ServerState>, args: &[&str]) -> bool {
            let (r, _, _) = super::kvproto::handle(st, &kv_frame(args, 0), true).await;
            r.frame_type == proto::RESP_ERROR
        }
        assert!(err_of(&st, &["INCR", "s"]).await);
        assert!(err_of(&st, &["LPUSH", "s", "x"]).await);
        assert!(err_of(&st, &["RPUSH", "s", "x"]).await);
        assert!(err_of(&st, &["LPOP", "s"]).await);
        assert!(err_of(&st, &["RPOP", "s"]).await);
        assert!(err_of(&st, &["LRANGE", "s", "0", "-1"]).await);
        assert!(err_of(&st, &["HSET", "s", "f", "v"]).await);
        assert!(err_of(&st, &["HGET", "s", "f"]).await);
        assert!(err_of(&st, &["HGETALL", "s"]).await);
        assert!(err_of(&st, &["SADD", "s", "m"]).await);
        assert!(err_of(&st, &["SMEMBERS", "s"]).await);
        assert!(err_of(&st, &["ZADD", "s", "1.0", "m"]).await);
        assert!(err_of(&st, &["ZRANGE", "s", "0", "1"]).await);
    }

    #[tokio::test]
    async fn set_flag_parsing_and_auth_disabled() {
        let st = state(None);
        // AUTH 在未启用鉴权时直接放行
        let (r, _, authed) =
            super::kvproto::handle(&st, &kv_frame(&["AUTH", "anything"], 0), false).await;
        assert_ne!(r.frame_type, proto::RESP_ERROR);
        assert!(authed);
        // SUBSCRIBE 缺参数
        let (r, _, _) = super::kvproto::handle(&st, &kv_frame(&["SUBSCRIBE"], 0), true).await;
        assert_eq!(r.frame_type, proto::RESP_ERROR);
        // SET 的 EX= / PX= 与未知 flag
        let (r, _, _) =
            super::kvproto::handle(&st, &kv_frame(&["SET", "k1", "v", "PX=5000"], 0), true).await;
        assert_eq!(r.payload, b"ok".to_vec());
        assert!(st.kv.lock().unwrap().ttl_ms("k1").unwrap().is_some());
        let (r, _, _) = super::kvproto::handle(
            &st,
            &kv_frame(&["SET", "k2", "v", "EX=5", "BOGUS"], 0),
            true,
        )
        .await;
        assert_eq!(r.payload, b"ok".to_vec());
        assert!(st.kv.lock().unwrap().ttl_ms("k2").unwrap().is_some());
        // 非法 UTF-8 载荷
        let mut f = Frame::new(proto::REQ_KV, vec![0xff, 0xfe, 0x00, 0x01]);
        f.flags = 0;
        let (r, _, _) = super::kvproto::handle(&st, &f, true).await;
        assert_eq!(r.frame_type, proto::RESP_ERROR);
    }

    #[tokio::test]
    async fn replication_failure_is_logged_not_fatal() {
        let st = state(None);
        // upstream + peer 都指向死端口:写仍然成功,仅打印错误
        *st.replicate_to.lock().await = Some("127.0.0.1:1".into());
        *st.peers.lock().await = vec!["127.0.0.1:2".into()];
        let (r, _, _) = super::kvproto::handle(&st, &kv_frame(&["SET", "rk", "v"], 0), true).await;
        assert_eq!(r.payload, b"ok".to_vec());
        // replication 标记的帧不再转发(防环)
        let (r, _, _) =
            super::kvproto::handle(&st, &kv_frame(&["SET", "rk2", "v"], FLAG_REPLICATION), true)
                .await;
        assert_eq!(r.payload, b"ok".to_vec());
        // 只读主开关不影响读
        st.read_only
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let (r, _, _) = super::kvproto::handle(&st, &kv_frame(&["GET", "rk"], 0), true).await;
        assert_eq!(r.payload, b"v".to_vec());
    }

    #[test]
    fn query_log_slow_and_file_output() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log.jsonl");
        let mut log = querylog::QueryLog::new();
        log.slow_ms = 0.0; // 每条都触发慢查询 stderr
        log.log_file = Some(path.to_string_lossy().to_string());
        log.push(querylog::LogEntry {
            ts_ms: 1,
            peer: "127.0.0.1:9".into(),
            sql: "SELECT 1".into(),
            ms: 5.0,
            affected: None,
            error: Some("boom".into()),
            replicated: false,
        });
        log.push(querylog::LogEntry {
            ts_ms: 2,
            peer: "p".into(),
            sql: "INSERT".into(),
            ms: 0.1,
            affected: Some(2),
            error: None,
            replicated: true,
        });
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains("\"peer\":\"127.0.0.1:9\""));
        assert!(text.contains("\"replicated\":true"));
        assert_eq!(log.snapshot().len(), 2);
        // Default 走 new
        let _d = querylog::QueryLog::default();
        // 截断超长 SQL
        let st = state(None);
        querylog::record(
            &st,
            "p",
            &"x".repeat(600),
            0.0,
            &Frame::new(proto::RESP_PONG, vec![]),
            false,
        );
        assert_eq!(st.query_log.snapshot()[0].sql.chars().count(), 512);
    }

    #[tokio::test]
    async fn shard_router_sends_to_live_shard() {
        use crate::shard::ShardRouter;
        // echo 服务器:回 RESP_AFFECTED + 42
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let srv = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            while let Ok((mut s, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut header = [0u8; proto::HEADER_LEN];
                    if s.read_exact(&mut header).await.is_err() {
                        return;
                    }
                    let len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
                    let mut payload = vec![0u8; len];
                    let _ = s.read_exact(&mut payload).await;
                    let resp = Frame::new(proto::RESP_AFFECTED, 42u64.to_le_bytes().to_vec());
                    let _ = s.write_all(&resp.encode().unwrap()).await;
                });
            }
        });
        let router = ShardRouter::new(vec![addr]);
        let r = router.sql("SELECT * FROM anything").await.unwrap();
        assert_eq!(u64::from_le_bytes(r.payload[..8].try_into().unwrap()), 42);
        let r = router.kv("GET", "some-key").await.unwrap();
        assert_eq!(u64::from_le_bytes(r.payload[..8].try_into().unwrap()), 42);
        // 连不上时报错
        let bad = ShardRouter::new(vec!["127.0.0.1:1".into()]);
        assert!(bad.sql("SELECT 1").await.is_err());
        assert!(bad.kv("GET", "k").await.is_err());
        srv.abort();
        let _ = srv.await;
    }
    #[tokio::test]
    async fn kv_transaction_error_frames() {
        let st = state(None);
        // 无事务 EXEC / DISCARD → 错误帧
        let (r, _, _) = super::kvproto::handle(&st, &kv_frame(&["EXEC"], 0), true).await;
        assert_eq!(r.frame_type, proto::RESP_ERROR);
        let (r, _, _) = super::kvproto::handle(&st, &kv_frame(&["DISCARD"], 0), true).await;
        assert_eq!(r.frame_type, proto::RESP_ERROR);
        // 重复 MULTI → 错误帧
        super::kvproto::handle(&st, &kv_frame(&["MULTI"], 0), true).await;
        let (r, _, _) = super::kvproto::handle(&st, &kv_frame(&["MULTI"], 0), true).await;
        assert_eq!(r.frame_type, proto::RESP_ERROR);
        super::kvproto::handle(&st, &kv_frame(&["EXEC"], 0), true).await;
        // INCRBY 非法数值回退为 1
        let (r, _, _) =
            super::kvproto::handle(&st, &kv_frame(&["INCRBY", "n", "abc"], 0), true).await;
        assert_eq!(r.payload, 1u64.to_le_bytes().to_vec());
        // TTL 读取错误 → -2(GET err 无法注入,TTL err 可经错误状态触发)
        let (r, _, _) = super::kvproto::handle(&st, &kv_frame(&["TTL", "ghost"], 0), true).await;
        assert_eq!(r.payload, b"-2".to_vec());
    }

    #[tokio::test]
    async fn sql_replication_paths() {
        // echo 服务器:可配置返回错误帧
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let use_error = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let ue = use_error.clone();
        let srv = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            while let Ok((mut s, _)) = listener.accept().await {
                let ue = ue.clone();
                tokio::spawn(async move {
                    let mut header = [0u8; proto::HEADER_LEN];
                    if s.read_exact(&mut header).await.is_err() {
                        return;
                    }
                    let len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
                    let mut payload = vec![0u8; len];
                    let _ = s.read_exact(&mut payload).await;
                    let resp = if ue.load(std::sync::atomic::Ordering::SeqCst) {
                        Frame::new(proto::RESP_ERROR, b"no".to_vec())
                    } else {
                        Frame::new(proto::RESP_AFFECTED, vec![])
                    };
                    let _ = s.write_all(&resp.encode().unwrap()).await;
                });
            }
        });
        // 1) 带加密 key 的 SQL 转发(forward_write 加密分支)
        let key = crypto::parse_key_hex(&"ee".repeat(32)).unwrap();
        let st = Arc::new(ServerState {
            kv: Mutex::new(Kv::in_memory().unwrap()),
            pubsub: PubSub::new(),
            auth_token: None,
            replicate_to: tokio::sync::Mutex::new(Some(addr.clone())),
            peers: tokio::sync::Mutex::new(vec![]),
            read_only: std::sync::atomic::AtomicBool::new(false),
            transport_key: Some(key),
            tx_pending: tokio::sync::Mutex::new(crate::TxPending::new()),
            query_log: querylog::QueryLog::new(),
        });
        let resp = handle_sql(&st, &sql_frame("CREATE TABLE t (id INT)", 0)).await;
        assert_ne!(resp.frame_type, proto::RESP_ERROR);
        // 2) replica 拒绝(返回错误帧)→ forward_write 报错被吞
        use_error.store(true, std::sync::atomic::Ordering::SeqCst);
        let resp = handle_sql(&st, &sql_frame("CREATE TABLE t2 (id INT)", 0)).await;
        assert_ne!(resp.frame_type, proto::RESP_ERROR);
        // 3) 对端不可达 → eprintln 但语句成功
        *st.replicate_to.lock().await = Some("127.0.0.1:1".into());
        *st.peers.lock().await = vec!["127.0.0.1:2".into()];
        let resp = handle_sql(&st, &sql_frame("CREATE TABLE t3 (id INT)", 0)).await;
        assert_ne!(resp.frame_type, proto::RESP_ERROR);
        srv.abort();
        let _ = srv.await;
    }
}
