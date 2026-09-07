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
