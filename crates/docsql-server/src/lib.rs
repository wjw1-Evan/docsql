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

pub mod kvproto;

use docsql_core::engine::ExecOutcome;
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
    /// Replicas reject client writes until promoted.
    pub read_only: std::sync::atomic::AtomicBool,
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
    /// Replica mode: reject client writes (replication frames excepted)
    /// until promoted via PROMOTE.
    pub read_only: bool,
}

pub async fn run(cfg: ServerConfig) -> std::io::Result<()> {
    let kv = Kv::open(&cfg.db_path).map_err(|e| std::io::Error::other(format!("open db: {e}")))?;
    let state = Arc::new(ServerState {
        kv: Mutex::new(kv),
        pubsub: PubSub::new(),
        auth_token: cfg.auth_token,
        replicate_to: tokio::sync::Mutex::new(cfg.replicate_to.clone()),
        read_only: std::sync::atomic::AtomicBool::new(cfg.read_only),
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
    let (rd, mut wr) = stream.into_split();
    let mut conn = Conn {
        stream: rd,
        buf: Vec::new(),
    };
    let (tx, mut rx) = mpsc::channel::<Frame>(256);

    // Writer task: serializes responses + pub/sub pushes.
    let writer = tokio::spawn(async move {
        while let Some(f) = rx.recv().await {
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
        while let Some(frame) = conn.read_frame().await? {
            let resp = match frame.frame_type {
                proto::REQ_PING => Frame::new(proto::RESP_PONG, vec![]),
                proto::REQ_SQL if authed => handle_sql(&state, &frame).await,
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
    let outcome = {
        let mut kv = state.kv.lock().unwrap();
        kv.db.execute(&sql)
    };
    // Replicate successful writes to the upstream (async, best-effort log).
    if !is_replication && matches!(outcome, Ok(ExecOutcome::Affected(_))) {
        if let Some(target) = state.replicate_to.lock().await.clone() {
            if let Err(e) = forward_write(&target, &sql).await {
                eprintln!("replication to {target} failed: {e}");
            }
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

/// Send one write to the replication upstream.
async fn forward_write(target: &str, sql: &str) -> std::io::Result<()> {
    let mut stream = tokio::net::TcpStream::connect(target).await?;
    let mut frame = Frame::new(proto::REQ_SQL, proto::encode_sql(sql).unwrap_or_default());
    frame.flags = FLAG_REPLICATION;
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
