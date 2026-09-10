//! docsql network server.
//!
//! Speaks the v1 binary protocol from `docsql_core::proto`:
//! - REQ_SQL       → RESP_ROWS / RESP_AFFECTED / RESP_ERROR
//! - REQ_AUTH      → session token authentication (RESP_AFFECTED/RESP_ERROR)
//! - REQ_PROMOTE   → clear read-only replica mode (failover)
//! - REQ_PING      → RESP_PONG
//! - REQ_STATUS    → RESP_STATUS: node state JSON for cluster monitoring
//! - REQ_SUBSCRIBE / REQ_PSUBSCRIBE / REQ_UNSUBSCRIBE / REQ_PUNSUBSCRIBE
//!   → pub/sub subscription management; history replay rides
//!   RESP_PUSH frames after the confirmation
//! - REQ_PUBLISH   → persist + fan out one pub/sub message
//! - REQ_PUBSUB    → channels / numsub / numpat introspection, trim
//! - REQ_LOGS      → RESP_LOGS: recent statement-audit entries + sync
//!   events (query-log ring + replication fan-out trail) for the web
//!   console's logs page
//! - REQ_BACKUP    → RESP_BACKUP: backup directory listing + last attempt,
//!   or trigger one backup now (automatic backups run on a timer, see
//!   backup.rs)
//! - REQ_SYNC      → RESP_SYNC chunks + RESP_AFFECTED: cluster join — a
//!   fresh node pulls the cluster's full state (see the join section)
//! - REQ_DIGEST    → RESP_DIGEST: per-table replication fingerprints; the
//!   rejoin repair compares these to find writes a node missed while
//!   offline and pulls a fresh snapshot from a majority peer
//! - REQ_HOLD / REQ_RELEASE → cluster-join quiesce helpers exchanged
//!   between nodes while a snapshot is taken
//!
//! Every connection shares one engine instance behind a mutex (single-writer
//! v1; the cluster milestone brings per-shard concurrency).

pub mod backup;
pub mod crypto;
pub mod pubsub;
pub mod querylog;

use docsql_core::engine::{Database, ExecOutcome, TableDigest, TxControl};
use docsql_core::proto::{self, Frame};
use docsql_core::value::Value;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

/// Plain error payload shared by all frame handlers.
pub fn err_payload(msg: &str) -> Vec<u8> {
    msg.as_bytes().to_vec()
}

pub struct ServerState {
    pub db: Mutex<Database>,
    pub auth_token: Option<String>,
    /// Read-only client credential (DOCSQL_READ_TOKEN). REQ_AUTH with it
    /// marks the connection as a least-privilege client: SELECT and
    /// subscription frames pass, every write (SQL, PUBLISH, PROMOTE) is
    /// rejected. Fan-out never presents it.
    pub read_token: Option<String>,
    /// Inter-node credential (DOCSQL_CLUSTER_TOKEN). When set, REQ_AUTH
    /// with this token marks the connection as a peer node and only peer
    /// connections may carry FLAG_REPLICATION frames; fan-out presents it
    /// to peers in preference to `auth_token`.
    pub cluster_token: Option<String>,
    /// Auth-failure lockout (identity-authentication failure handling):
    /// source IP -> failure timestamps. At AUTH_LOCK_THRESHOLD failures
    /// inside AUTH_LOCK_WINDOW the source is locked out for
    /// AUTH_LOCKOUT and further AUTH attempts are rejected unread.
    pub auth_failures:
        tokio::sync::Mutex<std::collections::HashMap<String, Vec<std::time::Instant>>>,
    /// Live connection budget (resource control). Acquired per accepted
    /// connection, released on close; None = unlimited.
    pub conn_slots: Option<std::sync::Arc<tokio::sync::Semaphore>>,
    /// Auth-failure lockout threshold per source IP (0 disables lockout).
    pub auth_lock_threshold: u32,
    /// Idle-session timeout: connections silent this long are closed by
    /// the server. None = unlimited (subscriptions rely on it).
    pub idle_timeout: Option<std::time::Duration>,
    /// Upstream replication target (empty when not replicating).
    pub replicate_to: tokio::sync::Mutex<Option<String>>,
    /// Symmetric peers: every successful write is forwarded to all of them
    /// and every node accepts writes (no primary/replica roles).
    pub peers: tokio::sync::Mutex<Vec<String>>,
    /// SQL write statements executed inside the open engine transaction,
    /// in execution order. They reach peers only when the transaction
    /// commits; a rollback discards them.
    pub tx_pending: tokio::sync::Mutex<TxPending>,
    /// The connection that opened the engine's global transaction, if any.
    /// Writes from any other connection (and replication applies) queue
    /// behind it instead of silently joining — the owner's ROLLBACK would
    /// otherwise drop writes the actor already acknowledged.
    pub tx_owner: std::sync::Mutex<Option<u64>>,
    /// Serializes write execution together with its fan-out so peers apply
    /// writes in the order this node executed them (and the
    /// buffer-vs-forward classification is race-free). Shared as an Arc so
    /// cluster-join holds can keep an `OwnedMutexGuard` past the handler.
    pub write_order: std::sync::Arc<tokio::sync::Mutex<()>>,
    /// Write-path holds taken by a peer serving REQ_SYNC (cluster-join
    /// quiesce): hold id -> guard. Dropping the guard releases the freeze.
    pub holds: tokio::sync::Mutex<std::collections::HashMap<u64, tokio::sync::OwnedMutexGuard<()>>>,
    /// Monotonic id source for REQ_HOLD grants.
    pub next_hold_id: std::sync::atomic::AtomicU64,
    /// Cluster-join intake: while this node bootstraps, replication writes
    /// are queued here and acknowledged immediately — stalling the origin's
    /// fan-out on a gate would hold its write path (fan-out runs inside it)
    /// and deadlock the very REQ_SYNC that needs that path. Replay order is
    /// the total order convergence needs: dump, then queue in arrival
    /// order, then direct applies once closed. Everything queued postdates
    /// the snapshot (origins only fan out to the joiner after registering
    /// it), so nothing replays twice.
    pub sync_queue: tokio::sync::Mutex<SyncGate>,
    /// Address this node advertises to the cluster when joining
    /// (DOCSQL_ADVERTISE), empty when it relies on static peer config.
    pub advertise: Option<String>,
    /// Listen address (self-peer detection for dynamic registration).
    pub listen: String,
    /// Replicas reject client writes until promoted.
    pub read_only: std::sync::atomic::AtomicBool,
    /// When set, every frame payload is sealed with AES-256-GCM.
    pub transport_key: Option<crypto::TransportKey>,
    /// Statement audit log (docsql_log view).
    pub query_log: querylog::QueryLog,
    /// Replication/sync event trail served by REQ_LOGS (write fan-out,
    /// pub/sub fan-out, PROMOTE). Same capacity as the query log.
    pub sync_log: querylog::SyncLog,
    /// Live pub/sub subscribers (messages persist in the engine's
    /// `_pubsub_messages` table; this tracks who gets pushed).
    pub pubsub: pubsub::PubSub,
    /// This node's persistent random identity (see CLUSTER_ID_TABLE):
    /// sent with every sequenced replication write so peers can record
    /// catch-up positions under a stable key.
    pub cluster_id: String,
    /// Catch-up journal window in entries (0 = unbounded); the journal
    /// is trimmed periodically as it grows past this.
    pub catchup_window: u64,
    /// Automatic backup cadence in seconds (0 = disabled).
    pub backup_interval_secs: u64,
    /// Backups retained per node (oldest pruned after each backup).
    pub backup_keep: usize,
    /// Backup directory (default `<db dir>/backups`).
    pub backup_dir: PathBuf,
    /// Backup shared state: in-flight flag + last attempt outcome.
    /// Never held while acquiring `write_order`/the engine (see backup.rs).
    pub backup: Mutex<backup::BackupShared>,
    /// Data file location (status reports its size on disk).
    pub db_path: PathBuf,
    /// Server start time (status reports uptime).
    pub started: std::time::Instant,
}

/// Replication buffer for the open engine transaction. Savepoint marks
/// mirror engine savepoints so ROLLBACK TO SAVEPOINT trims the tail.
#[derive(Default)]
pub struct TxPending {
    pub writes: Vec<String>,
    marks: Vec<(String, usize)>,
}

impl TxPending {
    pub fn new() -> Self {
        Self::default()
    }

    /// SAVEPOINT name: remember the buffer length to roll back to.
    pub fn mark(&mut self, name: &str) {
        self.marks.push((name.to_string(), self.writes.len()));
    }

    /// ROLLBACK TO SAVEPOINT name: drop writes past the mark (and later marks).
    pub fn rollback_to(&mut self, name: &str) {
        if let Some(pos) = self.marks.iter().position(|(n, _)| n == name) {
            let (_, len) = self.marks[pos].clone();
            self.writes.truncate(len);
            self.marks.truncate(pos);
        }
    }

    /// RELEASE SAVEPOINT name: forget the mark, keep the writes.
    pub fn release(&mut self, name: &str) {
        if let Some(pos) = self.marks.iter().position(|(n, _)| n == name) {
            self.marks.truncate(pos);
        }
    }

    pub fn clear(&mut self) {
        self.writes.clear();
        self.marks.clear();
    }
}

/// Flags bit 1 marks replication-internal frames (bypasses read-only).
pub const FLAG_REPLICATION: u16 = 0x0002;
/// Group-commit flush cadence in async-commit mode (the advertised ~2ms
/// loss window on power failure).
const ASYNC_COMMIT_INTERVAL_MS: u64 = 2;

/// Join-intake queue state; see [ServerState::sync_queue]. Opens (closed =
/// false) on a node that starts with peers configured — fresh (join
/// bootstrap) or holding data (rejoin repair); every other node starts
/// closed and never queues.
#[derive(Default)]
pub struct SyncGate {
    /// Replication writes acknowledged but not yet applied.
    pub pending: Vec<String>,
    /// True once bootstrap concluded: no more queuing, direct applies.
    pub closed: bool,
}

/// How a connection authenticated. `Peer` connections (cluster token)
/// carry node-to-node traffic; when a cluster token is configured, client
/// connections may not send FLAG_REPLICATION frames and peer connections
/// may send nothing else.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ConnRole {
    Unauthed,
    Client,
    /// Least-privilege client (read-only token): may read and subscribe,
    /// may not write.
    ReadOnly,
    Peer,
}

pub struct ServerConfig {
    pub db_path: PathBuf,
    pub listen: String,
    pub auth_token: Option<String>,
    /// Read-only client credential (DOCSQL_READ_TOKEN); connections
    /// authenticated with it may read and subscribe but not write.
    pub read_token: Option<String>,
    /// Maximum concurrent connections (resource control); 0 = unlimited.
    pub max_conn: usize,
    /// Idle-session timeout in seconds; 0 = unlimited.
    pub idle_timeout_secs: u64,
    /// Auth-failure lockout threshold per source IP (0 disables lockout).
    pub auth_lock_threshold: u32,
    /// Cluster-node credential. When set, node-to-node (FLAG_REPLICATION)
    /// frames are accepted only from connections that authenticated with
    /// it, and peer connections may send nothing else. Nodes in one
    /// cluster must share the value; leave unset to keep the historical
    /// behavior where any authenticated connection may replicate.
    pub cluster_token: Option<String>,
    /// Replication upstream: every successful write is forwarded here
    /// (host:port of a docsql-server in replica mode).
    pub replicate_to: Option<String>,
    /// Symmetric-cluster peers (host:port list). In this mode there is no
    /// read-only role: any node accepts writes and fans them out.
    pub peers: Vec<String>,
    /// Address other nodes should use to reach this one when it joins a
    /// cluster (host:port). A fresh node (no user tables) with peers
    /// configured automatically pulls a full snapshot from the first peer
    /// that answers and registers itself cluster-wide; empty disables the
    /// dynamic join (static peer config then has to list every node).
    pub advertise: Option<String>,
    /// Replica mode: reject client writes (replication frames excepted)
    /// until promoted via PROMOTE.
    pub read_only: bool,
    /// Transport encryption key (32 bytes); None = plaintext frames.
    pub transport_key: Option<crypto::TransportKey>,
    /// Async-commit mode: statement commits skip the WAL fsync; a
    /// background flusher batches fsyncs every ~2ms (MongoDB-style
    /// journal interval). Bounded loss window on power failure.
    pub async_commit: bool,
    /// Catch-up journal window in entries: how many locally-committed
    /// writes `_cluster_log` retains for rejoined peers to incrementally
    /// catch up (DOCSQL_CATCHUP_WINDOW). A peer positioned older than the
    /// window falls back to a full snapshot. 0 = unbounded.
    pub catchup_window: u64,
    /// Automatic backup cadence in seconds (0 = disabled;
    /// DOCSQL_BACKUP_INTERVAL_SECS, default 86400 = daily).
    pub backup_interval_secs: u64,
    /// Backups retained per node, oldest pruned (DOCSQL_BACKUP_KEEP).
    pub backup_keep: usize,
    /// Backup directory override; None = `<db dir>/backups`
    /// (DOCSQL_BACKUP_DIR).
    pub backup_dir: Option<PathBuf>,
}

pub async fn run(cfg: ServerConfig) -> std::io::Result<()> {
    let mut db =
        Database::open(&cfg.db_path).map_err(|e| std::io::Error::other(format!("open db: {e}")))?;
    db.set_async_commit(cfg.async_commit);
    pubsub::ensure_table(&mut db)
        .map_err(|e| std::io::Error::other(format!("pubsub store: {e}")))?;
    db.ensure_cluster_tables()
        .map_err(|e| std::io::Error::other(format!("catchup store: {e}")))?;
    if cfg.catchup_window > 0 {
        db.journal_trim(cfg.catchup_window)
            .map_err(|e| std::io::Error::other(format!("catchup trim: {e}")))?;
    }
    let cluster_id = db
        .cluster_id()
        .map_err(|e| std::io::Error::other(format!("cluster id: {e}")))?;
    // Drop peer entries that point at ourselves: forwarding to self would
    // double-apply every write locally.
    let peers: Vec<String> = cfg
        .peers
        .into_iter()
        .filter(|p| {
            if is_self_peer(&cfg.listen, p) {
                eprintln!("ignoring self-referencing peer entry {p}");
                false
            } else {
                true
            }
        })
        .collect();
    if let (Some(cluster), Some(client)) = (&cfg.cluster_token, &cfg.auth_token) {
        if cluster == client {
            eprintln!(
                "warning: DOCSQL_CLUSTER_TOKEN equals DOCSQL_TOKEN — \
                 cluster connections also hold client privileges; use distinct tokens"
            );
        }
    }
    // A node with peers configured syncs at startup: a fresh one (only the
    // pubsub system table exists) bootstraps the cluster state; one that
    // already holds data compares table digests with the peers and, if it
    // missed writes while it was away, adopts the cluster's snapshot
    // (rejoin repair). The sync gate stays open until the startup flow
    // concludes, so replication writes arriving mid-flow queue and land
    // after the snapshot — snapshot < queued < direct is the total order.
    let peers_configured = !peers.is_empty();
    let fresh = peers_configured
        && !db
            .catalog()
            .iter()
            .any(|t| !docsql_core::engine::is_system_table(&t.name));
    let backup_dir = cfg.backup_dir.clone().unwrap_or_else(|| {
        let mut dir = cfg
            .db_path
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_default();
        dir.push("backups");
        dir
    });
    let state = Arc::new(ServerState {
        db: Mutex::new(db),
        auth_token: cfg.auth_token,
        read_token: cfg.read_token,
        cluster_token: cfg.cluster_token,
        auth_failures: tokio::sync::Mutex::new(std::collections::HashMap::new()),
        conn_slots: (cfg.max_conn > 0)
            .then(|| std::sync::Arc::new(tokio::sync::Semaphore::new(cfg.max_conn))),
        auth_lock_threshold: cfg.auth_lock_threshold,
        idle_timeout: (cfg.idle_timeout_secs > 0)
            .then(|| std::time::Duration::from_secs(cfg.idle_timeout_secs)),
        replicate_to: tokio::sync::Mutex::new(cfg.replicate_to),
        peers: tokio::sync::Mutex::new(peers),
        tx_pending: tokio::sync::Mutex::new(TxPending::new()),
        tx_owner: std::sync::Mutex::new(None),
        write_order: std::sync::Arc::new(tokio::sync::Mutex::new(())),
        holds: tokio::sync::Mutex::new(std::collections::HashMap::new()),
        next_hold_id: std::sync::atomic::AtomicU64::new(1),
        sync_queue: tokio::sync::Mutex::new(SyncGate {
            pending: Vec::new(),
            closed: !peers_configured,
        }),
        advertise: cfg.advertise.clone(),
        listen: cfg.listen.clone(),
        read_only: std::sync::atomic::AtomicBool::new(cfg.read_only),
        transport_key: cfg.transport_key,
        query_log: querylog::QueryLog::new(),
        sync_log: querylog::SyncLog::new(1000),
        pubsub: pubsub::PubSub::new(),
        cluster_id,
        catchup_window: cfg.catchup_window,
        backup_interval_secs: cfg.backup_interval_secs,
        backup_keep: cfg.backup_keep,
        backup_dir,
        backup: Mutex::new(backup::BackupShared::default()),
        db_path: cfg.db_path,
        started: std::time::Instant::now(),
    });
    let listener = TcpListener::bind(&cfg.listen).await?;
    eprintln!("docsql-server listening on {}", cfg.listen);
    if peers_configured {
        if fresh {
            eprintln!("fresh node with peers configured: bootstrapping cluster state");
        } else {
            eprintln!("node with peers configured: comparing cluster digests (rejoin repair)");
        }
        let st = state.clone();
        tokio::spawn(bootstrap_sync(st, fresh));
    }
    if cfg.async_commit {
        // Group commit: statements committed deferred since the last tick
        // share one WAL fsync here. The data-file write-back and the WAL
        // checkpoint also ride this task (the per-commit fsync path used to
        // own them, so async mode must drive them or the WAL grows forever).
        let st = state.clone();
        tokio::spawn(async move {
            let mut tick =
                tokio::time::interval(std::time::Duration::from_millis(ASYNC_COMMIT_INTERVAL_MS));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let flushed = st
                    .db
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .sync_pending();
                if let Err(e) = flushed {
                    eprintln!("async-commit flush failed: {e}");
                }
            }
        });
    }
    if cfg.backup_interval_secs > 0 {
        // Automatic backups: a logical dump on a timer (first tick is
        // immediate, so a restart yields a fresh backup). Ticks during the
        // startup sync are skipped inside the task.
        let st = state.clone();
        tokio::spawn(backup::backup_task(st, cfg.backup_interval_secs));
    }
    loop {
        let (stream, peer) = listener.accept().await?;
        let state = state.clone();
        // Resource control: hold one slot per live connection. Over the
        // limit the connection is answered with an error and closed —
        // never queued, so a flooding client cannot pin server memory.
        let slot = match &state.conn_slots {
            Some(sem) => match sem.clone().try_acquire_owned() {
                Ok(g) => Some(g),
                Err(_) => {
                    let mut s = stream;
                    let msg = Frame::new(
                        proto::RESP_ERROR,
                        err_payload("too many connections; retry later"),
                    );
                    use tokio::io::AsyncWriteExt;
                    let _ = s.write_all(&msg.encode().unwrap_or_default()).await;
                    continue;
                }
            },
            None => None,
        };
        tokio::spawn(async move {
            let result = handle_connection(stream, state).await;
            drop(slot);
            if let Err(e) = result {
                eprintln!("connection {peer} closed: {e}");
            }
        });
    }
}

/// True when `peer` resolves to a loopback address on the port we listen on
/// (or is literally the listen address): forwarding there would loop back.
fn is_self_peer(listen: &str, peer: &str) -> bool {
    if listen == peer {
        return true;
    }
    use std::net::ToSocketAddrs;
    let Some(port) = listen
        .rsplit_once(':')
        .and_then(|(_, p)| p.parse::<u16>().ok())
    else {
        return false;
    };
    match peer.to_socket_addrs() {
        Ok(addrs) => addrs
            .collect::<Vec<_>>()
            .iter()
            .any(|a| a.port() == port && a.ip().is_loopback()),
        Err(_) => false,
    }
}

struct Conn {
    stream: tokio::net::tcp::OwnedReadHalf,
    buf: Vec<u8>,
}

impl Conn {
    async fn read_frame(&mut self) -> std::io::Result<Option<Frame>> {
        loop {
            match Frame::decode(&self.buf) {
                Ok((f, n)) => {
                    self.buf.drain(..n);
                    return Ok(Some(f));
                }
                // Truncated is the only recoverable case: read more bytes.
                Err(docsql_core::proto::ProtoError::Truncated(..)) => {}
                // Anything else (e.g. bad magic) is permanent garbage —
                // failing fast beats buffering up to the 64 MB cap.
                Err(e) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("protocol: {e}"),
                    ))
                }
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

    // Writer task: serializes responses (sealing when a transport key is
    // configured).
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
            // A stalled peer (zero TCP window, suspended laptop) must not
            // wedge the write path: replay and publish notify block on this
            // channel, so an unbounded write eventually deadlocks the node.
            let wrote = tokio::time::timeout(IO_TIMEOUT, async {
                wr.write_all(&bytes).await?;
                wr.flush().await
            })
            .await;
            match wrote {
                Ok(Ok(())) => {}
                _ => break, // peer gone or stalled past the IO budget
            }
        }
        let _ = wr.shutdown().await;
    });

    let conn_id = state.pubsub.next_conn_id();
    // Without a client token every connection starts authenticated as a
    // client (auth disabled); a cluster or read-only token alone does not
    // gate clients.
    let mut role = if state.auth_token.is_none() {
        ConnRole::Client
    } else {
        ConnRole::Unauthed
    };
    // Auth-failure lockout: a source past the threshold is rejected unread
    // until the lockout elapses (identity-authentication failure handling).
    let source_ip = peer.rsplit_once(':').map(|(ip, _)| ip).unwrap_or(&peer);
    let result = async {
        loop {
            let read = conn.read_frame();
            let frame = match state.idle_timeout {
                Some(t) => match tokio::time::timeout(t, read).await {
                    Ok(f) => f?,
                    Err(_) => {
                        let _ = tx
                            .send(Frame::new(
                                proto::RESP_ERROR,
                                err_payload("idle session timed out; reconnect"),
                            ))
                            .await;
                        break;
                    }
                },
                None => read.await?,
            };
            let Some(mut frame) = frame else { break };
            if state.auth_lock_threshold > 0 {
                let locked = {
                    let failures = state.auth_failures.lock().await;
                    failures
                        .get(source_ip)
                        .map(|v| {
                            v.len() >= state.auth_lock_threshold as usize
                                && v.last().is_some_and(|last| last.elapsed() < AUTH_LOCKOUT)
                        })
                        .unwrap_or(false)
                };
                if locked && frame.frame_type == proto::REQ_AUTH {
                    querylog::sync_event(
                        &state.sync_log,
                        "auth",
                        source_ip,
                        None,
                        false,
                        Some("source locked out after repeated auth failures".into()),
                    );
                    let _ = tx
                        .send(Frame::new(
                            proto::RESP_ERROR,
                            err_payload("account locked; retry later"),
                        ))
                        .await;
                    continue;
                }
            }
            if let Some(k) = key {
                if frame.flags & crypto::FLAG_ENCRYPTED == 0 {
                    let _ = tx
                        .send(Frame::new(
                            proto::RESP_ERROR,
                            err_payload("transport encrypted; client must send encrypted frames"),
                        ))
                        .await;
                    break;
                }
                match crypto::open(&k, &frame.payload) {
                    Ok(pt) => frame.payload = pt,
                    Err(e) => {
                        let _ = tx
                            .send(Frame::new(proto::RESP_ERROR, err_payload(&e)))
                            .await;
                        break;
                    }
                }
            }
            // Node-to-node trust boundary: once a cluster token is
            // configured, FLAG_REPLICATION frames are accepted only from
            // connections that authenticated with it, and peer connections
            // carry only replication traffic (AUTH and PING excepted).
            // Without a cluster token the historical behavior — any
            // authenticated connection may send replication frames — is
            // preserved.
            if state.cluster_token.is_some() && frame.frame_type != proto::REQ_AUTH {
                let is_repl = frame.flags & FLAG_REPLICATION != 0;
                if is_repl && role != ConnRole::Peer {
                    let _ = tx
                        .send(Frame::new(
                            proto::RESP_ERROR,
                            err_payload("replication frames require cluster auth"),
                        ))
                        .await;
                    continue;
                }
                if !is_repl && role == ConnRole::Peer && frame.frame_type != proto::REQ_PING {
                    let _ = tx
                        .send(Frame::new(
                            proto::RESP_ERROR,
                            err_payload("cluster connections only carry replication frames"),
                        ))
                        .await;
                    continue;
                }
            }
            let authed = matches!(role, ConnRole::Client | ConnRole::Peer | ConnRole::ReadOnly);
            // Least-privilege gate: a read-only-token connection may read
            // and subscribe, but every durable write is refused here so
            // no handler can accidentally apply one.
            if role == ConnRole::ReadOnly {
                let is_repl = frame.flags & FLAG_REPLICATION != 0;
                let sub = if frame.frame_type == proto::REQ_PUBSUB {
                    serde_json::from_slice::<serde_json::Value>(&frame.payload)
                        .ok()
                        .and_then(|v| v["sub"].as_str().map(String::from))
                } else {
                    None
                };
                let writes = frame.frame_type == proto::REQ_PUBLISH
                    || frame.frame_type == proto::REQ_PROMOTE
                    || sub.as_deref() == Some("trim")
                    || (frame.frame_type == proto::REQ_SQL
                        && !is_repl
                        && docsql_core::engine::Database::is_write_statement(
                            &proto::decode_sql(&frame.payload).unwrap_or_default(),
                        ));
                if writes {
                    let _ = tx
                        .send(Frame::new(
                            proto::RESP_ERROR,
                            err_payload("read-only token; writes are not permitted"),
                        ))
                        .await;
                    continue;
                }
            }
            // None = the handler already sent everything (subscribe
            // confirmation + replay) straight through the writer.
            let resp = match frame.frame_type {
                proto::REQ_PING => Some(Frame::new(proto::RESP_PONG, vec![])),
                proto::REQ_AUTH => {
                    // Payload is the raw token. Priority: cluster token
                    // (peer node), client token (full client), read-only
                    // token (least-privilege client). With no tokens
                    // configured every AUTH succeeds (auth disabled).
                    // Failures are counted per source IP; past the
                    // threshold the source is locked out for a window.
                    let token = String::from_utf8_lossy(&frame.payload);
                    let cluster_match = state.cluster_token.as_ref().is_some_and(|ct| {
                        crypto::constant_time_eq(token.as_bytes(), ct.as_bytes())
                    });
                    // Every authentication outcome is audited (source,
                    // granted role or failure reason): security baselines
                    // require login success AND failure trails.
                    let audit = |ok: bool, detail: &str, state: &Arc<ServerState>| {
                        querylog::sync_event(
                            &state.sync_log,
                            "auth",
                            source_ip,
                            None,
                            ok,
                            Some(detail.to_string()),
                        );
                    };
                    if cluster_match {
                        role = ConnRole::Peer;
                        state.auth_failures.lock().await.remove(source_ip);
                        audit(true, "cluster token accepted", &state);
                        Some(Frame::new(proto::RESP_AFFECTED, b"ok".to_vec()))
                    } else {
                        let client_match = state.auth_token.as_ref().is_some_and(|t| {
                            crypto::constant_time_eq(token.as_bytes(), t.as_bytes())
                        });
                        let read_match = state.read_token.as_ref().is_some_and(|t| {
                            crypto::constant_time_eq(token.as_bytes(), t.as_bytes())
                        });
                        let auth_disabled =
                            state.auth_token.is_none() && state.read_token.is_none();
                        if client_match || auth_disabled {
                            role = ConnRole::Client;
                            state.auth_failures.lock().await.remove(source_ip);
                            if state.auth_token.is_some() {
                                audit(true, "client token accepted", &state);
                            }
                            Some(Frame::new(proto::RESP_AFFECTED, b"ok".to_vec()))
                        } else if read_match {
                            role = ConnRole::ReadOnly;
                            state.auth_failures.lock().await.remove(source_ip);
                            audit(true, "read token accepted", &state);
                            Some(Frame::new(proto::RESP_AFFECTED, b"ok(read-only)".to_vec()))
                        } else {
                            if state.auth_lock_threshold > 0 {
                                let mut failures = state.auth_failures.lock().await;
                                let now = std::time::Instant::now();
                                let list = failures.entry(source_ip.to_string()).or_default();
                                list.retain(|t| now.duration_since(*t) < AUTH_LOCK_WINDOW);
                                list.push(now);
                                if list.len() as u32 == state.auth_lock_threshold {
                                    audit(false, "lockout: repeated auth failures", &state);
                                }
                            }
                            Some(Frame::new(proto::RESP_ERROR, err_payload("bad token")))
                        }
                    }
                }
                proto::REQ_PROMOTE if authed => {
                    // Failover: leave replica mode. Mirrors the former KV
                    // PROMOTE command — clears read-only and detaches the
                    // replication upstream so this node owns its writes.
                    state
                        .read_only
                        .store(false, std::sync::atomic::Ordering::SeqCst);
                    *state.replicate_to.lock().await = None;
                    querylog::sync_event(&state.sync_log, "promote", "", None, true, None);
                    Some(Frame::new(proto::RESP_AFFECTED, b"promoted".to_vec()))
                }
                proto::REQ_STATUS if authed => {
                    // Read-only node report for cluster monitoring; not an SQL
                    // statement, so it bypasses the query log.
                    let payload = serde_json::to_vec(&status_payload(&state).await)
                        .unwrap_or_else(|_| b"{}".to_vec());
                    Some(Frame::new(proto::RESP_STATUS, payload))
                }
                proto::REQ_LOGS if authed => {
                    // Recent statement-audit entries + sync events for the
                    // console's logs page; like REQ_STATUS this is not an SQL
                    // statement, so it bypasses the query log.
                    let limit = querylog::parse_logs_limit(&frame.payload);
                    Some(Frame::new(
                        proto::RESP_LOGS,
                        querylog::logs_payload(&state.query_log, &state.sync_log, limit),
                    ))
                }
                proto::REQ_META if authed => {
                    // Object-explorer metadata for the console's node
                    // switching: the same core::meta walk the console runs
                    // on its embedded engine, so remote nodes report
                    // shape-identical /api/meta payloads. Not an SQL
                    // statement, so it bypasses the query log.
                    let mut db = state.db.lock().unwrap_or_else(|p| p.into_inner());
                    let meta = docsql_core::meta::build_meta(
                        &mut db,
                        &state.db_path,
                        state.started,
                        env!("CARGO_PKG_VERSION"),
                    );
                    Some(Frame::new(
                        proto::RESP_META,
                        docsql_core::json::to_string(&meta).into_bytes(),
                    ))
                }
                proto::REQ_SQL if authed => {
                    let started = std::time::Instant::now();
                    // Full statement text: the query log truncates its own
                    // copy (querylog::record), and cutting here would
                    // silently garble every statement longer than 512 chars
                    // (e.g. document INSERTs proxied from the web console's
                    // node switching). The frame itself is length-capped on
                    // read.
                    let sql = proto::decode_sql(&frame.payload).unwrap_or_default();
                    let mut logged = false;
                    let resp = match querylog::try_serve_log_view(&sql, &state) {
                        // 读日志的查询本身不写日志(避免读日志刷日志)。
                        Some(f) => f,
                        None => {
                            // The docsql_pubsub view maps onto the system
                            // table, which the guard below would otherwise
                            // reject; rewritten statements execute with the
                            // system-table gate open. Replication frames are
                            // never rewritten.
                            let is_replication = frame.flags & FLAG_REPLICATION != 0;
                            // Join intake: while bootstrapping, replication
                            // writes queue and are acknowledged on the spot
                            // (see ServerState::sync_queue); they replay
                            // after the snapshot lands.
                            let queued = if is_replication
                                && docsql_core::engine::Database::is_write_statement(&sql)
                            {
                                let mut gate = state.sync_queue.lock().await;
                                if !gate.closed {
                                    gate.pending.push(sql.clone());
                                    Some(Frame::new(
                                        proto::RESP_AFFECTED,
                                        1u64.to_le_bytes().to_vec(),
                                    ))
                                } else {
                                    None
                                }
                            } else {
                                None
                            };
                            match queued {
                                Some(f) => {
                                    // Audited later by the drain replay.
                                    f
                                }
                                None => {
                                    let (effective, allow_system) = if is_replication {
                                        (sql.clone(), false)
                                    } else {
                                        match pubsub::try_rewrite_pubsub_view(&sql) {
                                            Some(rewritten) => (rewritten, true),
                                            None => (sql.clone(), false),
                                        }
                                    };
                                    logged = true;
                                    execute_sql(
                                        &state,
                                        &effective,
                                        allow_system,
                                        is_replication,
                                        if is_replication { None } else { Some(conn_id) },
                                        false,
                                    )
                                    .await
                                }
                            }
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
                    Some(resp)
                }
                proto::REQ_SQL_SEQ if authed && frame.flags & FLAG_REPLICATION != 0 => {
                    // Sequenced replication write: apply it like any
                    // replicated write, then record (origin node_id, seq)
                    // so a rejoin can pull exactly the ops it missed. The
                    // position update is a separate commit — a crash in
                    // between leaves the position behind the data, which
                    // only ever causes a redundant replay (an error that
                    // falls back to snapshot repair), never a missed one.
                    match parse_seq_frame(&frame.payload) {
                        Some((seq, node_id, sql)) => {
                            let started = std::time::Instant::now();
                            let resp = execute_sql(&state, &sql, false, true, None, false).await;
                            if resp.frame_type != proto::RESP_ERROR {
                                let mut db = state.db.lock().unwrap_or_else(|p| p.into_inner());
                                if let Err(e) = db.position_set(&node_id, seq) {
                                    eprintln!("catchup position update failed: {e}");
                                }
                            }
                            // Same audit trail as plain REQ_SQL applies.
                            querylog::record(
                                &state,
                                &peer,
                                &sql,
                                started.elapsed().as_secs_f64() * 1000.0,
                                &resp,
                                true,
                            );
                            Some(resp)
                        }
                        None => Some(Frame::new(
                            proto::RESP_ERROR,
                            err_payload("malformed REQ_SQL_SEQ payload"),
                        )),
                    }
                }
                proto::REQ_CATCHUP if authed && frame.flags & FLAG_REPLICATION != 0 => {
                    handle_catchup(&state, &frame, &tx).await;
                    None
                }
                proto::REQ_PUBLISH if authed => Some(handle_publish(&state, &frame).await),
                proto::REQ_SUBSCRIBE if authed => {
                    handle_subscribe(&state, conn_id, &frame, &tx, pubsub::SubKind::Channel).await;
                    None
                }
                proto::REQ_PSUBSCRIBE if authed => {
                    handle_subscribe(&state, conn_id, &frame, &tx, pubsub::SubKind::Pattern).await;
                    None
                }
                proto::REQ_UNSUBSCRIBE if authed => Some(
                    handle_unsubscribe(&state, conn_id, &frame, pubsub::SubKind::Channel).await,
                ),
                proto::REQ_PUNSUBSCRIBE if authed => Some(
                    handle_unsubscribe(&state, conn_id, &frame, pubsub::SubKind::Pattern).await,
                ),
                proto::REQ_PUBSUB if authed => Some(handle_pubsub_cmd(&state, &frame).await),
                proto::REQ_BACKUP if authed => {
                    Some(backup::handle_backup(&state, role, &frame).await)
                }
                // Cluster-join frames are node-internal: they always ride
                // FLAG_REPLICATION (peer connections under cluster-token
                // auth), so a plain client cannot freeze or dump a node.
                proto::REQ_SYNC if authed && frame.flags & FLAG_REPLICATION != 0 => {
                    handle_sync(&state, &frame, &tx).await;
                    None
                }
                proto::REQ_HOLD if authed && frame.flags & FLAG_REPLICATION != 0 => {
                    Some(handle_hold(&state, &frame).await)
                }
                proto::REQ_RELEASE if authed && frame.flags & FLAG_REPLICATION != 0 => {
                    Some(handle_release(&state, &frame).await)
                }
                proto::REQ_DIGEST if authed && frame.flags & FLAG_REPLICATION != 0 => {
                    Some(handle_digest(&state).await)
                }
                _ => Some(Frame::new(
                    proto::RESP_ERROR,
                    err_payload(if authed {
                        "unsupported frame"
                    } else {
                        "unauthorized"
                    }),
                )),
            };
            if let Some(resp) = resp {
                if tx.send(resp).await.is_err() {
                    break;
                }
            }
        }
        Ok::<(), std::io::Error>(())
    }
    .await;
    // Connection gone: drop its subscriptions so publishes stop fanning
    // out to a dead writer channel.
    state.pubsub.remove_conn(conn_id).await;
    // If it owned the open transaction, roll that transaction back: nobody
    // can commit it anymore, and leaving it open would queue every later
    // write behind a transaction that can never finish. write_order first —
    // the same order execute_sql takes.
    {
        let _order = state.write_order.lock().await;
        if *state.tx_owner.lock().unwrap() == Some(conn_id) {
            *state.tx_owner.lock().unwrap() = None;
            {
                let mut db = state.db.lock().unwrap_or_else(|p| p.into_inner());
                if db.in_transaction() {
                    let _ = db.execute("ROLLBACK");
                }
            }
            state.tx_pending.lock().await.clear();
        }
    }
    drop(tx);
    let _ = writer.await;
    result
}

/// How long a client BEGIN queues behind an engine transaction another
/// connection holds open before erroring. The engine supports one global
/// transaction; without the queue, concurrent EF contexts (one connection
/// each, SaveChanges wraps in BEGIN..COMMIT) would fail on collision.
pub(crate) const BEGIN_QUEUE_WAIT: std::time::Duration = std::time::Duration::from_secs(30);
pub(crate) const BEGIN_QUEUE_POLL: std::time::Duration = std::time::Duration::from_millis(5);

/// Identity-authentication failure handling: after this many failed AUTHs
/// from one source inside the window, further attempts from that source are
/// rejected unread until the lockout elapses. Lenient enough for fat-fingered
/// clients, tight enough to make online guessing impractical.
pub const AUTH_LOCK_THRESHOLD: u32 = 10;
pub(crate) const AUTH_LOCK_WINDOW: std::time::Duration = std::time::Duration::from_secs(60);
pub const AUTH_LOCKOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Minimum accepted credential length, enforced by the server binary for
/// every configured token (password complexity floor; the e2e suites build
/// `ServerConfig` directly and use short test tokens).
pub const MIN_TOKEN_LEN: usize = 8;

/// Credential complexity floor for configured tokens: reject obviously
/// weak secrets (too short, or a single repeated character) at startup so
/// a deployment cannot ship with a guessable credential.
pub fn check_token_strength(name: &str, token: &str) -> Result<(), String> {
    if token.chars().count() < MIN_TOKEN_LEN {
        return Err(format!(
            "{name} is too weak: use at least {MIN_TOKEN_LEN} characters"
        ));
    }
    let mut chars = token.chars();
    let first = chars.next().unwrap_or_default();
    if token.chars().all(|c| c == first) {
        return Err(format!("{name} is too weak: single repeated character"));
    }
    Ok(())
}

/// Assemble the REQ_STATUS payload: everything a monitoring console needs to
/// judge node health and cluster convergence in one round trip.
pub async fn status_payload(state: &ServerState) -> serde_json::Value {
    let peers = state.peers.lock().await.clone();
    let replicate_to = state.replicate_to.lock().await.clone();
    // Backup state first: it must be read without the engine lock held
    // (backup.rs never nests the two, keep it that way).
    let backup = serde_json::from_slice::<serde_json::Value>(&backup::backup_payload(state))
        .unwrap_or(serde_json::json!({}));
    let mut db = state.db.lock().unwrap_or_else(|p| p.into_inner());
    let mut total_rows = 0u64;
    let mut user_tables = 0usize;
    for t in &db.catalog() {
        // The pubsub backing table and the catch-up journal/positions are
        // system storage, not user objects.
        if docsql_core::engine::is_system_table(&t.name) {
            continue;
        }
        user_tables += 1;
        // Same COUNT(*) census the web console's /api/meta performs.
        total_rows += match db.execute(&format!(
            "SELECT COUNT(*) FROM \"{}\"",
            t.name.replace('"', "\"\"")
        )) {
            Ok(ExecOutcome::Rows(r)) => {
                r.rows.first().and_then(|row| row[0].as_i64()).unwrap_or(0) as u64
            }
            _ => 0,
        };
    }
    serde_json::json!({
        "name": "docsql",
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_ms": state.started.elapsed().as_millis() as u64,
        "read_only": state.read_only.load(std::sync::atomic::Ordering::SeqCst),
        "in_transaction": db.in_transaction(),
        "peers": peers,
        "replicate_to": replicate_to,
        "storage": {
            "page_size": db.page_size(),
            "num_pages": db.num_pages(),
            "db_bytes": file_bytes(&state.db_path),
            "wal_bytes": file_bytes(&wal_path(&state.db_path)),
        },
        "durable_lsn": db.durable_lsn(),
        "totals": {"tables": user_tables, "rows": total_rows},
        "cluster_id": state.cluster_id,
        "journal_head": db.journal_head().unwrap_or(0),
        "journal_oldest": db.journal_oldest().unwrap_or(0),
        "backup": backup,
    })
}

fn file_bytes(p: &std::path::Path) -> u64 {
    std::fs::metadata(p).map(|m| m.len()).unwrap_or(0)
}

fn wal_path(db: &std::path::Path) -> PathBuf {
    let mut s = db.as_os_str().to_os_string();
    s.push(".wal");
    PathBuf::from(s)
}

/// Sleep-poll until the shared engine transaction closes or `deadline`
/// passes. Nothing is held across awaits, so the owning connection's
/// COMMIT/ROLLBACK always makes progress.
pub(crate) async fn wait_engine_tx_free(state: &Arc<ServerState>, deadline: tokio::time::Instant) {
    while tokio::time::Instant::now() < deadline {
        let busy = state
            .db
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .in_transaction();
        if !busy {
            return;
        }
        tokio::time::sleep(BEGIN_QUEUE_POLL).await;
    }
}

/// Execute one SQL statement. `allow_system_table` marks the rewritten
/// `docsql_pubsub` view; system tables are otherwise blocked for anything
/// that could mutate them, while read-only queries may reference them (the
/// console's object tree shows them as a read-only 系统表 branch). `conn`
/// identifies the client connection (None for replication applies);
/// `order_held` marks the cluster-join drain path, which already holds
/// `write_order`.
async fn execute_sql(
    state: &Arc<ServerState>,
    sql: &str,
    allow_system_table: bool,
    is_replication: bool,
    conn: Option<u64>,
    order_held: bool,
) -> Frame {
    // One parse for the whole round-trip: the AST executes at the bottom,
    // the classification routes the request here (parse errors surface with
    // the same message `execute` would have produced).
    let mut parsed = Some(match Database::parse_classified(sql) {
        Ok(p) => p,
        Err(e) => return Frame::new(proto::RESP_ERROR, err_payload(&e.to_string())),
    });
    let p = parsed.as_ref().expect("parsed just above");
    let is_write = p.is_write;
    let tx_kind = p.tx.clone();
    // System tables are shielded from anything that could mutate them; the
    // check runs on the classified AST (exactly one statement per frame),
    // so a SELECT prefix can't smuggle a write past it, and read-only
    // queries go through to the console's read-only 系统表 branch.
    if !allow_system_table && is_write {
        let lower = sql.to_ascii_lowercase();
        if lower.contains(pubsub::PUBSUB_TABLE) {
            return Frame::new(
                proto::RESP_ERROR,
                err_payload(
                    "system table _pubsub_messages is internal: \
                     query the docsql_pubsub view, trim with PUBSUB TRIM",
                ),
            );
        }
        for internal in [
            docsql_core::engine::CLUSTER_LOG_TABLE,
            docsql_core::engine::CLUSTER_POS_TABLE,
            docsql_core::engine::CLUSTER_ID_TABLE,
        ] {
            if lower.contains(internal) {
                return Frame::new(
                    proto::RESP_ERROR,
                    err_payload("system table is internal to catch-up replication"),
                );
            }
        }
    }
    // Replica read-only gate (replication-internal frames pass through).
    let read_only = state.read_only.load(std::sync::atomic::Ordering::SeqCst);
    if read_only && !is_replication && is_write {
        return Frame::new(
            proto::RESP_ERROR,
            err_payload("read-only replica; PROMOTE to accept writes"),
        );
    }
    // The single global transaction belongs to the connection that opened
    // it: COMMIT/ROLLBACK/SAVEPOINT from anyone else would roll back or
    // flush another connection's work.
    if !is_replication && conn.is_some() {
        let foreign_control = !matches!(tx_kind, TxControl::None | TxControl::Begin)
            && state
                .tx_owner
                .lock()
                .unwrap()
                .is_some_and(|o| Some(o) != conn);
        if foreign_control {
            return Frame::new(
                proto::RESP_ERROR,
                err_payload("transaction is held open by another connection"),
            );
        }
    }
    // A client BEGIN queues behind an open engine transaction instead of
    // erroring immediately (single-writer engine). So does every write the
    // owner does not own — merging into the open transaction would let its
    // ROLLBACK silently drop a write that was already acknowledged to the
    // client. The deadline bounds both queues.
    let queues = !is_replication && matches!(tx_kind, TxControl::Begin);
    let deadline = tokio::time::Instant::now() + BEGIN_QUEUE_WAIT;
    let (outcome, in_tx, _order_guard, resolved) = loop {
        let foreign_tx = is_write && *state.tx_owner.lock().unwrap() != conn;
        if queues || foreign_tx {
            wait_engine_tx_free(state, deadline).await;
        }
        let guard = if !is_replication && (is_write || !matches!(tx_kind, TxControl::None)) {
            let order = state.write_order.lock().await;
            let busy = {
                let in_tx = state
                    .db
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .in_transaction();
                in_tx && *state.tx_owner.lock().unwrap() != conn
            };
            if busy {
                drop(order);
                if tokio::time::Instant::now() >= deadline {
                    return Frame::new(
                        proto::RESP_ERROR,
                        err_payload("timed out waiting for the transaction on another connection"),
                    );
                }
                tokio::time::sleep(BEGIN_QUEUE_POLL).await;
                continue;
            }
            Some(order)
        } else if is_replication && is_write && !order_held {
            // Replicated writes must land in autocommit: executing inside a
            // client's open transaction would let its ROLLBACK undo writes
            // the origin node already acknowledged (silent divergence).
            // Holding write_order keeps a BEGIN from slipping in between the
            // check and the execute; the cluster-join drain replays with
            // write_order already held (re-locking would self-deadlock).
            let order = state.write_order.lock().await;
            let busy = state
                .db
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .in_transaction();
            if busy {
                drop(order);
                if tokio::time::Instant::now() >= deadline {
                    return Frame::new(
                        proto::RESP_ERROR,
                        err_payload("replication write timed out waiting for the open transaction"),
                    );
                }
                tokio::time::sleep(BEGIN_QUEUE_POLL).await;
                continue;
            }
            Some(order)
        } else {
            None
        };
        let (out, in_tx, resolved) = {
            let mut db = state.db.lock().unwrap_or_else(|p| p.into_inner());
            let out = match parsed.take() {
                Some(p) => db.execute_parsed(p),
                // Consumed by a lost BEGIN-queue race above: re-parse (the
                // failed attempt left no state behind) and retry.
                None => db.execute(sql),
            };
            // Auto-GUID inserts rewrite themselves into explicit-value SQL;
            // grab it under the same lock so replication replays the exact
            // generated ids.
            let resolved = db.take_resolved_insert();
            // Capture inside the same lock: another connection must not be able
            // to open/close a transaction between execute and classification.
            let in_tx = db.in_transaction();
            (out, in_tx, resolved)
        };
        // Lost the race for the engine transaction between the wait and the
        // locks: requeue until the deadline, then let the error through.
        if queues
            && out
                .as_ref()
                .is_err_and(|e| e.to_string().contains("transaction already in progress"))
            && tokio::time::Instant::now() < deadline
        {
            drop(guard);
            tokio::time::sleep(BEGIN_QUEUE_POLL).await;
            continue;
        }
        break (out, in_tx, guard, resolved);
    };
    // Replication timing follows engine transaction state: writes inside an
    // open transaction buffer until COMMIT (ROLLBACK discards them), so peers
    // never observe writes this node later undoes. COMMIT/EXEC drain the
    // buffer in execution order, mixing SQL and KV writes alike.
    if !is_replication && outcome.is_ok() {
        match tx_kind {
            TxControl::Begin => {
                *state.tx_owner.lock().unwrap() = conn;
            }
            TxControl::Commit => {
                *state.tx_owner.lock().unwrap() = None;
                drain_tx_pending(state).await
            }
            TxControl::Rollback { savepoint: None } => {
                *state.tx_owner.lock().unwrap() = None;
                state.tx_pending.lock().await.clear()
            }
            TxControl::Rollback {
                savepoint: Some(name),
            } => state.tx_pending.lock().await.rollback_to(&name),
            TxControl::Savepoint(name) => state.tx_pending.lock().await.mark(&name),
            TxControl::Release(name) => state.tx_pending.lock().await.release(&name),
            _ if is_write => {
                // Auto-generated GUID values are random: peers cannot
                // re-derive them the way AUTOINCREMENT recomputes max+1,
                // so forward the engine's explicit-value rewrite when the
                // statement filled any.
                let forward = resolved.as_deref().unwrap_or(sql);
                if in_tx {
                    state
                        .tx_pending
                        .lock()
                        .await
                        .writes
                        .push(forward.to_string());
                } else {
                    // Journal the committed write so rejoined peers can
                    // pull exactly it later, then fan out carrying the
                    // journal seq (their positions advance with it).
                    let seq = journal_local_write(state, forward).await;
                    forward_sql_all(state, forward, seq).await;
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
        Err(e) => Frame::new(proto::RESP_ERROR, err_payload(&e.to_string())),
    }
}

/// Peer I/O budget: a partitioned or malicious peer must not wedge client
/// writes for the OS TCP timeout, nor force huge allocations.
pub(crate) const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);
pub(crate) const IO_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
pub(crate) const RECV_CAP: usize = 64 * 1024 * 1024;

/// Read one response frame from an outbound connection.
async fn read_response_frame(stream: &mut TcpStream) -> std::io::Result<Frame> {
    let mut header = [0u8; proto::HEADER_LEN];
    tokio::time::timeout(IO_TIMEOUT, stream.read_exact(&mut header)).await??;
    let len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
    if len > RECV_CAP {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "peer response too large",
        ));
    }
    let mut buf = header.to_vec();
    let mut payload = vec![0u8; len];
    tokio::time::timeout(IO_TIMEOUT, stream.read_exact(&mut payload)).await??;
    buf.extend_from_slice(&payload);
    let (f, _) = Frame::decode(&buf).map_err(std::io::Error::other)?;
    Ok(f)
}

/// AUTH on a fresh peer connection when the server requires a token (the
/// peer rejects everything else with "unauthorized").
async fn auth_on(
    stream: &mut TcpStream,
    token: &str,
    key: Option<&crypto::TransportKey>,
) -> std::io::Result<()> {
    let mut frame = Frame::new(proto::REQ_AUTH, token.as_bytes().to_vec());
    if let Some(k) = key {
        frame.payload = crypto::seal(k, &frame.payload);
        frame.flags |= crypto::FLAG_ENCRYPTED;
    }
    let bytes = frame.encode().map_err(std::io::Error::other)?;
    use tokio::io::AsyncWriteExt;
    tokio::time::timeout(IO_TIMEOUT, stream.write_all(&bytes)).await??;
    tokio::time::timeout(IO_TIMEOUT, stream.flush()).await??;
    let resp = read_response_frame(stream).await?;
    if resp.frame_type == proto::RESP_ERROR {
        return Err(std::io::Error::other(format!(
            "peer rejected AUTH: {}",
            String::from_utf8_lossy(&resp.payload)
        )));
    }
    Ok(())
}

/// Connect and authenticate one outbound peer connection.
async fn open_peer_conn(
    target: &str,
    key: Option<&crypto::TransportKey>,
    auth: Option<&str>,
) -> std::io::Result<TcpStream> {
    let mut stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(target)).await??;
    if let Some(token) = auth {
        auth_on(&mut stream, token, key).await?;
    }
    Ok(stream)
}

/// Write one replication-internal frame and read its response; the stream
/// comes back so the caller can pool it. Encryption is per frame.
async fn send_frame_on(
    mut stream: TcpStream,
    frame_type: u16,
    payload: &[u8],
    key: Option<&crypto::TransportKey>,
) -> std::io::Result<(Frame, TcpStream)> {
    let mut frame = Frame::new(frame_type, payload.to_vec());
    frame.flags = FLAG_REPLICATION;
    if let Some(k) = key {
        frame.payload = crypto::seal(k, &frame.payload);
        frame.flags |= crypto::FLAG_ENCRYPTED;
    }
    let bytes = frame.encode().map_err(std::io::Error::other)?;
    use tokio::io::AsyncWriteExt;
    tokio::time::timeout(IO_TIMEOUT, stream.write_all(&bytes)).await??;
    tokio::time::timeout(IO_TIMEOUT, stream.flush()).await??;
    let resp = read_response_frame(&mut stream).await?;
    if resp.frame_type == proto::RESP_ERROR {
        return Err(std::io::Error::other(format!(
            "replica rejected: {}",
            String::from_utf8_lossy(&resp.payload)
        )));
    }
    Ok((resp, stream))
}

/// Send one write to the replication upstream.
async fn forward_write(
    target: &str,
    sql: &str,
    seq: Option<u64>,
    node_id: &str,
    key: Option<&crypto::TransportKey>,
    auth: Option<&str>,
) -> std::io::Result<()> {
    match seq {
        None => {
            let payload = proto::encode_sql(sql).map_err(std::io::Error::other)?;
            forward_frame(target, proto::REQ_SQL, &payload, key, auth)
                .await
                .map(|_| ())
        }
        Some(seq) => {
            // Sequenced fan-out: the receiver records the position so a
            // later rejoin can pull exactly the ops it missed. Peers
            // predating REQ_SQL_SEQ reject the frame; fall back to the
            // legacy plain-SQL write (catch-up then never trusts that
            // peer's position, which is the safe direction).
            let mut payload = Vec::with_capacity(sql.len() + 64);
            payload.extend_from_slice(&seq.to_le_bytes());
            payload.extend_from_slice(&(node_id.len() as u32).to_le_bytes());
            payload.extend_from_slice(node_id.as_bytes());
            payload.extend_from_slice(&proto::encode_sql(sql).map_err(std::io::Error::other)?);
            let sequenced = forward_frame(target, proto::REQ_SQL_SEQ, &payload, key, auth).await;
            match sequenced {
                Ok(resp) if resp.frame_type == proto::RESP_ERROR => {
                    let payload = proto::encode_sql(sql).map_err(std::io::Error::other)?;
                    forward_frame(target, proto::REQ_SQL, &payload, key, auth)
                        .await
                        .map(|_| ())
                }
                Ok(_) => Ok(()),
                Err(e) => Err(e),
            }
        }
    }
}

/// One-shot (connect, auth, send, read response) replication frame.
///
/// Deliberately NOT a pooled connection: "peer offline" is detected by
/// connect-refusal, and that is load-bearing semantics — a peer whose server
/// task is gone (aborted listener, crashed process) must not keep receiving
/// writes over sockets that outlived it, or outage writes would silently
/// reach a node that will never acknowledge them in the sync log (and an
/// e2e/deploy test exactly pins the no-catchup behavior this preserves).
async fn forward_frame(
    target: &str,
    frame_type: u16,
    payload: &[u8],
    key: Option<&crypto::TransportKey>,
    auth: Option<&str>,
) -> std::io::Result<Frame> {
    let stream = open_peer_conn(target, key, auth).await?;
    let (resp, _stream) = send_frame_on(stream, frame_type, payload, key).await?;
    Ok(resp)
}

/// Credential fan-out presents to peers: the cluster token when
/// configured (nodes authenticate as nodes), else the client token.
fn fanout_auth(state: &ServerState) -> Option<&str> {
    state
        .cluster_token
        .as_deref()
        .or(state.auth_token.as_deref())
}

/// Fan one SQL write out to the replication upstream and every peer, in
/// parallel. Each attempt lands in the sync log so the console's logs page
/// shows the replication trail (target, statement, ok/error). Per-target
/// ordering is untouched: fan-out runs under `write_order`, so the next
/// write's fan-out only starts after this one finished everywhere.
/// Journal one locally-committed write (autocommit path: the write has
/// already committed when this runs; the drained-transaction path calls
/// it per buffered write after COMMIT). Returns the journal seq, or None
/// when journaling failed — peers then cannot place the op in the origin's
/// journal and fall back to snapshot repair on divergence.
async fn journal_local_write(state: &Arc<ServerState>, sql: &str) -> Option<u64> {
    let mut db = state.db.lock().unwrap_or_else(|p| p.into_inner());
    match db.journal_append(sql) {
        Ok(seq) => {
            // Amortized window trim: keep the journal bounded so positions
            // older than the window force snapshot fallback instead of
            // unbounded growth.
            if state.catchup_window > 0 && seq % 512 == 0 {
                if let Err(e) = db.journal_trim(state.catchup_window) {
                    eprintln!("catchup journal trim failed: {e}");
                }
            }
            Some(seq)
        }
        Err(e) => {
            eprintln!("catchup journal append failed: {e}");
            None
        }
    }
}

pub async fn forward_sql_all(state: &Arc<ServerState>, sql: &str, seq: Option<u64>) {
    let auth = fanout_auth(state).map(String::from);
    let key = state.transport_key;
    let mut targets: Vec<String> = Vec::new();
    if let Some(target) = state.replicate_to.lock().await.clone() {
        targets.push(target);
    }
    targets.extend(state.peers.lock().await.clone());
    let mut tasks = tokio::task::JoinSet::new();
    for target in targets {
        let sql = sql.to_string();
        let auth = auth.clone();
        let node_id = state.cluster_id.clone();
        tasks.spawn(async move {
            let res =
                forward_write(&target, &sql, seq, &node_id, key.as_ref(), auth.as_deref()).await;
            (target, sql, res)
        });
    }
    while let Some(joined) = tasks.join_next().await {
        let (target, sql, res) = joined.expect("fan-out task cannot panic");
        match &res {
            Ok(()) => {
                querylog::sync_event(&state.sync_log, "forward", &target, Some(&sql), true, None)
            }
            Err(e) => {
                eprintln!("replication to {target} failed: {e}");
                querylog::sync_event(
                    &state.sync_log,
                    "forward",
                    &target,
                    Some(&sql),
                    false,
                    Some(e.to_string()),
                );
            }
        }
    }
}

/// Forward everything buffered in the open transaction (SQL writes, in
/// execution order) and clear the buffer. Called after a successful COMMIT.
pub async fn drain_tx_pending(state: &Arc<ServerState>) {
    let mut pending = state.tx_pending.lock().await;
    let writes = std::mem::take(&mut pending.writes);
    pending.marks.clear();
    drop(pending);
    for sql in &writes {
        let seq = journal_local_write(state, sql).await;
        forward_sql_all(state, sql, seq).await;
    }
}

// ---------------------------------------------------------------------------
// Pub/sub frames.
// ---------------------------------------------------------------------------

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Acquire the write path (write_order) once no explicit transaction is
/// open. Protocol-level writes (PUBLISH, TRIM) must not join a client
/// transaction — the client's ROLLBACK would undo them — so they queue
/// behind it like a BEGIN, bounded by the same deadline. With write_order
/// held, no new transaction can open (BEGIN takes write_order too), so
/// callers may lock the engine directly afterwards.
async fn lock_engine_for_write(
    state: &Arc<ServerState>,
) -> Option<tokio::sync::MutexGuard<'_, ()>> {
    let deadline = tokio::time::Instant::now() + BEGIN_QUEUE_WAIT;
    loop {
        wait_engine_tx_free(state, deadline).await;
        let order = state.write_order.lock().await;
        let in_tx = state
            .db
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .in_transaction();
        if !in_tx {
            return Some(order);
        }
        drop(order);
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(BEGIN_QUEUE_POLL).await;
    }
}

/// REQ_PUBLISH: persist the message first (autocommit engine write, WAL
/// fsync unless DOCSQL_ASYNC_COMMIT), then push to local subscribers,
/// then replicate to the peers so their stores and subscribers get it.
/// Responds RESP_ROWS with columns [id, receivers].
async fn handle_publish(state: &Arc<ServerState>, frame: &Frame) -> Frame {
    let is_replication = frame.flags & FLAG_REPLICATION != 0;
    let v: serde_json::Value = match serde_json::from_slice(&frame.payload) {
        Ok(v) => v,
        Err(e) => {
            return Frame::new(
                proto::RESP_ERROR,
                err_payload(&format!("publish: bad payload: {e}")),
            )
        }
    };
    let (Some(channel), Some(payload)) = (v["channel"].as_str(), v["payload"].as_str()) else {
        return Frame::new(
            proto::RESP_ERROR,
            err_payload("publish: expected {\"channel\", \"payload\"}"),
        );
    };
    if channel.is_empty() || channel.len() > pubsub::MAX_CHANNEL_LEN {
        return Frame::new(
            proto::RESP_ERROR,
            err_payload(&format!(
                "publish: channel must be 1..={} bytes",
                pubsub::MAX_CHANNEL_LEN
            )),
        );
    }
    if payload.len() > pubsub::MAX_PAYLOAD_LEN {
        return Frame::new(
            proto::RESP_ERROR,
            err_payload(&format!(
                "publish: payload exceeds {} bytes",
                pubsub::MAX_PAYLOAD_LEN
            )),
        );
    }
    if state.read_only.load(std::sync::atomic::Ordering::SeqCst) && !is_replication {
        return Frame::new(
            proto::RESP_ERROR,
            err_payload("read-only replica; PROMOTE to accept writes"),
        );
    }
    let Some(_order) = lock_engine_for_write(state).await else {
        return Frame::new(
            proto::RESP_ERROR,
            err_payload("publish timed out waiting for the open transaction"),
        );
    };
    let ts = now_ms() as i64;
    // Block scope so the engine guard drops before the awaits below (the
    // connection future must stay Send).
    let id = {
        let mut db = state.db.lock().unwrap_or_else(|p| p.into_inner());
        let id = match pubsub::store_insert(&mut db, channel, ts, payload) {
            Ok(id) => id,
            Err(e) => return Frame::new(proto::RESP_ERROR, err_payload(&e)),
        };
        // Async-commit mode defers the WAL fsync to the background flusher,
        // but the pub/sub ordering contract needs this row durable before
        // any push or ack leaves the node (an acked id must be replayable
        // even if the process dies right after). No-op in durable mode.
        if let Err(e) = db.sync_pending() {
            return Frame::new(
                proto::RESP_ERROR,
                err_payload(&format!("publish flush: {e}")),
            );
        }
        id
    };
    // Persisted before any push leaves this node (ordering constraint:
    // a client that resumes from the returned/received id must find the
    // message in the store even if this node crashes right after).
    let mut receivers = state.pubsub.notify(channel, id, ts, payload).await;
    if !is_replication {
        for f in forward_pubsub_all(state, proto::REQ_PUBLISH, &frame.payload).await {
            receivers += receivers_of(&f);
        }
    }
    let body = serde_json::json!({"columns": ["id", "receivers"], "rows": [[id, receivers]]});
    Frame::new(
        proto::RESP_ROWS,
        serde_json::to_vec(&body).unwrap_or_default(),
    )
}

/// REQ_SUBSCRIBE / REQ_PSUBSCRIBE. The confirmation and the replay frames
/// go straight through the writer while the registry lock is held, so
/// replay history and live pushes cannot overlap or gap (see pubsub docs).
async fn handle_subscribe(
    state: &Arc<ServerState>,
    conn: pubsub::ConnId,
    frame: &Frame,
    tx: &mpsc::Sender<Frame>,
    kind: pubsub::SubKind,
) {
    let err = |msg: String| Frame::new(proto::RESP_ERROR, err_payload(&msg));
    let v: serde_json::Value = match serde_json::from_slice(&frame.payload) {
        Ok(v) => v,
        Err(e) => {
            let _ = tx.send(err(format!("subscribe: bad payload: {e}"))).await;
            return;
        }
    };
    let key = if kind == pubsub::SubKind::Pattern {
        "pattern"
    } else {
        "channel"
    };
    let Some(name) = v[key].as_str().map(String::from) else {
        let _ = tx.send(err(format!("subscribe: missing {key}"))).await;
        return;
    };
    if name.is_empty() || name.len() > pubsub::MAX_CHANNEL_LEN {
        let _ = tx
            .send(err(format!(
                "subscribe: {key} must be 1..={} bytes",
                pubsub::MAX_CHANNEL_LEN
            )))
            .await;
        return;
    }
    let after_id = match pubsub::parse_after_id(&v) {
        Ok(a) => a,
        Err(e) => {
            let _ = tx.send(err(format!("subscribe: {e}"))).await;
            return;
        }
    };
    let mut inner = state.pubsub.lock().await;
    let count = inner.register(conn, kind, &name, tx.clone());
    // Snapshot and history fetch under the same registry hold: publishes
    // committing after this point notify this subscription live with
    // id > watermark; earlier ones are covered by the replay below.
    let (watermark, history) = {
        let mut db = state.db.lock().unwrap_or_else(|p| p.into_inner());
        let wm = pubsub::query_max_id(&mut db);
        let hist = pubsub::query_history(&mut db, after_id.unwrap_or(wm), wm).unwrap_or_default();
        (wm, hist)
    };
    // Confirmation and replay use try_send: a subscriber that stopped
    // draining its socket must not park this task while it holds the
    // registry lock (every PUBLISH waits on that lock under write_order).
    // Losing frames to a slow subscriber is the documented at-least-once
    // contract — delivery resumes by re-subscribing from the last id.
    let _ = tx.try_send(Frame::new(
        proto::RESP_AFFECTED,
        count.to_le_bytes().to_vec(),
    ));
    // skip-through tracks replay progress, not the raw watermark: only
    // messages the subscriber actually received are suppressed from the
    // live stream, so an interrupted replay leaves a resumable gap.
    let mut progress = after_id.unwrap_or(watermark);
    for m in &history {
        let hit = match kind {
            pubsub::SubKind::Channel => m.channel == name,
            pubsub::SubKind::Pattern => pubsub::pattern_matches(&name, &m.channel),
        };
        if !hit {
            continue;
        }
        let pattern = (kind == pubsub::SubKind::Pattern).then_some(name.as_str());
        let f = pubsub::push_frame(pattern, &m.channel, m.id, m.ts, &m.payload);
        if tx.try_send(f).is_err() {
            break; // channel full or connection gone: stop replaying
        }
        progress = m.id;
    }
    inner.arm_filter(conn, kind, &name, progress);
}

/// REQ_UNSUBSCRIBE / REQ_PUNSUBSCRIBE: JSON array of names, empty = all.
async fn handle_unsubscribe(
    state: &Arc<ServerState>,
    conn: pubsub::ConnId,
    frame: &Frame,
    kind: pubsub::SubKind,
) -> Frame {
    let names = match parse_name_array(&frame.payload) {
        Ok(n) => n,
        Err(msg) => return Frame::new(proto::RESP_ERROR, err_payload(&msg)),
    };
    let remaining = state.pubsub.lock().await.unregister(conn, kind, &names);
    Frame::new(proto::RESP_AFFECTED, remaining.to_le_bytes().to_vec())
}

fn parse_name_array(payload: &[u8]) -> Result<Vec<String>, String> {
    let v: serde_json::Value =
        serde_json::from_slice(payload).map_err(|e| format!("bad payload: {e}"))?;
    let arr = v.as_array().ok_or("expected a JSON array of names")?;
    let mut names = Vec::with_capacity(arr.len());
    for item in arr {
        let s = item.as_str().ok_or("expected a JSON array of names")?;
        if s.is_empty() || s.len() > pubsub::MAX_CHANNEL_LEN {
            return Err(format!(
                "name must be 1..={} bytes",
                pubsub::MAX_CHANNEL_LEN
            ));
        }
        names.push(s.to_string());
    }
    Ok(names)
}

/// REQ_PUBSUB: channels / numsub / numpat introspection (local registry)
/// and trim (replicated write).
async fn handle_pubsub_cmd(state: &Arc<ServerState>, frame: &Frame) -> Frame {
    let is_replication = frame.flags & FLAG_REPLICATION != 0;
    let v: serde_json::Value = match serde_json::from_slice(&frame.payload) {
        Ok(v) => v,
        Err(e) => {
            return Frame::new(
                proto::RESP_ERROR,
                err_payload(&format!("pubsub: bad payload: {e}")),
            )
        }
    };
    match v["sub"].as_str() {
        Some("channels") => {
            let filter = v["pattern"].as_str();
            let list = state.pubsub.lock().await.channels(filter);
            let rows: Vec<serde_json::Value> =
                list.iter().map(|c| serde_json::json!([c])).collect();
            let body = serde_json::json!({"columns": ["channel"], "rows": rows});
            Frame::new(
                proto::RESP_ROWS,
                serde_json::to_vec(&body).unwrap_or_default(),
            )
        }
        Some("numsub") => {
            let names = v["channels"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str().map(String::from))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let list = state.pubsub.lock().await.numsub(&names);
            let rows: Vec<serde_json::Value> = list
                .iter()
                .map(|(c, n)| serde_json::json!([c, n]))
                .collect();
            let body = serde_json::json!({"columns": ["channel", "subscribers"], "rows": rows});
            Frame::new(
                proto::RESP_ROWS,
                serde_json::to_vec(&body).unwrap_or_default(),
            )
        }
        Some("numpat") => {
            let n = state.pubsub.lock().await.numpat();
            let body = serde_json::json!({"columns": ["patterns"], "rows": [[n]]});
            Frame::new(
                proto::RESP_ROWS,
                serde_json::to_vec(&body).unwrap_or_default(),
            )
        }
        Some("trim") => {
            let (Some(channel), Some(keep)) = (v["channel"].as_str(), v["keep"].as_i64()) else {
                return Frame::new(
                    proto::RESP_ERROR,
                    err_payload("pubsub trim: expected {\"channel\", \"keep\"}"),
                );
            };
            if keep < 1 {
                return Frame::new(
                    proto::RESP_ERROR,
                    err_payload(
                        "pubsub trim: keep must be >= 1 \
                         (emptying the channel would reset id monotonicity)",
                    ),
                );
            }
            if state.read_only.load(std::sync::atomic::Ordering::SeqCst) && !is_replication {
                return Frame::new(
                    proto::RESP_ERROR,
                    err_payload("read-only replica; PROMOTE to accept writes"),
                );
            }
            let Some(_order) = lock_engine_for_write(state).await else {
                return Frame::new(
                    proto::RESP_ERROR,
                    err_payload("pubsub trim timed out waiting for the open transaction"),
                );
            };
            let deleted = {
                let mut db = state.db.lock().unwrap_or_else(|p| p.into_inner());
                match pubsub::store_trim(&mut db, channel, keep) {
                    Ok(n) => n,
                    Err(e) => return Frame::new(proto::RESP_ERROR, err_payload(&e)),
                }
            };
            let mut total = deleted;
            if !is_replication {
                for f in forward_pubsub_all(state, proto::REQ_PUBSUB, &frame.payload).await {
                    total += affected_count(&f);
                }
            }
            Frame::new(proto::RESP_AFFECTED, total.to_le_bytes().to_vec())
        }
        other => Frame::new(
            proto::RESP_ERROR,
            err_payload(&format!(
                "pubsub: unknown subcommand {other:?} (channels|numsub|numpat|trim)"
            )),
        ),
    }
}

/// Fan one pub/sub frame (publish or trim) out to the upstream and every
/// peer. Each target executes it locally (FLAG_REPLICATION stops further
/// forwarding); failures log and are skipped — the message stays durable
/// wherever it landed, which is the at-least-once contract. Attempts land
/// in the sync log like SQL fan-out.
async fn forward_pubsub_all(
    state: &Arc<ServerState>,
    frame_type: u16,
    payload: &[u8],
) -> Vec<Frame> {
    let event = if frame_type == proto::REQ_PUBLISH {
        "publish"
    } else {
        "trim"
    };
    let auth = fanout_auth(state).map(String::from);
    let key = state.transport_key;
    let mut targets: Vec<String> = Vec::new();
    if let Some(target) = state.replicate_to.lock().await.clone() {
        targets.push(target);
    }
    targets.extend(state.peers.lock().await.clone());
    let mut tasks = tokio::task::JoinSet::new();
    for target in targets {
        let payload = payload.to_vec();
        let auth = auth.clone();
        tasks.spawn(async move {
            let res =
                forward_frame(&target, frame_type, &payload, key.as_ref(), auth.as_deref()).await;
            (target, res)
        });
    }
    let mut out = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        let (target, res) = joined.expect("fan-out task cannot panic");
        match res {
            Ok(f) => {
                querylog::sync_event(&state.sync_log, event, &target, None, true, None);
                out.push(f);
            }
            Err(e) => {
                eprintln!("pubsub replication to {target} failed: {e}");
                querylog::sync_event(
                    &state.sync_log,
                    event,
                    &target,
                    None,
                    false,
                    Some(e.to_string()),
                );
            }
        }
    }
    out
}

/// Receiver count out of a peer's REQ_PUBLISH response (RESP_ROWS
/// [id, receivers]).
fn receivers_of(f: &Frame) -> u64 {
    if f.frame_type != proto::RESP_ROWS {
        return 0;
    }
    serde_json::from_slice::<serde_json::Value>(&f.payload)
        .ok()
        .and_then(|v| v["rows"][0][1].as_u64())
        .unwrap_or(0)
}

/// Affect count out of a peer's RESP_AFFECTED.
fn affected_count(f: &Frame) -> u64 {
    if f.frame_type == proto::RESP_AFFECTED && f.payload.len() == 8 {
        u64::from_le_bytes(f.payload[..8].try_into().unwrap())
    } else {
        0
    }
}

// ---------------------------------------------------------------------------
// Cluster join: a fresh node bootstraps the cluster state.
// ---------------------------------------------------------------------------

/// Per-peer quiesce wait during a join: concurrent joins (two nodes serving
/// REQ_SYNC at once) can deadlock on each other's write paths; the short
/// timeout breaks the cycle — a hold that cannot acquire the write path
/// answers "busy" and the whole join attempt aborts and retries.
pub(crate) const SYNC_HOLD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
/// Safety net on the serving side: a hold whose REQ_RELEASE never arrives
/// (the syncing node crashed) must not wedge the cluster's writes forever.
pub(crate) const SYNC_HOLD_MAX: std::time::Duration = std::time::Duration::from_secs(60);
/// One whole REQ_SYNC attempt (connect + quiesce + dump stream).
pub(crate) const SYNC_ATTEMPT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(90);
/// Dump chunks are reassembled by the joiner, so the split can be plain
/// byte slices; the bound just keeps frames well under the 64 MB cap.
pub(crate) const SYNC_CHUNK_BYTES: usize = 4 * 1024 * 1024;
/// Fresh-node bootstrap retries before giving up (peers may still be
/// coming up during a full cluster start). Fast, short rounds: a
/// simultaneously started cluster settles in the first round or two, and
/// the shorter gate-open window keeps cross-node reads lagging by
/// milliseconds instead of seconds.
const SYNC_ROUNDS: usize = 10;
const SYNC_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(500);

/// True when the database holds anything beyond the pubsub system table.
fn has_user_tables(db: &Database) -> bool {
    db.catalog()
        .iter()
        .any(|t| !docsql_core::engine::is_system_table(&t.name))
}

/// Why a REQ_HOLD did not grant. `Unreachable` means the peer could not be
/// connected to at all (down, partitioned) — it cannot be committing
/// writes, so a join may safely proceed without freezing it. `Busy` means
/// the peer is alive but could not freeze in time — writes may be in
/// flight, so the join attempt must abort.
enum HoldFail {
    Unreachable(String),
    Busy(String),
}

/// Send REQ_HOLD to one peer and return the granted hold id.
async fn hold_peer(
    state: &Arc<ServerState>,
    target: &str,
    advertise: &str,
) -> Result<u64, HoldFail> {
    let unreachable = |e: std::io::Error| HoldFail::Unreachable(e.to_string());
    let mut stream = match tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(target)).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => return Err(unreachable(e)),
        Err(_) => return Err(HoldFail::Unreachable("connect timed out".into())),
    };
    if let Some(token) = fanout_auth(state) {
        auth_on(&mut stream, token, state.transport_key.as_ref())
            .await
            .map_err(|e| HoldFail::Busy(format!("auth: {e}")))?;
    }
    let mut frame = Frame::new(proto::REQ_HOLD, advertise.as_bytes().to_vec());
    frame.flags = FLAG_REPLICATION;
    if let Some(k) = state.transport_key.as_ref() {
        frame.payload = crypto::seal(k, &frame.payload);
        frame.flags |= crypto::FLAG_ENCRYPTED;
    }
    let bytes = match frame.encode() {
        Ok(b) => b,
        Err(e) => return Err(HoldFail::Unreachable(e.to_string())),
    };
    {
        use tokio::io::AsyncWriteExt;
        match tokio::time::timeout(IO_TIMEOUT, stream.write_all(&bytes)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(unreachable(e)),
            Err(_) => return Err(HoldFail::Unreachable("hold write timed out".into())),
        }
        match tokio::time::timeout(IO_TIMEOUT, stream.flush()).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(unreachable(e)),
            Err(_) => return Err(HoldFail::Unreachable("hold flush timed out".into())),
        }
    }
    // The peer accepted the connection: if it now fails to answer in time
    // it is busy (alive, writes possibly in flight) — never "unreachable".
    let resp = read_response_frame(&mut stream)
        .await
        .map_err(|e| HoldFail::Busy(format!("no hold answer: {e}")))?;
    match resp.frame_type {
        proto::RESP_AFFECTED if resp.payload.len() == 8 => {
            Ok(u64::from_le_bytes(resp.payload[..8].try_into().unwrap()))
        }
        proto::RESP_ERROR => Err(HoldFail::Busy(format!(
            "peer answered {}",
            String::from_utf8_lossy(&resp.payload)
        ))),
        other => Err(HoldFail::Busy(format!("unexpected frame {other:#06x}"))),
    }
}

/// REQ_SYNC: serve the cluster's current state to a fresh joiner.
///
/// Order of events is the correctness contract:
/// 1. take this node's write path (no local write can commit from here on;
///    in-flight ones — fan-outs included — have fully landed);
/// 2. REQ_HOLD every peer: each waits out its own in-flight writes, then
///    freezes and registers the joiner, so when all holds are granted no
///    write can be in flight anywhere in the mesh;
/// 3. capture the dump — with the cluster frozen it is exactly the state
///    every committed write produced, and nothing fanned to the joiner yet;
/// 4. register the joiner locally (still under the write path);
/// 5. release the holds, drop the write path, stream the dump.
///
/// Writes committing after (4) fan out to the joiner and postdate the
/// snapshot; the joiner queues them (acknowledging immediately) until the
/// dump lands, then replays them in arrival order. The quiesce is
/// all-or-nothing among *live* peers: a hold that cannot connect at all
/// means the peer is down (it commits nothing while down — its earlier
/// writes already fanned to the survivors and are in the snapshot), so the
/// join proceeds without it; a hold answered "busy" by a live peer aborts
/// the attempt with RESP_ERROR and the joiner retries, because writes may
/// be in flight that would neither reach the snapshot nor the joiner.
/// Simultaneous joins break their cycles through the hold timeout (5s),
/// never by blocking a peer's writes for long.
async fn handle_sync(state: &Arc<ServerState>, frame: &Frame, tx: &mpsc::Sender<Frame>) {
    let advertise = String::from_utf8_lossy(&frame.payload).trim().to_string();
    let abort = |msg: String| async move {
        let _ = tx
            .send(Frame::new(proto::RESP_ERROR, err_payload(&msg)))
            .await;
    };
    if !state.sync_queue.lock().await.closed {
        return abort("sync: this node is itself joining; retry later".into()).await;
    }
    // Bound the write-path acquisition: two nodes serving joins for each
    // other would otherwise queue indefinitely; 5s keeps the stall visible
    // and bounded, the joiner simply retries.
    let order =
        match tokio::time::timeout(SYNC_HOLD_TIMEOUT, state.write_order.clone().lock_owned()).await
        {
            Ok(g) => g,
            Err(_) => {
                return abort("sync: write path busy; retry later".into()).await;
            }
        };
    let mut targets = state.peers.lock().await.clone();
    targets.sort();
    let mut held: Vec<(String, u64)> = Vec::new();
    for target in targets {
        let hold = hold_peer(state, &target, &advertise).await;
        match hold {
            Ok(id) => {
                held.push((target.clone(), id));
            }
            Err(HoldFail::Unreachable(detail)) => {
                // The peer cannot even accept a connection, so it cannot be
                // committing writes either — its pre-outage writes already
                // fanned to the surviving peers and are in the snapshot.
                // Skipping keeps joins working in a cluster with a dead
                // node (the down node catches up through the rejoin repair
                // on its own restart).
                eprintln!("sync: hold on {target} skipped, peer unreachable ({detail})");
                querylog::sync_event(
                    &state.sync_log,
                    "join-hold",
                    &target,
                    None,
                    true,
                    Some(format!("skipped, unreachable: {detail}")),
                );
            }
            Err(HoldFail::Busy(detail)) => {
                // The peer is alive but could not freeze its write path in
                // time — writes may be in flight that would land after the
                // snapshot and never reach the joiner. Abort the whole
                // attempt (releasing the holds taken so far) and retry.
                eprintln!("sync: hold on {target} failed ({detail}); aborting this attempt");
                querylog::sync_event(
                    &state.sync_log,
                    "join-hold",
                    &target,
                    None,
                    false,
                    Some(detail.clone()),
                );
                for (t, id) in &held {
                    let _ = forward_frame(
                        t,
                        proto::REQ_RELEASE,
                        &id.to_le_bytes(),
                        state.transport_key.as_ref(),
                        fanout_auth(state),
                    )
                    .await;
                }
                return abort(format!("sync: quiesce failed ({detail}); retry")).await;
            }
        }
    }
    // Capture with the mesh quiesced. The engine lock alone is enough
    // here: write_order stops new local writes, the holds stopped the
    // peers, and replication applies were drained by the hold handshakes.
    let dump = {
        let mut db = state.db.lock().unwrap_or_else(|p| p.into_inner());
        db.dump_script()
    };
    let table_count = {
        let db = state.db.lock().unwrap_or_else(|p| p.into_inner());
        db.catalog()
            .iter()
            .filter(|t| t.name != pubsub::PUBSUB_TABLE)
            .count() as u64
    };
    // Register the joiner still under the write path: every write before
    // this point is inside the dump and could not have fanned to it;
    // every write after it fans out to the joiner and postdates the dump.
    if !advertise.is_empty() {
        let mut peers = state.peers.lock().await;
        if !peers.contains(&advertise) && !is_self_peer(&state.listen, &advertise) {
            peers.push(advertise.clone());
            eprintln!("cluster join: registered peer {advertise}");
            querylog::sync_event(&state.sync_log, "join", &advertise, None, true, None);
        }
    }
    for (target, id) in &held {
        if let Err(e) = forward_frame(
            target,
            proto::REQ_RELEASE,
            &id.to_le_bytes(),
            state.transport_key.as_ref(),
            fanout_auth(state),
        )
        .await
        {
            eprintln!("sync: release on {target} failed ({e}); watchdog will drop the hold");
        }
    }
    drop(order);
    let script = match dump {
        Ok(s) => s,
        Err(e) => {
            let _ = tx
                .send(Frame::new(
                    proto::RESP_ERROR,
                    err_payload(&format!("sync: dump failed: {e}")),
                ))
                .await;
            return;
        }
    };
    let mut start = 0usize;
    while start < script.len() {
        let end = (start + SYNC_CHUNK_BYTES).min(script.len());
        let chunk = &script[start..end];
        // `script` is a String, so chunks are already valid UTF-8.
        let payload = match proto::encode_sql(chunk).map_err(|e| e.to_string()) {
            Ok(p) => p,
            Err(e) => {
                let _ = tx
                    .send(Frame::new(proto::RESP_ERROR, err_payload(&e)))
                    .await;
                return;
            }
        };
        if tx
            .send(Frame::new(proto::RESP_SYNC, payload))
            .await
            .is_err()
        {
            return; // joiner went away mid-stream
        }
        start = end;
    }
    let _ = tx
        .send(Frame::new(
            proto::RESP_AFFECTED,
            table_count.to_le_bytes().to_vec(),
        ))
        .await;
    querylog::sync_event(
        &state.sync_log,
        "sync-serve",
        &advertise,
        None,
        true,
        Some(format!("{table_count} tables, {} bytes", script.len())),
    );
}

/// REQ_HOLD: freeze this node's write path on behalf of a joiner served
/// elsewhere. Waiting for write_order first means every write this node
/// already committed has finished fanning out (fan-outs run inside the
/// write path), which is what makes the requester's snapshot complete.
/// The wait is capped: a node under continuous local writes answers "busy"
/// instead of freezing indefinitely — the requesting sync aborts and
/// retries, and simultaneous joins break their mutual waits here.
async fn handle_hold(state: &Arc<ServerState>, frame: &Frame) -> Frame {
    let advertise = String::from_utf8_lossy(&frame.payload).trim().to_string();
    let guard =
        match tokio::time::timeout(SYNC_HOLD_TIMEOUT, state.write_order.clone().lock_owned()).await
        {
            Ok(g) => g,
            Err(_) => {
                return Frame::new(
                    proto::RESP_ERROR,
                    err_payload("hold: write path busy; retry"),
                )
            }
        };
    let id = state
        .next_hold_id
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    state.holds.lock().await.insert(id, guard);
    // Register the joiner while still frozen: writes that start after the
    // hold fans out to the joiner; ones before it are in the dump.
    if !advertise.is_empty() {
        let mut peers = state.peers.lock().await;
        if !peers.contains(&advertise) && !is_self_peer(&state.listen, &advertise) {
            peers.push(advertise.clone());
            querylog::sync_event(&state.sync_log, "join", &advertise, None, true, None);
        }
    }
    // Watchdog: a crashed sync initiator must not wedge writes forever.
    let st = state.clone();
    tokio::spawn(async move {
        tokio::time::sleep(SYNC_HOLD_MAX).await;
        if st.holds.lock().await.remove(&id).is_some() {
            eprintln!("sync: hold {id} expired (initiator never released it)");
        }
    });
    Frame::new(proto::RESP_AFFECTED, id.to_le_bytes().to_vec())
}

/// REQ_RELEASE: drop one hold; the joiner stays registered as a peer.
async fn handle_release(state: &Arc<ServerState>, frame: &Frame) -> Frame {
    let id = frame
        .payload
        .get(..8)
        .and_then(|b| b.try_into().ok())
        .map(u64::from_le_bytes);
    let dropped = match id {
        Some(id) => state.holds.lock().await.remove(&id).is_some(),
        None => false,
    };
    Frame::new(
        proto::RESP_AFFECTED,
        (dropped as u64).to_le_bytes().to_vec(),
    )
}

/// REQ_DIGEST: per-table replication fingerprints for the rejoin repair
/// (see `repair_sync`). Read-only over the engine lock; the node
/// answering holds its writes for the O(data) hashing — a startup-time
/// cost, same class as serving a join dump.
async fn handle_digest(state: &Arc<ServerState>) -> Frame {
    let mut db = state.db.lock().unwrap_or_else(|p| p.into_inner());
    match db.digests() {
        Ok(list) => match serde_json::to_vec(&list) {
            Ok(payload) => Frame::new(proto::RESP_DIGEST, payload),
            Err(e) => Frame::new(proto::RESP_ERROR, err_payload(&format!("digest: {e}"))),
        },
        Err(e) => Frame::new(proto::RESP_ERROR, err_payload(&format!("digest: {e}"))),
    }
}

/// Terminal outcome of replaying a dump on the joining node.
enum JoinApply {
    /// Dump applied; syncing gate lifted.
    Applied,
    /// A client wrote here before the dump landed: keep that data, stop
    /// trying to join (normal fan-out keeps the node converged).
    LocalData,
    Failed(String),
}

/// Ask one peer for the cluster state and return the dump script.
async fn request_sync(state: &Arc<ServerState>, peer: &str) -> std::io::Result<String> {
    let attempt = async {
        let mut stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(peer)).await??;
        if let Some(token) = fanout_auth(state) {
            auth_on(&mut stream, token, state.transport_key.as_ref()).await?;
        }
        let mut frame = Frame::new(
            proto::REQ_SYNC,
            state
                .advertise
                .as_deref()
                .map(|a| a.as_bytes().to_vec())
                .unwrap_or_default(),
        );
        frame.flags = FLAG_REPLICATION;
        if let Some(k) = state.transport_key.as_ref() {
            frame.payload = crypto::seal(k, &frame.payload);
            frame.flags |= crypto::FLAG_ENCRYPTED;
        }
        let bytes = frame.encode().map_err(std::io::Error::other)?;
        tokio::time::timeout(IO_TIMEOUT, stream.write_all(&bytes)).await??;
        tokio::time::timeout(IO_TIMEOUT, stream.flush()).await??;
        let mut script = String::new();
        loop {
            let f = read_response_frame(&mut stream).await?;
            match f.frame_type {
                proto::RESP_SYNC => {
                    script.push_str(&proto::decode_sql(&f.payload).map_err(std::io::Error::other)?);
                }
                proto::RESP_AFFECTED => return Ok(script),
                proto::RESP_ERROR => {
                    return Err(std::io::Error::other(format!(
                        "{peer} rejected sync: {}",
                        String::from_utf8_lossy(&f.payload)
                    )))
                }
                other => {
                    return Err(std::io::Error::other(format!(
                        "{peer}: unexpected frame {other:#06x} during sync"
                    )))
                }
            }
        }
    };
    tokio::time::timeout(SYNC_ATTEMPT_TIMEOUT, attempt).await?
}

/// Replay the dump exactly like the join protocol prescribes: under the
/// write path, only when still fresh, inside one transaction (a failure
/// rolls back and leaves the node fresh for the next attempt). On success
/// the join-intake queue replays right after the dump (still under the
/// write path) and the gate closes.
async fn apply_sync(
    state: &Arc<ServerState>,
    script: &str,
    peer: &str,
    heads: &[(String, u64)],
) -> JoinApply {
    let Some(_order) = lock_engine_for_write(state).await else {
        return JoinApply::Failed("timed out waiting for the open transaction".into());
    };
    {
        let mut db = state.db.lock().unwrap_or_else(|p| p.into_inner());
        if has_user_tables(&db) {
            eprintln!("bootstrap sync aborted: local data appeared before the dump landed");
            return JoinApply::LocalData;
        }
        if let Err(e) = db.execute("BEGIN") {
            return JoinApply::Failed(format!("BEGIN: {e}"));
        }
        let batch = db.execute_batch(script);
        if let Some(err) = batch.error {
            let _ = db.execute("ROLLBACK");
            return JoinApply::Failed(format!("statement {}: {}", err.statement, err.message));
        }
        if let Err(e) = db.execute("COMMIT") {
            let _ = db.execute("ROLLBACK");
            return JoinApply::Failed(format!("COMMIT: {e}"));
        }
    }
    drain_sync_queue(state).await;
    seed_positions(state, heads);
    eprintln!("bootstrap sync from {peer} complete");
    querylog::sync_event(
        &state.sync_log,
        "bootstrap",
        peer,
        None,
        true,
        Some(format!("{} bytes", script.len())),
    );
    JoinApply::Applied
}

/// Replay the join-intake queue in arrival order and close the gate
/// (snapshot < queued < direct is the total order; see
/// [ServerState::sync_queue]). Callers run it at every terminal state of
/// the bootstrap — the queue holds acknowledged writes that must land no
/// matter how the join concluded.
async fn drain_sync_queue(state: &Arc<ServerState>) {
    loop {
        let batch = {
            let mut gate = state.sync_queue.lock().await;
            if gate.pending.is_empty() {
                gate.closed = true;
                Vec::new()
            } else {
                std::mem::take(&mut gate.pending)
            }
        };
        if batch.is_empty() {
            return;
        }
        for sql in batch {
            let resp = execute_sql(state, &sql, false, true, None, true).await;
            if resp.frame_type == proto::RESP_ERROR {
                let msg = String::from_utf8_lossy(&resp.payload).into_owned();
                eprintln!("sync: queued replay failed: {msg}");
                querylog::sync_event(
                    &state.sync_log,
                    "bootstrap",
                    "",
                    Some(&sql),
                    false,
                    Some(msg),
                );
            } else {
                querylog::record(state, "sync", &sql, 0.0, &resp, true);
            }
        }
    }
}

/// Startup sync: a fresh node pulls the cluster state from a peer that
/// already holds data, retrying while peers come up; a node that already
/// holds data runs the rejoin repair (digest compare, snapshot adopt on
/// divergence). Probing first keeps a born-empty cluster (full
/// simultaneous start) from quiescing itself for the hold timeouts —
/// such meshes are covered by static peer config, and nodes that hold
/// data fan every later write to the registered joiner. Every terminal
/// state drains the join-intake queue: acknowledged writes land no
/// matter how the flow concluded.
async fn bootstrap_sync(state: Arc<ServerState>, fresh: bool) {
    let peers = state.peers.lock().await.clone();
    if !fresh {
        repair_sync(state, peers).await;
        return;
    }
    let mut last_err = String::from("no peer answered");
    for _round in 0..SYNC_ROUNDS {
        let mut saw_data = false;
        let mut saw_empty = false;
        let mut heads: Vec<(String, u64)> = Vec::new();
        for peer in &peers {
            match probe_peer_info(&state, peer).await {
                Ok(info) => {
                    if let (Some(node_id), Some(head)) = (&info.node_id, info.journal_head) {
                        heads.push((node_id.clone(), head));
                    }
                    if info.tables > 0 {
                        saw_data = true;
                        match join_from(&state, peer, &heads).await {
                            JoinApply::Applied => return,
                            JoinApply::LocalData => {
                                drain_sync_queue(&state).await;
                                return;
                            }
                            JoinApply::Failed(e) => {
                                eprintln!("bootstrap sync from {peer} failed: {e}");
                                last_err = e;
                            }
                        }
                    } else {
                        saw_empty = true;
                    }
                }
                Err(e) => {
                    eprintln!("bootstrap probe of {peer} failed: {e}");
                    last_err = e.to_string();
                }
            }
        }
        if !saw_data && saw_empty {
            eprintln!(
                "bootstrap sync: peers hold no data (born-empty cluster); \
                 serving fresh — later writes fan out from the data holders"
            );
            drain_sync_queue(&state).await;
            querylog::sync_event(
                &state.sync_log,
                "bootstrap",
                "",
                None,
                false,
                Some("born-empty cluster".into()),
            );
            return;
        }
        tokio::time::sleep(SYNC_RETRY_DELAY).await;
    }
    eprintln!("bootstrap sync gave up after {SYNC_ROUNDS} rounds ({last_err}); serving fresh");
    drain_sync_queue(&state).await;
    querylog::sync_event(
        &state.sync_log,
        "bootstrap",
        "",
        None,
        false,
        Some(last_err),
    );
}

/// Pull and apply the cluster state from one peer.
async fn join_from(state: &Arc<ServerState>, peer: &str, heads: &[(String, u64)]) -> JoinApply {
    let script = match request_sync(state, peer).await {
        Ok(s) => s,
        Err(e) => return JoinApply::Failed(e.to_string()),
    };
    apply_sync(state, &script, peer, heads).await
}

/// Rejoin-repair rounds before giving up. Must outlast a peer's own
/// startup-sync window (~5s of fresh-bootstrap rounds, during which it
/// refuses to serve a snapshot) so mutually-restarting nodes cannot
/// starve each other's pull. Probes run concurrently, so a round costs
/// one connect timeout at worst.
const REPAIR_ROUNDS: usize = 20;

/// Rejoin repair (anti-entropy at restart): a node that already holds
/// data compares table digests with every reachable peer and, when no
/// peer agrees with it, replaces its state with the cluster's snapshot —
/// the same quiesce + dump flow a fresh join uses, minus the LocalData
/// guard (replacing local data is the point). Fan-out never back-fills:
/// writes a node missed while offline would stay missing forever without
/// this.
///
/// Reference choice is election-based over the digest reports, and every
/// node evaluates the same rules, so the mesh agrees on one direction:
/// a peer reporting our exact state means we serve as-is; a group of
/// ≥2 peers reporting identical digests is a surviving majority and is
/// adopted wholesale (its members see each other and serve, non-members
/// pull); with no majority (every state unique — a full split), the
/// reference is elected by more rows first (the common rejoin shape: the
/// stale node is behind), ties by serialized digests so nodes never have
/// to compare addresses across namespaces. A lone node behind (offline
/// restart), a partition minority, and a fan-out-failure loser all
/// converge here; the cost is that minority-side writes are overwritten
/// by the adopted snapshot — there is no row-level merge (no vector
/// clocks to order conflicting writes). Partition divergence without a
/// restart is not repaired (no startup event); the next restart of any
/// divergent node heals the mesh.
///
/// An empty reference state is never adopted — a node holding the only
/// copy of data must not erase it because every peer lost theirs.
async fn repair_sync(state: Arc<ServerState>, peers: Vec<String>) {
    for round in 0..REPAIR_ROUNDS {
        // Probe every peer for digests + journal info, concurrently.
        let mut probes = Vec::new();
        for peer in &peers {
            let st = state.clone();
            let target = peer.clone();
            probes.push(tokio::spawn(async move {
                let digests = probe_peer_digests(&st, &target).await;
                let info = probe_peer_info(&st, &target).await;
                (target, digests, info)
            }));
        }
        let mut reports: Vec<(String, Vec<TableDigest>)> = Vec::new();
        let mut infos: Vec<(String, PeerInfo)> = Vec::new();
        for probe in probes {
            match probe.await {
                Ok((peer, Ok(digests), Ok(info))) => {
                    reports.push((peer.clone(), digests));
                    infos.push((peer, info));
                }
                Ok((peer, Ok(digests), Err(e))) => {
                    // Digests arrived, journal info did not: this peer can
                    // still serve a snapshot, never incremental catch-up.
                    eprintln!("repair: journal probe of {peer} failed: {e}");
                    reports.push((peer, digests));
                }
                Ok((peer, Err(e), _)) => {
                    eprintln!("repair: digest probe of {peer} failed: {e}");
                }
                Err(e) => eprintln!("repair: probe task failed: {e}"),
            }
        }
        if reports.is_empty() {
            // Nobody reachable: peers may still be coming up after a full
            // cluster restart — retry before serving possibly-stale state
            // (the majority signal needs more than zero witnesses).
            eprintln!("repair: no peer reachable (round {})", round + 1);
            tokio::time::sleep(SYNC_RETRY_DELAY).await;
            continue;
        }
        let local = {
            let mut db = state.db.lock().unwrap_or_else(|p| p.into_inner());
            db.digests()
        };
        let local = match local {
            Ok(d) => d,
            Err(e) => {
                eprintln!("repair: local digests failed: {e}");
                break;
            }
        };
        if reports.iter().any(|(_, d)| d == &local) {
            drain_sync_queue(&state).await;
            return;
        }

        // Phase 1 — incremental catch-up ("sync exactly what's missing"):
        // when every peer reports its journal window and every position is
        // known and inside it, pull the missed op ranges and re-check.
        let plan = catchup_plan(&state, &infos).await;
        match &plan {
            Some(plan) if !plan.is_empty() => {
                let total: u64 = plan.iter().map(|t| t.to - t.from).sum();
                eprintln!(
                    "rejoin repair (round {}): pulling {} missed op(s) from {} origin journal(s)",
                    round + 1,
                    total,
                    plan.len()
                );
                match run_catchup(&state, plan).await {
                    Ok(()) => {
                        // Landing the backlog lets the live writes queued on
                        // the open gate apply right after it (their seqs
                        // postdate the pulled range), so drain before the
                        // digest re-check — otherwise the queued writes
                        // would read as divergence and trigger a snapshot.
                        // NOTE: queued writes do not advance positions (a
                        // position may lag its origin until the next pull;
                        // replays are order-idempotent, and any residual
                        // divergence still lands in the snapshot fallback).
                        drain_sync_queue(&state).await;
                        // Fresh digest check: the mesh kept writing while
                        // the backlog replayed.
                        let mut fresh_probes = Vec::new();
                        for peer in &peers {
                            let st = state.clone();
                            let target = peer.clone();
                            fresh_probes.push(tokio::spawn(async move {
                                let d = probe_peer_digests(&st, &target).await;
                                (target, d)
                            }));
                        }
                        let mut fresh: Vec<(String, Vec<TableDigest>)> = Vec::new();
                        for probe in fresh_probes {
                            if let Ok((peer, Ok(d))) = probe.await {
                                fresh.push((peer, d));
                            }
                        }
                        let fresh_local = {
                            let mut db = state.db.lock().unwrap_or_else(|p| p.into_inner());
                            db.digests()
                        };
                        if let Ok(fresh_local) = fresh_local {
                            if fresh.iter().any(|(_, d)| d == &fresh_local) {
                                eprintln!(
                                    "rejoin repair: incremental catch-up complete — \
                                     cluster converged without a snapshot"
                                );
                                querylog::sync_event(
                                    &state.sync_log,
                                    "catchup",
                                    "",
                                    None,
                                    true,
                                    Some(format!("{total} ops from {} origins", plan.len())),
                                );
                                drain_sync_queue(&state).await;
                                return;
                            }
                        }
                        eprintln!(
                            "rejoin repair: digests still differ after catch-up; \
                             falling back to snapshot"
                        );
                    }
                    Err(e) => {
                        eprintln!("rejoin repair: catch-up failed: {e}; falling back to snapshot");
                        querylog::sync_event(&state.sync_log, "catchup", "", None, false, Some(e));
                    }
                }
            }
            Some(_) => {}
            None if round == 0 => {
                eprintln!(
                    "rejoin repair: no usable journal positions (new/old peer or \
                     trimmed window); snapshot repair"
                );
            }
            None => {}
        }

        // Phase 2 — snapshot election and adoption (the safe fallback).
        let heads: Vec<(String, u64)> = infos
            .iter()
            .filter_map(|(_, info)| Some((info.node_id.clone()?, info.journal_head?)))
            .collect();
        match decide_repair(&local, &reports) {
            RepairDecision::Converged => {
                drain_sync_queue(&state).await;
                return;
            }
            RepairDecision::Serve => {
                eprintln!(
                    "rejoin repair (round {}): no agreeing peer; election \
                     picked this node's state as the reference",
                    round + 1
                );
                drain_sync_queue(&state).await;
                return;
            }
            RepairDecision::Pull(source) => {
                eprintln!(
                    "rejoin repair (round {}): state differs from peers; \
                     adopting snapshot from {source}",
                    round + 1
                );
                let applied = match request_sync(&state, &source).await {
                    Ok(script) => match apply_repair_sync(&state, &script, &source, &heads).await {
                        JoinApply::Applied => Ok(()),
                        JoinApply::Failed(e) => Err(e),
                        JoinApply::LocalData => Err("unexpected local-data outcome".into()),
                    },
                    Err(e) => Err(e.to_string()),
                };
                match applied {
                    Ok(()) => return,
                    Err(e) => {
                        eprintln!("rejoin repair from {source} failed: {e}");
                        querylog::sync_event(
                            &state.sync_log,
                            "repair",
                            &source,
                            None,
                            false,
                            Some(e),
                        );
                    }
                }
            }
        }
        tokio::time::sleep(SYNC_RETRY_DELAY).await;
    }
    eprintln!(
        "rejoin repair gave up; serving local state (divergent peers \
         repair on their own restart)"
    );
    drain_sync_queue(&state).await;
    querylog::sync_event(
        &state.sync_log,
        "repair",
        "",
        None,
        false,
        Some("gave up".into()),
    );
}

/// One origin's missed-op range: pull journal entries (from, to] from
/// `addr` and apply them in order.
struct CatchupTask {
    addr: String,
    node_id: String,
    from: u64,
    to: u64,
}

/// Build the incremental catch-up plan, or None when catch-up is
/// impossible: any peer without journal info (old version, probe
/// failure), any unknown position (never adopted from that origin), or
/// any position outside the peer's retained window (trimmed past) all
/// force the snapshot path. Positions can only lag the data, so a
/// partial catch-up is always safe — the digest re-check after it
/// decides whether a snapshot is still needed.
async fn catchup_plan(
    state: &Arc<ServerState>,
    infos: &[(String, PeerInfo)],
) -> Option<Vec<CatchupTask>> {
    let mut plan = Vec::new();
    for (addr, info) in infos {
        let node_id = info.node_id.clone()?;
        let head = info.journal_head?;
        let oldest = info.journal_oldest?;
        let pos = {
            let mut db = state.db.lock().unwrap_or_else(|p| p.into_inner());
            db.position_get(&node_id).ok().flatten()
        }?;
        if pos > head || pos + 1 < oldest {
            return None;
        }
        if head > pos {
            plan.push(CatchupTask {
                addr: addr.clone(),
                node_id,
                from: pos,
                to: head,
            });
        }
    }
    Some(plan)
}

/// Execute a catch-up plan: pull each origin's missed range in order and
/// record the origin's head as the new position.
async fn run_catchup(state: &Arc<ServerState>, plan: &[CatchupTask]) -> Result<(), String> {
    for task in plan {
        let head = catch_up_from(state, &task.addr, task.from)
            .await
            .map_err(|e| format!("pull from {}: {e}", task.addr))?;
        {
            let mut db = state.db.lock().unwrap_or_else(|p| p.into_inner());
            db.position_set(&task.node_id, head)
                .map_err(|e| format!("position update: {e}"))?;
        }
    }
    Ok(())
}

/// After an adoption replaces the whole state, seed positions from the
/// heads sampled during the probe round: the snapshot contains every op
/// up to the freeze, and later writes arrive through the live fan-out
/// with higher seqs. Origins that could not be probed stay position-less
/// (no incremental trust) and are covered by snapshot repair.
fn seed_positions(state: &Arc<ServerState>, heads: &[(String, u64)]) {
    let mut db = state.db.lock().unwrap_or_else(|p| p.into_inner());
    for (node_id, head) in heads {
        if let Err(e) = db.position_set(node_id, *head) {
            eprintln!("catchup position seed failed: {e}");
        }
    }
}

/// Outcome of comparing the local digests with the peers' reports (see
/// `repair_sync`).
enum RepairDecision {
    /// A reachable peer reports the same state: nothing to repair.
    Converged,
    /// No peer agrees, but the election picked this node's state as the
    /// reference: close the sync gate and serve — the divergent peers
    /// pull from here.
    Serve,
    /// Adopt this peer's snapshot.
    Pull(String),
}

/// Election ordering key for one state: more rows first (a richer state
/// beats a sparser one — the common rejoin shape is a node that is
/// simply behind), ties by the serialized digests so every node computes
/// the same order without comparing addresses across namespaces.
fn digest_state_key(digests: &[TableDigest]) -> (u64, Vec<u8>) {
    (
        digests.iter().map(|t| t.rows).sum(),
        serde_json::to_vec(digests).unwrap_or_default(),
    )
}

/// `true` when `candidate` outranks `incumbent` under the election key.
fn state_key_better(candidate: &(u64, Vec<u8>), incumbent: &(u64, Vec<u8>)) -> bool {
    candidate.0 > incumbent.0 || (candidate.0 == incumbent.0 && candidate.1 < incumbent.1)
}

/// Decide what the rejoin repair should do (see `repair_sync` for the
/// election rules). Every node runs the same rules over its own view, so
/// the mesh converges on one reference without cross-node negotiation.
fn decide_repair(local: &[TableDigest], reports: &[(String, Vec<TableDigest>)]) -> RepairDecision {
    if reports.iter().any(|(_, d)| d == local) {
        return RepairDecision::Converged;
    }
    // Group peers by identical report; a group of ≥2 agreeing peers is a
    // surviving majority and always wins — unless it holds no data (never
    // adopt emptiness: a lone remaining copy must survive the others'
    // loss).
    let mut groups: Vec<(&Vec<TableDigest>, Vec<&str>)> = Vec::new();
    for (peer, digests) in reports {
        match groups.iter_mut().find(|(d, _)| *d == digests) {
            Some((_, members)) => members.push(peer.as_str()),
            None => groups.push((digests, vec![peer.as_str()])),
        }
    }
    groups.sort_by(|a, b| {
        b.1.len().cmp(&a.1.len()).then_with(|| {
            let (ka, kb) = (digest_state_key(a.0), digest_state_key(b.0));
            kb.0.cmp(&ka.0).then_with(|| ka.1.cmp(&kb.1))
        })
    });
    if let Some((digests, members)) = groups.first() {
        if members.len() >= 2 && digests.iter().map(|t| t.rows).sum::<u64>() > 0 {
            // Any member serves the same snapshot; picking the smallest
            // address is only for stable logs. Addresses come from this
            // node's own peer config, so no cross-namespace comparison is
            // needed.
            return RepairDecision::Pull(members.iter().min().unwrap().to_string());
        }
    }
    // Residue: no majority (every reachable state unique). Elect the
    // reference over local + all reports; the winner serves, everyone
    // else pulls.
    let local_key = digest_state_key(local);
    let mut best: Option<(&str, (u64, Vec<u8>))> = None;
    for (peer, digests) in reports {
        let key = digest_state_key(digests);
        let better = match &best {
            None => true,
            Some((_, bk)) => state_key_better(&key, bk),
        };
        if better {
            best = Some((peer.as_str(), key));
        }
    }
    match best {
        Some((peer, key)) if state_key_better(&key, &local_key) => {
            RepairDecision::Pull(peer.to_string())
        }
        _ => RepairDecision::Serve,
    }
}

/// Replace this node's whole user state with the cluster snapshot:
/// inside one transaction, wipe the local catalog (bypassing DROP's FK
/// guards — the snapshot defines the entire truth) and replay the dump.
/// A failure rolls back to the pre-repair state, divergence and all.
/// Inbound replication writes queue on the open sync gate meanwhile and
/// replay in arrival order right after — snapshot < queued < direct is
/// the total order (see [ServerState::sync_queue]). On success the
/// catch-up positions are seeded from the probe-round journal heads, so
/// later rejoins can pull increments instead of snapshots.
async fn apply_repair_sync(
    state: &Arc<ServerState>,
    script: &str,
    peer: &str,
    heads: &[(String, u64)],
) -> JoinApply {
    let Some(_order) = lock_engine_for_write(state).await else {
        return JoinApply::Failed("timed out waiting for the open transaction".into());
    };
    {
        let mut db = state.db.lock().unwrap_or_else(|p| p.into_inner());
        if let Err(e) = db.execute("BEGIN") {
            return JoinApply::Failed(format!("BEGIN: {e}"));
        }
        if let Err(e) = db.wipe_user_tables() {
            let _ = db.execute("ROLLBACK");
            return JoinApply::Failed(format!("catalog wipe: {e}"));
        }
        let batch = db.execute_batch(script);
        if let Some(err) = batch.error {
            let _ = db.execute("ROLLBACK");
            return JoinApply::Failed(format!("statement {}: {}", err.statement, err.message));
        }
        if let Err(e) = db.execute("COMMIT") {
            let _ = db.execute("ROLLBACK");
            return JoinApply::Failed(format!("COMMIT: {e}"));
        }
    }
    drain_sync_queue(state).await;
    seed_positions(state, heads);
    eprintln!("rejoin repair from {peer} complete");
    querylog::sync_event(
        &state.sync_log,
        "repair",
        peer,
        None,
        true,
        Some(format!("{} bytes", script.len())),
    );
    JoinApply::Applied
}

/// One peer's table digests over REQ_DIGEST (see `repair_sync`). Errors
/// mean "unknown" (unreachable / auth mismatch), never "divergent". The
/// frame rides FLAG_REPLICATION like every node-internal frame — under
/// cluster-token auth a peer-role connection only accepts replication
/// traffic.
async fn probe_peer_digests(
    state: &Arc<ServerState>,
    peer: &str,
) -> std::io::Result<Vec<TableDigest>> {
    let mut stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(peer)).await??;
    if let Some(token) = fanout_auth(state) {
        auth_on(&mut stream, token, state.transport_key.as_ref()).await?;
    }
    let mut frame = Frame::new(proto::REQ_DIGEST, vec![]);
    frame.flags = FLAG_REPLICATION;
    if let Some(k) = state.transport_key.as_ref() {
        frame.payload = crypto::seal(k, &frame.payload);
        frame.flags |= crypto::FLAG_ENCRYPTED;
    }
    let bytes = frame.encode().map_err(std::io::Error::other)?;
    tokio::time::timeout(IO_TIMEOUT, stream.write_all(&bytes)).await??;
    tokio::time::timeout(IO_TIMEOUT, stream.flush()).await??;
    let resp = read_response_frame(&mut stream).await?;
    if resp.frame_type != proto::RESP_DIGEST {
        return Err(std::io::Error::other(format!(
            "{peer}: digest probe answered {} {}",
            resp.frame_type,
            String::from_utf8_lossy(&resp.payload)
        )));
    }
    serde_json::from_slice(&resp.payload)
        .map_err(|e| std::io::Error::other(format!("bad digest payload: {e}")))
}

/// What one peer reported about itself over REQ_STATUS: its user-table
/// count (fresh-join probe), its persistent identity, and its journal
/// window. `node_id`/journal fields are None from peers predating
/// catch-up replication — incremental repair is impossible against them
/// and the snapshot path takes over.
#[derive(Debug, Clone)]
struct PeerInfo {
    tables: u64,
    node_id: Option<String>,
    journal_head: Option<u64>,
    journal_oldest: Option<u64>,
}

/// One peer's status + journal window over REQ_STATUS.
async fn probe_peer_info(state: &Arc<ServerState>, peer: &str) -> std::io::Result<PeerInfo> {
    let mut stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(peer)).await??;
    if let Some(token) = fanout_auth(state) {
        auth_on(&mut stream, token, state.transport_key.as_ref()).await?;
    }
    let mut frame = Frame::new(proto::REQ_STATUS, vec![]);
    frame.flags = FLAG_REPLICATION;
    if let Some(k) = state.transport_key.as_ref() {
        frame.payload = crypto::seal(k, &frame.payload);
        frame.flags |= crypto::FLAG_ENCRYPTED;
    }
    let bytes = frame.encode().map_err(std::io::Error::other)?;
    tokio::time::timeout(IO_TIMEOUT, stream.write_all(&bytes)).await??;
    tokio::time::timeout(IO_TIMEOUT, stream.flush()).await??;
    let resp = read_response_frame(&mut stream).await?;
    if resp.frame_type != proto::RESP_STATUS {
        return Err(std::io::Error::other(format!(
            "{peer}: status probe answered {} {}",
            resp.frame_type,
            String::from_utf8_lossy(&resp.payload)
        )));
    }
    let v: serde_json::Value = serde_json::from_slice(&resp.payload)
        .map_err(|e| std::io::Error::other(format!("bad status payload: {e}")))?;
    Ok(PeerInfo {
        tables: v["totals"]["tables"].as_u64().unwrap_or(0),
        node_id: v["cluster_id"].as_str().map(String::from),
        journal_head: v["journal_head"].as_u64(),
        journal_oldest: v["journal_oldest"].as_u64(),
    })
}

/// Parse a REQ_SQL_SEQ payload: [u64 seq][u32 len][node_id][sql].
fn parse_seq_frame(payload: &[u8]) -> Option<(u64, String, String)> {
    if payload.len() < 12 {
        return None;
    }
    let seq = u64::from_le_bytes(payload[..8].try_into().ok()?);
    let id_len = u32::from_le_bytes(payload[8..12].try_into().ok()?) as usize;
    let rest = payload.get(12..)?;
    let node_id = std::str::from_utf8(rest.get(..id_len)?).ok()?.to_string();
    let sql = proto::decode_sql(rest.get(id_len..)?).ok()?;
    Some((seq, node_id, sql))
}

/// Pack journal entries into one RESP_CATCHUP payload, respecting the
/// frame budget but always making progress (at least one entry).
/// Returns the payload and the number of entries packed.
fn pack_catchup_entries(entries: &[(u64, String)], budget: usize) -> (Vec<u8>, usize) {
    let mut out = Vec::new();
    let mut count = 0usize;
    for (seq, sql) in entries {
        let entry_len = 8 + 4 + sql.len();
        if count > 0 && out.len() + entry_len > budget {
            break;
        }
        out.extend_from_slice(&seq.to_le_bytes());
        out.extend_from_slice(&(sql.len() as u32).to_le_bytes());
        out.extend_from_slice(sql.as_bytes());
        count += 1;
    }
    (out, count)
}

/// Decode one RESP_CATCHUP payload.
fn decode_catchup_entries(payload: &[u8]) -> std::io::Result<Vec<(u64, String)>> {
    let mut out = Vec::new();
    let mut rest = payload;
    while !rest.is_empty() {
        if rest.len() < 12 {
            return Err(std::io::Error::other("truncated catchup entry"));
        }
        let seq = u64::from_le_bytes(rest[..8].try_into().unwrap());
        let len = u32::from_le_bytes(rest[8..12].try_into().unwrap()) as usize;
        let sql_bytes = rest
            .get(12..12 + len)
            .ok_or_else(|| std::io::Error::other("truncated catchup sql"))?;
        let sql = String::from_utf8(sql_bytes.to_vec())
            .map_err(|e| std::io::Error::other(format!("catchup sql utf8: {e}")))?;
        out.push((seq, sql));
        rest = &rest[12 + len..];
    }
    Ok(out)
}

/// REQ_CATCHUP: serve journal entries after the requester's position so a
/// rejoined peer can catch up incrementally ("sync exactly what's
/// missing"). Read-only over the journal table; chunks ride the
/// connection's frame channel, terminated by RESP_AFFECTED(head).
async fn handle_catchup(state: &Arc<ServerState>, frame: &Frame, tx: &mpsc::Sender<Frame>) {
    let after = if frame.payload.len() == 8 {
        u64::from_le_bytes(frame.payload[..8].try_into().unwrap())
    } else {
        let _ = tx
            .send(Frame::new(
                proto::RESP_ERROR,
                err_payload("malformed REQ_CATCHUP payload"),
            ))
            .await;
        return;
    };
    const BUDGET: usize = 4 * 1024 * 1024;
    let mut after = after;
    loop {
        let batch = {
            let mut db = state.db.lock().unwrap_or_else(|p| p.into_inner());
            db.journal_range(after, 512)
        };
        let batch = match batch {
            Ok(b) => b,
            Err(e) => {
                let _ = tx
                    .send(Frame::new(
                        proto::RESP_ERROR,
                        err_payload(&format!("catchup read: {e}")),
                    ))
                    .await;
                return;
            }
        };
        if batch.is_empty() {
            break;
        }
        let mut idx = 0;
        while idx < batch.len() {
            let (payload, packed) = pack_catchup_entries(&batch[idx..], BUDGET);
            if tx
                .send(Frame::new(proto::RESP_CATCHUP, payload))
                .await
                .is_err()
            {
                return; // requester went away mid-stream
            }
            idx += packed;
        }
        after = batch.last().expect("non-empty batch").0;
    }
    let head = {
        let mut db = state.db.lock().unwrap_or_else(|p| p.into_inner());
        db.journal_head()
    };
    let head = head.unwrap_or(0);
    let _ = tx
        .send(Frame::new(
            proto::RESP_AFFECTED,
            head.to_le_bytes().to_vec(),
        ))
        .await;
}

/// Pull journal entries after `from_seq` from one origin and apply them
/// in order. Returns the origin's journal head at serve time; the caller
/// records it as the new position. Any apply error aborts the pull —
/// the repair then falls back to snapshot adoption.
async fn catch_up_from(state: &Arc<ServerState>, peer: &str, after: u64) -> std::io::Result<u64> {
    let attempt = async {
        let mut stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(peer)).await??;
        if let Some(token) = fanout_auth(state) {
            auth_on(&mut stream, token, state.transport_key.as_ref()).await?;
        }
        let mut frame = Frame::new(proto::REQ_CATCHUP, after.to_le_bytes().to_vec());
        frame.flags = FLAG_REPLICATION;
        if let Some(k) = state.transport_key.as_ref() {
            frame.payload = crypto::seal(k, &frame.payload);
            frame.flags |= crypto::FLAG_ENCRYPTED;
        }
        let bytes = frame.encode().map_err(std::io::Error::other)?;
        tokio::time::timeout(IO_TIMEOUT, stream.write_all(&bytes)).await??;
        tokio::time::timeout(IO_TIMEOUT, stream.flush()).await??;
        loop {
            let f = tokio::time::timeout(IO_TIMEOUT, read_response_frame(&mut stream)).await??;
            match f.frame_type {
                proto::RESP_CATCHUP => {
                    for (_, sql) in decode_catchup_entries(&f.payload)? {
                        let resp = execute_sql(state, &sql, false, true, None, false).await;
                        if resp.frame_type == proto::RESP_ERROR {
                            return Err(std::io::Error::other(format!(
                                "catch-up replay failed: {}",
                                String::from_utf8_lossy(&resp.payload)
                            )));
                        }
                    }
                }
                proto::RESP_AFFECTED if f.payload.len() == 8 => {
                    return Ok(u64::from_le_bytes(f.payload[..8].try_into().unwrap()));
                }
                proto::RESP_ERROR => {
                    return Err(std::io::Error::other(format!(
                        "{peer} rejected catch-up: {}",
                        String::from_utf8_lossy(&f.payload)
                    )));
                }
                other => {
                    return Err(std::io::Error::other(format!(
                        "{peer}: unexpected frame {other:#06x} during catch-up"
                    )));
                }
            }
        }
    };
    tokio::time::timeout(SYNC_ATTEMPT_TIMEOUT, attempt).await?
}

#[cfg(test)]
mod security_tests {
    use super::*;

    #[test]
    fn token_strength_rejects_weak_secrets() {
        assert!(check_token_strength("DOCSQL_TOKEN", "long-enough-secret").is_ok());
        // Too short (below the complexity floor).
        let e = check_token_strength("DOCSQL_TOKEN", "s3cret").unwrap_err();
        assert!(e.contains("too weak"), "{e}");
        // Single repeated character, even long.
        let e = check_token_strength("DOCSQL_TOKEN", &"a".repeat(32)).unwrap_err();
        assert!(e.contains("too weak"), "{e}");
        // Exactly at the floor passes.
        assert!(check_token_strength("DOCSQL_TOKEN", "12345678").is_ok());
    }
}
