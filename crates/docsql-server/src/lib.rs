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
//! - REQ_SESSIONS  → RESP_ROWS: activity monitor — one row per live
//!   connection (id/peer/identity/state/current statement), admin-only
//! - REQ_KILL      → RESP_AFFECTED: close a connection by id after its
//!   current statement (admin-only; self-kill refused)
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
//! Every connection shares one engine instance behind an RwLock: write
//! statements take the write tier (single-writer v1 — tx_owner/write_order
//! unchanged), plain SELECTs take the read tier and run concurrently (MVCC
//! stage A; per-shard concurrency stays a later milestone).

pub mod backup;
pub mod crypto;
pub mod metrics;
pub mod pubsub;
pub mod querylog;
pub mod quorum;
pub mod s3;
pub mod tls;

use docsql_core::engine::{AnyStmt, Database, ExecOutcome, TableDigest, TxControl};
use docsql_core::now_ms;
use docsql_core::proto::{self, Frame, MAX_FRAME_BYTES};
use docsql_core::value::Value;
use std::collections::VecDeque;
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
    /// MVCC stage A: RwLock tiers the single-writer engine — write
    /// statements take the write lock (exclusive, unchanged semantics);
    /// plain SELECTs take the read lock and run concurrently. All other
    /// engine invariants (single writer, tx_owner, write_order) are
    /// unchanged: the read tier only ever executes classified read-only
    /// SELECTs.
    pub db: std::sync::RwLock<Database>,
    /// Process-lifetime runtime counters, embedded into REQ_STATUS and
    /// formatted into Prometheus text by the web console's /metrics.
    pub metrics: std::sync::Arc<metrics::Metrics>,
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
    /// Restore replay progress (applied/total), published locklessly so
    /// the replay loop never takes the backup mutex under `write_order`.
    pub restore_progress: std::sync::Arc<backup::RestoreProgress>,
    /// Bumped whenever user/role state changes (CREATE USER / GRANT / …):
    /// user connections re-resolve their grants when they notice a new
    /// epoch, so revocations take effect on the next statement.
    pub grants_epoch: std::sync::atomic::AtomicU64,
    /// True once at least one database user exists — token-less anonymous
    /// access closes from then on (legacy open mode covers user-less
    /// deployments only).
    pub has_users: std::sync::atomic::AtomicBool,
    /// Auth-failure lockout (identity-authentication failure handling):
    /// source IP -> failure timestamps. At AUTH_LOCK_THRESHOLD failures
    /// inside AUTH_LOCK_WINDOW the source is locked out for
    /// AUTH_LOCKOUT and further AUTH attempts are rejected unread.
    pub auth_failures:
        tokio::sync::Mutex<std::collections::HashMap<String, Vec<std::time::Instant>>>,
    /// Server-wide PBKDF2 concurrency gate: password verification runs on
    /// the blocking pool, and unbounded concurrent derivations would let
    /// distributed auth floods starve every other blocking task.
    pub auth_gate: std::sync::Arc<tokio::sync::Semaphore>,
    /// PITR journal epoch: bumped whenever a snapshot adoption voids the
    /// journal (the writes those incrementals carried are adjudicated
    /// away). The incremental exporter re-checks it between its journal
    /// read and the file rename — an in-flight export whose epoch moved is
    /// discarded instead of landing a resurrectable incr file.
    pub pitr_epoch: std::sync::atomic::AtomicU64,
    /// Per-origin serialization of sequenced replication applies. Two
    /// in-flight writes from one origin (its first attempt timed out while
    /// this node waited for a client transaction, then it sent the next)
    /// could otherwise apply out of order once the transaction closes:
    /// the older row image overwrites the newer one and the position jumps
    /// to the newer seq, hiding the divergence until a restart.
    pub apply_locks: tokio::sync::Mutex<
        std::collections::HashMap<String, std::sync::Arc<tokio::sync::Mutex<()>>>,
    >,
    /// Fan-out circuit breaker: target -> (retry_after, consecutive
    /// transport failures). A peer that accepts TCP but never answers would
    /// otherwise add the full IO budget to EVERY write (the fan-out runs
    /// under write_order); backing off for a growing window keeps the write
    /// path responsive while the peer recovers or its outage shows in the
    /// sync log. Application errors (peer rejected the statement) never trip
    /// it — the peer is alive and answering.
    pub peer_backoff:
        tokio::sync::Mutex<std::collections::HashMap<String, (std::time::Instant, u32)>>,
    /// Live connection budget (resource control). Acquired per accepted
    /// connection, released on close; None = unlimited.
    pub conn_slots: Option<std::sync::Arc<tokio::sync::Semaphore>>,
    /// Client-statement wall-clock budget (see ServerConfig).
    pub statement_timeout: Option<std::time::Duration>,
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
    /// Locking uses the engine's poison policy (`into_inner`): a panic in
    /// one holder must not cascade into every later statement panicking on
    /// a poisoned mutex.
    pub tx_owner: std::sync::Mutex<Option<u64>>,
    /// Live connections, keyed by the same per-connection id the pubsub
    /// registry uses: the activity monitor (REQ_SESSIONS) reads it and
    /// REQ_KILL flips the slot's kill flag. Entries are added at connection
    /// start and removed on close (any exit path — the cleanup runs after
    /// the frame loop, next to the pubsub deregistration).
    pub(crate) sessions: tokio::sync::Mutex<std::collections::HashMap<u64, Arc<SessionSlot>>>,
    /// Serializes write execution together with its fan-out so peers apply
    /// writes in the order this node executed them (and the
    /// buffer-vs-forward classification is race-free). Shared as an Arc so
    /// cluster-join holds can keep an `OwnedMutexGuard` past the handler.
    pub write_order: std::sync::Arc<tokio::sync::Mutex<()>>,
    /// Write-path holds taken by a peer serving REQ_SYNC (cluster-join
    /// quiesce): hold id -> (guard, joiner identity). The identity lets a
    /// joiner clear hold remnants of its own prior attempts (a REQ_HOLD
    /// answered after its response was lost freezes the peer for 60s
    /// otherwise). Dropping the guard releases the freeze.
    pub holds: tokio::sync::Mutex<
        std::collections::HashMap<u64, (tokio::sync::OwnedMutexGuard<()>, String)>,
    >,
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
    /// Outbound TLS dialer (DOCSQL_TLS_CONNECT=1): shared by every peer
    /// leg (fan-out, catch-up, join/hold, backup) and by the web console's
    /// node dials via the re-exported `tls::dial_protocol`. None = plain
    /// TCP, the historical behavior.
    pub tls_out: Option<tokio_rustls::TlsConnector>,
    /// Whether the listener wraps accepted connections in TLS
    /// (DOCSQL_TLS_CERT/KEY); surfaced in REQ_STATUS for fleet monitoring.
    pub tls_listener: bool,
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
    /// Remote backup copy client; None = backups stay local-only.
    pub backup_s3: Option<s3::S3Client>,
    /// Majority-visibility bookkeeping (design 004); None = feature off,
    /// the write gate is inert.
    pub quorum: Option<Arc<quorum::Quorum>>,
    /// Primary-role epoch (design 004 §4): incremented on every PROMOTE,
    /// in-memory only (a restarted node has never promoted). Status-visible
    /// so a recovered stale primary demotes itself.
    pub primary_epoch: std::sync::atomic::AtomicU64,
    /// The forwarding upstream as configured at startup: auto-PROMOTE's
    /// lost-primary trigger reads this (PROMOTE detaches the live
    /// replicate_to, the trigger must survive that).
    pub primary_addr: Option<String>,
    /// Set when a quorum demotion re-points this node at a higher-epoch
    /// primary: auto-PROMOTE stays frozen for the process lifetime after
    /// that (the trigger still watches the startup primary; unfrozen it
    /// would flap promote/demote every probe cycle). Restart unfreezes.
    pub auto_promote_frozen: std::sync::atomic::AtomicBool,
    /// DOCSQL_AUTO_PROMOTE (requires the quorum loop).
    pub auto_promote: bool,
    /// Lag-guard warning is once per lost-primary episode.
    pub promote_lag_warned: std::sync::atomic::AtomicBool,
    /// Probe cycle length for the quorum loop (milliseconds).
    pub quorum_probe_ms: u64,
    /// Arbiter mode: no data plane — status probes only.
    pub arbiter: bool,
    /// Backup shared state: in-flight flag + last attempt outcome.
    /// Never held while acquiring `write_order`/the engine (see backup.rs).
    pub backup: Mutex<backup::BackupShared>,
    /// Join-intake replays that failed to apply at drain time. Each one is
    /// a write some origin already had acknowledged; surfacing the count in
    /// REQ_STATUS keeps the divergence observable instead of burying it in
    /// stderr (digest repair on the next restart is the remedy).
    pub replay_failures: std::sync::atomic::AtomicU64,
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
    bytes: usize,
}

/// Transaction buffer bounds: an authenticated connection could otherwise
/// BEGIN and push writes forever (each buffered statement string retained
/// until COMMIT) — a cheap OOM/hold-the-writer DoS.
const MAX_TX_PENDING_WRITES: usize = 200_000;
const MAX_TX_PENDING_BYTES: usize = 64 * 1024 * 1024;

impl TxPending {
    pub fn new() -> Self {
        Self::default()
    }

    /// Room for one more buffered statement?
    pub fn check_room(&self, len: usize) -> bool {
        self.writes.len() < MAX_TX_PENDING_WRITES
            && self.bytes.saturating_add(len) <= MAX_TX_PENDING_BYTES
    }

    /// Buffer one write (the caller checked `check_room` first).
    pub fn push(&mut self, sql: String) {
        self.bytes = self.bytes.saturating_add(sql.len());
        self.writes.push(sql);
    }

    /// SAVEPOINT name: remember the buffer length to roll back to.
    pub fn mark(&mut self, name: &str) {
        self.marks.push((name.to_string(), self.writes.len()));
    }

    /// ROLLBACK TO SAVEPOINT name: drop writes past the mark (and later marks).
    /// Duplicate savepoint names resolve to the MOST RECENT one — the same
    /// `rposition` the engine uses (SQLite semantics); a `position` here
    /// truncated the buffer to the earliest duplicate and silently diverged
    /// the fan-out from what the engine actually kept.
    pub fn rollback_to(&mut self, name: &str) {
        if let Some(pos) = self.marks.iter().rposition(|(n, _)| n == name) {
            let (_, len) = self.marks[pos].clone();
            self.writes.truncate(len);
            self.marks.truncate(pos);
            self.bytes = self.writes.iter().map(|w| w.len()).sum();
        }
    }

    /// RELEASE SAVEPOINT name: forget the mark, keep the writes. Most recent
    /// duplicate wins, matching `rollback_to` and the engine.
    pub fn release(&mut self, name: &str) {
        if let Some(pos) = self.marks.iter().rposition(|(n, _)| n == name) {
            self.marks.truncate(pos + 1);
        }
    }

    /// COMMIT drain: hand over the buffered writes and reset ALL accounting
    /// (marks and byte budget). `bytes` used to survive every drain, so a
    /// server's lifetime total of buffered SQL text counted against the
    /// per-transaction cap until every explicit transaction was rejected
    /// with "transaction buffer exceeded".
    pub fn take_writes(&mut self) -> Vec<String> {
        self.marks.clear();
        self.bytes = 0;
        std::mem::take(&mut self.writes)
    }

    pub fn clear(&mut self) {
        self.writes.clear();
        self.marks.clear();
        self.bytes = 0;
    }
}

/// Flags bit 1 marks replication-internal frames (bypasses read-only).
pub const FLAG_REPLICATION: u16 = 0x0002;
/// Group-commit flush cadence in async-commit mode (the advertised ~2ms
/// loss window on power failure).
const ASYNC_COMMIT_INTERVAL_MS: u64 = 2;

/// Pre-auth frame deadline: a connection that has not completed AUTH must
/// not hold its task and read buffer open forever (slowloris). Generous
/// enough for a human at an interactive password prompt. Token-less
/// deployments (auth disabled) start connections as authenticated clients
/// and are unaffected — that mode's boundary is the network itself.
const PRE_AUTH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Concurrent PBKDF2 derivations server-wide (REQ_AUTH_USER verifications
/// and unknown-user decoy burns). Excess attempts queue on this gate
/// instead of pinning the blocking pool — a distributed password-spray
/// must not turn every login into CPU contention for everyone.
const AUTH_DERIVE_CONCURRENCY: usize = 8;

/// Join-window queue budget: acknowledged-but-not-yet-applied replication
/// writes during bootstrap. Past it, acks are REFUSED — the origin sees a
/// failed fan-out leg and convergence degrades to the documented digest →
/// snapshot path — instead of one long window pinning unbounded memory.
const MAX_SYNC_QUEUE_ENTRIES: usize = 200_000;
const MAX_SYNC_QUEUE_BYTES: usize = 256 * 1024 * 1024;

/// Prepared-statement budget per connection. The client never sends
/// REQ_CLOSE_STMT today, so the map grows for the life of a pooled
/// connection — without a cap, one connection pins unbounded server
/// memory with megabyte templates it never executes.
const MAX_PREPARED_STATEMENTS: usize = 1024;
const MAX_PREPARED_TEMPLATE_BYTES: usize = 64 * 1024;

/// One replication write acknowledged while the sync gate was open.
#[derive(Debug, Clone)]
pub struct QueuedWrite {
    /// Journal origin and seq when the write arrived sequenced
    /// (REQ_SQL_SEQ). The drain uses it to skip ops the adopted snapshot
    /// already covers and to advance the origin's position as entries
    /// apply; plain REQ_SQL re-fanouts (legacy peers) carry none.
    pub origin: Option<(String, u64)>,
    pub sql: String,
    /// Replication PUBLISH/TRIM ride the queue as writes against the
    /// `_pubsub_messages` SYSTEM table; the drain must replay them with
    /// the system-table gate lifted or every replay would be refused.
    pub allow_system_table: bool,
    /// A queued PUBLISH also owes its LOCAL subscribers a push once the
    /// replay lands (channel, ts_ms, payload) — the replay itself is a
    /// plain INSERT and would otherwise skip `notify` entirely.
    pub pubsub_notify: Option<(String, i64, String)>,
}

/// Join-intake queue state; see [ServerState::sync_queue]. Opens (closed =
/// false) on a node that starts with peers configured — fresh (join
/// bootstrap) or holding data (rejoin repair); every other node starts
/// closed and never queues.
#[derive(Default)]
pub struct SyncGate {
    /// Replication writes acknowledged but not yet applied. VecDeque: the
    /// drain is peek-front / pop-front, and a Vec's remove(0) made every
    /// replay O(n) (O(n²) over a full queue).
    pub pending: VecDeque<QueuedWrite>,
    /// Sum of `pending[*].sql.len()` — the budget feed for
    /// MAX_SYNC_QUEUE_BYTES, maintained by every mutation site.
    pub pending_bytes: usize,
    /// True once bootstrap concluded: no more queuing, direct applies.
    pub closed: bool,
    /// PUBLISH replays that owe local subscribers a push: (channel, id,
    /// ts_ms, payload), collected by the drain and flushed AFTER the
    /// write order is released (notify awaits; it must not run under the
    /// gate's engine hold).
    pub pubsub_after_drain: Vec<(String, i64, i64, String)>,
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

/// A database-user identity bound to one connection (REQ_AUTH_USER), with
/// its resolved privileges; refreshed by the connection loop whenever the
/// server-wide grants epoch moves.
pub(crate) struct UserAuth {
    name: String,
    grants: docsql_core::useradmin::UserGrants,
}

/// One live connection's operator-visible state (REQ_SESSIONS) plus its
/// kill switch (REQ_KILL). The slot is shared between the connection's
/// frame loop (which updates `info` around statement execution) and the
/// admin surface (which snapshots it / requests the kill).
pub(crate) struct SessionSlot {
    /// Kill request, as a watch channel rather than a bare flag+notify:
    /// the value survives being set between the loop's flag check and the
    /// select arm registration (a notify without a waiter would be lost
    /// and the kill silently delayed to the idle timeout).
    kill: tokio::sync::watch::Sender<bool>,
    kill_wait: tokio::sync::watch::Receiver<bool>,
    pub info: std::sync::Mutex<SessionInfo>,
}

/// Mutable per-connection snapshot fields. The std mutex is fine: every
/// critical section is a plain field write/read, never held across await.
#[derive(Clone)]
pub(crate) struct SessionInfo {
    pub peer: String,
    /// Operator-facing credential label, updated at each AUTH transition:
    /// "unauthenticated", "anonymous", "client token", "read-only token",
    /// "cluster peer", "user <name>".
    pub identity: String,
    pub connected_at: std::time::Instant,
    /// Statements executed on this connection (client traffic only —
    /// replication applies do not ride the session snapshot).
    pub statements: u64,
    /// The statement currently executing (text + start), None while idle.
    pub current: Option<(String, std::time::Instant)>,
}

impl SessionSlot {
    fn new(peer: &str) -> Self {
        let (kill, kill_wait) = tokio::sync::watch::channel(false);
        SessionSlot {
            kill,
            kill_wait,
            info: std::sync::Mutex::new(SessionInfo {
                peer: peer.to_string(),
                identity: "unauthenticated".into(),
                connected_at: std::time::Instant::now(),
                statements: 0,
                current: None,
            }),
        }
    }

    fn is_killed(&self) -> bool {
        *self.kill_wait.borrow()
    }

    /// REQ_KILL: marks the connection to close and wakes a blocked read.
    fn request_kill(&self) {
        let _ = self.kill.send(true);
    }

    /// Watcher for the read-side select (see `is_killed` for why watch).
    fn kill_watcher(&self) -> tokio::sync::watch::Receiver<bool> {
        self.kill_wait.clone()
    }

    /// Update the identity label (idempotent, cheap).
    fn set_identity(&self, identity: &str) {
        let mut info = self.info.lock().unwrap_or_else(|p| p.into_inner());
        info.identity = identity.to_string();
    }

    /// Mark a statement as running (display text capped at 256 chars).
    /// The text goes through `redact_sql` first: REQ_SESSIONS exposes it
    /// to admins, and a `CREATE USER … PASSWORD '…'` mid-flight must not
    /// leak its plaintext through the monitoring surface (same rule the
    /// audit log already enforces).
    fn start_statement(&self, sql: &str) {
        let mut info = self.info.lock().unwrap_or_else(|p| p.into_inner());
        let redacted = docsql_core::useradmin::redact_sql(sql);
        info.current = Some((
            redacted.chars().take(256).collect(),
            std::time::Instant::now(),
        ));
    }

    fn finish_statement(&self) {
        let mut info = self.info.lock().unwrap_or_else(|p| p.into_inner());
        info.current = None;
        info.statements += 1;
    }
}

/// Parse a peer address as `host:port` (IPv4/hostname, or bracketed IPv6).
/// Rejects empty/garbage: a black-hole entry would stretch every write by
/// the fan-out timeout.
fn parse_peer_addr(s: &str) -> Option<(String, u16)> {
    if s.is_empty() || s.len() > 300 || s.chars().any(|c| c.is_control() || c.is_whitespace()) {
        return None;
    }
    if let Ok(sa) = s.parse::<std::net::SocketAddr>() {
        return (sa.port() != 0).then(|| (sa.ip().to_string(), sa.port()));
    }
    let (host, port) = s.rsplit_once(':')?;
    if host.is_empty() {
        return None;
    }
    let port: u16 = port.parse().ok()?;
    if port == 0 {
        return None;
    }
    Some((host.to_string(), port))
}

/// Frame types spoken only by cluster nodes (sequenced writes, catch-up,
/// digests, join sync/hold). In the token-less compatibility mode these stay
/// answerable without credentials — the documented legacy contract — while
/// client-facing frames close once users exist.
fn is_node_frame(frame_type: u16) -> bool {
    matches!(
        frame_type,
        proto::REQ_SQL_SEQ
            | proto::REQ_CATCHUP
            | proto::REQ_DIGEST
            | proto::REQ_SYNC
            | proto::REQ_HOLD
            | proto::REQ_RELEASE
    )
}

/// Resolve when the process is asked to terminate (SIGTERM / SIGINT).
async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
    let mut int = signal(SignalKind::interrupt()).expect("SIGINT handler");
    tokio::select! {
        _ = term.recv() => {}
        _ = int.recv() => {}
    }
}

/// Post-signal connection drain: live connections get a bounded window to
/// finish (in-flight statements run to completion under the engine lock;
/// idle clients are expected to leave on their own). Bounded, never
/// indefinite — container orchestrators impose their own stop timeouts.
async fn graceful_drain(state: &Arc<ServerState>) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        if state
            .metrics
            .connections_active
            .load(std::sync::atomic::Ordering::Relaxed)
            <= 0
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

/// Long-lived client and fan-out connections must survive idle network
/// middleboxes: enable TCP keepalive (NAT/firewall timeout is the classic
/// silent killer of a database session) plus NODELAY for request latency.
fn set_tcp_keepalive(s: TcpStream) -> TcpStream {
    use socket2::{SockRef, TcpKeepalive};
    let sock: SockRef<'_> = SockRef::from(&s);
    let ka = TcpKeepalive::new().with_time(std::time::Duration::from_secs(60));
    let _ = sock.set_tcp_keepalive(&ka);
    let _ = s.set_nodelay(true);
    s
}

/// True when `peer` resolves to a loopback address on the port we listen on
/// (or is literally the listen address): forwarding there would loop back.
/// DNS goes through tokio's async resolver with a bounded budget: both live
/// call sites (handle_sync registration / handle_hold) sit on the write path
/// holding write_order, where a synchronous resolver would tax every write
/// with the DNS worst case (the resolver has no own timeout). Resolution
/// failure or timeout returns false — the same conservative answer as a
/// malformed address.
const SELF_PEER_DNS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// Does this machine own `ip`? A UDP bind to `(ip, 0)` succeeds exactly for
/// local addresses and sends no packets. Unlike name resolution this sees
/// through a wildcard listen: `lookup_host("0.0.0.0:port")` answers only
/// `0.0.0.0` itself, never the interface addresses, so the old
/// set-intersection missed a joiner advertising its own LAN IP while
/// listening on 0.0.0.0 — registering itself as its own peer and
/// double-applying every write.
fn ip_is_local(ip: std::net::IpAddr) -> bool {
    std::net::UdpSocket::bind(std::net::SocketAddr::new(ip, 0)).is_ok()
}

async fn is_self_peer(listen: &str, peer: &str) -> bool {
    if listen.eq_ignore_ascii_case(peer) {
        return true;
    }
    let Some(port) = listen
        .rsplit_once(':')
        .and_then(|(_, p)| p.parse::<u16>().ok())
    else {
        return false;
    };
    let peer_addrs =
        match tokio::time::timeout(SELF_PEER_DNS_TIMEOUT, tokio::net::lookup_host(peer)).await {
            Ok(Ok(a)) => a.collect::<Vec<_>>(),
            _ => return false,
        };
    // The peer is us when its address lives on this machine AND targets our
    // own listen port; the same IP with a DIFFERENT port is another node on
    // a shared host (dev setups deliberately run several that way).
    if peer_addrs
        .iter()
        .any(|a| a.port() == port && (a.ip().is_loopback() || ip_is_local(a.ip())))
    {
        return true;
    }
    match tokio::time::timeout(SELF_PEER_DNS_TIMEOUT, tokio::net::lookup_host(listen)).await {
        Ok(Ok(ls)) => {
            let lset: std::collections::HashSet<_> = ls.collect();
            peer_addrs.iter().any(|a| lset.contains(a))
        }
        _ => false,
    }
}

struct Conn {
    stream: tokio::io::ReadHalf<tls::BoxConn>,
    buf: Vec<u8>,
    /// Wire-byte accounting lives at the socket read: chunk size is the
    /// true network bytes (a decoded frame may span several reads).
    metrics: std::sync::Arc<metrics::Metrics>,
}

impl Conn {
    async fn read_frame(&mut self) -> std::io::Result<Option<Frame>> {
        loop {
            match Frame::decode(&self.buf) {
                Ok((f, n)) => {
                    self.buf.drain(..n);
                    // One oversized frame used to pin this connection's
                    // buffer capacity until it closed (a 64 MB frame ×
                    // max_conn connections stays resident). Give a drained
                    // buffer back to a sane floor.
                    if self.buf.is_empty() && self.buf.capacity() > 256 * 1024 {
                        self.buf.shrink_to(64 * 1024);
                    }
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
            // The cap counts the WHOLE frame: 20 bytes of header on top of
            // the 64 MiB payload cap, so a legal maximum frame split at an
            // arbitrary TCP boundary is never mistaken for an oversized one.
            if self.buf.len() > docsql_core::proto::HEADER_LEN + MAX_FRAME_BYTES {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "frame too large",
                ));
            }
            let mut chunk = [0u8; 8192];
            let n = self.stream.read(&mut chunk).await?;
            if n > 0 {
                self.metrics
                    .bytes_in_total
                    .fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
            }
            if n == 0 {
                return Ok(None);
            }
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }
}

async fn execute_read_sql(
    state: &Arc<ServerState>,
    sql: &str,
    deadline: Option<std::time::Instant>,
    user: Option<&UserAuth>,
    logged_sql: &mut Option<String>,
) -> (Frame, u64) {
    // Row-filter enforcement for the MVCC read tier (execute_sql_inner
    // does the same for every other user-carrying path; a statement goes
    // through exactly one of the two).
    let sql_owned;
    let sql = if let Some(u) = user {
        if !u.grants.admin && !u.grants.row_filters.is_empty() {
            let db = state.db.read().unwrap_or_else(|p| p.into_inner());
            match apply_row_filters(&db, sql, &u.grants.row_filters) {
                Ok(t) => {
                    drop(db);
                    sql_owned = t;
                    *logged_sql = Some(sql_owned.clone());
                    &sql_owned
                }
                Err(m) => {
                    drop(db);
                    return (outcome_frame(Err::<ExecOutcome, _>(m)), 0);
                }
            }
        } else {
            sql
        }
    } else {
        sql
    };
    // The read tier must feed the same statement counters the write tier
    // does (execute_sql_with_identity): SELECTs are the dominant statement
    // class, and a metrics object that only counts writes misstates the
    // node's error rate as zero.
    state
        .metrics
        .statements_total
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let view = {
        let db = state.db.read().unwrap_or_else(|p| p.into_inner());
        db.read_view()
    };
    view.set_statement_deadline(deadline);
    let outcome = view.execute(sql);
    // Row count for the caller's @@ROWCOUNT bookkeeping (SELECTs count the
    // rows they return, like the interpreter's rowset statements do).
    let rowcount = match &outcome {
        Ok(ExecOutcome::Rows(r)) => r.rows.len() as u64,
        Ok(ExecOutcome::Affected(n)) => *n,
        Err(_) => 0,
    };
    let resp = outcome_frame(outcome);
    if resp.frame_type == proto::RESP_ERROR {
        state
            .metrics
            .statement_errors_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    view.set_statement_deadline(None);
    drop(view); // ends the pager snapshot
    (resp, rowcount)
}

/// The MVCC read-tier eligibility test shared by the REQ_SQL and
/// REQ_EXECUTE arms: a classified read-only single SELECT (no CTE, no
/// transaction control) with no open transaction owner, passing the same
/// authorization the write tier applies. Anything else falls through to
/// the exclusive path and its error.
fn classify_read_eligible(state: &ServerState, sql: &str, user: Option<&UserAuth>) -> bool {
    if state
        .tx_owner
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .is_some()
    {
        return false;
    }
    let Ok(p) = Database::parse_classified(sql) else {
        return false;
    };
    if p.is_write || p.tx != TxControl::None {
        return false;
    }
    if !matches!(
        &p.stmt,
        AnyStmt::Sql(s) if matches!(&**s, sqlparser::ast::Statement::Query(q) if q.with.is_none())
    ) {
        return false;
    }
    // The db reference is load-bearing: view base-table expansion (a view
    // over docsql_users must refuse non-admins HERE, or the MVCC fast path
    // leaks the hashes the write tier refuses). The guard is a
    // microsecond-scale read lock.
    match user {
        Some(u) => {
            let db_ref = state.db.read().unwrap_or_else(|p| p.into_inner());
            authorize_statement(Some(&db_ref), &p.stmt, &p.tx, p.is_write, &u.grants).is_ok()
        }
        None => true, // token/开放连接
    }
}

/// Assemble the REQ_STATUS payload: everything a monitoring console needs to
/// judge node health and cluster convergence in one round trip.
/// REQ_SESSIONS payload: one row per live connection (id, peer,
/// identity, connected_ms, state, current_sql, current_ms, statements).
/// Sync snapshots only — nothing here holds a lock across an await.
fn sessions_payload(state: &ServerState) -> serde_json::Value {
    let sessions = state.sessions.try_lock();
    let Ok(sessions) = sessions else {
        // A concurrent KILL holds the map only for a flag flip; an empty
        // answer beats blocking the monitor.
        return serde_json::json!({"columns": [], "rows": []});
    };
    let tx_owner = *state.tx_owner.lock().unwrap_or_else(|p| p.into_inner());
    let now = std::time::Instant::now();
    let mut entries: Vec<(u64, SessionInfo)> = sessions
        .iter()
        .map(|(id, slot)| {
            let info = slot.info.lock().unwrap_or_else(|p| p.into_inner()).clone();
            (*id, info)
        })
        .collect();
    entries.sort_by_key(|(id, _)| *id);
    let rows: Vec<serde_json::Value> = entries
        .into_iter()
        .map(|(id, info)| {
            // A tx owner between statements is "in transaction", not idle —
            // that is the state an operator needs to see before killing.
            let state_label = if info.current.is_some() {
                "running"
            } else if tx_owner == Some(id) {
                "in transaction"
            } else {
                "idle"
            };
            let (current_sql, current_ms) = match &info.current {
                Some((sql, started)) => (
                    serde_json::Value::String(sql.clone()),
                    now.duration_since(*started).as_millis() as u64,
                ),
                None => (serde_json::Value::Null, 0),
            };
            serde_json::json!([
                id,
                info.peer,
                info.identity,
                now.duration_since(info.connected_at).as_millis() as u64,
                state_label,
                current_sql,
                current_ms,
                info.statements,
            ])
        })
        .collect();
    serde_json::json!({
        "columns": [
            "id", "peer", "identity", "connected_ms", "state",
            "current_sql", "current_ms", "statements"
        ],
        "rows": rows,
    })
}

pub async fn status_payload(state: &ServerState) -> serde_json::Value {
    let peers = state.peers.lock().await.clone();
    let replicate_to = state.replicate_to.lock().await.clone();
    // Backup state first: it must be read without the engine lock held
    // (backup.rs never nests the two, keep it that way).
    let backup = serde_json::from_slice::<serde_json::Value>(&backup::backup_payload(state))
        .unwrap_or(serde_json::json!({}));
    // journal_head/oldest may lazily create the cluster tables (&mut path):
    // take them on the WRITE tier FIRST — holding the read guard while
    // asking for the write lock would deadlock the same thread.
    let (journal_head, journal_oldest, user_state) = {
        let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
        (
            db.journal_head().unwrap_or(0),
            db.journal_oldest().unwrap_or(0),
            // A node whose only state is user accounts still holds data a
            // fresh joiner must adopt (the user tables replicate); `tables`
            // alone would classify it as empty and two such nodes would
            // never converge.
            db.any_user_exists().unwrap_or(false),
        )
    };
    // File sizes sampled BEFORE the read guard: holding the engine lock
    // across stat calls let a slow mount stall every writer for the same
    // duration on every status poll.
    let (db_bytes, wal_bytes) = (
        file_bytes(&state.db_path),
        file_bytes(&wal_path(&state.db_path)),
    );
    // Read tier: the census is a set of plain SELECTs — concurrent readers
    // share the engine (MVCC stage A) instead of blocking each other.
    let db = state.db.read().unwrap_or_else(|p| p.into_inner());
    let mut total_rows = 0u64;
    let mut user_tables = 0usize;
    for t in &db.catalog() {
        // The pubsub backing table and the catch-up journal/positions are
        // system storage, not user objects.
        if docsql_core::engine::is_internal_table(&t.name) {
            continue;
        }
        user_tables += 1;
        // Same COUNT(*) census the web console's /api/meta performs.
        total_rows += match db.execute_read(&format!(
            "SELECT COUNT(*) FROM {}",
            docsql_core::stmt::sql_quote_ident(&t.name)
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
        "arbiter": state.arbiter,
        "primary_epoch": state.primary_epoch.load(std::sync::atomic::Ordering::SeqCst),
        "quorum": {
            "enabled": state.quorum.is_some(),
            "members": state.quorum.as_ref().map(|q| q.member_count()).unwrap_or(0),
            "visible": state.quorum.as_ref().map(|q| q.visible()).unwrap_or(0),
            "fenced": state.quorum.as_ref().map(|q| q.fenced()).unwrap_or(false),
        },
        "tls_listener": state.tls_listener,
        "in_transaction": db.in_transaction(),
        "peers": peers,
        "replicate_to": replicate_to,
        "storage": {
            "page_size": db.page_size(),
            "num_pages": db.num_pages(),
            "db_bytes": db_bytes,
            "wal_bytes": wal_bytes,
        },
        "durable_lsn": db.durable_lsn(),
        "totals": {"tables": user_tables, "rows": total_rows},
        "user_state": user_state,
        "metrics": state.metrics.snapshot_json(),
        "cluster_id": state.cluster_id,
        "journal_head": journal_head,
        "journal_oldest": journal_oldest,
        "replay_failures": state
            .replay_failures
            .load(std::sync::atomic::Ordering::Relaxed),
        "backup": backup,
    })
}

fn file_bytes(p: &std::path::Path) -> u64 {
    docsql_core::file_bytes(p)
}

fn wal_path(db: &std::path::Path) -> PathBuf {
    docsql_core::pager::wal_path_for(db)
}
/// Sleep-poll until the shared engine transaction closes or `deadline`
/// passes. Nothing is held across awaits, so the owning connection's
/// COMMIT/ROLLBACK always makes progress.
pub(crate) async fn wait_engine_tx_free(state: &Arc<ServerState>, deadline: tokio::time::Instant) {
    while tokio::time::Instant::now() < deadline {
        let busy = state
            .db
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .in_transaction();
        if !busy {
            return;
        }
        tokio::time::sleep(BEGIN_QUEUE_POLL).await;
    }
}

/// Tables every authenticated connection may read (engine system tables
/// and compatibility views mirror the read-only token's reach).
fn readable_by_all(t: &str) -> bool {
    docsql_core::engine::is_system_table(t)
        // The compatibility views are addressed as
        // information_schema.tables / information_schema.columns — the
        // walker yields the qualified name, so an exact match on the bare
        // prefix never fired (and the virtual views cannot be GRANTed, so
        // custom-role users were locked out of the EF SchemaSync probes).
        || t.starts_with("information_schema.")
        || matches!(
            t,
            "sqlite_master" | "sqlite_temporal_master"
        )
        || t.starts_with('@')
}

/// Execute one SQL statement. `allow_system_table` marks the rewritten
/// `docsql_pubsub` view; system tables are otherwise blocked for anything
/// that could mutate them, while read-only queries may reference them (the
/// console's object tree shows them as a read-only 系统表 branch). `conn`
/// identifies the client connection (None for replication applies);
/// `order_held` marks the cluster-join drain path, which already holds
/// `write_order`.
///
/// `seq_pos` carries the (origin, seq) of a sequenced replication write
/// (REQ_SQL_SEQ): after the statement applies, the origin's position is
/// advanced **in the same fused write unit** — one WAL fsync lands the
/// write and its bookkeeping together, so a crash can no longer leave the
/// position lagging the applied data (the old two-commit window).
#[allow(clippy::too_many_arguments)]
async fn execute_sql(
    state: &Arc<ServerState>,
    sql: &str,
    allow_system_table: bool,
    is_replication: bool,
    conn: Option<u64>,
    order_held: bool,
    seq_pos: Option<(&str, u64)>,
    user: Option<&UserAuth>,
    stmt_deadline: Option<std::time::Instant>,
) -> Frame {
    execute_sql_with_identity(
        state,
        sql,
        allow_system_table,
        is_replication,
        conn,
        order_held,
        seq_pos,
        user,
        stmt_deadline,
        &mut None,
    )
    .await
    .0
}

/// Same statement pipeline as [`execute_sql`], plus the statement's
/// AUTOINCREMENT identity snapshot. The snapshot is taken INSIDE the engine
/// write lock right after `execute` returns — reading `Database::last_insert_id`
/// later (lock released) would race with another connection's INSERT and hand
/// this caller a foreign id (T-SQL identity is strictly per-session).
async fn execute_sql_with_identity(
    state: &Arc<ServerState>,
    sql: &str,
    allow_system_table: bool,
    is_replication: bool,
    conn: Option<u64>,
    order_held: bool,
    seq_pos: Option<(&str, u64)>,
    user: Option<&UserAuth>,
    stmt_deadline: Option<std::time::Instant>,
    logged_sql: &mut Option<String>,
) -> (Frame, Option<i64>) {
    #![allow(clippy::too_many_arguments)]

    // Statement throughput/error-rate accounting: every executor caller
    // (client REQ_SQL, replication replay, restore) funnels through here,
    // so the counters describe node-wide SQL work in one place.
    state
        .metrics
        .statements_total
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let (resp, identity) = execute_sql_inner(
        state,
        sql,
        allow_system_table,
        is_replication,
        conn,
        order_held,
        seq_pos,
        user,
        stmt_deadline,
        logged_sql,
    )
    .await;
    if resp.frame_type == proto::RESP_ERROR {
        state
            .metrics
            .statement_errors_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    (resp, identity)
}

#[allow(clippy::too_many_arguments)]
async fn execute_sql_inner(
    state: &Arc<ServerState>,
    sql: &str,
    allow_system_table: bool,
    is_replication: bool,
    conn: Option<u64>,
    order_held: bool,
    seq_pos: Option<(&str, u64)>,
    user: Option<&UserAuth>,
    stmt_deadline: Option<std::time::Instant>,
    // Set to the POST-ROW-FILTER text when a row filter rewrote the
    // statement, so the query log records what actually executed (the
    // journal/fan-out already carry the rewritten text; the log used to
    // keep the user's original, hiding the enforced predicate).
    logged_sql: &mut Option<String>,
) -> (Frame, Option<i64>) {
    // Row-filter enforcement BEFORE the single parse: the rewritten text is
    // what executes, is logged, journals and replays to peers (a replica
    // replaying the journal without a user identity reproduces exactly what
    // the origin executed). The read tier does the same inside
    // execute_read_sql — a statement goes through exactly one of the two.
    let sql_owned;
    let sql = if let Some(u) = user {
        if !u.grants.admin && !u.grants.row_filters.is_empty() {
            let db_ref = state.db.read().unwrap_or_else(|p| p.into_inner());
            match apply_row_filters(&db_ref, sql, &u.grants.row_filters) {
                Ok(t) => {
                    drop(db_ref);
                    sql_owned = t;
                    *logged_sql = Some(sql_owned.clone());
                    &sql_owned
                }
                Err(m) => {
                    drop(db_ref);
                    return (Frame::new(proto::RESP_ERROR, err_payload(&m)), None);
                }
            }
        } else {
            sql
        }
    } else {
        sql
    };
    // One parse for the whole round-trip: the AST executes at the bottom,
    // the classification routes the request here (parse errors surface with
    // the same message `execute` would have produced).
    let mut parsed = Some(match Database::parse_classified(sql) {
        Ok(p) => p,
        Err(e) => {
            return (
                Frame::new(proto::RESP_ERROR, err_payload(&e.to_string())),
                None,
            )
        }
    });
    let p = parsed.as_ref().expect("parsed just above");
    let is_write = p.is_write;
    let tx_kind = p.tx.clone();
    let is_user_admin_stmt = matches!(p.stmt, AnyStmt::UserAdmin(_));
    let user_count_changed = matches!(
        p.stmt,
        AnyStmt::UserAdmin(docsql_core::useradmin::UserAdminStmt::CreateUser { .. })
            | AnyStmt::UserAdmin(docsql_core::useradmin::UserAdminStmt::DropUser { .. })
    );
    // System tables are shielded from anything that could mutate them.
    // The check runs on the classified AST twice over: write-vs-read comes
    // from the classifier, and the table comparison uses the statement's
    // write TARGETS (`stmt_write_targets`) — not a substring scan, which
    // used to reject user writes that merely mentioned a system table in a
    // literal or comment. Read-only queries go through to the console's
    // read-only 系统表 branch.
    if !allow_system_table && is_write {
        let targets = match &p.stmt {
            AnyStmt::Sql(stmt) => Database::stmt_write_targets(stmt),
            // The user-management family IS the sanctioned path into the
            // reserved user/role tables; what the gate stops is plain DML.
            AnyStmt::UserAdmin(_) => vec![],
            // A refresh targets a user materialized view (never a system
            // table — CREATE refuses those names).
            AnyStmt::RefreshMatview(_) => vec![],
        };
        // The restore path (conn None, not replication) replays dumps that
        // drop and rebuild the reserved user tables wholesale.
        let internal_ok = conn.is_none() && !is_replication;
        let hit = targets.iter().find(|t| {
            docsql_core::engine::is_system_table(t)
                || (docsql_core::engine::is_internal_table(t) && !internal_ok)
        });
        if let Some(target) = hit {
            return (
                Frame::new(
                    proto::RESP_ERROR,
                    err_payload(&format!(
                        "system table {target} is internal to the engine and cannot be modified; \
                         SELECT is allowed (docsql_pubsub view / PUBSUB TRIM for _pubsub_messages)"
                    )),
                ),
                None,
            );
        }
    }
    // Per-user authorization (token/anonymous-legacy connections carry no
    // user identity and are not restricted here).
    if let Some(u) = user {
        let db_ref = state.db.read().unwrap_or_else(|p| p.into_inner());
        if let Err(denial) =
            authorize_statement(Some(&db_ref), &p.stmt, &tx_kind, is_write, &u.grants)
        {
            return (Frame::new(proto::RESP_ERROR, err_payload(&denial)), None);
        }
    }
    // Replica read-only gate (replication-internal frames pass through).
    let read_only = state.read_only.load(std::sync::atomic::Ordering::SeqCst);
    if read_only && !is_replication && is_write {
        return (
            Frame::new(
                proto::RESP_ERROR,
                err_payload("read-only replica; PROMOTE to accept writes"),
            ),
            None,
        );
    }
    // Majority write fence (design 004 §2.2): a quorum-lost node is
    // fenced exactly like a read-only replica — client writes refuse,
    // replication-internal frames and reads pass.
    if !is_replication && is_write {
        if let Some(denial) = quorum_write_denial(state) {
            return (Frame::new(proto::RESP_ERROR, err_payload(&denial)), None);
        }
    }
    // The single global transaction belongs to the connection that opened
    // it: COMMIT/ROLLBACK/SAVEPOINT from anyone else would roll back or
    // flush another connection's work.
    if !is_replication && conn.is_some() {
        let foreign_control = !matches!(tx_kind, TxControl::None | TxControl::Begin)
            && state
                .tx_owner
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_some_and(|o| Some(o) != conn);
        if foreign_control {
            return (
                Frame::new(
                    proto::RESP_ERROR,
                    err_payload("transaction is held open by another connection"),
                ),
                None,
            );
        }
    }
    if is_replication && !matches!(tx_kind, TxControl::None) {
        // Fan-out only ever replays autocommit writes. A BEGIN arriving on a
        // replication frame would open the global transaction with no owner
        // connection to clean it up — every later replicated write would
        // then spin in the busy-wait below for the full 30s and fail.
        return (
            Frame::new(
                proto::RESP_ERROR,
                err_payload(
                    "transaction control statements are not allowed on replication connections",
                ),
            ),
            None,
        );
    }
    // A client BEGIN queues behind an open engine transaction instead of
    // erroring immediately (single-writer engine). So does every write the
    // owner does not own — merging into the open transaction would let its
    // ROLLBACK silently drop a write that was already acknowledged to the
    // client. The deadline bounds both queues.
    // Transaction buffer bound, checked BEFORE executing: an over-budget
    // transaction must not apply a write locally and then fail to buffer it
    // (peers would never see a write this node reported as applied).
    if !is_replication && is_write && conn.is_some() {
        let owned = *state.tx_owner.lock().unwrap_or_else(|p| p.into_inner()) == conn;
        if owned && !state.tx_pending.lock().await.check_room(sql.len()) {
            return (
                Frame::new(
                    proto::RESP_ERROR,
                    err_payload("transaction buffer exceeded; COMMIT or ROLLBACK first"),
                ),
                None,
            );
        }
    }
    let queues = !is_replication && matches!(tx_kind, TxControl::Begin);
    let deadline = tokio::time::Instant::now() + BEGIN_QUEUE_WAIT;
    // Journaling is unconditional for originated writes: beyond catch-up it
    // is the PITR source (point-in-time restore replays base + journal up
    // to a timestamp), so a node without peers still journals. Replicated
    // applies are excluded — foreign ops live in the ORIGIN's journal;
    // re-journaling them here would re-broadcast them to third nodes with
    // this node's origin (unbounded amplification).
    let journal_wanted = !is_replication && is_write && matches!(tx_kind, TxControl::None);
    let (outcome, in_tx, _order_guard, resolved, journal_seq, identity) =
        loop {
            // Plain reads wait out a foreign transaction too: the engine applies
            // a transaction's statements to the in-memory tables immediately, so
            // reading while another connection holds BEGIN served uncommitted
            // rows (a dirty read that a later ROLLBACK made vanish). Same-owner
            // reads must pass — they see their own writes by design.
            let plain_read = !is_write && matches!(tx_kind, TxControl::None);
            let foreign_tx = (is_write || plain_read)
                && *state.tx_owner.lock().unwrap_or_else(|p| p.into_inner()) != conn;
            if queues || foreign_tx {
                wait_engine_tx_free(state, deadline).await;
                // A plain read that waited must re-check before executing: the
                // engine applies an open transaction's statements immediately,
                // so reading past a still-open transaction serves uncommitted
                // rows a later ROLLBACK would make vanish — the dirty read this
                // wait exists to prevent. Writes and replication applies
                // re-check inside their write_order branches below; without
                // this, only the plain read fell through on timeout.
                if plain_read && !is_replication && !order_held {
                    let busy = {
                        let in_tx = state
                            .db
                            .read()
                            .unwrap_or_else(|p| p.into_inner())
                            .in_transaction();
                        in_tx && *state.tx_owner.lock().unwrap_or_else(|p| p.into_inner()) != conn
                    };
                    if busy {
                        return (
                            Frame::new(
                                proto::RESP_ERROR,
                                err_payload(
                                    "timed out waiting for the transaction on another connection",
                                ),
                            ),
                            None,
                        );
                    }
                }
            }
            let guard =
                if order_held {
                    // Caller already holds write_order (cluster-join drain, backup
                    // restore replay) and waited out any open transaction on the
                    // way to taking it — re-locking would self-deadlock.
                    None
                } else if !is_replication && (is_write || !matches!(tx_kind, TxControl::None)) {
                    let order = state.write_order.lock().await;
                    let busy = {
                        let in_tx = state
                            .db
                            .read()
                            .unwrap_or_else(|p| p.into_inner())
                            .in_transaction();
                        in_tx && *state.tx_owner.lock().unwrap_or_else(|p| p.into_inner()) != conn
                    };
                    if busy {
                        drop(order);
                        if tokio::time::Instant::now() >= deadline {
                            return (Frame::new(
                        proto::RESP_ERROR,
                        err_payload("timed out waiting for the transaction on another connection"),
                    ), None);
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
                        .read()
                        .unwrap_or_else(|p| p.into_inner())
                        .in_transaction();
                    if busy {
                        drop(order);
                        if tokio::time::Instant::now() >= deadline {
                            return (Frame::new(
                        proto::RESP_ERROR,
                        err_payload("replication write timed out waiting for the open transaction"),
                    ), None);
                        }
                        tokio::time::sleep(BEGIN_QUEUE_POLL).await;
                        continue;
                    }
                    Some(order)
                } else {
                    None
                };
            let (out, in_tx, resolved, seq, sync_failed, identity) = {
                let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
                // Same-guard re-check of the plain-read dirty-read rule: the
                // earlier check ran before this write() acquisition, and a
                // foreign BEGIN that landed in between would otherwise serve
                // uncommitted rows. Check and execute share the guard, so
                // nothing can slip in between them anymore.
                if plain_read
                    && !is_replication
                    && !order_held
                    && db.in_transaction()
                    && *state.tx_owner.lock().unwrap_or_else(|p| p.into_inner()) != conn
                {
                    return (
                        Frame::new(
                            proto::RESP_ERROR,
                            err_payload(
                                "timed out waiting for the transaction on another connection",
                            ),
                        ),
                        None,
                    );
                }
                // User/role writes are authorization state, not transactional
                // data: inside an explicit transaction the per-frame grant
                // refresh would read the UNCOMMITTED rows (revoked too early,
                // and a ROLLBACK never re-bumps the epoch — the revoked state
                // stuck until the next user-admin write). Refuse; every
                // user-management statement is autocommit. Internal replay
                // (join/restore execute_batch) bypasses this function, so
                // snapshot scripts that embed CREATE USER stay unaffected.
                if is_user_admin_stmt
                    && conn.is_some()
                    && !is_replication
                    && db.in_transaction()
                    && *state.tx_owner.lock().unwrap_or_else(|p| p.into_inner()) == conn
                {
                    return (
                        Frame::new(
                            proto::RESP_ERROR,
                            err_payload(
                                "user management statements cannot run inside a transaction",
                            ),
                        ),
                        None,
                    );
                }
                // Arm the client-statement deadline inside the engine lock —
                // it is engine-global, so it must cover exactly this statement
                // and be cleared on every path below (the guard block does).
                db.set_statement_deadline(stmt_deadline);
                // Fuse the statement's commit with its replication bookkeeping
                // into ONE WAL fsync: journal append on the writing side,
                // position update on the receiving side. Unfused, each is an
                // engine commit of its own — two fsyncs per clustered write —
                // and a crash between them leaves bookkeeping lagging the data.
                // The unit lands both atomically (all-or-nothing prefix).
                // Async-commit mode keeps the unit (bookkeeping still runs,
                // grouped): the engine's `end` is async-aware and leaves the
                // fsync to the background flusher instead of imposing one per
                // write — group-commit batching stays intact.
                let fuse =
                    !db.in_transaction() && (journal_wanted || (seq_pos.is_some() && is_write));
                let (out, resolved, seq, sync_failed, identity) = if fuse {
                    let mut unit = db.write_unit();
                    let out = match parsed.take() {
                        Some(p) => unit.execute_parsed(p),
                        None => unit.execute(sql),
                    };
                    let resolved = unit.take_resolved_sql();
                    // Sample INSIDE the lock, right after the statement (through
                    // the unit's Deref): journal bookkeeping below writes tables
                    // of its own and would clobber the statement's id.
                    let identity = unit.last_insert_id();
                    let mut seq = None;
                    if out.is_ok() {
                        // Auto-generated GUID values are random: peers cannot
                        // re-derive them, so journal the explicit-value rewrite.
                        let forward = resolved.as_deref().unwrap_or(sql);
                        if journal_wanted {
                            seq = journal_append_trimmed(&mut unit, state, forward);
                        }
                        if let Some((origin, s)) = seq_pos {
                            advance_position(&mut unit, origin, s);
                        }
                    }
                    let end = unit.end();
                    (out, resolved, seq, end.is_err(), identity)
                } else {
                    let out = match parsed.take() {
                        Some(p) => db.execute_parsed(p),
                        // Consumed by a lost BEGIN-queue race above: re-parse (the
                        // failed attempt left no state behind) and retry.
                        None => db.execute(sql),
                    };
                    let identity = db.last_insert_id();
                    let resolved = db.take_resolved_sql();
                    (out, resolved, None, false, identity)
                };
                // Capture inside the same lock: another connection must not be
                // able to open/close a transaction between execute and
                // classification.
                let in_tx = db.in_transaction();
                db.set_statement_deadline(None);
                (out, in_tx, resolved, seq, sync_failed, identity)
            };
            if sync_failed {
                // The fused unit's single fsync failed: durable state is
                // unknown, the statement must not be acknowledged.
                return (
                    Frame::new(
                        proto::RESP_ERROR,
                        err_payload("failed to make the write durable"),
                    ),
                    None,
                );
            }
            // Lost the race for the engine transaction between the wait and the
            // locks: requeue until the deadline, then let the error through.
            // Skip the requeue when THIS connection already owns the open
            // transaction (its own duplicate BEGIN): the error cannot clear,
            // so waiting would only spin out the full 30s budget.
            if queues
                && out
                    .as_ref()
                    .is_err_and(|e| e.to_string().contains("transaction already in progress"))
                && *state.tx_owner.lock().unwrap_or_else(|p| p.into_inner()) != conn
                && tokio::time::Instant::now() < deadline
            {
                drop(guard);
                tokio::time::sleep(BEGIN_QUEUE_POLL).await;
                continue;
            }
            break (out, in_tx, guard, resolved, seq, identity);
        };
    // Successful user-management statements move the grants epoch (user
    // connections re-resolve) and may flip the has-users flag that closes
    // anonymous access. Runs on every node that applies the statement —
    // including replication/restore applies — so the whole mesh converges.
    if outcome.is_ok() && is_user_admin_stmt {
        state
            .grants_epoch
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if user_count_changed {
            // any_user_exists needs &mut Database: keep this refresh on the
            // write tier.
            let has = state
                .db
                .write()
                .unwrap_or_else(|p| p.into_inner())
                .any_user_exists()
                .unwrap_or(false);
            state
                .has_users
                .store(has, std::sync::atomic::Ordering::SeqCst);
        }
    }
    // Replication timing follows engine transaction state: writes inside an
    // open transaction buffer until COMMIT (ROLLBACK discards them), so peers
    // never observe writes this node later undoes. COMMIT/EXEC drain the
    // buffer in execution order, mixing SQL and KV writes alike.
    if !is_replication {
        // Commit/rollback release the owner and buffer — UNLESS the engine
        // kept the transaction open (a failed WAL fsync on COMMIT, or a
        // failed undo replay on ROLLBACK: the engine tears down only after
        // durability/restore succeed and documents "retry is possible").
        // In that state the owner and buffered writes must survive so the
        // client can retry or the disconnect cleanup can roll back;
        // clearing them unconditionally left an open engine transaction
        // nobody owned, wedging every later write (client, replication
        // apply, PUBLISH) behind the 30s busy loop until restart. A failed
        // COMMIT on a closed transaction still clears the buffer without
        // draining (the writes were never durably committed, so peers must
        // not see them).
        if matches!(tx_kind, TxControl::Commit) {
            if in_tx {
                // Engine transaction still open: owner + tx_pending stay.
            } else {
                *state.tx_owner.lock().unwrap_or_else(|p| p.into_inner()) = None;
                if outcome.is_ok() {
                    drain_tx_pending(state).await;
                } else {
                    state.tx_pending.lock().await.clear();
                }
            }
        } else if matches!(tx_kind, TxControl::Rollback { savepoint: None }) {
            if in_tx {
                // Undo replay failed, transaction still open for retry.
            } else {
                *state.tx_owner.lock().unwrap_or_else(|p| p.into_inner()) = None;
                state.tx_pending.lock().await.clear();
            }
        }
    }
    if !is_replication && outcome.is_ok() {
        match tx_kind {
            TxControl::Begin => {
                *state.tx_owner.lock().unwrap_or_else(|p| p.into_inner()) = conn;
            }
            TxControl::Commit | TxControl::Rollback { savepoint: None } => {}
            TxControl::Rollback {
                savepoint: Some(name),
            } => state.tx_pending.lock().await.rollback_to(&name),
            TxControl::Savepoint(name) => state.tx_pending.lock().await.mark(&name),
            TxControl::Release(name) => state.tx_pending.lock().await.release(&name),
            _ if is_write => {
                // Auto-generated GUID values are random: peers cannot
                // re-derive them the way AUTOINCREMENT recomputes max+1,
                // so forward the engine's explicit-value rewrite when the
                // statement filled any. The journal seq was fused into the
                // statement's write unit above (same fsync).
                let forward = resolved.as_deref().unwrap_or(sql);
                if in_tx {
                    state.tx_pending.lock().await.push(forward.to_string());
                } else {
                    forward_sql_all(state, forward, journal_seq).await;
                }
            }
            _ => {}
        }
    }
    (outcome_frame(outcome), identity)
}

/// Peer I/O budget: a partitioned or malicious peer must not wedge client
/// writes for the OS TCP timeout, nor force huge allocations.
pub(crate) const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);
pub(crate) const IO_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
pub(crate) const RECV_CAP: usize = 64 * 1024 * 1024;

/// Read one response frame from an outbound connection.
async fn read_response_frame_with_guard(
    stream: &mut tls::BoxConn,
    wire: WireKey<'_>,
    replay: &mut crypto::ReplayGuard,
) -> std::io::Result<Frame> {
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
    let (mut f, _) = Frame::decode(&buf).map_err(std::io::Error::other)?;
    // Peer responses are sealed like any other frame; the old code handed
    // encrypted payloads to its callers (status JSON, AUTH replies), so a
    // keyed cluster could not actually talk to itself.
    match wire {
        Some((k, hello)) => {
            if f.flags & crypto::FLAG_ENCRYPTED == 0 {
                return Err(std::io::Error::other(format!(
                    "peer sent an unencrypted {} response on a keyed transport",
                    f.frame_type
                )));
            }
            replay.check(&f.payload).map_err(std::io::Error::other)?;
            f.payload = crypto::open(k, f.frame_type, f.flags, &f.payload, hello)
                .map_err(std::io::Error::other)?;
        }
        None => {
            if f.flags & crypto::FLAG_ENCRYPTED != 0 {
                return Err(std::io::Error::other(
                    "peer sent an encrypted response but no transport key is configured",
                ));
            }
        }
    }
    Ok(f)
}

/// Keyed-transport state for one OUTBOUND peer connection: the key plus
/// the server's RESP_HELLO challenge, both bound into every frame's AAD.
/// `(None, _)` is a plaintext link.
pub(crate) type WireKey<'a> = Option<(&'a crypto::TransportKey, &'a [u8; 16])>;

/// Read the server's connection challenge on a fresh keyed link. Before
/// the hello, a plaintext RESP_ERROR is a terminal connection-level
/// failure (e.g. the peer's connection-limit rejection) — surface its
/// message instead of a framing error.
async fn read_wire_hello(
    stream: &mut tls::BoxConn,
    key: Option<&crypto::TransportKey>,
) -> std::io::Result<[u8; 16]> {
    let Some(_) = key else {
        return Ok([0u8; 16]);
    };
    let mut header = [0u8; proto::HEADER_LEN];
    tokio::time::timeout(IO_TIMEOUT, stream.read_exact(&mut header)).await??;
    let len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
    if len > RECV_CAP {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "peer hello too large",
        ));
    }
    let mut payload = vec![0u8; len];
    tokio::time::timeout(IO_TIMEOUT, stream.read_exact(&mut payload)).await??;
    let mut buf = header.to_vec();
    buf.extend_from_slice(&payload);
    let (f, _) = Frame::decode(&buf).map_err(std::io::Error::other)?;
    if f.frame_type == proto::RESP_ERROR && f.flags & crypto::FLAG_ENCRYPTED == 0 {
        return Err(std::io::Error::other(format!(
            "peer refused the connection: {}",
            String::from_utf8_lossy(&f.payload)
        )));
    }
    if f.frame_type != proto::RESP_HELLO || f.payload.len() != 16 {
        return Err(std::io::Error::other(
            "keyed peer did not send its RESP_HELLO challenge",
        ));
    }
    let mut out = [0u8; 16];
    out.copy_from_slice(&f.payload);
    Ok(out)
}

/// AUTH on a fresh peer connection when the server requires a token (the
/// peer rejects everything else with "unauthorized").
async fn auth_on(
    stream: &mut tls::BoxConn,
    token: &str,
    wire: WireKey<'_>,
    replay: &mut crypto::ReplayGuard,
) -> std::io::Result<()> {
    let mut frame = Frame::new(proto::REQ_AUTH, token.as_bytes().to_vec());
    if let Some((k, hello)) = wire {
        frame.flags |= crypto::FLAG_ENCRYPTED;
        frame.payload = crypto::seal(k, frame.frame_type, frame.flags, &frame.payload, hello)
            .map_err(std::io::Error::other)?;
    }
    write_frame_on(stream, &frame).await?;
    let resp = read_response_frame_with_guard(stream, wire, replay).await?;
    if resp.frame_type == proto::RESP_ERROR {
        return Err(std::io::Error::other(format!(
            "peer rejected AUTH: {}",
            String::from_utf8_lossy(&resp.payload)
        )));
    }
    Ok(())
}

/// Connect, consume the keyed handshake and authenticate one outbound peer
/// connection. `tls` is the node's outbound dialer (DOCSQL_TLS_CONNECT):
/// None = plain TCP. The returned ReplayGuard covers the WHOLE connection
/// (AUTH response included): a fresh guard accepts any nonce for its first
/// frame, so per-response guards used to let a captured AUTH-ok frame be
/// replayed as a later request's answer — a forged fan-out ACK under
/// DOCSQL_KEY without TLS.
async fn open_peer_conn(
    target: &str,
    key: Option<&crypto::TransportKey>,
    auth: Option<&str>,
    tls: Option<&tokio_rustls::TlsConnector>,
) -> std::io::Result<(tls::BoxConn, [u8; 16], crypto::ReplayGuard)> {
    let mut stream = tls::dial_protocol(target, tls).await?;
    let hello = read_wire_hello(&mut stream, key).await?;
    let mut replay = crypto::ReplayGuard::default();
    if let Some(token) = auth {
        let wire: WireKey = key.map(|k| (k, &hello));
        auth_on(&mut stream, token, wire, &mut replay).await?;
    }
    Ok((stream, hello, replay))
}

/// Build a replication-internal frame: FLAG_REPLICATION plus transport
/// sealing when a key is configured. Nonce exhaustion (>2^32 seals from
/// this process) is a restart-the-node event: reported once as a plaintext
/// RESP_ERROR instead of a sealed frame — receivers treat the error as a
/// failed operation, never as protocol data.
fn replication_frame(frame_type: u16, payload: Vec<u8>, wire: WireKey<'_>) -> Frame {
    let mut frame = Frame::new(frame_type, payload);
    frame.flags = FLAG_REPLICATION;
    if let Some((k, hello)) = wire {
        frame.flags |= crypto::FLAG_ENCRYPTED;
        match crypto::seal(k, frame.frame_type, frame.flags, &frame.payload, hello) {
            Ok(sealed) => frame.payload = sealed,
            Err(e) => {
                static EXHAUSTION_LOGGED: std::sync::atomic::AtomicBool =
                    std::sync::atomic::AtomicBool::new(false);
                if !EXHAUSTION_LOGGED.swap(true, std::sync::atomic::Ordering::Relaxed) {
                    eprintln!("docsql-crypto: {e}");
                }
                frame.flags &= !crypto::FLAG_ENCRYPTED;
                return Frame::new(proto::RESP_ERROR, e.into_bytes());
            }
        }
    }
    frame
}

/// Encode + write + flush one frame under the IO timeout.
async fn write_frame_on(stream: &mut tls::BoxConn, frame: &Frame) -> std::io::Result<()> {
    let bytes = frame.encode().map_err(std::io::Error::other)?;
    tokio::time::timeout(IO_TIMEOUT, stream.write_all(&bytes)).await??;
    tokio::time::timeout(IO_TIMEOUT, stream.flush()).await??;
    Ok(())
}

/// Write one replication-internal frame and read its response. RESP_ERROR
/// frames surface as `Err`. Encryption is per frame; the replay guard is
/// per CONNECTION (see `open_peer_conn`).
async fn send_frame_on(
    mut stream: tls::BoxConn,
    frame_type: u16,
    payload: &[u8],
    wire: WireKey<'_>,
    replay: &mut crypto::ReplayGuard,
) -> std::io::Result<(Frame, tls::BoxConn)> {
    let frame = replication_frame(frame_type, payload.to_vec(), wire);
    write_frame_on(&mut stream, &frame).await?;
    let resp = read_response_frame_with_guard(&mut stream, wire, replay).await?;
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
    tls: Option<&tokio_rustls::TlsConnector>,
) -> std::io::Result<()> {
    match seq {
        None => {
            let payload = proto::encode_sql(sql).map_err(std::io::Error::other)?;
            forward_frame(target, proto::REQ_SQL, &payload, key, auth, tls)
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
            // `forward_frame` (via `send_frame_on`) turns RESP_ERROR into
            // Err, hiding the one case the fallback must see — probe raw.
            let sequenced =
                forward_frame_raw(target, proto::REQ_SQL_SEQ, &payload, key, auth, tls).await;
            match sequenced {
                // Fall back ONLY for the legacy-peer shape ("unsupported
                // frame"): re-resending any other rejection as a plain
                // REQ_SQL replayed rejected writes (origin-cap bypass,
                // SQL errors) and doubled the 30s replication wait when
                // the peer was busy inside a client transaction.
                Ok(resp)
                    if resp.frame_type == proto::RESP_ERROR
                        && resp.payload.as_slice() == b"unsupported frame" =>
                {
                    let payload = proto::encode_sql(sql).map_err(std::io::Error::other)?;
                    forward_frame(target, proto::REQ_SQL, &payload, key, auth, tls)
                        .await
                        .map(|_| ())
                }
                // Any other RESP_ERROR is an application-layer rejection
                // (origin cap, malformed frame, SQL failure on apply).
                // Answering Ok made the fan-out track "forward ok=true",
                // cleared the peer's backoff, and left the write silently
                // missing on that peer — the digest/snapshot repair path
                // explicitly relies on fan-out failures being visible.
                Ok(resp) if resp.frame_type == proto::RESP_ERROR => {
                    Err(std::io::Error::other(format!(
                        "peer rejected sequenced write: {}",
                        String::from_utf8_lossy(&resp.payload)
                    )))
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
pub(crate) async fn forward_frame(
    target: &str,
    frame_type: u16,
    payload: &[u8],
    key: Option<&crypto::TransportKey>,
    auth: Option<&str>,
    tls: Option<&tokio_rustls::TlsConnector>,
) -> std::io::Result<Frame> {
    let (stream, hello, mut replay) = open_peer_conn(target, key, auth, tls).await?;
    let wire: WireKey = key.map(|k| (k, &hello));
    let (resp, _stream) = send_frame_on(stream, frame_type, payload, wire, &mut replay).await?;
    Ok(resp)
}

/// One-shot replication frame that does NOT translate RESP_ERROR into an
/// error — the legacy-peer fallback in `forward_write` must inspect the
/// frame type itself to decide whether to resend as plain REQ_SQL.
async fn forward_frame_raw(
    target: &str,
    frame_type: u16,
    payload: &[u8],
    key: Option<&crypto::TransportKey>,
    auth: Option<&str>,
    tls: Option<&tokio_rustls::TlsConnector>,
) -> std::io::Result<Frame> {
    let (mut stream, hello, mut replay) = open_peer_conn(target, key, auth, tls).await?;
    let wire: WireKey = key.map(|k| (k, &hello));
    let frame = replication_frame(frame_type, payload.to_vec(), wire);
    write_frame_on(&mut stream, &frame).await?;
    read_response_frame_with_guard(&mut stream, wire, &mut replay).await
}

/// Credential fan-out presents to peers: the cluster token when
/// configured (nodes authenticate as nodes), else the client token.
pub(crate) fn fanout_auth(state: &ServerState) -> Option<&str> {
    state
        .cluster_token
        .as_deref()
        .or(state.auth_token.as_deref())
}

/// Majority-visibility write fence (design 004 §2.2): a node that lost
/// quorum refuses client writes. Same entry points as the read_only
/// replica gate; replication-internal traffic and reads pass. Returns the
/// denial text when fenced.
fn quorum_write_denial(state: &ServerState) -> Option<String> {
    let q = state.quorum.as_ref()?;
    if !q.fenced() {
        return None;
    }
    state
        .metrics
        .writes_fenced_total
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Some(format!(
        "quorum lost: {}/{} members visible; writes fenced",
        q.visible(),
        q.member_count()
    ))
}

/// Append one locally-committed write to the catch-up journal, trimming the
/// bounded window on the amortized cadence. Journaling is UNCONDITIONAL for
/// originated writes (autocommit and explicit-transaction drains alike):
/// beyond catch-up the journal is the PITR source, so a node without peers
/// still journals. Only replicated applies are excluded (foreign ops live
/// in the ORIGIN's journal; re-journaling them here would re-broadcast them
/// to third nodes with this node's origin). Runs inside the caller's fused
/// write unit when there is one, so the journal entry shares the
/// statement's fsync. Returns the journal seq, or None
/// when journaling failed — peers then cannot place the op in the origin's
/// journal and fall back to snapshot repair on divergence.
/// Byte cap for the catch-up journal (`_cluster_log`), alongside the count
/// window: big-document workloads would otherwise grow the journal without
/// bound (entries reach MAX_DOC_SIZE; the count window can be disabled
/// entirely for PITR with DOCSQL_CATCHUP_WINDOW=0). Same magnitude as the
/// pub/sub store's byte cap.
const JOURNAL_MAX_BYTES: u64 = 512 * 1024 * 1024;

fn journal_append_trimmed(
    db: &mut docsql_core::engine::Database,
    state: &ServerState,
    sql: &str,
) -> Option<u64> {
    match db.journal_append(sql) {
        Ok(seq) => {
            // Amortized window trim: keep the journal bounded so positions
            // older than the window force snapshot fallback instead of
            // unbounded growth. The byte cap runs on the same cadence and is
            // a cached counter comparison while under the cap.
            if seq % 512 == 0 {
                if state.catchup_window > 0 {
                    if let Err(e) = db.journal_trim(state.catchup_window) {
                        eprintln!("catchup journal trim failed: {e}");
                    }
                }
                if let Err(e) = db.journal_trim_bytes(JOURNAL_MAX_BYTES) {
                    eprintln!("catchup journal byte trim failed: {e}");
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

/// Advance an origin's position to `seq`, never backwards. Catch-up
/// (records the served head after applying a batch) and live REQ_SQL_SEQ
/// receipts interleave, and a late low seq must not roll the position
/// back over an op already counted as applied — the next pull would
/// replay it, error on duplicate keys, and degrade to snapshot repair.
fn advance_position(db: &mut docsql_core::engine::Database, node_id: &str, seq: u64) {
    match db.position_get(node_id) {
        Ok(Some(pos)) if pos >= seq => {}
        _ => {
            if let Err(e) = db.position_set(node_id, seq) {
                eprintln!("catchup position update failed: {e}");
            }
        }
    }
}

/// Fan-out destinations: the replication upstream (replicate_to) first,
/// then every symmetric peer (DOCSQL_PEERS).
async fn fanout_targets(state: &ServerState) -> Vec<String> {
    let mut targets: Vec<String> = Vec::new();
    if let Some(target) = state.replicate_to.lock().await.clone() {
        targets.push(target);
    }
    for peer in state.peers.lock().await.clone() {
        if !targets.contains(&peer) {
            targets.push(peer);
        }
    }
    targets
}

/// Budget for a write to a peer currently in backoff: enough for a healthy
/// peer to answer (the receiver may be waiting out a client transaction, so
/// the budget is not tiny), small enough that a black-holed peer does not
/// add the full IO timeout to every write under write_order.
const FANOUT_TRIAL_BUDGET: std::time::Duration = std::time::Duration::from_millis(500);

/// Transport-level failure? Only these trip the circuit breaker.
fn transport_failure(e: &std::io::Error) -> bool {
    use std::io::ErrorKind::*;
    matches!(
        e.kind(),
        ConnectionRefused
            | ConnectionAborted
            | ConnectionReset
            | BrokenPipe
            | UnexpectedEof
            | NotConnected
            | AddrNotAvailable
            | TimedOut
    )
}

/// Record one fan-out outcome for `target`: success clears the breaker,
/// transport failures extend the backoff (2s doubling, capped at 32s).
async fn note_fanout_failure(state: &ServerState, target: &str, e: &std::io::Error) {
    if !transport_failure(e) {
        return;
    }
    let mut backoff = state.peer_backoff.lock().await;
    let failures = backoff.get(target).map(|(_, n)| *n + 1).unwrap_or(1);
    let secs = (1u64 << failures.min(5)).min(60);
    let was_cold = !backoff.contains_key(target);
    backoff.insert(
        target.to_string(),
        (
            std::time::Instant::now() + std::time::Duration::from_secs(secs),
            failures,
        ),
    );
    if was_cold {
        eprintln!(
            "replication: {target} unreachable ({e}); backoff {secs}s (writes buffered by peers' rejoin repair)"
        );
    }
}

async fn note_fanout_success(state: &ServerState, target: &str) {
    let mut backoff = state.peer_backoff.lock().await;
    if let Some((_, n)) = backoff.remove(target) {
        eprintln!("replication: {target} recovered after {n} failure(s)");
    }
}

/// Fan one SQL write out to the replication upstream and every peer, in
/// parallel. Each attempt lands in the sync log so the console's logs page
/// shows the replication trail (target, statement, ok/error). Per-target
/// ordering is untouched: fan-out runs under `write_order`, so the next
/// write's fan-out only starts after this one finished everywhere.
pub async fn forward_sql_all(state: &Arc<ServerState>, sql: &str, seq: Option<u64>) {
    let auth = fanout_auth(state).map(String::from);
    let key = state.transport_key;
    let tls = state.tls_out.clone();
    let targets = fanout_targets(state).await;
    let now = std::time::Instant::now();
    let in_backoff: std::collections::HashSet<String> = {
        let backoff = state.peer_backoff.lock().await;
        backoff
            .iter()
            .filter(|(_, (until, _))| *until > now)
            .map(|(t, _)| t.clone())
            .collect()
    };
    let mut tasks = tokio::task::JoinSet::new();
    for target in targets {
        let sql = sql.to_string();
        let auth = auth.clone();
        let tls = tls.clone();
        let node_id = state.cluster_id.clone();
        let trial = in_backoff.contains(&target);
        tasks.spawn(async move {
            let call = forward_write(
                &target,
                &sql,
                seq,
                &node_id,
                key.as_ref(),
                auth.as_deref(),
                tls.as_ref(),
            );
            // Backed-off peers still receive every write — just with a
            // short budget, so a restarted peer recovers immediately (its
            // answer clears the breaker) while a dead one cannot stretch
            // the write path.
            let res = if trial {
                match tokio::time::timeout(FANOUT_TRIAL_BUDGET, call).await {
                    Ok(r) => r,
                    Err(_) => Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "peer in backoff did not answer within the trial budget",
                    )),
                }
            } else {
                call.await
            };
            (target, sql, res)
        });
    }
    while let Some(joined) = tasks.join_next().await {
        let (target, sql, res) = joined.expect("fan-out task cannot panic");
        match &res {
            Ok(()) => {
                note_fanout_success(state, &target).await;
                querylog::sync_event(&state.sync_log, "forward", &target, Some(&sql), true, None)
            }
            Err(e) => {
                note_fanout_failure(state, &target, e).await;
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
/// The whole batch's journal appends land in ONE fused write unit — one WAL
/// fsync for N entries instead of N — before any fan-out starts (durable
/// before peers see it, same as single writes).
///
/// Journaling is unconditional for originated writes (same rule as the
/// autocommit path): beyond catch-up the journal is the PITR source, so a
/// node without peers still journals — an explicit transaction's writes
/// must not silently vanish from point-in-time restore. Only the FAN-OUT
/// below depends on having a target.
pub async fn drain_tx_pending(state: &Arc<ServerState>) {
    let writes = {
        let mut pending = state.tx_pending.lock().await;
        pending.take_writes()
    };
    let seqs: Vec<Option<u64>> = if !writes.is_empty() {
        let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
        let mut unit = db.write_unit();
        let seqs: Vec<Option<u64>> = writes
            .iter()
            .map(|sql| journal_append_trimmed(&mut unit, state, sql))
            .collect();
        match unit.end() {
            Ok(()) => seqs,
            Err(e) => {
                // The journal entries may never have become durable — the
                // assigned seqs could make a peer adopt a position over
                // entries that vanish on crash (catch-up's continuity audit
                // would bounce it to a snapshot, but the plain fan-out must
                // not carry the seq at all). Forward unsequenced; the data
                // writes themselves were already committed by COMMIT.
                eprintln!("transaction drain journal sync failed: {e}");
                vec![None; writes.len()]
            }
        }
    } else {
        Vec::new()
    };
    let has_target =
        !state.peers.lock().await.is_empty() || state.replicate_to.lock().await.is_some();
    if has_target {
        for (sql, seq) in writes
            .iter()
            .zip(seqs.iter().chain(std::iter::repeat(&None)))
        {
            forward_sql_all(state, sql, *seq).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Pub/sub frames.
// ---------------------------------------------------------------------------

/// Acquire the write path (write_order) once no explicit transaction is
/// open. Protocol-level writes (PUBLISH, TRIM) must not join a client
/// transaction — the client's ROLLBACK would undo them — so they queue
/// behind it like a BEGIN, bounded by the same deadline. With write_order
/// held, no new transaction can open (BEGIN takes write_order too), so
/// callers may lock the engine directly afterwards.
/// [`lock_engine_for_write`] on a replication budget: a restore replays a
/// whole backup under the write order (minutes), and a replication
/// PUBLISH/TRIM dropped on the single 30s window would fork
/// `_pubsub_messages` permanently — the system table has no digest,
/// snapshot or journal repair. Wait far longer instead; the connection
/// idle timeout still bounds the total if the operator wants it dead.
async fn lock_engine_for_write_replication(
    state: &Arc<ServerState>,
) -> Option<tokio::sync::MutexGuard<'_, ()>> {
    const REPPLICATION_LOCK_ATTEMPTS: usize = 20; // 20 × 30s = 10 minutes
    for attempt in 0..REPPLICATION_LOCK_ATTEMPTS {
        if let Some(order) = lock_engine_for_write(state).await {
            return Some(order);
        }
        if attempt == 0 || attempt.is_multiple_of(10) {
            eprintln!(
                "pubsub: replication frame still waiting for the write order \
                 (attempt {attempt}/{REPPLICATION_LOCK_ATTEMPTS}); dropping it would \
                 permanently fork the message store"
            );
        }
    }
    None
}

async fn lock_engine_for_write(
    state: &Arc<ServerState>,
) -> Option<tokio::sync::MutexGuard<'_, ()>> {
    let deadline = tokio::time::Instant::now() + BEGIN_QUEUE_WAIT;
    loop {
        wait_engine_tx_free(state, deadline).await;
        let order = state.write_order.lock().await;
        let in_tx = state
            .db
            .read()
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
async fn handle_publish(state: &Arc<ServerState>, frame: &Frame, is_replication: bool) -> Frame {
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
    // Same fence shape as `execute_sql_inner`: replication-internal PUBLISH
    // passes (the message was already accepted by a majority-side node;
    // rejecting it here would permanently fork `_pubsub_messages`, which no
    // digest/snapshot/journal ever repairs).
    if !is_replication {
        if let Some(denial) = quorum_write_denial(state) {
            return Frame::new(proto::RESP_ERROR, err_payload(&denial));
        }
    }
    if is_replication {
        // Gate window (bootstrap/repair snapshot replay): queue the
        // equivalent INSERT like any replicated SQL write instead of
        // racing the replay for the write order. The drain replays it
        // after adoption. NOTE: the autoincrement id only matches the
        // origin on nodes that were in sync when it was assigned — a
        // joiner (empty system tables) or a repaired node assigns its own
        // local id, so message ids are node-local, not cluster-global;
        // retention self-heals on the next publish/trim. The replay path lifts the system-table
        // gate via the queued write's flag.
        let ts = now_ms() as i64;
        let sql = format!(
            "INSERT INTO {} (channel, ts_ms, payload) VALUES ({}, {}, {})",
            pubsub::PUBSUB_TABLE,
            docsql_core::stmt::sql_string_literal(channel),
            ts,
            docsql_core::stmt::sql_string_literal(payload),
        );
        // The pubsub-aware enqueue also records the notify triple: local
        // subscribers are pushed after the drain replays the INSERT, so a
        // live subscriber during the gate window loses nothing.
        if let Some(f) =
            gate_enqueue_pubsub(state, &sql, (channel.to_string(), ts, payload.to_string())).await
        {
            return f;
        }
    }
    let _order_guard = if is_replication {
        lock_engine_for_write_replication(state).await
    } else {
        lock_engine_for_write(state).await
    };
    let Some(_order) = _order_guard else {
        return Frame::new(
            proto::RESP_ERROR,
            err_payload("publish timed out waiting for the open transaction"),
        );
    };
    let ts = now_ms() as i64;
    // Block scope so the engine guard drops before the awaits below (the
    // connection future must stay Send).
    let id = {
        let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
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
    let count = match inner.register(conn, kind, &name, tx.clone()) {
        Ok(c) => c,
        Err(e) => {
            drop(inner);
            let _ = tx.send(err(e)).await;
            return;
        }
    };
    let mut watermark = {
        let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
        pubsub::query_max_id(&mut db)
    };
    // Confirmation and replay use try_send: a subscriber that stopped
    // draining its socket must not park this task (each PUBLISH needs the
    // registry lock under write_order). Losing frames to a slow subscriber
    // is the documented at-least-once contract — delivery resumes by
    // re-subscribing from the last id.
    let _ = tx.try_send(Frame::new(
        proto::RESP_AFFECTED,
        count.to_le_bytes().to_vec(),
    ));
    // skip-through tracks replay progress, not the raw watermark: only
    // messages the subscriber actually received are suppressed from the
    // live stream, so an interrupted replay leaves a resumable gap.
    //
    // The replay itself runs WITHOUT the registry lock (the old code held
    // it end to end, so a `from earliest` backlog stalled every publish —
    // and every write behind write_order — for the whole replay). The
    // handoff correctness:
    //
    // The subscription registers with `skip_through = i64::MAX` and KEEPS it
    // for the whole catch-up — the replay is the only delivery path, so no
    // publish/notify interleaving can deliver a message twice. Each round
    // samples the next bound; chunks never scan past it. Exiting samples
    // the watermark and arms the filter at it INSIDE one registry lock (the
    // same lock `notify` takes): a message committed before that sample is
    // ≤ it and was already replayed (cursor ≥ sample), a message committed
    // after it sees a filter at ≤ itself and flows live — either way
    // exactly once. (Arming per round at the replayed progress — the
    // previous scheme — left (progress, next watermark] delivered live AND
    // replayed: a systematic duplicate for every publish mid-backlog.)
    drop(inner);
    // Chunked replay: each window is one short engine read sized by
    // [`pubsub::ReplayWindow`] (row cap 512, byte budget ~1 MB), so memory
    // stays proportional to a window instead of the whole history.
    let mut progress = after_id.unwrap_or(watermark);
    let mut cursor = progress;
    let mut window = pubsub::ReplayWindow::new();
    let mut stalled = false;
    loop {
        let mut exhausted = false;
        while cursor < watermark {
            let chunk = {
                let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
                match pubsub::query_history_chunk(&mut db, cursor, watermark, window.limit()) {
                    Ok(c) => c,
                    // An engine read failure must not masquerade as "ids
                    // trimmed away": that would leave the subscriber a
                    // permanent hole between progress and watermark with
                    // nothing but this log line to show for it. Treat it
                    // like a stall — the client can re-subscribe from its
                    // last id (at-least-once).
                    Err(e) => {
                        eprintln!("docsql-pubsub: replay read failed at id {cursor}: {e}");
                        stalled = true;
                        break;
                    }
                }
            };
            if chunk.is_empty() {
                // (cursor, watermark] holds no replayable rows (ids trimmed
                // away while this subscriber was reconnecting): stop
                // scanning this range, nothing here can be delivered.
                exhausted = true;
                break;
            }
            let rows = chunk.len();
            let chunk_bytes = chunk
                .iter()
                .map(|m| m.channel.len() + m.payload.len())
                .sum();
            let chunk_max_row = chunk
                .iter()
                .map(|m| m.channel.len() + m.payload.len())
                .max()
                .unwrap_or(0);
            cursor = chunk.last().map(|m| m.id).unwrap_or(cursor);
            for m in &chunk {
                let hit = match kind {
                    pubsub::SubKind::Channel => m.channel == name,
                    pubsub::SubKind::Pattern => pubsub::pattern_matches(&name, &m.channel),
                };
                if !hit {
                    continue;
                }
                let pattern = (kind == pubsub::SubKind::Pattern).then_some(name.as_str());
                let f = pubsub::push_frame(pattern, &m.channel, m.id, m.ts, &m.payload);
                // Bounded send, not try_send: a live consumer drains the
                // writer channel in microseconds, but blasting a whole chunk
                // through a depth-32 channel in a hot loop outruns the writer
                // task's scheduling — one spurious `Full` used to kill the
                // whole catch-up after ~32 frames and force the client into
                // re-subscribe grinding. Give the writer a real chance; only
                // a consumer that frees no capacity within the budget (slow
                // or gone) ends the replay early.
                let sent =
                    match tokio::time::timeout(std::time::Duration::from_millis(500), tx.send(f))
                        .await
                    {
                        Ok(Ok(())) => true,
                        Ok(Err(_)) | Err(_) => false,
                    };
                if !sent {
                    // Connection gone or not draining: stop replaying — the
                    // delivered prefix stays armed, the rest is resumable by
                    // id. Live pushes above `progress` still flow.
                    stalled = true;
                    break;
                }
                progress = m.id;
            }
            window.advance(rows, chunk_bytes, chunk_max_row);
            if stalled || cursor >= watermark {
                break;
            }
        }
        if stalled || exhausted {
            // Keep live delivery open above the last delivered id; the
            // unreplayed range stays resumable by re-subscribing from that
            // id (documented at-least-once recovery).
            let mut inner = state.pubsub.lock().await;
            inner.arm_filter(conn, kind, &name, progress);
            drop(inner);
            break;
        }
        // Catch-up continues: sample the next bound. No filter arm here —
        // the registration-time full suppression stays on for the whole
        // catch-up, so live delivery cannot race this sample; only the exit
        // below arms, and only under the registry lock.
        watermark = {
            let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
            pubsub::query_max_id(&mut db)
        };
        if cursor >= watermark {
            // Catch-up complete: everything ≤ the sampled watermark was
            // replayed. The SAMPLE must happen inside the registry lock
            // (registry → engine, the same nesting registration uses):
            // sampling before taking the lock leaves a window where a
            // message commits after the sample but before the arm — its
            // notify is dropped by the still-max skip_through and the
            // replay loop has already exited, losing it for good. With the
            // sample inside the lock, notify either runs before us (its
            // message id ≤ the sample, so it was replayed — cursor already
            // ≥ it) or after the arm (id > watermark, flows live).
            let mut inner = state.pubsub.lock().await;
            let sampled = {
                let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
                pubsub::query_max_id(&mut db)
            };
            if cursor >= sampled {
                inner.arm_filter(conn, kind, &name, sampled);
                drop(inner);
                break;
            }
            watermark = sampled;
            continue;
        }
    }
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
/// Identity suffix for audit trail entries of non-SQL operations
/// (PUBLISH/TRIM/PROMOTE/backup): token connections are identified by the
/// `peer` field the trail already carries; user logins by their name.
fn audit_identity(user: Option<&UserAuth>) -> String {
    match user {
        Some(u) => format!(" by user={}", u.name),
        None => String::new(),
    }
}

async fn handle_pubsub_cmd(
    state: &Arc<ServerState>,
    frame: &Frame,
    user: Option<&UserAuth>,
    peer: &str,
    is_replication: bool,
) -> Frame {
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
            rows_frame(serde_json::json!(["channel"]), rows)
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
            rows_frame(serde_json::json!(["channel", "subscribers"]), rows)
        }
        Some("numpat") => {
            let n = state.pubsub.lock().await.numpat();
            rows_frame(
                serde_json::json!(["patterns"]),
                vec![serde_json::json!([n])],
            )
        }
        Some("trim") => {
            // TRIM deletes persisted messages: readwrite or admin.
            if user.is_some_and(|u| !u.grants.admin && !u.grants.readwrite) {
                return Frame::new(
                    proto::RESP_ERROR,
                    err_payload("PUBSUB TRIM requires the readwrite or admin role"),
                );
            }
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
            // Replication TRIM passes the quorum fence (same shape as the
            // SQL and PUBLISH gates): a fenced minority node refusing it
            // would fork retention state with no repair path.
            if !is_replication {
                if let Some(denial) = quorum_write_denial(state) {
                    return Frame::new(proto::RESP_ERROR, err_payload(&denial));
                }
            }
            if is_replication {
                // Gate window (bootstrap/repair): queue the equivalent
                // DELETE. The retention threshold is computed from a READ
                // (the engine serves reads while the snapshot replays);
                // queued INSERTs that landed earlier carry lower ids and
                // stay subject to the trim, later ones survive it — the
                // same order store_trim would produce under the lock.
                let threshold = {
                    let db = state.db.read().unwrap_or_else(|p| p.into_inner());
                    let sel = format!(
                        "SELECT id FROM {} WHERE channel = {} ORDER BY id DESC LIMIT {keep}",
                        pubsub::PUBSUB_TABLE,
                        docsql_core::stmt::sql_string_literal(channel),
                    );
                    match db.execute_read(&sel) {
                        Ok(docsql_core::engine::ExecOutcome::Rows(r)) => r
                            .rows
                            .last()
                            .and_then(|row| row.first().and_then(|v| v.as_i64())),
                        _ => None,
                    }
                };
                match threshold {
                    None => {
                        // Channel has no rows to trim (the local store may
                        // lag the origin's); nothing to queue.
                        return Frame::new(proto::RESP_AFFECTED, 0u64.to_le_bytes().to_vec());
                    }
                    Some(t) => {
                        let sql = format!(
                            "DELETE FROM {} WHERE channel = {} AND id < {t}",
                            pubsub::PUBSUB_TABLE,
                            docsql_core::stmt::sql_string_literal(channel),
                        );
                        if let Some(f) = gate_enqueue(state, None, &sql, true).await {
                            return f;
                        }
                    }
                }
            }
            let _order_guard = if is_replication {
                lock_engine_for_write_replication(state).await
            } else {
                lock_engine_for_write(state).await
            };
            let Some(_order) = _order_guard else {
                return Frame::new(
                    proto::RESP_ERROR,
                    err_payload("pubsub trim timed out waiting for the open transaction"),
                );
            };
            let deleted = {
                let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
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
            // TRIM destroys the only persisted copy of the deleted range:
            // the trail must say who trimmed what.
            querylog::record(
                state,
                peer,
                &format!(
                    "PUBSUB TRIM {channel} KEEP {keep} (deleted {total}){}",
                    audit_identity(user)
                ),
                0.0,
                &Frame::new(proto::RESP_AFFECTED, total.to_le_bytes().to_vec()),
                is_replication,
            );
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
    let tls = state.tls_out.clone();
    let targets = fanout_targets(state).await;
    // Same circuit breaker as the SQL fan-out: a partitioned peer must not
    // stretch every PUBLISH by the full connect/IO timeout (PUBLISH holds
    // write_order for the whole call). Backed-off peers still receive every
    // frame — just on the short trial budget.
    let now = std::time::Instant::now();
    let in_backoff: std::collections::HashSet<String> = {
        let backoff = state.peer_backoff.lock().await;
        backoff
            .iter()
            .filter(|(_, (until, _))| *until > now)
            .map(|(t, _)| t.clone())
            .collect()
    };
    let mut tasks = tokio::task::JoinSet::new();
    for target in targets {
        let payload = payload.to_vec();
        let auth = auth.clone();
        let tls = tls.clone();
        let trial = in_backoff.contains(&target);
        tasks.spawn(async move {
            let call = forward_frame(
                &target,
                frame_type,
                &payload,
                key.as_ref(),
                auth.as_deref(),
                tls.as_ref(),
            );
            let res = if trial {
                match tokio::time::timeout(FANOUT_TRIAL_BUDGET, call).await {
                    Ok(r) => r,
                    Err(_) => Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "peer in backoff did not answer within the trial budget",
                    )),
                }
            } else {
                call.await
            };
            (target, res)
        });
    }
    let mut out = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        let (target, res) = joined.expect("fan-out task cannot panic");
        match res {
            Ok(f) => {
                note_fanout_success(state, &target).await;
                querylog::sync_event(&state.sync_log, event, &target, None, true, None);
                out.push(f);
            }
            Err(e) => {
                note_fanout_failure(state, &target, &e).await;
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
/// Dump chunks are reassembled by the joiner, so the split only has to fall
/// on a UTF-8 char boundary; the bound keeps frames well under the 64 MB cap.
pub(crate) const SYNC_CHUNK_BYTES: usize = 4 * 1024 * 1024;

#[cfg(test)]
mod security_tests {
    // ---- column-restricted grants: authorize_statement enforcement ----

    fn col_grants_db() -> docsql_core::engine::Database {
        let mut db = docsql_core::engine::Database::in_memory().unwrap();
        for sql in [
            "CREATE TABLE sales (id INT, region TEXT, amount INT)",
            "INSERT INTO sales VALUES (1, 'east', 10), (2, 'west', 20), (3, 'east', 30)",
        ] {
            db.execute(sql).unwrap();
        }
        db
    }

    fn grants_with(
        admin: bool,
        readonly: bool,
        cols: &[(&str, &[&str])],
        filters: &[(&str, &str)],
    ) -> docsql_core::useradmin::UserGrants {
        let mut g = docsql_core::useradmin::UserGrants {
            admin,
            readonly,
            ..Default::default()
        };
        for (t, list) in cols {
            g.table_cols.insert(
                (*t).to_string(),
                list.iter().map(|c| c.to_string()).collect(),
            );
            g.table_privs
                .insert((*t).to_string(), docsql_core::useradmin::PRIV_SELECT);
        }
        for (t, pred) in filters {
            g.row_filters
                .entry((*t).to_string())
                .or_default()
                .push(pred.to_string());
        }
        g
    }

    fn auth(
        db: &docsql_core::engine::Database,
        sql: &str,
        g: &docsql_core::useradmin::UserGrants,
    ) -> Result<(), String> {
        let p = Database::parse_classified(sql).unwrap();
        authorize_statement(Some(db), &p.stmt, &p.tx, p.is_write, g)
    }

    #[test]
    fn tx_pending_drain_resets_byte_budget() {
        // Regression: the COMMIT drain took the writes but never reset the
        // byte accounting, so the server's lifetime total of buffered SQL
        // text counted against the per-transaction cap — after 64 MiB of
        // cumulative explicit-transaction traffic every BEGIN..COMMIT was
        // rejected with "transaction buffer exceeded".
        let mut p = TxPending::new();
        let chunk = "x".repeat(MAX_TX_PENDING_BYTES / 4);
        for _ in 0..3 {
            for _ in 0..4 {
                assert!(p.check_room(chunk.len()));
                p.push(chunk.clone());
            }
            assert!(!p.check_room(chunk.len()), "cap reached");
            let drained = p.take_writes();
            assert_eq!(drained.len(), 4);
            assert!(p.check_room(chunk.len()), "budget must reset on drain");
        }
    }

    #[test]
    fn column_restricted_grant_authorizes_only_attributable_refs() {
        let db = col_grants_db();
        let g = grants_with(false, false, &[("sales", &["id", "amount"])], &[]);
        // Granted columns pass.
        auth(&db, "SELECT id, amount FROM sales", &g).unwrap();
        auth(&db, "SELECT amount FROM sales WHERE id = 1", &g).unwrap();
        auth(&db, "SELECT SUM(amount) FROM sales GROUP BY id", &g).unwrap();
        // A restricted column refuses.
        let e = auth(&db, "SELECT region FROM sales", &g).unwrap_err();
        assert!(e.contains("not granted"), "{e}");
        // Wildcards are unattributable — refuse (fail-closed).
        let e = auth(&db, "SELECT * FROM sales", &g).unwrap_err();
        assert!(e.contains("cannot be authorized"), "{e}");
        // Joins / subqueries / other tables refuse.
        let e = auth(&db, "SELECT id FROM sales JOIN sales ON 1 = 1", &g).unwrap_err();
        assert!(
            e.contains("cannot be authorized") || e.contains("single-table"),
            "{e}"
        );
        let e = auth(
            &db,
            "SELECT id FROM sales WHERE region IN (SELECT region FROM sales)",
            &g,
        )
        .unwrap_err();
        assert!(e.contains("cannot be authorized"), "{e}");
        // COUNT(*) reads every column: refused as an unattributable shape.
        let e = auth(&db, "SELECT COUNT(*) FROM sales", &g).unwrap_err();
        assert!(e.contains("cannot be authorized"), "{e}");
    }

    #[test]
    fn column_restricted_grant_multi_table_and_explain_refuse() {
        let mut db = col_grants_db();
        db.execute("CREATE TABLE other (id INT)").unwrap();
        let g = grants_with(false, false, &[("sales", &["id"])], &[]);
        // EXPLAIN of a restricted table: unattributable shape.
        let e = auth(&db, "EXPLAIN SELECT id FROM sales", &g).unwrap_err();
        assert!(e.contains("cannot be authorized"), "{e}");
        // Writes against a restricted-SELECT table still go through the
        // DML path (no SELECT involvement).
        let p = Database::parse_classified("INSERT INTO sales VALUES (9, 'x', 9)").unwrap();
        let mut gg = grants_with(false, false, &[("sales", &["id"])], &[]);
        gg.table_privs.insert("sales".into(), 0b1111);
        assert!(authorize_statement(Some(&db), &p.stmt, &p.tx, p.is_write, &gg).is_ok());
        let _ = g;
    }

    #[test]
    fn blanket_grants_void_column_restrictions() {
        let db = col_grants_db();
        // readonly role: resolve_grants returns no restriction maps at all
        // (the fixture mirrors that — no cols for the blanket role).
        let g = grants_with(false, true, &[], &[]);
        auth(&db, "SELECT * FROM sales", &g).unwrap();
        // A plain (unrestricted) table grant does the same via resolve —
        // represented here by an empty cols map with the priv bit set.
        let mut g2 = docsql_core::useradmin::UserGrants {
            name: "u".into(),
            admin: false,
            readonly: false,
            readwrite: false,
            table_privs: {
                let mut m = std::collections::BTreeMap::new();
                m.insert("sales".to_string(), docsql_core::useradmin::PRIV_SELECT);
                m
            },
            table_cols: Default::default(),
            row_filters: Default::default(),
        };
        auth(&db, "SELECT * FROM sales", &g2).unwrap();
        let _ = &mut g2;
    }

    // ---- row filters: apply_row_filters rewrite + enforcement ----

    fn filters_db() -> docsql_core::engine::Database {
        let mut db = docsql_core::engine::Database::in_memory().unwrap();
        for sql in [
            "CREATE TABLE sales (id INT, region TEXT, amount INT)",
            "INSERT INTO sales VALUES (1, 'east', 10), (2, 'west', 20), (3, 'east', 30)",
        ] {
            db.execute(sql).unwrap();
        }
        db
    }

    fn filtered(text: &str) -> String {
        let db = filters_db();
        let mut m = std::collections::BTreeMap::new();
        m.insert("sales".to_string(), vec!["region = 'east'".to_string()]);
        apply_row_filters(&db, text, &m).unwrap()
    }

    fn filtered_rows(text: &str) -> Vec<Vec<docsql_core::value::Value>> {
        let db = filters_db();
        let rewritten = {
            let mut m = std::collections::BTreeMap::new();
            m.insert("sales".to_string(), vec!["region = 'east'".to_string()]);
            apply_row_filters(&db, text, &m).unwrap()
        };
        match db.execute_read(&rewritten).unwrap() {
            docsql_core::engine::ExecOutcome::Rows(r) => r.rows,
            other => panic!("unexpected outcome {other:?}"),
        }
    }

    #[test]
    fn row_filter_rewrites_select_and_counts() {
        let out = filtered("SELECT id FROM sales ORDER BY id");
        assert!(out.to_lowercase().contains("region = 'east'"), "{out}");
        // Semantics through the engine: only east rows survive — including
        // the COUNT(*) fast path (the predicate rides in the WHERE).
        assert_eq!(filtered_rows("SELECT COUNT(*) FROM sales").len(), 1);
        let rows = filtered_rows("SELECT id FROM sales ORDER BY id");
        assert_eq!(
            rows,
            vec![
                vec![docsql_core::value::Value::Int(1)],
                vec![docsql_core::value::Value::Int(3)]
            ]
        );
    }

    #[test]
    fn row_filter_rewrites_update_delete_and_insert_source() {
        let out = filtered("UPDATE sales SET amount = 0 WHERE id = 2");
        assert!(out.to_lowercase().contains("region = 'east'"), "{out}");
        assert!(out.to_lowercase().contains("id = 2"), "{out}");
        let out = filtered("DELETE FROM sales");
        assert!(out.to_lowercase().contains("region = 'east'"), "{out}");
        let out = filtered("INSERT INTO audit SELECT id FROM sales");
        assert!(out.to_lowercase().contains("region = 'east'"), "{out}");
    }

    /// 第七轮审查回归:注入谓词必须加括号——sqlparser Display 不给
    /// BinaryOp 加优先级括号,`用户OR树 AND 过滤器` 重解析成
    /// `left OR (right AND filter)`,west 行从 OR 侧漏过过滤器。
    #[test]
    fn row_filter_parenthesizes_against_or_bypass() {
        // sales: (1,east),(2,west),(3,east). The user's OR spans both
        // regions; only east rows may come back (id=2 is west and must
        // not leak through the OR side).
        let rows = filtered_rows("SELECT id FROM sales WHERE id = 1 OR id = 2 ORDER BY id");
        assert_eq!(
            rows,
            vec![vec![docsql_core::value::Value::Int(1)]],
            "{rows:?}"
        );
        // The rewritten text carries the parenthesized filter.
        let out = filtered("SELECT id FROM sales WHERE id = 1 OR id = 2");
        assert!(
            out.contains("(region = 'east')") || out.contains("(region='east')"),
            "{out}"
        );
    }

    /// 第七轮审查回归:DELETE ... USING 与 UPDATE ... FROM 同构,合并像上
    /// 裸列谓词可绑 USING 表同名列短路过滤器——拒绝(fail-closed)。
    #[test]
    fn row_filter_refuses_delete_using() {
        let mut db = filters_db();
        db.execute("CREATE TABLE other (id INT, region TEXT)")
            .unwrap();
        let mut m = std::collections::BTreeMap::new();
        m.insert("sales".to_string(), vec!["region = 'east'".to_string()]);
        assert!(apply_row_filters(
            &db,
            "DELETE FROM sales USING other WHERE sales.id = other.id",
            &m
        )
        .is_err());
    }

    #[test]
    fn row_filter_refuses_unprotectable_shapes() {
        let mut db = filters_db();
        db.execute("CREATE TABLE other (id INT)").unwrap();
        db.execute("CREATE VIEW v AS SELECT * FROM sales").unwrap();
        let mut m = std::collections::BTreeMap::new();
        m.insert("sales".to_string(), vec!["region = 'east'".to_string()]);
        let run = |sql: &str| apply_row_filters(&db, sql, &m).is_err();
        // JOIN referencing the filtered table.
        assert!(run("SELECT s.id FROM sales s JOIN other o ON 1 = 1"));
        assert!(run("SELECT id FROM sales JOIN other ON 1 = 1"));
        // Subquery occurrence.
        assert!(run("SELECT (SELECT COUNT(*) FROM sales) FROM other"));
        assert!(run(
            "SELECT id FROM other WHERE id IN (SELECT id FROM sales)"
        ));
        // Set operations.
        assert!(run("SELECT id FROM other UNION SELECT id FROM sales"));
        // View over the filtered table (transitively).
        assert!(run("SELECT * FROM v"));
        // UPDATE ... FROM evaluates its WHERE over the MERGED image: the
        // injected predicate's bare `region` can bind a same-named column
        // of the FROM table (a controlled table short-circuits the filter
        // for every row) — refuse, same treatment as JOIN/子查询(第六轮
        // 审查:旧断言只验证了谓词文本落进 WHERE,没验证它绑到哪)。
        assert!(run(
            "UPDATE sales SET id = 1 FROM other WHERE other.id = sales.id"
        ));
        // 无 FROM 的 UPDATE 照常注入。
        let out = apply_row_filters(&db, "UPDATE sales SET id = 1 WHERE id = 2", &m).unwrap();
        assert!(out.to_lowercase().contains("region = 'east'"), "{out}");
        // T-SQL-ish unparseable text mentioning the table.
        assert!(run("DECLARE @x INT; SELECT id FROM sales"));
        // Unrelated statements pass through untouched.
        let keep = apply_row_filters(&db, "SELECT * FROM other", &m).unwrap();
        assert_eq!(keep, "SELECT * FROM other");
    }

    #[test]
    fn row_filter_same_table_twice_is_refused() {
        // 第三轮:读目标集合曾按表名去重,同表第二次出现(子查询位)把
        // deep 计数压回 top——子查询位完全逃过过滤,secret 逐字出线。
        let mut db = filters_db();
        db.execute("ALTER TABLE sales ADD COLUMN secret TEXT")
            .unwrap();
        let mut m = std::collections::BTreeMap::new();
        m.insert("sales".to_string(), vec!["region = 'east'".to_string()]);
        for sql in [
            "SELECT (SELECT secret FROM sales WHERE id = 42) FROM sales",
            "SELECT id FROM sales WHERE secret = (SELECT secret FROM sales WHERE id = 42)",
        ] {
            assert!(
                apply_row_filters(&db, sql, &m).is_err(),
                "same-table second read must be refused: {sql}"
            );
        }
        // 对照:单次出现照常改写。
        let out = apply_row_filters(&db, "SELECT id FROM sales", &m).unwrap();
        assert!(out.to_lowercase().contains("region = 'east'"), "{out}");
    }

    #[test]
    fn row_filter_parse_failure_without_mention_passes_through() {
        let db = filters_db();
        let mut m = std::collections::BTreeMap::new();
        m.insert("sales".to_string(), vec!["region = 'east'".to_string()]);
        let keep = apply_row_filters(&db, "DECLARE @x INT", &m).unwrap();
        assert_eq!(keep, "DECLARE @x INT");
    }

    #[test]
    fn row_filter_and_column_grants_coexist_via_resolve() {
        let mut db = col_grants_db();
        for sql in [
            "CREATE USER carol PASSWORD 'pw12345678'",
            "GRANT SELECT (id) ON sales WHERE region = 'east' TO carol",
        ] {
            db.execute(sql).unwrap();
        }
        let g = docsql_core::useradmin::resolve_grants(&mut db, "carol")
            .unwrap()
            .unwrap();
        assert_eq!(g.granted_cols("sales"), &["id".to_string()]);
        assert_eq!(g.filters_for("sales"), &["region = 'east'".to_string()]);
        // The filter rewrite applies for this user's SELECT.
        let mut m = g.filter_map();
        let rewritten = apply_row_filters(&db, "SELECT id FROM sales", &m).unwrap();
        m.clear();
        drop(m);
        assert!(rewritten.to_lowercase().contains("region = 'east'"));
    }

    use super::*;

    #[test]
    fn authorize_statement_routes_merge_by_its_arms() {
        use docsql_core::useradmin::{PRIV_DELETE, PRIV_INSERT, PRIV_UPDATE};
        let mut db = docsql_core::engine::Database::in_memory().unwrap();
        for sql in [
            "CREATE TABLE tgt (id INT PRIMARY KEY, v INT)",
            "CREATE TABLE src (id INT PRIMARY KEY, v INT)",
            "INSERT INTO tgt VALUES (1, 1)",
            "INSERT INTO src VALUES (1, 2)",
        ] {
            db.execute(sql).unwrap();
        }
        let parse = |sql: &str| docsql_core::engine::Database::parse_classified(sql).unwrap();
        let grants = |bits: u8| {
            let mut g = docsql_core::useradmin::UserGrants {
                admin: false,
                ..Default::default()
            };
            g.table_privs.insert("tgt".into(), bits);
            g.table_privs.insert("src".into(), 0xFF);
            g
        };
        let upd = parse(
            "MERGE INTO tgt USING src ON tgt.id = src.id \
             WHEN MATCHED THEN UPDATE SET v = src.v \
             WHEN NOT MATCHED THEN INSERT (id, v) VALUES (src.id, src.v)",
        );
        // MERGE used to fall into the DDL refusal for every non-admin.
        let r = authorize_statement(
            Some(&db),
            &upd.stmt,
            &upd.tx,
            upd.is_write,
            &grants(PRIV_INSERT | PRIV_UPDATE),
        );
        assert!(
            r.is_ok(),
            "readwrite-equivalent bits must authorize a MERGE: {r:?}"
        );
        // UPDATE-only grant cannot run the INSERT arm.
        assert!(authorize_statement(
            Some(&db),
            &upd.stmt,
            &upd.tx,
            upd.is_write,
            &grants(PRIV_UPDATE)
        )
        .is_err());
        // Matching arms only: an update-only MERGE passes with just UPDATE.
        let upd_only = parse(
            "MERGE INTO tgt USING src ON tgt.id = src.id \
             WHEN MATCHED THEN UPDATE SET v = src.v",
        );
        assert!(authorize_statement(
            Some(&db),
            &upd_only.stmt,
            &upd_only.tx,
            upd_only.is_write,
            &grants(PRIV_UPDATE)
        )
        .is_ok());
        let _ = PRIV_DELETE;
    }

    #[test]
    fn bind_params_treats_bracket_identifiers_as_opaque() {
        // `[col?]` is a T-SQL identifier; the ? inside is data. Binding used
        // to substitute it (or error on the count).
        let out = bind_params("SELECT [col?], name FROM t WHERE id = ?", &[Value::Int(7)]).unwrap();
        assert!(out.contains("[col?]"), "{out}");
        assert!(out.ends_with("7"), "{out}");
        // ]] escape inside brackets stays intact.
        let out = bind_params("SELECT [a]]?b] FROM t", &[]).unwrap();
        assert_eq!(out, "SELECT [a]]?b] FROM t");
    }

    #[test]
    fn bind_params_is_quote_aware_and_escapes_values() {
        // Plain positional binding of every value type.
        assert_eq!(
            bind_params(
                "INSERT INTO t VALUES (?, ?, ?, ?, ?)",
                &[
                    Value::Int(7),
                    Value::Float(1.5),
                    Value::Bool(true),
                    Value::Null,
                    Value::Str("it's".into()),
                ]
            )
            .unwrap(),
            "INSERT INTO t VALUES (7, 1.5, TRUE, NULL, 'it''s')"
        );
        // A `?` inside a string literal (and a doubled '' escape) is data.
        assert_eq!(
            bind_params("SELECT 'a?b''c?' , ? FROM t", &[Value::Int(1)]).unwrap(),
            "SELECT 'a?b''c?' , 1 FROM t"
        );
        // Quoted identifiers and comments too (the old scanner only knew
        // single quotes, so these misbound or errored).
        assert_eq!(
            bind_params("SELECT \"ready?\" FROM t WHERE id = ?", &[Value::Int(2)]).unwrap(),
            "SELECT \"ready?\" FROM t WHERE id = 2"
        );
        assert_eq!(
            bind_params("SELECT ? -- set?\n+ ?", &[Value::Int(1), Value::Int(2)]).unwrap(),
            "SELECT 1 -- set?\n+ 2"
        );
        assert_eq!(
            bind_params("SELECT /* ? */ ?", &[Value::Int(3)]).unwrap(),
            "SELECT /* ? */ 3"
        );
        // Unbalanced quotes/comments are copied without panicking.
        assert_eq!(bind_params("SELECT '? ;", &[]).unwrap(), "SELECT '? ;");
        assert_eq!(bind_params("SELECT \"x? ", &[]).unwrap(), "SELECT \"x? ");
        // Bytes render as hex literals.
        assert_eq!(
            bind_params(
                "INSERT INTO b VALUES (?)",
                &[Value::Bytes(vec![0xde, 0xad])]
            )
            .unwrap(),
            "INSERT INTO b VALUES (x'dead')"
        );
        // Decimals render as exact CAST text, never a lossy float.
        assert_eq!(
            bind_params(
                "INSERT INTO m VALUES (?)",
                &[Value::Decimal("1234567890.123456789".parse().unwrap())]
            )
            .unwrap(),
            "INSERT INTO m VALUES (CAST('1234567890.123456789' AS DECIMAL))"
        );
        // Arity mismatches are errors, never partial binds.
        assert!(bind_params("SELECT ?", &[]).is_err());
        assert!(bind_params("SELECT 1", &[Value::Int(1)]).is_err());
    }

    #[test]
    fn json_params_decode_dec_and_bytes_markers() {
        // The .NET client's typed markers decode to exact engine values.
        let p: serde_json::Value =
            serde_json::from_str(r#"{"$dec":"0.10000000000000000001"}"#).unwrap();
        assert_eq!(
            json_param_to_value(&p),
            Value::Decimal("0.10000000000000000001".parse().unwrap())
        );
        let p: serde_json::Value = serde_json::from_str(r#"{"$bytes":[0,255]}"#).unwrap();
        assert_eq!(json_param_to_value(&p), Value::Bytes(vec![0, 255]));
        let p: serde_json::Value = serde_json::from_str(r#"{"$bytes":[]}"#).unwrap();
        assert_eq!(json_param_to_value(&p), Value::Bytes(vec![]));
        // Malformed markers degrade to text instead of corrupting data.
        let p: serde_json::Value = serde_json::from_str(r#"{"$dec":"nope"}"#).unwrap();
        assert!(matches!(json_param_to_value(&p), Value::Str(_)));
        let p: serde_json::Value = serde_json::from_str(r#"{"$bytes":[256]}"#).unwrap();
        assert!(matches!(json_param_to_value(&p), Value::Str(_)));
        let p: serde_json::Value = serde_json::from_str(r#"{"$bytes":"not-an-array"}"#).unwrap();
        assert!(matches!(json_param_to_value(&p), Value::Str(_)));
        // $ts 在规范值域内解码为 Timestamp;超域值没有可重放的文本形,
        // 与 $dec 解析失败同样回退为字符串。
        let p: serde_json::Value = serde_json::from_str(r#"{"$ts":1577836800000}"#).unwrap();
        assert_eq!(json_param_to_value(&p), Value::Timestamp(1_577_836_800_000));
        let p: serde_json::Value = serde_json::from_str(r#"{"$ts":253402300800000}"#).unwrap();
        assert!(matches!(json_param_to_value(&p), Value::Str(_)));
        let p: serde_json::Value = serde_json::from_str(&format!(
            "{{\"$ts\":{}}}",
            docsql_core::value::TIMESTAMP_MIN_MS - 1
        ))
        .unwrap();
        assert!(matches!(json_param_to_value(&p), Value::Str(_)));
        // Ordinary objects stay text (render_param quotes them).
        let p: serde_json::Value = serde_json::from_str(r#"{"k":1}"#).unwrap();
        assert_eq!(json_param_to_value(&p), Value::Str(r#"{"k":1}"#.into()));
    }

    #[test]
    fn marker_payloads_cannot_escape_the_binding_literal() {
        // A hostile "$dec" payload that fails decimal parsing must fall back
        // to a quoted text literal, never raw SQL.
        let p: serde_json::Value =
            serde_json::from_str(r#"{"$dec":"1'; DROP TABLE t; --"}"#).unwrap();
        let v = json_param_to_value(&p);
        assert!(matches!(v, Value::Str(_)));
        let rendered = bind_params("SELECT ?", &[v]).unwrap();
        assert_eq!(rendered, r#"SELECT '{"$dec":"1''; DROP TABLE t; --"}'"#);
        // Nested arrays in "$bytes" are rejected (integer elements only).
        let p: serde_json::Value = serde_json::from_str(r#"{"$bytes":[[]]}"#).unwrap();
        assert!(matches!(json_param_to_value(&p), Value::Str(_)));
    }

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

    #[test]
    fn parse_peer_addr_accepts_shapes_and_rejects_garbage() {
        // SocketAddr 形态(IPv4/IPv6)与 host:port 形态。
        assert_eq!(
            parse_peer_addr("10.1.2.3:7600"),
            Some(("10.1.2.3".into(), 7600))
        );
        assert_eq!(parse_peer_addr("[::1]:7601"), Some(("::1".into(), 7601)));
        assert_eq!(
            parse_peer_addr("node-a.local:7600"),
            Some(("node-a.local".into(), 7600))
        );
        // 垃圾输入:空串、端口 0、空 host、非数字端口、控制字符、超长。
        assert_eq!(parse_peer_addr(""), None);
        assert_eq!(parse_peer_addr("10.1.2.3:0"), None);
        assert_eq!(parse_peer_addr("node-a.local:0"), None);
        assert_eq!(parse_peer_addr(":7600"), None);
        assert_eq!(parse_peer_addr("host:port"), None);
        assert_eq!(parse_peer_addr("host:7600\n"), None);
        assert_eq!(parse_peer_addr(&format!("{}:7600", "h".repeat(301))), None);
        assert_eq!(parse_peer_addr("no-port-here"), None);
    }

    #[tokio::test]
    async fn is_self_peer_matches_literal_loopback_and_resolved_forms() {
        // 字面相等(大小写不敏感)即自身。
        assert!(is_self_peer("0.0.0.0:7600", "0.0.0.0:7600").await);
        assert!(!is_self_peer("0.0.0.0:7600", "0.0.0.1:7600").await);
        // 回环地址 + 同监听端口 = 自身(advertise 127.0.0.1)。
        assert!(is_self_peer("0.0.0.0:7600", "127.0.0.1:7600").await);
        assert!(is_self_peer("0.0.0.0:7600", "localhost:7600").await);
        // 同端口但非回环 ≠ 自身;回环但端口不同 ≠ 自身。
        assert!(!is_self_peer("0.0.0.0:7600", "127.0.0.1:7601").await);
        assert!(!is_self_peer("0.0.0.0:7600", "192.0.2.99:7600").await);
        // 解析失败的 listen 串保守返回 false,不 panic。
        assert!(!is_self_peer("not an addr", "127.0.0.1:7600").await);
        assert!(!is_self_peer("0.0.0.0:7600", "definitely not an addr").await);
    }

    #[tokio::test]
    async fn is_self_peer_matches_own_lan_ip_behind_wildcard_listen() {
        // Regression: a 0.0.0.0 listen never resolves to the interface
        // addresses, so a node advertising its own LAN IP used to slip past
        // the self check and register itself as its own peer — every write
        // double-applied. Local-address ownership is proven by a UDP bind.
        let Ok(sock) = std::net::UdpSocket::bind("0.0.0.0:0") else {
            return;
        };
        if sock.connect("8.8.8.8:80").is_err() {
            return; // no route to choose an interface address; nothing to prove
        }
        let Ok(local) = sock.local_addr() else {
            return;
        };
        if local.ip().is_loopback() {
            return; // CI box without a routable interface
        }
        let same_port = std::net::SocketAddr::new(local.ip(), 7600);
        assert!(is_self_peer("0.0.0.0:7600", &same_port.to_string()).await);
        // Same machine, different port: a distinct node (dev setups run
        // several on one host) — must NOT count as self.
        let other_port = std::net::SocketAddr::new(local.ip(), 7601);
        assert!(!is_self_peer("0.0.0.0:7600", &other_port.to_string()).await);
    }

    #[test]
    fn render_param_covers_timestamp_and_nested_documents() {
        // TIMESTAMP 绑定为显式 CAST(重放零偏差)。
        let bound = bind_params("SELECT ?", &[Value::Timestamp(1_577_836_800_000)]).unwrap();
        assert_eq!(
            bound,
            "SELECT CAST('2020-01-01T00:00:00.000Z' AS TIMESTAMP)"
        );
        // 嵌套文档走 JSON 文本字面量。
        let v = Value::Array(vec![Value::Int(1), Value::Str("s".into())]);
        let bound = bind_params("SELECT ?", &[v]).unwrap();
        assert_eq!(bound, "SELECT '[1,\"s\"]'");
        // Float 与 Bool 的裸渲染。
        assert_eq!(
            bind_params("SELECT ?, ?", &[Value::Float(2.5), Value::Bool(false)]).unwrap(),
            "SELECT 2.5, FALSE"
        );
    }

    #[test]
    fn outcome_frame_renders_all_three_arms() {
        let f = outcome_frame::<String>(Ok(ExecOutcome::Affected(3)));
        assert_eq!(f.frame_type, proto::RESP_AFFECTED);
        assert_eq!(proto::decode_affected(&f.payload), 3);
        let f = outcome_frame::<String>(Ok(ExecOutcome::Rows(docsql_core::engine::QueryResult {
            columns: vec!["a".into()],
            rows: vec![vec![Value::Int(1)]],
        })));
        assert_eq!(f.frame_type, proto::RESP_ROWS);
        let v: serde_json::Value = serde_json::from_slice(&f.payload).unwrap();
        assert_eq!(v["rows"][0][0], 1);
        let f = outcome_frame::<String>(Err("kaput".into()));
        assert_eq!(f.frame_type, proto::RESP_ERROR);
        assert!(String::from_utf8_lossy(&f.payload).contains("kaput"));
    }

    #[test]
    fn readable_by_all_classifies_system_and_compat_surfaces() {
        assert!(readable_by_all("_pubsub_messages"));
        assert!(readable_by_all("_cluster_log"));
        assert!(readable_by_all("sqlite_master"));
        // The walker yields the QUALIFIED compatibility-view names; the
        // prefix match keeps custom-role users able to run the EF
        // SchemaSync probes (the virtual views cannot be GRANTed).
        assert!(readable_by_all("information_schema.tables"));
        assert!(readable_by_all("information_schema.columns"));
        assert!(readable_by_all("@compat"));
        assert!(!readable_by_all("user_tbl"));
        assert!(!readable_by_all("docsql_users"));
    }

    #[test]
    fn parse_name_array_validates_pubsub_name_payloads() {
        assert_eq!(
            parse_name_array(br#"["a","b"]"#).unwrap(),
            vec!["a".to_string(), "b".to_string()]
        );
        // 空数组 = 全部退订。
        assert_eq!(parse_name_array(b"[]").unwrap(), Vec::<String>::new());
        // 非数组 / 非字符串元素 / 空名 / 超长名 / 坏 JSON。
        assert!(parse_name_array(b"{\"a\":1}").is_err());
        assert!(parse_name_array(b"[1]").is_err());
        assert!(parse_name_array(br#"[""]"#).is_err());
        assert!(parse_name_array(&format!("[\"{}\"]", "c".repeat(300)).into_bytes()).is_err());
        assert!(parse_name_array(b"not json").is_err());
    }

    #[test]
    fn catchup_entries_pack_decode_round_trip_and_budget_split() {
        let entries = vec![
            (1u64, "INSERT INTO t VALUES (1)".to_string()),
            (2u64, "UPDATE t SET v = 'x' WHERE id = 1".to_string()),
            (3u64, "DELETE FROM t".to_string()),
        ];
        let (payload, count) = pack_catchup_entries(&entries, usize::MAX);
        assert_eq!(count, 3);
        assert_eq!(decode_catchup_entries(&payload).unwrap(), entries);
        // 预算装不下第二条:只发第一条,但至少前进一条。
        let first_len = 8 + 4 + entries[0].1.len();
        let (payload, count) = pack_catchup_entries(&entries, first_len + 4);
        assert_eq!(count, 1);
        assert_eq!(
            decode_catchup_entries(&payload).unwrap(),
            entries[..1].to_vec()
        );
        // 截断载荷报错,不静默丢尾。
        let (payload, _) = pack_catchup_entries(&entries, usize::MAX);
        assert!(decode_catchup_entries(&payload[..payload.len() - 3]).is_err());
        assert!(decode_catchup_entries(&[]).unwrap().is_empty());
    }

    #[test]
    fn parse_seq_frame_round_trip_and_rejects_malformed() {
        let sql = "INSERT INTO t VALUES (1)";
        let mut payload = Vec::new();
        payload.extend_from_slice(&42u64.to_le_bytes());
        payload.extend_from_slice(&(5u32).to_le_bytes());
        payload.extend_from_slice(b"node1");
        payload.extend_from_slice(&proto::encode_sql(sql).unwrap());
        assert_eq!(
            parse_seq_frame(&payload),
            Some((42, "node1".to_string(), sql.to_string()))
        );
        // 载荷过短 / id_len 越界 / 坏 UTF-8 / 坏 SQL 编码。
        assert_eq!(parse_seq_frame(&payload[..11]), None);
        let mut bad = payload.clone();
        bad[8..12].copy_from_slice(&9999u32.to_le_bytes());
        assert_eq!(parse_seq_frame(&bad), None);
        let mut bad = payload.clone();
        bad[12..17].copy_from_slice(&[0xff; 5]);
        assert_eq!(parse_seq_frame(&bad), None);
        let mut bad = payload.clone();
        bad.truncate(12 + 5 + 1); // 只剩 1 字节,不够 SQL 编码
        assert_eq!(parse_seq_frame(&bad), None);
    }

    #[test]
    fn affected_count_reads_wellformed_frames_only() {
        assert_eq!(
            affected_count(&Frame::new(
                proto::RESP_AFFECTED,
                9u64.to_le_bytes().to_vec()
            )),
            9
        );
        // 错误帧型 / 载荷长度不对 → 0。
        assert_eq!(
            affected_count(&Frame::new(proto::RESP_ERROR, 9u64.to_le_bytes().to_vec())),
            0
        );
        assert_eq!(
            affected_count(&Frame::new(proto::RESP_AFFECTED, vec![1, 2, 3])),
            0
        );
    }

    #[test]
    fn replication_frame_sets_flags_and_seals_with_key() {
        let f = replication_frame(proto::REQ_DIGEST, b"hi".to_vec(), None);
        assert_eq!(f.frame_type, proto::REQ_DIGEST);
        assert_eq!(f.flags & FLAG_REPLICATION, FLAG_REPLICATION);
        assert_eq!(f.payload, b"hi");
        // 有密钥时置加密位并封 payload。
        let key = crypto::parse_key_hex(
            "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
        )
        .unwrap();
        let hello = [7u8; 16];
        let wire: WireKey = Some((&key, &hello));
        let f = replication_frame(proto::REQ_DIGEST, b"hi".to_vec(), wire);
        assert_ne!(f.payload, b"hi");
        assert_eq!(f.flags & crypto::FLAG_ENCRYPTED, crypto::FLAG_ENCRYPTED);
    }

    #[test]
    fn advance_position_never_moves_backwards() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = docsql_core::engine::Database::open(&dir.path().join("db")).unwrap();
        advance_position(&mut db, "node-a", 10);
        advance_position(&mut db, "node-a", 7); // 迟到的低 seq 不得压低
        assert_eq!(db.position_get("node-a").unwrap(), Some(7 + 3));
        advance_position(&mut db, "node-a", 12);
        assert_eq!(db.position_get("node-a").unwrap(), Some(12));
    }

    #[test]
    fn view_reads_expand_base_tables_for_authorization() {
        // A legacy view over the user tables (created before the read gate,
        // replayed from an old dump) must not hand password hashes to a
        // readonly connection: the READ path expands view base tables
        // exactly like the write path.
        let mut db = docsql_core::engine::Database::in_memory().unwrap();
        // The user tables are created lazily by the first user-admin
        // statement; the view over them needs them to exist.
        db.execute("CREATE USER u1 PASSWORD 'a-password-1'")
            .unwrap();
        db.execute("CREATE VIEW legacy AS SELECT name FROM docsql_users")
            .unwrap();
        db.execute("CREATE TABLE t (a INT)").unwrap();
        db.execute("CREATE VIEW ok_v AS SELECT a FROM t").unwrap();
        let parse = |sql: &str| docsql_core::engine::Database::parse_classified(sql).unwrap();
        let mut g = docsql_core::useradmin::UserGrants {
            admin: false,
            ..Default::default()
        };
        g.readonly = true;
        // Ordinary view over a readable base: fine for readonly.
        let p = parse("SELECT * FROM ok_v");
        assert!(authorize_statement(Some(&db), &p.stmt, &p.tx, p.is_write, &g).is_ok());
        // View over the user tables: refused on the READ path (the create
        // itself is admin-only DDL, so only legacy/dump views get here).
        let p = parse("SELECT * FROM legacy");
        let e = authorize_statement(Some(&db), &p.stmt, &p.tx, p.is_write, &g).unwrap_err();
        assert!(e.contains("admin role only"), "{e}");
        // Admin still reads it.
        let p = parse("SELECT * FROM legacy");
        let mut admin = docsql_core::useradmin::UserGrants {
            admin: true,
            ..Default::default()
        };
        admin.readonly = true;
        assert!(authorize_statement(Some(&db), &p.stmt, &p.tx, p.is_write, &admin).is_ok());
    }

    #[test]
    fn compat_view_reads_yield_qualified_targets() {
        // The walker must keep the qualified name so readable_by_all's
        // information_schema. prefix actually fires — taking the last
        // segment made the EF SchemaSync probes ("columns") collide with
        // user-table authorization for custom-role users.
        let p = docsql_core::engine::Database::parse_classified(
            "SELECT * FROM information_schema.columns",
        )
        .unwrap();
        let docsql_core::engine::AnyStmt::Sql(stmt) = &p.stmt else {
            panic!("expected a SQL statement");
        };
        let targets = docsql_core::engine::Database::stmt_read_targets(stmt, &|_| false).unwrap();
        assert!(
            targets.contains(&"information_schema.columns".to_string()),
            "{targets:?}"
        );
        assert!(readable_by_all("information_schema.columns"));
    }

    #[test]
    fn truncate_cascade_authorizes_the_fk_children_too() {
        use docsql_core::useradmin::PRIV_DELETE;
        let mut db = docsql_core::engine::Database::in_memory().unwrap();
        for sql in [
            "CREATE TABLE parent (id INT PRIMARY KEY)",
            "CREATE TABLE child (id INT PRIMARY KEY, pid INT, \
             FOREIGN KEY (pid) REFERENCES parent (id))",
            "CREATE TABLE grandchild (id INT PRIMARY KEY, cid INT, \
             FOREIGN KEY (cid) REFERENCES child (id))",
            "INSERT INTO parent VALUES (1)",
            "INSERT INTO child VALUES (10, 1)",
            "INSERT INTO grandchild VALUES (100, 10)",
        ] {
            db.execute(sql).unwrap();
        }
        let parse = |sql: &str| docsql_core::engine::Database::parse_classified(sql).unwrap();
        let mut g = docsql_core::useradmin::UserGrants {
            admin: false,
            ..Default::default()
        };
        g.table_privs.insert("parent".into(), PRIV_DELETE);
        // Plain TRUNCATE on the granted table is fine.
        let p = parse("TRUNCATE parent");
        assert!(authorize_statement(Some(&db), &p.stmt, &p.tx, p.is_write, &g).is_ok());
        // CASCADE clears the FK children — every one of them is
        // authorization input now; holding DELETE on `parent` alone no
        // longer empties child/grandchild tables with no grant at all.
        let p = parse("TRUNCATE parent CASCADE");
        let e = authorize_statement(Some(&db), &p.stmt, &p.tx, p.is_write, &g).unwrap_err();
        assert!(e.contains("child"), "{e}");
        // Granting the whole closure satisfies the check again.
        g.table_privs.insert("child".into(), PRIV_DELETE);
        g.table_privs.insert("grandchild".into(), PRIV_DELETE);
        assert!(authorize_statement(Some(&db), &p.stmt, &p.tx, p.is_write, &g).is_ok());
    }

    #[test]
    fn authorize_statement_routes_by_role_and_expands_view_sources() {
        use docsql_core::useradmin::{UserAdminStmt, PRIV_INSERT, PRIV_SELECT};
        let mut db = docsql_core::engine::Database::in_memory().unwrap();
        for sql in [
            "CREATE TABLE secret_t (id INT PRIMARY KEY, v TEXT)",
            "INSERT INTO secret_t VALUES (1, 'x')",
            "CREATE TABLE mine (id INT PRIMARY KEY, v TEXT)",
            "CREATE VIEW pub_v AS SELECT id, v FROM secret_t",
        ] {
            db.execute(sql).unwrap();
        }
        let parse = |sql: &str| docsql_core::engine::Database::parse_classified(sql).unwrap();
        let grants = |admin: bool, privs: &[(&str, u8)]| {
            let mut g = docsql_core::useradmin::UserGrants {
                admin,
                ..Default::default()
            };
            for (t, b) in privs {
                g.table_privs.insert((*t).to_string(), *b);
            }
            g
        };
        // admin 全过。
        let p = parse("SELECT * FROM secret_t");
        assert!(
            authorize_statement(Some(&db), &p.stmt, &p.tx, p.is_write, &grants(true, &[])).is_ok()
        );
        let p = parse("DROP TABLE secret_t");
        assert!(
            authorize_statement(Some(&db), &p.stmt, &p.tx, p.is_write, &grants(true, &[])).is_ok()
        );
        // 用户管理语句对非 admin 拒绝(即使形状是 UserAdmin)。
        let ua = AnyStmt::UserAdmin(UserAdminStmt::CreateUser {
            name: "u".into(),
            password: "pw".into(),
        });
        assert!(
            authorize_statement(None, &ua, &TxControl::None, false, &grants(false, &[])).is_err()
        );
        // 事务内语句不在本检查点(缓冲语句逐条已授权)。
        let p = parse("SELECT * FROM secret_t");
        assert!(authorize_statement(
            Some(&db),
            &p.stmt,
            &TxControl::Begin,
            p.is_write,
            &grants(false, &[])
        )
        .is_ok());
        // 读:未授权表拒绝、系统表面放行、表级 SELECT 放行。
        let p = parse("SELECT * FROM secret_t");
        let e = authorize_statement(Some(&db), &p.stmt, &p.tx, p.is_write, &grants(false, &[]))
            .unwrap_err();
        assert!(e.contains("SELECT on table secret_t"), "{e}");
        let p = parse("SELECT * FROM _pubsub_messages");
        assert!(
            authorize_statement(Some(&db), &p.stmt, &p.tx, p.is_write, &grants(false, &[])).is_ok()
        );
        let p = parse("SELECT * FROM secret_t");
        assert!(authorize_statement(
            Some(&db),
            &p.stmt,
            &p.tx,
            p.is_write,
            &grants(false, &[("secret_t", PRIV_SELECT)])
        )
        .is_ok());
        // 写:INSERT 无位拒绝、有位放行。
        let p = parse("INSERT INTO mine VALUES (1, 'x')");
        let e = authorize_statement(Some(&db), &p.stmt, &p.tx, p.is_write, &grants(false, &[]))
            .unwrap_err();
        assert!(e.contains("INSERT on table mine"), "{e}");
        let p = parse("INSERT INTO mine VALUES (1, 'x')");
        assert!(authorize_statement(
            Some(&db),
            &p.stmt,
            &p.tx,
            p.is_write,
            &grants(false, &[("mine", PRIV_INSERT)])
        )
        .is_ok());
        // 写同时读:INSERT..SELECT 未授权读源拒绝;视图读源 fail-closed 展开为基表。
        let p = parse("INSERT INTO mine SELECT * FROM secret_t");
        let e = authorize_statement(
            Some(&db),
            &p.stmt,
            &p.tx,
            p.is_write,
            &grants(false, &[("mine", PRIV_INSERT)]),
        )
        .unwrap_err();
        assert!(e.contains("SELECT on table secret_t"), "{e}");
        let p = parse("INSERT INTO mine SELECT * FROM pub_v");
        let e = authorize_statement(
            Some(&db),
            &p.stmt,
            &p.tx,
            p.is_write,
            &grants(false, &[("mine", PRIV_INSERT)]),
        )
        .unwrap_err();
        assert!(e.contains("SELECT on table secret_t"), "{e}");
        let p = parse("INSERT INTO mine SELECT * FROM pub_v");
        assert!(authorize_statement(
            Some(&db),
            &p.stmt,
            &p.tx,
            p.is_write,
            &grants(false, &[("mine", PRIV_INSERT), ("secret_t", PRIV_SELECT)])
        )
        .is_ok());
        // 用户存储表对非 admin 的读写都拒绝。
        let p = parse("SELECT * FROM docsql_users");
        assert!(authorize_statement(
            Some(&db),
            &p.stmt,
            &p.tx,
            p.is_write,
            &grants(false, &[("docsql_users", PRIV_SELECT)])
        )
        .is_err());
        // DDL 对非 admin 拒绝。
        let p = parse("CREATE TABLE nope (id INT)");
        let e = authorize_statement(Some(&db), &p.stmt, &p.tx, p.is_write, &grants(false, &[]))
            .unwrap_err();
        assert!(e.contains("admin role"), "{e}");
        // 深视图链:fail-closed 现在分两层。引擎层拒建超过展开预算的链
        // (16 节点,创建即拒——目录里不再存在查不了的链);授权层对整条
        // 语句的总展开数另设上限,宽形状(引用多条独立链)同样触顶。
        db.execute("CREATE TABLE deep_base (id INT)").unwrap();
        db.execute("CREATE VIEW v0 AS SELECT id FROM deep_base")
            .unwrap();
        for i in 1..=15 {
            db.execute(&format!("CREATE VIEW v{i} AS SELECT id FROM v{}", i - 1))
                .unwrap();
        }
        // 第 17 层建不进去。
        let e = db
            .execute("CREATE VIEW v16 AS SELECT id FROM v15")
            .unwrap_err();
        assert!(e.to_string().contains("too deep"), "{e}");
        // 16 节点单链:视图是授权边界,给基表授权即可通过。
        let p = parse("INSERT INTO mine SELECT * FROM v15");
        authorize_statement(
            Some(&db),
            &p.stmt,
            &p.tx,
            p.is_write,
            &grants(false, &[("mine", PRIV_INSERT), ("deep_base", PRIV_SELECT)]),
        )
        .unwrap();
        // 宽形状:三条独立链,总展开数超 16 被授权层拒绝。
        db.execute("CREATE TABLE b2 (id INT)").unwrap();
        db.execute("CREATE VIEW w0 AS SELECT id FROM b2").unwrap();
        for i in 1..9 {
            db.execute(&format!("CREATE VIEW w{i} AS SELECT id FROM w{}", i - 1))
                .unwrap();
        }
        let p = parse("INSERT INTO mine SELECT * FROM v15, w8");
        let e = authorize_statement(
            Some(&db),
            &p.stmt,
            &p.tx,
            p.is_write,
            &grants(false, &[("mine", PRIV_INSERT)]),
        )
        .unwrap_err();
        assert!(e.contains("view chain too deep"), "{e}");
    }
}

#[cfg(test)]
mod sync_tests {
    use super::dump_chunk_end;

    #[test]
    fn dump_chunk_end_never_splits_multibyte_chars() {
        // 「通」占字节 3..6:预算 4 落在它的中间字节上,必须推进到 6。
        let s = format!("xxx通{}", "y".repeat(10));
        assert_eq!(dump_chunk_end(&s, 0, 4), 6);
        assert_eq!(&s[..6], "xxx通");
        // 边界恰在字符起点时不动;超过文末截到长度。
        assert_eq!(dump_chunk_end(&s, 0, 2), 2);
        assert_eq!(dump_chunk_end(&s, 0, 3), 3);
        assert_eq!(dump_chunk_end(&s, 0, usize::MAX), s.len());
        // 按小预算连续切分:覆盖全文,每段都是有效 UTF-8。
        let mut out = String::new();
        let mut start = 0;
        while start < s.len() {
            let end = dump_chunk_end(&s, start, 4);
            out.push_str(&s[start..end]);
            start = end;
        }
        assert_eq!(out, s);
    }
}

#[cfg(test)]
mod repair_election_tests {
    use super::{decide_repair, RepairDecision, TableDigest};

    fn digests(rows: u64, tag: u64) -> Vec<TableDigest> {
        vec![TableDigest {
            name: "t".into(),
            rows,
            rows_hash: tag,
            schema_hash: tag,
        }]
    }

    fn reports(states: &[(&str, Vec<TableDigest>)]) -> Vec<(String, Vec<TableDigest>)> {
        states
            .iter()
            .map(|(p, d)| (p.to_string(), d.clone()))
            .collect()
    }

    #[test]
    fn even_split_does_not_declare_convergence() {
        // A,B agree (state α); C,D agree (state β). The old "any peer
        // agrees" shortcut made every node Converged and the split
        // persisted forever. α has 2 rows, β has 1: both sides must pull α.
        let alpha = digests(2, 1);
        let beta = digests(1, 2);
        let view_a = reports(&[
            ("B", alpha.clone()),
            ("C", beta.clone()),
            ("D", beta.clone()),
        ]);
        // A's group (A,B) ties on size with (C,D); the row count elects α,
        // and A already agrees with a winner — nothing to repair.
        assert!(matches!(
            decide_repair(&alpha, &view_a),
            RepairDecision::Converged
        ));
        let view_c = reports(&[
            ("A", alpha.clone()),
            ("B", alpha.clone()),
            ("D", beta.clone()),
        ]);
        assert!(matches!(decide_repair(&beta, &view_c), RepairDecision::Pull(p) if p == "A"));
        // Once both sides hold α, they agree again.
        let view_a2 = reports(&[("C", alpha.clone()), ("D", alpha.clone())]);
        assert!(matches!(
            decide_repair(&alpha, &view_a2),
            RepairDecision::Converged
        ));
    }

    #[test]
    fn majority_agreement_converges_and_minority_pulls() {
        let alpha = digests(5, 1);
        let beta = digests(1, 2);
        let peer_view = reports(&[("B", alpha.clone()), ("C", alpha.clone())]);
        assert!(matches!(
            decide_repair(&alpha, &peer_view),
            RepairDecision::Converged
        ));
        assert!(matches!(decide_repair(&beta, &peer_view), RepairDecision::Pull(p) if p == "B"));
    }

    #[test]
    fn empty_group_never_wipes_local_data() {
        let empty = digests(0, 0);
        let local = digests(3, 7);
        // Three empty peers versus one non-empty local: size would elect the
        // empty group, but the last copy must survive.
        let peer_view = reports(&[
            ("B", empty.clone()),
            ("C", empty.clone()),
            ("D", empty.clone()),
        ]);
        assert!(matches!(
            decide_repair(&local, &peer_view),
            RepairDecision::Serve
        ));
        // Local empty too: nothing to lose either way.
        assert!(matches!(
            decide_repair(&empty, &peer_view),
            RepairDecision::Converged | RepairDecision::Serve
        ));
    }

    #[test]
    fn unique_states_elect_by_rows_then_digest_order() {
        // Three unique states, no agreement anywhere: the richer state wins.
        let small = digests(1, 1);
        let mid = digests(3, 3);
        let big = digests(9, 2);
        let peer_view = reports(&[("B", small.clone()), ("C", big.clone())]);
        assert!(matches!(decide_repair(&mid, &peer_view), RepairDecision::Pull(p) if p == "C"));
        // Holding the elected state (and agreeing with C) is convergence.
        assert!(matches!(
            decide_repair(&big, &peer_view),
            RepairDecision::Converged
        ));
        // Equal size and rows: the serialized-digest order breaks the tie
        // identically on every node.
        let t1 = digests(1, 1);
        let t2 = digests(1, 2);
        // A two-node disagreement: A holds t1 and sees B:t2, B mirrors it.
        let a = decide_repair(&t1, &[("B".to_string(), t2.clone())]);
        let b = decide_repair(&t2, &[("A".to_string(), t1.clone())]);
        // Exactly one of the two states is the reference (the smaller
        // serialized digest) and the other side pulls the same peer.
        if serde_json::to_vec(&t1).unwrap() < serde_json::to_vec(&t2).unwrap() {
            assert!(matches!(a, RepairDecision::Serve));
            assert!(matches!(b, RepairDecision::Pull(p) if p == "A"));
        } else {
            assert!(matches!(a, RepairDecision::Pull(p) if p == "B"));
            assert!(matches!(b, RepairDecision::Serve));
        }
    }
}

mod authz;
mod catchup;
mod config;
mod conn;
mod frames;
mod params;
mod sync;
use authz::*;
pub use authz::{check_token_strength, AUTH_LOCK_THRESHOLD, MIN_TOKEN_LEN};
use catchup::*;
pub use config::*;
use conn::*;
use frames::*;
use params::*;
use sync::*;
