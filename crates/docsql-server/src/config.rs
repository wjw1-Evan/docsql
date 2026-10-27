use super::*;

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
    /// Remote backup copy target (DOCSQL_BACKUP_S3_*); None = backups stay
    /// local-only. Every finished backup/incremental is additionally PUT
    /// here, and a restore whose local file is missing fetches it back.
    pub backup_s3: Option<s3::S3BackupConfig>,
    /// Majority-visibility write fence (DOCSQL_QUORUM, design 004 §2).
    /// Off = the historical any-node-writes semantics, byte for byte.
    pub quorum: bool,
    /// Probe cycle in milliseconds for the quorum loop
    /// (DOCSQL_QUORUM_PROBE_MS, default 1000).
    pub quorum_probe_ms: u64,
    /// Misses before a member counts as lost (DOCSQL_QUORUM_K, default 3).
    pub quorum_k: usize,
    /// Voting-member override (DOCSQL_QUORUM_MEMBERS); empty = DOCSQL_PEERS.
    pub quorum_members: Vec<String>,
    /// Extra zero-data voting members (DOCSQL_QUORUM_ARBITERS).
    pub quorum_arbiters: Vec<String>,
    /// Arbiter mode (DOCSQL_ARBITER): no data plane at all — the process
    /// answers status probes and nothing else, so 2-node clusters can vote
    /// with a third, independent failure domain.
    pub arbiter: bool,
    /// Automatic PROMOTE for read-only replicas (DOCSQL_AUTO_PROMOTE,
    /// design 004 §4): promote when the primary missed K probe cycles, a
    /// majority is visible, and the journal lag is within the catch-up
    /// window. Requires DOCSQL_QUORUM.
    pub auto_promote: bool,
    /// Per-statement wall-clock budget for CLIENT statements
    /// (DOCSQL_STATEMENT_TIMEOUT_MS; 0 = unlimited). Replication apply and
    /// restore replay are exempt — peers must apply what the origin
    /// confirmed regardless of their own speed, or clusters would diverge
    /// on slow nodes.
    pub statement_timeout_ms: u64,
    /// TLS listener certificate PEM path (DOCSQL_TLS_CERT). Must pair with
    /// `tls_key`; when set, accepted connections complete a TLS handshake
    /// before any protocol frame — plaintext clients fail loudly.
    pub tls_cert: Option<PathBuf>,
    /// TLS listener private key PEM path (DOCSQL_TLS_KEY).
    pub tls_key: Option<PathBuf>,
    /// Dial outbound protocol connections over TLS (DOCSQL_TLS_CONNECT):
    /// every peer leg this node initiates wraps the socket in TLS. Peers
    /// must have their own listener TLS configured — a plaintext target
    /// fails the handshake loudly, there is no downgrade.
    pub tls_connect: bool,
    /// Optional CA bundle PEM for outbound certificate verification
    /// (DOCSQL_TLS_CA). None = encrypt-only posture (self-signed fleets);
    /// Some = full chain + server-name verification against the anchor.
    pub tls_ca: Option<PathBuf>,
}

pub async fn run(cfg: ServerConfig) -> std::io::Result<()> {
    // TLS fail-fast: config_from_env already validates the pairing, but
    // ServerConfig can be assembled directly (tests) — the runtime guard
    // keeps a half-configured node from silently serving plaintext.
    match (&cfg.tls_cert, &cfg.tls_key) {
        (Some(_), None) | (None, Some(_)) => {
            return Err(std::io::Error::other(
                "DOCSQL_TLS_CERT and DOCSQL_TLS_KEY must be set together",
            ));
        }
        _ => {}
    }
    let tls_acceptor = match (&cfg.tls_cert, &cfg.tls_key) {
        (Some(cert), Some(key)) => Some(tls::load_tls_acceptor(cert, key)?),
        _ => None,
    };
    let tls_out = if cfg.tls_connect {
        let connector = tls::load_tls_connector(cfg.tls_ca.as_deref())?;
        if cfg.tls_ca.is_none() {
            eprintln!(
                "warning: DOCSQL_TLS_CONNECT without DOCSQL_TLS_CA — outbound TLS \
                 encrypts but does not verify peer certificates (self-signed posture; \
                 set DOCSQL_TLS_CA to a trust anchor to enable verification)"
            );
        }
        Some(connector)
    } else {
        None
    };
    let mut db = if cfg.arbiter {
        // The arbiter holds no data (design 004 §4): an in-memory database
        // satisfies the type, and the connection loop's arbiter gate keeps
        // every data-plane frame away from it.
        Database::in_memory().map_err(|e| std::io::Error::other(format!("arbiter db: {e}")))?
    } else {
        Database::open(&cfg.db_path).map_err(|e| std::io::Error::other(format!("open db: {e}")))?
    };
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
    let mut peers: Vec<String> = Vec::new();
    for p in cfg.peers {
        if is_self_peer(&cfg.listen, &p).await {
            eprintln!("ignoring self-referencing peer entry {p}");
        } else {
            peers.push(p);
        }
    }
    // Majority-visibility bookkeeping (design 004 §2): voting members =
    // DOCSQL_PEERS (+ arbiters), or the DOCSQL_QUORUM_MEMBERS override,
    // minus self entries. Without DOCSQL_QUORUM=1 the module stays
    // entirely inert (no loop, no write gate).
    let quorum = if cfg.quorum && !cfg.arbiter {
        let mut members = if cfg.quorum_members.is_empty() {
            peers.clone()
        } else {
            Vec::new()
        };
        if !cfg.quorum_members.is_empty() {
            for m in &cfg.quorum_members {
                if is_self_peer(&cfg.listen, m).await {
                    eprintln!("ignoring self-referencing quorum member {m}");
                } else {
                    members.push(m.clone());
                }
            }
        }
        for a in &cfg.quorum_arbiters {
            if is_self_peer(&cfg.listen, a).await {
                eprintln!("ignoring self-referencing arbiter entry {a}");
            } else {
                members.push(a.clone());
            }
        }
        members.sort();
        members.dedup();
        if members.is_empty() {
            eprintln!(
                "warning: DOCSQL_QUORUM enabled but no voting members configured — \
                 this node alone is always a majority, the write gate stays inert"
            );
        }
        Some(Arc::new(quorum::Quorum::new(members, cfg.quorum_k)))
    } else {
        None
    };
    // Auto-PROMOTE trigger basis: the primary as configured at startup
    // (PROMOTE detaches the live replicate_to; the trigger survives that).
    let primary_addr = if cfg.auto_promote {
        cfg.replicate_to.clone()
    } else {
        None
    };
    if let (true, Some(_)) = (cfg.arbiter, cfg.cluster_token.as_ref()) {
        eprintln!("arbiter mode: status probes only (no data plane)");
    }
    if let (true, None) = (cfg.arbiter, cfg.cluster_token.as_ref()) {
        eprintln!(
            "warning: arbiter mode without DOCSQL_CLUSTER_TOKEN — probes that \
             authenticate with the cluster token will be refused"
        );
    }
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
    let fresh = peers_configured && !has_user_tables(&db);
    let backup_dir = cfg.backup_dir.clone().unwrap_or_else(|| {
        let mut dir = cfg
            .db_path
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_default();
        dir.push("backups");
        dir
    });
    // User tables are created lazily by the first user-management statement;
    // a user-less node stays catalog-clean (digests/snapshots consistent
    // with pre-upgrade peers).
    let has_users0 = db.any_user_exists().unwrap_or(false);
    // Misconfiguration traps that silently widen the attack surface — say
    // them once, loudly, at startup:
    if cfg.auth_token.is_none() && (cfg.read_token.is_some() || cfg.cluster_token.is_some()) {
        eprintln!(
            "warning: DOCSQL_READ_TOKEN/DOCSQL_CLUSTER_TOKEN set but no DOCSQL_TOKEN — \
             the CLIENT plane is fully open (those tokens do not gate client connections)"
        );
    }
    if cfg.auth_token.is_none() && cfg.cluster_token.is_none() && has_users0 {
        eprintln!(
            "warning: user accounts exist but no DOCSQL_TOKEN/DOCSQL_CLUSTER_TOKEN is \
             configured — the documented token-less replication channel stays open to \
             unauthenticated connections (full dump, journal pull, write freeze via \
             REQ_HOLD); configure tokens for any networked deployment"
        );
    }
    // Remote backup copy client: a bad CA file fails here at startup — the
    // nightly backup must not be the first place config problems surface.
    let backup_s3 = match &cfg.backup_s3 {
        Some(c) => {
            if c.tls && c.ca.is_none() {
                eprintln!(
                    "warning: DOCSQL_BACKUP_S3_ENDPOINT is https without DOCSQL_BACKUP_S3_CA — \
                     remote backup uploads encrypt but do not verify the storage certificate; \
                     set DOCSQL_BACKUP_S3_CA to a trust anchor for full verification"
                );
            }
            Some(s3::S3Client::new(c.clone())?)
        }
        None => None,
    };
    let state = Arc::new(ServerState {
        db: std::sync::RwLock::new(db),
        metrics: metrics::Metrics::new(),
        auth_token: cfg.auth_token,
        read_token: cfg.read_token,
        cluster_token: cfg.cluster_token,
        auth_failures: tokio::sync::Mutex::new(std::collections::HashMap::new()),
        auth_gate: std::sync::Arc::new(tokio::sync::Semaphore::new(AUTH_DERIVE_CONCURRENCY)),
        conn_slots: (cfg.max_conn > 0)
            .then(|| std::sync::Arc::new(tokio::sync::Semaphore::new(cfg.max_conn))),
        auth_lock_threshold: cfg.auth_lock_threshold,
        statement_timeout: (cfg.statement_timeout_ms > 0)
            .then(|| std::time::Duration::from_millis(cfg.statement_timeout_ms)),
        idle_timeout: (cfg.idle_timeout_secs > 0)
            .then(|| std::time::Duration::from_secs(cfg.idle_timeout_secs)),
        replicate_to: tokio::sync::Mutex::new(cfg.replicate_to),
        peers: tokio::sync::Mutex::new(peers),
        tx_pending: tokio::sync::Mutex::new(TxPending::new()),
        apply_locks: tokio::sync::Mutex::new(std::collections::HashMap::new()),
        peer_backoff: tokio::sync::Mutex::new(std::collections::HashMap::new()),
        tx_owner: std::sync::Mutex::new(None),
        sessions: tokio::sync::Mutex::new(std::collections::HashMap::new()),
        write_order: std::sync::Arc::new(tokio::sync::Mutex::new(())),
        holds: tokio::sync::Mutex::new(std::collections::HashMap::new()),
        next_hold_id: std::sync::atomic::AtomicU64::new(1),
        sync_queue: tokio::sync::Mutex::new(SyncGate {
            pending: VecDeque::new(),
            pending_bytes: 0,
            closed: !peers_configured,
            pubsub_after_drain: Vec::new(),
        }),
        advertise: cfg.advertise.clone(),
        listen: cfg.listen.clone(),
        read_only: std::sync::atomic::AtomicBool::new(cfg.read_only),
        transport_key: cfg.transport_key,
        tls_out,
        tls_listener: tls_acceptor.is_some(),
        query_log: querylog::QueryLog::new(),
        sync_log: querylog::SyncLog::new(1000),
        pubsub: pubsub::PubSub::new(),
        cluster_id,
        catchup_window: cfg.catchup_window,
        backup_interval_secs: cfg.backup_interval_secs,
        backup_keep: cfg.backup_keep,
        backup_dir,
        backup_s3,
        quorum,
        quorum_probe_ms: cfg.quorum_probe_ms,
        arbiter: cfg.arbiter,
        primary_epoch: std::sync::atomic::AtomicU64::new(0),
        primary_addr,
        auto_promote_frozen: std::sync::atomic::AtomicBool::new(false),
        auto_promote: cfg.auto_promote,
        promote_lag_warned: std::sync::atomic::AtomicBool::new(false),
        backup: Mutex::new(backup::BackupShared::default()),
        restore_progress: std::sync::Arc::new(backup::RestoreProgress::default()),
        grants_epoch: std::sync::atomic::AtomicU64::new(0),
        pitr_epoch: std::sync::atomic::AtomicU64::new(0),
        has_users: std::sync::atomic::AtomicBool::new(has_users0),
        replay_failures: std::sync::atomic::AtomicU64::new(0),
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
        // Watchdog: every normal bootstrap exit drains and closes the gate
        // itself, but a task death (panic, future early-return) must never
        // leave the gate open — it would keep acknowledging writes into a
        // queue nothing will replay. On an abnormal exit, drain with a
        // bounded budget, then hard-close (loudly) rather than ack forever.
        tokio::spawn(async move {
            if tokio::spawn(bootstrap_sync(st.clone(), fresh))
                .await
                .is_err()
            {
                eprintln!("sync: bootstrap task died (panic?) — force-closing the sync gate");
            }
            if st.sync_queue.lock().await.closed {
                return;
            }
            eprintln!("sync: gate still open after bootstrap exit; draining the queue");
            let deadline = tokio::time::Instant::now() + SYNC_WATCHDOG_GRACE;
            if tokio::time::timeout_at(deadline, async {
                // Watchdog close: the owning task is gone, this node is
                // serving — the gate must close.
                drain_sync_queue(&st, false, true).await;
                flush_gate_pubsub_notify(&st).await;
            })
            .await
            .is_err()
            {
                let mut gate = st.sync_queue.lock().await;
                let n = gate.pending.len();
                gate.pending.clear();
                gate.pending_bytes = 0;
                gate.closed = true;
                // Acknowledged writes that will never be applied: keep the
                // divergence observable in REQ_STATUS like any other
                // failed replay, not only in stderr.
                st.replay_failures
                    .fetch_add(n as u64, std::sync::atomic::Ordering::SeqCst);
                eprintln!(
                    "sync: could not acquire the write path within {SYNC_WATCHDOG_GRACE:?}; \
                     gate hard-closed with {n} unapplied queued write(s) — restart this node"
                );
            }
        });
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
                    .write()
                    .unwrap_or_else(|p| p.into_inner())
                    .sync_pending();
                if let Err(e) = flushed {
                    eprintln!("async-commit flush failed: {e}");
                }
            }
        });
    }
    if cfg.backup_interval_secs > 0 && !cfg.arbiter {
        // Automatic backups: a logical dump on a timer (first tick is
        // immediate, so a restart yields a fresh backup). Ticks during the
        // startup sync are skipped inside the task.
        let st = state.clone();
        tokio::spawn(backup::backup_task(st, cfg.backup_interval_secs));
    }
    if state.quorum.is_some() {
        // Majority-visibility probe loop (design 004 §2.1): drives the
        // write fence; see quorum.rs for the bookkeeping rules.
        let st = state.clone();
        tokio::spawn(quorum::quorum_task(st));
    }
    loop {
        let (stream, peer) = tokio::select! {
            r = listener.accept() => r?,
            // Graceful shutdown: SIGTERM/SIGINT stop the accept loop, then
            // live connections get a bounded window to finish. What is
            // still open when the window elapses is covered by the
            // engine's disconnect-rollback plus WAL recovery.
            _ = shutdown_signal() => {
                eprintln!("shutdown signal: draining connections (max 10s)");
                graceful_drain(&state).await;
                return Ok(());
            }
        };
        let stream = set_tcp_keepalive(stream);
        state
            .metrics
            .connections_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let state = state.clone();
        // Resource control: hold one slot per live connection. Over the
        // limit the connection is answered with an error and closed —
        // never queued, so a flooding client cannot pin server memory.
        let slot = match &state.conn_slots {
            Some(sem) => match sem.clone().try_acquire_owned() {
                Ok(g) => Some(g),
                Err(_) => {
                    state
                        .metrics
                        .connections_rejected_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let mut s = stream;
                    let msg = Frame::new(
                        proto::RESP_ERROR,
                        err_payload("too many connections; retry later"),
                    );
                    // Plaintext by design: no per-connection challenge
                    // exists yet (the RESP_HELLO challenge is the first
                    // frame of a fully accepted connection), and keyed
                    // clients accept an unencrypted RESP_ERROR before the
                    // hello as a terminal connection-level failure. On a
                    // TLS listener this write is garbage to a TLS client —
                    // its handshake fails either way, which is the loud
                    // outcome we want.
                    let bytes = msg.encode().unwrap_or_default();
                    use tokio::io::AsyncWriteExt;
                    let _ = s.write_all(&bytes).await;
                    continue;
                }
            },
            None => None,
        };
        // TLS handshake AFTER the slot check (a flood cannot consume more
        // handshake budgets than slots) and BEFORE the connection task: a
        // failed handshake counts and drops the socket loudly — there is
        // no protocol-detection fallback to plaintext.
        let stream: tls::BoxConn = match &tls_acceptor {
            None => Box::new(stream),
            Some(acceptor) => {
                match tokio::time::timeout(tls::TLS_HANDSHAKE_TIMEOUT, acceptor.accept(stream))
                    .await
                {
                    Ok(Ok(s)) => Box::new(s),
                    Ok(Err(e)) => {
                        state
                            .metrics
                            .tls_handshake_failures_total
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        eprintln!("tls handshake from {peer} failed: {e}");
                        continue;
                    }
                    Err(_) => {
                        state
                            .metrics
                            .tls_handshake_failures_total
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        eprintln!("tls handshake from {peer} timed out");
                        continue;
                    }
                }
            }
        };
        tokio::spawn(async move {
            state
                .metrics
                .connections_active
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let result = handle_connection(stream, peer.to_string(), state.clone()).await;
            state
                .metrics
                .connections_active
                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            drop(slot);
            if let Err(e) = result {
                eprintln!("connection {peer} closed: {e}");
            }
        });
    }
}

/// Peers accepted from dynamic join registration. The static config list is
/// operator-controlled; this caps what an authenticated joiner can append
/// (each entry costs a per-write connection attempt, so an unbounded list is
/// a write-path DoS).
pub(super) const MAX_DYNAMIC_PEERS: usize = 64;
