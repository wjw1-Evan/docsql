use super::*;

/// REQ_AUTH_USER: verify a username/password pair and resolve the user's
/// privileges. Returns (response frame, identity on success). The PBKDF2
/// derivation runs on the blocking pool; the stored credential and the
/// grants are read under the engine lock on either side of it. Failure
/// accounting (per-source lockout + audit trail) mirrors token auth, and a
/// missing user burns the same derivation so timing reveals nothing.
/// Record one failed authentication from `source_ip`: bump the counter,
/// prune idle sources, append the failure, and audit a lockout event when
/// the threshold is hit. Shared by token and user/password auth so their
/// lockout behavior cannot drift apart.
pub(super) async fn record_auth_failure(state: &Arc<ServerState>, source_ip: &str) {
    state
        .metrics
        .auth_failures_total
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if state.auth_lock_threshold > 0 {
        let mut failures = state.auth_failures.lock().await;
        let now = std::time::Instant::now();
        // Prune idle sources along the way: a scanner leaving one entry per
        // IP would otherwise grow the map forever (entries are only
        // otherwise dropped on that IP's success).
        failures.retain(|_, list| {
            list.retain(|t| now.duration_since(*t) < AUTH_LOCK_WINDOW);
            !list.is_empty()
        });
        let list = failures.entry(source_ip.to_string()).or_default();
        list.push(now);
        if list.len() as u32 == state.auth_lock_threshold {
            querylog::sync_event(
                &state.sync_log,
                "auth",
                source_ip,
                None,
                false,
                Some("lockout: repeated auth failures".into()),
            );
        }
    }
}

pub(super) async fn user_login_frame(
    state: &Arc<ServerState>,
    source_ip: &str,
    payload: &[u8],
) -> (Frame, Option<UserAuth>, u64) {
    let bad = || Frame::new(proto::RESP_ERROR, err_payload("bad username or password"));
    let parsed: std::result::Result<(String, String), String> = (|| {
        let v: serde_json::Value =
            serde_json::from_slice(payload).map_err(|e| format!("user auth: bad payload: {e}"))?;
        let name = v["user"]
            .as_str()
            .ok_or("user auth: expected {\"user\",\"password\"}")?;
        let pw = v["password"]
            .as_str()
            .ok_or("user auth: expected {\"user\",\"password\"}")?;
        Ok((name.trim().to_lowercase(), pw.to_string()))
    })();
    let (name, pw) = match parsed {
        Ok(c) => c,
        Err(m) => return (Frame::new(proto::RESP_ERROR, err_payload(&m)), None, 0),
    };
    if name.is_empty()
        || name.len() > 64
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
    {
        return (bad(), None, 0);
    }
    let stored = {
        let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
        db.user_stored_pw(&name)
    };
    // Server-wide derivation gate: verification is CPU-bound on the
    // blocking pool; queuing excess attempts keeps a distributed auth
    // flood from starving every other blocking task. The WAIT is bounded:
    // without a deadline a flood of parked queue-waiters grew without
    // limit and pushed legitimate logins minutes behind the backlog (the
    // pre-auth frame timeout only covers the socket read, not this gate).
    let permit =
        match tokio::time::timeout(PRE_AUTH_TIMEOUT, state.auth_gate.clone().acquire_owned()).await
        {
            Ok(p) => p.expect("auth gate never closed"),
            Err(_) => {
                return (
                    Frame::new(
                        proto::RESP_ERROR,
                        err_payload("authentication timeout; reconnect"),
                    ),
                    None,
                    0,
                );
            }
        };
    let ok = tokio::task::spawn_blocking(move || match stored {
        Some(s) => docsql_core::kdf::StoredPw::parse(&s)
            .map(|p| p.verify(&pw))
            .unwrap_or(false),
        None => {
            let decoy = docsql_core::kdf::hash_password("x", &[0u8; 16]);
            if let Some(p) = docsql_core::kdf::StoredPw::parse(&decoy) {
                let _ = p.verify(&pw);
            }
            false
        }
    })
    .await
    .unwrap_or(false);
    drop(permit);
    if !ok {
        record_auth_failure(state, source_ip).await;
        querylog::sync_event(
            &state.sync_log,
            "auth",
            source_ip,
            None,
            false,
            Some("user login failed".into()),
        );
        return (bad(), None, 0);
    }
    let (grants, grants_epoch_now) = {
        let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
        // Sample the grants epoch INSIDE the critical section that resolves
        // the grants: a REVOKE applies under this lock and bumps the epoch
        // only after releasing it, so the "old grants + new epoch" pair
        // (which would skip every per-frame refresh forever) cannot be
        // assembled here — seeing the new epoch implies the revoke already
        // landed in the resolved grants.
        let g = docsql_core::useradmin::resolve_grants(&mut db, &name)
            .ok()
            .flatten();
        (
            g,
            state.grants_epoch.load(std::sync::atomic::Ordering::SeqCst),
        )
    };
    match grants {
        Some(g) => {
            state.auth_failures.lock().await.remove(source_ip);
            querylog::sync_event(
                &state.sync_log,
                "auth",
                source_ip,
                None,
                true,
                Some(format!("user {name} logged in")),
            );
            (
                Frame::new(
                    proto::RESP_AFFECTED,
                    format!("ok(user:{name})").into_bytes(),
                ),
                Some(UserAuth { name, grants: g }),
                grants_epoch_now,
            )
        }
        // Raced with DROP USER between verify and resolve.
        None => (bad(), None, 0),
    }
}

pub async fn handle_connection(
    stream: tls::BoxConn,
    peer: String,
    state: Arc<ServerState>,
) -> std::io::Result<()> {
    // Lock-based split (plain TCP used into_split's lock-free halves):
    // TLS cannot hand out independent socket halves, and one shared
    // transport type keeps a single frame loop. The bi-lock arbitrates
    // reader-vs-writer at nanosecond scale against syscalls + fsync.
    let (rd, mut wr) = tokio::io::split(stream);
    let mut conn = Conn {
        stream: rd,
        buf: Vec::new(),
        metrics: state.metrics.clone(),
    };
    // Depth 32, not the historical 256: large frames (4 MB dump/catch-up
    // chunks) queued behind a stalled socket used to buffer up to
    // 256 × 4 MB ≈ 1 GB per connection before the writer's 10 s stall
    // timeout tore it down. 32 keeps the same streaming throughput while
    // bounding the buffer.
    let (tx, mut rx) = mpsc::channel::<Frame>(32);
    let key = state.transport_key;
    let wmetrics = state.metrics.clone();
    // Per-connection challenge for keyed transports: 16 random bytes sent
    // as the FIRST frame (plaintext RESP_HELLO) and folded into every
    // frame's AAD in both directions. Without it a captured encrypted
    // session could be replayed byte-for-byte onto a fresh connection —
    // the static PSK would decrypt it happily and the per-connection
    // ReplayGuard would even see a perfectly increasing counter.
    let conn_challenge: [u8; 16] = rand::random();

    // Writer task: serializes responses (sealing when a transport key is
    // configured). The challenge goes out IMMEDIATELY — before any client
    // frame — because keyed clients block on the hello before they send
    // anything (deferring it to the first response would deadlock both
    // sides).
    let writer = tokio::spawn(async move {
        if key.is_some() {
            if let Ok(b) = Frame::new(proto::RESP_HELLO, conn_challenge.to_vec()).encode() {
                wmetrics
                    .bytes_out_total
                    .fetch_add(b.len() as u64, std::sync::atomic::Ordering::Relaxed);
                let wrote = tokio::time::timeout(IO_TIMEOUT, async {
                    wr.write_all(&b).await?;
                    wr.flush().await
                })
                .await;
                if !matches!(wrote, Ok(Ok(()))) {
                    return;
                }
            }
        }
        while let Some(f) = rx.recv().await {
            let f = if let Some(k) = key {
                let flags = f.flags | crypto::FLAG_ENCRYPTED;
                // The transmitted header + the connection challenge are the
                // tag's associated data: a MITM cannot flip a flag, and a
                // frame captured on another connection cannot replay here.
                // Nonce exhaustion (>2^32 seals from this process) closes
                // the connection — the node must be restarted.
                let payload =
                    match crypto::seal(&k, f.frame_type, flags, &f.payload, &conn_challenge) {
                        Ok(p) => p,
                        Err(e) => {
                            eprintln!("docsql-crypto: {e}; closing the connection");
                            break;
                        }
                    };
                Frame {
                    flags,
                    payload,
                    ..f
                }
            } else {
                f
            };
            let bytes = match f.encode() {
                Ok(b) => b,
                // A frame over the 64MiB wire cap (giant RESP_META/SESSIONS)
                // must answer the request, not vanish — silently dropping
                // it left the peer waiting until its idle timeout.
                Err(_) => {
                    let err =
                        Frame::new(proto::RESP_ERROR, b"response too large to encode".to_vec());
                    let _ = wr.write_all(&err.encode().unwrap_or_default()).await;
                    let _ = wr.flush().await;
                    break;
                }
            };
            wmetrics
                .bytes_out_total
                .fetch_add(bytes.len() as u64, std::sync::atomic::Ordering::Relaxed);
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
    // Token authentication state (distinguishes a token-authed Client from
    // the legacy anonymous Client) and the REQ_AUTH_USER identity, if any.
    let mut token_authed = false;
    let mut user: Option<UserAuth> = None;
    let mut user_epoch = 0u64;
    // Inbound replay guard for keyed transports (see crypto::ReplayGuard):
    // per connection, validated before every decrypt.
    let mut inbound_replay = crypto::ReplayGuard::default();
    // Bounded pre-auth PING budget (see the REQ_PING arm).
    let mut pre_auth_pings = 0u32;
    // Set when the connection must close after the current response (the
    // pre-auth PING budget): a slowloris must not hold its slot forever.
    let mut close_after = false;
    // Server-side prepared statements (REQ_PREPARE/REQ_EXECUTE): handle →
    // template SQL with `?` placeholders. Per connection, dies with it.
    let mut prepared: std::collections::HashMap<u64, String> = std::collections::HashMap::new();
    let mut next_stmt_handle: u64 = 1;
    // T-SQL session state (@variables, @@ROWCOUNT) lives with the
    // connection, exactly like prepared handles: DECLARE in one REQ_SQL
    // frame is visible to the next. Replication/replay traffic never
    // routes through it (that text is already literal).
    let mut tsql_session = docsql_core::tsql_batch::TsqlSession::new();
    // Legacy anonymous access is judged at connection start: connections
    // that were legitimate when they opened (user-less node) keep working
    // — the operator bootstrap (CREATE USER + GRANT over one session)
    // must not lock itself out mid-flight — while NEW anonymous connections
    // close once users exist.
    let anon_open = !state.has_users.load(std::sync::atomic::Ordering::SeqCst);
    // Auth-failure lockout: a source past the threshold is rejected unread
    // until the lockout elapses (identity-authentication failure handling).
    let source_ip = peer.rsplit_once(':').map(|(ip, _)| ip).unwrap_or(&peer);
    // Activity-monitor registration: visible in REQ_SESSIONS from here
    // until the cleanup after the frame loop. The initial identity label
    // matches the pre-AUTH role (auth-disabled nodes open as clients).
    let session = Arc::new(SessionSlot::new(&peer));
    if role == ConnRole::Client && state.auth_token.is_none() {
        session.set_identity("anonymous");
    }
    state.sessions.lock().await.insert(conn_id, session.clone());
    let result = async {
        loop {
            // REQ_KILL observed between iterations (e.g. set while a
            // statement was finishing): close before reading another frame.
            if session.is_killed() {
                let _ = tx
                    .send(Frame::new(
                        proto::RESP_ERROR,
                        err_payload("connection killed by administrator"),
                    ))
                    .await;
                break;
            }
            // Captured by value: the async block stays pinned across the
            // frame handling below, where `role` is reassigned by AUTH.
            let unauthed = role == ConnRole::Unauthed;
            let idle_timeout = state.idle_timeout;
            let read_outcome = async {
                if unauthed {
                    // Pre-auth budget: an unauthenticated socket gets one
                    // bounded window to say AUTH — it must not pin its task
                    // and read buffer indefinitely.
                    match tokio::time::timeout(PRE_AUTH_TIMEOUT, conn.read_frame()).await {
                        Ok(f) => f,
                        Err(_) => {
                            let _ = tx
                                .send(Frame::new(
                                    proto::RESP_ERROR,
                                    err_payload("authentication timeout; reconnect"),
                                ))
                                .await;
                            Ok(None)
                        }
                    }
                } else {
                    match idle_timeout {
                        Some(t) => match tokio::time::timeout(t, conn.read_frame()).await {
                            Ok(f) => f,
                            Err(_) => {
                                let _ = tx
                                    .send(Frame::new(
                                        proto::RESP_ERROR,
                                        err_payload("idle session timed out; reconnect"),
                                    ))
                                    .await;
                                Ok(None)
                            }
                        },
                        None => conn.read_frame().await,
                    }
                }
            };
            let mut kill_waiter = session.kill_watcher();
            tokio::pin!(read_outcome);
            let frame = tokio::select! {
                r = &mut read_outcome => r,
                // KILL on a blocked read: an idle session must not wait out
                // its idle/pre-auth budget to die. Watch semantics make the
                // request visible even when it raced the arm registration.
                changed = kill_waiter.changed() => {
                    let _ = changed;
                    if session.is_killed() {
                        let _ = tx
                            .send(Frame::new(
                                proto::RESP_ERROR,
                                err_payload("connection killed by administrator"),
                            ))
                            .await;
                        Ok(None)
                    } else {
                        // Spurious wake (no kill request): keep waiting.
                        (&mut read_outcome).await
                    }
                }
            };
            let Some(mut frame) = frame? else { break };
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
                if locked && matches!(frame.frame_type, proto::REQ_AUTH | proto::REQ_AUTH_USER) {
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
                if let Err(e) = inbound_replay.check(&frame.payload) {
                    let _ = tx
                        .send(Frame::new(proto::RESP_ERROR, err_payload(&e)))
                        .await;
                    break;
                }
                match crypto::open(
                    &k,
                    frame.frame_type,
                    frame.flags,
                    &frame.payload,
                    &conn_challenge,
                ) {
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
            // Trustworthy replication bit: a flag is not a credential. With a
            // cluster token only Peer connections qualify; without one
            // (legacy mode) a client-token connection, or a token-less and
            // user-less deployment, may replicate. A user login or read-only
            // token setting the bit used to skip the replica read-only gate,
            // the system-table guard and the transaction-owner gate.
            let replication_ok = frame.flags & FLAG_REPLICATION != 0
                && (role == ConnRole::Peer
                    || (state.cluster_token.is_none()
                        && (token_authed
                            || (state.auth_token.is_none()
                                && !state.has_users.load(std::sync::atomic::Ordering::SeqCst)))));
            // Least-privilege gate: a read-only-token connection may read
            // and subscribe, but every durable write is refused here so
            // no handler can accidentally apply one.
            if role == ConnRole::ReadOnly {
                let sub = if frame.frame_type == proto::REQ_PUBSUB {
                    serde_json::from_slice::<serde_json::Value>(&frame.payload)
                        .ok()
                        .and_then(|v| v["sub"].as_str().map(String::from))
                } else {
                    None
                };
                // Transaction control is state-changing too: BEGIN opens the
                // single global transaction (blocking every other writer),
                // SAVEPOINT deep-copies the whole database. A read-only
                // token must not be able to start either.
                let tx_control = |sql: &str| {
                    docsql_core::engine::Database::parse_classified(sql)
                        .map(|p| !matches!(p.tx, docsql_core::engine::TxControl::None))
                        .unwrap_or(true)
                };
                let writes = frame.frame_type == proto::REQ_PUBLISH
                    || frame.frame_type == proto::REQ_PROMOTE
                    || sub.as_deref() == Some("trim")
                    || (frame.frame_type == proto::REQ_SQL && {
                        let sql = proto::decode_sql(&frame.payload).unwrap_or_default();
                        docsql_core::engine::Database::is_write_statement(&sql) || tx_control(&sql)
                    })
                    || (frame.frame_type == proto::REQ_EXECUTE && {
                        // The prepared template decides the statement kind —
                        // bound params cannot turn a SELECT into a write.
                        // REQ_EXECUTE must be classified here like REQ_SQL,
                        // or a read-only token prepares a write once and
                        // executes it through the unguarded frame type.
                        serde_json::from_slice::<serde_json::Value>(&frame.payload)
                            .ok()
                            .and_then(|v| v.get("handle").and_then(|h| h.as_u64()))
                            .and_then(|h| prepared.get(&h))
                            .is_some_and(|tpl| {
                                docsql_core::engine::Database::is_write_statement(tpl)
                                    || tx_control(tpl)
                            })
                    });
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
            // Refresh a user connection's grants when user/role state has
            // changed since its last frame. Runs BEFORE the frame gates, so
            // a revocation applies to PUBLISH/TRIM/backup/PROMOTE too — and
            // a dropped account (refresh → None) falls into the anonymous
            // gate below instead of executing as an identity-less legacy
            // session (a one-statement full-privilege window, or worse:
            // stale grants kept PUBLISH/backup alive on non-SQL frames).
            if user.is_some()
                && state.grants_epoch.load(std::sync::atomic::Ordering::SeqCst) != user_epoch
            {
                let name = user.as_ref().expect("checked just above").name.clone();
                // Sample the epoch INSIDE the same critical section as the
                // resolve — a REVOKE committing between the resolve and an
                // outside load would pair pre-revoke grants with a
                // post-revoke epoch and skip every later refresh for this
                // session (same race the login path already closes).
                let refreshed = {
                    let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
                    let grants = docsql_core::useradmin::resolve_grants(&mut db, &name)
                        .ok()
                        .flatten();
                    let epoch = state.grants_epoch.load(std::sync::atomic::Ordering::SeqCst);
                    grants.map(|g| (UserAuth { name, grants: g }, epoch))
                };
                if refreshed.is_none() {
                    // The account was dropped (or its grants became
                    // unresolvable): close instead of degrading to an
                    // anonymous session. In the token-less compat mode an
                    // anonymous slot can still answer node-only frames, and
                    // a dropped account must not keep a live socket riding
                    // on that exemption.
                    let _ = tx
                        .send(Frame::new(
                            proto::RESP_ERROR,
                            err_payload("session closed: this account no longer exists"),
                        ))
                        .await;
                    break;
                }
                let (auth, epoch) = refreshed.expect("none case breaks above before reaching here");
                user = Some(auth);
                user_epoch = epoch;
            }
            // Once at least one database user exists, legacy anonymous
            // (token-less) access closes: every client-facing data frame
            // then requires the client token (admin) or a REQ_AUTH_USER
            // login. The replication channel (node-only frames below) stays
            // answerable to token-less peers — the documented compatibility
            // mode when no cluster token is configured — but a flagged
            // REQ_SQL/REQ_EXECUTE is *not* replication (see `replication_ok`
            // below): the old version exempted anything carrying
            // FLAG_REPLICATION and omitted META/STATUS/LOGS/PREPARE/
            // SUBSCRIBE, so one bit let an anonymous socket execute SQL,
            // dump the database, freeze writes or restore backups.
            // A flagged REQ_STATUS is the peer status probe (compat-mode joins
            // need it before any token exists); an unflagged one is a client
            // asking for topology and stays gated.
            let peer_probe =
                frame.frame_type == proto::REQ_STATUS && frame.flags & FLAG_REPLICATION != 0;
            // The data plane closes with the anonymous era; the replication
            // channel (REQ_SQL_SEQ/REQ_DIGEST/REQ_SYNC/…) deliberately does
            // NOT — token-less peers' AUTH is vacuous and join/fanout depend
            // on that compat mode (pinned by the e2e suite). Operators who
            // create users on a token-less node accept the open replication
            // channel; configuring any token closes it.
            if role == ConnRole::Client
                && !token_authed
                && user.is_none()
                && !anon_open
                && !peer_probe
                && !is_node_frame(frame.frame_type)
                && matches!(
                    frame.frame_type,
                    proto::REQ_SQL
                        | proto::REQ_EXECUTE
                        | proto::REQ_PREPARE
                        | proto::REQ_PUBLISH
                        | proto::REQ_PUBSUB
                        | proto::REQ_PROMOTE
                        | proto::REQ_BACKUP
                        | proto::REQ_META
                        | proto::REQ_STATUS
                        | proto::REQ_LOGS
                        | proto::REQ_SUBSCRIBE
                        | proto::REQ_PSUBSCRIBE
                        | proto::REQ_SESSIONS
                        | proto::REQ_KILL
                        | proto::REQ_SESSION_RESET
                        | proto::REQ_UNSUBSCRIBE
                        | proto::REQ_PUNSUBSCRIBE
                        | proto::REQ_CLOSE_STMT
                )
            {
                let _ = tx
                    .send(Frame::new(
                        proto::RESP_ERROR,
                        err_payload(
                            "authentication required: this node has user accounts \
                             (use the client token or a username/password login)",
                        ),
                    ))
                    .await;
                continue;
            }
            // The replication channel is for nodes. Frames only peers speak
            // (sequenced writes, catch-up, digests, join sync/hold) are
            // refused to user logins and read-only tokens — including the
            // token-less compatibility mode, where "any authenticated
            // connection may replicate" must not widen into "any connection
            // sheds its grants (or its replica's read-only posture) by
            // setting the replication flag". Peers authenticate with the
            // cluster/client token and carry no user, so real replication
            // is unaffected.
            if frame.flags & FLAG_REPLICATION != 0
                && matches!(
                    frame.frame_type,
                    proto::REQ_SQL_SEQ
                        | proto::REQ_CATCHUP
                        | proto::REQ_DIGEST
                        | proto::REQ_SYNC
                        | proto::REQ_HOLD
                        | proto::REQ_RELEASE
                )
                && (user.is_some() || role == ConnRole::ReadOnly)
            {
                let _ = tx
                    .send(Frame::new(
                        proto::RESP_ERROR,
                        err_payload("replication frames require node (token) credentials"),
                    ))
                    .await;
                continue;
            }
            // Arbiter mode (design 004 §4): this process holds no data and
            // exists to be seen. Status probes (and the auth/ping that
            // carries them) are the entire surface — every other frame
            // refuses, so a voting member can never be mistaken for a
            // data node.
            if state.arbiter
                && !matches!(
                    frame.frame_type,
                    proto::REQ_AUTH | proto::REQ_PING | proto::REQ_STATUS
                )
            {
                let _ = tx
                    .send(Frame::new(
                        proto::RESP_ERROR,
                        err_payload("arbiter: status probes only (no data plane)"),
                    ))
                    .await;
                continue;
            }
            // None = the handler already sent everything (subscribe
            // confirmation + replay) straight through the writer.
            let resp = match frame.frame_type {
                // PING is a keep-alive for authenticated sessions, but a
                // bounded number is allowed pre-auth: the web console's
                // cluster probe pings a node BEFORE sending credentials
                // (reachability first, auth second). Unbounded pre-auth
                // pings would let a socket hold its task, buffer and
                // connection slot forever — hence the counter, not the
                // blanket refusal that broke the probe. Past the budget the
                // connection CLOSES: replying an error and continuing let a
                // slowloris hold slots indefinitely (the per-frame
                // pre-auth timeout never expires a live socket).
                proto::REQ_PING if !authed => {
                    pre_auth_pings += 1;
                    if pre_auth_pings > 4 {
                        // Budget exhausted: one last error frame, then the
                        // connection closes (close_after = true).
                        close_after = true;
                        Some(Frame::new(
                            proto::RESP_ERROR,
                            err_payload("too many pre-auth pings; reconnect and authenticate"),
                        ))
                    } else {
                        Some(Frame::new(proto::RESP_PONG, vec![]))
                    }
                }
                proto::REQ_PING => Some(Frame::new(proto::RESP_PONG, vec![])),
                proto::REQ_AUTH => {
                    // Payload is the raw token. Priority: cluster token
                    // (peer node), client token (full client), read-only
                    // token (least-privilege client). With no tokens
                    // configured every AUTH succeeds (auth disabled).
                    // Failures are counted per source IP; past the
                    // threshold the source is locked out for a window.
                    //
                    // One identity per connection, same rule REQ_AUTH_USER
                    // already enforces: re-AUTH over a session holding a
                    // user/token kept the OLD user's grants alive under the
                    // NEW role (a mixed session — no escalation, since each
                    // gate re-checks its own identity, but never a clean
                    // state either).
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
                        token_authed = true;
                        // Re-AUTH is the documented mid-session credential
                        // switch (CLI `auth <token>;`): the token role
                        // REPLACES any user identity instead of stacking on
                        // it — a user login's grants surviving under a token
                        // role was a mixed session whose every gate had to
                        // remember which identity it checked.
                        user = None;
                        session.set_identity("cluster peer");
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
                            user = None; // the token/anonymous role replaces the user identity
                            if client_match {
                                token_authed = true;
                                session.set_identity("client token");
                            } else {
                                session.set_identity("anonymous");
                            }
                            state.auth_failures.lock().await.remove(source_ip);
                            if state.auth_token.is_some() {
                                audit(true, "client token accepted", &state);
                            }
                            Some(Frame::new(proto::RESP_AFFECTED, b"ok".to_vec()))
                        } else if read_match {
                            role = ConnRole::ReadOnly;
                            token_authed = true;
                            user = None; // same replacement rule
                            session.set_identity("read-only token");
                            state.auth_failures.lock().await.remove(source_ip);
                            audit(true, "read token accepted", &state);
                            Some(Frame::new(proto::RESP_AFFECTED, b"ok(read-only)".to_vec()))
                        } else {
                            // Audit every rejected attempt, not just the
                            // lockout trip: single/under-threshold failures
                            // were invisible in the sync log (REQ_LOGS auth
                            // trail), while every user-login failure is
                            // recorded — same baseline rule for tokens.
                            audit(false, "token rejected", &state);
                            record_auth_failure(&state, source_ip).await;
                            Some(Frame::new(proto::RESP_ERROR, err_payload("bad token")))
                        }
                    }
                }
                proto::REQ_AUTH_USER if role != ConnRole::Peer => {
                    // Username/password login (JSON {"user","password"}):
                    // binds a user identity with resolved privileges to
                    // this connection. Peer connections never authenticate
                    // as users.
                    if token_authed || user.is_some() {
                        Some(Frame::new(
                            proto::RESP_ERROR,
                            err_payload("already authenticated; reconnect to switch identity"),
                        ))
                    } else {
                        let (f, u, epoch) =
                            user_login_frame(&state, source_ip, &frame.payload).await;
                        if let Some(u) = u {
                            role = ConnRole::Client;
                            session.set_identity(&format!("user {}", u.name));
                            // Locked to the grants the login resolved (see
                            // user_login_frame): the per-frame refresh then
                            // catches any REVOKE that lands after this point.
                            user_epoch = epoch;
                            user = Some(u);
                        }
                        Some(f)
                    }
                }
                proto::REQ_PROMOTE if authed => {
                    if user.as_ref().is_some_and(|u| !u.grants.admin) {
                        Some(Frame::new(
                            proto::RESP_ERROR,
                            err_payload("PROMOTE requires the admin role"),
                        ))
                    } else if let Some(denial) = quorum_write_denial(&state) {
                        // A quorum-lost node promoting itself would trade a
                        // fenced minority for an unguarded writer — lift the
                        // fence first (design 004 §2.2).
                        Some(Frame::new(proto::RESP_ERROR, err_payload(&denial)))
                    } else {
                        // Failover: leave replica mode. Mirrors the former KV
                        // PROMOTE command — clears read-only and detaches the
                        // replication upstream so this node owns its writes.
                        state
                            .read_only
                            .store(false, std::sync::atomic::Ordering::SeqCst);
                        *state.replicate_to.lock().await = None;
                        // Role epoch (design 004 §4): the promotion is
                        // visible to stale primaries, which demote on sight.
                        state
                            .primary_epoch
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        // A manual PROMOTE re-arms auto-promotion: without
                        // this, a node demoted (and frozen) by the probe
                        // loop could never auto-promote again until restart
                        // — the quorum comment promises the unfreeze.
                        state
                            .auto_promote_frozen
                            .store(false, std::sync::atomic::Ordering::SeqCst);
                        querylog::sync_event(&state.sync_log, "promote", "", None, true, None);
                        // Failover changes who owns writes cluster-wide: the
                        // trail must say who pulled the trigger.
                        querylog::record(
                            &state,
                            &peer,
                            &format!("PROMOTE{}", audit_identity(user.as_ref())),
                            0.0,
                            &Frame::new(proto::RESP_AFFECTED, b"promoted".to_vec()),
                            false,
                        );
                        Some(Frame::new(proto::RESP_AFFECTED, b"promoted".to_vec()))
                    }
                }
                proto::REQ_STATUS if authed => {
                    if state.arbiter {
                        // Minimal identity payload: voters answer "here" and
                        // nothing else (design 004 §4).
                        Some(Frame::new(
                            proto::RESP_STATUS,
                            serde_json::to_vec(&serde_json::json!({
                                "name": "docsql-arbiter",
                                "node_id": state
                                    .advertise
                                    .clone()
                                    .unwrap_or_else(|| state.listen.clone()),
                                "arbiter": true,
                            }))
                            .unwrap_or_default(),
                        ))
                    } else {
                        // Topology, paths and journal windows are node-operational
                        // detail: user logins need the admin role for it (token
                        // connections are the operator's own credential).
                        if role == ConnRole::ReadOnly
                            || user.as_ref().is_some_and(|u| !u.grants.admin)
                        {
                            Some(Frame::new(
                                proto::RESP_ERROR,
                                err_payload("cluster status requires the admin role"),
                            ))
                        } else {
                            // Read-only node report for cluster monitoring; not an SQL
                            // statement, so it bypasses the query log.
                            let payload = serde_json::to_vec(&status_payload(&state).await)
                                .unwrap_or_else(|_| b"{}".to_vec());
                            Some(Frame::new(proto::RESP_STATUS, payload))
                        }
                    }
                }
                proto::REQ_SESSION_RESET if authed => {
                    // Pooled drivers reset the T-SQL batch session when a
                    // physical connection is re-borrowed (see proto.rs):
                    // the alternative — carrying the previous borrower's
                    // @variables/@@IDENTITY across the pool — surfaces as
                    // duplicate-DECLARE errors and silently wrong identity
                    // values.
                    tsql_session = docsql_core::tsql_batch::TsqlSession::new();
                    Some(Frame::new(proto::RESP_AFFECTED, b"session reset".to_vec()))
                }
                proto::REQ_SESSIONS if authed => {
                    // Activity monitor: seeing other connections' current
                    // statements is admin-only (statement texts carry data
                    // values; the audit-log rule applies).
                    if role == ConnRole::ReadOnly || user.as_ref().is_some_and(|u| !u.grants.admin)
                    {
                        Some(Frame::new(
                            proto::RESP_ERROR,
                            err_payload("the session list requires the admin role"),
                        ))
                    } else {
                        let payload = serde_json::to_vec(&sessions_payload(&state))
                            .unwrap_or_else(|_| br#"{"columns":[],"rows":[]}"#.to_vec());
                        Some(Frame::new(proto::RESP_ROWS, payload))
                    }
                }
                proto::REQ_KILL if authed => {
                    // Admin-only kill switch (see sessions_payload). Killing
                    // your OWN connection is refused loudly — the operator
                    // should disconnect instead, and the console must not be
                    // able to sever the very leg it is managing through.
                    if role == ConnRole::ReadOnly || user.as_ref().is_some_and(|u| !u.grants.admin)
                    {
                        Some(Frame::new(
                            proto::RESP_ERROR,
                            err_payload("killing connections requires the admin role"),
                        ))
                    } else if frame.payload.len() != 8 {
                        Some(Frame::new(
                            proto::RESP_ERROR,
                            err_payload("kill: payload must be exactly one u64 connection id"),
                        ))
                    } else {
                        let id = u64::from_le_bytes(frame.payload[..8].try_into().unwrap());
                        if id == conn_id {
                            Some(Frame::new(
                                proto::RESP_ERROR,
                                err_payload(
                                    "refusing to kill your own connection; disconnect instead",
                                ),
                            ))
                        } else {
                            let sessions = state.sessions.lock().await;
                            match sessions.get(&id) {
                                Some(slot) => {
                                    slot.request_kill();
                                    Some(Frame::new(
                                        proto::RESP_AFFECTED,
                                        b"kill scheduled".to_vec(),
                                    ))
                                }
                                None => Some(Frame::new(
                                    proto::RESP_ERROR,
                                    err_payload(&format!(
                                        "connection {id} does not exist (it may have closed; refresh)"
                                    )),
                                )),
                            }
                        }
                    }
                }
                proto::REQ_LOGS if authed => {
                    // The statement audit log carries other users' data
                    // values (only PASSWORD literals are redacted): same
                    // admin rule as reading the user tables.
                    if role == ConnRole::ReadOnly || user.as_ref().is_some_and(|u| !u.grants.admin)
                    {
                        Some(Frame::new(
                            proto::RESP_ERROR,
                            err_payload("the audit log requires the admin role"),
                        ))
                    } else {
                        // Recent statement-audit entries + sync events for the
                        // console's logs page; like REQ_STATUS this is not an SQL
                        // statement, so it bypasses the query log.
                        let limit = querylog::parse_logs_limit(&frame.payload);
                        Some(Frame::new(
                            proto::RESP_LOGS,
                            querylog::logs_payload(&state.query_log, &state.sync_log, limit),
                        ))
                    }
                }
                proto::REQ_META if authed => {
                    // Full catalog shape (tables, columns, row counts, paths):
                    // admin-only for user logins, same boundary as the audit
                    // log above.
                    if role == ConnRole::ReadOnly || user.as_ref().is_some_and(|u| !u.grants.admin)
                    {
                        Some(Frame::new(
                            proto::RESP_ERROR,
                            err_payload("catalog metadata requires the admin role"),
                        ))
                    } else {
                        // Object-explorer metadata for the console's node
                        // switching: the same core::meta walk the console runs
                        // on its embedded engine, so remote nodes report
                        // shape-identical /api/meta payloads. Not an SQL
                        // statement, so it bypasses the query log.
                        let db = state.db.read().unwrap_or_else(|p| p.into_inner());
                        let meta = docsql_core::meta::build_meta(
                            &db,
                            &state.db_path,
                            state.started,
                            env!("CARGO_PKG_VERSION"),
                        );
                        Some(Frame::new(
                            proto::RESP_META,
                            docsql_core::json::to_string(&meta).into_bytes(),
                        ))
                    }
                }
                proto::REQ_SQL if authed => {
                    // Grants were refreshed above, before the frame gates.
                    let started = std::time::Instant::now();
                    // Full statement text: the query log truncates its own
                    // copy (querylog::record), and cutting here would
                    // silently garble every statement longer than 512 chars
                    // (e.g. document INSERTs proxied from the web console's
                    // node switching). The frame itself is length-capped on
                    // read.
                    let sql = proto::decode_sql(&frame.payload).unwrap_or_default();
                    // Activity monitor: the statement runs synchronously
                    // below, so "running" is exactly this span.
                    session.start_statement(&sql);
                    let mut logged = false;
                    // Post-row-filter text when a filter rewrote the
                    // statement (None = record the user's original).
                    let mut logged_effective: Option<String> = None;
                    let resp = match querylog::try_serve_log_view(
                        &sql,
                        &state,
                        user.as_ref(),
                        role == ConnRole::ReadOnly,
                    ) {
                        // 读日志的查询本身不写日志(避免读日志刷日志)。
                        Some(f) => f,
                        None => {
                            // The docsql_pubsub view maps onto the system
                            // table, which the guard below would otherwise
                            // reject; rewritten statements execute with the
                            // system-table gate open. Replication frames are
                            // never rewritten.
                            // Replication semantics require a node-credentialed
                            // connection, not merely the flag (see
                            // `replication_ok` above).
                            let is_replication = replication_ok;
                            // Join intake: while bootstrapping, replication
                            // writes queue and are acknowledged on the spot
                            // (see ServerState::sync_queue); they replay
                            // after the snapshot lands.
                            let queued = if is_replication
                                && docsql_core::engine::Database::is_write_statement(&sql)
                            {
                                gate_enqueue(&state, None, &sql, false).await
                            } else {
                                None
                            };
                            match queued {
                                Some(f) => {
                                    // Audited later by the drain replay.
                                    f
                                }
                                None if !is_replication
                                    && docsql_core::tsql_batch::needs_interpretation(&sql) =>
                                {
                                    // T-SQL batch/variables/control flow:
                                    // interpret statement-by-statement; each
                                    // rendered statement goes through the
                                    // same execute_sql pipeline (auth, log,
                                    // fan-out, deadline) as a plain one.
                                    tsql_session.ctx = docsql_core::tsql_batch::SessionContext {
                                        user: user.as_ref().map(|u| u.name.clone()),
                                        app_name: None,
                                        host: None,
                                    };
                                    let deadline = state
                                        .statement_timeout
                                        .map(|t| std::time::Instant::now() + t);
                                    let mut exec = BatchPipeExec {
                                        state: &state,
                                        user: user.as_ref(),
                                        conn: Some(conn_id),
                                        deadline,
                                        peer: peer.clone(),
                                        stmt_identity: None,
                                    };
                                    match tsql_session.run_batch(&sql, &mut exec).await {
                                        Ok(out) => {
                                            for msg in tsql_session.take_prints() {
                                                eprintln!("conn {conn_id} PRINT: {msg}");
                                            }
                                            exec_result_frame(out)
                                        }
                                        Err(e) => {
                                            // Drain PRINTs on the failure
                                            // path too: batch scripts report
                                            // progress before failing, and
                                            // the CLI already keeps them —
                                            // dropping them here hid exactly
                                            // the output that explains the
                                            // failure.
                                            for msg in tsql_session.take_prints() {
                                                eprintln!("conn {conn_id} PRINT: {msg}");
                                            }
                                            Frame::new(
                                                proto::RESP_ERROR,
                                                err_payload(&e.to_string()),
                                            )
                                        }
                                    }
                                }
                                None => {
                                    // A user-created table named docsql_pubsub
                                    // must not be shadowed by the view (same
                                    // rule as docsql_log): its INSERTs would
                                    // reach the real table while SELECTs
                                    // silently swapped in message rows. The
                                    // catalog decides which one exists.
                                    let user_table_wins = !is_replication && {
                                        state
                                            .db
                                            .read()
                                            .unwrap_or_else(|p| p.into_inner())
                                            .table_exists_ci("docsql_pubsub")
                                    };
                                    let (effective, allow_system) =
                                        if is_replication || user_table_wins {
                                            (sql.clone(), false)
                                        } else {
                                            match pubsub::try_rewrite_pubsub_view(&sql) {
                                                Some(rewritten) => (rewritten, true),
                                                None => (sql.clone(), false),
                                            }
                                        };
                                    logged = true;
                                    // Client statements get the wall-clock
                                    // budget; replication apply must run
                                    // unbounded (peers apply what the
                                    // origin confirmed, at their own speed).
                                    let stmt_deadline = if is_replication {
                                        None
                                    } else {
                                        state
                                            .statement_timeout
                                            .map(|t| std::time::Instant::now() + t)
                                    };
                                    // MVCC stage A: classified read-only
                                    // SELECTs run on the read tier (multiple
                                    // readers share the engine); everything
                                    // else keeps the exclusive write tier.
                                    // No open transaction may exist (a tx
                                    // owner's reads must see its own staged
                                    // writes, which the read tier cannot),
                                    // and the statement must pass the SAME
                                    // authorization the write tier applies
                                    // (user grants / fail-closed read
                                    // targets) — anything unauthorized or
                                    // unclassifiable falls through to the
                                    // original path and its error.
                                    let read_eligible = !is_replication
                                        && classify_read_eligible(
                                            &state,
                                            &effective,
                                            user.as_ref(),
                                        );
                                    if read_eligible {
                                        let (resp, rowcount) = execute_read_sql(
                                            &state,
                                            &effective,
                                            stmt_deadline,
                                            user.as_ref(),
                                            &mut logged_effective,
                                        )
                                        .await;
                                        // Same session bookkeeping the write
                                        // branch below applies: the read tier
                                        // owns @@ERROR/@@ROWCOUNT for its
                                        // statements too (it used to leave
                                        // the last interpreted statement's
                                        // values in place).
                                        if resp.frame_type == proto::RESP_ERROR {
                                            tsql_session.note_error(
                                                docsql_core::tsql_batch::error_code_from_text(
                                                    &String::from_utf8_lossy(&resp.payload),
                                                ),
                                            );
                                        } else {
                                            tsql_session.note_error(0);
                                        }
                                        tsql_session.note_rowcount(rowcount);
                                        resp
                                    } else {
                                        let (resp, identity) = execute_sql_with_identity(
                                            &state,
                                            &effective,
                                            allow_system,
                                            is_replication,
                                            if is_replication { None } else { Some(conn_id) },
                                            false,
                                            None,
                                            // The user identity survives the
                                            // replication flag: peers carry no
                                            // user and are unaffected, but a
                                            // user connection must not drop its
                                            // grants (or the admin-only user
                                            // tables' write gate) by setting one
                                            // header bit.
                                            user.as_ref(),
                                            stmt_deadline,
                                            &mut logged_effective,
                                        )
                                        .await;
                                        // The fast path bypasses the T-SQL
                                        // session, but the session owns the
                                        // connection-scoped @@IDENTITY and
                                        // @@ERROR: feed it what just ran so a
                                        // later batch sees this statement's
                                        // effects.
                                        if !is_replication {
                                            if docsql_core::tsql_batch::is_insert_statement(
                                                &effective,
                                            ) {
                                                tsql_session.note_identity(
                                                    identity.map(docsql_core::Value::Int),
                                                );
                                            }
                                            if resp.frame_type == proto::RESP_ERROR {
                                                tsql_session.note_error(
                                                    docsql_core::tsql_batch::error_code_from_text(
                                                        &String::from_utf8_lossy(&resp.payload),
                                                    ),
                                                );
                                            } else {
                                                // T-SQL:每条语句都刷新
                                                // @@ERROR,成功归零 —— 不然
                                                // 批内失败后紧跟的快路径
                                                // 成功语句留下 stale 错误码。
                                                tsql_session.note_error(0);
                                            }
                                            tsql_session.note_rowcount(resp_row_count(&resp));
                                        }
                                        resp
                                    }
                                }
                            }
                        }
                    };
                    if logged {
                        querylog::record(
                            &state,
                            &peer,
                            logged_effective.as_deref().unwrap_or(&sql),
                            started.elapsed().as_secs_f64() * 1000.0,
                            &resp,
                            frame.flags & FLAG_REPLICATION != 0,
                        );
                    }
                    session.finish_statement();
                    Some(resp)
                }
                proto::REQ_PREPARE if authed => {
                    // Server-side prepared statements: the template carries
                    // `?` placeholders; binding happens at REQ_EXECUTE with
                    // typed literals rendered inside the server. The per-
                    // connection budget keeps a single connection from
                    // pinning unbounded server memory (the client never
                    // closes handles today).
                    let sql = proto::decode_sql(&frame.payload).unwrap_or_default();
                    if sql.trim().is_empty() {
                        Some(Frame::new(
                            proto::RESP_ERROR,
                            err_payload("prepare: empty statement"),
                        ))
                    } else if sql.len() > MAX_PREPARED_TEMPLATE_BYTES {
                        Some(Frame::new(
                            proto::RESP_ERROR,
                            err_payload("prepare: statement template too large (max 65536 bytes)"),
                        ))
                    } else if prepared.len() >= MAX_PREPARED_STATEMENTS {
                        Some(Frame::new(
                            proto::RESP_ERROR,
                            err_payload(
                                "prepare: too many prepared statements on this connection (max 1024)",
                            ),
                        ))
                    } else {
                        let h = next_stmt_handle;
                        next_stmt_handle += 1;
                        prepared.insert(h, sql);
                        Some(Frame::new(
                            proto::RESP_PREPARED,
                            format!(r#"{{"handle":{h}}}"#).into_bytes(),
                        ))
                    }
                }
                proto::REQ_EXECUTE if authed => {
                    // Bound execution runs the rendered text through the
                    // normal client path — grants, deadline and the query
                    // log all apply exactly like REQ_SQL (the log shows the
                    // rendered statement, same as client-side binding did).
                    let body: serde_json::Value =
                        serde_json::from_slice(&frame.payload).unwrap_or(serde_json::Value::Null);
                    let handle = body.get("handle").and_then(|v| v.as_u64());
                    let params: Vec<Value> = body
                        .get("params")
                        .and_then(|v| v.as_array())
                        .map(|a| a.iter().map(json_param_to_value).collect())
                        .unwrap_or_default();
                    // Audit/log text keeps the `?` template: rendering the
                    // bound values into the statement log stored application
                    // secrets (tokens, API keys) in cleartext. Execution still
                    // uses the rendered statement.
                    let (rendered, log_sql) = match handle.and_then(|h| prepared.get(&h)) {
                        Some(tpl) => (
                            bind_params(tpl, &params),
                            format!("{tpl}  [{} bound parameter(s)]", params.len()),
                        ),
                        None => (
                            Err("execute: unknown statement handle".to_string()),
                            String::new(),
                        ),
                    };
                    match rendered {
                        Ok(mut sql) => {
                            let started = std::time::Instant::now();
                            // Replication parity with the REQ_SQL arm: a
                            // FLAG_REPLICATION EXECUTE applies what a peer
                            // already confirmed — no statement deadline (a
                            // slow node must still converge), origin/journal
                            // under the replication flag (not this node's),
                            // and never the read tier.
                            let is_repl = replication_ok;
                            let deadline = if is_repl {
                                None
                            } else {
                                state
                                    .statement_timeout
                                    .map(|t| std::time::Instant::now() + t)
                            };
                            // Same docsql_pubsub decision as the REQ_SQL
                            // arm: a user table by that name must not be
                            // shadowed by the compat view here either —
                            // the prepared path used to skip the rewrite
                            // and fail with "no such table".
                            if !state
                                .db
                                .read()
                                .unwrap_or_else(|p| p.into_inner())
                                .table_exists_ci("docsql_pubsub")
                            {
                                if let Some(rewritten) = pubsub::try_rewrite_pubsub_view(&sql) {
                                    sql = rewritten;
                                }
                            }
                            let mut exec_logged_sql: Option<String> = None;
                            // Activity monitor + MVCC parity: a prepared
                            // SELECT is the same read-only statement its
                            // REQ_SQL twin is — route it through the
                            // read tier (it used to take the exclusive
                            // write lock for every parameterized
                            // SELECT, freezing the node's writers), and
                            // keep the session's statement counter
                            // current either way.
                            session.start_statement(&sql);
                            let read_eligible =
                                !is_repl && classify_read_eligible(&state, &sql, user.as_ref());
                            let (resp, logged) = match querylog::try_serve_log_view(
                                &sql,
                                &state,
                                user.as_ref(),
                                role == ConnRole::ReadOnly,
                            ) {
                                // 读日志的查询本身不写日志(避免读日志刷日志)。
                                Some(f) => ((f, None), false),
                                None if read_eligible => {
                                    let (f, _rc) = execute_read_sql(
                                        &state,
                                        &sql,
                                        deadline,
                                        user.as_ref(),
                                        &mut exec_logged_sql,
                                    )
                                    .await;
                                    // The row count is consumed below via
                                    // resp_row_count, same as the REQ_SQL
                                    // fast path.
                                    ((f, None), true)
                                }
                                None => (
                                    execute_sql_with_identity(
                                        &state,
                                        &sql,
                                        false,
                                        is_repl,
                                        if is_repl { None } else { Some(conn_id) },
                                        false,
                                        None,
                                        user.as_ref(),
                                        deadline,
                                        &mut exec_logged_sql,
                                    )
                                    .await,
                                    true,
                                ),
                            };
                            let (resp, identity) = resp;
                            if logged && !is_repl {
                                // @@ERROR/@@ROWCOUNT parity with the REQ_SQL
                                // fast paths (a failed EXECUTE used to leave
                                // the previous statement's code/current
                                // count in place).
                                if resp.frame_type == proto::RESP_ERROR {
                                    tsql_session.note_error(
                                        docsql_core::tsql_batch::error_code_from_text(
                                            &String::from_utf8_lossy(&resp.payload),
                                        ),
                                    );
                                } else {
                                    tsql_session.note_error(0);
                                }
                                tsql_session.note_rowcount(resp_row_count(&resp));
                            }
                            session.finish_statement();
                            // Same session bookkeeping as the REQ_SQL fast
                            // path: a parameterized INSERT owns the
                            // connection's @@IDENTITY/SCOPE_IDENTITY, and a
                            // later batch must see this statement's effects
                            // (it used to keep the previous fast-path
                            // INSERT's id).
                            if logged
                                && !is_repl
                                && docsql_core::tsql_batch::is_insert_statement(&sql)
                            {
                                tsql_session.note_identity(identity.map(docsql_core::Value::Int));
                            }
                            if logged {
                                querylog::record(
                                    &state,
                                    &peer,
                                    // Always the TEMPLATE for EXECUTE. The
                                    // row-filter rewrite embeds the RENDERED
                                    // bound values; displaying it re-leaked
                                    // the application secrets (tokens, API
                                    // keys) the template-only rule exists to
                                    // keep out of the audit log. The
                                    // enforced predicate text lives in the
                                    // grants table already.
                                    &if exec_logged_sql.is_some() {
                                        format!("{log_sql}  [row filter applied]")
                                    } else {
                                        log_sql.clone()
                                    },
                                    started.elapsed().as_secs_f64() * 1000.0,
                                    &resp,
                                    is_repl,
                                );
                            }
                            Some(resp)
                        }
                        Err(e) => Some(Frame::new(proto::RESP_ERROR, err_payload(&e))),
                    }
                }
                proto::REQ_CLOSE_STMT if authed => {
                    let body: serde_json::Value =
                        serde_json::from_slice(&frame.payload).unwrap_or(serde_json::Value::Null);
                    match body.get("handle").and_then(|v| v.as_u64()) {
                        Some(h) if prepared.remove(&h).is_some() => {
                            Some(Frame::new(proto::RESP_AFFECTED, Vec::new()))
                        }
                        _ => Some(Frame::new(
                            proto::RESP_ERROR,
                            err_payload("close: unknown statement handle"),
                        )),
                    }
                }
                proto::REQ_SQL_SEQ if authed && frame.flags & FLAG_REPLICATION != 0 => {
                    // Sequenced replication write: apply it like any
                    // replicated write, then record (origin node_id, seq)
                    // so a rejoin can pull exactly the ops it missed. The
                    // position update is fused into the write's commit unit
                    // — one fsync, and a crash can no longer leave the
                    // position behind the data (the old separate-commit
                    // window only ever caused a redundant replay that fell
                    // back to snapshot repair, but now it cannot happen).
                    match parse_seq_frame(&frame.payload) {
                        Some((seq, node_id, sql)) => {
                            // Join intake: while the sync gate is open,
                            // sequenced writes queue and are acknowledged
                            // on the spot, exactly like plain REQ_SQL —
                            // applying directly would land on a snapshot
                            // that is still replaying (or abort the
                            // bootstrap as seeming local data). The origin
                            // rides along so the drain can skip ops the
                            // snapshot already covers and keep the
                            // position exact.
                            let queued = if docsql_core::engine::Database::is_write_statement(&sql)
                            {
                                gate_enqueue(&state, Some((node_id.clone(), seq)), &sql, false)
                                    .await
                            } else {
                                None
                            };
                            match queued {
                                // Audited later by the drain replay.
                                Some(f) => Some(f),
                                None => {
                                    // Serialize per origin and skip frames the
                                    // position already covers: out-of-order
                                    // applies must never overwrite newer rows,
                                    // and a retried frame must be idempotent.
                                    let (origin_lock, origin_rejected) =
                                        origin_apply_lock(&state, &node_id).await;
                                    if origin_rejected {
                                        // node_id is client-controlled payload:
                                        // past the origin cap, reject instead of
                                        // growing apply_locks and the persisted
                                        // position table without bound. The
                                        // origin sees fan-out failure (backoff);
                                        // divergence converges through
                                        // digest/snapshot repair.
                                        Some(Frame::new(
                                            proto::RESP_ERROR,
                                            err_payload(&format!(
                                                "too many replication origins \
                                                 (max {MAX_APPLY_ORIGINS})"
                                            )),
                                        ))
                                    } else {
                                        let _origin_guard = origin_lock.lock_owned().await;
                                        let already_applied = {
                                            let mut db =
                                                state.db.write().unwrap_or_else(|p| p.into_inner());
                                            db.position_get(&node_id)
                                                .ok()
                                                .flatten()
                                                .is_some_and(|pos| pos >= seq)
                                        };
                                        if already_applied {
                                            Some(Frame::new(
                                                proto::RESP_AFFECTED,
                                                0u64.to_le_bytes().to_vec(),
                                            ))
                                        } else {
                                            let started = std::time::Instant::now();
                                            let resp = execute_sql(
                                                &state,
                                                &sql,
                                                false,
                                                true,
                                                None,
                                                false,
                                                Some((&node_id, seq)),
                                                None,
                                                None,
                                            )
                                            .await;
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
                                    }
                                }
                            }
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
                proto::REQ_PUBLISH if authed => {
                    // PUBLISH is a durable write: readwrite or admin.
                    if user
                        .as_ref()
                        .is_some_and(|u| !u.grants.admin && !u.grants.readwrite)
                    {
                        Some(Frame::new(
                            proto::RESP_ERROR,
                            err_payload("PUBLISH requires the readwrite or admin role"),
                        ))
                    } else {
                        state
                            .metrics
                            .publishes_total
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let started = std::time::Instant::now();
                        let resp = handle_publish(&state, &frame, replication_ok).await;
                        // PUBLISH persists a message (a durable write): the
                        // trail records who published where; the payload
                        // itself stays out (size only).
                        let channel = serde_json::from_slice::<serde_json::Value>(&frame.payload)
                            .ok()
                            .and_then(|v| v["channel"].as_str().map(String::from))
                            .unwrap_or_default();
                        querylog::record(
                            &state,
                            &peer,
                            &format!(
                                "PUBLISH {channel} ({} bytes){}",
                                frame.payload.len(),
                                audit_identity(user.as_ref())
                            ),
                            started.elapsed().as_secs_f64() * 1000.0,
                            &resp,
                            replication_ok,
                        );
                        Some(resp)
                    }
                }
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
                proto::REQ_PUBSUB if authed => Some(
                    handle_pubsub_cmd(&state, &frame, user.as_ref(), &peer, replication_ok).await,
                ),
                proto::REQ_BACKUP if authed => {
                    Some(backup::handle_backup(&state, role, &frame, user.as_ref(), &peer).await)
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
            if close_after {
                break;
            }
        }
        Ok::<(), std::io::Error>(())
    }
    .await;
    // Connection gone: drop its subscriptions so publishes stop fanning
    // out to a dead writer channel, and take it out of the activity
    // monitor listing.
    state.sessions.lock().await.remove(&conn_id);
    state.pubsub.remove_conn(conn_id).await;
    // If it owned the open transaction, roll that transaction back: nobody
    // can commit it anymore, and leaving it open would queue every later
    // write behind a transaction that can never finish. write_order first —
    // the same order execute_sql takes.
    {
        let _order = state.write_order.lock().await;
        if *state.tx_owner.lock().unwrap_or_else(|p| p.into_inner()) == Some(conn_id) {
            *state.tx_owner.lock().unwrap_or_else(|p| p.into_inner()) = None;
            {
                let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
                if db.in_transaction() {
                    rollback_or_abort(&mut db);
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

/// Per-statement privilege check for username/password connections.
/// Token/anonymous-legacy connections never reach here. Fails closed: a
/// query shape the read-target walker cannot fully classify is denied.
/// Row-filter enforcement (`GRANT SELECT ON t WHERE <pred> …`): rewrite
/// the statement TEXT so each filtered table's predicate rides as an
/// ordinary WHERE conjunct. The engine then treats the filter like any
/// predicate — the COUNT/window/probe fast paths stay semantically exact —
/// and the journal/peer replay carry the FILTERED text, so replays without
/// a user identity reproduce exactly what the origin executed.
///
/// Scope (everything else refuses loudly when a filtered table is
/// involved — fail-closed, never a silent bypass):
/// - single-table SELECT (no JOINs / set ops / WITH);
/// - INSERT whose source is such a SELECT (the source is rewritten);
/// - single-table UPDATE / DELETE (predicate ANDed into WHERE);
/// - EXPLAIN over a single-table SELECT.
///
/// Subquery or view occurrences of a filtered table are caught by an
/// occurrence-count check (deep read targets vs rewritten positions) and
/// a transitive view-base scan.
pub(super) fn apply_row_filters(
    db: &docsql_core::engine::Database,
    sql: &str,
    filters: &std::collections::BTreeMap<String, Vec<String>>,
) -> Result<String, String> {
    use sqlparser::ast::{Expr as E, SetExpr, Statement, TableFactor};
    let lower = |s: &str| s.to_ascii_lowercase();
    let filtered: std::collections::BTreeSet<String> = filters.keys().map(|k| lower(k)).collect();
    if filtered.is_empty() {
        return Ok(sql.to_string());
    }
    // Shapes the SQL parser or the classifier cannot fully handle (T-SQL
    // batches reach execution through the interpreter; exotic reads): the
    // raw text mentioning a filtered table refuses — fail-closed, never a
    // silent bypass through an unparseable statement.
    let mentions_filtered = || {
        let low = sql.to_ascii_lowercase();
        filtered.iter().any(|t| contains_word_ci(&low, t))
    };
    let parsed = match Database::parse_classified(sql) {
        Ok(p) => p,
        Err(_) => {
            return if mentions_filtered() {
                Err("row filters: statement shape cannot be classified; refusing".to_string())
            } else {
                Ok(sql.to_string())
            };
        }
    };
    let AnyStmt::Sql(stmt) = &parsed.stmt else {
        return if mentions_filtered() {
            Err("row filters apply to SQL statements only".into())
        } else {
            Ok(sql.to_string())
        };
    };
    // Deep read targets (per-occurrence): the coverage proof below needs
    // duplicates preserved. UPDATE/DELETE target tables join the set —
    // they are WRITE targets, but their rows are exactly what a filter on
    // them must protect (without this the early-exit below silently
    // skipped `UPDATE sales SET … WHERE …`).
    let objects = |n: &str| db.table_exists(n);
    let mut targets = match Database::stmt_read_target_occurrences(stmt, &objects) {
        Some(t) => t,
        None => {
            return if mentions_filtered() {
                Err("row filters: statement shape cannot be classified; refusing".to_string())
            } else {
                Ok(sql.to_string())
            };
        }
    };
    match stmt.as_ref() {
        Statement::Update(u) => {
            if let TableFactor::Table { name, .. } = &u.table.relation {
                targets.push(docsql_core::engine::table_ref_name(&name.clone()));
            }
        }
        Statement::Delete(d) => {
            // Both spellings: plain `DELETE FROM t` parses as WithKeyword
            // in sqlparser 0.62 (WithoutKeyword is the MySQL `DELETE t
            // FROM …` form).
            let factors = match &d.from {
                sqlparser::ast::FromTable::WithoutKeyword(v) => v,
                sqlparser::ast::FromTable::WithFromKeyword(v) => v,
            };
            for f in factors {
                if let TableFactor::Table { name, .. } = &f.relation {
                    targets.push(docsql_core::engine::table_ref_name(&name.clone()));
                }
            }
        }
        _ => {}
    }
    // Views over filtered tables: a view is its reader's permission
    // boundary, but a row filter on a BASE table must survive through it —
    // the rewrite cannot see inside the stored definition, so access is
    // refused (fail-closed).
    for t in &targets {
        let mut queue = vec![t.clone()];
        let mut seen = std::collections::BTreeSet::new();
        let mut depth = 0usize;
        while let Some(v) = queue.pop() {
            if !seen.insert(v.clone()) {
                continue;
            }
            if let Some(bases) = db.view_base_tables(&v) {
                depth += 1;
                if depth > 16 {
                    return Err("row filters: view chain too deep to verify".into());
                }
                for b in bases {
                    if filtered.contains(&lower(&b)) {
                        return Err(format!(
                            "view {t} reads row-filtered table {b}; \
                             row filters cannot protect it through the view"
                        ));
                    }
                    queue.push(b);
                }
            }
        }
    }
    // Nothing filtered is referenced: the statement passes untouched.
    if !targets.iter().any(|t| filtered.contains(&lower(t))) {
        return Ok(sql.to_string());
    }
    let lookup = |t: &str| -> Option<E> {
        let preds = filters
            .iter()
            .find(|(k, _)| lower(k) == lower(t))
            .map(|(_, v)| v)?;
        let mut parts = preds
            .iter()
            .map(|p| docsql_core::engine::parse_expr_text(p))
            .collect::<Result<Vec<_>, _>>()
            .ok()?;
        let mut acc = parts.remove(0);
        while let Some(next) = parts.pop() {
            acc = E::BinaryOp {
                left: Box::new(acc),
                op: sqlparser::ast::BinaryOperator::Or,
                right: Box::new(next),
            };
        }
        Some(acc)
    };
    fn and_into_sel(old: Option<E>, pred: E) -> E {
        // Parenthesize both sides as needed before AND-splicing:
        // sqlparser's Display emits BinaryOp children WITHOUT precedence
        // parentheses, so `user_or_tree AND filter` re-parsed as
        // `left-most OR right-most AND …` flipped the binding and let
        // `WHERE x = 1 OR y = 2` bypass the injected filter entirely.
        let wrap_if_or = |e: E| match e {
            E::BinaryOp {
                op: sqlparser::ast::BinaryOperator::Or,
                ..
            } => E::Nested(Box::new(e)),
            other => other,
        };
        let right = E::Nested(Box::new(pred));
        match old {
            Some(left) => E::BinaryOp {
                left: Box::new(wrap_if_or(left)),
                op: sqlparser::ast::BinaryOperator::And,
                right: Box::new(right),
            },
            None => right,
        }
    }
    // Rewrite ONE top-level plain table factor: single-table SELECT shape.
    // JOINs / derived tables / set operations fall through untouched; the
    // coverage check below refuses them when they reference a filtered
    // table (fail-closed).
    fn rewrite_select_factor(
        select: &mut sqlparser::ast::Select,
        filters: &std::collections::BTreeMap<String, Vec<String>>,
        rewritten_tables: &mut Vec<String>,
    ) {
        if select.from.len() != 1 {
            return;
        }
        let twj = &mut select.from[0];
        if !twj.joins.is_empty() {
            return;
        }
        let table = match &twj.relation {
            TableFactor::Table { name, .. } => docsql_core::engine::table_ref_name(&name.clone()),
            _ => return,
        };
        let Some(preds) = filters
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(&table))
            .map(|(_, v)| v)
        else {
            return;
        };
        let Ok(mut parts) = preds
            .iter()
            .map(|p| docsql_core::engine::parse_expr_text(p))
            .collect::<Result<Vec<_>, _>>()
        else {
            return; // validated at GRANT time; unreachable in practice
        };
        let mut acc = parts.remove(0);
        while let Some(next) = parts.pop() {
            acc = E::BinaryOp {
                left: Box::new(acc),
                op: sqlparser::ast::BinaryOperator::Or,
                right: Box::new(next),
            };
        }
        select.selection = Some(and_into_sel(select.selection.take(), acc));
        rewritten_tables.push(table.to_ascii_lowercase());
    }
    let mut rewritten_tables: Vec<String> = Vec::new();
    let mut new_stmt = (**stmt).clone();
    match &mut new_stmt {
        Statement::Query(q) => {
            if let SetExpr::Select(select) = q.body.as_mut() {
                rewrite_select_factor(select.as_mut(), filters, &mut rewritten_tables);
            }
        }
        Statement::Insert(ins) => {
            if let Some(source) = &mut ins.source {
                if let SetExpr::Select(select) = source.body.as_mut() {
                    rewrite_select_factor(select.as_mut(), filters, &mut rewritten_tables);
                }
            }
        }
        Statement::Update(u) => {
            let table = match &u.table.relation {
                TableFactor::Table { name, .. } => {
                    docsql_core::engine::table_ref_name(&name.clone())
                }
                _ => String::new(),
            };
            // UPDATE ... FROM evaluates the WHERE over the MERGED image
            // (row keys are `alias.col`): the injected predicate's bare
            // columns resolve through lookup_col's suffix fallback and can
            // bind to a FROM table's same-named column — a controlled FROM
            // table short-circuits the filter for every row. Refuse the
            // shape (fail-closed, same treatment as JOIN/子查询).
            if u.from.is_some() && filtered.contains(&lower(&table)) {
                return Err(format!(
                    "row filter on {table}: UPDATE ... FROM evaluates its WHERE over the \
                     joined image, where the filter's bare columns can bind to the FROM \
                     tables; rewrite without FROM or use an unrestricted grant"
                ));
            }
            if let Some(pred) = lookup(&table) {
                u.selection = Some(and_into_sel(u.selection.take(), pred));
                rewritten_tables.push(table.to_ascii_lowercase());
            }
        }
        Statement::Delete(d) => {
            // Same two-spellings rule as the mention set above.
            // `DELETE ... USING` evaluates the WHERE over the merged image
            // exactly like UPDATE ... FROM: the injected predicate's bare
            // columns can bind to a USING table's same-named column and
            // short-circuit the filter — refuse the shape (fail-closed).
            let del_target = match &d.from {
                sqlparser::ast::FromTable::WithoutKeyword(v)
                | sqlparser::ast::FromTable::WithFromKeyword(v) => {
                    if v.len() == 1 {
                        match &v[0].relation {
                            TableFactor::Table { name, .. } => {
                                docsql_core::engine::table_ref_name(name)
                            }
                            _ => String::new(),
                        }
                    } else {
                        String::new()
                    }
                }
            };
            if d.using.is_some() && !del_target.is_empty() && filtered.contains(&lower(&del_target))
            {
                return Err(format!(
                    "row filter on {del_target}: DELETE ... USING evaluates its WHERE over the \
                     joined image, where the filter's bare columns can bind to the USING \
                     tables; rewrite without USING or use an unrestricted grant"
                ));
            }
            let factors = match &mut d.from {
                sqlparser::ast::FromTable::WithoutKeyword(v) => v,
                sqlparser::ast::FromTable::WithFromKeyword(v) => v,
            };
            if factors.len() == 1 && factors[0].joins.is_empty() {
                if let TableFactor::Table { name, .. } = &factors[0].relation {
                    let table = docsql_core::engine::table_ref_name(name);
                    if let Some(pred) = lookup(&table) {
                        d.selection = Some(and_into_sel(d.selection.take(), pred));
                        rewritten_tables.push(table.to_ascii_lowercase());
                    }
                }
            }
        }
        Statement::Explain { statement, .. } => {
            // `EXPLAIN SELECT … FROM <filtered table>`: the walker counts
            // the inner query's reads as deep occurrences, so rewrite the
            // inner SELECT the same way — a restricted user can inspect
            // the plan of exactly the statements they may run (this used
            // to refuse, while the doc comment claimed support).
            if let Statement::Query(q) = statement.as_mut() {
                if let SetExpr::Select(select) = q.body.as_mut() {
                    rewrite_select_factor(select.as_mut(), filters, &mut rewritten_tables);
                }
            }
        }
        _ => {}
    }
    // Coverage proof: every filtered table's DEEP occurrence count must be
    // exactly the number of top-level rewrites — one more occurrence
    // anywhere (subquery, JOIN side, second UNION arm) is an unfilterable
    // read and refuses the statement.
    for t in &filtered {
        let deep = targets.iter().filter(|x| lower(x) == *t).count();
        let top = rewritten_tables.iter().filter(|x| x.as_str() == *t).count();
        if deep > top {
            return Err(format!(
                "row filter on {t}: the statement reads {t} in a position row filters \
                 cannot protect (subquery / JOIN / view / set operation); rewrite the \
                 statement or use an unrestricted grant"
            ));
        }
    }
    let rewritten_text = new_stmt.to_string();
    // The serialized AST must re-parse (peers and the journal consume the
    // text); a round-trip failure is a loud error, not a bypass.
    Database::parse_classified(&rewritten_text).map_err(|e| e.to_string())?;
    Ok(rewritten_text)
}

/// ASCII-case-insensitive word-boundary containment (`t` as a whole word
/// in `text`, which arrives lowercased).
pub(super) fn contains_word_ci(text: &str, word: &str) -> bool {
    let wb = word.as_bytes();
    let tb = text.as_bytes();
    if wb.is_empty() {
        return false;
    }
    let mut i = 0;
    while i + wb.len() <= tb.len() {
        if &tb[i..i + wb.len()] == wb {
            let before_ok = i == 0 || !tb[i - 1].is_ascii_alphanumeric();
            let after = tb.get(i + wb.len()).copied();
            let after_ok = after.is_none_or(|c| !c.is_ascii_alphanumeric());
            if before_ok && after_ok {
                return true;
            }
        }
        i += 1;
    }
    false
}

pub(super) fn authorize_statement(
    db: Option<&docsql_core::engine::Database>,
    stmt: &AnyStmt,
    tx_kind: &TxControl,
    is_write: bool,
    g: &docsql_core::useradmin::UserGrants,
) -> std::result::Result<(), String> {
    use docsql_core::useradmin::{PRIV_DELETE, PRIV_INSERT, PRIV_UPDATE};
    if g.admin {
        return Ok(());
    }
    match stmt {
        AnyStmt::UserAdmin(_) => Err("user and role management requires the admin role".into()),
        // REFRESH rewrites a whole derived table: DDL-grade, admin only
        // (mirrors every other CREATE/DROP/ALTER shape).
        AnyStmt::RefreshMatview(_) => {
            Err("materialized view refresh requires the admin role".into())
        }
        AnyStmt::Sql(s) => {
            // Transaction control itself is not privileged; the buffered
            // statements were each authorized when they arrived.
            if !matches!(tx_kind, TxControl::None) {
                return Ok(());
            }
            use sqlparser::ast::Statement as S;
            if !is_write {
                let objects = |n: &str| db.is_some_and(|d| d.table_exists(n));
                let Some(targets) = Database::stmt_read_targets(s, &objects) else {
                    return Err(
                        "this statement shape cannot be authorized for user connections".into(),
                    );
                };
                // A view is a SELECT permission boundary on the READ path
                // too: expand its base tables (fail-closed, chain capped)
                // exactly like the write path does. Without this, a view
                // over docsql_users left on a pre-upgrade volume handed
                // password hashes to any readonly/readwrite connection —
                // and the create-time refusal cannot help there (dumps
                // from old volumes must keep replaying).
                let targets = {
                    let mut queue = targets;
                    let mut seen = std::collections::BTreeSet::new();
                    let mut out: Vec<String> = Vec::new();
                    let mut depth = 0usize;
                    while let Some(t) = queue.pop() {
                        if !seen.insert(t.clone()) {
                            continue;
                        }
                        match db.and_then(|d| d.view_base_tables(&t)) {
                            Some(bases) => {
                                // A direct table grant on the view itself is
                                // a valid SELECT boundary: the view is the
                                // permission surface and its bases stay
                                // hidden (standard SQL view semantics — the
                                // grant row could previously never match).
                                // readonly/readwrite blanket access must NOT
                                // take this shortcut: the base expansion is
                                // exactly what refuses user-table views to
                                // those roles. And even for a direct view
                                // grant, the user/role subsystem stays
                                // admin-only behind ANY depth of views —
                                // GRANT-time checks only see the view name,
                                // so a view whose closure names a user table
                                // must not hand password hashes out here.
                                if !g.readonly && !g.readwrite && g.may_select(&t) {
                                    let mut closure: Vec<String> = bases.clone();
                                    let mut cseen = std::collections::BTreeSet::new();
                                    cseen.insert(t.clone());
                                    while let Some(b) = closure.pop() {
                                        if !cseen.insert(b.clone()) {
                                            continue;
                                        }
                                        if docsql_core::useradmin::is_user_table(&b) {
                                            return Err(
                                                "user/role data is visible to the admin role only"
                                                    .into(),
                                            );
                                        }
                                        if let Some(deeper) =
                                            db.and_then(|d| d.view_base_tables(&b))
                                        {
                                            closure.extend(deeper);
                                        }
                                    }
                                    continue;
                                }
                                depth += 1;
                                if depth > 16 {
                                    return Err("view chain too deep to authorize".into());
                                }
                                queue.extend(bases);
                            }
                            None => out.push(t),
                        }
                    }
                    out
                };
                for t in &targets {
                    if docsql_core::useradmin::is_user_table(t) {
                        return Err("user/role data is visible to the admin role only".into());
                    }
                    if readable_by_all(t) {
                        continue;
                    }
                    if !g.may_select(t) {
                        return Err(format!(
                            "SELECT on table {t} requires the readonly/readwrite role \
                             or a table grant"
                        ));
                    }
                }
                // Column-restricted grants (`GRANT SELECT (a, b) ON t`):
                // every read target must be attributable to a single-table
                // SELECT whose referenced columns are all granted. Anything
                // else (wildcards, joins, subqueries, multi-table and
                // unattributable shapes) refuses — fail-closed, never a
                // silent widening.
                if targets.iter().any(|t| g.is_restricted_select(t)) {
                    match docsql_core::engine::single_table_select_refs(s) {
                        Some((table, cols)) => {
                            for rt in &targets {
                                if !g.is_restricted_select(rt) {
                                    continue;
                                }
                                if !rt.eq_ignore_ascii_case(&table) {
                                    return Err(format!(
                                        "SELECT on {rt} with column-restricted grants requires \
                                         a single-table SELECT of exactly that table"
                                    ));
                                }
                                let granted = g.granted_cols(rt);
                                for c in &cols {
                                    if !granted.iter().any(|x| x.eq_ignore_ascii_case(c)) {
                                        return Err(format!(
                                            "SELECT on column {c} of {table} is not granted \
                                             (the grant lists its columns)"
                                        ));
                                    }
                                }
                            }
                        }
                        None => {
                            return Err("this statement shape cannot be authorized against \
                                 column-restricted grants (wildcards, joins, subqueries, \
                                 multi-table statements and EXPLAIN are not attributable)"
                                .into());
                        }
                    }
                }
                return Ok(());
            }
            let (bit, label) = match s.as_ref() {
                S::Insert(_) => (PRIV_INSERT, "INSERT"),
                S::Update(_) => (PRIV_UPDATE, "UPDATE"),
                S::Delete(_) => (PRIV_DELETE, "DELETE"),
                S::Truncate(_) => (PRIV_DELETE, "TRUNCATE"),
                // MERGE is ordinary DML (write-targets, read-targets and the
                // system-table gate all classify it); it used to fall into
                // the DDL refusal. Require the privileges of the arms the
                // statement actually carries, each held on the target.
                S::Merge(m) => {
                    let mut bits: u8 = 0;
                    for c in &m.clauses {
                        bits |= match &c.action {
                            sqlparser::ast::MergeAction::Insert(_) => PRIV_INSERT,
                            sqlparser::ast::MergeAction::Update(_) => PRIV_UPDATE,
                            sqlparser::ast::MergeAction::Delete { .. } => PRIV_DELETE,
                        };
                    }
                    let targets = Database::stmt_write_targets(s);
                    for t in &targets {
                        for (b, l) in [
                            (PRIV_INSERT, "INSERT"),
                            (PRIV_UPDATE, "UPDATE"),
                            (PRIV_DELETE, "DELETE"),
                        ] {
                            if bits & b != 0 && !g.may_dml(t, b) {
                                return Err(format!(
                                    "MERGE arm {l} on table {t} requires the readwrite role \
                                     or a table grant"
                                ));
                            }
                        }
                    }
                    // Arm bits are already verified above; fall through to
                    // the read-source checks with a no-op bit.
                    (0, "MERGE")
                }
                _ => return Err("DDL and administrative statements require the admin role".into()),
            };
            let mut write_targets = Database::stmt_write_targets(s);
            // TRUNCATE ... CASCADE clears the FK children too (transitive
            // closure, same as execution time) — those tables are as much
            // authorization input as the named ones. Without this, a user
            // holding DELETE on the parent alone could empty child tables
            // they hold no grant on at all.
            if let S::Truncate(tr) = s.as_ref() {
                if tr.cascade == Some(sqlparser::ast::CascadeOption::Cascade) {
                    match db {
                        Some(db) => {
                            let mut targets = write_targets.clone();
                            loop {
                                let referencing: Vec<String> = db
                                    .table_foreign_keys_of(&targets)
                                    .into_iter()
                                    .filter(|t| !targets.contains(t))
                                    .collect();
                                if referencing.is_empty() {
                                    break;
                                }
                                targets.extend(referencing);
                            }
                            write_targets = targets;
                        }
                        None => {
                            return Err(
                                "TRUNCATE ... CASCADE cannot be authorized while any cascade \
                                 target fails to resolve (views or unresolvable references \
                                 among the cascaded tables); resolve them first"
                                    .into(),
                            );
                        }
                    }
                }
            }
            for t in write_targets {
                // bit == 0: the MERGE arm verified each arm-bit above.
                if bit != 0 && !g.may_dml(&t, bit) {
                    return Err(format!(
                        "{label} on table {t} requires the readwrite role or a table grant"
                    ));
                }
            }
            // A write also READS: the INSERT .. SELECT source, UPDATE
            // assignment subqueries, MERGE WHEN predicates. Those tables are
            // authorization input — without this check a user holding only a
            // write grant could copy docsql_users password hashes (or any
            // ungranted table) into a table of their own and read them back.
            let objects = |n: &str| db.is_some_and(|d| d.table_exists(n));
            let sources = Database::stmt_read_targets(s, &objects).ok_or_else(|| {
                "this statement shape cannot be authorized for user connections".to_string()
            })?;
            // Column-restricted grants bind on WRITE sources too: an
            // INSERT .. SELECT copying an ungranted column into one's own
            // table used to pass the table-level may_select alone and leak
            // the value on read-back.
            let restricted: Vec<&String> = sources
                .iter()
                .filter(|t| g.is_restricted_select(t))
                .collect();
            if !restricted.is_empty() {
                let attributed = match s.as_ref() {
                    S::Insert(ins) => ins
                        .source
                        .as_ref()
                        .and_then(|q| docsql_core::engine::single_table_query_refs(q.as_ref())),
                    _ => None,
                };
                match attributed {
                    Some((table, cols)) => {
                        for rt in &restricted {
                            if !rt.eq_ignore_ascii_case(&table) {
                                return Err(format!(
                                    "reading {rt} with column-restricted grants requires \
                                     a single-table SELECT of exactly that table"
                                ));
                            }
                            let granted = g.granted_cols(rt);
                            for c in &cols {
                                if !granted.iter().any(|x| x.eq_ignore_ascii_case(c)) {
                                    return Err(format!(
                                        "SELECT on column {c} of {table} is not granted \
                                         (the grant lists its columns)"
                                    ));
                                }
                            }
                        }
                    }
                    None => {
                        return Err(
                            "write sources cannot be authorized against column-restricted \
                             grants (only a plain single-table INSERT ... SELECT is \
                             attributable; wildcards, joins, subqueries, UPDATE and MERGE \
                             sources are not)"
                                .into(),
                        );
                    }
                }
            }
            // View expansion: a view is a SELECT permission boundary, not a
            // license to write through it. A write reading a view needs the
            // grants on the view's BASE tables (fail-closed, chain capped).
            let sources = {
                let mut queue: Vec<String> = sources;
                let mut seen = std::collections::BTreeSet::new();
                let mut out: Vec<String> = Vec::new();
                let mut depth = 0usize;
                while let Some(t) = queue.pop() {
                    if !seen.insert(t.clone()) {
                        continue;
                    }
                    match db.and_then(|d| d.view_base_tables(&t)) {
                        Some(bases) => {
                            depth += 1;
                            if depth > 16 {
                                return Err("view chain too deep to authorize".into());
                            }
                            queue.extend(bases);
                        }
                        None => out.push(t),
                    }
                }
                out
            };
            for t in &sources {
                if docsql_core::useradmin::is_user_table(t) {
                    return Err("user/role data is visible to the admin role only".into());
                }
                if readable_by_all(t) {
                    continue;
                }
                if !g.may_select(t) {
                    return Err(format!(
                        "SELECT on table {t} requires the readonly/readwrite role \
                         or a table grant"
                    ));
                }
            }
            Ok(())
        }
    }
}
