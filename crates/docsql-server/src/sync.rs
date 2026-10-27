use super::*;

/// End offset of one dump chunk: `start + budget` clamped to the script
/// length, then advanced (at most 3 bytes) to the next char boundary. A raw
/// byte offset can land inside a multi-byte char (the dump carries user SQL
/// text, CJK included) and `&str[start..end]` would panic. Chunks are
/// reassembled by the joiner, so a chunk overshooting the budget by a few
/// bytes is harmless.
pub(super) fn dump_chunk_end(script: &str, start: usize, budget: usize) -> usize {
    let mut end = (start + budget).min(script.len());
    while end < script.len() && !script.is_char_boundary(end) {
        end += 1;
    }
    end
}
/// Fresh-node bootstrap retries before giving up (peers may still be
/// coming up during a full cluster start). Fast, short rounds: a
/// simultaneously started cluster settles in the first round or two, and
/// the shorter gate-open window keeps cross-node reads lagging by
/// milliseconds instead of seconds.
pub(super) const SYNC_ROUNDS: usize = 10;
pub(super) const SYNC_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(500);
/// Budget the gate watchdog gives a force drain to acquire the write path
/// before hard-closing the gate (see the bootstrap spawn site).
pub(super) const SYNC_WATCHDOG_GRACE: std::time::Duration = std::time::Duration::from_secs(60);

/// True when the database holds anything beyond the pubsub system table.
pub(super) fn has_user_tables(db: &Database) -> bool {
    db.catalog()
        .iter()
        .any(|t| !docsql_core::engine::is_internal_table(&t.name))
}

/// Why a REQ_HOLD did not grant. `Unreachable` means the peer could not be
/// connected to at all (down, partitioned) — it cannot be committing
/// writes, so a join may safely proceed without freezing it. `Busy` means
/// the peer is alive but could not freeze in time — writes may be in
/// flight, so the join attempt must abort.
pub(super) enum HoldFail {
    Unreachable(String),
    Busy(String),
}

/// Send REQ_HOLD to one peer and return the granted hold id.
pub(super) async fn hold_peer(
    state: &Arc<ServerState>,
    target: &str,
    advertise: &str,
) -> Result<u64, HoldFail> {
    let unreachable = |e: std::io::Error| HoldFail::Unreachable(e.to_string());
    let mut stream = tls::dial_protocol(target, state.tls_out.as_ref())
        .await
        .map_err(unreachable)?;
    let hello = read_wire_hello(&mut stream, state.transport_key.as_ref())
        .await
        .map_err(unreachable)?;
    let wire: WireKey = state.transport_key.as_ref().map(|k| (k, &hello));
    let mut replay = crypto::ReplayGuard::default();
    if let Some(token) = fanout_auth(state) {
        auth_on(&mut stream, token, wire, &mut replay)
            .await
            .map_err(|e| HoldFail::Busy(format!("auth: {e}")))?;
    }
    let frame = replication_frame(proto::REQ_HOLD, advertise.as_bytes().to_vec(), wire);
    // Encode/write failures on a connected peer mean the peer went away
    // mid-hold — same classification as an unreachable target.
    if let Err(e) = write_frame_on(&mut stream, &frame).await {
        return Err(HoldFail::Unreachable(e.to_string()));
    }
    // The peer accepted the connection: if it now fails to answer in time
    // it is busy (alive, writes possibly in flight) — never "unreachable".
    let resp = read_response_frame_with_guard(&mut stream, wire, &mut replay)
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
pub(super) async fn handle_sync(state: &Arc<ServerState>, frame: &Frame, tx: &mpsc::Sender<Frame>) {
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
    // and bounded, the joiner simply retries. `lock_engine_for_write` (not
    // a bare write_order lock) also waits out an open client transaction:
    // its uncommitted rows live in the in-memory tables and would otherwise
    // be serialized into the snapshot (and later vanish on ROLLBACK).
    let order = match tokio::time::timeout(SYNC_HOLD_TIMEOUT, lock_engine_for_write(state)).await {
        Ok(Some(g)) => g,
        Ok(None) => {
            return abort("sync: write path busy; retry later".into()).await;
        }
        Err(_) => {
            return abort("sync: write path busy; retry later".into()).await;
        }
    };
    let mut targets = state.peers.lock().await.clone();
    targets.sort();
    // Clear hold remnants of OUR prior attempts first: a REQ_HOLD whose
    // response was lost in an earlier try leaves the peer frozen (id
    // unknown to us) until the 60s watchdog — every hold we now request
    // would answer "busy" and the join would stall for that long. The
    // sweep (id 0 + our identity) is a no-op when nothing is stale, and
    // runs concurrently so dead peers do not stretch this node's write
    // freeze by (peers × timeout).
    let mut sweep = Vec::new();
    for target in &targets {
        let mut clear = Vec::with_capacity(8 + advertise.len());
        clear.extend_from_slice(&0u64.to_le_bytes());
        clear.extend_from_slice(advertise.as_bytes());
        let st = state.clone();
        let target = target.clone();
        sweep.push(tokio::spawn(async move {
            let _ = forward_frame(
                &target,
                proto::REQ_RELEASE,
                &clear,
                st.transport_key.as_ref(),
                fanout_auth(&st),
                st.tls_out.as_ref(),
            )
            .await;
        }));
    }
    for j in sweep {
        let _ = j.await;
    }
    // Holds go out CONCURRENTLY, like the sweeps and releases around them:
    // a peer that accepts TCP but never answers costs its full IO timeout,
    // and a serial hold loop held this node's write path for
    // (peers × timeout). Every result is still collected before deciding —
    // bailing on the first Busy would strand holds the other tasks granted.
    let mut holds = Vec::new();
    for target in &targets {
        let st = state.clone();
        let target = target.clone();
        let adv = advertise.clone();
        holds.push(tokio::spawn(
            async move { hold_peer(&st, &target, &adv).await },
        ));
    }
    let mut held: Vec<(String, u64)> = Vec::new();
    let mut busy: Option<String> = None;
    for (target, handle) in targets.iter().zip(holds) {
        let hold = match handle.await {
            Ok(r) => r,
            Err(e) => Err(HoldFail::Busy(format!("hold task failed: {e}"))),
        };
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
                    target,
                    None,
                    true,
                    Some(format!("skipped, unreachable: {detail}")),
                );
            }
            Err(HoldFail::Busy(detail)) => {
                // The peer is alive but could not freeze its write path in
                // time — writes may be in flight that would land after the
                // snapshot and never reach the joiner. Abort the whole
                // attempt (releasing every hold taken) and retry.
                eprintln!("sync: hold on {target} failed ({detail}); aborting this attempt");
                querylog::sync_event(
                    &state.sync_log,
                    "join-hold",
                    target,
                    None,
                    false,
                    Some(detail.clone()),
                );
                busy = busy.or(Some(detail));
            }
        }
    }
    if let Some(detail) = busy {
        let mut releases = Vec::new();
        for (t, id) in &held {
            let st = state.clone();
            let t = t.clone();
            let id = *id;
            releases.push(tokio::spawn(async move {
                let _ = forward_frame(
                    &t,
                    proto::REQ_RELEASE,
                    &id.to_le_bytes(),
                    st.transport_key.as_ref(),
                    fanout_auth(&st),
                    st.tls_out.as_ref(),
                )
                .await;
            }));
        }
        for r in releases {
            let _ = r.await;
        }
        return abort(format!("sync: quiesce failed ({detail}); retry")).await;
    }
    // Capture with the mesh quiesced. The engine lock alone is enough
    // here: write_order stops new local writes, the holds stopped the
    // peers, and replication applies were drained by the hold handshakes.
    let dump = {
        let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
        db.dump_script()
    };
    let table_count = {
        let db = state.db.write().unwrap_or_else(|p| p.into_inner());
        db.catalog()
            .iter()
            .filter(|t| !docsql_core::engine::is_internal_table(&t.name))
            .count() as u64
    };
    // Register the joiner still under the write path: every write before
    // this point is inside the dump and could not have fanned to it;
    // every write after it fans out to the joiner and postdates the dump.
    if !advertise.is_empty() {
        if parse_peer_addr(&advertise).is_none() {
            eprintln!("cluster join: ignoring malformed advertise {advertise:?}");
            querylog::sync_event(
                &state.sync_log,
                "join",
                "",
                None,
                false,
                Some(format!("malformed advertise {advertise:?}")),
            );
        } else {
            let mut peers = state.peers.lock().await;
            if !peers.contains(&advertise) && !is_self_peer(&state.listen, &advertise).await {
                if peers.len() >= MAX_DYNAMIC_PEERS {
                    eprintln!(
                        "cluster join: peer list full ({MAX_DYNAMIC_PEERS}), \
                         ignoring {advertise}"
                    );
                    querylog::sync_event(
                        &state.sync_log,
                        "join",
                        &advertise,
                        None,
                        false,
                        Some("peer list full".into()),
                    );
                } else {
                    peers.push(advertise.clone());
                    eprintln!("cluster join: registered peer {advertise}");
                    querylog::sync_event(&state.sync_log, "join", &advertise, None, true, None);
                }
            }
        }
    }
    // Releases are idempotent and watchdog-backed: send them concurrently
    // so a slow/dead peer cannot stretch this node's write freeze by
    // (peers × timeout) — the freeze is held until the sends conclude.
    let mut releases = Vec::new();
    for (target, id) in &held {
        let st = state.clone();
        let target = target.clone();
        let id = *id;
        releases.push(tokio::spawn(async move {
            if let Err(e) = forward_frame(
                &target,
                proto::REQ_RELEASE,
                &id.to_le_bytes(),
                st.transport_key.as_ref(),
                fanout_auth(&st),
                st.tls_out.as_ref(),
            )
            .await
            {
                eprintln!("sync: release on {target} failed ({e}); watchdog will drop the hold");
            }
        }));
    }
    for handle in releases {
        let _ = handle.await;
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
        let end = dump_chunk_end(&script, start, SYNC_CHUNK_BYTES);
        let chunk = &script[start..end];
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
pub(super) async fn handle_hold(state: &Arc<ServerState>, frame: &Frame) -> Frame {
    let advertise = String::from_utf8_lossy(&frame.payload).trim().to_string();
    // Validate BEFORE the write path freezes: an error after arming the hold
    // would leave writes frozen until the 60s watchdog for no reason. The
    // advertised address is validated and capped exactly like REQ_SYNC
    // registration — in the token-less compatibility mode any client token
    // can send REQ_HOLD, and a garbage address registered here would tax
    // every write fan-out with a doomed connect/DNS attempt forever after.
    if !advertise.is_empty() {
        if parse_peer_addr(&advertise).is_none() {
            return Frame::new(
                proto::RESP_ERROR,
                err_payload("hold: advertise address must be host:port"),
            );
        }
        // 满员检查只约束「新增」注册:已在 peer 列表内的节点(静态
        // DOCSQL_PEERS 或此前注册过的)重启后重新 join 时,其 HOLD 若被
        // 满员拒绝,静态节点数 ≥ MAX_DYNAMIC_PEERS 的集群里重启节点的
        // join 会永久失败——它本来就不新增任何列表条目。
        let (already_present, list_len) = {
            let peers = state.peers.lock().await;
            (peers.contains(&advertise), peers.len())
        };
        if !already_present && list_len >= MAX_DYNAMIC_PEERS {
            return Frame::new(
                proto::RESP_ERROR,
                err_payload(&format!(
                    "cluster join: peer list full ({MAX_DYNAMIC_PEERS}), \
                     restart with an explicit DOCSQL_PEERS list"
                )),
            );
        }
    }
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
    state
        .holds
        .lock()
        .await
        .insert(id, (guard, advertise.clone()));
    // Register the joiner while still frozen: writes that start after the
    // hold fans out to the joiner; ones before it are in the dump.
    if !advertise.is_empty() {
        let mut peers = state.peers.lock().await;
        if !peers.contains(&advertise) && !is_self_peer(&state.listen, &advertise).await {
            // Re-check the cap INSIDE the critical section: the pre-order
            // check sampled the list without holding it, and two
            // concurrent HOLDs could both pass before either pushed,
            // breaching the per-write fan-out bound (handle_sync registers
            // under its own write_order and has no such race).
            if peers.len() >= MAX_DYNAMIC_PEERS {
                drop(peers);
                // Undo the hold just armed: dropping the guard releases the
                // write gate, and the loud error tells the joiner it was
                // never registered (a silent skip would freeze a joiner
                // that believes it is in the fan-out set).
                state.holds.lock().await.remove(&id);
                return Frame::new(
                    proto::RESP_ERROR,
                    err_payload(&format!(
                        "cluster join: peer list full ({MAX_DYNAMIC_PEERS}), \
                         restart with an explicit DOCSQL_PEERS list"
                    )),
                );
            }
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

/// REQ_RELEASE: drop one hold (8-byte id payload); the joiner stays
/// registered as a peer. A payload of id 0 plus the joiner identity
/// (`id_le || advertise`) sweeps every hold that identity still owns —
/// how a joiner clears remnants of a prior attempt whose REQ_RELEASE was
/// lost (the peer would otherwise stay frozen until the 60s watchdog).
/// Receivers older than the extension read only the id and behave as
/// before.
pub(super) async fn handle_release(state: &Arc<ServerState>, frame: &Frame) -> Frame {
    let id = frame
        .payload
        .get(..8)
        .and_then(|b| b.try_into().ok())
        .map(u64::from_le_bytes);
    let owner = if frame.payload.len() > 8 {
        Some(String::from_utf8_lossy(&frame.payload[8..]).into_owned())
    } else {
        None
    };
    let mut holds = state.holds.lock().await;
    let mut dropped = match id {
        Some(id) => holds.remove(&id).is_some(),
        None => false,
    };
    if let Some(owner) = owner {
        let stale: Vec<u64> = holds
            .iter()
            .filter(|(_, (_, o))| *o == owner)
            .map(|(id, _)| *id)
            .collect();
        for id in stale {
            if holds.remove(&id).is_some() {
                dropped = true;
            }
        }
    }
    Frame::new(
        proto::RESP_AFFECTED,
        (dropped as u64).to_le_bytes().to_vec(),
    )
}

/// REQ_DIGEST: per-table replication fingerprints for the rejoin repair
/// (see `repair_sync`). Read-only over the engine lock; the node
/// answering holds its writes for the O(data) hashing — a startup-time
/// cost, same class as serving a join dump.
pub(super) async fn handle_digest(state: &Arc<ServerState>) -> Frame {
    // Same transaction quiesce as the dump: a digest that includes another
    // connection's uncommitted rows can win a repair election and become
    // the reference, or report convergence while the peer is only mid-tx.
    let _order = match tokio::time::timeout(SYNC_HOLD_TIMEOUT, lock_engine_for_write(state)).await {
        Ok(Some(g)) => g,
        Ok(None) | Err(_) => {
            return Frame::new(
                proto::RESP_ERROR,
                err_payload("digest: write path busy; retry later"),
            );
        }
    };
    let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
    match db.digests() {
        Ok(list) => match serde_json::to_vec(&list) {
            Ok(payload) => Frame::new(proto::RESP_DIGEST, payload),
            Err(e) => Frame::new(proto::RESP_ERROR, err_payload(&format!("digest: {e}"))),
        },
        Err(e) => Frame::new(proto::RESP_ERROR, err_payload(&format!("digest: {e}"))),
    }
}

/// Terminal outcome of replaying a dump on the joining node.
pub(super) enum JoinApply {
    /// Dump applied; syncing gate lifted.
    Applied,
    /// A client wrote here before the dump landed: keep that data, stop
    /// trying to join (normal fan-out keeps the node converged).
    LocalData,
    Failed(String),
}

/// Ask one peer for the cluster state and return the dump script.
pub(super) async fn request_sync(state: &Arc<ServerState>, peer: &str) -> std::io::Result<String> {
    // No whole-attempt budget: a multi-GB dump stream legitimately exceeds
    // any fixed one (the old 90s cap made big joins deterministically fail
    // and retry forever). Liveness comes from the per-connection timeouts
    // (dial/hello/auth) and the per-frame IO_TIMEOUT below — a peer that
    // stalls mid-stream still errors out.
    let (mut stream, hello, mut replay) = open_peer_conn(
        peer,
        state.transport_key.as_ref(),
        fanout_auth(state),
        state.tls_out.as_ref(),
    )
    .await?;
    let wire: WireKey = state.transport_key.as_ref().map(|k| (k, &hello));
    let frame = replication_frame(
        proto::REQ_SYNC,
        state
            .advertise
            .as_deref()
            .map(|a| a.as_bytes().to_vec())
            .unwrap_or_default(),
        wire,
    );
    write_frame_on(&mut stream, &frame).await?;
    let mut script = String::new();
    loop {
        let f = tokio::time::timeout(
            IO_TIMEOUT,
            read_response_frame_with_guard(&mut stream, wire, &mut replay),
        )
        .await??;
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
}

/// Roll an explicit transaction back, verifying it actually closed. A
/// silently failed ROLLBACK leaves the global transaction open with no
/// owner: every later client write waits out the 30s queue window and
/// fails until restart. Refuse to limp in that state.
pub(super) fn rollback_or_abort(db: &mut docsql_core::engine::Database) {
    let _ = db.execute("ROLLBACK");
    if db.in_transaction() {
        eprintln!(
            "fatal: transaction could not be rolled back (I/O failure?); \
             aborting instead of wedging every future write"
        );
        std::process::abort();
    }
}

/// Replay the dump exactly like the join protocol prescribes: under the
/// write path, only when still fresh, inside one transaction (a failure
/// rolls back and leaves the node fresh for the next attempt). On success
/// the join-intake queue replays right after the dump (still under the
/// write path) and the gate closes.
pub(super) async fn apply_sync(
    state: &Arc<ServerState>,
    script: &str,
    peer: &str,
    heads: &[(String, u64)],
) -> JoinApply {
    let Some(order) = lock_engine_for_write(state).await else {
        return JoinApply::Failed("timed out waiting for the open transaction".into());
    };
    {
        let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
        if has_user_tables(&db) {
            eprintln!("bootstrap sync aborted: local data appeared before the dump landed");
            return JoinApply::LocalData;
        }
        if let Err(e) = db.execute("BEGIN") {
            return JoinApply::Failed(format!("BEGIN: {e}"));
        }
        // A panic inside the replay (engine bug, task abort) must not leave
        // an ownerless open transaction: every later write would queue
        // against it forever. Roll back, then surface the failure — the
        // bootstrap retry loop treats it like any failed attempt.
        let batch = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            db.execute_batch(script)
        })) {
            Ok(b) => b,
            Err(panic) => {
                rollback_or_abort(&mut db);
                let msg = panic
                    .downcast_ref::<&str>()
                    .map(|s| s.to_string())
                    .or_else(|| panic.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "unknown panic".to_string());
                return JoinApply::Failed(format!("snapshot replay panicked: {msg}"));
            }
        };
        if let Some(err) = batch.error {
            rollback_or_abort(&mut db);
            return JoinApply::Failed(format!("statement {}: {}", err.statement, err.message));
        }
        if let Err(e) = db.execute("COMMIT") {
            rollback_or_abort(&mut db);
            return JoinApply::Failed(format!("COMMIT: {e}"));
        }
    }
    // Shared post-snapshot tail (join + rejoin repair): seed the
    // probe-round floor, drain the queue (snapshot < queued < direct),
    // free the write path, then raise every origin's position to its
    // CURRENT head. The floor is stale by the whole transfer; leaving it
    // low makes the next rejoin replay snapshot-covered ops, error, and
    // degrade to the snapshot fallback. The re-probe happens only after
    // the write path is released: network round-trips must not stall
    // client writes.
    finish_snapshot_adopt(
        state,
        order,
        heads,
        "bootstrap",
        "bootstrap sync",
        peer,
        script.len(),
    )
    .await;
    JoinApply::Applied
}

/// Sync-gate intake for a replicated write: while the gate is open the
/// write queues and is acknowledged on the spot — making the origin wait
/// for the apply would deadlock its write path against the snapshot it is
/// waiting on. None (gate closed) tells the caller to execute directly.
pub(super) async fn gate_enqueue(
    state: &ServerState,
    origin: Option<(String, u64)>,
    sql: &str,
    allow_system_table: bool,
) -> Option<Frame> {
    let mut gate = state.sync_queue.lock().await;
    if gate.closed {
        return None;
    }
    // The origin id is frame-controlled payload: budget what it actually
    // occupies in the queue, not just the sql text, or a compat-mode
    // sender with fat ids pins unbounded memory inside the entry cap.
    let origin_bytes = origin
        .as_ref()
        .map(|(node_id, _)| node_id.len())
        .unwrap_or(0);
    if gate.pending.len() >= MAX_SYNC_QUEUE_ENTRIES
        || gate.pending_bytes + sql.len() + origin_bytes > MAX_SYNC_QUEUE_BYTES
    {
        // Refuse the ack: the origin sees a failed fan-out leg, and the
        // digest check on the next rejoin degrades to snapshot repair —
        // the documented convergence path — instead of the window pinning
        // unbounded memory.
        return Some(Frame::new(
            proto::RESP_ERROR,
            err_payload("sync window saturated; retry after bootstrap"),
        ));
    }
    gate.pending_bytes += sql.len() + origin_bytes;
    gate.pending.push_back(QueuedWrite {
        origin,
        sql: sql.to_string(),
        allow_system_table,
        pubsub_notify: None,
    });
    Some(Frame::new(
        proto::RESP_AFFECTED,
        1u64.to_le_bytes().to_vec(),
    ))
}

/// [`gate_enqueue`] for a replication PUBLISH's INSERT: same window and
/// budget, plus the (channel, ts, payload) triple the drain owes local
/// subscribers after the replay lands.
pub(super) async fn gate_enqueue_pubsub(
    state: &ServerState,
    sql: &str,
    notify: (String, i64, String),
) -> Option<Frame> {
    let channel = notify.0.clone();
    let mut gate = state.sync_queue.lock().await;
    if gate.closed {
        return None;
    }
    if gate.pending.len() >= MAX_SYNC_QUEUE_ENTRIES
        || gate.pending_bytes + sql.len() > MAX_SYNC_QUEUE_BYTES
    {
        return Some(Frame::new(
            proto::RESP_ERROR,
            err_payload("sync window saturated; retry after bootstrap"),
        ));
    }
    gate.pending_bytes += sql.len();
    gate.pending.push_back(QueuedWrite {
        origin: None,
        sql: sql.to_string(),
        allow_system_table: true,
        pubsub_notify: Some(notify),
    });
    // Ack in the PUBLISH response shape (id unknown until the replay
    // assigns it; the ORIGIN's own id is what its client sees). The
    // receivers count keeps the origin's fan-out accounting identical to
    // a live leg's.
    let receivers = state.pubsub.count_receivers(&channel).await;
    let body = serde_json::json!({
        "columns": ["id", "receivers"],
        "rows": [[0, receivers]]
    });
    Some(Frame::new(
        proto::RESP_ROWS,
        serde_json::to_vec(&body).unwrap_or_default(),
    ))
}

/// Push queued PUBLISH replays to local subscribers. Called after every
/// drain concludes with the write order RELEASED (notify awaits subscriber
/// sends and must not run under the gate's engine hold).
pub(super) async fn flush_gate_pubsub_notify(state: &Arc<ServerState>) {
    let backlog: Vec<(String, i64, i64, String)> = {
        let mut gate = state.sync_queue.lock().await;
        std::mem::take(&mut gate.pubsub_after_drain)
    };
    for (channel, id, ts, payload) in backlog {
        state.pubsub.notify(&channel, id, ts, &payload).await;
    }
}

/// RESP_ROWS frame from a column list and pre-rendered row tuples.
pub(super) fn rows_frame(columns: serde_json::Value, rows: Vec<serde_json::Value>) -> Frame {
    let body = serde_json::json!({ "columns": columns, "rows": rows });
    Frame::new(
        proto::RESP_ROWS,
        serde_json::to_vec(&body).unwrap_or_default(),
    )
}

/// Shared tail of both snapshot-adopt paths (join and rejoin repair): seed
/// the probe-round floor (it predates the freeze, so every queued sequenced
/// write at or under it is inside the snapshot and the drain skips it),
/// drain the queue, free the write path, then raise every origin's position
/// to its CURRENT head — the floor is stale by the whole transfer, and
/// leaving it low would make the next rejoin replay snapshot-covered ops,
/// error, and degrade to the snapshot fallback. The re-probe happens only
/// after the write path is released: network round-trips must not stall
/// client writes.
pub(super) async fn finish_snapshot_adopt<'a>(
    state: &Arc<ServerState>,
    order: tokio::sync::MutexGuard<'a, ()>,
    heads: &[(String, u64)],
    event: &str,
    label: &str,
    peer: &str,
    script_len: usize,
) {
    seed_positions(state, heads);
    let drain_failures = drain_sync_queue(state, true, true).await;
    drop(order);
    flush_gate_pubsub_notify(state).await;
    if drain_failures == 0 {
        seed_fresh_positions(state).await;
    } else {
        // Some acknowledged queued write did not land: raising positions to
        // the origins' current heads would mark the gap as covered and hide
        // it from every future incremental catch-up. Leave the positions
        // where they are; the next restart's digest repair pulls the gap.
        eprintln!(
            "sync: {drain_failures} queued write(s) failed to replay; \
             positions left unseeded so a later rejoin re-pulls them"
        );
    }
    // The snapshot replayed through the engine batch path (not execute_sql),
    // so the epoch/has-users bookkeeping needs an explicit nudge: user
    // connections on THIS node must re-resolve, and if the snapshot carried
    // the first user, anonymous access closes now.
    state
        .grants_epoch
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let has = {
        let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
        db.any_user_exists().unwrap_or(false)
    };
    state
        .has_users
        .store(has, std::sync::atomic::Ordering::SeqCst);
    eprintln!("{label} from {peer} complete");
    querylog::sync_event(
        &state.sync_log,
        event,
        peer,
        None,
        true,
        Some(format!("{script_len} bytes")),
    );
}

/// Replay the join-intake queue in arrival order and close the gate
/// (snapshot < queued < direct is the total order; see
/// [ServerState::sync_queue]). Callers run it at every terminal state of
/// the bootstrap — the queue holds acknowledged writes that must land no
/// matter how the join concluded.
///
/// `order_held` must be true only when the caller already holds
/// `write_order` (the apply_* snapshot paths). Terminal-state drains run
/// unlocked and pass false: the drain then takes the write path itself so
/// replays cannot merge into a client's open transaction — its ROLLBACK
/// would drop writes the origins already had acknowledged. A stuck client
/// transaction delays the gate closing but never merges queued writes
/// into it; the drain waits with a loud heartbeat rather than give up on
/// acknowledged data.
pub(super) async fn drain_sync_queue(
    state: &Arc<ServerState>,
    order_held: bool,
    close: bool,
) -> u64 {
    // drain_inner flushes the gate's pubsub notifications while the write
    // order is still held (ordering contract; see its tail).
    drain_inner(state, order_held, close).await
}

pub(super) async fn drain_inner(state: &Arc<ServerState>, order_held: bool, close: bool) -> u64 {
    /// Extra in-place attempts for an acknowledged queued write whose replay
    /// errored: a transient failure (I/O, lock) must not strand data the
    /// origin already acknowledged. Permanent failures (duplicate keys from
    /// a snapshot-covered row) stay observable in `replay_failures`.
    const REPLAY_RETRIES: usize = 2;
    let _order = if order_held {
        None
    } else {
        let mut waits = 0u64;
        loop {
            match lock_engine_for_write(state).await {
                Some(order) => break Some(order),
                None => {
                    waits += 1;
                    if waits == 1 || waits.is_multiple_of(30) {
                        eprintln!(
                            "sync: drain waiting for an open client transaction \
                             ({waits} cycle(s)); acked queued writes must land"
                        );
                    }
                    tokio::time::sleep(BEGIN_QUEUE_POLL).await;
                }
            }
        }
    };
    let mut total_failures = 0u64;
    loop {
        // Peek-then-pop: the item stays in the queue until its replay
        // finishes. If this future is dropped mid-replay (the watchdog's
        // bounded budget), the item — and everything after it — remains in
        // `gate.pending`, so the hard-close accounting counts every
        // acknowledged write it is about to strand.
        let q = {
            let mut gate = state.sync_queue.lock().await;
            match gate.pending.front() {
                None => {
                    // Only a TERMINAL drain closes the gate: a process
                    // drain (mid-repair, between catch-up and the snapshot
                    // fallback) keeps it open so inbound replication writes
                    // keep queueing — closing here dropped the adoption
                    // window's writes back to the 30s direct-path timeout,
                    // the exact failure mode the queue exists to prevent.
                    if close {
                        gate.closed = true;
                    }
                    break;
                }
                Some(q) => q.clone(),
            }
        };
        // A sequenced write the adopted snapshot already covers (seq
        // at or under the position seeded from the probe round) is
        // skipped, not replayed — replaying it would only error on
        // the snapshot's own rows. Applied sequenced writes advance
        // their origin's position (apply-then-record, the same
        // invariant the live REQ_SQL_SEQ path keeps).
        if let Some((origin, seq)) = &q.origin {
            let covered = {
                let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
                db.position_get(origin)
                    .ok()
                    .flatten()
                    .is_some_and(|pos| pos >= *seq)
            };
            if covered {
                let mut gate = state.sync_queue.lock().await;
                let popped = gate.pending.pop_front().expect("front peeked at loop head");
                gate.pending_bytes -= popped.sql.len()
                    + popped
                        .origin
                        .as_ref()
                        .map(|(node_id, _)| node_id.len())
                        .unwrap_or(0);
                continue;
            }
        }
        let seq_pos = q
            .origin
            .as_ref()
            .map(|(origin, seq)| (origin.as_str(), *seq));
        let mut resp = execute_sql(
            state,
            &q.sql,
            q.allow_system_table,
            true,
            None,
            true,
            seq_pos,
            None,
            None,
        )
        .await;
        let mut attempts = 0usize;
        let mut failures = 0u64;
        while resp.frame_type == proto::RESP_ERROR && attempts < REPLAY_RETRIES {
            attempts += 1;
            // Replay failures leave no partial state (each replay is its own
            // write unit), so a bounded retry is safe and recovers transient
            // I/O/lock errors without stranding an acknowledged write.
            resp = execute_sql(
                state,
                &q.sql,
                q.allow_system_table,
                true,
                None,
                true,
                seq_pos,
                None,
                None,
            )
            .await;
        }
        if resp.frame_type == proto::RESP_ERROR {
            let msg = String::from_utf8_lossy(&resp.payload).into_owned();
            failures += 1;
            eprintln!("sync: queued replay failed after retries: {msg}");
            state
                .replay_failures
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            querylog::sync_event(
                &state.sync_log,
                "bootstrap",
                "",
                Some(&q.sql),
                false,
                Some(msg),
            );
        } else {
            // Position was fused into the replay's commit unit
            // (see execute_sql's `seq_pos`).
            querylog::record(state, "sync", &q.sql, 0.0, &resp, true);
            if let Some((channel, ts, payload)) = &q.pubsub_notify {
                // The write order is held here, so the engine's identity
                // is exactly this INSERT's autoincrement id — the same id
                // the origin assigned (stores stay in lockstep).
                let id = {
                    let db = state.db.read().unwrap_or_else(|p| p.into_inner());
                    db.last_insert_id().unwrap_or(i64::MIN)
                };
                if id != i64::MIN {
                    let mut gate = state.sync_queue.lock().await;
                    gate.pubsub_after_drain
                        .push((channel.clone(), id, *ts, payload.clone()));
                }
            }
        }
        {
            let mut gate = state.sync_queue.lock().await;
            let popped = gate.pending.pop_front().expect("front peeked at loop head");
            gate.pending_bytes -= popped.sql.len()
                + popped
                    .origin
                    .as_ref()
                    .map(|(node_id, _)| node_id.len())
                    .unwrap_or(0);
        }
        total_failures += failures;
    }
    // Flush the backlog notifications BEFORE `_order` drops (it drops at
    // this function's return): per-connection push frames arrive in id
    // order only if the queue's messages (smaller ids) notify before the
    // write order is released — a local PUBLISH acquiring the order next
    // would otherwise deliver a LARGER id first. The drain wrapper used to
    // flush after this return, outside the guard, violating its own
    // ordering contract.
    flush_gate_pubsub_notify(state).await;
    total_failures
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
pub(super) async fn bootstrap_sync(state: Arc<ServerState>, fresh: bool) {
    let peers = state.peers.lock().await.clone();
    if !fresh {
        repair_sync(state, peers).await;
        return;
    }
    let mut last_err = String::from("no peer answered");
    // Once a peer is seen holding data, "serving fresh" is no longer a safe
    // conclusion: an empty node would accept client writes while the data
    // holders exist, and the user set never converges. Keep retrying (the
    // sync gate stays open, so client writes queue within its budget)
    // instead of giving up; only a mesh where no peer ever answered may
    // conclude "serve fresh" after the rounds.
    let mut ever_saw_data = false;
    let mut round = 0usize;
    loop {
        round += 1;
        let mut saw_data = false;
        let mut saw_empty = false;
        let mut heads: Vec<(String, u64)> = Vec::new();
        for peer in &peers {
            match probe_peer_info(&state, peer).await {
                Ok(info) => {
                    if let (Some(node_id), Some(head)) = (&info.node_id, info.journal_head) {
                        heads.push((node_id.clone(), head));
                    }
                    if info.tables > 0 || info.user_state {
                        saw_data = true;
                        match join_from(&state, peer, &heads).await {
                            JoinApply::Applied => {
                                verify_join_convergence(&state, peers.clone()).await;
                                return;
                            }
                            JoinApply::LocalData => {
                                drain_sync_queue(&state, false, true).await;
                                flush_gate_pubsub_notify(&state).await;
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
            drain_sync_queue(&state, false, true).await;
            flush_gate_pubsub_notify(&state).await;
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
        if saw_data {
            ever_saw_data = true;
        }
        if round >= SYNC_ROUNDS {
            if ever_saw_data {
                // Local client writes may have arrived while the peer probe
                // was retrying (SQL is never gated — only the snapshot is).
                // If this node now holds data, it is no longer a joiner:
                // serve and let fan-out (plus the peer's own rejoin repair)
                // converge the mesh — retrying a snapshot would wipe the
                // writes this node just acknowledged.
                let local_has_data = {
                    let db = state.db.write().unwrap_or_else(|p| p.into_inner());
                    has_user_tables(&db)
                };
                if local_has_data {
                    eprintln!(
                        "bootstrap sync: local writes arrived while peers hold data; \
                         serving (fan-out keeps the mesh converged)"
                    );
                    drain_sync_queue(&state, false, true).await;
                    querylog::sync_event(
                        &state.sync_log,
                        "bootstrap",
                        "",
                        None,
                        false,
                        Some("local data arrived during bootstrap".into()),
                    );
                    return;
                }
                // Data exists somewhere but no transfer succeeded: serving
                // fresh would silently join an empty node into a live mesh.
                // Retry with a slower cadence; the open gate queues client
                // writes (bounded) and their errors say "joining" rather
                // than accepting data a later snapshot would wipe.
                eprintln!(
                    "bootstrap sync: peers hold data but the join keeps failing \
                     ({last_err}); still retrying — this node is NOT serving yet"
                );
                querylog::sync_event(
                    &state.sync_log,
                    "bootstrap",
                    "",
                    None,
                    false,
                    Some(format!("join failing, still retrying: {last_err}")),
                );
                round = 0;
                tokio::time::sleep(SYNC_RETRY_DELAY * 10).await;
                continue;
            }
            break;
        }
        tokio::time::sleep(SYNC_RETRY_DELAY).await;
    }
    eprintln!("bootstrap sync gave up after {SYNC_ROUNDS} rounds ({last_err}); serving fresh");
    drain_sync_queue(&state, false, true).await;
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
pub(super) async fn join_from(
    state: &Arc<ServerState>,
    peer: &str,
    heads: &[(String, u64)],
) -> JoinApply {
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
pub(super) const REPAIR_ROUNDS: usize = 20;

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
pub(super) async fn repair_sync(state: Arc<ServerState>, peers: Vec<String>) {
    for round in 0..REPAIR_ROUNDS {
        // Probe every peer for digests + journal info, concurrently.
        let mut reports: Vec<(String, Vec<TableDigest>)> = Vec::new();
        let mut infos: Vec<(String, PeerInfo)> = Vec::new();
        for (peer, digests, info) in probe_all_peer_reports(&state, &peers).await {
            match (digests, info) {
                (Ok(digests), Ok(info)) => {
                    reports.push((peer.clone(), digests));
                    infos.push((peer, info));
                }
                (Ok(digests), Err(e)) => {
                    // Digests arrived, journal info did not: this peer can
                    // still serve a snapshot, never incremental catch-up.
                    eprintln!("repair: journal probe of {peer} failed: {e}");
                    reports.push((peer, digests));
                }
                (Err(e), _) => {
                    eprintln!("repair: digest probe of {peer} failed: {e}");
                }
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
        // Post-catch-up digest views, when the incremental path produced
        // them this round (used by the phase-2 election below).
        type DigestViews = (
            Vec<docsql_core::engine::TableDigest>,
            Vec<(String, Vec<docsql_core::engine::TableDigest>)>,
        );
        let mut stale_views: Option<DigestViews> = None;
        let local = {
            let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
            db.digests()
        };
        let local = match local {
            Ok(d) => d,
            Err(e) => {
                eprintln!("repair: local digests failed: {e}");
                break;
            }
        };
        // No "any peer agrees with local" shortcut here: on an even split
        // (A,B | C,D) both sides have one agreeing peer, and returning early
        // on each restart would freeze the split forever — `decide_repair`
        // exists precisely so the LOCAL GROUP must win the election (its
        // `winner.3 && agreeing_peers > 0` Converged branch is this check's
        // correct form). Every reachable peer report flows on to the
        // catch-up plan and the election below.

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
                        // Sequenced queued writes skip ops the pulled
                        // range (or the seeded position) already covers
                        // and advance positions as they apply; plain
                        // queued writes replay as-is and leave positions
                        // untouched (a position may lag its origin until
                        // the next pull; replays are order-idempotent,
                        // and any residual divergence still lands in the
                        // snapshot fallback).
                        drain_sync_queue(&state, false, false).await;
                        flush_gate_pubsub_notify(&state).await;
                        // Fresh digest check: the mesh kept writing while
                        // the backlog replayed.
                        let fresh = probe_all_digests(&state, &peers).await;
                        let fresh_local = {
                            let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
                            db.digests()
                        };
                        if let Ok(fresh_local) = fresh_local {
                            // Keep the post-catch-up views for the phase-2
                            // election below: the round-start local/reports
                            // are stale by everything the replay applied.
                            stale_views = Some((fresh_local.clone(), fresh.clone()));
                            // Convergence must be the ELECTION's verdict
                            // (local group wins decide_repair), not "any
                            // single peer agrees": in an even split
                            // {A,B}=X | {C,D}=Y, catching up to B alone
                            // left A==B and this shortcut declared
                            // convergence — freezing the split exactly the
                            // way the (removed) loop-top shortcut did.
                            if matches!(
                                decide_repair(&fresh_local, &fresh),
                                RepairDecision::Converged
                            ) {
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
                                drain_sync_queue(&state, false, true).await;
                                flush_gate_pubsub_notify(&state).await;
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
        // Prefer the post-catch-up digests when the replay path produced
        // them: electing on round-start views could pick a reference group
        // the mesh has already moved past.
        let (local, reports) = match stale_views.take() {
            Some((fl, f)) => (fl, f),
            None => (local, reports),
        };
        match decide_repair(&local, &reports) {
            RepairDecision::Converged => {
                drain_sync_queue(&state, false, true).await;
                flush_gate_pubsub_notify(&state).await;
                return;
            }
            RepairDecision::Serve => {
                eprintln!(
                    "rejoin repair (round {}): no agreeing peer; election \
                     picked this node's state as the reference",
                    round + 1
                );
                drain_sync_queue(&state, false, true).await;
                flush_gate_pubsub_notify(&state).await;
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
    drain_sync_queue(&state, false, true).await;
    flush_gate_pubsub_notify(&state).await;
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
pub(crate) struct CatchupTask {
    addr: String,
    node_id: String,
    from: u64,
    to: u64,
}

/// Build the incremental catch-up plan, or None when catch-up is
/// impossible: any REACHED peer without journal info (old version), any
/// unknown position (never adopted from that origin), or any position
/// outside the peer's retained window (trimmed past) all force the
/// snapshot path. (Peers that could not be probed at all are absent from
/// `infos`; a partial catch-up runs and the digest re-check decides
/// whether a snapshot is still needed — positions can only lag the data,
/// so a partial catch-up is always safe.)
pub(crate) async fn catchup_plan(
    state: &Arc<ServerState>,
    infos: &[(String, PeerInfo)],
) -> Option<Vec<CatchupTask>> {
    let mut plan = Vec::new();
    for (addr, info) in infos {
        let node_id = info.node_id.clone()?;
        let head = info.journal_head?;
        let oldest = info.journal_oldest?;
        let pos = {
            let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
            db.position_get(&node_id).ok().flatten()
        }?;
        if pos > head || pos.saturating_add(1) < oldest {
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
pub(crate) async fn run_catchup(
    state: &Arc<ServerState>,
    plan: &[CatchupTask],
) -> Result<(), String> {
    for task in plan {
        // 与 live REQ_SQL_SEQ 应用路径同一把 per-origin 锁:拉取期间同源的
        // 直连扇出写可能正在途(源端的第一次尝试超时后已发出下一条)。
        // 不持锁时拉取到的旧行像会与新写的直连应用交错,非交换写乱序落地,
        // 而下方的位点推进随后掩盖分叉。锁序 origin → write_order 与 live
        // 路径一致(catch_up_from 内部经 execute_sql 走写路径)。逐任务
        // 持有:不同 origin 的任务在循环里本就串行,不引入跨 origin 锁序。
        let (origin_lock, origin_rejected) = origin_apply_lock(state, &task.node_id).await;
        if origin_rejected {
            // 与 live 路径同语义:超过 origin 上限时拒绝而非绕过串行化;
            // catch-up 失败后由 digest 复验走快照兜底。
            return Err(format!(
                "origin {} rejected (max {MAX_APPLY_ORIGINS} tracked origins)",
                task.node_id
            ));
        }
        let _origin_guard = origin_lock.lock_owned().await;
        let head = catch_up_from(state, &task.addr, task.from)
            .await
            .map_err(|e| format!("pull from {}: {e}", task.addr))?;
        {
            let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
            advance_position(&mut db, &task.node_id, head);
        }
    }
    Ok(())
}

/// Seed the catch-up positions from the heads sampled during the probe
/// round: they predate the snapshot freeze, so they are a conservative
/// floor — everything at or under them is guaranteed to be inside the
/// adopted snapshot, which lets the queue drain skip those sequenced
/// writes. The floor is deliberately stale; seed_fresh_positions raises
/// it to the origins' current heads once the snapshot and the queue
/// landed. Origins that could not be probed stay position-less (no
/// incremental trust) and are covered by snapshot repair.
pub(super) fn seed_positions(state: &Arc<ServerState>, heads: &[(String, u64)]) {
    let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
    for (node_id, head) in heads {
        // Never LOWER an existing position: a blind position_set was safe
        // only by context (fresh node, or positions cleared in the same
        // transaction as the snapshot); max keeps the seeding safe even
        // when those distant invariants change.
        let existing = db.position_get(node_id).ok().flatten().unwrap_or(0);
        if *head > existing {
            if let Err(e) = db.position_set(node_id, *head) {
                eprintln!("catchup position seed failed: {e}");
            }
        }
    }
}

/// After a snapshot + queue replay landed, raise every origin's position
/// to its journal head as of now (see apply_sync for why the probe-round
/// floor must not survive). A freshly probed head can exceed what this
/// node actually holds only when a fan-out was lost inside the transfer
/// window — the same trust the live REQ_SQL_SEQ receipts already get,
/// healed by digest repair on divergence. Probes that fail leave that
/// origin's position untouched (no incremental trust).
pub(super) async fn seed_fresh_positions(state: &Arc<ServerState>) {
    let peers = state.peers.lock().await.clone();
    for peer in &peers {
        let info = match probe_peer_info(state, peer).await {
            Ok(info) => info,
            Err(_) => continue,
        };
        let (Some(node_id), Some(head)) = (&info.node_id, info.journal_head) else {
            continue;
        };
        let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
        advance_position(&mut db, node_id, head);
    }
}

/// Outcome of comparing the local digests with the peers' reports (see
/// `repair_sync`).
pub(super) enum RepairDecision {
    /// A reachable peer reports the same state: nothing to repair.
    Converged,
    /// No peer agrees, but the election picked this node's state as the
    /// reference: close the sync gate and serve — the divergent peers
    /// pull from here.
    Serve,
    /// Adopt this peer's snapshot.
    Pull(String),
}

/// Decide what the rejoin repair should do (see `repair_sync` for the
/// election rules). Every node runs the same rules over its own view, so
/// the mesh converges on one reference without cross-node negotiation:
/// the largest group of identical states wins (local counts itself as a
/// member), ties go to the higher row count, then to the serialized-digest
/// order. An empty group is never adopted over local data.
pub(super) fn decide_repair(
    local: &[TableDigest],
    reports: &[(String, Vec<TableDigest>)],
) -> RepairDecision {
    // Group the peer reports by identical state.
    let mut groups: Vec<(&Vec<TableDigest>, Vec<&str>)> = Vec::new();
    for (peer, digests) in reports {
        match groups.iter_mut().find(|(d, _)| *d == digests) {
            Some((_, members)) => members.push(peer.as_str()),
            None => groups.push((digests, vec![peer.as_str()])),
        }
    }
    // The old "any peer agrees" shortcut let an even split (A,B | C,D)
    // persist forever: each side agreed with one peer and declared
    // convergence, so neither ever adopted the other's state. The local
    // group must *win the election*, not merely exist.
    let agreeing_peers = groups
        .iter()
        .find(|(d, _)| *d == local)
        .map(|(_, m)| m.len())
        .unwrap_or(0);
    let local_rows: u64 = local.iter().map(|t| t.rows).sum();
    let local_key = (local_rows, serde_json::to_vec(local).unwrap_or_default());
    // (winner source peer, group size, state key, is_local)
    let mut winner: (&str, usize, (u64, Vec<u8>), bool) =
        ("", agreeing_peers + 1, local_key.clone(), true);
    for (digests, members) in &groups {
        let key = (
            digests.iter().map(|t| t.rows).sum(),
            serde_json::to_vec(digests).unwrap_or_default(),
        );
        let better = members.len() > winner.1
            || (members.len() == winner.1
                && (key.0 > winner.2 .0 || (key.0 == winner.2 .0 && key.1 < winner.2 .1)));
        if better {
            winner = (members[0], members.len(), key, false);
        }
    }
    if winner.3 {
        return if agreeing_peers > 0 {
            RepairDecision::Converged
        } else {
            RepairDecision::Serve
        };
    }
    if winner.2 .0 == 0 {
        // An empty group never wins over local rows (row count breaks the
        // size tie only when local's group is as large; a larger empty group
        // must still not wipe the last copy).
        return if local_rows > 0 {
            RepairDecision::Serve
        } else {
            RepairDecision::Converged
        };
    }
    RepairDecision::Pull(winner.0.to_string())
}

/// Replace this node's whole user state with the cluster snapshot:
/// inside one transaction, wipe the local catalog (bypassing DROP's FK
/// guards — the snapshot defines the entire truth) and replay the dump.
/// A failure rolls back to the pre-repair state, divergence and all.
/// Inbound replication writes queue on the open sync gate meanwhile and
/// replay in arrival order right after — snapshot < queued < direct is
/// the total order (see [ServerState::sync_queue]). On success the
/// catch-up positions are re-established: the probe-round floor seeds
/// before the drain (letting it skip snapshot-covered sequenced writes),
/// and the origins' freshly probed heads raise it afterwards, so later
/// rejoins can pull increments instead of snapshots.
pub(super) async fn apply_repair_sync(
    state: &Arc<ServerState>,
    script: &str,
    peer: &str,
    heads: &[(String, u64)],
) -> JoinApply {
    let Some(order) = lock_engine_for_write(state).await else {
        return JoinApply::Failed("timed out waiting for the open transaction".into());
    };
    {
        let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
        if let Err(e) = db.execute("BEGIN") {
            return JoinApply::Failed(format!("BEGIN: {e}"));
        }
        if let Err(e) = db.wipe_user_tables() {
            rollback_or_abort(&mut db);
            return JoinApply::Failed(format!("catalog wipe: {e}"));
        }
        // Same panic guard as `apply_sync`: an ownerless open transaction
        // would wedge every later write until restart.
        let batch = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            db.execute_batch(script)
        })) {
            Ok(b) => b,
            Err(panic) => {
                rollback_or_abort(&mut db);
                let msg = panic
                    .downcast_ref::<&str>()
                    .map(|s| s.to_string())
                    .or_else(|| panic.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "unknown panic".to_string());
                return JoinApply::Failed(format!("repair replay panicked: {msg}"));
            }
        };
        if let Some(err) = batch.error {
            rollback_or_abort(&mut db);
            return JoinApply::Failed(format!("statement {}: {}", err.statement, err.message));
        }
        // The adopted snapshot replaces the state the old positions
        // described — drop them (a stale-high survivor would over-claim
        // coverage of a snapshot that no longer contains those ops) inside
        // the SAME transaction as the snapshot: a crash between an
        // autocommit clear and the next restart would otherwise survive the
        // Converged digest exit, which never re-clears positions.
        if let Err(e) = db.positions_clear() {
            rollback_or_abort(&mut db);
            return JoinApply::Failed(format!("position reset: {e}"));
        }
        // The adjudication is final: this node's own journal entries the
        // majority snapshot did not adopt must never replay again — a
        // later repair on some peer would otherwise pull them back and
        // resurrect discarded writes. Void their texts (seqs stay
        // allocated so the counter and every recorded position keep their
        // meaning; a replayed entry is a parsed no-op). Third parties that
        // pull the voided range land on the digest re-check → snapshot,
        // which is where a resurrected range would have sent them anyway.
        if let Err(e) = db.journal_void_all() {
            rollback_or_abort(&mut db);
            return JoinApply::Failed(format!("journal void: {e}"));
        }
        if let Err(e) = db.execute("COMMIT") {
            rollback_or_abort(&mut db);
            return JoinApply::Failed(format!("COMMIT: {e}"));
        }
    }
    // The voided journal text lives on in any exported incrementals; they
    // must not survive the adoption (see invalidate_incremental_exports).
    // The epoch bump also discards an in-flight export that read the
    // pre-void journal and is still writing its file.
    state
        .pitr_epoch
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    crate::backup::invalidate_incremental_exports(&state.backup_dir);
    // The S3 copies of the voided chain must not outlive the adoption
    // either: a DR restore pulls them back and the majority-discarded
    // writes replay (see invalidate_incremental_exports_remote).
    crate::backup::invalidate_incremental_exports_remote(state).await;
    // Probe-round floor before the drain, fresh heads after — same order
    // and same reasoning as the join path (see finish_snapshot_adopt).
    finish_snapshot_adopt(
        state,
        order,
        heads,
        "repair",
        "rejoin repair",
        peer,
        script.len(),
    )
    .await;
    JoinApply::Applied
}

/// Post-join sanity check: the snapshot + queue replay is only as
/// complete as the fan-outs that reached this node while the gate was
/// open. A fan-out lost to a transient refusal on either side would
/// otherwise sit as a silent divergence until the NEXT restart's digest
/// repair — the join path had no re-verification at all. Probe the peers
/// once; if none agrees with the bootstrapped state, hand the node to the
/// rejoin-repair machinery immediately (incremental pull when possible,
/// snapshot otherwise).
pub(super) async fn verify_join_convergence(state: &Arc<ServerState>, peers: Vec<String>) {
    let local = {
        let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
        db.digests()
    };
    let Ok(local) = local else {
        return;
    };
    let mut reachable = 0usize;
    let mut agreeing = 0usize;
    for (peer, d) in probe_all_digests(state, &peers).await {
        reachable += 1;
        if d == local {
            agreeing += 1;
        } else {
            eprintln!("join verify: state differs from {peer}");
        }
    }
    if reachable > 0 && agreeing == 0 {
        eprintln!(
            "join verify: no reachable peer agrees with the bootstrapped state; \
             running rejoin repair now instead of waiting for the next restart"
        );
        querylog::sync_event(
            &state.sync_log,
            "bootstrap",
            "",
            None,
            false,
            Some("post-join digest verification failed; repair engaged".into()),
        );
        repair_sync(state.clone(), peers).await;
    }
}

/// Connect + auth + send one zero-payload replication probe frame; returns
/// the response frame untouched — the caller validates the frame type and
/// parses the payload. Shared by the digest/status probes.
pub(super) async fn probe_frame(
    state: &ServerState,
    peer: &str,
    frame_type: u16,
) -> std::io::Result<Frame> {
    let (mut stream, hello, mut replay) = open_peer_conn(
        peer,
        state.transport_key.as_ref(),
        fanout_auth(state),
        state.tls_out.as_ref(),
    )
    .await?;
    let wire: WireKey = state.transport_key.as_ref().map(|k| (k, &hello));
    let frame = replication_frame(frame_type, vec![], wire);
    write_frame_on(&mut stream, &frame).await?;
    read_response_frame_with_guard(&mut stream, wire, &mut replay).await
}

/// One peer's table digests over REQ_DIGEST (see `repair_sync`). Errors
/// mean "unknown" (unreachable / auth mismatch), never "divergent". The
/// frame rides FLAG_REPLICATION like every node-internal frame — under
/// cluster-token auth a peer-role connection only accepts replication
/// traffic.
pub(crate) async fn probe_peer_digests(
    state: &ServerState,
    peer: &str,
) -> std::io::Result<Vec<TableDigest>> {
    let resp = probe_frame(state, peer, proto::REQ_DIGEST).await?;
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

/// Concurrently probe every peer for its table digests. Unreachable peers
/// are logged and dropped ("unknown", never "divergent"); the returned
/// order matches `peers`.
pub(super) async fn probe_all_digests(
    state: &Arc<ServerState>,
    peers: &[String],
) -> Vec<(String, Vec<TableDigest>)> {
    let mut probes = Vec::new();
    for peer in peers {
        let st = state.clone();
        let target = peer.clone();
        probes.push(tokio::spawn(async move {
            let d = probe_peer_digests(&st, &target).await;
            (target, d)
        }));
    }
    let mut out = Vec::new();
    for probe in probes {
        match probe.await {
            Ok((peer, Ok(d))) => out.push((peer, d)),
            Ok((peer, Err(e))) => eprintln!("repair: digest probe of {peer} failed: {e}"),
            Err(e) => eprintln!("repair: probe task failed: {e}"),
        }
    }
    out
}

/// Concurrently probe every peer for digests + journal info, keeping the
/// two results separate so a peer that answered digests but not status is
/// still usable for snapshot repair. Task-level failures are logged and
/// dropped; peer-level errors ride in the tuples for the caller's framing.
pub(super) async fn probe_all_peer_reports(
    state: &Arc<ServerState>,
    peers: &[String],
) -> Vec<(
    String,
    std::io::Result<Vec<TableDigest>>,
    std::io::Result<PeerInfo>,
)> {
    let mut probes = Vec::new();
    for peer in peers {
        let st = state.clone();
        let target = peer.clone();
        probes.push(tokio::spawn(async move {
            let digests = probe_peer_digests(&st, &target).await;
            let info = probe_peer_info(&st, &target).await;
            (target, digests, info)
        }));
    }
    let mut out = Vec::new();
    for probe in probes {
        match probe.await {
            Ok(report) => out.push(report),
            Err(e) => eprintln!("repair: probe task failed: {e}"),
        }
    }
    out
}

/// What one peer reported about itself over REQ_STATUS: its user-table
/// count (fresh-join probe), its persistent identity, and its journal
/// window. `node_id`/journal fields are None from peers predating
/// catch-up replication — incremental repair is impossible against them
/// and the snapshot path takes over. `restore_running` drives the
/// cluster-wide restore mutual-exclusion probe (see backup.rs).
#[derive(Debug, Clone)]
pub(crate) struct PeerInfo {
    pub(crate) tables: u64,
    /// Peer holds user accounts (see the status payload's `user_state`).
    pub(crate) user_state: bool,
    pub(crate) node_id: Option<String>,
    pub(crate) journal_head: Option<u64>,
    pub(crate) journal_oldest: Option<u64>,
    pub(crate) restore_running: bool,
}

/// One peer's status + journal window over REQ_STATUS.
pub(crate) async fn probe_peer_info(state: &ServerState, peer: &str) -> std::io::Result<PeerInfo> {
    let resp = probe_frame(state, peer, proto::REQ_STATUS).await?;
    if resp.frame_type != proto::RESP_STATUS {
        return Err(std::io::Error::other(format!(
            "{peer}: status probe answered {} {}",
            resp.frame_type,
            String::from_utf8_lossy(&resp.payload)
        )));
    }
    let v: serde_json::Value = serde_json::from_slice(&resp.payload)
        .map_err(|e| std::io::Error::other(format!("bad status payload: {e}")))?;
    // A successful probe proves the node is reachable: drop any fan-out
    // backoff so writes flow again without waiting the window out.
    note_fanout_success(state, peer).await;
    Ok(PeerInfo {
        tables: v["totals"]["tables"].as_u64().unwrap_or(0),
        user_state: v["user_state"].as_bool() == Some(true),
        node_id: v["cluster_id"].as_str().map(String::from),
        journal_head: v["journal_head"].as_u64(),
        journal_oldest: v["journal_oldest"].as_u64(),
        restore_running: v["backup"]["restore"]["running"].as_bool() == Some(true),
    })
}

/// Parse a REQ_SQL_SEQ payload: [u64 seq][u32 len][node_id][sql].
pub(super) fn parse_seq_frame(payload: &[u8]) -> Option<(u64, String, String)> {
    if payload.len() < 12 {
        return None;
    }
    let seq = u64::from_le_bytes(payload[..8].try_into().ok()?);
    let id_len = u32::from_le_bytes(payload[8..12].try_into().ok()?) as usize;
    // Real origin ids are GUID-shaped (~36 bytes). An oversized length is
    // malformed at best and a queue-budget evasion at worst (the sync
    // window accounts origin bytes, and the direct path already caps at
    // MAX_ORIGIN_ID_LEN).
    if id_len == 0 || id_len > MAX_ORIGIN_ID_LEN {
        return None;
    }
    let rest = payload.get(12..)?;
    let node_id = std::str::from_utf8(rest.get(..id_len)?).ok()?.to_string();
    let sql = proto::decode_sql(rest.get(id_len..)?).ok()?;
    Some((seq, node_id, sql))
}

/// Distinct REQ_SQL_SEQ origins tracked concurrently. `node_id` rides in the
/// frame payload, so without a cap a hostile (or compat-mode token-level)
/// sender grows `apply_locks` and the persisted `_cluster_pos` rows forever.
/// Real clusters have a handful of members; past the cap the frame is
/// rejected, which the origin observes as fan-out failure and the cluster
/// heals through digest/snapshot repair.
pub(super) const MAX_APPLY_ORIGINS: usize = 256;
/// Bound on one origin id's bytes (real ids are GUID-shaped, ~36 bytes).
pub(super) const MAX_ORIGIN_ID_LEN: usize = 128;

/// Per-origin apply lock; `origin_rejected` is true when the origin is new
/// and the cap is exhausted (the caller answers with an error frame).
pub(super) async fn origin_apply_lock(
    state: &Arc<ServerState>,
    node_id: &str,
) -> (std::sync::Arc<tokio::sync::Mutex<()>>, bool) {
    let mut locks = state.apply_locks.lock().await;
    if locks.contains_key(node_id) {
        return (
            locks.get(node_id).expect("checked just above").clone(),
            false,
        );
    }
    if node_id.is_empty() || node_id.len() > MAX_ORIGIN_ID_LEN || locks.len() >= MAX_APPLY_ORIGINS {
        return ((std::sync::Arc::new(tokio::sync::Mutex::new(()))), true);
    }
    let lock = locks
        .entry(node_id.to_string())
        .or_insert_with(|| std::sync::Arc::new(tokio::sync::Mutex::new(())))
        .clone();
    (lock, false)
}

/// Pack journal entries into one RESP_CATCHUP payload, respecting the
/// frame budget but always making progress (at least one entry).
/// Returns the payload and the number of entries packed.
pub(super) fn pack_catchup_entries(entries: &[(u64, String)], budget: usize) -> (Vec<u8>, usize) {
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
pub(super) fn decode_catchup_entries(payload: &[u8]) -> std::io::Result<Vec<(u64, String)>> {
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
pub(super) async fn handle_catchup(
    state: &Arc<ServerState>,
    frame: &Frame,
    tx: &mpsc::Sender<Frame>,
) {
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
    const BUDGET: usize = SYNC_CHUNK_BYTES;
    // The terminating head is the position the requester will adopt, so
    // it is sampled up front and the stream never serves past it: a head
    // taken after the batches could cover entries appended while the pull
    // ran, and the requester would count ops it never received (positions
    // may lag, never lead). Anything committed past the head simply waits
    // for the next pull.
    let head = {
        let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
        db.journal_head().unwrap_or(0)
    };
    let mut after = after;
    // Continuity audit: the requester will adopt `head` as its position,
    // so every seq in (after, head] must actually be served. A trim (or a
    // crash-lost entry) that removes part of the range mid-pull would
    // otherwise be invisible — journal_range just skips the missing seqs,
    // the stream ends cleanly, and the requester's position leaps over
    // data it never received (positions may lag, never lead). Detecting a
    // hole here turns the silent divergence into an explicit error the
    // requester answers with snapshot adoption.
    let mut expected = after.saturating_add(1);
    let mut last_served = after;
    loop {
        let batch = {
            let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
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
        // Batches are seq-ordered, so the first entry past the sampled
        // head ends both the batch and the pull.
        let served = batch.iter().take_while(|(seq, _)| *seq <= head).count();
        if served == 0 {
            break;
        }
        let hole = batch[..served]
            .iter()
            .enumerate()
            .find_map(|(i, (seq, _))| (*seq != expected.saturating_add(i as u64)).then_some(*seq));
        if let Some(seq) = hole {
            let _ = tx
                .send(Frame::new(
                    proto::RESP_ERROR,
                    err_payload(&format!(
                        "catchup: journal hole before seq {seq} \
                         (entry trimmed or lost); snapshot repair required"
                    )),
                ))
                .await;
            return;
        }
        let mut idx = 0;
        while idx < served {
            // A single entry the requester cannot read (its inbound frame cap
            // is RECV_CAP) would kill the connection mid-pull. Fail the pull
            // explicitly instead, so the requester adopts a snapshot.
            if batch[idx].1.len() + 12 > RECV_CAP {
                let _ = tx
                    .send(Frame::new(
                        proto::RESP_ERROR,
                        err_payload(&format!(
                            "catchup: journal entry at seq {} is too large ({} bytes) \
                             for one frame; snapshot repair required",
                            batch[idx].0,
                            batch[idx].1.len()
                        )),
                    ))
                    .await;
                return;
            }
            let (payload, packed) = pack_catchup_entries(&batch[idx..served], BUDGET);
            if tx
                .send(Frame::new(proto::RESP_CATCHUP, payload))
                .await
                .is_err()
            {
                return; // requester went away mid-stream
            }
            idx += packed;
        }
        expected = batch[served - 1].0 + 1;
        last_served = batch[served - 1].0;
        if served < batch.len() {
            break;
        }
        after = batch.last().expect("non-empty batch").0;
    }
    if last_served != head {
        // The stream ended short of the sampled head: the tail of the
        // requested range is gone from the journal (trimmed mid-pull).
        let _ = tx
            .send(Frame::new(
                proto::RESP_ERROR,
                err_payload(&format!(
                    "catchup: journal ends at {last_served} but head is {head} \
                     (trimmed mid-pull); snapshot repair required"
                )),
            ))
            .await;
        return;
    }
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
pub(super) async fn catch_up_from(
    state: &Arc<ServerState>,
    peer: &str,
    after: u64,
) -> std::io::Result<u64> {
    // No whole-attempt budget: each replayed entry durably commits (its own
    // WAL fsync), so a full CATCHUP_WINDOW (default 100k entries) exceeds
    // any fixed cap — the old 90s ceiling degraded every large catch-up to
    // snapshot repair, which then failed the same way. Liveness comes from
    // the connection timeouts and the per-frame IO_TIMEOUT.
    let (mut stream, hello, mut replay) = open_peer_conn(
        peer,
        state.transport_key.as_ref(),
        fanout_auth(state),
        state.tls_out.as_ref(),
    )
    .await?;
    let wire: WireKey = state.transport_key.as_ref().map(|k| (k, &hello));
    let frame = replication_frame(proto::REQ_CATCHUP, after.to_le_bytes().to_vec(), wire);
    write_frame_on(&mut stream, &frame).await?;
    loop {
        let f = tokio::time::timeout(
            IO_TIMEOUT,
            read_response_frame_with_guard(&mut stream, wire, &mut replay),
        )
        .await??;
        match f.frame_type {
            proto::RESP_CATCHUP => {
                for (_, sql) in decode_catchup_entries(&f.payload)? {
                    let resp =
                        execute_sql(state, &sql, false, true, None, false, None, None, None).await;
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
}
