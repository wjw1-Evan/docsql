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
