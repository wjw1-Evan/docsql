use super::*;

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
