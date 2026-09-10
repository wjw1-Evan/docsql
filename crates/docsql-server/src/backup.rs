//! Automatic backups: periodic logical snapshots via
//! `Database::dump_script()` — the same dump the cluster join serves, so
//! system tables (`_cluster_log`, `_cluster_pos`, `_pubsub_messages`,
//! `_cluster_id`) are excluded and the script re-applies wholesale.
//!
//! Consistency copies the REQ_SYNC serving pattern: hold the write path
//! (`write_order`, waiting out any open transaction), capture the dump
//! under the engine lock, then release everything before touching the
//! filesystem — the O(data) dump happens under the lock, the file write
//! never blocks writers. Per-node point-in-time consistency; each node in
//! a cluster backs up independently.
//!
//! Lock discipline: the backup-state mutex (`ServerState::backup`) is
//! never held across `write_order`/engine acquisition, and the engine
//! guards are dropped before the shared backup state is updated — the
//! two locks never nest in either order, so REQ_STATUS can read backup
//! state without deadlock concerns.
//!
//! Files land in the backup directory (default `<db dir>/backups`, i.e.
//! `/data/backups` in the containers — inside the data volume) as
//! `backup-<UTC stamp>.sql`, written to a `.tmp` name and renamed into
//! place (atomic on the same filesystem). The stamp has fixed width and
//! millisecond resolution, so lexicographic order is chronological.
//! Retention keeps the newest `backup_keep` files and prunes only files
//! matching the backup naming pattern.
//!
//! Restore (`REQ_BACKUP` action `"restore"`) replays a backup file's
//! statements one by one through the normal write path (`execute_sql`),
//! so every statement commits locally AND fans out to the peers — the
//! whole cluster converges to the backup's state, exactly like the manual
//! runbook of piping the file into `docsql-cli`. The node must hold the
//! backup file itself (its own backup directory); restore runs in the
//! background with progress in the shared status. Semantics: tables in
//! the backup are dropped and recreated with the backup's data; tables
//! created after the backup are not touched. The replay holds the write
//! path for its whole duration — client writes on this node queue (and
//! error after the 30s deadline), so nothing interleaves into the
//! replayed stream; fan-ins from peers are refused meanwhile and pulled
//! back from their journals by the post-replay convergence pass, which
//! then verifies every reachable peer's digest and reports `converged`
//! (a still-diverged node heals on its own restart repair). Restores are
//! mutually exclusive cluster-wide: the initiating node probes every
//! peer's status and refuses while one is already running there.

use crate::querylog;
use crate::{ConnRole, ServerState};
use docsql_core::proto::{self, Frame};
use std::path::Path;
use std::sync::Arc;

/// Outcome of the last backup attempt, reported by REQ_STATUS/REQ_BACKUP.
#[derive(Clone, Debug)]
pub struct BackupStatus {
    pub ts_ms: u64,
    /// Backup file name (empty on failure).
    pub file: String,
    pub ok: bool,
    pub error: Option<String>,
}

/// State of the last/current restore attempt: replays a backup file's
/// statements through the normal write path, so the whole cluster
/// converges to the backup's state (see the module docs).
#[derive(Clone, Debug)]
pub struct RestoreStatus {
    pub ts_ms: u64,
    pub file: String,
    pub running: bool,
    pub ok: bool,
    pub error: Option<String>,
    /// Statements applied so far / in the backup script (progress).
    pub applied: usize,
    pub total: usize,
    /// Post-replay cluster verification: Some(true) when every reachable
    /// peer's digest equals this node's, Some(false) when a reachable
    /// peer still differs (its restart repair heals it), None while
    /// running / failed / no peers. Reports truth instead of assuming the
    /// fan-out reached everyone.
    pub converged: Option<bool>,
    /// Human-readable follow-up (unreachable peers, unusable journals).
    pub note: Option<String>,
}

impl RestoreStatus {
    fn started(file: &str, total: usize) -> Self {
        RestoreStatus {
            ts_ms: now_ms(),
            file: file.to_string(),
            running: true,
            ok: false,
            error: None,
            applied: 0,
            total,
            converged: None,
            note: None,
        }
    }
}

/// Shared backup state: one backup/restore runs at a time (timer tick and
/// manual triggers dedupe on the flags; a restore excludes a backup and
/// vice versa — both contend for the same write path). `last` reports the
/// newest backup attempt, `restore` the newest restore attempt.
#[derive(Default)]
pub struct BackupShared {
    pub running: bool,
    pub last: Option<BackupStatus>,
    pub restore: Option<RestoreStatus>,
}

/// true = this caller owns the backup; false = one is already in flight
/// (or a restore is running — they share the write path).
fn try_begin_backup(state: &ServerState) -> bool {
    let mut b = state.backup.lock().unwrap_or_else(|p| p.into_inner());
    if b.running || b.restore.as_ref().is_some_and(|r| r.running) {
        return false;
    }
    b.running = true;
    true
}

/// Run one backup (caller must have won `try_begin_backup`): dump under
/// the write path, write the file, prune old backups, record the outcome
/// in the shared state and the sync log (visible on the console logs page).
async fn finish_backup(state: &Arc<ServerState>) -> Result<String, String> {
    let res = backup_inner(state).await;
    let status = match &res {
        Ok(file) => BackupStatus {
            ts_ms: now_ms(),
            file: file.clone(),
            ok: true,
            error: None,
        },
        Err(e) => BackupStatus {
            ts_ms: now_ms(),
            file: String::new(),
            ok: false,
            error: Some(e.clone()),
        },
    };
    querylog::sync_event(
        &state.sync_log,
        "backup",
        "",
        None,
        status.ok,
        match &status.error {
            Some(e) => Some(e.clone()),
            None => Some(format!(
                "{} bytes written",
                file_bytes(&state.backup_dir.join(&status.file))
            )),
        },
    );
    let mut b = state.backup.lock().unwrap_or_else(|p| p.into_inner());
    b.running = false;
    b.last = Some(status);
    res
}

async fn backup_inner(state: &Arc<ServerState>) -> Result<String, String> {
    let Some(_order) = crate::lock_engine_for_write(state).await else {
        return Err("backup timed out waiting for the open transaction".into());
    };
    // Block scope: the engine guard drops before any further await (the
    // dump itself is synchronous, O(data) in memory).
    let script = {
        let mut db = state.db.lock().unwrap_or_else(|p| p.into_inner());
        db.dump_script().map_err(|e| format!("backup dump: {e}"))?
    };
    drop(_order);
    // All locks released: filesystem work never blocks writers. The stamp
    // is monotonic against the directory's contents, so name order stays
    // creation order even when the wall clock steps backwards.
    let name = format!("backup-{}.sql", next_stamp(&state.backup_dir, now_ms()));
    std::fs::create_dir_all(&state.backup_dir).map_err(|e| format!("backup dir: {e}"))?;
    let tmp = state.backup_dir.join(format!("{name}.tmp"));
    std::fs::write(&tmp, script.as_bytes()).map_err(|e| format!("backup write: {e}"))?;
    std::fs::rename(&tmp, state.backup_dir.join(&name))
        .map_err(|e| format!("backup rename: {e}"))?;
    prune_backups(&state.backup_dir, state.backup_keep);
    Ok(name)
}

/// Periodic backup task: the first tick is immediate when no usable
/// backup exists or the newest one is older than one interval — a restart
/// still yields a fresh backup, but a restart STORM (rolling releases,
/// crash loops) no longer writes a near-identical snapshot per restart,
/// which used to squeeze the real history out of the keep-N window. Ticks
/// during startup sync (join / rejoin repair) are skipped — the gate
/// closes when the node's startup state settles, and the next tick
/// snapshots the settled data. A tick while a manual backup runs is
/// skipped silently.
pub async fn backup_task(state: Arc<ServerState>, interval_secs: u64) {
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(interval_secs.max(1)));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut first = true;
    loop {
        tick.tick().await;
        if first {
            first = false;
            if let Some(newest) = read_backup_files(&state.backup_dir).pop() {
                let age_secs = std::fs::metadata(state.backup_dir.join(&newest))
                    .ok()
                    .and_then(|m| m.modified().ok())
                    .and_then(|t| t.elapsed().ok())
                    .map(|d| d.as_secs());
                if age_secs.is_some_and(|age| age < interval_secs) {
                    continue;
                }
            }
        }
        if !state.sync_queue.lock().await.closed {
            continue; // startup sync still in flight; snapshot the settled state
        }
        if try_begin_backup(&state) {
            if let Err(e) = finish_backup(&state).await {
                eprintln!("backup failed: {e}");
            }
        }
    }
}

/// REQ_BACKUP: `{"action": "list"}` reports the backup directory and last
/// attempt; `{"action": "trigger"}` starts one backup now (rejected for
/// read-only connections like every other durable-writing operation);
/// `{"action": "restore", "file": "backup-….sql"}` replays that backup
/// through the write path (whole-cluster restore). trigger/restore answer
/// immediately — the outcome shows up in REQ_STATUS/REQ_BACKUP.
pub async fn handle_backup(state: &Arc<ServerState>, role: ConnRole, frame: &Frame) -> Frame {
    let action = if frame.payload.is_empty() {
        "list".to_string()
    } else {
        match serde_json::from_slice::<serde_json::Value>(&frame.payload) {
            Ok(v) => v["action"].as_str().unwrap_or("list").to_string(),
            Err(e) => {
                return Frame::new(
                    proto::RESP_ERROR,
                    crate::err_payload(&format!("backup: bad payload: {e}")),
                )
            }
        }
    };
    match action.as_str() {
        "list" => {
            let payload = backup_payload(state);
            Frame::new(proto::RESP_BACKUP, payload)
        }
        "trigger" => {
            if role == ConnRole::ReadOnly {
                return Frame::new(
                    proto::RESP_ERROR,
                    crate::err_payload("read-only token; writes are not permitted"),
                );
            }
            // Same window rule as the timer and restore: a snapshot taken
            // mid-bootstrap captures a state the bootstrap is about to
            // replace — misleading to hand to an operator.
            if !state.sync_queue.lock().await.closed {
                return Frame::new(
                    proto::RESP_ERROR,
                    crate::err_payload("backup: node is still in startup sync; retry later"),
                );
            }
            if !try_begin_backup(state) {
                return Frame::new(
                    proto::RESP_ERROR,
                    crate::err_payload("backup already in progress"),
                );
            }
            let st = state.clone();
            tokio::spawn(async move {
                if let Err(e) = finish_backup(&st).await {
                    eprintln!("backup failed: {e}");
                }
            });
            Frame::new(proto::RESP_AFFECTED, b"backup started".to_vec())
        }
        "restore" => {
            if role == ConnRole::ReadOnly {
                return Frame::new(
                    proto::RESP_ERROR,
                    crate::err_payload("read-only token; writes are not permitted"),
                );
            }
            // A replica accepts no writes at all; every replayed statement
            // would be refused, so fail the request up front.
            if state.read_only.load(std::sync::atomic::Ordering::SeqCst) {
                return Frame::new(
                    proto::RESP_ERROR,
                    crate::err_payload("read-only replica; PROMOTE to accept writes"),
                );
            }
            let file = serde_json::from_slice::<serde_json::Value>(&frame.payload)
                .ok()
                .and_then(|v| v["file"].as_str().map(String::from))
                .unwrap_or_default();
            if !valid_backup_name(&file) {
                return Frame::new(
                    proto::RESP_ERROR,
                    crate::err_payload(
                        "restore: expected {\"file\": \"backup-<stamp>.sql\"} - the bare name of a file in this node's backup directory",
                    ),
                );
            }
            if !state.backup_dir.join(&file).is_file() {
                return Frame::new(
                    proto::RESP_ERROR,
                    crate::err_payload(&format!("restore: no such backup file {file}")),
                );
            }
            if !state.sync_queue.lock().await.closed {
                return Frame::new(
                    proto::RESP_ERROR,
                    crate::err_payload("restore: node is still in startup sync; retry later"),
                );
            }
            // Cluster-wide mutual exclusion: a restore converges EVERY peer
            // via fan-out, so a second restore running on another node
            // would interleave two conflicting DROP/CREATE/INSERT streams
            // across the whole mesh. The local flag cannot see it — probe
            // the peers' status first. (Simultaneous submissions remain a
            // tiny race; the replay itself stays serialized by write_order
            // per node, so the outcome is messy but never corrupt.)
            {
                let peers = state.peers.lock().await.clone();
                for peer in &peers {
                    if let Ok(info) = crate::probe_peer_info(state, peer).await {
                        if info.restore_running {
                            return Frame::new(
                                proto::RESP_ERROR,
                                crate::err_payload(&format!(
                                    "restore: already running on peer {peer}"
                                )),
                            );
                        }
                    }
                }
            }
            {
                let mut b = state.backup.lock().unwrap_or_else(|p| p.into_inner());
                let busy = b.running || b.restore.as_ref().is_some_and(|r| r.running);
                if busy {
                    return Frame::new(
                        proto::RESP_ERROR,
                        crate::err_payload("backup/restore already in progress"),
                    );
                }
                b.restore = Some(RestoreStatus::started(&file, 0));
            }
            let st = state.clone();
            tokio::spawn(async move {
                if let Err(e) = run_restore(&st, &file).await {
                    eprintln!("restore of {file} failed: {e}");
                }
            });
            Frame::new(proto::RESP_AFFECTED, b"restore started".to_vec())
        }
        other => Frame::new(
            proto::RESP_ERROR,
            crate::err_payload(&format!("backup: unknown action \"{other}\"")),
        ),
    }
}

/// A restorable file is a bare backup name from this node's backup
/// directory: no separators, no traversal, no look-alike paths.
fn valid_backup_name(name: &str) -> bool {
    name.len() <= 128
        && name.starts_with("backup-")
        && name.ends_with(".sql")
        && !name.contains('/')
        && !name.contains('\\')
        && !name.contains("..")
}

/// Replay one backup file through the normal write path. Caller must have
/// claimed `BackupShared.restore`; this finishes the status either way,
/// including the post-replay cluster convergence pass.
async fn run_restore(state: &Arc<ServerState>, file: &str) -> Result<usize, String> {
    let res = restore_inner(state, file).await;
    // Convergence pass on success: the replay fanned out every statement,
    // but peers offline (or mid-fan-out-failure) during the restore missed
    // statements. Used to be "wait for their next restart repair" — now
    // the restoring node pulls the missed increments itself and then
    // reports the verified truth instead of a hopeful ok=true.
    let mut converged = None;
    let mut note = None;
    if res.is_ok() {
        let peers = state.peers.lock().await.clone();
        let mut infos = Vec::new();
        let mut unreachable = Vec::new();
        for peer in &peers {
            match crate::probe_peer_info(state, peer).await {
                Ok(info) => infos.push((peer.clone(), info)),
                Err(_) => unreachable.push(peer.clone()),
            }
        }
        // Pull what THIS node missed: fan-ins were refused while the write
        // path was frozen by the replay; the origins hold them in their
        // journals. Unknown positions or trimmed windows leave the plan
        // unusable — the note says so.
        match crate::catchup_plan(state, &infos).await {
            Some(plan) if !plan.is_empty() => {
                if let Err(e) = crate::run_catchup(state, &plan).await {
                    eprintln!("restore: post-replay catch-up failed: {e}");
                }
            }
            Some(_) => {}
            None if !infos.is_empty() => {
                note = Some(
                    "some peer journals cannot be pulled incrementally \
                     (positions unknown or trimmed); a diverged node repairs on its restart"
                        .into(),
                );
            }
            None => {}
        }
        // Verify against every reachable peer's digest.
        let local = {
            let mut db = state.db.lock().unwrap_or_else(|p| p.into_inner());
            db.digests()
        };
        if let Ok(local) = local {
            if peers.is_empty() {
                converged = Some(true);
            } else {
                let mut all = true;
                let mut diverged = Vec::new();
                for (peer, _) in &infos {
                    match crate::probe_peer_digests(state, peer).await {
                        Ok(d) if d == local => {}
                        Ok(_) => {
                            all = false;
                            diverged.push(peer.clone());
                        }
                        Err(_) => {
                            all = false;
                            diverged.push(format!("{peer} (unreachable)"));
                        }
                    }
                }
                converged = Some(all);
                if !all {
                    note = Some(format!(
                        "cluster not verified converged: {}; a diverged node \
                         repairs on its restart",
                        diverged.join(", ")
                    ));
                }
            }
        }
        if !unreachable.is_empty() {
            let suffix = format!(
                "unreachable during verification: {}",
                unreachable.join(", ")
            );
            note = Some(match note {
                Some(n) => format!("{n}; {suffix}"),
                None => suffix,
            });
        }
    }
    let applied = {
        let mut b = state.backup.lock().unwrap_or_else(|p| p.into_inner());
        let mut applied = 0;
        if let Some(r) = b.restore.as_mut() {
            if r.file == file {
                applied = match &res {
                    Ok(n) => *n,
                    // Keep the progress: how far the replay got matters
                    // more than a zero on failure.
                    Err(_) => r.applied,
                };
                r.running = false;
                r.ok = res.is_ok();
                r.error = res.as_ref().err().cloned();
                r.applied = applied;
                r.converged = converged;
                r.note = note.clone();
            }
        }
        applied
    };
    querylog::sync_event(
        &state.sync_log,
        "restore",
        file,
        None,
        res.is_ok(),
        match res.as_ref().err() {
            Some(e) => Some(e.clone()),
            None => Some(format!(
                "{applied} statements replayed{}",
                match (&converged, &note) {
                    (Some(true), _) => ", cluster verified converged".to_string(),
                    (Some(false), Some(n)) => format!(", {n}"),
                    _ => String::new(),
                }
            )),
        },
    );
    res
}

async fn restore_inner(state: &Arc<ServerState>, file: &str) -> Result<usize, String> {
    // Filesystem work before any engine lock: the script is O(data).
    let script = std::fs::read_to_string(state.backup_dir.join(file))
        .map_err(|e| format!("restore read: {e}"))?;
    // A backup of an empty database is an empty script: restoring it is a
    // clean no-op (post-backup tables survive), not a parse error.
    let stmts = if script.trim().is_empty() {
        Vec::new()
    } else {
        docsql_core::stmt::split_statements(&script).map_err(|e| format!("restore parse: {e}"))?
    };
    let total = stmts.len();
    {
        let mut b = state.backup.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(r) = b.restore.as_mut() {
            if r.file == file {
                r.total = total;
            }
        }
    }
    // One write-order acquisition for the WHOLE replay: statements used to
    // contend per statement, letting client writes interleave between them
    // — a client INSERT into a just-recreated empty table would collide
    // with the replayed rows (PK conflict) and abort the restore halfway,
    // already fanned out. Holding the path makes the replay deterministic;
    // lock_engine_for_write waited out any open transaction first, and no
    // new one can start while it is held (BEGIN needs the write path).
    // Fan-ins from other nodes' client writes are refused meanwhile (their
    // origins log the failure); the convergence pass below pulls them back
    // from the origins' journals.
    let order = crate::lock_engine_for_write(state)
        .await
        .ok_or_else(|| "restore timed out waiting for the open transaction".to_string())?;
    // Per-statement replay through the normal write path: each statement
    // commits locally, is journaled, and fans out to every peer — the
    // cluster converges to the backup's state (the dump drops everything
    // in one statement first, so replaying over live data is idempotent;
    // AUTOINCREMENT counters continue from the restored max).
    let mut replayed = 0usize;
    for (i, stmt) in stmts.iter().enumerate() {
        let resp = crate::execute_sql(state, stmt, false, false, None, true, None).await;
        if resp.frame_type == proto::RESP_ERROR {
            drop(order);
            return Err(format!(
                "statement {} of {total} failed: {}",
                i + 1,
                String::from_utf8_lossy(&resp.payload)
            ));
        }
        replayed = i + 1;
        let mut b = state.backup.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(r) = b.restore.as_mut() {
            if r.file == file {
                r.applied = i + 1;
            }
        }
    }
    drop(order);
    Ok(replayed)
}

/// REQ_BACKUP (list) / `status_payload().backup` body. Reads only the
/// backup-state mutex and the filesystem — never the engine lock.
pub fn backup_payload(state: &ServerState) -> Vec<u8> {
    let b = state.backup.lock().unwrap_or_else(|p| p.into_inner());
    let files = list_backups(&state.backup_dir);
    let body = serde_json::json!({
        "dir": state.backup_dir.display().to_string(),
        "interval_secs": state.backup_interval_secs,
        "keep": state.backup_keep,
        "running": b.running,
        "count": files.len(),
        "files": files,
        "last": b.last.as_ref().map(|l| serde_json::json!({
            "ts_ms": l.ts_ms,
            "file": l.file,
            "ok": l.ok,
            "error": l.error,
        })),
        "restore": b.restore.as_ref().map(|r| serde_json::json!({
            "ts_ms": r.ts_ms,
            "file": r.file,
            "running": r.running,
            "ok": r.ok,
            "error": r.error,
            "applied": r.applied,
            "total": r.total,
            "converged": r.converged,
            "note": r.note,
        })),
    });
    serde_json::to_vec(&body).unwrap_or_default()
}

/// One entry per backup file, newest first.
fn list_backups(dir: &Path) -> Vec<serde_json::Value> {
    let mut files: Vec<(String, u64, u64)> = read_backup_files(dir)
        .into_iter()
        .map(|name| {
            let meta = std::fs::metadata(dir.join(&name)).ok();
            let bytes = meta.as_ref().map(|m| m.len()).unwrap_or(0);
            let ts_ms = meta
                .and_then(|m| m.modified().ok())
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            (name, bytes, ts_ms)
        })
        .collect();
    files.sort_by(|a, b| b.0.cmp(&a.0));
    files
        .into_iter()
        .map(|(name, bytes, ts_ms)| {
            serde_json::json!({"name": name, "bytes": bytes, "ts_ms": ts_ms})
        })
        .collect()
}

/// Keep the newest `keep` backup files, delete the rest (only files in the
/// backup naming pattern are ever touched). Name order equals write order
/// by construction: `next_stamp` never emits a stamp at or below an
/// existing file's, so retention stays correct even through a clock roll
/// back (a plain mtime comparison would not — mtimes roll back too).
fn prune_backups(dir: &Path, keep: usize) {
    let keep = keep.max(1);
    // read_backup_files is sorted ascending: the head is the oldest.
    let mut names = read_backup_files(dir);
    while names.len() > keep {
        let victim = names.remove(0);
        if let Err(e) = std::fs::remove_file(dir.join(&victim)) {
            eprintln!("backup prune failed for {victim}: {e}");
            break;
        }
    }
}

/// A backup stamp strictly newer than every file already in the
/// directory: `now`, unless the clock is behind what we wrote before
/// (NTP correction, VM restore), in which case step one millisecond past
/// the newest existing stamp. Keeps lexicographic name order equal to
/// creation order, which retention and listing both rely on.
fn next_stamp(dir: &Path, now: u64) -> String {
    let mut ms = now;
    for name in read_backup_files(dir) {
        let stem = name
            .strip_prefix("backup-")
            .and_then(|s| s.strip_suffix(".sql"))
            .unwrap_or("");
        if let Some(existing) = parse_stamp(stem) {
            ms = ms.max(existing + 1);
        }
    }
    utc_stamp(ms)
}

/// Parse `YYYYMMDDTHHMMSSmmmZ` back to epoch milliseconds (inverse of
/// `utc_stamp`, same civil algorithm).
fn parse_stamp(s: &str) -> Option<u64> {
    if s.len() != 19 || !s.ends_with('Z') {
        return None;
    }
    let num = |a: usize, b: usize| s[a..b].parse::<u64>().ok();
    let (y, mo, d) = (num(0, 4)? as i64, num(4, 6)? as i64, num(6, 8)? as i64);
    let (h, mi, se, milli) = (num(9, 11)?, num(11, 13)?, num(13, 15)?, num(15, 18)?);
    // Inverse of civil_from_days (Howard Hinnant's days_from_civil).
    let y2 = if mo <= 2 { y - 1 } else { y };
    let era = if y2 >= 0 { y2 } else { y2 - 399 } / 400;
    let yoe = y2 - era * 400;
    let mp = if mo > 2 { mo - 3 } else { mo + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let secs = (days as u64) * 86_400 + h * 3_600 + mi * 60 + se;
    Some(secs * 1_000 + milli)
}

fn read_backup_files(dir: &Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    // Sorted: read_dir order is arbitrary, and every caller (retention,
    // listing, tests) reasons in name = time order.
    let mut names: Vec<String> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("backup-") && n.ends_with(".sql"))
        .collect();
    names.sort();
    names
}

fn file_bytes(p: &Path) -> u64 {
    std::fs::metadata(p).map(|m| m.len()).unwrap_or(0)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Fixed-width UTC stamp with millisecond resolution:
/// `20260910T081530123Z`. Lexicographic order is chronological.
fn utc_stamp(ms: u64) -> String {
    let secs = ms / 1000;
    let milli = ms % 1000;
    let (h, m, s) = ((secs % 86400) / 3600, (secs % 3600) / 60, secs % 60);
    let (y, mo, d) = civil_from_days((secs / 86400) as i64);
    format!("{y:04}{mo:02}{d:02}T{h:02}{m:02}{s:02}{milli:03}Z")
}

/// Days since the Unix epoch to (year, month, day) — Howard Hinnant's
/// civil-from-days algorithm (proleptic Gregorian, no date-time crate).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backup_names_are_validated_for_restore() {
        assert!(valid_backup_name("backup-20260910T081530123Z.sql"));
        assert!(valid_backup_name("backup-1.sql"));
        // Path traversal and separator games never pass.
        assert!(!valid_backup_name("../docsql.db"));
        assert!(!valid_backup_name("backup-../../etc/passwd.sql"));
        assert!(!valid_backup_name("backups/backup-1.sql"));
        assert!(!valid_backup_name(r"backup-1\..\x.sql"));
        assert!(!valid_backup_name("backup-.sql.tmp"));
        assert!(!valid_backup_name("other-1.sql"));
        assert!(!valid_backup_name(""));
        assert!(!valid_backup_name(&"backup-1.sql".repeat(30)));
    }

    #[test]
    fn prune_keeps_newest_and_never_touches_foreign_files() {
        let dir = tempfile::tempdir().unwrap();
        let put = |name: &str| {
            std::fs::write(dir.path().join(name), b"x").unwrap();
        };
        put("backup-a.sql");
        put("backup-a.sql.tmp"); // atomic-rename side file: not a backup
        put("backup-b.sql");
        put("backup-c.sql");
        put("notes.txt"); // not ours: retention must leave it alone
        assert_eq!(read_backup_files(dir.path()).len(), 3);

        prune_backups(dir.path(), 2);
        let left = read_backup_files(dir.path());
        assert_eq!(left, vec!["backup-b.sql", "backup-c.sql"]);
        // Foreign files survive: retention only ever deletes its own pattern.
        assert!(dir.path().join("notes.txt").is_file());
        assert!(dir.path().join("backup-a.sql.tmp").is_file());

        // keep=0 clamps to 1 (a deployment can never prune everything).
        prune_backups(dir.path(), 0);
        assert_eq!(read_backup_files(dir.path()), vec!["backup-c.sql"]);
    }

    #[test]
    fn utc_stamp_is_fixed_width_and_ordered() {
        // 2026-09-10T08:15:30.123Z
        let ms = 1_789_028_130_123u64;
        let s = utc_stamp(ms);
        assert_eq!(s, "20260910T081530123Z");
        assert_eq!(s.len(), 19);
        assert!(utc_stamp(ms + 1) > s);
        assert!(utc_stamp(ms + 1000) > utc_stamp(ms + 999));
        // Epoch and a leap-year day round-trip through the civil algorithm.
        assert_eq!(utc_stamp(0), "19700101T000000000Z");
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(19_723), (2024, 1, 1)); // 2024-01-01
    }

    #[test]
    fn stamps_round_trip_and_next_stamp_stays_monotonic() {
        // parse_stamp inverts utc_stamp across a spread of timestamps.
        for ms in [
            0u64,
            1,
            1_789_028_130_123,
            951_827_696_000, // 2000-02-29 (leap day)
            4_102_444_800_000,
        ] {
            assert_eq!(
                parse_stamp(&utc_stamp(ms)),
                Some(ms),
                "round trip failed for {ms}"
            );
        }
        assert_eq!(parse_stamp("not-a-stamp"), None);
        assert_eq!(parse_stamp("20260910T08153012Z"), None);

        // Clock rolled back (now older than existing files): next_stamp
        // steps one millisecond past the newest existing stamp, keeping
        // name order == creation order, which retention relies on.
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("backup-20260910T081530999Z.sql"), b"x").unwrap();
        let rolled_back_now = 1_700_000_000_000u64; // well before the file
        let next = next_stamp(dir.path(), rolled_back_now);
        assert_eq!(next, "20260910T081531000Z");
        assert!(next.as_str() > "20260910T081530999Z");
        // Normal case: now is ahead of an empty directory, the stamp just
        // tracks the clock.
        let empty = tempfile::tempdir().unwrap();
        assert_eq!(
            next_stamp(empty.path(), 1_789_028_130_123),
            "20260910T081530123Z"
        );
    }
}
