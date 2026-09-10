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

/// Shared backup state: one backup runs at a time (timer tick and manual
/// trigger dedupe on `running`), `last` reports the newest attempt.
#[derive(Default)]
pub struct BackupShared {
    pub running: bool,
    pub last: Option<BackupStatus>,
}

/// true = this caller owns the backup; false = one is already in flight.
fn try_begin_backup(state: &ServerState) -> bool {
    let mut b = state.backup.lock().unwrap_or_else(|p| p.into_inner());
    if b.running {
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
    // All locks released: filesystem work never blocks writers.
    let name = format!("backup-{}.sql", utc_stamp(now_ms()));
    std::fs::create_dir_all(&state.backup_dir).map_err(|e| format!("backup dir: {e}"))?;
    let tmp = state.backup_dir.join(format!("{name}.tmp"));
    std::fs::write(&tmp, script.as_bytes()).map_err(|e| format!("backup write: {e}"))?;
    std::fs::rename(&tmp, state.backup_dir.join(&name))
        .map_err(|e| format!("backup rename: {e}"))?;
    prune_backups(&state.backup_dir, state.backup_keep);
    Ok(name)
}

/// Periodic backup task: first tick is immediate (a restart yields a fresh
/// backup), then every `interval_secs`. Ticks during startup sync (join /
/// rejoin repair) are skipped — the gate closes when the node's startup
/// state settles, and the next tick snapshots the settled data. A tick
/// while a manual backup runs is skipped silently.
pub async fn backup_task(state: Arc<ServerState>, interval_secs: u64) {
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(interval_secs.max(1)));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
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
/// read-only connections like every other durable-writing operation) and
/// answers immediately — the outcome shows up in REQ_STATUS/REQ_BACKUP.
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
        other => Frame::new(
            proto::RESP_ERROR,
            crate::err_payload(&format!("backup: unknown action \"{other}\"")),
        ),
    }
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
/// backup naming pattern are ever touched).
fn prune_backups(dir: &Path, keep: usize) {
    let keep = keep.max(1);
    let mut names = read_backup_files(dir);
    names.sort();
    while names.len() > keep {
        let victim = names.remove(0);
        if let Err(e) = std::fs::remove_file(dir.join(&victim)) {
            eprintln!("backup prune failed for {victim}: {e}");
            break;
        }
    }
}

fn read_backup_files(dir: &Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    entries
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("backup-") && n.ends_with(".sql"))
        .collect()
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
}
