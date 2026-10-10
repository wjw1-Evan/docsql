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
//!
//! Remote copy (DOCSQL_BACKUP_S3_*): every finished backup AND incremental
//! segment is additionally PUT to an S3-compatible bucket (sha256 sidecar
//! included), so a lost data volume cannot take the only backup with it —
//! the remote pair is the off-volume recovery copy, and a restore whose
//! local file is missing fetches base + incremental chain back before
//! replaying. Uploads run inside the backup-operation window (all engine
//! locks are already dropped) and never fail the local backup that has
//! already fsynced — the outcome lands in the status payload, the sync
//! log and a metric instead. Remote retention mirrors the local keep-N
//! over the same naming patterns.

use crate::querylog;
use crate::{ConnRole, ServerState};
use docsql_core::now_ms;
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
    fn started(file: &str) -> Self {
        RestoreStatus {
            ts_ms: now_ms(),
            file: file.to_string(),
            running: true,
            ok: false,
            error: None,
            converged: None,
            note: None,
        }
    }
}

/// Outcome of the last remote-copy upload attempt (`DOCSQL_BACKUP_S3_*`),
/// reported by REQ_BACKUP list / the status payload. Separate from
/// [`BackupStatus`]: incremental uploads land here too, and a failed
/// upload must not fail the local backup that already fsynced.
#[derive(Clone, Debug)]
pub struct RemoteUpload {
    pub ts_ms: u64,
    pub file: String,
    pub ok: bool,
    pub error: Option<String>,
}

/// Shared backup state: one backup/restore runs at a time (timer tick and
/// manual triggers dedupe on the flags; a restore excludes a backup and
/// vice versa — both contend for the same write path). `last` reports the
/// newest backup attempt, `restore` the newest restore attempt,
/// `remote_last` the newest remote-copy upload (backup or incremental).
#[derive(Default)]
pub struct BackupShared {
    pub running: bool,
    pub last: Option<BackupStatus>,
    pub restore: Option<RestoreStatus>,
    /// Monotonic id of the current/last restore: a same-file retry must
    /// not be confused with the previous run by the drop guard (the file
    /// name alone collided in the window between the old run's internal
    /// completion and its guard's drop, clearing the NEW run's flag).
    pub restore_generation: u64,
    pub remote_last: Option<RemoteUpload>,
}

/// Live restore-replay counters, shared locklessly between the replay loop
/// and the status payloads: the replay holds `write_order` for its whole
/// duration, and the lock discipline forbids taking the backup-state mutex
/// under it (a REQ_STATUS reading backup would then block on the engine).
#[derive(Default)]
pub struct RestoreProgress {
    pub applied: std::sync::atomic::AtomicUsize,
    pub total: std::sync::atomic::AtomicUsize,
}

/// Resets `running` when dropped — including during a panic unwind, so a
/// dying backup task cannot wedge local backups (and, for restores, the
/// cluster-wide restore mutex) until restart.
struct RunningFlagGuard<'a>(&'a ServerState);
impl Drop for RunningFlagGuard<'_> {
    fn drop(&mut self) {
        self.0
            .backup
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .running = false;
    }
}

/// Same as [`RunningFlagGuard`] for the restore flag, matching the file
/// AND the restore generation so a stale guard never touches a newer
/// restore's status (an immediate same-file retry armed while the old
/// run's guard was still dropping).
struct RestoreFlagGuard(Arc<ServerState>, String, u64);
impl Drop for RestoreFlagGuard {
    fn drop(&mut self) {
        let mut b = self.0.backup.lock().unwrap_or_else(|p| p.into_inner());
        if b.restore_generation != self.2 {
            return;
        }
        if let Some(r) = b.restore.as_mut() {
            if r.file == self.1 && r.running {
                r.running = false;
            }
        }
    }
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
    let _running = RunningFlagGuard(state);
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
    let mut b = state.backup.lock().unwrap_or_else(|p| p.into_inner());
    b.running = false;
    b.last = Some(status.clone());
    // The sync-log trail must carry the remote-copy outcome: a nightly
    // backup whose off-volume copy failed is exactly the failure an
    // operator scans the logs page for.
    let remote = b.remote_last.clone();
    drop(b);
    let bytes = if status.file.is_empty() {
        String::new()
    } else {
        format!(
            "{} bytes written",
            docsql_core::file_bytes(&state.backup_dir.join(&status.file))
        )
    };
    let detail = match (&status.error, &remote) {
        (Some(e), _) => Some(e.clone()),
        (None, Some(r)) if !r.ok => Some(format!(
            "{bytes}; REMOTE COPY FAILED: {}",
            r.error.clone().unwrap_or_default()
        )),
        (None, Some(_)) => Some(format!("{bytes}; remote copy ok")),
        (None, None) => Some(bytes),
    };
    querylog::sync_event(&state.sync_log, "backup", "", None, status.ok, detail);
    res
}

async fn backup_inner(state: &Arc<ServerState>) -> Result<String, String> {
    let Some(_order) = crate::lock_engine_for_write(state).await else {
        return Err("backup timed out waiting for the open transaction".into());
    };
    // Block scope: the engine guard drops before any further await (the
    // dump itself is synchronous, O(data) in memory). Header and body stay
    // separate Strings: concatenating them here used to double the peak
    // (two full-library copies) while write_order and the engine write
    // lock were held.
    let (header, body) = {
        let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
        // PITR anchor: the journal position this dump covers. A restore to
        // a timestamp T replays journal entries with seq > this and
        // commit-time <= T on top of the dump. Recorded under the SAME
        // write-lock window as the dump, so entry N is inside the dump iff
        // its seq <= N_head — no replication-lag ambiguity.
        let head = db.journal_head().map_err(|e| format!("backup dump: {e}"))?;
        let dump = db.dump_script().map_err(|e| format!("backup dump: {e}"))?;
        (
            format!("-- docsql-backup v2 journal-seq={head} ts={}\n", now_ms()),
            dump,
        )
    };
    drop(_order);
    // All locks released: filesystem work never blocks writers. The stamp
    // is monotonic against the directory's contents, so name order stays
    // creation order even when the wall clock steps backwards.
    let name = format!("backup-{}.sql", next_stamp(&state.backup_dir, now_ms()));
    std::fs::create_dir_all(&state.backup_dir).map_err(|e| format!("backup dir: {e}"))?;
    let tmp = state.backup_dir.join(format!("{name}.tmp"));
    write_private_two(&tmp, header.as_bytes(), body.as_bytes())
        .map_err(|e| format!("backup write: {e}"))?;
    rename_synced(&tmp, &state.backup_dir.join(&name))
        .map_err(|e| format!("backup rename: {e}"))?;
    // Integrity sidecar (sha256sum format: "<hex>  <name>"), written
    // alongside the file it covers. Restore verifies it before replaying —
    // a corrupted dump must be caught at the door, not halfway through a
    // whole-cluster replay.
    let digest = docsql_core::kdf::sha256_parts(&[header.as_bytes(), body.as_bytes()]);
    let sidecar = state.backup_dir.join(format!("{name}.sha256"));
    let tmp = state.backup_dir.join(format!("{name}.sha256.tmp"));
    if let Err(e) = write_private(
        &tmp,
        format!("{}  {}\n", docsql_core::kdf::hex(&digest), name).as_bytes(),
    )
    .and_then(|()| rename_synced(&tmp, &sidecar))
    {
        // A body left without its sidecar is a "new but unverifiable"
        // backup: restore tolerates missing sidecars as legacy files and
        // would replay it unverified. Remove the body (and any .tmp) —
        // the failed status names it, and the next tick retries whole.
        let _ = std::fs::remove_file(state.backup_dir.join(&name));
        let _ = std::fs::remove_file(&tmp);
        return Err(format!("backup checksum: {e} ({name})"));
    }
    prune_backups(&state.backup_dir, state.backup_keep);
    // Remote copy: the local pair is durable — push both objects to the
    // bucket and mirror retention. Failures are recorded (metric, sync
    // log, status payload), never fail the local backup.
    if let Some(s3) = &state.backup_s3 {
        let hex_digest = docsql_core::kdf::hex(&digest);
        // Checksum-FIRST, and the object only follows a LANDED checksum:
        // a backup object without its sidecar is indistinguishable from a
        // pre-checksum-era legacy object on the restore side (which then
        // replays it unverified), while an orphaned sidecar is harmless —
        // the next tick retries the pair.
        let sidecar_landed = match std::fs::read(state.backup_dir.join(format!("{name}.sha256"))) {
            Ok(bytes) => upload_backup_bytes(state, s3, &format!("{name}.sha256"), &bytes)
                .await
                .is_ok(),
            Err(_) => false,
        };
        if sidecar_landed {
            upload_backup_file(state, s3, &name, &state.backup_dir.join(&name), &hex_digest).await;
        }
        reconcile_remote(s3, state).await;
        prune_remote(s3, state).await;
    }
    Ok(name)
}

/// Upload one object and record the outcome in the shared state + the
/// sync-log-visible metric counters. Loud by design: a remote copy that
/// silently stopped working defeats its entire purpose.
async fn upload_backup_file(
    state: &Arc<ServerState>,
    s3: &crate::s3::S3Client,
    name: &str,
    path: &Path,
    sha256_hex: &str,
) {
    let key = s3.config().object_key(name);
    let res = s3.put_file(&key, path, sha256_hex).await;
    record_upload(state, name, &res).await;
}

async fn upload_backup_bytes(
    state: &Arc<ServerState>,
    s3: &crate::s3::S3Client,
    name: &str,
    body: &[u8],
) -> Result<(), String> {
    let key = s3.config().object_key(name);
    let res = s3.put_bytes(&key, body).await;
    record_upload(state, name, &res).await;
    res
}

async fn record_upload(state: &Arc<ServerState>, name: &str, res: &Result<(), String>) {
    use std::sync::atomic::Ordering;
    state
        .metrics
        .backup_uploads_total
        .fetch_add(1, Ordering::Relaxed);
    if let Err(e) = &res {
        state
            .metrics
            .backup_upload_failures_total
            .fetch_add(1, Ordering::Relaxed);
        eprintln!("remote backup copy of {name} failed: {e}");
    }
    let entry = RemoteUpload {
        ts_ms: now_ms(),
        file: name.to_string(),
        ok: res.is_ok(),
        error: res.as_ref().err().cloned(),
    };
    state
        .backup
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .remote_last = Some(entry);
}

/// Mirror the local keep-N onto the remote prefix: full backups keep
/// `effective_keep`, incrementals keep max(keep, 4) — the same clamps as
/// the local pruner. Best effort: a failed listing or delete is logged
/// and retried on the next backup. Deletion only ever considers keys
/// matching the local naming patterns; newest-N by lexicographic order
/// (== chronological, the same invariant next_stamp maintains locally).
async fn prune_remote(s3: &crate::s3::S3Client, state: &Arc<ServerState>) {
    if let Err(e) = prune_remote_inner(s3, state).await {
        eprintln!("remote backup retention failed: {e}");
    }
}

async fn prune_remote_inner(
    s3: &crate::s3::S3Client,
    state: &Arc<ServerState>,
) -> Result<(), String> {
    let cfg = s3.config();
    let keys = owned_keys(&s3.list(&cfg.prefix).await?, &cfg.prefix);
    let keep = cfg.effective_keep(state.backup_keep);
    prune_remote_class(s3, &keys, "backup-", keep).await?;
    prune_remote_class(s3, &keys, "incr-", keep.max(4)).await?;
    Ok(())
}

async fn prune_remote_class(
    s3: &crate::s3::S3Client,
    keys: &[String],
    pattern_start: &str,
    keep: usize,
) -> Result<(), String> {
    let mut victims: Vec<&String> = keys
        .iter()
        .filter(|k| {
            key_file_name(k).is_some_and(|n| n.starts_with(pattern_start) && n.ends_with(".sql"))
        })
        .collect();
    victims.sort();
    let excess = victims.len().saturating_sub(keep);
    for key in victims.into_iter().take(excess) {
        if let Err(e) = s3.delete(key).await {
            eprintln!("remote prune delete {key} failed: {e}");
            continue;
        }
        let sidecar = format!("{key}.sha256");
        if let Err(e) = s3.delete(&sidecar).await {
            eprintln!("remote prune delete {sidecar} failed: {e}");
        }
    }
    Ok(())
}

/// File-name part of an object key (`fleet-a/backup-1.sql` →
/// `backup-1.sql`).
fn key_file_name(key: &str) -> Option<&str> {
    let n = key.rsplit('/').next()?;
    (!n.is_empty()).then_some(n)
}

/// Keys that belong to THIS node's prefix. S3 prefixes are plain string
/// prefixes, so a node with prefix `db` also sees a sibling node's
/// `db2/…` keys; treating them as own would prune the sibling's remote
/// copies and mix its incremental chain into local PITR. Own objects are
/// exactly `{prefix}/{name}` — remainder starts with `/` and carries no
/// further `/`.
fn owned_keys(keys: &[String], prefix: &str) -> Vec<String> {
    keys.iter()
        .filter(|k| {
            if prefix.is_empty() {
                // Root-level layout: own objects are bare names (no `/`).
                // The old `/`-strip filtered EVERYTHING out — restore,
                // prune and reconcile all no-oped against the remote copy.
                !k.contains('/')
            } else {
                k.strip_prefix(prefix)
                    .and_then(|rest| rest.strip_prefix('/'))
                    .is_some_and(|name| !name.is_empty() && !name.contains('/'))
            }
        })
        .cloned()
        .collect()
}

/// A file name safe to join onto the backup directory: no separators, no
/// `..`, ASCII identifier-ish bytes only (backup-*/incr-* names and their
/// .sha256 sidecars all fit). S3 listing content is environment-controlled,
/// never trusted for path building.
fn safe_backup_file_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && !name.starts_with('.')
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_'))
        && !name.contains("..")
}

/// `fs::rename` plus a parent-directory fsync: on journaling filesystems a
/// rename can be visible without being durable, and a crash landing there
/// drops the NEW name entirely (the file data itself was synced before the
/// rename). The backup/sidecar pair's "the name exists ⇒ the checksum
/// story holds" contract needs the directory entry durable too.
pub(crate) fn rename_synced(tmp: &Path, dest: &Path) -> std::io::Result<()> {
    std::fs::rename(tmp, dest)?;
    if let Some(dir) = dest.parent() {
        if let Ok(d) = std::fs::File::open(dir) {
            // Best effort: filesystems without directory fsync semantics
            // (or unsupported open-on-dir) simply keep the old behavior.
            let _ = d.sync_all();
        }
    }
    Ok(())
}

/// Re-upload local backup assets the remote copy is missing. A failed PUT
/// used to be lost for good: the next tick's cursor had already advanced
/// past the file and no code path ever revisited the name — the newest
/// full/segment stayed local-only until local keep-N pruned it too, and
/// then existed nowhere. Best effort like `prune_remote`; the next tick
/// retries. A `.sql` object's digest comes from its local sidecar, so the
/// checksum-first ordering survives reconciliation.
async fn reconcile_remote(s3: &crate::s3::S3Client, state: &Arc<ServerState>) {
    let cfg = s3.config();
    let Ok(keys) = s3.list(&cfg.prefix).await else {
        return;
    };
    // Only THIS prefix's objects count as "already remote": a sibling
    // node whose prefix is a plain string prefix of ours (db vs db2)
    // must not suppress our upload of the same FILE name.
    let remote: std::collections::HashSet<String> =
        owned_keys(&keys, &cfg.prefix).into_iter().collect();
    let Ok(entries) = std::fs::read_dir(&state.backup_dir) else {
        return;
    };
    // Checksum-FIRST, the same contract as the live backup path: a .sql
    // object landing without its sidecar is indistinguishable from a
    // pre-checksum-era legacy object on the restore side (which then
    // replays it unverified). One interleaved pass could upload the .sql
    // and die before its sidecar; two passes close that window.
    let mut sql_jobs: Vec<(String, std::path::PathBuf)> = Vec::new();
    // Sidecar names THIS pass uploaded successfully.
    let mut landed_sidecars: std::collections::HashSet<String> = std::collections::HashSet::new();
    for e in entries.flatten() {
        let Ok(name) = e.file_name().into_string() else {
            continue;
        };
        let tracked = (name.starts_with("backup-") || name.starts_with("incr-"))
            && (name.ends_with(".sha256") || name.ends_with(".sql"));
        if !tracked || remote.contains(&cfg.object_key(&name)) {
            continue;
        }
        let mut sidecar_landed = false;
        if name.ends_with(".sha256") {
            if let Ok(bytes) = std::fs::read(e.path()) {
                sidecar_landed = upload_backup_bytes(state, s3, &name, &bytes).await.is_ok();
            }
            // A failed sidecar upload must gate its .sql: remember success
            // for this name only; the second pass re-checks remotely.
            if sidecar_landed {
                landed_sidecars.insert(name);
            }
            continue;
        }
        sql_jobs.push((name, e.path()));
    }
    for (name, path) in sql_jobs {
        let sidecar_name = format!("{name}.sha256");
        // The .sql goes up only when its sidecar is CONFIRMED remote (this
        // pass uploaded it, or it was already listed): the live backup
        // path gates the same way, and a transient sidecar failure must
        // not strand a checksum-less object the restore side would replay
        // unverified.
        if !remote.contains(&cfg.object_key(&sidecar_name))
            && !landed_sidecars.contains(&sidecar_name)
        {
            continue;
        }
        let Ok(sidecar) = std::fs::read_to_string(state.backup_dir.join(&sidecar_name)) else {
            continue; // pruned pair or legacy file: nothing to reconcile
        };
        let Some(hex) = sidecar.split_whitespace().next() else {
            continue;
        };
        let _ = upload_backup_file(state, s3, &name, &path, hex).await;
    }
}

/// Write bytes with owner-only permissions, fully synced before returning.
/// The backup file must be durable (not merely visible) by the time the
/// rename makes it appear: a crash landing between the data write and the
/// sidecar rename used to leave a checksum-less truncated backup that the
/// restore path then accepted as "pre-checksum legacy" — silently
/// "restoring" nothing. The same guarantee covers the sidecar itself.
/// Like [`write_private`] over two byte slices without concatenating them
/// (the backup header + the dump body are written back to back).
fn write_private_two(path: &Path, a: &[u8], b: &[u8]) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::io::Write as _;
        use std::os::unix::fs::OpenOptionsExt as _;
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)?;
        f.write_all(a)?;
        f.write_all(b)?;
        f.sync_all()
    }
    #[cfg(not(unix))]
    {
        let mut joined = Vec::with_capacity(a.len() + b.len());
        joined.extend_from_slice(a);
        joined.extend_from_slice(b);
        std::fs::write(path, joined)
    }
}

fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::io::Write as _;
        use std::os::unix::fs::OpenOptionsExt as _;
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)?;
        f.write_all(bytes)?;
        f.sync_all()
    }
    #[cfg(not(unix))]
    {
        std::fs::write(path, bytes)
    }
}

/// Verify a backup file against its `.sha256` sidecar (sha256sum format).
/// Missing sidecar = legacy backup, allowed (the format predates
/// checksums). A present-but-mismatched sidecar is a hard refusal.
fn verify_backup_checksum(dir: &Path, name: &str) -> Result<(), String> {
    let sidecar = dir.join(format!("{name}.sha256"));
    // Missing sidecar = legacy backup from before checksums existed:
    // tolerated, but loudly — an operator must be able to tell an
    // integrity-verified restore from an unverifiable one (a missing
    // sidecar is indistinguishable from a deleted one otherwise).
    let recorded = match std::fs::read_to_string(&sidecar) {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            eprintln!(
                "docsql-backup: {name} has no .sha256 sidecar (pre-checksum legacy \
                 backup); integrity cannot be verified for this restore"
            );
            return Ok(());
        }
        Err(e) => return Err(format!("checksum read: {e}")),
    };
    let recorded = recorded.split_whitespace().next().unwrap_or("");
    if recorded.len() != 64 || !recorded.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(format!("checksum file for {name} is malformed"));
    }
    let bytes = std::fs::read(dir.join(name)).map_err(|e| format!("backup read: {e}"))?;
    let actual = docsql_core::kdf::hex(&docsql_core::kdf::sha256(&bytes));
    if !docsql_core::kdf::constant_time_eq(actual.as_bytes(), recorded.as_bytes()) {
        return Err(format!(
            "backup {name} failed checksum verification (corrupted or tampered); refusing to restore"
        ));
    }
    Ok(())
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
    // 0 disables automatic backups (documented in main.rs help and
    // docs/operations.md): clamping it to 1 turned "disabled" into a
    // full dump every second — write-path pressure, IO storm, and the
    // retention window spinning through real backups.
    if interval_secs == 0 {
        return;
    }
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(interval_secs.max(1)));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut first = true;
    loop {
        tick.tick().await;
        // Retry a remote incremental-chain invalidation that has not been
        // confirmed clean (the marker is written at adoption; see
        // invalidate_incremental_exports_remote).
        if state.backup_s3.is_some() && state.backup_dir.join(REMOTE_INVAL_MARKER).exists() {
            invalidate_incremental_exports_remote(&state).await;
        }
        // Incremental export runs on EVERY tick (cheap, journal-bounded):
        // skipping it on the fresh-base first tick would leave the PITR
        // chain behind across restarts.
        if state.sync_queue.lock().await.closed && try_begin_backup(&state) {
            // A fenced node must not vouch for ANY durable export — the
            // quorum gate below states it for full backups, and an
            // incremental segment uploaded from a minority view re-arms
            // exactly the same DR risk (a restore replays writes the
            // majority partition ruled away).
            if let Some(denial) = crate::quorum_write_denial(&state) {
                eprintln!("incremental export tick skipped: {denial}");
            } else {
                // RunningFlagGuard:export_incremental panic 时 running 标志在
                // unwind 中也能复位,定时备份不会卡死到重启(与 finish_backup
                // 同一形态)。
                let _running = RunningFlagGuard(&state);
                if let Err(e) = export_incremental(&state).await {
                    eprintln!("incremental export failed: {e}");
                }
            }
        }
        // Full backup freshness: first tick writes one when none exists or
        // the newest is older than the interval; a fresh base is kept (a
        // restart storm must not squeeze real history out of keep-N).
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
        // A fenced node must not mint new snapshots (design 004): its
        // state is a minority view, and the S3 copy would become the
        // "newest" remote backup — a DR restore from it silently rolls
        // the majority's partition-period writes back. The manual
        // trigger/export paths check the same denial.
        if let Some(denial) = crate::quorum_write_denial(&state) {
            eprintln!("backup tick skipped: {denial}");
            continue;
        }
        if try_begin_backup(&state) {
            if let Err(e) = finish_backup(&state).await {
                eprintln!("backup failed: {e}");
            }
        }
    }
}

/// Parsed base-backup header (the `-- docsql-backup v2` line).
#[derive(Debug, Clone, Copy, PartialEq)]
struct BackupHeader {
    journal_seq: u64,
    ts_ms: u64,
}

/// Parse a base backup's v2 header (absent in v1 files → journal_seq 0).
fn parse_backup_header(dir: &Path, name: &str) -> Option<BackupHeader> {
    parse_backup_header_text(&read_file_head(dir, name))
}

/// First 64 KiB of a file as UTF-8 — all either header ever needs, and the
/// callers sit on periodic timers: reading whole multi-GB backups to see one
/// line spiked memory every export tick.
fn read_file_head(dir: &Path, name: &str) -> String {
    use std::io::Read as _;
    let mut f = match std::fs::File::open(dir.join(name)) {
        Ok(f) => f,
        Err(_) => return String::new(),
    };
    let mut buf = vec![0u8; 64 * 1024];
    let mut filled = 0usize;
    loop {
        match f.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => {
                filled += n;
                if filled == buf.len() {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    buf.truncate(filled);
    String::from_utf8_lossy(&buf).into_owned()
}

fn parse_backup_header_text(text: &str) -> Option<BackupHeader> {
    for line in text.lines().take(4) {
        let Some(rest) = line.strip_prefix("-- docsql-backup v2 ") else {
            continue;
        };
        let mut journal_seq = None;
        let mut ts_ms = None;
        for part in rest.split_whitespace() {
            if let Some(v) = part.strip_prefix("journal-seq=") {
                journal_seq = v.parse().ok();
            }
            if let Some(v) = part.strip_prefix("ts=") {
                ts_ms = v.parse().ok();
            }
        }
        return Some(BackupHeader {
            journal_seq: journal_seq?,
            ts_ms: ts_ms.unwrap_or(0),
        });
    }
    None
}

/// Parsed incremental header (`-- docsql-pitr incr from=<F> to=<T>`).
#[derive(Debug, Clone, Copy, PartialEq)]
struct PitrHeader {
    from: u64,
    to: u64,
}

fn parse_pitr_header(dir: &Path, name: &str) -> Option<PitrHeader> {
    parse_pitr_header_text(&read_file_head(dir, name))
}

fn parse_pitr_header_text(text: &str) -> Option<PitrHeader> {
    let line = text.lines().next()?;
    let rest = line.strip_prefix("-- docsql-pitr incr ")?;
    let mut from = None;
    let mut to = None;
    for part in rest.split_whitespace() {
        if let Some(v) = part.strip_prefix("from=") {
            from = v.parse().ok();
        }
        if let Some(v) = part.strip_prefix("to=") {
            to = v.parse().ok();
        }
    }
    Some(PitrHeader {
        from: from?,
        to: to?,
    })
}

/// One replayable journal entry out of an incremental file body line.
fn parse_pitr_entry(line: &str) -> Option<(u64, i64, String)> {
    let v: serde_json::Value = serde_json::from_str(line).ok()?;
    let seq = v.get("seq")?.as_u64()?;
    let ts = v.get("ts")?.as_i64()?;
    let sql = v.get("sql")?.as_str()?.to_string();
    Some((seq, ts, sql))
}

/// Journal entries to replay for a restore targeting `target_ms`:
/// everything with seq > base_seq and commit-time <= target, in seq order,
/// across all incremental files. Entries with unknown ts (pre-PITR rows)
/// never match a time window — they always sit before the first post-
/// upgrade base's journal position anyway. The chain is audited for
/// continuity against the base (and for overlaps between files): a trimmed
/// journal window or a pruned incremental segment must fail the restore
/// loudly — replaying a partial chain would silently drop committed writes.
fn collect_pitr_entries(
    dir: &Path,
    base_seq: u64,
    target_ms: i64,
) -> Result<(Vec<String>, u64), String> {
    let mut entries: Vec<(u64, i64, String)> = Vec::new();
    // Highest journal seq any on-disk incremental segment claims to cover
    // beyond the base: the chain must reach it, or the tail (possibly the
    // whole chain) is missing and a restore would silently stop at the
    // base while reporting success.
    let mut chain_ceiling = base_seq;
    for name in read_incr_files(dir) {
        verify_backup_checksum(dir, &name)?;
        let text = std::fs::read_to_string(dir.join(&name))
            .map_err(|e| format!("incr read {name}: {e}"))?;
        if let Some(h) = parse_pitr_header_text(&text) {
            if h.to <= base_seq {
                continue; // entirely covered by the base dump
            }
            chain_ceiling = chain_ceiling.max(h.to);
        } else {
            // A headerless/truncated incr file that still carries entries
            // beyond the base is unverifiable as a chain link: refuse
            // rather than silently restoring a prefix.
            for line in text.lines() {
                if let Some((seq, _, _)) = parse_pitr_entry(line) {
                    if seq > base_seq {
                        return Err(format!(
                            "incremental file {name} has no readable header but carries \
                             journal seq {seq} > base {base_seq}; take a fresh full backup"
                        ));
                    }
                }
            }
            continue;
        }
        for line in text.lines() {
            if line.starts_with("--") || line.trim().is_empty() {
                continue;
            }
            if let Some((seq, ts, sql)) = parse_pitr_entry(line) {
                // Per-entry check, not just file-level: a segment exported
                // before the base dump can still straddle its journal head,
                // and entries ≤ base_seq are already inside the dump —
                // replaying them again duplicates rows (or trips PKs).
                if seq > base_seq {
                    entries.push((seq, ts, sql));
                }
            }
        }
    }
    // Files are chronological but overlapping exports can interleave;
    // order by seq, then walk the chain demanding no gap.
    entries.sort_by_key(|(seq, _, _)| *seq);
    let mut expected = base_seq + 1;
    let mut out = Vec::new();
    for (seq, ts, sql) in entries {
        if seq < expected {
            continue; // duplicate of an already-audited seq (overlapping export)
        }
        if seq > expected {
            return Err(format!(
                "incremental chain has a gap: journal seq {expected}..{seq} is missing \
                 (journal window trimmed or a segment pruned); take a fresh full backup"
            ));
        }
        // Snapshot-adoption voids occupy their seq (exported for contiguity)
        // but replay as nothing.
        if sql != JOURNAL_VOID_SENTINEL && ts <= target_ms {
            out.push(sql);
        }
        expected += 1;
    }
    if expected <= chain_ceiling {
        return Err(format!(
            "incremental chain is missing its tail: segments claim journal seq up to \
             {chain_ceiling} but the chain stops at {} (a segment was pruned or the \
             window trimmed); take a fresh full backup",
            expected - 1
        ));
    }
    // The highest seq the chain actually carried (base_seq when nothing
    // qualified): the caller audits the live journal against it — writes
    // committed after the last export tick are not in ANY segment yet, and
    // replaying the chain without them silently truncates the restore.
    let chain_last = expected - 1;
    Ok((out, chain_last))
}

/// Sentinel the snapshot-adopt path writes over voided journal text: never
/// export it (replaying it is a no-op PRAGMA, but it would waste the window).
const JOURNAL_VOID_SENTINEL: &str = "PRAGMA discarded_by_snapshot_adoption;";

/// PITR cursor: the journal seq everything on disk already covers — the max
/// of the newest incremental's `to` and the newest base backup's
/// journal-seq, else 0. The max matters when a base lands mid-window
/// (export before, full after): resuming from the older incremental `to`
/// would re-export entries the base already contains.
fn pitr_cursor(dir: &Path) -> u64 {
    let mut cursor = 0u64;
    if let Some(name) = read_incr_files(dir).last() {
        cursor = parse_pitr_header(dir, name).map(|h| h.to).unwrap_or(0);
    }
    if let Some(name) = read_backup_files(dir).last() {
        if let Some(h) = parse_backup_header(dir, name) {
            cursor = cursor.max(h.journal_seq);
        }
    }
    cursor
}

/// Export journal entries after the cursor into one incremental file.
async fn export_incremental(state: &Arc<ServerState>) -> Result<(), String> {
    let cursor = pitr_cursor(&state.backup_dir);
    let entries;
    let read_epoch;
    {
        let Some(_order) = crate::lock_engine_for_write(state).await else {
            return Err("incremental export timed out waiting for the open transaction".into());
        };
        read_epoch = state.pitr_epoch.load(std::sync::atomic::Ordering::SeqCst);
        let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
        entries = db
            .journal_entries_after(cursor, 200_000)
            .map_err(|e| format!("journal read: {e}"))?;
    }
    // Everything with a ts rides into the file — INCLUDING snapshot-adoption
    // sentinels: they occupy journal seqs, and dropping them here would punch
    // holes into the chain that the restore-side continuity audit (rightly)
    // rejects. The restore skips replaying them.
    let usable: Vec<&(u64, Option<i64>, String)> =
        entries.iter().filter(|(_, ts, _)| ts.is_some()).collect();
    if usable.is_empty() {
        return Ok(());
    }
    // The window must be contiguous from the cursor: a first entry above
    // cursor+1 means the journal window was trimmed below the cursor, and a
    // segment that starts with a hole can never restore truthfully.
    let first_seq = usable[0].0;
    if first_seq > cursor + 1 {
        return Err(format!(
            "journal window trimmed below the PITR cursor (first live seq {first_seq}, \
             cursor {cursor}); take a fresh full backup to re-anchor the chain"
        ));
    }
    std::fs::create_dir_all(&state.backup_dir).map_err(|e| format!("backup dir: {e}"))?;
    let name = format!("incr-{}.sql", next_incr_stamp(&state.backup_dir, now_ms()));
    // `from` is the segment's true first seq (not the cursor): restore-side
    // audits and operators reading the file must see what is actually inside.
    let mut body = format!(
        "-- docsql-pitr incr from={first_seq} to={}\n",
        usable.last().unwrap().0
    );
    for (seq, ts, sql) in &usable {
        body.push_str(&format!(
            "{{\"seq\":{},\"ts\":{},\"sql\":{}}}\n",
            seq,
            ts.unwrap_or(0),
            serde_json::to_string(sql).map_err(|e| format!("incr encode: {e}"))?
        ));
    }
    let bytes = body.into_bytes();
    let tmp = state.backup_dir.join(format!("{name}.tmp"));
    write_private(&tmp, &bytes).map_err(|e| format!("incr write: {e}"))?;
    // Epoch re-check right before the rename: a snapshot adoption that
    // ran while this file was being written voided exactly the entries it
    // carries — landing it would keep resurrectable journal text alive
    // past the adoption's invalidation sweep.
    if state.pitr_epoch.load(std::sync::atomic::Ordering::SeqCst) != read_epoch {
        let _ = std::fs::remove_file(&tmp);
        return Err(
            "journal was voided by a snapshot adoption during the export; \
             the incremental was discarded (PITR restarts from the next full backup)"
                .into(),
        );
    }
    rename_synced(&tmp, &state.backup_dir.join(&name)).map_err(|e| format!("incr rename: {e}"))?;
    // Second epoch check AFTER the rename: the pre-rename check above can
    // be preempted right before the rename lands, and an adoption sweeping
    // in that gap only ever saw the `.tmp` name. If the epoch moved, the
    // file (and its sidecar-to-be) must not survive — remove and fail the
    // export; PITR restarts from the next full backup.
    if state.pitr_epoch.load(std::sync::atomic::Ordering::SeqCst) != read_epoch {
        let _ = std::fs::remove_file(state.backup_dir.join(&name));
        let _ = std::fs::remove_file(state.backup_dir.join(format!("{name}.sha256.tmp")));
        let _ = std::fs::remove_file(state.backup_dir.join(format!("{name}.sha256")));
        return Err(
            "journal was voided by a snapshot adoption during the export; \
             the incremental was discarded (PITR restarts from the next full backup)"
                .into(),
        );
    }
    let digest = docsql_core::kdf::sha256(&bytes);
    let tmp = state.backup_dir.join(format!("{name}.sha256.tmp"));
    let sidecar = state.backup_dir.join(format!("{name}.sha256"));
    if let Err(e) = write_private(
        &tmp,
        format!("{}  {}\n", docsql_core::kdf::hex(&digest), name).as_bytes(),
    )
    .and_then(|()| rename_synced(&tmp, &sidecar))
    {
        // Same rule as the full-backup path: a segment body without its
        // sidecar is an unverifiable chain link restore would replay
        // unverified — remove it and let the next tick re-export.
        let _ = std::fs::remove_file(state.backup_dir.join(&name));
        let _ = std::fs::remove_file(&tmp);
        return Err(format!("incr checksum: {e} ({name})"));
    }
    prune_incr(&state.backup_dir, state.backup_keep.max(4));
    // Remote copy rides along with the local write: a disaster-recovery
    // restore to a point in time needs the base AND the whole incremental
    // chain off-volume, so every new segment goes up as it lands.
    // Checksum-first for the same reason as the full backup: an incremental
    // object without its sidecar would restore unverified.
    if let Some(s3) = &state.backup_s3 {
        let hex_digest = docsql_core::kdf::hex(&digest);
        let sidecar_landed = match std::fs::read(state.backup_dir.join(format!("{name}.sha256"))) {
            Ok(bytes) => upload_backup_bytes(state, s3, &format!("{name}.sha256"), &bytes)
                .await
                .is_ok(),
            Err(_) => false,
        };
        if sidecar_landed {
            upload_backup_file(state, s3, &name, &state.backup_dir.join(&name), &hex_digest).await;
        }
        reconcile_remote(s3, state).await;
        prune_remote(s3, state).await;
    }
    Ok(())
}

fn read_incr_files(dir: &Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut names: Vec<String> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("incr-") && n.ends_with(".sql"))
        .collect();
    names.sort();
    names
}

/// A snapshot adoption re-adjudicates the node's whole write history: the
/// journal is voided inside the adoption transaction, but incrementals
/// already exported to `incr-*.sql` still carry the discarded writes' texts
/// verbatim — restoring an older base plus those files would resurrect
/// exactly the writes the void exists to bury. Delete the exported chain
/// (sidecars included); PITR restarts from the next full backup. Best
/// effort by nature (files are not transactional), but it runs before the
/// adoption is reported Applied.
/// Remote-side half of [`invalidate_incremental_exports`]: a snapshot
/// adoption that voids the LOCAL incremental chain must also drop the S3
/// copies — a disaster-recovery pull otherwise re-fetches the voided
/// segments, and the lexicographic dedup in `collect_pitr_entries` prefers
/// the older REAL text over the post-adoption sentinel, resurrecting the
/// writes the majority discarded.
/// Marker file recording a remote incremental-chain invalidation that has
/// not been confirmed clean yet: the backup tick retries while it exists,
/// so a transient S3 outage during adoption cannot leave voided segments
/// fetchable forever (the one-shot delete had no retry path — a node that
/// never re-adopts kept them until DR pulled them back).
const REMOTE_INVAL_MARKER: &str = ".remote-incr-inval-pending";

pub(crate) async fn invalidate_incremental_exports_remote(state: &Arc<ServerState>) {
    let Some(s3) = state.backup_s3.as_ref() else {
        return;
    };
    let cfg = s3.config();
    // Record the intent FIRST: a crash or S3 outage mid-way must leave the
    // marker so the backup tick keeps retrying.
    let marker = state.backup_dir.join(REMOTE_INVAL_MARKER);
    if let Err(e) = std::fs::write(&marker, b"") {
        eprintln!("sync: cannot record remote incremental invalidation ({e}); continuing");
    }
    let remote_incr_left = |keys: &[String]| {
        owned_keys(keys, &cfg.prefix).iter().any(|k| {
            key_file_name(k).is_some_and(|n| {
                n.starts_with("incr-") && (n.ends_with(".sql") || n.ends_with(".sha256"))
            })
        })
    };
    let Ok(keys) = s3.list(&cfg.prefix).await else {
        eprintln!(
            "sync: listing remote incrementals for invalidation failed \
             (the backup tick retries while the marker remains)"
        );
        return;
    };
    for key in owned_keys(&keys, &cfg.prefix) {
        let Some(name) = key_file_name(&key) else {
            continue;
        };
        if name.starts_with("incr-") && (name.ends_with(".sql") || name.ends_with(".sha256")) {
            if let Err(e) = s3.delete(&key).await {
                eprintln!("sync: remote incremental delete {key} failed: {e}");
            }
        }
    }
    // Clear the marker only when a fresh listing confirms the prefix is
    // clean — a failed delete or a listing error keeps the retry alive.
    match s3.list(&cfg.prefix).await {
        Ok(keys) if !remote_incr_left(&keys) => {
            let _ = std::fs::remove_file(&marker);
        }
        _ => {}
    }
}

pub(crate) fn invalidate_incremental_exports(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for e in entries.flatten() {
        let name = e.file_name();
        let Some(name) = name.to_str() else { continue };
        // `.tmp` suffixes included: an export mid-write adopts only its
        // temporary names, and a sweep blind to them let a preempted export
        // rename a resurrectable file back to life after the adoption.
        if name.starts_with("incr-")
            && (name.ends_with(".sql") || name.ends_with(".sql.sha256") || name.ends_with(".tmp"))
        {
            let _ = std::fs::remove_file(e.path());
        }
    }
}

fn next_incr_stamp(dir: &Path, now: u64) -> String {
    let mut ms = now;
    for name in read_incr_files(dir) {
        let stem = name
            .strip_prefix("incr-")
            .and_then(|s| s.strip_suffix(".sql"))
            .unwrap_or("");
        if let Some(existing) = parse_stamp(stem) {
            ms = ms.max(existing + 1);
        }
    }
    utc_stamp(ms)
}

/// Prune the oldest incremental files past `keep` (sidecars go with them).
fn prune_incr(dir: &Path, keep: usize) {
    let keep = keep.max(1);
    let mut names = read_incr_files(dir);
    while names.len() > keep {
        let victim = names.remove(0);
        if let Err(e) = std::fs::remove_file(dir.join(&victim)) {
            eprintln!("incr prune failed for {victim}: {e}");
            break;
        }
        let _ = std::fs::remove_file(dir.join(format!("{victim}.sha256")));
    }
}

/// REQ_BACKUP: `{"action": "list"}` reports the backup directory and last
/// attempt; `{"action": "trigger"}` starts one backup now (rejected for
/// read-only connections like every other durable-writing operation);
/// `{"action": "restore", "file": "backup-….sql"}` replays that backup
/// through the write path (whole-cluster restore). trigger/restore answer
/// immediately — the outcome shows up in REQ_STATUS/REQ_BACKUP.
pub(crate) async fn handle_backup(
    state: &Arc<ServerState>,
    role: ConnRole,
    frame: &Frame,
    user: Option<&crate::UserAuth>,
    peer: &str,
) -> Frame {
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
            // The listing names every snapshot of the whole database (and
            // the status payload carries the backup directory): admin rule,
            // same as trigger/restore below — except read-only tokens,
            // which may list but not trigger (documented split, covered by
            // backup_trigger_over_wire).
            if user.is_some_and(|u| !u.grants.admin) {
                return Frame::new(
                    proto::RESP_ERROR,
                    crate::err_payload("backup listing requires the admin role"),
                );
            }
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
            // Quorum fence, same layer as the SQL/PUBLISH/TRIM/PROMOTE gates
            // (design 004: a fenced node must not mint new snapshots).
            if let Some(denial) = crate::quorum_write_denial(state) {
                return Frame::new(proto::RESP_ERROR, crate::err_payload(&denial));
            }
            if user.is_some_and(|u| !u.grants.admin) {
                return Frame::new(
                    proto::RESP_ERROR,
                    crate::err_payload("backup trigger requires the admin role"),
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
            // Audit: snapshots replace the whole backup set (keep-N) — the
            // trail must say who triggered it.
            querylog::record(
                state,
                peer,
                &format!("BACKUP TRIGGER{}", audit_identity(user)),
                0.0,
                &Frame::new(proto::RESP_AFFECTED, b"backup started".to_vec()),
                false,
            );
            Frame::new(proto::RESP_AFFECTED, b"backup started".to_vec())
        }
        "export" => {
            // Manual incremental export: journal entries after the cursor
            // land in one incr file (the timer does this every tick; tests
            // and operators can force it on demand). Same gates as trigger:
            // it takes the write path and writes durable files.
            if role == ConnRole::ReadOnly {
                return Frame::new(
                    proto::RESP_ERROR,
                    crate::err_payload("read-only token; writes are not permitted"),
                );
            }
            // Quorum fence: an export writes durable files from a journal a
            // fenced node must not vouch for.
            if let Some(denial) = crate::quorum_write_denial(state) {
                return Frame::new(proto::RESP_ERROR, crate::err_payload(&denial));
            }
            if user.is_some_and(|u| !u.grants.admin) {
                return Frame::new(
                    proto::RESP_ERROR,
                    crate::err_payload("backup export requires the admin role"),
                );
            }
            if !state.sync_queue.lock().await.closed {
                return Frame::new(
                    proto::RESP_ERROR,
                    crate::err_payload("export: node is still in startup sync; retry later"),
                );
            }
            if !try_begin_backup(state) {
                return Frame::new(
                    proto::RESP_ERROR,
                    crate::err_payload("backup already in progress"),
                );
            }
            // RunningFlagGuard:export 路径 panic 时 running 标志同样复位,
            // 不再手动清零(手动复位在 unwind 时被跳过,会 wedged 后续备份)。
            let _running = RunningFlagGuard(state);
            let res = export_incremental(state).await;
            // Audit: a manual export is an operator action on the backup
            // trail, same as trigger/restore.
            querylog::record(
                state,
                peer,
                &format!("BACKUP EXPORT{}", audit_identity(user)),
                0.0,
                &Frame::new(proto::RESP_AFFECTED, b"export done".to_vec()),
                false,
            );
            match res {
                Ok(()) => Frame::new(proto::RESP_AFFECTED, b"export done".to_vec()),
                Err(e) => Frame::new(proto::RESP_ERROR, crate::err_payload(&e)),
            }
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
            // Quorum fence up front: a fenced node used to pass every gate
            // here and die on the first replayed statement instead — a
            // dirty "started then failed" restore path that briefly held
            // the cluster-wide restore mutex.
            if let Some(denial) = crate::quorum_write_denial(state) {
                return Frame::new(proto::RESP_ERROR, crate::err_payload(&denial));
            }
            let file = serde_json::from_slice::<serde_json::Value>(&frame.payload)
                .ok()
                .and_then(|v| v["file"].as_str().map(String::from))
                .unwrap_or_default();
            if user.is_some_and(|u| !u.grants.admin) {
                return Frame::new(
                    proto::RESP_ERROR,
                    crate::err_payload("restore requires the admin role"),
                );
            }
            if !valid_backup_name(&file) {
                return Frame::new(
                    proto::RESP_ERROR,
                    crate::err_payload(
                        "restore: expected {\"file\": \"backup-<stamp>.sql\"} - the bare name of a file in this node's backup directory",
                    ),
                );
            }
            // Local copy may be gone (volume loss) while the remote copy
            // survives: with a remote target configured, a missing file
            // defers to the prefetch inside the restore task, which
            // fetches it — or fails with a precise error when the remote
            // copy does not have it either.
            if !state.backup_dir.join(&file).is_file() && state.backup_s3.is_none() {
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
            // Optional point-in-time target: ISO timestamp text or UTC ms.
            // Replays base + journal chain entries committed at/before it.
            // Parsed and validated BEFORE the restore flag is armed: a bad
            // request must fail cleanly, never wedge the node's backup/restore
            // state (the flag's cleanup guard only exists once run_restore
            // spawns). An unparseable value is an error, not a silent fall
            // back to full restore — the operator asked for a point in time.
            let parsed_target: Option<Result<i64, String>> =
                serde_json::from_slice::<serde_json::Value>(&frame.payload)
                    .ok()
                    // JSON null = absent: several proxies serialize an
                    // Option::None field as an explicit null rather than
                    // omitting the key, and that must mean a full restore.
                    .and_then(|v| match v.get("to") {
                        Some(serde_json::Value::Null) | None => None,
                        Some(to) => Some(to.clone()),
                    })
                    .map(|to| match to {
                        serde_json::Value::Number(n) => n.as_i64().ok_or_else(|| {
                            "restore: \"to\" number is not an integer UTC-millis value".to_string()
                        }),
                        serde_json::Value::String(s) => docsql_core::value::parse_timestamp_ms(&s)
                            .ok_or_else(|| {
                                format!("restore: \"to\" is not a parseable timestamp: \"{s}\"")
                            }),
                        _ => Err("restore: \"to\" must be a number or string".into()),
                    });
            let target_ms = match parsed_target {
                Some(Ok(t)) => Some(t),
                Some(Err(m)) => return Frame::new(proto::RESP_ERROR, crate::err_payload(&m)),
                None => None,
            };
            {
                let mut b = state.backup.lock().unwrap_or_else(|p| p.into_inner());
                let busy = b.running || b.restore.as_ref().is_some_and(|r| r.running);
                if busy {
                    return Frame::new(
                        proto::RESP_ERROR,
                        crate::err_payload("backup/restore already in progress"),
                    );
                }
                b.restore_generation += 1;
                b.restore = Some(RestoreStatus::started(&file));
            }
            use std::sync::atomic::Ordering;
            state.restore_progress.applied.store(0, Ordering::Relaxed);
            state.restore_progress.total.store(0, Ordering::Relaxed);
            let st = state.clone();
            let file_clone = file.clone();
            let generation = state
                .backup
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .restore_generation;
            tokio::spawn(async move {
                if let Err(e) = run_restore(&st, &file_clone, target_ms, generation).await {
                    eprintln!("restore of {file_clone} failed: {e}");
                }
            });
            // Audit: a restore replaces every table in the database — the
            // single most destructive operation the wire exposes; the trail
            // must say who started it and from which file.
            querylog::record(
                state,
                peer,
                &format!("RESTORE {file}{}", audit_identity(user)),
                0.0,
                &Frame::new(proto::RESP_AFFECTED, b"restore started".to_vec()),
                false,
            );
            Frame::new(proto::RESP_AFFECTED, b"restore started".to_vec())
        }
        other => Frame::new(
            proto::RESP_ERROR,
            crate::err_payload(&format!("backup: unknown action \"{other}\"")),
        ),
    }
}

/// Identity suffix for backup audit entries: token connections are already
/// identified by the trail's `peer` field; user logins by their name.
fn audit_identity(user: Option<&crate::UserAuth>) -> String {
    match user {
        Some(u) => format!(" by user={}", u.name),
        None => String::new(),
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
async fn run_restore(
    state: &Arc<ServerState>,
    file: &str,
    target_ms: Option<i64>,
    generation: u64,
) -> Result<usize, String> {
    let _running = RestoreFlagGuard(state.clone(), file.to_string(), generation);
    let mut audit_note = None;
    let res = restore_inner(state, file, target_ms, &mut audit_note).await;
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
            let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
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
    // The chain-tail audit rides along even (especially) on success.
    match (&audit_note, &mut note) {
        (Some(a), None) => note = Some(a.clone()),
        (Some(a), Some(n)) => *n = format!("{n}; {a}"),
        _ => {}
    }
    let applied = {
        let applied = match &res {
            Ok(n) => *n,
            // Keep the progress: how far the replay got matters more than a
            // zero on failure.
            Err(_) => state
                .restore_progress
                .applied
                .load(std::sync::atomic::Ordering::Relaxed),
        };
        let mut b = state.backup.lock().unwrap_or_else(|p| p.into_inner());
        let same_run = b.restore_generation == generation;
        if same_run {
            if let Some(r) = b.restore.as_mut() {
                if r.file == file {
                    r.running = false;
                    r.ok = res.is_ok();
                    r.error = res.as_ref().err().cloned();
                    r.converged = converged;
                    r.note = note.clone();
                }
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

/// Fetch backup assets from the remote copy target when they are missing
/// locally: the requested base file (always) and the incremental chain
/// (for a point-in-time target). The remote listing is authoritative —
/// a sidecar that EXISTS remotely must come back, so a failed download
/// of it fails the restore loudly instead of silently degrading to an
/// unverified replay (the legacy missing-sidecar tolerance only applies
/// to backups that predate checksums, not to network failures).
async fn ensure_local_backup_assets(
    state: &Arc<ServerState>,
    file: &str,
    target_ms: Option<i64>,
) -> Result<(), String> {
    let Some(s3) = &state.backup_s3 else {
        if !state.backup_dir.join(file).is_file() {
            return Err(format!("no such backup file {file}"));
        }
        return Ok(());
    };
    let cfg = s3.config();
    let remote: std::collections::HashSet<String> =
        owned_keys(&s3.list(&cfg.prefix).await?, &cfg.prefix)
            .into_iter()
            .collect();
    let remote_has = |name: &str| remote.contains(&cfg.object_key(name));
    if !state.backup_dir.join(file).is_file() {
        if !remote_has(file) {
            return Err(format!(
                "restore: no such backup file {file} locally or in the remote copy"
            ));
        }
        fetch_remote_object(state, s3, file).await?;
    }
    let base_sidecar = format!("{file}.sha256");
    if !state.backup_dir.join(&base_sidecar).is_file() && remote_has(&base_sidecar) {
        fetch_remote_object(state, s3, &base_sidecar).await?;
    }
    if target_ms.is_some() {
        // Point-in-time replay needs the whole incremental chain, not just
        // the segments this node still holds locally.
        for key in &remote {
            let Some(fname) = key_file_name(key) else {
                continue;
            };
            if !(fname.starts_with("incr-") && fname.ends_with(".sql")) {
                continue;
            }
            // Defense in depth on top of the owned-keys filter: listing
            // content is environment-controlled and must never drive a
            // path join outside the backup directory.
            if !safe_backup_file_name(fname) {
                eprintln!("remote backup listing: skipping non-file name {fname:?}");
                continue;
            }
            if !state.backup_dir.join(fname).is_file() {
                fetch_remote_object(state, s3, fname).await?;
            }
            let sidecar = format!("{fname}.sha256");
            if !state.backup_dir.join(&sidecar).is_file() && remote_has(&sidecar) {
                fetch_remote_object(state, s3, &sidecar).await?;
            }
        }
    }
    Ok(())
}

/// Download one object into the backup directory under its file name.
async fn fetch_remote_object(
    state: &Arc<ServerState>,
    s3: &crate::s3::S3Client,
    name: &str,
) -> Result<(), String> {
    std::fs::create_dir_all(&state.backup_dir).map_err(|e| format!("backup dir: {e}"))?;
    let key = s3.config().object_key(name);
    let dest = state.backup_dir.join(name);
    s3.get_file(&key, &dest)
        .await
        .map_err(|e| format!("remote copy fetch for {name}: {e}"))?;
    eprintln!("restored {name} from the remote backup copy");
    Ok(())
}

async fn restore_inner(
    state: &Arc<ServerState>,
    file: &str,
    target_ms: Option<i64>,
    audit_note: &mut Option<String>,
) -> Result<usize, String> {
    // Disaster recovery first: pull base (+ incremental chain, for a
    // point-in-time target) back from the remote copy when the local copy
    // is missing. All of it runs before any engine lock; the checksum
    // verification below then treats downloaded files exactly like local
    // ones.
    ensure_local_backup_assets(state, file, target_ms).await?;
    // Filesystem work before any engine lock: the script is O(data).
    // Integrity first: a sidecar mismatch refuses the whole-cluster replay
    // up front (missing sidecar = legacy backup, tolerated).
    verify_backup_checksum(&state.backup_dir, file)?;
    let script = std::fs::read_to_string(state.backup_dir.join(file))
        .map_err(|e| format!("restore read: {e}"))?;
    // Point-in-time: base dump + this node's journal chain up to the
    // target. Requires a v2 base (journal-seq anchor) — v1 files have no
    // position to anchor the replay window and fail loudly instead.
    let pitr_stmts = match target_ms {
        Some(target) => {
            let head = parse_backup_header_text(&script).ok_or_else(|| {
                "restore to timestamp requires a v2 backup (journal-seq header);                      take a new full backup first"
                    .to_string()
            })?;
            // A target EARLIER than the base snapshot itself cannot be
            // honored: the base already contains every write up to its own
            // ts, and the replay would silently resurrect the post-target
            // prefix (the mirror image of the "missing chain" refusal).
            if head.ts_ms > 0 && (target < 0 || (head.ts_ms as i64) > target) {
                return Err(format!(
                    "restore target {} precedes the base backup's snapshot time {}; \
                     take an earlier full backup",
                    target, head.ts_ms
                ));
            }
            let (stmts, chain_last_seq) =
                collect_pitr_entries(&state.backup_dir, head.journal_seq, target)?;
            // Tail audit against the LIVE journal: the incremental chain
            // ends at the last export tick — any write committed at or
            // before the target but AFTER that export is in no segment,
            // and replaying the chain without it silently truncates the
            // restore while reporting ok (up to one backup interval of
            // writes, by default a full day). Refuse loudly; the operator
            // re-runs after the next export tick or takes a fresh full
            // backup. (A DR restore on a node whose journal is empty
            // cannot see the gap — the convergence pass remains the net.)
            {
                // Tail audit against the LIVE journal: the chain ends at
                // the last export tick, and writes committed at or before
                // the target but after that export are in no segment — the
                // restore replays without them and reports ok. Surface the
                // count in the status note (a loud eprintln too): a hard
                // refusal misfires when this volume already ran a plain
                // restore — its replay re-journals the base itself, and
                // those entries are exactly the "missing" ones.
                let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
                if let Ok(pending) = db.journal_entries_after(chain_last_seq, 1000) {
                    let unexported = pending
                        .iter()
                        .filter(|(_, ts, _)| ts.is_some_and(|t| t <= target))
                        .count();
                    if unexported > 0 {
                        let msg = format!(
                            "{unexported} journal write(s) at or before the target are not \
                             in the incremental chain (they postdate the last export); \
                             the restore stops at the chain tail — verify, then re-run \
                             after the next export or a fresh full backup"
                        );
                        eprintln!("restore: {msg}");
                        *audit_note = Some(msg);
                    }
                }
            }
            stmts
        }
        None => Vec::new(),
    };
    // A backup of an empty database is an empty script: restoring it is a
    // clean no-op (post-backup tables survive), not a parse error.
    let mut stmts = if script.trim().is_empty() {
        Vec::new()
    } else {
        docsql_core::stmt::split_statements(&script).map_err(|e| format!("restore parse: {e}"))?
    };
    if !pitr_stmts.is_empty() {
        stmts.extend(pitr_stmts);
    }
    let total = stmts.len();
    use std::sync::atomic::Ordering;
    state.restore_progress.total.store(total, Ordering::Relaxed);
    state.restore_progress.applied.store(0, Ordering::Relaxed);
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
        // Restore replay never carries a deadline: a large dump must
        // finish regardless of DOCSQL_STATEMENT_TIMEOUT_MS.
        let resp =
            crate::execute_sql(state, stmt, false, false, None, true, None, None, None).await;
        if resp.frame_type == proto::RESP_ERROR {
            drop(order);
            return Err(format!(
                "statement {} of {total} failed: {}",
                i + 1,
                String::from_utf8_lossy(&resp.payload)
            ));
        }
        replayed = i + 1;
        // Progress publishes via the atomic cell: the backup mutex must not
        // nest under write_order (lock discipline — see RestoreProgress).
        state
            .restore_progress
            .applied
            .store(replayed, Ordering::Relaxed);
    }
    drop(order);
    Ok(replayed)
}

/// REQ_BACKUP (list) / `status_payload().backup` body. Reads only the
/// backup-state mutex and the filesystem — never the engine lock.
pub fn backup_payload(state: &ServerState) -> Vec<u8> {
    let b = state.backup.lock().unwrap_or_else(|p| p.into_inner());
    let files = list_backups(&state.backup_dir);
    // Config summary only — endpoint/bucket/prefix/keep/tls; the access
    // and secret keys never leave the process.
    let remote = state.backup_s3.as_ref().map(|s3| {
        let cfg = s3.config();
        serde_json::json!({
            "endpoint": cfg.endpoint,
            "bucket": cfg.bucket,
            "prefix": cfg.prefix,
            "keep": cfg.effective_keep(state.backup_keep),
            "tls": cfg.tls,
            "last": b.remote_last.as_ref().map(|r| serde_json::json!({
                "ts_ms": r.ts_ms,
                "file": r.file,
                "ok": r.ok,
                "error": r.error,
            })),
        })
    });
    let body = serde_json::json!({
        "dir": state.backup_dir.display().to_string(),
        "interval_secs": state.backup_interval_secs,
        "keep": state.backup_keep,
        "running": b.running,
        "count": files.len(),
        "files": files,
        "remote": remote,
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
            "applied": state.restore_progress.applied.load(std::sync::atomic::Ordering::Relaxed),
            "total": state.restore_progress.total.load(std::sync::atomic::Ordering::Relaxed),
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
            // Cheap presence flag, not a digest (list runs on every console
            // poll; the real verification happens at restore time).
            let checksum = dir.join(format!("{name}.sha256")).is_file();
            serde_json::json!({"name": name, "bytes": bytes, "ts_ms": ts_ms, "checksum": checksum})
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
        // The integrity sidecar goes with its file.
        let _ = std::fs::remove_file(dir.join(format!("{victim}.sha256")));
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
    // Byte-indexed slices below: a multibyte char in a 19-byte stem would
    // panic on a non-char-boundary. Backup names are ASCII by construction;
    // anything else is not a timestamp.
    if s.len() != 19 || !s.is_ascii() || !s.ends_with('Z') {
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

/// Fixed-width UTC stamp with millisecond resolution:
/// `20260910T081530123Z`. Lexicographic order is chronological.
fn utc_stamp(ms: u64) -> String {
    let secs = ms / 1000;
    let milli = ms % 1000;
    let (h, m, s) = ((secs % 86400) / 3600, (secs % 3600) / 60, secs % 60);
    let (y, mo, d) = docsql_core::engine::civil_from_days((secs / 86400) as i64);
    format!("{y:04}{mo:02}{d:02}T{h:02}{m:02}{s:02}{milli:03}Z")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalidate_incremental_exports_removes_only_the_incr_chain() {
        // A snapshot adoption voids the journal, but exported incrementals
        // carry the discarded writes verbatim — they must go (sidecars
        // included) while full backups and unrelated files stay.
        let dir = tempfile::tempdir().unwrap();
        for name in [
            "backup-1.sql",
            "backup-1.sql.sha256",
            "incr-1.sql",
            "incr-1.sql.sha256",
            "incr-2.sql",
            "incr-2.sql.sha256",
            "incr-2.sql.tmp",
            "incr-2.sql.sha256.tmp",
            "incr-notes.txt",
            "backup-2.sql.tmp",
        ] {
            std::fs::write(dir.path().join(name), b"x").unwrap();
        }
        super::invalidate_incremental_exports(dir.path());
        let mut left: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        left.sort();
        assert_eq!(
            left,
            vec![
                String::from("backup-1.sql"),
                String::from("backup-1.sql.sha256"),
                String::from("backup-2.sql.tmp"),
                String::from("incr-notes.txt"),
            ]
        );
    }

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
    fn backup_checksum_verifies_and_prunes_with_its_file() {
        let dir = tempfile::tempdir().unwrap();
        let payload = b"DROP TABLE IF EXISTS \"t\";\nCREATE TABLE t (id INT);\n";
        std::fs::write(dir.path().join("backup-x.sql"), payload).unwrap();
        let digest = docsql_core::kdf::sha256(payload);
        std::fs::write(
            dir.path().join("backup-x.sql.sha256"),
            format!("{}  backup-x.sql\n", docsql_core::kdf::hex(&digest)),
        )
        .unwrap();

        // Matching sidecar verifies; missing sidecar = legacy backup,
        // tolerated.
        assert!(verify_backup_checksum(dir.path(), "backup-x.sql").is_ok());
        std::fs::remove_file(dir.path().join("backup-x.sql.sha256")).unwrap();
        assert!(verify_backup_checksum(dir.path(), "backup-x.sql").is_ok());

        // A present-but-wrong or malformed sidecar is a hard refusal.
        std::fs::write(
            dir.path().join("backup-x.sql.sha256"),
            format!("{}  backup-x.sql\n", docsql_core::kdf::hex(&[0u8; 32])),
        )
        .unwrap();
        let e = verify_backup_checksum(dir.path(), "backup-x.sql").unwrap_err();
        assert!(e.contains("failed checksum"), "{e}");
        std::fs::write(dir.path().join("backup-x.sql.sha256"), "not-a-digest\n").unwrap();
        assert!(verify_backup_checksum(dir.path(), "backup-x.sql").is_err());

        // Retention removes the sidecar together with its file.
        std::fs::write(dir.path().join("backup-y.sql"), b"y").unwrap();
        prune_backups(dir.path(), 1);
        assert_eq!(read_backup_files(dir.path()), vec!["backup-y.sql"]);
        assert!(!dir.path().join("backup-y.sql.sha256").is_file());
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
        assert_eq!(docsql_core::engine::civil_from_days(0), (1970, 1, 1));
        assert_eq!(docsql_core::engine::civil_from_days(19_723), (2024, 1, 1)); // 2024-01-01
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

    #[test]
    fn pitr_header_and_entry_parsers() {
        // Base header (v2).
        let text =
            "-- docsql-backup v2 journal-seq=42 ts=1789000000000\nDROP TABLE IF EXISTS \"t\";\n";
        let h = parse_backup_header_text(text).expect("v2 header parses");
        assert_eq!(h.journal_seq, 42);
        assert_eq!(h.ts_ms, 1_789_000_000_000);
        // v1 (no header) → None: restore-to-timestamp refuses it loudly.
        assert_eq!(parse_backup_header_text("SELECT 1;\n"), None);
        // Incremental header.
        let ih = parse_pitr_header_text("-- docsql-pitr incr from=7 to=9\n").expect("incr header");
        assert_eq!((ih.from, ih.to), (7, 9));
        // Entry lines round-trip with embedded quotes in SQL.
        let e = parse_pitr_entry(
            "{\"seq\":8,\"ts\":1789000000123,\"sql\":\"INSERT INTO t VALUES ('it''s')\"}",
        )
        .expect("entry parses");
        assert_eq!(e.0, 8);
        assert_eq!(e.1, 1_789_000_000_123);
        assert_eq!(e.2, "INSERT INTO t VALUES ('it''s')");
        // Malformed lines never parse into partial entries.
        assert!(parse_pitr_entry("not json").is_none());
        assert!(parse_pitr_entry("{\"seq\":\"x\",\"ts\":1,\"sql\":\"\"}").is_none());
        assert!(parse_pitr_entry("-- comment").is_none());
    }

    #[test]
    fn collect_pitr_entries_filters_by_time_and_base_seq() {
        let dir = tempfile::tempdir().unwrap();
        // Two incrementals: one entirely below the base seq (skipped), one
        // straddling the base head (per-entry seq filter — entries ≤ base
        // are inside the dump and must not replay twice) with per-entry ts
        // filtering on top.
        std::fs::write(
            dir.path().join("incr-1.sql"),
            "-- docsql-pitr incr from=1 to=2\n{\"seq\":1,\"ts\":100,\"sql\":\"A1\"}\n{\"seq\":2,\"ts\":200,\"sql\":\"A2\"}\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("incr-2.sql"),
            "-- docsql-pitr incr from=3 to=5\n{\"seq\":3,\"ts\":300,\"sql\":\"B1\"}\n{\"seq\":4,\"ts\":420,\"sql\":\"B2\"}\n{\"seq\":5,\"ts\":500,\"sql\":\"B3\"}\n",
        )
        .unwrap();
        // base_seq=2: everything after the base replays, ts<=400 keeps B1.
        let (out, chain_last) = collect_pitr_entries(dir.path(), 2, 400).unwrap();
        // 链尾 = 链实际携带的最大 seq(ts 过滤只影响重放哪些,不影响
        // 链覆盖到哪里;restore 侧据此对账活期刊)。
        assert_eq!(chain_last, 5);
        assert_eq!(out, vec!["B1"], "ts 过滤");
        // base_seq=3: seq 3 is INSIDE the base dump — replaying it would
        // duplicate the write (or trip a PK); the per-entry filter drops it.
        let (out, _) = collect_pitr_entries(dir.path(), 3, i64::MAX).unwrap();
        assert_eq!(out, vec!["B2", "B3"], "seq<=base 不重放");
        // base_seq=0: the whole chain replays.
        let (out, _) = collect_pitr_entries(dir.path(), 0, i64::MAX).unwrap();
        assert_eq!(out, vec!["A1", "A2", "B1", "B2", "B3"]);
        // Checksum verification is honored for incremental files too.
        std::fs::write(
            dir.path().join("incr-2.sql.sha256"),
            b"deadbeef  incr-2.sql\n",
        )
        .unwrap();
        assert!(collect_pitr_entries(dir.path(), 0, i64::MAX).is_err());
    }

    #[test]
    fn collect_pitr_entries_rejects_gaps_and_skips_sentinels() {
        let dir = tempfile::tempdir().unwrap();
        // A trimmed/pruned window leaves a hole: seq 5 exists, 3..=4 do not.
        std::fs::write(
            dir.path().join("incr-1.sql"),
            "-- docsql-pitr incr from=3 to=5\n{\"seq\":5,\"ts\":500,\"sql\":\"C1\"}\n",
        )
        .unwrap();
        let err = collect_pitr_entries(dir.path(), 2, i64::MAX).unwrap_err();
        assert!(err.contains("gap"), "断链必须响亮报错: {err}");
        // Snapshot-adoption sentinels occupy their seq but replay as
        // nothing — the chain stays contiguous through them. The body must
        // reach the header's `to` (a truncated body is exactly the
        // missing-tail case the chain audit refuses).
        let dir2 = tempfile::tempdir().unwrap();
        std::fs::write(
            dir2.path().join("incr-1.sql"),
            concat!(
                "-- docsql-pitr incr from=3 to=5\n",
                "{\"seq\":3,\"ts\":300,\"sql\":\"PRAGMA discarded_by_snapshot_adoption;\"}\n",
                "{\"seq\":4,\"ts\":400,\"sql\":\"C2\"}\n",
                "{\"seq\":5,\"ts\":500,\"sql\":\"C3\"}\n",
            ),
        )
        .unwrap();
        let (out, _) = collect_pitr_entries(dir2.path(), 2, i64::MAX).unwrap();
        assert_eq!(out, vec!["C2", "C3"], "哨兵占位不重放");
        // A body that stops short of its header's `to` is a truncated
        // segment: the restore must refuse instead of silently stopping at
        // the last delivered seq.
        let dir2b = tempfile::tempdir().unwrap();
        std::fs::write(
            dir2b.path().join("incr-1.sql"),
            concat!(
                "-- docsql-pitr incr from=3 to=5\n",
                "{\"seq\":3,\"ts\":300,\"sql\":\"C1\"}\n",
                "{\"seq\":4,\"ts\":400,\"sql\":\"C2\"}\n",
            ),
        )
        .unwrap();
        let err = collect_pitr_entries(dir2b.path(), 2, i64::MAX).unwrap_err();
        assert!(err.contains("missing its tail"), "{err}");
        // Overlapping exports dedup by seq.
        let dir3 = tempfile::tempdir().unwrap();
        std::fs::write(
            dir3.path().join("incr-1.sql"),
            "-- docsql-pitr incr from=1 to=2\n{\"seq\":1,\"ts\":100,\"sql\":\"D1\"}\n{\"seq\":2,\"ts\":200,\"sql\":\"D2\"}\n",
        )
        .unwrap();
        std::fs::write(
            dir3.path().join("incr-2.sql"),
            "-- docsql-pitr incr from=2 to=3\n{\"seq\":2,\"ts\":200,\"sql\":\"D2\"}\n{\"seq\":3,\"ts\":300,\"sql\":\"D3\"}\n",
        )
        .unwrap();
        let (out, _) = collect_pitr_entries(dir3.path(), 0, i64::MAX).unwrap();
        assert_eq!(out, vec!["D1", "D2", "D3"], "重叠导出去重");
    }
}
