//! Paged data file + buffer pool, wired through the WAL.
//!
//! File layout: page 0 is the file header; pages 1.. hold data. Page size
//! is 4096. Writes go through in-memory transactions (`Tx`): on commit the
//! dirty page after-images are logged to the WAL and fsynced (commit record)
//! *before* the data file is updated — that ordering is what makes committed
//! transactions survive crashes.
//!
//! Checkpointing is asynchronous: a background thread fsyncs the data file
//! once the WAL crosses a soft limit, while writes keep serving; only at the
//! hard limit does the write path stall, wait for a sync that covers the
//! whole log, and then truncate it. The log is never truncated before every
//! byte in it is durable in the data file. Soft truncation additionally
//! defers to active read snapshots (MVCC stage B): their as-of page history
//! exists only in the WAL until read. At the hard limit flow control wins
//! and the truncation proceeds anyway — snapshots that lose their history
//! fail loudly ("snapshot too old") instead of stalling writes. Within an
//! epoch the history is complete: a commit that overwrites a page with no
//! frame in the current epoch first stashes the old image into every active
//! same-epoch snapshot (the image would otherwise exist nowhere — see
//! `commit_tx_inner`), so an as-of read never fails with a false "history
//! predates the retention window" while its epoch survives.
//!
//! Every mutating method takes `&self`: the pager is shared by the engine's
//! write path and (stage B) guardless snapshot readers, so the WAL, the
//! deferred-write queue, the page counter and the per-page commit LSNs all
//! sit behind interior locks. The engine's write lock still serializes all
//! writers; the locks here only separate writers from readers.

use crate::wal::{Wal, WalError, KIND_COMMIT, KIND_COMMIT_DEFERRED, KIND_FENCE, KIND_WRITE};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

pub const PAGE_SIZE: usize = 4096;
/// Buffer pool target in pages (32 MB). A working set that fits avoids
/// re-reading data-file pages entirely; the old 4 MB default thrashed on
/// any table scan larger than a few thousand rows.
const DEFAULT_POOL_PAGES: usize = 8 * 1024;
const MAGIC: &[u8; 8] = b"DOCSQLP1";
/// header: magic(8) page_size:u32(4) num_pages:u32(4) reserved
const HEADER_LEN: usize = 16;
/// Soft checkpoint threshold: WAL bytes at which a background data-file
/// fsync is requested (keeps the kernel's dirty pages drained so the
/// hard-limit stall stays small).
const SOFT_WAL_LIMIT: u64 = 8 * 1024 * 1024;
/// Hard checkpoint threshold: WAL bytes at which the write path itself
/// waits for a covering fsync and truncates the log — flow control against
/// unbounded WAL growth when writes outpace the background thread.
const HARD_WAL_LIMIT: u64 = 8 * SOFT_WAL_LIMIT;

#[derive(Debug, thiserror::Error)]
pub enum PagerError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("wal error: {0}")]
    Wal(#[from] WalError),
    #[error("page {0} out of range (file has {1} pages)")]
    OutOfRange(u32, u32),
    #[error("data file corrupt: bad header")]
    BadHeader,
    /// The snapshot's page history is gone (WAL checkpointed past it) or the
    /// snapshot was already ended. Reads fail loudly — serving a newer page
    /// version would be a silent isolation violation.
    #[error("snapshot too old: {0}")]
    SnapshotTooOld(String),
}

pub type Result<T> = std::result::Result<T, PagerError>;

/// A read snapshot (MVCC stage B): pins the WAL position `(epoch, lsn)` so
/// [`Pager::read_page_as_of`] can reconstruct page versions as of the
/// snapshot's start while writes keep committing. End it with
/// [`Pager::end_snapshot`] (the server drops its read view, which calls it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Snapshot {
    pub(crate) id: u64,
    pub(crate) epoch: u64,
    pub(crate) lsn: u64,
}

/// One registered snapshot: identity plus, after the first as-of miss, the
/// materialized page images (latest committed version ≤ lsn per page,
/// reconstructed from one WAL scan) kept so later reads are lookups.
#[derive(Default)]
struct SnapEntry {
    materialized: bool,
    cache: HashMap<u32, Vec<u8>>,
}

#[derive(Default)]
struct SnapState {
    next_id: u64,
    active: HashMap<u64, (u64, u64, SnapEntry)>, // id → (epoch, lsn, entry)
}

pub struct Pager {
    /// Raw data file, behind a mutex: the shared-reader path (`&Pager`)
    /// must seek+read it, and a shared cursor cannot be read from two
    /// threads at once.
    file: std::sync::Mutex<std::fs::File>,
    path: PathBuf,
    /// WAL appends and checkpoint truncation behind a mutex: the write path
    /// holds it across append + commit + read-surface apply, so a snapshot
    /// begin (which reads the commit head under the same lock) can never
    /// observe an LSN whose page images are not yet visible.
    wal: std::sync::Mutex<Wal>,
    num_pages: AtomicU32,
    /// Header `num_pages` known to be on disk. Page-count growth is logged
    /// via the WAL (recovery replays it), but the header must be persisted
    /// before the WAL that justifies it can be checkpointed away — else a
    /// reopen after truncation would not count pages that are on disk.
    persisted_pages: AtomicU32,
    /// Mirror of `Wal::epoch` for lock-free snapshot validation on every
    /// as-of page read. Only the truncation paths (single writer) store it.
    epoch: AtomicU64,
    /// Transaction id source: atomic so `begin_tx` is callable on `&Pager`
    /// (MVCC stage A shared readers need a read-only Tx handle).
    next_txid: AtomicU64,
    /// Buffer pool, behind a lock so that **read-only** callers (`&Pager`,
    /// MVCC stage A: concurrent SELECTs under the server's read lock) can
    /// fetch pages while the write path holds nothing but this short-lived
    /// lock. Write-path mutators share this same lock now.
    pool: std::sync::Mutex<PoolState>,
    max_pool: usize,
    /// Committed page images not yet written to the data file (deferred
    /// commits). They are WAL-logged but the data file must not see them
    /// before the WAL is fsynced, or a crash could resurrect pages of a
    /// transaction whose commit record was lost. A page stays queued until
    /// its data-file write has *completed* — a concurrent reader that misses
    /// the pool must find the image in exactly one of pending/file, never a
    /// torn in-flight file write.
    pending_writes: std::sync::Mutex<std::collections::BTreeMap<u32, Vec<u8>>>,
    /// Latest committed image LSN per page. Drives the snapshot fast path:
    /// a page whose newest version predates the snapshot is served from the
    /// current read surfaces without touching the WAL.
    page_lsn: std::sync::Mutex<HashMap<u32, u64>>,
    /// Pages freed by committed transactions, reused by the next
    /// allocation (see [`ReusablePages`]). Shared with every `Tx` so an
    /// abandoned transaction can hand back the ids it popped.
    reusable: std::sync::Arc<std::sync::Mutex<ReusablePages>>,
    /// Explicit-transaction undo journal (empty unless a SQL BEGIN is
    /// active). Holds one 4 KB pre-image per *modified* page — proportional
    /// to the transaction's writes, not to the database size.
    undo: std::sync::Mutex<UndoState>,
    /// Registered read snapshots (MVCC stage B); see [`SnapEntry`].
    snaps: std::sync::Mutex<SnapState>,
    /// Background checkpoint coordinator; see `maybe_checkpoint`.
    ckpt: std::sync::Arc<CkptShared>,
    ckpt_thread: Option<std::thread::JoinHandle<()>>,
    /// WAL bytes that trigger a background data-file fsync (soft limit).
    ckpt_soft: u64,
    /// WAL bytes at which the write path stalls and truncates the log
    /// (hard limit).
    ckpt_hard: u64,
}

/// Shared state between the pager and its background checkpoint thread.
/// The thread only fsyncs a dup'd data-file handle — it never touches the
/// WAL, the pool or `pending_writes`, so the write-ahead order and the
/// engine's lock discipline stay exactly as they were.
struct CkptShared {
    st: std::sync::Mutex<CkptState>,
    cv: std::sync::Condvar,
    /// Bumped by the writer on every WAL-mutating commit/abort. The
    /// background thread syncs only once this has been quiet for a short
    /// window: a bulk flush racing sustained commits inflates every
    /// foreground WAL fsync on fsync-bound storage.
    appends: AtomicU64,
}

#[derive(Default)]
struct CkptState {
    /// A data-file fsync was requested and not yet picked up.
    requested: bool,
    /// WAL length at the time the queued request was made.
    pending_len: u64,
    /// Monotonic count of fsyncs started (bumped before each sync_all).
    started: u64,
    /// Monotonic count of fsyncs completed.
    completed: u64,
    /// WAL length covered by the last *successful* fsync (0 = none since
    /// the last checkpoint): every WAL byte below it is durable in the
    /// data file. This holds because fsync requests are only recorded right
    /// after `flush_pending` drained the queue (both request sites — the
    /// commit path and `sync_wal` — run post-flush), and each later commit
    /// flushes its own pages before another request can be recorded. Any
    /// new `maybe_checkpoint`/`try_free_truncate` call site must preserve
    /// that post-flush ordering, or the truncation could drop WAL bytes
    /// whose page images never reached the data file.
    covered_len: u64,
    last_ok: bool,
    last_err: Option<String>,
    shutdown: bool,
}

/// How long the WAL must stay append-free before the background thread
/// starts its data-file fsync, and the poll cadence while waiting.
const CKPT_IDLE_POLL: std::time::Duration = std::time::Duration::from_millis(10);

/// Background checkpoint thread: picks up fsync requests, waits for a short
/// append-free window (see `CkptShared::appends`), syncs the data file on
/// its own (dup'd) handle and records the WAL length each sync covers. The
/// writer only ever interacts through the condvar handshake and the append
/// counter.
fn spawn_checkpoint_thread(
    shared: std::sync::Arc<CkptShared>,
    file: std::fs::File,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("docsql-ckpt".into())
        .spawn(move || loop {
            let covered;
            {
                let mut st = shared.st.lock().unwrap_or_else(|p| p.into_inner());
                while !st.requested && !st.shutdown {
                    st = shared.cv.wait(st).unwrap_or_else(|p| p.into_inner());
                }
                if st.shutdown {
                    return;
                }
                st.requested = false;
                st.started += 1;
                covered = st.pending_len;
            }
            // Idle gate: wait until no commit has landed for one poll
            // period. During a hot write loop this never fires — the sync
            // only runs when the writer pauses, or while it is already
            // stalled at the hard limit (appends frozen by construction).
            let mut last = shared.appends.load(Ordering::Relaxed);
            loop {
                std::thread::sleep(CKPT_IDLE_POLL);
                if shared.st.lock().unwrap_or_else(|p| p.into_inner()).shutdown {
                    return;
                }
                let cur = shared.appends.load(Ordering::Relaxed);
                if cur == last {
                    break;
                }
                last = cur;
            }
            let ok = file.sync_all();
            let mut st = shared.st.lock().unwrap_or_else(|p| p.into_inner());
            st.completed += 1;
            match &ok {
                Ok(()) => {
                    st.last_ok = true;
                    st.covered_len = covered;
                }
                Err(e) => {
                    st.last_ok = false;
                    st.last_err = Some(e.to_string());
                }
            }
            shared.cv.notify_all();
        })
        .expect("spawn docsql checkpoint thread")
}

struct Page {
    data: Vec<u8>,
    /// Ticket at the page's last touch. Queue entries whose ticket no
    /// longer matches the page's current one are stale re-pushes.
    ticket: u64,
}

/// Pages released by committed transactions, available for immediate reuse.
/// In-memory only: a restart starts with an empty pool and whatever was
/// freed-but-unused is simply never reused again (the file keeps its size,
/// the pages stay unreferenced) — correctness never depends on a durable
/// free list, which is why this needs no WAL or header format change. The
/// `set` dedups: a page freed twice must never be handed out twice.
#[derive(Default)]
struct ReusablePages {
    order: Vec<u32>,
    set: std::collections::HashSet<u32>,
}

impl ReusablePages {
    fn push(&mut self, id: u32) {
        if self.set.insert(id) {
            self.order.push(id);
        }
    }

    fn pop(&mut self) -> Option<u32> {
        // Entries reserved by an undo (`reserve`) stay in `order` but are no
        // longer in `set`; skip them.
        while let Some(id) = self.order.pop() {
            if self.set.remove(&id) {
                return Some(id);
            }
        }
        None
    }
}

/// One entry of the explicit-transaction undo journal (see
/// [`Pager::begin_undo`]). Replayed in reverse to restore the pre-transaction
/// page state; the catalog itself is snapshotted by the engine as Arc clones.
pub enum UndoOp {
    /// A page image to restore: the first modification in a savepoint region
    /// records the region's starting image, and every SAVEPOINT records a
    /// fresh image for each page touched so far (`freed` handling below).
    Set(u32, Vec<u8>),
    /// A page allocated by the transaction (not by freeing another page it
    /// had itself released): rollback returns it to the reusable pool.
    Alloc(u32),
    /// A page allocated *after* the transaction released it: rollback makes
    /// it live again (its content was never overwritten before the reuse, so
    /// the recorded/restored image is already correct).
    Realloc(u32),
    /// A page released by the transaction: rollback reserves it so no later
    /// allocation can hand it out while the restored catalog references it.
    Free(u32),
}

#[derive(Default)]
struct UndoState {
    active: bool,
    /// Set while a rollback replay writes pre-images back (`restore_transaction`).
    /// A replayed `Set` for a page whose first write postdates the savepoint
    /// would otherwise record the *current dirty image* as its pre-image —
    /// and a later full ROLLBACK would resurrect exactly the changes being
    /// rolled back and commit them. Replay must never append to the journal.
    replaying: bool,
    ops: Vec<UndoOp>,
    /// Pages whose current-region `Set` pre-image is already in `ops`.
    recorded: std::collections::HashSet<u32>,
    /// Pages released by this transaction so far (decides Alloc vs Realloc).
    freed: std::collections::HashSet<u32>,
}

/// Buffer pool state guarded by `Pager.pool`'s mutex.
#[derive(Default)]
struct PoolState {
    map: HashMap<u32, Page>,
    /// (page, ticket) history in arrival order: every hit re-tickets the
    /// page and appends, so the live tail of the queue is recency order —
    /// LRU eviction (FIFO let a cyclic working set slightly larger than
    /// the pool evict its own hot pages every pass). Stale entries drain
    /// lazily at eviction/compaction.
    order: std::collections::VecDeque<(u32, u64)>,
    ticket: u64,
}

impl PoolState {
    /// Promote `id` to most-recently-used and copy its image out.
    fn touch(&mut self, id: u32) -> Option<Vec<u8>> {
        let data = {
            let p = self.map.get_mut(&id)?;
            self.ticket += 1;
            p.ticket = self.ticket;
            self.order.push_back((id, self.ticket));
            p.data.clone()
        };
        self.compact_if_needed();
        Some(data)
    }

    /// Cap the history queue by dropping entries that no longer name the
    /// page's current ticket. Amortized O(1) per push.
    fn compact_if_needed(&mut self) {
        if self.order.len() < self.map.len() * 2 + 64 {
            return;
        }
        self.order
            .retain(|(id, t)| self.map.get(id).is_some_and(|p| p.ticket == *t));
    }

    /// Evict the least-recently-used live page, skipping stale queue
    /// entries; false when the queue is drained.
    fn evict_one(&mut self) -> bool {
        while let Some((id, t)) = self.order.pop_front() {
            if self.map.get(&id).is_some_and(|p| p.ticket == t) {
                self.map.remove(&id);
                return true;
            }
        }
        false
    }
}

fn lock<T>(m: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

/// Promote a pooled page to most-recently-used and copy its image out
/// (`None` when not pooled). The pool guard lives only inside this call so
/// readers never hold the pool lock across file IO.
fn pool_touch(pager: &Pager, id: u32) -> Option<Vec<u8>> {
    lock(&pager.pool).touch(id)
}

/// The 16-byte data-file header (`magic | page_size | num_pages`), shared by
/// the initial open and every later page-count persistence.
fn encode_header(num_pages: u32) -> [u8; HEADER_LEN] {
    let mut header = [0u8; HEADER_LEN];
    header[..8].copy_from_slice(MAGIC);
    header[8..12].copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
    header[12..16].copy_from_slice(&num_pages.to_le_bytes());
    header
}

impl Pager {
    /// Open (creating if needed) a database at `path` with WAL at `path.wal`,
    /// running crash recovery first.
    pub fn open(path: &Path) -> Result<Pager> {
        let wal_path = wal_path_for(path);
        let wal = Wal::open(&wal_path)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false) // never clobber an existing database
            .open(path)?;

        let num_pages = if file.metadata()?.len() == 0 {
            file.write_all_at(&encode_header(1), 0)?; // page 0 only
            file.sync_all()?;
            1
        } else {
            let mut header = [0u8; HEADER_LEN];
            file.read_exact_at(&mut header, 0)?;
            if &header[..8] != MAGIC
                || u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize != PAGE_SIZE
            {
                return Err(PagerError::BadHeader);
            }
            u32::from_le_bytes(header[12..16].try_into().unwrap())
        };
        let mut pager = Pager {
            file: std::sync::Mutex::new(file),
            path: path.to_path_buf(),
            wal: std::sync::Mutex::new(wal),
            num_pages: AtomicU32::new(num_pages),
            persisted_pages: AtomicU32::new(num_pages),
            epoch: AtomicU64::new(0),
            next_txid: AtomicU64::new(1),
            pool: std::sync::Mutex::new(PoolState::default()),
            max_pool: DEFAULT_POOL_PAGES,
            pending_writes: std::sync::Mutex::new(std::collections::BTreeMap::new()),
            page_lsn: std::sync::Mutex::new(HashMap::new()),
            reusable: std::sync::Arc::new(std::sync::Mutex::new(ReusablePages::default())),
            undo: std::sync::Mutex::new(UndoState::default()),
            snaps: std::sync::Mutex::new(SnapState::default()),
            ckpt: std::sync::Arc::new(CkptShared {
                st: std::sync::Mutex::new(CkptState::default()),
                cv: std::sync::Condvar::new(),
                appends: AtomicU64::new(0),
            }),
            ckpt_thread: None,
            ckpt_soft: SOFT_WAL_LIMIT,
            ckpt_hard: HARD_WAL_LIMIT,
        };
        pager.recover()?;
        // The checkpoint thread starts after recovery so the replay stays
        // single-threaded; it only ever fsyncs its own dup'd handle.
        let ckpt_file = lock(&pager.file).try_clone()?;
        pager.ckpt_thread = Some(spawn_checkpoint_thread(pager.ckpt.clone(), ckpt_file));
        Ok(pager)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn num_pages(&self) -> u32 {
        self.num_pages.load(Ordering::Relaxed)
    }

    /// Last WAL LSN known durable (fsynced). Cluster monitoring uses it as a
    /// per-node convergence indicator.
    pub fn durable_lsn(&self) -> u64 {
        lock(&self.wal).durable_lsn
    }

    /// Crash recovery: replay after-images of committed transactions, in LSN
    /// order, then checkpoint the WAL. Uncommitted transactions are dropped.
    ///
    /// Both passes **stream** the log (`Wal::frames`): a log can be far
    /// larger than memory (a killed multi-GB transaction appends tens of MB
    /// per attempt, and a crash loop re-appends before any truncation), and
    /// materializing it used to OOM the node before recovery could truncate
    /// — wedging the process in a restart loop. Pass 2 writes each
    /// after-image straight through in LSN order: a later frame for the same
    /// page overwrites an earlier one, so last-write-wins holds without
    /// holding any page images in memory.
    fn recover(&self) -> Result<()> {
        let wal_path = wal_path_for(&self.path);
        // Pass 1: decide which transactions are committed. A normal commit
        // counts directly; a deferred commit (a statement inside an explicit
        // SQL transaction) only counts when a later fence — the SQL COMMIT
        // boundary — is present in the valid prefix. A crash mid-transaction
        // therefore drops the whole prefix instead of replaying part of it.
        let mut committed = std::collections::HashSet::new();
        let mut deferred: Vec<(u64, u64)> = Vec::new();
        let mut last_fence_lsn = 0u64;
        for rec in Wal::frames(&wal_path)? {
            let rec = rec?;
            match rec.kind {
                KIND_COMMIT => {
                    committed.insert(rec.txid);
                }
                KIND_COMMIT_DEFERRED => deferred.push((rec.lsn, rec.txid)),
                KIND_FENCE => last_fence_lsn = last_fence_lsn.max(rec.lsn),
                _ => {}
            }
        }
        for (lsn, txid) in deferred {
            if lsn < last_fence_lsn {
                committed.insert(txid);
            }
        }
        let mut applied = false;
        for rec in Wal::frames(&wal_path)? {
            let rec = rec?;
            if rec.kind != KIND_WRITE || !committed.contains(&rec.txid) {
                continue;
            }
            let Some((page, data)) = decode_page_image(&rec.payload)? else {
                continue;
            };
            self.write_file_page(page, &data)?;
            applied = true;
        }
        if applied {
            self.persist_header()?;
            lock(&self.file).sync_all()?;
        }
        self.truncate_wal_locked()?;
        Ok(())
    }

    fn read_file_page(&self, id: u32) -> Result<Vec<u8>> {
        if id >= self.num_pages() {
            return Err(PagerError::OutOfRange(id, self.num_pages()));
        }
        let mut buf = vec![0u8; PAGE_SIZE];
        lock(&self.file).read_exact_at(&mut buf, id as u64 * PAGE_SIZE as u64)?;
        Ok(buf)
    }

    fn write_file_page(&self, id: u32, data: &[u8]) -> Result<()> {
        debug_assert_eq!(data.len(), PAGE_SIZE);
        if id >= self.num_pages() {
            self.num_pages.store(id + 1, Ordering::Relaxed);
        }
        if id >= self.persisted_pages.load(Ordering::Relaxed) {
            self.persist_header()?;
        }
        let file = lock(&self.file);
        file.write_all_at(data, id as u64 * PAGE_SIZE as u64)?;
        Ok(())
    }

    fn persist_header(&self) -> Result<()> {
        let file = lock(&self.file);
        file.write_all_at(&encode_header(self.num_pages()), 0)?;
        self.persisted_pages
            .store(self.num_pages(), Ordering::Relaxed);
        Ok(())
    }

    /// Read a page (buffered). Page 0 is the header — callers use data
    /// pages. Owned copy: the pool is behind a mutex (MVCC stage A shared
    /// readers), so borrowed access is no longer possible.
    pub fn read_page(&self, id: u32) -> Result<Vec<u8>> {
        self.read_page_shared(id)
    }

    /// Shared-reader page read: owned copy, callable from `&Pager` while
    /// several MVCC stage-A readers run concurrently. Same visibility rules
    /// (pool → pending deferred writes → data file). Returns an owned copy
    /// because the pool is behind a mutex.
    ///
    /// The file-read path is version-guarded (seqlock against `page_lsn` +
    /// WAL epoch): a commit + flush can otherwise complete between the file
    /// read and the pool insert, and inserting the pre-commit image would
    /// pin a stale page in the pool until eviction — silently rolling back
    /// a committed write for every later reader.
    pub fn read_page_shared(&self, id: u32) -> Result<Vec<u8>> {
        if id == 0 {
            return Err(PagerError::OutOfRange(0, self.num_pages()));
        }
        if let Some(data) = pool_touch(self, id) {
            return Ok(data);
        }
        if id >= self.num_pages() {
            return Err(PagerError::OutOfRange(id, self.num_pages()));
        }
        for _ in 0..64 {
            // Version before any observation: must be re-checked after the
            // file read, since a commit + flush in between is invisible in
            // the file image but reflected in the version.
            let v0 = self.page_version(id);
            // Deferred images are always the newest committed version and
            // are never torn — prefer them over reading the file at all.
            if let Some(p) = lock(&self.pending_writes).get(&id).cloned() {
                return Ok(p);
            }
            // Cold miss: read the file outside the pool lock — holding it
            // across disk IO would serialize every other reader and stall
            // the commit path (which takes this lock inside its WAL critical
            // section). Positional read (`FileExt`): no shared file cursor.
            let (file_img, file_err) = {
                let file = lock(&self.file);
                let mut buf = vec![0u8; PAGE_SIZE];
                match file.read_exact_at(&mut buf, id as u64 * PAGE_SIZE as u64) {
                    Ok(()) => (Some(buf), None),
                    // Beyond EOF is fine while a deferred image sits in
                    // `pending_writes` (not yet flushed); any real error
                    // propagates when pending cannot supply the page.
                    Err(e) => (None, Some(e)),
                }
            };
            // Insert under the pool lock together with the pending +
            // version check: commits hold pool + pending + page_lsn in one
            // critical section, so once this lock is held the verdicts
            // cannot race a commit.
            let mut st = lock(&self.pool);
            if let Some(data) = st.touch(id) {
                return Ok(data);
            }
            let data = match (
                lock(&self.pending_writes).get(&id).cloned(),
                file_img,
                file_err,
            ) {
                (Some(p), _, _) => p,
                (None, Some(f), _) if self.page_version(id) == v0 => f,
                // A commit+flush landed during the file read: the file image
                // is pre-commit. Retry (the pending map now has the newer
                // image, or the file has been rewritten).
                (None, Some(_), _) => {
                    drop(st);
                    continue;
                }
                (None, None, Some(e)) => return Err(e.into()),
                (None, None, None) => unreachable!("read_exact_at either succeeds or errors"),
            };
            while st.map.len() >= self.max_pool && st.evict_one() {}
            st.ticket += 1;
            let ticket = st.ticket;
            st.order.push_back((id, ticket));
            st.map.insert(id, Page { data, ticket });
            return Ok(st.map.get(&id).unwrap().data.clone());
        }
        Err(PagerError::Io(io::Error::other(format!(
            "page {id} kept being rewritten while reading; retry the statement"
        ))))
    }

    /// Version stamp for the seqlock in [`Pager::read_page_shared`]:
    /// `(epoch, latest commit LSN)`. Both components only change under the
    /// WAL lock (commit apply / truncation), and a truncation bumps the
    /// epoch, so `None → None` across a checkpoint is still detected.
    fn page_version(&self, id: u32) -> (u64, Option<u64>) {
        (
            self.epoch.load(Ordering::Acquire),
            lock(&self.page_lsn).get(&id).copied(),
        )
    }

    /// Allocate a zero-filled page, preferring one released by an earlier
    /// committed transaction (visible only after commit either way). Reuse
    /// keeps TRUNCATE/ROLLBACK/rewrite churn from growing the file without
    /// bound; callers always write the page in full, so the zero fill only
    /// matters for pages written partially (none today).
    pub fn allocate_page(&self, tx: &mut Tx) -> Result<u32> {
        let reused = lock(&self.reusable).pop();
        let id = match reused {
            Some(id) => {
                tx.reused.push(id);
                id
            }
            None => {
                let id = self.num_pages.fetch_add(1, Ordering::Relaxed);
                // A brand-new page id exists nowhere to return to once the
                // tx aborts (the file header already counts it) — track it
                // like a pool borrow so abort/drop recycles it instead of
                // stranding the id (and its 4 KB) until restart.
                tx.reused.push(id);
                id
            }
        };
        // Realloc: the transaction freed this page itself, so the restored
        // catalog still owns it and its content must be recorded NOW (the
        // zero-fill below would otherwise become the rollback target, and
        // the caller's write_page finds the page already staged).
        let was_freed = {
            let u = lock(&self.undo);
            u.active && !u.replaying && u.freed.contains(&id)
        };
        {
            let mut u = lock(&self.undo);
            if u.active && !u.replaying && was_freed && u.recorded.insert(id) {
                drop(u);
                let image = self.current_page_image(id)?;
                let mut u = lock(&self.undo);
                u.ops.push(UndoOp::Set(id, image));
                u.ops.push(UndoOp::Realloc(id));
            } else if u.active && !u.replaying && !was_freed {
                u.ops.push(UndoOp::Alloc(id));
            }
        }
        tx.staged.entry(id).or_insert_with(|| vec![0u8; PAGE_SIZE]);
        Ok(id)
    }

    /// Release a page. The release takes effect only when `tx` commits —
    /// until then the current catalog may still reference it, and an
    /// aborted transaction must leave it owned. A page allocated by this
    /// same transaction is simply discarded (its staged image is dropped).
    pub fn free_page(&self, tx: &mut Tx, id: u32) -> Result<()> {
        if id == 0 || id >= self.num_pages() {
            return Err(PagerError::OutOfRange(id, self.num_pages()));
        }
        {
            let mut u = lock(&self.undo);
            if u.active && !u.replaying {
                // Frees never modify content, so no image is needed: undoing
                // the Free (reserving the page) leaves the restored catalog
                // reading the bytes it always had. A later reuse's first
                // write records the page's content (still intact) as its
                // region image, which the rollback replay writes back.
                u.freed.insert(id);
                u.ops.push(UndoOp::Free(id));
            }
        }
        tx.staged.remove(&id);
        tx.to_free.push(id);
        Ok(())
    }

    /// Begin recording the undo journal for an explicit transaction.
    pub fn begin_undo(&self) {
        let mut u = lock(&self.undo);
        *u = UndoState {
            active: true,
            ..UndoState::default()
        };
    }

    /// Stop recording and drop the journal (SQL COMMIT).
    pub fn end_undo(&self) {
        let mut u = lock(&self.undo);
        *u = UndoState::default();
    }

    /// Journal length marker for SAVEPOINT; call before `undo_savepoint` so
    /// the savepoint's baseline images land inside the tail a ROLLBACK TO
    /// replays.
    pub fn undo_mark(&self) -> usize {
        lock(&self.undo).ops.len()
    }

    /// SAVEPOINT baseline: record the *current* image of every page touched
    /// in this transaction so far. Per-page-per-region accounting keeps the
    /// journal proportional to what each region wrote, while savepoints stay
    /// exact (a page rewritten after the savepoint is restored to the
    /// baseline, not to the transaction start).
    pub fn undo_savepoint(&self) -> Result<()> {
        let ids: Vec<u32> = {
            let u = lock(&self.undo);
            if !u.active {
                return Ok(());
            }
            u.recorded.iter().copied().collect()
        };
        let mut images = Vec::with_capacity(ids.len());
        for id in ids {
            images.push((id, self.current_page_image(id)?));
        }
        let mut u = lock(&self.undo);
        if u.active {
            for (id, image) in images {
                u.ops.push(UndoOp::Set(id, image));
            }
        }
        Ok(())
    }

    /// Take the journal tail from `mark` (ROLLBACK TO SAVEPOINT) and rebuild
    /// the recording sets from what remains.
    pub fn take_undo_to(&self, mark: usize) -> Vec<UndoOp> {
        let mut u = lock(&self.undo);
        let tail = u.ops.split_off(mark);
        u.recorded = u
            .ops
            .iter()
            .filter_map(|op| match op {
                UndoOp::Set(id, _) => Some(*id),
                _ => None,
            })
            .collect();
        u.freed = u
            .ops
            .iter()
            .filter_map(|op| match op {
                UndoOp::Free(id) => Some(*id),
                _ => None,
            })
            .collect();
        tail
    }

    /// Take the whole journal and stop recording (SQL ROLLBACK).
    pub fn take_undo(&self) -> Vec<UndoOp> {
        let mut u = lock(&self.undo);
        let ops = std::mem::take(&mut u.ops);
        u.active = false;
        u.recorded.clear();
        u.freed.clear();
        ops
    }

    /// Suppress undo recording while a rollback replay writes pre-images
    /// back. Replay writes must never append: a replayed `Set` whose page was
    /// first written after the savepoint would otherwise record the current
    /// dirty image as its pre-image, and a later full ROLLBACK would commit
    /// exactly the changes being rolled back. Must be paired with
    /// [`Pager::resume_undo`] on every path.
    pub fn pause_undo(&self) {
        lock(&self.undo).replaying = true;
    }

    /// Resume undo recording after [`Pager::pause_undo`].
    pub fn resume_undo(&self) {
        lock(&self.undo).replaying = false;
    }

    /// Undo of a `Free`: the page is live again in the restored catalog, so
    /// make sure no allocation can hand it out.
    pub fn reserve_page(&self, id: u32) {
        lock(&self.reusable).set.remove(&id);
    }

    /// Commit-time release of `to_free` into the reusable pool.
    fn release_pages(&self, to_free: Vec<u32>) {
        if to_free.is_empty() {
            return;
        }
        let mut r = lock(&self.reusable);
        for id in to_free {
            r.push(id);
        }
    }

    /// Return ids popped from the reusable pool by an abandoned tx.
    fn return_reused(&self, reused: Vec<u32>) {
        if reused.is_empty() {
            return;
        }
        let mut r = lock(&self.reusable);
        for id in reused {
            r.push(id);
        }
    }

    /// Begin a write transaction. Staged page writes are private until commit.
    pub fn begin_tx(&self) -> Tx {
        let txid = self.next_txid.fetch_add(1, Ordering::Relaxed) + 1;
        Tx {
            id: txid,
            staged: HashMap::new(),
            to_free: Vec::new(),
            reused: Vec::new(),
            reusable: self.reusable.clone(),
        }
    }

    /// Latest committed image of a page: pool copy first, then a deferred
    /// (not yet flushed) image, then the data file. A read failure is a real
    /// I/O error and must propagate — silently staging a zero page would
    /// commit 4 KB of zeros over live data.
    fn current_page_image(&self, id: u32) -> Result<Vec<u8>> {
        if let Some(p) = lock(&self.pool).map.get(&id) {
            return Ok(p.data.clone());
        }
        if let Some(p) = lock(&self.pending_writes).get(&id) {
            return Ok(p.clone());
        }
        self.read_file_page(id)
    }

    /// Stage a full-page write inside `tx`.
    pub fn write_page(&self, tx: &mut Tx, id: u32, offset: usize, data: &[u8]) -> Result<()> {
        if offset + data.len() > PAGE_SIZE {
            return Err(PagerError::OutOfRange(id, u32::MAX));
        }
        if id >= self.num_pages() && !tx.staged.contains_key(&id) {
            return Err(PagerError::OutOfRange(id, self.num_pages()));
        }
        if let std::collections::hash_map::Entry::Vacant(e) = tx.staged.entry(id) {
            let base = self.current_page_image(id)?;
            {
                let mut u = lock(&self.undo);
                if u.active && !u.replaying && u.recorded.insert(id) {
                    u.ops.push(UndoOp::Set(id, base.clone()));
                }
            }
            e.insert(base);
        }
        let page = tx.staged.get_mut(&id).expect("just inserted");
        page[offset..offset + data.len()].copy_from_slice(data);
        Ok(())
    }

    /// Commit: WAL-log every staged after-image, fsync WAL, then write pages
    /// to the data file. Returns the commit LSN.
    pub fn commit_tx(&self, tx: Tx) -> Result<u64> {
        self.commit_tx_inner(tx, true)
    }

    /// Commit without the WAL fsync — used inside explicit SQL
    /// transactions so the whole batch pays one flush at COMMIT
    /// (`sync_wal`) instead of one per statement. The data file is NOT
    /// touched until the WAL is durable: staged images become visible via
    /// the buffer pool and `pending_writes`.
    pub fn commit_tx_deferred(&self, tx: Tx) -> Result<u64> {
        self.commit_tx_inner(tx, false)
    }

    /// Make every deferred commit durable: append the SQL-COMMIT fence to
    /// the WAL, fsync it (so the fence is the atomic durable boundary — a
    /// crash before it drops the whole open transaction), then flush its
    /// page images to the data file.
    pub fn sync_wal(&self) -> Result<()> {
        {
            let mut wal = lock(&self.wal);
            wal.fence()?;
            wal.sync().map_err(PagerError::Wal)?;
        }
        // The fence is durable: the transaction committed no matter what
        // happens below. A failed flush/checkpoint keeps its images queued
        // (reads fall back to `pending_writes`, recovery replays the WAL)
        // and the next call retries — surfacing the error would make the
        // client re-run an already-durable COMMIT.
        if let Err(e) = self.flush_pending() {
            eprintln!("docsql-pager: post-fence flush failed (will retry): {e}");
        }
        if let Err(e) = self.maybe_checkpoint() {
            eprintln!("docsql-pager: post-fence checkpoint failed (will retry): {e}");
        }
        Ok(())
    }

    /// Write `pending_writes` through to the data file and update the pool.
    /// A page is removed from the queue only after its write succeeded (and
    /// only if it is still the same image — a newer commit may have replaced
    /// it meanwhile): on an I/O failure the failing page and the rest stay
    /// queued (their WAL records are already durable and replay on reopen);
    /// popping first would strand a page in neither queue nor pool.
    fn flush_pending(&self) -> Result<()> {
        loop {
            let front = {
                let p = lock(&self.pending_writes);
                p.iter().next().map(|(k, v)| (*k, v.clone()))
            };
            let Some((id, data)) = front else {
                break;
            };
            self.write_file_page(id, &data)?;
            {
                let mut p = lock(&self.pending_writes);
                // Only retire the image we wrote: a concurrent commit may
                // have queued a newer one for the same page while the write
                // was in flight.
                if p.get(&id).map(|v| v == &data) == Some(true) {
                    p.remove(&id);
                }
            }
            if let Some(pg) = lock(&self.pool).map.get_mut(&id) {
                pg.data = data;
            }
        }
        Ok(())
    }

    /// Free truncation: if a completed background sync already covers the
    /// whole log (it ran during an append-free window), the WAL can be
    /// dropped with no further fsync. Deferred while read snapshots are
    /// active — their as-of page history exists only in the WAL until read.
    /// Returns whether the log was truncated.
    fn free_truncate_if_covered(&self, len: u64) -> Result<bool> {
        let covered = {
            let st = lock(&self.ckpt.st);
            st.last_ok && st.covered_len >= len
        };
        if !covered {
            return Ok(false);
        }
        if !lock(&self.snaps).active.is_empty() {
            return Ok(false);
        }
        self.truncate_wal_locked()?;
        lock(&self.ckpt.st).covered_len = 0;
        Ok(true)
    }

    /// Truncate the log before a commit appends: a burst starting after an
    /// idle gap starts from a clean log when the background sync already
    /// covered everything.
    fn try_free_truncate(&self) -> Result<()> {
        // Never truncate while page images are queued for the data file:
        // their WAL frames are the only redo for pages that are not durable
        // yet (see `CkptState::covered_len`).
        if !lock(&self.pending_writes).is_empty() {
            return Ok(());
        }
        let len = lock(&self.wal).file_len()?;
        if len < self.ckpt_soft {
            return Ok(());
        }
        self.free_truncate_if_covered(len)?;
        Ok(())
    }

    /// Drop the log (data file already synced past it) and bump the epoch
    /// mirror. Callers must hold no other pager lock; the WAL mutex is taken
    /// here, the epoch mirror is refreshed and the per-page LSN table is
    /// cleared under it.
    fn truncate_wal_locked(&self) -> Result<()> {
        let mut wal = lock(&self.wal);
        wal.checkpoint()?;
        // Old-epoch page LSNs are meaningless once the checkpoint resets
        // LSNs to 1: a stale entry would compare against new-epoch snapshot
        // LSNs as "dirtied after the snapshot" and push correct fast-path
        // reads into the (now history-less) slow path. Cleared in the
        // commit path's lock order (wal → page_lsn).
        lock(&self.page_lsn).clear();
        // Publish while the WAL lock is held so snapshot validation never
        // observes a torn pre/post-checkpoint state.
        self.epoch.store(wal.epoch(), Ordering::Release);
        Ok(())
    }

    fn commit_tx_inner(&self, mut tx: Tx, fsync: bool) -> Result<u64> {
        if tx.staged.is_empty() {
            // A transaction that only released pages: nothing to log (the
            // pool is in-memory and the old catalog still owns the pages
            // until this commit), just take the release.
            self.release_pages(std::mem::take(&mut tx.to_free));
            tx.reused.clear();
            return Ok(0);
        }
        self.try_free_truncate()?;
        self.ckpt.appends.fetch_add(1, Ordering::Relaxed);
        let lsn = {
            let mut wal = lock(&self.wal);
            wal.begin(tx.id)?;
            for (id, data) in &tx.staged {
                let mut payload = Vec::with_capacity(4 + PAGE_SIZE);
                payload.extend_from_slice(&id.to_le_bytes());
                payload.extend_from_slice(data);
                wal.log_write(tx.id, &payload)?;
            }
            let lsn = if fsync {
                wal.commit(tx.id)?
            } else {
                wal.commit_deferred(tx.id)?
            };
            // Pre-epoch image preservation (MVCC stage B): a staged page
            // with no `page_lsn` entry has no frame in the current epoch,
            // so for EVERY snapshot registered in this epoch its as-of
            // image is the current read-surface image — the one this
            // commit is about to overwrite. Once overwritten, that history
            // exists nowhere in this epoch's WAL and the snapshot's scan
            // could only fail with a false "history predates the retention
            // window". Stash the old image into those snapshots' caches
            // first. Registration is atomic with the commit-head read
            // under this same WAL lock (`begin_snapshot`), so every
            // snapshot that can still need these images is in the registry
            // right now; a page that already gained a `page_lsn` entry was
            // preserved by the commit that wrote it. A capture failure must
            // NOT fail a WAL-durable commit: the page just degrades to the
            // old loud SnapshotTooOld behavior.
            {
                let mut st = lock(&self.snaps); // wal → snaps (see begin_snapshot)
                if !st.active.is_empty() {
                    let epoch_now = wal.epoch();
                    let mut targets = Vec::new();
                    for (id, (epoch, _, _)) in st.active.iter() {
                        if *epoch == epoch_now {
                            targets.push(*id);
                        }
                    }
                    if !targets.is_empty() {
                        let preserve: Vec<(u32, Vec<u8>)> = {
                            let page_lsn = lock(&self.page_lsn);
                            let pool = lock(&self.pool);
                            let mut out: Vec<(u32, Vec<u8>)> = Vec::new();
                            for id in tx.staged.keys() {
                                if page_lsn.contains_key(id) {
                                    continue;
                                }
                                // Current image = pool, else a not-yet-flushed
                                // committed image, else the data file. No
                                // commit can interleave (we hold the WAL lock),
                                // so the surfaces are stable wrt writes.
                                let image = pool
                                    .map
                                    .get(id)
                                    .map(|p| p.data.clone())
                                    .or_else(|| lock(&self.pending_writes).get(id).cloned())
                                    .or_else(|| self.read_file_page(*id).ok());
                                if let Some(image) = image {
                                    out.push((*id, image));
                                }
                            }
                            out
                        };
                        for id in targets {
                            if let Some((_, _, entry)) = st.active.get_mut(&id) {
                                for (page, image) in &preserve {
                                    entry.cache.insert(*page, image.clone());
                                }
                            }
                        }
                    }
                }
            }
            // WAL durable — now apply to the read surfaces while still
            // holding the WAL lock: a snapshot's begin reads the commit head
            // under the same lock, so it can never observe an LSN whose page
            // images are not yet visible. In the deferred path the data file
            // is deliberately left alone until `sync_wal`, so a crash can
            // never leave uncommitted page images in the data file.
            {
                let mut pool = lock(&self.pool);
                let mut pending = lock(&self.pending_writes);
                let mut page_lsn = lock(&self.page_lsn);
                for (id, data) in &tx.staged {
                    if let Some(p) = pool.map.get_mut(id) {
                        p.data = data.clone();
                    }
                    pending.insert(*id, data.clone());
                    page_lsn.insert(*id, lsn);
                }
            }
            lsn
        };
        // WAL-committed: the old catalog version can no longer be observed
        // (snapshots rebuild from the WAL), so the pages are safe to reuse.
        self.release_pages(std::mem::take(&mut tx.to_free));
        // The committed tx's allocations keep their ids: do not return them
        // to the pool via Drop.
        tx.reused.clear();
        if fsync {
            // The WAL fsync also durable-d every earlier deferred commit:
            // flush those pages too, then write this tx's pages (they are in
            // `pending_writes` like any deferred image). Past the fsync the
            // commit point is crossed — the transaction replays on recovery
            // no matter what — so a data-file flush/checkpoint failure must
            // NOT fail the statement: the client would retry an
            // already-durable write. The images stay queued and the next
            // flush/checkpoint retries them.
            if let Err(e) = self.flush_pending() {
                eprintln!("docsql-pager: post-commit flush failed (will retry): {e}");
            }
            if let Err(e) = self.maybe_checkpoint() {
                eprintln!("docsql-pager: post-commit checkpoint failed (will retry): {e}");
            }
        }
        Ok(lsn)
    }

    /// Bound WAL growth without paying the data-file fsync on the write
    /// path: past the soft limit a background thread fsyncs the data file
    /// while writes keep serving; past the hard limit the writer stalls
    /// until a sync that covers the whole log has completed and then
    /// truncates the log inline. Truncation still happens strictly after
    /// the covering data-file fsync — the write-ahead order is unchanged,
    /// only lifted off the hot path. Soft truncation defers to active read
    /// snapshots; the hard limit does not (writes are flow-controlled, the
    /// snapshots fail loudly on their next as-of read instead).
    fn maybe_checkpoint(&self) -> Result<()> {
        // Central guard for the `covered_len` invariant: a data-file fsync
        // request may only be recorded when no page image is still queued
        // for the data file. Otherwise the background thread would report
        // covering WAL bytes whose images never reached the file, and the
        // next truncation would drop their only redo.
        if !lock(&self.pending_writes).is_empty() {
            return Ok(());
        }
        let len = lock(&self.wal).file_len()?;
        if len < self.ckpt_soft {
            return Ok(());
        }
        if self.free_truncate_if_covered(len)? {
            return Ok(());
        }
        {
            let mut st = lock(&self.ckpt.st);
            if !st.requested && st.completed == st.started {
                st.requested = true;
                st.pending_len = len;
                self.ckpt.cv.notify_all();
            }
        }
        if len < self.ckpt_hard {
            return Ok(());
        }
        // Flow control. This thread is the only WAL appender, so while it
        // waits nothing new enters the log; once a completed fsync covers
        // every byte appended so far, the log can be dropped.
        let mut st = lock(&self.ckpt.st);
        loop {
            if st.shutdown {
                // Pager is dropping; the log replays on next open.
                return Ok(());
            }
            if st.requested || st.completed != st.started {
                // Let the queued/in-flight sync finish, then re-decide.
                st = self.ckpt.cv.wait(st).unwrap_or_else(|p| p.into_inner());
                continue;
            }
            if st.last_ok && st.covered_len >= len {
                break;
            }
            st.requested = true;
            st.pending_len = len;
            let baseline = st.started;
            self.ckpt.cv.notify_all();
            while st.completed <= baseline && !st.shutdown {
                st = self.ckpt.cv.wait(st).unwrap_or_else(|p| p.into_inner());
            }
            if st.shutdown {
                return Ok(());
            }
            if !st.last_ok {
                let msg = st
                    .last_err
                    .take()
                    .unwrap_or_else(|| "data file sync failed".into());
                return Err(PagerError::Io(io::Error::other(msg)));
            }
            // covered_len == pending_len == len (no append could land while
            // this thread waited) — the loop re-checks and breaks.
        }
        drop(st);
        self.truncate_wal_locked()?;
        let mut st = lock(&self.ckpt.st);
        st.covered_len = 0;
        Ok(())
    }

    /// Abort: staged writes never reach disk, nothing to undo (WAL never
    /// got a commit record), so this only bookkeeps. It still runs the
    /// WAL bound: a ruled-out transaction leaves dead frames in the log
    /// (a failed snapshot adoption appends tens of MB), and nothing else
    /// would shrink it until the next successful commit.
    pub fn abort_tx(&self, mut tx: Tx) -> Result<()> {
        if !tx.staged.is_empty() {
            self.ckpt.appends.fetch_add(1, Ordering::Relaxed);
            lock(&self.wal).abort(tx.id)?;
            self.maybe_checkpoint()?;
        }
        // Frees never took effect; reuse ids popped from the pool go back.
        self.return_reused(std::mem::take(&mut tx.reused));
        Ok(())
    }

    /// Sync the data file (e.g. before checkpointing in the future).
    pub fn sync(&self) -> Result<()> {
        lock(&self.file).sync_all().map_err(PagerError::Io)
    }

    // ---- MVCC stage B: snapshot reads ----

    /// Begin a read snapshot at the current commit head. Every page image
    /// committed at or before this head is fully applied to the read
    /// surfaces (the commit path applies under the WAL lock this also
    /// takes), so the snapshot sees a consistent state. Note the WAL lock
    /// is held across commit fsyncs, so a view created while a commit is
    /// mid-fsync waits that one fsync out; only creation pays it — the
    /// as-of reads themselves run without the WAL lock.
    ///
    /// Registration happens atomically with the head read UNDER the WAL
    /// lock (wal → snaps): the commit path's pre-epoch history stash (see
    /// `commit_tx_inner`) iterates the registry while holding the WAL lock,
    /// so every snapshot whose head predates a committing write is already
    /// registered when that write preserves the images it can still need.
    pub fn begin_snapshot(&self) -> Snapshot {
        let wal = lock(&self.wal);
        let (epoch, lsn) = (wal.epoch(), wal.last_commit_lsn());
        let mut st = lock(&self.snaps);
        st.next_id += 1;
        let id = st.next_id;
        st.active.insert(id, (epoch, lsn, SnapEntry::default()));
        drop(st);
        drop(wal);
        Snapshot { id, epoch, lsn }
    }

    /// End a snapshot: forget its registration and drop any materialized
    /// page cache it holds.
    pub fn end_snapshot(&self, snap: Snapshot) {
        lock(&self.snaps).active.remove(&snap.id);
    }

    /// Read a page as of a snapshot: the latest committed version at the
    /// snapshot's LSN, never anything committed after it.
    ///
    /// Fast path: the page's newest committed version predates the snapshot
    /// (or the page was never dirtied here) — the current read surfaces
    /// already hold exactly that version. Slow path: the page was written
    /// after the snapshot began, so its as-of image is reconstructed from
    /// the WAL. The first slow-path read materializes every page's as-of
    /// image in one WAL scan and caches it on the snapshot; later reads are
    /// lookups.
    pub fn read_page_as_of(&self, snap: &Snapshot, id: u32) -> Result<Vec<u8>> {
        if id == 0 {
            return Err(PagerError::OutOfRange(0, self.num_pages()));
        }
        if self.epoch.load(Ordering::Acquire) != snap.epoch {
            return Err(PagerError::SnapshotTooOld(
                "the WAL was checkpointed while the read was running; retry the query".into(),
            ));
        }
        let dirty_at = lock(&self.page_lsn).get(&id).copied();
        if dirty_at.is_none_or(|lsn| lsn <= snap.lsn) {
            let image = self.read_page_shared(id)?;
            // Seqlock-style double check: the commit path applies the pool
            // image and the page LSN in one WAL-locked section (the pool
            // lock is released last), so if the LSN is still within the
            // snapshot after the read, no commit could have swapped in a
            // post-snapshot version between the two observations.
            let still = lock(&self.page_lsn).get(&id).copied();
            if still.is_none_or(|lsn| lsn <= snap.lsn) {
                return Ok(image);
            }
        }
        self.materialize_snapshot(snap)?;
        let st = lock(&self.snaps);
        let Some(entry) = st.active.get(&snap.id) else {
            return Err(PagerError::SnapshotTooOld("snapshot already ended".into()));
        };
        match entry.2.cache.get(&id) {
            Some(image) => Ok(image.clone()),
            None => Err(PagerError::SnapshotTooOld(format!(
                "page {id} history predates the WAL retention window"
            ))),
        }
    }

    /// Ensure the snapshot's as-of page cache exists: one WAL scan keeping
    /// each page's latest image among transactions whose commit frame is at
    /// or before the snapshot's LSN. The scan runs holding no pager lock —
    /// it reads the whole WAL file, and holding `snaps` (or the WAL mutex)
    /// across it would block every commit and every concurrent snapshot
    /// begin/end for the IO duration.
    fn materialize_snapshot(&self, snap: &Snapshot) -> Result<()> {
        if lock(&self.snaps)
            .active
            .get(&snap.id)
            .is_some_and(|e| e.2.materialized)
        {
            return Ok(());
        }
        // Fresh read-only handle, streamed twice (`Wal::frames`) for the
        // same reason recovery streams: the log may not fit in memory.
        // Concurrent appends are fine — every frame this snapshot needs was
        // fully written before it began, and a torn tail only stops the scan
        // early.
        let wal_path = wal_path_for(&self.path);
        // Fence-aware committed set: a deferred commit only counts when a
        // fence at or before the snapshot's LSN covers it (SQL-committed);
        // an open transaction's statements must stay invisible.
        let mut committed: HashMap<u64, u64> = HashMap::new();
        let mut deferred: Vec<(u64, u64)> = Vec::new();
        let mut last_fence = 0u64;
        for rec in Wal::frames(&wal_path)? {
            let r = rec?;
            if r.lsn > snap.lsn {
                break;
            }
            match r.kind {
                KIND_COMMIT => {
                    committed.insert(r.txid, r.lsn);
                }
                KIND_COMMIT_DEFERRED => deferred.push((r.lsn, r.txid)),
                KIND_FENCE => last_fence = last_fence.max(r.lsn),
                _ => {}
            }
        }
        for (lsn, txid) in deferred {
            if lsn < last_fence {
                committed.insert(txid, lsn);
            }
        }
        let mut cache: HashMap<u32, Vec<u8>> = HashMap::new();
        for rec in Wal::frames(&wal_path)? {
            let r = rec?;
            if r.kind != KIND_WRITE {
                continue;
            }
            let Some(&commit_lsn) = committed.get(&r.txid) else {
                continue;
            };
            if commit_lsn > snap.lsn {
                continue;
            }
            if let Some((page, image)) = decode_page_image(&r.payload)? {
                cache.insert(page, image);
            }
        }
        // A concurrent hard checkpoint may have truncated the log mid-scan;
        // what was read is then the wrong epoch's history. Re-validate under
        // the WAL lock (mutual exclusion with `checkpoint`) before trusting
        // it. Lock order wal → snaps matches `begin_snapshot` and the
        // commit path's stash; the reverse nesting (snaps held while
        // waiting on wal) would deadlock against them. The install MERGES
        // into the cache instead of replacing it: pages stashed by the
        // commit path (pre-epoch images) never appear in the WAL scan, and
        // a replace would wipe exactly the entries this snapshot depends on.
        let wal = lock(&self.wal);
        if wal.epoch() != snap.epoch {
            return Err(PagerError::SnapshotTooOld(
                "the WAL was checkpointed while the read was running; retry the query".into(),
            ));
        }
        let mut st = lock(&self.snaps);
        if let Some(entry) = st.active.get_mut(&snap.id) {
            if !entry.2.materialized {
                for (page, image) in cache {
                    entry.2.cache.insert(page, image);
                }
                entry.2.materialized = true;
            }
        }
        drop(st);
        drop(wal);
        Ok(())
    }
}

/// Page-read dispatch for the SELECT execution chain: `Current` serves the
/// latest committed image (write-lock paths and MVCC stage-A readers),
/// `Snapshot` reconstructs the image as of a snapshot (guardless stage-B
/// reads — the read never sees anything committed after it began).
pub enum PageReader<'a> {
    Current(&'a Pager),
    Snapshot(&'a Pager, &'a Snapshot),
}

impl<'a> PageReader<'a> {
    pub fn current(pager: &'a Pager) -> Self {
        Self::Current(pager)
    }

    /// Latest image of `id` visible to this reader: the transaction's own
    /// staged version first (write transactions must see their own writes;
    /// snapshot readers carry staged-less txs), then the pager — current or
    /// as-of the snapshot.
    pub fn page(&self, tx: Option<&Tx>, id: u32) -> Result<Vec<u8>> {
        if let Some(tx) = tx {
            if let Some(p) = tx.staged_page(id) {
                return Ok(p.to_vec());
            }
        }
        match self {
            Self::Current(p) => p.read_page_shared(id),
            Self::Snapshot(p, snap) => p.read_page_as_of(snap, id),
        }
    }
}

/// An in-flight page transaction.
pub struct Tx {
    id: u64,
    staged: HashMap<u32, Vec<u8>>,
    /// Pages to release into the reusable pool when this tx commits.
    to_free: Vec<u32>,
    /// Ids this tx borrowed from the reusable pool, plus brand-new page ids
    /// it grew the file by (both are returned on abort/drop; a commit keeps
    /// them and clears the list).
    reused: Vec<u32>,
    /// Shared with the pager so a dropped (never committed) tx can hand the
    /// reused ids back.
    reusable: std::sync::Arc<std::sync::Mutex<ReusablePages>>,
}

impl Drop for Tx {
    fn drop(&mut self) {
        if !self.reused.is_empty() {
            let mut r = lock(&self.reusable);
            for id in self.reused.drain(..) {
                r.push(id);
            }
        }
    }
}

impl Drop for Pager {
    fn drop(&mut self) {
        // Stop the checkpoint thread before the file/WAL handles die.
        if let Some(handle) = self.ckpt_thread.take() {
            {
                let mut st = self.ckpt.st.lock().unwrap_or_else(|p| p.into_inner());
                st.shutdown = true;
            }
            self.ckpt.cv.notify_all();
            let _ = handle.join();
        }
    }
}

impl Tx {
    /// A staged (uncommitted) image of a page, if this tx wrote it.
    pub fn staged_page(&self, id: u32) -> Option<&[u8]> {
        self.staged.get(&id).map(|v| v.as_slice())
    }
}

/// WAL companion path for a database file (`<db>.wal`).
pub fn wal_path_for(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_os_string();
    s.push(".wal");
    PathBuf::from(s)
}

fn decode_page_image(payload: &[u8]) -> Result<Option<(u32, Vec<u8>)>> {
    if payload.len() != 4 + PAGE_SIZE {
        return Ok(None); // foreign record; skip
    }
    let id = u32::from_le_bytes(payload[0..4].try_into().unwrap());
    Ok(Some((id, payload[4..].to_vec())))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_db(name: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join(name);
        (dir, p)
    }

    #[test]
    fn alloc_write_read_roundtrip() {
        let (_dir, path) = tmp_db("a.db");
        let pager = Pager::open(&path).unwrap();
        let mut tx = pager.begin_tx();
        let p1 = pager.allocate_page(&mut tx).unwrap();
        let p2 = pager.allocate_page(&mut tx).unwrap();
        pager.write_page(&mut tx, p1, 0, b"hello page one").unwrap();
        pager
            .write_page(&mut tx, p2, 100, b"page two @100")
            .unwrap();
        pager.commit_tx(tx).unwrap();

        let one = pager.read_page(p1).unwrap().to_vec();
        assert_eq!(&one[..14], b"hello page one");
        let two = pager.read_page(p2).unwrap().to_vec();
        assert_eq!(&two[100..113], b"page two @100");
    }

    #[test]
    fn committed_data_survives_reopen() {
        let (_dir, path) = tmp_db("b.db");
        let magic = b"committed bytes";
        {
            let pager = Pager::open(&path).unwrap();
            let mut tx = pager.begin_tx();
            let p = pager.allocate_page(&mut tx).unwrap();
            pager.write_page(&mut tx, p, 0, magic).unwrap();
            pager.commit_tx(tx).unwrap();
        }
        let pager = Pager::open(&path).unwrap();
        let page = pager.read_page(1).unwrap().to_vec();
        assert_eq!(&page[..magic.len()], magic);
    }

    #[test]
    fn uncommitted_writes_are_lost_on_reopen() {
        let (_dir, path) = tmp_db("c.db");
        {
            let pager = Pager::open(&path).unwrap();
            let mut tx = pager.begin_tx();
            let p = pager.allocate_page(&mut tx).unwrap();
            pager.write_page(&mut tx, p, 0, b"should vanish").unwrap();
            pager.abort_tx(tx).unwrap();
            // Even without explicit abort (crash), staged pages never
            // reached commit_tx, so reopen must not see them.
        }
        let pager = Pager::open(&path).unwrap();
        assert_eq!(pager.num_pages(), 1); // header only
    }

    #[test]
    fn page_write_updates_in_place() {
        let (_dir, path) = tmp_db("d.db");
        let pager = Pager::open(&path).unwrap();
        let mut tx = pager.begin_tx();
        let p = pager.allocate_page(&mut tx).unwrap();
        pager.write_page(&mut tx, p, 0, b"version1").unwrap();
        pager.commit_tx(tx).unwrap();

        let mut tx = pager.begin_tx();
        pager.write_page(&mut tx, p, 0, b"version2").unwrap();
        pager.commit_tx(tx).unwrap();

        let page = pager.read_page(p).unwrap().to_vec();
        assert_eq!(&page[..8], b"version2");
    }

    #[test]
    fn out_of_range_reads_rejected() {
        let (_dir, path) = tmp_db("e.db");
        let pager = Pager::open(&path).unwrap();
        assert!(pager.read_page(99).is_err());
        assert!(pager.read_page(0).is_err()); // header page
        let mut tx = pager.begin_tx();
        assert!(pager.write_page(&mut tx, 42, 0, b"x").is_err());
    }

    #[test]
    fn crash_between_wal_commit_and_data_write_recovers() {
        // Commit to the WAL, but corrupt the data file as if the crash
        // happened before data pages were written. Reopen must recover the
        // page from the WAL.
        let (_dir, path) = tmp_db("f.db");
        {
            let pager = Pager::open(&path).unwrap();
            let mut tx = pager.begin_tx();
            let p = pager.allocate_page(&mut tx).unwrap();
            pager.write_page(&mut tx, p, 0, b"wal-rescued").unwrap();
            pager.commit_tx(tx).unwrap();
        }
        // Simulate lost data-file write: truncate data file back to header.
        {
            let f = std::fs::metadata(&path).unwrap().len();
            assert!(f > PAGE_SIZE as u64);
            let fh = OpenOptions::new().write(true).open(&path).unwrap();
            fh.set_len(PAGE_SIZE as u64).unwrap();
            fh.sync_all().unwrap();
        }
        let pager = Pager::open(&path).unwrap();
        let page = pager.read_page(1).unwrap().to_vec();
        assert_eq!(&page[..11], b"wal-rescued");
    }

    #[test]
    fn recover_streams_uncommitted_wal_and_truncates() {
        // 被杀死的大事务(如中途崩溃的快照采纳):帧已进 WAL、没有 COMMIT。
        // 恢复必须流式处理(不物化整个日志)、丢弃这些帧并在结束时截断 WAL。
        let (_dir, path) = tmp_db("h.db");
        let (p, wal_len) = {
            let pager = Pager::open(&path).unwrap();
            let mut tx = pager.begin_tx();
            let p = pager.allocate_page(&mut tx).unwrap();
            pager.write_page(&mut tx, p, 0, b"keeper").unwrap();
            pager.commit_tx(tx).unwrap();
            {
                let mut wal = lock(&pager.wal);
                wal.begin(999).unwrap();
                let mut payload = Vec::with_capacity(4 + PAGE_SIZE);
                for i in 0..2000u32 {
                    payload.clear();
                    payload.extend_from_slice(&(p + 1 + i).to_le_bytes());
                    payload.extend_from_slice(&[7u8; PAGE_SIZE]);
                    wal.log_write(999, &payload).unwrap();
                }
            }
            (p, std::fs::metadata(wal_path_for(&path)).unwrap().len())
        };
        assert!(wal_len > 8_000_000, "dead log is multi-MB: {wal_len}");

        let pager = Pager::open(&path).unwrap();
        assert_eq!(&pager.read_page(p).unwrap()[..6], b"keeper");
        assert!(
            pager.read_page(p + 1).is_err(),
            "abandoned transaction must not surface"
        );
        assert_eq!(
            std::fs::metadata(wal_path_for(&path)).unwrap().len(),
            8,
            "recovery truncates the dead WAL"
        );
    }

    #[test]
    fn recovery_applies_last_image_in_lsn_order() {
        // 直接落文件(不做 last-image 聚合)时,同一页的多版本必须按 LSN
        // 顺序覆盖:最后提交的版本胜出。
        let (_dir, path) = tmp_db("i.db");
        let p = {
            let pager = Pager::open(&path).unwrap();
            let mut tx = pager.begin_tx();
            let p = pager.allocate_page(&mut tx).unwrap();
            pager.write_page(&mut tx, p, 0, b"v1").unwrap();
            pager.commit_tx(tx).unwrap();
            let mut tx = pager.begin_tx();
            pager.write_page(&mut tx, p, 0, b"v2").unwrap();
            pager.commit_tx(tx).unwrap();
            p
        };
        // 模拟数据页丢失:只留头页,WAL 里的两个版本都要重放。
        {
            let fh = OpenOptions::new().write(true).open(&path).unwrap();
            fh.set_len(PAGE_SIZE as u64).unwrap();
            fh.sync_all().unwrap();
        }
        let pager = Pager::open(&path).unwrap();
        assert_eq!(&pager.read_page(p).unwrap()[..2], b"v2");
    }

    #[test]
    fn multiple_pages_and_partial_writes() {
        let (_dir, path) = tmp_db("g.db");
        let pager = Pager::open(&path).unwrap();
        let mut tx = pager.begin_tx();
        let p = pager.allocate_page(&mut tx).unwrap();
        pager.write_page(&mut tx, p, 10, b"AAA").unwrap();
        pager.write_page(&mut tx, p, 20, b"BBB").unwrap();
        pager.commit_tx(tx).unwrap();
        let page = pager.read_page(p).unwrap().to_vec();
        assert_eq!(&page[10..13], b"AAA");
        assert_eq!(&page[20..23], b"BBB");
        assert_eq!(&page[0..10], &[0u8; 10]);
    }
    // ---- 覆盖率补充:坏头 / 越界写 / 元信息 ----

    #[test]
    fn corrupt_header_rejected() {
        let (_dir, path) = tmp_db("bad.db");
        std::fs::write(&path, vec![0u8; PAGE_SIZE]).unwrap();
        assert!(matches!(Pager::open(&path), Err(PagerError::BadHeader)));
    }

    #[test]
    fn out_of_range_write_and_meta() {
        let (_dir, path) = tmp_db("meta.db");
        let pager = Pager::open(&path).unwrap();
        assert_eq!(pager.path(), path.as_path());
        // 未分配页的写入被拒
        let mut tx = pager.begin_tx();
        let e = pager.write_page(&mut tx, 999, 0, b"x").unwrap_err();
        assert!(matches!(e, PagerError::OutOfRange(..)), "{e:?}");
        // 页内越界
        let p = pager.allocate_page(&mut tx).unwrap();
        let e = pager
            .write_page(&mut tx, p, PAGE_SIZE - 1, b"toolong")
            .unwrap_err();
        assert!(matches!(e, PagerError::OutOfRange(..)));
        pager.commit_tx(tx).unwrap();
        pager.sync().unwrap();
    }
    #[test]
    fn many_pages_commit_and_reopen() {
        let (_dir, path) = tmp_db("many.db");
        let pager = Pager::open(&path).unwrap();
        for batch in 0..3 {
            let mut tx = pager.begin_tx();
            for i in 0..120u32 {
                let p = pager.allocate_page(&mut tx).unwrap();
                pager
                    .write_page(&mut tx, p, 0, format!("batch{batch}-page{i}").as_bytes())
                    .unwrap();
            }
            pager.commit_tx(tx).unwrap();
        }
        let reopened = Pager::open(&path).unwrap();
        assert!(reopened.num_pages() >= 360);
        let page = reopened.read_page(200).unwrap();
        assert!(!page.is_empty());
    }

    #[test]
    fn deferred_commit_waits_for_sync_before_touching_data_file() {
        // Write-ahead rule: a deferred commit must not put page images into
        // the data file before the WAL is fsynced, or a crash could persist
        // pages of a transaction whose commit record was lost.
        let (_dir, path) = tmp_db("h.db");
        let magic = b"deferred bytes";
        let page_id;
        {
            let pager = Pager::open(&path).unwrap();
            let mut tx = pager.begin_tx();
            let p = pager.allocate_page(&mut tx).unwrap();
            page_id = p;
            pager.write_page(&mut tx, p, 0, magic).unwrap();
            pager.commit_tx_deferred(tx).unwrap();

            // Visible through the pool (statement-to-statement reads work)...
            let page = pager.read_page(p).unwrap().to_vec();
            assert_eq!(&page[..magic.len()], magic);
            // ...but the data file must not carry the page yet (only the
            // 16-byte file header exists).
            let len = std::fs::metadata(&path).unwrap().len();
            assert_eq!(len, HEADER_LEN as u64, "no page image on disk yet");

            // A second statement sees the deferred image when re-staging
            // (read-modify-write within the transaction).
            let mut tx = pager.begin_tx();
            pager.write_page(&mut tx, p, 20, b"APPEND").unwrap();
            pager.commit_tx_deferred(tx).unwrap();

            pager.sync_wal().unwrap(); // SQL COMMIT
        }
        let pager = Pager::open(&path).unwrap();
        let page = pager.read_page(page_id).unwrap().to_vec();
        assert_eq!(&page[..magic.len()], magic);
        assert_eq!(&page[20..26], b"APPEND");
    }

    #[test]
    fn freed_pages_are_reused_and_stay_valid() {
        let (_dir, path) = tmp_db("reuse.db");
        let pager = Pager::open(&path).unwrap();
        let mut tx = pager.begin_tx();
        let a = pager.allocate_page(&mut tx).unwrap();
        let b = pager.allocate_page(&mut tx).unwrap();
        pager.write_page(&mut tx, a, 0, b"alive").unwrap();
        pager.write_page(&mut tx, b, 0, b"second").unwrap();
        pager.commit_tx(tx).unwrap();
        let pages_before = pager.num_pages();

        // Free b and commit: the next allocation must get b back, not grow.
        let mut tx = pager.begin_tx();
        pager.free_page(&mut tx, b).unwrap();
        pager.commit_tx(tx).unwrap();
        let mut tx = pager.begin_tx();
        let c = pager.allocate_page(&mut tx).unwrap();
        assert_eq!(c, b, "freed page must be reused");
        pager.write_page(&mut tx, c, 0, b"recycled").unwrap();
        pager.commit_tx(tx).unwrap();
        assert_eq!(pager.num_pages(), pages_before);
        assert_eq!(&pager.read_page(a).unwrap()[..5], b"alive");
        assert_eq!(&pager.read_page(b).unwrap()[..8], b"recycled");

        // A free rolled back by an aborted tx never takes effect.
        let mut tx = pager.begin_tx();
        pager.free_page(&mut tx, a).unwrap();
        pager.abort_tx(tx).unwrap();
        assert_eq!(&pager.read_page(a).unwrap()[..5], b"alive");
        // A page allocated in an aborted tx returns to the pool.
        let mut tx = pager.begin_tx();
        pager.free_page(&mut tx, b).unwrap();
        pager.commit_tx(tx).unwrap();
        let mut tx = pager.begin_tx();
        let d = pager.allocate_page(&mut tx).unwrap();
        assert_eq!(d, b);
        pager.abort_tx(tx).unwrap();
        let mut tx = pager.begin_tx();
        let e = pager.allocate_page(&mut tx).unwrap();
        assert_eq!(e, b, "aborted allocation must hand the id back");
        pager.abort_tx(tx).unwrap();

        // Reopen: the file is intact and the pool starts empty.
        drop(pager);
        let pager = Pager::open(&path).unwrap();
        assert_eq!(&pager.read_page(a).unwrap()[..5], b"alive");
        assert_eq!(&pager.read_page(b).unwrap()[..8], b"recycled");
    }

    #[test]
    fn deferred_commits_without_fence_are_dropped_on_reopen() {
        // An explicit SQL transaction that never reached COMMIT must not
        // resurrect after a process crash: its statements are deferred
        // commits and only a fence (the SQL COMMIT boundary) makes them
        // durable. Dropping the pager simulates kill -9 (no graceful sync).
        let (_dir, path) = tmp_db("nofence.db");
        {
            let pager = Pager::open(&path).unwrap();
            let mut tx = pager.begin_tx();
            let p = pager.allocate_page(&mut tx).unwrap();
            pager.write_page(&mut tx, p, 0, b"uncommitted").unwrap();
            pager.commit_tx_deferred(tx).unwrap();
            // no sync_wal: the transaction is still open
        }
        let pager = Pager::open(&path).unwrap();
        assert!(
            pager.read_page(1).is_err(),
            "open transaction must not survive recovery"
        );
    }

    #[test]
    fn deferred_commits_survive_reopen_after_fence() {
        let (_dir, path) = tmp_db("fence.db");
        let page_id;
        {
            let pager = Pager::open(&path).unwrap();
            let mut tx = pager.begin_tx();
            let p = pager.allocate_page(&mut tx).unwrap();
            page_id = p;
            pager.write_page(&mut tx, p, 0, b"sql commit").unwrap();
            pager.commit_tx_deferred(tx).unwrap();
            pager.sync_wal().unwrap(); // SQL COMMIT: fence + fsync
        }
        let pager = Pager::open(&path).unwrap();
        assert_eq!(&pager.read_page(page_id).unwrap()[..10], b"sql commit");
    }

    #[test]
    fn background_checkpoint_truncates_wal_off_the_write_path() {
        // Soft limit only: a background sync must eventually make the whole
        // log coverable, and the next commit truncates it for free — the
        // write path itself never pays a data-file fsync here.
        let (_dir, path) = tmp_db("bgckpt.db");
        let mut pager = Pager::open(&path).unwrap();
        pager.ckpt_soft = 64 * 1024;
        pager.ckpt_hard = u64::MAX;
        let mut page_id = None;
        for round in 0..40u8 {
            let mut tx = pager.begin_tx();
            let p = match page_id {
                Some(p) => p,
                None => {
                    let p = pager.allocate_page(&mut tx).unwrap();
                    page_id = Some(p);
                    p
                }
            };
            pager.write_page(&mut tx, p, 0, &[round; 64]).unwrap();
            pager.commit_tx_deferred(tx).unwrap();
        }
        assert!(lock(&pager.wal).file_len().unwrap() > 64 * 1024);
        pager.sync_wal().unwrap();
        let mut truncated = false;
        for _ in 0..500 {
            pager.sync_wal().unwrap();
            if lock(&pager.wal).file_len().unwrap() < 64 * 1024 {
                truncated = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(truncated, "WAL never truncated via background checkpoint");
        drop(pager);
        let pager = Pager::open(&path).unwrap();
        let page = pager.read_page(page_id.unwrap()).unwrap();
        assert_eq!(&page[..64], &[39u8; 64]);
    }

    #[test]
    fn hard_limit_checkpoint_stalls_and_truncates_inline() {
        // Crossing the hard limit must synchronously bring the WAL back
        // under it (stall → covering fsync → truncate) and keep every
        // committed page readable after reopen.
        let (_dir, path) = tmp_db("hardckpt.db");
        let mut pager = Pager::open(&path).unwrap();
        pager.ckpt_soft = 32 * 1024;
        pager.ckpt_hard = 128 * 1024;
        for round in 0..60u8 {
            let mut tx = pager.begin_tx();
            let p = if round == 0 {
                pager.allocate_page(&mut tx).unwrap()
            } else {
                1
            };
            pager.write_page(&mut tx, p, 0, &[round; 64]).unwrap();
            pager.commit_tx(tx).unwrap();
            assert!(
                lock(&pager.wal).file_len().unwrap() < 128 * 1024,
                "WAL exceeded the hard limit without inline truncation"
            );
        }
        drop(pager);
        let pager = Pager::open(&path).unwrap();
        let page = pager.read_page(1).unwrap();
        assert_eq!(&page[..64], &[59u8; 64]);
    }

    // ---- MVCC stage B: snapshot reads ----

    #[test]
    fn snapshot_reads_pre_snapshot_page_state() {
        let (_dir, path) = tmp_db("snap1.db");
        let pager = Pager::open(&path).unwrap();
        let mut tx = pager.begin_tx();
        let p = pager.allocate_page(&mut tx).unwrap();
        pager.write_page(&mut tx, p, 0, b"version1").unwrap();
        pager.commit_tx(tx).unwrap();

        let snap = pager.begin_snapshot();

        let mut tx = pager.begin_tx();
        pager.write_page(&mut tx, p, 0, b"version2").unwrap();
        pager.commit_tx(tx).unwrap();

        // As-of read stays at the snapshot; current read sees the commit.
        assert_eq!(&pager.read_page_as_of(&snap, p).unwrap()[..8], b"version1");
        assert_eq!(&pager.read_page_shared(p).unwrap()[..8], b"version2");

        pager.end_snapshot(snap);
        let e = pager.read_page_as_of(&snap, p).unwrap_err();
        assert!(matches!(e, PagerError::SnapshotTooOld(_)), "{e:?}");
    }

    #[test]
    fn snapshot_fast_path_and_slow_path_agree() {
        let (_dir, path) = tmp_db("snap2.db");
        let pager = Pager::open(&path).unwrap();
        let mut tx = pager.begin_tx();
        let p = pager.allocate_page(&mut tx).unwrap();
        pager.write_page(&mut tx, p, 0, b"version1").unwrap();
        pager.commit_tx(tx).unwrap();

        // Snapshot before any later write: fast path (page_lsn ≤ snap).
        let snap = pager.begin_snapshot();
        assert_eq!(&pager.read_page_as_of(&snap, p).unwrap()[..8], b"version1");

        // Two more commits dirty the page after the snapshot; the as-of
        // read must now take the WAL-reconstruction path and still see v1.
        for v in 2..=3u8 {
            let mut tx = pager.begin_tx();
            pager
                .write_page(&mut tx, p, 0, format!("version{v}").as_bytes())
                .unwrap();
            pager.commit_tx(tx).unwrap();
        }
        assert_eq!(&pager.read_page_as_of(&snap, p).unwrap()[..8], b"version1");
        assert_eq!(&pager.read_page_shared(p).unwrap()[..8], b"version3");
        pager.end_snapshot(snap);
    }

    #[test]
    fn snapshot_read_survives_epoch_boundary_overwrite() {
        // The MVCC stage-B gap this guards: after a WAL truncation (epoch
        // bump), a snapshot begun in the FRESH epoch used to fail with a
        // false SnapshotTooOld when a page last written before the
        // truncation got overwritten during the snapshot — that page's
        // as-of image existed only in the read surfaces, which the
        // overwrite destroyed. The commit path now stashes the pre-epoch
        // image into every active same-epoch snapshot before applying.
        let (_dir, path) = tmp_db("snap-epoch.db");
        let mut pager = Pager::open(&path).unwrap();
        pager.ckpt_soft = 32 * 1024;
        pager.ckpt_hard = u64::MAX;
        let mut page = None;
        for round in 0..40u8 {
            let mut tx = pager.begin_tx();
            let p = match page {
                Some(p) => p,
                None => {
                    let p = pager.allocate_page(&mut tx).unwrap();
                    page = Some(p);
                    p
                }
            };
            pager.write_page(&mut tx, p, 0, &[round; 64]).unwrap();
            pager.commit_tx_deferred(tx).unwrap();
        }
        // Drive the background checkpoint until the log truncates.
        let start_epoch = pager.epoch.load(std::sync::atomic::Ordering::Acquire);
        for _ in 0..500 {
            pager.sync_wal().unwrap();
            if lock(&pager.wal).file_len().unwrap() < 32 * 1024 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(
            pager.epoch.load(std::sync::atomic::Ordering::Acquire) > start_epoch,
            "test setup must cross an epoch (truncation)"
        );
        let p = page.unwrap();
        // The truncation cleared page_lsn: the page has no frame this epoch.
        assert!(lock(&pager.page_lsn).get(&p).is_none());

        let snap = pager.begin_snapshot();
        let mut tx = pager.begin_tx();
        pager.write_page(&mut tx, p, 0, b"version2").unwrap();
        pager.commit_tx(tx).unwrap();

        // As-of read must see the pre-epoch image — this used to be the
        // false "history predates the WAL retention window" error.
        assert_eq!(&pager.read_page_as_of(&snap, p).unwrap()[..8], &[39u8; 8]);
        assert_eq!(&pager.read_page_shared(p).unwrap()[..8], b"version2");
        pager.end_snapshot(snap);

        // A snapshot begun AFTER the overwrite takes the fast path and sees
        // the new image (the stash must not leak into younger snapshots).
        let snap2 = pager.begin_snapshot();
        assert_eq!(&pager.read_page_as_of(&snap2, p).unwrap()[..8], b"version2");
        pager.end_snapshot(snap2);
    }

    #[test]
    fn snapshot_misses_deferred_commit_taken_after_it() {
        let (_dir, path) = tmp_db("snap3.db");
        let pager = Pager::open(&path).unwrap();
        let mut tx = pager.begin_tx();
        let p = pager.allocate_page(&mut tx).unwrap();
        pager.write_page(&mut tx, p, 0, b"version1").unwrap();
        pager.commit_tx_deferred(tx).unwrap();
        pager.sync_wal().unwrap();

        let snap = pager.begin_snapshot();
        let mut tx = pager.begin_tx();
        pager.write_page(&mut tx, p, 0, b"version2").unwrap();
        pager.commit_tx_deferred(tx).unwrap(); // no fsync — still committed

        assert_eq!(&pager.read_page_as_of(&snap, p).unwrap()[..8], b"version1");
        // A snapshot begun after the deferred commit sees it.
        let snap2 = pager.begin_snapshot();
        assert_eq!(&pager.read_page_as_of(&snap2, p).unwrap()[..8], b"version2");
        pager.end_snapshot(snap);
        pager.end_snapshot(snap2);
    }

    #[test]
    fn soft_truncation_defers_to_active_snapshots() {
        let (_dir, path) = tmp_db("snap4.db");
        let mut pager = Pager::open(&path).unwrap();
        pager.ckpt_soft = 32 * 1024;
        pager.ckpt_hard = u64::MAX;
        let mut tx = pager.begin_tx();
        let p = pager.allocate_page(&mut tx).unwrap();
        pager.write_page(&mut tx, p, 0, &[0u8; 64]).unwrap();
        pager.commit_tx(tx).unwrap();

        let snap = pager.begin_snapshot();
        // Write well past the soft limit and let background syncs complete;
        // with the snapshot active the log must stay (no truncation).
        for round in 0..200u8 {
            let mut tx = pager.begin_tx();
            pager.write_page(&mut tx, p, 0, &[round; 512]).unwrap();
            pager.commit_tx_deferred(tx).unwrap();
            pager.sync_wal().unwrap();
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(
            lock(&pager.wal).file_len().unwrap() > 32 * 1024,
            "WAL truncated while a snapshot was active"
        );

        pager.end_snapshot(snap);
        let mut truncated = false;
        for _ in 0..500 {
            pager.sync_wal().unwrap();
            if lock(&pager.wal).file_len().unwrap() < 32 * 1024 {
                truncated = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(truncated, "WAL never truncated after the snapshot ended");
    }

    #[test]
    fn hard_limit_truncates_despite_snapshot_and_fails_it_loudly() {
        let (_dir, path) = tmp_db("snap5.db");
        let mut pager = Pager::open(&path).unwrap();
        pager.ckpt_soft = 16 * 1024;
        pager.ckpt_hard = 64 * 1024;
        let mut tx = pager.begin_tx();
        let p = pager.allocate_page(&mut tx).unwrap();
        pager.write_page(&mut tx, p, 0, &[0u8; 64]).unwrap();
        pager.commit_tx(tx).unwrap();

        let snap = pager.begin_snapshot();
        // Flow control wins over the snapshot: crossing the hard limit
        // truncates inline (writes are never starved by a read).
        for round in 0..200u8 {
            let mut tx = pager.begin_tx();
            pager.write_page(&mut tx, p, 0, &[round; 512]).unwrap();
            pager.commit_tx(tx).unwrap();
            assert!(lock(&pager.wal).file_len().unwrap() < 64 * 1024);
        }
        let e = pager.read_page_as_of(&snap, p).unwrap_err();
        assert!(matches!(e, PagerError::SnapshotTooOld(_)), "{e:?}");
        // The writer's view is unaffected.
        assert_eq!(&pager.read_page_shared(p).unwrap()[..64], &[199u8; 64]);
        pager.end_snapshot(snap);
    }

    #[test]
    fn post_checkpoint_snapshots_are_not_poisoned_by_stale_page_lsns() {
        // Truncation resets the LSN space to 1; a stale old-epoch page LSN
        // would compare against fresh snapshot heads as "dirtied after the
        // snapshot", routing fast-path-eligible reads into the history-less
        // slow path and failing them as SnapshotTooOld.
        let (_dir, path) = tmp_db("snap7.db");
        let pager = Pager::open(&path).unwrap();
        let mut tx = pager.begin_tx();
        let p = pager.allocate_page(&mut tx).unwrap();
        pager.write_page(&mut tx, p, 0, b"version1").unwrap();
        pager.commit_tx(tx).unwrap();
        for v in 2..=30u8 {
            let mut tx = pager.begin_tx();
            pager
                .write_page(&mut tx, p, 0, format!("version{v}").as_bytes())
                .unwrap();
            pager.commit_tx(tx).unwrap();
        }

        pager.truncate_wal_locked().unwrap();

        // A fresh snapshot reads the pre-checkpoint version via the fast
        // path (a truncation only happens after a covering data-file fsync,
        // so the current surfaces are exactly the as-of state).
        let snap = pager.begin_snapshot();
        assert_eq!(&pager.read_page_as_of(&snap, p).unwrap()[..9], b"version30");
        pager.end_snapshot(snap);

        // Inside the fresh epoch the slow path reconstructs from the new
        // WAL: a snapshot taken after one commit keeps that version when
        // the page is dirtied again.
        let mut tx = pager.begin_tx();
        pager.write_page(&mut tx, p, 0, b"version31").unwrap();
        pager.commit_tx(tx).unwrap();
        let snap2 = pager.begin_snapshot();
        let mut tx = pager.begin_tx();
        pager.write_page(&mut tx, p, 0, b"version32").unwrap();
        pager.commit_tx(tx).unwrap();
        assert_eq!(
            &pager.read_page_as_of(&snap2, p).unwrap()[..9],
            b"version31"
        );
        assert_eq!(&pager.read_page_shared(p).unwrap()[..9], b"version32");
        pager.end_snapshot(snap2);
    }

    #[test]
    fn concurrent_writes_do_not_disturb_an_active_snapshot() {
        let (_dir, path) = tmp_db("snap6.db");
        let pager = std::sync::Arc::new(Pager::open(&path).unwrap());
        let mut tx = pager.begin_tx();
        let p = pager.allocate_page(&mut tx).unwrap();
        pager.write_page(&mut tx, p, 0, b"version1").unwrap();
        pager.commit_tx(tx).unwrap();

        let snap = pager.begin_snapshot();
        let handle = {
            let pager = pager.clone();
            std::thread::spawn(move || {
                for round in 0..50u8 {
                    let mut tx = pager.begin_tx();
                    pager
                        .write_page(&mut tx, p, 0, format!("version{round}").as_bytes())
                        .unwrap();
                    pager.commit_tx(tx).unwrap();
                }
            })
        };
        // The snapshot keeps seeing v1 across all concurrent commits.
        for _ in 0..20 {
            assert_eq!(&pager.read_page_as_of(&snap, p).unwrap()[..8], b"version1");
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        handle.join().unwrap();
        pager.end_snapshot(snap);
    }
}
