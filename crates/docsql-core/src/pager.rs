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
//! fail loudly ("snapshot too old") instead of stalling writes.
//!
//! Every mutating method takes `&self`: the pager is shared by the engine's
//! write path and (stage B) guardless snapshot readers, so the WAL, the
//! deferred-write queue, the page counter and the per-page commit LSNs all
//! sit behind interior locks. The engine's write lock still serializes all
//! writers; the locks here only separate writers from readers.

use crate::wal::{Wal, WalError, KIND_COMMIT, KIND_WRITE};
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
        let mut committed = std::collections::HashSet::new();
        for rec in Wal::frames(&wal_path)? {
            let rec = rec?;
            if rec.kind == KIND_COMMIT {
                committed.insert(rec.txid);
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
        // Cold miss: read the file outside the pool lock — holding it across
        // disk IO would serialize every other reader and stall the commit
        // path (which takes this lock inside its WAL critical section).
        // Positional read (`FileExt`): no shared file cursor.
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
        // Insert under the pool lock together with the pending check:
        // commits hold pool + pending in one critical section, so once this
        // lock is held the pending verdict cannot race a commit, and a newer
        // committed image in pending wins over the possibly-stale file copy.
        let mut st = lock(&self.pool);
        if let Some(data) = st.touch(id) {
            return Ok(data);
        }
        let data = match (lock(&self.pending_writes).get(&id).cloned(), file_img) {
            (Some(p), _) => p,
            (None, Some(f)) => f,
            (None, None) => return Err(file_err.expect("one of the two is set").into()),
        };
        while st.map.len() >= self.max_pool && st.evict_one() {}
        st.ticket += 1;
        let ticket = st.ticket;
        st.order.push_back((id, ticket));
        st.map.insert(id, Page { data, ticket });
        Ok(st.map.get(&id).unwrap().data.clone())
    }

    /// Allocate a new page, zero-filled (visible only after commit).
    pub fn allocate_page(&self, tx: &mut Tx) -> Result<u32> {
        let id = self.num_pages.fetch_add(1, Ordering::Relaxed);
        tx.staged.insert(id, vec![0u8; PAGE_SIZE]);
        Ok(id)
    }

    /// Begin a write transaction. Staged page writes are private until commit.
    pub fn begin_tx(&self) -> Tx {
        let txid = self.next_txid.fetch_add(1, Ordering::Relaxed) + 1;
        Tx {
            id: txid,
            staged: HashMap::new(),
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

    /// Make every deferred commit durable: fsync the WAL first (write-ahead
    /// rule), then flush its page images to the data file.
    pub fn sync_wal(&self) -> Result<()> {
        lock(&self.wal).sync().map_err(PagerError::Wal)?;
        self.flush_pending()?;
        self.maybe_checkpoint()
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

    fn commit_tx_inner(&self, tx: Tx, fsync: bool) -> Result<u64> {
        if tx.staged.is_empty() {
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
        if fsync {
            // The WAL fsync also durable-d every earlier deferred commit:
            // flush those pages too, then write this tx's pages (they are in
            // `pending_writes` like any deferred image).
            self.flush_pending()?;
            self.maybe_checkpoint()?;
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
    pub fn abort_tx(&self, tx: Tx) -> Result<()> {
        if !tx.staged.is_empty() {
            self.ckpt.appends.fetch_add(1, Ordering::Relaxed);
            lock(&self.wal).abort(tx.id)?;
            self.maybe_checkpoint()?;
        }
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
    pub fn begin_snapshot(&self) -> Snapshot {
        let mut st = lock(&self.snaps);
        let (epoch, lsn) = {
            let wal = lock(&self.wal);
            (wal.epoch(), wal.last_commit_lsn())
        };
        st.next_id += 1;
        let id = st.next_id;
        st.active.insert(id, (epoch, lsn, SnapEntry::default()));
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
        let mut committed: HashMap<u64, u64> = HashMap::new();
        for rec in Wal::frames(&wal_path)? {
            let r = rec?;
            if r.kind == KIND_COMMIT && r.lsn <= snap.lsn {
                committed.insert(r.txid, r.lsn);
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
        // it. Lock order matches `begin_snapshot`: snaps → wal.
        let mut st = lock(&self.snaps);
        {
            let wal = lock(&self.wal);
            if wal.epoch() != snap.epoch {
                return Err(PagerError::SnapshotTooOld(
                    "the WAL was checkpointed while the read was running; retry the query".into(),
                ));
            }
        }
        if let Some(entry) = st.active.get_mut(&snap.id) {
            if !entry.2.materialized {
                entry.2.cache = cache;
                entry.2.materialized = true;
            }
        }
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
