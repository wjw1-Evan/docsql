//! Write-ahead log.
//!
//! Every page modification is logged before the data file is touched.
//! A transaction's frames only become durable at `commit`, which appends a
//! Commit frame and fsyncs. Statements inside an explicit SQL transaction
//! use [`Wal::commit_deferred`]: they append a *deferred* commit frame and
//! are replayed on recovery only when a later [`Wal::fence`] (the SQL
//! COMMIT boundary) is present — so a crash mid-transaction can never
//! resurrect a prefix of it. Recovery replays committed transactions in
//! LSN order and discards the rest.
//!
//! Frame layout (little-endian):
//! ```text
//! lsn:u64 | kind:u8 | txid:u64 | len:u32 | payload:len bytes | crc:u32
//! ```
//! `crc` covers kind..payload so truncated/corrupt tails are detected and
//! stop replay at the first bad frame (torn-write tolerance).

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

pub const KIND_BEGIN: u8 = 1;
pub const KIND_WRITE: u8 = 2;
pub const KIND_COMMIT: u8 = 3;
pub const KIND_ABORT: u8 = 4;

/// Recovery-side bookkeeping for deferred commits shared by both scanners
/// ([`Wal::open`] and `Pager::recover` — they MUST agree). Each entry walks
/// pending → durable (a later FENCE covers it) or → voided (a later ABORT
/// of the same txid: the ROLLBACK path appends one ABORT per outstanding
/// deferred statement). A voided entry stays voided even if a subsequent
/// fence covers its LSN — a rolled-back transaction must not resurrect.
#[derive(Default)]
pub(crate) struct DeferredSet {
    /// (lsn, txid, status) with 0 = pending, 1 = durable, 2 = voided.
    entries: Vec<(u64, u64, u8)>,
}

impl DeferredSet {
    pub(crate) fn push(&mut self, lsn: u64, txid: u64) {
        self.entries.push((lsn, txid, 0));
    }
    pub(crate) fn fence(&mut self, fence_lsn: u64) {
        for e in self.entries.iter_mut() {
            if e.0 < fence_lsn && e.2 == 0 {
                e.2 = 1;
            }
        }
    }
    pub(crate) fn abort(&mut self, txid: u64, abort_lsn: u64) {
        for e in self.entries.iter_mut() {
            if e.1 == txid && e.0 < abort_lsn {
                e.2 = 2;
            }
        }
    }
    fn durable_lsns(&self) -> impl Iterator<Item = u64> + '_ {
        self.entries.iter().filter(|e| e.2 == 1).map(|e| e.0)
    }
    pub(crate) fn durable_txids(&self) -> impl Iterator<Item = u64> + '_ {
        self.entries.iter().filter(|e| e.2 == 1).map(|e| e.1)
    }
}
/// Commit of a single statement inside an explicit SQL transaction: the
/// transaction is still open, so recovery must not replay it unless a
/// later [`KIND_FENCE`] (the SQL COMMIT's durable boundary) covers it.
pub const KIND_COMMIT_DEFERRED: u8 = 5;
/// Durable-boundary marker appended by the pager's `sync_wal` before its
/// fsync: every deferred commit before it belongs to a committed SQL
/// transaction; deferred commits after it (open transaction) must be
/// dropped by recovery.
pub const KIND_FENCE: u8 = 6;

const HEADER: &[u8; 8] = b"DOCSWAL1";

/// Largest payload a valid frame may carry. The pager logs one page image
/// per write frame (4 + 4096 bytes); anything beyond this is a corrupt
/// length field, treated as a torn tail instead of an allocation request.
const MAX_FRAME_PAYLOAD: usize = 1 << 20;

#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("wal corrupt at lsn {0}: {1}")]
    Corrupt(u64, &'static str),
}

pub type Result<T> = std::result::Result<T, WalError>;

/// CRC-32 (IEEE 802.3, reflected) over a 256-entry table: identical values
/// to the original bitwise loop, ~8× faster. The bitwise version burned
/// ~30k iterations per 4 KB page frame right on the commit path.
fn crc32(data: &[u8]) -> u32 {
    static TABLE: std::sync::OnceLock<[u32; 256]> = std::sync::OnceLock::new();
    let table = TABLE.get_or_init(|| {
        let mut t = [0u32; 256];
        for (i, slot) in t.iter_mut().enumerate() {
            let mut c = i as u32;
            for _ in 0..8 {
                c = if c & 1 != 0 {
                    0xEDB8_8320 ^ (c >> 1)
                } else {
                    c >> 1
                };
            }
            *slot = c;
        }
        t
    });
    let mut crc = 0xFFFF_FFFFu32;
    for &b in data {
        crc = table[((crc ^ b as u32) & 0xFF) as usize] ^ (crc >> 8);
    }
    !crc
}

/// A recovered WAL entry.
#[derive(Debug, Clone)]
pub struct LogRecord {
    pub lsn: u64,
    pub kind: u8,
    pub txid: u64,
    pub payload: Vec<u8>,
}

pub struct Wal {
    file: File,
    path: PathBuf,
    next_lsn: u64,
    /// Highest appended Commit-frame LSN (0 = nothing appended).
    last_commit_lsn: u64,
    /// Highest commit LSN known durable (fsynced) — only [`Wal::sync`]
    /// advances it, so a deferred commit can lag but never lead durability.
    pub durable_lsn: u64,
    /// In-memory log size (bytes, header included). Appends and checkpoints
    /// are the only size changes, so `file_len` needs no `fstat` per commit.
    appended: u64,
    /// Number of deferred commit frames appended since the last fence.
    /// [`Wal::fence`] is a no-op when this is zero, so an ordinary commit
    /// batch does not pay an extra frame + fsync cycle.
    deferred_since_fence: u64,
    /// Txids of the outstanding (unfenced) deferred commits — the set
    /// [`Wal::abort_deferred`] voids on ROLLBACK.
    deferred_txids: Vec<u64>,
    /// Checkpoint generation: LSNs restart at 1 after every checkpoint, so a
    /// snapshot's LSN is only meaningful within its epoch (MVCC stage B).
    /// Monotonic across checkpoints for the lifetime of the process.
    epoch: u64,
    /// Set when the on-disk file has diverged from the in-memory counters in
    /// a way appends cannot recover from: `checkpoint` truncated the file
    /// but could not rewrite (or sync) the header. Appending after that
    /// would positional-write at the stale offset into a zero hole, and the
    /// log would reopen as `Corrupt(0, "bad header")` — a crash loop only a
    /// human can fix. Instead every later append fails loudly; the data
    /// file was already fully synced before the truncation (checkpoint
    /// precondition), so a restart rebuilds a fresh, valid, empty log.
    poisoned: Option<String>,
}

impl Wal {
    pub fn open(path: &Path) -> Result<Wal> {
        let exists = path.try_exists().map_err(WalError::Io)?;
        let mut file = {
            let mut opts = OpenOptions::new();
            opts.read(true).write(true).create(true).truncate(false); // never clobber an existing log
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt as _;
                // Owner-only, same rationale as the data file: the WAL is
                // a strict superset of a backup's sensitive content, and
                // pre-existing files are tightened on open.
                let f = opts.mode(0o600).open(path)?;
                {
                    use std::os::unix::fs::PermissionsExt as _;
                    if let Ok(m) = f.metadata() {
                        if m.permissions().mode() & 0o777 != 0o600 {
                            let _ = f.set_permissions(std::fs::Permissions::from_mode(0o600));
                        }
                    }
                }
                f
            }
            #[cfg(not(unix))]
            {
                opts.open(path)?
            }
        };
        if !exists || file.metadata()?.len() == 0 {
            file.write_all(HEADER)?;
            file.sync_all()?;
            return Ok(Wal {
                file,
                path: path.to_path_buf(),
                next_lsn: 1,
                last_commit_lsn: 0,
                durable_lsn: 0,
                appended: HEADER.len() as u64,
                deferred_since_fence: 0,
                deferred_txids: Vec::new(),
                epoch: 0,
                poisoned: None,
            });
        }
        // Validate the header, then stream the valid prefix: the log can be
        // arbitrarily large (a killed multi-GB transaction), so the scan
        // must not materialize it — one frame at a time, truncating at the
        // first torn/corrupt frame exactly like the old in-memory walk did.
        let len = file.metadata()?.len();
        if len < HEADER.len() as u64 {
            return Err(WalError::Corrupt(0, "bad header"));
        }
        let mut header = [0u8; HEADER.len()];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut header)?;
        if &header != HEADER {
            return Err(WalError::Corrupt(0, "bad header"));
        }
        let mut next_lsn = 1u64;
        let mut durable = 0u64;
        let mut good_end = HEADER.len() as u64;
        let mut open_tx: std::collections::HashSet<u64> = std::collections::HashSet::new();
        let mut deferred = DeferredSet::default();
        let mut reader = FrameReader::open(path)?;
        while let Some(rec) = reader.next() {
            let rec = rec?;
            good_end = reader.valid_end();
            match rec.kind {
                KIND_BEGIN => {
                    open_tx.insert(rec.txid);
                }
                KIND_COMMIT if open_tx.remove(&rec.txid) => durable = durable.max(rec.lsn),
                KIND_COMMIT_DEFERRED if open_tx.contains(&rec.txid) => {
                    deferred.push(rec.lsn, rec.txid);
                }
                KIND_FENCE => deferred.fence(rec.lsn),
                KIND_ABORT => {
                    open_tx.remove(&rec.txid);
                    deferred.abort(rec.txid, rec.lsn);
                }
                _ => {}
            }
            next_lsn = rec.lsn + 1;
        }
        drop(reader);
        // A deferred commit is only durable when a later fence (the SQL
        // COMMIT boundary) made it to disk. Without the fence the explicit
        // transaction never committed — dropping it is what keeps recovery
        // atomic. A deferred commit aborted by ROLLBACK stays dropped even
        // when a later fence covers its LSN.
        for lsn in deferred.durable_lsns() {
            durable = durable.max(lsn);
        }
        file.set_len(good_end)?;
        file.seek(SeekFrom::End(0))?;
        Ok(Wal {
            file,
            path: path.to_path_buf(),
            next_lsn,
            last_commit_lsn: durable,
            durable_lsn: durable,
            appended: good_end,
            deferred_since_fence: 0,
            deferred_txids: Vec::new(),
            epoch: 0,
            poisoned: None,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Checkpoint generation (bumped by [`Wal::checkpoint`]). A snapshot's
    /// LSN is only comparable within its epoch.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Highest appended Commit-frame LSN — the visible head for MVCC
    /// snapshot reads (deferred commits included: their page images are
    /// applied to the read surfaces before the append lock is released).
    pub fn last_commit_lsn(&self) -> u64 {
        self.last_commit_lsn
    }

    /// Current log size in bytes (in-memory counter: appends and
    /// checkpoints are the only size changes after `open`).
    pub fn file_len(&self) -> Result<u64> {
        Ok(self.appended)
    }

    fn append(&mut self, kind: u8, txid: u64, payload: &[u8]) -> Result<u64> {
        if let Some(reason) = &self.poisoned {
            return Err(WalError::Io(std::io::Error::other(reason.clone())));
        }
        let lsn = self.next_lsn;
        let mut frame = Vec::with_capacity(9 + 1 + 8 + 4 + payload.len() + 4);
        frame.extend_from_slice(&lsn.to_le_bytes());
        frame.push(kind);
        frame.extend_from_slice(&txid.to_le_bytes());
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(payload);
        let crc = crc32(&frame[8..]);
        frame.extend_from_slice(&crc.to_le_bytes());
        // Positional append at the tracked end: a failed write (ENOSPC/EIO)
        // must not leave the file cursor inside the frame, or the next
        // append would start after a partial frame and recovery would stop
        // there — silently discarding every later committed transaction.
        // On failure truncate the partial bytes so the retry overwrites.
        if let Err(e) = self.file.write_all_at(&frame, self.appended) {
            let _ = self.file.set_len(self.appended);
            return Err(e.into());
        }
        self.appended += frame.len() as u64;
        if kind == KIND_COMMIT_DEFERRED {
            self.deferred_since_fence += 1;
            self.deferred_txids.push(txid);
        }
        self.next_lsn += 1;
        Ok(lsn)
    }

    pub fn begin(&mut self, txid: u64) -> Result<u64> {
        self.append(KIND_BEGIN, txid, &[])
    }

    /// Log an after-image. Payload format is up to the caller (pager uses
    /// page_id:u32 + page bytes).
    pub fn log_write(&mut self, txid: u64, payload: &[u8]) -> Result<u64> {
        self.append(KIND_WRITE, txid, payload)
    }

    /// Commit: append Commit frame and fsync. After this call returns Ok,
    /// the transaction survives any crash. Unlike [`Wal::commit_deferred`]
    /// this is an immediately durable transaction (autocommit path).
    ///
    /// A fence is appended first when deferred commits are outstanding: this
    /// fsync makes them durable, so they must become recoverable too — a
    /// leaked write unit's pages are flushed to the data file by this same
    /// commit, and dropping their redo would roll the data file back to an
    /// older image on recovery.
    pub fn commit(&mut self, txid: u64) -> Result<u64> {
        self.fence()?;
        let lsn = self.append(KIND_COMMIT, txid, &[])?;
        self.last_commit_lsn = self.last_commit_lsn.max(lsn);
        self.sync()?;
        Ok(lsn)
    }

    /// Commit without fsync — durability arrives with the next `sync`
    /// (used to batch an explicit BEGIN..COMMIT into one flush). Only the
    /// appended commit LSN moves here; `durable_lsn` follows in `sync`.
    ///
    /// The frame is a *deferred* commit: recovery only counts it once a
    /// later [`Wal::fence`] is present, so a crash mid-transaction drops the
    /// whole prefix instead of replaying it.
    pub fn commit_deferred(&mut self, txid: u64) -> Result<u64> {
        let lsn = self.append(KIND_COMMIT_DEFERRED, txid, &[])?;
        self.last_commit_lsn = self.last_commit_lsn.max(lsn);
        Ok(lsn)
    }

    /// Append the durable-boundary marker for the deferred commits written
    /// since the last fence. Called by the pager's `sync_wal` immediately
    /// before its fsync: once the fsync succeeds, every deferred commit
    /// before the fence survives a crash; anything after it belongs to a
    /// still-open transaction and recovery drops it. A no-op (returns 0)
    /// when no deferred commit is outstanding, so autocommit batches don't
    /// grow an extra frame per fsync.
    pub fn fence(&mut self) -> Result<u64> {
        if self.deferred_since_fence == 0 {
            return Ok(0);
        }
        if self.deferred_txids.is_empty() {
            // Count > 0 with an empty list means the bookkeeping diverged
            // (only reachable through a corrupted abort path): fencing now
            // would legitimize deferred commits whose txids were lost.
            return Err(WalError::Io(io::Error::other(
                "WAL deferred bookkeeping diverged (outstanding count without txids); refusing to fence",
            )));
        }
        let lsn = self.append(KIND_FENCE, 0, &[])?;
        self.deferred_since_fence = 0;
        self.deferred_txids.clear();
        // The fence is a visible commit head too: snapshots begun after the
        // SQL COMMIT must include the deferred commits it covers.
        self.last_commit_lsn = self.last_commit_lsn.max(lsn);
        Ok(lsn)
    }

    /// Flush the log tail: every commit_deferred before this point is now
    /// durable. Recovery only replays the fsynced prefix, so a crash before
    /// sync simply drops those (uncommitted) transactions.
    pub fn sync(&mut self) -> Result<()> {
        if let Err(e) = self.file.sync_data() {
            // The already-appended COMMIT/FENCE frames may still reach the
            // disk via kernel writeback, so recovery could replay a
            // transaction the caller is about to report as failed — the
            // live catalog and any later recovery would diverge. Fail-stop
            // (see `poisoned`): refuse every further append; a restart
            // replays whatever actually made it to disk.
            self.poisoned = Some("WAL fsync failed; on-disk durability is ambiguous".into());
            return Err(e.into());
        }
        self.durable_lsn = self.durable_lsn.max(self.last_commit_lsn);
        Ok(())
    }

    pub fn abort(&mut self, txid: u64) -> Result<u64> {
        self.append(KIND_ABORT, txid, &[])
    }

    /// The ROLLBACK marker: append one ABORT frame per outstanding (unfenced)
    /// deferred commit and clear the pending set. Without this, the restore
    /// transaction's synchronous commit (`Wal::commit`) fences FIRST — a torn
    /// crash landing between "FENCE durable" and "restore COMMIT durable"
    /// would count every deferred statement of the rolled-back transaction as
    /// committed and resurrect it. With the ABORT frames, recovery voids those
    /// deferred commits no matter what fence follows.
    pub fn abort_deferred(&mut self) -> Result<()> {
        // Consume in place: on a mid-loop append failure the remaining txids
        // must survive. `fence()` keys off the counter alone, so an emptied
        // list with `deferred_since_fence > 0` would let the next fence
        // cover the very transactions this ROLLBACK is voiding and
        // resurrect them. Re-appending an ABORT for an already-voided txid
        // is harmless on recovery, so retrying the loop is idempotent.
        let mut i = 0;
        while i < self.deferred_txids.len() {
            let txid = self.deferred_txids[i];
            self.append(KIND_ABORT, txid, &[])?;
            i += 1;
        }
        self.deferred_txids.clear();
        self.deferred_since_fence = 0;
        Ok(())
    }

    /// Iterate all valid frames (recovery input), in LSN order. Continuity
    /// is seeded from the first frame (via [`FrameReader`]).
    pub fn records(&self) -> Result<Vec<LogRecord>> {
        // Through a fresh read-only handle: `self.file`'s cursor is owned by
        // the append path, and this must stay callable while it appends.
        Self::scan_file(&self.path)
    }

    /// Streaming frame source over `path` via a fresh read-only handle,
    /// holding no lock: safe while a writer appends. Callers that process
    /// records one at a time (recovery, snapshot materialization) must use
    /// this instead of [`Wal::scan_file`] — the log can outgrow memory (a
    /// killed multi-GB transaction), and collecting every frame first then
    /// OOMs before any replay/truncation can happen.
    pub fn frames(path: &Path) -> Result<FrameReader> {
        FrameReader::open(path)
    }

    /// Read and parse every valid frame from `path` via a fresh read-only
    /// handle, holding no lock: safe while a writer appends (frames an
    /// existing snapshot needs were fully written before that snapshot
    /// began; a torn tail just stops the scan). A concurrent `checkpoint`
    /// can invalidate what is read — callers must re-validate the epoch
    /// under the append lock afterwards. Materializing callers only.
    pub fn scan_file(path: &Path) -> Result<Vec<LogRecord>> {
        let mut out = Vec::new();
        for rec in Self::frames(path)? {
            out.push(rec?);
        }
        Ok(out)
    }

    /// Checkpoint: after the data file is fully synced, drop the log.
    /// Safe because every committed change is now in the data file.
    /// `next_lsn` restarts at 1 so the fresh log's first frame validates
    /// against reopen scans (which seed continuity from the first frame).
    pub fn checkpoint(&mut self) -> Result<()> {
        self.file.set_len(0)?;
        // Rewrite the header (seek first) before the counters reset: if the
        // truncation landed but the header (or its sync) failed, the file on
        // disk no longer matches `appended`/`next_lsn` — poison the log so
        // no later append positional-writes into the zero hole (see
        // `poisoned`). The seek belongs inside the guarded block: failing
        // after set_len(0) leaves the same header-less zero-hole state.
        if let Err(e) = self
            .file
            .seek(SeekFrom::Start(0))
            .and_then(|_| self.file.write_all(HEADER))
            .and_then(|()| self.file.sync_all())
        {
            self.poisoned =
                Some("checkpoint truncated the WAL but could not rewrite its header".into());
            return Err(e.into());
        }
        self.durable_lsn = 0;
        self.last_commit_lsn = 0;
        self.next_lsn = 1;
        self.appended = HEADER.len() as u64;
        self.deferred_since_fence = 0;
        self.deferred_txids.clear();
        self.epoch += 1;
        // A retry that got this far rewrote and synced a valid header over
        // the truncated file with the counters reset to match — the file
        // and the in-memory state are consistent again, so a previous
        // poison no longer applies.
        self.poisoned = None;
        Ok(())
    }
}

/// Streaming frame reader (see [`Wal::frames`]): parses one frame at a
/// time off a buffered read-only handle, so a log of any size costs one
/// frame of memory instead of a copy of the file. Yields records in LSN
/// order with the exact `scan_file` torn-write rule: a corrupt/torn tail
/// (short frame, bad CRC, absurd length) stops iteration silently; an LSN
/// gap is a hard error — `Wal::open` stops there and truncates, replay
/// callers propagate it.
pub struct FrameReader {
    reader: io::BufReader<File>,
    next: Option<u64>,
    pos: u64,
    file_len: u64,
    done: bool,
}

impl FrameReader {
    pub fn open(path: &Path) -> Result<FrameReader> {
        let file = File::open(path)?;
        let file_len = file.metadata()?.len();
        let mut reader = io::BufReader::with_capacity(64 * 1024, file);
        reader.seek(SeekFrom::Start(HEADER.len() as u64))?;
        Ok(FrameReader {
            reader,
            next: None,
            pos: HEADER.len() as u64,
            file_len,
            done: false,
        })
    }

    /// End offset of the last yielded frame — the torn-tail truncation
    /// point after iteration stops.
    pub fn valid_end(&self) -> u64 {
        self.pos
    }

    fn read_frame(&mut self) -> Result<Option<(LogRecord, usize)>> {
        // Frame layout: lsn:u64 | kind:u8 | txid:u64 | len:u32 | payload | crc:u32.
        const HEAD_LEN: usize = 8 + 1 + 8 + 4;
        let mut head = [0u8; HEAD_LEN];
        match self.reader.read_exact(&mut head) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        let lsn = u64::from_le_bytes(head[0..8].try_into().unwrap());
        let len = u32::from_le_bytes(head[17..21].try_into().unwrap()) as usize;
        if len > MAX_FRAME_PAYLOAD {
            // Garbage length: cannot know where the frame would end, so it
            // is indistinguishable from a torn tail. Stop (the opener
            // truncates here); never allocate on the strength of it.
            return Ok(None);
        }
        let frame_end = self.pos + (HEAD_LEN as u64) + (len as u64) + 4;
        let mut frame = vec![0u8; HEAD_LEN + len + 4];
        frame[..HEAD_LEN].copy_from_slice(&head);
        match self.reader.read_exact(&mut frame[HEAD_LEN..]) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        match parse_frame(&frame, self.next) {
            Ok(Some((rec, adv))) => Ok(Some((rec, adv))),
            // A torn/corrupt frame is only tolerable as the file tail. With
            // valid bytes after it (frame_end < file_len) the damage is
            // mid-log: silently truncating here would discard every later
            // committed transaction, so fail loudly instead.
            Ok(None) | Err(_) if frame_end < self.file_len => {
                Err(WalError::Corrupt(lsn, "corrupt frame mid-log"))
            }
            Ok(None) => Ok(None),
            Err(_e) => Ok(None),
        }
    }
}

impl Iterator for FrameReader {
    type Item = Result<LogRecord>;

    fn next(&mut self) -> Option<Result<LogRecord>> {
        if self.done {
            return None;
        }
        match self.read_frame() {
            Ok(Some((rec, adv))) => {
                self.pos += adv as u64;
                self.next = Some(rec.lsn + 1);
                Some(Ok(rec))
            }
            Ok(None) => {
                self.done = true;
                None
            }
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}

/// Parse one frame at the start of `buf`. `expect_lsn` validates continuity
/// (`None` accepts any LSN, used for the first frame in the log).
/// Returns None on a torn/corrupt tail (replay must stop there).
fn parse_frame(buf: &[u8], expect_lsn: Option<u64>) -> Result<Option<(LogRecord, usize)>> {
    if buf.is_empty() {
        return Ok(None);
    }
    const MIN: usize = 8 + 1 + 8 + 4 + 4;
    if buf.len() < MIN {
        return Ok(None);
    }
    let lsn = u64::from_le_bytes(buf[0..8].try_into().unwrap());
    if let Some(expect) = expect_lsn {
        if lsn != expect {
            return Err(WalError::Corrupt(expect, "lsn gap"));
        }
    }
    let kind = buf[8];
    let txid = u64::from_le_bytes(buf[9..17].try_into().unwrap());
    let len = u32::from_le_bytes(buf[17..21].try_into().unwrap()) as usize;
    let end = 21 + len + 4;
    if buf.len() < end {
        return Ok(None);
    }
    let stored = u32::from_le_bytes(buf[21 + len..end].try_into().unwrap());
    if crc32(&buf[8..21 + len]) != stored {
        return Ok(None);
    }
    Ok(Some((
        LogRecord {
            lsn,
            kind,
            txid,
            payload: buf[21..21 + len].to_vec(),
        },
        end,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wal_dir() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("test.wal");
        (dir, p)
    }

    #[test]
    fn crc32_known_answer() {
        // Standard CRC-32 check value: pins the table-driven implementation
        // to byte-identical output of the original bitwise loop (WAL frames
        // written by older versions must keep verifying).
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    /// ROLLBACK 的 ABORT 语义:被作废的 deferred 提交即使被更晚的 FENCE
    /// 覆盖也不得复活。模拟撕裂崩溃停在「FENCE 已持久、恢复事务 COMMIT
    /// 未持久」—— 曾经这条序列会把整个已回滚事务判成已提交。
    #[test]
    fn aborted_deferred_stays_dropped_under_later_fence() {
        let (_dir, path) = wal_dir();
        {
            let mut w = Wal::open(&path).unwrap();
            let t = 7u64;
            w.begin(t).unwrap();
            w.log_write(t, b"after-image").unwrap();
            w.commit_deferred(t).unwrap();
            // ROLLBACK:先作废,再(撕裂后)出现一个 fence。
            w.abort_deferred().unwrap();
            w.fence().unwrap();
            w.sync().unwrap();
        }
        let w = Wal::open(&path).unwrap();
        assert_eq!(w.durable_lsn, 0, "aborted deferred 不得因 fence 复活");
        // 正常提交(fence 覆盖、未 ABORT)照常 durable。
        let (_dir2, path2) = wal_dir();
        {
            let mut w = Wal::open(&path2).unwrap();
            let t = 9u64;
            w.begin(t).unwrap();
            w.log_write(t, b"img").unwrap();
            w.commit_deferred(t).unwrap();
            w.fence().unwrap();
            w.sync().unwrap();
        }
        let w2 = Wal::open(&path2).unwrap();
        assert!(w2.durable_lsn > 0);
    }

    #[test]
    fn commit_durable_across_reopen() {
        let (_dir, path) = wal_dir();
        {
            let mut w = Wal::open(&path).unwrap();
            w.begin(1).unwrap();
            w.log_write(1, b"page0-after-image").unwrap();
            w.commit(1).unwrap();
        }
        let w = Wal::open(&path).unwrap();
        assert_eq!(w.durable_lsn, 3);
        let recs = w.records().unwrap();
        let kinds: Vec<u8> = recs.iter().map(|r| r.kind).collect();
        assert_eq!(kinds, vec![KIND_BEGIN, KIND_WRITE, KIND_COMMIT]);
        assert!(_dir.path().exists());
    }

    #[test]
    fn uncommitted_tx_missing_after_reopen() {
        let (_dir, path) = wal_dir();
        {
            let mut w = Wal::open(&path).unwrap();
            w.begin(1).unwrap();
            w.log_write(1, b"doomed").unwrap();
            // no commit: crash
        }
        let w = Wal::open(&path).unwrap();
        assert_eq!(w.durable_lsn, 0);
        assert_eq!(w.records().unwrap().len(), 2);
    }

    #[test]
    fn torn_tail_is_truncated_and_appends_work() {
        let (_dir, path) = wal_dir();
        {
            let mut w = Wal::open(&path).unwrap();
            w.begin(1).unwrap();
            w.commit(1).unwrap();
        }
        // Simulate torn write: append half a frame.
        let size = std::fs::metadata(&path).unwrap().len();
        let mut f = OpenOptions::new().write(true).open(&path).unwrap();
        f.seek(SeekFrom::Start(size)).unwrap();
        f.write_all(&[9, 9, 9]).unwrap();
        f.sync_all().unwrap();

        let mut w = Wal::open(&path).unwrap();
        assert_eq!(w.durable_lsn, 2);
        w.begin(2).unwrap();
        w.commit(2).unwrap();
        let recs = w.records().unwrap();
        assert_eq!(recs.len(), 4);
        assert_eq!(recs[3].kind, KIND_COMMIT);
    }

    #[test]
    fn checksum_corruption_mid_log_fails_open_loudly() {
        // A bad frame with valid frames after it is real corruption, not a
        // torn tail: truncating there would silently discard every later
        // committed transaction. The open must fail so an operator can see
        // it (recovery must never paper over mid-log damage).
        let (_dir, path) = wal_dir();
        {
            let mut w = Wal::open(&path).unwrap();
            w.begin(1).unwrap();
            w.log_write(1, b"payload!").unwrap();
            w.commit(1).unwrap();
        }
        let mut data = std::fs::read(&path).unwrap();
        let idx = data.iter().position(|&b| b == b'!').unwrap();
        data[idx] ^= 0xff;
        std::fs::write(&path, data).unwrap();

        let err = match Wal::open(&path) {
            Err(e) => e,
            Ok(_) => panic!("corrupt mid-log frame must fail the open"),
        };
        assert!(matches!(err, WalError::Corrupt(_, _)), "{err:?}");
    }

    #[test]
    fn checksum_corruption_at_tail_is_truncated() {
        // A corrupt *last* frame is indistinguishable from a torn write and
        // is truncated like before.
        let (_dir, path) = wal_dir();
        {
            let mut w = Wal::open(&path).unwrap();
            w.begin(1).unwrap();
            w.log_write(1, b"payload!").unwrap();
            w.commit(1).unwrap();
        }
        let mut data = std::fs::read(&path).unwrap();
        // Flip a byte in the COMMIT frame's txid (last frame, tail).
        let n = data.len();
        data[n - 10] ^= 0xff;
        std::fs::write(&path, data).unwrap();

        let w = Wal::open(&path).unwrap();
        // BEGIN + WRITE survive; the corrupt tail frame is dropped.
        assert_eq!(w.records().unwrap().len(), 2);
        assert_eq!(w.durable_lsn, 0);
    }

    #[test]
    fn frame_reader_streams_what_scan_file_collects() {
        let (_dir, path) = wal_dir();
        {
            let mut w = Wal::open(&path).unwrap();
            w.begin(1).unwrap();
            w.log_write(1, b"one").unwrap();
            w.commit(1).unwrap();
            w.begin(2).unwrap();
            w.log_write(2, b"two").unwrap();
            w.abort(2).unwrap();
        }
        let scanned = Wal::scan_file(&path).unwrap();
        let streamed: Vec<LogRecord> = Wal::frames(&path).unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(streamed.len(), scanned.len());
        for (a, b) in streamed.iter().zip(scanned.iter()) {
            assert_eq!((a.lsn, a.kind, a.txid), (b.lsn, b.kind, b.txid));
            assert_eq!(a.payload, b.payload);
        }
        // 全部帧有效:valid_end 落在文末。
        let mut reader = Wal::frames(&path).unwrap();
        while reader.next().is_some() {}
        assert_eq!(reader.valid_end(), std::fs::metadata(&path).unwrap().len());
    }

    #[test]
    fn frame_reader_stops_at_torn_tail() {
        let (_dir, path) = wal_dir();
        {
            let mut w = Wal::open(&path).unwrap();
            w.begin(1).unwrap();
            w.commit(1).unwrap();
        }
        let good = std::fs::metadata(&path).unwrap().len();
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[9, 9, 9]).unwrap(); // half a frame header
        }
        let mut reader = Wal::frames(&path).unwrap();
        assert_eq!(reader.by_ref().filter(|r| r.is_ok()).count(), 2);
        assert_eq!(
            reader.valid_end(),
            good,
            "torn tail excluded from the prefix"
        );
        // 打开时按前缀截断,后续 append 不受影响。
        let w = Wal::open(&path).unwrap();
        assert_eq!(w.durable_lsn, 2);
    }

    #[test]
    fn frame_reader_reports_lsn_gap_and_open_truncates_at_it() {
        let (_dir, path) = wal_dir();
        {
            let mut w = Wal::open(&path).unwrap();
            w.begin(1).unwrap();
            w.log_write(1, b"one").unwrap();
            w.commit(1).unwrap();
        }
        // begin(25B) + write(28B) + commit(25B) + 8B 头 = 86;把 COMMIT 的
        // lsn 改掉制造断号(lsn 不在 CRC 覆盖范围内,校验仍通过)。
        let mut data = std::fs::read(&path).unwrap();
        assert_eq!(data.len(), 86);
        let gap_at = 8 + 25 + 28;
        let lsn = u64::from_le_bytes(data[gap_at..gap_at + 8].try_into().unwrap());
        data[gap_at..gap_at + 8].copy_from_slice(&(lsn + 10).to_le_bytes());
        std::fs::write(&path, data).unwrap();

        let mut reader = Wal::frames(&path).unwrap();
        assert_eq!(reader.next().unwrap().unwrap().kind, KIND_BEGIN);
        assert_eq!(reader.next().unwrap().unwrap().kind, KIND_WRITE);
        // A gap at the very tail is treated like a torn write: the scan
        // stops (and `open` truncates there). A gap with valid bytes after
        // it (tested separately) is a hard error instead.
        assert!(reader.next().is_none());
        // 打开器把断号处当尾部截断,只保留前缀。
        let w = Wal::open(&path).unwrap();
        assert_eq!(w.durable_lsn, 0);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 8 + 25 + 28);
    }

    #[test]
    fn frame_reader_rejects_absurd_length_as_torn_tail() {
        // 长度字段损坏成巨大值时按撕裂尾处理,绝不据此分配内存。
        let (_dir, path) = wal_dir();
        {
            let mut w = Wal::open(&path).unwrap();
            w.begin(1).unwrap();
            w.commit(1).unwrap();
        }
        let good = std::fs::metadata(&path).unwrap().len();
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            let mut head = vec![0u8; 21];
            head[0] = 7; // lsn
            head[8] = KIND_WRITE;
            head[17..21].copy_from_slice(&(u32::MAX / 2).to_le_bytes());
            f.write_all(&head).unwrap();
        }
        let mut reader = Wal::frames(&path).unwrap();
        assert_eq!(reader.by_ref().filter(|r| r.is_ok()).count(), 2);
        assert_eq!(reader.valid_end(), good);
    }
    #[test]
    fn checkpoint_clears_log() {
        let (_dir, path) = wal_dir();
        let mut w = Wal::open(&path).unwrap();
        w.begin(1).unwrap();
        w.commit(1).unwrap();
        w.checkpoint().unwrap();
        assert_eq!(w.records().unwrap().len(), 0);
        assert_eq!(w.durable_lsn, 0);
        // Can keep writing after checkpoint (LSNs restart at 1).
        w.begin(2).unwrap();
        w.commit(2).unwrap();
        assert_eq!(w.durable_lsn, 2);
    }

    #[test]
    fn post_checkpoint_records_survive_reopen() {
        let (_dir, path) = wal_dir();
        {
            let mut w = Wal::open(&path).unwrap();
            w.begin(1).unwrap();
            w.commit(1).unwrap();
            w.checkpoint().unwrap();
            // Post-checkpoint frames continue from the in-memory next_lsn.
            w.begin(2).unwrap();
            w.log_write(2, b"after-checkpoint").unwrap();
            w.commit(2).unwrap();
        }
        let w = Wal::open(&path).unwrap();
        // Reopen must NOT truncate the post-checkpoint tail as a "torn" end:
        // all three frames survive and the commit replays.
        let recs = w.records().unwrap();
        assert_eq!(recs.len(), 3);
        assert!(recs.iter().any(|r| r.payload == b"after-checkpoint"));
        assert_eq!(w.durable_lsn, recs[2].lsn);
    }

    #[test]
    fn reopen_after_checkpoint_and_more_writes_keeps_durable_data() {
        // Full pager-level roundtrip: write past a checkpoint, reopen, and
        // confirm replay still finds the post-checkpoint commit.
        let (_dir, path) = wal_dir();
        {
            let mut w = Wal::open(&path).unwrap();
            w.begin(1).unwrap();
            w.commit(1).unwrap();
            w.checkpoint().unwrap();
            w.begin(2).unwrap();
            w.log_write(2, b"page-image").unwrap();
            w.commit(2).unwrap();
        }
        // Simulate a fresh open tearing the tail: reopen scans with the first
        // frame's LSN as the continuity seed, so nothing is discarded.
        let w = Wal::open(&path).unwrap();
        let recs = w.records().unwrap();
        assert_eq!(recs.len(), 3);
        assert!(recs.iter().any(|r| r.payload == b"page-image"));
        assert_eq!(w.durable_lsn, w.last_commit_lsn());
        assert_eq!(w.durable_lsn, recs.last().unwrap().lsn);
    }

    #[test]
    fn lsn_monotonic_and_multi_tx() {
        let (_dir, path) = wal_dir();
        let mut w = Wal::open(&path).unwrap();
        w.begin(1).unwrap();
        w.begin(2).unwrap();
        let a = w.log_write(1, b"a").unwrap();
        let b = w.log_write(2, b"b").unwrap();
        assert!(b > a);
        w.commit(1).unwrap();
        w.abort(2).unwrap();
        let recs = w.records().unwrap();
        assert_eq!(recs.len(), 6);
        assert_eq!(w.durable_lsn, 5);
    }
    #[test]
    fn corrupt_wal_header_and_path() {
        let (_dir, path) = wal_dir();
        std::fs::write(&path, vec![0u8; 64]).unwrap();
        assert!(matches!(Wal::open(&path), Err(WalError::Corrupt(0, _))));
        let w = Wal::open(&path).unwrap_or_else(|_| {
            // 坏头文件被拒后换个新路径
            let p2 = _dir.path().join("ok.wal");
            Wal::open(&p2).unwrap()
        });
        let p = w.path().to_path_buf();
        assert!(p.to_string_lossy().ends_with(".wal"));
    }
}
