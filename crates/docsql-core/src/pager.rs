//! Paged data file + buffer pool, wired through the WAL.
//!
//! File layout: page 0 is the file header; pages 1.. hold data. Page size
//! is 4096. Writes go through in-memory transactions (`Tx`): on commit the
//! dirty page after-images are logged to the WAL and fsynced (commit record)
//! *before* the data file is updated — that ordering is what makes committed
//! transactions survive crashes.

use crate::wal::{Wal, WalError, KIND_WRITE};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub const PAGE_SIZE: usize = 4096;
const MAGIC: &[u8; 8] = b"DOCSQLP1";
/// header: magic(8) page_size:u32(4) num_pages:u32(4) reserved
const HEADER_LEN: usize = 16;

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
}

pub type Result<T> = std::result::Result<T, PagerError>;

pub struct Pager {
    file: File,
    path: PathBuf,
    wal: Wal,
    num_pages: u32,
    next_txid: u64,
    pool: HashMap<u32, Page>,
    pool_order: Vec<u32>, // simple FIFO eviction track
    max_pool: usize,
    /// Committed page images not yet written to the data file (deferred
    /// commits). They are WAL-logged but the data file must not see them
    /// before the WAL is fsynced, or a crash could resurrect pages of a
    /// transaction whose commit record was lost. Flushed by `sync_wal` /
    /// the next fsyncing commit.
    pending_writes: std::collections::BTreeMap<u32, Vec<u8>>,
}

struct Page {
    data: Vec<u8>,
    dirty: bool,
}

impl Pager {
    /// Open (creating if needed) a database at `path` with WAL at `path.wal`,
    /// running crash recovery first.
    pub fn open(path: &Path) -> Result<Pager> {
        let wal_path = wal_path_for(path);
        let wal = Wal::open(&wal_path)?;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false) // never clobber an existing database
            .open(path)?;

        let num_pages = if file.metadata()?.len() == 0 {
            let mut header = vec![0u8; HEADER_LEN];
            header[..8].copy_from_slice(MAGIC);
            header[8..12].copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
            header[12..16].copy_from_slice(&1u32.to_le_bytes()); // page 0 only
            file.write_all(&header)?;
            file.sync_all()?;
            1
        } else {
            let mut header = [0u8; HEADER_LEN];
            file.seek(SeekFrom::Start(0))?;
            file.read_exact(&mut header)?;
            if &header[..8] != MAGIC
                || u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize != PAGE_SIZE
            {
                return Err(PagerError::BadHeader);
            }
            u32::from_le_bytes(header[12..16].try_into().unwrap())
        };
        let mut pager = Pager {
            file,
            path: path.to_path_buf(),
            wal,
            num_pages,
            next_txid: 1,
            pool: HashMap::new(),
            pool_order: Vec::new(),
            max_pool: 1024,
            pending_writes: std::collections::BTreeMap::new(),
        };
        pager.recover()?;
        Ok(pager)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn num_pages(&self) -> u32 {
        self.num_pages
    }

    /// Crash recovery: replay after-images of committed transactions, in LSN
    /// order, then checkpoint the WAL. Uncommitted transactions are dropped.
    fn recover(&mut self) -> Result<()> {
        let records = self.wal.records()?;
        let mut committed = std::collections::HashSet::new();
        for r in &records {
            if r.kind == crate::wal::KIND_COMMIT {
                committed.insert(r.txid);
            }
        }
        let mut replayed: HashMap<u32, Vec<u8>> = HashMap::new();
        for r in &records {
            if r.kind != KIND_WRITE || !committed.contains(&r.txid) {
                continue;
            }
            let Some(page) = decode_page_image(&r.payload)? else {
                continue;
            };
            replayed.insert(page.0, page.1);
            if page.0 >= self.num_pages {
                self.num_pages = page.0 + 1;
            }
        }
        if !replayed.is_empty() {
            for (id, data) in &replayed {
                self.write_file_page(*id, data)?;
            }
            self.persist_header()?;
            self.file.sync_all()?;
        }
        self.wal.checkpoint()?;
        Ok(())
    }

    fn read_file_page(&mut self, id: u32) -> Result<Vec<u8>> {
        if id >= self.num_pages {
            return Err(PagerError::OutOfRange(id, self.num_pages));
        }
        let mut buf = vec![0u8; PAGE_SIZE];
        self.file
            .seek(SeekFrom::Start(id as u64 * PAGE_SIZE as u64))?;
        self.file.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn write_file_page(&mut self, id: u32, data: &[u8]) -> Result<()> {
        debug_assert_eq!(data.len(), PAGE_SIZE);
        if id >= self.num_pages {
            self.num_pages = id + 1;
            self.persist_header()?;
        }
        self.file
            .seek(SeekFrom::Start(id as u64 * PAGE_SIZE as u64))?;
        self.file.write_all(data)?;
        Ok(())
    }

    fn persist_header(&mut self) -> Result<()> {
        let mut header = [0u8; HEADER_LEN];
        header[..8].copy_from_slice(MAGIC);
        header[8..12].copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
        header[12..16].copy_from_slice(&self.num_pages.to_le_bytes());
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&header)?;
        Ok(())
    }

    /// Read a page (buffered). Page 0 is the header — callers use data pages.
    pub fn read_page(&mut self, id: u32) -> Result<&[u8]> {
        if id == 0 {
            return Err(PagerError::OutOfRange(0, self.num_pages));
        }
        if !self.pool.contains_key(&id) {
            let data = if let Some(p) = self.pending_writes.get(&id) {
                p.clone()
            } else {
                self.read_file_page(id)?
            };
            self.evict_if_full();
            self.pool_order.push(id);
            self.pool.insert(id, Page { data, dirty: false });
        }
        Ok(&self.pool.get(&id).unwrap().data)
    }

    fn evict_if_full(&mut self) {
        while self.pool.len() >= self.max_pool {
            // Evict oldest clean page from the front of pool_order.
            let mut evict: Option<u32> = None;
            for &id in &self.pool_order {
                if let Some(p) = self.pool.get(&id) {
                    if !p.dirty {
                        evict = Some(id);
                        break;
                    }
                }
            }
            match evict {
                Some(id) => {
                    self.pool.remove(&id);
                    self.pool_order.retain(|&x| x != id);
                }
                None => break, // all dirty; grow beyond target rather than lose data
            }
        }
    }

    /// Allocate a new page, zero-filled (visible only after commit).
    pub fn allocate_page(&mut self, tx: &mut Tx) -> Result<u32> {
        let id = self.num_pages;
        self.num_pages += 1;
        tx.staged.insert(id, vec![0u8; PAGE_SIZE]);
        Ok(id)
    }

    pub fn num_pages_now(&self) -> u32 {
        self.num_pages
    }

    /// Begin a write transaction. Staged page writes are private until commit.
    pub fn begin_tx(&mut self) -> Tx {
        let txid = self.next_txid;
        self.next_txid += 1;
        Tx {
            id: txid,
            staged: HashMap::new(),
        }
    }

    /// Latest committed image of a page: pool copy first, then a deferred
    /// (not yet flushed) image, then the data file. A read failure is a real
    /// I/O error and must propagate — silently staging a zero page would
    /// commit 4 KB of zeros over live data.
    fn current_page_image(&mut self, id: u32) -> Result<Vec<u8>> {
        if let Some(p) = self.pool.get(&id) {
            return Ok(p.data.clone());
        }
        if let Some(p) = self.pending_writes.get(&id) {
            return Ok(p.clone());
        }
        self.read_file_page(id)
    }

    /// Stage a full-page write inside `tx`.
    pub fn write_page(&mut self, tx: &mut Tx, id: u32, offset: usize, data: &[u8]) -> Result<()> {
        if offset + data.len() > PAGE_SIZE {
            return Err(PagerError::OutOfRange(id, u32::MAX));
        }
        if id >= self.num_pages && !tx.staged.contains_key(&id) {
            return Err(PagerError::OutOfRange(id, self.num_pages));
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
    pub fn commit_tx(&mut self, tx: Tx) -> Result<u64> {
        self.commit_tx_inner(tx, true)
    }

    /// Commit without the WAL fsync — used inside explicit SQL
    /// transactions so the whole batch pays one flush at COMMIT
    /// (`sync_wal`) instead of one per statement. The data file is NOT
    /// touched until the WAL is durable: staged images become visible via
    /// the buffer pool and `pending_writes`.
    pub fn commit_tx_deferred(&mut self, tx: Tx) -> Result<u64> {
        self.commit_tx_inner(tx, false)
    }

    /// Make every deferred commit durable: fsync the WAL first (write-ahead
    /// rule), then flush its page images to the data file.
    pub fn sync_wal(&mut self) -> Result<()> {
        self.wal.sync().map_err(PagerError::Wal)?;
        self.flush_pending()?;
        self.maybe_checkpoint()
    }

    /// Write `pending_writes` through to the data file and update the pool.
    /// Entries are popped one at a time so an I/O failure leaves the rest
    /// queued (their WAL records are already durable and replay on reopen).
    fn flush_pending(&mut self) -> Result<()> {
        while let Some((&id, _)) = self.pending_writes.iter().next() {
            let (_, data) = self
                .pending_writes
                .pop_first()
                .expect("key just observed present");
            self.write_file_page(id, &data)?;
            if let Some(p) = self.pool.get_mut(&id) {
                p.data = data;
                p.dirty = false;
            }
        }
        Ok(())
    }

    fn commit_tx_inner(&mut self, tx: Tx, fsync: bool) -> Result<u64> {
        if tx.staged.is_empty() {
            return Ok(0);
        }
        self.wal.begin(tx.id)?;
        for (id, data) in &tx.staged {
            let mut payload = Vec::with_capacity(4 + PAGE_SIZE);
            payload.extend_from_slice(&id.to_le_bytes());
            payload.extend_from_slice(data);
            self.wal.log_write(tx.id, &payload)?;
        }
        let lsn = if fsync {
            self.wal.commit(tx.id)?
        } else {
            self.wal.commit_deferred(tx.id)?
        };
        // WAL durable — now apply to the buffer pool. In the deferred path
        // the data file is deliberately left alone until `sync_wal`, so a
        // crash can never leave uncommitted page images in the data file.
        for (id, data) in &tx.staged {
            if let Some(p) = self.pool.get_mut(id) {
                p.data = data.clone();
                p.dirty = false;
            }
            if !fsync {
                self.pending_writes.insert(*id, data.clone());
            }
        }
        if fsync {
            // The WAL fsync also durable-d every earlier deferred commit:
            // flush those pages too, then write this tx's pages.
            self.flush_pending()?;
            for (id, data) in &tx.staged {
                self.write_file_page(*id, data)?;
            }
            self.maybe_checkpoint()?;
        }
        Ok(lsn)
    }

    /// Bound WAL growth (and with it fsync cost): once the log exceeds a
    /// few MB and the data file is synced, every committed change lives in
    /// the data file and the log can be dropped.
    fn maybe_checkpoint(&mut self) -> Result<()> {
        const WAL_LIMIT: u64 = 8 * 1024 * 1024;
        if self.wal.file_len()? < WAL_LIMIT {
            return Ok(());
        }
        self.file.sync_all()?;
        self.wal.checkpoint()?;
        Ok(())
    }

    /// Abort: staged writes never reach disk, nothing to undo (WAL never
    /// got a commit record), so this only bookkeeps.
    pub fn abort_tx(&mut self, tx: Tx) -> Result<()> {
        if !tx.staged.is_empty() {
            self.wal.abort(tx.id)?;
        }
        Ok(())
    }

    /// Sync the data file (e.g. before checkpointing in the future).
    pub fn sync(&mut self) -> Result<()> {
        self.file.sync_all().map_err(PagerError::Io)
    }
}

/// An in-flight page transaction.
pub struct Tx {
    id: u64,
    staged: HashMap<u32, Vec<u8>>,
}

impl Tx {
    pub fn id(&self) -> u64 {
        self.id
    }

    /// A staged (uncommitted) image of a page, if this tx wrote it.
    pub fn staged_page(&self, id: u32) -> Option<&[u8]> {
        self.staged.get(&id).map(|v| v.as_slice())
    }
}

fn wal_path_for(path: &Path) -> PathBuf {
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
        let mut pager = Pager::open(&path).unwrap();
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
            let mut pager = Pager::open(&path).unwrap();
            let mut tx = pager.begin_tx();
            let p = pager.allocate_page(&mut tx).unwrap();
            pager.write_page(&mut tx, p, 0, magic).unwrap();
            pager.commit_tx(tx).unwrap();
        }
        let mut pager = Pager::open(&path).unwrap();
        let page = pager.read_page(1).unwrap().to_vec();
        assert_eq!(&page[..magic.len()], magic);
    }

    #[test]
    fn uncommitted_writes_are_lost_on_reopen() {
        let (_dir, path) = tmp_db("c.db");
        {
            let mut pager = Pager::open(&path).unwrap();
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
        let mut pager = Pager::open(&path).unwrap();
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
        let mut pager = Pager::open(&path).unwrap();
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
            let mut pager = Pager::open(&path).unwrap();
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
        let mut pager = Pager::open(&path).unwrap();
        let page = pager.read_page(1).unwrap().to_vec();
        assert_eq!(&page[..11], b"wal-rescued");
    }

    #[test]
    fn multiple_pages_and_partial_writes() {
        let (_dir, path) = tmp_db("g.db");
        let mut pager = Pager::open(&path).unwrap();
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
        let mut pager = Pager::open(&path).unwrap();
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
        let mut pager = Pager::open(&path).unwrap();
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
        let mut reopened = Pager::open(&path).unwrap();
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
            let mut pager = Pager::open(&path).unwrap();
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
        let mut pager = Pager::open(&path).unwrap();
        let page = pager.read_page(page_id).unwrap().to_vec();
        assert_eq!(&page[..magic.len()], magic);
        assert_eq!(&page[20..26], b"APPEND");
    }
}
