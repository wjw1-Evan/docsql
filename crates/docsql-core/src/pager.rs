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
            let data = self.read_file_page(id)?;
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

    /// Stage a full-page write inside `tx`.
    pub fn write_page(&mut self, tx: &mut Tx, id: u32, offset: usize, data: &[u8]) -> Result<()> {
        if offset + data.len() > PAGE_SIZE {
            return Err(PagerError::OutOfRange(id, u32::MAX));
        }
        if id >= self.num_pages && !tx.staged.contains_key(&id) {
            return Err(PagerError::OutOfRange(id, self.num_pages));
        }
        let page = tx.staged.entry(id).or_insert_with(|| {
            self.read_file_page(id)
                .unwrap_or_else(|_| vec![0u8; PAGE_SIZE])
        });
        page[offset..offset + data.len()].copy_from_slice(data);
        Ok(())
    }

    /// Commit: WAL-log every staged after-image, fsync WAL, then write pages
    /// to the data file. Returns the commit LSN.
    pub fn commit_tx(&mut self, tx: Tx) -> Result<u64> {
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
        let lsn = self.wal.commit(tx.id)?;
        // WAL durable — now apply to data file and update the buffer pool.
        for (id, data) in &tx.staged {
            self.write_file_page(*id, data)?;
            if let Some(p) = self.pool.get_mut(id) {
                p.data = data.clone();
                p.dirty = false;
            }
        }
        Ok(lsn)
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
}
