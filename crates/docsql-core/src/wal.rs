//! Write-ahead log.
//!
//! Every page modification is logged before the data file is touched.
//! A transaction's frames only become durable at `commit`, which appends a
//! Commit frame and fsyncs. Recovery replays Commit-marked transactions in
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
use std::path::{Path, PathBuf};

pub const KIND_BEGIN: u8 = 1;
pub const KIND_WRITE: u8 = 2;
pub const KIND_COMMIT: u8 = 3;
pub const KIND_ABORT: u8 = 4;

const HEADER: &[u8; 8] = b"DOCSWAL1";

#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("wal corrupt at lsn {0}: {1}")]
    Corrupt(u64, &'static str),
}

pub type Result<T> = std::result::Result<T, WalError>;

fn crc32(data: &[u8]) -> u32 {
    // Standard CRC-32 (IEEE 802.3, reflected), table computed on the fly
    // would be slow; use a small constant-time loop over 8 bits per byte.
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in data {
        crc ^= b as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
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
    /// Highest committed LSN (0 = nothing committed).
    pub durable_lsn: u64,
}

impl Wal {
    pub fn open(path: &Path) -> Result<Wal> {
        let exists = path.try_exists().map_err(WalError::Io)?;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false) // never clobber an existing log
            .open(path)?;
        if exists && file.metadata()?.len() > 0 {
            // Validate header and truncate any torn tail so appends start clean.
            let mut buf = Vec::new();
            file.seek(SeekFrom::Start(0))?;
            file.read_to_end(&mut buf)?;
            if buf.len() < HEADER.len() || &buf[..HEADER.len()] != HEADER {
                return Err(WalError::Corrupt(0, "bad header"));
            }
            let good_end = Self::scan_end(&buf);
            file.set_len(good_end as u64)?;
        }
        file.seek(SeekFrom::Start(0))?;
        if file.metadata()?.len() == 0 {
            file.write_all(HEADER)?;
            file.sync_all()?;
        }
        // Compute durable_lsn and next_lsn from the clean prefix.
        let mut buf = Vec::new();
        file.seek(SeekFrom::Start(0))?;
        file.read_to_end(&mut buf)?;
        let (durable, next) = Self::scan(&buf);
        file.seek(SeekFrom::End(0))?;
        Ok(Wal {
            file,
            path: path.to_path_buf(),
            next_lsn: next,
            durable_lsn: durable,
        })
    }

    /// Returns (durable_commit_lsn, next_lsn) over all valid frames.
    fn scan(buf: &[u8]) -> (u64, u64) {
        let mut pos = HEADER.len();
        let mut next = 1u64;
        let mut open: std::collections::HashSet<u64> = std::collections::HashSet::new();
        let mut durable = 0u64;
        while pos < buf.len() {
            let Some((rec, adv)) = parse_frame(&buf[pos..], next).unwrap_or(None) else {
                break;
            };
            match rec.kind {
                KIND_BEGIN => {
                    open.insert(rec.txid);
                }
                KIND_COMMIT if open.remove(&rec.txid) => {
                    durable = rec.lsn;
                }
                _ => {}
            }
            next = rec.lsn + 1;
            pos += adv;
        }
        (durable, next)
    }

    fn scan_end(buf: &[u8]) -> usize {
        let mut pos = HEADER.len();
        let mut next = 1u64;
        while pos < buf.len() {
            let Some((_rec, adv)) = parse_frame(&buf[pos..], next).unwrap_or(None) else {
                break;
            };
            next += 1;
            pos += adv;
        }
        pos
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn append(&mut self, kind: u8, txid: u64, payload: &[u8]) -> Result<u64> {
        let lsn = self.next_lsn;
        let mut frame = Vec::with_capacity(9 + 1 + 8 + 4 + payload.len() + 4);
        frame.extend_from_slice(&lsn.to_le_bytes());
        frame.push(kind);
        frame.extend_from_slice(&txid.to_le_bytes());
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(payload);
        let crc = crc32(&frame[8..]);
        frame.extend_from_slice(&crc.to_le_bytes());
        self.file.write_all(&frame)?;
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
    /// the transaction survives any crash.
    pub fn commit(&mut self, txid: u64) -> Result<u64> {
        let lsn = self.append(KIND_COMMIT, txid, &[])?;
        self.file.sync_data()?;
        self.durable_lsn = self.durable_lsn.max(lsn);
        Ok(lsn)
    }

    pub fn abort(&mut self, txid: u64) -> Result<u64> {
        self.append(KIND_ABORT, txid, &[])
    }

    /// Iterate all valid frames (recovery input), in LSN order.
    pub fn records(&self) -> Result<Vec<LogRecord>> {
        let mut buf = Vec::new();
        let mut f = &self.file;
        f.seek(SeekFrom::Start(0))?;
        f.read_to_end(&mut buf)?;
        let mut out = Vec::new();
        let mut pos = HEADER.len();
        let mut next = 1u64;
        while pos < buf.len() {
            let Some((rec, adv)) = parse_frame(&buf[pos..], next)? else {
                break;
            };
            next = rec.lsn + 1;
            pos += adv;
            out.push(rec);
        }
        Ok(out)
    }

    /// Checkpoint: after the data file is fully synced, drop the log.
    /// Safe because every committed change is now in the data file.
    pub fn checkpoint(&mut self) -> Result<()> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(HEADER)?;
        self.file.sync_data()?;
        self.durable_lsn = 0;
        Ok(())
    }
}

/// Parse one frame at the start of `buf`. `expect_lsn` validates continuity.
/// Returns None on a torn/corrupt tail (replay must stop there).
fn parse_frame(buf: &[u8], expect_lsn: u64) -> Result<Option<(LogRecord, usize)>> {
    if buf.is_empty() {
        return Ok(None);
    }
    const MIN: usize = 8 + 1 + 8 + 4 + 4;
    if buf.len() < MIN {
        return Ok(None);
    }
    let lsn = u64::from_le_bytes(buf[0..8].try_into().unwrap());
    if lsn != expect_lsn {
        return Err(WalError::Corrupt(expect_lsn, "lsn gap"));
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
    fn checksum_corruption_stops_replay() {
        let (_dir, path) = wal_dir();
        {
            let mut w = Wal::open(&path).unwrap();
            w.begin(1).unwrap();
            w.log_write(1, b"payload!").unwrap();
            w.commit(1).unwrap();
        }
        // Flip a payload byte in the WRITE frame.
        let mut data = std::fs::read(&path).unwrap();
        let idx = data.iter().position(|&b| b == b'!').unwrap();
        data[idx] ^= 0xff;
        std::fs::write(&path, data).unwrap();

        let w = Wal::open(&path).unwrap();
        // Only BEGIN survives; WRITE is corrupt, so COMMIT... also valid frame
        // but its txid never saw its write applied. durable_lsn only counts
        // full scan of valid frames; corruption truncates at WRITE frame.
        assert!(w.records().unwrap().len() < 3);
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
        // Can keep writing after checkpoint (LSNs keep counting up).
        w.begin(2).unwrap();
        w.commit(2).unwrap();
        assert_eq!(w.durable_lsn, 4);
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
}
