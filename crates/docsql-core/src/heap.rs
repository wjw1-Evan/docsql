//! Document heap: page-level storage for a table's documents.
//!
//! Page layout (little-endian, PAGE_SIZE bytes):
//! ```text
//! u16 count | (u16 offset, u16 len) * count | ...free gap... | documents packed from the page end
//! ```
//! Documents are appended to the last non-full page; new pages come from the
//! pager. Deletion compacts in place within a page (M4); updates rewrite the
//! doc if it still fits.

use crate::encode;
use crate::pager::{Pager, PagerError, Tx, PAGE_SIZE};
use crate::value::{Object, Value};

const SLOT_SIZE: usize = 4;
const HEADER_FIXED: usize = 2;

#[derive(Debug, thiserror::Error)]
pub enum HeapError {
    #[error("page {0}: {1}")]
    Page(u32, &'static str),
    #[error("encode error: {0}")]
    Encode(#[from] encode::EncodeError),
    #[error("storage error: {0}")]
    Pager(#[from] PagerError),
    #[error("document too large ({0} bytes, max {1})")]
    DocTooLarge(usize, usize),
}

pub type Result<T> = std::result::Result<T, HeapError>;

fn count_of(page: &[u8]) -> usize {
    u16::from_le_bytes([page[0], page[1]]) as usize
}

fn slot(page: &[u8], i: usize) -> (usize, usize) {
    let b: [u8; 4] = page[HEADER_FIXED + i * SLOT_SIZE..HEADER_FIXED + i * SLOT_SIZE + 4]
        .try_into()
        .unwrap();
    let off = u16::from_le_bytes([b[0], b[1]]) as usize;
    let len = u16::from_le_bytes([b[2], b[3]]) as usize;
    (off, len)
}

fn set_count(page: &mut [u8], n: usize) {
    page[0..2].copy_from_slice(&(n as u16).to_le_bytes());
}

fn push_slot(page: &mut [u8], off: usize, len: usize) {
    let i = count_of(page);
    let at = HEADER_FIXED + i * SLOT_SIZE;
    page[at..at + 2].copy_from_slice(&(off as u16).to_le_bytes());
    page[at + 2..at + 4].copy_from_slice(&(len as u16).to_le_bytes());
    set_count(page, i + 1);
}

/// Free space = gap between the slot directory (front) and the packed
/// document content (back, grows toward the front of the page).
fn free_space(page: &[u8]) -> usize {
    let n = count_of(page);
    let content_start = content_start(page);
    content_start - (HEADER_FIXED + n * SLOT_SIZE)
}

/// Lowest document offset (documents pack from the page end toward the front).
fn content_start(page: &[u8]) -> usize {
    let n = count_of(page);
    if n == 0 {
        PAGE_SIZE
    } else {
        let mut min = PAGE_SIZE;
        for i in 0..n {
            let (off, _) = slot(page, i);
            min = min.min(off);
        }
        min
    }
}

/// A document slot location: (page_id, slot_index).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DocId {
    pub page: u32,
    pub slot: usize,
}

#[derive(Default)]
pub struct Heap {
    /// Pages belonging to this table, in insertion order.
    pub pages: Vec<u32>,
}

impl Heap {
    /// Read every live document in insertion order.
    pub fn scan(&self, pager: &mut Pager) -> Result<Vec<Object>> {
        let mut out = Vec::new();
        for &pid in &self.pages {
            let page = pager.read_page(pid)?.to_vec();
            for i in 0..count_of(&page) {
                let (off, len) = slot(&page, i);
                let (v, _) = encode::decode_prefix(&page[off..off + len])?;
                if let Value::Object(o) = v {
                    out.push(o);
                }
            }
        }
        Ok(out)
    }

    /// Append a document; extends the heap with a new page when needed.
    pub fn insert(&mut self, pager: &mut Pager, tx: &mut Tx, doc: &Object) -> Result<()> {
        let bytes = encode::encode_to_vec(&Value::Object(doc.clone()))?;
        if bytes.len() + SLOT_SIZE > PAGE_SIZE - HEADER_FIXED {
            return Err(HeapError::DocTooLarge(
                bytes.len(),
                PAGE_SIZE - HEADER_FIXED - SLOT_SIZE,
            ));
        }
        // Try the last page first (prefer this tx's staged image — the
        // page may not be on disk yet).
        let mut placed = false;
        if let Some(&last) = self.pages.last() {
            let mut page = match tx.staged_page(last) {
                Some(p) => p.to_vec(),
                None => pager.read_page(last)?.to_vec(),
            };
            if free_space(&page) >= bytes.len() + SLOT_SIZE {
                let off = content_start(&page) - bytes.len();
                page[off..off + bytes.len()].copy_from_slice(&bytes);
                push_slot(&mut page, off, bytes.len());
                pager.write_page(tx, last, 0, &page)?;
                placed = true;
            }
        }
        if !placed {
            let pid = pager.allocate_page(tx)?;
            let mut page = vec![0u8; PAGE_SIZE];
            let off = PAGE_SIZE - bytes.len();
            page[off..].copy_from_slice(&bytes);
            push_slot(&mut page, off, bytes.len());
            pager.write_page(tx, pid, 0, &page)?;
            self.pages.push(pid);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn db(name: &str) -> (tempfile::TempDir, Pager) {
        let dir = tempfile::tempdir().unwrap();
        let pager = Pager::open(&dir.path().join(name)).unwrap();
        (dir, pager)
    }

    fn doc(id: i64, text: &str) -> Object {
        let padded = format!("{text}{}", "p".repeat(90));
        Object::from([
            ("_id".into(), Value::Int(id)),
            ("text".into(), Value::Str(padded)),
        ])
    }

    #[test]
    fn insert_and_scan_roundtrip() {
        let (_d, mut pager) = db("heap1.db");
        let mut heap = Heap::default();
        for i in 0..50 {
            let mut tx = pager.begin_tx();
            heap.insert(&mut pager, &mut tx, &doc(i, &format!("row-{i}")))
                .unwrap();
            pager.commit_tx(tx).unwrap();
        }
        let docs = heap.scan(&mut pager).unwrap();
        assert_eq!(docs.len(), 50);
        assert_eq!(docs[0].get("_id").unwrap(), &Value::Int(0));
        assert!(docs[0]
            .get("text")
            .unwrap()
            .as_str()
            .unwrap()
            .starts_with("row-0"));
        assert_eq!(docs[49].get("_id").unwrap(), &Value::Int(49));
        // Many docs spread over multiple pages.
        assert!(heap.pages.len() > 1, "expected multi-page heap");
    }

    #[test]
    fn survives_reopen() {
        let (d, mut pager) = db("heap2.db");
        let mut heap = Heap::default();
        for i in 0..10 {
            let mut tx = pager.begin_tx();
            heap.insert(&mut pager, &mut tx, &doc(i, "persist me"))
                .unwrap();
            pager.commit_tx(tx).unwrap();
        }
        drop(pager);
        let mut pager = Pager::open(&d.path().join("heap2.db")).unwrap();
        let docs = heap.scan(&mut pager).unwrap();
        assert_eq!(docs.len(), 10);
    }

    #[test]
    fn oversize_doc_rejected() {
        let (_d, mut pager) = db("heap3.db");
        let mut heap = Heap::default();
        let big = Object::from([("blob".into(), Value::Str("x".repeat(9000)))]);
        let mut tx = pager.begin_tx();
        assert!(heap.insert(&mut pager, &mut tx, &big).is_err());
    }
}
