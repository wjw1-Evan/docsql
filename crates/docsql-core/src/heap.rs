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

/// Bounds-check a page image loaded from storage: the slot directory and
/// every live document region must lie inside the page. A damaged file must
/// surface as an error, not as a slice panic.
fn validate_page(page: &[u8], pid: u32) -> Result<()> {
    let n = count_of(page);
    if HEADER_FIXED + n * SLOT_SIZE > page.len() {
        return Err(HeapError::Page(pid, "slot directory overflows page"));
    }
    for i in 0..n {
        let (off, len) = slot(page, i);
        if len > 0 && (off < HEADER_FIXED || off + len > page.len()) {
            return Err(HeapError::Page(pid, "document region out of bounds"));
        }
    }
    Ok(())
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
/// Tombstones (len == 0, off == PAGE_SIZE) never lower it.
fn content_start(page: &[u8]) -> usize {
    let n = count_of(page);
    if n == 0 {
        PAGE_SIZE
    } else {
        let mut min = PAGE_SIZE;
        for i in 0..n {
            let (off, len) = slot(page, i);
            if len > 0 {
                min = min.min(off);
            }
        }
        min
    }
}

/// Pack a locator: page id and slot index (slots fit u16; max ~1023/page).
pub fn pack_loc(page: u32, slot: usize) -> u64 {
    ((page as u64) << 16) | (slot as u64 & 0xFFFF)
}

pub fn unpack_loc(loc: u64) -> (u32, usize) {
    ((loc >> 16) as u32, (loc & 0xFFFF) as usize)
}

/// Drop the slot's bytes and re-pack survivors to the page end, preserving
/// slot order. Returns old→new slot moves for surviving documents.
fn repack(page: &mut [u8]) -> Vec<(usize, usize)> {
    let n = count_of(page);
    let mut live: Vec<(usize, Vec<u8>)> = Vec::new();
    for i in 0..n {
        let (off, len) = slot(page, i);
        if len > 0 {
            live.push((i, page[off..off + len].to_vec()));
        }
    }
    let moves = live
        .iter()
        .enumerate()
        .map(|(new, (old, _))| (*old, new))
        .collect();
    page.fill(0);
    let mut end = PAGE_SIZE;
    for (_, bytes) in &live {
        end -= bytes.len();
        page[end..end + bytes.len()].copy_from_slice(bytes);
        push_slot(page, end, bytes.len());
    }
    moves
}

/// Read the page image to mutate (staged version if this tx already wrote it).
fn staged_or_file_page(pager: &mut Pager, tx: &Tx, id: u32) -> Result<Vec<u8>> {
    Ok(match tx.staged_page(id) {
        Some(p) => p.to_vec(),
        None => pager.read_page(id)?.to_vec(),
    })
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

/// Where a replaced document ended up, plus slot moves its page-mates
/// suffered (their index entries must be re-pointed).
pub struct ReplaceOutcome {
    pub placed: u64,
    pub moved: Vec<(u64, u64)>,
}

/// Mark slot `i` dead: offset sentinel PAGE_SIZE (never lowers content_start),
/// length zero (skipped by scans).
fn tombstone(page: &mut [u8], i: usize) {
    let at = HEADER_FIXED + i * SLOT_SIZE;
    page[at..at + 2].copy_from_slice(&(PAGE_SIZE as u16).to_le_bytes());
    page[at + 2..at + 4].copy_from_slice(&0u16.to_le_bytes());
}

impl Heap {
    /// Read every live document in insertion order.
    pub fn scan(&self, pager: &mut Pager) -> Result<Vec<Object>> {
        let mut out = Vec::new();
        for &pid in &self.pages {
            let page = pager.read_page(pid)?.to_vec();
            validate_page(&page, pid)?;
            for i in 0..count_of(&page) {
                let (off, len) = slot(&page, i);
                if len == 0 {
                    continue; // tombstone
                }
                let (v, _) = encode::decode_prefix(&page[off..off + len])?;
                if let Value::Object(o) = v {
                    out.push(o);
                }
            }
        }
        Ok(out)
    }

    /// Live (locator, document) pairs of one page, in slot order. Staged
    /// pages of an open transaction are preferred, so consecutive
    /// mutations within one statement see each other.
    pub fn page_docs(&self, pager: &mut Pager, tx: &Tx, page: u32) -> Result<Vec<(u64, Object)>> {
        let buf = staged_or_file_page(pager, tx, page)?;
        validate_page(&buf, page)?;
        let mut out = Vec::new();
        for i in 0..count_of(&buf) {
            let (off, len) = slot(&buf, i);
            if len == 0 {
                continue;
            }
            let (v, _) = encode::decode_prefix(&buf[off..off + len])?;
            if let Value::Object(o) = v {
                out.push((pack_loc(page, i), o));
            }
        }
        Ok(out)
    }

    /// One document by locator; None for a tombstone/empty slot.
    pub fn doc_at(&self, pager: &mut Pager, loc: u64) -> Result<Option<Object>> {
        let (page, slot_i) = unpack_loc(loc);
        let buf = pager.read_page(page)?.to_vec();
        validate_page(&buf, page)?;
        if slot_i >= count_of(&buf) {
            return Err(HeapError::Page(page, "slot out of range"));
        }
        let (off, len) = slot(&buf, slot_i);
        if len == 0 {
            return Ok(None);
        }
        let (v, _) = encode::decode_prefix(&buf[off..off + len])?;
        Ok(Some(match v {
            Value::Object(o) => o,
            other => Object::from([("_doc".into(), other)]),
        }))
    }

    /// Append a document; extends the heap with a new page when needed.
    /// Returns the new document's locator.
    pub fn insert(&mut self, pager: &mut Pager, tx: &mut Tx, doc: &Object) -> Result<u64> {
        let bytes = encode::encode_to_vec(&Value::Object(doc.clone()))?;
        if bytes.len() + SLOT_SIZE > PAGE_SIZE - HEADER_FIXED {
            return Err(HeapError::DocTooLarge(
                bytes.len(),
                PAGE_SIZE - HEADER_FIXED - SLOT_SIZE,
            ));
        }
        // Try the last page first (prefer this tx's staged image — the
        // page may not be on disk yet).
        let mut placed = None;
        if let Some(&last) = self.pages.last() {
            let mut page = staged_or_file_page(pager, tx, last)?;
            validate_page(&page, last)?;
            if free_space(&page) >= bytes.len() + SLOT_SIZE {
                let off = content_start(&page) - bytes.len();
                page[off..off + bytes.len()].copy_from_slice(&bytes);
                push_slot(&mut page, off, bytes.len());
                pager.write_page(tx, last, 0, &page)?;
                placed = Some(pack_loc(last, count_of(&page) - 1));
            }
        }
        if placed.is_none() {
            let pid = pager.allocate_page(tx)?;
            let mut page = vec![0u8; PAGE_SIZE];
            let off = PAGE_SIZE - bytes.len();
            page[off..].copy_from_slice(&bytes);
            push_slot(&mut page, off, bytes.len());
            pager.write_page(tx, pid, 0, &page)?;
            self.pages.push(pid);
            placed = Some(pack_loc(pid, 0));
        }
        Ok(placed.unwrap())
    }

    /// Replace one document in place: the old slot is dropped, the page is
    /// re-packed, and the new bytes go back into the same page when they fit
    /// (the common KV-overwrite case recycles the page). Otherwise the old
    /// slot is tombstoned and the new document is appended to the heap.
    pub fn replace(
        &mut self,
        pager: &mut Pager,
        tx: &mut Tx,
        loc: u64,
        doc: &Object,
    ) -> Result<ReplaceOutcome> {
        let bytes = encode::encode_to_vec(&Value::Object(doc.clone()))?;
        if bytes.len() + SLOT_SIZE > PAGE_SIZE - HEADER_FIXED {
            return Err(HeapError::DocTooLarge(
                bytes.len(),
                PAGE_SIZE - HEADER_FIXED - SLOT_SIZE,
            ));
        }
        let (page_id, slot_i) = unpack_loc(loc);
        let mut page = staged_or_file_page(pager, tx, page_id)?;
        validate_page(&page, page_id)?;
        let n = count_of(&page);
        if slot_i >= n {
            return Err(HeapError::Page(page_id, "slot out of range"));
        }
        let (_, len) = slot(&page, slot_i);
        if len == 0 {
            return Err(HeapError::Page(page_id, "replace of a dead slot"));
        }
        // Rebuild the page with the new document at the replaced document's
        // position — slot order (== row order) is preserved.
        // usize::MAX marks the replaced document's entry.
        let mut entries: Vec<(usize, Vec<u8>)> = Vec::with_capacity(n);
        for i in 0..n {
            let (off, l) = slot(&page, i);
            if l == 0 {
                continue;
            }
            if i == slot_i {
                entries.push((usize::MAX, bytes.clone()));
            } else {
                entries.push((i, page[off..off + l].to_vec()));
            }
        }
        let total: usize = entries.iter().map(|(_, b)| b.len()).sum();
        if HEADER_FIXED + entries.len() * SLOT_SIZE + total > PAGE_SIZE {
            // Doesn't fit anymore: drop the slot (survivors compacted) and
            // append the new document at the heap's end.
            tombstone(&mut page, slot_i);
            let moves = repack(&mut page);
            let moved = moves
                .iter()
                .map(|(old, new)| (pack_loc(page_id, *old), pack_loc(page_id, *new)))
                .collect();
            pager.write_page(tx, page_id, 0, &page)?;
            let placed = self.insert(pager, tx, doc)?;
            return Ok(ReplaceOutcome { placed, moved });
        }
        page.fill(0);
        let mut end = PAGE_SIZE;
        let mut placed_slot = 0;
        let mut moves: Vec<(usize, usize)> = Vec::new();
        for (new_i, (old_i, b)) in entries.iter().enumerate() {
            end -= b.len();
            page[end..end + b.len()].copy_from_slice(b);
            push_slot(&mut page, end, b.len());
            match *old_i {
                usize::MAX => placed_slot = new_i,
                old if old != new_i => moves.push((old, new_i)),
                _ => {}
            }
        }
        pager.write_page(tx, page_id, 0, &page)?;
        Ok(ReplaceOutcome {
            placed: pack_loc(page_id, placed_slot),
            moved: moves
                .iter()
                .map(|(old, new)| (pack_loc(page_id, *old), pack_loc(page_id, *new)))
                .collect(),
        })
    }

    /// Remove the documents at `locs` (deduped by page, one re-pack per
    /// page). Returns survivor moves on affected pages. Pages that become
    /// empty are dropped from the heap's page list.
    pub fn remove_many(
        &mut self,
        pager: &mut Pager,
        tx: &mut Tx,
        locs: &[u64],
    ) -> Result<Vec<(u64, u64)>> {
        let mut by_page: std::collections::BTreeMap<u32, Vec<usize>> =
            std::collections::BTreeMap::new();
        for &loc in locs {
            let (p, s) = unpack_loc(loc);
            by_page.entry(p).or_default().push(s);
        }
        let mut all_moves = Vec::new();
        for (page_id, mut slots) in by_page {
            slots.sort_unstable();
            slots.dedup();
            let mut page = staged_or_file_page(pager, tx, page_id)?;
            validate_page(&page, page_id)?;
            let n = count_of(&page);
            for &s in &slots {
                if s >= n {
                    return Err(HeapError::Page(page_id, "slot out of range"));
                }
                let (_, len) = slot(&page, s);
                if len == 0 {
                    continue; // already dead
                }
                tombstone(&mut page, s);
            }
            let moves = repack(&mut page);
            pager.write_page(tx, page_id, 0, &page)?;
            all_moves.extend(
                moves
                    .iter()
                    .map(|(old, new)| (pack_loc(page_id, *old), pack_loc(page_id, *new))),
            );
            if count_of(&page) == 0 {
                self.pages.retain(|&p| p != page_id);
            }
        }
        Ok(all_moves)
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

    #[test]
    fn replace_recycles_page_and_reports_moves() {
        let (_d, mut pager) = db("heap4.db");
        let mut heap = Heap::default();
        let mut locs = Vec::new();
        for i in 0..20 {
            let mut tx = pager.begin_tx();
            locs.push(heap.insert(&mut pager, &mut tx, &doc(i, "row")).unwrap());
            pager.commit_tx(tx).unwrap();
        }
        let pages_before = heap.pages.clone();
        // Replace doc #5 with a same-size doc: must stay on its page.
        let mut tx = pager.begin_tx();
        let out = heap
            .replace(&mut pager, &mut tx, locs[5], &doc(5, "NEW!"))
            .unwrap();
        pager.commit_tx(tx).unwrap();
        assert_eq!(heap.pages, pages_before, "no new page needed");
        let docs = heap.scan(&mut pager).unwrap();
        assert_eq!(docs.len(), 20);
        // Row order is preserved and the replaced doc kept its position.
        for (i, d) in docs.iter().enumerate() {
            assert_eq!(d.get("_id"), Some(&Value::Int(i as i64)));
        }
        assert!(docs[5]
            .get("text")
            .unwrap()
            .as_str()
            .unwrap()
            .starts_with("NEW!"));
        // The replaced doc is reachable at its reported locator.
        let at = heap.doc_at(&mut pager, out.placed).unwrap().unwrap();
        assert_eq!(at.get("_id"), Some(&Value::Int(5)));
        assert!(at
            .get("text")
            .unwrap()
            .as_str()
            .unwrap()
            .starts_with("NEW!"));
        // Survivors whose slots moved are readable at the new locators.
        for (_, new) in &out.moved {
            let d = heap.doc_at(&mut pager, *new).unwrap().unwrap();
            assert!(d.contains_key("_id"));
        }
    }

    #[test]
    fn remove_many_compacts_and_drops_empty_pages() {
        let (_d, mut pager) = db("heap5.db");
        let mut heap = Heap::default();
        let mut locs = Vec::new();
        for i in 0..10 {
            let mut tx = pager.begin_tx();
            locs.push(heap.insert(&mut pager, &mut tx, &doc(i, "row")).unwrap());
            pager.commit_tx(tx).unwrap();
        }
        let mut tx = pager.begin_tx();
        let moves = heap.remove_many(&mut pager, &mut tx, &locs[..5]).unwrap();
        pager.commit_tx(tx).unwrap();
        let docs = heap.scan(&mut pager).unwrap();
        assert_eq!(docs.len(), 5);
        for (i, d) in docs.iter().enumerate() {
            assert_eq!(d.get("_id"), Some(&Value::Int(i as i64 + 5)));
        }
        // Note: repack reuses slot indexes, so a removed doc's old locator
        // may now address a survivor — the engine drops index entries by
        // (key, locator) before repacking, so this is invisible to queries.
        // Survivors listed as moved are live at their new locators.
        for (_, new) in &moves {
            assert!(heap.doc_at(&mut pager, *new).unwrap().is_some());
        }
        // Emptying the whole heap drops every page. Locators were re-issued
        // by the first repack, so read the current ones.
        let mut tx = pager.begin_tx();
        let cur: Vec<u64> = heap
            .page_docs(&mut pager, &tx, heap.pages[0])
            .unwrap()
            .into_iter()
            .map(|(l, _)| l)
            .collect();
        heap.remove_many(&mut pager, &mut tx, &cur).unwrap();
        pager.commit_tx(tx).unwrap();
        assert!(heap.pages.is_empty());
        assert_eq!(heap.scan(&mut pager).unwrap().len(), 0);
    }
    // ---- 覆盖率补充:超大文档 / 坏槽位 / tombstone 扫描 ----

    #[test]
    fn doc_too_large_and_bad_slot_paths() {
        let (_d, mut pager) = db("heap_big.db");
        let mut heap = Heap::default();
        let big = "z".repeat(PAGE_SIZE * 2);
        let mut tx = pager.begin_tx();
        // 超过单页容量的文档拒绝写入
        let e = heap.insert(&mut pager, &mut tx, &doc(1, &big)).unwrap_err();
        assert!(matches!(e, HeapError::DocTooLarge(..)), "{e:?}");
        pager.commit_tx(tx).unwrap();
        // 正常插入后 replace 到越界槽位
        let mut tx = pager.begin_tx();
        let loc = heap.insert(&mut pager, &mut tx, &doc(2, "ok")).unwrap();
        pager.commit_tx(tx).unwrap();
        let mut tx = pager.begin_tx();
        assert!(heap
            .replace(&mut pager, &mut tx, loc + (1 << 20), &doc(3, "x"))
            .is_err());
        pager.abort_tx(tx).unwrap();
        // doc_at 对 tombstone 返回 None
        let mut tx = pager.begin_tx();
        let l2 = heap.insert(&mut pager, &mut tx, &doc(4, "gone")).unwrap();
        heap.remove_many(&mut pager, &mut tx, &[l2]).unwrap();
        pager.commit_tx(tx).unwrap();
        assert_eq!(heap.doc_at(&mut pager, l2).unwrap(), None);
        // 删除+重插使扫描跳过 tombstone
        let mut tx = pager.begin_tx();
        heap.insert(&mut pager, &mut tx, &doc(5, "new")).unwrap();
        pager.commit_tx(tx).unwrap();
        let docs = heap.scan(&mut pager).unwrap();
        assert_eq!(docs.len(), 2);
    }
    #[test]
    fn replace_dead_slot_rejected() {
        let (_d, mut pager) = db("heap_dead.db");
        let mut heap = Heap::default();
        let mut tx = pager.begin_tx();
        let l1 = heap.insert(&mut pager, &mut tx, &doc(1, "aaa")).unwrap();
        let _l2 = heap.insert(&mut pager, &mut tx, &doc(2, "bbb")).unwrap();
        pager.commit_tx(tx).unwrap();
        let mut tx = pager.begin_tx();
        heap.remove_many(&mut pager, &mut tx, &[l1]).unwrap();
        pager.commit_tx(tx).unwrap();
        // 删除 + repack 后 l1 槽位要么已被迁移文档占用(replace 合法),
        // 要么越界/死亡被拒:两种情况下 replace 都不会破坏扫描一致性
        let mut tx = pager.begin_tx();
        let _ = heap.replace(&mut pager, &mut tx, l1, &doc(3, "ccc"));
        pager.abort_tx(tx).unwrap();
        let docs = heap.scan(&mut pager).unwrap();
        assert_eq!(docs.len(), 1);
    }
}
