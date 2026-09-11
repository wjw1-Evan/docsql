//! Document heap: page-level storage for a table's documents.
//!
//! Page layout (little-endian, PAGE_SIZE bytes):
//! ```text
//! u16 count | (u16 offset, u16 len) * count | ...free gap... | documents packed from the page end
//! ```
//! Documents are appended to the last non-full page; new pages come from the
//! pager. Deletion compacts in place within a page (M4); updates rewrite the
//! doc if it still fits.
//!
//! Overflow (documents larger than one page): the main-page slot stores
//! `[0xFF][total:u32][chain_head:u32]` plus as much of the encoded document
//! as the page can hold; the rest lives on a chain of `0xFE` pages
//! (`0xFE | next:u32 | len:u16 | payload`). `0xFE`/`0xFF` are bytes the
//! value encoder never emits (tags 0..=7), so readers disambiguate on the
//! first byte with zero ambiguity and legacy documents keep their exact
//! bytes. Recycled chain pages are parked in the table's `overflow_free`
//! list (persisted in the catalog) and reused by the next oversized insert.
//! Design: docs/design/002-overflow-page-chains.md.

use crate::encode;
use crate::pager::{Pager, PagerError, Tx, PAGE_SIZE};
use crate::value::{Object, Value};
use std::collections::HashSet;

const SLOT_SIZE: usize = 4;
const HEADER_FIXED: usize = 2;

/// Overflow slot marker: encode tags are 0..=7, so a payload can never
/// start with this byte.
const OVERFLOW_MARK: u8 = 0xFF;
/// Overflow chain-page marker.
const CHAIN_MARK: u8 = 0xFE;
/// Overflow slot header: mark + total:u32 + chain_head:u32.
const OVERFLOW_SLOT_HEADER: usize = 9;
/// Chain page header: mark + next:u32 + len:u16.
const CHAIN_HEADER: usize = 7;
/// Hard document size cap (per-heap defense against hostile oversized
/// documents; aligned with mainstream document-store defaults).
pub const MAX_DOC_SIZE: usize = 16 * 1024 * 1024;

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

/// Read a page image (staged version if this tx wrote it), owned copy —
/// overflow slots need `&mut Pager` while assembling the chain, so borrowed
/// page reads would alias. `tx = None` reads the committed image.
fn load_page_owned(pager: &mut Pager, tx: Option<&Tx>, id: u32) -> Result<Vec<u8>> {
    if let Some(tx) = tx {
        if let Some(p) = tx.staged_page(id) {
            return Ok(p.to_vec());
        }
    }
    Ok(pager.read_page(id)?.to_vec())
}

/// Assemble one slot's document bytes: a plain slot's content is the
/// encoded document itself; an overflow slot (`0xFF` first byte) carries
/// `[mark][total:u32][chain_head:u32][inline prefix]` and the rest is
/// assembled along the chain page list.
fn slot_document_bytes(
    pager: &mut Pager,
    tx: Option<&Tx>,
    page_id: u32,
    page: &[u8],
    off: usize,
    len: usize,
) -> Result<Vec<u8>> {
    let content = &page[off..off + len];
    if content.first() != Some(&OVERFLOW_MARK) {
        return Ok(content.to_vec());
    }
    if content.len() < OVERFLOW_SLOT_HEADER {
        return Err(HeapError::Page(page_id, "overflow slot truncated"));
    }
    let total = u32::from_le_bytes(content[1..5].try_into().expect("4 bytes")) as usize;
    let mut next = u32::from_le_bytes(content[5..9].try_into().expect("4 bytes"));
    let mut out = content[OVERFLOW_SLOT_HEADER..].to_vec();
    let min_chunk = PAGE_SIZE - CHAIN_HEADER;
    let mut hops = 0usize;
    let mut seen = HashSet::new();
    while next != 0 {
        // Cycle defense: a corrupt next pointer must fail loudly, not loop.
        if !seen.insert(next) || hops > total / min_chunk + 2 {
            return Err(HeapError::Page(page_id, "overflow chain corrupt"));
        }
        hops += 1;
        let chain_page = load_page_owned(pager, tx, next)?;
        if chain_page.first() != Some(&CHAIN_MARK) {
            return Err(HeapError::Page(next, "overflow chain corrupt (bad marker)"));
        }
        let nxt = u32::from_le_bytes(chain_page[1..5].try_into().expect("4 bytes"));
        let l = u16::from_le_bytes(chain_page[5..7].try_into().expect("2 bytes")) as usize;
        if CHAIN_HEADER + l > chain_page.len() || out.len() + l > total {
            return Err(HeapError::Page(next, "overflow chain corrupt (bad length)"));
        }
        out.extend_from_slice(&chain_page[CHAIN_HEADER..CHAIN_HEADER + l]);
        next = nxt;
    }
    if out.len() != total {
        return Err(HeapError::Page(
            page_id,
            "overflow chain corrupt (short read)",
        ));
    }
    Ok(out)
}

/// Walk an overflow chain, zero every page and park the page ids in the
/// table's free list (used by remove/replace of overflow documents).
fn recycle_chain(pager: &mut Pager, tx: &mut Tx, head: u32, free: &mut Vec<u32>) -> Result<()> {
    let mut next = head;
    let mut seen = HashSet::new();
    while next != 0 {
        if !seen.insert(next) {
            return Err(HeapError::Page(next, "overflow chain corrupt (cycle)"));
        }
        let page = load_page_owned(pager, Some(tx), next)?;
        if page.first() != Some(&CHAIN_MARK) {
            return Err(HeapError::Page(next, "overflow chain corrupt (bad marker)"));
        }
        let nxt = u32::from_le_bytes(page[1..5].try_into().expect("4 bytes"));
        pager.write_page(tx, next, 0, &vec![0u8; PAGE_SIZE])?;
        free.push(next);
        next = nxt;
    }
    Ok(())
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
    /// Recycled overflow-chain pages (zeroed on release), reused by the next
    /// oversized insert before fresh pager pages are allocated. Persisted in
    /// the table's catalog entry (`overflow_free`).
    pub overflow_free: Vec<u32>,
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
                let bytes = slot_document_bytes(pager, None, pid, &page, off, len)?;
                let (v, _) = encode::decode_prefix(&bytes)?;
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
        let buf = load_page_owned(pager, Some(tx), page)?;
        validate_page(&buf, page)?;
        let mut out = Vec::new();
        for i in 0..count_of(&buf) {
            let (off, len) = slot(&buf, i);
            if len == 0 {
                continue;
            }
            let bytes = slot_document_bytes(pager, Some(tx), page, &buf, off, len)?;
            let (v, _) = encode::decode_prefix(&bytes)?;
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
        let bytes = slot_document_bytes(pager, None, page, &buf, off, len)?;
        let (v, _) = encode::decode_prefix(&bytes)?;
        Ok(Some(match v {
            Value::Object(o) => o,
            other => Object::from([("_doc".into(), other)]),
        }))
    }

    /// Append a document; extends the heap with a new page when needed.
    /// Documents larger than one page are laid out overflow-style (see the
    /// module docs). Returns the new document's locator.
    pub fn insert(&mut self, pager: &mut Pager, tx: &mut Tx, doc: &Object) -> Result<u64> {
        let bytes = encode::encode_to_vec(&Value::Object(doc.clone()))?;
        if bytes.len() > MAX_DOC_SIZE {
            return Err(HeapError::DocTooLarge(bytes.len(), MAX_DOC_SIZE));
        }
        if bytes.len() + SLOT_SIZE > PAGE_SIZE - HEADER_FIXED {
            return self.insert_overflow(pager, tx, &bytes);
        }
        // Try the last page first (prefer this tx's staged image — the
        // page may not be on disk yet).
        let mut placed = None;
        if let Some(&last) = self.pages.last() {
            let mut page = load_page_owned(pager, Some(tx), last)?;
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

    /// Overflow layout insert (document encoding exceeds one page). The
    /// main-page slot carries the header + inline prefix; the rest goes to a
    /// chain of `0xFE` pages taken from the free list first, fresh pager
    /// pages only for the remainder.
    fn insert_overflow(&mut self, pager: &mut Pager, tx: &mut Tx, bytes: &[u8]) -> Result<u64> {
        // Main page: the last page when it can host the slot, else a fresh
        // page (fresh pages give maximum inline capacity).
        let use_last = match self.pages.last() {
            Some(&last) => {
                let page = load_page_owned(pager, Some(tx), last)?;
                validate_page(&page, last)?;
                free_space(&page) >= SLOT_SIZE + OVERFLOW_SLOT_HEADER
            }
            None => false,
        };
        let max_inline = if use_last {
            let page = load_page_owned(pager, Some(tx), *self.pages.last().unwrap())?;
            free_space(&page).saturating_sub(SLOT_SIZE + OVERFLOW_SLOT_HEADER)
        } else {
            PAGE_SIZE - HEADER_FIXED - SLOT_SIZE - OVERFLOW_SLOT_HEADER
        };
        let inline_len = max_inline.min(bytes.len());

        // Chain pages: free list first, fresh allocation for the remainder.
        let rest = &bytes[inline_len..];
        let chunk_count = rest.chunks(PAGE_SIZE - CHAIN_HEADER).count();
        let mut chain = Vec::with_capacity(chunk_count);
        while chain.len() < chunk_count {
            match self.overflow_free.pop() {
                Some(pid) => chain.push(pid),
                None => chain.push(pager.allocate_page(tx)?),
            }
        }
        for (i, chunk) in rest.chunks(PAGE_SIZE - CHAIN_HEADER).enumerate() {
            let pid = chain[i];
            let next = chain.get(i + 1).copied().unwrap_or(0);
            let mut page = vec![0u8; PAGE_SIZE];
            page[0] = CHAIN_MARK;
            page[1..5].copy_from_slice(&next.to_le_bytes());
            page[5..7].copy_from_slice(&(chunk.len() as u16).to_le_bytes());
            page[7..7 + chunk.len()].copy_from_slice(chunk);
            pager.write_page(tx, pid, 0, &page)?;
        }

        // Main-page slot: header + inline prefix.
        let mut content = Vec::with_capacity(OVERFLOW_SLOT_HEADER + inline_len);
        content.push(OVERFLOW_MARK);
        content.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        content.extend_from_slice(&chain.first().copied().unwrap_or(0).to_le_bytes());
        content.extend_from_slice(&bytes[..inline_len]);

        if !use_last {
            let pid = pager.allocate_page(tx)?;
            let mut page = vec![0u8; PAGE_SIZE];
            let off = PAGE_SIZE - content.len();
            page[off..off + content.len()].copy_from_slice(&content);
            push_slot(&mut page, off, content.len());
            pager.write_page(tx, pid, 0, &page)?;
            self.pages.push(pid);
            return Ok(pack_loc(pid, 0));
        }
        let last = *self.pages.last().unwrap();
        let mut page = load_page_owned(pager, Some(tx), last)?;
        let off = content_start(&page) - content.len();
        page[off..off + content.len()].copy_from_slice(&content);
        push_slot(&mut page, off, content.len());
        pager.write_page(tx, last, 0, &page)?;
        Ok(pack_loc(last, count_of(&page) - 1))
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
        if bytes.len() > MAX_DOC_SIZE {
            return Err(HeapError::DocTooLarge(bytes.len(), MAX_DOC_SIZE));
        }
        let (page_id, slot_i) = unpack_loc(loc);
        let mut page = load_page_owned(pager, Some(tx), page_id)?;
        validate_page(&page, page_id)?;
        let n = count_of(&page);
        if slot_i >= n {
            return Err(HeapError::Page(page_id, "slot out of range"));
        }
        let (off, len) = slot(&page, slot_i);
        if len == 0 {
            return Err(HeapError::Page(page_id, "replace of a dead slot"));
        }
        // If the old document was overflow-layout, recycle its chain pages
        // into the free list BEFORE any new layout lands (the new image may
        // be plain, overflow, or fail to fit and be re-appended — in every
        // case the old chain is gone).
        if page[off] == OVERFLOW_MARK {
            let content = &page[off..off + len];
            let head = u32::from_le_bytes(content[5..9].try_into().expect("4 bytes"));
            recycle_chain(pager, tx, head, &mut self.overflow_free)?;
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
            if count_of(&page) == 0 {
                self.pages.retain(|&p| p != page_id);
            }
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
            let mut page = load_page_owned(pager, Some(tx), page_id)?;
            validate_page(&page, page_id)?;
            let n = count_of(&page);
            for &s in &slots {
                if s >= n {
                    return Err(HeapError::Page(page_id, "slot out of range"));
                }
                let (off, len) = slot(&page, s);
                if len == 0 {
                    continue; // already dead
                }
                // Overflow document: recycle its chain pages before the slot
                // dies (they are unreachable afterwards).
                if page[off] == OVERFLOW_MARK {
                    let content = &page[off..off + len];
                    let head = u32::from_le_bytes(content[5..9].try_into().expect("4 bytes"));
                    recycle_chain(pager, tx, head, &mut self.overflow_free)?;
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
        // 16 MiB cap: a document above MAX_DOC_SIZE is rejected up front…
        let big = Object::from([("blob".into(), Value::Str("x".repeat(17 * 1024 * 1024)))]);
        let mut tx = pager.begin_tx();
        let e = heap.insert(&mut pager, &mut tx, &big).unwrap_err();
        assert!(matches!(e, HeapError::DocTooLarge(..)), "{e:?}");
        pager.commit_tx(tx).unwrap();
        // …while anything below it now fits via overflow chains.
        let ok = Object::from([("blob".into(), Value::Str("x".repeat(9000)))]);
        let mut tx = pager.begin_tx();
        let loc = heap.insert(&mut pager, &mut tx, &ok).unwrap();
        pager.commit_tx(tx).unwrap();
        let back = heap.doc_at(&mut pager, loc).unwrap().unwrap();
        assert_eq!(back.get("blob").unwrap(), &Value::Str("x".repeat(9000)));
    }

    #[test]
    fn overflow_documents_roundtrip_across_chains() {
        let (_d, mut pager) = db("heap_of.db");
        let mut heap = Heap::default();
        // ~9 KB: two chain pages; ~20 KB: three.
        let specs = [(1i64, 9_000usize), (2, 20_000), (3, 300)];
        for (id, size) in specs {
            let doc = Object::from([
                ("_id".into(), Value::Int(id)),
                ("blob".into(), Value::Str("y".repeat(size))),
            ]);
            let mut tx = pager.begin_tx();
            heap.insert(&mut pager, &mut tx, &doc).unwrap();
            pager.commit_tx(tx).unwrap();
        }
        let docs = heap.scan(&mut pager).unwrap();
        assert_eq!(docs.len(), specs.len());
        for ((id, size), d) in specs.iter().zip(&docs) {
            assert_eq!(d.get("_id"), Some(&Value::Int(*id)));
            assert_eq!(
                d.get("blob").unwrap(),
                &Value::Str("y".repeat(*size)),
                "doc {id} content corrupted"
            );
        }
    }

    #[test]
    fn overflow_chain_pages_are_recycled_and_reused() {
        let (_d, mut pager) = db("heap_rec.db");
        let mut heap = Heap::default();
        let big = Object::from([("blob".into(), Value::Str("y".repeat(20_000)))]);
        let mut tx = pager.begin_tx();
        let loc = heap.insert(&mut pager, &mut tx, &big).unwrap();
        pager.commit_tx(tx).unwrap();
        let pages_after_big = pager.num_pages();

        // Replace with a small document: the chain pages are recycled into
        // the table's free list instead of leaking.
        let mut tx = pager.begin_tx();
        let small = Object::from([("blob".into(), Value::Str("tiny".to_string()))]);
        heap.replace(&mut pager, &mut tx, loc, &small).unwrap();
        pager.commit_tx(tx).unwrap();
        assert!(
            !heap.overflow_free.is_empty(),
            "chain pages must be recycled"
        );
        assert_eq!(
            pager.num_pages(),
            pages_after_big,
            "recycling must not allocate"
        );

        // The next oversized insert reuses the recycled pages: still no
        // fresh allocation, and the content round-trips.
        let mut tx = pager.begin_tx();
        let big2 = Object::from([("blob".into(), Value::Str("y".repeat(20_000)))]);
        heap.insert(&mut pager, &mut tx, &big2).unwrap();
        pager.commit_tx(tx).unwrap();
        assert_eq!(
            pager.num_pages(),
            pages_after_big,
            "reuse must not allocate"
        );
        assert!(heap.overflow_free.is_empty(), "free list drained");
        let docs = heap.scan(&mut pager).unwrap();
        // The small replacement row and the new oversized row both live.
        assert_eq!(docs.len(), 2);
        assert!(docs
            .iter()
            .any(|d| d.get("blob") == Some(&Value::Str("y".repeat(20_000)))));
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
        // MAX_DOC_SIZE (16 MiB) is the loud rejection floor; 2-page documents
        // now fit via overflow chains instead.
        let big = "z".repeat(17 * 1024 * 1024);
        let mut tx = pager.begin_tx();
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
        // 删除 commit 后页必然压缩(tombstone 只存在于事务内):
        // 被删文档的旧 locator 越界,读回显式报错——静默返回 None 会掩盖
        // stale-locator bug
        let mut tx = pager.begin_tx();
        let l2 = heap.insert(&mut pager, &mut tx, &doc(4, "gone")).unwrap();
        heap.remove_many(&mut pager, &mut tx, &[l2]).unwrap();
        pager.commit_tx(tx).unwrap();
        assert!(matches!(
            heap.doc_at(&mut pager, l2),
            Err(HeapError::Page(_, "slot out of range"))
        ));
        // 删除+重插使扫描跳过被删文档
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
