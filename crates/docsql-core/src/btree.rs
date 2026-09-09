//! Page-backed B+ tree index.
//!
//! Keys are document `Value`s ordered by [Value::cmp_values]; payloads are
//! u64 record locators. Nodes live in pager pages:
//!
//! ```text
//! byte 0: 1 = leaf, 2 = internal
//! u16 count
//! leaf:     (u16 klen, key bytes, u64 val) * count
//! internal: u32 leftmost_child, then (u16 klen, key bytes, u32 child) * count
//! ```
//!
//! Design note (v0): deletions remove entries from leaves but underfull
//! internal nodes are tolerated — search stays correct, balance degrades
//! only in pathological delete-heavy workloads. Splits keep the tree usable
//! for lookups and ordered scans; compaction can rebuild it later.

use crate::encode;
use crate::pager::{Pager, PagerError, Tx, PAGE_SIZE};
use crate::value::Value;
use std::cmp::Ordering;

const LEAF: u8 = 1;
const INTERNAL: u8 = 2;

/// A cell that cannot occupy at most half a page leaves no split point with
/// both halves inside a page, so such keys are rejected up front.
const HALF_PAGE: usize = PAGE_SIZE / 2;

/// Serialized node size — `header` is 3 (leaf) or 7 (internal), `payload`
/// the per-cell locator width (8 or 4).
fn node_bytes<T>(cells: &[(Value, T)], header: usize, payload: usize) -> Result<usize> {
    let mut n = header;
    for (k, _) in cells {
        n += 2 + encode::encode_to_vec(k)?.len() + payload;
    }
    Ok(n)
}

/// Choose a split index for an overflowing node whose new cell sits at
/// `at` (its insert position). Prefers `min(byte_boundary, at)`: splitting
/// at the insert position makes the right half start with the new cell,
/// which keeps equal-key entries ordered by insertion across leaves — a
/// split that strands old equal-key cells in a far-right leaf interleaves
/// old and new locators within one key run. When the suffix from `at`
/// would not fit a page the index walks up toward the byte boundary,
/// which always fits (every cell is capped at half a page).
fn split_at_for_insert<T>(
    cells: &[(Value, T)],
    at: usize,
    header: usize,
    payload: usize,
) -> Result<usize> {
    let mut prefix = Vec::with_capacity(cells.len() + 1);
    prefix.push(header);
    for (k, _) in cells {
        let last = *prefix.last().unwrap();
        prefix.push(last + 2 + encode::encode_to_vec(k)?.len() + payload);
    }
    let total = *prefix.last().unwrap();
    debug_assert!(total > PAGE_SIZE, "only overflowing nodes split");
    // Largest prefix that still fits a page (>= 1 cell: cells are capped).
    let boundary = prefix
        .iter()
        .position(|&p| p > PAGE_SIZE)
        .map(|i| i.max(1))
        .unwrap_or(cells.len() - 1);
    let mut m = boundary.min(at).max(1);
    while m < boundary && total - prefix[m] > PAGE_SIZE {
        m += 1;
    }
    Ok(m.min(cells.len() - 1))
}

/// Route `key` to a child page: go right at the first separator >= key
/// (matches split behavior, where the separator is the right leaf's first
/// key, so equal keys always live in the right subtree).
fn descend(cells: &[(Value, u32)], leftmost: u32, key: &Value) -> u32 {
    // Child ranges with equal-to-separator keys going right:
    // leftmost = (-inf, sep0), cells[i].1 = [sep_i, sep_{i+1}), last = [sep_n, inf).
    let idx = cells.partition_point(|(k, _)| Value::cmp_values(k, key) != Ordering::Greater);
    if idx == 0 {
        leftmost
    } else {
        cells[idx - 1].1
    }
}

/// Children that may contain `key`, descended (primary) child first.
/// Nominal ranges say exactly one child brackets `key`, but a split can
/// leave keys EQUAL to a separator in the child left of it (see
/// range_from_rec), so every child whose nominal range brackets `key` is
/// a candidate — lookup and delete must not miss the left siblings.
fn candidate_children(cells: &[(Value, u32)], leftmost: u32, key: &Value) -> Vec<u32> {
    let primary = descend(cells, leftmost, key);
    let mut out = vec![primary];
    let leftmost_upper_ge = match cells.first() {
        Some((k, _)) => Value::cmp_values(k, key) != Ordering::Less,
        None => true,
    };
    if leftmost_upper_ge && leftmost != primary {
        out.push(leftmost);
    }
    for (i, (_, child)) in cells.iter().enumerate() {
        if *child == primary {
            continue;
        }
        let lower_le = Value::cmp_values(&cells[i].0, key) != Ordering::Greater;
        let upper_ge = match cells.get(i + 1) {
            Some((k, _)) => Value::cmp_values(k, key) != Ordering::Less,
            None => true,
        };
        if lower_le && upper_ge {
            out.push(*child);
        }
    }
    out
}

#[derive(Debug, thiserror::Error)]
pub enum BTreeError {
    #[error("storage error: {0}")]
    Pager(#[from] PagerError),
    #[error("encode error: {0}")]
    Encode(#[from] encode::EncodeError),
    #[error("key too large for index ({0} bytes)")]
    KeyTooLarge(usize),
    #[error("duplicate key in unique index")]
    Duplicate,
    #[error("index corrupt: {0}")]
    Corrupt(&'static str),
}

pub type Result<T> = std::result::Result<T, BTreeError>;

enum Node {
    Leaf {
        cells: Vec<(Value, u64)>,
    },
    Internal {
        leftmost: u32,
        cells: Vec<(Value, u32)>,
    },
}

pub struct BTree {
    pub root: u32,
}

struct Ctx<'a> {
    pager: &'a mut Pager,
    tx: &'a mut Tx,
}

impl BTree {
    /// Create a fresh tree (allocates a root leaf inside `tx`).
    pub fn create(pager: &mut Pager, tx: &mut Tx) -> Result<BTree> {
        let next_page = pager.num_pages_now();
        let root = pager.allocate_page(tx)?;
        debug_assert_eq!(root, next_page);
        let mut page = vec![0u8; PAGE_SIZE];
        page[0] = LEAF;
        pager.write_page(tx, root, 0, &page)?;
        Ok(BTree { root })
    }

    pub fn open(root: u32) -> BTree {
        BTree { root }
    }

    fn read_node(pager: &mut Pager, tx: &Tx, id: u32) -> Result<Node> {
        // Borrow the page bytes in place (staged image or pool) — decoding
        // only reads them, and a full-page copy here would run on every
        // node visit of every tree operation.
        let page: &[u8] = match tx.staged_page(id) {
            Some(p) => p,
            None => pager.read_page(id)?,
        };
        // All offsets below come from the page bytes; a damaged file must
        // surface as Corrupt, not as a slice panic.
        fn take(page: &[u8], pos: usize, n: usize) -> Result<&[u8]> {
            page.get(pos..pos + n)
                .ok_or(BTreeError::Corrupt("truncated node cell"))
        }
        let kind = page[0];
        let count = u16::from_le_bytes([page[1], page[2]]) as usize;
        match kind {
            LEAF => {
                let mut cells = Vec::with_capacity(count.min(PAGE_SIZE / 10));
                let mut pos = 3;
                for _ in 0..count {
                    let klen = u16::from_le_bytes(take(page, pos, 2)?.try_into().unwrap()) as usize;
                    pos += 2;
                    let (k, used) = encode::decode_prefix(take(page, pos, klen)?)?;
                    pos += used;
                    let val = u64::from_le_bytes(take(page, pos, 8)?.try_into().unwrap());
                    pos += 8;
                    cells.push((k, val));
                }
                Ok(Node::Leaf { cells })
            }
            INTERNAL => {
                let leftmost = u32::from_le_bytes(page[3..7].try_into().unwrap());
                let mut pos = 7;
                let mut cells = Vec::with_capacity(count.min(PAGE_SIZE / 10));
                for _ in 0..count {
                    let klen = u16::from_le_bytes(take(page, pos, 2)?.try_into().unwrap()) as usize;
                    pos += 2;
                    let (k, used) = encode::decode_prefix(take(page, pos, klen)?)?;
                    pos += used;
                    let child = u32::from_le_bytes(take(page, pos, 4)?.try_into().unwrap());
                    pos += 4;
                    cells.push((k, child));
                }
                Ok(Node::Internal { leftmost, cells })
            }
            _ => Err(BTreeError::Corrupt("unknown node type")),
        }
    }

    fn write_node(pager: &mut Pager, tx: &mut Tx, id: u32, node: &Node) -> Result<()> {
        let mut page = vec![0u8; PAGE_SIZE];
        let mut pos = 3;
        match node {
            Node::Leaf { cells } => {
                page[0] = LEAF;
                page[1..3].copy_from_slice(&(cells.len() as u16).to_le_bytes());
                for (k, v) in cells {
                    let kb = encode::encode_to_vec(k)?;
                    let kl =
                        u16::try_from(kb.len()).map_err(|_| BTreeError::KeyTooLarge(kb.len()))?;
                    if pos + 2 + kb.len() + 8 > PAGE_SIZE {
                        return Err(BTreeError::KeyTooLarge(kb.len()));
                    }
                    page[pos..pos + 2].copy_from_slice(&kl.to_le_bytes());
                    pos += 2;
                    page[pos..pos + kb.len()].copy_from_slice(&kb);
                    pos += kb.len();
                    page[pos..pos + 8].copy_from_slice(&v.to_le_bytes());
                    pos += 8;
                }
            }
            Node::Internal { leftmost, cells } => {
                page[0] = INTERNAL;
                page[1..3].copy_from_slice(&(cells.len() as u16).to_le_bytes());
                page[3..7].copy_from_slice(&leftmost.to_le_bytes());
                pos = 7;
                for (k, child) in cells {
                    let kb = encode::encode_to_vec(k)?;
                    let kl =
                        u16::try_from(kb.len()).map_err(|_| BTreeError::KeyTooLarge(kb.len()))?;
                    if pos + 2 + kb.len() + 4 > PAGE_SIZE {
                        return Err(BTreeError::KeyTooLarge(kb.len()));
                    }
                    page[pos..pos + 2].copy_from_slice(&kl.to_le_bytes());
                    pos += 2;
                    page[pos..pos + kb.len()].copy_from_slice(&kb);
                    pos += kb.len();
                    page[pos..pos + 4].copy_from_slice(&child.to_le_bytes());
                    pos += 4;
                }
            }
        }
        pager.write_page(tx, id, 0, &page)?;
        Ok(())
    }

    /// Exact lookup.
    pub fn get(&self, pager: &mut Pager, tx: &Tx, key: &Value) -> Result<Option<u64>> {
        Self::get_at(pager, tx, self.root, key)
    }

    fn get_at(pager: &mut Pager, tx: &Tx, id: u32, key: &Value) -> Result<Option<u64>> {
        match Self::read_node(pager, tx, id)? {
            Node::Leaf { cells } => Ok(cells
                .iter()
                .find(|(k, _)| Value::cmp_values(k, key) == Ordering::Equal)
                .map(|(_, v)| *v)),
            Node::Internal { leftmost, cells } => {
                for child in candidate_children(&cells, leftmost, key) {
                    if let Some(v) = Self::get_at(pager, tx, child, key)? {
                        return Ok(Some(v));
                    }
                }
                Ok(None)
            }
        }
    }

    /// Insert; returns Err(Duplicate) if `unique` and key exists.
    pub fn insert(
        &mut self,
        pager: &mut Pager,
        tx: &mut Tx,
        key: Value,
        val: u64,
        unique: bool,
    ) -> Result<()> {
        let mut ctx = Ctx { pager, tx };
        if let Some((mid, right)) = Self::insert_rec(&mut ctx, self.root, &key, val, unique)? {
            // Grow a new root above the split pair.
            let old_root = self.root;
            let new_root = ctx.pager.allocate_page(ctx.tx)?;
            let node = Node::Internal {
                leftmost: old_root,
                cells: vec![(mid, right)],
            };
            Self::write_node(ctx.pager, ctx.tx, new_root, &node)?;
            self.root = new_root;
        }
        Ok(())
    }

    /// Returns Some((mid_key, new_right_page)) when the node split.
    fn insert_rec(
        ctx: &mut Ctx,
        id: u32,
        key: &Value,
        val: u64,
        unique: bool,
    ) -> Result<Option<(Value, u32)>> {
        match Self::read_node(ctx.pager, ctx.tx, id)? {
            Node::Leaf { mut cells } => {
                // Insert after all keys <= key (rightmost of an equal run) so
                // duplicate keys accumulate and routing (equal goes right)
                // stays consistent.
                let at =
                    cells.partition_point(|(k, _)| Value::cmp_values(k, key) != Ordering::Greater);
                if unique && at > 0 && Value::cmp_values(&cells[at - 1].0, key) == Ordering::Equal {
                    return Err(BTreeError::Duplicate);
                }
                let kb = encode::encode_to_vec(key)?;
                if 2 + kb.len() + 8 > HALF_PAGE {
                    return Err(BTreeError::KeyTooLarge(kb.len()));
                }
                cells.insert(at, (key.clone(), val));
                if node_bytes(&cells, 3, 8)? <= PAGE_SIZE {
                    Self::write_node(ctx.pager, ctx.tx, id, &Node::Leaf { cells })?;
                    Ok(None)
                } else {
                    let m = split_at_for_insert(&cells, at, 3, 8)?;
                    let mid_key = cells[m].0.clone();
                    let right_cells = cells.split_off(m);
                    let right = ctx.pager.allocate_page(ctx.tx)?;
                    Self::write_node(ctx.pager, ctx.tx, id, &Node::Leaf { cells })?;
                    Self::write_node(ctx.pager, ctx.tx, right, &Node::Leaf { cells: right_cells })?;
                    Ok(Some((mid_key, right)))
                }
            }
            Node::Internal {
                leftmost,
                mut cells,
            } => {
                let child = descend(&cells, leftmost, key);
                let mut to_insert = None;
                if let Some((mid, right)) = Self::insert_rec(ctx, child, key, val, unique)? {
                    to_insert = Some((mid, right));
                }
                if let Some((mid, right)) = to_insert {
                    // Insert after equal separators (duplicate keys can both
                    // split into parents); binary_search would panic on Ok.
                    let at = cells
                        .partition_point(|(k, _)| Value::cmp_values(k, &mid) != Ordering::Greater);
                    let mb = encode::encode_to_vec(&mid)?;
                    if 2 + mb.len() + 4 > HALF_PAGE {
                        return Err(BTreeError::KeyTooLarge(mb.len()));
                    }
                    cells.insert(at, (mid, right));
                    if node_bytes(&cells, 7, 4)? <= PAGE_SIZE {
                        Self::write_node(
                            ctx.pager,
                            ctx.tx,
                            id,
                            &Node::Internal { leftmost, cells },
                        )?;
                        Ok(None)
                    } else {
                        let m = split_at_for_insert(&cells, at, 7, 4)?;
                        let mid_key = cells[m].0.clone();
                        let right_cells = cells.split_off(m);
                        let new_leftmost = right_cells[0].1;
                        let right = ctx.pager.allocate_page(ctx.tx)?;
                        Self::write_node(
                            ctx.pager,
                            ctx.tx,
                            id,
                            &Node::Internal { leftmost, cells },
                        )?;
                        Self::write_node(
                            ctx.pager,
                            ctx.tx,
                            right,
                            &Node::Internal {
                                leftmost: new_leftmost,
                                cells: right_cells[1..].to_vec(),
                            },
                        )?;
                        Ok(Some((mid_key, right)))
                    }
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// In-order scan of all (key, val) pairs.
    pub fn scan(&self, pager: &mut Pager, tx: &Tx) -> Result<Vec<(Value, u64)>> {
        // Leaves are not chained in v0; walk the tree recursively.
        let mut out = Vec::new();
        Self::scan_rec(pager, tx, self.root, &mut out)?;
        Ok(out)
    }

    fn scan_rec(pager: &mut Pager, tx: &Tx, id: u32, out: &mut Vec<(Value, u64)>) -> Result<()> {
        match Self::read_node(pager, tx, id)? {
            Node::Leaf { cells } => out.extend(cells),
            Node::Internal { leftmost, cells } => {
                Self::scan_rec(pager, tx, leftmost, out)?;
                for (_, child) in cells {
                    Self::scan_rec(pager, tx, child, out)?;
                }
            }
        }
        Ok(())
    }

    /// Remove a key. Returns whether it was present.
    pub fn delete(&mut self, pager: &mut Pager, tx: &mut Tx, key: &Value) -> Result<bool> {
        Self::delete_rec(pager, tx, self.root, key)
    }

    /// Remove the exact `(key, locator)` pair. Needed for non-unique trees
    /// where several entries share a key: `delete` would drop an arbitrary
    /// one of them. Returns whether the pair was present.
    pub fn delete_entry(
        &mut self,
        pager: &mut Pager,
        tx: &mut Tx,
        key: &Value,
        loc: u64,
    ) -> Result<bool> {
        Self::delete_entry_rec(pager, tx, self.root, key, loc)
    }

    fn delete_entry_rec(
        pager: &mut Pager,
        tx: &mut Tx,
        id: u32,
        key: &Value,
        loc: u64,
    ) -> Result<bool> {
        match Self::read_node(pager, tx, id)? {
            Node::Leaf { mut cells } => {
                // binary_search lands on *an* equal key; equal keys may not be
                // contiguous after interleaved updates, so scan the whole leaf.
                for i in 0..cells.len() {
                    if Value::cmp_values(&cells[i].0, key) == Ordering::Equal && cells[i].1 == loc {
                        cells.remove(i);
                        Self::write_node(pager, tx, id, &Node::Leaf { cells })?;
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            Node::Internal { leftmost, cells } => {
                for child in candidate_children(&cells, leftmost, key) {
                    if Self::delete_entry_rec(pager, tx, child, key, loc)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
        }
    }

    /// All pairs with key >= `key`, in key order (range-scan entry point).
    /// Cheap: only the subtree that can hold `key..` is visited.
    pub fn range_from(&self, pager: &mut Pager, tx: &Tx, key: &Value) -> Result<Vec<(Value, u64)>> {
        let mut out = Vec::new();
        Self::range_from_rec(pager, tx, self.root, key, &mut out)?;
        Ok(out)
    }

    fn range_from_rec(
        pager: &mut Pager,
        tx: &Tx,
        id: u32,
        key: &Value,
        out: &mut Vec<(Value, u64)>,
    ) -> Result<()> {
        match Self::read_node(pager, tx, id)? {
            Node::Leaf { cells } => {
                out.extend(
                    cells
                        .into_iter()
                        .filter(|(k, _)| Value::cmp_values(k, key) != Ordering::Less),
                );
            }
            Node::Internal { leftmost, cells } => {
                // Child i nominally covers [sep_i, sep_{i+1}) — but a split
                // can leave keys EQUAL to a separator in the child left of
                // it, so any child whose upper separator is >= key may hold
                // matching entries; only upper < key is safely skippable.
                let leftmost_upper_ge = match cells.first() {
                    Some((k, _)) => Value::cmp_values(k, key) != Ordering::Less,
                    None => true,
                };
                if leftmost_upper_ge {
                    Self::range_from_rec(pager, tx, leftmost, key, out)?;
                }
                for (i, (_, child)) in cells.iter().enumerate() {
                    let upper_ge = match cells.get(i + 1) {
                        Some((k, _)) => Value::cmp_values(k, key) != Ordering::Less,
                        None => true,
                    };
                    if upper_ge {
                        Self::range_from_rec(pager, tx, *child, key, out)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Entries with key >= `lo`, optionally stopping early at `hi` —
    /// `(bound, inclusive)`. Subtrees whose guaranteed lower bound lies
    /// beyond `hi` are skipped whole, so an equality probe visits only the
    /// leaves that can hold equal keys instead of materializing the entire
    /// right side of the tree.
    pub fn range_bounded(
        &self,
        pager: &mut Pager,
        tx: &Tx,
        lo: &Value,
        hi: Option<(&Value, bool)>,
    ) -> Result<Vec<(Value, u64)>> {
        let mut out = Vec::new();
        Self::range_bounded_rec(pager, tx, self.root, lo, hi, &mut out)?;
        Ok(out)
    }

    /// True when a key at or past `hi` ends the in-order scan.
    fn beyond_hi(k: &Value, hi: Option<(&Value, bool)>) -> bool {
        match hi {
            None => false,
            Some((h, true)) => Value::cmp_values(k, h) == Ordering::Greater,
            Some((h, false)) => Value::cmp_values(k, h) != Ordering::Less,
        }
    }

    fn range_bounded_rec(
        pager: &mut Pager,
        tx: &Tx,
        id: u32,
        lo: &Value,
        hi: Option<(&Value, bool)>,
        out: &mut Vec<(Value, u64)>,
    ) -> Result<()> {
        match Self::read_node(pager, tx, id)? {
            Node::Leaf { cells } => {
                for (k, v) in cells {
                    if Self::beyond_hi(&k, hi) {
                        break; // cells are in key order; the rest is beyond too
                    }
                    if Value::cmp_values(&k, lo) != Ordering::Less {
                        out.push((k, v));
                    }
                }
            }
            Node::Internal { leftmost, cells } => {
                // Same ordering caveat as range_from_rec — a split can leave
                // keys equal to a separator in the child LEFT of it, so a
                // child is skipped only when its own keys are guaranteed
                // beyond hi (lower separator > hi, or == hi when exclusive):
                // every key of the child is >= its lower separator.
                let skip_by_hi = |sep: &Value| -> bool {
                    match hi {
                        None => false,
                        Some((h, true)) => Value::cmp_values(sep, h) == Ordering::Greater,
                        Some((h, false)) => Value::cmp_values(sep, h) != Ordering::Less,
                    }
                };
                let leftmost_upper_ge = match cells.first() {
                    Some((k, _)) => Value::cmp_values(k, lo) != Ordering::Less,
                    None => true,
                };
                if leftmost_upper_ge {
                    Self::range_bounded_rec(pager, tx, leftmost, lo, hi, out)?;
                }
                for (i, (sep, child)) in cells.iter().enumerate() {
                    if skip_by_hi(sep) {
                        continue;
                    }
                    let upper_ge = match cells.get(i + 1) {
                        Some((k, _)) => Value::cmp_values(k, lo) != Ordering::Less,
                        None => true,
                    };
                    if upper_ge {
                        Self::range_bounded_rec(pager, tx, *child, lo, hi, out)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn delete_rec(pager: &mut Pager, tx: &mut Tx, id: u32, key: &Value) -> Result<bool> {
        match Self::read_node(pager, tx, id)? {
            Node::Leaf { mut cells } => {
                match cells.binary_search_by(|(k, _)| Value::cmp_values(k, key)) {
                    Ok(i) => {
                        cells.remove(i);
                        Self::write_node(pager, tx, id, &Node::Leaf { cells })?;
                        Ok(true)
                    }
                    Err(_) => Ok(false),
                }
            }
            Node::Internal { leftmost, cells } => {
                for child in candidate_children(&cells, leftmost, key) {
                    if Self::delete_rec(pager, tx, child, key)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh(name: &str) -> (tempfile::TempDir, Pager) {
        let dir = tempfile::tempdir().unwrap();
        let pager = Pager::open(&dir.path().join(name)).unwrap();
        (dir, pager)
    }

    #[test]
    fn insert_lookup_roundtrip() {
        let (_d, mut pager) = fresh("bt1.db");
        let mut tx = pager.begin_tx();
        let mut tree = BTree::create(&mut pager, &mut tx).unwrap();
        for i in 0..100i64 {
            tree.insert(&mut pager, &mut tx, Value::Int(i), i as u64 * 10, true)
                .unwrap();
        }
        pager.commit_tx(tx).unwrap();
        let tx = pager.begin_tx();
        for i in 0..100i64 {
            assert_eq!(
                tree.get(&mut pager, &tx, &Value::Int(i)).unwrap(),
                Some(i as u64 * 10)
            );
        }
        assert_eq!(tree.get(&mut pager, &tx, &Value::Int(999)).unwrap(), None);
    }

    #[test]
    fn unique_violation_and_duplicate_appends() {
        let (_d, mut pager) = fresh("bt2.db");
        let mut tx = pager.begin_tx();
        let mut tree = BTree::create(&mut pager, &mut tx).unwrap();
        tree.insert(&mut pager, &mut tx, Value::Str("k".into()), 1, true)
            .unwrap();
        assert!(matches!(
            tree.insert(&mut pager, &mut tx, Value::Str("k".into()), 2, true),
            Err(BTreeError::Duplicate)
        ));
        // non-unique appends a second entry for the same key
        tree.insert(&mut pager, &mut tx, Value::Str("k".into()), 2, false)
            .unwrap();
        pager.commit_tx(tx).unwrap();
        let tx = pager.begin_tx();
        let all = tree.scan(&mut pager, &tx).unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].1, 1);
        assert_eq!(all[1].1, 2);
    }

    #[test]
    fn duplicate_runs_survive_splits() {
        let (_d, mut pager) = fresh("bt9.db");
        let mut tx = pager.begin_tx();
        let mut tree = BTree::create(&mut pager, &mut tx).unwrap();
        // Only 10 distinct keys; runs far exceed page capacity, forcing
        // splits with equal separators.
        let mut model = std::collections::BTreeMap::<i64, usize>::new();
        for i in 0..500i64 {
            let k = i % 10;
            tree.insert(&mut pager, &mut tx, Value::Int(k), i as u64, false)
                .unwrap();
            *model.entry(k).or_insert(0) += 1;
        }
        pager.commit_tx(tx).unwrap();
        let tx = pager.begin_tx();
        let all = tree.scan(&mut pager, &tx).unwrap();
        assert_eq!(all.len(), 500);
        // grouped and sorted by key, locators ascending within each run
        let mut last_key = i64::MIN;
        let mut last_loc = 0u64;
        for (k, v) in &all {
            let k = k.as_i64().unwrap();
            assert!(k >= last_key, "keys must be non-decreasing");
            if k == last_key {
                assert!(*v > last_loc, "locators ascending within a run");
            }
            last_key = k;
            last_loc = *v;
        }
        for (k, n) in &model {
            let got = all
                .iter()
                .filter(|(key, _)| key.as_i64() == Some(*k))
                .count();
            assert_eq!(got, *n, "key {k}");
        }
        // range_from lands inside the run correctly
        let from_5 = tree.range_from(&mut pager, &tx, &Value::Int(5)).unwrap();
        assert_eq!(from_5.len(), 500 - 5 * 50);
    }

    #[test]
    fn ordered_scan_across_splits() {
        let (_d, mut pager) = fresh("bt3.db");
        let mut tx = pager.begin_tx();
        let mut tree = BTree::create(&mut pager, &mut tx).unwrap();
        // Descending insert forces many splits.
        for i in (0..500i64).rev() {
            tree.insert(&mut pager, &mut tx, Value::Int(i), i as u64, true)
                .unwrap();
        }
        pager.commit_tx(tx).unwrap();
        let tx = pager.begin_tx();
        let all = tree.scan(&mut pager, &tx).unwrap();
        assert_eq!(all.len(), 500);
        for (i, (k, v)) in all.iter().enumerate() {
            assert_eq!(*k, Value::Int(i as i64));
            assert_eq!(*v, i as u64);
        }
    }

    #[test]
    fn delete_then_lookup() {
        let (_d, mut pager) = fresh("bt4.db");
        let mut tx = pager.begin_tx();
        let mut tree = BTree::create(&mut pager, &mut tx).unwrap();
        for i in 0..200i64 {
            tree.insert(&mut pager, &mut tx, Value::Int(i), i as u64, true)
                .unwrap();
        }
        for i in (0..200i64).step_by(2) {
            assert!(tree.delete(&mut pager, &mut tx, &Value::Int(i)).unwrap());
        }
        assert!(!tree.delete(&mut pager, &mut tx, &Value::Int(0)).unwrap()); // already gone
        pager.commit_tx(tx).unwrap();
        let tx = pager.begin_tx();
        for i in 0..200i64 {
            let expect = if i % 2 == 0 { None } else { Some(i as u64) };
            assert_eq!(tree.get(&mut pager, &tx, &Value::Int(i)).unwrap(), expect);
        }
    }

    #[test]
    fn survives_reopen_via_pager() {
        let (d, mut pager) = fresh("bt5.db");
        let root;
        {
            let mut tx = pager.begin_tx();
            let mut tree = BTree::create(&mut pager, &mut tx).unwrap();
            for i in 0..300i64 {
                tree.insert(&mut pager, &mut tx, Value::Int(i), i as u64, true)
                    .unwrap();
            }
            root = tree.root;
            pager.commit_tx(tx).unwrap();
        }
        drop(pager);
        let mut pager = Pager::open(&d.path().join("bt5.db")).unwrap();
        let tree = BTree::open(root);
        let tx = pager.begin_tx();
        assert_eq!(
            tree.get(&mut pager, &tx, &Value::Int(299)).unwrap(),
            Some(299)
        );
        assert_eq!(tree.get(&mut pager, &tx, &Value::Int(0)).unwrap(), Some(0));
    }

    #[test]
    fn range_from_finds_suffix_in_order() {
        let (_d, mut pager) = fresh("bt7.db");
        let mut tx = pager.begin_tx();
        let mut tree = BTree::create(&mut pager, &mut tx).unwrap();
        for i in 0..300i64 {
            tree.insert(&mut pager, &mut tx, Value::Int(i), i as u64, true)
                .unwrap();
        }
        pager.commit_tx(tx).unwrap();
        let tx = pager.begin_tx();
        let got = tree.range_from(&mut pager, &tx, &Value::Int(295)).unwrap();
        let keys: Vec<i64> = got.iter().map(|(k, _)| k.as_i64().unwrap()).collect();
        assert_eq!(keys, vec![295, 296, 297, 298, 299]);
        // Below everything → whole tree; above everything → empty.
        assert_eq!(
            tree.range_from(&mut pager, &tx, &Value::Int(-1))
                .unwrap()
                .len(),
            300
        );
        assert!(tree
            .range_from(&mut pager, &tx, &Value::Int(300))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn delete_entry_removes_exact_pair() {
        let (_d, mut pager) = fresh("bt8.db");
        let mut tx = pager.begin_tx();
        let mut tree = BTree::create(&mut pager, &mut tx).unwrap();
        // Same key, three locators (non-unique index; inserts append).
        for loc in [10u64, 20, 30] {
            tree.insert(&mut pager, &mut tx, Value::Str("k".into()), loc, false)
                .unwrap();
        }
        // Non-unique inserts append separate entries per locator.
        assert_eq!(tree.scan(&mut pager, &tx).unwrap().len(), 3);
        assert!(tree
            .delete_entry(&mut pager, &mut tx, &Value::Str("k".into()), 20)
            .unwrap());
        let left: Vec<u64> = tree
            .scan(&mut pager, &tx)
            .unwrap()
            .into_iter()
            .map(|(_, v)| v)
            .collect();
        assert_eq!(left, vec![10, 30]);
        assert!(!tree
            .delete_entry(&mut pager, &mut tx, &Value::Str("k".into()), 20)
            .unwrap());
        pager.commit_tx(tx).unwrap();
    }

    /// Property test against BTreeMap: interleaved random-ish ops must agree.
    #[test]
    fn matches_btreemap_under_lcg_ops() {
        let (_d, mut pager) = fresh("bt6.db");
        let mut tx = pager.begin_tx();
        let mut tree = BTree::create(&mut pager, &mut tx).unwrap();
        let mut model = std::collections::BTreeMap::<i64, u64>::new();
        // Deterministic LCG for reproducibility.
        let mut seed: u64 = 0x2545F4914F6CDD1D;
        let mut next = || {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            seed >> 33
        };
        for _ in 0..2000 {
            let r = next();
            let k = (r % 500) as i64;
            match r % 3 {
                0 | 1 => {
                    let v = (r >> 16) % 10_000;
                    let res = tree.insert(&mut pager, &mut tx, Value::Int(k), v, true);
                    match (model.contains_key(&k), res) {
                        (true, Err(BTreeError::Duplicate)) => {}
                        (false, Ok(())) => {
                            model.insert(k, v);
                        }
                        (_, r) => panic!("unexpected insert result for k={k}: {r:?}"),
                    }
                }
                _ => {
                    let got = tree.delete(&mut pager, &mut tx, &Value::Int(k)).unwrap();
                    let expect = model.remove(&k).is_some();
                    assert_eq!(got, expect);
                }
            }
        }
        pager.commit_tx(tx).unwrap();
        let tx = pager.begin_tx();
        for (k, v) in &model {
            assert_eq!(
                tree.get(&mut pager, &tx, &Value::Int(*k)).unwrap(),
                Some(*v),
                "key {k}"
            );
        }
        let scanned = tree.scan(&mut pager, &tx).unwrap();
        assert_eq!(scanned.len(), model.len());
        for ((k, v), (mk, mv)) in scanned.iter().zip(model.iter()) {
            assert_eq!(k.as_i64(), Some(*mk));
            assert_eq!(*v, *mv);
        }
    }
    // ---- 覆盖率补充:节点分裂 / 超大键 / 越界写 ----

    #[test]
    fn tree_splits_under_load_and_range_reads_all() {
        let (_d, mut pager) = fresh("bt_split.db");
        let mut tx = pager.begin_tx();
        let mut tree = BTree::create(&mut pager, &mut tx).unwrap();
        // 大量插入迫使叶节点多次分裂
        for i in 0..2000i64 {
            tree.insert(&mut pager, &mut tx, Value::Int(i), i as u64, false)
                .unwrap();
        }
        pager.commit_tx(tx).unwrap();
        let tx = pager.begin_tx();
        let all = tree.range_from(&mut pager, &tx, &Value::Int(0)).unwrap();
        assert_eq!(all.len(), 2000);
        assert_eq!(all[0].0, Value::Int(0));
        assert_eq!(all[1999].0, Value::Int(1999));
        pager.abort_tx(tx).unwrap();
    }

    #[test]
    fn oversized_key_rejected() {
        let (_d, mut pager) = fresh("bt_bigkey.db");
        let mut tx = pager.begin_tx();
        let mut tree = BTree::create(&mut pager, &mut tx).unwrap();
        let big = Value::Str("K".repeat(PAGE_SIZE));
        let e = tree.insert(&mut pager, &mut tx, big, 1, false).unwrap_err();
        assert!(matches!(e, BTreeError::KeyTooLarge(_)), "{e:?}");
        pager.abort_tx(tx).unwrap();
    }
    #[test]
    fn delete_through_internal_nodes() {
        let (_d, mut pager) = fresh("bt_del.db");
        let mut tx = pager.begin_tx();
        let mut tree = BTree::create(&mut pager, &mut tx).unwrap();
        for i in 0..1500i64 {
            tree.insert(&mut pager, &mut tx, Value::Int(i), i as u64, false)
                .unwrap();
        }
        pager.commit_tx(tx).unwrap();
        // 删除一半,迫使删除路径下沉经过内部节点
        for i in (0..1500i64).step_by(2) {
            let mut tx = pager.begin_tx();
            tree.delete_entry(&mut pager, &mut tx, &Value::Int(i), i as u64)
                .unwrap();
            pager.commit_tx(tx).unwrap();
        }
        let tx = pager.begin_tx();
        let all = tree.range_from(&mut pager, &tx, &Value::Int(0)).unwrap();
        assert_eq!(all.len(), 750);
        assert!(all.iter().all(|(k, _)| k.as_i64().unwrap() % 2 == 1));
        pager.abort_tx(tx).unwrap();
    }

    #[test]
    fn medium_string_keys_split_by_bytes() {
        // ~90 字节编码的键在 ~43 个时就会占满一页——远低于任何条数阈值;
        // 分裂必须按字节驱动,否则这些完全合法的键会让索引建不起来。
        let (_d, mut pager) = fresh("bt_medkey.db");
        let mut tx = pager.begin_tx();
        let mut tree = BTree::create(&mut pager, &mut tx).unwrap();
        let key = |i: i64| Value::Str(format!("k{i:06}-{}", "x".repeat(80)));
        for i in 0..200i64 {
            tree.insert(&mut pager, &mut tx, key(i), i as u64, true)
                .unwrap();
        }
        pager.commit_tx(tx).unwrap();
        let tx = pager.begin_tx();
        for i in 0..200i64 {
            assert_eq!(
                tree.get(&mut pager, &tx, &key(i)).unwrap(),
                Some(i as u64),
                "key {i}"
            );
        }
        let all = tree.scan(&mut pager, &tx).unwrap();
        assert_eq!(all.len(), 200);
        let mut sorted = all.clone();
        sorted.sort_by(|a, b| Value::cmp_values(&a.0, &b.0));
        assert_eq!(all, sorted, "scan must stay in key order");
        pager.abort_tx(tx).unwrap();
    }

    #[test]
    fn delete_entry_across_equal_separator_splits() {
        // 非唯一等键 run 横跨分裂点后,条目可能落在与 separator 相等的
        // 左子树里(descend 只往右路由)——delete_entry 必须找得到它们。
        let (_d, mut pager) = fresh("bt_eqdel.db");
        let mut tx = pager.begin_tx();
        let mut tree = BTree::create(&mut pager, &mut tx).unwrap();
        for i in 0..500i64 {
            let k = i % 10;
            tree.insert(&mut pager, &mut tx, Value::Int(k), i as u64, false)
                .unwrap();
        }
        pager.commit_tx(tx).unwrap();
        for i in 0..500i64 {
            let mut tx = pager.begin_tx();
            let k = Value::Int(i % 10);
            assert!(
                tree.delete_entry(&mut pager, &mut tx, &k, i as u64)
                    .unwrap(),
                "entry (k={}, loc={i}) must be found",
                i % 10
            );
            pager.commit_tx(tx).unwrap();
        }
        let tx = pager.begin_tx();
        assert!(
            tree.scan(&mut pager, &tx).unwrap().is_empty(),
            "no ghost entries may survive"
        );
        pager.abort_tx(tx).unwrap();
    }
}
