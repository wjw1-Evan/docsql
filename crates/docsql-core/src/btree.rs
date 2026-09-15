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
use crate::pager::{PageReader, Pager, PagerError, Tx, PAGE_SIZE};
use crate::value::Value;
use std::cmp::Ordering;

const LEAF: u8 = 1;
const INTERNAL: u8 = 2;

/// A cell that cannot occupy at most half a page leaves no split point with
/// both halves inside a page, so such keys are rejected up front.
const HALF_PAGE: usize = PAGE_SIZE / 2;

/// Serialized node size — `header` is 3 (leaf) or 7 (internal), `payload`
/// the per-cell locator width (8 or 4). `scratch` is a caller-owned reuse
/// buffer: key bytes are encoded into it (cleared per key) instead of a
/// fresh `Vec` per key — an insert used to re-encode every key of the node
/// just to test page fit.
fn node_bytes<T>(
    cells: &[(Value, T)],
    header: usize,
    payload: usize,
    scratch: &mut Vec<u8>,
) -> Result<usize> {
    let mut n = header;
    for (k, _) in cells {
        scratch.clear();
        encode::encode(k, scratch)?;
        n += 2 + scratch.len() + payload;
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
    scratch: &mut Vec<u8>,
) -> Result<usize> {
    let mut prefix = Vec::with_capacity(cells.len() + 1);
    prefix.push(header);
    for (k, _) in cells {
        scratch.clear();
        encode::encode(k, scratch)?;
        let last = *prefix.last().unwrap();
        prefix.push(last + 2 + scratch.len() + payload);
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
    pager: &'a Pager,
    tx: &'a mut Tx,
    /// Reuse buffer for key encoding during size probes (`node_bytes`,
    /// `split_at_for_insert`) — never outlives a call.
    scratch: Vec<u8>,
}

impl BTree {
    /// Create a fresh tree (allocates a root leaf inside `tx`).
    pub fn create(pager: &Pager, tx: &mut Tx) -> Result<BTree> {
        let next_page = pager.num_pages();
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

    fn read_node(reader: &PageReader, tx: &Tx, id: u32) -> Result<Node> {
        // Owned page copy: btree reads run on `&Pager` (MVCC stage A shared
        // readers / stage B snapshot readers). A 4 KiB copy per node visit is
        // noise next to decode.
        let page: Vec<u8> = reader.page(Some(tx), id)?;
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
                    let klen =
                        u16::from_le_bytes(take(&page, pos, 2)?.try_into().unwrap()) as usize;
                    pos += 2;
                    let (k, used) = encode::decode_prefix(take(&page, pos, klen)?)?;
                    pos += used;
                    let val = u64::from_le_bytes(take(&page, pos, 8)?.try_into().unwrap());
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
                    let klen =
                        u16::from_le_bytes(take(&page, pos, 2)?.try_into().unwrap()) as usize;
                    pos += 2;
                    let (k, used) = encode::decode_prefix(take(&page, pos, klen)?)?;
                    pos += used;
                    let child = u32::from_le_bytes(take(&page, pos, 4)?.try_into().unwrap());
                    pos += 4;
                    cells.push((k, child));
                }
                Ok(Node::Internal { leftmost, cells })
            }
            _ => Err(BTreeError::Corrupt("unknown node type")),
        }
    }

    fn write_node(pager: &Pager, tx: &mut Tx, id: u32, node: &Node) -> Result<()> {
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
    pub fn get(&self, reader: &PageReader, tx: &Tx, key: &Value) -> Result<Option<u64>> {
        Self::get_at(reader, tx, self.root, key)
    }

    fn get_at(reader: &PageReader, tx: &Tx, id: u32, key: &Value) -> Result<Option<u64>> {
        match Self::read_node(reader, tx, id)? {
            // Leaves are kept sorted by cmp_values (insert uses
            // partition_point), so the first key >= `key` decides.
            Node::Leaf { cells } => {
                let i = cells.partition_point(|(k, _)| Value::cmp_values(k, key) == Ordering::Less);
                Ok(
                    if i < cells.len() && Value::cmp_values(&cells[i].0, key) == Ordering::Equal {
                        Some(cells[i].1)
                    } else {
                        None
                    },
                )
            }
            Node::Internal { leftmost, cells } => {
                for child in candidate_children(&cells, leftmost, key) {
                    if let Some(v) = Self::get_at(reader, tx, child, key)? {
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
        pager: &Pager,
        tx: &mut Tx,
        key: Value,
        val: u64,
        unique: bool,
    ) -> Result<()> {
        let mut ctx = Ctx {
            pager,
            tx,
            scratch: Vec::new(),
        };
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
        match Self::read_node(&PageReader::current(ctx.pager), ctx.tx, id)? {
            Node::Leaf { mut cells } => {
                // Insert after all keys <= key (rightmost of an equal run) so
                // duplicate keys accumulate and routing (equal goes right)
                // stays consistent.
                let at =
                    cells.partition_point(|(k, _)| Value::cmp_values(k, key) != Ordering::Greater);
                if unique && at > 0 && Value::cmp_values(&cells[at - 1].0, key) == Ordering::Equal {
                    return Err(BTreeError::Duplicate);
                }
                let kb_len = {
                    ctx.scratch.clear();
                    encode::encode(key, &mut ctx.scratch)?;
                    ctx.scratch.len()
                };
                if 2 + kb_len + 8 > HALF_PAGE {
                    return Err(BTreeError::KeyTooLarge(kb_len));
                }
                cells.insert(at, (key.clone(), val));
                if node_bytes(&cells, 3, 8, &mut ctx.scratch)? <= PAGE_SIZE {
                    Self::write_node(ctx.pager, ctx.tx, id, &Node::Leaf { cells })?;
                    Ok(None)
                } else {
                    let m = split_at_for_insert(&cells, at, 3, 8, &mut ctx.scratch)?;
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
                    let mb_len = {
                        ctx.scratch.clear();
                        encode::encode(&mid, &mut ctx.scratch)?;
                        ctx.scratch.len()
                    };
                    if 2 + mb_len + 4 > HALF_PAGE {
                        return Err(BTreeError::KeyTooLarge(mb_len));
                    }
                    cells.insert(at, (mid, right));
                    if node_bytes(&cells, 7, 4, &mut ctx.scratch)? <= PAGE_SIZE {
                        Self::write_node(
                            ctx.pager,
                            ctx.tx,
                            id,
                            &Node::Internal { leftmost, cells },
                        )?;
                        Ok(None)
                    } else {
                        let m = split_at_for_insert(&cells, at, 7, 4, &mut ctx.scratch)?;
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
    pub fn scan(&self, reader: &PageReader, tx: &Tx) -> Result<Vec<(Value, u64)>> {
        self.scan_limited(reader, tx, None)
    }

    /// In-order scan capped at `max` entries (`None` = unlimited). Callers
    /// that only need an ORDER BY … LIMIT prefix stop walking the tree as
    /// soon as the window is filled.
    pub fn scan_limited(
        &self,
        reader: &PageReader,
        tx: &Tx,
        max: Option<usize>,
    ) -> Result<Vec<(Value, u64)>> {
        // Leaves are not chained in v0; walk the tree recursively.
        let mut out = Vec::new();
        Self::scan_rec(reader, tx, self.root, max, &mut out)?;
        Ok(out)
    }

    fn scan_rec(
        reader: &PageReader,
        tx: &Tx,
        id: u32,
        max: Option<usize>,
        out: &mut Vec<(Value, u64)>,
    ) -> Result<()> {
        if max.is_some_and(|m| out.len() >= m) {
            return Ok(());
        }
        match Self::read_node(reader, tx, id)? {
            Node::Leaf { cells } => {
                for cell in cells {
                    out.push(cell);
                    if max.is_some_and(|m| out.len() >= m) {
                        break;
                    }
                }
            }
            Node::Internal { leftmost, cells } => {
                Self::scan_rec(reader, tx, leftmost, max, out)?;
                for (_, child) in cells {
                    if max.is_some_and(|m| out.len() >= m) {
                        break;
                    }
                    Self::scan_rec(reader, tx, child, max, out)?;
                }
            }
        }
        Ok(())
    }

    /// Reverse (descending key) scan capped at `max` entries — the
    /// `ORDER BY … DESC LIMIT` mirror of [`BTree::scan_limited`].
    pub fn scan_limited_rev(
        &self,
        reader: &PageReader,
        tx: &Tx,
        max: Option<usize>,
    ) -> Result<Vec<(Value, u64)>> {
        let mut out = Vec::new();
        Self::scan_rev_rec(reader, tx, self.root, max, &mut out)?;
        Ok(out)
    }

    fn scan_rev_rec(
        reader: &PageReader,
        tx: &Tx,
        id: u32,
        max: Option<usize>,
        out: &mut Vec<(Value, u64)>,
    ) -> Result<()> {
        if max.is_some_and(|m| out.len() >= m) {
            return Ok(());
        }
        match Self::read_node(reader, tx, id)? {
            Node::Leaf { cells } => {
                for cell in cells.into_iter().rev() {
                    out.push(cell);
                    if max.is_some_and(|m| out.len() >= m) {
                        break;
                    }
                }
            }
            Node::Internal { leftmost, cells } => {
                for (_, child) in cells.iter().rev() {
                    if max.is_some_and(|m| out.len() >= m) {
                        break;
                    }
                    Self::scan_rev_rec(reader, tx, *child, max, out)?;
                }
                Self::scan_rev_rec(reader, tx, leftmost, max, out)?;
            }
        }
        Ok(())
    }

    /// Remove a key. Returns whether it was present.
    pub fn delete(&mut self, pager: &Pager, tx: &mut Tx, key: &Value) -> Result<bool> {
        Self::delete_rec(pager, tx, self.root, key, &|cells: &mut Vec<(
            Value,
            u64,
        )>| {
            match cells.binary_search_by(|(k, _)| Value::cmp_values(k, key)) {
                Ok(i) => {
                    cells.remove(i);
                    true
                }
                Err(_) => false,
            }
        })
    }

    /// Remove the exact `(key, locator)` pair. Needed for non-unique trees
    /// where several entries share a key: `delete` would drop an arbitrary
    /// one of them. Returns whether the pair was present.
    pub fn delete_entry(
        &mut self,
        pager: &Pager,
        tx: &mut Tx,
        key: &Value,
        loc: u64,
    ) -> Result<bool> {
        Self::delete_rec(pager, tx, self.root, key, &|cells: &mut Vec<(
            Value,
            u64,
        )>| {
            // Cells are sorted by cmp_values (insert maintains the order;
            // `delete` above already relies on binary search over them), so
            // equal keys form one contiguous run: land on it with
            // binary_search, then scan only that run for the locator — the
            // old full-leaf scan was O(cells) on every index-entry removal.
            match cells.binary_search_by(|(k, _)| Value::cmp_values(k, key)) {
                Err(_) => false,
                Ok(mut i) => {
                    while i > 0 && Value::cmp_values(&cells[i - 1].0, key) == Ordering::Equal {
                        i -= 1;
                    }
                    let mut j = i;
                    while j < cells.len() && Value::cmp_values(&cells[j].0, key) == Ordering::Equal
                    {
                        if cells[j].1 == loc {
                            cells.remove(j);
                            return true;
                        }
                        j += 1;
                    }
                    false
                }
            }
        })
    }

    /// Shared recursion for the two delete shapes: the internal walk (via
    /// `candidate_children`, since equal-key runs can straddle a split) is
    /// identical, only the leaf matcher differs.
    fn delete_rec<F>(
        pager: &Pager,
        tx: &mut Tx,
        id: u32,
        key: &Value,
        leaf_match: &F,
    ) -> Result<bool>
    where
        F: Fn(&mut Vec<(Value, u64)>) -> bool,
    {
        match Self::read_node(&PageReader::current(pager), tx, id)? {
            Node::Leaf { mut cells } => {
                if leaf_match(&mut cells) {
                    Self::write_node(pager, tx, id, &Node::Leaf { cells })?;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Node::Internal { leftmost, cells } => {
                for child in candidate_children(&cells, leftmost, key) {
                    if Self::delete_rec(pager, tx, child, key, leaf_match)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
        }
    }

    /// All pairs with key >= `key`, in key order (range-scan entry point).
    /// Cheap: only the subtree that can hold `key..` is visited.
    pub fn range_from(
        &self,
        reader: &PageReader,
        tx: &Tx,
        key: &Value,
    ) -> Result<Vec<(Value, u64)>> {
        // The unbounded case of range_bounded (hi = None skips nothing by
        // upper bound), so one walker serves both.
        self.range_bounded(reader, tx, key, None)
    }

    /// Entries with key >= `lo`, optionally stopping early at `hi` —
    /// `(bound, inclusive)`. Subtrees whose guaranteed lower bound lies
    /// beyond `hi` are skipped whole, so an equality probe visits only the
    /// leaves that can hold equal keys instead of materializing the entire
    /// right side of the tree.
    pub fn range_bounded(
        &self,
        reader: &PageReader,
        tx: &Tx,
        lo: &Value,
        hi: Option<(&Value, bool)>,
    ) -> Result<Vec<(Value, u64)>> {
        self.range_bounded_limited(reader, tx, lo, hi, None)
    }

    /// `range_bounded` with an entry cap (see [`BTree::scan_limited`]).
    pub fn range_bounded_limited(
        &self,
        reader: &PageReader,
        tx: &Tx,
        lo: &Value,
        hi: Option<(&Value, bool)>,
        max: Option<usize>,
    ) -> Result<Vec<(Value, u64)>> {
        let mut out = Vec::new();
        Self::range_bounded_rec(reader, tx, self.root, lo, true, hi, max, &mut out)?;
        Ok(out)
    }

    /// `range_bounded_limited` with a strict lower bound: entries equal to
    /// `lo` are dropped during the walk, so a cap counts only kept entries
    /// (a `retain` afterwards would let an equal-key run exhaust the cap).
    pub fn range_bounded_excl_limited(
        &self,
        reader: &PageReader,
        tx: &Tx,
        lo: &Value,
        hi: Option<(&Value, bool)>,
        max: Option<usize>,
    ) -> Result<Vec<(Value, u64)>> {
        let mut out = Vec::new();
        Self::range_bounded_rec(reader, tx, self.root, lo, false, hi, max, &mut out)?;
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

    /// True when `k` fails an optional lower bound `(value, inclusive)`.
    /// Works for values as well as subtree upper bounds: a bound `u` below
    /// `lo` means every key `<= u` fails it too.
    fn below_lo(k: &Value, lo: Option<(&Value, bool)>) -> bool {
        match lo {
            None => false,
            Some((l, true)) => Value::cmp_values(k, l) == Ordering::Less,
            Some((l, false)) => Value::cmp_values(k, l) != Ordering::Greater,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn range_bounded_rec(
        reader: &PageReader,
        tx: &Tx,
        id: u32,
        lo: &Value,
        lo_incl: bool,
        hi: Option<(&Value, bool)>,
        max: Option<usize>,
        out: &mut Vec<(Value, u64)>,
    ) -> Result<()> {
        if max.is_some_and(|m| out.len() >= m) {
            return Ok(());
        }
        match Self::read_node(reader, tx, id)? {
            Node::Leaf { cells } => {
                for (k, v) in cells {
                    if Self::beyond_hi(&k, hi) {
                        break; // cells are in key order; the rest is beyond too
                    }
                    let o = Value::cmp_values(&k, lo);
                    if o == Ordering::Less || (o == Ordering::Equal && !lo_incl) {
                        continue;
                    }
                    out.push((k, v));
                    if max.is_some_and(|m| out.len() >= m) {
                        break;
                    }
                }
            }
            Node::Internal { leftmost, cells } => {
                // Same ordering caveat as range_from_rec — a split can leave
                // keys equal to a separator in the child LEFT of it, so a
                // child is skipped only when its own keys are guaranteed
                // beyond hi (lower separator > hi, or == hi when exclusive):
                // every key of the child is >= its lower separator.
                let skip_by_hi = |sep: &Value| -> bool { Self::beyond_hi(sep, hi) };
                let leftmost_upper_ge = match cells.first() {
                    Some((k, _)) => Value::cmp_values(k, lo) != Ordering::Less,
                    None => true,
                };
                if leftmost_upper_ge {
                    Self::range_bounded_rec(reader, tx, leftmost, lo, lo_incl, hi, max, out)?;
                }
                for (i, (sep, child)) in cells.iter().enumerate() {
                    if max.is_some_and(|m| out.len() >= m) {
                        break;
                    }
                    if skip_by_hi(sep) {
                        continue;
                    }
                    let upper_ge = match cells.get(i + 1) {
                        Some((k, _)) => Value::cmp_values(k, lo) != Ordering::Less,
                        None => true,
                    };
                    if upper_ge {
                        Self::range_bounded_rec(reader, tx, *child, lo, lo_incl, hi, max, out)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Entries with key >(=) `lo` and <(=) `hi`, largest first, capped at
    /// `max` — the descending mirror of [`BTree::range_bounded_limited`]
    /// used by `ORDER BY … DESC LIMIT` windows. The walk starts at the
    /// child that can still hold `hi` and moves left, so it costs
    /// O(log n + collected) rather than materializing the range.
    pub fn range_bounded_rev_limited(
        &self,
        reader: &PageReader,
        tx: &Tx,
        lo: Option<(&Value, bool)>,
        hi: Option<(&Value, bool)>,
        max: Option<usize>,
    ) -> Result<Vec<(Value, u64)>> {
        let mut out = Vec::new();
        Self::range_rev_rec(reader, tx, self.root, lo, hi, max, &mut out)?;
        Ok(out)
    }

    fn range_rev_rec(
        reader: &PageReader,
        tx: &Tx,
        id: u32,
        lo: Option<(&Value, bool)>,
        hi: Option<(&Value, bool)>,
        max: Option<usize>,
        out: &mut Vec<(Value, u64)>,
    ) -> Result<()> {
        if max.is_some_and(|m| out.len() >= m) {
            return Ok(());
        }
        match Self::read_node(reader, tx, id)? {
            Node::Leaf { cells } => {
                for (k, v) in cells.into_iter().rev() {
                    if Self::beyond_hi(&k, hi) {
                        continue; // above the upper bound; keep walking down
                    }
                    if Self::below_lo(&k, lo) {
                        break; // cells are in key order; the rest is below too
                    }
                    out.push((k, v));
                    if max.is_some_and(|m| out.len() >= m) {
                        break;
                    }
                }
            }
            Node::Internal { leftmost, cells } => {
                // Children right of the last separator at or below `hi` hold
                // only keys beyond it and are skipped whole; the walk starts
                // there and moves left. A child whose upper separator already
                // fails `lo` ends the walk (everything further left fails too).
                if let Some(b) = cells.iter().rposition(|(sep, _)| !Self::beyond_hi(sep, hi)) {
                    for i in (0..=b).rev() {
                        if max.is_some_and(|m| out.len() >= m) {
                            return Ok(());
                        }
                        let upper = cells.get(i + 1).map(|(k, _)| k);
                        if upper.is_some_and(|u| Self::below_lo(u, lo)) {
                            return Ok(());
                        }
                        Self::range_rev_rec(reader, tx, cells[i].1, lo, hi, max, out)?;
                    }
                }
                if max.is_some_and(|m| out.len() >= m) {
                    return Ok(());
                }
                let upper = cells.first().map(|(k, _)| k);
                if upper.is_some_and(|u| Self::below_lo(u, lo)) {
                    return Ok(());
                }
                Self::range_rev_rec(reader, tx, leftmost, lo, hi, max, out)?;
            }
        }
        Ok(())
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
        let (_d, pager) = fresh("bt1.db");
        let mut tx = pager.begin_tx();
        let mut tree = BTree::create(&pager, &mut tx).unwrap();
        for i in 0..100i64 {
            tree.insert(&pager, &mut tx, Value::Int(i), i as u64 * 10, true)
                .unwrap();
        }
        pager.commit_tx(tx).unwrap();
        let tx = pager.begin_tx();
        for i in 0..100i64 {
            assert_eq!(
                tree.get(&PageReader::current(&pager), &tx, &Value::Int(i))
                    .unwrap(),
                Some(i as u64 * 10)
            );
        }
        assert_eq!(
            tree.get(&PageReader::current(&pager), &tx, &Value::Int(999))
                .unwrap(),
            None
        );
    }

    #[test]
    fn unique_violation_and_duplicate_appends() {
        let (_d, pager) = fresh("bt2.db");
        let mut tx = pager.begin_tx();
        let mut tree = BTree::create(&pager, &mut tx).unwrap();
        tree.insert(&pager, &mut tx, Value::Str("k".into()), 1, true)
            .unwrap();
        assert!(matches!(
            tree.insert(&pager, &mut tx, Value::Str("k".into()), 2, true),
            Err(BTreeError::Duplicate)
        ));
        // non-unique appends a second entry for the same key
        tree.insert(&pager, &mut tx, Value::Str("k".into()), 2, false)
            .unwrap();
        pager.commit_tx(tx).unwrap();
        let tx = pager.begin_tx();
        let all = tree.scan(&PageReader::current(&pager), &tx).unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].1, 1);
        assert_eq!(all[1].1, 2);
    }

    #[test]
    fn duplicate_runs_survive_splits() {
        let (_d, pager) = fresh("bt9.db");
        let mut tx = pager.begin_tx();
        let mut tree = BTree::create(&pager, &mut tx).unwrap();
        // Only 10 distinct keys; runs far exceed page capacity, forcing
        // splits with equal separators.
        let mut model = std::collections::BTreeMap::<i64, usize>::new();
        for i in 0..500i64 {
            let k = i % 10;
            tree.insert(&pager, &mut tx, Value::Int(k), i as u64, false)
                .unwrap();
            *model.entry(k).or_insert(0) += 1;
        }
        pager.commit_tx(tx).unwrap();
        let tx = pager.begin_tx();
        let all = tree.scan(&PageReader::current(&pager), &tx).unwrap();
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
        let from_5 = tree
            .range_from(&PageReader::current(&pager), &tx, &Value::Int(5))
            .unwrap();
        assert_eq!(from_5.len(), 500 - 5 * 50);
    }

    #[test]
    fn deep_tree_splits_internal_nodes_and_keeps_every_key() {
        let (_d, pager) = fresh("bt-deep.db");
        let mut tx = pager.begin_tx();
        let mut tree = BTree::create(&pager, &mut tx).unwrap();
        // Long keys keep the fan-out low (~20 entries per page), so a couple
        // of thousand inserts push the root past one page and exercise the
        // internal split path, not just leaf splits.
        let key = |i: i64| Value::Str(format!("key-{i:0>180}"));
        for i in 0..2000i64 {
            tree.insert(&pager, &mut tx, key(i), i as u64, true)
                .unwrap();
        }
        pager.commit_tx(tx).unwrap();
        let tx = pager.begin_tx();
        let reader = PageReader::current(&pager);
        for i in (0..2000i64).step_by(37) {
            assert_eq!(tree.get(&reader, &tx, &key(i)).unwrap(), Some(i as u64));
        }
        let all = tree.scan(&reader, &tx).unwrap();
        assert_eq!(all.len(), 2000);
        assert!(all
            .windows(2)
            .all(|w| Value::cmp_values(&w[0].0, &w[1].0) == std::cmp::Ordering::Less));
    }

    #[test]
    fn capped_scans_stop_at_max() {
        let (_d, pager) = fresh("bt10.db");
        let mut tx = pager.begin_tx();
        let mut tree = BTree::create(&pager, &mut tx).unwrap();
        for i in 0..300i64 {
            tree.insert(&pager, &mut tx, Value::Int(i), i as u64, true)
                .unwrap();
        }
        pager.commit_tx(tx).unwrap();
        let tx = pager.begin_tx();
        let reader = PageReader::current(&pager);
        let full = tree.scan(&reader, &tx).unwrap();
        assert_eq!(tree.scan_limited(&reader, &tx, Some(7)).unwrap(), full[..7]);
        assert!(tree.scan_limited(&reader, &tx, Some(0)).unwrap().is_empty());
        assert_eq!(tree.scan_limited(&reader, &tx, None).unwrap(), full);
        assert_eq!(
            tree.range_bounded_limited(&reader, &tx, &Value::Int(100), None, Some(5))
                .unwrap(),
            full[100..105]
        );
        // The exclusive walk drops the equal keys itself, so the cap still
        // yields five kept entries (a post-scan retain would waste it).
        assert_eq!(
            tree.range_bounded_excl_limited(&reader, &tx, &Value::Int(100), None, Some(5))
                .unwrap(),
            full[101..106]
        );
        // Descending mirrors.
        let mut rev = full.clone();
        rev.reverse();
        assert_eq!(
            tree.scan_limited_rev(&reader, &tx, Some(7)).unwrap(),
            rev[..7]
        );
        assert_eq!(tree.scan_limited_rev(&reader, &tx, None).unwrap(), rev);
        assert_eq!(
            tree.range_bounded_rev_limited(
                &reader,
                &tx,
                Some((&Value::Int(100), true)),
                Some((&Value::Int(104), true)),
                Some(5)
            )
            .unwrap(),
            rev[195..200]
        );
        assert_eq!(
            tree.range_bounded_rev_limited(
                &reader,
                &tx,
                Some((&Value::Int(100), false)),
                Some((&Value::Int(104), false)),
                Some(3)
            )
            .unwrap(),
            rev[196..199]
        );
    }

    #[test]
    fn bounded_reverse_walk_matches_filtered_scan() {
        let (_d, pager) = fresh("bt12.db");
        let mut tx = pager.begin_tx();
        let mut tree = BTree::create(&pager, &mut tx).unwrap();
        // Non-unique keys: every key has three entries, so equal-key runs
        // straddle splits.
        for i in 0..200i64 {
            for r in 0..3u64 {
                tree.insert(
                    &pager,
                    &mut tx,
                    Value::Int(i / 3),
                    (i as u64) * 10 + r,
                    false,
                )
                .unwrap();
            }
        }
        pager.commit_tx(tx).unwrap();
        let tx = pager.begin_tx();
        let reader = PageReader::current(&pager);
        let full = tree.scan(&reader, &tx).unwrap();
        let check_rev =
            |lo: Option<(&Value, bool)>, hi: Option<(&Value, bool)>, max: Option<usize>| {
                let mut want: Vec<(Value, u64)> = full
                    .iter()
                    .filter(|(k, _)| !BTree::below_lo(k, lo) && !BTree::beyond_hi(k, hi))
                    .cloned()
                    .collect();
                want.reverse();
                if let Some(m) = max {
                    want.truncate(m);
                }
                let got = tree
                    .range_bounded_rev_limited(&reader, &tx, lo, hi, max)
                    .unwrap();
                assert_eq!(got, want, "rev lo={lo:?} hi={hi:?} max={max:?}");
            };
        let v10 = Value::Int(10);
        let v33 = Value::Int(33);
        let v50 = Value::Int(50);
        for max in [None, Some(0), Some(1), Some(7), Some(10_000)] {
            check_rev(None, None, max);
            check_rev(Some((&v10, true)), None, max);
            check_rev(Some((&v10, false)), None, max);
            check_rev(None, Some((&v50, true)), max);
            check_rev(None, Some((&v50, false)), max);
            check_rev(Some((&v10, false)), Some((&v50, true)), max);
            check_rev(Some((&v10, true)), Some((&v50, false)), max);
            check_rev(Some((&v33, true)), Some((&v33, true)), max);
            check_rev(Some((&v33, false)), Some((&v33, false)), max);
        }
        // Forward exclusive walk with a cap keeps the same entry set.
        for max in [None, Some(1), Some(7)] {
            let mut want: Vec<(Value, u64)> = full
                .iter()
                .filter(|(k, _)| {
                    !BTree::below_lo(k, Some((&v10, false)))
                        && !BTree::beyond_hi(k, Some((&v50, true)))
                })
                .cloned()
                .collect();
            if let Some(m) = max {
                want.truncate(m);
            }
            let got = tree
                .range_bounded_excl_limited(&reader, &tx, &v10, Some((&v50, true)), max)
                .unwrap();
            assert_eq!(got, want, "fwd max={max:?}");
        }
    }

    #[test]
    fn ordered_scan_across_splits() {
        let (_d, pager) = fresh("bt3.db");
        let mut tx = pager.begin_tx();
        let mut tree = BTree::create(&pager, &mut tx).unwrap();
        // Descending insert forces many splits.
        for i in (0..500i64).rev() {
            tree.insert(&pager, &mut tx, Value::Int(i), i as u64, true)
                .unwrap();
        }
        pager.commit_tx(tx).unwrap();
        let tx = pager.begin_tx();
        let all = tree.scan(&PageReader::current(&pager), &tx).unwrap();
        assert_eq!(all.len(), 500);
        for (i, (k, v)) in all.iter().enumerate() {
            assert_eq!(*k, Value::Int(i as i64));
            assert_eq!(*v, i as u64);
        }
    }

    #[test]
    fn delete_then_lookup() {
        let (_d, pager) = fresh("bt4.db");
        let mut tx = pager.begin_tx();
        let mut tree = BTree::create(&pager, &mut tx).unwrap();
        for i in 0..200i64 {
            tree.insert(&pager, &mut tx, Value::Int(i), i as u64, true)
                .unwrap();
        }
        for i in (0..200i64).step_by(2) {
            assert!(tree.delete(&pager, &mut tx, &Value::Int(i)).unwrap());
        }
        assert!(!tree.delete(&pager, &mut tx, &Value::Int(0)).unwrap()); // already gone
        pager.commit_tx(tx).unwrap();
        let tx = pager.begin_tx();
        for i in 0..200i64 {
            let expect = if i % 2 == 0 { None } else { Some(i as u64) };
            assert_eq!(
                tree.get(&PageReader::current(&pager), &tx, &Value::Int(i))
                    .unwrap(),
                expect
            );
        }
    }

    #[test]
    fn survives_reopen_via_pager() {
        let (d, pager) = fresh("bt5.db");
        let root;
        {
            let mut tx = pager.begin_tx();
            let mut tree = BTree::create(&pager, &mut tx).unwrap();
            for i in 0..300i64 {
                tree.insert(&pager, &mut tx, Value::Int(i), i as u64, true)
                    .unwrap();
            }
            root = tree.root;
            pager.commit_tx(tx).unwrap();
        }
        drop(pager);
        let pager = Pager::open(&d.path().join("bt5.db")).unwrap();
        let tree = BTree::open(root);
        let tx = pager.begin_tx();
        assert_eq!(
            tree.get(&PageReader::current(&pager), &tx, &Value::Int(299))
                .unwrap(),
            Some(299)
        );
        assert_eq!(
            tree.get(&PageReader::current(&pager), &tx, &Value::Int(0))
                .unwrap(),
            Some(0)
        );
    }

    #[test]
    fn range_from_finds_suffix_in_order() {
        let (_d, pager) = fresh("bt7.db");
        let mut tx = pager.begin_tx();
        let mut tree = BTree::create(&pager, &mut tx).unwrap();
        for i in 0..300i64 {
            tree.insert(&pager, &mut tx, Value::Int(i), i as u64, true)
                .unwrap();
        }
        pager.commit_tx(tx).unwrap();
        let tx = pager.begin_tx();
        let got = tree
            .range_from(&PageReader::current(&pager), &tx, &Value::Int(295))
            .unwrap();
        let keys: Vec<i64> = got.iter().map(|(k, _)| k.as_i64().unwrap()).collect();
        assert_eq!(keys, vec![295, 296, 297, 298, 299]);
        // Below everything → whole tree; above everything → empty.
        assert_eq!(
            tree.range_from(&PageReader::current(&pager), &tx, &Value::Int(-1))
                .unwrap()
                .len(),
            300
        );
        assert!(tree
            .range_from(&PageReader::current(&pager), &tx, &Value::Int(300))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn delete_entry_removes_exact_pair() {
        let (_d, pager) = fresh("bt8.db");
        let mut tx = pager.begin_tx();
        let mut tree = BTree::create(&pager, &mut tx).unwrap();
        // Same key, three locators (non-unique index; inserts append).
        for loc in [10u64, 20, 30] {
            tree.insert(&pager, &mut tx, Value::Str("k".into()), loc, false)
                .unwrap();
        }
        // Non-unique inserts append separate entries per locator.
        assert_eq!(
            tree.scan(&PageReader::current(&pager), &tx).unwrap().len(),
            3
        );
        assert!(tree
            .delete_entry(&pager, &mut tx, &Value::Str("k".into()), 20)
            .unwrap());
        let left: Vec<u64> = tree
            .scan(&PageReader::current(&pager), &tx)
            .unwrap()
            .into_iter()
            .map(|(_, v)| v)
            .collect();
        assert_eq!(left, vec![10, 30]);
        assert!(!tree
            .delete_entry(&pager, &mut tx, &Value::Str("k".into()), 20)
            .unwrap());
        pager.commit_tx(tx).unwrap();
    }

    /// Property test against BTreeMap: interleaved random-ish ops must agree.
    #[test]
    fn matches_btreemap_under_lcg_ops() {
        let (_d, pager) = fresh("bt6.db");
        let mut tx = pager.begin_tx();
        let mut tree = BTree::create(&pager, &mut tx).unwrap();
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
                    let res = tree.insert(&pager, &mut tx, Value::Int(k), v, true);
                    match (model.contains_key(&k), res) {
                        (true, Err(BTreeError::Duplicate)) => {}
                        (false, Ok(())) => {
                            model.insert(k, v);
                        }
                        (_, r) => panic!("unexpected insert result for k={k}: {r:?}"),
                    }
                }
                _ => {
                    let got = tree.delete(&pager, &mut tx, &Value::Int(k)).unwrap();
                    let expect = model.remove(&k).is_some();
                    assert_eq!(got, expect);
                }
            }
        }
        pager.commit_tx(tx).unwrap();
        let tx = pager.begin_tx();
        for (k, v) in &model {
            assert_eq!(
                tree.get(&PageReader::current(&pager), &tx, &Value::Int(*k))
                    .unwrap(),
                Some(*v),
                "key {k}"
            );
        }
        let scanned = tree.scan(&PageReader::current(&pager), &tx).unwrap();
        assert_eq!(scanned.len(), model.len());
        for ((k, v), (mk, mv)) in scanned.iter().zip(model.iter()) {
            assert_eq!(k.as_i64(), Some(*mk));
            assert_eq!(*v, *mv);
        }
    }
    // ---- 覆盖率补充:节点分裂 / 超大键 / 越界写 ----

    #[test]
    fn tree_splits_under_load_and_range_reads_all() {
        let (_d, pager) = fresh("bt_split.db");
        let mut tx = pager.begin_tx();
        let mut tree = BTree::create(&pager, &mut tx).unwrap();
        // 大量插入迫使叶节点多次分裂
        for i in 0..2000i64 {
            tree.insert(&pager, &mut tx, Value::Int(i), i as u64, false)
                .unwrap();
        }
        pager.commit_tx(tx).unwrap();
        let tx = pager.begin_tx();
        let all = tree
            .range_from(&PageReader::current(&pager), &tx, &Value::Int(0))
            .unwrap();
        assert_eq!(all.len(), 2000);
        assert_eq!(all[0].0, Value::Int(0));
        assert_eq!(all[1999].0, Value::Int(1999));
        pager.abort_tx(tx).unwrap();
    }

    #[test]
    fn oversized_key_rejected() {
        let (_d, pager) = fresh("bt_bigkey.db");
        let mut tx = pager.begin_tx();
        let mut tree = BTree::create(&pager, &mut tx).unwrap();
        let big = Value::Str("K".repeat(PAGE_SIZE));
        let e = tree.insert(&pager, &mut tx, big, 1, false).unwrap_err();
        assert!(matches!(e, BTreeError::KeyTooLarge(_)), "{e:?}");
        pager.abort_tx(tx).unwrap();
    }
    #[test]
    fn delete_through_internal_nodes() {
        let (_d, pager) = fresh("bt_del.db");
        let mut tx = pager.begin_tx();
        let mut tree = BTree::create(&pager, &mut tx).unwrap();
        for i in 0..1500i64 {
            tree.insert(&pager, &mut tx, Value::Int(i), i as u64, false)
                .unwrap();
        }
        pager.commit_tx(tx).unwrap();
        // 删除一半,迫使删除路径下沉经过内部节点
        for i in (0..1500i64).step_by(2) {
            let mut tx = pager.begin_tx();
            tree.delete_entry(&pager, &mut tx, &Value::Int(i), i as u64)
                .unwrap();
            pager.commit_tx(tx).unwrap();
        }
        let tx = pager.begin_tx();
        let all = tree
            .range_from(&PageReader::current(&pager), &tx, &Value::Int(0))
            .unwrap();
        assert_eq!(all.len(), 750);
        assert!(all.iter().all(|(k, _)| k.as_i64().unwrap() % 2 == 1));
        pager.abort_tx(tx).unwrap();
    }

    #[test]
    fn medium_string_keys_split_by_bytes() {
        // ~90 字节编码的键在 ~43 个时就会占满一页——远低于任何条数阈值;
        // 分裂必须按字节驱动,否则这些完全合法的键会让索引建不起来。
        let (_d, pager) = fresh("bt_medkey.db");
        let mut tx = pager.begin_tx();
        let mut tree = BTree::create(&pager, &mut tx).unwrap();
        let key = |i: i64| Value::Str(format!("k{i:06}-{}", "x".repeat(80)));
        for i in 0..200i64 {
            tree.insert(&pager, &mut tx, key(i), i as u64, true)
                .unwrap();
        }
        pager.commit_tx(tx).unwrap();
        let tx = pager.begin_tx();
        for i in 0..200i64 {
            assert_eq!(
                tree.get(&PageReader::current(&pager), &tx, &key(i))
                    .unwrap(),
                Some(i as u64),
                "key {i}"
            );
        }
        let all = tree.scan(&PageReader::current(&pager), &tx).unwrap();
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
        let (_d, pager) = fresh("bt_eqdel.db");
        let mut tx = pager.begin_tx();
        let mut tree = BTree::create(&pager, &mut tx).unwrap();
        for i in 0..500i64 {
            let k = i % 10;
            tree.insert(&pager, &mut tx, Value::Int(k), i as u64, false)
                .unwrap();
        }
        pager.commit_tx(tx).unwrap();
        for i in 0..500i64 {
            let mut tx = pager.begin_tx();
            let k = Value::Int(i % 10);
            assert!(
                tree.delete_entry(&pager, &mut tx, &k, i as u64).unwrap(),
                "entry (k={}, loc={i}) must be found",
                i % 10
            );
            pager.commit_tx(tx).unwrap();
        }
        let tx = pager.begin_tx();
        assert!(
            tree.scan(&PageReader::current(&pager), &tx)
                .unwrap()
                .is_empty(),
            "no ghost entries may survive"
        );
        pager.abort_tx(tx).unwrap();
    }
}
