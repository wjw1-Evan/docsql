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
/// Max keys per node — chosen so worst-case cells (long string keys are
// rejected as oversized) always fit in a page.
const MAX_KEYS: usize = 64;

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
        let page = match tx.staged_page(id) {
            Some(p) => p.to_vec(),
            None => pager.read_page(id)?.to_vec(),
        };
        let kind = page[0];
        let count = u16::from_le_bytes([page[1], page[2]]) as usize;
        match kind {
            LEAF => {
                let mut cells = Vec::with_capacity(count);
                let mut pos = 3;
                for _ in 0..count {
                    let klen = u16::from_le_bytes(page[pos..pos + 2].try_into().unwrap()) as usize;
                    pos += 2;
                    let (k, used) = encode::decode_prefix(&page[pos..pos + klen])?;
                    pos += used;
                    let val = u64::from_le_bytes(page[pos..pos + 8].try_into().unwrap());
                    pos += 8;
                    cells.push((k, val));
                }
                Ok(Node::Leaf { cells })
            }
            INTERNAL => {
                let leftmost = u32::from_le_bytes(page[3..7].try_into().unwrap());
                let mut pos = 7;
                let mut cells = Vec::with_capacity(count);
                for _ in 0..count {
                    let klen = u16::from_le_bytes(page[pos..pos + 2].try_into().unwrap()) as usize;
                    pos += 2;
                    let (k, used) = encode::decode_prefix(&page[pos..pos + klen])?;
                    pos += used;
                    let child = u32::from_le_bytes(page[pos..pos + 4].try_into().unwrap());
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
        let mut id = self.root;
        loop {
            match Self::read_node(pager, tx, id)? {
                Node::Leaf { cells } => {
                    return Ok(cells
                        .iter()
                        .find(|(k, _)| Value::cmp_values(k, key) == Ordering::Equal)
                        .map(|(_, v)| *v));
                }
                Node::Internal { leftmost, cells } => {
                    id = descend(&cells, leftmost, key);
                }
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
                let at = cells
                    .binary_search_by(|(k, _)| Value::cmp_values(k, key))
                    .unwrap_or_else(|i| i);
                if at < cells.len() && Value::cmp_values(&cells[at].0, key) == Ordering::Equal {
                    if unique {
                        return Err(BTreeError::Duplicate);
                    }
                    cells[at].1 = val;
                    Self::write_node(ctx.pager, ctx.tx, id, &Node::Leaf { cells })?;
                    return Ok(None);
                }
                cells.insert(at, (key.clone(), val));
                if cells.len() <= MAX_KEYS {
                    Self::write_node(ctx.pager, ctx.tx, id, &Node::Leaf { cells })?;
                    Ok(None)
                } else {
                    let mid = cells.len() / 2;
                    let mid_key = cells[mid].0.clone();
                    let right_cells = cells.split_off(mid);
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
                    let at = cells
                        .binary_search_by(|(k, _)| Value::cmp_values(k, &mid))
                        .unwrap_err();
                    cells.insert(at, (mid, right));
                    if cells.len() <= MAX_KEYS {
                        Self::write_node(
                            ctx.pager,
                            ctx.tx,
                            id,
                            &Node::Internal { leftmost, cells },
                        )?;
                        Ok(None)
                    } else {
                        let m = cells.len() / 2;
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
                let child = descend(&cells, leftmost, key);
                Self::delete_rec(pager, tx, child, key)
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
    fn unique_violation_and_overwrite() {
        let (_d, mut pager) = fresh("bt2.db");
        let mut tx = pager.begin_tx();
        let mut tree = BTree::create(&mut pager, &mut tx).unwrap();
        tree.insert(&mut pager, &mut tx, Value::Str("k".into()), 1, true)
            .unwrap();
        assert!(matches!(
            tree.insert(&mut pager, &mut tx, Value::Str("k".into()), 2, true),
            Err(BTreeError::Duplicate)
        ));
        // non-unique overwrites
        tree.insert(&mut pager, &mut tx, Value::Str("k".into()), 2, false)
            .unwrap();
        pager.commit_tx(tx).unwrap();
        let tx = pager.begin_tx();
        assert_eq!(
            tree.get(&mut pager, &tx, &Value::Str("k".into())).unwrap(),
            Some(2)
        );
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
}
