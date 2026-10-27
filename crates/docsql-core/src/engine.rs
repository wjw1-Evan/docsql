//! SQL engine: catalog + executor over document heaps.
//!
//! The catalog itself is a document stored on page 1:
//! `{ "tables": { "<name>": { "columns": [names...], "pages": [...] } } }`
//! so metadata rides the same WAL/pager machinery as data.

use crate::btree::{BTree, BTreeError};
use crate::encode;
use crate::heap::Heap;
use crate::pager::{PageReader, Pager, PagerError, Snapshot, PAGE_SIZE};
use crate::value::{Decimal, Object, Value};
use num_traits::ToPrimitive;
use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, Function, LimitClause, ObjectName, ObjectNamePart, Query,
    SelectItem, SetExpr, Statement, TableObject,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::cmp::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub struct Database {
    pager: std::sync::Arc<Pager>,
    tables: std::collections::BTreeMap<String, std::sync::Arc<TableMeta>>,
    /// Row triggers by name (see [`TriggerDef`]). Only the write path ever
    /// reads this map, always under the engine write lock — no snapshot
    /// plumbing (unlike `tables`, catalog snapshots don't carry triggers;
    /// trigger bodies never read triggers).
    triggers: std::collections::BTreeMap<String, TriggerDef>,
    /// Trigger-body execution depth (> 0 = a triggering statement is
    /// mid-flight under its statement guard; nested DML joins that guard
    /// instead of opening its own).
    trigger_depth: u8,
    /// Async-commit mode (MongoDB-style journal interval): every statement
    /// commit skips the WAL fsync; a background flusher batches fsyncs.
    /// Trades bounded (ms) loss window on power failure for throughput.
    async_commit: bool,
    /// True when deferred commits are waiting for a WAL fsync.
    pending_sync: bool,
    /// Open fused write units (see [`Database::write_unit`]); depth-counted
    /// because the guard borrows the database. While non-zero, statement
    /// commits defer their WAL fsync to the unit's `end`, so a statement and
    /// its replication bookkeeping (journal append / position update) share
    /// one fsync and are atomic on crash.
    write_unit: u8,
    /// SAVEPOINT stack inside the open transaction: (name, snapshot, undo
    /// journal mark). Each ROLLBACK TO restores its snapshot and drops
    /// everything above it, including the journal tail.
    savepoints: Vec<(String, TableSnapshot, usize)>,
    /// Session transaction snapshot: full table state at BEGIN. Rollback
    /// restores it; commit just discards it (durability is the WAL's job).
    tx_snapshot: Option<TableSnapshot>,
    /// Cooperative deadline for the statement currently executing (see
    /// [`StmtDeadline`]); armed per client statement by the server, never
    /// set for replication apply / restore replay.
    stmt_deadline: StmtDeadline,
    /// Lazily-computed AUTOINCREMENT next id per table (max(id)+1 over the
    /// heap). Invalidated by rewrites/deletes; not persisted — recomputed
    /// after restart, preserving max(existing)+1 semantics.
    autoinc_cache: std::collections::HashMap<String, i64>,
    /// Canonical rewrite of the last executed statement when it auto-filled
    /// GUID values (explicit generated ids embedded). Replicating callers
    /// must forward this text instead of the original: random GUIDs cannot
    /// be re-derived on peers the way AUTOINCREMENT's deterministic max+1
    /// can. Reset at the start of every statement; see `take_resolved_sql`.
    resolved_sql: Option<String>,
    /// AUTOINCREMENT id assigned by the last INSERT (max over inserted
    /// rows; explicit values count). Lets callers that must know the
    /// assigned id — protocol INSERTs, the pub/sub store — read it back
    /// without a second `SELECT MAX(id)` statement, which is a full heap
    /// scan. Reset at the start of every statement.
    last_insert_id: Option<i64>,
    /// Next seq for the catch-up journal ([`Database::journal_append`]).
    /// Seeded lazily from MAX(seq)+1 (in-memory counter under the write
    /// lock; gaps from failed statements are harmless — positions pull
    /// ranges, not counts).
    journal_next: Option<u64>,
    /// Running total of journal SQL bytes (None = recompute lazily with one
    /// SUM scan on the next [`Database::journal_trim_bytes`]). Drives the
    /// byte cap: a count-only window lets a big-document workload grow
    /// `_cluster_log` without bound (single documents reach 16 MiB).
    journal_bytes: Option<u64>,
    /// This node's persistent random identity (see CLUSTER_ID_TABLE),
    /// cached after first read.
    cluster_id: Option<String>,
    /// Set only while `ensure_user_tables` runs its own DDL: the reserved
    /// user/role table names are otherwise rejected in CREATE TABLE so
    /// clients cannot shadow them.
    pub(crate) internal_ddl: bool,
}

impl Database {
    /// Commit a statement's pager tx: while an explicit SQL transaction or a
    /// fused write unit is open the WAL fsync is deferred — the whole batch
    /// pays one flush at COMMIT / [`WriteUnit::end`], turning N fsyncs per
    /// transaction into one.
    fn commit_pager_tx(&mut self, tx: crate::pager::Tx) -> Result<u64> {
        if self.async_commit || self.tx_snapshot.is_some() || self.write_unit > 0 {
            self.pending_sync = true;
            self.pager.commit_tx_deferred(tx).map_err(SqlError::from)
        } else {
            // The synchronous commit fsyncs the WAL and flushes every page
            // image queued before it, so earlier deferred commits are durable
            // too — nothing is left pending. On failure the flag stays set:
            // the earlier deferred commits may not be durable yet.
            let result = self.pager.commit_tx(tx).map_err(SqlError::from);
            if result.is_ok() {
                self.pending_sync = false;
            }
            result
        }
    }

    /// Open fused write unit: every statement committed until
    /// [`WriteUnit::end`] defers its WAL fsync, and the unit's single `end`
    /// fsync makes them durable together — a crash mid-unit loses them all,
    /// never a prefix. Servers use this to land a write and its replication
    /// bookkeeping (catch-up journal entry on the origin, position update on
    /// the receiver) in one fsync instead of two, closing the
    /// "bookkeeping lags the data" crash window entirely.
    ///
    /// Unlike BEGIN it takes no rollback snapshot; a failed trailing
    /// statement leaves the earlier ones applied-but-unsynced, so callers
    /// that want the prefix to survive must call `end` on error paths too.
    /// Dropping the guard without `end` only forgets the fusion (the flag
    /// clears on drop; the next durable statement fsyncs anything deferred
    /// before it), so a leaked unit can never wedge durability.
    pub fn write_unit(&mut self) -> WriteUnit<'_> {
        self.write_unit += 1;
        WriteUnit { db: self }
    }

    /// Enable async-commit mode: statement commits skip the WAL fsync and a
    /// caller-provided flusher (see `sync_pending`) batches them.
    pub fn set_async_commit(&mut self, on: bool) {
        self.async_commit = on;
    }

    /// True when deferred commits still need a WAL fsync.
    pub fn has_pending_sync(&self) -> bool {
        self.pending_sync
    }

    /// Flush deferred commits (the background flusher's entry point).
    pub fn sync_pending(&mut self) -> Result<()> {
        if !self.pending_sync {
            return Ok(());
        }
        if self.tx_snapshot.is_some() {
            // Mid-transaction: these deferred commits belong to an open SQL
            // transaction. Appending the durable fence now would make a
            // crash replay a prefix of it; the transaction's own COMMIT
            // fences the whole batch, ROLLBACK discards it.
            return Ok(());
        }
        self.pager.sync_wal().map_err(SqlError::from)?;
        self.pending_sync = false;
        Ok(())
    }
    pub fn open(path: &std::path::Path) -> Result<Database> {
        let pager = Pager::open(path)?;
        // Reserve pages 0 (header) and 1 (catalog); allocate 1 if missing.
        while pager.num_pages() <= CATALOG_PAGE {
            let mut tx = pager.begin_tx();
            let _ = pager.num_pages();
            pager.allocate_page(&mut tx)?;
            pager.commit_tx(tx)?;
        }
        let mut tables = std::collections::BTreeMap::new();
        let mut triggers = std::collections::BTreeMap::new();
        // New format: a magic-headed chain of pages. Legacy databases keep a
        // raw encoded catalog on page 1 — decode it in place; the first
        // catalog save rewrites it in the new format.
        let raw = pager.read_page(CATALOG_PAGE)?.to_vec();
        let cat_bytes: Option<Vec<u8>> = if raw.starts_with(CATALOG_MAGIC) {
            let total = u32::from_le_bytes(raw[8..12].try_into().expect("header fits")) as usize;
            let mut data = raw[CATALOG_HDR..].to_vec();
            let mut next = u32::from_le_bytes(raw[12..16].try_into().expect("header fits"));
            // A cyclic chain of valid page ids would loop forever appending
            // 4 KB per hop, so bound the walk by the page count.
            let mut hops = 0usize;
            while next != 0 {
                if next >= pager.num_pages() {
                    return err("catalog chain is corrupt (dangling overflow page)");
                }
                hops += 1;
                if hops > pager.num_pages() as usize {
                    return err("catalog chain is corrupt (cycle)");
                }
                let page = pager.read_page(next)?.to_vec();
                if !page.starts_with(CATALOG_MAGIC) {
                    return err("catalog chain is corrupt (overflow page without magic)");
                }
                data.extend_from_slice(&page[CATALOG_HDR..]);
                next = u32::from_le_bytes(page[12..16].try_into().expect("header fits"));
            }
            if data.len() < total {
                return err("catalog chain is corrupt (payload shorter than header length)");
            }
            data.truncate(total);
            Some(data)
        } else if raw.iter().any(|&b| b != 0) {
            Some(raw)
        } else {
            None
        };
        if let Some(bytes) = cat_bytes {
            let (v, _) = encode::decode_prefix(&bytes)?;
            if let Value::Object(o) = v {
                if let Some(Value::Object(t)) = o.get("tables") {
                    for (name, meta) in t {
                        if let Value::Object(m) = meta {
                            let columns = match m.get("columns") {
                                Some(Value::Array(a)) => a
                                    .iter()
                                    .filter_map(|x| x.as_str().map(String::from))
                                    .collect(),
                                _ => vec![],
                            };
                            let pages = match m.get("pages") {
                                Some(Value::Array(a)) => a
                                    .iter()
                                    .filter_map(|x| x.as_i64().map(|i| i as u32))
                                    .collect(),
                                _ => vec![],
                            };
                            let primary_key = m
                                .get("primary_key")
                                .and_then(|v| v.as_str())
                                .map(String::from);
                            let unique = str_list(m, "unique");
                            let not_null = str_list(m, "not_null");
                            let autoinc =
                                m.get("autoinc").and_then(|v| v.as_str()).map(String::from);
                            let autoguid =
                                m.get("autoguid").and_then(|v| v.as_str()).map(String::from);
                            let view_sql =
                                m.get("view_sql").and_then(|v| v.as_str()).map(String::from);
                            let matview_sql = m
                                .get("matview_sql")
                                .and_then(|v| v.as_str())
                                .map(String::from);
                            let index_defs = match m.get("index_defs") {
                                Some(Value::Array(a)) => a
                                    .iter()
                                    .filter_map(|x| match x {
                                        Value::Array(t) => match (
                                            t.first().and_then(|v| v.as_str()),
                                            t.get(1).and_then(|v| v.as_str()),
                                            t.get(2).and_then(|v| v.as_bool()),
                                        ) {
                                            (Some(n), Some(c), Some(u)) => {
                                                // Fourth element (optional,
                                                // new-format catalogs): the
                                                // full ordered column list of
                                                // a possibly composite index.
                                                // Legacy three-element entries
                                                // are single-column.
                                                let columns = match t.get(3) {
                                                    Some(Value::Array(a)) => {
                                                        // An EMPTY array must
                                                        // not survive: IndexDef
                                                        // assumes columns
                                                        // non-empty (`column()`
                                                        // indexes [0]) and
                                                        // panics on the next
                                                        // catalog save.
                                                        let cols: Vec<_> = a
                                                            .iter()
                                                            .filter_map(|v| {
                                                                v.as_str().map(String::from)
                                                            })
                                                            .collect();
                                                        if cols.is_empty() {
                                                            vec![c.to_string()]
                                                        } else {
                                                            cols
                                                        }
                                                    }
                                                    _ => vec![c.to_string()],
                                                };
                                                // Optional 5th element (design
                                                // 005): the JSON path. Older
                                                // catalogs lack it; readers
                                                // that predate path indexes
                                                // ignore the extra element, so
                                                // the tuple stays bidirectionally
                                                // compatible with zero migration.
                                                let path = t
                                                    .get(4)
                                                    .and_then(|v| v.as_str())
                                                    .map(String::from);
                                                // Optional 6th element: the
                                                // partial-index predicate text.
                                                let pred = t
                                                    .get(5)
                                                    .and_then(|v| v.as_str())
                                                    .map(String::from);
                                                Some(IndexDef {
                                                    name: n.to_string(),
                                                    columns,
                                                    unique: u,
                                                    path,
                                                    pred,
                                                })
                                            }
                                            _ => None,
                                        },
                                        _ => None,
                                    })
                                    .collect(),
                                _ => vec![],
                            };
                            // Index names derive from the persisted defs.
                            let indexes = index_defs
                                .iter()
                                .map(|d| d.name.clone())
                                .collect::<Vec<_>>();
                            let constraint_unique = str_list(m, "constraint_unique");
                            let defaults = match m.get("defaults") {
                                Some(Value::Array(a)) => a
                                    .iter()
                                    .filter_map(|x| match x {
                                        Value::Array(pair) => match (
                                            pair.first().and_then(|c| c.as_str()),
                                            pair.get(1).and_then(|e| e.as_str()),
                                        ) {
                                            (Some(c), Some(e)) => {
                                                Some((c.to_string(), e.to_string()))
                                            }
                                            _ => None,
                                        },
                                        _ => None,
                                    })
                                    .collect(),
                                _ => vec![],
                            };
                            let checks = str_list(m, "checks");
                            let index_roots = match m.get("index_roots") {
                                Some(Value::Object(o)) => o
                                    .iter()
                                    .filter_map(|(c, v)| v.as_i64().map(|i| (c.clone(), i as u32)))
                                    .collect(),
                                _ => std::collections::BTreeMap::new(),
                            };
                            // Recycled overflow-chain pages (optional key:
                            // catalogs predating overflow documents lack it).
                            let overflow_free = match m.get("overflow_free") {
                                Some(Value::Array(a)) => a
                                    .iter()
                                    .filter_map(|v| v.as_i64().map(|i| i as u32))
                                    .collect(),
                                _ => vec![],
                            };
                            let foreign_keys = match m.get("foreign_keys") {
                                Some(Value::Array(a)) => a
                                    .iter()
                                    .filter_map(|x| match x {
                                        Value::Array(t) => match (
                                            t.first().and_then(|v| v.as_str()),
                                            t.get(1).and_then(|v| v.as_str()),
                                            t.get(2).and_then(|v| v.as_str()),
                                        ) {
                                            (Some(c), Some(rt), Some(rc)) => Some((
                                                c.to_string(),
                                                rt.to_string(),
                                                rc.to_string(),
                                            )),
                                            _ => None,
                                        },
                                        _ => None,
                                    })
                                    .collect(),
                                _ => vec![],
                            };
                            tables.insert(
                                name.clone(),
                                std::sync::Arc::new(TableMeta {
                                    name: name.clone(),
                                    columns,
                                    pages,
                                    primary_key,
                                    unique,
                                    not_null,
                                    autoinc,
                                    autoguid,
                                    view_sql,
                                    matview_sql,
                                    indexes,
                                    index_defs,
                                    constraint_unique,
                                    defaults,
                                    checks,
                                    foreign_keys,
                                    index_roots,
                                    overflow_free,
                                    dotted_keys: matches!(
                                        m.get("dotted_keys"),
                                        Some(Value::Bool(true))
                                    ),
                                }),
                            );
                        }
                    }
                }
                // Row triggers (see [`TriggerDef`]). Same tolerant decode as
                // the table metas: a malformed entry is skipped, not fatal —
                // the encoder above only ever writes the shapes decoded here.
                if let Some(Value::Object(ts)) = o.get("triggers") {
                    for (name, td) in ts {
                        let Some(Value::Object(t)) = Some(td) else {
                            continue;
                        };
                        let (Some(table), Some(before), Some(insert), Some(delete), Some(update)) = (
                            t.get("table").and_then(|v| v.as_str()),
                            t.get("before").and_then(|v| v.as_bool()),
                            t.get("insert").and_then(|v| v.as_bool()),
                            t.get("delete").and_then(|v| v.as_bool()),
                            t.get("update").and_then(|v| v.as_bool()),
                        ) else {
                            continue;
                        };
                        let update_of = match t.get("update_of") {
                            Some(Value::Array(a)) => Some(
                                a.iter()
                                    .filter_map(|v| v.as_str().map(String::from))
                                    .collect(),
                            ),
                            _ => None,
                        };
                        let when = t.get("when").and_then(|v| v.as_str()).map(String::from);
                        let body = match t.get("body") {
                            Some(Value::Array(a)) => a
                                .iter()
                                .filter_map(|x| match x {
                                    Value::Array(parts) => match (
                                        parts.first().and_then(|v| v.as_str()),
                                        parts.get(1).and_then(|v| v.as_str()),
                                    ) {
                                        (Some("sql"), Some(text)) => {
                                            Some(TriggerAction::Sql(text.to_string()))
                                        }
                                        (Some("raise"), Some(msg)) => Some(TriggerAction::Raise {
                                            fail: matches!(parts.get(2), Some(Value::Bool(true))),
                                            msg: msg.to_string(),
                                        }),
                                        _ => None,
                                    },
                                    _ => None,
                                })
                                .collect(),
                            _ => vec![],
                        };
                        triggers.insert(
                            name.clone(),
                            TriggerDef {
                                name: name.clone(),
                                table: table.to_string(),
                                before,
                                insert,
                                delete,
                                update,
                                update_of,
                                when,
                                body,
                            },
                        );
                    }
                }
            }
        }
        Ok(Database {
            pager: std::sync::Arc::new(pager),
            tables,
            triggers,
            trigger_depth: 0,
            async_commit: false,
            pending_sync: false,
            write_unit: 0,
            savepoints: Vec::new(),
            tx_snapshot: None,
            stmt_deadline: StmtDeadline::default(),
            autoinc_cache: std::collections::HashMap::new(),
            resolved_sql: None,
            last_insert_id: None,
            journal_next: None,
            journal_bytes: None,
            cluster_id: None,
            internal_ddl: false,
        })
    }

    pub fn in_memory() -> Result<Database> {
        let dir = tempfile::tempdir().map_err(|e| SqlError::Message(e.to_string()))?;
        // Leak: process-lifetime scratch DB, fine for embedded use/tests.
        let path = dir.keep().join("mem.db");
        Database::open(&path)
    }

    /// Record a statement's resulting heap layout and index roots in the
    /// catalog inside `tx`. On save failure the catalog map is restored to
    /// the pre-statement truth: the tx is dropped on that path, so its
    /// staged pages never reach the data file, and a map left pointing at
    /// them would let a later successful save persist ghost pages.
    fn sync_table_layout(
        &mut self,
        tx: &mut crate::pager::Tx,
        tname: &str,
        pages: Vec<u32>,
        roots: std::collections::BTreeMap<String, u32>,
        overflow_free: Vec<u32>,
    ) -> Result<()> {
        let prev = self.tables.get(tname).map(|m| {
            (
                m.pages.clone(),
                m.index_roots.clone(),
                m.overflow_free.clone(),
            )
        });
        if let Some(m) = self.catalog_mut(tname) {
            m.pages = pages;
            m.index_roots = roots;
            m.overflow_free = overflow_free;
        }
        if let Err(e) = self.save_catalog_into(tx) {
            if let Some((pages, index_roots, overflow_free)) = prev {
                if let Some(m) = self.catalog_mut(tname) {
                    m.pages = pages;
                    m.index_roots = index_roots;
                    // Restoring the free list matters as much as the page
                    // list: leaving recycled chain pages in it would let a
                    // later oversized insert overwrite live documents.
                    m.overflow_free = overflow_free;
                }
            }
            return Err(e);
        }
        Ok(())
    }

    fn save_catalog_into(&mut self, tx: &mut crate::pager::Tx) -> Result<()> {
        let mut tables = Object::new();
        for (name, meta) in &self.tables {
            let mut m = Object::new();
            m.insert(
                "columns".into(),
                Value::Array(meta.columns.iter().map(|c| Value::Str(c.clone())).collect()),
            );
            m.insert(
                "pages".into(),
                Value::Array(meta.pages.iter().map(|p| Value::Int(*p as i64)).collect()),
            );
            if let Some(pk) = &meta.primary_key {
                m.insert("primary_key".into(), Value::Str(pk.clone()));
            }
            // AUTOINCREMENT column must survive reopen: dropping it silently
            // turns the column into a plain nullable INT (inserts stop
            // assigning ids after a restart).
            if let Some(col) = &meta.autoinc {
                m.insert("autoinc".into(), Value::Str(col.clone()));
            }
            // Auto-generated GUID column must survive reopen for the same
            // reason as AUTOINCREMENT.
            if let Some(col) = &meta.autoguid {
                m.insert("autoguid".into(), Value::Str(col.clone()));
            }
            if let Some(sql) = &meta.view_sql {
                m.insert("view_sql".into(), Value::Str(sql.clone()));
            }
            if !meta.unique.is_empty() {
                m.insert(
                    "unique".into(),
                    Value::Array(meta.unique.iter().map(|c| Value::Str(c.clone())).collect()),
                );
            }
            if !meta.not_null.is_empty() {
                m.insert(
                    "not_null".into(),
                    Value::Array(
                        meta.not_null
                            .iter()
                            .map(|c| Value::Str(c.clone()))
                            .collect(),
                    ),
                );
            }
            if !meta.defaults.is_empty() {
                m.insert(
                    "defaults".into(),
                    Value::Array(
                        meta.defaults
                            .iter()
                            .map(|(c, e)| {
                                Value::Array(vec![Value::Str(c.clone()), Value::Str(e.clone())])
                            })
                            .collect(),
                    ),
                );
            }
            if !meta.checks.is_empty() {
                m.insert(
                    "checks".into(),
                    Value::Array(meta.checks.iter().map(|c| Value::Str(c.clone())).collect()),
                );
            }
            if !meta.foreign_keys.is_empty() {
                m.insert(
                    "foreign_keys".into(),
                    Value::Array(
                        meta.foreign_keys
                            .iter()
                            .map(|(c, rt, rc)| {
                                Value::Array(vec![
                                    Value::Str(c.clone()),
                                    Value::Str(rt.clone()),
                                    Value::Str(rc.clone()),
                                ])
                            })
                            .collect(),
                    ),
                );
            }
            if !meta.constraint_unique.is_empty() {
                m.insert(
                    "constraint_unique".into(),
                    Value::Array(
                        meta.constraint_unique
                            .iter()
                            .map(|c| Value::Str(c.clone()))
                            .collect(),
                    ),
                );
            }
            if !meta.index_defs.is_empty() {
                m.insert(
                    "index_defs".into(),
                    Value::Array(
                        meta.index_defs
                            .iter()
                            .map(|d| {
                                let mut entry = vec![
                                    Value::Str(d.name.clone()),
                                    Value::Str(d.column().to_string()),
                                    Value::Bool(d.unique),
                                    Value::Array(
                                        d.columns.iter().map(|c| Value::Str(c.clone())).collect(),
                                    ),
                                ];
                                // 5th element only for path indexes, 6th only
                                // for partial indexes: older readers ignore
                                // the tail, so the tuple stays compatible in
                                // both directions (design 005).
                                if let Some(p) = &d.path {
                                    entry.push(Value::Str(p.clone()));
                                }
                                if let Some(p) = &d.pred {
                                    entry.push(Value::Str(p.clone()));
                                }
                                Value::Array(entry)
                            })
                            .collect(),
                    ),
                );
            }
            if !meta.index_roots.is_empty() {
                m.insert(
                    "index_roots".into(),
                    Value::Object(
                        meta.index_roots
                            .iter()
                            .map(|(c, r)| (c.clone(), Value::Int(*r as i64)))
                            .collect(),
                    ),
                );
            }
            if !meta.overflow_free.is_empty() {
                m.insert(
                    "overflow_free".into(),
                    Value::Array(
                        meta.overflow_free
                            .iter()
                            .map(|p| Value::Int(*p as i64))
                            .collect(),
                    ),
                );
            }
            if let Some(q) = &meta.matview_sql {
                m.insert("matview_sql".into(), Value::Str(q.clone()));
            }
            if meta.dotted_keys {
                m.insert("dotted_keys".into(), Value::Bool(true));
            }
            tables.insert(name.clone(), Value::Object(m));
        }
        let mut cat = Object::new();
        cat.insert("tables".into(), Value::Object(tables));
        // Row triggers: absent when none exist, so a trigger-free catalog
        // keeps its exact historical byte shape (older readers ignore the
        // key; trigger-free volumes never gain one).
        if !self.triggers.is_empty() {
            let mut ts = Object::new();
            for d in self.triggers.values() {
                ts.insert(
                    d.name.clone(),
                    Value::Object(Object::from([
                        ("table".into(), Value::Str(d.table.clone())),
                        ("before".into(), Value::Bool(d.before)),
                        ("insert".into(), Value::Bool(d.insert)),
                        ("delete".into(), Value::Bool(d.delete)),
                        ("update".into(), Value::Bool(d.update)),
                        (
                            "update_of".into(),
                            match &d.update_of {
                                Some(cols) => Value::Array(
                                    cols.iter().map(|c| Value::Str(c.clone())).collect(),
                                ),
                                None => Value::Null,
                            },
                        ),
                        (
                            "when".into(),
                            match &d.when {
                                Some(w) => Value::Str(w.clone()),
                                None => Value::Null,
                            },
                        ),
                        (
                            "body".into(),
                            Value::Array(
                                d.body
                                    .iter()
                                    .map(|a| match a {
                                        TriggerAction::Sql(text) => Value::Array(vec![
                                            Value::Str("sql".into()),
                                            Value::Str(text.clone()),
                                        ]),
                                        TriggerAction::Raise { fail, msg } => Value::Array(vec![
                                            Value::Str("raise".into()),
                                            Value::Str(msg.clone()),
                                            Value::Bool(*fail),
                                        ]),
                                    })
                                    .collect(),
                            ),
                        ),
                    ])),
                );
            }
            cat.insert("triggers".into(), Value::Object(ts));
        }
        let bytes = encode::encode_to_vec(&Value::Object(cat))?;
        self.write_catalog_pages(tx, &bytes)?;
        Ok(())
    }

    /// Walk the committed catalog chain, starting at `CATALOG_PAGE`. A page 1
    /// without the magic (a legacy single-page catalog, or an empty database)
    /// is treated as a fresh one-page chain — its content is about to be
    /// rewritten in the new format anyway.
    fn catalog_chain(&mut self) -> Result<Vec<u32>> {
        let first = self.pager.read_page(CATALOG_PAGE)?;
        if !first.starts_with(CATALOG_MAGIC) {
            return Ok(vec![CATALOG_PAGE]);
        }
        let mut chain = vec![CATALOG_PAGE];
        loop {
            let last = *chain.last().expect("seeded above");
            let page = self.pager.read_page(last)?;
            let next = u32::from_le_bytes(page[12..16].try_into().expect("header fits"));
            if next == 0 {
                return Ok(chain);
            }
            chain.push(next);
            // A damaged chain must surface as an error, not loop forever:
            // every link is a distinct page in the file.
            if chain.len() > self.pager.num_pages() as usize {
                return err("catalog chain is corrupt (cycle or dangling overflow page)");
            }
        }
    }

    /// Stage the catalog payload across its page chain inside `tx`, growing
    /// the chain by freshly allocated pages when the payload outgrew it.
    fn write_catalog_pages(&mut self, tx: &mut crate::pager::Tx, bytes: &[u8]) -> Result<()> {
        let need = bytes.len().div_ceil(CATALOG_CHUNK).max(1);
        let mut chain = self.catalog_chain()?;
        while chain.len() < need {
            let id = self.pager.allocate_page(tx)?;
            chain.push(id);
        }
        // A shrinking catalog releases its tail pages for reuse (they are no
        // longer linked into the chain the save below writes).
        for &extra in &chain[need..] {
            self.pager.free_page(tx, extra)?;
        }
        chain.truncate(need);
        for i in 0..need {
            let start = i * CATALOG_CHUNK;
            let end = (start + CATALOG_CHUNK).min(bytes.len());
            let mut page = vec![0u8; PAGE_SIZE];
            page[..8].copy_from_slice(CATALOG_MAGIC);
            if i == 0 {
                page[8..12].copy_from_slice(&(bytes.len() as u32).to_le_bytes());
            }
            let next = if i + 1 < need { chain[i + 1] } else { 0 };
            page[12..16].copy_from_slice(&next.to_le_bytes());
            page[CATALOG_HDR..CATALOG_HDR + (end - start)].copy_from_slice(&bytes[start..end]);
            self.pager.write_page(tx, chain[i], 0, &page)?;
        }
        Ok(())
    }
    fn save_catalog(&mut self) -> Result<()> {
        let mut tx = self.pager.begin_tx();
        self.save_catalog_into(&mut tx)?;
        self.commit_pager_tx(tx)?;
        Ok(())
    }

    /// Cheap catalog snapshot (Arc clones + the autoinc cache): document
    /// rollback rides the pager's page undo journal instead of a deep copy.
    fn catalog_snapshot(&self) -> TableSnapshot {
        TableSnapshot {
            tables: self.tables.clone(),
            autoinc: self.autoinc_cache.clone(),
        }
    }

    /// Restore the catalog from `snap` and replay `undo` in reverse into one
    /// commit: `Set` writes the pre-image back, `Alloc` returns the page to
    /// the reusable pool, `Free` reserves it (the restored catalog owns it
    /// again), `Realloc` needs nothing (its pre-image was recorded when the
    /// transaction freed it).
    ///
    /// Recording is paused for the replay (`pause_undo`): a replayed `Set`
    /// for a page first written after the savepoint must not treat the
    /// current dirty image as a new pre-image — that would let a later full
    /// ROLLBACK resurrect and commit the changes being rolled back here.
    /// The restored image *is* the transaction-start state, so future writes
    /// re-record it naturally.
    fn restore_transaction(
        &mut self,
        snap: TableSnapshot,
        undo: &mut [crate::pager::UndoOp],
        sync_catalog: bool,
    ) -> Result<()> {
        self.pager.pause_undo();
        let replay = || -> Result<()> {
            let mut tx = self.pager.begin_tx();
            // By index, never consuming: a mid-replay failure must leave the
            // journal intact so a retried ROLLBACK still restores the pages.
            // `into_iter()` here used to drop every op on the first error —
            // the retry then "restored" only the catalog snapshot while the
            // transaction's page images stayed applied.
            for idx in (0..undo.len()).rev() {
                match &undo[idx] {
                    crate::pager::UndoOp::Set(id, image) => {
                        self.pager.write_page(&mut tx, *id, 0, image)?;
                    }
                    crate::pager::UndoOp::Alloc(id) => {
                        self.pager.free_page(&mut tx, *id)?;
                    }
                    crate::pager::UndoOp::Free(id) => {
                        self.pager.reserve_page(*id);
                    }
                    crate::pager::UndoOp::Realloc(_) => {}
                }
            }
            // Publish AFTER durability (the mirror of COMMIT's
            // durability-first order): the restored view must be installed
            // for the catalog write below, but a failed commit reverts it —
            // otherwise the in-memory catalog leads the pager with a view
            // whose rollback never became durable, and the next
            // `save_catalog` would persist the mix.
            let prev_tables = std::mem::replace(&mut self.tables, snap.tables);
            let prev_autoinc = std::mem::replace(&mut self.autoinc_cache, snap.autoinc);
            let result = if sync_catalog {
                self.save_catalog_into(&mut tx)
                    .and_then(|_| self.commit_pager_tx(tx))
            } else {
                self.commit_pager_tx(tx)
            };
            if result.is_err() {
                self.tables = prev_tables;
                self.autoinc_cache = prev_autoinc;
            }
            result.map(|_| ())
        };
        let result = replay();
        self.pager.resume_undo();
        result
    }

    fn rollback_tx(&mut self) -> Result<ExecOutcome> {
        if self.tx_snapshot.is_none() {
            return err("no transaction in progress");
        }
        // Durability FIRST, teardown after (mirrors COMMIT above): taking
        // the snapshot before the undo-replay commit used to leave a failed
        // ROLLBACK wedged — retry hit "no transaction in progress" while
        // the engine held neither the rolled-back pages nor a consistent
        // catalog. On failure the transaction stays open and the error is
        // loud; teardown only runs once the restore is durable.
        // Take the journal only AFTER the deferred commits are voided:
        // abort_deferred failing (I/O error on the ABORT append) used to
        // drop the just-taken undo journal — a retried ROLLBACK then found
        // an empty journal and "succeeded" after restoring nothing but the
        // in-memory catalog, leaving the transaction's page images applied.
        // 先把本事务各语句的 deferred 提交显式作废:没有这组 ABORT 帧,
        // 恢复事务的同步提交会先落 FENCE,撕裂崩溃停在「FENCE 已持久、
        // 恢复事务 COMMIT 未持久」窗口时,整个已回滚事务会复活。
        self.pager.abort_deferred()?;
        let mut undo = self.pager.take_undo();
        let snap = self
            .tx_snapshot
            .clone()
            .expect("checked above; single writer keeps it in place");
        if let Err(e) = self.restore_transaction(snap, &mut undo, true) {
            // Put the journal back: the transaction stays open and the
            // retry must replay the same ops, not an empty tail.
            self.pager.restore_undo_journal(undo);
            return Err(e);
        }
        let _ = self.tx_snapshot.take();
        self.savepoints.clear();
        // The restore transaction committed DEFERRED (the snapshot was still
        // in place during it): fence + fsync now. ROLLBACK is a terminal
        // statement — no later COMMIT exists to trigger the WAL cap — so
        // without this a BEGIN/INSERT/ROLLBACK workload grew the log and
        // pending_writes without bound. The fence is also the durability
        // boundary for the undo-replayed page images. On failure the error
        // is loud and a retried ROLLBACK is idempotent (the undo journal is
        // empty by then; the catalog is already at the snapshot state).
        self.pager.sync_wal()?;
        Ok(ExecOutcome::Affected(0))
    }

    /// SAVEPOINT name: mark the current transaction state so a later
    /// ROLLBACK TO can restore it (the transaction stays open). Holds Arc
    /// pointers plus a journal mark — cost is O(tables), not O(database).
    /// Capped so a savepoint flood cannot pin unbounded journal memory.
    fn savepoint(&mut self, name: &str) -> Result<ExecOutcome> {
        const MAX_SAVEPOINTS: usize = 64;
        if self.tx_snapshot.is_none() {
            return err("SAVEPOINT requires an active transaction");
        }
        if self.savepoints.len() >= MAX_SAVEPOINTS {
            return err(format!(
                "too many savepoints (max {MAX_SAVEPOINTS}); release or roll back some first"
            ));
        }
        let snap = self.catalog_snapshot();
        // Mark BEFORE the baseline images: the tail a later ROLLBACK TO
        // replays must include them.
        let mark = self.pager.undo_mark();
        self.pager.undo_savepoint()?;
        self.savepoints.push((name.to_string(), snap, mark));
        Ok(ExecOutcome::Affected(0))
    }

    /// RELEASE SAVEPOINT name: forget the savepoint (changes stay). With
    /// duplicate names the most recent one is released (SQLite semantics).
    fn release_savepoint(&mut self, name: &str) -> Result<ExecOutcome> {
        let Some(pos) = self.savepoints.iter().rposition(|(n, _, _)| n == name) else {
            return err(format!("savepoint {name} does not exist"));
        };
        self.savepoints.truncate(pos);
        Ok(ExecOutcome::Affected(0))
    }

    /// ROLLBACK TO SAVEPOINT name: restore that savepoint's state; the
    /// outer transaction continues and later savepoints are discarded.
    /// The named savepoint itself is discarded too (a documented divergence
    /// from SQLite/PG, pinned by duplicate_savepoint_names_resolve_to_newest:
    /// a second ROLLBACK TO the same name resolves to the OLDER same-named
    /// savepoint, not an error).
    fn rollback_to_savepoint(&mut self, name: &str) -> Result<ExecOutcome> {
        let Some(pos) = self.savepoints.iter().rposition(|(n, _, _)| n == name) else {
            return err(format!("savepoint {name} does not exist"));
        };
        let snap = self.savepoints[pos].1.clone();
        let mark = self.savepoints[pos].2;
        self.savepoints.truncate(pos);
        let mut undo = self.pager.take_undo_to(mark);
        // The transaction stays open: no catalog save is needed (the journal
        // replay already restored the catalog pages; the in-memory map comes
        // from the snapshot). `sync_catalog=false` keeps this cheap.
        if let Err(e) = self.restore_transaction(snap, &mut undo, false) {
            // Reattach the tail: a failed ROLLBACK TO must leave the journal
            // whole for the retry (and a later full ROLLBACK), exactly like
            // the full-ROLLBACK path.
            self.pager.reattach_undo_tail(undo);
            return Err(e);
        }
        Ok(ExecOutcome::Affected(0))
    }

    /// True when the statement mutates data (used by replicas to reject
    /// client writes while accepting replicated ones).
    pub fn is_write_statement(sql: &str) -> bool {
        match Parser::parse_sql(&GenericDialect {}, sql) {
            Ok(stmts) => match stmts.first() {
                Some(stmt) => Self::stmt_is_write(stmt),
                // An empty batch writes nothing.
                None => false,
            },
            Err(_) => true,
        }
    }

    /// Write classification of an already-parsed statement: everything
    /// except queries, PRAGMA shims and session-local transaction control
    /// mutates data (transaction control replicates only its inner data
    /// statements, never BEGIN/COMMIT/ROLLBACK themselves).
    fn stmt_is_write(stmt: &Statement) -> bool {
        !matches!(
            stmt,
            Statement::Query(_)
                | Statement::Explain { .. }
                | Statement::Pragma { .. }
                | Statement::StartTransaction { .. }
                | Statement::Commit { .. }
                | Statement::Rollback { .. }
                | Statement::Savepoint { .. }
                | Statement::ReleaseSavepoint { .. }
        )
    }

    /// Table names a mutating statement writes to (its targets, not the
    /// tables it merely reads in a subquery). The server's system-table
    /// gate classifies on this instead of a substring scan, so a user
    /// write that merely MENTIONS a system table in a literal is not
    /// rejected while any write aimed at one still is.
    /// Tables declaring a FOREIGN KEY into any of `targets` (one closure
    /// step; callers iterate for transitivity). Authorization input for
    /// `TRUNCATE ... CASCADE`, whose execution clears the FK children —
    /// those children must be authorization input too.
    pub fn table_foreign_keys_of(&self, targets: &[String]) -> Vec<String> {
        self.tables
            .iter()
            .filter(|(t, m)| {
                !targets.contains(t) && m.foreign_keys.iter().any(|(_, rt, _)| targets.contains(rt))
            })
            .map(|(t, _)| t.clone())
            .collect()
    }

    pub fn stmt_write_targets(stmt: &Statement) -> Vec<String> {
        fn factor_name(factor: &sqlparser::ast::TableFactor) -> Option<String> {
            match factor {
                sqlparser::ast::TableFactor::Table { name, .. } => Some(obj_name(name)),
                _ => None,
            }
        }
        match stmt {
            Statement::Insert(insert) => match &insert.table {
                TableObject::TableName(n) => vec![obj_name(n)],
                _ => vec![],
            },
            Statement::Merge(m) => factor_name(&m.table).into_iter().collect(),
            Statement::Update(u) => factor_name(&u.table.relation).into_iter().collect(),
            Statement::Delete(d) => match &d.from {
                sqlparser::ast::FromTable::WithFromKeyword(tables) => tables
                    .iter()
                    .filter_map(|t| factor_name(&t.relation))
                    .collect(),
                sqlparser::ast::FromTable::WithoutKeyword(tables) => tables
                    .iter()
                    .filter_map(|t| factor_name(&t.relation))
                    .collect(),
            },
            Statement::CreateTable(create) => vec![obj_name(&create.name)],
            Statement::CreateView(view) => vec![obj_name(&view.name)],
            Statement::CreateIndex(idx) => vec![obj_name(&idx.table_name)],
            Statement::Drop { names, .. } => names.iter().map(obj_name).collect(),
            Statement::AlterTable(alter) => {
                let mut out = vec![obj_name(&alter.name)];
                for op in &alter.operations {
                    if let sqlparser::ast::AlterTableOperation::RenameTable { table_name } = op {
                        match table_name {
                            sqlparser::ast::RenameTableNameKind::As(n)
                            | sqlparser::ast::RenameTableNameKind::To(n) => out.push(obj_name(n)),
                        }
                    }
                }
                out
            }
            Statement::Truncate(tr) => tr.table_names.iter().map(|t| obj_name(&t.name)).collect(),
            _ => vec![],
        }
    }

    /// Transaction-control classification of an already-parsed statement.
    fn classify_tx(stmt: &Statement) -> TxControl {
        match stmt {
            Statement::StartTransaction { .. } => TxControl::Begin,
            Statement::Commit { .. } => TxControl::Commit,
            Statement::Rollback { savepoint, .. } => TxControl::Rollback {
                savepoint: savepoint.as_ref().map(|i| i.value.clone()),
            },
            Statement::Savepoint { name } => TxControl::Savepoint(name.value.clone()),
            Statement::ReleaseSavepoint { name } => TxControl::Release(name.value.clone()),
            _ => TxControl::None,
        }
    }

    /// Base tables a view's body reads (one level). `None` when `name` is
    /// not a view or the body cannot be classified — authorization callers
    /// must fail closed on None. Views are a SELECT permission boundary
    /// (a grant on the view hides its bases), but a write reading THROUGH
    /// a view still needs the base grants.
    /// True when the named catalog object is a view (view definition
    /// present). Missing objects are not views.
    pub fn object_is_view(&self, name: &str) -> bool {
        self.tables.get(name).is_some_and(|m| m.is_view())
    }

    pub fn view_base_tables(&self, name: &str) -> Option<Vec<String>> {
        let meta = self.tables.get(name)?;
        let sql = meta.view_sql.as_ref()?;
        let mut stmts = Parser::parse_sql(&GenericDialect {}, sql).ok()?;
        let stmt = stmts.swap_remove(0);
        let objects = |n: &str| self.tables.contains_key(n);
        let mut out = Vec::new();
        match stmt {
            Statement::Query(q) => walk_query(&q, &mut out, &objects)?,
            _ => return None,
        }
        Some(out)
    }

    /// Views whose bodies directly read any of `targets` (one level).
    /// Unparseable bodies contribute no edge — the CREATE would have
    /// failed, and a corrupt catalog must not turn DROP into a panic.
    fn direct_view_dependents(&self, targets: &[String]) -> Vec<String> {
        self.tables
            .iter()
            .filter(|(n, m)| m.is_view() && !targets.contains(n))
            .filter(|(n, _)| {
                self.view_base_tables(n)
                    .is_some_and(|bases| bases.iter().any(|b| targets.contains(b)))
            })
            .map(|(n, _)| n.clone())
            .collect()
    }

    /// Expand a DROP target list with dependent views. Without CASCADE a
    /// dependent refuses the drop — the old code dropped the base object
    /// and left the view dangling, failing only when someone SELECTed it.
    /// With CASCADE the dependents join the drop set transitively (views
    /// form a DAG), so restoring the schema order is never needed.
    fn expand_view_dependents(&self, targets: &mut Vec<String>, cascade: bool) -> Result<()> {
        loop {
            let deps = self.direct_view_dependents(targets);
            if deps.is_empty() {
                return Ok(());
            }
            if !cascade {
                return err(format!(
                    "cannot drop {}: referenced by VIEW {} \
                     (drop the view first or use CASCADE)",
                    targets.join(", "),
                    deps.join(", ")
                ));
            }
            targets.extend(deps);
        }
    }

    /// Tables a statement READS (authorization input for user connections):
    /// every table referenced in a query's FROM/JOINs (derived tables and
    /// subqueries included — inside CTE bodies, projections and predicates
    /// too), the source of an INSERT ... SELECT, the tables read by a
    /// write's expressions (UPDATE assignment subqueries, MERGE WHEN
    /// predicates, aggregate FILTER conditions) and a CTAS/CREATE VIEW
    /// source query. Fails CLOSED: a query shape the walker cannot fully
    /// classify returns `None`, and the caller must deny rather than guess.
    pub fn stmt_read_targets(
        stmt: &Statement,
        objects: &dyn Fn(&str) -> bool,
    ) -> Option<Vec<String>> {
        let mut out = Self::stmt_read_target_occurrences(stmt, objects)?;
        out.sort();
        out.dedup();
        Some(out)
    }

    /// Same walk as [`Self::stmt_read_targets`] WITHOUT sort+dedup: the
    /// row-filter coverage proof counts DEEP occurrences per appearance —
    /// a filtered table read twice (outer FROM + a subquery) must count
    /// twice, but the deduped set collapsed it to one and made deep ==
    /// top, letting the subquery position escape filtering entirely.
    pub fn stmt_read_target_occurrences(
        stmt: &Statement,
        objects: &dyn Fn(&str) -> bool,
    ) -> Option<Vec<String>> {
        let mut out = Vec::new();
        match stmt {
            Statement::Query(q) => walk_query(q, &mut out, objects)?,
            // EXPLAIN reads exactly what its inner query reads (fail-closed
            // on any inner shape the walker cannot classify).
            Statement::Explain { statement, .. } => match statement.as_ref() {
                Statement::Query(q) => walk_query(q, &mut out, objects)?,
                _ => return None,
            },
            Statement::Insert(insert) => {
                if let Some(source) = &insert.source {
                    walk_query(source, &mut out, objects)?;
                }
            }
            Statement::Update(u) => {
                // Assignment values may carry subqueries (`SET v =
                // (SELECT pw FROM docsql_users …)`); exec_update
                // substitutes and evaluates them, so they are reads.
                for a in &u.assignments {
                    walk_expr(&a.value, &mut out, objects)?;
                }
                // The update target itself is a WRITE target; extra FROM
                // tables are reads.
                if let Some(kind) = &u.from {
                    let list = match kind {
                        sqlparser::ast::UpdateTableFromKind::BeforeSet(v)
                        | sqlparser::ast::UpdateTableFromKind::AfterSet(v) => v,
                    };
                    for twj in list {
                        walk_factor(&twj.relation, &mut out, objects)?;
                    }
                }
                if let Some(sel) = &u.selection {
                    walk_expr(sel, &mut out, objects)?;
                }
            }
            Statement::Delete(d) => {
                // USING tables are reads; the delete targets come from
                // stmt_write_targets.
                if let Some(using) = &d.using {
                    for twj in using {
                        walk_factor(&twj.relation, &mut out, objects)?;
                    }
                }
                if let Some(sel) = &d.selection {
                    walk_expr(sel, &mut out, objects)?;
                }
            }
            Statement::Merge(m) => {
                // The USING side is a read; the merge target comes from
                // stmt_write_targets. ON/assignments referencing the target
                // are writes, not reads. WHEN predicates are evaluated per
                // row and may carry subqueries — walk them too.
                walk_factor(&m.source, &mut out, objects)?;
                for clause in &m.clauses {
                    if let Some(pred) = &clause.predicate {
                        walk_expr(pred, &mut out, objects)?;
                    }
                }
            }
            Statement::CreateTable(create) => {
                // CTAS: the source query executes at creation time and is
                // authorization input (the admin-only DDL gate shadows this
                // today, but the classification must stay honest).
                if let Some(q) = &create.query {
                    walk_query(q, &mut out, objects)?;
                }
            }
            Statement::CreateView(view) => walk_query(&view.query, &mut out, objects)?,
            _ => {}
        }
        Some(out)
    }

    /// Parse a single statement once and classify it in the same pass: the
    /// AST feeds [`Database::execute_parsed`], the flags route the request
    /// (write path, transaction control) without re-parsing the text.
    /// User-management statements are hand-parsed first (sqlparser 0.62
    /// rejects their grammar) and always classify as writes.
    ///
    /// Results go through the process-wide statement cache: parsing is pure
    /// syntax — nothing catalog- or connection-dependent — so entries never
    /// go stale and no invalidation exists to get wrong. Every tier parses
    /// through here (write path, MVCC read views, routing probes), so a
    /// repeated point query skips the ~4 µs sqlparser pass.
    pub fn parse_classified(sql: &str) -> Result<ParsedStatement> {
        const CACHE_MAX_SQL: usize = 4096;
        const CACHE_ENTRIES: usize = 256;
        const SEEN_CAP: usize = 1024;
        let cacheable = sql.len() <= CACHE_MAX_SQL;
        if cacheable {
            let hit = stmt_cache()
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(sql)
                .cloned();
            if let Some(parsed) = hit {
                return Ok(parsed);
            }
        }
        let parsed = Self::parse_classified_uncached(sql)?;
        // User-management statements carry the plaintext password in the AST
        // (the resolved hash rewrite happens at execution): caching them
        // would pin credentials in process-wide memory beyond the statement
        // and outlive the "plaintext never leaves the executing path"
        // contract.
        let cacheable = cacheable && !matches!(parsed.stmt, AnyStmt::UserAdmin(_));
        if cacheable {
            // Second-sight gate: pay the cache-write cost (an AST clone)
            // only for text seen before, so distinct-literal workloads —
            // per-row generated lookups, rendered bindings — skip it. The
            // u64 hash is a heuristic only: a false positive merely writes
            // the cache early (the cache key is the exact text, so
            // correctness never depends on the hash), a false negative
            // just delays caching by one sight.
            let mut seen = stmt_seen().lock().unwrap_or_else(|p| p.into_inner());
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            sql.hash(&mut hasher);
            let h = hasher.finish();
            if seen.contains(&h) {
                drop(seen);
                let mut cache = stmt_cache().lock().unwrap_or_else(|p| p.into_inner());
                if cache.len() >= CACHE_ENTRIES {
                    cache.clear();
                }
                cache.insert(sql.to_string(), parsed.clone());
            } else {
                if seen.len() >= SEEN_CAP {
                    seen.clear();
                }
                seen.insert(h);
            }
        }
        Ok(parsed)
    }

    /// The uncached parse behind [`Self::parse_classified`].
    fn parse_classified_uncached(sql: &str) -> Result<ParsedStatement> {
        // REFRESH MATERIALIZED VIEW [CONCURRENTLY] name — the generic
        // dialect has no REFRESH statement; hand-parse the fixed shape
        // (same pattern as the user-management statements).
        {
            let up = sql.trim_start().to_ascii_uppercase();
            let body = up
                .strip_prefix("REFRESH MATERIALIZED VIEW ")
                .or_else(|| up.strip_prefix("REFRESH MATERIALIZED VIEW	"));
            if let Some(rest) = body {
                let mut parts = rest.split_ascii_whitespace();
                let mut name_part = parts.next().unwrap_or("");
                if name_part == "CONCURRENTLY" {
                    // PG's CONCURRENTLY needs a unique index to delta-apply;
                    // this engine rewrites wholesale — accept and ignore.
                    name_part = parts.next().unwrap_or("");
                }
                if parts.next().is_some() || name_part.is_empty() {
                    return err("REFRESH MATERIALIZED VIEW takes exactly one view name");
                }
                // Recover the original spelling from the source text: the
                // name is the last token of the statement (after the
                // optional CONCURRENTLY).
                let tokens: Vec<&str> = sql.trim().split_ascii_whitespace().collect();
                let name = tokens
                    .last()
                    .copied()
                    .unwrap_or("")
                    .trim_end_matches(';')
                    .to_string();
                if name.is_empty()
                    || !name
                        .chars()
                        .next()
                        .map(|c| c.is_alphanumeric() || c == '_' || c == '"' || c == '#')
                        .unwrap_or(false)
                {
                    return err("REFRESH MATERIALIZED VIEW takes exactly one view name");
                }
                return Ok(ParsedStatement {
                    stmt: AnyStmt::RefreshMatview(name),
                    tx: TxControl::None,
                    is_write: true,
                    source: sql.to_string(),
                });
            }
        }
        match crate::useradmin::parse(sql) {
            Some(Ok(ua)) => {
                return Ok(ParsedStatement {
                    stmt: AnyStmt::UserAdmin(ua),
                    tx: TxControl::None,
                    is_write: true,
                    source: sql.to_string(),
                })
            }
            Some(Err(m)) => return err(m),
            None => {}
        }
        // T-SQL notations the generic parser cannot express ([brackets],
        // CONVERT/PARSE argument order, SET/USE/PRINT shims) are rewritten
        // on a text copy; `source` keeps the original text so journals and
        // logs carry what the user wrote (the rewrite is deterministic and
        // re-applies on every replay).
        let parsed_sql = crate::tsql::preprocess(sql);
        let parsed_sql = parsed_sql.as_str();
        let mut stmts = Parser::parse_sql(&GenericDialect {}, parsed_sql)
            .map_err(|e| SqlError::Parse(e.to_string()))?;
        let stmt = match stmts.len() {
            0 => return err("empty statement"),
            1 => stmts.swap_remove(0),
            _ => return err("exactly one statement per execute() call"),
        };
        let tx = Self::classify_tx(&stmt);
        let is_write = Self::stmt_is_write(&stmt);
        Ok(ParsedStatement {
            stmt: AnyStmt::Sql(Box::new(stmt)),
            tx,
            is_write,
            source: sql.to_string(),
        })
    }

    /// True when the catalog holds a table with this exact name.
    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    /// ASCII-case-insensitive catalog probe: the server-side compat-view
    /// rewrites (docsql_pubsub / docsql_log) must not shadow a user table
    /// under ANY spelling of the name — an exact-only probe let
    /// `DocSQL_PubSub` INSERT land in the real table while the SELECT was
    /// silently rewritten onto the system view.
    pub fn table_exists_ci(&self, name: &str) -> bool {
        self.tables
            .keys()
            .any(|k| k.len() == name.len() && k.eq_ignore_ascii_case(name))
    }

    /// Arm the cooperative deadline for the statement about to execute
    /// (server-side statement timeout). Engine-global, not per connection:
    /// the caller MUST clear it (`set_statement_deadline(None)`) once the
    /// statement finished, on every path.
    pub fn set_statement_deadline(&self, deadline: Option<std::time::Instant>) {
        self.stmt_deadline.set(deadline);
    }

    /// True when a session transaction is open.
    pub fn in_transaction(&self) -> bool {
        self.tx_snapshot.is_some()
    }

    /// Build a guardless read view (MVCC stage B): clones the catalog as
    /// `Arc` bumps and begins a pager snapshot at the current commit head.
    /// The caller must hold *a* database lock while calling this; the view
    /// is executed after the guard is released, so long SELECTs never block
    /// writers. Dropping the view ends the snapshot.
    pub fn read_view(&self) -> ReadView {
        ReadView {
            pager: self.pager.clone(),
            catalog: CatalogSnapshot(self.tables.clone()),
            deadline: std::sync::Arc::new(StmtDeadline::default()),
            snap: self.pager.begin_snapshot(),
        }
    }

    /// Borrowed read context under the current lock: current visibility,
    /// no snapshot. Write paths and the census use this to share the SELECT
    /// chain with the guardless stage-B views.
    pub(crate) fn read_cx(&self) -> ReadCx<'_> {
        ReadCx {
            pager: &self.pager,
            tables: &self.tables,
            deadline: &self.stmt_deadline,
            snap: None,
        }
    }

    /// The plain-SELECT gate shared by both read paths (stage A
    /// [`Database::execute_read`] and stage B [`ReadView::execute`]):
    /// writes, transaction control and WITH queries are refused — the
    /// server tiers them to the write lock (WITH materialization is
    /// statement-local mutable state). Returns the query to execute.
    fn plain_read_query(parsed: &ParsedStatement) -> Result<Query> {
        if parsed.is_write {
            return err("read path refused a write statement");
        }
        if parsed.tx != TxControl::None {
            return err("read path refused a transaction statement");
        }
        match &parsed.stmt {
            AnyStmt::Sql(stmt) => match &**stmt {
                Statement::Query(q) if q.with.is_none() => Ok((**q).clone()),
                Statement::Query(_) => err("read path: WITH queries must use the write path"),
                _ => err("read path: statement is not a query"),
            },
            _ => err("read path: statement is not a query"),
        }
    }

    /// Read-only execution path (MVCC stage A): plain SELECT only, callable
    /// on `&Database` under the server's read lock so read-only statements
    /// run concurrently. Guardless reads (stage B) go through
    /// [`Database::read_view`] instead.
    ///
    /// No statement deadline here: this path shares the engine-global slot
    /// the write path arms (a concurrent arming would be overwritten), so
    /// stage-A reads are not preemptible. Stage-B views carry their own
    /// deadline (see `ReadView::set_statement_deadline`).
    pub fn execute_read(&self, sql: &str) -> Result<ExecOutcome> {
        let q = Self::plain_read_query(&Self::parse_classified(sql)?)?;
        self.exec_query_cx(q)
    }

    // Write-path shims: run the shared SELECT-chain helpers under a borrowed
    // context (current visibility — the write lock already excludes snapshot
    // readers from mutating anything mid-statement).
    /// Mutable catalog entry for the write path: `Arc::make_mut` deep-clones
    /// the meta only while a detached stage-B reader still holds the previous
    /// version (a concurrent long SELECT); single-writer idle runs mutate in
    /// place exactly as before.
    fn catalog_mut(&mut self, name: &str) -> Option<&mut TableMeta> {
        match self.tables.get_mut(name) {
            Some(a) => Some(std::sync::Arc::make_mut(a)),
            None => None,
        }
    }

    /// Feed every NEW document image a write statement produced: the first
    /// literal dotted field name ("a.b" as ONE key) sticks the table's
    /// `dotted_keys` marker (see TableMeta). Returns true when the marker
    /// was just set — callers fold that into their layout-sync condition so
    /// the marker reaches the catalog in the same transaction.
    fn note_dotted_fields<'a>(
        &mut self,
        table: &str,
        mut docs: impl Iterator<Item = &'a Object>,
    ) -> bool {
        if self.tables.get(table).is_some_and(|m| m.dotted_keys) {
            return false;
        }
        if !docs.any(|d| d.keys().any(|k| k.contains('.'))) {
            return false;
        }
        if let Some(m) = self.catalog_mut(table) {
            m.dotted_keys = true;
            return true;
        }
        false
    }

    pub(crate) fn table_docs_cx(&self, table: &str) -> Result<Vec<Object>> {
        self.read_cx().table_docs(table)
    }
    fn table_pairs_cx(&self, table: &str) -> Result<Vec<(u64, Object)>> {
        self.read_cx().table_pairs(table)
    }
    /// Write-path pre-pass with the outer scope of the target statement
    /// (UPDATE/DELETE target + FROM/USING tables): correlated subqueries on
    /// row-local surfaces (WHERE, SET) stay in place for per-row evaluation.
    fn subst_expr_cx_scoped(&self, e: &mut SqlExpr, outer: &OuterFrom) -> Result<()> {
        self.read_cx().subst_expr_scoped(e, outer, true)
    }
    fn index_probe_cx(
        &self,
        table: &str,
        alias: Option<&str>,
        selection: &Option<SqlExpr>,
        ctes: &Ctes,
    ) -> Result<Option<Vec<(u64, Object)>>> {
        self.read_cx().index_probe(table, alias, selection, ctes)
    }
    fn matches_cx_in(
        &self,
        selection: &Option<SqlExpr>,
        doc: &Object,
        outer: &OuterFrom,
    ) -> Result<bool> {
        self.read_cx().matches_in(selection, doc, outer)
    }
    /// [`eval_assignments`] with correlated-subquery support: SET values
    /// still holding subquery nodes resolve their outer references per row.
    fn eval_assignments_cx(
        &self,
        assignments: &[sqlparser::ast::Assignment],
        doc: &Object,
        outer: &OuterFrom,
    ) -> Result<Vec<(String, Value)>> {
        if !assignments.iter().any(|a| expr_has_subquery(&a.value)) {
            return eval_assignments(assignments, doc);
        }
        let mut subst = assignments.to_vec();
        for a in &mut subst {
            self.read_cx().subst_expr_row(&mut a.value, doc, outer)?;
        }
        eval_assignments(&subst, doc)
    }
    fn load_from_cx(
        &self,
        from: &[sqlparser::ast::TableWithJoins],
        selection: &Option<SqlExpr>,
        ctes: &Ctes,
    ) -> Result<Vec<Object>> {
        self.read_cx().load_from(from, selection, ctes)
    }
    fn exec_query_cx(&self, query: Query) -> Result<ExecOutcome> {
        self.read_cx().exec_query(query)
    }

    /// Execute exactly one SQL statement.
    pub fn execute(&mut self, sql: &str) -> Result<ExecOutcome> {
        match Self::parse_classified(sql) {
            Ok(parsed) => self.execute_parsed(parsed),
            Err(e) => Err(e),
        }
    }

    /// Execute an already-parsed statement — the parse-once path servers
    /// use after [`Database::parse_classified`] routed the request.
    pub fn execute_parsed(&mut self, parsed: ParsedStatement) -> Result<ExecOutcome> {
        self.resolved_sql = None;
        self.last_insert_id = None;
        // Wall-clock fold (journaled-write red line): a write carrying
        // NOW()/SYSDATE()/CURRENT_TIMESTAMP resolves the instant ONCE here
        // and ships the literal through resolved_sql — a replica replaying
        // the text would stamp its own clock and silently diverge. The
        // re-parsed statement carries the folded literal, so the INSERT
        // canonical rewrite below (if any) renders folded values too.
        //
        // Exception — statements that only DECLARE future behavior (plain
        // CREATE TABLE / ALTER): folding `DEFAULT NOW()` would freeze the
        // DDL-time instant into every future row. The call text stays; at
        // INSERT time exec_insert resolves the instant per row and rewrites
        // the journaled INSERT with explicit values (the auto-GUID
        // mechanism). CTAS still folds — its query is row data created now.
        if parsed.is_write && crate::stmt::mentions_wall_clock(&parsed.source) {
            let declares_defaults = match &parsed.stmt {
                AnyStmt::Sql(stmt) => match stmt.as_ref() {
                    Statement::CreateTable(create) => create.query.is_none(),
                    Statement::AlterTable(_) => true,
                    // A MATERIALIZED view stores the query text and replays
                    // it at REFRESH: the fold would freeze one instant into
                    // every future refresh, silently changing the semantics.
                    // The determinism gate in exec_create_matview refuses
                    // the raw call instead.
                    Statement::CreateView(v) if v.materialized => true,
                    _ => false,
                },
                _ => false,
            };
            if !declares_defaults {
                if let Some(folded) =
                    crate::stmt::fold_wall_clocks(&parsed.source, crate::now_ms() as i64)
                {
                    let reparsed = Self::parse_classified(&folded)?;
                    let outcome = self.execute_parsed(reparsed)?;
                    if self.resolved_sql.is_none() {
                        self.resolved_sql = Some(folded);
                    }
                    return Ok(outcome);
                }
            }
        }
        // T-SQL wall clocks are per-STATEMENT on the READ path too: the
        // scalar arm re-samples now_ms() per call site, so every row (and
        // two GETDATE()s in one projection) drifted across the scan. Fold
        // reads exactly like RAND below — one instant per statement.
        if !parsed.is_write && crate::stmt::mentions_wall_clock(&parsed.source) {
            if let Some(folded) =
                crate::stmt::fold_wall_clocks(&parsed.source, crate::now_ms() as i64)
            {
                let reparsed = Self::parse_classified(&folded)?;
                return self.execute_parsed(reparsed);
            }
        }
        // T-SQL RAND(): evaluated ONCE per statement — every row of the
        // result sees the same value. Reads (and CTAS, whose query is row
        // data created now) fold the calls into one literal before
        // executing; other writes keep the call in the text so exec_insert's
        // rewrite ships resolved values and UPDATE/DELETE/MERGE keep their
        // loud refusal below.
        let is_ctas = matches!(
            &parsed.stmt,
            AnyStmt::Sql(s) if matches!(
                s.as_ref(),
                Statement::CreateTable(c) if c.query.is_some()
            )
        );
        if (!parsed.is_write || is_ctas) && crate::stmt::mentions_nondet_call(&parsed.source) {
            let lit = crate::engine::value_literal(&crate::value::Value::Float(
                crate::tsql::rand_unit(),
            ))?;
            if let Some(folded) = crate::stmt::fold_rand_calls(&parsed.source, &lit) {
                let reparsed = Self::parse_classified(&folded)?;
                let outcome = self.execute_parsed(reparsed)?;
                if is_ctas && self.resolved_sql.is_none() {
                    self.resolved_sql = Some(folded);
                }
                return Ok(outcome);
            }
        }
        // NEWID()/NEWSEQUENTIALID() in writes: only INSERT has the canonical
        // per-row rewrite that ships generated ids as literals (exec_insert).
        // UPDATE/DELETE/MERGE journal their original text, so a random id
        // there would be re-rolled on every replaying peer — refuse loudly.
        // AST-level: a substring scan over the source would match the words
        // inside string literals ('x-newid-y') and unrelated identifiers
        // (a column named newid), rejecting perfectly legal writes.
        if parsed.is_write {
            if let AnyStmt::Sql(stmt) = &parsed.stmt {
                if stmt_calls_newid(stmt) {
                    return err(
                        "NEWID()/NEWSEQUENTIALID()/RAND() are only supported in SELECT and INSERT",
                    );
                }
            }
        }
        match parsed.stmt {
            AnyStmt::Sql(stmt) => self.exec_stmt(*stmt),
            AnyStmt::UserAdmin(ua) => self.exec_user_admin(&ua),
            AnyStmt::RefreshMatview(name) => self.exec_refresh_matview(&name),
        }
    }

    /// Parse-check a statement batch without executing anything (the web
    /// console's "parse" button).
    pub fn parse_check(sql: &str) -> std::result::Result<(), String> {
        match crate::useradmin::parse(sql) {
            Some(Ok(_)) => Ok(()),
            Some(Err(m)) => Err(m),
            None => match Parser::parse_sql(&GenericDialect {}, &crate::tsql::preprocess(sql)) {
                Ok(stmts) if !stmts.is_empty() => Ok(()),
                Ok(_) => Err("empty statement".into()),
                Err(e) => Err(e.to_string()),
            },
        }
    }

    /// Execute every statement in `sql`, in order, stopping at the first
    /// error. Successfully executed prefixes are still reported so callers
    /// (e.g. the web console) can render partial results. Splitting first
    /// lets a batch mix plain SQL with the hand-parsed user-management
    /// statements (join snapshots and backups embed them).
    pub fn execute_batch(&mut self, sql: &str) -> BatchResult {
        let parts = match crate::stmt::split_statements(sql) {
            Ok(p) => p,
            Err(m) => {
                return BatchResult {
                    statements: 0,
                    outcomes: vec![],
                    error: Some(BatchError {
                        statement: 0,
                        message: m,
                    }),
                }
            }
        };
        let statements = parts.len();
        let mut outcomes = Vec::new();
        for (i, part) in parts.into_iter().enumerate() {
            match self.execute(&part) {
                Ok(o) => outcomes.push(o),
                Err(e) => {
                    return BatchResult {
                        statements,
                        outcomes,
                        error: Some(BatchError {
                            statement: i,
                            message: e.to_string(),
                        }),
                    }
                }
            }
        }
        BatchResult {
            statements,
            outcomes,
            error: None,
        }
    }

    /// Take the canonical rewrite of the last statement: the auto-GUID
    /// INSERT rewrite or the canonical user-management form (password in
    /// stored-hash form). Replicating callers replace the original
    /// statement text with this one so peers apply the exact same values.
    pub fn take_resolved_sql(&mut self) -> Option<String> {
        self.resolved_sql.take()
    }

    /// Publish the canonical rewrite of the executing statement (used by
    /// the user-management family; auto-GUID INSERTs write the field
    /// directly inside `exec_insert`).
    pub(crate) fn set_resolved_sql(&mut self, sql: String) {
        self.resolved_sql = Some(sql);
    }

    /// AUTOINCREMENT id the last INSERT assigned (max over inserted rows,
    /// explicit values included; None when the last statement was not an
    /// inserting one or the table has no AUTOINCREMENT column). The
    /// statement-scoped readback avoids a second `SELECT MAX(id)` — a full
    /// heap scan — on every insert-and-learn-the-id caller.
    pub fn last_insert_id(&self) -> Option<i64> {
        self.last_insert_id
    }

    /// Current MAX(id) of a table's AUTOINCREMENT column via the counter
    /// cache: O(1) warm, one heap scan when an UPDATE/DELETE/restart
    /// invalidated it. None when the table is unknown or has no
    /// AUTOINCREMENT column — callers fall back to the SQL aggregate.
    pub fn max_autoinc(&mut self, table: &str) -> Option<i64> {
        let meta = self.tables.get(table)?.clone();
        meta.autoinc.as_ref()?;
        self.autoinc_next_for(table, &meta).ok()?;
        self.autoinc_cache.get(table).map(|next| next - 1)
    }

    /// PRIMARY KEY / UNIQUE constraint indexes as named entries,
    /// SQLite-style: `sqlite_autoindex_<table>_<n>`, PK column first.
    /// Derived from table-declared constraints only — columns that got a
    /// UNIQUE index through CREATE INDEX already have a named definition —
    /// and never written to the catalog, so already-deployed databases
    /// gain them on upgrade with no migration, and the names are identical
    /// on every peer (recomputed from the replicated DDL).
    fn autoindex_defs(table: &str, meta: &TableMeta) -> Vec<IndexInfo> {
        let mut cols: Vec<&String> = Vec::new();
        for c in meta.primary_key.iter().chain(meta.constraint_unique.iter()) {
            if !cols.contains(&c) {
                cols.push(c);
            }
        }
        cols.into_iter()
            .enumerate()
            .map(|(i, c)| IndexInfo {
                name: format!("sqlite_autoindex_{table}_{}", i + 1),
                column: (*c).clone(),
                columns: vec![(*c).clone()],
                unique: true,
                auto: true,
                pred: None,
            })
            .collect()
    }

    /// Read-only catalog snapshot for tooling (object explorer, drivers).
    pub fn catalog(&self) -> Vec<TableInfo> {
        self.tables
            .iter()
            .map(|(name, meta)| {
                let auto = Self::autoindex_defs(name, meta);
                TableInfo {
                    name: name.clone(),
                    columns: meta
                        .columns
                        .iter()
                        .map(|c| ColumnInfo {
                            name: c.clone(),
                            nullable: !meta.not_null.contains(c),
                            primary_key: meta.primary_key.as_deref() == Some(c.as_str()),
                            unique: meta.unique.contains(c),
                            autoinc: meta.autoinc.as_deref() == Some(c.as_str())
                                || meta.autoguid.as_deref() == Some(c.as_str()),
                            data_type: if meta.autoguid.as_deref() == Some(c.as_str()) {
                                "GUID".to_string()
                            } else {
                                "ANY".to_string()
                            },
                            default_value: meta
                                .defaults
                                .iter()
                                .find(|(dc, _)| dc == c)
                                .map(|(_, t)| t.clone()),
                        })
                        .collect(),
                    keys: meta
                        .primary_key
                        .iter()
                        .chain(meta.unique.iter())
                        .cloned()
                        .collect(),
                    indexes: auto
                        .iter()
                        .map(|i| i.name.clone())
                        .chain(meta.indexes.iter().cloned())
                        .collect(),
                    index_defs: auto
                        .into_iter()
                        .chain(meta.index_defs.iter().map(|d| IndexInfo {
                            name: d.name.clone(),
                            column: d.column().to_string(),
                            columns: d.columns.clone(),
                            unique: d.unique,
                            auto: false,
                            pred: d.pred.clone(),
                        }))
                        .collect(),
                    pages: meta.pages.len(),
                    matview: meta.is_matview(),
                }
            })
            .collect()
    }

    /// Observed (data-derived) column names for one table: the union of
    /// top-level field names across its documents — the same projection
    /// `SELECT *` builds for a non-empty table. Declared columns
    /// (`catalog()`) are the constraint/introspection surface and never
    /// auto-sync with data; this is the data-side counterpart. It scans
    /// the table, so only deliberate introspection surfaces (object
    /// explorer) should call it. Unknown tables yield an empty list.
    pub fn observed_columns(&self, table: &str) -> Vec<String> {
        if !self.tables.contains_key(table) {
            return Vec::new();
        }
        self.table_docs_cx(table)
            .map(|d| union_of_fields(&d))
            .unwrap_or_default()
    }

    /// Replication fingerprints for every user table, sorted by name;
    /// `_pubsub_messages` is skipped (node-local queue state, like
    /// [Database::dump_script]). `rows_hash` is a wrapping sum of
    /// per-row hashes over the documents' canonical encoded bytes, so it
    /// is independent of heap layout: nodes holding the same logical
    /// data agree regardless of which node applied a row first. The
    /// cluster rejoin repair compares these to decide whether a node
    /// that was offline missed writes.
    pub fn digests(&mut self) -> Result<Vec<TableDigest>> {
        let mut names: Vec<String> = self
            .tables
            .keys()
            .filter(|n| !is_system_table(n))
            .cloned()
            .collect();
        names.sort();
        let mut out = Vec::with_capacity(names.len());
        for name in &names {
            let meta = self.tables.get(name).cloned().unwrap();
            let docs = self.table_docs_cx(name)?;
            let mut rows_hash: u64 = 0;
            for doc in &docs {
                let mut h = DefaultHasher::new();
                for (k, v) in doc {
                    k.hash(&mut h);
                    let mut buf = Vec::new();
                    encode::encode(v, &mut buf)?;
                    h.write(&buf);
                }
                rows_hash = rows_hash.wrapping_add(h.finish());
            }
            out.push(TableDigest {
                name: name.clone(),
                rows: docs.len() as u64,
                rows_hash,
                schema_hash: schema_hash(&meta),
            });
        }
        Ok(out)
    }

    /// Remove every user table from the catalog, bypassing DROP TABLE's
    /// FOREIGN KEY guards. Cluster rejoin repair replaces the node's
    /// whole state with the cluster snapshot inside one transaction:
    /// tables the snapshot no longer contains must go too, and no
    /// removal order satisfies DROP's "referenced by FOREIGN KEY" check
    /// for every mixed state (cycles included). Runs inside the
    /// caller's transaction — rollback restores the pre-wipe catalog.
    /// Engine-managed system tables survive: the journal and positions
    /// describe stream-applied ops, which the snapshot adoption
    /// accounting (positions reset to the sampled heads) keeps valid.
    pub fn wipe_user_tables(&mut self) -> Result<()> {
        let doomed: Vec<String> = self
            .tables
            .keys()
            .filter(|n| !is_system_table(n))
            .cloned()
            .collect();
        let mut tx = self.pager.begin_tx();
        for name in doomed {
            if let Some(meta) = self.tables.get(&name) {
                self.free_table_storage(&mut tx, meta)?;
            }
            self.tables.remove(&name);
        }
        // Wiped tables may be recreated from a snapshot: drop their
        // AUTOINCREMENT watermarks with them or the recreated table resumes
        // from a stale counter (see DROP TABLE). Triggers die too — the
        // snapshot replay recreates them from its own CREATE TRIGGER block
        // (emitted after the data, pg_dump style).
        self.autoinc_cache.clear();
        self.triggers.clear();
        self.save_catalog_into(&mut tx)?;
        self.commit_pager_tx(tx)?;
        Ok(())
    }

    /// Release every page a table's storage occupies: heap pages, index-tree
    /// pages and recycled overflow-chain pages. Called for tables being
    /// dropped or wiped (a `rewrite_table` keeps `overflow_free`, which its
    /// next incarnation reuses for chains, so it frees only heap/tree pages).
    fn free_table_storage(&self, tx: &mut crate::pager::Tx, meta: &TableMeta) -> Result<()> {
        // Live overflow chains are anchored only by the slots' head pointers
        // (recycled ones sit in `overflow_free`); free them before the heap
        // pages themselves go away.
        crate::heap::free_overflow_chains(&self.pager, tx, &meta.pages)?;
        for &p in &meta.pages {
            self.pager.free_page(tx, p)?;
        }
        for &p in &meta.overflow_free {
            self.pager.free_page(tx, p)?;
        }
        for (root_key, &root) in &meta.index_roots {
            for p in BTree::open(root)
                .collect_pages(&PageReader::current(&self.pager), tx)
                .map_err(|e| index_err(root_key, e))?
            {
                self.pager.free_page(tx, p)?;
            }
        }
        Ok(())
    }

    // ---- catch-up replication: journal + positions ----

    /// Create the catch-up system tables if missing. Called by the server
    /// at startup, before any journal or position access.
    pub fn ensure_cluster_tables(&mut self) -> Result<()> {
        if self.tables.contains_key(CLUSTER_LOG_TABLE)
            && self.tables.contains_key(CLUSTER_POS_TABLE)
            && self.tables.contains_key(CLUSTER_ID_TABLE)
        {
            return Ok(());
        }
        // Reserved engine names: the internal-ddl flag is the sanctioned
        // way past the CREATE TABLE guard (same pattern as
        // ensure_user_tables; always cleared on the way out).
        self.internal_ddl = true;
        let result = self.ensure_cluster_tables_inner();
        self.internal_ddl = false;
        result
    }

    fn ensure_cluster_tables_inner(&mut self) -> Result<()> {
        let preexisting = self.tables.contains_key(CLUSTER_LOG_TABLE);
        self.execute(&format!(
            "CREATE TABLE IF NOT EXISTS {CLUSTER_LOG_TABLE} (seq INT PRIMARY KEY, sql TEXT, ts INT)"
        ))?;
        // Migration: journals created before PITR have no ts column. Entries
        // migrated this way keep ts NULL — PITR export skips NULL-ts rows
        // (they predate the feature and always sit before the first
        // post-upgrade base backup's journal position).
        if preexisting {
            let has_ts = self
                .execute(&format!("SELECT ts FROM {CLUSTER_LOG_TABLE} LIMIT 1"))
                .is_ok();
            if !has_ts {
                self.execute(&format!(
                    "ALTER TABLE {CLUSTER_LOG_TABLE} ADD COLUMN ts INT"
                ))?;
            }
        }
        self.execute(&format!(
            "CREATE TABLE IF NOT EXISTS {CLUSTER_POS_TABLE} (node_id TEXT PRIMARY KEY, seq INT)"
        ))?;
        self.execute(&format!(
            "CREATE TABLE IF NOT EXISTS {CLUSTER_ID_TABLE} (id TEXT)"
        ))?;
        Ok(())
    }

    /// Create the pub/sub backing table (idempotent). Lives in core so the
    /// server's pubsub store and every test use the same sanctioned path
    /// past the reserved-name guard in CREATE TABLE.
    pub fn ensure_pubsub_table(&mut self) -> Result<()> {
        self.internal_ddl = true;
        let result = self
            .execute(&format!(
                "CREATE TABLE IF NOT EXISTS {PUBSUB_TABLE} \
             (id INT PRIMARY KEY AUTOINCREMENT, channel TEXT, ts_ms INT, payload TEXT)"
            ))
            .map(|_| ());
        self.internal_ddl = false;
        result
    }

    /// This node's persistent random identity: the key other nodes store
    /// this node's journal position under. Generated once, then stable.
    pub fn cluster_id(&mut self) -> Result<String> {
        self.ensure_cluster_tables()?;
        if let Some(id) = &self.cluster_id {
            return Ok(id.clone());
        }
        let existing = self.execute(&format!("SELECT id FROM {CLUSTER_ID_TABLE} LIMIT 1"))?;
        if let ExecOutcome::Rows(r) = existing {
            if let Some(row) = r.rows.first() {
                if let Some(Value::Str(id)) = row.first() {
                    self.cluster_id = Some(id.clone());
                    return Ok(id.clone());
                }
            }
        }
        let id = crate::guid::uuidv7();
        self.execute(&format!(
            "INSERT INTO {CLUSTER_ID_TABLE} VALUES ({})",
            value_literal(&Value::Str(id.clone()))?
        ))?;
        self.cluster_id = Some(id.clone());
        Ok(id)
    }

    /// MAX/MIN(seq) over the journal (0 when empty or negative somehow).
    fn journal_seq_boundary(&mut self, agg: &str) -> Result<u64> {
        self.ensure_cluster_tables()?;
        let r = self.execute(&format!("SELECT {agg}(seq) FROM {CLUSTER_LOG_TABLE}"))?;
        Ok(match r {
            ExecOutcome::Rows(rows) => rows
                .rows
                .first()
                .and_then(|row| row.first())
                .and_then(Value::as_i64)
                .unwrap_or(0)
                .max(0) as u64,
            _ => 0,
        })
    }

    /// Append one locally-committed write to the journal and return its
    /// seq. Call immediately after the write commits, before fanning out:
    /// the entry then exists for every peer that ever pulls catch-up,
    /// whatever happens to the fan-out itself. The insert is its own
    /// statement (a crash between the data commit and this append loses
    /// the entry — the repair's digest re-check catches the resulting
    /// divergence and falls back to a snapshot).
    pub fn journal_append(&mut self, sql: &str) -> Result<u64> {
        self.ensure_cluster_tables()?;
        let next = match self.journal_next {
            Some(n) => n,
            None => self.journal_seq_boundary("MAX")? + 1,
        };
        // ts = entry commit time (ms): the PITR replay watermark. One clock
        // read per journaled write is nothing next to the write itself.
        let ts = crate::now_ms() as i64;
        self.execute(&format!(
            "INSERT INTO {CLUSTER_LOG_TABLE} (seq, sql, ts) VALUES ({next}, {}, {ts})",
            value_literal(&Value::Str(sql.to_string()))?
        ))?;
        self.journal_next = Some(next + 1);
        if let Some(bytes) = self.journal_bytes.as_mut() {
            *bytes += sql.len() as u64 + 64;
        }
        Ok(next)
    }

    /// Highest journal seq (0 when empty).
    pub fn journal_head(&mut self) -> Result<u64> {
        self.journal_seq_boundary("MAX")
    }

    /// Lowest journal seq still retained (0 when empty). A position below
    /// this minus one means the needed range was trimmed away and the
    /// peer must fall back to a full snapshot.
    pub fn journal_oldest(&mut self) -> Result<u64> {
        self.journal_seq_boundary("MIN")
    }

    /// Journal entries with seq > `after`, in order, capped at `limit`
    /// entries per call. Callers loop, advancing `after` to the last
    /// returned seq, until the result is empty.
    pub fn journal_range(&mut self, after: u64, limit: usize) -> Result<Vec<(u64, String)>> {
        self.ensure_cluster_tables()?;
        let r = self.execute(&format!(
            "SELECT seq, sql FROM {CLUSTER_LOG_TABLE} WHERE seq > {after} \
             ORDER BY seq ASC LIMIT {limit}"
        ))?;
        let mut out = Vec::new();
        let mut bytes = 0usize;
        if let ExecOutcome::Rows(rows) = r {
            for row in &rows.rows {
                let (Some(Value::Int(seq)), Some(Value::Str(sql))) = (row.first(), row.get(1))
                else {
                    return err("journal row has unexpected shape");
                };
                bytes = bytes.saturating_add(sql.len());
                if !out.is_empty() && bytes > JOURNAL_RANGE_MAX_BYTES {
                    // Byte budget for the catch-up batch: rows can carry
                    // ~16MiB documents, so a 512-row page could otherwise
                    // materialize gigabytes in one Vec on the serving node.
                    break;
                }
                out.push((*seq as u64, sql.clone()));
            }
        }
        Ok(out)
    }

    /// Drop journal entries beyond the newest `keep` — the catch-up
    /// window. Peers positioned older than the window's start fall back
    /// to a full snapshot. `keep == 0` keeps everything.
    pub fn journal_trim(&mut self, keep: u64) -> Result<()> {
        self.ensure_cluster_tables()?;
        if keep == 0 {
            return Ok(());
        }
        let head = self.journal_head()?;
        if head <= keep {
            return Ok(());
        }
        self.journal_bytes = None; // rows deleted; recompute lazily
        self.execute(&format!(
            "DELETE FROM {CLUSTER_LOG_TABLE} WHERE seq <= {}",
            head - keep
        ))
        .map(|_| ())
    }

    /// Byte-dimension journal cap alongside the count window: drop oldest
    /// entries until the journal's SQL text fits `max_bytes`. The count
    /// window alone bounds nothing for big-document workloads (a single
    /// journal entry can reach 16 MiB, so 100k entries ≈ 100 GB), and the
    /// PITR-recommended `DOCSQL_CATCHUP_WINDOW=0` removes the count cap
    /// entirely. Cheap when under the cap: the running total is cached in
    /// memory (one SUM scan after open, then incremented per append).
    pub fn journal_trim_bytes(&mut self, max_bytes: u64) -> Result<()> {
        self.ensure_cluster_tables()?;
        let total = match self.journal_bytes {
            Some(b) => b,
            None => {
                let r =
                    self.execute(&format!("SELECT SUM(LENGTH(sql)) FROM {CLUSTER_LOG_TABLE}"))?;
                let b = match r {
                    ExecOutcome::Rows(rows) => rows
                        .rows
                        .first()
                        .and_then(|row| row.first())
                        .and_then(Value::as_i64)
                        .unwrap_or(0)
                        .max(0) as u64,
                    _ => 0,
                };
                self.journal_bytes = Some(b);
                b
            }
        };
        if total <= max_bytes {
            return Ok(());
        }
        // Delete oldest seqs until the removed bytes cover the excess. One
        // ordered scan (rows are already seq-ordered in the heap) plus one
        // ranged DELETE — both amortized against the many appends that let
        // the total drift past the cap.
        let r = self.execute(&format!(
            "SELECT seq, LENGTH(sql) FROM {CLUSTER_LOG_TABLE} ORDER BY seq ASC"
        ))?;
        let excess = total - max_bytes;
        let mut acc = 0u64;
        let mut cutoff = 0u64;
        if let ExecOutcome::Rows(rows) = r {
            for row in &rows.rows {
                let (Some(Value::Int(seq)), Some(Value::Int(n))) = (row.first(), row.get(1)) else {
                    continue;
                };
                acc += (*n).max(0) as u64;
                cutoff = *seq as u64;
                if acc >= excess {
                    break;
                }
            }
        }
        if cutoff > 0 {
            self.journal_bytes = None; // recompute exactly after the delete
            self.execute(&format!(
                "DELETE FROM {CLUSTER_LOG_TABLE} WHERE seq <= {cutoff}"
            ))?;
        }
        Ok(())
    }

    /// Void every journal entry's text, keeping the seqs allocated. Used
    /// when a snapshot adoption overrules this node's own recent writes:
    /// the adjudicated entries must never replay again (a later repair on
    /// some peer would pull them back and resurrect discarded writes), but
    /// the seq space and every recorded position must keep their meaning.
    /// Replays of a voided entry are a parsed no-op.
    /// Journal entries with `seq > after`, in seq order, capped at `limit`:
    /// the PITR incremental export source. `ts` is the entry's commit time
    /// (None for rows migrated from the pre-PITR journal shape).
    pub fn journal_entries_after(
        &mut self,
        after: u64,
        limit: usize,
    ) -> Result<Vec<(u64, Option<i64>, String)>> {
        self.ensure_cluster_tables()?;
        let r = self.execute(&format!(
            "SELECT seq, ts, sql FROM {CLUSTER_LOG_TABLE} WHERE seq > {after} \
             ORDER BY seq LIMIT {limit}"
        ))?;
        let mut out = Vec::new();
        if let ExecOutcome::Rows(rows) = r {
            for row in rows.rows {
                let seq = row.first().and_then(Value::as_i64).unwrap_or(0).max(0) as u64;
                let ts = row.get(1).and_then(Value::as_i64);
                if let Some(Value::Str(sql)) = row.get(2) {
                    out.push((seq, ts, sql.clone()));
                }
            }
        }
        Ok(out)
    }

    pub fn journal_void_all(&mut self) -> Result<()> {
        self.ensure_cluster_tables()?;
        self.journal_bytes = None; // text shrinks to the sentinel; recompute lazily
        self.execute(&format!(
            "UPDATE {CLUSTER_LOG_TABLE} SET sql = 'PRAGMA discarded_by_snapshot_adoption;'"
        ))
        .map(|_| ())
    }

    /// Last applied journal seq for the origin `node_id`.
    pub fn position_get(&mut self, node_id: &str) -> Result<Option<u64>> {
        self.ensure_cluster_tables()?;
        let r = self.execute(&format!(
            "SELECT seq FROM {CLUSTER_POS_TABLE} WHERE node_id = {}",
            value_literal(&Value::Str(node_id.to_string()))?
        ))?;
        Ok(match r {
            ExecOutcome::Rows(rows) => rows
                .rows
                .first()
                .and_then(|row| row.first())
                .and_then(Value::as_i64)
                .map(|s| s as u64),
            _ => None,
        })
    }

    /// Record that everything up to `seq` from origin `node_id` is
    /// applied here. Called after a replicated write commits (advancing)
    /// and after an adoption samples the origin's head (establishing).
    pub fn position_set(&mut self, node_id: &str, seq: u64) -> Result<()> {
        self.ensure_cluster_tables()?;
        let key = value_literal(&Value::Str(node_id.to_string()))?;
        let updated = match self.execute(&format!(
            "UPDATE {CLUSTER_POS_TABLE} SET seq = {seq} WHERE node_id = {key}"
        ))? {
            ExecOutcome::Affected(n) => n,
            ExecOutcome::Rows(_) => 0,
        };
        if updated == 0 {
            self.execute(&format!(
                "INSERT INTO {CLUSTER_POS_TABLE} (node_id, seq) VALUES ({key}, {seq})"
            ))?;
        }
        Ok(())
    }

    /// Forget all positions: used when an adoption replaces the state in
    /// a way the sampled heads cannot account for.
    pub fn positions_clear(&mut self) -> Result<()> {
        self.ensure_cluster_tables()?;
        self.execute(&format!("DELETE FROM {CLUSTER_POS_TABLE}"))
            .map(|_| ())
    }

    /// Full logical dump as a SQL script: all DDL first, then all data.
    /// System storage (`_pubsub_messages`) is skipped — its ids are
    /// node-local and it is not user data. The join protocol replays this
    /// inside one transaction on a fresh node to bootstrap the cluster's
    /// current state; every CREATE precedes every INSERT, so FOREIGN KEY
    /// declarations are safe in the DDL phase (referential checks run at
    /// insert time). Applying the script replaces tables wholesale
    /// (DROP + CREATE prefix), so re-applying is idempotent.
    ///
    /// All user tables are dropped by ONE statement: the DROP guard
    /// exempts tables dropped by the same statement, so the drop cannot
    /// be blocked by FK references — per-table drops interleaved with
    /// CREATEs would deadlock against the restored schema (the new child
    /// referencing a parent whose old copy is still to be dropped), and
    /// backup restore replays this script over LIVE data, not a fresh
    /// node.
    pub fn dump_script(&mut self) -> Result<String> {
        let names: Vec<String> = self
            .tables
            .keys()
            .filter(|n| !is_internal_table(n))
            .cloned()
            .collect();
        // Views drop with their own statement kind: DROP TABLE on a view is
        // a wrong-kind error (restore replays the prelude verbatim).
        let view_names: Vec<String> = names
            .iter()
            .filter(|n| self.tables.get(*n).is_some_and(|m| m.is_view()))
            .cloned()
            .collect();
        // Materialized views are third kind: real storage but dumped as
        // CREATE MATERIALIZED VIEW (the replay re-runs the stored query —
        // deterministic, validated at CREATE — so the restored content is
        // a fresh recompute over the restored base data, never the
        // possibly-stale origin rows).
        let matview_names: Vec<String> = names
            .iter()
            .filter(|n| self.tables.get(*n).is_some_and(|m| m.is_matview()))
            .cloned()
            .collect();
        let table_names: Vec<String> = names
            .iter()
            .filter(|n| {
                !self
                    .tables
                    .get(*n)
                    .is_some_and(|m| m.is_view() || m.is_matview())
            })
            .cloned()
            .collect();
        // CREATE order must respect FK dependencies: the engine (rightly)
        // rejects a CREATE whose referenced table does not exist yet, so
        // alphabetical DDL order fails the replay whenever the child sorts
        // before its parent. Drops go the opposite way (children first).
        let ordered_tables = fk_dependency_order(&table_names, &self.tables);
        let drop_order: Vec<String> = {
            let mut v = ordered_tables.clone();
            v.reverse();
            v
        };
        let mut ddl = String::new();
        if !view_names.is_empty() {
            let drops = view_names
                .iter()
                .map(|n| quote_ident(n))
                .collect::<Vec<_>>()
                .join(", ");
            ddl.push_str(&format!("DROP VIEW IF EXISTS {drops};\n"));
        }
        if !table_names.is_empty() {
            let drops = drop_order
                .iter()
                .map(|n| quote_ident(n))
                .collect::<Vec<_>>()
                .join(", ");
            ddl.push_str(&format!("DROP TABLE IF EXISTS {drops};\n"));
        }
        for name in &ordered_tables {
            let meta = self.tables.get(name).cloned().unwrap();
            ddl.push_str(&create_table_ddl(name, &meta));
            ddl.push('\n');
            for d in &meta.index_defs {
                // Path indexes (design 005) re-emit as the JSON_EXTRACT
                // expression they were created from — replay parses it back
                // through the same CREATE INDEX path.
                let col_text = match &d.path {
                    Some(p) => format!("JSON_EXTRACT({}, '{}')", quote_ident(&d.columns[0]), p),
                    None => d
                        .columns
                        .iter()
                        .map(|c| quote_ident(c))
                        .collect::<Vec<_>>()
                        .join(", "),
                };
                ddl.push_str(&format!(
                    "CREATE {}INDEX {} ON {} ({}){};\n",
                    if d.unique { "UNIQUE " } else { "" },
                    quote_ident(&d.name),
                    quote_ident(name),
                    col_text,
                    d.pred
                        .as_deref()
                        .map(|p| format!(" WHERE {p}"))
                        .unwrap_or_default()
                ));
            }
        }
        // Insert order must respect FK dependencies: referenced tables
        // first. Catalog iteration is alphabetical, so without this a
        // child table's rows could replay before its parent's and fail
        // the per-row FK check.
        let mut dml = String::new();
        for name in fk_dependency_order(&table_names, &self.tables) {
            let docs = self.table_docs_cx(&name)?;
            // 自引用表按行级依赖序出:存储序的前向引用曾让回放必败。
            let docs = match self.tables.get(&name) {
                Some(meta) => self_ref_row_order(&name, meta, docs),
                None => docs,
            };
            for doc in docs {
                let cols: Vec<String> = doc.keys().cloned().collect();
                let vals = cols
                    .iter()
                    .map(|c| value_literal(&doc[c]))
                    .collect::<Result<Vec<_>>>()?;
                dml.push_str(&format!(
                    "INSERT INTO {} ({}) VALUES ({});\n",
                    quote_ident(&name),
                    cols.iter()
                        .map(|c| quote_ident(c))
                        .collect::<Vec<_>>()
                        .join(", "),
                    vals.join(", ")
                ));
            }
        }
        // Append in place instead of `ddl + &dml`: the dump is taken under
        // the engine lock, so a third full-size copy would double the peak
        // memory for nothing.
        ddl.push_str(&dml);
        // Views: their bodies reference restored tables (the CREATE
        // dry-runs the body, so the table must exist by then), and a view
        // of a view needs its base view restored first — dependency order,
        // not catalog (alphabetical) order.
        for name in &view_dependency_order(&view_names, &self.tables) {
            if let Some(meta) = self.tables.get(name) {
                if let Some(body) = &meta.view_sql {
                    ddl.push_str(&format!("CREATE VIEW {} AS {body};\n", quote_ident(name)));
                }
            }
        }
        // Triggers come AFTER the data (pg_dump semantics): the dumped rows
        // already include every effect their firing ever produced, and
        // replaying the INSERTs with live triggers would fire them again —
        // duplicated audit rows, doubled cascades. Created post-data, the
        // restore reproduces the snapshot byte-for-byte and the triggers arm
        // only for statements that run after it.
        for d in self.triggers.values() {
            ddl.push_str(&create_trigger_sql(d));
            ddl.push_str(";\n");
        }
        // Materialized views after views (a matview query may read a view;
        // the CREATE re-runs the query over the restored data — REFRESH
        // semantics — so the possibly-stale stored rows are deliberately
        // NOT dumped) and before users.
        for name in &matview_names {
            let meta = self.tables.get(name).cloned().unwrap();
            let q = meta.matview_sql.as_deref().expect("matview has a query");
            ddl.push_str(&format!(
                "CREATE MATERIALIZED VIEW {} AS {q};\n",
                quote_ident(name)
            ));
            for d in &meta.index_defs {
                let col_text = d
                    .columns
                    .iter()
                    .map(|c| quote_ident(c))
                    .collect::<Vec<_>>()
                    .join(", ");
                ddl.push_str(&format!(
                    "CREATE {}INDEX {} ON {} ({}){};\n",
                    if d.unique { "UNIQUE " } else { "" },
                    quote_ident(&d.name),
                    quote_ident(name),
                    col_text,
                    d.pred
                        .as_deref()
                        .map(|p| format!(" WHERE {p}"))
                        .unwrap_or_default()
                ));
            }
        }
        // Users/roles last of all: `GRANT … ON <object>` validates that the
        // target exists, and a grant on a VIEW must replay after that view
        // is created (emitting them before the view block made every dump
        // from a mesh with view grants unrestorable). Passwords ride in
        // stored-hash form; plain INSERTs against the reserved tables are
        // rejected by the write gate, so the statement family is the one
        // replication form everywhere. The DROP prelude above does not
        // cover the reserved tables — restoring a backup with fewer users
        // must still remove the extras — so drop them here; the replaying
        // node re-creates them via ensure_user_tables before the first
        // CREATE USER applies.
        ddl.push_str(&format!(
            "DROP TABLE IF EXISTS {}, {}, {}, {};\n",
            crate::useradmin::USERS_TABLE,
            crate::useradmin::ROLES_TABLE,
            crate::useradmin::MEMBERS_TABLE,
            crate::useradmin::GRANTS_TABLE
        ));
        for s in crate::useradmin::dump_user_statements(self)? {
            ddl.push_str(&s);
            ddl.push_str(";\n");
        }
        Ok(ddl)
    }

    /// Page size of the underlying storage file.
    pub fn page_size(&self) -> usize {
        PAGE_SIZE
    }

    /// Number of allocated pages.
    pub fn num_pages(&self) -> u32 {
        self.pager.num_pages()
    }

    /// Last WAL LSN known durable; nodes that received the same writes should
    /// report converging values.
    pub fn durable_lsn(&self) -> u64 {
        self.pager.durable_lsn()
    }

    /// Table/view grants for a dropped object are garbage: delete them
    /// before the catalog change (recreating the same name must not
    /// resurrect the old privileges). Autocommit statements; replay order
    /// on peers is identical. The deleted ROWS come back to the caller:
    /// a failure of the main DDL after this point must restore them, or
    /// the node keeps a grants divergence no journal entry explains.
    /// Errors propagate — the old `.ok()` swallowed disk failures into a
    /// silent half-drop.
    fn delete_grants_for(&mut self, names: &[String]) -> Result<Vec<Object>> {
        // The grants table is created lazily on the first user-admin write;
        // a node with no users has no grants to clean.
        if !self.tables.contains_key(crate::useradmin::GRANTS_TABLE) {
            return Ok(Vec::new());
        }
        let mut deleted = Vec::new();
        for name in names {
            if !is_internal_table(name) {
                let lit_name = crate::stmt::sql_string_literal(name);
                if let Ok(ExecOutcome::Rows(r)) = self.execute(&format!(
                    "SELECT * FROM {} WHERE tbl = {}",
                    crate::useradmin::GRANTS_TABLE,
                    lit_name
                )) {
                    for row in &r.rows {
                        let mut obj = Object::new();
                        for (c, v) in r.columns.iter().zip(row.iter()) {
                            obj.insert(c.clone(), v.clone());
                        }
                        deleted.push(obj);
                    }
                }
                self.execute(&format!(
                    "DELETE FROM {} WHERE tbl = {}",
                    crate::useradmin::GRANTS_TABLE,
                    lit_name
                ))?;
            }
        }
        Ok(deleted)
    }

    /// Best-effort restore of grant rows a failed DDL must put back. Only
    /// reachable on a main-DDL failure after the grants autocommit landed;
    /// a restore failure is shouted to stderr (the alternative — silence —
    /// is the divergence this helper exists to close).
    fn restore_grant_rows(&mut self, rows: &[Object]) {
        for row in rows {
            let cols: Vec<String> = row.keys().cloned().collect();
            let vals: Vec<String> = row
                .values()
                .map(|v| value_literal(v).unwrap_or_else(|_| "NULL".to_string()))
                .collect();
            let sql = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                crate::useradmin::GRANTS_TABLE,
                cols.iter()
                    .map(|c| quote_ident(c))
                    .collect::<Vec<_>>()
                    .join(", "),
                vals.join(", ")
            );
            if let Err(e) = self.execute(&sql) {
                eprintln!(
                    "warning: failed to restore grant rows after a failed DDL ({e}); \
                     run the DDL again or REVOKE manually"
                );
            }
        }
    }

    fn exec_stmt_inner(&mut self, stmt: Statement) -> Result<ExecOutcome> {
        match stmt {
            Statement::CreateTable(create) => self.exec_create(create),
            Statement::CreateTrigger(ct) => self.exec_create_trigger(ct),
            Statement::DropTrigger(dt) => {
                if dt.option.is_some() {
                    return err("DROP TRIGGER CASCADE / RESTRICT is not supported");
                }
                let names: Vec<ObjectName> = std::iter::once(dt.trigger_name.clone()).collect();
                self.exec_drop_trigger(&names, dt.if_exists)
            }
            Statement::CreateView(view) => self.exec_create_view(view),
            Statement::Drop {
                object_type,
                names,
                if_exists,
                cascade,
                purge,
                ..
            } => {
                if purge {
                    // Hive's PURGE has no meaning for a database that always
                    // reclaims storage; accepting it would imply a recycle
                    // bin this engine does not have.
                    return err("DROP ... PURGE is not supported");
                }
                if !self.internal_ddl
                    && (object_type == sqlparser::ast::ObjectType::Table
                        || object_type == sqlparser::ast::ObjectType::View)
                {
                    for name in names.iter().map(obj_name) {
                        // Engine SYSTEM tables only: the user tables must
                        // stay droppable — the dump prelude drops them in
                        // every wholesale snapshot replay (the network path
                        // still refuses user-table DDL via
                        // stmt_write_targets).
                        if crate::engine::is_system_table(&name) {
                            return err(format!("cannot DROP the internal table {name}"));
                        }
                    }
                }
                if object_type == sqlparser::ast::ObjectType::Index {
                    // Composites roots removed below release their tree in
                    // the same catalog transaction; pages staged here never
                    // fight the readers (write lock held, snapshots use WAL).
                    //
                    // Validate the whole list first: the mutation loop below
                    // edits the in-memory catalog, so a mid-list failure must
                    // not strand uncommitted changes (a later save would
                    // persist them without a journal entry).
                    for n in &names {
                        let iname = obj_name(n);
                        // Constraint indexes are derived from the table's
                        // PRIMARY KEY/UNIQUE declarations (SQLite semantics).
                        if iname.starts_with("sqlite_autoindex_") {
                            return err("index associated with UNIQUE or PRIMARY KEY constraint \
                                 cannot be dropped");
                        }
                        let owner = self
                            .tables
                            .iter()
                            .any(|(_, m)| m.indexes.iter().any(|i| i == &iname));
                        if !owner && !if_exists {
                            return err(format!("index {iname} does not exist"));
                        }
                    }
                    // Same catalog snapshot as DROP TABLE/VIEW: a failure in
                    // the page-free loop or the catalog save must restore the
                    // in-memory catalog, or the next successful write would
                    // persist the (unjournalled) half-drop.
                    let catalog_prev = self.tables.clone();
                    let mut dropped_roots: Vec<(String, u32)> = Vec::new();
                    for n in &names {
                        let iname = obj_name(n);
                        // Index names are database-wide, so at most one table
                        // owns the index. Locate it read-only first, then
                        // write-clone just that entry (`catalog_mut` →
                        // `Arc::make_mut`) — mapping make_mut over every
                        // table would deep-clone the whole catalog whenever
                        // any detached reader holds an old version.
                        let owner = self
                            .tables
                            .iter()
                            .find(|(_, m)| m.indexes.iter().any(|i| i == &iname))
                            .map(|(t, _)| t.clone());
                        if let Some(owner) = owner {
                            let Some(meta) = self.catalog_mut(&owner) else {
                                return err(format!("table {owner} does not exist"));
                            };
                            if let Some(pos) = meta.indexes.iter().position(|i| i == &iname) {
                                meta.indexes.remove(pos);
                                // The B+ tree itself stays in index_roots:
                                // non-unique lookups still benefit from
                                // it — but only for single-column trees
                                // (keyed by the column itself). A
                                // composite tree is keyed by the index
                                // *name*; with the definition gone
                                // nothing can address it, and leaving it
                                // behind would let a column that shares
                                // the name look like a scalar tree.
                                if let Some(dpos) = meta
                                    .index_defs
                                    .iter()
                                    .position(|d| d.name == iname.as_str())
                                {
                                    let def = meta.index_defs.remove(dpos);
                                    // Trees keyed by the index NAME (composite
                                    // or JSON-path indexes) are unaddressable
                                    // once the definition is gone — and a
                                    // path tree left behind gets mistaken for
                                    // the column's own scalar tree, silently
                                    // hijacking probes and ORDER BY walks.
                                    if def.columns.len() > 1
                                        || def.path.is_some()
                                        || def.pred.is_some()
                                    {
                                        if let Some(root) = meta.index_roots.remove(&iname) {
                                            dropped_roots.push((iname.clone(), root));
                                        }
                                    }
                                    // Lift UNIQUE only when this index was
                                    // the sole source: table-declared
                                    // constraints and other unique indexes
                                    // on the same column keep it enforced.
                                    // Composite unique indexes never join
                                    // meta.unique, so single-column lift
                                    // rules cover everything here. The
                                    // "other source" test must name the
                                    // column AS ITS SINGLE key: a composite
                                    // unique index containing the column
                                    // never guaranteed single-column
                                    // uniqueness, and counting it kept the
                                    // dropped constraint enforced forever
                                    // (schema hash divergence included).
                                    if def.unique
                                        && def.columns.len() == 1
                                        && !meta.constraint_unique.contains(&def.columns[0])
                                        && meta.primary_key.as_deref()
                                            != Some(def.columns[0].as_str())
                                        && !meta.index_defs.iter().any(|d| {
                                            d.unique
                                                && d.columns.len() == 1
                                                && d.columns[0] == def.columns[0]
                                        })
                                    {
                                        meta.unique.retain(|c| c != &def.columns[0]);
                                    }
                                }
                            }
                        }
                    }
                    let mut tx = self.pager.begin_tx();
                    let drop_result: Result<()> = (|| {
                        for (root_key, root) in &dropped_roots {
                            for p in BTree::open(*root)
                                .collect_pages(&PageReader::current(&self.pager), &tx)
                                .map_err(|e| index_err(root_key, e))?
                            {
                                self.pager.free_page(&mut tx, p)?;
                            }
                        }
                        self.save_catalog_into(&mut tx)
                    })();
                    match drop_result {
                        Ok(()) => {
                            self.commit_pager_tx(tx)?;
                            return Ok(ExecOutcome::Affected(0));
                        }
                        Err(e) => {
                            self.pager.abort_tx(tx)?;
                            self.tables = catalog_prev;
                            return Err(e);
                        }
                    }
                }
                if object_type == sqlparser::ast::ObjectType::View {
                    // Views have no storage: drop the catalog entries only.
                    // DROP TABLE on a view errors (wrong-kind mismatch), and
                    // so does DROP VIEW on a table. Validate the whole list
                    // before touching the catalog — a mid-list failure must
                    // not leave earlier names already dropped and committed.
                    let mut dropping: Vec<String> = names.iter().map(obj_name).collect();
                    for name in &dropping {
                        match self.tables.get(name) {
                            None if if_exists => {}
                            None => return err(format!("view {name} does not exist")),
                            Some(meta) if !meta.is_view() => {
                                return err(format!(
                                    "{name} is a table, not a view (use DROP TABLE)"
                                ));
                            }
                            Some(_) => {}
                        }
                    }
                    // A dependent view left behind used to fail only at the
                    // next SELECT; refuse without CASCADE, join with it.
                    self.expand_view_dependents(&mut dropping, cascade)?;
                    // Same snapshot discipline as DROP TABLE: a failed
                    // save_catalog_into must not leave the views gone from
                    // the in-memory catalog (the next successful write would
                    // persist a half-drop that never freed anything).
                    let catalog_prev = self.tables.clone();
                    let grants_deleted = self.delete_grants_for(&dropping)?;
                    let mut tx = self.pager.begin_tx();
                    for name in &dropping {
                        self.tables.remove(name);
                    }
                    match self.save_catalog_into(&mut tx) {
                        Ok(()) => {
                            self.commit_pager_tx(tx)?;
                            return Ok(ExecOutcome::Affected(0));
                        }
                        Err(e) => {
                            self.pager.abort_tx(tx)?;
                            self.tables = catalog_prev;
                            self.restore_grant_rows(&grants_deleted);
                            return Err(e);
                        }
                    }
                }
                if object_type != sqlparser::ast::ObjectType::Table
                    && object_type != sqlparser::ast::ObjectType::MaterializedView
                {
                    return err("only DROP TABLE/VIEW/INDEX are supported");
                }
                // Mixed-kind target: DROP TABLE on a view must not silently
                // drop the rest.
                for name in names.iter().map(obj_name) {
                    if self.tables.get(&name).is_some_and(|m| m.is_view()) {
                        return err(format!("{name} is a view, not a table (use DROP VIEW)"));
                    }
                }
                // Validate every target before touching the catalog: a missing
                // name mid-list must not leave earlier drops applied in memory
                // only, and dropping a table other tables still reference
                // leaves dangling FKs (their inserts fail and the join
                // snapshot can never replay).
                let mut dropping: Vec<String> = names.iter().map(obj_name).collect();
                // Dependent views: refuse without CASCADE (the dangling view
                // used to fail only at SELECT time), join the drop set with
                // it. Views are catalog-only, so they never hit the FK or
                // storage paths below. Missing targets (IF EXISTS) cannot
                // block a drop — their legacy dangling views join a CASCADE
                // instead.
                if cascade {
                    self.expand_view_dependents(&mut dropping, true)?;
                } else {
                    let mut existing: Vec<String> = dropping
                        .iter()
                        .filter(|n| self.tables.contains_key(*n))
                        .cloned()
                        .collect();
                    self.expand_view_dependents(&mut existing, false)?;
                }
                let view_dropping: Vec<String> = dropping
                    .iter()
                    .filter(|n| self.tables.get(*n).is_some_and(|m| m.is_view()))
                    .cloned()
                    .collect();
                let table_dropping: Vec<String> = dropping
                    .iter()
                    .filter(|n| !self.tables.get(*n).is_some_and(|m| m.is_view()))
                    .cloned()
                    .collect();
                let drop_set: std::collections::BTreeSet<&str> =
                    table_dropping.iter().map(|s| s.as_str()).collect();
                let mut fk_removals: Vec<String> = Vec::new();
                for name in &table_dropping {
                    if !self.tables.contains_key(name) {
                        if if_exists {
                            continue;
                        }
                        return err(format!("table {name} does not exist"));
                    }
                    let referencing: Vec<String> = self
                        .tables
                        .iter()
                        .filter(|(t, m)| {
                            t.as_str() != name.as_str()
                                && !drop_set.contains(t.as_str())
                                && m.foreign_keys.iter().any(|(_, rt, _)| rt == name)
                        })
                        .map(|(t, _)| t.clone())
                        .collect();
                    if !referencing.is_empty() {
                        if !cascade {
                            return err(format!(
                                "cannot drop table {name}: referenced by FOREIGN KEY in {}",
                                referencing.join(", ")
                            ));
                        }
                        fk_removals.extend(referencing);
                    }
                }
                fk_removals.sort();
                fk_removals.dedup();
                // Snapshot the catalog: any failure between here and the
                // commit must leave the in-memory catalog untouched (the
                // pager tx undoes the pages; without this the removed
                // entries stayed gone while nothing was journaled, and the
                // next successful write would persist a half-drop whose
                // pages were never freed).
                let catalog_prev = self.tables.clone();
                let grants_deleted = self.delete_grants_for(&dropping)?;
                let mut tx = self.pager.begin_tx();
                // CASCADE drops the referencing FK declarations along with
                // the table (PostgreSQL semantics): the child table stays,
                // its constraint on the vanished parent goes.
                let drop_result: Result<()> = (|| {
                    for child in &fk_removals {
                        if let Some(meta) = self.catalog_mut(child) {
                            meta.foreign_keys
                                .retain(|(_, rt, _)| !drop_set.contains(rt.as_str()));
                        }
                    }
                    for name in &table_dropping {
                        if let Some(meta) = self.tables.get(name) {
                            self.free_table_storage(&mut tx, meta)?;
                        }
                        self.tables.remove(name);
                        // A recreated table must start its AUTOINCREMENT counter
                        // from its own rows (max+1), not inherit the dropped
                        // table's watermark — replicas that restarted compute a
                        // different value and the nodes diverge.
                        self.autoinc_cache.remove(name);
                    }
                    for name in &view_dropping {
                        self.tables.remove(name);
                    }
                    // A dropped table's triggers die with it (SQLite
                    // semantics) — a surviving trigger would dangle on the
                    // table name and misfire when a new table takes it.
                    self.triggers
                        .retain(|_, d| !drop_set.contains(d.table.as_str()));
                    self.save_catalog_into(&mut tx)?;
                    Ok(())
                })();
                match drop_result {
                    Ok(()) => {
                        self.commit_pager_tx(tx)?;
                        Ok(ExecOutcome::Affected(0))
                    }
                    Err(e) => {
                        self.pager.abort_tx(tx)?;
                        self.tables = catalog_prev;
                        self.restore_grant_rows(&grants_deleted);
                        Err(e)
                    }
                }
            }
            Statement::Insert(insert) => self.exec_insert(insert),
            Statement::Update(update) => {
                if update.output.is_some() {
                    return err("OUTPUT is not supported (use RETURNING)");
                }
                if update.or.is_some() {
                    return err("UPDATE OR ... is not supported");
                }
                if !update.order_by.is_empty() || update.limit.is_some() {
                    return err("UPDATE ... ORDER BY/LIMIT is not supported");
                }
                let sqlparser::ast::Update {
                    table,
                    assignments,
                    from,
                    selection,
                    returning,
                    ..
                } = update;
                self.exec_update(table, assignments, from, selection, returning)
            }
            Statement::Delete(delete) => {
                if delete.output.is_some() {
                    return err("OUTPUT is not supported (use RETURNING)");
                }
                if !delete.order_by.is_empty() || delete.limit.is_some() {
                    return err("DELETE ... ORDER BY/LIMIT is not supported");
                }
                if !delete.tables.is_empty() {
                    return err(
                        "DELETE target lists are not supported (use DELETE FROM ... USING ...)",
                    );
                }
                let sqlparser::ast::Delete {
                    from,
                    using,
                    selection,
                    returning,
                    ..
                } = delete;
                self.exec_delete(from, using, selection, returning)
            }
            Statement::AlterTable(alter) => self.exec_alter(alter),
            Statement::StartTransaction { .. } => {
                if self.tx_snapshot.is_some() {
                    return err("transaction already in progress");
                }
                self.pager.begin_undo();
                self.tx_snapshot = Some(self.catalog_snapshot());
                Ok(ExecOutcome::Affected(0))
            }
            Statement::Commit { .. } => {
                if self.tx_snapshot.is_none() {
                    return err("no transaction in progress");
                }
                // Durability FIRST, teardown after: consuming the undo
                // journal and catalog snapshot before the fsync used to
                // leave a failed COMMIT unrecoverable — the transaction's
                // deferred frames sat unfenced with no undo left, and the
                // NEXT unrelated commit's fence would silently persist the
                // "failed" transaction while the client retried it. With
                // this order a failed fsync keeps the transaction open
                // (rollback and retry both still possible); the teardown
                // only runs once the boundary is durable (async-commit
                // mode defers that to the background flusher).
                if !self.async_commit {
                    self.pager.sync_wal().map_err(SqlError::from)?;
                    self.pending_sync = false;
                }
                let _ = self.tx_snapshot.take();
                // The page undo journal dies with the transaction.
                self.pager.end_undo();
                // Savepoints die with their transaction: a stale mark must
                // not be reachable from a later transaction's ROLLBACK TO.
                self.savepoints.clear();
                Ok(ExecOutcome::Affected(0))
            }
            Statement::Rollback {
                savepoint: Some(name),
                ..
            } => self.rollback_to_savepoint(&name.value),
            Statement::Rollback {
                savepoint: None, ..
            } => self.rollback_tx(),
            Statement::Savepoint { name } => self.savepoint(&name.value),
            Statement::ReleaseSavepoint { name } => self.release_savepoint(&name.value),
            Statement::CreateIndex(idx) => self.exec_create_index(idx),
            Statement::Merge(m) => {
                if m.output.is_some() {
                    return err("MERGE OUTPUT is not supported (use RETURNING on the statements)");
                }
                self.exec_merge(m.clone())
            }
            Statement::Query(q) => self.exec_query_cx(*q),
            Statement::Explain {
                statement,
                analyze,
                estimate,
                format,
                options,
                ..
            } => self
                .read_cx()
                .explain_outer(&statement, analyze, estimate, &format, &options),
            Statement::Truncate(tr) => {
                // Empty the tables; shape (columns/constraints) is kept.
                // Validate every target (existence, kind, FK children)
                // BEFORE truncating: a mid-list failure must not leave
                // earlier tables already emptied and committed while the
                // failed statement never reaches the journal — replicas
                // would keep rows the origin lost.
                if tr.partitions.is_some() || tr.on_cluster.is_some() {
                    return err("TRUNCATE partitions/ON CLUSTER are not supported");
                }
                if tr.identity == Some(sqlparser::ast::TruncateIdentityOption::Continue) {
                    // The AUTOINCREMENT watermark is derived from the live
                    // rows, so truncation necessarily restarts it; honoring
                    // CONTINUE IDENTITY would require a sequence store the
                    // engine does not have.
                    return err("TRUNCATE ... CONTINUE IDENTITY is not supported");
                }
                let mut targets: Vec<String> =
                    tr.table_names.iter().map(|t| obj_name(&t.name)).collect();
                for t in &tr.table_names {
                    if t.only {
                        return err("TRUNCATE ... ONLY is not supported (no inheritance)");
                    }
                }
                // CASCADE used to be accepted and ignored (the FK guard then
                // rejected the truncate whenever child rows existed). Honor
                // it: referencing tables join the target list transitively,
                // PostgreSQL-style; RESTRICT/no clause keeps the old guard.
                if tr.cascade == Some(sqlparser::ast::CascadeOption::Cascade) {
                    loop {
                        let referencing: Vec<String> = self
                            .tables
                            .iter()
                            .filter(|(t, m)| {
                                !targets.contains(t)
                                    && m.foreign_keys.iter().any(|(_, rt, _)| targets.contains(rt))
                            })
                            .map(|(t, _)| t.clone())
                            .collect();
                        if referencing.is_empty() {
                            break;
                        }
                        targets.extend(referencing);
                    }
                }
                for name in &targets {
                    if !self.tables.contains_key(name) && !tr.if_exists {
                        return err(format!("table {name} does not exist"));
                    }
                    if self.tables.get(name).is_some_and(|m| m.is_view()) {
                        return err(format!(
                            "cannot TRUNCATE view {name} (it is a view, not a table)"
                        ));
                    }
                    if self.tables.get(name).is_some_and(|m| m.is_matview()) {
                        return err(format!(
                            "cannot TRUNCATE materialized view {name} \
                             (REFRESH MATERIALIZED VIEW {name} recomputes it)"
                        ));
                    }
                    if self.tables.contains_key(name) {
                        // TRUNCATE is a full-table DELETE: child rows must
                        // not be orphaned silently (same guard as DELETE).
                        // Co-truncated tables are excluded — the whole list
                        // is validated before any table is emptied, so
                        // `TRUNCATE parent, child` must not fail on the
                        // parent's rows the child still references.
                        let removed = self.table_docs_cx(name)?;
                        self.check_fk_parent_delete_skipping(name, &removed, &[], &targets, None)?;
                    }
                }
                // One pager transaction for the WHOLE batch: each table's
                // rewrite used to commit its own transaction, so a mid-list
                // failure left earlier tables emptied AND committed while
                // the failed statement never reached the journal — replicas
                // kept rows the origin lost. Any failure now aborts the
                // batch and restores the in-memory catalog snapshot.
                let touched: Vec<String> = targets
                    .iter()
                    .filter(|n| self.tables.contains_key(*n))
                    .cloned()
                    .collect();
                let catalog_snapshot: Vec<(String, Option<std::sync::Arc<TableMeta>>)> = touched
                    .iter()
                    .map(|n| (n.clone(), self.tables.get(n).cloned()))
                    .collect();
                let mut tx = self.pager.begin_tx();
                let batch = (|| -> Result<()> {
                    for name in &touched {
                        let mut meta = self
                            .tables
                            .get(name)
                            .map(|a| a.as_ref().clone())
                            .unwrap_or_default();
                        self.rewrite_table_tx(
                            &mut tx,
                            name,
                            &mut meta,
                            Vec::new(),
                            true,
                            Vec::new(),
                        )?;
                    }
                    Ok(())
                })();
                match batch {
                    Ok(()) => {
                        self.commit_pager_tx(tx)?;
                    }
                    Err(e) => {
                        for (name, old) in catalog_snapshot {
                            match old {
                                Some(m) => {
                                    self.tables.insert(name, m);
                                }
                                None => {
                                    self.tables.remove(&name);
                                }
                            }
                        }
                        self.pager.abort_tx(tx)?;
                        return Err(e);
                    }
                }
                Ok(ExecOutcome::Affected(0))
            }
            Statement::Pragma { .. } => {
                // Compatibility shim: accept and ignore PRAGMA statements
                // (SQLite-dialect callers, e.g. EF Core startup probes).
                Ok(ExecOutcome::Affected(0))
            }
            other => err(format!("unsupported statement: {other}")),
        }
    }

    /// Rewrite the whole table from `docs` (delete+reinsert). Rebuilds every
    /// index tree for the table, updates `meta` in place, and re-inserts it
    /// into the catalog. The old heap pages and index-tree pages are released
    /// into the pager's reusable pool inside the same transaction, so a
    /// rebuild reuses its own storage instead of growing the file.
    fn rewrite_table(
        &mut self,
        table: &str,
        meta: &mut TableMeta,
        docs: Vec<Object>,
    ) -> Result<()> {
        self.rewrite_table_with_dead(table, meta, docs, Vec::new())
    }

    /// Like [`Self::rewrite_table`], but also releases the page trees of
    /// indexes that were removed from `meta` BEFORE this rebuild (DROP
    /// COLUMN): their roots are no longer in `meta.index_roots`, so the
    /// generic free loop below would skip them and the pages would leak for
    /// good — nothing else ever references them. Freed in the SAME write
    /// unit as the catalog switch, so a crash can never leave a committed
    /// catalog pointing at recycled pages.
    fn rewrite_table_with_dead(
        &mut self,
        table: &str,
        meta: &mut TableMeta,
        docs: Vec<Object>,
        dead_trees: Vec<(String, u32)>,
    ) -> Result<()> {
        self.rewrite_table_inner(table, meta, docs, true, dead_trees)
    }

    fn rewrite_table_inner(
        &mut self,
        table: &str,
        meta: &mut TableMeta,
        docs: Vec<Object>,
        free_old: bool,
        dead_trees: Vec<(String, u32)>,
    ) -> Result<()> {
        let mut tx = self.pager.begin_tx();
        if let Err(e) = self.rewrite_table_tx(&mut tx, table, meta, docs, free_old, dead_trees) {
            // Same failure semantics as the old inline body: the staged
            // pages never reach the data file (Tx's drop restores them),
            // and the in-memory catalog entry was already restored by
            // rewrite_table_tx's save-failure path or never swapped.
            self.pager.abort_tx(tx)?;
            return Err(e);
        }
        self.commit_pager_tx(tx)?;
        Ok(())
    }

    /// [`Self::rewrite_table_inner`] on a CALLER-OWNED transaction: the
    /// TRUNCATE batch rewrites several tables inside ONE pager transaction
    /// so a mid-list failure rolls the whole statement back (each table
    /// used to commit its own rebuild, leaving earlier tables emptied and
    /// committed while the failed statement never reached the journal —
    /// replicas kept rows the origin lost).
    fn rewrite_table_tx(
        &mut self,
        tx: &mut crate::pager::Tx,
        table: &str,
        meta: &mut TableMeta,
        docs: Vec<Object>,
        free_old: bool,
        dead_trees: Vec<(String, u32)>,
    ) -> Result<()> {
        let mut heap = Heap {
            pages: Vec::new(),
            overflow_free: meta.overflow_free.clone(),
            dropped: Vec::new(),
        };
        let cols: Vec<String> = meta.index_roots.keys().cloned().collect();
        // Release old storage BEFORE the rebuild: allocations below then pop
        // those pages back (same transaction, so the catalog switch and the
        // page rewrites are atomic; snapshots rebuild old versions from the
        // WAL and never observe the reuse).
        if free_old {
            // The docs being rewritten carried live overflow chains anchored
            // only by the old slots' head pointers; free them too or every
            // rewrite/TRUNCATE of a table with oversized documents orphans
            // the chains (the file grows without bound under churn).
            crate::heap::free_overflow_chains(&self.pager, tx, &meta.pages)?;
            for &p in &meta.pages {
                self.pager.free_page(tx, p)?;
            }
            for (root_key, &root) in &meta.index_roots {
                for p in BTree::open(root)
                    .collect_pages(&PageReader::current(&self.pager), tx)
                    .map_err(|e| index_err(root_key, e))?
                {
                    self.pager.free_page(tx, p)?;
                }
            }
            for (root_key, root) in &dead_trees {
                for p in BTree::open(*root)
                    .collect_pages(&PageReader::current(&self.pager), tx)
                    .map_err(|e| index_err(root_key, e))?
                {
                    self.pager.free_page(tx, p)?;
                }
            }
        }
        let mut pairs: Vec<(u64, Object)> = Vec::with_capacity(docs.len());
        for doc in &docs {
            // Rebuilds walk every row (ALTER/RENAME on a large table must
            // stay interruptible like every other long row loop).
            self.stmt_deadline.check()?;
            let loc = heap.insert(&self.pager, tx, doc)?;
            pairs.push((loc, doc.clone()));
        }
        let roots = self.build_trees(
            tx,
            &pairs,
            &cols,
            &cols
                .iter()
                .map(|c| meta.index_columns_of(c))
                .collect::<Vec<_>>(),
            &cols
                .iter()
                .map(|k| {
                    meta.index_defs
                        .iter()
                        .find(|d| d.name == *k)
                        .and_then(|d| d.path.clone())
                })
                .collect::<Vec<_>>(),
            &cols
                .iter()
                .map(|c| meta.root_key_unique(c))
                .collect::<Vec<_>>(),
            // Partial-index predicates: the rebuilt tree must hold exactly
            // the qualifying rows or every gated probe reads wrong.
            &cols
                .iter()
                .map(|k| {
                    meta.index_defs
                        .iter()
                        .find(|d| d.name == *k)
                        .and_then(|d| d.pred.as_deref())
                        .and_then(|p| parse_expr_text(p).ok())
                })
                .collect::<Vec<_>>(),
        )?;
        // Keep the previous catalog entry so a failed persist can restore the
        // in-memory map — after `?` below the tx is dropped and its staged
        // pages never reach the data file; the map must not keep pointing at
        // them, or a later successful save would persist the ghost pages and
        // orphan the real data.
        let prev_meta = self.tables.get(table).cloned();
        meta.pages = heap.pages;
        meta.index_roots = roots;
        // The rebuild drained the recycled-chain free list above: every
        // oversized document popped a page from it. Persist what is left —
        // keeping the pre-rebuild list would advertise pages the rebuild just
        // made live, and the next oversized insert would overwrite a live
        // document's chain (reads then fail with "overflow chain corrupt" or
        // return another row's bytes). The DML paths have always written the
        // heap's list back through `sync_table_layout`; the rebuild is the
        // one that did not.
        meta.overflow_free = heap.overflow_free;
        self.autoinc_cache.remove(table);
        self.tables
            .insert(table.to_string(), std::sync::Arc::new(meta.clone()));
        if let Err(e) = self.save_catalog_into(tx) {
            match prev_meta {
                Some(old) => {
                    *meta = old.as_ref().clone();
                    self.tables.insert(table.to_string(), old);
                }
                None => {
                    self.tables.remove(table);
                }
            }
            return Err(e);
        }
        Ok(())
    }

    /// Build one B+ tree per (root_key, columns, unique) spec, populated
    /// from the table's (locator, document) pairs. Single-column specs key
    /// by the bare value (legacy layout); multi-column specs key by
    /// `Value::Array` over the columns in order. Unique specs reject
    /// duplicates at insert time.
    #[allow(clippy::too_many_arguments)]
    fn build_trees(
        &mut self,
        tx: &mut crate::pager::Tx,
        pairs: &[(u64, Object)],
        root_keys: &[String],
        cols_per_key: &[Vec<String>],
        paths_per_key: &[Option<String>],
        unique_keys: &[bool],
        preds_per_key: &[Option<SqlExpr>],
    ) -> Result<std::collections::BTreeMap<String, u32>> {
        let mut roots = std::collections::BTreeMap::new();
        for i in 0..root_keys.len() {
            let root_key = &root_keys[i];
            let cols = &cols_per_key[i];
            let path = paths_per_key[i].as_deref();
            let unique = unique_keys[i];
            let pred = preds_per_key[i].as_ref();
            let mut tree = BTree::create(&self.pager, tx).map_err(|e| index_err(root_key, e))?;
            for (loc, doc) in pairs {
                // The backfill is the expensive half of CREATE INDEX on a big
                // table (the materialization before it samples already) —
                // without a check here the statement was uninterruptible
                // once it entered the tree builds.
                self.stmt_deadline.check()?;
                // Partial index: only pred-satisfying rows enter the tree
                // (NULL/missing fields fail the predicate like any WHERE).
                if let Some(p) = pred {
                    if !matches!(eval_expr(p, doc)?, Value::Bool(true)) {
                        continue;
                    }
                }
                let key = match path {
                    // JSON path index: extract from the column's JSON text
                    // with the exact lookup JSON_EXTRACT uses (design 005).
                    Some(p) => json_path_key(doc, &cols[0], p),
                    None => index_key_of(doc, cols),
                };
                if let Some(v) = key {
                    tree.insert(&self.pager, tx, v, *loc, unique)
                        .map_err(|e| index_err(root_key, e))?;
                }
            }
            roots.insert(root_key.clone(), tree.root);
        }
        Ok(roots)
    }

    fn autoinc_next_for(&mut self, table: &str, meta: &TableMeta) -> Result<i64> {
        if let Some(&n) = self.autoinc_cache.get(table) {
            return Ok(n);
        }
        let mut max: i64 = 0;
        if let Some(col) = &meta.autoinc {
            // Propagate read errors: defaulting to an empty scan would cache
            // a reset counter and hand out ids that collide after restart.
            let docs = Heap {
                pages: meta.pages.clone(),
                overflow_free: meta.overflow_free.clone(),
                dropped: Vec::new(),
            }
            .scan(&PageReader::current(&self.pager))?;
            for d in &docs {
                if let Some(Value::Int(i)) = d.get(col) {
                    max = max.max(*i);
                }
            }
        }
        let next = max + 1;
        self.autoinc_cache.insert(table.to_string(), next);
        Ok(next)
    }

    #[allow(clippy::too_many_arguments)]
    fn exec_update(
        &mut self,
        table: sqlparser::ast::TableWithJoins,
        mut assignments: Vec<sqlparser::ast::Assignment>,
        from: Option<sqlparser::ast::UpdateTableFromKind>,
        mut selection: Option<SqlExpr>,
        update_returning: Option<Vec<SelectItem>>,
    ) -> Result<ExecOutcome> {
        let sqlparser::ast::TableFactor::Table { name, alias, .. } = table.relation else {
            return err("only simple table names in UPDATE");
        };
        // MySQL-style `UPDATE t JOIN u ON … SET …` parses with the join in
        // `table.joins`; ignoring it would update every row of `t` with
        // values evaluated without `u`. Reject loudly instead.
        if !table.joins.is_empty() {
            return err("UPDATE ... JOIN is not supported (use UPDATE ... FROM)");
        }
        let tname = obj_name(&name);
        if self.tables.get(&tname).is_some_and(|m| m.is_view()) {
            return err(format!(
                "cannot UPDATE view {tname} (it is a view, not a table)"
            ));
        }
        if self.tables.get(&tname).is_some_and(|m| m.is_matview()) {
            return err(format!(
                "cannot UPDATE materialized view {tname} \
                 (REFRESH MATERIALIZED VIEW {tname} recomputes it)"
            ));
        }
        let tkey = alias
            .as_ref()
            .map(|a| a.name.value.clone())
            .unwrap_or_else(|| tname.clone());
        // ROWNUM has no row stream to number in an UPDATE's own SET/WHERE:
        // it reads NULL, and `NULL <= n` is TRUE under the documented total
        // order — an Oracle-style `UPDATE ... WHERE ROWNUM <= 1` would
        // silently rewrite every row. Fail loudly instead.
        if assignments.iter().any(|a| refs_rownum(&a.value))
            || selection.as_ref().is_some_and(refs_rownum)
        {
            return err("ROWNUM is not supported in UPDATE (use a subquery: WHERE id IN (SELECT ... WHERE ROWNUM <= n))");
        }
        // Outer scope for correlated subqueries: the target (name + alias)
        // plus the FROM tables (the FROM form evaluates WHERE/SET over the
        // merged image).
        let mut outer = OuterFrom::new();
        outer.insert(tname.clone());
        outer.insert(tkey.clone());
        if let Some(kind) = &from {
            match kind {
                sqlparser::ast::UpdateTableFromKind::BeforeSet(twjs)
                | sqlparser::ast::UpdateTableFromKind::AfterSet(twjs) => {
                    from_list_names(twjs, &mut outer);
                }
            }
        }
        for a in &mut assignments {
            self.subst_expr_cx_scoped(&mut a.value, &outer)?;
        }
        if let Some(sel) = &mut selection {
            self.subst_expr_cx_scoped(sel, &outer)?;
        }
        let mut meta = self
            .tables
            .get(&tname)
            .map(|a| a.as_ref().clone())
            .unwrap_or_default();

        // Fast path: probe-able WHERE on an indexed column — update the
        // affected rows in place instead of rewriting the whole table.
        if from.is_none() {
            if let Some(matches) =
                self.index_probe_cx(&tname, Some(tkey.as_str()), &selection, &Ctes::new())?
            {
                return self.exec_update_fast(
                    tname,
                    meta,
                    matches,
                    &assignments,
                    &selection,
                    &update_returning,
                    &outer,
                );
            }
        }

        let docs = self.table_docs_cx(&tname)?;
        let (out, changed_docs, old_changed, count) = match from {
            None => {
                // Plain UPDATE: assignments and WHERE see only target columns.
                let mut out = Vec::new();
                let mut changed_docs: Vec<Object> = Vec::new();
                let mut old_changed: Vec<Object> = Vec::new();
                let mut count = 0u64;
                for doc in docs {
                    if self.matches_cx_in(&selection, &doc, &outer)? {
                        old_changed.push(doc.clone());
                        let new_vals = self.eval_assignments_cx(&assignments, &doc, &outer)?;
                        let mut doc = doc;
                        for (col_name, v) in new_vals {
                            doc.insert(col_name, v);
                        }
                        count += 1;
                        changed_docs.push(doc.clone());
                        out.push(doc);
                    } else {
                        out.push(doc);
                    }
                }
                (out, changed_docs, old_changed, count)
            }
            Some(kind) => {
                // UPDATE ... FROM: each target row is matched against the
                // FROM tables' joined rows; the first qualifying match
                // supplies the values.
                let extra = match kind {
                    sqlparser::ast::UpdateTableFromKind::BeforeSet(v)
                    | sqlparser::ast::UpdateTableFromKind::AfterSet(v) => v,
                };
                let mut from_list = vec![sqlparser::ast::TableWithJoins {
                    relation: sqlparser::ast::TableFactor::Table {
                        name: name.clone(),
                        alias: alias.clone(),
                        args: None,
                        with_hints: vec![],
                        version: None,
                        with_ordinality: false,
                        partitions: vec![],
                        json_path: None,
                        sample: None,
                        index_hints: vec![],
                    },
                    joins: vec![],
                }];
                from_list.extend(extra);
                let merged = self.load_from_cx(&from_list, &None, &Ctes::new())?;
                let prefix = format!("{tkey}.");
                // First qualifying match per target content: every output
                // row with that content receives that image — including
                // byte-identical duplicate rows, which are distinct rows
                // despite sharing a key.
                let mut updates: std::collections::HashMap<Vec<u8>, Object> =
                    std::collections::HashMap::new();
                let mut changed_docs: Vec<Object> = Vec::new();
                let mut old_changed: Vec<Object> = Vec::new();
                for m in &merged {
                    let tdoc = target_doc_from_merged(m, &prefix);
                    let key = object_key(&tdoc);
                    if updates.contains_key(&key) {
                        continue; // first qualifying match wins
                    }
                    if !self.matches_cx_in(&selection, m, &outer)? {
                        continue;
                    }
                    old_changed.push(tdoc.clone());
                    let new_vals = self.eval_assignments_cx(&assignments, m, &outer)?;
                    let mut tdoc = tdoc;
                    for (col_name, v) in new_vals {
                        tdoc.insert(col_name, v);
                    }
                    changed_docs.push(tdoc.clone());
                    updates.insert(key, tdoc);
                }
                let mut count = 0u64;
                let out: Vec<Object> = docs
                    .into_iter()
                    .map(|doc| match updates.get(&object_key(&doc)) {
                        Some(img) => {
                            count += 1;
                            img.clone()
                        }
                        None => doc,
                    })
                    .collect();
                (out, changed_docs, old_changed, count)
            }
        };
        // Validate BEFORE writing: a failed UPDATE must not change data.
        // 自引用 FK 的父候选是本语句的最终像集合(不只是表里的旧像)。
        for doc in &out {
            // Full-table UPDATEs walk this loop (plus a parent-table scan
            // per FK inside check_fks_in) — keep the timeout armed.
            self.stmt_deadline.check()?;
            meta.check(doc)?;
            self.check_fks_in(&tname, &meta, doc, &out, Some(&out))?;
        }
        meta.check_unique(&out)?;
        // Parent-side FK: key values that disappear must not be referenced.
        // The self-reference child scan runs over the final images (`out`
        // IS the whole final table here — untouched rows included).
        self.check_fk_parent_delete_skipping(&tname, &old_changed, &out, &[], Some(&out))?;
        let changed = changed_docs.clone();
        // Row triggers: BEFORE over the pre/post image pairs while the old
        // state is still the committed truth, AFTER once the rewrite landed.
        let changed_cols: Vec<String> = assignments
            .iter()
            .filter_map(|a| match &a.target {
                sqlparser::ast::AssignmentTarget::ColumnName(c) => Some(obj_name(c)),
                _ => None,
            })
            .collect();
        let fire_update = !changed_docs.is_empty() && self.triggers_exist_for(&tname);
        if fire_update {
            for (old, new) in old_changed.iter().zip(changed_docs.iter()) {
                self.fire_row_triggers(
                    &tname,
                    true,
                    TriggerFireEvent::Update(Some(&changed_cols)),
                    Some(old),
                    Some(new),
                )?;
            }
        }
        // Sticky dotted-field marker on the LOCAL meta clone: rewrite_table
        // persists exactly this meta, so a self.tables write here would be
        // overwritten by the rewrite's own catalog save.
        if !meta.dotted_keys && out.iter().any(|d| d.keys().any(|k| k.contains('.'))) {
            meta.dotted_keys = true;
        }
        self.rewrite_table(&tname, &mut meta, out)?;
        if fire_update {
            for (old, new) in old_changed.iter().zip(changed_docs.iter()) {
                self.fire_row_triggers(
                    &tname,
                    false,
                    TriggerFireEvent::Update(Some(&changed_cols)),
                    Some(old),
                    Some(new),
                )?;
            }
        }
        if let Some(ret) = &update_returning {
            return project_returning(ret, &changed);
        }
        Ok(ExecOutcome::Affected(count))
    }

    #[allow(clippy::too_many_arguments)]
    fn exec_update_fast(
        &mut self,
        tname: String,
        meta: TableMeta,
        matches: Vec<(u64, Object)>,
        assignments: &[sqlparser::ast::Assignment],
        selection: &Option<SqlExpr>,
        update_returning: &Option<Vec<SelectItem>>,
        outer: &OuterFrom,
    ) -> Result<ExecOutcome> {
        let mut updates: Vec<(u64, Object, Object)> = Vec::new(); // (loc, old, new)
        for (loc, doc) in matches {
            if !self.matches_cx_in(selection, &doc, outer)? {
                continue; // probe col matched; some other conjunct did not
            }
            let mut doc = doc;
            // The pre-update image must be captured before assignments run —
            // it drives the old-key removal in the apply loop below; pushing
            // the mutated doc as both old and new leaves stale entries.
            let old_doc = doc.clone();
            let new_vals = self.eval_assignments_cx(assignments, &doc, outer)?;
            for (col_name, v) in new_vals {
                doc.insert(col_name, v);
            }
            meta.check(&doc)?;
            updates.push((loc, old_doc, doc));
        }
        // 子侧 FK 在收集完本语句全部最终像之后统一校验:自引用/行间引用
        // 的父候选包含批次自身。零命中在下方早退(空批次检查是 no-op)。
        if updates.is_empty() {
            if let Some(ret) = update_returning {
                return project_returning(ret, &[]);
            }
            return Ok(ExecOutcome::Affected(0));
        }
        // Self-referential FKs resolve parents against the statement's
        // COMPLETE final state (pre-state minus replaced old images plus the
        // new images); the pre-state heap alone would satisfy keys this
        // statement vacates (`SET id = 3, p = 1 WHERE id = 1`). Only
        // self-FK tables pay the extra table load.
        let self_final: Option<Vec<Object>> =
            if meta.foreign_keys.iter().any(|(_, rt, _)| rt == &tname) {
                let old_keys: std::collections::BTreeSet<Vec<u8>> =
                    updates.iter().map(|(_, o, _)| object_key(o)).collect();
                let mut final_state = self.table_docs_cx(&tname)?;
                final_state.retain(|d| !old_keys.contains(&object_key(d)));
                final_state.extend(updates.iter().map(|(_, _, n)| n.clone()));
                Some(final_state)
            } else {
                None
            };
        {
            let batch: Vec<Object> = updates.iter().map(|(_, _, n)| n.clone()).collect();
            for doc in &batch {
                // Big UPDATEs (with a FK, check_fks_in also scans the parent
                // table per row) must stay interruptible.
                self.stmt_deadline.check()?;
                self.check_fks_in(&tname, &meta, doc, &batch, self_final.as_deref())?;
            }
        }
        // Parent-side FK check before any page is touched. `updates` rows
        // are the only ones whose key values can disappear; their new docs
        // are the survivors (referenced columns are unique in practice).
        let old_docs: Vec<Object> = updates.iter().map(|(_, o, _)| o.clone()).collect();
        let new_docs: Vec<Object> = updates.iter().map(|(_, _, n)| n.clone()).collect();
        self.check_fk_parent_delete_skipping(
            &tname,
            &old_docs,
            &new_docs,
            &[],
            self_final.as_deref(),
        )?;
        // Pre-statement docs for the legacy whole-set unique check below —
        // loaded only when that check can actually run (a constraint column
        // without a tree). Tables with full trees enforce uniqueness in the
        // apply loop; loading every doc here made single-row UPDATE loops
        // quadratic.
        let legacy_check = (meta.primary_key.is_some() || !meta.unique.is_empty())
            && meta
                .primary_key
                .iter()
                .chain(meta.unique.iter())
                .any(|c| !meta.index_roots.contains_key(c));
        let all_docs: Option<Vec<Object>> = if legacy_check {
            Some(self.table_docs_cx(&tname)?)
        } else {
            None
        };
        let mut heap = Heap {
            pages: meta.pages.clone(),
            overflow_free: meta.overflow_free.clone(),
            dropped: Vec::new(),
        };
        let mut roots = meta.index_roots.clone();
        let idx_specs = idx_specs(&meta, &roots);
        // BEFORE UPDATE over the pre/post pairs while the old images are
        // still the committed truth (rows update in place below).
        let update_changed_cols: Vec<String> = assignments
            .iter()
            .filter_map(|a| match &a.target {
                sqlparser::ast::AssignmentTarget::ColumnName(c) => Some(obj_name(c)),
                _ => None,
            })
            .collect();
        let fire_update = self.triggers_exist_for(&tname);
        if fire_update {
            for (_, old, new) in &updates {
                self.fire_row_triggers(
                    &tname,
                    true,
                    TriggerFireEvent::Update(Some(&update_changed_cols)),
                    Some(old),
                    Some(new),
                )?;
            }
        }
        let mut tx = self.pager.begin_tx();
        // Pass 1: drop every updated row's old index entries before any
        // replacement lands — a multi-row unique-key shift (SET id = id + 1,
        // key swaps between rows) must not collide with a not-yet-replaced
        // row's still-present old key.
        for (loc, old_doc, _) in &updates {
            if let Err(e) =
                reindex_remove(&self.pager, &mut tx, &idx_specs, &mut roots, old_doc, *loc)
            {
                self.pager.abort_tx(tx)?;
                return Err(e);
            }
        }
        // Pass 2: apply the new images.
        for i in 0..updates.len() {
            let (loc, new_doc) = (updates[i].0, updates[i].2.clone());
            let page = crate::heap::unpack_loc(loc).0;
            let before = heap.page_docs(&PageReader::current(&self.pager), &tx, page)?;
            // Locators of the rows not yet processed *before* the repack
            // fixup below mutates them: a survivor move naming one of these
            // slots is the pending row's own image, and repointing it would
            // resurrect the old-key entry pass 1 just removed.
            let pending_locs: Vec<u64> = updates.iter().skip(i + 1).map(|(l, _, _)| *l).collect();
            let out = heap.replace(&self.pager, &mut tx, loc, &new_doc)?;
            // An in-page re-pack moved this page's survivors: pending locators
            // must follow, or a later update would target whatever document
            // now occupies the stale slot.
            if !out.moved.is_empty() {
                for pending in updates.iter_mut().skip(i + 1) {
                    if let Some((_, new_l)) = out.moved.iter().find(|(old, _)| *old == pending.0) {
                        pending.0 = *new_l;
                    }
                }
            }
            for (old_l, new_l) in &out.moved {
                // Current row: entries removed in pass 1, new entry lands at
                // `out.placed` below. Pending row: same, in its own
                // iteration. Already-processed rows (and untouched rows)
                // must be repointed — their new/current image is what sat in
                // that slot, and skipping them leaves a stale index entry.
                if *old_l == loc || pending_locs.contains(old_l) {
                    continue;
                }
                if let Err(e) = reindex_repoint(
                    &self.pager,
                    &mut tx,
                    &idx_specs,
                    &mut roots,
                    &before,
                    *old_l,
                    *new_l,
                ) {
                    self.pager.abort_tx(tx)?;
                    return Err(e);
                }
            }
            if let Err(e) = reindex_insert(
                &self.pager,
                &mut tx,
                &idx_specs,
                &mut roots,
                &new_doc,
                out.placed,
            ) {
                self.pager.abort_tx(tx)?;
                return Err(e);
            }
        }
        // Legacy files whose constraint columns predate trees keep the
        // whole-set duplicate check (mirrors the INSERT path); tables with
        // trees enforce uniqueness through the apply loop above.
        if legacy_check {
            let combined: Vec<Object> = all_docs
                .expect("pre-statement docs loaded for the legacy check")
                .into_iter()
                .map(|d| match updates.iter().find(|(_, old, _)| *old == d) {
                    Some((_, _, new)) => new.clone(),
                    None => d,
                })
                .collect();
            if let Err(e) = meta.check_unique(&combined) {
                self.pager.abort_tx(tx)?;
                return Err(e);
            }
        }
        // Emptied heap pages are released inside this transaction so later
        // statements reuse them instead of growing the file.
        for p in std::mem::take(&mut heap.dropped) {
            self.pager.free_page(&mut tx, p)?;
        }
        let dotted_now = self.note_dotted_fields(&tname, updates.iter().map(|(_, _, new)| new));
        if dotted_now
            || heap.pages != meta.pages
            || roots != meta.index_roots
            || heap.overflow_free != meta.overflow_free
        {
            self.sync_table_layout(
                &mut tx,
                &tname,
                heap.pages.clone(),
                roots,
                heap.overflow_free.clone(),
            )?;
        }
        self.commit_pager_tx(tx)?;
        // AFTER UPDATE once the new images are committed.
        if fire_update {
            for (_, old, new) in &updates {
                self.fire_row_triggers(
                    &tname,
                    false,
                    TriggerFireEvent::Update(Some(&update_changed_cols)),
                    Some(old),
                    Some(new),
                )?;
            }
        }
        // Explicit ids may bump the AUTOINCREMENT watermark.
        if let Some(col) = &meta.autoinc {
            if assignments
                .iter()
                .any(|a| matches!(&a.target, sqlparser::ast::AssignmentTarget::ColumnName(c) if obj_name(c) == *col))
            {
                self.autoinc_cache.remove(&tname);
            }
        }
        let changed: Vec<Object> = updates.into_iter().map(|(_, _, d)| d).collect();
        if let Some(ret) = update_returning {
            return project_returning(ret, &changed);
        }
        Ok(ExecOutcome::Affected(changed.len() as u64))
    }

    fn exec_delete(
        &mut self,
        from: sqlparser::ast::FromTable,
        using: Option<Vec<sqlparser::ast::TableWithJoins>>,
        mut selection: Option<SqlExpr>,
        returning: Option<Vec<SelectItem>>,
    ) -> Result<ExecOutcome> {
        let sqlparser::ast::FromTable::WithFromKeyword(tables) = from else {
            return err("unsupported DELETE form");
        };
        if tables.len() != 1 {
            return err("DELETE from exactly one table");
        }
        // `DELETE t FROM t JOIN u …` / `DELETE FROM t JOIN u …` parse with
        // the join in `tables[0].joins`; dropping it would delete every row
        // regardless of the join condition. Reject loudly (use USING).
        if !tables[0].joins.is_empty() {
            return err("DELETE ... JOIN is not supported (use DELETE ... USING)");
        }
        let sqlparser::ast::TableFactor::Table { name, alias, .. } = tables[0].relation.clone()
        else {
            return err("only simple table names in DELETE");
        };
        let tname = obj_name(&name);
        if self.tables.get(&tname).is_some_and(|m| m.is_view()) {
            return err(format!(
                "cannot DELETE from view {tname} (it is a view, not a table)"
            ));
        }
        if self.tables.get(&tname).is_some_and(|m| m.is_matview()) {
            return err(format!(
                "cannot DELETE from materialized view {tname} \
                 (REFRESH MATERIALIZED VIEW {tname} recomputes it)"
            ));
        }
        let tkey = alias
            .as_ref()
            .map(|a| a.name.value.clone())
            .unwrap_or_else(|| tname.clone());
        // ROWNUM has no row stream to number in a DELETE's own WHERE: it
        // reads NULL, and `NULL <= n` is TRUE under the documented total
        // order — an Oracle-style `DELETE ... WHERE ROWNUM <= 1` would
        // silently mass-delete. Fail loudly instead.
        if selection.as_ref().is_some_and(refs_rownum) {
            return err("ROWNUM is not supported in DELETE (use a subquery: WHERE id IN (SELECT ... WHERE ROWNUM <= n))");
        }
        // Outer scope for correlated subqueries: the target (name + alias)
        // plus the USING tables (the USING form evaluates WHERE over the
        // merged image).
        let mut outer = OuterFrom::new();
        outer.insert(tname.clone());
        outer.insert(tkey.clone());
        if let Some(using) = &using {
            from_list_names(using, &mut outer);
        }
        if let Some(sel) = &mut selection {
            self.subst_expr_cx_scoped(sel, &outer)?;
        }
        let mut meta = self
            .tables
            .get(&tname)
            .map(|a| a.as_ref().clone())
            .unwrap_or_default();
        // Fast path: probe-able WHERE on an indexed column — remove the
        // matching rows in place (page re-pack) instead of rewriting the
        // whole table.
        if using.is_none() {
            if let Some(matches) =
                self.index_probe_cx(&tname, Some(tkey.as_str()), &selection, &Ctes::new())?
            {
                return self.exec_delete_fast(tname, meta, matches, &selection, &returning, &outer);
            }
        }
        let docs = self.table_docs_cx(&tname)?;
        let (kept, removed) = if let Some(using) = using {
            // DELETE ... USING: qualify over the joined rows and mark the
            // target docs whose combination satisfies WHERE.
            let mut from_list = vec![tables[0].clone()];
            from_list.extend(using);
            let merged = self.load_from_cx(&from_list, &None, &Ctes::new())?;
            let prefix = format!("{tkey}.");
            let mut rm: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
            for m in &merged {
                if !self.matches_cx_in(&selection, m, &outer)? {
                    continue;
                }
                let tdoc = target_doc_from_merged(m, &prefix);
                rm.insert(object_key(&tdoc));
            }
            let mut kept = Vec::new();
            let mut removed = Vec::new();
            for doc in docs {
                if rm.contains(&object_key(&doc)) {
                    removed.push(doc);
                } else {
                    kept.push(doc);
                }
            }
            (kept, removed)
        } else {
            let mut kept = Vec::new();
            let mut removed = Vec::new();
            for doc in docs {
                if self.matches_cx_in(&selection, &doc, &outer)? {
                    removed.push(doc);
                } else {
                    kept.push(doc);
                }
            }
            (kept, removed)
        };
        let count = removed.len() as u64;
        // The self-reference child scan runs over the FINAL state (`kept`):
        // rows deleted by this same statement cannot end up orphaned, so
        // `DELETE FROM self_ref_table` may clear parent and child rows
        // together (SQLite/PG statement-final semantics).
        self.check_fk_parent_delete_skipping(&tname, &removed, &[], &[], Some(&kept))?;
        // BEFORE DELETE while the rows are still the committed truth.
        let fire_delete = !removed.is_empty() && self.triggers_exist_for(&tname);
        if fire_delete {
            for doc in &removed {
                self.fire_row_triggers(&tname, true, TriggerFireEvent::Delete, Some(doc), None)?;
            }
        }
        self.rewrite_table(&tname, &mut meta, kept)?;
        if fire_delete {
            for doc in &removed {
                self.fire_row_triggers(&tname, false, TriggerFireEvent::Delete, Some(doc), None)?;
            }
        }
        if let Some(ret) = &returning {
            return project_returning(ret, &removed);
        }
        Ok(ExecOutcome::Affected(count))
    }

    fn exec_delete_fast(
        &mut self,
        tname: String,
        meta: TableMeta,
        matches: Vec<(u64, Object)>,
        selection: &Option<SqlExpr>,
        returning: &Option<Vec<SelectItem>>,
        outer: &OuterFrom,
    ) -> Result<ExecOutcome> {
        let mut targets: Vec<(u64, Object)> = Vec::new();
        for (loc, doc) in matches {
            if self.matches_cx_in(selection, &doc, outer)? {
                targets.push((loc, doc));
            }
        }
        if targets.is_empty() {
            if let Some(ret) = returning {
                return project_returning(ret, &[]);
            }
            return Ok(ExecOutcome::Affected(0));
        }
        // Parent-side FK check before any page is touched. The self-reference
        // child scan runs over the final state (pre-state minus the removed
        // rows), so deleting parent and child rows of a self-FK table in one
        // statement is legal; only self-FK tables pay the extra table load.
        let removed_docs: Vec<Object> = targets.iter().map(|(_, d)| d.clone()).collect();
        let self_final: Option<Vec<Object>> =
            if meta.foreign_keys.iter().any(|(_, rt, _)| rt == &tname) {
                let rm: std::collections::BTreeSet<Vec<u8>> =
                    targets.iter().map(|(_, d)| object_key(d)).collect();
                Some(
                    self.table_docs_cx(&tname)?
                        .into_iter()
                        .filter(|d| !rm.contains(&object_key(d)))
                        .collect(),
                )
            } else {
                None
            };
        self.check_fk_parent_delete_skipping(
            &tname,
            &removed_docs,
            &[],
            &[],
            self_final.as_deref(),
        )?;
        let mut heap = Heap {
            pages: meta.pages.clone(),
            overflow_free: meta.overflow_free.clone(),
            dropped: Vec::new(),
        };
        let mut roots = meta.index_roots.clone();
        let idx_specs = idx_specs(&meta, &roots);
        // BEFORE DELETE while the rows are still the committed truth.
        let fire_delete = self.triggers_exist_for(&tname);
        if fire_delete {
            for (_, doc) in &targets {
                self.fire_row_triggers(&tname, true, TriggerFireEvent::Delete, Some(doc), None)?;
            }
        }
        let mut tx = self.pager.begin_tx();
        let affected: std::collections::BTreeSet<u32> = targets
            .iter()
            .map(|(l, _)| crate::heap::unpack_loc(*l).0)
            .collect();
        let mut before = Vec::new();
        for pid in affected {
            before.extend(heap.page_docs(&PageReader::current(&self.pager), &tx, pid)?);
        }
        let locs: Vec<u64> = targets.iter().map(|(l, _)| *l).collect();
        let moves = heap.remove_many(&self.pager, &mut tx, &locs)?;
        for (loc, doc) in &targets {
            reindex_remove(&self.pager, &mut tx, &idx_specs, &mut roots, doc, *loc)?;
        }
        for (old_l, new_l) in &moves {
            reindex_repoint(
                &self.pager,
                &mut tx,
                &idx_specs,
                &mut roots,
                &before,
                *old_l,
                *new_l,
            )?;
        }
        // Emptied heap pages are released inside this transaction so later
        // statements reuse them instead of growing the file.
        for p in std::mem::take(&mut heap.dropped) {
            self.pager.free_page(&mut tx, p)?;
        }
        if heap.pages != meta.pages
            || roots != meta.index_roots
            || heap.overflow_free != meta.overflow_free
        {
            self.sync_table_layout(
                &mut tx,
                &tname,
                heap.pages.clone(),
                roots,
                heap.overflow_free.clone(),
            )?;
        }
        self.commit_pager_tx(tx)?;
        // Deleted rows may have held the AUTOINCREMENT watermark.
        self.autoinc_cache.remove(&tname);
        let removed: Vec<Object> = targets.into_iter().map(|(_, d)| d).collect();
        if fire_delete {
            for doc in &removed {
                self.fire_row_triggers(&tname, false, TriggerFireEvent::Delete, Some(doc), None)?;
            }
        }
        let count = removed.len() as u64;
        if let Some(ret) = returning {
            return project_returning(ret, &removed);
        }
        Ok(ExecOutcome::Affected(count))
    }

    fn exec_create_index(&mut self, idx: sqlparser::ast::CreateIndex) -> Result<ExecOutcome> {
        // Clause-level guard: every index option the engine cannot honor must
        // be refused, not dropped (a filtered/INCLUDE index silently becomes
        // a different index).
        if !idx.include.is_empty() {
            return err("CREATE INDEX INCLUDE is not supported");
        }
        // Partial index (CREATE INDEX … WHERE): the predicate is validated
        // against the table below; the tree then holds only the qualifying
        // rows and every read must prove the query implies the predicate
        // before probing (superset safety, see [`query_implies_pred`]).
        let pred_expr = match &idx.predicate {
            Some(e) => {
                let p = validate_partial_pred(e)?;
                Some(p)
            }
            None => None,
        };
        if idx.using.is_some() {
            return err("index types (USING ...) are not supported");
        }
        if idx.nulls_distinct.is_some() {
            return err("NULLS [NOT] DISTINCT is not supported");
        }
        if idx.concurrently {
            return err("CREATE INDEX CONCURRENTLY is not supported");
        }
        if !idx.with.is_empty() || !idx.index_options.is_empty() {
            return err("index storage options are not supported");
        }
        let table = obj_name(&idx.table_name);
        if !self.internal_ddl
            && (crate::useradmin::is_user_table(&table) || crate::engine::is_system_table(&table))
        {
            // An index built on the internal user tables is invisible to
            // dumps (user tables replay through their own statement
            // channel) yet lands in the table's schema hash — an embedded
            // node would silently diverge from its cluster's digest.
            return err(format!("cannot index the internal table {table}"));
        }
        if self.tables.get(&table).is_some_and(|m| m.is_view()) {
            return err(format!(
                "cannot index view {table} (it is a view, not a table)"
            ));
        }
        let iname = idx
            .name
            .as_ref()
            .map(obj_name)
            .unwrap_or_else(|| format!("idx_{}", table));
        // Reserved for the table's own PRIMARY KEY/UNIQUE constraint
        // indexes; a user index with such a name would shadow them.
        if iname.starts_with("sqlite_autoindex_") {
            return err(
                "index names beginning with sqlite_autoindex_ are reserved for \
                 PRIMARY KEY/UNIQUE constraint indexes",
            );
        }
        // System/internal table names are unusable as index names: the
        // server's write gate classifies DROP INDEX by name and would
        // refuse to ever drop it (no other DROP INDEX entry point exists).
        if is_system_table(&iname) || is_internal_table(&iname) {
            return err(format!(
                "index name {iname} is reserved for a system/internal table"
            ));
        }
        // Index names share a database-wide namespace (SQLite semantics):
        // a name taken by any table blocks reuse elsewhere.
        let owner = self.tables.values().find(|m| m.indexes.contains(&iname));
        if owner.is_some() {
            if idx.if_not_exists {
                return Ok(ExecOutcome::Affected(0));
            }
            return err(format!("index {iname} already exists"));
        }
        let Some(mut meta) = self.tables.get(&table).map(|a| a.as_ref().clone()) else {
            return err(format!("table {table} does not exist"));
        };
        // Partial-predicate columns must be declared: a ghost name would
        // read NULL on every row and silently empty the tree.
        if let Some(p) = &pred_expr {
            let mut refs = std::collections::BTreeSet::new();
            pred_columns(p, &mut refs);
            for c in &refs {
                if !meta.columns.contains(c) {
                    return err(format!("column {c} does not exist"));
                }
            }
        }
        // Parse the indexed columns: plain identifiers only (expression
        // indexes stay unsupported), each must exist, duplicates rejected.
        if idx.columns.is_empty() {
            return err("CREATE INDEX requires at least one column");
        }
        let mut cols: Vec<String> = Vec::with_capacity(idx.columns.len());
        // JSON path index (design 005): exactly one column of the form
        // JSON_EXTRACT(col, '$.path'). Tree keys are values extracted from
        // the column's JSON text — the same lookup JSON_EXTRACT evaluates
        // at query time, so probes and inline evaluation cannot diverge.
        let mut path: Option<String> = None;
        for c in &idx.columns {
            // Direction is part of the index definition: accepting DESC here
            // and silently building ASC (as before) creates an index whose
            // ORDER BY optimization direction is the opposite of what was
            // declared. Same refusal discipline as INCLUDE/WHERE/USING.
            if c.column.options.asc == Some(false) {
                return err("descending indexes (CREATE INDEX ... DESC) are not supported");
            }
            if let SqlExpr::Function(f) = &c.column.expr {
                let fname = f.name.to_string().to_uppercase();
                if fname == "JSON_EXTRACT" {
                    if path.is_some() || cols.len() > 1 {
                        return err("a JSON path index is single-column: one JSON_EXTRACT(col, \
                             '$.path') term per index");
                    }
                    let (col, p) = parse_json_index_column(f)?;
                    if !meta.columns.contains(&col) {
                        return err(format!("column {col} does not exist"));
                    }
                    cols.push(col);
                    path = Some(p);
                    continue;
                }
                return err(
                    "expression indexes are not supported (only JSON_EXTRACT(col, '$.path'))",
                );
            }
            let col = match &c.column.expr {
                SqlExpr::Identifier(i) => i.value.clone(),
                other => expr_name(other),
            };
            if !meta.columns.contains(&col) {
                return err(format!("column {col} does not exist"));
            }
            if cols.contains(&col) {
                return err(format!("duplicate column name: {col} (in index {iname})"));
            }
            cols.push(col);
        }
        // UNIQUE path indexes are refused, not degraded: uniqueness
        // enforcement is column-based (meta.unique / constraint_unique and
        // the OR REPLACE displacement machinery do not know paths), and a
        // tree-level-only unique would leave UPDATE displacements unguarded.
        if idx.unique && path.is_some() {
            return err(
                "UNIQUE JSON path indexes are not supported (uniqueness enforcement \
                 is column-based)",
            );
        }
        if path.is_some() && pred_expr.is_some() {
            return err("a partial index cannot combine with a JSON path index");
        }
        let root_key = if path.is_some() {
            // Path trees are keyed by extracted values, never column values:
            // they always key by the index name and never reuse a column
            // tree (a scalar tree on the same column has a different key
            // shape; sharing one would mix value kinds in one tree).
            iname.clone()
        } else if cols.len() > 1 {
            iname.clone()
        } else if pred_expr.is_some() {
            // A partial tree holds a SUBSET of the column's rows — it must
            // never occupy the column-name root slot where ungated readers
            // (conflict probes, constraint lookups) expect a full tree.
            iname.clone()
        } else {
            cols[0].clone()
        };
        // root_key collision guard: `index_roots` is a mixed pool — column-
        // named constraint/single-column trees plus index-name-keyed
        // composite trees — and `BTreeMap::append` silently OVERWRITES an
        // existing key. A collision with a DIFFERENTLY-keyed tree (an index
        // named exactly like the PK column, a single-column index on a
        // column whose root holds a composite tree) would replace a live
        // tree: uniqueness enforcement would probe a tree keyed with a
        // different shape, old and new rows would mix key types in one
        // tree, and the overwritten tree's pages would leak — refuse.
        // The one benign case REUSES the existing tree instead: a second
        // index over exactly the same single column (e.g. UNIQUE INDEX on a
        // constraint column). Both indexes read the same tree; single-
        // column uniqueness is enforced through `meta.unique` (pushed
        // below), never through tree-level rejection. `IF NOT EXISTS` does
        // not apply: it is about the index NAME, and the colliding index
        // is a different one.
        let mut reuse_root: Option<u32> = None;
        if let Some(&root) = meta.index_roots.get(&root_key) {
            if path.is_none() && pred_expr.is_none() && meta.index_columns_of(&root_key) == cols {
                reuse_root = Some(root);
            } else {
                return err(format!(
                    "index root key {root_key} is already in use by another index or \
                     constraint; choose a different index name or column"
                ));
            }
        }
        // Single-column UNIQUE INDEX rides the constraint machinery: the
        // column joins meta.unique, so inserts/updates enforce duplicates
        // from here on. Composite unique indexes cannot use the per-column
        // container — their tree enforces uniqueness itself (unique insert).
        // A PARTIAL unique enforces only among its qualifying rows via the
        // tree; the column must NOT join meta.unique (that would enforce
        // globally, far beyond the predicate's scope).
        let composite = cols.len() > 1;
        if idx.unique && !composite && pred_expr.is_none() && !meta.unique.contains(&cols[0]) {
            let docs = self.table_docs_cx(&table)?;
            meta.unique.push(cols[0].clone());
            meta.check_unique(&docs)?;
        }
        // Build the B+ tree over the existing rows. Single-column trees are
        // non-unique at the tree level (duplicates allowed, lookups collect
        // every match); composite UNIQUE trees enforce duplicates on insert.
        let mut tx = self.pager.begin_tx();
        match reuse_root {
            // Same-semantics reuse: no tree build, the existing root stays
            // authoritative for both indexes.
            Some(root) => {
                meta.index_roots.insert(root_key.clone(), root);
            }
            None => {
                let pairs = self.table_pairs_cx(&table)?;
                let mut roots = self.build_trees(
                    &mut tx,
                    &pairs,
                    std::slice::from_ref(&root_key),
                    std::slice::from_ref(&cols),
                    std::slice::from_ref(&path),
                    std::slice::from_ref(&idx.unique),
                    std::slice::from_ref(&pred_expr),
                )?;
                meta.index_roots.append(&mut roots);
            }
        }
        meta.index_defs.push(IndexDef {
            name: iname.clone(),
            columns: cols,
            unique: idx.unique,
            path: path.clone(),
            pred: pred_expr.as_ref().map(|p| p.to_string()),
        });
        meta.indexes.push(iname);
        let prev_meta = self.tables.get(&table).cloned();
        self.tables.insert(table.clone(), std::sync::Arc::new(meta));
        // The catalog rides the SAME write unit as the tree pages: committing
        // the tree first and saving the catalog in a second transaction left
        // a crash window holding a committed tree no catalog references (the
        // pages would leak for good — nothing would ever free them).
        let persisted = self
            .save_catalog_into(&mut tx)
            .and_then(|()| self.commit_pager_tx(tx));
        if let Err(e) = persisted {
            // The tx dropped without committing: its staged tree pages went
            // back to the reusable pool. The in-memory entry must not keep
            // pointing at them — a later write would land index entries on
            // pages a concurrent allocation may hand to someone else.
            match prev_meta {
                Some(old) => {
                    self.tables.insert(table, old);
                }
                None => {
                    self.tables.remove(&table);
                }
            }
            return Err(e);
        }
        Ok(ExecOutcome::Affected(0))
    }

    /// All-or-nothing validation for ALTER TABLE: every operation is checked
    /// against a simulated meta (columns / PK / CHECKs evolve op by op) so a
    /// failure in a later operation cannot leave earlier ones committed —
    /// each rewrite persists its own write unit, so a mid-statement failure
    /// used to leave the statement half-applied (with no journal entry for
    /// the applied part, diverging replicas until repair). Also fronts the
    /// per-op existence/duplication checks the apply loop historically
    /// skipped (DROP of a missing column silently "succeeded").
    #[allow(clippy::too_many_lines)]
    fn validate_alter_ops(
        &self,
        base: &TableMeta,
        tname: &str,
        ops: &[sqlparser::ast::AlterTableOperation],
    ) -> Result<()> {
        use sqlparser::ast::AlterTableOperation as Op;
        // RENAME TABLE returns from the apply loop early; anything after it
        // in the same statement was silently discarded.
        if ops.iter().any(|o| matches!(o, Op::RenameTable { .. })) && ops.len() > 1 {
            return err(
                "ALTER TABLE ... RENAME TO must be the only operation in the statement".to_string(),
            );
        }
        let mut columns: Vec<String> = base.columns.clone();
        let mut pk: Option<String> = base.primary_key.clone();
        let mut checks: Vec<String> = base.checks.clone();
        // Index namespaces evolve too (DROP COLUMN kills trees over the
        // dropped column); kept as owned copies so the rename-collision
        // check sees the state earlier operations produced.
        let mut roots: std::collections::BTreeSet<String> =
            base.index_roots.keys().cloned().collect();
        let mut index_defs: Vec<IndexDef> = base.index_defs.clone();
        // Column renames applied so far in this statement, so a later DROP
        // COLUMN matches FK references under their post-rename spelling
        // (`RENAME COLUMN a TO b, DROP COLUMN b` must still see the child
        // FKs that the apply loop will rewrite a→b).
        let mut renamed: Vec<(String, String)> = Vec::new();
        for op in ops {
            match op {
                Op::AddColumn { column_def, .. } => {
                    let col = &column_def.name.value;
                    if columns.contains(col) {
                        return err(format!("duplicate column name: {col}"));
                    }
                    let mut add_not_null = false;
                    let mut default_text: Option<String> = None;
                    for opt in &column_def.options {
                        use sqlparser::ast::ColumnOption as CO;
                        match &opt.option {
                            CO::Check(c) => {
                                if expr_calls_wall_clock(&c.expr) {
                                    return err(
                                        "CHECK constraints cannot call wall-clock or random \
                                         functions (NOW/SYSDATE/CURRENT_TIMESTAMP/GETDATE/RAND); \
                                         every node would evaluate a different value",
                                    );
                                }
                                // Column references must resolve (the new
                                // column itself is in scope for its own
                                // CHECK): the apply loop rejects unknown
                                // references MID-STATEMENT, after earlier
                                // operations of the same ALTER already
                                // committed — catch it here instead.
                                let mut refs = Vec::new();
                                check_expr_column_refs(&c.expr, &mut refs)?;
                                for r in refs {
                                    if r != *col && !columns.contains(&r) {
                                        return err(format!(
                                            "CHECK constraint references unknown column {r}"
                                        ));
                                    }
                                }
                                // Mirror the apply loop: later operations in
                                // the same statement must see this CHECK in
                                // the simulated state, or `DROP COLUMN` of
                                // the just-added column passes validation and
                                // half-applies.
                                checks.push(format!("{}", c.expr));
                            }
                            CO::Default(e) => {
                                // The apply loop constant-folds every new
                                // DEFAULT for the backfill; pre-empt the same
                                // failure here so a later operation failing
                                // on it can never leave an earlier one
                                // committed.
                                let text = default_expr_text(e);
                                let expr = parse_expr_text(&text)?;
                                eval_const(&expr)?;
                                default_text = Some(text);
                            }
                            CO::NotNull => add_not_null = true,
                            CO::PrimaryKey { .. }
                            | CO::Unique { .. }
                            | CO::ForeignKey(_)
                            | CO::DialectSpecific(_) => {
                                return err(format!(
                                    "constraint options are not supported on ADD COLUMN {col}"
                                ));
                            }
                            // Same parity as the apply loop (and CREATE
                            // TABLE): swallowing these silently degraded the
                            // column to a plain one.
                            CO::Generated { .. }
                            | CO::Materialized(_)
                            | CO::Ephemeral(_)
                            | CO::Alias(_) => {
                                return err(format!(
                                    "generated/computed columns are not supported \
                                     (ADD COLUMN {col})"
                                ));
                            }
                            CO::Null => {}
                            _ => {
                                return err(format!(
                                    "unsupported column option on ADD COLUMN {col}"
                                ))
                            }
                        }
                    }
                    // A new column's default can only arrive in this very
                    // definition (the apply loop registers it in the same
                    // pass, before the NOT NULL rule is judged).
                    if add_not_null
                        && !column_def
                            .options
                            .iter()
                            .any(|o| matches!(o.option, sqlparser::ast::ColumnOption::Default(_)))
                    {
                        return err(format!(
                            "cannot add NOT NULL column {col} without a DEFAULT"
                        ));
                    }
                    // Backfill-vs-CHECK parity with the apply loop: every
                    // check — including one riding on THIS ADD COLUMN — is
                    // evaluated against the backfilled rows there; without
                    // this simulation a later operation in the same ALTER
                    // commits first and the statement half-applies.
                    if let Some(text) = &default_text {
                        if !checks.is_empty() {
                            let fill = eval_const(&parse_expr_text(text)?)?;
                            for d in self.table_docs_cx(tname)? {
                                let mut filled = d;
                                if !filled.contains_key(col) {
                                    filled.insert(col.clone(), fill.clone());
                                }
                                for c in &checks {
                                    let e = parse_expr_text(c)?;
                                    if matches!(
                                        check_expr_outcome(&e, &filled)?,
                                        CheckOutcome::Fail
                                    ) {
                                        return err(format!("CHECK constraint failed: {c}"));
                                    }
                                }
                            }
                        }
                    }
                    columns.push(col.clone());
                }
                Op::DropColumn { column_names, .. } => {
                    for id in column_names {
                        let name = &id.value;
                        if !columns.contains(name) {
                            return err(format!("column {name} does not exist"));
                        }
                        if pk.as_deref() == Some(name.as_str()) {
                            return err("cannot drop a PRIMARY KEY column");
                        }
                        // A partial-index predicate naming the column would
                        // silently read NULL after the drop — every row would
                        // fall out of the tree at the next rewrite. Refuse
                        // like CHECK constraints do (drop the index first).
                        if let Some(bad) = index_defs.iter().find_map(|d| {
                            d.pred.as_deref().and_then(|p| {
                                parse_expr_text(p)
                                    .map(|e| {
                                        let mut cols = std::collections::BTreeSet::new();
                                        pred_columns(&e, &mut cols);
                                        cols.contains(name).then(|| d.name.clone())
                                    })
                                    .unwrap_or(None)
                            })
                        }) {
                            return err(format!(
                                "cannot drop column {name}: partial index {bad} references it \
                                 in its WHERE predicate"
                            ));
                        }
                        if let Some(bad) = checks.iter().find(|c| {
                            parse_expr_text(c)
                                .map(|e| expr_references_ident(&e, name))
                                .unwrap_or(true)
                        }) {
                            return err(format!(
                                "cannot drop column {name}: CHECK constraint {bad} references it"
                            ));
                        }
                        // Stored view SQL is not rewritten: dropping a column
                        // a view still names breaks the view (its missing
                        // column reads NULL — for a WHERE that is silently
                        // wrong rows) while the dump replays "fine". Refuse
                        // like RENAME does, scoped to views that actually
                        // name the column. The chain is TRANSITIVE: a
                        // `SELECT *` middle layer carries the column upward
                        // without naming it, so direct dependents alone
                        // would let v2 = SELECT v FROM v1 degrade to NULL.
                        let mut dep_chain = vec![tname.to_string()];
                        loop {
                            let deps = self.direct_view_dependents(&dep_chain);
                            if deps.is_empty() {
                                break;
                            }
                            dep_chain.extend(deps);
                        }
                        let view_refs: Vec<String> = dep_chain
                            .into_iter()
                            .filter(|v| {
                                v != tname
                                    && self
                                        .tables
                                        .get(v)
                                        .and_then(|m| m.view_sql.clone())
                                        .is_some_and(|sql| text_references_ident(&sql, name))
                            })
                            .collect();
                        if !view_refs.is_empty() {
                            return err(format!(
                                "cannot drop column {tname}.{name}: referenced by VIEW {} \
                                 (drop the view first; its stored SQL is not rewritten)",
                                view_refs.join(", ")
                            ));
                        }
                        let referencing: Vec<String> = self
                            .tables
                            .iter()
                            .filter(|(_, m)| {
                                m.foreign_keys.iter().any(|(_, rt, rc)| {
                                    if rt != tname {
                                        return false;
                                    }
                                    // Match under the rc spelling the apply
                                    // loop will have produced by the time
                                    // this DROP runs (renames earlier in the
                                    // same statement rewrote it).
                                    let evolved = renamed.iter().fold(rc.clone(), |cur, (o, n)| {
                                        if cur == *o {
                                            n.clone()
                                        } else {
                                            cur
                                        }
                                    });
                                    evolved == *name
                                })
                            })
                            .map(|(t, _)| t.clone())
                            .collect();
                        if !referencing.is_empty() {
                            return err(format!(
                                "cannot drop column {tname}.{name}: referenced by FOREIGN KEY in {}",
                                referencing.join(", ")
                            ));
                        }
                        columns.retain(|c| c != name);
                        // Mirror the apply loop's index teardown on the
                        // simulated state: the dropped column's single-column
                        // tree dies, and every index over the column dies with
                        // it. RenameColumn later in the statement must check
                        // collisions against this EVOLVED state, or
                        // `DROP COLUMN x, RENAME COLUMN y TO x` is wrongly
                        // refused for x's dead index.
                        roots.remove(name);
                        index_defs.retain(|d| !d.columns.iter().any(|c| c == name));
                    }
                }
                Op::RenameColumn {
                    old_column_name,
                    new_column_name,
                } => {
                    let (old, new) = (&old_column_name.value, &new_column_name.value);
                    if !columns.contains(old) {
                        return err(format!("column {old} does not exist"));
                    }
                    // Stored view SQL is not rewritten: renaming a column of
                    // a referenced table breaks the view (and makes its
                    // dump_script unreplayable). Refuse like DROP does.
                    let deps = self.direct_view_dependents(&[tname.to_string()]);
                    if !deps.is_empty() {
                        return err(format!(
                            "cannot rename column {tname}.{old}: table is referenced by VIEW {} \
                             (drop the view first; its stored SQL is not rewritten)",
                            deps.join(", ")
                        ));
                    }
                    if old != new {
                        // Every namespace the new name can collide with: a
                        // duplicate column collapses two columns' data into
                        // one key, and an index root-key (single-column roots
                        // key by column name, composites by index name) would
                        // be silently overwritten by the rename. Both checks
                        // run on the EVOLVED state (earlier DROP COLUMNs may
                        // have removed trees/defs).
                        if columns.contains(new) {
                            return err(format!("duplicate column name: {new}"));
                        }
                        if roots.contains(new) || index_defs.iter().any(|d| d.name == *new) {
                            return err(format!("name {new} is already used by an index"));
                        }
                        // Same collapse through the schemaless door: an
                        // undeclared field already present in the stored
                        // documents. The apply loop's remove(old)+insert(new)
                        // would silently overwrite those values.
                        if self
                            .table_docs_cx(tname)?
                            .iter()
                            .any(|d| d.contains_key(new.as_str()))
                        {
                            return err(format!(
                                "cannot rename column {tname}.{old} to {new}: \
                                 {new} already exists as a data field"
                            ));
                        }
                    }
                    renamed.push((old.clone(), new.clone()));
                    columns = columns
                        .iter()
                        .map(|c| if c == old { new.clone() } else { c.clone() })
                        .collect();
                    if pk.as_deref() == Some(old.as_str()) {
                        pk = Some(new.clone());
                    }
                    // The apply loop rekeys the single-column tree from the
                    // old column name to the new one — evolve the simulated
                    // roots the same way, or a later rename falsely reports
                    // a collision with the vacated name. Same ownership rule
                    // as the apply loop: a composite/path def NAMED like the
                    // column owns the def-name root and blocks the rekey.
                    let own_tree = !index_defs
                        .iter()
                        .any(|d| d.name == *old && (d.path.is_some() || d.columns.len() > 1));
                    if own_tree && roots.remove(old.as_str()) {
                        roots.insert(new.clone());
                    }
                    for d in index_defs.iter_mut() {
                        for c in d.columns.iter_mut() {
                            if c == old {
                                *c = new.clone();
                            }
                        }
                    }
                    checks = checks
                        .iter()
                        .map(|c| rename_ident_in_text(c, old, &ident_spelling(new)))
                        .collect();
                }
                Op::RenameTable { table_name } => {
                    let new_name = match table_name {
                        sqlparser::ast::RenameTableNameKind::As(n)
                        | sqlparser::ast::RenameTableNameKind::To(n) => obj_name(n),
                    };
                    if self.tables.contains_key(&new_name) {
                        return err(format!("table {new_name} already exists"));
                    }
                    // Reserved names are filtered out of dump/snapshot or
                    // collide with internal tables; CREATE refuses them, so
                    // rename must too (a renamed-away table would silently
                    // vanish from every backup).
                    if is_system_table(&new_name) || crate::useradmin::is_user_table(&new_name) {
                        return err(format!("table name {new_name} is reserved for the engine"));
                    }
                    // Same parity as CREATE TABLE: a # temp-table name is
                    // refused outright, and a compat dictionary name would
                    // shadow the table at FROM-resolution — writes would
                    // land but SELECTs would read the dictionary instead,
                    // and dump_script would emit a CREATE the replay guard
                    // rejects (backup/restore and join snapshots fail).
                    if new_name.starts_with('#') {
                        return err("temp tables (#name/##name) are not supported");
                    }
                    if is_compat_view(&new_name) {
                        return err(format!(
                            "table name {new_name} is reserved for the compatibility dictionary views"
                        ));
                    }
                    // Stored view SQL is not rewritten: a view left pointing
                    // at the old name breaks every SELECT and — worse — makes
                    // dump_script unreplayable (backup/restore and join/repair
                    // snapshots all fail). Refuse like DROP does.
                    let deps = self.direct_view_dependents(&[tname.to_string()]);
                    if !deps.is_empty() {
                        return err(format!(
                            "cannot rename table {tname}: referenced by VIEW {} \
                             (drop the view first; its stored SQL is not rewritten)",
                            deps.join(", ")
                        ));
                    }
                }
                other => return err(format!("unsupported ALTER TABLE operation: {other}")),
            }
        }
        Ok(())
    }

    fn exec_alter(&mut self, alter: sqlparser::ast::AlterTable) -> Result<ExecOutcome> {
        use sqlparser::ast::AlterTableOperation as Op;
        let tname = obj_name(&alter.name);
        if !self.internal_ddl
            && (crate::useradmin::is_user_table(&tname) || crate::engine::is_system_table(&tname))
        {
            // The network path already classifies these writes away
            // (stmt_write_targets); an embedded/in-process caller could
            // ALTER the replicated user tables directly, and the altered
            // schema would diverge from every peer's digest.
            return err(format!("cannot ALTER the internal table {tname}"));
        }
        if self.tables.get(&tname).is_some_and(|m| m.is_matview()) && !self.internal_ddl {
            return err(format!(
                "cannot ALTER materialized view {tname} (it is refreshed, not altered)"
            ));
        }
        let Some(mut meta) = self.tables.get(&tname).map(|a| a.as_ref().clone()) else {
            return err(format!("table {tname} does not exist"));
        };
        ensure_not_view("ALTER", &tname, &meta)?;
        self.validate_alter_ops(&meta, &tname, &alter.operations)?;
        // rewrite_table persists the catalog in its own transaction; only
        // metadata-only ALTERs (plain ADD COLUMN) need the tail save.
        // Trees detached from `meta.index_roots` by DROP COLUMN below; freed
        // inside the rebuild's write unit (see rewrite_table_with_dead).
        let mut dead_trees: Vec<(String, u32)> = Vec::new();
        for op in &alter.operations {
            match op {
                Op::AddColumn { column_def, .. } => {
                    let col = column_def.name.value.clone();
                    if !meta.columns.contains(&col) {
                        meta.columns.push(col.clone());
                    }
                    // Column-level DEFAULT / CHECK ride along with ADD COLUMN.
                    // NOT NULL is validated after the loop: the conventional
                    // spelling `NOT NULL DEFAULT x` registers the default in
                    // a later option, so the "NOT NULL needs a DEFAULT" rule
                    // can only be judged once all options are in.
                    let mut add_not_null = false;
                    for opt in &column_def.options {
                        use sqlparser::ast::ColumnOption as CO;
                        match &opt.option {
                            CO::Default(e) => {
                                meta.defaults.push((col.clone(), default_expr_text(e)))
                            }
                            CO::Check(c) => {
                                if expr_calls_wall_clock(&c.expr) {
                                    return err(
                                        "CHECK constraints cannot call wall-clock or random \
                                         functions (NOW/SYSDATE/CURRENT_TIMESTAMP/GETDATE/RAND); \
                                         every node would evaluate a different value",
                                    );
                                }
                                let mut refs = Vec::new();
                                check_expr_column_refs(&c.expr, &mut refs)?;
                                for r in refs {
                                    if !meta.columns.contains(&r) {
                                        return err(format!(
                                            "CHECK constraint references unknown column {r}"
                                        ));
                                    }
                                }
                                meta.checks.push(format!("{}", c.expr))
                            }
                            CO::NotNull => add_not_null = true,
                            // Same parity as CREATE TABLE: these options on
                            // ADD COLUMN would dangle (no tree, no backfill).
                            CO::PrimaryKey { .. }
                            | CO::Unique { .. }
                            | CO::ForeignKey(_)
                            | CO::DialectSpecific(_) => {
                                return err(format!(
                                    "constraint options are not supported on ADD COLUMN {col}"
                                ));
                            }
                            // Swallowing these silently degraded the column
                            // to a plain one (the CREATE TABLE path rejects
                            // the same spellings).
                            CO::Generated { .. }
                            | CO::Materialized(_)
                            | CO::Ephemeral(_)
                            | CO::Alias(_) => {
                                return err(format!(
                                    "generated/computed columns are not supported \
                                     (ADD COLUMN {col})"
                                ));
                            }
                            CO::Null => {}
                            _ => {
                                return err(format!(
                                    "unsupported column option on ADD COLUMN {col}"
                                ))
                            }
                        }
                    }
                    // SQLite rule: a NOT NULL column needs a default to
                    // backfill existing rows, else every later UPDATE of a
                    // pre-existing row would fail.
                    if add_not_null && !meta.defaults.iter().any(|(c, _)| c == &col) {
                        return err(format!(
                            "cannot add NOT NULL column {col} without a DEFAULT"
                        ));
                    }
                    if add_not_null {
                        meta.not_null.push(col.clone());
                    }
                    // ADD COLUMN with DEFAULT backfills existing rows.
                    if let Some((_, text)) = meta.defaults.iter().find(|(c, _)| c == &col).cloned()
                    {
                        let e = parse_expr_text(&text)?;
                        // A wall-clock/random default would backfill THIS
                        // node's clock/roll into every existing row with no
                        // resolved-text rewrite — replaying peers stamp
                        // their own values and the tables diverge. Refuse
                        // while there are rows to backfill (an empty table
                        // keeps the default text for future INSERTs, which
                        // do rewrite deterministically).
                        if expr_calls_wall_clock(&e) && !self.table_docs_cx(&tname)?.is_empty() {
                            return err(
                                "ADD COLUMN cannot backfill a wall-clock or random DEFAULT \
                                 (NOW/SYSDATE/CURRENT_TIMESTAMP/GETDATE/RAND) into existing \
                                 rows; supply the column explicitly on INSERT instead",
                            );
                        }
                        let fill = eval_const(&e)?;
                        let docs = self.table_docs_cx(&tname)?;
                        let filled: Vec<Object> = docs
                            .into_iter()
                            .map(|mut d| {
                                if !d.contains_key(&col) {
                                    d.insert(col.clone(), fill.clone());
                                }
                                d
                            })
                            .collect();
                        // The backfilled image must satisfy the checks —
                        // including one riding along on THIS ADD COLUMN:
                        // `ADD COLUMN c INT DEFAULT -5 CHECK (c > 0)` used
                        // to backfill violating values while later INSERTs
                        // were correctly rejected.
                        for d in &filled {
                            meta.check(d)?;
                        }
                        self.rewrite_table(&tname, &mut meta, filled)?;
                    }
                }
                Op::DropColumn { column_names, .. } => {
                    for id in column_names {
                        let name = id.value.clone();
                        if meta.primary_key.as_deref() == Some(name.as_str()) {
                            return err("cannot drop a PRIMARY KEY column");
                        }
                        // A CHECK text still naming the column would read it
                        // as NULL afterwards and silently stop enforcing
                        // (the rename path already guards against this).
                        if let Some(bad) = meta.checks.iter().find(|c| {
                            parse_expr_text(c)
                                .map(|e| expr_references_ident(&e, &name))
                                .unwrap_or(true)
                        }) {
                            return err(format!(
                                "cannot drop column {name}: CHECK constraint {bad} references it"
                            ));
                        }
                        // Another table's (or this table's own) FK pointing
                        // at the column would leave every future child insert
                        // failing with a dangling reference.
                        let referencing: Vec<String> = self
                            .tables
                            .iter()
                            .filter(|(_, m)| {
                                m.foreign_keys
                                    .iter()
                                    .any(|(_, rt, rc)| rt == &tname && rc == &name)
                            })
                            .map(|(t, _)| t.clone())
                            .collect();
                        if !referencing.is_empty() {
                            return err(format!(
                                "cannot drop column {tname}.{name}: referenced by FOREIGN KEY in {}",
                                referencing.join(", ")
                            ));
                        }
                        meta.columns.retain(|c| c != &name);
                        meta.unique.retain(|c| c != &name);
                        meta.constraint_unique.retain(|c| c != &name);
                        meta.not_null.retain(|c| c != &name);
                        meta.defaults.retain(|(c, _)| c != &name);
                        meta.foreign_keys.retain(|(c, _, _)| c != &name);
                        if let Some(root) = meta.index_roots.remove(&name) {
                            // Capture the tree BEFORE dropping it from the
                            // map: the rebuild below only frees trees still
                            // listed in `index_roots`, so these roots must
                            // ride `dead_trees` or their pages leak forever.
                            dead_trees.push((name.clone(), root));
                        }
                        if meta.autoinc.as_deref() == Some(name.as_str()) {
                            meta.autoinc = None;
                        }
                        if meta.autoguid.as_deref() == Some(name.as_str()) {
                            meta.autoguid = None;
                        }
                        // Indexes over the dropped column lose their
                        // definitions (trees above were already removed).
                        // A composite index over the column dies with it too.
                        let dead: Vec<String> = meta
                            .index_defs
                            .iter()
                            .filter(|d| d.columns.iter().any(|c| c == name.as_str()))
                            .map(|d| d.name.clone())
                            .collect();
                        for n in &dead {
                            meta.indexes.retain(|i| i != n);
                        }
                        for (k, root) in meta.index_roots.iter() {
                            if dead.contains(k) {
                                dead_trees.push((k.clone(), *root));
                            }
                        }
                        meta.index_roots.retain(|k, _| !dead.contains(k));
                        meta.index_defs.retain(|d| !dead.contains(&d.name));
                    }
                    let docs = self.table_docs_cx(&tname)?;
                    let stripped: Vec<Object> = docs
                        .into_iter()
                        .map(|mut d| {
                            for id in column_names {
                                d.remove(&id.value);
                            }
                            d
                        })
                        .collect();
                    self.rewrite_table_with_dead(
                        &tname,
                        &mut meta,
                        stripped,
                        std::mem::take(&mut dead_trees),
                    )?;
                }
                Op::RenameColumn {
                    old_column_name,
                    new_column_name,
                } => {
                    let (old, new) = (&old_column_name.value, &new_column_name.value);
                    if !meta.columns.contains(old) {
                        return err(format!("column {old} does not exist"));
                    }
                    meta.columns = meta
                        .columns
                        .iter()
                        .map(|c| if c == old { new.clone() } else { c.clone() })
                        .collect();
                    if meta.primary_key.as_deref() == Some(old.as_str()) {
                        meta.primary_key = Some(new.clone());
                    }
                    meta.unique = meta
                        .unique
                        .iter()
                        .map(|c| if c == old { new.clone() } else { c.clone() })
                        .collect();
                    meta.not_null = meta
                        .not_null
                        .iter()
                        .map(|c| if c == old { new.clone() } else { c.clone() })
                        .collect();
                    if meta.autoinc.as_deref() == Some(old.as_str()) {
                        meta.autoinc = Some(new.clone());
                    }
                    if meta.autoguid.as_deref() == Some(old.as_str()) {
                        meta.autoguid = Some(new.clone());
                    }
                    // Only the column's OWN single-column tree rekeys with
                    // the rename. A composite or JSON-path index NAMED like
                    // the column owns the def-name root (root_key = index
                    // name) — the def name does not follow the column, and
                    // rekeying would graft an Array/path-keyed tree onto
                    // the new column key where DML would mix scalar keys
                    // into it. A SINGLE-column def named like the column
                    // but built on ANOTHER column owns that other column's
                    // root, not `old` — the column's own tree still rekeys
                    // (stranding it let duplicate PKs in silently and
                    // diverged replicas whose dump replay enforced them).
                    let own_tree = !meta
                        .index_defs
                        .iter()
                        .any(|d| d.name == *old && (d.path.is_some() || d.columns.len() > 1));
                    if own_tree {
                        if let Some(root) = meta.index_roots.remove(old) {
                            meta.index_roots.insert(new.clone(), root);
                        }
                    }
                    meta.defaults = meta
                        .defaults
                        .iter()
                        .map(|(c, t)| {
                            if c == old {
                                (new.clone(), t.clone())
                            } else {
                                (c.clone(), t.clone())
                            }
                        })
                        .collect();
                    meta.constraint_unique = meta
                        .constraint_unique
                        .iter()
                        .map(|c| if c == old { new.clone() } else { c.clone() })
                        .collect();
                    meta.index_defs = meta
                        .index_defs
                        .iter()
                        .map(|d| {
                            if d.columns.iter().any(|c| c == old) {
                                IndexDef {
                                    name: d.name.clone(),
                                    columns: d
                                        .columns
                                        .iter()
                                        .map(|c| if c == old { new.clone() } else { c.clone() })
                                        .collect(),
                                    unique: d.unique,
                                    path: d.path.clone(),
                                    pred: d.pred.clone(),
                                }
                            } else if d.pred.is_some() {
                                // The predicate is SQL text over row fields:
                                // a renamed column must not leave it naming a
                                // ghost (its rows would silently read NULL
                                // and drop out of the tree). Same rewrite the
                                // CHECK texts get.
                                IndexDef {
                                    name: d.name.clone(),
                                    columns: d.columns.clone(),
                                    unique: d.unique,
                                    path: d.path.clone(),
                                    pred: d.pred.as_deref().map(|p| {
                                        rename_ident_in_text(p, old, &ident_spelling(new))
                                    }),
                                }
                            } else {
                                d.clone()
                            }
                        })
                        .collect();
                    // FK columns on this table follow the rename, and so does
                    // the remote column of a same-table FK (self-references
                    // and sibling references): a dangling rc would fail every
                    // subsequent child write with FK errors.
                    meta.foreign_keys = meta
                        .foreign_keys
                        .iter()
                        .map(|(c, rt, rc)| {
                            (
                                if c == old { new.clone() } else { c.clone() },
                                rt.clone(),
                                if rt == &tname && rc == old {
                                    new.clone()
                                } else {
                                    rc.clone()
                                },
                            )
                        })
                        .collect();
                    // CHECK texts reference the old column name; leaving them
                    // stale silently disables the constraint (a missing column
                    // reads as NULL -> unknown -> check skipped).
                    meta.checks = meta
                        .checks
                        .iter()
                        .map(|c| rename_ident_in_text(c, old, &ident_spelling(new)))
                        .collect();
                    let docs = self.table_docs_cx(&tname)?;
                    let renamed: Vec<Object> = docs
                        .into_iter()
                        .map(|mut d| {
                            if let Some(v) = d.remove(old) {
                                d.insert(new.clone(), v);
                            }
                            d
                        })
                        .collect();
                    self.rewrite_table(&tname, &mut meta, renamed)?;
                    // Referencing tables' remote columns follow too (the same
                    // rule RENAME TABLE applies to their rt, also after the
                    // commit so the failure path stays atomic): a dangling rc
                    // makes every child write fail FK checks and aborts dump
                    // replay mid-script (backup/restore, join snapshots).
                    // Only tables actually referencing the old name get a
                    // targeted make_mut — never a whole-catalog deep copy.
                    let referrers: Vec<String> = self
                        .tables
                        .iter()
                        .filter(|(n, m)| {
                            n.as_str() != tname.as_str()
                                && m.foreign_keys
                                    .iter()
                                    .any(|(_, rt, rc)| rt == &tname && rc == old)
                        })
                        .map(|(n, _)| n.clone())
                        .collect();
                    if !referrers.is_empty() {
                        // Snapshot the referrer entries: a failed catalog
                        // save must leave memory matching disk — the next
                        // successful write persists whatever sits in memory,
                        // so a diverged (rewritten-but-unsaved) entry would
                        // silently drift to peers.
                        let before: Vec<(String, std::sync::Arc<TableMeta>)> = referrers
                            .iter()
                            .filter_map(|n| self.tables.get(n).map(|m| (n.clone(), m.clone())))
                            .collect();
                        for name in referrers {
                            if let Some(entry) = self.tables.get_mut(&name) {
                                let m = std::sync::Arc::make_mut(entry);
                                for (_, rt, rc) in m.foreign_keys.iter_mut() {
                                    if rt == &tname && rc == old {
                                        *rc = new.clone();
                                    }
                                }
                            }
                        }
                        if let Err(e) = self.save_catalog() {
                            for (n, m) in before {
                                self.tables.insert(n, m);
                            }
                            return Err(e);
                        }
                    }
                }
                Op::RenameTable { table_name } => {
                    let new_name = match table_name {
                        sqlparser::ast::RenameTableNameKind::As(n)
                        | sqlparser::ast::RenameTableNameKind::To(n) => obj_name(n),
                    };
                    // rewrite_table would silently replace the target's entry
                    // and orphan its data pages.
                    if self.tables.contains_key(&new_name) {
                        return err(format!("table {new_name} already exists"));
                    }
                    let docs = self.table_docs_cx(&tname)?;
                    let old_entry = self.tables.get(&tname).cloned();
                    // The mirrored catalog key follows the rename; constraint
                    // errors keep naming the table users actually see.
                    meta.name = new_name.clone();
                    // 自引用外键的 rt 跟随新名(与其它引用方一并改写)。
                    for (_, rt, _) in meta.foreign_keys.iter_mut() {
                        if *rt == tname {
                            *rt = new_name.clone();
                        }
                    }
                    self.tables.remove(&tname);
                    // The old name must not keep a stale AUTOINCREMENT
                    // watermark for a table created under it later.
                    self.autoinc_cache.remove(&tname);
                    // Grants follow the rename: a row left on the old name
                    // would (a) resurrect onto a future same-named table —
                    // silent privilege escalation across unrelated data —
                    // and (b) break dump replay (GRANT on a table that no
                    // longer exists aborts the whole restore). A failure of
                    // the UPDATE fails the whole rename BEFORE the rewrite
                    // (the catalog entry is restored below like every other
                    // failure path); a rewrite failure reverts the grants
                    // rows the same way.
                    let lit_old = crate::stmt::sql_string_literal(&tname);
                    let lit_new = crate::stmt::sql_string_literal(&new_name);
                    let grants_update = if self.tables.contains_key(crate::useradmin::GRANTS_TABLE)
                    {
                        self.execute(&format!(
                            "UPDATE {} SET tbl = {} WHERE tbl = {}",
                            crate::useradmin::GRANTS_TABLE,
                            lit_new,
                            lit_old
                        ))
                    } else {
                        Ok(ExecOutcome::Affected(0))
                    };
                    if let Err(e) = grants_update {
                        if let Some(old) = old_entry {
                            self.tables.insert(tname.clone(), old);
                        }
                        return Err(e);
                    }
                    if let Err(e) = self.rewrite_table(&new_name, &mut meta, docs) {
                        // rewrite_table's own failure restore only covers the
                        // NEW name's entry (absent here) — without this the
                        // old entry would stay removed and the table (and its
                        // data pages) would vanish from the catalog.
                        if let Some(old) = old_entry {
                            self.tables.insert(tname.clone(), old);
                        }
                        // The grants UPDATE above already landed (autocommit):
                        // point the rows back at the old name or the next
                        // same-named table would inherit them.
                        if let Err(e2) = self.execute(&format!(
                            "UPDATE {} SET tbl = {} WHERE tbl = {}",
                            crate::useradmin::GRANTS_TABLE,
                            lit_old,
                            lit_new
                        )) {
                            eprintln!(
                                "warning: failed to revert grant rows after a failed \
                                 RENAME ({e2}); re-run the RENAME or fix grants manually"
                            );
                        }
                        return Err(e);
                    }
                    // 引用方的外键表名跟随改写:悬挂 rt 会让子表的一切写
                    // 路径报 "table does not exist",dump/restore 还会把
                    // 坏状态原样带回(旧条目尚未动,失败路径保持原子)。
                    // 只对真正引用旧名的表定点 make_mut——全库逐表 make_mut
                    // 会在有 detached 读者时深拷每一张表的 meta。
                    let referrers: Vec<String> = self
                        .tables
                        .iter()
                        .filter(|(_, m)| m.foreign_keys.iter().any(|(_, rt, _)| rt == &tname))
                        .map(|(n, _)| n.clone())
                        .collect();
                    // Snapshot first: a failed catalog save must restore the
                    // referrer entries, or memory diverges from disk (the
                    // next successful write would persist the divergence).
                    let before: Vec<(String, std::sync::Arc<TableMeta>)> = referrers
                        .iter()
                        .filter_map(|n| self.tables.get(n).map(|m| (n.clone(), m.clone())))
                        .collect();
                    for name in referrers {
                        if let Some(m) = self.tables.get_mut(&name) {
                            let m = std::sync::Arc::make_mut(m);
                            for (_, rt, _) in m.foreign_keys.iter_mut() {
                                if *rt == tname {
                                    *rt = new_name.clone();
                                }
                            }
                        }
                    }
                    if let Err(e) = self.save_catalog() {
                        for (n, m) in before {
                            self.tables.insert(n, m);
                        }
                        return Err(e);
                    }
                    return Ok(ExecOutcome::Affected(0));
                }
                other => return err(format!("unsupported ALTER TABLE operation: {other}")),
            }
        }
        // A rewrite persists the meta CLONE it saw; any metadata-only
        // operations after it in the same statement (ADD COLUMN without a
        // DEFAULT, a CHECK) only touched the local `meta` and were silently
        // dropped. Save the final state unconditionally — the extra save
        // after a bare rewrite is idempotent. A failed save restores the
        // in-memory entry: the next successful write persists whatever is
        // in memory, so leaving the new meta would diverge from disk.
        let prev = self.tables.get(&tname).cloned();
        self.tables.insert(tname.clone(), std::sync::Arc::new(meta));
        if let Err(e) = self.save_catalog() {
            match prev {
                Some(old) => {
                    self.tables.insert(tname.clone(), old);
                }
                None => {
                    self.tables.remove(&tname);
                }
            }
            return Err(e);
        }
        Ok(ExecOutcome::Affected(0))
    }

    /// Validate FOREIGN KEY constraints of `meta` for one candidate doc:
    /// non-null values must exist in the referenced table's column.
    /// Child-side FK check. `same_batch` carries the statement's own new
    /// images for `table` — when an FK targets the SAME table (self- or
    /// intra-statement references: `UPDATE emp SET mgr = 1 WHERE id = 1`,
    /// multi-row inserts whose later rows reference earlier ones), those
    /// images are legitimate parent candidates. Without them such rows were
    /// rejected (or, via UPDATE, created rows that dump/restore could never
    /// replay) even though SQLite/PG immediate FKs accept them.
    ///
    /// `self_final` is the statement's COMPLETE final state of `table` (the
    /// pre-statement rows minus replaced old images plus the new images).
    /// When provided, self-referential lookups use it ALONE: `same_batch`
    /// plus the pre-statement heap would still satisfy values this very
    /// statement removes (`UPDATE t SET id = 3, p = 1 WHERE id = 1`).
    #[allow(clippy::too_many_arguments)]
    fn check_fks_in(
        &mut self,
        table: &str,
        meta: &TableMeta,
        doc: &Object,
        same_batch: &[Object],
        self_final: Option<&[Object]>,
    ) -> Result<()> {
        for (col, rtable, rcol) in &meta.foreign_keys {
            let Some(v) = doc.get(col) else {
                continue;
            };
            if matches!(v, Value::Null) {
                continue; // NULL passes (MATCH SIMPLE semantics)
            }
            // cmp_values (not PartialEq): the engine's comparison semantics
            // treat Int(1)/Float(1.0)/Decimal(1) as equal; a join would
            // match them, so the FK must not reject what the rest of the
            // engine considers the same key.
            let matches_parent = |rd: &Object| {
                rd.get(rcol)
                    .map(|rv| Value::cmp_values(rv, v) == Ordering::Equal)
                    .unwrap_or(false)
            };
            if rtable == table {
                if let Some(final_state) = self_final {
                    if !final_state.iter().any(matches_parent) {
                        return err(format!(
                            "FOREIGN KEY constraint failed: {table}.{col} references \
                             {rtable}.{rcol}"
                        ));
                    }
                    continue;
                }
                if same_batch.iter().any(matches_parent) {
                    continue;
                }
            }
            let ref_docs = self.table_docs_cx(rtable)?;
            if !ref_docs.iter().any(matches_parent) {
                return err(format!(
                    "FOREIGN KEY constraint failed: {table}.{col} references {rtable}.{rcol}"
                ));
            }
        }
        Ok(())
    }

    /// Referential integrity on the *parent* side (RESTRICT): rows removed
    /// from `table` (DELETE, or UPDATE rows whose key value changes) may be
    /// referenced by child tables. `replacement_docs` carries the statement's
    /// surviving/updated rows of `table`: a parent-key value kept by any of
    /// them stays referenceable.
    /// [`check_fk_parent_delete_skipping`] with a child-table exclusion list: children
    /// named in `skip` are also being emptied by the same statement (e.g.
    /// `TRUNCATE parent, child`), so their rows cannot end up orphaned.
    /// `self_children` overrides the scan of `table`'s own rows with the
    /// statement's FINAL images (MERGE: the heap still shows pre-statement
    /// rows, so a self-reference created by the INSERT arm would be missed
    /// and one removed by the UPDATE arm wrongly reported).
    #[allow(clippy::too_many_arguments)]
    fn check_fk_parent_delete_skipping(
        &mut self,
        table: &str,
        removed_docs: &[Object],
        replacement_docs: &[Object],
        skip: &[String],
        self_children: Option<&[Object]>,
    ) -> Result<()> {
        if removed_docs.is_empty() {
            return Ok(());
        }
        // Child FKs referencing `table`: (child_table, child_col, parent_col).
        let children: Vec<(String, String, String)> = self
            .tables
            .iter()
            .filter(|(child_name, _)| !skip.iter().any(|s| s.as_str() == child_name.as_str()))
            .flat_map(|(child_name, m)| {
                m.foreign_keys
                    .iter()
                    .filter(|(_, rt, _)| rt == table)
                    .map(|(c, _, rc)| (child_name.clone(), c.clone(), rc.clone()))
                    .collect::<Vec<_>>()
            })
            .collect();
        if children.is_empty() {
            return Ok(());
        }
        let parent_cols: Vec<&String> = {
            let mut v: Vec<&String> = children.iter().map(|(_, _, rc)| rc).collect();
            v.sort();
            v.dedup();
            v
        };
        // Parent-key values that disappear with this statement.
        let mut lost: Vec<&Value> = Vec::new();
        for doc in removed_docs {
            for rc in &parent_cols {
                if let Some(v) = doc.get(rc.as_str()) {
                    let kept = replacement_docs.iter().any(|d| {
                        d.get(rc.as_str())
                            .map(|rv| Value::cmp_values(rv, v) == Ordering::Equal)
                            .unwrap_or(false)
                    });
                    if !matches!(v, Value::Null) && !kept && !lost.contains(&v) {
                        lost.push(v);
                    }
                }
            }
        }
        if lost.is_empty() {
            return Ok(());
        }
        // Non-unique referenced columns: a value can survive in rows this
        // statement never touched. `replacement_docs` only carries the
        // statement's own rows, so screen `lost` against the whole
        // pre-statement table: a value the removed set does not exhaust
        // (current count > removed count) is still referenceable. For
        // PK/UNIQUE referenced columns the counts are equal and this is a
        // no-op; for anything else it un-rejects legitimate deletes.
        {
            let current = self.table_docs_cx(table)?;
            let count_in = |docs: &[Object], rc: &str, v: &Value| {
                docs.iter()
                    .filter(|d| {
                        d.get(rc)
                            .map(|rv| Value::cmp_values(rv, v) == Ordering::Equal)
                            .unwrap_or(false)
                    })
                    .count()
            };
            lost.retain(|v| {
                !parent_cols
                    .iter()
                    .any(|rc| count_in(&current, rc, v) > count_in(removed_docs, rc, v))
            });
        }
        if lost.is_empty() {
            return Ok(());
        }
        for (child_table, child_col, rc) in &children {
            let fallback;
            let child_docs: &[Object] = if child_table == table {
                match self_children {
                    Some(docs) => docs,
                    None => {
                        fallback = self.table_docs_cx(child_table)?;
                        &fallback
                    }
                }
            } else {
                fallback = self.table_docs_cx(child_table)?;
                &fallback
            };
            let offender = child_docs.iter().any(|cd| {
                cd.get(child_col)
                    .map(|cv| {
                        !matches!(cv, Value::Null)
                            && lost
                                .iter()
                                .any(|lv| Value::cmp_values(cv, lv) == Ordering::Equal)
                    })
                    .unwrap_or(false)
            });
            if offender {
                return err(format!(
                    "FOREIGN KEY constraint failed: {child_table}.{child_col} references \
                     {table}.{rc}; delete/update the child rows first"
                ));
            }
        }
        Ok(())
    }

    /// CREATE VIEW: a named SELECT stored in the catalog namespace. Views
    /// have no storage, no indexes and no declared columns — a SELECT naming
    /// one expands to the stored query's rows (see `load_table_factor`).
    /// The body is dry-run once at create time so missing tables/columns
    /// fail loudly, and `CREATE OR REPLACE` re-points an existing view.
    // ===== Row triggers (CREATE TRIGGER / DROP TRIGGER) =====
    //
    // Replication red line: the creating DDL rides the journal like every
    // other write and every node fires the same bodies over the same rows,
    // so a body may contain NOTHING nondeterministic — no NEWID()/RAND(),
    // no wall-clock calls, no INSERT omitting an auto-GUID column, no
    // INSERT leaving a wall-clock DEFAULT to be filled. Each is refused at
    // CREATE time: at fire time the divergence has already happened by the
    // time anyone could notice.
    fn exec_create_trigger(&mut self, ct: sqlparser::ast::CreateTrigger) -> Result<ExecOutcome> {
        if ct.or_alter {
            return err("CREATE OR ALTER TRIGGER is not supported (use CREATE OR REPLACE TRIGGER)");
        }
        if ct.temporary {
            return err("TEMP triggers are not supported");
        }
        if ct.is_constraint {
            return err("CONSTRAINT triggers are not supported");
        }
        if !ct.referencing.is_empty() {
            return err("REFERENCING (transition tables) is not supported");
        }
        if ct.characteristics.is_some() {
            return err("trigger characteristics (DEFERRABLE / INITIALLY …) are not supported");
        }
        if ct.exec_body.is_some() {
            return err("EXECUTE FUNCTION triggers are not supported (use a BEGIN … END body)");
        }
        if ct.statements_as {
            return err("AS trigger bodies are not supported (use BEGIN … END)");
        }
        let before = match ct.period {
            Some(sqlparser::ast::TriggerPeriod::Before) => true,
            Some(sqlparser::ast::TriggerPeriod::After) => false,
            Some(sqlparser::ast::TriggerPeriod::InsteadOf) => {
                return err("INSTEAD OF triggers are not supported (views are not writable)");
            }
            Some(sqlparser::ast::TriggerPeriod::For) | None => {
                return err("trigger requires BEFORE or AFTER");
            }
        };
        match ct.trigger_object {
            None => {}
            Some(sqlparser::ast::TriggerObjectKind::ForEach(
                sqlparser::ast::TriggerObject::Row,
            ))
            | Some(sqlparser::ast::TriggerObjectKind::For(sqlparser::ast::TriggerObject::Row)) => {}
            Some(_) => {
                return err(
                    "statement-level triggers (FOR EACH STATEMENT) are not supported (triggers are row-level)",
                );
            }
        }
        if ct.events.len() != 1 {
            return err("exactly one event (INSERT / UPDATE / DELETE) per trigger");
        }
        let (insert, delete, update, update_of) = match ct
            .events
            .into_iter()
            .next()
            .expect("checked above")
        {
            sqlparser::ast::TriggerEvent::Insert => (true, false, false, None),
            sqlparser::ast::TriggerEvent::Delete => (false, true, false, None),
            sqlparser::ast::TriggerEvent::Update(cols) => (
                false,
                false,
                true,
                if cols.is_empty() {
                    None
                } else {
                    Some(cols.iter().map(|i| i.value.clone()).collect())
                },
            ),
            sqlparser::ast::TriggerEvent::Truncate => {
                return err(
                    "TRUNCATE triggers are not supported (row triggers fire on DELETE; TRUNCATE itself fires none)",
                );
            }
        };
        let name = obj_name(&ct.name);
        let table = obj_name(&ct.table_name);
        if crate::engine::is_system_table(&table) || is_internal_table(&table) {
            return err(format!("triggers cannot target the internal table {table}"));
        }
        let Some(tmeta) = self.tables.get(&table).cloned() else {
            return err(format!("table {table} does not exist"));
        };
        if tmeta.is_view() {
            return err(format!("cannot create a trigger on view {table}"));
        }
        // WHEN: only NEW./OLD. references over literals and operators — an
        // unqualified column name would silently read NULL and skip every
        // firing.
        let when = match &ct.condition {
            Some(e) => {
                validate_trigger_when(e)?;
                Some(e.to_string())
            }
            None => None,
        };
        let body_statements = match &ct.statements {
            Some(cs) => cs.statements().clone(),
            None => return err("trigger body is required (BEGIN … END)"),
        };
        if body_statements.is_empty() {
            return err("trigger body is empty");
        }
        let mut body = Vec::with_capacity(body_statements.len());
        for s in &body_statements {
            body.push(self.validate_trigger_body_stmt(s)?);
        }
        if self.triggers.contains_key(&name) && !ct.or_replace {
            return err(format!("trigger {name} already exists"));
        }
        self.triggers.insert(
            name.clone(),
            TriggerDef {
                name,
                table,
                before,
                insert,
                delete,
                update,
                update_of,
                when,
                body,
            },
        );
        self.save_catalog()?;
        Ok(ExecOutcome::Affected(0))
    }

    /// CREATE-time validation of one trigger body statement: the accepted
    /// shapes (INSERT / UPDATE / DELETE / `SELECT RAISE(ABORT|FAIL, …)`)
    /// plus the determinism gates that keep every replaying node in
    /// agreement. Returns the catalog form.
    fn validate_trigger_body_stmt(&self, stmt: &Statement) -> Result<TriggerAction> {
        match stmt {
            Statement::Insert(i) => {
                let sqlparser::ast::TableObject::TableName(n) = &i.table else {
                    return err("trigger body INSERT must target a simple table");
                };
                let t = obj_name(n);
                self.validate_trigger_target(&t)?;
                let tmeta = self.tables.get(&t).cloned().unwrap_or_default();
                // INSERT would otherwise rewrite NEWID per row, and RAND()
                // would re-roll on every replaying node — same gate the
                // direct path applies to INSERT ... SELECT sources.
                let text = stmt.to_string();
                if crate::stmt::mentions_nondet_call(&text) {
                    return err(
                        "NEWID()/NEWSEQUENTIALID()/RAND() are not allowed in trigger bodies \
                         (each node would generate different values)",
                    );
                }
                if crate::stmt::mentions_wall_clock(&text) {
                    return err(
                        "wall-clock functions are not allowed in trigger bodies (each node would stamp its own clock)",
                    );
                }
                // A wall-clock DEFAULT on the TARGET table would be filled
                // per node at fire time even though the body text is clean:
                // the body must supply those columns explicitly.
                let cols: Vec<String> = i.columns.iter().map(obj_name).collect();
                let positional = cols.is_empty();
                for (col, text) in &tmeta.defaults {
                    let Ok(e) = parse_expr_text(text) else {
                        continue;
                    };
                    if expr_calls_wall_clock(&e) && (positional || !cols.iter().any(|c| c == col)) {
                        return err(format!(
                            "trigger body INSERT into {t} omits column {col}, whose DEFAULT is a \
                             wall-clock call (each node would stamp its own clock); supply the \
                             column explicitly"
                        ));
                    }
                }
                // An auto-GUID fill is a random id per node. The body must
                // carry an explicit non-NULL value for that column.
                if let Some(gcol) = &tmeta.autoguid {
                    // Positional INSERT maps values onto the DECLARED column
                    // list; an explicit list must name the column itself.
                    let idx: Option<usize> = if positional {
                        tmeta.columns.iter().position(|c| c == gcol)
                    } else {
                        cols.iter().position(|c| c == gcol)
                    };
                    let values: Option<&Vec<SqlExpr>> =
                        i.source.as_ref().and_then(|q| match &*q.body {
                            SetExpr::Values(v) => v.rows.first().map(|r| &r.content),
                            _ => None,
                        });
                    let supplied: Option<&SqlExpr> = match (idx, values) {
                        (Some(i), Some(vals)) => vals.get(i),
                        _ => None,
                    };
                    let explicit = match supplied {
                        // NULL literal still triggers the fill at fire time.
                        Some(SqlExpr::Value(v)) => !matches!(v.value, sqlparser::ast::Value::Null),
                        // NEW.col / OLD.col references and expressions cannot
                        // be proven non-NULL here; the runtime NULL-fill is a
                        // documented edge (substitute a literal to be safe).
                        Some(_) => true,
                        None => false,
                    };
                    if !explicit {
                        return err(format!(
                            "trigger body INSERT into {t} must supply the auto-generated GUID \
                             column {gcol} explicitly (each node would generate a different id)"
                        ));
                    }
                }
            }
            Statement::Update(u) => {
                let sqlparser::ast::TableFactor::Table { name, .. } = &u.table.relation else {
                    return err("trigger body UPDATE must target a simple table");
                };
                let t = obj_name(name);
                self.validate_trigger_target(&t)?;
                if crate::stmt::mentions_nondet_call(&stmt.to_string()) {
                    return err(
                        "NEWID()/NEWSEQUENTIALID()/RAND() are not allowed in trigger bodies \
                         (each node would generate different values)",
                    );
                }
                if crate::stmt::mentions_wall_clock(&stmt.to_string()) {
                    return err(
                        "wall-clock functions are not allowed in trigger bodies (each node would stamp its own clock)",
                    );
                }
            }
            Statement::Delete(d) => {
                let sqlparser::ast::FromTable::WithFromKeyword(twjs) = &d.from else {
                    return err("trigger body DELETE must target a simple table");
                };
                let Some(sqlparser::ast::TableFactor::Table { name, .. }) =
                    twjs.first().map(|t| &t.relation)
                else {
                    return err("trigger body DELETE must target a simple table");
                };
                let t = obj_name(name);
                self.validate_trigger_target(&t)?;
                if crate::stmt::mentions_nondet_call(&stmt.to_string()) {
                    return err(
                        "NEWID()/NEWSEQUENTIALID()/RAND() are not allowed in trigger bodies \
                         (each node would generate different values)",
                    );
                }
                if crate::stmt::mentions_wall_clock(&stmt.to_string()) {
                    return err(
                        "wall-clock functions are not allowed in trigger bodies (each node would stamp its own clock)",
                    );
                }
            }
            Statement::Query(q) => return raise_action(q),
            other => {
                return err(format!(
                    "unsupported trigger body statement ({other}); INSERT / UPDATE / DELETE / \
                     SELECT RAISE(ABORT|FAIL, …) only"
                ));
            }
        }
        Ok(TriggerAction::Sql(stmt.to_string()))
    }

    fn validate_trigger_target(&self, t: &str) -> Result<()> {
        if crate::engine::is_system_table(t) || is_internal_table(t) {
            return err(format!(
                "trigger bodies cannot write the internal table {t}"
            ));
        }
        let Some(m) = self.tables.get(t) else {
            return err(format!("table {t} does not exist"));
        };
        if m.is_view() {
            return err(format!("trigger bodies cannot write view {t}"));
        }
        Ok(())
    }

    fn exec_drop_trigger(&mut self, names: &[ObjectName], if_exists: bool) -> Result<ExecOutcome> {
        for n in names {
            let t = obj_name(n);
            if !self.triggers.contains_key(&t) && !if_exists {
                return err(format!("trigger {t} does not exist"));
            }
        }
        for n in names {
            self.triggers.remove(&obj_name(n));
        }
        self.save_catalog()?;
        Ok(ExecOutcome::Affected(0))
    }

    /// True when any trigger targets `table` — the zero-cost gate for the
    /// statement guard on deployments without triggers.
    fn triggers_exist_for(&self, table: &str) -> bool {
        self.triggers.values().any(|d| d.table == table)
    }

    /// Trigger names defined on `table` (object explorer / console meta),
    /// catalog (name) order.
    pub fn trigger_names_of(&self, table: &str) -> Vec<String> {
        self.triggers
            .values()
            .filter(|d| d.table == table)
            .map(|d| d.name.clone())
            .collect()
    }

    /// Fire matching row triggers for one row image. `old`/`new` carry the
    /// OLD./NEW. images (None where the event has none: INSERT has no OLD,
    /// DELETE no NEW). `changed` names the columns an UPDATE assigns — an
    /// UPDATE OF trigger fires only when they intersect.
    fn fire_row_triggers(
        &mut self,
        table: &str,
        before: bool,
        ev: TriggerFireEvent<'_>,
        old: Option<&Object>,
        new: Option<&Object>,
    ) -> Result<()> {
        if self.triggers.is_empty() {
            return Ok(());
        }
        let matched: Vec<TriggerDef> = self
            .triggers
            .values()
            .filter(|d| {
                d.table == table
                    && d.before == before
                    && match ev {
                        TriggerFireEvent::Insert => d.insert,
                        TriggerFireEvent::Delete => d.delete,
                        TriggerFireEvent::Update(changed) => {
                            d.update
                                && match (&d.update_of, changed) {
                                    (None, _) | (_, None) => true,
                                    (Some(of), Some(ch)) => {
                                        ch.iter().any(|c| of.iter().any(|o| o == c))
                                    }
                                }
                        }
                    }
            })
            .cloned()
            .collect();
        if matched.is_empty() {
            return Ok(());
        }
        if self.trigger_depth >= MAX_TRIGGER_DEPTH {
            return err(format!(
                "trigger nesting too deep (>{MAX_TRIGGER_DEPTH}); likely an infinite trigger cycle"
            ));
        }
        self.trigger_depth += 1;
        // The triggering statement's canonical rewrite and assigned id are
        // ITS replication state — body statements get their own (validated
        // deterministic, so discarded) and the outer values come back.
        let saved_resolved = self.resolved_sql.take();
        let saved_last_id = self.last_insert_id.take();
        let r = self.run_trigger_bodies(&matched, table, old, new);
        self.resolved_sql = saved_resolved;
        self.last_insert_id = saved_last_id;
        self.trigger_depth -= 1;
        r
    }

    fn run_trigger_bodies(
        &mut self,
        defs: &[TriggerDef],
        firing_table: &str,
        old: Option<&Object>,
        new: Option<&Object>,
    ) -> Result<()> {
        for d in defs {
            if let Some(w) = &d.when {
                let mut e = parse_expr_text(w)?;
                subst_new_old_expr(&mut e, old, new)?;
                // WHEN passes only on TRUE (false and NULL skip, SQL
                // semantics).
                if !matches!(eval_expr(&e, &Object::new())?, Value::Bool(true)) {
                    continue;
                }
            }
            for a in &d.body {
                self.stmt_deadline.check()?;
                match a {
                    TriggerAction::Raise { msg, .. } => {
                        return err(format!("RAISE in trigger {}: {msg}", d.name));
                    }
                    TriggerAction::Sql(text) => {
                        let stmts = Parser::parse_sql(&GenericDialect {}, text)
                            .map_err(|e| SqlError::Parse(e.to_string()))?;
                        let Some(mut stmt) = stmts.into_iter().next() else {
                            continue;
                        };
                        // MySQL's rule, for the same reason: the triggering
                        // statement's row images are staged in ITS pager
                        // transaction until it commits — a body write to the
                        // same table would apply against the pre-statement
                        // pages and be overwritten when the outer statement
                        // commits. Reads stay legal (they see the committed
                        // pre-statement state deterministically, origin and
                        // replays alike).
                        if let Some(t) = trigger_dml_target(&stmt) {
                            if t == firing_table {
                                return err(format!(
                                    "cannot modify table {t} from a trigger on {t} while a \
                                     statement on {t} is in progress (split the write onto a \
                                     second table)"
                                ));
                            }
                        }
                        subst_new_old_stmt(&mut stmt, old, new)?;
                        let text = stmt.to_string();
                        if text_has_trigger_ref_outside_literals(&text) {
                            return err(
                                "NEW./OLD. references inside a subquery are not supported in \
                                 trigger bodies",
                            );
                        }
                        let parsed = Self::parse_classified(&text)?;
                        self.execute_parsed(parsed)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Statement guard for DML aimed at a trigger-carrying table: the
    /// statement and every trigger it fires commit or vanish together.
    /// Zero cost when no trigger targets the statement's table (the guard
    /// never opens) — trigger-less deployments keep their exact write path.
    fn exec_stmt(&mut self, stmt: Statement) -> Result<ExecOutcome> {
        if self.trigger_depth == 0 {
            if let Some(t) = trigger_dml_target(&stmt) {
                if self.triggers_exist_for(&t) {
                    return self.run_with_trigger_guard(move |slf| slf.exec_stmt_inner(stmt));
                }
            }
        }
        self.exec_stmt_inner(stmt)
    }

    fn run_with_trigger_guard(
        &mut self,
        run: impl FnOnce(&mut Self) -> Result<ExecOutcome>,
    ) -> Result<ExecOutcome> {
        if self.tx_snapshot.is_some() {
            // Inside an explicit transaction the statement-local revert is a
            // savepoint (page-undo mark + catalog snapshot): the transaction
            // and its earlier statements stay.
            let mark = self.pager.undo_mark();
            self.pager.undo_savepoint()?;
            let snap = self.catalog_snapshot();
            let r = run(self);
            if r.is_err() {
                let mut undo = self.pager.take_undo_to(mark);
                // sync_catalog=false — the journal replay already restored
                // the catalog pages (same cheap path as ROLLBACK TO).
                self.restore_transaction(snap, &mut undo, false)?;
            }
            return r;
        }
        // Autocommit: statement + triggers fuse into one write unit (one
        // WAL fsync, crash loses all of them together), and any failure
        // reverts pages (undo journal) and catalog (snapshot) exactly like
        // a transaction ROLLBACK — a half-fired statement never survives.
        self.pager.begin_undo();
        let snap = self.catalog_snapshot();
        let mut unit = self.write_unit();
        let r = run(&mut unit);
        match r {
            Ok(out) => {
                unit.end()?;
                self.pager.end_undo();
                Ok(out)
            }
            Err(e) => {
                // Mirror ROLLBACK: void the deferred frames BEFORE writing
                // the revert, or the revert's fence would commit the failed
                // work on a torn crash.
                let db: &mut Database = &mut unit;
                db.pager.abort_deferred()?;
                let mut undo = db.pager.take_undo();
                db.restore_transaction(snap, &mut undo, true)?;
                unit.end()?;
                Err(e)
            }
        }
    }

    /// CREATE MATERIALIZED VIEW: materialize the query into real storage
    /// and record the query for REFRESH. The query must be deterministic
    /// (no NEWID/RAND/wall-clock): REFRESH replays it on every node, and a
    /// nondeterministic result would silently fork the cluster's derived
    /// tables.
    fn exec_create_matview(&mut self, view: sqlparser::ast::CreateView) -> Result<ExecOutcome> {
        if view.secure || view.with_no_schema_binding {
            return err("SECURE / WITH NO SCHEMA BINDING is not supported");
        }
        if !view.columns.is_empty() {
            return err("CREATE MATERIALIZED VIEW column list is not supported                  (alias the SELECT outputs)");
        }
        let name = obj_name(&view.name);
        if name.starts_with('#') {
            return err("temp materialized views (#name) are not supported");
        }
        if crate::useradmin::is_user_table(&name) {
            return err(format!(
                "materialized view name {name} is reserved for the user/role subsystem"
            ));
        }
        if crate::engine::is_system_table(&name) || is_internal_table(&name) {
            return err(format!(
                "materialized view name {name} is reserved for the engine"
            ));
        }
        if is_compat_view(&name) {
            return err(format!(
                "materialized view name {name} is reserved for the compatibility \
                 dictionary views"
            ));
        }
        if let Some(existing) = self.tables.get(&name) {
            if !view.or_replace {
                if view.if_not_exists {
                    return Ok(ExecOutcome::Affected(0));
                }
                return err(format!("materialized view {name} already exists"));
            }
            if !existing.is_matview() {
                return err(format!(
                    "{name} is not a materialized view; use DROP {} before \
                     CREATE OR REPLACE MATERIALIZED VIEW",
                    if existing.is_view() { "VIEW" } else { "TABLE" }
                ));
            }
        }
        let query = view.query.to_string();
        // Determinism gates — REFRESH replays this text on every node over
        // the same base data; a nondeterministic result diverges silently.
        if crate::stmt::mentions_wall_clock(&query) {
            return err(
                "materialized view queries cannot call wall-clock functions \
                 (NOW/SYSDATE/CURRENT_TIMESTAMP/GETDATE): every node would \
                 recompute a different value at REFRESH",
            );
        }
        if crate::stmt::mentions_nondet_call(&query) {
            return err(
                "materialized view queries cannot call NEWID()/NEWSEQUENTIALID()/RAND(): \
                 every node would recompute a different value at REFRESH",
            );
        }
        // A materialized view over another materialized view would make the
        // dump's replay order (and every refresh cascade) a dependency
        // puzzle; v1 refuses the nesting — query base tables (or views).
        {
            let objects = |n: &str| self.tables.contains_key(n);
            let stmt = sqlparser::ast::Statement::Query(view.query.clone());
            let Some(targets) = Database::stmt_read_targets(&stmt, &objects) else {
                return err("materialized view queries must be plain SELECTs over base tables");
            };
            if let Some(mv) = targets
                .iter()
                .find(|t| self.tables.get(*t).is_some_and(|m| m.is_matview()))
            {
                return err(format!(
                    "materialized view {name} cannot query materialized view {mv} \
                     (refresh cascades and dump ordering are not supported)"
                ));
            }
        }
        // Materialize now (same shape CTAS produces: columns from the query
        // outputs, schemaless rows).
        let ExecOutcome::Rows(r) = self.exec_query_cx((*view.query).clone())? else {
            return err("CREATE MATERIALIZED VIEW requires a SELECT");
        };
        {
            let mut seen = std::collections::BTreeSet::new();
            if let Some(dup) = r.columns.iter().find(|c| !seen.insert((*c).clone())) {
                return err(format!("duplicate column name: {dup}"));
            }
        }
        let docs: Vec<Object> = r
            .rows
            .into_iter()
            .map(|row| r.columns.iter().cloned().zip(row).collect())
            .collect();
        let columns = r.columns;
        // OR REPLACE swaps the definition and the content in one statement:
        // reuse the storage (rewrite_table frees + rebuilds), never drop and
        // recreate (a mid-statement failure would leave nothing behind).
        let mut meta = match self.tables.get(&name).cloned() {
            Some(prev) if prev.is_matview() => {
                let mut m = prev.as_ref().clone();
                m.columns = columns;
                m
            }
            Some(prev) => prev.as_ref().clone(),
            None => TableMeta {
                name: name.clone(),
                columns,
                ..Default::default()
            },
        };
        meta.matview_sql = Some(query.clone());
        self.rewrite_table(&name, &mut meta, docs)?;
        Ok(ExecOutcome::Affected(0))
    }

    /// REFRESH MATERIALIZED VIEW: re-execute the stored query and replace
    /// the stored rows wholesale (one rewrite = one atomic statement). The
    /// stored text was determinism-validated at CREATE; re-validate at
    /// REFRESH so a pre-validation volume cannot replay a divergent query.
    fn exec_refresh_matview(&mut self, name: &str) -> Result<ExecOutcome> {
        let Some(meta) = self.tables.get(name).cloned() else {
            return err(format!("materialized view {name} does not exist"));
        };
        if !meta.is_matview() {
            return err(format!(
                "{name} is not a materialized view (it is a {})",
                if meta.is_view() { "view" } else { "table" }
            ));
        }
        let query = meta.matview_sql.clone().expect("checked above");
        if crate::stmt::mentions_wall_clock(&query) {
            return err("materialized view query calls wall-clock functions");
        }
        if crate::stmt::mentions_nondet_call(&query) {
            return err("materialized view query calls NEWID()/NEWSEQUENTIALID()/RAND()");
        }
        let parsed = Self::parse_classified(&query)?;
        let AnyStmt::Sql(stmt) = parsed.stmt else {
            return err("materialized view query must be a SELECT");
        };
        let Statement::Query(q) = *stmt else {
            return err("materialized view query must be a SELECT");
        };
        let ExecOutcome::Rows(r) = self.exec_query_cx(*q)? else {
            return err("materialized view query must be a SELECT");
        };
        {
            let mut seen = std::collections::BTreeSet::new();
            if let Some(dup) = r.columns.iter().find(|c| !seen.insert((*c).clone())) {
                return err(format!("duplicate column name: {dup}"));
            }
        }
        let docs: Vec<Object> = r
            .rows
            .into_iter()
            .map(|row| r.columns.iter().cloned().zip(row).collect())
            .collect();
        let mut meta = meta.as_ref().clone();
        meta.columns = r.columns;
        // One rewrite = one atomic statement: the old content and every
        // index tree rebuild together, readers never see a half-refreshed
        // view (MVCC snapshot reads see the pre-refresh content throughout).
        self.rewrite_table(name, &mut meta, docs)?;
        Ok(ExecOutcome::Affected(0))
    }

    fn exec_create_view(&mut self, view: sqlparser::ast::CreateView) -> Result<ExecOutcome> {
        if view.materialized {
            return self.exec_create_matview(view);
        }
        if view.secure || view.with_no_schema_binding {
            return err("SECURE / WITH NO SCHEMA BINDING views are not supported");
        }
        if !view.columns.is_empty() {
            // Column aliases would need per-row projection renaming; the
            // projection's own aliases already name the columns.
            return err("CREATE VIEW column list is not supported (alias the projection)");
        }
        let name = obj_name(&view.name);
        if name.starts_with('#') {
            return err("temp views (#name) are not supported");
        }
        if crate::useradmin::is_user_table(&name) {
            return err(format!(
                "view name {name} is reserved for the user/role subsystem"
            ));
        }
        if crate::engine::is_system_table(&name) {
            return err(format!("view name {name} is reserved for the engine"));
        }
        if is_compat_view(&name) {
            // Same shadow rule as CREATE TABLE: the compatibility dictionary
            // views always win at FROM-resolution, so a view created under
            // one of these names would be unreachable.
            return err(format!(
                "view name {name} is reserved for the compatibility dictionary views"
            ));
        }
        let exists = self.tables.contains_key(&name);
        if exists && !view.or_replace {
            if view.if_not_exists {
                return Ok(ExecOutcome::Affected(0));
            }
            return err(format!("view {name} already exists"));
        }
        // Views over the user/role tables are NOT refused here: dumps and
        // join snapshots from older volumes may legitimately contain one,
        // and refusing would break the whole replay. The leak is closed on
        // the READ side instead — the server's authorize expands view base
        // tables and refuses user-table bases to non-admins (same rule the
        // write path applies), so such a view is admin-readable only.
        if exists {
            let meta = self.tables.get(&name).expect("checked just above");
            if !meta.is_view() {
                return err(format!(
                    "{name} is a table; use DROP TABLE before CREATE VIEW"
                ));
            }
        }
        let body = format!("{}", view.query);
        // Cycle guard: with OR REPLACE the old view still resolves during
        // the dry-run below, and a replacement body can point back at
        // itself through other views (a→b, then `CREATE OR REPLACE VIEW
        // a AS SELECT * FROM b`). A direct self-reference is rejected up
        // front; for OR REPLACE the view→view edges reachable from the
        // new body must never return to `name` — otherwise every later
        // SELECT would recurse until the expansion budget trips.
        {
            let objects = |n: &str| self.tables.contains_key(n);
            let mut refs = Vec::new();
            let mut stmts = Parser::parse_sql(&GenericDialect {}, &body)
                .map_err(|e| SqlError::Parse(e.to_string()))?;
            if let Statement::Query(q) = stmts.swap_remove(0) {
                if walk_query(&q, &mut refs, &objects).is_none() {
                    return err("view body cannot be classified");
                }
                if refs.iter().any(|r| r == &name) {
                    return err(format!("view {name} cannot reference itself"));
                }
                if exists {
                    let mut stack = refs.clone();
                    let mut seen = std::collections::BTreeSet::new();
                    while let Some(next) = stack.pop() {
                        if next == name {
                            return err(format!(
                                "view {name} cannot reference itself (through {next})"
                            ));
                        }
                        if !seen.insert(next.clone()) {
                            continue;
                        }
                        if let Some(deeper) = self.view_base_tables(&next) {
                            stack.extend(deeper);
                        }
                    }
                }
            } else {
                return err("view body must be a SELECT");
            }
        }
        // Dry-run the body once: missing tables/columns/unsupported shapes
        // fail at CREATE time, not at first SELECT. The body executes as a
        // plain SELECT over the CURRENT catalog (a self-reference errors —
        // the name does not resolve yet — which is exactly the cycle check).
        // The view under construction consumes one expansion level of its
        // own, so a chain that would be too deep to SELECT is also too deep
        // to CREATE (creatable implies selectable).
        {
            let _guard = enter_view_expansion()?;
            let q = Self::plain_read_query(&Self::parse_classified(&body)?)?;
            self.exec_query_cx(q)?;
        }
        let meta = TableMeta {
            name: name.clone(),
            view_sql: Some(body),
            ..Default::default()
        };
        // Keep the old entry (or absence) so a failed catalog save leaves
        // the in-memory catalog untouched — a ghost view that only exists
        // here would persist on the next unrelated write and diverge every
        // peer (same snapshot discipline as exec_create/exec_drop).
        let prev = self.tables.insert(name.clone(), std::sync::Arc::new(meta));
        if let Err(e) = self.save_catalog() {
            match prev {
                Some(old) => {
                    self.tables.insert(name, old);
                }
                None => {
                    self.tables.remove(&name);
                }
            }
            return Err(e);
        }
        Ok(ExecOutcome::Affected(0))
    }

    fn exec_create(&mut self, create: sqlparser::ast::CreateTable) -> Result<ExecOutcome> {
        use sqlparser::ast::ColumnOption as CO;
        let name = obj_name(&create.name);
        if name.starts_with('#') {
            // T-SQL temp tables are session-scoped; making them persistent
            // global tables would silently change their lifetime/visibility.
            return err("temp tables (#name/##name) are not supported");
        }
        if crate::useradmin::is_user_table(&name) && !self.internal_ddl {
            return err(format!(
                "table name {name} is reserved for the user/role subsystem"
            ));
        }
        if crate::engine::is_system_table(&name) && !self.internal_ddl {
            // Squatting an engine system name would break replication,
            // digests and pub/sub on this database (the tables are excluded
            // from dumps by name). Only the internal replay path may create
            // reserved names, and it never creates these.
            return err(format!("table name {name} is reserved for the engine"));
        }
        if is_compat_view(&name) && !self.internal_ddl {
            // The dictionary/compat views always shadow a same-named table
            // at FROM-resolution (DUAL/sqlite_master/information_schema and
            // the Oracle dictionary spellings): a table created under one of
            // these names accepts writes but every SELECT reads the
            // dictionary instead — data that can never be read back. Refuse
            // at CREATE instead (existing pre-guard tables keep their
            // shadowed behavior).
            return err(format!(
                "table name {name} is reserved for the compatibility dictionary views"
            ));
        }
        if self.tables.contains_key(&name) {
            if create.if_not_exists {
                return Ok(ExecOutcome::Affected(0));
            }
            return err(format!("table {name} already exists"));
        }
        // Clause-level guard: a CREATE TABLE shape the engine cannot honor
        // must be refused, not silently degraded (`LIKE` used to create a
        // zero-column table; OR REPLACE ignored the replacement semantics).
        if create.like.is_some() || create.clone.is_some() {
            return err("CREATE TABLE ... LIKE/CLONE is not supported");
        }
        if create.or_replace {
            return err("CREATE OR REPLACE TABLE is not supported (drop the table first)");
        }
        if create.external
            || create.global.is_some()
            || create.transient
            || create.volatile
            || create.iceberg
            || !matches!(
                create.table_options,
                sqlparser::ast::CreateTableOptions::None
            )
            || create.cluster_by.is_some()
            || create.partition_by.is_some()
            || create.order_by.is_some()
        {
            return err("CREATE TABLE storage/partitioning options are not supported");
        }
        if create.strict || create.comment.is_some() {
            return err("CREATE TABLE engine/comment options are not supported");
        }
        // Storage/layout clauses the GenericDialect parses but the engine
        // cannot honor. They used to be dropped silently (same red-line
        // class as the options above): inheriting columns, rowid layout,
        // Hive/Redshift placement and partition bounds all change what the
        // statement means, so refusing loudly is the only honest answer.
        if create.inherits.is_some()
            || create.without_rowid
            || create.on_commit.is_some()
            || create.on_cluster.is_some()
            || create.partition_of.is_some()
            || create.for_values.is_some()
            || create.location.is_some()
            || create.file_format.is_some()
            || create.clustered_by.is_some()
            || !matches!(
                create.hive_distribution,
                sqlparser::ast::HiveDistributionStyle::NONE
            )
            || create.hive_formats.is_some()
            || create.diststyle.is_some()
            || create.distkey.is_some()
            || create.sortkey.is_some()
            || create.backup.is_some()
        {
            return err("CREATE TABLE storage/partitioning options are not supported");
        }
        // CREATE TABLE ... AS SELECT: shape and rows come from the query.
        // (`temporary` is accepted and treated as a regular table.)
        if let Some(q) = &create.query {
            let ExecOutcome::Rows(r) = self.exec_query_cx(q.as_ref().clone())? else {
                return err("CREATE TABLE AS requires a SELECT");
            };
            // An explicit column list alongside AS SELECT would rename the
            // outputs (and its constraints would apply) — silently dropping
            // it shaped the table after the query instead. Refuse loudly
            // rather than ignore half the statement.
            if !create.columns.is_empty() {
                return err(
                    "CREATE TABLE (…) AS SELECT with an explicit column list is not supported \
                     (omit the list, or alias the SELECT outputs)",
                );
            }
            let docs: Vec<Object> = r
                .rows
                .into_iter()
                .map(|row| r.columns.iter().cloned().zip(row).collect())
                .collect();
            // Duplicate output names would collapse two logical columns into
            // one document key (the later value silently wins) — same rule
            // as the explicit column list below.
            {
                let mut seen = std::collections::BTreeSet::new();
                if let Some(dup) = r.columns.iter().find(|c| !seen.insert((*c).clone())) {
                    return err(format!("duplicate column name: {dup}"));
                }
            }
            let mut meta = TableMeta {
                name: name.clone(),
                columns: r.columns,
                ..Default::default()
            };
            self.rewrite_table(&name, &mut meta, docs)?;
            return Ok(ExecOutcome::Affected(0));
        }
        let columns: Vec<String> = create
            .columns
            .iter()
            .map(|c| c.name.value.clone())
            .collect();
        // Duplicate column names collapse two logical columns into one
        // document key (the later one silently wins); reject like SQLite.
        {
            let mut seen = std::collections::BTreeSet::new();
            if let Some(dup) = columns.iter().find(|c| !seen.insert((*c).clone())) {
                return err(format!("duplicate column name: {dup}"));
            }
        }
        let mut meta = TableMeta {
            name: name.clone(),
            columns,
            ..Default::default()
        };
        for col in &create.columns {
            for opt in &col.options {
                match &opt.option {
                    CO::PrimaryKey(pk) => {
                        reject_constraint_decorations(
                            "PRIMARY KEY",
                            pk.characteristics.as_ref(),
                            None,
                            None,
                        )?;
                        // A second PK declaration silently replaced the
                        // first (its uniqueness tree was never built).
                        if meta.primary_key.replace(col.name.value.clone()).is_some() {
                            return err("table has more than one primary key".to_string());
                        }
                    }
                    CO::Unique(u) => {
                        reject_constraint_decorations(
                            "UNIQUE",
                            u.characteristics.as_ref(),
                            Some(&u.nulls_distinct),
                            None,
                        )?;
                        // Deduplicate like the table-level constraint below:
                        // `a INT UNIQUE UNIQUE` pushed the column twice, the
                        // tree-maintenance loop inserted the same key twice,
                        // and the table was permanently unwritable.
                        let col = col.name.value.clone();
                        if !meta.unique.contains(&col) {
                            meta.unique.push(col.clone());
                        }
                        if !meta.constraint_unique.contains(&col) {
                            meta.constraint_unique.push(col);
                        }
                    }
                    CO::NotNull => meta.not_null.push(col.name.value.clone()),
                    CO::Null => {}
                    CO::Default(e) => {
                        meta.defaults
                            .push((col.name.value.clone(), default_expr_text(e)));
                    }
                    CO::Check(c) => {
                        if expr_calls_wall_clock(&c.expr) {
                            return err("CHECK constraints cannot call wall-clock or random \
                                 functions (NOW/SYSDATE/CURRENT_TIMESTAMP/GETDATE/RAND); \
                                 every node would evaluate a different value");
                        }
                        // Unknown column references resolve to NULL at
                        // evaluation time, so a typo'd CHECK would never
                        // fire — validate at DDL time.
                        let mut refs = Vec::new();
                        check_expr_column_refs(&c.expr, &mut refs)?;
                        for r in refs {
                            if !meta.columns.contains(&r) {
                                return err(format!(
                                    "CHECK constraint references unknown column {r}"
                                ));
                            }
                        }
                        meta.checks.push(format!("{}", c.expr))
                    }
                    CO::ForeignKey(fk) => {
                        reject_constraint_decorations(
                            "FOREIGN KEY",
                            fk.characteristics.as_ref(),
                            None,
                            fk.match_kind.as_ref(),
                        )?;
                        if fk.on_delete.is_some() || fk.on_update.is_some() {
                            return err(
                                "FOREIGN KEY ON DELETE/ON UPDATE actions are not supported \
                                 (declare the constraint and keep parent keys stable)",
                            );
                        }
                        if fk.referred_columns.len() != 1 {
                            // Silently taking the first referred column
                            // quietly changed the constraint's meaning.
                            return err("column-level FOREIGN KEY must reference a single column");
                        }
                        let (Some(rt), Some(rc)) = (
                            fk.foreign_table.0.last().map(|p| match p {
                                ObjectNamePart::Identifier(i) => i.value.clone(),
                                other => other.to_string(),
                            }),
                            fk.referred_columns.first().map(|c| c.value.clone()),
                        ) else {
                            return err("FOREIGN KEY requires a table and column");
                        };
                        meta.foreign_keys.push((col.name.value.clone(), rt, rc));
                    }
                    CO::DialectSpecific(tokens) => {
                        let text = tokens
                            .iter()
                            .map(|t| t.to_string().to_uppercase())
                            .collect::<Vec<_>>()
                            .join(" ");
                        if text.contains("AUTOINCREMENT") || text.contains("AUTO_INCREMENT") {
                            if is_guid_type(&col.data_type) {
                                // GUID columns treat the auto flag as
                                // UUIDv7 generation instead of max+1.
                                meta.autoguid = Some(col.name.value.clone());
                            } else {
                                meta.autoinc = Some(col.name.value.clone());
                            }
                        } else {
                            return err("unsupported column constraint");
                        }
                    }
                    _ => return err("unsupported column constraint"),
                }
            }
        }
        // Table-level constraints.
        for c in &create.constraints {
            use sqlparser::ast::TableConstraint as TC;
            match c {
                TC::Unique(u) => {
                    reject_constraint_decorations(
                        "UNIQUE",
                        u.characteristics.as_ref(),
                        Some(&u.nulls_distinct),
                        None,
                    )?;
                    // A multi-column constraint would need a composite unique
                    // tree; declaring it as per-column uniques silently
                    // changes the semantics (any single column becomes
                    // unique), so refuse and point at the supported form.
                    if u.columns.len() != 1 {
                        return err("composite UNIQUE table constraints are not supported \
                             (use CREATE UNIQUE INDEX)");
                    }
                    for ic in &u.columns {
                        if !matches!(ic.column.expr, SqlExpr::Identifier(_)) {
                            return err("expression UNIQUE constraints are not supported \
                                 (constraint a plain column)");
                        }
                        let col = expr_name(&ic.column.expr);
                        if !meta.columns.contains(&col) {
                            // A ghost column builds a constraint tree that can
                            // never fire and vanishes from dump_script.
                            return err(format!("UNIQUE column {col} does not exist"));
                        }
                        if !meta.unique.contains(&col) {
                            meta.unique.push(col.clone());
                            meta.constraint_unique.push(col);
                        }
                    }
                }
                TC::PrimaryKey(pk) => {
                    reject_constraint_decorations(
                        "PRIMARY KEY",
                        pk.characteristics.as_ref(),
                        None,
                        None,
                    )?;
                    // DocSQL has single-column primary keys; a composite key
                    // must not silently degrade to its first column.
                    if pk.columns.len() != 1 {
                        return err("composite PRIMARY KEY is not supported \
                             (declare a single-column PRIMARY KEY or use CREATE UNIQUE INDEX)");
                    }
                    if let Some(ic) = pk.columns.first() {
                        if !matches!(ic.column.expr, SqlExpr::Identifier(_)) {
                            return err("expression PRIMARY KEY constraints are not supported \
                                 (constraint a plain column)");
                        }
                        let name = expr_name(&ic.column.expr);
                        if !meta.columns.contains(&name) {
                            return err(format!("PRIMARY KEY column {name} does not exist"));
                        }
                        if meta.primary_key.replace(name).is_some() {
                            return err("table has more than one primary key".to_string());
                        }
                    }
                }
                TC::Check(chk) => {
                    if expr_calls_wall_clock(&chk.expr) {
                        return err("CHECK constraints cannot call wall-clock functions \
                             (NOW/SYSDATE/CURRENT_TIMESTAMP); every node would \
                             evaluate a different instant");
                    }
                    let mut refs = Vec::new();
                    check_expr_column_refs(&chk.expr, &mut refs)?;
                    for r in refs {
                        if !meta.columns.contains(&r) {
                            return err(format!("CHECK constraint references unknown column {r}"));
                        }
                    }
                    meta.checks.push(format!("{}", chk.expr))
                }
                TC::ForeignKey(fk) => {
                    reject_constraint_decorations(
                        "FOREIGN KEY",
                        fk.characteristics.as_ref(),
                        None,
                        fk.match_kind.as_ref(),
                    )?;
                    if fk.on_delete.is_some() || fk.on_update.is_some() {
                        return err("FOREIGN KEY ON DELETE/ON UPDATE actions are not supported \
                             (declare the constraint and keep parent keys stable)");
                    }
                    let Some(rt) = fk.foreign_table.0.last().map(|p| match p {
                        ObjectNamePart::Identifier(i) => i.value.clone(),
                        other => other.to_string(),
                    }) else {
                        return err("FOREIGN KEY requires a table");
                    };
                    // A composite FK silently split into per-column pairs
                    // enforces a different (weaker) constraint — each column
                    // independently instead of the tuple. Refuse like the
                    // composite PK/UNIQUE forms.
                    if fk.columns.len() != 1 {
                        return err("composite FOREIGN KEY constraints are not supported");
                    }
                    // zip 会静默截断较短的列清单,约束悄悄少一列 —— 列数
                    // 不匹配必须显式报错(子句级静默忽略是红线)。
                    if fk.columns.len() != fk.referred_columns.len() {
                        return err(format!(
                            "FOREIGN KEY column count mismatch: {} referencing columns \
                             vs {} referred columns",
                            fk.columns.len(),
                            fk.referred_columns.len()
                        ));
                    }
                    for (lc, rc) in fk.columns.iter().zip(fk.referred_columns.iter()) {
                        if !meta.columns.contains(&lc.value) {
                            return err(format!("FOREIGN KEY column {} does not exist", lc.value));
                        }
                        meta.foreign_keys
                            .push((lc.value.clone(), rt.clone(), rc.value.clone()));
                    }
                }
                other => return err(format!("unsupported table constraint: {other}")),
            }
        }
        // Constraint columns get their B+ trees right away: duplicate
        // enforcement and index probes depend on them existing. The trees
        // ride the SAME write unit as the catalog entry (the shape
        // `exec_create_index` settled on): committing the tree first and
        // Forward-declared FKs (referencing a table that does not exist
        // yet) can form DECLARATION cycles the dump's topological order
        // cannot break — the fallback emits cycle members in catalog
        // order, and a replay then inserts child rows before their
        // parents exist (restore fails mid-flight). Self-references are
        // fine: the table being created is its own parent. When the
        // parent exists, the referenced column must be one of its
        // declared columns — an undeclared field would silently match
        // nothing and every child write would fail the constraint.
        for (fc, rt, rc) in &meta.foreign_keys {
            if rt == &name {
                continue;
            }
            let Some(parent) = self.tables.get(rt) else {
                return err(format!(
                    "FOREIGN KEY on {name}.{fc} references table {rt}, which does not exist \
                     (create {rt} first)"
                ));
            };
            if parent.is_view() {
                return err(format!(
                    "FOREIGN KEY on {name}.{fc} cannot reference the VIEW {rt}"
                ));
            }
            if !parent.columns.contains(rc) {
                return err(format!(
                    "FOREIGN KEY on {name}.{fc} references unknown column {rt}.{rc}"
                ));
            }
        }
        // saving the catalog in a second transaction left a crash window
        // holding a committed tree no catalog references — those pages would
        // leak for good, nothing would ever free them.
        let constraint_cols: Vec<String> = meta
            .primary_key
            .iter()
            .chain(meta.unique.iter())
            .cloned()
            .collect();
        let mut tx = self.pager.begin_tx();
        if !constraint_cols.is_empty() {
            let roots = self.build_trees(
                &mut tx,
                &[],
                &constraint_cols,
                &constraint_cols
                    .iter()
                    .map(|c| vec![c.clone()])
                    .collect::<Vec<_>>(),
                &vec![None; constraint_cols.len()],
                &vec![true; constraint_cols.len()],
                &vec![None; constraint_cols.len()],
            )?;
            meta.index_roots = roots;
        }
        self.tables.insert(name.clone(), std::sync::Arc::new(meta));
        if let Err(e) = self
            .save_catalog_into(&mut tx)
            .and_then(|()| self.commit_pager_tx(tx))
        {
            // The tx dropped without committing: staged tree pages went back
            // to the reusable pool, and the in-memory entry must not keep
            // pointing at them.
            self.tables.remove(&name);
            return Err(e);
        }
        Ok(ExecOutcome::Affected(0))
    }

    fn exec_insert(&mut self, insert: sqlparser::ast::Insert) -> Result<ExecOutcome> {
        if insert.output.is_some() {
            return err("OUTPUT is not supported (use RETURNING)");
        }
        let TableObject::TableName(name) = &insert.table else {
            return err("unsupported INSERT target");
        };
        let table = obj_name(name);
        // Arc bump, not a meta deep clone: INSERT only ever reads the meta,
        // and this is the hottest write path.
        let Some(meta) = self.tables.get(&table).cloned() else {
            return err(format!("table {table} does not exist"));
        };
        ensure_not_view("INSERT into", &table, &meta)?;
        let mut columns: Vec<String> = if insert.columns.is_empty() {
            meta.columns.clone()
        } else {
            // A repeated name makes the later zip().collect() silently
            // overwrite the earlier value — reject like mainstream engines.
            let cols: Vec<String> = insert.columns.iter().map(obj_name).collect();
            let mut seen = std::collections::BTreeSet::new();
            if let Some(dup) = cols.iter().find(|c| !seen.insert((*c).clone())) {
                return err(format!("duplicate column name: {dup}"));
            }
            // Deliberately NOT validated against meta.columns: this is a
            // document store — an explicit list may name schemaless fields
            // the declared columns have not seen yet (observed-columns
            // tracking and the schemaless SELECT * union depend on it;
            // useradmin's own grants rows ride the same behavior).
            cols
        };
        let Some(source) = &insert.source else {
            return err("INSERT requires VALUES");
        };
        let rows: Vec<Vec<Value>> = match &*source.body {
            SetExpr::Values(value_rows) => {
                let mut rows = Vec::new();
                for parens in &value_rows.rows {
                    rows.push(
                        parens
                            .content
                            .iter()
                            .map(eval_const)
                            .collect::<Result<_>>()?,
                    );
                }
                rows
            }
            // INSERT INTO t (cols...) SELECT ...: take the SELECT's rows.
            // Without a column list the mapping is POSITIONAL onto the
            // target table's declared columns (standard SQL) — the query's
            // output NAMES used to redefine the keys, silently dropping
            // every value into ghost fields whenever the projection wasn't
            // bare same-named identifiers (aliases, literals, `SELECT *`
            // from a differently-named source).
            SetExpr::Select(_) => {
                let ExecOutcome::Rows(r) = self.exec_query_cx(source.as_ref().clone())? else {
                    return err("INSERT source must be VALUES or SELECT");
                };
                if r.columns.len() != columns.len() {
                    return err(format!(
                        "INSERT SELECT has {} columns but {} are expected",
                        r.columns.len(),
                        columns.len()
                    ));
                }
                r.rows
            }
            other => return err(format!("unsupported INSERT source: {other}")),
        };
        // An empty VALUES list is a syntax-level mistake; an empty SELECT
        // source (WHERE filtered everything) inserts 0 rows like every
        // mainstream engine — ETL batches legitimately hit it.
        if rows.is_empty() && matches!(&*source.body, SetExpr::Values(_)) {
            return err("INSERT has no rows");
        }
        // INSERT ... SELECT into an auto-GUID table cannot be replicated:
        // peers re-run the SELECT and would each fill their own random
        // GUIDs, silently diverging. VALUES is required.
        if meta.autoguid.is_some() && matches!(&*source.body, SetExpr::Select(_)) {
            return err(
                "auto-generated GUID columns do not support INSERT ... SELECT \
                 (use INSERT ... VALUES, with or without explicit ids)",
            );
        }
        // Build all documents first, validate, and only then write — a
        // failed statement must leave the table untouched.
        // AUTOINCREMENT: append the column when the INSERT omits it, then
        // assign max(id)+1 to missing/NULL values per row. Auto-generated
        // GUID columns follow the same omitted/NULL fill with fresh
        // time-ordered UUIDv7 values.
        let autoinc_appended = match &meta.autoinc {
            Some(col) if !columns.contains(col) => {
                columns.push(col.clone());
                true
            }
            _ => false,
        };
        let autoguid_appended = match &meta.autoguid {
            Some(col) if !columns.contains(col) => {
                columns.push(col.clone());
                true
            }
            _ => false,
        };
        let mut guid_filled = autoguid_appended;
        // A wall-clock DEFAULT (`DEFAULT NOW()`) resolved per row on THIS
        // node: the journaled INSERT must carry the explicit value or every
        // peer would stamp its own clock at replay (same rewrite trigger as
        // auto-GUID below).
        let mut default_clock_filled = false;
        let mut next_autoinc = self.autoinc_next_for(&table, &meta)?;
        // DEFAULT texts parsed once per statement (was re-parsed by
        // sqlparser for every row that omitted the column).
        let mut default_exprs: std::collections::HashMap<String, SqlExpr> =
            std::collections::HashMap::new();
        let mut new_docs: Vec<Object> = Vec::new();
        for row in rows {
            // INSERT … SELECT walks every source row (millions on a big
            // backfill): the timeout must be able to interrupt it.
            self.stmt_deadline.check()?;
            let mut row = row;
            if autoinc_appended {
                row.push(Value::Int(next_autoinc));
                next_autoinc = next_autoinc.saturating_add(1);
            }
            if autoguid_appended {
                row.push(Value::Str(crate::guid::uuidv7()));
            }
            if row.len() != columns.len() {
                return err(format!(
                    "INSERT has {} values but {} columns",
                    row.len(),
                    columns.len()
                ));
            }
            if !autoinc_appended {
                if let Some(col) = &meta.autoinc {
                    if let Some(idx) = columns.iter().position(|c| c == col) {
                        match row.get(idx) {
                            Some(Value::Null) | None => {
                                row[idx] = Value::Int(next_autoinc);
                                next_autoinc = next_autoinc.saturating_add(1);
                            }
                            // An explicit id at/above the statement-start
                            // watermark must advance the watermark *within*
                            // the statement, or a later row of the same
                            // INSERT that omits the id reuses it and dies on
                            // UNIQUE.
                            Some(Value::Int(v)) => {
                                next_autoinc = next_autoinc.max(v.saturating_add(1));
                            }
                            _ => {}
                        }
                    }
                }
            }
            if !autoguid_appended {
                if let Some(col) = &meta.autoguid {
                    if let Some(idx) = columns.iter().position(|c| c == col) {
                        if matches!(row.get(idx), Some(Value::Null) | None) {
                            row[idx] = Value::Str(crate::guid::uuidv7());
                            guid_filled = true;
                        }
                    }
                }
            }
            let mut doc: Object = columns.iter().cloned().zip(row).collect();
            // DEFAULT: fill declared columns the INSERT omitted (parse once
            // per statement; evaluation stays per row).
            for (col, text) in &meta.defaults {
                if !doc.contains_key(col) {
                    let e = match default_exprs.get(col) {
                        Some(e) => e,
                        None => {
                            let e = parse_expr_text(text)?;
                            default_exprs.insert(col.clone(), e);
                            default_exprs.get(col).expect("just inserted")
                        }
                    };
                    if expr_calls_wall_clock(e) {
                        default_clock_filled = true;
                    }
                    doc.insert(col.clone(), eval_const(e)?);
                }
            }
            meta.check(&doc)?;
            new_docs.push(doc);
        }
        // Conflict policy: plain (error on duplicates), REPLACE INTO /
        // OR REPLACE (drop conflicting rows first), ON CONFLICT DO NOTHING /
        // OR IGNORE (skip conflicting new rows), and the upsert-UPDATE
        // forms: ON CONFLICT <target> DO UPDATE SET … [WHERE …]
        // (PostgreSQL) / ON DUPLICATE KEY UPDATE … (MySQL) — the
        // conflicting row is updated in place.
        let replace = insert.replace_into
            || matches!(insert.or, Some(sqlparser::ast::SqliteOnConflict::Replace));
        let do_nothing = matches!(
            insert.on,
            Some(sqlparser::ast::OnInsert::OnConflict(ref c))
                if matches!(c.action, sqlparser::ast::OnConflictAction::DoNothing)
        ) || matches!(insert.or, Some(sqlparser::ast::SqliteOnConflict::Ignore));
        let (do_update, du_assignments, du_selection) = match &insert.on {
            Some(sqlparser::ast::OnInsert::OnConflict(c)) => match &c.action {
                sqlparser::ast::OnConflictAction::DoUpdate(du) => {
                    (true, du.assignments.clone(), du.selection.clone())
                }
                _ => (false, Vec::new(), None),
            },
            Some(sqlparser::ast::OnInsert::DuplicateKeyUpdate(assignments)) => {
                (true, assignments.clone(), None)
            }
            _ => (false, Vec::new(), None),
        };
        if replace && do_nothing {
            // Displacement deletes the conflicting old rows first, then DO
            // NOTHING skips every incoming row — a silent net delete. Reject
            // the mix like the DO UPDATE combination below.
            return err("OR REPLACE / REPLACE INTO cannot combine with ON CONFLICT DO NOTHING");
        }
        if do_update {
            if replace || do_nothing {
                return err(
                    "OR REPLACE / REPLACE INTO / OR IGNORE cannot combine with an upsert-UPDATE clause",
                );
            }
            // Non-deterministic values in the update arm would diverge the
            // cluster: peers replay the statement text verbatim (INSERT
            // defaults are rewritten with resolved literals, the UPDATE arm
            // has no such rewrite — same rule as UPDATE/DELETE/MERGE;
            // wall clocks are exempt because the fold covers them).
            for e in du_assignments
                .iter()
                .map(|a| &a.value)
                .chain(du_selection.iter())
            {
                if calls_newid(e) {
                    return err(
                        "NEWID()/NEWSEQUENTIALID() is not supported in DO UPDATE SET/WHERE \
                         (replication replay would diverge)",
                    );
                }
            }
            // Wall-clock functions in the arm are FOLDED to literals by the
            // statement-level preprocessing (fold_wall_clocks rewrites the
            // whole text, on-clause included), so they are accepted with
            // deterministic replay — no guard here.
            if refs_rownum_in(&du_assignments, &du_selection) {
                return err("ROWNUM is not supported in DO UPDATE SET/WHERE");
            }
            // PostgreSQL requires the conflict target for DO UPDATE; the
            // MySQL spelling is untargeted by definition (any unique key).
            if matches!(
                insert.on,
                Some(sqlparser::ast::OnInsert::OnConflict(ref c))
                    if c.conflict_target.is_none()
            ) {
                return err(
                    "ON CONFLICT DO UPDATE requires a conflict target (PostgreSQL semantics) — \
                     name the unique key with ON CONFLICT (cols) / ON CONSTRAINT, or use the \
                     MySQL spelling ON DUPLICATE KEY UPDATE for any-key matching",
                );
            }
            // EXCLUDED / VALUES() are rewritten only in the direct SET/WHERE
            // trees; inside a subquery they would mis-resolve to the
            // existing row's column — reject loudly instead.
            for e in du_assignments
                .iter()
                .map(|a| &a.value)
                .chain(du_selection.iter())
            {
                if upsert_proposed_in_subquery(e) {
                    return err(
                        "EXCLUDED / VALUES() inside a subquery of DO UPDATE SET/WHERE is not supported",
                    );
                }
            }
        }
        // Replication-safe rewrite: the statement as written would let every
        // peer fill its own random GUIDs — or stamp its own clock for a
        // wall-clock DEFAULT. Emit a canonical INSERT carrying the generated
        // ids / resolved instants explicitly — with the conflict clause
        // preserved so peers resolve the same conflicts against identical
        // state. NEWID()/NEWSEQUENTIALID() in the source (VALUES or SELECT)
        // are the same class: the writing node rolls the ids and ships the
        // literal values.
        let source_calls_nondet = insert
            .source
            .as_ref()
            .is_some_and(|s| crate::stmt::mentions_nondet_call(&s.to_string()));
        // A zero-row INSERT ... SELECT (e.g. NEWID() in the source under a
        // false WHERE) must NOT enter the rewrite: with no documents the
        // canonical form below degenerates to `INSERT INTO t () VALUES ` —
        // unparseable text that rode the journal and broke every replaying
        // peer. The original statement replays identically (deterministically
        // empty) on each node, so keeping it is replication-safe.
        if (guid_filled || default_clock_filled || source_calls_nondet) && !new_docs.is_empty() {
            let policy = if replace {
                "OR REPLACE "
            } else if do_nothing {
                "OR IGNORE "
            } else {
                ""
            };
            let mut sql = format!("INSERT {}INTO {} (", policy, quote_ident(&table));
            let cols: Vec<String> = new_docs
                .first()
                .map(|d| d.keys().cloned().collect())
                .unwrap_or_default();
            sql.push_str(
                &cols
                    .iter()
                    .map(|c| quote_ident(c))
                    .collect::<Vec<_>>()
                    .join(", "),
            );
            sql.push_str(") VALUES ");
            let mut rendered = Vec::with_capacity(new_docs.len());
            for doc in &new_docs {
                let vals = cols
                    .iter()
                    .map(|c| value_literal(doc.get(c).unwrap_or(&Value::Null)))
                    .collect::<Result<Vec<_>>>()?;
                rendered.push(format!("({})", vals.join(", ")));
            }
            sql.push_str(&rendered.join(", "));
            // Preserve the conflict clause verbatim: the OR-prefix only
            // covers the SQLite spellings, and a targeted ON CONFLICT scope
            // must not widen to OR IGNORE on replaying peers.
            if let Some(on) = &insert.on {
                sql.push_str(&on.to_string());
            }
            self.resolved_sql = Some(sql);
        } else if source_calls_nondet && new_docs.is_empty() {
            // Zero rows from a NONDETERMINISTIC source: replaying the
            // original text re-rolls RAND()/NEWID() on every peer — an
            // empty draw here can be non-empty there, silently forking the
            // cluster. Ship a deterministically-empty statement instead.
            // (INSERT ... SELECT is safe to emit: an auto-GUID table never
            // reaches this point — that shape is rejected above.)
            let col = meta.columns.first().cloned().unwrap_or_else(|| "v".into());
            let q = quote_ident(&col);
            self.resolved_sql = Some(format!(
                "INSERT INTO {} ({q}) SELECT {q} FROM (VALUES (NULL)) AS __empty__({q}) WHERE 1 = 0",
                quote_ident(&table)
            ));
        }

        // One pager transaction for heap pages and index trees alike: a
        // statement is either fully applied or not at all, and commits once.
        let mut heap = Heap {
            pages: meta.pages.clone(),
            overflow_free: meta.overflow_free.clone(),
            dropped: Vec::new(),
        };
        let mut roots = meta.index_roots.clone();
        let mut tx = self.pager.begin_tx();

        // Constraint columns with a usable tree: duplicates are detected and
        // enforced through the trees (O(log n) per row) instead of scanning
        // and cloning the whole table.
        let indexed: Vec<String> = meta
            .primary_key
            .iter()
            .chain(meta.unique.iter())
            .filter(|c| roots.contains_key(*c))
            .cloned()
            .collect();
        // Every indexed column (constraint trees + plain CREATE INDEX trees),
        // hoisted once so the per-row maintenance loops below do not clone
        // the roots map per row.
        let idx_specs = idx_specs(&meta, &roots);
        // Composite UNIQUE trees (CREATE UNIQUE INDEX over several columns)
        // take part in the *undirected* conflict policies (OR IGNORE /
        // REPLACE INTO / ON DUPLICATE KEY UPDATE = "any unique key") exactly
        // like the single-column constraint trees — probing only `indexed`
        // let those conflicts fall through to the tree-level insert and
        // error instead of skipping/replacing/updating.
        let composite_uniques: Vec<&IdxSpec> = idx_specs
            .iter()
            .filter(|s| s.unique && s.cols.len() > 1)
            .collect();
        // PARTIAL UNIQUE trees (CREATE UNIQUE INDEX … WHERE): their
        // conflicts are predicate-scoped — a candidate row conflicts only
        // when it ITSELF satisfies the predicate (then the hit row is in
        // the tree by construction). Probed for the undirected policies and
        // OR REPLACE displacement; never an ON CONFLICT target scope
        // (resolve_conflict_columns knows constraint columns only).
        let partial_uniques: Vec<&IdxSpec> = idx_specs
            .iter()
            .filter(|s| s.unique && s.pred.is_some())
            .collect();

        // ON CONFLICT [target]: the target names one unique constraint whose
        // conflicts are skipped; without it every unique constraint takes
        // part (SQLite/PG semantics). An unresolvable target errors instead
        // of silently widening the scope.
        let conflict_scope: Option<String> = match &insert.on {
            Some(sqlparser::ast::OnInsert::OnConflict(c)) => match &c.conflict_target {
                None => None,
                Some(sqlparser::ast::ConflictTarget::Columns(cols)) => {
                    let cols: Vec<String> = cols.iter().map(|i| i.value.clone()).collect();
                    Some(resolve_conflict_columns(&meta, &cols)?)
                }
                Some(sqlparser::ast::ConflictTarget::OnConstraint(name)) => {
                    let iname = obj_name(name);
                    Some(resolve_conflict_constraint(&meta, &table, &iname)?)
                }
            },
            _ => None,
        };
        // The targeted conflict root's column list, resolved once per
        // statement (was a fresh `index_columns_of` allocation per row).
        let scope_cols: Option<Vec<String>> = conflict_scope
            .as_deref()
            .map(|root| meta.index_columns_of(root));

        // 批内重复键:后者胜(SQLite/MySQL 的 OR REPLACE 逐行语义)。位移
        // 扫描只看语句前的树,批内冲突随后被树级 unique 检查整条打报错;
        // 先按唯一键把「被更晚的行覆盖」的早行去掉,再走位移扫描。判重用
        // 编码字节,与树查找同源。
        if replace
            && (!indexed.is_empty() || !composite_uniques.is_empty() || !partial_uniques.is_empty())
            && new_docs.len() > 1
        {
            let mut last_at: std::collections::BTreeMap<(String, Vec<u8>), usize> =
                std::collections::BTreeMap::new();
            for (i, doc) in new_docs.iter().enumerate() {
                for col in &indexed {
                    if let Some(k) = doc
                        .get(col)
                        .filter(|v| !matches!(v, Value::Null))
                        .and_then(|v| encode::encode_to_vec(v).ok())
                    {
                        last_at.insert((col.clone(), k), i);
                    }
                }
                for spec in &composite_uniques {
                    if let Some(k) =
                        index_key_of_spec(doc, spec).and_then(|v| encode::encode_to_vec(&v).ok())
                    {
                        last_at.insert((spec.root_key.clone(), k), i);
                    }
                }
                for spec in &partial_uniques {
                    if !matches!(
                        eval_expr(spec.pred.as_ref().expect("pred set"), doc)?,
                        Value::Bool(true)
                    ) {
                        continue;
                    }
                    if let Some(k) =
                        index_key_of_spec(doc, spec).and_then(|v| encode::encode_to_vec(&v).ok())
                    {
                        last_at.insert((spec.root_key.clone(), k), i);
                    }
                }
            }
            let mut kept: Vec<Object> = Vec::with_capacity(new_docs.len());
            for (i, doc) in new_docs.iter().enumerate() {
                let shadowed = indexed.iter().any(|col| {
                    doc.get(col)
                        .filter(|v| !matches!(v, Value::Null))
                        .and_then(|v| encode::encode_to_vec(v).ok())
                        .and_then(|k| last_at.get(&(col.clone(), k)))
                        .is_some_and(|&j| j > i)
                }) || composite_uniques.iter().any(|spec| {
                    index_key_of_spec(doc, spec)
                        .and_then(|v| encode::encode_to_vec(&v).ok())
                        .and_then(|k| last_at.get(&(spec.root_key.clone(), k)))
                        .is_some_and(|&j| j > i)
                }) || partial_uniques.iter().any(|spec| {
                    matches!(
                        eval_expr(spec.pred.as_ref().expect("pred set"), doc),
                        Ok(Value::Bool(true))
                    ) && index_key_of_spec(doc, spec)
                        .and_then(|v| encode::encode_to_vec(&v).ok())
                        .and_then(|k| last_at.get(&(spec.root_key.clone(), k)))
                        .is_some_and(|&j| j > i)
                });
                if !shadowed {
                    kept.push(doc.clone());
                }
            }
            new_docs = kept;
        }

        // Rows displaced by REPLACE INTO / OR REPLACE.
        let mut displaced: Vec<u64> = Vec::new();
        let mut displaced_docs: Vec<Object> = Vec::new();
        if replace
            && (!indexed.is_empty() || !composite_uniques.is_empty() || !partial_uniques.is_empty())
        {
            for n in &new_docs {
                for col in &indexed {
                    let Some(v) = n.get(col) else { continue };
                    if matches!(v, Value::Null) {
                        continue;
                    }
                    if let Some(loc) = BTree::open(roots[col])
                        .get(&PageReader::current(&self.pager), &tx, v)
                        .map_err(|e| index_err(col, e))?
                    {
                        displaced.push(loc);
                    }
                }
                for spec in &composite_uniques {
                    let Some(k) = index_key_of_spec(n, spec) else {
                        continue;
                    };
                    if let Some(loc) = BTree::open(roots[&spec.root_key])
                        .get(&PageReader::current(&self.pager), &tx, &k)
                        .map_err(|e| index_err(&spec.root_key, e))?
                    {
                        displaced.push(loc);
                    }
                }
                // Predicate-scoped displacement: only a qualifying candidate
                // can conflict with the partial tree (a non-qualifying row
                // inserts beside it, displacing nothing).
                for spec in &partial_uniques {
                    if !matches!(
                        eval_expr(spec.pred.as_ref().expect("pred set"), n)?,
                        Value::Bool(true)
                    ) {
                        continue;
                    }
                    let Some(k) = index_key_of_spec(n, spec) else {
                        continue;
                    };
                    if let Some(loc) = BTree::open(roots[&spec.root_key])
                        .get(&PageReader::current(&self.pager), &tx, &k)
                        .map_err(|e| index_err(&spec.root_key, e))?
                    {
                        displaced.push(loc);
                    }
                }
            }
        } else if replace
            && indexed.is_empty()
            && composite_uniques.is_empty()
            && (meta.primary_key.is_some() || !meta.unique.is_empty())
            && !new_docs.is_empty()
        {
            // Legacy layout: the constraint columns predate trees, so no
            // probe can locate the rows OR REPLACE displaces — the rows
            // survived into the whole-set unique check and failed the very
            // keys they were replacing. Find them by content instead
            // (new tables always build trees; only legacy volumes land
            // here). Intra-batch duplicates keep the tree path's
            // "later row wins" semantics.
            let constraint_cols: Vec<&String> =
                meta.primary_key.iter().chain(meta.unique.iter()).collect();
            let key_of = |d: &Object| -> Option<Vec<Vec<u8>>> {
                let mut parts = Vec::with_capacity(constraint_cols.len());
                for c in &constraint_cols {
                    let v = d.get(*c)?;
                    // NULL keys never displace (trees hold no NULL keys).
                    if matches!(v, Value::Null) {
                        return None;
                    }
                    parts.push(encode::encode_to_vec(v).ok()?);
                }
                Some(parts)
            };
            let mut last_at: std::collections::BTreeMap<Vec<Vec<u8>>, usize> =
                std::collections::BTreeMap::new();
            for (i, d) in new_docs.iter().enumerate() {
                if let Some(k) = key_of(d) {
                    last_at.insert(k, i);
                }
            }
            let kept: Vec<Object> = new_docs
                .iter()
                .enumerate()
                .filter(|(i, d)| {
                    let Some(k) = key_of(d) else {
                        return true;
                    };
                    last_at.get(&k).is_some_and(|&j| j == *i)
                })
                .map(|(_, d)| d.clone())
                .collect();
            let old_pairs = self.table_pairs_cx(&table)?;
            for (loc, od) in old_pairs {
                if let Some(k) = key_of(&od) {
                    if kept.iter().any(|n| key_of(n).as_deref() == Some(&k[..])) {
                        displaced.push(loc);
                    }
                }
            }
            new_docs = kept;
        }
        if !displaced.is_empty() {
            // One old row can be hit through several constraints at once
            // (its PK by one new row and its UNIQUE key by another — or one
            // new row matching both). Duplicates would double-count in the
            // FK parent-side scan (`count_in(removed)`) and un-defuse a
            // lost key that surviving rows still carry, rejecting a legal
            // replace; remove_many would also see the locator twice.
            displaced.sort_unstable();
            displaced.dedup();
            let affected: std::collections::BTreeSet<u32> = displaced
                .iter()
                .map(|l| crate::heap::unpack_loc(*l).0)
                .collect();
            let mut before = Vec::new();
            for pid in affected {
                before.extend(heap.page_docs(&PageReader::current(&self.pager), &tx, pid)?);
            }
            // Rows displaced by OR REPLACE are parent-side deletions: their
            // referenced keys must not vanish silently (same contract as
            // DELETE). New rows that keep the same key are replacements.
            displaced_docs = displaced
                .iter()
                .filter_map(|loc| {
                    before
                        .iter()
                        .find(|(l, _)| l == loc)
                        .map(|(_, d)| d.clone())
                })
                .collect();
            // Self-referential FKs must see the statement's FINAL state —
            // pre-state minus the displaced rows plus the placed rows. The
            // committed-state fallback would count references that vanish
            // together with their parent and reject a legal replace.
            let displaced_set: std::collections::BTreeSet<u64> =
                displaced.iter().copied().collect();
            let mut final_rows: Vec<Object> = self
                .table_pairs_cx(&table)?
                .into_iter()
                .filter(|(l, _)| !displaced_set.contains(l))
                .map(|(_, d)| d)
                .collect();
            final_rows.extend(new_docs.iter().cloned());
            self.check_fk_parent_delete_skipping(
                &table,
                &displaced_docs,
                &new_docs,
                &[],
                Some(&final_rows),
            )?;
            // OR REPLACE displacement is a real row deletion: DELETE
            // triggers fire for it (SQLite semantics), BEFORE before the
            // removal lands, AFTER once the row is gone.
            for d in &displaced_docs {
                self.fire_row_triggers(&table, true, TriggerFireEvent::Delete, Some(d), None)?;
            }
            let moves = heap.remove_many(&self.pager, &mut tx, &displaced)?;
            for loc in &displaced {
                let Some((_, doc)) = before.iter().find(|(l, _)| l == loc) else {
                    continue;
                };
                reindex_remove(&self.pager, &mut tx, &idx_specs, &mut roots, doc, *loc)?;
            }
            for (old_l, new_l) in &moves {
                reindex_repoint(
                    &self.pager,
                    &mut tx,
                    &idx_specs,
                    &mut roots,
                    &before,
                    *old_l,
                    *new_l,
                )?;
            }
            for d in &displaced_docs {
                self.fire_row_triggers(&table, false, TriggerFireEvent::Delete, Some(d), None)?;
            }
        }

        // Append the surviving rows and maintain the trees. Tree inserts
        // with unique=true ARE the duplicate check (within-batch duplicates
        // included: staged pages are visible to tree reads).
        let mut placed: Vec<(u64, Object)> = Vec::with_capacity(new_docs.len());
        let mut insert_failed: Option<SqlError> = None;
        // Upsert-UPDATE state: rows updated in place during this statement
        // (locator, pre-image, post-image) plus the RETURNING projection in
        // input order (inserted and updated rows alike).
        let mut updated: Vec<(u64, Object, Object)> = Vec::new();
        // heap.replace can MOVE a row past its page budget (tombstone +
        // append at the heap tail): a later touch of the same logical row
        // then finds it at the NEW locator, and the locator-keyed collapse
        // below would keep two entries — leaking the intermediate image
        // into the FK/unique checks and RETURNING. Map each relocated
        // locator back to the row's first-seen locator.
        let mut origin_of: std::collections::BTreeMap<u64, u64> = std::collections::BTreeMap::new();
        let mut ret_docs: Vec<Object> = Vec::with_capacity(new_docs.len());
        let mut du_outer = OuterFrom::new();
        du_outer.insert(table.clone());
        // Upsert arms fire UPDATE triggers with the arm's assigned columns
        // (UPDATE OF gating applies to them too).
        let du_changed: Option<Vec<String>> = if do_update {
            Some(
                du_assignments
                    .iter()
                    .filter_map(|a| match &a.target {
                        sqlparser::ast::AssignmentTarget::ColumnName(c) => Some(obj_name(c)),
                        _ => None,
                    })
                    .collect(),
            )
        } else {
            None
        };
        'outer: for doc in new_docs.into_iter() {
            // BEFORE INSERT fires before conflict resolution, per candidate
            // row (SQLite ordering: triggers, then constraints).
            self.fire_row_triggers(&table, true, TriggerFireEvent::Insert, None, Some(&doc))?;
            // Conflict probe shared by skip (DO NOTHING / OR IGNORE) and
            // upsert (DO UPDATE): locate the conflicting row's locator.
            // Untargeted = any unique constraint (first hit wins, PK probes
            // first); targeted = the named constraint's tree only — other
            // keys' conflicts still error at the insert below.
            let conflict_loc: Option<u64> = if !do_nothing && !do_update {
                None
            } else {
                match &conflict_scope {
                    None => {
                        let mut hit = None;
                        for col in &indexed {
                            let Some(v) = doc.get(col) else { continue };
                            if matches!(v, Value::Null) {
                                continue;
                            }
                            if let Some(loc) = BTree::open(roots[col])
                                .get(&PageReader::current(&self.pager), &tx, v)
                                .map_err(|e| index_err(col, e))?
                            {
                                hit = Some(loc);
                                break;
                            }
                        }
                        // Composite unique indexes resolve "any unique key"
                        // for the undirected policies too.
                        if hit.is_none() {
                            for spec in &composite_uniques {
                                let Some(k) = index_key_of_spec(&doc, spec) else {
                                    continue;
                                };
                                if let Some(loc) = BTree::open(roots[&spec.root_key])
                                    .get(&PageReader::current(&self.pager), &tx, &k)
                                    .map_err(|e| index_err(&spec.root_key, e))?
                                {
                                    hit = Some(loc);
                                    break;
                                }
                            }
                        }
                        // Predicate-scoped conflicts: only a qualifying
                        // candidate probes the partial tree.
                        if hit.is_none() {
                            for spec in &partial_uniques {
                                if !matches!(
                                    eval_expr(spec.pred.as_ref().expect("pred set"), &doc)?,
                                    Value::Bool(true)
                                ) {
                                    continue;
                                }
                                let Some(k) = index_key_of_spec(&doc, spec) else {
                                    continue;
                                };
                                if let Some(loc) = BTree::open(roots[&spec.root_key])
                                    .get(&PageReader::current(&self.pager), &tx, &k)
                                    .map_err(|e| index_err(&spec.root_key, e))?
                                {
                                    hit = Some(loc);
                                    break;
                                }
                            }
                        }
                        hit
                    }
                    Some(root) => {
                        if let Some(key) = index_key_of(&doc, scope_cols.as_deref().unwrap()) {
                            BTree::open(roots[root.as_str()])
                                .get(&PageReader::current(&self.pager), &tx, &key)
                                .map_err(|e| index_err(root, e))?
                        } else {
                            None
                        }
                    }
                }
            };
            if let Some(loc) = conflict_loc {
                if do_nothing {
                    continue 'outer; // conflict: skip this row
                }
                // ---- upsert arm: update the conflicting row in place ----
                // PostgreSQL semantics: WHERE and SET see the OLD row for
                // bare column references; EXCLUDED.col (and MySQL's
                // VALUES(col)) carry the candidate row's values.
                let page = crate::heap::unpack_loc(loc).0;
                let before = heap.page_docs(&PageReader::current(&self.pager), &tx, page)?;
                let Some((_, existing)) = before.iter().find(|(l, _)| *l == loc) else {
                    self.pager.abort_tx(tx)?;
                    return err(format!(
                        "upsert: conflicting row vanished mid-statement (locator {loc:#x})"
                    ));
                };
                if let Some(sel) = &du_selection {
                    let mut sel = sel.clone();
                    subst_upsert_proposed(&mut sel, &doc)?;
                    if !self.matches_cx_in(&Some(sel), existing, &du_outer)? {
                        // WHERE filtered the update out: the row keeps its
                        // image and the candidate is not inserted (PG).
                        continue 'outer;
                    }
                }
                let mut subst = du_assignments.clone();
                for a in &mut subst {
                    subst_upsert_proposed(&mut a.value, &doc)?;
                }
                let new_vals = match self.eval_assignments_cx(&subst, existing, &du_outer) {
                    Ok(v) => v,
                    Err(e) => {
                        self.pager.abort_tx(tx)?;
                        return Err(e);
                    }
                };
                let mut merged = existing.clone();
                for (col_name, v) in new_vals {
                    merged.insert(col_name, v);
                }
                if let Err(e) = meta.check(&merged) {
                    self.pager.abort_tx(tx)?;
                    return Err(e);
                }
                // Two-phase index maintenance for THIS row: drop its old
                // entries, write the new image, repoint the in-page repack's
                // moved survivors, insert the new entries. Later batch rows
                // see the updated trees (sequential upsert semantics, as in
                // MySQL; PostgreSQL would reject a second touch instead).
                if let Err(e) =
                    reindex_remove(&self.pager, &mut tx, &idx_specs, &mut roots, existing, loc)
                {
                    self.pager.abort_tx(tx)?;
                    return Err(e);
                }
                let out = match heap.replace(&self.pager, &mut tx, loc, &merged) {
                    Ok(o) => o,
                    Err(e) => {
                        self.pager.abort_tx(tx)?;
                        return Err(e.into());
                    }
                };
                for (old_l, new_l) in &out.moved {
                    if *old_l == loc {
                        continue; // this row's own slot: entries removed above
                    }
                    if let Err(e) = reindex_repoint(
                        &self.pager,
                        &mut tx,
                        &idx_specs,
                        &mut roots,
                        &before,
                        *old_l,
                        *new_l,
                    ) {
                        self.pager.abort_tx(tx)?;
                        return Err(e);
                    }
                }
                if let Err(e) = reindex_insert(
                    &self.pager,
                    &mut tx,
                    &idx_specs,
                    &mut roots,
                    &merged,
                    out.placed,
                ) {
                    self.pager.abort_tx(tx)?;
                    return Err(e);
                }
                if out.placed != loc {
                    let origin = origin_of.get(&loc).copied().unwrap_or(loc);
                    origin_of.insert(out.placed, origin);
                }
                let collapse_key = origin_of.get(&loc).copied().unwrap_or(loc);
                updated.push((collapse_key, existing.clone(), merged.clone()));
                ret_docs.push(merged);
                // An upsert that updated in place fires the UPDATE triggers
                // (SQLite/PostgreSQL semantics), not the INSERT ones.
                let changed = du_changed.as_deref();
                let fired_new = ret_docs.last().expect("just pushed");
                self.fire_row_triggers(
                    &table,
                    true,
                    TriggerFireEvent::Update(changed),
                    Some(existing),
                    Some(fired_new),
                )?;
                self.fire_row_triggers(
                    &table,
                    false,
                    TriggerFireEvent::Update(changed),
                    Some(existing),
                    Some(fired_new),
                )?;
                continue 'outer;
            }
            let loc = match heap.insert(&self.pager, &mut tx, &doc) {
                Ok(l) => l,
                Err(e) => {
                    self.pager.abort_tx(tx)?;
                    return Err(e.into());
                }
            };
            for col in &indexed {
                let Some(v) = doc.get(col) else { continue };
                if matches!(v, Value::Null) {
                    continue;
                }
                let root = roots[col];
                let mut tree = BTree::open(root);
                if let Err(e) = tree.insert(&self.pager, &mut tx, v.clone(), loc, true) {
                    insert_failed = Some(index_err(col, e));
                    break 'outer;
                }
                if tree.root != root {
                    roots.insert(col.clone(), tree.root);
                }
            }
            // Non-constraint CREATE INDEX trees. Composite unique trees
            // (CREATE UNIQUE INDEX over several columns) enforce their
            // duplicates right here, at tree-insert time.
            for spec in &idx_specs {
                if indexed.contains(&spec.root_key) {
                    continue;
                }
                // Partial index: only qualifying rows enter the tree.
                if let Some(p) = &spec.pred {
                    if !matches!(eval_expr(p, &doc)?, Value::Bool(true)) {
                        continue;
                    }
                }
                let Some(v) = index_key_of_spec(&doc, spec) else {
                    continue;
                };
                let root = roots[&spec.root_key];
                let mut tree = BTree::open(root);
                tree.insert(&self.pager, &mut tx, v, loc, spec.unique)
                    .map_err(|e| index_err(&spec.root_key, e))?;
                if tree.root != root {
                    roots.insert(spec.root_key.clone(), tree.root);
                }
            }
            if insert.returning.is_some() {
                ret_docs.push(doc.clone());
            }
            placed.push((loc, doc));
            // AFTER INSERT: the row (and its tree entries) is in place.
            self.fire_row_triggers(
                &table,
                false,
                TriggerFireEvent::Insert,
                None,
                Some(&placed.last().expect("just pushed").1),
            )?;
        }
        if let Some(e) = insert_failed {
            self.pager.abort_tx(tx)?;
            return Err(e);
        }
        // Child-side FK over the FINAL row set. ON CONFLICT DO NOTHING /
        // OR IGNORE filter rows out after the batch was built — validating
        // against the pre-filter batch let a skipped parent candidate
        // legitimize a child that lands alone (dangling reference).
        // Self-references resolve against the placed batch itself;
        // statement-internal references count, including forward ones.
        // Upsert-UPDATE rows join the final set: their new images must
        // reference existing parents like any inserted row.
        // A multi-row batch may touch the SAME row twice (sequential
        // upsert semantics: later rows can route onto an earlier row
        // through either unique key). Every touch pushed (old, merged),
        // but only the FIRST old image and the LAST merged image of a
        // locator are real — intermediate images were overwritten and do
        // not exist in the final table. Feeding them into the FK/unique
        // checks below could validate (or reject) against keys no
        // surviving row carries, e.g. a parent key vacated by touch one
        // and re-supply-shaped by touch two's intermediate image.
        if updated.len() > 1 {
            let mut collapsed: std::collections::BTreeMap<u64, (usize, usize)> =
                std::collections::BTreeMap::new();
            for (i, (loc, _, _)) in updated.iter().enumerate() {
                match collapsed.get_mut(loc) {
                    Some((_, last)) => *last = i,
                    None => {
                        collapsed.insert(*loc, (i, i));
                    }
                }
            }
            if collapsed.len() < updated.len() {
                updated = collapsed
                    .into_iter()
                    .map(|(loc, (first, last))| {
                        (loc, updated[first].1.clone(), updated[last].2.clone())
                    })
                    .collect();
            }
        }
        if !meta.foreign_keys.is_empty() && (!placed.is_empty() || !updated.is_empty()) {
            let mut final_docs: Vec<Object> = placed.iter().map(|(_, d)| d.clone()).collect();
            final_docs.extend(updated.iter().map(|(_, _, n)| n.clone()));
            // Self-referential FKs need the statement's COMPLETE final state
            // (pre-state minus the replaced old images plus these images):
            // the pre-state heap still carries keys an upsert-UPDATE vacates
            // (`VALUES (1,1) ON CONFLICT (id) DO UPDATE SET id = 3, p = 1`).
            // Plain INSERT keeps `None`: nothing is replaced, so same_batch
            // plus the pre-state heap is already the final state.
            let self_final: Option<Vec<Object>> =
                if !updated.is_empty() && meta.foreign_keys.iter().any(|(_, rt, _)| rt == &table) {
                    let old_keys: std::collections::BTreeSet<Vec<u8>> =
                        updated.iter().map(|(_, o, _)| object_key(o)).collect();
                    let mut final_state = self.table_docs_cx(&table)?;
                    final_state.retain(|d| !old_keys.contains(&object_key(d)));
                    final_state.extend(final_docs.iter().cloned());
                    Some(final_state)
                } else {
                    None
                };
            for doc in &final_docs {
                // A big INSERT ... SELECT walks this per placed row: keep the
                // statement timeout armed like every other row loop.
                self.stmt_deadline.check()?;
                if let Err(e) =
                    self.check_fks_in(&table, &meta, doc, &final_docs, self_final.as_deref())
                {
                    self.pager.abort_tx(tx)?;
                    return Err(e);
                }
            }
        }
        // Parent-side FK for upsert-updated rows: the old image's key
        // values disappear exactly like a DELETE's and must not be
        // referenced by children (new images already checked above). This
        // runs OUTSIDE the target's own-FK gate — the referencing children
        // belong to OTHER tables, which do not care whether the target
        // declares FKs of its own.
        if !updated.is_empty() {
            let has_self_fk = meta.foreign_keys.iter().any(|(_, rt, _)| rt == &table);
            let self_final: Option<Vec<Object>> = if has_self_fk {
                let old_keys: std::collections::BTreeSet<Vec<u8>> =
                    updated.iter().map(|(_, o, _)| object_key(o)).collect();
                let mut final_state = self.table_docs_cx(&table)?;
                final_state.retain(|d| !old_keys.contains(&object_key(d)));
                final_state.extend(updated.iter().map(|(_, _, n)| n.clone()));
                final_state.extend(placed.iter().map(|(_, d)| d.clone()));
                Some(final_state)
            } else {
                None
            };
            let olds: Vec<Object> = updated.iter().map(|(_, o, _)| o.clone()).collect();
            // `news` is the "keys that still exist after this statement"
            // set: rows PLACED by the same batch can re-supply a key the
            // updated rows vacated (INSERT INTO p VALUES (1),(1) ON CONFLICT
            // (id) DO UPDATE SET id = 99 — the second (1) no longer conflicts
            // and lands as a plain insert). Counting only the updated images
            // false-errored children referencing a key that is still present
            // through the placed row.
            let mut news: Vec<Object> = updated.iter().map(|(_, _, n)| n.clone()).collect();
            news.extend(placed.iter().map(|(_, d)| d.clone()));
            if let Err(e) = self.check_fk_parent_delete_skipping(
                &table,
                &olds,
                &news,
                &[],
                self_final.as_deref(),
            ) {
                self.pager.abort_tx(tx)?;
                return Err(e);
            }
        }
        // Legacy files whose constraint columns predate trees keep the
        // whole-set duplicate check; tables with no constraints skip it.
        // ANY untreed constraint column triggers it (same gate as the
        // UPDATE fast path): an all-or-nothing gate used to skip the check
        // whenever a sibling column had a tree, silently admitting
        // duplicates on the treeless one.
        let has_constraints = meta.primary_key.is_some() || !meta.unique.is_empty();
        let all_treed = meta
            .primary_key
            .iter()
            .chain(meta.unique.iter())
            .all(|c| indexed.contains(c));
        if has_constraints && !all_treed && (!placed.is_empty() || !updated.is_empty()) {
            // A failed scan must abort, not silently skip the unique check.
            let mut combined = match (Heap {
                pages: meta.pages.clone(),
                overflow_free: meta.overflow_free.clone(),
                dropped: Vec::new(),
            }
            .scan(&PageReader::current(&self.pager)))
            {
                Ok(docs) => docs,
                Err(e) => {
                    self.pager.abort_tx(tx)?;
                    return Err(e.into());
                }
            };
            if !updated.is_empty() {
                // The scan sees the PRE-statement table: updated rows carry
                // their old images here. Swap them for the new images
                // (encoding-compare removes exactly the one old row — its
                // unique-key values are distinct per row, so byte-identical
                // duplicates of an old image cannot exist).
                let olds: std::collections::HashSet<Vec<u8>> = updated
                    .iter()
                    .filter_map(|(_, o, _)| encode::encode_to_vec(&Value::Object(o.clone())).ok())
                    .collect();
                combined.retain(|d| {
                    !encode::encode_to_vec(&Value::Object(d.clone()))
                        .map(|k| olds.contains(&k))
                        .unwrap_or(false)
                });
                combined.extend(updated.iter().map(|(_, _, n)| n.clone()));
            }
            if !displaced_docs.is_empty() {
                // The no-tx scan also still sees OR REPLACE's displaced rows
                // (their deletions are staged in THIS tx): keep them and the
                // displaced old image pairs with its own replacement, making
                // check_unique false-error on the very keys REPLACE displaced.
                // Same encoding-compare argument as `updated`: byte-identical
                // duplicates of a displaced image cannot coexist in a table
                // that has constraints at all.
                let gone: std::collections::HashSet<Vec<u8>> = displaced_docs
                    .iter()
                    .filter_map(|d| encode::encode_to_vec(&Value::Object(d.clone())).ok())
                    .collect();
                combined.retain(|d| {
                    !encode::encode_to_vec(&Value::Object(d.clone()))
                        .map(|k| gone.contains(&k))
                        .unwrap_or(false)
                });
            }
            combined.extend(placed.iter().map(|(_, d)| d.clone()));
            if let Err(e) = meta.check_unique(&combined) {
                self.pager.abort_tx(tx)?;
                return Err(e);
            }
        }
        // Affected = inserted + upsert-updated rows (PostgreSQL reports the
        // union the same way).
        let count = (placed.len() + updated.len()) as u64;
        // Emptied heap pages are released inside this transaction so later
        // statements reuse them instead of growing the file.
        for p in std::mem::take(&mut heap.dropped) {
            self.pager.free_page(&mut tx, p)?;
        }
        let dotted_now = self.note_dotted_fields(
            &table,
            placed
                .iter()
                .map(|(_, d)| d)
                .chain(updated.iter().map(|(_, _, new)| new)),
        );
        if dotted_now
            || heap.pages != meta.pages
            || roots != meta.index_roots
            || heap.overflow_free != meta.overflow_free
        {
            self.sync_table_layout(
                &mut tx,
                &table,
                heap.pages.clone(),
                roots,
                heap.overflow_free.clone(),
            )?;
        }
        self.commit_pager_tx(tx)?;
        // AUTOINCREMENT counter: never regress, follow explicit max.
        if let Some(col) = &meta.autoinc {
            // The upsert-UPDATE arm can raise the live max past every placed
            // id (SET id = <higher>) — invalidate like the UPDATE fast path,
            // or the stale cache keeps issuing already-taken ids and every
            // subsequent autoincrement INSERT fails on the unique tree.
            let du_touched = do_update
                && du_assignments.iter().any(|a| {
                    matches!(&a.target, sqlparser::ast::AssignmentTarget::ColumnName(c) if obj_name(c) == *col)
                });
            if du_touched {
                self.autoinc_cache.remove(&table);
            } else {
                let used = placed
                    .iter()
                    .filter_map(|(_, d)| match d.get(col) {
                        Some(Value::Int(i)) => Some((*i).saturating_add(1)),
                        _ => None,
                    })
                    .max()
                    .unwrap_or(next_autoinc)
                    .max(next_autoinc);
                self.autoinc_cache.insert(table.clone(), used);
            }
            // Max id this statement actually placed (explicit values count;
            // an INSERT that placed no rows leaves the previous value).
            self.last_insert_id = placed
                .iter()
                .filter_map(|(_, d)| d.get(col))
                .filter_map(|v| v.as_i64())
                .max();
        }
        if let Some(ret) = &insert.returning {
            return project_returning(ret, &ret_docs);
        }
        Ok(ExecOutcome::Affected(count))
    }

    /// MERGE INTO target USING source ON (cond) — Oracle/SQL-standard
    /// upsert over arbitrary match conditions (ON CONFLICT only covers
    /// unique-key conflicts). Execution: load the source rows, evaluate ON
    /// against every (target, source) pair, then apply per pair —
    /// WHEN MATCHED THEN UPDATE SET …, WHEN NOT MATCHED THEN INSERT ….
    /// Determinism: the merge walks target storage order, so replication
    /// replays converge. Two source rows matching one target row is an
    /// error (Oracle ORA-30926 semantics).
    fn exec_merge(&mut self, merge: sqlparser::ast::Merge) -> Result<ExecOutcome> {
        use sqlparser::ast::{MergeAction, MergeClauseKind};

        // Target: a simple table.
        let sqlparser::ast::TableFactor::Table {
            name: tname_ident,
            alias: talias,
            ..
        } = &merge.table
        else {
            return err("MERGE target must be a simple table");
        };
        let tname = obj_name(tname_ident);
        let tkey = talias
            .as_ref()
            .map(|a| a.name.value.clone())
            .unwrap_or_else(|| tname.clone());
        let Some(meta) = self.tables.get(&tname).cloned() else {
            return err(format!("table {tname} does not exist"));
        };
        // Views have no storage: every other write shape rejects them here
        // (exec_insert/update/delete, TRUNCATE, ALTER, CREATE INDEX);
        // without this a MERGE inserts into the view's private empty
        // heap — invisible rows and a dump that cannot replay.
        ensure_not_view("MERGE into", &tname, &meta)?;

        // Exactly one WHEN MATCHED (UPDATE) and one WHEN NOT MATCHED
        // (INSERT) clause; per-clause predicates, BY SOURCE, DELETE WHERE
        // and UPDATE WHERE are rejected loudly (v1 surface).
        let mut matched_upd: Option<Vec<sqlparser::ast::Assignment>> = None;
        let mut not_matched_ins: Option<(Vec<String>, Vec<SqlExpr>)> = None;
        for cl in &merge.clauses {
            if cl.predicate.is_some() {
                return err("MERGE WHEN … AND <predicate> is not supported");
            }
            match cl.clause_kind {
                MergeClauseKind::NotMatchedBySource => {
                    return err("WHEN … BY SOURCE is not supported");
                }
                MergeClauseKind::Matched | MergeClauseKind::NotMatchedByTarget => {
                    if matched_upd.is_some() {
                        return err("duplicate WHEN MATCHED clause");
                    }
                    let MergeAction::Update(upd) = &cl.action else {
                        return err("WHEN MATCHED supports only UPDATE \
                             (DELETE/other actions are not supported)");
                    };
                    if upd.update_predicate.is_some() || upd.delete_predicate.is_some() {
                        return err("MERGE UPDATE WHERE/DELETE WHERE is not supported");
                    }
                    matched_upd = Some(upd.assignments.clone());
                }
                MergeClauseKind::NotMatched => {
                    if not_matched_ins.is_some() {
                        return err("duplicate WHEN NOT MATCHED clause");
                    }
                    let MergeAction::Insert(ins) = &cl.action else {
                        return err("WHEN NOT MATCHED THEN UPDATE is not supported");
                    };
                    if ins.insert_predicate.is_some() {
                        return err("MERGE INSERT predicate is not supported");
                    }
                    let sqlparser::ast::MergeInsertKind::Values(values) = &ins.kind else {
                        return err("MERGE INSERT ROW is not supported");
                    };
                    if values.rows.len() != 1 {
                        return err("MERGE INSERT VALUES must have exactly one row");
                    }
                    let mut cols = Vec::new();
                    for c in &ins.columns {
                        let col = obj_name(c);
                        if !meta.columns.contains(&col) {
                            return err(format!("column {col} does not exist"));
                        }
                        if cols.contains(&col) {
                            return err(format!("duplicate column name: {col}"));
                        }
                        cols.push(col);
                    }
                    if cols.is_empty() {
                        // No column list: the VALUES map onto the table's
                        // declared column order (the INSERT rule). Zipping
                        // against the empty list used to silently drop the
                        // values and append an all-NULL row.
                        if values.rows[0].content.len() != meta.columns.len() {
                            return err(format!(
                                "MERGE INSERT has {} values but the table has {} \
                                 columns; use an explicit column list",
                                values.rows[0].content.len(),
                                meta.columns.len()
                            ));
                        }
                        cols = meta.columns.clone();
                    }
                    let mut exprs = Vec::new();
                    for row in &values.rows {
                        for e in row.content.iter() {
                            exprs.push(e.clone());
                        }
                    }
                    not_matched_ins = Some((cols, exprs));
                }
            }
        }
        // Source rows: plain table or derived subquery (no CTE access in
        // v1 — the MERGE statement is not a SELECT statement).
        let (_, src_alias, src_rows) = self
            .read_cx()
            .load_table_factor(&merge.source, &Ctes::new())?;
        // Source-column qualifier: the alias for a plain table (falling back
        // to its name), the derived-table alias when given, else `source`.
        // Losing the derived alias here would make `feed.col` resolve to the
        // target's bare column via fallback and every source row would match
        // the same target row (spurious "multiple source rows matched").
        let skey = match &merge.source {
            sqlparser::ast::TableFactor::Table { name, alias, .. } => alias
                .as_ref()
                .map(|a| a.name.value.clone())
                .unwrap_or_else(|| obj_name(name)),
            _ => src_alias
                .filter(|a| !a.is_empty())
                .unwrap_or_else(|| "source".to_string()),
        };
        // Same alias for source and target: the join row keys collide and
        // the TARGET qualifier overwrites the source's, so `ON x.id =
        // x.id` is trivially true and SET reads the target's own pre-image
        // — every target row matches every source row (mass wrong updates
        // or a spurious "cannot update the same row twice"). MySQL and SQL
        // Server reject the shape; so do we.
        if skey == tkey {
            return err(format!(
                "MERGE source and target aliases must differ (both are '{skey}')"
            ));
        }

        // Target rows with locators (storage order — deterministic replay
        // on replication peers).
        let tdocs = self.table_pairs_cx(&tname)?;

        // ROWNUM has no row stream to number in MERGE's ON / arms: it reads
        // NULL and `NULL <= n` is TRUE under the documented total order, so
        // every pair would silently match. Fail loudly (subquery sources may
        // still use ROWNUM — that stream is their own).
        if refs_rownum(&merge.on)
            || matched_upd
                .as_ref()
                .is_some_and(|asg| asg.iter().any(|a| refs_rownum(&a.value)))
            || not_matched_ins
                .as_ref()
                .is_some_and(|(_, exprs)| exprs.iter().any(refs_rownum))
        {
            return err("ROWNUM is not supported in MERGE ON / WHEN arms");
        }

        // Pair every source row with its matching target row: ON evaluates
        // on the merged namespace (target columns first; source columns are
        // additionally available qualified as `skey.col`).
        let mut pairs: Vec<(u64, Object, usize)> = Vec::new(); // (t_loc, t_doc, s_idx)
        let mut s_consumed = vec![false; src_rows.len()];
        for (t_loc, t_doc) in &tdocs {
            let mut hits = 0;
            for (si, s_doc) in src_rows.iter().enumerate() {
                // O(target×source) pairing must stay interruptible under
                // the statement timeout like every other row loop.
                self.stmt_deadline.check()?;
                let row = Self::merge_join_row(t_doc, tkey.as_str(), s_doc, &skey);
                if matches!(eval_expr(&merge.on, &row)?, Value::Bool(true)) {
                    hits += 1;
                    if hits > 1 && matched_upd.is_some() {
                        // Only an UPDATE arm makes a second match a conflict
                        // (it would update the row twice). With no WHEN
                        // MATCHED clause every source row that matched is
                        // simply consumed — nothing to update.
                        return err(format!(
                            "MERGE cannot update the same row of {tname} twice \
                             (multiple source rows matched)"
                        ));
                    }
                    pairs.push((*t_loc, t_doc.clone(), si));
                    s_consumed[si] = true;
                }
            }
        }

        // Build the matched updates: assignments evaluate on the merged
        // namespace (target columns and qualified source columns).
        let mut updates: Vec<(u64, Object, Object)> = Vec::new();
        // An explicit autoinc id (INSERT arm) or an assignment touching the
        // autoinc column (UPDATE arm) invalidates the session's high-water
        // cache: `merge_next_autoinc` only tracks engine-generated ids, and
        // keeping the stale cache made the next plain INSERT collide with
        // the explicit value (UNIQUE constraint failed).
        let mut autoinc_mutated = false;
        if let Some(assignments) = &matched_upd {
            for (t_loc, t_doc, si) in &pairs {
                let s_doc = &src_rows[*si];
                let row = Self::merge_join_row(t_doc, tkey.as_str(), s_doc, &skey);
                let mut new_doc = t_doc.clone();
                for a in assignments {
                    let col = match &a.target {
                        sqlparser::ast::AssignmentTarget::ColumnName(c) => obj_name(c),
                        other => {
                            return err(format!("unsupported MERGE assignment target: {other}"))
                        }
                    };
                    if Some(&col) == meta.autoinc.as_ref() {
                        autoinc_mutated = true;
                    }
                    let v = eval_expr(&a.value, &row)?;
                    new_doc.insert(col, v);
                }
                // Same constraint gate as UPDATE: NOT NULL/CHECK and the
                // child-side FK must hold for the merged image (FK check
                // runs after the batch is collected — self/intra-statement
                // references count as parent candidates).
                meta.check(&new_doc)?;
                updates.push((*t_loc, t_doc.clone(), new_doc));
            }
            // NOTE: both FK directions are validated against the statement's
            // FINAL state after the INSERT arm below — checking here (against
            // pre-statement data) let a MERGE land a dangling self-reference
            // (UPDATE removes the parent the INSERT arm still points at) and
            // wrongly reject parents the other arm had just created.
        }

        // One pager transaction for heap pages and index trees alike.
        let mut heap = Heap {
            pages: meta.pages.clone(),
            overflow_free: meta.overflow_free.clone(),
            dropped: Vec::new(),
        };
        let mut roots = meta.index_roots.clone();
        let idx_specs = idx_specs(&meta, &roots);
        let mut tx = self.pager.begin_tx();

        // Pass 1: drop every matched row's old index entries before any
        // replacement lands (a multi-row key shift must not collide with a
        // not-yet-replaced row's still-present old key).
        for (loc, old_doc, _) in &updates {
            if let Err(e) =
                reindex_remove(&self.pager, &mut tx, &idx_specs, &mut roots, old_doc, *loc)
            {
                self.pager.abort_tx(tx)?;
                return Err(e);
            }
        }
        // Pass 2: replace the matched rows, following in-page repacks.
        for i in 0..updates.len() {
            let (loc, new_doc) = (updates[i].0, updates[i].2.clone());
            let page = crate::heap::unpack_loc(loc).0;
            let before = heap.page_docs(&PageReader::current(&self.pager), &tx, page)?;
            // Pre-fixup pending locators; see the identical logic in the
            // UPDATE fast path.
            let pending_locs: Vec<u64> = updates.iter().skip(i + 1).map(|(l, _, _)| *l).collect();
            let out = heap.replace(&self.pager, &mut tx, loc, &new_doc)?;
            if !out.moved.is_empty() {
                for pending in updates.iter_mut().skip(i + 1) {
                    if let Some((_, new_l)) = out.moved.iter().find(|(old, _)| *old == pending.0) {
                        pending.0 = *new_l;
                    }
                }
            }
            for (old_l, new_l) in &out.moved {
                // Same rule as the UPDATE fast path: pending rows were
                // stripped in pass 1 and get their new entries in their own
                // iteration; already-processed rows must follow the repack.
                if *old_l == loc || pending_locs.contains(old_l) {
                    continue;
                }
                if let Err(e) = reindex_repoint(
                    &self.pager,
                    &mut tx,
                    &idx_specs,
                    &mut roots,
                    &before,
                    *old_l,
                    *new_l,
                ) {
                    self.pager.abort_tx(tx)?;
                    return Err(e);
                }
            }
            if let Err(e) = reindex_insert(
                &self.pager,
                &mut tx,
                &idx_specs,
                &mut roots,
                &new_doc,
                out.placed,
            ) {
                self.pager.abort_tx(tx)?;
                return Err(e);
            }
        }
        // WHEN NOT MATCHED THEN INSERT: appended rows go through the same
        // tree inserts (unique trees reject duplicates) and FK checks as
        // plain inserts. Only source rows that matched no target row are
        // inserted.
        let mut inserted = 0usize;
        let mut inserted_docs: Vec<Object> = Vec::new();
        // 本语句插入行的最大自增 id 与下一个自增值:提交成功后才写进会话
        // 状态(与 INSERT 路径对齐),失败回滚不留幻影 SCOPE_IDENTITY。
        let mut merge_last_id: Option<i64> = None;
        let mut merge_next_autoinc = if meta.autoinc.is_some() {
            self.autoinc_next_for(&tname, &meta)?
        } else {
            0
        };
        if let Some((cols, exprs)) = &not_matched_ins {
            // Auto-generated GUIDs are random: letting each node generate its
            // own would silently diverge replicas (the INSERT path rewrites
            // the statement with the values; a MERGE rewrite would have to
            // rewrite the whole branching statement).
            if meta.autoguid.is_some() {
                return err(
                    "MERGE ... WHEN NOT MATCHED THEN INSERT is not supported on \
                     tables with an auto-generated GUID column",
                );
            }
            let mut next_autoinc = merge_next_autoinc;
            let mut default_exprs: std::collections::HashMap<String, SqlExpr> =
                std::collections::HashMap::new();
            for (si, s_doc) in src_rows.iter().enumerate() {
                if s_consumed[si] {
                    continue;
                }
                self.stmt_deadline.check()?;
                let mut doc = Object::new();
                for (c, e) in cols.iter().zip(exprs.iter()) {
                    let v = eval_expr(e, s_doc)?;
                    doc.insert(c.clone(), v);
                }
                // Same generation rules as INSERT: missing/NULL AUTOINCREMENT
                // gets max+1, declared DEFAULTs fill omitted columns.
                if let Some(col) = &meta.autoinc {
                    match doc.get(col) {
                        Some(Value::Null) | None => {
                            doc.insert(col.clone(), Value::Int(next_autoinc));
                            next_autoinc = next_autoinc.saturating_add(1);
                        }
                        // Explicit id: advance the in-statement watermark so a
                        // later source row that omits the id doesn't reuse it
                        // (same rule as INSERT); the engine didn't allocate
                        // it, so the cache is still dropped at commit and the
                        // next statement recomputes max+1.
                        Some(Value::Int(v)) => {
                            next_autoinc = next_autoinc.max(v.saturating_add(1));
                            autoinc_mutated = true;
                        }
                        _ => {
                            autoinc_mutated = true;
                        }
                    }
                }
                for (col, text) in &meta.defaults {
                    if !doc.contains_key(col) {
                        let e = match default_exprs.get(col) {
                            Some(e) => e,
                            None => {
                                let e = parse_expr_text(text)?;
                                default_exprs.insert(col.clone(), e);
                                default_exprs.get(col).expect("just inserted")
                            }
                        };
                        if expr_calls_wall_clock(e) {
                            // MERGE has no canonical resolved-text rewrite
                            // (unlike INSERT); replaying the statement on a
                            // peer would stamp a different clock. Fail loudly
                            // instead of silently diverging.
                            return err(
                                "MERGE INSERT cannot apply a wall-clock or random DEFAULT \
                                 (NOW/SYSDATE/CURRENT_TIMESTAMP/GETDATE/RAND); \
                                 supply the column explicitly",
                            );
                        }
                        doc.insert(col.clone(), eval_const(e)?);
                    }
                }
                meta.check(&doc)?;
                // FK child-side validation moved to the unified post-statement
                // check below (final state, both arms included).
                inserted_docs.push(doc.clone());
                let loc = heap.insert(&self.pager, &mut tx, &doc)?;
                reindex_insert(&self.pager, &mut tx, &idx_specs, &mut roots, &doc, loc)?;
                if let Some(col) = &meta.autoinc {
                    if let Some(Value::Int(i)) = doc.get(col) {
                        // 只累计,提交成功后才落到 last_insert_id/autoinc_cache:
                        // 循环中途失败回滚后,SCOPE_IDENTITY 不得读到幻影 id。
                        let cur = merge_last_id.unwrap_or(i64::MIN);
                        merge_last_id = Some(cur.max(*i));
                    }
                }
                inserted += 1;
            }
            merge_next_autoinc = next_autoinc;
        }
        // Legacy files whose constraint columns predate trees keep the
        // whole-set duplicate check (same gate as the INSERT path): with no
        // constraint tree, the reindex inserts above cannot reject
        // duplicates, and a MERGE could silently write a repeated business
        // key. Everything so far is staged in `tx` — the abort paths below
        // discard it (all-or-nothing).
        let has_constraints = meta.primary_key.is_some() || !meta.unique.is_empty();
        // ALL constraint columns must be treed to skip the whole-set check —
        // `any` here would let a mixed state (one treed, one treeless
        // constraint) skip the check and silently admit duplicates on the
        // treeless one (same all-or-nothing gate as the INSERT path).
        let constraint_treed = meta
            .primary_key
            .iter()
            .chain(meta.unique.iter())
            .all(|c| roots.contains_key(c));
        if has_constraints && !constraint_treed && (updates.len() + inserted) > 0 {
            let mut combined: Vec<Object> = match (Heap {
                pages: meta.pages.clone(),
                overflow_free: meta.overflow_free.clone(),
                dropped: Vec::new(),
            }
            .scan(&PageReader::current(&self.pager)))
            {
                Ok(docs) => docs,
                Err(e) => {
                    self.pager.abort_tx(tx)?;
                    return Err(e.into());
                }
            };
            // Swap the updated rows' pre-images for their post-images
            // (unambiguous: a pre-image's PK value is unique in committed
            // data), then add the not-matched inserts.
            for (_, old, _) in &updates {
                combined.retain(|d| d != old);
            }
            combined.extend(updates.iter().map(|(_, _, n)| n.clone()));
            combined.extend(inserted_docs.iter().cloned());
            if let Err(e) = meta.check_unique(&combined) {
                self.pager.abort_tx(tx)?;
                return Err(e);
            }
        }
        // FK validation against the statement's FINAL state: pre-state −
        // updated pre-images + updated post-images + inserted rows. Both
        // directions used to be checked per-arm against pre-statement data,
        // which let a MERGE land a dangling self-reference (UPDATE removes
        // the parent the INSERT arm points at) or wrongly reject a parent
        // the other arm had just created.
        if !updates.is_empty() || !inserted_docs.is_empty() {
            let final_docs: Vec<Object> = {
                let mut docs = match self.table_docs_cx(&tname) {
                    Ok(d) => d,
                    Err(e) => {
                        self.pager.abort_tx(tx)?;
                        return Err(e);
                    }
                };
                for (_, old, _) in &updates {
                    docs.retain(|d| d != old);
                }
                docs.extend(updates.iter().map(|(_, _, n)| n.clone()));
                docs.extend(inserted_docs.iter().cloned());
                docs
            };
            // Child side: every FK value in a new/inserted image must exist
            // in the final state. For self-references the candidate set is
            // EXACTLY the final images — `check_fks_in`'s same_batch is only
            // an ADDITIONAL parent source, so the pre-state heap would still
            // satisfy values this very statement removes.
            for (col, rtable, rcol) in &meta.foreign_keys {
                for doc in updates
                    .iter()
                    .map(|(_, _, n)| n)
                    .chain(inserted_docs.iter())
                {
                    self.stmt_deadline.check()?;
                    let Some(v) = doc.get(col) else { continue };
                    if matches!(v, Value::Null) {
                        continue; // NULL passes (MATCH SIMPLE semantics)
                    }
                    let matches_parent = |rd: &Object| {
                        rd.get(rcol)
                            .map(|rv| Value::cmp_values(rv, v) == Ordering::Equal)
                            .unwrap_or(false)
                    };
                    let hit = if rtable == &tname {
                        final_docs.iter().any(matches_parent)
                    } else {
                        match self.table_docs_cx(rtable) {
                            Ok(d) => d.iter().any(matches_parent),
                            Err(e) => {
                                self.pager.abort_tx(tx)?;
                                return Err(e);
                            }
                        }
                    };
                    if !hit {
                        self.pager.abort_tx(tx)?;
                        return err(format!(
                            "FOREIGN KEY constraint failed: {tname}.{col} references \
                             {rtable}.{rcol}"
                        ));
                    }
                }
            }
            // Parent side: removed values must not stay referenced — the
            // self-reference child scan runs over the final images.
            let removed: Vec<Object> = updates.iter().map(|(_, o, _)| o.clone()).collect();
            if !removed.is_empty() {
                if let Err(e) = self.check_fk_parent_delete_skipping(
                    &tname,
                    &removed,
                    &final_docs,
                    &[],
                    Some(&final_docs),
                ) {
                    self.pager.abort_tx(tx)?;
                    return Err(e);
                }
            }
        }
        // Emptied heap pages are released inside this transaction so later
        // statements reuse them instead of growing the file.
        for p in std::mem::take(&mut heap.dropped) {
            self.pager.free_page(&mut tx, p)?;
        }
        let dotted_now = self.note_dotted_fields(
            &tname,
            inserted_docs
                .iter()
                .chain(updates.iter().map(|(_, _, new)| new)),
        );
        if dotted_now
            || heap.pages != meta.pages
            || roots != meta.index_roots
            || heap.overflow_free != meta.overflow_free
        {
            self.sync_table_layout(
                &mut tx,
                &tname,
                heap.pages.clone(),
                roots,
                heap.overflow_free.clone(),
            )?;
        }
        self.commit_pager_tx(tx)?;
        if let Some(id) = merge_last_id {
            let cur = self.last_insert_id.unwrap_or(i64::MIN);
            self.last_insert_id = Some(cur.max(id));
        }
        if autoinc_mutated {
            // Same recovery rule as the plain UPDATE path: an id this
            // statement set explicitly (or reassigned) is invisible to
            // `merge_next_autoinc`, so only a recompute stays correct.
            self.autoinc_cache.remove(&tname);
        } else if merge_last_id.is_some() {
            self.autoinc_cache.insert(tname.clone(), merge_next_autoinc);
        }
        Ok(ExecOutcome::Affected((updates.len() + inserted) as u64))
    }

    /// Merge-namespaced evaluation row: target columns (unqualified) plus
    /// source columns (qualified `skey.col`, and unqualified when the name
    /// is not taken by the target). Qualified target refs resolve to the
    /// target's own columns only — a column the target row lacks pins to
    /// NULL instead of falling back to the source's bare-name value.
    fn merge_join_row(t_doc: &Object, t_key: &str, s_doc: &Object, s_key: &str) -> Object {
        let mut row = t_doc.clone();
        for (k, v) in s_doc {
            row.insert(format!("{s_key}.{k}"), v.clone());
            if !row.contains_key(k.as_str()) {
                row.insert(k.clone(), v.clone());
            }
        }
        for k in t_doc.keys() {
            row.insert(format!("{t_key}.{k}"), t_doc[k].clone());
        }
        for k in s_doc.keys() {
            row.entry(format!("{t_key}.{k}")).or_insert(Value::Null);
        }
        row
    }
}

/// Borrow-guarded fused write unit ([`Database::write_unit`]). Statements
/// run through the guard (it derefs to [`Database`]); [`WriteUnit::end`]
/// makes the whole unit durable with one WAL fsync.
pub struct WriteUnit<'a> {
    db: &'a mut Database,
}

impl std::ops::Deref for WriteUnit<'_> {
    type Target = Database;
    fn deref(&self) -> &Database {
        self.db
    }
}

impl std::ops::DerefMut for WriteUnit<'_> {
    fn deref_mut(&mut self) -> &mut Database {
        self.db
    }
}

impl WriteUnit<'_> {
    /// Close the unit and make its statements durable with one WAL fsync.
    /// No-op when nothing deferred a commit. Async-commit mode never fsyncs
    /// here: durability belongs to the background flusher's batching, and a
    /// per-unit sync would defeat it — the unit then only groups the
    /// statements' commits (the same deferred path they take anyway).
    pub fn end(self) -> Result<()> {
        let result = {
            self.db.write_unit = self.db.write_unit.saturating_sub(1);
            if self.db.async_commit || !self.db.pending_sync {
                Ok(())
            } else {
                self.db
                    .pager
                    .sync_wal()
                    .map_err(SqlError::from)
                    .map(|_| self.db.pending_sync = false)
            }
        };
        // The depth was already decremented above; skip Drop's.
        std::mem::forget(self);
        result
    }
}

impl Drop for WriteUnit<'_> {
    fn drop(&mut self) {
        self.db.write_unit = self.db.write_unit.saturating_sub(1);
    }
}

/// Root key of the unique constraint an `ON CONFLICT (cols)` target names:
/// a single-column spelling resolves through the constraint containers, a
/// multi-column one must match a UNIQUE index (column sets compare
/// order-insensitively, as in PostgreSQL).
fn resolve_conflict_columns(meta: &TableMeta, cols: &[String]) -> Result<String> {
    if cols.len() == 1 {
        let col = &cols[0];
        let constraint =
            meta.primary_key.as_deref() == Some(col.as_str()) || meta.unique.contains(col);
        if constraint && meta.index_roots.contains_key(col) {
            return Ok(col.clone());
        }
    }
    // Partial unique indexes are not targetable: their conflicts are
    // predicate-scoped and the untargeted policies handle them; a targeted
    // scope would need per-row predicate evaluation against the target.
    if let Some(def) = meta.index_defs.iter().find(|d| {
        d.pred.is_none()
            && d.unique
            && d.columns.len() == cols.len()
            && cols.iter().all(|c| d.columns.contains(c))
    }) {
        return Ok(if def.columns.len() == 1 {
            def.columns[0].clone()
        } else {
            def.name.clone()
        });
    }
    err(format!(
        "there is no unique constraint matching the ON CONFLICT target ({})",
        cols.join(", ")
    ))
}

/// Root key of the unique index `ON CONFLICT ON CONSTRAINT name` names.
fn resolve_conflict_constraint(meta: &TableMeta, table: &str, name: &str) -> Result<String> {
    // PK / table-declared UNIQUE constraints surface as derived
    // `sqlite_autoindex_<table>_<n>` names (PK first) — resolvable here
    // even though they are never persisted in the catalog.
    let prefix = format!("sqlite_autoindex_{table}_");
    if let Some(n) = name.strip_prefix(&prefix) {
        if let Some(i) = n.parse::<usize>().ok().filter(|i| *i >= 1) {
            let mut cols: Vec<&String> = Vec::new();
            for c in meta.primary_key.iter().chain(meta.constraint_unique.iter()) {
                if !cols.contains(&c) {
                    cols.push(c);
                }
            }
            if let Some(c) = cols.get(i - 1) {
                return Ok((*c).clone());
            }
        }
        return err(format!("constraint {name} does not exist"));
    }
    let Some(def) = meta.index_defs.iter().find(|d| d.name == name) else {
        return err(format!("constraint {name} does not exist"));
    };
    if !def.unique {
        return err(format!("constraint {name} is not unique"));
    }
    Ok(if def.columns.len() == 1 {
        def.columns[0].clone()
    } else {
        def.name.clone()
    })
}

/// Prefix every field with the source alias: "col" -> "alias.col".
/// True when the query's FROM factors mention `name` (recursive CTE
/// detection; T-SQL omits the RECURSIVE keyword, so shape + self-reference
/// is the signal). Rendered-text scan over the query AST is enough here —
/// a false positive merely routes a non-recursive CTE through the
/// recursive path, whose anchor evaluation then fails loudly naming it.
/// Does this CTE body reference `name` in a TABLE position (FROM/JOIN,
/// derived tables, subqueries)? Only those positions make the CTE
/// self-referential (recursion); the old whole-text word scan also hit the
/// CTE's own column names and output aliases — `WITH region AS (SELECT
/// region FROM … UNION ALL …)` is NOT recursive, but was misdiagnosed:
/// either rejected as "must be anchor UNION arm", or its non-recursive
/// UNION ALL body was iterated as recursion until the convergence caps
/// tripped on a perfectly legal query.
fn cte_references(q: &sqlparser::ast::Query, name: &str) -> bool {
    let mut refs = Vec::new();
    // `objects` = always-true keeps every WITH name out of the shadow set,
    // so factor names inside nested WITH bodies are still collected —
    // over-detecting costs a loud materialization error at worst, while
    // under-detecting silently reads a same-named base table instead.
    let _ = walk_query(q, &mut refs, &|_: &str| true);
    refs.iter().any(|r| r.eq_ignore_ascii_case(name))
}

/// PIVOT core: group docs by every field except the pivot column and the
/// aggregate argument's column, then emit `group keys + one column per
/// IN-value` holding the aggregate over the matching rows.
fn pivot_factor(
    docs: Vec<Object>,
    aggs: &[sqlparser::ast::ExprWithAlias],
    value_column: &[SqlExpr],
    value_source: &sqlparser::ast::PivotValueSource,
) -> Result<Vec<Object>> {
    let agg = &aggs[0];
    // The aggregate must be a plain single-argument function call
    // (`SUM(qty)`); its argument names the aggregated field.
    let sqlparser::ast::Expr::Function(f) = &agg.expr else {
        return err("PIVOT aggregate must be a function call like SUM(qty)");
    };
    let agg_name = f.name.to_string().to_uppercase();
    let arg_expr = match &f.args {
        sqlparser::ast::FunctionArguments::List(list) if list.args.len() == 1 => {
            match &list.args[0] {
                sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) => {
                    e.clone()
                }
                _ => return err("PIVOT aggregate must take one expression argument"),
            }
        }
        _ => return err("PIVOT aggregate must take one expression argument"),
    };
    // The pivot column: one bare identifier (`FOR yr IN …`).
    let pivot_col = match value_column {
        [SqlExpr::Identifier(i)] => i.value.clone(),
        _ => return err("PIVOT supports a single FOR column"),
    };
    // IN-list of literals (the output column names).
    let sqlparser::ast::PivotValueSource::List(values) = value_source else {
        return err("PIVOT IN (<subquery>) is not supported (list the values)");
    };
    let mut pivot_vals: Vec<(String, Value)> = Vec::new();
    for v in values {
        let expr = match v {
            sqlparser::ast::ExprWithAlias { expr, alias: None } => expr.clone(),
            sqlparser::ast::ExprWithAlias {
                expr,
                alias: Some(a),
            } => {
                // `[2024] AS q1`: the alias names the output column.
                let val = eval_const(expr)?;
                pivot_vals.push((a.value.clone(), val));
                continue;
            }
        };
        let val = eval_const(&expr)?;
        pivot_vals.push((value_to_text(&val), val));
    }
    // Output column names must be unique AND must not collide with a group
    // key field: `row.insert` writes straight into the group-key row, so a
    // collision silently replaced the grouping value (or an earlier pivot
    // cell) instead of erroring like every other duplicate-column shape.
    {
        let mut seen = std::collections::BTreeSet::new();
        for (name, _) in &pivot_vals {
            if !seen.insert(name.clone()) {
                return err(format!(
                    "PIVOT IN list produces duplicate output column '{name}'"
                ));
            }
        }
    }
    // Which fields does the aggregate argument read? Group by every other
    // field (T-SQL's implicit grouping). Single bare column only.
    let agg_col = match &arg_expr {
        SqlExpr::Identifier(i) => i.value.clone(),
        _ => return err("PIVOT aggregate argument must be a bare column"),
    };
    // First-seen group order. Members are bucketed by the group key at
    // partition time — the buckets ARE the grouping. Matching rows back
    // with a field predicate instead would be asymmetric (it would only
    // constrain the doc's fields against the key, not the key's fields
    // against the doc), so schemaless rows with differing field sets could
    // land in several groups at once and their aggregates would double-count.
    let mut groups: Vec<(Object, Vec<usize>)> = Vec::new();
    let mut index: std::collections::BTreeMap<String, usize> = Default::default();
    for (di, d) in docs.iter().enumerate() {
        let mut key = Object::new();
        for (k, v) in d {
            if suffix_eq(k, &pivot_col) || suffix_eq(k, &agg_col) {
                continue;
            }
            key.insert(k.clone(), v.clone());
        }
        let key_text = value_to_text(&Value::Object(key.clone()));
        match index.get(&key_text) {
            Some(&i) => groups[i].1.push(di),
            None => {
                index.insert(key_text, groups.len());
                groups.push((key, vec![di]));
            }
        }
    }
    // A pivot cell writes into the group-key row; a name that collides with
    // a grouping field would silently replace that field's value.
    for (name, _) in &pivot_vals {
        if groups
            .iter()
            .any(|(key, _)| key.keys().any(|k| suffix_eq(k, name)))
        {
            return err(format!(
                "PIVOT output column '{name}' collides with a grouping column; alias it"
            ));
        }
    }
    // Reduce per group × pivot value.
    let mut out = Vec::with_capacity(groups.len());
    for (key, member_idx) in &groups {
        let members: Vec<&Object> = member_idx.iter().map(|&i| &docs[i]).collect();
        let mut row = key.clone();
        for (name, want) in &pivot_vals {
            let matched: Vec<Value> = members
                .iter()
                .filter(|d| {
                    d.iter().any(|(k, v)| {
                        suffix_eq(k, &pivot_col) && Value::cmp_values(v, want) == Ordering::Equal
                    })
                })
                .map(|d| eval_expr(&arg_expr, d).unwrap_or(Value::Null))
                .collect();
            let cell = reduce_aggregate(&agg_name, &matched)?;
            row.insert(name.clone(), cell);
        }
        out.push(row);
    }
    Ok(out)
}

/// True when `key` equals `col` or ends with `.col` (schemaless suffix
/// rule, same as column lookup).
fn suffix_eq(key: &str, col: &str) -> bool {
    key == col || key.ends_with(&format!(".{col}"))
}

/// SUM/AVG/MIN/MAX/COUNT over evaluated values (PIVOT's local reducer).
/// Empty input (no rows matched the pivot value, or every matched value is
/// NULL) follows SQL aggregate semantics: COUNT → 0, everything else → NULL.
fn reduce_aggregate(name: &str, vals: &[Value]) -> Result<Value> {
    let non_null: Vec<&Value> = vals.iter().filter(|v| !matches!(v, Value::Null)).collect();
    match name {
        "COUNT" | "COUNT_BIG" => Ok(Value::Int(non_null.len() as i64)),
        "MIN" => Ok(non_null
            .iter()
            .min_by(|a, b| Value::cmp_values(a, b))
            .map(|v| (*v).clone())
            .unwrap_or(Value::Null)),
        "MAX" => Ok(non_null
            .iter()
            .max_by(|a, b| Value::cmp_values(a, b))
            .map(|v| (*v).clone())
            .unwrap_or(Value::Null)),
        "SUM" | "AVG" => {
            if non_null.is_empty() {
                return Ok(Value::Null);
            }
            if name == "AVG" {
                // Mirror eval_agg's AVG (float, or exact when decimal input):
                // `arith`'s Int/Int Divide truncates, so PIVOT(AVG) used to
                // disagree with a plain AVG over the same rows.
                if non_null.iter().any(|v| matches!(v, Value::Decimal(_))) {
                    let mut acc = Decimal::ZERO;
                    for v in &non_null {
                        acc = acc
                            .checked_add(as_decimal(v)?)
                            .ok_or_else(|| SqlError::Message("AVG sum overflow".into()))?;
                    }
                    return Ok(acc
                        .checked_div(Decimal::from(non_null.len() as i64))
                        .map(Value::Decimal)
                        .unwrap_or(Value::Null));
                }
                let n = non_null.len() as f64;
                let mut acc = 0.0f64;
                for v in &non_null {
                    acc += match v {
                        Value::Int(i) => *i as f64,
                        Value::Float(f) => *f,
                        _ => return err("AVG of non-numeric"),
                    };
                }
                return Ok(Value::Float(acc / n));
            }
            let mut acc = Value::Int(0);
            for v in &non_null {
                acc = arith(acc, &BinaryOperator::Plus, (*v).clone())?;
            }
            Ok(acc)
        }
        other => err(format!(
            "PIVOT aggregate {other} is not supported (SUM/AVG/MIN/MAX/COUNT)"
        )),
    }
}

/// UNPIVOT core: for each row, one output row per listed column with the
/// remaining fields plus {name_col: column, value_col: value}; NULL cells
/// are dropped (T-SQL default).
fn unpivot_factor(
    docs: Vec<Object>,
    value: &SqlExpr,
    name: &sqlparser::ast::Ident,
    columns: &[sqlparser::ast::ExprWithAlias],
) -> Result<Vec<Object>> {
    let value_col = match value {
        SqlExpr::Identifier(i) => i.value.clone(),
        _ => return err("UNPIVOT value must be a bare column name"),
    };
    let name_col = name.value.clone();
    let mut wanted: Vec<(String, String)> = Vec::new();
    for c in columns {
        let src = match &c.expr {
            SqlExpr::Identifier(i) => i.value.clone(),
            _ => return err("UNPIVOT IN-list must be bare column names"),
        };
        let out_name = c
            .alias
            .as_ref()
            .map(|a| a.value.clone())
            .unwrap_or_else(|| src.clone());
        wanted.push((src, out_name));
    }
    let mut out = Vec::new();
    for d in &docs {
        for (src, label) in &wanted {
            let v = d
                .iter()
                .find(|(k, _)| suffix_eq(k, src))
                .map(|(_, v)| v.clone())
                .unwrap_or(Value::Null);
            if matches!(v, Value::Null) {
                continue;
            }
            let mut row = Object::new();
            for (k, val) in d {
                if wanted.iter().any(|(s, _)| suffix_eq(k, s)) {
                    continue;
                }
                row.insert(k.clone(), val.clone());
            }
            row.insert(name_col.clone(), Value::Str(label.clone()));
            row.insert(value_col.clone(), v);
            out.push(row);
        }
    }
    Ok(out)
}

fn qualify(doc: &Object, alias: &str) -> Object {
    doc.iter()
        .map(|(k, v)| (format!("{alias}.{k}"), v.clone()))
        .collect()
}

/// True when the expression calls NEWID()/NEWSEQUENTIALID() at any depth.
/// Mirrors the expression shapes `rewrite_windows`/`walk_expr` cover.
pub(crate) fn calls_newid(e: &SqlExpr) -> bool {
    match e {
        SqlExpr::Function(f) => {
            let n = f.name.to_string().to_uppercase();
            if matches!(n.as_str(), "NEWID" | "NEWSEQUENTIALID" | "RAND") {
                return true;
            }
            child_exprs(e).into_iter().any(calls_newid)
        }
        SqlExpr::Nested(inner) => calls_newid(inner),
        SqlExpr::BinaryOp { left, right, .. } => calls_newid(left) || calls_newid(right),
        SqlExpr::UnaryOp { expr, .. } => calls_newid(expr),
        SqlExpr::IsNull(inner)
        | SqlExpr::IsNotNull(inner)
        | SqlExpr::IsTrue(inner)
        | SqlExpr::IsNotTrue(inner)
        | SqlExpr::IsFalse(inner)
        | SqlExpr::IsNotFalse(inner)
        | SqlExpr::IsUnknown(inner)
        | SqlExpr::IsNotUnknown(inner) => calls_newid(inner),
        SqlExpr::Between {
            expr, low, high, ..
        } => calls_newid(expr) || calls_newid(low) || calls_newid(high),
        SqlExpr::IsDistinctFrom(l, r) | SqlExpr::IsNotDistinctFrom(l, r) => {
            calls_newid(l) || calls_newid(r)
        }
        SqlExpr::Like { expr, pattern, .. } | SqlExpr::ILike { expr, pattern, .. } => {
            calls_newid(expr) || calls_newid(pattern)
        }
        SqlExpr::InList { expr, list, .. } => calls_newid(expr) || list.iter().any(calls_newid),
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand.as_deref().is_some_and(calls_newid)
                || conditions
                    .iter()
                    .any(|w| calls_newid(&w.condition) || calls_newid(&w.result))
                || else_result.as_deref().is_some_and(calls_newid)
        }
        SqlExpr::Cast { expr, .. } => calls_newid(expr),
        // Subqueries must recurse too: a NEWID() inside a subquery of an
        // UPDATE/DELETE/MERGE predicate would evaluate per node on replay
        // and silently diverge replicas — the same reason the statement is
        // rejected at the top level.
        SqlExpr::Subquery(q) | SqlExpr::Exists { subquery: q, .. } => query_calls_newid(q),
        SqlExpr::InSubquery { expr, subquery, .. } => {
            calls_newid(expr) || query_calls_newid(subquery)
        }
        SqlExpr::AnyOp { left, right, .. } | SqlExpr::AllOp { left, right, .. } => {
            calls_newid(left) || calls_newid(right)
        }
        // Same shared traversal as the other tree-wide predicates: wrappers
        // (CAST/FLOOR/IS NULL/…) must not hide a NEWID()/RAND() from the
        // write-path rejection.
        _ => child_exprs(e).into_iter().any(calls_newid),
    }
}

/// `calls_newid` over every scope-local surface of a query (mirrors the
/// outer-reference walkers' surface set).
fn query_calls_newid(q: &Query) -> bool {
    if let Some(w) = &q.with {
        for cte in &w.cte_tables {
            if query_calls_newid(&cte.query) {
                return true;
            }
        }
    }
    if setexpr_calls_newid(&q.body) {
        return true;
    }
    if let Some(ob) = &q.order_by {
        if let sqlparser::ast::OrderByKind::Expressions(exprs) = &ob.kind {
            if exprs.iter().any(|o| calls_newid(&o.expr)) {
                return true;
            }
        }
    }
    false
}

/// A FROM factor carrying nondeterministic calls: derived-table
/// subqueries and table-function arguments evaluate per execution — a
/// journal replay of the original text would re-roll them on every peer
/// and silently fork the cluster. Covers the relation itself and every
/// joined relation.
fn factor_calls_newid(factor: &sqlparser::ast::TableFactor) -> bool {
    match factor {
        sqlparser::ast::TableFactor::Derived { subquery, .. } => query_calls_newid(subquery),
        sqlparser::ast::TableFactor::TableFunction { expr, .. } => calls_newid(expr),
        _ => false,
    }
}

fn twj_calls_newid(twj: &sqlparser::ast::TableWithJoins) -> bool {
    factor_calls_newid(&twj.relation) || twj.joins.iter().any(|j| factor_calls_newid(&j.relation))
}

fn setexpr_calls_newid(body: &sqlparser::ast::SetExpr) -> bool {
    use sqlparser::ast::SetExpr;
    match body {
        SetExpr::Select(sel) => {
            for twj in &sel.from {
                if twj_calls_newid(twj) {
                    return true;
                }
                for j in &twj.joins {
                    if let Some(e) = join_on_expr(&j.join_operator) {
                        if calls_newid(e) {
                            return true;
                        }
                    }
                }
            }
            if sel.selection.as_ref().is_some_and(calls_newid) {
                return true;
            }
            if sel.having.as_ref().is_some_and(calls_newid) {
                return true;
            }
            if let sqlparser::ast::GroupByExpr::Expressions(es, _) = &sel.group_by {
                if es.iter().any(calls_newid) {
                    return true;
                }
            }
            sel.projection.iter().any(|item| match item {
                sqlparser::ast::SelectItem::UnnamedExpr(e)
                | sqlparser::ast::SelectItem::ExprWithAlias { expr: e, .. } => calls_newid(e),
                _ => false,
            })
        }
        SetExpr::SetOperation { left, right, .. } => {
            setexpr_calls_newid(left) || setexpr_calls_newid(right)
        }
        SetExpr::Query(inner) => query_calls_newid(inner),
        SetExpr::Values(rows) => rows
            .rows
            .iter()
            .flat_map(|parens| parens.content.iter())
            .any(calls_newid),
        _ => false,
    }
}

/// NEWID()/NEWSEQUENTIALID() anywhere in an UPDATE/DELETE/MERGE statement
/// (assignments, predicates, join conditions, MERGE clause actions).
fn stmt_calls_newid(stmt: &Statement) -> bool {
    use sqlparser::ast::{FromTable, JoinConstraint, JoinOperator};
    let join_ons = |twj: &sqlparser::ast::TableWithJoins| -> bool {
        twj.joins.iter().any(|j| {
            matches!(
                &j.join_operator,
                JoinOperator::Join(c)
                    | JoinOperator::Inner(c)
                    | JoinOperator::Left(c)
                    | JoinOperator::LeftOuter(c)
                    | JoinOperator::Right(c)
                    | JoinOperator::RightOuter(c)
                    | JoinOperator::FullOuter(c)
                    if matches!(c, JoinConstraint::On(e) if calls_newid(e))
            )
        })
    };
    match stmt {
        Statement::Update(u) => {
            u.assignments.iter().any(|a| calls_newid(&a.value))
                || u.selection.as_ref().is_some_and(calls_newid)
                || join_ons(&u.table)
                || twj_calls_newid(&u.table)
                || u.from.as_ref().is_some_and(|f| match f {
                    sqlparser::ast::UpdateTableFromKind::BeforeSet(twjs)
                    | sqlparser::ast::UpdateTableFromKind::AfterSet(twjs) => {
                        twjs.iter().any(|t| join_ons(t) || twj_calls_newid(t))
                    }
                })
        }
        Statement::Delete(d) => {
            d.selection.as_ref().is_some_and(calls_newid)
                || d.using
                    .as_ref()
                    .is_some_and(|twjs| twjs.iter().any(|t| join_ons(t) || twj_calls_newid(t)))
                || match &d.from {
                    FromTable::WithFromKeyword(twjs) | FromTable::WithoutKeyword(twjs) => {
                        twjs.iter().any(|t| join_ons(t) || twj_calls_newid(t))
                    }
                }
        }
        Statement::CreateTable(c) => {
            // CTAS journals its original text: a NEWID()/RAND() in the
            // query has no per-row rewrite here (unlike INSERT), so each
            // replaying peer would roll its own values and silently
            // diverge the cluster — refuse like UPDATE/DELETE/MERGE.
            c.query.as_ref().is_some_and(|q| query_calls_newid(q))
        }
        Statement::Merge(m) => {
            factor_calls_newid(&m.source)
                || calls_newid(&m.on)
                || m.clauses.iter().any(|c| {
                    c.predicate.as_ref().is_some_and(calls_newid)
                        || match &c.action {
                            sqlparser::ast::MergeAction::Insert(ins) => {
                                ins.insert_predicate.as_ref().is_some_and(calls_newid)
                                    || match &ins.kind {
                                        sqlparser::ast::MergeInsertKind::Values(v) => v
                                            .rows
                                            .iter()
                                            .any(|row| row.content.iter().any(calls_newid)),
                                        _ => false,
                                    }
                            }
                            sqlparser::ast::MergeAction::Update(up) => {
                                up.assignments.iter().any(|a| calls_newid(&a.value))
                                    || up.update_predicate.as_ref().is_some_and(calls_newid)
                                    || up.delete_predicate.as_ref().is_some_and(calls_newid)
                            }
                            sqlparser::ast::MergeAction::Delete { .. } => false,
                        }
                })
        }
        _ => false,
    }
}

/// Build a rows result from RETURNING items over the affected documents.
fn project_returning(items: &[SelectItem], docs: &[Object]) -> Result<ExecOutcome> {
    let mut want_star = false;
    let mut columns = Vec::new();
    let mut exprs = Vec::new();
    for item in items {
        match item {
            SelectItem::Wildcard(_) => want_star = true,
            SelectItem::UnnamedExpr(e) => {
                columns.push(expr_name(e));
                exprs.push(e.clone());
            }
            SelectItem::ExprWithAlias { expr, alias, .. } => {
                columns.push(alias.value.clone());
                exprs.push(expr.clone());
            }
            _ => return err("unsupported RETURNING item"),
        }
    }
    // `RETURNING *` projects like `SELECT *`: the returned documents' field
    // union, ordered, missing fields as NULL. It used to be documented but
    // rejected here because only the expression arms were wired up.
    if want_star {
        let mut star_cols = union_of_fields(docs);
        star_cols.extend(columns);
        columns = star_cols;
    }
    let base = columns.len() - exprs.len();
    let mut rows = Vec::new();
    for doc in docs {
        let mut row = Vec::with_capacity(columns.len());
        if want_star {
            row.extend(
                columns[..base]
                    .iter()
                    .map(|c| doc.get(c).cloned().unwrap_or(Value::Null)),
            );
        }
        for e in &exprs {
            row.push(eval_expr(e, doc)?);
        }
        rows.push(row);
    }
    Ok(ExecOutcome::Rows(QueryResult { columns, rows }))
}

fn str_list(m: &Object, key: &str) -> Vec<String> {
    match m.get(key) {
        Some(Value::Array(a)) => a
            .iter()
            .filter_map(|x| x.as_str().map(String::from))
            .collect(),
        _ => vec![],
    }
}

/// Column types that declare a GUID. Values live as canonical lowercase
/// UUID strings, so string order equals UUIDv7 time order.
fn is_guid_type(dt: &sqlparser::ast::DataType) -> bool {
    let t = dt.to_string().trim().to_uppercase();
    matches!(t.as_str(), "GUID" | "UUID" | "UNIQUEIDENTIFIER" | "UUIDV7")
}

/// Reject constraint decorations the engine cannot honor instead of silently
/// dropping them (house red line). `DEFERRABLE` / `INITIALLY DEFERRED`
/// promise check-at-commit, `NOT ENFORCED` promises no checking, `NULLS NOT
/// DISTINCT` changes duplicate handling and `MATCH FULL/PARTIAL` changes null
/// treatment — all semantics this engine does not implement, so accepting
/// them would lie about what the constraint does. No-op spellings that
/// describe the existing behavior (`NOT DEFERRABLE`, `INITIALLY IMMEDIATE`,
/// `ENFORCED`, `MATCH SIMPLE`, `NULLS DISTINCT`) stay accepted.
fn reject_constraint_decorations(
    what: &str,
    characteristics: Option<&sqlparser::ast::ConstraintCharacteristics>,
    nulls_distinct: Option<&sqlparser::ast::NullsDistinctOption>,
    match_kind: Option<&sqlparser::ast::ConstraintReferenceMatchKind>,
) -> Result<()> {
    use sqlparser::ast::{
        ConstraintReferenceMatchKind as MatchKind, DeferrableInitial, NullsDistinctOption,
    };
    if let Some(ch) = characteristics {
        if ch.deferrable == Some(true) {
            return err(format!(
                "{what}: DEFERRABLE constraints are not supported (checks are immediate)"
            ));
        }
        if ch.initially == Some(DeferrableInitial::Deferred) {
            return err(format!(
                "{what}: INITIALLY DEFERRED constraints are not supported (checks are immediate)"
            ));
        }
        if ch.enforced == Some(false) {
            return err(format!(
                "{what}: NOT ENFORCED constraints are not supported (checks always run)"
            ));
        }
    }
    if matches!(nulls_distinct, Some(NullsDistinctOption::NotDistinct)) {
        return err(format!("{what}: NULLS NOT DISTINCT is not supported"));
    }
    if matches!(match_kind, Some(MatchKind::Full) | Some(MatchKind::Partial)) {
        return err(format!(
            "{what}: MATCH FULL/PARTIAL is not supported (MATCH SIMPLE semantics)"
        ));
    }
    Ok(())
}

/// SQL literal for a value carried over into a replicated statement.
pub(crate) fn value_literal(v: &Value) -> Result<String> {
    match v {
        Value::Null => Ok("NULL".into()),
        Value::Bool(b) => Ok(if *b { "TRUE" } else { "FALSE" }.into()),
        Value::Int(i) => Ok(i.to_string()),
        Value::Float(f) => {
            // Non-finite floats have no bare literal: CAST('NaN'/'inf' AS
            // REAL) parses back to the same value, so a dump or replication
            // rewrite round-trips instead of failing or degrading to Float.
            // Debug formatting keeps integral floats distinguishable from Int
            // ("3.0" vs "3"), preserving the type across resolved-INSERT
            // replay.
            if !f.is_finite() {
                return Ok(format!(
                    "CAST({} AS REAL)",
                    crate::stmt::sql_string_literal(crate::json::float_marker_text(*f))
                ));
            }
            Ok(format!("{f:?}"))
        }
        // Exact text form wrapped in a CAST: bare decimal text would re-parse
        // as Float on replay and silently lose precision.
        Value::Decimal(d) => Ok(format!(
            "CAST({} AS DECIMAL)",
            crate::stmt::sql_string_literal(&d.to_string())
        )),
        Value::Bytes(b) => {
            let mut hex = String::with_capacity(b.len() * 2 + 3);
            hex.push_str("x'");
            for byte in b {
                hex.push_str(&format!("{byte:02x}"));
            }
            hex.push('\'');
            Ok(hex)
        }
        Value::Str(s) => Ok(crate::stmt::sql_string_literal(s)),
        // TIMESTAMP '…' form: bare ISO text would re-parse as Str on replay.
        Value::Timestamp(ms) => {
            // Construction sites keep Timestamps inside 0001..=9999, but a
            // legacy volume written before that guard could still hold an
            // out-of-domain instant — fail the DUMP loudly here instead of
            // emitting a literal the restore would reject half-way through.
            if !crate::value::is_valid_timestamp_ms(*ms) {
                return err(format!(
                    "TIMESTAMP value {ms} is outside 0001-01-01..=9999-12-31 and cannot be dumped"
                ));
            }
            Ok(format!(
                "CAST({} AS TIMESTAMP)",
                crate::stmt::sql_string_literal(&crate::value::format_timestamp_ms(*ms))
            ))
        }
        // Structured values (JSON_EXTRACT output, INSERT ... SELECT from a
        // JSON function, etc.) have no literal syntax in this dialect: the
        // engine's canonical JSON text wrapped in JSON_EXTRACT round-trips
        // exactly (including `$dec`/`$bytes`/`$float` markers), so backups
        // and join snapshots can carry them.
        Value::Array(_) | Value::Object(_) => Ok(format!(
            "JSON_EXTRACT({})",
            crate::stmt::sql_string_literal(&crate::json::to_string(v))
        )),
    }
}

/// Canonical text for a stored DEFAULT expression. sqlparser keeps
/// redundant parentheses as `Expr::Nested`, so `DEFAULT ('anon')` from a
/// replayed dump would otherwise be stored as `('anon')` and grow one
/// layer per dump/restore cycle — silently poisoning `schema_hash` and
/// permanently degrading digest equality (every rejoin falls back to a
/// snapshot). Strip the nesting so the stored form is a fixed point of
/// dump → parse → store.
fn default_expr_text(e: &SqlExpr) -> String {
    match e {
        SqlExpr::Nested(inner) => default_expr_text(inner),
        _ => format!("{e}"),
    }
}

/// Double-quoted SQL identifier.
fn quote_ident(name: &str) -> String {
    crate::stmt::sql_quote_ident(name)
}

/// Views reject every write and every schema change: they are named SELECTs
/// with no storage. The guard names the offending kind explicitly (SQLite:
/// "cannot modify X because it is a view").
fn ensure_not_view(kind: &str, table: &str, meta: &TableMeta) -> Result<()> {
    if meta.is_view() {
        return Err(SqlError::Message(format!(
            "cannot {kind} view {table} (it is a view, not a table)"
        )));
    }
    if meta.is_matview() {
        return Err(SqlError::Message(format!(
            "cannot {kind} materialized view {table} (it is refreshed, not written: \
             REFRESH MATERIALIZED VIEW {table})"
        )));
    }
    Ok(())
}

/// Leaf table name of an ObjectName (last part). Crate-internal callers
/// use [`obj_name`]; the server's row-filter rewrite reaches it through
/// [`table_ref_name`].
pub fn table_ref_name(name: &ObjectName) -> String {
    obj_name(name)
}

fn obj_name(name: &ObjectName) -> String {
    name.0
        .last()
        .map(|p| match p {
            ObjectNamePart::Identifier(i) => i.value.clone(),
            other => other.to_string(),
        })
        .unwrap_or_default()
}

/// True for names that `load_table_factor` resolves to a virtual
/// compatibility view instead of a real heap. Fast paths must not look
/// their rows up in `tables`; keep this list in sync with that resolver.
fn is_compat_view(name: &str) -> bool {
    if name.eq_ignore_ascii_case("DUAL")
        || name.eq_ignore_ascii_case("sqlite_master")
        || name.eq_ignore_ascii_case("sqlite_temporal_master")
        || name.eq_ignore_ascii_case("information_schema.tables")
        || name.eq_ignore_ascii_case("information_schema.columns")
    {
        return true;
    }
    matches!(
        name.to_ascii_uppercase().as_str(),
        "ALL_TABLES"
            | "USER_TABLES"
            | "ALL_TAB_COLUMNS"
            | "USER_TAB_COLUMNS"
            | "ALL_INDEXES"
            | "USER_INDEXES"
    )
}

/// Explicit alias column list (`AS t(a, b)` on a derived table or CTE):
/// renamed positionally, with duplicate or wrong-width lists rejected.
/// `None` means the producing query's own column names are kept.
fn table_alias_columns(
    alias_columns: &[sqlparser::ast::TableAliasColumnDef],
    width: usize,
) -> Result<Option<Vec<String>>> {
    if alias_columns.is_empty() {
        return Ok(None);
    }
    if alias_columns.len() != width {
        return err(format!(
            "alias declares {} columns but the table produces {width}",
            alias_columns.len()
        ));
    }
    let names: Vec<String> = alias_columns.iter().map(|c| c.name.value.clone()).collect();
    let mut seen = std::collections::BTreeSet::new();
    if !names.iter().all(|n| seen.insert(n.clone())) {
        return err("alias column names must be distinct");
    }
    Ok(Some(names))
}

/// Fixed output shape of a known table function. STRING_SPLIT's `ordinal`
/// column only materializes when the third argument is enabled, so the
/// width check for `AS s(v, ord)` follows the actual output, not the
/// argument count.
fn tablefn_static_cols(fname_upper: &str, ordinal_on: bool) -> &'static [&'static str] {
    match fname_upper {
        "STRING_SPLIT" => {
            if ordinal_on {
                &["value", "ordinal"]
            } else {
                &["value"]
            }
        }
        "GENERATE_SERIES" => &["value"],
        _ => &["key", "value", "type"],
    }
}

/// Apply a table function's alias column list (`FROM f(..) AS t(c1, c2)`)
/// as a positional rename over its BARE output columns. Used by every
/// `FROM`-position table function; the APPLY path renames qualified keys
/// itself. Silent omission here made `s.v` read NULL while `s.value`
/// kept working.
fn rename_tablefn_docs(
    alias_obj: &sqlparser::ast::TableAlias,
    fname_upper: &str,
    ordinal_on: bool,
    docs: &mut [Object],
) -> Result<()> {
    let actual: Vec<String> = match docs.first() {
        Some(d) => d.keys().cloned().collect(),
        None => tablefn_static_cols(fname_upper, ordinal_on)
            .iter()
            .map(|s| s.to_string())
            .collect(),
    };
    let Some(rename) = table_alias_columns(&alias_obj.columns, actual.len())? else {
        return Ok(());
    };
    for row in docs.iter_mut() {
        for (new, old) in rename.iter().zip(actual.iter()) {
            if let Some(v) = row.remove(old) {
                row.insert(new.clone(), v);
            }
        }
    }
    Ok(())
}

/// Reject SELECT clause syntax the engine does not implement instead of
/// silently ignoring it (a dropped clause changes results; house rule is
/// loud refusal). Advisory hints (optimizer/index hints) are exempt —
/// they affect planning, not results.
fn reject_unsupported_select_clauses(select: &sqlparser::ast::Select) -> Result<()> {
    if select.into.is_some() {
        return err("SELECT INTO is not supported");
    }
    if select.top.is_some() {
        return err("SELECT TOP is not supported (use LIMIT)");
    }
    // DISTINCT ON is only rejected inside exec_plain_select today — the
    // EXPLAIN mirror plans right past it, describing a statement that can
    // never run.
    if matches!(&select.distinct, Some(sqlparser::ast::Distinct::On(_))) {
        return err("DISTINCT ON is not supported");
    }
    if !select.lateral_views.is_empty() {
        return err("LATERAL VIEW is not supported");
    }
    if select.prewhere.is_some() {
        return err("PREWHERE is not supported");
    }
    if !select.connect_by.is_empty() {
        return err("CONNECT BY is not supported");
    }
    if !select.cluster_by.is_empty() {
        return err("CLUSTER BY is not supported");
    }
    if !select.distribute_by.is_empty() {
        return err("DISTRIBUTE BY is not supported");
    }
    if !select.sort_by.is_empty() {
        return err("SORT BY is not supported");
    }
    if !select.named_window.is_empty() {
        return err("WINDOW clause is not supported");
    }
    if select.value_table_mode.is_some() {
        return err("SELECT AS VALUE/STRUCT is not supported");
    }
    if select.exclude.is_some() {
        return err("SELECT * EXCLUDE is not supported");
    }
    for item in &select.projection {
        // `* EXCLUDE/EXCEPT/REPLACE/RENAME/ILIKE` parse into wildcard
        // options; `exec_plain_select` expands the plain `*`, so anything
        // else here would silently drop the modifier.
        let opts = match item {
            SelectItem::Wildcard(opts) | SelectItem::QualifiedWildcard(_, opts) => opts,
            _ => continue,
        };
        if opts.opt_ilike.is_some()
            || opts.opt_exclude.is_some()
            || opts.opt_except.is_some()
            || opts.opt_replace.is_some()
            || opts.opt_rename.is_some()
            || opts.opt_alias.is_some()
        {
            return err("SELECT * EXCLUDE/EXCEPT/REPLACE/RENAME/ILIKE is not supported");
        }
    }
    if select.select_modifiers.is_some() {
        return err("SELECT modifiers are not supported");
    }
    Ok(())
}

/// Query-level clause guard; see [`reject_unsupported_select_clauses`].
fn reject_unsupported_query_clauses(query: &Query) -> Result<()> {
    if !query.locks.is_empty() {
        return err("FOR UPDATE/FOR SHARE is not supported");
    }
    if query.for_clause.is_some() {
        return err("FOR XML/FOR JSON is not supported");
    }
    if query.settings.is_some() {
        return err("SETTINGS is not supported");
    }
    if query.format_clause.is_some() {
        return err("FORMAT is not supported");
    }
    if !query.pipe_operators.is_empty() {
        return err("pipe operators are not supported");
    }
    // ClickHouse `LIMIT n BY cols`:dropping the BY list would silently run
    // a bare LIMIT — the red line on clause-level silent ignoring.
    if let Some(sqlparser::ast::LimitClause::LimitOffset { limit_by, .. }) = &query.limit_clause {
        if !limit_by.is_empty() {
            return err("LIMIT ... BY is not supported");
        }
    }
    Ok(())
}

/// T-SQL `SELECT TOP (n) [WITH TIES]`: the generic parser carries the
/// clause in `Select.top`; translate it onto the standard LIMIT/FETCH
/// machinery so the executor sees one pagination path (and the ORDER BY
/// requirement of WITH TIES is enforced by the FETCH branch). PERCENT has
/// no pre-count pass to size it and stays a loud error. Set-operation
/// arms re-enter `exec_query_body`, so TOP inside a UNION arm scopes to
/// that arm — SQL Server semantics.
fn rewrite_tsql_top(mut query: Query) -> Result<Query> {
    let top = match query.body.as_mut() {
        SetExpr::Select(select) => select.top.take(),
        _ => None,
    };
    let Some(top) = top else {
        return Ok(query);
    };
    if top.percent {
        return err("SELECT TOP ... PERCENT is not supported (use LIMIT)");
    }
    let n = match top.quantity {
        Some(sqlparser::ast::TopQuantity::Constant(n)) => {
            SqlExpr::Value(sqlparser::ast::Value::Number(n.to_string(), false).into())
        }
        Some(sqlparser::ast::TopQuantity::Expr(e)) => e,
        None => return err("SELECT TOP requires a row count"),
    };
    if top.with_ties {
        if query.fetch.is_some() {
            return err("TOP WITH TIES cannot be combined with FETCH");
        }
        query.fetch = Some(sqlparser::ast::Fetch {
            with_ties: true,
            percent: false,
            quantity: Some(n),
        });
    } else {
        if query.limit_clause.is_some() {
            return err("TOP cannot be combined with LIMIT");
        }
        query.limit_clause = Some(LimitClause::LimitOffset {
            limit: Some(n),
            offset: None,
            limit_by: vec![],
        });
    }
    Ok(query)
}

fn value_to_literal(v: Value) -> Result<SqlExpr> {
    use sqlparser::ast::Value as V;
    Ok(match v {
        Value::Null => SqlExpr::Value(V::Null.into()),
        Value::Bool(b) => SqlExpr::Value(V::Boolean(b).into()),
        Value::Int(i) => SqlExpr::Value(V::Number(i.to_string(), false).into()),
        // Debug formatting keeps integral floats distinguishable from Int
        // (`3.0` prints "3" via Display and would re-parse as Int(3)); same
        // rule value_literal applies to dumps.
        Value::Float(f) => SqlExpr::Value(V::Number(format!("{f:?}"), false).into()),
        Value::Str(s) => SqlExpr::Value(V::SingleQuotedString(s).into()),
        // Exact/exotic scalars ride as subquery-independent expressions the
        // same way value_literal renders them for dumps — a bare number or
        // string would degrade Decimal to Float and Timestamp to Str.
        other @ (Value::Decimal(_) | Value::Timestamp(_)) => {
            parse_expr_text(&value_literal(&other)?)?
        }
        Value::Bytes(b) => {
            let mut hex = String::with_capacity(b.len() * 2 + 3);
            hex.push_str("x'");
            for byte in &b {
                hex.push_str(&format!("{byte:02x}"));
            }
            hex.push('\'');
            parse_expr_text(&hex)?
        }
        other => return err(format!("cannot inline value as literal: {other:?}")),
    })
}

/// Substitute the proposed-row references in a DO UPDATE SET/WHERE tree:
/// `EXCLUDED.col` (PostgreSQL) and `VALUES(col)` (MySQL) become literals of
/// the candidate row's values. Descends the DIRECT expression tree only —
/// a subquery body mentioning the proposed row is rejected afterwards
/// (see [`upsert_proposed_in_subquery`]), so a reference can never
/// silently mis-resolve to the existing row's same-named column.
fn subst_upsert_proposed(e: &mut SqlExpr, proposed: &Object) -> Result<()> {
    match e {
        SqlExpr::CompoundIdentifier(parts)
            if parts.len() >= 2 && parts[0].value.eq_ignore_ascii_case("EXCLUDED") =>
        {
            let leaf = parts.last().map(|p| p.value.clone()).unwrap_or_default();
            let v = proposed.get(&leaf).cloned().unwrap_or(Value::Null);
            *e = value_to_literal(v)?;
        }
        SqlExpr::Function(f) if is_values_call(f) => {
            let col = values_call_column(f);
            let v = proposed.get(&col).cloned().unwrap_or(Value::Null);
            *e = value_to_literal(v)?;
        }
        SqlExpr::BinaryOp { left, right, .. } => {
            subst_upsert_proposed(left, proposed)?;
            subst_upsert_proposed(right, proposed)?;
        }
        SqlExpr::UnaryOp { expr, .. } => subst_upsert_proposed(expr, proposed)?,
        SqlExpr::Nested(inner) => subst_upsert_proposed(inner, proposed)?,
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            subst_upsert_proposed(expr, proposed)?;
            subst_upsert_proposed(low, proposed)?;
            subst_upsert_proposed(high, proposed)?;
        }
        SqlExpr::IsDistinctFrom(l, r) | SqlExpr::IsNotDistinctFrom(l, r) => {
            subst_upsert_proposed(l, proposed)?;
            subst_upsert_proposed(r, proposed)?;
        }
        SqlExpr::Like { expr, pattern, .. } | SqlExpr::ILike { expr, pattern, .. } => {
            subst_upsert_proposed(expr, proposed)?;
            subst_upsert_proposed(pattern, proposed)?;
        }
        SqlExpr::InList { expr, list, .. } => {
            subst_upsert_proposed(expr, proposed)?;
            for item in list {
                subst_upsert_proposed(item, proposed)?;
            }
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                subst_upsert_proposed(op, proposed)?;
            }
            for w in conditions.iter_mut() {
                subst_upsert_proposed(&mut w.condition, proposed)?;
                subst_upsert_proposed(&mut w.result, proposed)?;
            }
            if let Some(el) = else_result {
                subst_upsert_proposed(el, proposed)?;
            }
        }
        SqlExpr::Cast { expr, .. } => subst_upsert_proposed(expr, proposed)?,
        SqlExpr::IsNull(inner) | SqlExpr::IsNotNull(inner) => {
            subst_upsert_proposed(inner, proposed)?
        }
        SqlExpr::Floor { expr, .. } | SqlExpr::Ceil { expr, .. } => {
            subst_upsert_proposed(expr, proposed)?
        }
        SqlExpr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            subst_upsert_proposed(expr, proposed)?;
            if let Some(f) = substring_from {
                subst_upsert_proposed(f, proposed)?;
            }
            if let Some(f) = substring_for {
                subst_upsert_proposed(f, proposed)?;
            }
        }
        SqlExpr::Trim {
            expr,
            trim_what,
            trim_characters,
            ..
        } => {
            subst_upsert_proposed(expr, proposed)?;
            if let Some(w) = trim_what {
                subst_upsert_proposed(w, proposed)?;
            }
            if let Some(chars) = trim_characters {
                for c in chars {
                    subst_upsert_proposed(c, proposed)?;
                }
            }
        }
        SqlExpr::Position { expr, r#in } => {
            subst_upsert_proposed(expr, proposed)?;
            subst_upsert_proposed(r#in, proposed)?;
        }
        SqlExpr::Overlay {
            expr,
            overlay_what,
            overlay_from,
            overlay_for,
        } => {
            subst_upsert_proposed(expr, proposed)?;
            subst_upsert_proposed(overlay_what, proposed)?;
            subst_upsert_proposed(overlay_from, proposed)?;
            if let Some(f) = overlay_for {
                subst_upsert_proposed(f, proposed)?;
            }
        }
        SqlExpr::Tuple(items) => {
            for item in items {
                subst_upsert_proposed(item, proposed)?;
            }
        }
        SqlExpr::Function(f) => {
            if let sqlparser::ast::FunctionArguments::List(list) = &mut f.args {
                for a in &mut list.args {
                    if let sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(inner),
                    ) = a
                    {
                        subst_upsert_proposed(inner, proposed)?;
                    }
                }
            }
            if let Some(filter) = &mut f.filter {
                subst_upsert_proposed(filter, proposed)?;
            }
        }
        // Subquery bodies stay untouched (checked by the caller).
        SqlExpr::InSubquery { expr, .. } => subst_upsert_proposed(expr, proposed)?,
        SqlExpr::AnyOp { left, .. } | SqlExpr::AllOp { left, .. } => {
            subst_upsert_proposed(left, proposed)?
        }
        _ => {}
    }
    Ok(())
}

/// `VALUES(col)` — the MySQL proposed-row reference, parsed as a function.
/// Recognizes exactly `VALUES(<single column>)`; anything else is a normal
/// function (and stays one).
fn is_values_call(f: &sqlparser::ast::Function) -> bool {
    f.name.0.last().is_some_and(|p| {
        p.as_ident()
            .is_some_and(|i| i.value.eq_ignore_ascii_case("VALUES"))
    }) && matches!(
        &f.args,
        sqlparser::ast::FunctionArguments::List(list)
            if list.args.len() == 1
                && matches!(
                    list.args.first(),
                    Some(sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(
                            SqlExpr::Identifier(_)
                        )
                    ))
                )
    )
}

fn values_call_column(f: &sqlparser::ast::Function) -> String {
    match &f.args {
        sqlparser::ast::FunctionArguments::List(list) if list.args.len() == 1 => {
            match list.args.first() {
                Some(sqlparser::ast::FunctionArg::Unnamed(
                    sqlparser::ast::FunctionArgExpr::Expr(SqlExpr::Identifier(i)),
                )) => i.value.clone(),
                _ => String::new(),
            }
        }
        _ => String::new(),
    }
}

/// True when a DO UPDATE SET/WHERE tree still references the proposed row
/// AFTER the direct-tree substitution — i.e. inside a subquery body, which
/// the driver does not rewrite. Subquery bodies are probed textually
/// (uppercased rendering); a false positive from a string literal is a
/// loud error, never a silent mis-resolution.
fn upsert_proposed_in_subquery(e: &SqlExpr) -> bool {
    let subquery_text = |q: &sqlparser::ast::Query| -> bool {
        let t = q.to_string().to_ascii_uppercase();
        t.contains("EXCLUDED") || t.contains("VALUES(")
    };
    match e {
        SqlExpr::Subquery(q) => subquery_text(q),
        SqlExpr::Exists { subquery: q, .. } => subquery_text(q),
        SqlExpr::InSubquery { expr, subquery, .. } => {
            upsert_proposed_in_subquery(expr) || subquery_text(subquery)
        }
        SqlExpr::AnyOp { left, right, .. } | SqlExpr::AllOp { left, right, .. } => {
            upsert_proposed_in_subquery(left) || upsert_proposed_in_subquery(right)
        }
        SqlExpr::BinaryOp { left, right, .. } => {
            upsert_proposed_in_subquery(left) || upsert_proposed_in_subquery(right)
        }
        SqlExpr::UnaryOp { expr, .. } => upsert_proposed_in_subquery(expr),
        SqlExpr::Nested(inner) => upsert_proposed_in_subquery(inner),
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            upsert_proposed_in_subquery(expr)
                || upsert_proposed_in_subquery(low)
                || upsert_proposed_in_subquery(high)
        }
        SqlExpr::IsDistinctFrom(l, r) | SqlExpr::IsNotDistinctFrom(l, r) => {
            upsert_proposed_in_subquery(l) || upsert_proposed_in_subquery(r)
        }
        SqlExpr::Like { expr, pattern, .. } | SqlExpr::ILike { expr, pattern, .. } => {
            upsert_proposed_in_subquery(expr) || upsert_proposed_in_subquery(pattern)
        }
        SqlExpr::InList { expr, list, .. } => {
            upsert_proposed_in_subquery(expr) || list.iter().any(upsert_proposed_in_subquery)
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand
                .as_ref()
                .is_some_and(|x| upsert_proposed_in_subquery(x))
                || conditions.iter().any(|w| {
                    upsert_proposed_in_subquery(&w.condition)
                        || upsert_proposed_in_subquery(&w.result)
                })
                || else_result
                    .as_ref()
                    .is_some_and(|x| upsert_proposed_in_subquery(x))
        }
        SqlExpr::Cast { expr, .. } => upsert_proposed_in_subquery(expr),
        SqlExpr::Function(f) => {
            let args_hit = match &f.args {
                sqlparser::ast::FunctionArguments::List(list) => list.args.iter().any(|a| {
                    matches!(
                        a,
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(inner)
                        ) if upsert_proposed_in_subquery(inner)
                    )
                }),
                _ => false,
            };
            args_hit
                || f.filter
                    .as_ref()
                    .is_some_and(|x| upsert_proposed_in_subquery(x))
        }
        _ => false,
    }
}

/// ROWNUM guard for the DO UPDATE arm (same rule as UPDATE): assignments
/// and the WHERE predicate see no row stream to number.
fn refs_rownum_in(assignments: &[sqlparser::ast::Assignment], selection: &Option<SqlExpr>) -> bool {
    assignments.iter().any(|a| refs_rownum(&a.value)) || selection.as_ref().is_some_and(refs_rownum)
}

/// Column-restricted grant support: for a single-table SELECT (no joins,
/// no set operations, no WITH, no table functions) return the table name
/// plus every column the statement references. `None` = the shape cannot
/// be attributed (wildcards, subqueries whose columns would need deeper
/// analysis, exotic clauses) — callers must treat None as fail-closed and
/// refuse the statement for column-restricted grants.
pub fn single_table_select_refs(stmt: &Statement) -> Option<(String, Vec<String>)> {
    let Statement::Query(q) = stmt else {
        return None;
    };
    single_table_query_refs(q)
}

/// [`single_table_select_refs`] over a bare query — the INSERT ... SELECT
/// source (used by the write path's column-grant attribution).
pub fn single_table_query_refs(q: &Query) -> Option<(String, Vec<String>)> {
    use sqlparser::ast::{GroupByExpr, OrderByKind, SelectItem, SetExpr, TableFactor};
    if q.with.is_some() {
        return None;
    }
    // LIMIT/OFFSET/FETCH reference no columns; ORDER BY may.
    let SetExpr::Select(select) = q.body.as_ref() else {
        return None;
    };
    if select.from.len() != 1 {
        return None;
    }
    let twj = &select.from[0];
    if !twj.joins.is_empty() {
        return None;
    }
    let sqlparser::ast::TableWithJoins { relation, .. } = twj;
    let TableFactor::Table {
        name,
        alias,
        args: None,
        ..
    } = relation
    else {
        return None;
    };
    if alias.is_some() {
        // An alias renames the row namespace; a column-restricted grant
        // qualifies only the plain single-table spelling.
        return None;
    }
    // A wildcard function argument (COUNT(*)) reads every column of every
    // row — as unattributable as `SELECT *`.
    fn expr_has_star(e: &SqlExpr) -> bool {
        use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
        match e {
            SqlExpr::Function(f) => match &f.args {
                FunctionArguments::List(list) => list.args.iter().any(|a| {
                    matches!(a, FunctionArg::Unnamed(FunctionArgExpr::Wildcard))
                        || matches!(
                            a,
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(inner))
                                if expr_has_star(inner)
                        )
                }),
                _ => false,
            },
            SqlExpr::BinaryOp { left, right, .. } => expr_has_star(left) || expr_has_star(right),
            SqlExpr::UnaryOp { expr, .. } => expr_has_star(expr),
            SqlExpr::Nested(inner) => expr_has_star(inner),
            _ => false,
        }
    }
    let table = obj_name(name);
    let mut out = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(e) => {
                if expr_has_star(e) {
                    return None;
                }
                column_refs(e, &mut out)?;
            }
            SelectItem::ExprWithAlias { expr, .. } => {
                if expr_has_star(expr) {
                    return None;
                }
                column_refs(expr, &mut out)?;
            }
            // `*` / `t.*` read every column: not attributable to a subset.
            _ => return None,
        }
    }
    if let Some(sel) = &select.selection {
        column_refs(sel, &mut out)?;
    }
    match &select.group_by {
        GroupByExpr::Expressions(exprs, _) => {
            for e in exprs {
                column_refs(e, &mut out)?;
            }
        }
        GroupByExpr::All(_) => return None,
    }
    if let Some(having) = &select.having {
        column_refs(having, &mut out)?;
    }
    if let Some(ob) = &q.order_by {
        if let OrderByKind::Expressions(exprs) = &ob.kind {
            for oe in exprs {
                column_refs(&oe.expr, &mut out)?;
            }
        }
    }
    if let Some(limit) = &q.limit_clause {
        // LIMIT/OFFSET carry no column refs; ANY other decoration (LOCK
        // etc.) is unsupported-shape territory.
        let _ = limit;
    }
    Some((table, out))
}

/// Map a B+ tree error to a SQL error; duplicate hits become the familiar
/// constraint-failure message.
/// Human spelling of a join operator for error messages (sqlparser's
/// `JoinOperator` implements no Display).
fn join_op_name(op: &sqlparser::ast::JoinOperator) -> &'static str {
    use sqlparser::ast::JoinOperator as J;
    match op {
        J::Join(_) => "JOIN",
        J::Inner(_) => "INNER JOIN",
        J::Left(_) => "LEFT JOIN",
        J::LeftOuter(_) => "LEFT OUTER JOIN",
        J::Right(_) => "RIGHT JOIN",
        J::RightOuter(_) => "RIGHT OUTER JOIN",
        J::FullOuter(_) => "FULL OUTER JOIN",
        J::CrossJoin(_) => "CROSS JOIN",
        J::Semi(_) => "SEMI JOIN",
        J::LeftSemi(_) => "LEFT SEMI JOIN",
        J::RightSemi(_) => "RIGHT SEMI JOIN",
        J::Anti(_) => "ANTI JOIN",
        J::LeftAnti(_) => "LEFT ANTI JOIN",
        J::RightAnti(_) => "RIGHT ANTI JOIN",
        J::CrossApply => "CROSS APPLY",
        J::OuterApply => "OUTER APPLY",
        J::AsOf { .. } => "ASOF JOIN",
        J::StraightJoin(_) => "STRAIGHT_JOIN",
        J::ArrayJoin => "ARRAY JOIN",
        J::LeftArrayJoin => "LEFT ARRAY JOIN",
        J::InnerArrayJoin => "INNER ARRAY JOIN",
    }
}

/// Composite aggregate projections may reference columns outside the
/// aggregate arguments only when they are GROUP BY expressions — the
/// bare-column rule of `agg_spec`, applied inside nested expressions.
/// Constructs without explicit support reject loudly rather than silently
/// evaluating columns to NULL.
fn check_group_refs(e: &SqlExpr, group_exprs: &[SqlExpr]) -> Result<()> {
    match e {
        SqlExpr::Function(f) if is_agg_fn(f) => Ok(()), // per-row inside eval_agg
        SqlExpr::Function(f) => {
            if let sqlparser::ast::FunctionArguments::List(list) = &f.args {
                for a in &list.args {
                    if let sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(inner),
                    ) = a
                    {
                        check_group_refs(inner, group_exprs)?;
                    }
                }
            }
            Ok(())
        }
        SqlExpr::Nested(inner) => check_group_refs(inner, group_exprs),
        SqlExpr::BinaryOp { left, right, .. } => {
            check_group_refs(left, group_exprs)?;
            check_group_refs(right, group_exprs)
        }
        SqlExpr::UnaryOp { expr, .. } => check_group_refs(expr, group_exprs),
        SqlExpr::Cast { expr, .. } => check_group_refs(expr, group_exprs),
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(o) = operand {
                check_group_refs(o, group_exprs)?;
            }
            for w in conditions {
                check_group_refs(&w.condition, group_exprs)?;
                check_group_refs(&w.result, group_exprs)?;
            }
            if let Some(r) = else_result {
                check_group_refs(r, group_exprs)?;
            }
            Ok(())
        }
        SqlExpr::Identifier(i) => {
            if group_exprs
                .iter()
                .any(|g| matches!(g, SqlExpr::Identifier(gi) if gi.value == i.value))
            {
                Ok(())
            } else {
                err(format!(
                    "column {} must appear in GROUP BY or an aggregate",
                    i.value
                ))
            }
        }
        SqlExpr::CompoundIdentifier(parts) => {
            let name = parts
                .iter()
                .map(|p| p.value.clone())
                .collect::<Vec<_>>()
                .join(".");
            if group_exprs.iter().any(|g| expr_name(g) == name) {
                Ok(())
            } else {
                err(format!(
                    "column {name} must appear in GROUP BY or an aggregate"
                ))
            }
        }
        SqlExpr::Value(_) => Ok(()),
        other => err(format!("unsupported aggregate expression: {other}")),
    }
}

/// HAVING variant of [`check_group_refs`]: a bare identifier may also be one
/// of the projection's output names (aliases resolve through the output row
/// during evaluation). Without this check a column that is neither grouped
/// nor aliased silently read NULL and the predicate dropped every group.
fn check_having_refs(e: &SqlExpr, group_exprs: &[SqlExpr], out_columns: &[String]) -> Result<()> {
    match e {
        SqlExpr::Function(f) if is_agg_fn(f) => Ok(()),
        SqlExpr::Function(f) => {
            if let sqlparser::ast::FunctionArguments::List(list) = &f.args {
                for a in &list.args {
                    if let sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(inner),
                    ) = a
                    {
                        check_having_refs(inner, group_exprs, out_columns)?;
                    }
                }
            }
            Ok(())
        }
        SqlExpr::Nested(inner) => check_having_refs(inner, group_exprs, out_columns),
        // Common predicate shapes the row-local evaluator (eval_group_expr's
        // fallback) already handles; without them `HAVING k IS NULL` — the
        // idiomatic ROLLUP total-row filter — was rejected as "unsupported".
        SqlExpr::IsNull(inner) => check_having_refs(inner, group_exprs, out_columns),
        SqlExpr::IsNotNull(inner) => check_having_refs(inner, group_exprs, out_columns),
        SqlExpr::IsDistinctFrom(a, b) | SqlExpr::IsNotDistinctFrom(a, b) => {
            check_having_refs(a, group_exprs, out_columns)?;
            check_having_refs(b, group_exprs, out_columns)
        }
        SqlExpr::Like { expr, pattern, .. } | SqlExpr::ILike { expr, pattern, .. } => {
            check_having_refs(expr, group_exprs, out_columns)?;
            check_having_refs(pattern, group_exprs, out_columns)
        }
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            check_having_refs(expr, group_exprs, out_columns)?;
            check_having_refs(low, group_exprs, out_columns)?;
            check_having_refs(high, group_exprs, out_columns)
        }
        SqlExpr::InList { expr, list, .. } => {
            check_having_refs(expr, group_exprs, out_columns)?;
            for item in list {
                check_having_refs(item, group_exprs, out_columns)?;
            }
            Ok(())
        }
        SqlExpr::BinaryOp { left, right, .. } => {
            check_having_refs(left, group_exprs, out_columns)?;
            check_having_refs(right, group_exprs, out_columns)
        }
        SqlExpr::UnaryOp { expr, .. } => check_having_refs(expr, group_exprs, out_columns),
        SqlExpr::Cast { expr, .. } => check_having_refs(expr, group_exprs, out_columns),
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(o) = operand {
                check_having_refs(o, group_exprs, out_columns)?;
            }
            for w in conditions {
                check_having_refs(&w.condition, group_exprs, out_columns)?;
                check_having_refs(&w.result, group_exprs, out_columns)?;
            }
            if let Some(r) = else_result {
                check_having_refs(r, group_exprs, out_columns)?;
            }
            Ok(())
        }
        SqlExpr::Identifier(i) => {
            if out_columns.iter().any(|c| c == &i.value)
                || group_exprs
                    .iter()
                    .any(|g| matches!(g, SqlExpr::Identifier(gi) if gi.value == i.value))
            {
                Ok(())
            } else {
                err(format!(
                    "column {} must appear in GROUP BY, an aggregate, or the SELECT list",
                    i.value
                ))
            }
        }
        SqlExpr::CompoundIdentifier(parts) => {
            let name = parts
                .iter()
                .map(|p| p.value.clone())
                .collect::<Vec<_>>()
                .join(".");
            if out_columns.iter().any(|c| c == &name)
                || group_exprs.iter().any(|g| expr_name(g) == name)
            {
                Ok(())
            } else {
                err(format!(
                    "column {name} must appear in GROUP BY, an aggregate, or the SELECT list"
                ))
            }
        }
        SqlExpr::Value(_) => Ok(()),
        other => err(format!("unsupported aggregate expression: {other}")),
    }
}

/// Group-aware expression evaluation (HAVING and composite aggregate
/// projections): aggregate subcalls run over the group's rows, GROUP BY
/// expressions resolve to the group key (via the group's rows — every row
/// shares the key value), and everything else evaluates against `out` —
/// the projected output row for HAVING, empty for projection items (where
/// `check_group_refs` has already rejected bare non-group columns).
fn eval_group_expr(
    e: &SqlExpr,
    out: &Object,
    docs: &[&Object],
    group_exprs: &[SqlExpr],
) -> Result<Value> {
    match e {
        SqlExpr::Function(f) if is_agg_fn(f) => {
            let (op, arg, distinct, sep, filter) = agg_parts(f, false)?;
            eval_agg(
                &AggSpec::Agg {
                    op,
                    arg,
                    distinct,
                    sep,
                    filter,
                },
                docs,
                &[],
                group_exprs,
            )
        }
        SqlExpr::Function(f) => {
            // Scalar function whose arguments may contain aggregates
            // (`COALESCE(SUM(x), 0)`).
            let name = f.name.to_string().to_uppercase();
            if f.over.is_some() {
                return err(format!("window functions (OVER) are not supported: {name}"));
            }
            let mut args = Vec::new();
            if let sqlparser::ast::FunctionArguments::List(list) = &f.args {
                for a in &list.args {
                    match a {
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(inner),
                        ) => args.push(eval_group_expr(inner, out, docs, group_exprs)?),
                        _ => return err(format!("unsupported argument to {name}")),
                    }
                }
            }
            scalar_function(&name, &args)
        }
        SqlExpr::Identifier(i) => {
            if let Some(g) = group_exprs
                .iter()
                .find(|g| matches!(g, SqlExpr::Identifier(gi) if gi.value == i.value))
            {
                return match docs.first() {
                    Some(d) => eval_expr(g, d),
                    None => Ok(Value::Null),
                };
            }
            eval_expr(e, out)
        }
        SqlExpr::CompoundIdentifier(parts) => {
            let name = parts
                .iter()
                .map(|p| p.value.clone())
                .collect::<Vec<_>>()
                .join(".");
            if let Some(g) = group_exprs.iter().find(|g| expr_name(g) == name) {
                return match docs.first() {
                    Some(d) => eval_expr(g, d),
                    None => Ok(Value::Null),
                };
            }
            eval_expr(e, out)
        }
        SqlExpr::BinaryOp { left, op, right } => {
            let l = eval_group_expr(left, out, docs, group_exprs)?;
            let r = eval_group_expr(right, out, docs, group_exprs)?;
            binop(l, op, r)
        }
        SqlExpr::UnaryOp { op, expr } => {
            let v = eval_group_expr(expr, out, docs, group_exprs)?;
            match (op, v) {
                (sqlparser::ast::UnaryOperator::Minus, Value::Int(i)) => match i.checked_neg() {
                    Some(n) => Ok(Value::Int(n)),
                    // -i64::MIN overflows; mirror literal handling and keep
                    // the value instead of panicking in debug builds.
                    None => Ok(Value::Float(-(i as f64))),
                },
                (sqlparser::ast::UnaryOperator::Minus, Value::Float(f)) => Ok(Value::Float(-f)),
                (sqlparser::ast::UnaryOperator::Minus, Value::Decimal(d)) => {
                    // Signed-zero guard: decimal text cannot replay -0
                    // ("−0.00" parses back to +0), so the dump/replica
                    // rewrite would silently flip the encoded bytes.
                    let neg = -d;
                    Ok(Value::Decimal(if neg.is_zero() && neg.is_sign_negative() {
                        rust_decimal::Decimal::ZERO
                    } else {
                        neg
                    }))
                }
                (sqlparser::ast::UnaryOperator::Not, Value::Bool(b)) => Ok(Value::Bool(!b)),
                (sqlparser::ast::UnaryOperator::Not, Value::Null) => Ok(Value::Null),
                // T-SQL bitwise NOT (~x): two's-complement on 64-bit ints.
                (sqlparser::ast::UnaryOperator::BitwiseNot, Value::Int(i)) => Ok(Value::Int(!i)),
                _ => err("unsupported unary operand"),
            }
        }
        SqlExpr::Nested(inner) => eval_group_expr(inner, out, docs, group_exprs),
        SqlExpr::Cast {
            kind,
            expr,
            data_type,
            ..
        } => {
            let v = eval_group_expr(expr, out, docs, group_exprs)?;
            let out = cast_value(v, &data_type.to_string());
            // T-SQL TRY_CAST: a failed conversion yields NULL, not an error.
            if matches!(kind, sqlparser::ast::CastKind::TryCast) {
                out.or(Ok(Value::Null))
            } else {
                out
            }
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            let base = operand
                .as_ref()
                .map(|o| eval_group_expr(o, out, docs, group_exprs))
                .transpose()?;
            for w in conditions {
                let hit = match &base {
                    Some(b) => {
                        let cv = eval_group_expr(&w.condition, out, docs, group_exprs)?;
                        // Same coerced equality as the scalar CASE arm
                        // (Str ↔ Timestamp lifts etc.); a bare cmp_values
                        // here made grouped CASE disagree with ungrouped.
                        !matches!(b, Value::Null)
                            && !matches!(cv, Value::Null)
                            && cmp_coerced(b, &cv) == Some(Ordering::Equal)
                    }
                    None => matches!(
                        eval_group_expr(&w.condition, out, docs, group_exprs)?,
                        Value::Bool(true)
                    ),
                };
                if hit {
                    return eval_group_expr(&w.result, out, docs, group_exprs);
                }
            }
            match else_result {
                Some(r) => eval_group_expr(r, out, docs, group_exprs),
                None => Ok(Value::Null),
            }
        }
        other => eval_expr(other, out),
    }
}

/// `cmp_values` behind the predicate-level TIMESTAMP↔string rule
/// ([`coerce_timestamp_operands`]): BETWEEN, IN, CASE and IS [NOT] DISTINCT
/// FROM must compare exactly like `=`/`<`/`>` do. `None` mirrors the
/// comparison-unknown result those operators report for an unparseable
/// string against a TIMESTAMP.
/// The full CREATE TABLE text for one catalog entry (columns, constraints)
/// — shared by dump_script and the sqlite_master table rows, so clients
/// (the EF keep-data rebuild) see the same constraint set the dump would
/// re-create.
fn create_table_ddl(name: &str, meta: &TableMeta) -> String {
    let mut ddl = format!("CREATE TABLE {} (\n", quote_ident(name));
    let mut parts: Vec<String> = Vec::new();
    for col in &meta.columns {
        let mut def = quote_ident(col);
        if meta.autoguid.as_deref() == Some(col.as_str()) {
            def.push_str(" GUID AUTOINCREMENT");
        } else if meta.autoinc.as_deref() == Some(col.as_str()) {
            def.push_str(" INT AUTOINCREMENT");
        } else {
            // Declared types are not enforced (document storage);
            // TEXT round-trips the column shape without pretending
            // to preserve the original declaration.
            def.push_str(" TEXT");
        }
        if meta.primary_key.as_deref() == Some(col.as_str()) {
            def.push_str(" PRIMARY KEY");
        }
        if meta.constraint_unique.contains(col) {
            def.push_str(" UNIQUE");
        }
        if meta.not_null.contains(col) {
            def.push_str(" NOT NULL");
        }
        if let Some((_, expr)) = meta.defaults.iter().find(|(c, _)| c == col) {
            def.push_str(&format!(" DEFAULT ({expr})"));
        }
        parts.push(def);
    }
    for chk in &meta.checks {
        parts.push(format!("CHECK ({chk})"));
    }
    for (lc, rt, rc) in &meta.foreign_keys {
        parts.push(format!(
            "FOREIGN KEY ({}) REFERENCES {} ({})",
            quote_ident(lc),
            quote_ident(rt),
            quote_ident(rc)
        ));
    }
    ddl.push_str(&parts.join(",\n"));
    ddl.push_str("\n);");
    ddl
}

/// Rows of a single-column value-list subquery (scalar / IN / ANY / ALL):
/// more than one output column is a cardinality error. The old silent
/// `row.first()` read only the first column and dropped visible matches
/// from IN lists.
fn single_col_rows(r: &QueryResult) -> Result<()> {
    if r.columns.len() > 1 {
        return err(format!(
            "subquery returns {} columns; a single column is required here",
            r.columns.len()
        ));
    }
    Ok(())
}

fn union_of_fields(docs: &[Object]) -> Vec<String> {
    let mut seen: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for d in docs {
        for k in d.keys() {
            seen.insert(k.clone());
        }
    }
    seen.into_iter().collect()
}

#[cfg(test)]
mod complex_query_tests;
#[cfg(test)]
mod matview_tests;
#[cfg(test)]
mod partial_index_tests;
#[cfg(test)]
mod read_view_tests;
#[cfg(test)]
mod tests;
/// End-to-end T-SQL compatibility: everything `tsql.rs` promises, driven
/// through the public `execute` path (preprocess → parse → executor).
#[cfg(test)]
mod tsql_compat_tests;
#[cfg(test)]
mod tx_rollback_tests;
#[cfg(test)]
mod window_tests;

mod read_cx;
mod types;
pub use read_cx::ReadView;
pub(crate) use read_cx::*;
pub use types::*;

mod agg;
mod expr;
mod join;
mod probe;
mod subst;
mod targets;
mod trigger;
mod window;
use agg::*;
pub use expr::*;
use join::*;
use probe::*;
use subst::*;
use targets::*;
use trigger::*;
use window::*;
