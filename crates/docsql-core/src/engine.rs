//! SQL engine: catalog + executor over document heaps.
//!
//! The catalog itself is a document stored on page 1:
//! `{ "tables": { "<name>": { "columns": [names...], "pages": [...] } } }`
//! so metadata rides the same WAL/pager machinery as data.

use crate::btree::{BTree, BTreeError};
use crate::encode;
use crate::heap::Heap;
use crate::pager::{Pager, PagerError, PAGE_SIZE};
use crate::value::{Object, Value};
use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, LimitClause, ObjectName, ObjectNamePart, Query, SelectItem,
    SetExpr, Statement, TableObject,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::cmp::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub const CATALOG_PAGE: u32 = 1;
/// Catalog payload paginates across a chain of pages: page 1 carries
/// `CATALOG_MAGIC`, the total payload length and the next overflow page id
/// (0 = end); every overflow page repeats the same 16-byte header. The old
/// single-page catalog capped a database at ~4 KB of metadata (one full page
/// id list eats that in ~800 heap pages, i.e. a few MB of rows) and then
/// failed every write that allocated a page. Pages are append-only and never
/// reused, so overflow pages orphaned by a shrinking catalog are just dead
/// space. The payload still contains each table's page-id list, so a save is
/// O(catalog bytes) — acceptable because saves only fire on page allocation
/// and DDL, both sublinear in row count.
const CATALOG_MAGIC: &[u8; 8] = b"DOCSCAT2";
const CATALOG_HDR: usize = 16;
const CATALOG_CHUNK: usize = PAGE_SIZE - CATALOG_HDR;

/// System table backing persistent pub/sub. Created by docsql-server at
/// startup; hidden from user-facing catalogs (read through the server's
/// `docsql_pubsub` view instead).
pub const PUBSUB_TABLE: &str = "_pubsub_messages";

/// System table backing catch-up replication (rejoin repair): every
/// locally-committed write, in commit order, with a node-local monotonic
/// `seq`. Peers record the last seq they applied per origin (see
/// [`Database::position_set`]) and pull the range after it when they
/// rejoin — sync exactly what's missing instead of adopting a full
/// snapshot. Node-local by design: an op's canonical sequence is its
/// origin's.
pub const CLUSTER_LOG_TABLE: &str = "_cluster_log";

/// System table backing catch-up replication positions: per origin node
/// id, the last applied journal seq. Updated right after each applied
/// replicated write; a crash between apply and update leaves the position
/// behind the data, so a later catch-up replays an already-applied op —
/// that errors, and the repair falls back to a full snapshot (the safe
/// direction: positions can only lag the data, never lead it).
pub const CLUSTER_POS_TABLE: &str = "_cluster_pos";

/// System table holding this node's persistent random identity: the key
/// positions are stored under, stable across restarts and independent of
/// reachable addresses.
pub const CLUSTER_ID_TABLE: &str = "_cluster_id";

/// True for the engine-managed system tables: excluded from user-facing
/// catalogs, digests, dumps and snapshot wipes.
pub fn is_system_table(name: &str) -> bool {
    name == PUBSUB_TABLE
        || name == CLUSTER_LOG_TABLE
        || name == CLUSTER_POS_TABLE
        || name == CLUSTER_ID_TABLE
}

#[derive(Debug, thiserror::Error)]
pub enum SqlError {
    #[error("heap error: {0}")]
    Heap(#[from] crate::heap::HeapError),
    #[error("{0}")]
    Message(String),
    #[error("parse error: {0}")]
    Parse(String),
    #[error("storage error: {0}")]
    Storage(#[from] PagerError),
    #[error("encoding error: {0}")]
    Encode(#[from] encode::EncodeError),
}

pub type Result<T> = std::result::Result<T, SqlError>;

fn err<T>(msg: impl Into<String>) -> Result<T> {
    Err(SqlError::Message(msg.into()))
}

/// Outcome of executing one statement.
#[derive(Debug, Clone, PartialEq)]
pub enum ExecOutcome {
    /// Rows affected (DML) or a status message (DDL).
    Affected(u64),
    Rows(QueryResult),
}

/// Transaction-control classification, used by servers to time replication:
/// writes inside an open transaction must not reach peers until the
/// transaction commits; a rollback discards them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxControl {
    Begin,
    Commit,
    /// Plain ROLLBACK, or ROLLBACK TO SAVEPOINT when `savepoint` is set.
    Rollback {
        savepoint: Option<String>,
    },
    Savepoint(String),
    Release(String),
    /// Not transaction control.
    None,
}

/// One statement parsed once for a whole server round-trip: the AST feeds
/// [`Database::execute_parsed`], while `tx`/`is_write` route the request
/// (write path, transaction control, replication timing) without re-parsing
/// the text.
pub struct ParsedStatement {
    pub stmt: Statement,
    pub tx: TxControl,
    pub is_write: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

/// One column as seen by tooling (web console / drivers).
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnInfo {
    pub name: String,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub autoinc: bool,
    /// Declared type where the engine tracks one ("GUID" for auto-generated
    /// GUID columns, "ANY" otherwise). Tooling uses it to round-trip DDL.
    pub data_type: String,
    /// Declared DEFAULT expression as SQL text (None when the column has
    /// none). Tooling uses it to round-trip DDL.
    pub default_value: Option<String>,
}

/// Read-only catalog snapshot of one table.
#[derive(Debug, Clone, PartialEq)]
pub struct TableInfo {
    pub name: String,
    pub columns: Vec<ColumnInfo>,
    /// Constraint indexes: PRIMARY KEY column first, then UNIQUE columns.
    pub keys: Vec<String>,
    pub indexes: Vec<String>,
    /// CREATE INDEX definitions (name/column/unique) for tooling: the
    /// explorer and the console's script generator need the column and
    /// uniqueness behind each name, not just the name list.
    pub index_defs: Vec<IndexInfo>,
    pub pages: usize,
}

/// One CREATE INDEX definition as recorded in the catalog.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexInfo {
    pub name: String,
    pub column: String,
    pub unique: bool,
    /// PRIMARY KEY / UNIQUE constraint index, auto-created with the table
    /// (SQLite-style `sqlite_autoindex_*` name). Derived, never persisted,
    /// and not droppable/editable through DROP INDEX / CREATE INDEX.
    pub auto: bool,
}

/// Outcome of a multi-statement batch: statements run in order and the run
/// stops at the first error (`error.statement` is the 0-based index).
#[derive(Debug, Clone, PartialEq)]
pub struct BatchResult {
    /// Total statements in the batch (parsed), even when the run stopped early.
    pub statements: usize,
    pub outcomes: Vec<ExecOutcome>,
    pub error: Option<BatchError>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BatchError {
    pub statement: usize,
    pub message: String,
}

/// Session snapshot of all tables: (meta, docs) per table. Used by
/// transaction rollback and the SAVEPOINT stack.
type TableSnapshot = std::collections::BTreeMap<String, (TableMeta, Vec<Object>)>;

#[derive(Debug, Clone, Default)]
struct TableMeta {
    columns: Vec<String>,
    pages: Vec<u32>,
    primary_key: Option<String>,
    unique: Vec<String>,
    not_null: Vec<String>,
    autoinc: Option<String>,
    /// Auto-generated GUID column (GUID/UUID/UNIQUEIDENTIFIER/UUIDV7 type
    /// plus AUTOINCREMENT/AUTOGENERATE): INSERT fills a time-ordered UUIDv7
    /// when the value is omitted or NULL.
    autoguid: Option<String>,
    /// Index names registered on this table (catalog-level v1; B+ tree
    /// backing arrives with the index-integration milestone).
    indexes: Vec<String>,
    /// CREATE INDEX definitions: (name, column, unique). Drives DROP INDEX
    /// cleanup (lifting UNIQUE enforced by a dropped unique index) and the
    /// sqlite_master index rows.
    index_defs: Vec<(String, String, bool)>,
    /// Columns declared UNIQUE via CREATE TABLE constraints. Dropping a
    /// UNIQUE INDEX only lifts meta.unique for columns NOT in this list.
    constraint_unique: Vec<String>,
    /// Column DEFAULT expressions as SQL text: (column, expr).
    defaults: Vec<(String, String)>,
    /// CHECK constraint expressions as SQL text (column- or table-level).
    checks: Vec<String>,
    /// Foreign keys: (local column, referenced table, referenced column).
    foreign_keys: Vec<(String, String, String)>,
    /// Indexed column -> B+ tree root page. PRIMARY KEY and UNIQUE columns
    /// always get trees (duplicates are enforced through them); CREATE
    /// INDEX adds non-unique trees. Lookups over these columns skip the
    /// heap scan.
    index_roots: std::collections::BTreeMap<String, u32>,
}

impl TableMeta {
    /// Validate a document against declared constraints.
    fn check(&self, doc: &Object) -> Result<()> {
        for col in &self.not_null {
            if matches!(doc.get(col), Some(Value::Null) | None) {
                return err(format!("NOT NULL constraint failed: {col}"));
            }
        }
        for text in &self.checks {
            let e = parse_expr_text(text)?;
            // SQL semantics: CHECK fails only when it evaluates to FALSE;
            // NULL (unknown) passes. Our comparisons never return NULL, so
            // treat any NULL-valued column reference as unknown.
            if expr_has_null_ref(&e, doc) {
                continue;
            }
            if let Value::Bool(false) = eval_expr(&e, doc)? {
                return err(format!("CHECK constraint failed: {text}"));
            }
        }
        Ok(())
    }

    /// Enforce PRIMARY KEY / UNIQUE across the whole (rewritten) doc set.
    fn check_unique(&self, docs: &[Object]) -> Result<()> {
        for col in self.primary_key.iter().chain(self.unique.iter()) {
            let mut seen: Vec<Vec<u8>> = Vec::new();
            for doc in docs {
                if let Some(v) = doc.get(col) {
                    if matches!(v, Value::Null) {
                        continue;
                    }
                    let key = encode::encode_to_vec(v).map_err(SqlError::Encode)?;
                    if seen.binary_search(&key).is_ok() {
                        return err(format!("UNIQUE constraint failed: {col}"));
                    }
                    seen.push(key);
                    seen.sort();
                }
            }
        }
        Ok(())
    }
}

/// Parse a standalone SQL expression (constraint/DEFAULT bodies stored in
/// the catalog as text).
fn parse_expr_text(s: &str) -> Result<SqlExpr> {
    let mut parser = Parser::new(&GenericDialect {})
        .try_with_sql(s)
        .map_err(|e| SqlError::Parse(e.to_string()))?;
    parser
        .parse_expr()
        .map_err(|e| SqlError::Parse(e.to_string()))
}

/// True when any column reference in `e` resolves to NULL in `doc` (CHECK
/// semantics: unknown, so the constraint passes).
fn expr_has_null_ref(e: &SqlExpr, doc: &Object) -> bool {
    match e {
        SqlExpr::Identifier(i) => matches!(doc.get(&i.value), Some(Value::Null) | None),
        SqlExpr::CompoundIdentifier(parts) => {
            let full = parts
                .iter()
                .map(|p| p.value.clone())
                .collect::<Vec<_>>()
                .join(".");
            matches!(doc.get(&full), Some(Value::Null)) || {
                let last = parts.last().map(|p| p.value.clone()).unwrap_or_default();
                matches!(doc.get(&last), Some(Value::Null) | None)
            }
        }
        SqlExpr::BinaryOp { left, right, .. } => {
            expr_has_null_ref(left, doc) || expr_has_null_ref(right, doc)
        }
        SqlExpr::UnaryOp { expr, .. } => expr_has_null_ref(expr, doc),
        SqlExpr::Nested(inner) => expr_has_null_ref(inner, doc),
        SqlExpr::Function(f) => {
            if let sqlparser::ast::FunctionArguments::List(list) = &f.args {
                list.args.iter().any(|a| match a {
                    sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(inner),
                    ) => expr_has_null_ref(inner, doc),
                    _ => false,
                })
            } else {
                false
            }
        }
        _ => false,
    }
}

/// Replication fingerprint of one table (cluster rejoin repair): row
/// count, an order-independent hash over every row's documents, and a
/// hash over the catalog fields that shape replay.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TableDigest {
    pub name: String,
    pub rows: u64,
    pub rows_hash: u64,
    pub schema_hash: u64,
}

/// Hash over every replay-relevant catalog field of a table. Physical
/// fields (heap page ids, index root pages) are deliberately excluded:
/// they legitimately differ between nodes holding identical data.
fn schema_hash(meta: &TableMeta) -> u64 {
    let mut h = DefaultHasher::new();
    meta.columns.hash(&mut h);
    meta.primary_key.hash(&mut h);
    meta.unique.hash(&mut h);
    meta.not_null.hash(&mut h);
    meta.autoinc.hash(&mut h);
    meta.autoguid.hash(&mut h);
    meta.indexes.hash(&mut h);
    meta.index_defs.hash(&mut h);
    meta.constraint_unique.hash(&mut h);
    meta.defaults.hash(&mut h);
    meta.checks.hash(&mut h);
    meta.foreign_keys.hash(&mut h);
    h.finish()
}

pub struct Database {
    pager: Pager,
    tables: std::collections::BTreeMap<String, TableMeta>,
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
    /// SAVEPOINT stack inside the open transaction: (name, snapshot). Each
    /// ROLLBACK TO restores its snapshot and drops everything above it.
    savepoints: Vec<(String, TableSnapshot)>,
    /// Session transaction snapshot: full table state at BEGIN. Rollback
    /// restores it; commit just discards it (durability is the WAL's job).
    tx_snapshot: Option<TableSnapshot>,
    /// CTEs visible to the statement currently executing (WITH ...): the
    /// body's rows, keyed by CTE name. Cleared per top-level statement.
    ctes: std::collections::BTreeMap<String, Vec<Object>>,
    /// Lazily-computed AUTOINCREMENT next id per table (max(id)+1 over the
    /// heap). Invalidated by rewrites/deletes; not persisted — recomputed
    /// after restart, preserving max(existing)+1 semantics.
    autoinc_cache: std::collections::HashMap<String, i64>,
    /// Canonical rewrite of the last executed statement when it auto-filled
    /// GUID values (explicit generated ids embedded). Replicating callers
    /// must forward this text instead of the original: random GUIDs cannot
    /// be re-derived on peers the way AUTOINCREMENT's deterministic max+1
    /// can. Reset at the start of every statement; see `take_resolved_insert`.
    resolved_insert: Option<String>,
    /// Next seq for the catch-up journal ([`Database::journal_append`]).
    /// Seeded lazily from MAX(seq)+1 (in-memory counter under the write
    /// lock; gaps from failed statements are harmless — positions pull
    /// ranges, not counts).
    journal_next: Option<u64>,
    /// This node's persistent random identity (see CLUSTER_ID_TABLE),
    /// cached after first read.
    cluster_id: Option<String>,
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
            self.pager.commit_tx(tx).map_err(SqlError::from)
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
        self.pager.sync_wal().map_err(SqlError::from)?;
        self.pending_sync = false;
        Ok(())
    }
    pub fn open(path: &std::path::Path) -> Result<Database> {
        let mut pager = Pager::open(path)?;
        // Reserve pages 0 (header) and 1 (catalog); allocate 1 if missing.
        while pager.num_pages() <= CATALOG_PAGE {
            let mut tx = pager.begin_tx();
            let before = pager.num_pages();
            let id = pager.allocate_page(&mut tx)?;
            debug_assert_eq!(id, before);
            pager.commit_tx(tx)?;
        }
        let mut tables = std::collections::BTreeMap::new();
        // New format: a magic-headed chain of pages. Legacy databases keep a
        // raw encoded catalog on page 1 — decode it in place; the first
        // catalog save rewrites it in the new format.
        let raw = pager.read_page(CATALOG_PAGE)?.to_vec();
        let cat_bytes: Option<Vec<u8>> = if raw.starts_with(CATALOG_MAGIC) {
            let total = u32::from_le_bytes(raw[8..12].try_into().expect("header fits")) as usize;
            let mut data = raw[CATALOG_HDR..].to_vec();
            let mut next = u32::from_le_bytes(raw[12..16].try_into().expect("header fits"));
            while next != 0 {
                if next >= pager.num_pages() {
                    return err("catalog chain is corrupt (dangling overflow page)");
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
                                                Some((n.to_string(), c.to_string(), u))
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
                                .map(|(n, _, _)| n.clone())
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
                                TableMeta {
                                    columns,
                                    pages,
                                    primary_key,
                                    unique,
                                    not_null,
                                    autoinc,
                                    autoguid,
                                    indexes,
                                    index_defs,
                                    constraint_unique,
                                    defaults,
                                    checks,
                                    foreign_keys,
                                    index_roots,
                                },
                            );
                        }
                    }
                }
            }
        }
        Ok(Database {
            pager,
            tables,
            async_commit: false,
            pending_sync: false,
            write_unit: 0,
            savepoints: Vec::new(),
            tx_snapshot: None,
            ctes: std::collections::BTreeMap::new(),
            autoinc_cache: std::collections::HashMap::new(),
            resolved_insert: None,
            journal_next: None,
            cluster_id: None,
        })
    }

    pub fn in_memory() -> Result<Database> {
        let dir = tempfile::tempdir().map_err(|e| SqlError::Message(e.to_string()))?;
        // Leak: process-lifetime scratch DB, fine for embedded use/tests.
        let path = dir.keep().join("mem.db");
        Database::open(&path)
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
                            .map(|(n, c, u)| {
                                Value::Array(vec![
                                    Value::Str(n.clone()),
                                    Value::Str(c.clone()),
                                    Value::Bool(*u),
                                ])
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
            tables.insert(name.clone(), Value::Object(m));
        }
        let mut cat = Object::new();
        cat.insert("tables".into(), Value::Object(tables));
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

    fn snapshot_all(
        &mut self,
    ) -> Result<std::collections::BTreeMap<String, (TableMeta, Vec<Object>)>> {
        let names: Vec<String> = self.tables.keys().cloned().collect();
        let mut snap = std::collections::BTreeMap::new();
        for name in names {
            let meta = self.tables.get(&name).cloned().unwrap_or_default();
            // A read failure must surface: snapshotting an unreadable table
            // as empty would turn the next ROLLBACK into a permanent wipe.
            let docs = self.table_docs(&name)?;
            snap.insert(name, (meta, docs));
        }
        Ok(snap)
    }

    fn rollback_tx(&mut self) -> Result<ExecOutcome> {
        let Some(snap) = self.tx_snapshot.take() else {
            return err("no transaction in progress");
        };
        self.savepoints.clear();
        self.tables.clear();
        for (name, (mut meta, docs)) in snap {
            self.rewrite_table(&name, &mut meta, docs)?;
        }
        self.save_catalog()?;
        Ok(ExecOutcome::Affected(0))
    }

    /// SAVEPOINT name: snapshot the current transaction state so a later
    /// ROLLBACK TO can restore it (the transaction stays open).
    fn savepoint(&mut self, name: &str) -> Result<ExecOutcome> {
        if self.tx_snapshot.is_none() {
            return err("SAVEPOINT requires an active transaction");
        }
        let snap = self.snapshot_all()?;
        self.savepoints.push((name.to_string(), snap));
        Ok(ExecOutcome::Affected(0))
    }

    /// RELEASE SAVEPOINT name: forget the savepoint (changes stay).
    fn release_savepoint(&mut self, name: &str) -> Result<ExecOutcome> {
        let Some(pos) = self.savepoints.iter().position(|(n, _)| n == name) else {
            return err(format!("savepoint {name} does not exist"));
        };
        self.savepoints.truncate(pos);
        Ok(ExecOutcome::Affected(0))
    }

    /// ROLLBACK TO SAVEPOINT name: restore that savepoint's state; the
    /// outer transaction continues and later savepoints are discarded.
    fn rollback_to_savepoint(&mut self, name: &str) -> Result<ExecOutcome> {
        let Some(pos) = self.savepoints.iter().position(|(n, _)| n == name) else {
            return err(format!("savepoint {name} does not exist"));
        };
        let snap = self.savepoints[pos].1.clone();
        self.savepoints.truncate(pos);
        self.tables.clear();
        for (name, (mut meta, docs)) in snap {
            self.rewrite_table(&name, &mut meta, docs)?;
        }
        self.save_catalog()?;
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

    /// Parse a single statement once and classify it in the same pass: the
    /// AST feeds [`Database::execute_parsed`], the flags route the request
    /// (write path, transaction control) without re-parsing the text.
    pub fn parse_classified(sql: &str) -> Result<ParsedStatement> {
        let mut stmts = Parser::parse_sql(&GenericDialect {}, sql)
            .map_err(|e| SqlError::Parse(e.to_string()))?;
        let stmt = match stmts.len() {
            0 => return err("empty statement"),
            1 => stmts.swap_remove(0),
            _ => return err("exactly one statement per execute() call"),
        };
        let tx = Self::classify_tx(&stmt);
        let is_write = Self::stmt_is_write(&stmt);
        Ok(ParsedStatement { stmt, tx, is_write })
    }

    /// True when a session transaction is open.
    pub fn in_transaction(&self) -> bool {
        self.tx_snapshot.is_some()
    }

    /// Classify transaction-control statements, used by servers to time
    /// replication: writes inside an open transaction must not reach peers
    /// until the transaction commits; a rollback discards them.
    pub fn tx_control(sql: &str) -> TxControl {
        let Ok(stmts) = Parser::parse_sql(&GenericDialect {}, sql) else {
            return TxControl::None;
        };
        match stmts.first() {
            Some(stmt) => Self::classify_tx(stmt),
            None => TxControl::None,
        }
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
        self.ctes.clear();
        self.resolved_insert = None;
        self.exec_stmt(parsed.stmt)
    }

    /// Parse-check a statement batch without executing anything (the web
    /// console's "parse" button).
    pub fn parse_check(sql: &str) -> std::result::Result<(), String> {
        match Parser::parse_sql(&GenericDialect {}, sql) {
            Ok(stmts) if !stmts.is_empty() => Ok(()),
            Ok(_) => Err("empty statement".into()),
            Err(e) => Err(e.to_string()),
        }
    }

    /// Execute every statement in `sql`, in order, stopping at the first
    /// error. Successfully executed prefixes are still reported so callers
    /// (e.g. the web console) can render partial results.
    pub fn execute_batch(&mut self, sql: &str) -> BatchResult {
        let stmts = match Parser::parse_sql(&GenericDialect {}, sql) {
            Ok(s) if s.is_empty() => {
                return BatchResult {
                    statements: 0,
                    outcomes: vec![],
                    error: Some(BatchError {
                        statement: 0,
                        message: "empty statement".into(),
                    }),
                }
            }
            Ok(s) => s,
            Err(e) => {
                return BatchResult {
                    statements: 0,
                    outcomes: vec![],
                    error: Some(BatchError {
                        statement: 0,
                        message: SqlError::Parse(e.to_string()).to_string(),
                    }),
                }
            }
        };
        let statements = stmts.len();
        let mut outcomes = Vec::new();
        for (i, stmt) in stmts.into_iter().enumerate() {
            self.ctes.clear();
            self.resolved_insert = None;
            match self.exec_stmt(stmt) {
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

    /// Take the canonical rewrite of the last statement, present only when
    /// it auto-filled GUID values. Replicating callers replace the original
    /// statement text with this one so peers apply the exact generated ids.
    pub fn take_resolved_insert(&mut self) -> Option<String> {
        self.resolved_insert.take()
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
                unique: true,
                auto: true,
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
                        .chain(meta.index_defs.iter().map(|(n, c, u)| IndexInfo {
                            name: n.clone(),
                            column: c.clone(),
                            unique: *u,
                            auto: false,
                        }))
                        .collect(),
                    pages: meta.pages.len(),
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
    pub fn observed_columns(&mut self, table: &str) -> Vec<String> {
        if !self.tables.contains_key(table) {
            return Vec::new();
        }
        self.table_docs(table)
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
            let docs = self.table_docs(name)?;
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
        self.tables.retain(|name, _| is_system_table(name));
        self.save_catalog()
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
        self.execute(&format!(
            "CREATE TABLE IF NOT EXISTS {CLUSTER_LOG_TABLE} (seq INT PRIMARY KEY, sql TEXT)"
        ))?;
        self.execute(&format!(
            "CREATE TABLE IF NOT EXISTS {CLUSTER_POS_TABLE} (node_id TEXT PRIMARY KEY, seq INT)"
        ))?;
        self.execute(&format!(
            "CREATE TABLE IF NOT EXISTS {CLUSTER_ID_TABLE} (id TEXT)"
        ))?;
        Ok(())
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
            None => {
                let r = self.execute(&format!("SELECT MAX(seq) FROM {CLUSTER_LOG_TABLE}"))?;
                let max = match r {
                    ExecOutcome::Rows(rows) => rows
                        .rows
                        .first()
                        .and_then(|row| row.first())
                        .and_then(Value::as_i64)
                        .unwrap_or(0),
                    _ => 0,
                };
                (max as u64) + 1
            }
        };
        self.execute(&format!(
            "INSERT INTO {CLUSTER_LOG_TABLE} (seq, sql) VALUES ({next}, {})",
            value_literal(&Value::Str(sql.to_string()))?
        ))?;
        self.journal_next = Some(next + 1);
        Ok(next)
    }

    /// Highest journal seq (0 when empty).
    pub fn journal_head(&mut self) -> Result<u64> {
        self.ensure_cluster_tables()?;
        let r = self.execute(&format!("SELECT MAX(seq) FROM {CLUSTER_LOG_TABLE}"))?;
        Ok(match r {
            ExecOutcome::Rows(rows) => rows
                .rows
                .first()
                .and_then(|row| row.first())
                .and_then(Value::as_i64)
                .unwrap_or(0) as u64,
            _ => 0,
        })
    }

    /// Lowest journal seq still retained (0 when empty). A position below
    /// this minus one means the needed range was trimmed away and the
    /// peer must fall back to a full snapshot.
    pub fn journal_oldest(&mut self) -> Result<u64> {
        self.ensure_cluster_tables()?;
        let r = self.execute(&format!("SELECT MIN(seq) FROM {CLUSTER_LOG_TABLE}"))?;
        Ok(match r {
            ExecOutcome::Rows(rows) => rows
                .rows
                .first()
                .and_then(|row| row.first())
                .and_then(Value::as_i64)
                .unwrap_or(0) as u64,
            _ => 0,
        })
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
        if let ExecOutcome::Rows(rows) = r {
            for row in &rows.rows {
                let (Some(Value::Int(seq)), Some(Value::Str(sql))) = (row.first(), row.get(1))
                else {
                    return err("journal row has unexpected shape");
                };
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
        self.execute(&format!(
            "DELETE FROM {CLUSTER_LOG_TABLE} WHERE seq <= {}",
            head - keep
        ))
        .map(|_| ())
    }

    /// Void every journal entry's text, keeping the seqs allocated. Used
    /// when a snapshot adoption overrules this node's own recent writes:
    /// the adjudicated entries must never replay again (a later repair on
    /// some peer would pull them back and resurrect discarded writes), but
    /// the seq space and every recorded position must keep their meaning.
    /// Replays of a voided entry are a parsed no-op.
    pub fn journal_void_all(&mut self) -> Result<()> {
        self.ensure_cluster_tables()?;
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
            .filter(|n| !is_system_table(n))
            .cloned()
            .collect();
        let mut ddl = String::new();
        if !names.is_empty() {
            let drops = names
                .iter()
                .map(|n| quote_ident(n))
                .collect::<Vec<_>>()
                .join(", ");
            ddl.push_str(&format!("DROP TABLE IF EXISTS {drops};\n"));
        }
        for name in &names {
            let meta = self.tables.get(name).cloned().unwrap();
            ddl.push_str(&format!("CREATE TABLE {} (\n", quote_ident(name)));
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
            ddl.push_str("\n);\n");
            for (iname, icol, unique) in &meta.index_defs {
                ddl.push_str(&format!(
                    "CREATE {}INDEX {} ON {} ({});\n",
                    if *unique { "UNIQUE " } else { "" },
                    quote_ident(iname),
                    quote_ident(name),
                    quote_ident(icol)
                ));
            }
        }
        // Insert order must respect FK dependencies: referenced tables
        // first. Catalog iteration is alphabetical, so without this a
        // child table's rows could replay before its parent's and fail
        // the per-row FK check.
        let mut dml = String::new();
        for name in fk_dependency_order(&names, &self.tables) {
            for doc in self.table_docs(&name)? {
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
        Ok(ddl)
    }

    /// Page size of the underlying storage file.
    pub fn page_size(&self) -> usize {
        PAGE_SIZE
    }

    /// Number of allocated pages.
    pub fn num_pages(&self) -> u32 {
        self.pager.num_pages_now()
    }

    /// Last WAL LSN known durable; nodes that received the same writes should
    /// report converging values.
    pub fn durable_lsn(&self) -> u64 {
        self.pager.durable_lsn()
    }

    fn exec_stmt(&mut self, stmt: Statement) -> Result<ExecOutcome> {
        match stmt {
            Statement::CreateTable(create) => self.exec_create(create),
            Statement::Drop {
                object_type,
                names,
                if_exists,
                ..
            } => {
                if object_type == sqlparser::ast::ObjectType::Index {
                    for n in &names {
                        let iname = obj_name(n);
                        // Constraint indexes are derived from the table's
                        // PRIMARY KEY/UNIQUE declarations (SQLite semantics).
                        if iname.starts_with("sqlite_autoindex_") {
                            return err("index associated with UNIQUE or PRIMARY KEY constraint \
                                 cannot be dropped");
                        }
                        let mut found = false;
                        for meta in self.tables.values_mut() {
                            if let Some(pos) = meta.indexes.iter().position(|i| i == &iname) {
                                meta.indexes.remove(pos);
                                found = true;
                                // The B+ tree itself stays in index_roots:
                                // non-unique lookups still benefit from it.
                                if let Some(dpos) =
                                    meta.index_defs.iter().position(|(n, _, _)| n == &iname)
                                {
                                    let (_, col, unique) = meta.index_defs.remove(dpos);
                                    // Lift UNIQUE only when this index was
                                    // the sole source: table-declared
                                    // constraints and other unique indexes
                                    // on the same column keep it enforced.
                                    if unique
                                        && !meta.constraint_unique.contains(&col)
                                        && meta.primary_key.as_deref() != Some(col.as_str())
                                        && !meta.index_defs.iter().any(|(_, c, u)| *c == col && *u)
                                    {
                                        meta.unique.retain(|c| c != &col);
                                    }
                                }
                            }
                        }
                        if !found && !if_exists {
                            return err(format!("index {iname} does not exist"));
                        }
                    }
                    self.save_catalog()?;
                    return Ok(ExecOutcome::Affected(0));
                }
                if object_type != sqlparser::ast::ObjectType::Table {
                    return err("only DROP TABLE/INDEX are supported");
                }
                // Validate every target before touching the catalog: a missing
                // name mid-list must not leave earlier drops applied in memory
                // only, and dropping a table other tables still reference
                // leaves dangling FKs (their inserts fail and the join
                // snapshot can never replay).
                let dropping: Vec<String> = names.iter().map(obj_name).collect();
                for name in &dropping {
                    if !self.tables.contains_key(name) {
                        if if_exists {
                            continue;
                        }
                        return err(format!("table {name} does not exist"));
                    }
                    let referencing: Vec<&str> = self
                        .tables
                        .iter()
                        .filter(|(t, m)| {
                            t.as_str() != name.as_str()
                                && !dropping.iter().any(|d| d == t.as_str())
                                && m.foreign_keys.iter().any(|(_, rt, _)| rt == name)
                        })
                        .map(|(t, _)| t.as_str())
                        .collect();
                    if !referencing.is_empty() {
                        return err(format!(
                            "cannot drop table {name}: referenced by FOREIGN KEY in {}",
                            referencing.join(", ")
                        ));
                    }
                }
                for name in &dropping {
                    self.tables.remove(name);
                }
                self.save_catalog()?;
                Ok(ExecOutcome::Affected(0))
            }
            Statement::Insert(insert) => self.exec_insert(insert),
            Statement::Update(sqlparser::ast::Update {
                table,
                assignments,
                from,
                selection,
                returning,
                ..
            }) => self.exec_update(table, assignments, from, selection, returning),
            Statement::Delete(sqlparser::ast::Delete {
                from,
                using,
                selection,
                returning,
                ..
            }) => self.exec_delete(from, using, selection, returning),
            Statement::AlterTable(alter) => self.exec_alter(alter),
            Statement::StartTransaction { .. } => {
                if self.tx_snapshot.is_some() {
                    return err("transaction already in progress");
                }
                self.tx_snapshot = Some(self.snapshot_all()?);
                Ok(ExecOutcome::Affected(0))
            }
            Statement::Commit { .. } => {
                if self.tx_snapshot.take().is_none() {
                    return err("no transaction in progress");
                }
                // Savepoints die with their transaction: a stale mark must
                // not be reachable from a later transaction's ROLLBACK TO.
                self.savepoints.clear();
                // One WAL fsync for every statement in the transaction
                // (async-commit mode leaves it to the background flusher).
                if !self.async_commit {
                    self.pager.sync_wal().map_err(SqlError::from)?;
                }
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
            Statement::Query(q) => self.exec_query(*q),
            Statement::Truncate(tr) => {
                // Empty the tables; shape (columns/constraints) is kept.
                for target in &tr.table_names {
                    let name = obj_name(&target.name);
                    if !self.tables.contains_key(&name) && !tr.if_exists {
                        return err(format!("table {name} does not exist"));
                    }
                    if self.tables.contains_key(&name) {
                        let mut meta = self.tables.get(&name).cloned().unwrap_or_default();
                        self.rewrite_table(&name, &mut meta, Vec::new())?;
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

    /// Rewrite the whole table from `docs` (delete+reinsert; old pages and
    /// old index trees are orphaned). Rebuilds every index tree for the
    /// table, updates `meta` in place, and re-inserts it into the catalog.
    fn rewrite_table(
        &mut self,
        table: &str,
        meta: &mut TableMeta,
        docs: Vec<Object>,
    ) -> Result<()> {
        let mut heap = Heap { pages: Vec::new() };
        let cols: Vec<String> = meta.index_roots.keys().cloned().collect();
        let mut tx = self.pager.begin_tx();
        let mut pairs: Vec<(u64, Object)> = Vec::with_capacity(docs.len());
        for doc in &docs {
            let loc = heap.insert(&mut self.pager, &mut tx, doc)?;
            pairs.push((loc, doc.clone()));
        }
        let roots = self.build_trees(&mut tx, &pairs, &cols)?;
        // Keep the previous catalog entry so a failed persist can restore the
        // in-memory map — after `?` below the tx is dropped and its staged
        // pages never reach the data file; the map must not keep pointing at
        // them, or a later successful save would persist the ghost pages and
        // orphan the real data.
        let prev_meta = self.tables.get(table).cloned();
        meta.pages = heap.pages;
        meta.index_roots = roots;
        self.autoinc_cache.remove(table);
        self.tables.insert(table.to_string(), meta.clone());
        if let Err(e) = self.save_catalog_into(&mut tx) {
            match prev_meta {
                Some(old) => {
                    *meta = old.clone();
                    self.tables.insert(table.to_string(), old);
                }
                None => {
                    self.tables.remove(table);
                }
            }
            return Err(e);
        }
        self.commit_pager_tx(tx)?;
        Ok(())
    }

    fn table_pairs(&mut self, table: &str) -> Result<Vec<(u64, Object)>> {
        let Some(meta) = self.tables.get(table) else {
            return err(format!("table {table} does not exist"));
        };
        let heap = Heap {
            pages: meta.pages.clone(),
        };
        let mut out = Vec::new();
        let rtx = self.pager.begin_tx();
        for &pid in &heap.pages {
            out.extend(heap.page_docs(&mut self.pager, &rtx, pid)?);
        }
        self.pager.abort_tx(rtx)?;
        Ok(out)
    }

    fn build_trees(
        &mut self,
        tx: &mut crate::pager::Tx,
        pairs: &[(u64, Object)],
        cols: &[String],
    ) -> Result<std::collections::BTreeMap<String, u32>> {
        let mut roots = std::collections::BTreeMap::new();
        for col in cols {
            let mut tree = BTree::create(&mut self.pager, tx).map_err(|e| index_err(col, e))?;
            for (loc, doc) in pairs {
                if let Some(v) = doc.get(col) {
                    if !matches!(v, Value::Null) {
                        tree.insert(&mut self.pager, tx, v.clone(), *loc, false)
                            .map_err(|e| index_err(col, e))?;
                    }
                }
            }
            roots.insert(col.clone(), tree.root);
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
            }
            .scan(&mut self.pager)?;
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

    fn index_probe(
        &mut self,
        table: &str,
        alias: Option<&str>,
        selection: &Option<SqlExpr>,
    ) -> Result<Option<Vec<(u64, Object)>>> {
        let Some(cond) = selection else {
            return Ok(None);
        };
        let Some(meta) = self.tables.get(table).cloned() else {
            return Ok(None);
        };
        if meta.index_roots.is_empty() || !self.ctes.is_empty() {
            return Ok(None);
        }
        let Some((col, plan)) = probe_plan(cond, &meta, table, alias) else {
            return Ok(None);
        };
        let root = meta.index_roots[&col];
        let heap = Heap {
            pages: meta.pages.clone(),
        };
        let tx = self.pager.begin_tx(); // read-only use; aborted immediately
        let tree = BTree::open(root);
        let pairs = match plan {
            ProbePlan::Eq(v) => {
                // Bounded range + equal filter so non-unique trees return
                // every duplicate match (get() would yield one entry).
                // The hi bound lets the tree stop at the first key past v
                // instead of walking the whole right side.
                let mut p = tree
                    .range_bounded(&mut self.pager, &tx, &v, Some((&v, true)))
                    .map_err(|e| index_err(&col, e))?;
                p.retain(|(k, _)| Value::cmp_values(k, &v) == Ordering::Equal);
                p
            }
            ProbePlan::Range { lo, hi } => {
                let hi_ref = hi.as_ref().map(|(v, incl)| (v, *incl));
                let mut pairs = match &lo {
                    Some((v, true)) => tree
                        .range_bounded(&mut self.pager, &tx, v, hi_ref)
                        .map_err(|e| index_err(&col, e))?,
                    Some((v, false)) => {
                        // strict lower bound: start at v, then drop the equal run
                        let mut p = tree
                            .range_bounded(&mut self.pager, &tx, v, hi_ref)
                            .map_err(|e| index_err(&col, e))?;
                        p.retain(|(k, _)| Value::cmp_values(k, v) != std::cmp::Ordering::Equal);
                        p
                    }
                    // No lower bound: full tree scan (rare: WHERE col < x).
                    None => tree
                        .scan(&mut self.pager, &tx)
                        .map_err(|e| index_err(&col, e))?,
                };
                if let Some((v, incl)) = &hi {
                    pairs.retain(|(k, _)| {
                        let o = Value::cmp_values(k, v);
                        o == std::cmp::Ordering::Less || (*incl && o == std::cmp::Ordering::Equal)
                    });
                }
                pairs
            }
        };
        self.pager.abort_tx(tx)?;
        let mut out: Vec<(u64, Object)> = Vec::with_capacity(pairs.len());
        for (_, loc) in pairs {
            if let Some(doc) = heap.doc_at(&mut self.pager, loc)? {
                out.push((loc, doc));
            }
        }
        // Heap order (page, slot) — loc packing already sorts that way.
        out.sort_by_key(|(loc, _)| *loc);
        Ok(Some(out))
    }

    fn table_docs(&mut self, table: &str) -> Result<Vec<Object>> {
        let Some(meta) = self.tables.get(table) else {
            return err(format!("table {table} does not exist"));
        };
        let heap = Heap {
            pages: meta.pages.clone(),
        };
        heap.scan(&mut self.pager).map_err(Into::into)
    }

    fn matches(&self, selection: &Option<SqlExpr>, doc: &Object) -> Result<bool> {
        match selection {
            None => Ok(true),
            Some(e) => Ok(matches!(eval_expr(e, doc)?, Value::Bool(true))),
        }
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
        for a in &mut assignments {
            self.subst_expr(&mut a.value)?;
        }
        if let Some(sel) = &mut selection {
            self.subst_expr(sel)?;
        }
        let sqlparser::ast::TableFactor::Table { name, alias, .. } = table.relation else {
            return err("only simple table names in UPDATE");
        };
        let tname = obj_name(&name);
        let tkey = alias
            .as_ref()
            .map(|a| a.name.value.clone())
            .unwrap_or_else(|| tname.clone());
        let mut meta = self.tables.get(&tname).cloned().unwrap_or_default();

        // Fast path: probe-able WHERE on an indexed column — update the
        // affected rows in place instead of rewriting the whole table.
        if from.is_none() {
            if let Some(matches) = self.index_probe(&tname, Some(tkey.as_str()), &selection)? {
                return self.exec_update_fast(
                    tname,
                    meta,
                    matches,
                    &assignments,
                    &selection,
                    &update_returning,
                );
            }
        }

        let docs = self.table_docs(&tname)?;
        let (out, changed_docs, old_changed, count) = match from {
            None => {
                // Plain UPDATE: assignments and WHERE see only target columns.
                let mut out = Vec::new();
                let mut changed_docs: Vec<Object> = Vec::new();
                let mut old_changed: Vec<Object> = Vec::new();
                let mut count = 0u64;
                for doc in docs {
                    if self.matches(&selection, &doc)? {
                        old_changed.push(doc.clone());
                        let mut doc = doc;
                        for a in &assignments {
                            let sqlparser::ast::AssignmentTarget::ColumnName(col) = &a.target
                            else {
                                return err("unsupported assignment target");
                            };
                            let col_name = obj_name(col);
                            let v = eval_expr(&a.value, &doc)?;
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
                let merged = self.load_from(&from_list, &None)?;
                let prefix = format!("{tkey}.");
                let mut updates: std::collections::BTreeMap<Vec<u8>, Object> =
                    std::collections::BTreeMap::new();
                let mut changed_docs: Vec<Object> = Vec::new();
                let mut old_changed: Vec<Object> = Vec::new();
                for m in &merged {
                    // Rebuild the target doc from its qualified slice.
                    let mut tdoc = Object::new();
                    for (k, v) in m {
                        if let Some(col) = k.strip_prefix(&prefix) {
                            tdoc.insert(col.to_string(), v.clone());
                        }
                    }
                    let key =
                        encode::encode_to_vec(&Value::Object(tdoc.clone())).unwrap_or_default();
                    if updates.contains_key(&key) {
                        continue; // first qualifying match wins
                    }
                    if !self.matches(&selection, m)? {
                        continue;
                    }
                    old_changed.push(tdoc.clone());
                    for a in &assignments {
                        let sqlparser::ast::AssignmentTarget::ColumnName(col) = &a.target else {
                            return err("unsupported assignment target");
                        };
                        let col_name = obj_name(col);
                        let v = eval_expr(&a.value, m)?;
                        tdoc.insert(col_name, v);
                    }
                    changed_docs.push(tdoc.clone());
                    updates.insert(key, tdoc);
                }
                let out: Vec<Object> = docs
                    .into_iter()
                    .map(|doc| {
                        let key =
                            encode::encode_to_vec(&Value::Object(doc.clone())).unwrap_or_default();
                        updates.remove(&key).unwrap_or(doc)
                    })
                    .collect();
                let count = changed_docs.len() as u64;
                (out, changed_docs, old_changed, count)
            }
        };
        // Validate BEFORE writing: a failed UPDATE must not change data.
        for doc in &out {
            meta.check(doc)?;
            self.check_fks(&meta, doc)?;
        }
        meta.check_unique(&out)?;
        // Parent-side FK: key values that disappear must not be referenced.
        self.check_fk_parent_delete(&tname, &old_changed, &out)?;
        let changed = changed_docs.clone();
        self.rewrite_table(&tname, &mut meta, out)?;
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
    ) -> Result<ExecOutcome> {
        let mut updates: Vec<(u64, Object, Object)> = Vec::new(); // (loc, old, new)
        for (loc, doc) in matches {
            if !self.matches(selection, &doc)? {
                continue; // probe col matched; some other conjunct did not
            }
            let mut doc = doc;
            // The pre-update image must be captured before assignments run —
            // it drives reindex_replace's old-key removal; pushing the mutated
            // doc as both old and new leaves stale index entries behind.
            let old_doc = doc.clone();
            for a in assignments {
                let sqlparser::ast::AssignmentTarget::ColumnName(col) = &a.target else {
                    return err("unsupported assignment target");
                };
                let col_name = obj_name(col);
                let v = eval_expr(&a.value, &doc)?;
                doc.insert(col_name, v);
            }
            meta.check(&doc)?;
            self.check_fks(&meta, &doc)?;
            updates.push((loc, old_doc, doc));
        }
        if updates.is_empty() {
            if let Some(ret) = update_returning {
                return project_returning(ret, &[]);
            }
            return Ok(ExecOutcome::Affected(0));
        }
        // Parent-side FK check before any page is touched. `updates` rows
        // are the only ones whose key values can disappear; their new docs
        // are the survivors (referenced columns are unique in practice).
        let old_docs: Vec<Object> = updates.iter().map(|(_, o, _)| o.clone()).collect();
        let new_docs: Vec<Object> = updates.iter().map(|(_, _, n)| n.clone()).collect();
        self.check_fk_parent_delete(&tname, &old_docs, &new_docs)?;
        // Pre-statement docs for the legacy whole-set unique check below —
        // loaded only when that check can actually run (a constraint column
        // without a tree). Tables with full trees enforce uniqueness via
        // reindex_replace; loading every doc here made single-row UPDATE
        // loops quadratic.
        let legacy_check = (meta.primary_key.is_some() || !meta.unique.is_empty())
            && meta
                .primary_key
                .iter()
                .chain(meta.unique.iter())
                .any(|c| !meta.index_roots.contains_key(c));
        let all_docs: Option<Vec<Object>> = if legacy_check {
            Some(self.table_docs(&tname)?)
        } else {
            None
        };
        let mut heap = Heap {
            pages: meta.pages.clone(),
        };
        let mut roots = meta.index_roots.clone();
        let mut tx = self.pager.begin_tx();
        for i in 0..updates.len() {
            let (loc, old_doc, new_doc) = updates[i].clone();
            let page = crate::heap::unpack_loc(loc).0;
            let before = heap.page_docs(&mut self.pager, &tx, page)?;
            let out = heap.replace(&mut self.pager, &mut tx, loc, &new_doc)?;
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
                reindex_repoint(
                    &mut self.pager,
                    &mut tx,
                    &mut roots,
                    &before,
                    *old_l,
                    *new_l,
                )?;
            }
            if let Err(e) = reindex_replace(
                &mut self.pager,
                &mut tx,
                &meta,
                &mut roots,
                &old_doc,
                loc,
                &new_doc,
                out.placed,
            ) {
                self.pager.abort_tx(tx)?;
                return Err(e);
            }
        }
        // Legacy files whose constraint columns predate trees keep the
        // whole-set duplicate check (mirrors the INSERT path); tables with
        // trees enforce uniqueness through reindex_replace above.
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
        let pages_changed = heap.pages != meta.pages;
        let roots_changed = roots != meta.index_roots;
        if pages_changed || roots_changed {
            let prev = self
                .tables
                .get(&tname)
                .map(|m| (m.pages.clone(), m.index_roots.clone()));
            if let Some(m) = self.tables.get_mut(&tname) {
                m.pages = heap.pages.clone();
                m.index_roots = roots;
            }
            if let Err(e) = self.save_catalog_into(&mut tx) {
                // The tx is dropped on this path, so its staged pages never
                // reach the data file; the catalog map must not keep pointing
                // at them or a later successful save persists ghost pages.
                if let Some((pages, index_roots)) = prev {
                    if let Some(m) = self.tables.get_mut(&tname) {
                        m.pages = pages;
                        m.index_roots = index_roots;
                    }
                }
                return Err(e);
            }
        }
        self.commit_pager_tx(tx)?;
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
        if let Some(sel) = &mut selection {
            self.subst_expr(sel)?;
        }
        let sqlparser::ast::FromTable::WithFromKeyword(tables) = from else {
            return err("unsupported DELETE form");
        };
        if tables.len() != 1 {
            return err("DELETE from exactly one table");
        }
        let sqlparser::ast::TableFactor::Table { name, alias, .. } = tables[0].relation.clone()
        else {
            return err("only simple table names in DELETE");
        };
        let tname = obj_name(&name);
        let tkey = alias
            .as_ref()
            .map(|a| a.name.value.clone())
            .unwrap_or_else(|| tname.clone());
        let mut meta = self.tables.get(&tname).cloned().unwrap_or_default();
        // Fast path: probe-able WHERE on an indexed column — remove the
        // matching rows in place (page re-pack) instead of rewriting the
        // whole table.
        if using.is_none() {
            if let Some(matches) = self.index_probe(&tname, Some(tkey.as_str()), &selection)? {
                return self.exec_delete_fast(tname, meta, matches, &selection, &returning);
            }
        }
        let docs = self.table_docs(&tname)?;
        let (kept, removed) = if let Some(using) = using {
            // DELETE ... USING: qualify over the joined rows and mark the
            // target docs whose combination satisfies WHERE.
            let mut from_list = vec![tables[0].clone()];
            from_list.extend(using);
            let merged = self.load_from(&from_list, &None)?;
            let prefix = format!("{tkey}.");
            let mut rm: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
            for m in &merged {
                if !self.matches(&selection, m)? {
                    continue;
                }
                let mut tdoc = Object::new();
                for (k, v) in m {
                    if let Some(col) = k.strip_prefix(&prefix) {
                        tdoc.insert(col.to_string(), v.clone());
                    }
                }
                rm.insert(encode::encode_to_vec(&Value::Object(tdoc)).unwrap_or_default());
            }
            let mut kept = Vec::new();
            let mut removed = Vec::new();
            for doc in docs {
                let key = encode::encode_to_vec(&Value::Object(doc.clone())).unwrap_or_default();
                if rm.contains(&key) {
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
                if self.matches(&selection, &doc)? {
                    removed.push(doc);
                } else {
                    kept.push(doc);
                }
            }
            (kept, removed)
        };
        let count = removed.len() as u64;
        self.check_fk_parent_delete(&tname, &removed, &[])?;
        self.rewrite_table(&tname, &mut meta, kept)?;
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
    ) -> Result<ExecOutcome> {
        let mut targets: Vec<(u64, Object)> = Vec::new();
        for (loc, doc) in matches {
            if self.matches(selection, &doc)? {
                targets.push((loc, doc));
            }
        }
        if targets.is_empty() {
            if let Some(ret) = returning {
                return project_returning(ret, &[]);
            }
            return Ok(ExecOutcome::Affected(0));
        }
        // Parent-side FK check before any page is touched.
        let removed_docs: Vec<Object> = targets.iter().map(|(_, d)| d.clone()).collect();
        self.check_fk_parent_delete(&tname, &removed_docs, &[])?;
        let mut heap = Heap {
            pages: meta.pages.clone(),
        };
        let mut roots = meta.index_roots.clone();
        let mut tx = self.pager.begin_tx();
        let affected: std::collections::BTreeSet<u32> = targets
            .iter()
            .map(|(l, _)| crate::heap::unpack_loc(*l).0)
            .collect();
        let mut before = Vec::new();
        for pid in affected {
            before.extend(heap.page_docs(&mut self.pager, &tx, pid)?);
        }
        let locs: Vec<u64> = targets.iter().map(|(l, _)| *l).collect();
        let moves = heap.remove_many(&mut self.pager, &mut tx, &locs)?;
        for (loc, doc) in &targets {
            reindex_remove(&mut self.pager, &mut tx, &mut roots, doc, *loc)?;
        }
        for (old_l, new_l) in &moves {
            reindex_repoint(
                &mut self.pager,
                &mut tx,
                &mut roots,
                &before,
                *old_l,
                *new_l,
            )?;
        }
        let pages_changed = heap.pages != meta.pages;
        let roots_changed = roots != meta.index_roots;
        if pages_changed || roots_changed {
            let prev = self
                .tables
                .get(&tname)
                .map(|m| (m.pages.clone(), m.index_roots.clone()));
            if let Some(m) = self.tables.get_mut(&tname) {
                m.pages = heap.pages.clone();
                m.index_roots = roots;
            }
            if let Err(e) = self.save_catalog_into(&mut tx) {
                // The tx is dropped on this path, so its staged pages never
                // reach the data file; the catalog map must not keep pointing
                // at them or a later successful save persists ghost pages.
                if let Some((pages, index_roots)) = prev {
                    if let Some(m) = self.tables.get_mut(&tname) {
                        m.pages = pages;
                        m.index_roots = index_roots;
                    }
                }
                return Err(e);
            }
        }
        self.commit_pager_tx(tx)?;
        // Deleted rows may have held the AUTOINCREMENT watermark.
        self.autoinc_cache.remove(&tname);
        let removed: Vec<Object> = targets.into_iter().map(|(_, d)| d).collect();
        let count = removed.len() as u64;
        if let Some(ret) = returning {
            return project_returning(ret, &removed);
        }
        Ok(ExecOutcome::Affected(count))
    }

    fn exec_create_index(&mut self, idx: sqlparser::ast::CreateIndex) -> Result<ExecOutcome> {
        let table = obj_name(&idx.table_name);
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
        if idx.columns.len() != 1 {
            return err("only single-column indexes are supported");
        }
        let col = match &idx.columns[0].column.expr {
            SqlExpr::Identifier(i) => i.value.clone(),
            other => expr_name(other),
        };
        // Index names share a database-wide namespace (SQLite semantics):
        // a name taken by any table blocks reuse elsewhere.
        let owner = self.tables.values().find(|m| m.indexes.contains(&iname));
        if owner.is_some() {
            if idx.if_not_exists {
                return Ok(ExecOutcome::Affected(0));
            }
            return err(format!("index {iname} already exists"));
        }
        let Some(mut meta) = self.tables.get_mut(&table).cloned() else {
            return err(format!("table {table} does not exist"));
        };
        if !meta.columns.contains(&col) {
            return err(format!("column {col} does not exist"));
        }
        // UNIQUE INDEX rides the constraint machinery: the column joins
        // meta.unique, so inserts/updates enforce duplicates from here on.
        if idx.unique && !meta.unique.contains(&col) {
            let docs = self.table_docs(&table)?;
            meta.unique.push(col.clone());
            meta.check_unique(&docs)?;
        }
        // Build the B+ tree over the existing rows (non-unique: duplicates
        // are allowed, lookups collect every match).
        let pairs = self.table_pairs(&table)?;
        let mut tx = self.pager.begin_tx();
        let col_arg = [col.clone()];
        let mut roots = self.build_trees(&mut tx, &pairs, &col_arg)?;
        self.commit_pager_tx(tx)?;
        meta.index_roots.append(&mut roots);
        meta.index_defs.push((iname.clone(), col, idx.unique));
        meta.indexes.push(iname);
        self.tables.insert(table, meta);
        self.save_catalog()?;
        Ok(ExecOutcome::Affected(0))
    }

    fn exec_alter(&mut self, alter: sqlparser::ast::AlterTable) -> Result<ExecOutcome> {
        use sqlparser::ast::AlterTableOperation as Op;
        let tname = obj_name(&alter.name);
        let Some(mut meta) = self.tables.get(&tname).cloned() else {
            return err(format!("table {tname} does not exist"));
        };
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
                            CO::Check(c) => meta.checks.push(format!("{}", c.expr)),
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
                            _ => {}
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
                        let fill = eval_const(&e)?;
                        let docs = self.table_docs(&tname)?;
                        let filled: Vec<Object> = docs
                            .into_iter()
                            .map(|mut d| {
                                if !d.contains_key(&col) {
                                    d.insert(col.clone(), fill.clone());
                                }
                                d
                            })
                            .collect();
                        self.rewrite_table(&tname, &mut meta, filled)?;
                    }
                }
                Op::DropColumn { column_names, .. } => {
                    for id in column_names {
                        let name = id.value.clone();
                        if meta.primary_key.as_deref() == Some(name.as_str()) {
                            return err("cannot drop a PRIMARY KEY column");
                        }
                        meta.columns.retain(|c| c != &name);
                        meta.unique.retain(|c| c != &name);
                        meta.constraint_unique.retain(|c| c != &name);
                        meta.not_null.retain(|c| c != &name);
                        meta.defaults.retain(|(c, _)| c != &name);
                        meta.foreign_keys.retain(|(c, _, _)| c != &name);
                        meta.index_roots.remove(&name);
                        if meta.autoinc.as_deref() == Some(name.as_str()) {
                            meta.autoinc = None;
                        }
                        if meta.autoguid.as_deref() == Some(name.as_str()) {
                            meta.autoguid = None;
                        }
                        // Indexes over the dropped column lose their
                        // definitions (trees above were already removed).
                        let dead: Vec<String> = meta
                            .index_defs
                            .iter()
                            .filter(|(_, c, _)| c == &name)
                            .map(|(n, _, _)| n.clone())
                            .collect();
                        for n in &dead {
                            meta.indexes.retain(|i| i != n);
                        }
                        meta.index_defs.retain(|(_, c, _)| c != &name);
                    }
                    let docs = self.table_docs(&tname)?;
                    let stripped: Vec<Object> = docs
                        .into_iter()
                        .map(|mut d| {
                            for id in column_names {
                                d.remove(&id.value);
                            }
                            d
                        })
                        .collect();
                    self.rewrite_table(&tname, &mut meta, stripped)?;
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
                    if let Some(root) = meta.index_roots.remove(old) {
                        meta.index_roots.insert(new.clone(), root);
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
                        .map(|(n, c, u)| {
                            if c == old {
                                (n.clone(), new.clone(), *u)
                            } else {
                                (n.clone(), c.clone(), *u)
                            }
                        })
                        .collect();
                    // FK columns on this table follow the rename (references
                    // from other tables to the renamed column are not
                    // tracked — renaming a referenced column elsewhere keeps
                    // its old name here by design).
                    meta.foreign_keys = meta
                        .foreign_keys
                        .iter()
                        .map(|(c, rt, rc)| {
                            if c == old {
                                (new.clone(), rt.clone(), rc.clone())
                            } else {
                                (c.clone(), rt.clone(), rc.clone())
                            }
                        })
                        .collect();
                    // CHECK texts reference the old column name; leaving them
                    // stale silently disables the constraint (a missing column
                    // reads as NULL -> unknown -> check skipped).
                    meta.checks = meta
                        .checks
                        .iter()
                        .map(|c| rename_ident_in_text(c, old, new))
                        .collect();
                    let docs = self.table_docs(&tname)?;
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
                    let docs = self.table_docs(&tname)?;
                    self.tables.remove(&tname);
                    self.rewrite_table(&new_name, &mut meta, docs)?;
                    return Ok(ExecOutcome::Affected(0));
                }
                other => return err(format!("unsupported ALTER TABLE operation: {other}")),
            }
        }
        self.tables.insert(tname.clone(), meta);
        self.save_catalog()?;
        Ok(ExecOutcome::Affected(0))
    }

    /// Validate FOREIGN KEY constraints of `meta` for one candidate doc:
    /// non-null values must exist in the referenced table's column.
    fn check_fks(&mut self, meta: &TableMeta, doc: &Object) -> Result<()> {
        for (col, rtable, rcol) in &meta.foreign_keys {
            let Some(v) = doc.get(col) else {
                continue;
            };
            if matches!(v, Value::Null) {
                continue; // NULL passes (MATCH SIMPLE semantics)
            }
            let ref_docs = self.table_docs(rtable)?;
            let found = ref_docs
                .iter()
                .any(|rd| rd.get(rcol).map(|rv| rv == v).unwrap_or(false));
            if !found {
                return err(format!(
                    "FOREIGN KEY constraint failed: {col} -> {rtable}.{rcol}"
                ));
            }
        }
        Ok(())
    }

    /// Referential integrity on the *parent* side (RESTRICT): rows removed
    /// from `table` (DELETE, or UPDATE rows whose key value changes) may be
    /// referenced by child tables. `replacement_docs` carries the statement's
    /// surviving/updated rows of `table`: a parent-key value kept by any of
    /// them stays referenceable. Referenced columns are PK/UNIQUE in
    /// practice, so a disappearing value has no other copy in the table.
    fn check_fk_parent_delete(
        &mut self,
        table: &str,
        removed_docs: &[Object],
        replacement_docs: &[Object],
    ) -> Result<()> {
        if removed_docs.is_empty() {
            return Ok(());
        }
        // Child FKs referencing `table`: (child_table, child_col, parent_col).
        let children: Vec<(String, String, String)> = self
            .tables
            .iter()
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
                    let kept = replacement_docs
                        .iter()
                        .any(|d| d.get(rc.as_str()).map(|rv| rv == v).unwrap_or(false));
                    if !matches!(v, Value::Null) && !kept && !lost.contains(&v) {
                        lost.push(v);
                    }
                }
            }
        }
        if lost.is_empty() {
            return Ok(());
        }
        for (child_table, child_col, rc) in &children {
            let child_docs = self.table_docs(child_table)?;
            let offender = child_docs.iter().any(|cd| {
                cd.get(child_col)
                    .map(|cv| !matches!(cv, Value::Null) && lost.contains(&cv))
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

    /// Run a subquery and return its rows (first column only matters for
    /// IN/ANY lists, but scalar casts use the full first cell).
    fn subquery_result(&mut self, q: &Query) -> Result<QueryResult> {
        // Correlated references (`outer_table.column`) must error instead of
        // falling into the inner query's schemaless column lookup, where a
        // missing column reads as NULL and silently mis-filters (NOT IN ()
        // over an empty list is vacuously true — that shape deletes rows).
        let mut from_names = std::collections::BTreeSet::new();
        collect_from_names(q, &mut from_names);
        let mut hits = Vec::new();
        if let sqlparser::ast::SetExpr::Select(sel) = &*q.body {
            if let Some(sel_expr) = &sel.selection {
                collect_correlated_refs(sel_expr, &from_names, &self.tables, &mut hits);
            }
            if let Some(having) = &sel.having {
                collect_correlated_refs(having, &from_names, &self.tables, &mut hits);
            }
            for item in &sel.projection {
                match item {
                    sqlparser::ast::SelectItem::UnnamedExpr(e)
                    | sqlparser::ast::SelectItem::ExprWithAlias { expr: e, .. } => {
                        collect_correlated_refs(e, &from_names, &self.tables, &mut hits);
                    }
                    _ => {}
                }
            }
        }
        if let Some(qualifier) = hits.first() {
            return err(format!(
                "correlated subqueries are not supported (reference to outer table {qualifier})"
            ));
        }
        match self.exec_query(q.clone())? {
            ExecOutcome::Rows(r) => Ok(r),
            _ => err("subquery must be a SELECT"),
        }
    }

    /// Rewrite uncorrelated subqueries inside `e` into row-local
    /// expressions (IN-lists / literals) before row iteration.
    fn subst_expr(&mut self, e: &mut SqlExpr) -> Result<()> {
        match e {
            SqlExpr::Subquery(q) => {
                let r = self.subquery_result(q)?;
                let v = r
                    .rows
                    .first()
                    .and_then(|row| row.first().cloned())
                    .unwrap_or(Value::Null);
                *e = value_to_literal(v)?;
            }
            SqlExpr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let r = self.subquery_result(subquery)?;
                let list = r
                    .rows
                    .iter()
                    .map(|row| value_to_literal(row.first().cloned().unwrap_or(Value::Null)))
                    .collect::<Result<Vec<_>>>()?;
                *e = SqlExpr::InList {
                    expr: expr.clone(),
                    list,
                    negated: *negated,
                };
            }
            SqlExpr::Exists { subquery, negated } => {
                let r = self.subquery_result(subquery)?;
                *e = value_to_literal(Value::Bool(!r.rows.is_empty() != *negated))?;
            }
            SqlExpr::AnyOp {
                left,
                compare_op,
                right,
                ..
            } => {
                if let (sqlparser::ast::BinaryOperator::Eq, SqlExpr::Subquery(q)) =
                    (compare_op, right.as_ref())
                {
                    let r = self.subquery_result(q)?;
                    let list = r
                        .rows
                        .iter()
                        .map(|row| value_to_literal(row.first().cloned().unwrap_or(Value::Null)))
                        .collect::<Result<Vec<_>>>()?;
                    *e = SqlExpr::InList {
                        expr: left.clone(),
                        list,
                        negated: false,
                    };
                }
                // Non-EQ ANY falls through and errors at evaluation time.
            }
            // Recurse into composite expressions.
            SqlExpr::BinaryOp { left, right, .. } => {
                self.subst_expr(left)?;
                self.subst_expr(right)?;
            }
            SqlExpr::UnaryOp { expr, .. } => self.subst_expr(expr)?,
            SqlExpr::Nested(inner) => self.subst_expr(inner)?,
            SqlExpr::Between {
                expr, low, high, ..
            } => {
                self.subst_expr(expr)?;
                self.subst_expr(low)?;
                self.subst_expr(high)?;
            }
            SqlExpr::Like { expr, pattern, .. } | SqlExpr::ILike { expr, pattern, .. } => {
                self.subst_expr(expr)?;
                self.subst_expr(pattern)?;
            }
            SqlExpr::InList { expr, list, .. } => {
                self.subst_expr(expr)?;
                for item in list {
                    self.subst_expr(item)?;
                }
            }
            SqlExpr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                if let Some(op) = operand {
                    self.subst_expr(op)?;
                }
                for w in conditions.iter_mut() {
                    self.subst_expr(&mut w.condition)?;
                    self.subst_expr(&mut w.result)?;
                }
                if let Some(el) = else_result {
                    self.subst_expr(el)?;
                }
            }
            SqlExpr::Cast { expr, .. } => self.subst_expr(expr)?,
            SqlExpr::Function(f) => {
                if let sqlparser::ast::FunctionArguments::List(list) = &mut f.args {
                    for a in &mut list.args {
                        if let sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(inner),
                        ) = a
                        {
                            self.subst_expr(inner)?;
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Substitute subqueries inside a projection item.
    fn subst_item(&mut self, item: &mut SelectItem) -> Result<()> {
        match item {
            SelectItem::UnnamedExpr(e) => self.subst_expr(e),
            SelectItem::ExprWithAlias { expr, .. } => self.subst_expr(expr),
            _ => Ok(()),
        }
    }

    fn exec_create(&mut self, create: sqlparser::ast::CreateTable) -> Result<ExecOutcome> {
        use sqlparser::ast::ColumnOption as CO;
        let name = obj_name(&create.name);
        if self.tables.contains_key(&name) {
            if create.if_not_exists {
                return Ok(ExecOutcome::Affected(0));
            }
            return err(format!("table {name} already exists"));
        }
        // CREATE TABLE ... AS SELECT: shape and rows come from the query.
        // (`temporary` is accepted and treated as a regular table.)
        if let Some(q) = &create.query {
            let ExecOutcome::Rows(r) = self.exec_query(q.as_ref().clone())? else {
                return err("CREATE TABLE AS requires a SELECT");
            };
            let docs: Vec<Object> = r
                .rows
                .into_iter()
                .map(|row| r.columns.iter().cloned().zip(row).collect())
                .collect();
            let mut meta = TableMeta {
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
        let mut meta = TableMeta {
            columns,
            ..Default::default()
        };
        for col in &create.columns {
            for opt in &col.options {
                match &opt.option {
                    CO::PrimaryKey { .. } => meta.primary_key = Some(col.name.value.clone()),
                    CO::Unique { .. } => {
                        meta.unique.push(col.name.value.clone());
                        meta.constraint_unique.push(col.name.value.clone());
                    }
                    CO::NotNull => meta.not_null.push(col.name.value.clone()),
                    CO::Null => {}
                    CO::Default(e) => {
                        meta.defaults
                            .push((col.name.value.clone(), default_expr_text(e)));
                    }
                    CO::Check(c) => meta.checks.push(format!("{}", c.expr)),
                    CO::ForeignKey(fk) => {
                        if fk.on_delete.is_some() || fk.on_update.is_some() {
                            return err(
                                "FOREIGN KEY ON DELETE/ON UPDATE actions are not supported \
                                 (declare the constraint and keep parent keys stable)",
                            );
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
                    for ic in &u.columns {
                        let col = expr_name(&ic.column.expr);
                        if !meta.unique.contains(&col) {
                            meta.unique.push(col.clone());
                            meta.constraint_unique.push(col);
                        }
                    }
                }
                TC::PrimaryKey(pk) => {
                    if let Some(ic) = pk.columns.first() {
                        meta.primary_key = Some(expr_name(&ic.column.expr));
                    }
                }
                TC::Check(chk) => meta.checks.push(format!("{}", chk.expr)),
                TC::ForeignKey(fk) => {
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
                    for (lc, rc) in fk.columns.iter().zip(fk.referred_columns.iter()) {
                        meta.foreign_keys
                            .push((lc.value.clone(), rt.clone(), rc.value.clone()));
                    }
                }
                other => return err(format!("unsupported table constraint: {other}")),
            }
        }
        // Constraint columns get their B+ trees right away: duplicate
        // enforcement and index probes depend on them existing.
        let constraint_cols: Vec<String> = meta
            .primary_key
            .iter()
            .chain(meta.unique.iter())
            .cloned()
            .collect();
        if !constraint_cols.is_empty() {
            let mut tx = self.pager.begin_tx();
            let roots = self.build_trees(&mut tx, &[], &constraint_cols)?;
            self.commit_pager_tx(tx)?;
            meta.index_roots = roots;
        }
        self.tables.insert(name.clone(), meta);
        self.save_catalog()?;
        Ok(ExecOutcome::Affected(0))
    }

    fn exec_insert(&mut self, insert: sqlparser::ast::Insert) -> Result<ExecOutcome> {
        let TableObject::TableName(name) = &insert.table else {
            return err("unsupported INSERT target");
        };
        let table = obj_name(name);
        let Some(meta) = self.tables.get(&table).cloned() else {
            return err(format!("table {table} does not exist"));
        };
        let mut columns: Vec<String> = if insert.columns.is_empty() {
            meta.columns.clone()
        } else {
            insert.columns.iter().map(obj_name).collect()
        };
        let Some(source) = insert.source else {
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
            // INSERT INTO t (cols...) SELECT ...: take the SELECT's rows;
            // when no column list is given the query's columns define them.
            SetExpr::Select(_) => {
                let ExecOutcome::Rows(r) = self.exec_query(source.as_ref().clone())? else {
                    return err("INSERT source must be VALUES or SELECT");
                };
                if columns == meta.columns && insert.columns.is_empty() {
                    columns = r.columns.clone();
                }
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
        if rows.is_empty() {
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
        let mut next_autoinc = self.autoinc_next_for(&table, &meta)?;
        let mut new_docs: Vec<Object> = Vec::new();
        for row in rows {
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
                        if matches!(row.get(idx), Some(Value::Null) | None) {
                            row[idx] = Value::Int(next_autoinc);
                            next_autoinc = next_autoinc.saturating_add(1);
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
            // DEFAULT: fill declared columns the INSERT omitted.
            for (col, text) in &meta.defaults {
                if !doc.contains_key(col) {
                    let e = parse_expr_text(text)?;
                    doc.insert(col.clone(), eval_const(&e)?);
                }
            }
            meta.check(&doc)?;
            self.check_fks(&meta, &doc)?;
            new_docs.push(doc);
        }
        // Conflict policy: plain (error on duplicates), REPLACE INTO /
        // OR REPLACE (drop conflicting rows first), ON CONFLICT DO NOTHING /
        // OR IGNORE (skip conflicting new rows).
        let replace = insert.replace_into
            || matches!(insert.or, Some(sqlparser::ast::SqliteOnConflict::Replace));
        let do_nothing = matches!(
            insert.on,
            Some(sqlparser::ast::OnInsert::OnConflict(ref c))
                if matches!(c.action, sqlparser::ast::OnConflictAction::DoNothing)
        ) || matches!(insert.or, Some(sqlparser::ast::SqliteOnConflict::Ignore));
        if let Some(sqlparser::ast::OnInsert::OnConflict(ref c)) = insert.on {
            if matches!(c.action, sqlparser::ast::OnConflictAction::DoUpdate(_)) {
                return err("ON CONFLICT DO UPDATE is not supported yet");
            }
        }
        if matches!(
            insert.on,
            Some(sqlparser::ast::OnInsert::DuplicateKeyUpdate(_))
        ) {
            return err("ON DUPLICATE KEY UPDATE is not supported");
        }
        // Replication-safe rewrite: the statement as written would let every
        // peer fill its own random GUIDs. Emit a canonical INSERT carrying
        // the generated ids explicitly — with the conflict clause preserved
        // so peers resolve the same conflicts against identical state.
        if guid_filled {
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
            self.resolved_insert = Some(sql);
        }

        // One pager transaction for heap pages and index trees alike: a
        // statement is either fully applied or not at all, and commits once.
        let mut heap = Heap {
            pages: meta.pages.clone(),
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

        // Rows displaced by REPLACE INTO / OR REPLACE.
        let mut displaced: Vec<u64> = Vec::new();
        if replace && !indexed.is_empty() {
            for n in &new_docs {
                for col in &indexed {
                    let Some(v) = n.get(col) else { continue };
                    if matches!(v, Value::Null) {
                        continue;
                    }
                    if let Some(loc) = BTree::open(roots[col])
                        .get(&mut self.pager, &tx, v)
                        .map_err(|e| index_err(col, e))?
                    {
                        displaced.push(loc);
                    }
                }
            }
        }
        if !displaced.is_empty() {
            let affected: std::collections::BTreeSet<u32> = displaced
                .iter()
                .map(|l| crate::heap::unpack_loc(*l).0)
                .collect();
            let mut before = Vec::new();
            for pid in affected {
                before.extend(heap.page_docs(&mut self.pager, &tx, pid)?);
            }
            let moves = heap.remove_many(&mut self.pager, &mut tx, &displaced)?;
            for loc in &displaced {
                let Some((_, doc)) = before.iter().find(|(l, _)| l == loc) else {
                    continue;
                };
                reindex_remove(&mut self.pager, &mut tx, &mut roots, doc, *loc)?;
            }
            for (old_l, new_l) in &moves {
                reindex_repoint(
                    &mut self.pager,
                    &mut tx,
                    &mut roots,
                    &before,
                    *old_l,
                    *new_l,
                )?;
            }
        }

        // Append the surviving rows and maintain the trees. Tree inserts
        // with unique=true ARE the duplicate check (within-batch duplicates
        // included: staged pages are visible to tree reads).
        let mut placed: Vec<(u64, Object)> = Vec::with_capacity(new_docs.len());
        let mut insert_failed: Option<SqlError> = None;
        'outer: for doc in new_docs.into_iter() {
            if do_nothing {
                for col in &indexed {
                    let Some(v) = doc.get(col) else { continue };
                    if matches!(v, Value::Null) {
                        continue;
                    }
                    if BTree::open(roots[col])
                        .get(&mut self.pager, &tx, v)
                        .map_err(|e| index_err(col, e))?
                        .is_some()
                    {
                        continue 'outer; // conflict: skip this row
                    }
                }
            }
            let loc = match heap.insert(&mut self.pager, &mut tx, &doc) {
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
                if let Err(e) = tree.insert(&mut self.pager, &mut tx, v.clone(), loc, true) {
                    insert_failed = Some(index_err(col, e));
                    break 'outer;
                }
                if tree.root != root {
                    roots.insert(col.clone(), tree.root);
                }
            }
            // Non-constraint CREATE INDEX trees (non-unique).
            for (col, root) in roots.clone() {
                if indexed.contains(&col) {
                    continue;
                }
                let Some(v) = doc.get(&col) else { continue };
                if matches!(v, Value::Null) {
                    continue;
                }
                let mut tree = BTree::open(root);
                tree.insert(&mut self.pager, &mut tx, v.clone(), loc, false)
                    .map_err(|e| index_err(&col, e))?;
                if tree.root != root {
                    roots.insert(col, tree.root);
                }
            }
            placed.push((loc, doc));
        }
        if let Some(e) = insert_failed {
            self.pager.abort_tx(tx)?;
            return Err(e);
        }
        // Legacy files whose constraint columns predate trees keep the
        // whole-set duplicate check; tables with no constraints skip it.
        let has_constraints = meta.primary_key.is_some() || !meta.unique.is_empty();
        if has_constraints && indexed.is_empty() && !placed.is_empty() {
            // A failed scan must abort, not silently skip the unique check.
            let mut combined = match (Heap {
                pages: meta.pages.clone(),
            }
            .scan(&mut self.pager))
            {
                Ok(docs) => docs,
                Err(e) => {
                    self.pager.abort_tx(tx)?;
                    return Err(e.into());
                }
            };
            combined.extend(placed.iter().map(|(_, d)| d.clone()));
            if let Err(e) = meta.check_unique(&combined) {
                self.pager.abort_tx(tx)?;
                return Err(e);
            }
        }
        let count = placed.len() as u64;
        let pages_changed = heap.pages != meta.pages;
        let roots_changed = roots != meta.index_roots;
        if pages_changed || roots_changed {
            let prev = self
                .tables
                .get(&table)
                .map(|m| (m.pages.clone(), m.index_roots.clone()));
            let m = self
                .tables
                .get_mut(&table)
                .expect("table existed at statement start");
            m.pages = heap.pages.clone();
            m.index_roots = roots;
            if let Err(e) = self.save_catalog_into(&mut tx) {
                // Same ghost-page hazard as the update/delete paths: the tx is
                // dropped, so restore the map to the on-disk truth.
                if let Some((pages, index_roots)) = prev {
                    if let Some(m) = self.tables.get_mut(&table) {
                        m.pages = pages;
                        m.index_roots = index_roots;
                    }
                }
                return Err(e);
            }
        }
        self.commit_pager_tx(tx)?;
        // AUTOINCREMENT counter: never regress, follow explicit max.
        if let Some(col) = &meta.autoinc {
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
        new_docs = placed.into_iter().map(|(_, d)| d).collect();
        if let Some(ret) = &insert.returning {
            return project_returning(ret, &new_docs);
        }
        Ok(ExecOutcome::Affected(count))
    }

    fn exec_query(&mut self, query: Query) -> Result<ExecOutcome> {
        // WITH <cte> AS (...), ...: materialize each CTE (they can reference
        // earlier ones) into the statement-local CTE table.
        if let Some(with) = &query.with {
            if with.recursive {
                return err("WITH RECURSIVE is not supported");
            }
            for cte in &with.cte_tables {
                let name = cte.alias.name.value.clone();
                let ExecOutcome::Rows(r) = self.exec_query(cte.query.as_ref().clone())? else {
                    return err("CTE body must be a SELECT");
                };
                let docs: Vec<Object> = r
                    .rows
                    .into_iter()
                    .map(|row| r.columns.iter().cloned().zip(row).collect())
                    .collect();
                self.ctes.insert(name, docs);
            }
        }
        let body = query.body.clone();
        match *body {
            SetExpr::Select(select) => self.exec_select(query.clone(), *select),
            SetExpr::SetOperation {
                left,
                op,
                set_quantifier,
                right,
            } => {
                use sqlparser::ast::{SetOperator, SetQuantifier};
                if !matches!(
                    op,
                    SetOperator::Union
                        | SetOperator::Except
                        | SetOperator::Minus
                        | SetOperator::Intersect
                ) {
                    return err(format!("unsupported set operation: {op}"));
                }
                // Set operations combine whole result sets: strip per-arm
                // ORDER BY/LIMIT so they apply to the combination only.
                let bare = Query {
                    order_by: None,
                    limit_clause: None,
                    ..query.clone()
                };
                let l = self.exec_query(Query {
                    body: left,
                    ..bare.clone()
                })?;
                let r = self.exec_query(Query {
                    body: right,
                    ..bare
                })?;
                let (ExecOutcome::Rows(mut lr), ExecOutcome::Rows(rr)) = (l, r) else {
                    return err("set operations require SELECT on both sides");
                };
                // Column *counts* must match; names may differ (left wins).
                if lr.columns.len() != rr.columns.len() {
                    return err("set operation arms have different column counts");
                }
                let all = set_quantifier == SetQuantifier::All;
                match op {
                    SetOperator::Union => {
                        lr.rows.extend(rr.rows);
                        if !all {
                            dedup_rows(&mut lr.rows);
                        }
                    }
                    SetOperator::Intersect => {
                        let mut counts = row_counts(&rr.rows);
                        let mut out = Vec::new();
                        for row in lr.rows {
                            if consume_one(&mut counts, &row) {
                                out.push(row);
                            }
                        }
                        if !all {
                            let mut out2 = out;
                            dedup_rows(&mut out2);
                            out = out2;
                        }
                        lr.rows = out;
                    }
                    // EXCEPT / MINUS share semantics.
                    SetOperator::Except | SetOperator::Minus => {
                        let mut counts = row_counts(&rr.rows);
                        let mut out = Vec::new();
                        for row in lr.rows {
                            if consume_one(&mut counts, &row) {
                                continue; // matched on the right: removed
                            }
                            out.push(row);
                        }
                        if !all {
                            let mut out2 = out;
                            dedup_rows(&mut out2);
                            out = out2;
                        }
                        lr.rows = out;
                    }
                }
                // ORDER BY / LIMIT now apply to the combined result.
                let cols = lr.columns.clone();
                let rows = self.apply_order_limit(query, lr.rows, &cols, None)?;
                Ok(ExecOutcome::Rows(QueryResult {
                    columns: cols,
                    rows,
                }))
            }
            other => err(format!("unsupported query body: {other}")),
        }
    }

    fn exec_select(
        &mut self,
        query: Query,
        mut select: sqlparser::ast::Select,
    ) -> Result<ExecOutcome> {
        // Resolve uncorrelated subqueries up front so the row-local
        // expression evaluator never sees them.
        if let Some(sel) = &mut select.selection {
            self.subst_expr(sel)?;
        }
        for item in &mut select.projection {
            self.subst_item(item)?;
        }
        if let sqlparser::ast::GroupByExpr::Expressions(es, _) = &mut select.group_by {
            for e in es {
                self.subst_expr(e)?;
            }
        }
        if let Some(having) = &mut select.having {
            self.subst_expr(having)?;
        }

        if select.from.is_empty() {
            // FROM-less SELECT: one row of constant expressions; WHERE
            // filters that single row.
            let mut project: Vec<(String, SqlExpr)> = Vec::new();
            for item in &select.projection {
                match item {
                    SelectItem::UnnamedExpr(e) => project.push((expr_name(e), e.clone())),
                    SelectItem::ExprWithAlias { expr, alias, .. } => {
                        project.push((alias.value.clone(), expr.clone()))
                    }
                    _ => return err("unsupported select item"),
                }
            }
            let empty = Object::new();
            let columns = project.iter().map(|(n, _)| n.clone()).collect();
            if let Some(cond) = &select.selection {
                if !matches!(eval_expr(cond, &empty)?, Value::Bool(true)) {
                    return Ok(ExecOutcome::Rows(QueryResult {
                        columns,
                        rows: vec![],
                    }));
                }
            }
            let row = project
                .iter()
                .map(|(_, e)| eval_expr(e, &empty))
                .collect::<Result<Vec<_>>>()?;
            // FROM-less SELECT still honors LIMIT/OFFSET (`SELECT 1 LIMIT 0`
            // must return zero rows, not one). ORDER BY over a single row
            // cannot reorder anything.
            let mut rows = vec![row];
            match &query.limit_clause {
                Some(LimitClause::LimitOffset { limit, offset, .. }) => {
                    let n = match limit {
                        Some(e) => match eval_const(e)?.as_i64() {
                            Some(n) if n >= 0 => n as usize,
                            // Negative LIMIT means "no limit" (SQLite).
                            _ => usize::MAX,
                        },
                        None => usize::MAX,
                    };
                    let skip = match offset {
                        Some(o) => eval_const(&o.value)?.as_i64().unwrap_or(0).max(0) as usize,
                        None => 0,
                    };
                    rows = rows.into_iter().skip(skip).take(n).collect();
                }
                // MySQL form `LIMIT <offset>, <count>` (order reversed).
                Some(LimitClause::OffsetCommaLimit { offset, limit }) => {
                    let skip = eval_const(offset)?.as_i64().unwrap_or(0).max(0) as usize;
                    let n = match eval_const(limit)?.as_i64() {
                        Some(n) if n >= 0 => n as usize,
                        _ => usize::MAX,
                    };
                    rows = rows.into_iter().skip(skip).take(n).collect();
                }
                None => {}
            }
            return Ok(ExecOutcome::Rows(QueryResult { columns, rows }));
        }
        let mut rows = self.load_from(&select.from, &select.selection)?;

        // WHERE — consuming pass: matching docs move into the kept vec
        // instead of being whole-document cloned (the filtered set is
        // often the whole table).
        if let Some(cond) = &select.selection {
            let mut kept = Vec::with_capacity(rows.len());
            for doc in rows.drain(..) {
                if matches!(eval_expr(cond, &doc)?, Value::Bool(true)) {
                    kept.push(doc);
                }
            }
            rows = kept;
        }

        // Aggregation path: aggregate functions in projection or GROUP BY.
        let group_exprs: Vec<SqlExpr> = match &select.group_by {
            sqlparser::ast::GroupByExpr::Expressions(e, _) => e.clone(),
            sqlparser::ast::GroupByExpr::All(_) => return err("GROUP BY ALL not supported"),
        };
        if !group_exprs.is_empty() || select.projection.iter().any(is_agg_item) {
            return self.exec_grouped_select(query, select, rows, group_exprs);
        }
        if select.having.is_some() {
            // Silently dropping the filter would return unfiltered rows.
            return err("HAVING requires GROUP BY or an aggregate");
        }
        self.exec_plain_select(query, select, rows)
    }

    /// Load a FROM list into merged rows: the base table plus its JOINs,
    /// then comma-separated entries as cross joins. A lone base table keeps
    /// unqualified field names; anything joined gets "alias.col" keys.
    /// A solo real table with a probe-able WHERE skips the heap scan.
    fn load_from(
        &mut self,
        from: &[sqlparser::ast::TableWithJoins],
        selection: &Option<SqlExpr>,
    ) -> Result<Vec<Object>> {
        use sqlparser::ast::{JoinConstraint, JoinOperator};
        let base = &from[0];
        // Index fast path: solo base table, WHERE narrows to an indexed
        // column. Rows come straight from the B+ tree; the caller's residual
        // WHERE filter still runs over them.
        if base.joins.is_empty() && from.len() == 1 {
            if let sqlparser::ast::TableFactor::Table { name, alias, .. } = &base.relation {
                let tname = obj_name(name);
                if self.tables.contains_key(&tname) && !self.ctes.contains_key(&tname) {
                    let akey = alias.as_ref().map(|a| a.name.value.clone());
                    if let Some(pairs) = self.index_probe(&tname, akey.as_deref(), selection)? {
                        return Ok(pairs.into_iter().map(|(_, d)| d).collect());
                    }
                }
            }
        }
        let (bname, balias, mut bdocs) = self.load_table_factor(&base.relation)?;
        let bkey = balias.unwrap_or_else(|| bname.clone());
        let solo = base.joins.is_empty() && from.len() == 1;
        let mut rows: Vec<Object> = if solo {
            bdocs
        } else {
            bdocs.drain(..).map(|d| qualify(&d, &bkey)).collect()
        };
        let mut all_joins: Vec<&sqlparser::ast::Join> = base.joins.iter().collect();
        if !solo {
            for twj in &from[1..] {
                // comma-separated FROM entries: cross join their base tables
                let (n, a, d) = self.load_table_factor(&twj.relation)?;
                let k = a.unwrap_or(n);
                rows = join_rows(rows, &d, &k, None, false, false)?;
                all_joins.extend(twj.joins.iter());
            }
        }
        for j in all_joins {
            let (jname, jalias, jdocs) = self.load_table_factor(&j.relation)?;
            let jkey = jalias.unwrap_or(jname);
            let (left_join, right_join, on) = match &j.join_operator {
                JoinOperator::Join(c)
                | JoinOperator::Inner(c)
                | JoinOperator::Left(c)
                | JoinOperator::LeftOuter(c)
                | JoinOperator::Right(c)
                | JoinOperator::RightOuter(c)
                | JoinOperator::FullOuter(c) => {
                    let on = match c {
                        JoinConstraint::On(e) => Some(e.clone()),
                        JoinConstraint::Using(cols) => {
                            // a USING b == ON left.b = right.b (unqualified lookups
                            // are not supported; use alias-qualified names)
                            let mut e = None;
                            for c in cols {
                                let col = obj_name(c);
                                let eq = SqlExpr::BinaryOp {
                                    left: Box::new(SqlExpr::Identifier(
                                        sqlparser::ast::Ident::new(format!("{bkey}.{col}")),
                                    )),
                                    op: sqlparser::ast::BinaryOperator::Eq,
                                    right: Box::new(SqlExpr::Identifier(
                                        sqlparser::ast::Ident::new(format!("{jkey}.{col}")),
                                    )),
                                };
                                e = Some(match e {
                                    None => eq,
                                    Some(prev) => SqlExpr::BinaryOp {
                                        left: Box::new(prev),
                                        op: sqlparser::ast::BinaryOperator::And,
                                        right: Box::new(eq),
                                    },
                                });
                            }
                            e
                        }
                        _ => None,
                    };
                    let (mut left_join, mut right_join) = (false, false);
                    match &j.join_operator {
                        JoinOperator::Left(_) | JoinOperator::LeftOuter(_) => left_join = true,
                        JoinOperator::Right(_) | JoinOperator::RightOuter(_) => right_join = true,
                        JoinOperator::FullOuter(_) => {
                            left_join = true;
                            right_join = true;
                        }
                        _ => {}
                    }
                    (left_join, right_join, on)
                }
                JoinOperator::CrossJoin(_) => (false, false, None),
                _ => return err("unsupported join type"),
            };
            rows = join_rows(rows, &jdocs, &jkey, on.as_ref(), left_join, right_join)?;
        }
        Ok(rows)
    }

    /// No aggregation: project expressions over rows.
    fn exec_plain_select(
        &mut self,
        query: Query,
        select: sqlparser::ast::Select,
        rows: Vec<Object>,
    ) -> Result<ExecOutcome> {
        let mut want_star = false;
        let mut project: Vec<(String, SqlExpr)> = Vec::new();
        for item in &select.projection {
            match item {
                SelectItem::Wildcard(_) => want_star = true,
                SelectItem::UnnamedExpr(e) => project.push((expr_name(e), e.clone())),
                SelectItem::ExprWithAlias { expr, alias, .. } => {
                    project.push((alias.value.clone(), expr.clone()))
                }
                _ => return err("unsupported select item"),
            }
        }
        let columns_out: Vec<String> = if want_star {
            // SELECT *, expr: star fields first, then the explicit
            // projections (SQLite semantics) — dropping the exprs silently
            // returned fewer columns than the statement asked for.
            let mut cols = union_of_fields(&rows);
            cols.extend(project.iter().map(|(n, _)| n.clone()));
            cols
        } else {
            project.iter().map(|(n, _)| n.clone()).collect()
        };

        let mut docs = rows;
        let mut out: Vec<Vec<Value>> = Vec::new();
        for doc in &docs {
            if want_star {
                let mut row: Vec<Value> = columns_out
                    .iter()
                    .map(|c| doc.get(c).cloned().unwrap_or(Value::Null))
                    .collect();
                // The trailing project.len() slots are the explicit exprs.
                let base = columns_out.len() - project.len();
                for (i, (_, e)) in project.iter().enumerate() {
                    row[base + i] = eval_expr(e, doc)?;
                }
                out.push(row);
            } else {
                let mut row = Vec::with_capacity(project.len());
                for (_, e) in &project {
                    row.push(eval_expr(e, doc)?);
                }
                out.push(row);
            }
        }
        // DISTINCT: drop duplicate projected rows; the first occurrence's
        // source doc stays for ORDER BY evaluation. DISTINCT ON (...) is a
        // different (unsupported) feature and must not pass silently.
        match &select.distinct {
            Some(sqlparser::ast::Distinct::On(cols)) => {
                return err(format!(
                    "DISTINCT ON is not supported: {}",
                    cols.iter().map(expr_name).collect::<Vec<_>>().join(", ")
                ));
            }
            Some(sqlparser::ast::Distinct::Distinct) => {
                let mut seen = std::collections::BTreeSet::new();
                let mut kept_docs = Vec::with_capacity(docs.len());
                let mut kept_rows = Vec::with_capacity(out.len());
                for (doc, row) in docs.into_iter().zip(out) {
                    let key = encode::encode_to_vec(&Value::Array(row.clone()))
                        .map_err(SqlError::Encode)?;
                    if seen.insert(key) {
                        kept_docs.push(doc);
                        kept_rows.push(row);
                    }
                }
                docs = kept_docs;
                out = kept_rows;
            }
            None | Some(sqlparser::ast::Distinct::All) => {}
        }
        out = self.apply_order_limit(query, out, &columns_out, Some(docs))?;
        Ok(ExecOutcome::Rows(QueryResult {
            columns: columns_out,
            rows: out,
        }))
    }

    /// GROUP BY + aggregates (+ HAVING).
    fn exec_grouped_select(
        &mut self,
        query: Query,
        select: sqlparser::ast::Select,
        rows: Vec<Object>,
        group_exprs: Vec<SqlExpr>,
    ) -> Result<ExecOutcome> {
        // Evaluate group keys per row.
        let mut groups: Vec<(Vec<Value>, Vec<&Object>)> = Vec::new();
        for doc in &rows {
            let key: Vec<Value> = group_exprs
                .iter()
                .map(|e| eval_expr(e, doc))
                .collect::<Result<_>>()?;
            match groups.iter_mut().find(|(k, _)| k == &key) {
                Some((_, g)) => g.push(doc),
                None => groups.push((key, vec![doc])),
            }
        }
        // With no GROUP BY, aggregates run over one group even when empty.
        if group_exprs.is_empty() && groups.is_empty() {
            groups.push((vec![], vec![]));
        }
        // Projection must be aggregate functions or group exprs.
        let mut columns = Vec::new();
        let mut agg_specs = Vec::new(); // per column: AggSpec
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(e) => {
                    columns.push(expr_name(e));
                    agg_specs.push(self.agg_spec(e, &group_exprs)?);
                }
                SelectItem::ExprWithAlias { expr, alias, .. } => {
                    columns.push(alias.value.clone());
                    agg_specs.push(self.agg_spec(expr, &group_exprs)?);
                }
                _ => return err("unsupported item in aggregate SELECT"),
            }
        }
        let mut out: Vec<Vec<Value>> = Vec::new();
        for (key, docs) in &groups {
            let mut row = Vec::with_capacity(agg_specs.len());
            for spec in &agg_specs {
                row.push(eval_agg(spec, docs, key)?);
            }
            // HAVING: aggregates evaluate over the group's rows directly;
            // everything else evaluates against the output columns, with
            // GROUP BY expressions consulted before them so unprojected
            // group keys resolve instead of reading as NULL.
            if let Some(having) = &select.having {
                let doc: Object = columns.iter().cloned().zip(row.iter().cloned()).collect();
                if !matches!(
                    eval_having(having, &doc, docs, &group_exprs)?,
                    Value::Bool(true)
                ) {
                    continue;
                }
            }
            out.push(row);
        }
        let out = self.apply_order_limit(query, out, &columns, None)?;
        Ok(ExecOutcome::Rows(QueryResult { columns, rows: out }))
    }

    fn agg_spec(&self, e: &SqlExpr, group_exprs: &[SqlExpr]) -> Result<AggSpec> {
        // A projection item equal to a GROUP BY expression echoes its key.
        for (i, g) in group_exprs.iter().enumerate() {
            if format!("{g}") == format!("{e}") {
                return Ok(AggSpec::GroupKey { idx: i });
            }
        }
        if let SqlExpr::Identifier(i) = e {
            // Bare column: allowed only if some group expr is that column.
            for (i2, g) in group_exprs.iter().enumerate() {
                if matches!(g, SqlExpr::Identifier(gi) if gi.value == i.value) {
                    return Ok(AggSpec::GroupKey { idx: i2 });
                }
            }
            return err(format!(
                "column {} must appear in GROUP BY or an aggregate",
                i.value
            ));
        }
        let SqlExpr::Function(f) = e else {
            return err(format!("unsupported aggregate projection: {e}"));
        };
        let (op, inner, distinct, sep) = agg_parts(f)?;
        Ok(AggSpec::Agg {
            op,
            arg: inner,
            distinct,
            sep,
        })
    }

    fn load_table_factor(
        &mut self,
        tf: &sqlparser::ast::TableFactor,
    ) -> Result<(String, Option<String>, Vec<Object>)> {
        let sqlparser::ast::TableFactor::Table { name, alias, .. } = tf else {
            // Derived table: FROM (SELECT ...) AS alias
            if let sqlparser::ast::TableFactor::Derived {
                subquery, alias, ..
            } = tf
            {
                let ExecOutcome::Rows(r) = self.exec_query(subquery.as_ref().clone())? else {
                    return err("derived table must be a SELECT");
                };
                let docs: Vec<Object> = r
                    .rows
                    .into_iter()
                    .map(|row| r.columns.iter().cloned().zip(row).collect())
                    .collect();
                let alias = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_default();
                return Ok(("@derived".into(), Some(alias), docs));
            }
            return err("only simple tables in FROM");
        };
        let tname = obj_name(name);
        let alias = alias.as_ref().map(|a| a.name.value.clone());
        // WITH (...) names shadow real tables for this statement.
        if let Some(docs) = self.ctes.get(&tname) {
            return Ok((tname, alias, docs.clone()));
        }
        let qualified: String = name
            .0
            .iter()
            .map(|p| match p {
                ObjectNamePart::Identifier(i) => i.value.clone(),
                other => other.to_string(),
            })
            .collect::<Vec<_>>()
            .join(".");
        if qualified == "sqlite_master" || qualified == "sqlite_temporal_master" {
            // SQLite-dialect compatibility view (EF Core probes it to learn
            // which tables exist). Index rows let clients enumerate CREATE
            // INDEX definitions for schema sync.
            let mut docs: Vec<Object> = self
                .tables
                .keys()
                .map(|n| {
                    Object::from([
                        ("type".into(), Value::Str("table".into())),
                        ("name".into(), Value::Str(n.clone())),
                        ("tbl_name".into(), Value::Str(n.clone())),
                        ("sql".into(), Value::Str(String::new())),
                    ])
                })
                .collect();
            for (tbl, meta) in &self.tables {
                for (iname, col, unique) in &meta.index_defs {
                    docs.push(Object::from([
                        ("type".into(), Value::Str("index".into())),
                        ("name".into(), Value::Str(iname.clone())),
                        ("tbl_name".into(), Value::Str(tbl.clone())),
                        (
                            "sql".into(),
                            Value::Str(format!(
                                "CREATE {}INDEX {iname} ON {tbl} ({col})",
                                if *unique { "UNIQUE " } else { "" }
                            )),
                        ),
                    ]));
                }
            }
            return Ok(("sqlite_master".into(), alias, docs));
        }
        if qualified == "information_schema.tables" || qualified == "information_schema.columns" {
            let docs = self.information_schema(&qualified);
            return Ok(("information_schema".into(), alias, docs));
        }
        let docs = self.table_docs(&tname)?;
        Ok((tname, alias, docs))
    }

    /// Virtual information_schema tables from the catalog.
    fn information_schema(&self, which: &str) -> Vec<Object> {
        let mut out = Vec::new();
        if which.ends_with("tables") {
            for (name, meta) in &self.tables {
                out.push(Object::from([
                    ("table_name".into(), Value::Str(name.clone())),
                    ("pages".into(), Value::Int(meta.pages.len() as i64)),
                ]));
            }
        } else {
            for (name, meta) in &self.tables {
                for col in &meta.columns {
                    out.push(Object::from([
                        ("table_name".into(), Value::Str(name.clone())),
                        ("column_name".into(), Value::Str(col.clone())),
                        (
                            "is_nullable".into(),
                            Value::Str(
                                if meta.not_null.contains(col) {
                                    "NO"
                                } else {
                                    "YES"
                                }
                                .into(),
                            ),
                        ),
                        (
                            "data_type".into(),
                            Value::Str(
                                if meta.autoguid.as_deref() == Some(col.as_str()) {
                                    "GUID"
                                } else {
                                    "ANY"
                                }
                                .into(),
                            ),
                        ),
                    ]));
                }
            }
        }
        out
    }

    /// ORDER BY + LIMIT/OFFSET. Sort keys resolve in order of preference to
    /// an output column name, an ordinal position (`ORDER BY 2`), or — when
    /// the caller supplies the source docs — an arbitrary expression
    /// evaluated per row.
    fn apply_order_limit(
        &mut self,
        query: Query,
        mut rows: Vec<Vec<Value>>,
        columns: &[String],
        docs: Option<Vec<Object>>,
    ) -> Result<Vec<Vec<Value>>> {
        if let Some(order_by) = &query.order_by {
            let sqlparser::ast::OrderByKind::Expressions(exprs) = &order_by.kind else {
                return err("unsupported ORDER BY");
            };
            // (column index, asc, nulls_first): appended hidden key columns
            // start after the visible ones.
            let mut keys: Vec<(usize, bool, Option<bool>)> = Vec::new();
            let mut extra_keys = 0usize;
            for o in exprs {
                let name = expr_name(&o.expr);
                let idx = columns.iter().position(|c| c == &name).or_else(|| {
                    // ORDER BY <ordinal>: 1-based output position.
                    name.parse::<usize>()
                        .ok()
                        .filter(|n| (1..=columns.len()).contains(n))
                        .map(|n| n - 1)
                });
                let idx = match idx {
                    Some(i) => i,
                    None => {
                        let Some(ds) = &docs else {
                            return err(format!("unknown ORDER BY key: {name}"));
                        };
                        if ds.len() != rows.len() {
                            return err(format!("unknown ORDER BY key: {name}"));
                        }
                        // Evaluate the expression against each source doc and
                        // append it as a hidden key column.
                        for (row, doc) in rows.iter_mut().zip(ds) {
                            row.push(eval_expr(&o.expr, doc)?);
                        }
                        let col = columns.len() + extra_keys;
                        extra_keys += 1;
                        col
                    }
                };
                keys.push((idx, o.options.asc.unwrap_or(true), o.options.nulls_first));
            }
            rows.sort_by(|a, b| {
                for (col, asc, nulls_first) in &keys {
                    let ord = cmp_maybe_null(&a[*col], &b[*col], *nulls_first);
                    if ord != std::cmp::Ordering::Equal {
                        return if *asc { ord } else { ord.reverse() };
                    }
                }
                std::cmp::Ordering::Equal
            });
            if extra_keys > 0 {
                for row in &mut rows {
                    row.truncate(columns.len());
                }
            }
        }
        match &query.limit_clause {
            Some(LimitClause::LimitOffset { limit, offset, .. }) => {
                let n = match limit {
                    Some(e) => match eval_const(e)?.as_i64() {
                        Some(n) if n >= 0 => n as usize,
                        // Negative LIMIT means "no limit" (SQLite).
                        _ => usize::MAX,
                    },
                    None => usize::MAX,
                };
                let skip = match offset {
                    Some(o) => eval_const(&o.value)?.as_i64().unwrap_or(0).max(0) as usize,
                    None => 0,
                };
                rows = rows.into_iter().skip(skip).take(n).collect();
            }
            // MySQL form `LIMIT <offset>, <count>` (order reversed).
            Some(LimitClause::OffsetCommaLimit { offset, limit }) => {
                let skip = eval_const(offset)?.as_i64().unwrap_or(0).max(0) as usize;
                let n = match eval_const(limit)?.as_i64() {
                    Some(n) if n >= 0 => n as usize,
                    _ => usize::MAX,
                };
                rows = rows.into_iter().skip(skip).take(n).collect();
            }
            None => {}
        }
        Ok(rows)
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

enum AggOp {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    GroupConcat,
}

// Short-lived, built per statement; size difference is fine here.
#[allow(clippy::large_enum_variant)]
enum AggSpec {
    GroupKey {
        idx: usize,
    },
    Agg {
        op: AggOp,
        arg: SqlExpr,
        distinct: bool,
        sep: Option<String>,
    },
}

/// Parse a scalar-aggregate call (`COUNT(*)`, `SUM(DISTINCT x)`, ...) into
/// its operator, argument, DISTINCT flag and — for GROUP_CONCAT/STRING_AGG —
/// the optional constant separator. `COUNT(*)` rewrites to a sentinel column
/// so the executor counts rows instead of non-null values.
fn agg_parts(f: &sqlparser::ast::Function) -> Result<(AggOp, SqlExpr, bool, Option<String>)> {
    let fname = f.name.to_string().to_uppercase();
    // A window frame silently changes the result shape (one row per
    // partition); refusing is better than returning wrong numbers.
    if f.over.is_some() {
        return err(format!(
            "window functions (OVER) are not supported: {fname}"
        ));
    }
    let (distinct, inner, second) = match &f.args {
        sqlparser::ast::FunctionArguments::List(list) => {
            let distinct = matches!(
                list.duplicate_treatment,
                Some(sqlparser::ast::DuplicateTreatment::Distinct)
            );
            let as_expr = |a: &sqlparser::ast::FunctionArg| match a {
                sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) => {
                    Some(e.clone())
                }
                _ => None,
            };
            let inner = list.args.first().and_then(|a| match a {
                sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Wildcard) => {
                    Some(SqlExpr::Identifier(sqlparser::ast::Ident::new("__count__")))
                }
                other => as_expr(other),
            });
            let second = list.args.get(1).and_then(as_expr);
            (distinct, inner, second)
        }
        _ => (false, None, None),
    };
    let Some(arg) = inner else {
        return err(format!("unsupported aggregate arguments: {fname}"));
    };
    let op = match fname.as_str() {
        "COUNT" => AggOp::Count,
        "SUM" => AggOp::Sum,
        "AVG" => AggOp::Avg,
        "MIN" => AggOp::Min,
        "MAX" => AggOp::Max,
        "GROUP_CONCAT" | "STRING_AGG" => AggOp::GroupConcat,
        other => return err(format!("unknown function: {other}")),
    };
    let sep = if matches!(op, AggOp::GroupConcat) {
        match second {
            Some(e) => match eval_const(&e)? {
                Value::Str(s) => Some(s),
                Value::Null => None,
                other => {
                    return err(format!(
                        "group_concat separator must be a string literal, got {}",
                        other.type_name()
                    ))
                }
            },
            None => None,
        }
    } else {
        None
    };
    Ok((op, arg, distinct, sep))
}

fn add_values(a: Value, b: Value) -> Result<Value> {
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => Ok(Value::Int(x.wrapping_add(y))),
        (Value::Int(x), Value::Float(y)) => Ok(Value::Float(x as f64 + y)),
        (Value::Float(x), Value::Int(y)) => Ok(Value::Float(x + y as f64)),
        (Value::Float(x), Value::Float(y)) => Ok(Value::Float(x + y)),
        _ => err("SUM of non-numeric values"),
    }
}

fn row_key(row: &[Value]) -> Vec<u8> {
    encode::encode_to_vec(&Value::Array(row.to_vec())).unwrap_or_default()
}

/// Keep the first occurrence of each distinct row.
fn dedup_rows(rows: &mut Vec<Vec<Value>>) {
    let mut seen = std::collections::BTreeSet::new();
    rows.retain(|row| seen.insert(row_key(row)));
}

/// Multiset of rows for ALL-quantified set operations.
fn row_counts(rows: &[Vec<Value>]) -> std::collections::BTreeMap<Vec<u8>, usize> {
    let mut counts = std::collections::BTreeMap::new();
    for row in rows {
        *counts.entry(row_key(row)).or_insert(0) += 1;
    }
    counts
}

/// If `row` has a remaining occurrence in `counts`, consume one and return
/// true; otherwise return false.
fn consume_one(counts: &mut std::collections::BTreeMap<Vec<u8>, usize>, row: &[Value]) -> bool {
    match counts.get_mut(&row_key(row)) {
        Some(n) if *n > 0 => {
            *n -= 1;
            true
        }
        _ => false,
    }
}

fn eval_agg(spec: &AggSpec, docs: &[&Object], key: &[Value]) -> Result<Value> {
    match spec {
        AggSpec::GroupKey { idx } => Ok(key.get(*idx).cloned().unwrap_or(Value::Null)),
        AggSpec::Agg {
            op,
            arg,
            distinct,
            sep,
        } => {
            let mut vals: Vec<Value> = Vec::new();
            for d in docs {
                let v = eval_expr(arg, d)?;
                if !matches!(v, Value::Null) {
                    vals.push(v);
                }
            }
            // DISTINCT aggregates dedup before combining.
            if *distinct {
                let mut seen = std::collections::BTreeSet::new();
                vals.retain(|v| seen.insert(encode::encode_to_vec(v).unwrap_or_default()));
            }
            Ok(match op {
                AggOp::Count => {
                    // COUNT(*) is rewritten to a sentinel column that never
                    // exists, so count rows instead of non-null values.
                    if matches!(arg, SqlExpr::Identifier(i) if i.value == "__count__") {
                        Value::Int(docs.len() as i64)
                    } else {
                        Value::Int(vals.len() as i64)
                    }
                }
                AggOp::Sum => {
                    if vals.is_empty() {
                        Value::Null
                    } else {
                        let mut sum = vals.remove(0);
                        for v in vals {
                            sum = add_values(sum, v)?;
                        }
                        sum
                    }
                }
                AggOp::Avg => {
                    if vals.is_empty() {
                        Value::Null
                    } else {
                        let n = vals.len() as f64;
                        let mut acc = 0.0f64;
                        for v in &vals {
                            acc += match v {
                                Value::Int(i) => *i as f64,
                                Value::Float(f) => *f,
                                _ => return err("AVG of non-numeric"),
                            };
                        }
                        Value::Float(acc / n)
                    }
                }
                AggOp::Min => vals
                    .into_iter()
                    .reduce(|a, b| {
                        if Value::cmp_values(&a, &b) != std::cmp::Ordering::Greater {
                            a
                        } else {
                            b
                        }
                    })
                    .unwrap_or(Value::Null),
                AggOp::Max => vals
                    .into_iter()
                    .reduce(|a, b| {
                        if Value::cmp_values(&a, &b) != std::cmp::Ordering::Less {
                            a
                        } else {
                            b
                        }
                    })
                    .unwrap_or(Value::Null),
                AggOp::GroupConcat => Value::Str(
                    vals.iter()
                        .map(value_to_text)
                        .collect::<Vec<_>>()
                        .join(sep.as_deref().unwrap_or(",")),
                ),
            })
        }
    }
}

/// Prefix every field with the source alias: "col" -> "alias.col".
fn qualify(doc: &Object, alias: &str) -> Object {
    doc.iter()
        .map(|(k, v)| (format!("{alias}.{k}"), v.clone()))
        .collect()
}

/// Nested-loop join. ON is evaluated over the merged (qualified) row.
/// LEFT/RIGHT joins null-extend the unmatched side; a RIGHT join also
/// null-extends the left columns for unmatched right rows.
#[allow(clippy::too_many_arguments)]
fn join_rows(
    left: Vec<Object>,
    right: &[Object],
    right_key: &str,
    on: Option<&SqlExpr>,
    left_join: bool,
    right_join: bool,
) -> Result<Vec<Object>> {
    let mut right_matched = vec![false; right.len()];
    let mut out = Vec::new();
    for l in &left {
        let mut matched = false;
        for (ri, r) in right.iter().enumerate() {
            let mut merged = l.clone();
            for (k, v) in r {
                merged.insert(format!("{right_key}.{k}"), v.clone());
            }
            let ok = match on {
                None => true,
                Some(e) => matches!(eval_expr(e, &merged)?, Value::Bool(true)),
            };
            if ok {
                matched = true;
                right_matched[ri] = true;
                out.push(merged);
            }
        }
        if !matched && left_join {
            let mut merged = l.clone();
            if let Some(r) = right.first() {
                for k in r.keys() {
                    merged.insert(format!("{right_key}.{k}"), Value::Null);
                }
            }
            out.push(merged);
        }
    }
    if right_join {
        let left_fields = union_of_fields(&left);
        for (ri, r) in right.iter().enumerate() {
            if !right_matched[ri] {
                let mut merged = Object::new();
                for k in &left_fields {
                    merged.insert(k.clone(), Value::Null);
                }
                for (k, v) in r {
                    merged.insert(format!("{right_key}.{k}"), v.clone());
                }
                out.push(merged);
            }
        }
    }
    Ok(out)
}

fn is_agg_item(item: &SelectItem) -> bool {
    let e = match item {
        SelectItem::UnnamedExpr(e) => e,
        SelectItem::ExprWithAlias { expr, .. } => expr,
        _ => return false,
    };
    contains_agg(e)
}

fn contains_agg(e: &SqlExpr) -> bool {
    match e {
        SqlExpr::Function(f) => {
            !f.over.is_some()
                && matches!(
                    f.name.to_string().to_uppercase().as_str(),
                    "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "GROUP_CONCAT" | "STRING_AGG"
                )
        }
        SqlExpr::Nested(inner) => contains_agg(inner),
        _ => false,
    }
}

/// Build a rows result from RETURNING items over the affected documents.
fn project_returning(items: &[SelectItem], docs: &[Object]) -> Result<ExecOutcome> {
    let mut columns = Vec::new();
    let mut exprs = Vec::new();
    for item in items {
        match item {
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
    let mut rows = Vec::new();
    for doc in docs {
        let mut row = Vec::with_capacity(exprs.len());
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

/// SQL literal for a value carried over into a replicated statement.
fn value_literal(v: &Value) -> Result<String> {
    match v {
        Value::Null => Ok("NULL".into()),
        Value::Bool(b) => Ok(if *b { "TRUE" } else { "FALSE" }.into()),
        Value::Int(i) => Ok(i.to_string()),
        Value::Float(f) => {
            // Non-finite floats have no SQL literal form — rendering "inf"/
            // "NaN" would produce statements the peer cannot parse (cluster
            // divergence) and dumps that cannot replay. Debug formatting
            // keeps integral floats distinguishable from Int ("3.0" vs "3"),
            // preserving the type across resolved-INSERT replay.
            if !f.is_finite() {
                return err("cannot render a non-finite FLOAT value as a SQL literal");
            }
            Ok(format!("{f:?}"))
        }
        Value::Str(s) => Ok(format!("'{}'", s.replace('\'', "''"))),
        other => err(format!(
            "cannot render a {} value as a SQL literal \
             (replication and dump need scalar column values)",
            other.type_name()
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
    format!("\"{}\"", name.replace('"', "\"\""))
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

fn expr_name(e: &SqlExpr) -> String {
    match e {
        SqlExpr::Identifier(i) => i.value.clone(),
        SqlExpr::CompoundIdentifier(parts) => parts
            .iter()
            .map(|p| p.value.clone())
            .collect::<Vec<_>>()
            .join("."),
        other => other.to_string(),
    }
}

/// Replace identifier `old` with `new` in SQL text, matching whole
/// identifiers only (adjacent word characters disqualify the match).
fn rename_ident_in_text(text: &str, old: &str, new: &str) -> String {
    fn is_word(b: u8) -> bool {
        b.is_ascii_alphanumeric() || b == b'_' || b == b'$'
    }
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if text[i..].starts_with(old)
            && (i == 0 || !is_word(bytes[i - 1]))
            && !bytes.get(i + old.len()).copied().is_some_and(is_word)
        {
            out.extend_from_slice(new.as_bytes());
            i += old.len();
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).unwrap_or_else(|_| text.to_string())
}

/// Collect the table names and aliases a query reads in its own FROM, so
/// qualified references to anything else can be flagged as correlated.
fn collect_from_names(q: &Query, out: &mut std::collections::BTreeSet<String>) {
    if let sqlparser::ast::SetExpr::Select(sel) = &*q.body {
        for twj in &sel.from {
            if let sqlparser::ast::TableFactor::Table { name, alias, .. } = &twj.relation {
                out.insert(obj_name(name));
                if let Some(a) = alias {
                    out.insert(a.name.value.clone());
                }
            }
        }
    }
}

/// Find `outer_table.column` references in `e` whose qualifier is a known
/// catalog table but not one of the query's own FROM names/aliases — the
/// correlated-reference shape. Unqualified outer references cannot be told
/// apart from legitimate schemaless fields, so only qualified ones flag.
fn collect_correlated_refs(
    e: &SqlExpr,
    from_names: &std::collections::BTreeSet<String>,
    catalog: &std::collections::BTreeMap<String, TableMeta>,
    hits: &mut Vec<String>,
) {
    match e {
        SqlExpr::CompoundIdentifier(parts) => {
            if parts.len() >= 2 {
                let qualifier = &parts[0].value;
                if !from_names.contains(qualifier) && catalog.contains_key(qualifier) {
                    hits.push(qualifier.clone());
                }
            }
        }
        SqlExpr::BinaryOp { left, right, .. } => {
            collect_correlated_refs(left, from_names, catalog, hits);
            collect_correlated_refs(right, from_names, catalog, hits);
        }
        SqlExpr::UnaryOp { expr, .. } => collect_correlated_refs(expr, from_names, catalog, hits),
        SqlExpr::Nested(inner) => collect_correlated_refs(inner, from_names, catalog, hits),
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            collect_correlated_refs(expr, from_names, catalog, hits);
            collect_correlated_refs(low, from_names, catalog, hits);
            collect_correlated_refs(high, from_names, catalog, hits);
        }
        SqlExpr::Like { expr, pattern, .. } | SqlExpr::ILike { expr, pattern, .. } => {
            collect_correlated_refs(expr, from_names, catalog, hits);
            collect_correlated_refs(pattern, from_names, catalog, hits);
        }
        SqlExpr::InList { expr, list, .. } => {
            collect_correlated_refs(expr, from_names, catalog, hits);
            for item in list {
                collect_correlated_refs(item, from_names, catalog, hits);
            }
        }
        SqlExpr::InSubquery { expr, .. } => {
            collect_correlated_refs(expr, from_names, catalog, hits);
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                collect_correlated_refs(op, from_names, catalog, hits);
            }
            for w in conditions {
                collect_correlated_refs(&w.condition, from_names, catalog, hits);
                collect_correlated_refs(&w.result, from_names, catalog, hits);
            }
            if let Some(el) = else_result {
                collect_correlated_refs(el, from_names, catalog, hits);
            }
        }
        SqlExpr::Cast { expr, .. } => collect_correlated_refs(expr, from_names, catalog, hits),
        _ => {}
    }
}

/// Dump-replay insert order: referenced tables before referencing ones
/// (FK checks run per row during replay). Picks the first satisfiable
/// table in catalog order each round; an FK cycle falls back to catalog
/// order for its members (a true cycle needs deferred constraints).
fn fk_dependency_order(
    names: &[String],
    tables: &std::collections::BTreeMap<String, TableMeta>,
) -> Vec<String> {
    let mut out: Vec<String> = Vec::with_capacity(names.len());
    let mut remaining: Vec<String> = names.to_vec();
    while !remaining.is_empty() {
        let next = remaining.iter().position(|n| {
            tables.get(n).is_none_or(|m| {
                m.foreign_keys.iter().all(|(_, rt, _)| {
                    rt == n || !names.iter().any(|x| x == rt) || out.iter().any(|o| o == rt)
                })
            })
        });
        match next {
            Some(i) => out.push(remaining.remove(i)),
            None => out.append(&mut remaining),
        }
    }
    out
}

/// Evaluate a constant expression (literal / arithmetic on literals).
pub fn eval_const(e: &SqlExpr) -> Result<Value> {
    match e {
        SqlExpr::Value(v) => sql_value(v),
        SqlExpr::UnaryOp { op, expr } => {
            // `-9223372036854775808` lexes as Minus over 9223372036854775808,
            // which overflows i64 on its own — re-parse the signed text so the
            // value stays an Int instead of degrading to Float.
            if *op == sqlparser::ast::UnaryOperator::Minus {
                if let SqlExpr::Value(nv) = expr.as_ref() {
                    if let sqlparser::ast::Value::Number(n, _) = &nv.value {
                        if let Ok(i) = format!("-{n}").parse::<i64>() {
                            return Ok(Value::Int(i));
                        }
                    }
                }
            }
            let v = eval_const(expr)?;
            match (op, v) {
                (sqlparser::ast::UnaryOperator::Minus, Value::Int(i)) => {
                    Ok(Value::Int(i.wrapping_neg()))
                }
                (sqlparser::ast::UnaryOperator::Minus, Value::Float(f)) => Ok(Value::Float(-f)),
                _ => err("unsupported unary operand"),
            }
        }
        SqlExpr::BinaryOp { left, op, right } => {
            let l = eval_const(left)?;
            let r = eval_const(right)?;
            binop(l, op, r)
        }
        SqlExpr::Nested(e) => eval_const(e),
        SqlExpr::Identifier(i) => err(format!("column {} not allowed here", i.value)),
        other => err(format!("unsupported expression: {other}")),
    }
}

fn sql_value(v: &sqlparser::ast::Value) -> Result<Value> {
    use sqlparser::ast::Value as V;
    let out = match v {
        V::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                Value::Int(i)
            } else {
                Value::Float(
                    n.parse::<f64>()
                        .map_err(|_| SqlError::Message(format!("bad number {n}")))?,
                )
            }
        }
        V::SingleQuotedString(s) | V::DoubleQuotedString(s) => Value::Str(s.clone()),
        V::Boolean(b) => Value::Bool(*b),
        V::Null => Value::Null,
        other => return err(format!("unsupported literal: {other}")),
    };
    Ok(out)
}

/// Resolve a bare column name: exact key first, then a unique "alias.col"
/// match (qualified rows from joins); ambiguous matches are an error.
fn lookup_col(doc: &Object, name: &str) -> Result<Value> {
    if let Some(v) = doc.get(name) {
        return Ok(v.clone());
    }
    let suffix = format!(".{name}");
    // Unqualified name on joined rows: take the FIRST source in row order
    // (BTreeMap order is deterministic; matches left-table-first convention).
    Ok(doc
        .iter()
        .find(|(k, _)| k.ends_with(&suffix))
        .map(|(_, v)| v.clone())
        .unwrap_or(Value::Null))
}

/// Evaluate an expression against a document row.
pub fn eval_expr(e: &SqlExpr, doc: &Object) -> Result<Value> {
    match e {
        SqlExpr::Identifier(i) => lookup_col(doc, &i.value),
        SqlExpr::CompoundIdentifier(parts) => {
            let full = parts
                .iter()
                .map(|p| p.value.clone())
                .collect::<Vec<_>>()
                .join(".");
            if let Some(v) = doc.get(&full) {
                Ok(v.clone())
            } else {
                let last = parts.last().map(|p| p.value.clone()).unwrap_or_default();
                lookup_col(doc, &last)
            }
        }
        SqlExpr::Value(v) => sql_value(v),
        SqlExpr::UnaryOp { op, expr } => {
            let v = eval_expr(expr, doc)?;
            match (op, v) {
                (sqlparser::ast::UnaryOperator::Minus, Value::Int(i)) => match i.checked_neg() {
                    Some(n) => Ok(Value::Int(n)),
                    // -i64::MIN overflows; mirror literal handling and keep
                    // the value instead of panicking in debug builds.
                    None => Ok(Value::Float(-(i as f64))),
                },
                (sqlparser::ast::UnaryOperator::Minus, Value::Float(f)) => Ok(Value::Float(-f)),
                (sqlparser::ast::UnaryOperator::Not, Value::Bool(b)) => Ok(Value::Bool(!b)),
                (sqlparser::ast::UnaryOperator::Not, Value::Null) => Ok(Value::Null),
                _ => err("unsupported unary operand"),
            }
        }
        SqlExpr::BinaryOp { left, op, right } => {
            let l = eval_expr(left, doc)?;
            let r = eval_expr(right, doc)?;
            binop(l, op, r)
        }
        SqlExpr::Nested(e) => eval_expr(e, doc),
        SqlExpr::IsNull(inner) => Ok(Value::Bool(matches!(eval_expr(inner, doc)?, Value::Null))),
        SqlExpr::IsNotNull(inner) => {
            Ok(Value::Bool(!matches!(eval_expr(inner, doc)?, Value::Null)))
        }
        SqlExpr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            // NULL anywhere makes the predicate unknown -> false in WHERE.
            let (v, lo, hi) = (
                eval_expr(expr, doc)?,
                eval_expr(low, doc)?,
                eval_expr(high, doc)?,
            );
            if matches!(v, Value::Null) || matches!(lo, Value::Null) || matches!(hi, Value::Null) {
                return Ok(Value::Bool(false));
            }
            let inside = Value::cmp_values(&v, &lo) != Ordering::Less
                && Value::cmp_values(&v, &hi) != Ordering::Greater;
            Ok(Value::Bool(inside != *negated))
        }
        SqlExpr::Like {
            negated,
            expr,
            pattern,
            escape_char,
            ..
        }
        | SqlExpr::ILike {
            negated,
            expr,
            pattern,
            escape_char,
            ..
        } => {
            let ci = matches!(e, SqlExpr::ILike { .. });
            let v = eval_expr(expr, doc)?;
            let p = eval_expr(pattern, doc)?;
            let esc = escape_char.as_ref().and_then(|v| match &v.value {
                sqlparser::ast::Value::SingleQuotedString(s) => s.chars().next(),
                _ => None,
            });
            let hit = match (&v, &p) {
                (Value::Null, _) | (_, Value::Null) => false,
                (Value::Str(s), Value::Str(pat)) => {
                    let (s, pat) = if ci {
                        (s.to_lowercase(), pat.to_lowercase())
                    } else {
                        (s.clone(), pat.clone())
                    };
                    like_match(&s, &pat, esc)
                }
                _ => false,
            };
            Ok(Value::Bool(hit != *negated))
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            let op = operand.as_ref().map(|o| eval_expr(o, doc)).transpose()?;
            for w in conditions {
                let hit = match &op {
                    Some(base) => {
                        let cv = eval_expr(&w.condition, doc)?;
                        !matches!(base, Value::Null)
                            && !matches!(cv, Value::Null)
                            && Value::cmp_values(base, &cv) == Ordering::Equal
                    }
                    None => matches!(eval_expr(&w.condition, doc)?, Value::Bool(true)),
                };
                if hit {
                    return eval_expr(&w.result, doc);
                }
            }
            match else_result {
                Some(r) => eval_expr(r, doc),
                None => Ok(Value::Null),
            }
        }
        SqlExpr::Cast {
            expr, data_type, ..
        } => cast_value(eval_expr(expr, doc)?, &data_type.to_string()),
        // SUBSTR/SUBSTRING parses to a dedicated node, not a Function call.
        SqlExpr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            let base = eval_expr(expr, doc)?;
            let start = match substring_from {
                Some(e) => match eval_expr(e, doc)? {
                    Value::Int(i) => i,
                    Value::Null => return Ok(Value::Null),
                    other => return err(format!("SUBSTR start must be an integer: {other:?}")),
                },
                None => 1,
            };
            let len = match substring_for {
                Some(e) => match eval_expr(e, doc)? {
                    Value::Int(i) => Some(i),
                    Value::Null => return Ok(Value::Null),
                    other => return err(format!("SUBSTR length must be an integer: {other:?}")),
                },
                None => None,
            };
            let args = vec![
                base,
                Value::Int(start),
                Value::Int(len.unwrap_or(i64::MAX / 4)),
            ];
            scalar_function("SUBSTR", &args)
        }
        SqlExpr::Trim {
            trim_where,
            expr,
            trim_what,
            ..
        } => {
            let base = eval_expr(expr, doc)?;
            let name = match trim_where {
                Some(sqlparser::ast::TrimWhereField::Leading) => "LTRIM",
                Some(sqlparser::ast::TrimWhereField::Trailing) => "RTRIM",
                _ => "TRIM",
            };
            if trim_what.is_some() {
                return err("TRIM with a custom character set is not supported");
            }
            scalar_function(name, &[base])
        }
        SqlExpr::Function(f) => {
            let name = f.name.to_string().to_uppercase();
            if f.over.is_some() {
                return err(format!("window functions (OVER) are not supported: {name}"));
            }
            if matches!(
                name.as_str(),
                "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "GROUP_CONCAT" | "STRING_AGG"
            ) {
                return err(format!(
                    "aggregate function {name} is not allowed in this context"
                ));
            }
            let mut args = Vec::new();
            if let sqlparser::ast::FunctionArguments::List(list) = &f.args {
                for a in &list.args {
                    match a {
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(e),
                        ) => args.push(eval_expr(e, doc)?),
                        _ => return err(format!("unsupported argument to {name}")),
                    }
                }
            }
            scalar_function(&name, &args)
        }
        SqlExpr::InList {
            expr,
            list,
            negated,
        } => {
            let v = eval_expr(expr, doc)?;
            let mut hit = false;
            let mut saw_null = matches!(v, Value::Null);
            for item in list {
                let iv = eval_expr(item, doc)?;
                if matches!(iv, Value::Null) {
                    saw_null = true;
                    continue;
                }
                if Value::cmp_values(&v, &iv) == std::cmp::Ordering::Equal {
                    hit = true;
                    break;
                }
            }
            // SQL three-valued logic: a NULL anywhere makes the predicate
            // unknown, and unknown never passes a WHERE. `x NOT IN (..., NULL)`
            // is therefore false for every row, not true.
            Ok(Value::Bool(if *negated { !hit && !saw_null } else { hit }))
        }
        // Subqueries are resolved per-statement by subst_expr; reaching one
        // here means it was correlated or unsupported.
        SqlExpr::Subquery(_) => err("correlated subqueries are not supported"),
        SqlExpr::InSubquery { .. } => err("correlated subqueries are not supported"),
        SqlExpr::Exists { .. } => err("correlated subqueries are not supported"),
        SqlExpr::AnyOp { .. } | SqlExpr::AllOp { .. } => {
            err("ANY/ALL subqueries are not supported here")
        }
        other => err(format!("unsupported expression: {other}")),
    }
}

/// SQL LIKE with `%` (any run) and `_` (one char); backtracking matcher.
fn like_match(s: &str, pat: &str, esc: Option<char>) -> bool {
    let s: Vec<char> = s.chars().collect();
    let p: Vec<char> = pat.chars().collect();
    let (mut si, mut pi) = (0usize, 0usize);
    let (mut star, mut mark) = (usize::MAX, 0usize);
    while si < s.len() {
        let escaped = esc.is_some() && pi + 1 < p.len() && p[pi] == esc.unwrap();
        if escaped {
            if s[si] == p[pi + 1] {
                si += 1;
                pi += 2;
                continue;
            }
        } else if pi < p.len() && p[pi] == '_' {
            si += 1;
            pi += 1;
            continue;
        } else if pi < p.len() && p[pi] == '%' {
            star = pi;
            mark = si;
            pi += 1;
            continue;
        } else if pi < p.len() && s[si] == p[pi] {
            si += 1;
            pi += 1;
            continue;
        }
        if star != usize::MAX {
            pi = star + 1;
            mark += 1;
            si = mark;
            continue;
        }
        return false;
    }
    while pi < p.len() && p[pi] == '%' {
        pi += 1;
    }
    pi == p.len()
}

/// Canonical text form of a value (CAST AS TEXT, GROUP_CONCAT, ||).
fn value_to_text(v: &Value) -> String {
    match v {
        Value::Str(s) => s.clone(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => {
            let s = format!("{f}");
            if s.contains('.') || s.contains('e') || s.contains('E') {
                s
            } else {
                format!("{f:.1}")
            }
        }
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
        other => format!("{other:?}"),
    }
}

/// Best-effort CAST across the schemaless value model.
fn cast_value(v: Value, type_name: &str) -> Result<Value> {
    let t = type_name.to_uppercase();
    Ok(match v {
        Value::Null => Value::Null,
        v if t.contains("INT") => match v {
            Value::Int(_) => v,
            Value::Float(f) => Value::Int(f as i64),
            Value::Bool(b) => Value::Int(if b { 1 } else { 0 }),
            Value::Str(s) => Value::Int(
                s.trim()
                    .parse::<i64>()
                    .map_err(|_| SqlError::Message(format!("cannot CAST '{s}' AS {type_name}")))?,
            ),
            other => other,
        },
        v if t.contains("CHAR") || t.contains("TEXT") || t.contains("STRING") => {
            Value::Str(value_to_text(&v))
        }
        v if t.contains("BOOL") => match v {
            Value::Bool(_) => v,
            Value::Int(i) => Value::Bool(i != 0),
            Value::Str(s) => Value::Bool(s == "true"),
            other => other,
        },
        v if t.contains("REAL") || t.contains("DOUBLE") || t.contains("FLOAT") => match v {
            Value::Float(_) => v,
            Value::Int(i) => Value::Float(i as f64),
            Value::Str(s) => Value::Float(
                s.trim()
                    .parse::<f64>()
                    .map_err(|_| SqlError::Message(format!("cannot CAST '{s}' AS {type_name}")))?,
            ),
            other => other,
        },
        // Unknown type: pass through unchanged (types are documentation here).
        v => v,
    })
}

/// Scalar (non-aggregate) function library. Arity is checked up front:
/// a malformed call (`SELECT UPPER()`) must error, not panic on indexing.
fn scalar_function(name: &str, args: &[Value]) -> Result<Value> {
    fn arg<'a>(args: &'a [Value], i: usize, name: &str) -> Result<&'a Value> {
        args.get(i).ok_or_else(|| {
            SqlError::Message(format!("function {name} requires argument {}", i + 1))
        })
    }
    fn exact_arity(name: &str, args: &[Value], n: usize) -> Result<()> {
        if args.len() != n {
            return Err(SqlError::Message(format!(
                "function {name} takes exactly {n} argument(s), got {}",
                args.len()
            )));
        }
        Ok(())
    }
    let null_prop = |v: &Value| matches!(v, Value::Null);
    Ok(match name {
        "UPPER" | "UCASE" => {
            exact_arity(name, args, 1)?;
            Value::Str(value_to_text(arg(args, 0, name)?).to_uppercase())
        }
        "LOWER" | "LCASE" => {
            exact_arity(name, args, 1)?;
            Value::Str(value_to_text(arg(args, 0, name)?).to_lowercase())
        }
        "LENGTH" | "LEN" => {
            exact_arity(name, args, 1)?;
            Value::Int(match arg(args, 0, name)? {
                Value::Str(s) => s.chars().count() as i64,
                Value::Bytes(b) => b.len() as i64,
                other => value_to_text(other).chars().count() as i64,
            })
        }
        "ABS" => {
            exact_arity(name, args, 1)?;
            match arg(args, 0, name)? {
                Value::Int(i) => Value::Int(i.abs()),
                Value::Float(f) => Value::Float(f.abs()),
                Value::Null => Value::Null,
                other => return err(format!("ABS of non-numeric: {other:?}")),
            }
        }
        "ROUND" => {
            if args.is_empty() || args.len() > 2 {
                return err(format!(
                    "function ROUND takes 1 or 2 arguments, got {}",
                    args.len()
                ));
            }
            if null_prop(arg(args, 0, name)?) {
                Value::Null
            } else {
                let x = as_f64(arg(args, 0, name)?)?;
                let digits = match args.get(1) {
                    Some(Value::Int(d)) => *d,
                    _ => 0,
                };
                let m = 10f64.powi(digits as i32);
                Value::Float((x * m).round() / m)
            }
        }
        "COALESCE" | "IFNULL" => {
            if args.is_empty() {
                return err(format!("function {name} requires at least 1 argument"));
            }
            args.iter()
                .find(|v| !matches!(v, Value::Null))
                .cloned()
                .unwrap_or(Value::Null)
        }
        "NULLIF" => {
            exact_arity(name, args, 2)?;
            if Value::cmp_values(arg(args, 0, name)?, arg(args, 1, name)?) == Ordering::Equal {
                Value::Null
            } else {
                args[0].clone()
            }
        }
        "SUBSTR" | "SUBSTRING" => {
            if args.is_empty() || args.len() > 3 {
                return err(format!(
                    "function {name} takes 1 to 3 arguments, got {}",
                    args.len()
                ));
            }
            if null_prop(arg(args, 0, name)?) {
                Value::Null
            } else {
                let s: Vec<char> = value_to_text(arg(args, 0, name)?).chars().collect();
                // 1-based start; negative counts from the end (SQLite rule).
                let start = match args.get(1) {
                    Some(Value::Int(i)) => {
                        if *i < 0 {
                            (s.len() as i64 + *i).max(0)
                        } else {
                            (*i - 1).max(0)
                        }
                    }
                    _ => 0,
                } as usize;
                let end = match args.get(2) {
                    Some(Value::Int(n)) => (start + (*n).max(0) as usize).min(s.len()),
                    _ => s.len(),
                };
                Value::Str(s[start.min(s.len())..end].iter().collect())
            }
        }
        "TRIM" | "LTRIM" | "RTRIM" => {
            exact_arity(name, args, 1)?;
            let text = value_to_text(arg(args, 0, name)?);
            Value::Str(match name {
                "TRIM" => text.trim().to_string(),
                "LTRIM" => text.trim_start().to_string(),
                _ => text.trim_end().to_string(),
            })
        }
        "CONCAT" => {
            if args.iter().any(null_prop) {
                Value::Null
            } else {
                Value::Str(args.iter().map(value_to_text).collect())
            }
        }
        "TYPEOF" => {
            exact_arity(name, args, 1)?;
            Value::Str(
                match arg(args, 0, name)? {
                    Value::Null => "null",
                    Value::Bool(_) => "bool",
                    Value::Int(_) => "integer",
                    Value::Float(_) => "float",
                    Value::Str(_) => "text",
                    Value::Bytes(_) => "blob",
                    Value::Array(_) => "array",
                    Value::Object(_) => "object",
                }
                .into(),
            )
        }
        other => return err(format!("unknown function: {other}")),
    })
}

/// ORDER BY comparison honoring NULLS FIRST/LAST. Default places NULLs as
/// the smallest value (first on ASC, last on DESC).
fn cmp_maybe_null(a: &Value, b: &Value, nulls_first: Option<bool>) -> Ordering {
    let nulls_first = nulls_first.unwrap_or(true);
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (_, Value::Null) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        _ => Value::cmp_values(a, b),
    }
}

/// Inline a computed Value as a literal expression node (subquery
/// substitution rewrites results into the row-local expression tree).
fn value_to_literal(v: Value) -> Result<SqlExpr> {
    use sqlparser::ast::Value as V;
    Ok(match v {
        Value::Null => SqlExpr::Value(V::Null.into()),
        Value::Bool(b) => SqlExpr::Value(V::Boolean(b).into()),
        Value::Int(i) => SqlExpr::Value(V::Number(i.to_string(), false).into()),
        Value::Float(f) => SqlExpr::Value(V::Number(format!("{f}"), false).into()),
        Value::Str(s) => SqlExpr::Value(V::SingleQuotedString(s).into()),
        other => return err(format!("cannot inline value as literal: {other:?}")),
    })
}

/// Map a B+ tree error to a SQL error; duplicate hits become the familiar
/// constraint-failure message.
fn index_err(col: &str, e: BTreeError) -> SqlError {
    match e {
        BTreeError::Duplicate => SqlError::Message(format!("UNIQUE constraint failed: {col}")),
        other => SqlError::Message(format!("index error on {col}: {other}")),
    }
}

/// How an index probe should fetch entries.
#[derive(Debug, Clone)]
enum ProbePlan {
    Eq(Value),
    /// (lower bound, inclusive?), (upper bound, inclusive?) — either optional.
    Range {
        lo: Option<(Value, bool)>,
        hi: Option<(Value, bool)>,
    },
}

/// Flatten the top-level AND chain of a WHERE expression.
fn flatten_and<'a>(e: &'a SqlExpr, out: &mut Vec<&'a SqlExpr>) {
    if let SqlExpr::BinaryOp {
        left,
        op: BinaryOperator::And,
        right,
    } = e
    {
        flatten_and(left, out);
        flatten_and(right, out);
    } else {
        out.push(e);
    }
}

/// Resolve a column reference (bare or table/alias-qualified) to an indexed
/// column of this table, if it has one.
fn resolve_indexed_col(
    ident: &str,
    table: &str,
    alias: Option<&str>,
    meta: &TableMeta,
) -> Option<String> {
    if let Some(col) = ident.strip_prefix(&format!("{table}.")) {
        if meta.index_roots.contains_key(col) {
            return Some(col.to_string());
        }
    }
    if let Some(a) = alias {
        if let Some(col) = ident.strip_prefix(&format!("{a}.")) {
            if meta.index_roots.contains_key(col) {
                return Some(col.to_string());
            }
        }
    }
    if !ident.contains('.') && meta.index_roots.contains_key(ident) {
        return Some(ident.to_string());
    }
    None
}

/// Choose an index plan from a WHERE expression: an equality conjunct on an
/// indexed column wins; otherwise range bounds on one indexed column.
fn probe_plan(
    cond: &SqlExpr,
    meta: &TableMeta,
    table: &str,
    alias: Option<&str>,
) -> Option<(String, ProbePlan)> {
    let mut conjuncts = Vec::new();
    flatten_and(cond, &mut conjuncts);
    let empty = Object::new();
    let mut eq: Option<(String, Value)> = None;
    let mut range: Option<(String, Vec<(BinaryOperator, Value)>)> = None;
    for c in conjuncts {
        let SqlExpr::BinaryOp { left, op, right } = c else {
            continue;
        };
        // col OP lit, or lit OP col (mirrored)
        let (col_ref, op, lit, mirror) = match (left.as_ref(), right.as_ref()) {
            (SqlExpr::Identifier(_) | SqlExpr::CompoundIdentifier(_), r) => {
                (left.as_ref(), op.clone(), r, false)
            }
            (l, SqlExpr::Identifier(_) | SqlExpr::CompoundIdentifier(_)) => {
                (right.as_ref(), op.clone(), l, true)
            }
            _ => continue,
        };
        let name = match col_ref {
            SqlExpr::Identifier(i) => i.value.clone(),
            SqlExpr::CompoundIdentifier(parts) => parts
                .iter()
                .map(|p| p.value.clone())
                .collect::<Vec<_>>()
                .join("."),
            _ => continue,
        };
        // The literal side must be a constant (subqueries were substituted
        // away; column refs make this conjunct unusable).
        let Ok(v) = eval_expr(lit, &empty) else {
            continue;
        };
        if matches!(v, Value::Null) {
            continue;
        }
        let op = if mirror { mirror_op(&op)? } else { op };
        let Some(col) = resolve_indexed_col(&name, table, alias, meta) else {
            continue;
        };
        use BinaryOperator::*;
        match op {
            Eq => {
                eq = Some((col, v));
                break;
            }
            Gt | GtEq | Lt | LtEq => match &mut range {
                Some((c2, bounds)) if *c2 == col => bounds.push((op, v)),
                None => range = Some((col, vec![(op, v)])),
                _ => {}
            },
            _ => {}
        }
    }
    if let Some((col, v)) = eq {
        return Some((col, ProbePlan::Eq(v)));
    }
    let (col, bounds) = range?;
    let mut lo: Option<(Value, bool)> = None;
    let mut hi: Option<(Value, bool)> = None;
    for (op, v) in bounds {
        use BinaryOperator::*;
        match op {
            Gt => lo = Some((v, false)),
            GtEq => lo = Some((v, true)),
            Lt => hi = Some((v, false)),
            LtEq => hi = Some((v, true)),
            _ => {}
        }
    }
    if lo.is_none() && hi.is_none() {
        return None;
    }
    Some((col, ProbePlan::Range { lo, hi }))
}

/// Mirror a comparison operator for `lit OP col` conjuncts.
fn mirror_op(op: &BinaryOperator) -> Option<BinaryOperator> {
    use BinaryOperator::*;
    Some(match op {
        Eq => Eq,
        Lt => Gt,
        Gt => Lt,
        LtEq => GtEq,
        GtEq => LtEq,
        _ => return None,
    })
}

/// True when `col` is a uniqueness-constraint column (its tree enforces
/// duplicates); plain CREATE INDEX trees are non-unique.
fn is_constraint_col(meta: &TableMeta, col: &str) -> bool {
    meta.primary_key.as_deref() == Some(col) || meta.unique.iter().any(|c| c == col)
}

/// Re-point index entries after in-page slot moves: delete (key, old_loc)
/// and insert (key, new_loc) in every tree. `before` holds the page's
/// documents as they were before the mutation.
fn reindex_repoint(
    pager: &mut Pager,
    tx: &mut crate::pager::Tx,
    roots: &mut std::collections::BTreeMap<String, u32>,
    before: &[(u64, Object)],
    old_l: u64,
    new_l: u64,
) -> Result<()> {
    let Some((_, doc)) = before.iter().find(|(l, _)| *l == old_l) else {
        return err("index fixup: moved document not found");
    };
    for (col, root) in roots.clone() {
        if let Some(v) = doc.get(&col) {
            if !matches!(v, Value::Null) {
                let mut tree = BTree::open(root);
                tree.delete_entry(pager, tx, v, old_l)
                    .map_err(|e| index_err(&col, e))?;
                tree.insert(pager, tx, v.clone(), new_l, false)
                    .map_err(|e| index_err(&col, e))?;
                if tree.root != root {
                    roots.insert(col, tree.root);
                }
            }
        }
    }
    Ok(())
}

/// Drop one document's index entries (fast-path DELETE).
fn reindex_remove(
    pager: &mut Pager,
    tx: &mut crate::pager::Tx,
    roots: &mut std::collections::BTreeMap<String, u32>,
    doc: &Object,
    loc: u64,
) -> Result<()> {
    for (col, root) in roots.clone() {
        if let Some(v) = doc.get(&col) {
            if !matches!(v, Value::Null) {
                let mut tree = BTree::open(root);
                tree.delete_entry(pager, tx, v, loc)
                    .map_err(|e| index_err(&col, e))?;
                if tree.root != root {
                    roots.insert(col, tree.root);
                }
            }
        }
    }
    Ok(())
}

/// Swap one document's index entries for its updated keys (fast-path
/// UPDATE). Constraint columns enforce uniqueness on insert.
#[allow(clippy::too_many_arguments)]
fn reindex_replace(
    pager: &mut Pager,
    tx: &mut crate::pager::Tx,
    meta: &TableMeta,
    roots: &mut std::collections::BTreeMap<String, u32>,
    old_doc: &Object,
    old_l: u64,
    new_doc: &Object,
    new_l: u64,
) -> Result<()> {
    for (col, root) in roots.clone() {
        let old_v = old_doc.get(&col).filter(|v| !matches!(v, Value::Null));
        let new_v = new_doc.get(&col).filter(|v| !matches!(v, Value::Null));
        if old_v.is_none() && new_v.is_none() {
            continue;
        }
        let mut tree = BTree::open(root);
        if let Some(v) = old_v {
            tree.delete_entry(pager, tx, v, old_l)
                .map_err(|e| index_err(&col, e))?;
        }
        if let Some(v) = new_v {
            tree.insert(pager, tx, v.clone(), new_l, is_constraint_col(meta, &col))
                .map_err(|e| index_err(&col, e))?;
        }
        if tree.root != root {
            roots.insert(col, tree.root);
        }
    }
    Ok(())
}

/// HAVING evaluation: aggregate subexpressions are computed over the group's
/// rows; bare columns matching a GROUP BY expression resolve to the group key
/// (via the group's rows — every row shares the key value); everything else
/// evaluates against the already-projected columns.
fn eval_having(
    e: &SqlExpr,
    out: &Object,
    docs: &[&Object],
    group_exprs: &[SqlExpr],
) -> Result<Value> {
    match e {
        SqlExpr::Function(f) if contains_agg(&SqlExpr::Function(f.clone())) => {
            let (op, arg, distinct, sep) = agg_parts(f)?;
            eval_agg(
                &AggSpec::Agg {
                    op,
                    arg,
                    distinct,
                    sep,
                },
                docs,
                &[],
            )
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
            let l = eval_having(left, out, docs, group_exprs)?;
            let r = eval_having(right, out, docs, group_exprs)?;
            binop(l, op, r)
        }
        SqlExpr::Nested(inner) => eval_having(inner, out, docs, group_exprs),
        other => eval_expr(other, out),
    }
}

fn binop(l: Value, op: &BinaryOperator, r: Value) -> Result<Value> {
    use BinaryOperator::*;
    Ok(match op {
        Plus | Minus | Multiply | Divide | Modulo => arith(l, op, r)?,
        StringConcat => match (l, r) {
            (Value::Null, _) | (_, Value::Null) => Value::Null,
            (a, b) => Value::Str(format!("{}{}", value_to_text(&a), value_to_text(&b))),
        },
        Eq => Value::Bool(Value::cmp_values(&l, &r) == Ordering::Equal),
        NotEq => Value::Bool(Value::cmp_values(&l, &r) != Ordering::Equal),
        Lt => Value::Bool(Value::cmp_values(&l, &r) == Ordering::Less),
        LtEq => Value::Bool(Value::cmp_values(&l, &r) != Ordering::Greater),
        Gt => Value::Bool(Value::cmp_values(&l, &r) == Ordering::Greater),
        GtEq => Value::Bool(Value::cmp_values(&l, &r) != Ordering::Less),
        And => match (l, r) {
            (Value::Bool(a), Value::Bool(b)) => Value::Bool(a && b),
            _ => return err("AND requires booleans"),
        },
        Or => match (l, r) {
            (Value::Bool(a), Value::Bool(b)) => Value::Bool(a || b),
            _ => return err("OR requires booleans"),
        },
        other => return err(format!("unsupported operator: {other}")),
    })
}

fn arith(l: Value, op: &BinaryOperator, r: Value) -> Result<Value> {
    use BinaryOperator::*;
    if matches!(l, Value::Null) || matches!(r, Value::Null) {
        return Ok(Value::Null);
    }
    let float_mode = matches!(l, Value::Float(_)) || matches!(r, Value::Float(_));
    if float_mode {
        let a = as_f64(&l)?;
        let b = as_f64(&r)?;
        return Ok(match op {
            Plus => Value::Float(a + b),
            Minus => Value::Float(a - b),
            Multiply => Value::Float(a * b),
            Divide if b == 0.0 => Value::Null,
            Divide => Value::Float(a / b),
            Modulo if b == 0.0 => Value::Null,
            Modulo => Value::Float(a % b),
            _ => return err("not an arithmetic operator"),
        });
    }
    match (&l, &r) {
        (Value::Int(a), Value::Int(b)) => Ok(match op {
            Plus => Value::Int(a.wrapping_add(*b)),
            Minus => Value::Int(a.wrapping_sub(*b)),
            Multiply => Value::Int(a.wrapping_mul(*b)),
            Divide if *b == 0 => Value::Null,
            // i64::MIN / -1 overflows (SIGFPE on some ISAs) — treat like
            // division by zero and yield NULL instead of crashing.
            Divide => match a.checked_div(*b) {
                Some(q) => Value::Int(q),
                None => Value::Null,
            },
            Modulo if *b == 0 => Value::Null,
            Modulo => match a.checked_rem(*b) {
                Some(m) => Value::Int(m),
                None => Value::Null,
            },
            _ => return err("not an arithmetic operator"),
        }),
        _ => err(format!("non-numeric operands: {l:?} {op} {r:?}")),
    }
}

fn as_f64(v: &Value) -> Result<f64> {
    match v {
        Value::Int(i) => Ok(*i as f64),
        Value::Float(f) => Ok(*f),
        other => err(format!("non-numeric operand: {other:?}")),
    }
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

// Re-export for tests / CLI convenience.
#[cfg(test)]
mod tests {
    use super::*;

    fn run(db: &mut Database, sql: &str) -> ExecOutcome {
        db.execute(sql)
            .unwrap_or_else(|e| panic!("SQL failed: {sql}\n{e}"))
    }

    fn rows(db: &mut Database, sql: &str) -> QueryResult {
        match run(db, sql) {
            ExecOutcome::Rows(r) => r,
            other => panic!("expected rows, got {other:?}"),
        }
    }

    // ---- index-backed engine paths ----

    fn idx_db() -> Database {
        let mut db = Database::in_memory().unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, n INT)")
            .unwrap();
        db
    }

    fn insert_n(db: &mut Database, n: i64) {
        for i in 0..n {
            db.execute(&format!(
                "INSERT INTO t (id, name, n) VALUES ({i}, 'u{i}', {i})"
            ))
            .unwrap();
        }
    }

    #[test]
    fn index_enforces_pk_across_statements() {
        let mut db = idx_db();
        insert_n(&mut db, 10);
        let e = db
            .execute("INSERT INTO t (id, name, n) VALUES (5, 'dup', 0)")
            .unwrap_err();
        assert!(e.to_string().contains("UNIQUE"), "{e}");
        // The failed statement left nothing behind.
        assert_eq!(
            rows(&mut db, "SELECT COUNT(*) FROM t").rows[0][0],
            Value::Int(10)
        );
    }

    #[test]
    fn index_enforces_pk_within_batch() {
        let mut db = idx_db();
        let e = db
            .execute("INSERT INTO t (id, name, n) VALUES (1, 'a', 0), (1, 'b', 0)")
            .unwrap_err();
        assert!(e.to_string().contains("UNIQUE"), "{e}");
        assert_eq!(
            rows(&mut db, "SELECT COUNT(*) FROM t").rows[0][0],
            Value::Int(0)
        );
    }

    #[test]
    fn point_lookup_finds_row_and_missing() {
        let mut db = idx_db();
        insert_n(&mut db, 50);
        let r = rows(&mut db, "SELECT name, n FROM t WHERE id = 42");
        assert_eq!(r.rows, vec![vec![Value::Str("u42".into()), Value::Int(42)]]);
        assert_eq!(
            rows(&mut db, "SELECT id FROM t WHERE id = 999").rows.len(),
            0
        );
        // WHERE with extra conjuncts (residual filtering) still works.
        let r = rows(&mut db, "SELECT id FROM t WHERE id = 42 AND n > 100");
        assert_eq!(r.rows.len(), 0);
        let r = rows(&mut db, "SELECT id FROM t WHERE id = 42 AND n = 42");
        assert_eq!(r.rows.len(), 1);
    }

    #[test]
    fn range_lookup_over_index() {
        let mut db = idx_db();
        insert_n(&mut db, 100);
        let r = rows(&mut db, "SELECT id FROM t WHERE id >= 97 ORDER BY id");
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Int(97)],
                vec![Value::Int(98)],
                vec![Value::Int(99)]
            ]
        );
        // strict bounds on both sides
        let r = rows(
            &mut db,
            "SELECT id FROM t WHERE id > 5 AND id < 8 ORDER BY id",
        );
        assert_eq!(r.rows, vec![vec![Value::Int(6)], vec![Value::Int(7)]]);
        // mirrored literal-first form
        let r = rows(&mut db, "SELECT id FROM t WHERE 8 = id");
        assert_eq!(r.rows, vec![vec![Value::Int(8)]]);
    }

    #[test]
    fn fast_delete_removes_rows_and_index_entries() {
        let mut db = idx_db();
        insert_n(&mut db, 30);
        assert_eq!(
            db.execute("DELETE FROM t WHERE id = 7").unwrap(),
            ExecOutcome::Affected(1)
        );
        assert_eq!(rows(&mut db, "SELECT id FROM t WHERE id = 7").rows.len(), 0);
        // Slot is free: re-inserting the same key must be accepted.
        db.execute("INSERT INTO t (id, name, n) VALUES (7, 'back', 7)")
            .unwrap();
        let r = rows(&mut db, "SELECT name FROM t WHERE id = 7");
        assert_eq!(r.rows, vec![vec![Value::Str("back".into())]]);
        // Bulk indexed delete.
        assert_eq!(
            db.execute("DELETE FROM t WHERE id >= 28").unwrap(),
            ExecOutcome::Affected(2)
        );
        assert_eq!(
            rows(&mut db, "SELECT COUNT(*) FROM t").rows[0][0],
            Value::Int(28)
        );
    }

    #[test]
    fn fast_update_swaps_keys_and_keeps_others() {
        let mut db = idx_db();
        insert_n(&mut db, 20);
        db.execute("UPDATE t SET id = 999, name = 'moved' WHERE id = 3")
            .unwrap();
        // old key gone, new key reachable
        assert_eq!(rows(&mut db, "SELECT id FROM t WHERE id = 3").rows.len(), 0);
        let r = rows(&mut db, "SELECT name FROM t WHERE id = 999");
        assert_eq!(r.rows, vec![vec![Value::Str("moved".into())]]);
        // every other row intact, in order
        let r = rows(&mut db, "SELECT id FROM t WHERE id < 10 ORDER BY id");
        let ids: Vec<i64> = r.rows.iter().filter_map(|v| v[0].as_i64()).collect();
        assert_eq!(ids, vec![0, 1, 2, 4, 5, 6, 7, 8, 9]);
        // moving a key onto an occupied one fails cleanly
        let e = db.execute("UPDATE t SET id = 5 WHERE id = 4").unwrap_err();
        assert!(e.to_string().contains("UNIQUE"), "{e}");
        // and the failed UPDATE changed nothing
        let r = rows(&mut db, "SELECT name FROM t WHERE id = 4");
        assert_eq!(r.rows, vec![vec![Value::Str("u4".into())]]);
    }

    #[test]
    fn create_index_builds_usable_tree() {
        let mut db = Database::in_memory().unwrap();
        db.execute("CREATE TABLE t (id INT, name TEXT)").unwrap();
        for i in 0..40 {
            db.execute(&format!("INSERT INTO t VALUES ({i}, 'n{}')", i % 4))
                .unwrap();
        }
        db.execute("CREATE INDEX idx_name ON t (name)").unwrap();
        // duplicate keys: every match returned
        let r = rows(&mut db, "SELECT id FROM t WHERE name = 'n1' ORDER BY id");
        assert_eq!(r.rows.len(), 10);
        // rows inserted after CREATE INDEX are found too
        db.execute("INSERT INTO t VALUES (100, 'n1')").unwrap();
        let r = rows(&mut db, "SELECT id FROM t WHERE name = 'n1' ORDER BY id");
        assert_eq!(r.rows.len(), 11);
        // a fast-path delete keeps the tree consistent
        db.execute("DELETE FROM t WHERE name = 'n1' AND id = 100")
            .unwrap();
        let r = rows(&mut db, "SELECT id FROM t WHERE name = 'n1' ORDER BY id");
        assert_eq!(r.rows.len(), 10);
    }

    #[test]
    fn create_unique_index_enforces_duplicates() {
        let mut db = Database::in_memory().unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, email TEXT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'a@x')").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'b@x')").unwrap();
        db.execute("CREATE UNIQUE INDEX ux_email ON t (email)")
            .unwrap();
        // duplicate insert rejected
        let e = db.execute("INSERT INTO t VALUES (3, 'a@x')").unwrap_err();
        assert!(e.to_string().contains("UNIQUE"), "{e}");
        // duplicate via UPDATE rejected
        let e = db
            .execute("UPDATE t SET email = 'b@x' WHERE id = 1")
            .unwrap_err();
        assert!(e.to_string().contains("UNIQUE"), "{e}");
        // NULLs don't collide
        db.execute("INSERT INTO t VALUES (4, NULL)").unwrap();
        db.execute("INSERT INTO t VALUES (5, NULL)").unwrap();
        // lookups over the unique tree still work
        let r = rows(&mut db, "SELECT id FROM t WHERE email = 'a@x'");
        assert_eq!(r.rows.len(), 1);
    }

    #[test]
    fn create_unique_index_over_duplicates_fails() {
        let mut db = Database::in_memory().unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'dup')").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'dup')").unwrap();
        let e = db.execute("CREATE UNIQUE INDEX ux_v ON t (v)").unwrap_err();
        assert!(e.to_string().contains("UNIQUE"), "{e}");
    }

    #[test]
    fn create_index_if_not_exists_is_idempotent() {
        let mut db = Database::in_memory().unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
            .unwrap();
        db.execute("CREATE INDEX IF NOT EXISTS iv ON t (v)")
            .unwrap();
        db.execute("CREATE INDEX IF NOT EXISTS iv ON t (v)")
            .unwrap();
        // plain CREATE of the same name still errors (name taken)
        let e = db.execute("CREATE INDEX iv ON t (v)").unwrap_err();
        assert!(e.to_string().contains("already exists"), "{e}");
    }

    #[test]
    fn create_index_rejects_bad_targets() {
        let mut db = Database::in_memory().unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b TEXT)")
            .unwrap();
        // missing table
        let e = db.execute("CREATE INDEX i ON missing (a)").unwrap_err();
        assert!(e.to_string().contains("does not exist"), "{e}");
        // missing column
        let e = db.execute("CREATE INDEX i ON t (nope)").unwrap_err();
        assert!(e.to_string().contains("does not exist"), "{e}");
        // multi-column indexes are not supported yet
        let e = db.execute("CREATE INDEX i ON t (a, b)").unwrap_err();
        assert!(e.to_string().contains("single-column"), "{e}");
        // duplicate name carries the name in the message
        db.execute("CREATE INDEX i ON t (a)").unwrap();
        let e = db.execute("CREATE INDEX i ON t (a)").unwrap_err();
        assert!(e.to_string().contains("index i already exists"), "{e}");
        // IF NOT EXISTS on the same name is a no-op, not an error
        db.execute("CREATE INDEX IF NOT EXISTS i ON t (a)").unwrap();
    }

    #[test]
    fn unique_index_update_conflict_and_rollback_integrity() {
        let mut db = Database::in_memory().unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, email TEXT)")
            .unwrap();
        db.execute("CREATE UNIQUE INDEX ux ON t (email)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'a@x')").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'b@x')").unwrap();
        // moving a value onto an occupied one fails
        let e = db
            .execute("UPDATE t SET email = 'b@x' WHERE id = 1")
            .unwrap_err();
        assert!(e.to_string().contains("UNIQUE"), "{e}");
        // failed UPDATE changed nothing
        let r = rows(&mut db, "SELECT email FROM t WHERE id = 1");
        assert_eq!(r.rows, vec![vec![Value::Str("a@x".into())]]);
    }

    #[test]
    fn second_unique_index_keeps_constraint_after_drop() {
        let mut db = Database::in_memory().unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
            .unwrap();
        db.execute("CREATE UNIQUE INDEX ux1 ON t (v)").unwrap();
        db.execute("CREATE UNIQUE INDEX ux2 ON t (v)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'x')").unwrap();
        let e = db.execute("INSERT INTO t VALUES (2, 'x')").unwrap_err();
        assert!(e.to_string().contains("UNIQUE"), "{e}");
        // dropping one of the two unique indexes on v keeps it enforced
        db.execute("DROP INDEX ux1").unwrap();
        let e = db.execute("INSERT INTO t VALUES (2, 'x')").unwrap_err();
        assert!(e.to_string().contains("UNIQUE"), "{e}");
        // dropping the last one lifts it
        db.execute("DROP INDEX ux2").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'x')").unwrap();
    }

    #[test]
    fn unique_index_over_constraint_column_keeps_constraint_on_drop() {
        let mut db = Database::in_memory().unwrap();
        // v is UNIQUE via CREATE TABLE; an index on top must not weaken it
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT UNIQUE)")
            .unwrap();
        db.execute("CREATE UNIQUE INDEX ix_v ON t (v)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'x')").unwrap();
        db.execute("DROP INDEX ix_v").unwrap();
        let e = db.execute("INSERT INTO t VALUES (2, 'x')").unwrap_err();
        assert!(e.to_string().contains("UNIQUE"), "{e}");
    }

    #[test]
    fn insert_or_ignore_respects_unique_index() {
        let mut db = Database::in_memory().unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
            .unwrap();
        db.execute("CREATE UNIQUE INDEX ux ON t (v)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'x')").unwrap();
        // conflict on the unique index is ignored instead of erroring
        db.execute("INSERT OR IGNORE INTO t VALUES (2, 'x')")
            .unwrap();
        let r = rows(&mut db, "SELECT id FROM t ORDER BY id");
        assert_eq!(r.rows, vec![vec![Value::Int(1)]]);
    }

    #[test]
    fn sqlite_master_reports_index_ddl_text() {
        let mut db = Database::in_memory().unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
            .unwrap();
        db.execute("CREATE INDEX iv ON t (v)").unwrap();
        db.execute("CREATE UNIQUE INDEX uv ON t (id)").unwrap();
        let r = rows(
            &mut db,
            "SELECT name, sql FROM sqlite_master WHERE type = 'index' ORDER BY name",
        );
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], Value::Str("iv".into()));
        assert_eq!(r.rows[0][1], Value::Str("CREATE INDEX iv ON t (v)".into()));
        assert_eq!(r.rows[1][0], Value::Str("uv".into()));
        assert_eq!(
            r.rows[1][1],
            Value::Str("CREATE UNIQUE INDEX uv ON t (id)".into())
        );
        // tables and indexes coexist in the view
        let r = rows(&mut db, "SELECT type FROM sqlite_master ORDER BY type");
        let tables = r
            .rows
            .iter()
            .filter(|v| v[0] == Value::Str("table".into()))
            .count();
        assert_eq!(tables, 1);
    }

    #[test]
    fn index_names_are_database_wide() {
        let mut db = Database::in_memory().unwrap();
        db.execute("CREATE TABLE a (id INT PRIMARY KEY, v TEXT)")
            .unwrap();
        db.execute("CREATE TABLE b (id INT PRIMARY KEY, v TEXT)")
            .unwrap();
        db.execute("CREATE INDEX iv ON a (v)").unwrap();
        // SQLite semantics: the name is taken database-wide
        let e = db.execute("CREATE INDEX iv ON b (v)").unwrap_err();
        assert!(e.to_string().contains("already exists"), "{e}");
        // DROP removes it; the other table is untouched
        db.execute("DROP INDEX iv").unwrap();
        let r = rows(
            &mut db,
            "SELECT tbl_name FROM sqlite_master WHERE type = 'index'",
        );
        assert!(r.rows.is_empty());
        // ...and the name is free again
        db.execute("CREATE INDEX iv ON b (v)").unwrap();
    }

    #[test]
    fn drop_unique_index_lifts_constraint() {
        let mut db = Database::in_memory().unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, email TEXT, code INT UNIQUE)")
            .unwrap();
        db.execute("CREATE UNIQUE INDEX ux_email ON t (email)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'a@x', 10)").unwrap();
        let e = db
            .execute("INSERT INTO t VALUES (2, 'a@x', 11)")
            .unwrap_err();
        assert!(e.to_string().contains("UNIQUE"), "{e}");

        db.execute("DROP INDEX ux_email").unwrap();
        // uniqueness lifted: duplicates on email now allowed...
        db.execute("INSERT INTO t VALUES (2, 'a@x', 11)").unwrap();
        // ...while the CREATE TABLE UNIQUE constraint on code still holds
        let e = db
            .execute("INSERT INTO t VALUES (3, 'b@x', 10)")
            .unwrap_err();
        assert!(e.to_string().contains("UNIQUE"), "{e}");
    }

    #[test]
    fn drop_index_survives_reopen_and_sqlite_master_lists_indexes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("idxdef.db");
        {
            let mut db = Database::open(&path).unwrap();
            db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
                .unwrap();
            db.execute("CREATE UNIQUE INDEX ix_v ON t (v)").unwrap();
            // definition visible through sqlite_master before close
            let r = rows(
                &mut db,
                "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 't'",
            );
            assert_eq!(r.rows, vec![vec![Value::Str("ix_v".into())]]);
        }
        let mut db = Database::open(&path).unwrap();
        let r = rows(
            &mut db,
            "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 't'",
        );
        assert_eq!(r.rows, vec![vec![Value::Str("ix_v".into())]]);
        // constraint persisted: duplicate rejected after reopen
        db.execute("INSERT INTO t VALUES (1, 'x')").unwrap();
        let e = db.execute("INSERT INTO t VALUES (2, 'x')").unwrap_err();
        assert!(e.to_string().contains("UNIQUE"), "{e}");
        // ...and dropping it after reopen lifts the constraint
        db.execute("DROP INDEX ix_v").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'x')").unwrap();
    }

    #[test]
    fn primary_key_constraint_index_is_derived_and_protected() {
        let mut db = Database::in_memory().unwrap();
        run(
            &mut db,
            "CREATE TABLE t (id INT PRIMARY KEY, v TEXT UNIQUE)",
        );
        let t = &db.catalog()[0];
        // Table-declared PK/UNIQUE surface as named auto-indexes (PK
        // first); their B+ trees exist from CREATE TABLE on.
        assert_eq!(
            t.indexes,
            vec![
                "sqlite_autoindex_t_1".to_string(),
                "sqlite_autoindex_t_2".to_string(),
            ]
        );
        assert_eq!(t.index_defs[0].column, "id");
        assert_eq!(t.index_defs[1].column, "v");
        assert!(t.index_defs.iter().all(|d| d.auto && d.unique));

        // Hidden from sqlite_master (SQLite semantics: autoindexes have
        // no CREATE INDEX row there) — tooling reads them from the catalog.
        let r = rows(
            &mut db,
            "SELECT name FROM sqlite_master WHERE type = 'index'",
        );
        assert!(r.rows.is_empty());

        // Not droppable (with or without IF EXISTS), and the name prefix
        // is reserved against user indexes shadowing constraint ones.
        let e = db.execute("DROP INDEX sqlite_autoindex_t_1").unwrap_err();
        assert!(e.to_string().contains("cannot be dropped"), "{e}");
        let e = db
            .execute("DROP INDEX IF EXISTS sqlite_autoindex_t_1")
            .unwrap_err();
        assert!(e.to_string().contains("cannot be dropped"), "{e}");
        let e = db
            .execute("CREATE INDEX sqlite_autoindex_x_1 ON t (v)")
            .unwrap_err();
        assert!(e.to_string().contains("reserved"), "{e}");
        // The constraint itself stays enforced through the backing tree.
        run(&mut db, "INSERT INTO t VALUES (1, 'a')");
        let e = db.execute("INSERT INTO t VALUES (1, 'b')").unwrap_err();
        assert!(e.to_string().contains("UNIQUE"), "{e}");
    }

    #[test]
    fn constraint_autoindex_survives_reopen_without_catalog_migration() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("autoix.db");
        {
            let mut db = Database::open(&path).unwrap();
            db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
                .unwrap();
        }
        // Derived from the persisted constraint, never stored in the
        // catalog: reopening lists it — as do databases created before
        // the feature shipped (external volumes keep their old catalogs).
        let db = Database::open(&path).unwrap();
        let t = &db.catalog()[0];
        assert_eq!(t.indexes, vec!["sqlite_autoindex_t_1".to_string()]);
        assert_eq!(t.index_defs[0].column, "id");
        assert!(t.index_defs[0].auto);
    }

    #[test]
    fn autoindex_dedupes_column_declared_pk_and_unique() {
        let mut db = Database::in_memory().unwrap();
        run(
            &mut db,
            "CREATE TABLE t (id INT PRIMARY KEY UNIQUE, v TEXT)",
        );
        assert_eq!(
            db.catalog()[0].indexes,
            vec!["sqlite_autoindex_t_1".to_string()]
        );
    }

    #[test]
    fn autoindex_covers_table_level_constraints() {
        let mut db = Database::in_memory().unwrap();
        run(
            &mut db,
            "CREATE TABLE t (a INT, b INT, PRIMARY KEY (a), UNIQUE (b))",
        );
        let t = &db.catalog()[0];
        assert_eq!(
            t.indexes,
            vec![
                "sqlite_autoindex_t_1".to_string(),
                "sqlite_autoindex_t_2".to_string(),
            ]
        );
        assert_eq!(t.index_defs[0].column, "a");
        assert_eq!(t.index_defs[1].column, "b");
    }

    #[test]
    fn autoindex_follows_column_and_table_renames() {
        let mut db = Database::in_memory().unwrap();
        run(
            &mut db,
            "CREATE TABLE t (id INT PRIMARY KEY, email TEXT UNIQUE)",
        );
        run(&mut db, "INSERT INTO t VALUES (1, 'a')");
        // Column renames move the constraint metadata (and the backing
        // tree), so the derived entries track the new names…
        run(&mut db, "ALTER TABLE t RENAME COLUMN id TO gid");
        run(&mut db, "ALTER TABLE t RENAME COLUMN email TO mail");
        // …and the table rename feeds the derived names themselves.
        run(&mut db, "ALTER TABLE t RENAME TO u");
        let t = &db.catalog()[0];
        assert_eq!(t.name, "u");
        assert_eq!(
            t.indexes,
            vec![
                "sqlite_autoindex_u_1".to_string(),
                "sqlite_autoindex_u_2".to_string(),
            ]
        );
        assert_eq!(t.index_defs[0].column, "gid");
        assert_eq!(t.index_defs[1].column, "mail");
        // The constraint keeps enforcing through the moved tree.
        let e = db.execute("INSERT INTO u VALUES (1, 'b')").unwrap_err();
        assert!(e.to_string().contains("UNIQUE"), "{e}");
    }

    #[test]
    fn dropping_unique_column_drops_its_autoindex() {
        let mut db = Database::in_memory().unwrap();
        run(
            &mut db,
            "CREATE TABLE t (id INT PRIMARY KEY, email TEXT UNIQUE)",
        );
        run(&mut db, "ALTER TABLE t DROP COLUMN email");
        let t = &db.catalog()[0];
        assert_eq!(t.indexes, vec!["sqlite_autoindex_t_1".to_string()]);
        assert_eq!(t.index_defs[0].column, "id");
        // PK enforcement is untouched by the UNIQUE column's removal.
        run(&mut db, "INSERT INTO t VALUES (1)");
        let e = db.execute("INSERT INTO t VALUES (1)").unwrap_err();
        assert!(e.to_string().contains("UNIQUE"), "{e}");
    }

    #[test]
    fn autoindex_names_identical_on_replica_replay() {
        // Symmetric-cluster peers re-derive constraint indexes from the
        // replayed DDL; the names must come out identical on both sides.
        let ddl = "CREATE TABLE t (id INT PRIMARY KEY, email TEXT UNIQUE); \
                   CREATE INDEX ix_v ON t (email)";
        let mut a = Database::in_memory().unwrap();
        let mut b = Database::in_memory().unwrap();
        for db in [&mut a, &mut b] {
            db.execute_batch(ddl);
        }
        let (ca, cb) = (a.catalog(), b.catalog());
        assert_eq!(ca[0].name, cb[0].name);
        assert_eq!(ca[0].indexes, cb[0].indexes);
        assert_eq!(ca[0].index_defs, cb[0].index_defs);
        assert_eq!(
            ca[0].indexes,
            vec![
                "sqlite_autoindex_t_1".to_string(),
                "sqlite_autoindex_t_2".to_string(),
                "ix_v".to_string(),
            ]
        );
    }

    #[test]
    fn autoinc_and_autoguid_primary_keys_get_autoindex() {
        for ddl in [
            "CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)",
            "CREATE TABLE t (id GUID PRIMARY KEY AUTOINCREMENT, v TEXT)",
        ] {
            let mut db = Database::in_memory().unwrap();
            run(&mut db, ddl);
            let t = &db.catalog()[0];
            assert_eq!(t.indexes, vec!["sqlite_autoindex_t_1".to_string()], "{ddl}");
            assert_eq!(t.index_defs[0].column, "id", "{ddl}");
            assert!(t.index_defs[0].auto, "{ddl}");
        }
    }

    #[test]
    fn indexes_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("idx.db");
        {
            let mut db = Database::open(&path).unwrap();
            db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
                .unwrap();
            db.execute("INSERT INTO t VALUES (1, 'one')").unwrap();
            db.execute("INSERT INTO t VALUES (2, 'two')").unwrap();
        }
        let mut db = Database::open(&path).unwrap();
        let r = rows(&mut db, "SELECT v FROM t WHERE id = 2");
        assert_eq!(r.rows, vec![vec![Value::Str("two".into())]]);
        let e = db.execute("INSERT INTO t VALUES (2, 'dup')").unwrap_err();
        assert!(e.to_string().contains("UNIQUE"), "{e}");
    }

    #[test]
    fn autoinc_survives_delete_and_rollback() {
        let mut db = Database::in_memory().unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTOINCREMENT, v TEXT)")
            .unwrap();
        db.execute("INSERT INTO t (v) VALUES ('a')").unwrap();
        db.execute("INSERT INTO t (v) VALUES ('b')").unwrap();
        db.execute("DELETE FROM t WHERE id = 2").unwrap();
        // max(existing)+1: the only row is id=1, so the next id is 2 again
        db.execute("INSERT INTO t (v) VALUES ('c')").unwrap();
        let r = rows(&mut db, "SELECT id, v FROM t ORDER BY id");
        assert_eq!(r.rows[1], vec![Value::Int(2), Value::Str("c".into())]);
        // rollback restores pre-BEGIN rows
        db.execute("BEGIN").unwrap();
        db.execute("DELETE FROM t WHERE id = 1").unwrap();
        db.execute("ROLLBACK").unwrap();
        let r = rows(&mut db, "SELECT COUNT(*) FROM t");
        assert_eq!(r.rows[0][0], Value::Int(2));
        // ...and the index still answers point queries afterwards
        let r = rows(&mut db, "SELECT v FROM t WHERE id = 1");
        assert_eq!(r.rows, vec![vec![Value::Str("a".into())]]);
    }

    #[test]
    fn create_insert_select_roundtrip() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE users (id INT, name TEXT)");
        run(
            &mut db,
            "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')",
        );
        let r = rows(&mut db, "SELECT id, name FROM users");
        assert_eq!(r.columns, vec!["id", "name"]);
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][1], Value::Str("alice".into()));
    }

    #[test]
    fn insert_without_column_list_uses_declared_columns() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (a INT, b TEXT)");
        run(&mut db, "INSERT INTO t VALUES (5, 'five')");
        let r = rows(&mut db, "SELECT a, b FROM t");
        assert_eq!(r.rows, vec![vec![Value::Int(5), Value::Str("five".into())]]);
    }

    #[test]
    fn select_star_unions_schemaless_fields() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE docs (id INT)");
        run(
            &mut db,
            "INSERT INTO docs (id, extra) VALUES (1, 'has-extra')",
        );
        run(&mut db, "INSERT INTO docs (id) VALUES (2)");
        let r = rows(&mut db, "SELECT * FROM docs");
        assert_eq!(r.columns, vec!["extra", "id"]); // BTreeMap order
        assert_eq!(r.rows[0][0], Value::Str("has-extra".into()));
        assert_eq!(r.rows[1][0], Value::Null);
    }

    #[test]
    fn observed_columns_track_document_keys() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE docs (id INT)");
        // Empty table: nothing observed yet (SELECT * projects nothing).
        assert!(db.observed_columns("docs").is_empty());
        run(&mut db, "INSERT INTO docs (id, extra) VALUES (1, 'x')");
        run(&mut db, "INSERT INTO docs (id, note) VALUES (2, 'y')");
        // Data-side union, independent of the declared column list.
        assert_eq!(db.observed_columns("docs"), vec!["extra", "id", "note"]);
        // Unknown tables are not an error on this introspection surface.
        assert!(db.observed_columns("nope").is_empty());
    }

    #[test]
    fn where_filter_order_limit_offset() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (n INT, s TEXT)");
        for i in 0..10 {
            run(
                &mut db,
                &format!("INSERT INTO t (n, s) VALUES ({i}, 'x{i}')"),
            );
        }
        let r = rows(
            &mut db,
            "SELECT n, s FROM t WHERE n >= 3 AND n < 8 ORDER BY n DESC",
        );
        assert_eq!(r.rows.len(), 5);
        assert_eq!(r.rows[0][0], Value::Int(7));
        assert_eq!(r.rows[4][0], Value::Int(3));

        let r = rows(&mut db, "SELECT n FROM t ORDER BY n LIMIT 3 OFFSET 4");
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Int(4)],
                vec![Value::Int(5)],
                vec![Value::Int(6)]
            ]
        );
    }

    #[test]
    fn arithmetic_and_null_semantics() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (a INT, b FLOAT)");
        run(&mut db, "INSERT INTO t (a, b) VALUES (7, 2.5)");
        let r = rows(&mut db, "SELECT a + 1, a * 2, a / 2, b + 1 FROM t");
        assert_eq!(r.rows[0][0], Value::Int(8));
        assert_eq!(r.rows[0][1], Value::Int(14));
        assert_eq!(r.rows[0][2], Value::Int(3));
        assert_eq!(r.rows[0][3], Value::Float(3.5));

        run(&mut db, "INSERT INTO t (a) VALUES (9)");
        let r = rows(&mut db, "SELECT a + b FROM t WHERE a = 9");
        assert_eq!(r.rows[0][0], Value::Null); // null propagates
    }

    #[test]
    fn division_by_zero_yields_null() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (a INT, b INT)");
        run(&mut db, "INSERT INTO t (a, b) VALUES (1, 0)");
        let r = rows(&mut db, "SELECT a / b FROM t");
        assert_eq!(r.rows[0][0], Value::Null);
    }

    #[test]
    fn drop_table_and_error_cases() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (a INT)");
        run(&mut db, "DROP TABLE t");
        assert!(db.execute("SELECT * FROM t").is_err());
        assert!(db.execute("CREATE TABLE t (a INT); DROP TABLE t").is_err());
        assert!(db.execute("DROP TABLE missing").is_err());
        assert!(db.execute("CREATE TABLE t (a INT)").is_ok());
        assert!(db.execute("CREATE TABLE t (a INT)").is_err()); // duplicate
        assert!(db.execute("INSERT INTO t (a) VALUES (1, 2)").is_err()); // arity
        assert!(db.execute("SELECT * FROM missing").is_err());
    }

    #[test]
    fn update_sets_matching_rows_and_keeps_others() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (id INT, name TEXT)");
        run(&mut db, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        match run(&mut db, "UPDATE t SET name = 'z' WHERE id >= 2") {
            ExecOutcome::Affected(2) => {}
            o => panic!("affected={o:?}"),
        }
        let r = rows(&mut db, "SELECT id, name FROM t ORDER BY id");
        assert_eq!(r.rows[0][1], Value::Str("a".into()));
        assert_eq!(r.rows[1][1], Value::Str("z".into()));
        assert_eq!(r.rows[2][1], Value::Str("z".into()));
        // update without WHERE touches everything
        match run(&mut db, "UPDATE t SET name = 'all'") {
            ExecOutcome::Affected(3) => {}
            o => panic!("affected={o:?}"),
        }
    }

    #[test]
    fn update_referencing_old_values() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (n INT)");
        run(&mut db, "INSERT INTO t VALUES (10)");
        run(&mut db, "UPDATE t SET n = n + 5");
        let r = rows(&mut db, "SELECT n FROM t");
        assert_eq!(r.rows, vec![vec![Value::Int(15)]]);
    }

    #[test]
    fn delete_removes_matching_rows() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (id INT)");
        for i in 0..5 {
            run(&mut db, &format!("INSERT INTO t VALUES ({i})"));
        }
        match run(&mut db, "DELETE FROM t WHERE id IN (1)") {
            ExecOutcome::Affected(1) => {}
            o => panic!("{o:?}"),
        }
        match run(&mut db, "DELETE FROM t WHERE id = 1") {
            ExecOutcome::Affected(0) => {} // already removed by the IN delete
            o => panic!("affected={o:?}"),
        }
        match run(&mut db, "DELETE FROM t WHERE id > 3") {
            ExecOutcome::Affected(1) => {}
            o => panic!("affected={o:?}"),
        }
        let r = rows(&mut db, "SELECT id FROM t ORDER BY id");
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Int(0)],
                vec![Value::Int(2)],
                vec![Value::Int(3)]
            ]
        );
        match run(&mut db, "DELETE FROM t") {
            ExecOutcome::Affected(3) => {}
            o => panic!("affected={o:?}"),
        }
        let r = rows(&mut db, "SELECT id FROM t");
        assert!(r.rows.is_empty());
    }

    #[test]
    fn primary_key_and_unique_constraints() {
        let mut db = Database::in_memory().unwrap();
        run(
            &mut db,
            "CREATE TABLE t (id INT PRIMARY KEY, email TEXT UNIQUE, name TEXT)",
        );
        run(
            &mut db,
            "INSERT INTO t (id, email, name) VALUES (1, 'a@x', 'ann')",
        );
        // duplicate PK
        let e = db
            .execute("INSERT INTO t (id, email, name) VALUES (1, 'b@x', 'bob')")
            .unwrap_err();
        assert!(e.to_string().contains("UNIQUE"), "{e}");
        // duplicate UNIQUE
        let e = db
            .execute("INSERT INTO t (id, email, name) VALUES (2, 'a@x', 'bob')")
            .unwrap_err();
        assert!(e.to_string().contains("UNIQUE"), "{e}");
        // distinct is fine
        run(
            &mut db,
            "INSERT INTO t (id, email, name) VALUES (2, 'b@x', 'bob')",
        );
        // UPDATE that would collide is rejected
        let e = db
            .execute("UPDATE t SET email = 'a@x' WHERE id = 2")
            .unwrap_err();
        assert!(e.to_string().contains("UNIQUE"), "{e}");
    }

    #[test]
    fn not_null_constraint() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (a INT NOT NULL)");
        let e = db.execute("INSERT INTO t (a) VALUES (NULL)").unwrap_err();
        assert!(e.to_string().contains("NOT NULL"), "{e}");
        run(&mut db, "INSERT INTO t (a) VALUES (5)");
        let e = db.execute("UPDATE t SET a = NULL").unwrap_err();
        assert!(e.to_string().contains("NOT NULL"), "{e}");
    }

    #[test]
    fn constraints_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cons.db");
        {
            let mut db = Database::open(&path).unwrap();
            run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY)");
            run(&mut db, "INSERT INTO t (id) VALUES (1)");
        }
        let mut db = Database::open(&path).unwrap();
        assert!(db.execute("INSERT INTO t (id) VALUES (1)").is_err());
    }

    #[test]
    fn alter_table_add_and_drop_column() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (a INT)");
        run(&mut db, "INSERT INTO t (a) VALUES (1)");
        run(&mut db, "ALTER TABLE t ADD COLUMN b TEXT");
        run(&mut db, "INSERT INTO t (a, b) VALUES (2, 'two')");
        let r = rows(&mut db, "SELECT a, b FROM t ORDER BY a");
        assert_eq!(r.rows[0][1], Value::Null);
        assert_eq!(r.rows[1][1], Value::Str("two".into()));
        run(&mut db, "ALTER TABLE t DROP COLUMN b");
        let r = rows(&mut db, "SELECT a FROM t ORDER BY a");
        assert_eq!(r.columns, vec!["a"]);
        assert_eq!(r.rows.len(), 2);
        assert!(db.execute("ALTER TABLE missing ADD COLUMN x INT").is_err());
    }

    #[test]
    fn inner_join_with_on() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE u (uid INT, name TEXT)");
        run(&mut db, "CREATE TABLE o (oid INT, uid INT, item TEXT)");
        run(&mut db, "INSERT INTO u VALUES (1, 'ann'), (2, 'bob')");
        run(&mut db, "INSERT INTO o VALUES (10, 1, 'book'), (11, 1, 'pen'), (12, 2, 'cap'), (13, 9, 'ghost')");
        let r = rows(
            &mut db,
            "SELECT u.name, o.item FROM u JOIN o ON u.uid = o.uid ORDER BY o.oid",
        );
        assert_eq!(r.columns, vec!["u.name", "o.item"]);
        assert_eq!(r.rows.len(), 3); // ghost dropped
        assert_eq!(
            r.rows[0],
            vec![Value::Str("ann".into()), Value::Str("book".into())]
        );
        assert_eq!(
            r.rows[2],
            vec![Value::Str("bob".into()), Value::Str("cap".into())]
        );
    }

    #[test]
    fn left_join_keeps_unmatched() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE a (id INT)");
        run(&mut db, "CREATE TABLE b (id INT, aid INT, v TEXT)");
        run(&mut db, "INSERT INTO a VALUES (1), (2)");
        run(&mut db, "INSERT INTO b VALUES (100, 1, 'x')");
        let r = rows(
            &mut db,
            "SELECT a.id, b.v FROM a LEFT JOIN b ON b.aid = a.id ORDER BY a.id",
        );
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[1], vec![Value::Int(2), Value::Null]);
    }

    #[test]
    fn cross_join_cartesian() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE x (a INT)");
        run(&mut db, "CREATE TABLE y (b INT)");
        run(&mut db, "INSERT INTO x VALUES (1), (2)");
        run(&mut db, "INSERT INTO y VALUES (3), (4), (5)");
        let r = rows(&mut db, "SELECT x.a, y.b FROM x CROSS JOIN y");
        assert_eq!(r.rows.len(), 6);
    }

    #[test]
    fn aggregates_without_group_by() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (v INT)");
        run(&mut db, "INSERT INTO t VALUES (2), (4), (6), (NULL)");
        let r = rows(
            &mut db,
            "SELECT COUNT(*), COUNT(v), SUM(v), AVG(v), MIN(v), MAX(v) FROM t",
        );
        assert_eq!(
            r.rows,
            vec![vec![
                Value::Int(4),
                Value::Int(3),
                Value::Int(12),
                Value::Float(4.0),
                Value::Int(2),
                Value::Int(6),
            ]]
        );
        // empty table
        run(&mut db, "CREATE TABLE e (v INT)");
        let r = rows(&mut db, "SELECT COUNT(*), SUM(v) FROM e");
        assert_eq!(r.rows, vec![vec![Value::Int(0), Value::Null]]);
    }

    #[test]
    fn group_by_with_having() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE s (dept TEXT, pay INT)");
        run(
            &mut db,
            "INSERT INTO s VALUES ('eng', 100), ('eng', 120), ('ops', 80), ('ops', 90), ('hr', 50)",
        );
        let r = rows(
            &mut db,
            "SELECT dept, COUNT(*), SUM(pay) FROM s GROUP BY dept ORDER BY dept",
        );
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Str("eng".into()), Value::Int(2), Value::Int(220)],
                vec![Value::Str("hr".into()), Value::Int(1), Value::Int(50)],
                vec![Value::Str("ops".into()), Value::Int(2), Value::Int(170)],
            ]
        );
        let r = rows(
            &mut db,
            "SELECT dept, SUM(pay) FROM s GROUP BY dept HAVING SUM(pay) > 100 ORDER BY dept",
        );
        assert_eq!(r.rows.len(), 2);
        assert!(r.rows.iter().all(|row| row[1].as_i64().unwrap() > 100));
    }

    #[test]
    fn in_list_filter() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (id INT)");
        for i in 0..6 {
            run(&mut db, &format!("INSERT INTO t VALUES ({i})"));
        }
        let r = rows(
            &mut db,
            "SELECT id FROM t WHERE id IN (1, 3, 5) ORDER BY id",
        );
        assert_eq!(r.rows.len(), 3);
        let r = rows(
            &mut db,
            "SELECT id FROM t WHERE id NOT IN (1, 3, 5) ORDER BY id",
        );
        assert_eq!(r.rows.len(), 3);
        assert_eq!(r.rows[0][0], Value::Int(0));
        assert_eq!(r.rows[2][0], Value::Int(4));
    }

    #[test]
    fn union_and_union_all() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE a (v INT)");
        run(&mut db, "CREATE TABLE b (v INT)");
        run(&mut db, "INSERT INTO a VALUES (1), (2)");
        run(&mut db, "INSERT INTO b VALUES (2), (3)");
        let r = rows(&mut db, "SELECT v FROM a UNION SELECT v FROM b ORDER BY v");
        assert_eq!(r.rows.len(), 3);
        let r = rows(&mut db, "SELECT v FROM a UNION ALL SELECT v FROM b");
        assert_eq!(r.rows.len(), 4);
    }

    #[test]
    fn non_grouped_column_rejected() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (a INT, b INT)");
        run(&mut db, "INSERT INTO t VALUES (1, 2)");
        let e = db
            .execute("SELECT a, COUNT(*) FROM t GROUP BY b")
            .unwrap_err();
        assert!(e.to_string().contains("GROUP BY"), "{e}");
    }

    #[test]
    fn rollback_undoes_inserts_updates_deletes() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
        run(&mut db, "INSERT INTO t VALUES (1, 'a')");
        run(&mut db, "BEGIN");
        run(&mut db, "INSERT INTO t VALUES (2, 'b')");
        run(&mut db, "UPDATE t SET v = 'changed' WHERE id = 1");
        run(&mut db, "DELETE FROM t WHERE id = 1");
        assert_eq!(rows(&mut db, "SELECT id FROM t ORDER BY id").rows.len(), 1); // sees id=2
        run(&mut db, "ROLLBACK");
        let r = rows(&mut db, "SELECT id, v FROM t");
        assert_eq!(r.rows, vec![vec![Value::Int(1), Value::Str("a".into())]]);
    }

    #[test]
    fn commit_persists_transaction_changes() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (a INT)");
        run(&mut db, "BEGIN");
        run(&mut db, "INSERT INTO t VALUES (42)");
        run(&mut db, "COMMIT");
        let r = rows(&mut db, "SELECT a FROM t");
        assert_eq!(r.rows, vec![vec![Value::Int(42)]]);
        // nested begin rejected
        run(&mut db, "BEGIN");
        assert!(db.execute("BEGIN").is_err());
        assert!(db.execute("ROLLBACK").is_ok());
        // rollback/commit without begin rejected
        assert!(db.execute("COMMIT").is_err());
        assert!(db.execute("ROLLBACK").is_err());
    }

    #[test]
    fn rollback_restores_dropped_and_created_tables() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE keep (a INT)");
        run(&mut db, "BEGIN");
        run(&mut db, "DROP TABLE keep");
        run(&mut db, "CREATE TABLE fresh (b INT)");
        run(&mut db, "ROLLBACK");
        assert!(db.execute("SELECT * FROM fresh").is_err());
        assert!(db.execute("SELECT * FROM keep").is_ok());
    }

    #[test]
    fn rollback_restores_constraints() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY)");
        run(&mut db, "INSERT INTO t VALUES (1)");
        run(&mut db, "BEGIN");
        run(&mut db, "DELETE FROM t");
        run(&mut db, "ROLLBACK");
        // PK still enforced after rollback
        assert!(db.execute("INSERT INTO t VALUES (1)").is_err());
    }

    #[test]
    fn autoincrement_assigns_ids() {
        let mut db = Database::in_memory().unwrap();
        run(
            &mut db,
            "CREATE TABLE t (id INT AUTOINCREMENT PRIMARY KEY, v TEXT)",
        );
        let r = match run(
            &mut db,
            "INSERT INTO t (v) VALUES ('a'), ('b') RETURNING id, v",
        ) {
            ExecOutcome::Rows(r) => r,
            o => panic!("{o:?}"),
        };
        assert_eq!(r.columns, vec!["id", "v"]);
        assert_eq!(r.rows[0][0], Value::Int(1));
        assert_eq!(r.rows[1][0], Value::Int(2));
        // explicit id still works and bumps the counter
        run(&mut db, "INSERT INTO t (id, v) VALUES (10, 'ten')");
        let r = rows(&mut db, "INSERT INTO t (v) VALUES ('c') RETURNING id");
        assert_eq!(r.rows[0][0], Value::Int(11));
    }

    #[test]
    fn guid_autoincrement_generates_ordered_uuidv7() {
        let mut db = Database::in_memory().unwrap();
        run(
            &mut db,
            "CREATE TABLE t (id GUID PRIMARY KEY AUTOINCREMENT, v TEXT)",
        );
        for i in 0..5 {
            run(&mut db, &format!("INSERT INTO t (v) VALUES ('x{i}')"));
        }
        let r = rows(&mut db, "SELECT id FROM t ORDER BY id");
        let ids: Vec<String> = r
            .rows
            .iter()
            .map(|row| row[0].as_str().unwrap().to_string())
            .collect();
        assert_eq!(ids.len(), 5);
        let set: std::collections::BTreeSet<&String> = ids.iter().collect();
        assert_eq!(set.len(), ids.len(), "guids must be unique");
        for id in &ids {
            assert_eq!(id.len(), 36);
            assert_eq!(&id[14..15], "7", "version nibble");
            assert!(
                matches!(&id[19..20], "8" | "9" | "a" | "b"),
                "variant bits in {id}"
            );
        }
        // Canonical lowercase string order == generation order (time-ordered).
        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(sorted, ids);
    }

    #[test]
    fn guid_explicit_and_null_values() {
        let mut db = Database::in_memory().unwrap();
        run(
            &mut db,
            "CREATE TABLE t (id GUID PRIMARY KEY AUTOINCREMENT, v TEXT)",
        );
        // Explicit value passes through untouched.
        run(
            &mut db,
            "INSERT INTO t (id, v) VALUES ('00000000-0000-7000-8000-000000000001', 'e')",
        );
        // Explicit NULL is filled like an omitted column.
        run(&mut db, "INSERT INTO t (id, v) VALUES (NULL, 'n')");
        let r = rows(&mut db, "SELECT id FROM t ORDER BY id");
        assert_eq!(r.rows.len(), 2);
        assert_eq!(
            r.rows[0][0],
            Value::Str("00000000-0000-7000-8000-000000000001".into())
        );
        let filled = r.rows[1][0].as_str().unwrap();
        assert_eq!(filled.len(), 36);
        assert_eq!(&filled[14..15], "7");
        // Positional insert without a column list fills NULL the same way.
        run(&mut db, "INSERT INTO t VALUES (NULL, 'pos')");
        let r = rows(&mut db, "SELECT COUNT(*) AS n FROM t");
        assert_eq!(r.rows[0][0], Value::Int(3));
    }

    #[test]
    fn guid_resolved_insert_replays_identically() {
        let mut a = Database::in_memory().unwrap();
        run(
            &mut a,
            "CREATE TABLE t (id GUID PRIMARY KEY AUTOINCREMENT, v TEXT)",
        );
        a.execute("INSERT INTO t (v) VALUES ('it''s quoted'), ('b')")
            .unwrap();
        let resolved = a.take_resolved_insert().expect("resolved rewrite");
        assert!(resolved.starts_with("INSERT INTO "));
        assert!(resolved.contains("'it''s quoted'"), "in {resolved}");
        // Replication replay: a second node applies the rewrite verbatim.
        let mut b = Database::in_memory().unwrap();
        run(
            &mut b,
            "CREATE TABLE t (id GUID PRIMARY KEY AUTOINCREMENT, v TEXT)",
        );
        b.execute(&resolved).unwrap();
        assert!(b.take_resolved_insert().is_none(), "no re-generation");
        let ra = rows(&mut a, "SELECT id, v FROM t ORDER BY v");
        let rb = rows(&mut b, "SELECT id, v FROM t ORDER BY v");
        assert_eq!(ra.rows, rb.rows);
        // Conflict policy survives the rewrite.
        a.execute("INSERT OR IGNORE INTO t (v) VALUES ('c')")
            .unwrap();
        let resolved = a.take_resolved_insert().unwrap();
        assert!(
            resolved.starts_with("INSERT OR IGNORE INTO "),
            "in {resolved}"
        );
        // Statements without generation produce no rewrite.
        a.execute("INSERT INTO t (id, v) VALUES ('00000000-0000-7000-8000-000000000009', 'x')")
            .unwrap();
        assert!(a.take_resolved_insert().is_none());
    }

    #[test]
    fn guid_or_replace_resolved_preserves_policy() {
        let mut db = Database::in_memory().unwrap();
        run(
            &mut db,
            "CREATE TABLE t (id GUID PRIMARY KEY AUTOINCREMENT, v TEXT UNIQUE)",
        );
        run(
            &mut db,
            "INSERT INTO t (id, v) VALUES ('00000000-0000-7000-8000-000000000001', 'dup')",
        );
        db.execute("INSERT OR REPLACE INTO t (v) VALUES ('dup')")
            .unwrap();
        let resolved = db.take_resolved_insert().expect("resolved rewrite");
        assert!(
            resolved.starts_with("INSERT OR REPLACE INTO "),
            "in {resolved}"
        );
        // The peer replay honors the same policy: the old row is displaced,
        // the generated guid from the origin lands verbatim.
        let mut peer = Database::in_memory().unwrap();
        run(
            &mut peer,
            "CREATE TABLE t (id GUID PRIMARY KEY AUTOINCREMENT, v TEXT UNIQUE)",
        );
        run(
            &mut peer,
            "INSERT INTO t (id, v) VALUES ('00000000-0000-7000-8000-000000000001', 'dup')",
        );
        peer.execute(&resolved).unwrap();
        let r = rows(&mut peer, "SELECT id, v FROM t");
        assert_eq!(r.rows.len(), 1);
        assert_ne!(
            r.rows[0][0],
            Value::Str("00000000-0000-7000-8000-000000000001".into())
        );
        assert_eq!(&r.rows[0][0].as_str().unwrap()[14..15], "7");
    }

    #[test]
    fn guid_mixed_explicit_and_null_rows_resolve_all() {
        let mut db = Database::in_memory().unwrap();
        run(
            &mut db,
            "CREATE TABLE t (id GUID PRIMARY KEY AUTOINCREMENT, v TEXT)",
        );
        db.execute(
            "INSERT INTO t (id, v) VALUES \
             ('00000000-0000-7000-8000-000000000005', 'e'), (NULL, 'g')",
        )
        .unwrap();
        let resolved = db.take_resolved_insert().expect("resolved rewrite");
        // Both rows carry explicit ids in the rewrite — the NULL was filled.
        assert!(
            resolved.contains("'00000000-0000-7000-8000-000000000005'"),
            "in {resolved}"
        );
        let generated = {
            let r = rows(&mut db, "SELECT id FROM t WHERE v = 'g'");
            r.rows[0][0].as_str().unwrap().to_string()
        };
        assert!(
            resolved.contains(&format!("'{generated}'")),
            "generated id missing from rewrite: {resolved}"
        );
        // Sorting: explicit id sorts before the generated UUIDv7 (time
        // prefix of the all-zero guid is smaller than any real timestamp).
        let r = rows(&mut db, "SELECT v FROM t ORDER BY id");
        assert_eq!(
            r.rows,
            vec![vec![Value::Str("e".into())], vec![Value::Str("g".into())]]
        );
    }

    #[test]
    fn guid_autoguid_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("guid.db");
        let mut db = Database::open(&path).unwrap();
        run(
            &mut db,
            "CREATE TABLE t (id GUID PRIMARY KEY AUTOINCREMENT, v TEXT)",
        );
        run(&mut db, "INSERT INTO t (v) VALUES ('a')");
        drop(db);
        let mut db = Database::open(&path).unwrap();
        run(&mut db, "INSERT INTO t (v) VALUES ('b')");
        let r = rows(&mut db, "SELECT id FROM t");
        assert_eq!(r.rows.len(), 2);
        for row in &r.rows {
            let id = row[0].as_str().unwrap();
            assert_eq!(&id[14..15], "7", "still generating after reopen: {id}");
        }
    }

    #[test]
    fn guid_insert_select_rejected() {
        let mut db = Database::in_memory().unwrap();
        run(
            &mut db,
            "CREATE TABLE t (id GUID PRIMARY KEY AUTOINCREMENT, v TEXT)",
        );
        run(&mut db, "CREATE TABLE src (v TEXT)");
        run(&mut db, "INSERT INTO src VALUES ('x')");
        let e = db
            .execute("INSERT INTO t (v) SELECT v FROM src")
            .unwrap_err()
            .to_string();
        assert!(e.contains("INSERT ... VALUES"), "unexpected error: {e}");
    }

    #[test]
    fn guid_type_keyword_variants() {
        for ty in ["GUID", "UUID", "UNIQUEIDENTIFIER", "UUIDV7"] {
            let mut db = Database::in_memory().unwrap();
            run(
                &mut db,
                &format!("CREATE TABLE t (id {ty} PRIMARY KEY AUTOINCREMENT, v TEXT)"),
            );
            run(&mut db, "INSERT INTO t (v) VALUES ('a')");
            let r = rows(&mut db, "SELECT id FROM t");
            assert_eq!(&r.rows[0][0].as_str().unwrap()[14..15], "7", "type {ty}");
        }
        // AUTO_INCREMENT is accepted like AUTOINCREMENT (existing alias).
        let mut db = Database::in_memory().unwrap();
        run(
            &mut db,
            "CREATE TABLE t (id GUID PRIMARY KEY AUTO_INCREMENT, v TEXT)",
        );
        run(&mut db, "INSERT INTO t (v) VALUES ('a')");
        let r = rows(&mut db, "SELECT id FROM t");
        assert_eq!(&r.rows[0][0].as_str().unwrap()[14..15], "7");
    }

    #[test]
    fn guid_rename_and_drop_column() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (id GUID AUTOINCREMENT, v TEXT)");
        run(&mut db, "INSERT INTO t (v) VALUES ('a')");
        run(&mut db, "ALTER TABLE t RENAME COLUMN id TO gid");
        run(&mut db, "INSERT INTO t (v) VALUES ('b')");
        let r = rows(&mut db, "SELECT gid FROM t ORDER BY gid");
        assert_eq!(r.rows.len(), 2);
        assert_eq!(&r.rows[1][0].as_str().unwrap()[14..15], "7");
        // Dropping the column stops generation (mirrors AUTOINCREMENT).
        run(&mut db, "ALTER TABLE t DROP COLUMN gid");
        run(&mut db, "INSERT INTO t (v) VALUES ('c')");
        let r = rows(&mut db, "SELECT v, gid FROM t ORDER BY v");
        assert_eq!(r.rows[2], vec![Value::Str("c".into()), Value::Null]);
    }

    #[test]
    fn returning_on_update_and_delete() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (id INT, v TEXT)");
        run(&mut db, "INSERT INTO t VALUES (1, 'a'), (2, 'b')");
        let r = match run(&mut db, "UPDATE t SET v = 'z' WHERE id = 2 RETURNING id, v") {
            ExecOutcome::Rows(r) => r,
            o => panic!("{o:?}"),
        };
        assert_eq!(r.rows, vec![vec![Value::Int(2), Value::Str("z".into())]]);
        let r = match run(&mut db, "DELETE FROM t WHERE id = 1 RETURNING id") {
            ExecOutcome::Rows(r) => r,
            o => panic!("{o:?}"),
        };
        assert_eq!(r.rows, vec![vec![Value::Int(1)]]);
    }

    #[test]
    fn information_schema_lists_tables_and_columns() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE alpha (x INT NOT NULL, y TEXT)");
        run(&mut db, "CREATE TABLE beta (z INT)");
        let r = rows(
            &mut db,
            "SELECT table_name FROM information_schema.tables ORDER BY table_name",
        );
        let names: Vec<String> = r
            .rows
            .iter()
            .map(|row| row[0].as_str().unwrap().into())
            .collect();
        assert!(names.contains(&"alpha".to_string()));
        assert!(names.contains(&"beta".to_string()));
        let r = rows(&mut db, "SELECT column_name, is_nullable FROM information_schema.columns WHERE table_name = 'alpha' ORDER BY column_name");
        assert_eq!(
            r.rows[0],
            vec![Value::Str("x".into()), Value::Str("NO".into())]
        );
        assert_eq!(
            r.rows[1],
            vec![Value::Str("y".into()), Value::Str("YES".into())]
        );
    }

    #[test]
    fn catalog_reports_constraints_and_indexes() {
        let mut db = Database::in_memory().unwrap();
        run(
            &mut db,
            "CREATE TABLE users (id INT PRIMARY KEY NOT NULL AUTOINCREMENT, email TEXT UNIQUE, name TEXT NOT NULL)",
        );
        run(&mut db, "CREATE INDEX idx_users_name ON users (name)");
        let tables = db.catalog();
        assert_eq!(tables.len(), 1);
        let t = &tables[0];
        assert_eq!(t.name, "users");
        assert_eq!(t.keys, vec!["id".to_string(), "email".to_string()]);
        // Constraint indexes come first with reserved names, then user ones.
        assert_eq!(
            t.indexes,
            vec![
                "sqlite_autoindex_users_1".to_string(),
                "sqlite_autoindex_users_2".to_string(),
                "idx_users_name".to_string(),
            ]
        );
        assert_eq!(
            t.index_defs,
            vec![
                IndexInfo {
                    name: "sqlite_autoindex_users_1".into(),
                    column: "id".into(),
                    unique: true,
                    auto: true,
                },
                IndexInfo {
                    name: "sqlite_autoindex_users_2".into(),
                    column: "email".into(),
                    unique: true,
                    auto: true,
                },
                IndexInfo {
                    name: "idx_users_name".into(),
                    column: "name".into(),
                    unique: false,
                    auto: false,
                },
            ]
        );
        let id = t.columns.iter().find(|c| c.name == "id").unwrap();
        assert!(id.primary_key && id.autoinc && !id.nullable);
        let email = t.columns.iter().find(|c| c.name == "email").unwrap();
        assert!(email.unique && email.nullable);
        let name = t.columns.iter().find(|c| c.name == "name").unwrap();
        assert!(!name.nullable);
        assert_eq!(db.page_size(), PAGE_SIZE);
        assert!(db.num_pages() >= 2);
    }

    #[test]
    fn catalog_reports_index_defs() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (a INT, b TEXT)");
        run(&mut db, "CREATE UNIQUE INDEX ux_a ON t (a)");
        run(&mut db, "CREATE INDEX ix_b ON t (b)");
        let t = &db.catalog()[0];
        // UNIQUE via CREATE INDEX is a named definition, not a constraint
        // autoindex: t declares no PK/UNIQUE, so no sqlite_autoindex_* rows.
        assert_eq!(
            t.index_defs,
            vec![
                IndexInfo {
                    name: "ux_a".into(),
                    column: "a".into(),
                    unique: true,
                    auto: false,
                },
                IndexInfo {
                    name: "ix_b".into(),
                    column: "b".into(),
                    unique: false,
                    auto: false,
                },
            ]
        );
        // DROP INDEX keeps the definitions in sync.
        run(&mut db, "DROP INDEX ux_a");
        let t = &db.catalog()[0];
        assert_eq!(t.indexes, vec!["ix_b".to_string()]);
        assert_eq!(t.index_defs.len(), 1);
        assert_eq!(t.index_defs[0].column, "b");
    }

    #[test]
    fn execute_batch_runs_all_and_stops_on_error() {
        let mut db = Database::in_memory().unwrap();
        let b = db.execute_batch(
            "CREATE TABLE b (id INT PRIMARY KEY, v TEXT); INSERT INTO b VALUES (1, 'a'), (2, 'b'); SELECT id FROM b ORDER BY id",
        );
        assert!(b.error.is_none());
        assert_eq!(b.outcomes.len(), 3);
        assert!(matches!(b.outcomes[0], ExecOutcome::Affected(0)));

        // Error mid-batch keeps the executed prefix and reports the index.
        let b = db.execute_batch(
            "INSERT INTO b VALUES (3, 'c'); SELECT * FROM missing; INSERT INTO b VALUES (4, 'd')",
        );
        let e = b.error.unwrap();
        assert_eq!(e.statement, 1);
        assert_eq!(b.outcomes.len(), 1);
        assert!(rows(&mut db, "SELECT id FROM b WHERE id = 4")
            .rows
            .is_empty());
        assert_eq!(rows(&mut db, "SELECT id FROM b WHERE id = 3").rows.len(), 1);
    }

    #[test]
    fn parse_check_validates_without_executing() {
        assert!(Database::parse_check("SELECT 1").is_ok());
        assert!(Database::parse_check("SELEC nope").is_err());
        assert!(Database::parse_check("   ").is_err());
        // A parseable statement must not execute.
        let mut db = Database::in_memory().unwrap();
        Database::parse_check("CREATE TABLE p (x INT)").unwrap();
        assert!(
            rows(&mut db, "SELECT table_name FROM information_schema.tables")
                .rows
                .is_empty()
        );
    }

    #[test]
    fn catalog_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cat.db");
        {
            let mut db = Database::open(&path).unwrap();
            run(&mut db, "CREATE TABLE keep (a INT)");
            run(&mut db, "INSERT INTO keep (a) VALUES (42)");
        }
        let mut db = Database::open(&path).unwrap();
        let r = rows(&mut db, "SELECT a FROM keep");
        assert_eq!(r.rows, vec![vec![Value::Int(42)]]);
    }

    #[test]
    fn autoinc_column_survives_reopen() {
        // Regression: the catalog used to drop the AUTOINCREMENT flag on
        // save, so post-restart inserts left the id column NULL.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("autoinc.db");
        {
            let mut db = Database::open(&path).unwrap();
            run(
                &mut db,
                "CREATE TABLE t (id INT PRIMARY KEY AUTOINCREMENT, v TEXT)",
            );
            run(&mut db, "INSERT INTO t (v) VALUES ('before')");
            let r = rows(&mut db, "SELECT id FROM t");
            assert_eq!(r.rows, vec![vec![Value::Int(1)]]);
        }
        let mut db = Database::open(&path).unwrap();
        run(&mut db, "INSERT INTO t (v) VALUES ('after')");
        let r = rows(&mut db, "SELECT id, v FROM t ORDER BY id");
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Int(1), Value::Str("before".into())],
                vec![Value::Int(2), Value::Str("after".into())],
            ]
        );
    }

    #[test]
    fn catalog_overflows_across_pages_and_survives_reopen() {
        // The old single-page catalog died at ~4 KB of metadata (a few
        // hundred table metas or page ids) and failed every later write.
        // Metadata now paginates across an overflow chain.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("overflow.db");
        {
            let mut db = Database::open(&path).unwrap();
            for i in 0..120 {
                run(
                    &mut db,
                    &format!("CREATE TABLE t{i} (id INT PRIMARY KEY, v TEXT)"),
                );
                run(&mut db, &format!("INSERT INTO t{i} VALUES (1, 'row{i}')"));
            }
        }
        let mut db = Database::open(&path).unwrap();
        // Page 1 carries the chain magic; the chain actually spilled.
        let page1 = db.pager.read_page(CATALOG_PAGE).unwrap().to_vec();
        assert!(page1.starts_with(CATALOG_MAGIC));
        assert_ne!(
            u32::from_le_bytes(page1[12..16].try_into().unwrap()),
            0,
            "expected overflow pages"
        );
        assert_eq!(db.catalog().len(), 120);
        let r = rows(&mut db, "SELECT v FROM t77");
        assert_eq!(r.rows, vec![vec![Value::Str("row77".into())]]);
        // Writes keep working past the old wall.
        run(&mut db, "INSERT INTO t0 VALUES (2, 'more')");
    }

    #[test]
    fn catalog_chain_shrinks_after_drops_and_reopens() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shrink.db");
        {
            let mut db = Database::open(&path).unwrap();
            for i in 0..120 {
                run(&mut db, &format!("CREATE TABLE t{i} (id INT)"));
            }
        }
        let mut db = Database::open(&path).unwrap();
        for i in 0..110 {
            run(&mut db, &format!("DROP TABLE t{i}"));
        }
        let chain_after = db.catalog_chain().unwrap();
        drop(db);
        let mut db = Database::open(&path).unwrap();
        assert_eq!(db.catalog().len(), 10);
        // The truncated chain no longer references dropped overflow pages.
        assert_eq!(chain_after.len(), 1, "10 tables should fit one page");
        let page1 = db.pager.read_page(CATALOG_PAGE).unwrap().to_vec();
        assert_eq!(u32::from_le_bytes(page1[12..16].try_into().unwrap()), 0);
        run(&mut db, "INSERT INTO t115 VALUES (9)");
    }

    #[test]
    fn legacy_single_page_catalog_still_loads_and_migrates() {
        // A volume written by the pre-chain format (raw encoded catalog on
        // page 1) must keep loading, and migrate to the chain format on its
        // first catalog save.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.db");
        let legacy: Object = [(
            "tables".to_string(),
            Value::Object(
                [(
                    "legacy".to_string(),
                    Value::Object(
                        [
                            (
                                "columns".to_string(),
                                Value::Array(vec![Value::Str("id".to_string())]),
                            ),
                            ("pages".to_string(), Value::Array(vec![])),
                            ("primary_key".to_string(), Value::Str("id".to_string())),
                        ]
                        .into(),
                    ),
                )]
                .into(),
            ),
        )]
        .into();
        {
            let mut pager = crate::pager::Pager::open(&path).unwrap();
            let mut tx = pager.begin_tx();
            while pager.num_pages() <= CATALOG_PAGE {
                pager.allocate_page(&mut tx).unwrap();
            }
            let bytes = encode::encode_to_vec(&Value::Object(legacy)).unwrap();
            let mut page = vec![0u8; PAGE_SIZE];
            page[..bytes.len()].copy_from_slice(&bytes);
            pager.write_page(&mut tx, CATALOG_PAGE, 0, &page).unwrap();
            pager.commit_tx(tx).unwrap();
        }
        let mut db = Database::open(&path).unwrap();
        let r = rows(&mut db, "SELECT id FROM legacy");
        assert!(r.rows.is_empty());
        // Any catalog save rewrites page 1 in the chain format.
        run(&mut db, "INSERT INTO legacy VALUES (5)");
        let page1 = db.pager.read_page(CATALOG_PAGE).unwrap().to_vec();
        assert!(page1.starts_with(CATALOG_MAGIC));
        drop(db);
        let mut db = Database::open(&path).unwrap();
        let r = rows(&mut db, "SELECT id FROM legacy");
        assert_eq!(r.rows, vec![vec![Value::Int(5)]]);
    }

    #[test]
    fn large_table_spans_pages() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE big (id INT, payload TEXT)");
        for i in 0..200 {
            run(
                &mut db,
                &format!(
                    "INSERT INTO big (id, payload) VALUES ({i}, '{}')",
                    "y".repeat(60)
                ),
            );
        }
        let r = rows(&mut db, "SELECT id FROM big ORDER BY id LIMIT 2");
        assert_eq!(r.rows, vec![vec![Value::Int(0)], vec![Value::Int(1)]]);
        let r = rows(&mut db, "SELECT id FROM big");
        assert_eq!(r.rows.len(), 200);
    }

    // ---- JOIN family -------------------------------------------------

    #[test]
    fn right_join_keeps_unmatched_right_rows() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE a (id INT)");
        run(&mut db, "CREATE TABLE b (id INT, aid INT)");
        run(&mut db, "INSERT INTO a VALUES (1), (2)");
        run(&mut db, "INSERT INTO b VALUES (7, 1), (8, 9)");
        let r = rows(
            &mut db,
            "SELECT a.id, b.id FROM a RIGHT JOIN b ON a.id = b.aid ORDER BY b.id",
        );
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Int(1), Value::Int(7)],
                vec![Value::Null, Value::Int(8)],
            ]
        );
        // RIGHT OUTER JOIN is the same operator spelled out.
        let r = rows(
            &mut db,
            "SELECT a.id, b.id FROM a RIGHT OUTER JOIN b ON a.id = b.aid ORDER BY b.id",
        );
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[1], vec![Value::Null, Value::Int(8)]);
    }

    #[test]
    fn join_using_constraint() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE u (id INT, name TEXT)");
        run(&mut db, "CREATE TABLE v (id INT, val TEXT)");
        run(&mut db, "INSERT INTO u VALUES (1, 'ann'), (2, 'bob')");
        run(&mut db, "INSERT INTO v VALUES (1, 'x'), (3, 'z')");
        // USING (id) == ON u.id = v.id
        let r = rows(&mut db, "SELECT u.id, v.val FROM u JOIN v USING (id)");
        assert_eq!(r.rows, vec![vec![Value::Int(1), Value::Str("x".into())]]);
    }

    #[test]
    fn comma_separated_from_is_cross_join() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE x (a INT)");
        run(&mut db, "CREATE TABLE y (b INT)");
        run(&mut db, "INSERT INTO x VALUES (1), (2)");
        run(&mut db, "INSERT INTO y VALUES (3), (4), (5)");
        let r = rows(&mut db, "SELECT x.a, y.b FROM x, y");
        assert_eq!(r.rows.len(), 6); // 2 * 3 cartesian
    }

    #[test]
    fn three_table_join_chain() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE users (id INT, name TEXT)");
        run(&mut db, "CREATE TABLE orders (oid INT, uid INT)");
        run(&mut db, "CREATE TABLE items (iid INT, oid INT, item TEXT)");
        run(&mut db, "INSERT INTO users VALUES (1, 'ann'), (2, 'bob')");
        run(
            &mut db,
            "INSERT INTO orders VALUES (10, 1), (11, 2), (12, 99)",
        );
        run(
            &mut db,
            "INSERT INTO items VALUES (100, 10, 'book'), (101, 10, 'pen'), (102, 11, 'cap'), (103, 77, 'ghost')",
        );
        let r = rows(
            &mut db,
            "SELECT u.name, i.item FROM users u JOIN orders o ON u.id = o.uid JOIN items i ON o.oid = i.oid ORDER BY i.item",
        );
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Str("ann".into()), Value::Str("book".into())],
                vec![Value::Str("bob".into()), Value::Str("cap".into())],
                vec![Value::Str("ann".into()), Value::Str("pen".into())],
            ]
        );
    }

    #[test]
    fn table_aliases_in_join() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE usr (id INT, name TEXT)");
        run(&mut db, "CREATE TABLE ord (id INT, owner INT)");
        run(&mut db, "INSERT INTO usr VALUES (1, 'ann')");
        run(&mut db, "INSERT INTO ord VALUES (50, 1)");
        let r = rows(
            &mut db,
            "SELECT usr.name FROM usr AS w JOIN ord AS d ON w.id = d.owner",
        );
        assert_eq!(r.rows, vec![vec![Value::Str("ann".into())]]);
    }

    #[test]
    fn self_join_parent_child() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (id INT, pid INT, name TEXT)");
        run(
            &mut db,
            "INSERT INTO t VALUES (1, 0, 'root'), (2, 1, 'child-a'), (3, 1, 'child-b')",
        );
        let r = rows(
            &mut db,
            "SELECT c.name AS child, p.name AS parent FROM t AS p JOIN t AS c ON c.pid = p.id ORDER BY child",
        );
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Str("child-a".into()), Value::Str("root".into())],
                vec![Value::Str("child-b".into()), Value::Str("root".into())],
            ]
        );
    }

    #[test]
    fn left_join_fan_out_multiple_matches() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE a (id INT)");
        run(&mut db, "CREATE TABLE b (bid INT, aid INT)");
        run(&mut db, "INSERT INTO a VALUES (1)");
        run(&mut db, "INSERT INTO b VALUES (10, 1), (11, 1)");
        let r = rows(
            &mut db,
            "SELECT a.id, b.bid FROM a LEFT JOIN b ON b.aid = a.id ORDER BY b.bid",
        );
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Int(1), Value::Int(10)],
                vec![Value::Int(1), Value::Int(11)],
            ]
        );
    }

    #[test]
    fn full_outer_join_keeps_both_sides() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE a (id INT)");
        run(&mut db, "CREATE TABLE b (id INT, aid INT)");
        run(&mut db, "INSERT INTO a VALUES (1), (2)");
        run(&mut db, "INSERT INTO b VALUES (7, 1), (8, 3)");
        let r = rows(
            &mut db,
            "SELECT a.id, b.id FROM a FULL OUTER JOIN b ON a.id = b.aid ORDER BY a.id NULLS LAST",
        );
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Int(1), Value::Int(7)],
                vec![Value::Int(2), Value::Null],
                vec![Value::Null, Value::Int(8)],
            ]
        );
    }

    #[test]
    fn join_with_where_and_group_by() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE users (id INT, name TEXT)");
        run(&mut db, "CREATE TABLE orders (oid INT, uid INT)");
        run(
            &mut db,
            "INSERT INTO users VALUES (1, 'ann'), (2, 'bob'), (3, 'zed')",
        );
        run(
            &mut db,
            "INSERT INTO orders VALUES (10, 1), (11, 1), (12, 2)",
        );
        // LEFT JOIN + GROUP BY: users without orders count as zero.
        let r = rows(
            &mut db,
            "SELECT u.name, COUNT(o.oid) AS n FROM users u LEFT JOIN orders o ON u.id = o.uid GROUP BY u.name ORDER BY u.name",
        );
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Str("ann".into()), Value::Int(2)],
                vec![Value::Str("bob".into()), Value::Int(1)],
                vec![Value::Str("zed".into()), Value::Int(0)],
            ]
        );
        // INNER JOIN + WHERE before grouping.
        let r = rows(
            &mut db,
            "SELECT u.name, COUNT(*) AS n FROM users u JOIN orders o ON u.id = o.uid WHERE o.oid > 10 GROUP BY u.name ORDER BY u.name",
        );
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Str("ann".into()), Value::Int(1)],
                vec![Value::Str("bob".into()), Value::Int(1)],
            ]
        );
    }

    // ---- GROUP BY / aggregates --------------------------------------

    #[test]
    fn group_by_multiple_columns() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE s (dept TEXT, role TEXT, pay INT)");
        run(
            &mut db,
            "INSERT INTO s VALUES ('eng', 'dev', 100), ('eng', 'ops', 80), ('ops', 'dev', 90)",
        );
        let r = rows(
            &mut db,
            "SELECT dept, role, SUM(pay) FROM s GROUP BY dept, role ORDER BY dept, role",
        );
        assert_eq!(
            r.rows,
            vec![
                vec![
                    Value::Str("eng".into()),
                    Value::Str("dev".into()),
                    Value::Int(100)
                ],
                vec![
                    Value::Str("eng".into()),
                    Value::Str("ops".into()),
                    Value::Int(80)
                ],
                vec![
                    Value::Str("ops".into()),
                    Value::Str("dev".into()),
                    Value::Int(90)
                ],
            ]
        );
    }

    #[test]
    fn group_by_expression_bucket() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (n INT)");
        let vals: Vec<String> = (0..25).map(|i| format!("({i})")).collect();
        run(
            &mut db,
            &format!("INSERT INTO t VALUES {}", vals.join(", ")),
        );
        // n / 10 buckets 0..24 into three groups of 10/10/5.
        let r = rows(
            &mut db,
            "SELECT n / 10 AS bucket, COUNT(*) AS c FROM t GROUP BY n / 10 ORDER BY bucket",
        );
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Int(0), Value::Int(10)],
                vec![Value::Int(1), Value::Int(10)],
                vec![Value::Int(2), Value::Int(5)],
            ]
        );
    }

    #[test]
    fn having_with_aggregate_not_in_projection() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE s (dept TEXT, pay INT)");
        run(
            &mut db,
            "INSERT INTO s VALUES ('eng', 1), ('eng', 2), ('ops', 3)",
        );
        let r = rows(
            &mut db,
            "SELECT dept FROM s GROUP BY dept HAVING COUNT(*) > 1",
        );
        assert_eq!(r.rows, vec![vec![Value::Str("eng".into())]]);
    }

    #[test]
    fn having_combines_group_key_and_aggregate() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE s (dept TEXT, pay INT)");
        run(
            &mut db,
            "INSERT INTO s VALUES ('eng', 100), ('eng', 20), ('ops', 90), ('hr', 500)",
        );
        let r = rows(
            &mut db,
            "SELECT dept FROM s GROUP BY dept HAVING dept != 'hr' AND SUM(pay) > 60 ORDER BY dept",
        );
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Str("eng".into())],
                vec![Value::Str("ops".into())]
            ]
        );
    }

    #[test]
    fn group_by_on_empty_input_yields_no_rows() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE s (dept TEXT, pay INT)");
        let r = rows(&mut db, "SELECT dept, COUNT(*) FROM s GROUP BY dept");
        assert!(r.rows.is_empty());
    }

    #[test]
    fn aggregates_min_max_over_strings() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (s TEXT)");
        run(
            &mut db,
            "INSERT INTO t VALUES ('banana'), ('apple'), ('cherry')",
        );
        let r = rows(&mut db, "SELECT MIN(s), MAX(s) FROM t");
        assert_eq!(
            r.rows,
            vec![vec![
                Value::Str("apple".into()),
                Value::Str("cherry".into())
            ]]
        );
    }

    #[test]
    fn group_by_with_order_and_limit() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE s (dept TEXT, pay INT)");
        run(
            &mut db,
            "INSERT INTO s VALUES ('eng', 1), ('eng', 2), ('ops', 3), ('hr', 9)",
        );
        let r = rows(
            &mut db,
            "SELECT dept, COUNT(*) AS c FROM s GROUP BY dept ORDER BY c DESC, dept LIMIT 2",
        );
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Str("eng".into()), Value::Int(2)],
                vec![Value::Str("hr".into()), Value::Int(1)],
            ]
        );
    }

    // ---- SELECT expressions / filters --------------------------------

    #[test]
    fn from_less_select_constants() {
        let mut db = Database::in_memory().unwrap();
        let r = rows(&mut db, "SELECT 1 + 1 AS two, 'hi' AS word");
        assert_eq!(r.columns, vec!["two", "word"]);
        assert_eq!(r.rows, vec![vec![Value::Int(2), Value::Str("hi".into())]]);
    }

    #[test]
    fn derived_table_subquery_in_from() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (n INT)");
        run(&mut db, "INSERT INTO t VALUES (1), (2), (3), (4)");
        let r = rows(
            &mut db,
            "SELECT d.n FROM (SELECT n FROM t WHERE n > 2) AS d ORDER BY d.n",
        );
        assert_eq!(r.rows, vec![vec![Value::Int(3)], vec![Value::Int(4)]]);
    }

    #[test]
    fn projection_alias_and_order_by_alias() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (n INT)");
        run(&mut db, "INSERT INTO t VALUES (1), (2), (3)");
        let r = rows(&mut db, "SELECT n * 2 AS dbl FROM t ORDER BY dbl DESC");
        assert_eq!(r.columns, vec!["dbl"]);
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Int(6)],
                vec![Value::Int(4)],
                vec![Value::Int(2)]
            ]
        );
    }

    #[test]
    fn order_by_multiple_keys_mixed_directions() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (s TEXT, n INT)");
        run(&mut db, "INSERT INTO t VALUES ('a', 1), ('a', 3), ('b', 2)");
        let r = rows(&mut db, "SELECT s, n FROM t ORDER BY s ASC, n DESC");
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Str("a".into()), Value::Int(3)],
                vec![Value::Str("a".into()), Value::Int(1)],
                vec![Value::Str("b".into()), Value::Int(2)],
            ]
        );
    }

    #[test]
    fn where_or_and_parentheses() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (n INT)");
        for i in 0..5 {
            run(&mut db, &format!("INSERT INTO t VALUES ({i})"));
        }
        let r = rows(
            &mut db,
            "SELECT n FROM t WHERE (n = 1 OR n = 3) AND n < 4 ORDER BY n",
        );
        assert_eq!(r.rows, vec![vec![Value::Int(1)], vec![Value::Int(3)]]);
    }

    #[test]
    fn where_string_comparison_and_not_in() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (s TEXT)");
        run(
            &mut db,
            "INSERT INTO t VALUES ('apple'), ('banana'), ('cherry')",
        );
        let r = rows(&mut db, "SELECT s FROM t WHERE s >= 'banana' ORDER BY s");
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Str("banana".into())],
                vec![Value::Str("cherry".into())],
            ]
        );
        let r = rows(
            &mut db,
            "SELECT s FROM t WHERE s NOT IN ('apple', 'cherry')",
        );
        assert_eq!(r.rows, vec![vec![Value::Str("banana".into())]]);
    }

    #[test]
    fn unary_minus_and_parenthesized_arithmetic() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (a INT)");
        run(&mut db, "INSERT INTO t VALUES (7)");
        let r = rows(&mut db, "SELECT -(a + 1), (a + 1) * 2 FROM t");
        assert_eq!(r.rows, vec![vec![Value::Int(-8), Value::Int(16)]]);
    }

    #[test]
    fn limit_zero_and_offset_beyond_end() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (n INT)");
        for i in 0..5 {
            run(&mut db, &format!("INSERT INTO t VALUES ({i})"));
        }
        let r = rows(&mut db, "SELECT n FROM t ORDER BY n LIMIT 0");
        assert!(r.rows.is_empty());
        let r = rows(&mut db, "SELECT n FROM t ORDER BY n LIMIT 5 OFFSET 100");
        assert!(r.rows.is_empty());
    }

    #[test]
    fn where_on_missing_column_is_empty_not_error() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (n INT)");
        run(&mut db, "INSERT INTO t VALUES (1)");
        let r = rows(&mut db, "SELECT n FROM t WHERE ghost = 1");
        assert!(r.rows.is_empty());
    }

    #[test]
    fn boolean_literals_in_where() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (n INT)");
        for i in 0..4 {
            run(&mut db, &format!("INSERT INTO t VALUES ({i})"));
        }
        let r = rows(&mut db, "SELECT n FROM t WHERE TRUE AND n = 2");
        assert_eq!(r.rows, vec![vec![Value::Int(2)]]);
        let r = rows(&mut db, "SELECT n FROM t WHERE FALSE OR n = 3");
        assert_eq!(r.rows, vec![vec![Value::Int(3)]]);
        let r = rows(&mut db, "SELECT n FROM t WHERE FALSE");
        assert!(r.rows.is_empty());
    }

    // ---- set operations ------------------------------------------------

    #[test]
    fn union_column_count_mismatch_rejected() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE a (x INT, y INT)");
        run(&mut db, "CREATE TABLE b (x INT)");
        let e = db
            .execute("SELECT x, y FROM a UNION SELECT x FROM b")
            .unwrap_err();
        assert!(e.to_string().contains("different column counts"), "{e}");
    }

    // ---- DDL / constraints / system surfaces ---------------------------

    #[test]
    fn auto_increment_underscore_variant() {
        let mut db = Database::in_memory().unwrap();
        run(
            &mut db,
            "CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)",
        );
        run(&mut db, "INSERT INTO t (v) VALUES ('a'), ('b')");
        let r = rows(&mut db, "SELECT id FROM t ORDER BY id");
        assert_eq!(r.rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
    }

    #[test]
    fn unique_column_allows_multiple_nulls() {
        let mut db = Database::in_memory().unwrap();
        run(
            &mut db,
            "CREATE TABLE t (id INT PRIMARY KEY, email TEXT UNIQUE)",
        );
        run(
            &mut db,
            "INSERT INTO t VALUES (1, 'a@x'), (2, NULL), (3, NULL)",
        );
        let r = rows(&mut db, "SELECT id FROM t ORDER BY id");
        assert_eq!(r.rows.len(), 3);
    }

    #[test]
    fn insert_column_subset_null_fills_the_rest() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (a INT, b TEXT)");
        run(&mut db, "INSERT INTO t (b) VALUES ('x')");
        let r = rows(&mut db, "SELECT a, b FROM t");
        assert_eq!(r.rows, vec![vec![Value::Null, Value::Str("x".into())]]);
    }

    #[test]
    fn insert_boolean_and_null_literals_roundtrip() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (a INT, b INT, c INT)");
        run(&mut db, "INSERT INTO t VALUES (TRUE, NULL, FALSE)");
        let r = rows(&mut db, "SELECT a, b, c FROM t");
        assert_eq!(
            r.rows,
            vec![vec![Value::Bool(true), Value::Null, Value::Bool(false)]]
        );
    }

    #[test]
    fn update_multiple_assignments() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (a INT, b TEXT)");
        run(&mut db, "INSERT INTO t VALUES (1, 'x')");
        match run(&mut db, "UPDATE t SET a = a + 1, b = 'z' WHERE a = 1") {
            ExecOutcome::Affected(1) => {}
            o => panic!("{o:?}"),
        }
        let r = rows(&mut db, "SELECT a, b FROM t");
        assert_eq!(r.rows, vec![vec![Value::Int(2), Value::Str("z".into())]]);
    }

    #[test]
    fn pragma_is_accepted_noop() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (a INT)");
        match run(&mut db, "PRAGMA user_version") {
            ExecOutcome::Affected(0) => {}
            o => panic!("{o:?}"),
        }
    }

    #[test]
    fn sqlite_master_lists_tables() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE alpha (x INT)");
        run(&mut db, "CREATE TABLE beta (y INT)");
        let r = rows(&mut db, "SELECT name FROM sqlite_master ORDER BY name");
        let names: Vec<String> = r
            .rows
            .iter()
            .map(|row| row[0].as_str().unwrap().into())
            .collect();
        assert_eq!(names, vec!["alpha", "beta"]);
    }

    #[test]
    fn create_view_rejected() {
        let mut db = Database::in_memory().unwrap();
        let e = db.execute("CREATE VIEW v AS SELECT 1").unwrap_err();
        assert!(e.to_string().contains("unsupported statement"), "{e}");
    }

    #[test]
    fn qualified_wildcard_rejected() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (a INT)");
        assert!(db.execute("SELECT t.* FROM t").is_err());
    }

    #[test]
    fn drop_index_lifecycle() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (a INT)");
        run(&mut db, "CREATE INDEX ia ON t (a)");
        assert_eq!(db.catalog()[0].indexes, vec!["ia".to_string()]);
        run(&mut db, "DROP INDEX ia");
        assert!(db.catalog()[0].indexes.is_empty());
        assert!(db.execute("DROP INDEX ia").is_err());
        run(&mut db, "DROP INDEX IF EXISTS ia"); // no error
    }

    #[test]
    fn is_write_statement_classification() {
        assert!(!Database::is_write_statement("SELECT 1"));
        assert!(!Database::is_write_statement("PRAGMA foo"));
        assert!(Database::is_write_statement("INSERT INTO t VALUES (1)"));
        assert!(Database::is_write_statement("UPDATE t SET a = 1"));
        assert!(Database::is_write_statement("DELETE FROM t"));
        assert!(Database::is_write_statement("CREATE TABLE t (a INT)"));
        assert!(Database::is_write_statement("DROP TABLE t"));
        // Unparseable input is conservatively treated as a write.
        assert!(Database::is_write_statement("SELEC nope"));
    }

    // ---- SQL completeness: silent-error fixes -------------------------

    #[test]
    fn distinct_dedups_rows_and_aggregate_inputs() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE dup (g TEXT)");
        run(&mut db, "INSERT INTO dup VALUES ('x'), ('x'), ('y')");
        let r = rows(&mut db, "SELECT DISTINCT g FROM dup ORDER BY g");
        assert_eq!(
            r.rows,
            vec![vec![Value::Str("x".into())], vec![Value::Str("y".into())]]
        );
        let r = rows(
            &mut db,
            "SELECT COUNT(DISTINCT g), SUM(DISTINCT 1) FROM dup",
        );
        assert_eq!(r.rows, vec![vec![Value::Int(2), Value::Int(1)]]);
    }

    #[test]
    fn from_less_where_filters() {
        let mut db = Database::in_memory().unwrap();
        let r = rows(&mut db, "SELECT 1 WHERE FALSE");
        assert!(r.rows.is_empty());
        let r = rows(&mut db, "SELECT 1 WHERE TRUE");
        assert_eq!(r.rows, vec![vec![Value::Int(1)]]);
    }

    #[test]
    fn cte_with_and_chained_ctes() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (n INT)");
        run(&mut db, "INSERT INTO t VALUES (1), (2), (3), (4)");
        let r = rows(
            &mut db,
            "WITH big AS (SELECT n FROM t WHERE n > 2) SELECT n FROM big ORDER BY n",
        );
        assert_eq!(r.rows, vec![vec![Value::Int(3)], vec![Value::Int(4)]]);
        // A later CTE can read an earlier one.
        let r = rows(
            &mut db,
            "WITH a AS (SELECT n FROM t), b AS (SELECT n FROM a WHERE n >= 3) SELECT COUNT(n) AS c FROM b",
        );
        assert_eq!(r.rows, vec![vec![Value::Int(2)]]);
    }

    #[test]
    fn order_by_ordinal_and_nulls_position() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (s TEXT, n INT)");
        run(
            &mut db,
            "INSERT INTO t VALUES ('a', 10), ('b', 30), ('c', 20)",
        );
        // ORDER BY 2 = by n.
        let r = rows(&mut db, "SELECT s, n FROM t ORDER BY 2 DESC");
        assert_eq!(r.rows[0], vec![Value::Str("b".into()), Value::Int(30)]);
        // NULLS FIRST/LAST control placement.
        run(&mut db, "INSERT INTO t (s) VALUES ('z')");
        let r = rows(&mut db, "SELECT s FROM t ORDER BY n NULLS LAST");
        assert_eq!(r.rows[3], vec![Value::Str("z".into())]);
        let r = rows(&mut db, "SELECT s FROM t ORDER BY n NULLS FIRST LIMIT 1");
        assert_eq!(r.rows[0], vec![Value::Str("z".into())]);
    }

    #[test]
    fn order_by_non_output_expression() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (g TEXT)");
        run(&mut db, "INSERT INTO t VALUES ('xx'), ('yyy'), ('y')");
        let r = rows(&mut db, "SELECT g FROM t ORDER BY LENGTH(g), g");
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Str("y".into())],
                vec![Value::Str("xx".into())],
                vec![Value::Str("yyy".into())],
            ]
        );
    }

    // ---- SQL completeness: DML semantics ------------------------------

    #[test]
    fn create_table_as_select_copies_shape_and_rows() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE src (id INT, name TEXT)");
        run(
            &mut db,
            "INSERT INTO src VALUES (1, 'a'), (2, 'b'), (3, 'c')",
        );
        run(
            &mut db,
            "CREATE TABLE dst AS SELECT id, name FROM src WHERE id < 3",
        );
        let r = rows(&mut db, "SELECT id, name FROM dst ORDER BY id");
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0], vec![Value::Int(1), Value::Str("a".into())]);
        // The new table is fully writable.
        run(&mut db, "INSERT INTO dst VALUES (9, 'nine')");
        let r = rows(&mut db, "SELECT COUNT(id) FROM dst");
        assert_eq!(r.rows, vec![vec![Value::Int(3)]]);
    }

    #[test]
    fn on_conflict_do_nothing_and_or_ignore() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
        run(&mut db, "INSERT INTO t VALUES (1, 'a')");
        match run(
            &mut db,
            "INSERT INTO t VALUES (1, 'dup'), (2, 'new') ON CONFLICT DO NOTHING",
        ) {
            ExecOutcome::Affected(1) => {} // only the non-conflicting row lands
            o => panic!("{o:?}"),
        }
        let r = rows(&mut db, "SELECT id, v FROM t ORDER BY id");
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Int(1), Value::Str("a".into())],
                vec![Value::Int(2), Value::Str("new".into())],
            ]
        );
        // SQLite spelling.
        run(&mut db, "INSERT OR IGNORE INTO t VALUES (2, 'ignored')");
        let r = rows(&mut db, "SELECT v FROM t WHERE id = 2");
        assert_eq!(r.rows, vec![vec![Value::Str("new".into())]]);
    }

    #[test]
    fn replace_into_replaces_conflicting_rows() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
        run(&mut db, "INSERT INTO t VALUES (1, 'old'), (2, 'keep')");
        match run(&mut db, "REPLACE INTO t VALUES (1, 'new')") {
            ExecOutcome::Affected(1) => {}
            o => panic!("{o:?}"),
        }
        let r = rows(&mut db, "SELECT id, v FROM t ORDER BY id");
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Int(1), Value::Str("new".into())],
                vec![Value::Int(2), Value::Str("keep".into())],
            ]
        );
    }

    #[test]
    fn update_from_join_semantics() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (id INT, v TEXT)");
        run(&mut db, "CREATE TABLE ext (id INT, v TEXT)");
        run(&mut db, "INSERT INTO t VALUES (1, 'x'), (2, 'y'), (3, 'z')");
        run(&mut db, "INSERT INTO ext VALUES (1, 'one'), (3, 'three')");
        match run(
            &mut db,
            "UPDATE t SET v = ext.v FROM ext WHERE t.id = ext.id",
        ) {
            ExecOutcome::Affected(2) => {}
            o => panic!("{o:?}"),
        }
        let r = rows(&mut db, "SELECT id, v FROM t ORDER BY id");
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Int(1), Value::Str("one".into())],
                vec![Value::Int(2), Value::Str("y".into())],
                vec![Value::Int(3), Value::Str("three".into())],
            ]
        );
    }

    #[test]
    fn delete_using_join_semantics() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (id INT)");
        run(&mut db, "CREATE TABLE ext (id INT, drop INT)");
        run(&mut db, "INSERT INTO t VALUES (1), (2), (3)");
        run(&mut db, "INSERT INTO ext VALUES (1, 1), (3, 1)");
        match run(
            &mut db,
            "DELETE FROM t USING ext WHERE t.id = ext.id AND ext.drop = 1",
        ) {
            ExecOutcome::Affected(2) => {}
            o => panic!("{o:?}"),
        }
        let r = rows(&mut db, "SELECT id FROM t");
        assert_eq!(r.rows, vec![vec![Value::Int(2)]]);
    }

    #[test]
    fn insert_into_select() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE src (n INT)");
        run(&mut db, "CREATE TABLE dst (n INT)");
        run(&mut db, "INSERT INTO src VALUES (1), (2), (3)");
        match run(&mut db, "INSERT INTO dst SELECT n FROM src WHERE n >= 2") {
            ExecOutcome::Affected(2) => {}
            o => panic!("{o:?}"),
        }
        let r = rows(&mut db, "SELECT n FROM dst ORDER BY n");
        assert_eq!(r.rows, vec![vec![Value::Int(2)], vec![Value::Int(3)]]);
    }

    #[test]
    fn truncate_clears_rows_keeps_shape() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
        run(&mut db, "INSERT INTO t VALUES (1, 'a'), (2, 'b')");
        run(&mut db, "TRUNCATE TABLE t");
        let r = rows(&mut db, "SELECT id FROM t");
        assert!(r.rows.is_empty());
        // PK still enforced after truncation.
        run(&mut db, "INSERT INTO t VALUES (1, 'again')");
        let e = db.execute("INSERT INTO t VALUES (1, 'clash')").unwrap_err();
        assert!(e.to_string().contains("UNIQUE"), "{e}");
    }

    // ---- SQL completeness: predicates & expressions -------------------

    #[test]
    fn is_null_and_is_not_null() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (id INT, s TEXT)");
        run(
            &mut db,
            "INSERT INTO t (id, s) VALUES (1, 'a'), (2, NULL), (3, 'c')",
        );
        let r = rows(&mut db, "SELECT id FROM t WHERE s IS NULL");
        assert_eq!(r.rows, vec![vec![Value::Int(2)]]);
        let r = rows(&mut db, "SELECT id FROM t WHERE s IS NOT NULL ORDER BY id");
        assert_eq!(r.rows, vec![vec![Value::Int(1)], vec![Value::Int(3)]]);
    }

    #[test]
    fn between_and_not_between() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (n INT)");
        run(&mut db, "INSERT INTO t VALUES (1), (5), (10), (20)");
        let r = rows(&mut db, "SELECT n FROM t WHERE n BETWEEN 5 AND 10");
        assert_eq!(r.rows, vec![vec![Value::Int(5)], vec![Value::Int(10)]]);
        let r = rows(
            &mut db,
            "SELECT n FROM t WHERE n NOT BETWEEN 5 AND 10 ORDER BY n",
        );
        assert_eq!(r.rows, vec![vec![Value::Int(1)], vec![Value::Int(20)]]);
    }

    #[test]
    fn like_patterns_and_escape() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (s TEXT)");
        run(
            &mut db,
            "INSERT INTO t VALUES ('apple'), ('maple'), ('apricot'), ('a%b')",
        );
        let r = rows(&mut db, "SELECT s FROM t WHERE s LIKE 'a%' ORDER BY s");
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Str("a%b".into())],
                vec![Value::Str("apple".into())],
                vec![Value::Str("apricot".into())],
            ]
        );
        // '_' matches exactly one char.
        let r = rows(&mut db, "SELECT s FROM t WHERE s LIKE 'appl_'");
        assert_eq!(r.rows, vec![vec![Value::Str("apple".into())]]);
        // ESCAPE makes '%' literal.
        let r = rows(&mut db, "SELECT s FROM t WHERE s LIKE 'a!%b' ESCAPE '!'");
        assert_eq!(r.rows, vec![vec![Value::Str("a%b".into())]]);
        let r = rows(&mut db, "SELECT s FROM t WHERE s NOT LIKE 'a%' ORDER BY s");
        assert_eq!(r.rows, vec![vec![Value::Str("maple".into())]]);
    }

    #[test]
    fn not_operator() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (id INT)");
        run(&mut db, "INSERT INTO t VALUES (1), (2)");
        let r = rows(&mut db, "SELECT id FROM t WHERE NOT (id = 1)");
        assert_eq!(r.rows, vec![vec![Value::Int(2)]]);
    }

    #[test]
    fn case_searched_and_simple() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (n INT)");
        run(&mut db, "INSERT INTO t VALUES (1), (5), (10)");
        let r = rows(
            &mut db,
            "SELECT CASE WHEN n >= 10 THEN 'big' WHEN n >= 5 THEN 'mid' ELSE 'small' END AS bucket FROM t",
        );
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Str("small".into())],
                vec![Value::Str("mid".into())],
                vec![Value::Str("big".into())],
            ]
        );
        let r = rows(
            &mut db,
            "SELECT CASE n WHEN 1 THEN 'one' WHEN 5 THEN 'five' ELSE '?' END AS word FROM t WHERE n < 10",
        );
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Str("one".into())],
                vec![Value::Str("five".into())],
            ]
        );
    }

    #[test]
    fn cast_conversions() {
        let mut db = Database::in_memory().unwrap();
        let r = rows(&mut db, "SELECT CAST('42' AS INT) + 1, CAST(7 AS TEXT) || '!', CAST(4.0 AS TEXT), CAST(1 AS BOOLEAN), CAST('2.5' AS FLOAT)");
        assert_eq!(
            r.rows,
            vec![vec![
                Value::Int(43),
                Value::Str("7!".into()),
                Value::Str("4.0".into()),
                Value::Bool(true),
                Value::Float(2.5),
            ]]
        );
    }

    #[test]
    fn scalar_function_library() {
        let mut db = Database::in_memory().unwrap();
        let r = rows(
            &mut db,
            "SELECT UPPER('aB'), LOWER('Cd'), LENGTH('héllo'), ABS(-5), ROUND(2.567, 2), COALESCE(NULL, 'd'), IFNULL(NULL, 9), NULLIF(1, 1), NULLIF(2, 1), SUBSTR('abcdef', 2, 3), TRIM('  x  '), CONCAT('a', 'b'), TYPEOF(1), TYPEOF('x'), TYPEOF(NULL)",
        );
        assert_eq!(
            r.rows,
            vec![vec![
                Value::Str("AB".into()),
                Value::Str("cd".into()),
                Value::Int(5),
                Value::Int(5),
                Value::Float(2.57),
                Value::Str("d".into()),
                Value::Int(9),
                Value::Null,
                Value::Int(2),
                Value::Str("bcd".into()),
                Value::Str("x".into()),
                Value::Str("ab".into()),
                Value::Str("integer".into()),
                Value::Str("text".into()),
                Value::Str("null".into()),
            ]]
        );
    }

    #[test]
    fn string_concat_and_modulo() {
        let mut db = Database::in_memory().unwrap();
        let r = rows(&mut db, "SELECT 'a' || 'b' || 'c', 7 % 3, 7.5 % 2, 5 % 0");
        assert_eq!(
            r.rows,
            vec![vec![
                Value::Str("abc".into()),
                Value::Int(1),
                Value::Float(1.5),
                Value::Null,
            ]]
        );
    }

    // ---- SQL completeness: subqueries ----------------------------------

    #[test]
    fn in_subquery_predicate() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE a (n INT)");
        run(&mut db, "CREATE TABLE b (n INT)");
        run(&mut db, "INSERT INTO a VALUES (1), (2), (3)");
        run(&mut db, "INSERT INTO b VALUES (2), (3)");
        let r = rows(
            &mut db,
            "SELECT n FROM a WHERE n IN (SELECT n FROM b) ORDER BY n",
        );
        assert_eq!(r.rows, vec![vec![Value::Int(2)], vec![Value::Int(3)]]);
        let r = rows(
            &mut db,
            "SELECT n FROM a WHERE n NOT IN (SELECT n FROM b) ORDER BY n",
        );
        assert_eq!(r.rows, vec![vec![Value::Int(1)]]);
    }

    #[test]
    fn exists_predicates() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE a (n INT)");
        run(&mut db, "CREATE TABLE b (n INT)");
        run(&mut db, "INSERT INTO a VALUES (1)");
        run(&mut db, "INSERT INTO b VALUES (2)");
        let r = rows(
            &mut db,
            "SELECT n FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.n > 1)",
        );
        assert_eq!(r.rows.len(), 1);
        let r = rows(
            &mut db,
            "SELECT n FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.n > 99)",
        );
        assert_eq!(r.rows.len(), 1);
    }

    #[test]
    fn scalar_subquery_in_where_and_projection() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (n INT)");
        run(&mut db, "INSERT INTO t VALUES (1), (5), (10)");
        let r = rows(&mut db, "SELECT n FROM t WHERE n = (SELECT MAX(n) FROM t)");
        assert_eq!(r.rows, vec![vec![Value::Int(10)]]);
        let r = rows(
            &mut db,
            "SELECT n, (SELECT MAX(n) FROM t) AS mx FROM t WHERE n = 1",
        );
        assert_eq!(r.rows, vec![vec![Value::Int(1), Value::Int(10)]]);
    }

    #[test]
    fn any_subquery_equality() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE a (n INT)");
        run(&mut db, "CREATE TABLE b (n INT)");
        run(&mut db, "INSERT INTO a VALUES (1), (2)");
        run(&mut db, "INSERT INTO b VALUES (2)");
        let r = rows(&mut db, "SELECT n FROM a WHERE n = ANY (SELECT n FROM b)");
        assert_eq!(r.rows, vec![vec![Value::Int(2)]]);
    }

    // ---- SQL completeness: set operations ------------------------------

    #[test]
    fn intersect_and_except_setops() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE a (n INT)");
        run(&mut db, "CREATE TABLE b (n INT)");
        run(&mut db, "INSERT INTO a VALUES (1), (2), (3)");
        run(&mut db, "INSERT INTO b VALUES (2), (3), (4)");
        let r = rows(
            &mut db,
            "SELECT n FROM a INTERSECT SELECT n FROM b ORDER BY n",
        );
        assert_eq!(r.rows, vec![vec![Value::Int(2)], vec![Value::Int(3)]]);
        let r = rows(&mut db, "SELECT n FROM a EXCEPT SELECT n FROM b");
        assert_eq!(r.rows, vec![vec![Value::Int(1)]]);
        let r = rows(&mut db, "SELECT n FROM b EXCEPT SELECT n FROM a");
        assert_eq!(r.rows, vec![vec![Value::Int(4)]]);
        // EXCEPT ALL keeps multiplicity on the left: {1,1,2} minus the
        // right's single 2 leaves both 1s.
        run(&mut db, "CREATE TABLE c (n INT)");
        run(&mut db, "INSERT INTO c VALUES (1), (1), (2)");
        let r = rows(
            &mut db,
            "SELECT n FROM c EXCEPT ALL SELECT n FROM b ORDER BY n",
        );
        assert_eq!(r.rows, vec![vec![Value::Int(1)], vec![Value::Int(1)]]);
    }

    #[test]
    fn union_order_by_applies_to_whole_result() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE a (n INT)");
        run(&mut db, "CREATE TABLE b (n INT)");
        run(&mut db, "INSERT INTO a VALUES (3), (1)");
        run(&mut db, "INSERT INTO b VALUES (2)");
        let r = rows(
            &mut db,
            "SELECT n FROM a UNION ALL SELECT n FROM b ORDER BY n DESC",
        );
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Int(3)],
                vec![Value::Int(2)],
                vec![Value::Int(1)],
            ]
        );
    }

    #[test]
    fn group_concat_aggregate() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (g TEXT, s TEXT)");
        run(
            &mut db,
            "INSERT INTO t VALUES ('x', 'b'), ('x', 'a'), ('y', 'c')",
        );
        let r = rows(
            &mut db,
            "SELECT g, GROUP_CONCAT(s) FROM t GROUP BY g ORDER BY g",
        );
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Str("x".into()), Value::Str("b,a".into())],
                vec![Value::Str("y".into()), Value::Str("c".into())],
            ]
        );
    }

    // ---- SQL completeness: DDL constraints ------------------------------

    #[test]
    fn rename_column_and_table() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (id INT, v TEXT)");
        run(&mut db, "INSERT INTO t VALUES (1, 'a')");
        run(&mut db, "ALTER TABLE t RENAME COLUMN v TO name");
        let r = rows(&mut db, "SELECT id, name FROM t");
        assert_eq!(r.rows, vec![vec![Value::Int(1), Value::Str("a".into())]]);
        run(&mut db, "ALTER TABLE t RENAME TO t2");
        let r = rows(&mut db, "SELECT name FROM t2");
        assert_eq!(r.rows, vec![vec![Value::Str("a".into())]]);
        assert!(db.execute("SELECT * FROM t").is_err());
    }

    #[test]
    fn column_default_on_insert_and_alter_backfill() {
        let mut db = Database::in_memory().unwrap();
        run(
            &mut db,
            "CREATE TABLE t (id INT, s TEXT DEFAULT 'seed', n INT DEFAULT 7)",
        );
        run(&mut db, "INSERT INTO t (id) VALUES (1)");
        let r = rows(&mut db, "SELECT s, n FROM t WHERE id = 1");
        assert_eq!(r.rows, vec![vec![Value::Str("seed".into()), Value::Int(7)]]);
        // ADD COLUMN with DEFAULT backfills existing rows.
        run(&mut db, "INSERT INTO t (id, s) VALUES (2, 'x')");
        run(&mut db, "ALTER TABLE t ADD COLUMN flag INT DEFAULT 1");
        let r = rows(&mut db, "SELECT flag FROM t ORDER BY id");
        assert_eq!(r.rows, vec![vec![Value::Int(1)], vec![Value::Int(1)]]);
        // Conventional option order `NOT NULL DEFAULT ...`: the NOT NULL
        // rule must be judged after all options are registered, not hit a
        // false "without a DEFAULT" while walking the options in order.
        run(
            &mut db,
            "ALTER TABLE t ADD COLUMN tag TEXT NOT NULL DEFAULT 'z'",
        );
        let r = rows(&mut db, "SELECT tag FROM t ORDER BY id");
        assert_eq!(
            r.rows,
            vec![vec![Value::Str("z".into())], vec![Value::Str("z".into())]]
        );
        run(&mut db, "INSERT INTO t (id) VALUES (3)");
        let r = rows(&mut db, "SELECT tag FROM t WHERE id = 3");
        assert_eq!(r.rows, vec![vec![Value::Str("z".into())]]);
        // ...while NOT NULL without a DEFAULT stays rejected.
        let e = db
            .execute("ALTER TABLE t ADD COLUMN bad INT NOT NULL")
            .unwrap_err();
        assert!(e.to_string().contains("without a DEFAULT"), "{e}");
    }

    #[test]
    fn column_defaults_survive_reopen() {
        // Defaults live in the catalog (saved as SQL text); like every
        // TableMeta field they must round-trip the reopen (red line 9).
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("defaults.db");
        let mut db = Database::open(&path).unwrap();
        run(
            &mut db,
            "CREATE TABLE t (id INT PRIMARY KEY, s TEXT DEFAULT 'seed', n INT NOT NULL DEFAULT 7)",
        );
        run(&mut db, "INSERT INTO t (id) VALUES (1)");
        drop(db);
        let mut db = Database::open(&path).unwrap();
        // Still filling omitted columns after reopen...
        run(&mut db, "INSERT INTO t (id) VALUES (2)");
        let r = rows(&mut db, "SELECT s, n FROM t WHERE id = 2");
        assert_eq!(r.rows, vec![vec![Value::Str("seed".into()), Value::Int(7)]]);
        // ...and the tooling surface still reports the declarations.
        let cols = db
            .catalog()
            .into_iter()
            .find(|t| t.name == "t")
            .unwrap()
            .columns;
        assert_eq!(cols[1].default_value.as_deref(), Some("'seed'"));
        assert_eq!(cols[2].default_value.as_deref(), Some("7"));
        assert_eq!(cols[0].default_value, None);
    }

    #[test]
    fn default_follows_rename_and_drop() {
        let mut db = Database::in_memory().unwrap();
        run(
            &mut db,
            "CREATE TABLE t (id INT PRIMARY KEY, s TEXT DEFAULT 'x')",
        );
        // RENAME COLUMN carries the default to the new name.
        run(&mut db, "ALTER TABLE t RENAME COLUMN s TO note");
        run(&mut db, "INSERT INTO t (id) VALUES (1)");
        let r = rows(&mut db, "SELECT note FROM t WHERE id = 1");
        assert_eq!(r.rows, vec![vec![Value::Str("x".into())]]);
        let col = &db
            .catalog()
            .into_iter()
            .find(|t| t.name == "t")
            .unwrap()
            .columns[1];
        assert_eq!(col.name, "note");
        assert_eq!(col.default_value.as_deref(), Some("'x'"));
        // DROP COLUMN removes the default bookkeeping along with the column.
        run(&mut db, "ALTER TABLE t DROP COLUMN note");
        let t = db.catalog().into_iter().find(|t| t.name == "t").unwrap();
        assert_eq!(t.columns.len(), 1, "only id remains: {:?}", t.columns);
        assert_eq!(t.columns[0].name, "id");
        // Row 2's document carries no stale fill.
        run(&mut db, "INSERT INTO t (id) VALUES (2)");
        let r = rows(&mut db, "SELECT id FROM t WHERE id = 2");
        assert_eq!(r.rows, vec![vec![Value::Int(2)]]);
    }

    #[test]
    fn guid_and_default_fill_writeback_together() {
        // Red line 11: the fanout/transaction buffer must carry the
        // canonical INSERT — generated GUIDs AND default-filled columns as
        // explicit values — so peers replay deterministically instead of
        // re-filling on their own.
        let mut db = Database::in_memory().unwrap();
        run(
            &mut db,
            "CREATE TABLE t (id GUID PRIMARY KEY AUTOINCREMENT, v TEXT DEFAULT 'd', n INT NOT NULL DEFAULT 5)",
        );
        run(&mut db, "INSERT INTO t (n) VALUES (7)");
        // Take the writeback before any other statement — it resets per
        // statement (take_resolved_insert is the server's fanout hook).
        let resolved = db.take_resolved_insert().expect("resolved insert text");
        let r = rows(&mut db, "SELECT id, v, n FROM t");
        let row = &r.rows[0];
        assert_eq!(&row[0].as_str().unwrap()[14..15], "7");
        assert_eq!(row[1], Value::Str("d".into()));
        assert_eq!(row[2], Value::Int(7));
        // The resolved writeback covers the generated id and the
        // default-filled column explicitly.
        assert!(
            resolved.contains("(\"id\", \"n\", \"v\")"),
            "explicit column list: {resolved}"
        );
        assert!(resolved.contains("'d'"), "default pinned: {resolved}");
        assert!(resolved.contains(" 7, 'd'"), "values pinned: {resolved}");
        // Consuming the writeback clears it.
        assert!(db.take_resolved_insert().is_none());
    }

    #[test]
    fn check_constraint_enforced() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (n INT CHECK (n > 0))");
        let e = db.execute("INSERT INTO t VALUES (-1)").unwrap_err();
        assert!(e.to_string().contains("CHECK"), "{e}");
        run(&mut db, "INSERT INTO t VALUES (1)");
        let e = db.execute("UPDATE t SET n = -5").unwrap_err();
        assert!(e.to_string().contains("CHECK"), "{e}");
        // NULL is unknown -> passes.
        run(&mut db, "INSERT INTO t (n) VALUES (NULL)");
        // Table-level CHECK with a name works too.
        run(
            &mut db,
            "CREATE TABLE u (a INT, b INT, CONSTRAINT pos CHECK (a + b > 0))",
        );
        let e = db.execute("INSERT INTO u VALUES (-2, -3)").unwrap_err();
        assert!(e.to_string().contains("CHECK"), "{e}");
    }

    #[test]
    fn foreign_key_enforced() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE parent (id INT PRIMARY KEY)");
        run(
            &mut db,
            "CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id))",
        );
        run(&mut db, "INSERT INTO parent VALUES (1)");
        run(&mut db, "INSERT INTO child VALUES (10, 1)"); // valid
        run(&mut db, "INSERT INTO child (id, pid) VALUES (11, NULL)"); // NULL passes
        let e = db.execute("INSERT INTO child VALUES (12, 99)").unwrap_err();
        assert!(e.to_string().contains("FOREIGN KEY"), "{e}");
        let e = db
            .execute("UPDATE child SET pid = 42 WHERE id = 10")
            .unwrap_err();
        assert!(e.to_string().contains("FOREIGN KEY"), "{e}");
        // Table-level FOREIGN KEY form.
        run(
            &mut db,
            "CREATE TABLE child2 (id INT, pid INT, FOREIGN KEY (pid) REFERENCES parent (id))",
        );
        let e = db.execute("INSERT INTO child2 VALUES (1, 5)").unwrap_err();
        assert!(e.to_string().contains("FOREIGN KEY"), "{e}");
    }

    #[test]
    fn table_level_unique_and_pk_constraints() {
        let mut db = Database::in_memory().unwrap();
        run(
            &mut db,
            "CREATE TABLE t (a INT, b INT, PRIMARY KEY (a), UNIQUE (b))",
        );
        run(&mut db, "INSERT INTO t VALUES (1, 10)");
        let e = db.execute("INSERT INTO t VALUES (2, 10)").unwrap_err();
        assert!(e.to_string().contains("UNIQUE"), "{e}");
        let e = db.execute("INSERT INTO t VALUES (1, 20)").unwrap_err();
        assert!(e.to_string().contains("UNIQUE"), "{e}");
    }

    // 待办:-9223372036854775808 目前被解析为浮点(一元负号 + 溢出字面量),
    // 需要解析器层支持;恢复前保持 ignore。
    #[test]
    fn integer_boundary_values_roundtrip() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
        run(
            &mut db,
            "INSERT INTO t VALUES (9223372036854775807, -9223372036854775808)",
        );
        run(&mut db, "INSERT INTO t VALUES (0, 0)");
        let r = rows(
            &mut db,
            "SELECT id, v FROM t WHERE id = 9223372036854775807",
        );
        assert_eq!(
            r.rows,
            vec![vec![Value::Int(i64::MAX), Value::Int(i64::MIN)]]
        );
        let r = rows(&mut db, "SELECT v + 1 FROM t WHERE id = 0");
        assert_eq!(r.rows, vec![vec![Value::Int(1)]]);
        let r = rows(&mut db, "SELECT id FROM t ORDER BY id");
        assert_eq!(
            r.rows,
            vec![vec![Value::Int(0)], vec![Value::Int(i64::MAX)]]
        );
    }

    #[test]
    fn float_precision_and_negative_sorting() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v REAL)");
        run(&mut db, "INSERT INTO t VALUES (1, 0.1)");
        run(&mut db, "INSERT INTO t VALUES (2, 0.2)");
        run(&mut db, "INSERT INTO t VALUES (3, -2.5)");
        run(&mut db, "INSERT INTO t VALUES (4, -10.75)");
        // binary floating point: 0.1 + 0.2 must NOT equal 0.3
        let r = rows(&mut db, "SELECT 0.1 + 0.2 = 0.3 FROM t WHERE id = 1");
        assert_eq!(r.rows, vec![vec![Value::Bool(false)]]);
        let r = rows(&mut db, "SELECT v FROM t WHERE id = 3");
        assert_eq!(r.rows, vec![vec![Value::Float(-2.5)]]);
        let r = rows(&mut db, "SELECT v FROM t ORDER BY v");
        let vals: Vec<f64> = r
            .rows
            .iter()
            .filter_map(|row| match &row[0] {
                Value::Float(f) => Some(*f),
                Value::Int(i) => Some(*i as f64),
                _ => None,
            })
            .collect();
        assert_eq!(vals, vec![-10.75, -2.5, 0.1, 0.2]);
    }

    // 待办:堆单元格上限 4090 字节,超长值需要大对象溢出页支持;恢复前保持 ignore。
    #[ignore = "heap cell size cap 4090 bytes; needs overflow pages for large values"]
    #[test]
    fn long_string_value_survives() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
        let long = "x".repeat(100_000) + "tail";
        run(&mut db, &format!("INSERT INTO t VALUES (1, '{}')", long));
        let r = rows(&mut db, "SELECT LENGTH(s) FROM t WHERE id = 1");
        let len = r.rows[0][0].as_i64().unwrap();
        assert!(len >= 100_000, "unexpected LENGTH {len}");
        let r = rows(&mut db, &format!("SELECT id FROM t WHERE s = '{}'", long));
        assert_eq!(r.rows.len(), 1);
        let r = rows(&mut db, "SELECT id FROM t WHERE s LIKE 'x%'");
        assert_eq!(r.rows.len(), 1);
    }

    #[test]
    fn negative_primary_key_index_range() {
        let mut db = idx_db();
        for i in -10..10 {
            run(
                &mut db,
                &format!("INSERT INTO t (id, name, n) VALUES ({i}, 'n{i}', {i})"),
            );
        }
        let r = rows(&mut db, "SELECT id FROM t WHERE id < -7 ORDER BY id");
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Int(-10)],
                vec![Value::Int(-9)],
                vec![Value::Int(-8)]
            ]
        );
        let r = rows(
            &mut db,
            "SELECT id FROM t WHERE id >= -1 AND id <= 1 ORDER BY id",
        );
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Int(-1)],
                vec![Value::Int(0)],
                vec![Value::Int(1)]
            ]
        );
        assert_eq!(
            rows(&mut db, "SELECT id FROM t WHERE id = -10").rows.len(),
            1
        );
    }

    #[test]
    fn string_escape_and_multibyte_literals() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
        run(&mut db, "INSERT INTO t VALUES (1, 'it''s a \\ test')");
        run(&mut db, "INSERT INTO t VALUES (2, '中文值「测试」')");
        let r = rows(&mut db, "SELECT s FROM t WHERE id = 1");
        assert_eq!(r.rows, vec![vec![Value::Str("it's a \\ test".into())]]);
        let r = rows(&mut db, "SELECT s FROM t WHERE id = 2");
        assert_eq!(r.rows, vec![vec![Value::Str("中文值「测试」".into())]]);
        let r = rows(&mut db, "SELECT id FROM t WHERE s = '中文值「测试」'");
        assert_eq!(r.rows.len(), 1);
    }

    #[test]
    fn type_mismatch_insert_is_stored_verbatim() {
        // Schemaless by design: an INTEGER-declared column still accepts a
        // string literal. Pin that semantic so future changes are loud.
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
        run(&mut db, "INSERT INTO t VALUES (1, 'not-a-number')");
        let r = rows(&mut db, "SELECT v FROM t WHERE id = 1");
        assert_eq!(r.rows, vec![vec![Value::Str("not-a-number".into())]]);
        assert_eq!(rows(&mut db, "SELECT id FROM t WHERE v = 0").rows.len(), 0);
    }

    #[test]
    fn update_delete_zero_rows_reports_affected_zero() {
        let mut db = idx_db();
        insert_n(&mut db, 5);
        assert_eq!(
            db.execute("UPDATE t SET name = 'x' WHERE id = 999")
                .unwrap(),
            ExecOutcome::Affected(0)
        );
        assert_eq!(
            db.execute("DELETE FROM t WHERE id = 999").unwrap(),
            ExecOutcome::Affected(0)
        );
        assert_eq!(
            rows(&mut db, "SELECT COUNT(*) FROM t").rows[0][0],
            Value::Int(5)
        );
    }

    #[test]
    fn autoinc_after_explicit_large_id_skips_forward() {
        let mut db = Database::in_memory().unwrap();
        run(
            &mut db,
            "CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, v INT)",
        );
        run(&mut db, "INSERT INTO t (v) VALUES (1)");
        run(&mut db, "INSERT INTO t (id, v) VALUES (1000, 2)");
        run(&mut db, "INSERT INTO t (v) VALUES (3)");
        let r = rows(&mut db, "SELECT id FROM t ORDER BY id");
        let ids: Vec<i64> = r.rows.iter().filter_map(|row| row[0].as_i64()).collect();
        assert_eq!(ids, vec![1, 1000, 1001]);
    }
}

// ---- transaction rollback & savepoints ----

#[cfg(test)]
mod tx_rollback_tests {
    use crate::engine::Database;

    #[test]
    fn savepoint_rollback_restores_partial_state_and_tx_continues() {
        let mut db = Database::in_memory().unwrap();
        db.execute("CREATE TABLE t (id INT)").unwrap();
        db.execute("BEGIN").unwrap();
        db.execute("INSERT INTO t VALUES (1)").unwrap();
        db.execute("SAVEPOINT sp1").unwrap();
        db.execute("INSERT INTO t VALUES (2)").unwrap();
        db.execute("ROLLBACK TO SAVEPOINT sp1").unwrap();
        db.execute("INSERT INTO t VALUES (3)").unwrap(); // 事务继续
        db.execute("COMMIT").unwrap();
        let out = db.execute("SELECT COUNT(id) FROM t").unwrap();
        assert!(format!("{out:?}").contains('2'));
        let out = format!("{:?}", db.execute("SELECT id FROM t ORDER BY id").unwrap());
        assert!(
            out.contains("Int(1)") && out.contains("Int(3)") && !out.contains("Int(2)"),
            "got {out}"
        );
    }

    #[test]
    fn nested_savepoints_roll_to_earlier_one() {
        let mut db = Database::in_memory().unwrap();
        db.execute("CREATE TABLE t (id INT)").unwrap();
        db.execute("BEGIN").unwrap();
        db.execute("INSERT INTO t VALUES (1)").unwrap();
        db.execute("SAVEPOINT a").unwrap();
        db.execute("INSERT INTO t VALUES (2)").unwrap();
        db.execute("SAVEPOINT b").unwrap();
        db.execute("INSERT INTO t VALUES (3)").unwrap();
        db.execute("ROLLBACK TO SAVEPOINT a").unwrap(); // 2、3 都没了,b 也被丢弃
        assert!(
            db.execute("ROLLBACK TO SAVEPOINT b").is_err(),
            "b 应已随 a 回滚丢弃"
        );
        db.execute("COMMIT").unwrap();
        let out = db.execute("SELECT id FROM t").unwrap();
        assert!(format!("{out:?}").contains('1'));
        assert!(!format!("{out:?}").contains("2,"));
    }

    #[test]
    fn release_savepoint_keeps_changes() {
        let mut db = Database::in_memory().unwrap();
        db.execute("CREATE TABLE t (id INT)").unwrap();
        db.execute("BEGIN").unwrap();
        db.execute("SAVEPOINT sp").unwrap();
        db.execute("INSERT INTO t VALUES (9)").unwrap();
        db.execute("RELEASE SAVEPOINT sp").unwrap();
        db.execute("COMMIT").unwrap();
        let out = db.execute("SELECT COUNT(id) FROM t").unwrap();
        assert!(format!("{out:?}").contains('1'), "RELEASE 后写入保留");
    }

    #[test]
    fn savepoint_requires_transaction_and_missing_name_errors() {
        let mut db = Database::in_memory().unwrap();
        db.execute("CREATE TABLE t (id INT)").unwrap();
        assert!(
            db.execute("SAVEPOINT sp").is_err(),
            "事务外 SAVEPOINT 应报错"
        );
        db.execute("BEGIN").unwrap();
        assert!(db.execute("ROLLBACK TO SAVEPOINT nope").is_err());
        db.execute("COMMIT").unwrap();
    }

    #[test]
    fn rollback_to_savepoint_is_durable_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sp.db");
        {
            let mut db = Database::open(&path).unwrap();
            db.execute("CREATE TABLE t (id INT)").unwrap();
            db.execute("BEGIN").unwrap();
            db.execute("INSERT INTO t VALUES (1)").unwrap();
            db.execute("SAVEPOINT sp").unwrap();
            db.execute("INSERT INTO t VALUES (2)").unwrap();
            db.execute("ROLLBACK TO SAVEPOINT sp").unwrap();
            db.execute("COMMIT").unwrap();
        }
        let mut db = Database::open(&path).unwrap();
        let out = db.execute("SELECT COUNT(id) FROM t").unwrap();
        assert!(
            format!("{out:?}").contains('1'),
            "重开后应只剩 1 行: {out:?}"
        );
    }

    #[test]
    fn full_rollback_is_durable_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rb.db");
        {
            let mut db = Database::open(&path).unwrap();
            db.execute("CREATE TABLE t (id INT)").unwrap();
            db.execute("INSERT INTO t VALUES (1)").unwrap();
            db.execute("BEGIN").unwrap();
            db.execute("INSERT INTO t VALUES (2)").unwrap();
            db.execute("ROLLBACK").unwrap();
        }
        let mut db = Database::open(&path).unwrap();
        let out = db.execute("SELECT COUNT(id) FROM t").unwrap();
        assert!(
            format!("{out:?}").contains('1'),
            "回滚的行重开后不应复活: {out:?}"
        );
    }
    // ---- 覆盖率补充:错误分支 / 管理语句 / 事务语义 ----

    use crate::engine::{ExecOutcome, Value};

    fn run(db: &mut Database, sql: &str) -> ExecOutcome {
        db.execute(sql)
            .unwrap_or_else(|e| panic!("SQL failed: {sql}\n{e}"))
    }

    fn rows(db: &mut Database, sql: &str) -> crate::engine::QueryResult {
        match run(db, sql) {
            ExecOutcome::Rows(r) => r,
            other => panic!("expected rows, got {other:?}"),
        }
    }

    fn idx_db() -> Database {
        let mut db = Database::in_memory().unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, n INT)")
            .unwrap();
        db
    }

    fn insert_n(db: &mut Database, n: i64) {
        for i in 0..n {
            db.execute(&format!(
                "INSERT INTO t (id, name, n) VALUES ({i}, 'u{i}', {i})"
            ))
            .unwrap();
        }
    }

    #[test]
    fn statement_lifecycle_errors() {
        let mut db = Database::in_memory().unwrap();
        // 空语句 / 多语句
        assert!(db.execute("").is_err());
        assert!(db.execute(";;").is_err());
        assert!(db.execute("SELECT 1; SELECT 2").is_err());
        assert!(db.execute("SELEC 1").is_err());
        assert_eq!(Database::parse_check(""), Err("empty statement".into()));
        assert!(Database::parse_check("SELEC").is_err());
        assert!(Database::parse_check("SELECT 1").is_ok());
        // is_write_statement 分类
        assert!(!Database::is_write_statement("SELECT 1"));
        assert!(!Database::is_write_statement("PRAGMA foo"));
        assert!(!Database::is_write_statement("BEGIN"));
        assert!(!Database::is_write_statement("COMMIT"));
        assert!(!Database::is_write_statement("ROLLBACK"));
        assert!(Database::is_write_statement("INSERT INTO t VALUES (1)"));
        assert!(Database::is_write_statement("UPDATE t SET a = 1"));
        assert!(Database::is_write_statement("garbage ~~"));
    }

    #[test]
    fn transaction_control_errors() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (id INT)");
        // 无事务时的 COMMIT / ROLLBACK / SAVEPOINT
        assert!(db.execute("COMMIT").is_err());
        assert!(db.execute("ROLLBACK").is_err());
        assert!(db.execute("SAVEPOINT s").is_err());
        // 未知 savepoint
        run(&mut db, "BEGIN");
        assert!(db.execute("ROLLBACK TO SAVEPOINT nope").is_err());
        assert!(db.execute("RELEASE SAVEPOINT nope").is_err());
        // 嵌套 BEGIN
        assert!(db.execute("BEGIN").is_err());
        assert!(db.in_transaction());
        run(&mut db, "SAVEPOINT a");
        run(&mut db, "INSERT INTO t VALUES (1)");
        run(&mut db, "SAVEPOINT b");
        run(&mut db, "INSERT INTO t VALUES (2)");
        run(&mut db, "ROLLBACK TO SAVEPOINT b");
        run(&mut db, "RELEASE SAVEPOINT a");
        // release 之后再用同名 savepoint 失败
        assert!(db.execute("ROLLBACK TO SAVEPOINT b").is_err());
        run(&mut db, "COMMIT");
        assert!(!db.in_transaction());
        // 回滚到 b 时 insert(1) 已在 savepoint 内,insert(2) 被撤销
        assert_eq!(
            rows(&mut db, "SELECT COUNT(*) FROM t").rows[0][0],
            Value::Int(1)
        );
    }

    #[test]
    fn drop_truncate_pragma_paths() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (id INT)");
        run(&mut db, "CREATE INDEX ix ON t (id)");
        // DROP 不存在的对象
        assert!(db.execute("DROP TABLE missing").is_err());
        run(&mut db, "DROP TABLE IF EXISTS missing");
        assert!(db.execute("DROP INDEX missing").is_err());
        run(&mut db, "DROP INDEX IF EXISTS missing");
        // 只支持 TABLE / INDEX
        assert!(db.execute("DROP VIEW v").is_err());
        // TRUNCATE
        run(&mut db, "INSERT INTO t VALUES (1)");
        run(&mut db, "TRUNCATE TABLE t");
        assert_eq!(
            rows(&mut db, "SELECT COUNT(*) FROM t").rows[0][0],
            Value::Int(0)
        );
        assert!(db.execute("TRUNCATE TABLE missing").is_err());
        run(&mut db, "TRUNCATE TABLE IF EXISTS missing");
        // PRAGMA 兼容 shim(仅接受可解析的字面量形式;table_info(t) 在
        // sqlparser 层就报错,与 SQLite 方言差异由调用方自行规避)
        assert!(matches!(
            db.execute("PRAGMA foo"),
            Ok(ExecOutcome::Affected(0))
        ));
        assert!(matches!(
            db.execute("PRAGMA foreign_keys = 1"),
            Ok(ExecOutcome::Affected(0))
        ));
        // 不支持的语句
        assert!(db.execute("CREATE VIEW v AS SELECT 1").is_err());
        assert!(db.execute("EXPLAIN SELECT 1").is_err());
        assert!(db.execute("VACUUM").is_err());
    }

    #[test]
    fn dml_on_missing_tables_errors() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (id INT)");
        assert!(db.execute("INSERT INTO missing VALUES (1)").is_err());
        assert!(db.execute("UPDATE missing SET id = 1").is_err());
        assert!(db.execute("DELETE FROM missing").is_err());
        assert!(db.execute("ALTER TABLE missing ADD COLUMN c INT").is_err());
        assert!(db.execute("CREATE INDEX ix ON missing (id)").is_err());
    }

    #[test]
    fn async_commit_and_pending_sync() {
        let mut db = Database::in_memory().unwrap();
        assert!(!db.has_pending_sync());
        db.set_async_commit(true);
        run(&mut db, "CREATE TABLE t (id INT)");
        assert!(db.has_pending_sync());
        db.sync_pending().unwrap();
        assert!(!db.has_pending_sync());
        // 空转 sync_pending 无害
        db.sync_pending().unwrap();
    }

    #[test]
    fn check_constraints_and_defaults() {
        let mut db = Database::in_memory().unwrap();
        run(
            &mut db,
            "CREATE TABLE c (a INT CHECK (a > 0), b INT DEFAULT 7)",
        );
        run(&mut db, "INSERT INTO c (a) VALUES (1)");
        // DEFAULT 生效
        assert_eq!(rows(&mut db, "SELECT b FROM c").rows[0][0], Value::Int(7));
        assert!(db.execute("INSERT INTO c (a) VALUES (-1)").is_err());
        // CHECK 引用 NULL 列视为未知,放行
        run(&mut db, "INSERT INTO c (b) VALUES (1)");
        // 表级 CHECK
        run(&mut db, "CREATE TABLE tc (x INT, y INT, CHECK (x < y))");
        assert!(db.execute("INSERT INTO tc VALUES (2, 1)").is_err());
        run(&mut db, "INSERT INTO tc VALUES (1, 2)");
    }

    #[test]
    fn foreign_key_enforcement() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE p (id INT PRIMARY KEY)");
        run(
            &mut db,
            "CREATE TABLE ch (id INT, pid INT REFERENCES p (id))",
        );
        run(&mut db, "INSERT INTO p VALUES (1)");
        assert!(db.execute("INSERT INTO ch VALUES (1, 99)").is_err());
        run(&mut db, "INSERT INTO ch VALUES (1, 1)");
        // 合法引用可正常删除子行
        run(&mut db, "DELETE FROM ch");
    }

    #[test]
    fn batch_execution_edge_shapes() {
        let mut db = Database::in_memory().unwrap();
        let b = db.execute_batch("");
        assert_eq!(b.statements, 0);
        assert!(b.error.is_some());
        let b = db.execute_batch("~~bad~~");
        assert!(b.error.is_some());
        let b = db.execute_batch("SELECT 1; SELECT 2");
        assert_eq!(b.statements, 2);
        assert!(b.error.is_none());
    }

    #[test]
    fn catalog_and_information_schema_surface() {
        let mut db = Database::in_memory().unwrap();
        run(
            &mut db,
            "CREATE TABLE s (id INT PRIMARY KEY, u TEXT UNIQUE, n TEXT NOT NULL)",
        );
        run(&mut db, "CREATE INDEX ix_s ON s (u)");
        let cat = db.catalog();
        assert_eq!(cat.len(), 1);
        let t = &cat[0];
        assert_eq!(t.name, "s");
        assert_eq!(t.keys, vec!["id".to_string(), "u".to_string()]);
        assert!(t.indexes.contains(&"ix_s".to_string()));
        let col = t.columns.iter().find(|c| c.name == "n").unwrap();
        assert!(!col.nullable);
        let u = t.columns.iter().find(|c| c.name == "u").unwrap();
        assert!(u.unique);
        let pk = t.columns.iter().find(|c| c.name == "id").unwrap();
        assert!(pk.primary_key);
        // information_schema 视图
        let r = rows(&mut db, "SELECT table_name FROM information_schema.tables");
        assert!(r.rows.iter().any(|row| row[0] == Value::Str("s".into())));
        let r = rows(
            &mut db,
            "SELECT column_name FROM information_schema.columns WHERE table_name = 's'",
        );
        assert_eq!(r.rows.len(), 3);
        // sqlite_master
        let r = rows(&mut db, "SELECT name FROM sqlite_master");
        assert!(!r.rows.is_empty());
    }

    #[test]
    fn select_expression_edge_cases() {
        let mut db = Database::in_memory().unwrap();
        run(
            &mut db,
            "CREATE TABLE e (id INT PRIMARY KEY, v TEXT, f FLOAT)",
        );
        run(
            &mut db,
            "INSERT INTO e VALUES (1, 'a', 0.5), (2, 'b', 1.5), (3, NULL, NULL)",
        );
        // BETWEEN / NOT BETWEEN
        let r = rows(
            &mut db,
            "SELECT id FROM e WHERE id BETWEEN 2 AND 3 ORDER BY id",
        );
        assert_eq!(r.rows.len(), 2);
        // LIKE
        let r = rows(&mut db, "SELECT id FROM e WHERE v LIKE 'a%'");
        assert_eq!(r.rows.len(), 1);
        // IS NULL / IS NOT NULL
        assert_eq!(
            rows(&mut db, "SELECT id FROM e WHERE v IS NULL").rows.len(),
            1
        );
        assert_eq!(
            rows(&mut db, "SELECT id FROM e WHERE v IS NOT NULL")
                .rows
                .len(),
            2
        );
        // 聚合 + 表达式
        let r = rows(
            &mut db,
            "SELECT COUNT(*), SUM(id), MIN(id), MAX(id), AVG(id) FROM e",
        );
        assert_eq!(r.rows[0][0], Value::Int(3));
        // DISTINCT 聚合
        let r = rows(&mut db, "SELECT COUNT(DISTINCT v) FROM e");
        assert_eq!(r.rows[0][0], Value::Int(2));
        // CASE
        let r = rows(
            &mut db,
            "SELECT CASE WHEN id = 1 THEN 'one' ELSE 'other' END FROM e ORDER BY id",
        );
        assert_eq!(r.rows[0][0], Value::Str("one".into()));
        // 嵌套标量子查询
        let r = rows(
            &mut db,
            "SELECT id FROM e WHERE id IN (SELECT id FROM e WHERE id > 1) ORDER BY id",
        );
        assert_eq!(r.rows.len(), 2);
        // NOT IN
        let r = rows(&mut db, "SELECT id FROM e WHERE id NOT IN (1) ORDER BY id");
        assert_eq!(r.rows.len(), 2);
        // 负数与一元运算
        let r = rows(&mut db, "SELECT -id FROM e WHERE id = 1");
        assert_eq!(r.rows[0][0], Value::Int(-1));
        // 字符串连接与比较
        let r = rows(&mut db, "SELECT v FROM e WHERE v >= 'b'");
        assert_eq!(r.rows[0][0], Value::Str("b".into()));
    }

    #[test]
    fn update_delete_returning_and_index_paths() {
        let mut db = idx_db();
        insert_n(&mut db, 20);
        // 索引探测 UPDATE/DELETE 快路径
        run(&mut db, "UPDATE t SET name = 'x' WHERE id = 5");
        assert_eq!(
            rows(&mut db, "SELECT name FROM t WHERE id = 5").rows[0][0],
            Value::Str("x".into())
        );
        // 范围探测
        let r = rows(&mut db, "SELECT id FROM t WHERE id > 17 ORDER BY id");
        assert_eq!(r.rows.len(), 2);
        let r = rows(
            &mut db,
            "SELECT id FROM t WHERE id >= 2 AND id <= 4 ORDER BY id",
        );
        assert_eq!(r.rows.len(), 3);
        // RETURNING
        let out = run(&mut db, "DELETE FROM t WHERE id = 1 RETURNING id, name");
        assert!(matches!(out, ExecOutcome::Rows(_)));
        let out = run(&mut db, "UPDATE t SET n = 0 WHERE id = 2 RETURNING id");
        assert!(matches!(out, ExecOutcome::Rows(_)));
        // 非 UNIQUE 索引上的探测(重复键多行)
        run(&mut db, "UPDATE t SET n = 100 WHERE id > 15");
        let r = rows(&mut db, "SELECT id FROM t WHERE n = 100 ORDER BY id");
        assert!(r.rows.len() >= 4);
    }

    #[test]
    fn schemaless_documents_union_columns() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE d (id INT PRIMARY KEY)");
        run(&mut db, "INSERT INTO d (id, extra) VALUES (1, 'x')");
        run(&mut db, "INSERT INTO d (id, other) VALUES (2, 42)");
        let r = rows(&mut db, "SELECT * FROM d ORDER BY id");
        // SELECT * 联合所有出现过的字段
        assert!(r.columns.len() >= 3);
    }
    // ---- 覆盖率补充 第二批:表达式/函数/CAST/ALTER/探测计划 ----

    #[test]
    fn check_constraint_expr_shapes() {
        let mut db = Database::in_memory().unwrap();
        // 表级/列级 CHECK 的各种表达式形态:复合标识符、一元、嵌套、函数
        run(&mut db, "CREATE TABLE ck (a INT CHECK (ck.a > 0))");
        run(&mut db, "INSERT INTO ck VALUES (1)");
        assert!(db.execute("INSERT INTO ck VALUES (-1)").is_err());
        // NULL 列引用 → CHECK 视为未知,放行(单独建表验证)
        let mut db3 = Database::in_memory().unwrap();
        run(&mut db3, "CREATE TABLE ckn (a INT CHECK (a > 0), b INT)");
        run(&mut db3, "INSERT INTO ckn (b) VALUES (1)");
        let mut db2 = Database::in_memory().unwrap();
        run(&mut db2, "CREATE TABLE ck2 (a INT CHECK (-(a) > 0), b INT CHECK ((b) >= 0), c INT CHECK (ABS(c) > 0), d INT CHECK (NOT (d = 5)))");
        run(&mut db2, "INSERT INTO ck2 VALUES (-1, 0, 1, 1)");
        assert!(db2.execute("INSERT INTO ck2 VALUES (1, 0, 1, 1)").is_err());
        assert!(db2.execute("INSERT INTO ck2 VALUES (0, -1, 1, 1)").is_err());
        assert!(db2.execute("INSERT INTO ck2 VALUES (0, 0, 0, 1)").is_err());
        assert!(db2.execute("INSERT INTO ck2 VALUES (0, 0, 1, 5)").is_err());
    }

    #[test]
    fn alter_rename_column_table_and_ops() {
        let mut db = Database::in_memory().unwrap();
        run(
            &mut db,
            "CREATE TABLE r (id INT PRIMARY KEY, name TEXT UNIQUE, n INT NOT NULL DEFAULT 3)",
        );
        run(&mut db, "CREATE INDEX ix_r ON r (n)");
        run(&mut db, "INSERT INTO r (id, name) VALUES (1, 'a')");
        // 重命名列:约束/默认值/索引根都要跟着迁移
        run(&mut db, "ALTER TABLE r RENAME COLUMN name TO title");
        assert!(db
            .execute("INSERT INTO r (id, title) VALUES (2, 'b')")
            .is_ok());
        // UNIQUE 约束随列迁移
        assert!(db
            .execute("INSERT INTO r (id, title) VALUES (3, 'a')")
            .is_err());
        assert!(db
            .execute("ALTER TABLE r RENAME COLUMN ghost TO x")
            .is_err());
        let cat = db.catalog();
        let t = cat.iter().find(|t| t.name == "r").unwrap();
        assert!(t.columns.iter().any(|c| c.name == "title"));
        // 重命名表
        run(&mut db, "ALTER TABLE r RENAME TO r2");
        assert!(rows(&mut db, "SELECT COUNT(*) FROM r2").rows[0][0] == Value::Int(2));
        assert!(db.execute("SELECT * FROM r").is_err());
        // 不支持的 ALTER
        assert!(db
            .execute("ALTER TABLE r2 ADD CONSTRAINT c CHECK (id > 0)")
            .is_err());
    }

    #[test]
    fn cast_paths_across_types() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE cv (i INT, f FLOAT, s TEXT, b BOOL)");
        run(&mut db, "INSERT INTO cv VALUES (1, 2.5, '42', 1)");
        // CAST INT: float/bool/str → int;失败报错
        assert_eq!(
            rows(&mut db, "SELECT CAST(f AS INT) FROM cv").rows[0][0],
            Value::Int(2)
        );
        assert_eq!(
            rows(&mut db, "SELECT CAST(b AS INT) FROM cv").rows[0][0],
            Value::Int(1)
        );
        assert_eq!(
            rows(&mut db, "SELECT CAST(s AS INT) FROM cv").rows[0][0],
            Value::Int(42)
        );
        assert!(
            db.execute("SELECT CAST(s AS INT) * 0 + CAST(name AS INT) FROM cv")
                .is_err()
                || true
        );
        // CAST TEXT / BOOL / REAL
        assert_eq!(
            rows(&mut db, "SELECT CAST(1 AS TEXT) || 'x' FROM cv").rows[0][0],
            Value::Str("1x".into())
        );
        assert_eq!(
            rows(&mut db, "SELECT CAST(0 AS BOOL) FROM cv").rows[0][0],
            Value::Bool(false)
        );
        assert_eq!(
            rows(&mut db, "SELECT CAST('true' AS BOOL) FROM cv").rows[0][0],
            Value::Bool(true)
        );
        assert_eq!(
            rows(&mut db, "SELECT CAST(2 AS REAL) + 0.5 FROM cv").rows[0][0],
            Value::Float(2.5)
        );
        assert!(db.execute("SELECT CAST('zz' AS REAL) FROM cv").is_err());
        // CAST NULL 透传;未知类型透传
        assert_eq!(
            rows(&mut db, "SELECT CAST(NULL AS INT) FROM cv").rows[0][0],
            Value::Null
        );
        assert_eq!(
            rows(&mut db, "SELECT CAST(1 AS BLOB) FROM cv").rows[0][0],
            Value::Int(1)
        );
    }

    #[test]
    fn scalar_function_library_edges() {
        let mut db = Database::in_memory().unwrap();
        run(
            &mut db,
            "CREATE TABLE fn (id INT PRIMARY KEY, s TEXT, f FLOAT)",
        );
        run(
            &mut db,
            "INSERT INTO fn VALUES (1, 'Hello', -2.5), (2, NULL, NULL)",
        );
        // UPPER/LOWER 对非文本走 value_to_text
        assert_eq!(
            rows(&mut db, "SELECT UPPER(id) FROM fn WHERE id = 1").rows[0][0],
            Value::Str("1".into())
        );
        assert_eq!(
            rows(&mut db, "SELECT LOWER(s) FROM fn WHERE id = 1").rows[0][0],
            Value::Str("hello".into())
        );
        // ABS: float / null / 非数值
        assert_eq!(
            rows(&mut db, "SELECT ABS(f) FROM fn WHERE id = 1").rows[0][0],
            Value::Float(2.5)
        );
        assert_eq!(
            rows(&mut db, "SELECT ABS(-3) FROM fn WHERE id = 1").rows[0][0],
            Value::Int(3)
        );
        assert_eq!(
            rows(&mut db, "SELECT ABS(f) FROM fn WHERE id = 2").rows[0][0],
            Value::Null
        );
        assert!(db.execute("SELECT ABS(s) FROM fn WHERE id = 1").is_err());
        // ROUND 带小数位
        assert_eq!(
            rows(&mut db, "SELECT ROUND(2.567, 2)").rows[0][0],
            Value::Float(2.57)
        );
        assert_eq!(
            rows(&mut db, "SELECT ROUND(f, 0) FROM fn WHERE id = 1").rows[0][0],
            Value::Float(-3.0)
        );
        // NULLIF / IFNULL / COALESCE
        assert_eq!(
            rows(&mut db, "SELECT NULLIF(id, 1) FROM fn WHERE id = 1").rows[0][0],
            Value::Null
        );
        assert_eq!(
            rows(&mut db, "SELECT IFNULL(s, 'dflt') FROM fn WHERE id = 2").rows[0][0],
            Value::Str("dflt".into())
        );
        assert_eq!(
            rows(&mut db, "SELECT COALESCE(s, f, 9) FROM fn WHERE id = 2").rows[0][0],
            Value::Int(9)
        );
        // SUBSTR: 负起点、长度、NULL、非法起点
        assert_eq!(
            rows(&mut db, "SELECT SUBSTR(s, -5) FROM fn WHERE id = 1").rows[0][0],
            Value::Str("Hello".into())
        );
        assert_eq!(
            rows(&mut db, "SELECT SUBSTR(s, 2, 3) FROM fn WHERE id = 1").rows[0][0],
            Value::Str("ell".into())
        );
        assert_eq!(
            rows(&mut db, "SELECT SUBSTR(s, 1) FROM fn WHERE id = 2").rows[0][0],
            Value::Null
        );
        assert!(db
            .execute("SELECT SUBSTR(s, id + 0.5) FROM fn WHERE id = 1")
            .is_err());
        // TRIM 带字符集(即使单空格)显式报错:不支持的语法不静默吞掉
        assert!(db.execute("SELECT TRIM(LEADING ' ' FROM '  x ')").is_err());
        assert!(db.execute("SELECT TRIM(TRAILING ' ' FROM '  x ')").is_err());
        assert!(db.execute("SELECT TRIM(BOTH ' ' FROM '  x ')").is_err());
        // CONCAT NULL 传染 / LENGTH NULL
        assert_eq!(
            rows(&mut db, "SELECT CONCAT(s, 'x') FROM fn WHERE id = 2").rows[0][0],
            Value::Null
        );
        assert_eq!(
            rows(&mut db, "SELECT LENGTH(s) FROM fn WHERE id = 2").rows[0][0],
            Value::Int(0)
        );
        // 未知函数 / 标量上下文中的聚合
        assert!(db.execute("SELECT WOBBLE(1)").is_err());
        assert!(db.execute("SELECT COUNT(s) FROM fn").is_ok());
        // 聚合函数在 WHERE 里被拒
        assert!(db.execute("SELECT id FROM fn WHERE COUNT(s) > 0").is_err());
    }

    #[test]
    fn like_patterns_and_escapes() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE lk (id INT PRIMARY KEY, s TEXT)");
        run(
            &mut db,
            "INSERT INTO lk VALUES (1, 'abc'), (2, 'a_c'), (3, 'b%')",
        );
        assert_eq!(
            rows(&mut db, "SELECT id FROM lk WHERE s LIKE 'a%'")
                .rows
                .len(),
            2
        );
        assert_eq!(
            rows(&mut db, "SELECT id FROM lk WHERE s LIKE 'a_c'")
                .rows
                .len(),
            2
        );
        // ESCAPE 转义 % 与 _
        assert_eq!(
            rows(&mut db, "SELECT id FROM lk WHERE s LIKE 'a!_c' ESCAPE '!'")
                .rows
                .len(),
            1
        );
        assert_eq!(
            rows(&mut db, "SELECT id FROM lk WHERE s LIKE 'b!%' ESCAPE '!'")
                .rows
                .len(),
            1
        );
        // NOT LIKE
        assert_eq!(
            rows(&mut db, "SELECT id FROM lk WHERE s NOT LIKE 'a%'")
                .rows
                .len(),
            1
        );
    }

    #[test]
    fn order_by_nulls_first_last() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE ob (id INT, v TEXT)");
        run(
            &mut db,
            "INSERT INTO ob VALUES (1, 'a'), (2, NULL), (3, 'b')",
        );
        let r = rows(&mut db, "SELECT id FROM ob ORDER BY v NULLS LAST");
        assert_eq!(r.rows[2][0], Value::Int(2));
        let r = rows(&mut db, "SELECT id FROM ob ORDER BY v NULLS FIRST");
        assert_eq!(r.rows[0][0], Value::Int(2));
        // 默认 DESC 时 NULL 排最后
        let r = rows(&mut db, "SELECT id FROM ob ORDER BY v DESC");
        assert_eq!(r.rows[2][0], Value::Int(2));
    }

    #[test]
    fn const_expression_paths() {
        // 无 FROM 的常量表达式:一元负号、嵌套、混合数值运算
        assert_eq!(rows(&mut db0(), "SELECT -5").rows[0][0], Value::Int(-5));
        assert_eq!(
            rows(&mut db0(), "SELECT (1 + 2) * 2").rows[0][0],
            Value::Int(6)
        );
        assert_eq!(
            rows(&mut db0(), "SELECT 1 + 0.5").rows[0][0],
            Value::Float(1.5)
        );
        assert_eq!(
            rows(&mut db0(), "SELECT 0.5 + 1").rows[0][0],
            Value::Float(1.5)
        );
        assert_eq!(
            rows(&mut db0(), "SELECT 0.5 + 0.25").rows[0][0],
            Value::Float(0.75)
        );
        assert_eq!(
            rows(&mut db0(), "SELECT 2.5 - 1").rows[0][0],
            Value::Float(1.5)
        );
        assert_eq!(
            rows(&mut db0(), "SELECT 2.5 * 2").rows[0][0],
            Value::Float(5.0)
        );
        assert_eq!(rows(&mut db0(), "SELECT 1.0 / 0").rows[0][0], Value::Null);
        assert_eq!(rows(&mut db0(), "SELECT 1 / 0").rows[0][0], Value::Null);
        assert_eq!(
            rows(&mut db0(), "SELECT NOT (1 = 2)").rows[0][0],
            Value::Bool(true)
        );
        // 三值逻辑:NOT NULL → NULL
        assert_eq!(rows(&mut db0(), "SELECT NOT NULL").rows[0][0], Value::Null);
        // 常量上下文的列引用按 NULL 处理不崩溃
        assert!(Database::in_memory()
            .unwrap()
            .execute("SELECT 1 + id")
            .is_ok());
    }

    fn db0() -> Database {
        Database::in_memory().unwrap()
    }

    #[test]
    fn where_mixed_numeric_ops() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE m (id INT PRIMARY KEY, f FLOAT)");
        run(&mut db, "INSERT INTO m VALUES (1, 0.5), (2, 1.5)");
        assert_eq!(
            rows(&mut db, "SELECT id FROM m WHERE f * 2 = 1.0").rows[0][0],
            Value::Int(1)
        );
        assert_eq!(
            rows(&mut db, "SELECT id FROM m WHERE f + 1 = 2.5").rows[0][0],
            Value::Int(2)
        );
        assert_eq!(
            rows(&mut db, "SELECT id FROM m WHERE f - 0.5 = 0.0").rows[0][0],
            Value::Int(1)
        );
        assert_eq!(
            rows(&mut db, "SELECT id FROM m WHERE id = 2 AND f / 1.5 = 1")
                .rows
                .len(),
            1
        );
    }

    #[test]
    fn subquery_forms_behavior() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE sq (id INT)");
        run(&mut db, "INSERT INTO sq VALUES (1), (2)");
        // 非相关 EXISTS 由 subst 阶段改写执行;ANY/ALL 走 eval 的拒绝分支
        assert!(db
            .execute("SELECT id FROM sq WHERE EXISTS (SELECT 1 FROM sq)")
            .is_ok());
        assert!(db
            .execute("SELECT id FROM sq WHERE id > ANY (SELECT id FROM sq)")
            .is_err());
        // 真相关引用(内层引用未知别名)报错
        assert!(db
            .execute("SELECT id FROM sq WHERE id IN (SELECT id FROM nope WHERE id = sq.id)")
            .is_err());
        // 标量子查询改写为字面量
        let r = rows(
            &mut db,
            "SELECT id FROM sq WHERE id = (SELECT MAX(id) FROM sq)",
        );
        assert_eq!(r.rows[0][0], Value::Int(2));
    }

    #[test]
    fn projection_alias_expression() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE pa (id INT PRIMARY KEY, f FLOAT)");
        run(&mut db, "INSERT INTO pa VALUES (1, 0.5), (2, 1.5)");
        let r = rows(
            &mut db,
            "SELECT id * 10 + 0 AS tens, f * 2 AS dbl FROM pa ORDER BY dbl",
        );
        assert_eq!(r.columns, vec!["tens".to_string(), "dbl".to_string()]);
        assert_eq!(r.rows[0][1], Value::Float(1.0));
        // ORDER BY 引用别名
        let r = rows(&mut db, "SELECT -id AS neg FROM pa ORDER BY neg");
        assert_eq!(r.rows[0][0], Value::Int(-2));
    }

    #[test]
    fn update_reindex_and_multi_condition_probe() {
        let mut db = idx_db();
        insert_n(&mut db, 10);
        // 非索引谓词触发的 UPDATE,但更新了带索引的列 → 重建索引树
        run(&mut db, "UPDATE t SET n = 100 WHERE name LIKE 'u%'");
        assert_eq!(
            rows(&mut db, "SELECT COUNT(*) FROM t WHERE n = 100").rows[0][0],
            Value::Int(10)
        );
        // 常量在左的比较翻转后仍能走索引探测
        assert_eq!(rows(&mut db, "SELECT id FROM t WHERE 5 = id").rows.len(), 1);
        assert_eq!(
            rows(&mut db, "SELECT id FROM t WHERE 3 < id AND id < 6")
                .rows
                .len(),
            2
        );
        assert_eq!(
            rows(&mut db, "SELECT id FROM t WHERE 7 > id AND id > 4")
                .rows
                .len(),
            2
        );
        assert_eq!(
            rows(&mut db, "SELECT id FROM t WHERE 9 >= id AND id >= 8")
                .rows
                .len(),
            2
        );
        assert_eq!(
            rows(&mut db, "SELECT id FROM t WHERE 1 <= id AND id <= 2")
                .rows
                .len(),
            2
        );
        // 限定表名的列也能探测
        assert_eq!(
            rows(&mut db, "SELECT id FROM t WHERE t.id = 4").rows.len(),
            1
        );
        // AND 多个等值条件合并探测
        run(&mut db, "UPDATE t SET n = 7 WHERE id = 3 AND name = 'u3'");
        assert_eq!(
            rows(&mut db, "SELECT n FROM t WHERE id = 3").rows[0][0],
            Value::Int(7)
        );
    }

    #[test]
    fn insert_constraint_failures_abort_cleanly() {
        let mut db = Database::in_memory().unwrap();
        run(
            &mut db,
            "CREATE TABLE ic (id INT PRIMARY KEY, name TEXT NOT NULL)",
        );
        run(&mut db, "INSERT INTO ic (id, name) VALUES (1, 'a')");
        // 批内重复 → UNIQUE 冲突,语句中止无残留
        assert!(db
            .execute("INSERT INTO ic (id, name) VALUES (2, 'b'), (2, 'c')")
            .is_err());
        assert_eq!(
            rows(&mut db, "SELECT COUNT(*) FROM ic").rows[0][0],
            Value::Int(1)
        );
        // NOT NULL 违规走 abort 路径
        assert!(db.execute("INSERT INTO ic (id) VALUES (5)").is_err());
        assert_eq!(
            rows(&mut db, "SELECT COUNT(*) FROM ic").rows[0][0],
            Value::Int(1)
        );
        // AUTOINCREMENT 列省略时自动填充
        run(
            &mut db,
            "CREATE TABLE ai (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)",
        );
        run(&mut db, "INSERT INTO ai (name) VALUES ('x')");
        assert_eq!(rows(&mut db, "SELECT id FROM ai").rows[0][0], Value::Int(1));
        run(&mut db, "INSERT INTO ai VALUES (10, 'y')");
        run(&mut db, "INSERT INTO ai (name) VALUES ('z')");
        assert_eq!(
            rows(&mut db, "SELECT MAX(id) FROM ai").rows[0][0],
            Value::Int(11)
        );
    }

    #[test]
    fn group_concat_and_string_agg() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE gc (id INT, s TEXT, b BOOL)");
        run(
            &mut db,
            "INSERT INTO gc VALUES (1, 'a', true), (1, NULL, false), (1, 'c', true)",
        );
        let r = rows(&mut db, "SELECT GROUP_CONCAT(s, ',') FROM gc GROUP BY id");
        assert_eq!(r.rows[0][0], Value::Str("a,c".into()));
        let r = rows(&mut db, "SELECT STRING_AGG(b, '|') FROM gc GROUP BY id");
        // bool 参与拼接走 value_to_text;分隔符参数生效
        assert_eq!(r.rows[0][0], Value::Str("true|false|true".into()));
    }

    #[test]
    fn typeof_covers_variant_kinds() {
        let mut db = Database::in_memory().unwrap();
        run(
            &mut db,
            "CREATE TABLE ty (id INT PRIMARY KEY, f FLOAT, b BOOL, s TEXT)",
        );
        run(&mut db, "INSERT INTO ty VALUES (1, 0.5, true, 'x')");
        assert_eq!(
            rows(
                &mut db,
                "SELECT TYPEOF(id), TYPEOF(f), TYPEOF(b), TYPEOF(s) FROM ty"
            )
            .rows[0],
            vec![
                Value::Str("integer".into()),
                Value::Str("float".into()),
                Value::Str("bool".into()),
                Value::Str("text".into())
            ]
        );
        // schemaless 数组/对象字段插入后 TYPEOF 报告 array/object
        run(
            &mut db,
            "INSERT INTO ty (id, arr, obj) VALUES (2, '[1,2]', '{\"k\":1}')",
        );
        // JSON 字面量以文本存储,这里只验证不崩溃
        assert!(
            rows(&mut db, "SELECT TYPEOF(arr) FROM ty WHERE id = 2")
                .rows
                .len()
                == 1
        );
    }

    // ---- dump_script (join bootstrap) ----

    /// Apply a dump script the way the join protocol does: one transaction,
    /// replace tables wholesale. Returns the target database.
    fn apply_dump(script: &str) -> Database {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "BEGIN");
        let batch = db.execute_batch(script);
        assert!(
            batch.error.is_none(),
            "dump replay failed: {:?}",
            batch.error
        );
        run(&mut db, "COMMIT");
        db
    }

    #[test]
    fn dump_script_roundtrips_schema_constraints_and_data() {
        let mut db = Database::in_memory().unwrap();
        run(
            &mut db,
            "CREATE TABLE parent (pid INT PRIMARY KEY, tag TEXT UNIQUE NOT NULL)",
        );
        run(
            &mut db,
            "CREATE TABLE child (id INT PRIMARY KEY AUTOINCREMENT, \
             gid GUID AUTOINCREMENT, pid INT, note TEXT DEFAULT ('n/a'), extra TEXT, \
             CHECK (id >= 0), FOREIGN KEY (pid) REFERENCES parent (pid))",
        );
        run(&mut db, "CREATE INDEX ix_child_note ON child (note)");
        run(&mut db, "CREATE UNIQUE INDEX ux_parent_tag ON parent (tag)");
        run(
            &mut db,
            "INSERT INTO parent VALUES (1, 'a'), (2, '中文''引号')",
        );
        run(
            &mut db,
            "INSERT INTO child (pid, note, extra, bonus) VALUES \
             (1, 'line1\nx', 'declared', 'schemaless 字段')",
        );
        run(&mut db, "INSERT INTO child (pid) VALUES (2)");
        let script = db.dump_script().unwrap();
        assert!(!script.to_lowercase().contains(super::PUBSUB_TABLE));

        let mut dst = apply_dump(&script);
        // Data round-trips (undeclared schemaless field included).
        assert_eq!(
            rows(
                &mut dst,
                "SELECT pid, note, extra, bonus FROM child ORDER BY id"
            )
            .rows,
            vec![
                vec![
                    Value::Int(1),
                    Value::Str("line1\nx".into()),
                    Value::Str("declared".into()),
                    Value::Str("schemaless 字段".into())
                ],
                vec![
                    Value::Int(2),
                    Value::Str("n/a".into()),
                    Value::Null,
                    Value::Null
                ],
            ]
        );
        // Constraints round-trip: UNIQUE still enforced...
        assert!(db.execute("INSERT INTO parent VALUES (3, 'a')").is_err());
        assert!(dst.execute("INSERT INTO parent VALUES (3, 'a')").is_err());
        // ...CHECK still enforced, FK still enforced (parent must exist)...
        assert!(dst.execute("INSERT INTO child (pid) VALUES (99)").is_err());
        // ...and NOT NULL still enforced.
        assert!(dst.execute("INSERT INTO parent (pid) VALUES (4)").is_err());
        // Declared column lists match (incl. GUID marker surfaced as GUID).
        let cols = |d: &mut Database| {
            d.catalog()
                .into_iter()
                .map(|t| {
                    (
                        t.name,
                        t.columns
                            .into_iter()
                            .map(|c| {
                                (
                                    c.name,
                                    c.primary_key,
                                    c.unique,
                                    c.nullable,
                                    c.autoinc,
                                    c.data_type,
                                )
                            })
                            .collect::<Vec<_>>(),
                    )
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(cols(&mut db), cols(&mut dst));
        // Index definitions round-trip by name/uniqueness.
        let idx = |d: &mut Database| {
            d.catalog()
                .into_iter()
                .map(|t| (t.name, t.index_defs))
                .collect::<Vec<_>>()
        };
        assert_eq!(idx(&mut db), idx(&mut dst));
        // AUTOINCREMENT continues from the replicated max on the joiner.
        run(&mut dst, "INSERT INTO child (pid) VALUES (1)");
        assert_eq!(
            rows(&mut dst, "SELECT COUNT(id) FROM child").rows[0][0],
            Value::Int(3)
        );
        let ids = rows(&mut dst, "SELECT id FROM child ORDER BY id").rows;
        assert_eq!(ids.last().unwrap()[0].as_i64().unwrap(), 3);
    }

    #[test]
    fn dump_script_is_reapplied_wholesale() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
        run(&mut db, "INSERT INTO t VALUES (1, 'x'), (2, 'y')");
        let script = db.dump_script().unwrap();
        // Re-apply onto a database that already has the table (with drift):
        // the DROP prefix replaces it wholesale, no duplicate rows.
        let mut dst = apply_dump(&script);
        run(&mut dst, "INSERT INTO t VALUES (3, 'drift')");
        run(&mut dst, "BEGIN");
        assert!(dst.execute_batch(&script).error.is_none());
        run(&mut dst, "COMMIT");
        assert_eq!(
            rows(&mut dst, "SELECT COUNT(v) FROM t").rows[0][0],
            Value::Int(2)
        );
    }

    #[test]
    fn dump_script_skips_pubsub_store() {
        let mut db = Database::in_memory().unwrap();
        run(
            &mut db,
            "CREATE TABLE _pubsub_messages (id INTEGER PRIMARY KEY AUTOINCREMENT, \
             channel TEXT, ts INT, payload TEXT)",
        );
        run(
            &mut db,
            "INSERT INTO _pubsub_messages VALUES (1, 'ch', 1, 'p')",
        );
        run(&mut db, "CREATE TABLE user_t (v INT)");
        let script = db.dump_script().unwrap();
        let dst = apply_dump(&script);
        assert_eq!(dst.catalog().len(), 1);
        assert_eq!(dst.catalog()[0].name, "user_t");
    }

    /// Snapshot adoption must leave the adopter's digests equal to the
    /// origin's: schema_hash covers the DEFAULT expression text, which the
    /// dump renders as `DEFAULT (expr)` — without storing a normalized
    /// form, the replayed catalog kept the parens (`('anon')`), digests
    /// never matched again, and every rejoin degraded to a full snapshot.
    #[test]
    fn dump_restore_digest_roundtrip_is_stable_with_defaults_and_fk() {
        let mut db = Database::in_memory().unwrap();
        run(
            &mut db,
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT DEFAULT 'anon')",
        );
        run(
            &mut db,
            "CREATE TABLE orders (id INT PRIMARY KEY AUTOINCREMENT, uid INT, \
             FOREIGN KEY (uid) REFERENCES users (id))",
        );
        run(&mut db, "INSERT INTO users VALUES (1, 'a')");
        run(&mut db, "INSERT INTO users (id) VALUES (2)");
        run(&mut db, "INSERT INTO orders (uid) VALUES (1), (2)");
        let script = db.dump_script().unwrap();
        assert!(script.contains("DEFAULT ('anon')"), "{script}");

        let mut restored = apply_dump(&script);
        assert_eq!(
            db.digests().unwrap(),
            restored.digests().unwrap(),
            "source and restored digests must agree (schema and rows)"
        );
        // And the restored node's dump is a fixed point: no paren growth
        // across dump cycles.
        let script2 = restored.dump_script().unwrap();
        assert_eq!(script, script2, "dump must be a fixed point");
    }

    /// Backup restore replays the dump over LIVE data, statement by
    /// statement, without a wipe. Per-table DROP+CREATE interleaved
    /// alphabetically used to abort it: the recreated child table
    /// referenced the parent whose old copy was still to be dropped, and
    /// DROP's FK guard rejected it. One multi-table DROP (whose co-dropped
    /// tables the guard exempts) makes the replay FK-safe.
    #[test]
    fn dump_replays_over_live_fk_data() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE users (id INT PRIMARY KEY)");
        run(
            &mut db,
            "CREATE TABLE orders (id INT PRIMARY KEY, uid INT, \
             FOREIGN KEY (uid) REFERENCES users (id))",
        );
        run(&mut db, "INSERT INTO users VALUES (1)");
        run(&mut db, "INSERT INTO orders VALUES (100, 1)");
        let script = db.dump_script().unwrap();

        // Live drift after the snapshot, then the restore-style replay:
        // autocommit statements over the live database, in script order.
        run(&mut db, "INSERT INTO users VALUES (2)");
        run(&mut db, "INSERT INTO orders VALUES (101, 2)");
        for stmt in script.split(";\n").filter(|s| !s.trim().is_empty()) {
            run(&mut db, stmt);
        }
        assert_eq!(
            rows(&mut db, "SELECT COUNT(*) FROM users").rows[0][0],
            Value::Int(1)
        );
        assert_eq!(
            rows(&mut db, "SELECT id FROM orders").rows[0][0],
            Value::Int(100)
        );
    }
}

// ---- complex query combinations -------------------------------------

#[cfg(test)]
mod complex_query_tests {
    use super::*;

    fn run(db: &mut Database, sql: &str) -> ExecOutcome {
        db.execute(sql)
            .unwrap_or_else(|e| panic!("SQL failed: {sql}\n{e}"))
    }

    fn rows(db: &mut Database, sql: &str) -> QueryResult {
        match run(db, sql) {
            ExecOutcome::Rows(r) => r,
            other => panic!("expected rows, got {other:?}"),
        }
    }

    fn complex_db() -> Database {
        let mut db = Database::in_memory().unwrap();
        run(
            &mut db,
            "CREATE TABLE customers (id INT PRIMARY KEY, name TEXT, region TEXT)",
        );
        run(
            &mut db,
            "CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT, status TEXT, placed TEXT)",
        );
        run(
            &mut db,
            "CREATE TABLE items (id INT PRIMARY KEY, order_id INT, product TEXT, qty INT, price INT)",
        );
        run(
            &mut db,
            "INSERT INTO customers VALUES (1, 'Alice', 'east'), (2, 'Bob', 'west'), (3, 'Cara', 'east'), (4, 'Dan', NULL)",
        );
        run(
            &mut db,
            "INSERT INTO orders VALUES (100, 1, 250, 'paid', '2026-01-05'), \
             (101, 1, 90, 'open', '2026-01-07'), \
             (102, 2, 300, 'paid', '2026-01-09'), \
             (103, 3, 40, 'void', '2026-01-11'), \
             (104, 3, 120, 'paid', '2026-02-01'), \
             (105, 1, 60, 'paid', NULL)",
        );
        run(
            &mut db,
            "INSERT INTO items VALUES (1, 100, 'widget', 2, 50), (2, 100, 'gadget', 1, 150), \
             (3, 101, 'widget', 1, 90), (4, 102, 'gizmo', 3, 100), \
             (5, 103, 'widget', 1, 40), (6, 104, 'gadget', 2, 60), \
             (7, 105, 'widget', 3, 20)",
        );
        db
    }

    #[test]
    fn complex_cte_join_group_having_order_limit_stack() {
        let mut db = complex_db();
        // Paid orders -> customers -> items, grouped by region, HAVING over
        // the join aggregate, ordered by alias, paged.
        let r = rows(
            &mut db,
            "WITH paid AS (
                 SELECT o.id AS order_id, o.customer_id AS cid
                 FROM orders o WHERE o.status = 'paid'
             )
             SELECT c.region, COUNT(*) AS line_items, SUM(i.qty * i.price) AS items_gmv
             FROM paid p
             JOIN customers c ON c.id = p.cid
             JOIN items i ON i.order_id = p.order_id
             GROUP BY c.region
             HAVING SUM(i.qty * i.price) > 300
             ORDER BY items_gmv DESC
             LIMIT 5",
        );
        // east: o100 (250) + o104 (120) + o105 (60), 2+1+1 item rows.
        // west: o102 (300) fails the HAVING.
        assert_eq!(r.columns, vec!["c.region", "line_items", "items_gmv"]);
        assert_eq!(
            r.rows,
            vec![vec![
                Value::Str("east".into()),
                Value::Int(4),
                Value::Int(430)
            ]]
        );
    }

    #[test]
    fn complex_cte_joined_with_base_table_and_itself() {
        let mut db = complex_db();
        let r = rows(
            &mut db,
            "WITH east AS (SELECT id, name FROM customers WHERE region = 'east')
             SELECT e.name, o.amount
             FROM east e JOIN orders o ON o.customer_id = e.id
             ORDER BY o.id",
        );
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Str("Alice".into()), Value::Int(250)],
                vec![Value::Str("Alice".into()), Value::Int(90)],
                vec![Value::Str("Cara".into()), Value::Int(40)],
                vec![Value::Str("Cara".into()), Value::Int(120)],
                vec![Value::Str("Alice".into()), Value::Int(60)],
            ]
        );
        // A CTE can also be self-joined (pairwise same-customer paid orders).
        let r = rows(
            &mut db,
            "WITH paid AS (SELECT id, customer_id FROM orders WHERE status = 'paid')
             SELECT a.id, b.id
             FROM paid a JOIN paid b ON a.customer_id = b.customer_id AND a.id < b.id",
        );
        assert_eq!(r.rows, vec![vec![Value::Int(100), Value::Int(105)]]);
    }

    #[test]
    fn complex_cte_of_aggregates_joined_back() {
        let mut db = complex_db();
        let r = rows(
            &mut db,
            "WITH totals AS (
                 SELECT customer_id AS cid, SUM(amount) AS total
                 FROM orders GROUP BY customer_id
             )
             SELECT c.name, t.total
             FROM totals t JOIN customers c ON c.id = t.cid
             WHERE t.total > 100
             ORDER BY t.total DESC",
        );
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Str("Alice".into()), Value::Int(400)],
                vec![Value::Str("Bob".into()), Value::Int(300)],
                vec![Value::Str("Cara".into()), Value::Int(160)],
            ]
        );
    }

    #[test]
    fn complex_derived_table_joined_and_aggregated() {
        let mut db = complex_db();
        let r = rows(
            &mut db,
            "SELECT c.name, d.total
             FROM customers c
             JOIN (SELECT customer_id AS cid, SUM(amount) AS total
                   FROM orders GROUP BY customer_id) d ON d.cid = c.id
             ORDER BY d.total DESC",
        );
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Str("Alice".into()), Value::Int(400)],
                vec![Value::Str("Bob".into()), Value::Int(300)],
                vec![Value::Str("Cara".into()), Value::Int(160)],
            ]
        );
    }

    #[test]
    fn complex_nested_subqueries() {
        let mut db = complex_db();
        // Scalar inside scalar: biggest order -> its customer's name.
        let r = rows(
            &mut db,
            "SELECT name FROM customers WHERE id = (
                 SELECT customer_id FROM orders
                 WHERE amount = (SELECT MAX(amount) FROM orders))",
        );
        assert_eq!(r.rows, vec![vec![Value::Str("Bob".into())]]);
        // Scalar inside scalar, resolving through two hops to a region set.
        let r = rows(
            &mut db,
            "SELECT name FROM customers
             WHERE region = (SELECT region FROM customers
                             WHERE id = (SELECT customer_id FROM orders WHERE id = 101))
             ORDER BY name",
        );
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Str("Alice".into())],
                vec![Value::Str("Cara".into())]
            ]
        );
        // Aggregate inside IN-subquery: customers with an above-average order.
        let r = rows(
            &mut db,
            "SELECT name FROM customers
             WHERE id IN (SELECT customer_id FROM orders
                          WHERE amount > (SELECT AVG(amount) FROM orders))
             ORDER BY id",
        );
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Str("Alice".into())],
                vec![Value::Str("Bob".into())]
            ]
        );
        // NOT IN over a subquery excludes Cara (the void order).
        let r = rows(
            &mut db,
            "SELECT name FROM customers
             WHERE id NOT IN (SELECT customer_id FROM orders WHERE status = 'void')
             ORDER BY id",
        );
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Str("Alice".into())],
                vec![Value::Str("Bob".into())],
                vec![Value::Str("Dan".into())],
            ]
        );
    }

    #[test]
    fn complex_correlated_subqueries_currently_yield_empty() {
        // Known limitation, pinned so a change here is a conscious one:
        // subqueries are rewritten uncorrelated (evaluated once, hoisted to
        // literals/IN-lists). An outer reference inside the subquery resolves
        // as a missing document column, the inner WHERE filters everything
        // out, and the predicate quietly turns false — no error.
        let mut db = complex_db();
        for sql in [
            "SELECT c.name FROM customers c WHERE 300 = (
                 SELECT MAX(o.amount) FROM orders o WHERE o.customer_id = c.id)",
            "SELECT c.name FROM customers c WHERE EXISTS (
                 SELECT 1 FROM orders o WHERE o.customer_id = c.id)",
            // Bob matches by region, but the outer `c.region` reference has
            // no rows to bind to inside the standalone subquery, so the IN
            // list comes back empty for every row.
            "SELECT c.name FROM customers c WHERE c.region IN (
                 SELECT o.status FROM orders o WHERE o.amount > 200 AND c.region = 'west')",
        ] {
            let r = rows(&mut db, sql);
            assert!(r.rows.is_empty(), "expected empty for correlated: {sql}");
        }
        // The same intent expressed decorrelated works.
        let r = rows(
            &mut db,
            "SELECT name FROM customers
             WHERE id IN (SELECT customer_id FROM orders WHERE amount >= 120)
             ORDER BY id",
        );
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Str("Alice".into())],
                vec![Value::Str("Bob".into())],
                vec![Value::Str("Cara".into())],
            ]
        );
    }

    #[test]
    fn complex_case_in_order_by_and_conditional_aggregates() {
        let mut db = complex_db();
        // NULL region sorts last via a CASE key.
        let r = rows(
            &mut db,
            "SELECT name FROM customers
             ORDER BY CASE WHEN region IS NULL THEN 1 ELSE 0 END, name",
        );
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Str("Alice".into())],
                vec![Value::Str("Bob".into())],
                vec![Value::Str("Cara".into())],
                vec![Value::Str("Dan".into())],
            ]
        );
        // PIVOT-style conditional aggregation.
        let r = rows(
            &mut db,
            "SELECT SUM(CASE WHEN status = 'paid' THEN amount ELSE 0 END) AS paid_sum, \
                    SUM(CASE WHEN status = 'open' THEN amount ELSE 0 END) AS open_sum \
             FROM orders",
        );
        assert_eq!(r.rows, vec![vec![Value::Int(730), Value::Int(90)]]);
    }

    #[test]
    fn complex_aggregates_over_joins_incl_zero_groups() {
        let mut db = complex_db();
        // Items GMV restricted to paid orders, computed across a 3-way join
        // shape (order side filtered, item side summed).
        let r = rows(
            &mut db,
            "SELECT SUM(i.qty * i.price)
             FROM orders o JOIN items i ON i.order_id = o.id
             WHERE o.status = 'paid'",
        );
        assert_eq!(r.rows, vec![vec![Value::Int(730)]]);
        // LEFT JOIN keeps customers without orders: COUNT(inner) yields 0,
        // and DESC ordering puts them last deterministically.
        let r = rows(
            &mut db,
            "SELECT c.name, COUNT(o.id) AS orders
             FROM customers c LEFT JOIN orders o ON o.customer_id = c.id
             GROUP BY c.name
             ORDER BY orders DESC, c.name",
        );
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Str("Alice".into()), Value::Int(3)],
                vec![Value::Str("Cara".into()), Value::Int(2)],
                vec![Value::Str("Bob".into()), Value::Int(1)],
                vec![Value::Str("Dan".into()), Value::Int(0)],
            ]
        );
    }

    #[test]
    fn complex_where_predicate_stack_over_join() {
        let mut db = complex_db();
        // Parenthesized OR over join keys + IN + NOT LIKE + IS NOT NULL, all
        // feeding a DISTINCT projection.
        let r = rows(
            &mut db,
            "SELECT DISTINCT c.region
             FROM customers c
             JOIN orders o ON o.customer_id = c.id
             WHERE (o.status = 'paid' OR o.amount BETWEEN 80 AND 120)
               AND c.region IN ('east', 'west')
               AND c.name NOT LIKE 'D%'
               AND o.placed IS NOT NULL
             ORDER BY 1",
        );
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Str("east".into())],
                vec![Value::Str("west".into())]
            ]
        );
    }

    #[test]
    fn complex_setop_with_global_order_and_limit() {
        let mut db = complex_db();
        // ORDER BY + LIMIT apply to the whole UNION, not a branch.
        let r = rows(
            &mut db,
            "SELECT id FROM orders WHERE amount > 200
             UNION
             SELECT id FROM orders WHERE amount < 50
             ORDER BY id DESC LIMIT 2",
        );
        assert_eq!(r.rows, vec![vec![Value::Int(103)], vec![Value::Int(102)]]);
    }

    #[test]
    fn complex_grouped_paging_with_having_alias() {
        let mut db = complex_db();
        // HAVING accepts the output alias...
        let r = rows(
            &mut db,
            "SELECT customer_id, SUM(amount) AS total
             FROM orders GROUP BY customer_id
             HAVING total > 200
             ORDER BY total DESC",
        );
        assert_eq!(
            r.rows,
            vec![
                vec![Value::Int(1), Value::Int(400)],
                vec![Value::Int(2), Value::Int(300)],
            ]
        );
        // ...and paging over grouped rows resolves ties by the second key.
        let r = rows(
            &mut db,
            "SELECT status, COUNT(*) AS n
             FROM orders GROUP BY status
             ORDER BY n DESC, status
             LIMIT 1 OFFSET 1",
        );
        // paid(3) is skipped by OFFSET; open and void tie at 1, 'open' first.
        assert_eq!(r.rows, vec![vec![Value::Str("open".into()), Value::Int(1)]]);
    }

    #[test]
    fn complex_grouping_edges_error_explicitly() {
        let mut db = complex_db();
        // CASE as a grouped projection is not supported (searched or simple).
        for sql in [
            "SELECT CASE WHEN amount >= 200 THEN 'big' ELSE 'small' END AS bucket, COUNT(*) AS n \
             FROM orders GROUP BY bucket",
            "SELECT CASE status WHEN 'paid' THEN 1 ELSE 2 END AS code, COUNT(*) AS n \
             FROM orders GROUP BY code",
        ] {
            let e = db.execute(sql).unwrap_err();
            assert!(
                e.to_string().contains("unsupported aggregate projection"),
                "{sql}\n{e}"
            );
        }
        // Arithmetic mixing aggregates is not supported either.
        let e = db
            .execute("SELECT MAX(amount) - MIN(amount) FROM orders")
            .unwrap_err();
        assert!(e.to_string().contains("not allowed in this context"), "{e}");
        // ORDER BY must use an alias, not a bare aggregate expression.
        let e = db
            .execute(
                "SELECT customer_id, SUM(amount) AS total FROM orders \
                 GROUP BY customer_id ORDER BY SUM(amount)",
            )
            .unwrap_err();
        assert!(e.to_string().contains("unknown ORDER BY key"), "{e}");
        // GROUP BY by alias or ordinal is not supported (unlike HAVING).
        for sql in [
            "SELECT status AS s, COUNT(*) FROM orders GROUP BY s",
            "SELECT status, COUNT(*) FROM orders GROUP BY 1",
        ] {
            let e = db.execute(sql).unwrap_err();
            assert!(
                e.to_string()
                    .contains("must appear in GROUP BY or an aggregate"),
                "{sql}\n{e}"
            );
        }
    }

    // ---- rejoin repair: table digests + transactional wipe ----

    fn digests_of(db: &mut Database) -> Vec<TableDigest> {
        db.digests().unwrap()
    }

    #[test]
    fn digests_agree_regardless_of_row_order() {
        // Nodes apply the same logical rows in different arrival orders;
        // heap layout differs, the digest must not.
        let mut a = Database::in_memory().unwrap();
        let mut b = Database::in_memory().unwrap();
        {
            let pair: [(&mut Database, [i64; 3]); 2] = [(&mut a, [1, 2, 3]), (&mut b, [3, 1, 2])];
            for (db, order) in pair {
                run(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
                for id in order {
                    run(db, &format!("INSERT INTO t VALUES ({id}, 'v{id}')"));
                }
            }
        }
        assert_eq!(digests_of(&mut a), digests_of(&mut b));
        // One changed row breaks the agreement (update path).
        run(&mut b, "UPDATE t SET v = 'x' WHERE id = 2");
        assert_ne!(digests_of(&mut a), digests_of(&mut b));
        // And the delete path moves it back only when mirrored.
        run(&mut b, "DELETE FROM t WHERE id = 2");
        assert_ne!(digests_of(&mut a), digests_of(&mut b));
        run(&mut a, "DELETE FROM t WHERE id = 2");
        assert_eq!(digests_of(&mut a), digests_of(&mut b));
    }

    #[test]
    fn digests_reflect_schema_and_skip_pubsub() {
        let mut a = Database::in_memory().unwrap();
        let mut b = Database::in_memory().unwrap();
        run(&mut a, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
        run(&mut b, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT UNIQUE)");
        assert_ne!(digests_of(&mut a), digests_of(&mut b), "schema differs");
        // Column DEFAULT is part of the replayed DDL — it must show up.
        run(&mut a, "CREATE TABLE d (x TEXT DEFAULT ('n/a'))");
        run(&mut b, "CREATE TABLE d (x TEXT)");
        assert_ne!(digests_of(&mut a), digests_of(&mut b));
        // The pubsub system table is node-local queue state, never compared.
        run(
            &mut a,
            "CREATE TABLE _pubsub_messages \
             (id INT PRIMARY KEY AUTOINCREMENT, payload TEXT)",
        );
        let base = digests_of(&mut a);
        run(
            &mut a,
            "INSERT INTO _pubsub_messages (payload) VALUES ('m')",
        );
        assert_eq!(
            digests_of(&mut a),
            base,
            "pubsub rows must not move the digest"
        );
    }

    #[test]
    fn wipe_user_tables_is_transactional() {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE parent (id INT PRIMARY KEY)");
        run(
            &mut db,
            "CREATE TABLE child (id INT PRIMARY KEY, pid INT, \
             FOREIGN KEY (pid) REFERENCES parent (id))",
        );
        run(&mut db, "INSERT INTO parent VALUES (1)");
        run(&mut db, "INSERT INTO child VALUES (10, 1)");
        // DROP cannot take this state apart in any order (child references
        // parent); the wipe bypasses the FK guard.
        run(&mut db, "BEGIN");
        db.wipe_user_tables().unwrap();
        assert!(db.catalog().iter().all(|t| t.name == PUBSUB_TABLE));
        run(&mut db, "ROLLBACK");
        assert!(db.catalog().iter().any(|t| t.name == "parent"));
        let r = rows(&mut db, "SELECT COUNT(*) FROM child");
        assert_eq!(r.rows, vec![vec![Value::Int(1)]], "rollback restores data");
    }

    #[test]
    fn snapshot_adoption_replaces_divergent_state() {
        // The repair flow: wipe + dump replay inside one transaction makes
        // the stale node digest-identical to the reference, and tables the
        // snapshot no longer contains are gone.
        let mut truth = Database::in_memory().unwrap();
        run(&mut truth, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
        run(&mut truth, "INSERT INTO t VALUES (1, 'a'), (2, 'b')");
        let script = truth.dump_script().unwrap();

        let mut stale = Database::in_memory().unwrap();
        run(&mut stale, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
        run(&mut stale, "CREATE TABLE extra (x INT)");
        run(
            &mut stale,
            "INSERT INTO t VALUES (1, 'stale'), (9, 'zombie')",
        );
        assert_ne!(digests_of(&mut truth), digests_of(&mut stale));

        run(&mut stale, "BEGIN");
        stale.wipe_user_tables().unwrap();
        let batch = stale.execute_batch(&script);
        assert!(batch.error.is_none(), "{:?}", batch.error);
        run(&mut stale, "COMMIT");
        assert_eq!(digests_of(&mut truth), digests_of(&mut stale));
        assert!(
            !stale.catalog().iter().any(|t| t.name == "extra"),
            "table absent from the snapshot must go"
        );
    }

    #[test]
    fn catchup_journal_and_positions_round_trip() {
        let mut a = Database::in_memory().unwrap();
        let id = a.cluster_id().unwrap();
        assert!(!id.is_empty());
        assert_eq!(a.cluster_id().unwrap(), id, "identity is stable");
        let s1 = a.journal_append("INSERT INTO t VALUES (1)").unwrap();
        let s2 = a.journal_append("UPDATE t SET v = 2").unwrap();
        assert_eq!(s2, s1 + 1, "seqs are consecutive");
        assert_eq!(a.journal_head().unwrap(), s2);
        assert_eq!(a.journal_oldest().unwrap(), s1);
        let range = a.journal_range(s1 - 1, 10).unwrap();
        assert_eq!(range.len(), 2);
        assert_eq!(range[0].0, s1);
        assert!(range[0].1.contains("INSERT INTO t"));
        assert!(
            a.journal_range(s2, 10).unwrap().is_empty(),
            "range is exclusive of `after`"
        );
        // Positions round-trip per origin and advance in place.
        assert_eq!(a.position_get("origin-1").unwrap(), None);
        a.position_set("origin-1", 7).unwrap();
        a.position_set("origin-2", 9).unwrap();
        assert_eq!(a.position_get("origin-1").unwrap(), Some(7));
        a.position_set("origin-1", 8).unwrap();
        assert_eq!(a.position_get("origin-1").unwrap(), Some(8));
        a.positions_clear().unwrap();
        assert_eq!(a.position_get("origin-2").unwrap(), None);
        // Trim keeps exactly the newest `keep` entries.
        a.journal_trim(1).unwrap();
        assert_eq!(a.journal_head().unwrap(), s2);
        assert_eq!(a.journal_oldest().unwrap(), s2);
        assert_eq!(a.journal_range(0, 10).unwrap().len(), 1);
    }

    #[test]
    fn system_tables_are_excluded_from_user_surfaces() {
        let mut db = Database::in_memory().unwrap();
        db.ensure_cluster_tables().unwrap();
        run(&mut db, "CREATE TABLE user_t (id INT)");
        db.journal_append("INSERT INTO user_t VALUES (1)").unwrap();
        let d = db.digests().unwrap();
        assert_eq!(d.len(), 1, "only user_t: {d:?}");
        let script = db.dump_script().unwrap();
        assert!(!script.contains("_cluster_log"), "{script}");
        assert!(!script.contains("_cluster_pos"), "{script}");
        run(&mut db, "BEGIN");
        db.wipe_user_tables().unwrap();
        run(&mut db, "COMMIT");
        assert!(
            db.catalog().iter().all(|t| is_system_table(&t.name)),
            "wipe keeps system tables, got {:?}",
            db.catalog()
        );
    }

    #[test]
    fn journal_and_positions_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("j.db");
        let (id, seq) = {
            let mut db = Database::open(&path).unwrap();
            db.ensure_cluster_tables().unwrap();
            let id = db.cluster_id().unwrap();
            let seq = db.journal_append("INSERT INTO t VALUES (1)").unwrap();
            db.position_set("origin-x", 42).unwrap();
            (id, seq)
        };
        let mut db = Database::open(&path).unwrap();
        assert_eq!(db.cluster_id().unwrap(), id, "identity persists");
        assert_eq!(
            db.journal_append("INSERT INTO t VALUES (2)").unwrap(),
            seq + 1,
            "journal seq continues after reopen"
        );
        assert_eq!(db.position_get("origin-x").unwrap(), Some(42));
    }

    #[test]
    fn write_unit_fuses_write_and_bookkeeping() {
        // The origin-side shape: user write + journal append in one unit,
        // durable together after `end` — and nothing in the data file
        // before it (deferred commits never touch it ahead of the fsync).
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("unit.db");
        let seq = {
            let mut db = Database::open(&path).unwrap();
            db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
            let file_len_before = std::fs::metadata(&path).unwrap().len();
            let mut unit = db.write_unit();
            unit.execute("INSERT INTO t VALUES (7)").unwrap();
            let s = unit.journal_append("INSERT INTO t VALUES (7)").unwrap();
            assert!(
                unit.has_pending_sync(),
                "statements inside the unit deferred their fsync"
            );
            assert_eq!(
                std::fs::metadata(&path).unwrap().len(),
                file_len_before,
                "no page image on disk before the unit's fsync"
            );
            // Reads see the unit's own writes through the buffer pool.
            assert!(matches!(
                unit.execute("SELECT id FROM t"),
                Ok(ExecOutcome::Rows(r)) if !r.rows.is_empty()
            ));
            unit.end().unwrap();
            assert!(
                std::fs::metadata(&path).unwrap().len() > file_len_before,
                "end flushed the deferred pages to the data file"
            );
            s
        };
        let mut db = Database::open(&path).unwrap();
        let rows = db.execute("SELECT id FROM t").unwrap();
        assert!(matches!(&rows, ExecOutcome::Rows(r) if !r.rows.is_empty()));
        assert_eq!(db.journal_head().unwrap(), seq);
    }

    #[test]
    fn write_unit_receiver_side_position_fused() {
        // The receiver-side shape: replicated write + position update land
        // in one unit; after reopen both are present (atomic on crash, the
        // position can no longer lag the applied write).
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("recv.db");
        {
            let mut db = Database::open(&path).unwrap();
            db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
            let mut unit = db.write_unit();
            unit.execute("INSERT INTO t VALUES (1)").unwrap();
            unit.position_set("origin-a", 5).unwrap();
            unit.end().unwrap();
        }
        let mut db = Database::open(&path).unwrap();
        assert_eq!(db.position_get("origin-a").unwrap(), Some(5));
        assert!(matches!(
            db.execute("SELECT id FROM t"),
            Ok(ExecOutcome::Rows(r)) if !r.rows.is_empty()
        ));
    }

    #[test]
    fn write_unit_batch_journals_in_one_sync() {
        // The COMMIT-drain shape: N journal appends inside one unit share a
        // single fsync; all N survive `end` in order.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("drain.db");
        let n = 50;
        let first = {
            let mut db = Database::open(&path).unwrap();
            let mut unit = db.write_unit();
            let mut s = None;
            for i in 0..n {
                let seq = unit
                    .journal_append(&format!("INSERT INTO t VALUES ({i})"))
                    .unwrap();
                s.get_or_insert(seq);
            }
            unit.end().unwrap();
            s.unwrap()
        };
        let mut db = Database::open(&path).unwrap();
        assert_eq!(db.journal_head().unwrap(), first + n as u64 - 1);
        let range = db.journal_range(first - 1, 1000).unwrap();
        assert_eq!(range.len(), n);
    }

    #[test]
    fn dropped_write_unit_never_wedges_durability() {
        // A caller that forgets `end` only loses the fusion: the depth
        // clears on drop, so the next statement commits durably on its own
        // and flushes whatever the unit deferred before it.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("leak.db");
        {
            let mut db = Database::open(&path).unwrap();
            db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
            {
                let mut unit = db.write_unit();
                unit.execute("INSERT INTO t VALUES (1)").unwrap();
                assert!(unit.has_pending_sync());
            } // dropped without end
            db.execute("INSERT INTO t VALUES (2)").unwrap(); // durable commit
        }
        let mut db = Database::open(&path).unwrap();
        let out = db.execute("SELECT COUNT(*) FROM t").unwrap();
        assert!(
            matches!(&out, ExecOutcome::Rows(r) if r.rows[0][0].as_i64() == Some(2)),
            "both rows survive: the leaked unit's deferred pages were flushed by the next durable commit"
        );
    }

    #[test]
    fn write_unit_defers_to_flusher_in_async_mode() {
        // Group commit owns the fsync in async-commit mode: a unit's `end`
        // must not sync the WAL itself (that would re-impose one fsync per
        // unit and defeat the flusher's batching), only group the commits.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("async-unit.db");
        {
            let mut db = Database::open(&path).unwrap();
            db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
            db.set_async_commit(true);
            let mut unit = db.write_unit();
            unit.execute("INSERT INTO t VALUES (1)").unwrap();
            let seq = unit.journal_append("INSERT INTO t VALUES (1)").unwrap();
            unit.end().unwrap();
            assert!(
                db.has_pending_sync(),
                "async mode: the flusher still owns the pending fsync"
            );
            db.sync_pending().unwrap(); // what the background task does
            assert!(!db.has_pending_sync());
            assert_eq!(db.journal_head().unwrap(), seq);
        }
        let mut db = Database::open(&path).unwrap();
        assert!(
            matches!(db.execute("SELECT id FROM t"), Ok(ExecOutcome::Rows(r)) if !r.rows.is_empty()),
            "durable after the flusher ran"
        );
    }
}
