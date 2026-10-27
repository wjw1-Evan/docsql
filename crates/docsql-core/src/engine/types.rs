use super::*;
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
pub(super) const CATALOG_MAGIC: &[u8; 8] = b"DOCSCAT2";
pub(super) const CATALOG_HDR: usize = 16;
pub(super) const CATALOG_CHUNK: usize = PAGE_SIZE - CATALOG_HDR;

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

/// Byte cap for one `journal_range` catch-up batch: large documents make a
/// fixed row count an unbounded memory commitment on the serving node.
pub(super) const JOURNAL_RANGE_MAX_BYTES: usize = 8 * 1024 * 1024;

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

/// Upper bound for LPAD/RPAD target lengths (in characters). The padded
/// result goes into a document whose hard cap is `MAX_DOC_SIZE`, so this
/// is pure allocation-bomb armor: an unbounded `String::with_capacity`
/// used to abort the process on a single `SELECT LPAD('a', 2^62)`.
pub(super) const MAX_PAD_LENGTH: usize = 1 << 20;

/// Upper bound for the number of grouping sets one GROUP BY may expand to
/// (ROLLUP/CUBE/GROUPING SETS combine as a cartesian product — three
/// 12-element CUBEs used to multiply into an astronomic with_capacity).
pub(super) const MAX_GROUPING_SETS: usize = 16_384;

/// True for the engine-managed system tables: excluded from user-facing
/// catalogs, digests, dumps and snapshot wipes.
pub fn is_system_table(name: &str) -> bool {
    name == PUBSUB_TABLE
        || name == CLUSTER_LOG_TABLE
        || name == CLUSTER_POS_TABLE
        || name == CLUSTER_ID_TABLE
}

/// Internal tables in the broad sense: engine system tables (excluded from
/// replication — digests/snapshots never carry them) PLUS the user/role
/// storage tables, which DO replicate (they are regular data) but are
/// shielded from direct SQL writes and hidden from the object browser.
/// Display filters and the server's write gate use this predicate;
/// replication-exclusion logic must keep using `is_system_table`.
pub fn is_internal_table(name: &str) -> bool {
    is_system_table(name) || crate::useradmin::is_user_table(name)
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

/// Statement-local CTE table (`WITH name AS (SELECT …)` materialized rows),
/// threaded through the SELECT execution chain so it can run on `&Database`
/// (MVCC stage A concurrent readers).
pub(super) type Ctes = std::collections::BTreeMap<String, Vec<Object>>;

pub(crate) fn err<T>(msg: impl Into<String>) -> Result<T> {
    Err(SqlError::Message(msg.into()))
}

/// Cooperative statement deadline for the server-side statement timeout
/// (`DOCSQL_STATEMENT_TIMEOUT_MS`). The engine is synchronous inside one
/// global writer — nothing can preempt a runaway statement from the
/// outside — so long row loops (table scans, nested-loop joins, UPDATE/
/// DELETE matching) check in here instead. The wall clock is sampled once
/// per [`StmtDeadline::TICK_BATCH`] iterations; a fired deadline surfaces
/// as a normal SQL error, rolling the statement back like any failure.
/// Mutex-based (not Cell): `Database: Sync` is required once the server
/// shares `&Database` across threads under the read tier.
#[derive(Default)]
pub struct StmtDeadline {
    inner: std::sync::Mutex<(Option<std::time::Instant>, u64)>,
}

impl StmtDeadline {
    pub fn set(&self, deadline: Option<std::time::Instant>) {
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        *g = (deadline, 0);
    }

    /// Call once per row iteration from long loops. Engine-global, not per
    /// connection — the caller arms and clears it around exactly one
    /// statement (see [`Database::set_statement_deadline`]).
    pub fn check(&self) -> Result<()> {
        const TICK_BATCH: u64 = 1024;
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        g.1 = (g.1 + 1) % TICK_BATCH;
        if g.1 != 0 {
            return Ok(());
        }
        if let Some(d) = g.0 {
            if std::time::Instant::now() >= d {
                return err("statement timeout exceeded (DOCSQL_STATEMENT_TIMEOUT_MS)");
            }
        }
        Ok(())
    }
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
/// Process-wide parsed-statement cache (`SQL text → classified AST`).
/// Bounded two ways: oversized statements never enter (a multi-MB batch
/// INSERT would churn the map), and a full map clears wholesale — a
/// workload with more distinct texts than the cap simply falls back to
/// always-parse, the previous behavior. Errors are not cached.
pub(super) fn stmt_cache(
) -> &'static std::sync::Mutex<std::collections::HashMap<String, ParsedStatement>> {
    static CACHE: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<String, ParsedStatement>>,
    > = std::sync::OnceLock::new();
    CACHE.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

/// Second-sight filter behind the statement cache: hashes of texts that
/// parsed once (cap-bounded, cleared wholesale). A repeat sighting promotes
/// the text into the cache — see [`Database::parse_classified`].
pub(super) fn stmt_seen() -> &'static std::sync::Mutex<std::collections::HashSet<u64>> {
    static SEEN: std::sync::OnceLock<std::sync::Mutex<std::collections::HashSet<u64>>> =
        std::sync::OnceLock::new();
    SEEN.get_or_init(|| std::sync::Mutex::new(std::collections::HashSet::new()))
}

#[derive(Clone)]
pub struct ParsedStatement {
    pub stmt: AnyStmt,
    pub tx: TxControl,
    pub is_write: bool,
    /// Original statement text. Wall-clock folding re-parses a rewritten
    /// copy of this (journaled writes must not carry NOW()/SYSDATE()) and
    /// the server's queue path holds a parsed statement long before the
    /// write lock — the text rides along so the fold is available at every
    /// execute_parsed call site.
    pub source: String,
}
/// A parsed statement: standard SQL (`sqlparser` AST) or one of the
/// hand-parsed user-management statements (sqlparser 0.62 does not accept
/// `CREATE USER … PASSWORD` / `GRANT role TO user`). The SQL AST is boxed —
/// it dwarfs the user-admin shape by two orders of magnitude.
#[derive(Debug, Clone)]
pub enum AnyStmt {
    Sql(Box<Statement>),
    UserAdmin(crate::useradmin::UserAdminStmt),
    /// REFRESH MATERIALIZED VIEW (hand-parsed: sqlparser 0.62's generic
    /// dialect has no REFRESH statement). Carries the view name.
    RefreshMatview(String),
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
    /// Materialized view (CREATE MATERIALIZED VIEW): table reads, writes
    /// refused, REFRESH recomputes.
    pub matview: bool,
}

/// One CREATE INDEX definition as recorded in the catalog.
///
/// `columns.len() >= 1`; `len() > 1` is a composite index whose B+ tree keys
/// are `Value::Array` over the columns in order (ordered by
/// `Value::cmp_values` element-wise — no separate byte-order encoding).
/// `columns[0]` mirrors the legacy `column` field for tooling compatibility.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexDef {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
    /// JSON path index (design 005): the single column in `columns` holds
    /// JSON text; tree keys are values extracted at this path via the same
    /// lookup `JSON_EXTRACT` uses. None = a plain column index.
    pub path: Option<String>,
    /// Partial index (CREATE INDEX … WHERE): the predicate as SQL text. The
    /// tree holds ONLY the rows satisfying it, so every probe over the tree
    /// must first prove the query's WHERE implies the predicate
    /// ([`query_implies_pred`]) — an ungated probe would silently drop the
    /// pred-failing rows. Row-local only (no subqueries/aggregates),
    /// validated at CREATE time.
    pub pred: Option<String>,
}

impl IndexDef {
    /// Catalog/tuple-compat view: first column.
    pub fn column(&self) -> &str {
        &self.columns[0]
    }
}

/// One index as exposed to tooling (catalog snapshot): CREATE INDEX
/// definitions plus the derived `sqlite_autoindex_*` constraint indexes.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexInfo {
    pub name: String,
    /// First indexed column (kept for tooling compatibility; `columns`
    /// carries the full list for composite indexes).
    pub column: String,
    /// Full column list in key order (single-column indexes: one entry).
    pub columns: Vec<String>,
    pub unique: bool,
    /// PRIMARY KEY / UNIQUE constraint index, auto-created with the table
    /// (SQLite-style `sqlite_autoindex_*` name). Derived, never persisted,
    /// and not droppable/editable through DROP INDEX / CREATE INDEX.
    pub auto: bool,
    /// Partial-index predicate text (None for full indexes).
    pub pred: Option<String>,
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
/// Catalog state captured at BEGIN / SAVEPOINT: table metas are shared
/// behind `Arc`, so a snapshot clones pointers (and each later mutation
/// deep-copies just the one table it touches), not the database. Document
/// content is restored through the pager's page undo journal — the old
/// whole-database deep copy made every BEGIN an O(database) allocation.
#[derive(Clone)]
pub(super) struct TableSnapshot {
    pub(super) tables: std::collections::BTreeMap<String, std::sync::Arc<TableMeta>>,
    pub(super) autoinc: std::collections::HashMap<String, i64>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct TableMeta {
    /// Table name, mirroring the catalog key: constraint errors carry it so
    /// multi-table statements say WHICH table failed. Rebuilt on load (the
    /// catalog serializes the key itself, so this is never part of the
    /// on-disk format) and empty only for synthesized/test metas.
    pub(super) name: String,
    pub(super) columns: Vec<String>,
    pub(super) pages: Vec<u32>,
    pub(super) primary_key: Option<String>,
    pub(super) unique: Vec<String>,
    pub(super) not_null: Vec<String>,
    pub(super) autoinc: Option<String>,
    /// Auto-generated GUID column (GUID/UUID/UNIQUEIDENTIFIER/UUIDV7 type
    /// plus AUTOINCREMENT/AUTOGENERATE): INSERT fills a time-ordered UUIDv7
    /// when the value is omitted or NULL.
    pub(super) autoguid: Option<String>,
    /// Index names registered on this table (catalog-level v1; B+ tree
    /// backing arrives with the index-integration milestone).
    pub(super) indexes: Vec<String>,
    /// CREATE INDEX definitions: name + ordered columns + uniqueness. Drives
    /// DROP INDEX cleanup (lifting UNIQUE enforced by a dropped unique index)
    /// and the sqlite_master index rows. Composite indexes key their tree by
    /// the index name; single-column indexes key by the column name (legacy
    /// layout, zero-migration for deployed databases).
    pub(super) index_defs: Vec<IndexDef>,
    /// Columns declared UNIQUE via CREATE TABLE constraints. Dropping a
    /// UNIQUE INDEX only lifts meta.unique for columns NOT in this list.
    pub(super) constraint_unique: Vec<String>,
    /// Column DEFAULT expressions as SQL text: (column, expr).
    pub(super) defaults: Vec<(String, String)>,
    /// CHECK constraint expressions as SQL text (column- or table-level).
    pub(super) checks: Vec<String>,
    /// Foreign keys: (local column, referenced table, referenced column).
    pub(super) foreign_keys: Vec<(String, String, String)>,
    /// Indexed column -> B+ tree root page. PRIMARY KEY and UNIQUE columns
    /// always get trees (duplicates are enforced through them); CREATE
    /// INDEX adds non-unique trees. Lookups over these columns skip the
    /// heap scan.
    pub(super) index_roots: std::collections::BTreeMap<String, u32>,
    /// Recycled overflow-chain page ids (documents larger than one page;
    /// see heap.rs / docs/design/002): reused by the next oversized insert.
    /// Persisted in the catalog; empty for tables without overflow history.
    pub(super) overflow_free: Vec<u32>,
    /// Sticky marker: some stored document carries a literal dotted field
    /// name ("a.b" as ONE key). Eval resolves a qualified reference `a.b`
    /// by that exact key first, so on such a table a qualifier-stripped
    /// index probe is not a superset — probes and index-ordered windows
    /// must refuse stripped names while it is set. Written once when the
    /// first dotted field lands; never cleared (conservative is correct).
    pub(super) dotted_keys: bool,
    /// User view: the AS query text (CREATE VIEW). `None` for real tables.
    /// Views live in the same catalog namespace (one name, one object); a
    /// SELECT naming one expands to this query's rows. No storage, no
    /// indexes, no writes.
    pub(super) view_sql: Option<String>,
    /// Materialized view: the AS query text (CREATE MATERIALIZED VIEW).
    /// Unlike `view_sql` this object HAS storage — a real table materialized
    /// from the query at CREATE/REFRESH time, readable (and indexable) like
    /// any table, but not writable (REFRESH MATERIALIZED VIEW recomputes
    /// it). The stored query is determinism-validated at CREATE: REFRESH
    /// replays it on every node, and a RAND()/wall-clock result would
    /// silently fork the cluster.
    pub(super) matview_sql: Option<String>,
}

impl TableMeta {
    /// True when this catalog entry is a user view (CREATE VIEW), not a
    /// stored table.
    pub(crate) fn is_view(&self) -> bool {
        self.view_sql.is_some()
    }

    /// True when this catalog entry is a materialized view: real storage,
    /// table reads, writes refused (REFRESH recomputes it).
    pub(crate) fn is_matview(&self) -> bool {
        self.matview_sql.is_some()
    }
}

impl TableMeta {
    /// Heap view over this table's storage: live pages plus the recycled
    /// overflow-chain free list.
    pub(super) fn heap_of(&self) -> Heap {
        Heap {
            pages: self.pages.clone(),
            overflow_free: self.overflow_free.clone(),
            dropped: Vec::new(),
        }
    }

    /// Columns making up the index behind `root_key`: the definition's
    /// ordered column list for a named (possibly composite) index, or the
    /// key itself for legacy single-column entries (constraint trees and
    /// single-column CREATE INDEX key their tree by the column name).
    pub(super) fn index_columns_of(&self, root_key: &str) -> Vec<String> {
        match self.index_defs.iter().find(|d| d.name == root_key) {
            Some(d) => d.columns.clone(),
            // Legacy/constraint single-column tree: the root key *is* the
            // column. Any other key is an orphaned composite tree (its index
            // was dropped, the def is gone): returning `[root_key]` would
            // make it look like a scalar column tree and mix scalar keys
            // into an Array-keyed tree.
            None => {
                if self.columns.iter().any(|c| c == root_key)
                    || self.primary_key.as_deref() == Some(root_key)
                    || self.unique.iter().any(|c| c == root_key)
                {
                    vec![root_key.to_string()]
                } else {
                    Vec::new()
                }
            }
        }
    }

    /// True when the tree behind `root_key` enforces uniqueness: a PK or
    /// UNIQUE constraint column (single-column constraint tree), or a
    /// `CREATE UNIQUE INDEX` definition (possibly composite).
    pub(super) fn root_key_unique(&self, root_key: &str) -> bool {
        if self.primary_key.as_deref() == Some(root_key)
            || self.unique.contains(&root_key.to_string())
        {
            return true;
        }
        self.index_defs
            .iter()
            .any(|d| d.name == root_key && d.unique)
    }

    /// Qualify a column with the table name for constraint errors
    /// (`users.email`); synthesized metas without a name stay unqualified.
    pub(super) fn qual(&self, col: &str) -> String {
        if self.name.is_empty() {
            col.to_string()
        } else {
            format!("{}.{}", self.name, col)
        }
    }

    /// Validate a document against declared constraints.
    pub(super) fn check(&self, doc: &Object) -> Result<()> {
        for col in &self.not_null {
            if matches!(doc.get(col), Some(Value::Null) | None) {
                return err(format!("NOT NULL constraint failed: {}", self.qual(col)));
            }
        }
        for text in &self.checks {
            let e = parse_expr_text(text)?;
            // SQL three-valued CHECK semantics: only FALSE rejects; unknown
            // (a NULL operand) passes. A syntactic "any NULL column ref"
            // shortcut is wrong in both directions (`a BETWEEN …` with NULL
            // was rejected, `a > 0 AND b > 0` with one NULL accepted a
            // false conjunct), so the expression is evaluated with proper
            // unknown propagation.
            if matches!(check_expr_outcome(&e, doc)?, CheckOutcome::Fail) {
                return err(format!("CHECK constraint failed on {}: {text}", self.name));
            }
        }
        Ok(())
    }

    /// Enforce PRIMARY KEY / UNIQUE across the whole (rewritten) doc set.
    pub(super) fn check_unique(&self, docs: &[Object]) -> Result<()> {
        for col in self.primary_key.iter().chain(self.unique.iter()) {
            let mut seen = std::collections::BTreeSet::new();
            for doc in docs {
                if let Some(v) = doc.get(col) {
                    if matches!(v, Value::Null) {
                        continue;
                    }
                    let key = encode::encode_to_vec(v).map_err(SqlError::Encode)?;
                    if !seen.insert(key) {
                        return err(format!("UNIQUE constraint failed: {}", self.qual(col)));
                    }
                }
            }
        }
        Ok(())
    }
}

/// Parse a standalone SQL expression (constraint/DEFAULT bodies stored in
/// the catalog as text).
pub fn parse_expr_text(s: &str) -> Result<SqlExpr> {
    let mut parser = Parser::new(&GenericDialect {})
        .try_with_sql(s)
        .map_err(|e| SqlError::Parse(e.to_string()))?;
    let expr = parser
        .parse_expr()
        .map_err(|e| SqlError::Parse(e.to_string()))?;
    // Trailing input is corruption of the stored body, not a suffix to
    // ignore: a row-filter predicate sliced as `region = 'east'; anything`
    // would otherwise be stored whole while every consumer evaluates only
    // the prefix.
    if parser.peek_token().token != sqlparser::tokenizer::Token::EOF {
        return Err(SqlError::Parse("trailing input after expression".into()));
    }
    Ok(expr)
}

/// Canonical enumeration of the direct child expressions of every `SqlExpr`
/// variant the engine can evaluate. Subquery-bearing nodes (Subquery /
/// Exists / InSubquery / AnyOp / AllOp) are excluded on purpose: every
/// tree-wide walker treats those nodes specially. Identifiers, literals and
/// other leaves fall through and yield nothing. The macro body uses plain
/// place matches only, so it expands identically over `&SqlExpr` and
/// `&mut SqlExpr`.
macro_rules! each_child_expr {
    ($e:expr, $($f:tt)+) => {
        match $e {
            SqlExpr::IsFalse(c)
            | SqlExpr::IsNotFalse(c)
            | SqlExpr::IsTrue(c)
            | SqlExpr::IsNotTrue(c)
            | SqlExpr::IsNull(c)
            | SqlExpr::IsNotNull(c)
            | SqlExpr::IsUnknown(c)
            | SqlExpr::IsNotUnknown(c)
            | SqlExpr::Nested(c) => $($f)*(c),
            SqlExpr::IsNormalized { expr: c, .. }
            | SqlExpr::Collate { expr: c, .. }
            | SqlExpr::Ceil { expr: c, .. }
            | SqlExpr::Floor { expr: c, .. }
            | SqlExpr::Extract { expr: c, .. }
            | SqlExpr::Cast { expr: c, .. }
            | SqlExpr::Prefixed { value: c, .. }
            | SqlExpr::Named { expr: c, .. } => $($f)*(c),
            SqlExpr::CompoundFieldAccess { root, .. } => $($f)*(root),
            SqlExpr::JsonAccess { value, .. } => $($f)*(value),
            SqlExpr::Convert { expr, styles, .. } => {
                $($f)*(expr);
                for c in styles {
                    $($f)*(c);
                }
            }
            SqlExpr::AtTimeZone { timestamp, time_zone } => {
                $($f)*(timestamp);
                $($f)*(time_zone);
            }
            SqlExpr::Position { expr, r#in } => {
                $($f)*(expr);
                $($f)*(r#in);
            }
            SqlExpr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                $($f)*(expr);
                match substring_from {
                    Some(c) => $($f)*(c),
                    None => {}
                }
                match substring_for {
                    Some(c) => $($f)*(c),
                    None => {}
                }
            }
            SqlExpr::Trim {
                expr,
                trim_what,
                trim_characters,
                ..
            } => {
                $($f)*(expr);
                match trim_what {
                    Some(c) => $($f)*(c),
                    None => {}
                }
                match trim_characters {
                    Some(chars) => {
                        for c in chars {
                            $($f)*(c);
                        }
                    }
                    None => {}
                }
            }
            SqlExpr::Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => {
                $($f)*(expr);
                $($f)*(overlay_what);
                $($f)*(overlay_from);
                match overlay_for {
                    Some(c) => $($f)*(c),
                    None => {}
                }
            }
            SqlExpr::BinaryOp { left, right, .. } => {
                $($f)*(left);
                $($f)*(right);
            }
            SqlExpr::UnaryOp { expr, .. } => $($f)*(expr),
            SqlExpr::Between {
                expr, low, high, ..
            } => {
                $($f)*(expr);
                $($f)*(low);
                $($f)*(high);
            }
            SqlExpr::IsDistinctFrom(l, r) | SqlExpr::IsNotDistinctFrom(l, r) => {
                $($f)*(l);
                $($f)*(r);
            }
            SqlExpr::Like { expr, pattern, .. }
            | SqlExpr::ILike { expr, pattern, .. }
            | SqlExpr::SimilarTo { expr, pattern, .. }
            | SqlExpr::RLike { expr, pattern, .. } => {
                $($f)*(expr);
                $($f)*(pattern);
            }
            SqlExpr::InList { expr, list, .. } => {
                $($f)*(expr);
                for c in list {
                    $($f)*(c);
                }
            }
            SqlExpr::InUnnest { expr, array_expr, .. } => {
                $($f)*(expr);
                $($f)*(array_expr);
            }
            SqlExpr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                match operand {
                    Some(c) => $($f)*(c),
                    None => {}
                }
                for w in conditions {
                    let sqlparser::ast::CaseWhen {
                        condition, result, ..
                    } = w;
                    $($f)*(condition);
                    $($f)*(result);
                }
                match else_result {
                    Some(c) => $($f)*(c),
                    None => {}
                }
            }
            SqlExpr::Function(func) => {
                let sqlparser::ast::Function { args, filter, .. } = func;
                match args {
                    sqlparser::ast::FunctionArguments::List(list) => {
                        let sqlparser::ast::FunctionArgumentList { args, .. } = list;
                        for a in args {
                            match a {
                                sqlparser::ast::FunctionArg::Unnamed(
                                    sqlparser::ast::FunctionArgExpr::Expr(c),
                                ) => $($f)*(c),
                                sqlparser::ast::FunctionArg::Named {
                                    arg: sqlparser::ast::FunctionArgExpr::Expr(c),
                                    ..
                                } => $($f)*(c),
                                sqlparser::ast::FunctionArg::ExprNamed {
                                    arg: sqlparser::ast::FunctionArgExpr::Expr(c),
                                    ..
                                } => $($f)*(c),
                                _ => {}
                            }
                        }
                    }
                    _ => {}
                }
                match filter {
                    Some(c) => $($f)*(c),
                    None => {}
                }
            }
            SqlExpr::GroupingSets(groups) | SqlExpr::Cube(groups) | SqlExpr::Rollup(groups) => {
                for g in groups {
                    for c in g {
                        $($f)*(c);
                    }
                }
            }
            SqlExpr::Tuple(items) => {
                for c in items {
                    $($f)*(c);
                }
            }
            SqlExpr::Struct { values, .. } => {
                for c in values {
                    $($f)*(c);
                }
            }
            SqlExpr::Dictionary(fields) => {
                for field in fields {
                    let sqlparser::ast::DictionaryField { value, .. } = field;
                    $($f)*(value);
                }
            }
            _ => {}
        }
    };
}

pub(crate) fn child_exprs(e: &SqlExpr) -> Vec<&SqlExpr> {
    let mut out: Vec<&SqlExpr> = Vec::new();
    each_child_expr!(e, out.push);
    out
}

pub(crate) fn child_exprs_mut(e: &mut SqlExpr) -> Vec<&mut SqlExpr> {
    let mut out: Vec<&mut SqlExpr> = Vec::new();
    each_child_expr!(e, out.push);
    out
}

/// True when the expression calls a non-deterministic function — the
/// wall-clock family (NOW/SYSDATE/CURRENT_TIMESTAMP/GETDATE/…) or the
/// random-id family (NEWID/NEWSEQUENTIALID): its value differs between
/// nodes and instants, so a stored DEFAULT or CHECK carrying it must be
/// resolved per row on the writing node and shipped as a literal (red
/// line: non-deterministic generated values are fixed by the writer).
pub(crate) fn expr_calls_wall_clock(e: &SqlExpr) -> bool {
    match e {
        SqlExpr::Function(f) => {
            let n = f.name.to_string().to_ascii_lowercase();
            if matches!(
                n.as_str(),
                "now"
                    | "now_ms"
                    | "sysdate"
                    | "current_timestamp"
                    | "getdate"
                    | "getutcdate"
                    | "sysdatetime"
                    | "sysutcdatetime"
                    | "sysdatetimeoffset"
                    | "newid"
                    | "newsequentialid"
                    | "rand"
            ) {
                return true;
            }
            child_exprs(e).into_iter().any(expr_calls_wall_clock)
        }
        // A wall-clock call inside a CASE branch or a CAST/FLOOR/… wrapper
        // (e.g. `CAST(GETDATE() AS TIMESTAMP)`, which the text-level fold
        // cannot reduce) must still trip this check — child_exprs is the
        // one shared traversal, so wrappers cannot slip past again.
        _ => child_exprs(e).into_iter().any(expr_calls_wall_clock),
    }
}

/// Three-valued outcome of a CHECK predicate (SQL: FALSE rejects, TRUE and
/// UNKNOWN pass).
pub(super) enum CheckOutcome {
    Pass,
    Fail,
    Unknown,
}

/// Evaluate a CHECK predicate with SQL three-valued logic. Comparisons and
/// boolean operators propagate UNKNOWN structurally; predicates the engine
/// evaluates two-valued (BETWEEN/LIKE/IN/functions) fall back to the
/// null-reference walker so a NULL operand stays unknown instead of being
/// read as FALSE.
pub(super) fn check_expr_outcome(e: &SqlExpr, doc: &Object) -> Result<CheckOutcome> {
    use CheckOutcome::*;
    let eval_boolish = |e: &SqlExpr, doc: &Object| -> Result<CheckOutcome> {
        // Generic fallback: NULL reference → unknown, else the engine's
        // two-valued evaluation (non-boolean results keep the historical
        // lenient pass).
        if expr_has_null_ref(e, doc) {
            return Ok(Unknown);
        }
        Ok(match eval_expr(e, doc)? {
            Value::Bool(false) => Fail,
            _ => Pass,
        })
    };
    match e {
        SqlExpr::Nested(inner) => check_expr_outcome(inner, doc),
        SqlExpr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Not,
            expr,
        } => Ok(match check_expr_outcome(expr, doc)? {
            Pass => Fail,
            Fail => Pass,
            Unknown => Unknown,
        }),
        SqlExpr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let l = check_expr_outcome(left, doc)?;
            let r = check_expr_outcome(right, doc)?;
            Ok(match (l, r) {
                (Fail, _) | (_, Fail) => Fail,
                (Pass, Pass) => Pass,
                _ => Unknown,
            })
        }
        SqlExpr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => {
            let l = check_expr_outcome(left, doc)?;
            let r = check_expr_outcome(right, doc)?;
            Ok(match (l, r) {
                (Pass, _) | (_, Pass) => Pass,
                (Fail, Fail) => Fail,
                _ => Unknown,
            })
        }
        SqlExpr::BinaryOp { left, op, right } => {
            let l = eval_expr(left, doc)?;
            let r = eval_expr(right, doc)?;
            if matches!(l, Value::Null) || matches!(r, Value::Null) {
                return Ok(Unknown);
            }
            Ok(match binop(l, op, r)? {
                Value::Bool(false) => Fail,
                Value::Bool(true) => Pass,
                Value::Null => Unknown,
                _ => Pass,
            })
        }
        SqlExpr::IsNull(_)
        | SqlExpr::IsNotNull(_)
        | SqlExpr::IsDistinctFrom(..)
        | SqlExpr::IsNotDistinctFrom(..) => Ok(match eval_expr(e, doc)? {
            Value::Bool(false) => Fail,
            _ => Pass,
        }),
        SqlExpr::Between { .. }
        | SqlExpr::Like { .. }
        | SqlExpr::ILike { .. }
        | SqlExpr::InList { .. } => eval_boolish(e, doc),
        _ => eval_boolish(e, doc),
    }
}

/// True when any column reference in `e` resolves to NULL in `doc` (CHECK
/// semantics: unknown, so the constraint passes).
pub(super) fn expr_references_ident(e: &SqlExpr, name: &str) -> bool {
    match e {
        SqlExpr::Identifier(i) => i.value == name,
        SqlExpr::CompoundIdentifier(parts) => parts.last().is_some_and(|p| p.value == name),
        SqlExpr::Nested(inner) => expr_references_ident(inner, name),
        SqlExpr::UnaryOp { expr, .. } => expr_references_ident(expr, name),
        SqlExpr::BinaryOp { left, right, .. } => {
            expr_references_ident(left, name) || expr_references_ident(right, name)
        }
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            expr_references_ident(expr, name)
                || expr_references_ident(low, name)
                || expr_references_ident(high, name)
        }
        SqlExpr::InList { expr, list, .. } => {
            expr_references_ident(expr, name) || list.iter().any(|i| expr_references_ident(i, name))
        }
        SqlExpr::Cast { expr, .. } => expr_references_ident(expr, name),
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand
                .as_deref()
                .is_some_and(|o| expr_references_ident(o, name))
                || conditions.iter().any(|w| {
                    expr_references_ident(&w.condition, name)
                        || expr_references_ident(&w.result, name)
                })
                || else_result
                    .as_deref()
                    .is_some_and(|r| expr_references_ident(r, name))
        }
        SqlExpr::Function(f) => {
            if let sqlparser::ast::FunctionArguments::List(list) = &f.args {
                list.args.iter().any(|a| match a {
                    sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(inner),
                    ) => expr_references_ident(inner, name),
                    _ => false,
                })
            } else {
                false
            }
        }
        _ => false,
    }
}

pub(super) fn expr_has_null_ref(e: &SqlExpr, doc: &Object) -> bool {
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
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            expr_has_null_ref(expr, doc)
                || expr_has_null_ref(low, doc)
                || expr_has_null_ref(high, doc)
        }
        SqlExpr::InList { expr, list, .. } => {
            expr_has_null_ref(expr, doc) || list.iter().any(|i| expr_has_null_ref(i, doc))
        }
        SqlExpr::Cast { expr, .. } => expr_has_null_ref(expr, doc),
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand
                .as_deref()
                .is_some_and(|o| expr_has_null_ref(o, doc))
                || conditions.iter().any(|w| {
                    expr_has_null_ref(&w.condition, doc) || expr_has_null_ref(&w.result, doc)
                })
                || else_result
                    .as_deref()
                    .is_some_and(|r| expr_has_null_ref(r, doc))
        }
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
pub(super) fn schema_hash(meta: &TableMeta) -> u64 {
    let mut h = DefaultHasher::new();
    meta.view_sql.hash(&mut h);
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

/// Owned catalog view for a guardless read (MVCC stage B): cloned under the
/// server's brief read lock as `Arc` bumps, then held outside every database
/// lock while the statement runs. Writers mutating a table's meta use
/// `Arc::make_mut`, so the deep clone only happens while a detached reader
/// actually holds the table's entry.
#[derive(Clone, Default)]
pub(crate) struct CatalogSnapshot(
    pub(super) std::collections::BTreeMap<String, std::sync::Arc<TableMeta>>,
);
