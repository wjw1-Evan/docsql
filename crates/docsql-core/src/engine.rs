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

mod catalog;
mod classify;
mod ddl;
mod dml;
mod execute;
mod journal;
