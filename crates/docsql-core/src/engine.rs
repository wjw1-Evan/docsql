//! SQL engine: catalog + executor over document heaps.
//!
//! The catalog itself is a document stored on page 1:
//! `{ "tables": { "<name>": { "columns": [names...], "pages": [...] } } }`
//! so metadata rides the same WAL/pager machinery as data.

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

pub const CATALOG_PAGE: u32 = 1;

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

#[derive(Debug, Clone, PartialEq)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

#[derive(Debug, Clone, Default)]
struct TableMeta {
    columns: Vec<String>,
    pages: Vec<u32>,
    primary_key: Option<String>,
    unique: Vec<String>,
    not_null: Vec<String>,
}

impl TableMeta {
    /// Validate a document against declared constraints.
    fn check(&self, doc: &Object) -> Result<()> {
        for col in &self.not_null {
            if matches!(doc.get(col), Some(Value::Null) | None) {
                return err(format!("NOT NULL constraint failed: {col}"));
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

pub struct Database {
    pager: Pager,
    tables: std::collections::BTreeMap<String, TableMeta>,
    /// Session transaction snapshot: full table state at BEGIN. Rollback
    /// restores it; commit just discards it (durability is the WAL's job).
    tx_snapshot: Option<std::collections::BTreeMap<String, (TableMeta, Vec<Object>)>>,
}

impl Database {
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
        let page = pager.read_page(CATALOG_PAGE)?.to_vec();
        if page.iter().any(|&b| b != 0) {
            let (v, _) = encode::decode_prefix(&page)?;
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
                            tables.insert(
                                name.clone(),
                                TableMeta {
                                    columns,
                                    pages,
                                    primary_key,
                                    unique,
                                    not_null,
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
            tx_snapshot: None,
        })
    }

    pub fn in_memory() -> Result<Database> {
        let dir = tempfile::tempdir().map_err(|e| SqlError::Message(e.to_string()))?;
        // Leak: process-lifetime scratch DB, fine for embedded use/tests.
        let path = dir.keep().join("mem.db");
        Database::open(&path)
    }

    fn save_catalog(&mut self) -> Result<()> {
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
            tables.insert(name.clone(), Value::Object(m));
        }
        let mut cat = Object::new();
        cat.insert("tables".into(), Value::Object(tables));
        let bytes = encode::encode_to_vec(&Value::Object(cat))?;
        let mut tx = self.pager.begin_tx();
        let mut page = vec![0u8; PAGE_SIZE];
        page[..bytes.len()].copy_from_slice(&bytes);
        self.pager.write_page(&mut tx, CATALOG_PAGE, 0, &page)?;
        self.pager.commit_tx(tx)?;
        Ok(())
    }

    fn snapshot_all(&mut self) -> std::collections::BTreeMap<String, (TableMeta, Vec<Object>)> {
        let names: Vec<String> = self.tables.keys().cloned().collect();
        let mut snap = std::collections::BTreeMap::new();
        for name in names {
            let meta = self.tables.get(&name).cloned().unwrap_or_default();
            let docs = self.table_docs(&name).unwrap_or_default();
            snap.insert(name, (meta, docs));
        }
        snap
    }

    fn rollback_tx(&mut self) -> Result<ExecOutcome> {
        let Some(snap) = self.tx_snapshot.take() else {
            return err("no transaction in progress");
        };
        self.tables.clear();
        for (name, (mut meta, docs)) in snap {
            let pages = self.rewrite_table(&name, docs)?;
            meta.pages = pages;
            self.tables.insert(name, meta);
        }
        self.save_catalog()?;
        Ok(ExecOutcome::Affected(0))
    }

    /// True when a session transaction is open.
    pub fn in_transaction(&self) -> bool {
        self.tx_snapshot.is_some()
    }

    /// Execute exactly one SQL statement.
    pub fn execute(&mut self, sql: &str) -> Result<ExecOutcome> {
        let stmts = Parser::parse_sql(&GenericDialect {}, sql)
            .map_err(|e| SqlError::Parse(e.to_string()))?;
        match stmts.len() {
            0 => err("empty statement"),
            1 => self.exec_stmt(stmts.into_iter().next().unwrap()),
            _ => err("exactly one statement per execute() call"),
        }
    }

    fn exec_stmt(&mut self, stmt: Statement) -> Result<ExecOutcome> {
        match stmt {
            Statement::CreateTable(create) => self.exec_create(create),
            Statement::Drop {
                object_type, names, ..
            } => {
                if object_type != sqlparser::ast::ObjectType::Table {
                    return err("only DROP TABLE is supported");
                }
                let name = names.first().map(obj_name).unwrap_or_default();
                if self.tables.remove(&name).is_none() {
                    return err(format!("table {name} does not exist"));
                }
                self.save_catalog()?;
                Ok(ExecOutcome::Affected(0))
            }
            Statement::Insert(insert) => self.exec_insert(insert),
            Statement::Update(sqlparser::ast::Update {
                table,
                assignments,
                selection,
                ..
            }) => self.exec_update(table, assignments, selection),
            Statement::Delete(sqlparser::ast::Delete {
                from, selection, ..
            }) => self.exec_delete(from, selection),
            Statement::AlterTable(alter) => self.exec_alter(alter),
            Statement::StartTransaction { .. } => {
                if self.tx_snapshot.is_some() {
                    return err("transaction already in progress");
                }
                self.tx_snapshot = Some(self.snapshot_all());
                Ok(ExecOutcome::Affected(0))
            }
            Statement::Commit { .. } => {
                if self.tx_snapshot.take().is_none() {
                    return err("no transaction in progress");
                }
                Ok(ExecOutcome::Affected(0))
            }
            Statement::Rollback { .. } => self.rollback_tx(),
            Statement::Query(q) => self.exec_query(*q),
            other => err(format!("unsupported statement: {other}")),
        }
    }

    /// Rewrite the whole table from `docs` (delete+reinsert; old pages are
    /// orphaned until compaction arrives with the B+ tree milestone).
    /// Rewrite the table from `docs`; returns the new page list.
    fn rewrite_table(&mut self, table: &str, docs: Vec<Object>) -> Result<Vec<u32>> {
        let mut heap = Heap { pages: Vec::new() };
        let mut tx = self.pager.begin_tx();
        for doc in &docs {
            heap.insert(&mut self.pager, &mut tx, doc)?;
        }
        self.pager.commit_tx(tx)?;
        if let Some(meta) = self.tables.get_mut(table) {
            meta.pages = heap.pages.clone();
        }
        self.save_catalog()?;
        Ok(heap.pages)
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

    fn exec_update(
        &mut self,
        table: sqlparser::ast::TableWithJoins,
        assignments: Vec<sqlparser::ast::Assignment>,
        selection: Option<SqlExpr>,
    ) -> Result<ExecOutcome> {
        let sqlparser::ast::TableFactor::Table { name, .. } = table.relation else {
            return err("only simple table names in UPDATE");
        };
        let tname = obj_name(&name);
        let docs = self.table_docs(&tname)?;
        let mut out = Vec::new();
        let mut count = 0u64;
        for doc in docs {
            if self.matches(&selection, &doc)? {
                let mut doc = doc;
                for a in &assignments {
                    let sqlparser::ast::AssignmentTarget::ColumnName(col) = &a.target else {
                        return err("unsupported assignment target");
                    };
                    let col_name = obj_name(col);
                    let v = eval_expr(&a.value, &doc)?;
                    doc.insert(col_name, v);
                }
                count += 1;
                out.push(doc);
            } else {
                out.push(doc);
            }
        }
        let meta = self.tables.get(&tname).cloned().unwrap_or_default();
        // Validate BEFORE writing: a failed UPDATE must not change data.
        for doc in &out {
            meta.check(doc)?;
        }
        meta.check_unique(&out)?;
        self.rewrite_table(&tname, out)?;
        Ok(ExecOutcome::Affected(count))
    }

    fn exec_delete(
        &mut self,
        from: sqlparser::ast::FromTable,
        selection: Option<SqlExpr>,
    ) -> Result<ExecOutcome> {
        let sqlparser::ast::FromTable::WithFromKeyword(tables) = from else {
            return err("unsupported DELETE form");
        };
        if tables.len() != 1 {
            return err("DELETE from exactly one table");
        }
        let sqlparser::ast::TableFactor::Table { name, .. } = tables[0].relation.clone() else {
            return err("only simple table names in DELETE");
        };
        let tname = obj_name(&name);
        let docs = self.table_docs(&tname)?;
        let mut kept = Vec::new();
        let mut count = 0u64;
        for doc in docs {
            if self.matches(&selection, &doc)? {
                count += 1;
            } else {
                kept.push(doc);
            }
        }
        self.rewrite_table(&tname, kept)?;
        Ok(ExecOutcome::Affected(count))
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
                        meta.columns.push(col);
                    }
                }
                Op::DropColumn { column_names, .. } => {
                    for id in column_names {
                        meta.columns.retain(|c| c != &id.value);
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
                    self.rewrite_table(&tname, stripped)?;
                }
                other => return err(format!("unsupported ALTER TABLE operation: {other}")),
            }
        }
        self.tables.insert(tname.clone(), meta);
        self.save_catalog()?;
        Ok(ExecOutcome::Affected(0))
    }

    fn exec_create(&mut self, create: sqlparser::ast::CreateTable) -> Result<ExecOutcome> {
        let name = obj_name(&create.name);
        if self.tables.contains_key(&name) {
            return err(format!("table {name} already exists"));
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
                use sqlparser::ast::ColumnOption as CO;
                match &opt.option {
                    CO::PrimaryKey { .. } => meta.primary_key = Some(col.name.value.clone()),
                    CO::Unique { .. } => meta.unique.push(col.name.value.clone()),
                    CO::NotNull => meta.not_null.push(col.name.value.clone()),
                    CO::Null => {}
                    _ => return err("unsupported column constraint"),
                }
            }
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
        let columns: Vec<String> = if insert.columns.is_empty() {
            meta.columns.clone()
        } else {
            insert.columns.iter().map(obj_name).collect()
        };
        let Some(source) = insert.source else {
            return err("INSERT requires VALUES");
        };
        let SetExpr::Values(value_rows) = &*source.body else {
            return err("INSERT ... SELECT is not supported yet");
        };
        let mut rows: Vec<Vec<Value>> = Vec::new();
        for parens in &value_rows.rows {
            rows.push(
                parens
                    .content
                    .iter()
                    .map(eval_const)
                    .collect::<Result<_>>()?,
            );
        }
        if rows.is_empty() {
            return err("INSERT has no rows");
        }
        // Build all documents first, validate, and only then write — a
        // failed statement must leave the table untouched.
        let mut new_docs: Vec<Object> = Vec::new();
        for row in rows {
            if row.len() != columns.len() {
                return err(format!(
                    "INSERT has {} values but {} columns",
                    row.len(),
                    columns.len()
                ));
            }
            let doc: Object = columns.iter().cloned().zip(row).collect();
            meta.check(&doc)?;
            new_docs.push(doc);
        }
        let existing: Vec<Object> = Heap {
            pages: meta.pages.clone(),
        }
        .scan(&mut self.pager)
        .unwrap_or_default();
        let mut combined = existing.clone();
        combined.extend(new_docs.iter().cloned());
        meta.check_unique(&combined)?;

        let mut heap = Heap {
            pages: meta.pages.clone(),
        };
        for doc in &new_docs {
            let mut tx = self.pager.begin_tx();
            heap.insert(&mut self.pager, &mut tx, doc)?;
            self.pager.commit_tx(tx)?;
        }
        let count = new_docs.len() as u64;
        if let Some(m) = self.tables.get_mut(&table) {
            m.pages = heap.pages.clone();
        }
        self.save_catalog()?;
        Ok(ExecOutcome::Affected(count))
    }

    fn exec_query(&mut self, query: Query) -> Result<ExecOutcome> {
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
                if op != SetOperator::Union {
                    return err("only UNION is supported");
                }
                let l = self.exec_query(Query {
                    body: left,
                    ..query.clone()
                })?;
                let r = self.exec_query(Query {
                    body: right,
                    ..query
                })?;
                let (ExecOutcome::Rows(mut lr), ExecOutcome::Rows(rr)) = (l, r) else {
                    return err("UNION requires SELECT on both sides");
                };
                if lr.columns != rr.columns {
                    return err("UNION arms have different column counts");
                }
                lr.rows.extend(rr.rows);
                if set_quantifier != SetQuantifier::All {
                    let mut seen = std::collections::BTreeSet::new();
                    lr.rows.retain(|row| {
                        seen.insert(
                            encode::encode_to_vec(&Value::Array(row.clone())).unwrap_or_default(),
                        )
                    });
                }
                Ok(ExecOutcome::Rows(lr))
            }
            other => err(format!("unsupported query body: {other}")),
        }
    }

    fn exec_select(&mut self, query: Query, select: sqlparser::ast::Select) -> Result<ExecOutcome> {
        use sqlparser::ast::{JoinConstraint, JoinOperator};
        if select.from.is_empty() {
            return err("SELECT requires FROM in this version");
        }
        let base = &select.from[0];
        let (bname, balias, mut bdocs) = self.load_table_factor(&base.relation)?;
        let bkey = balias.unwrap_or_else(|| bname.clone());
        // A lone base table keeps unqualified field names; anything joined
        // gets "alias.col" keys to keep namespaces apart.
        let solo = base.joins.is_empty() && select.from.len() == 1;
        let mut rows: Vec<Object> = if solo {
            bdocs
        } else {
            bdocs.drain(..).map(|d| qualify(&d, &bkey)).collect()
        };
        let mut all_joins: Vec<&sqlparser::ast::Join> = base.joins.iter().collect();
        if !solo {
            for twj in &select.from[1..] {
                // comma-separated FROM entries: cross join their base tables
                let (n, a, d) = self.load_table_factor(&twj.relation)?;
                let k = a.unwrap_or(n);
                rows = join_rows(rows, &d, &k, None, false)?;
                all_joins.extend(twj.joins.iter());
            }
        }
        for j in all_joins {
            let (jname, jalias, jdocs) = self.load_table_factor(&j.relation)?;
            let jkey = jalias.unwrap_or(jname);
            let (left_join, on) = match &j.join_operator {
                JoinOperator::Join(c)
                | JoinOperator::Inner(c)
                | JoinOperator::Left(c)
                | JoinOperator::LeftOuter(c)
                | JoinOperator::Right(c)
                | JoinOperator::RightOuter(c) => {
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
                    let right_join = matches!(
                        j.join_operator,
                        JoinOperator::Right(_) | JoinOperator::RightOuter(_)
                    );
                    (
                        matches!(
                            j.join_operator,
                            JoinOperator::Left(_) | JoinOperator::LeftOuter(_)
                        ) && !right_join,
                        on,
                    )
                }
                JoinOperator::CrossJoin(_) => (false, None),
                _ => return err("unsupported join type"),
            };
            rows = join_rows(rows, &jdocs, &jkey, on.as_ref(), left_join)?;
        }

        // WHERE
        if let Some(cond) = &select.selection {
            let mut kept = Vec::new();
            for doc in &rows {
                if matches!(eval_expr(cond, doc)?, Value::Bool(true)) {
                    kept.push(doc.clone());
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
        self.exec_plain_select(query, select, rows)
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
            union_of_fields(&rows)
        } else {
            project.iter().map(|(n, _)| n.clone()).collect()
        };

        let mut out: Vec<Vec<Value>> = Vec::new();
        for doc in &rows {
            if want_star {
                out.push(
                    columns_out
                        .iter()
                        .map(|c| doc.get(c).cloned().unwrap_or(Value::Null))
                        .collect(),
                );
            } else {
                let mut row = Vec::with_capacity(project.len());
                for (_, e) in &project {
                    row.push(eval_expr(e, doc)?);
                }
                out.push(row);
            }
        }
        out = self.apply_order_limit(query, out, &columns_out)?;
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
            // HAVING: aggregate subexpressions resolve to output columns
            // (matched by their SQL text), everything else evaluates normally.
            if let Some(having) = &select.having {
                let doc: Object = columns.iter().cloned().zip(row.iter().cloned()).collect();
                if !matches!(eval_having(having, &doc)?, Value::Bool(true)) {
                    continue;
                }
            }
            out.push(row);
        }
        let out = self.apply_order_limit(query, out, &columns)?;
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
        let fname = f.name.to_string().to_uppercase();
        let inner = match &f.args {
            sqlparser::ast::FunctionArguments::List(list) => {
                list.args.first().and_then(|a| match a {
                    sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(e),
                    ) => Some(e.clone()),
                    sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Wildcard,
                    ) => Some(SqlExpr::Identifier(sqlparser::ast::Ident::new("__count__"))),
                    _ => None,
                })
            }
            _ => None,
        };
        let Some(inner) = inner else {
            return err(format!("unsupported aggregate arguments: {fname}"));
        };
        let op = match fname.as_str() {
            "COUNT" => AggOp::Count,
            "SUM" => AggOp::Sum,
            "AVG" => AggOp::Avg,
            "MIN" => AggOp::Min,
            "MAX" => AggOp::Max,
            other => return err(format!("unknown function: {other}")),
        };
        Ok(AggSpec::Agg { op, arg: inner })
    }

    fn load_table_factor(
        &mut self,
        tf: &sqlparser::ast::TableFactor,
    ) -> Result<(String, Option<String>, Vec<Object>)> {
        let sqlparser::ast::TableFactor::Table { name, alias, .. } = tf else {
            return err("only simple tables in FROM");
        };
        let tname = obj_name(name);
        let docs = self.table_docs(&tname)?;
        let alias = alias.as_ref().map(|a| a.name.value.clone());
        Ok((tname, alias, docs))
    }

    fn apply_order_limit(
        &mut self,
        query: Query,
        mut rows: Vec<Vec<Value>>,
        columns: &[String],
    ) -> Result<Vec<Vec<Value>>> {
        if let Some(order_by) = &query.order_by {
            let sqlparser::ast::OrderByKind::Expressions(exprs) = &order_by.kind else {
                return err("unsupported ORDER BY");
            };
            let keys: Vec<(String, bool)> = exprs
                .iter()
                .map(|o| (expr_name(&o.expr), o.options.asc.unwrap_or(true)))
                .collect();
            rows.sort_by(|a, b| {
                for (col, asc) in &keys {
                    let idx = columns.iter().position(|c| c == col);
                    let ord = match idx {
                        Some(i) => Value::cmp_values(&a[i], &b[i]),
                        None => std::cmp::Ordering::Equal,
                    };
                    if ord != std::cmp::Ordering::Equal {
                        return if *asc { ord } else { ord.reverse() };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }
        if let Some(LimitClause::LimitOffset { limit, offset, .. }) = &query.limit_clause {
            let n = match limit {
                Some(e) => eval_const(e)?.as_i64().unwrap_or(i64::MAX).max(0) as usize,
                None => usize::MAX,
            };
            let skip = match offset {
                Some(o) => eval_const(&o.value)?.as_i64().unwrap_or(0).max(0) as usize,
                None => 0,
            };
            rows = rows.into_iter().skip(skip).take(n).collect();
        }
        Ok(rows)
    }
}

enum AggOp {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

// Short-lived, built per statement; size difference is fine here.
#[allow(clippy::large_enum_variant)]
enum AggSpec {
    GroupKey { idx: usize },
    Agg { op: AggOp, arg: SqlExpr },
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

fn eval_agg(spec: &AggSpec, docs: &[&Object], key: &[Value]) -> Result<Value> {
    match spec {
        AggSpec::GroupKey { idx } => Ok(key.get(*idx).cloned().unwrap_or(Value::Null)),
        AggSpec::Agg { op, arg } => {
            let mut vals: Vec<Value> = Vec::new();
            for d in docs {
                let v = eval_expr(arg, d)?;
                if !matches!(v, Value::Null) {
                    vals.push(v);
                }
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
fn join_rows(
    left: Vec<Object>,
    right: &[Object],
    right_key: &str,
    on: Option<&SqlExpr>,
    left_join: bool,
) -> Result<Vec<Object>> {
    let mut out = Vec::new();
    for l in &left {
        let mut matched = false;
        for r in right {
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
        SqlExpr::Function(f) => matches!(
            f.name.to_string().to_uppercase().as_str(),
            "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
        ),
        SqlExpr::Nested(inner) => contains_agg(inner),
        _ => false,
    }
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

/// Evaluate a constant expression (literal / arithmetic on literals).
pub fn eval_const(e: &SqlExpr) -> Result<Value> {
    match e {
        SqlExpr::Value(v) => sql_value(v),
        SqlExpr::UnaryOp { op, expr } => {
            let v = eval_const(expr)?;
            match (op, v) {
                (sqlparser::ast::UnaryOperator::Minus, Value::Int(i)) => Ok(Value::Int(-i)),
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
    let hits: Vec<&Value> = doc
        .iter()
        .filter(|(k, _)| k.ends_with(&suffix))
        .map(|(_, v)| v)
        .collect();
    match hits.len() {
        0 => Ok(Value::Null),
        1 => Ok(hits[0].clone()),
        _ => err(format!("ambiguous column: {name}")),
    }
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
                (sqlparser::ast::UnaryOperator::Minus, Value::Int(i)) => Ok(Value::Int(-i)),
                (sqlparser::ast::UnaryOperator::Minus, Value::Float(f)) => Ok(Value::Float(-f)),
                _ => err("unsupported unary operand"),
            }
        }
        SqlExpr::BinaryOp { left, op, right } => {
            let l = eval_expr(left, doc)?;
            let r = eval_expr(right, doc)?;
            binop(l, op, r)
        }
        SqlExpr::Nested(e) => eval_expr(e, doc),
        SqlExpr::InList {
            expr,
            list,
            negated,
        } => {
            let v = eval_expr(expr, doc)?;
            let mut hit = false;
            for item in list {
                let iv = eval_expr(item, doc)?;
                if Value::cmp_values(&v, &iv) == std::cmp::Ordering::Equal {
                    hit = true;
                    break;
                }
            }
            Ok(Value::Bool(hit != *negated))
        }
        other => err(format!("unsupported expression: {other}")),
    }
}

/// HAVING evaluation where aggregate expressions are replaced by the
/// already-computed output column values.
fn eval_having(e: &SqlExpr, out: &Object) -> Result<Value> {
    match e {
        SqlExpr::Function(f) if contains_agg(&SqlExpr::Function(f.clone())) => {
            let name = expr_name(e);
            Ok(out.get(&name).cloned().unwrap_or(Value::Null))
        }
        SqlExpr::BinaryOp { left, op, right } => {
            let l = eval_having(left, out)?;
            let r = eval_having(right, out)?;
            binop(l, op, r)
        }
        SqlExpr::Nested(inner) => eval_having(inner, out),
        other => eval_expr(other, out),
    }
}

fn binop(l: Value, op: &BinaryOperator, r: Value) -> Result<Value> {
    use BinaryOperator::*;
    Ok(match op {
        Plus | Minus | Multiply | Divide => arith(l, op, r)?,
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
            _ => return err("not an arithmetic operator"),
        });
    }
    match (&l, &r) {
        (Value::Int(a), Value::Int(b)) => Ok(match op {
            Plus => Value::Int(a.wrapping_add(*b)),
            Minus => Value::Int(a.wrapping_sub(*b)),
            Multiply => Value::Int(a.wrapping_mul(*b)),
            Divide if *b == 0 => Value::Null,
            Divide => Value::Int(a / b),
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
}
