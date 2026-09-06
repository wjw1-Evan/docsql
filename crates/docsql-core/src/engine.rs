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

#[derive(Debug, Clone)]
struct TableMeta {
    columns: Vec<String>,
    pages: Vec<u32>,
}

pub struct Database {
    pager: Pager,
    tables: std::collections::BTreeMap<String, TableMeta>,
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
                            tables.insert(name.clone(), TableMeta { columns, pages });
                        }
                    }
                }
            }
        }
        Ok(Database { pager, tables })
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
            Statement::Query(q) => self.exec_query(*q),
            other => err(format!("unsupported statement: {other}")),
        }
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
        self.tables.insert(
            name.clone(),
            TableMeta {
                columns,
                pages: vec![],
            },
        );
        self.save_catalog()?;
        Ok(ExecOutcome::Affected(0))
    }

    fn exec_insert(&mut self, insert: sqlparser::ast::Insert) -> Result<ExecOutcome> {
        let TableObject::TableName(name) = &insert.table else {
            return err("unsupported INSERT target");
        };
        let table = obj_name(name);
        let Some(meta) = self.tables.get(&table) else {
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
        let mut heap = Heap {
            pages: self.tables.get(&table).unwrap().pages.clone(),
        };
        let mut count = 0u64;
        for row in rows {
            if row.len() != columns.len() {
                return err(format!(
                    "INSERT has {} values but {} columns",
                    row.len(),
                    columns.len()
                ));
            }
            let doc: Object = columns.iter().cloned().zip(row).collect();
            let mut tx = self.pager.begin_tx();
            heap.insert(&mut self.pager, &mut tx, &doc)?;
            self.pager.commit_tx(tx)?;
            count += 1;
        }
        if let Some(meta) = self.tables.get_mut(&table) {
            meta.pages = heap.pages.clone();
        }
        self.save_catalog()?;
        Ok(ExecOutcome::Affected(count))
    }

    fn exec_query(&mut self, query: Query) -> Result<ExecOutcome> {
        let SetExpr::Select(select) = *query.body else {
            return err("only SELECT ... FROM is supported here");
        };
        if select.from.len() != 1 || !select.from[0].joins.is_empty() {
            return err("expected exactly one table in FROM");
        }
        let sqlparser::ast::TableFactor::Table { name, .. } = &select.from[0].relation else {
            return err("only simple table names in FROM");
        };
        let table = obj_name(name);
        let Some(meta) = self.tables.get(&table) else {
            return err(format!("table {table} does not exist"));
        };
        let heap = Heap {
            pages: meta.pages.clone(),
        };
        let docs = heap.scan(&mut self.pager)?;

        // Projection: output column list plus (optional) expressions to
        // evaluate per row. Plain identifiers read fields; anything else
        // (arithmetic, comparisons) is computed.
        let mut project: Vec<(String, SqlExpr)> = Vec::new();
        let mut want_star = false;
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
            union_of_fields(&docs)
        } else {
            project.iter().map(|(n, _)| n.clone()).collect()
        };

        // WHERE filter.
        let mut rows: Vec<Vec<Value>> = Vec::new();
        for doc in &docs {
            if let Some(cond) = &select.selection {
                match eval_expr(cond, doc)? {
                    Value::Bool(true) => {}
                    _ => continue,
                }
            }
            if want_star {
                let get = |c: &str| doc.get(c).cloned().unwrap_or(Value::Null);
                rows.push(columns_out.iter().map(|c| get(c)).collect());
            } else {
                let mut row = Vec::with_capacity(project.len());
                for (_, e) in &project {
                    row.push(eval_expr(e, doc)?);
                }
                rows.push(row);
            }
        }

        // ORDER BY on output columns.
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
                    let idx = columns_out.iter().position(|c| c == col);
                    let ord = match idx {
                        Some(i) => Value::cmp_values(&a[i], &b[i]),
                        None => Ordering::Equal,
                    };
                    if ord != Ordering::Equal {
                        return if *asc { ord } else { ord.reverse() };
                    }
                }
                Ordering::Equal
            });
        }

        // LIMIT / OFFSET.
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

        Ok(ExecOutcome::Rows(QueryResult {
            columns: columns_out,
            rows,
        }))
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
        SqlExpr::CompoundIdentifier(parts) => {
            parts.last().map(|p| p.value.clone()).unwrap_or_default()
        }
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

/// Evaluate an expression against a document row.
pub fn eval_expr(e: &SqlExpr, doc: &Object) -> Result<Value> {
    match e {
        SqlExpr::Identifier(i) => Ok(doc.get(&i.value).cloned().unwrap_or(Value::Null)),
        SqlExpr::CompoundIdentifier(parts) => {
            let name = parts.last().map(|p| p.value.clone()).unwrap_or_default();
            Ok(doc.get(&name).cloned().unwrap_or(Value::Null))
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
        other => err(format!("unsupported expression: {other}")),
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
        assert!(db.execute("UPDATE t SET a = 1").is_err()); // M4
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
