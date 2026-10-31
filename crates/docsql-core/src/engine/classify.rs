use super::*;

impl Database {
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
    pub(super) fn stmt_is_write(stmt: &Statement) -> bool {
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
                !targets.contains(t)
                    && m.foreign_keys
                        .iter()
                        .any(|fk| targets.contains(&fk.ref_table))
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
    pub(super) fn classify_tx(stmt: &Statement) -> TxControl {
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
    pub(super) fn direct_view_dependents(&self, targets: &[String]) -> Vec<String> {
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
    pub(super) fn expand_view_dependents(
        &self,
        targets: &mut Vec<String>,
        cascade: bool,
    ) -> Result<()> {
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
}
