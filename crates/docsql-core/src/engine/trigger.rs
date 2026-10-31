use super::*;

/// One row trigger's definition (`CREATE TRIGGER`). Stored in the catalog
/// (`triggers` key) and re-parsed from the AST at CREATE time; replication
/// replays the creating DDL verbatim on every node, so the body must be
/// deterministic — enforced by [`Database::exec_create_trigger`], never
/// assumed.
#[derive(Clone, Debug)]
pub(crate) struct TriggerDef {
    /// Global unique name (one namespace with tables/views/indexes is NOT
    /// enforced — SQLite likewise scopes nothing; uniqueness among triggers
    /// is what DROP TRIGGER resolves).
    pub(crate) name: String,
    /// Target table (never a view, never a system/internal table).
    pub(crate) table: String,
    /// BEFORE or AFTER (INSTEAD OF is rejected at CREATE time).
    pub(crate) before: bool,
    pub(crate) insert: bool,
    pub(crate) delete: bool,
    /// UPDATE trigger. `Some(cols)` = UPDATE OF: fire only when the
    /// statement assigns at least one listed column.
    pub(crate) update: bool,
    pub(crate) update_of: Option<Vec<String>>,
    /// WHEN predicate over NEW./OLD. references, stored as SQL text.
    /// Evaluated per row after substitution; false/NULL skips the body.
    pub(crate) when: Option<String>,
    pub(crate) body: Vec<TriggerAction>,
}

/// One statement of a trigger body. `Sql` re-executes verbatim with
/// NEW./OLD. references substituted per row; `Raise` aborts the statement.
#[derive(Clone, Debug)]
pub(crate) enum TriggerAction {
    Sql(String),
    /// `SELECT RAISE(ABORT|FAIL, 'msg')` — both spellings abort the
    /// statement (DocSQL statement atomicity makes them equivalent; SQLite's
    /// FAIL would leave prior rows of the statement applied, which this
    /// engine's all-or-nothing statements never do). `RAISE(IGNORE)` and
    /// `RAISE(ROLLBACK)` are rejected at CREATE time.
    Raise {
        fail: bool,
        msg: String,
    },
}

/// Max trigger cascade depth (a trigger body statement firing further
/// triggers). SQLite allows deep cascades and dies on its own budget; ours
/// is loud at 16 so an infinite `t1 → t2 → t1` cycle errors instead of
/// hanging the single writer.
pub(super) const MAX_TRIGGER_DEPTH: u8 = 16;

/// Which event a [`Database::fire_row_triggers`] call is firing for; the
/// UPDATE arm carries the columns the statement assigns (UPDATE OF gating).
pub(super) enum TriggerFireEvent<'a> {
    Insert,
    Delete,
    Update(Option<&'a [String]>),
}

/// Target table of a DML statement, for the trigger statement guard in
/// [`Database::exec_stmt`]. `None` for shapes the guard does not cover
/// (multi-table FROM lists etc. — those never had row images to fire over).
pub(super) fn trigger_dml_target(stmt: &Statement) -> Option<String> {
    match stmt {
        Statement::Insert(i) => match &i.table {
            sqlparser::ast::TableObject::TableName(n) => Some(obj_name(n)),
            _ => None,
        },
        Statement::Update(u) => match &u.table.relation {
            sqlparser::ast::TableFactor::Table { name, .. } => Some(obj_name(name)),
            _ => None,
        },
        Statement::Delete(d) => match &d.from {
            sqlparser::ast::FromTable::WithFromKeyword(twjs) => {
                twjs.first().and_then(|t| match &t.relation {
                    sqlparser::ast::TableFactor::Table { name, .. } => Some(obj_name(name)),
                    _ => None,
                })
            }
            sqlparser::ast::FromTable::WithoutKeyword(_) => None,
        },
        Statement::Merge(m) => match &m.table {
            sqlparser::ast::TableFactor::Table { name, .. } => Some(obj_name(name)),
            _ => None,
        },
        _ => None,
    }
}

/// WHEN validation: only NEW./OLD. references over literals and operators.
/// An unqualified column would silently read NULL (schemaless semantics)
/// and skip every firing — refuse instead.
pub(super) fn validate_trigger_when(e: &SqlExpr) -> Result<()> {
    match e {
        SqlExpr::Identifier(_) => err(
            "WHEN must reference columns as NEW.col / OLD.col (an unqualified name would read NULL)",
        ),
        SqlExpr::CompoundIdentifier(parts) => {
            let head = parts
                .first()
                .map(|p| p.value.to_ascii_uppercase())
                .unwrap_or_default();
            if head != "NEW" && head != "OLD" {
                return err("WHEN must reference columns as NEW.col / OLD.col");
            }
            Ok(())
        }
        SqlExpr::Subquery(_) | SqlExpr::Exists { .. } | SqlExpr::InSubquery { .. } => {
            err("subqueries are not supported in a trigger WHEN clause")
        }
        SqlExpr::Nested(e) => validate_trigger_when(e),
        SqlExpr::UnaryOp { expr, .. } => validate_trigger_when(expr),
        SqlExpr::BinaryOp { left, right, .. } => {
            validate_trigger_when(left)?;
            validate_trigger_when(right)
        }
        SqlExpr::Between { expr, low, high, .. } => {
            validate_trigger_when(expr)?;
            validate_trigger_when(low)?;
            validate_trigger_when(high)
        }
        SqlExpr::InList { expr, list, .. } => {
            validate_trigger_when(expr)?;
            for item in list {
                validate_trigger_when(item)?;
            }
            Ok(())
        }
        SqlExpr::IsDistinctFrom(l, r) | SqlExpr::IsNotDistinctFrom(l, r) => {
            validate_trigger_when(l)?;
            validate_trigger_when(r)
        }
        SqlExpr::IsNull(e) | SqlExpr::IsNotNull(e) => validate_trigger_when(e),
        SqlExpr::Cast { expr, .. } => validate_trigger_when(expr),
        SqlExpr::Like { expr, pattern, .. } | SqlExpr::ILike { expr, pattern, .. } => {
            validate_trigger_when(expr)?;
            validate_trigger_when(pattern)
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(o) = operand {
                validate_trigger_when(o)?;
            }
            for w in conditions {
                validate_trigger_when(&w.condition)?;
                validate_trigger_when(&w.result)?;
            }
            if let Some(r) = else_result {
                validate_trigger_when(r)?;
            }
            Ok(())
        }
        SqlExpr::Function(f) => {
            if let sqlparser::ast::FunctionArguments::List(list) = &f.args {
                for a in &list.args {
                    if let sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(inner),
                    ) = a
                    {
                        validate_trigger_when(inner)?;
                    }
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Recognize the trigger-body `SELECT RAISE(ABORT|FAIL, 'msg')` statement
/// (SQLite spelling). Everything else in a SELECT-shaped body is refused —
/// a bare SELECT has no effect in a trigger body and only ever meant
/// RAISE here.
pub(super) fn raise_action(q: &sqlparser::ast::Query) -> Result<TriggerAction> {
    if q.with.is_some() || !q.limit_clause.is_none() || !matches!(&*q.body, SetExpr::Select(_)) {
        return err(
            "only SELECT RAISE(ABORT|FAIL, 'msg') is supported in trigger bodies (other SELECTs have no effect)",
        );
    }
    let SetExpr::Select(sel) = &*q.body else {
        unreachable!("checked above");
    };
    if !sel.from.is_empty() || sel.selection.is_some() {
        return err("RAISE must be a plain SELECT RAISE(ABORT|FAIL, 'msg') (no FROM / WHERE)");
    }
    if sel.projection.len() != 1 {
        return err("RAISE takes exactly one projection");
    }
    let sqlparser::ast::SelectItem::UnnamedExpr(SqlExpr::Function(f)) = &sel.projection[0] else {
        return err("only SELECT RAISE(ABORT|FAIL, 'msg') is supported in trigger bodies");
    };
    if !f.name.to_string().eq_ignore_ascii_case("RAISE") {
        return err("only SELECT RAISE(ABORT|FAIL, 'msg') is supported in trigger bodies");
    }
    let list = match &f.args {
        sqlparser::ast::FunctionArguments::List(l) => l,
        _ => return err("RAISE requires (ABORT|FAIL, 'message')"),
    };
    if list.args.len() != 2 {
        return err(
            "RAISE(IGNORE)/RAISE(ROLLBACK) are not supported — use RAISE(ABORT|FAIL, 'message')",
        );
    }
    let kind = match &list.args[0] {
        sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(
            SqlExpr::Identifier(i),
        )) => i.value.to_ascii_uppercase(),
        _ => {
            return err("RAISE first argument must be ABORT or FAIL");
        }
    };
    if kind != "ABORT" && kind != "FAIL" {
        return err(format!(
            "RAISE({kind}) is not supported (use ABORT or FAIL; DocSQL statements are \
             atomic so both roll the statement back — IGNORE/ROLLBACK have no separate effect)"
        ));
    }
    let msg = match &list.args[1] {
        sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(
            SqlExpr::Value(v),
        )) => match &v.value {
            sqlparser::ast::Value::SingleQuotedString(s) => s.clone(),
            _ => return err("RAISE message must be a string literal"),
        },
        _ => return err("RAISE message must be a string literal"),
    };
    Ok(TriggerAction::Raise {
        fail: kind == "FAIL",
        msg,
    })
}

/// Substitute NEW.col / OLD.col references with the row image's values
/// (recursively through the expression shapes a trigger body uses).
/// References to the image the event does not have (NEW on DELETE, OLD on
/// INSERT — SQLite semantics) read NULL.
pub(super) fn subst_new_old_expr(
    e: &mut SqlExpr,
    old: Option<&Object>,
    new: Option<&Object>,
) -> Result<()> {
    match e {
        SqlExpr::CompoundIdentifier(parts)
            if parts.len() >= 2
                && matches!(parts[0].value.to_ascii_uppercase().as_str(), "NEW" | "OLD") =>
        {
            let head = parts[0].value.to_ascii_uppercase();
            let leaf = parts.last().map(|p| p.value.clone()).unwrap_or_default();
            let img = match (head.as_str(), old, new) {
                ("NEW", _, Some(n)) => n,
                ("OLD", Some(o), _) => o,
                _ => {
                    // No such image for this event: NULL, like SQLite.
                    *e = value_to_literal(Value::Null)?;
                    return Ok(());
                }
            };
            let v = img.get(&leaf).cloned().unwrap_or(Value::Null);
            *e = value_to_literal(v)?;
        }
        SqlExpr::Nested(inner) => subst_new_old_expr(inner, old, new)?,
        SqlExpr::UnaryOp { expr, .. } => subst_new_old_expr(expr, old, new)?,
        SqlExpr::BinaryOp { left, right, .. } => {
            subst_new_old_expr(left, old, new)?;
            subst_new_old_expr(right, old, new)?;
        }
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            subst_new_old_expr(expr, old, new)?;
            subst_new_old_expr(low, old, new)?;
            subst_new_old_expr(high, old, new)?;
        }
        SqlExpr::InList { expr, list, .. } => {
            subst_new_old_expr(expr, old, new)?;
            for item in list {
                subst_new_old_expr(item, old, new)?;
            }
        }
        SqlExpr::IsDistinctFrom(l, r) | SqlExpr::IsNotDistinctFrom(l, r) => {
            subst_new_old_expr(l, old, new)?;
            subst_new_old_expr(r, old, new)?;
        }
        SqlExpr::IsNull(e) | SqlExpr::IsNotNull(e) => subst_new_old_expr(e, old, new)?,
        SqlExpr::Cast { expr, .. } => subst_new_old_expr(expr, old, new)?,
        SqlExpr::Like { expr, pattern, .. } | SqlExpr::ILike { expr, pattern, .. } => {
            subst_new_old_expr(expr, old, new)?;
            subst_new_old_expr(pattern, old, new)?;
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(o) = operand {
                subst_new_old_expr(o, old, new)?;
            }
            for w in conditions {
                subst_new_old_expr(&mut w.condition, old, new)?;
                subst_new_old_expr(&mut w.result, old, new)?;
            }
            if let Some(r) = else_result {
                subst_new_old_expr(r, old, new)?;
            }
        }
        SqlExpr::Function(f) => {
            if let sqlparser::ast::FunctionArguments::List(list) = &mut f.args {
                for a in &mut list.args {
                    if let sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(inner),
                    ) = a
                    {
                        subst_new_old_expr(inner, old, new)?;
                    }
                }
            }
        }
        // Subquery interiors are NOT substituted — a NEW./OLD. that
        // survived down there is caught by the literal-skipping scan on the
        // rendered text (loud error, never a silent NULL read).
        _ => {}
    }
    Ok(())
}

/// Apply [`subst_new_old_expr`] over a trigger body statement's expression
/// surfaces (INSERT VALUES / upsert arm, UPDATE SET + WHERE, DELETE WHERE).
pub(super) fn subst_new_old_stmt(
    stmt: &mut Statement,
    old: Option<&Object>,
    new: Option<&Object>,
) -> Result<()> {
    match stmt {
        Statement::Insert(i) => {
            if let Some(q) = &mut i.source {
                if let SetExpr::Values(v) = &mut *q.body {
                    for row in &mut v.rows {
                        for e in row.content.iter_mut() {
                            subst_new_old_expr(e, old, new)?;
                        }
                    }
                }
            }
            if let Some(sqlparser::ast::OnInsert::OnConflict(c)) = &mut i.on {
                if let sqlparser::ast::OnConflictAction::DoUpdate(du) = &mut c.action {
                    for a in &mut du.assignments {
                        subst_new_old_expr(&mut a.value, old, new)?;
                    }
                    if let Some(sel) = &mut du.selection {
                        subst_new_old_expr(sel, old, new)?;
                    }
                }
            }
        }
        Statement::Update(u) => {
            for a in &mut u.assignments {
                subst_new_old_expr(&mut a.value, old, new)?;
            }
            if let Some(s) = &mut u.selection {
                subst_new_old_expr(s, old, new)?;
            }
        }
        Statement::Delete(d) => {
            if let Some(s) = &mut d.selection {
                subst_new_old_expr(s, old, new)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// True when the rendered statement still carries a NEW. / OLD. reference
/// outside a string literal — the catch-all for substitution walkers that
/// deliberately do not descend into subqueries. Quote-aware via
/// [`crate::stmt::sql_literal_end`] so a literal mentioning "NEW." never
/// false-positives.
pub(super) fn text_has_trigger_ref_outside_literals(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                let (end, _) = crate::stmt::sql_literal_end(sql, i);
                i = end;
            }
            b'"' | b'`' => {
                let q = bytes[i];
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == q {
                        i += 1;
                        break;
                    }
                    i += 1;
                }
            }
            b'N' | b'n' | b'O' | b'o' => {
                let word_start =
                    i > 0 && (bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_');
                if !word_start
                    && i + 4 <= bytes.len()
                    && (sql[i..i + 3].eq_ignore_ascii_case("new")
                        || sql[i..i + 3].eq_ignore_ascii_case("old"))
                    && bytes[i + 3] == b'.'
                {
                    return true;
                }
                i += 1;
            }
            _ => i += 1,
        }
    }
    false
}

/// Canonical CREATE TRIGGER text for a stored definition (dump/join
/// snapshots). Body statements re-emit verbatim (they were stored from the
/// parsed AST's own rendering), so replay parses exactly what was accepted.
pub(super) fn create_trigger_sql(d: &TriggerDef) -> String {
    use std::fmt::Write as _;
    let mut s = String::new();
    let _ = write!(
        s,
        "CREATE TRIGGER {} {} {} ON {} ",
        crate::stmt::sql_quote_ident(&d.name),
        if d.before { "BEFORE" } else { "AFTER" },
        if d.insert {
            "INSERT".to_string()
        } else if d.delete {
            "DELETE".to_string()
        } else {
            match &d.update_of {
                Some(cols) => format!(
                    "UPDATE OF {}",
                    cols.iter()
                        .map(|c| crate::stmt::sql_quote_ident(c))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                None => "UPDATE".to_string(),
            }
        },
        crate::stmt::sql_quote_ident(&d.table)
    );
    if let Some(w) = &d.when {
        let _ = write!(s, "WHEN {w} ");
    }
    s.push_str("BEGIN ");
    for a in &d.body {
        match a {
            TriggerAction::Sql(text) => {
                s.push_str(text.trim_end().trim_end_matches(';'));
                s.push_str("; ");
            }
            TriggerAction::Raise { fail, msg } => {
                let _ = write!(
                    s,
                    "SELECT RAISE({}, {}); ",
                    if *fail { "FAIL" } else { "ABORT" },
                    crate::stmt::sql_string_literal(msg)
                );
            }
        }
    }
    s.push_str("END");
    s
}

impl Database {
    // Replication red line: the creating DDL rides the journal like every
    // other write and every node fires the same bodies over the same rows,
    // so a body may contain NOTHING nondeterministic — no NEWID()/RAND(),
    // no wall-clock calls, no INSERT omitting an auto-GUID column, no
    // INSERT leaving a wall-clock DEFAULT to be filled. Each is refused at
    // CREATE time: at fire time the divergence has already happened by the
    // time anyone could notice.
    pub(super) fn exec_create_trigger(
        &mut self,
        ct: sqlparser::ast::CreateTrigger,
    ) -> Result<ExecOutcome> {
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
    pub(super) fn validate_trigger_body_stmt(&self, stmt: &Statement) -> Result<TriggerAction> {
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

    pub(super) fn validate_trigger_target(&self, t: &str) -> Result<()> {
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

    pub(super) fn exec_drop_trigger(
        &mut self,
        names: &[ObjectName],
        if_exists: bool,
    ) -> Result<ExecOutcome> {
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
    pub(super) fn triggers_exist_for(&self, table: &str) -> bool {
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
    pub(super) fn fire_row_triggers(
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

    pub(super) fn run_trigger_bodies(
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
                        // replays alike). Widened for referential actions:
                        // a cascading statement rewrites EVERY table in its
                        // plan from pre-computed deltas, so a body write to
                        // any of them would be silently clobbered by that
                        // rewrite.
                        if let Some(t) = trigger_dml_target(&stmt) {
                            if t == firing_table {
                                return err(format!(
                                    "cannot modify table {t} from a trigger on {t} while a \
                                     statement on {t} is in progress (split the write onto a \
                                     second table)"
                                ));
                            }
                            if let Some(guarded) = &self.referential_guard {
                                if guarded.contains(&t) {
                                    return err(format!(
                                        "cannot modify table {t} from a trigger while a \
                                         referential-action statement is rewriting it (split \
                                         the write onto a second table)"
                                    ));
                                }
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
}
