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
