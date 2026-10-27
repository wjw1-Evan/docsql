use super::*;

impl<'a> ReadCx<'a> {
    /// WITH <cte> AS (...), ...: materialize each CTE (they can reference
    /// earlier ones) into a statement-local map on top of `base` — owned by
    /// this call, so the whole SELECT execution chain can run on
    /// `&Database` (MVCC stage A concurrent readers). Extracted so derived
    /// tables and parenthesized bodies can chain the OUTER scope through
    /// instead of rebuilding an empty one (a `WITH t AS (…) SELECT * FROM
    /// (SELECT * FROM t) d` used to lose t inside the derived table and
    /// silently read a same-named base table).
    pub(super) fn materialize_ctes(
        &self,
        with: &Option<sqlparser::ast::With>,
        base: &Ctes,
    ) -> Result<Ctes> {
        let mut ctes = base.clone();
        let Some(with) = with else {
            return Ok(ctes);
        };
        for cte in &with.cte_tables {
            let name = cte.alias.name.value.clone();
            // T-SQL spells recursion without a keyword: any CTE whose
            // UNION body references its own name recurses. The standard
            // RECURSIVE flag just makes that explicit (and is no longer
            // a rejection).
            if cte_references(&cte.query, &name) {
                let docs = self.materialize_recursive_cte(cte, &name, &ctes, with.recursive)?;
                ctes.insert(name, docs);
                continue;
            }
            let ExecOutcome::Rows(r) = self.exec_query_body(cte.query.as_ref().clone(), &ctes)?
            else {
                return err("CTE body must be a SELECT");
            };
            let alias_columns = table_alias_columns(&cte.alias.columns, r.columns.len())?;
            let docs: Vec<Object> = r
                .rows
                .into_iter()
                .map(|row| match &alias_columns {
                    Some(cols) => cols.iter().cloned().zip(row).collect(),
                    None => r.columns.iter().cloned().zip(row).collect(),
                })
                .collect();
            ctes.insert(name, docs);
        }
        Ok(ctes)
    }

    /// Semi-naive recursive CTE evaluation: run the anchor arm (which must
    /// not reference the CTE itself), then feed each iteration's rows back
    /// as the CTE's binding for the recursive arm until it produces no new
    /// rows (UNION semantics) or the caps trip (UNION ALL cycles). The
    /// working-table semantics match SQL Server: each iteration sees only
    /// the previous iteration's rows.
    pub(super) fn materialize_recursive_cte(
        &self,
        cte: &sqlparser::ast::Cte,
        name: &str,
        outer: &Ctes,
        _declared_recursive: bool,
    ) -> Result<Vec<Object>> {
        const MAX_ITERATIONS: usize = 100;
        const MAX_ROWS: usize = 100_000;
        // The CTE body's own top-level ORDER BY / LIMIT / FETCH cannot ride
        // along: the anchor and recursive arms are evaluated per iteration
        // and only the outer query's clauses apply to the merged result.
        // Silently dropping them made the same CTE return different row
        // sets with and without a self-reference — reject instead.
        if cte.query.order_by.is_some()
            || cte.query.limit_clause.is_some()
            || cte.query.fetch.is_some()
        {
            return err(format!(
                "recursive CTE {name} does not support ORDER BY / LIMIT / FETCH on the \
                 CTE body (apply them in the outer query)"
            ));
        }
        let sqlparser::ast::SetExpr::SetOperation {
            left,
            op: sqlparser::ast::SetOperator::Union,
            set_quantifier,
            right,
        } = cte.query.body.as_ref()
        else {
            return err(format!(
                "recursive CTE {name} must be <anchor> UNION [ALL] <recursive arm>"
            ));
        };
        // Anchor: evaluated WITHOUT the CTE bound — a self-reference here
        // is the classic "recursive reference in an anchor" mistake.
        let ExecOutcome::Rows(anchor) = self.exec_query_body(
            sqlparser::ast::Query {
                with: None,
                body: Box::new(left.as_ref().clone()),
                order_by: None,
                limit_clause: None,
                fetch: None,
                locks: vec![],
                for_clause: None,
                settings: None,
                format_clause: None,
                pipe_operators: vec![],
            },
            outer,
        )?
        else {
            return err("recursive CTE anchor must be a SELECT");
        };
        let alias_columns = table_alias_columns(&cte.alias.columns, anchor.columns.len())?;
        let to_docs = |rows: Vec<Vec<Value>>, columns: &[String]| -> Vec<Object> {
            rows.into_iter()
                .map(|row| match &alias_columns {
                    Some(cols) => cols.iter().cloned().zip(row).collect(),
                    None => columns.iter().cloned().zip(row).collect(),
                })
                .collect()
        };
        let columns = anchor.columns.clone();
        // UNION (distinct) keeps a seen-set over encoded bytes; UNION ALL
        // appends everything and relies on the caps. The anchor dedupes
        // through the same set — UNION semantics cover the seed rows too,
        // not just the recursive arm's output.
        let distinct = !matches!(set_quantifier, sqlparser::ast::SetQuantifier::All);
        let doc_key = |d: &Object| -> Vec<u8> {
            let mut buf = Vec::new();
            crate::encode::encode_object(d, &mut buf).unwrap_or_default();
            buf
        };
        let mut seen: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
        let mut all: Vec<Object> = Vec::new();
        for d in to_docs(anchor.rows, &columns) {
            if distinct && !seen.insert(doc_key(&d)) {
                continue;
            }
            all.push(d);
        }
        let arm_query = |binding: Vec<Object>| -> Result<QueryResult> {
            let mut scope = outer.clone();
            scope.insert(name.to_string(), binding);
            let ExecOutcome::Rows(r) = self.exec_query_body(
                sqlparser::ast::Query {
                    with: None,
                    body: Box::new(right.as_ref().clone()),
                    order_by: None,
                    limit_clause: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                    pipe_operators: vec![],
                },
                &scope,
            )?
            else {
                return err("recursive CTE arm must be a SELECT");
            };
            Ok(r)
        };
        let mut working = all.clone();
        let mut converged = true;
        for _ in 0..MAX_ITERATIONS {
            if working.is_empty() {
                converged = true;
                break;
            }
            converged = false;
            self.deadline.check()?;
            let r = arm_query(std::mem::take(&mut working))?;
            if r.columns.len() != columns.len() {
                return err(format!(
                    "recursive arm produces {} columns but the anchor produces {}",
                    r.columns.len(),
                    columns.len()
                ));
            }
            let docs = to_docs(r.rows, &columns);
            let mut next_working = Vec::new();
            for d in docs {
                if distinct {
                    let key = doc_key(&d);
                    if !seen.insert(key) {
                        continue;
                    }
                }
                next_working.push(d.clone());
                all.push(d);
                if all.len() > MAX_ROWS {
                    return err(format!(
                        "recursive CTE {name} exceeded the {MAX_ROWS}-row budget (UNION ALL cycle?)"
                    ));
                }
            }
            working = next_working;
        }
        if !converged && !working.is_empty() {
            return err(format!(
                "recursive CTE {name} did not converge within {MAX_ITERATIONS} iterations \
                 (UNION ALL cycle?)"
            ));
        }
        Ok(all)
    }

    /// Rewrite uncorrelated subqueries inside `e` into row-local
    /// expressions (IN-lists / literals) before row iteration.
    pub(super) fn subst_expr(&self, e: &mut SqlExpr) -> Result<()> {
        self.subst_expr_scoped(e, &OuterFrom::new(), false)
    }

    /// [`subst_expr`] with an outer scope: `outer` names this query level's
    /// FROM names/aliases, and `allow_rowcorr` marks the row-local surfaces
    /// (WHERE / projection / ORDER BY / UPDATE·DELETE predicates and SET)
    /// where a correlated subquery node may stay in place for per-row
    /// evaluation. Everywhere else correlation must keep erroring loudly —
    /// the row-local evaluator would read an outer column as NULL and
    /// silently mis-filter (`NOT IN ()` over an empty list is vacuously
    /// true, a row-deleting shape).
    pub(super) fn subst_expr_scoped(
        &self,
        e: &mut SqlExpr,
        outer: &OuterFrom,
        allow_rowcorr: bool,
    ) -> Result<()> {
        let corr = |q: &Query| query_mentions_outer(q, outer);
        let reject = || {
            err(
                "correlated subqueries are not supported here \
                 (supported: WHERE / SELECT projection / ORDER BY / UPDATE·DELETE predicates and SET)",
            )
        };
        match e {
            SqlExpr::Subquery(q) => {
                if corr(q) {
                    if allow_rowcorr {
                        return Ok(());
                    }
                    return reject();
                }
                let r = self.subquery_result(q)?;
                single_col_rows(&r)?;
                if r.rows.len() > 1 {
                    // Silently taking the first row makes the result depend
                    // on scan order — a wrong answer that looks plausible.
                    return err("scalar subquery returned more than one row");
                }
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
                if corr(subquery) {
                    if allow_rowcorr {
                        return self.subst_expr_scoped(expr, outer, allow_rowcorr);
                    }
                    return reject();
                }
                let r = self.subquery_result(subquery)?;
                single_col_rows(&r)?;
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
                if corr(subquery) {
                    if allow_rowcorr {
                        return Ok(());
                    }
                    return reject();
                }
                let r = self.subquery_result(subquery)?;
                *e = value_to_literal(Value::Bool(!r.rows.is_empty() != *negated))?;
            }
            SqlExpr::AnyOp {
                left,
                compare_op,
                right,
                ..
            } => {
                self.subst_expr_scoped(left, outer, allow_rowcorr)?;
                let SqlExpr::Subquery(q) = right.as_ref() else {
                    return err("ANY/ALL requires a subquery operand");
                };
                if corr(q) {
                    if allow_rowcorr {
                        return Ok(());
                    }
                    return reject();
                }
                if *compare_op == sqlparser::ast::BinaryOperator::Eq {
                    // `= ANY` is an IN list: cheaper than an OR chain and
                    // the shape the planner already understands.
                    let r = self.subquery_result(q)?;
                    single_col_rows(&r)?;
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
                } else {
                    *e = self.quantified_chain(q, left, compare_op, true)?;
                }
            }
            SqlExpr::AllOp {
                left,
                compare_op,
                right,
            } => {
                self.subst_expr_scoped(left, outer, allow_rowcorr)?;
                let SqlExpr::Subquery(q) = right.as_ref() else {
                    return err("ANY/ALL requires a subquery operand");
                };
                if corr(q) {
                    if allow_rowcorr {
                        return Ok(());
                    }
                    return reject();
                }
                *e = self.quantified_chain(q, left, compare_op, false)?;
            }
            // Recurse into composite expressions.
            SqlExpr::BinaryOp { left, right, .. } => {
                self.subst_expr_scoped(left, outer, allow_rowcorr)?;
                self.subst_expr_scoped(right, outer, allow_rowcorr)?;
            }
            SqlExpr::UnaryOp { expr, .. } => self.subst_expr_scoped(expr, outer, allow_rowcorr)?,
            SqlExpr::Nested(inner) => self.subst_expr_scoped(inner, outer, allow_rowcorr)?,
            SqlExpr::Between {
                expr, low, high, ..
            } => {
                self.subst_expr_scoped(expr, outer, allow_rowcorr)?;
                self.subst_expr_scoped(low, outer, allow_rowcorr)?;
                self.subst_expr_scoped(high, outer, allow_rowcorr)?;
            }
            SqlExpr::IsDistinctFrom(l, r) | SqlExpr::IsNotDistinctFrom(l, r) => {
                self.subst_expr_scoped(l, outer, allow_rowcorr)?;
                self.subst_expr_scoped(r, outer, allow_rowcorr)?;
            }
            SqlExpr::Rollup(groups) | SqlExpr::Cube(groups) | SqlExpr::GroupingSets(groups) => {
                for g in groups {
                    for e in g {
                        self.subst_expr_scoped(e, outer, allow_rowcorr)?;
                    }
                }
            }
            SqlExpr::Like { expr, pattern, .. } | SqlExpr::ILike { expr, pattern, .. } => {
                self.subst_expr_scoped(expr, outer, allow_rowcorr)?;
                self.subst_expr_scoped(pattern, outer, allow_rowcorr)?;
            }
            SqlExpr::InList { expr, list, .. } => {
                self.subst_expr_scoped(expr, outer, allow_rowcorr)?;
                for item in list {
                    self.subst_expr_scoped(item, outer, allow_rowcorr)?;
                }
            }
            SqlExpr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                if let Some(op) = operand {
                    self.subst_expr_scoped(op, outer, allow_rowcorr)?;
                }
                for w in conditions.iter_mut() {
                    self.subst_expr_scoped(&mut w.condition, outer, allow_rowcorr)?;
                    self.subst_expr_scoped(&mut w.result, outer, allow_rowcorr)?;
                }
                if let Some(el) = else_result {
                    self.subst_expr_scoped(el, outer, allow_rowcorr)?;
                }
            }
            SqlExpr::Cast { expr, .. } => self.subst_expr_scoped(expr, outer, allow_rowcorr)?,
            SqlExpr::Function(f) => {
                if let sqlparser::ast::FunctionArguments::List(list) = &mut f.args {
                    for a in &mut list.args {
                        if let sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(inner),
                        ) = a
                        {
                            self.subst_expr_scoped(inner, outer, allow_rowcorr)?;
                        }
                    }
                }
                if let Some(filter) = &mut f.filter {
                    self.subst_expr_scoped(filter, outer, allow_rowcorr)?;
                }
            }
            _ => {
                // Shared traversal: subqueries under wrapper nodes
                // (FLOOR/IS NULL/SUBSTRING/…) resolve here instead of
                // erroring at row level.
                for child in child_exprs_mut(e) {
                    self.subst_expr_scoped(child, outer, allow_rowcorr)?;
                }
            }
        }
        Ok(())
    }

    /// Per-row resolution of the correlated subquery nodes the pre-pass
    /// left in place: substitute outer references with this row's values
    /// (precise scoping — inner FROM names shadow the outer scope), then
    /// run the standard once-eval substitution over the now-self-contained
    /// node. Non-correlated leftovers (nested one level below a skipped
    /// node) resolve through the same delegation.
    pub(super) fn subst_expr_row(
        &self,
        e: &mut SqlExpr,
        doc: &Object,
        outer: &OuterFrom,
    ) -> Result<()> {
        let resolve_node = |e: &mut SqlExpr| -> Result<()> {
            let _g = enter_correlated_eval()?;
            if let Some(q) = subquery_of_mut(e) {
                self.deadline.check()?;
                subst_outer_refs_query(q, &OuterFrom::new(), outer, doc);
            }
            self.subst_expr(e)
        };
        match e {
            SqlExpr::Subquery(_) | SqlExpr::Exists { .. } => resolve_node(e)?,
            SqlExpr::InSubquery { expr, .. } => {
                // The left side may itself nest a correlated subquery.
                self.subst_expr_row(expr, doc, outer)?;
                resolve_node(e)?;
            }
            SqlExpr::AnyOp { left, .. } | SqlExpr::AllOp { left, .. } => {
                self.subst_expr_row(left, doc, outer)?;
                resolve_node(e)?;
            }
            // Same composite recursion as the pre-pass.
            SqlExpr::BinaryOp { left, right, .. } => {
                self.subst_expr_row(left, doc, outer)?;
                self.subst_expr_row(right, doc, outer)?;
            }
            SqlExpr::UnaryOp { expr, .. } => self.subst_expr_row(expr, doc, outer)?,
            SqlExpr::Nested(inner) => self.subst_expr_row(inner, doc, outer)?,
            SqlExpr::Between {
                expr, low, high, ..
            } => {
                self.subst_expr_row(expr, doc, outer)?;
                self.subst_expr_row(low, doc, outer)?;
                self.subst_expr_row(high, doc, outer)?;
            }
            SqlExpr::IsDistinctFrom(l, r) | SqlExpr::IsNotDistinctFrom(l, r) => {
                self.subst_expr_row(l, doc, outer)?;
                self.subst_expr_row(r, doc, outer)?;
            }
            SqlExpr::Like { expr, pattern, .. } | SqlExpr::ILike { expr, pattern, .. } => {
                self.subst_expr_row(expr, doc, outer)?;
                self.subst_expr_row(pattern, doc, outer)?;
            }
            SqlExpr::InList { expr, list, .. } => {
                self.subst_expr_row(expr, doc, outer)?;
                for item in list {
                    self.subst_expr_row(item, doc, outer)?;
                }
            }
            SqlExpr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                if let Some(op) = operand {
                    self.subst_expr_row(op, doc, outer)?;
                }
                for w in conditions.iter_mut() {
                    self.subst_expr_row(&mut w.condition, doc, outer)?;
                    self.subst_expr_row(&mut w.result, doc, outer)?;
                }
                if let Some(el) = else_result {
                    self.subst_expr_row(el, doc, outer)?;
                }
            }
            SqlExpr::Cast { expr, .. } => self.subst_expr_row(expr, doc, outer)?,
            SqlExpr::Function(f) => {
                if let sqlparser::ast::FunctionArguments::List(list) = &mut f.args {
                    for a in &mut list.args {
                        if let sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(inner),
                        ) = a
                        {
                            self.subst_expr_row(inner, doc, outer)?;
                        }
                    }
                }
                if let Some(filter) = &mut f.filter {
                    self.subst_expr_row(filter, doc, outer)?;
                }
            }
            _ => {
                // Same shared traversal as the pre-pass: correlated
                // subqueries left under wrapper nodes resolve per row.
                for child in child_exprs_mut(e) {
                    self.subst_expr_row(child, doc, outer)?;
                }
            }
        }
        Ok(())
    }

    /// Rewrite `x op ANY|ALL (subquery)` into an OR/AND chain of literal
    /// comparisons. DocSQL comparisons are two-valued (no UNKNOWN), so
    /// boolean quantification over the chain is exact. Empty ANY → FALSE,
    /// empty ALL → TRUE (SQL standard).
    pub(super) fn quantified_chain(
        &self,
        q: &Query,
        left: &SqlExpr,
        compare_op: &BinaryOperator,
        any: bool,
    ) -> Result<SqlExpr> {
        fn lit_bool(b: bool) -> SqlExpr {
            SqlExpr::Value(sqlparser::ast::Value::Boolean(b).into())
        }
        fn lit_null() -> SqlExpr {
            SqlExpr::Value(sqlparser::ast::Value::Null.into())
        }
        let r = self.subquery_result(q)?;
        single_col_rows(&r)?;
        if r.rows.is_empty() {
            // ANY over the empty set is FALSE, ALL is TRUE (standard).
            return value_to_literal(Value::Bool(!any));
        }
        let mut cmps = Vec::with_capacity(r.rows.len());
        for row in &r.rows {
            let v = value_to_literal(row.first().cloned().unwrap_or(Value::Null))?;
            cmps.push(SqlExpr::BinaryOp {
                left: Box::new(left.clone()),
                op: compare_op.clone(),
                right: Box::new(v),
            });
        }
        // Three-valued quantification. The engine's boolean AND/OR reject
        // Null, and the old COALESCE-to-FALSE fold was only WHERE-visible:
        // under NOT it admitted rows three-valued logic filters
        // (`NOT (x > ALL (…, NULL))` must stay UNKNOWN, not become TRUE).
        // `c = TRUE` / `c = FALSE` are Null-safe (a Null comparison is
        // FALSE under the engine's total order, never an error), so a CASE
        // over the folded chains reproduces the exact ANY/ALL truth table
        // under any nesting.
        let eq_bool = |c: SqlExpr, b: bool| -> SqlExpr {
            SqlExpr::BinaryOp {
                left: Box::new(c),
                op: BinaryOperator::Eq,
                right: Box::new(lit_bool(b)),
            }
        };
        let fold = |mut v: Vec<SqlExpr>, op: BinaryOperator| -> SqlExpr {
            let mut acc = v.swap_remove(0);
            for e in v {
                acc = SqlExpr::BinaryOp {
                    left: Box::new(acc),
                    op: op.clone(),
                    right: Box::new(e),
                };
            }
            acc
        };
        let mut t_eqs = Vec::with_capacity(cmps.len());
        let mut f_eqs = Vec::with_capacity(cmps.len());
        for c in cmps {
            t_eqs.push(eq_bool(c.clone(), true));
            f_eqs.push(eq_bool(c, false));
        }
        // ANY: TRUE if some comparison is TRUE, FALSE if all are FALSE,
        // else UNKNOWN. ALL mirrors it.
        let (c1, r1, c2, r2) = if any {
            (
                fold(t_eqs, BinaryOperator::Or),
                lit_bool(true),
                fold(f_eqs, BinaryOperator::And),
                lit_bool(false),
            )
        } else {
            (
                fold(f_eqs, BinaryOperator::Or),
                lit_bool(false),
                fold(t_eqs, BinaryOperator::And),
                lit_bool(true),
            )
        };
        Ok(SqlExpr::Case {
            case_token: sqlparser::ast::helpers::attached_token::AttachedToken::empty(),
            end_token: sqlparser::ast::helpers::attached_token::AttachedToken::empty(),
            operand: None,
            conditions: vec![
                sqlparser::ast::CaseWhen {
                    condition: c1,
                    result: r1,
                },
                sqlparser::ast::CaseWhen {
                    condition: c2,
                    result: r2,
                },
            ],
            else_result: Some(Box::new(lit_null())),
        })
    }

    /// Run a subquery and return its rows (first column only matters for
    /// IN/ANY lists, but scalar casts use the full first cell).
    pub(super) fn subquery_result(&self, q: &Query) -> Result<QueryResult> {
        // Correlated references (`outer_table.column`) must error instead of
        // falling into the inner query's schemaless column lookup, where a
        // missing column reads as NULL and silently mis-filters (NOT IN ()
        // over an empty list is vacuously true — that shape deletes rows).
        let mut from_names = std::collections::BTreeSet::new();
        collect_from_names(q, &mut from_names);
        let mut hits = Vec::new();
        self.collect_correlated_in_body(&q.body, &from_names, &mut hits);
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

    /// Scan every expression surface of a subquery body for outer references.
    /// Set-operation arms, parenthesized query bodies, GROUP BY keys and JOIN
    /// ON conditions all evaluate in the inner scope — a shape the old
    /// Select-only scan skipped silently read the outer column as NULL.
    pub(super) fn collect_correlated_in_body(
        &self,
        body: &sqlparser::ast::SetExpr,
        from_names: &std::collections::BTreeSet<String>,
        hits: &mut Vec<String>,
    ) {
        use sqlparser::ast::SetExpr;
        match body {
            SetExpr::Select(sel) => {
                for twj in &sel.from {
                    for j in &twj.joins {
                        if let Some(e) = join_on_expr(&j.join_operator) {
                            collect_correlated_refs(e, from_names, self.tables, hits);
                        }
                    }
                }
                if let Some(sel_expr) = &sel.selection {
                    collect_correlated_refs(sel_expr, from_names, self.tables, hits);
                }
                if let Some(having) = &sel.having {
                    collect_correlated_refs(having, from_names, self.tables, hits);
                }
                if let sqlparser::ast::GroupByExpr::Expressions(es, _) = &sel.group_by {
                    for e in es {
                        collect_correlated_refs(e, from_names, self.tables, hits);
                    }
                }
                for item in &sel.projection {
                    match item {
                        sqlparser::ast::SelectItem::UnnamedExpr(e)
                        | sqlparser::ast::SelectItem::ExprWithAlias { expr: e, .. } => {
                            collect_correlated_refs(e, from_names, self.tables, hits);
                        }
                        _ => {}
                    }
                }
            }
            SetExpr::SetOperation { left, right, .. } => {
                // Each arm's own FROM names shadow at its level (mirror of
                // the Query arm): passing the parent set straight through
                // misread a qualified arm-local reference as correlated.
                for arm in [left, right] {
                    let mut nested = from_names.clone();
                    collect_setexpr_from_names(arm, &mut nested);
                    self.collect_correlated_in_body(arm, &nested, hits);
                }
            }
            SetExpr::Query(inner) => {
                let mut nested = from_names.clone();
                collect_setexpr_from_names(&inner.body, &mut nested);
                self.collect_correlated_in_body(&inner.body, &nested, hits);
            }
            _ => {}
        }
    }
}
