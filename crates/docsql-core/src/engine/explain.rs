use super::window::*;
use super::*;

impl<'a> ReadCx<'a> {
    /// EXPLAIN entry (both dispatch sites: the write-path parser match and
    /// the guardless read view): validates the EXPLAIN modifiers, then
    /// plans the inner SELECT without executing it.
    pub(crate) fn explain_outer(
        &self,
        statement: &Statement,
        analyze: bool,
        estimate: bool,
        format: &Option<sqlparser::ast::AnalyzeFormatKind>,
        options: &Option<Vec<sqlparser::ast::UtilityOption>>,
    ) -> Result<ExecOutcome> {
        if analyze {
            return err("EXPLAIN ANALYZE is not supported (plan only)");
        }
        if estimate {
            return err("EXPLAIN ESTIMATE is not supported");
        }
        if format.is_some() {
            return err("EXPLAIN FORMAT is not supported");
        }
        if options.is_some() {
            return err("EXPLAIN utility options are not supported");
        }
        // `EXPLAIN QUERY PLAN` (SQLite) and `verbose` are accepted as the
        // same plan output; `DESC <stmt>` parses into the same variant.
        match statement {
            Statement::Query(q) => self.explain_query((**q).clone()),
            _ => err("EXPLAIN supports SELECT queries only"),
        }
    }

    pub(super) fn explain_query(&self, query: Query) -> Result<ExecOutcome> {
        // Same preprocessing order as `exec_query_body`: `SELECT TOP (n)`
        // rewrites into LIMIT before the clause gates run, so EXPLAIN must
        // not reject (by skipping the rewrite) a statement that executes
        // fine.
        let query = rewrite_tsql_top(query)?;
        reject_unsupported_query_clauses(&query)?;
        if query.with.is_some() {
            return err("EXPLAIN: WITH (CTE) planning is not supported");
        }
        let select = match &*query.body {
            sqlparser::ast::SetExpr::Select(select) => (**select).clone(),
            // A parenthesized body re-dispatches at execution time; explain
            // the inner query instead of refusing with a set-operations
            // message that describes a query that never ran.
            sqlparser::ast::SetExpr::Query(inner) => {
                return self.explain_query((**inner).clone());
            }
            _ => return err("EXPLAIN supports a single SELECT (set operations are not planned)"),
        };
        let mut rows: Vec<Vec<Value>> = Vec::new();
        self.explain_select(&query, select, &mut rows)?;
        Ok(ExecOutcome::Rows(QueryResult {
            columns: vec!["plan".to_string(), "detail".to_string()],
            rows,
        }))
    }

    pub(super) fn explain_row(out: &mut Vec<Vec<Value>>, op: &str, detail: String) {
        out.push(vec![Value::Str(op.to_string()), Value::Str(detail)]);
    }

    pub(super) fn explain_select(
        &self,
        query: &Query,
        select: sqlparser::ast::Select,
        out: &mut Vec<Vec<Value>>,
    ) -> Result<()> {
        reject_unsupported_select_clauses(&select)?;
        let row = |out: &mut Vec<Vec<Value>>, op: &str, detail: String| {
            Self::explain_row(out, op, detail);
        };

        // Aggregate detection mirrors exec_select.
        let group_exprs: Vec<SqlExpr> = match &select.group_by {
            sqlparser::ast::GroupByExpr::Expressions(e, modifiers) => {
                // Mirror the executor's refusal: exec_select rejects
                // non-empty modifiers, so a plan that silently dropped them
                // would describe a query that never runs.
                if !modifiers.is_empty() {
                    return err(
                        "GROUP BY modifiers (WITH ROLLUP/CUBE/TOTALS) are not supported \
                         (use GROUP BY ROLLUP(...)/CUBE(...))",
                    );
                }
                e.clone()
            }
            sqlparser::ast::GroupByExpr::All(_) => return err("GROUP BY ALL is not supported"),
        };
        let is_aggregate = !group_exprs.is_empty() || select.projection.iter().any(is_agg_item);
        let distinct_all = matches!(&select.distinct, None | Some(sqlparser::ast::Distinct::All));
        let has_window = !window_calls_of(&select.projection).is_empty();
        let rownum_wanted = select_refs_rownum(&select) && !is_aggregate;

        if select.from.is_empty() {
            row(out, "SCAN", "no FROM — one constant row".to_string());
            return Ok(());
        }

        let where_text = select.selection.as_ref().map(|c| format!("WHERE ({c})"));
        let solo = select.from.len() == 1 && select.from[0].joins.is_empty();

        if solo {
            if let sqlparser::ast::TableFactor::Table {
                name, alias, args, ..
            } = &select.from[0].relation
            {
                let tname = obj_name(name);
                if args.is_some() {
                    // Table-valued function factor: the executor
                    // materializes it at runtime; there is no stored table
                    // to plan against (refusing with "table does not
                    // exist" described a query that runs fine).
                    row(
                        out,
                        "SCAN",
                        format!(
                            "table function {tname} — materialized at runtime (plan not detailed)"
                        ),
                    );
                } else if !plain_table_factor(&select.from[0].relation) {
                    // TABLESAMPLE / index hints / alias column lists / VERSION
                    // AS OF …: load_table_factor rejects these at runtime —
                    // printing an INDEX PROBE plan for a statement that
                    // cannot execute breaks the EXPLAIN/exec mirror.
                    return err("table factor carries clauses the executor rejects \
                         (TABLESAMPLE / index hints / alias columns / VERSION AS OF)");
                } else if is_compat_view(&tname) {
                    row(
                        out,
                        "SCAN",
                        format!("compatibility view {tname} (virtual, no heap)"),
                    );
                } else if let Some(meta) = self.tables.get(&tname) {
                    if meta.is_view() {
                        row(
                            out,
                            "VIEW",
                            format!(
                                "{tname} — stored definition expanded at runtime; \
                                 the base-table plan is not detailed"
                            ),
                        );
                    } else if self.explain_count_star_gate(query, &select)? {
                        row(
                            out,
                            "SCAN",
                            format!("COUNT(*) ON {tname} — live slot count, no document decode"),
                        );
                        return Ok(());
                    } else {
                        self.explain_real_table_scan(
                            query,
                            &select,
                            &tname,
                            alias.as_ref().map(|a| a.name.value.clone()),
                            is_aggregate,
                            distinct_all,
                            has_window,
                            rownum_wanted,
                            out,
                        )?;
                    }
                } else {
                    return err(format!("table {tname} does not exist"));
                }
            } else {
                row(
                    out,
                    "SCAN",
                    "non-plain FROM factor — executed at runtime (plan not detailed)".to_string(),
                );
            }
        } else {
            self.explain_join_shape(&select, out)?;
        }

        // Post-scan clause rows (the window paths consume WHERE + ORDER +
        // LIMIT, which their SCAN rows already say).
        let windowed = out
            .iter()
            .any(|r| r[1].as_str().is_some_and(|d| d.contains("WINDOW")));
        if let Some(w) = &where_text {
            if !windowed {
                row(out, "FILTER", format!("{w} (row-local evaluation)"));
            }
        }
        if query.order_by.is_some() && !windowed {
            row(out, "SORT", "in-memory generic sort".to_string());
        }
        if (query.limit_clause.is_some() || query.fetch.is_some()) && !windowed {
            row(out, "LIMIT", "applied after scanning/sorting".to_string());
        }
        if !distinct_all {
            row(out, "DISTINCT", "dedup after scan".to_string());
        }
        if select.having.is_some() {
            row(out, "HAVING", "post-aggregate filter".to_string());
        }
        if is_aggregate {
            row(
                out,
                "AGGREGATE",
                "grouped/aggregate execution path".to_string(),
            );
        }
        if has_window {
            row(
                out,
                "WINDOW",
                "window functions force full rowset materialization (fast paths off)".to_string(),
            );
        }
        if rownum_wanted {
            row(
                out,
                "ROWNUM",
                "numbered before WHERE (Oracle semantics)".to_string(),
            );
        }
        Ok(())
    }

    /// The scan rows for a solo REAL table: window fast paths, then the
    /// index probe, then the full-scan fallback — in exec order.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn explain_real_table_scan(
        &self,
        query: &Query,
        select: &sqlparser::ast::Select,
        tname: &str,
        alias: Option<String>,
        is_aggregate: bool,
        distinct_all: bool,
        has_window: bool,
        rownum_wanted: bool,
        out: &mut Vec<Vec<Value>>,
    ) -> Result<()> {
        let row = |out: &mut Vec<Vec<Value>>, op: &str, detail: String| {
            Self::explain_row(out, op, detail);
        };
        let meta = self.tables.get(tname).expect("caller resolved the table");
        let ctes = Ctes::new();
        let fast_eligible = !is_aggregate
            && !rownum_wanted
            && select.having.is_none()
            && distinct_all
            && !has_window;
        if fast_eligible {
            if let Some(p) = self.ordered_window_plan(
                select,
                &select.from,
                &select.selection,
                &query.order_by,
                query,
                &ctes,
            )? {
                row(
                    out,
                    "SCAN",
                    format!(
                        "ORDERED INDEX WINDOW ON {tname} USING {} {} SKIP {} LIMIT {}{}",
                        p.root_key,
                        if p.asc { "ASC" } else { "DESC" },
                        p.skip,
                        p.take,
                        if p.exact { "" } else { " + residual WHERE" }
                    ),
                );
                if let Some(plan) = &p.probe {
                    row(out, "PROBE", probe_plan_text(&p.root_key, plan));
                }
                return Ok(());
            }
            if let Some(up) = self.unindexed_window_plan(select, query, &ctes)? {
                let fields = up
                    .keys
                    .iter()
                    .map(|(f, _, _)| f.first().cloned().unwrap_or_default())
                    .collect::<Vec<_>>()
                    .join(", ");
                row(
                    out,
                    "SCAN",
                    format!(
                        "TOP-K WINDOW ON {} BY {fields} SKIP {} LIMIT {} \
                         (encoded-byte key extraction)",
                        up.table, up.skip, up.take
                    ),
                );
                return Ok(());
            }
        }
        // Index probe (load_from's fast path): solo plain real table with a
        // WHERE whose AND-conjuncts the planner can subsume.
        let akey = alias.as_deref();
        if let Some(cond) = &select.selection {
            if let Some((root, plan, exact)) = probe_plan(cond, meta, tname, akey) {
                // Mirror the executor exactly: probe_pairs falls back to a
                // full scan when promote_plan_bounds cannot lift the bounds
                // into the tree's key type (mixed Str/Timestamp column). A
                // plan line promising an index probe the executor never
                // takes would mislead tuning.
                let tree = BTree::open(meta.index_roots[&root]);
                let tx = self.pager.begin_tx();
                let liftable = self.promote_plan_bounds(&tree, &tx, &plan)?.is_some();
                drop(tx);
                if liftable {
                    row(
                        out,
                        "SCAN",
                        format!(
                            "INDEX PROBE ON {tname} USING {root}{}",
                            if exact { "" } else { " + residual WHERE" }
                        ),
                    );
                    row(out, "PROBE", probe_plan_text(&root, &plan));
                    return Ok(());
                }
            }
        }
        row(out, "SCAN", format!("FULL HEAP SCAN ON {tname}"));
        Ok(())
    }

    /// Join shapes: every side loads fully, then combines — the per-join
    /// strategy (equi/hash vs nested loop) mirrors `join_rows`.
    pub(super) fn explain_join_shape(
        &self,
        select: &sqlparser::ast::Select,
        out: &mut Vec<Vec<Value>>,
    ) -> Result<()> {
        let row = |out: &mut Vec<Vec<Value>>, op: &str, detail: String| {
            Self::explain_row(out, op, detail);
        };
        for twj in &select.from {
            let desc = self.explain_factor_desc(&twj.relation)?;
            row(out, "SCAN", format!("JOIN SIDE FULL LOAD — {desc}"));
            for j in &twj.joins {
                let kind = match &j.join_operator {
                    sqlparser::ast::JoinOperator::Inner(_) => "INNER",
                    sqlparser::ast::JoinOperator::LeftOuter(_) => "LEFT",
                    sqlparser::ast::JoinOperator::RightOuter(_) => "RIGHT",
                    sqlparser::ast::JoinOperator::FullOuter(_) => "FULL",
                    sqlparser::ast::JoinOperator::CrossJoin(_) => "CROSS",
                    sqlparser::ast::JoinOperator::CrossApply => {
                        row(
                            out,
                            "JOIN",
                            "CROSS APPLY — right side per left row".to_string(),
                        );
                        continue;
                    }
                    sqlparser::ast::JoinOperator::OuterApply => {
                        row(
                            out,
                            "JOIN",
                            "OUTER APPLY — right side per left row".to_string(),
                        );
                        continue;
                    }
                    _ => "JOIN",
                };
                let right_desc = self.explain_factor_desc(&j.relation)?;
                let right_key = match &j.relation {
                    sqlparser::ast::TableFactor::Table { name, alias, .. } => alias
                        .as_ref()
                        .map(|a| a.name.value.clone())
                        .unwrap_or_else(|| obj_name(name)),
                    sqlparser::ast::TableFactor::Derived { alias, .. } => alias
                        .as_ref()
                        .map(|a| a.name.value.clone())
                        .unwrap_or_else(|| "?".to_string()),
                    _ => "?".to_string(),
                };
                // Same extraction the executor uses (the plain JOIN
                // keyword parses as JoinOperator::Join).
                let on = join_on_expr(&j.join_operator);
                let strategy = match on {
                    None => "nested loop (no ON)".to_string(),
                    Some(e) => {
                        if equi_plan(&right_key, e).is_some() {
                            "hash join (equi ON)".to_string()
                        } else {
                            "nested loop (non-equi or unqualified ON)".to_string()
                        }
                    }
                };
                let on_text = on.map(|e| format!(" ON ({e})")).unwrap_or_default();
                row(
                    out,
                    "JOIN",
                    format!("{kind} {strategy}{on_text} — right side: {right_desc}"),
                );
            }
        }
        Ok(())
    }

    pub(super) fn explain_factor_desc(&self, tf: &sqlparser::ast::TableFactor) -> Result<String> {
        // Mirror `load_table_factor`'s rejection gates: a factor carrying
        // clauses the executor refuses must not be described as a runnable
        // plan — EXPLAIN used to print "JOIN SIDE FULL LOAD" for
        // TABLESAMPLE / index-hint / time-travel factors that then failed
        // on execution (same red line the solo branch enforces).
        match tf {
            sqlparser::ast::TableFactor::Table {
                sample,
                version,
                partitions,
                with_hints,
                index_hints,
                with_ordinality,
                json_path,
                alias,
                ..
            } => {
                if sample.is_some() {
                    return err("TABLESAMPLE is not supported");
                }
                if version.is_some() {
                    return err("table time travel (AS OF / FOR SYSTEM_TIME) is not supported");
                }
                if !partitions.is_empty() {
                    return err("partition selection (PARTITION (...)) is not supported");
                }
                if !with_hints.is_empty() {
                    return err("table hints (WITH (...)) are not supported");
                }
                if !index_hints.is_empty() {
                    return err("index hints are not supported");
                }
                if *with_ordinality {
                    return err("WITH ORDINALITY is not supported");
                }
                if json_path.is_some() {
                    return err("JSON path table sources are not supported");
                }
                if alias.as_ref().is_some_and(|a| !a.columns.is_empty()) {
                    return err("column aliases on a table alias are not supported");
                }
            }
            sqlparser::ast::TableFactor::Derived {
                lateral, sample, ..
            } => {
                if *lateral {
                    return err("LATERAL derived tables are not supported");
                }
                if sample.is_some() {
                    return err("TABLESAMPLE is not supported");
                }
            }
            _ => {}
        }
        Ok(match tf {
            sqlparser::ast::TableFactor::Table { name, alias, .. } => {
                let tname = obj_name(name);
                if is_compat_view(&tname) {
                    format!("compatibility view {tname} (virtual)")
                } else if self.tables.get(&tname).is_some_and(|m| m.is_view()) {
                    format!("view {tname} (expanded at runtime)")
                } else {
                    let a = alias
                        .as_ref()
                        .map(|a| format!(" AS {}", a.name.value))
                        .unwrap_or_default();
                    format!("table {tname}{a}")
                }
            }
            sqlparser::ast::TableFactor::Derived { alias, .. } => {
                let a = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| "?".to_string());
                format!("derived table {a} (subquery executed at runtime)")
            }
            sqlparser::ast::TableFactor::TableFunction { .. } => {
                "table function (executed at runtime)".to_string()
            }
            _ => "subquery/NESTED factor (executed at runtime)".to_string(),
        })
    }

    /// Pure-AST mirror of `try_count_star`'s eligibility gates (no heap
    /// access): true when `SELECT COUNT(*) FROM <solo real table>` takes
    /// the live-count fast path.
    pub(super) fn explain_count_star_gate(
        &self,
        query: &Query,
        select: &sqlparser::ast::Select,
    ) -> Result<bool> {
        if select.selection.is_some()
            || select.having.is_some()
            || select.top.is_some()
            || !matches!(&select.distinct, None | Some(sqlparser::ast::Distinct::All))
            || query.order_by.is_some()
            || query.limit_clause.is_some()
            || query.fetch.is_some()
            || select.projection.len() != 1
            || select.from.len() != 1
            || !select.from[0].joins.is_empty()
            || !matches!(&select.group_by, sqlparser::ast::GroupByExpr::Expressions(e, _) if e.is_empty())
        {
            return Ok(false);
        }
        if !plain_table_factor(&select.from[0].relation) {
            return Ok(false);
        }
        let sqlparser::ast::TableFactor::Table { name, .. } = &select.from[0].relation else {
            return Ok(false);
        };
        let tname = obj_name(name);
        let Some(meta) = self.tables.get(&tname).filter(|m| !m.is_view()) else {
            return Ok(false);
        };
        let _ = meta;
        let expr = match &select.projection[0] {
            SelectItem::UnnamedExpr(e) => e,
            SelectItem::ExprWithAlias { expr, .. } => expr,
            _ => return Ok(false),
        };
        let SqlExpr::Function(f) = expr else {
            return Ok(false);
        };
        if !f.name.to_string().eq_ignore_ascii_case("count")
            || f.filter.is_some()
            || f.over.is_some()
        {
            return Ok(false);
        }
        use sqlparser::ast::{DuplicateTreatment, FunctionArg, FunctionArgExpr, FunctionArguments};
        Ok(matches!(&f.args, FunctionArguments::List(list) if {
            !matches!(list.duplicate_treatment, Some(DuplicateTreatment::Distinct))
                && list.args.len() == 1
                && matches!(list.args[0], FunctionArg::Unnamed(FunctionArgExpr::Wildcard))
        }))
    }
}
