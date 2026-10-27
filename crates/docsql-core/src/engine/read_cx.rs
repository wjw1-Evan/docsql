use super::*;
/// Execution context for the SELECT chain: everything a read needs — pager,
/// catalog, deadline and optionally a snapshot — without borrowing the
/// `Database` itself. Stage A runs it under the server's read lock
/// (`snap: None`, current visibility); stage B runs it entirely outside the
/// database locks (`snap: Some`, as-of visibility), so reads never block
/// writes. The SELECT chain methods live on this type; the write path
/// reaches them through `Database::read_cx` and the `_cx` shims.
pub(crate) struct ReadCx<'a> {
    pub(crate) pager: &'a Pager,
    pub(crate) tables: &'a std::collections::BTreeMap<String, std::sync::Arc<TableMeta>>,
    pub(crate) deadline: &'a StmtDeadline,
    pub(crate) snap: Option<&'a Snapshot>,
}

impl<'a> ReadCx<'a> {
    /// Page reader for this context: current visibility without a snapshot,
    /// as-of reconstruction with one.
    pub(crate) fn reader(&self) -> PageReader<'a> {
        match self.snap {
            Some(snap) => PageReader::Snapshot(self.pager, snap),
            None => PageReader::Current(self.pager),
        }
    }
}

/// A guardless read view (MVCC stage B): the server builds one under a
/// microsecond-scale read lock, then executes SELECTs with no database lock
/// held — reads never block writes. Page reads go through the pager snapshot
/// (as-of the commit head at creation); the catalog is the cloned snapshot.
/// Dropping the view ends the pager snapshot.
pub struct ReadView {
    pub(super) pager: std::sync::Arc<Pager>,
    pub(super) catalog: CatalogSnapshot,
    pub(super) deadline: std::sync::Arc<StmtDeadline>,
    pub(super) snap: Snapshot,
}

impl ReadView {
    /// Arm the cooperative statement deadline. The view owns its deadline,
    /// so concurrent readers never overwrite each other's arming.
    pub fn set_statement_deadline(&self, deadline: Option<std::time::Instant>) {
        self.deadline.set(deadline);
    }

    /// Execute a read-only statement as of this view's snapshot. Plain
    /// SELECT only — the same refusals as [`Database::execute_read`].
    pub fn execute(&self, sql: &str) -> Result<ExecOutcome> {
        let parsed = Database::parse_classified(sql)?;
        let cx = ReadCx {
            pager: &self.pager,
            tables: &self.catalog.0,
            deadline: &self.deadline,
            snap: Some(&self.snap),
        };
        // EXPLAIN is a pure read — a readonly role diagnosing a plan is
        // exactly the audience; the inner query's reads are what
        // `stmt_read_targets` classifies for authorization.
        if let AnyStmt::Sql(stmt) = &parsed.stmt {
            if let Statement::Explain {
                statement,
                analyze,
                estimate,
                format,
                options,
                ..
            } = stmt.as_ref()
            {
                return cx.explain_outer(statement, *analyze, *estimate, format, options);
            }
        }
        let q = Database::plain_read_query(&parsed)?;
        cx.exec_query(q)
    }
}

impl Drop for ReadView {
    fn drop(&mut self) {
        self.pager.end_snapshot(self.snap);
    }
}

/// View-expansion recursion budget. `CREATE OR REPLACE` makes catalog
/// cycles constructible (see `exec_create_view`'s transitive check), and a
/// chain deep enough would blow the thread stack — one expansion level
/// costs several engine frames, so the budget stays far under the 2 MiB
/// test-thread stack while exceeding any sane view stack. Creation refuses
/// chains past this depth too (the CREATE dry-run expands the body).
pub(super) const MAX_VIEW_EXPANSION_DEPTH: usize = 16;

thread_local! {
    static VIEW_DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// RAII for one level of view expansion (a `Drop` decrement paired with
/// the increment here, so early `?` returns unwind the count too).
pub(super) struct ViewDepthGuard;

pub(super) fn enter_view_expansion() -> Result<ViewDepthGuard> {
    let entered = VIEW_DEPTH.with(|d| {
        let n = d.get();
        if n < MAX_VIEW_EXPANSION_DEPTH {
            d.set(n + 1);
            true
        } else {
            false
        }
    });
    if entered {
        Ok(ViewDepthGuard)
    } else {
        err(format!(
            "view expansion too deep (view cycle or more than {MAX_VIEW_EXPANSION_DEPTH} nested views)"
        ))
    }
}

impl Drop for ViewDepthGuard {
    fn drop(&mut self) {
        VIEW_DEPTH.with(|d| d.set(d.get().saturating_sub(1)));
    }
}

/// Correlated-subquery evaluation budget. Each per-row correlated
/// evaluation re-enters the engine's SELECT chain (a correlated subquery
/// may itself contain one), so nesting depth costs real stack frames and
/// quadratic work; anything past this depth is a pathological shape, not a
/// query plan.
pub(super) const MAX_CORRELATED_DEPTH: usize = 8;

thread_local! {
    static CORR_DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub(super) struct CorrDepthGuard;

pub(super) fn enter_correlated_eval() -> Result<CorrDepthGuard> {
    let entered = CORR_DEPTH.with(|d| {
        let n = d.get();
        if n < MAX_CORRELATED_DEPTH {
            d.set(n + 1);
            true
        } else {
            false
        }
    });
    if entered {
        Ok(CorrDepthGuard)
    } else {
        err(format!(
            "correlated subquery nesting too deep (more than {MAX_CORRELATED_DEPTH} levels)"
        ))
    }
}

impl Drop for CorrDepthGuard {
    fn drop(&mut self) {
        CORR_DEPTH.with(|d| d.set(d.get().saturating_sub(1)));
    }
}

impl<'a> ReadCx<'a> {
    /// Evaluate one row-local expression; when it still holds subquery
    /// nodes (correlated — the pre-pass left them), resolve the outer
    /// references against this row first.
    pub(super) fn eval_row_expr(
        &self,
        e: &SqlExpr,
        correlated: bool,
        doc: &Object,
        outer: &OuterFrom,
    ) -> Result<Value> {
        if correlated {
            let mut row_local = e.clone();
            self.subst_expr_row(&mut row_local, doc, outer)?;
            eval_expr(&row_local, doc)
        } else {
            eval_expr(e, doc)
        }
    }

    /// ORDER BY + LIMIT/OFFSET. Sort keys resolve in order of preference to
    /// an output column name, an ordinal position (`ORDER BY 2`), or — when
    /// the caller supplies the source docs — an arbitrary expression
    /// evaluated per row. `corr` supplies the outer scope for correlated
    /// ORDER BY expressions (plain-select path only).
    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_order_limit(
        &self,
        query: Query,
        mut rows: Vec<Vec<Value>>,
        columns: &[String],
        docs: Option<Vec<Object>>,
        corr: Option<&OuterFrom>,
    ) -> Result<Vec<Vec<Value>>> {
        // (column index, asc, nulls_first); hidden key columns appended for
        // ORDER BY expressions that are not output columns stay until the
        // FETCH tie check has run.
        let mut keys: Vec<(usize, bool, Option<bool>)> = Vec::new();
        let mut extra_keys = 0usize;
        if let Some(order_by) = &query.order_by {
            let sqlparser::ast::OrderByKind::Expressions(exprs) = &order_by.kind else {
                return err("unsupported ORDER BY");
            };
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
                        // append it as a hidden key column. Correlated nodes
                        // resolve their outer references per row (plain
                        // path, which is the only caller passing docs).
                        let sub = expr_has_subquery(&o.expr);
                        for (row, doc) in rows.iter_mut().zip(ds) {
                            row.push(match corr {
                                Some(outer) => self.eval_row_expr(&o.expr, sub, doc, outer)?,
                                None => eval_expr(&o.expr, doc)?,
                            });
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
        }
        if let Some(lc) = &query.limit_clause {
            rows = apply_limit_clause(lc, rows)?;
        }
        // Oracle 12c+/SQL-standard FETCH: `FETCH FIRST n ROWS ONLY` (and the
        // NEXT form). WITH TIES extends the window to the whole tie run of
        // the last kept row, so it needs an ORDER BY to define the peers.
        if let Some(fetch) = &query.fetch {
            if fetch.percent {
                return err("FETCH … PERCENT is not supported");
            }
            if fetch.with_ties && keys.is_empty() {
                return err("FETCH … WITH TIES requires ORDER BY");
            }
            let n = match fetch.quantity.as_ref().map(eval_const).transpose()? {
                Some(Value::Int(n)) if n >= 0 => n as usize,
                Some(Value::Int(n)) => {
                    return err(format!("FETCH quantity must be non-negative ({n})"))
                }
                Some(_) | None => return err("FETCH quantity must be an integer"),
            };
            if fetch.with_ties && n > 0 && n < rows.len() {
                let last = &rows[n - 1];
                let mut end = n;
                while end < rows.len()
                    && keys.iter().all(|(col, _, nulls_first)| {
                        cmp_maybe_null(&last[*col], &rows[end][*col], *nulls_first)
                            == Ordering::Equal
                    })
                {
                    end += 1;
                }
                rows.truncate(end);
            } else {
                rows.truncate(n);
            }
        }
        if extra_keys > 0 {
            for row in &mut rows {
                row.truncate(columns.len());
            }
        }
        Ok(rows)
    }

    /// Virtual information_schema tables from the catalog.
    pub(super) fn information_schema(&self, which: &str) -> Vec<Object> {
        // Engine/internal tables stay out of the standard dictionaries —
        // same display rule as the object tree and the Oracle views (their
        // existence, names and shapes are not every user's business).
        let visible = self.tables.iter().filter(|(n, _)| !is_internal_table(n));
        let mut out = Vec::new();
        if which.ends_with("tables") {
            for (name, meta) in visible {
                out.push(Object::from([
                    ("table_name".into(), Value::Str(name.clone())),
                    ("pages".into(), Value::Int(meta.pages.len() as i64)),
                ]));
            }
        } else {
            for (name, meta) in visible {
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

    /// CROSS/OUTER APPLY over a table function: evaluate the function's
    /// arguments against EACH left row (correlated lateral semantics),
    /// materialize the rowset, and merge. `outer` keeps the left row when
    /// the applied rowset is empty (right columns read as NULL — missing
    /// fields on the schemaless row shape).
    pub(super) fn apply_table_function(
        &self,
        left: Vec<Object>,
        fname: &str,
        alias: &Option<sqlparser::ast::TableAlias>,
        args: &[sqlparser::ast::FunctionArg],
        outer: bool,
    ) -> Result<Vec<Object>> {
        let Some(alias_obj) = alias else {
            return err(format!(
                "{fname} requires a table alias (APPLY {fname}(...) AS t)"
            ));
        };
        let akey = alias_obj.name.value.clone();
        let fname_upper = fname.to_uppercase();
        // Static ordinal guess for the width check when no row ever
        // materializes; the argument is correlated so a non-constant
        // third argument falls back to the two-column shape.
        let ordinal_on = args.len() == 3
            && matches!(
                eval_const(match args.last() {
                    Some(sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(e),
                    )) => e,
                    _ => return err("table function arguments must be values"),
                }),
                Ok(Value::Int(n)) if n != 0
            );
        // Alias column renames apply to the produced rows (same rule as
        // derived tables).
        let mut out = Vec::with_capacity(left.len());
        let mut produced_cols: Option<Vec<String>> = None;
        for lrow in &left {
            self.deadline.check()?;
            let mut vals = Vec::new();
            for a in args {
                match a {
                    sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(e),
                    ) => vals.push(eval_expr(e, lrow)?),
                    _ => return err("table function arguments must be values"),
                }
            }
            let docs = match crate::tsql::table_function_checked(&fname_upper, &vals, &mut || {
                self.deadline.check()
            }) {
                Some(Ok(rows)) => rows,
                Some(Err(e)) => return Err(e),
                None => return err(format!("unknown table function: {fname}")),
            };
            if docs.is_empty() {
                if outer {
                    out.push(lrow.clone());
                }
                continue;
            }
            if produced_cols.is_none() {
                produced_cols = Some(docs[0].keys().cloned().collect());
            }
            for d in docs {
                let qright = qualify(&d, &akey);
                out.push(merged_row(lrow, &qright));
            }
        }
        // Actual output columns (bare names) for the rename and NULL pad.
        let cols: Vec<String> = produced_cols.unwrap_or_else(|| {
            tablefn_static_cols(&fname_upper, ordinal_on)
                .iter()
                .map(|s| s.to_string())
                .collect()
        });
        // A wrong column list must fail loudly, exactly like the derived
        // table path — swallowing it silently dropped the rename.
        let rename = table_alias_columns(&alias_obj.columns, cols.len())?;
        let final_names: Vec<String> = rename.clone().unwrap_or_else(|| cols.clone());
        let prefix = format!("{akey}.");
        for row in &mut out {
            if let Some(rename) = &rename {
                // Rename IN PLACE (move the value from the function's output
                // column to its alias). Inserting alongside would leak the
                // original column into SELECT *.
                for (new, old) in rename.iter().zip(cols.iter()) {
                    let old_key = format!("{akey}.{old}");
                    if let Some(v) = row.remove(&old_key) {
                        row.insert(format!("{akey}.{new}"), v);
                    }
                }
            }
            if outer && !row.keys().any(|k| k.starts_with(&prefix)) {
                // OUTER APPLY kept this left row because the applied rowset
                // was empty. Pad the right columns with explicit NULLs:
                // without keys, a same-named LEFT column would answer the
                // suffix fallback and masquerade as the right value.
                for c in &final_names {
                    row.insert(format!("{akey}.{c}"), Value::Null);
                }
            }
        }
        Ok(out)
    }

    /// Planned BARE column list of a table factor, when statically known:
    /// base tables/views from the catalog, table functions from their fixed
    /// shape. Used to pad explicit NULL keys when an OUTER join's other side
    /// is empty (no row to sample the key shape from). `None` = unknown
    /// (derived tables with non-trivial projections, CTEs, …) and the join
    /// keeps its historical behavior.
    pub(super) fn table_factor_columns(
        &self,
        tf: &sqlparser::ast::TableFactor,
        ctes: &Ctes,
    ) -> Option<Vec<String>> {
        match tf {
            sqlparser::ast::TableFactor::Table { name, args, .. } => {
                if args.is_some() {
                    // Table-with-args = a table function whose output shape
                    // depends on evaluated arguments (STRING_SPLIT's ordinal
                    // toggle); let load_table_factor handle it and report no
                    // planned shape here.
                    return None;
                }
                let tname = obj_name(name);
                if is_compat_view(&tname) || ctes.contains_key(&tname) {
                    return None;
                }
                self.tables.get(&tname).map(|m| m.columns.clone())
            }
            sqlparser::ast::TableFactor::OpenJsonTable { .. } => Some(
                tablefn_static_cols("OPENJSON", false)
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
            ),
            _ => None,
        }
    }

    pub(super) fn load_table_factor(
        &self,
        tf: &sqlparser::ast::TableFactor,
        ctes: &Ctes,
    ) -> Result<(String, Option<String>, Vec<Object>)> {
        let sqlparser::ast::TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            sample,
            index_hints,
        } = tf
        else {
            // Derived table: FROM (SELECT ...) AS alias
            if let sqlparser::ast::TableFactor::Derived {
                lateral,
                subquery,
                alias,
                sample,
            } = tf
            {
                if *lateral {
                    return err("LATERAL derived tables are not supported");
                }
                if sample.is_some() {
                    return err("TABLESAMPLE is not supported");
                }
                // Chain the outer CTE scope through: a derived table is a
                // subquery of the statement it appears in, so `WITH t AS (…)
                // SELECT * FROM (SELECT * FROM t) d` must read the CTE —
                // and NOT fall through to a same-named base table. An own
                // WITH on the subquery materializes on top (its names
                // shadow, lexical nesting).
                let scoped = self.materialize_ctes(&subquery.with, ctes)?;
                let ExecOutcome::Rows(r) =
                    self.exec_query_body(subquery.as_ref().clone(), &scoped)?
                else {
                    return err("derived table must be a SELECT");
                };
                let alias_columns = match alias {
                    Some(a) => table_alias_columns(&a.columns, r.columns.len())?,
                    None => None,
                };
                let docs: Vec<Object> = r
                    .rows
                    .into_iter()
                    .map(|row| match &alias_columns {
                        Some(cols) => cols.iter().cloned().zip(row).collect(),
                        None => r.columns.iter().cloned().zip(row).collect(),
                    })
                    .collect();
                let alias = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_default();
                return Ok(("@derived".into(), Some(alias), docs));
            }
            // Bare function call in FROM: `FROM STRING_SPLIT('a,b', ',') AS s`
            // parses here, not into TableFactor::Table's args.
            if let sqlparser::ast::TableFactor::Function {
                lateral,
                name,
                args,
                with_ordinality,
                alias,
            } = tf
            {
                if *lateral {
                    return err("LATERAL table functions are not supported");
                }
                if *with_ordinality {
                    return err("WITH ORDINALITY is not supported");
                }
                let fname = obj_name(name).to_uppercase();
                if !matches!(
                    fname.as_str(),
                    "STRING_SPLIT" | "GENERATE_SERIES" | "OPENJSON"
                ) {
                    return err("table functions or FROM hints are not supported");
                }
                let mut vals = Vec::new();
                for a in args {
                    match a {
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(e),
                        ) => vals.push(eval_const(e)?),
                        _ => return err("table function arguments must be values"),
                    }
                }
                let Some(alias_obj) = alias else {
                    return err(format!(
                        "{fname} requires a table alias (FROM {fname}(...) AS t)"
                    ));
                };
                let mut docs = match crate::tsql::table_function_checked(&fname, &vals, &mut || {
                    self.deadline.check()
                }) {
                    Some(Ok(rows)) => rows,
                    Some(Err(e)) => return Err(e),
                    None => return err(format!("unknown table function: {fname}")),
                };
                let ordinal_on = vals.len() == 3 && matches!(vals[2], Value::Int(n) if n != 0);
                rename_tablefn_docs(alias_obj, &fname, ordinal_on, &mut docs)?;
                return Ok(("@tablefn".into(), Some(alias_obj.name.value.clone()), docs));
            }
            // `OPENJSON(x)` parses into a dedicated factor (WITH shapes and
            // path arguments are not supported — default key/value/type
            // rows only).
            if let sqlparser::ast::TableFactor::OpenJsonTable {
                json_expr,
                json_path,
                columns,
                alias,
            } = tf
            {
                if !columns.is_empty() || json_path.is_some() {
                    return err("OPENJSON WITH (...) and path arguments are not supported");
                }
                let val = eval_const(json_expr)?;
                let Some(alias_obj) = alias else {
                    return err("OPENJSON requires a table alias (FROM OPENJSON(...) AS j)");
                };
                let mut docs =
                    match crate::tsql::table_function_checked("OPENJSON", &[val], &mut || {
                        self.deadline.check()
                    }) {
                        Some(Ok(rows)) => rows,
                        Some(Err(e)) => return Err(e),
                        None => return err("unknown table function: OPENJSON"),
                    };
                rename_tablefn_docs(alias_obj, "OPENJSON", false, &mut docs)?;
                return Ok(("@tablefn".into(), Some(alias_obj.name.value.clone()), docs));
            }
            // T-SQL PIVOT: implicit-group every column except the pivot
            // column and the aggregate's argument, then emit one column
            // per IN-list value holding the aggregate over the matching
            // rows (NULL when the group has none).
            if let sqlparser::ast::TableFactor::Pivot {
                table,
                aggregate_functions,
                value_column,
                value_source,
                default_on_null,
                alias,
            } = tf
            {
                if default_on_null.is_some() || aggregate_functions.len() != 1 {
                    return err("PIVOT supports one aggregate without DEFAULT ON NULL here");
                }
                let (inner_name, _inner_alias, docs) = self.load_table_factor(table, ctes)?;
                let pivot = pivot_factor(docs, aggregate_functions, value_column, value_source)?;
                // Same alias fallback as UNPIVOT: without it the sentinel
                // "@pivot" leaked into output column names on JOIN.
                return Ok((
                    "@pivot".into(),
                    alias
                        .as_ref()
                        .map(|a| a.name.value.clone())
                        .or(Some(inner_name)),
                    pivot,
                ));
            }
            // T-SQL UNPIVOT: one output row per (input row × listed
            // column); NULL values are excluded (T-SQL default).
            if let sqlparser::ast::TableFactor::Unpivot {
                table,
                value,
                name,
                columns,
                null_inclusion,
                alias,
            } = tf
            {
                if null_inclusion.is_some() {
                    return err(
                        "UNPIVOT INCLUDE|EXCLUDE NULLS is not supported (NULLs are excluded)",
                    );
                }
                let (inner_name, _inner_alias, docs) = self.load_table_factor(table, ctes)?;
                let out = unpivot_factor(docs, value, name, columns)?;
                return Ok((
                    "@unpivot".into(),
                    alias
                        .as_ref()
                        .map(|a| a.name.value.clone())
                        .or(Some(inner_name)),
                    out,
                ));
            }
            return err("unsupported FROM item (expected a table, derived table or VALUES)");
        };
        if let Some(args) = &args {
            // T-SQL/standard table-valued functions with a fixed, known
            // output shape (STRING_SPLIT, GENERATE_SERIES, OPENJSON):
            // constant arguments only — they materialize once per query.
            // Everything else keeps the loud table-function error.
            let fname = obj_name(name).to_uppercase();
            if !matches!(
                fname.as_str(),
                "STRING_SPLIT" | "GENERATE_SERIES" | "OPENJSON"
            ) {
                return err("table functions or FROM hints are not supported");
            }
            let list = &args.args;
            let mut vals = Vec::new();
            for a in list {
                match a {
                    sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(e),
                    ) => vals.push(eval_const(e)?),
                    _ => return err("table function arguments must be values"),
                }
            }
            let Some(alias_obj) = &alias else {
                return err(format!(
                    "{fname} requires a table alias (FROM {fname}(...) AS t)"
                ));
            };
            let alias = alias_obj.name.value.clone();
            let mut docs = match crate::tsql::table_function_checked(&fname, &vals, &mut || {
                self.deadline.check()
            }) {
                Some(Ok(rows)) => rows,
                Some(Err(e)) => return Err(e),
                None => return err(format!("unknown table function: {fname}")),
            };
            let ordinal_on = vals.len() == 3 && matches!(vals[2], Value::Int(n) if n != 0);
            rename_tablefn_docs(alias_obj, &fname, ordinal_on, &mut docs)?;
            return Ok(("@tablefn".into(), Some(alias), docs));
        }
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
            // MSSQL `WITH (NOLOCK)` and friends: reads are snapshot-isolated
            // here; silently accepting locking hints would misstate them.
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
            // Renaming a base table's columns is not implemented; ignoring
            // the list would silently keep the original names.
            return err("column aliases on a table alias are not supported");
        }
        let tname = obj_name(name);
        let alias = alias.as_ref().map(|a| a.name.value.clone());
        // Oracle DUAL: the one-row dummy table. Case-insensitive, works even
        // when no user table named DUAL exists (it always shadows).
        if tname.eq_ignore_ascii_case("DUAL") {
            return Ok(("DUAL".into(), alias, vec![Object::new()]));
        }
        // WITH (...) names shadow real tables for this statement.
        if let Some(docs) = ctes.get(&tname) {
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
        // SQL Server-style catalog names: explicit guidance instead of the
        // confusing "table <last part> does not exist".
        let qualified_lower = qualified.to_ascii_lowercase();
        if qualified_lower.starts_with("sys.") || qualified_lower == "sysobjects" {
            return err(
                "SQL Server catalog views (sys.*/sysobjects) are not supported \
                 (use information_schema or sqlite_master)",
            );
        }
        if qualified.eq_ignore_ascii_case("sqlite_master")
            || qualified.eq_ignore_ascii_case("sqlite_temporal_master")
        {
            // SQLite-dialect compatibility view (EF Core probes it to learn
            // which tables exist). Index rows let clients enumerate CREATE
            // INDEX definitions for schema sync.
            let mut docs: Vec<Object> = self
                .tables
                .iter()
                // Same display rule as information_schema: engine/user
                // storage tables are not listed to readers.
                .filter(|(n, _)| !is_internal_table(n))
                .map(|(n, m)| {
                    Object::from([
                        ("type".into(), Value::Str("table".into())),
                        ("name".into(), Value::Str(n.clone())),
                        ("tbl_name".into(), Value::Str(n.clone())),
                        // Full DDL text (same generator as dump_script): the
                        // EF keep-data rebuild reads it to re-attach
                        // table-level UNIQUE/CHECK/FOREIGN KEY constraints
                        // the model does not know about — an empty text
                        // silently dropped them from every rebuilt table.
                        (
                            "sql".into(),
                            if m.is_view() {
                                Value::Str(String::new())
                            } else {
                                Value::Str(create_table_ddl(n, m))
                            },
                        ),
                    ])
                })
                .collect();
            for (tbl, meta) in self.tables.iter().filter(|(n, _)| !is_internal_table(n)) {
                for d in &meta.index_defs {
                    docs.push(Object::from([
                        ("type".into(), Value::Str("index".into())),
                        ("name".into(), Value::Str(d.name.clone())),
                        ("tbl_name".into(), Value::Str(tbl.clone())),
                        (
                            "sql".into(),
                            // Identifiers quoted: the EF provider replays
                            // this DDL verbatim during its keep-data rebuild,
                            // and a reserved-word table/column name (Order,
                            // Order) failed to parse there — taking the user's
                            // hand-made indexes down with it.
                            Value::Str(format!(
                                "CREATE {}INDEX {iname} ON {tbl} ({col}){pred}",
                                if d.unique { "UNIQUE " } else { "" },
                                iname = crate::stmt::sql_quote_ident(&d.name),
                                tbl = crate::stmt::sql_quote_ident(tbl),
                                col = d
                                    .columns
                                    .iter()
                                    .map(|c| crate::stmt::sql_quote_ident(c))
                                    .collect::<Vec<_>>()
                                    .join(", "),
                                pred = d
                                    .pred
                                    .as_deref()
                                    .map(|p| format!(" WHERE {p}"))
                                    .unwrap_or_default()
                            )),
                        ),
                    ]));
                }
            }
            return Ok(("sqlite_master".into(), alias, docs));
        }
        if qualified.eq_ignore_ascii_case("information_schema.tables")
            || qualified.eq_ignore_ascii_case("information_schema.columns")
        {
            let docs = self.information_schema(&qualified_lower);
            return Ok(("information_schema".into(), alias, docs));
        }
        // Oracle data-dictionary compatibility views (case-insensitive):
        // USER_* and ALL_* carry the same data — DocSQL has one global
        // namespace, so every owner sees every user table.
        let dq = qualified.to_ascii_uppercase();
        if matches!(
            dq.as_str(),
            "ALL_TABLES"
                | "USER_TABLES"
                | "ALL_TAB_COLUMNS"
                | "USER_TAB_COLUMNS"
                | "ALL_INDEXES"
                | "USER_INDEXES"
        ) {
            let owner = "DOCSQL";
            let mut docs: Vec<Object> = Vec::new();
            let user_tables: Vec<(&String, &std::sync::Arc<TableMeta>)> = self
                .tables
                .iter()
                .filter(|(n, _)| !is_internal_table(n))
                .collect();
            match dq.as_str() {
                "ALL_TABLES" | "USER_TABLES" => {
                    for (n, meta) in &user_tables {
                        docs.push(Object::from([
                            ("owner".into(), Value::Str(owner.into())),
                            ("table_name".into(), Value::Str((*n).clone())),
                            ("num_rows".into(), Value::Null),
                            ("blocks".into(), Value::Int(meta.pages.len() as i64)),
                        ]));
                    }
                }
                "ALL_TAB_COLUMNS" | "USER_TAB_COLUMNS" => {
                    for (n, meta) in &user_tables {
                        for (i, col) in meta.columns.iter().enumerate() {
                            docs.push(Object::from([
                                ("owner".into(), Value::Str(owner.into())),
                                ("table_name".into(), Value::Str((*n).clone())),
                                ("column_name".into(), Value::Str(col.clone())),
                                ("data_type".into(), Value::Str("TEXT".into())),
                                (
                                    "nullable".into(),
                                    Value::Str(
                                        if meta.not_null.contains(col) {
                                            "N"
                                        } else {
                                            "Y"
                                        }
                                        .into(),
                                    ),
                                ),
                                ("column_id".into(), Value::Int(i as i64 + 1)),
                            ]));
                        }
                    }
                }
                _ => {
                    for (tbl, meta) in &user_tables {
                        for d in &meta.index_defs {
                            docs.push(Object::from([
                                ("owner".into(), Value::Str(owner.into())),
                                ("index_name".into(), Value::Str(d.name.clone())),
                                ("table_name".into(), Value::Str((*tbl).clone())),
                                (
                                    "uniqueness".into(),
                                    Value::Str(
                                        if d.unique { "UNIQUE" } else { "NONUNIQUE" }.into(),
                                    ),
                                ),
                            ]));
                        }
                    }
                }
            }
            let view = match dq.as_str() {
                v if v.starts_with("ALL_") => v.to_string(),
                v => format!("USER_{}", &v[5..]),
            };
            return Ok((view, alias, docs));
        }
        // User view (CREATE VIEW): expand to the stored query's rows. The
        // expansion re-executes the view body as a plain SELECT over the
        // current catalog (a view of a view recurses naturally). CREATE
        // OR REPLACE can still produce a catalog cycle despite the
        // creation-time checks, so every expansion level is budgeted —
        // a cycle or an absurdly deep chain errors instead of blowing
        // the stack.
        if let Some(meta) = self.tables.get(&tname) {
            if meta.is_view() {
                let _guard = enter_view_expansion()?;
                let body = meta.view_sql.clone().unwrap_or_default();
                let q = Database::plain_read_query(&Database::parse_classified(&body)?)?;
                let ExecOutcome::Rows(r) = self.exec_query(q)? else {
                    return err(format!("view {tname} body must be a SELECT"));
                };
                let docs: Vec<Object> = r
                    .rows
                    .into_iter()
                    .map(|row| r.columns.iter().cloned().zip(row).collect())
                    .collect();
                return Ok((tname, alias, docs));
            }
        }
        let docs = self.table_docs(&tname)?;
        Ok((tname, alias, docs))
    }

    pub(super) fn agg_spec(&self, e: &SqlExpr, group_exprs: &[SqlExpr]) -> Result<AggSpec> {
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
        if let SqlExpr::Function(f) = e {
            if f.name.to_string().eq_ignore_ascii_case("grouping") {
                // GROUPING(expr): expression must be one of the grouping-set
                // expressions (validated here, resolved per set at eval).
                let arg = match &f.args {
                    sqlparser::ast::FunctionArguments::List(list) if list.args.len() == 1 => {
                        match &list.args[0] {
                            sqlparser::ast::FunctionArg::Unnamed(
                                sqlparser::ast::FunctionArgExpr::Expr(inner),
                            ) => inner.clone(),
                            _ => return err("GROUPING argument must be a column expression"),
                        }
                    }
                    _ => return err("GROUPING takes exactly one argument"),
                };
                for (i, g) in group_exprs.iter().enumerate() {
                    let same = format!("{g}") == format!("{arg}")
                        || matches!((g, &arg),
                            (SqlExpr::Identifier(a), SqlExpr::Identifier(b)) if a.value == b.value);
                    if same {
                        return Ok(AggSpec::Grouping { idx: i });
                    }
                }
                return err(format!("GROUPING argument {arg} must appear in GROUP BY"));
            }
            if is_agg_fn(f) {
                let (op, inner, distinct, sep, filter) = agg_parts(f, false)?;
                return Ok(AggSpec::Agg {
                    op,
                    arg: inner,
                    distinct,
                    sep,
                    filter,
                });
            }
        }
        // Composite expressions containing aggregate calls.
        if contains_agg(e) {
            check_group_refs(e, group_exprs)?;
            return Ok(AggSpec::Composite { expr: e.clone() });
        }
        err(format!("unsupported aggregate projection: {e}"))
    }

    /// GROUP BY + aggregates (+ HAVING).
    /// GROUP BY + aggregates (+ HAVING) over one or more grouping sets
    /// (`GROUP BY` is one set; ROLLUP/CUBE/GROUPING SETS expand to several).
    /// Rows of a set that omits an expression emit NULL for it; GROUPING()
    /// distinguishes that from a NULL value.
    pub(super) fn exec_grouped_select(
        &self,
        query: Query,
        select: sqlparser::ast::Select,
        rows: Vec<Object>,
        grouping_sets: Vec<Vec<SqlExpr>>,
    ) -> Result<ExecOutcome> {
        // Union of every set expression, in first-appearance order:
        // projection specs resolve against it, and each set resolves the
        // subset it carries.
        let mut all_exprs: Vec<SqlExpr> = Vec::new();
        for set in &grouping_sets {
            for e in set {
                if !all_exprs.iter().any(|g| format!("{g}") == format!("{e}")) {
                    all_exprs.push(e.clone());
                }
            }
        }
        // Projection must be aggregate functions or group exprs.
        let mut columns = Vec::new();
        let mut agg_specs = Vec::new(); // per column: AggSpec
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(e) => {
                    columns.push(expr_name(e));
                    agg_specs.push(self.agg_spec(e, &all_exprs)?);
                }
                SelectItem::ExprWithAlias { expr, alias, .. } => {
                    columns.push(alias.value.clone());
                    agg_specs.push(self.agg_spec(expr, &all_exprs)?);
                }
                _ => return err("unsupported item in aggregate SELECT"),
            }
        }
        let mut out: Vec<Vec<Value>> = Vec::new();
        if let Some(having) = &select.having {
            // Resolve references before evaluating: a bare column that is
            // neither grouped nor projected would silently read NULL and drop
            // every group instead of erroring.
            check_having_refs(having, &all_exprs, &columns)?;
        }
        for set in &grouping_sets {
            // Statement timeout reaches here too: the sets × rows × groups
            // loops below run outside the WHERE scan's own sampling.
            self.deadline.check()?;
            // Group the rows by this set's expressions. Groups hash by the
            // encoded key — the same byte identity DISTINCT uses — instead
            // of a linear scan per row (O(rows × groups)).
            let mut groups: Vec<(Vec<Value>, Vec<&Object>)> = Vec::new();
            let mut group_index: std::collections::HashMap<Vec<u8>, usize> =
                std::collections::HashMap::new();
            for doc in &rows {
                // Inside the row loop too: one set over a big table must
                // stay interruptible (the per-set check above samples only
                // once per set).
                self.deadline.check()?;
                let key: Vec<Value> = set
                    .iter()
                    .map(|e| eval_expr(e, doc))
                    .collect::<Result<_>>()?;
                let kb = row_key(&key);
                match group_index.get(&kb) {
                    Some(&gi) => groups[gi].1.push(doc),
                    None => {
                        group_index.insert(kb, groups.len());
                        groups.push((key, vec![doc]));
                    }
                }
            }
            // An empty set is one group over everything, even with no rows.
            if set.is_empty() && groups.is_empty() {
                groups.push((vec![], vec![]));
            }
            for (key, docs) in &groups {
                self.deadline.check()?;
                let mut row = Vec::with_capacity(agg_specs.len());
                for spec in &agg_specs {
                    row.push(eval_grouped_spec(spec, &all_exprs, set, key, docs)?);
                }
                // HAVING: aggregates evaluate over the group's rows directly;
                // everything else evaluates against the output columns, with
                // this set's expressions consulted before them so group keys
                // resolve and omitted ones read as NULL.
                if let Some(having) = &select.having {
                    let doc: Object = columns.iter().cloned().zip(row.iter().cloned()).collect();
                    if !matches!(eval_group_expr(having, &doc, docs, set)?, Value::Bool(true)) {
                        continue;
                    }
                }
                out.push(row);
            }
        }
        // DISTINCT applies to the projected rows here too: when GROUP BY
        // keys are finer than the projection (or GROUPING SETS repeat a
        // shape) duplicate output rows must fold — the plain path does the
        // same at this position, by encoded row identity. DISTINCT ON stays
        // an explicit unsupported feature instead of silently passing.
        let out = match &select.distinct {
            Some(sqlparser::ast::Distinct::On(cols)) => {
                return err(format!(
                    "DISTINCT ON is not supported: {}",
                    cols.iter().map(expr_name).collect::<Vec<_>>().join(", ")
                ));
            }
            Some(sqlparser::ast::Distinct::Distinct) => {
                let mut seen = std::collections::BTreeSet::new();
                let mut kept = Vec::with_capacity(out.len());
                for row in out {
                    self.deadline.check()?;
                    let key = encode::encode_to_vec(&Value::Array(row.clone()))
                        .map_err(SqlError::Encode)?;
                    if seen.insert(key) {
                        kept.push(row);
                    }
                }
                kept
            }
            None | Some(sqlparser::ast::Distinct::All) => out,
        };
        let out = self.apply_order_limit(query, out, &columns, None, None)?;
        Ok(ExecOutcome::Rows(QueryResult { columns, rows: out }))
    }

    /// No aggregation: project expressions over rows.
    pub(super) fn exec_plain_select(
        &self,
        query: Query,
        mut select: sqlparser::ast::Select,
        rows: Vec<Object>,
        pre_windowed: bool,
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
            // returned fewer columns than the statement asked for. The
            // ROWNUM pseudo-column (if injected) is not part of * — Oracle
            // semantics.
            let mut cols: Vec<String> = union_of_fields(&rows)
                .into_iter()
                .filter(|c| c != "ROWNUM")
                .collect();
            if cols.is_empty() {
                // Empty rowset: `SELECT *` still owes its column metadata —
                // ADO readers (EF SchemaSync's ExistingColumns via
                // `SELECT * … LIMIT 0`) learn the shape from the names, and
                // a data-derived union left them with ZERO columns on an
                // empty table. Declared columns of the FROM factors are the
                // shape; the SAME qualification rule as load_from applies:
                // a solo table keeps bare names, anything joined carries
                // "alias.col" keys (the merged-row key shape) — matching
                // what the same query returns with rows present. Views keep
                // zero columns honestly (their shape is the stored query's,
                // not the catalog's).
                let joined = select.from.len() > 1
                    || select.from.first().is_some_and(|f| !f.joins.is_empty());
                let mut v: Vec<String> = Vec::new();
                for twj in &select.from {
                    collect_star_columns(&twj.relation, self.tables, &mut v, joined);
                    for j in &twj.joins {
                        collect_star_columns(&j.relation, self.tables, &mut v, true);
                    }
                }
                v.sort();
                v.dedup();
                cols = v;
            }
            cols.extend(project.iter().map(|(n, _)| n.clone()));
            cols
        } else {
            project.iter().map(|(n, _)| n.clone()).collect()
        };

        let mut docs = rows;
        // SELECT-list + QUALIFY window functions: compute over the filtered
        // rowset (SQL window semantics — after WHERE, before DISTINCT/ORDER
        // BY/QUALIFY), then rewrite each call site into a reference of its
        // private computed column so projection/QUALIFY evaluation is plain
        // column reads.
        let mut win_calls = window_calls_of(&select.projection);
        if let Some(q) = &select.qualify {
            for c in window_calls_of_expr(q) {
                if !win_calls.iter().any(|w| format!("{w}") == format!("{c}")) {
                    win_calls.push(c);
                }
            }
        }
        let mut win_tmps: Vec<String> = Vec::new();
        if !win_calls.is_empty() {
            if pre_windowed {
                return err("internal: window function reached the pre-windowed fast path");
            }
            // Declared columns of the FROM tables: a column ADDed without a
            // DEFAULT never appears in stored rows, so the row-key universe
            // alone would misjudge its window references as unknown.
            let mut from_cols: std::collections::HashSet<String> = std::collections::HashSet::new();
            for twj in &select.from {
                collect_factor_columns(&twj.relation, self.tables, &mut from_cols);
                for j in &twj.joins {
                    collect_factor_columns(&j.relation, self.tables, &mut from_cols);
                }
            }
            win_tmps = self.compute_windows(&win_calls, &mut docs, &from_cols)?;
            for (_, e) in project.iter_mut() {
                rewrite_windows(e, &win_calls, &win_tmps);
            }
        }
        // QUALIFY (SQL:1999 T618, Snowflake/DuckDB/Teradata spelling):
        // row-local filter over the windowed rowset — after windows, before
        // DISTINCT/ORDER BY. Identical window calls share the projection's
        // computed columns (dedupe by rendered key). A windowless QUALIFY
        // (plain columns only) is a late WHERE and filters the same way —
        // dropping the filter would silently return unqualified rows.
        if let Some(q) = &mut select.qualify {
            rewrite_windows(q, &win_calls, &win_tmps);
            let outer_from = {
                let mut s = OuterFrom::new();
                from_list_names(&select.from, &mut s);
                s
            };
            docs.retain(|doc| {
                matches!(
                    self.eval_row_expr(q, expr_has_subquery(q), doc, &outer_from),
                    Ok(Value::Bool(true))
                )
            });
        }
        let mut out: Vec<Vec<Value>> = Vec::new();
        // Correlated-subquery support: projection expressions still holding
        // subquery nodes resolve their outer references per row.
        let outer_from = {
            let mut s = OuterFrom::new();
            from_list_names(&select.from, &mut s);
            s
        };
        let corr: Vec<bool> = project.iter().map(|(_, e)| expr_has_subquery(e)).collect();
        for doc in &docs {
            // The projection loop is the only per-row work a WHERE-less
            // SELECT does over a big table — keep the statement timeout
            // armed here (the WHERE path samples inside matches_in).
            self.deadline.check()?;
            if want_star {
                let mut row: Vec<Value> = columns_out
                    .iter()
                    .map(|c| doc.get(c).cloned().unwrap_or(Value::Null))
                    .collect();
                // The trailing project.len() slots are the explicit exprs.
                let base = columns_out.len() - project.len();
                for (i, (_, e)) in project.iter().enumerate() {
                    row[base + i] = self.eval_row_expr(e, corr[i], doc, &outer_from)?;
                }
                out.push(row);
            } else {
                let mut row = Vec::with_capacity(project.len());
                for (i, (_, e)) in project.iter().enumerate() {
                    row.push(self.eval_row_expr(e, corr[i], doc, &outer_from)?);
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
                    self.deadline.check()?;
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
        if !pre_windowed {
            out =
                self.apply_order_limit(query, out, &columns_out, Some(docs), Some(&outer_from))?;
        }
        Ok(ExecOutcome::Rows(QueryResult {
            columns: columns_out,
            rows: out,
        }))
    }

    /// Evaluate one SELECT list's window functions over the (already
    /// WHERE-filtered) rows and inject the results as private `__w<i>$`
    /// columns. Row order in `rows` is the stable scan order: it defines
    /// the arbitrary-but-deterministic order when the OVER spec carries no
    /// ORDER BY, and the tie order the stable sort preserves. Returns the
    /// private column chosen per call (collision-free against the rows' own
    /// fields) for the projection rewrite to reference.
    pub(super) fn compute_windows(
        &self,
        calls: &[Function],
        rows: &mut [Object],
        from_cols: &std::collections::HashSet<String>,
    ) -> Result<Vec<String>> {
        // Column universe for the unknown-column guard below, doubling as
        // the name pool the private `__w<i>$` columns must not collide with.
        // Row keys carry the JOIN-qualified names; `from_cols` adds the
        // tables' declared columns (present in meta even when no stored row
        // carries them yet).
        let mut occupied: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut cols: std::collections::HashSet<String> = std::collections::HashSet::new();
        for r in rows.iter() {
            for k in r.keys() {
                occupied.insert(k.clone());
                cols.insert(k.clone());
            }
        }
        for c in from_cols {
            occupied.insert(c.clone());
            cols.insert(c.clone());
        }
        let mut tmp_names: Vec<String> = Vec::with_capacity(calls.len());
        for (ci, f) in calls.iter().enumerate() {
            self.deadline.check()?;
            let fname = f.name.to_string().to_uppercase();
            let aggregate = is_agg_name(f);
            let spec = validate_window_spec(f, &fname, aggregate)?;
            // Custom frame (ROWS/RANGE/GROUPS BETWEEN … AND …): structure
            // checked and offsets const-evaluated once per call.
            let frame_plan = match &spec.window_frame {
                Some(fr) => Some(parse_window_frame(fr, spec.order_by.len())?),
                None => None,
            };
            // Unknown columns evaluate to NULL on every row, which for a
            // window key silently collapses the whole computation (RANK all
            // 1s, running totals over the entire partition) — refuse the
            // statement instead. Resolution mirrors `lookup_col`: the exact
            // key, or any joined "alias.col" key with a matching leaf. An
            // EMPTY rowset carries no column universe — skip (there is
            // nothing to compute anyway) rather than rejecting everything.
            if !rows.is_empty() {
                for e in spec
                    .partition_by
                    .iter()
                    .chain(spec.order_by.iter().map(|o| &o.expr))
                {
                    check_window_columns(e, &cols)?;
                }
                for a in fn_args(f) {
                    check_window_columns(&a, &cols)?;
                }
                if let Some(flt) = &f.filter {
                    check_window_columns(flt, &cols)?;
                }
            }
            let ordered = !spec.order_by.is_empty();
            // Partition keys per row: groups hash by encoded key, the same
            // identity DISTINCT and GROUP BY use.
            let mut groups: Vec<(Vec<u8>, Vec<usize>)> = Vec::new();
            let mut part_index: std::collections::HashMap<Vec<u8>, usize> =
                std::collections::HashMap::new();
            for (i, doc) in rows.iter().enumerate() {
                self.deadline.check()?;
                let mut key = Vec::with_capacity(spec.partition_by.len());
                for p in &spec.partition_by {
                    key.push(eval_expr(p, doc)?);
                }
                let kb = row_key(&key);
                match part_index.get(&kb) {
                    Some(&gi) => groups[gi].1.push(i),
                    None => {
                        part_index.insert(kb.clone(), groups.len());
                        groups.push((kb, vec![i]));
                    }
                }
            }
            // Aggregates fold incrementally over the growing default frame
            // (RunningAgg below); scalar window functions are computed here.
            let agg = if aggregate {
                let (op, arg, distinct, sep, filter) = agg_parts(f, true)?;
                Some((op, arg, distinct, sep, filter))
            } else {
                None
            };
            let mut vals: Vec<Value> = vec![Value::Null; rows.len()];
            for (_, idxs) in &groups {
                self.deadline.check()?;
                // Sorted order within the partition: the outer ORDER BY
                // comparator (cmp_maybe_null + per-key ASC), stable so ties
                // keep scan order. `keyed` pairs each row index with its
                // order-key tuple; peer runs compare consecutive tuples.
                let mut keyed: Vec<(usize, Vec<Value>)> = Vec::with_capacity(idxs.len());
                for &i in idxs {
                    self.deadline.check()?;
                    let mut ks = Vec::with_capacity(spec.order_by.len());
                    for o in &spec.order_by {
                        ks.push(eval_expr(&o.expr, &rows[i])?);
                    }
                    keyed.push((i, ks));
                }
                if ordered {
                    keyed.sort_by(|a, b| {
                        for ((va, vb), o) in a.1.iter().zip(b.1.iter()).zip(&spec.order_by) {
                            let ord = cmp_maybe_null(va, vb, o.options.nulls_first);
                            if ord != Ordering::Equal {
                                return if o.options.asc.unwrap_or(true) {
                                    ord
                                } else {
                                    ord.reverse()
                                };
                            }
                        }
                        Ordering::Equal
                    });
                }
                let sorted: Vec<usize> = keyed.iter().map(|(i, _)| *i).collect();
                let n = sorted.len();
                // Peer runs: run_end[p] / run_start[p] bound the run holding
                // p (consecutive order-key tuples compare equal);
                // group_spans/group_of carry the same layout for GROUPS
                // frame units. Unordered partitions treat every row as its
                // own group.
                let mut run_end = vec![n - 1; n];
                let mut run_start = vec![0usize; n];
                let mut group_spans: Vec<(usize, usize)> = if ordered {
                    Vec::new()
                } else {
                    (0..n).map(|i| (i, i)).collect()
                };
                let mut group_of: Vec<usize> = if ordered {
                    vec![0; n]
                } else {
                    (0..n).collect()
                };
                if ordered {
                    let mut p = 0usize;
                    while p < n {
                        let mut q = p;
                        while q + 1 < n {
                            let equal = spec
                                .order_by
                                .iter()
                                .zip(&keyed[p].1)
                                .zip(&keyed[q + 1].1)
                                .all(|((o, ka), kb)| {
                                    cmp_maybe_null(ka, kb, o.options.nulls_first) == Ordering::Equal
                                });
                            if !equal {
                                break;
                            }
                            q += 1;
                        }
                        run_end[p..=q].fill(q);
                        run_start[p..=q].fill(p);
                        let gi = group_spans.len();
                        group_spans.push((p, q));
                        group_of[p..=q].fill(gi);
                        p = q + 1;
                    }
                }
                if let Some((op, arg, distinct, sep, filter)) = &agg {
                    let count_star =
                        matches!(arg, SqlExpr::Identifier(i) if i.value == "__count__");
                    // FILTER first, argument second — exactly the batch
                    // order in eval_agg: an argument expression that only
                    // errors on filtered-out rows (e.g. a bitwise op on a
                    // text row) must not fail the whole window.
                    let mut passes: Vec<bool> = Vec::with_capacity(n);
                    let mut argvals: Vec<Option<Value>> = Vec::with_capacity(n);
                    for &i in &sorted {
                        self.deadline.check()?;
                        let kept = match filter {
                            Some(flt) => matches!(eval_expr(flt, &rows[i])?, Value::Bool(true)),
                            None => true,
                        };
                        passes.push(kept);
                        argvals.push(if kept {
                            Some(eval_expr(arg, &rows[i])?)
                        } else {
                            None
                        });
                    }
                    // Custom frame: per-row [lo, hi] bounds over the sorted
                    // partition.
                    let bounds = match &frame_plan {
                        Some(p) => Some(frame_bounds(
                            p,
                            &keyed,
                            &spec.order_by,
                            &group_spans,
                            &group_of,
                            n,
                        )?),
                        None => None,
                    };
                    match bounds {
                        None => {
                            // Default frame = partition start through the
                            // current row's peer run (RANGE). Without ORDER
                            // BY the frame is the whole partition. Every
                            // frame is a PREFIX of the partition, so one
                            // left-to-right fold reproduces the batch
                            // `eval_agg` over each frame exactly (same
                            // evaluation order, same NULL/DISTINCT/FILTER
                            // and tie rules) in O(n) evaluations instead of
                            // re-folding each frame O(n²).
                            let mut run =
                                RunningAgg::new(op.clone(), sep.clone(), *distinct, count_star);
                            let mut pushed = 0usize; // sorted rows [0..pushed) are folded in
                            let mut p = 0usize;
                            while p < n {
                                let end = run_end[p];
                                while pushed <= end {
                                    run.push_row(passes[pushed], argvals[pushed].as_ref())?;
                                    pushed += 1;
                                }
                                let v = run.snapshot();
                                for r in p..=end {
                                    vals[sorted[r]] = v.clone();
                                }
                                p = end + 1;
                            }
                        }
                        Some(bounds) => {
                            // Additive aggregates and MIN/MAX invert: the
                            // frame slides in O(n) total. DISTINCT and
                            // GROUP_CONCAT cannot invert — a fresh fold per
                            // row (the GROUP_CONCAT text is inherently
                            // frame-sized anyway).
                            let invertible = matches!(
                                op,
                                AggOp::Count | AggOp::Sum | AggOp::Avg | AggOp::Min | AggOp::Max
                            ) && !distinct;
                            if invertible {
                                let mut win = SlidingFrameAgg::new(op.clone(), count_star);
                                let mut next = 0usize; // next sorted position to push
                                for p in 0..n {
                                    self.deadline.check()?;
                                    let (lo, hi) = bounds[p];
                                    if hi < lo || win.degraded {
                                        // Empty frame (a value interval that
                                        // excludes the key) or unrecoverable
                                        // sliding state: fresh fold of the
                                        // slice — an empty slice keeps the
                                        // empty aggregate value (COUNT 0,
                                        // SUM/AVG/MIN/MAX NULL).
                                        let mut run = RunningAgg::new(
                                            op.clone(),
                                            sep.clone(),
                                            false,
                                            count_star,
                                        );
                                        for r in lo..=hi {
                                            run.push_row(passes[r], argvals[r].as_ref())?;
                                        }
                                        vals[sorted[p]] = run.snapshot();
                                        continue;
                                    }
                                    while next <= hi {
                                        win.push(next, passes[next], argvals[next].as_ref())?;
                                        next += 1;
                                    }
                                    win.pop_to(lo);
                                    if win.degraded {
                                        // The retraction this very row triggered
                                        // was unrecoverable: fold the frame fresh
                                        // (future rows keep taking this branch).
                                        let mut run = RunningAgg::new(
                                            op.clone(),
                                            sep.clone(),
                                            false,
                                            count_star,
                                        );
                                        for r in lo..=hi {
                                            run.push_row(passes[r], argvals[r].as_ref())?;
                                        }
                                        vals[sorted[p]] = run.snapshot();
                                        continue;
                                    }
                                    vals[sorted[p]] = win.value();
                                }
                            } else {
                                for p in 0..n {
                                    self.deadline.check()?;
                                    let (lo, hi) = bounds[p];
                                    let mut run = RunningAgg::new(
                                        op.clone(),
                                        sep.clone(),
                                        *distinct,
                                        count_star,
                                    );
                                    for r in lo..=hi {
                                        run.push_row(passes[r], argvals[r].as_ref())?;
                                    }
                                    vals[sorted[p]] = run.snapshot();
                                }
                            }
                        }
                    }
                } else {
                    let args = fn_args(f);
                    // A frame clause rides only the value functions:
                    // ranking/offset functions have frame-independent
                    // semantics (SQL standard: they don't accept one).
                    let takes_frame = matches!(fname.as_str(), "FIRST_VALUE" | "LAST_VALUE");
                    if frame_plan.is_some() && !takes_frame {
                        return err(format!(
                            "window function {fname} does not accept a frame clause"
                        ));
                    }
                    // FIRST/LAST_VALUE walk the frame: default bounds are
                    // partition start .. peer-run end (the RANGE default).
                    let scalar_bounds = if takes_frame {
                        Some(match &frame_plan {
                            Some(p) => {
                                frame_bounds(p, &keyed, &spec.order_by, &group_spans, &group_of, n)?
                            }
                            None => (0..n).map(|p| (0, run_end[p])).collect(),
                        })
                    } else {
                        None
                    };
                    match fname.as_str() {
                        "ROW_NUMBER" | "RANK" | "DENSE_RANK" => {
                            if !args.is_empty() {
                                return err(format!("function {fname} takes no arguments"));
                            }
                            let mut dense = 0i64;
                            let mut p = 0usize;
                            while p < n {
                                self.deadline.check()?;
                                let end = run_end[p];
                                dense += 1;
                                for r in p..=end {
                                    vals[sorted[r]] = match fname.as_str() {
                                        "ROW_NUMBER" => Value::Int(r as i64 + 1),
                                        "RANK" => Value::Int(p as i64 + 1),
                                        _ => Value::Int(dense),
                                    };
                                }
                                p = end + 1;
                            }
                        }
                        "NTILE" => {
                            let buckets = match eval_const(args.first().ok_or_else(|| {
                                SqlError::Message(
                                    "function NTILE takes exactly 1 argument, got 0".into(),
                                )
                            })?)? {
                                Value::Int(b) if b >= 1 => b as usize,
                                other => {
                                    return err(format!(
                                        "NTILE buckets must be an integer >= 1, got {}",
                                        other.type_name()
                                    ));
                                }
                            };
                            let q = n / buckets;
                            let r = n % buckets;
                            for (p, &ri) in sorted.iter().enumerate() {
                                self.deadline.check()?;
                                let b = if q == 0 {
                                    p
                                } else if p < r * (q + 1) {
                                    p / (q + 1)
                                } else {
                                    r + (p - r * (q + 1)) / q
                                };
                                vals[ri] = Value::Int(b as i64 + 1);
                            }
                        }
                        "LAG" | "LEAD" => {
                            if args.is_empty() || args.len() > 3 {
                                return err(format!(
                                    "function {fname} takes 1 to 3 arguments, got {}",
                                    args.len()
                                ));
                            }
                            let offset = match args.get(1).map(eval_const).transpose()? {
                                None => 1i64,
                                Some(Value::Int(o)) if o >= 0 => o,
                                Some(other) => {
                                    return err(format!(
                                        "{fname} offset must be a non-negative integer, got {}",
                                        other.type_name()
                                    ));
                                }
                            };
                            for (p, &ri) in sorted.iter().enumerate() {
                                self.deadline.check()?;
                                let target: Option<usize> = if fname == "LAG" {
                                    p.checked_sub(offset as usize)
                                } else {
                                    p.checked_add(offset as usize).filter(|&t| t < n)
                                };
                                vals[ri] = match target {
                                    Some(t) => eval_expr(&args[0], &rows[sorted[t]])?,
                                    None => match args.get(2) {
                                        Some(d) => eval_expr(d, &rows[ri])?,
                                        None => Value::Null,
                                    },
                                };
                            }
                        }
                        "FIRST_VALUE" | "LAST_VALUE" => {
                            if args.len() != 1 {
                                return err(format!(
                                    "function {fname} takes exactly 1 argument, got {}",
                                    args.len()
                                ));
                            }
                            let bounds = scalar_bounds
                                .as_ref()
                                .expect("computed for the frame-taking functions");
                            for (p, &ri) in sorted.iter().enumerate() {
                                self.deadline.check()?;
                                let (lo, hi) = bounds[p];
                                let target = if fname == "FIRST_VALUE" { lo } else { hi };
                                vals[ri] = eval_expr(&args[0], &rows[sorted[target]])?;
                            }
                        }
                        other => {
                            return err(format!("unknown window function: {other}"));
                        }
                    }
                }
            }
            // Private column name: never collide with a user field — a
            // user column literally named `__w0$` would otherwise be
            // silently overwritten by the injected window value.
            let mut k = ci;
            let tmp = loop {
                let cand = window_tmp_col(k);
                if occupied.insert(cand.clone()) {
                    break cand;
                }
                k += 1;
            };
            tmp_names.push(tmp.clone());
            for (i, v) in vals.into_iter().enumerate() {
                rows[i].insert(tmp.clone(), v);
            }
        }
        Ok(tmp_names)
    }

    /// Load a FROM list into merged rows: the base table plus its JOINs,
    /// then comma-separated entries as cross joins. A lone base table keeps
    /// unqualified field names; anything joined gets "alias.col" keys.
    /// A solo real table with a probe-able WHERE skips the heap scan.
    pub(super) fn load_from(
        &self,
        from: &[sqlparser::ast::TableWithJoins],
        selection: &Option<SqlExpr>,
        ctes: &Ctes,
    ) -> Result<Vec<Object>> {
        use sqlparser::ast::{JoinConstraint, JoinOperator};
        let base = &from[0];
        // Index fast path: solo base table, WHERE narrows to an indexed
        // column. Rows come straight from the B+ tree; the caller's residual
        // WHERE filter still runs over them.
        if base.joins.is_empty() && from.len() == 1 {
            if let sqlparser::ast::TableFactor::Table { name, alias, .. } = &base.relation {
                let tname = obj_name(name);
                // Same gates as the window paths: a non-plain table factor
                // (TABLESAMPLE / index hints / alias column lists / …) must
                // reach load_table_factor's loud rejections instead of being
                // silently executed as a plain scan, and compat views (dual,
                // sqlite_master, …) must not be shadowed by same-named user
                // tables depending on index presence.
                if plain_table_factor(&base.relation)
                    && !is_compat_view(&tname)
                    && self.tables.contains_key(&tname)
                    && !ctes.contains_key(&tname)
                {
                    let akey = alias.as_ref().map(|a| a.name.value.clone());
                    if let Some(pairs) =
                        self.index_probe(&tname, akey.as_deref(), selection, ctes)?
                    {
                        return Ok(pairs.into_iter().map(|(_, d)| d).collect());
                    }
                }
            }
        }
        let (bname, balias, mut bdocs) = self.load_table_factor(&base.relation, ctes)?;
        let bkey = balias.unwrap_or_else(|| bname.clone());
        let solo = base.joins.is_empty() && from.len() == 1;
        let mut rows: Vec<Object> = if solo {
            bdocs
        } else {
            bdocs.drain(..).map(|d| qualify(&d, &bkey)).collect()
        };
        // Accumulated LEFT key shape for RIGHT/FULL joins: rows carry bare
        // base columns plus qualified keys of earlier joins; an empty left
        // side cannot be sampled, so the planned list answers instead.
        let mut left_cols: Option<Vec<String>> = if solo {
            None
        } else {
            self.table_factor_columns(&base.relation, ctes)
                .map(|cs| cs.iter().map(|c| format!("{bkey}.{c}")).collect())
        };
        let mut all_joins: Vec<&sqlparser::ast::Join> = base.joins.iter().collect();
        if !solo {
            for twj in &from[1..] {
                // comma-separated FROM entries: cross join their base tables
                let (n, a, d) = self.load_table_factor(&twj.relation, ctes)?;
                let k = a.unwrap_or(n);
                let dcols = self.table_factor_columns(&twj.relation, ctes);
                rows = join_rows(
                    rows,
                    &d,
                    &k,
                    None,
                    false,
                    false,
                    self.deadline,
                    dcols.as_deref(),
                    left_cols.as_deref(),
                )?;
                if let (Some(lc), Some(dc)) = (&mut left_cols, &dcols) {
                    lc.extend(dc.iter().map(|c| format!("{k}.{c}")));
                } else {
                    left_cols = None;
                }
                all_joins.extend(twj.joins.iter());
            }
        }
        for j in all_joins {
            // CROSS/OUTER APPLY: the right side is evaluated per left row
            // (T-SQL lateral semantics). Supported over the table-function
            // family; an APPLY over a derived table is a correlated
            // subquery — refused loudly like every other correlation.
            if matches!(
                j.join_operator,
                JoinOperator::CrossApply | JoinOperator::OuterApply
            ) {
                let outer = matches!(j.join_operator, JoinOperator::OuterApply);
                // `CROSS APPLY f(..) AS t` parses as a Table factor with
                // args (same shape as `FROM f(..)`); a factor without args
                // is an APPLY over a subquery/table — a correlated shape
                // the engine refuses like every other correlation.
                if let sqlparser::ast::TableFactor::Table {
                    name,
                    alias,
                    args: Some(tf_args),
                    ..
                } = &j.relation
                {
                    rows = self.apply_table_function(
                        rows,
                        &obj_name(name),
                        alias,
                        &tf_args.args,
                        outer,
                    )?;
                    // Applied rows carry `{alias}.{fn-col}` keys; the base
                    // column shape no longer describes the left side.
                    left_cols = None;
                    continue;
                }
                // OPENJSON keeps its dedicated factor shape in APPLY
                // position (no WITH columns/path here — correlated per-row
                // evaluation of the default key/value/type form).
                if let sqlparser::ast::TableFactor::OpenJsonTable {
                    json_expr,
                    json_path,
                    columns,
                    alias,
                } = &j.relation
                {
                    if !columns.is_empty() || json_path.is_some() {
                        return err("OPENJSON WITH (...) and path arguments are not supported");
                    }
                    let Some(alias_obj) = alias else {
                        return err("OPENJSON requires a table alias (APPLY OPENJSON(...) AS j)");
                    };
                    let akey = alias_obj.name.value.clone();
                    let mut out = Vec::with_capacity(rows.len());
                    let mut produced_cols = false;
                    let mut cols: Vec<String> = tablefn_static_cols("OPENJSON", false)
                        .iter()
                        .map(|s| s.to_string())
                        .collect();
                    for lrow in &rows {
                        self.deadline.check()?;
                        let val = eval_expr(json_expr, lrow)?;
                        let docs = match crate::tsql::table_function("OPENJSON", &[val]) {
                            Some(Ok(docs)) => docs,
                            Some(Err(e)) => return Err(e),
                            None => return err("unknown table function: OPENJSON"),
                        };
                        if docs.is_empty() {
                            if outer {
                                out.push(lrow.clone());
                            }
                            continue;
                        }
                        if !produced_cols {
                            cols = docs[0].keys().cloned().collect();
                            produced_cols = true;
                        }
                        for d in docs {
                            out.push(merged_row(lrow, &qualify(&d, &akey)));
                        }
                    }
                    // Alias column renames (`APPLY OPENJSON(..) AS j(k, v, t)`)
                    // apply positionally over the qualified output keys; an
                    // empty rowset on OUTER APPLY pads explicit NULLs so the
                    // right columns cannot fall back onto same-named left
                    // fields.
                    let rename = table_alias_columns(&alias_obj.columns, cols.len())?;
                    let final_names: Vec<String> = rename.clone().unwrap_or_else(|| cols.clone());
                    let prefix = format!("{akey}.");
                    for row in &mut out {
                        if let Some(rename) = &rename {
                            for (new, old) in rename.iter().zip(cols.iter()) {
                                let old_key = format!("{akey}.{old}");
                                if let Some(v) = row.remove(&old_key) {
                                    row.insert(format!("{akey}.{new}"), v);
                                }
                            }
                        }
                        if outer && !row.keys().any(|k| k.starts_with(&prefix)) {
                            for c in &final_names {
                                row.insert(format!("{akey}.{c}"), Value::Null);
                            }
                        }
                    }
                    rows = out;
                    left_cols = None;
                    continue;
                }
                return err(
                    "APPLY over a subquery is not supported (correlated subqueries); \
                     apply a table function (STRING_SPLIT/GENERATE_SERIES/OPENJSON)",
                );
            }
            let (jname, jalias, jdocs) = self.load_table_factor(&j.relation, ctes)?;
            let jkey = jalias.unwrap_or(jname);
            let jcols = self.table_factor_columns(&j.relation, ctes);
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
                            // a USING b == ON left.b = right.b, with `left`
                            // resolved against the ACCUMULATED left rows: in
                            // a join chain the column may belong to a middle
                            // table, and binding it to the base table read
                            // the merged row as NULL (silently zero rows).
                            let mut e = None;
                            for c in cols {
                                let col = obj_name(c);
                                let suffix = format!(".{col}");
                                let owner = match rows.first() {
                                    Some(sample) => {
                                        let owners: Vec<&str> = sample
                                            .keys()
                                            .filter_map(|k| k.strip_suffix(&suffix))
                                            .collect();
                                        match owners.as_slice() {
                                            [] => {
                                                return err(format!(
                                                    "USING column {col} does not exist on the left side of the join"
                                                ))
                                            }
                                            [one] => (*one).to_string(),
                                            _ => {
                                                return err(format!(
                                                    "USING column {col} is ambiguous on the left side of the join"
                                                ))
                                            }
                                        }
                                    }
                                    // No rows yet: nothing to verify against;
                                    // keep the historical base-table binding.
                                    None => bkey.clone(),
                                };
                                let eq = SqlExpr::BinaryOp {
                                    left: Box::new(SqlExpr::Identifier(
                                        sqlparser::ast::Ident::new(format!("{owner}.{col}")),
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
                        // NATURAL would need the common-column set; the
                        // schemaless row shape has no schema to derive it at
                        // plan time, so refuse instead of cross-joining.
                        JoinConstraint::Natural => {
                            return err("NATURAL JOIN is not supported (use ON/USING)")
                        }
                        // JOIN without ON: MySQL-style cross join.
                        JoinConstraint::None => None,
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
                other => return err(format!("unsupported join type: {}", join_op_name(other))),
            };
            rows = join_rows(
                rows,
                &jdocs,
                &jkey,
                on.as_ref(),
                left_join,
                right_join,
                self.deadline,
                jcols.as_deref(),
                left_cols.as_deref(),
            )?;
            if let (Some(lc), Some(jc)) = (&mut left_cols, &jcols) {
                lc.extend(jc.iter().map(|c| format!("{jkey}.{c}")));
            } else {
                left_cols = None;
            }
        }
        Ok(rows)
    }

    pub(super) fn exec_select(
        &self,
        mut query: Query,
        mut select: sqlparser::ast::Select,
        ctes: &Ctes,
    ) -> Result<ExecOutcome> {
        reject_unsupported_select_clauses(&select)?;
        // Outer scope for correlated subqueries: this level's FROM names and
        // aliases. WHERE / projection / ORDER BY are row-local surfaces and
        // may keep correlated nodes for per-row evaluation; JOIN ON, GROUP BY
        // and HAVING evaluate outside a full row (partial join images, group
        // images), where correlation must stay a loud error.
        let outer_from = {
            let mut s = OuterFrom::new();
            from_list_names(&select.from, &mut s);
            s
        };
        // Resolve uncorrelated subqueries up front so the row-local
        // expression evaluator never sees them.
        if let Some(sel) = &mut select.selection {
            self.subst_expr_scoped(sel, &outer_from, true)?;
        }
        // QUALIFY is a row-local post-window surface (same phase as WHERE,
        // evaluated after window computation) — uncorrelated subqueries
        // resolve up front like WHERE's.
        if let Some(q) = &mut select.qualify {
            self.subst_expr_scoped(q, &outer_from, true)?;
        }
        // JOIN ON conditions can carry uncorrelated subqueries too
        // (`ON a.x = (SELECT MIN(y) FROM b)`); without the same substitution
        // the row-local evaluator misreports them as correlated.
        for twj in &mut select.from {
            for j in &mut twj.joins {
                if let Some(e) = join_on_expr_mut(&mut j.join_operator) {
                    self.subst_expr_scoped(e, &outer_from, false)?;
                }
            }
        }
        // ORDER BY expressions too: EF Core emits `ORDER BY (SELECT 1)` for
        // Skip/Take without an explicit ordering key.
        if let Some(ob) = &mut query.order_by {
            if let sqlparser::ast::OrderByKind::Expressions(exprs) = &mut ob.kind {
                for o in exprs.iter_mut() {
                    self.subst_expr_scoped(&mut o.expr, &outer_from, true)?;
                }
            }
        }
        for item in &mut select.projection {
            match item {
                SelectItem::UnnamedExpr(e) => self.subst_expr_scoped(e, &outer_from, true)?,
                SelectItem::ExprWithAlias { expr, .. } => {
                    self.subst_expr_scoped(expr, &outer_from, true)?
                }
                _ => {}
            }
        }
        if let sqlparser::ast::GroupByExpr::Expressions(es, _) = &mut select.group_by {
            for e in es {
                self.subst_expr_scoped(e, &outer_from, false)?;
            }
        }
        if let Some(having) = &mut select.having {
            self.subst_expr_scoped(having, &outer_from, false)?;
        }

        if select.from.is_empty() {
            if select.qualify.is_some() {
                return err("QUALIFY requires a FROM clause (it filters windowed rows)");
            }
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
            // must return zero rows, not one) and the standard FETCH clause
            // (same truncation semantics). ORDER BY over a single row cannot
            // reorder anything.
            let mut rows = vec![row];
            if let Some(lc) = &query.limit_clause {
                rows = apply_limit_clause(lc, rows)?;
            }
            if let Some(fetch) = &query.fetch {
                if fetch.percent {
                    return err("FETCH … PERCENT is not supported");
                }
                if fetch.with_ties {
                    return err("FETCH … WITH TIES requires ORDER BY");
                }
                let n = match fetch.quantity.as_ref().map(eval_const).transpose()? {
                    Some(Value::Int(n)) if n >= 0 => n as usize,
                    Some(Value::Int(n)) => {
                        return err(format!("FETCH quantity must be non-negative ({n})"))
                    }
                    Some(_) | None => return err("FETCH quantity must be an integer"),
                };
                rows.truncate(n);
            }
            return Ok(ExecOutcome::Rows(QueryResult { columns, rows }));
        }
        // Oracle ROWNUM pseudo-column: number each row right after it is
        // retrieved (before WHERE and before ORDER BY — Oracle semantics).
        // Injected only when the statement references it, and only for the
        // non-aggregated path (aggregates + ROWNUM is refused below).
        let group_exprs: Vec<SqlExpr> = match &select.group_by {
            sqlparser::ast::GroupByExpr::Expressions(e, modifiers) => {
                // MySQL `WITH ROLLUP` / ClickHouse `WITH TOTALS` modifiers
                // would be silently dropped; the standard spellings are
                // GROUP BY ROLLUP(...) / CUBE(...) / GROUPING SETS.
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
        let grouping_sets = expand_grouping_sets(&group_exprs)?;
        let is_aggregate = !group_exprs.is_empty() || select.projection.iter().any(is_agg_item);
        // With GROUP BY/aggregates ROWNUM has no row stream to number — the
        // schemaless NULL read silently passed `ROWNUM <= n` filters, so
        // refuse loudly instead.
        let refs_rownum = select_refs_rownum(&select);
        if refs_rownum && is_aggregate {
            return err("ROWNUM is not supported combined with GROUP BY or aggregate projections");
        }
        let rownum_wanted = refs_rownum && !is_aggregate;
        let distinct_all = matches!(&select.distinct, None | Some(sqlparser::ast::Distinct::All));
        // SELECT-list window functions (`… OVER (…)`) need the full
        // filtered rowset: the ORDER BY+LIMIT fast paths below would hand
        // them a truncated window, so they are gated off, and mixing with
        // GROUP BY/aggregates is refused up front (window-over-groups is
        // unsupported; refusals stay loud).
        let has_window = !window_calls_of(&select.projection).is_empty()
            || select
                .qualify
                .as_ref()
                .is_some_and(|q| !window_calls_of_expr(q).is_empty());
        if has_window && is_aggregate {
            return err("window functions (OVER) are not supported combined with \
                 GROUP BY or aggregate projections");
        }
        if select.qualify.is_some() && is_aggregate {
            return err("QUALIFY is not supported combined with \
                 GROUP BY or aggregate projections");
        }

        // `SELECT COUNT(*) FROM t` without WHERE/GROUP BY: count live heap
        // slots instead of decoding every document.
        if is_aggregate {
            if let Some(out) = self.try_count_star(&query, &select, ctes)? {
                return Ok(out);
            }
        }

        // ORDER BY <index key> + LIMIT: index-ordered window, skipping the
        // full heap scan and sort. Already filtered and windowed; the WHERE
        // pass below would be a no-op, so it is skipped too.
        let mut pre_windowed = false;
        let mut rows = if !is_aggregate
            && !rownum_wanted
            && select.having.is_none()
            && distinct_all
            && !has_window
        {
            match self.ordered_index_window(
                &select,
                &select.from,
                &select.selection,
                &query.order_by,
                &query,
                ctes,
            )? {
                Some(rows) => {
                    pre_windowed = true;
                    rows
                }
                None => match self.unindexed_order_window(&select, &query, ctes)? {
                    Some(rows) => {
                        pre_windowed = true;
                        rows
                    }
                    None => self.load_from(&select.from, &select.selection, ctes)?,
                },
            }
        } else {
            self.load_from(&select.from, &select.selection, ctes)?
        };

        if rownum_wanted {
            for (i, row) in rows.iter_mut().enumerate() {
                row.insert("ROWNUM".into(), Value::Int(i as i64 + 1));
            }
        }

        // WHERE — consuming pass: matching docs move into the kept vec
        // instead of being whole-document cloned (the filtered set is
        // often the whole table).
        if !pre_windowed && select.selection.is_some() {
            let mut kept = Vec::with_capacity(rows.len());
            for doc in rows.drain(..) {
                if self.matches_in(&select.selection, &doc, &outer_from)? {
                    kept.push(doc);
                }
            }
            rows = kept;
        }

        // Aggregation path: aggregate functions in projection or GROUP BY.
        if is_aggregate {
            return self.exec_grouped_select(query, select, rows, grouping_sets);
        }
        if select.having.is_some() {
            // Silently dropping the filter would return unfiltered rows.
            return err("HAVING requires GROUP BY or an aggregate");
        }
        self.exec_plain_select(query, select, rows, pre_windowed)
    }

    /// Execute the query body (no WITH: it was materialized by the caller
    /// into `ctes`).
    pub(super) fn exec_query_body(&self, query: Query, ctes: &Ctes) -> Result<ExecOutcome> {
        let query = rewrite_tsql_top(query)?;
        reject_unsupported_query_clauses(&query)?;
        let body = query.body.clone();
        match *body {
            SetExpr::Select(select) => self.exec_select(query.clone(), *select, ctes),
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
                // BY NAME alignment is not implemented: silently running it
                // positionally would pair unrelated columns (and ALL BY NAME
                // would additionally lose the bag semantics to the implicit
                // DISTINCT below).
                if matches!(
                    set_quantifier,
                    SetQuantifier::ByName
                        | SetQuantifier::AllByName
                        | SetQuantifier::DistinctByName
                ) {
                    return err(
                        "UNION/INTERSECT/EXCEPT ... BY NAME is not supported (columns align positionally)".to_string(),
                    );
                }
                // Set operations combine whole result sets: strip per-arm
                // ORDER BY/LIMIT/FETCH so they apply to the combination only.
                // (Leaving `fetch` in applied `FETCH FIRST n ROWS ONLY` to
                // each arm *and* the combination, truncating the result.)
                // The WITH clause is dropped here too: the CTEs are already
                // materialized in `ctes` — re-entering exec_query per arm
                // would re-run every CTE body 2k-1 times on a k-arm chain.
                let bare = Query {
                    order_by: None,
                    limit_clause: None,
                    fetch: None,
                    with: None,
                    ..query.clone()
                };
                let l = self.exec_query_body(
                    Query {
                        body: left,
                        ..bare.clone()
                    },
                    ctes,
                )?;
                let r = self.exec_query_body(
                    Query {
                        body: right,
                        ..bare
                    },
                    ctes,
                )?;
                let (ExecOutcome::Rows(mut lr), ExecOutcome::Rows(rr)) = (l, r) else {
                    return err("set operations require SELECT on both sides");
                };
                // Column *counts* must match; names may differ (left wins).
                if lr.columns.len() != rr.columns.len() {
                    return err(format!(
                        "set operation arms have different column counts (left {}, right {})",
                        lr.columns.len(),
                        rr.columns.len()
                    ));
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
                let rows = self.apply_order_limit(query, lr.rows, &cols, None, None)?;
                Ok(ExecOutcome::Rows(QueryResult {
                    columns: cols,
                    rows,
                }))
            }
            SetExpr::Values(mut values) => {
                // Row-value constructor (standard SQL): constant expressions,
                // one row per parenthesized list, `column1..n` by default.
                let width = values.rows.first().map_or(0, |r| r.content.len());
                let columns: Vec<String> = (1..=width).map(|i| format!("column{i}")).collect();
                let empty = Object::new();
                let mut rows = Vec::with_capacity(values.rows.len());
                for parens in values.rows.drain(..) {
                    let mut exprs = parens.content;
                    if exprs.len() != width {
                        return err("VALUES rows must all have the same column count");
                    }
                    for e in &mut exprs {
                        self.subst_expr(e)?;
                    }
                    rows.push(
                        exprs
                            .iter()
                            .map(|e| eval_expr(e, &empty))
                            .collect::<Result<Vec<_>>>()?,
                    );
                }
                let rows = self.apply_order_limit(query, rows, &columns, None, None)?;
                Ok(ExecOutcome::Rows(QueryResult { columns, rows }))
            }
            SetExpr::Query(inner) => {
                // Parenthesized query body, e.g. `((SELECT 1))`: re-dispatch
                // the inner query (its ORDER BY/LIMIT apply) instead of
                // rejecting the shape. The parenthesized body may carry its
                // OWN WITH — materialize it on top of the current scope
                // rather than dropping it (dropping made `(WITH x AS …
                // SELECT * FROM x)` resolve x to a same-named base table or
                // fail outright).
                let mut q = *inner;
                let inner_with = q.with.take();
                let scoped = self.materialize_ctes(&inner_with, ctes)?;
                self.exec_query_body(q, &scoped)
            }
            other => err(format!("unsupported query body: {other}")),
        }
    }

    pub(super) fn exec_query(&self, query: Query) -> Result<ExecOutcome> {
        let ctes = self.materialize_ctes(&query.with, &Ctes::new())?;
        self.exec_query_body(query, &ctes)
    }

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

    /// Row filter with correlated-subquery support: `outer` names the
    /// enclosing FROM scopes whose row values `doc` carries. A selection
    /// still holding subquery nodes at row time means the pre-pass skipped
    /// them as correlated — substitute this row's outer values, then
    /// evaluate.
    pub(super) fn matches_in(
        &self,
        selection: &Option<SqlExpr>,
        doc: &Object,
        outer: &OuterFrom,
    ) -> Result<bool> {
        self.deadline.check()?;
        match selection {
            None => Ok(true),
            Some(e) if expr_has_subquery(e) => {
                let mut row_local = e.clone();
                self.subst_expr_row(&mut row_local, doc, outer)?;
                Ok(matches!(eval_expr(&row_local, doc)?, Value::Bool(true)))
            }
            Some(e) => Ok(matches!(eval_expr(e, doc)?, Value::Bool(true))),
        }
    }

    pub(crate) fn table_docs(&self, table: &str) -> Result<Vec<Object>> {
        let Some(meta) = self.tables.get(table) else {
            return err(format!("table {table} does not exist"));
        };
        let heap = meta.heap_of();
        heap.scan(&self.reader()).map_err(Into::into)
    }

    pub(super) fn index_probe(
        &self,
        table: &str,
        alias: Option<&str>,
        selection: &Option<SqlExpr>,
        ctes: &Ctes,
    ) -> Result<Option<Vec<(u64, Object)>>> {
        let Some(cond) = selection else {
            return Ok(None);
        };
        let Some(meta) = self.tables.get(table) else {
            return Ok(None);
        };
        if meta.index_roots.is_empty() || !ctes.is_empty() {
            return Ok(None);
        }
        let Some((col, plan, _)) = probe_plan(cond, meta, table, alias) else {
            return Ok(None);
        };
        let root = meta.index_roots[&col];
        let heap = meta.heap_of();
        let tx = self.pager.begin_tx(); // read-only handle: nothing staged, dropping it is the cleanup
        let tree = BTree::open(root);
        let pairs = match self.probe_pairs(&col, &tree, &tx, &plan, None)? {
            Some(pairs) => pairs,
            // 混合带不可安全探测:回退通用扫描,结果不能依赖索引存在性。
            None => return Ok(None),
        };
        // Read-only tx: no staged pages, dropping it is the cleanup.
        drop(tx);
        let mut out: Vec<(u64, Object)> = Vec::with_capacity(pairs.len());
        for (_, loc) in pairs {
            if let Some(doc) = heap.doc_at(&self.reader(), loc)? {
                out.push((loc, doc));
            }
        }
        // Heap order (page, slot) — loc packing already sorts that way.
        out.sort_by_key(|(loc, _)| *loc);
        Ok(Some(out))
    }

    /// Promote a probe plan's string bounds into the tree's key band: a
    /// parseable timestamp string against a TIMESTAMP-keyed tree must
    /// compare chronologically — the raw cmp_values band ranks Str above
    /// every Timestamp, which would silently turn `WHERE ts > '2026-…'`
    /// into an empty result. Sampling min+max keys decides the band (two
    /// leaf reads). Returns `None` when the band is NOT safely probeable:
    ///
    /// * a MIXED band (extremes disagree — Timestamps and Strs coexist in
    ///   the tree): keeping the string literal probes only the Str band and
    ///   silently loses every Timestamp row, while promoting it pulls the
    ///   whole Str band into the range — either way the result set depends
    ///   on whether the index exists. The generic scan path (cmp_coerced)
    ///   is the only correct answer, so callers fall back to it.
    /// * an Eq/Prefix composite bound whose element beyond position 0 would
    ///   be promoted: element purity is only provable at position 0 (the
    ///   primary sort key); a middle key holding the literal STRING at that
    ///   position would be missed by the promoted seek.
    ///
    /// The bool flag reports whether any bound was actually lifted; callers
    /// owning an "exact" plan must demote exactness then (promoted bounds
    /// admit candidates the coercing predicate would exclude).
    pub(super) fn promote_plan_bounds(
        &self,
        tree: &BTree,
        tx: &crate::pager::Tx,
        plan: &ProbePlan,
    ) -> Result<Option<(ProbePlan, bool)>> {
        // ANY string bound (parseable or not) enters the band analysis: an
        // unparseable one used to short-circuit the pre-check as "cannot
        // promote", leaving an exact plan whose raw `k < Str` filter passes
        // EVERY Timestamp key (Timestamps rank below Strs) — the coercing
        // predicate evaluates such a comparison as NULL/unknown and excludes
        // the row, so the window silently returned rows WHERE should have
        // dropped. Whether the string can actually lift is decided per band
        // below.
        let is_str = |v: &Value| -> bool { matches!(v, Value::Str(_)) };
        fn bound_elements(v: &Value) -> Option<&[Value]> {
            match v {
                Value::Array(items) if !items.is_empty() => Some(items),
                _ => None,
            }
        }
        // Cheap pre-check: no string bound anywhere (scalar or in a
        // composite Array) → the plan is untouched and the two leaf reads
        // are not paid.
        let has_str_bound =
            match plan {
                ProbePlan::Eq(v) | ProbePlan::Prefix(v) => {
                    is_str(v) || bound_elements(v).is_some_and(|items| items.iter().any(is_str))
                }
                ProbePlan::In(vals) => vals.iter().any(is_str),
                ProbePlan::Range { lo, hi } => [lo.as_ref(), hi.as_ref()]
                    .into_iter()
                    .flatten()
                    .any(|(v, _)| {
                        is_str(v) || bound_elements(v).is_some_and(|items| items.iter().any(is_str))
                    }),
            };
        // A TIMESTAMP-typed bound (TIMESTAMP '…' literal / CAST fold) needs
        // the same band check from the other side: the coercing predicate
        // (cmp_coerced) lifts Str KEYS to Timestamps, so a raw-band probe
        // against a Str or mixed band silently loses rows (Eq probes match
        // nothing — Timestamps rank below every Str; exact windows return
        // the whole Str band). Only a pure-Timestamp band keeps the probe.
        let ts_positions = |v: &Value| -> Vec<usize> {
            match v {
                Value::Array(items) => items
                    .iter()
                    .enumerate()
                    .filter(|(_, e)| matches!(e, Value::Timestamp(_)))
                    .map(|(i, _)| i)
                    .collect(),
                Value::Timestamp(_) => vec![0],
                _ => Vec::new(),
            }
        };
        let has_ts_bound = match plan {
            ProbePlan::Eq(v) | ProbePlan::Prefix(v) => !ts_positions(v).is_empty(),
            ProbePlan::In(vals) => vals.iter().any(|v| !ts_positions(v).is_empty()),
            ProbePlan::Range { lo, hi } => [lo.as_ref(), hi.as_ref()]
                .into_iter()
                .flatten()
                .any(|(v, _)| !ts_positions(v).is_empty()),
        };
        // Range sweeps with NUMERIC bounds need the band looked at too (Eq /
        // Prefix / In probes with numeric elements are safe without it: a
        // seek that matches nothing mirrors the predicate excluding every
        // row, so only the sweeping shapes can diverge). The predicate's
        // Timestamp↔numeric comparison is NULL — the row is excluded — while
        // the raw band order ranks Timestamps ABOVE numerics, so a numeric
        // bound over a Timestamp(-containing) band admits keys the predicate
        // must exclude (`WHERE ts > 0 ORDER BY ts LIMIT 1` would return a
        // row whose correct answer is zero rows).
        let is_num = |v: &Value| -> bool {
            matches!(v, Value::Int(_) | Value::Float(_) | Value::Decimal(_))
        };
        let num_positions = |v: &Value| -> Vec<usize> {
            match v {
                Value::Array(items) => items
                    .iter()
                    .enumerate()
                    .filter(|(_, e)| is_num(e))
                    .map(|(i, _)| i)
                    .collect(),
                other if is_num(other) => vec![0],
                _ => Vec::new(),
            }
        };
        let has_num_range_bound = match plan {
            ProbePlan::Range { lo, hi } => [lo.as_ref(), hi.as_ref()]
                .into_iter()
                .flatten()
                .any(|(v, _)| !num_positions(v).is_empty()),
            _ => false,
        };
        if !has_str_bound && !has_ts_bound && !has_num_range_bound {
            return Ok(Some((plan.clone(), false)));
        }
        let reader = self.reader();
        let sample = |rev: bool| -> Result<Vec<(Value, u64)>> {
            if rev {
                tree.scan_limited_rev(&reader, tx, Some(1))
                    .map_err(|e| index_err("probe bound", e))
            } else {
                tree.scan_limited(&reader, tx, Some(1))
                    .map_err(|e| index_err("probe bound", e))
            }
        };
        let min_key = sample(false)?.into_iter().next().map(|(k, _)| k);
        let max_key = sample(true)?.into_iter().next().map(|(k, _)| k);
        // Position i of the tree band is Timestamp when both sampled keys
        // carry a Timestamp there (scalar keys are position 0). Because the
        // tree is totally ordered and Timestamps rank below every Str, both
        // extremes being Timestamp PROVES a pure band at the primary sort
        // position; the same holds for both-Str (no Timestamps exist).
        let band_ts_at = |i: usize| -> bool {
            let at = |k: &Option<Value>| -> bool {
                k.as_ref().is_some_and(|k| match k {
                    Value::Timestamp(_) if i == 0 => true,
                    Value::Array(items) => items
                        .get(i)
                        .is_some_and(|e| matches!(e, Value::Timestamp(_))),
                    _ => false,
                })
            };
            at(&min_key) && at(&max_key)
        };
        let band_str_at = |i: usize| -> bool {
            let at = |k: &Option<Value>| -> bool {
                k.as_ref().is_some_and(|k| match k {
                    Value::Str(_) if i == 0 => true,
                    Value::Array(items) => items.get(i).is_some_and(|e| matches!(e, Value::Str(_))),
                    _ => false,
                })
            };
            at(&min_key) && at(&max_key)
        };
        let band_num_at = |i: usize| -> bool {
            let at = |k: &Option<Value>| -> bool {
                k.as_ref().is_some_and(|k| match k {
                    Value::Int(_) | Value::Float(_) | Value::Decimal(_) if i == 0 => true,
                    Value::Array(items) => items.get(i).is_some_and(|e| {
                        matches!(e, Value::Int(_) | Value::Float(_) | Value::Decimal(_))
                    }),
                    _ => false,
                })
            };
            at(&min_key) && at(&max_key)
        };
        // 一致才可判:该位要么纯 Timestamp(提升)、要么纯 Str(保留字面量,
        // 与 cmp_coerced 的 Str↔Str 明文比较一致);两极不一致 = 混合带。
        // 只需检查边界里携带字符串的位 —— 其它位的带型与该边界的比较
        // 语义无关(Int 位永远不会被提升)。
        let str_positions = |v: &Value| -> Vec<usize> {
            match v {
                Value::Array(items) => items
                    .iter()
                    .enumerate()
                    .filter(|(_, e)| is_str(e))
                    .map(|(i, _)| i)
                    .collect(),
                other if is_str(other) => vec![0],
                _ => Vec::new(),
            }
        };
        // Can the string at position i of `v` actually lift to a Timestamp?
        let parseable_at = |v: &Value, i: usize| -> bool {
            match v {
                Value::Array(items) => items.get(i).is_some_and(
                    |e| matches!(e, Value::Str(s) if crate::value::parse_timestamp_ms(s).is_some()),
                ),
                Value::Str(s) => i == 0 && crate::value::parse_timestamp_ms(s).is_some(),
                _ => false,
            }
        };
        let safe_at = |i: usize| band_ts_at(i) || band_str_at(i);
        match plan {
            ProbePlan::Eq(v) | ProbePlan::Prefix(v) => {
                for i in str_positions(v) {
                    if i > 0 {
                        // 复合 Eq/Prefix 的非首元素:纯 Str 带保留字面量即可,
                        // 其它带型(含纯 Timestamp —— 元素纯度不可证)回退。
                        if !band_str_at(i) {
                            return Ok(None);
                        }
                    } else if !(band_str_at(0) || (band_ts_at(0) && parseable_at(v, 0))) {
                        // Timestamp 带上的字符串边界只有真提升成
                        // Timestamp 才与谓词同义;不可解析字符串对每个
                        // Timestamp 键都判 NULL/未知 —— 原样保留字面量会让
                        // 探针/窗口放进谓词必然排除的行,只能整体回退。
                        return Ok(None);
                    }
                }
                for i in ts_positions(v) {
                    // Timestamp 边界只在纯 Timestamp 带上与谓词同义;
                    // Str/混合带回退全扫。
                    if !band_ts_at(i) {
                        return Ok(None);
                    }
                }
            }
            // IN 的每个元素都是一次独立 Eq 探测(位置 0),逐元素套用
            // Eq 的首元素规则:任一元素与带型不谐即整体回退。
            ProbePlan::In(vals) => {
                for v in vals {
                    for i in str_positions(v) {
                        if !(band_str_at(i) || (band_ts_at(i) && parseable_at(v, i))) {
                            return Ok(None);
                        }
                    }
                    for i in ts_positions(v) {
                        if !band_ts_at(i) {
                            return Ok(None);
                        }
                    }
                }
            }
            ProbePlan::Range { lo, hi } => {
                for (v, _) in [lo.as_ref(), hi.as_ref()].into_iter().flatten() {
                    for i in str_positions(v) {
                        if !safe_at(i) {
                            return Ok(None);
                        }
                        // Timestamp 带上的字符串边界必须真能提升;不可解析
                        // 字符串对每个 Timestamp 键判 NULL/未知,原样保留
                        // 会让探针放进(或吃掉)谓词必然排除/包含的行。
                        // 纯 Str 带保留字面量即可,与谓词的 Str↔Str 比较一致。
                        if band_ts_at(i) && !parseable_at(v, i) {
                            return Ok(None);
                        }
                    }
                }
                for i in [lo.as_ref(), hi.as_ref()]
                    .into_iter()
                    .flatten()
                    .flat_map(|(v, _)| ts_positions(v))
                {
                    if !band_ts_at(i) {
                        return Ok(None);
                    }
                }
                // A numeric bound may only sweep a band PROVABLY free of
                // Timestamps. Purity is provable when both extremes agree
                // on one family (anything else ranks strictly outside
                // [min, max]); a band whose extremes disagree may still
                // hold Timestamps in between (numeric … Timestamp … Str),
                // and Str↔numeric stays raw on both sides so a pure Str
                // band is as safe as a pure numeric one.
                for i in [lo.as_ref(), hi.as_ref()]
                    .into_iter()
                    .flatten()
                    .flat_map(|(v, _)| num_positions(v))
                {
                    if !(band_num_at(i) || band_str_at(i)) {
                        return Ok(None);
                    }
                }
            }
        }
        let any_band_ts = match min_key.as_ref() {
            Some(Value::Array(items)) => (0..items.len()).any(band_ts_at),
            Some(_) => band_ts_at(0),
            None => false,
        };
        if !any_band_ts {
            return Ok(Some((plan.clone(), false)));
        }
        let mut promoted_any = false;
        let promote = |v: &Value, promoted_any: &mut bool| -> Value {
            match v {
                Value::Str(s) => {
                    if band_ts_at(0) {
                        if let Some(ms) = crate::value::parse_timestamp_ms(s) {
                            *promoted_any = true;
                            return Value::Timestamp(ms);
                        }
                    }
                    v.clone()
                }
                Value::Array(items) => Value::Array(
                    items
                        .iter()
                        .enumerate()
                        .map(|(i, e)| {
                            if let Value::Str(s) = e {
                                if band_ts_at(i) {
                                    if let Some(ms) = crate::value::parse_timestamp_ms(s) {
                                        *promoted_any = true;
                                        return Value::Timestamp(ms);
                                    }
                                }
                            }
                            e.clone()
                        })
                        .collect(),
                ),
                other => other.clone(),
            }
        };
        let out = match &plan {
            ProbePlan::Eq(v) => ProbePlan::Eq(promote(v, &mut promoted_any)),
            ProbePlan::Prefix(v) => ProbePlan::Prefix(promote(v, &mut promoted_any)),
            ProbePlan::In(vals) => {
                ProbePlan::In(vals.iter().map(|v| promote(v, &mut promoted_any)).collect())
            }
            ProbePlan::Range { lo, hi } => ProbePlan::Range {
                lo: lo
                    .as_ref()
                    .map(|(v, incl)| (promote(v, &mut promoted_any), *incl)),
                hi: hi
                    .as_ref()
                    .map(|(v, incl)| (promote(v, &mut promoted_any), *incl)),
            },
        };
        Ok(Some((out, promoted_any)))
    }

    /// Index entries selected by `plan`, in key order. `max` caps the walk to
    /// the first `max` entries; pass it only when every entry the plan can
    /// return is a result (exact probes), since a non-unique run must not
    /// spend the cap on entries a filter would drop.
    pub(super) fn probe_pairs(
        &self,
        col: &str,
        tree: &BTree,
        tx: &crate::pager::Tx,
        plan: &ProbePlan,
        max: Option<usize>,
    ) -> Result<Option<Vec<(Value, u64)>>> {
        let reader = self.reader();
        let plan = match self.promote_plan_bounds(tree, tx, plan)? {
            Some((p, _)) => p,
            None => return Ok(None),
        };
        let pairs = match &plan {
            ProbePlan::Eq(v) => {
                // Bounded range + equal filter so non-unique trees return
                // every duplicate match (get() would yield one entry).
                // The hi bound lets the tree stop at the first key past v
                // instead of walking the whole right side. For composite
                // indexes v is the full Array key — element-wise cmp_values
                // equality is exactly a key match.
                let mut p = tree
                    .range_bounded_limited(&reader, tx, v, Some((v, true)), max)
                    .map_err(|e| index_err(col, e))?;
                p.retain(|(k, _)| Value::cmp_values(k, v) == Ordering::Equal);
                p
            }
            ProbePlan::Prefix(prefix) => {
                // Composite prefix probe: walk from the prefix (its rank makes
                // Array(prefix) sort right before every key extending it) and
                // stop at the first key that leaves the run — the walk itself
                // yields only prefix matches, so neither a `retain` nor the
                // tail beyond the run is needed. `max` stays ignored, as
                // before: the cap exists for plans whose every entry is a
                // result, and this one keeps nothing else. Walking to the end
                // of the tree and filtering afterwards (the old shape) made
                // `WHERE leading = 'x'` cost the whole index in time and in
                // materialized pairs, however few rows matched.
                let Value::Array(pfx) = prefix else {
                    // Only a composite Array key has a prefix run; no other
                    // shape is probeable — generic path, never a wrong answer.
                    return Ok(None);
                };
                tree.range_prefix_limited(&reader, tx, pfx, None)
                    .map_err(|e| index_err(col, e))?
            }
            ProbePlan::In(vals) => {
                // Multi-probe equality set: one bounded Eq walk per element
                // (the cap per element still covers the global first `max` —
                // each element contributes its smallest keys), merged back
                // into key order because window plans trust it. Distinct
                // elements probe disjoint runs; repeated list entries
                // double-report the same run and dedupe away here.
                let mut pairs: Vec<(Value, u64)> = Vec::new();
                for v in vals {
                    self.deadline.check()?;
                    let mut p = tree
                        .range_bounded_limited(&reader, tx, v, Some((v, true)), max)
                        .map_err(|e| index_err(col, e))?;
                    p.retain(|(k, _)| Value::cmp_values(k, v) == Ordering::Equal);
                    pairs.append(&mut p);
                }
                // 按 (键, locator) 全对排序:重复列表元素探出的同键同
                // locator 对必须相邻才能去重;cmp_values 相等的键可以是
                // 两行(Int(3)/Float(3.0) 同判等,AGENTS 红线 #2),按键
                // 单独去重会丢掉非唯一树的合法同行键。
                pairs.sort_by(|a, b| Value::cmp_values(&a.0, &b.0).then_with(|| a.1.cmp(&b.1)));
                pairs.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);
                if let Some(m) = max {
                    pairs.truncate(m);
                }
                pairs
            }
            ProbePlan::Range { lo, hi } => {
                let hi_ref = hi.as_ref().map(|(v, incl)| (v, *incl));
                let mut pairs = match &lo {
                    Some((v, true)) => tree
                        .range_bounded_limited(&reader, tx, v, hi_ref, max)
                        .map_err(|e| index_err(col, e))?,
                    // Strict lower bound: the walk drops the equal run itself
                    // so the cap counts kept entries only.
                    Some((v, false)) => tree
                        .range_bounded_excl_limited(&reader, tx, v, hi_ref, max)
                        .map_err(|e| index_err(col, e))?,
                    // No lower bound: in-range keys are a prefix of the scan,
                    // so a cap collects them all (or exactly `max` of them).
                    None => tree
                        .scan_limited(&reader, tx, max)
                        .map_err(|e| index_err(col, e))?,
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
        Ok(Some(pairs))
    }

    /// [`Database::probe_pairs`] walking in descending key order — the
    /// `ORDER BY … DESC` window. Same `max` caveats; a prefix scan ignores
    /// it.
    pub(super) fn probe_pairs_rev(
        &self,
        col: &str,
        tree: &BTree,
        tx: &crate::pager::Tx,
        plan: &ProbePlan,
        max: Option<usize>,
    ) -> Result<Option<Vec<(Value, u64)>>> {
        let reader = self.reader();
        let plan = match self.promote_plan_bounds(tree, tx, plan)? {
            Some((p, _)) => p,
            None => return Ok(None),
        };
        let pairs = match &plan {
            ProbePlan::Eq(v) => tree
                .range_bounded_rev_limited(&reader, tx, Some((v, true)), Some((v, true)), max)
                .map_err(|e| index_err(col, e))?,
            ProbePlan::Prefix(prefix) => {
                // Descending mirror of the prefix probe: the same bounded
                // forward run, reversed. `max` stays ignored, as above.
                let Value::Array(pfx) = prefix else {
                    return Ok(None);
                };
                tree.range_prefix_rev_limited(&reader, tx, pfx, None)
                    .map_err(|e| index_err(col, e))?
            }
            ProbePlan::In(vals) => {
                // Descending mirror of the In probe: one bounded Eq walk per
                // element (each takes the element's LAST keys under the cap —
                // their union covers the global last `max`), merged into
                // descending key order for the DESC window.
                let mut pairs: Vec<(Value, u64)> = Vec::new();
                for v in vals {
                    self.deadline.check()?;
                    let mut p = tree
                        .range_bounded_rev_limited(
                            &reader,
                            tx,
                            Some((v, true)),
                            Some((v, true)),
                            max,
                        )
                        .map_err(|e| index_err(col, e))?;
                    p.retain(|(k, _)| Value::cmp_values(k, v) == Ordering::Equal);
                    pairs.append(&mut p);
                }
                // 降序镜像:同键段内按 locator 定序,同对相邻可去重。
                pairs.sort_by(|a, b| Value::cmp_values(&b.0, &a.0).then_with(|| a.1.cmp(&b.1)));
                pairs.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);
                if let Some(m) = max {
                    pairs.truncate(m);
                }
                pairs
            }
            ProbePlan::Range { lo, hi } => {
                let lo_ref = lo.as_ref().map(|(v, incl)| (v, *incl));
                let hi_ref = hi.as_ref().map(|(v, incl)| (v, *incl));
                tree.range_bounded_rev_limited(&reader, tx, lo_ref, hi_ref, max)
                    .map_err(|e| index_err(col, e))?
            }
        };
        Ok(Some(pairs))
    }

    /// `SELECT COUNT(*) FROM t` with no WHERE/GROUP BY/window: count the
    /// heap's live slots without decoding documents. `None` sends anything
    /// more complex down the generic aggregate path.
    pub(super) fn try_count_star(
        &self,
        query: &Query,
        select: &sqlparser::ast::Select,
        ctes: &Ctes,
    ) -> Result<Option<ExecOutcome>> {
        if !ctes.is_empty()
            || select.selection.is_some()
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
            return Ok(None);
        }
        if !plain_table_factor(&select.from[0].relation) {
            return Ok(None);
        }
        let sqlparser::ast::TableFactor::Table { name, .. } = &select.from[0].relation else {
            return Ok(None);
        };
        let Some(meta) = self.real_table_meta(name) else {
            return Ok(None);
        };
        let (column, expr) = match &select.projection[0] {
            SelectItem::UnnamedExpr(e) => (expr_name(e), e),
            SelectItem::ExprWithAlias { expr, alias, .. } => (alias.value.clone(), expr),
            _ => return Ok(None),
        };
        let SqlExpr::Function(f) = expr else {
            return Ok(None);
        };
        if !f.name.to_string().eq_ignore_ascii_case("count")
            || f.filter.is_some()
            || f.over.is_some()
        {
            return Ok(None);
        }
        use sqlparser::ast::{DuplicateTreatment, FunctionArg, FunctionArgExpr, FunctionArguments};
        let is_star = match &f.args {
            FunctionArguments::List(list) => {
                !matches!(list.duplicate_treatment, Some(DuplicateTreatment::Distinct))
                    && list.args.len() == 1
                    && matches!(
                        list.args[0],
                        FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
                    )
            }
            _ => false,
        };
        if !is_star {
            return Ok(None);
        }
        let count = meta.heap_of().live_count(&self.reader())?;
        Ok(Some(ExecOutcome::Rows(QueryResult {
            columns: vec![column],
            rows: vec![vec![Value::Int(count as i64)]],
        })))
    }

    /// A FROM relation that resolves to a real heap table: single-part name,
    /// present in the catalog, and neither a compatibility view that shadows
    /// user tables of the same name in `load_table_factor` nor a user view
    /// (views resolve by expanding their stored SELECT — their own empty
    /// storage must never satisfy a heap fast path).
    pub(super) fn real_table_meta(
        &self,
        name: &sqlparser::ast::ObjectName,
    ) -> Option<&std::sync::Arc<TableMeta>> {
        if name.0.len() != 1 {
            return None;
        }
        let tname = obj_name(name);
        if is_compat_view(&tname) {
            return None;
        }
        self.tables.get(&tname).filter(|m| !m.is_view())
    }
}

/// True when a table factor is a plain `t` / `t AS x` relation with no
/// per-factor clauses. The window/count fast paths test only the variant, so
/// without this `SELECT COUNT(*) FROM t TABLESAMPLE (10)`, table functions
/// (`FROM t(1)`), alias column lists and index hints would be silently
/// ignored instead of rejected like the general path does.
pub(super) fn plain_table_factor(tf: &sqlparser::ast::TableFactor) -> bool {
    let sqlparser::ast::TableFactor::Table {
        args,
        with_hints,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
        alias,
        ..
    } = tf
    else {
        return false;
    };
    args.is_none()
        && with_hints.is_empty()
        && version.is_none()
        && !*with_ordinality
        && partitions.is_empty()
        && json_path.is_none()
        && sample.is_none()
        && index_hints.is_empty()
        && alias.as_ref().is_none_or(|a| a.columns.is_empty())
}

/// Plan of the unindexed top-K window ([`ReadCx::unindexed_order_window`]):
/// the planning prefix shared with EXPLAIN — no rows are touched.
pub(super) struct UnindexedWindowPlan {
    table: String,
    /// Per ORDER BY key: candidate field names (first hit wins), ASC?,
    /// NULLS FIRST override.
    keys: Vec<(Vec<String>, bool, Option<bool>)>,
    skip: usize,
    take: usize,
}

/// Plan of the ordered index window ([`ReadCx::ordered_index_window`]):
/// the planning prefix (through the band-promoted probe bounds) shared
/// with EXPLAIN — no rows are touched.
pub(super) struct OrderedWindowPlan {
    table: String,
    root_key: String,
    asc: bool,
    skip: usize,
    take: usize,
    /// The probe subsumes the whole WHERE: OFFSET rows are only counted,
    /// never decoded.
    exact: bool,
    /// Band-promoted probe bounds; None = whole-tree walk.
    probe: Option<ProbePlan>,
}

impl<'a> ReadCx<'a> {
    /// Planning prefix of [`Self::unindexed_order_window`], shared with
    /// EXPLAIN: every gate from the fast path's entry through the ORDER BY
    /// key extraction — no row loading. `None` = this query does not take
    /// the top-K window (caller falls through to the generic scan).
    pub(super) fn unindexed_window_plan(
        &self,
        select: &sqlparser::ast::Select,
        query: &Query,
        ctes: &Ctes,
    ) -> Result<Option<UnindexedWindowPlan>> {
        if !ctes.is_empty()
            || select.selection.is_some()
            || select.from.len() != 1
            || !select.from[0].joins.is_empty()
        {
            return Ok(None);
        }
        if !plain_table_factor(&select.from[0].relation) {
            return Ok(None);
        }
        let sqlparser::ast::TableFactor::Table { name, .. } = &select.from[0].relation else {
            return Ok(None);
        };
        if self.real_table_meta(name).is_none() {
            return Ok(None);
        }
        let table = obj_name(name);
        let Some(order_by) = &query.order_by else {
            return Ok(None);
        };
        let sqlparser::ast::OrderByKind::Expressions(exprs) = &order_by.kind else {
            return Ok(None);
        };
        if exprs.is_empty() {
            return Ok(None);
        }
        let Some((skip, take)) = constant_limit_window(query) else {
            return Ok(None);
        };
        if take == usize::MAX {
            return Ok(None); // unbounded: every row is materialized anyway
        }
        // take == 0 carries through: the executor returns an empty window.
        // Resolve every ORDER BY key to a source field, mirroring
        // `apply_order_limit`: an output-column name wins over the raw
        // expression, and a bare column ref (possibly qualified, with the
        // bare name as fallback) is the only extractable shape.
        let mut project: Vec<(String, &SqlExpr)> = Vec::new();
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(e) => project.push((expr_name(e), e)),
                SelectItem::ExprWithAlias { expr, alias, .. } => {
                    project.push((alias.value.clone(), expr))
                }
                _ => return Ok(None), // wildcard / unsupported item
            }
        }
        let mut keys: Vec<(Vec<String>, bool, Option<bool>)> = Vec::new();
        for o in exprs {
            let name = expr_name(&o.expr);
            let src = match project.iter().find(|(c, _)| *c == name) {
                Some((_, e)) => *e,
                None => {
                    if name.parse::<usize>().is_ok() {
                        return Ok(None); // ordinal position: generic path
                    }
                    &o.expr
                }
            };
            let fields = match src {
                SqlExpr::Identifier(i) => vec![i.value.clone()],
                SqlExpr::CompoundIdentifier(parts) => {
                    // Same resolution order as `eval_expr` for qualified
                    // names: the full dotted key first (schemaless docs can
                    // hold a literal "t.name" field), then the bare column.
                    let dotted = parts
                        .iter()
                        .map(|p| p.value.clone())
                        .collect::<Vec<_>>()
                        .join(".");
                    let mut fields = vec![dotted];
                    if let Some(last) = parts.last() {
                        fields.push(last.value.clone());
                    }
                    fields
                }
                _ => return Ok(None),
            };
            keys.push((fields, o.options.asc.unwrap_or(true), o.options.nulls_first));
        }
        Ok(Some(UnindexedWindowPlan {
            table,
            keys,
            skip,
            take,
        }))
    }
}

impl<'a> ReadCx<'a> {
    /// Unindexed `ORDER BY <fields> LIMIT/OFFSET`: extract only the sort
    /// fields from each document's encoded bytes (no full decode), keep the
    /// best `skip+take` rows in a bounded heap, then materialize just those
    /// documents. `None` (generic decode + sort) unless every ORDER BY key
    /// is a bare top-level field, there is no WHERE, and the window is
    /// bounded. Ties keep scan order, matching the stable generic sort.
    pub(super) fn unindexed_order_window(
        &self,
        select: &sqlparser::ast::Select,
        query: &Query,
        ctes: &Ctes,
    ) -> Result<Option<Vec<Object>>> {
        let Some(p) = self.unindexed_window_plan(select, query, ctes)? else {
            return Ok(None);
        };
        if p.take == 0 {
            return Ok(Some(Vec::new()));
        }
        let keys = p.keys;
        let skip = p.skip;
        let take = p.take;
        // Plan resolved the table through real_table_meta; re-fetch by name.
        let Some(meta) = self.tables.get(&p.table) else {
            return Ok(None);
        };
        let dirs: Vec<(bool, Option<bool>)> = keys.iter().map(|(_, asc, nf)| (*asc, *nf)).collect();
        let cap = skip.saturating_add(take);
        let heap = meta.heap_of();
        let reader = self.reader();
        let mut top: std::collections::BinaryHeap<OrderKeyed> =
            std::collections::BinaryHeap::with_capacity(cap.min(64));
        let mut seq = 0u64;
        heap.for_each_doc::<SqlError>(&reader, |loc, bytes| {
            // Extract the first key; when the window is full, reject before
            // decoding the remaining keys (the common case).
            let mut first = Value::Null;
            for field in &keys[0].0 {
                if let Some(v) = encode::extract_field_lookup(bytes, field)? {
                    first = v;
                    break;
                }
            }
            if top.len() >= cap {
                let worst = top.peek().expect("cap > 0");
                let ord = cmp_maybe_null(&first, &worst.keys[0], dirs[0].1);
                let worse = if dirs[0].0 {
                    ord == Ordering::Greater
                } else {
                    ord == Ordering::Less
                };
                if worse {
                    seq += 1;
                    return Ok(());
                }
            }
            let mut key_vals = Vec::with_capacity(keys.len());
            key_vals.push(first);
            for (fields, _, _) in keys.iter().skip(1) {
                let mut v = Value::Null;
                for field in fields {
                    if let Some(found) = encode::extract_field_lookup(bytes, field)? {
                        v = found;
                        break;
                    }
                }
                key_vals.push(v);
            }
            let entry = OrderKeyed {
                keys: key_vals,
                loc,
                seq,
                dirs: &dirs,
            };
            seq += 1;
            if top.len() < cap {
                top.push(entry);
            } else if top.peek().is_some_and(|worst| entry < *worst) {
                top.pop();
                top.push(entry);
            }
            Ok(())
        })?;
        // Only the window needs ordering: partition the skipped prefix off
        // in O(cap) and sort just the suffix (at most `take` rows — the
        // heap holds at most `skip+take`), so deep OFFSET no longer sorts
        // the whole candidate set.
        let mut entries = top.into_vec();
        if skip < entries.len() {
            entries.select_nth_unstable(skip);
            let mut window = entries.split_off(skip);
            window.sort();
            entries = window;
        } else {
            entries.clear();
        }
        let mut out = Vec::with_capacity(entries.len());
        for e in entries {
            if let Some(doc) = heap.doc_at(&reader, e.loc)? {
                out.push(doc);
            }
        }
        Ok(Some(out))
    }

    /// `ORDER BY <index key> LIMIT/OFFSET` window: walk one B+ tree in key
    /// order (forward for ASC, reverse for DESC) and load only the documents
    /// in the requested window — no full heap scan, no sort. `None` (generic
    /// path) unless the plan applies: solo real table, ORDER BY the tree's
    /// columns in order with a single direction, all key columns declared
    /// NOT NULL (trees omit NULL keys), a constant LIMIT/OFFSET, and a WHERE
    /// that either is absent or probes the same tree (residual conjuncts
    /// still filter while walking). An exact probe (one that subsumes the
    /// whole WHERE) lets the walk stop at the window's end and count OFFSET
    /// rows without reading them.
    pub(super) fn ordered_index_window(
        &self,
        select: &sqlparser::ast::Select,
        from: &[sqlparser::ast::TableWithJoins],
        selection: &Option<SqlExpr>,
        order_by: &Option<sqlparser::ast::OrderBy>,
        query: &Query,
        ctes: &Ctes,
    ) -> Result<Option<Vec<Object>>> {
        let Some(p) = self.ordered_window_plan(select, from, selection, order_by, query, ctes)?
        else {
            return Ok(None);
        };
        if p.take == 0 {
            return Ok(Some(Vec::new()));
        }
        let Some(meta) = self.tables.get(&p.table) else {
            return Ok(None);
        };
        let root = meta.index_roots[&p.root_key];
        let tree = BTree::open(root);
        let tx = self.pager.begin_tx();
        // An exact walk yields exactly the window rows in the ORDER BY
        // direction, so it can stop collecting keys at the window's end;
        // residual filters need the whole candidate range first.
        let cap = p.exact.then(|| p.skip.saturating_add(p.take));
        let pairs = match &p.probe {
            None if p.asc => tree
                .scan_limited(&self.reader(), &tx, cap)
                .map_err(|e| index_err(&p.root_key, e))?,
            None => tree
                .scan_limited_rev(&self.reader(), &tx, cap)
                .map_err(|e| index_err(&p.root_key, e))?,
            // promote_plan_bounds 已在 plan 阶段把边界提升过,这里幂等无操作。
            Some(plan) if p.asc => match self.probe_pairs(&p.root_key, &tree, &tx, plan, cap)? {
                Some(pairs) => pairs,
                None => {
                    drop(tx);
                    return Ok(None);
                }
            },
            Some(plan) => match self.probe_pairs_rev(&p.root_key, &tree, &tx, plan, cap)? {
                Some(pairs) => pairs,
                None => {
                    drop(tx);
                    return Ok(None);
                }
            },
        };
        drop(tx);
        let heap = meta.heap_of();
        let reader = self.reader();
        // Both walkers already yield pairs in the ORDER BY direction.
        let locs: Vec<u64> = pairs.iter().map(|(_, loc)| *loc).collect();
        let mut out = Vec::new();
        let mut skipped = 0usize;
        for loc in locs {
            // Exact walks return rows straight from the window: OFFSET rows
            // need only be counted, never decoded.
            if p.exact && skipped < p.skip {
                skipped += 1;
                continue;
            }
            let Some(doc) = heap.doc_at(&reader, loc)? else {
                continue;
            };
            if !p.exact {
                // Residual-filter scope (inexact probes re-check WHERE per
                // row): built once, lazily, on the first residual row.
                let outer_from = {
                    let mut set = OuterFrom::new();
                    from_list_names(from, &mut set);
                    set
                };
                if !self.matches_in(selection, &doc, &outer_from)? {
                    continue;
                }
            }
            if skipped < p.skip {
                skipped += 1;
                continue;
            }
            out.push(doc);
            if out.len() >= p.take {
                break;
            }
        }
        Ok(Some(out))
    }

    /// Planning prefix of [`Self::ordered_index_window`], shared with
    /// EXPLAIN: every gate from the fast path's entry through the
    /// band-promoted probe bounds — no row loading (the band sampling
    /// reads two leaf pages, mirroring the executor's fallback decision).
    /// `None` = this query does not take the ordered window.
    pub(super) fn ordered_window_plan(
        &self,
        select: &sqlparser::ast::Select,
        from: &[sqlparser::ast::TableWithJoins],
        selection: &Option<SqlExpr>,
        order_by: &Option<sqlparser::ast::OrderBy>,
        query: &Query,
        ctes: &Ctes,
    ) -> Result<Option<OrderedWindowPlan>> {
        if !ctes.is_empty() || from.len() != 1 {
            return Ok(None);
        }
        let base = &from[0];
        if !base.joins.is_empty() {
            return Ok(None);
        }
        if !plain_table_factor(&base.relation) {
            return Ok(None);
        }
        let sqlparser::ast::TableFactor::Table { name, alias, .. } = &base.relation else {
            return Ok(None);
        };
        let tname = obj_name(name);
        if is_compat_view(&tname) {
            return Ok(None);
        }
        let Some(meta) = self.tables.get(&tname) else {
            return Ok(None);
        };
        let Some(order_by) = order_by else {
            return Ok(None);
        };
        let sqlparser::ast::OrderByKind::Expressions(exprs) = &order_by.kind else {
            return Ok(None);
        };
        if exprs.is_empty() {
            return Ok(None);
        }
        // `SELECT *`(含 `*, expr`)的输出列集是「全部过滤行」的字段并集
        // (通用路径在 LIMIT 之前推导,空集时退化为声明列);窗口快路径
        // 只装载 skip+take 行,并集会丢掉只存在于窗口之外行的字段——列
        // 形状随 ORDER BY 列有无索引而漂移。通配符一律回退通用路径(与
        // unindexed top-K 窗口同一门禁)。
        if select.projection.iter().any(|i| {
            matches!(
                i,
                sqlparser::ast::SelectItem::Wildcard(_)
                    | sqlparser::ast::SelectItem::QualifiedWildcard(_, _)
            )
        }) {
            return Ok(None);
        }
        // 输出列名优先(与 apply_order_limit 同一解析序):ORDER BY 键名命中
        // 投影别名时,通用路径按该输出列的值排序;索引窗口按同名表列走
        // 树序 —— 只有输出列表达式恰是同一裸列引用时两者才一致,否则
        // 必须回退通用路径,别让别名遮蔽带索引列静默给出错误行序。
        // 通配符项展开为全部表列的恒等映射(position() 首个命中即表列),
        // 无需逐项判定。
        let mut effective: Vec<&SqlExpr> = Vec::with_capacity(exprs.len());
        if !select.projection.iter().all(|i| {
            matches!(
                i,
                sqlparser::ast::SelectItem::UnnamedExpr(_)
                    | sqlparser::ast::SelectItem::ExprWithAlias { .. }
            )
        }) {
            for o in exprs {
                effective.push(&o.expr);
            }
        } else {
            let mut project: Vec<(String, &SqlExpr)> = Vec::new();
            for item in &select.projection {
                match item {
                    sqlparser::ast::SelectItem::UnnamedExpr(e) => project.push((expr_name(e), e)),
                    sqlparser::ast::SelectItem::ExprWithAlias { expr, alias, .. } => {
                        project.push((alias.value.clone(), expr))
                    }
                    _ => unreachable!("gated above"),
                }
            }
            for o in exprs {
                let name = expr_name(&o.expr);
                effective.push(match project.iter().find(|(c, _)| *c == name) {
                    Some((_, e)) => *e,
                    None => {
                        // 序号键(`ORDER BY 2`)按输出位置排序:树序无法
                        // 保证一致,回退。
                        if name.parse::<usize>().is_ok() {
                            return Ok(None);
                        }
                        &o.expr
                    }
                });
            }
        }
        let akey = alias.as_ref().map(|a| a.name.value.clone());
        let Some((root_key, asc)) =
            order_walk_index(meta, &tname, akey.as_deref(), selection, exprs, &effective)
        else {
            return Ok(None);
        };
        let Some((skip, take)) = constant_limit_window(query) else {
            return Ok(None);
        };
        if take == 0 {
            return Ok(Some(OrderedWindowPlan {
                table: tname,
                root_key,
                asc,
                skip,
                take: 0,
                exact: true,
                probe: None,
            }));
        }
        let root = meta.index_roots[&root_key];
        let tree = BTree::open(root);
        let tx = self.pager.begin_tx();
        let (plan, exact) = match selection {
            None => (None, true),
            Some(cond) => match probe_plan(cond, meta, &tname, akey.as_deref()) {
                Some((plan_root, plan, exact)) if plan_root == root_key => {
                    // 混合带不可安全探测:整体回退通用路径;边界被实际
                    // 提升时计划降级为非 exact(残余过滤兜住多拉的候选)。
                    match self.promote_plan_bounds(&tree, &tx, &plan)? {
                        None => {
                            drop(tx);
                            return Ok(None);
                        }
                        Some((p, promoted)) => (Some(p), exact && !promoted),
                    }
                }
                _ => {
                    drop(tx);
                    return Ok(None);
                }
            },
        };
        drop(tx);
        Ok(Some(OrderedWindowPlan {
            table: tname,
            root_key,
            asc,
            skip,
            take,
            exact,
            probe: plan,
        }))
    }

    pub(super) fn table_pairs(&self, table: &str) -> Result<Vec<(u64, Object)>> {
        let Some(meta) = self.tables.get(table) else {
            return err(format!("table {table} does not exist"));
        };
        let heap = meta.heap_of();
        let mut out = Vec::new();
        let rtx = self.pager.begin_tx();
        for &pid in &heap.pages {
            // The heap walk is the first cost a WHERE-less SELECT pays over a
            // big table — keep the statement timeout armed here too.
            self.deadline.check()?;
            out.extend(heap.page_docs(&self.reader(), &rtx, pid)?);
        }
        // The read-only tx has no staged pages: dropping it is the cleanup.
        Ok(out)
    }
}

// ---- EXPLAIN SELECT -------------------------------------------------
//
// The plan ladder without execution: the rows describe what `exec_select`
// WOULD do for this shape — same gates, same planning helpers — so an
// operator can see index use / window eligibility / join strategy without
// running an O(table) query. The mirrors are load-bearing: a gate added
// to `exec_select` or the window planners must be mirrored here, and the
// differential window tests plus the EXPLAIN suite pin both sides.

/// Render a probe plan as an EXPLAIN `PROBE` row's detail text.
pub(super) fn probe_plan_text(root: &str, plan: &ProbePlan) -> String {
    let lit = |v: &Value| value_literal(v).unwrap_or_else(|_| "?".to_string());
    match plan {
        ProbePlan::Eq(v) => format!("{root} = {}", lit(v)),
        ProbePlan::Prefix(v) => format!("{root} key prefix {}", lit(v)),
        ProbePlan::In(vals) => {
            // A rewritten IN (SELECT …) can carry thousands of literals —
            // the EXPLAIN row summarizes instead of echoing them all.
            const SHOW: usize = 16;
            let items = vals.iter().map(&lit).collect::<Vec<_>>();
            if items.len() <= SHOW {
                format!("{root} IN ({})", items.join(", "))
            } else {
                format!(
                    "{root} IN ({}, … +{} more)",
                    items[..SHOW].join(", "),
                    items.len() - SHOW
                )
            }
        }
        ProbePlan::Range { lo, hi } => {
            let mut parts: Vec<String> = Vec::new();
            match lo {
                Some((v, true)) => parts.push(format!(">= {}", lit(v))),
                Some((v, false)) => parts.push(format!("> {}", lit(v))),
                None => {}
            }
            match hi {
                Some((v, true)) => parts.push(format!("<= {}", lit(v))),
                Some((v, false)) => parts.push(format!("< {}", lit(v))),
                None => {}
            }
            if parts.is_empty() {
                format!("{root} (full range)")
            } else {
                format!("{root} {}", parts.join(" AND "))
            }
        }
    }
}

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
