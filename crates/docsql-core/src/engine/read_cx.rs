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

impl<'a> ReadCx<'a> {
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
