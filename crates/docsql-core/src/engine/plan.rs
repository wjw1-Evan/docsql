use super::*;

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
    pub(super) table: String,
    /// Per ORDER BY key: candidate field names (first hit wins), ASC?,
    /// NULLS FIRST override.
    pub(super) keys: Vec<(Vec<String>, bool, Option<bool>)>,
    pub(super) skip: usize,
    pub(super) take: usize,
}

/// Plan of the ordered index window ([`ReadCx::ordered_index_window`]):
/// the planning prefix (through the band-promoted probe bounds) shared
/// with EXPLAIN — no rows are touched.
pub(super) struct OrderedWindowPlan {
    pub(super) table: String,
    pub(super) root_key: String,
    pub(super) asc: bool,
    pub(super) skip: usize,
    pub(super) take: usize,
    /// The probe subsumes the whole WHERE: OFFSET rows are only counted,
    /// never decoded.
    pub(super) exact: bool,
    /// Band-promoted probe bounds; None = whole-tree walk.
    pub(super) probe: Option<ProbePlan>,
}

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
}
