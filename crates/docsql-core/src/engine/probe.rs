use super::*;

pub(super) fn index_err(col: &str, e: BTreeError) -> SqlError {
    match e {
        BTreeError::Duplicate => SqlError::Message(format!("UNIQUE constraint failed: {col}")),
        other => SqlError::Message(format!("index error on {col}: {other}")),
    }
}

/// How an index probe should fetch entries.
#[derive(Debug, Clone)]
pub(super) enum ProbePlan {
    Eq(Value),
    /// Composite-index prefix probe: `lo` is an `Array` prefix of the full
    /// key; every returned key must start with it (element-wise), which the
    /// probe retains after the bounded scan.
    Prefix(Value),
    /// (lower bound, inclusive?), (upper bound, inclusive?) — either optional.
    Range {
        lo: Option<(Value, bool)>,
        hi: Option<(Value, bool)>,
    },
    /// Equality set (`col IN (…)` / rewritten `IN (SELECT …)`): one bounded
    /// Eq walk per element, merged back into key order. Distinct elements
    /// probe disjoint key runs (a row holds one column value), so the merge
    /// only has to dedupe repeated list entries.
    In(Vec<Value>),
}

/// Smallest string sorting after every string starting with `prefix`: the
/// last character incremented (`abc` → `abd`). A trailing char::MAX has no
/// successor — incrementing an earlier character instead (`ab\u{10FFFF}` →
/// `ac`) would cut the prefix off, since the prefix itself sorts above the
/// carried bound; the caller then walks open-ended and lets the residual
/// filter drop the tail.
pub(super) fn str_prefix_successor(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    let last = chars.pop()?;
    let next = char::from_u32(last as u32 + 1)?;
    chars.push(next);
    Some(chars.into_iter().collect())
}

/// Flatten the top-level AND chain of a WHERE expression. Parenthesized
/// conjuncts unwrap transparently (`(a = 1) AND b = 'x'`): a Nested node
/// carries no semantics of its own, and skipping it used to hide every
/// conjunct from the probe/window planners.
pub(super) fn flatten_and<'a>(e: &'a SqlExpr, out: &mut Vec<&'a SqlExpr>) {
    if let SqlExpr::BinaryOp {
        left,
        op: BinaryOperator::And,
        right,
    } = e
    {
        flatten_and(left, out);
        flatten_and(right, out);
    } else if let SqlExpr::Nested(inner) = e {
        flatten_and(inner, out);
    } else {
        out.push(e);
    }
}

/// Spelling of a probe conjunct's column reference. A quoted identifier
/// WITH a dot is a literal field name ("v.x" as ONE key): eval resolves it
/// by that exact key, so a qualifier-stripped probe would read a different
/// column than the residual/row evaluation — the conjunct must stay opaque
/// (no probe) instead.
pub(super) fn probe_col_name(e: &SqlExpr) -> Option<String> {
    match e {
        SqlExpr::Identifier(i) if i.value.contains('.') => None,
        SqlExpr::Identifier(i) => Some(i.value.clone()),
        SqlExpr::CompoundIdentifier(_) => Some(expr_name(e)),
        _ => None,
    }
}

/// Resolve a column reference (bare, table- or alias-qualified) to its bare
/// column name when it names a column of this table. `dotted` is the
/// table's literal-dotted-field marker: while set, a qualifier-stripped
/// reference may collide with a literal "a.b" key (eval tries that key
/// first), so stripping is refused.
pub(super) fn unqualified_col(
    ident: &str,
    table: &str,
    alias: Option<&str>,
    dotted: bool,
) -> Option<String> {
    if let Some(c) = ident.strip_prefix(&format!("{table}.")) {
        if dotted {
            return None;
        }
        return Some(c.to_string());
    }
    if let Some(a) = alias {
        if let Some(c) = ident.strip_prefix(&format!("{a}.")) {
            if dotted {
                return None;
            }
            return Some(c.to_string());
        }
    }
    if !ident.contains('.') {
        return Some(ident.to_string());
    }
    None
}

/// Match ORDER BY to one of the table's B+ trees: every key resolves to the
/// tree's columns in order, all one direction, default NULL placement
/// (NULLS FIRST/LAST is a different order — the sort path handles it). The
/// trees omit NULL keys (`index_key_of`), so every key column must be
/// declared NOT NULL or rows would silently go missing from the walk.
///
/// `effective` carries each ORDER BY key's sort SOURCE — the output-column
/// expression when the key name hits a projection alias (the resolution
/// `apply_order_limit` uses), the raw key otherwise — parallel to `exprs`.
pub(super) fn order_walk_index(
    meta: &TableMeta,
    table: &str,
    alias: Option<&str>,
    selection: &Option<SqlExpr>,
    exprs: &[sqlparser::ast::OrderByExpr],
    effective: &[&SqlExpr],
) -> Option<(String, bool)> {
    let mut cols = Vec::with_capacity(exprs.len());
    let mut asc = true;
    for (i, o) in exprs.iter().enumerate() {
        if o.options.nulls_first.is_some() {
            return None;
        }
        let col = unqualified_col(
            &probe_col_name(effective[i])?,
            table,
            alias,
            meta.dotted_keys,
        )?;
        let this_asc = o.options.asc.unwrap_or(true);
        if i == 0 {
            asc = this_asc;
        } else if asc != this_asc {
            return None;
        }
        cols.push(col);
    }
    if cols.iter().any(|c| !meta.not_null.contains(c)) {
        return None;
    }
    meta.index_roots.keys().find_map(|root| {
        // Path indexes (design 005) hold extracted values, not column
        // values — an ORDER BY over the column name must not walk them.
        let is_path = meta
            .index_defs
            .iter()
            .any(|d| d.name == *root && d.path.is_some());
        // A PARTIAL index walks only its qualifying rows: without a WHERE
        // implying the predicate the window would silently drop the rest.
        let partial_blocked = meta
            .index_defs
            .iter()
            .find(|d| d.name == *root)
            .and_then(|d| d.pred.as_deref())
            .is_some_and(|p| {
                selection
                    .as_ref()
                    .is_none_or(|c| !query_implies_pred(c, p, table, alias))
            });
        (!is_path && !partial_blocked && meta.index_columns_of(root) == cols)
            .then(|| (root.clone(), asc))
    })
}

/// Resolve a column reference (bare or table/alias-qualified) to an indexed
/// column of this table, if it has one.
/// Resolve a column reference to an index root usable for a scalar probe.
/// Only roots that are single-column trees for exactly that column qualify:
/// a composite tree is keyed by the index *name* (or arbitrary root_key), so
/// accepting `index_roots.contains_key(ident)` blindly made a column that
/// shares a composite index's name probe an Array-keyed tree and silently
/// return nothing.
/// CREATE-time validation of a partial-index predicate: row-local only —
/// bare identifiers that name table columns, literals, operators, scalar
/// functions, IS NULL / IN (constants) / BETWEEN / LIKE. Subqueries,
/// aggregates, window functions and qualified names are refused (a probe
/// gate evaluates the predicate per row with the same row-local evaluator,
/// so anything richer could diverge between gate and maintenance). The
/// caller checks identifier existence against the table's columns.
pub(super) fn validate_partial_pred(e: &SqlExpr) -> Result<SqlExpr> {
    let bad = |what: &str| -> Result<SqlExpr> {
        err(format!(
            "partial-index predicates support row-local expressions only ({what} is not allowed)"
        ))
    };
    match e {
        SqlExpr::Subquery(_) | SqlExpr::Exists { .. } | SqlExpr::InSubquery { .. } => {
            bad("a subquery")
        }
        SqlExpr::CompoundIdentifier(_) => bad("qualified column names (use bare column names)"),
        SqlExpr::Function(f) => {
            if is_agg_fn(f) {
                return bad("an aggregate");
            }
            if f.over.is_some() {
                return bad("a window function");
            }
            walk_partial_pred_args(f)?;
            Ok(e.clone())
        }
        SqlExpr::Nested(inner) => validate_partial_pred(inner),
        SqlExpr::UnaryOp { expr, .. } => validate_partial_pred(expr),
        SqlExpr::BinaryOp { left, right, .. } => {
            validate_partial_pred(left)?;
            validate_partial_pred(right)?;
            Ok(e.clone())
        }
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            validate_partial_pred(expr)?;
            validate_partial_pred(low)?;
            validate_partial_pred(high)?;
            Ok(e.clone())
        }
        SqlExpr::InList { expr, list, .. } => {
            validate_partial_pred(expr)?;
            for item in list {
                validate_partial_pred(item)?;
            }
            Ok(e.clone())
        }
        SqlExpr::IsDistinctFrom(l, r) | SqlExpr::IsNotDistinctFrom(l, r) => {
            validate_partial_pred(l)?;
            validate_partial_pred(r)?;
            Ok(e.clone())
        }
        SqlExpr::IsNull(x) | SqlExpr::IsNotNull(x) => validate_partial_pred(x),
        SqlExpr::Cast { expr, .. } => validate_partial_pred(expr),
        SqlExpr::Like { expr, pattern, .. } | SqlExpr::ILike { expr, pattern, .. } => {
            validate_partial_pred(expr)?;
            validate_partial_pred(pattern)
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(o) = operand {
                validate_partial_pred(o)?;
            }
            for w in conditions {
                validate_partial_pred(&w.condition)?;
                validate_partial_pred(&w.result)?;
            }
            if let Some(r) = else_result {
                validate_partial_pred(r)?;
            }
            Ok(e.clone())
        }
        _ => Ok(e.clone()),
    }
}

pub(super) fn walk_partial_pred_args(f: &sqlparser::ast::Function) -> Result<()> {
    if let sqlparser::ast::FunctionArguments::List(list) = &f.args {
        for a in &list.args {
            if let sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(
                inner,
            )) = a
            {
                validate_partial_pred(inner)?;
            }
        }
    }
    Ok(())
}

/// Collect the bare column names a predicate references.
pub(super) fn pred_columns(e: &SqlExpr, out: &mut std::collections::BTreeSet<String>) {
    match e {
        SqlExpr::Identifier(i) => {
            out.insert(i.value.clone());
        }
        SqlExpr::Nested(inner) => pred_columns(inner, out),
        SqlExpr::UnaryOp { expr, .. } => pred_columns(expr, out),
        SqlExpr::BinaryOp { left, right, .. } => {
            pred_columns(left, out);
            pred_columns(right, out);
        }
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            pred_columns(expr, out);
            pred_columns(low, out);
            pred_columns(high, out);
        }
        SqlExpr::InList { expr, list, .. } => {
            pred_columns(expr, out);
            for item in list {
                pred_columns(item, out);
            }
        }
        SqlExpr::IsDistinctFrom(l, r) | SqlExpr::IsNotDistinctFrom(l, r) => {
            pred_columns(l, out);
            pred_columns(r, out);
        }
        SqlExpr::IsNull(x) | SqlExpr::IsNotNull(x) => pred_columns(x, out),
        SqlExpr::Cast { expr, .. } => pred_columns(expr, out),
        SqlExpr::Like { expr, pattern, .. } | SqlExpr::ILike { expr, pattern, .. } => {
            pred_columns(expr, out);
            pred_columns(pattern, out);
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(o) = operand {
                pred_columns(o, out);
            }
            for w in conditions {
                pred_columns(&w.condition, out);
                pred_columns(&w.result, out);
            }
            if let Some(r) = else_result {
                pred_columns(r, out);
            }
        }
        SqlExpr::Function(f) => {
            if let sqlparser::ast::FunctionArguments::List(list) = &f.args {
                for a in &list.args {
                    if let sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(inner),
                    ) = a
                    {
                        pred_columns(inner, out);
                    }
                }
            }
        }
        _ => {}
    }
}

/// One query conjunct's equality binding: (bare column, values) — `col =
/// const` yields one value, `col IN (consts)` the whole list. NULL never
/// binds (it binds to nothing).
pub(super) fn pred_eq_binding(q: &SqlExpr) -> Option<(String, Vec<Value>)> {
    match q {
        SqlExpr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            let (id_side, lit_side) = match (left.as_ref(), right.as_ref()) {
                (SqlExpr::Identifier(i), other) => (i, other),
                (other, SqlExpr::Identifier(i)) => (i, other),
                _ => return None,
            };
            let v = eval_const(lit_side).ok()?;
            if matches!(v, Value::Null) {
                return None;
            }
            Some((id_side.value.clone(), vec![v]))
        }
        SqlExpr::InList {
            expr,
            list,
            negated: false,
            ..
        } => {
            let SqlExpr::Identifier(i) = expr.as_ref() else {
                return None;
            };
            let vals: Option<Vec<Value>> = list.iter().map(|el| eval_const(el).ok()).collect();
            let vals = vals?;
            if vals.is_empty() || vals.iter().any(|v| matches!(v, Value::Null)) {
                return None;
            }
            Some((i.value.clone(), vals))
        }
        _ => None,
    }
}

/// Substitute every bare `col` identifier in `e` with the literal `v`
/// (recursive over the row-local shapes [`validate_partial_pred`] accepts).
/// Returns false when the shape escapes substitution.
pub(super) fn pred_subst_ident(e: &mut SqlExpr, col: &str, v: &Value) -> bool {
    let mut replaced = false;
    match e {
        SqlExpr::Identifier(i) if i.value == col => {
            if let Ok(lit) = value_to_literal(v.clone()) {
                *e = lit;
                replaced = true;
            }
        }
        SqlExpr::Nested(inner) => replaced = pred_subst_ident(inner, col, v),
        SqlExpr::UnaryOp { expr, .. } => replaced = pred_subst_ident(expr, col, v),
        SqlExpr::BinaryOp { left, right, .. } => {
            let a = pred_subst_ident(left, col, v);
            let b = pred_subst_ident(right, col, v);
            replaced = a || b;
        }
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            let a = pred_subst_ident(expr, col, v);
            let b = pred_subst_ident(low, col, v);
            let c = pred_subst_ident(high, col, v);
            replaced = a || b || c;
        }
        SqlExpr::InList { expr, list, .. } => {
            replaced = pred_subst_ident(expr, col, v);
            for item in list {
                replaced |= pred_subst_ident(item, col, v);
            }
        }
        SqlExpr::IsDistinctFrom(l, r) | SqlExpr::IsNotDistinctFrom(l, r) => {
            let a = pred_subst_ident(l, col, v);
            let b = pred_subst_ident(r, col, v);
            replaced = a || b;
        }
        SqlExpr::IsNull(x) | SqlExpr::IsNotNull(x) => replaced = pred_subst_ident(x, col, v),
        SqlExpr::Cast { expr, .. } => replaced = pred_subst_ident(expr, col, v),
        SqlExpr::Like { expr, pattern, .. } | SqlExpr::ILike { expr, pattern, .. } => {
            let a = pred_subst_ident(expr, col, v);
            let b = pred_subst_ident(pattern, col, v);
            replaced = a || b;
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(o) = operand {
                replaced |= pred_subst_ident(o, col, v);
            }
            for w in conditions {
                replaced |= pred_subst_ident(&mut w.condition, col, v);
                replaced |= pred_subst_ident(&mut w.result, col, v);
            }
            if let Some(r) = else_result {
                replaced |= pred_subst_ident(r, col, v);
            }
        }
        SqlExpr::Function(f) => {
            if let sqlparser::ast::FunctionArguments::List(list) = &mut f.args {
                for a in &mut list.args {
                    if let sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(inner),
                    ) = a
                    {
                        replaced |= pred_subst_ident(inner, col, v);
                    }
                }
            }
        }
        _ => {}
    }
    replaced
}

/// True when the query's WHERE clause implies the partial-index predicate —
/// the gate that makes a probe over the partial tree a SUPERSET of the
/// answer set (every row the WHERE keeps satisfies the predicate, so it is
/// in the tree). Conservative conjunction proof: both sides decompose into
/// AND-conjuncts; every predicate conjunct must be implied by some query
/// conjunct via (a) normalized syntactic identity, or (b) equality
/// substitution — `col = const` / `col IN (consts)` implies any
/// single-column conjunct that evaluates TRUE under every bound value.
/// Anything unprovable leaves the index invisible to the query (full scan /
/// other indexes) — never wrong rows.
pub(super) fn query_implies_pred(
    cond: &SqlExpr,
    pred_text: &str,
    table: &str,
    alias: Option<&str>,
) -> bool {
    let _ = (table, alias);
    let Ok(pred) = parse_expr_text(pred_text) else {
        return false;
    };
    let mut pred_terms = Vec::new();
    flatten_and(&pred, &mut pred_terms);
    let mut q_terms = Vec::new();
    flatten_and(cond, &mut q_terms);
    'every_term: for t in &pred_terms {
        let mut t_cols = std::collections::BTreeSet::new();
        pred_columns(t, &mut t_cols);
        let t_key = format!("{t}");
        for q in &q_terms {
            if format!("{q}") == t_key {
                continue 'every_term;
            }
            if let Some((col, vals)) = pred_eq_binding(q) {
                let implied = t_cols.len() == 1
                    && t_cols.contains(&col)
                    && vals.iter().all(|v| {
                        let mut sub: SqlExpr = (*t).clone();
                        pred_subst_ident(&mut sub, &col, v)
                            && matches!(eval_expr(&sub, &Object::new()), Ok(Value::Bool(true)))
                    });
                if implied {
                    continue 'every_term;
                }
            }
        }
        return false;
    }
    true
}

pub(super) fn resolve_indexed_col(
    ident: &str,
    table: &str,
    alias: Option<&str>,
    meta: &TableMeta,
) -> Option<String> {
    let scalar_root = |col: &str| -> Option<String> {
        // The root must exist and name exactly this one column. Testing
        // `index_columns_of` alone is not enough: for an unknown key it
        // synthesizes `[key]`, which would accept any column name.
        if !meta.index_roots.contains_key(col) {
            return None;
        }
        // A JSON path index keyed by the column's NAME (root_key == index
        // name, design 005) holds extracted values, not column values —
        // it must never serve a scalar column probe. Same for a PARTIAL
        // index under that name (its tree is a gated subset).
        if meta
            .index_defs
            .iter()
            .any(|d| d.name == col && (d.path.is_some() || d.pred.is_some()))
        {
            return None;
        }
        let cols = meta.index_columns_of(col);
        (cols.len() == 1 && cols[0] == col).then(|| col.to_string())
    };
    // While the table carries literal dotted field names, eval may answer a
    // qualified reference through the exact "a.b" key — the stripped tree
    // window is not a superset, so refuse to strip.
    if !meta.dotted_keys {
        if let Some(col) = ident.strip_prefix(&format!("{table}.")) {
            if let Some(c) = scalar_root(col) {
                return Some(c);
            }
        }
        if let Some(a) = alias {
            if let Some(col) = ident.strip_prefix(&format!("{a}.")) {
                if let Some(c) = scalar_root(col) {
                    return Some(c);
                }
            }
        }
    }
    if !ident.contains('.') {
        if let Some(c) = scalar_root(ident) {
            return Some(c);
        }
    }
    None
}

/// Choose an index plan from a WHERE expression: an equality conjunct on an
/// indexed column wins; otherwise range bounds on one indexed column. The
/// bool is true when the plan subsumes the whole condition — every conjunct
/// is implied by the probe, so callers may trust the entry count and stop a
/// window walk at the requested end (no residual re-filter).
/// Collapse the collected range bounds (op, value) into ProbePlan's
/// (lo, hi) pair: the TIGHTEST bound wins on each side — repeated
/// inequalities narrow, mirroring the scalar branch's invariant
/// (`a > 10 AND a > 18` must probe from 18). The probe re-filters
/// row-exactly anyway when the plan is inexact, and an exact plan cannot
/// carry contradicting bounds because eq_conflict/path conflict detection
/// refuses those shapes.
pub(super) fn path_bound(bounds: &[(BinaryOperator, Value)], lower: bool) -> Option<(Value, bool)> {
    let mut best: Option<(Value, bool)> = None;
    for (op, v) in bounds {
        let inclusive = matches!(op, BinaryOperator::GtEq | BinaryOperator::LtEq);
        let is_lower = matches!(op, BinaryOperator::Gt | BinaryOperator::GtEq);
        if is_lower != lower {
            continue;
        }
        best = Some(match best {
            None => (v.clone(), inclusive),
            Some((bv, bi)) => {
                let ord = Value::cmp_values(&bv, v);
                // Keep the bound that narrows the window: the larger lower
                // bound / the smaller upper bound.
                if (lower && ord == std::cmp::Ordering::Less)
                    || (!lower && ord == std::cmp::Ordering::Greater)
                {
                    (v.clone(), inclusive)
                } else {
                    (bv, bi)
                }
            }
        });
    }
    best
}

/// A range-probe target: the tree root key plus the probed column (a FULL
/// single-column index roots at the column itself; a gated PARTIAL index
/// roots at its index name).
pub(super) type ProbeTarget = (String, String, Vec<(BinaryOperator, Value)>);

pub(super) fn probe_plan(
    cond: &SqlExpr,
    meta: &TableMeta,
    table: &str,
    alias: Option<&str>,
) -> Option<(String, ProbePlan, bool)> {
    let mut conjuncts = Vec::new();
    flatten_and(cond, &mut conjuncts);
    let empty = Object::new();
    // A conjunct the plan cannot consume: its filter still has to run after
    // the probe, so the plan is inexact.
    let mut opaque = false;
    // All equality conjuncts: composite prefixes need every leading column's
    // value, not just the first one seen.
    let mut eq_map: std::collections::BTreeMap<String, Value> = std::collections::BTreeMap::new();
    // `a = 1 AND a = 2`: a plan built from one of the values would return
    // rows the other conjunct excludes, and `covered` sees a single map key
    // so the plan would be declared exact. Refuse any index plan instead.
    let mut eq_conflict = false;
    // (tree root key, column name, …): a FULL single-column index roots at
    // the column itself; a gated PARTIAL index roots at its index name.
    let mut eq: Option<(String, String, Value)> = None;
    let mut range: Option<ProbeTarget> = None;
    // JSON path conjuncts (design 005): JSON_EXTRACT(col,'$.p') op const.
    // One (col, path) target per plan; a second distinct target or a mix
    // with named-column conjuncts stays usable but inexact.
    let mut path_target: Option<(String, String)> = None;
    let mut path_eq: Option<Value> = None;
    let mut path_range: Option<Vec<(BinaryOperator, Value)>> = None;
    // Scalar equality set: `col IN (…)` on a single-column tree. One target
    // per plan; a second IN list rides the residual filter (inexact).
    let mut in_target: Option<(String, String, Vec<Value>)> = None;
    // Column → tree-root resolution for probe candidates: the column's own
    // full single-column tree, or — when the query WHERE implies the
    // predicate — a single-column PARTIAL index (its tree keys by the index
    // name). Not implied → the partial index is invisible (superset safety).
    let resolve_probe = |name: &str| -> Option<(String, String)> {
        if let Some(c) = resolve_indexed_col(name, table, alias, meta) {
            return Some((c.clone(), c));
        }
        let bare = unqualified_col(name, table, alias, meta.dotted_keys)?;
        for d in &meta.index_defs {
            if d.pred.is_none() || d.columns.len() != 1 || d.columns[0] != *bare {
                continue;
            }
            if !meta.index_roots.contains_key(&d.name) {
                continue;
            }
            if query_implies_pred(cond, d.pred.as_deref().expect("pred set"), table, alias) {
                return Some((d.name.clone(), bare.to_string()));
            }
        }
        None
    };
    for c in conjuncts {
        // col BETWEEN low AND high — a closed range on one indexed column,
        // exactly the two inclusive bounds the Range plan already walks.
        // NOT BETWEEN and NULL bounds stay opaque (NULL never matches, the
        // residual filter decides).
        if let SqlExpr::Between {
            expr,
            negated,
            low,
            high,
        } = c
        {
            if *negated {
                opaque = true;
                continue;
            }
            let name = match probe_col_name(expr.as_ref()) {
                Some(n) => n,
                None => {
                    opaque = true;
                    continue;
                }
            };
            let Some((root, col)) = resolve_probe(&name) else {
                opaque = true;
                continue;
            };
            let (Ok(lo), Ok(hi)) = (eval_const(low), eval_const(high)) else {
                opaque = true;
                continue;
            };
            if matches!(lo, Value::Null) || matches!(hi, Value::Null) {
                opaque = true;
                continue;
            }
            match &mut range {
                Some((_, c2, bounds)) if *c2 == col => {
                    bounds.push((BinaryOperator::GtEq, lo));
                    bounds.push((BinaryOperator::LtEq, hi));
                }
                None => {
                    range = Some((
                        root,
                        col,
                        vec![(BinaryOperator::GtEq, lo), (BinaryOperator::LtEq, hi)],
                    ));
                }
                _ => opaque = true,
            }
            continue;
        }
        // col IN (list of constants): the multi-probe plan. NULL elements
        // are dropped from the probe keys (the tree holds no NULL keys);
        // on a nullable column that costs exactness — NULL rows match the
        // predicate via NULL = NULL — so the conjunct rides the residual
        // filter instead. Non-constant elements forfeit the probe.
        if let SqlExpr::InList {
            expr,
            list,
            negated,
        } = c
        {
            if *negated {
                opaque = true;
                continue;
            }
            let name = match probe_col_name(expr.as_ref()) {
                Some(n) => n,
                None => {
                    opaque = true;
                    continue;
                }
            };
            let Some((root, col)) = resolve_probe(&name) else {
                opaque = true;
                continue;
            };
            let mut saw_null_elem = false;
            let mut vals = Vec::with_capacity(list.len());
            for el in list {
                match eval_const(el) {
                    Ok(Value::Null) => saw_null_elem = true,
                    Ok(v) => vals.push(v),
                    Err(_) => {
                        vals.clear();
                        break;
                    }
                }
            }
            if vals.is_empty() {
                opaque = true;
                continue;
            }
            // Under the engine's total order a NULL element makes
            // `v IN (…, NULL)` TRUE for v-NULL rows as well — but the tree
            // holds no NULL keys, so probe candidates can never include
            // them. A nullable column must ride the residual filter (a NOT
            // NULL column has no NULL rows to lose; non-constant elements
            // forfeit the probe).
            if saw_null_elem && !meta.not_null.contains(&col) {
                opaque = true;
                continue;
            }
            match &mut in_target {
                None => in_target = Some((root, col, vals)),
                Some(_) => opaque = true, // second IN list: rides the residual filter
            }
            continue;
        }
        // col LIKE 'literal-prefix%' — a Str band [prefix, successor) on a
        // single-column tree. `_`, `[...]` classes, interior `%` and ILIKE
        // stay opaque; a prefix ending at char::MAX has no finite successor
        // and probes open-ended (inexact — the residual filter drops the
        // tail beyond the prefix run).
        if let SqlExpr::Like {
            expr,
            pattern,
            escape_char,
            negated,
            ..
        } = c
        {
            if *negated {
                opaque = true;
                continue;
            }
            let name = match probe_col_name(expr.as_ref()) {
                Some(n) => n,
                None => {
                    opaque = true;
                    continue;
                }
            };
            let Some((root, col)) = resolve_probe(&name) else {
                opaque = true;
                continue;
            };
            let esc = escape_char.as_ref().and_then(|v| match &v.value {
                sqlparser::ast::Value::SingleQuotedString(s) => s.chars().next(),
                _ => None,
            });
            let Ok(Value::Str(pat)) = eval_const(pattern) else {
                opaque = true;
                continue;
            };
            let Some(prefix) = crate::tsql::like_literal_prefix(&pat, esc) else {
                opaque = true;
                continue;
            };
            let hi = str_prefix_successor(&prefix);
            if hi.is_none() {
                opaque = true;
            }
            let mut bounds = vec![(BinaryOperator::GtEq, Value::Str(prefix))];
            if let Some(h) = hi {
                bounds.push((BinaryOperator::Lt, Value::Str(h)));
            }
            match &mut range {
                Some((_, c2, prev)) if *c2 == col => prev.append(&mut bounds),
                None => range = Some((root, col, bounds)),
                _ => opaque = true,
            }
            continue;
        }
        let SqlExpr::BinaryOp { left, op, right } = c else {
            opaque = true;
            continue;
        };
        // JSON_EXTRACT(col, '$.p') OP const — or the mirror image.
        let json_side = [left.as_ref(), right.as_ref()].into_iter().find(|e| {
            matches!(e, SqlExpr::Function(f) if f.name.to_string().to_uppercase() == "JSON_EXTRACT")
        });
        if let Some(SqlExpr::Function(f)) = json_side {
            let (Ok((pcol, ppath)), Ok(v)) = (
                parse_json_index_column(f),
                eval_const(if std::ptr::eq(left.as_ref(), json_side.unwrap()) {
                    right
                } else {
                    left
                }),
            ) else {
                opaque = true;
                continue;
            };
            if matches!(v, Value::Null) {
                opaque = true;
                continue;
            }
            let target = (pcol, ppath);
            let mut op = op.clone();
            if !std::ptr::eq(left.as_ref(), json_side.unwrap()) {
                let Some(m) = mirror_op(&op) else {
                    opaque = true;
                    continue;
                };
                op = m;
            }
            match &path_target {
                Some(t) if t != &target => {
                    opaque = true; // second distinct path target: inexact
                    continue;
                }
                None => path_target = Some(target),
                _ => {}
            }
            use BinaryOperator::*;
            match op {
                Eq => {
                    if let Some(prev) = &path_eq {
                        if Value::cmp_values(prev, &v) != Ordering::Equal {
                            eq_conflict = true;
                        }
                    } else {
                        path_eq = Some(v);
                    }
                }
                Gt | GtEq | Lt | LtEq => match &mut path_range {
                    Some(bounds) => bounds.push((op, v)),
                    None => path_range = Some(vec![(op, v)]),
                },
                _ => opaque = true,
            }
            continue;
        }
        // col OP lit, or lit OP col (mirrored)
        let (col_ref, op, lit, mirror) = match (left.as_ref(), right.as_ref()) {
            (SqlExpr::Identifier(_) | SqlExpr::CompoundIdentifier(_), r) => {
                (left.as_ref(), op.clone(), r, false)
            }
            (l, SqlExpr::Identifier(_) | SqlExpr::CompoundIdentifier(_)) => {
                (right.as_ref(), op.clone(), l, true)
            }
            _ => {
                opaque = true;
                continue;
            }
        };
        let name = match probe_col_name(col_ref) {
            Some(n) => n,
            None => {
                opaque = true;
                continue;
            }
        };
        // The literal side must be a constant (subqueries were substituted
        // away; column refs make this conjunct unusable).
        let Ok(v) = eval_expr(lit, &empty) else {
            opaque = true;
            continue;
        };
        if matches!(v, Value::Null) {
            opaque = true;
            continue;
        }
        let op = if mirror { mirror_op(&op)? } else { op };
        use BinaryOperator::*;
        match op {
            Eq => {
                // Composite prefixes match on declared column names, which
                // need not be index_roots keys themselves — collect every
                // equality (raw name), resolve the scalar path separately.
                if let Some(prev) = eq_map.get(&name) {
                    if Value::cmp_values(prev, &v) != Ordering::Equal {
                        eq_conflict = true;
                    }
                }
                if eq.is_none() {
                    if let Some((root, col)) = resolve_probe(&name) {
                        eq = Some((root, col, v.clone()));
                    }
                }
                eq_map.insert(name, v);
            }
            Gt | GtEq | Lt | LtEq => {
                let Some((root, col)) = resolve_probe(&name) else {
                    opaque = true;
                    continue;
                };
                match &mut range {
                    Some((_, c2, bounds)) if *c2 == col => bounds.push((op, v)),
                    None => range = Some((root, col, vec![(op, v)])),
                    _ => opaque = true,
                }
            }
            _ => opaque = true,
        }
    }
    // Equality coverage: every equality conjunct must name one of the probed
    // leading index columns, and do so distinctly (a bare/qualified duplicate
    // of the same column is not proof that both values were consumed).
    let covered = |cols: &[String], map: &std::collections::BTreeMap<String, Value>| -> bool {
        let mut seen = std::collections::BTreeSet::new();
        map.keys().all(|k| {
            let Some(name) = unqualified_col(k, table, alias, meta.dotted_keys) else {
                return false;
            };
            cols.contains(&name) && seen.insert(name)
        })
    };
    if eq_conflict {
        // Two different constants constrain the same column: no index probe
        // can be exact. Fall back to the full scan + residual filter.
        return None;
    }
    // JSON path probe (design 005 §3): a single-column index_def whose
    // column AND path both match, with a live root. exact only when this
    // path condition is the WHOLE plan — any other conjunct forces the
    // residual filter (conservative-correct; the fast paths re-check).
    if let Some((col, ppath)) = &path_target {
        if let Some(def) = meta.index_defs.iter().find(|d| {
            d.path.as_deref() == Some(ppath.as_str())
                && d.columns.as_slice() == std::slice::from_ref(col)
        }) {
            if meta.index_roots.contains_key(&def.name)
                && eq.is_none()
                && range.is_none()
                && eq_map.is_empty()
                && in_target.is_none()
            {
                let exact = !opaque
                    // An Eq plan leaves an unconsumed same-path range
                    // conjunct (`extract = 5 AND extract > 7`) out of the
                    // probe; exactness would claim every hit satisfies the
                    // whole predicate. Mirror the scalar branch's
                    // `range.is_none()` requirement.
                    && !(path_eq.is_some() && path_range.is_some());
                let plan = match (path_eq, path_range) {
                    (Some(v), _) => ProbePlan::Eq(v),
                    (None, Some(bounds)) => {
                        // An unbounded lower side must still reach rows whose
                        // extraction is NULL (`NULL < k` is TRUE — NULL sorts
                        // below everything), but those rows have no tree key.
                        // Only a bounded range is a candidate superset.
                        let lo = path_bound(&bounds, true);
                        lo.as_ref()?;
                        ProbePlan::Range {
                            lo,
                            hi: path_bound(&bounds, false),
                        }
                    }
                    (None, None) => return None,
                };
                return Some((def.name.clone(), plan, exact));
            }
        }
    }
    // Path conjuncts the dedicated plan did not consume (no matching path
    // index, or named-column conjuncts sharing the WHERE) ride the residual
    // filter; every column-plan branch below must stay inexact for them or
    // the exact window would silently drop the JSON predicate.
    if path_target.is_some() {
        opaque = true;
    }
    // Composite-index probe: longest equality prefix over the index columns
    // wins. Full-column equality is an exact key (Eq of the Array); a
    // partial prefix becomes a Prefix scan with a prefix retain. Non-leading
    // columns alone cannot use the tree — they fall back to the scan.
    if !eq_map.is_empty() {
        // eq_map keys keep their SQL spelling (t.a / x.a / a): normalize to
        // bare column names so qualified conjuncts still build the prefix —
        // `covered` already mapped them, only the prefix lookup missed.
        let mut bare_eq: std::collections::BTreeMap<String, Value> =
            std::collections::BTreeMap::new();
        for (k, v) in &eq_map {
            if let Some(name) = unqualified_col(k, table, alias, meta.dotted_keys) {
                bare_eq.insert(name, v.clone());
            }
        }
        let mut best: Option<(String, Vec<Value>)> = None;
        for root_key in meta.index_roots.keys() {
            let cols = meta.index_columns_of(root_key);
            if cols.len() < 2 {
                continue; // single-column trees take the scalar path below
            }
            // Partial composite index: usable only when the query WHERE
            // implies its predicate — otherwise the tree is missing rows the
            // query keeps (superset safety).
            if let Some(d) = meta.index_defs.iter().find(|d| d.name == *root_key) {
                if d.pred.is_some()
                    && !query_implies_pred(cond, d.pred.as_deref().expect("pred set"), table, alias)
                {
                    continue;
                }
            }
            let mut prefix = Vec::with_capacity(cols.len());
            for c in &cols {
                match bare_eq.get(c) {
                    Some(v) => prefix.push(v.clone()),
                    None => break,
                }
            }
            if prefix.is_empty() {
                continue;
            }
            // Partial prefix: a row matching the prefix whose remaining
            // index columns are NULL has no tree key, yet the WHERE (which
            // only constrains the prefix) keeps it — probing would silently
            // drop it. Full-column equality is immune (a NULL anywhere makes
            // the equality false for that row).
            if prefix.len() < cols.len()
                && cols[prefix.len()..]
                    .iter()
                    .any(|c| !meta.not_null.contains(c))
            {
                continue;
            }
            let longer = match &best {
                None => true,
                Some((_, bp)) => prefix.len() > bp.len(),
            };
            if longer {
                best = Some((root_key.clone(), prefix));
            }
        }
        if let Some((root_key, prefix)) = best {
            let cols = meta.index_columns_of(&root_key);
            let prefix_len = prefix.len();
            let plan = if prefix_len == cols.len() {
                ProbePlan::Eq(Value::Array(prefix))
            } else {
                ProbePlan::Prefix(Value::Array(prefix))
            };
            let exact = !opaque
                && range.is_none()
                && in_target.is_none()
                && covered(&cols[..prefix_len], &eq_map);
            return Some((root_key, plan, exact));
        }
    }
    if let Some((root, col, v)) = eq {
        let exact = !opaque
            && range.is_none()
            && in_target.is_none()
            && eq_map.len() == 1
            && eq_map.iter().all(|(k, ev)| {
                unqualified_col(k, table, alias, meta.dotted_keys).as_deref() == Some(&col)
                    && *ev == v
            });
        return Some((root, ProbePlan::Eq(v), exact));
    }
    // Equality-set probe: `col IN (…)` on a single-column tree, one bounded
    // Eq walk per element. NULL elements never match (the tree holds no NULL
    // keys), so the probe subsumes its conjunct exactly; equality/range
    // conjuncts it cannot carry stay in the residual filter and demote
    // exactness. An In plan beats a range when both exist — the list is
    // usually the tighter shape.
    if let Some((iroot, _icol, ivals)) = &in_target {
        if eq.is_none() && path_target.is_none() {
            let exact = !opaque && range.is_none() && eq_map.is_empty();
            return Some((iroot.clone(), ProbePlan::In(ivals.clone()), exact));
        }
        // Unconsumed: the winning plan keeps the IN list in the residual
        // filter — inexact.
        opaque = true;
    }
    let (root, col, bounds) = range?;
    let mut lo: Option<(Value, bool)> = None;
    let mut hi: Option<(Value, bool)> = None;
    // Same-side bounds from different comparison families where one is a
    // Timestamp and the other numeric must not be tightened against each
    // other: the raw order ranks Timestamps above numerics, so the
    // Timestamp literal would "win" and silently swallow the numeric
    // conjunct — an exact plan then sweeps a band whose rows the numeric
    // comparison excludes as NULL. The scan path keeps both conjuncts.
    let ts_num_mix = |a: &Value, b: &Value| -> bool {
        let fam = |v: &Value| -> u8 {
            match v {
                Value::Int(_) | Value::Float(_) | Value::Decimal(_) => 0,
                Value::Timestamp(_) => 1,
                _ => 2,
            }
        };
        let (x, y) = (fam(a), fam(b));
        (x == 0 && y == 1) || (x == 1 && y == 0)
    };
    for (op, v) in bounds {
        use BinaryOperator::*;
        use Ordering::Equal as Eq;
        // Keep the TIGHTEST bound per side, not the last one written —
        // `a > 10 AND a > 5` must probe from 10 (the residual filter
        // guarantees correctness either way; this only narrows the scan).
        match op {
            Gt | GtEq => {
                if lo.as_ref().is_some_and(|(cv, _)| ts_num_mix(&v, cv)) {
                    return None;
                }
                if lo.as_ref().is_none_or(|(cv, incl)| {
                    Value::cmp_values(&v, cv) == Ordering::Greater
                        || (Value::cmp_values(&v, cv) == Eq && *incl && op == Gt)
                }) {
                    lo = Some((v, op == GtEq));
                }
            }
            Lt | LtEq => {
                if hi.as_ref().is_some_and(|(cv, _)| ts_num_mix(&v, cv)) {
                    return None;
                }
                if hi.as_ref().is_none_or(|(cv, incl)| {
                    Value::cmp_values(&v, cv) == Ordering::Less
                        || (Value::cmp_values(&v, cv) == Eq && *incl && op == Lt)
                }) {
                    hi = Some((v, op == LtEq));
                }
            }
            _ => {}
        }
    }
    if lo.is_none() && hi.is_none() {
        return None;
    }
    // An unbounded lower side (`col < k`) is TRUE for NULL keys — NULL
    // sorts below every value — but the tree holds no NULL keys, so the
    // probe candidates would silently drop those rows. Only a NOT NULL
    // column makes the unbounded scan a superset.
    if lo.is_none() && !meta.not_null.contains(&col) {
        return None;
    }
    let exact = !opaque && eq_map.is_empty() && in_target.is_none();
    Some((root, ProbePlan::Range { lo, hi }, exact))
}

/// Mirror a comparison operator for `lit OP col` conjuncts.
pub(super) fn mirror_op(op: &BinaryOperator) -> Option<BinaryOperator> {
    use BinaryOperator::*;
    Some(match op {
        Eq => Eq,
        Lt => Gt,
        Gt => Lt,
        LtEq => GtEq,
        GtEq => LtEq,
        _ => return None,
    })
}

/// Per-tree maintenance spec, resolved once per statement: column list and
/// uniqueness of one index root. Hoisted because the per-row maintenance
/// loops used to re-resolve both through `index_columns_of` /
/// `root_key_unique` — a linear `index_defs` scan plus a `Vec` clone — for
/// every row of every tree.
pub(super) struct IdxSpec {
    pub(super) root_key: String,
    pub(super) cols: Vec<String>,
    pub(super) unique: bool,
    /// JSON path index (design 005): keys come from
    /// [`json_path_key`] over `cols[0]`'s JSON text, not the column value.
    pub(super) path: Option<String>,
    /// Partial index predicate (row-local, parsed once per statement):
    /// rows failing it never enter the tree, so [`reindex_insert`] skips
    /// them. Removal/repoint need no check — [`BTree::delete_entry`]
    /// no-ops on entries that were never there.
    pub(super) pred: Option<SqlExpr>,
}

/// Resolve [`IdxSpec`]s for every tree in `roots`, in root-key order
/// (matches the previous per-row `roots.keys()` iteration order).
pub(super) fn idx_specs(
    meta: &TableMeta,
    roots: &std::collections::BTreeMap<String, u32>,
) -> Vec<IdxSpec> {
    roots
        .keys()
        .filter_map(|k| {
            let cols = meta.index_columns_of(k);
            // Orphaned tree (dropped composite index): no column list, so no
            // maintenance key. Keep it out of every write path.
            (!cols.is_empty()).then(|| {
                let def = meta.index_defs.iter().find(|d| d.name == *k);
                IdxSpec {
                    root_key: k.clone(),
                    path: def.and_then(|d| d.path.clone()),
                    // Partial predicate parsed once per statement; a
                    // malformed stored text (impossible via CREATE) leaves
                    // the spec unfiltered — the tree then behaves full, so
                    // the read-side gate (which re-parses and on failure
                    // refuses the probe) still refuses to use it.
                    pred: def
                        .and_then(|d| d.pred.as_deref())
                        .and_then(|p| parse_expr_text(p).ok()),
                    cols,
                    unique: meta.root_key_unique(k),
                }
            })
        })
        .collect()
}

/// Re-point index entries after in-page slot moves: delete (key, old_loc)
/// and insert (key, new_loc) in every tree. `before` holds the page's
/// documents as they were before the mutation.
pub(super) fn reindex_repoint(
    pager: &Pager,
    tx: &mut crate::pager::Tx,
    specs: &[IdxSpec],
    roots: &mut std::collections::BTreeMap<String, u32>,
    before: &[(u64, Object)],
    old_l: u64,
    new_l: u64,
) -> Result<()> {
    let Some((_, doc)) = before.iter().find(|(l, _)| *l == old_l) else {
        return err("index fixup: moved document not found");
    };
    for spec in specs {
        if let Some(key) = index_key_of_spec(doc, spec) {
            let root = roots[&spec.root_key];
            let mut tree = BTree::open(root);
            let removed = tree
                .delete_entry(pager, tx, &key, old_l)
                .map_err(|e| index_err(&spec.root_key, e))?;
            // Only repoint an entry that was actually there: inserting on a
            // missed delete would add a second entry for the same document
            // (and a unique tree would spuriously reject it).
            if removed {
                tree.insert(pager, tx, key, new_l, spec.unique)
                    .map_err(|e| index_err(&spec.root_key, e))?;
            }
            if tree.root != root {
                roots.insert(spec.root_key.clone(), tree.root);
            }
        }
    }
    Ok(())
}

/// Drop one document's index entries (fast-path DELETE).
/// Composite/single-column index key for one document: single column → the
/// bare value (legacy tree layout); multiple columns → `Value::Array` over
/// the columns in key order (ordered element-wise by `cmp_values`).
/// `None` when any key column is missing or NULL — NULL does not enter
/// indexes (composite rule: one NULL column skips the whole key).
/// JSON path index key (design 005): parse the column's JSON text and
/// walk `path` with the SAME lookup `JSON_EXTRACT` evaluates inline —
/// extraction and query evaluation are one function, so probes cannot
/// diverge from row-local semantics. Column missing / non-text value /
/// malformed JSON / missing path → no key (the tree omits NULL keys, and
/// `JSON_EXTRACT(...) = x` is never true when the extract is NULL, so a
/// probe over this tree is semantically exact).
pub(super) fn json_path_key(doc: &Object, col: &str, path: &str) -> Option<Value> {
    let Value::Str(text) = doc.get(col)? else {
        return None;
    };
    let parsed = crate::json::from_str(text).ok()?;
    let v = json_path_lookup(&parsed, path)?;
    if matches!(v, Value::Null) {
        return None;
    }
    Some(v.clone())
}

/// Key extraction for one maintenance spec: JSON path or plain column(s).
pub(super) fn index_key_of_spec(doc: &Object, spec: &IdxSpec) -> Option<Value> {
    match &spec.path {
        Some(p) => json_path_key(doc, &spec.cols[0], p),
        None => index_key_of(doc, &spec.cols),
    }
}

/// Validate a JSON index path (design 005 §2): `$` followed by `.key` /
/// `[n]` segments only — the exact grammar [`json_path_lookup`] walks.
/// Quotes and backslashes are refused so the path can be embedded in the
/// dumped CREATE INDEX text without escaping.
pub(super) fn validate_json_index_path(path: &str) -> std::result::Result<(), String> {
    let p = path.trim();
    if !p.starts_with('$') {
        return Err(format!(
            "JSON index path {path:?} must start with '$' (e.g. '$.field[0].sub')"
        ));
    }
    let rest = &p[1..];
    if rest.is_empty() {
        return Err(
            "JSON index path must have at least one segment after '$' (e.g. '$.field')".into(),
        );
    }
    let mut rest = rest;
    while !rest.is_empty() {
        if let Some(stripped) = rest.strip_prefix('.') {
            let end = stripped.find(['.', '[']).unwrap_or(stripped.len());
            let key = &stripped[..end];
            if key.is_empty() {
                return Err(format!(
                    "JSON index path {path:?} has an empty '.key' segment"
                ));
            }
            if key.contains(['\'', '\\']) {
                return Err(format!(
                    "JSON index path {path:?} must not contain quotes or backslashes"
                ));
            }
            rest = &stripped[end..];
        } else if let Some(stripped) = rest.strip_prefix('[') {
            let Some(end) = stripped.find(']') else {
                return Err(format!("JSON index path {path:?} has an unterminated '[…'"));
            };
            if stripped[..end].parse::<usize>().is_err() {
                return Err(format!(
                    "JSON index path {path:?} has a non-numeric array index {:?}",
                    &stripped[..end]
                ));
            }
            rest = &stripped[end + 1..];
        } else {
            return Err(format!(
                "JSON index path {path:?} has a malformed segment (expected '.key' or '[n]')"
            ));
        }
    }
    Ok(())
}

/// Parse one CREATE INDEX column of the form JSON_EXTRACT(col, '$.path')
/// into (column, path). Anything else in expression position errors loudly.
pub(super) fn parse_json_index_column(f: &sqlparser::ast::Function) -> Result<(String, String)> {
    // Result here is the engine's alias (SqlError) — err() builds it.
    use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
    let args: Vec<&FunctionArg> = match &f.args {
        FunctionArguments::List(list) => list.args.iter().collect(),
        _ => return err("JSON_EXTRACT index column requires (col, '$.path') arguments"),
    };
    if args.len() != 2 {
        return err(format!(
            "JSON_EXTRACT index column requires exactly 2 arguments, got {}",
            args.len()
        ));
    }
    let col = match args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Identifier(i))) => i.value.clone(),
        _ => return err("JSON_EXTRACT index column's first argument must be a plain column name"),
    };
    let FunctionArg::Unnamed(FunctionArgExpr::Expr(path_expr)) = args[1] else {
        return err("JSON_EXTRACT index column's path must be a '$…' string literal");
    };
    let Value::Str(path) = eval_const(path_expr)? else {
        return err("JSON_EXTRACT index column's path must be a '$…' string literal");
    };
    validate_json_index_path(&path).map_err(SqlError::Parse)?;
    Ok((col, path))
}

pub(super) fn index_key_of(doc: &Object, cols: &[String]) -> Option<Value> {
    if cols.is_empty() {
        // Orphaned tree with no resolvable column list: no key belongs to it.
        return None;
    }
    if cols.len() == 1 {
        return match doc.get(&cols[0]) {
            Some(v) if !matches!(v, Value::Null) => Some(v.clone()),
            _ => None,
        };
    }
    let mut key = Vec::with_capacity(cols.len());
    for c in cols {
        match doc.get(c) {
            Some(v) if !matches!(v, Value::Null) => key.push(v.clone()),
            _ => return None,
        }
    }
    Some(Value::Array(key))
}

/// Remove one document's entries from every index tree (`specs` carries the
/// resolved column lists; see [`IdxSpec`]).
pub(super) fn reindex_remove(
    pager: &Pager,
    tx: &mut crate::pager::Tx,
    specs: &[IdxSpec],
    roots: &mut std::collections::BTreeMap<String, u32>,
    doc: &Object,
    loc: u64,
) -> Result<()> {
    for spec in specs {
        if let Some(key) = index_key_of_spec(doc, spec) {
            let root = roots[&spec.root_key];
            let mut tree = BTree::open(root);
            tree.delete_entry(pager, tx, &key, loc)
                .map_err(|e| index_err(&spec.root_key, e))?;
            if tree.root != root {
                roots.insert(spec.root_key.clone(), tree.root);
            }
        }
    }
    Ok(())
}

/// Insert one document's index entries (fast-path UPDATE pass 2). Uniqueness
/// comes from the tree: `unique` trees (constraint columns and
/// CREATE UNIQUE INDEX) reject duplicates at insert.
pub(super) fn reindex_insert(
    pager: &Pager,
    tx: &mut crate::pager::Tx,
    specs: &[IdxSpec],
    roots: &mut std::collections::BTreeMap<String, u32>,
    doc: &Object,
    loc: u64,
) -> Result<()> {
    for spec in specs {
        // Partial index: a row failing the predicate never enters the tree
        // (the removal side needs no mirror check — delete_entry no-ops on
        // absent entries, so flips in either direction stay consistent).
        if let Some(p) = &spec.pred {
            if !matches!(eval_expr(p, doc)?, Value::Bool(true)) {
                continue;
            }
        }
        if let Some(key) = index_key_of_spec(doc, spec) {
            let root = roots[&spec.root_key];
            let mut tree = BTree::open(root);
            tree.insert(pager, tx, key, loc, spec.unique)
                .map_err(|e| index_err(&spec.root_key, e))?;
            if tree.root != root {
                roots.insert(spec.root_key.clone(), tree.root);
            }
        }
    }
    Ok(())
}

impl<'a> ReadCx<'a> {
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
}
