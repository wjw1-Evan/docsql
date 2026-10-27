use super::*;

#[derive(Clone)]
pub(super) enum AggOp {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    GroupConcat,
}

// Short-lived, built per statement; size difference is fine here.
#[allow(clippy::large_enum_variant)]
pub(super) enum AggSpec {
    GroupKey {
        idx: usize,
    },
    /// `GROUPING(expr)`: 1 when the current grouping set omits `expr`, else
    /// 0 (SQL:2016). `idx` indexes the union of all set expressions.
    Grouping {
        idx: usize,
    },
    Agg {
        op: AggOp,
        arg: SqlExpr,
        distinct: bool,
        sep: Option<String>,
        /// Aggregate FILTER (WHERE …): rows that do not evaluate true are
        /// excluded before DISTINCT and aggregation.
        filter: Option<SqlExpr>,
    },
    /// Aggregate arithmetic in the projection (`SUM(x) + 1`, `-COUNT(*)`,
    /// `COALESCE(SUM(x), 0)`): the whole expression evaluates over the
    /// group with aggregate subcalls resolved.
    Composite {
        expr: SqlExpr,
    },
}

/// Parse a scalar-aggregate call (`COUNT(*)`, `SUM(DISTINCT x)`, ...) into
/// its operator, argument, DISTINCT flag and — for GROUP_CONCAT/STRING_AGG —
/// the optional constant separator. `COUNT(*)` rewrites to a sentinel column
/// so the executor counts rows instead of non-null values. The window
/// aggregate path (`SUM(x) OVER (...)`) passes `for_window` to reuse the
/// same argument parsing over a frame slice.
#[allow(clippy::type_complexity)]
pub(super) fn agg_parts(
    f: &sqlparser::ast::Function,
    for_window: bool,
) -> Result<(AggOp, SqlExpr, bool, Option<String>, Option<SqlExpr>)> {
    let fname = f.name.to_string().to_uppercase();
    // A window frame silently changes the result shape (one row per
    // partition); refusing is better than returning wrong numbers.
    if f.over.is_some() && !for_window {
        return err(format!(
            "window functions (OVER) are not supported: {fname}"
        ));
    }
    // ORDER BY inside the aggregate's own argument list (e.g.
    // STRING_AGG(x, ',' ORDER BY y)) is only meaningful for order-sensitive
    // aggregates; dropping it silently would concatenate in scan order.
    // WITHIN GROUP is the same request spelled as a clause.
    let arg_order = match &f.args {
        sqlparser::ast::FunctionArguments::List(list) => list.clauses.iter().any(|c| {
            matches!(
                c,
                sqlparser::ast::FunctionArgumentClause::OrderBy(_)
                    | sqlparser::ast::FunctionArgumentClause::Limit(_)
                    | sqlparser::ast::FunctionArgumentClause::OnOverflow(_)
            )
        }),
        _ => false,
    };
    if arg_order || !f.within_group.is_empty() {
        return err(format!(
            "ORDER BY inside aggregate arguments is not supported: {fname}"
        ));
    }
    let (distinct, inner, second) = match &f.args {
        sqlparser::ast::FunctionArguments::List(list) => {
            let distinct = matches!(
                list.duplicate_treatment,
                Some(sqlparser::ast::DuplicateTreatment::Distinct)
            );
            let as_expr = |a: &sqlparser::ast::FunctionArg| match a {
                sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) => {
                    Some(e.clone())
                }
                _ => None,
            };
            let inner = list.args.first().and_then(|a| match a {
                sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Wildcard) => {
                    Some(SqlExpr::Identifier(sqlparser::ast::Ident::new("__count__")))
                }
                other => as_expr(other),
            });
            let second = list.args.get(1).and_then(as_expr);
            (distinct, inner, second)
        }
        _ => (false, None, None),
    };
    let Some(arg) = inner else {
        return err(format!("unsupported aggregate arguments: {fname}"));
    };
    let op = match fname.as_str() {
        // COUNT_BIG is T-SQL's COUNT returning bigint; same semantics here.
        "COUNT" | "COUNT_BIG" => AggOp::Count,
        "SUM" => AggOp::Sum,
        "AVG" => AggOp::Avg,
        "MIN" => AggOp::Min,
        "MAX" => AggOp::Max,
        "GROUP_CONCAT" | "STRING_AGG" => AggOp::GroupConcat,
        other => return err(format!("unknown function: {other}")),
    };
    let sep = if matches!(op, AggOp::GroupConcat) {
        match second {
            Some(e) => match eval_const(&e)? {
                Value::Str(s) => Some(s),
                Value::Null => None,
                other => {
                    return err(format!(
                        "group_concat separator must be a string literal, got {}",
                        other.type_name()
                    ))
                }
            },
            None => None,
        }
    } else {
        None
    };
    // FILTER (WHERE …) is an aggregate-row filter, applied before DISTINCT
    // and the aggregate itself (SQL:2003).
    let filter = f.filter.as_deref().cloned();
    Ok((op, arg, distinct, sep, filter))
}

pub(super) fn add_values(a: Value, b: Value) -> Result<Value> {
    // NULL is sticky: an overflowed partial (see the Int/Int arm below) must
    // keep the whole SUM NULL instead of erroring "non-numeric" on the next
    // addition — the result used to depend on where the overflow landed.
    if matches!(a, Value::Null) || matches!(b, Value::Null) {
        return Ok(Value::Null);
    }
    // Decimal poisons the sum toward exactness: Int/Float operands convert
    // losslessly (a Float contributes its exact binary value).
    if matches!(a, Value::Decimal(_)) || matches!(b, Value::Decimal(_)) {
        let (x, y) = (as_decimal(&a)?, as_decimal(&b)?);
        return Ok(x.checked_add(y).map(Value::Decimal).unwrap_or(Value::Null));
    }
    match (a, b) {
        // Overflow yields NULL, matching the scalar `+` path (a wrapped
        // negative SUM would silently book a huge total as a loss).
        (Value::Int(x), Value::Int(y)) => {
            Ok(x.checked_add(y).map(Value::Int).unwrap_or(Value::Null))
        }
        (Value::Int(x), Value::Float(y)) => Ok(Value::Float(x as f64 + y)),
        (Value::Float(x), Value::Int(y)) => Ok(Value::Float(x + y as f64)),
        (Value::Float(x), Value::Float(y)) => Ok(Value::Float(x + y)),
        _ => err("SUM of non-numeric values"),
    }
}

pub(super) fn row_key(row: &[Value]) -> Vec<u8> {
    encode::encode_to_vec(&Value::Array(row.to_vec())).unwrap_or_default()
}

/// Keep the first occurrence of each distinct row.
pub(super) fn dedup_rows(rows: &mut Vec<Vec<Value>>) {
    let mut seen = std::collections::BTreeSet::new();
    rows.retain(|row| seen.insert(row_key(row)));
}

/// Multiset of rows for ALL-quantified set operations.
pub(super) fn row_counts(rows: &[Vec<Value>]) -> std::collections::BTreeMap<Vec<u8>, usize> {
    let mut counts = std::collections::BTreeMap::new();
    for row in rows {
        *counts.entry(row_key(row)).or_insert(0) += 1;
    }
    counts
}

/// If `row` has a remaining occurrence in `counts`, consume one and return
/// true; otherwise return false.
pub(super) fn consume_one(
    counts: &mut std::collections::BTreeMap<Vec<u8>, usize>,
    row: &[Value],
) -> bool {
    match counts.get_mut(&row_key(row)) {
        Some(n) if *n > 0 => {
            *n -= 1;
            true
        }
        _ => false,
    }
}

pub(super) fn eval_agg(
    spec: &AggSpec,
    docs: &[&Object],
    key: &[Value],
    group_exprs: &[SqlExpr],
) -> Result<Value> {
    match spec {
        AggSpec::GroupKey { idx } => Ok(key.get(*idx).cloned().unwrap_or(Value::Null)),
        // Resolved per grouping set by `eval_grouped_spec` before reaching
        // the aggregate evaluator.
        AggSpec::Grouping { .. } => err("GROUPING is only supported in the SELECT list"),
        AggSpec::Composite { expr } => {
            let empty = Object::new();
            eval_group_expr(expr, &empty, docs, group_exprs)
        }
        AggSpec::Agg {
            op,
            arg,
            distinct,
            sep,
            filter,
        } => {
            // FILTER applies first: only rows whose predicate is TRUE take
            // part in DISTINCT and the aggregate.
            let kept: Vec<&Object> = match filter {
                Some(f) => {
                    let mut kept = Vec::with_capacity(docs.len());
                    for d in docs {
                        if matches!(eval_expr(f, d)?, Value::Bool(true)) {
                            kept.push(*d);
                        }
                    }
                    kept
                }
                None => docs.to_vec(),
            };
            let mut vals: Vec<Value> = Vec::new();
            for d in &kept {
                let v = eval_expr(arg, d)?;
                if !matches!(v, Value::Null) {
                    vals.push(v);
                }
            }
            // DISTINCT aggregates dedup before combining.
            if *distinct {
                let mut seen = std::collections::BTreeSet::new();
                vals.retain(|v| seen.insert(encode::encode_to_vec(v).unwrap_or_default()));
            }
            Ok(match op {
                AggOp::Count => {
                    // COUNT(*) is rewritten to a sentinel column that never
                    // exists, so count rows instead of non-null values.
                    if matches!(arg, SqlExpr::Identifier(i) if i.value == "__count__") {
                        Value::Int(kept.len() as i64)
                    } else {
                        Value::Int(vals.len() as i64)
                    }
                }
                AggOp::Sum => {
                    if vals.is_empty() {
                        Value::Null
                    } else {
                        // Fold from a numeric zero so a lone value goes
                        // through add_values' type check too: SUM over a
                        // single BLOB/text used to return the value itself
                        // and only erroed once a second row arrived.
                        let mut sum = Value::Int(0);
                        for v in vals {
                            sum = add_values(sum, v)?;
                        }
                        sum
                    }
                }
                AggOp::Avg => {
                    if vals.is_empty() {
                        Value::Null
                    } else if vals.iter().any(|v| matches!(v, Value::Decimal(_))) {
                        // Exact averaging when any input is decimal: sum first
                        // (exact), then divide once.
                        let mut acc = Decimal::ZERO;
                        for v in &vals {
                            acc = acc
                                .checked_add(as_decimal(v)?)
                                .ok_or_else(|| SqlError::Message("AVG sum overflow".into()))?;
                        }
                        match acc.checked_div(Decimal::from(vals.len() as i64)) {
                            Some(d) => Value::Decimal(d),
                            None => Value::Null,
                        }
                    } else {
                        let n = vals.len() as f64;
                        let mut acc = 0.0f64;
                        for v in &vals {
                            acc += match v {
                                Value::Int(i) => *i as f64,
                                Value::Float(f) => *f,
                                _ => return err("AVG of non-numeric"),
                            };
                        }
                        Value::Float(acc / n)
                    }
                }
                AggOp::Min => vals
                    .into_iter()
                    .reduce(|a, b| {
                        if Value::cmp_values(&a, &b) != std::cmp::Ordering::Greater {
                            a
                        } else {
                            b
                        }
                    })
                    .unwrap_or(Value::Null),
                AggOp::Max => vals
                    .into_iter()
                    .reduce(|a, b| {
                        if Value::cmp_values(&a, &b) != std::cmp::Ordering::Less {
                            a
                        } else {
                            b
                        }
                    })
                    .unwrap_or(Value::Null),
                AggOp::GroupConcat => {
                    // Zero contributing rows (or all-NULL input) is NULL, not
                    // the empty string — IS NULL checks depend on it.
                    if vals.is_empty() {
                        Value::Null
                    } else {
                        Value::Str(
                            vals.iter()
                                .map(value_to_text)
                                .collect::<Vec<_>>()
                                .join(sep.as_deref().unwrap_or(",")),
                        )
                    }
                }
            })
        }
    }
}

/// Expand GROUP BY into grouping sets. Plain expressions belong to every
/// set; `ROLLUP(a, b)` contributes `(a,b), (a), ()`; `CUBE(a, b)` all
/// subsets; `GROUPING SETS` its explicit list. Several super-grouping items
/// combine as a cartesian product (SQL:2016).
pub(super) fn expand_grouping_sets(exprs: &[SqlExpr]) -> Result<Vec<Vec<SqlExpr>>> {
    let mut sets: Vec<Vec<SqlExpr>> = vec![Vec::new()];
    for e in exprs {
        let alternatives: Vec<Vec<SqlExpr>> = match e {
            SqlExpr::Rollup(groups) => (0..=groups.len())
                .rev()
                .map(|n| groups[..n].iter().flatten().cloned().collect())
                .collect(),
            SqlExpr::Cube(groups) => {
                if groups.len() > 12 {
                    return err("CUBE supports at most 12 elements");
                }
                (0..(1u32 << groups.len()))
                    .map(|mask| {
                        groups
                            .iter()
                            .enumerate()
                            .filter(|(i, _)| mask & (1 << i) != 0)
                            .flat_map(|(_, g)| g.iter().cloned())
                            .collect()
                    })
                    .collect()
            }
            SqlExpr::GroupingSets(lists) => lists.clone(),
            other => vec![vec![other.clone()]],
        };
        // Super-grouping items multiply (SQL:2016 cartesian semantics);
        // cap the product so `CUBE(a1..a12), CUBE(b1..b12), …` cannot turn
        // into an astronomic allocation before the error surfaces.
        let product = match sets.len().checked_mul(alternatives.len()) {
            Some(p) => p,
            None => return err("GROUP BY expands past the supported grouping set limit"),
        };
        if product > MAX_GROUPING_SETS {
            return err(format!(
                "GROUP BY expands to more than {MAX_GROUPING_SETS} grouping sets"
            ));
        }
        let mut next = Vec::with_capacity(product);
        for base in &sets {
            for alt in &alternatives {
                let mut combined = base.clone();
                combined.extend(alt.iter().cloned());
                next.push(combined);
            }
        }
        sets = next;
    }
    let mut seen = std::collections::BTreeSet::new();
    for set in &sets {
        for e in set {
            if matches!(
                e,
                SqlExpr::Rollup(_) | SqlExpr::Cube(_) | SqlExpr::GroupingSets(_)
            ) {
                return err("nested grouping sets are not supported");
            }
            // Inside GROUPING SETS the parser turns ROLLUP/CUBE/GROUPING
            // into function calls; they are still nested super-grouping.
            if let SqlExpr::Function(f) = e {
                let name = f.name.to_string().to_uppercase();
                if name == "ROLLUP" || name == "CUBE" || name == "GROUPING" {
                    return err("nested grouping sets are not supported");
                }
            }
        }
        // Duplicate sets would emit duplicate rows; the standard forbids them.
        let key: Vec<String> = set.iter().map(|e| e.to_string()).collect();
        if !seen.insert(key) {
            return err("duplicate grouping set");
        }
    }
    Ok(sets)
}

/// Position of `all_exprs[idx]` inside one grouping set, by expression text
/// (the same identity `agg_spec` resolves group keys with).
pub(super) fn set_position(all_exprs: &[SqlExpr], set: &[SqlExpr], idx: usize) -> Option<usize> {
    let target = &all_exprs[idx];
    set.iter()
        .position(|s| format!("{s}") == format!("{target}"))
}

/// Evaluate one projection spec for a group of one grouping set: keys that
/// this set omits read as NULL, GROUPING() reports the omission, and the
/// aggregates run over the group's rows.
pub(super) fn eval_grouped_spec(
    spec: &AggSpec,
    all_exprs: &[SqlExpr],
    set: &[SqlExpr],
    key: &[Value],
    docs: &[&Object],
) -> Result<Value> {
    match spec {
        AggSpec::GroupKey { idx } => Ok(set_position(all_exprs, set, *idx)
            .and_then(|p| key.get(p).cloned())
            .unwrap_or(Value::Null)),
        AggSpec::Grouping { idx } => Ok(Value::Int(
            if set_position(all_exprs, set, *idx).is_some() {
                0
            } else {
                1
            },
        )),
        AggSpec::Agg { .. } => eval_agg(spec, docs, key, set),
        AggSpec::Composite { expr } => {
            let empty = Object::new();
            eval_group_expr(expr, &empty, docs, set)
        }
    }
}

pub(super) fn is_agg_item(item: &SelectItem) -> bool {
    let e = match item {
        SelectItem::UnnamedExpr(e) => e,
        SelectItem::ExprWithAlias { expr, .. } => expr,
        _ => return false,
    };
    contains_agg(e)
}

/// True for the aggregate functions the executor knows (`COUNT`/`SUM`/...),
/// regardless of window usage: the window aggregate path reuses the same
/// argument parsing.
pub(super) fn is_agg_name(f: &sqlparser::ast::Function) -> bool {
    matches!(
        f.name.to_string().to_uppercase().as_str(),
        "COUNT" | "COUNT_BIG" | "SUM" | "AVG" | "MIN" | "MAX" | "GROUP_CONCAT" | "STRING_AGG"
    )
}

/// True for the aggregate functions the executor knows (`COUNT`/`SUM`/...).
/// Window forms (`OVER`) are never aggregates here.
pub(super) fn is_agg_fn(f: &sqlparser::ast::Function) -> bool {
    !f.over.is_some() && is_agg_name(f)
}

pub(super) fn contains_agg(e: &SqlExpr) -> bool {
    match e {
        SqlExpr::Function(f) => {
            if is_agg_fn(f) {
                return true;
            }
            // Aggregate calls wrapped in scalar functions or arithmetic
            // (`COALESCE(SUM(x), 0)`, `SUM(x) + 1`) count too.
            match &f.args {
                sqlparser::ast::FunctionArguments::List(list) => list.args.iter().any(|a| {
                    matches!(
                        a,
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(inner),
                        ) if contains_agg(inner)
                    )
                }),
                _ => false,
            }
        }
        SqlExpr::Nested(inner) => contains_agg(inner),
        SqlExpr::BinaryOp { left, right, .. } => contains_agg(left) || contains_agg(right),
        SqlExpr::UnaryOp { expr, .. } => contains_agg(expr),
        // Wrappers the projection router must see through: `CAST(SUM(x))`,
        // `SUM(x) BETWEEN …`, `SUM(x) IS NULL`, `SUM(x) LIKE …`,
        // `SUM(x) IN (…)` — all evaluate aggregates (check_group_refs and
        // eval_group_expr implement the arms; only the routing predicate
        // was missing, which turned these into loud errors).
        SqlExpr::Cast { expr, .. } => contains_agg(expr),
        SqlExpr::Between {
            expr, low, high, ..
        } => contains_agg(expr) || contains_agg(low) || contains_agg(high),
        SqlExpr::IsNull(expr) => contains_agg(expr),
        SqlExpr::IsNotNull(expr) => contains_agg(expr),
        SqlExpr::IsDistinctFrom(a, b) | SqlExpr::IsNotDistinctFrom(a, b) => {
            contains_agg(a) || contains_agg(b)
        }
        SqlExpr::InList { expr, list, .. } => contains_agg(expr) || list.iter().any(contains_agg),
        SqlExpr::Like { .. } | SqlExpr::ILike { .. } => {
            // negated/any flags and the char escape cannot carry aggregates;
            // only the two operand expressions can.
            let (expr, pattern) = match e {
                SqlExpr::Like { expr, pattern, .. } | SqlExpr::ILike { expr, pattern, .. } => {
                    (expr, pattern)
                }
                _ => unreachable!(),
            };
            contains_agg(expr) || contains_agg(pattern)
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand.as_deref().is_some_and(contains_agg)
                || conditions
                    .iter()
                    .any(|w| contains_agg(&w.condition) || contains_agg(&w.result))
                || else_result.as_deref().is_some_and(contains_agg)
        }
        _ => false,
    }
}

// ---------- Window functions (OVER …) ----------
//
// SELECT-list window computation: calls are collected in first-appearance
// order (deduplicated by full text — two identical calls share one column),
// evaluated per row right after WHERE (SQL semantics: windows see the
// filtered rowset, before DISTINCT/ORDER BY/LIMIT), injected into the row
// docs as private `__w<i>$` columns and each call site rewritten to a
// reference of that column — DISTINCT / ORDER BY / LIMIT then run unchanged.
// Frames other than the default (RANGE UNBOUNDED PRECEDING AND CURRENT ROW)
// are refused explicitly, never silently narrowed.
