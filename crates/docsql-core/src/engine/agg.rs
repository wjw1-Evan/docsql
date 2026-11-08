use super::*;

/// COUNT(*) is parsed into a sentinel identifier that never names a real
/// column; every producer/consumer of the rewrite shares this one spelling.
pub(super) const COUNT_STAR_SENTINEL: &str = "__count__";

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
    let fname = func_upper(&f.name.to_string());
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
                    Some(SqlExpr::Identifier(sqlparser::ast::Ident::new(
                        COUNT_STAR_SENTINEL,
                    )))
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
    let Some(op) = parse_agg_name(f) else {
        return err(format!("unknown function: {fname}"));
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

/// The ONE aggregate fold (SUM/AVG/MIN/MAX/COUNT/GROUP_CONCAT) behind the
/// batch evaluator ([`eval_agg`]'s Agg arm), PIVOT's cell reducer and the
/// window [`RunningAgg`] — the three used to hand-mirror the same
/// semantics (AVG's exact-decimal mode flip, the `cmp_values` tie rules
/// keeping the FIRST value, the NULL contracts) in three places, and the
/// mirrors had already drifted (PIVOT's MAX kept the LAST tie via
/// `max_by`; its SUM folded through `arith` instead of `add_values`).
/// The batch evaluator is the authoritative shape the other two now ride.
///
/// The sliding-frame window aggregate is the deliberate exception: it also
/// REMOVES rows (an inverse mirror of add with a degraded-refold path),
/// which this add-only core does not express.
pub(super) struct AggState {
    op: AggOp,
    sep: Option<String>,
    /// COUNT(*): the arg is the sentinel identifier — count rows, not
    /// non-null argument values.
    count_star: bool,
    distinct: bool,
    seen: std::collections::BTreeSet<Vec<u8>>,
    /// Rows passing FILTER (COUNT(*) counts them even when the arg is NULL).
    kept: i64,
    /// Non-null, post-DISTINCT argument values (COUNT(x); SUM/AVG empty).
    nonnull: i64,
    sum: Option<Value>,
    /// AVG exact-decimal mode flips on permanently at the first Decimal
    /// input (batch AVG picks its mode from the whole frame); the pre-flip
    /// prefix stays buffered for the one-time exact conversion.
    avg_dec_mode: bool,
    avg_vals: Vec<Value>,
    avg_dec: Decimal,
    avg_f64: f64,
    min: Option<Value>,
    max: Option<Value>,
    concat: String,
    concat_any: bool,
}

impl AggState {
    pub(super) fn new(op: AggOp, sep: Option<String>, count_star: bool, distinct: bool) -> Self {
        Self {
            op,
            sep,
            count_star,
            distinct,
            seen: std::collections::BTreeSet::new(),
            kept: 0,
            nonnull: 0,
            sum: None,
            avg_dec_mode: false,
            avg_vals: Vec::new(),
            avg_dec: Decimal::ZERO,
            avg_f64: 0.0,
            min: None,
            max: None,
            concat: String::new(),
            concat_any: false,
        }
    }

    /// A row passing FILTER (COUNT(*) counts these even when the argument
    /// value is NULL or excluded).
    pub(super) fn note_kept(&mut self) {
        self.kept += 1;
    }

    /// Fold one contributing value: NULL is skipped, DISTINCT dedups by
    /// encoded bytes (an encode failure is loud — a silent default key
    /// would dedup unrelated values into one).
    pub(super) fn push(&mut self, v: &Value) -> Result<()> {
        if matches!(v, Value::Null) {
            return Ok(());
        }
        if self.distinct
            && !self
                .seen
                .insert(encode::encode_to_vec(v).map_err(SqlError::Encode)?)
        {
            return Ok(());
        }
        self.nonnull += 1;
        match self.op {
            AggOp::Count => {}
            AggOp::Sum => {
                let next = match self.sum.take() {
                    // Fold from a numeric zero so a lone value goes through
                    // add_values' type check too: SUM over a single
                    // BLOB/text used to return the value itself and only
                    // errored once a second row arrived.
                    None => add_values(Value::Int(0), v.clone())?,
                    Some(s) => add_values(s, v.clone())?,
                };
                self.sum = Some(next);
            }
            AggOp::Avg => match v {
                Value::Decimal(d) => {
                    if !self.avg_dec_mode {
                        self.avg_dec_mode = true;
                        self.avg_dec = Decimal::ZERO;
                        for buffered in std::mem::take(&mut self.avg_vals) {
                            self.avg_dec = self
                                .avg_dec
                                .checked_add(as_decimal(&buffered)?)
                                .ok_or_else(|| SqlError::Message("AVG sum overflow".into()))?;
                        }
                    }
                    self.avg_dec = self
                        .avg_dec
                        .checked_add(*d)
                        .ok_or_else(|| SqlError::Message("AVG sum overflow".into()))?;
                }
                other => {
                    if self.avg_dec_mode {
                        self.avg_dec = self
                            .avg_dec
                            .checked_add(as_decimal(other)?)
                            .ok_or_else(|| SqlError::Message("AVG sum overflow".into()))?;
                    } else {
                        self.avg_vals.push(other.clone());
                        self.avg_f64 += match other {
                            Value::Int(i) => *i as f64,
                            Value::Float(f) => *f,
                            _ => return err("AVG of non-numeric"),
                        };
                    }
                }
            },
            AggOp::Min => match &self.min {
                // Ties keep the first value, exactly like the batch reduce.
                Some(m) if Value::cmp_values(m, v) != std::cmp::Ordering::Greater => {}
                _ => self.min = Some(v.clone()),
            },
            AggOp::Max => match &self.max {
                Some(m) if Value::cmp_values(m, v) != std::cmp::Ordering::Less => {}
                _ => self.max = Some(v.clone()),
            },
            AggOp::GroupConcat => {
                // The separator must precede every element after the FIRST
                // pushed one (an element rendering as "" still counts).
                if self.concat_any {
                    self.concat.push_str(self.sep.as_deref().unwrap_or(","));
                } else {
                    self.concat_any = true;
                }
                self.concat.push_str(&value_to_text(v));
            }
        }
        Ok(())
    }

    /// Value of the aggregate over everything folded so far.
    pub(super) fn snapshot(&self) -> Value {
        match self.op {
            AggOp::Count => Value::Int(if self.count_star {
                self.kept
            } else {
                self.nonnull
            }),
            AggOp::Sum => self.sum.clone().unwrap_or(Value::Null),
            AggOp::Avg => {
                if self.nonnull == 0 {
                    Value::Null
                } else if self.avg_dec_mode {
                    match self.avg_dec.checked_div(Decimal::from(self.nonnull)) {
                        Some(d) => Value::Decimal(d),
                        None => Value::Null,
                    }
                } else {
                    Value::Float(self.avg_f64 / self.nonnull as f64)
                }
            }
            AggOp::Min => self.min.clone().unwrap_or(Value::Null),
            AggOp::Max => self.max.clone().unwrap_or(Value::Null),
            // Zero contributing rows (or an all-NULL frame) is NULL, not the
            // empty string — IS NULL checks depend on it.
            AggOp::GroupConcat => {
                if self.concat_any {
                    Value::Str(self.concat.clone())
                } else {
                    Value::Null
                }
            }
        }
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
            // part in DISTINCT and the aggregate. The fold itself is the
            // shared AggState — the batch evaluator is its authoritative
            // shape.
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
            let count_star =
                matches!(arg, SqlExpr::Identifier(i) if i.value == COUNT_STAR_SENTINEL);
            let mut st = AggState::new(op.clone(), sep.clone(), count_star, *distinct);
            for d in &kept {
                st.note_kept();
                let v = eval_expr(arg, d)?;
                st.push(&v)?;
            }
            Ok(st.snapshot())
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
                let name = func_upper(&f.name.to_string());
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
/// True for the aggregate functions the executor knows, regardless of window
/// usage. The single word list: parse (agg dispatch) and the
/// aggregate-not-allowed guards read the same one.
pub(super) fn parse_agg_name(f: &sqlparser::ast::Function) -> Option<AggOp> {
    match func_upper(&f.name.to_string()).as_str() {
        // COUNT_BIG is T-SQL.s COUNT returning bigint; same semantics here.
        "COUNT" | "COUNT_BIG" => Some(AggOp::Count),
        "SUM" => Some(AggOp::Sum),
        "AVG" => Some(AggOp::Avg),
        "MIN" => Some(AggOp::Min),
        "MAX" => Some(AggOp::Max),
        "GROUP_CONCAT" | "STRING_AGG" => Some(AggOp::GroupConcat),
        _ => None,
    }
}

pub(super) fn is_agg_name(f: &sqlparser::ast::Function) -> bool {
    parse_agg_name(f).is_some()
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
