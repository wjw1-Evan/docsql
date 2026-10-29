use super::*;

pub(super) fn window_tmp_col(i: usize) -> String {
    format!("__w{i}$")
}

/// Running (prefix-frame) fold for aggregate OVER with the default frame.
/// Mirrors `eval_agg`'s Agg arm exactly — same left-to-right evaluation
/// order, same FILTER/NULL/DISTINCT handling, same tie rules, same AVG
/// decimal-mode selection — so each snapshot equals the batch aggregate
/// evaluated over the frame ending at the current peer run, while the whole
/// partition costs O(n) evaluations instead of O(n²) re-folds.
pub(super) struct RunningAgg {
    op: AggOp,
    sep: Option<String>,
    distinct: bool,
    seen: std::collections::BTreeSet<Vec<u8>>,
    /// COUNT(*): the arg is the `__count__` sentinel — count rows, not
    /// non-null arg values.
    count_star: bool,
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

impl RunningAgg {
    pub(super) fn new(op: AggOp, sep: Option<String>, distinct: bool, count_star: bool) -> Self {
        Self {
            op,
            sep,
            distinct,
            seen: std::collections::BTreeSet::new(),
            count_star,
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

    /// Fold one partition row (in frame order) into the running state.
    /// `argval` is None for FILTER-excluded rows (their argument was never
    /// evaluated, matching the batch evaluator's filter-first order).
    pub(super) fn push_row(&mut self, kept: bool, argval: Option<&Value>) -> Result<()> {
        if kept {
            self.kept += 1;
        }
        let Some(argval) = argval else {
            return Ok(());
        };
        if matches!(argval, Value::Null) {
            return Ok(());
        }
        if self.distinct
            && !self
                .seen
                .insert(encode::encode_to_vec(argval).map_err(SqlError::Encode)?)
        {
            return Ok(());
        }
        self.nonnull += 1;
        match self.op {
            AggOp::Count => {}
            AggOp::Sum => {
                let next = match self.sum.take() {
                    None => add_values(Value::Int(0), argval.clone())?,
                    Some(s) => add_values(s, argval.clone())?,
                };
                self.sum = Some(next);
            }
            AggOp::Avg => match argval {
                Value::Decimal(d) => {
                    if !self.avg_dec_mode {
                        self.avg_dec_mode = true;
                        self.avg_dec = Decimal::ZERO;
                        for v in std::mem::take(&mut self.avg_vals) {
                            self.avg_dec = self
                                .avg_dec
                                .checked_add(as_decimal(&v)?)
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
                Some(m) if Value::cmp_values(m, argval) != std::cmp::Ordering::Greater => {}
                _ => self.min = Some(argval.clone()),
            },
            AggOp::Max => match &self.max {
                Some(m) if Value::cmp_values(m, argval) != std::cmp::Ordering::Less => {}
                _ => self.max = Some(argval.clone()),
            },
            AggOp::GroupConcat => {
                // The separator must precede every element after the FIRST
                // pushed one (an element rendering as "" still counts).
                if self.concat_any {
                    self.concat.push_str(self.sep.as_deref().unwrap_or(","));
                } else {
                    self.concat_any = true;
                }
                self.concat.push_str(&value_to_text(argval));
            }
        }
        Ok(())
    }

    /// Value of the aggregate over everything folded so far — the frame
    /// ending at the current peer run.
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
            // empty string — the batch reduce keeps the same contract and
            // IS NULL checks depend on it.
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

/// Sliding-frame aggregate for custom window frames (ROWS/RANGE/GROUPS
/// BETWEEN): the frame's rows live in a FIFO deque and every aggregate is
/// maintained incrementally — push advances the frame's high edge, `pop_to`
/// its low edge. Both edges only move forward (frame bounds are monotone
/// over the sorted partition), so the whole computation is O(n) pushes and
/// pops. SUM/AVG removal mirrors [`add_values`]; an unrecoverable inverse
/// (Int overflow — the batch fold NULLs there too) sets `degraded` and the
/// caller refolds that partition's remaining rows from the frame slice.
/// DISTINCT and GROUP_CONCAT cannot invert and never route here.
pub(super) struct SlidingFrameAgg {
    pub(super) op: AggOp,
    pub(super) count_star: bool,
    pub(super) kept: i64,
    pub(super) nonnull: i64,
    pub(super) sum: Option<Value>,
    pub(super) avg_dec_mode: bool,
    pub(super) avg_vals: std::collections::VecDeque<Value>,
    pub(super) avg_dec: Decimal,
    pub(super) avg_f64: f64,
    /// Monotone candidate deque for MIN/MAX: (sorted position, value).
    /// The front is the current extremum, earliest on ties — exactly the
    /// batch reduce's first-wins rule.
    pub(super) extreme: std::collections::VecDeque<(usize, Value)>,
    /// The frame's rows in order: (sorted position, FILTER-kept, argument).
    pub(super) window: std::collections::VecDeque<(usize, bool, Option<Value>)>,
    pub(super) degraded: bool,
}

impl SlidingFrameAgg {
    pub(super) fn new(op: AggOp, count_star: bool) -> Self {
        Self {
            op,
            count_star,
            kept: 0,
            nonnull: 0,
            sum: None,
            avg_dec_mode: false,
            avg_vals: std::collections::VecDeque::new(),
            avg_dec: Decimal::ZERO,
            avg_f64: 0.0,
            extreme: std::collections::VecDeque::new(),
            window: std::collections::VecDeque::new(),
            degraded: false,
        }
    }

    /// Fold the row at sorted position `pos` into the state (frame high edge
    /// advances). Same fold order and NULL/FILTER rules as
    /// [`RunningAgg::push_row`].
    pub(super) fn push(&mut self, pos: usize, kept: bool, argval: Option<&Value>) -> Result<()> {
        self.window.push_back((pos, kept, argval.cloned()));
        if kept {
            self.kept += 1;
        }
        let Some(argval) = argval else {
            return Ok(());
        };
        if matches!(argval, Value::Null) {
            return Ok(());
        }
        match self.op {
            AggOp::Min => {
                while self
                    .extreme
                    .back()
                    .is_some_and(|(_, b)| Value::cmp_values(b, argval) == Ordering::Greater)
                {
                    self.extreme.pop_back();
                }
                self.extreme.push_back((pos, argval.clone()));
                return Ok(());
            }
            AggOp::Max => {
                while self
                    .extreme
                    .back()
                    .is_some_and(|(_, b)| Value::cmp_values(b, argval) == Ordering::Less)
                {
                    self.extreme.pop_back();
                }
                self.extreme.push_back((pos, argval.clone()));
                return Ok(());
            }
            _ => {}
        }
        self.nonnull += 1;
        match self.op {
            AggOp::Count => {}
            AggOp::Sum => {
                let next = match self.sum.take() {
                    None => add_values(Value::Int(0), argval.clone())?,
                    Some(s) => add_values(s, argval.clone())?,
                };
                self.sum = Some(next);
            }
            AggOp::Avg => match argval {
                Value::Decimal(d) => {
                    if !self.avg_dec_mode {
                        self.avg_dec_mode = true;
                        self.avg_dec = Decimal::ZERO;
                        for v in std::mem::take(&mut self.avg_vals) {
                            self.avg_dec = self
                                .avg_dec
                                .checked_add(as_decimal(&v)?)
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
                        self.avg_vals.push_back(other.clone());
                        self.avg_f64 += match other {
                            Value::Int(i) => *i as f64,
                            Value::Float(f) => *f,
                            _ => return err("AVG of non-numeric"),
                        };
                    }
                }
            },
            AggOp::Min | AggOp::Max | AggOp::GroupConcat => {}
        }
        Ok(())
    }

    /// Retract the frame's low edge: pop every row left of sorted position
    /// `lo` and undo its contribution. `Min`/`Max` drop their deque fronts.
    pub(super) fn pop_to(&mut self, lo: usize) {
        while self.window.front().is_some_and(|(pos, _, _)| *pos < lo) {
            let (pos, kept, arg) = self.window.pop_front().expect("checked above");
            if matches!(self.op, AggOp::Min | AggOp::Max) {
                if self.extreme.front().is_some_and(|(p, _)| *p == pos) {
                    self.extreme.pop_front();
                }
                continue;
            }
            if kept {
                self.kept -= 1;
            }
            let Some(v) = arg else { continue };
            if matches!(v, Value::Null) {
                continue;
            }
            self.nonnull -= 1;
            match self.op {
                AggOp::Count => {}
                AggOp::Sum => match self.sum.take() {
                    // A sticky-NULL (Int overflow) or absent sum cannot be
                    // inverted — the caller refolds from the slice.
                    None | Some(Value::Null) => {
                        self.degraded = true;
                        self.sum = Some(Value::Null);
                    }
                    Some(s) => match sub_values(s, v.clone()) {
                        Ok(next) => {
                            if matches!(next, Value::Null) {
                                self.degraded = true;
                            }
                            self.sum = Some(next);
                        }
                        Err(_) => self.degraded = true,
                    },
                },
                AggOp::Avg => match v {
                    Value::Decimal(d) => {
                        // A Decimal push flips the mode permanently; undoing
                        // one before any flip means state is inconsistent.
                        if !self.avg_dec_mode {
                            self.degraded = true;
                        } else {
                            match self.avg_dec.checked_sub(d) {
                                Some(nd) => self.avg_dec = nd,
                                None => self.degraded = true,
                            }
                        }
                    }
                    other => {
                        if self.avg_dec_mode {
                            match as_decimal(&other)
                                .ok()
                                .and_then(|d| self.avg_dec.checked_sub(d))
                            {
                                Some(nd) => self.avg_dec = nd,
                                None => self.degraded = true,
                            }
                        } else {
                            match self.avg_vals.pop_front() {
                                Some(front) => {
                                    let f = match front {
                                        Value::Int(i) => i as f64,
                                        Value::Float(f) => f,
                                        _ => {
                                            self.degraded = true;
                                            0.0
                                        }
                                    };
                                    self.avg_f64 -= f;
                                }
                                None => self.degraded = true,
                            }
                        }
                    }
                },
                AggOp::Min | AggOp::Max | AggOp::GroupConcat => {}
            }
        }
    }

    /// Aggregate value over the current frame — mirrors
    /// [`RunningAgg::snapshot`] field for field.
    pub(super) fn value(&self) -> Value {
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
                    self.avg_dec
                        .checked_div(Decimal::from(self.nonnull))
                        .map(Value::Decimal)
                        .unwrap_or(Value::Null)
                } else {
                    Value::Float(self.avg_f64 / self.nonnull as f64)
                }
            }
            AggOp::Min | AggOp::Max => self
                .extreme
                .front()
                .map(|(_, v)| v.clone())
                .unwrap_or(Value::Null),
            AggOp::GroupConcat => Value::Null,
        }
    }
}

/// Declared column names of a real-table table factor (views and CTEs have
/// no meta columns — their rows carry the keys already).
pub(super) fn collect_factor_columns(
    factor: &sqlparser::ast::TableFactor,
    catalog: &std::collections::BTreeMap<String, std::sync::Arc<TableMeta>>,
    out: &mut std::collections::HashSet<String>,
) {
    if let sqlparser::ast::TableFactor::Table { name, .. } = factor {
        let tname = obj_name(name);
        if let Some(meta) = catalog.get(&tname) {
            if !meta.is_view() {
                out.extend(meta.columns.iter().cloned());
            }
        }
    }
}

/// `SELECT *` column metadata for an EMPTY rowset: declared columns of one
/// FROM factor, qualified as the merged rows would be (`key` = bare column
/// for a solo table, "alias.col" / "table.col" once rows get joined — the
/// exact shape `load_from` produces). Views contribute nothing (their shape
/// belongs to the stored query, not the catalog).
pub(super) fn collect_star_columns(
    factor: &sqlparser::ast::TableFactor,
    catalog: &std::collections::BTreeMap<String, std::sync::Arc<TableMeta>>,
    out: &mut Vec<String>,
    qualify: bool,
) {
    if let sqlparser::ast::TableFactor::Table { name, alias, .. } = factor {
        let tname = obj_name(name);
        let Some(meta) = catalog.get(&tname) else {
            return;
        };
        if meta.is_view() {
            return;
        }
        for c in &meta.columns {
            if qualify {
                let key = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| tname.clone());
                out.push(format!("{key}.{c}"));
            } else {
                out.push(c.clone());
            }
        }
    }
}

/// Refuse unknown columns inside window PARTITION BY / ORDER BY / arguments:
/// `eval_expr` resolves a missing column to NULL, which silently collapses
/// the whole window computation (see compute_windows). Resolution mirrors
/// `lookup_col` (exact key or joined "alias.col" leaf). Walks the shapes
/// `rewrite_windows` knows; anything else keeps the legacy NULL behavior
/// rather than risking a false reject.
pub(super) fn check_window_columns(
    e: &SqlExpr,
    cols: &std::collections::HashSet<String>,
) -> Result<()> {
    let resolvable = |leaf: &str| -> bool {
        if cols.contains(leaf) {
            return true;
        }
        let suffix = format!(".{leaf}");
        cols.iter().any(|k| k.ends_with(&suffix))
    };
    match e {
        // `__count__` is the COUNT(*) sentinel (never a real column); T-SQL
        // `@var` names get their own loud refusal inside eval_expr.
        SqlExpr::Identifier(i) if i.value != "__count__" && !i.value.starts_with('@') => {
            if resolvable(&i.value) {
                Ok(())
            } else {
                err(format!("column {} does not exist", i.value))
            }
        }
        SqlExpr::CompoundIdentifier(parts) => {
            let leaf = parts.last().map(|p| p.value.as_str()).unwrap_or("");
            if resolvable(leaf) {
                Ok(())
            } else {
                err(format!("column {leaf} does not exist"))
            }
        }
        SqlExpr::Nested(inner) => check_window_columns(inner, cols),
        SqlExpr::BinaryOp { left, right, .. } => {
            check_window_columns(left, cols)?;
            check_window_columns(right, cols)
        }
        SqlExpr::UnaryOp { expr, .. } => check_window_columns(expr, cols),
        SqlExpr::IsNull(inner)
        | SqlExpr::IsNotNull(inner)
        | SqlExpr::IsTrue(inner)
        | SqlExpr::IsNotTrue(inner)
        | SqlExpr::IsFalse(inner)
        | SqlExpr::IsNotFalse(inner)
        | SqlExpr::IsUnknown(inner)
        | SqlExpr::IsNotUnknown(inner) => check_window_columns(inner, cols),
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            check_window_columns(expr, cols)?;
            check_window_columns(low, cols)?;
            check_window_columns(high, cols)
        }
        SqlExpr::IsDistinctFrom(l, r) | SqlExpr::IsNotDistinctFrom(l, r) => {
            check_window_columns(l, cols)?;
            check_window_columns(r, cols)
        }
        SqlExpr::Like { expr, pattern, .. } | SqlExpr::ILike { expr, pattern, .. } => {
            check_window_columns(expr, cols)?;
            check_window_columns(pattern, cols)
        }
        SqlExpr::InList { expr, list, .. } => {
            check_window_columns(expr, cols)?;
            for item in list {
                check_window_columns(item, cols)?;
            }
            Ok(())
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                check_window_columns(op, cols)?;
            }
            for w in conditions {
                check_window_columns(&w.condition, cols)?;
                check_window_columns(&w.result, cols)?;
            }
            if let Some(el) = else_result {
                check_window_columns(el, cols)?;
            }
            Ok(())
        }
        SqlExpr::Cast { expr, .. } => check_window_columns(expr, cols),
        SqlExpr::Function(f) => {
            // Mirror `eval_fn_arg`: the datepart family's FIRST argument is
            // a bare datepart keyword (`DATEDIFF(day, a, b)`), never a
            // column reference — validating it as a column would reject
            // every windowed DATEDIFF/DATEADD.
            let fname = f.name.to_string().to_uppercase();
            let skip_first = takes_datepart_name(&fname);
            let mut first_unnamed = true;
            if let sqlparser::ast::FunctionArguments::List(list) = &f.args {
                for a in &list.args {
                    if let sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(inner),
                    ) = a
                    {
                        let skip = skip_first && first_unnamed;
                        first_unnamed = false;
                        if !skip {
                            check_window_columns(inner, cols)?;
                        }
                    }
                }
            }
            if let Some(flt) = &f.filter {
                check_window_columns(flt, cols)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Window functions in one SELECT's projection, first-appearance order.
/// Window calls inside a single expression (QUALIFY predicate): same
/// dedupe-by-rendered-key as [`window_calls_of`], so an identical call in
/// the projection and QUALIFY shares one computed column.
pub(super) fn window_calls_of_expr(e: &SqlExpr) -> Vec<Function> {
    let mut out: Vec<Function> = Vec::new();
    let mut seen: Vec<String> = Vec::new();
    scan_windows(e, &mut out, &mut seen);
    out
}

pub(super) fn window_calls_of(items: &[SelectItem]) -> Vec<Function> {
    let mut out: Vec<Function> = Vec::new();
    let mut seen: Vec<String> = Vec::new();
    for item in items {
        let e = match item {
            SelectItem::UnnamedExpr(e) => e,
            SelectItem::ExprWithAlias { expr, .. } => expr,
            _ => continue,
        };
        scan_windows(e, &mut out, &mut seen);
    }
    out
}

/// Mirrors `subst_expr`'s recursion: descend into the composite expression
/// shapes the executor evaluates, so a window call wrapped in arithmetic /
/// CASE / COALESCE is still found. Subquery-shaped nodes are never descended
/// into — `subst_expr` has already rewritten them, and anything surviving is
/// a loud evaluation error, not a silently dropped window call.
pub(super) fn scan_windows(e: &SqlExpr, out: &mut Vec<Function>, seen: &mut Vec<String>) {
    match e {
        SqlExpr::Function(f) => {
            if f.over.is_some() {
                let key = format!("{f}");
                if !seen.contains(&key) {
                    seen.push(key);
                    out.push(f.clone());
                }
            }
            if let sqlparser::ast::FunctionArguments::List(list) = &f.args {
                for a in &list.args {
                    if let sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(inner),
                    ) = a
                    {
                        scan_windows(inner, out, seen);
                    }
                }
            }
            if let Some(filter) = &f.filter {
                scan_windows(filter, out, seen);
            }
        }
        SqlExpr::Nested(inner) => scan_windows(inner, out, seen),
        SqlExpr::BinaryOp { left, right, .. } => {
            scan_windows(left, out, seen);
            scan_windows(right, out, seen);
        }
        SqlExpr::UnaryOp { expr, .. } => scan_windows(expr, out, seen),
        SqlExpr::IsNull(inner)
        | SqlExpr::IsNotNull(inner)
        | SqlExpr::IsTrue(inner)
        | SqlExpr::IsNotTrue(inner)
        | SqlExpr::IsFalse(inner)
        | SqlExpr::IsNotFalse(inner)
        | SqlExpr::IsUnknown(inner)
        | SqlExpr::IsNotUnknown(inner) => scan_windows(inner, out, seen),
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            scan_windows(expr, out, seen);
            scan_windows(low, out, seen);
            scan_windows(high, out, seen);
        }
        SqlExpr::IsDistinctFrom(l, r) | SqlExpr::IsNotDistinctFrom(l, r) => {
            scan_windows(l, out, seen);
            scan_windows(r, out, seen);
        }
        SqlExpr::Like { expr, pattern, .. } | SqlExpr::ILike { expr, pattern, .. } => {
            scan_windows(expr, out, seen);
            scan_windows(pattern, out, seen);
        }
        SqlExpr::InList { expr, list, .. } => {
            scan_windows(expr, out, seen);
            for item in list {
                scan_windows(item, out, seen);
            }
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                scan_windows(op, out, seen);
            }
            for w in conditions {
                scan_windows(&w.condition, out, seen);
                scan_windows(&w.result, out, seen);
            }
            if let Some(el) = else_result {
                scan_windows(el, out, seen);
            }
        }
        SqlExpr::Cast { expr, .. } => scan_windows(expr, out, seen),
        _ => {}
    }
}

/// Substitute SELECT-list aliases inside a QUALIFY predicate (in place).
///
/// `… ROW_NUMBER() OVER (…) AS rn … QUALIFY rn = 1` used to resolve the bare
/// `rn` as a source column, find nothing (window results live only in the
/// private `__win*` columns) and evaluate the whole predicate over NULLs —
/// silently filtering out every row or, with the engine's total-order NULL
/// comparisons, keeping all of them. Aliases are the documented shape on
/// DuckDB/Snowflake, so a bare identifier naming a projection alias is
/// replaced by the aliased expression; the (now identical) window call then
/// collapses onto the projection's shared computed column via
/// [`rewrite_windows`].
///
/// Source columns win: a name that is a declared FROM column or appears as a
/// bare row key is never substituted (alias shadowing stays illegal, column
/// semantics hold). Chained aliases (`a → b → expr`) resolve via bounded
/// re-passes; a self-referential cycle just stops at the cap.
pub(super) fn substitute_qualify_aliases(
    e: &mut SqlExpr,
    project: &[(String, SqlExpr)],
    from_cols: &std::collections::HashSet<String>,
    docs: &[Object],
) {
    let aliases: Vec<(String, SqlExpr)> = project
        .iter()
        .filter(|(n, _)| !from_cols.contains(n))
        .filter(|(n, _)| !docs.iter().any(|d| d.contains_key(n)))
        .map(|(n, e)| (n.clone(), e.clone()))
        .collect();
    if aliases.is_empty() {
        return;
    }
    for _ in 0..8 {
        let mut changed = false;
        subst_aliases(e, &aliases, &mut changed);
        if !changed {
            break;
        }
    }
}

/// One in-place substitution pass over the same expression shapes
/// [`rewrite_windows`] covers; subqueries keep their own scope (aliases are
/// a SELECT-list surface, not a scope-wide macro).
fn subst_aliases(e: &mut SqlExpr, aliases: &[(String, SqlExpr)], changed: &mut bool) {
    match e {
        SqlExpr::Identifier(i) => {
            if let Some((_, rep)) = aliases.iter().find(|(n, _)| *n == i.value) {
                *e = rep.clone();
                *changed = true;
            }
        }
        SqlExpr::Function(f) => {
            if let sqlparser::ast::FunctionArguments::List(list) = &mut f.args {
                for a in &mut list.args {
                    if let sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(inner),
                    ) = a
                    {
                        subst_aliases(inner, aliases, changed);
                    }
                }
            }
            if let Some(filter) = &mut f.filter {
                subst_aliases(filter, aliases, changed);
            }
        }
        SqlExpr::Nested(inner) => subst_aliases(inner, aliases, changed),
        SqlExpr::BinaryOp { left, right, .. } => {
            subst_aliases(left, aliases, changed);
            subst_aliases(right, aliases, changed);
        }
        SqlExpr::UnaryOp { expr, .. } => subst_aliases(expr, aliases, changed),
        SqlExpr::IsNull(inner)
        | SqlExpr::IsNotNull(inner)
        | SqlExpr::IsTrue(inner)
        | SqlExpr::IsNotTrue(inner)
        | SqlExpr::IsFalse(inner)
        | SqlExpr::IsNotFalse(inner)
        | SqlExpr::IsUnknown(inner)
        | SqlExpr::IsNotUnknown(inner) => subst_aliases(inner, aliases, changed),
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            subst_aliases(expr, aliases, changed);
            subst_aliases(low, aliases, changed);
            subst_aliases(high, aliases, changed);
        }
        SqlExpr::IsDistinctFrom(l, r) | SqlExpr::IsNotDistinctFrom(l, r) => {
            subst_aliases(l, aliases, changed);
            subst_aliases(r, aliases, changed);
        }
        SqlExpr::Like { expr, pattern, .. } | SqlExpr::ILike { expr, pattern, .. } => {
            subst_aliases(expr, aliases, changed);
            subst_aliases(pattern, aliases, changed);
        }
        SqlExpr::InList { expr, list, .. } => {
            subst_aliases(expr, aliases, changed);
            for item in list {
                subst_aliases(item, aliases, changed);
            }
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                subst_aliases(op, aliases, changed);
            }
            for w in conditions {
                subst_aliases(&mut w.condition, aliases, changed);
                subst_aliases(&mut w.result, aliases, changed);
            }
            if let Some(el) = else_result {
                subst_aliases(el, aliases, changed);
            }
        }
        SqlExpr::Cast { expr, .. } => subst_aliases(expr, aliases, changed),
        _ => {}
    }
}

/// Replace every window call with a reference to its computed column; the
/// dedup key (full text) matches [`scan_windows`], so indices agree.
pub(super) fn rewrite_windows(e: &mut SqlExpr, calls: &[Function], names: &[String]) {
    match e {
        SqlExpr::Function(f) if f.over.is_some() => {
            let key = format!("{f}");
            if let Some(i) = calls.iter().position(|c| format!("{c}") == key) {
                *e = SqlExpr::Identifier(sqlparser::ast::Ident::new(
                    names.get(i).cloned().unwrap_or_else(|| window_tmp_col(i)),
                ));
            }
        }
        SqlExpr::Function(f) => {
            if let sqlparser::ast::FunctionArguments::List(list) = &mut f.args {
                for a in &mut list.args {
                    if let sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(inner),
                    ) = a
                    {
                        rewrite_windows(inner, calls, names);
                    }
                }
            }
            if let Some(filter) = &mut f.filter {
                rewrite_windows(filter, calls, names);
            }
        }
        SqlExpr::Nested(inner) => rewrite_windows(inner, calls, names),
        SqlExpr::BinaryOp { left, right, .. } => {
            rewrite_windows(left, calls, names);
            rewrite_windows(right, calls, names);
        }
        SqlExpr::UnaryOp { expr, .. } => rewrite_windows(expr, calls, names),
        SqlExpr::IsNull(inner)
        | SqlExpr::IsNotNull(inner)
        | SqlExpr::IsTrue(inner)
        | SqlExpr::IsNotTrue(inner)
        | SqlExpr::IsFalse(inner)
        | SqlExpr::IsNotFalse(inner)
        | SqlExpr::IsUnknown(inner)
        | SqlExpr::IsNotUnknown(inner) => rewrite_windows(inner, calls, names),
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            rewrite_windows(expr, calls, names);
            rewrite_windows(low, calls, names);
            rewrite_windows(high, calls, names);
        }
        SqlExpr::IsDistinctFrom(l, r) | SqlExpr::IsNotDistinctFrom(l, r) => {
            rewrite_windows(l, calls, names);
            rewrite_windows(r, calls, names);
        }
        SqlExpr::Like { expr, pattern, .. } | SqlExpr::ILike { expr, pattern, .. } => {
            rewrite_windows(expr, calls, names);
            rewrite_windows(pattern, calls, names);
        }
        SqlExpr::InList { expr, list, .. } => {
            rewrite_windows(expr, calls, names);
            for item in list {
                rewrite_windows(item, calls, names);
            }
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                rewrite_windows(op, calls, names);
            }
            for w in conditions {
                rewrite_windows(&mut w.condition, calls, names);
                rewrite_windows(&mut w.result, calls, names);
            }
            if let Some(el) = else_result {
                rewrite_windows(el, calls, names);
            }
        }
        SqlExpr::Cast { expr, .. } => rewrite_windows(expr, calls, names),
        _ => {}
    }
}

/// Unnamed-argument exprs of a function call (`LAG(v, 1, 0)` → `[v, 1, 0]`).
pub(super) fn fn_args(f: &Function) -> Vec<SqlExpr> {
    match &f.args {
        sqlparser::ast::FunctionArguments::List(list) => list
            .args
            .iter()
            .filter_map(|a| match a {
                sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(
                    inner,
                )) => Some(inner.clone()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// Supported window specification shape: no named window, no frame beyond
/// the default, no IGNORE NULLS. Refusals stay explicit — an unsupported
/// frame would silently change the result shape (one row per partition vs
/// one value per row).
pub(super) fn validate_window_spec<'a>(
    f: &'a Function,
    fname: &str,
    aggregate: bool,
) -> Result<&'a sqlparser::ast::WindowSpec> {
    // FILTER is meaningful only for aggregate window functions (it rides the
    // frame); for the scalar family it is not defined (Postgres/SQLite both
    // refuse) and silently ignoring it would return unfiltered numbers.
    if f.filter.is_some() && !aggregate {
        return err(format!(
            "FILTER is not supported for non-aggregate window functions: {fname}"
        ));
    }
    let spec = match &f.over {
        Some(sqlparser::ast::WindowType::WindowSpec(s)) => s,
        Some(sqlparser::ast::WindowType::NamedWindow(n)) => {
            return err(format!(
                "named window \"{}\" requires a WINDOW clause, which is not supported; \
                 write the OVER (...) inline",
                n.value
            ));
        }
        None => return err("window call without OVER"),
    };
    if let Some(n) = &spec.window_name {
        // `OVER (name)` references a WINDOW clause definition, which the
        // engine refuses; anything else would silently ignore the name.
        return err(format!(
            "named window \"{}\" requires a WINDOW clause, which is not supported; \
             write the OVER (...) inline",
            n.value
        ));
    }
    // Frames are accepted (SQL:2003 ROWS/RANGE/GROUPS BETWEEN … AND …);
    // the structural rules and offset shapes are checked by
    // [`parse_window_frame`] in `compute_windows`.
    if matches!(
        f.null_treatment,
        Some(sqlparser::ast::NullTreatment::IgnoreNulls)
    ) {
        return err(format!("IGNORE NULLS is not supported: {fname}"));
    }
    Ok(spec)
}

/// Parsed custom window frame ([`parse_window_frame`]): bounds are checked
/// and offsets const-evaluated once per window call, not per partition.
#[derive(Clone, Debug)]
pub(super) enum WindowFramePlan {
    /// `ROWS BETWEEN … AND …`: bounds are row counts within the (sorted)
    /// partition.
    Rows {
        start: FrameRowBound,
        end: FrameRowBound,
    },
    /// `GROUPS BETWEEN … AND …`: bounds count PEER GROUPS, mapped back to
    /// row spans through the partition's run layout.
    Groups {
        start: FrameRowBound,
        end: FrameRowBound,
    },
    /// `RANGE BETWEEN … AND …`: value bounds are arithmetic shifts of the
    /// single ORDER BY key.
    Range {
        start: FrameValueBound,
        end: FrameValueBound,
    },
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum FrameRowBound {
    Unbounded,
    Preceding(u64),
    CurrentRow,
    Following(u64),
    UnboundedFollowing,
}

/// One RANGE offset: the constant value plus whether the source spelling
/// was `INTERVAL 'n' UNIT` — an interval offset shifts only a TIMESTAMP
/// key (by the converted milliseconds), never a numeric one.
#[derive(Clone, Debug)]
pub(super) struct FrameRangeOffset {
    off: Value,
    interval: bool,
}

#[derive(Clone, Debug)]
pub(super) enum FrameValueBound {
    Unbounded,
    Preceding(FrameRangeOffset),
    CurrentRow,
    Following(FrameRangeOffset),
    UnboundedFollowing,
}

/// Rank bounds in frame order (a start must not come after its end).
pub(super) fn frame_row_rank(b: &FrameRowBound) -> u8 {
    match b {
        FrameRowBound::Unbounded => 0,
        FrameRowBound::Preceding(_) => 1,
        FrameRowBound::CurrentRow => 2,
        FrameRowBound::Following(_) => 3,
        FrameRowBound::UnboundedFollowing => 4,
    }
}

pub(super) fn frame_value_rank(b: &FrameValueBound) -> u8 {
    match b {
        FrameValueBound::Unbounded => 0,
        FrameValueBound::Preceding(_) => 1,
        FrameValueBound::CurrentRow => 2,
        FrameValueBound::Following(_) => 3,
        FrameValueBound::UnboundedFollowing => 4,
    }
}

/// Validate structure and const-eval offsets. `order_len` is the ORDER BY
/// key count — RANGE value bounds shift THE sort key, so they demand
/// exactly one (SQL standard).
pub(super) fn parse_window_frame(
    frame: &sqlparser::ast::WindowFrame,
    order_len: usize,
) -> Result<WindowFramePlan> {
    // The bare `ROWS 3 PRECEDING` spelling means `BETWEEN … AND CURRENT ROW`.
    let end_default = frame
        .end_bound
        .clone()
        .unwrap_or(sqlparser::ast::WindowFrameBound::CurrentRow);
    let row_bound = |b: &sqlparser::ast::WindowFrameBound| -> Result<FrameRowBound> {
        Ok(match b {
            sqlparser::ast::WindowFrameBound::Preceding(None) => FrameRowBound::Unbounded,
            sqlparser::ast::WindowFrameBound::Preceding(Some(e)) => {
                FrameRowBound::Preceding(row_offset(e)?)
            }
            sqlparser::ast::WindowFrameBound::CurrentRow => FrameRowBound::CurrentRow,
            sqlparser::ast::WindowFrameBound::Following(Some(e)) => {
                FrameRowBound::Following(row_offset(e)?)
            }
            sqlparser::ast::WindowFrameBound::Following(None) => FrameRowBound::UnboundedFollowing,
        })
    };
    Ok(match frame.units {
        sqlparser::ast::WindowFrameUnits::Rows => {
            let (start, end) = (row_bound(&frame.start_bound)?, row_bound(&end_default)?);
            if frame_row_rank(&start) > frame_row_rank(&end)
                || match (start, end) {
                    (FrameRowBound::Preceding(a), FrameRowBound::Preceding(b)) => a < b,
                    (FrameRowBound::Following(a), FrameRowBound::Following(b)) => a > b,
                    _ => false,
                }
            {
                return err("window frame start cannot be after its end");
            }
            WindowFramePlan::Rows { start, end }
        }
        sqlparser::ast::WindowFrameUnits::Groups => {
            let (start, end) = (row_bound(&frame.start_bound)?, row_bound(&end_default)?);
            if frame_row_rank(&start) > frame_row_rank(&end)
                || match (start, end) {
                    (FrameRowBound::Preceding(a), FrameRowBound::Preceding(b)) => a < b,
                    (FrameRowBound::Following(a), FrameRowBound::Following(b)) => a > b,
                    _ => false,
                }
            {
                return err("window frame start cannot be after its end");
            }
            WindowFramePlan::Groups { start, end }
        }
        sqlparser::ast::WindowFrameUnits::Range => {
            let value_bound = |b: &sqlparser::ast::WindowFrameBound| -> Result<FrameValueBound> {
                Ok(match b {
                    sqlparser::ast::WindowFrameBound::Preceding(None) => FrameValueBound::Unbounded,
                    sqlparser::ast::WindowFrameBound::Preceding(Some(e)) => {
                        FrameValueBound::Preceding(range_offset(e)?)
                    }
                    sqlparser::ast::WindowFrameBound::CurrentRow => FrameValueBound::CurrentRow,
                    sqlparser::ast::WindowFrameBound::Following(Some(e)) => {
                        FrameValueBound::Following(range_offset(e)?)
                    }
                    sqlparser::ast::WindowFrameBound::Following(None) => {
                        FrameValueBound::UnboundedFollowing
                    }
                })
            };
            let (start, end) = (value_bound(&frame.start_bound)?, value_bound(&end_default)?);
            // Structural order: UNBOUNDED PRECEDING < n PRECEDING < CURRENT
            // ROW < n FOLLOWING < UNBOUNDED FOLLOWING, with equal-shape
            // offset comparisons (`5 PRECEDING` to `2 PRECEDING` is legal,
            // the reverse frames nothing and the standard refuses it).
            let misplaced = match (&start, &end) {
                (FrameValueBound::Preceding(a), FrameValueBound::Preceding(b)) => {
                    Value::cmp_values(&a.off, &b.off) == Ordering::Less
                }
                (FrameValueBound::Following(a), FrameValueBound::Following(b)) => {
                    Value::cmp_values(&a.off, &b.off) == Ordering::Greater
                }
                _ => false,
            };
            if frame_value_rank(&start) > frame_value_rank(&end) || misplaced {
                return err("window frame start cannot be after its end");
            }
            // A value offset shifts the single sort key; without exactly one
            // ORDER BY key there is nothing to shift. UNBOUNDED / CURRENT ROW
            // spellings stay legal unordered (they degenerate to the whole
            // partition / the peer set, which unordered makes everything).
            let has_offset = |b: &FrameValueBound| {
                matches!(
                    b,
                    FrameValueBound::Preceding(_) | FrameValueBound::Following(_)
                )
            };
            if (has_offset(&start) || has_offset(&end)) && order_len != 1 {
                return err(
                    "RANGE with a value offset requires exactly one ORDER BY key                      (the frame shifts THE sort key)",
                );
            }
            WindowFramePlan::Range { start, end }
        }
    })
}

/// ROWS/GROUPS offsets: non-negative integer constants.
pub(super) fn row_offset(e: &SqlExpr) -> Result<u64> {
    match eval_const(e)? {
        Value::Int(k) if k >= 0 => Ok(k as u64),
        other => err(format!(
            "ROWS/GROUPS frame offsets must be non-negative integers, got {}",
            other.type_name()
        )),
    }
}

/// RANGE offsets: non-negative numeric constants (Int/Float/Decimal).
/// INTERVAL spellings are rejected — TIMESTAMP keys shift by integer
/// milliseconds, mirroring the engine's TIMESTAMP ± INTEGER arithmetic.
pub(super) fn range_offset(e: &SqlExpr) -> Result<FrameRangeOffset> {
    if let SqlExpr::Interval(iv) = e {
        let unit = match iv.leading_field {
            Some(sqlparser::ast::DateTimeField::Day) => 86_400_000i64,
            Some(sqlparser::ast::DateTimeField::Hour) => 3_600_000,
            Some(sqlparser::ast::DateTimeField::Minute) => 60_000,
            Some(sqlparser::ast::DateTimeField::Second) => 1_000,
            _ => {
                return err(
                    "INTERVAL frame offsets support DAY/HOUR/MINUTE/SECOND only \
                     (YEAR/MONTH calendar arithmetic is not supported)",
                );
            }
        };
        if iv.last_field.is_some() || iv.leading_field.is_none() {
            return err(
                "INTERVAL frame offsets take a single unit: INTERVAL 'n' DAY/HOUR/MINUTE/SECOND",
            );
        }
        let SqlExpr::Value(v) = iv.value.as_ref() else {
            return err("INTERVAL frame offset value must be a string literal");
        };
        let sqlparser::ast::Value::SingleQuotedString(text) = &v.value else {
            return err("INTERVAL frame offset value must be a string literal");
        };
        let secs: f64 = text
            .trim()
            .parse()
            .map_err(|_| SqlError::Message(format!("invalid INTERVAL value '{text}'")))?;
        if !secs.is_finite() || secs < 0.0 {
            return err("INTERVAL frame offsets must be non-negative");
        }
        let ms = (secs * unit as f64) as i64;
        if ms == 0 && secs > 0.0 {
            return err("INTERVAL frame offset is smaller than one millisecond");
        }
        return Ok(FrameRangeOffset {
            off: Value::Int(ms),
            interval: true,
        });
    }
    match eval_const(e)? {
        v @ (Value::Int(_) | Value::Float(_) | Value::Decimal(_)) => {
            if Value::cmp_values(&v, &Value::Int(0)) == Ordering::Less {
                return err("RANGE frame offsets must be non-negative");
            }
            Ok(FrameRangeOffset {
                off: v,
                interval: false,
            })
        }
        other => err(format!(
            "RANGE frame offsets must be numeric constants, got {}",
            other.type_name()
        )),
    }
}

pub(super) fn frame_offset_f64(off: &Value) -> Result<f64> {
    match off {
        Value::Int(k) => Ok(*k as f64),
        Value::Float(f) => Ok(*f),
        Value::Decimal(d) => Ok(d.to_f64().unwrap_or(f64::NAN)),
        _ => err("RANGE frame offset must be numeric"),
    }
}

/// Shift a RANGE frame endpoint: `key ± off`, direction-aware, mirroring the
/// engine's arithmetic (Decimal exact, Int checked, TIMESTAMP ± integer ms
/// with the 0001..=9999 domain). `None` = the endpoint fell outside the
/// value domain (Int/Timestamp overflow) — the interval is unbounded on
/// that side, never a silent truncation.
pub(super) fn shift_range_key(
    key: &Value,
    off: &FrameRangeOffset,
    up: bool,
) -> Result<Option<Value>> {
    // An INTERVAL spelling shifts only a TIMESTAMP key: DAY/HOUR/… are time
    // units, and applying them to a number would silently reinterpret them.
    if off.interval && !matches!(key, Value::Timestamp(_) | Value::Null) {
        return err("INTERVAL frame offsets require a TIMESTAMP ORDER BY key \
             (numeric keys take a plain constant offset)");
    }
    shift_range_key_value(key, &off.off, up)
}

pub(super) fn shift_range_key_value(key: &Value, off: &Value, up: bool) -> Result<Option<Value>> {
    match (key, off) {
        (Value::Null, _) => Ok(None),
        (Value::Int(k), Value::Int(o)) => {
            let v = if up {
                k.checked_add(*o)
            } else {
                k.checked_sub(*o)
            };
            Ok(v.map(Value::Int))
        }
        (Value::Float(k), _) => {
            let f = frame_offset_f64(off)?;
            Ok(Some(Value::Float(if up { k + f } else { k - f })))
        }
        (Value::Int(_), Value::Float(_)) => {
            let f = frame_offset_f64(off)?;
            let k = match key {
                Value::Int(k) => *k as f64,
                _ => unreachable!("matched above"),
            };
            Ok(Some(Value::Float(if up { k + f } else { k - f })))
        }
        (Value::Decimal(_), _) | (Value::Int(_), Value::Decimal(_)) => {
            let (a, b) = (as_decimal(key)?, as_decimal(off)?);
            let v = if up {
                a.checked_add(b)
            } else {
                a.checked_sub(b)
            };
            Ok(v.map(Value::Decimal))
        }
        (Value::Timestamp(t), Value::Int(o)) => {
            let v = if up {
                t.checked_add(*o)
            } else {
                t.checked_sub(*o)
            };
            // Outside 0001..=9999 there is no canonical instant: the bound
            // covers everything up to the domain edge.
            Ok(v.filter(|ms| crate::value::is_valid_timestamp_ms(*ms))
                .map(Value::Timestamp))
        }
        _ => err("RANGE value offsets need a numeric or TIMESTAMP ORDER BY key"),
    }
}

/// Inverse of [`add_values`] for sliding-window SUM removal: the same type
/// rules mirrored (Decimal exact, Int checked, Float binary). `None` where
/// the inverse is unrecoverable (Int overflow — the batch fold NULLs there
/// too), so the caller can fall back to a fresh fold of the frame.
pub(super) fn sub_values(a: Value, b: Value) -> Result<Value> {
    if matches!(a, Value::Null) || matches!(b, Value::Null) {
        return Ok(Value::Null);
    }
    if matches!(a, Value::Decimal(_)) || matches!(b, Value::Decimal(_)) {
        let (x, y) = (as_decimal(&a)?, as_decimal(&b)?);
        return Ok(x.checked_sub(y).map(Value::Decimal).unwrap_or(Value::Null));
    }
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => {
            Ok(x.checked_sub(y).map(Value::Int).unwrap_or(Value::Null))
        }
        (Value::Int(x), Value::Float(y)) => Ok(Value::Float(x as f64 - y)),
        (Value::Float(x), Value::Int(y)) => Ok(Value::Float(x - y as f64)),
        (Value::Float(x), Value::Float(y)) => Ok(Value::Float(x - y)),
        _ => err("SUM of non-numeric values"),
    }
}

/// First partition position whose ORDER BY key is at-or-after `target` in
/// sort order (`upper = false`), or strictly after it (`upper = true`). The
/// partition's keys are monotone under the sort comparator, so a plain
/// binary search applies; direction (ASC/DESC) and NULL placement ride the
/// same [`cmp_maybe_null`] the sort used.
pub(super) fn window_key_search(
    keyed: &[(usize, Vec<Value>)],
    target: &Value,
    nulls_first: Option<bool>,
    asc: bool,
    upper: bool,
) -> usize {
    let pred = |v: &Value| {
        let o = cmp_maybe_null(v, target, nulls_first);
        if upper {
            if asc {
                o == Ordering::Greater
            } else {
                o == Ordering::Less
            }
        } else if asc {
            o != Ordering::Less
        } else {
            o != Ordering::Greater
        }
    };
    let (mut lo, mut hi) = (0usize, keyed.len());
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if pred(&keyed[mid].1[0]) {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    lo
}

/// Per-row frame bounds `[lo, hi]` (inclusive sorted positions) for one
/// partition. `group_spans`/`group_of` carry the peer-group layout for
/// GROUPS units. Monotone by construction — bounds only move forward as the
/// current row advances, which the sliding aggregators rely on.
pub(super) fn frame_bounds(
    plan: &WindowFramePlan,
    keyed: &[(usize, Vec<Value>)],
    order_by: &[sqlparser::ast::OrderByExpr],
    group_spans: &[(usize, usize)],
    group_of: &[usize],
    n: usize,
) -> Result<Vec<(usize, usize)>> {
    let clamp = |k: u64| (k as usize).min(n);
    Ok(match plan {
        WindowFramePlan::Rows { start, end } => (0..n)
            .map(|p| {
                // `lo` may legally exceed `n - 1` (an offset frame past the
                // partition end) — that is an EMPTY frame, aggregate-of-
                // nothing, never a clamp onto the last row.
                let lo = match start {
                    FrameRowBound::Unbounded => 0,
                    FrameRowBound::Preceding(k) => p.saturating_sub(clamp(*k)),
                    FrameRowBound::CurrentRow => p,
                    FrameRowBound::Following(k) => p + clamp(*k),
                    FrameRowBound::UnboundedFollowing => n,
                };
                let hi = match end {
                    FrameRowBound::Unbounded => 0,
                    FrameRowBound::Preceding(k) => p.saturating_sub(clamp(*k)),
                    FrameRowBound::CurrentRow => p,
                    FrameRowBound::Following(k) => (p + clamp(*k)).min(n - 1),
                    FrameRowBound::UnboundedFollowing => n - 1,
                };
                (lo, hi)
            })
            .collect(),
        WindowFramePlan::Groups { start, end } => {
            let groups = group_spans.len();
            (0..n)
                .map(|p| {
                    let g = group_of[p];
                    let g_lo = match start {
                        FrameRowBound::Unbounded => 0,
                        FrameRowBound::Preceding(k) => g.saturating_sub(*k as usize),
                        FrameRowBound::CurrentRow => g,
                        FrameRowBound::Following(k) => (g + *k as usize).min(groups - 1),
                        FrameRowBound::UnboundedFollowing => groups - 1,
                    };
                    let g_hi = match end {
                        FrameRowBound::Unbounded => 0,
                        FrameRowBound::Preceding(k) => g.saturating_sub(*k as usize),
                        FrameRowBound::CurrentRow => g,
                        FrameRowBound::Following(k) => (g + *k as usize).min(groups - 1),
                        FrameRowBound::UnboundedFollowing => groups - 1,
                    };
                    let lo = if matches!(start, FrameRowBound::Following(_)) {
                        // A start past the last group: empty frame.
                        group_spans.get(g_lo).map(|s| s.0).unwrap_or(n)
                    } else {
                        group_spans[g_lo].0
                    };
                    (lo, group_spans[g_hi].1)
                })
                .collect()
        }
        WindowFramePlan::Range { start, end } => {
            if order_by.is_empty() {
                // Unordered RANGE: every row is a peer of every other, so
                // any frame covers the whole partition.
                return Ok(vec![(0, n - 1); n]);
            }
            let nf = order_by[0].options.nulls_first;
            let asc = order_by[0].options.asc.unwrap_or(true);
            let mut out = Vec::with_capacity(n);
            for p in 0..n {
                let key = &keyed[p].1[0];
                if matches!(key, Value::Null) {
                    // NULL keys have no arithmetic: the frame is their peer
                    // run (NULLs equal only NULLs, SQL semantics).
                    let lo = window_key_search(keyed, key, nf, asc, false);
                    let hi = window_key_search(keyed, key, nf, asc, true).saturating_sub(1);
                    out.push((lo, hi));
                    continue;
                }
                // PRECEDING/FOLLOWING are SORT-ORDER relative: ASC `n
                // PRECEDING` = n value units DOWN, DESC `n PRECEDING` = n
                // value units UP. The value interval [vlo, vhi] reads the
                // START bound on one side and the END bound on the other,
                // per direction; the position search then targets the
                // matching endpoint (ASC keys ascend: lo hunts vlo, hi vhi;
                // DESC keys descend: lo hunts vhi, hi vlo).
                let bound_value = |b: &FrameValueBound| -> Result<Option<Value>> {
                    match b {
                        FrameValueBound::Unbounded | FrameValueBound::UnboundedFollowing => {
                            Ok(None)
                        }
                        FrameValueBound::CurrentRow => Ok(Some(key.clone())),
                        FrameValueBound::Preceding(v) => shift_range_key(key, v, !asc),
                        FrameValueBound::Following(v) => shift_range_key(key, v, asc),
                    }
                };
                let (lo_v, hi_v) = if asc {
                    (bound_value(start)?, bound_value(end)?)
                } else {
                    (bound_value(end)?, bound_value(start)?)
                };
                let (mut lo, mut hi) = if asc {
                    (
                        match lo_v {
                            None => 0,
                            Some(t) => window_key_search(keyed, &t, nf, true, false),
                        },
                        match hi_v {
                            None => n - 1,
                            Some(t) => window_key_search(keyed, &t, nf, true, true),
                        },
                    )
                } else {
                    (
                        match hi_v {
                            None => 0,
                            Some(t) => window_key_search(keyed, &t, nf, false, false),
                        },
                        match lo_v {
                            None => n - 1,
                            Some(t) => window_key_search(keyed, &t, nf, false, true),
                        },
                    )
                };
                // `hi` is the first-strictly-after search position; the
                // inclusive end is one before it. Zero means NO key reached
                // the far endpoint: an empty frame, which the consumers read
                // as `hi < lo` — drop `hi` to 0 and push `lo` past it.
                if hi == 0 {
                    hi = 0;
                    lo = lo.max(1);
                } else {
                    hi -= 1;
                }
                out.push((lo, hi));
            }
            out
        }
    })
}
