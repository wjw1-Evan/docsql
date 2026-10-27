use super::*;

/// Canonical join key for the hash-join path. Equality under
/// [`Value::cmp_values`] implies equality here, which is the only contract
/// the index needs: candidates re-evaluate the full ON, so the key may
/// over-merge (Int vs Int beyond f64's 2^53 exact range collapses to the
/// same bits, mirroring `cmp_values`' cross-type numeric branch) but must
/// never split a matching pair. NULL is a key like any other — `binop`'s `=`
/// is `cmp_values`-based, so NULL = NULL joins.
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Debug)]
pub(super) enum JoinKey {
    Null,
    Bool(bool),
    /// f64 bit pattern, NaN collapsed to one payload and ±0.0 to +0.0 so
    /// `cmp_values`-equal numerics hash equal.
    Num(u64),
    Str(String),
    Bytes(Vec<u8>),
    Array(Vec<JoinKey>),
}

pub(super) fn join_key_of(v: &Value) -> Option<JoinKey> {
    Some(match v {
        Value::Null => JoinKey::Null,
        Value::Bool(b) => JoinKey::Bool(*b),
        Value::Int(i) => JoinKey::Num(num_key(*i as f64)),
        Value::Float(f) => JoinKey::Num(num_key(*f)),
        // Decimal hashes through its nearest f64: over-merging only in the
        // ultra-wide-integer range, mirroring Int/Float (cmp_values then
        // terminates candidates exactly).
        Value::Decimal(d) => JoinKey::Num(num_key(d.to_f64()?)),
        Value::Timestamp(ms) => JoinKey::Num(num_key(*ms as f64)),
        Value::Str(s) => {
            // A parseable timestamp string joins against TIMESTAMP values —
            // binop coerces the pair before comparing — so both sides must
            // land in the same bucket or the hash path would drop a match
            // the nested loop finds. Keying parseable strings by their
            // instant only ever over-merges (string≠string pairs become
            // candidates the ON re-evaluation rejects); it never splits.
            match crate::value::parse_timestamp_ms(s) {
                Some(ms) => JoinKey::Num(num_key(ms as f64)),
                None => JoinKey::Str(s.clone()),
            }
        }
        Value::Bytes(b) => JoinKey::Bytes(b.clone()),
        Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for it in items {
                out.push(join_key_of(it)?);
            }
            JoinKey::Array(out)
        }
        // Documents as join keys have no practical shape; the join falls
        // back to the nested loop instead.
        Value::Object(_) => return None,
    })
}

pub(super) fn num_key(f: f64) -> u64 {
    if f.is_nan() {
        f64::NAN.to_bits()
    } else if f == 0.0 {
        0.0f64.to_bits()
    } else {
        f.to_bits()
    }
}

/// Flatten `e` into its AND-conjuncts (parenthesized groups unwrapped).
pub(super) fn and_conjuncts<'e>(e: &'e SqlExpr, out: &mut Vec<&'e SqlExpr>) {
    match e {
        SqlExpr::BinaryOp {
            left,
            op: sqlparser::ast::BinaryOperator::And,
            right,
        } => {
            and_conjuncts(left, out);
            and_conjuncts(right, out);
        }
        SqlExpr::Nested(inner) => and_conjuncts(inner, out),
        other => out.push(other),
    }
}

/// Column references inside `e`, as the dotted names the row-local evaluator
/// resolves (`Identifier("a.id")` and `CompoundIdentifier[a, id]` both yield
/// `"a.id"`). `None` = the tree contains something the collector does not
/// model (subqueries, exotic nodes) — the caller then leaves the conjunct in
/// the residual instead of indexing on it.
pub(super) fn column_refs(e: &SqlExpr, out: &mut Vec<String>) -> Option<()> {
    use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
    match e {
        SqlExpr::Identifier(i) => out.push(i.value.clone()),
        SqlExpr::CompoundIdentifier(parts) => {
            out.push(
                parts
                    .iter()
                    .map(|p| p.value.clone())
                    .collect::<Vec<_>>()
                    .join("."),
            );
        }
        SqlExpr::Value(_) => {}
        SqlExpr::UnaryOp { expr, .. } => column_refs(expr, out)?,
        SqlExpr::BinaryOp { left, right, .. } => {
            column_refs(left, out)?;
            column_refs(right, out)?;
        }
        SqlExpr::Nested(inner) => column_refs(inner, out)?,
        SqlExpr::IsNull(inner) | SqlExpr::IsNotNull(inner) => column_refs(inner, out)?,
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            column_refs(expr, out)?;
            column_refs(low, out)?;
            column_refs(high, out)?;
        }
        SqlExpr::Like { expr, pattern, .. } | SqlExpr::ILike { expr, pattern, .. } => {
            column_refs(expr, out)?;
            column_refs(pattern, out)?;
        }
        SqlExpr::InList { expr, list, .. } => {
            column_refs(expr, out)?;
            for item in list {
                column_refs(item, out)?;
            }
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(o) = operand {
                column_refs(o, out)?;
            }
            for w in conditions {
                column_refs(&w.condition, out)?;
                column_refs(&w.result, out)?;
            }
            if let Some(r) = else_result {
                column_refs(r, out)?;
            }
        }
        SqlExpr::Cast { expr, .. } => column_refs(expr, out)?,
        SqlExpr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            column_refs(expr, out)?;
            if let Some(f) = substring_from {
                column_refs(f, out)?;
            }
            if let Some(l) = substring_for {
                column_refs(l, out)?;
            }
        }
        SqlExpr::Function(f) => {
            match &f.args {
                FunctionArguments::List(list) => {
                    for a in &list.args {
                        match a {
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) => {
                                column_refs(inner, out)?
                            }
                            // `COUNT(*)` and friends reference no column; named or
                            // wildcard argument shapes stay unmodeled.
                            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {}
                            _ => return None,
                        }
                    }
                }
                FunctionArguments::None => {}
                _ => return None,
            }
            // Aggregate FILTER (WHERE …): the engine evaluates the predicate
            // per row (agg_parts/eval_agg), so its columns are read exactly
            // like a projection's — a column-restricted grant that skips
            // them leaks the filtered column's boolean information
            // (`COUNT(id) FILTER (WHERE secret = …)`). WITHIN GROUP's order
            // keys are unmodeled → fail closed like any unknown shape.
            if let Some(flt) = &f.filter {
                column_refs(flt, out)?;
            }
            if !f.within_group.is_empty() {
                return None;
            }
        }
        // Subqueries and anything unmodeled keep the conjunct in the residual.
        _ => return None,
    }
    Some(())
}

/// Which side of the join boundary an expression's column refs touch.
pub(super) enum Side {
    Left,
    Right,
    /// Refs from both sides — never indexable, the conjunct stays residual.
    Both,
}

/// One indexed equi condition: `left = right`, with the left expression
/// evaluated against the left row and the right expression against the
/// qualified right row. The ref lists are the exact keys a row must contain
/// for that single-side evaluation to agree with merged-row evaluation:
/// `eval_expr` falls back to suffix matching when the exact key is missing,
/// which can bind `r.x` to `l.x` only on the merged row — so a missing key
/// degrades that row to "probe everything" instead of keying it.
pub(super) struct EquiPair {
    left: Box<SqlExpr>,
    right: Box<SqlExpr>,
    left_refs: Vec<String>,
    right_refs: Vec<String>,
}

/// The equi-join plan for `on`: `left_expr = right_expr` pairs whose sides
/// cleanly split across the join boundary. Only qualified references
/// participate — unqualified names resolve through `lookup_col`'s suffix
/// fallback and can silently bind to either side, so they stay in the
/// residual. Same-side (`l.x = l.y`), mixed-side (`l.a = r.b + l.c`) and
/// unmodelable conjuncts stay in the residual too; the plan is `None` only
/// when nothing extractable remains.
pub(super) fn equi_plan(right_key: &str, on: &SqlExpr) -> Option<Vec<EquiPair>> {
    let mut conjuncts = Vec::new();
    and_conjuncts(on, &mut conjuncts);
    let jprefix = format!("{right_key}.");
    let classify = |e: &SqlExpr| -> Option<(Vec<String>, Side)> {
        let mut refs = Vec::new();
        column_refs(e, &mut refs)?;
        let (mut saw_left, mut saw_right) = (false, false);
        for r in &refs {
            if r.starts_with(&jprefix) {
                saw_right = true;
            } else if r.contains('.') {
                saw_left = true;
            } else {
                // Unqualified refs are ambiguous under suffix resolution.
                return None;
            }
        }
        let side = match (saw_left, saw_right) {
            (true, false) => Side::Left,
            (false, true) => Side::Right,
            // Literal-only keys evaluate identically on either side; treat
            // them as left so `r.x = 5` pairs probe a constant.
            (false, false) => Side::Left,
            (true, true) => Side::Both,
        };
        Some((refs, side))
    };
    let mut plan = Vec::new();
    for c in conjuncts {
        let SqlExpr::BinaryOp {
            left,
            op: sqlparser::ast::BinaryOperator::Eq,
            right,
        } = c
        else {
            continue; // residual predicate: re-evaluated per candidate
        };
        let (l, r) = match (classify(left), classify(right)) {
            (Some(l), Some(r)) => (l, r),
            // Unmodelable conjunct stays in the residual; the remaining
            // conjuncts still form a plan.
            _ => continue,
        };
        let (le, le_refs, re, re_refs) = match (l.1, r.1) {
            (Side::Left, Side::Right) => (left, l.0, right, r.0),
            (Side::Right, Side::Left) => (right, r.0, left, l.0),
            // Same-side filters and side-mixing expressions.
            _ => continue,
        };
        // SqlExpr has no PartialEq; identical printed forms are duplicates.
        if plan.iter().any(|p: &EquiPair| {
            p.left.to_string() == le.to_string() && p.right.to_string() == re.to_string()
        }) {
            continue; // duplicate conjunct adds no key component
        }
        plan.push(EquiPair {
            left: le.clone(),
            right: re.clone(),
            left_refs: le_refs,
            right_refs: re_refs,
        });
    }
    if plan.is_empty() {
        None
    } else {
        Some(plan)
    }
}

/// Merged join row: `l` plus the right row's fields. `right` keys must
/// already be qualified (`r.k`).
pub(super) fn merged_row(l: &Object, right: &Object) -> Object {
    let mut merged = l.clone();
    for (k, v) in right {
        merged.insert(k.clone(), v.clone());
    }
    merged
}

/// LEFT/INNER join's unmatched left row: the right side's columns padded
/// with NULL. The pad set is the union of every qualified right row's keys
/// (row schemas vary in a schemaless store — padding only the FIRST row's
/// keys left a column only later rows carry unpadded, and the suffix
/// fallback answered it with the left side's same-named value). An EMPTY
/// right side has no sample, so `right_cols` (the factor's planned columns)
/// pads the keys — without them a same-named left column answers the
/// suffix fallback and masquerades as the right value (`WHERE r.id IS
/// NULL` never fires).
pub(super) fn null_extended_left(
    l: &Object,
    qright: &[Object],
    right_key: &str,
    right_cols: Option<&[String]>,
) -> Object {
    let mut merged = l.clone();
    if qright.is_empty() {
        if let Some(cols) = right_cols {
            for c in cols {
                merged.insert(format!("{right_key}.{c}"), Value::Null);
            }
        }
    } else {
        for row in qright {
            for k in row.keys() {
                merged.insert(k.clone(), Value::Null);
            }
        }
    }
    merged
}

/// RIGHT/FULL join's unmatched right row: the left side's columns padded
/// with NULL. `right` keys must already be qualified. `left_cols` is the
/// planned left key shape used when the left side produced no rows at all
/// (bare base columns plus the qualified keys of earlier joins).
pub(super) fn null_extended_right(
    left_fields: &[String],
    right: &Object,
    left_cols: Option<&[String]>,
) -> Object {
    let mut merged = Object::new();
    if left_fields.is_empty() {
        // The left side produced no rows: its key shape cannot be read off
        // data, so pad the PLANNED columns instead (without them a
        // same-named right column answers the suffix fallback and leaks the
        // right value into a left-side reference).
        for k in left_cols.unwrap_or(&[]) {
            merged.insert(k.clone(), Value::Null);
        }
    }
    for k in left_fields {
        merged.insert(k.clone(), Value::Null);
    }
    for (k, v) in right {
        merged.insert(k.clone(), v.clone());
    }
    merged
}

/// Equi-join over a canonical-key index: right rows are bucketed by their
/// key, each left row probes its bucket, and every candidate still passes
/// the full ON on the merged row (the index is a superset filter, the ON is
/// the decider). Rows whose key expressions do not resolve to exact keys
/// (missing columns, which suffix fallback could rebind across the boundary
/// on the merged row) or produce unmodelable values (documents) degrade
/// only their own row to "probe everything" instead of falling back for the
/// whole join. Emission order matches the nested loop: left order, then
/// right order within a left row.
#[allow(clippy::too_many_arguments)]
pub(super) fn hash_join(
    left: &[Object],
    qright: &[Object],
    on: &SqlExpr,
    plan: &[EquiPair],
    left_join: bool,
    right_join: bool,
    deadline: &StmtDeadline,
    right_key: &str,
    right_cols: Option<&[String]>,
    left_cols: Option<&[String]>,
) -> Result<Vec<Object>> {
    let mut map: std::collections::HashMap<Vec<JoinKey>, Vec<usize>> =
        std::collections::HashMap::new();
    let mut always: Vec<usize> = Vec::new();
    for (i, qr) in qright.iter().enumerate() {
        deadline.check()?;
        let mut key = Vec::with_capacity(plan.len());
        let mut indexed = true;
        for p in plan {
            if p.right_refs.iter().all(|r| qr.get(r).is_some()) {
                match join_key_of(&eval_expr(&p.right, qr)?) {
                    Some(k) => key.push(k),
                    None => indexed = false,
                }
            } else {
                indexed = false;
            }
            if !indexed {
                break;
            }
        }
        if indexed {
            map.entry(key).or_default().push(i);
        } else {
            always.push(i);
        }
    }
    let mut right_matched = vec![false; qright.len()];
    let mut out = Vec::new();
    for l in left {
        deadline.check()?;
        let mut lkey = Vec::with_capacity(plan.len());
        let mut unindexed = false;
        for p in plan {
            if p.left_refs.iter().all(|r| l.get(r).is_some()) {
                match join_key_of(&eval_expr(&p.left, l)?) {
                    Some(k) => lkey.push(k),
                    None => unindexed = true,
                }
            } else {
                unindexed = true;
            }
            if unindexed {
                break;
            }
        }
        let mut cands: Vec<usize> = Vec::new();
        if unindexed {
            cands.extend(0..qright.len());
        } else {
            // The bucket and `always` are each ascending (built in right-row
            // order) and disjoint, so a linear merge restores overall right
            // order in O(k) instead of sorting per left row.
            let empty: &[usize] = &[];
            let bucket = map.get(&lkey).map(Vec::as_slice).unwrap_or(empty);
            cands.reserve(bucket.len() + always.len());
            let (mut bi, mut ai) = (0usize, 0usize);
            while bi < bucket.len() || ai < always.len() {
                let take_bucket =
                    ai >= always.len() || (bi < bucket.len() && bucket[bi] < always[ai]);
                if take_bucket {
                    cands.push(bucket[bi]);
                    bi += 1;
                } else {
                    cands.push(always[ai]);
                    ai += 1;
                }
            }
        }
        let mut matched = false;
        for &ri in &cands {
            deadline.check()?;
            let merged = merged_row(l, &qright[ri]);
            if matches!(eval_expr(on, &merged)?, Value::Bool(true)) {
                matched = true;
                right_matched[ri] = true;
                out.push(merged);
            }
        }
        if !matched && left_join {
            out.push(null_extended_left(l, qright, right_key, right_cols));
        }
    }
    if right_join {
        let left_fields = union_of_fields(left);
        for (ri, r) in qright.iter().enumerate() {
            if !right_matched[ri] {
                out.push(null_extended_right(&left_fields, r, left_cols));
            }
        }
    }
    Ok(out)
}

/// Nested-loop join and hash-join dispatcher. ON is evaluated over the
/// merged (qualified) row; LEFT/RIGHT joins null-extend the unmatched side.
/// `right_cols`/`left_cols` are the factors' planned (bare / row-shape)
/// column lists, used to pad NULL keys when a side is EMPTY (no row to
/// sample the key shape from).
#[allow(clippy::too_many_arguments)]
pub(super) fn join_rows(
    left: Vec<Object>,
    right: &[Object],
    right_key: &str,
    on: Option<&SqlExpr>,
    left_join: bool,
    right_join: bool,
    deadline: &StmtDeadline,
    right_cols: Option<&[String]>,
    left_cols: Option<&[String]>,
) -> Result<Vec<Object>> {
    // Qualify the right rows once: both paths merge them into left rows as
    // pre-qualified key-value pairs, so assembly stays byte-identical
    // between the hash path and the nested loop.
    let qright: Vec<Object> = right.iter().map(|r| qualify(r, right_key)).collect();
    // Equi-join fast path: an ON whose conjuncts split cleanly across the
    // join boundary (qualified `l.x = r.y` pairs) indexes the right side and
    // probes per left row instead of walking every pair. Everything else —
    // cross joins, non-equi predicates, unqualified/ambiguous ON — keeps the
    // nested loop below.
    if let Some(on) = on {
        if let Some(plan) = equi_plan(right_key, on) {
            return hash_join(
                &left, &qright, on, &plan, left_join, right_join, deadline, right_key, right_cols,
                left_cols,
            );
        }
    }
    let mut right_matched = vec![false; qright.len()];
    let mut out = Vec::new();
    for l in &left {
        deadline.check()?;
        let mut matched = false;
        for (ri, qr) in qright.iter().enumerate() {
            deadline.check()?;
            let merged = merged_row(l, qr);
            let ok = match on {
                None => true,
                Some(e) => matches!(eval_expr(e, &merged)?, Value::Bool(true)),
            };
            if ok {
                matched = true;
                right_matched[ri] = true;
                out.push(merged);
            }
        }
        if !matched && left_join {
            out.push(null_extended_left(l, &qright, right_key, right_cols));
        }
    }
    if right_join {
        let left_fields = union_of_fields(&left);
        for (ri, qr) in qright.iter().enumerate() {
            if !right_matched[ri] {
                out.push(null_extended_right(&left_fields, qr, left_cols));
            }
        }
    }
    Ok(out)
}
