use super::*;

/// Evaluate a constant expression (literal / arithmetic on literals).
pub fn eval_const(e: &SqlExpr) -> Result<Value> {
    match e {
        SqlExpr::Value(v) => sql_value(v),
        SqlExpr::UnaryOp { op, expr } => {
            // `-9223372036854775808` lexes as Minus over 9223372036854775808,
            // which overflows i64 on its own — re-parse the signed text so the
            // value stays an Int instead of degrading to Float.
            if *op == sqlparser::ast::UnaryOperator::Minus {
                if let SqlExpr::Value(nv) = expr.as_ref() {
                    if let sqlparser::ast::Value::Number(n, _) = &nv.value {
                        if let Ok(i) = format!("-{n}").parse::<i64>() {
                            return Ok(Value::Int(i));
                        }
                    }
                }
            }
            let v = eval_const(expr)?;
            match (op, v) {
                (sqlparser::ast::UnaryOperator::Minus, Value::Int(i)) => {
                    Ok(Value::Int(i.wrapping_neg()))
                }
                (sqlparser::ast::UnaryOperator::Minus, Value::Float(f)) => Ok(Value::Float(-f)),
                (sqlparser::ast::UnaryOperator::Minus, Value::Decimal(d)) => {
                    // Signed-zero guard, same as the runtime unary minus.
                    let neg = -d;
                    Ok(Value::Decimal(if neg.is_zero() && neg.is_sign_negative() {
                        rust_decimal::Decimal::ZERO
                    } else {
                        neg
                    }))
                }
                // T-SQL bitwise NOT (~x): two's-complement on 64-bit ints.
                (sqlparser::ast::UnaryOperator::BitwiseNot, Value::Int(i)) => Ok(Value::Int(!i)),
                _ => err("unsupported unary operand"),
            }
        }
        SqlExpr::BinaryOp { left, op, right } => {
            let l = eval_const(left)?;
            let r = eval_const(right)?;
            binop(l, op, r)
        }
        SqlExpr::Nested(e) => eval_const(e),
        // SQL standard POSITION(sub IN str): 1-based, 0 when absent.
        SqlExpr::Position { expr, r#in } => {
            let needle = eval_const(expr)?;
            let hay = eval_const(r#in)?;
            position_of(&needle, &hay)
        }
        SqlExpr::Floor { expr, .. } => scalar_function("FLOOR", &[eval_const(expr)?]),
        SqlExpr::Ceil { expr, .. } => scalar_function("CEILING", &[eval_const(expr)?]),
        // CAST of a constant (bound decimal/blob parameters render as
        // CAST/hex literals; resolved INSERT replay re-evaluates them).
        SqlExpr::Cast {
            kind,
            expr,
            data_type,
            ..
        } => {
            let v = cast_value(eval_const(expr)?, &data_type.to_string());
            // T-SQL TRY_CAST: a failed conversion yields NULL, not an error.
            if matches!(kind, sqlparser::ast::CastKind::TryCast) {
                v.or(Ok(Value::Null))
            } else {
                v
            }
        }
        // Pure scalar functions of constants: resolved replication rewrites
        // and dump scripts render structured values and non-finite floats as
        // JSON_EXTRACT(...)/CAST(...) expressions, which must evaluate back
        // to the same value on replay. Only argument shapes the normal
        // expression path accepts are allowed here.
        SqlExpr::Function(f) => {
            let name = f.name.to_string().to_uppercase();
            if f.over.is_some() {
                return err(format!("window functions (OVER) are not supported: {name}"));
            }
            // MySQL IF(): lazy branches, like CASE — the untaken branch must
            // not be evaluated (an error there would be spurious).
            if name == "IF" {
                return eval_if_expr(f, eval_const);
            }
            let mut args = Vec::new();
            if let sqlparser::ast::FunctionArguments::List(list) = &f.args {
                for a in &list.args {
                    match a {
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(inner),
                        ) => {
                            // Datepart names are bare identifiers, not
                            // columns (see `eval_fn_arg`).
                            if args.is_empty() && takes_datepart_name(&name) {
                                if let SqlExpr::Identifier(i) = inner {
                                    args.push(Value::Str(i.value.clone()));
                                    continue;
                                }
                            }
                            args.push(eval_const(inner)?)
                        }
                        _ => return err(format!("unsupported argument to {name}")),
                    }
                }
            }
            scalar_function(&name, &args)
        }
        SqlExpr::Identifier(i) => err(format!("column {} not allowed here", i.value)),
        SqlExpr::TypedString(ts) => typed_string_value(ts),
        // Constant CASE (`VALUES (CASE WHEN … THEN … END)` in a resolved
        // INSERT, replayed scripts): every branch is constant-foldable, the
        // same comparison rule as the row-level CASE arm applies.
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            let base = operand.as_ref().map(|o| eval_const(o)).transpose()?;
            for w in conditions {
                let hit = match &base {
                    Some(b) => {
                        let cv = eval_const(&w.condition)?;
                        !matches!(b, Value::Null)
                            && !matches!(cv, Value::Null)
                            && cmp_coerced(b, &cv) == Some(std::cmp::Ordering::Equal)
                    }
                    None => matches!(eval_const(&w.condition)?, Value::Bool(true)),
                };
                if hit {
                    return eval_const(&w.result);
                }
            }
            match else_result {
                Some(r) => eval_const(r),
                None => Ok(Value::Null),
            }
        }
        other => err(format!("unsupported expression: {other}")),
    }
}

pub(super) fn sql_value(v: &sqlparser::ast::Value) -> Result<Value> {
    use sqlparser::ast::Value as V;
    let out = match v {
        V::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                Value::Int(i)
            } else {
                let f = n
                    .parse::<f64>()
                    .map_err(|_| SqlError::Message(format!("bad number {n}")))?;
                // Overflowing literals ("1e999") parse to inf; admitting
                // them would store rows no dump or journal replay can render.
                if !f.is_finite() {
                    return err(format!("non-finite numeric literal: {n}"));
                }
                Value::Float(f)
            }
        }
        V::SingleQuotedString(s) | V::DoubleQuotedString(s) => Value::Str(s.clone()),
        // T-SQL national strings: the engine has one UTF-8 text type, so the
        // prefix only selects the literal form.
        V::NationalStringLiteral(s) => Value::Str(s.clone()),
        V::Boolean(b) => Value::Bool(*b),
        V::Null => Value::Null,
        // x'deadbeef' — the canonical BLOB literal (params, dumps, replays).
        V::HexStringLiteral(s) => {
            let cleaned: String = s.chars().filter(|c| !c.is_whitespace()).collect();
            if !cleaned.len().is_multiple_of(2) {
                return err(format!("hex literal has an odd digit count: x'{s}'"));
            }
            let mut bytes = Vec::with_capacity(cleaned.len() / 2);
            let chars: Vec<char> = cleaned.chars().collect();
            for pair in chars.chunks(2) {
                let hi = pair[0]
                    .to_digit(16)
                    .ok_or_else(|| SqlError::Message(format!("bad hex digit: {s}")))?;
                let lo = pair[1]
                    .to_digit(16)
                    .ok_or_else(|| SqlError::Message(format!("bad hex digit: {s}")))?;
                bytes.push(((hi << 4) | lo) as u8);
            }
            Value::Bytes(bytes)
        }
        other => return err(format!("unsupported literal: {other}")),
    };
    Ok(out)
}

/// Resolve a bare column name: exact key first, then a unique "alias.col"
/// match (qualified rows from joins); ambiguous matches are an error.
pub(super) fn lookup_col(doc: &Object, name: &str) -> Result<Value> {
    if let Some(v) = doc.get(name) {
        return Ok(v.clone());
    }
    // Unqualified name on JOINED rows (every key is "alias.col"): take the
    // FIRST source in row order (BTreeMap order is deterministic; matches
    // left-table-first convention). Restricted to genuinely join-shaped
    // rows — every key qualified AND at least two distinct qualifiers. A
    // single-table doc whose only fields are literal dotted names ("g.x")
    // has one prefix: the suffix answer made query results depend on
    // whether an index exists (trees only ever key the bare name). The
    // injected ROWNUM pseudo-column is a bare key that must not disqualify
    // a joined row from the fallback.
    let mut qualifiers = 0usize;
    let mut last_prefix = "";
    let mut join_shaped = true;
    for k in doc.keys() {
        if k == "ROWNUM" {
            continue;
        }
        match k.rfind('.') {
            Some(i) => {
                let p = &k[..i];
                if p != last_prefix {
                    qualifiers += 1;
                    last_prefix = p;
                }
            }
            None => {
                join_shaped = false;
                break;
            }
        }
    }
    if !join_shaped || qualifiers < 2 {
        return Ok(Value::Null);
    }
    let suffix = format!(".{name}");
    Ok(doc
        .iter()
        .find(|(k, _)| k.ends_with(&suffix))
        .map(|(_, v)| v.clone())
        .unwrap_or(Value::Null))
}

/// True for the T-SQL datepart family whose FIRST argument is a bare
/// datepart name (`DATEPART(day, d)`) — never a column reference.
pub(super) fn takes_datepart_name(name: &str) -> bool {
    matches!(
        name,
        "DATEADD" | "DATEDIFF" | "DATEDIFF_BIG" | "DATEPART" | "DATENAME"
    )
}

/// Evaluate one function argument for the Function arm of [`eval_expr`]:
/// the leading identifier of the datepart family rides through as the
/// datepart name itself.
pub(super) fn eval_fn_arg(e: &SqlExpr, name: &str, first: bool, doc: &Object) -> Result<Value> {
    if first && takes_datepart_name(name) {
        if let SqlExpr::Identifier(i) = e {
            return Ok(Value::Str(i.value.clone()));
        }
    }
    eval_expr(e, doc)
}

/// 1-based character position of `needle` in `hay`, 0 when absent; NULL
/// propagates. Shared by the standard `POSITION(x IN y)` expression and
/// MySQL `LOCATE` so both count characters identically.
fn position_of(needle: &Value, hay: &Value) -> Result<Value> {
    if matches!(needle, Value::Null) || matches!(hay, Value::Null) {
        return Ok(Value::Null);
    }
    let needle = value_to_text(needle);
    let hay = value_to_text(hay);
    Ok(Value::Int(match hay.find(&needle) {
        Some(b) => hay[..b].chars().count() as i64 + 1,
        None => 0,
    }))
}

/// MySQL `IF(cond, then, else)` with lazy branches: only the taken branch is
/// evaluated (same as CASE — an error in the untaken branch must not fire).
/// Truthiness follows MySQL: nonzero numbers are true, NULL is false.
fn eval_if_expr<F>(f: &sqlparser::ast::Function, mut eval: F) -> Result<Value>
where
    F: FnMut(&SqlExpr) -> Result<Value>,
{
    let mut exprs: Vec<&SqlExpr> = Vec::new();
    if let sqlparser::ast::FunctionArguments::List(list) = &f.args {
        for a in &list.args {
            match a {
                sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) => {
                    exprs.push(e)
                }
                _ => return err("unsupported argument to IF"),
            }
        }
    }
    if exprs.len() != 3 {
        return err(format!(
            "function IF takes exactly 3 arguments, got {}",
            exprs.len()
        ));
    }
    let truthy = match eval(exprs[0])? {
        Value::Bool(b) => b,
        Value::Int(i) => i != 0,
        Value::Float(x) => x != 0.0,
        Value::Decimal(d) => !d.is_zero(),
        Value::Str(s) => s.trim().parse::<f64>().is_ok_and(|x| x != 0.0),
        _ => false,
    };
    eval(exprs[if truthy { 1 } else { 2 }])
}

/// Evaluate an expression against a document row.
pub fn eval_expr(e: &SqlExpr, doc: &Object) -> Result<Value> {
    match e {
        // T-SQL variables/parameters (`@p`, `@@ROWCOUNT`) read a document
        // field by that name today, i.e. silently NULL unless the document
        // happens to carry it. Bare (unquoted) forms are almost always a
        // variable: refuse and point at binding/quoted field access.
        SqlExpr::Identifier(i) if i.quote_style.is_none() && i.value.starts_with('@') => err(
            format!(
                "variables/parameters ({0}) are not supported; bind the value or quote the field name (\"{0}\")",
                i.value
            ),
        ),
        SqlExpr::Identifier(i) => lookup_col(doc, &i.value),
        SqlExpr::CompoundIdentifier(parts)
            if parts
                .first()
                .is_some_and(|p| p.quote_style.is_none() && p.value.starts_with('@')) =>
        {
            err(format!(
                "variables/parameters ({}) are not supported; bind the value or quote the field name",
                parts.first().map(|p| p.value.as_str()).unwrap_or("@")
            ))
        }
        SqlExpr::CompoundIdentifier(parts) => {
            let full = parts
                .iter()
                .map(|p| p.value.clone())
                .collect::<Vec<_>>()
                .join(".");
            // Oracle sequence pseudo-columns have no DocSQL equivalent; a
            // silent NULL here would corrupt every migrated key. Quote the
            // name (`"nextval"`) to read a literal field with that name.
            if let Some(last) = parts.last() {
                if last.quote_style.is_none()
                    && (last.value.eq_ignore_ascii_case("nextval")
                        || last.value.eq_ignore_ascii_case("currval"))
                {
                    return err(format!(
                        "Oracle sequence reference {full} is not supported; migrate the key to AUTOINCREMENT / IDENTITY"
                    ));
                }
            }
            if let Some(v) = doc.get(&full) {
                Ok(v.clone())
            } else {
                let last = parts.last().map(|p| p.value.clone()).unwrap_or_default();
                lookup_col(doc, &last)
            }
        }
        SqlExpr::Value(v) => sql_value(v),
        // Typed string literal: `TIMESTAMP '2026-09-16T00:00:00Z'`. A
        // malformed timestamp is a query error (static text, evaluated on
        // every row) — unlike a compared string, which degrades to NULL.
        SqlExpr::TypedString(ts) => typed_string_value(ts),
        SqlExpr::UnaryOp { op, expr } => {
            let v = eval_expr(expr, doc)?;
            match (op, v) {
                (sqlparser::ast::UnaryOperator::Minus, Value::Int(i)) => match i.checked_neg() {
                    Some(n) => Ok(Value::Int(n)),
                    // -i64::MIN overflows; mirror literal handling and keep
                    // the value instead of panicking in debug builds.
                    None => Ok(Value::Float(-(i as f64))),
                },
                (sqlparser::ast::UnaryOperator::Minus, Value::Float(f)) => Ok(Value::Float(-f)),
                (sqlparser::ast::UnaryOperator::Minus, Value::Decimal(d)) => {
                    // Signed-zero guard: decimal text cannot replay -0
                    // ("−0.00" parses back to +0), so the dump/replica
                    // rewrite would silently flip the encoded bytes.
                    let neg = -d;
                    Ok(Value::Decimal(
                        if neg.is_zero() && neg.is_sign_negative() {
                            rust_decimal::Decimal::ZERO
                        } else {
                            neg
                        },
                    ))
                }
                (sqlparser::ast::UnaryOperator::Not, Value::Bool(b)) => Ok(Value::Bool(!b)),
                (sqlparser::ast::UnaryOperator::Not, Value::Null) => Ok(Value::Null),
                // T-SQL bitwise NOT (~x): two's-complement on 64-bit ints.
                (sqlparser::ast::UnaryOperator::BitwiseNot, Value::Int(i)) => Ok(Value::Int(!i)),
                _ => err("unsupported unary operand"),
            }
        }
        SqlExpr::BinaryOp { left, op, right } => {
            let l = eval_expr(left, doc)?;
            let r = eval_expr(right, doc)?;
            binop(l, op, r)
        }
        SqlExpr::Nested(e) => eval_expr(e, doc),
        // SQL standard POSITION(sub IN str): 1-based, 0 when absent.
        SqlExpr::Position { expr, r#in } => {
            let needle = eval_expr(expr, doc)?;
            let hay = eval_expr(r#in, doc)?;
            position_of(&needle, &hay)
        }
        SqlExpr::IsNull(inner) => Ok(Value::Bool(matches!(eval_expr(inner, doc)?, Value::Null))),
        SqlExpr::IsNotNull(inner) => {
            Ok(Value::Bool(!matches!(eval_expr(inner, doc)?, Value::Null)))
        }
        // NULL-safe comparison (SQL:2003): a comparison that is false or
        // unknown is "distinct"; NULL matches only NULL.
        SqlExpr::IsDistinctFrom(l, r) => {
            let (lv, rv) = (eval_expr(l, doc)?, eval_expr(r, doc)?);
            Ok(Value::Bool(!values_equal_null_safe(&lv, &rv)))
        }
        SqlExpr::IsNotDistinctFrom(l, r) => {
            let (lv, rv) = (eval_expr(l, doc)?, eval_expr(r, doc)?);
            Ok(Value::Bool(values_equal_null_safe(&lv, &rv)))
        }
        SqlExpr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            // NULL anywhere makes the predicate unknown -> false in WHERE.
            let (v, lo, hi) = (
                eval_expr(expr, doc)?,
                eval_expr(low, doc)?,
                eval_expr(high, doc)?,
            );
            if matches!(v, Value::Null) || matches!(lo, Value::Null) || matches!(hi, Value::Null) {
                return Ok(Value::Bool(false));
            }
            // Same TIMESTAMP↔string coercion as the bare comparisons; an
            // uncoercable pair is unknown, and unknown is false under either
            // polarity (NOT BETWEEN an unknown is still unknown).
            let (Some(lo_ord), Some(hi_ord)) = (cmp_coerced(&v, &lo), cmp_coerced(&v, &hi))
            else {
                return Ok(Value::Bool(false));
            };
            let inside = lo_ord != Ordering::Less && hi_ord != Ordering::Greater;
            Ok(Value::Bool(inside != *negated))
        }
        SqlExpr::Like {
            negated,
            expr,
            pattern,
            escape_char,
            ..
        }
        | SqlExpr::ILike {
            negated,
            expr,
            pattern,
            escape_char,
            ..
        } => {
            let ci = matches!(e, SqlExpr::ILike { .. });
            let v = eval_expr(expr, doc)?;
            let p = eval_expr(pattern, doc)?;
            let esc = escape_char.as_ref().and_then(|v| match &v.value {
                sqlparser::ast::Value::SingleQuotedString(s) => s.chars().next(),
                _ => None,
            });
            let hit = match (&v, &p) {
                (Value::Null, _) | (_, Value::Null) => false,
                (Value::Str(s), Value::Str(pat)) => {
                    let (s, pat) = if ci {
                        // Char-wise case fold: a full-string to_lowercase()
                        // changes the CHARACTER COUNT for some Unicode
                        // (U+0130 'İ' expands to two chars), skewing `_`
                        // wildcard counts. Folding each char to its first
                        // lowercase char keeps pattern and text aligned.
                        (
                            s.chars()
                                .map(|c| c.to_lowercase().next().unwrap_or(c))
                                .collect::<String>(),
                            pat.chars()
                                .map(|c| c.to_lowercase().next().unwrap_or(c))
                                .collect::<String>(),
                        )
                    } else {
                        (s.clone(), pat.clone())
                    };
                    like_match(&s, &pat, esc)
                }
                _ => false,
            };
            Ok(Value::Bool(hit != *negated))
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            let op = operand.as_ref().map(|o| eval_expr(o, doc)).transpose()?;
            for w in conditions {
                let hit = match &op {
                    Some(base) => {
                        let cv = eval_expr(&w.condition, doc)?;
                        !matches!(base, Value::Null)
                            && !matches!(cv, Value::Null)
                            // Same coercion funnel as `=`: `CASE ts WHEN
                            // '2026-…' THEN` compares like `ts = '2026-…'`.
                            && cmp_coerced(base, &cv) == Some(Ordering::Equal)
                    }
                    None => matches!(eval_expr(&w.condition, doc)?, Value::Bool(true)),
                };
                if hit {
                    return eval_expr(&w.result, doc);
                }
            }
            match else_result {
                Some(r) => eval_expr(r, doc),
                None => Ok(Value::Null),
            }
        }
        SqlExpr::Cast {
            kind,
            expr,
            data_type,
            ..
        } => {
            let v = cast_value(eval_expr(expr, doc)?, &data_type.to_string());
            // T-SQL TRY_CAST: a failed conversion yields NULL, not an error.
            if matches!(kind, sqlparser::ast::CastKind::TryCast) {
                v.or(Ok(Value::Null))
            } else {
                v
            }
        }
        // SUBSTR/SUBSTRING parses to a dedicated node, not a Function call.
        SqlExpr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            let base = eval_expr(expr, doc)?;
            let start = match substring_from {
                Some(e) => match eval_expr(e, doc)? {
                    Value::Int(i) => i,
                    Value::Null => return Ok(Value::Null),
                    other => return err(format!("SUBSTR start must be an integer: {other:?}")),
                },
                None => 1,
            };
            let len = match substring_for {
                Some(e) => match eval_expr(e, doc)? {
                    Value::Int(i) => Some(i),
                    Value::Null => return Ok(Value::Null),
                    other => return err(format!("SUBSTR length must be an integer: {other:?}")),
                },
                None => None,
            };
            let args = vec![
                base,
                Value::Int(start),
                Value::Int(len.unwrap_or(i64::MAX / 4)),
            ];
            scalar_function("SUBSTR", &args)
        }
        SqlExpr::Trim {
            trim_where,
            expr,
            trim_what,
            ..
        } => {
            let base = eval_expr(expr, doc)?;
            // With a character set (T-SQL `TRIM('ab' FROM x)` / the
            // LEADING/TRAILING/BOTH forms) strip exactly those characters
            // from the chosen side; without one this is the classic
            // whitespace TRIM/LTRIM/RTRIM.
            if let Some(what) = trim_what {
                let chars = match eval_expr(what, doc)? {
                    Value::Null => return Ok(Value::Null),
                    v => value_to_text(&v).chars().collect::<Vec<char>>(),
                };
                let strip = |s: &str, head: bool, tail: bool| -> String {
                    if head {
                        s.trim_start_matches(|c| chars.contains(&c))
                    } else {
                        s
                    }
                    .trim_end_matches(|c| tail && chars.contains(&c))
                    .to_string()
                };
                let text = value_to_text(&base);
                return Ok(Value::Str(match trim_where {
                    Some(sqlparser::ast::TrimWhereField::Leading) => strip(&text, true, false),
                    Some(sqlparser::ast::TrimWhereField::Trailing) => strip(&text, false, true),
                    _ => strip(&text, true, true),
                }));
            }
            let name = match trim_where {
                Some(sqlparser::ast::TrimWhereField::Leading) => "LTRIM",
                Some(sqlparser::ast::TrimWhereField::Trailing) => "RTRIM",
                _ => "TRIM",
            };
            scalar_function(name, &[base])
        }
        // FLOOR/CEILING parse into dedicated nodes on some paths; route
        // them onto the scalar library.
        SqlExpr::Floor { expr, .. } => {
            scalar_function("FLOOR", &[eval_expr(expr, doc)?])
        }
        SqlExpr::Ceil { expr, .. } => {
            scalar_function("CEILING", &[eval_expr(expr, doc)?])
        }
        SqlExpr::Function(f) => {
            let name = f.name.to_string().to_uppercase();
            if f.over.is_some() {
                return err(format!("window functions (OVER) are not supported: {name}"));
            }
            if matches!(
                name.as_str(),
                "COUNT" | "COUNT_BIG" | "SUM" | "AVG" | "MIN" | "MAX" | "GROUP_CONCAT"
                    | "STRING_AGG" | "GROUPING"
            ) {
                return err(format!(
                    "aggregate function {name} is not allowed in this context"
                ));
            }
            // MySQL IF(): lazy branches, like CASE — the untaken branch must
            // not be evaluated (an error there would be spurious).
            if name == "IF" {
                return eval_if_expr(f, |e| eval_expr(e, doc));
            }
            let mut args = Vec::new();
            if let sqlparser::ast::FunctionArguments::List(list) = &f.args {
                for a in &list.args {
                    match a {
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(e),
                        ) => args.push(eval_fn_arg(e, &name, args.is_empty(), doc)?),
                        _ => return err(format!("unsupported argument to {name}")),
                    }
                }
            }
            scalar_function(&name, &args)
        }
        SqlExpr::InList {
            expr,
            list,
            negated,
        } => {
            let v = eval_expr(expr, doc)?;
            let mut hit = false;
            // The ONLY unknown source (kept three-valued so `x NOT IN
            // ('garbage')` on a TIMESTAMP column behaves exactly like
            // `x <> 'garbage'`: unknown, filtered — never silently true).
            let mut unknown = false;
            for item in list {
                let iv = eval_expr(item, doc)?;
                // NULL is a comparable value in this engine (the same
                // total order `x = NULL` / hash joins / unique checks
                // use): a NULL ELEMENT therefore participates in the
                // equality like any other. The old saw_null poisoning
                // made `x NOT IN (NULL)` false while `x <> NULL` was
                // true — two spellings of one predicate disagreed, and
                // `= ANY` (rewritten to IN) diverged from `= ALL`.
                // Equality through the TIMESTAMP↔string coercion funnel,
                // so `ts IN ('2026-…', …)` matches like `ts = '2026-…'`.
                match cmp_coerced(&v, &iv) {
                    Some(Ordering::Equal) => {
                        hit = true;
                        break;
                    }
                    None => unknown = true,
                    Some(_) => {}
                }
            }
            if hit {
                return Ok(Value::Bool(!*negated));
            }
            if unknown {
                return Ok(Value::Null);
            }
            Ok(Value::Bool(*negated))
        }
        // Subqueries are resolved per-statement by subst_expr; reaching one
        // here means it was correlated or unsupported.
        SqlExpr::Subquery(_) => err("correlated subqueries are not supported"),
        SqlExpr::InSubquery { .. } => err("correlated subqueries are not supported"),
        SqlExpr::Exists { .. } => err("correlated subqueries are not supported"),
        SqlExpr::AnyOp { .. } | SqlExpr::AllOp { .. } => {
            err("ANY/ALL subqueries are not supported here")
        }
        other => err(format!("unsupported expression: {other}")),
    }
}

/// Typed string literal (`TIMESTAMP '…'` / `DATE '…'`): a malformed
/// timestamp is a query error — the text is static, unlike a compared
/// string, which degrades to NULL in predicates.
pub(super) fn typed_string_value(ts: &sqlparser::ast::TypedString) -> Result<Value> {
    let is_ts = matches!(
        ts.data_type,
        sqlparser::ast::DataType::Timestamp(_, _)
            | sqlparser::ast::DataType::TimestampNtz(_)
            | sqlparser::ast::DataType::Datetime(_)
            | sqlparser::ast::DataType::Date
    );
    if !is_ts {
        return err(format!("typed literal {} is not supported", ts.data_type));
    }
    let text = match &ts.value.value {
        sqlparser::ast::Value::SingleQuotedString(s)
        | sqlparser::ast::Value::DoubleQuotedString(s) => s.clone(),
        other => return err(format!("bad typed literal value: {other}")),
    };
    Ok(Value::Timestamp(
        crate::value::parse_timestamp_ms(&text)
            .ok_or_else(|| SqlError::Message(format!("bad TIMESTAMP literal '{text}'")))?,
    ))
}

/// SQL LIKE: `%` (any run), `_` (one char), `ESCAPE`, and the T-SQL
/// `[abc]`/`[^a-z]` character classes. The matcher lives in `tsql` and is
/// pinned by its own known-answer tests.
pub(super) fn like_match(s: &str, pat: &str, esc: Option<char>) -> bool {
    crate::tsql::like_match(s, pat, esc)
}

/// `(skip, take)` when LIMIT/OFFSET are integer constants. `None` means the
/// window cannot be bounded ahead of time (no LIMIT clause, FETCH, or a
/// non-integer bound) — the generic path decides. Negative bounds follow
/// SQLite semantics (LIMIT -1 = no limit, OFFSET -1 = skip nothing), so a
/// pure `LIMIT -1 OFFSET n` skip still uses the index-ordered window with an
/// unbounded take.
pub(super) fn constant_limit_window(query: &Query) -> Option<(usize, usize)> {
    if query.fetch.is_some() {
        return None;
    }
    pub(super) fn bound(e: &SqlExpr) -> Option<i64> {
        eval_const(e).ok()?.as_i64()
    }
    pub(super) fn take(n: i64) -> usize {
        if n < 0 {
            usize::MAX
        } else {
            n as usize
        }
    }
    pub(super) fn skip(n: i64) -> usize {
        if n < 0 {
            0
        } else {
            n as usize
        }
    }
    match query.limit_clause.as_ref()? {
        LimitClause::LimitOffset { limit, offset, .. } => {
            let limit = bound(limit.as_ref()?)?;
            let offset = match offset {
                Some(o) => bound(&o.value)?,
                None => 0,
            };
            Some((skip(offset), take(limit)))
        }
        LimitClause::OffsetCommaLimit { offset, limit } => {
            Some((skip(bound(offset)?), take(bound(limit)?)))
        }
    }
}

/// LIMIT/OFFSET windowing shared by the FROM-less and general SELECT paths.
/// A negative limit or offset means "no limit"/"skip nothing" (SQLite);
/// non-integer values are an error, not a silently unlimited query.
pub(super) fn apply_limit_clause(
    limit_clause: &LimitClause,
    rows: Vec<Vec<Value>>,
) -> Result<Vec<Vec<Value>>> {
    pub(super) fn limit_count(e: &SqlExpr) -> Result<usize> {
        match eval_const(e)?.as_i64() {
            Some(n) if n >= 0 => Ok(n as usize),
            Some(_) => Ok(usize::MAX), // negative LIMIT means "no limit" (SQLite)
            None => err("LIMIT must be a non-negative integer"),
        }
    }
    pub(super) fn offset_count(e: &SqlExpr) -> Result<usize> {
        match eval_const(e)?.as_i64() {
            Some(n) if n >= 0 => Ok(n as usize),
            Some(_) => Ok(0),
            None => err("OFFSET must be a non-negative integer"),
        }
    }
    match limit_clause {
        LimitClause::LimitOffset { limit, offset, .. } => {
            let n = match limit {
                Some(e) => limit_count(e)?,
                None => usize::MAX,
            };
            let skip = match offset {
                Some(o) => offset_count(&o.value)?,
                None => 0,
            };
            Ok(rows.into_iter().skip(skip).take(n).collect())
        }
        // MySQL form `LIMIT <offset>, <count>` (order reversed).
        LimitClause::OffsetCommaLimit { offset, limit } => {
            let skip = offset_count(offset)?;
            let n = limit_count(limit)?;
            Ok(rows.into_iter().skip(skip).take(n).collect())
        }
    }
}

/// Canonical text form of a value (CAST AS TEXT, GROUP_CONCAT, ||).
/// Current wall clock as the canonical text form (UTC, RFC 3339-style
/// seconds precision) — the SYSDATE() compatibility function's value.
pub(super) fn now_ms_string() -> String {
    crate::value::format_timestamp_ms(crate::now_ms() as i64)
}

/// Days since epoch → (y, m, d) — Howard Hinnant's `civil_from_days`.
/// Shared with the server's backup UTC stamping; pinned by known-answer
/// tests in both engine and backup suites.
pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// True when the statement's projection or WHERE references the Oracle
/// ROWNUM pseudo-column.
pub(super) fn select_refs_rownum(select: &sqlparser::ast::Select) -> bool {
    let mut refs = false;
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(e) => refs |= refs_rownum(e),
            SelectItem::ExprWithAlias { expr, .. } => refs |= refs_rownum(expr),
            _ => {}
        }
    }
    if let Some(cond) = &select.selection {
        refs |= refs_rownum(cond);
    }
    refs
}

/// Walk an expression tree looking for the Oracle ROWNUM pseudo-column
/// reference (case-insensitive identifier). Non-recursive shapes recurse;
/// subqueries do NOT — a ROWNUM inside `(SELECT ... WHERE ROWNUM <= n)`
/// belongs to the subquery's own row stream and is legal in a write's WHERE.
pub(super) fn refs_rownum(e: &SqlExpr) -> bool {
    match e {
        SqlExpr::Identifier(i) => i.value.eq_ignore_ascii_case("ROWNUM"),
        SqlExpr::BinaryOp { left, right, .. } => refs_rownum(left) || refs_rownum(right),
        SqlExpr::UnaryOp { expr, .. } => refs_rownum(expr),
        SqlExpr::Nested(e)
        | SqlExpr::IsFalse(e)
        | SqlExpr::IsTrue(e)
        | SqlExpr::IsNotNull(e)
        | SqlExpr::IsNull(e)
        | SqlExpr::Cast { expr: e, .. } => refs_rownum(e),
        SqlExpr::Between {
            expr, low, high, ..
        } => refs_rownum(expr) || refs_rownum(low) || refs_rownum(high),
        SqlExpr::InList { expr, list, .. } => refs_rownum(expr) || list.iter().any(refs_rownum),
        SqlExpr::Like { expr, pattern, .. } | SqlExpr::ILike { expr, pattern, .. } => {
            refs_rownum(expr) || refs_rownum(pattern)
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand.as_deref().is_some_and(refs_rownum)
                || conditions
                    .iter()
                    .any(|w| refs_rownum(&w.condition) || refs_rownum(&w.result))
                || else_result.as_deref().is_some_and(refs_rownum)
        }
        SqlExpr::Function(f) => {
            if let sqlparser::ast::FunctionArguments::List(list) = &f.args {
                list.args.iter().any(|a| match a {
                    sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(inner),
                    ) => refs_rownum(inner),
                    _ => false,
                })
            } else {
                false
            }
        }
        _ => false,
    }
}

pub(crate) fn value_to_text(v: &Value) -> String {
    match v {
        Value::Str(s) => s.clone(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => {
            let s = format!("{f}");
            if s.contains('.') || s.contains('e') || s.contains('E') {
                s
            } else {
                format!("{f:.1}")
            }
        }
        Value::Decimal(d) => d.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
        // Timestamp renders as canonical fixed-width ISO text (Display),
        // not the debug enum shape.
        Value::Timestamp(ms) => crate::value::format_timestamp_ms(*ms),
        // BLOB text renders in its canonical `x'…'` literal form (Display)
        // — the Debug enum shape used to leak out here and the result could
        // never be parsed back.
        Value::Bytes(_) => v.to_string(),
        other => format!("{other:?}"),
    }
}

/// Best-effort CAST across the schemaless value model.
pub(crate) fn cast_value(v: Value, type_name: &str) -> Result<Value> {
    let t = type_name.to_uppercase();
    Ok(match v {
        Value::Null => Value::Null,
        // T-SQL BIT is the boolean-ish column type.
        v if t == "BIT" => match v {
            Value::Bool(_) => v,
            Value::Int(i) => Value::Bool(i != 0),
            Value::Float(f) => Value::Bool(f != 0.0),
            Value::Decimal(d) => Value::Bool(!d.is_zero()),
            Value::Str(s) => Value::Bool(s == "1" || s.eq_ignore_ascii_case("true")),
            other => other,
        },
        v if t.contains("INT") => match v {
            Value::Int(_) => v,
            Value::Float(f) => {
                // `f as i64` saturates silently; a financial conversion that
                // cannot be represented must fail loudly instead of turning
                // a huge amount into i64::MAX. (`i64::MAX as f64` rounds up
                // to 2^63, so `>=` there; `i64::MIN as f64` is exact.)
                if !f.is_finite() || f >= i64::MAX as f64 || f < i64::MIN as f64 {
                    return err(format!("cannot CAST {f} AS {type_name}: out of range"));
                }
                Value::Int(f as i64)
            }
            Value::Decimal(d) => Value::Int(d.trunc().to_i64().ok_or_else(|| {
                SqlError::Message(format!("cannot CAST {d} AS {type_name}: out of range"))
            })?),
            Value::Bool(b) => Value::Int(if b { 1 } else { 0 }),
            Value::Timestamp(ms) => Value::Int(ms),
            Value::Str(s) => Value::Int(
                s.trim()
                    .parse::<i64>()
                    .map_err(|_| SqlError::Message(format!("cannot CAST '{s}' AS {type_name}")))?,
            ),
            // A BLOB silently passing through as "the value" made every
            // downstream comparison/arithmetic silently wrong (`WHERE
            // CONVERT(INT, x'01') = 1` matched nothing, forever).
            Value::Bytes(_) => {
                return err(format!(
                    "cannot CAST a BLOB AS {type_name} (decode it explicitly first)"
                ))
            }
            other => other,
        },
        v if t.contains("CHAR") || t.contains("TEXT") || t.contains("STRING") => {
            Value::Str(value_to_text(&v))
        }
        // TIMESTAMP/DATETIME: strings parse in any accepted timestamp form
        // (malformed → error, like the numeric casts); integers are UTC
        // milliseconds. Timestamp→Int (ms) has its arm in the INT branch.
        v if t.contains("TIMESTAMP") || t.contains("DATETIME") => match v {
            Value::Timestamp(_) => v,
            Value::Int(i) => {
                // Domain check: an out-of-range instant has no canonical
                // text form, so `value_literal` could never replay it
                // (backup restore / join snapshot would fail on this row).
                if !crate::value::is_valid_timestamp_ms(i) {
                    return err(format!("cannot CAST {i} AS {type_name}: out of range"));
                }
                Value::Timestamp(i)
            }
            Value::Str(s) => {
                Value::Timestamp(crate::value::parse_timestamp_ms(&s).ok_or_else(|| {
                    SqlError::Message(format!("cannot CAST '{s}' AS {type_name}: not a timestamp"))
                })?)
            }
            other => {
                return err(format!(
                    "cannot CAST {} AS {type_name}: cast from {} is not defined",
                    value_to_text(&other),
                    other.type_name()
                ))
            }
        },
        // DATE: a real conversion with T-SQL semantics — parse/truncate to
        // UTC midnight (`CONVERT(DATE, getdate())` is the classic idiom).
        // This used to pass the input through untouched, storing a plain
        // string with no date semantics at all.
        v if t == "DATE" => match v {
            Value::Timestamp(ms) => Value::Timestamp(ms - ms.rem_euclid(86_400_000)),
            Value::Str(s) => {
                let ms = crate::value::parse_timestamp_ms(&s).ok_or_else(|| {
                    SqlError::Message(format!("cannot CAST '{s}' AS {type_name}: not a date"))
                })?;
                Value::Timestamp(ms - ms.rem_euclid(86_400_000))
            }
            other => {
                return err(format!(
                    "cannot CAST {} AS {type_name}: cast from {} is not defined",
                    value_to_text(&other),
                    other.type_name()
                ))
            }
        },
        v if t.contains("BOOL") => match v {
            Value::Bool(_) => v,
            Value::Int(i) => Value::Bool(i != 0),
            Value::Decimal(d) => Value::Bool(!d.is_zero()),
            Value::Str(s) => Value::Bool(s == "true"),
            other => other,
        },
        // Exact DECIMAL/NUMERIC: text and integer sources convert losslessly;
        // a Float source only promises its exact binary value.
        v if t.contains("DECIMAL") || t.contains("NUMERIC") => match v {
            Value::Decimal(_) => v,
            Value::Int(i) => Value::Decimal(Decimal::from(i)),
            Value::Float(f) => Value::Decimal(
                Decimal::from_f64_retain(f)
                    .ok_or_else(|| SqlError::Message(format!("cannot CAST {f} AS {type_name}")))?,
            ),
            Value::Str(s) => Value::Decimal(
                s.trim()
                    .parse::<Decimal>()
                    .map_err(|_| SqlError::Message(format!("cannot CAST '{s}' AS {type_name}")))?,
            ),
            Value::Bytes(_) => {
                return err(format!(
                    "cannot CAST a BLOB AS {type_name} (decode it explicitly first)"
                ))
            }
            other => other,
        },
        v if t.contains("REAL") || t.contains("DOUBLE") || t.contains("FLOAT") => match v {
            Value::Float(_) => v,
            Value::Int(i) => Value::Float(i as f64),
            Value::Decimal(d) => Value::Float(d.to_f64().unwrap_or(f64::NAN)),
            Value::Str(s) => Value::Float(
                s.trim()
                    .parse::<f64>()
                    .map_err(|_| SqlError::Message(format!("cannot CAST '{s}' AS {type_name}")))?,
            ),
            Value::Bytes(_) => {
                return err(format!(
                    "cannot CAST a BLOB AS {type_name} (decode it explicitly first)"
                ))
            }
            other => other,
        },
        // BLOB: bytes pass through; text keeps its UTF-8 bytes (SQLite-style
        // CAST semantics that drivers rely on for `CAST(@p AS BLOB)`).
        v if t.contains("BLOB") || t.contains("BYTES") || t.contains("BINARY") => match v {
            Value::Bytes(_) => v,
            Value::Str(s) => Value::Bytes(s.into_bytes()),
            other => other,
        },
        // Unknown type: pass through unchanged (types are documentation here).
        v => v,
    })
}

/// Scalar (non-aggregate) function library. Arity is checked up front:
/// a malformed call (`SELECT UPPER()`) must error, not panic on indexing.
pub(super) fn scalar_function(name: &str, args: &[Value]) -> Result<Value> {
    pub(super) fn arg<'a>(args: &'a [Value], i: usize, name: &str) -> Result<&'a Value> {
        args.get(i).ok_or_else(|| {
            SqlError::Message(format!("function {name} requires argument {}", i + 1))
        })
    }
    pub(super) fn exact_arity(name: &str, args: &[Value], n: usize) -> Result<()> {
        if args.len() != n {
            return Err(SqlError::Message(format!(
                "function {name} takes exactly {n} argument(s), got {}",
                args.len()
            )));
        }
        Ok(())
    }
    let null_prop = |v: &Value| matches!(v, Value::Null);
    Ok(match name {
        "NOW" | "CURRENT_TIMESTAMP" | "NOW_MS" => {
            exact_arity(name, args, 0)?;
            Value::Timestamp(crate::now_ms() as i64)
        }
        "UPPER" | "UCASE" => {
            exact_arity(name, args, 1)?;
            let v = arg(args, 0, name)?;
            if null_prop(v) {
                Value::Null
            } else {
                Value::Str(value_to_text(v).to_uppercase())
            }
        }
        "LOWER" | "LCASE" => {
            exact_arity(name, args, 1)?;
            let v = arg(args, 0, name)?;
            if null_prop(v) {
                Value::Null
            } else {
                Value::Str(value_to_text(v).to_lowercase())
            }
        }
        "LENGTH" | "LEN" => {
            exact_arity(name, args, 1)?;
            Value::Int(match arg(args, 0, name)? {
                Value::Null => return Ok(Value::Null),
                Value::Str(s) => {
                    if name == "LEN" {
                        // T-SQL LEN: trailing blanks are not counted.
                        s.trim_end_matches(' ').chars().count() as i64
                    } else {
                        s.chars().count() as i64
                    }
                }
                Value::Bytes(b) => b.len() as i64,
                other => value_to_text(other).chars().count() as i64,
            })
        }
        // SQL standard aliases for character length; MySQL/PostgreSQL
        // programs use them interchangeably with LENGTH.
        "CHAR_LENGTH" | "CHARACTER_LENGTH" => {
            exact_arity(name, args, 1)?;
            match arg(args, 0, name)? {
                Value::Null => Value::Null,
                v => Value::Int(value_to_text(v).chars().count() as i64),
            }
        }
        // Byte length: text counts its UTF-8 bytes (PostgreSQL semantics).
        "OCTET_LENGTH" => {
            exact_arity(name, args, 1)?;
            Value::Int(match arg(args, 0, name)? {
                Value::Null => return Ok(Value::Null),
                Value::Bytes(b) => b.len() as i64,
                v => value_to_text(v).len() as i64,
            })
        }
        // MySQL LOCATE(substr, str [, start]): 1-based character position of
        // the first occurrence at/after `start`, 0 when absent.
        "LOCATE" => {
            if args.len() < 2 || args.len() > 3 {
                return err(format!(
                    "function LOCATE takes 2 or 3 arguments, got {}",
                    args.len()
                ));
            }
            if args.iter().any(null_prop) {
                return Ok(Value::Null);
            }
            let needle = value_to_text(arg(args, 0, name)?);
            let hay = value_to_text(arg(args, 1, name)?);
            let start = match args.get(2) {
                Some(Value::Int(n)) => *n,
                Some(other) => {
                    return err(format!(
                        "function LOCATE: start must be an integer, got {}",
                        other.type_name()
                    ))
                }
                None => 1,
            };
            let chars: Vec<char> = hay.chars().collect();
            let skip = start.saturating_sub(1);
            if skip > chars.len() as i64 {
                return Ok(Value::Int(0));
            }
            let tail: String = chars[skip as usize..].iter().collect();
            Value::Int(match tail.find(&needle) {
                Some(b) => skip + tail[..b].chars().count() as i64 + 1,
                None => 0,
            })
        }
        "ABS" => {
            exact_arity(name, args, 1)?;
            match arg(args, 0, name)? {
                Value::Int(i) => Value::Int(i.abs()),
                Value::Float(f) => Value::Float(f.abs()),
                Value::Decimal(d) => Value::Decimal(d.abs()),
                Value::Null => Value::Null,
                other => return err(format!("ABS of non-numeric: {other:?}")),
            }
        }
        "ROUND" => {
            if args.is_empty() || args.len() > 2 {
                return err(format!(
                    "function ROUND takes 1 or 2 arguments, got {}",
                    args.len()
                ));
            }
            if null_prop(arg(args, 0, name)?) {
                Value::Null
            } else if let Value::Decimal(d) = arg(args, 0, name)? {
                // Exact rounding: half away from zero (SQL Server semantics).
                // A negative length rounds to tens/hundreds (T-SQL); the
                // decimal crate only rounds toward positive scale, so
                // pre-scale by the magnitude, round at scale 0, scale back.
                let digits = match args.get(1) {
                    // Clamp like the Float branch: a huge i64 truncated to
                    // u32 picked an arbitrary scale (2^32 → 0), and
                    // i64::MIN negated below overflowed in debug builds.
                    Some(Value::Int(d)) => (*d).clamp(-15, 28),
                    _ => 0,
                };
                if digits >= 0 {
                    Value::Decimal(d.round_dp_with_strategy(
                        (digits as u32).min(28),
                        rust_decimal::RoundingStrategy::MidpointAwayFromZero,
                    ))
                } else {
                    let shift_i64 = 10i64
                        .checked_pow(((-digits).min(15)) as u32)
                        .unwrap_or(1_000_000_000_000_000);
                    let shift = Decimal::from(shift_i64);
                    let shifted = (d / shift).round_dp_with_strategy(
                        0,
                        rust_decimal::RoundingStrategy::MidpointAwayFromZero,
                    ) * shift;
                    Value::Decimal(shifted)
                }
            } else {
                let x = as_f64(arg(args, 0, name)?)?;
                let digits = match args.get(1) {
                    // Clamp into f64-precision territory: the raw i64 would
                    // wrap through powi's i32 and round to a random scale.
                    Some(Value::Int(d)) => (*d).clamp(-15, 15),
                    _ => 0,
                };
                let m = 10f64.powi(digits as i32);
                Value::Float((x * m).round() / m)
            }
        }
        // MySQL MOD(a,b) — identical semantics to the `%` operator (NULL on
        // division by zero, exact for DECIMAL).
        "MOD" => {
            exact_arity(name, args, 2)?;
            arith(
                args[0].clone(),
                &sqlparser::ast::BinaryOperator::Modulo,
                args[1].clone(),
            )?
        }
        // MySQL/PowerShell spelling of POWER: one numeric implementation.
        "POW" => {
            return crate::tsql::scalar("POWER", args)
                .expect("POWER is a registered T-SQL scalar function")
        }
        // MySQL/SQL Server TRUNCATE(x, digits): truncate toward zero (never
        // rounds), negative digits cut at tens/hundreds.
        "TRUNCATE" => {
            exact_arity(name, args, 2)?;
            let v = arg(args, 0, name)?;
            let digits = match arg(args, 1, name)? {
                Value::Int(d) => (*d).clamp(-15, 28),
                other => {
                    return err(format!(
                        "function TRUNCATE: digits must be an integer, got {}",
                        other.type_name()
                    ))
                }
            };
            match v {
                Value::Null => Value::Null,
                Value::Int(i) => {
                    if digits >= 0 {
                        Value::Int(*i)
                    } else {
                        let shift = 10i64
                            .checked_pow(((-digits).min(15)) as u32)
                            .unwrap_or(1_000_000_000_000_000);
                        Value::Int((*i / shift) * shift)
                    }
                }
                Value::Decimal(d) => {
                    if digits >= 0 {
                        Value::Decimal(d.round_dp_with_strategy(
                            (digits as u32).min(28),
                            rust_decimal::RoundingStrategy::ToZero,
                        ))
                    } else {
                        let shift = Decimal::from(
                            10i64
                                .checked_pow(((-digits).min(15)) as u32)
                                .unwrap_or(1_000_000_000_000_000),
                        );
                        Value::Decimal(
                            (d / shift)
                                .round_dp_with_strategy(0, rust_decimal::RoundingStrategy::ToZero)
                                * shift,
                        )
                    }
                }
                Value::Float(f) => {
                    let m = 10f64.powi(digits as i32);
                    Value::Float((f * m).trunc() / m)
                }
                other => return err(format!("TRUNCATE of non-numeric: {}", other.type_name())),
            }
        }
        "COALESCE" | "IFNULL" | "ISNULL" => {
            if args.is_empty() {
                return err(format!("function {name} requires at least 1 argument"));
            }
            args.iter()
                .find(|v| !matches!(v, Value::Null))
                .cloned()
                .unwrap_or(Value::Null)
        }
        "NULLIF" => {
            exact_arity(name, args, 2)?;
            let a = arg(args, 0, name)?;
            let b = arg(args, 1, name)?;
            // Same comparison funnel as `=` (cmp_coerced): a Timestamp↔
            // parseable-string pair must null out here exactly as the
            // equality predicate matches it.
            if cmp_coerced(a, b) == Some(Ordering::Equal) {
                Value::Null
            } else {
                a.clone()
            }
        }
        "SUBSTR" | "SUBSTRING" => {
            if args.is_empty() || args.len() > 3 {
                return err(format!(
                    "function {name} takes 1 to 3 arguments, got {}",
                    args.len()
                ));
            }
            if null_prop(arg(args, 0, name)?) {
                Value::Null
            } else {
                let s: Vec<char> = value_to_text(arg(args, 0, name)?).chars().collect();
                // SQLite rules: 1-based start; negative start counts from
                // the end; start 0 sits at a virtual pre-string position
                // (so `substr('abcde',0,2)` yields 'a' — one char shorter);
                // negative length selects the |len| characters *before*
                // the start position.
                let n = s.len() as i64;
                let y = match args.get(1) {
                    Some(Value::Int(i)) => *i,
                    _ => 1,
                };
                let z = match args.get(2) {
                    Some(Value::Int(i)) => *i,
                    _ => i64::MAX,
                };
                let (b, take) = if y < 0 {
                    let b0 = n + y;
                    if b0 < 0 {
                        (0, z + b0)
                    } else {
                        (b0, z)
                    }
                } else if y > 0 {
                    (y - 1, z)
                } else {
                    (0, z - 1)
                };
                let (lo, hi) = if take < 0 {
                    ((b + take).max(0), b.max(0))
                } else {
                    (b.max(0), b.saturating_add(take))
                };
                let lo = lo.clamp(0, n) as usize;
                let hi = hi.clamp(0, n) as usize;
                Value::Str(if lo < hi {
                    s[lo..hi].iter().collect()
                } else {
                    String::new()
                })
            }
        }
        "TRIM" | "LTRIM" | "RTRIM" => {
            exact_arity(name, args, 1)?;
            let v = arg(args, 0, name)?;
            if null_prop(v) {
                return Ok(Value::Null);
            }
            let text = value_to_text(v);
            Value::Str(match name {
                "TRIM" => text.trim().to_string(),
                "LTRIM" => text.trim_start().to_string(),
                _ => text.trim_end().to_string(),
            })
        }
        "CONCAT" => {
            // T-SQL CONCAT: NULL arguments are treated as empty strings
            // (only an all-NULL argument list yields the empty string).
            // `||` keeps NULL propagation — the two are deliberately
            // different operators.
            Value::Str(
                args.iter()
                    .map(|v| {
                        if matches!(v, Value::Null) {
                            String::new()
                        } else {
                            value_to_text(v)
                        }
                    })
                    .collect(),
            )
        }
        "TYPEOF" => {
            exact_arity(name, args, 1)?;
            Value::Str(
                match arg(args, 0, name)? {
                    Value::Null => "null",
                    Value::Bool(_) => "bool",
                    Value::Int(_) => "integer",
                    Value::Float(_) => "float",
                    Value::Decimal(_) => "decimal",
                    Value::Timestamp(_) => "timestamp",
                    Value::Str(_) => "text",
                    Value::Bytes(_) => "blob",
                    Value::Array(_) => "array",
                    Value::Object(_) => "object",
                }
                .into(),
            )
        }
        "JSON_VALID" => {
            exact_arity(name, args, 1)?;
            match arg(args, 0, name)? {
                Value::Str(s) => Value::Bool(crate::json::from_str(s).is_ok()),
                _ => Value::Bool(false),
            }
        }
        // Array membership for EF's entity primitive-collection Contains:
        // JSON_ARRAY_CONTAINS(json_text, needle) — NULL text/parse failure and
        // non-array JSON are false (subquery-existence semantics).
        "JSON_ARRAY_CONTAINS" => {
            exact_arity(name, args, 2)?;
            let (haystack, needle) = (arg(args, 0, name)?, arg(args, 1, name)?);
            // Membership uses the same comparison funnel as `IN`/`=`
            // (cmp_coerced): EF serializes DateTime elements to ISO strings
            // while the typed needle arrives as a Timestamp — the raw total
            // order never equates the two, silently emptying the query.
            let contains = |items: &[Value]| {
                items
                    .iter()
                    .any(|it| cmp_coerced(it, needle) == Some(Ordering::Equal))
            };
            let items: &[Value] = match haystack {
                Value::Null => return Ok(Value::Null),
                Value::Array(items) => items,
                Value::Str(s) => match crate::json::from_str(s) {
                    Ok(Value::Array(items)) => {
                        return Ok(Value::Bool(contains(&items)));
                    }
                    _ => return Ok(Value::Bool(false)),
                },
                _ => return Ok(Value::Bool(false)),
            };
            Value::Bool(contains(items))
        }
        // ---- Oracle-style function family (compatibility surface) ----
        "NVL" => {
            exact_arity(name, args, 2)?;
            match arg(args, 0, name)? {
                Value::Null => arg(args, 1, name)?.clone(),
                v => v.clone(),
            }
        }
        "NVL2" => {
            exact_arity(name, args, 3)?;
            match arg(args, 0, name)? {
                Value::Null => arg(args, 2, name)?.clone(),
                _ => arg(args, 1, name)?.clone(),
            }
        }
        "DECODE" => {
            // Oracle DECODE: (expr, search1, result1, … [, default]). Oracle
            // treats NULL as equal to NULL in the comparisons; no match and
            // no default yields NULL.
            if args.len() < 3 {
                return err(format!(
                    "function {name} takes at least 3 arguments, got {}",
                    args.len()
                ));
            }
            let expr = &args[0];
            let mut i = 1;
            while i + 1 < args.len() {
                let s = &args[i];
                let matched = match (expr, s) {
                    (Value::Null, Value::Null) => true,
                    (Value::Null, _) | (_, Value::Null) => false,
                    // Same comparison funnel as `=` (cmp_coerced): a
                    // Timestamp↔parseable-string pair must decode-match
                    // exactly as the equality predicate matches it.
                    (a, b) => cmp_coerced(a, b) == Some(Ordering::Equal),
                };
                if matched {
                    return Ok(args[i + 1].clone());
                }
                i += 2;
            }
            if args.len().is_multiple_of(2) {
                return Ok(args[args.len() - 1].clone());
            }
            Value::Null
        }
        "INSTR" => {
            // 1-based position of the first occurrence; 0 when absent.
            exact_arity(name, args, 2)?;
            if args.iter().any(|v| matches!(v, Value::Null)) {
                return Ok(Value::Null);
            }
            let (Value::Str(hay), Value::Str(needle)) = (arg(args, 0, name)?, arg(args, 1, name)?)
            else {
                return err(format!("function {name} requires text arguments"));
            };
            let pos = hay
                .find(needle.as_str())
                .map(|byte_idx| hay[..byte_idx].chars().count() + 1)
                .unwrap_or(0);
            Value::Int(pos as i64)
        }
        "LPAD" | "RPAD" => {
            // Pad/truncate `str` to `len` display characters with `pad`
            // (default a single space).
            if args.len() < 2 || args.len() > 3 {
                return err(format!(
                    "function {name} takes 2 or 3 arguments, got {}",
                    args.len()
                ));
            }
            if args.iter().any(|v| matches!(v, Value::Null)) {
                return Ok(Value::Null);
            }
            let Value::Str(s) = arg(args, 0, name)? else {
                return err(format!("function {name} requires a text argument"));
            };
            let len = match arg(args, 1, name)? {
                Value::Int(n) if *n >= 0 => *n as usize,
                Value::Int(n) => {
                    return err(format!(
                        "function {name}: length must be non-negative ({n})"
                    ))
                }
                _ => return err(format!("function {name}: length must be an integer")),
            };
            // Bound the padded width: an astronomic length used to reach
            // String::with_capacity directly and abort the whole process
            // on allocation failure (single-writer engine = node-wide DoS
            // from one SELECT). Documents cap out at MAX_DOC_SIZE, so a
            // 1 MiB character budget is far above every legitimate use.
            if len > MAX_PAD_LENGTH {
                return err(format!(
                    "function {name}: length exceeds the supported maximum ({MAX_PAD_LENGTH})"
                ));
            }
            let pad = match args.get(2) {
                Some(Value::Str(p)) if !p.is_empty() => p.clone(),
                Some(Value::Str(_)) => " ".to_string(),
                _ => " ".to_string(),
            };
            let chars: Vec<char> = s.chars().collect();
            if chars.len() >= len {
                return Ok(Value::Str(chars[..len].iter().collect()));
            }
            let fill: Vec<char> = pad.chars().collect();
            let mut out = String::with_capacity(len * 2);
            let need = len - chars.len();
            if name == "LPAD" {
                for k in 0..need {
                    out.push(fill[k % fill.len()]);
                }
                out.extend(chars);
            } else {
                out.extend(chars);
                for k in 0..need {
                    out.push(fill[k % fill.len()]);
                }
            }
            Value::Str(out)
        }
        "GREATEST" | "LEAST" => {
            // Oracle: any NULL argument → NULL; ties return the first.
            if args.is_empty() {
                return err(format!("function {name} requires at least 1 argument"));
            }
            if args.iter().any(|v| matches!(v, Value::Null)) {
                return Ok(Value::Null);
            }
            let mut best = args[0].clone();
            for v in &args[1..] {
                // Prefer the coercion funnel (a Timestamp↔parseable-string
                // pair orders by time, like the predicates do); the total
                // order settles cross-type mixes that never coerce.
                let o = cmp_coerced(v, &best).unwrap_or_else(|| Value::cmp_values(v, &best));
                let better = if name == "GREATEST" {
                    o == Ordering::Greater
                } else {
                    o == Ordering::Less
                };
                if better {
                    best = v.clone();
                }
            }
            best
        }
        "TO_NUMBER" => {
            exact_arity(name, args, 1)?;
            match arg(args, 0, name)?.clone() {
                Value::Int(_) | Value::Float(_) | Value::Decimal(_) => args[0].clone(),
                Value::Str(s) => {
                    let t = s.trim();
                    if let Ok(i) = t.parse::<i64>() {
                        Value::Int(i)
                    } else {
                        // Oracle NUMBER 是精确数值:非整数文本进 DECIMAL 而非 f64
                        match t.parse::<Decimal>() {
                            Ok(d) => Value::Decimal(d),
                            Err(_) => {
                                return err(format!("TO_NUMBER: invalid number {s:?}"));
                            }
                        }
                    }
                }
                _ => return err("function TO_NUMBER requires a text argument"),
            }
        }
        "TO_CHAR" => {
            // Single-argument form: value → text. The Oracle format-mask
            // form requires a typed date/number system and is rejected.
            if args.is_empty() || args.len() > 2 {
                return err(format!(
                    "function {name} takes 1 or 2 arguments, got {}",
                    args.len()
                ));
            }
            if args.len() == 2 {
                return err("TO_CHAR format masks are not supported");
            }
            match arg(args, 0, name)? {
                Value::Str(s) => Value::Str(s.clone()),
                Value::Decimal(d) => Value::Str(d.to_string()),
                other => Value::Str(crate::json::to_string(other).trim_matches('"').to_string()),
            }
        }
        "SYSDATE" => {
            exact_arity(name, args, 0)?;
            Value::Str(now_ms_string())
        }
        "JSON_EXTRACT" | "JSON_TYPE" => {
            // Documents live as JSON text, so point reads into them are the
            // JSON function family's job: extract returns the sub-value
            // (object/array stay structured), type names it SQLite-style.
            // Malformed text or a missing path yields NULL — a query over
            // heterogeneous documents must not abort on one bad row.
            let want_type = name == "JSON_TYPE";
            if args.is_empty() || args.len() > 2 {
                return err(format!(
                    "function {name} takes 1 or 2 arguments, got {}",
                    args.len()
                ));
            }
            let Value::Str(text) = arg(args, 0, name)? else {
                return Ok(Value::Null);
            };
            let parsed = match crate::json::from_str(text) {
                Ok(v) => v,
                Err(_) => return Ok(Value::Null),
            };
            let target = if args.len() == 2 {
                let Value::Str(path) = arg(args, 1, name)? else {
                    return err(format!("function {name} path must be text"));
                };
                match json_path_lookup(&parsed, path) {
                    Some(v) => v.clone(),
                    None => return Ok(Value::Null),
                }
            } else {
                parsed
            };
            if want_type {
                Value::Str(json_type_name(&target).into())
            } else {
                target
            }
        }
        // T-SQL function families live in their own module; None means the
        // name is genuinely unknown to the whole engine.
        other => {
            return match crate::tsql::scalar(other, args) {
                Some(res) => res,
                None => err(format!("unknown function: {other}")),
            }
        }
    })
}

/// Navigate a JSON document by a minimal path grammar: `$` root, `.key`
/// object members, `[n]` array indexes — the point-read subset of the
/// SQLite/JQ path syntax documents stored as JSON text need.
pub(super) fn json_path_lookup<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
    let p = path.trim();
    if !p.starts_with('$') {
        return None;
    }
    let mut cur = root;
    let mut rest = &p[1..];
    while !rest.is_empty() {
        if let Some(stripped) = rest.strip_prefix('.') {
            let end = stripped.find(['.', '[']).unwrap_or(stripped.len());
            let key = &stripped[..end];
            if key.is_empty() {
                return None;
            }
            cur = match cur {
                Value::Object(o) => o.get(key)?,
                _ => return None,
            };
            rest = &stripped[end..];
        } else {
            let stripped = rest.strip_prefix('[')?;
            let end = stripped.find(']')?;
            let idx: usize = stripped[..end].parse().ok()?;
            cur = match cur {
                Value::Array(a) => a.get(idx)?,
                _ => return None,
            };
            rest = &stripped[end + 1..];
        }
    }
    Some(cur)
}

/// SQLite-style JSON type names for JSON_TYPE.
pub(super) fn json_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Int(_) => "integer",
        Value::Float(_) => "real",
        Value::Decimal(_) => "real",
        Value::Timestamp(_) => "timestamp",
        Value::Str(_) => "text",
        Value::Bytes(_) => "blob",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// ORDER BY comparison honoring NULLS FIRST/LAST. Default places NULLs as
/// the smallest value (first on ASC, last on DESC).
pub(super) fn cmp_maybe_null(a: &Value, b: &Value, nulls_first: Option<bool>) -> Ordering {
    let nulls_first = nulls_first.unwrap_or(true);
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (_, Value::Null) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        _ => Value::cmp_values(a, b),
    }
}

/// One candidate row of the unindexed ORDER BY top-K heap: extracted sort
/// keys, heap locator, scan sequence (stable tie-break) and the shared
/// per-key directions. The ordering is the row's `ORDER BY` position, so
/// "better" compares less: `BinaryHeap` keeps the worst window entry on top
/// and `into_sorted_vec` yields best first.
pub(super) struct OrderKeyed<'a> {
    pub(super) keys: Vec<Value>,
    pub(super) loc: u64,
    pub(super) seq: u64,
    pub(super) dirs: &'a [(bool, Option<bool>)],
}

impl PartialEq for OrderKeyed<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for OrderKeyed<'_> {}

impl PartialOrd for OrderKeyed<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderKeyed<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        for (i, (asc, nulls_first)) in self.dirs.iter().enumerate() {
            let ord = cmp_maybe_null(&self.keys[i], &other.keys[i], *nulls_first);
            if ord != Ordering::Equal {
                return if *asc { ord } else { ord.reverse() };
            }
        }
        self.seq.cmp(&other.seq)
    }
}

pub(super) fn binop(l: Value, op: &BinaryOperator, r: Value) -> Result<Value> {
    use BinaryOperator::*;
    Ok(match op {
        // T-SQL `+` concatenates when both operands are text (NULL
        // propagates through `arith` when either side is NULL); a
        // string/number mix falls through to numeric addition, where the
        // string must convert to a number (SQL Server implicit conversion)
        // or the statement errors loudly. These arms must precede the
        // generic arith arm — `Plus` matches it first otherwise.
        Plus if matches!(l, Value::Str(_)) && matches!(r, Value::Str(_)) => {
            Value::Str(format!("{}{}", value_to_text(&l), value_to_text(&r)))
        }
        Plus if matches!(l, Value::Str(_)) || matches!(r, Value::Str(_)) => {
            if matches!(l, Value::Null) || matches!(r, Value::Null) {
                Value::Null
            } else {
                let num = |v: Value| -> Result<Value> {
                    match &v {
                        Value::Str(s) => {
                            if let Ok(i) = s.trim().parse::<i64>() {
                                Ok(Value::Int(i))
                            } else {
                                s.trim()
                                    .parse::<Decimal>()
                                    .map(Value::Decimal)
                                    .map_err(|_| {
                                        SqlError::Message(format!(
                                            "cannot convert {s:?} to a number for '+'"
                                        ))
                                    })
                            }
                        }
                        other => Ok(other.clone()),
                    }
                };
                arith(num(l)?, op, num(r)?)?
            }
        }
        Plus | Minus | Multiply | Divide | Modulo => arith(l, op, r)?,
        // T-SQL bitwise operators: 64-bit two's complement, integers (and
        // bools as T-SQL BIT) only.
        BitwiseOr => bitwise(l, r, |a, b| a | b)?,
        BitwiseAnd => bitwise(l, r, |a, b| a & b)?,
        BitwiseXor => bitwise(l, r, |a, b| a ^ b)?,
        StringConcat => match (l, r) {
            (Value::Null, _) | (_, Value::Null) => Value::Null,
            (a, b) => Value::Str(format!("{}{}", value_to_text(&a), value_to_text(&b))),
        },
        Eq | NotEq | Lt | LtEq | Gt | GtEq => {
            // Coercion only (timestamp vs parseable string); it must never
            // touch NULL — NULL is a comparable value here (NULL = NULL is
            // true; hash-join and unique-check semantics rely on that).
            let Some((l, r)) = coerce_timestamp_operands(l, r) else {
                // A string that is not a timestamp compared with a
                // TIMESTAMP: unknown, not a silent wrong ordering.
                return Ok(Value::Null);
            };
            let ord = Value::cmp_values(&l, &r);
            let out = match op {
                Eq => ord == Ordering::Equal,
                NotEq => ord != Ordering::Equal,
                Lt => ord == Ordering::Less,
                LtEq => ord != Ordering::Greater,
                Gt => ord == Ordering::Greater,
                GtEq => ord != Ordering::Less,
                _ => unreachable!("matched above"),
            };
            Value::Bool(out)
        }
        // Three-valued AND over the engine's one UNKNOWN source (the
        // uncoercable Timestamp↔string/numeric comparison yields Null):
        // FALSE dominates, TRUE keeps the unknown, and the result stays
        // Null — WHERE already filters anything that is not Bool(true),
        // so a row whose predicate is unknown is dropped instead of
        // erroring the whole statement (`WHERE ts = s AND id = 1` used to
        // fail on the first row with an unparseable s).
        And => match (l, r) {
            (Value::Bool(a), Value::Bool(b)) => Value::Bool(a && b),
            (Value::Bool(false), Value::Null) | (Value::Null, Value::Bool(false)) => {
                Value::Bool(false)
            }
            (Value::Bool(true), Value::Null)
            | (Value::Null, Value::Bool(true))
            | (Value::Null, Value::Null) => Value::Null,
            _ => return err("AND requires booleans"),
        },
        // Three-valued OR: TRUE dominates, FALSE keeps the unknown.
        Or => match (l, r) {
            (Value::Bool(a), Value::Bool(b)) => Value::Bool(a || b),
            (Value::Bool(true), Value::Null) | (Value::Null, Value::Bool(true)) => {
                Value::Bool(true)
            }
            (Value::Bool(false), Value::Null)
            | (Value::Null, Value::Bool(false))
            | (Value::Null, Value::Null) => Value::Null,
            _ => return err("OR requires booleans"),
        },
        other => return err(format!("unsupported operator: {other}")),
    })
}

/// T-SQL bitwise operators (`& | ^`): integers only; BOOLs coerce like
/// T-SQL BIT (1/0) and the result is an integer.
pub(super) fn bitwise(l: Value, r: Value, f: impl Fn(i64, i64) -> i64) -> Result<Value> {
    let as_int = |v: &Value| -> Option<i64> {
        match v {
            Value::Int(i) => Some(*i),
            Value::Bool(b) => Some(i64::from(*b)),
            _ => None,
        }
    };
    match (&l, &r) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => match (as_int(&l), as_int(&r)) {
            (Some(a), Some(b)) => Ok(Value::Int(f(a, b))),
            _ => err("bitwise operators require integer operands"),
        },
    }
}

/// Comparison-side TIMESTAMP coercion: a string operand against a TIMESTAMP
/// parses into the comparison (any accepted timestamp form — `WHERE ts >
/// '2026-09-16'` works); an unparseable string yields None → the comparison
/// is NULL. cmp_values itself never mixes the bands (a parseable string
/// inserted into the time order would break transitivity), so this is the
/// single funnel where the predicate-level rule lives — the index-probe
/// bound builder shares it (see `promote_timestamp_bound`).
pub(super) fn coerce_timestamp_operands(l: Value, r: Value) -> Option<(Value, Value)> {
    use crate::value::parse_timestamp_ms;
    match (&l, &r) {
        (Value::Timestamp(t), Value::Str(s)) => {
            parse_timestamp_ms(s).map(|p| (Value::Timestamp(*t), Value::Timestamp(p)))
        }
        (Value::Str(s), Value::Timestamp(t)) => {
            parse_timestamp_ms(s).map(|p| (Value::Timestamp(p), Value::Timestamp(*t)))
        }
        (Value::Timestamp(_), Value::Int(_))
        | (Value::Int(_), Value::Timestamp(_))
        | (Value::Timestamp(_), Value::Decimal(_))
        | (Value::Decimal(_), Value::Timestamp(_))
        | (Value::Timestamp(_), Value::Float(_))
        | (Value::Float(_), Value::Timestamp(_)) => {
            // No silent numeric-date semantics: CAST(ms AS TIMESTAMP)
            // makes the intent explicit.
            None
        }
        _ => Some((l, r)),
    }
}

/// NULL-safe equality for `IS [NOT] DISTINCT FROM` (SQL:2003). The engine's
/// comparison is total — `NULL` is a sortable value, so `NULL` compares
/// equal to `NULL` and unequal to everything else — which is exactly the
/// DISTINCT FROM truth table. Comparison goes through the TIMESTAMP↔string
/// coercion funnel so `ts IS NOT DISTINCT FROM '2026-…'` behaves like `ts =
/// '2026-…'`; an uncoercable pair is simply not equal.
pub(super) fn values_equal_null_safe(a: &Value, b: &Value) -> bool {
    cmp_coerced(a, b) == Some(Ordering::Equal)
}

pub(super) fn cmp_coerced(a: &Value, b: &Value) -> Option<Ordering> {
    coerce_timestamp_operands(a.clone(), b.clone()).map(|(a, b)| Value::cmp_values(&a, &b))
}

pub(super) fn arith(l: Value, op: &BinaryOperator, r: Value) -> Result<Value> {
    use BinaryOperator::*;
    if matches!(l, Value::Null) || matches!(r, Value::Null) {
        return Ok(Value::Null);
    }
    // TIMESTAMP arithmetic: ± INTEGER shifts by milliseconds,
    // TIMESTAMP - TIMESTAMP yields the duration in milliseconds
    // (checked: i64 ms covers ~292 million years either way, but a
    // pathological literal must not panic). Timestamp + Timestamp and
    // numeric↔timestamp mixing are errors — CAST explicitly.
    if matches!(l, Value::Timestamp(_)) || matches!(r, Value::Timestamp(_)) {
        // Result domain: an instant outside 0001..=9999 has no canonical
        // text form (value_literal could not replay it), so it degrades to
        // NULL like any other out-of-range arithmetic result.
        let ts = |ms: i64| {
            if crate::value::is_valid_timestamp_ms(ms) {
                Value::Timestamp(ms)
            } else {
                Value::Null
            }
        };
        return Ok(match (op, l, r) {
            (Plus, Value::Timestamp(t), Value::Int(n))
            | (Plus, Value::Int(n), Value::Timestamp(t)) => {
                t.checked_add(n).map(ts).unwrap_or(Value::Null)
            }
            (Minus, Value::Timestamp(t), Value::Int(n)) => {
                t.checked_sub(n).map(ts).unwrap_or(Value::Null)
            }
            (Minus, Value::Timestamp(a), Value::Timestamp(b)) => {
                a.checked_sub(b).map(Value::Int).unwrap_or(Value::Null)
            }
            _ => {
                return err(
                    "timestamp arithmetic: TIMESTAMP ± INTEGER (ms) or TIMESTAMP - TIMESTAMP",
                )
            }
        });
    }
    // Decimal wins over Float in mixed arithmetic: exactness survives
    // `amount * 1.5` while Int stays promoted losslessly.
    if matches!(l, Value::Decimal(_)) || matches!(r, Value::Decimal(_)) {
        let a = as_decimal(&l)?;
        let b = as_decimal(&r)?;
        let out = match op {
            Plus => a.checked_add(b),
            Minus => a.checked_sub(b),
            Multiply => a.checked_mul(b),
            Divide => a.checked_div(b),
            Modulo => a.checked_rem(b),
            _ => return err("not an arithmetic operator"),
        };
        // Overflow yields NULL (SQLite-style) rather than a panic; division
        // by zero is None from checked_div/checked_rem.
        return Ok(out.map(Value::Decimal).unwrap_or(Value::Null));
    }
    let float_mode = matches!(l, Value::Float(_)) || matches!(r, Value::Float(_));
    if float_mode {
        let a = as_f64(&l)?;
        let b = as_f64(&r)?;
        return Ok(match op {
            Plus => Value::Float(a + b),
            Minus => Value::Float(a - b),
            Multiply => Value::Float(a * b),
            Divide if b == 0.0 => Value::Null,
            Divide => Value::Float(a / b),
            Modulo if b == 0.0 => Value::Null,
            Modulo => Value::Float(a % b),
            _ => return err("not an arithmetic operator"),
        });
    }
    match (&l, &r) {
        (Value::Int(a), Value::Int(b)) => {
            // Copy out so the inherent i64 checked_* methods win over the
            // num-traits impls imported for Decimal.
            let (a, b) = (*a, *b);
            // Overflow yields NULL (SQLite/Decimal semantics) — the old
            // wrapping arithmetic silently returned i64::MIN for
            // `9223372036854775807 + 1` and replicated/backed it up as truth.
            Ok(match op {
                Plus => match a.checked_add(b) {
                    Some(v) => Value::Int(v),
                    None => Value::Null,
                },
                Minus => match a.checked_sub(b) {
                    Some(v) => Value::Int(v),
                    None => Value::Null,
                },
                Multiply => match a.checked_mul(b) {
                    Some(v) => Value::Int(v),
                    None => Value::Null,
                },
                Divide if b == 0 => Value::Null,
                // i64::MIN / -1 overflows (SIGFPE on some ISAs) — treat like
                // division by zero and yield NULL instead of crashing.
                Divide => match a.checked_div(b) {
                    Some(q) => Value::Int(q),
                    None => Value::Null,
                },
                Modulo if b == 0 => Value::Null,
                Modulo => match a.checked_rem(b) {
                    Some(m) => Value::Int(m),
                    None => Value::Null,
                },
                _ => return err("not an arithmetic operator"),
            })
        }
        _ => err(format!(
            "non-numeric operands: {} {op} {}",
            l.type_name(),
            r.type_name()
        )),
    }
}

pub(super) fn as_f64(v: &Value) -> Result<f64> {
    match v {
        Value::Int(i) => Ok(*i as f64),
        Value::Float(f) => Ok(*f),
        Value::Decimal(d) => Ok(d.to_f64().unwrap_or(f64::NAN)),
        other => err(format!("non-numeric operand of type {}", other.type_name())),
    }
}

/// Exact numeric view of Int/Float/Decimal operands (the decimal arithmetic
/// path). A Float contributes its exact binary value.
pub(super) fn as_decimal(v: &Value) -> Result<Decimal> {
    match v {
        Value::Decimal(d) => Ok(*d),
        Value::Int(i) => Ok(Decimal::from(*i)),
        Value::Float(f) => Decimal::from_f64_retain(*f)
            .ok_or_else(|| SqlError::Message(format!("non-finite float in arithmetic: {f}"))),
        other => err(format!("non-numeric operand of type {}", other.type_name())),
    }
}
