use super::*;

pub(super) fn expr_name(e: &SqlExpr) -> String {
    match e {
        SqlExpr::Identifier(i) => i.value.clone(),
        SqlExpr::CompoundIdentifier(parts) => parts
            .iter()
            .map(|p| p.value.clone())
            .collect::<Vec<_>>()
            .join("."),
        other => other.to_string(),
    }
}

/// Column references of a CHECK constraint expression (bare identifiers and
/// the leaf of qualified names). Subqueries are rejected outright — the
/// runtime evaluator resolves unknown columns to NULL, so without this DDL
/// gate a typo'd CHECK would be created and never fire.
pub(super) fn check_expr_column_refs(e: &SqlExpr, out: &mut Vec<String>) -> Result<()> {
    use SqlExpr::*;
    match e {
        Identifier(i) => {
            out.push(i.value.clone());
            Ok(())
        }
        CompoundIdentifier(parts) => {
            if let Some(last) = parts.last() {
                out.push(last.value.clone());
            }
            Ok(())
        }
        Value(_) | TypedString { .. } => Ok(()),
        Nested(i) | IsNull(i) | IsNotNull(i) | Cast { expr: i, .. } => {
            check_expr_column_refs(i, out)
        }
        UnaryOp { expr, .. } => check_expr_column_refs(expr, out),
        BinaryOp { left, right, .. } => {
            check_expr_column_refs(left, out)?;
            check_expr_column_refs(right, out)
        }
        IsDistinctFrom(a, b) | IsNotDistinctFrom(a, b) => {
            check_expr_column_refs(a, out)?;
            check_expr_column_refs(b, out)
        }
        Between {
            expr, low, high, ..
        } => {
            check_expr_column_refs(expr, out)?;
            check_expr_column_refs(low, out)?;
            check_expr_column_refs(high, out)
        }
        Like { expr, pattern, .. } | ILike { expr, pattern, .. } => {
            check_expr_column_refs(expr, out)?;
            check_expr_column_refs(pattern, out)
        }
        InList { expr, list, .. } => {
            check_expr_column_refs(expr, out)?;
            for item in list {
                check_expr_column_refs(item, out)?;
            }
            Ok(())
        }
        Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(o) = operand {
                check_expr_column_refs(o, out)?;
            }
            for w in conditions {
                check_expr_column_refs(&w.condition, out)?;
                check_expr_column_refs(&w.result, out)?;
            }
            if let Some(r) = else_result {
                check_expr_column_refs(r, out)?;
            }
            Ok(())
        }
        Function(f) => {
            if let sqlparser::ast::FunctionArguments::List(list) = &f.args {
                for a in &list.args {
                    if let sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(inner),
                    ) = a
                    {
                        check_expr_column_refs(inner, out)?;
                    }
                }
            }
            Ok(())
        }
        Subquery(_) | Exists { .. } | InSubquery { .. } => {
            err("CHECK constraints cannot contain subqueries")
        }
        _ => Ok(()),
    }
}

/// Encoded identity of a document (UPDATE ... FROM / DELETE ... USING match
/// target rows by content).
pub(super) fn object_key(doc: &Object) -> Vec<u8> {
    let mut out = Vec::new();
    match encode::encode_object(doc, &mut out) {
        Ok(()) => out,
        Err(_) => Vec::new(),
    }
}

/// Rebuild a target table's document from its qualified slice of a merged
/// FROM row (UPDATE ... FROM / DELETE ... USING).
pub(super) fn target_doc_from_merged(m: &Object, prefix: &str) -> Object {
    let mut tdoc = Object::new();
    for (k, v) in m {
        if let Some(col) = k.strip_prefix(prefix) {
            tdoc.insert(col.to_string(), v.clone());
        }
    }
    tdoc
}

/// Evaluate SET assignments against the pre-update image and return them as
/// (column, value) pairs. SQL binds every right-hand side before any column
/// is written: in `SET a = a + 1, b = a`, `b` must observe the old `a`.
pub(super) fn eval_assignments(
    assignments: &[sqlparser::ast::Assignment],
    doc: &Object,
) -> Result<Vec<(String, Value)>> {
    assignments
        .iter()
        .map(|a| {
            let sqlparser::ast::AssignmentTarget::ColumnName(col) = &a.target else {
                return err("unsupported assignment target");
            };
            Ok((obj_name(col), eval_expr(&a.value, doc)?))
        })
        .collect()
}

/// Replace identifier `old` with `new` in SQL text, matching whole
/// identifiers only (adjacent identifier characters disqualify the match).
/// String literals are copied through verbatim — a CHECK like
/// `name <> 'name'` must not have its *literal* rewritten when the column
/// `name` is renamed, or the constraint silently changes meaning. Walks char
/// boundaries — a byte walk would panic slicing into a multibyte identifier,
/// and non-ASCII chars count as identifier characters so the `a` inside
/// `éa` can never match.
/// Replacement spelling for a renamed identifier inside stored SQL text
/// (CHECK expressions): bare when the name is a plain word, quoted (with
/// `""` escaping) otherwise — a `"` in the new name must not break the
/// lexical structure of the stored text (dump replays it verbatim).
pub(super) fn ident_spelling(name: &str) -> String {
    let plain = !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$');
    if plain {
        name.to_string()
    } else {
        crate::stmt::sql_quote_ident(name)
    }
}

pub(super) fn rename_ident_in_text(text: &str, old: &str, new: &str) -> String {
    pub(super) fn is_word(c: char) -> bool {
        !c.is_ascii() || c.is_ascii_alphanumeric() || c == '_' || c == '$'
    }
    let chars: Vec<(usize, char)> = text.char_indices().collect();
    let old_chars = old.chars().count();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < chars.len() {
        let (byte_i, c) = chars[i];
        if c == '\'' {
            // Copy the literal span untouched (same scanner the bind/redact/
            // lexing paths share — never a hand-rolled replacement walk).
            let (end, _) = crate::stmt::sql_literal_end(text, byte_i);
            out.push_str(&text[byte_i..end]);
            i += text[byte_i..end].chars().count();
            continue;
        }
        let prev_word = i > 0 && is_word(chars[i - 1].1);
        let next_word = chars
            .get(i + old_chars)
            .copied()
            .map(|(_, c2)| is_word(c2))
            .unwrap_or(false);
        if !prev_word && !next_word && text[byte_i..].starts_with(old) {
            out.push_str(new);
            i += old_chars;
        } else {
            out.push(c);
            i += 1;
        }
    }
    out
}

/// Literal-aware word-boundary test: does `name` occur as a bare identifier
/// outside string literals? Dependency checks over stored SQL text that a
/// full parse-walk is not worth it — a false positive only over-refuses a
/// DDL, a false negative would silently degrade a view. Same char-boundary
/// walk (and word rule) as [`rename_ident_in_text`].
pub(super) fn text_references_ident(text: &str, name: &str) -> bool {
    pub(super) fn is_word(c: char) -> bool {
        !c.is_ascii() || c.is_ascii_alphanumeric() || c == '_' || c == '$'
    }
    let chars: Vec<(usize, char)> = text.char_indices().collect();
    let name_chars = name.chars().count();
    let mut i = 0;
    while i < chars.len() {
        let (byte_i, c) = chars[i];
        if c == '\'' {
            let (end, _) = crate::stmt::sql_literal_end(text, byte_i);
            i += text[byte_i..end].chars().count();
            continue;
        }
        let prev_word = i > 0 && is_word(chars[i - 1].1);
        let next_word = chars
            .get(i + name_chars)
            .copied()
            .map(|(_, c2)| is_word(c2))
            .unwrap_or(false);
        if !prev_word && !next_word && text[byte_i..].starts_with(name) {
            return true;
        }
        i += 1;
    }
    false
}

/// Collect the table names and aliases a query reads in its own FROM, so
/// qualified references to anything else can be flagged as correlated.
pub(super) fn collect_from_names(q: &Query, out: &mut std::collections::BTreeSet<String>) {
    collect_setexpr_from_names(&q.body, out);
}

/// FROM names of a subquery body, recursing through set-operation arms and
/// nested query bodies — a body-level Select-only scan collected nothing
/// for `SELECT … FROM a UNION SELECT … FROM b`, making every qualified arm
/// reference look correlated.
pub(super) fn collect_setexpr_from_names(
    body: &sqlparser::ast::SetExpr,
    out: &mut std::collections::BTreeSet<String>,
) {
    use sqlparser::ast::SetExpr;
    match body {
        SetExpr::Select(sel) => from_list_names(&sel.from, out),
        SetExpr::SetOperation { left, right, .. } => {
            collect_setexpr_from_names(left, out);
            collect_setexpr_from_names(right, out);
        }
        SetExpr::Query(inner) => collect_setexpr_from_names(&inner.body, out),
        _ => {}
    }
}

/// FROM names + aliases of a FROM list — the qualifiers legal at that query
/// level, and the outer scope a correlated subquery may reference.
pub(super) fn from_list_names(from: &[sqlparser::ast::TableWithJoins], out: &mut OuterFrom) {
    for twj in from {
        if let sqlparser::ast::TableFactor::Table { name, alias, .. } = &twj.relation {
            out.insert(obj_name(name));
            if let Some(a) = alias {
                out.insert(a.name.value.clone());
            }
        }
        // Joined tables are inner-scope names too: a reference to them
        // from inside the subquery is NOT a correlation (the old
        // base-relation-only scan misrejected legitimate subqueries).
        for j in &twj.joins {
            if let sqlparser::ast::TableFactor::Table { name, alias, .. } = &j.relation {
                out.insert(obj_name(name));
                if let Some(a) = alias {
                    out.insert(a.name.value.clone());
                }
            }
        }
    }
}

/// The outer scope of a query level: its FROM names and aliases. A
/// correlated subquery's outer references must be qualified with one of
/// these (`alias.col` / `table.col`); unqualified outer references stay
/// out of scope on purpose — in a schemaless engine "inner doc lacks the
/// field" and "outer column" are indistinguishable, so binding misses to
/// the outer row would silently change sparse-document semantics.
pub(super) type OuterFrom = std::collections::BTreeSet<String>;

/// Does `e` contain a subquery-bearing node anywhere (the shapes the
/// pre-pass resolves)? Row-local sites use this to decide whether the
/// per-row correlated pass must run.
pub(super) fn expr_has_subquery(e: &SqlExpr) -> bool {
    match e {
        SqlExpr::Subquery(_)
        | SqlExpr::InSubquery { .. }
        | SqlExpr::Exists { .. }
        | SqlExpr::AnyOp { .. }
        | SqlExpr::AllOp { .. } => true,
        SqlExpr::BinaryOp { left, right, .. } => {
            expr_has_subquery(left) || expr_has_subquery(right)
        }
        SqlExpr::UnaryOp { expr, .. } => expr_has_subquery(expr),
        SqlExpr::Nested(inner) => expr_has_subquery(inner),
        SqlExpr::Between {
            expr, low, high, ..
        } => expr_has_subquery(expr) || expr_has_subquery(low) || expr_has_subquery(high),
        SqlExpr::IsDistinctFrom(l, r) | SqlExpr::IsNotDistinctFrom(l, r) => {
            expr_has_subquery(l) || expr_has_subquery(r)
        }
        SqlExpr::Like { expr, pattern, .. } | SqlExpr::ILike { expr, pattern, .. } => {
            expr_has_subquery(expr) || expr_has_subquery(pattern)
        }
        SqlExpr::InList { expr, list, .. } => {
            expr_has_subquery(expr) || list.iter().any(expr_has_subquery)
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand.as_deref().is_some_and(expr_has_subquery)
                || conditions
                    .iter()
                    .any(|w| expr_has_subquery(&w.condition) || expr_has_subquery(&w.result))
                || else_result.as_deref().is_some_and(expr_has_subquery)
        }
        SqlExpr::Cast { expr, .. } => expr_has_subquery(expr),
        SqlExpr::Function(_) => child_exprs(e).into_iter().any(expr_has_subquery),
        // Shared traversal: a subquery under a wrapper node (FLOOR/IS
        // NULL/SUBSTRING/…) must be seen here so the pre-pass can resolve
        // it instead of erroring at row level.
        _ => child_exprs(e).into_iter().any(expr_has_subquery),
    }
}

/// Conservative correlation probe: any qualified reference in `e` whose
/// qualifier belongs to `outer`. Deliberately ignores inner-FROM shadowing
/// — a false positive only routes the node through the per-row path (the
/// substitution itself scopes precisely), while a false negative would
/// silently read the outer column as NULL.
pub(super) fn expr_mentions_outer(e: &SqlExpr, outer: &OuterFrom) -> bool {
    match e {
        SqlExpr::CompoundIdentifier(parts) => parts.len() >= 2 && outer.contains(&parts[0].value),
        SqlExpr::Subquery(q) => query_mentions_outer(q, outer),
        SqlExpr::InSubquery { expr, subquery, .. } => {
            expr_mentions_outer(expr, outer) || query_mentions_outer(subquery, outer)
        }
        SqlExpr::Exists { subquery, .. } => query_mentions_outer(subquery, outer),
        SqlExpr::AnyOp { left, right, .. } | SqlExpr::AllOp { left, right, .. } => {
            expr_mentions_outer(left, outer) || expr_mentions_outer(right, outer)
        }
        SqlExpr::BinaryOp { left, right, .. } => {
            expr_mentions_outer(left, outer) || expr_mentions_outer(right, outer)
        }
        SqlExpr::UnaryOp { expr, .. } => expr_mentions_outer(expr, outer),
        SqlExpr::Nested(inner) => expr_mentions_outer(inner, outer),
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            expr_mentions_outer(expr, outer)
                || expr_mentions_outer(low, outer)
                || expr_mentions_outer(high, outer)
        }
        SqlExpr::IsDistinctFrom(l, r) | SqlExpr::IsNotDistinctFrom(l, r) => {
            expr_mentions_outer(l, outer) || expr_mentions_outer(r, outer)
        }
        SqlExpr::Like { expr, pattern, .. } | SqlExpr::ILike { expr, pattern, .. } => {
            expr_mentions_outer(expr, outer) || expr_mentions_outer(pattern, outer)
        }
        SqlExpr::InList { expr, list, .. } => {
            expr_mentions_outer(expr, outer) || list.iter().any(|e| expr_mentions_outer(e, outer))
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand
                .as_deref()
                .is_some_and(|o| expr_mentions_outer(o, outer))
                || conditions.iter().any(|w| {
                    expr_mentions_outer(&w.condition, outer)
                        || expr_mentions_outer(&w.result, outer)
                })
                || else_result
                    .as_deref()
                    .is_some_and(|o| expr_mentions_outer(o, outer))
        }
        SqlExpr::Cast { expr, .. } => expr_mentions_outer(expr, outer),
        SqlExpr::Function(_) => child_exprs(e)
            .into_iter()
            .any(|c| expr_mentions_outer(c, outer)),
        // Shared traversal: an outer reference under a wrapper node
        // (FLOOR/IS NULL/SUBSTRING/…) is still correlated — missing it
        // would silently resolve the subquery once with NULL outer values.
        _ => child_exprs(e)
            .into_iter()
            .any(|c| expr_mentions_outer(c, outer)),
    }
}

/// `expr_mentions_outer` over a query: every surface that evaluates in the
/// query's own scope (mirrors `collect_correlated_in_body` plus ORDER BY).
pub(super) fn query_mentions_outer(q: &Query, outer: &OuterFrom) -> bool {
    if let Some(w) = &q.with {
        for cte in &w.cte_tables {
            if query_mentions_outer(&cte.query, outer) {
                return true;
            }
        }
    }
    if body_mentions_outer(&q.body, outer) {
        return true;
    }
    if let Some(ob) = &q.order_by {
        if let sqlparser::ast::OrderByKind::Expressions(exprs) = &ob.kind {
            if exprs.iter().any(|o| expr_mentions_outer(&o.expr, outer)) {
                return true;
            }
        }
    }
    false
}

pub(super) fn body_mentions_outer(body: &sqlparser::ast::SetExpr, outer: &OuterFrom) -> bool {
    use sqlparser::ast::SetExpr;
    match body {
        SetExpr::Select(sel) => {
            for twj in &sel.from {
                for j in &twj.joins {
                    if let Some(e) = join_on_expr(&j.join_operator) {
                        if expr_mentions_outer(e, outer) {
                            return true;
                        }
                    }
                }
            }
            if sel
                .selection
                .as_ref()
                .is_some_and(|e| expr_mentions_outer(e, outer))
            {
                return true;
            }
            if sel
                .having
                .as_ref()
                .is_some_and(|e| expr_mentions_outer(e, outer))
            {
                return true;
            }
            if let sqlparser::ast::GroupByExpr::Expressions(es, _) = &sel.group_by {
                if es.iter().any(|e| expr_mentions_outer(e, outer)) {
                    return true;
                }
            }
            sel.projection.iter().any(|item| match item {
                sqlparser::ast::SelectItem::UnnamedExpr(e)
                | sqlparser::ast::SelectItem::ExprWithAlias { expr: e, .. } => {
                    expr_mentions_outer(e, outer)
                }
                _ => false,
            })
        }
        SetExpr::SetOperation { left, right, .. } => {
            body_mentions_outer(left, outer) || body_mentions_outer(right, outer)
        }
        SetExpr::Query(inner) => query_mentions_outer(inner, outer),
        SetExpr::Values(rows) => rows
            .rows
            .iter()
            .flat_map(|parens| parens.content.iter())
            .any(|e| expr_mentions_outer(e, outer)),
        _ => false,
    }
}

/// The subquery payload of a subquery-bearing node (`Subquery`,
/// `InSubquery`, `Exists`, `ANY/ALL right side), for per-row outer-ref
/// substitution.
pub(super) fn subquery_of_mut(e: &mut SqlExpr) -> Option<&mut Query> {
    match e {
        SqlExpr::Subquery(q) => Some(q),
        SqlExpr::InSubquery { subquery, .. } => Some(subquery),
        SqlExpr::Exists { subquery, .. } => Some(subquery),
        SqlExpr::AnyOp { right, .. } | SqlExpr::AllOp { right, .. } => match right.as_mut() {
            SqlExpr::Subquery(q) => Some(q),
            _ => None,
        },
        _ => None,
    }
}

/// Outer-row value for a correlated reference: the enclosing row carries
/// `alias.col` keys when joined, bare `col` keys for a solo base table —
/// the same resolution conventions `eval_expr` applies to the outer query
/// itself. A missing column reads as NULL (schemaless semantics).
pub(super) fn outer_row_value(
    doc: &Object,
    qualifier: &str,
    rest: &[sqlparser::ast::Ident],
) -> Value {
    let rest = rest
        .iter()
        .map(|i| i.value.as_str())
        .collect::<Vec<_>>()
        .join(".");
    let full = format!("{qualifier}.{rest}");
    // Only a genuinely MISSING qualified key falls back to the bare name
    // (solo-table rows carry bare keys). `lookup_col` cannot tell an
    // explicit NULL from a missing column — both are Ok(Null) — so routing
    // Ok(Null) through the fallback let a joined row's explicit
    // `alias.col = NULL` resolve to a DIFFERENT alias's same-named column
    // in correlated subqueries.
    match doc.get(&full) {
        Some(v) => v.clone(),
        None => lookup_col(doc, &rest).unwrap_or(Value::Null),
    }
}

/// Substitute outer references in `e` with literals from the enclosing row
/// `doc`. Precise SQL scoping: names in `shadow` (the inner FROM names at
/// this nesting level) win over `outer`. Returns true when at least one
/// reference was substituted.
pub(super) fn subst_outer_refs_expr(
    e: &mut SqlExpr,
    shadow: &OuterFrom,
    outer: &OuterFrom,
    doc: &Object,
) -> bool {
    let mut hit = false;
    match e {
        SqlExpr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let qualifier = parts[0].value.clone();
            if !shadow.contains(&qualifier) && outer.contains(&qualifier) {
                let v = outer_row_value(doc, &qualifier, &parts[1..]);
                hit = true;
                *e = match value_to_literal(v) {
                    Ok(lit) => lit,
                    // Only exotic values fail to render (out-of-domain
                    // timestamps); NULL is the honest row-local answer.
                    Err(_) => SqlExpr::Value(sqlparser::ast::Value::Null.into()),
                };
            }
        }
        SqlExpr::Subquery(q) => {
            hit = subst_outer_refs_query(q, shadow, outer, doc);
        }
        SqlExpr::InSubquery { expr, subquery, .. } => {
            hit = subst_outer_refs_expr(expr, shadow, outer, doc);
            hit |= subst_outer_refs_query(subquery, shadow, outer, doc);
        }
        SqlExpr::Exists { subquery, .. } => {
            hit = subst_outer_refs_query(subquery, shadow, outer, doc);
        }
        SqlExpr::AnyOp { left, right, .. } | SqlExpr::AllOp { left, right, .. } => {
            hit = subst_outer_refs_expr(left, shadow, outer, doc);
            hit |= subst_outer_refs_expr(right, shadow, outer, doc);
        }
        SqlExpr::BinaryOp { left, right, .. } => {
            hit = subst_outer_refs_expr(left, shadow, outer, doc);
            hit |= subst_outer_refs_expr(right, shadow, outer, doc);
        }
        SqlExpr::UnaryOp { expr, .. } => hit = subst_outer_refs_expr(expr, shadow, outer, doc),
        SqlExpr::Nested(inner) => hit = subst_outer_refs_expr(inner, shadow, outer, doc),
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            hit = subst_outer_refs_expr(expr, shadow, outer, doc);
            hit |= subst_outer_refs_expr(low, shadow, outer, doc);
            hit |= subst_outer_refs_expr(high, shadow, outer, doc);
        }
        SqlExpr::IsDistinctFrom(l, r) | SqlExpr::IsNotDistinctFrom(l, r) => {
            hit = subst_outer_refs_expr(l, shadow, outer, doc);
            hit |= subst_outer_refs_expr(r, shadow, outer, doc);
        }
        SqlExpr::Like { expr, pattern, .. } | SqlExpr::ILike { expr, pattern, .. } => {
            hit = subst_outer_refs_expr(expr, shadow, outer, doc);
            hit |= subst_outer_refs_expr(pattern, shadow, outer, doc);
        }
        SqlExpr::InList { expr, list, .. } => {
            hit = subst_outer_refs_expr(expr, shadow, outer, doc);
            for item in list.iter_mut() {
                hit |= subst_outer_refs_expr(item, shadow, outer, doc);
            }
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                hit = subst_outer_refs_expr(op, shadow, outer, doc);
            }
            for w in conditions.iter_mut() {
                hit |= subst_outer_refs_expr(&mut w.condition, shadow, outer, doc);
                hit |= subst_outer_refs_expr(&mut w.result, shadow, outer, doc);
            }
            if let Some(el) = else_result {
                hit |= subst_outer_refs_expr(el, shadow, outer, doc);
            }
        }
        SqlExpr::Cast { expr, .. } => hit = subst_outer_refs_expr(expr, shadow, outer, doc),
        SqlExpr::Function(f) => {
            if let sqlparser::ast::FunctionArguments::List(list) = &mut f.args {
                for a in list.args.iter_mut() {
                    if let sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(inner),
                    ) = a
                    {
                        hit |= subst_outer_refs_expr(inner, shadow, outer, doc);
                    }
                }
            }
            if let Some(flt) = &mut f.filter {
                hit |= subst_outer_refs_expr(flt, shadow, outer, doc);
            }
        }
        // Shared traversal (mirror of expr_mentions_outer above): an outer
        // reference under a wrapper node (IS NULL/SUBSTRING/TRIM/…) must be
        // substituted too — leaving it reads the outer column as NULL (or an
        // inner same-name column via suffix fallback) inside the subquery.
        _ => {
            for c in child_exprs_mut(e) {
                hit |= subst_outer_refs_expr(c, shadow, outer, doc);
            }
        }
    }
    hit
}

/// `subst_outer_refs_expr` over every scope-local surface of a query.
/// `shadow` is the scope ENCLOSING the query; the query's own FROM names
/// extend a local copy (they win over `outer` inside), and every nested
/// subquery body starts from that copy — so sibling subqueries never
/// inherit each other's FROM names, and an inner alias equal to an outer
/// one (`EXISTS(SELECT 1 FROM t c WHERE c.x = 1)` under an outer alias
/// `c`) keeps the inner `c.x` untouched.
pub(super) fn subst_outer_refs_query(
    q: &mut Query,
    shadow: &OuterFrom,
    outer: &OuterFrom,
    doc: &Object,
) -> bool {
    let mut hit = false;
    if let Some(w) = &mut q.with {
        // CTE bodies of one WITH share the level's accumulating scope (a
        // later CTE may reference an earlier one): insert each name AFTER
        // its body is rewritten so self-references keep resolving to the
        // CTE, while later bodies see it (a clone taken once never
        // accumulated, so a qualified reference to an earlier CTE could be
        // mistaken for the outer row's alias and substituted with outer
        // values — silently reading the wrong table).
        let mut cte_shadow = shadow.clone();
        for cte in &mut w.cte_tables {
            hit |= subst_outer_refs_query(&mut cte.query, &cte_shadow, outer, doc);
            cte_shadow.insert(cte.alias.name.value.clone());
        }
    }
    hit |= subst_outer_refs_setexpr(&mut q.body, shadow, outer, doc);
    if let Some(ob) = &mut q.order_by {
        // ORDER BY resolves in the body's own scope: reuse its FROM names.
        let mut own = shadow.clone();
        if let sqlparser::ast::SetExpr::Select(sel) = &*q.body {
            from_list_names(&sel.from, &mut own);
        }
        if let sqlparser::ast::OrderByKind::Expressions(exprs) = &mut ob.kind {
            for o in exprs.iter_mut() {
                hit |= subst_outer_refs_expr(&mut o.expr, &own, outer, doc);
            }
        }
    }
    hit
}

pub(super) fn subst_outer_refs_setexpr(
    body: &mut sqlparser::ast::SetExpr,
    shadow: &OuterFrom,
    outer: &OuterFrom,
    doc: &Object,
) -> bool {
    use sqlparser::ast::SetExpr;
    match body {
        SetExpr::Select(sel) => {
            let mut own = shadow.clone();
            from_list_names(&sel.from, &mut own);
            let mut hit = false;
            for twj in &mut sel.from {
                for j in &mut twj.joins {
                    if let Some(e) = join_on_expr_mut(&mut j.join_operator) {
                        hit |= subst_outer_refs_expr(e, &own, outer, doc);
                    }
                }
            }
            if let Some(sel_expr) = &mut sel.selection {
                hit |= subst_outer_refs_expr(sel_expr, &own, outer, doc);
            }
            if let Some(having) = &mut sel.having {
                hit |= subst_outer_refs_expr(having, &own, outer, doc);
            }
            if let sqlparser::ast::GroupByExpr::Expressions(es, _) = &mut sel.group_by {
                for e in es.iter_mut() {
                    hit |= subst_outer_refs_expr(e, &own, outer, doc);
                }
            }
            for item in &mut sel.projection {
                match item {
                    sqlparser::ast::SelectItem::UnnamedExpr(e)
                    | sqlparser::ast::SelectItem::ExprWithAlias { expr: e, .. } => {
                        hit |= subst_outer_refs_expr(e, &own, outer, doc);
                    }
                    _ => {}
                }
            }
            hit
        }
        SetExpr::SetOperation { left, right, .. } => {
            // Arms do not see each other's FROM names.
            subst_outer_refs_setexpr(left, shadow, outer, doc)
                | subst_outer_refs_setexpr(right, shadow, outer, doc)
        }
        SetExpr::Query(inner) => subst_outer_refs_query(inner, shadow, outer, doc),
        SetExpr::Values(rows) => {
            let mut hit = false;
            for parens in rows.rows.iter_mut() {
                for e in parens.content.iter_mut() {
                    hit |= subst_outer_refs_expr(e, shadow, outer, doc);
                }
            }
            hit
        }
        _ => false,
    }
}

// ---- read-target walkers (stmt_read_targets) ----
// Every walker returns Option<()>: `None` means "shape not fully
// classifiable" and the authorization caller must DENY (fail closed).
