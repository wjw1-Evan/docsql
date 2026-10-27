use super::*;

pub(super) fn walk_query(
    q: &Query,
    out: &mut Vec<String>,
    objects: &dyn Fn(&str) -> bool,
) -> Option<()> {
    walk_query_shadowed(q, out, &[], objects)
}

/// `shadow` carries the CTE names visible at this scope: a WITH alias shadows
/// same-named tables/views for its entire scope (subqueries included), so
/// FROM refs under those names read the CTE, not a base object, and must not
/// land in the base set — a view whose body opens `WITH v AS (…) SELECT *
/// FROM v` used to be misjudged as self-referencing and refused.
/// `objects` tells whether a name matches a real table/view: the engine binds
/// a CTE's own body (and a recursive anchor) BEFORE the CTE enters scope, so
/// a self-name that also names a real object reads the BASE object there —
/// shadowing it would hide that read from authorization and row filters
/// entirely (the name only shadows once its body is done).
pub(super) fn walk_query_shadowed(
    q: &Query,
    out: &mut Vec<String>,
    outer: &[String],
    objects: &dyn Fn(&str) -> bool,
) -> Option<()> {
    let mut shadow: Vec<String> = outer.to_vec();
    if let Some(w) = &q.with {
        for cte in &w.cte_tables {
            let name = cte.alias.name.value.clone();
            if !objects(&name) {
                shadow.push(name.clone());
            }
            walk_query_shadowed(&cte.query, out, &shadow, objects)?;
            // From here on (later CTE bodies, the main query) the name is a
            // materialized CTE read for everyone.
            shadow.push(name);
        }
    }
    // Fast path only when NO names are shadowed at this level: a body walk
    // arrives with the enclosing scope's shadow in `outer`, and short-
    // circuiting there wrote the refs straight into `out`, skipping the
    // filter entirely (the body's own CTE reads leaked into the base set).
    if shadow.is_empty() {
        return walk_query_inner(q, out, objects);
    }
    let mut refs = Vec::new();
    walk_query_inner(q, &mut refs, objects)?;
    out.extend(refs.into_iter().filter(|r| !shadow.iter().any(|s| s == r)));
    Some(())
}

pub(super) fn walk_query_inner(
    q: &Query,
    out: &mut Vec<String>,
    objects: &dyn Fn(&str) -> bool,
) -> Option<()> {
    walk_setexpr(&q.body, out, objects)?;
    if let Some(order_by) = &q.order_by {
        if let sqlparser::ast::OrderByKind::Expressions(exprs) = &order_by.kind {
            for obe in exprs {
                walk_expr(&obe.expr, out, objects)?;
            }
        }
    }
    Some(())
}

pub(super) fn walk_setexpr(
    se: &SetExpr,
    out: &mut Vec<String>,
    objects: &dyn Fn(&str) -> bool,
) -> Option<()> {
    match se {
        SetExpr::Select(sel) => {
            for twj in &sel.from {
                walk_factor(&twj.relation, out, objects)?;
                for j in &twj.joins {
                    walk_factor(&j.relation, out, objects)?;
                    if let Some(e) = join_on_expr(&j.join_operator) {
                        walk_expr(e, out, objects)?;
                    }
                }
            }
            for item in &sel.projection {
                match item {
                    SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                        walk_expr(e, out, objects)?
                    }
                    _ => {}
                }
            }
            if let Some(sel_expr) = &sel.selection {
                walk_expr(sel_expr, out, objects)?;
            }
            if let sqlparser::ast::GroupByExpr::Expressions(exprs, _) = &sel.group_by {
                for e in exprs {
                    walk_expr(e, out, objects)?;
                }
            }
            if let Some(having) = &sel.having {
                walk_expr(having, out, objects)?;
            }
            Some(())
        }
        SetExpr::Query(inner) => walk_query(inner, out, objects),
        SetExpr::SetOperation { left, right, .. } => {
            walk_setexpr(left, out, objects)?;
            walk_setexpr(right, out, objects)
        }
        SetExpr::Values(values) => {
            for row in &values.rows {
                for e in row.content.iter() {
                    walk_expr(e, out, objects)?;
                }
            }
            Some(())
        }
        // TABLE / InsertStatements shapes: no table refs to collect.
        _ => Some(()),
    }
}

/// The ON predicate of a join, when it has one (USING lists reference
/// already-counted tables; NATURAL/CROSS have no predicate).
pub(super) fn join_on_expr(op: &sqlparser::ast::JoinOperator) -> Option<&SqlExpr> {
    use sqlparser::ast::JoinConstraint as JC;
    use sqlparser::ast::JoinOperator as JO;
    let constraint = match op {
        JO::Join(c)
        | JO::Inner(c)
        | JO::Left(c)
        | JO::LeftOuter(c)
        | JO::Right(c)
        | JO::RightOuter(c)
        | JO::FullOuter(c)
        | JO::CrossJoin(c)
        | JO::Semi(c)
        | JO::LeftSemi(c)
        | JO::RightSemi(c)
        | JO::Anti(c)
        | JO::LeftAnti(c)
        | JO::RightAnti(c) => c,
        _ => return None,
    };
    match constraint {
        JC::On(e) => Some(e),
        _ => None,
    }
}

/// Mutable twin of [`join_on_expr`] for in-place expression substitution.
/// Every constraint-carrying variant must be covered here AND there: the
/// read-target walkers use the shared accessor, so an uncovered variant
/// would let a subquery inside its ON read tables the authorizer never saw.
pub(super) fn join_on_expr_mut(op: &mut sqlparser::ast::JoinOperator) -> Option<&mut SqlExpr> {
    use sqlparser::ast::JoinConstraint as JC;
    use sqlparser::ast::JoinOperator as JO;
    let constraint = match op {
        JO::Join(c)
        | JO::Inner(c)
        | JO::Left(c)
        | JO::LeftOuter(c)
        | JO::Right(c)
        | JO::RightOuter(c)
        | JO::FullOuter(c)
        | JO::CrossJoin(c)
        | JO::Semi(c)
        | JO::LeftSemi(c)
        | JO::RightSemi(c)
        | JO::Anti(c)
        | JO::LeftAnti(c)
        | JO::RightAnti(c) => c,
        _ => return None,
    };
    match constraint {
        JC::On(e) => Some(e),
        _ => None,
    }
}

pub(super) fn walk_factor(
    tf: &sqlparser::ast::TableFactor,
    out: &mut Vec<String>,
    objects: &dyn Fn(&str) -> bool,
) -> Option<()> {
    match tf {
        sqlparser::ast::TableFactor::Table { name, .. } => {
            // Multi-part names keep their QUALIFIED form for the
            // authorization walkers: the compatibility views are addressed
            // as information_schema.columns, and taking the last segment
            // made them indistinguishable from a user table of the same
            // leaf name (the server's readable_by_all prefix never fired).
            // The engine's own name resolution is unchanged — this walker
            // only feeds classification.
            if name.0.len() > 1 {
                let qualified = name
                    .0
                    .iter()
                    .map(|p| match p {
                        sqlparser::ast::ObjectNamePart::Identifier(i) => i.value.clone(),
                        other => other.to_string(),
                    })
                    .collect::<Vec<_>>()
                    .join(".");
                out.push(qualified);
            } else {
                out.push(obj_name(name));
            }
            Some(())
        }
        sqlparser::ast::TableFactor::Derived { subquery, .. } => walk_query(subquery, out, objects),
        sqlparser::ast::TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            walk_factor(&table_with_joins.relation, out, objects)?;
            for j in &table_with_joins.joins {
                walk_factor(&j.relation, out, objects)?;
            }
            Some(())
        }
        // Table functions / UNNEST: unsupported shapes — deny.
        _ => None,
    }
}

pub(super) fn walk_expr(
    e: &SqlExpr,
    out: &mut Vec<String>,
    objects: &dyn Fn(&str) -> bool,
) -> Option<()> {
    use SqlExpr::*;
    match e {
        Value(_) | Identifier(_) | CompoundIdentifier(_) | TypedString { .. } => Some(()),
        IsNull(i) | IsNotNull(i) | Nested(i) | Cast { expr: i, .. } => walk_expr(i, out, objects),
        UnaryOp { expr, .. } => walk_expr(expr, out, objects),
        BinaryOp { left, right, .. } => {
            walk_expr(left, out, objects)?;
            walk_expr(right, out, objects)
        }
        Between {
            expr, low, high, ..
        } => {
            walk_expr(expr, out, objects)?;
            walk_expr(low, out, objects)?;
            walk_expr(high, out, objects)
        }
        Like { expr, pattern, .. } | ILike { expr, pattern, .. } => {
            walk_expr(expr, out, objects)?;
            walk_expr(pattern, out, objects)
        }
        InList { expr, list, .. } => {
            walk_expr(expr, out, objects)?;
            for item in list {
                walk_expr(item, out, objects)?;
            }
            Some(())
        }
        InSubquery { expr, subquery, .. } => {
            walk_expr(expr, out, objects)?;
            walk_query(subquery, out, objects)
        }
        Subquery(q) => walk_query(q, out, objects),
        Exists { subquery, .. } => walk_query(subquery, out, objects),
        Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(o) = operand {
                walk_expr(o, out, objects)?;
            }
            for w in conditions {
                walk_expr(&w.condition, out, objects)?;
                walk_expr(&w.result, out, objects)?;
            }
            if let Some(r) = else_result {
                walk_expr(r, out, objects)?;
            }
            Some(())
        }
        Function(f) => {
            if let sqlparser::ast::FunctionArguments::List(list) = &f.args {
                for a in &list.args {
                    if let sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(inner),
                    ) = a
                    {
                        walk_expr(inner, out, objects)?;
                    }
                }
            }
            // Aggregate FILTER (WHERE …) is evaluated per row and may carry
            // subqueries — an unwalked filter would let a read-only user
            // run a hidden subquery the classifier never sees.
            if let Some(cond) = &f.filter {
                walk_expr(cond, out, objects)?;
            }
            for og in &f.within_group {
                walk_expr(&og.expr, out, objects)?;
            }
            Some(())
        }
        Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            walk_expr(expr, out, objects)?;
            if let Some(f) = substring_from {
                walk_expr(f, out, objects)?;
            }
            if let Some(l) = substring_for {
                walk_expr(l, out, objects)?;
            }
            Some(())
        }
        Trim {
            expr, trim_what, ..
        } => {
            walk_expr(expr, out, objects)?;
            if let Some(w) = trim_what {
                walk_expr(w, out, objects)?;
            }
            Some(())
        }
        // ANY/ALL and the long tail of rare expression shapes: deny rather
        // than risk authorizing an unread subquery (fail closed).
        _ => None,
    }
}

/// Find `outer_table.column` references in `e` whose qualifier is a known
/// catalog table but not one of the query's own FROM names/aliases — the
/// correlated-reference shape. Unqualified outer references cannot be told
/// apart from legitimate schemaless fields, so only qualified ones flag.
pub(super) fn collect_correlated_refs(
    e: &SqlExpr,
    from_names: &std::collections::BTreeSet<String>,
    catalog: &std::collections::BTreeMap<String, std::sync::Arc<TableMeta>>,
    hits: &mut Vec<String>,
) {
    match e {
        SqlExpr::CompoundIdentifier(parts) => {
            if parts.len() >= 2 {
                let qualifier = &parts[0].value;
                if !from_names.contains(qualifier) && catalog.contains_key(qualifier) {
                    hits.push(qualifier.clone());
                }
            }
        }
        SqlExpr::BinaryOp { left, right, .. } => {
            collect_correlated_refs(left, from_names, catalog, hits);
            collect_correlated_refs(right, from_names, catalog, hits);
        }
        SqlExpr::UnaryOp { expr, .. } => collect_correlated_refs(expr, from_names, catalog, hits),
        SqlExpr::Nested(inner) => collect_correlated_refs(inner, from_names, catalog, hits),
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            collect_correlated_refs(expr, from_names, catalog, hits);
            collect_correlated_refs(low, from_names, catalog, hits);
            collect_correlated_refs(high, from_names, catalog, hits);
        }
        SqlExpr::Like { expr, pattern, .. } | SqlExpr::ILike { expr, pattern, .. } => {
            collect_correlated_refs(expr, from_names, catalog, hits);
            collect_correlated_refs(pattern, from_names, catalog, hits);
        }
        SqlExpr::InList { expr, list, .. } => {
            collect_correlated_refs(expr, from_names, catalog, hits);
            for item in list {
                collect_correlated_refs(item, from_names, catalog, hits);
            }
        }
        SqlExpr::InSubquery { expr, .. } => {
            collect_correlated_refs(expr, from_names, catalog, hits);
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                collect_correlated_refs(op, from_names, catalog, hits);
            }
            for w in conditions {
                collect_correlated_refs(&w.condition, from_names, catalog, hits);
                collect_correlated_refs(&w.result, from_names, catalog, hits);
            }
            if let Some(el) = else_result {
                collect_correlated_refs(el, from_names, catalog, hits);
            }
        }
        SqlExpr::Cast { expr, .. } => collect_correlated_refs(expr, from_names, catalog, hits),
        // Shared traversal (mirror of expr_mentions_outer): a qualified outer
        // reference under a wrapper node (IS NULL/SUBSTRING/…) is still a
        // correlation; missing it let the subquery fall back to the
        // schemaless NULL read.
        _ => {
            for c in child_exprs(e) {
                collect_correlated_refs(c, from_names, catalog, hits);
            }
        }
    }
}

/// Dump-replay insert order: referenced tables before referencing ones
/// (FK checks run per row during replay). Picks the first satisfiable
/// table in catalog order each round; an FK cycle falls back to catalog
/// order for its members (a true cycle needs deferred constraints).
pub(super) fn fk_dependency_order(
    names: &[String],
    tables: &std::collections::BTreeMap<String, std::sync::Arc<TableMeta>>,
) -> Vec<String> {
    let mut out: Vec<String> = Vec::with_capacity(names.len());
    let mut remaining: Vec<String> = names.to_vec();
    while !remaining.is_empty() {
        let next = remaining.iter().position(|n| {
            tables.get(n).is_none_or(|m| {
                m.foreign_keys.iter().all(|(_, rt, _)| {
                    rt == n || !names.iter().any(|x| x == rt) || out.iter().any(|o| o == rt)
                })
            })
        });
        match next {
            Some(i) => out.push(remaining.remove(i)),
            None => out.append(&mut remaining),
        }
    }
    out
}

/// Row order for a table that references ITSELF in a dump: parents first.
/// Per-row FK checks run at replay, so a storage-order dump of a
/// self-referencing table fails restore/join/repair replay whenever a row
/// references a later-stored row (reachable via UPDATE, which never had the
/// child-side batch check). Rows with NULL/missing FK values have no
/// dependency; a row whose reference can be satisfied by ITSELF (the replay
/// check counts the statement's own batch) is free too. Same-value parent
/// groups open as soon as one member is emitted (the replay check needs one
/// present parent). A cycle keeps the remaining storage order and fails
/// loudly at replay instead of silently dropping rows. Kahn-style, O(rows +
/// edges) — a long reference chain must not turn the dump quadratic.
pub(super) fn self_ref_row_order(table: &str, meta: &TableMeta, docs: Vec<Object>) -> Vec<Object> {
    let self_fks: Vec<(&str, &str)> = meta
        .foreign_keys
        .iter()
        .filter(|(_, rt, _)| rt == table)
        .map(|(lc, _, rc)| (lc.as_str(), rc.as_str()))
        .collect();
    if self_fks.is_empty() || docs.len() < 2 {
        return docs;
    }
    // 编码字节作值键:与 B+ 树/判重同源(Value 本身不作 Ord)。
    let key_of = |v: &Value| encode::encode_to_vec(v).ok();
    // 值 → 组号;组员 = rc 值落在该组的行。只把「能当父行」的值建组:
    // NULL 被引用列值不当父(子行 NULL 引用本来就免检,值 NULL 的父行
    // 也不会成为任何非 NULL 引用的目标)。
    let mut group_of: std::collections::BTreeMap<Vec<u8>, usize> =
        std::collections::BTreeMap::new();
    let mut members: Vec<Vec<usize>> = Vec::new();
    for (i, doc) in docs.iter().enumerate() {
        for (_, rc) in &self_fks {
            if let Some(v) = doc.get(*rc).filter(|v| !matches!(v, Value::Null)) {
                if let Some(k) = key_of(v) {
                    let gid = *group_of.entry(k).or_insert_with(|| {
                        members.push(Vec::new());
                        members.len() - 1
                    });
                    members[gid].push(i);
                }
            }
        }
    }
    let n = docs.len();
    // 每行未满足的依赖数 + 每组的等待者;初始扫描定队列。
    let mut blocked: Vec<usize> = vec![0; n];
    let mut waiters: Vec<Vec<usize>> = vec![Vec::new(); members.len()];
    let mut open: Vec<bool> = vec![false; members.len()];
    let mut queue: std::collections::VecDeque<usize> = std::collections::VecDeque::new();
    let mut in_queue = vec![false; n];
    for (i, doc) in docs.iter().enumerate() {
        for (lc, _) in &self_fks {
            let Some(v) = doc.get(*lc) else { continue };
            if matches!(v, Value::Null) {
                continue;
            }
            let Some(k) = key_of(v) else { continue };
            let Some(&gid) = group_of.get(&k) else {
                continue;
            };
            if members[gid].contains(&i) {
                // 引用可由本行自身满足(回放检查计入本语句批次)。
                continue;
            }
            blocked[i] += 1;
            waiters[gid].push(i);
        }
        if blocked[i] == 0 {
            queue.push_back(i);
            in_queue[i] = true;
        }
    }
    let mut order: Vec<usize> = Vec::with_capacity(n);
    let mut emitted = vec![false; n];
    while let Some(i) = queue.pop_front() {
        in_queue[i] = false;
        if emitted[i] {
            continue;
        }
        emitted[i] = true;
        order.push(i);
        // 本行出列后,其 rc 值对应的组随之打开,唤醒等待者。
        for (_, rc) in &self_fks {
            let Some(v) = doc_i_rc(&docs[i], rc) else {
                continue;
            };
            let Some(k) = key_of(v) else { continue };
            let Some(&gid) = group_of.get(&k) else {
                continue;
            };
            if !open[gid] {
                open[gid] = true;
                for &w in &waiters[gid] {
                    if !emitted[w] && !in_queue[w] {
                        // 该依赖被满足,计数清到零入队(blocked 精确记账
                        // 复杂化,直接按「全部依赖已满足」重判一次)。
                        if deps_ready(&docs[w], &self_fks, &group_of, &members, &open, w) {
                            queue.push_back(w);
                            in_queue[w] = true;
                        }
                    }
                }
            }
        }
    }
    if order.len() == n {
        order.into_iter().map(|i| docs[i].clone()).collect()
    } else {
        // 环:剩余行按存储序输出,回放响亮报错。
        let mut out: Vec<Object> = order.into_iter().map(|i| docs[i].clone()).collect();
        out.extend((0..n).filter(|&i| !emitted[i]).map(|i| docs[i].clone()));
        out
    }
}

pub(super) fn doc_i_rc<'a>(doc: &'a Object, col: &str) -> Option<&'a Value> {
    doc.get(col).filter(|v| !matches!(**v, Value::Null))
}

pub(super) fn deps_ready(
    doc: &Object,
    self_fks: &[(&str, &str)],
    group_of: &std::collections::BTreeMap<Vec<u8>, usize>,
    members: &[Vec<usize>],
    open: &[bool],
    self_idx: usize,
) -> bool {
    for (lc, _) in self_fks {
        let Some(v) = doc.get(*lc) else { continue };
        if matches!(v, Value::Null) {
            continue;
        }
        let Some(k) = encode::encode_to_vec(v).ok() else {
            continue;
        };
        let Some(&gid) = group_of.get(&k) else {
            continue;
        };
        if !open[gid] && !members[gid].contains(&self_idx) {
            return false;
        }
    }
    true
}

/// Views in creation-safe order for a dump: every view after the views its
/// body references (CREATE VIEW dry-runs the body, so a view of a view needs
/// its base view restored first — alphabetical order would replay
/// `CREATE VIEW a AS SELECT * FROM z` before z exists). A cycle (not
/// constructible through CREATE VIEW; defensive) falls back to name order,
/// and replay fails loudly at the offending CREATE instead of silently
/// omitting the rest.
pub(super) fn view_dependency_order(
    names: &[String],
    tables: &std::collections::BTreeMap<String, std::sync::Arc<TableMeta>>,
) -> Vec<String> {
    let view_set: std::collections::BTreeSet<&str> = names.iter().map(|s| s.as_str()).collect();
    let objects = |n: &str| tables.contains_key(n);
    // Direct view→view edges (an unparseable body contributes none; the
    // CREATE would fail at replay time anyway).
    let edges: Vec<(String, Vec<String>)> = names
        .iter()
        .map(|n| {
            let mut deps = Vec::new();
            if let Some(sql) = tables.get(n).and_then(|m| m.view_sql.as_ref()) {
                if let Ok(mut stmts) = Parser::parse_sql(&GenericDialect {}, sql) {
                    if let Some(Statement::Query(q)) = stmts.pop() {
                        let mut refs = Vec::new();
                        if walk_query(&q, &mut refs, &objects).is_some() {
                            deps = refs
                                .into_iter()
                                .filter(|r| view_set.contains(r.as_str()))
                                .collect();
                        }
                    }
                }
            }
            (n.clone(), deps)
        })
        .collect();
    let mut out: Vec<String> = Vec::with_capacity(names.len());
    while out.len() < names.len() {
        let before = out.len();
        for (name, deps) in edges.iter() {
            if !out.contains(name) && deps.iter().all(|d| out.contains(d)) {
                out.push(name.clone());
            }
        }
        if out.len() == before {
            let rest: Vec<String> = edges
                .iter()
                .map(|(n, _)| n.clone())
                .filter(|n| !out.contains(n))
                .collect();
            out.extend(rest);
            break;
        }
    }
    out
}
