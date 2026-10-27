use super::*;

/// REQ_AUTH_USER: verify a username/password pair and resolve the user's
/// privileges. Returns (response frame, identity on success). The PBKDF2
/// derivation runs on the blocking pool; the stored credential and the
/// grants are read under the engine lock on either side of it. Failure
/// accounting (per-source lockout + audit trail) mirrors token auth, and a
/// missing user burns the same derivation so timing reveals nothing.
/// Record one failed authentication from `source_ip`: bump the counter,
/// prune idle sources, append the failure, and audit a lockout event when
/// the threshold is hit. Shared by token and user/password auth so their
/// lockout behavior cannot drift apart.
pub(super) async fn record_auth_failure(state: &Arc<ServerState>, source_ip: &str) {
    state
        .metrics
        .auth_failures_total
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if state.auth_lock_threshold > 0 {
        let mut failures = state.auth_failures.lock().await;
        let now = std::time::Instant::now();
        // Prune idle sources along the way: a scanner leaving one entry per
        // IP would otherwise grow the map forever (entries are only
        // otherwise dropped on that IP's success).
        failures.retain(|_, list| {
            list.retain(|t| now.duration_since(*t) < AUTH_LOCK_WINDOW);
            !list.is_empty()
        });
        let list = failures.entry(source_ip.to_string()).or_default();
        list.push(now);
        if list.len() as u32 == state.auth_lock_threshold {
            querylog::sync_event(
                &state.sync_log,
                "auth",
                source_ip,
                None,
                false,
                Some("lockout: repeated auth failures".into()),
            );
        }
    }
}

pub(super) async fn user_login_frame(
    state: &Arc<ServerState>,
    source_ip: &str,
    payload: &[u8],
) -> (Frame, Option<UserAuth>, u64) {
    let bad = || Frame::new(proto::RESP_ERROR, err_payload("bad username or password"));
    let parsed: std::result::Result<(String, String), String> = (|| {
        let v: serde_json::Value =
            serde_json::from_slice(payload).map_err(|e| format!("user auth: bad payload: {e}"))?;
        let name = v["user"]
            .as_str()
            .ok_or("user auth: expected {\"user\",\"password\"}")?;
        let pw = v["password"]
            .as_str()
            .ok_or("user auth: expected {\"user\",\"password\"}")?;
        Ok((name.trim().to_lowercase(), pw.to_string()))
    })();
    let (name, pw) = match parsed {
        Ok(c) => c,
        Err(m) => return (Frame::new(proto::RESP_ERROR, err_payload(&m)), None, 0),
    };
    if name.is_empty()
        || name.len() > 64
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
    {
        return (bad(), None, 0);
    }
    let stored = {
        let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
        db.user_stored_pw(&name)
    };
    // Server-wide derivation gate: verification is CPU-bound on the
    // blocking pool; queuing excess attempts keeps a distributed auth
    // flood from starving every other blocking task. The WAIT is bounded:
    // without a deadline a flood of parked queue-waiters grew without
    // limit and pushed legitimate logins minutes behind the backlog (the
    // pre-auth frame timeout only covers the socket read, not this gate).
    let permit =
        match tokio::time::timeout(PRE_AUTH_TIMEOUT, state.auth_gate.clone().acquire_owned()).await
        {
            Ok(p) => p.expect("auth gate never closed"),
            Err(_) => {
                return (
                    Frame::new(
                        proto::RESP_ERROR,
                        err_payload("authentication timeout; reconnect"),
                    ),
                    None,
                    0,
                );
            }
        };
    let ok = tokio::task::spawn_blocking(move || match stored {
        Some(s) => docsql_core::kdf::StoredPw::parse(&s)
            .map(|p| p.verify(&pw))
            .unwrap_or(false),
        None => {
            let decoy = docsql_core::kdf::hash_password("x", &[0u8; 16]);
            if let Some(p) = docsql_core::kdf::StoredPw::parse(&decoy) {
                let _ = p.verify(&pw);
            }
            false
        }
    })
    .await
    .unwrap_or(false);
    drop(permit);
    if !ok {
        record_auth_failure(state, source_ip).await;
        querylog::sync_event(
            &state.sync_log,
            "auth",
            source_ip,
            None,
            false,
            Some("user login failed".into()),
        );
        return (bad(), None, 0);
    }
    let (grants, grants_epoch_now) = {
        let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
        // Sample the grants epoch INSIDE the critical section that resolves
        // the grants: a REVOKE applies under this lock and bumps the epoch
        // only after releasing it, so the "old grants + new epoch" pair
        // (which would skip every per-frame refresh forever) cannot be
        // assembled here — seeing the new epoch implies the revoke already
        // landed in the resolved grants.
        let g = docsql_core::useradmin::resolve_grants(&mut db, &name)
            .ok()
            .flatten();
        (
            g,
            state.grants_epoch.load(std::sync::atomic::Ordering::SeqCst),
        )
    };
    match grants {
        Some(g) => {
            state.auth_failures.lock().await.remove(source_ip);
            querylog::sync_event(
                &state.sync_log,
                "auth",
                source_ip,
                None,
                true,
                Some(format!("user {name} logged in")),
            );
            (
                Frame::new(
                    proto::RESP_AFFECTED,
                    format!("ok(user:{name})").into_bytes(),
                ),
                Some(UserAuth { name, grants: g }),
                grants_epoch_now,
            )
        }
        // Raced with DROP USER between verify and resolve.
        None => (bad(), None, 0),
    }
}

/// Identity-authentication failure handling: after this many failed AUTHs
/// from one source inside the window, further attempts from that source are
/// rejected unread until the lockout elapses. Lenient enough for fat-fingered
/// clients, tight enough to make online guessing impractical.
pub const AUTH_LOCK_THRESHOLD: u32 = 10;
pub(crate) const AUTH_LOCK_WINDOW: std::time::Duration = std::time::Duration::from_secs(60);
pub const AUTH_LOCKOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Minimum accepted credential length, enforced by the server binary for
/// every configured token (password complexity floor; the e2e suites build
/// `ServerConfig` directly and use short test tokens).
pub const MIN_TOKEN_LEN: usize = 8;

/// Credential complexity floor for configured tokens: reject obviously
/// weak secrets (too short, or a single repeated character) at startup so
/// a deployment cannot ship with a guessable credential.
pub fn check_token_strength(name: &str, token: &str) -> Result<(), String> {
    if token.chars().count() < MIN_TOKEN_LEN {
        return Err(format!(
            "{name} is too weak: use at least {MIN_TOKEN_LEN} characters"
        ));
    }
    let mut chars = token.chars();
    let first = chars.next().unwrap_or_default();
    if token.chars().all(|c| c == first) {
        return Err(format!("{name} is too weak: single repeated character"));
    }
    Ok(())
}

/// Per-statement privilege check for username/password connections.
/// Token/anonymous-legacy connections never reach here. Fails closed: a
/// query shape the read-target walker cannot fully classify is denied.
/// Row-filter enforcement (`GRANT SELECT ON t WHERE <pred> …`): rewrite
/// the statement TEXT so each filtered table's predicate rides as an
/// ordinary WHERE conjunct. The engine then treats the filter like any
/// predicate — the COUNT/window/probe fast paths stay semantically exact —
/// and the journal/peer replay carry the FILTERED text, so replays without
/// a user identity reproduce exactly what the origin executed.
///
/// Scope (everything else refuses loudly when a filtered table is
/// involved — fail-closed, never a silent bypass):
/// - single-table SELECT (no JOINs / set ops / WITH);
/// - INSERT whose source is such a SELECT (the source is rewritten);
/// - single-table UPDATE / DELETE (predicate ANDed into WHERE);
/// - EXPLAIN over a single-table SELECT.
///
/// Subquery or view occurrences of a filtered table are caught by an
/// occurrence-count check (deep read targets vs rewritten positions) and
/// a transitive view-base scan.
pub(super) fn apply_row_filters(
    db: &docsql_core::engine::Database,
    sql: &str,
    filters: &std::collections::BTreeMap<String, Vec<String>>,
) -> Result<String, String> {
    use sqlparser::ast::{Expr as E, SetExpr, Statement, TableFactor};
    let lower = |s: &str| s.to_ascii_lowercase();
    let filtered: std::collections::BTreeSet<String> = filters.keys().map(|k| lower(k)).collect();
    if filtered.is_empty() {
        return Ok(sql.to_string());
    }
    // Shapes the SQL parser or the classifier cannot fully handle (T-SQL
    // batches reach execution through the interpreter; exotic reads): the
    // raw text mentioning a filtered table refuses — fail-closed, never a
    // silent bypass through an unparseable statement.
    let mentions_filtered = || {
        let low = sql.to_ascii_lowercase();
        filtered.iter().any(|t| contains_word_ci(&low, t))
    };
    let parsed = match Database::parse_classified(sql) {
        Ok(p) => p,
        Err(_) => {
            return if mentions_filtered() {
                Err("row filters: statement shape cannot be classified; refusing".to_string())
            } else {
                Ok(sql.to_string())
            };
        }
    };
    let AnyStmt::Sql(stmt) = &parsed.stmt else {
        return if mentions_filtered() {
            Err("row filters apply to SQL statements only".into())
        } else {
            Ok(sql.to_string())
        };
    };
    // Deep read targets (per-occurrence): the coverage proof below needs
    // duplicates preserved. UPDATE/DELETE target tables join the set —
    // they are WRITE targets, but their rows are exactly what a filter on
    // them must protect (without this the early-exit below silently
    // skipped `UPDATE sales SET … WHERE …`).
    let objects = |n: &str| db.table_exists(n);
    let mut targets = match Database::stmt_read_target_occurrences(stmt, &objects) {
        Some(t) => t,
        None => {
            return if mentions_filtered() {
                Err("row filters: statement shape cannot be classified; refusing".to_string())
            } else {
                Ok(sql.to_string())
            };
        }
    };
    match stmt.as_ref() {
        Statement::Update(u) => {
            if let TableFactor::Table { name, .. } = &u.table.relation {
                targets.push(docsql_core::engine::table_ref_name(&name.clone()));
            }
        }
        Statement::Delete(d) => {
            // Both spellings: plain `DELETE FROM t` parses as WithKeyword
            // in sqlparser 0.62 (WithoutKeyword is the MySQL `DELETE t
            // FROM …` form).
            let factors = match &d.from {
                sqlparser::ast::FromTable::WithoutKeyword(v) => v,
                sqlparser::ast::FromTable::WithFromKeyword(v) => v,
            };
            for f in factors {
                if let TableFactor::Table { name, .. } = &f.relation {
                    targets.push(docsql_core::engine::table_ref_name(&name.clone()));
                }
            }
        }
        _ => {}
    }
    // Views over filtered tables: a view is its reader's permission
    // boundary, but a row filter on a BASE table must survive through it —
    // the rewrite cannot see inside the stored definition, so access is
    // refused (fail-closed).
    for t in &targets {
        let mut queue = vec![t.clone()];
        let mut seen = std::collections::BTreeSet::new();
        let mut depth = 0usize;
        while let Some(v) = queue.pop() {
            if !seen.insert(v.clone()) {
                continue;
            }
            if let Some(bases) = db.view_base_tables(&v) {
                depth += 1;
                if depth > 16 {
                    return Err("row filters: view chain too deep to verify".into());
                }
                for b in bases {
                    if filtered.contains(&lower(&b)) {
                        return Err(format!(
                            "view {t} reads row-filtered table {b}; \
                             row filters cannot protect it through the view"
                        ));
                    }
                    queue.push(b);
                }
            }
        }
    }
    // Nothing filtered is referenced: the statement passes untouched.
    if !targets.iter().any(|t| filtered.contains(&lower(t))) {
        return Ok(sql.to_string());
    }
    let lookup = |t: &str| -> Option<E> {
        let preds = filters
            .iter()
            .find(|(k, _)| lower(k) == lower(t))
            .map(|(_, v)| v)?;
        let mut parts = preds
            .iter()
            .map(|p| docsql_core::engine::parse_expr_text(p))
            .collect::<Result<Vec<_>, _>>()
            .ok()?;
        let mut acc = parts.remove(0);
        while let Some(next) = parts.pop() {
            acc = E::BinaryOp {
                left: Box::new(acc),
                op: sqlparser::ast::BinaryOperator::Or,
                right: Box::new(next),
            };
        }
        Some(acc)
    };
    fn and_into_sel(old: Option<E>, pred: E) -> E {
        // Parenthesize both sides as needed before AND-splicing:
        // sqlparser's Display emits BinaryOp children WITHOUT precedence
        // parentheses, so `user_or_tree AND filter` re-parsed as
        // `left-most OR right-most AND …` flipped the binding and let
        // `WHERE x = 1 OR y = 2` bypass the injected filter entirely.
        let wrap_if_or = |e: E| match e {
            E::BinaryOp {
                op: sqlparser::ast::BinaryOperator::Or,
                ..
            } => E::Nested(Box::new(e)),
            other => other,
        };
        let right = E::Nested(Box::new(pred));
        match old {
            Some(left) => E::BinaryOp {
                left: Box::new(wrap_if_or(left)),
                op: sqlparser::ast::BinaryOperator::And,
                right: Box::new(right),
            },
            None => right,
        }
    }
    // Rewrite ONE top-level plain table factor: single-table SELECT shape.
    // JOINs / derived tables / set operations fall through untouched; the
    // coverage check below refuses them when they reference a filtered
    // table (fail-closed).
    fn rewrite_select_factor(
        select: &mut sqlparser::ast::Select,
        filters: &std::collections::BTreeMap<String, Vec<String>>,
        rewritten_tables: &mut Vec<String>,
    ) {
        if select.from.len() != 1 {
            return;
        }
        let twj = &mut select.from[0];
        if !twj.joins.is_empty() {
            return;
        }
        let table = match &twj.relation {
            TableFactor::Table { name, .. } => docsql_core::engine::table_ref_name(&name.clone()),
            _ => return,
        };
        let Some(preds) = filters
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(&table))
            .map(|(_, v)| v)
        else {
            return;
        };
        let Ok(mut parts) = preds
            .iter()
            .map(|p| docsql_core::engine::parse_expr_text(p))
            .collect::<Result<Vec<_>, _>>()
        else {
            return; // validated at GRANT time; unreachable in practice
        };
        let mut acc = parts.remove(0);
        while let Some(next) = parts.pop() {
            acc = E::BinaryOp {
                left: Box::new(acc),
                op: sqlparser::ast::BinaryOperator::Or,
                right: Box::new(next),
            };
        }
        select.selection = Some(and_into_sel(select.selection.take(), acc));
        rewritten_tables.push(table.to_ascii_lowercase());
    }
    let mut rewritten_tables: Vec<String> = Vec::new();
    let mut new_stmt = (**stmt).clone();
    match &mut new_stmt {
        Statement::Query(q) => {
            if let SetExpr::Select(select) = q.body.as_mut() {
                rewrite_select_factor(select.as_mut(), filters, &mut rewritten_tables);
            }
        }
        Statement::Insert(ins) => {
            if let Some(source) = &mut ins.source {
                if let SetExpr::Select(select) = source.body.as_mut() {
                    rewrite_select_factor(select.as_mut(), filters, &mut rewritten_tables);
                }
            }
        }
        Statement::Update(u) => {
            let table = match &u.table.relation {
                TableFactor::Table { name, .. } => {
                    docsql_core::engine::table_ref_name(&name.clone())
                }
                _ => String::new(),
            };
            // UPDATE ... FROM evaluates the WHERE over the MERGED image
            // (row keys are `alias.col`): the injected predicate's bare
            // columns resolve through lookup_col's suffix fallback and can
            // bind to a FROM table's same-named column — a controlled FROM
            // table short-circuits the filter for every row. Refuse the
            // shape (fail-closed, same treatment as JOIN/子查询).
            if u.from.is_some() && filtered.contains(&lower(&table)) {
                return Err(format!(
                    "row filter on {table}: UPDATE ... FROM evaluates its WHERE over the \
                     joined image, where the filter's bare columns can bind to the FROM \
                     tables; rewrite without FROM or use an unrestricted grant"
                ));
            }
            if let Some(pred) = lookup(&table) {
                u.selection = Some(and_into_sel(u.selection.take(), pred));
                rewritten_tables.push(table.to_ascii_lowercase());
            }
        }
        Statement::Delete(d) => {
            // Same two-spellings rule as the mention set above.
            // `DELETE ... USING` evaluates the WHERE over the merged image
            // exactly like UPDATE ... FROM: the injected predicate's bare
            // columns can bind to a USING table's same-named column and
            // short-circuit the filter — refuse the shape (fail-closed).
            let del_target = match &d.from {
                sqlparser::ast::FromTable::WithoutKeyword(v)
                | sqlparser::ast::FromTable::WithFromKeyword(v) => {
                    if v.len() == 1 {
                        match &v[0].relation {
                            TableFactor::Table { name, .. } => {
                                docsql_core::engine::table_ref_name(name)
                            }
                            _ => String::new(),
                        }
                    } else {
                        String::new()
                    }
                }
            };
            if d.using.is_some() && !del_target.is_empty() && filtered.contains(&lower(&del_target))
            {
                return Err(format!(
                    "row filter on {del_target}: DELETE ... USING evaluates its WHERE over the \
                     joined image, where the filter's bare columns can bind to the USING \
                     tables; rewrite without USING or use an unrestricted grant"
                ));
            }
            let factors = match &mut d.from {
                sqlparser::ast::FromTable::WithoutKeyword(v) => v,
                sqlparser::ast::FromTable::WithFromKeyword(v) => v,
            };
            if factors.len() == 1 && factors[0].joins.is_empty() {
                if let TableFactor::Table { name, .. } = &factors[0].relation {
                    let table = docsql_core::engine::table_ref_name(name);
                    if let Some(pred) = lookup(&table) {
                        d.selection = Some(and_into_sel(d.selection.take(), pred));
                        rewritten_tables.push(table.to_ascii_lowercase());
                    }
                }
            }
        }
        Statement::Explain { statement, .. } => {
            // `EXPLAIN SELECT … FROM <filtered table>`: the walker counts
            // the inner query's reads as deep occurrences, so rewrite the
            // inner SELECT the same way — a restricted user can inspect
            // the plan of exactly the statements they may run (this used
            // to refuse, while the doc comment claimed support).
            if let Statement::Query(q) = statement.as_mut() {
                if let SetExpr::Select(select) = q.body.as_mut() {
                    rewrite_select_factor(select.as_mut(), filters, &mut rewritten_tables);
                }
            }
        }
        _ => {}
    }
    // Coverage proof: every filtered table's DEEP occurrence count must be
    // exactly the number of top-level rewrites — one more occurrence
    // anywhere (subquery, JOIN side, second UNION arm) is an unfilterable
    // read and refuses the statement.
    for t in &filtered {
        let deep = targets.iter().filter(|x| lower(x) == *t).count();
        let top = rewritten_tables.iter().filter(|x| x.as_str() == *t).count();
        if deep > top {
            return Err(format!(
                "row filter on {t}: the statement reads {t} in a position row filters \
                 cannot protect (subquery / JOIN / view / set operation); rewrite the \
                 statement or use an unrestricted grant"
            ));
        }
    }
    let rewritten_text = new_stmt.to_string();
    // The serialized AST must re-parse (peers and the journal consume the
    // text); a round-trip failure is a loud error, not a bypass.
    Database::parse_classified(&rewritten_text).map_err(|e| e.to_string())?;
    Ok(rewritten_text)
}

/// ASCII-case-insensitive word-boundary containment (`t` as a whole word
/// in `text`, which arrives lowercased).
pub(super) fn contains_word_ci(text: &str, word: &str) -> bool {
    let wb = word.as_bytes();
    let tb = text.as_bytes();
    if wb.is_empty() {
        return false;
    }
    let mut i = 0;
    while i + wb.len() <= tb.len() {
        if &tb[i..i + wb.len()] == wb {
            let before_ok = i == 0 || !tb[i - 1].is_ascii_alphanumeric();
            let after = tb.get(i + wb.len()).copied();
            let after_ok = after.is_none_or(|c| !c.is_ascii_alphanumeric());
            if before_ok && after_ok {
                return true;
            }
        }
        i += 1;
    }
    false
}

pub(super) fn authorize_statement(
    db: Option<&docsql_core::engine::Database>,
    stmt: &AnyStmt,
    tx_kind: &TxControl,
    is_write: bool,
    g: &docsql_core::useradmin::UserGrants,
) -> std::result::Result<(), String> {
    use docsql_core::useradmin::{PRIV_DELETE, PRIV_INSERT, PRIV_UPDATE};
    if g.admin {
        return Ok(());
    }
    match stmt {
        AnyStmt::UserAdmin(_) => Err("user and role management requires the admin role".into()),
        // REFRESH rewrites a whole derived table: DDL-grade, admin only
        // (mirrors every other CREATE/DROP/ALTER shape).
        AnyStmt::RefreshMatview(_) => {
            Err("materialized view refresh requires the admin role".into())
        }
        AnyStmt::Sql(s) => {
            // Transaction control itself is not privileged; the buffered
            // statements were each authorized when they arrived.
            if !matches!(tx_kind, TxControl::None) {
                return Ok(());
            }
            use sqlparser::ast::Statement as S;
            if !is_write {
                let objects = |n: &str| db.is_some_and(|d| d.table_exists(n));
                let Some(targets) = Database::stmt_read_targets(s, &objects) else {
                    return Err(
                        "this statement shape cannot be authorized for user connections".into(),
                    );
                };
                // A view is a SELECT permission boundary on the READ path
                // too: expand its base tables (fail-closed, chain capped)
                // exactly like the write path does. Without this, a view
                // over docsql_users left on a pre-upgrade volume handed
                // password hashes to any readonly/readwrite connection —
                // and the create-time refusal cannot help there (dumps
                // from old volumes must keep replaying).
                let targets = {
                    let mut queue = targets;
                    let mut seen = std::collections::BTreeSet::new();
                    let mut out: Vec<String> = Vec::new();
                    let mut depth = 0usize;
                    while let Some(t) = queue.pop() {
                        if !seen.insert(t.clone()) {
                            continue;
                        }
                        match db.and_then(|d| d.view_base_tables(&t)) {
                            Some(bases) => {
                                // A direct table grant on the view itself is
                                // a valid SELECT boundary: the view is the
                                // permission surface and its bases stay
                                // hidden (standard SQL view semantics — the
                                // grant row could previously never match).
                                // readonly/readwrite blanket access must NOT
                                // take this shortcut: the base expansion is
                                // exactly what refuses user-table views to
                                // those roles. And even for a direct view
                                // grant, the user/role subsystem stays
                                // admin-only behind ANY depth of views —
                                // GRANT-time checks only see the view name,
                                // so a view whose closure names a user table
                                // must not hand password hashes out here.
                                if !g.readonly && !g.readwrite && g.may_select(&t) {
                                    let mut closure: Vec<String> = bases.clone();
                                    let mut cseen = std::collections::BTreeSet::new();
                                    cseen.insert(t.clone());
                                    while let Some(b) = closure.pop() {
                                        if !cseen.insert(b.clone()) {
                                            continue;
                                        }
                                        if docsql_core::useradmin::is_user_table(&b) {
                                            return Err(
                                                "user/role data is visible to the admin role only"
                                                    .into(),
                                            );
                                        }
                                        if let Some(deeper) =
                                            db.and_then(|d| d.view_base_tables(&b))
                                        {
                                            closure.extend(deeper);
                                        }
                                    }
                                    continue;
                                }
                                depth += 1;
                                if depth > 16 {
                                    return Err("view chain too deep to authorize".into());
                                }
                                queue.extend(bases);
                            }
                            None => out.push(t),
                        }
                    }
                    out
                };
                for t in &targets {
                    if docsql_core::useradmin::is_user_table(t) {
                        return Err("user/role data is visible to the admin role only".into());
                    }
                    if readable_by_all(t) {
                        continue;
                    }
                    if !g.may_select(t) {
                        return Err(format!(
                            "SELECT on table {t} requires the readonly/readwrite role \
                             or a table grant"
                        ));
                    }
                }
                // Column-restricted grants (`GRANT SELECT (a, b) ON t`):
                // every read target must be attributable to a single-table
                // SELECT whose referenced columns are all granted. Anything
                // else (wildcards, joins, subqueries, multi-table and
                // unattributable shapes) refuses — fail-closed, never a
                // silent widening.
                if targets.iter().any(|t| g.is_restricted_select(t)) {
                    match docsql_core::engine::single_table_select_refs(s) {
                        Some((table, cols)) => {
                            for rt in &targets {
                                if !g.is_restricted_select(rt) {
                                    continue;
                                }
                                if !rt.eq_ignore_ascii_case(&table) {
                                    return Err(format!(
                                        "SELECT on {rt} with column-restricted grants requires \
                                         a single-table SELECT of exactly that table"
                                    ));
                                }
                                let granted = g.granted_cols(rt);
                                for c in &cols {
                                    if !granted.iter().any(|x| x.eq_ignore_ascii_case(c)) {
                                        return Err(format!(
                                            "SELECT on column {c} of {table} is not granted \
                                             (the grant lists its columns)"
                                        ));
                                    }
                                }
                            }
                        }
                        None => {
                            return Err("this statement shape cannot be authorized against \
                                 column-restricted grants (wildcards, joins, subqueries, \
                                 multi-table statements and EXPLAIN are not attributable)"
                                .into());
                        }
                    }
                }
                return Ok(());
            }
            let (bit, label) = match s.as_ref() {
                S::Insert(_) => (PRIV_INSERT, "INSERT"),
                S::Update(_) => (PRIV_UPDATE, "UPDATE"),
                S::Delete(_) => (PRIV_DELETE, "DELETE"),
                S::Truncate(_) => (PRIV_DELETE, "TRUNCATE"),
                // MERGE is ordinary DML (write-targets, read-targets and the
                // system-table gate all classify it); it used to fall into
                // the DDL refusal. Require the privileges of the arms the
                // statement actually carries, each held on the target.
                S::Merge(m) => {
                    let mut bits: u8 = 0;
                    for c in &m.clauses {
                        bits |= match &c.action {
                            sqlparser::ast::MergeAction::Insert(_) => PRIV_INSERT,
                            sqlparser::ast::MergeAction::Update(_) => PRIV_UPDATE,
                            sqlparser::ast::MergeAction::Delete { .. } => PRIV_DELETE,
                        };
                    }
                    let targets = Database::stmt_write_targets(s);
                    for t in &targets {
                        for (b, l) in [
                            (PRIV_INSERT, "INSERT"),
                            (PRIV_UPDATE, "UPDATE"),
                            (PRIV_DELETE, "DELETE"),
                        ] {
                            if bits & b != 0 && !g.may_dml(t, b) {
                                return Err(format!(
                                    "MERGE arm {l} on table {t} requires the readwrite role \
                                     or a table grant"
                                ));
                            }
                        }
                    }
                    // Arm bits are already verified above; fall through to
                    // the read-source checks with a no-op bit.
                    (0, "MERGE")
                }
                _ => return Err("DDL and administrative statements require the admin role".into()),
            };
            let mut write_targets = Database::stmt_write_targets(s);
            // TRUNCATE ... CASCADE clears the FK children too (transitive
            // closure, same as execution time) — those tables are as much
            // authorization input as the named ones. Without this, a user
            // holding DELETE on the parent alone could empty child tables
            // they hold no grant on at all.
            if let S::Truncate(tr) = s.as_ref() {
                if tr.cascade == Some(sqlparser::ast::CascadeOption::Cascade) {
                    match db {
                        Some(db) => {
                            let mut targets = write_targets.clone();
                            loop {
                                let referencing: Vec<String> = db
                                    .table_foreign_keys_of(&targets)
                                    .into_iter()
                                    .filter(|t| !targets.contains(t))
                                    .collect();
                                if referencing.is_empty() {
                                    break;
                                }
                                targets.extend(referencing);
                            }
                            write_targets = targets;
                        }
                        None => {
                            return Err(
                                "TRUNCATE ... CASCADE cannot be authorized while any cascade \
                                 target fails to resolve (views or unresolvable references \
                                 among the cascaded tables); resolve them first"
                                    .into(),
                            );
                        }
                    }
                }
            }
            for t in write_targets {
                // bit == 0: the MERGE arm verified each arm-bit above.
                if bit != 0 && !g.may_dml(&t, bit) {
                    return Err(format!(
                        "{label} on table {t} requires the readwrite role or a table grant"
                    ));
                }
            }
            // A write also READS: the INSERT .. SELECT source, UPDATE
            // assignment subqueries, MERGE WHEN predicates. Those tables are
            // authorization input — without this check a user holding only a
            // write grant could copy docsql_users password hashes (or any
            // ungranted table) into a table of their own and read them back.
            let objects = |n: &str| db.is_some_and(|d| d.table_exists(n));
            let sources = Database::stmt_read_targets(s, &objects).ok_or_else(|| {
                "this statement shape cannot be authorized for user connections".to_string()
            })?;
            // Column-restricted grants bind on WRITE sources too: an
            // INSERT .. SELECT copying an ungranted column into one's own
            // table used to pass the table-level may_select alone and leak
            // the value on read-back.
            let restricted: Vec<&String> = sources
                .iter()
                .filter(|t| g.is_restricted_select(t))
                .collect();
            if !restricted.is_empty() {
                let attributed = match s.as_ref() {
                    S::Insert(ins) => ins
                        .source
                        .as_ref()
                        .and_then(|q| docsql_core::engine::single_table_query_refs(q.as_ref())),
                    _ => None,
                };
                match attributed {
                    Some((table, cols)) => {
                        for rt in &restricted {
                            if !rt.eq_ignore_ascii_case(&table) {
                                return Err(format!(
                                    "reading {rt} with column-restricted grants requires \
                                     a single-table SELECT of exactly that table"
                                ));
                            }
                            let granted = g.granted_cols(rt);
                            for c in &cols {
                                if !granted.iter().any(|x| x.eq_ignore_ascii_case(c)) {
                                    return Err(format!(
                                        "SELECT on column {c} of {table} is not granted \
                                         (the grant lists its columns)"
                                    ));
                                }
                            }
                        }
                    }
                    None => {
                        return Err(
                            "write sources cannot be authorized against column-restricted \
                             grants (only a plain single-table INSERT ... SELECT is \
                             attributable; wildcards, joins, subqueries, UPDATE and MERGE \
                             sources are not)"
                                .into(),
                        );
                    }
                }
            }
            // View expansion: a view is a SELECT permission boundary, not a
            // license to write through it. A write reading a view needs the
            // grants on the view's BASE tables (fail-closed, chain capped).
            let sources = {
                let mut queue: Vec<String> = sources;
                let mut seen = std::collections::BTreeSet::new();
                let mut out: Vec<String> = Vec::new();
                let mut depth = 0usize;
                while let Some(t) = queue.pop() {
                    if !seen.insert(t.clone()) {
                        continue;
                    }
                    match db.and_then(|d| d.view_base_tables(&t)) {
                        Some(bases) => {
                            depth += 1;
                            if depth > 16 {
                                return Err("view chain too deep to authorize".into());
                            }
                            queue.extend(bases);
                        }
                        None => out.push(t),
                    }
                }
                out
            };
            for t in &sources {
                if docsql_core::useradmin::is_user_table(t) {
                    return Err("user/role data is visible to the admin role only".into());
                }
                if readable_by_all(t) {
                    continue;
                }
                if !g.may_select(t) {
                    return Err(format!(
                        "SELECT on table {t} requires the readonly/readwrite role \
                         or a table grant"
                    ));
                }
            }
            Ok(())
        }
    }
}
