use super::*;

impl Database {
    /// Parse a single statement once and classify it in the same pass: the
    /// AST feeds [`Database::execute_parsed`], the flags route the request
    /// (write path, transaction control) without re-parsing the text.
    /// User-management statements are hand-parsed first (sqlparser 0.62
    /// rejects their grammar) and always classify as writes.
    ///
    /// Results go through the process-wide statement cache: parsing is pure
    /// syntax — nothing catalog- or connection-dependent — so entries never
    /// go stale and no invalidation exists to get wrong. Every tier parses
    /// through here (write path, MVCC read views, routing probes), so a
    /// repeated point query skips the ~4 µs sqlparser pass.
    pub fn parse_classified(sql: &str) -> Result<ParsedStatement> {
        const CACHE_MAX_SQL: usize = 4096;
        const CACHE_ENTRIES: usize = 256;
        const SEEN_CAP: usize = 1024;
        let cacheable = sql.len() <= CACHE_MAX_SQL;
        if cacheable {
            let hit = stmt_cache()
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(sql)
                .cloned();
            if let Some(parsed) = hit {
                return Ok(parsed);
            }
        }
        let parsed = Self::parse_classified_uncached(sql)?;
        // User-management statements carry the plaintext password in the AST
        // (the resolved hash rewrite happens at execution): caching them
        // would pin credentials in process-wide memory beyond the statement
        // and outlive the "plaintext never leaves the executing path"
        // contract.
        let cacheable = cacheable && !matches!(parsed.stmt, AnyStmt::UserAdmin(_));
        if cacheable {
            // Second-sight gate: pay the cache-write cost (an AST clone)
            // only for text seen before, so distinct-literal workloads —
            // per-row generated lookups, rendered bindings — skip it. The
            // u64 hash is a heuristic only: a false positive merely writes
            // the cache early (the cache key is the exact text, so
            // correctness never depends on the hash), a false negative
            // just delays caching by one sight.
            let mut seen = stmt_seen().lock().unwrap_or_else(|p| p.into_inner());
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            sql.hash(&mut hasher);
            let h = hasher.finish();
            if seen.contains(&h) {
                drop(seen);
                let mut cache = stmt_cache().lock().unwrap_or_else(|p| p.into_inner());
                if cache.len() >= CACHE_ENTRIES {
                    cache.clear();
                }
                cache.insert(sql.to_string(), parsed.clone());
            } else {
                if seen.len() >= SEEN_CAP {
                    seen.clear();
                }
                seen.insert(h);
            }
        }
        Ok(parsed)
    }

    /// The uncached parse behind [`Self::parse_classified`].
    pub(super) fn parse_classified_uncached(sql: &str) -> Result<ParsedStatement> {
        // REFRESH MATERIALIZED VIEW [CONCURRENTLY] name — the generic
        // dialect has no REFRESH statement; hand-parse the fixed shape
        // (same pattern as the user-management statements).
        {
            let up = sql.trim_start().to_ascii_uppercase();
            let body = up
                .strip_prefix("REFRESH MATERIALIZED VIEW ")
                .or_else(|| up.strip_prefix("REFRESH MATERIALIZED VIEW	"));
            if let Some(rest) = body {
                let mut parts = rest.split_ascii_whitespace();
                let mut name_part = parts.next().unwrap_or("");
                if name_part == "CONCURRENTLY" {
                    // PG's CONCURRENTLY needs a unique index to delta-apply;
                    // this engine rewrites wholesale — accept and ignore.
                    name_part = parts.next().unwrap_or("");
                }
                if parts.next().is_some() || name_part.is_empty() {
                    return err("REFRESH MATERIALIZED VIEW takes exactly one view name");
                }
                // Recover the original spelling from the source text: the
                // name is the last token of the statement (after the
                // optional CONCURRENTLY).
                let tokens: Vec<&str> = sql.trim().split_ascii_whitespace().collect();
                let name = tokens
                    .last()
                    .copied()
                    .unwrap_or("")
                    .trim_end_matches(';')
                    .to_string();
                if name.is_empty()
                    || !name
                        .chars()
                        .next()
                        .map(|c| c.is_alphanumeric() || c == '_' || c == '"' || c == '#')
                        .unwrap_or(false)
                {
                    return err("REFRESH MATERIALIZED VIEW takes exactly one view name");
                }
                return Ok(ParsedStatement {
                    stmt: AnyStmt::RefreshMatview(name),
                    tx: TxControl::None,
                    is_write: true,
                    source: sql.to_string(),
                });
            }
        }
        match crate::useradmin::parse(sql) {
            Some(Ok(ua)) => {
                return Ok(ParsedStatement {
                    stmt: AnyStmt::UserAdmin(ua),
                    tx: TxControl::None,
                    is_write: true,
                    source: sql.to_string(),
                })
            }
            Some(Err(m)) => return err(m),
            None => {}
        }
        // T-SQL notations the generic parser cannot express ([brackets],
        // CONVERT/PARSE argument order, SET/USE/PRINT shims) are rewritten
        // on a text copy; `source` keeps the original text so journals and
        // logs carry what the user wrote (the rewrite is deterministic and
        // re-applies on every replay).
        let parsed_sql = crate::tsql::preprocess(sql);
        let parsed_sql = parsed_sql.as_str();
        let mut stmts = Parser::parse_sql(&GenericDialect {}, parsed_sql)
            .map_err(|e| SqlError::Parse(e.to_string()))?;
        let stmt = match stmts.len() {
            0 => return err("empty statement"),
            1 => stmts.swap_remove(0),
            _ => return err("exactly one statement per execute() call"),
        };
        let tx = Self::classify_tx(&stmt);
        let is_write = Self::stmt_is_write(&stmt);
        Ok(ParsedStatement {
            stmt: AnyStmt::Sql(Box::new(stmt)),
            tx,
            is_write,
            source: sql.to_string(),
        })
    }

    /// True when the catalog holds a table with this exact name.
    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    /// ASCII-case-insensitive catalog probe: the server-side compat-view
    /// rewrites (docsql_pubsub / docsql_log) must not shadow a user table
    /// under ANY spelling of the name — an exact-only probe let
    /// `DocSQL_PubSub` INSERT land in the real table while the SELECT was
    /// silently rewritten onto the system view.
    pub fn table_exists_ci(&self, name: &str) -> bool {
        self.tables
            .keys()
            .any(|k| k.len() == name.len() && k.eq_ignore_ascii_case(name))
    }

    /// Arm the cooperative deadline for the statement about to execute
    /// (server-side statement timeout). Engine-global, not per connection:
    /// the caller MUST clear it (`set_statement_deadline(None)`) once the
    /// statement finished, on every path.
    pub fn set_statement_deadline(&self, deadline: Option<std::time::Instant>) {
        self.stmt_deadline.set(deadline);
    }

    /// True when a session transaction is open.
    pub fn in_transaction(&self) -> bool {
        self.tx_snapshot.is_some()
    }

    /// Build a guardless read view (MVCC stage B): clones the catalog as
    /// `Arc` bumps and begins a pager snapshot at the current commit head.
    /// The caller must hold *a* database lock while calling this; the view
    /// is executed after the guard is released, so long SELECTs never block
    /// writers. Dropping the view ends the snapshot.
    pub fn read_view(&self) -> ReadView {
        ReadView {
            pager: self.pager.clone(),
            catalog: CatalogSnapshot(self.tables.clone()),
            deadline: std::sync::Arc::new(StmtDeadline::default()),
            snap: self.pager.begin_snapshot(),
        }
    }

    /// Borrowed read context under the current lock: current visibility,
    /// no snapshot. Write paths and the census use this to share the SELECT
    /// chain with the guardless stage-B views.
    pub(crate) fn read_cx(&self) -> ReadCx<'_> {
        ReadCx {
            pager: &self.pager,
            tables: &self.tables,
            deadline: &self.stmt_deadline,
            snap: None,
        }
    }

    /// The plain-SELECT gate shared by both read paths (stage A
    /// [`Database::execute_read`] and stage B [`ReadView::execute`]):
    /// writes, transaction control and WITH queries are refused — the
    /// server tiers them to the write lock (WITH materialization is
    /// statement-local mutable state). Returns the query to execute.
    pub(super) fn plain_read_query(parsed: &ParsedStatement) -> Result<Query> {
        if parsed.is_write {
            return err("read path refused a write statement");
        }
        if parsed.tx != TxControl::None {
            return err("read path refused a transaction statement");
        }
        match &parsed.stmt {
            AnyStmt::Sql(stmt) => match &**stmt {
                Statement::Query(q) if q.with.is_none() => Ok((**q).clone()),
                Statement::Query(_) => err("read path: WITH queries must use the write path"),
                _ => err("read path: statement is not a query"),
            },
            _ => err("read path: statement is not a query"),
        }
    }

    /// Read-only execution path (MVCC stage A): plain SELECT only, callable
    /// on `&Database` under the server's read lock so read-only statements
    /// run concurrently. Guardless reads (stage B) go through
    /// [`Database::read_view`] instead.
    ///
    /// No statement deadline here: this path shares the engine-global slot
    /// the write path arms (a concurrent arming would be overwritten), so
    /// stage-A reads are not preemptible. Stage-B views carry their own
    /// deadline (see `ReadView::set_statement_deadline`).
    pub fn execute_read(&self, sql: &str) -> Result<ExecOutcome> {
        let q = Self::plain_read_query(&Self::parse_classified(sql)?)?;
        self.exec_query_cx(q)
    }

    // Write-path shims: run the shared SELECT-chain helpers under a borrowed
    // context (current visibility — the write lock already excludes snapshot
    // readers from mutating anything mid-statement).
    /// Mutable catalog entry for the write path: `Arc::make_mut` deep-clones
    /// the meta only while a detached stage-B reader still holds the previous
    /// version (a concurrent long SELECT); single-writer idle runs mutate in
    /// place exactly as before.
    pub(super) fn catalog_mut(&mut self, name: &str) -> Option<&mut TableMeta> {
        match self.tables.get_mut(name) {
            Some(a) => Some(std::sync::Arc::make_mut(a)),
            None => None,
        }
    }

    /// Feed every NEW document image a write statement produced: the first
    /// literal dotted field name ("a.b" as ONE key) sticks the table's
    /// `dotted_keys` marker (see TableMeta). Returns true when the marker
    /// was just set — callers fold that into their layout-sync condition so
    /// the marker reaches the catalog in the same transaction.
    pub(super) fn note_dotted_fields<'a>(
        &mut self,
        table: &str,
        mut docs: impl Iterator<Item = &'a Object>,
    ) -> bool {
        if self.tables.get(table).is_some_and(|m| m.dotted_keys) {
            return false;
        }
        if !docs.any(|d| d.keys().any(|k| k.contains('.'))) {
            return false;
        }
        if let Some(m) = self.catalog_mut(table) {
            m.dotted_keys = true;
            return true;
        }
        false
    }

    pub(crate) fn table_docs_cx(&self, table: &str) -> Result<Vec<Object>> {
        self.read_cx().table_docs(table)
    }
    pub(super) fn table_pairs_cx(&self, table: &str) -> Result<Vec<(u64, Object)>> {
        self.read_cx().table_pairs(table)
    }
    /// Write-path pre-pass with the outer scope of the target statement
    /// (UPDATE/DELETE target + FROM/USING tables): correlated subqueries on
    /// row-local surfaces (WHERE, SET) stay in place for per-row evaluation.
    pub(super) fn subst_expr_cx_scoped(&self, e: &mut SqlExpr, outer: &OuterFrom) -> Result<()> {
        self.read_cx().subst_expr_scoped(e, outer, true)
    }
    pub(super) fn index_probe_cx(
        &self,
        table: &str,
        alias: Option<&str>,
        selection: &Option<SqlExpr>,
        ctes: &Ctes,
    ) -> Result<Option<Vec<(u64, Object)>>> {
        self.read_cx().index_probe(table, alias, selection, ctes)
    }
    pub(super) fn matches_cx_in(
        &self,
        selection: &Option<SqlExpr>,
        doc: &Object,
        outer: &OuterFrom,
    ) -> Result<bool> {
        self.read_cx().matches_in(selection, doc, outer)
    }
    /// [`eval_assignments`] with correlated-subquery support: SET values
    /// still holding subquery nodes resolve their outer references per row.
    pub(super) fn eval_assignments_cx(
        &self,
        assignments: &[sqlparser::ast::Assignment],
        doc: &Object,
        outer: &OuterFrom,
    ) -> Result<Vec<(String, Value)>> {
        if !assignments.iter().any(|a| expr_has_subquery(&a.value)) {
            return eval_assignments(assignments, doc);
        }
        let mut subst = assignments.to_vec();
        for a in &mut subst {
            self.read_cx().subst_expr_row(&mut a.value, doc, outer)?;
        }
        eval_assignments(&subst, doc)
    }
    pub(super) fn load_from_cx(
        &self,
        from: &[sqlparser::ast::TableWithJoins],
        selection: &Option<SqlExpr>,
        ctes: &Ctes,
    ) -> Result<Vec<Object>> {
        self.read_cx().load_from(from, selection, ctes)
    }
    pub(super) fn exec_query_cx(&self, query: Query) -> Result<ExecOutcome> {
        self.read_cx().exec_query(query)
    }

    /// Execute exactly one SQL statement.
    pub fn execute(&mut self, sql: &str) -> Result<ExecOutcome> {
        match Self::parse_classified(sql) {
            Ok(parsed) => self.execute_parsed(parsed),
            Err(e) => Err(e),
        }
    }

    /// Execute an already-parsed statement — the parse-once path servers
    /// use after [`Database::parse_classified`] routed the request.
    pub fn execute_parsed(&mut self, parsed: ParsedStatement) -> Result<ExecOutcome> {
        self.resolved_sql = None;
        self.last_insert_id = None;
        // Wall-clock fold (journaled-write red line): a write carrying
        // NOW()/SYSDATE()/CURRENT_TIMESTAMP resolves the instant ONCE here
        // and ships the literal through resolved_sql — a replica replaying
        // the text would stamp its own clock and silently diverge. The
        // re-parsed statement carries the folded literal, so the INSERT
        // canonical rewrite below (if any) renders folded values too.
        //
        // Exception — statements that only DECLARE future behavior (plain
        // CREATE TABLE / ALTER): folding `DEFAULT NOW()` would freeze the
        // DDL-time instant into every future row. The call text stays; at
        // INSERT time exec_insert resolves the instant per row and rewrites
        // the journaled INSERT with explicit values (the auto-GUID
        // mechanism). CTAS still folds — its query is row data created now.
        if parsed.is_write && crate::stmt::mentions_wall_clock(&parsed.source) {
            let declares_defaults = match &parsed.stmt {
                AnyStmt::Sql(stmt) => match stmt.as_ref() {
                    Statement::CreateTable(create) => create.query.is_none(),
                    Statement::AlterTable(_) => true,
                    // A MATERIALIZED view stores the query text and replays
                    // it at REFRESH: the fold would freeze one instant into
                    // every future refresh, silently changing the semantics.
                    // The determinism gate in exec_create_matview refuses
                    // the raw call instead.
                    Statement::CreateView(v) if v.materialized => true,
                    _ => false,
                },
                _ => false,
            };
            if !declares_defaults {
                if let Some(folded) =
                    crate::stmt::fold_wall_clocks(&parsed.source, crate::now_ms() as i64)
                {
                    let reparsed = Self::parse_classified(&folded)?;
                    let outcome = self.execute_parsed(reparsed)?;
                    if self.resolved_sql.is_none() {
                        self.resolved_sql = Some(folded);
                    }
                    return Ok(outcome);
                }
            }
        }
        // T-SQL wall clocks are per-STATEMENT on the READ path too: the
        // scalar arm re-samples now_ms() per call site, so every row (and
        // two GETDATE()s in one projection) drifted across the scan. Fold
        // reads exactly like RAND below — one instant per statement.
        if !parsed.is_write && crate::stmt::mentions_wall_clock(&parsed.source) {
            if let Some(folded) =
                crate::stmt::fold_wall_clocks(&parsed.source, crate::now_ms() as i64)
            {
                let reparsed = Self::parse_classified(&folded)?;
                return self.execute_parsed(reparsed);
            }
        }
        // T-SQL RAND(): evaluated ONCE per statement — every row of the
        // result sees the same value. Reads (and CTAS, whose query is row
        // data created now) fold the calls into one literal before
        // executing; other writes keep the call in the text so exec_insert's
        // rewrite ships resolved values and UPDATE/DELETE/MERGE keep their
        // loud refusal below.
        let is_ctas = matches!(
            &parsed.stmt,
            AnyStmt::Sql(s) if matches!(
                s.as_ref(),
                Statement::CreateTable(c) if c.query.is_some()
            )
        );
        if (!parsed.is_write || is_ctas) && crate::stmt::mentions_nondet_call(&parsed.source) {
            let lit = crate::engine::value_literal(&crate::value::Value::Float(
                crate::tsql::rand_unit(),
            ))?;
            if let Some(folded) = crate::stmt::fold_rand_calls(&parsed.source, &lit) {
                let reparsed = Self::parse_classified(&folded)?;
                let outcome = self.execute_parsed(reparsed)?;
                if is_ctas && self.resolved_sql.is_none() {
                    self.resolved_sql = Some(folded);
                }
                return Ok(outcome);
            }
        }
        // NEWID()/NEWSEQUENTIALID() in writes: only INSERT has the canonical
        // per-row rewrite that ships generated ids as literals (exec_insert).
        // UPDATE/DELETE/MERGE journal their original text, so a random id
        // there would be re-rolled on every replaying peer — refuse loudly.
        // AST-level: a substring scan over the source would match the words
        // inside string literals ('x-newid-y') and unrelated identifiers
        // (a column named newid), rejecting perfectly legal writes.
        if parsed.is_write {
            if let AnyStmt::Sql(stmt) = &parsed.stmt {
                if stmt_calls_newid(stmt) {
                    return err(
                        "NEWID()/NEWSEQUENTIALID()/RAND() are only supported in SELECT and INSERT",
                    );
                }
            }
        }
        match parsed.stmt {
            AnyStmt::Sql(stmt) => self.exec_stmt(*stmt),
            AnyStmt::UserAdmin(ua) => self.exec_user_admin(&ua),
            AnyStmt::RefreshMatview(name) => self.exec_refresh_matview(&name),
        }
    }

    /// Parse-check a statement batch without executing anything (the web
    /// console's "parse" button).
    pub fn parse_check(sql: &str) -> std::result::Result<(), String> {
        match crate::useradmin::parse(sql) {
            Some(Ok(_)) => Ok(()),
            Some(Err(m)) => Err(m),
            None => match Parser::parse_sql(&GenericDialect {}, &crate::tsql::preprocess(sql)) {
                Ok(stmts) if !stmts.is_empty() => Ok(()),
                Ok(_) => Err("empty statement".into()),
                Err(e) => Err(e.to_string()),
            },
        }
    }

    /// Execute every statement in `sql`, in order, stopping at the first
    /// error. Successfully executed prefixes are still reported so callers
    /// (e.g. the web console) can render partial results. Splitting first
    /// lets a batch mix plain SQL with the hand-parsed user-management
    /// statements (join snapshots and backups embed them).
    pub fn execute_batch(&mut self, sql: &str) -> BatchResult {
        let parts = match crate::stmt::split_statements(sql) {
            Ok(p) => p,
            Err(m) => {
                return BatchResult {
                    statements: 0,
                    outcomes: vec![],
                    error: Some(BatchError {
                        statement: 0,
                        message: m,
                    }),
                }
            }
        };
        let statements = parts.len();
        let mut outcomes = Vec::new();
        for (i, part) in parts.into_iter().enumerate() {
            match self.execute(&part) {
                Ok(o) => outcomes.push(o),
                Err(e) => {
                    return BatchResult {
                        statements,
                        outcomes,
                        error: Some(BatchError {
                            statement: i,
                            message: e.to_string(),
                        }),
                    }
                }
            }
        }
        BatchResult {
            statements,
            outcomes,
            error: None,
        }
    }

    /// Take the canonical rewrite of the last statement: the auto-GUID
    /// INSERT rewrite or the canonical user-management form (password in
    /// stored-hash form). Replicating callers replace the original
    /// statement text with this one so peers apply the exact same values.
    pub fn take_resolved_sql(&mut self) -> Option<String> {
        self.resolved_sql.take()
    }

    /// Publish the canonical rewrite of the executing statement (used by
    /// the user-management family; auto-GUID INSERTs write the field
    /// directly inside `exec_insert`).
    pub(crate) fn set_resolved_sql(&mut self, sql: String) {
        self.resolved_sql = Some(sql);
    }

    /// AUTOINCREMENT id the last INSERT assigned (max over inserted rows,
    /// explicit values included; None when the last statement was not an
    /// inserting one or the table has no AUTOINCREMENT column). The
    /// statement-scoped readback avoids a second `SELECT MAX(id)` — a full
    /// heap scan — on every insert-and-learn-the-id caller.
    pub fn last_insert_id(&self) -> Option<i64> {
        self.last_insert_id
    }

    /// Current MAX(id) of a table's AUTOINCREMENT column via the counter
    /// cache: O(1) warm, one heap scan when an UPDATE/DELETE/restart
    /// invalidated it. None when the table is unknown or has no
    /// AUTOINCREMENT column — callers fall back to the SQL aggregate.
    pub fn max_autoinc(&mut self, table: &str) -> Option<i64> {
        let meta = self.tables.get(table)?.clone();
        meta.autoinc.as_ref()?;
        self.autoinc_next_for(table, &meta).ok()?;
        self.autoinc_cache.get(table).map(|next| next - 1)
    }

    /// PRIMARY KEY / UNIQUE constraint indexes as named entries,
    /// SQLite-style: `sqlite_autoindex_<table>_<n>`, PK column first.
    /// Derived from table-declared constraints only — columns that got a
    /// UNIQUE index through CREATE INDEX already have a named definition —
    /// and never written to the catalog, so already-deployed databases
    /// gain them on upgrade with no migration, and the names are identical
    /// on every peer (recomputed from the replicated DDL).
    pub(super) fn autoindex_defs(table: &str, meta: &TableMeta) -> Vec<IndexInfo> {
        let mut cols: Vec<&String> = Vec::new();
        for c in meta.primary_key.iter().chain(meta.constraint_unique.iter()) {
            if !cols.contains(&c) {
                cols.push(c);
            }
        }
        cols.into_iter()
            .enumerate()
            .map(|(i, c)| IndexInfo {
                name: format!("sqlite_autoindex_{table}_{}", i + 1),
                column: (*c).clone(),
                columns: vec![(*c).clone()],
                unique: true,
                auto: true,
                pred: None,
            })
            .collect()
    }

    /// Read-only catalog snapshot for tooling (object explorer, drivers).
    pub fn catalog(&self) -> Vec<TableInfo> {
        self.tables
            .iter()
            .map(|(name, meta)| {
                let auto = Self::autoindex_defs(name, meta);
                TableInfo {
                    name: name.clone(),
                    columns: meta
                        .columns
                        .iter()
                        .map(|c| ColumnInfo {
                            name: c.clone(),
                            nullable: !meta.not_null.contains(c),
                            primary_key: meta.primary_key.as_deref() == Some(c.as_str()),
                            unique: meta.unique.contains(c),
                            autoinc: meta.autoinc.as_deref() == Some(c.as_str())
                                || meta.autoguid.as_deref() == Some(c.as_str()),
                            data_type: if meta.autoguid.as_deref() == Some(c.as_str()) {
                                "GUID".to_string()
                            } else {
                                "ANY".to_string()
                            },
                            default_value: meta
                                .defaults
                                .iter()
                                .find(|(dc, _)| dc == c)
                                .map(|(_, t)| t.clone()),
                        })
                        .collect(),
                    keys: meta
                        .primary_key
                        .iter()
                        .chain(meta.unique.iter())
                        .cloned()
                        .collect(),
                    indexes: auto
                        .iter()
                        .map(|i| i.name.clone())
                        .chain(meta.indexes.iter().cloned())
                        .collect(),
                    index_defs: auto
                        .into_iter()
                        .chain(meta.index_defs.iter().map(|d| IndexInfo {
                            name: d.name.clone(),
                            column: d.column().to_string(),
                            columns: d.columns.clone(),
                            unique: d.unique,
                            auto: false,
                            pred: d.pred.clone(),
                        }))
                        .collect(),
                    pages: meta.pages.len(),
                    matview: meta.is_matview(),
                }
            })
            .collect()
    }

    /// Observed (data-derived) column names for one table: the union of
    /// top-level field names across its documents — the same projection
    /// `SELECT *` builds for a non-empty table. Declared columns
    /// (`catalog()`) are the constraint/introspection surface and never
    /// auto-sync with data; this is the data-side counterpart. It scans
    /// the table, so only deliberate introspection surfaces (object
    /// explorer) should call it. Unknown tables yield an empty list.
    pub fn observed_columns(&self, table: &str) -> Vec<String> {
        if !self.tables.contains_key(table) {
            return Vec::new();
        }
        self.table_docs_cx(table)
            .map(|d| union_of_fields(&d))
            .unwrap_or_default()
    }

    pub(super) fn exec_stmt_inner(&mut self, stmt: Statement) -> Result<ExecOutcome> {
        match stmt {
            Statement::CreateTable(create) => self.exec_create(create),
            Statement::CreateTrigger(ct) => self.exec_create_trigger(ct),
            Statement::DropTrigger(dt) => {
                if dt.option.is_some() {
                    return err("DROP TRIGGER CASCADE / RESTRICT is not supported");
                }
                let names: Vec<ObjectName> = std::iter::once(dt.trigger_name.clone()).collect();
                self.exec_drop_trigger(&names, dt.if_exists)
            }
            Statement::CreateView(view) => self.exec_create_view(view),
            Statement::Drop {
                object_type,
                names,
                if_exists,
                cascade,
                purge,
                ..
            } => {
                if purge {
                    // Hive's PURGE has no meaning for a database that always
                    // reclaims storage; accepting it would imply a recycle
                    // bin this engine does not have.
                    return err("DROP ... PURGE is not supported");
                }
                if !self.internal_ddl
                    && (object_type == sqlparser::ast::ObjectType::Table
                        || object_type == sqlparser::ast::ObjectType::View)
                {
                    for name in names.iter().map(obj_name) {
                        // Engine SYSTEM tables only: the user tables must
                        // stay droppable — the dump prelude drops them in
                        // every wholesale snapshot replay (the network path
                        // still refuses user-table DDL via
                        // stmt_write_targets).
                        if crate::engine::is_system_table(&name) {
                            return err(format!("cannot DROP the internal table {name}"));
                        }
                    }
                }
                if object_type == sqlparser::ast::ObjectType::Index {
                    // Composites roots removed below release their tree in
                    // the same catalog transaction; pages staged here never
                    // fight the readers (write lock held, snapshots use WAL).
                    //
                    // Validate the whole list first: the mutation loop below
                    // edits the in-memory catalog, so a mid-list failure must
                    // not strand uncommitted changes (a later save would
                    // persist them without a journal entry).
                    for n in &names {
                        let iname = obj_name(n);
                        // Constraint indexes are derived from the table's
                        // PRIMARY KEY/UNIQUE declarations (SQLite semantics).
                        if iname.starts_with("sqlite_autoindex_") {
                            return err("index associated with UNIQUE or PRIMARY KEY constraint \
                                 cannot be dropped");
                        }
                        let owner = self
                            .tables
                            .iter()
                            .any(|(_, m)| m.indexes.iter().any(|i| i == &iname));
                        if !owner && !if_exists {
                            return err(format!("index {iname} does not exist"));
                        }
                    }
                    // Same catalog snapshot as DROP TABLE/VIEW: a failure in
                    // the page-free loop or the catalog save must restore the
                    // in-memory catalog, or the next successful write would
                    // persist the (unjournalled) half-drop.
                    let catalog_prev = self.tables.clone();
                    let mut dropped_roots: Vec<(String, u32)> = Vec::new();
                    for n in &names {
                        let iname = obj_name(n);
                        // Index names are database-wide, so at most one table
                        // owns the index. Locate it read-only first, then
                        // write-clone just that entry (`catalog_mut` →
                        // `Arc::make_mut`) — mapping make_mut over every
                        // table would deep-clone the whole catalog whenever
                        // any detached reader holds an old version.
                        let owner = self
                            .tables
                            .iter()
                            .find(|(_, m)| m.indexes.iter().any(|i| i == &iname))
                            .map(|(t, _)| t.clone());
                        if let Some(owner) = owner {
                            let Some(meta) = self.catalog_mut(&owner) else {
                                return err(format!("table {owner} does not exist"));
                            };
                            if let Some(pos) = meta.indexes.iter().position(|i| i == &iname) {
                                meta.indexes.remove(pos);
                                // The B+ tree itself stays in index_roots:
                                // non-unique lookups still benefit from
                                // it — but only for single-column trees
                                // (keyed by the column itself). A
                                // composite tree is keyed by the index
                                // *name*; with the definition gone
                                // nothing can address it, and leaving it
                                // behind would let a column that shares
                                // the name look like a scalar tree.
                                if let Some(dpos) = meta
                                    .index_defs
                                    .iter()
                                    .position(|d| d.name == iname.as_str())
                                {
                                    let def = meta.index_defs.remove(dpos);
                                    // Trees keyed by the index NAME (composite
                                    // or JSON-path indexes) are unaddressable
                                    // once the definition is gone — and a
                                    // path tree left behind gets mistaken for
                                    // the column's own scalar tree, silently
                                    // hijacking probes and ORDER BY walks.
                                    if def.columns.len() > 1
                                        || def.path.is_some()
                                        || def.pred.is_some()
                                    {
                                        if let Some(root) = meta.index_roots.remove(&iname) {
                                            dropped_roots.push((iname.clone(), root));
                                        }
                                    }
                                    // Lift UNIQUE only when this index was
                                    // the sole source: table-declared
                                    // constraints and other unique indexes
                                    // on the same column keep it enforced.
                                    // Composite unique indexes never join
                                    // meta.unique, so single-column lift
                                    // rules cover everything here. The
                                    // "other source" test must name the
                                    // column AS ITS SINGLE key: a composite
                                    // unique index containing the column
                                    // never guaranteed single-column
                                    // uniqueness, and counting it kept the
                                    // dropped constraint enforced forever
                                    // (schema hash divergence included).
                                    if def.unique
                                        && def.columns.len() == 1
                                        && !meta.constraint_unique.contains(&def.columns[0])
                                        && meta.primary_key.as_deref()
                                            != Some(def.columns[0].as_str())
                                        && !meta.index_defs.iter().any(|d| {
                                            d.unique
                                                && d.columns.len() == 1
                                                && d.columns[0] == def.columns[0]
                                        })
                                    {
                                        meta.unique.retain(|c| c != &def.columns[0]);
                                    }
                                }
                            }
                        }
                    }
                    let mut tx = self.pager.begin_tx();
                    let drop_result: Result<()> = (|| {
                        for (root_key, root) in &dropped_roots {
                            for p in BTree::open(*root)
                                .collect_pages(&PageReader::current(&self.pager), &tx)
                                .map_err(|e| index_err(root_key, e))?
                            {
                                self.pager.free_page(&mut tx, p)?;
                            }
                        }
                        self.save_catalog_into(&mut tx)
                    })();
                    match drop_result {
                        Ok(()) => {
                            self.commit_pager_tx(tx)?;
                            return Ok(ExecOutcome::Affected(0));
                        }
                        Err(e) => {
                            self.pager.abort_tx(tx)?;
                            self.tables = catalog_prev;
                            return Err(e);
                        }
                    }
                }
                if object_type == sqlparser::ast::ObjectType::View {
                    // Views have no storage: drop the catalog entries only.
                    // DROP TABLE on a view errors (wrong-kind mismatch), and
                    // so does DROP VIEW on a table. Validate the whole list
                    // before touching the catalog — a mid-list failure must
                    // not leave earlier names already dropped and committed.
                    let mut dropping: Vec<String> = names.iter().map(obj_name).collect();
                    for name in &dropping {
                        match self.tables.get(name) {
                            None if if_exists => {}
                            None => return err(format!("view {name} does not exist")),
                            Some(meta) if !meta.is_view() => {
                                return err(format!(
                                    "{name} is a table, not a view (use DROP TABLE)"
                                ));
                            }
                            Some(_) => {}
                        }
                    }
                    // A dependent view left behind used to fail only at the
                    // next SELECT; refuse without CASCADE, join with it.
                    self.expand_view_dependents(&mut dropping, cascade)?;
                    // Same snapshot discipline as DROP TABLE: a failed
                    // save_catalog_into must not leave the views gone from
                    // the in-memory catalog (the next successful write would
                    // persist a half-drop that never freed anything).
                    let catalog_prev = self.tables.clone();
                    let grants_deleted = self.delete_grants_for(&dropping)?;
                    let mut tx = self.pager.begin_tx();
                    for name in &dropping {
                        self.tables.remove(name);
                    }
                    match self.save_catalog_into(&mut tx) {
                        Ok(()) => {
                            self.commit_pager_tx(tx)?;
                            return Ok(ExecOutcome::Affected(0));
                        }
                        Err(e) => {
                            self.pager.abort_tx(tx)?;
                            self.tables = catalog_prev;
                            self.restore_grant_rows(&grants_deleted);
                            return Err(e);
                        }
                    }
                }
                if object_type != sqlparser::ast::ObjectType::Table
                    && object_type != sqlparser::ast::ObjectType::MaterializedView
                {
                    return err("only DROP TABLE/VIEW/INDEX are supported");
                }
                // Mixed-kind target: DROP TABLE on a view must not silently
                // drop the rest.
                for name in names.iter().map(obj_name) {
                    if self.tables.get(&name).is_some_and(|m| m.is_view()) {
                        return err(format!("{name} is a view, not a table (use DROP VIEW)"));
                    }
                }
                // Validate every target before touching the catalog: a missing
                // name mid-list must not leave earlier drops applied in memory
                // only, and dropping a table other tables still reference
                // leaves dangling FKs (their inserts fail and the join
                // snapshot can never replay).
                let mut dropping: Vec<String> = names.iter().map(obj_name).collect();
                // Dependent views: refuse without CASCADE (the dangling view
                // used to fail only at SELECT time), join the drop set with
                // it. Views are catalog-only, so they never hit the FK or
                // storage paths below. Missing targets (IF EXISTS) cannot
                // block a drop — their legacy dangling views join a CASCADE
                // instead.
                if cascade {
                    self.expand_view_dependents(&mut dropping, true)?;
                } else {
                    let mut existing: Vec<String> = dropping
                        .iter()
                        .filter(|n| self.tables.contains_key(*n))
                        .cloned()
                        .collect();
                    self.expand_view_dependents(&mut existing, false)?;
                }
                let view_dropping: Vec<String> = dropping
                    .iter()
                    .filter(|n| self.tables.get(*n).is_some_and(|m| m.is_view()))
                    .cloned()
                    .collect();
                let table_dropping: Vec<String> = dropping
                    .iter()
                    .filter(|n| !self.tables.get(*n).is_some_and(|m| m.is_view()))
                    .cloned()
                    .collect();
                let drop_set: std::collections::BTreeSet<&str> =
                    table_dropping.iter().map(|s| s.as_str()).collect();
                let mut fk_removals: Vec<String> = Vec::new();
                for name in &table_dropping {
                    if !self.tables.contains_key(name) {
                        if if_exists {
                            continue;
                        }
                        return err(format!("table {name} does not exist"));
                    }
                    let referencing: Vec<String> = self
                        .tables
                        .iter()
                        .filter(|(t, m)| {
                            t.as_str() != name.as_str()
                                && !drop_set.contains(t.as_str())
                                && m.foreign_keys.iter().any(|(_, rt, _)| rt == name)
                        })
                        .map(|(t, _)| t.clone())
                        .collect();
                    if !referencing.is_empty() {
                        if !cascade {
                            return err(format!(
                                "cannot drop table {name}: referenced by FOREIGN KEY in {}",
                                referencing.join(", ")
                            ));
                        }
                        fk_removals.extend(referencing);
                    }
                }
                fk_removals.sort();
                fk_removals.dedup();
                // Snapshot the catalog: any failure between here and the
                // commit must leave the in-memory catalog untouched (the
                // pager tx undoes the pages; without this the removed
                // entries stayed gone while nothing was journaled, and the
                // next successful write would persist a half-drop whose
                // pages were never freed).
                let catalog_prev = self.tables.clone();
                let grants_deleted = self.delete_grants_for(&dropping)?;
                let mut tx = self.pager.begin_tx();
                // CASCADE drops the referencing FK declarations along with
                // the table (PostgreSQL semantics): the child table stays,
                // its constraint on the vanished parent goes.
                let drop_result: Result<()> = (|| {
                    for child in &fk_removals {
                        if let Some(meta) = self.catalog_mut(child) {
                            meta.foreign_keys
                                .retain(|(_, rt, _)| !drop_set.contains(rt.as_str()));
                        }
                    }
                    for name in &table_dropping {
                        if let Some(meta) = self.tables.get(name) {
                            self.free_table_storage(&mut tx, meta)?;
                        }
                        self.tables.remove(name);
                        // A recreated table must start its AUTOINCREMENT counter
                        // from its own rows (max+1), not inherit the dropped
                        // table's watermark — replicas that restarted compute a
                        // different value and the nodes diverge.
                        self.autoinc_cache.remove(name);
                    }
                    for name in &view_dropping {
                        self.tables.remove(name);
                    }
                    // A dropped table's triggers die with it (SQLite
                    // semantics) — a surviving trigger would dangle on the
                    // table name and misfire when a new table takes it.
                    self.triggers
                        .retain(|_, d| !drop_set.contains(d.table.as_str()));
                    self.save_catalog_into(&mut tx)?;
                    Ok(())
                })();
                match drop_result {
                    Ok(()) => {
                        self.commit_pager_tx(tx)?;
                        Ok(ExecOutcome::Affected(0))
                    }
                    Err(e) => {
                        self.pager.abort_tx(tx)?;
                        self.tables = catalog_prev;
                        self.restore_grant_rows(&grants_deleted);
                        Err(e)
                    }
                }
            }
            Statement::Insert(insert) => self.exec_insert(insert),
            Statement::Update(update) => {
                if update.output.is_some() {
                    return err("OUTPUT is not supported (use RETURNING)");
                }
                if update.or.is_some() {
                    return err("UPDATE OR ... is not supported");
                }
                if !update.order_by.is_empty() || update.limit.is_some() {
                    return err("UPDATE ... ORDER BY/LIMIT is not supported");
                }
                let sqlparser::ast::Update {
                    table,
                    assignments,
                    from,
                    selection,
                    returning,
                    ..
                } = update;
                self.exec_update(table, assignments, from, selection, returning)
            }
            Statement::Delete(delete) => {
                if delete.output.is_some() {
                    return err("OUTPUT is not supported (use RETURNING)");
                }
                if !delete.order_by.is_empty() || delete.limit.is_some() {
                    return err("DELETE ... ORDER BY/LIMIT is not supported");
                }
                if !delete.tables.is_empty() {
                    return err(
                        "DELETE target lists are not supported (use DELETE FROM ... USING ...)",
                    );
                }
                let sqlparser::ast::Delete {
                    from,
                    using,
                    selection,
                    returning,
                    ..
                } = delete;
                self.exec_delete(from, using, selection, returning)
            }
            Statement::AlterTable(alter) => self.exec_alter(alter),
            Statement::StartTransaction { .. } => {
                if self.tx_snapshot.is_some() {
                    return err("transaction already in progress");
                }
                self.pager.begin_undo();
                self.tx_snapshot = Some(self.catalog_snapshot());
                Ok(ExecOutcome::Affected(0))
            }
            Statement::Commit { .. } => {
                if self.tx_snapshot.is_none() {
                    return err("no transaction in progress");
                }
                // Durability FIRST, teardown after: consuming the undo
                // journal and catalog snapshot before the fsync used to
                // leave a failed COMMIT unrecoverable — the transaction's
                // deferred frames sat unfenced with no undo left, and the
                // NEXT unrelated commit's fence would silently persist the
                // "failed" transaction while the client retried it. With
                // this order a failed fsync keeps the transaction open
                // (rollback and retry both still possible); the teardown
                // only runs once the boundary is durable (async-commit
                // mode defers that to the background flusher).
                if !self.async_commit {
                    self.pager.sync_wal().map_err(SqlError::from)?;
                    self.pending_sync = false;
                }
                let _ = self.tx_snapshot.take();
                // The page undo journal dies with the transaction.
                self.pager.end_undo();
                // Savepoints die with their transaction: a stale mark must
                // not be reachable from a later transaction's ROLLBACK TO.
                self.savepoints.clear();
                Ok(ExecOutcome::Affected(0))
            }
            Statement::Rollback {
                savepoint: Some(name),
                ..
            } => self.rollback_to_savepoint(&name.value),
            Statement::Rollback {
                savepoint: None, ..
            } => self.rollback_tx(),
            Statement::Savepoint { name } => self.savepoint(&name.value),
            Statement::ReleaseSavepoint { name } => self.release_savepoint(&name.value),
            Statement::CreateIndex(idx) => self.exec_create_index(idx),
            Statement::Merge(m) => {
                if m.output.is_some() {
                    return err("MERGE OUTPUT is not supported (use RETURNING on the statements)");
                }
                self.exec_merge(m.clone())
            }
            Statement::Query(q) => self.exec_query_cx(*q),
            Statement::Explain {
                statement,
                analyze,
                estimate,
                format,
                options,
                ..
            } => self
                .read_cx()
                .explain_outer(&statement, analyze, estimate, &format, &options),
            Statement::Truncate(tr) => {
                // Empty the tables; shape (columns/constraints) is kept.
                // Validate every target (existence, kind, FK children)
                // BEFORE truncating: a mid-list failure must not leave
                // earlier tables already emptied and committed while the
                // failed statement never reaches the journal — replicas
                // would keep rows the origin lost.
                if tr.partitions.is_some() || tr.on_cluster.is_some() {
                    return err("TRUNCATE partitions/ON CLUSTER are not supported");
                }
                if tr.identity == Some(sqlparser::ast::TruncateIdentityOption::Continue) {
                    // The AUTOINCREMENT watermark is derived from the live
                    // rows, so truncation necessarily restarts it; honoring
                    // CONTINUE IDENTITY would require a sequence store the
                    // engine does not have.
                    return err("TRUNCATE ... CONTINUE IDENTITY is not supported");
                }
                let mut targets: Vec<String> =
                    tr.table_names.iter().map(|t| obj_name(&t.name)).collect();
                for t in &tr.table_names {
                    if t.only {
                        return err("TRUNCATE ... ONLY is not supported (no inheritance)");
                    }
                }
                // CASCADE used to be accepted and ignored (the FK guard then
                // rejected the truncate whenever child rows existed). Honor
                // it: referencing tables join the target list transitively,
                // PostgreSQL-style; RESTRICT/no clause keeps the old guard.
                if tr.cascade == Some(sqlparser::ast::CascadeOption::Cascade) {
                    loop {
                        let referencing: Vec<String> = self
                            .tables
                            .iter()
                            .filter(|(t, m)| {
                                !targets.contains(t)
                                    && m.foreign_keys.iter().any(|(_, rt, _)| targets.contains(rt))
                            })
                            .map(|(t, _)| t.clone())
                            .collect();
                        if referencing.is_empty() {
                            break;
                        }
                        targets.extend(referencing);
                    }
                }
                for name in &targets {
                    if !self.tables.contains_key(name) && !tr.if_exists {
                        return err(format!("table {name} does not exist"));
                    }
                    if self.tables.get(name).is_some_and(|m| m.is_view()) {
                        return err(format!(
                            "cannot TRUNCATE view {name} (it is a view, not a table)"
                        ));
                    }
                    if self.tables.get(name).is_some_and(|m| m.is_matview()) {
                        return err(format!(
                            "cannot TRUNCATE materialized view {name} \
                             (REFRESH MATERIALIZED VIEW {name} recomputes it)"
                        ));
                    }
                    if self.tables.contains_key(name) {
                        // TRUNCATE is a full-table DELETE: child rows must
                        // not be orphaned silently (same guard as DELETE).
                        // Co-truncated tables are excluded — the whole list
                        // is validated before any table is emptied, so
                        // `TRUNCATE parent, child` must not fail on the
                        // parent's rows the child still references.
                        let removed = self.table_docs_cx(name)?;
                        self.check_fk_parent_delete_skipping(name, &removed, &[], &targets, None)?;
                    }
                }
                // One pager transaction for the WHOLE batch: each table's
                // rewrite used to commit its own transaction, so a mid-list
                // failure left earlier tables emptied AND committed while
                // the failed statement never reached the journal — replicas
                // kept rows the origin lost. Any failure now aborts the
                // batch and restores the in-memory catalog snapshot.
                let touched: Vec<String> = targets
                    .iter()
                    .filter(|n| self.tables.contains_key(*n))
                    .cloned()
                    .collect();
                let catalog_snapshot: Vec<(String, Option<std::sync::Arc<TableMeta>>)> = touched
                    .iter()
                    .map(|n| (n.clone(), self.tables.get(n).cloned()))
                    .collect();
                let mut tx = self.pager.begin_tx();
                let batch = (|| -> Result<()> {
                    for name in &touched {
                        let mut meta = self
                            .tables
                            .get(name)
                            .map(|a| a.as_ref().clone())
                            .unwrap_or_default();
                        self.rewrite_table_tx(
                            &mut tx,
                            name,
                            &mut meta,
                            Vec::new(),
                            true,
                            Vec::new(),
                        )?;
                    }
                    Ok(())
                })();
                match batch {
                    Ok(()) => {
                        self.commit_pager_tx(tx)?;
                    }
                    Err(e) => {
                        for (name, old) in catalog_snapshot {
                            match old {
                                Some(m) => {
                                    self.tables.insert(name, m);
                                }
                                None => {
                                    self.tables.remove(&name);
                                }
                            }
                        }
                        self.pager.abort_tx(tx)?;
                        return Err(e);
                    }
                }
                Ok(ExecOutcome::Affected(0))
            }
            Statement::Pragma { .. } => {
                // Compatibility shim: accept and ignore PRAGMA statements
                // (SQLite-dialect callers, e.g. EF Core startup probes).
                Ok(ExecOutcome::Affected(0))
            }
            other => err(format!("unsupported statement: {other}")),
        }
    }

    /// Statement guard for DML aimed at a trigger-carrying table: the
    /// statement and every trigger it fires commit or vanish together.
    /// Zero cost when no trigger targets the statement's table (the guard
    /// never opens) — trigger-less deployments keep their exact write path.
    pub(super) fn exec_stmt(&mut self, stmt: Statement) -> Result<ExecOutcome> {
        if self.trigger_depth == 0 {
            if let Some(t) = trigger_dml_target(&stmt) {
                if self.triggers_exist_for(&t) {
                    return self.run_with_trigger_guard(move |slf| slf.exec_stmt_inner(stmt));
                }
            }
        }
        self.exec_stmt_inner(stmt)
    }

    pub(super) fn run_with_trigger_guard(
        &mut self,
        run: impl FnOnce(&mut Self) -> Result<ExecOutcome>,
    ) -> Result<ExecOutcome> {
        if self.tx_snapshot.is_some() {
            // Inside an explicit transaction the statement-local revert is a
            // savepoint (page-undo mark + catalog snapshot): the transaction
            // and its earlier statements stay.
            let mark = self.pager.undo_mark();
            self.pager.undo_savepoint()?;
            let snap = self.catalog_snapshot();
            let r = run(self);
            if r.is_err() {
                let mut undo = self.pager.take_undo_to(mark);
                // sync_catalog=false — the journal replay already restored
                // the catalog pages (same cheap path as ROLLBACK TO).
                self.restore_transaction(snap, &mut undo, false)?;
            }
            return r;
        }
        // Autocommit: statement + triggers fuse into one write unit (one
        // WAL fsync, crash loses all of them together), and any failure
        // reverts pages (undo journal) and catalog (snapshot) exactly like
        // a transaction ROLLBACK — a half-fired statement never survives.
        self.pager.begin_undo();
        let snap = self.catalog_snapshot();
        let mut unit = self.write_unit();
        let r = run(&mut unit);
        match r {
            Ok(out) => {
                unit.end()?;
                self.pager.end_undo();
                Ok(out)
            }
            Err(e) => {
                // Mirror ROLLBACK: void the deferred frames BEFORE writing
                // the revert, or the revert's fence would commit the failed
                // work on a torn crash.
                let db: &mut Database = &mut unit;
                db.pager.abort_deferred()?;
                let mut undo = db.pager.take_undo();
                db.restore_transaction(snap, &mut undo, true)?;
                unit.end()?;
                Err(e)
            }
        }
    }
}
