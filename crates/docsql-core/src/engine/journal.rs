use super::*;

impl Database {
    /// Replication fingerprints for every user table, sorted by name;
    /// `_pubsub_messages` is skipped (node-local queue state, like
    /// [Database::dump_script]). `rows_hash` is a wrapping sum of
    /// per-row hashes over the documents' canonical encoded bytes, so it
    /// is independent of heap layout: nodes holding the same logical
    /// data agree regardless of which node applied a row first. The
    /// cluster rejoin repair compares these to decide whether a node
    /// that was offline missed writes.
    pub fn digests(&mut self) -> Result<Vec<TableDigest>> {
        let mut names: Vec<String> = self
            .tables
            .keys()
            .filter(|n| !is_system_table(n))
            .cloned()
            .collect();
        names.sort();
        let mut out = Vec::with_capacity(names.len());
        for name in &names {
            let meta = self.tables.get(name).cloned().unwrap();
            let docs = self.table_docs_cx(name)?;
            let mut rows_hash: u64 = 0;
            for doc in &docs {
                let mut h = DefaultHasher::new();
                for (k, v) in doc {
                    k.hash(&mut h);
                    let mut buf = Vec::new();
                    encode::encode(v, &mut buf)?;
                    h.write(&buf);
                }
                rows_hash = rows_hash.wrapping_add(h.finish());
            }
            out.push(TableDigest {
                name: name.clone(),
                rows: docs.len() as u64,
                rows_hash,
                schema_hash: schema_hash(&meta),
            });
        }
        Ok(out)
    }

    /// MAX/MIN(seq) over the journal (0 when empty or negative somehow).
    pub(super) fn journal_seq_boundary(&mut self, agg: &str) -> Result<u64> {
        self.ensure_cluster_tables()?;
        let r = self.execute(&format!("SELECT {agg}(seq) FROM {CLUSTER_LOG_TABLE}"))?;
        Ok(match r {
            ExecOutcome::Rows(rows) => rows
                .rows
                .first()
                .and_then(|row| row.first())
                .and_then(Value::as_i64)
                .unwrap_or(0)
                .max(0) as u64,
            _ => 0,
        })
    }

    /// Append one locally-committed write to the journal and return its
    /// seq. Call immediately after the write commits, before fanning out:
    /// the entry then exists for every peer that ever pulls catch-up,
    /// whatever happens to the fan-out itself. The insert is its own
    /// statement (a crash between the data commit and this append loses
    /// the entry — the repair's digest re-check catches the resulting
    /// divergence and falls back to a snapshot).
    pub fn journal_append(&mut self, sql: &str) -> Result<u64> {
        self.ensure_cluster_tables()?;
        let next = match self.journal_next {
            Some(n) => n,
            None => self.journal_seq_boundary("MAX")? + 1,
        };
        // ts = entry commit time (ms): the PITR replay watermark. One clock
        // read per journaled write is nothing next to the write itself.
        let ts = crate::now_ms() as i64;
        self.execute(&format!(
            "INSERT INTO {CLUSTER_LOG_TABLE} (seq, sql, ts) VALUES ({next}, {}, {ts})",
            value_literal(&Value::Str(sql.to_string()))?
        ))?;
        self.journal_next = Some(next + 1);
        if let Some(bytes) = self.journal_bytes.as_mut() {
            *bytes += sql.len() as u64 + 64;
        }
        Ok(next)
    }

    /// Highest journal seq (0 when empty).
    pub fn journal_head(&mut self) -> Result<u64> {
        self.journal_seq_boundary("MAX")
    }

    /// Lowest journal seq still retained (0 when empty). A position below
    /// this minus one means the needed range was trimmed away and the
    /// peer must fall back to a full snapshot.
    pub fn journal_oldest(&mut self) -> Result<u64> {
        self.journal_seq_boundary("MIN")
    }

    /// Journal entries with seq > `after`, in order, capped at `limit`
    /// entries per call. Callers loop, advancing `after` to the last
    /// returned seq, until the result is empty.
    pub fn journal_range(&mut self, after: u64, limit: usize) -> Result<Vec<(u64, String)>> {
        self.ensure_cluster_tables()?;
        let r = self.execute(&format!(
            "SELECT seq, sql FROM {CLUSTER_LOG_TABLE} WHERE seq > {after} \
             ORDER BY seq ASC LIMIT {limit}"
        ))?;
        let mut out = Vec::new();
        let mut bytes = 0usize;
        if let ExecOutcome::Rows(rows) = r {
            for row in &rows.rows {
                let (Some(Value::Int(seq)), Some(Value::Str(sql))) = (row.first(), row.get(1))
                else {
                    return err("journal row has unexpected shape");
                };
                bytes = bytes.saturating_add(sql.len());
                if !out.is_empty() && bytes > JOURNAL_RANGE_MAX_BYTES {
                    // Byte budget for the catch-up batch: rows can carry
                    // ~16MiB documents, so a 512-row page could otherwise
                    // materialize gigabytes in one Vec on the serving node.
                    break;
                }
                out.push((*seq as u64, sql.clone()));
            }
        }
        Ok(out)
    }

    /// Drop journal entries beyond the newest `keep` — the catch-up
    /// window. Peers positioned older than the window's start fall back
    /// to a full snapshot. `keep == 0` keeps everything.
    pub fn journal_trim(&mut self, keep: u64) -> Result<()> {
        self.ensure_cluster_tables()?;
        if keep == 0 {
            return Ok(());
        }
        let head = self.journal_head()?;
        if head <= keep {
            return Ok(());
        }
        self.journal_bytes = None; // rows deleted; recompute lazily
        self.execute(&format!(
            "DELETE FROM {CLUSTER_LOG_TABLE} WHERE seq <= {}",
            head - keep
        ))
        .map(|_| ())
    }

    /// Byte-dimension journal cap alongside the count window: drop oldest
    /// entries until the journal's SQL text fits `max_bytes`. The count
    /// window alone bounds nothing for big-document workloads (a single
    /// journal entry can reach 16 MiB, so 100k entries ≈ 100 GB), and the
    /// PITR-recommended `DOCSQL_CATCHUP_WINDOW=0` removes the count cap
    /// entirely. Cheap when under the cap: the running total is cached in
    /// memory (one SUM scan after open, then incremented per append).
    pub fn journal_trim_bytes(&mut self, max_bytes: u64) -> Result<()> {
        self.ensure_cluster_tables()?;
        let total = match self.journal_bytes {
            Some(b) => b,
            None => {
                let r =
                    self.execute(&format!("SELECT SUM(LENGTH(sql)) FROM {CLUSTER_LOG_TABLE}"))?;
                let b = match r {
                    ExecOutcome::Rows(rows) => rows
                        .rows
                        .first()
                        .and_then(|row| row.first())
                        .and_then(Value::as_i64)
                        .unwrap_or(0)
                        .max(0) as u64,
                    _ => 0,
                };
                self.journal_bytes = Some(b);
                b
            }
        };
        if total <= max_bytes {
            return Ok(());
        }
        // Delete oldest seqs until the removed bytes cover the excess. One
        // ordered scan (rows are already seq-ordered in the heap) plus one
        // ranged DELETE — both amortized against the many appends that let
        // the total drift past the cap.
        let r = self.execute(&format!(
            "SELECT seq, LENGTH(sql) FROM {CLUSTER_LOG_TABLE} ORDER BY seq ASC"
        ))?;
        let excess = total - max_bytes;
        let mut acc = 0u64;
        let mut cutoff = 0u64;
        if let ExecOutcome::Rows(rows) = r {
            for row in &rows.rows {
                let (Some(Value::Int(seq)), Some(Value::Int(n))) = (row.first(), row.get(1)) else {
                    continue;
                };
                acc += (*n).max(0) as u64;
                cutoff = *seq as u64;
                if acc >= excess {
                    break;
                }
            }
        }
        if cutoff > 0 {
            self.journal_bytes = None; // recompute exactly after the delete
            self.execute(&format!(
                "DELETE FROM {CLUSTER_LOG_TABLE} WHERE seq <= {cutoff}"
            ))?;
        }
        Ok(())
    }

    /// Void every journal entry's text, keeping the seqs allocated. Used
    /// when a snapshot adoption overrules this node's own recent writes:
    /// the adjudicated entries must never replay again (a later repair on
    /// some peer would pull them back and resurrect discarded writes), but
    /// the seq space and every recorded position must keep their meaning.
    /// Replays of a voided entry are a parsed no-op.
    /// Journal entries with `seq > after`, in seq order, capped at `limit`:
    /// the PITR incremental export source. `ts` is the entry's commit time
    /// (None for rows migrated from the pre-PITR journal shape).
    pub fn journal_entries_after(
        &mut self,
        after: u64,
        limit: usize,
    ) -> Result<Vec<(u64, Option<i64>, String)>> {
        self.ensure_cluster_tables()?;
        let r = self.execute(&format!(
            "SELECT seq, ts, sql FROM {CLUSTER_LOG_TABLE} WHERE seq > {after} \
             ORDER BY seq LIMIT {limit}"
        ))?;
        let mut out = Vec::new();
        if let ExecOutcome::Rows(rows) = r {
            for row in rows.rows {
                let seq = row.first().and_then(Value::as_i64).unwrap_or(0).max(0) as u64;
                let ts = row.get(1).and_then(Value::as_i64);
                if let Some(Value::Str(sql)) = row.get(2) {
                    out.push((seq, ts, sql.clone()));
                }
            }
        }
        Ok(out)
    }

    pub fn journal_void_all(&mut self) -> Result<()> {
        self.ensure_cluster_tables()?;
        self.journal_bytes = None; // text shrinks to the sentinel; recompute lazily
        self.execute(&format!(
            "UPDATE {CLUSTER_LOG_TABLE} SET sql = 'PRAGMA discarded_by_snapshot_adoption;'"
        ))
        .map(|_| ())
    }

    /// Last applied journal seq for the origin `node_id`.
    pub fn position_get(&mut self, node_id: &str) -> Result<Option<u64>> {
        self.ensure_cluster_tables()?;
        let r = self.execute(&format!(
            "SELECT seq FROM {CLUSTER_POS_TABLE} WHERE node_id = {}",
            value_literal(&Value::Str(node_id.to_string()))?
        ))?;
        Ok(match r {
            ExecOutcome::Rows(rows) => rows
                .rows
                .first()
                .and_then(|row| row.first())
                .and_then(Value::as_i64)
                .map(|s| s as u64),
            _ => None,
        })
    }

    /// Record that everything up to `seq` from origin `node_id` is
    /// applied here. Called after a replicated write commits (advancing)
    /// and after an adoption samples the origin's head (establishing).
    pub fn position_set(&mut self, node_id: &str, seq: u64) -> Result<()> {
        self.ensure_cluster_tables()?;
        let key = value_literal(&Value::Str(node_id.to_string()))?;
        let updated = match self.execute(&format!(
            "UPDATE {CLUSTER_POS_TABLE} SET seq = {seq} WHERE node_id = {key}"
        ))? {
            ExecOutcome::Affected(n) => n,
            ExecOutcome::Rows(_) => 0,
        };
        if updated == 0 {
            self.execute(&format!(
                "INSERT INTO {CLUSTER_POS_TABLE} (node_id, seq) VALUES ({key}, {seq})"
            ))?;
        }
        Ok(())
    }

    /// Forget all positions: used when an adoption replaces the state in
    /// a way the sampled heads cannot account for.
    pub fn positions_clear(&mut self) -> Result<()> {
        self.ensure_cluster_tables()?;
        self.execute(&format!("DELETE FROM {CLUSTER_POS_TABLE}"))
            .map(|_| ())
    }

    /// Full logical dump as a SQL script: all DDL first, then all data.
    /// System storage (`_pubsub_messages`) is skipped — its ids are
    /// node-local and it is not user data. The join protocol replays this
    /// inside one transaction on a fresh node to bootstrap the cluster's
    /// current state; every CREATE precedes every INSERT, so FOREIGN KEY
    /// declarations are safe in the DDL phase (referential checks run at
    /// insert time). Applying the script replaces tables wholesale
    /// (DROP + CREATE prefix), so re-applying is idempotent.
    ///
    /// All user tables are dropped by ONE statement: the DROP guard
    /// exempts tables dropped by the same statement, so the drop cannot
    /// be blocked by FK references — per-table drops interleaved with
    /// CREATEs would deadlock against the restored schema (the new child
    /// referencing a parent whose old copy is still to be dropped), and
    /// backup restore replays this script over LIVE data, not a fresh
    /// node.
    pub fn dump_script(&mut self) -> Result<String> {
        let names: Vec<String> = self
            .tables
            .keys()
            .filter(|n| !is_internal_table(n))
            .cloned()
            .collect();
        // Views drop with their own statement kind: DROP TABLE on a view is
        // a wrong-kind error (restore replays the prelude verbatim).
        let view_names: Vec<String> = names
            .iter()
            .filter(|n| self.tables.get(*n).is_some_and(|m| m.is_view()))
            .cloned()
            .collect();
        // Materialized views are third kind: real storage but dumped as
        // CREATE MATERIALIZED VIEW (the replay re-runs the stored query —
        // deterministic, validated at CREATE — so the restored content is
        // a fresh recompute over the restored base data, never the
        // possibly-stale origin rows).
        let matview_names: Vec<String> = names
            .iter()
            .filter(|n| self.tables.get(*n).is_some_and(|m| m.is_matview()))
            .cloned()
            .collect();
        let table_names: Vec<String> = names
            .iter()
            .filter(|n| {
                !self
                    .tables
                    .get(*n)
                    .is_some_and(|m| m.is_view() || m.is_matview())
            })
            .cloned()
            .collect();
        // CREATE order must respect FK dependencies: the engine (rightly)
        // rejects a CREATE whose referenced table does not exist yet, so
        // alphabetical DDL order fails the replay whenever the child sorts
        // before its parent. Drops go the opposite way (children first).
        let ordered_tables = fk_dependency_order(&table_names, &self.tables);
        let drop_order: Vec<String> = {
            let mut v = ordered_tables.clone();
            v.reverse();
            v
        };
        let mut ddl = String::new();
        if !view_names.is_empty() {
            let drops = view_names
                .iter()
                .map(|n| quote_ident(n))
                .collect::<Vec<_>>()
                .join(", ");
            ddl.push_str(&format!("DROP VIEW IF EXISTS {drops};\n"));
        }
        if !table_names.is_empty() {
            let drops = drop_order
                .iter()
                .map(|n| quote_ident(n))
                .collect::<Vec<_>>()
                .join(", ");
            ddl.push_str(&format!("DROP TABLE IF EXISTS {drops};\n"));
        }
        for name in &ordered_tables {
            let meta = self.tables.get(name).cloned().unwrap();
            ddl.push_str(&create_table_ddl(name, &meta));
            ddl.push('\n');
            for d in &meta.index_defs {
                // Path indexes (design 005) re-emit as the JSON_EXTRACT
                // expression they were created from — replay parses it back
                // through the same CREATE INDEX path.
                let col_text = match &d.path {
                    Some(p) => format!("JSON_EXTRACT({}, '{}')", quote_ident(&d.columns[0]), p),
                    None => d
                        .columns
                        .iter()
                        .map(|c| quote_ident(c))
                        .collect::<Vec<_>>()
                        .join(", "),
                };
                ddl.push_str(&format!(
                    "CREATE {}INDEX {} ON {} ({}){};\n",
                    if d.unique { "UNIQUE " } else { "" },
                    quote_ident(&d.name),
                    quote_ident(name),
                    col_text,
                    d.pred
                        .as_deref()
                        .map(|p| format!(" WHERE {p}"))
                        .unwrap_or_default()
                ));
            }
        }
        // Insert order must respect FK dependencies: referenced tables
        // first. Catalog iteration is alphabetical, so without this a
        // child table's rows could replay before its parent's and fail
        // the per-row FK check.
        let mut dml = String::new();
        for name in fk_dependency_order(&table_names, &self.tables) {
            let docs = self.table_docs_cx(&name)?;
            // 自引用表按行级依赖序出:存储序的前向引用曾让回放必败。
            let docs = match self.tables.get(&name) {
                Some(meta) => self_ref_row_order(&name, meta, docs),
                None => docs,
            };
            for doc in docs {
                let cols: Vec<String> = doc.keys().cloned().collect();
                let vals = cols
                    .iter()
                    .map(|c| value_literal(&doc[c]))
                    .collect::<Result<Vec<_>>>()?;
                dml.push_str(&format!(
                    "INSERT INTO {} ({}) VALUES ({});\n",
                    quote_ident(&name),
                    cols.iter()
                        .map(|c| quote_ident(c))
                        .collect::<Vec<_>>()
                        .join(", "),
                    vals.join(", ")
                ));
            }
        }
        // Append in place instead of `ddl + &dml`: the dump is taken under
        // the engine lock, so a third full-size copy would double the peak
        // memory for nothing.
        ddl.push_str(&dml);
        // Views: their bodies reference restored tables (the CREATE
        // dry-runs the body, so the table must exist by then), and a view
        // of a view needs its base view restored first — dependency order,
        // not catalog (alphabetical) order.
        for name in &view_dependency_order(&view_names, &self.tables) {
            if let Some(meta) = self.tables.get(name) {
                if let Some(body) = &meta.view_sql {
                    ddl.push_str(&format!("CREATE VIEW {} AS {body};\n", quote_ident(name)));
                }
            }
        }
        // Triggers come AFTER the data (pg_dump semantics): the dumped rows
        // already include every effect their firing ever produced, and
        // replaying the INSERTs with live triggers would fire them again —
        // duplicated audit rows, doubled cascades. Created post-data, the
        // restore reproduces the snapshot byte-for-byte and the triggers arm
        // only for statements that run after it.
        for d in self.triggers.values() {
            ddl.push_str(&create_trigger_sql(d));
            ddl.push_str(";\n");
        }
        // Materialized views after views (a matview query may read a view;
        // the CREATE re-runs the query over the restored data — REFRESH
        // semantics — so the possibly-stale stored rows are deliberately
        // NOT dumped) and before users.
        for name in &matview_names {
            let meta = self.tables.get(name).cloned().unwrap();
            let q = meta.matview_sql.as_deref().expect("matview has a query");
            ddl.push_str(&format!(
                "CREATE MATERIALIZED VIEW {} AS {q};\n",
                quote_ident(name)
            ));
            for d in &meta.index_defs {
                let col_text = d
                    .columns
                    .iter()
                    .map(|c| quote_ident(c))
                    .collect::<Vec<_>>()
                    .join(", ");
                ddl.push_str(&format!(
                    "CREATE {}INDEX {} ON {} ({}){};\n",
                    if d.unique { "UNIQUE " } else { "" },
                    quote_ident(&d.name),
                    quote_ident(name),
                    col_text,
                    d.pred
                        .as_deref()
                        .map(|p| format!(" WHERE {p}"))
                        .unwrap_or_default()
                ));
            }
        }
        // Users/roles last of all: `GRANT … ON <object>` validates that the
        // target exists, and a grant on a VIEW must replay after that view
        // is created (emitting them before the view block made every dump
        // from a mesh with view grants unrestorable). Passwords ride in
        // stored-hash form; plain INSERTs against the reserved tables are
        // rejected by the write gate, so the statement family is the one
        // replication form everywhere. The DROP prelude above does not
        // cover the reserved tables — restoring a backup with fewer users
        // must still remove the extras — so drop them here; the replaying
        // node re-creates them via ensure_user_tables before the first
        // CREATE USER applies.
        ddl.push_str(&format!(
            "DROP TABLE IF EXISTS {}, {}, {}, {};\n",
            crate::useradmin::USERS_TABLE,
            crate::useradmin::ROLES_TABLE,
            crate::useradmin::MEMBERS_TABLE,
            crate::useradmin::GRANTS_TABLE
        ));
        for s in crate::useradmin::dump_user_statements(self)? {
            ddl.push_str(&s);
            ddl.push_str(";\n");
        }
        Ok(ddl)
    }

    /// Page size of the underlying storage file.
    pub fn page_size(&self) -> usize {
        PAGE_SIZE
    }

    /// Number of allocated pages.
    pub fn num_pages(&self) -> u32 {
        self.pager.num_pages()
    }

    /// Last WAL LSN known durable; nodes that received the same writes should
    /// report converging values.
    pub fn durable_lsn(&self) -> u64 {
        self.pager.durable_lsn()
    }
}
