use super::*;

impl Database {
    /// MERGE INTO target USING source ON (cond) — Oracle/SQL-standard
    /// upsert over arbitrary match conditions (ON CONFLICT only covers
    /// unique-key conflicts). Execution: load the source rows, evaluate ON
    /// against every (target, source) pair, then apply per pair —
    /// WHEN MATCHED THEN UPDATE SET …, WHEN NOT MATCHED THEN INSERT ….
    /// Determinism: the merge walks target storage order, so replication
    /// replays converge. Two source rows matching one target row is an
    /// error (Oracle ORA-30926 semantics).
    pub(super) fn exec_merge(&mut self, merge: sqlparser::ast::Merge) -> Result<ExecOutcome> {
        use sqlparser::ast::{MergeAction, MergeClauseKind};

        // Target: a simple table.
        let sqlparser::ast::TableFactor::Table {
            name: tname_ident,
            alias: talias,
            ..
        } = &merge.table
        else {
            return err("MERGE target must be a simple table");
        };
        let tname = obj_name(tname_ident);
        let tkey = talias
            .as_ref()
            .map(|a| a.name.value.clone())
            .unwrap_or_else(|| tname.clone());
        let Some(meta) = self.tables.get(&tname).cloned() else {
            return err(format!("table {tname} does not exist"));
        };
        // Views have no storage: every other write shape rejects them here
        // (exec_insert/update/delete, TRUNCATE, ALTER, CREATE INDEX);
        // without this a MERGE inserts into the view's private empty
        // heap — invisible rows and a dump that cannot replay.
        ensure_not_view("MERGE into", &tname, &meta)?;

        // Exactly one WHEN MATCHED (UPDATE) and one WHEN NOT MATCHED
        // (INSERT) clause; per-clause predicates, BY SOURCE, DELETE WHERE
        // and UPDATE WHERE are rejected loudly (v1 surface).
        let mut matched_upd: Option<Vec<sqlparser::ast::Assignment>> = None;
        let mut not_matched_ins: Option<(Vec<String>, Vec<SqlExpr>)> = None;
        for cl in &merge.clauses {
            if cl.predicate.is_some() {
                return err("MERGE WHEN … AND <predicate> is not supported");
            }
            match cl.clause_kind {
                MergeClauseKind::NotMatchedBySource => {
                    return err("WHEN … BY SOURCE is not supported");
                }
                MergeClauseKind::Matched | MergeClauseKind::NotMatchedByTarget => {
                    if matched_upd.is_some() {
                        return err("duplicate WHEN MATCHED clause");
                    }
                    let MergeAction::Update(upd) = &cl.action else {
                        return err("WHEN MATCHED supports only UPDATE \
                             (DELETE/other actions are not supported)");
                    };
                    if upd.update_predicate.is_some() || upd.delete_predicate.is_some() {
                        return err("MERGE UPDATE WHERE/DELETE WHERE is not supported");
                    }
                    matched_upd = Some(upd.assignments.clone());
                }
                MergeClauseKind::NotMatched => {
                    if not_matched_ins.is_some() {
                        return err("duplicate WHEN NOT MATCHED clause");
                    }
                    let MergeAction::Insert(ins) = &cl.action else {
                        return err("WHEN NOT MATCHED THEN UPDATE is not supported");
                    };
                    if ins.insert_predicate.is_some() {
                        return err("MERGE INSERT predicate is not supported");
                    }
                    let sqlparser::ast::MergeInsertKind::Values(values) = &ins.kind else {
                        return err("MERGE INSERT ROW is not supported");
                    };
                    if values.rows.len() != 1 {
                        return err("MERGE INSERT VALUES must have exactly one row");
                    }
                    let mut cols = Vec::new();
                    for c in &ins.columns {
                        let col = obj_name(c);
                        if !meta.columns.contains(&col) {
                            return err(format!("column {col} does not exist"));
                        }
                        if cols.contains(&col) {
                            return err(format!("duplicate column name: {col}"));
                        }
                        cols.push(col);
                    }
                    if cols.is_empty() {
                        // No column list: the VALUES map onto the table's
                        // declared column order (the INSERT rule). Zipping
                        // against the empty list used to silently drop the
                        // values and append an all-NULL row.
                        if values.rows[0].content.len() != meta.columns.len() {
                            return err(format!(
                                "MERGE INSERT has {} values but the table has {} \
                                 columns; use an explicit column list",
                                values.rows[0].content.len(),
                                meta.columns.len()
                            ));
                        }
                        cols = meta.columns.clone();
                    }
                    let mut exprs = Vec::new();
                    for row in &values.rows {
                        for e in row.content.iter() {
                            exprs.push(e.clone());
                        }
                    }
                    not_matched_ins = Some((cols, exprs));
                }
            }
        }
        // Source rows: plain table or derived subquery (no CTE access in
        // v1 — the MERGE statement is not a SELECT statement).
        let (_, src_alias, src_rows) = self
            .read_cx()
            .load_table_factor(&merge.source, &Ctes::new())?;
        // Source-column qualifier: the alias for a plain table (falling back
        // to its name), the derived-table alias when given, else `source`.
        // Losing the derived alias here would make `feed.col` resolve to the
        // target's bare column via fallback and every source row would match
        // the same target row (spurious "multiple source rows matched").
        let skey = match &merge.source {
            sqlparser::ast::TableFactor::Table { name, alias, .. } => alias
                .as_ref()
                .map(|a| a.name.value.clone())
                .unwrap_or_else(|| obj_name(name)),
            _ => src_alias
                .filter(|a| !a.is_empty())
                .unwrap_or_else(|| "source".to_string()),
        };
        // Same alias for source and target: the join row keys collide and
        // the TARGET qualifier overwrites the source's, so `ON x.id =
        // x.id` is trivially true and SET reads the target's own pre-image
        // — every target row matches every source row (mass wrong updates
        // or a spurious "cannot update the same row twice"). MySQL and SQL
        // Server reject the shape; so do we.
        if skey == tkey {
            return err(format!(
                "MERGE source and target aliases must differ (both are '{skey}')"
            ));
        }

        // Target rows with locators (storage order — deterministic replay
        // on replication peers).
        let tdocs = self.table_pairs_cx(&tname)?;

        // ROWNUM has no row stream to number in MERGE's ON / arms: it reads
        // NULL and `NULL <= n` is TRUE under the documented total order, so
        // every pair would silently match. Fail loudly (subquery sources may
        // still use ROWNUM — that stream is their own).
        if refs_rownum(&merge.on)
            || matched_upd
                .as_ref()
                .is_some_and(|asg| asg.iter().any(|a| refs_rownum(&a.value)))
            || not_matched_ins
                .as_ref()
                .is_some_and(|(_, exprs)| exprs.iter().any(refs_rownum))
        {
            return err("ROWNUM is not supported in MERGE ON / WHEN arms");
        }

        // Pair every source row with its matching target row: ON evaluates
        // on the merged namespace (target columns first; source columns are
        // additionally available qualified as `skey.col`).
        let mut pairs: Vec<(u64, Object, usize)> = Vec::new(); // (t_loc, t_doc, s_idx)
        let mut s_consumed = vec![false; src_rows.len()];
        for (t_loc, t_doc) in &tdocs {
            let mut hits = 0;
            for (si, s_doc) in src_rows.iter().enumerate() {
                // O(target×source) pairing must stay interruptible under
                // the statement timeout like every other row loop.
                self.stmt_deadline.check()?;
                let row = Self::merge_join_row(t_doc, tkey.as_str(), s_doc, &skey);
                if matches!(eval_expr(&merge.on, &row)?, Value::Bool(true)) {
                    hits += 1;
                    if hits > 1 && matched_upd.is_some() {
                        // Only an UPDATE arm makes a second match a conflict
                        // (it would update the row twice). With no WHEN
                        // MATCHED clause every source row that matched is
                        // simply consumed — nothing to update.
                        return err(format!(
                            "MERGE cannot update the same row of {tname} twice \
                             (multiple source rows matched)"
                        ));
                    }
                    pairs.push((*t_loc, t_doc.clone(), si));
                    s_consumed[si] = true;
                }
            }
        }

        // Build the matched updates: assignments evaluate on the merged
        // namespace (target columns and qualified source columns).
        let mut updates: Vec<(u64, Object, Object)> = Vec::new();
        // An explicit autoinc id (INSERT arm) or an assignment touching the
        // autoinc column (UPDATE arm) invalidates the session's high-water
        // cache: `merge_next_autoinc` only tracks engine-generated ids, and
        // keeping the stale cache made the next plain INSERT collide with
        // the explicit value (UNIQUE constraint failed).
        let mut autoinc_mutated = false;
        if let Some(assignments) = &matched_upd {
            for (t_loc, t_doc, si) in &pairs {
                let s_doc = &src_rows[*si];
                let row = Self::merge_join_row(t_doc, tkey.as_str(), s_doc, &skey);
                let mut new_doc = t_doc.clone();
                for a in assignments {
                    let col = match &a.target {
                        sqlparser::ast::AssignmentTarget::ColumnName(c) => obj_name(c),
                        other => {
                            return err(format!("unsupported MERGE assignment target: {other}"))
                        }
                    };
                    if Some(&col) == meta.autoinc.as_ref() {
                        autoinc_mutated = true;
                    }
                    let v = eval_expr(&a.value, &row)?;
                    new_doc.insert(col, v);
                }
                // Same constraint gate as UPDATE: NOT NULL/CHECK and the
                // child-side FK must hold for the merged image (FK check
                // runs after the batch is collected — self/intra-statement
                // references count as parent candidates).
                meta.check(&new_doc)?;
                updates.push((*t_loc, t_doc.clone(), new_doc));
            }
            // NOTE: both FK directions are validated against the statement's
            // FINAL state after the INSERT arm below — checking here (against
            // pre-statement data) let a MERGE land a dangling self-reference
            // (UPDATE removes the parent the INSERT arm still points at) and
            // wrongly reject parents the other arm had just created.
        }

        // One pager transaction for heap pages and index trees alike.
        let mut heap = Heap {
            pages: meta.pages.clone(),
            overflow_free: meta.overflow_free.clone(),
            dropped: Vec::new(),
        };
        let mut roots = meta.index_roots.clone();
        let idx_specs = idx_specs(&meta, &roots);
        let mut tx = self.pager.begin_tx();

        // Pass 1: drop every matched row's old index entries before any
        // replacement lands (a multi-row key shift must not collide with a
        // not-yet-replaced row's still-present old key).
        for (loc, old_doc, _) in &updates {
            if let Err(e) =
                reindex_remove(&self.pager, &mut tx, &idx_specs, &mut roots, old_doc, *loc)
            {
                self.pager.abort_tx(tx)?;
                return Err(e);
            }
        }
        // Pass 2: replace the matched rows, following in-page repacks.
        for i in 0..updates.len() {
            let (loc, new_doc) = (updates[i].0, updates[i].2.clone());
            let page = crate::heap::unpack_loc(loc).0;
            let before = heap.page_docs(&PageReader::current(&self.pager), &tx, page)?;
            // Pre-fixup pending locators; see the identical logic in the
            // UPDATE fast path.
            let pending_locs: Vec<u64> = updates.iter().skip(i + 1).map(|(l, _, _)| *l).collect();
            let out = heap.replace(&self.pager, &mut tx, loc, &new_doc)?;
            if !out.moved.is_empty() {
                for pending in updates.iter_mut().skip(i + 1) {
                    if let Some((_, new_l)) = out.moved.iter().find(|(old, _)| *old == pending.0) {
                        pending.0 = *new_l;
                    }
                }
            }
            for (old_l, new_l) in &out.moved {
                // Same rule as the UPDATE fast path: pending rows were
                // stripped in pass 1 and get their new entries in their own
                // iteration; already-processed rows must follow the repack.
                if *old_l == loc || pending_locs.contains(old_l) {
                    continue;
                }
                if let Err(e) = reindex_repoint(
                    &self.pager,
                    &mut tx,
                    &idx_specs,
                    &mut roots,
                    &before,
                    *old_l,
                    *new_l,
                ) {
                    self.pager.abort_tx(tx)?;
                    return Err(e);
                }
            }
            if let Err(e) = reindex_insert(
                &self.pager,
                &mut tx,
                &idx_specs,
                &mut roots,
                &new_doc,
                out.placed,
            ) {
                self.pager.abort_tx(tx)?;
                return Err(e);
            }
        }
        // WHEN NOT MATCHED THEN INSERT: appended rows go through the same
        // tree inserts (unique trees reject duplicates) and FK checks as
        // plain inserts. Only source rows that matched no target row are
        // inserted.
        let mut inserted = 0usize;
        let mut inserted_docs: Vec<Object> = Vec::new();
        // 本语句插入行的最大自增 id 与下一个自增值:提交成功后才写进会话
        // 状态(与 INSERT 路径对齐),失败回滚不留幻影 SCOPE_IDENTITY。
        let mut merge_last_id: Option<i64> = None;
        let mut merge_next_autoinc = if meta.autoinc.is_some() {
            self.autoinc_next_for(&tname, &meta)?
        } else {
            0
        };
        if let Some((cols, exprs)) = &not_matched_ins {
            // Auto-generated GUIDs are random: letting each node generate its
            // own would silently diverge replicas (the INSERT path rewrites
            // the statement with the values; a MERGE rewrite would have to
            // rewrite the whole branching statement).
            if meta.autoguid.is_some() {
                return err(
                    "MERGE ... WHEN NOT MATCHED THEN INSERT is not supported on \
                     tables with an auto-generated GUID column",
                );
            }
            let mut next_autoinc = merge_next_autoinc;
            let mut default_exprs: std::collections::HashMap<String, SqlExpr> =
                std::collections::HashMap::new();
            for (si, s_doc) in src_rows.iter().enumerate() {
                if s_consumed[si] {
                    continue;
                }
                self.stmt_deadline.check()?;
                let mut doc = Object::new();
                for (c, e) in cols.iter().zip(exprs.iter()) {
                    let v = eval_expr(e, s_doc)?;
                    doc.insert(c.clone(), v);
                }
                // Same generation rules as INSERT: missing/NULL AUTOINCREMENT
                // gets max+1, declared DEFAULTs fill omitted columns.
                if let Some(col) = &meta.autoinc {
                    match doc.get(col) {
                        Some(Value::Null) | None => {
                            doc.insert(col.clone(), Value::Int(next_autoinc));
                            next_autoinc = next_autoinc.saturating_add(1);
                        }
                        // Explicit id: advance the in-statement watermark so a
                        // later source row that omits the id doesn't reuse it
                        // (same rule as INSERT); the engine didn't allocate
                        // it, so the cache is still dropped at commit and the
                        // next statement recomputes max+1.
                        Some(Value::Int(v)) => {
                            next_autoinc = next_autoinc.max(v.saturating_add(1));
                            autoinc_mutated = true;
                        }
                        _ => {
                            autoinc_mutated = true;
                        }
                    }
                }
                for (col, text) in &meta.defaults {
                    if !doc.contains_key(col) {
                        let e = match default_exprs.get(col) {
                            Some(e) => e,
                            None => {
                                let e = parse_expr_text(text)?;
                                default_exprs.insert(col.clone(), e);
                                default_exprs.get(col).expect("just inserted")
                            }
                        };
                        if expr_calls_wall_clock(e) {
                            // MERGE has no canonical resolved-text rewrite
                            // (unlike INSERT); replaying the statement on a
                            // peer would stamp a different clock. Fail loudly
                            // instead of silently diverging.
                            return err(
                                "MERGE INSERT cannot apply a wall-clock or random DEFAULT \
                                 (NOW/SYSDATE/CURRENT_TIMESTAMP/GETDATE/RAND); \
                                 supply the column explicitly",
                            );
                        }
                        doc.insert(col.clone(), eval_const(e)?);
                    }
                }
                meta.check(&doc)?;
                // FK child-side validation moved to the unified post-statement
                // check below (final state, both arms included).
                inserted_docs.push(doc.clone());
                let loc = heap.insert(&self.pager, &mut tx, &doc)?;
                reindex_insert(&self.pager, &mut tx, &idx_specs, &mut roots, &doc, loc)?;
                if let Some(col) = &meta.autoinc {
                    if let Some(Value::Int(i)) = doc.get(col) {
                        // 只累计,提交成功后才落到 last_insert_id/autoinc_cache:
                        // 循环中途失败回滚后,SCOPE_IDENTITY 不得读到幻影 id。
                        let cur = merge_last_id.unwrap_or(i64::MIN);
                        merge_last_id = Some(cur.max(*i));
                    }
                }
                inserted += 1;
            }
            merge_next_autoinc = next_autoinc;
        }
        // Legacy files whose constraint columns predate trees keep the
        // whole-set duplicate check (same gate as the INSERT path): with no
        // constraint tree, the reindex inserts above cannot reject
        // duplicates, and a MERGE could silently write a repeated business
        // key. Everything so far is staged in `tx` — the abort paths below
        // discard it (all-or-nothing).
        let has_constraints = meta.primary_key.is_some() || !meta.unique.is_empty();
        // ALL constraint columns must be treed to skip the whole-set check —
        // `any` here would let a mixed state (one treed, one treeless
        // constraint) skip the check and silently admit duplicates on the
        // treeless one (same all-or-nothing gate as the INSERT path).
        let constraint_treed = meta
            .primary_key
            .iter()
            .chain(meta.unique.iter())
            .all(|c| roots.contains_key(c));
        if has_constraints && !constraint_treed && (updates.len() + inserted) > 0 {
            let mut combined: Vec<Object> = match (Heap {
                pages: meta.pages.clone(),
                overflow_free: meta.overflow_free.clone(),
                dropped: Vec::new(),
            }
            .scan(&PageReader::current(&self.pager)))
            {
                Ok(docs) => docs,
                Err(e) => {
                    self.pager.abort_tx(tx)?;
                    return Err(e.into());
                }
            };
            // Swap the updated rows' pre-images for their post-images
            // (unambiguous: a pre-image's PK value is unique in committed
            // data), then add the not-matched inserts.
            for (_, old, _) in &updates {
                combined.retain(|d| d != old);
            }
            combined.extend(updates.iter().map(|(_, _, n)| n.clone()));
            combined.extend(inserted_docs.iter().cloned());
            if let Err(e) = meta.check_unique(&combined) {
                self.pager.abort_tx(tx)?;
                return Err(e);
            }
        }
        // FK validation against the statement's FINAL state: pre-state −
        // updated pre-images + updated post-images + inserted rows. Both
        // directions used to be checked per-arm against pre-statement data,
        // which let a MERGE land a dangling self-reference (UPDATE removes
        // the parent the INSERT arm points at) or wrongly reject a parent
        // the other arm had just created.
        if !updates.is_empty() || !inserted_docs.is_empty() {
            let final_docs: Vec<Object> = {
                let mut docs = match self.table_docs_cx(&tname) {
                    Ok(d) => d,
                    Err(e) => {
                        self.pager.abort_tx(tx)?;
                        return Err(e);
                    }
                };
                for (_, old, _) in &updates {
                    docs.retain(|d| d != old);
                }
                docs.extend(updates.iter().map(|(_, _, n)| n.clone()));
                docs.extend(inserted_docs.iter().cloned());
                docs
            };
            // Child side: every FK value in a new/inserted image must exist
            // in the final state. For self-references the candidate set is
            // EXACTLY the final images — `check_fks_in`'s same_batch is only
            // an ADDITIONAL parent source, so the pre-state heap would still
            // satisfy values this very statement removes.
            for fk in &meta.foreign_keys {
                let (col, rtable, rcol) = (&fk.col, &fk.ref_table, &fk.ref_col);
                for doc in updates
                    .iter()
                    .map(|(_, _, n)| n)
                    .chain(inserted_docs.iter())
                {
                    self.stmt_deadline.check()?;
                    let Some(v) = doc.get(col) else { continue };
                    if matches!(v, Value::Null) {
                        continue; // NULL passes (MATCH SIMPLE semantics)
                    }
                    let matches_parent = |rd: &Object| {
                        rd.get(rcol)
                            .map(|rv| Value::cmp_values(rv, v) == Ordering::Equal)
                            .unwrap_or(false)
                    };
                    let hit = if rtable == &tname {
                        final_docs.iter().any(matches_parent)
                    } else {
                        match self.table_docs_cx(rtable) {
                            Ok(d) => d.iter().any(matches_parent),
                            Err(e) => {
                                self.pager.abort_tx(tx)?;
                                return Err(e);
                            }
                        }
                    };
                    if !hit {
                        self.pager.abort_tx(tx)?;
                        return err(format!(
                            "FOREIGN KEY constraint failed: {tname}.{col} references \
                             {rtable}.{rcol}"
                        ));
                    }
                }
            }
            // Parent side: removed values must not stay referenced — the
            // self-reference child scan runs over the final images.
            let removed: Vec<Object> = updates.iter().map(|(_, o, _)| o.clone()).collect();
            if !removed.is_empty() {
                if let Err(e) = self.check_fk_parent_delete_skipping(
                    &tname,
                    &removed,
                    &final_docs,
                    &[],
                    Some(&final_docs),
                ) {
                    self.pager.abort_tx(tx)?;
                    return Err(e);
                }
            }
        }
        // Emptied heap pages are released inside this transaction so later
        // statements reuse them instead of growing the file.
        for p in std::mem::take(&mut heap.dropped) {
            self.pager.free_page(&mut tx, p)?;
        }
        let dotted_now = self.note_dotted_fields(
            &tname,
            inserted_docs
                .iter()
                .chain(updates.iter().map(|(_, _, new)| new)),
        );
        if dotted_now
            || heap.pages != meta.pages
            || roots != meta.index_roots
            || heap.overflow_free != meta.overflow_free
        {
            self.sync_table_layout(
                &mut tx,
                &tname,
                heap.pages.clone(),
                roots,
                heap.overflow_free.clone(),
            )?;
        }
        self.commit_pager_tx(tx)?;
        if let Some(id) = merge_last_id {
            let cur = self.last_insert_id.unwrap_or(i64::MIN);
            self.last_insert_id = Some(cur.max(id));
        }
        if autoinc_mutated {
            // Same recovery rule as the plain UPDATE path: an id this
            // statement set explicitly (or reassigned) is invisible to
            // `merge_next_autoinc`, so only a recompute stays correct.
            self.autoinc_cache.remove(&tname);
        } else if merge_last_id.is_some() {
            self.autoinc_cache.insert(tname.clone(), merge_next_autoinc);
        }
        Ok(ExecOutcome::Affected((updates.len() + inserted) as u64))
    }

    /// Merge-namespaced evaluation row: target columns (unqualified) plus
    /// source columns (qualified `skey.col`, and unqualified when the name
    /// is not taken by the target). Qualified target refs resolve to the
    /// target's own columns only — a column the target row lacks pins to
    /// NULL instead of falling back to the source's bare-name value.
    pub(super) fn merge_join_row(
        t_doc: &Object,
        t_key: &str,
        s_doc: &Object,
        s_key: &str,
    ) -> Object {
        let mut row = t_doc.clone();
        for (k, v) in s_doc {
            row.insert(format!("{s_key}.{k}"), v.clone());
            if !row.contains_key(k.as_str()) {
                row.insert(k.clone(), v.clone());
            }
        }
        for k in t_doc.keys() {
            row.insert(format!("{t_key}.{k}"), t_doc[k].clone());
        }
        for k in s_doc.keys() {
            row.entry(format!("{t_key}.{k}")).or_insert(Value::Null);
        }
        row
    }
}
