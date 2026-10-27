use super::*;

impl Database {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn exec_update(
        &mut self,
        table: sqlparser::ast::TableWithJoins,
        mut assignments: Vec<sqlparser::ast::Assignment>,
        from: Option<sqlparser::ast::UpdateTableFromKind>,
        mut selection: Option<SqlExpr>,
        update_returning: Option<Vec<SelectItem>>,
    ) -> Result<ExecOutcome> {
        let sqlparser::ast::TableFactor::Table { name, alias, .. } = table.relation else {
            return err("only simple table names in UPDATE");
        };
        // MySQL-style `UPDATE t JOIN u ON … SET …` parses with the join in
        // `table.joins`; ignoring it would update every row of `t` with
        // values evaluated without `u`. Reject loudly instead.
        if !table.joins.is_empty() {
            return err("UPDATE ... JOIN is not supported (use UPDATE ... FROM)");
        }
        let tname = obj_name(&name);
        if self.tables.get(&tname).is_some_and(|m| m.is_view()) {
            return err(format!(
                "cannot UPDATE view {tname} (it is a view, not a table)"
            ));
        }
        if self.tables.get(&tname).is_some_and(|m| m.is_matview()) {
            return err(format!(
                "cannot UPDATE materialized view {tname} \
                 (REFRESH MATERIALIZED VIEW {tname} recomputes it)"
            ));
        }
        let tkey = alias
            .as_ref()
            .map(|a| a.name.value.clone())
            .unwrap_or_else(|| tname.clone());
        // ROWNUM has no row stream to number in an UPDATE's own SET/WHERE:
        // it reads NULL, and `NULL <= n` is TRUE under the documented total
        // order — an Oracle-style `UPDATE ... WHERE ROWNUM <= 1` would
        // silently rewrite every row. Fail loudly instead.
        if assignments.iter().any(|a| refs_rownum(&a.value))
            || selection.as_ref().is_some_and(refs_rownum)
        {
            return err("ROWNUM is not supported in UPDATE (use a subquery: WHERE id IN (SELECT ... WHERE ROWNUM <= n))");
        }
        // Outer scope for correlated subqueries: the target (name + alias)
        // plus the FROM tables (the FROM form evaluates WHERE/SET over the
        // merged image).
        let mut outer = OuterFrom::new();
        outer.insert(tname.clone());
        outer.insert(tkey.clone());
        if let Some(kind) = &from {
            match kind {
                sqlparser::ast::UpdateTableFromKind::BeforeSet(twjs)
                | sqlparser::ast::UpdateTableFromKind::AfterSet(twjs) => {
                    from_list_names(twjs, &mut outer);
                }
            }
        }
        for a in &mut assignments {
            self.subst_expr_cx_scoped(&mut a.value, &outer)?;
        }
        if let Some(sel) = &mut selection {
            self.subst_expr_cx_scoped(sel, &outer)?;
        }
        let mut meta = self
            .tables
            .get(&tname)
            .map(|a| a.as_ref().clone())
            .unwrap_or_default();

        // Fast path: probe-able WHERE on an indexed column — update the
        // affected rows in place instead of rewriting the whole table.
        if from.is_none() {
            if let Some(matches) =
                self.index_probe_cx(&tname, Some(tkey.as_str()), &selection, &Ctes::new())?
            {
                return self.exec_update_fast(
                    tname,
                    meta,
                    matches,
                    &assignments,
                    &selection,
                    &update_returning,
                    &outer,
                );
            }
        }

        let docs = self.table_docs_cx(&tname)?;
        let (out, changed_docs, old_changed, count) = match from {
            None => {
                // Plain UPDATE: assignments and WHERE see only target columns.
                let mut out = Vec::new();
                let mut changed_docs: Vec<Object> = Vec::new();
                let mut old_changed: Vec<Object> = Vec::new();
                let mut count = 0u64;
                for doc in docs {
                    if self.matches_cx_in(&selection, &doc, &outer)? {
                        old_changed.push(doc.clone());
                        let new_vals = self.eval_assignments_cx(&assignments, &doc, &outer)?;
                        let mut doc = doc;
                        for (col_name, v) in new_vals {
                            doc.insert(col_name, v);
                        }
                        count += 1;
                        changed_docs.push(doc.clone());
                        out.push(doc);
                    } else {
                        out.push(doc);
                    }
                }
                (out, changed_docs, old_changed, count)
            }
            Some(kind) => {
                // UPDATE ... FROM: each target row is matched against the
                // FROM tables' joined rows; the first qualifying match
                // supplies the values.
                let extra = match kind {
                    sqlparser::ast::UpdateTableFromKind::BeforeSet(v)
                    | sqlparser::ast::UpdateTableFromKind::AfterSet(v) => v,
                };
                let mut from_list = vec![sqlparser::ast::TableWithJoins {
                    relation: sqlparser::ast::TableFactor::Table {
                        name: name.clone(),
                        alias: alias.clone(),
                        args: None,
                        with_hints: vec![],
                        version: None,
                        with_ordinality: false,
                        partitions: vec![],
                        json_path: None,
                        sample: None,
                        index_hints: vec![],
                    },
                    joins: vec![],
                }];
                from_list.extend(extra);
                let merged = self.load_from_cx(&from_list, &None, &Ctes::new())?;
                let prefix = format!("{tkey}.");
                // First qualifying match per target content: every output
                // row with that content receives that image — including
                // byte-identical duplicate rows, which are distinct rows
                // despite sharing a key.
                let mut updates: std::collections::HashMap<Vec<u8>, Object> =
                    std::collections::HashMap::new();
                let mut changed_docs: Vec<Object> = Vec::new();
                let mut old_changed: Vec<Object> = Vec::new();
                for m in &merged {
                    let tdoc = target_doc_from_merged(m, &prefix);
                    let key = object_key(&tdoc);
                    if updates.contains_key(&key) {
                        continue; // first qualifying match wins
                    }
                    if !self.matches_cx_in(&selection, m, &outer)? {
                        continue;
                    }
                    old_changed.push(tdoc.clone());
                    let new_vals = self.eval_assignments_cx(&assignments, m, &outer)?;
                    let mut tdoc = tdoc;
                    for (col_name, v) in new_vals {
                        tdoc.insert(col_name, v);
                    }
                    changed_docs.push(tdoc.clone());
                    updates.insert(key, tdoc);
                }
                let mut count = 0u64;
                let out: Vec<Object> = docs
                    .into_iter()
                    .map(|doc| match updates.get(&object_key(&doc)) {
                        Some(img) => {
                            count += 1;
                            img.clone()
                        }
                        None => doc,
                    })
                    .collect();
                (out, changed_docs, old_changed, count)
            }
        };
        // Validate BEFORE writing: a failed UPDATE must not change data.
        // 自引用 FK 的父候选是本语句的最终像集合(不只是表里的旧像)。
        for doc in &out {
            // Full-table UPDATEs walk this loop (plus a parent-table scan
            // per FK inside check_fks_in) — keep the timeout armed.
            self.stmt_deadline.check()?;
            meta.check(doc)?;
            self.check_fks_in(&tname, &meta, doc, &out, Some(&out))?;
        }
        meta.check_unique(&out)?;
        // Parent-side FK: key values that disappear must not be referenced.
        // The self-reference child scan runs over the final images (`out`
        // IS the whole final table here — untouched rows included).
        self.check_fk_parent_delete_skipping(&tname, &old_changed, &out, &[], Some(&out))?;
        let changed = changed_docs.clone();
        // Row triggers: BEFORE over the pre/post image pairs while the old
        // state is still the committed truth, AFTER once the rewrite landed.
        let changed_cols: Vec<String> = assignments
            .iter()
            .filter_map(|a| match &a.target {
                sqlparser::ast::AssignmentTarget::ColumnName(c) => Some(obj_name(c)),
                _ => None,
            })
            .collect();
        let fire_update = !changed_docs.is_empty() && self.triggers_exist_for(&tname);
        if fire_update {
            for (old, new) in old_changed.iter().zip(changed_docs.iter()) {
                self.fire_row_triggers(
                    &tname,
                    true,
                    TriggerFireEvent::Update(Some(&changed_cols)),
                    Some(old),
                    Some(new),
                )?;
            }
        }
        // Sticky dotted-field marker on the LOCAL meta clone: rewrite_table
        // persists exactly this meta, so a self.tables write here would be
        // overwritten by the rewrite's own catalog save.
        if !meta.dotted_keys && out.iter().any(|d| d.keys().any(|k| k.contains('.'))) {
            meta.dotted_keys = true;
        }
        self.rewrite_table(&tname, &mut meta, out)?;
        if fire_update {
            for (old, new) in old_changed.iter().zip(changed_docs.iter()) {
                self.fire_row_triggers(
                    &tname,
                    false,
                    TriggerFireEvent::Update(Some(&changed_cols)),
                    Some(old),
                    Some(new),
                )?;
            }
        }
        if let Some(ret) = &update_returning {
            return project_returning(ret, &changed);
        }
        Ok(ExecOutcome::Affected(count))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn exec_update_fast(
        &mut self,
        tname: String,
        meta: TableMeta,
        matches: Vec<(u64, Object)>,
        assignments: &[sqlparser::ast::Assignment],
        selection: &Option<SqlExpr>,
        update_returning: &Option<Vec<SelectItem>>,
        outer: &OuterFrom,
    ) -> Result<ExecOutcome> {
        let mut updates: Vec<(u64, Object, Object)> = Vec::new(); // (loc, old, new)
        for (loc, doc) in matches {
            if !self.matches_cx_in(selection, &doc, outer)? {
                continue; // probe col matched; some other conjunct did not
            }
            let mut doc = doc;
            // The pre-update image must be captured before assignments run —
            // it drives the old-key removal in the apply loop below; pushing
            // the mutated doc as both old and new leaves stale entries.
            let old_doc = doc.clone();
            let new_vals = self.eval_assignments_cx(assignments, &doc, outer)?;
            for (col_name, v) in new_vals {
                doc.insert(col_name, v);
            }
            meta.check(&doc)?;
            updates.push((loc, old_doc, doc));
        }
        // 子侧 FK 在收集完本语句全部最终像之后统一校验:自引用/行间引用
        // 的父候选包含批次自身。零命中在下方早退(空批次检查是 no-op)。
        if updates.is_empty() {
            if let Some(ret) = update_returning {
                return project_returning(ret, &[]);
            }
            return Ok(ExecOutcome::Affected(0));
        }
        // Self-referential FKs resolve parents against the statement's
        // COMPLETE final state (pre-state minus replaced old images plus the
        // new images); the pre-state heap alone would satisfy keys this
        // statement vacates (`SET id = 3, p = 1 WHERE id = 1`). Only
        // self-FK tables pay the extra table load.
        let self_final: Option<Vec<Object>> =
            if meta.foreign_keys.iter().any(|(_, rt, _)| rt == &tname) {
                let old_keys: std::collections::BTreeSet<Vec<u8>> =
                    updates.iter().map(|(_, o, _)| object_key(o)).collect();
                let mut final_state = self.table_docs_cx(&tname)?;
                final_state.retain(|d| !old_keys.contains(&object_key(d)));
                final_state.extend(updates.iter().map(|(_, _, n)| n.clone()));
                Some(final_state)
            } else {
                None
            };
        {
            let batch: Vec<Object> = updates.iter().map(|(_, _, n)| n.clone()).collect();
            for doc in &batch {
                // Big UPDATEs (with a FK, check_fks_in also scans the parent
                // table per row) must stay interruptible.
                self.stmt_deadline.check()?;
                self.check_fks_in(&tname, &meta, doc, &batch, self_final.as_deref())?;
            }
        }
        // Parent-side FK check before any page is touched. `updates` rows
        // are the only ones whose key values can disappear; their new docs
        // are the survivors (referenced columns are unique in practice).
        let old_docs: Vec<Object> = updates.iter().map(|(_, o, _)| o.clone()).collect();
        let new_docs: Vec<Object> = updates.iter().map(|(_, _, n)| n.clone()).collect();
        self.check_fk_parent_delete_skipping(
            &tname,
            &old_docs,
            &new_docs,
            &[],
            self_final.as_deref(),
        )?;
        // Pre-statement docs for the legacy whole-set unique check below —
        // loaded only when that check can actually run (a constraint column
        // without a tree). Tables with full trees enforce uniqueness in the
        // apply loop; loading every doc here made single-row UPDATE loops
        // quadratic.
        let legacy_check = (meta.primary_key.is_some() || !meta.unique.is_empty())
            && meta
                .primary_key
                .iter()
                .chain(meta.unique.iter())
                .any(|c| !meta.index_roots.contains_key(c));
        let all_docs: Option<Vec<Object>> = if legacy_check {
            Some(self.table_docs_cx(&tname)?)
        } else {
            None
        };
        let mut heap = Heap {
            pages: meta.pages.clone(),
            overflow_free: meta.overflow_free.clone(),
            dropped: Vec::new(),
        };
        let mut roots = meta.index_roots.clone();
        let idx_specs = idx_specs(&meta, &roots);
        // BEFORE UPDATE over the pre/post pairs while the old images are
        // still the committed truth (rows update in place below).
        let update_changed_cols: Vec<String> = assignments
            .iter()
            .filter_map(|a| match &a.target {
                sqlparser::ast::AssignmentTarget::ColumnName(c) => Some(obj_name(c)),
                _ => None,
            })
            .collect();
        let fire_update = self.triggers_exist_for(&tname);
        if fire_update {
            for (_, old, new) in &updates {
                self.fire_row_triggers(
                    &tname,
                    true,
                    TriggerFireEvent::Update(Some(&update_changed_cols)),
                    Some(old),
                    Some(new),
                )?;
            }
        }
        let mut tx = self.pager.begin_tx();
        // Pass 1: drop every updated row's old index entries before any
        // replacement lands — a multi-row unique-key shift (SET id = id + 1,
        // key swaps between rows) must not collide with a not-yet-replaced
        // row's still-present old key.
        for (loc, old_doc, _) in &updates {
            if let Err(e) =
                reindex_remove(&self.pager, &mut tx, &idx_specs, &mut roots, old_doc, *loc)
            {
                self.pager.abort_tx(tx)?;
                return Err(e);
            }
        }
        // Pass 2: apply the new images.
        for i in 0..updates.len() {
            let (loc, new_doc) = (updates[i].0, updates[i].2.clone());
            let page = crate::heap::unpack_loc(loc).0;
            let before = heap.page_docs(&PageReader::current(&self.pager), &tx, page)?;
            // Locators of the rows not yet processed *before* the repack
            // fixup below mutates them: a survivor move naming one of these
            // slots is the pending row's own image, and repointing it would
            // resurrect the old-key entry pass 1 just removed.
            let pending_locs: Vec<u64> = updates.iter().skip(i + 1).map(|(l, _, _)| *l).collect();
            let out = heap.replace(&self.pager, &mut tx, loc, &new_doc)?;
            // An in-page re-pack moved this page's survivors: pending locators
            // must follow, or a later update would target whatever document
            // now occupies the stale slot.
            if !out.moved.is_empty() {
                for pending in updates.iter_mut().skip(i + 1) {
                    if let Some((_, new_l)) = out.moved.iter().find(|(old, _)| *old == pending.0) {
                        pending.0 = *new_l;
                    }
                }
            }
            for (old_l, new_l) in &out.moved {
                // Current row: entries removed in pass 1, new entry lands at
                // `out.placed` below. Pending row: same, in its own
                // iteration. Already-processed rows (and untouched rows)
                // must be repointed — their new/current image is what sat in
                // that slot, and skipping them leaves a stale index entry.
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
        // Legacy files whose constraint columns predate trees keep the
        // whole-set duplicate check (mirrors the INSERT path); tables with
        // trees enforce uniqueness through the apply loop above.
        if legacy_check {
            let combined: Vec<Object> = all_docs
                .expect("pre-statement docs loaded for the legacy check")
                .into_iter()
                .map(|d| match updates.iter().find(|(_, old, _)| *old == d) {
                    Some((_, _, new)) => new.clone(),
                    None => d,
                })
                .collect();
            if let Err(e) = meta.check_unique(&combined) {
                self.pager.abort_tx(tx)?;
                return Err(e);
            }
        }
        // Emptied heap pages are released inside this transaction so later
        // statements reuse them instead of growing the file.
        for p in std::mem::take(&mut heap.dropped) {
            self.pager.free_page(&mut tx, p)?;
        }
        let dotted_now = self.note_dotted_fields(&tname, updates.iter().map(|(_, _, new)| new));
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
        // AFTER UPDATE once the new images are committed.
        if fire_update {
            for (_, old, new) in &updates {
                self.fire_row_triggers(
                    &tname,
                    false,
                    TriggerFireEvent::Update(Some(&update_changed_cols)),
                    Some(old),
                    Some(new),
                )?;
            }
        }
        // Explicit ids may bump the AUTOINCREMENT watermark.
        if let Some(col) = &meta.autoinc {
            if assignments
                .iter()
                .any(|a| matches!(&a.target, sqlparser::ast::AssignmentTarget::ColumnName(c) if obj_name(c) == *col))
            {
                self.autoinc_cache.remove(&tname);
            }
        }
        let changed: Vec<Object> = updates.into_iter().map(|(_, _, d)| d).collect();
        if let Some(ret) = update_returning {
            return project_returning(ret, &changed);
        }
        Ok(ExecOutcome::Affected(changed.len() as u64))
    }

    pub(super) fn exec_delete(
        &mut self,
        from: sqlparser::ast::FromTable,
        using: Option<Vec<sqlparser::ast::TableWithJoins>>,
        mut selection: Option<SqlExpr>,
        returning: Option<Vec<SelectItem>>,
    ) -> Result<ExecOutcome> {
        let sqlparser::ast::FromTable::WithFromKeyword(tables) = from else {
            return err("unsupported DELETE form");
        };
        if tables.len() != 1 {
            return err("DELETE from exactly one table");
        }
        // `DELETE t FROM t JOIN u …` / `DELETE FROM t JOIN u …` parse with
        // the join in `tables[0].joins`; dropping it would delete every row
        // regardless of the join condition. Reject loudly (use USING).
        if !tables[0].joins.is_empty() {
            return err("DELETE ... JOIN is not supported (use DELETE ... USING)");
        }
        let sqlparser::ast::TableFactor::Table { name, alias, .. } = tables[0].relation.clone()
        else {
            return err("only simple table names in DELETE");
        };
        let tname = obj_name(&name);
        if self.tables.get(&tname).is_some_and(|m| m.is_view()) {
            return err(format!(
                "cannot DELETE from view {tname} (it is a view, not a table)"
            ));
        }
        if self.tables.get(&tname).is_some_and(|m| m.is_matview()) {
            return err(format!(
                "cannot DELETE from materialized view {tname} \
                 (REFRESH MATERIALIZED VIEW {tname} recomputes it)"
            ));
        }
        let tkey = alias
            .as_ref()
            .map(|a| a.name.value.clone())
            .unwrap_or_else(|| tname.clone());
        // ROWNUM has no row stream to number in a DELETE's own WHERE: it
        // reads NULL, and `NULL <= n` is TRUE under the documented total
        // order — an Oracle-style `DELETE ... WHERE ROWNUM <= 1` would
        // silently mass-delete. Fail loudly instead.
        if selection.as_ref().is_some_and(refs_rownum) {
            return err("ROWNUM is not supported in DELETE (use a subquery: WHERE id IN (SELECT ... WHERE ROWNUM <= n))");
        }
        // Outer scope for correlated subqueries: the target (name + alias)
        // plus the USING tables (the USING form evaluates WHERE over the
        // merged image).
        let mut outer = OuterFrom::new();
        outer.insert(tname.clone());
        outer.insert(tkey.clone());
        if let Some(using) = &using {
            from_list_names(using, &mut outer);
        }
        if let Some(sel) = &mut selection {
            self.subst_expr_cx_scoped(sel, &outer)?;
        }
        let mut meta = self
            .tables
            .get(&tname)
            .map(|a| a.as_ref().clone())
            .unwrap_or_default();
        // Fast path: probe-able WHERE on an indexed column — remove the
        // matching rows in place (page re-pack) instead of rewriting the
        // whole table.
        if using.is_none() {
            if let Some(matches) =
                self.index_probe_cx(&tname, Some(tkey.as_str()), &selection, &Ctes::new())?
            {
                return self.exec_delete_fast(tname, meta, matches, &selection, &returning, &outer);
            }
        }
        let docs = self.table_docs_cx(&tname)?;
        let (kept, removed) = if let Some(using) = using {
            // DELETE ... USING: qualify over the joined rows and mark the
            // target docs whose combination satisfies WHERE.
            let mut from_list = vec![tables[0].clone()];
            from_list.extend(using);
            let merged = self.load_from_cx(&from_list, &None, &Ctes::new())?;
            let prefix = format!("{tkey}.");
            let mut rm: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
            for m in &merged {
                if !self.matches_cx_in(&selection, m, &outer)? {
                    continue;
                }
                let tdoc = target_doc_from_merged(m, &prefix);
                rm.insert(object_key(&tdoc));
            }
            let mut kept = Vec::new();
            let mut removed = Vec::new();
            for doc in docs {
                if rm.contains(&object_key(&doc)) {
                    removed.push(doc);
                } else {
                    kept.push(doc);
                }
            }
            (kept, removed)
        } else {
            let mut kept = Vec::new();
            let mut removed = Vec::new();
            for doc in docs {
                if self.matches_cx_in(&selection, &doc, &outer)? {
                    removed.push(doc);
                } else {
                    kept.push(doc);
                }
            }
            (kept, removed)
        };
        let count = removed.len() as u64;
        // The self-reference child scan runs over the FINAL state (`kept`):
        // rows deleted by this same statement cannot end up orphaned, so
        // `DELETE FROM self_ref_table` may clear parent and child rows
        // together (SQLite/PG statement-final semantics).
        self.check_fk_parent_delete_skipping(&tname, &removed, &[], &[], Some(&kept))?;
        // BEFORE DELETE while the rows are still the committed truth.
        let fire_delete = !removed.is_empty() && self.triggers_exist_for(&tname);
        if fire_delete {
            for doc in &removed {
                self.fire_row_triggers(&tname, true, TriggerFireEvent::Delete, Some(doc), None)?;
            }
        }
        self.rewrite_table(&tname, &mut meta, kept)?;
        if fire_delete {
            for doc in &removed {
                self.fire_row_triggers(&tname, false, TriggerFireEvent::Delete, Some(doc), None)?;
            }
        }
        if let Some(ret) = &returning {
            return project_returning(ret, &removed);
        }
        Ok(ExecOutcome::Affected(count))
    }

    pub(super) fn exec_delete_fast(
        &mut self,
        tname: String,
        meta: TableMeta,
        matches: Vec<(u64, Object)>,
        selection: &Option<SqlExpr>,
        returning: &Option<Vec<SelectItem>>,
        outer: &OuterFrom,
    ) -> Result<ExecOutcome> {
        let mut targets: Vec<(u64, Object)> = Vec::new();
        for (loc, doc) in matches {
            if self.matches_cx_in(selection, &doc, outer)? {
                targets.push((loc, doc));
            }
        }
        if targets.is_empty() {
            if let Some(ret) = returning {
                return project_returning(ret, &[]);
            }
            return Ok(ExecOutcome::Affected(0));
        }
        // Parent-side FK check before any page is touched. The self-reference
        // child scan runs over the final state (pre-state minus the removed
        // rows), so deleting parent and child rows of a self-FK table in one
        // statement is legal; only self-FK tables pay the extra table load.
        let removed_docs: Vec<Object> = targets.iter().map(|(_, d)| d.clone()).collect();
        let self_final: Option<Vec<Object>> =
            if meta.foreign_keys.iter().any(|(_, rt, _)| rt == &tname) {
                let rm: std::collections::BTreeSet<Vec<u8>> =
                    targets.iter().map(|(_, d)| object_key(d)).collect();
                Some(
                    self.table_docs_cx(&tname)?
                        .into_iter()
                        .filter(|d| !rm.contains(&object_key(d)))
                        .collect(),
                )
            } else {
                None
            };
        self.check_fk_parent_delete_skipping(
            &tname,
            &removed_docs,
            &[],
            &[],
            self_final.as_deref(),
        )?;
        let mut heap = Heap {
            pages: meta.pages.clone(),
            overflow_free: meta.overflow_free.clone(),
            dropped: Vec::new(),
        };
        let mut roots = meta.index_roots.clone();
        let idx_specs = idx_specs(&meta, &roots);
        // BEFORE DELETE while the rows are still the committed truth.
        let fire_delete = self.triggers_exist_for(&tname);
        if fire_delete {
            for (_, doc) in &targets {
                self.fire_row_triggers(&tname, true, TriggerFireEvent::Delete, Some(doc), None)?;
            }
        }
        let mut tx = self.pager.begin_tx();
        let affected: std::collections::BTreeSet<u32> = targets
            .iter()
            .map(|(l, _)| crate::heap::unpack_loc(*l).0)
            .collect();
        let mut before = Vec::new();
        for pid in affected {
            before.extend(heap.page_docs(&PageReader::current(&self.pager), &tx, pid)?);
        }
        let locs: Vec<u64> = targets.iter().map(|(l, _)| *l).collect();
        let moves = heap.remove_many(&self.pager, &mut tx, &locs)?;
        for (loc, doc) in &targets {
            reindex_remove(&self.pager, &mut tx, &idx_specs, &mut roots, doc, *loc)?;
        }
        for (old_l, new_l) in &moves {
            reindex_repoint(
                &self.pager,
                &mut tx,
                &idx_specs,
                &mut roots,
                &before,
                *old_l,
                *new_l,
            )?;
        }
        // Emptied heap pages are released inside this transaction so later
        // statements reuse them instead of growing the file.
        for p in std::mem::take(&mut heap.dropped) {
            self.pager.free_page(&mut tx, p)?;
        }
        if heap.pages != meta.pages
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
        // Deleted rows may have held the AUTOINCREMENT watermark.
        self.autoinc_cache.remove(&tname);
        let removed: Vec<Object> = targets.into_iter().map(|(_, d)| d).collect();
        if fire_delete {
            for doc in &removed {
                self.fire_row_triggers(&tname, false, TriggerFireEvent::Delete, Some(doc), None)?;
            }
        }
        let count = removed.len() as u64;
        if let Some(ret) = returning {
            return project_returning(ret, &removed);
        }
        Ok(ExecOutcome::Affected(count))
    }

    pub(super) fn exec_insert(&mut self, insert: sqlparser::ast::Insert) -> Result<ExecOutcome> {
        if insert.output.is_some() {
            return err("OUTPUT is not supported (use RETURNING)");
        }
        let TableObject::TableName(name) = &insert.table else {
            return err("unsupported INSERT target");
        };
        let table = obj_name(name);
        // Arc bump, not a meta deep clone: INSERT only ever reads the meta,
        // and this is the hottest write path.
        let Some(meta) = self.tables.get(&table).cloned() else {
            return err(format!("table {table} does not exist"));
        };
        ensure_not_view("INSERT into", &table, &meta)?;
        let mut columns: Vec<String> = if insert.columns.is_empty() {
            meta.columns.clone()
        } else {
            // A repeated name makes the later zip().collect() silently
            // overwrite the earlier value — reject like mainstream engines.
            let cols: Vec<String> = insert.columns.iter().map(obj_name).collect();
            let mut seen = std::collections::BTreeSet::new();
            if let Some(dup) = cols.iter().find(|c| !seen.insert((*c).clone())) {
                return err(format!("duplicate column name: {dup}"));
            }
            // Deliberately NOT validated against meta.columns: this is a
            // document store — an explicit list may name schemaless fields
            // the declared columns have not seen yet (observed-columns
            // tracking and the schemaless SELECT * union depend on it;
            // useradmin's own grants rows ride the same behavior).
            cols
        };
        let Some(source) = &insert.source else {
            return err("INSERT requires VALUES");
        };
        let rows: Vec<Vec<Value>> = match &*source.body {
            SetExpr::Values(value_rows) => {
                let mut rows = Vec::new();
                for parens in &value_rows.rows {
                    rows.push(
                        parens
                            .content
                            .iter()
                            .map(eval_const)
                            .collect::<Result<_>>()?,
                    );
                }
                rows
            }
            // INSERT INTO t (cols...) SELECT ...: take the SELECT's rows.
            // Without a column list the mapping is POSITIONAL onto the
            // target table's declared columns (standard SQL) — the query's
            // output NAMES used to redefine the keys, silently dropping
            // every value into ghost fields whenever the projection wasn't
            // bare same-named identifiers (aliases, literals, `SELECT *`
            // from a differently-named source).
            SetExpr::Select(_) => {
                let ExecOutcome::Rows(r) = self.exec_query_cx(source.as_ref().clone())? else {
                    return err("INSERT source must be VALUES or SELECT");
                };
                if r.columns.len() != columns.len() {
                    return err(format!(
                        "INSERT SELECT has {} columns but {} are expected",
                        r.columns.len(),
                        columns.len()
                    ));
                }
                r.rows
            }
            other => return err(format!("unsupported INSERT source: {other}")),
        };
        // An empty VALUES list is a syntax-level mistake; an empty SELECT
        // source (WHERE filtered everything) inserts 0 rows like every
        // mainstream engine — ETL batches legitimately hit it.
        if rows.is_empty() && matches!(&*source.body, SetExpr::Values(_)) {
            return err("INSERT has no rows");
        }
        // INSERT ... SELECT into an auto-GUID table cannot be replicated:
        // peers re-run the SELECT and would each fill their own random
        // GUIDs, silently diverging. VALUES is required.
        if meta.autoguid.is_some() && matches!(&*source.body, SetExpr::Select(_)) {
            return err(
                "auto-generated GUID columns do not support INSERT ... SELECT \
                 (use INSERT ... VALUES, with or without explicit ids)",
            );
        }
        // Build all documents first, validate, and only then write — a
        // failed statement must leave the table untouched.
        // AUTOINCREMENT: append the column when the INSERT omits it, then
        // assign max(id)+1 to missing/NULL values per row. Auto-generated
        // GUID columns follow the same omitted/NULL fill with fresh
        // time-ordered UUIDv7 values.
        let autoinc_appended = match &meta.autoinc {
            Some(col) if !columns.contains(col) => {
                columns.push(col.clone());
                true
            }
            _ => false,
        };
        let autoguid_appended = match &meta.autoguid {
            Some(col) if !columns.contains(col) => {
                columns.push(col.clone());
                true
            }
            _ => false,
        };
        let mut guid_filled = autoguid_appended;
        // A wall-clock DEFAULT (`DEFAULT NOW()`) resolved per row on THIS
        // node: the journaled INSERT must carry the explicit value or every
        // peer would stamp its own clock at replay (same rewrite trigger as
        // auto-GUID below).
        let mut default_clock_filled = false;
        let mut next_autoinc = self.autoinc_next_for(&table, &meta)?;
        // DEFAULT texts parsed once per statement (was re-parsed by
        // sqlparser for every row that omitted the column).
        let mut default_exprs: std::collections::HashMap<String, SqlExpr> =
            std::collections::HashMap::new();
        let mut new_docs: Vec<Object> = Vec::new();
        for row in rows {
            // INSERT … SELECT walks every source row (millions on a big
            // backfill): the timeout must be able to interrupt it.
            self.stmt_deadline.check()?;
            let mut row = row;
            if autoinc_appended {
                row.push(Value::Int(next_autoinc));
                next_autoinc = next_autoinc.saturating_add(1);
            }
            if autoguid_appended {
                row.push(Value::Str(crate::guid::uuidv7()));
            }
            if row.len() != columns.len() {
                return err(format!(
                    "INSERT has {} values but {} columns",
                    row.len(),
                    columns.len()
                ));
            }
            if !autoinc_appended {
                if let Some(col) = &meta.autoinc {
                    if let Some(idx) = columns.iter().position(|c| c == col) {
                        match row.get(idx) {
                            Some(Value::Null) | None => {
                                row[idx] = Value::Int(next_autoinc);
                                next_autoinc = next_autoinc.saturating_add(1);
                            }
                            // An explicit id at/above the statement-start
                            // watermark must advance the watermark *within*
                            // the statement, or a later row of the same
                            // INSERT that omits the id reuses it and dies on
                            // UNIQUE.
                            Some(Value::Int(v)) => {
                                next_autoinc = next_autoinc.max(v.saturating_add(1));
                            }
                            _ => {}
                        }
                    }
                }
            }
            if !autoguid_appended {
                if let Some(col) = &meta.autoguid {
                    if let Some(idx) = columns.iter().position(|c| c == col) {
                        if matches!(row.get(idx), Some(Value::Null) | None) {
                            row[idx] = Value::Str(crate::guid::uuidv7());
                            guid_filled = true;
                        }
                    }
                }
            }
            let mut doc: Object = columns.iter().cloned().zip(row).collect();
            // DEFAULT: fill declared columns the INSERT omitted (parse once
            // per statement; evaluation stays per row).
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
                        default_clock_filled = true;
                    }
                    doc.insert(col.clone(), eval_const(e)?);
                }
            }
            meta.check(&doc)?;
            new_docs.push(doc);
        }
        // Conflict policy: plain (error on duplicates), REPLACE INTO /
        // OR REPLACE (drop conflicting rows first), ON CONFLICT DO NOTHING /
        // OR IGNORE (skip conflicting new rows), and the upsert-UPDATE
        // forms: ON CONFLICT <target> DO UPDATE SET … [WHERE …]
        // (PostgreSQL) / ON DUPLICATE KEY UPDATE … (MySQL) — the
        // conflicting row is updated in place.
        let replace = insert.replace_into
            || matches!(insert.or, Some(sqlparser::ast::SqliteOnConflict::Replace));
        let do_nothing = matches!(
            insert.on,
            Some(sqlparser::ast::OnInsert::OnConflict(ref c))
                if matches!(c.action, sqlparser::ast::OnConflictAction::DoNothing)
        ) || matches!(insert.or, Some(sqlparser::ast::SqliteOnConflict::Ignore));
        let (do_update, du_assignments, du_selection) = match &insert.on {
            Some(sqlparser::ast::OnInsert::OnConflict(c)) => match &c.action {
                sqlparser::ast::OnConflictAction::DoUpdate(du) => {
                    (true, du.assignments.clone(), du.selection.clone())
                }
                _ => (false, Vec::new(), None),
            },
            Some(sqlparser::ast::OnInsert::DuplicateKeyUpdate(assignments)) => {
                (true, assignments.clone(), None)
            }
            _ => (false, Vec::new(), None),
        };
        if replace && do_nothing {
            // Displacement deletes the conflicting old rows first, then DO
            // NOTHING skips every incoming row — a silent net delete. Reject
            // the mix like the DO UPDATE combination below.
            return err("OR REPLACE / REPLACE INTO cannot combine with ON CONFLICT DO NOTHING");
        }
        if do_update {
            if replace || do_nothing {
                return err(
                    "OR REPLACE / REPLACE INTO / OR IGNORE cannot combine with an upsert-UPDATE clause",
                );
            }
            // Non-deterministic values in the update arm would diverge the
            // cluster: peers replay the statement text verbatim (INSERT
            // defaults are rewritten with resolved literals, the UPDATE arm
            // has no such rewrite — same rule as UPDATE/DELETE/MERGE;
            // wall clocks are exempt because the fold covers them).
            for e in du_assignments
                .iter()
                .map(|a| &a.value)
                .chain(du_selection.iter())
            {
                if calls_newid(e) {
                    return err(
                        "NEWID()/NEWSEQUENTIALID() is not supported in DO UPDATE SET/WHERE \
                         (replication replay would diverge)",
                    );
                }
            }
            // Wall-clock functions in the arm are FOLDED to literals by the
            // statement-level preprocessing (fold_wall_clocks rewrites the
            // whole text, on-clause included), so they are accepted with
            // deterministic replay — no guard here.
            if refs_rownum_in(&du_assignments, &du_selection) {
                return err("ROWNUM is not supported in DO UPDATE SET/WHERE");
            }
            // PostgreSQL requires the conflict target for DO UPDATE; the
            // MySQL spelling is untargeted by definition (any unique key).
            if matches!(
                insert.on,
                Some(sqlparser::ast::OnInsert::OnConflict(ref c))
                    if c.conflict_target.is_none()
            ) {
                return err(
                    "ON CONFLICT DO UPDATE requires a conflict target (PostgreSQL semantics) — \
                     name the unique key with ON CONFLICT (cols) / ON CONSTRAINT, or use the \
                     MySQL spelling ON DUPLICATE KEY UPDATE for any-key matching",
                );
            }
            // EXCLUDED / VALUES() are rewritten only in the direct SET/WHERE
            // trees; inside a subquery they would mis-resolve to the
            // existing row's column — reject loudly instead.
            for e in du_assignments
                .iter()
                .map(|a| &a.value)
                .chain(du_selection.iter())
            {
                if upsert_proposed_in_subquery(e) {
                    return err(
                        "EXCLUDED / VALUES() inside a subquery of DO UPDATE SET/WHERE is not supported",
                    );
                }
            }
        }
        // Replication-safe rewrite: the statement as written would let every
        // peer fill its own random GUIDs — or stamp its own clock for a
        // wall-clock DEFAULT. Emit a canonical INSERT carrying the generated
        // ids / resolved instants explicitly — with the conflict clause
        // preserved so peers resolve the same conflicts against identical
        // state. NEWID()/NEWSEQUENTIALID() in the source (VALUES or SELECT)
        // are the same class: the writing node rolls the ids and ships the
        // literal values.
        let source_calls_nondet = insert
            .source
            .as_ref()
            .is_some_and(|s| crate::stmt::mentions_nondet_call(&s.to_string()));
        // A zero-row INSERT ... SELECT (e.g. NEWID() in the source under a
        // false WHERE) must NOT enter the rewrite: with no documents the
        // canonical form below degenerates to `INSERT INTO t () VALUES ` —
        // unparseable text that rode the journal and broke every replaying
        // peer. The original statement replays identically (deterministically
        // empty) on each node, so keeping it is replication-safe.
        if (guid_filled || default_clock_filled || source_calls_nondet) && !new_docs.is_empty() {
            let policy = if replace {
                "OR REPLACE "
            } else if do_nothing {
                "OR IGNORE "
            } else {
                ""
            };
            let mut sql = format!("INSERT {}INTO {} (", policy, quote_ident(&table));
            let cols: Vec<String> = new_docs
                .first()
                .map(|d| d.keys().cloned().collect())
                .unwrap_or_default();
            sql.push_str(
                &cols
                    .iter()
                    .map(|c| quote_ident(c))
                    .collect::<Vec<_>>()
                    .join(", "),
            );
            sql.push_str(") VALUES ");
            let mut rendered = Vec::with_capacity(new_docs.len());
            for doc in &new_docs {
                let vals = cols
                    .iter()
                    .map(|c| value_literal(doc.get(c).unwrap_or(&Value::Null)))
                    .collect::<Result<Vec<_>>>()?;
                rendered.push(format!("({})", vals.join(", ")));
            }
            sql.push_str(&rendered.join(", "));
            // Preserve the conflict clause verbatim: the OR-prefix only
            // covers the SQLite spellings, and a targeted ON CONFLICT scope
            // must not widen to OR IGNORE on replaying peers.
            if let Some(on) = &insert.on {
                sql.push_str(&on.to_string());
            }
            self.resolved_sql = Some(sql);
        } else if source_calls_nondet && new_docs.is_empty() {
            // Zero rows from a NONDETERMINISTIC source: replaying the
            // original text re-rolls RAND()/NEWID() on every peer — an
            // empty draw here can be non-empty there, silently forking the
            // cluster. Ship a deterministically-empty statement instead.
            // (INSERT ... SELECT is safe to emit: an auto-GUID table never
            // reaches this point — that shape is rejected above.)
            let col = meta.columns.first().cloned().unwrap_or_else(|| "v".into());
            let q = quote_ident(&col);
            self.resolved_sql = Some(format!(
                "INSERT INTO {} ({q}) SELECT {q} FROM (VALUES (NULL)) AS __empty__({q}) WHERE 1 = 0",
                quote_ident(&table)
            ));
        }

        // One pager transaction for heap pages and index trees alike: a
        // statement is either fully applied or not at all, and commits once.
        let mut heap = Heap {
            pages: meta.pages.clone(),
            overflow_free: meta.overflow_free.clone(),
            dropped: Vec::new(),
        };
        let mut roots = meta.index_roots.clone();
        let mut tx = self.pager.begin_tx();

        // Constraint columns with a usable tree: duplicates are detected and
        // enforced through the trees (O(log n) per row) instead of scanning
        // and cloning the whole table.
        let indexed: Vec<String> = meta
            .primary_key
            .iter()
            .chain(meta.unique.iter())
            .filter(|c| roots.contains_key(*c))
            .cloned()
            .collect();
        // Every indexed column (constraint trees + plain CREATE INDEX trees),
        // hoisted once so the per-row maintenance loops below do not clone
        // the roots map per row.
        let idx_specs = idx_specs(&meta, &roots);
        // Composite UNIQUE trees (CREATE UNIQUE INDEX over several columns)
        // take part in the *undirected* conflict policies (OR IGNORE /
        // REPLACE INTO / ON DUPLICATE KEY UPDATE = "any unique key") exactly
        // like the single-column constraint trees — probing only `indexed`
        // let those conflicts fall through to the tree-level insert and
        // error instead of skipping/replacing/updating.
        let composite_uniques: Vec<&IdxSpec> = idx_specs
            .iter()
            .filter(|s| s.unique && s.cols.len() > 1)
            .collect();
        // PARTIAL UNIQUE trees (CREATE UNIQUE INDEX … WHERE): their
        // conflicts are predicate-scoped — a candidate row conflicts only
        // when it ITSELF satisfies the predicate (then the hit row is in
        // the tree by construction). Probed for the undirected policies and
        // OR REPLACE displacement; never an ON CONFLICT target scope
        // (resolve_conflict_columns knows constraint columns only).
        let partial_uniques: Vec<&IdxSpec> = idx_specs
            .iter()
            .filter(|s| s.unique && s.pred.is_some())
            .collect();

        // ON CONFLICT [target]: the target names one unique constraint whose
        // conflicts are skipped; without it every unique constraint takes
        // part (SQLite/PG semantics). An unresolvable target errors instead
        // of silently widening the scope.
        let conflict_scope: Option<String> = match &insert.on {
            Some(sqlparser::ast::OnInsert::OnConflict(c)) => match &c.conflict_target {
                None => None,
                Some(sqlparser::ast::ConflictTarget::Columns(cols)) => {
                    let cols: Vec<String> = cols.iter().map(|i| i.value.clone()).collect();
                    Some(resolve_conflict_columns(&meta, &cols)?)
                }
                Some(sqlparser::ast::ConflictTarget::OnConstraint(name)) => {
                    let iname = obj_name(name);
                    Some(resolve_conflict_constraint(&meta, &table, &iname)?)
                }
            },
            _ => None,
        };
        // The targeted conflict root's column list, resolved once per
        // statement (was a fresh `index_columns_of` allocation per row).
        let scope_cols: Option<Vec<String>> = conflict_scope
            .as_deref()
            .map(|root| meta.index_columns_of(root));

        // 批内重复键:后者胜(SQLite/MySQL 的 OR REPLACE 逐行语义)。位移
        // 扫描只看语句前的树,批内冲突随后被树级 unique 检查整条打报错;
        // 先按唯一键把「被更晚的行覆盖」的早行去掉,再走位移扫描。判重用
        // 编码字节,与树查找同源。
        if replace
            && (!indexed.is_empty() || !composite_uniques.is_empty() || !partial_uniques.is_empty())
            && new_docs.len() > 1
        {
            let mut last_at: std::collections::BTreeMap<(String, Vec<u8>), usize> =
                std::collections::BTreeMap::new();
            for (i, doc) in new_docs.iter().enumerate() {
                for col in &indexed {
                    if let Some(k) = doc
                        .get(col)
                        .filter(|v| !matches!(v, Value::Null))
                        .and_then(|v| encode::encode_to_vec(v).ok())
                    {
                        last_at.insert((col.clone(), k), i);
                    }
                }
                for spec in &composite_uniques {
                    if let Some(k) =
                        index_key_of_spec(doc, spec).and_then(|v| encode::encode_to_vec(&v).ok())
                    {
                        last_at.insert((spec.root_key.clone(), k), i);
                    }
                }
                for spec in &partial_uniques {
                    if !matches!(
                        eval_expr(spec.pred.as_ref().expect("pred set"), doc)?,
                        Value::Bool(true)
                    ) {
                        continue;
                    }
                    if let Some(k) =
                        index_key_of_spec(doc, spec).and_then(|v| encode::encode_to_vec(&v).ok())
                    {
                        last_at.insert((spec.root_key.clone(), k), i);
                    }
                }
            }
            let mut kept: Vec<Object> = Vec::with_capacity(new_docs.len());
            for (i, doc) in new_docs.iter().enumerate() {
                let shadowed = indexed.iter().any(|col| {
                    doc.get(col)
                        .filter(|v| !matches!(v, Value::Null))
                        .and_then(|v| encode::encode_to_vec(v).ok())
                        .and_then(|k| last_at.get(&(col.clone(), k)))
                        .is_some_and(|&j| j > i)
                }) || composite_uniques.iter().any(|spec| {
                    index_key_of_spec(doc, spec)
                        .and_then(|v| encode::encode_to_vec(&v).ok())
                        .and_then(|k| last_at.get(&(spec.root_key.clone(), k)))
                        .is_some_and(|&j| j > i)
                }) || partial_uniques.iter().any(|spec| {
                    matches!(
                        eval_expr(spec.pred.as_ref().expect("pred set"), doc),
                        Ok(Value::Bool(true))
                    ) && index_key_of_spec(doc, spec)
                        .and_then(|v| encode::encode_to_vec(&v).ok())
                        .and_then(|k| last_at.get(&(spec.root_key.clone(), k)))
                        .is_some_and(|&j| j > i)
                });
                if !shadowed {
                    kept.push(doc.clone());
                }
            }
            new_docs = kept;
        }

        // Rows displaced by REPLACE INTO / OR REPLACE.
        let mut displaced: Vec<u64> = Vec::new();
        let mut displaced_docs: Vec<Object> = Vec::new();
        if replace
            && (!indexed.is_empty() || !composite_uniques.is_empty() || !partial_uniques.is_empty())
        {
            for n in &new_docs {
                for col in &indexed {
                    let Some(v) = n.get(col) else { continue };
                    if matches!(v, Value::Null) {
                        continue;
                    }
                    if let Some(loc) = BTree::open(roots[col])
                        .get(&PageReader::current(&self.pager), &tx, v)
                        .map_err(|e| index_err(col, e))?
                    {
                        displaced.push(loc);
                    }
                }
                for spec in &composite_uniques {
                    let Some(k) = index_key_of_spec(n, spec) else {
                        continue;
                    };
                    if let Some(loc) = BTree::open(roots[&spec.root_key])
                        .get(&PageReader::current(&self.pager), &tx, &k)
                        .map_err(|e| index_err(&spec.root_key, e))?
                    {
                        displaced.push(loc);
                    }
                }
                // Predicate-scoped displacement: only a qualifying candidate
                // can conflict with the partial tree (a non-qualifying row
                // inserts beside it, displacing nothing).
                for spec in &partial_uniques {
                    if !matches!(
                        eval_expr(spec.pred.as_ref().expect("pred set"), n)?,
                        Value::Bool(true)
                    ) {
                        continue;
                    }
                    let Some(k) = index_key_of_spec(n, spec) else {
                        continue;
                    };
                    if let Some(loc) = BTree::open(roots[&spec.root_key])
                        .get(&PageReader::current(&self.pager), &tx, &k)
                        .map_err(|e| index_err(&spec.root_key, e))?
                    {
                        displaced.push(loc);
                    }
                }
            }
        } else if replace
            && indexed.is_empty()
            && composite_uniques.is_empty()
            && (meta.primary_key.is_some() || !meta.unique.is_empty())
            && !new_docs.is_empty()
        {
            // Legacy layout: the constraint columns predate trees, so no
            // probe can locate the rows OR REPLACE displaces — the rows
            // survived into the whole-set unique check and failed the very
            // keys they were replacing. Find them by content instead
            // (new tables always build trees; only legacy volumes land
            // here). Intra-batch duplicates keep the tree path's
            // "later row wins" semantics.
            let constraint_cols: Vec<&String> =
                meta.primary_key.iter().chain(meta.unique.iter()).collect();
            let key_of = |d: &Object| -> Option<Vec<Vec<u8>>> {
                let mut parts = Vec::with_capacity(constraint_cols.len());
                for c in &constraint_cols {
                    let v = d.get(*c)?;
                    // NULL keys never displace (trees hold no NULL keys).
                    if matches!(v, Value::Null) {
                        return None;
                    }
                    parts.push(encode::encode_to_vec(v).ok()?);
                }
                Some(parts)
            };
            let mut last_at: std::collections::BTreeMap<Vec<Vec<u8>>, usize> =
                std::collections::BTreeMap::new();
            for (i, d) in new_docs.iter().enumerate() {
                if let Some(k) = key_of(d) {
                    last_at.insert(k, i);
                }
            }
            let kept: Vec<Object> = new_docs
                .iter()
                .enumerate()
                .filter(|(i, d)| {
                    let Some(k) = key_of(d) else {
                        return true;
                    };
                    last_at.get(&k).is_some_and(|&j| j == *i)
                })
                .map(|(_, d)| d.clone())
                .collect();
            let old_pairs = self.table_pairs_cx(&table)?;
            for (loc, od) in old_pairs {
                if let Some(k) = key_of(&od) {
                    if kept.iter().any(|n| key_of(n).as_deref() == Some(&k[..])) {
                        displaced.push(loc);
                    }
                }
            }
            new_docs = kept;
        }
        if !displaced.is_empty() {
            // One old row can be hit through several constraints at once
            // (its PK by one new row and its UNIQUE key by another — or one
            // new row matching both). Duplicates would double-count in the
            // FK parent-side scan (`count_in(removed)`) and un-defuse a
            // lost key that surviving rows still carry, rejecting a legal
            // replace; remove_many would also see the locator twice.
            displaced.sort_unstable();
            displaced.dedup();
            let affected: std::collections::BTreeSet<u32> = displaced
                .iter()
                .map(|l| crate::heap::unpack_loc(*l).0)
                .collect();
            let mut before = Vec::new();
            for pid in affected {
                before.extend(heap.page_docs(&PageReader::current(&self.pager), &tx, pid)?);
            }
            // Rows displaced by OR REPLACE are parent-side deletions: their
            // referenced keys must not vanish silently (same contract as
            // DELETE). New rows that keep the same key are replacements.
            displaced_docs = displaced
                .iter()
                .filter_map(|loc| {
                    before
                        .iter()
                        .find(|(l, _)| l == loc)
                        .map(|(_, d)| d.clone())
                })
                .collect();
            // Self-referential FKs must see the statement's FINAL state —
            // pre-state minus the displaced rows plus the placed rows. The
            // committed-state fallback would count references that vanish
            // together with their parent and reject a legal replace.
            let displaced_set: std::collections::BTreeSet<u64> =
                displaced.iter().copied().collect();
            let mut final_rows: Vec<Object> = self
                .table_pairs_cx(&table)?
                .into_iter()
                .filter(|(l, _)| !displaced_set.contains(l))
                .map(|(_, d)| d)
                .collect();
            final_rows.extend(new_docs.iter().cloned());
            self.check_fk_parent_delete_skipping(
                &table,
                &displaced_docs,
                &new_docs,
                &[],
                Some(&final_rows),
            )?;
            // OR REPLACE displacement is a real row deletion: DELETE
            // triggers fire for it (SQLite semantics), BEFORE before the
            // removal lands, AFTER once the row is gone.
            for d in &displaced_docs {
                self.fire_row_triggers(&table, true, TriggerFireEvent::Delete, Some(d), None)?;
            }
            let moves = heap.remove_many(&self.pager, &mut tx, &displaced)?;
            for loc in &displaced {
                let Some((_, doc)) = before.iter().find(|(l, _)| l == loc) else {
                    continue;
                };
                reindex_remove(&self.pager, &mut tx, &idx_specs, &mut roots, doc, *loc)?;
            }
            for (old_l, new_l) in &moves {
                reindex_repoint(
                    &self.pager,
                    &mut tx,
                    &idx_specs,
                    &mut roots,
                    &before,
                    *old_l,
                    *new_l,
                )?;
            }
            for d in &displaced_docs {
                self.fire_row_triggers(&table, false, TriggerFireEvent::Delete, Some(d), None)?;
            }
        }

        // Append the surviving rows and maintain the trees. Tree inserts
        // with unique=true ARE the duplicate check (within-batch duplicates
        // included: staged pages are visible to tree reads).
        let mut placed: Vec<(u64, Object)> = Vec::with_capacity(new_docs.len());
        let mut insert_failed: Option<SqlError> = None;
        // Upsert-UPDATE state: rows updated in place during this statement
        // (locator, pre-image, post-image) plus the RETURNING projection in
        // input order (inserted and updated rows alike).
        let mut updated: Vec<(u64, Object, Object)> = Vec::new();
        // heap.replace can MOVE a row past its page budget (tombstone +
        // append at the heap tail): a later touch of the same logical row
        // then finds it at the NEW locator, and the locator-keyed collapse
        // below would keep two entries — leaking the intermediate image
        // into the FK/unique checks and RETURNING. Map each relocated
        // locator back to the row's first-seen locator.
        let mut origin_of: std::collections::BTreeMap<u64, u64> = std::collections::BTreeMap::new();
        let mut ret_docs: Vec<Object> = Vec::with_capacity(new_docs.len());
        let mut du_outer = OuterFrom::new();
        du_outer.insert(table.clone());
        // Upsert arms fire UPDATE triggers with the arm's assigned columns
        // (UPDATE OF gating applies to them too).
        let du_changed: Option<Vec<String>> = if do_update {
            Some(
                du_assignments
                    .iter()
                    .filter_map(|a| match &a.target {
                        sqlparser::ast::AssignmentTarget::ColumnName(c) => Some(obj_name(c)),
                        _ => None,
                    })
                    .collect(),
            )
        } else {
            None
        };
        'outer: for doc in new_docs.into_iter() {
            // BEFORE INSERT fires before conflict resolution, per candidate
            // row (SQLite ordering: triggers, then constraints).
            self.fire_row_triggers(&table, true, TriggerFireEvent::Insert, None, Some(&doc))?;
            // Conflict probe shared by skip (DO NOTHING / OR IGNORE) and
            // upsert (DO UPDATE): locate the conflicting row's locator.
            // Untargeted = any unique constraint (first hit wins, PK probes
            // first); targeted = the named constraint's tree only — other
            // keys' conflicts still error at the insert below.
            let conflict_loc: Option<u64> = if !do_nothing && !do_update {
                None
            } else {
                match &conflict_scope {
                    None => {
                        let mut hit = None;
                        for col in &indexed {
                            let Some(v) = doc.get(col) else { continue };
                            if matches!(v, Value::Null) {
                                continue;
                            }
                            if let Some(loc) = BTree::open(roots[col])
                                .get(&PageReader::current(&self.pager), &tx, v)
                                .map_err(|e| index_err(col, e))?
                            {
                                hit = Some(loc);
                                break;
                            }
                        }
                        // Composite unique indexes resolve "any unique key"
                        // for the undirected policies too.
                        if hit.is_none() {
                            for spec in &composite_uniques {
                                let Some(k) = index_key_of_spec(&doc, spec) else {
                                    continue;
                                };
                                if let Some(loc) = BTree::open(roots[&spec.root_key])
                                    .get(&PageReader::current(&self.pager), &tx, &k)
                                    .map_err(|e| index_err(&spec.root_key, e))?
                                {
                                    hit = Some(loc);
                                    break;
                                }
                            }
                        }
                        // Predicate-scoped conflicts: only a qualifying
                        // candidate probes the partial tree.
                        if hit.is_none() {
                            for spec in &partial_uniques {
                                if !matches!(
                                    eval_expr(spec.pred.as_ref().expect("pred set"), &doc)?,
                                    Value::Bool(true)
                                ) {
                                    continue;
                                }
                                let Some(k) = index_key_of_spec(&doc, spec) else {
                                    continue;
                                };
                                if let Some(loc) = BTree::open(roots[&spec.root_key])
                                    .get(&PageReader::current(&self.pager), &tx, &k)
                                    .map_err(|e| index_err(&spec.root_key, e))?
                                {
                                    hit = Some(loc);
                                    break;
                                }
                            }
                        }
                        hit
                    }
                    Some(root) => {
                        if let Some(key) = index_key_of(&doc, scope_cols.as_deref().unwrap()) {
                            BTree::open(roots[root.as_str()])
                                .get(&PageReader::current(&self.pager), &tx, &key)
                                .map_err(|e| index_err(root, e))?
                        } else {
                            None
                        }
                    }
                }
            };
            if let Some(loc) = conflict_loc {
                if do_nothing {
                    continue 'outer; // conflict: skip this row
                }
                // ---- upsert arm: update the conflicting row in place ----
                // PostgreSQL semantics: WHERE and SET see the OLD row for
                // bare column references; EXCLUDED.col (and MySQL's
                // VALUES(col)) carry the candidate row's values.
                let page = crate::heap::unpack_loc(loc).0;
                let before = heap.page_docs(&PageReader::current(&self.pager), &tx, page)?;
                let Some((_, existing)) = before.iter().find(|(l, _)| *l == loc) else {
                    self.pager.abort_tx(tx)?;
                    return err(format!(
                        "upsert: conflicting row vanished mid-statement (locator {loc:#x})"
                    ));
                };
                if let Some(sel) = &du_selection {
                    let mut sel = sel.clone();
                    subst_upsert_proposed(&mut sel, &doc)?;
                    if !self.matches_cx_in(&Some(sel), existing, &du_outer)? {
                        // WHERE filtered the update out: the row keeps its
                        // image and the candidate is not inserted (PG).
                        continue 'outer;
                    }
                }
                let mut subst = du_assignments.clone();
                for a in &mut subst {
                    subst_upsert_proposed(&mut a.value, &doc)?;
                }
                let new_vals = match self.eval_assignments_cx(&subst, existing, &du_outer) {
                    Ok(v) => v,
                    Err(e) => {
                        self.pager.abort_tx(tx)?;
                        return Err(e);
                    }
                };
                let mut merged = existing.clone();
                for (col_name, v) in new_vals {
                    merged.insert(col_name, v);
                }
                if let Err(e) = meta.check(&merged) {
                    self.pager.abort_tx(tx)?;
                    return Err(e);
                }
                // Two-phase index maintenance for THIS row: drop its old
                // entries, write the new image, repoint the in-page repack's
                // moved survivors, insert the new entries. Later batch rows
                // see the updated trees (sequential upsert semantics, as in
                // MySQL; PostgreSQL would reject a second touch instead).
                if let Err(e) =
                    reindex_remove(&self.pager, &mut tx, &idx_specs, &mut roots, existing, loc)
                {
                    self.pager.abort_tx(tx)?;
                    return Err(e);
                }
                let out = match heap.replace(&self.pager, &mut tx, loc, &merged) {
                    Ok(o) => o,
                    Err(e) => {
                        self.pager.abort_tx(tx)?;
                        return Err(e.into());
                    }
                };
                for (old_l, new_l) in &out.moved {
                    if *old_l == loc {
                        continue; // this row's own slot: entries removed above
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
                    &merged,
                    out.placed,
                ) {
                    self.pager.abort_tx(tx)?;
                    return Err(e);
                }
                if out.placed != loc {
                    let origin = origin_of.get(&loc).copied().unwrap_or(loc);
                    origin_of.insert(out.placed, origin);
                }
                let collapse_key = origin_of.get(&loc).copied().unwrap_or(loc);
                updated.push((collapse_key, existing.clone(), merged.clone()));
                ret_docs.push(merged);
                // An upsert that updated in place fires the UPDATE triggers
                // (SQLite/PostgreSQL semantics), not the INSERT ones.
                let changed = du_changed.as_deref();
                let fired_new = ret_docs.last().expect("just pushed");
                self.fire_row_triggers(
                    &table,
                    true,
                    TriggerFireEvent::Update(changed),
                    Some(existing),
                    Some(fired_new),
                )?;
                self.fire_row_triggers(
                    &table,
                    false,
                    TriggerFireEvent::Update(changed),
                    Some(existing),
                    Some(fired_new),
                )?;
                continue 'outer;
            }
            let loc = match heap.insert(&self.pager, &mut tx, &doc) {
                Ok(l) => l,
                Err(e) => {
                    self.pager.abort_tx(tx)?;
                    return Err(e.into());
                }
            };
            for col in &indexed {
                let Some(v) = doc.get(col) else { continue };
                if matches!(v, Value::Null) {
                    continue;
                }
                let root = roots[col];
                let mut tree = BTree::open(root);
                if let Err(e) = tree.insert(&self.pager, &mut tx, v.clone(), loc, true) {
                    insert_failed = Some(index_err(col, e));
                    break 'outer;
                }
                if tree.root != root {
                    roots.insert(col.clone(), tree.root);
                }
            }
            // Non-constraint CREATE INDEX trees. Composite unique trees
            // (CREATE UNIQUE INDEX over several columns) enforce their
            // duplicates right here, at tree-insert time.
            for spec in &idx_specs {
                if indexed.contains(&spec.root_key) {
                    continue;
                }
                // Partial index: only qualifying rows enter the tree.
                if let Some(p) = &spec.pred {
                    if !matches!(eval_expr(p, &doc)?, Value::Bool(true)) {
                        continue;
                    }
                }
                let Some(v) = index_key_of_spec(&doc, spec) else {
                    continue;
                };
                let root = roots[&spec.root_key];
                let mut tree = BTree::open(root);
                tree.insert(&self.pager, &mut tx, v, loc, spec.unique)
                    .map_err(|e| index_err(&spec.root_key, e))?;
                if tree.root != root {
                    roots.insert(spec.root_key.clone(), tree.root);
                }
            }
            if insert.returning.is_some() {
                ret_docs.push(doc.clone());
            }
            placed.push((loc, doc));
            // AFTER INSERT: the row (and its tree entries) is in place.
            self.fire_row_triggers(
                &table,
                false,
                TriggerFireEvent::Insert,
                None,
                Some(&placed.last().expect("just pushed").1),
            )?;
        }
        if let Some(e) = insert_failed {
            self.pager.abort_tx(tx)?;
            return Err(e);
        }
        // Child-side FK over the FINAL row set. ON CONFLICT DO NOTHING /
        // OR IGNORE filter rows out after the batch was built — validating
        // against the pre-filter batch let a skipped parent candidate
        // legitimize a child that lands alone (dangling reference).
        // Self-references resolve against the placed batch itself;
        // statement-internal references count, including forward ones.
        // Upsert-UPDATE rows join the final set: their new images must
        // reference existing parents like any inserted row.
        // A multi-row batch may touch the SAME row twice (sequential
        // upsert semantics: later rows can route onto an earlier row
        // through either unique key). Every touch pushed (old, merged),
        // but only the FIRST old image and the LAST merged image of a
        // locator are real — intermediate images were overwritten and do
        // not exist in the final table. Feeding them into the FK/unique
        // checks below could validate (or reject) against keys no
        // surviving row carries, e.g. a parent key vacated by touch one
        // and re-supply-shaped by touch two's intermediate image.
        if updated.len() > 1 {
            let mut collapsed: std::collections::BTreeMap<u64, (usize, usize)> =
                std::collections::BTreeMap::new();
            for (i, (loc, _, _)) in updated.iter().enumerate() {
                match collapsed.get_mut(loc) {
                    Some((_, last)) => *last = i,
                    None => {
                        collapsed.insert(*loc, (i, i));
                    }
                }
            }
            if collapsed.len() < updated.len() {
                updated = collapsed
                    .into_iter()
                    .map(|(loc, (first, last))| {
                        (loc, updated[first].1.clone(), updated[last].2.clone())
                    })
                    .collect();
            }
        }
        if !meta.foreign_keys.is_empty() && (!placed.is_empty() || !updated.is_empty()) {
            let mut final_docs: Vec<Object> = placed.iter().map(|(_, d)| d.clone()).collect();
            final_docs.extend(updated.iter().map(|(_, _, n)| n.clone()));
            // Self-referential FKs need the statement's COMPLETE final state
            // (pre-state minus the replaced old images plus these images):
            // the pre-state heap still carries keys an upsert-UPDATE vacates
            // (`VALUES (1,1) ON CONFLICT (id) DO UPDATE SET id = 3, p = 1`).
            // Plain INSERT keeps `None`: nothing is replaced, so same_batch
            // plus the pre-state heap is already the final state.
            let self_final: Option<Vec<Object>> =
                if !updated.is_empty() && meta.foreign_keys.iter().any(|(_, rt, _)| rt == &table) {
                    let old_keys: std::collections::BTreeSet<Vec<u8>> =
                        updated.iter().map(|(_, o, _)| object_key(o)).collect();
                    let mut final_state = self.table_docs_cx(&table)?;
                    final_state.retain(|d| !old_keys.contains(&object_key(d)));
                    final_state.extend(final_docs.iter().cloned());
                    Some(final_state)
                } else {
                    None
                };
            for doc in &final_docs {
                // A big INSERT ... SELECT walks this per placed row: keep the
                // statement timeout armed like every other row loop.
                self.stmt_deadline.check()?;
                if let Err(e) =
                    self.check_fks_in(&table, &meta, doc, &final_docs, self_final.as_deref())
                {
                    self.pager.abort_tx(tx)?;
                    return Err(e);
                }
            }
        }
        // Parent-side FK for upsert-updated rows: the old image's key
        // values disappear exactly like a DELETE's and must not be
        // referenced by children (new images already checked above). This
        // runs OUTSIDE the target's own-FK gate — the referencing children
        // belong to OTHER tables, which do not care whether the target
        // declares FKs of its own.
        if !updated.is_empty() {
            let has_self_fk = meta.foreign_keys.iter().any(|(_, rt, _)| rt == &table);
            let self_final: Option<Vec<Object>> = if has_self_fk {
                let old_keys: std::collections::BTreeSet<Vec<u8>> =
                    updated.iter().map(|(_, o, _)| object_key(o)).collect();
                let mut final_state = self.table_docs_cx(&table)?;
                final_state.retain(|d| !old_keys.contains(&object_key(d)));
                final_state.extend(updated.iter().map(|(_, _, n)| n.clone()));
                final_state.extend(placed.iter().map(|(_, d)| d.clone()));
                Some(final_state)
            } else {
                None
            };
            let olds: Vec<Object> = updated.iter().map(|(_, o, _)| o.clone()).collect();
            // `news` is the "keys that still exist after this statement"
            // set: rows PLACED by the same batch can re-supply a key the
            // updated rows vacated (INSERT INTO p VALUES (1),(1) ON CONFLICT
            // (id) DO UPDATE SET id = 99 — the second (1) no longer conflicts
            // and lands as a plain insert). Counting only the updated images
            // false-errored children referencing a key that is still present
            // through the placed row.
            let mut news: Vec<Object> = updated.iter().map(|(_, _, n)| n.clone()).collect();
            news.extend(placed.iter().map(|(_, d)| d.clone()));
            if let Err(e) = self.check_fk_parent_delete_skipping(
                &table,
                &olds,
                &news,
                &[],
                self_final.as_deref(),
            ) {
                self.pager.abort_tx(tx)?;
                return Err(e);
            }
        }
        // Legacy files whose constraint columns predate trees keep the
        // whole-set duplicate check; tables with no constraints skip it.
        // ANY untreed constraint column triggers it (same gate as the
        // UPDATE fast path): an all-or-nothing gate used to skip the check
        // whenever a sibling column had a tree, silently admitting
        // duplicates on the treeless one.
        let has_constraints = meta.primary_key.is_some() || !meta.unique.is_empty();
        let all_treed = meta
            .primary_key
            .iter()
            .chain(meta.unique.iter())
            .all(|c| indexed.contains(c));
        if has_constraints && !all_treed && (!placed.is_empty() || !updated.is_empty()) {
            // A failed scan must abort, not silently skip the unique check.
            let mut combined = match (Heap {
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
            if !updated.is_empty() {
                // The scan sees the PRE-statement table: updated rows carry
                // their old images here. Swap them for the new images
                // (encoding-compare removes exactly the one old row — its
                // unique-key values are distinct per row, so byte-identical
                // duplicates of an old image cannot exist).
                let olds: std::collections::HashSet<Vec<u8>> = updated
                    .iter()
                    .filter_map(|(_, o, _)| encode::encode_to_vec(&Value::Object(o.clone())).ok())
                    .collect();
                combined.retain(|d| {
                    !encode::encode_to_vec(&Value::Object(d.clone()))
                        .map(|k| olds.contains(&k))
                        .unwrap_or(false)
                });
                combined.extend(updated.iter().map(|(_, _, n)| n.clone()));
            }
            if !displaced_docs.is_empty() {
                // The no-tx scan also still sees OR REPLACE's displaced rows
                // (their deletions are staged in THIS tx): keep them and the
                // displaced old image pairs with its own replacement, making
                // check_unique false-error on the very keys REPLACE displaced.
                // Same encoding-compare argument as `updated`: byte-identical
                // duplicates of a displaced image cannot coexist in a table
                // that has constraints at all.
                let gone: std::collections::HashSet<Vec<u8>> = displaced_docs
                    .iter()
                    .filter_map(|d| encode::encode_to_vec(&Value::Object(d.clone())).ok())
                    .collect();
                combined.retain(|d| {
                    !encode::encode_to_vec(&Value::Object(d.clone()))
                        .map(|k| gone.contains(&k))
                        .unwrap_or(false)
                });
            }
            combined.extend(placed.iter().map(|(_, d)| d.clone()));
            if let Err(e) = meta.check_unique(&combined) {
                self.pager.abort_tx(tx)?;
                return Err(e);
            }
        }
        // Affected = inserted + upsert-updated rows (PostgreSQL reports the
        // union the same way).
        let count = (placed.len() + updated.len()) as u64;
        // Emptied heap pages are released inside this transaction so later
        // statements reuse them instead of growing the file.
        for p in std::mem::take(&mut heap.dropped) {
            self.pager.free_page(&mut tx, p)?;
        }
        let dotted_now = self.note_dotted_fields(
            &table,
            placed
                .iter()
                .map(|(_, d)| d)
                .chain(updated.iter().map(|(_, _, new)| new)),
        );
        if dotted_now
            || heap.pages != meta.pages
            || roots != meta.index_roots
            || heap.overflow_free != meta.overflow_free
        {
            self.sync_table_layout(
                &mut tx,
                &table,
                heap.pages.clone(),
                roots,
                heap.overflow_free.clone(),
            )?;
        }
        self.commit_pager_tx(tx)?;
        // AUTOINCREMENT counter: never regress, follow explicit max.
        if let Some(col) = &meta.autoinc {
            // The upsert-UPDATE arm can raise the live max past every placed
            // id (SET id = <higher>) — invalidate like the UPDATE fast path,
            // or the stale cache keeps issuing already-taken ids and every
            // subsequent autoincrement INSERT fails on the unique tree.
            let du_touched = do_update
                && du_assignments.iter().any(|a| {
                    matches!(&a.target, sqlparser::ast::AssignmentTarget::ColumnName(c) if obj_name(c) == *col)
                });
            if du_touched {
                self.autoinc_cache.remove(&table);
            } else {
                let used = placed
                    .iter()
                    .filter_map(|(_, d)| match d.get(col) {
                        Some(Value::Int(i)) => Some((*i).saturating_add(1)),
                        _ => None,
                    })
                    .max()
                    .unwrap_or(next_autoinc)
                    .max(next_autoinc);
                self.autoinc_cache.insert(table.clone(), used);
            }
            // Max id this statement actually placed (explicit values count;
            // an INSERT that placed no rows leaves the previous value).
            self.last_insert_id = placed
                .iter()
                .filter_map(|(_, d)| d.get(col))
                .filter_map(|v| v.as_i64())
                .max();
        }
        if let Some(ret) = &insert.returning {
            return project_returning(ret, &ret_docs);
        }
        Ok(ExecOutcome::Affected(count))
    }

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
            for (col, rtable, rcol) in &meta.foreign_keys {
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
