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
        // Referential actions need the generic path (cascaded re-keys or
        // set-null writes on child tables).
        if from.is_none() && !self.referential_actions_reach(&tname) {
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
        // Referential actions (any transitively reachable FK declaring
        // ON DELETE/ON UPDATE CASCADE / SET NULL / SET DEFAULT): the plain
        // checks are replaced by the full multi-table plan — cascaded
        // re-keys / set-null writes on child tables, restrict FKs still
        // reject — and every table applies through the same rewrite
        // machinery.
        if self.referential_actions_reach(&tname) {
            return self.exec_update_referential(
                &tname,
                old_changed,
                changed_docs,
                &update_returning,
            );
        }
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
            if meta.foreign_keys.iter().any(|fk| fk.ref_table == tname) {
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
        // whole table. Referential actions need the generic path: the
        // statement may rewrite OTHER tables (cascade/set-null), which the
        // in-place fast path cannot do.
        if using.is_none() && !self.referential_actions_reach(&tname) {
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
        // Referential actions (any transitively reachable FK declaring
        // ON DELETE/ON UPDATE CASCADE / SET NULL / SET DEFAULT): the plain
        // restrict check is replaced by the full multi-table plan — actions
        // delete/update the referencing child rows, restrict FKs still
        // reject, and everything applies through the same rewrite machinery.
        if self.referential_actions_reach(&tname) {
            return self.exec_delete_referential(&tname, removed, &returning);
        }
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
            if meta.foreign_keys.iter().any(|fk| fk.ref_table == tname) {
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
}
