//! Referential actions (FOREIGN KEY ON DELETE/ON UPDATE CASCADE / SET NULL /
//! SET DEFAULT): the expansion engine that turns one DELETE/UPDATE on a
//! parent table into a validated, deterministic multi-table plan.
//!
//! Determinism contract: every node replays the same statement text over the
//! same pre-statement data, so the plan (which rows get deleted or updated,
//! with which values) must be a pure function of that data. That rules out
//! wall-clock/random DEFAULT evaluation under `SET DEFAULT` (loud error), and
//! it is why the apply order is a BFS from the origin with each depth in
//! table-name order.

use super::*;

/// Cap on referential-closure fixpoint passes. The staged effect set only
/// grows while the candidate row sets only shrink, so a finite database
/// always converges; the cap turns a hypothetical logic bug into a loud
/// error instead of a hung single writer.
const MAX_FK_ACTION_PASSES: usize = 32;

/// One affected table's per-statement delta: rows the statement deletes
/// (pre-images, first-touch order) and (old, new) image pairs it updates.
/// Row identity is the full-document encoding ([`object_key`]).
#[derive(Default)]
struct ReferentialDelta {
    deletes: Vec<Object>,
    delete_keys: std::collections::BTreeSet<Vec<u8>>,
    updates: Vec<(Object, Object)>,
    update_keys: std::collections::BTreeSet<Vec<u8>>,
}

impl ReferentialDelta {
    fn new() -> Self {
        Self {
            deletes: Vec::new(),
            delete_keys: std::collections::BTreeSet::new(),
            updates: Vec::new(),
            update_keys: std::collections::BTreeSet::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.deletes.is_empty() && self.updates.is_empty()
    }

    /// Remove a row. Deletes win over any update already staged for the
    /// same row (two referencing FKs firing on one child row resolve to the
    /// stronger effect; PostgreSQL resolves the ambiguity the same way).
    fn stage_delete(&mut self, doc: &Object) {
        let key = object_key(doc);
        if !self.delete_keys.insert(key.clone()) {
            return;
        }
        self.deletes.push(doc.clone());
        if self.update_keys.contains(&key) {
            self.updates.retain(|(old, _)| object_key(old) != key);
            self.update_keys.remove(&key);
        }
    }

    /// Stage an update of `col` to `val` on the row whose pre-image key is
    /// `key`. A row already staged for update composes onto its new image;
    /// a column two sources claim with different values is a loud conflict
    /// (this also covers a statement's own SET racing a cascaded write on
    /// the same row — the statement's explicit SET never gets silently
    /// overwritten). Returns whether anything changed.
    fn stage_update(&mut self, doc: &Object, col: &str, val: Value) -> Result<bool> {
        let key = object_key(doc);
        if self.delete_keys.contains(&key) {
            return Ok(false); // delete wins
        }
        if let Some(pos) = self
            .updates
            .iter()
            .position(|(old, _)| object_key(old) == key)
        {
            let pair = &mut self.updates[pos];
            let cur = pair.1.get(col).cloned().unwrap_or(Value::Null);
            if Value::cmp_values(&cur, &val) == Ordering::Equal {
                return Ok(false);
            }
            let originally = pair.0.get(col).cloned().unwrap_or(Value::Null);
            if Value::cmp_values(&cur, &originally) != Ordering::Equal {
                return err(format!(
                    "conflicting referential actions on column {col}: the statement and a \
                     cascaded action both assign it"
                ));
            }
            pair.1.insert(col.to_string(), val);
            return Ok(true);
        }
        let mut new_doc = doc.clone();
        new_doc.insert(col.to_string(), val);
        self.update_keys.insert(key);
        self.updates.push((doc.clone(), new_doc.clone()));
        Ok(true)
    }

    /// Final images: pre-state minus deletes, updates applied by row key.
    fn final_docs(&self, pre: &[Object]) -> Vec<Object> {
        pre.iter()
            .filter(|d| !self.delete_keys.contains(&object_key(d)))
            .map(|d| match self.update_keys.contains(&object_key(d)) {
                true => self
                    .updates
                    .iter()
                    .find(|(old, _)| object_key(old) == object_key(d))
                    .map(|(_, n)| n.clone())
                    .unwrap_or_else(|| d.clone()),
                false => d.clone(),
            })
            .collect()
    }
}

/// The completed referential-actions plan: per affected table, the delta and
/// the full final doc set (the rewrite input), plus the deterministic apply
/// order (origin first, then BFS closure in name order, tables with nothing
/// staged dropped).
struct ReferentialPlan {
    order: Vec<String>,
    deltas: std::collections::BTreeMap<String, ReferentialDelta>,
    finals: std::collections::BTreeMap<String, Vec<Object>>,
}

impl ReferentialPlan {
    /// Per-table trigger inputs: delete pre-images and update pairs.
    fn delta(&self, table: &str) -> Option<&ReferentialDelta> {
        self.deltas.get(table)
    }
}

impl Database {
    /// Transitive set of tables a statement writing `origin` reaches through
    /// FOREIGN KEYs that declare non-default ON DELETE/ON UPDATE actions:
    /// child tables pulled into the statement's rewrite (and THEIR children,
    /// transitively). Restrict-only children never appear — they are only
    /// validated, never rewritten. A self-referential action FK on `origin`
    /// pulls the origin itself into the set (it rewrites its own rows).
    /// BFS from `origin`, each depth in table name order: the deterministic
    /// apply order on every replaying node.
    pub(super) fn referential_closure_of(&self, origin: &str) -> Vec<String> {
        let mut order: Vec<String> = Vec::new();
        let mut seen: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        let mut queue: std::collections::VecDeque<String> = [origin.to_string()].into();
        while let Some(parent) = queue.pop_front() {
            for (child_name, m) in &self.tables {
                if seen.contains(child_name)
                    || !m
                        .foreign_keys
                        .iter()
                        .any(|fk| fk.ref_table == parent && fk.has_action())
                {
                    continue;
                }
                seen.insert(child_name.clone());
                order.push(child_name.clone());
                queue.push_back(child_name.clone());
            }
        }
        order
    }

    /// True when a statement writing `origin` must expand referential
    /// actions (any transitively reachable action FK). Cheap gate for the
    /// fast paths and the statement guard.
    pub(super) fn referential_actions_reach(&self, origin: &str) -> bool {
        !self.referential_closure_of(origin).is_empty()
    }

    /// Referential default of a child column for `SET DEFAULT`: the declared
    /// DEFAULT evaluated once, or NULL when none (PostgreSQL semantics).
    /// Wall-clock / random defaults are refused: the cascaded value must be
    /// identical on every replaying node, and only a deterministic
    /// evaluation can promise that.
    fn referential_default_value(&self, child_meta: &TableMeta, col: &str) -> Result<Value> {
        let Some((_, text)) = child_meta.defaults.iter().find(|(c, _)| c == col) else {
            return Ok(Value::Null);
        };
        if crate::stmt::mentions_wall_clock(text) || crate::stmt::mentions_nondet_call(text) {
            return err(format!(
                "SET DEFAULT cannot evaluate a wall-clock or random DEFAULT on column \
                 {col}; declare a deterministic default for the column"
            ));
        }
        eval_const(&parse_expr_text(text)?)
    }

    /// Compute the full referential-actions plan for a statement on
    /// `origin` that deletes `base_deletes` rows and updates
    /// `base_updates` (old → new image pairs). Expands CASCADE / SET NULL /
    /// SET DEFAULT actions transitively, then validates every staged
    /// effect (CHECK/NOT NULL, child-side FK against FINAL parent state,
    /// table-wide uniqueness) BEFORE anything is applied.
    ///
    /// The origin's own final doc set derives from the base delta exactly
    /// like the caller's rewrite input (pre-state minus deletes with the
    /// statement's update pairs applied) — the caller must not diverge from
    /// it, and self-FK actions compose onto it here.
    fn plan_referential_actions(
        &mut self,
        origin: &str,
        base_deletes: Vec<Object>,
        base_updates: Vec<(Object, Object)>,
    ) -> Result<ReferentialPlan> {
        // Pre-statement docs for every table the plan may read: the origin
        // and its action-closure up front, restrict-only children and
        // out-of-plan parents discovered lazily below (cached here).
        let closure = self.referential_closure_of(origin);
        let mut pre_state: std::collections::BTreeMap<String, Vec<Object>> =
            std::collections::BTreeMap::new();
        for t in std::iter::once(origin).chain(closure.iter().map(String::as_str)) {
            if !pre_state.contains_key(t) {
                pre_state.insert(t.to_string(), self.table_docs_cx(t)?);
            }
        }

        let mut deltas: std::collections::BTreeMap<String, ReferentialDelta> =
            std::collections::BTreeMap::new();
        let mut origin_delta = ReferentialDelta::new();
        for d in &base_deletes {
            origin_delta.stage_delete(d);
        }
        for (old, new) in &base_updates {
            origin_delta.update_keys.insert(object_key(old));
            origin_delta.updates.push((old.clone(), new.clone()));
        }
        deltas.insert(origin.to_string(), origin_delta);

        // Fixpoint: delete/update events on any affected table trigger the
        // referencing FKs' actions. One pass walks every affected parent and
        // every referencing child; a staged effect can unlock further events
        // on that child, so the loop re-walks until stable.
        let mut converged = false;
        for _ in 0..MAX_FK_ACTION_PASSES {
            let mut changed = false;
            let parents: Vec<String> = deltas.keys().cloned().collect();
            'parents: for parent in parents {
                // Owned copies up front: the parent's delta borrow must be
                // released before the child loop takes `deltas.entry` for
                // the child tables.
                let (parent_deletes, parent_updates): (Vec<Object>, Vec<(Object, Object)>) =
                    match deltas.get(&parent) {
                        Some(d) => (d.deletes.clone(), d.updates.clone()),
                        None => (Vec::new(), Vec::new()),
                    };
                let parent_final: Vec<Object> = {
                    let d = &deltas[&parent];
                    d.final_docs(pre_state.get(&parent).map(Vec::as_slice).unwrap_or(&[]))
                };
                for (child_name, child_arc) in &self.tables {
                    // Self-FKs land here naturally: the child table IS the
                    // parent, and its (col, ref_col) pair acts on its own
                    // rows. Multiple FKs from one child each get their turn.
                    let fk_refs = child_arc
                        .foreign_keys
                        .iter()
                        .filter(|fk| fk.ref_table == parent)
                        .cloned()
                        .collect::<Vec<_>>();
                    if fk_refs.is_empty() {
                        continue;
                    }
                    let child_meta = child_arc.as_ref().clone();
                    let child_pre: Vec<Object> = match pre_state.get(child_name) {
                        Some(v) => v.clone(),
                        None => self.table_docs_cx(child_name)?,
                    };
                    for fk in fk_refs {
                        let ccol = fk.col.clone();
                        // DELETE events for THIS FK: referenced-column values
                        // that vanish with the parent's staged deletes and
                        // survive in no final parent row. Update events: the
                        // (old → new) map over the referenced column, which
                        // only acts when no surviving parent row keeps the
                        // old value. Both computations read the parent's
                        // staged delta — FK declarations live on the child
                        // tables, so the events are derived per referencing
                        // FK, not from the parent's own FK list.
                        let rc = fk.ref_col.as_str();
                        let mut lost: Vec<Value> = Vec::new();
                        for d in &parent_deletes {
                            if let Some(v) = d.get(rc) {
                                if matches!(v, Value::Null) {
                                    continue;
                                }
                                let survives = parent_final.iter().any(|f| {
                                    f.get(rc).is_some_and(|fv| {
                                        Value::cmp_values(fv, v) == Ordering::Equal
                                    })
                                });
                                if !survives
                                    && !lost
                                        .iter()
                                        .any(|l| Value::cmp_values(l, v) == Ordering::Equal)
                                {
                                    lost.push(v.clone());
                                }
                            }
                        }
                        let mut map: Vec<(Value, Value)> = Vec::new();
                        for (old, new) in &parent_updates {
                            let (Some(ov), Some(nv)) = (old.get(rc), new.get(rc)) else {
                                continue;
                            };
                            if Value::cmp_values(ov, nv) == Ordering::Equal
                                || matches!(ov, Value::Null)
                            {
                                continue;
                            }
                            match map
                                .iter_mut()
                                .find(|(o, _)| Value::cmp_values(o, ov) == Ordering::Equal)
                            {
                                Some((_, prev))
                                    if Value::cmp_values(prev, nv) != Ordering::Equal =>
                                {
                                    return err(format!(
                                        "cannot cascade FOREIGN KEY update: parent key value \
                                         in {parent}.{rc} changes to multiple values in one \
                                         statement"
                                    ));
                                }
                                Some(_) => {}
                                None => map.push((ov.clone(), nv.clone())),
                            }
                        }
                        if lost.is_empty() && map.is_empty() {
                            continue;
                        }
                        let child_delta = deltas.entry(child_name.clone()).or_default();
                        // DELETE events first (deletes win over updates).
                        if !lost.is_empty() {
                            let hits: Vec<Object> = child_pre
                                .iter()
                                .filter(|d| {
                                    d.get(&ccol).is_some_and(|cv| {
                                        !matches!(cv, Value::Null)
                                            && lost.iter().any(|lv| {
                                                Value::cmp_values(cv, lv) == Ordering::Equal
                                            })
                                    })
                                })
                                .cloned()
                                .collect();
                            match fk.on_delete {
                                FkAction::None => {
                                    // Restrict: surviving child rows may not
                                    // reference a lost parent value. Surviving
                                    // = the child's FINAL rows (rows this same
                                    // statement deletes are exempt — deleting
                                    // parent and child together is legal).
                                    let final_rows = child_delta.final_docs(&child_pre);
                                    let offender = final_rows.iter().any(|d| {
                                        d.get(&ccol).is_some_and(|cv| {
                                            !matches!(cv, Value::Null)
                                                && lost.iter().any(|lv| {
                                                    Value::cmp_values(cv, lv) == Ordering::Equal
                                                })
                                        })
                                    });
                                    if offender {
                                        return err(format!(
                                            "FOREIGN KEY constraint failed: {child_name}.{ccol} \
                                             references {parent}.{rc}; delete/update the child \
                                             rows first"
                                        ));
                                    }
                                }
                                FkAction::Cascade => {
                                    for d in hits {
                                        self.stmt_deadline.check()?;
                                        if !child_delta.delete_keys.contains(&object_key(&d)) {
                                            child_delta.stage_delete(&d);
                                            changed = true;
                                        }
                                        if changed {
                                            // A staged effect can unlock further
                                            // events on this child; re-walk from
                                            // the top.
                                            break 'parents;
                                        }
                                    }
                                }
                                FkAction::SetNull | FkAction::SetDefault => {
                                    let val = match fk.on_delete {
                                        FkAction::SetDefault => {
                                            self.referential_default_value(&child_meta, &ccol)?
                                        }
                                        _ => Value::Null,
                                    };
                                    for d in hits {
                                        self.stmt_deadline.check()?;
                                        if child_delta.stage_update(&d, &ccol, val.clone())? {
                                            changed = true;
                                            break 'parents;
                                        }
                                    }
                                }
                            }
                        }
                        // UPDATE events: value maps over the referenced column.
                        if !map.is_empty() {
                            let survives = |v: &Value| {
                                parent_final.iter().any(|f| {
                                    f.get(rc).is_some_and(|fv| {
                                        Value::cmp_values(fv, v) == Ordering::Equal
                                    })
                                })
                            };
                            let lost_old: Vec<&(Value, Value)> =
                                map.iter().filter(|(o, _)| !survives(o)).collect();
                            match fk.on_update {
                                FkAction::None => {
                                    // Restrict: only SURVIVING child rows
                                    // offend (rows this same statement deletes
                                    // are exempt).
                                    let final_rows = child_delta.final_docs(&child_pre);
                                    let offender = final_rows.iter().any(|d| {
                                        d.get(&ccol).is_some_and(|cv| {
                                            !matches!(cv, Value::Null)
                                                && lost_old.iter().any(|(o, _)| {
                                                    Value::cmp_values(cv, o) == Ordering::Equal
                                                })
                                        })
                                    });
                                    if offender {
                                        return err(format!(
                                            "FOREIGN KEY constraint failed: {child_name}.{ccol} \
                                             references {parent}.{rc}; delete/update the child \
                                             rows first"
                                        ));
                                    }
                                }
                                FkAction::Cascade => {
                                    for d in child_pre.iter() {
                                        let Some(cv) = d.get(&ccol) else {
                                            continue;
                                        };
                                        if matches!(cv, Value::Null) {
                                            continue;
                                        }
                                        let Some((_, nv)) = lost_old.iter().find(|(o, _)| {
                                            Value::cmp_values(cv, o) == Ordering::Equal
                                        }) else {
                                            continue;
                                        };
                                        self.stmt_deadline.check()?;
                                        if child_delta.stage_update(d, &ccol, nv.clone())? {
                                            changed = true;
                                            break 'parents;
                                        }
                                    }
                                }
                                FkAction::SetNull | FkAction::SetDefault => {
                                    let val = match fk.on_update {
                                        FkAction::SetDefault => {
                                            self.referential_default_value(&child_meta, &ccol)?
                                        }
                                        _ => Value::Null,
                                    };
                                    for d in child_pre.iter() {
                                        let Some(cv) = d.get(&ccol) else {
                                            continue;
                                        };
                                        if matches!(cv, Value::Null) {
                                            continue;
                                        }
                                        let hit = lost_old.iter().any(|(o, _)| {
                                            Value::cmp_values(cv, o) == Ordering::Equal
                                        });
                                        if !hit {
                                            continue;
                                        }
                                        self.stmt_deadline.check()?;
                                        if child_delta.stage_update(d, &ccol, val.clone())? {
                                            changed = true;
                                            break 'parents;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if !changed {
                converged = true;
                break;
            }
        }

        if !converged {
            return err("referential cascade did not converge (over 32 passes)");
        }
        // ---- validations over the staged plan (before ANY apply) ----
        for (table, delta) in &deltas {
            if delta.updates.is_empty() {
                continue;
            }
            let meta = match self.tables.get(table) {
                Some(m) => m.as_ref().clone(),
                None => continue,
            };
            for (_, new) in &delta.updates {
                meta.check(new)?;
                // Child-side FK of an updated row: parents must exist in the
                // FINAL plan state — a SET DEFAULT pointing at a parent row
                // this same statement deletes must fail now, not on the next
                // statement (the pre-state heap would still show that row).
                for fk in &meta.foreign_keys {
                    let Some(v) = new.get(&fk.col) else {
                        continue;
                    };
                    if matches!(v, Value::Null) {
                        continue;
                    }
                    let parent_final: Vec<Object> = match deltas.get(&fk.ref_table) {
                        Some(pd) => pd.final_docs(
                            pre_state
                                .get(&fk.ref_table)
                                .map(Vec::as_slice)
                                .unwrap_or(&[]),
                        ),
                        None => match pre_state.get(&fk.ref_table) {
                            Some(docs) => docs.clone(),
                            None => {
                                let docs = self.table_docs_cx(&fk.ref_table)?;
                                pre_state.insert(fk.ref_table.clone(), docs.clone());
                                docs
                            }
                        },
                    };
                    let ok = parent_final.iter().any(|p| {
                        p.get(&fk.ref_col)
                            .is_some_and(|pv| Value::cmp_values(pv, v) == Ordering::Equal)
                    });
                    if !ok {
                        return err(format!(
                            "FOREIGN KEY constraint failed: {table}.{} references {}.{}",
                            fk.col, fk.ref_table, fk.ref_col
                        ));
                    }
                }
            }
            // Table-wide uniqueness over the final state (the generic UPDATE
            // path's whole-image check); check_unique skips NULLs, so SET
            // NULL on a unique column stays legal.
            let final_docs =
                delta.final_docs(pre_state.get(table).map(Vec::as_slice).unwrap_or(&[]));
            meta.check_unique(&final_docs)?;
        }
        // Apply order: origin first, then the BFS closure (the origin can
        // appear in it too — a self-referential action FK rewrites its own
        // rows); tables with nothing staged drop out.
        let mut order = vec![origin.to_string()];
        order.extend(closure.into_iter().filter(|c| c.as_str() != origin));
        order.retain(|t| deltas.get(t).map(|d| !d.is_empty()).unwrap_or(t == origin));
        let mut finals = std::collections::BTreeMap::new();
        for t in &order {
            let delta = &deltas[t];
            finals.insert(t.clone(), delta.final_docs(pre_state[t].as_slice()));
        }
        Ok(ReferentialPlan {
            order,
            deltas,
            finals,
        })
    }

    /// Apply a referential plan: per affected table in order, fire BEFORE
    /// row triggers over the staged pre-images, rewrite the table to its
    /// final doc set, fire AFTER triggers. Each rewrite is its own pager
    /// transaction; statement atomicity is the statement guard's job (undo
    /// journal + catalog snapshot cover every sub-transaction).
    ///
    /// While applying, [`Database::referential_guard`] names every affected
    /// table so trigger bodies cannot write any of them mid-flight (their
    /// deltas are already computed; a mid-flight write would be silently
    /// clobbered by the rewrite).
    fn apply_referential_plan(&mut self, plan: &ReferentialPlan) -> Result<()> {
        self.referential_guard = Some(plan.order.iter().cloned().collect());
        let r = self.apply_referential_plan_inner(plan);
        self.referential_guard = None;
        r
    }

    fn apply_referential_plan_inner(&mut self, plan: &ReferentialPlan) -> Result<()> {
        for table in &plan.order {
            let Some(delta) = plan.delta(table) else {
                continue;
            };
            let Some(final_docs) = plan.finals.get(table).cloned() else {
                continue;
            };
            if delta.is_empty() {
                continue;
            }
            let mut meta = match self.tables.get(table) {
                // Sticky dotted-field marker (red line: rewrite_table
                // persists exactly this meta clone).
                Some(m) => {
                    let mut m = m.as_ref().clone();
                    if !m.dotted_keys
                        && final_docs.iter().any(|d| d.keys().any(|k| k.contains('.')))
                    {
                        m.dotted_keys = true;
                    }
                    m
                }
                None => continue,
            };
            let fire = self.triggers_exist_for(table);
            let mut per_pair_cols: Vec<Vec<String>> = Vec::new();
            if fire {
                for doc in &delta.deletes {
                    self.fire_row_triggers(table, true, TriggerFireEvent::Delete, Some(doc), None)?;
                }
                for (old, new) in &delta.updates {
                    // UPDATE OF gate columns: what actually changed.
                    let changed_cols: Vec<String> = meta
                        .columns
                        .iter()
                        .filter(|c| match (old.get(c.as_str()), new.get(c.as_str())) {
                            (Some(a), Some(b)) => Value::cmp_values(a, b) != Ordering::Equal,
                            (None, None) => false,
                            _ => true,
                        })
                        .cloned()
                        .collect();
                    per_pair_cols.push(changed_cols);
                    self.fire_row_triggers(
                        table,
                        true,
                        TriggerFireEvent::Update(Some(per_pair_cols.last().expect("just pushed"))),
                        Some(old),
                        Some(new),
                    )?;
                }
            }
            self.rewrite_table(table, &mut meta, final_docs)?;
            if fire {
                for doc in &delta.deletes {
                    self.fire_row_triggers(
                        table,
                        false,
                        TriggerFireEvent::Delete,
                        Some(doc),
                        None,
                    )?;
                }
                for ((old, new), cols) in delta.updates.iter().zip(per_pair_cols.iter()) {
                    self.fire_row_triggers(
                        table,
                        false,
                        TriggerFireEvent::Update(Some(cols)),
                        Some(old),
                        Some(new),
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Execute a DELETE on `origin` with referential actions: build the
    /// plan from the statement's own removed rows, apply per table, keep
    /// RETURNING scoped to the origin's own rows.
    pub(super) fn exec_delete_referential(
        &mut self,
        tname: &str,
        removed: Vec<Object>,
        returning: &Option<Vec<SelectItem>>,
    ) -> Result<ExecOutcome> {
        let plan = self.plan_referential_actions(tname, removed.clone(), Vec::new())?;
        let count = plan
            .delta(tname)
            .map(|d| d.deletes.len() as u64)
            .unwrap_or(0);
        self.apply_referential_plan(&plan)?;
        if let Some(ret) = returning {
            return project_returning(ret, &removed);
        }
        Ok(ExecOutcome::Affected(count))
    }

    /// Execute an UPDATE on `origin` with referential actions: build the
    /// plan from the statement's own (old, new) image pairs, apply per
    /// table, keep RETURNING scoped to the origin's own new images.
    pub(super) fn exec_update_referential(
        &mut self,
        tname: &str,
        old_changed: Vec<Object>,
        changed_docs: Vec<Object>,
        update_returning: &Option<Vec<SelectItem>>,
    ) -> Result<ExecOutcome> {
        let pairs: Vec<(Object, Object)> = old_changed
            .into_iter()
            .zip(changed_docs.iter().cloned())
            .collect();
        let plan = self.plan_referential_actions(tname, Vec::new(), pairs)?;
        let count = plan
            .delta(tname)
            .map(|d| d.updates.len() as u64)
            .unwrap_or(0);
        self.apply_referential_plan(&plan)?;
        if let Some(ret) = update_returning {
            let changed: Vec<Object> = plan
                .delta(tname)
                .map(|d| d.updates.iter().map(|(_, n)| n.clone()).collect())
                .unwrap_or_default();
            return project_returning(ret, &changed);
        }
        Ok(ExecOutcome::Affected(count))
    }
}
