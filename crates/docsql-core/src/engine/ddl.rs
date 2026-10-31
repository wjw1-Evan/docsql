use super::*;

impl Database {
    pub(super) fn exec_create_index(
        &mut self,
        idx: sqlparser::ast::CreateIndex,
    ) -> Result<ExecOutcome> {
        // Clause-level guard: every index option the engine cannot honor must
        // be refused, not dropped (a filtered/INCLUDE index silently becomes
        // a different index).
        if !idx.include.is_empty() {
            return err("CREATE INDEX INCLUDE is not supported");
        }
        // Partial index (CREATE INDEX … WHERE): the predicate is validated
        // against the table below; the tree then holds only the qualifying
        // rows and every read must prove the query implies the predicate
        // before probing (superset safety, see [`query_implies_pred`]).
        let pred_expr = match &idx.predicate {
            Some(e) => {
                let p = validate_partial_pred(e)?;
                Some(p)
            }
            None => None,
        };
        if idx.using.is_some() {
            return err("index types (USING ...) are not supported");
        }
        if idx.nulls_distinct.is_some() {
            return err("NULLS [NOT] DISTINCT is not supported");
        }
        if idx.concurrently {
            return err("CREATE INDEX CONCURRENTLY is not supported");
        }
        if !idx.with.is_empty() || !idx.index_options.is_empty() {
            return err("index storage options are not supported");
        }
        let table = obj_name(&idx.table_name);
        if !self.internal_ddl
            && (crate::useradmin::is_user_table(&table) || crate::engine::is_system_table(&table))
        {
            // An index built on the internal user tables is invisible to
            // dumps (user tables replay through their own statement
            // channel) yet lands in the table's schema hash — an embedded
            // node would silently diverge from its cluster's digest.
            return err(format!("cannot index the internal table {table}"));
        }
        if self.tables.get(&table).is_some_and(|m| m.is_view()) {
            return err(format!(
                "cannot index view {table} (it is a view, not a table)"
            ));
        }
        let iname = idx
            .name
            .as_ref()
            .map(obj_name)
            .unwrap_or_else(|| format!("idx_{}", table));
        // Reserved for the table's own PRIMARY KEY/UNIQUE constraint
        // indexes; a user index with such a name would shadow them.
        if iname.starts_with("sqlite_autoindex_") {
            return err(
                "index names beginning with sqlite_autoindex_ are reserved for \
                 PRIMARY KEY/UNIQUE constraint indexes",
            );
        }
        // System/internal table names are unusable as index names: the
        // server's write gate classifies DROP INDEX by name and would
        // refuse to ever drop it (no other DROP INDEX entry point exists).
        if is_system_table(&iname) || is_internal_table(&iname) {
            return err(format!(
                "index name {iname} is reserved for a system/internal table"
            ));
        }
        // Index names share a database-wide namespace (SQLite semantics):
        // a name taken by any table blocks reuse elsewhere.
        let owner = self.tables.values().find(|m| m.indexes.contains(&iname));
        if owner.is_some() {
            if idx.if_not_exists {
                return Ok(ExecOutcome::Affected(0));
            }
            return err(format!("index {iname} already exists"));
        }
        let Some(mut meta) = self.tables.get(&table).map(|a| a.as_ref().clone()) else {
            return err(format!("table {table} does not exist"));
        };
        // Partial-predicate columns must be declared: a ghost name would
        // read NULL on every row and silently empty the tree.
        if let Some(p) = &pred_expr {
            let mut refs = std::collections::BTreeSet::new();
            pred_columns(p, &mut refs);
            for c in &refs {
                if !meta.columns.contains(c) {
                    return err(format!("column {c} does not exist"));
                }
            }
        }
        // Parse the indexed columns: plain identifiers only (expression
        // indexes stay unsupported), each must exist, duplicates rejected.
        if idx.columns.is_empty() {
            return err("CREATE INDEX requires at least one column");
        }
        let mut cols: Vec<String> = Vec::with_capacity(idx.columns.len());
        // JSON path index (design 005): exactly one column of the form
        // JSON_EXTRACT(col, '$.path'). Tree keys are values extracted from
        // the column's JSON text — the same lookup JSON_EXTRACT evaluates
        // at query time, so probes and inline evaluation cannot diverge.
        let mut path: Option<String> = None;
        for c in &idx.columns {
            // Direction is part of the index definition: accepting DESC here
            // and silently building ASC (as before) creates an index whose
            // ORDER BY optimization direction is the opposite of what was
            // declared. Same refusal discipline as INCLUDE/WHERE/USING.
            if c.column.options.asc == Some(false) {
                return err("descending indexes (CREATE INDEX ... DESC) are not supported");
            }
            if let SqlExpr::Function(f) = &c.column.expr {
                let fname = f.name.to_string().to_uppercase();
                if fname == "JSON_EXTRACT" {
                    if path.is_some() || cols.len() > 1 {
                        return err("a JSON path index is single-column: one JSON_EXTRACT(col, \
                             '$.path') term per index");
                    }
                    let (col, p) = parse_json_index_column(f)?;
                    if !meta.columns.contains(&col) {
                        return err(format!("column {col} does not exist"));
                    }
                    cols.push(col);
                    path = Some(p);
                    continue;
                }
                return err(
                    "expression indexes are not supported (only JSON_EXTRACT(col, '$.path'))",
                );
            }
            let col = match &c.column.expr {
                SqlExpr::Identifier(i) => i.value.clone(),
                other => expr_name(other),
            };
            if !meta.columns.contains(&col) {
                return err(format!("column {col} does not exist"));
            }
            if cols.contains(&col) {
                return err(format!("duplicate column name: {col} (in index {iname})"));
            }
            cols.push(col);
        }
        // UNIQUE path indexes are refused, not degraded: uniqueness
        // enforcement is column-based (meta.unique / constraint_unique and
        // the OR REPLACE displacement machinery do not know paths), and a
        // tree-level-only unique would leave UPDATE displacements unguarded.
        if idx.unique && path.is_some() {
            return err(
                "UNIQUE JSON path indexes are not supported (uniqueness enforcement \
                 is column-based)",
            );
        }
        if path.is_some() && pred_expr.is_some() {
            return err("a partial index cannot combine with a JSON path index");
        }
        let root_key = if path.is_some() {
            // Path trees are keyed by extracted values, never column values:
            // they always key by the index name and never reuse a column
            // tree (a scalar tree on the same column has a different key
            // shape; sharing one would mix value kinds in one tree).
            iname.clone()
        } else if cols.len() > 1 {
            iname.clone()
        } else if pred_expr.is_some() {
            // A partial tree holds a SUBSET of the column's rows — it must
            // never occupy the column-name root slot where ungated readers
            // (conflict probes, constraint lookups) expect a full tree.
            iname.clone()
        } else {
            cols[0].clone()
        };
        // root_key collision guard: `index_roots` is a mixed pool — column-
        // named constraint/single-column trees plus index-name-keyed
        // composite trees — and `BTreeMap::append` silently OVERWRITES an
        // existing key. A collision with a DIFFERENTLY-keyed tree (an index
        // named exactly like the PK column, a single-column index on a
        // column whose root holds a composite tree) would replace a live
        // tree: uniqueness enforcement would probe a tree keyed with a
        // different shape, old and new rows would mix key types in one
        // tree, and the overwritten tree's pages would leak — refuse.
        // The one benign case REUSES the existing tree instead: a second
        // index over exactly the same single column (e.g. UNIQUE INDEX on a
        // constraint column). Both indexes read the same tree; single-
        // column uniqueness is enforced through `meta.unique` (pushed
        // below), never through tree-level rejection. `IF NOT EXISTS` does
        // not apply: it is about the index NAME, and the colliding index
        // is a different one.
        let mut reuse_root: Option<u32> = None;
        if let Some(&root) = meta.index_roots.get(&root_key) {
            if path.is_none() && pred_expr.is_none() && meta.index_columns_of(&root_key) == cols {
                reuse_root = Some(root);
            } else {
                return err(format!(
                    "index root key {root_key} is already in use by another index or \
                     constraint; choose a different index name or column"
                ));
            }
        }
        // Single-column UNIQUE INDEX rides the constraint machinery: the
        // column joins meta.unique, so inserts/updates enforce duplicates
        // from here on. Composite unique indexes cannot use the per-column
        // container — their tree enforces uniqueness itself (unique insert).
        // A PARTIAL unique enforces only among its qualifying rows via the
        // tree; the column must NOT join meta.unique (that would enforce
        // globally, far beyond the predicate's scope).
        let composite = cols.len() > 1;
        if idx.unique && !composite && pred_expr.is_none() && !meta.unique.contains(&cols[0]) {
            let docs = self.table_docs_cx(&table)?;
            meta.unique.push(cols[0].clone());
            meta.check_unique(&docs)?;
        }
        // Build the B+ tree over the existing rows. Single-column trees are
        // non-unique at the tree level (duplicates allowed, lookups collect
        // every match); composite UNIQUE trees enforce duplicates on insert.
        let mut tx = self.pager.begin_tx();
        match reuse_root {
            // Same-semantics reuse: no tree build, the existing root stays
            // authoritative for both indexes.
            Some(root) => {
                meta.index_roots.insert(root_key.clone(), root);
            }
            None => {
                let pairs = self.table_pairs_cx(&table)?;
                let mut roots = self.build_trees(
                    &mut tx,
                    &pairs,
                    std::slice::from_ref(&root_key),
                    std::slice::from_ref(&cols),
                    std::slice::from_ref(&path),
                    std::slice::from_ref(&idx.unique),
                    std::slice::from_ref(&pred_expr),
                )?;
                meta.index_roots.append(&mut roots);
            }
        }
        meta.index_defs.push(IndexDef {
            name: iname.clone(),
            columns: cols,
            unique: idx.unique,
            path: path.clone(),
            pred: pred_expr.as_ref().map(|p| p.to_string()),
        });
        meta.indexes.push(iname);
        let prev_meta = self.tables.get(&table).cloned();
        self.tables.insert(table.clone(), std::sync::Arc::new(meta));
        // The catalog rides the SAME write unit as the tree pages: committing
        // the tree first and saving the catalog in a second transaction left
        // a crash window holding a committed tree no catalog references (the
        // pages would leak for good — nothing would ever free them).
        let persisted = self
            .save_catalog_into(&mut tx)
            .and_then(|()| self.commit_pager_tx(tx));
        if let Err(e) = persisted {
            // The tx dropped without committing: its staged tree pages went
            // back to the reusable pool. The in-memory entry must not keep
            // pointing at them — a later write would land index entries on
            // pages a concurrent allocation may hand to someone else.
            match prev_meta {
                Some(old) => {
                    self.tables.insert(table, old);
                }
                None => {
                    self.tables.remove(&table);
                }
            }
            return Err(e);
        }
        Ok(ExecOutcome::Affected(0))
    }

    /// All-or-nothing validation for ALTER TABLE: every operation is checked
    /// against a simulated meta (columns / PK / CHECKs evolve op by op) so a
    /// failure in a later operation cannot leave earlier ones committed —
    /// each rewrite persists its own write unit, so a mid-statement failure
    /// used to leave the statement half-applied (with no journal entry for
    /// the applied part, diverging replicas until repair). Also fronts the
    /// per-op existence/duplication checks the apply loop historically
    /// skipped (DROP of a missing column silently "succeeded").
    #[allow(clippy::too_many_lines)]
    pub(super) fn validate_alter_ops(
        &self,
        base: &TableMeta,
        tname: &str,
        ops: &[sqlparser::ast::AlterTableOperation],
    ) -> Result<()> {
        use sqlparser::ast::AlterTableOperation as Op;
        // RENAME TABLE returns from the apply loop early; anything after it
        // in the same statement was silently discarded.
        if ops.iter().any(|o| matches!(o, Op::RenameTable { .. })) && ops.len() > 1 {
            return err(
                "ALTER TABLE ... RENAME TO must be the only operation in the statement".to_string(),
            );
        }
        let mut columns: Vec<String> = base.columns.clone();
        let mut pk: Option<String> = base.primary_key.clone();
        let mut checks: Vec<String> = base.checks.clone();
        // Index namespaces evolve too (DROP COLUMN kills trees over the
        // dropped column); kept as owned copies so the rename-collision
        // check sees the state earlier operations produced.
        let mut roots: std::collections::BTreeSet<String> =
            base.index_roots.keys().cloned().collect();
        let mut index_defs: Vec<IndexDef> = base.index_defs.clone();
        // Column renames applied so far in this statement, so a later DROP
        // COLUMN matches FK references under their post-rename spelling
        // (`RENAME COLUMN a TO b, DROP COLUMN b` must still see the child
        // FKs that the apply loop will rewrite a→b).
        let mut renamed: Vec<(String, String)> = Vec::new();
        for op in ops {
            match op {
                Op::AddColumn { column_def, .. } => {
                    let col = &column_def.name.value;
                    if columns.contains(col) {
                        return err(format!("duplicate column name: {col}"));
                    }
                    let mut add_not_null = false;
                    let mut default_text: Option<String> = None;
                    for opt in &column_def.options {
                        use sqlparser::ast::ColumnOption as CO;
                        match &opt.option {
                            CO::Check(c) => {
                                if expr_calls_wall_clock(&c.expr) {
                                    return err(
                                        "CHECK constraints cannot call wall-clock or random \
                                         functions (NOW/SYSDATE/CURRENT_TIMESTAMP/GETDATE/RAND); \
                                         every node would evaluate a different value",
                                    );
                                }
                                // Column references must resolve (the new
                                // column itself is in scope for its own
                                // CHECK): the apply loop rejects unknown
                                // references MID-STATEMENT, after earlier
                                // operations of the same ALTER already
                                // committed — catch it here instead.
                                let mut refs = Vec::new();
                                check_expr_column_refs(&c.expr, &mut refs)?;
                                for r in refs {
                                    if r != *col && !columns.contains(&r) {
                                        return err(format!(
                                            "CHECK constraint references unknown column {r}"
                                        ));
                                    }
                                }
                                // Mirror the apply loop: later operations in
                                // the same statement must see this CHECK in
                                // the simulated state, or `DROP COLUMN` of
                                // the just-added column passes validation and
                                // half-applies.
                                checks.push(format!("{}", c.expr));
                            }
                            CO::Default(e) => {
                                // The apply loop constant-folds every new
                                // DEFAULT for the backfill; pre-empt the same
                                // failure here so a later operation failing
                                // on it can never leave an earlier one
                                // committed.
                                let text = default_expr_text(e);
                                let expr = parse_expr_text(&text)?;
                                eval_const(&expr)?;
                                default_text = Some(text);
                            }
                            CO::NotNull => add_not_null = true,
                            CO::PrimaryKey { .. }
                            | CO::Unique { .. }
                            | CO::ForeignKey(_)
                            | CO::DialectSpecific(_) => {
                                return err(format!(
                                    "constraint options are not supported on ADD COLUMN {col}"
                                ));
                            }
                            // Same parity as the apply loop (and CREATE
                            // TABLE): swallowing these silently degraded the
                            // column to a plain one.
                            CO::Generated { .. }
                            | CO::Materialized(_)
                            | CO::Ephemeral(_)
                            | CO::Alias(_) => {
                                return err(format!(
                                    "generated/computed columns are not supported \
                                     (ADD COLUMN {col})"
                                ));
                            }
                            CO::Null => {}
                            _ => {
                                return err(format!(
                                    "unsupported column option on ADD COLUMN {col}"
                                ))
                            }
                        }
                    }
                    // A new column's default can only arrive in this very
                    // definition (the apply loop registers it in the same
                    // pass, before the NOT NULL rule is judged).
                    if add_not_null
                        && !column_def
                            .options
                            .iter()
                            .any(|o| matches!(o.option, sqlparser::ast::ColumnOption::Default(_)))
                    {
                        return err(format!(
                            "cannot add NOT NULL column {col} without a DEFAULT"
                        ));
                    }
                    // Backfill-vs-CHECK parity with the apply loop: every
                    // check — including one riding on THIS ADD COLUMN — is
                    // evaluated against the backfilled rows there; without
                    // this simulation a later operation in the same ALTER
                    // commits first and the statement half-applies.
                    if let Some(text) = &default_text {
                        if !checks.is_empty() {
                            let fill = eval_const(&parse_expr_text(text)?)?;
                            for d in self.table_docs_cx(tname)? {
                                let mut filled = d;
                                if !filled.contains_key(col) {
                                    filled.insert(col.clone(), fill.clone());
                                }
                                for c in &checks {
                                    let e = parse_expr_text(c)?;
                                    if matches!(
                                        check_expr_outcome(&e, &filled)?,
                                        CheckOutcome::Fail
                                    ) {
                                        return err(format!("CHECK constraint failed: {c}"));
                                    }
                                }
                            }
                        }
                    }
                    columns.push(col.clone());
                }
                Op::DropColumn { column_names, .. } => {
                    for id in column_names {
                        let name = &id.value;
                        if !columns.contains(name) {
                            return err(format!("column {name} does not exist"));
                        }
                        if pk.as_deref() == Some(name.as_str()) {
                            return err("cannot drop a PRIMARY KEY column");
                        }
                        // A partial-index predicate naming the column would
                        // silently read NULL after the drop — every row would
                        // fall out of the tree at the next rewrite. Refuse
                        // like CHECK constraints do (drop the index first).
                        if let Some(bad) = index_defs.iter().find_map(|d| {
                            d.pred.as_deref().and_then(|p| {
                                parse_expr_text(p)
                                    .map(|e| {
                                        let mut cols = std::collections::BTreeSet::new();
                                        pred_columns(&e, &mut cols);
                                        cols.contains(name).then(|| d.name.clone())
                                    })
                                    .unwrap_or(None)
                            })
                        }) {
                            return err(format!(
                                "cannot drop column {name}: partial index {bad} references it \
                                 in its WHERE predicate"
                            ));
                        }
                        if let Some(bad) = checks.iter().find(|c| {
                            parse_expr_text(c)
                                .map(|e| expr_references_ident(&e, name))
                                .unwrap_or(true)
                        }) {
                            return err(format!(
                                "cannot drop column {name}: CHECK constraint {bad} references it"
                            ));
                        }
                        // Stored view SQL is not rewritten: dropping a column
                        // a view still names breaks the view (its missing
                        // column reads NULL — for a WHERE that is silently
                        // wrong rows) while the dump replays "fine". Refuse
                        // like RENAME does, scoped to views that actually
                        // name the column. The chain is TRANSITIVE: a
                        // `SELECT *` middle layer carries the column upward
                        // without naming it, so direct dependents alone
                        // would let v2 = SELECT v FROM v1 degrade to NULL.
                        let mut dep_chain = vec![tname.to_string()];
                        loop {
                            let deps = self.direct_view_dependents(&dep_chain);
                            if deps.is_empty() {
                                break;
                            }
                            dep_chain.extend(deps);
                        }
                        let view_refs: Vec<String> = dep_chain
                            .into_iter()
                            .filter(|v| {
                                v != tname
                                    && self
                                        .tables
                                        .get(v)
                                        .and_then(|m| m.view_sql.clone())
                                        .is_some_and(|sql| text_references_ident(&sql, name))
                            })
                            .collect();
                        if !view_refs.is_empty() {
                            return err(format!(
                                "cannot drop column {tname}.{name}: referenced by VIEW {} \
                                 (drop the view first; its stored SQL is not rewritten)",
                                view_refs.join(", ")
                            ));
                        }
                        let referencing: Vec<String> = self
                            .tables
                            .iter()
                            .filter(|(_, m)| {
                                m.foreign_keys.iter().any(|fk| {
                                    if fk.ref_table != tname {
                                        return false;
                                    }
                                    // Match under the rc spelling the apply
                                    // loop will have produced by the time
                                    // this DROP runs (renames earlier in the
                                    // same statement rewrote it).
                                    let evolved =
                                        renamed.iter().fold(fk.ref_col.clone(), |cur, (o, n)| {
                                            if cur == *o {
                                                n.clone()
                                            } else {
                                                cur
                                            }
                                        });
                                    evolved == *name
                                })
                            })
                            .map(|(t, _)| t.clone())
                            .collect();
                        if !referencing.is_empty() {
                            return err(format!(
                                "cannot drop column {tname}.{name}: referenced by FOREIGN KEY in {}",
                                referencing.join(", ")
                            ));
                        }
                        columns.retain(|c| c != name);
                        // Mirror the apply loop's index teardown on the
                        // simulated state: the dropped column's single-column
                        // tree dies, and every index over the column dies with
                        // it. RenameColumn later in the statement must check
                        // collisions against this EVOLVED state, or
                        // `DROP COLUMN x, RENAME COLUMN y TO x` is wrongly
                        // refused for x's dead index.
                        roots.remove(name);
                        index_defs.retain(|d| !d.columns.iter().any(|c| c == name));
                    }
                }
                Op::RenameColumn {
                    old_column_name,
                    new_column_name,
                } => {
                    let (old, new) = (&old_column_name.value, &new_column_name.value);
                    if !columns.contains(old) {
                        return err(format!("column {old} does not exist"));
                    }
                    // Stored view SQL is not rewritten: renaming a column of
                    // a referenced table breaks the view (and makes its
                    // dump_script unreplayable). Refuse like DROP does.
                    let deps = self.direct_view_dependents(&[tname.to_string()]);
                    if !deps.is_empty() {
                        return err(format!(
                            "cannot rename column {tname}.{old}: table is referenced by VIEW {} \
                             (drop the view first; its stored SQL is not rewritten)",
                            deps.join(", ")
                        ));
                    }
                    if old != new {
                        // Every namespace the new name can collide with: a
                        // duplicate column collapses two columns' data into
                        // one key, and an index root-key (single-column roots
                        // key by column name, composites by index name) would
                        // be silently overwritten by the rename. Both checks
                        // run on the EVOLVED state (earlier DROP COLUMNs may
                        // have removed trees/defs).
                        if columns.contains(new) {
                            return err(format!("duplicate column name: {new}"));
                        }
                        if roots.contains(new) || index_defs.iter().any(|d| d.name == *new) {
                            return err(format!("name {new} is already used by an index"));
                        }
                        // Same collapse through the schemaless door: an
                        // undeclared field already present in the stored
                        // documents. The apply loop's remove(old)+insert(new)
                        // would silently overwrite those values.
                        if self
                            .table_docs_cx(tname)?
                            .iter()
                            .any(|d| d.contains_key(new.as_str()))
                        {
                            return err(format!(
                                "cannot rename column {tname}.{old} to {new}: \
                                 {new} already exists as a data field"
                            ));
                        }
                    }
                    renamed.push((old.clone(), new.clone()));
                    columns = columns
                        .iter()
                        .map(|c| if c == old { new.clone() } else { c.clone() })
                        .collect();
                    if pk.as_deref() == Some(old.as_str()) {
                        pk = Some(new.clone());
                    }
                    // The apply loop rekeys the single-column tree from the
                    // old column name to the new one — evolve the simulated
                    // roots the same way, or a later rename falsely reports
                    // a collision with the vacated name. Same ownership rule
                    // as the apply loop: a composite/path def NAMED like the
                    // column owns the def-name root and blocks the rekey.
                    let own_tree = !index_defs
                        .iter()
                        .any(|d| d.name == *old && (d.path.is_some() || d.columns.len() > 1));
                    if own_tree && roots.remove(old.as_str()) {
                        roots.insert(new.clone());
                    }
                    for d in index_defs.iter_mut() {
                        for c in d.columns.iter_mut() {
                            if c == old {
                                *c = new.clone();
                            }
                        }
                    }
                    checks = checks
                        .iter()
                        .map(|c| rename_ident_in_text(c, old, &ident_spelling(new)))
                        .collect();
                }
                Op::RenameTable { table_name } => {
                    let new_name = match table_name {
                        sqlparser::ast::RenameTableNameKind::As(n)
                        | sqlparser::ast::RenameTableNameKind::To(n) => obj_name(n),
                    };
                    if self.tables.contains_key(&new_name) {
                        return err(format!("table {new_name} already exists"));
                    }
                    // Reserved names are filtered out of dump/snapshot or
                    // collide with internal tables; CREATE refuses them, so
                    // rename must too (a renamed-away table would silently
                    // vanish from every backup).
                    if is_system_table(&new_name) || crate::useradmin::is_user_table(&new_name) {
                        return err(format!("table name {new_name} is reserved for the engine"));
                    }
                    // Same parity as CREATE TABLE: a # temp-table name is
                    // refused outright, and a compat dictionary name would
                    // shadow the table at FROM-resolution — writes would
                    // land but SELECTs would read the dictionary instead,
                    // and dump_script would emit a CREATE the replay guard
                    // rejects (backup/restore and join snapshots fail).
                    if new_name.starts_with('#') {
                        return err("temp tables (#name/##name) are not supported");
                    }
                    if is_compat_view(&new_name) {
                        return err(format!(
                            "table name {new_name} is reserved for the compatibility dictionary views"
                        ));
                    }
                    // Stored view SQL is not rewritten: a view left pointing
                    // at the old name breaks every SELECT and — worse — makes
                    // dump_script unreplayable (backup/restore and join/repair
                    // snapshots all fail). Refuse like DROP does.
                    let deps = self.direct_view_dependents(&[tname.to_string()]);
                    if !deps.is_empty() {
                        return err(format!(
                            "cannot rename table {tname}: referenced by VIEW {} \
                             (drop the view first; its stored SQL is not rewritten)",
                            deps.join(", ")
                        ));
                    }
                }
                other => return err(format!("unsupported ALTER TABLE operation: {other}")),
            }
        }
        Ok(())
    }

    pub(super) fn exec_alter(&mut self, alter: sqlparser::ast::AlterTable) -> Result<ExecOutcome> {
        use sqlparser::ast::AlterTableOperation as Op;
        let tname = obj_name(&alter.name);
        if !self.internal_ddl
            && (crate::useradmin::is_user_table(&tname) || crate::engine::is_system_table(&tname))
        {
            // The network path already classifies these writes away
            // (stmt_write_targets); an embedded/in-process caller could
            // ALTER the replicated user tables directly, and the altered
            // schema would diverge from every peer's digest.
            return err(format!("cannot ALTER the internal table {tname}"));
        }
        if self.tables.get(&tname).is_some_and(|m| m.is_matview()) && !self.internal_ddl {
            return err(format!(
                "cannot ALTER materialized view {tname} (it is refreshed, not altered)"
            ));
        }
        let Some(mut meta) = self.tables.get(&tname).map(|a| a.as_ref().clone()) else {
            return err(format!("table {tname} does not exist"));
        };
        ensure_not_view("ALTER", &tname, &meta)?;
        self.validate_alter_ops(&meta, &tname, &alter.operations)?;
        // rewrite_table persists the catalog in its own transaction; only
        // metadata-only ALTERs (plain ADD COLUMN) need the tail save.
        // Trees detached from `meta.index_roots` by DROP COLUMN below; freed
        // inside the rebuild's write unit (see rewrite_table_with_dead).
        let mut dead_trees: Vec<(String, u32)> = Vec::new();
        for op in &alter.operations {
            match op {
                Op::AddColumn { column_def, .. } => {
                    let col = column_def.name.value.clone();
                    if !meta.columns.contains(&col) {
                        meta.columns.push(col.clone());
                    }
                    // Column-level DEFAULT / CHECK ride along with ADD COLUMN.
                    // NOT NULL is validated after the loop: the conventional
                    // spelling `NOT NULL DEFAULT x` registers the default in
                    // a later option, so the "NOT NULL needs a DEFAULT" rule
                    // can only be judged once all options are in.
                    let mut add_not_null = false;
                    for opt in &column_def.options {
                        use sqlparser::ast::ColumnOption as CO;
                        match &opt.option {
                            CO::Default(e) => {
                                meta.defaults.push((col.clone(), default_expr_text(e)))
                            }
                            CO::Check(c) => {
                                if expr_calls_wall_clock(&c.expr) {
                                    return err(
                                        "CHECK constraints cannot call wall-clock or random \
                                         functions (NOW/SYSDATE/CURRENT_TIMESTAMP/GETDATE/RAND); \
                                         every node would evaluate a different value",
                                    );
                                }
                                let mut refs = Vec::new();
                                check_expr_column_refs(&c.expr, &mut refs)?;
                                for r in refs {
                                    if !meta.columns.contains(&r) {
                                        return err(format!(
                                            "CHECK constraint references unknown column {r}"
                                        ));
                                    }
                                }
                                meta.checks.push(format!("{}", c.expr))
                            }
                            CO::NotNull => add_not_null = true,
                            // Same parity as CREATE TABLE: these options on
                            // ADD COLUMN would dangle (no tree, no backfill).
                            CO::PrimaryKey { .. }
                            | CO::Unique { .. }
                            | CO::ForeignKey(_)
                            | CO::DialectSpecific(_) => {
                                return err(format!(
                                    "constraint options are not supported on ADD COLUMN {col}"
                                ));
                            }
                            // Swallowing these silently degraded the column
                            // to a plain one (the CREATE TABLE path rejects
                            // the same spellings).
                            CO::Generated { .. }
                            | CO::Materialized(_)
                            | CO::Ephemeral(_)
                            | CO::Alias(_) => {
                                return err(format!(
                                    "generated/computed columns are not supported \
                                     (ADD COLUMN {col})"
                                ));
                            }
                            CO::Null => {}
                            _ => {
                                return err(format!(
                                    "unsupported column option on ADD COLUMN {col}"
                                ))
                            }
                        }
                    }
                    // SQLite rule: a NOT NULL column needs a default to
                    // backfill existing rows, else every later UPDATE of a
                    // pre-existing row would fail.
                    if add_not_null && !meta.defaults.iter().any(|(c, _)| c == &col) {
                        return err(format!(
                            "cannot add NOT NULL column {col} without a DEFAULT"
                        ));
                    }
                    if add_not_null {
                        meta.not_null.push(col.clone());
                    }
                    // ADD COLUMN with DEFAULT backfills existing rows.
                    if let Some((_, text)) = meta.defaults.iter().find(|(c, _)| c == &col).cloned()
                    {
                        let e = parse_expr_text(&text)?;
                        // A wall-clock/random default would backfill THIS
                        // node's clock/roll into every existing row with no
                        // resolved-text rewrite — replaying peers stamp
                        // their own values and the tables diverge. Refuse
                        // while there are rows to backfill (an empty table
                        // keeps the default text for future INSERTs, which
                        // do rewrite deterministically).
                        if expr_calls_wall_clock(&e) && !self.table_docs_cx(&tname)?.is_empty() {
                            return err(
                                "ADD COLUMN cannot backfill a wall-clock or random DEFAULT \
                                 (NOW/SYSDATE/CURRENT_TIMESTAMP/GETDATE/RAND) into existing \
                                 rows; supply the column explicitly on INSERT instead",
                            );
                        }
                        let fill = eval_const(&e)?;
                        let docs = self.table_docs_cx(&tname)?;
                        let filled: Vec<Object> = docs
                            .into_iter()
                            .map(|mut d| {
                                if !d.contains_key(&col) {
                                    d.insert(col.clone(), fill.clone());
                                }
                                d
                            })
                            .collect();
                        // The backfilled image must satisfy the checks —
                        // including one riding along on THIS ADD COLUMN:
                        // `ADD COLUMN c INT DEFAULT -5 CHECK (c > 0)` used
                        // to backfill violating values while later INSERTs
                        // were correctly rejected.
                        for d in &filled {
                            meta.check(d)?;
                        }
                        self.rewrite_table(&tname, &mut meta, filled)?;
                    }
                }
                Op::DropColumn { column_names, .. } => {
                    for id in column_names {
                        let name = id.value.clone();
                        if meta.primary_key.as_deref() == Some(name.as_str()) {
                            return err("cannot drop a PRIMARY KEY column");
                        }
                        // A CHECK text still naming the column would read it
                        // as NULL afterwards and silently stop enforcing
                        // (the rename path already guards against this).
                        if let Some(bad) = meta.checks.iter().find(|c| {
                            parse_expr_text(c)
                                .map(|e| expr_references_ident(&e, &name))
                                .unwrap_or(true)
                        }) {
                            return err(format!(
                                "cannot drop column {name}: CHECK constraint {bad} references it"
                            ));
                        }
                        // Another table's (or this table's own) FK pointing
                        // at the column would leave every future child insert
                        // failing with a dangling reference.
                        let referencing: Vec<String> = self
                            .tables
                            .iter()
                            .filter(|(_, m)| {
                                m.foreign_keys
                                    .iter()
                                    .any(|fk| fk.ref_table == tname && fk.ref_col == name)
                            })
                            .map(|(t, _)| t.clone())
                            .collect();
                        if !referencing.is_empty() {
                            return err(format!(
                                "cannot drop column {tname}.{name}: referenced by FOREIGN KEY in {}",
                                referencing.join(", ")
                            ));
                        }
                        meta.columns.retain(|c| c != &name);
                        meta.unique.retain(|c| c != &name);
                        meta.constraint_unique.retain(|c| c != &name);
                        meta.not_null.retain(|c| c != &name);
                        meta.defaults.retain(|(c, _)| c != &name);
                        meta.foreign_keys.retain(|fk| fk.col != name);
                        if let Some(root) = meta.index_roots.remove(&name) {
                            // Capture the tree BEFORE dropping it from the
                            // map: the rebuild below only frees trees still
                            // listed in `index_roots`, so these roots must
                            // ride `dead_trees` or their pages leak forever.
                            dead_trees.push((name.clone(), root));
                        }
                        if meta.autoinc.as_deref() == Some(name.as_str()) {
                            meta.autoinc = None;
                        }
                        if meta.autoguid.as_deref() == Some(name.as_str()) {
                            meta.autoguid = None;
                        }
                        // Indexes over the dropped column lose their
                        // definitions (trees above were already removed).
                        // A composite index over the column dies with it too.
                        let dead: Vec<String> = meta
                            .index_defs
                            .iter()
                            .filter(|d| d.columns.iter().any(|c| c == name.as_str()))
                            .map(|d| d.name.clone())
                            .collect();
                        for n in &dead {
                            meta.indexes.retain(|i| i != n);
                        }
                        for (k, root) in meta.index_roots.iter() {
                            if dead.contains(k) {
                                dead_trees.push((k.clone(), *root));
                            }
                        }
                        meta.index_roots.retain(|k, _| !dead.contains(k));
                        meta.index_defs.retain(|d| !dead.contains(&d.name));
                    }
                    let docs = self.table_docs_cx(&tname)?;
                    let stripped: Vec<Object> = docs
                        .into_iter()
                        .map(|mut d| {
                            for id in column_names {
                                d.remove(&id.value);
                            }
                            d
                        })
                        .collect();
                    self.rewrite_table_with_dead(
                        &tname,
                        &mut meta,
                        stripped,
                        std::mem::take(&mut dead_trees),
                    )?;
                }
                Op::RenameColumn {
                    old_column_name,
                    new_column_name,
                } => {
                    let (old, new) = (&old_column_name.value, &new_column_name.value);
                    if !meta.columns.contains(old) {
                        return err(format!("column {old} does not exist"));
                    }
                    meta.columns = meta
                        .columns
                        .iter()
                        .map(|c| if c == old { new.clone() } else { c.clone() })
                        .collect();
                    if meta.primary_key.as_deref() == Some(old.as_str()) {
                        meta.primary_key = Some(new.clone());
                    }
                    meta.unique = meta
                        .unique
                        .iter()
                        .map(|c| if c == old { new.clone() } else { c.clone() })
                        .collect();
                    meta.not_null = meta
                        .not_null
                        .iter()
                        .map(|c| if c == old { new.clone() } else { c.clone() })
                        .collect();
                    if meta.autoinc.as_deref() == Some(old.as_str()) {
                        meta.autoinc = Some(new.clone());
                    }
                    if meta.autoguid.as_deref() == Some(old.as_str()) {
                        meta.autoguid = Some(new.clone());
                    }
                    // Only the column's OWN single-column tree rekeys with
                    // the rename. A composite or JSON-path index NAMED like
                    // the column owns the def-name root (root_key = index
                    // name) — the def name does not follow the column, and
                    // rekeying would graft an Array/path-keyed tree onto
                    // the new column key where DML would mix scalar keys
                    // into it. A SINGLE-column def named like the column
                    // but built on ANOTHER column owns that other column's
                    // root, not `old` — the column's own tree still rekeys
                    // (stranding it let duplicate PKs in silently and
                    // diverged replicas whose dump replay enforced them).
                    let own_tree = !meta
                        .index_defs
                        .iter()
                        .any(|d| d.name == *old && (d.path.is_some() || d.columns.len() > 1));
                    if own_tree {
                        if let Some(root) = meta.index_roots.remove(old) {
                            meta.index_roots.insert(new.clone(), root);
                        }
                    }
                    meta.defaults = meta
                        .defaults
                        .iter()
                        .map(|(c, t)| {
                            if c == old {
                                (new.clone(), t.clone())
                            } else {
                                (c.clone(), t.clone())
                            }
                        })
                        .collect();
                    meta.constraint_unique = meta
                        .constraint_unique
                        .iter()
                        .map(|c| if c == old { new.clone() } else { c.clone() })
                        .collect();
                    meta.index_defs = meta
                        .index_defs
                        .iter()
                        .map(|d| {
                            if d.columns.iter().any(|c| c == old) {
                                IndexDef {
                                    name: d.name.clone(),
                                    columns: d
                                        .columns
                                        .iter()
                                        .map(|c| if c == old { new.clone() } else { c.clone() })
                                        .collect(),
                                    unique: d.unique,
                                    path: d.path.clone(),
                                    pred: d.pred.clone(),
                                }
                            } else if d.pred.is_some() {
                                // The predicate is SQL text over row fields:
                                // a renamed column must not leave it naming a
                                // ghost (its rows would silently read NULL
                                // and drop out of the tree). Same rewrite the
                                // CHECK texts get.
                                IndexDef {
                                    name: d.name.clone(),
                                    columns: d.columns.clone(),
                                    unique: d.unique,
                                    path: d.path.clone(),
                                    pred: d.pred.as_deref().map(|p| {
                                        rename_ident_in_text(p, old, &ident_spelling(new))
                                    }),
                                }
                            } else {
                                d.clone()
                            }
                        })
                        .collect();
                    // FK columns on this table follow the rename, and so does
                    // the remote column of a same-table FK (self-references
                    // and sibling references): a dangling rc would fail every
                    // subsequent child write with FK errors.
                    meta.foreign_keys = meta
                        .foreign_keys
                        .iter()
                        .map(|fk| FkRef {
                            col: if fk.col == *old {
                                new.clone()
                            } else {
                                fk.col.clone()
                            },
                            ref_table: fk.ref_table.clone(),
                            ref_col: if fk.ref_table == tname && fk.ref_col == *old {
                                new.clone()
                            } else {
                                fk.ref_col.clone()
                            },
                            on_delete: fk.on_delete,
                            on_update: fk.on_update,
                        })
                        .collect();
                    // CHECK texts reference the old column name; leaving them
                    // stale silently disables the constraint (a missing column
                    // reads as NULL -> unknown -> check skipped).
                    meta.checks = meta
                        .checks
                        .iter()
                        .map(|c| rename_ident_in_text(c, old, &ident_spelling(new)))
                        .collect();
                    let docs = self.table_docs_cx(&tname)?;
                    let renamed: Vec<Object> = docs
                        .into_iter()
                        .map(|mut d| {
                            if let Some(v) = d.remove(old) {
                                d.insert(new.clone(), v);
                            }
                            d
                        })
                        .collect();
                    self.rewrite_table(&tname, &mut meta, renamed)?;
                    // Referencing tables' remote columns follow too (the same
                    // rule RENAME TABLE applies to their rt, also after the
                    // commit so the failure path stays atomic): a dangling rc
                    // makes every child write fail FK checks and aborts dump
                    // replay mid-script (backup/restore, join snapshots).
                    // Only tables actually referencing the old name get a
                    // targeted make_mut — never a whole-catalog deep copy.
                    let referrers: Vec<String> = self
                        .tables
                        .iter()
                        .filter(|(n, m)| {
                            n.as_str() != tname.as_str()
                                && m.foreign_keys
                                    .iter()
                                    .any(|fk| fk.ref_table == tname && fk.ref_col == *old)
                        })
                        .map(|(n, _)| n.clone())
                        .collect();
                    if !referrers.is_empty() {
                        // Snapshot the referrer entries: a failed catalog
                        // save must leave memory matching disk — the next
                        // successful write persists whatever sits in memory,
                        // so a diverged (rewritten-but-unsaved) entry would
                        // silently drift to peers.
                        let before: Vec<(String, std::sync::Arc<TableMeta>)> = referrers
                            .iter()
                            .filter_map(|n| self.tables.get(n).map(|m| (n.clone(), m.clone())))
                            .collect();
                        for name in referrers {
                            if let Some(entry) = self.tables.get_mut(&name) {
                                let m = std::sync::Arc::make_mut(entry);
                                for fk in m.foreign_keys.iter_mut() {
                                    if fk.ref_table == tname && fk.ref_col == *old {
                                        fk.ref_col = new.clone();
                                    }
                                }
                            }
                        }
                        if let Err(e) = self.save_catalog() {
                            for (n, m) in before {
                                self.tables.insert(n, m);
                            }
                            return Err(e);
                        }
                    }
                }
                Op::RenameTable { table_name } => {
                    let new_name = match table_name {
                        sqlparser::ast::RenameTableNameKind::As(n)
                        | sqlparser::ast::RenameTableNameKind::To(n) => obj_name(n),
                    };
                    // rewrite_table would silently replace the target's entry
                    // and orphan its data pages.
                    if self.tables.contains_key(&new_name) {
                        return err(format!("table {new_name} already exists"));
                    }
                    let docs = self.table_docs_cx(&tname)?;
                    let old_entry = self.tables.get(&tname).cloned();
                    // The mirrored catalog key follows the rename; constraint
                    // errors keep naming the table users actually see.
                    meta.name = new_name.clone();
                    // 自引用外键的 rt 跟随新名(与其它引用方一并改写)。
                    for fk in meta.foreign_keys.iter_mut() {
                        if fk.ref_table == tname {
                            fk.ref_table = new_name.clone();
                        }
                    }
                    self.tables.remove(&tname);
                    // The old name must not keep a stale AUTOINCREMENT
                    // watermark for a table created under it later.
                    self.autoinc_cache.remove(&tname);
                    // Grants follow the rename: a row left on the old name
                    // would (a) resurrect onto a future same-named table —
                    // silent privilege escalation across unrelated data —
                    // and (b) break dump replay (GRANT on a table that no
                    // longer exists aborts the whole restore). A failure of
                    // the UPDATE fails the whole rename BEFORE the rewrite
                    // (the catalog entry is restored below like every other
                    // failure path); a rewrite failure reverts the grants
                    // rows the same way.
                    let lit_old = crate::stmt::sql_string_literal(&tname);
                    let lit_new = crate::stmt::sql_string_literal(&new_name);
                    let grants_update = if self.tables.contains_key(crate::useradmin::GRANTS_TABLE)
                    {
                        self.execute(&format!(
                            "UPDATE {} SET tbl = {} WHERE tbl = {}",
                            crate::useradmin::GRANTS_TABLE,
                            lit_new,
                            lit_old
                        ))
                    } else {
                        Ok(ExecOutcome::Affected(0))
                    };
                    if let Err(e) = grants_update {
                        if let Some(old) = old_entry {
                            self.tables.insert(tname.clone(), old);
                        }
                        return Err(e);
                    }
                    if let Err(e) = self.rewrite_table(&new_name, &mut meta, docs) {
                        // rewrite_table's own failure restore only covers the
                        // NEW name's entry (absent here) — without this the
                        // old entry would stay removed and the table (and its
                        // data pages) would vanish from the catalog.
                        if let Some(old) = old_entry {
                            self.tables.insert(tname.clone(), old);
                        }
                        // The grants UPDATE above already landed (autocommit):
                        // point the rows back at the old name or the next
                        // same-named table would inherit them.
                        if let Err(e2) = self.execute(&format!(
                            "UPDATE {} SET tbl = {} WHERE tbl = {}",
                            crate::useradmin::GRANTS_TABLE,
                            lit_old,
                            lit_new
                        )) {
                            eprintln!(
                                "warning: failed to revert grant rows after a failed \
                                 RENAME ({e2}); re-run the RENAME or fix grants manually"
                            );
                        }
                        return Err(e);
                    }
                    // 引用方的外键表名跟随改写:悬挂 rt 会让子表的一切写
                    // 路径报 "table does not exist",dump/restore 还会把
                    // 坏状态原样带回(旧条目尚未动,失败路径保持原子)。
                    // 只对真正引用旧名的表定点 make_mut——全库逐表 make_mut
                    // 会在有 detached 读者时深拷每一张表的 meta。
                    let referrers: Vec<String> = self
                        .tables
                        .iter()
                        .filter(|(_, m)| m.foreign_keys.iter().any(|fk| fk.ref_table == tname))
                        .map(|(n, _)| n.clone())
                        .collect();
                    // Snapshot first: a failed catalog save must restore the
                    // referrer entries, or memory diverges from disk (the
                    // next successful write would persist the divergence).
                    let before: Vec<(String, std::sync::Arc<TableMeta>)> = referrers
                        .iter()
                        .filter_map(|n| self.tables.get(n).map(|m| (n.clone(), m.clone())))
                        .collect();
                    for name in referrers {
                        if let Some(m) = self.tables.get_mut(&name) {
                            let m = std::sync::Arc::make_mut(m);
                            for fk in m.foreign_keys.iter_mut() {
                                if fk.ref_table == tname {
                                    fk.ref_table = new_name.clone();
                                }
                            }
                        }
                    }
                    if let Err(e) = self.save_catalog() {
                        for (n, m) in before {
                            self.tables.insert(n, m);
                        }
                        return Err(e);
                    }
                    return Ok(ExecOutcome::Affected(0));
                }
                other => return err(format!("unsupported ALTER TABLE operation: {other}")),
            }
        }
        // A rewrite persists the meta CLONE it saw; any metadata-only
        // operations after it in the same statement (ADD COLUMN without a
        // DEFAULT, a CHECK) only touched the local `meta` and were silently
        // dropped. Save the final state unconditionally — the extra save
        // after a bare rewrite is idempotent. A failed save restores the
        // in-memory entry: the next successful write persists whatever is
        // in memory, so leaving the new meta would diverge from disk.
        let prev = self.tables.get(&tname).cloned();
        self.tables.insert(tname.clone(), std::sync::Arc::new(meta));
        if let Err(e) = self.save_catalog() {
            match prev {
                Some(old) => {
                    self.tables.insert(tname.clone(), old);
                }
                None => {
                    self.tables.remove(&tname);
                }
            }
            return Err(e);
        }
        Ok(ExecOutcome::Affected(0))
    }

    /// Validate FOREIGN KEY constraints of `meta` for one candidate doc:
    /// non-null values must exist in the referenced table's column.
    /// Child-side FK check. `same_batch` carries the statement's own new
    /// images for `table` — when an FK targets the SAME table (self- or
    /// intra-statement references: `UPDATE emp SET mgr = 1 WHERE id = 1`,
    /// multi-row inserts whose later rows reference earlier ones), those
    /// images are legitimate parent candidates. Without them such rows were
    /// rejected (or, via UPDATE, created rows that dump/restore could never
    /// replay) even though SQLite/PG immediate FKs accept them.
    ///
    /// `self_final` is the statement's COMPLETE final state of `table` (the
    /// pre-statement rows minus replaced old images plus the new images).
    /// When provided, self-referential lookups use it ALONE: `same_batch`
    /// plus the pre-statement heap would still satisfy values this very
    /// statement removes (`UPDATE t SET id = 3, p = 1 WHERE id = 1`).
    #[allow(clippy::too_many_arguments)]
    pub(super) fn check_fks_in(
        &mut self,
        table: &str,
        meta: &TableMeta,
        doc: &Object,
        same_batch: &[Object],
        self_final: Option<&[Object]>,
    ) -> Result<()> {
        for fk in &meta.foreign_keys {
            let (col, rtable, rcol) = (&fk.col, &fk.ref_table, &fk.ref_col);
            let Some(v) = doc.get(col) else {
                continue;
            };
            if matches!(v, Value::Null) {
                continue; // NULL passes (MATCH SIMPLE semantics)
            }
            // cmp_values (not PartialEq): the engine's comparison semantics
            // treat Int(1)/Float(1.0)/Decimal(1) as equal; a join would
            // match them, so the FK must not reject what the rest of the
            // engine considers the same key.
            let matches_parent = |rd: &Object| {
                rd.get(rcol)
                    .map(|rv| Value::cmp_values(rv, v) == Ordering::Equal)
                    .unwrap_or(false)
            };
            if rtable == table {
                if let Some(final_state) = self_final {
                    if !final_state.iter().any(matches_parent) {
                        return err(format!(
                            "FOREIGN KEY constraint failed: {table}.{col} references \
                             {rtable}.{rcol}"
                        ));
                    }
                    continue;
                }
                if same_batch.iter().any(matches_parent) {
                    continue;
                }
            }
            let ref_docs = self.table_docs_cx(rtable)?;
            if !ref_docs.iter().any(matches_parent) {
                return err(format!(
                    "FOREIGN KEY constraint failed: {table}.{col} references {rtable}.{rcol}"
                ));
            }
        }
        Ok(())
    }

    /// Referential integrity on the *parent* side (RESTRICT): rows removed
    /// from `table` (DELETE, or UPDATE rows whose key value changes) may be
    /// referenced by child tables. `replacement_docs` carries the statement's
    /// surviving/updated rows of `table`: a parent-key value kept by any of
    /// them stays referenceable.
    /// [`check_fk_parent_delete_skipping`] with a child-table exclusion list: children
    /// named in `skip` are also being emptied by the same statement (e.g.
    /// `TRUNCATE parent, child`), so their rows cannot end up orphaned.
    /// `self_children` overrides the scan of `table`'s own rows with the
    /// statement's FINAL images (MERGE: the heap still shows pre-statement
    /// rows, so a self-reference created by the INSERT arm would be missed
    /// and one removed by the UPDATE arm wrongly reported).
    #[allow(clippy::too_many_arguments)]
    pub(super) fn check_fk_parent_delete_skipping(
        &mut self,
        table: &str,
        removed_docs: &[Object],
        replacement_docs: &[Object],
        skip: &[String],
        self_children: Option<&[Object]>,
    ) -> Result<()> {
        if removed_docs.is_empty() {
            return Ok(());
        }
        // Child FKs referencing `table`: (child_table, child_col, parent_col).
        let children: Vec<(String, String, String)> = self
            .tables
            .iter()
            .filter(|(child_name, _)| !skip.iter().any(|s| s.as_str() == child_name.as_str()))
            .flat_map(|(child_name, m)| {
                m.foreign_keys
                    .iter()
                    .filter(|fk| fk.ref_table == table)
                    .map(|fk| (child_name.clone(), fk.col.clone(), fk.ref_col.clone()))
                    .collect::<Vec<_>>()
            })
            .collect();
        if children.is_empty() {
            return Ok(());
        }
        let parent_cols: Vec<&String> = {
            let mut v: Vec<&String> = children.iter().map(|(_, _, rc)| rc).collect();
            v.sort();
            v.dedup();
            v
        };
        // Parent-key values that disappear with this statement.
        let mut lost: Vec<&Value> = Vec::new();
        for doc in removed_docs {
            for rc in &parent_cols {
                if let Some(v) = doc.get(rc.as_str()) {
                    let kept = replacement_docs.iter().any(|d| {
                        d.get(rc.as_str())
                            .map(|rv| Value::cmp_values(rv, v) == Ordering::Equal)
                            .unwrap_or(false)
                    });
                    if !matches!(v, Value::Null) && !kept && !lost.contains(&v) {
                        lost.push(v);
                    }
                }
            }
        }
        if lost.is_empty() {
            return Ok(());
        }
        // Non-unique referenced columns: a value can survive in rows this
        // statement never touched. `replacement_docs` only carries the
        // statement's own rows, so screen `lost` against the whole
        // pre-statement table: a value the removed set does not exhaust
        // (current count > removed count) is still referenceable. For
        // PK/UNIQUE referenced columns the counts are equal and this is a
        // no-op; for anything else it un-rejects legitimate deletes.
        {
            let current = self.table_docs_cx(table)?;
            let count_in = |docs: &[Object], rc: &str, v: &Value| {
                docs.iter()
                    .filter(|d| {
                        d.get(rc)
                            .map(|rv| Value::cmp_values(rv, v) == Ordering::Equal)
                            .unwrap_or(false)
                    })
                    .count()
            };
            lost.retain(|v| {
                !parent_cols
                    .iter()
                    .any(|rc| count_in(&current, rc, v) > count_in(removed_docs, rc, v))
            });
        }
        if lost.is_empty() {
            return Ok(());
        }
        for (child_table, child_col, rc) in &children {
            let fallback;
            let child_docs: &[Object] = if child_table == table {
                match self_children {
                    Some(docs) => docs,
                    None => {
                        fallback = self.table_docs_cx(child_table)?;
                        &fallback
                    }
                }
            } else {
                fallback = self.table_docs_cx(child_table)?;
                &fallback
            };
            let offender = child_docs.iter().any(|cd| {
                cd.get(child_col)
                    .map(|cv| {
                        !matches!(cv, Value::Null)
                            && lost
                                .iter()
                                .any(|lv| Value::cmp_values(cv, lv) == Ordering::Equal)
                    })
                    .unwrap_or(false)
            });
            if offender {
                return err(format!(
                    "FOREIGN KEY constraint failed: {child_table}.{child_col} references \
                     {table}.{rc}; delete/update the child rows first"
                ));
            }
        }
        Ok(())
    }

    /// CREATE VIEW: a named SELECT stored in the catalog namespace. Views
    /// have no storage, no indexes and no declared columns — a SELECT naming
    /// one expands to the stored query's rows (see `load_table_factor`).
    /// The body is dry-run once at create time so missing tables/columns
    /// fail loudly, and `CREATE OR REPLACE` re-points an existing view.
    // ===== Row triggers (CREATE TRIGGER / DROP TRIGGER) =====
    //
    /// CREATE MATERIALIZED VIEW: materialize the query into real storage
    /// and record the query for REFRESH. The query must be deterministic
    /// (no NEWID/RAND/wall-clock): REFRESH replays it on every node, and a
    /// nondeterministic result would silently fork the cluster's derived
    /// tables.
    pub(super) fn exec_create_matview(
        &mut self,
        view: sqlparser::ast::CreateView,
    ) -> Result<ExecOutcome> {
        if view.secure || view.with_no_schema_binding {
            return err("SECURE / WITH NO SCHEMA BINDING is not supported");
        }
        if !view.columns.is_empty() {
            return err("CREATE MATERIALIZED VIEW column list is not supported                  (alias the SELECT outputs)");
        }
        let name = obj_name(&view.name);
        if name.starts_with('#') {
            return err("temp materialized views (#name) are not supported");
        }
        if crate::useradmin::is_user_table(&name) {
            return err(format!(
                "materialized view name {name} is reserved for the user/role subsystem"
            ));
        }
        if crate::engine::is_system_table(&name) || is_internal_table(&name) {
            return err(format!(
                "materialized view name {name} is reserved for the engine"
            ));
        }
        if is_compat_view(&name) {
            return err(format!(
                "materialized view name {name} is reserved for the compatibility \
                 dictionary views"
            ));
        }
        if let Some(existing) = self.tables.get(&name) {
            if !view.or_replace {
                if view.if_not_exists {
                    return Ok(ExecOutcome::Affected(0));
                }
                return err(format!("materialized view {name} already exists"));
            }
            if !existing.is_matview() {
                return err(format!(
                    "{name} is not a materialized view; use DROP {} before \
                     CREATE OR REPLACE MATERIALIZED VIEW",
                    if existing.is_view() { "VIEW" } else { "TABLE" }
                ));
            }
        }
        let query = view.query.to_string();
        // Determinism gates — REFRESH replays this text on every node over
        // the same base data; a nondeterministic result diverges silently.
        if crate::stmt::mentions_wall_clock(&query) {
            return err(
                "materialized view queries cannot call wall-clock functions \
                 (NOW/SYSDATE/CURRENT_TIMESTAMP/GETDATE): every node would \
                 recompute a different value at REFRESH",
            );
        }
        if crate::stmt::mentions_nondet_call(&query) {
            return err(
                "materialized view queries cannot call NEWID()/NEWSEQUENTIALID()/RAND(): \
                 every node would recompute a different value at REFRESH",
            );
        }
        // A materialized view over another materialized view would make the
        // dump's replay order (and every refresh cascade) a dependency
        // puzzle; v1 refuses the nesting — query base tables (or views).
        {
            let objects = |n: &str| self.tables.contains_key(n);
            let stmt = sqlparser::ast::Statement::Query(view.query.clone());
            let Some(targets) = Database::stmt_read_targets(&stmt, &objects) else {
                return err("materialized view queries must be plain SELECTs over base tables");
            };
            if let Some(mv) = targets
                .iter()
                .find(|t| self.tables.get(*t).is_some_and(|m| m.is_matview()))
            {
                return err(format!(
                    "materialized view {name} cannot query materialized view {mv} \
                     (refresh cascades and dump ordering are not supported)"
                ));
            }
        }
        // Materialize now (same shape CTAS produces: columns from the query
        // outputs, schemaless rows).
        let ExecOutcome::Rows(r) = self.exec_query_cx((*view.query).clone())? else {
            return err("CREATE MATERIALIZED VIEW requires a SELECT");
        };
        {
            let mut seen = std::collections::BTreeSet::new();
            if let Some(dup) = r.columns.iter().find(|c| !seen.insert((*c).clone())) {
                return err(format!("duplicate column name: {dup}"));
            }
        }
        let docs: Vec<Object> = r
            .rows
            .into_iter()
            .map(|row| r.columns.iter().cloned().zip(row).collect())
            .collect();
        let columns = r.columns;
        // OR REPLACE swaps the definition and the content in one statement:
        // reuse the storage (rewrite_table frees + rebuilds), never drop and
        // recreate (a mid-statement failure would leave nothing behind).
        let mut meta = match self.tables.get(&name).cloned() {
            Some(prev) if prev.is_matview() => {
                let mut m = prev.as_ref().clone();
                m.columns = columns;
                m
            }
            Some(prev) => prev.as_ref().clone(),
            None => TableMeta {
                name: name.clone(),
                columns,
                ..Default::default()
            },
        };
        meta.matview_sql = Some(query.clone());
        self.rewrite_table(&name, &mut meta, docs)?;
        Ok(ExecOutcome::Affected(0))
    }

    /// REFRESH MATERIALIZED VIEW: re-execute the stored query and replace
    /// the stored rows wholesale (one rewrite = one atomic statement). The
    /// stored text was determinism-validated at CREATE; re-validate at
    /// REFRESH so a pre-validation volume cannot replay a divergent query.
    pub(super) fn exec_refresh_matview(&mut self, name: &str) -> Result<ExecOutcome> {
        let Some(meta) = self.tables.get(name).cloned() else {
            return err(format!("materialized view {name} does not exist"));
        };
        if !meta.is_matview() {
            return err(format!(
                "{name} is not a materialized view (it is a {})",
                if meta.is_view() { "view" } else { "table" }
            ));
        }
        let query = meta.matview_sql.clone().expect("checked above");
        if crate::stmt::mentions_wall_clock(&query) {
            return err("materialized view query calls wall-clock functions");
        }
        if crate::stmt::mentions_nondet_call(&query) {
            return err("materialized view query calls NEWID()/NEWSEQUENTIALID()/RAND()");
        }
        let parsed = Self::parse_classified(&query)?;
        let AnyStmt::Sql(stmt) = parsed.stmt else {
            return err("materialized view query must be a SELECT");
        };
        let Statement::Query(q) = *stmt else {
            return err("materialized view query must be a SELECT");
        };
        let ExecOutcome::Rows(r) = self.exec_query_cx(*q)? else {
            return err("materialized view query must be a SELECT");
        };
        {
            let mut seen = std::collections::BTreeSet::new();
            if let Some(dup) = r.columns.iter().find(|c| !seen.insert((*c).clone())) {
                return err(format!("duplicate column name: {dup}"));
            }
        }
        let docs: Vec<Object> = r
            .rows
            .into_iter()
            .map(|row| r.columns.iter().cloned().zip(row).collect())
            .collect();
        let mut meta = meta.as_ref().clone();
        meta.columns = r.columns;
        // One rewrite = one atomic statement: the old content and every
        // index tree rebuild together, readers never see a half-refreshed
        // view (MVCC snapshot reads see the pre-refresh content throughout).
        self.rewrite_table(name, &mut meta, docs)?;
        Ok(ExecOutcome::Affected(0))
    }

    pub(super) fn exec_create_view(
        &mut self,
        view: sqlparser::ast::CreateView,
    ) -> Result<ExecOutcome> {
        if view.materialized {
            return self.exec_create_matview(view);
        }
        if view.secure || view.with_no_schema_binding {
            return err("SECURE / WITH NO SCHEMA BINDING views are not supported");
        }
        if !view.columns.is_empty() {
            // Column aliases would need per-row projection renaming; the
            // projection's own aliases already name the columns.
            return err("CREATE VIEW column list is not supported (alias the projection)");
        }
        let name = obj_name(&view.name);
        if name.starts_with('#') {
            return err("temp views (#name) are not supported");
        }
        if crate::useradmin::is_user_table(&name) {
            return err(format!(
                "view name {name} is reserved for the user/role subsystem"
            ));
        }
        if crate::engine::is_system_table(&name) {
            return err(format!("view name {name} is reserved for the engine"));
        }
        if is_compat_view(&name) {
            // Same shadow rule as CREATE TABLE: the compatibility dictionary
            // views always win at FROM-resolution, so a view created under
            // one of these names would be unreachable.
            return err(format!(
                "view name {name} is reserved for the compatibility dictionary views"
            ));
        }
        let exists = self.tables.contains_key(&name);
        if exists && !view.or_replace {
            if view.if_not_exists {
                return Ok(ExecOutcome::Affected(0));
            }
            return err(format!("view {name} already exists"));
        }
        // Views over the user/role tables are NOT refused here: dumps and
        // join snapshots from older volumes may legitimately contain one,
        // and refusing would break the whole replay. The leak is closed on
        // the READ side instead — the server's authorize expands view base
        // tables and refuses user-table bases to non-admins (same rule the
        // write path applies), so such a view is admin-readable only.
        if exists {
            let meta = self.tables.get(&name).expect("checked just above");
            if !meta.is_view() {
                return err(format!(
                    "{name} is a table; use DROP TABLE before CREATE VIEW"
                ));
            }
        }
        let body = format!("{}", view.query);
        // Cycle guard: with OR REPLACE the old view still resolves during
        // the dry-run below, and a replacement body can point back at
        // itself through other views (a→b, then `CREATE OR REPLACE VIEW
        // a AS SELECT * FROM b`). A direct self-reference is rejected up
        // front; for OR REPLACE the view→view edges reachable from the
        // new body must never return to `name` — otherwise every later
        // SELECT would recurse until the expansion budget trips.
        {
            let objects = |n: &str| self.tables.contains_key(n);
            let mut refs = Vec::new();
            let mut stmts = Parser::parse_sql(&GenericDialect {}, &body)
                .map_err(|e| SqlError::Parse(e.to_string()))?;
            if let Statement::Query(q) = stmts.swap_remove(0) {
                if walk_query(&q, &mut refs, &objects).is_none() {
                    return err("view body cannot be classified");
                }
                if refs.iter().any(|r| r == &name) {
                    return err(format!("view {name} cannot reference itself"));
                }
                if exists {
                    let mut stack = refs.clone();
                    let mut seen = std::collections::BTreeSet::new();
                    while let Some(next) = stack.pop() {
                        if next == name {
                            return err(format!(
                                "view {name} cannot reference itself (through {next})"
                            ));
                        }
                        if !seen.insert(next.clone()) {
                            continue;
                        }
                        if let Some(deeper) = self.view_base_tables(&next) {
                            stack.extend(deeper);
                        }
                    }
                }
            } else {
                return err("view body must be a SELECT");
            }
        }
        // Dry-run the body once: missing tables/columns/unsupported shapes
        // fail at CREATE time, not at first SELECT. The body executes as a
        // plain SELECT over the CURRENT catalog (a self-reference errors —
        // the name does not resolve yet — which is exactly the cycle check).
        // The view under construction consumes one expansion level of its
        // own, so a chain that would be too deep to SELECT is also too deep
        // to CREATE (creatable implies selectable).
        {
            let _guard = enter_view_expansion()?;
            let q = Self::plain_read_query(&Self::parse_classified(&body)?)?;
            self.exec_query_cx(q)?;
        }
        let meta = TableMeta {
            name: name.clone(),
            view_sql: Some(body),
            ..Default::default()
        };
        // Keep the old entry (or absence) so a failed catalog save leaves
        // the in-memory catalog untouched — a ghost view that only exists
        // here would persist on the next unrelated write and diverge every
        // peer (same snapshot discipline as exec_create/exec_drop).
        let prev = self.tables.insert(name.clone(), std::sync::Arc::new(meta));
        if let Err(e) = self.save_catalog() {
            match prev {
                Some(old) => {
                    self.tables.insert(name, old);
                }
                None => {
                    self.tables.remove(&name);
                }
            }
            return Err(e);
        }
        Ok(ExecOutcome::Affected(0))
    }

    pub(super) fn exec_create(
        &mut self,
        create: sqlparser::ast::CreateTable,
    ) -> Result<ExecOutcome> {
        use sqlparser::ast::ColumnOption as CO;
        let name = obj_name(&create.name);
        if name.starts_with('#') {
            // T-SQL temp tables are session-scoped; making them persistent
            // global tables would silently change their lifetime/visibility.
            return err("temp tables (#name/##name) are not supported");
        }
        if crate::useradmin::is_user_table(&name) && !self.internal_ddl {
            return err(format!(
                "table name {name} is reserved for the user/role subsystem"
            ));
        }
        if crate::engine::is_system_table(&name) && !self.internal_ddl {
            // Squatting an engine system name would break replication,
            // digests and pub/sub on this database (the tables are excluded
            // from dumps by name). Only the internal replay path may create
            // reserved names, and it never creates these.
            return err(format!("table name {name} is reserved for the engine"));
        }
        if is_compat_view(&name) && !self.internal_ddl {
            // The dictionary/compat views always shadow a same-named table
            // at FROM-resolution (DUAL/sqlite_master/information_schema and
            // the Oracle dictionary spellings): a table created under one of
            // these names accepts writes but every SELECT reads the
            // dictionary instead — data that can never be read back. Refuse
            // at CREATE instead (existing pre-guard tables keep their
            // shadowed behavior).
            return err(format!(
                "table name {name} is reserved for the compatibility dictionary views"
            ));
        }
        if self.tables.contains_key(&name) {
            if create.if_not_exists {
                return Ok(ExecOutcome::Affected(0));
            }
            return err(format!("table {name} already exists"));
        }
        // Clause-level guard: a CREATE TABLE shape the engine cannot honor
        // must be refused, not silently degraded (`LIKE` used to create a
        // zero-column table; OR REPLACE ignored the replacement semantics).
        if create.like.is_some() || create.clone.is_some() {
            return err("CREATE TABLE ... LIKE/CLONE is not supported");
        }
        if create.or_replace {
            return err("CREATE OR REPLACE TABLE is not supported (drop the table first)");
        }
        if create.external
            || create.global.is_some()
            || create.transient
            || create.volatile
            || create.iceberg
            || !matches!(
                create.table_options,
                sqlparser::ast::CreateTableOptions::None
            )
            || create.cluster_by.is_some()
            || create.partition_by.is_some()
            || create.order_by.is_some()
        {
            return err("CREATE TABLE storage/partitioning options are not supported");
        }
        if create.strict || create.comment.is_some() {
            return err("CREATE TABLE engine/comment options are not supported");
        }
        // Storage/layout clauses the GenericDialect parses but the engine
        // cannot honor. They used to be dropped silently (same red-line
        // class as the options above): inheriting columns, rowid layout,
        // Hive/Redshift placement and partition bounds all change what the
        // statement means, so refusing loudly is the only honest answer.
        if create.inherits.is_some()
            || create.without_rowid
            || create.on_commit.is_some()
            || create.on_cluster.is_some()
            || create.partition_of.is_some()
            || create.for_values.is_some()
            || create.location.is_some()
            || create.file_format.is_some()
            || create.clustered_by.is_some()
            || !matches!(
                create.hive_distribution,
                sqlparser::ast::HiveDistributionStyle::NONE
            )
            || create.hive_formats.is_some()
            || create.diststyle.is_some()
            || create.distkey.is_some()
            || create.sortkey.is_some()
            || create.backup.is_some()
        {
            return err("CREATE TABLE storage/partitioning options are not supported");
        }
        // CREATE TABLE ... AS SELECT: shape and rows come from the query.
        // (`temporary` is accepted and treated as a regular table.)
        if let Some(q) = &create.query {
            let ExecOutcome::Rows(r) = self.exec_query_cx(q.as_ref().clone())? else {
                return err("CREATE TABLE AS requires a SELECT");
            };
            // An explicit column list alongside AS SELECT would rename the
            // outputs (and its constraints would apply) — silently dropping
            // it shaped the table after the query instead. Refuse loudly
            // rather than ignore half the statement.
            if !create.columns.is_empty() {
                return err(
                    "CREATE TABLE (…) AS SELECT with an explicit column list is not supported \
                     (omit the list, or alias the SELECT outputs)",
                );
            }
            let docs: Vec<Object> = r
                .rows
                .into_iter()
                .map(|row| r.columns.iter().cloned().zip(row).collect())
                .collect();
            // Duplicate output names would collapse two logical columns into
            // one document key (the later value silently wins) — same rule
            // as the explicit column list below.
            {
                let mut seen = std::collections::BTreeSet::new();
                if let Some(dup) = r.columns.iter().find(|c| !seen.insert((*c).clone())) {
                    return err(format!("duplicate column name: {dup}"));
                }
            }
            let mut meta = TableMeta {
                name: name.clone(),
                columns: r.columns,
                ..Default::default()
            };
            self.rewrite_table(&name, &mut meta, docs)?;
            return Ok(ExecOutcome::Affected(0));
        }
        let columns: Vec<String> = create
            .columns
            .iter()
            .map(|c| c.name.value.clone())
            .collect();
        // Duplicate column names collapse two logical columns into one
        // document key (the later one silently wins); reject like SQLite.
        {
            let mut seen = std::collections::BTreeSet::new();
            if let Some(dup) = columns.iter().find(|c| !seen.insert((*c).clone())) {
                return err(format!("duplicate column name: {dup}"));
            }
        }
        let mut meta = TableMeta {
            name: name.clone(),
            columns,
            ..Default::default()
        };
        for col in &create.columns {
            for opt in &col.options {
                match &opt.option {
                    CO::PrimaryKey(pk) => {
                        reject_constraint_decorations(
                            "PRIMARY KEY",
                            pk.characteristics.as_ref(),
                            None,
                            None,
                        )?;
                        // A second PK declaration silently replaced the
                        // first (its uniqueness tree was never built).
                        if meta.primary_key.replace(col.name.value.clone()).is_some() {
                            return err("table has more than one primary key".to_string());
                        }
                    }
                    CO::Unique(u) => {
                        reject_constraint_decorations(
                            "UNIQUE",
                            u.characteristics.as_ref(),
                            Some(&u.nulls_distinct),
                            None,
                        )?;
                        // Deduplicate like the table-level constraint below:
                        // `a INT UNIQUE UNIQUE` pushed the column twice, the
                        // tree-maintenance loop inserted the same key twice,
                        // and the table was permanently unwritable.
                        let col = col.name.value.clone();
                        if !meta.unique.contains(&col) {
                            meta.unique.push(col.clone());
                        }
                        if !meta.constraint_unique.contains(&col) {
                            meta.constraint_unique.push(col);
                        }
                    }
                    CO::NotNull => meta.not_null.push(col.name.value.clone()),
                    CO::Null => {}
                    CO::Default(e) => {
                        meta.defaults
                            .push((col.name.value.clone(), default_expr_text(e)));
                    }
                    CO::Check(c) => {
                        if expr_calls_wall_clock(&c.expr) {
                            return err("CHECK constraints cannot call wall-clock or random \
                                 functions (NOW/SYSDATE/CURRENT_TIMESTAMP/GETDATE/RAND); \
                                 every node would evaluate a different value");
                        }
                        // Unknown column references resolve to NULL at
                        // evaluation time, so a typo'd CHECK would never
                        // fire — validate at DDL time.
                        let mut refs = Vec::new();
                        check_expr_column_refs(&c.expr, &mut refs)?;
                        for r in refs {
                            if !meta.columns.contains(&r) {
                                return err(format!(
                                    "CHECK constraint references unknown column {r}"
                                ));
                            }
                        }
                        meta.checks.push(format!("{}", c.expr))
                    }
                    CO::ForeignKey(fk) => {
                        reject_constraint_decorations(
                            "FOREIGN KEY",
                            fk.characteristics.as_ref(),
                            None,
                            fk.match_kind.as_ref(),
                        )?;
                        if fk.referred_columns.len() != 1 {
                            // Silently taking the first referred column
                            // quietly changed the constraint's meaning.
                            return err("column-level FOREIGN KEY must reference a single column");
                        }
                        let (Some(rt), Some(rc)) = (
                            fk.foreign_table.0.last().map(|p| match p {
                                ObjectNamePart::Identifier(i) => i.value.clone(),
                                other => other.to_string(),
                            }),
                            fk.referred_columns.first().map(|c| c.value.clone()),
                        ) else {
                            return err("FOREIGN KEY requires a table and column");
                        };
                        meta.foreign_keys.push(FkRef {
                            col: col.name.value.clone(),
                            ref_table: rt,
                            ref_col: rc,
                            on_delete: fk_action_of(fk.on_delete.as_ref()),
                            on_update: fk_action_of(fk.on_update.as_ref()),
                        });
                    }
                    CO::DialectSpecific(tokens) => {
                        let text = tokens
                            .iter()
                            .map(|t| t.to_string().to_uppercase())
                            .collect::<Vec<_>>()
                            .join(" ");
                        if text.contains("AUTOINCREMENT") || text.contains("AUTO_INCREMENT") {
                            if is_guid_type(&col.data_type) {
                                // GUID columns treat the auto flag as
                                // UUIDv7 generation instead of max+1.
                                meta.autoguid = Some(col.name.value.clone());
                            } else {
                                meta.autoinc = Some(col.name.value.clone());
                            }
                        } else {
                            return err("unsupported column constraint");
                        }
                    }
                    _ => return err("unsupported column constraint"),
                }
            }
        }
        // Table-level constraints.
        for c in &create.constraints {
            use sqlparser::ast::TableConstraint as TC;
            match c {
                TC::Unique(u) => {
                    reject_constraint_decorations(
                        "UNIQUE",
                        u.characteristics.as_ref(),
                        Some(&u.nulls_distinct),
                        None,
                    )?;
                    // A multi-column constraint would need a composite unique
                    // tree; declaring it as per-column uniques silently
                    // changes the semantics (any single column becomes
                    // unique), so refuse and point at the supported form.
                    if u.columns.len() != 1 {
                        return err("composite UNIQUE table constraints are not supported \
                             (use CREATE UNIQUE INDEX)");
                    }
                    for ic in &u.columns {
                        if !matches!(ic.column.expr, SqlExpr::Identifier(_)) {
                            return err("expression UNIQUE constraints are not supported \
                                 (constraint a plain column)");
                        }
                        let col = expr_name(&ic.column.expr);
                        if !meta.columns.contains(&col) {
                            // A ghost column builds a constraint tree that can
                            // never fire and vanishes from dump_script.
                            return err(format!("UNIQUE column {col} does not exist"));
                        }
                        if !meta.unique.contains(&col) {
                            meta.unique.push(col.clone());
                            meta.constraint_unique.push(col);
                        }
                    }
                }
                TC::PrimaryKey(pk) => {
                    reject_constraint_decorations(
                        "PRIMARY KEY",
                        pk.characteristics.as_ref(),
                        None,
                        None,
                    )?;
                    // DocSQL has single-column primary keys; a composite key
                    // must not silently degrade to its first column.
                    if pk.columns.len() != 1 {
                        return err("composite PRIMARY KEY is not supported \
                             (declare a single-column PRIMARY KEY or use CREATE UNIQUE INDEX)");
                    }
                    if let Some(ic) = pk.columns.first() {
                        if !matches!(ic.column.expr, SqlExpr::Identifier(_)) {
                            return err("expression PRIMARY KEY constraints are not supported \
                                 (constraint a plain column)");
                        }
                        let name = expr_name(&ic.column.expr);
                        if !meta.columns.contains(&name) {
                            return err(format!("PRIMARY KEY column {name} does not exist"));
                        }
                        if meta.primary_key.replace(name).is_some() {
                            return err("table has more than one primary key".to_string());
                        }
                    }
                }
                TC::Check(chk) => {
                    if expr_calls_wall_clock(&chk.expr) {
                        return err("CHECK constraints cannot call wall-clock functions \
                             (NOW/SYSDATE/CURRENT_TIMESTAMP); every node would \
                             evaluate a different instant");
                    }
                    let mut refs = Vec::new();
                    check_expr_column_refs(&chk.expr, &mut refs)?;
                    for r in refs {
                        if !meta.columns.contains(&r) {
                            return err(format!("CHECK constraint references unknown column {r}"));
                        }
                    }
                    meta.checks.push(format!("{}", chk.expr))
                }
                TC::ForeignKey(fk) => {
                    reject_constraint_decorations(
                        "FOREIGN KEY",
                        fk.characteristics.as_ref(),
                        None,
                        fk.match_kind.as_ref(),
                    )?;
                    let Some(rt) = fk.foreign_table.0.last().map(|p| match p {
                        ObjectNamePart::Identifier(i) => i.value.clone(),
                        other => other.to_string(),
                    }) else {
                        return err("FOREIGN KEY requires a table");
                    };
                    // A composite FK silently split into per-column pairs
                    // enforces a different (weaker) constraint — each column
                    // independently instead of the tuple. Refuse like the
                    // composite PK/UNIQUE forms.
                    if fk.columns.len() != 1 {
                        return err("composite FOREIGN KEY constraints are not supported");
                    }
                    // zip 会静默截断较短的列清单,约束悄悄少一列 —— 列数
                    // 不匹配必须显式报错(子句级静默忽略是红线)。
                    if fk.columns.len() != fk.referred_columns.len() {
                        return err(format!(
                            "FOREIGN KEY column count mismatch: {} referencing columns \
                             vs {} referred columns",
                            fk.columns.len(),
                            fk.referred_columns.len()
                        ));
                    }
                    for (lc, rc) in fk.columns.iter().zip(fk.referred_columns.iter()) {
                        if !meta.columns.contains(&lc.value) {
                            return err(format!("FOREIGN KEY column {} does not exist", lc.value));
                        }
                        meta.foreign_keys.push(FkRef {
                            col: lc.value.clone(),
                            ref_table: rt.clone(),
                            ref_col: rc.value.clone(),
                            on_delete: fk_action_of(fk.on_delete.as_ref()),
                            on_update: fk_action_of(fk.on_update.as_ref()),
                        });
                    }
                }
                other => return err(format!("unsupported table constraint: {other}")),
            }
        }
        // Constraint columns get their B+ trees right away: duplicate
        // enforcement and index probes depend on them existing. The trees
        // ride the SAME write unit as the catalog entry (the shape
        // `exec_create_index` settled on): committing the tree first and
        // Forward-declared FKs (referencing a table that does not exist
        // yet) can form DECLARATION cycles the dump's topological order
        // cannot break — the fallback emits cycle members in catalog
        // order, and a replay then inserts child rows before their
        // parents exist (restore fails mid-flight). Self-references are
        // fine: the table being created is its own parent. When the
        // parent exists, the referenced column must be one of its
        // declared columns — an undeclared field would silently match
        // nothing and every child write would fail the constraint.
        for fk in &meta.foreign_keys {
            let (fc, rt, rc) = (&fk.col, &fk.ref_table, &fk.ref_col);
            if rt == &name {
                continue;
            }
            let Some(parent) = self.tables.get(rt) else {
                return err(format!(
                    "FOREIGN KEY on {name}.{fc} references table {rt}, which does not exist \
                     (create {rt} first)"
                ));
            };
            if parent.is_view() {
                return err(format!(
                    "FOREIGN KEY on {name}.{fc} cannot reference the VIEW {rt}"
                ));
            }
            if !parent.columns.contains(rc) {
                return err(format!(
                    "FOREIGN KEY on {name}.{fc} references unknown column {rt}.{rc}"
                ));
            }
        }
        // saving the catalog in a second transaction left a crash window
        // holding a committed tree no catalog references — those pages would
        // leak for good, nothing would ever free them.
        let constraint_cols: Vec<String> = meta
            .primary_key
            .iter()
            .chain(meta.unique.iter())
            .cloned()
            .collect();
        let mut tx = self.pager.begin_tx();
        if !constraint_cols.is_empty() {
            let roots = self.build_trees(
                &mut tx,
                &[],
                &constraint_cols,
                &constraint_cols
                    .iter()
                    .map(|c| vec![c.clone()])
                    .collect::<Vec<_>>(),
                &vec![None; constraint_cols.len()],
                &vec![true; constraint_cols.len()],
                &vec![None; constraint_cols.len()],
            )?;
            meta.index_roots = roots;
        }
        self.tables.insert(name.clone(), std::sync::Arc::new(meta));
        if let Err(e) = self
            .save_catalog_into(&mut tx)
            .and_then(|()| self.commit_pager_tx(tx))
        {
            // The tx dropped without committing: staged tree pages went back
            // to the reusable pool, and the in-memory entry must not keep
            // pointing at them.
            self.tables.remove(&name);
            return Err(e);
        }
        Ok(ExecOutcome::Affected(0))
    }
}
