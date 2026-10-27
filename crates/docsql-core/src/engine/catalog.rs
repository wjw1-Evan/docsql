use super::*;

impl Database {
    /// Commit a statement's pager tx: while an explicit SQL transaction or a
    /// fused write unit is open the WAL fsync is deferred — the whole batch
    /// pays one flush at COMMIT / [`WriteUnit::end`], turning N fsyncs per
    /// transaction into one.
    pub(super) fn commit_pager_tx(&mut self, tx: crate::pager::Tx) -> Result<u64> {
        if self.async_commit || self.tx_snapshot.is_some() || self.write_unit > 0 {
            self.pending_sync = true;
            self.pager.commit_tx_deferred(tx).map_err(SqlError::from)
        } else {
            // The synchronous commit fsyncs the WAL and flushes every page
            // image queued before it, so earlier deferred commits are durable
            // too — nothing is left pending. On failure the flag stays set:
            // the earlier deferred commits may not be durable yet.
            let result = self.pager.commit_tx(tx).map_err(SqlError::from);
            if result.is_ok() {
                self.pending_sync = false;
            }
            result
        }
    }

    /// Open fused write unit: every statement committed until
    /// [`WriteUnit::end`] defers its WAL fsync, and the unit's single `end`
    /// fsync makes them durable together — a crash mid-unit loses them all,
    /// never a prefix. Servers use this to land a write and its replication
    /// bookkeeping (catch-up journal entry on the origin, position update on
    /// the receiver) in one fsync instead of two, closing the
    /// "bookkeeping lags the data" crash window entirely.
    ///
    /// Unlike BEGIN it takes no rollback snapshot; a failed trailing
    /// statement leaves the earlier ones applied-but-unsynced, so callers
    /// that want the prefix to survive must call `end` on error paths too.
    /// Dropping the guard without `end` only forgets the fusion (the flag
    /// clears on drop; the next durable statement fsyncs anything deferred
    /// before it), so a leaked unit can never wedge durability.
    pub fn write_unit(&mut self) -> WriteUnit<'_> {
        self.write_unit += 1;
        WriteUnit { db: self }
    }

    /// Enable async-commit mode: statement commits skip the WAL fsync and a
    /// caller-provided flusher (see `sync_pending`) batches them.
    pub fn set_async_commit(&mut self, on: bool) {
        self.async_commit = on;
    }

    /// True when deferred commits still need a WAL fsync.
    pub fn has_pending_sync(&self) -> bool {
        self.pending_sync
    }

    /// Flush deferred commits (the background flusher's entry point).
    pub fn sync_pending(&mut self) -> Result<()> {
        if !self.pending_sync {
            return Ok(());
        }
        if self.tx_snapshot.is_some() {
            // Mid-transaction: these deferred commits belong to an open SQL
            // transaction. Appending the durable fence now would make a
            // crash replay a prefix of it; the transaction's own COMMIT
            // fences the whole batch, ROLLBACK discards it.
            return Ok(());
        }
        self.pager.sync_wal().map_err(SqlError::from)?;
        self.pending_sync = false;
        Ok(())
    }
    pub fn open(path: &std::path::Path) -> Result<Database> {
        let pager = Pager::open(path)?;
        // Reserve pages 0 (header) and 1 (catalog); allocate 1 if missing.
        while pager.num_pages() <= CATALOG_PAGE {
            let mut tx = pager.begin_tx();
            let _ = pager.num_pages();
            pager.allocate_page(&mut tx)?;
            pager.commit_tx(tx)?;
        }
        let mut tables = std::collections::BTreeMap::new();
        let mut triggers = std::collections::BTreeMap::new();
        // New format: a magic-headed chain of pages. Legacy databases keep a
        // raw encoded catalog on page 1 — decode it in place; the first
        // catalog save rewrites it in the new format.
        let raw = pager.read_page(CATALOG_PAGE)?.to_vec();
        let cat_bytes: Option<Vec<u8>> = if raw.starts_with(CATALOG_MAGIC) {
            let total = u32::from_le_bytes(raw[8..12].try_into().expect("header fits")) as usize;
            let mut data = raw[CATALOG_HDR..].to_vec();
            let mut next = u32::from_le_bytes(raw[12..16].try_into().expect("header fits"));
            // A cyclic chain of valid page ids would loop forever appending
            // 4 KB per hop, so bound the walk by the page count.
            let mut hops = 0usize;
            while next != 0 {
                if next >= pager.num_pages() {
                    return err("catalog chain is corrupt (dangling overflow page)");
                }
                hops += 1;
                if hops > pager.num_pages() as usize {
                    return err("catalog chain is corrupt (cycle)");
                }
                let page = pager.read_page(next)?.to_vec();
                if !page.starts_with(CATALOG_MAGIC) {
                    return err("catalog chain is corrupt (overflow page without magic)");
                }
                data.extend_from_slice(&page[CATALOG_HDR..]);
                next = u32::from_le_bytes(page[12..16].try_into().expect("header fits"));
            }
            if data.len() < total {
                return err("catalog chain is corrupt (payload shorter than header length)");
            }
            data.truncate(total);
            Some(data)
        } else if raw.iter().any(|&b| b != 0) {
            Some(raw)
        } else {
            None
        };
        if let Some(bytes) = cat_bytes {
            let (v, _) = encode::decode_prefix(&bytes)?;
            if let Value::Object(o) = v {
                if let Some(Value::Object(t)) = o.get("tables") {
                    for (name, meta) in t {
                        if let Value::Object(m) = meta {
                            let columns = match m.get("columns") {
                                Some(Value::Array(a)) => a
                                    .iter()
                                    .filter_map(|x| x.as_str().map(String::from))
                                    .collect(),
                                _ => vec![],
                            };
                            let pages = match m.get("pages") {
                                Some(Value::Array(a)) => a
                                    .iter()
                                    .filter_map(|x| x.as_i64().map(|i| i as u32))
                                    .collect(),
                                _ => vec![],
                            };
                            let primary_key = m
                                .get("primary_key")
                                .and_then(|v| v.as_str())
                                .map(String::from);
                            let unique = str_list(m, "unique");
                            let not_null = str_list(m, "not_null");
                            let autoinc =
                                m.get("autoinc").and_then(|v| v.as_str()).map(String::from);
                            let autoguid =
                                m.get("autoguid").and_then(|v| v.as_str()).map(String::from);
                            let view_sql =
                                m.get("view_sql").and_then(|v| v.as_str()).map(String::from);
                            let matview_sql = m
                                .get("matview_sql")
                                .and_then(|v| v.as_str())
                                .map(String::from);
                            let index_defs = match m.get("index_defs") {
                                Some(Value::Array(a)) => a
                                    .iter()
                                    .filter_map(|x| match x {
                                        Value::Array(t) => match (
                                            t.first().and_then(|v| v.as_str()),
                                            t.get(1).and_then(|v| v.as_str()),
                                            t.get(2).and_then(|v| v.as_bool()),
                                        ) {
                                            (Some(n), Some(c), Some(u)) => {
                                                // Fourth element (optional,
                                                // new-format catalogs): the
                                                // full ordered column list of
                                                // a possibly composite index.
                                                // Legacy three-element entries
                                                // are single-column.
                                                let columns = match t.get(3) {
                                                    Some(Value::Array(a)) => {
                                                        // An EMPTY array must
                                                        // not survive: IndexDef
                                                        // assumes columns
                                                        // non-empty (`column()`
                                                        // indexes [0]) and
                                                        // panics on the next
                                                        // catalog save.
                                                        let cols: Vec<_> = a
                                                            .iter()
                                                            .filter_map(|v| {
                                                                v.as_str().map(String::from)
                                                            })
                                                            .collect();
                                                        if cols.is_empty() {
                                                            vec![c.to_string()]
                                                        } else {
                                                            cols
                                                        }
                                                    }
                                                    _ => vec![c.to_string()],
                                                };
                                                // Optional 5th element (design
                                                // 005): the JSON path. Older
                                                // catalogs lack it; readers
                                                // that predate path indexes
                                                // ignore the extra element, so
                                                // the tuple stays bidirectionally
                                                // compatible with zero migration.
                                                let path = t
                                                    .get(4)
                                                    .and_then(|v| v.as_str())
                                                    .map(String::from);
                                                // Optional 6th element: the
                                                // partial-index predicate text.
                                                let pred = t
                                                    .get(5)
                                                    .and_then(|v| v.as_str())
                                                    .map(String::from);
                                                Some(IndexDef {
                                                    name: n.to_string(),
                                                    columns,
                                                    unique: u,
                                                    path,
                                                    pred,
                                                })
                                            }
                                            _ => None,
                                        },
                                        _ => None,
                                    })
                                    .collect(),
                                _ => vec![],
                            };
                            // Index names derive from the persisted defs.
                            let indexes = index_defs
                                .iter()
                                .map(|d| d.name.clone())
                                .collect::<Vec<_>>();
                            let constraint_unique = str_list(m, "constraint_unique");
                            let defaults = match m.get("defaults") {
                                Some(Value::Array(a)) => a
                                    .iter()
                                    .filter_map(|x| match x {
                                        Value::Array(pair) => match (
                                            pair.first().and_then(|c| c.as_str()),
                                            pair.get(1).and_then(|e| e.as_str()),
                                        ) {
                                            (Some(c), Some(e)) => {
                                                Some((c.to_string(), e.to_string()))
                                            }
                                            _ => None,
                                        },
                                        _ => None,
                                    })
                                    .collect(),
                                _ => vec![],
                            };
                            let checks = str_list(m, "checks");
                            let index_roots = match m.get("index_roots") {
                                Some(Value::Object(o)) => o
                                    .iter()
                                    .filter_map(|(c, v)| v.as_i64().map(|i| (c.clone(), i as u32)))
                                    .collect(),
                                _ => std::collections::BTreeMap::new(),
                            };
                            // Recycled overflow-chain pages (optional key:
                            // catalogs predating overflow documents lack it).
                            let overflow_free = match m.get("overflow_free") {
                                Some(Value::Array(a)) => a
                                    .iter()
                                    .filter_map(|v| v.as_i64().map(|i| i as u32))
                                    .collect(),
                                _ => vec![],
                            };
                            let foreign_keys = match m.get("foreign_keys") {
                                Some(Value::Array(a)) => a
                                    .iter()
                                    .filter_map(|x| match x {
                                        Value::Array(t) => match (
                                            t.first().and_then(|v| v.as_str()),
                                            t.get(1).and_then(|v| v.as_str()),
                                            t.get(2).and_then(|v| v.as_str()),
                                        ) {
                                            (Some(c), Some(rt), Some(rc)) => Some((
                                                c.to_string(),
                                                rt.to_string(),
                                                rc.to_string(),
                                            )),
                                            _ => None,
                                        },
                                        _ => None,
                                    })
                                    .collect(),
                                _ => vec![],
                            };
                            tables.insert(
                                name.clone(),
                                std::sync::Arc::new(TableMeta {
                                    name: name.clone(),
                                    columns,
                                    pages,
                                    primary_key,
                                    unique,
                                    not_null,
                                    autoinc,
                                    autoguid,
                                    view_sql,
                                    matview_sql,
                                    indexes,
                                    index_defs,
                                    constraint_unique,
                                    defaults,
                                    checks,
                                    foreign_keys,
                                    index_roots,
                                    overflow_free,
                                    dotted_keys: matches!(
                                        m.get("dotted_keys"),
                                        Some(Value::Bool(true))
                                    ),
                                }),
                            );
                        }
                    }
                }
                // Row triggers (see [`TriggerDef`]). Same tolerant decode as
                // the table metas: a malformed entry is skipped, not fatal —
                // the encoder above only ever writes the shapes decoded here.
                if let Some(Value::Object(ts)) = o.get("triggers") {
                    for (name, td) in ts {
                        let Some(Value::Object(t)) = Some(td) else {
                            continue;
                        };
                        let (Some(table), Some(before), Some(insert), Some(delete), Some(update)) = (
                            t.get("table").and_then(|v| v.as_str()),
                            t.get("before").and_then(|v| v.as_bool()),
                            t.get("insert").and_then(|v| v.as_bool()),
                            t.get("delete").and_then(|v| v.as_bool()),
                            t.get("update").and_then(|v| v.as_bool()),
                        ) else {
                            continue;
                        };
                        let update_of = match t.get("update_of") {
                            Some(Value::Array(a)) => Some(
                                a.iter()
                                    .filter_map(|v| v.as_str().map(String::from))
                                    .collect(),
                            ),
                            _ => None,
                        };
                        let when = t.get("when").and_then(|v| v.as_str()).map(String::from);
                        let body = match t.get("body") {
                            Some(Value::Array(a)) => a
                                .iter()
                                .filter_map(|x| match x {
                                    Value::Array(parts) => match (
                                        parts.first().and_then(|v| v.as_str()),
                                        parts.get(1).and_then(|v| v.as_str()),
                                    ) {
                                        (Some("sql"), Some(text)) => {
                                            Some(TriggerAction::Sql(text.to_string()))
                                        }
                                        (Some("raise"), Some(msg)) => Some(TriggerAction::Raise {
                                            fail: matches!(parts.get(2), Some(Value::Bool(true))),
                                            msg: msg.to_string(),
                                        }),
                                        _ => None,
                                    },
                                    _ => None,
                                })
                                .collect(),
                            _ => vec![],
                        };
                        triggers.insert(
                            name.clone(),
                            TriggerDef {
                                name: name.clone(),
                                table: table.to_string(),
                                before,
                                insert,
                                delete,
                                update,
                                update_of,
                                when,
                                body,
                            },
                        );
                    }
                }
            }
        }
        Ok(Database {
            pager: std::sync::Arc::new(pager),
            tables,
            triggers,
            trigger_depth: 0,
            async_commit: false,
            pending_sync: false,
            write_unit: 0,
            savepoints: Vec::new(),
            tx_snapshot: None,
            stmt_deadline: StmtDeadline::default(),
            autoinc_cache: std::collections::HashMap::new(),
            resolved_sql: None,
            last_insert_id: None,
            journal_next: None,
            journal_bytes: None,
            cluster_id: None,
            internal_ddl: false,
        })
    }

    pub fn in_memory() -> Result<Database> {
        let dir = tempfile::tempdir().map_err(|e| SqlError::Message(e.to_string()))?;
        // Leak: process-lifetime scratch DB, fine for embedded use/tests.
        let path = dir.keep().join("mem.db");
        Database::open(&path)
    }

    /// Record a statement's resulting heap layout and index roots in the
    /// catalog inside `tx`. On save failure the catalog map is restored to
    /// the pre-statement truth: the tx is dropped on that path, so its
    /// staged pages never reach the data file, and a map left pointing at
    /// them would let a later successful save persist ghost pages.
    pub(super) fn sync_table_layout(
        &mut self,
        tx: &mut crate::pager::Tx,
        tname: &str,
        pages: Vec<u32>,
        roots: std::collections::BTreeMap<String, u32>,
        overflow_free: Vec<u32>,
    ) -> Result<()> {
        let prev = self.tables.get(tname).map(|m| {
            (
                m.pages.clone(),
                m.index_roots.clone(),
                m.overflow_free.clone(),
            )
        });
        if let Some(m) = self.catalog_mut(tname) {
            m.pages = pages;
            m.index_roots = roots;
            m.overflow_free = overflow_free;
        }
        if let Err(e) = self.save_catalog_into(tx) {
            if let Some((pages, index_roots, overflow_free)) = prev {
                if let Some(m) = self.catalog_mut(tname) {
                    m.pages = pages;
                    m.index_roots = index_roots;
                    // Restoring the free list matters as much as the page
                    // list: leaving recycled chain pages in it would let a
                    // later oversized insert overwrite live documents.
                    m.overflow_free = overflow_free;
                }
            }
            return Err(e);
        }
        Ok(())
    }

    pub(super) fn save_catalog_into(&mut self, tx: &mut crate::pager::Tx) -> Result<()> {
        let mut tables = Object::new();
        for (name, meta) in &self.tables {
            let mut m = Object::new();
            m.insert(
                "columns".into(),
                Value::Array(meta.columns.iter().map(|c| Value::Str(c.clone())).collect()),
            );
            m.insert(
                "pages".into(),
                Value::Array(meta.pages.iter().map(|p| Value::Int(*p as i64)).collect()),
            );
            if let Some(pk) = &meta.primary_key {
                m.insert("primary_key".into(), Value::Str(pk.clone()));
            }
            // AUTOINCREMENT column must survive reopen: dropping it silently
            // turns the column into a plain nullable INT (inserts stop
            // assigning ids after a restart).
            if let Some(col) = &meta.autoinc {
                m.insert("autoinc".into(), Value::Str(col.clone()));
            }
            // Auto-generated GUID column must survive reopen for the same
            // reason as AUTOINCREMENT.
            if let Some(col) = &meta.autoguid {
                m.insert("autoguid".into(), Value::Str(col.clone()));
            }
            if let Some(sql) = &meta.view_sql {
                m.insert("view_sql".into(), Value::Str(sql.clone()));
            }
            if !meta.unique.is_empty() {
                m.insert(
                    "unique".into(),
                    Value::Array(meta.unique.iter().map(|c| Value::Str(c.clone())).collect()),
                );
            }
            if !meta.not_null.is_empty() {
                m.insert(
                    "not_null".into(),
                    Value::Array(
                        meta.not_null
                            .iter()
                            .map(|c| Value::Str(c.clone()))
                            .collect(),
                    ),
                );
            }
            if !meta.defaults.is_empty() {
                m.insert(
                    "defaults".into(),
                    Value::Array(
                        meta.defaults
                            .iter()
                            .map(|(c, e)| {
                                Value::Array(vec![Value::Str(c.clone()), Value::Str(e.clone())])
                            })
                            .collect(),
                    ),
                );
            }
            if !meta.checks.is_empty() {
                m.insert(
                    "checks".into(),
                    Value::Array(meta.checks.iter().map(|c| Value::Str(c.clone())).collect()),
                );
            }
            if !meta.foreign_keys.is_empty() {
                m.insert(
                    "foreign_keys".into(),
                    Value::Array(
                        meta.foreign_keys
                            .iter()
                            .map(|(c, rt, rc)| {
                                Value::Array(vec![
                                    Value::Str(c.clone()),
                                    Value::Str(rt.clone()),
                                    Value::Str(rc.clone()),
                                ])
                            })
                            .collect(),
                    ),
                );
            }
            if !meta.constraint_unique.is_empty() {
                m.insert(
                    "constraint_unique".into(),
                    Value::Array(
                        meta.constraint_unique
                            .iter()
                            .map(|c| Value::Str(c.clone()))
                            .collect(),
                    ),
                );
            }
            if !meta.index_defs.is_empty() {
                m.insert(
                    "index_defs".into(),
                    Value::Array(
                        meta.index_defs
                            .iter()
                            .map(|d| {
                                let mut entry = vec![
                                    Value::Str(d.name.clone()),
                                    Value::Str(d.column().to_string()),
                                    Value::Bool(d.unique),
                                    Value::Array(
                                        d.columns.iter().map(|c| Value::Str(c.clone())).collect(),
                                    ),
                                ];
                                // 5th element only for path indexes, 6th only
                                // for partial indexes: older readers ignore
                                // the tail, so the tuple stays compatible in
                                // both directions (design 005).
                                if let Some(p) = &d.path {
                                    entry.push(Value::Str(p.clone()));
                                }
                                if let Some(p) = &d.pred {
                                    entry.push(Value::Str(p.clone()));
                                }
                                Value::Array(entry)
                            })
                            .collect(),
                    ),
                );
            }
            if !meta.index_roots.is_empty() {
                m.insert(
                    "index_roots".into(),
                    Value::Object(
                        meta.index_roots
                            .iter()
                            .map(|(c, r)| (c.clone(), Value::Int(*r as i64)))
                            .collect(),
                    ),
                );
            }
            if !meta.overflow_free.is_empty() {
                m.insert(
                    "overflow_free".into(),
                    Value::Array(
                        meta.overflow_free
                            .iter()
                            .map(|p| Value::Int(*p as i64))
                            .collect(),
                    ),
                );
            }
            if let Some(q) = &meta.matview_sql {
                m.insert("matview_sql".into(), Value::Str(q.clone()));
            }
            if meta.dotted_keys {
                m.insert("dotted_keys".into(), Value::Bool(true));
            }
            tables.insert(name.clone(), Value::Object(m));
        }
        let mut cat = Object::new();
        cat.insert("tables".into(), Value::Object(tables));
        // Row triggers: absent when none exist, so a trigger-free catalog
        // keeps its exact historical byte shape (older readers ignore the
        // key; trigger-free volumes never gain one).
        if !self.triggers.is_empty() {
            let mut ts = Object::new();
            for d in self.triggers.values() {
                ts.insert(
                    d.name.clone(),
                    Value::Object(Object::from([
                        ("table".into(), Value::Str(d.table.clone())),
                        ("before".into(), Value::Bool(d.before)),
                        ("insert".into(), Value::Bool(d.insert)),
                        ("delete".into(), Value::Bool(d.delete)),
                        ("update".into(), Value::Bool(d.update)),
                        (
                            "update_of".into(),
                            match &d.update_of {
                                Some(cols) => Value::Array(
                                    cols.iter().map(|c| Value::Str(c.clone())).collect(),
                                ),
                                None => Value::Null,
                            },
                        ),
                        (
                            "when".into(),
                            match &d.when {
                                Some(w) => Value::Str(w.clone()),
                                None => Value::Null,
                            },
                        ),
                        (
                            "body".into(),
                            Value::Array(
                                d.body
                                    .iter()
                                    .map(|a| match a {
                                        TriggerAction::Sql(text) => Value::Array(vec![
                                            Value::Str("sql".into()),
                                            Value::Str(text.clone()),
                                        ]),
                                        TriggerAction::Raise { fail, msg } => Value::Array(vec![
                                            Value::Str("raise".into()),
                                            Value::Str(msg.clone()),
                                            Value::Bool(*fail),
                                        ]),
                                    })
                                    .collect(),
                            ),
                        ),
                    ])),
                );
            }
            cat.insert("triggers".into(), Value::Object(ts));
        }
        let bytes = encode::encode_to_vec(&Value::Object(cat))?;
        self.write_catalog_pages(tx, &bytes)?;
        Ok(())
    }

    /// Walk the committed catalog chain, starting at `CATALOG_PAGE`. A page 1
    /// without the magic (a legacy single-page catalog, or an empty database)
    /// is treated as a fresh one-page chain — its content is about to be
    /// rewritten in the new format anyway.
    pub(super) fn catalog_chain(&mut self) -> Result<Vec<u32>> {
        let first = self.pager.read_page(CATALOG_PAGE)?;
        if !first.starts_with(CATALOG_MAGIC) {
            return Ok(vec![CATALOG_PAGE]);
        }
        let mut chain = vec![CATALOG_PAGE];
        loop {
            let last = *chain.last().expect("seeded above");
            let page = self.pager.read_page(last)?;
            let next = u32::from_le_bytes(page[12..16].try_into().expect("header fits"));
            if next == 0 {
                return Ok(chain);
            }
            chain.push(next);
            // A damaged chain must surface as an error, not loop forever:
            // every link is a distinct page in the file.
            if chain.len() > self.pager.num_pages() as usize {
                return err("catalog chain is corrupt (cycle or dangling overflow page)");
            }
        }
    }

    /// Stage the catalog payload across its page chain inside `tx`, growing
    /// the chain by freshly allocated pages when the payload outgrew it.
    pub(super) fn write_catalog_pages(
        &mut self,
        tx: &mut crate::pager::Tx,
        bytes: &[u8],
    ) -> Result<()> {
        let need = bytes.len().div_ceil(CATALOG_CHUNK).max(1);
        let mut chain = self.catalog_chain()?;
        while chain.len() < need {
            let id = self.pager.allocate_page(tx)?;
            chain.push(id);
        }
        // A shrinking catalog releases its tail pages for reuse (they are no
        // longer linked into the chain the save below writes).
        for &extra in &chain[need..] {
            self.pager.free_page(tx, extra)?;
        }
        chain.truncate(need);
        for i in 0..need {
            let start = i * CATALOG_CHUNK;
            let end = (start + CATALOG_CHUNK).min(bytes.len());
            let mut page = vec![0u8; PAGE_SIZE];
            page[..8].copy_from_slice(CATALOG_MAGIC);
            if i == 0 {
                page[8..12].copy_from_slice(&(bytes.len() as u32).to_le_bytes());
            }
            let next = if i + 1 < need { chain[i + 1] } else { 0 };
            page[12..16].copy_from_slice(&next.to_le_bytes());
            page[CATALOG_HDR..CATALOG_HDR + (end - start)].copy_from_slice(&bytes[start..end]);
            self.pager.write_page(tx, chain[i], 0, &page)?;
        }
        Ok(())
    }
    pub(super) fn save_catalog(&mut self) -> Result<()> {
        let mut tx = self.pager.begin_tx();
        self.save_catalog_into(&mut tx)?;
        self.commit_pager_tx(tx)?;
        Ok(())
    }

    /// Cheap catalog snapshot (Arc clones + the autoinc cache): document
    /// rollback rides the pager's page undo journal instead of a deep copy.
    pub(super) fn catalog_snapshot(&self) -> TableSnapshot {
        TableSnapshot {
            tables: self.tables.clone(),
            autoinc: self.autoinc_cache.clone(),
        }
    }

    /// Restore the catalog from `snap` and replay `undo` in reverse into one
    /// commit: `Set` writes the pre-image back, `Alloc` returns the page to
    /// the reusable pool, `Free` reserves it (the restored catalog owns it
    /// again), `Realloc` needs nothing (its pre-image was recorded when the
    /// transaction freed it).
    ///
    /// Recording is paused for the replay (`pause_undo`): a replayed `Set`
    /// for a page first written after the savepoint must not treat the
    /// current dirty image as a new pre-image — that would let a later full
    /// ROLLBACK resurrect and commit the changes being rolled back here.
    /// The restored image *is* the transaction-start state, so future writes
    /// re-record it naturally.
    pub(super) fn restore_transaction(
        &mut self,
        snap: TableSnapshot,
        undo: &mut [crate::pager::UndoOp],
        sync_catalog: bool,
    ) -> Result<()> {
        self.pager.pause_undo();
        let replay = || -> Result<()> {
            let mut tx = self.pager.begin_tx();
            // By index, never consuming: a mid-replay failure must leave the
            // journal intact so a retried ROLLBACK still restores the pages.
            // `into_iter()` here used to drop every op on the first error —
            // the retry then "restored" only the catalog snapshot while the
            // transaction's page images stayed applied.
            for idx in (0..undo.len()).rev() {
                match &undo[idx] {
                    crate::pager::UndoOp::Set(id, image) => {
                        self.pager.write_page(&mut tx, *id, 0, image)?;
                    }
                    crate::pager::UndoOp::Alloc(id) => {
                        self.pager.free_page(&mut tx, *id)?;
                    }
                    crate::pager::UndoOp::Free(id) => {
                        self.pager.reserve_page(*id);
                    }
                    crate::pager::UndoOp::Realloc(_) => {}
                }
            }
            // Publish AFTER durability (the mirror of COMMIT's
            // durability-first order): the restored view must be installed
            // for the catalog write below, but a failed commit reverts it —
            // otherwise the in-memory catalog leads the pager with a view
            // whose rollback never became durable, and the next
            // `save_catalog` would persist the mix.
            let prev_tables = std::mem::replace(&mut self.tables, snap.tables);
            let prev_autoinc = std::mem::replace(&mut self.autoinc_cache, snap.autoinc);
            let result = if sync_catalog {
                self.save_catalog_into(&mut tx)
                    .and_then(|_| self.commit_pager_tx(tx))
            } else {
                self.commit_pager_tx(tx)
            };
            if result.is_err() {
                self.tables = prev_tables;
                self.autoinc_cache = prev_autoinc;
            }
            result.map(|_| ())
        };
        let result = replay();
        self.pager.resume_undo();
        result
    }

    pub(super) fn rollback_tx(&mut self) -> Result<ExecOutcome> {
        if self.tx_snapshot.is_none() {
            return err("no transaction in progress");
        }
        // Durability FIRST, teardown after (mirrors COMMIT above): taking
        // the snapshot before the undo-replay commit used to leave a failed
        // ROLLBACK wedged — retry hit "no transaction in progress" while
        // the engine held neither the rolled-back pages nor a consistent
        // catalog. On failure the transaction stays open and the error is
        // loud; teardown only runs once the restore is durable.
        // Take the journal only AFTER the deferred commits are voided:
        // abort_deferred failing (I/O error on the ABORT append) used to
        // drop the just-taken undo journal — a retried ROLLBACK then found
        // an empty journal and "succeeded" after restoring nothing but the
        // in-memory catalog, leaving the transaction's page images applied.
        // 先把本事务各语句的 deferred 提交显式作废:没有这组 ABORT 帧,
        // 恢复事务的同步提交会先落 FENCE,撕裂崩溃停在「FENCE 已持久、
        // 恢复事务 COMMIT 未持久」窗口时,整个已回滚事务会复活。
        self.pager.abort_deferred()?;
        let mut undo = self.pager.take_undo();
        let snap = self
            .tx_snapshot
            .clone()
            .expect("checked above; single writer keeps it in place");
        if let Err(e) = self.restore_transaction(snap, &mut undo, true) {
            // Put the journal back: the transaction stays open and the
            // retry must replay the same ops, not an empty tail.
            self.pager.restore_undo_journal(undo);
            return Err(e);
        }
        let _ = self.tx_snapshot.take();
        self.savepoints.clear();
        // The restore transaction committed DEFERRED (the snapshot was still
        // in place during it): fence + fsync now. ROLLBACK is a terminal
        // statement — no later COMMIT exists to trigger the WAL cap — so
        // without this a BEGIN/INSERT/ROLLBACK workload grew the log and
        // pending_writes without bound. The fence is also the durability
        // boundary for the undo-replayed page images. On failure the error
        // is loud and a retried ROLLBACK is idempotent (the undo journal is
        // empty by then; the catalog is already at the snapshot state).
        self.pager.sync_wal()?;
        Ok(ExecOutcome::Affected(0))
    }

    /// SAVEPOINT name: mark the current transaction state so a later
    /// ROLLBACK TO can restore it (the transaction stays open). Holds Arc
    /// pointers plus a journal mark — cost is O(tables), not O(database).
    /// Capped so a savepoint flood cannot pin unbounded journal memory.
    pub(super) fn savepoint(&mut self, name: &str) -> Result<ExecOutcome> {
        const MAX_SAVEPOINTS: usize = 64;
        if self.tx_snapshot.is_none() {
            return err("SAVEPOINT requires an active transaction");
        }
        if self.savepoints.len() >= MAX_SAVEPOINTS {
            return err(format!(
                "too many savepoints (max {MAX_SAVEPOINTS}); release or roll back some first"
            ));
        }
        let snap = self.catalog_snapshot();
        // Mark BEFORE the baseline images: the tail a later ROLLBACK TO
        // replays must include them.
        let mark = self.pager.undo_mark();
        self.pager.undo_savepoint()?;
        self.savepoints.push((name.to_string(), snap, mark));
        Ok(ExecOutcome::Affected(0))
    }

    /// RELEASE SAVEPOINT name: forget the savepoint (changes stay). With
    /// duplicate names the most recent one is released (SQLite semantics).
    pub(super) fn release_savepoint(&mut self, name: &str) -> Result<ExecOutcome> {
        let Some(pos) = self.savepoints.iter().rposition(|(n, _, _)| n == name) else {
            return err(format!("savepoint {name} does not exist"));
        };
        self.savepoints.truncate(pos);
        Ok(ExecOutcome::Affected(0))
    }

    /// ROLLBACK TO SAVEPOINT name: restore that savepoint's state; the
    /// outer transaction continues and later savepoints are discarded.
    /// The named savepoint itself is discarded too (a documented divergence
    /// from SQLite/PG, pinned by duplicate_savepoint_names_resolve_to_newest:
    /// a second ROLLBACK TO the same name resolves to the OLDER same-named
    /// savepoint, not an error).
    pub(super) fn rollback_to_savepoint(&mut self, name: &str) -> Result<ExecOutcome> {
        let Some(pos) = self.savepoints.iter().rposition(|(n, _, _)| n == name) else {
            return err(format!("savepoint {name} does not exist"));
        };
        let snap = self.savepoints[pos].1.clone();
        let mark = self.savepoints[pos].2;
        self.savepoints.truncate(pos);
        let mut undo = self.pager.take_undo_to(mark);
        // The transaction stays open: no catalog save is needed (the journal
        // replay already restored the catalog pages; the in-memory map comes
        // from the snapshot). `sync_catalog=false` keeps this cheap.
        if let Err(e) = self.restore_transaction(snap, &mut undo, false) {
            // Reattach the tail: a failed ROLLBACK TO must leave the journal
            // whole for the retry (and a later full ROLLBACK), exactly like
            // the full-ROLLBACK path.
            self.pager.reattach_undo_tail(undo);
            return Err(e);
        }
        Ok(ExecOutcome::Affected(0))
    }

    /// Remove every user table from the catalog, bypassing DROP TABLE's
    /// FOREIGN KEY guards. Cluster rejoin repair replaces the node's
    /// whole state with the cluster snapshot inside one transaction:
    /// tables the snapshot no longer contains must go too, and no
    /// removal order satisfies DROP's "referenced by FOREIGN KEY" check
    /// for every mixed state (cycles included). Runs inside the
    /// caller's transaction — rollback restores the pre-wipe catalog.
    /// Engine-managed system tables survive: the journal and positions
    /// describe stream-applied ops, which the snapshot adoption
    /// accounting (positions reset to the sampled heads) keeps valid.
    pub fn wipe_user_tables(&mut self) -> Result<()> {
        let doomed: Vec<String> = self
            .tables
            .keys()
            .filter(|n| !is_system_table(n))
            .cloned()
            .collect();
        let mut tx = self.pager.begin_tx();
        for name in doomed {
            if let Some(meta) = self.tables.get(&name) {
                self.free_table_storage(&mut tx, meta)?;
            }
            self.tables.remove(&name);
        }
        // Wiped tables may be recreated from a snapshot: drop their
        // AUTOINCREMENT watermarks with them or the recreated table resumes
        // from a stale counter (see DROP TABLE). Triggers die too — the
        // snapshot replay recreates them from its own CREATE TRIGGER block
        // (emitted after the data, pg_dump style).
        self.autoinc_cache.clear();
        self.triggers.clear();
        self.save_catalog_into(&mut tx)?;
        self.commit_pager_tx(tx)?;
        Ok(())
    }

    /// Release every page a table's storage occupies: heap pages, index-tree
    /// pages and recycled overflow-chain pages. Called for tables being
    /// dropped or wiped (a `rewrite_table` keeps `overflow_free`, which its
    /// next incarnation reuses for chains, so it frees only heap/tree pages).
    pub(super) fn free_table_storage(
        &self,
        tx: &mut crate::pager::Tx,
        meta: &TableMeta,
    ) -> Result<()> {
        // Live overflow chains are anchored only by the slots' head pointers
        // (recycled ones sit in `overflow_free`); free them before the heap
        // pages themselves go away.
        crate::heap::free_overflow_chains(&self.pager, tx, &meta.pages)?;
        for &p in &meta.pages {
            self.pager.free_page(tx, p)?;
        }
        for &p in &meta.overflow_free {
            self.pager.free_page(tx, p)?;
        }
        for (root_key, &root) in &meta.index_roots {
            for p in BTree::open(root)
                .collect_pages(&PageReader::current(&self.pager), tx)
                .map_err(|e| index_err(root_key, e))?
            {
                self.pager.free_page(tx, p)?;
            }
        }
        Ok(())
    }

    // ---- catch-up replication: journal + positions ----

    /// Create the catch-up system tables if missing. Called by the server
    /// at startup, before any journal or position access.
    pub fn ensure_cluster_tables(&mut self) -> Result<()> {
        if self.tables.contains_key(CLUSTER_LOG_TABLE)
            && self.tables.contains_key(CLUSTER_POS_TABLE)
            && self.tables.contains_key(CLUSTER_ID_TABLE)
        {
            return Ok(());
        }
        // Reserved engine names: the internal-ddl flag is the sanctioned
        // way past the CREATE TABLE guard (same pattern as
        // ensure_user_tables; always cleared on the way out).
        self.internal_ddl = true;
        let result = self.ensure_cluster_tables_inner();
        self.internal_ddl = false;
        result
    }

    pub(super) fn ensure_cluster_tables_inner(&mut self) -> Result<()> {
        let preexisting = self.tables.contains_key(CLUSTER_LOG_TABLE);
        self.execute(&format!(
            "CREATE TABLE IF NOT EXISTS {CLUSTER_LOG_TABLE} (seq INT PRIMARY KEY, sql TEXT, ts INT)"
        ))?;
        // Migration: journals created before PITR have no ts column. Entries
        // migrated this way keep ts NULL — PITR export skips NULL-ts rows
        // (they predate the feature and always sit before the first
        // post-upgrade base backup's journal position).
        if preexisting {
            let has_ts = self
                .execute(&format!("SELECT ts FROM {CLUSTER_LOG_TABLE} LIMIT 1"))
                .is_ok();
            if !has_ts {
                self.execute(&format!(
                    "ALTER TABLE {CLUSTER_LOG_TABLE} ADD COLUMN ts INT"
                ))?;
            }
        }
        self.execute(&format!(
            "CREATE TABLE IF NOT EXISTS {CLUSTER_POS_TABLE} (node_id TEXT PRIMARY KEY, seq INT)"
        ))?;
        self.execute(&format!(
            "CREATE TABLE IF NOT EXISTS {CLUSTER_ID_TABLE} (id TEXT)"
        ))?;
        Ok(())
    }

    /// Create the pub/sub backing table (idempotent). Lives in core so the
    /// server's pubsub store and every test use the same sanctioned path
    /// past the reserved-name guard in CREATE TABLE.
    pub fn ensure_pubsub_table(&mut self) -> Result<()> {
        self.internal_ddl = true;
        let result = self
            .execute(&format!(
                "CREATE TABLE IF NOT EXISTS {PUBSUB_TABLE} \
             (id INT PRIMARY KEY AUTOINCREMENT, channel TEXT, ts_ms INT, payload TEXT)"
            ))
            .map(|_| ());
        self.internal_ddl = false;
        result
    }

    /// This node's persistent random identity: the key other nodes store
    /// this node's journal position under. Generated once, then stable.
    pub fn cluster_id(&mut self) -> Result<String> {
        self.ensure_cluster_tables()?;
        if let Some(id) = &self.cluster_id {
            return Ok(id.clone());
        }
        let existing = self.execute(&format!("SELECT id FROM {CLUSTER_ID_TABLE} LIMIT 1"))?;
        if let ExecOutcome::Rows(r) = existing {
            if let Some(row) = r.rows.first() {
                if let Some(Value::Str(id)) = row.first() {
                    self.cluster_id = Some(id.clone());
                    return Ok(id.clone());
                }
            }
        }
        let id = crate::guid::uuidv7();
        self.execute(&format!(
            "INSERT INTO {CLUSTER_ID_TABLE} VALUES ({})",
            value_literal(&Value::Str(id.clone()))?
        ))?;
        self.cluster_id = Some(id.clone());
        Ok(id)
    }

    /// Table/view grants for a dropped object are garbage: delete them
    /// before the catalog change (recreating the same name must not
    /// resurrect the old privileges). Autocommit statements; replay order
    /// on peers is identical. The deleted ROWS come back to the caller:
    /// a failure of the main DDL after this point must restore them, or
    /// the node keeps a grants divergence no journal entry explains.
    /// Errors propagate — the old `.ok()` swallowed disk failures into a
    /// silent half-drop.
    pub(super) fn delete_grants_for(&mut self, names: &[String]) -> Result<Vec<Object>> {
        // The grants table is created lazily on the first user-admin write;
        // a node with no users has no grants to clean.
        if !self.tables.contains_key(crate::useradmin::GRANTS_TABLE) {
            return Ok(Vec::new());
        }
        let mut deleted = Vec::new();
        for name in names {
            if !is_internal_table(name) {
                let lit_name = crate::stmt::sql_string_literal(name);
                if let Ok(ExecOutcome::Rows(r)) = self.execute(&format!(
                    "SELECT * FROM {} WHERE tbl = {}",
                    crate::useradmin::GRANTS_TABLE,
                    lit_name
                )) {
                    for row in &r.rows {
                        let mut obj = Object::new();
                        for (c, v) in r.columns.iter().zip(row.iter()) {
                            obj.insert(c.clone(), v.clone());
                        }
                        deleted.push(obj);
                    }
                }
                self.execute(&format!(
                    "DELETE FROM {} WHERE tbl = {}",
                    crate::useradmin::GRANTS_TABLE,
                    lit_name
                ))?;
            }
        }
        Ok(deleted)
    }

    /// Best-effort restore of grant rows a failed DDL must put back. Only
    /// reachable on a main-DDL failure after the grants autocommit landed;
    /// a restore failure is shouted to stderr (the alternative — silence —
    /// is the divergence this helper exists to close).
    pub(super) fn restore_grant_rows(&mut self, rows: &[Object]) {
        for row in rows {
            let cols: Vec<String> = row.keys().cloned().collect();
            let vals: Vec<String> = row
                .values()
                .map(|v| value_literal(v).unwrap_or_else(|_| "NULL".to_string()))
                .collect();
            let sql = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                crate::useradmin::GRANTS_TABLE,
                cols.iter()
                    .map(|c| quote_ident(c))
                    .collect::<Vec<_>>()
                    .join(", "),
                vals.join(", ")
            );
            if let Err(e) = self.execute(&sql) {
                eprintln!(
                    "warning: failed to restore grant rows after a failed DDL ({e}); \
                     run the DDL again or REVOKE manually"
                );
            }
        }
    }

    /// Rewrite the whole table from `docs` (delete+reinsert). Rebuilds every
    /// index tree for the table, updates `meta` in place, and re-inserts it
    /// into the catalog. The old heap pages and index-tree pages are released
    /// into the pager's reusable pool inside the same transaction, so a
    /// rebuild reuses its own storage instead of growing the file.
    pub(super) fn rewrite_table(
        &mut self,
        table: &str,
        meta: &mut TableMeta,
        docs: Vec<Object>,
    ) -> Result<()> {
        self.rewrite_table_with_dead(table, meta, docs, Vec::new())
    }

    /// Like [`Self::rewrite_table`], but also releases the page trees of
    /// indexes that were removed from `meta` BEFORE this rebuild (DROP
    /// COLUMN): their roots are no longer in `meta.index_roots`, so the
    /// generic free loop below would skip them and the pages would leak for
    /// good — nothing else ever references them. Freed in the SAME write
    /// unit as the catalog switch, so a crash can never leave a committed
    /// catalog pointing at recycled pages.
    pub(super) fn rewrite_table_with_dead(
        &mut self,
        table: &str,
        meta: &mut TableMeta,
        docs: Vec<Object>,
        dead_trees: Vec<(String, u32)>,
    ) -> Result<()> {
        self.rewrite_table_inner(table, meta, docs, true, dead_trees)
    }

    pub(super) fn rewrite_table_inner(
        &mut self,
        table: &str,
        meta: &mut TableMeta,
        docs: Vec<Object>,
        free_old: bool,
        dead_trees: Vec<(String, u32)>,
    ) -> Result<()> {
        let mut tx = self.pager.begin_tx();
        if let Err(e) = self.rewrite_table_tx(&mut tx, table, meta, docs, free_old, dead_trees) {
            // Same failure semantics as the old inline body: the staged
            // pages never reach the data file (Tx's drop restores them),
            // and the in-memory catalog entry was already restored by
            // rewrite_table_tx's save-failure path or never swapped.
            self.pager.abort_tx(tx)?;
            return Err(e);
        }
        self.commit_pager_tx(tx)?;
        Ok(())
    }

    /// [`Self::rewrite_table_inner`] on a CALLER-OWNED transaction: the
    /// TRUNCATE batch rewrites several tables inside ONE pager transaction
    /// so a mid-list failure rolls the whole statement back (each table
    /// used to commit its own rebuild, leaving earlier tables emptied and
    /// committed while the failed statement never reached the journal —
    /// replicas kept rows the origin lost).
    pub(super) fn rewrite_table_tx(
        &mut self,
        tx: &mut crate::pager::Tx,
        table: &str,
        meta: &mut TableMeta,
        docs: Vec<Object>,
        free_old: bool,
        dead_trees: Vec<(String, u32)>,
    ) -> Result<()> {
        let mut heap = Heap {
            pages: Vec::new(),
            overflow_free: meta.overflow_free.clone(),
            dropped: Vec::new(),
        };
        let cols: Vec<String> = meta.index_roots.keys().cloned().collect();
        // Release old storage BEFORE the rebuild: allocations below then pop
        // those pages back (same transaction, so the catalog switch and the
        // page rewrites are atomic; snapshots rebuild old versions from the
        // WAL and never observe the reuse).
        if free_old {
            // The docs being rewritten carried live overflow chains anchored
            // only by the old slots' head pointers; free them too or every
            // rewrite/TRUNCATE of a table with oversized documents orphans
            // the chains (the file grows without bound under churn).
            crate::heap::free_overflow_chains(&self.pager, tx, &meta.pages)?;
            for &p in &meta.pages {
                self.pager.free_page(tx, p)?;
            }
            for (root_key, &root) in &meta.index_roots {
                for p in BTree::open(root)
                    .collect_pages(&PageReader::current(&self.pager), tx)
                    .map_err(|e| index_err(root_key, e))?
                {
                    self.pager.free_page(tx, p)?;
                }
            }
            for (root_key, root) in &dead_trees {
                for p in BTree::open(*root)
                    .collect_pages(&PageReader::current(&self.pager), tx)
                    .map_err(|e| index_err(root_key, e))?
                {
                    self.pager.free_page(tx, p)?;
                }
            }
        }
        let mut pairs: Vec<(u64, Object)> = Vec::with_capacity(docs.len());
        for doc in &docs {
            // Rebuilds walk every row (ALTER/RENAME on a large table must
            // stay interruptible like every other long row loop).
            self.stmt_deadline.check()?;
            let loc = heap.insert(&self.pager, tx, doc)?;
            pairs.push((loc, doc.clone()));
        }
        let roots = self.build_trees(
            tx,
            &pairs,
            &cols,
            &cols
                .iter()
                .map(|c| meta.index_columns_of(c))
                .collect::<Vec<_>>(),
            &cols
                .iter()
                .map(|k| {
                    meta.index_defs
                        .iter()
                        .find(|d| d.name == *k)
                        .and_then(|d| d.path.clone())
                })
                .collect::<Vec<_>>(),
            &cols
                .iter()
                .map(|c| meta.root_key_unique(c))
                .collect::<Vec<_>>(),
            // Partial-index predicates: the rebuilt tree must hold exactly
            // the qualifying rows or every gated probe reads wrong.
            &cols
                .iter()
                .map(|k| {
                    meta.index_defs
                        .iter()
                        .find(|d| d.name == *k)
                        .and_then(|d| d.pred.as_deref())
                        .and_then(|p| parse_expr_text(p).ok())
                })
                .collect::<Vec<_>>(),
        )?;
        // Keep the previous catalog entry so a failed persist can restore the
        // in-memory map — after `?` below the tx is dropped and its staged
        // pages never reach the data file; the map must not keep pointing at
        // them, or a later successful save would persist the ghost pages and
        // orphan the real data.
        let prev_meta = self.tables.get(table).cloned();
        meta.pages = heap.pages;
        meta.index_roots = roots;
        // The rebuild drained the recycled-chain free list above: every
        // oversized document popped a page from it. Persist what is left —
        // keeping the pre-rebuild list would advertise pages the rebuild just
        // made live, and the next oversized insert would overwrite a live
        // document's chain (reads then fail with "overflow chain corrupt" or
        // return another row's bytes). The DML paths have always written the
        // heap's list back through `sync_table_layout`; the rebuild is the
        // one that did not.
        meta.overflow_free = heap.overflow_free;
        self.autoinc_cache.remove(table);
        self.tables
            .insert(table.to_string(), std::sync::Arc::new(meta.clone()));
        if let Err(e) = self.save_catalog_into(tx) {
            match prev_meta {
                Some(old) => {
                    *meta = old.as_ref().clone();
                    self.tables.insert(table.to_string(), old);
                }
                None => {
                    self.tables.remove(table);
                }
            }
            return Err(e);
        }
        Ok(())
    }

    /// Build one B+ tree per (root_key, columns, unique) spec, populated
    /// from the table's (locator, document) pairs. Single-column specs key
    /// by the bare value (legacy layout); multi-column specs key by
    /// `Value::Array` over the columns in order. Unique specs reject
    /// duplicates at insert time.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn build_trees(
        &mut self,
        tx: &mut crate::pager::Tx,
        pairs: &[(u64, Object)],
        root_keys: &[String],
        cols_per_key: &[Vec<String>],
        paths_per_key: &[Option<String>],
        unique_keys: &[bool],
        preds_per_key: &[Option<SqlExpr>],
    ) -> Result<std::collections::BTreeMap<String, u32>> {
        let mut roots = std::collections::BTreeMap::new();
        for i in 0..root_keys.len() {
            let root_key = &root_keys[i];
            let cols = &cols_per_key[i];
            let path = paths_per_key[i].as_deref();
            let unique = unique_keys[i];
            let pred = preds_per_key[i].as_ref();
            let mut tree = BTree::create(&self.pager, tx).map_err(|e| index_err(root_key, e))?;
            for (loc, doc) in pairs {
                // The backfill is the expensive half of CREATE INDEX on a big
                // table (the materialization before it samples already) —
                // without a check here the statement was uninterruptible
                // once it entered the tree builds.
                self.stmt_deadline.check()?;
                // Partial index: only pred-satisfying rows enter the tree
                // (NULL/missing fields fail the predicate like any WHERE).
                if let Some(p) = pred {
                    if !matches!(eval_expr(p, doc)?, Value::Bool(true)) {
                        continue;
                    }
                }
                let key = match path {
                    // JSON path index: extract from the column's JSON text
                    // with the exact lookup JSON_EXTRACT uses (design 005).
                    Some(p) => json_path_key(doc, &cols[0], p),
                    None => index_key_of(doc, cols),
                };
                if let Some(v) = key {
                    tree.insert(&self.pager, tx, v, *loc, unique)
                        .map_err(|e| index_err(root_key, e))?;
                }
            }
            roots.insert(root_key.clone(), tree.root);
        }
        Ok(roots)
    }

    pub(super) fn autoinc_next_for(&mut self, table: &str, meta: &TableMeta) -> Result<i64> {
        if let Some(&n) = self.autoinc_cache.get(table) {
            return Ok(n);
        }
        let mut max: i64 = 0;
        if let Some(col) = &meta.autoinc {
            // Propagate read errors: defaulting to an empty scan would cache
            // a reset counter and hand out ids that collide after restart.
            let docs = Heap {
                pages: meta.pages.clone(),
                overflow_free: meta.overflow_free.clone(),
                dropped: Vec::new(),
            }
            .scan(&PageReader::current(&self.pager))?;
            for d in &docs {
                if let Some(Value::Int(i)) = d.get(col) {
                    max = max.max(*i);
                }
            }
        }
        let next = max + 1;
        self.autoinc_cache.insert(table.to_string(), next);
        Ok(next)
    }
}
