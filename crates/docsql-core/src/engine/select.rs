use super::window::*;
use super::*;

impl<'a> ReadCx<'a> {
    /// Virtual information_schema tables from the catalog.
    pub(super) fn information_schema(&self, which: &str) -> Vec<Object> {
        // Engine/internal tables stay out of the standard dictionaries —
        // same display rule as the object tree and the Oracle views (their
        // existence, names and shapes are not every user's business).
        let visible = self.tables.iter().filter(|(n, _)| !is_internal_table(n));
        let mut out = Vec::new();
        if which.ends_with("tables") {
            for (name, meta) in visible {
                out.push(Object::from([
                    ("table_name".into(), Value::Str(name.clone())),
                    ("pages".into(), Value::Int(meta.pages.len() as i64)),
                ]));
            }
        } else {
            for (name, meta) in visible {
                for col in &meta.columns {
                    out.push(Object::from([
                        ("table_name".into(), Value::Str(name.clone())),
                        ("column_name".into(), Value::Str(col.clone())),
                        (
                            "is_nullable".into(),
                            Value::Str(
                                if meta.not_null.contains(col) {
                                    "NO"
                                } else {
                                    "YES"
                                }
                                .into(),
                            ),
                        ),
                        (
                            "data_type".into(),
                            Value::Str(
                                if meta.autoguid.as_deref() == Some(col.as_str()) {
                                    "GUID"
                                } else {
                                    "ANY"
                                }
                                .into(),
                            ),
                        ),
                    ]));
                }
            }
        }
        out
    }

    /// CROSS/OUTER APPLY over a table function: evaluate the function's
    /// arguments against EACH left row (correlated lateral semantics),
    /// materialize the rowset, and merge. `outer` keeps the left row when
    /// the applied rowset is empty (right columns read as NULL — missing
    /// fields on the schemaless row shape).
    pub(super) fn apply_table_function(
        &self,
        left: Vec<Object>,
        fname: &str,
        alias: &Option<sqlparser::ast::TableAlias>,
        args: &[sqlparser::ast::FunctionArg],
        outer: bool,
    ) -> Result<Vec<Object>> {
        let Some(alias_obj) = alias else {
            return err(format!(
                "{fname} requires a table alias (APPLY {fname}(...) AS t)"
            ));
        };
        let akey = alias_obj.name.value.clone();
        let fname_upper = fname.to_uppercase();
        // Static ordinal guess for the width check when no row ever
        // materializes; the argument is correlated so a non-constant
        // third argument falls back to the two-column shape.
        let ordinal_on = args.len() == 3
            && matches!(
                eval_const(match args.last() {
                    Some(sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(e),
                    )) => e,
                    _ => return err("table function arguments must be values"),
                }),
                Ok(Value::Int(n)) if n != 0
            );
        // Alias column renames apply to the produced rows (same rule as
        // derived tables).
        let mut out = Vec::with_capacity(left.len());
        let mut produced_cols: Option<Vec<String>> = None;
        for lrow in &left {
            self.deadline.check()?;
            let mut vals = Vec::new();
            for a in args {
                match a {
                    sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(e),
                    ) => vals.push(eval_expr(e, lrow)?),
                    _ => return err("table function arguments must be values"),
                }
            }
            let docs = match crate::tsql::table_function_checked(&fname_upper, &vals, &mut || {
                self.deadline.check()
            }) {
                Some(Ok(rows)) => rows,
                Some(Err(e)) => return Err(e),
                None => return err(format!("unknown table function: {fname}")),
            };
            if docs.is_empty() {
                if outer {
                    out.push(lrow.clone());
                }
                continue;
            }
            if produced_cols.is_none() {
                produced_cols = Some(docs[0].keys().cloned().collect());
            }
            for d in docs {
                let qright = qualify(&d, &akey);
                out.push(merged_row(lrow, &qright));
            }
        }
        // Actual output columns (bare names) for the rename and NULL pad.
        let cols: Vec<String> = produced_cols.unwrap_or_else(|| {
            tablefn_static_cols(&fname_upper, ordinal_on)
                .iter()
                .map(|s| s.to_string())
                .collect()
        });
        // A wrong column list must fail loudly, exactly like the derived
        // table path — swallowing it silently dropped the rename.
        let rename = table_alias_columns(&alias_obj.columns, cols.len())?;
        let final_names: Vec<String> = rename.clone().unwrap_or_else(|| cols.clone());
        let prefix = format!("{akey}.");
        for row in &mut out {
            if let Some(rename) = &rename {
                // Rename IN PLACE (move the value from the function's output
                // column to its alias). Inserting alongside would leak the
                // original column into SELECT *.
                for (new, old) in rename.iter().zip(cols.iter()) {
                    let old_key = format!("{akey}.{old}");
                    if let Some(v) = row.remove(&old_key) {
                        row.insert(format!("{akey}.{new}"), v);
                    }
                }
            }
            if outer && !row.keys().any(|k| k.starts_with(&prefix)) {
                // OUTER APPLY kept this left row because the applied rowset
                // was empty. Pad the right columns with explicit NULLs:
                // without keys, a same-named LEFT column would answer the
                // suffix fallback and masquerade as the right value.
                for c in &final_names {
                    row.insert(format!("{akey}.{c}"), Value::Null);
                }
            }
        }
        Ok(out)
    }

    /// Planned BARE column list of a table factor, when statically known:
    /// base tables/views from the catalog, table functions from their fixed
    /// shape. Used to pad explicit NULL keys when an OUTER join's other side
    /// is empty (no row to sample the key shape from). `None` = unknown
    /// (derived tables with non-trivial projections, CTEs, …) and the join
    /// keeps its historical behavior.
    pub(super) fn table_factor_columns(
        &self,
        tf: &sqlparser::ast::TableFactor,
        ctes: &Ctes,
    ) -> Option<Vec<String>> {
        match tf {
            sqlparser::ast::TableFactor::Table { name, args, .. } => {
                if args.is_some() {
                    // Table-with-args = a table function whose output shape
                    // depends on evaluated arguments (STRING_SPLIT's ordinal
                    // toggle); let load_table_factor handle it and report no
                    // planned shape here.
                    return None;
                }
                let tname = obj_name(name);
                if is_compat_view(&tname) || ctes.contains_key(&tname) {
                    return None;
                }
                self.tables.get(&tname).map(|m| m.columns.clone())
            }
            sqlparser::ast::TableFactor::OpenJsonTable { .. } => Some(
                tablefn_static_cols("OPENJSON", false)
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
            ),
            _ => None,
        }
    }

    pub(super) fn load_table_factor(
        &self,
        tf: &sqlparser::ast::TableFactor,
        ctes: &Ctes,
    ) -> Result<(String, Option<String>, Vec<Object>)> {
        let sqlparser::ast::TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            sample,
            index_hints,
        } = tf
        else {
            // Derived table: FROM (SELECT ...) AS alias
            if let sqlparser::ast::TableFactor::Derived {
                lateral,
                subquery,
                alias,
                sample,
            } = tf
            {
                if *lateral {
                    return err("LATERAL derived tables are not supported");
                }
                if sample.is_some() {
                    return err("TABLESAMPLE is not supported");
                }
                // Chain the outer CTE scope through: a derived table is a
                // subquery of the statement it appears in, so `WITH t AS (…)
                // SELECT * FROM (SELECT * FROM t) d` must read the CTE —
                // and NOT fall through to a same-named base table. An own
                // WITH on the subquery materializes on top (its names
                // shadow, lexical nesting).
                let scoped = self.materialize_ctes(&subquery.with, ctes)?;
                let ExecOutcome::Rows(r) =
                    self.exec_query_body(subquery.as_ref().clone(), &scoped)?
                else {
                    return err("derived table must be a SELECT");
                };
                let alias_columns = match alias {
                    Some(a) => table_alias_columns(&a.columns, r.columns.len())?,
                    None => None,
                };
                let keys = match &alias_columns {
                    Some(cols) => cols.clone(),
                    None => super::subquery::output_doc_keys(&r.columns),
                };
                let docs: Vec<Object> = r
                    .rows
                    .into_iter()
                    .map(|row| keys.iter().cloned().zip(row).collect())
                    .collect();
                let alias = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_default();
                return Ok(("@derived".into(), Some(alias), docs));
            }
            // Bare function call in FROM: `FROM STRING_SPLIT('a,b', ',') AS s`
            // parses here, not into TableFactor::Table's args.
            if let sqlparser::ast::TableFactor::Function {
                lateral,
                name,
                args,
                with_ordinality,
                alias,
            } = tf
            {
                if *lateral {
                    return err("LATERAL table functions are not supported");
                }
                if *with_ordinality {
                    return err("WITH ORDINALITY is not supported");
                }
                let fname = obj_name(name).to_uppercase();
                if !matches!(
                    fname.as_str(),
                    "STRING_SPLIT" | "GENERATE_SERIES" | "OPENJSON"
                ) {
                    return err("table functions or FROM hints are not supported");
                }
                let mut vals = Vec::new();
                for a in args {
                    match a {
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(e),
                        ) => vals.push(eval_const(e)?),
                        _ => return err("table function arguments must be values"),
                    }
                }
                let Some(alias_obj) = alias else {
                    return err(format!(
                        "{fname} requires a table alias (FROM {fname}(...) AS t)"
                    ));
                };
                let mut docs = match crate::tsql::table_function_checked(&fname, &vals, &mut || {
                    self.deadline.check()
                }) {
                    Some(Ok(rows)) => rows,
                    Some(Err(e)) => return Err(e),
                    None => return err(format!("unknown table function: {fname}")),
                };
                let ordinal_on = vals.len() == 3 && matches!(vals[2], Value::Int(n) if n != 0);
                rename_tablefn_docs(alias_obj, &fname, ordinal_on, &mut docs)?;
                return Ok(("@tablefn".into(), Some(alias_obj.name.value.clone()), docs));
            }
            // `OPENJSON(x)` parses into a dedicated factor (WITH shapes and
            // path arguments are not supported — default key/value/type
            // rows only).
            if let sqlparser::ast::TableFactor::OpenJsonTable {
                json_expr,
                json_path,
                columns,
                alias,
            } = tf
            {
                if !columns.is_empty() || json_path.is_some() {
                    return err("OPENJSON WITH (...) and path arguments are not supported");
                }
                let val = eval_const(json_expr)?;
                let Some(alias_obj) = alias else {
                    return err("OPENJSON requires a table alias (FROM OPENJSON(...) AS j)");
                };
                let mut docs =
                    match crate::tsql::table_function_checked("OPENJSON", &[val], &mut || {
                        self.deadline.check()
                    }) {
                        Some(Ok(rows)) => rows,
                        Some(Err(e)) => return Err(e),
                        None => return err("unknown table function: OPENJSON"),
                    };
                rename_tablefn_docs(alias_obj, "OPENJSON", false, &mut docs)?;
                return Ok(("@tablefn".into(), Some(alias_obj.name.value.clone()), docs));
            }
            // T-SQL PIVOT: implicit-group every column except the pivot
            // column and the aggregate's argument, then emit one column
            // per IN-list value holding the aggregate over the matching
            // rows (NULL when the group has none).
            if let sqlparser::ast::TableFactor::Pivot {
                table,
                aggregate_functions,
                value_column,
                value_source,
                default_on_null,
                alias,
            } = tf
            {
                if default_on_null.is_some() || aggregate_functions.len() != 1 {
                    return err("PIVOT supports one aggregate without DEFAULT ON NULL here");
                }
                let (inner_name, _inner_alias, docs) = self.load_table_factor(table, ctes)?;
                let pivot = pivot_factor(docs, aggregate_functions, value_column, value_source)?;
                // Same alias fallback as UNPIVOT: without it the sentinel
                // "@pivot" leaked into output column names on JOIN.
                return Ok((
                    "@pivot".into(),
                    alias
                        .as_ref()
                        .map(|a| a.name.value.clone())
                        .or(Some(inner_name)),
                    pivot,
                ));
            }
            // T-SQL UNPIVOT: one output row per (input row × listed
            // column); NULL values are excluded (T-SQL default).
            if let sqlparser::ast::TableFactor::Unpivot {
                table,
                value,
                name,
                columns,
                null_inclusion,
                alias,
            } = tf
            {
                if null_inclusion.is_some() {
                    return err(
                        "UNPIVOT INCLUDE|EXCLUDE NULLS is not supported (NULLs are excluded)",
                    );
                }
                let (inner_name, _inner_alias, docs) = self.load_table_factor(table, ctes)?;
                let out = unpivot_factor(docs, value, name, columns)?;
                return Ok((
                    "@unpivot".into(),
                    alias
                        .as_ref()
                        .map(|a| a.name.value.clone())
                        .or(Some(inner_name)),
                    out,
                ));
            }
            return err("unsupported FROM item (expected a table, derived table or VALUES)");
        };
        if let Some(args) = &args {
            // T-SQL/standard table-valued functions with a fixed, known
            // output shape (STRING_SPLIT, GENERATE_SERIES, OPENJSON):
            // constant arguments only — they materialize once per query.
            // Everything else keeps the loud table-function error.
            let fname = obj_name(name).to_uppercase();
            if !matches!(
                fname.as_str(),
                "STRING_SPLIT" | "GENERATE_SERIES" | "OPENJSON"
            ) {
                return err("table functions or FROM hints are not supported");
            }
            let list = &args.args;
            let mut vals = Vec::new();
            for a in list {
                match a {
                    sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(e),
                    ) => vals.push(eval_const(e)?),
                    _ => return err("table function arguments must be values"),
                }
            }
            let Some(alias_obj) = &alias else {
                return err(format!(
                    "{fname} requires a table alias (FROM {fname}(...) AS t)"
                ));
            };
            let alias = alias_obj.name.value.clone();
            let mut docs = match crate::tsql::table_function_checked(&fname, &vals, &mut || {
                self.deadline.check()
            }) {
                Some(Ok(rows)) => rows,
                Some(Err(e)) => return Err(e),
                None => return err(format!("unknown table function: {fname}")),
            };
            let ordinal_on = vals.len() == 3 && matches!(vals[2], Value::Int(n) if n != 0);
            rename_tablefn_docs(alias_obj, &fname, ordinal_on, &mut docs)?;
            return Ok(("@tablefn".into(), Some(alias), docs));
        }
        if sample.is_some() {
            return err("TABLESAMPLE is not supported");
        }
        if version.is_some() {
            return err("table time travel (AS OF / FOR SYSTEM_TIME) is not supported");
        }
        if !partitions.is_empty() {
            return err("partition selection (PARTITION (...)) is not supported");
        }
        if !with_hints.is_empty() {
            // MSSQL `WITH (NOLOCK)` and friends: reads are snapshot-isolated
            // here; silently accepting locking hints would misstate them.
            return err("table hints (WITH (...)) are not supported");
        }
        if !index_hints.is_empty() {
            return err("index hints are not supported");
        }
        if *with_ordinality {
            return err("WITH ORDINALITY is not supported");
        }
        if json_path.is_some() {
            return err("JSON path table sources are not supported");
        }
        if alias.as_ref().is_some_and(|a| !a.columns.is_empty()) {
            // Renaming a base table's columns is not implemented; ignoring
            // the list would silently keep the original names.
            return err("column aliases on a table alias are not supported");
        }
        let tname = obj_name(name);
        let alias = alias.as_ref().map(|a| a.name.value.clone());
        // Oracle DUAL: the one-row dummy table. Case-insensitive, works even
        // when no user table named DUAL exists (it always shadows).
        if tname.eq_ignore_ascii_case("DUAL") {
            return Ok(("DUAL".into(), alias, vec![Object::new()]));
        }
        // WITH (...) names shadow real tables for this statement.
        if let Some(docs) = ctes.get(&tname) {
            return Ok((tname, alias, docs.clone()));
        }
        let qualified: String = name
            .0
            .iter()
            .map(|p| match p {
                ObjectNamePart::Identifier(i) => i.value.clone(),
                other => other.to_string(),
            })
            .collect::<Vec<_>>()
            .join(".");
        // SQL Server-style catalog names: explicit guidance instead of the
        // confusing "table <last part> does not exist".
        let qualified_lower = qualified.to_ascii_lowercase();
        if qualified_lower.starts_with("sys.") || qualified_lower == "sysobjects" {
            return err(
                "SQL Server catalog views (sys.*/sysobjects) are not supported \
                 (use information_schema or sqlite_master)",
            );
        }
        if qualified.eq_ignore_ascii_case("sqlite_master")
            || qualified.eq_ignore_ascii_case("sqlite_temporal_master")
        {
            // SQLite-dialect compatibility view (EF Core probes it to learn
            // which tables exist). Index rows let clients enumerate CREATE
            // INDEX definitions for schema sync.
            let mut docs: Vec<Object> = self
                .tables
                .iter()
                // Same display rule as information_schema: engine/user
                // storage tables are not listed to readers.
                .filter(|(n, _)| !is_internal_table(n))
                .map(|(n, m)| {
                    Object::from([
                        ("type".into(), Value::Str("table".into())),
                        ("name".into(), Value::Str(n.clone())),
                        ("tbl_name".into(), Value::Str(n.clone())),
                        // Full DDL text (same generator as dump_script): the
                        // EF keep-data rebuild reads it to re-attach
                        // table-level UNIQUE/CHECK/FOREIGN KEY constraints
                        // the model does not know about — an empty text
                        // silently dropped them from every rebuilt table.
                        (
                            "sql".into(),
                            if m.is_view() {
                                Value::Str(String::new())
                            } else {
                                Value::Str(create_table_ddl(n, m))
                            },
                        ),
                    ])
                })
                .collect();
            for (tbl, meta) in self.tables.iter().filter(|(n, _)| !is_internal_table(n)) {
                for d in &meta.index_defs {
                    docs.push(Object::from([
                        ("type".into(), Value::Str("index".into())),
                        ("name".into(), Value::Str(d.name.clone())),
                        ("tbl_name".into(), Value::Str(tbl.clone())),
                        (
                            "sql".into(),
                            // Identifiers quoted: the EF provider replays
                            // this DDL verbatim during its keep-data rebuild,
                            // and a reserved-word table/column name (Order,
                            // Order) failed to parse there — taking the user's
                            // hand-made indexes down with it.
                            Value::Str(format!(
                                "CREATE {}INDEX {iname} ON {tbl} ({col}){pred}",
                                if d.unique { "UNIQUE " } else { "" },
                                iname = crate::stmt::sql_quote_ident(&d.name),
                                tbl = crate::stmt::sql_quote_ident(tbl),
                                col = d
                                    .columns
                                    .iter()
                                    .map(|c| crate::stmt::sql_quote_ident(c))
                                    .collect::<Vec<_>>()
                                    .join(", "),
                                pred = d
                                    .pred
                                    .as_deref()
                                    .map(|p| format!(" WHERE {p}"))
                                    .unwrap_or_default()
                            )),
                        ),
                    ]));
                }
            }
            return Ok(("sqlite_master".into(), alias, docs));
        }
        if qualified.eq_ignore_ascii_case("information_schema.tables")
            || qualified.eq_ignore_ascii_case("information_schema.columns")
        {
            let docs = self.information_schema(&qualified_lower);
            return Ok(("information_schema".into(), alias, docs));
        }
        // Oracle data-dictionary compatibility views (case-insensitive):
        // USER_* and ALL_* carry the same data — DocSQL has one global
        // namespace, so every owner sees every user table.
        let dq = qualified.to_ascii_uppercase();
        if matches!(
            dq.as_str(),
            "ALL_TABLES"
                | "USER_TABLES"
                | "ALL_TAB_COLUMNS"
                | "USER_TAB_COLUMNS"
                | "ALL_INDEXES"
                | "USER_INDEXES"
        ) {
            let owner = "DOCSQL";
            let mut docs: Vec<Object> = Vec::new();
            let user_tables: Vec<(&String, &std::sync::Arc<TableMeta>)> = self
                .tables
                .iter()
                .filter(|(n, _)| !is_internal_table(n))
                .collect();
            match dq.as_str() {
                "ALL_TABLES" | "USER_TABLES" => {
                    for (n, meta) in &user_tables {
                        docs.push(Object::from([
                            ("owner".into(), Value::Str(owner.into())),
                            ("table_name".into(), Value::Str((*n).clone())),
                            ("num_rows".into(), Value::Null),
                            ("blocks".into(), Value::Int(meta.pages.len() as i64)),
                        ]));
                    }
                }
                "ALL_TAB_COLUMNS" | "USER_TAB_COLUMNS" => {
                    for (n, meta) in &user_tables {
                        for (i, col) in meta.columns.iter().enumerate() {
                            docs.push(Object::from([
                                ("owner".into(), Value::Str(owner.into())),
                                ("table_name".into(), Value::Str((*n).clone())),
                                ("column_name".into(), Value::Str(col.clone())),
                                ("data_type".into(), Value::Str("TEXT".into())),
                                (
                                    "nullable".into(),
                                    Value::Str(
                                        if meta.not_null.contains(col) {
                                            "N"
                                        } else {
                                            "Y"
                                        }
                                        .into(),
                                    ),
                                ),
                                ("column_id".into(), Value::Int(i as i64 + 1)),
                            ]));
                        }
                    }
                }
                _ => {
                    for (tbl, meta) in &user_tables {
                        for d in &meta.index_defs {
                            docs.push(Object::from([
                                ("owner".into(), Value::Str(owner.into())),
                                ("index_name".into(), Value::Str(d.name.clone())),
                                ("table_name".into(), Value::Str((*tbl).clone())),
                                (
                                    "uniqueness".into(),
                                    Value::Str(
                                        if d.unique { "UNIQUE" } else { "NONUNIQUE" }.into(),
                                    ),
                                ),
                            ]));
                        }
                    }
                }
            }
            let view = match dq.as_str() {
                v if v.starts_with("ALL_") => v.to_string(),
                v => format!("USER_{}", &v[5..]),
            };
            return Ok((view, alias, docs));
        }
        // User view (CREATE VIEW): expand to the stored query's rows. The
        // expansion re-executes the view body as a plain SELECT over the
        // current catalog (a view of a view recurses naturally). CREATE
        // OR REPLACE can still produce a catalog cycle despite the
        // creation-time checks, so every expansion level is budgeted —
        // a cycle or an absurdly deep chain errors instead of blowing
        // the stack.
        if let Some(meta) = self.tables.get(&tname) {
            if meta.is_view() {
                let _guard = enter_view_expansion()?;
                let body = meta.view_sql.clone().unwrap_or_default();
                let q = Database::plain_read_query(&Database::parse_classified(&body)?)?;
                let ExecOutcome::Rows(r) = self.exec_query(q)? else {
                    return err(format!("view {tname} body must be a SELECT"));
                };
                let docs: Vec<Object> = r
                    .rows
                    .into_iter()
                    .map(|row| r.columns.iter().cloned().zip(row).collect())
                    .collect();
                return Ok((tname, alias, docs));
            }
        }
        let docs = self.table_docs(&tname)?;
        Ok((tname, alias, docs))
    }

    pub(super) fn agg_spec(&self, e: &SqlExpr, group_exprs: &[SqlExpr]) -> Result<AggSpec> {
        // A projection item equal to a GROUP BY expression echoes its key.
        for (i, g) in group_exprs.iter().enumerate() {
            if format!("{g}") == format!("{e}") {
                return Ok(AggSpec::GroupKey { idx: i });
            }
        }
        if let SqlExpr::Identifier(i) = e {
            // Bare column: allowed only if some group expr is that column.
            for (i2, g) in group_exprs.iter().enumerate() {
                if matches!(g, SqlExpr::Identifier(gi) if gi.value == i.value) {
                    return Ok(AggSpec::GroupKey { idx: i2 });
                }
            }
            return err(format!(
                "column {} must appear in GROUP BY or an aggregate",
                i.value
            ));
        }
        if let SqlExpr::Function(f) = e {
            if f.name.to_string().eq_ignore_ascii_case("grouping") {
                // GROUPING(expr): expression must be one of the grouping-set
                // expressions (validated here, resolved per set at eval).
                let arg = match &f.args {
                    sqlparser::ast::FunctionArguments::List(list) if list.args.len() == 1 => {
                        match &list.args[0] {
                            sqlparser::ast::FunctionArg::Unnamed(
                                sqlparser::ast::FunctionArgExpr::Expr(inner),
                            ) => inner.clone(),
                            _ => return err("GROUPING argument must be a column expression"),
                        }
                    }
                    _ => return err("GROUPING takes exactly one argument"),
                };
                for (i, g) in group_exprs.iter().enumerate() {
                    let same = format!("{g}") == format!("{arg}")
                        || matches!((g, &arg),
                            (SqlExpr::Identifier(a), SqlExpr::Identifier(b)) if a.value == b.value);
                    if same {
                        return Ok(AggSpec::Grouping { idx: i });
                    }
                }
                return err(format!("GROUPING argument {arg} must appear in GROUP BY"));
            }
            if is_agg_fn(f) {
                let (op, inner, distinct, sep, filter) = agg_parts(f, false)?;
                return Ok(AggSpec::Agg {
                    op,
                    arg: inner,
                    distinct,
                    sep,
                    filter,
                });
            }
        }
        // Composite expressions containing aggregate calls.
        if contains_agg(e) {
            check_group_refs(e, group_exprs)?;
            return Ok(AggSpec::Composite { expr: e.clone() });
        }
        err(format!("unsupported aggregate projection: {e}"))
    }

    /// GROUP BY + aggregates (+ HAVING).
    /// GROUP BY + aggregates (+ HAVING) over one or more grouping sets
    /// (`GROUP BY` is one set; ROLLUP/CUBE/GROUPING SETS expand to several).
    /// Rows of a set that omits an expression emit NULL for it; GROUPING()
    /// distinguishes that from a NULL value.
    pub(super) fn exec_grouped_select(
        &self,
        query: Query,
        select: sqlparser::ast::Select,
        rows: Vec<Object>,
        grouping_sets: Vec<Vec<SqlExpr>>,
    ) -> Result<ExecOutcome> {
        // Union of every set expression, in first-appearance order:
        // projection specs resolve against it, and each set resolves the
        // subset it carries.
        let mut all_exprs: Vec<SqlExpr> = Vec::new();
        for set in &grouping_sets {
            for e in set {
                if !all_exprs.iter().any(|g| format!("{g}") == format!("{e}")) {
                    all_exprs.push(e.clone());
                }
            }
        }
        // Projection must be aggregate functions or group exprs.
        let mut columns = Vec::new();
        let mut agg_specs = Vec::new(); // per column: AggSpec
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(e) => {
                    columns.push(expr_name(e));
                    agg_specs.push(self.agg_spec(e, &all_exprs)?);
                }
                SelectItem::ExprWithAlias { expr, alias, .. } => {
                    columns.push(alias.value.clone());
                    agg_specs.push(self.agg_spec(expr, &all_exprs)?);
                }
                _ => return err("unsupported item in aggregate SELECT"),
            }
        }
        let mut out: Vec<Vec<Value>> = Vec::new();
        if let Some(having) = &select.having {
            // Resolve references before evaluating: a bare column that is
            // neither grouped nor projected would silently read NULL and drop
            // every group instead of erroring.
            check_having_refs(having, &all_exprs, &columns)?;
        }
        for set in &grouping_sets {
            // Statement timeout reaches here too: the sets × rows × groups
            // loops below run outside the WHERE scan's own sampling.
            self.deadline.check()?;
            // Group the rows by this set's expressions. Groups hash by the
            // encoded key — the same byte identity DISTINCT uses — instead
            // of a linear scan per row (O(rows × groups)).
            let mut groups: Vec<(Vec<Value>, Vec<&Object>)> = Vec::new();
            let mut group_index: std::collections::HashMap<Vec<u8>, usize> =
                std::collections::HashMap::new();
            for doc in &rows {
                // Inside the row loop too: one set over a big table must
                // stay interruptible (the per-set check above samples only
                // once per set).
                self.deadline.check()?;
                let key: Vec<Value> = set
                    .iter()
                    .map(|e| eval_expr(e, doc))
                    .collect::<Result<_>>()?;
                let kb = row_key(&key);
                match group_index.get(&kb) {
                    Some(&gi) => groups[gi].1.push(doc),
                    None => {
                        group_index.insert(kb, groups.len());
                        groups.push((key, vec![doc]));
                    }
                }
            }
            // An empty set is one group over everything, even with no rows.
            if set.is_empty() && groups.is_empty() {
                groups.push((vec![], vec![]));
            }
            for (key, docs) in &groups {
                self.deadline.check()?;
                let mut row = Vec::with_capacity(agg_specs.len());
                for spec in &agg_specs {
                    row.push(eval_grouped_spec(spec, &all_exprs, set, key, docs)?);
                }
                // HAVING: aggregates evaluate over the group's rows directly;
                // everything else evaluates against the output columns, with
                // this set's expressions consulted before them so group keys
                // resolve and omitted ones read as NULL.
                if let Some(having) = &select.having {
                    let doc: Object = columns.iter().cloned().zip(row.iter().cloned()).collect();
                    if !matches!(eval_group_expr(having, &doc, docs, set)?, Value::Bool(true)) {
                        continue;
                    }
                }
                out.push(row);
            }
        }
        // DISTINCT applies to the projected rows here too: when GROUP BY
        // keys are finer than the projection (or GROUPING SETS repeat a
        // shape) duplicate output rows must fold — the plain path does the
        // same at this position, by encoded row identity. DISTINCT ON stays
        // an explicit unsupported feature instead of silently passing.
        let out = match &select.distinct {
            Some(sqlparser::ast::Distinct::On(cols)) => {
                return err(format!(
                    "DISTINCT ON is not supported: {}",
                    cols.iter().map(expr_name).collect::<Vec<_>>().join(", ")
                ));
            }
            Some(sqlparser::ast::Distinct::Distinct) => {
                let mut seen = std::collections::BTreeSet::new();
                let mut kept = Vec::with_capacity(out.len());
                for row in out {
                    self.deadline.check()?;
                    let key = encode::encode_to_vec(&Value::Array(row.clone()))
                        .map_err(SqlError::Encode)?;
                    if seen.insert(key) {
                        kept.push(row);
                    }
                }
                kept
            }
            None | Some(sqlparser::ast::Distinct::All) => out,
        };
        let out = self.apply_order_limit(query, out, &columns, None, None)?;
        Ok(ExecOutcome::Rows(QueryResult { columns, rows: out }))
    }

    /// No aggregation: project expressions over rows.
    pub(super) fn exec_plain_select(
        &self,
        query: Query,
        mut select: sqlparser::ast::Select,
        rows: Vec<Object>,
        pre_windowed: bool,
    ) -> Result<ExecOutcome> {
        let mut want_star = false;
        let mut project: Vec<(String, SqlExpr)> = Vec::new();
        for item in &select.projection {
            match item {
                SelectItem::Wildcard(_) => want_star = true,
                SelectItem::UnnamedExpr(e) => project.push((expr_name(e), e.clone())),
                SelectItem::ExprWithAlias { expr, alias, .. } => {
                    project.push((alias.value.clone(), expr.clone()))
                }
                _ => return err("unsupported select item"),
            }
        }
        let columns_out: Vec<String> = if want_star {
            // SELECT *, expr: star fields first, then the explicit
            // projections (SQLite semantics) — dropping the exprs silently
            // returned fewer columns than the statement asked for. The
            // ROWNUM pseudo-column (if injected) is not part of * — Oracle
            // semantics.
            let mut cols: Vec<String> = union_of_fields(&rows)
                .into_iter()
                .filter(|c| c != "ROWNUM")
                .collect();
            if cols.is_empty() {
                // Empty rowset: `SELECT *` still owes its column metadata —
                // ADO readers (EF SchemaSync's ExistingColumns via
                // `SELECT * … LIMIT 0`) learn the shape from the names, and
                // a data-derived union left them with ZERO columns on an
                // empty table. Declared columns of the FROM factors are the
                // shape; the SAME qualification rule as load_from applies:
                // a solo table keeps bare names, anything joined carries
                // "alias.col" keys (the merged-row key shape) — matching
                // what the same query returns with rows present. Views keep
                // zero columns honestly (their shape is the stored query's,
                // not the catalog's).
                let joined = select.from.len() > 1
                    || select.from.first().is_some_and(|f| !f.joins.is_empty());
                let mut v: Vec<String> = Vec::new();
                for twj in &select.from {
                    collect_star_columns(&twj.relation, self.tables, &mut v, joined);
                    for j in &twj.joins {
                        collect_star_columns(&j.relation, self.tables, &mut v, true);
                    }
                }
                v.sort();
                v.dedup();
                cols = v;
            }
            cols.extend(project.iter().map(|(n, _)| n.clone()));
            cols
        } else {
            project.iter().map(|(n, _)| n.clone()).collect()
        };

        let mut docs = rows;
        // SELECT-list + QUALIFY window functions: compute over the filtered
        // rowset (SQL window semantics — after WHERE, before DISTINCT/ORDER
        // BY/QUALIFY), then rewrite each call site into a reference of its
        // private computed column so projection/QUALIFY evaluation is plain
        // column reads.
        let mut win_calls = window_calls_of(&select.projection);
        if let Some(q) = &select.qualify {
            for c in window_calls_of_expr(q) {
                if !win_calls.iter().any(|w| format!("{w}") == format!("{c}")) {
                    win_calls.push(c);
                }
            }
        }
        let mut win_tmps: Vec<String> = Vec::new();
        if !win_calls.is_empty() {
            if pre_windowed {
                return err("internal: window function reached the pre-windowed fast path");
            }
            // Declared columns of the FROM tables: a column ADDed without a
            // DEFAULT never appears in stored rows, so the row-key universe
            // alone would misjudge its window references as unknown.
            let mut from_cols: std::collections::HashSet<String> = std::collections::HashSet::new();
            for twj in &select.from {
                collect_factor_columns(&twj.relation, self.tables, &mut from_cols);
                for j in &twj.joins {
                    collect_factor_columns(&j.relation, self.tables, &mut from_cols);
                }
            }
            win_tmps = self.compute_windows(&win_calls, &mut docs, &from_cols)?;
            for (_, e) in project.iter_mut() {
                rewrite_windows(e, &win_calls, &win_tmps);
            }
        }
        // QUALIFY (SQL:1999 T618, Snowflake/DuckDB/Teradata spelling):
        // row-local filter over the windowed rowset — after windows, before
        // DISTINCT/ORDER BY. Identical window calls share the projection's
        // computed columns (dedupe by rendered key). A windowless QUALIFY
        // (plain columns only) is a late WHERE and filters the same way —
        // dropping the filter would silently return unqualified rows.
        if let Some(q) = &mut select.qualify {
            rewrite_windows(q, &win_calls, &win_tmps);
            let outer_from = {
                let mut s = OuterFrom::new();
                from_list_names(&select.from, &mut s);
                s
            };
            docs.retain(|doc| {
                matches!(
                    self.eval_row_expr(q, expr_has_subquery(q), doc, &outer_from),
                    Ok(Value::Bool(true))
                )
            });
        }
        let mut out: Vec<Vec<Value>> = Vec::new();
        // Correlated-subquery support: projection expressions still holding
        // subquery nodes resolve their outer references per row.
        let outer_from = {
            let mut s = OuterFrom::new();
            from_list_names(&select.from, &mut s);
            s
        };
        let corr: Vec<bool> = project.iter().map(|(_, e)| expr_has_subquery(e)).collect();
        for doc in &docs {
            // The projection loop is the only per-row work a WHERE-less
            // SELECT does over a big table — keep the statement timeout
            // armed here (the WHERE path samples inside matches_in).
            self.deadline.check()?;
            if want_star {
                let mut row: Vec<Value> = columns_out
                    .iter()
                    .map(|c| doc.get(c).cloned().unwrap_or(Value::Null))
                    .collect();
                // The trailing project.len() slots are the explicit exprs.
                let base = columns_out.len() - project.len();
                for (i, (_, e)) in project.iter().enumerate() {
                    row[base + i] = self.eval_row_expr(e, corr[i], doc, &outer_from)?;
                }
                out.push(row);
            } else {
                let mut row = Vec::with_capacity(project.len());
                for (i, (_, e)) in project.iter().enumerate() {
                    row.push(self.eval_row_expr(e, corr[i], doc, &outer_from)?);
                }
                out.push(row);
            }
        }
        // DISTINCT: drop duplicate projected rows; the first occurrence's
        // source doc stays for ORDER BY evaluation. DISTINCT ON (...) is a
        // different (unsupported) feature and must not pass silently.
        match &select.distinct {
            Some(sqlparser::ast::Distinct::On(cols)) => {
                return err(format!(
                    "DISTINCT ON is not supported: {}",
                    cols.iter().map(expr_name).collect::<Vec<_>>().join(", ")
                ));
            }
            Some(sqlparser::ast::Distinct::Distinct) => {
                let mut seen = std::collections::BTreeSet::new();
                let mut kept_docs = Vec::with_capacity(docs.len());
                let mut kept_rows = Vec::with_capacity(out.len());
                for (doc, row) in docs.into_iter().zip(out) {
                    self.deadline.check()?;
                    let key = encode::encode_to_vec(&Value::Array(row.clone()))
                        .map_err(SqlError::Encode)?;
                    if seen.insert(key) {
                        kept_docs.push(doc);
                        kept_rows.push(row);
                    }
                }
                docs = kept_docs;
                out = kept_rows;
            }
            None | Some(sqlparser::ast::Distinct::All) => {}
        }
        if !pre_windowed {
            out =
                self.apply_order_limit(query, out, &columns_out, Some(docs), Some(&outer_from))?;
        }
        Ok(ExecOutcome::Rows(QueryResult {
            columns: columns_out,
            rows: out,
        }))
    }

    /// Evaluate one SELECT list's window functions over the (already
    /// WHERE-filtered) rows and inject the results as private `__w<i>$`
    /// columns. Row order in `rows` is the stable scan order: it defines
    /// the arbitrary-but-deterministic order when the OVER spec carries no
    /// ORDER BY, and the tie order the stable sort preserves. Returns the
    /// private column chosen per call (collision-free against the rows' own
    /// fields) for the projection rewrite to reference.
    pub(super) fn compute_windows(
        &self,
        calls: &[Function],
        rows: &mut [Object],
        from_cols: &std::collections::HashSet<String>,
    ) -> Result<Vec<String>> {
        // Column universe for the unknown-column guard below, doubling as
        // the name pool the private `__w<i>$` columns must not collide with.
        // Row keys carry the JOIN-qualified names; `from_cols` adds the
        // tables' declared columns (present in meta even when no stored row
        // carries them yet).
        let mut occupied: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut cols: std::collections::HashSet<String> = std::collections::HashSet::new();
        for r in rows.iter() {
            for k in r.keys() {
                occupied.insert(k.clone());
                cols.insert(k.clone());
            }
        }
        for c in from_cols {
            occupied.insert(c.clone());
            cols.insert(c.clone());
        }
        let mut tmp_names: Vec<String> = Vec::with_capacity(calls.len());
        for (ci, f) in calls.iter().enumerate() {
            self.deadline.check()?;
            let fname = f.name.to_string().to_uppercase();
            let aggregate = is_agg_name(f);
            let spec = validate_window_spec(f, &fname, aggregate)?;
            // Custom frame (ROWS/RANGE/GROUPS BETWEEN … AND …): structure
            // checked and offsets const-evaluated once per call.
            let frame_plan = match &spec.window_frame {
                Some(fr) => Some(parse_window_frame(fr, spec.order_by.len())?),
                None => None,
            };
            // Unknown columns evaluate to NULL on every row, which for a
            // window key silently collapses the whole computation (RANK all
            // 1s, running totals over the entire partition) — refuse the
            // statement instead. Resolution mirrors `lookup_col`: the exact
            // key, or any joined "alias.col" key with a matching leaf. An
            // EMPTY rowset carries no column universe — skip (there is
            // nothing to compute anyway) rather than rejecting everything.
            if !rows.is_empty() {
                for e in spec
                    .partition_by
                    .iter()
                    .chain(spec.order_by.iter().map(|o| &o.expr))
                {
                    check_window_columns(e, &cols)?;
                }
                for a in fn_args(f) {
                    check_window_columns(&a, &cols)?;
                }
                if let Some(flt) = &f.filter {
                    check_window_columns(flt, &cols)?;
                }
            }
            let ordered = !spec.order_by.is_empty();
            // Partition keys per row: groups hash by encoded key, the same
            // identity DISTINCT and GROUP BY use.
            let mut groups: Vec<(Vec<u8>, Vec<usize>)> = Vec::new();
            let mut part_index: std::collections::HashMap<Vec<u8>, usize> =
                std::collections::HashMap::new();
            for (i, doc) in rows.iter().enumerate() {
                self.deadline.check()?;
                let mut key = Vec::with_capacity(spec.partition_by.len());
                for p in &spec.partition_by {
                    key.push(eval_expr(p, doc)?);
                }
                let kb = row_key(&key);
                match part_index.get(&kb) {
                    Some(&gi) => groups[gi].1.push(i),
                    None => {
                        part_index.insert(kb.clone(), groups.len());
                        groups.push((kb, vec![i]));
                    }
                }
            }
            // Aggregates fold incrementally over the growing default frame
            // (RunningAgg below); scalar window functions are computed here.
            let agg = if aggregate {
                let (op, arg, distinct, sep, filter) = agg_parts(f, true)?;
                Some((op, arg, distinct, sep, filter))
            } else {
                None
            };
            let mut vals: Vec<Value> = vec![Value::Null; rows.len()];
            for (_, idxs) in &groups {
                self.deadline.check()?;
                // Sorted order within the partition: the outer ORDER BY
                // comparator (cmp_maybe_null + per-key ASC), stable so ties
                // keep scan order. `keyed` pairs each row index with its
                // order-key tuple; peer runs compare consecutive tuples.
                let mut keyed: Vec<(usize, Vec<Value>)> = Vec::with_capacity(idxs.len());
                for &i in idxs {
                    self.deadline.check()?;
                    let mut ks = Vec::with_capacity(spec.order_by.len());
                    for o in &spec.order_by {
                        ks.push(eval_expr(&o.expr, &rows[i])?);
                    }
                    keyed.push((i, ks));
                }
                if ordered {
                    keyed.sort_by(|a, b| {
                        for ((va, vb), o) in a.1.iter().zip(b.1.iter()).zip(&spec.order_by) {
                            let ord = cmp_maybe_null(va, vb, o.options.nulls_first);
                            if ord != Ordering::Equal {
                                return if o.options.asc.unwrap_or(true) {
                                    ord
                                } else {
                                    ord.reverse()
                                };
                            }
                        }
                        Ordering::Equal
                    });
                }
                let sorted: Vec<usize> = keyed.iter().map(|(i, _)| *i).collect();
                let n = sorted.len();
                // Peer runs: run_end[p] / run_start[p] bound the run holding
                // p (consecutive order-key tuples compare equal);
                // group_spans/group_of carry the same layout for GROUPS
                // frame units. Unordered partitions treat every row as its
                // own group.
                let mut run_end = vec![n - 1; n];
                let mut run_start = vec![0usize; n];
                let mut group_spans: Vec<(usize, usize)> = if ordered {
                    Vec::new()
                } else {
                    (0..n).map(|i| (i, i)).collect()
                };
                let mut group_of: Vec<usize> = if ordered {
                    vec![0; n]
                } else {
                    (0..n).collect()
                };
                if ordered {
                    let mut p = 0usize;
                    while p < n {
                        let mut q = p;
                        while q + 1 < n {
                            let equal = spec
                                .order_by
                                .iter()
                                .zip(&keyed[p].1)
                                .zip(&keyed[q + 1].1)
                                .all(|((o, ka), kb)| {
                                    cmp_maybe_null(ka, kb, o.options.nulls_first) == Ordering::Equal
                                });
                            if !equal {
                                break;
                            }
                            q += 1;
                        }
                        run_end[p..=q].fill(q);
                        run_start[p..=q].fill(p);
                        let gi = group_spans.len();
                        group_spans.push((p, q));
                        group_of[p..=q].fill(gi);
                        p = q + 1;
                    }
                }
                if let Some((op, arg, distinct, sep, filter)) = &agg {
                    let count_star =
                        matches!(arg, SqlExpr::Identifier(i) if i.value == "__count__");
                    // FILTER first, argument second — exactly the batch
                    // order in eval_agg: an argument expression that only
                    // errors on filtered-out rows (e.g. a bitwise op on a
                    // text row) must not fail the whole window.
                    let mut passes: Vec<bool> = Vec::with_capacity(n);
                    let mut argvals: Vec<Option<Value>> = Vec::with_capacity(n);
                    for &i in &sorted {
                        self.deadline.check()?;
                        let kept = match filter {
                            Some(flt) => matches!(eval_expr(flt, &rows[i])?, Value::Bool(true)),
                            None => true,
                        };
                        passes.push(kept);
                        argvals.push(if kept {
                            Some(eval_expr(arg, &rows[i])?)
                        } else {
                            None
                        });
                    }
                    // Custom frame: per-row [lo, hi] bounds over the sorted
                    // partition.
                    let bounds = match &frame_plan {
                        Some(p) => Some(frame_bounds(
                            p,
                            &keyed,
                            &spec.order_by,
                            &group_spans,
                            &group_of,
                            n,
                        )?),
                        None => None,
                    };
                    match bounds {
                        None => {
                            // Default frame = partition start through the
                            // current row's peer run (RANGE). Without ORDER
                            // BY the frame is the whole partition. Every
                            // frame is a PREFIX of the partition, so one
                            // left-to-right fold reproduces the batch
                            // `eval_agg` over each frame exactly (same
                            // evaluation order, same NULL/DISTINCT/FILTER
                            // and tie rules) in O(n) evaluations instead of
                            // re-folding each frame O(n²).
                            let mut run =
                                RunningAgg::new(op.clone(), sep.clone(), *distinct, count_star);
                            let mut pushed = 0usize; // sorted rows [0..pushed) are folded in
                            let mut p = 0usize;
                            while p < n {
                                let end = run_end[p];
                                while pushed <= end {
                                    run.push_row(passes[pushed], argvals[pushed].as_ref())?;
                                    pushed += 1;
                                }
                                let v = run.snapshot();
                                for r in p..=end {
                                    vals[sorted[r]] = v.clone();
                                }
                                p = end + 1;
                            }
                        }
                        Some(bounds) => {
                            // Additive aggregates and MIN/MAX invert: the
                            // frame slides in O(n) total. DISTINCT and
                            // GROUP_CONCAT cannot invert — a fresh fold per
                            // row (the GROUP_CONCAT text is inherently
                            // frame-sized anyway).
                            let invertible = matches!(
                                op,
                                AggOp::Count | AggOp::Sum | AggOp::Avg | AggOp::Min | AggOp::Max
                            ) && !distinct;
                            if invertible {
                                let mut win = SlidingFrameAgg::new(op.clone(), count_star);
                                let mut next = 0usize; // next sorted position to push
                                for p in 0..n {
                                    self.deadline.check()?;
                                    let (lo, hi) = bounds[p];
                                    if hi < lo || win.degraded {
                                        // Empty frame (a value interval that
                                        // excludes the key) or unrecoverable
                                        // sliding state: fresh fold of the
                                        // slice — an empty slice keeps the
                                        // empty aggregate value (COUNT 0,
                                        // SUM/AVG/MIN/MAX NULL).
                                        let mut run = RunningAgg::new(
                                            op.clone(),
                                            sep.clone(),
                                            false,
                                            count_star,
                                        );
                                        for r in lo..=hi {
                                            run.push_row(passes[r], argvals[r].as_ref())?;
                                        }
                                        vals[sorted[p]] = run.snapshot();
                                        continue;
                                    }
                                    while next <= hi {
                                        win.push(next, passes[next], argvals[next].as_ref())?;
                                        next += 1;
                                    }
                                    win.pop_to(lo);
                                    if win.degraded {
                                        // The retraction this very row triggered
                                        // was unrecoverable: fold the frame fresh
                                        // (future rows keep taking this branch).
                                        let mut run = RunningAgg::new(
                                            op.clone(),
                                            sep.clone(),
                                            false,
                                            count_star,
                                        );
                                        for r in lo..=hi {
                                            run.push_row(passes[r], argvals[r].as_ref())?;
                                        }
                                        vals[sorted[p]] = run.snapshot();
                                        continue;
                                    }
                                    vals[sorted[p]] = win.value();
                                }
                            } else {
                                for p in 0..n {
                                    self.deadline.check()?;
                                    let (lo, hi) = bounds[p];
                                    let mut run = RunningAgg::new(
                                        op.clone(),
                                        sep.clone(),
                                        *distinct,
                                        count_star,
                                    );
                                    for r in lo..=hi {
                                        run.push_row(passes[r], argvals[r].as_ref())?;
                                    }
                                    vals[sorted[p]] = run.snapshot();
                                }
                            }
                        }
                    }
                } else {
                    let args = fn_args(f);
                    // A frame clause rides only the value functions:
                    // ranking/offset functions have frame-independent
                    // semantics (SQL standard: they don't accept one).
                    let takes_frame = matches!(fname.as_str(), "FIRST_VALUE" | "LAST_VALUE");
                    if frame_plan.is_some() && !takes_frame {
                        return err(format!(
                            "window function {fname} does not accept a frame clause"
                        ));
                    }
                    // FIRST/LAST_VALUE walk the frame: default bounds are
                    // partition start .. peer-run end (the RANGE default).
                    let scalar_bounds = if takes_frame {
                        Some(match &frame_plan {
                            Some(p) => {
                                frame_bounds(p, &keyed, &spec.order_by, &group_spans, &group_of, n)?
                            }
                            None => (0..n).map(|p| (0, run_end[p])).collect(),
                        })
                    } else {
                        None
                    };
                    match fname.as_str() {
                        "ROW_NUMBER" | "RANK" | "DENSE_RANK" => {
                            if !args.is_empty() {
                                return err(format!("function {fname} takes no arguments"));
                            }
                            let mut dense = 0i64;
                            let mut p = 0usize;
                            while p < n {
                                self.deadline.check()?;
                                let end = run_end[p];
                                dense += 1;
                                for r in p..=end {
                                    vals[sorted[r]] = match fname.as_str() {
                                        "ROW_NUMBER" => Value::Int(r as i64 + 1),
                                        "RANK" => Value::Int(p as i64 + 1),
                                        _ => Value::Int(dense),
                                    };
                                }
                                p = end + 1;
                            }
                        }
                        "NTILE" => {
                            let buckets = match eval_const(args.first().ok_or_else(|| {
                                SqlError::Message(
                                    "function NTILE takes exactly 1 argument, got 0".into(),
                                )
                            })?)? {
                                Value::Int(b) if b >= 1 => b as usize,
                                other => {
                                    return err(format!(
                                        "NTILE buckets must be an integer >= 1, got {}",
                                        other.type_name()
                                    ));
                                }
                            };
                            let q = n / buckets;
                            let r = n % buckets;
                            for (p, &ri) in sorted.iter().enumerate() {
                                self.deadline.check()?;
                                let b = if q == 0 {
                                    p
                                } else if p < r * (q + 1) {
                                    p / (q + 1)
                                } else {
                                    r + (p - r * (q + 1)) / q
                                };
                                vals[ri] = Value::Int(b as i64 + 1);
                            }
                        }
                        "LAG" | "LEAD" => {
                            if args.is_empty() || args.len() > 3 {
                                return err(format!(
                                    "function {fname} takes 1 to 3 arguments, got {}",
                                    args.len()
                                ));
                            }
                            let offset = match args.get(1).map(eval_const).transpose()? {
                                None => 1i64,
                                Some(Value::Int(o)) if o >= 0 => o,
                                Some(other) => {
                                    return err(format!(
                                        "{fname} offset must be a non-negative integer, got {}",
                                        other.type_name()
                                    ));
                                }
                            };
                            for (p, &ri) in sorted.iter().enumerate() {
                                self.deadline.check()?;
                                let target: Option<usize> = if fname == "LAG" {
                                    p.checked_sub(offset as usize)
                                } else {
                                    p.checked_add(offset as usize).filter(|&t| t < n)
                                };
                                vals[ri] = match target {
                                    Some(t) => eval_expr(&args[0], &rows[sorted[t]])?,
                                    None => match args.get(2) {
                                        Some(d) => eval_expr(d, &rows[ri])?,
                                        None => Value::Null,
                                    },
                                };
                            }
                        }
                        "FIRST_VALUE" | "LAST_VALUE" => {
                            if args.len() != 1 {
                                return err(format!(
                                    "function {fname} takes exactly 1 argument, got {}",
                                    args.len()
                                ));
                            }
                            let bounds = scalar_bounds
                                .as_ref()
                                .expect("computed for the frame-taking functions");
                            for (p, &ri) in sorted.iter().enumerate() {
                                self.deadline.check()?;
                                let (lo, hi) = bounds[p];
                                let target = if fname == "FIRST_VALUE" { lo } else { hi };
                                vals[ri] = eval_expr(&args[0], &rows[sorted[target]])?;
                            }
                        }
                        other => {
                            return err(format!("unknown window function: {other}"));
                        }
                    }
                }
            }
            // Private column name: never collide with a user field — a
            // user column literally named `__w0$` would otherwise be
            // silently overwritten by the injected window value.
            let mut k = ci;
            let tmp = loop {
                let cand = window_tmp_col(k);
                if occupied.insert(cand.clone()) {
                    break cand;
                }
                k += 1;
            };
            tmp_names.push(tmp.clone());
            for (i, v) in vals.into_iter().enumerate() {
                rows[i].insert(tmp.clone(), v);
            }
        }
        Ok(tmp_names)
    }

    /// Load a FROM list into merged rows: the base table plus its JOINs,
    /// then comma-separated entries as cross joins. A lone base table keeps
    /// unqualified field names; anything joined gets "alias.col" keys.
    /// A solo real table with a probe-able WHERE skips the heap scan.
    pub(super) fn load_from(
        &self,
        from: &[sqlparser::ast::TableWithJoins],
        selection: &Option<SqlExpr>,
        ctes: &Ctes,
    ) -> Result<Vec<Object>> {
        use sqlparser::ast::{JoinConstraint, JoinOperator};
        let base = &from[0];
        // Index fast path: solo base table, WHERE narrows to an indexed
        // column. Rows come straight from the B+ tree; the caller's residual
        // WHERE filter still runs over them.
        if base.joins.is_empty() && from.len() == 1 {
            if let sqlparser::ast::TableFactor::Table { name, alias, .. } = &base.relation {
                let tname = obj_name(name);
                // Same gates as the window paths: a non-plain table factor
                // (TABLESAMPLE / index hints / alias column lists / …) must
                // reach load_table_factor's loud rejections instead of being
                // silently executed as a plain scan, and compat views (dual,
                // sqlite_master, …) must not be shadowed by same-named user
                // tables depending on index presence.
                if plain_table_factor(&base.relation)
                    && !is_compat_view(&tname)
                    && self.tables.contains_key(&tname)
                    && !ctes.contains_key(&tname)
                {
                    let akey = alias.as_ref().map(|a| a.name.value.clone());
                    if let Some(pairs) =
                        self.index_probe(&tname, akey.as_deref(), selection, ctes)?
                    {
                        return Ok(pairs.into_iter().map(|(_, d)| d).collect());
                    }
                }
            }
        }
        let (bname, balias, mut bdocs) = self.load_table_factor(&base.relation, ctes)?;
        let bkey = balias.unwrap_or_else(|| bname.clone());
        let solo = base.joins.is_empty() && from.len() == 1;
        let mut rows: Vec<Object> = if solo {
            bdocs
        } else {
            bdocs.drain(..).map(|d| qualify(&d, &bkey)).collect()
        };
        // Accumulated LEFT key shape for RIGHT/FULL joins: rows carry bare
        // base columns plus qualified keys of earlier joins; an empty left
        // side cannot be sampled, so the planned list answers instead.
        let mut left_cols: Option<Vec<String>> = if solo {
            None
        } else {
            self.table_factor_columns(&base.relation, ctes)
                .map(|cs| cs.iter().map(|c| format!("{bkey}.{c}")).collect())
        };
        let mut all_joins: Vec<&sqlparser::ast::Join> = base.joins.iter().collect();
        if !solo {
            for twj in &from[1..] {
                // comma-separated FROM entries: cross join their base tables
                let (n, a, d) = self.load_table_factor(&twj.relation, ctes)?;
                let k = a.unwrap_or(n);
                let dcols = self.table_factor_columns(&twj.relation, ctes);
                rows = join_rows(
                    rows,
                    &d,
                    &k,
                    None,
                    false,
                    false,
                    self.deadline,
                    dcols.as_deref(),
                    left_cols.as_deref(),
                )?;
                if let (Some(lc), Some(dc)) = (&mut left_cols, &dcols) {
                    lc.extend(dc.iter().map(|c| format!("{k}.{c}")));
                } else {
                    left_cols = None;
                }
                all_joins.extend(twj.joins.iter());
            }
        }
        for j in all_joins {
            // CROSS/OUTER APPLY: the right side is evaluated per left row
            // (T-SQL lateral semantics). Supported over the table-function
            // family; an APPLY over a derived table is a correlated
            // subquery — refused loudly like every other correlation.
            if matches!(
                j.join_operator,
                JoinOperator::CrossApply | JoinOperator::OuterApply
            ) {
                let outer = matches!(j.join_operator, JoinOperator::OuterApply);
                // `CROSS APPLY f(..) AS t` parses as a Table factor with
                // args (same shape as `FROM f(..)`); a factor without args
                // is an APPLY over a subquery/table — a correlated shape
                // the engine refuses like every other correlation.
                if let sqlparser::ast::TableFactor::Table {
                    name,
                    alias,
                    args: Some(tf_args),
                    ..
                } = &j.relation
                {
                    rows = self.apply_table_function(
                        rows,
                        &obj_name(name),
                        alias,
                        &tf_args.args,
                        outer,
                    )?;
                    // Applied rows carry `{alias}.{fn-col}` keys; the base
                    // column shape no longer describes the left side.
                    left_cols = None;
                    continue;
                }
                // OPENJSON keeps its dedicated factor shape in APPLY
                // position (no WITH columns/path here — correlated per-row
                // evaluation of the default key/value/type form).
                if let sqlparser::ast::TableFactor::OpenJsonTable {
                    json_expr,
                    json_path,
                    columns,
                    alias,
                } = &j.relation
                {
                    if !columns.is_empty() || json_path.is_some() {
                        return err("OPENJSON WITH (...) and path arguments are not supported");
                    }
                    let Some(alias_obj) = alias else {
                        return err("OPENJSON requires a table alias (APPLY OPENJSON(...) AS j)");
                    };
                    let akey = alias_obj.name.value.clone();
                    let mut out = Vec::with_capacity(rows.len());
                    let mut produced_cols = false;
                    let mut cols: Vec<String> = tablefn_static_cols("OPENJSON", false)
                        .iter()
                        .map(|s| s.to_string())
                        .collect();
                    for lrow in &rows {
                        self.deadline.check()?;
                        let val = eval_expr(json_expr, lrow)?;
                        let docs = match crate::tsql::table_function("OPENJSON", &[val]) {
                            Some(Ok(docs)) => docs,
                            Some(Err(e)) => return Err(e),
                            None => return err("unknown table function: OPENJSON"),
                        };
                        if docs.is_empty() {
                            if outer {
                                out.push(lrow.clone());
                            }
                            continue;
                        }
                        if !produced_cols {
                            cols = docs[0].keys().cloned().collect();
                            produced_cols = true;
                        }
                        for d in docs {
                            out.push(merged_row(lrow, &qualify(&d, &akey)));
                        }
                    }
                    // Alias column renames (`APPLY OPENJSON(..) AS j(k, v, t)`)
                    // apply positionally over the qualified output keys; an
                    // empty rowset on OUTER APPLY pads explicit NULLs so the
                    // right columns cannot fall back onto same-named left
                    // fields.
                    let rename = table_alias_columns(&alias_obj.columns, cols.len())?;
                    let final_names: Vec<String> = rename.clone().unwrap_or_else(|| cols.clone());
                    let prefix = format!("{akey}.");
                    for row in &mut out {
                        if let Some(rename) = &rename {
                            for (new, old) in rename.iter().zip(cols.iter()) {
                                let old_key = format!("{akey}.{old}");
                                if let Some(v) = row.remove(&old_key) {
                                    row.insert(format!("{akey}.{new}"), v);
                                }
                            }
                        }
                        if outer && !row.keys().any(|k| k.starts_with(&prefix)) {
                            for c in &final_names {
                                row.insert(format!("{akey}.{c}"), Value::Null);
                            }
                        }
                    }
                    rows = out;
                    left_cols = None;
                    continue;
                }
                return err(
                    "APPLY over a subquery is not supported (correlated subqueries); \
                     apply a table function (STRING_SPLIT/GENERATE_SERIES/OPENJSON)",
                );
            }
            let (jname, jalias, jdocs) = self.load_table_factor(&j.relation, ctes)?;
            let jkey = jalias.unwrap_or(jname);
            let jcols = self.table_factor_columns(&j.relation, ctes);
            let (left_join, right_join, on) = match &j.join_operator {
                JoinOperator::Join(c)
                | JoinOperator::Inner(c)
                | JoinOperator::Left(c)
                | JoinOperator::LeftOuter(c)
                | JoinOperator::Right(c)
                | JoinOperator::RightOuter(c)
                | JoinOperator::FullOuter(c) => {
                    let on = match c {
                        JoinConstraint::On(e) => Some(e.clone()),
                        JoinConstraint::Using(cols) => {
                            // a USING b == ON left.b = right.b, with `left`
                            // resolved against the ACCUMULATED left rows: in
                            // a join chain the column may belong to a middle
                            // table, and binding it to the base table read
                            // the merged row as NULL (silently zero rows).
                            let mut e = None;
                            for c in cols {
                                let col = obj_name(c);
                                let suffix = format!(".{col}");
                                let owner = match rows.first() {
                                    Some(sample) => {
                                        let owners: Vec<&str> = sample
                                            .keys()
                                            .filter_map(|k| k.strip_suffix(&suffix))
                                            .collect();
                                        match owners.as_slice() {
                                            [] => {
                                                return err(format!(
                                                    "USING column {col} does not exist on the left side of the join"
                                                ))
                                            }
                                            [one] => (*one).to_string(),
                                            _ => {
                                                return err(format!(
                                                    "USING column {col} is ambiguous on the left side of the join"
                                                ))
                                            }
                                        }
                                    }
                                    // No rows yet: nothing to verify against;
                                    // keep the historical base-table binding.
                                    None => bkey.clone(),
                                };
                                let eq = SqlExpr::BinaryOp {
                                    left: Box::new(SqlExpr::Identifier(
                                        sqlparser::ast::Ident::new(format!("{owner}.{col}")),
                                    )),
                                    op: sqlparser::ast::BinaryOperator::Eq,
                                    right: Box::new(SqlExpr::Identifier(
                                        sqlparser::ast::Ident::new(format!("{jkey}.{col}")),
                                    )),
                                };
                                e = Some(match e {
                                    None => eq,
                                    Some(prev) => SqlExpr::BinaryOp {
                                        left: Box::new(prev),
                                        op: sqlparser::ast::BinaryOperator::And,
                                        right: Box::new(eq),
                                    },
                                });
                            }
                            e
                        }
                        // NATURAL would need the common-column set; the
                        // schemaless row shape has no schema to derive it at
                        // plan time, so refuse instead of cross-joining.
                        JoinConstraint::Natural => {
                            return err("NATURAL JOIN is not supported (use ON/USING)")
                        }
                        // JOIN without ON: MySQL-style cross join.
                        JoinConstraint::None => None,
                    };
                    let (mut left_join, mut right_join) = (false, false);
                    match &j.join_operator {
                        JoinOperator::Left(_) | JoinOperator::LeftOuter(_) => left_join = true,
                        JoinOperator::Right(_) | JoinOperator::RightOuter(_) => right_join = true,
                        JoinOperator::FullOuter(_) => {
                            left_join = true;
                            right_join = true;
                        }
                        _ => {}
                    }
                    (left_join, right_join, on)
                }
                JoinOperator::CrossJoin(_) => (false, false, None),
                other => return err(format!("unsupported join type: {}", join_op_name(other))),
            };
            rows = join_rows(
                rows,
                &jdocs,
                &jkey,
                on.as_ref(),
                left_join,
                right_join,
                self.deadline,
                jcols.as_deref(),
                left_cols.as_deref(),
            )?;
            if let (Some(lc), Some(jc)) = (&mut left_cols, &jcols) {
                lc.extend(jc.iter().map(|c| format!("{jkey}.{c}")));
            } else {
                left_cols = None;
            }
        }
        Ok(rows)
    }

    pub(super) fn exec_select(
        &self,
        mut query: Query,
        mut select: sqlparser::ast::Select,
        ctes: &Ctes,
    ) -> Result<ExecOutcome> {
        reject_unsupported_select_clauses(&select)?;
        // Outer scope for correlated subqueries: this level's FROM names and
        // aliases. WHERE / projection / ORDER BY are row-local surfaces and
        // may keep correlated nodes for per-row evaluation; JOIN ON, GROUP BY
        // and HAVING evaluate outside a full row (partial join images, group
        // images), where correlation must stay a loud error.
        let outer_from = {
            let mut s = OuterFrom::new();
            from_list_names(&select.from, &mut s);
            s
        };
        // Resolve uncorrelated subqueries up front so the row-local
        // expression evaluator never sees them.
        if let Some(sel) = &mut select.selection {
            self.subst_expr_scoped(sel, &outer_from, true)?;
        }
        // QUALIFY is a row-local post-window surface (same phase as WHERE,
        // evaluated after window computation) — uncorrelated subqueries
        // resolve up front like WHERE's.
        if let Some(q) = &mut select.qualify {
            self.subst_expr_scoped(q, &outer_from, true)?;
        }
        // JOIN ON conditions can carry uncorrelated subqueries too
        // (`ON a.x = (SELECT MIN(y) FROM b)`); without the same substitution
        // the row-local evaluator misreports them as correlated.
        for twj in &mut select.from {
            for j in &mut twj.joins {
                if let Some(e) = join_on_expr_mut(&mut j.join_operator) {
                    self.subst_expr_scoped(e, &outer_from, false)?;
                }
            }
        }
        // ORDER BY expressions too: EF Core emits `ORDER BY (SELECT 1)` for
        // Skip/Take without an explicit ordering key.
        if let Some(ob) = &mut query.order_by {
            if let sqlparser::ast::OrderByKind::Expressions(exprs) = &mut ob.kind {
                for o in exprs.iter_mut() {
                    self.subst_expr_scoped(&mut o.expr, &outer_from, true)?;
                }
            }
        }
        for item in &mut select.projection {
            match item {
                SelectItem::UnnamedExpr(e) => self.subst_expr_scoped(e, &outer_from, true)?,
                SelectItem::ExprWithAlias { expr, .. } => {
                    self.subst_expr_scoped(expr, &outer_from, true)?
                }
                _ => {}
            }
        }
        if let sqlparser::ast::GroupByExpr::Expressions(es, _) = &mut select.group_by {
            for e in es {
                self.subst_expr_scoped(e, &outer_from, false)?;
            }
        }
        if let Some(having) = &mut select.having {
            self.subst_expr_scoped(having, &outer_from, false)?;
        }

        if select.from.is_empty() {
            if select.qualify.is_some() {
                return err("QUALIFY requires a FROM clause (it filters windowed rows)");
            }
            // FROM-less SELECT: one row of constant expressions; WHERE
            // filters that single row.
            let mut project: Vec<(String, SqlExpr)> = Vec::new();
            for item in &select.projection {
                match item {
                    SelectItem::UnnamedExpr(e) => project.push((expr_name(e), e.clone())),
                    SelectItem::ExprWithAlias { expr, alias, .. } => {
                        project.push((alias.value.clone(), expr.clone()))
                    }
                    _ => return err("unsupported select item"),
                }
            }
            let empty = Object::new();
            let columns = project.iter().map(|(n, _)| n.clone()).collect();
            if let Some(cond) = &select.selection {
                if !matches!(eval_expr(cond, &empty)?, Value::Bool(true)) {
                    return Ok(ExecOutcome::Rows(QueryResult {
                        columns,
                        rows: vec![],
                    }));
                }
            }
            let row = project
                .iter()
                .map(|(_, e)| eval_expr(e, &empty))
                .collect::<Result<Vec<_>>>()?;
            // FROM-less SELECT still honors LIMIT/OFFSET (`SELECT 1 LIMIT 0`
            // must return zero rows, not one) and the standard FETCH clause
            // (same truncation semantics). ORDER BY over a single row cannot
            // reorder anything.
            let mut rows = vec![row];
            if let Some(lc) = &query.limit_clause {
                rows = apply_limit_clause(lc, rows)?;
            }
            if let Some(fetch) = &query.fetch {
                if fetch.percent {
                    return err("FETCH … PERCENT is not supported");
                }
                if fetch.with_ties {
                    return err("FETCH … WITH TIES requires ORDER BY");
                }
                let n = match fetch.quantity.as_ref().map(eval_const).transpose()? {
                    Some(Value::Int(n)) if n >= 0 => n as usize,
                    Some(Value::Int(n)) => {
                        return err(format!("FETCH quantity must be non-negative ({n})"))
                    }
                    Some(_) | None => return err("FETCH quantity must be an integer"),
                };
                rows.truncate(n);
            }
            return Ok(ExecOutcome::Rows(QueryResult { columns, rows }));
        }
        // Oracle ROWNUM pseudo-column: number each row right after it is
        // retrieved (before WHERE and before ORDER BY — Oracle semantics).
        // Injected only when the statement references it, and only for the
        // non-aggregated path (aggregates + ROWNUM is refused below).
        let group_exprs: Vec<SqlExpr> = match &select.group_by {
            sqlparser::ast::GroupByExpr::Expressions(e, modifiers) => {
                // MySQL `WITH ROLLUP` / ClickHouse `WITH TOTALS` modifiers
                // would be silently dropped; the standard spellings are
                // GROUP BY ROLLUP(...) / CUBE(...) / GROUPING SETS.
                if !modifiers.is_empty() {
                    return err(
                        "GROUP BY modifiers (WITH ROLLUP/CUBE/TOTALS) are not supported \
                         (use GROUP BY ROLLUP(...)/CUBE(...))",
                    );
                }
                e.clone()
            }
            sqlparser::ast::GroupByExpr::All(_) => return err("GROUP BY ALL is not supported"),
        };
        let grouping_sets = expand_grouping_sets(&group_exprs)?;
        let is_aggregate = !group_exprs.is_empty() || select.projection.iter().any(is_agg_item);
        // With GROUP BY/aggregates ROWNUM has no row stream to number — the
        // schemaless NULL read silently passed `ROWNUM <= n` filters, so
        // refuse loudly instead.
        let refs_rownum = select_refs_rownum(&select);
        if refs_rownum && is_aggregate {
            return err("ROWNUM is not supported combined with GROUP BY or aggregate projections");
        }
        let rownum_wanted = refs_rownum && !is_aggregate;
        let distinct_all = matches!(&select.distinct, None | Some(sqlparser::ast::Distinct::All));
        // SELECT-list window functions (`… OVER (…)`) need the full
        // filtered rowset: the ORDER BY+LIMIT fast paths below would hand
        // them a truncated window, so they are gated off, and mixing with
        // GROUP BY/aggregates is refused up front (window-over-groups is
        // unsupported; refusals stay loud).
        let has_window = !window_calls_of(&select.projection).is_empty()
            || select
                .qualify
                .as_ref()
                .is_some_and(|q| !window_calls_of_expr(q).is_empty());
        if has_window && is_aggregate {
            return err("window functions (OVER) are not supported combined with \
                 GROUP BY or aggregate projections");
        }
        if select.qualify.is_some() && is_aggregate {
            return err("QUALIFY is not supported combined with \
                 GROUP BY or aggregate projections");
        }

        // `SELECT COUNT(*) FROM t` without WHERE/GROUP BY: count live heap
        // slots instead of decoding every document.
        if is_aggregate {
            if let Some(out) = self.try_count_star(&query, &select, ctes)? {
                return Ok(out);
            }
        }

        // ORDER BY <index key> + LIMIT: index-ordered window, skipping the
        // full heap scan and sort. Already filtered and windowed; the WHERE
        // pass below would be a no-op, so it is skipped too.
        let mut pre_windowed = false;
        let mut rows = if !is_aggregate
            && !rownum_wanted
            && select.having.is_none()
            && distinct_all
            && !has_window
        {
            match self.ordered_index_window(
                &select,
                &select.from,
                &select.selection,
                &query.order_by,
                &query,
                ctes,
            )? {
                Some(rows) => {
                    pre_windowed = true;
                    rows
                }
                None => match self.unindexed_order_window(&select, &query, ctes)? {
                    Some(rows) => {
                        pre_windowed = true;
                        rows
                    }
                    None => self.load_from(&select.from, &select.selection, ctes)?,
                },
            }
        } else {
            self.load_from(&select.from, &select.selection, ctes)?
        };

        if rownum_wanted {
            for (i, row) in rows.iter_mut().enumerate() {
                row.insert("ROWNUM".into(), Value::Int(i as i64 + 1));
            }
        }

        // WHERE — consuming pass: matching docs move into the kept vec
        // instead of being whole-document cloned (the filtered set is
        // often the whole table).
        if !pre_windowed && select.selection.is_some() {
            let mut kept = Vec::with_capacity(rows.len());
            for doc in rows.drain(..) {
                if self.matches_in(&select.selection, &doc, &outer_from)? {
                    kept.push(doc);
                }
            }
            rows = kept;
        }

        // Aggregation path: aggregate functions in projection or GROUP BY.
        if is_aggregate {
            return self.exec_grouped_select(query, select, rows, grouping_sets);
        }
        if select.having.is_some() {
            // Silently dropping the filter would return unfiltered rows.
            return err("HAVING requires GROUP BY or an aggregate");
        }
        self.exec_plain_select(query, select, rows, pre_windowed)
    }

    /// Execute the query body (no WITH: it was materialized by the caller
    /// into `ctes`).
    pub(super) fn exec_query_body(&self, query: Query, ctes: &Ctes) -> Result<ExecOutcome> {
        let query = rewrite_tsql_top(query)?;
        reject_unsupported_query_clauses(&query)?;
        let body = query.body.clone();
        match *body {
            SetExpr::Select(select) => self.exec_select(query.clone(), *select, ctes),
            SetExpr::SetOperation {
                left,
                op,
                set_quantifier,
                right,
            } => {
                use sqlparser::ast::{SetOperator, SetQuantifier};
                if !matches!(
                    op,
                    SetOperator::Union
                        | SetOperator::Except
                        | SetOperator::Minus
                        | SetOperator::Intersect
                ) {
                    return err(format!("unsupported set operation: {op}"));
                }
                // BY NAME alignment is not implemented: silently running it
                // positionally would pair unrelated columns (and ALL BY NAME
                // would additionally lose the bag semantics to the implicit
                // DISTINCT below).
                if matches!(
                    set_quantifier,
                    SetQuantifier::ByName
                        | SetQuantifier::AllByName
                        | SetQuantifier::DistinctByName
                ) {
                    return err(
                        "UNION/INTERSECT/EXCEPT ... BY NAME is not supported (columns align positionally)".to_string(),
                    );
                }
                // Set operations combine whole result sets: strip per-arm
                // ORDER BY/LIMIT/FETCH so they apply to the combination only.
                // (Leaving `fetch` in applied `FETCH FIRST n ROWS ONLY` to
                // each arm *and* the combination, truncating the result.)
                // The WITH clause is dropped here too: the CTEs are already
                // materialized in `ctes` — re-entering exec_query per arm
                // would re-run every CTE body 2k-1 times on a k-arm chain.
                let bare = Query {
                    order_by: None,
                    limit_clause: None,
                    fetch: None,
                    with: None,
                    ..query.clone()
                };
                let l = self.exec_query_body(
                    Query {
                        body: left,
                        ..bare.clone()
                    },
                    ctes,
                )?;
                let r = self.exec_query_body(
                    Query {
                        body: right,
                        ..bare
                    },
                    ctes,
                )?;
                let (ExecOutcome::Rows(mut lr), ExecOutcome::Rows(rr)) = (l, r) else {
                    return err("set operations require SELECT on both sides");
                };
                // Column *counts* must match; names may differ (left wins).
                if lr.columns.len() != rr.columns.len() {
                    return err(format!(
                        "set operation arms have different column counts (left {}, right {})",
                        lr.columns.len(),
                        rr.columns.len()
                    ));
                }
                let all = set_quantifier == SetQuantifier::All;
                match op {
                    SetOperator::Union => {
                        lr.rows.extend(rr.rows);
                        if !all {
                            dedup_rows(&mut lr.rows);
                        }
                    }
                    SetOperator::Intersect => {
                        let mut counts = row_counts(&rr.rows);
                        let mut out = Vec::new();
                        for row in lr.rows {
                            if consume_one(&mut counts, &row) {
                                out.push(row);
                            }
                        }
                        if !all {
                            let mut out2 = out;
                            dedup_rows(&mut out2);
                            out = out2;
                        }
                        lr.rows = out;
                    }
                    // EXCEPT / MINUS share semantics.
                    SetOperator::Except | SetOperator::Minus => {
                        let mut counts = row_counts(&rr.rows);
                        let mut out = Vec::new();
                        for row in lr.rows {
                            if consume_one(&mut counts, &row) {
                                continue; // matched on the right: removed
                            }
                            out.push(row);
                        }
                        if !all {
                            let mut out2 = out;
                            dedup_rows(&mut out2);
                            out = out2;
                        }
                        lr.rows = out;
                    }
                }
                // ORDER BY / LIMIT now apply to the combined result.
                let cols = lr.columns.clone();
                let rows = self.apply_order_limit(query, lr.rows, &cols, None, None)?;
                Ok(ExecOutcome::Rows(QueryResult {
                    columns: cols,
                    rows,
                }))
            }
            SetExpr::Values(mut values) => {
                // Row-value constructor (standard SQL): constant expressions,
                // one row per parenthesized list, `column1..n` by default.
                let width = values.rows.first().map_or(0, |r| r.content.len());
                let columns: Vec<String> = (1..=width).map(|i| format!("column{i}")).collect();
                let empty = Object::new();
                let mut rows = Vec::with_capacity(values.rows.len());
                for parens in values.rows.drain(..) {
                    let mut exprs = parens.content;
                    if exprs.len() != width {
                        return err("VALUES rows must all have the same column count");
                    }
                    for e in &mut exprs {
                        self.subst_expr(e)?;
                    }
                    rows.push(
                        exprs
                            .iter()
                            .map(|e| eval_expr(e, &empty))
                            .collect::<Result<Vec<_>>>()?,
                    );
                }
                let rows = self.apply_order_limit(query, rows, &columns, None, None)?;
                Ok(ExecOutcome::Rows(QueryResult { columns, rows }))
            }
            SetExpr::Query(inner) => {
                // Parenthesized query body, e.g. `((SELECT 1))`: re-dispatch
                // the inner query (its ORDER BY/LIMIT apply) instead of
                // rejecting the shape. The parenthesized body may carry its
                // OWN WITH — materialize it on top of the current scope
                // rather than dropping it (dropping made `(WITH x AS …
                // SELECT * FROM x)` resolve x to a same-named base table or
                // fail outright).
                let mut q = *inner;
                let inner_with = q.with.take();
                let scoped = self.materialize_ctes(&inner_with, ctes)?;
                self.exec_query_body(q, &scoped)
            }
            other => err(format!("unsupported query body: {other}")),
        }
    }

    pub(super) fn exec_query(&self, query: Query) -> Result<ExecOutcome> {
        let ctes = self.materialize_ctes(&query.with, &Ctes::new())?;
        self.exec_query_body(query, &ctes)
    }
}
