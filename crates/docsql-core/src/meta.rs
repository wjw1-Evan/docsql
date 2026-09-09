//! Object-explorer metadata assembly shared by every surface that reports
//! catalog shape: the web console's `/api/meta` (local embedded engine) and
//! the server's REQ_META frame (the console's remote node switching). One
//! implementation so a remote node reports the exact same JSON the console
//! builds locally — including index definitions, observed columns, and
//! storage counters.

use crate::engine::{Database, ExecOutcome, PUBSUB_TABLE};
use crate::value::{Object, Value};
use std::path::Path;
use std::time::Instant;

fn str(s: &str) -> Value {
    Value::Str(s.to_string())
}

fn int(n: u64) -> Value {
    Value::Int(n as i64)
}

fn file_bytes(p: &Path) -> u64 {
    std::fs::metadata(p).map(|m| m.len()).unwrap_or(0)
}

fn wal_path(db: &Path) -> std::path::PathBuf {
    let mut s = db.as_os_str().to_os_string();
    s.push(".wal");
    std::path::PathBuf::from(s)
}

/// The `/api/meta` payload: server identity, storage counters, totals, and
/// one entry per user table (the pub/sub backing store is filtered out).
pub fn build_meta(db: &mut Database, db_path: &Path, started: Instant, version: &str) -> Value {
    let catalog: Vec<_> = db
        .catalog()
        .into_iter()
        // The pub/sub backing table is system storage, not a user object.
        .filter(|t| t.name != PUBSUB_TABLE)
        .collect();
    let mut tables = Vec::new();
    let mut total_rows = 0u64;
    for t in &catalog {
        let row_count = match db.execute(&format!(
            "SELECT COUNT(*) FROM \"{}\"",
            t.name.replace('"', "\"\"")
        )) {
            Ok(ExecOutcome::Rows(r)) => {
                r.rows.first().and_then(|row| row[0].as_i64()).unwrap_or(0) as u64
            }
            _ => 0,
        };
        total_rows += row_count;
        let columns: Vec<Value> = t
            .columns
            .iter()
            .map(|c| {
                Value::Object(Object::from([
                    ("name".into(), str(&c.name)),
                    ("nullable".into(), Value::Bool(c.nullable)),
                    ("primary_key".into(), Value::Bool(c.primary_key)),
                    ("unique".into(), Value::Bool(c.unique)),
                    ("autoinc".into(), Value::Bool(c.autoinc)),
                    ("data_type".into(), str(&c.data_type)),
                    // Declared DEFAULT as SQL text; Null when the column has
                    // none (the console's edit-table grid reads it to show
                    // and round-trip defaults).
                    (
                        "default".into(),
                        match &c.default_value {
                            Some(d) => str(d),
                            None => Value::Null,
                        },
                    ),
                ]))
            })
            .collect();
        // Index definitions behind the names: the console's index management
        // (create/edit/drop) needs column + uniqueness. `auto` marks PRIMARY
        // KEY/UNIQUE constraint indexes: visible, but maintained by the table
        // definition, not editable.
        let index_defs: Vec<Value> = t
            .index_defs
            .iter()
            .map(|i| {
                Value::Object(Object::from([
                    ("name".into(), str(&i.name)),
                    ("column".into(), str(&i.column)),
                    ("unique".into(), Value::Bool(i.unique)),
                    ("auto".into(), Value::Bool(i.auto)),
                ]))
            })
            .collect();
        tables.push(Value::Object(Object::from([
            ("name".into(), str(&t.name)),
            ("row_count".into(), int(row_count)),
            ("pages".into(), int(t.pages as u64)),
            (
                "keys".into(),
                Value::Array(t.keys.iter().map(|k| str(k)).collect()),
            ),
            (
                "indexes".into(),
                Value::Array(t.indexes.iter().map(|i| str(i)).collect()),
            ),
            ("index_defs".into(), Value::Array(index_defs)),
            ("columns".into(), Value::Array(columns)),
            // Data-side schema: union of top-level field names across the
            // table's documents (what SELECT * projects). Declared columns
            // never auto-sync with schemaless writes; this is how the object
            // explorer shows the drift.
            (
                "observed".into(),
                Value::Array(
                    db.observed_columns(&t.name)
                        .into_iter()
                        .map(|c| str(&c))
                        .collect(),
                ),
            ),
        ])));
    }
    Value::Object(Object::from([
        (
            "server".into(),
            Value::Object(Object::from([
                ("name".into(), str("docsql")),
                ("version".into(), str(version)),
                (
                    "uptime_ms".into(),
                    Value::Int(started.elapsed().as_millis() as i64),
                ),
            ])),
        ),
        (
            "storage".into(),
            Value::Object(Object::from([
                ("page_size".into(), int(db.page_size() as u64)),
                ("num_pages".into(), int(db.num_pages() as u64)),
                ("db_bytes".into(), int(file_bytes(db_path))),
                ("wal_bytes".into(), int(file_bytes(&wal_path(db_path)))),
            ])),
        ),
        (
            "totals".into(),
            Value::Object(Object::from([
                ("tables".into(), int(tables.len() as u64)),
                ("rows".into(), int(total_rows)),
            ])),
        ),
        ("tables".into(), Value::Array(tables)),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_reports_tables_columns_and_index_defs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.db");
        let mut d = Database::open(&path).unwrap();
        d.execute("CREATE TABLE m (id INT PRIMARY KEY, name TEXT DEFAULT 'anon')")
            .unwrap();
        d.execute("INSERT INTO m VALUES (1, 'a'), (2, 'b')")
            .unwrap();
        d.execute("CREATE INDEX mi ON m (name)").unwrap();
        let v = build_meta(&mut d, &path, Instant::now(), "1.0");
        let tables = match &v {
            Value::Object(o) => match o.get("tables") {
                Some(Value::Array(a)) => a.clone(),
                _ => panic!("tables missing"),
            },
            _ => panic!("root not an object"),
        };
        assert_eq!(tables.len(), 1);
        let fields = |t: &Value, k: &str| -> Value {
            match t {
                Value::Object(o) => o.get(k).cloned().unwrap_or(Value::Null),
                _ => panic!("table not an object"),
            }
        };
        assert_eq!(fields(&tables[0], "row_count"), Value::Int(2));
        assert_eq!(fields(&tables[0], "name"), str("m"));
        let cols = match fields(&tables[0], "columns") {
            Value::Array(a) => a,
            _ => panic!("columns missing"),
        };
        assert_eq!(cols.len(), 2);
        let id_col = match &cols[0] {
            Value::Object(o) => (
                o.get("name").cloned(),
                o.get("primary_key").cloned().unwrap_or(Value::Bool(false)),
                o.get("default").cloned().unwrap_or(Value::Bool(false)),
            ),
            _ => panic!("column not an object"),
        };
        assert_eq!(
            id_col,
            (Some(str("id")), Value::Bool(true), Value::Null),
            "PK column carries no default"
        );
        let name_default = match &cols[1] {
            Value::Object(o) => o.get("default").cloned().unwrap_or(Value::Bool(false)),
            _ => panic!("column not an object"),
        };
        assert_eq!(
            name_default,
            str("'anon'"),
            "declared DEFAULT round-trips as SQL text"
        );
        let defs = match fields(&tables[0], "index_defs") {
            Value::Array(a) => a,
            _ => panic!("index_defs missing"),
        };
        assert_eq!(defs.len(), 2); // PK autoindex + mi
        let totals = match &v {
            Value::Object(o) => o.get("totals").cloned().unwrap_or(Value::Null),
            _ => unreachable!(),
        };
        assert_eq!(
            totals,
            Value::Object(Object::from([
                ("tables".into(), Value::Int(1)),
                ("rows".into(), Value::Int(2)),
            ]))
        );
    }

    #[test]
    fn meta_hides_pubsub_backing_table() {
        let dir = tempfile::tempdir().unwrap();
        let mut d = Database::open(&dir.path().join("m.db")).unwrap();
        d.execute(&format!("CREATE TABLE {PUBSUB_TABLE} (payload TEXT)"))
            .unwrap();
        let v = build_meta(&mut d, &dir.path().join("m.db"), Instant::now(), "1.0");
        match &v {
            Value::Object(o) => match o.get("tables") {
                Some(Value::Array(a)) => assert!(a.is_empty()),
                _ => panic!("tables missing"),
            },
            _ => panic!(),
        }
    }
}
