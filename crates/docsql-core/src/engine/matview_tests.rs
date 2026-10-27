
use super::*;

fn run(db: &mut Database, sql: &str) -> ExecOutcome {
    db.execute(sql)
        .unwrap_or_else(|e| panic!("SQL failed: {sql}\n{e}"))
}

fn rows(db: &mut Database, sql: &str) -> QueryResult {
    match run(db, sql) {
        ExecOutcome::Rows(r) => r,
        other => panic!("expected rows, got {other:?}"),
    }
}

fn int_col(r: &QueryResult, c: usize) -> Vec<i64> {
    r.rows
        .iter()
        .map(|row| match &row[c] {
            Value::Int(i) => *i,
            other => panic!("expected int, got {other:?}"),
        })
        .collect()
}

fn seed() -> Database {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE orders (id INT PRIMARY KEY, k INT NOT NULL, status TEXT)",
    );
    run(
        &mut db,
        "INSERT INTO orders VALUES (1, 10, 'done'), (2, 20, 'open'), (3, 30, 'done')",
    );
    db
}

#[test]
fn matview_create_read_and_refresh_lifecycle() {
    let mut db = seed();
    run(
        &mut db,
        "CREATE MATERIALIZED VIEW done_totals AS \
             SELECT id, k FROM orders WHERE status = 'done'",
    );
    let r = rows(&mut db, "SELECT id FROM done_totals ORDER BY id");
    assert_eq!(int_col(&r, 0), vec![1, 3]);
    // Base writes do NOT propagate: the content is frozen until REFRESH.
    run(&mut db, "INSERT INTO orders VALUES (4, 40, 'done')");
    let r = rows(&mut db, "SELECT id FROM done_totals ORDER BY id");
    assert_eq!(int_col(&r, 0), vec![1, 3], "must stay stale pre-refresh");
    run(&mut db, "REFRESH MATERIALIZED VIEW done_totals");
    let r = rows(&mut db, "SELECT id FROM done_totals ORDER BY id");
    assert_eq!(int_col(&r, 0), vec![1, 3, 4]);
    // Deletes too.
    run(&mut db, "DELETE FROM orders WHERE id = 1");
    run(&mut db, "REFRESH MATERIALIZED VIEW done_totals");
    let r = rows(&mut db, "SELECT id FROM done_totals ORDER BY id");
    assert_eq!(int_col(&r, 0), vec![3, 4]);
    // Joins/aggregates are ordinary SELECT bodies.
    run(&mut db, "CREATE TABLE notes (oid INT, note TEXT)");
    run(&mut db, "INSERT INTO notes VALUES (3, 'ok')");
    run(
        &mut db,
        "CREATE MATERIALIZED VIEW note_view AS \
             SELECT o.id, n.note FROM orders o JOIN notes n ON o.id = n.oid",
    );
    let r = rows(&mut db, "SELECT note FROM note_view WHERE id = 3");
    assert_eq!(r.rows, vec![vec![Value::Str("ok".into())]], "{r:?}");
}

#[test]
fn matview_determinism_gates() {
    let mut db = seed();
    for q in [
        "SELECT NOW() AS t FROM orders",
        "SELECT GETDATE() AS t FROM orders",
        "SELECT NEWID() AS g FROM orders",
        "SELECT RAND() AS r FROM orders",
    ] {
        let e = db
            .execute(&format!("CREATE MATERIALIZED VIEW bad AS {q}"))
            .unwrap_err();
        let msg = e.to_string();
        assert!(
            msg.contains("deterministic") || msg.contains("NEWID") || msg.contains("wall"),
            "{q}: {msg}"
        );
    }
    // A CREATE that slipped through pre-validation cannot REFRESH either.
    let mut db2 = Database::in_memory().unwrap();
    {
        let mut meta = TableMeta {
            name: "legacy".into(),
            columns: vec!["v".into()],
            ..Default::default()
        };
        meta.matview_sql = Some("SELECT NOW() AS t".into());
        let _ = db2.rewrite_table("legacy", &mut meta, Vec::new());
    }
    let e = db2.execute("REFRESH MATERIALIZED VIEW legacy").unwrap_err();
    assert!(e.to_string().contains("wall-clock"), "{e}");
}

#[test]
fn matview_write_gates_and_drop() {
    let mut db = seed();
    run(
        &mut db,
        "CREATE MATERIALIZED VIEW mv AS SELECT id FROM orders WHERE k > 15",
    );
    for sql in [
        "INSERT INTO mv VALUES (9, 9)",
        "UPDATE mv SET id = 9",
        "DELETE FROM mv",
        "TRUNCATE TABLE mv",
        "ALTER TABLE mv ADD COLUMN pad TEXT",
        "ALTER TABLE mv DROP COLUMN id",
    ] {
        let e = db.execute(sql).unwrap_err().to_string();
        assert!(
            e.contains("materialized view") || e.contains("refreshed"),
            "{sql}: {e}"
        );
    }
    // Indexes are allowed (PG semantics): they probe and survive REFRESH.
    run(&mut db, "CREATE INDEX mv_k ON mv (id)");
    let r = rows(&mut db, "EXPLAIN SELECT id FROM mv WHERE id = 30");
    assert!(
        matches!(&r.rows[0][1], Value::Str(s) if s.contains("INDEX PROBE ON mv")),
        "{r:?}"
    );
    run(&mut db, "INSERT INTO orders VALUES (4, 40, 'done')");
    run(&mut db, "REFRESH MATERIALIZED VIEW mv");
    let r = rows(&mut db, "SELECT id FROM mv WHERE id = 4");
    assert_eq!(r.rows, vec![vec![Value::Int(4)]], "{r:?}");
    // Wrong-kind and missing-name errors are loud.
    run(&mut db, "CREATE TABLE plain (id INT)");
    let e = db
        .execute("REFRESH MATERIALIZED VIEW plain")
        .unwrap_err()
        .to_string();
    assert!(e.contains("not a materialized view"), "{e}");
    let e = db
        .execute("REFRESH MATERIALIZED VIEW ghost")
        .unwrap_err()
        .to_string();
    assert!(e.contains("does not exist"), "{e}");
    // DROP MATERIALIZED VIEW removes it; reads die loudly afterwards.
    run(&mut db, "DROP MATERIALIZED VIEW mv");
    let e = db.execute("SELECT id FROM mv").unwrap_err().to_string();
    assert!(e.contains("does not exist"), "{e}");
    let e = db
        .execute("DROP MATERIALIZED VIEW mv")
        .unwrap_err()
        .to_string();
    assert!(e.contains("does not exist"), "{e}");
}

#[test]
fn matview_or_replace_if_not_exists_and_mat_over_mat() {
    let mut db = seed();
    run(
        &mut db,
        "CREATE MATERIALIZED VIEW mv AS SELECT id FROM orders WHERE k > 15",
    );
    // IF NOT EXISTS on an existing mv is a no-op…
    run(
        &mut db,
        "CREATE MATERIALIZED VIEW IF NOT EXISTS mv AS SELECT id FROM orders",
    );
    let r = rows(&mut db, "SELECT COUNT(*) FROM mv");
    assert_eq!(r.rows, vec![vec![Value::Int(2)]], "{r:?}");
    // …OR REPLACE swaps the definition and the content.
    run(
        &mut db,
        "CREATE OR REPLACE MATERIALIZED VIEW mv AS SELECT id FROM orders WHERE status = 'open'",
    );
    let r = rows(&mut db, "SELECT id FROM mv");
    assert_eq!(int_col(&r, 0), vec![2]);
    // OR REPLACE over a plain table is refused (wrong kind).
    run(&mut db, "CREATE TABLE plain (id INT)");
    let e = db
        .execute("CREATE OR REPLACE MATERIALIZED VIEW plain AS SELECT 1 AS x")
        .unwrap_err()
        .to_string();
    assert!(e.contains("not a materialized view"), "{e}");
    // Mat-over-mat is refused (refresh cascades / dump order).
    let e = db
        .execute("CREATE MATERIALIZED VIEW mv2 AS SELECT id FROM mv")
        .unwrap_err()
        .to_string();
    assert!(e.contains("cannot query materialized view"), "{e}");
}

#[test]
fn matview_dump_roundtrip_and_replication_convergence() {
    let mut db = seed();
    run(
        &mut db,
        "CREATE MATERIALIZED VIEW mv AS SELECT id, k FROM orders WHERE status = 'done'",
    );
    let script = db.dump_script().unwrap();
    assert!(script.contains("CREATE MATERIALIZED VIEW"), "{script}");
    // The stored rows are NOT dumped (the replay recomputes them).
    let mv_section = &script[script.find("CREATE MATERIALIZED VIEW").unwrap()..];
    let data_section_start = script.find("INSERT INTO").unwrap();
    assert!(
        data_section_start < script.find("CREATE MATERIALIZED VIEW").unwrap(),
        "matview DDL comes after data"
    );
    assert!(
        !mv_section.contains("INSERT INTO"),
        "no stored rows for the matview"
    );
    let mut dst = Database::in_memory().unwrap();
    run(&mut dst, "BEGIN");
    let batch = dst.execute_batch(&script);
    assert!(batch.error.is_none(), "{:?}", batch.error);
    run(&mut dst, "COMMIT");
    let r = rows(&mut dst, "SELECT id FROM mv ORDER BY id");
    assert_eq!(int_col(&r, 0), vec![1, 3]);
    // The refreshed content survives the catalog (matview_sql persisted).
    run(&mut dst, "INSERT INTO orders VALUES (4, 40, 'done')");
    run(&mut dst, "REFRESH MATERIALIZED VIEW mv");
    let r = rows(&mut dst, "SELECT id FROM mv ORDER BY id");
    assert_eq!(int_col(&r, 0), vec![1, 3, 4]);
    // Replication determinism: replaying the same journal text on two
    // independent databases (CREATE included) converges byte-for-byte.
    let mut a = seed();
    let mut b = seed();
    let journal = [
        "CREATE MATERIALIZED VIEW mv AS SELECT id, k FROM orders WHERE status = 'done'",
        "INSERT INTO orders VALUES (4, 40, 'done')",
        "REFRESH MATERIALIZED VIEW mv",
    ];
    for sql in journal {
        run(&mut a, sql);
        run(&mut b, sql);
    }
    for q in [
        "SELECT * FROM mv ORDER BY id",
        "SELECT id FROM orders ORDER BY id",
    ] {
        assert_eq!(rows(&mut a, q).rows, rows(&mut b, q).rows, "{q}");
    }
}
