use crate::engine::{Database, ExecOutcome, ReadView};
use crate::value::Value;

fn count(db: &Database, sql: &str) -> i64 {
    match db.execute_read(sql).unwrap() {
        ExecOutcome::Rows(r) => r.rows[0][0].as_i64().unwrap(),
        _ => panic!("expected rows"),
    }
}

fn view_count(view: &ReadView, sql: &str) -> i64 {
    match view.execute(sql).unwrap() {
        ExecOutcome::Rows(r) => r.rows[0][0].as_i64().unwrap(),
        _ => panic!("expected rows"),
    }
}

/// A snapshot whose head lands on an UNFENCED deferred commit (created
/// between two statements of an open explicit transaction) must serve
/// that statement's effects from both paths: the fast path from the
/// pool, and — once a later write pushes the page off it — the slow
/// path from the WAL. The slow path used to require a covering fence
/// and failed such snapshots with a false "history predates the
/// retention window".
#[test]
fn snapshot_across_open_transaction_reads_consistently() {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE items (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO items VALUES (1, 10), (2, 20)")
        .unwrap();

    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO items VALUES (3, 30)").unwrap(); // deferred, unfenced
    let view = db.read_view(); // head includes the deferred commit
    assert_eq!(view_count(&view, "SELECT COUNT(*) FROM items"), 3);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM items"), 3);

    // Push the same pages past the snapshot: the next read takes the
    // slow path and must reconstruct the as-of image (count 3, the
    // pre-UPDATE values) — not error, not the newer image.
    db.execute("UPDATE items SET v = v + 100").unwrap();
    assert_eq!(view_count(&view, "SELECT COUNT(*) FROM items"), 3);
    let vs = view.execute("SELECT v FROM items WHERE id = 2").unwrap();
    match vs {
        ExecOutcome::Rows(r) => {
            assert_eq!(r.rows[0][0].as_i64().unwrap(), 20, "as-of image");
        }
        other => panic!("snapshot read failed: {other:?}"),
    }
    db.execute("COMMIT").unwrap();
    // After the transaction closes, fresh reads see the final state.
    assert_eq!(count(&db, "SELECT COUNT(*) FROM items"), 3);
}

#[test]
fn read_view_executes_explain_for_readonly_connections() {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY NOT NULL, n INT NOT NULL)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    let view = db.read_view();
    // EXPLAIN rides the read view: readonly roles may diagnose plans.
    let out = view
        .execute("EXPLAIN SELECT * FROM t WHERE id = 1")
        .unwrap();
    let ExecOutcome::Rows(r) = out else {
        panic!("expected plan rows");
    };
    assert_eq!(r.columns, vec!["plan".to_string(), "detail".to_string()]);
    let detail = r.rows[0][1].as_str().unwrap_or_default().to_string();
    assert!(detail.contains("INDEX PROBE ON t USING id"), "{r:?}");
    // DDL/DML still refuse on the read view.
    assert!(view.execute("INSERT INTO t VALUES (2, 20)").is_err());
}

#[test]
fn read_view_is_repeatable_read_while_writes_commit() {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE t (id INT)").unwrap();
    for i in 1..=3 {
        db.execute(&format!("INSERT INTO t VALUES ({i})")).unwrap();
    }

    let view = db.read_view();
    // Writes keep committing while the view is open — at the engine
    // level nothing blocks them (the view holds no database lock).
    for i in 4..=6 {
        db.execute(&format!("INSERT INTO t VALUES ({i})")).unwrap();
    }

    // The view stays at its snapshot: repeatable read, no dirty data.
    assert_eq!(view_count(&view, "SELECT COUNT(id) FROM t"), 3);
    assert_eq!(view_count(&view, "SELECT COUNT(id) FROM t"), 3);
    // Fresh views see the committed writes.
    let view2 = db.read_view();
    assert_eq!(view_count(&view2, "SELECT COUNT(id) FROM t"), 6);
    // Ending the snapshot keeps the pager healthy.
    drop(view);
    assert_eq!(count(&db, "SELECT COUNT(id) FROM t"), 6);
}

#[test]
fn read_view_survives_index_updates_and_rewrites_underneath() {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    for i in 1..=50 {
        db.execute(&format!("INSERT INTO t VALUES ({i}, 'v{i}')"))
            .unwrap();
    }
    let view = db.read_view();
    // Post-snapshot: more inserts (root splits move the index root),
    // an update and a delete — none of it may leak into the view.
    for i in 51..=120 {
        db.execute(&format!("INSERT INTO t VALUES ({i}, 'v{i}')"))
            .unwrap();
    }
    db.execute("UPDATE t SET v = 'changed' WHERE id = 7")
        .unwrap();
    db.execute("DELETE FROM t WHERE id = 9").unwrap();

    assert_eq!(view_count(&view, "SELECT COUNT(id) FROM t"), 50);
    match view.execute("SELECT v FROM t WHERE id = 7").unwrap() {
        ExecOutcome::Rows(r) => assert_eq!(r.rows[0][0], Value::Str("v7".into())),
        _ => panic!("expected rows"),
    }
    // PK probe over the as-of tree still finds row 9 (deleted only in
    // the live tree) and misses 100 (inserted after the snapshot).
    match view.execute("SELECT id FROM t WHERE id = 9").unwrap() {
        ExecOutcome::Rows(r) => assert_eq!(r.rows.len(), 1),
        _ => panic!("expected rows"),
    }
    match view.execute("SELECT id FROM t WHERE id = 100").unwrap() {
        ExecOutcome::Rows(r) => assert_eq!(r.rows.len(), 0),
        _ => panic!("expected rows"),
    }
    assert_eq!(count(&db, "SELECT COUNT(id) FROM t"), 119);
}
