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

/// Deterministic table: 60 rows, duplicate k values, three active
/// states. `with_index` builds the partial index `px ON (k)` (and a
/// composite partial `pc ON (k, id)` for the active=2 cohort); the twin
/// goes without — every query must return identical rows on both.
fn twin(with_index: bool) -> Database {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, v TEXT, active INT)",
    );
    let mut batch = Vec::new();
    for id in 1..=60i64 {
        batch.push(format!(
            "({}, {}, 'v{}', {})",
            id,
            (id * 7) % 23,
            id,
            id % 3
        ));
    }
    run(
        &mut db,
        &format!("INSERT INTO t VALUES {}", batch.join(",")),
    );
    if with_index {
        run(&mut db, "CREATE INDEX px ON t (k) WHERE active = 1");
        run(&mut db, "CREATE INDEX pc ON t (k, id) WHERE active = 2");
    }
    db
}

/// The query battery both twins must answer identically: implied and
/// non-implied shapes, point/range/IN/LIKE probes, ordered windows with
/// and without a WHERE, aggregates.
fn battery(db: &mut Database) -> Vec<QueryResult> {
    [
        // Implied probes over the partial tree.
        "SELECT id FROM t WHERE active = 1 AND k = 5 ORDER BY id",
        "SELECT id FROM t WHERE active = 1 AND k > 10 AND k < 20 ORDER BY id",
        "SELECT id FROM t WHERE active = 1 AND k IN (3, 7, 11) ORDER BY id",
        "SELECT id FROM t WHERE active = 1 AND v LIKE 'v1%' ORDER BY id",
        "SELECT COUNT(*) FROM t WHERE active = 1 AND k = 7",
        // Implied ordered window (NOT NULL k, single direction).
        "SELECT id, k FROM t WHERE active = 1 ORDER BY k DESC, id LIMIT 5",
        // NOT implied: the partial indexes must stay invisible.
        "SELECT id FROM t WHERE k = 5 ORDER BY id",
        "SELECT id FROM t WHERE active = 2 AND k = 5 ORDER BY id",
        "SELECT id FROM t WHERE k > 15 ORDER BY id",
        // No WHERE: a partial tree must never serve an ORDER BY walk.
        "SELECT id, k FROM t ORDER BY k, id LIMIT 5",
        // Composite partial (active = 2 cohort), full-key equality.
        "SELECT id FROM t WHERE active = 2 AND k = 3 AND id = 9 ORDER BY id",
        "SELECT id FROM t WHERE active = 2 AND k = 3 ORDER BY id",
    ]
    .iter()
    .map(|q| rows(db, q))
    .collect()
}

#[test]
fn partial_index_twin_table_differential() {
    let mut indexed = twin(true);
    let mut plain = twin(false);
    let a = battery(&mut indexed);
    let b = battery(&mut plain);
    for (i, (x, y)) in a.iter().zip(b.iter()).enumerate() {
        assert_eq!(x.rows, y.rows, "query {i} diverges between twins");
    }
    // Write-path membership flips: the same statements on both twins
    // (one maintains the partial trees, one doesn't) must converge again.
    for sql in [
        "INSERT INTO t VALUES (61, 5, 'v61', 1)",
        "INSERT INTO t VALUES (62, 5, 'v62', 0)",
        "UPDATE t SET active = 0 WHERE id = 3",
        "UPDATE t SET active = 1 WHERE id = 12",
        "DELETE FROM t WHERE id = 24",
        "UPDATE t SET k = 21 WHERE id = 15",
    ] {
        run(&mut indexed, sql);
        run(&mut plain, sql);
    }
    let a = battery(&mut indexed);
    let b = battery(&mut plain);
    for (i, (x, y)) in a.iter().zip(b.iter()).enumerate() {
        assert_eq!(x.rows, y.rows, "post-write query {i} diverges");
    }
    // Full-table rewrites (ALTER backfill) rebuild every tree: the
    // partial trees must come back with exactly their qualifying rows.
    run(
        &mut indexed,
        "ALTER TABLE t ADD COLUMN pad TEXT DEFAULT ('p')",
    );
    run(
        &mut plain,
        "ALTER TABLE t ADD COLUMN pad TEXT DEFAULT ('p')",
    );
    let a = battery(&mut indexed);
    let b = battery(&mut plain);
    for (i, (x, y)) in a.iter().zip(b.iter()).enumerate() {
        assert_eq!(x.rows, y.rows, "post-rewrite query {i} diverges");
    }
}

#[test]
fn partial_index_probe_gate_explain_and_persistence() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("px.db");
    {
        let mut db = Database::open(&path).unwrap();
        run(
            &mut db,
            "CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, active INT)",
        );
        run(
            &mut db,
            "INSERT INTO t VALUES (1, 5, 1), (2, 6, 1), (3, 5, 0)",
        );
        run(&mut db, "CREATE INDEX px ON t (k) WHERE active = 1");
        // Implied: the plan probes the PARTIAL tree by name.
        let r = rows(
            &mut db,
            "EXPLAIN SELECT id FROM t WHERE active = 1 AND k = 5",
        );
        let plan = match &r.rows[0][1] {
            Value::Str(s) => s.clone(),
            other => panic!("{other:?}"),
        };
        assert!(plan.contains("px"), "plan should probe px: {plan}");
        // Not implied: full scan, the partial tree stays invisible.
        let r = rows(&mut db, "EXPLAIN SELECT id FROM t WHERE k = 5");
        let plan = match &r.rows[0][1] {
            Value::Str(s) => s.clone(),
            other => panic!("{other:?}"),
        };
        assert!(!plan.contains("px"), "plan must not probe px: {plan}");
        run(&mut db, "INSERT INTO t VALUES (4, 9, 1)");
    }
    // The predicate persists in the catalog and keeps gating after a
    // reopen (an ungated tree would leak the active=0 row 3 into every
    // k=5 probe).
    let mut db = Database::open(&path).unwrap();
    let r = rows(
        &mut db,
        "SELECT id FROM t WHERE active = 1 AND k = 5 ORDER BY id",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(1)]], "{r:?}");
    let r = rows(&mut db, "SELECT id FROM t WHERE k = 5 ORDER BY id");
    assert_eq!(
        r.rows,
        vec![vec![Value::Int(1)], vec![Value::Int(3)]],
        "{r:?}"
    );
}

#[test]
fn partial_index_equality_substitution_implies() {
    // The substitution rule: a query `k = 5` implies the predicate
    // `k > 0` (substituting 5 evaluates TRUE), so the probe may use the
    // partial tree on `tag` — while `k = -3` does not imply it and must
    // scan. (The probe column has no full index, or the full tree would
    // shadow the partial one — which is the better plan anyway.)
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, tag TEXT)",
    );
    run(
        &mut db,
        "INSERT INTO t VALUES (1, 5, 'a'), (2, -3, 'a'), (3, 7, 'b')",
    );
    run(&mut db, "CREATE INDEX pk ON t (tag) WHERE k > 0");
    // Implied via substitution: the plan probes pk.
    let r = rows(
        &mut db,
        "EXPLAIN SELECT id FROM t WHERE k = 5 AND tag = 'a'",
    );
    assert!(
        matches!(&r.rows[0][1], Value::Str(s) if s.contains("pk")),
        "{r:?}"
    );
    let r = rows(&mut db, "SELECT id FROM t WHERE k = 5 AND tag = 'a'");
    assert_eq!(r.rows, vec![vec![Value::Int(1)]], "{r:?}");
    // The twin without the index answers identically.
    let mut plain = Database::in_memory().unwrap();
    run(
        &mut plain,
        "CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, tag TEXT)",
    );
    run(
        &mut plain,
        "INSERT INTO t VALUES (1, 5, 'a'), (2, -3, 'a'), (3, 7, 'b')",
    );
    let r2 = rows(&mut plain, "SELECT id FROM t WHERE k = 5 AND tag = 'a'");
    assert_eq!(r.rows, r2.rows);
    // NOT implied (-3 > 0 fails): full scan.
    let r = rows(
        &mut db,
        "EXPLAIN SELECT id FROM t WHERE k = -3 AND tag = 'a'",
    );
    assert!(
        matches!(&r.rows[0][1], Value::Str(s) if !s.contains("pk")),
        "{r:?}"
    );
    let r = rows(&mut db, "SELECT id FROM t WHERE k = -3 AND tag = 'a'");
    assert_eq!(r.rows, vec![vec![Value::Int(2)]], "{r:?}");
    // IN lists: every element must imply the predicate.
    let r = rows(
        &mut db,
        "EXPLAIN SELECT id FROM t WHERE k IN (5, 7) AND tag = 'a'",
    );
    assert!(
        matches!(&r.rows[0][1], Value::Str(s) if s.contains("pk")),
        "{r:?}"
    );
    let r = rows(
        &mut db,
        "EXPLAIN SELECT id FROM t WHERE k IN (5, -3) AND tag = 'a'",
    );
    assert!(
        matches!(&r.rows[0][1], Value::Str(s) if !s.contains("pk")),
        "{r:?}"
    );
}

#[test]
fn partial_unique_enforces_only_among_qualifying_rows() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE users (id INT PRIMARY KEY, email TEXT, deleted INT NOT NULL)",
    );
    run(
        &mut db,
        "CREATE UNIQUE INDEX ux_live ON users (email) WHERE deleted = 0",
    );
    // The classic soft-delete pattern: live emails are unique, deleted
    // ones may repeat freely.
    run(&mut db, "INSERT INTO users VALUES (1, 'a@x', 0)");
    let e = db
        .execute("INSERT INTO users VALUES (2, 'a@x', 0)")
        .unwrap_err();
    assert!(e.to_string().to_uppercase().contains("UNIQUE"), "{e}");
    // Non-qualifying rows insert beside the tree — even with the same
    // email, even several of them.
    run(&mut db, "INSERT INTO users VALUES (3, 'a@x', 1)");
    run(&mut db, "INSERT INTO users VALUES (8, 'a@x', 1)");
    // Flipping IN collides; flipping OUT frees the key.
    let e = db
        .execute("UPDATE users SET deleted = 0 WHERE id = 3")
        .unwrap_err();
    assert!(e.to_string().to_uppercase().contains("UNIQUE"), "{e}");
    run(&mut db, "UPDATE users SET deleted = 1 WHERE id = 1");
    run(&mut db, "INSERT INTO users VALUES (4, 'a@x', 0)");
    // OR IGNORE: the qualifying duplicate is skipped via the partial
    // tree (untargeted policy), not surfaced as a tree error.
    run(&mut db, "INSERT OR IGNORE INTO users VALUES (5, 'a@x', 0)");
    let r = rows(
        &mut db,
        "SELECT COUNT(*) FROM users WHERE email = 'a@x' AND deleted = 0",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(1)]], "{r:?}");
    // OR REPLACE: the qualifying duplicate displaces the old row.
    run(&mut db, "INSERT OR REPLACE INTO users VALUES (6, 'a@x', 0)");
    let r = rows(
        &mut db,
        "SELECT id FROM users WHERE email = 'a@x' AND deleted = 0 ORDER BY id",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(6)]], "{r:?}");
    // Upsert (MySQL spelling, untargeted): conflict via the partial tree
    // updates the row in place.
    run(
        &mut db,
        "INSERT INTO users VALUES (7, 'a@x', 0) ON DUPLICATE KEY UPDATE deleted = 9",
    );
    let r = rows(&mut db, "SELECT deleted FROM users WHERE id = 6");
    assert_eq!(r.rows, vec![vec![Value::Int(9)]], "{r:?}");
    // Targeted ON CONFLICT on the partial index's column cannot resolve
    // (conflict scopes are constraint columns) — loud, documented.
    let e = db
        .execute("INSERT INTO users VALUES (9, 'a@x', 0) ON CONFLICT (email) DO NOTHING")
        .unwrap_err();
    assert!(e.to_string().contains("email"), "{e}");
    // Reads: the gated probe must answer exactly like a scan — after the
    // upsert flipped id=6 out of the predicate (deleted = 9) NO row
    // qualifies, and the deleted rows must not leak through the tree.
    let r = rows(
        &mut db,
        "SELECT id FROM users WHERE deleted = 0 AND email = 'a@x' ORDER BY id",
    );
    assert_eq!(r.rows, Vec::<Vec<Value>>::new(), "{r:?}");
    let r = rows(
        &mut db,
        "SELECT id FROM users WHERE email = 'a@x' ORDER BY id",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(3)],
            vec![Value::Int(6)],
            vec![Value::Int(8)]
        ],
        "{r:?}"
    );
}

#[test]
fn partial_index_dump_roundtrip() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, active INT)",
    );
    run(
        &mut db,
        "INSERT INTO t VALUES (1, 5, 1), (2, 6, 1), (3, 5, 0)",
    );
    run(&mut db, "CREATE INDEX px ON t (k) WHERE active = 1");
    let script = db.dump_script().unwrap();
    assert!(script.contains("WHERE active = 1"), "{script}");
    let mut dst = Database::in_memory().unwrap();
    run(&mut dst, "BEGIN");
    let batch = dst.execute_batch(&script);
    assert!(batch.error.is_none(), "{:?}", batch.error);
    run(&mut dst, "COMMIT");
    // The restored predicate gates again: implied probe sees only the
    // qualifying row.
    let r = rows(
        &mut dst,
        "SELECT id FROM t WHERE active = 1 AND k = 5 ORDER BY id",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(1)]], "{r:?}");
    let r = rows(&mut dst, "SELECT id FROM t WHERE k = 5 ORDER BY id");
    assert_eq!(r.rows.len(), 2, "{r:?}");
    // And the restored index shows its predicate in sqlite_master.
    let r = rows(&mut dst, "SELECT sql FROM sqlite_master WHERE name = 'px'");
    assert!(
        matches!(&r.rows[0][0], Value::Str(s) if s.contains("WHERE active = 1")),
        "{r:?}"
    );
}

#[test]
fn partial_index_validation_and_alter_guards() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY, k INT, tag TEXT)",
    );
    run(&mut db, "INSERT INTO t VALUES (1, 5, 'a')");
    // Subquery predicate.
    let e = db
        .execute("CREATE INDEX px ON t (k) WHERE id IN (SELECT id FROM t)")
        .unwrap_err();
    assert!(e.to_string().contains("row-local"), "{e}");
    // Aggregate predicate.
    let e = db
        .execute("CREATE INDEX px ON t (k) WHERE COUNT(id) > 0")
        .unwrap_err();
    assert!(e.to_string().contains("row-local"), "{e}");
    // Qualified column.
    let e = db
        .execute("CREATE INDEX px ON t (k) WHERE t.k > 0")
        .unwrap_err();
    assert!(e.to_string().contains("bare column names"), "{e}");
    // Unknown column.
    let e = db
        .execute("CREATE INDEX px ON t (k) WHERE ghost > 0")
        .unwrap_err();
    assert!(e.to_string().contains("ghost"), "{e}");
    // Path + partial refused.
    let e = db
        .execute("CREATE INDEX pj ON t (JSON_EXTRACT(tag, '$.a')) WHERE k > 0")
        .unwrap_err();
    assert!(e.to_string().contains("cannot combine"), "{e}");
    // RENAME COLUMN rewrites the predicate text.
    run(&mut db, "CREATE INDEX px ON t (k) WHERE tag = 'a'");
    run(&mut db, "ALTER TABLE t RENAME COLUMN tag TO label");
    run(&mut db, "INSERT INTO t VALUES (2, 6, 'a')");
    let r = rows(
        &mut db,
        "SELECT id FROM t WHERE label = 'a' AND k = 6 ORDER BY id",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(2)]], "{r:?}");
    // DROP COLUMN named by a predicate is refused (drop the index first).
    let e = db.execute("ALTER TABLE t DROP COLUMN label").unwrap_err();
    assert!(
        e.to_string().contains("partial index px references it"),
        "{e}"
    );
    // Dropping the index unblocks the drop.
    run(&mut db, "DROP INDEX px");
    run(&mut db, "ALTER TABLE t DROP COLUMN label");
}
