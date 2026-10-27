
use super::*;

#[test]
fn parse_classified_cache_hits_and_bypasses() {
    let a = Database::parse_classified("SELECT 1, 'x' FROM dual").unwrap();
    let b = Database::parse_classified("SELECT 1, 'x' FROM dual").unwrap();
    assert_eq!(a.is_write, b.is_write);
    assert_eq!(a.tx, b.tx);
    assert_eq!(format!("{:?}", a.stmt), format!("{:?}", b.stmt));
    // Different text must not collide.
    let c = Database::parse_classified("SELECT 2, 'x' FROM dual").unwrap();
    assert_ne!(format!("{:?}", a.stmt), format!("{:?}", c.stmt));
    // Oversized statements bypass the cache and still parse.
    let big = format!("SELECT '{}'", "x".repeat(6000));
    assert!(big.len() > 4096);
    assert!(Database::parse_classified(&big).is_ok());
    // Parse errors stay per-call (never cached).
    assert!(Database::parse_classified("SELECT !").is_err());
}

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

#[test]
fn table_rewrite_persists_the_drained_overflow_free_list() {
    // A rebuild seeds its new heap from the recycled-chain free list and
    // oversized rows pop pages from it. The catalog kept the PRE-rebuild
    // list, so the next oversized insert handed out pages the rebuild had
    // just made live and overwrote a live document's chain.
    let dir = tempfile::tempdir().unwrap();
    let mut db = Database::open(&dir.path().join("ovfree.db")).unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, b TEXT)");
    let long = "x".repeat(20_000);
    run(&mut db, &format!("INSERT INTO t VALUES (1, '{long}')"));
    run(&mut db, "INSERT INTO t VALUES (2, 'keep')");
    // Shrink row 1: its chain pages are parked in the free list.
    run(&mut db, "UPDATE t SET b = 'small' WHERE id = 1");
    assert!(
        !db.tables.get("t").unwrap().overflow_free.is_empty(),
        "chain pages must be parked"
    );
    // A rebuild that re-inserts oversized rows drains that list.
    run(
        &mut db,
        &format!("ALTER TABLE t ADD COLUMN pad TEXT DEFAULT ('{long}')"),
    );
    assert!(
        db.tables.get("t").unwrap().overflow_free.is_empty(),
        "the rebuild must not leave the pages it handed out in the free list"
    );
    // The next oversized insert must not reuse them: every row's chain
    // still reads back whole.
    run(&mut db, &format!("INSERT INTO t VALUES (3, 'y', '{long}')"));
    for id in 1..=3 {
        assert_eq!(
            rows(&mut db, &format!("SELECT pad FROM t WHERE id = {id}")).rows[0],
            vec![Value::Str(long.clone())],
            "row {id} chain corrupted"
        );
    }
}

#[test]
fn rollback_only_workload_keeps_wal_bounded() {
    // ROLLBACK is a terminal statement: without the fence+sync in its
    // tail, nothing ever triggered the WAL cap and a BEGIN/INSERT/
    // ROLLBACK loop grew the log without bound.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rb.db");
    {
        let mut db = Database::open(&path).unwrap();
        run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, b TEXT)");
        for i in 0..600 {
            run(&mut db, "BEGIN");
            run(
                &mut db,
                &format!("INSERT INTO t VALUES ({i}, '{}')", "x".repeat(3000)),
            );
            run(&mut db, "ROLLBACK");
        }
        let r = rows(&mut db, "SELECT COUNT(*) FROM t");
        assert_eq!(r.rows[0][0], Value::Int(0));
    }
    // Deterministic observable, part 1: a workload far below the 8MB
    // soft checkpoint threshold cannot have its fences reclaimed by a
    // truncation, so EVERY ROLLBACK's closing fence must still be in
    // the log. Before the fix a rollback-only workload never fenced.
    let wal_path = crate::pager::wal_path_for(&path);
    let count_fences = |p: &std::path::Path| -> usize {
        crate::wal::Wal::scan_file(p)
            .unwrap()
            .into_iter()
            .filter(|r| r.kind == crate::wal::KIND_FENCE)
            .count()
    };
    let fences = count_fences(&wal_path);
    let wal_len = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
    assert!(
        fences >= 600 || wal_len < 8 * 1024 * 1024,
        "expected one fence per ROLLBACK (got {fences}) or a reclaimed log, \
             WAL is {wal_len} bytes"
    );
    // Part 2: the 3KB-row loop above crosses the soft threshold, so a
    // background truncation may legitimately reclaim dead frames
    // mid-run — fences live in the log they fence, so a COMPLETED
    // truncation drops earlier fences with them. Cross-checked below
    // via a separate small-workload run (threshold unreachable) whose
    // fence count is exact, keeping this assertion flake-free while
    // still exercising the cap machinery under rollback-only load.
    {
        let path2 = dir.path().join("rb2.db");
        {
            let mut db = Database::open(&path2).unwrap();
            run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, b TEXT)");
            for i in 0..40 {
                run(&mut db, "BEGIN");
                run(&mut db, &format!("INSERT INTO t VALUES ({i}, 'x')"));
                run(&mut db, "ROLLBACK");
            }
        }
        let fences2 = count_fences(&crate::pager::wal_path_for(&path2));
        assert!(
            fences2 >= 40,
            "small rollback-only workload must keep one fence per \
                 ROLLBACK, got {fences2}"
        );
    }
}

#[test]
fn oversized_documents_survive_transactions() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Database::open(&dir.path().join("big.db")).unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, b TEXT)");
    let big1 = "a".repeat(20_000);
    let big2 = "b".repeat(20_000);
    let big3 = "c".repeat(20_000);
    run(&mut db, &format!("INSERT INTO t VALUES (1, '{big1}')"));
    run(&mut db, "INSERT INTO t VALUES (2, 'small2')");
    run(&mut db, "INSERT INTO t VALUES (3, 'small3')");

    // (1) one explicit transaction: grow a row, delete an oversized row
    // (recycling its chain), insert another oversized row (spending it).
    run(&mut db, "BEGIN");
    run(&mut db, &format!("UPDATE t SET b = '{big2}' WHERE id = 2"));
    run(&mut db, "DELETE FROM t WHERE id = 1");
    run(&mut db, &format!("INSERT INTO t VALUES (4, '{big3}')"));
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM t").rows[0][0],
        Value::Int(3)
    );
    assert_eq!(
        rows(&mut db, "SELECT b FROM t WHERE id = 2").rows[0][0],
        Value::Str(big2.clone())
    );
    run(&mut db, "ROLLBACK");
    // Rolled back: row 1 whole again, row 2 small, no row 4.
    assert_eq!(
        rows(&mut db, "SELECT b FROM t WHERE id = 1").rows[0][0],
        Value::Str(big1.clone()),
        "row 1 chain after ROLLBACK"
    );
    assert_eq!(
        rows(&mut db, "SELECT b FROM t WHERE id = 2").rows[0][0],
        Value::Str("small2".into())
    );
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM t WHERE id = 4").rows[0][0],
        Value::Int(0)
    );
    // (2) savepoint: recycle row 1's chain, spend it, roll back to the
    // savepoint — the pages must return to row 1, not to the free list.
    run(&mut db, "BEGIN");
    run(&mut db, "UPDATE t SET b = 'grown' WHERE id = 2");
    run(&mut db, "SAVEPOINT s1");
    run(&mut db, "DELETE FROM t WHERE id = 1");
    run(&mut db, &format!("INSERT INTO t VALUES (5, '{big2}')"));
    run(&mut db, "ROLLBACK TO s1");
    run(&mut db, "COMMIT");
    assert_eq!(
        rows(&mut db, "SELECT b FROM t WHERE id = 1").rows[0][0],
        Value::Str(big1.clone()),
        "row 1 chain after ROLLBACK TO"
    );
    assert_eq!(
        rows(&mut db, "SELECT b FROM t WHERE id = 2").rows[0][0],
        Value::Str("grown".into())
    );
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM t WHERE id = 5").rows[0][0],
        Value::Int(0)
    );
    // (3) after the commit, a fresh oversized insert must not reuse the
    // pages row 1 owns.
    run(&mut db, &format!("INSERT INTO t VALUES (6, '{big3}')"));
    assert_eq!(
        rows(&mut db, "SELECT b FROM t WHERE id = 1").rows[0][0],
        Value::Str(big1.clone()),
        "row 1 chain after a later big INSERT"
    );
    assert_eq!(
        rows(&mut db, "SELECT b FROM t WHERE id = 6").rows[0][0],
        Value::Str(big3.clone())
    );
    // (4) survives reopen
    drop(db);
    let mut db = Database::open(&dir.path().join("big.db")).unwrap();
    for (id, want) in [(1, big1.as_str()), (2, "grown"), (6, big3.as_str())] {
        assert_eq!(
            rows(&mut db, &format!("SELECT b FROM t WHERE id = {id}")).rows[0][0],
            Value::Str((*want).into()),
            "row {id} after reopen"
        );
    }
}

#[test]
fn corrupt_data_file_never_panics() {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    let base = "z".repeat(9_000);
    let big = base.clone();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fuzz.db");
    {
        let mut db = Database::open(&path).unwrap();
        run(
            &mut db,
            "CREATE TABLE t (id INT PRIMARY KEY, b TEXT, blob TEXT, n INT)",
        );
        run(&mut db, "CREATE INDEX ix ON t (b, n)");
        // Two more tables so the catalog chain spans several pages (its
        // `next` pointer and per-page header are corruption targets), and
        // a second oversized row so an overflow chain spans 3+ pages.
        run(&mut db, "CREATE TABLE u (k TEXT PRIMARY KEY, v INT)");
        run(&mut db, "CREATE TABLE v (k TEXT, v INT)");
        for i in 0..40 {
            run(&mut db, &format!("INSERT INTO u VALUES ('k{i:04}', {i})"));
            run(&mut db, &format!("INSERT INTO v VALUES ('w{i:04}', {i})"));
        }
        for i in 0..12 {
            run(
                &mut db,
                &format!(
                    "INSERT INTO t VALUES ({i}, 'row-{i}-{}', 'small', {i})",
                    "q".repeat(60 * i)
                ),
            );
        }
        run(
            &mut db,
            &format!("INSERT INTO t VALUES (99, 'row-99', '{big}', 99)"),
        );
    }
    let npages = std::fs::metadata(&path).unwrap().len() as usize / 4096;
    let original = std::fs::read(&path).unwrap();
    let battery = [
        "SELECT COUNT(*) FROM t",
        "SELECT * FROM t",
        "SELECT * FROM t WHERE b = 'row-3-ccc'",
        "SELECT * FROM t ORDER BY b, n LIMIT 3",
        "SELECT * FROM t ORDER BY b, n DESC LIMIT 3",
        "UPDATE t SET b = 'x' WHERE id = 3",
        "UPDATE t SET blob = 'y' WHERE id = 99",
        "DELETE FROM t WHERE id = 4",
        "INSERT INTO t VALUES (500, 'new', 'small', 1)",
        "TRUNCATE TABLE t",
        "ALTER TABLE t ADD COLUMN zzz TEXT DEFAULT 'd'",
        "DROP INDEX ix",
        "SELECT * FROM u WHERE k = 'k0007'",
    ];
    // Byte-level damage must surface as an error, never as a panic: one
    // corrupt page anywhere in the file is enough to take a node down if
    // any read or write path slices or subtracts without checking.
    let mut panics: Vec<String> = Vec::new();
    let run = |label: String, stmts: &[&str], panics: &mut Vec<String>| {
        for sql in stmts {
            let r = catch_unwind(AssertUnwindSafe(|| {
                // A loud open error is a fine outcome for a damaged file;
                // only a panic is a defect.
                if let Ok(mut db) = Database::open(&path) {
                    let _ = db.execute(sql);
                }
            }));
            if let Err(e) = r {
                let msg = e
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string()))
                    .unwrap_or_default();
                let site = msg.split('\n').next().unwrap_or("").to_string();
                panics.push(format!("{label}: {sql} :: {site}"));
            }
        }
    };
    // Phase 1: one flipped byte per page (page 0 = the file header too).
    for page in 0..npages {
        for off in [0usize, 2, 8, 12, 16, 40, 2048, 4090] {
            for delta in [0x01u8, 0xff] {
                let mut bytes = original.clone();
                bytes[page * 4096 + off] ^= delta;
                std::fs::write(&path, &bytes).unwrap();
                run(
                    format!("page {page} off {off} delta {delta:#x}"),
                    &battery,
                    &mut panics,
                );
            }
        }
    }
    // Phase 2: bursts of damage (deterministic LCG, so a failure
    // reproduces exactly).
    let mut seed = 0x2545_F491_4F6C_DD1Du64;
    let mut next = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    // Bursts get the read/write half of the battery: 400+ damage shapes
    // at full breadth would dominate the suite's runtime.
    let burst_battery = &battery[..6];
    for round in 0..100 {
        let mut bytes = original.clone();
        for _ in 0..6 {
            let at = (next() as usize) % bytes.len();
            bytes[at] = (next() & 0xff) as u8;
        }
        std::fs::write(&path, &bytes).unwrap();
        run(format!("burst {round}"), burst_battery, &mut panics);
    }
    // Phase 3: header page-count damage (claims more/fewer pages than the
    // file holds) and a physically truncated file.
    for count in [0u32, 1, 2, npages as u32 - 1, npages as u32 + 1, u32::MAX] {
        let mut bytes = original.clone();
        bytes[12..16].copy_from_slice(&count.to_le_bytes());
        std::fs::write(&path, &bytes).unwrap();
        run(format!("num_pages {count}"), &battery, &mut panics);
    }
    for cut in [1usize, 2, 3, npages - 1, npages / 2] {
        let mut bytes = original.clone();
        bytes.truncate(cut * 4096);
        std::fs::write(&path, &bytes).unwrap();
        run(format!("truncated to {cut} pages"), &battery, &mut panics);
    }
    std::fs::write(&path, &original).unwrap();
    assert!(
        panics.is_empty(),
        "{} panics, first 10: {:#?}",
        panics.len(),
        &panics[..panics.len().min(10)]
    );
}

#[test]
fn create_index_root_key_collisions_are_loud() {
    // `index_roots` is a mixed pool (column-named trees + index-name
    // composite trees); a differently-shaped collision must refuse —
    // the old `append` silently replaced the tree and broke PK/UNIQUE
    // enforcement. Same-column indexes share the tree instead.
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (e INT PRIMARY KEY, c INT, d INT, v INT)",
    );
    run(
        &mut db,
        "INSERT INTO t VALUES (1, 10, 100, 5), (2, 20, 200, 6)",
    );
    // Composite index named exactly like the PK column.
    let e = db.execute("CREATE INDEX e ON t (c, d)").unwrap_err();
    assert!(e.to_string().contains("root key"), "{e}");
    // The PK still enforces after the refused collision.
    assert!(db.execute("INSERT INTO t VALUES (1, 1, 1, 1)").is_err());
    // Same-column second index reuses the tree: both names work.
    run(&mut db, "CREATE INDEX iv ON t (v)");
    run(&mut db, "CREATE INDEX iv2 ON t (v)");
    let r = rows(&mut db, "SELECT e FROM t WHERE v = 6");
    assert_eq!(r.rows.len(), 1);
    // And the catalog round-trips through a dump replay.
    let script = db.dump_script().unwrap();
    let mut db2 = Database::in_memory().unwrap();
    for stmt in script.split(';').filter(|s| !s.trim().is_empty()) {
        db2.execute(stmt).unwrap();
    }
    let r = db2.execute("SELECT e FROM t WHERE v = 6").unwrap();
    assert!(matches!(r, ExecOutcome::Rows(_)));
}

#[test]
fn drop_column_reclaims_index_tree_pages() {
    // A composite index over a dropped column dies with it; its tree
    // pages must return to the reusable pool. Discriminator: two
    // identical databases, one carries the doomed composite index.
    // After DROP COLUMN both reinsert enough rows to exhaust the
    // non-index pool share — with the reclaim, the indexed variant
    // covers part of the demand from the freed tree pages and the file
    // ends smaller; with the pre-fix leak both ended identical (the
    // dead tree's pages were lost for good).
    let pad = "b".repeat(200);
    let build = |with_index: bool| -> Database {
        let mut db = Database::in_memory().unwrap();
        run(
            &mut db,
            "CREATE TABLE t (id INT PRIMARY KEY, b TEXT, c INT)",
        );
        if with_index {
            run(&mut db, "CREATE INDEX ibc ON t (b, c)");
        }
        for i in 0..300 {
            run(
                &mut db,
                &format!("INSERT INTO t VALUES ({i}, '{pad}', {i})"),
            );
        }
        db
    };
    // Sized past BOTH pool shares: each variant then grows by
    // (demand - its pool), so the growth difference equals the dead
    // tree's page share — the reclaim, made observable.
    let reinsert = |db: &mut Database| {
        for chunk in 0..12 {
            let mut v = String::from("INSERT INTO t VALUES ");
            for i in 300 + chunk * 500..300 + (chunk + 1) * 500 {
                if i > 300 + chunk * 500 {
                    v.push(',');
                }
                v.push_str(&format!("({i}, {i})"));
            }
            run(db, &v);
        }
    };
    let mut indexed = build(true);
    run(&mut indexed, "ALTER TABLE t DROP COLUMN b");
    let indexed_at_drop = indexed.num_pages();
    reinsert(&mut indexed);
    // The freed tree pages (plus the rewrite's other frees) must cover
    // the whole reinsert: `num_pages` is a high-water mark, so zero
    // growth is the observable form of the reclaim. Pre-fix, the dead
    // tree's pages never entered the pool and this grew.
    let indexed_growth = indexed.num_pages() - indexed_at_drop;
    // Control: the identical demand against the pool WITHOUT the tree
    // share genuinely grows the file — the demand sits above H+T_id
    // and below H+T_id+T_ibc, which is what makes the zero above
    // meaningful.
    let mut plain = build(false);
    run(&mut plain, "ALTER TABLE t DROP COLUMN b");
    let plain_at_drop = plain.num_pages();
    reinsert(&mut plain);
    let plain_growth = plain.num_pages() - plain_at_drop;
    // Pre-fix both grew identically (the dead tree never entered the
    // pool); with the reclaim the indexed variant grows by strictly
    // less — by roughly the tree's own page share.
    assert!(
        indexed_growth + 8 <= plain_growth,
        "DROP COLUMN must reclaim the dead index tree into the pool \
             (indexed growth {indexed_growth} vs plain growth {plain_growth})"
    );
    assert!(plain_growth > 0);
    // And the surviving single-column lookups still work.
    let q = "SELECT COUNT(*) FROM t WHERE c < 100";
    assert_eq!(rows(&mut indexed, q).rows[0][0].as_i64().unwrap(), 100);
}

#[test]
fn timestamp_literals_outside_domain_are_rejected() {
    // A UTC offset can push an in-range calendar date outside the
    // 0001..=9999 instant domain; such values have no round-trippable
    // text and must be refused at every string constructor (one such
    // value in a table used to make the whole dump fail).
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT, ts TIMESTAMP)");
    for bad in ["9999-12-31T23:59:59.999-23:59", "0001-01-01T00:00:00+23:59"] {
        let e = db
            .execute(&format!("INSERT INTO t VALUES (1, TIMESTAMP '{bad}')"))
            .unwrap_err();
        assert!(e.to_string().to_lowercase().contains("timestamp"), "{e}");
        let e = db
            .execute(&format!(
                "INSERT INTO t VALUES (2, CAST('{bad}' AS TIMESTAMP))"
            ))
            .unwrap_err();
        assert!(e.to_string().to_lowercase().contains("timestamp"), "{e}");
    }
    // Domain boundaries themselves parse and dump cleanly.
    run(
        &mut db,
        "INSERT INTO t VALUES (3, TIMESTAMP '0001-01-01T00:00:00.000Z')",
    );
    run(
        &mut db,
        "INSERT INTO t VALUES (4, TIMESTAMP '9999-12-31T23:59:59.999Z')",
    );
    let script = db.dump_script().unwrap();
    assert!(script.contains("0001-01-01"), "{script}");
}

#[test]
fn float_literal_inlining_keeps_float_type() {
    // Scalar-subquery inlining used to render Float(3.0) via Display
    // ("3"), re-parsing as Int and silently changing the type.
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (x INT)");
    run(&mut db, "INSERT INTO t VALUES (1)");
    let r = rows(&mut db, "SELECT TYPEOF((SELECT 3.0))");
    assert!(
        matches!(&r.rows[0][0], Value::Str(s) if s == "float"),
        "{r:?}"
    );
    // Distinctness follows encoding: 3.0 (float) and 3 (int) stay two
    // values instead of collapsing.
    let r = rows(
        &mut db,
        "SELECT COUNT(*) FROM (SELECT DISTINCT v FROM (SELECT 3.0 AS v UNION ALL SELECT 3))",
    );
    assert_eq!(r.rows[0][0].as_i64().unwrap(), 2);
}

#[test]
fn views_over_user_tables_still_create_but_read_admin_only() {
    // Creation is deliberately allowed: dumps and join snapshots from
    // older volumes may carry such a view, and refusing would break
    // the whole replay. The exposure is closed on the READ side — the
    // server's authorize expands view base tables and refuses
    // user-table bases to non-admins (covered by the server's
    // authorize tests).
    let mut db = Database::in_memory().unwrap();
    // The user tables are created lazily by the first user-admin
    // statement; the view over them needs them to exist.
    run(&mut db, "CREATE USER u1 PASSWORD 'a-password-1'");
    run(
        &mut db,
        "CREATE VIEW legacy AS SELECT name, pw FROM docsql_users",
    );
    // Views over ordinary tables keep working.
    run(&mut db, "CREATE TABLE t (a INT)");
    run(&mut db, "CREATE VIEW v AS SELECT a FROM t");
}

#[test]
fn merge_insert_without_column_list_maps_declared_columns() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE tgt (id INT PRIMARY KEY, v INT)");
    run(&mut db, "CREATE TABLE src (id INT, v INT)");
    run(&mut db, "INSERT INTO src VALUES (5, 50)");
    run(
        &mut db,
        "MERGE INTO tgt USING src ON tgt.id = src.id \
             WHEN NOT MATCHED THEN INSERT VALUES (src.id, src.v)",
    );
    let r = rows(&mut db, "SELECT id, v FROM tgt");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0].as_i64().unwrap(), 5);
    assert_eq!(r.rows[0][1].as_i64().unwrap(), 50);
    // Mismatched width is a loud error, never a silent NULL row.
    let e = db
        .execute(
            "MERGE INTO tgt USING src ON tgt.id = src.id \
                 WHEN NOT MATCHED THEN INSERT VALUES (src.id)",
        )
        .unwrap_err();
    assert!(e.to_string().contains("column list"), "{e}");
}

// ---- index-backed engine paths ----

fn idx_db() -> Database {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, n INT)")
        .unwrap();
    db
}

fn insert_n(db: &mut Database, n: i64) {
    for i in 0..n {
        db.execute(&format!(
            "INSERT INTO t (id, name, n) VALUES ({i}, 'u{i}', {i})"
        ))
        .unwrap();
    }
}

#[test]
fn index_enforces_pk_across_statements() {
    let mut db = idx_db();
    insert_n(&mut db, 10);
    let e = db
        .execute("INSERT INTO t (id, name, n) VALUES (5, 'dup', 0)")
        .unwrap_err();
    assert!(e.to_string().contains("UNIQUE"), "{e}");
    // The failed statement left nothing behind.
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM t").rows[0][0],
        Value::Int(10)
    );
}

#[test]
fn index_enforces_pk_within_batch() {
    let mut db = idx_db();
    let e = db
        .execute("INSERT INTO t (id, name, n) VALUES (1, 'a', 0), (1, 'b', 0)")
        .unwrap_err();
    assert!(e.to_string().contains("UNIQUE"), "{e}");
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM t").rows[0][0],
        Value::Int(0)
    );
}

#[test]
fn point_lookup_finds_row_and_missing() {
    let mut db = idx_db();
    insert_n(&mut db, 50);
    let r = rows(&mut db, "SELECT name, n FROM t WHERE id = 42");
    assert_eq!(r.rows, vec![vec![Value::Str("u42".into()), Value::Int(42)]]);
    assert_eq!(
        rows(&mut db, "SELECT id FROM t WHERE id = 999").rows.len(),
        0
    );
    // WHERE with extra conjuncts (residual filtering) still works.
    let r = rows(&mut db, "SELECT id FROM t WHERE id = 42 AND n > 100");
    assert_eq!(r.rows.len(), 0);
    let r = rows(&mut db, "SELECT id FROM t WHERE id = 42 AND n = 42");
    assert_eq!(r.rows.len(), 1);
}

#[test]
fn range_lookup_over_index() {
    let mut db = idx_db();
    insert_n(&mut db, 100);
    let r = rows(&mut db, "SELECT id FROM t WHERE id >= 97 ORDER BY id");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(97)],
            vec![Value::Int(98)],
            vec![Value::Int(99)]
        ]
    );
    // strict bounds on both sides
    let r = rows(
        &mut db,
        "SELECT id FROM t WHERE id > 5 AND id < 8 ORDER BY id",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(6)], vec![Value::Int(7)]]);
    // mirrored literal-first form
    let r = rows(&mut db, "SELECT id FROM t WHERE 8 = id");
    assert_eq!(r.rows, vec![vec![Value::Int(8)]]);
}

#[test]
fn fast_delete_removes_rows_and_index_entries() {
    let mut db = idx_db();
    insert_n(&mut db, 30);
    assert_eq!(
        db.execute("DELETE FROM t WHERE id = 7").unwrap(),
        ExecOutcome::Affected(1)
    );
    assert_eq!(rows(&mut db, "SELECT id FROM t WHERE id = 7").rows.len(), 0);
    // Slot is free: re-inserting the same key must be accepted.
    db.execute("INSERT INTO t (id, name, n) VALUES (7, 'back', 7)")
        .unwrap();
    let r = rows(&mut db, "SELECT name FROM t WHERE id = 7");
    assert_eq!(r.rows, vec![vec![Value::Str("back".into())]]);
    // Bulk indexed delete.
    assert_eq!(
        db.execute("DELETE FROM t WHERE id >= 28").unwrap(),
        ExecOutcome::Affected(2)
    );
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM t").rows[0][0],
        Value::Int(28)
    );
}

#[test]
fn fast_update_swaps_keys_and_keeps_others() {
    let mut db = idx_db();
    insert_n(&mut db, 20);
    db.execute("UPDATE t SET id = 999, name = 'moved' WHERE id = 3")
        .unwrap();
    // old key gone, new key reachable
    assert_eq!(rows(&mut db, "SELECT id FROM t WHERE id = 3").rows.len(), 0);
    let r = rows(&mut db, "SELECT name FROM t WHERE id = 999");
    assert_eq!(r.rows, vec![vec![Value::Str("moved".into())]]);
    // every other row intact, in order
    let r = rows(&mut db, "SELECT id FROM t WHERE id < 10 ORDER BY id");
    let ids: Vec<i64> = r.rows.iter().filter_map(|v| v[0].as_i64()).collect();
    assert_eq!(ids, vec![0, 1, 2, 4, 5, 6, 7, 8, 9]);
    // moving a key onto an occupied one fails cleanly
    let e = db.execute("UPDATE t SET id = 5 WHERE id = 4").unwrap_err();
    assert!(e.to_string().contains("UNIQUE"), "{e}");
    // and the failed UPDATE changed nothing
    let r = rows(&mut db, "SELECT name FROM t WHERE id = 4");
    assert_eq!(r.rows, vec![vec![Value::Str("u4".into())]]);
}

#[test]
fn rollback_restores_truncate_and_large_tables_from_page_undo() {
    // Transaction rollback rides the pager's page undo journal (no
    // whole-database deep copy at BEGIN). TRUNCATE rewrites every page:
    // rolling it back must restore content and page ownership exactly.
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
    for i in 0..300 {
        run(&mut db, &format!("INSERT INTO t VALUES ({i}, 'value-{i}')"));
    }
    run(&mut db, "BEGIN");
    run(&mut db, "TRUNCATE TABLE t");
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM t").rows[0][0],
        Value::Int(0)
    );
    run(&mut db, "INSERT INTO t VALUES (999, 'after-truncate')");
    run(&mut db, "ROLLBACK");
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM t").rows[0][0],
        Value::Int(300)
    );
    assert_eq!(
        rows(&mut db, "SELECT v FROM t WHERE id = 7").rows,
        vec![vec![Value::Str("value-7".into())]]
    );
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM t WHERE id = 999").rows[0][0],
        Value::Int(0)
    );
    // Page ownership after the rollback is coherent: new writes reuse
    // storage and survive a reopen.
    run(&mut db, "INSERT INTO t VALUES (1000, 'post-rollback')");
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rb-undo.db");
    drop(db);
    let mut disk = Database::open(&path).unwrap();
    run(&mut disk, "CREATE TABLE u (id INT PRIMARY KEY)");
    for i in 0..50 {
        run(&mut disk, &format!("INSERT INTO u VALUES ({i})"));
    }
    let pages = disk.num_pages();
    run(&mut disk, "BEGIN");
    run(&mut disk, "DELETE FROM u");
    run(&mut disk, "ROLLBACK");
    assert_eq!(
        rows(&mut disk, "SELECT COUNT(*) FROM u").rows[0][0],
        Value::Int(50)
    );
    // Reopen after the rollback: everything intact.
    drop(disk);
    let mut disk = Database::open(&path).unwrap();
    assert_eq!(
        rows(&mut disk, "SELECT COUNT(*) FROM u").rows[0][0],
        Value::Int(50)
    );
    assert!(disk.num_pages() <= pages + 8);
}

#[test]
fn rewrite_truncate_and_drop_reuse_pages() {
    // Page reuse: rebuild/TRUNCATE/DROP churn must not grow the data file
    // without bound (the old behavior orphaned every replaced page).
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
    for i in 0..200 {
        run(&mut db, &format!("INSERT INTO t VALUES ({i}, 'value-{i}')"));
    }
    let baseline = db.num_pages();
    for _ in 0..5 {
        run(&mut db, "TRUNCATE TABLE t");
        for i in 0..200 {
            run(&mut db, &format!("INSERT INTO t VALUES ({i}, 'value-{i}')"));
        }
    }
    assert!(
        db.num_pages() <= baseline + 16,
        "TRUNCATE churn grew the file: {baseline} -> {}",
        db.num_pages()
    );
    for _ in 0..5 {
        run(&mut db, "UPDATE t SET v = v || '!'");
    }
    assert!(
        db.num_pages() <= baseline + 32,
        "rewrite churn grew the file: {baseline} -> {}",
        db.num_pages()
    );
    // Data is intact after all the churn.
    let r = rows(&mut db, "SELECT COUNT(*) FROM t");
    assert_eq!(r.rows[0][0], Value::Int(200));
    let before_drop = db.num_pages();
    run(&mut db, "DROP TABLE t");
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
    assert!(
        db.num_pages() <= before_drop + 4,
        "DROP+CREATE grew the file: {before_drop} -> {}",
        db.num_pages()
    );
}

#[test]
fn fast_update_repoints_processed_rows_after_repack() {
    // Heap order is insertion order; the index probe visits rows in key
    // order. Inserting id=2 then id=1 makes the probe update the *later*
    // slot first: B(id=1, slot 1) gets its new (in-place) image first,
    // then A(id=2, slot 0) grows past the page and its re-pack shifts B
    // down to slot 0. B is already processed, so its locator in
    // `updates` is still the original (slot 1) — the old moved-locator
    // filter saw that match and skipped repointing B's index entry,
    // leaving it pointing at A's new image.
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (2, 'a')").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'b')").unwrap();
    let big = "v".repeat(2200);
    db.execute(&format!("UPDATE t SET v = '{big}' WHERE id > 0"))
        .unwrap();
    for id in [1i64, 2] {
        let r = rows(&mut db, &format!("SELECT v FROM t WHERE id = {id}"));
        assert_eq!(r.rows.len(), 1, "id={id} lost from the index: {r:?}");
        assert_eq!(r.rows[0][0], Value::Str(big.clone()), "id={id} wrong row");
    }
    let r = rows(&mut db, "SELECT id FROM t ORDER BY id");
    let ids: Vec<i64> = r.rows.iter().filter_map(|v| v[0].as_i64()).collect();
    assert_eq!(ids, vec![1, 2]);
    let r = rows(&mut db, "SELECT COUNT(*) FROM t WHERE v IS NOT NULL");
    assert_eq!(r.rows[0][0], Value::Int(2));
}

#[test]
fn fast_update_shifts_unique_keys_across_rows() {
    let mut db = idx_db();
    insert_n(&mut db, 5);
    // A multi-row shift used to collide each row's new key with the
    // next row's still-present old entry.
    db.execute("UPDATE t SET id = id + 1 WHERE id >= 0")
        .unwrap();
    let r = rows(&mut db, "SELECT id FROM t ORDER BY id");
    let ids: Vec<i64> = r.rows.iter().filter_map(|v| v[0].as_i64()).collect();
    assert_eq!(ids, vec![1, 2, 3, 4, 5]);
    // the trees stay consistent for probes after the shift
    for i in 1..=5i64 {
        assert_eq!(
            rows(&mut db, &format!("SELECT name FROM t WHERE id = {i}")).rows,
            vec![vec![Value::Str(format!("u{}", i - 1))]]
        );
    }
    assert_eq!(rows(&mut db, "SELECT id FROM t WHERE id = 0").rows.len(), 0);
    // keys swapped between two rows in one statement
    db.execute("UPDATE t SET id = CASE id WHEN 1 THEN 2 ELSE 1 END WHERE id <= 2")
        .unwrap();
    let r = rows(&mut db, "SELECT id, name FROM t WHERE id <= 2 ORDER BY id");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(1), Value::Str("u1".into())],
            vec![Value::Int(2), Value::Str("u0".into())],
        ]
    );
}

#[test]
fn rollback_discards_autoinc_watermark_of_created_table() {
    let mut db = Database::in_memory().unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("CREATE TABLE t (id INT AUTOINCREMENT, v TEXT)")
        .unwrap();
    db.execute("INSERT INTO t (v) VALUES ('a'), ('b')").unwrap();
    db.execute("ROLLBACK").unwrap();
    db.execute("CREATE TABLE t (id INT AUTOINCREMENT, v TEXT)")
        .unwrap();
    db.execute("INSERT INTO t (v) VALUES ('c')").unwrap();
    // Without the cache wipe the recreated table resumed at 3, and any
    // peer replaying the same INSERTs diverged from max+1 = 1.
    let r = rows(&mut db, "SELECT id FROM t");
    assert_eq!(r.rows, vec![vec![Value::Int(1)]]);
    // same hazard through ROLLBACK TO SAVEPOINT for a table created
    // after the savepoint
    db.execute("BEGIN").unwrap();
    db.execute("SAVEPOINT sp").unwrap();
    db.execute("CREATE TABLE u (id INT AUTOINCREMENT, v TEXT)")
        .unwrap();
    db.execute("INSERT INTO u (v) VALUES ('x'), ('y')").unwrap();
    db.execute("ROLLBACK TO SAVEPOINT sp").unwrap();
    db.execute("CREATE TABLE u (id INT AUTOINCREMENT, v TEXT)")
        .unwrap();
    db.execute("INSERT INTO u (v) VALUES ('z')").unwrap();
    let r = rows(&mut db, "SELECT id FROM u");
    assert_eq!(r.rows, vec![vec![Value::Int(1)]]);
    db.execute("COMMIT").unwrap();
}

#[test]
fn aggregate_arithmetic_in_projection() {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE t (g TEXT, x INT)").unwrap();
    db.execute("INSERT INTO t VALUES ('a', 1), ('a', 2), ('b', 10)")
        .unwrap();
    let r = rows(&mut db, "SELECT SUM(x) + 1 FROM t");
    assert_eq!(r.rows, vec![vec![Value::Int(14)]]);
    let r = rows(&mut db, "SELECT -COUNT(*) FROM t");
    assert_eq!(r.rows, vec![vec![Value::Int(-3)]]);
    let r = rows(&mut db, "SELECT COALESCE(SUM(x), 0) FROM t WHERE x > 99");
    assert_eq!(r.rows, vec![vec![Value::Int(0)]]);
    let r = rows(
        &mut db,
        "SELECT g, COUNT(*) * 2 FROM t GROUP BY g ORDER BY g",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("a".into()), Value::Int(4)],
            vec![Value::Str("b".into()), Value::Int(2)],
        ]
    );
    // HAVING over aggregate arithmetic
    let r = rows(&mut db, "SELECT g FROM t GROUP BY g HAVING SUM(x) + 1 > 4");
    assert_eq!(r.rows, vec![vec![Value::Str("b".into())]]);
    // a bare non-group column inside composite arithmetic still errors
    let e = db
        .execute("SELECT x + COUNT(*) FROM t GROUP BY g")
        .unwrap_err();
    assert!(e.to_string().contains("must appear in GROUP BY"), "{e}");
}

#[test]
fn duplicate_savepoint_names_resolve_to_newest() {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE t (id INT)").unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.execute("SAVEPOINT sp").unwrap();
    db.execute("INSERT INTO t VALUES (2)").unwrap();
    db.execute("SAVEPOINT sp").unwrap();
    db.execute("INSERT INTO t VALUES (3)").unwrap();
    // rolls back to the SECOND sp (row 3 gone), not the first
    db.execute("ROLLBACK TO SAVEPOINT sp").unwrap();
    let r = rows(&mut db, "SELECT COUNT(*) FROM t");
    assert_eq!(r.rows[0][0], Value::Int(2));
    // ROLLBACK TO dropped the second sp; the remaining one predates row 2
    db.execute("ROLLBACK TO SAVEPOINT sp").unwrap();
    let r = rows(&mut db, "SELECT COUNT(*) FROM t");
    assert_eq!(r.rows[0][0], Value::Int(1));
    db.execute("COMMIT").unwrap();
}

#[test]
fn assignments_bind_the_pre_update_image() {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE t (a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 0)").unwrap();
    db.execute("UPDATE t SET a = a + 1, b = a").unwrap();
    let r = rows(&mut db, "SELECT a, b FROM t");
    assert_eq!(r.rows, vec![vec![Value::Int(2), Value::Int(1)]]);
    // the fast (index-probed) path binds identically
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    db.execute("INSERT INTO u VALUES (1, 5, 0)").unwrap();
    db.execute("UPDATE u SET a = a + 1, b = a WHERE id = 1")
        .unwrap();
    let r = rows(&mut db, "SELECT a, b FROM u");
    assert_eq!(r.rows, vec![vec![Value::Int(6), Value::Int(5)]]);
}

#[test]
fn non_integer_limit_is_an_error() {
    let mut db = idx_db();
    insert_n(&mut db, 3);
    let e = db.execute("SELECT id FROM t LIMIT 2.5").unwrap_err();
    assert!(e.to_string().contains("LIMIT"), "{e}");
    // integers still fine; negative means "no limit" (SQLite)
    assert_eq!(rows(&mut db, "SELECT id FROM t LIMIT 2").rows.len(), 2);
    assert_eq!(rows(&mut db, "SELECT id FROM t LIMIT -1").rows.len(), 3);
}

#[test]
fn non_finite_float_literals_are_rejected() {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE t (x FLOAT)").unwrap();
    let e = db.execute("INSERT INTO t VALUES (1e999)").unwrap_err();
    assert!(e.to_string().contains("non-finite"), "{e}");
    db.execute("INSERT INTO t VALUES (1.5)").unwrap();
    assert_eq!(
        rows(&mut db, "SELECT x FROM t").rows[0][0],
        Value::Float(1.5)
    );
}

#[test]
fn rename_column_keeps_multibyte_check_references() {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE t (a INT CHECK (a > 0), \"éa\" INT CHECK (\"éa\" > 0))")
        .unwrap();
    run(&mut db, "ALTER TABLE t RENAME COLUMN a TO b");
    // the renamed column's check followed the rename
    let e = db.execute("INSERT INTO t (b) VALUES (-1)").unwrap_err();
    assert!(e.to_string().contains("CHECK"), "{e}");
    // the multibyte column's check was NOT rewritten (the ASCII-word
    // boundary bug renamed the `a` inside `éa`, and a missing column in
    // a CHECK reads as NULL → constraint silently disabled)
    let e = db
        .execute("INSERT INTO t (\"éa\") VALUES (-1)")
        .unwrap_err();
    assert!(e.to_string().contains("CHECK"), "{e}");
    db.execute("INSERT INTO t (b, \"éa\") VALUES (2, 2)")
        .unwrap();
}

#[test]
fn update_from_updates_identical_duplicate_rows() {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE t (id INT, v TEXT)").unwrap();
    // two byte-identical rows and no PK
    db.execute("INSERT INTO t VALUES (1, 'x'), (1, 'x')")
        .unwrap();
    db.execute("CREATE TABLE s (id INT, nv TEXT)").unwrap();
    db.execute("INSERT INTO s VALUES (1, 'y')").unwrap();
    db.execute("UPDATE t SET v = s.nv FROM s WHERE t.id = s.id")
        .unwrap();
    // both duplicates receive the update (content-keyed matching used
    // to update only the first)
    let r = rows(&mut db, "SELECT COUNT(*) FROM t WHERE v = 'y'");
    assert_eq!(r.rows[0][0], Value::Int(2));
}

#[test]
fn create_index_builds_usable_tree() {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE t (id INT, name TEXT)").unwrap();
    for i in 0..40 {
        db.execute(&format!("INSERT INTO t VALUES ({i}, 'n{}')", i % 4))
            .unwrap();
    }
    db.execute("CREATE INDEX idx_name ON t (name)").unwrap();
    // duplicate keys: every match returned
    let r = rows(&mut db, "SELECT id FROM t WHERE name = 'n1' ORDER BY id");
    assert_eq!(r.rows.len(), 10);
    // rows inserted after CREATE INDEX are found too
    db.execute("INSERT INTO t VALUES (100, 'n1')").unwrap();
    let r = rows(&mut db, "SELECT id FROM t WHERE name = 'n1' ORDER BY id");
    assert_eq!(r.rows.len(), 11);
    // a fast-path delete keeps the tree consistent
    db.execute("DELETE FROM t WHERE name = 'n1' AND id = 100")
        .unwrap();
    let r = rows(&mut db, "SELECT id FROM t WHERE name = 'n1' ORDER BY id");
    assert_eq!(r.rows.len(), 10);
}

#[test]
fn create_unique_index_enforces_duplicates() {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, email TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a@x')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'b@x')").unwrap();
    db.execute("CREATE UNIQUE INDEX ux_email ON t (email)")
        .unwrap();
    // duplicate insert rejected
    let e = db.execute("INSERT INTO t VALUES (3, 'a@x')").unwrap_err();
    assert!(e.to_string().contains("UNIQUE"), "{e}");
    // duplicate via UPDATE rejected
    let e = db
        .execute("UPDATE t SET email = 'b@x' WHERE id = 1")
        .unwrap_err();
    assert!(e.to_string().contains("UNIQUE"), "{e}");
    // NULLs don't collide
    db.execute("INSERT INTO t VALUES (4, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (5, NULL)").unwrap();
    // lookups over the unique tree still work
    let r = rows(&mut db, "SELECT id FROM t WHERE email = 'a@x'");
    assert_eq!(r.rows.len(), 1);
}

#[test]
fn create_unique_index_over_duplicates_fails() {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'dup')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'dup')").unwrap();
    let e = db.execute("CREATE UNIQUE INDEX ux_v ON t (v)").unwrap_err();
    assert!(e.to_string().contains("UNIQUE"), "{e}");
}

#[test]
fn create_index_if_not_exists_is_idempotent() {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    db.execute("CREATE INDEX IF NOT EXISTS iv ON t (v)")
        .unwrap();
    db.execute("CREATE INDEX IF NOT EXISTS iv ON t (v)")
        .unwrap();
    // plain CREATE of the same name still errors (name taken)
    let e = db.execute("CREATE INDEX iv ON t (v)").unwrap_err();
    assert!(e.to_string().contains("already exists"), "{e}");
}

#[test]
fn constraint_errors_carry_table_name() {
    // NOT NULL / CHECK / FK failures must name the table: multi-table
    // statements (JOIN, INSERT..SELECT) otherwise leave the user
    // guessing WHICH table's column failed. UNIQUE carries the table on
    // the set-level path (check_unique); tree-level duplicates identify
    // the key column via the index root key.
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE customers (id INT PRIMARY KEY)")
        .unwrap();
    db.execute("INSERT INTO customers (id) VALUES (1)").unwrap();
    db.execute(
        "CREATE TABLE orders (id INT PRIMARY KEY, sku TEXT NOT NULL, \
             cid INT REFERENCES customers (id), CHECK (id >= 0))",
    )
    .unwrap();
    let e = db
        .execute("INSERT INTO orders (id, cid) VALUES (1, 1)")
        .unwrap_err();
    assert!(
        e.to_string()
            .contains("NOT NULL constraint failed: orders.sku"),
        "{e}"
    );
    let e = db
        .execute("INSERT INTO orders (id, sku, cid) VALUES (-1, 'x', 1)")
        .unwrap_err();
    assert!(
        e.to_string().contains("CHECK constraint failed on orders"),
        "{e}"
    );
    let e = db
        .execute("INSERT INTO orders (id, sku, cid) VALUES (1, 'x', 99)")
        .unwrap_err();
    assert!(
        e.to_string()
            .contains("FOREIGN KEY constraint failed: orders.cid references customers.id"),
        "{e}"
    );
    // The .NET driver detects unique violations by the message prefix —
    // both qualification shapes must keep it.
    let e = db
        .execute("INSERT INTO orders (id, sku, cid) VALUES (1, 'a', 1)")
        .unwrap();
    drop(e);
    let e = db
        .execute("INSERT INTO orders (id, sku, cid) VALUES (1, 'b', 1)")
        .unwrap_err();
    assert!(e.to_string().starts_with("UNIQUE constraint failed"), "{e}");
}

#[test]
fn create_index_rejects_bad_targets() {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b TEXT)")
        .unwrap();
    // missing table
    let e = db.execute("CREATE INDEX i ON missing (a)").unwrap_err();
    assert!(e.to_string().contains("does not exist"), "{e}");
    // missing column
    let e = db.execute("CREATE INDEX i ON t (nope)").unwrap_err();
    assert!(e.to_string().contains("does not exist"), "{e}");
    // duplicate column inside one index
    let e = db.execute("CREATE INDEX i ON t (a, a)").unwrap_err();
    assert!(e.to_string().contains("duplicate column name: a"), "{e}");
    // duplicate name carries the name in the message
    db.execute("CREATE INDEX i ON t (a)").unwrap();
    let e = db.execute("CREATE INDEX i ON t (a)").unwrap_err();
    assert!(e.to_string().contains("index i already exists"), "{e}");
    // IF NOT EXISTS on the same name is a no-op, not an error
    db.execute("CREATE INDEX IF NOT EXISTS i ON t (a)").unwrap();
}

#[test]
fn unique_index_update_conflict_and_rollback_integrity() {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, email TEXT)")
        .unwrap();
    db.execute("CREATE UNIQUE INDEX ux ON t (email)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a@x')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'b@x')").unwrap();
    // moving a value onto an occupied one fails
    let e = db
        .execute("UPDATE t SET email = 'b@x' WHERE id = 1")
        .unwrap_err();
    assert!(e.to_string().contains("UNIQUE"), "{e}");
    // failed UPDATE changed nothing
    let r = rows(&mut db, "SELECT email FROM t WHERE id = 1");
    assert_eq!(r.rows, vec![vec![Value::Str("a@x".into())]]);
}

#[test]
fn second_unique_index_keeps_constraint_after_drop() {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    db.execute("CREATE UNIQUE INDEX ux1 ON t (v)").unwrap();
    db.execute("CREATE UNIQUE INDEX ux2 ON t (v)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'x')").unwrap();
    let e = db.execute("INSERT INTO t VALUES (2, 'x')").unwrap_err();
    assert!(e.to_string().contains("UNIQUE"), "{e}");
    // dropping one of the two unique indexes on v keeps it enforced
    db.execute("DROP INDEX ux1").unwrap();
    let e = db.execute("INSERT INTO t VALUES (2, 'x')").unwrap_err();
    assert!(e.to_string().contains("UNIQUE"), "{e}");
    // dropping the last one lifts it
    db.execute("DROP INDEX ux2").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'x')").unwrap();
}

#[test]
fn unique_index_over_constraint_column_keeps_constraint_on_drop() {
    let mut db = Database::in_memory().unwrap();
    // v is UNIQUE via CREATE TABLE; an index on top must not weaken it
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT UNIQUE)")
        .unwrap();
    db.execute("CREATE UNIQUE INDEX ix_v ON t (v)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'x')").unwrap();
    db.execute("DROP INDEX ix_v").unwrap();
    let e = db.execute("INSERT INTO t VALUES (2, 'x')").unwrap_err();
    assert!(e.to_string().contains("UNIQUE"), "{e}");
}

#[test]
fn insert_or_ignore_respects_unique_index() {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    db.execute("CREATE UNIQUE INDEX ux ON t (v)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'x')").unwrap();
    // conflict on the unique index is ignored instead of erroring
    db.execute("INSERT OR IGNORE INTO t VALUES (2, 'x')")
        .unwrap();
    let r = rows(&mut db, "SELECT id FROM t ORDER BY id");
    assert_eq!(r.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn sqlite_master_reports_index_ddl_text() {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    db.execute("CREATE INDEX iv ON t (v)").unwrap();
    db.execute("CREATE UNIQUE INDEX uv ON t (id)").unwrap();
    let r = rows(
        &mut db,
        "SELECT name, sql FROM sqlite_master WHERE type = 'index' ORDER BY name",
    );
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::Str("iv".into()));
    // Identifiers are quoted now: the EF provider replays this DDL
    // verbatim in its keep-data rebuild, and a reserved-word name used
    // to fail the replay (taking user indexes with it).
    assert_eq!(
        r.rows[0][1],
        Value::Str("CREATE INDEX \"iv\" ON \"t\" (\"v\")".into())
    );
    assert_eq!(r.rows[1][0], Value::Str("uv".into()));
    assert_eq!(
        r.rows[1][1],
        Value::Str("CREATE UNIQUE INDEX \"uv\" ON \"t\" (\"id\")".into())
    );
    // tables and indexes coexist in the view
    let r = rows(&mut db, "SELECT type FROM sqlite_master ORDER BY type");
    let tables = r
        .rows
        .iter()
        .filter(|v| v[0] == Value::Str("table".into()))
        .count();
    assert_eq!(tables, 1);
}

#[test]
fn index_names_are_database_wide() {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE a (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    db.execute("CREATE TABLE b (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    db.execute("CREATE INDEX iv ON a (v)").unwrap();
    // SQLite semantics: the name is taken database-wide
    let e = db.execute("CREATE INDEX iv ON b (v)").unwrap_err();
    assert!(e.to_string().contains("already exists"), "{e}");
    // DROP removes it; the other table is untouched
    db.execute("DROP INDEX iv").unwrap();
    let r = rows(
        &mut db,
        "SELECT tbl_name FROM sqlite_master WHERE type = 'index'",
    );
    assert!(r.rows.is_empty());
    // ...and the name is free again
    db.execute("CREATE INDEX iv ON b (v)").unwrap();
}

#[test]
fn drop_unique_index_lifts_constraint() {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, email TEXT, code INT UNIQUE)")
        .unwrap();
    db.execute("CREATE UNIQUE INDEX ux_email ON t (email)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a@x', 10)").unwrap();
    let e = db
        .execute("INSERT INTO t VALUES (2, 'a@x', 11)")
        .unwrap_err();
    assert!(e.to_string().contains("UNIQUE"), "{e}");

    db.execute("DROP INDEX ux_email").unwrap();
    // uniqueness lifted: duplicates on email now allowed...
    db.execute("INSERT INTO t VALUES (2, 'a@x', 11)").unwrap();
    // ...while the CREATE TABLE UNIQUE constraint on code still holds
    let e = db
        .execute("INSERT INTO t VALUES (3, 'b@x', 10)")
        .unwrap_err();
    assert!(e.to_string().contains("UNIQUE"), "{e}");
}

#[test]
fn drop_index_survives_reopen_and_sqlite_master_lists_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("idxdef.db");
    {
        let mut db = Database::open(&path).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
            .unwrap();
        db.execute("CREATE UNIQUE INDEX ix_v ON t (v)").unwrap();
        // definition visible through sqlite_master before close
        let r = rows(
            &mut db,
            "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 't'",
        );
        assert_eq!(r.rows, vec![vec![Value::Str("ix_v".into())]]);
    }
    let mut db = Database::open(&path).unwrap();
    let r = rows(
        &mut db,
        "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 't'",
    );
    assert_eq!(r.rows, vec![vec![Value::Str("ix_v".into())]]);
    // constraint persisted: duplicate rejected after reopen
    db.execute("INSERT INTO t VALUES (1, 'x')").unwrap();
    let e = db.execute("INSERT INTO t VALUES (2, 'x')").unwrap_err();
    assert!(e.to_string().contains("UNIQUE"), "{e}");
    // ...and dropping it after reopen lifts the constraint
    db.execute("DROP INDEX ix_v").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'x')").unwrap();
}

#[test]
fn catalog_reopen_roundtrips_constraints_and_overflow_free() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cattrip.db");
    {
        let mut db = Database::open(&path).unwrap();
        run(
            &mut db,
            "CREATE TABLE parent (pid INT PRIMARY KEY, tag TEXT)",
        );
        run(
            &mut db,
            "CREATE TABLE child (id INT PRIMARY KEY AUTOINCREMENT, pid INT, \
                 note TEXT DEFAULT ('n/a'), flag BOOL, \
                 CHECK (id >= 0), FOREIGN KEY (pid) REFERENCES parent (pid))",
        );
        run(
            &mut db,
            "CREATE UNIQUE INDEX ux_child_pair ON child (note, flag)",
        );
        run(&mut db, "INSERT INTO parent VALUES (1, 'a')");
        run(
            &mut db,
            "INSERT INTO child (pid, note, flag) VALUES (1, 'n1', true)",
        );
        // Overflow-chain document, then freed so overflow_free is non-empty.
        let long = "x".repeat(20_000);
        run(
            &mut db,
            &format!(
                "INSERT INTO child (pid, note, flag, blob) VALUES (1, 'long', false, '{long}')"
            ),
        );
        run(&mut db, "DELETE FROM child WHERE flag = false");
    }
    let mut db = Database::open(&path).unwrap();
    // The composite index is a pair, not per-column uniqueness: repeating
    // row 1's note with a different flag is fine...
    run(&mut db, "INSERT INTO parent VALUES (2, 'b')");
    run(
        &mut db,
        "INSERT INTO child (pid, note, flag) VALUES (2, 'n1', false)",
    );
    assert!(
        db.execute("INSERT INTO child (pid) VALUES (99)").is_err(),
        "FK lost"
    );
    assert!(
        db.execute("INSERT INTO child (id, pid) VALUES (-1, 2)")
            .is_err(),
        "CHECK lost"
    );
    // ...but the exact pair is a duplicate.
    assert!(
        db.execute("INSERT INTO child (pid, note, flag) VALUES (2, 'n1', false)")
            .is_err(),
        "composite UNIQUE lost"
    );
    // DEFAULT and AUTOINCREMENT survived reopen.
    run(&mut db, "INSERT INTO child (pid, flag) VALUES (2, true)");
    assert_eq!(
        rows(
            &mut db,
            "SELECT note, id FROM child WHERE flag = true AND pid = 2"
        )
        .rows[0],
        vec![Value::Str("n/a".into()), Value::Int(3)]
    );
}

#[test]
fn join_key_of_covers_every_value_kind() {
    assert_eq!(join_key_of(&Value::Null), Some(JoinKey::Null));
    assert_eq!(join_key_of(&Value::Bool(false)), Some(JoinKey::Bool(false)));
    assert_eq!(
        join_key_of(&Value::Int(7)),
        Some(JoinKey::Num(num_key(7.0)))
    );
    assert_eq!(
        join_key_of(&Value::Float(-0.5)),
        Some(JoinKey::Num(num_key(-0.5)))
    );
    assert_eq!(
        join_key_of(&Value::Str("k".into())),
        Some(JoinKey::Str("k".into()))
    );
    assert_eq!(
        join_key_of(&Value::Bytes(vec![1, 2])),
        Some(JoinKey::Bytes(vec![1, 2]))
    );
    assert_eq!(
        join_key_of(&Value::Array(vec![Value::Int(1), Value::Str("a".into())])),
        Some(JoinKey::Array(vec![
            JoinKey::Num(num_key(1.0)),
            JoinKey::Str("a".into())
        ]))
    );
    // A nested item that has no key (Object) degrades the whole array.
    assert_eq!(
        join_key_of(&Value::Array(vec![Value::Object(Object::new())])),
        None
    );
    assert_eq!(join_key_of(&Value::Object(Object::new())), None);
}

fn parse_select_expr(sql: &str) -> SqlExpr {
    let mut stmts = Parser::parse_sql(&GenericDialect {}, &format!("SELECT {sql}")).unwrap();
    let Statement::Query(q) = stmts.swap_remove(0) else {
        panic!("not a query")
    };
    let SetExpr::Select(boxed) = *q.body else {
        panic!("not a select")
    };
    let SelectItem::UnnamedExpr(e) = boxed.projection.first().unwrap() else {
        panic!("not a bare expression")
    };
    e.clone()
}

#[test]
fn walk_expr_accepts_shapes_in_read_targets() {
    // UPDATE-shaped statements route WHERE through walk_expr; every
    // supported shape keeps the statement classifiable as a read-write.
    let ok = |sql: &str| {
        let mut stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        let stmt = stmts.swap_remove(0);
        assert!(
            Database::stmt_read_targets(&stmt, &|_| false).is_some(),
            "read-target classifier rejected supported shape: {sql}"
        );
    };
    ok("UPDATE t SET v = 1 WHERE CASE s WHEN 1 THEN v = 2 ELSE v = 3 END");
    ok("UPDATE t SET v = 1 WHERE SUBSTRING(name FROM 1 FOR 2) = 'a'");
    ok("UPDATE t SET v = 1 WHERE COALESCE(n, 0) < 5");
    ok("UPDATE t SET v = 1 WHERE n BETWEEN 1 AND 3");
    ok("UPDATE t SET v = 1 WHERE id IN (SELECT id FROM other)");
    // Rare shapes (tuples/ANY/...) are denied fail-closed.
    let mut out = Vec::new();
    assert!(walk_expr(&parse_select_expr("(1, 2)"), &mut out, &|_| false).is_none());
    // The parser never emits `Substring` under the generic dialect, so
    // walk its from/for arms directly.
    let sub = SqlExpr::Substring {
        expr: Box::new(parse_select_expr("name")),
        substring_from: Some(Box::new(parse_select_expr("2"))),
        substring_for: Some(Box::new(parse_select_expr("4"))),
        special: false,
        shorthand: false,
    };
    let mut out = Vec::new();
    assert!(walk_expr(&sub, &mut out, &|_| false).is_some());
}

#[test]
fn column_refs_covers_every_shape() {
    let c = |sql: &str| {
        let mut out = Vec::new();
        assert!(
            column_refs(&parse_select_expr(sql), &mut out).is_some(),
            "column_refs rejected {sql}"
        );
        out
    };
    assert_eq!(
        c("CASE id WHEN 1 THEN name ELSE other.v END"),
        vec!["id".to_string(), "name".to_string(), "other.v".to_string()]
    );
    assert_eq!(
        c("SUBSTRING(name FROM 2 FOR 4)"),
        vec!["name".to_string()],
        "integer substring bounds are literals, not column refs"
    );
    assert_eq!(c("COUNT(*)"), Vec::<String>::new());
    assert_eq!(c("ABS(n)"), vec!["n".to_string()]);
    assert_eq!(c("CAST(n AS TEXT)"), vec!["n".to_string()]);
    // 第六轮审查回归:聚合 FILTER 的谓词按行求值,其列是真实读——
    // 授权收集器漏掉它会让列级 GRANT 泄露被过滤列的布尔信息。
    assert_eq!(
        c("COUNT(id) FILTER (WHERE secret = 'x')"),
        vec!["id".to_string(), "secret".to_string()],
        "aggregate FILTER predicates are column reads"
    );
    // EXISTS / subqueries are unmodeled by the collector (stays residual).
    let mut out = Vec::new();
    assert!(column_refs(&parse_select_expr("EXISTS (SELECT 1 FROM other)"), &mut out).is_none());
}

#[test]
fn group_by_qualified_group_column_in_composite() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT, n INT)");
    run(&mut db, "INSERT INTO t VALUES (1, 10), (1, 20), (2, 5)");
    let r = rows(
        &mut db,
        "SELECT t.id + SUM(n) AS s FROM t GROUP BY t.id ORDER BY s",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(7)], vec![Value::Int(31)]]);
    // The aggregate arm with a scalar wrapper (COALESCE(SUM..)) is a
    // separate path; an empty group returns the scalar default.
    let r = rows(
        &mut db,
        "SELECT COALESCE(SUM(n), 0) AS total FROM t WHERE id = 99",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(0)]]);
}

#[test]
fn join_on_expr_covers_semi_anti_and_other_operators() {
    use sqlparser::ast::{JoinConstraint as JC, JoinOperator as JO};
    let on = parse_select_expr("a.id = b.id");
    for op in [
        JO::LeftSemi(JC::On(on.clone())),
        JO::RightSemi(JC::On(on.clone())),
        JO::LeftAnti(JC::On(on.clone())),
        JO::RightAnti(JC::On(on.clone())),
    ] {
        assert!(join_on_expr(&op).is_some(), "missing ON for {op:?}");
    }
    // Non-outer operators and non-ON constraints carry no ON predicate.
    assert!(join_on_expr(&JO::CrossJoin(JC::None)).is_none());
    assert!(join_on_expr(&JO::Semi(JC::Natural)).is_none());
    assert!(join_on_expr(&JO::Inner(JC::Using(vec![]))).is_none());
}

#[test]
fn explicit_id_update_resets_autoinc_watermark() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY AUTOINCREMENT, v TEXT)",
    );
    run(&mut db, "INSERT INTO t (v) VALUES ('a'), ('b')");
    // The fast path sees the assignment to the autoinc column and drops
    // the cached watermark; the next insert continues past the new max.
    run(&mut db, "UPDATE t SET id = 50 WHERE id = 1");
    run(&mut db, "INSERT INTO t (v) VALUES ('c')");
    let r = rows(&mut db, "SELECT id FROM t ORDER BY id");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(2)],
            vec![Value::Int(50)],
            vec![Value::Int(51)],
        ]
    );
}

#[test]
fn primary_key_constraint_index_is_derived_and_protected() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY, v TEXT UNIQUE)",
    );
    let t = &db.catalog()[0];
    // Table-declared PK/UNIQUE surface as named auto-indexes (PK
    // first); their B+ trees exist from CREATE TABLE on.
    assert_eq!(
        t.indexes,
        vec![
            "sqlite_autoindex_t_1".to_string(),
            "sqlite_autoindex_t_2".to_string(),
        ]
    );
    assert_eq!(t.index_defs[0].column, "id");
    assert_eq!(t.index_defs[1].column, "v");
    assert!(t.index_defs.iter().all(|d| d.auto && d.unique));

    // Hidden from sqlite_master (SQLite semantics: autoindexes have
    // no CREATE INDEX row there) — tooling reads them from the catalog.
    let r = rows(
        &mut db,
        "SELECT name FROM sqlite_master WHERE type = 'index'",
    );
    assert!(r.rows.is_empty());

    // Not droppable (with or without IF EXISTS), and the name prefix
    // is reserved against user indexes shadowing constraint ones.
    let e = db.execute("DROP INDEX sqlite_autoindex_t_1").unwrap_err();
    assert!(e.to_string().contains("cannot be dropped"), "{e}");
    let e = db
        .execute("DROP INDEX IF EXISTS sqlite_autoindex_t_1")
        .unwrap_err();
    assert!(e.to_string().contains("cannot be dropped"), "{e}");
    let e = db
        .execute("CREATE INDEX sqlite_autoindex_x_1 ON t (v)")
        .unwrap_err();
    assert!(e.to_string().contains("reserved"), "{e}");
    // The constraint itself stays enforced through the backing tree.
    run(&mut db, "INSERT INTO t VALUES (1, 'a')");
    let e = db.execute("INSERT INTO t VALUES (1, 'b')").unwrap_err();
    assert!(e.to_string().contains("UNIQUE"), "{e}");
}

#[test]
fn constraint_autoindex_survives_reopen_without_catalog_migration() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("autoix.db");
    {
        let mut db = Database::open(&path).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
            .unwrap();
    }
    // Derived from the persisted constraint, never stored in the
    // catalog: reopening lists it — as do databases created before
    // the feature shipped (external volumes keep their old catalogs).
    let db = Database::open(&path).unwrap();
    let t = &db.catalog()[0];
    assert_eq!(t.indexes, vec!["sqlite_autoindex_t_1".to_string()]);
    assert_eq!(t.index_defs[0].column, "id");
    assert!(t.index_defs[0].auto);
}

#[test]
fn autoindex_dedupes_column_declared_pk_and_unique() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY UNIQUE, v TEXT)",
    );
    assert_eq!(
        db.catalog()[0].indexes,
        vec!["sqlite_autoindex_t_1".to_string()]
    );
}

#[test]
fn autoindex_covers_table_level_constraints() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (a INT, b INT, PRIMARY KEY (a), UNIQUE (b))",
    );
    let t = &db.catalog()[0];
    assert_eq!(
        t.indexes,
        vec![
            "sqlite_autoindex_t_1".to_string(),
            "sqlite_autoindex_t_2".to_string(),
        ]
    );
    assert_eq!(t.index_defs[0].column, "a");
    assert_eq!(t.index_defs[1].column, "b");
}

#[test]
fn autoindex_follows_column_and_table_renames() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY, email TEXT UNIQUE)",
    );
    run(&mut db, "INSERT INTO t VALUES (1, 'a')");
    // Column renames move the constraint metadata (and the backing
    // tree), so the derived entries track the new names…
    run(&mut db, "ALTER TABLE t RENAME COLUMN id TO gid");
    run(&mut db, "ALTER TABLE t RENAME COLUMN email TO mail");
    // …and the table rename feeds the derived names themselves.
    run(&mut db, "ALTER TABLE t RENAME TO u");
    let t = &db.catalog()[0];
    assert_eq!(t.name, "u");
    assert_eq!(
        t.indexes,
        vec![
            "sqlite_autoindex_u_1".to_string(),
            "sqlite_autoindex_u_2".to_string(),
        ]
    );
    assert_eq!(t.index_defs[0].column, "gid");
    assert_eq!(t.index_defs[1].column, "mail");
    // The constraint keeps enforcing through the moved tree.
    let e = db.execute("INSERT INTO u VALUES (1, 'b')").unwrap_err();
    assert!(e.to_string().contains("UNIQUE"), "{e}");
}

#[test]
fn dropping_unique_column_drops_its_autoindex() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY, email TEXT UNIQUE)",
    );
    run(&mut db, "ALTER TABLE t DROP COLUMN email");
    let t = &db.catalog()[0];
    assert_eq!(t.indexes, vec!["sqlite_autoindex_t_1".to_string()]);
    assert_eq!(t.index_defs[0].column, "id");
    // PK enforcement is untouched by the UNIQUE column's removal.
    run(&mut db, "INSERT INTO t VALUES (1)");
    let e = db.execute("INSERT INTO t VALUES (1)").unwrap_err();
    assert!(e.to_string().contains("UNIQUE"), "{e}");
}

#[test]
fn autoindex_names_identical_on_replica_replay() {
    // Symmetric-cluster peers re-derive constraint indexes from the
    // replayed DDL; the names must come out identical on both sides.
    let ddl = "CREATE TABLE t (id INT PRIMARY KEY, email TEXT UNIQUE); \
                   CREATE INDEX ix_v ON t (email)";
    let mut a = Database::in_memory().unwrap();
    let mut b = Database::in_memory().unwrap();
    for db in [&mut a, &mut b] {
        db.execute_batch(ddl);
    }
    let (ca, cb) = (a.catalog(), b.catalog());
    assert_eq!(ca[0].name, cb[0].name);
    assert_eq!(ca[0].indexes, cb[0].indexes);
    assert_eq!(ca[0].index_defs, cb[0].index_defs);
    assert_eq!(
        ca[0].indexes,
        vec![
            "sqlite_autoindex_t_1".to_string(),
            "sqlite_autoindex_t_2".to_string(),
            "ix_v".to_string(),
        ]
    );
}

#[test]
fn autoinc_and_autoguid_primary_keys_get_autoindex() {
    for ddl in [
        "CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)",
        "CREATE TABLE t (id GUID PRIMARY KEY AUTOINCREMENT, v TEXT)",
    ] {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, ddl);
        let t = &db.catalog()[0];
        assert_eq!(t.indexes, vec!["sqlite_autoindex_t_1".to_string()], "{ddl}");
        assert_eq!(t.index_defs[0].column, "id", "{ddl}");
        assert!(t.index_defs[0].auto, "{ddl}");
    }
}

#[test]
fn indexes_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("idx.db");
    {
        let mut db = Database::open(&path).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'one')").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'two')").unwrap();
    }
    let mut db = Database::open(&path).unwrap();
    let r = rows(&mut db, "SELECT v FROM t WHERE id = 2");
    assert_eq!(r.rows, vec![vec![Value::Str("two".into())]]);
    let e = db.execute("INSERT INTO t VALUES (2, 'dup')").unwrap_err();
    assert!(e.to_string().contains("UNIQUE"), "{e}");
}

#[test]
fn autoinc_survives_delete_and_rollback() {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTOINCREMENT, v TEXT)")
        .unwrap();
    db.execute("INSERT INTO t (v) VALUES ('a')").unwrap();
    db.execute("INSERT INTO t (v) VALUES ('b')").unwrap();
    db.execute("DELETE FROM t WHERE id = 2").unwrap();
    // max(existing)+1: the only row is id=1, so the next id is 2 again
    db.execute("INSERT INTO t (v) VALUES ('c')").unwrap();
    let r = rows(&mut db, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(r.rows[1], vec![Value::Int(2), Value::Str("c".into())]);
    // rollback restores pre-BEGIN rows
    db.execute("BEGIN").unwrap();
    db.execute("DELETE FROM t WHERE id = 1").unwrap();
    db.execute("ROLLBACK").unwrap();
    let r = rows(&mut db, "SELECT COUNT(*) FROM t");
    assert_eq!(r.rows[0][0], Value::Int(2));
    // ...and the index still answers point queries afterwards
    let r = rows(&mut db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r.rows, vec![vec![Value::Str("a".into())]]);
}

#[test]
fn create_insert_select_roundtrip() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE users (id INT, name TEXT)");
    run(
        &mut db,
        "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')",
    );
    let r = rows(&mut db, "SELECT id, name FROM users");
    assert_eq!(r.columns, vec!["id", "name"]);
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][1], Value::Str("alice".into()));
}

#[test]
fn insert_without_column_list_uses_declared_columns() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (a INT, b TEXT)");
    run(&mut db, "INSERT INTO t VALUES (5, 'five')");
    let r = rows(&mut db, "SELECT a, b FROM t");
    assert_eq!(r.rows, vec![vec![Value::Int(5), Value::Str("five".into())]]);
}

#[test]
fn select_star_unions_schemaless_fields() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE docs (id INT)");
    run(
        &mut db,
        "INSERT INTO docs (id, extra) VALUES (1, 'has-extra')",
    );
    run(&mut db, "INSERT INTO docs (id) VALUES (2)");
    let r = rows(&mut db, "SELECT * FROM docs");
    assert_eq!(r.columns, vec!["extra", "id"]); // BTreeMap order
    assert_eq!(r.rows[0][0], Value::Str("has-extra".into()));
    assert_eq!(r.rows[1][0], Value::Null);
}

#[test]
fn observed_columns_track_document_keys() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE docs (id INT)");
    // Empty table: nothing observed yet (SELECT * projects nothing).
    assert!(db.observed_columns("docs").is_empty());
    run(&mut db, "INSERT INTO docs (id, extra) VALUES (1, 'x')");
    run(&mut db, "INSERT INTO docs (id, note) VALUES (2, 'y')");
    // Data-side union, independent of the declared column list.
    assert_eq!(db.observed_columns("docs"), vec!["extra", "id", "note"]);
    // Unknown tables are not an error on this introspection surface.
    assert!(db.observed_columns("nope").is_empty());
}

#[test]
fn where_filter_order_limit_offset() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (n INT, s TEXT)");
    for i in 0..10 {
        run(
            &mut db,
            &format!("INSERT INTO t (n, s) VALUES ({i}, 'x{i}')"),
        );
    }
    let r = rows(
        &mut db,
        "SELECT n, s FROM t WHERE n >= 3 AND n < 8 ORDER BY n DESC",
    );
    assert_eq!(r.rows.len(), 5);
    assert_eq!(r.rows[0][0], Value::Int(7));
    assert_eq!(r.rows[4][0], Value::Int(3));

    let r = rows(&mut db, "SELECT n FROM t ORDER BY n LIMIT 3 OFFSET 4");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(4)],
            vec![Value::Int(5)],
            vec![Value::Int(6)]
        ]
    );
}

#[test]
fn arithmetic_and_null_semantics() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (a INT, b FLOAT)");
    run(&mut db, "INSERT INTO t (a, b) VALUES (7, 2.5)");
    let r = rows(&mut db, "SELECT a + 1, a * 2, a / 2, b + 1 FROM t");
    assert_eq!(r.rows[0][0], Value::Int(8));
    assert_eq!(r.rows[0][1], Value::Int(14));
    assert_eq!(r.rows[0][2], Value::Int(3));
    assert_eq!(r.rows[0][3], Value::Float(3.5));

    run(&mut db, "INSERT INTO t (a) VALUES (9)");
    let r = rows(&mut db, "SELECT a + b FROM t WHERE a = 9");
    assert_eq!(r.rows[0][0], Value::Null); // null propagates
}

#[test]
fn division_by_zero_yields_null() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (a INT, b INT)");
    run(&mut db, "INSERT INTO t (a, b) VALUES (1, 0)");
    let r = rows(&mut db, "SELECT a / b FROM t");
    assert_eq!(r.rows[0][0], Value::Null);
}

#[test]
fn drop_table_and_error_cases() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (a INT)");
    run(&mut db, "DROP TABLE t");
    assert!(db.execute("SELECT * FROM t").is_err());
    assert!(db.execute("CREATE TABLE t (a INT); DROP TABLE t").is_err());
    assert!(db.execute("DROP TABLE missing").is_err());
    assert!(db.execute("CREATE TABLE t (a INT)").is_ok());
    assert!(db.execute("CREATE TABLE t (a INT)").is_err()); // duplicate
    assert!(db.execute("INSERT INTO t (a) VALUES (1, 2)").is_err()); // arity
    assert!(db.execute("SELECT * FROM missing").is_err());
}

#[test]
fn update_sets_matching_rows_and_keeps_others() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT, name TEXT)");
    run(&mut db, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    match run(&mut db, "UPDATE t SET name = 'z' WHERE id >= 2") {
        ExecOutcome::Affected(2) => {}
        o => panic!("affected={o:?}"),
    }
    let r = rows(&mut db, "SELECT id, name FROM t ORDER BY id");
    assert_eq!(r.rows[0][1], Value::Str("a".into()));
    assert_eq!(r.rows[1][1], Value::Str("z".into()));
    assert_eq!(r.rows[2][1], Value::Str("z".into()));
    // update without WHERE touches everything
    match run(&mut db, "UPDATE t SET name = 'all'") {
        ExecOutcome::Affected(3) => {}
        o => panic!("affected={o:?}"),
    }
}

#[test]
fn update_referencing_old_values() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (n INT)");
    run(&mut db, "INSERT INTO t VALUES (10)");
    run(&mut db, "UPDATE t SET n = n + 5");
    let r = rows(&mut db, "SELECT n FROM t");
    assert_eq!(r.rows, vec![vec![Value::Int(15)]]);
}

#[test]
fn delete_removes_matching_rows() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT)");
    for i in 0..5 {
        run(&mut db, &format!("INSERT INTO t VALUES ({i})"));
    }
    match run(&mut db, "DELETE FROM t WHERE id IN (1)") {
        ExecOutcome::Affected(1) => {}
        o => panic!("{o:?}"),
    }
    match run(&mut db, "DELETE FROM t WHERE id = 1") {
        ExecOutcome::Affected(0) => {} // already removed by the IN delete
        o => panic!("affected={o:?}"),
    }
    match run(&mut db, "DELETE FROM t WHERE id > 3") {
        ExecOutcome::Affected(1) => {}
        o => panic!("affected={o:?}"),
    }
    let r = rows(&mut db, "SELECT id FROM t ORDER BY id");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(0)],
            vec![Value::Int(2)],
            vec![Value::Int(3)]
        ]
    );
    match run(&mut db, "DELETE FROM t") {
        ExecOutcome::Affected(3) => {}
        o => panic!("affected={o:?}"),
    }
    let r = rows(&mut db, "SELECT id FROM t");
    assert!(r.rows.is_empty());
}

#[test]
fn primary_key_and_unique_constraints() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY, email TEXT UNIQUE, name TEXT)",
    );
    run(
        &mut db,
        "INSERT INTO t (id, email, name) VALUES (1, 'a@x', 'ann')",
    );
    // duplicate PK
    let e = db
        .execute("INSERT INTO t (id, email, name) VALUES (1, 'b@x', 'bob')")
        .unwrap_err();
    assert!(e.to_string().contains("UNIQUE"), "{e}");
    // duplicate UNIQUE
    let e = db
        .execute("INSERT INTO t (id, email, name) VALUES (2, 'a@x', 'bob')")
        .unwrap_err();
    assert!(e.to_string().contains("UNIQUE"), "{e}");
    // distinct is fine
    run(
        &mut db,
        "INSERT INTO t (id, email, name) VALUES (2, 'b@x', 'bob')",
    );
    // UPDATE that would collide is rejected
    let e = db
        .execute("UPDATE t SET email = 'a@x' WHERE id = 2")
        .unwrap_err();
    assert!(e.to_string().contains("UNIQUE"), "{e}");
}

#[test]
fn not_null_constraint() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (a INT NOT NULL)");
    let e = db.execute("INSERT INTO t (a) VALUES (NULL)").unwrap_err();
    assert!(e.to_string().contains("NOT NULL"), "{e}");
    run(&mut db, "INSERT INTO t (a) VALUES (5)");
    let e = db.execute("UPDATE t SET a = NULL").unwrap_err();
    assert!(e.to_string().contains("NOT NULL"), "{e}");
}

#[test]
fn constraints_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cons.db");
    {
        let mut db = Database::open(&path).unwrap();
        run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY)");
        run(&mut db, "INSERT INTO t (id) VALUES (1)");
    }
    let mut db = Database::open(&path).unwrap();
    assert!(db.execute("INSERT INTO t (id) VALUES (1)").is_err());
}

#[test]
fn alter_table_add_and_drop_column() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (a INT)");
    run(&mut db, "INSERT INTO t (a) VALUES (1)");
    run(&mut db, "ALTER TABLE t ADD COLUMN b TEXT");
    run(&mut db, "INSERT INTO t (a, b) VALUES (2, 'two')");
    let r = rows(&mut db, "SELECT a, b FROM t ORDER BY a");
    assert_eq!(r.rows[0][1], Value::Null);
    assert_eq!(r.rows[1][1], Value::Str("two".into()));
    run(&mut db, "ALTER TABLE t DROP COLUMN b");
    let r = rows(&mut db, "SELECT a FROM t ORDER BY a");
    assert_eq!(r.columns, vec!["a"]);
    assert_eq!(r.rows.len(), 2);
    assert!(db.execute("ALTER TABLE missing ADD COLUMN x INT").is_err());
}

#[test]
fn inner_join_with_on() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE u (uid INT, name TEXT)");
    run(&mut db, "CREATE TABLE o (oid INT, uid INT, item TEXT)");
    run(&mut db, "INSERT INTO u VALUES (1, 'ann'), (2, 'bob')");
    run(
        &mut db,
        "INSERT INTO o VALUES (10, 1, 'book'), (11, 1, 'pen'), (12, 2, 'cap'), (13, 9, 'ghost')",
    );
    let r = rows(
        &mut db,
        "SELECT u.name, o.item FROM u JOIN o ON u.uid = o.uid ORDER BY o.oid",
    );
    assert_eq!(r.columns, vec!["u.name", "o.item"]);
    assert_eq!(r.rows.len(), 3); // ghost dropped
    assert_eq!(
        r.rows[0],
        vec![Value::Str("ann".into()), Value::Str("book".into())]
    );
    assert_eq!(
        r.rows[2],
        vec![Value::Str("bob".into()), Value::Str("cap".into())]
    );
}

#[test]
fn left_join_keeps_unmatched() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE a (id INT)");
    run(&mut db, "CREATE TABLE b (id INT, aid INT, v TEXT)");
    run(&mut db, "INSERT INTO a VALUES (1), (2)");
    run(&mut db, "INSERT INTO b VALUES (100, 1, 'x')");
    let r = rows(
        &mut db,
        "SELECT a.id, b.v FROM a LEFT JOIN b ON b.aid = a.id ORDER BY a.id",
    );
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[1], vec![Value::Int(2), Value::Null]);
}

#[test]
fn cross_join_cartesian() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE x (a INT)");
    run(&mut db, "CREATE TABLE y (b INT)");
    run(&mut db, "INSERT INTO x VALUES (1), (2)");
    run(&mut db, "INSERT INTO y VALUES (3), (4), (5)");
    let r = rows(&mut db, "SELECT x.a, y.b FROM x CROSS JOIN y");
    assert_eq!(r.rows.len(), 6);
}

// ---- hash join path (equi_plan) semantics ----

#[test]
fn hash_join_null_equals_null_matches() {
    // Engine semantics: `=` is cmp_values-based, so NULL = NULL joins.
    // The canonical key must include NULL or this pair would be missed.
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE p (k INT, tag TEXT)");
    run(&mut db, "CREATE TABLE q (k INT, val TEXT)");
    run(&mut db, "INSERT INTO p VALUES (NULL, 'pnull'), (1, 'p1')");
    run(
        &mut db,
        "INSERT INTO q VALUES (NULL, 'qnull'), (1, 'q1'), (2, 'q2')",
    );
    let r = rows(
        &mut db,
        "SELECT p.tag, q.val FROM p JOIN q ON p.k = q.k ORDER BY p.tag",
    );
    // NULL-NULL pair matches, plus the 1-1 pair; 2 never matches.
    assert_eq!(r.rows.len(), 2);
    assert_eq!(
        r.rows[0],
        vec![Value::Str("p1".into()), Value::Str("q1".into())]
    );
    assert_eq!(
        r.rows[1],
        vec![Value::Str("pnull".into()), Value::Str("qnull".into())]
    );
}

#[test]
fn hash_join_mixed_equi_and_residual() {
    // Equi conjunct indexes the candidates; the residual (x.v <> y.v)
    // still decides per candidate on the merged row.
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE x (id INT, v INT)");
    run(&mut db, "CREATE TABLE y (id INT, w INT)");
    run(&mut db, "INSERT INTO x VALUES (1, 10), (2, 20)");
    run(&mut db, "INSERT INTO y VALUES (1, 99), (1, 10), (2, 5)");
    let r = rows(
        &mut db,
        "SELECT x.id, y.w FROM x JOIN y ON x.id = y.id AND x.v <> y.w ORDER BY y.w",
    );
    // (1,10,1,10) is dropped by the residual; (1,10,1,99) and (2,20,2,5) stay.
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0], vec![Value::Int(2), Value::Int(5)]);
    assert_eq!(r.rows[1], vec![Value::Int(1), Value::Int(99)]);
}

#[test]
fn hash_join_int_float_keys_join() {
    // cmp_values compares Int(3) and Float(3.0) equal; the canonical key
    // (f64 bits) must keep that pair in the same bucket.
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE m (id INT, v TEXT)");
    run(&mut db, "CREATE TABLE n (id FLOAT, w TEXT)");
    run(&mut db, "INSERT INTO m VALUES (3, 'mint'), (4, 'mfour')");
    run(
        &mut db,
        "INSERT INTO n VALUES (3.0, 'nthree'), (4.5, 'nhalf')",
    );
    let r = rows(
        &mut db,
        "SELECT m.v, n.w FROM m JOIN n ON m.id = n.id ORDER BY m.v",
    );
    assert_eq!(r.rows.len(), 1);
    assert_eq!(
        r.rows[0],
        vec![Value::Str("mint".into()), Value::Str("nthree".into())]
    );
}

#[test]
fn hash_join_full_outer_and_unqualified_fallback() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE f (id INT, v TEXT)");
    run(&mut db, "CREATE TABLE g (id INT, w TEXT)");
    run(&mut db, "INSERT INTO f VALUES (1, 'f1'), (2, 'f2')");
    run(&mut db, "INSERT INTO g VALUES (2, 'g2'), (3, 'g3')");
    // FULL OUTER over an equi ON: matched row + one NULL-padded row per side.
    let r = rows(
        &mut db,
        "SELECT f.id, g.id FROM f FULL OUTER JOIN g ON f.id = g.id ORDER BY f.id, g.id",
    );
    assert_eq!(r.rows.len(), 3);
    // ORDER BY defaults NULLs first (smallest).
    assert_eq!(r.rows[0], vec![Value::Null, Value::Int(3)]);
    assert_eq!(r.rows[1], vec![Value::Int(1), Value::Null]);
    assert_eq!(r.rows[2], vec![Value::Int(2), Value::Int(2)]);
    // Unqualified ON references resolve via suffix matching and are
    // ambiguous — the nested-loop fallback must keep them correct.
    run(&mut db, "CREATE TABLE h (id INT, aid INT)");
    run(&mut db, "INSERT INTO h VALUES (9, 1), (9, 2)");
    let r = rows(
        &mut db,
        "SELECT f.id, h.aid FROM f JOIN h ON f.id = aid ORDER BY f.id",
    );
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0], vec![Value::Int(1), Value::Int(1)]);
    assert_eq!(r.rows[1], vec![Value::Int(2), Value::Int(2)]);
}

#[test]
fn hash_join_same_side_conjunct_stays_residual() {
    // `p.x = p.y` touches only the left side; it must stay a residual
    // predicate, never become an indexed pair whose second expression is
    // (wrongly) evaluated against the right row.
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE p (x INT, y INT, id INT)");
    run(&mut db, "CREATE TABLE q (id INT, v TEXT)");
    run(&mut db, "INSERT INTO p VALUES (5, 5, 1), (5, 6, 2)");
    run(&mut db, "INSERT INTO q VALUES (1, 'one')");
    let r = rows(
        &mut db,
        "SELECT q.v FROM p JOIN q ON p.x = p.y AND p.id = q.id",
    );
    // Only the row with x = y and matching id joins.
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0], vec![Value::Str("one".into())]);
}

#[test]
fn hash_join_missing_column_degrades_per_row() {
    // l2.foo does not exist as an exact key; on the merged row suffix
    // resolution binds it to r2.foo, so the nested loop finds the match.
    // The hash path must degrade the left row (not key it as NULL and
    // lose the pair).
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE l2 (id INT)");
    run(&mut db, "CREATE TABLE r2 (foo INT, bar INT)");
    run(&mut db, "INSERT INTO l2 VALUES (1)");
    run(&mut db, "INSERT INTO r2 VALUES (5, 5)");
    let r = rows(&mut db, "SELECT l2.id FROM l2 JOIN r2 ON l2.foo = r2.bar");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0], vec![Value::Int(1)]);
}

#[test]
fn hash_join_unqualified_conjunct_keeps_other_pairs() {
    // The unqualified `name` conjunct is residual, but the equi pair on
    // ids still forms a plan; results must match the nested loop.
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE x (id INT, name TEXT)");
    run(&mut db, "CREATE TABLE y (id INT, name TEXT)");
    run(&mut db, "INSERT INTO x VALUES (1, 'a')");
    run(&mut db, "INSERT INTO y VALUES (1, 'a'), (1, 'b')");
    let r = rows(
        &mut db,
        "SELECT y.name FROM x JOIN y ON x.id = y.id AND name = 'a'",
    );
    // Unqualified `name` resolves to the first suffix match on the
    // merged row (x.name = 'a', constant across candidates): both
    // id-matched pairs pass the residual.
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0], vec![Value::Str("a".into())]);
    assert_eq!(r.rows[1], vec![Value::Str("b".into())]);
}

#[test]
fn hash_join_document_key_values_degrade_to_full_probe() {
    // Documents have no canonical join key. A doc-valued right row must
    // stay probe-able by every left row (the `always` bucket) and a
    // doc-valued left row must probe every right row — dropping either
    // would lose matches the ON accepts (documents compare structurally,
    // and equal documents are `=`).
    let doc = Value::Object(Object::from([("a".to_string(), Value::Int(1))]));
    let left_row = |k: Value| Object::from([("l.k".to_string(), k)]);
    let right_row = |k: Value| Object::from([("k".to_string(), k)]);

    let on = parse_expr_text("l.k = r.k").unwrap();
    let deadline = StmtDeadline::default();
    let out = join_rows(
        vec![left_row(doc.clone()), left_row(Value::Int(9))],
        &[right_row(doc.clone()), right_row(Value::Int(9))],
        "r",
        Some(&on),
        false,
        false,
        &deadline,
        None,
        None,
    )
    .unwrap();
    // doc ↔ doc (always bucket + unindexed probe) and 9 ↔ 9 by key; the
    // always row comes first in right-row order.
    assert_eq!(out.len(), 2);
    assert_eq!(out[0]["l.k"], doc);
    assert_eq!(out[0]["r.k"], doc);
    assert_eq!(out[1]["l.k"], Value::Int(9));
}

#[test]
fn hash_join_beyond_f64_int_keys_do_not_false_match() {
    // 2^53 and 2^53+1 collapse to the same canonical key; the full ON
    // (exact i64 comparison) must reject the pair — the superset filter
    // may over-merge but never decides.
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE a (id INT)");
    run(&mut db, "CREATE TABLE b (id INT)");
    run(
        &mut db,
        "INSERT INTO a VALUES (9007199254740992), (9007199254740993)",
    );
    run(
        &mut db,
        "INSERT INTO b VALUES (9007199254740992), (9007199254740993)",
    );
    let r = rows(&mut db, "SELECT a.id, b.id FROM a JOIN b ON a.id = b.id");
    assert_eq!(r.rows.len(), 2);
}

#[test]
fn hash_join_deadline_fires() {
    // The hash path's build/probe/candidate loops are the preemption
    // points for an expired deadline, same contract as the nested loop.
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE a (id INT PRIMARY KEY)").unwrap();
    db.execute("CREATE TABLE b (id INT)").unwrap();
    // Enough rows to cross the deadline's 1024-call sampling batch; a
    // small join finishes before the first clock sample by design.
    let mut vals = String::from("INSERT INTO a VALUES (0)");
    for i in 1..2000 {
        vals.push_str(&format!(", ({i})"));
    }
    db.execute(&vals).unwrap();
    db.execute("INSERT INTO b VALUES (0)").unwrap();
    let expired = std::time::Instant::now() - std::time::Duration::from_secs(1);
    db.set_statement_deadline(Some(expired));
    let e = db
        .execute("SELECT COUNT(*) FROM a JOIN b ON a.id = b.id")
        .unwrap_err();
    assert!(
        e.to_string().contains("statement timeout"),
        "wrong error: {e}"
    );
    db.set_statement_deadline(None);
    let n = rows(&mut db, "SELECT COUNT(*) FROM a JOIN b ON a.id = b.id");
    assert_eq!(n.rows[0], vec![Value::Int(1)]);
}

#[test]
fn aggregates_without_group_by() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (v INT)");
    run(&mut db, "INSERT INTO t VALUES (2), (4), (6), (NULL)");
    let r = rows(
        &mut db,
        "SELECT COUNT(*), COUNT(v), SUM(v), AVG(v), MIN(v), MAX(v) FROM t",
    );
    assert_eq!(
        r.rows,
        vec![vec![
            Value::Int(4),
            Value::Int(3),
            Value::Int(12),
            Value::Float(4.0),
            Value::Int(2),
            Value::Int(6),
        ]]
    );
    // empty table
    run(&mut db, "CREATE TABLE e (v INT)");
    let r = rows(&mut db, "SELECT COUNT(*), SUM(v) FROM e");
    assert_eq!(r.rows, vec![vec![Value::Int(0), Value::Null]]);
}

#[test]
fn group_by_with_having() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE s (dept TEXT, pay INT)");
    run(
        &mut db,
        "INSERT INTO s VALUES ('eng', 100), ('eng', 120), ('ops', 80), ('ops', 90), ('hr', 50)",
    );
    let r = rows(
        &mut db,
        "SELECT dept, COUNT(*), SUM(pay) FROM s GROUP BY dept ORDER BY dept",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("eng".into()), Value::Int(2), Value::Int(220)],
            vec![Value::Str("hr".into()), Value::Int(1), Value::Int(50)],
            vec![Value::Str("ops".into()), Value::Int(2), Value::Int(170)],
        ]
    );
    let r = rows(
        &mut db,
        "SELECT dept, SUM(pay) FROM s GROUP BY dept HAVING SUM(pay) > 100 ORDER BY dept",
    );
    assert_eq!(r.rows.len(), 2);
    assert!(r.rows.iter().all(|row| row[1].as_i64().unwrap() > 100));
}

#[test]
fn in_list_filter() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT)");
    for i in 0..6 {
        run(&mut db, &format!("INSERT INTO t VALUES ({i})"));
    }
    let r = rows(
        &mut db,
        "SELECT id FROM t WHERE id IN (1, 3, 5) ORDER BY id",
    );
    assert_eq!(r.rows.len(), 3);
    let r = rows(
        &mut db,
        "SELECT id FROM t WHERE id NOT IN (1, 3, 5) ORDER BY id",
    );
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][0], Value::Int(0));
    assert_eq!(r.rows[2][0], Value::Int(4));
}

#[test]
fn union_and_union_all() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE a (v INT)");
    run(&mut db, "CREATE TABLE b (v INT)");
    run(&mut db, "INSERT INTO a VALUES (1), (2)");
    run(&mut db, "INSERT INTO b VALUES (2), (3)");
    let r = rows(&mut db, "SELECT v FROM a UNION SELECT v FROM b ORDER BY v");
    assert_eq!(r.rows.len(), 3);
    let r = rows(&mut db, "SELECT v FROM a UNION ALL SELECT v FROM b");
    assert_eq!(r.rows.len(), 4);
}

#[test]
fn non_grouped_column_rejected() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (a INT, b INT)");
    run(&mut db, "INSERT INTO t VALUES (1, 2)");
    let e = db
        .execute("SELECT a, COUNT(*) FROM t GROUP BY b")
        .unwrap_err();
    assert!(e.to_string().contains("GROUP BY"), "{e}");
}

#[test]
fn rollback_undoes_inserts_updates_deletes() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
    run(&mut db, "INSERT INTO t VALUES (1, 'a')");
    run(&mut db, "BEGIN");
    run(&mut db, "INSERT INTO t VALUES (2, 'b')");
    run(&mut db, "UPDATE t SET v = 'changed' WHERE id = 1");
    run(&mut db, "DELETE FROM t WHERE id = 1");
    assert_eq!(rows(&mut db, "SELECT id FROM t ORDER BY id").rows.len(), 1); // sees id=2
    run(&mut db, "ROLLBACK");
    let r = rows(&mut db, "SELECT id, v FROM t");
    assert_eq!(r.rows, vec![vec![Value::Int(1), Value::Str("a".into())]]);
}

#[test]
fn commit_persists_transaction_changes() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (a INT)");
    run(&mut db, "BEGIN");
    run(&mut db, "INSERT INTO t VALUES (42)");
    run(&mut db, "COMMIT");
    let r = rows(&mut db, "SELECT a FROM t");
    assert_eq!(r.rows, vec![vec![Value::Int(42)]]);
    // nested begin rejected
    run(&mut db, "BEGIN");
    assert!(db.execute("BEGIN").is_err());
    assert!(db.execute("ROLLBACK").is_ok());
    // rollback/commit without begin rejected
    assert!(db.execute("COMMIT").is_err());
    assert!(db.execute("ROLLBACK").is_err());
}

#[test]
fn rollback_restores_dropped_and_created_tables() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE keep (a INT)");
    run(&mut db, "BEGIN");
    run(&mut db, "DROP TABLE keep");
    run(&mut db, "CREATE TABLE fresh (b INT)");
    run(&mut db, "ROLLBACK");
    assert!(db.execute("SELECT * FROM fresh").is_err());
    assert!(db.execute("SELECT * FROM keep").is_ok());
}

#[test]
fn rollback_restores_constraints() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY)");
    run(&mut db, "INSERT INTO t VALUES (1)");
    run(&mut db, "BEGIN");
    run(&mut db, "DELETE FROM t");
    run(&mut db, "ROLLBACK");
    // PK still enforced after rollback
    assert!(db.execute("INSERT INTO t VALUES (1)").is_err());
}

#[test]
fn autoincrement_assigns_ids() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT AUTOINCREMENT PRIMARY KEY, v TEXT)",
    );
    let r = match run(
        &mut db,
        "INSERT INTO t (v) VALUES ('a'), ('b') RETURNING id, v",
    ) {
        ExecOutcome::Rows(r) => r,
        o => panic!("{o:?}"),
    };
    assert_eq!(r.columns, vec!["id", "v"]);
    assert_eq!(r.rows[0][0], Value::Int(1));
    assert_eq!(r.rows[1][0], Value::Int(2));
    // explicit id still works and bumps the counter
    run(&mut db, "INSERT INTO t (id, v) VALUES (10, 'ten')");
    let r = rows(&mut db, "INSERT INTO t (v) VALUES ('c') RETURNING id");
    assert_eq!(r.rows[0][0], Value::Int(11));
}

#[test]
fn last_insert_id_tracks_the_assigned_autoincrement_value() {
    let mut db = Database::in_memory().unwrap();
    // Not an insert: None.
    assert_eq!(db.last_insert_id(), None);
    run(&mut db, "CREATE TABLE t (id INT AUTOINCREMENT, v TEXT)");
    run(&mut db, "INSERT INTO t (v) VALUES ('a'), ('b')");
    // Multi-row insert: the max assigned id, no SELECT MAX round-trip.
    assert_eq!(db.last_insert_id(), Some(2));
    // Explicit id counts too.
    run(&mut db, "INSERT INTO t (id, v) VALUES (10, 'ten')");
    assert_eq!(db.last_insert_id(), Some(10));
    // Every statement scope resets it, including reads and failures.
    run(&mut db, "SELECT 1");
    assert_eq!(db.last_insert_id(), None);
    assert!(db.execute("INSERT INTO missing VALUES (1)").is_err());
    assert_eq!(db.last_insert_id(), None);
    // Table without AUTOINCREMENT: stays None.
    run(&mut db, "CREATE TABLE u (id INT, v TEXT)");
    run(&mut db, "INSERT INTO u VALUES (7, 'x')");
    assert_eq!(db.last_insert_id(), None);
}

#[test]
fn max_autoinc_matches_sql_max_and_survives_invalidation() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT AUTOINCREMENT, v TEXT)");
    // Empty table: 0 (matching the SQL aggregate on no rows).
    assert_eq!(db.max_autoinc("t"), Some(0));
    run(&mut db, "INSERT INTO t (v) VALUES ('a'), ('b')");
    run(&mut db, "INSERT INTO t (id, v) VALUES (10, 'ten')");
    assert_eq!(db.max_autoinc("t"), Some(10));
    // Missing table / no AUTOINCREMENT column: None (fallback signal).
    assert_eq!(db.max_autoinc("ghost"), None);
    run(&mut db, "CREATE TABLE u (id INT)");
    assert_eq!(db.max_autoinc("u"), None);
    // DELETE invalidates the counter cache; the re-derived max still
    // equals the SQL aggregate.
    run(&mut db, "DELETE FROM t WHERE id = 10");
    let sql_max = match run(&mut db, "SELECT MAX(id) FROM t") {
        ExecOutcome::Rows(r) => r.rows[0][0].clone(),
        o => panic!("{o:?}"),
    };
    assert_eq!(db.max_autoinc("t"), sql_max.as_i64());
}

#[test]
fn guid_autoincrement_generates_ordered_uuidv7() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id GUID PRIMARY KEY AUTOINCREMENT, v TEXT)",
    );
    for i in 0..5 {
        run(&mut db, &format!("INSERT INTO t (v) VALUES ('x{i}')"));
    }
    let r = rows(&mut db, "SELECT id FROM t ORDER BY id");
    let ids: Vec<String> = r
        .rows
        .iter()
        .map(|row| row[0].as_str().unwrap().to_string())
        .collect();
    assert_eq!(ids.len(), 5);
    let set: std::collections::BTreeSet<&String> = ids.iter().collect();
    assert_eq!(set.len(), ids.len(), "guids must be unique");
    for id in &ids {
        assert_eq!(id.len(), 36);
        assert_eq!(&id[14..15], "7", "version nibble");
        assert!(
            matches!(&id[19..20], "8" | "9" | "a" | "b"),
            "variant bits in {id}"
        );
    }
    // Canonical lowercase string order == generation order (time-ordered).
    let mut sorted = ids.clone();
    sorted.sort();
    assert_eq!(sorted, ids);
}

#[test]
fn guid_explicit_and_null_values() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id GUID PRIMARY KEY AUTOINCREMENT, v TEXT)",
    );
    // Explicit value passes through untouched.
    run(
        &mut db,
        "INSERT INTO t (id, v) VALUES ('00000000-0000-7000-8000-000000000001', 'e')",
    );
    // Explicit NULL is filled like an omitted column.
    run(&mut db, "INSERT INTO t (id, v) VALUES (NULL, 'n')");
    let r = rows(&mut db, "SELECT id FROM t ORDER BY id");
    assert_eq!(r.rows.len(), 2);
    assert_eq!(
        r.rows[0][0],
        Value::Str("00000000-0000-7000-8000-000000000001".into())
    );
    let filled = r.rows[1][0].as_str().unwrap();
    assert_eq!(filled.len(), 36);
    assert_eq!(&filled[14..15], "7");
    // Positional insert without a column list fills NULL the same way.
    run(&mut db, "INSERT INTO t VALUES (NULL, 'pos')");
    let r = rows(&mut db, "SELECT COUNT(*) AS n FROM t");
    assert_eq!(r.rows[0][0], Value::Int(3));
}

#[test]
fn guid_resolved_sql_replays_identically() {
    let mut a = Database::in_memory().unwrap();
    run(
        &mut a,
        "CREATE TABLE t (id GUID PRIMARY KEY AUTOINCREMENT, v TEXT)",
    );
    a.execute("INSERT INTO t (v) VALUES ('it''s quoted'), ('b')")
        .unwrap();
    let resolved = a.take_resolved_sql().expect("resolved rewrite");
    assert!(resolved.starts_with("INSERT INTO "));
    assert!(resolved.contains("'it''s quoted'"), "in {resolved}");
    // Replication replay: a second node applies the rewrite verbatim.
    let mut b = Database::in_memory().unwrap();
    run(
        &mut b,
        "CREATE TABLE t (id GUID PRIMARY KEY AUTOINCREMENT, v TEXT)",
    );
    b.execute(&resolved).unwrap();
    assert!(b.take_resolved_sql().is_none(), "no re-generation");
    let ra = rows(&mut a, "SELECT id, v FROM t ORDER BY v");
    let rb = rows(&mut b, "SELECT id, v FROM t ORDER BY v");
    assert_eq!(ra.rows, rb.rows);
    // Conflict policy survives the rewrite.
    a.execute("INSERT OR IGNORE INTO t (v) VALUES ('c')")
        .unwrap();
    let resolved = a.take_resolved_sql().unwrap();
    assert!(
        resolved.starts_with("INSERT OR IGNORE INTO "),
        "in {resolved}"
    );
    // Statements without generation produce no rewrite.
    a.execute("INSERT INTO t (id, v) VALUES ('00000000-0000-7000-8000-000000000009', 'x')")
        .unwrap();
    assert!(a.take_resolved_sql().is_none());
}

#[test]
fn guid_or_replace_resolved_preserves_policy() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id GUID PRIMARY KEY AUTOINCREMENT, v TEXT UNIQUE)",
    );
    run(
        &mut db,
        "INSERT INTO t (id, v) VALUES ('00000000-0000-7000-8000-000000000001', 'dup')",
    );
    db.execute("INSERT OR REPLACE INTO t (v) VALUES ('dup')")
        .unwrap();
    let resolved = db.take_resolved_sql().expect("resolved rewrite");
    assert!(
        resolved.starts_with("INSERT OR REPLACE INTO "),
        "in {resolved}"
    );
    // The peer replay honors the same policy: the old row is displaced,
    // the generated guid from the origin lands verbatim.
    let mut peer = Database::in_memory().unwrap();
    run(
        &mut peer,
        "CREATE TABLE t (id GUID PRIMARY KEY AUTOINCREMENT, v TEXT UNIQUE)",
    );
    run(
        &mut peer,
        "INSERT INTO t (id, v) VALUES ('00000000-0000-7000-8000-000000000001', 'dup')",
    );
    peer.execute(&resolved).unwrap();
    let r = rows(&mut peer, "SELECT id, v FROM t");
    assert_eq!(r.rows.len(), 1);
    assert_ne!(
        r.rows[0][0],
        Value::Str("00000000-0000-7000-8000-000000000001".into())
    );
    assert_eq!(&r.rows[0][0].as_str().unwrap()[14..15], "7");
}

#[test]
fn guid_mixed_explicit_and_null_rows_resolve_all() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id GUID PRIMARY KEY AUTOINCREMENT, v TEXT)",
    );
    db.execute(
        "INSERT INTO t (id, v) VALUES \
             ('00000000-0000-7000-8000-000000000005', 'e'), (NULL, 'g')",
    )
    .unwrap();
    let resolved = db.take_resolved_sql().expect("resolved rewrite");
    // Both rows carry explicit ids in the rewrite — the NULL was filled.
    assert!(
        resolved.contains("'00000000-0000-7000-8000-000000000005'"),
        "in {resolved}"
    );
    let generated = {
        let r = rows(&mut db, "SELECT id FROM t WHERE v = 'g'");
        r.rows[0][0].as_str().unwrap().to_string()
    };
    assert!(
        resolved.contains(&format!("'{generated}'")),
        "generated id missing from rewrite: {resolved}"
    );
    // Sorting: explicit id sorts before the generated UUIDv7 (time
    // prefix of the all-zero guid is smaller than any real timestamp).
    let r = rows(&mut db, "SELECT v FROM t ORDER BY id");
    assert_eq!(
        r.rows,
        vec![vec![Value::Str("e".into())], vec![Value::Str("g".into())]]
    );
}

#[test]
fn guid_autoguid_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("guid.db");
    let mut db = Database::open(&path).unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id GUID PRIMARY KEY AUTOINCREMENT, v TEXT)",
    );
    run(&mut db, "INSERT INTO t (v) VALUES ('a')");
    drop(db);
    let mut db = Database::open(&path).unwrap();
    run(&mut db, "INSERT INTO t (v) VALUES ('b')");
    let r = rows(&mut db, "SELECT id FROM t");
    assert_eq!(r.rows.len(), 2);
    for row in &r.rows {
        let id = row[0].as_str().unwrap();
        assert_eq!(&id[14..15], "7", "still generating after reopen: {id}");
    }
}

#[test]
fn guid_insert_select_rejected() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id GUID PRIMARY KEY AUTOINCREMENT, v TEXT)",
    );
    run(&mut db, "CREATE TABLE src (v TEXT)");
    run(&mut db, "INSERT INTO src VALUES ('x')");
    let e = db
        .execute("INSERT INTO t (v) SELECT v FROM src")
        .unwrap_err()
        .to_string();
    assert!(e.contains("INSERT ... VALUES"), "unexpected error: {e}");
}

#[test]
fn guid_type_keyword_variants() {
    for ty in ["GUID", "UUID", "UNIQUEIDENTIFIER", "UUIDV7"] {
        let mut db = Database::in_memory().unwrap();
        run(
            &mut db,
            &format!("CREATE TABLE t (id {ty} PRIMARY KEY AUTOINCREMENT, v TEXT)"),
        );
        run(&mut db, "INSERT INTO t (v) VALUES ('a')");
        let r = rows(&mut db, "SELECT id FROM t");
        assert_eq!(&r.rows[0][0].as_str().unwrap()[14..15], "7", "type {ty}");
    }
    // AUTO_INCREMENT is accepted like AUTOINCREMENT (existing alias).
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id GUID PRIMARY KEY AUTO_INCREMENT, v TEXT)",
    );
    run(&mut db, "INSERT INTO t (v) VALUES ('a')");
    let r = rows(&mut db, "SELECT id FROM t");
    assert_eq!(&r.rows[0][0].as_str().unwrap()[14..15], "7");
}

#[test]
fn guid_rename_and_drop_column() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id GUID AUTOINCREMENT, v TEXT)");
    run(&mut db, "INSERT INTO t (v) VALUES ('a')");
    run(&mut db, "ALTER TABLE t RENAME COLUMN id TO gid");
    run(&mut db, "INSERT INTO t (v) VALUES ('b')");
    let r = rows(&mut db, "SELECT gid FROM t ORDER BY gid");
    assert_eq!(r.rows.len(), 2);
    assert_eq!(&r.rows[1][0].as_str().unwrap()[14..15], "7");
    // Dropping the column stops generation (mirrors AUTOINCREMENT).
    run(&mut db, "ALTER TABLE t DROP COLUMN gid");
    run(&mut db, "INSERT INTO t (v) VALUES ('c')");
    let r = rows(&mut db, "SELECT v, gid FROM t ORDER BY v");
    assert_eq!(r.rows[2], vec![Value::Str("c".into()), Value::Null]);
}

#[test]
fn returning_on_update_and_delete() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT, v TEXT)");
    run(&mut db, "INSERT INTO t VALUES (1, 'a'), (2, 'b')");
    let r = match run(&mut db, "UPDATE t SET v = 'z' WHERE id = 2 RETURNING id, v") {
        ExecOutcome::Rows(r) => r,
        o => panic!("{o:?}"),
    };
    assert_eq!(r.rows, vec![vec![Value::Int(2), Value::Str("z".into())]]);
    let r = match run(&mut db, "DELETE FROM t WHERE id = 1 RETURNING id") {
        ExecOutcome::Rows(r) => r,
        o => panic!("{o:?}"),
    };
    assert_eq!(r.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn returning_star_projects_changed_documents() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT, v TEXT)");
    let r = rows(&mut db, "INSERT INTO t VALUES (1, 'a') RETURNING *");
    assert_eq!(r.columns, vec!["id".to_string(), "v".to_string()]);
    assert_eq!(r.rows, vec![vec![Value::Int(1), Value::Str("a".into())]]);
    // Schemaless docs: the changed set's field union picks the columns;
    // `RETURNING *, expr` appends the explicit projection after the star.
    run(&mut db, "INSERT INTO t (id, extra) VALUES (2, 'x')");
    let r = rows(
        &mut db,
        "UPDATE t SET v = 'b' WHERE id = 1 RETURNING *, v AS twice",
    );
    assert_eq!(
        r.columns,
        vec!["id".to_string(), "v".to_string(), "twice".to_string()]
    );
    assert_eq!(
        r.rows,
        vec![vec![
            Value::Int(1),
            Value::Str("b".into()),
            Value::Str("b".into())
        ]]
    );
    let r = rows(&mut db, "DELETE FROM t WHERE id = 2 RETURNING *");
    assert_eq!(r.columns, vec!["extra".to_string(), "id".to_string()]);
    assert_eq!(r.rows, vec![vec![Value::Str("x".into()), Value::Int(2)]]);
}

#[test]
fn information_schema_lists_tables_and_columns() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE alpha (x INT NOT NULL, y TEXT)");
    run(&mut db, "CREATE TABLE beta (z INT)");
    let r = rows(
        &mut db,
        "SELECT table_name FROM information_schema.tables ORDER BY table_name",
    );
    let names: Vec<String> = r
        .rows
        .iter()
        .map(|row| row[0].as_str().unwrap().into())
        .collect();
    assert!(names.contains(&"alpha".to_string()));
    assert!(names.contains(&"beta".to_string()));
    let r = rows(&mut db, "SELECT column_name, is_nullable FROM information_schema.columns WHERE table_name = 'alpha' ORDER BY column_name");
    assert_eq!(
        r.rows[0],
        vec![Value::Str("x".into()), Value::Str("NO".into())]
    );
    assert_eq!(
        r.rows[1],
        vec![Value::Str("y".into()), Value::Str("YES".into())]
    );
}

#[test]
fn catalog_reports_constraints_and_indexes() {
    let mut db = Database::in_memory().unwrap();
    run(
            &mut db,
            "CREATE TABLE users (id INT PRIMARY KEY NOT NULL AUTOINCREMENT, email TEXT UNIQUE, name TEXT NOT NULL)",
        );
    run(&mut db, "CREATE INDEX idx_users_name ON users (name)");
    let tables = db.catalog();
    assert_eq!(tables.len(), 1);
    let t = &tables[0];
    assert_eq!(t.name, "users");
    assert_eq!(t.keys, vec!["id".to_string(), "email".to_string()]);
    // Constraint indexes come first with reserved names, then user ones.
    assert_eq!(
        t.indexes,
        vec![
            "sqlite_autoindex_users_1".to_string(),
            "sqlite_autoindex_users_2".to_string(),
            "idx_users_name".to_string(),
        ]
    );
    assert_eq!(
        t.index_defs,
        vec![
            IndexInfo {
                name: "sqlite_autoindex_users_1".into(),
                column: "id".into(),
                columns: vec!["id".into()],
                unique: true,
                auto: true,
                pred: None,
            },
            IndexInfo {
                name: "sqlite_autoindex_users_2".into(),
                column: "email".into(),
                columns: vec!["email".into()],
                unique: true,
                auto: true,
                pred: None,
            },
            IndexInfo {
                name: "idx_users_name".into(),
                column: "name".into(),
                columns: vec!["name".into()],
                unique: false,
                auto: false,
                pred: None,
            },
        ]
    );
    let id = t.columns.iter().find(|c| c.name == "id").unwrap();
    assert!(id.primary_key && id.autoinc && !id.nullable);
    let email = t.columns.iter().find(|c| c.name == "email").unwrap();
    assert!(email.unique && email.nullable);
    let name = t.columns.iter().find(|c| c.name == "name").unwrap();
    assert!(!name.nullable);
    assert_eq!(db.page_size(), PAGE_SIZE);
    assert!(db.num_pages() >= 2);
}

#[test]
fn catalog_reports_index_defs() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (a INT, b TEXT)");
    run(&mut db, "CREATE UNIQUE INDEX ux_a ON t (a)");
    run(&mut db, "CREATE INDEX ix_b ON t (b)");
    let t = &db.catalog()[0];
    // UNIQUE via CREATE INDEX is a named definition, not a constraint
    // autoindex: t declares no PK/UNIQUE, so no sqlite_autoindex_* rows.
    assert_eq!(
        t.index_defs,
        vec![
            IndexInfo {
                name: "ux_a".into(),
                column: "a".into(),
                columns: vec!["a".into()],
                unique: true,
                auto: false,
                pred: None,
            },
            IndexInfo {
                name: "ix_b".into(),
                column: "b".into(),
                columns: vec!["b".into()],
                unique: false,
                auto: false,
                pred: None,
            },
        ]
    );
    // DROP INDEX keeps the definitions in sync.
    run(&mut db, "DROP INDEX ux_a");
    let t = &db.catalog()[0];
    assert_eq!(t.indexes, vec!["ix_b".to_string()]);
    assert_eq!(t.index_defs.len(), 1);
    assert_eq!(t.index_defs[0].column, "b");
}

#[test]
fn execute_batch_runs_all_and_stops_on_error() {
    let mut db = Database::in_memory().unwrap();
    let b = db.execute_batch(
            "CREATE TABLE b (id INT PRIMARY KEY, v TEXT); INSERT INTO b VALUES (1, 'a'), (2, 'b'); SELECT id FROM b ORDER BY id",
        );
    assert!(b.error.is_none());
    assert_eq!(b.outcomes.len(), 3);
    assert!(matches!(b.outcomes[0], ExecOutcome::Affected(0)));

    // Error mid-batch keeps the executed prefix and reports the index.
    let b = db.execute_batch(
        "INSERT INTO b VALUES (3, 'c'); SELECT * FROM missing; INSERT INTO b VALUES (4, 'd')",
    );
    let e = b.error.unwrap();
    assert_eq!(e.statement, 1);
    assert_eq!(b.outcomes.len(), 1);
    assert!(rows(&mut db, "SELECT id FROM b WHERE id = 4")
        .rows
        .is_empty());
    assert_eq!(rows(&mut db, "SELECT id FROM b WHERE id = 3").rows.len(), 1);
}

#[test]
fn parse_check_validates_without_executing() {
    assert!(Database::parse_check("SELECT 1").is_ok());
    assert!(Database::parse_check("SELEC nope").is_err());
    assert!(Database::parse_check("   ").is_err());
    // A parseable statement must not execute.
    let mut db = Database::in_memory().unwrap();
    Database::parse_check("CREATE TABLE p (x INT)").unwrap();
    assert!(
        rows(&mut db, "SELECT table_name FROM information_schema.tables")
            .rows
            .is_empty()
    );
}

#[test]
fn catalog_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cat.db");
    {
        let mut db = Database::open(&path).unwrap();
        run(&mut db, "CREATE TABLE keep (a INT)");
        run(&mut db, "INSERT INTO keep (a) VALUES (42)");
    }
    let mut db = Database::open(&path).unwrap();
    let r = rows(&mut db, "SELECT a FROM keep");
    assert_eq!(r.rows, vec![vec![Value::Int(42)]]);
}

#[test]
fn autoinc_column_survives_reopen() {
    // Regression: the catalog used to drop the AUTOINCREMENT flag on
    // save, so post-restart inserts left the id column NULL.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("autoinc.db");
    {
        let mut db = Database::open(&path).unwrap();
        run(
            &mut db,
            "CREATE TABLE t (id INT PRIMARY KEY AUTOINCREMENT, v TEXT)",
        );
        run(&mut db, "INSERT INTO t (v) VALUES ('before')");
        let r = rows(&mut db, "SELECT id FROM t");
        assert_eq!(r.rows, vec![vec![Value::Int(1)]]);
    }
    let mut db = Database::open(&path).unwrap();
    run(&mut db, "INSERT INTO t (v) VALUES ('after')");
    let r = rows(&mut db, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(1), Value::Str("before".into())],
            vec![Value::Int(2), Value::Str("after".into())],
        ]
    );
}

#[test]
fn catalog_overflows_across_pages_and_survives_reopen() {
    // The old single-page catalog died at ~4 KB of metadata (a few
    // hundred table metas or page ids) and failed every later write.
    // Metadata now paginates across an overflow chain.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("overflow.db");
    {
        let mut db = Database::open(&path).unwrap();
        for i in 0..120 {
            run(
                &mut db,
                &format!("CREATE TABLE t{i} (id INT PRIMARY KEY, v TEXT)"),
            );
            run(&mut db, &format!("INSERT INTO t{i} VALUES (1, 'row{i}')"));
        }
    }
    let mut db = Database::open(&path).unwrap();
    // Page 1 carries the chain magic; the chain actually spilled.
    let page1 = db.pager.read_page(CATALOG_PAGE).unwrap().to_vec();
    assert!(page1.starts_with(CATALOG_MAGIC));
    assert_ne!(
        u32::from_le_bytes(page1[12..16].try_into().unwrap()),
        0,
        "expected overflow pages"
    );
    assert_eq!(db.catalog().len(), 120);
    let r = rows(&mut db, "SELECT v FROM t77");
    assert_eq!(r.rows, vec![vec![Value::Str("row77".into())]]);
    // Writes keep working past the old wall.
    run(&mut db, "INSERT INTO t0 VALUES (2, 'more')");
}

#[test]
fn catalog_chain_shrinks_after_drops_and_reopens() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shrink.db");
    {
        let mut db = Database::open(&path).unwrap();
        for i in 0..120 {
            run(&mut db, &format!("CREATE TABLE t{i} (id INT)"));
        }
    }
    let mut db = Database::open(&path).unwrap();
    for i in 0..110 {
        run(&mut db, &format!("DROP TABLE t{i}"));
    }
    let chain_after = db.catalog_chain().unwrap();
    drop(db);
    let mut db = Database::open(&path).unwrap();
    assert_eq!(db.catalog().len(), 10);
    // The truncated chain no longer references dropped overflow pages.
    assert_eq!(chain_after.len(), 1, "10 tables should fit one page");
    let page1 = db.pager.read_page(CATALOG_PAGE).unwrap().to_vec();
    assert_eq!(u32::from_le_bytes(page1[12..16].try_into().unwrap()), 0);
    run(&mut db, "INSERT INTO t115 VALUES (9)");
}

#[test]
fn legacy_single_page_catalog_still_loads_and_migrates() {
    // A volume written by the pre-chain format (raw encoded catalog on
    // page 1) must keep loading, and migrate to the chain format on its
    // first catalog save.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.db");
    let legacy: Object = [(
        "tables".to_string(),
        Value::Object(
            [(
                "legacy".to_string(),
                Value::Object(
                    [
                        (
                            "columns".to_string(),
                            Value::Array(vec![Value::Str("id".to_string())]),
                        ),
                        ("pages".to_string(), Value::Array(vec![])),
                        ("primary_key".to_string(), Value::Str("id".to_string())),
                    ]
                    .into(),
                ),
            )]
            .into(),
        ),
    )]
    .into();
    {
        let pager = crate::pager::Pager::open(&path).unwrap();
        let mut tx = pager.begin_tx();
        while pager.num_pages() <= CATALOG_PAGE {
            pager.allocate_page(&mut tx).unwrap();
        }
        let bytes = encode::encode_to_vec(&Value::Object(legacy)).unwrap();
        let mut page = vec![0u8; PAGE_SIZE];
        page[..bytes.len()].copy_from_slice(&bytes);
        pager.write_page(&mut tx, CATALOG_PAGE, 0, &page).unwrap();
        pager.commit_tx(tx).unwrap();
    }
    let mut db = Database::open(&path).unwrap();
    let r = rows(&mut db, "SELECT id FROM legacy");
    assert!(r.rows.is_empty());
    // Any catalog save rewrites page 1 in the chain format.
    run(&mut db, "INSERT INTO legacy VALUES (5)");
    let page1 = db.pager.read_page(CATALOG_PAGE).unwrap().to_vec();
    assert!(page1.starts_with(CATALOG_MAGIC));
    drop(db);
    let mut db = Database::open(&path).unwrap();
    let r = rows(&mut db, "SELECT id FROM legacy");
    assert_eq!(r.rows, vec![vec![Value::Int(5)]]);
}

#[test]
fn large_table_spans_pages() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE big (id INT, payload TEXT)");
    for i in 0..200 {
        run(
            &mut db,
            &format!(
                "INSERT INTO big (id, payload) VALUES ({i}, '{}')",
                "y".repeat(60)
            ),
        );
    }
    let r = rows(&mut db, "SELECT id FROM big ORDER BY id LIMIT 2");
    assert_eq!(r.rows, vec![vec![Value::Int(0)], vec![Value::Int(1)]]);
    let r = rows(&mut db, "SELECT id FROM big");
    assert_eq!(r.rows.len(), 200);
}

// ---- JOIN family -------------------------------------------------

#[test]
fn right_join_keeps_unmatched_right_rows() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE a (id INT)");
    run(&mut db, "CREATE TABLE b (id INT, aid INT)");
    run(&mut db, "INSERT INTO a VALUES (1), (2)");
    run(&mut db, "INSERT INTO b VALUES (7, 1), (8, 9)");
    let r = rows(
        &mut db,
        "SELECT a.id, b.id FROM a RIGHT JOIN b ON a.id = b.aid ORDER BY b.id",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(1), Value::Int(7)],
            vec![Value::Null, Value::Int(8)],
        ]
    );
    // RIGHT OUTER JOIN is the same operator spelled out.
    let r = rows(
        &mut db,
        "SELECT a.id, b.id FROM a RIGHT OUTER JOIN b ON a.id = b.aid ORDER BY b.id",
    );
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[1], vec![Value::Null, Value::Int(8)]);
}

#[test]
fn join_using_constraint() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE u (id INT, name TEXT)");
    run(&mut db, "CREATE TABLE v (id INT, val TEXT)");
    run(&mut db, "INSERT INTO u VALUES (1, 'ann'), (2, 'bob')");
    run(&mut db, "INSERT INTO v VALUES (1, 'x'), (3, 'z')");
    // USING (id) == ON u.id = v.id
    let r = rows(&mut db, "SELECT u.id, v.val FROM u JOIN v USING (id)");
    assert_eq!(r.rows, vec![vec![Value::Int(1), Value::Str("x".into())]]);
}

#[test]
fn comma_separated_from_is_cross_join() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE x (a INT)");
    run(&mut db, "CREATE TABLE y (b INT)");
    run(&mut db, "INSERT INTO x VALUES (1), (2)");
    run(&mut db, "INSERT INTO y VALUES (3), (4), (5)");
    let r = rows(&mut db, "SELECT x.a, y.b FROM x, y");
    assert_eq!(r.rows.len(), 6); // 2 * 3 cartesian
}

#[test]
fn three_table_join_chain() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE users (id INT, name TEXT)");
    run(&mut db, "CREATE TABLE orders (oid INT, uid INT)");
    run(&mut db, "CREATE TABLE items (iid INT, oid INT, item TEXT)");
    run(&mut db, "INSERT INTO users VALUES (1, 'ann'), (2, 'bob')");
    run(
        &mut db,
        "INSERT INTO orders VALUES (10, 1), (11, 2), (12, 99)",
    );
    run(
            &mut db,
            "INSERT INTO items VALUES (100, 10, 'book'), (101, 10, 'pen'), (102, 11, 'cap'), (103, 77, 'ghost')",
        );
    let r = rows(
            &mut db,
            "SELECT u.name, i.item FROM users u JOIN orders o ON u.id = o.uid JOIN items i ON o.oid = i.oid ORDER BY i.item",
        );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("ann".into()), Value::Str("book".into())],
            vec![Value::Str("bob".into()), Value::Str("cap".into())],
            vec![Value::Str("ann".into()), Value::Str("pen".into())],
        ]
    );
}

#[test]
fn table_aliases_in_join() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE usr (id INT, name TEXT)");
    run(&mut db, "CREATE TABLE ord (id INT, owner INT)");
    run(&mut db, "INSERT INTO usr VALUES (1, 'ann')");
    run(&mut db, "INSERT INTO ord VALUES (50, 1)");
    let r = rows(
        &mut db,
        "SELECT usr.name FROM usr AS w JOIN ord AS d ON w.id = d.owner",
    );
    assert_eq!(r.rows, vec![vec![Value::Str("ann".into())]]);
}

#[test]
fn self_join_parent_child() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT, pid INT, name TEXT)");
    run(
        &mut db,
        "INSERT INTO t VALUES (1, 0, 'root'), (2, 1, 'child-a'), (3, 1, 'child-b')",
    );
    let r = rows(
            &mut db,
            "SELECT c.name AS child, p.name AS parent FROM t AS p JOIN t AS c ON c.pid = p.id ORDER BY child",
        );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("child-a".into()), Value::Str("root".into())],
            vec![Value::Str("child-b".into()), Value::Str("root".into())],
        ]
    );
}

#[test]
fn left_join_fan_out_multiple_matches() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE a (id INT)");
    run(&mut db, "CREATE TABLE b (bid INT, aid INT)");
    run(&mut db, "INSERT INTO a VALUES (1)");
    run(&mut db, "INSERT INTO b VALUES (10, 1), (11, 1)");
    let r = rows(
        &mut db,
        "SELECT a.id, b.bid FROM a LEFT JOIN b ON b.aid = a.id ORDER BY b.bid",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(1), Value::Int(11)],
        ]
    );
}

#[test]
fn full_outer_join_keeps_both_sides() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE a (id INT)");
    run(&mut db, "CREATE TABLE b (id INT, aid INT)");
    run(&mut db, "INSERT INTO a VALUES (1), (2)");
    run(&mut db, "INSERT INTO b VALUES (7, 1), (8, 3)");
    let r = rows(
        &mut db,
        "SELECT a.id, b.id FROM a FULL OUTER JOIN b ON a.id = b.aid ORDER BY a.id NULLS LAST",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(1), Value::Int(7)],
            vec![Value::Int(2), Value::Null],
            vec![Value::Null, Value::Int(8)],
        ]
    );
}

#[test]
fn join_with_where_and_group_by() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE users (id INT, name TEXT)");
    run(&mut db, "CREATE TABLE orders (oid INT, uid INT)");
    run(
        &mut db,
        "INSERT INTO users VALUES (1, 'ann'), (2, 'bob'), (3, 'zed')",
    );
    run(
        &mut db,
        "INSERT INTO orders VALUES (10, 1), (11, 1), (12, 2)",
    );
    // LEFT JOIN + GROUP BY: users without orders count as zero.
    let r = rows(
            &mut db,
            "SELECT u.name, COUNT(o.oid) AS n FROM users u LEFT JOIN orders o ON u.id = o.uid GROUP BY u.name ORDER BY u.name",
        );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("ann".into()), Value::Int(2)],
            vec![Value::Str("bob".into()), Value::Int(1)],
            vec![Value::Str("zed".into()), Value::Int(0)],
        ]
    );
    // INNER JOIN + WHERE before grouping.
    let r = rows(
            &mut db,
            "SELECT u.name, COUNT(*) AS n FROM users u JOIN orders o ON u.id = o.uid WHERE o.oid > 10 GROUP BY u.name ORDER BY u.name",
        );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("ann".into()), Value::Int(1)],
            vec![Value::Str("bob".into()), Value::Int(1)],
        ]
    );
}

// ---- GROUP BY / aggregates --------------------------------------

#[test]
fn group_by_multiple_columns() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE s (dept TEXT, role TEXT, pay INT)");
    run(
        &mut db,
        "INSERT INTO s VALUES ('eng', 'dev', 100), ('eng', 'ops', 80), ('ops', 'dev', 90)",
    );
    let r = rows(
        &mut db,
        "SELECT dept, role, SUM(pay) FROM s GROUP BY dept, role ORDER BY dept, role",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![
                Value::Str("eng".into()),
                Value::Str("dev".into()),
                Value::Int(100)
            ],
            vec![
                Value::Str("eng".into()),
                Value::Str("ops".into()),
                Value::Int(80)
            ],
            vec![
                Value::Str("ops".into()),
                Value::Str("dev".into()),
                Value::Int(90)
            ],
        ]
    );
}

#[test]
fn group_by_expression_bucket() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (n INT)");
    let vals: Vec<String> = (0..25).map(|i| format!("({i})")).collect();
    run(
        &mut db,
        &format!("INSERT INTO t VALUES {}", vals.join(", ")),
    );
    // n / 10 buckets 0..24 into three groups of 10/10/5.
    let r = rows(
        &mut db,
        "SELECT n / 10 AS bucket, COUNT(*) AS c FROM t GROUP BY n / 10 ORDER BY bucket",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(0), Value::Int(10)],
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(5)],
        ]
    );
}

#[test]
fn having_with_aggregate_not_in_projection() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE s (dept TEXT, pay INT)");
    run(
        &mut db,
        "INSERT INTO s VALUES ('eng', 1), ('eng', 2), ('ops', 3)",
    );
    let r = rows(
        &mut db,
        "SELECT dept FROM s GROUP BY dept HAVING COUNT(*) > 1",
    );
    assert_eq!(r.rows, vec![vec![Value::Str("eng".into())]]);
    // A bare column that is neither grouped nor projected must error
    // instead of silently reading NULL (which dropped every group).
    let e = db
        .execute("SELECT dept, COUNT(*) FROM s GROUP BY dept HAVING pay > 0")
        .unwrap_err();
    assert!(e.to_string().contains("must appear in GROUP BY"), "{e}");
    // Aliases in HAVING still resolve through the output row.
    let r = rows(
        &mut db,
        "SELECT dept AS d, COUNT(*) AS c FROM s GROUP BY dept HAVING c > 1",
    );
    assert_eq!(r.rows, vec![vec![Value::Str("eng".into()), Value::Int(2)]]);
}

#[test]
fn having_combines_group_key_and_aggregate() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE s (dept TEXT, pay INT)");
    run(
        &mut db,
        "INSERT INTO s VALUES ('eng', 100), ('eng', 20), ('ops', 90), ('hr', 500)",
    );
    let r = rows(
        &mut db,
        "SELECT dept FROM s GROUP BY dept HAVING dept != 'hr' AND SUM(pay) > 60 ORDER BY dept",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("eng".into())],
            vec![Value::Str("ops".into())]
        ]
    );
}

#[test]
fn group_by_on_empty_input_yields_no_rows() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE s (dept TEXT, pay INT)");
    let r = rows(&mut db, "SELECT dept, COUNT(*) FROM s GROUP BY dept");
    assert!(r.rows.is_empty());
}

#[test]
fn aggregates_min_max_over_strings() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (s TEXT)");
    run(
        &mut db,
        "INSERT INTO t VALUES ('banana'), ('apple'), ('cherry')",
    );
    let r = rows(&mut db, "SELECT MIN(s), MAX(s) FROM t");
    assert_eq!(
        r.rows,
        vec![vec![
            Value::Str("apple".into()),
            Value::Str("cherry".into())
        ]]
    );
}

#[test]
fn group_by_with_order_and_limit() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE s (dept TEXT, pay INT)");
    run(
        &mut db,
        "INSERT INTO s VALUES ('eng', 1), ('eng', 2), ('ops', 3), ('hr', 9)",
    );
    let r = rows(
        &mut db,
        "SELECT dept, COUNT(*) AS c FROM s GROUP BY dept ORDER BY c DESC, dept LIMIT 2",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("eng".into()), Value::Int(2)],
            vec![Value::Str("hr".into()), Value::Int(1)],
        ]
    );
}

// ---- SELECT expressions / filters --------------------------------

#[test]
fn from_less_select_constants() {
    let mut db = Database::in_memory().unwrap();
    let r = rows(&mut db, "SELECT 1 + 1 AS two, 'hi' AS word");
    assert_eq!(r.columns, vec!["two", "word"]);
    assert_eq!(r.rows, vec![vec![Value::Int(2), Value::Str("hi".into())]]);
}

#[test]
fn derived_table_subquery_in_from() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (n INT)");
    run(&mut db, "INSERT INTO t VALUES (1), (2), (3), (4)");
    let r = rows(
        &mut db,
        "SELECT d.n FROM (SELECT n FROM t WHERE n > 2) AS d ORDER BY d.n",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(3)], vec![Value::Int(4)]]);
}

#[test]
fn projection_alias_and_order_by_alias() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (n INT)");
    run(&mut db, "INSERT INTO t VALUES (1), (2), (3)");
    let r = rows(&mut db, "SELECT n * 2 AS dbl FROM t ORDER BY dbl DESC");
    assert_eq!(r.columns, vec!["dbl"]);
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(6)],
            vec![Value::Int(4)],
            vec![Value::Int(2)]
        ]
    );
}

#[test]
fn order_by_multiple_keys_mixed_directions() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (s TEXT, n INT)");
    run(&mut db, "INSERT INTO t VALUES ('a', 1), ('a', 3), ('b', 2)");
    let r = rows(&mut db, "SELECT s, n FROM t ORDER BY s ASC, n DESC");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("a".into()), Value::Int(3)],
            vec![Value::Str("a".into()), Value::Int(1)],
            vec![Value::Str("b".into()), Value::Int(2)],
        ]
    );
}

#[test]
fn order_by_indexed_not_null_key_limit_windows() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY NOT NULL, v TEXT)",
    );
    for i in 1..=20 {
        run(&mut db, &format!("INSERT INTO t VALUES ({i}, 'v{i}')"));
    }
    let r = rows(&mut db, "SELECT id FROM t ORDER BY id LIMIT 5 OFFSET 10");
    assert_eq!(
        r.rows,
        (11..=15).map(|i| vec![Value::Int(i)]).collect::<Vec<_>>()
    );
    let r = rows(&mut db, "SELECT id FROM t ORDER BY id DESC LIMIT 3");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(20)],
            vec![Value::Int(19)],
            vec![Value::Int(18)]
        ]
    );
    let r = rows(&mut db, "SELECT id FROM t ORDER BY id LIMIT 0 OFFSET 5");
    assert!(r.rows.is_empty());
    let r = rows(&mut db, "SELECT id FROM t ORDER BY id LIMIT 5 OFFSET 18");
    assert_eq!(r.rows, vec![vec![Value::Int(19)], vec![Value::Int(20)]]);
    let r = rows(&mut db, "SELECT id FROM t ORDER BY id LIMIT 5 OFFSET 50");
    assert!(r.rows.is_empty());
    // ORDER BY key need not be in the projection.
    let r = rows(&mut db, "SELECT id * 2 AS dbl FROM t ORDER BY id LIMIT 2");
    assert_eq!(r.columns, vec!["dbl"]);
    assert_eq!(r.rows, vec![vec![Value::Int(2)], vec![Value::Int(4)]]);
}

#[test]
fn order_by_nullable_indexed_column_keeps_null_rows() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (n INT, note TEXT)");
    run(&mut db, "CREATE INDEX ix_n ON t (n)");
    run(
        &mut db,
        "INSERT INTO t VALUES (NULL, 'null'), (1, 'one'), (2, 'two')",
    );
    // Trees omit NULL keys: the walk must stay off for nullable columns,
    // or these rows would silently disappear.
    let r = rows(&mut db, "SELECT note FROM t ORDER BY n LIMIT 2");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("null".into())],
            vec![Value::Str("one".into())]
        ]
    );
    let r = rows(&mut db, "SELECT note FROM t ORDER BY n DESC LIMIT 1");
    assert_eq!(r.rows, vec![vec![Value::Str("two".into())]]);
}

#[test]
fn count_star_fast_path_matches_generic_aggregate() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY NOT NULL, v TEXT)",
    );
    run(&mut db, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    let count_fast = rows(&mut db, "SELECT COUNT(*) FROM t");
    let count_slow = rows(&mut db, "SELECT COUNT(*) FROM t WHERE 1 = 1");
    assert_eq!(count_fast.columns, count_slow.columns);
    assert_eq!(count_fast.rows, vec![vec![Value::Int(3)]]);
    let alias_fast = rows(&mut db, "SELECT COUNT(*) AS n FROM t");
    let alias_slow = rows(&mut db, "SELECT COUNT(*) AS n FROM t WHERE 1 = 1");
    assert_eq!(alias_fast.columns, alias_slow.columns);
    assert_eq!(alias_fast.rows, vec![vec![Value::Int(3)]]);
    // COUNT(col) counts non-null values and stays on the generic path.
    assert_eq!(
        rows(&mut db, "SELECT COUNT(v) FROM t").rows,
        vec![vec![Value::Int(3)]]
    );
    // Liveness: updates and deletes must not leave tombstones counted.
    run(&mut db, "UPDATE t SET v = 'a' WHERE id = 2");
    run(&mut db, "DELETE FROM t WHERE id = 1");
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM t").rows,
        vec![vec![Value::Int(2)]]
    );
    run(&mut db, "INSERT INTO t VALUES (4, NULL)");
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM t").rows,
        vec![vec![Value::Int(3)]]
    );
    assert_eq!(
        rows(&mut db, "SELECT COUNT(v) FROM t").rows,
        vec![vec![Value::Int(2)]]
    );
    run(&mut db, "DELETE FROM t");
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM t").rows,
        vec![vec![Value::Int(0)]]
    );
    // LIMIT / WHERE keep the generic path but the same answer.
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM t LIMIT 1").rows,
        vec![vec![Value::Int(0)]]
    );
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM t WHERE 1 = 1").rows,
        vec![vec![Value::Int(0)]]
    );
}

#[test]
fn unindexed_order_window_resolves_dotted_suffix_keys() {
    // 全点号键的文档上,通用路径的 lookup_col 用 ".x" 后缀解析裸名;
    // top-K 窗口的字节级提取必须同答,否则选出的窗口行本身漂移。
    // (表必须全列点号——任何无点号键都会让后缀回退失效。)
    let mut db = Database::in_memory().unwrap();
    run(&mut db, r#"CREATE TABLE d2 ("a.x" INT, "a.y" INT)"#);
    run(&mut db, r#"INSERT INTO d2 VALUES (3, 0), (1, 0), (2, 0)"#);
    let a = rows(&mut db, r#"SELECT "a.y" FROM d2 ORDER BY x LIMIT 2"#);
    let b = rows(
        &mut db,
        r#"SELECT "a.y" FROM d2 WHERE 1 = 1 ORDER BY x LIMIT 2"#,
    );
    assert_eq!(a.rows, b.rows, "top-K 窗口与通用路径必须同答");
    // x 解析为 "a.x":升序前两名是 1、2(其 a.y 均为 0)。
    assert_eq!(a.rows, vec![vec![Value::Int(0)], vec![Value::Int(0)]]);
    // 用可区分的 a.y 验证排序确实按 a.x:注入三行不同值。
    run(&mut db, r#"DELETE FROM d2"#);
    run(
        &mut db,
        r#"INSERT INTO d2 VALUES (3, 30), (1, 10), (2, 20)"#,
    );
    let a = rows(&mut db, r#"SELECT "a.y" FROM d2 ORDER BY x LIMIT 2"#);
    assert_eq!(a.rows, vec![vec![Value::Int(10)], vec![Value::Int(20)]]);
    let a = rows(&mut db, r#"SELECT "a.y" FROM d2 ORDER BY x DESC LIMIT 1"#);
    assert_eq!(a.rows, vec![vec![Value::Int(30)]]);
}

#[test]
fn unindexed_order_window_matches_generic_sort() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY NOT NULL, name TEXT, v INT)",
    );
    // Duplicates, NULLs, mixed directions and a missing field: the
    // schemaless sort semantics and the stable tie order must match the
    // generic full decode + sort path.
    run(
        &mut db,
        "INSERT INTO t VALUES (1, 'b', 10), (2, NULL, 5), (3, 'a', 7), (4, 'b', 9), \
             (5, NULL, 5), (6, 'c', 3)",
    );
    run(&mut db, "INSERT INTO t (id) VALUES (7)");
    let pairs = [
        (
            "SELECT id FROM t ORDER BY name LIMIT 3 OFFSET 1",
            "SELECT id FROM t WHERE 1 = 1 ORDER BY name LIMIT 3 OFFSET 1",
        ),
        (
            "SELECT id FROM t ORDER BY name DESC LIMIT 2",
            "SELECT id FROM t WHERE 1 = 1 ORDER BY name DESC LIMIT 2",
        ),
        (
            "SELECT id FROM t ORDER BY name NULLS LAST LIMIT 2",
            "SELECT id FROM t WHERE 1 = 1 ORDER BY name NULLS LAST LIMIT 2",
        ),
        (
            "SELECT id FROM t ORDER BY v DESC, name ASC LIMIT 4",
            "SELECT id FROM t WHERE 1 = 1 ORDER BY v DESC, name ASC LIMIT 4",
        ),
        (
            "SELECT id FROM t ORDER BY v LIMIT 3 OFFSET 2",
            "SELECT id FROM t WHERE 1 = 1 ORDER BY v LIMIT 3 OFFSET 2",
        ),
        (
            "SELECT id, name FROM t ORDER BY name LIMIT 20",
            "SELECT id, name FROM t WHERE 1 = 1 ORDER BY name LIMIT 20",
        ),
        (
            "SELECT id FROM t ORDER BY name LIMIT 2 OFFSET 99",
            "SELECT id FROM t WHERE 1 = 1 ORDER BY name LIMIT 2 OFFSET 99",
        ),
        (
            "SELECT v AS k FROM t ORDER BY k LIMIT 2",
            "SELECT v AS k FROM t WHERE 1 = 1 ORDER BY k LIMIT 2",
        ),
        // Output alias shadows a source field: the key is the projected
        // expression, not the field with the same name.
        (
            "SELECT v AS name FROM t ORDER BY name LIMIT 2",
            "SELECT v AS name FROM t WHERE 1 = 1 ORDER BY name LIMIT 2",
        ),
        (
            "SELECT id FROM t AS x ORDER BY x.name LIMIT 2",
            "SELECT id FROM t AS x WHERE 1 = 1 ORDER BY x.name LIMIT 2",
        ),
        (
            "SELECT id FROM t ORDER BY t.name LIMIT 2",
            "SELECT id FROM t WHERE 1 = 1 ORDER BY t.name LIMIT 2",
        ),
    ];
    for (fast, generic) in pairs {
        assert_eq!(
            rows(&mut db, fast).rows,
            rows(&mut db, generic).rows,
            "fast={fast} generic={generic}"
        );
    }
}

/// 索引序窗口的别名遮蔽差分:ORDER BY 键名命中输出别名时,通用路径按
/// 输出列值排序,索引窗口曾按同名带索引表列走树序 —— 行序与 LIMIT
/// 截断的行集都错。
#[test]
fn ordered_index_window_alias_shadow_matches_generic() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (a INT NOT NULL PRIMARY KEY, b INT NOT NULL)",
    );
    run(&mut db, "CREATE INDEX i_b ON t(b)");
    run(&mut db, "INSERT INTO t VALUES (1, 30), (2, 20), (3, 10)");
    let pairs = [
        // 别名遮蔽带索引列:键是投影表达式(b 的值),不是列 a。
        (
            "SELECT b AS a FROM t ORDER BY a LIMIT 2",
            "SELECT b AS a FROM t WHERE 1 = 1 ORDER BY a LIMIT 2",
        ),
        // 恒等别名(表达式就是同一裸列):索引窗口照常生效且等价。
        (
            "SELECT b AS b FROM t ORDER BY b LIMIT 2",
            "SELECT b AS b FROM t WHERE 1 = 1 ORDER BY b LIMIT 2",
        ),
        (
            "SELECT a AS x FROM t ORDER BY a LIMIT 2",
            "SELECT a AS x FROM t WHERE 1 = 1 ORDER BY a LIMIT 2",
        ),
        // 通配符投影:恒等映射,不受影响。
        (
            "SELECT * FROM t ORDER BY a LIMIT 2",
            "SELECT * FROM t WHERE 1 = 1 ORDER BY a LIMIT 2",
        ),
        // 序号键回退通用路径。
        (
            "SELECT b, a FROM t ORDER BY 1 LIMIT 2",
            "SELECT b, a FROM t WHERE 1 = 1 ORDER BY 1 LIMIT 2",
        ),
    ];
    for (fast, generic) in pairs {
        assert_eq!(
            rows(&mut db, fast).rows,
            rows(&mut db, generic).rows,
            "fast={fast} generic={generic}"
        );
    }
}

#[test]
fn ordered_index_window_wildcard_keeps_full_field_union() {
    // Regression: the wildcard's column set is the union over ALL
    // filtered rows (the generic path derives it before LIMIT); the
    // index window loaded only the skip+take rows, so a field living
    // solely in a skipped row vanished from `SELECT *` — the output
    // shape drifted with whether the ORDER BY column had an index.
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY NOT NULL, a INT NOT NULL, name TEXT)",
    );
    run(&mut db, "CREATE INDEX i_a ON t(a)");
    run(&mut db, "INSERT INTO t VALUES (1,1,'x'),(2,2,'y')");
    run(
        &mut db,
        "INSERT INTO t (id,a,name,extra) VALUES (3,3,'z','surprise')",
    );
    let fast = rows(&mut db, "SELECT * FROM t ORDER BY a LIMIT 2");
    let generic = rows(&mut db, "SELECT * FROM t WHERE 1 = 1 ORDER BY a LIMIT 2");
    assert_eq!(fast.columns, generic.columns);
    assert!(
        fast.columns.contains(&"extra".to_string()),
        "columns: {:?}",
        fast.columns
    );
}

#[test]
fn grouped_select_honors_distinct() {
    // Regression: the grouped path used to ignore `select.distinct`
    // entirely — GROUP BY keys finer than the projection returned
    // duplicate rows, and DISTINCT ON passed silently where the plain
    // path reports it unsupported.
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE g (a INT, b INT)");
    run(&mut db, "INSERT INTO g VALUES (1,1),(1,2),(2,1),(2,2)");
    let r = rows(&mut db, "SELECT DISTINCT a, COUNT(*) FROM g GROUP BY a, b");
    assert_eq!(r.rows.len(), 2, "{:?}", r.rows);
    let r = rows(&mut db, "SELECT DISTINCT COUNT(*) FROM g GROUP BY a");
    assert_eq!(r.rows.len(), 1, "{:?}", r.rows);
    assert!(db
        .execute("SELECT DISTINCT ON (a) COUNT(*) FROM g GROUP BY a")
        .is_err());
}

// ---- standard clause matrix ----

#[test]
fn grouping_sets_rollup_cube_and_grouping() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (g TEXT, h TEXT, v INT)");
    run(
        &mut db,
        "INSERT INTO t VALUES ('a','x',1),('a','y',2),('b','x',3),('b','x',4)",
    );
    // ROLLUP(g): detail rows, subtotal (NULL g) and grand total.
    let r = rows(
        &mut db,
        "SELECT g, SUM(v) FROM t GROUP BY ROLLUP(g) ORDER BY g",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Null, Value::Int(10)],
            vec![Value::Str("a".into()), Value::Int(3)],
            vec![Value::Str("b".into()), Value::Int(7)],
        ]
    );
    // GROUPING distinguishes the subtotal NULL from a NULL key.
    let r = rows(
        &mut db,
        "SELECT g, GROUPING(g) AS grp, SUM(v) FROM t GROUP BY ROLLUP(g) ORDER BY g",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Null, Value::Int(1), Value::Int(10)],
            vec![Value::Str("a".into()), Value::Int(0), Value::Int(3)],
            vec![Value::Str("b".into()), Value::Int(0), Value::Int(7)],
        ]
    );
    // CUBE(g, h) expands to (g,h), (g), (h), (): 3 + 2 + 2 + 1 groups.
    let r = rows(
        &mut db,
        "SELECT COUNT(*) FROM (SELECT g, h, SUM(v) FROM t GROUP BY CUBE(g, h)) AS c",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(8)]]);
    // GROUPING SETS over explicit subsets.
    let r = rows(
        &mut db,
        "SELECT g, h, SUM(v) FROM t GROUP BY GROUPING SETS ((g), (h)) ORDER BY g, h",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Null, Value::Str("x".into()), Value::Int(8)],
            vec![Value::Null, Value::Str("y".into()), Value::Int(2)],
            vec![Value::Str("a".into()), Value::Null, Value::Int(3)],
            vec![Value::Str("b".into()), Value::Null, Value::Int(7)],
        ]
    );
    // HAVING applies per set-group (aggregates over that group's rows).
    let r = rows(
        &mut db,
        "SELECT g, SUM(v) FROM t GROUP BY ROLLUP(g) HAVING SUM(v) > 5 ORDER BY g",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Null, Value::Int(10)],
            vec![Value::Str("b".into()), Value::Int(7)],
        ]
    );
    // Duplicate and nested grouping sets are rejected.
    let e = db
        .execute("SELECT g FROM t GROUP BY GROUPING SETS ((g), (g))")
        .unwrap_err();
    assert!(e.to_string().contains("duplicate"), "{e}");
    let e = db
        .execute("SELECT g FROM t GROUP BY GROUPING SETS ((g), ROLLUP(g))")
        .unwrap_err();
    assert!(e.to_string().contains("nested"), "{e}");
    // GROUPING() only accepts a grouping-set expression.
    let e = db
        .execute("SELECT GROUPING(v) FROM t GROUP BY g")
        .unwrap_err();
    assert!(e.to_string().contains("GROUPING"), "{e}");
    // CUBE is bounded to keep the set explosion sane.
    let e = db
        .execute("SELECT COUNT(*) FROM t GROUP BY CUBE(a,b,c,d,e,f,g,h,i,j,k,l,m)")
        .unwrap_err();
    assert!(e.to_string().contains("CUBE"), "{e}");
}

#[test]
fn aggregate_filter_where() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (g TEXT, v INT)");
    run(
        &mut db,
        "INSERT INTO t VALUES ('a',1),('a',2),('b',3),('b',4),('b',5)",
    );
    let r = rows(
        &mut db,
        "SELECT COUNT(*) FILTER (WHERE v > 2), SUM(v) FILTER (WHERE g = 'a') FROM t",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(3), Value::Int(3)]]);
    // FILTER applies before DISTINCT and per group.
    let r = rows(
        &mut db,
        "SELECT g, COUNT(DISTINCT v) FILTER (WHERE v > 1) FROM t GROUP BY g ORDER BY g",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("a".into()), Value::Int(1)],
            vec![Value::Str("b".into()), Value::Int(3)],
        ]
    );
    // Nothing passes the filter: COUNT is 0, SUM over no rows is NULL.
    let r = rows(
        &mut db,
        "SELECT COUNT(*) FILTER (WHERE v > 99), SUM(v) FILTER (WHERE v > 99) FROM t",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(0), Value::Null]]);
    // FILTER inside a composite aggregate expression.
    let r = rows(
        &mut db,
        "SELECT COALESCE(SUM(v) FILTER (WHERE g = 'a'), 0) + 1 FROM t",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(4)]]);
}

#[test]
fn fetch_with_ties_extends_the_window() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (g TEXT, v INT)");
    run(
        &mut db,
        "INSERT INTO t VALUES ('a',1),('a',2),('b',3),('b',4),('b',5)",
    );
    // One key, five rows of peers: the whole tie run comes back.
    let r = rows(
        &mut db,
        "SELECT g FROM t ORDER BY g FETCH FIRST 1 ROWS WITH TIES",
    );
    assert_eq!(
        r.rows,
        vec![vec![Value::Str("a".into())], vec![Value::Str("a".into())]]
    );
    // Unique ORDER BY values: ties do not extend.
    let r = rows(
        &mut db,
        "SELECT g FROM t ORDER BY v FETCH FIRST 2 ROWS WITH TIES",
    );
    assert_eq!(
        r.rows,
        vec![vec![Value::Str("a".into())], vec![Value::Str("a".into())]]
    );
    // Hidden ORDER BY keys (not projected) still define the peers.
    let r = rows(
        &mut db,
        "SELECT g FROM t ORDER BY v DESC FETCH FIRST 1 ROWS WITH TIES",
    );
    assert_eq!(r.rows, vec![vec![Value::Str("b".into())]]);
}

#[test]
fn values_rows_and_alias_columns() {
    let mut db = Database::in_memory().unwrap();
    // Top-level VALUES: default column names; ORDER BY/LIMIT apply.
    let r = rows(&mut db, "VALUES (2, 'b'), (1, 'a') ORDER BY column1");
    assert_eq!(r.columns, vec!["column1", "column2"]);
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(1), Value::Str("a".into())],
            vec![Value::Int(2), Value::Str("b".into())],
        ]
    );
    // Derived table with an explicit alias column list.
    let r = rows(
        &mut db,
        "SELECT p.id, p.s FROM (VALUES (1,'x'),(2,'y')) AS p(id, s) WHERE p.id > 1",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(2), Value::Str("y".into())]]);
    // CTE alias columns rename positionally too.
    let r = rows(
        &mut db,
        "WITH c(id, s) AS (VALUES (7, 'z')) SELECT c.id, c.s FROM c",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(7), Value::Str("z".into())]]);
    // Alias width/duplicates are rejected instead of silently dropping.
    let e = db
        .execute("SELECT * FROM (VALUES (1, 2)) AS p(a)")
        .unwrap_err();
    assert!(e.to_string().contains("alias"), "{e}");
    let e = db
        .execute("SELECT * FROM (VALUES (1, 2)) AS p(a, a)")
        .unwrap_err();
    assert!(e.to_string().contains("distinct"), "{e}");
    // Ragged VALUES rows are rejected.
    let e = db.execute("VALUES (1), (2, 3)").unwrap_err();
    assert!(e.to_string().contains("same column count"), "{e}");
}

#[test]
fn is_distinct_from_is_null_safe() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (a INT, b INT)");
    run(
        &mut db,
        "INSERT INTO t VALUES (1,1),(1,2),(NULL,NULL),(NULL,1)",
    );
    let r = rows(
        &mut db,
        "SELECT a IS NOT DISTINCT FROM b AS same, a IS DISTINCT FROM b AS diff \
             FROM t ORDER BY a IS NULL, a, b",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Bool(true), Value::Bool(false)],
            vec![Value::Bool(false), Value::Bool(true)],
            vec![Value::Bool(true), Value::Bool(false)],
            vec![Value::Bool(false), Value::Bool(true)],
        ]
    );
}

#[test]
fn quantified_any_all_comparisons() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (v INT)");
    run(&mut db, "INSERT INTO t VALUES (1),(2),(3),(4),(5)");
    // Subquery yields {4, 5}: v > 4 or v > 5.
    let r = rows(
        &mut db,
        "SELECT v FROM t WHERE v > ANY (SELECT v FROM t WHERE v > 3)",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(5)]]);
    let r = rows(
        &mut db,
        "SELECT v FROM t WHERE v <> ALL (SELECT v FROM t WHERE v > 3) ORDER BY v",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)],
        ]
    );
    // Empty subquery: ANY is false, ALL is vacuously true.
    let r = rows(
        &mut db,
        "SELECT COUNT(*) FROM t WHERE v > ANY (SELECT v FROM t WHERE v > 99)",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(0)]]);
    let r = rows(
        &mut db,
        "SELECT COUNT(*) FROM t WHERE v > ALL (SELECT v FROM t WHERE v > 99)",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(5)]]);
    // SOME is a synonym for ANY (equality form goes through IN).
    let r = rows(
        &mut db,
        "SELECT COUNT(*) FROM t WHERE v = SOME (SELECT v FROM t WHERE v > 4)",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn unsupported_clauses_error_loudly() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (v INT)");
    run(&mut db, "INSERT INTO t VALUES (1)");
    let cases: &[(&str, &str)] = &[
        ("SELECT * FROM t FOR UPDATE", "FOR UPDATE"),
        ("SELECT * FROM t FOR SHARE", "FOR UPDATE"),
        ("SELECT * INTO x FROM t", "SELECT INTO"),
        // SELECT TOP is supported now (rewritten onto LIMIT/FETCH);
        // TOP ... PERCENT keeps its loud error.
        ("SELECT TOP 50 PERCENT * FROM t", "PERCENT"),
        ("SELECT * FROM t TABLESAMPLE BERNOULLI (10)", "TABLESAMPLE"),
        ("SELECT * FROM t NATURAL JOIN t AS u", "NATURAL JOIN"),
        ("SELECT * FROM t, LATERAL (SELECT 1) AS l", "LATERAL"),
        ("SELECT v FROM t WINDOW w AS (ORDER BY v)", "WINDOW"),
        // Known table functions work; unknown ones still refuse.
        ("SELECT * FROM make_interval(1)", "table functions"),
        ("SELECT v FROM t GROUP BY v SETTINGS x = 1", "SETTINGS"),
        (
            "SELECT v FROM t GROUP BY v WITH ROLLUP",
            "GROUP BY modifiers",
        ),
        ("SELECT * EXCLUDE (v) FROM t", "EXCLUDE"),
        (
            "SELECT v FROM t LATERAL VIEW explode(v) x AS y",
            "LATERAL VIEW",
        ),
        ("SELECT v FROM t PREWHERE v = 1", "PREWHERE"),
        (
            "SELECT v FROM t START WITH v = 1 CONNECT BY PRIOR v = v",
            "CONNECT BY",
        ),
        ("SELECT v FROM t CLUSTER BY v", "CLUSTER BY"),
        ("SELECT v FROM t DISTRIBUTE BY v", "DISTRIBUTE BY"),
        ("SELECT v FROM t SORT BY v", "SORT BY"),
        ("SELECT v FROM t FOR XML AUTO", "FOR XML"),
        ("SELECT v FROM t FORMAT JSONCompact", "FORMAT"),
        ("SELECT v FROM t |> SELECT v", "pipe operators"),
        // ClickHouse LIMIT … BY:曾被静默当成裸 LIMIT 处理。
        ("SELECT v FROM t LIMIT 1 BY v", "LIMIT ... BY"),
    ];
    for (sql, want) in cases {
        let e = db.execute(sql).unwrap_err();
        assert!(
            e.to_string().contains(want),
            "sql={sql} error={e} want={want}"
        );
    }
}

/// FROM-less SELECT 的 FETCH 截断(曾完全忽略该子句)。
#[test]
fn from_less_select_honors_fetch() {
    let mut db = Database::in_memory().unwrap();
    let r = rows(&mut db, "SELECT 1 FETCH FIRST 0 ROWS ONLY");
    assert!(r.rows.is_empty(), "{r:?}");
    let r = rows(&mut db, "SELECT 1 FETCH FIRST 5 ROWS ONLY");
    assert_eq!(r.rows, vec![vec![Value::Int(1)]]);
    assert!(db.execute("SELECT 1 FETCH FIRST 1 ROWS WITH TIES").is_err());
}

/// 集合操作的臂共享一次 CTE 物化(语义等价;行集与重复执行一致)。
#[test]
fn union_arms_share_cte_materialization() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE u (v INT)");
    run(&mut db, "INSERT INTO u VALUES (1), (2), (3)");
    let r = rows(
        &mut db,
        "WITH c AS (SELECT v FROM u WHERE v > 1) \
             SELECT v FROM c UNION SELECT v + 10 FROM c UNION SELECT 0 ORDER BY v",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(0)],
            vec![Value::Int(2)],
            vec![Value::Int(3)],
            vec![Value::Int(12)],
            vec![Value::Int(13)]
        ]
    );
    // EXCEPT/INTERSECT 臂同样受益且语义不变。
    let r = rows(
        &mut db,
        "WITH c AS (SELECT v FROM u WHERE v > 1) \
             SELECT v FROM c EXCEPT SELECT 3",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(2)]]);
}

#[test]
fn count_star_fast_path_skips_grouped_queries() {
    // Regression: the no-decode COUNT(*) shortcut must not swallow
    // GROUP BY (it would count every row once instead of per group).
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (g TEXT)");
    run(&mut db, "INSERT INTO t VALUES ('a'),('a'),('b')");
    let r = rows(&mut db, "SELECT g, COUNT(*) FROM t GROUP BY g ORDER BY g");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("a".into()), Value::Int(2)],
            vec![Value::Str("b".into()), Value::Int(1)],
        ]
    );
}

#[test]
fn unsupported_constraint_and_conflict_forms_error() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (a INT, b INT)");
    for (sql, want) in [
        (
            "CREATE TABLE c1 (a INT, b INT, PRIMARY KEY (a, b))",
            "composite PRIMARY KEY",
        ),
        (
            "CREATE TABLE c2 (a INT, b INT, UNIQUE (a, b))",
            "composite UNIQUE",
        ),
        (
            "INSERT INTO t VALUES (1, 1) ON CONFLICT (a) DO NOTHING",
            "no unique constraint",
        ),
        // DO UPDATE is now supported (see the upsert tests below); on a
        // constraint-less table its target resolution is what errors.
    ] {
        let e = db.execute(sql).unwrap_err();
        assert!(e.to_string().contains(want), "sql={sql} error={e}");
    }
    // The single-column spellings keep working, including DO NOTHING.
    run(&mut db, "CREATE TABLE ok (a INT PRIMARY KEY, b INT UNIQUE)");
    run(&mut db, "INSERT INTO ok VALUES (1, 1)");
    run(
        &mut db,
        "INSERT INTO ok VALUES (1, 2) ON CONFLICT DO NOTHING",
    );
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM ok").rows,
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn tsql_aliases_and_silent_forms() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v INT, b INT)");
    run(&mut db, "INSERT INTO t VALUES (1, 10, 20), (2, 30, NULL)");
    // T-SQL aliases that map exactly onto existing behavior.
    let r = rows(&mut db, "SELECT COUNT_BIG(*) FROM t");
    assert_eq!(r.rows, vec![vec![Value::Int(2)]]);
    let r = rows(&mut db, "SELECT ISNULL(b, -1) FROM t ORDER BY id");
    assert_eq!(r.rows, vec![vec![Value::Int(20)], vec![Value::Int(-1)]]);
    // T-SQL BIT target maps to BOOL.
    let r = rows(&mut db, "SELECT CAST(1 AS BIT), CAST(0 AS BIT)");
    assert_eq!(r.rows, vec![vec![Value::Bool(true), Value::Bool(false)]]);
    // EF Core's Skip/Take-without-order shape now substitutes the
    // subquery instead of failing the correlated check.
    let r = rows(
        &mut db,
        "SELECT id FROM t ORDER BY (SELECT 1) LIMIT 1 OFFSET 1",
    );
    assert_eq!(r.rows.len(), 1);
    // Uppercase catalog view names resolve like their lowercase forms.
    let r = rows(&mut db, "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS");
    assert_eq!(r.rows, vec![vec![Value::Int(3)]]);
    // Forms that used to be silently ignored now refuse loudly.
    for (sql, want) in [
        ("SELECT @@ROWCOUNT", "variables/parameters"),
        ("SELECT @@VERSION", "variables/parameters"),
        ("SELECT @x = 1", "variables/parameters"),
        ("SELECT * FROM t WITH (NOLOCK)", "table hints"),
        ("SELECT * FROM t AS x (a, b)", "column aliases"),
        ("SELECT * FROM t OPTION (RECOMPILE)", "column aliases"),
        ("SELECT * FROM t (NOLOCK)", "table functions or FROM hints"),
        ("INSERT INTO t VALUES (3, 1, 1) OUTPUT INSERTED.*", "OUTPUT"),
        (
            "UPDATE t SET v = 1 OUTPUT INSERTED.* WHERE id = 1",
            "OUTPUT",
        ),
        ("DELETE FROM t OUTPUT DELETED.* WHERE id = 2", "OUTPUT"),
        (
            "MERGE INTO t USING t AS s ON t.id = s.id \
                 WHEN MATCHED THEN UPDATE SET v = s.v OUTPUT $action",
            "MERGE OUTPUT",
        ),
        ("CREATE TABLE #temp (a INT)", "temp tables"),
        ("CREATE INDEX ix_i ON t (v) INCLUDE (b)", "INCLUDE"),
        (
            "CREATE INDEX ix_pj ON t (JSON_EXTRACT(v, '$.a')) WHERE v > 0",
            "cannot combine",
        ),
        ("SELECT * FROM sys.tables", "catalog views"),
        ("SELECT * FROM sysobjects", "catalog views"),
    ] {
        let e = db.execute(sql).unwrap_err();
        assert!(
            e.to_string().contains(want),
            "sql={sql} error={e} want={want}"
        );
    }
    // MERGE DELETE is refused with an accurate message.
    let e = db
        .execute("MERGE INTO t USING t AS s ON t.id = s.id WHEN MATCHED THEN DELETE")
        .unwrap_err();
    assert!(e.to_string().contains("UPDATE"), "{e}");
}

#[test]
fn on_conflict_target_scopes_the_skip() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY, v TEXT UNIQUE)",
    );
    run(&mut db, "INSERT INTO t VALUES (1, 'a')");
    // Target matches the PK: a conflicting id is skipped.
    run(
        &mut db,
        "INSERT INTO t VALUES (1, 'b') ON CONFLICT (id) DO NOTHING",
    );
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM t").rows,
        vec![vec![Value::Int(1)]]
    );
    // A conflict on a non-target unique constraint still errors.
    let e = db
        .execute("INSERT INTO t VALUES (2, 'a') ON CONFLICT (id) DO NOTHING")
        .unwrap_err();
    assert!(
        e.to_string().contains("unique") || e.to_string().contains("UNIQUE"),
        "{e}"
    );
    // A target that names no unique constraint errors.
    let e = db
        .execute("INSERT INTO t VALUES (3, 'c') ON CONFLICT (v, id) DO NOTHING")
        .unwrap_err();
    assert!(e.to_string().contains("no unique constraint"), "{e}");
}

// ---- upsert-UPDATE (ON CONFLICT DO UPDATE / ON DUPLICATE KEY UPDATE) ----

#[test]
fn on_conflict_do_update_updates_the_conflicting_row() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY, v TEXT, n INT)",
    );
    run(&mut db, "INSERT INTO t VALUES (1, 'a', 10)");
    // Insert a NEW row: unaffected by the upsert arm.
    run(
        &mut db,
        "INSERT INTO t VALUES (2, 'b', 5) ON CONFLICT (id) DO UPDATE SET n = n + 1",
    );
    // Conflict on id=1: updated in place (EXCLUDED carries the
    // candidate's values, bare columns the existing row's).
    run(
        &mut db,
        "INSERT INTO t VALUES (1, 'z', 7) ON CONFLICT (id) DO UPDATE \
             SET v = EXCLUDED.v, n = n + EXCLUDED.n",
    );
    let r = rows(&mut db, "SELECT id, v, n FROM t ORDER BY id");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(1), Value::Str("z".into()), Value::Int(17)],
            vec![Value::Int(2), Value::Str("b".into()), Value::Int(5)],
        ]
    );
}

#[test]
fn on_conflict_do_update_where_filters_the_update() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, n INT)");
    run(&mut db, "INSERT INTO t VALUES (1, 1)");
    // WHERE false: the row keeps its image, nothing is inserted.
    run(
        &mut db,
        "INSERT INTO t VALUES (1, 99) ON CONFLICT (id) DO UPDATE SET n = 99 WHERE n > 100",
    );
    assert_eq!(
        rows(&mut db, "SELECT id, n FROM t").rows,
        vec![vec![Value::Int(1), Value::Int(1)]]
    );
    // WHERE true (EXCLUDED visible in the predicate): applied.
    run(
        &mut db,
        "INSERT INTO t VALUES (1, 99) ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n \
             WHERE EXCLUDED.n > n",
    );
    assert_eq!(
        rows(&mut db, "SELECT id, n FROM t").rows,
        vec![vec![Value::Int(1), Value::Int(99)]]
    );
}

#[test]
fn on_conflict_do_update_multi_row_is_sequential() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, n INT)");
    run(&mut db, "INSERT INTO t VALUES (1, 0)");
    // Both candidates target id=1: the second sees the first's effect
    // (MySQL ON DUPLICATE KEY semantics; PostgreSQL would reject a
    // second touch of the same row).
    run(
        &mut db,
        "INSERT INTO t VALUES (1, 1), (1, 2) ON CONFLICT (id) DO UPDATE SET n = n + EXCLUDED.n",
    );
    assert_eq!(
        rows(&mut db, "SELECT n FROM t").rows,
        vec![vec![Value::Int(3)]]
    );
}

#[test]
fn on_conflict_do_update_targeted_other_keys_still_error() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY, v TEXT UNIQUE)",
    );
    run(&mut db, "INSERT INTO t VALUES (1, 'a')");
    // Conflict on the UNIQUE column while the target names the PK:
    // the upsert arm must not swallow it (directed-conflict contract).
    let e = db
        .execute("INSERT INTO t VALUES (2, 'a') ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v")
        .unwrap_err();
    assert!(e.to_string().contains("UNIQUE constraint"), "{e}");
}

#[test]
fn on_conflict_do_update_maintains_unique_index_images() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY, k INT UNIQUE, n INT)",
    );
    run(&mut db, "CREATE INDEX ix_n ON t(n)");
    run(&mut db, "INSERT INTO t VALUES (1, 100, 1), (2, 200, 2)");
    // Upsert rewrites the key k=100 → k=150: the unique tree must
    // follow both directions.
    run(
            &mut db,
            "INSERT INTO t VALUES (1, 150, 9) ON CONFLICT (id) DO UPDATE SET k = EXCLUDED.k, n = EXCLUDED.n",
        );
    assert_eq!(
        rows(&mut db, "SELECT id FROM t WHERE k = 150").rows,
        vec![vec![Value::Int(1)]]
    );
    assert!(rows(&mut db, "SELECT id FROM t WHERE k = 100")
        .rows
        .is_empty());
    // The freed key accepts a fresh insert.
    run(&mut db, "INSERT INTO t VALUES (3, 100, 3)");
    assert_eq!(
        rows(&mut db, "SELECT n FROM t WHERE k = 100").rows,
        vec![vec![Value::Int(3)]]
    );
}

#[test]
fn on_conflict_do_update_on_constraint_spelling() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, n INT)");
    run(&mut db, "INSERT INTO t VALUES (1, 1)");
    run(
        &mut db,
        "INSERT INTO t VALUES (1, 2) ON CONFLICT ON CONSTRAINT sqlite_autoindex_t_1 \
             DO UPDATE SET n = EXCLUDED.n",
    );
    assert_eq!(
        rows(&mut db, "SELECT n FROM t").rows,
        vec![vec![Value::Int(2)]]
    );
}

#[test]
fn on_duplicate_key_update_mysql_spelling() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, n INT)");
    run(&mut db, "INSERT INTO t VALUES (1, 10)");
    // MySQL: any unique key matches (no target); VALUES(col) carries
    // the proposed value.
    run(
        &mut db,
        "INSERT INTO t VALUES (1, 5) ON DUPLICATE KEY UPDATE n = n + VALUES(n)",
    );
    assert_eq!(
        rows(&mut db, "SELECT n FROM t").rows,
        vec![vec![Value::Int(15)]]
    );
    // Untargeted also catches a second unique column's conflict.
    run(&mut db, "CREATE TABLE u (a INT PRIMARY KEY, b INT UNIQUE)");
    run(&mut db, "INSERT INTO u VALUES (1, 7)");
    run(
        &mut db,
        "INSERT INTO u VALUES (9, 7) ON DUPLICATE KEY UPDATE b = 8",
    );
    assert_eq!(
        rows(&mut db, "SELECT a, b FROM u").rows,
        vec![vec![Value::Int(1), Value::Int(8)]]
    );
}

/// EXCLUDED/VALUES() 在 FLOOR/SUBSTRING/TRIM/IS NULL 等专属 AST 节点里
/// 曾不被替换,静默解析到已存在行的同名列(候选行的值被丢弃)。
#[test]
fn on_conflict_do_update_substitutes_excluded_in_special_nodes() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY, q INT, flag TEXT)",
    );
    run(&mut db, "INSERT INTO t VALUES (1, 10, 'x')");
    // FLOOR(EXCLUDED.q / 2):候选 q=5 → 2;旧行 q=10 → 5(错值)。
    run(
        &mut db,
        "INSERT INTO t (id, q) VALUES (1, 5) ON CONFLICT (id) DO UPDATE \
             SET q = FLOOR(EXCLUDED.q / 2)",
    );
    let r = rows(&mut db, "SELECT q FROM t WHERE id = 1");
    assert_eq!(r.rows, vec![vec![Value::Int(2)]]);

    // WHERE 臂里的 TRIM(EXCLUDED.flag) 同理:候选 'x' 命中才更新。
    run(
        &mut db,
        "INSERT INTO t (id, q, flag) VALUES (1, 7, 'x') ON CONFLICT (id) DO UPDATE \
             SET q = EXCLUDED.q WHERE TRIM(EXCLUDED.flag) = 'x'",
    );
    let r = rows(&mut db, "SELECT q FROM t WHERE id = 1");
    assert_eq!(r.rows, vec![vec![Value::Int(7)]]);

    // IS NULL / SUBSTRING 节点也不再漏替换。
    run(
        &mut db,
        "INSERT INTO t (id, q, flag) VALUES (1, 9, 'x') ON CONFLICT (id) DO UPDATE \
             SET q = EXCLUDED.q, flag = SUBSTRING(EXCLUDED.flag FROM 1) \
             WHERE EXCLUDED.q IS NOT NULL",
    );
    let r = rows(&mut db, "SELECT q, flag FROM t WHERE id = 1");
    assert_eq!(r.rows, vec![vec![Value::Int(9), Value::Str("x".into())]]);
}

/// 零行 INSERT ... SELECT NEWID():回写曾退化成 `INSERT INTO t () VALUES `
/// (不可解析文本)并进 journal/扇出,重放节点永久失败。
#[test]
fn zero_row_insert_select_with_nondet_replays_deterministically() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id TEXT PRIMARY KEY)");
    run(&mut db, "CREATE TABLE src (x INT)");
    run(
        &mut db,
        "INSERT INTO t (id) SELECT NEWID() FROM src WHERE 1 = 0",
    );
    // The original text re-rolls NEWID()/RAND() on every replaying peer
    // (an empty draw here can be non-empty there). The rewrite must be a
    // deterministically-empty statement instead — never the degenerate
    // zero-column canonical form.
    let Some(resolved) = db.take_resolved_sql() else {
        panic!("zero-row nondet INSERT must ship a deterministic empty rewrite");
    };
    assert!(
        resolved.contains("WHERE 1 = 0") && resolved.contains("INSERT INTO"),
        "rewrite must be a deterministically-empty INSERT: {resolved}"
    );
    let r = rows(&mut db, "SELECT COUNT(*) FROM t");
    assert_eq!(r.rows, vec![vec![Value::Int(0)]]);
    // Replaying the rewrite on a fresh node stays empty.
    let mut db2 = Database::in_memory().unwrap();
    run(&mut db2, "CREATE TABLE t (id TEXT PRIMARY KEY)");
    run(&mut db2, "CREATE TABLE src (x INT)");
    run(&mut db2, &resolved);
    let r = rows(&mut db2, "SELECT COUNT(*) FROM t");
    assert_eq!(r.rows, vec![vec![Value::Int(0)]]);
}

#[test]
fn or_replace_with_do_nothing_is_rejected() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    run(&mut db, "INSERT INTO t VALUES (1, 10)");
    let m = db
        .execute("INSERT OR REPLACE INTO t (id, v) VALUES (1, 20) ON CONFLICT (id) DO NOTHING")
        .expect_err("displacement + skip must be rejected");
    assert!(m.to_string().contains("cannot combine"), "{m}");
    // The original row is intact — the statement was rejected up front.
    let r = rows(&mut db, "SELECT v FROM t");
    assert_eq!(r.rows, vec![vec![Value::Int(10)]]);
}

/// 位移删除的自引用父行不得把「随它一起消失的自引用」计为孤儿。
#[test]
fn or_replace_self_fk_sees_final_state() {
    let mut db = Database::in_memory().unwrap();
    run(
            &mut db,
            "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, boss INT, FOREIGN KEY (boss) REFERENCES t(id))",
        );
    run(&mut db, "CREATE UNIQUE INDEX uq_ab ON t (a, b)");
    run(&mut db, "INSERT INTO t VALUES (9, 1, 1, 9)");
    // uq_ab 冲突位移旧行(其 boss=9 指向自身);终态只剩新行,合法。
    run(
        &mut db,
        "INSERT OR REPLACE INTO t (id, a, b, boss) VALUES (5, 1, 1, NULL)",
    );
    let r = rows(&mut db, "SELECT id FROM t");
    assert_eq!(r.rows, vec![vec![Value::Int(5)]]);
}

#[test]
fn upsert_update_raising_autoinc_refreshes_cache() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY AUTOINCREMENT, v INT)",
    );
    run(&mut db, "INSERT INTO t (v) VALUES (1)");
    run(
        &mut db,
        "INSERT INTO t (id, v) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET id = 50",
    );
    // 2..=50 次自增插入全部成功;抬到 50 后不得再撞已占 id。
    for _ in 0..60 {
        match run(&mut db, "INSERT INTO t (v) VALUES (1)") {
            ExecOutcome::Affected(_) => {}
            other => panic!("autoincrement insert must not hit a taken id: {other:?}"),
        }
    }
    let r = rows(&mut db, "SELECT COUNT(*) FROM t");
    assert_eq!(r.rows[0][0], Value::Int(61));
}

/// 同批 placed 行重新供给了被 upsert 更新腾出的父键:父侧 FK 检查
/// 曾只看 updated 的新像,把这种批次误报 FOREIGN KEY failed。
#[test]
fn upsert_parent_fk_accepts_key_resupplied_by_placed_rows() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE p (id INT PRIMARY KEY)");
    run(&mut db, "CREATE TABLE c (pid INT REFERENCES p(id))");
    run(&mut db, "INSERT INTO p VALUES (1)");
    run(&mut db, "INSERT INTO c VALUES (1)");
    run(
        &mut db,
        "INSERT INTO p (id) VALUES (1), (1) ON CONFLICT (id) DO UPDATE SET id = 99",
    );
    // 终态:p 里 99(更新像)与 1(第二行落为普通插入)并存,c.pid=1 仍可解析。
    let r = rows(&mut db, "SELECT id FROM p ORDER BY id");
    assert_eq!(r.rows, vec![vec![Value::Int(1)], vec![Value::Int(99)]]);
    let r = rows(&mut db, "SELECT COUNT(*) FROM c WHERE pid = 1");
    assert_eq!(r.rows, vec![vec![Value::Int(1)]]);
}

/// 不可解析时间戳字符串的有序索引窗口:谓词按 cmp_coerced 判 NULL/未知
/// (0 行),窗口曾因 raw Timestamp<Str 比较放进全部行——结果随索引
/// 存在性漂移。
#[test]
fn unparseable_timestamp_bound_matches_scan_semantics_with_index() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY, ts TIMESTAMP NOT NULL)",
    );
    run(&mut db, "CREATE INDEX i_ts ON t (ts)");
    run(
        &mut db,
        "INSERT INTO t VALUES (1, TIMESTAMP '2026-01-01T00:00:00Z'), \
             (2, TIMESTAMP '2026-06-01T00:00:00Z'), (3, TIMESTAMP '2026-12-01T00:00:00Z')",
    );
    // 无索引路径(比较两侧经 cmp_coerced):不可解析 → NULL → 不成立。
    run(&mut db, "DROP INDEX i_ts");
    let plain = rows(
        &mut db,
        "SELECT id FROM t WHERE ts < '2026-13-01' ORDER BY ts",
    );
    assert_eq!(plain.rows, Vec::<Vec<Value>>::new());
    // 有索引 + ORDER BY ts LIMIT:必须与扫描路径同答案。
    run(&mut db, "CREATE INDEX i_ts ON t (ts)");
    let windowed = rows(
        &mut db,
        "SELECT id FROM t WHERE ts < '2026-13-01' ORDER BY ts LIMIT 2",
    );
    assert_eq!(windowed.rows, Vec::<Vec<Value>>::new());
    // 可解析边界仍走提升,行为不变。
    let hit = rows(
        &mut db,
        "SELECT id FROM t WHERE ts < '2026-07-01T00:00:00Z' ORDER BY ts LIMIT 2",
    );
    assert_eq!(hit.rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
}

/// 相关子查询里外层别名的显式 NULL 列:曾因 lookup_col 的 NULL=缺失
/// 不可区分而回退裸名,JOIN 行上解析到另一个别名的同名列。
#[test]
fn correlated_subquery_explicit_null_does_not_leak_other_alias() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE a (k INT, x INT)");
    run(&mut db, "CREATE TABLE b (k INT, x INT)");
    run(&mut db, "CREATE TABLE s (y INT)");
    run(&mut db, "INSERT INTO a VALUES (1, NULL)");
    run(&mut db, "INSERT INTO b VALUES (1, 5)");
    run(&mut db, "INSERT INTO s VALUES (5)");
    let r = rows(
        &mut db,
        "SELECT z.x FROM a AS z JOIN b ON z.k = b.k \
             WHERE EXISTS (SELECT 1 FROM s WHERE s.y = z.x)",
    );
    // z.x 是 NULL:s.y = NULL 不成立 → 0 行;曾返回 1 行(z.x 借道 b.x=5)。
    assert_eq!(r.rows, Vec::<Vec<Value>>::new());
}

/// EXPLAIN 与执行器门禁同步:`SELECT TOP (n)` 执行路径会改写成 LIMIT,
/// EXPLAIN 曾直接拒;GROUP BY WITH ROLLUP 执行被拒,EXPLAIN 曾静默丢
/// 修饰符给出一个永远不会跑的计划。
#[test]
fn explain_matches_executor_gates() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    run(&mut db, "INSERT INTO t VALUES (1, 10), (2, 20)");
    // TOP:normally executes; EXPLAIN must plan it, not reject it.
    let r = rows(&mut db, "EXPLAIN SELECT TOP (2) id FROM t ORDER BY id");
    assert_eq!(r.columns, vec!["plan".to_string(), "detail".to_string()]);
    // WITH ROLLUP modifier: the executor refuses it; EXPLAIN must too.
    match db.execute("EXPLAIN SELECT v, COUNT(*) FROM t GROUP BY v WITH ROLLUP") {
        Err(_) => {}
        Ok(_) => panic!("EXPLAIN must refuse GROUP BY modifiers like the executor"),
    }
}

#[test]
fn on_conflict_do_update_returning_covers_updated_rows() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, n INT)");
    run(&mut db, "INSERT INTO t VALUES (1, 1)");
    let outcome = db
            .execute(
                "INSERT INTO t VALUES (1, 2), (3, 3) ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n RETURNING id, n",
            )
            .unwrap();
    let ExecOutcome::Rows(r) = outcome else {
        panic!("expected rows");
    };
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(1), Value::Int(2)],
            vec![Value::Int(3), Value::Int(3)],
        ]
    );
}

#[test]
fn on_conflict_do_update_resolved_sql_preserves_the_clause() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id GUID AUTOINCREMENT PRIMARY KEY, k INT UNIQUE, n INT)",
    );
    // Auto-GUID fills a default → the statement is rewritten for
    // replay; the upsert clause must survive the rewrite verbatim.
    run(
        &mut db,
        "INSERT INTO t (k, n) VALUES (1, 1) ON CONFLICT (k) DO UPDATE SET n = EXCLUDED.n",
    );
    run(
        &mut db,
        "INSERT INTO t (k, n) VALUES (1, 7) ON CONFLICT (k) DO UPDATE SET n = EXCLUDED.n",
    );
    assert_eq!(
        rows(&mut db, "SELECT n FROM t").rows,
        vec![vec![Value::Int(7)]]
    );
}

#[test]
fn on_conflict_do_update_fk_checks_cover_updated_rows() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE parent (id INT PRIMARY KEY)");
    run(
        &mut db,
        "CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id))",
    );
    run(&mut db, "INSERT INTO parent VALUES (1), (2)");
    run(&mut db, "INSERT INTO child VALUES (1, 1)");
    // Updating the referenced parent key away must refuse (parent-side
    // FK, same contract as DELETE).
    let e = db
        .execute(
            "INSERT INTO parent VALUES (1) ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id + 2",
        )
        .unwrap_err();
    assert!(e.to_string().to_lowercase().contains("foreign"), "{e}");
    // Updating the child's reference to a live parent is fine.
    run(
        &mut db,
        "INSERT INTO child VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET pid = EXCLUDED.pid",
    );
    assert_eq!(
        rows(&mut db, "SELECT pid FROM child").rows,
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn on_conflict_do_update_rejects_nonsense_loudly() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
    run(&mut db, "INSERT INTO t VALUES (1, 'a')");
    // PG requires the conflict target for DO UPDATE.
    let e = db
        .execute("INSERT INTO t VALUES (1, 'b') ON CONFLICT DO UPDATE SET v = 'x'")
        .unwrap_err();
    assert!(e.to_string().contains("requires a conflict target"), "{e}");
    // OR REPLACE cannot combine.
    let e = db
        .execute("INSERT OR REPLACE INTO t VALUES (1, 'b') ON CONFLICT (id) DO UPDATE SET v = 'x'")
        .unwrap_err();
    assert!(e.to_string().contains("cannot combine"), "{e}");
    // OR IGNORE cannot combine.
    let e = db
        .execute("INSERT OR IGNORE INTO t VALUES (1, 'b') ON CONFLICT (id) DO UPDATE SET v = 'x'")
        .unwrap_err();
    assert!(e.to_string().contains("cannot combine"), "{e}");
    // NEWID() in the update arm would diverge replay.
    let e = db
        .execute("INSERT INTO t VALUES (1, 'b') ON CONFLICT (id) DO UPDATE SET v = NEWID()")
        .unwrap_err();
    assert!(e.to_string().contains("NEWID"), "{e}");
    // Wall-clock functions in the arm are FOLDED to literals by the
    // statement-level preprocessing (deterministic replay) — accepted.
    run(
        &mut db,
        "INSERT INTO t VALUES (1, 'b') ON CONFLICT (id) DO UPDATE SET v = 'x-' || NOW()",
    );
    assert!(rows(&mut db, "SELECT v FROM t WHERE id = 1").rows[0][0]
        .to_string()
        .starts_with("x-"));
    // ROWNUM has no row stream here.
    let e = db
            .execute("INSERT INTO t VALUES (1, 'b') ON CONFLICT (id) DO UPDATE SET v = 'x' WHERE ROWNUM <= 1")
            .unwrap_err();
    assert!(e.to_string().contains("ROWNUM"), "{e}");
    // EXCLUDED inside a subquery of the update arm: not rewritten.
    let e = db
        .execute(
            "INSERT INTO t VALUES (1, 'b') ON CONFLICT (id) DO UPDATE SET v = 'x' \
                 WHERE id IN (SELECT id FROM t WHERE v = EXCLUDED.v)",
        )
        .unwrap_err();
    assert!(e.to_string().contains("EXCLUDED"), "{e}");
    // CHECK constraint on the merged image: enforced, statement aborts.
    run(
        &mut db,
        "CREATE TABLE c (id INT PRIMARY KEY, q INT CHECK (q >= 0))",
    );
    run(&mut db, "INSERT INTO c VALUES (1, 1)");
    let e = db
        .execute("INSERT INTO c VALUES (1, -5) ON CONFLICT (id) DO UPDATE SET q = EXCLUDED.q")
        .unwrap_err();
    assert!(
        e.to_string().contains("CHECK") || e.to_string().contains("check"),
        "{e}"
    );
    assert_eq!(
        rows(&mut db, "SELECT q FROM c").rows,
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn order_by_indexed_key_with_where_on_same_index() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY NOT NULL, v TEXT)",
    );
    run(
        &mut db,
        "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'a'), (4, 'b'), (5, 'a'), (6, 'b')",
    );
    let r = rows(
        &mut db,
        "SELECT id FROM t WHERE id >= 2 AND id <= 9 ORDER BY id LIMIT 3 OFFSET 1",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(3)],
            vec![Value::Int(4)],
            vec![Value::Int(5)]
        ]
    );
    let r = rows(
        &mut db,
        "SELECT id FROM t WHERE id >= 4 AND id <= 9 ORDER BY id LIMIT 3 OFFSET 1",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(5)], vec![Value::Int(6)]]);
    // The probe bounds the walk; the residual filter still runs before
    // OFFSET counts.
    let r = rows(
        &mut db,
        "SELECT id FROM t WHERE id >= 1 AND v = 'b' ORDER BY id LIMIT 1 OFFSET 1",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(4)]]);
    let r = rows(&mut db, "SELECT id FROM t WHERE id = 3 ORDER BY id LIMIT 2");
    assert_eq!(r.rows, vec![vec![Value::Int(3)]]);
}

#[test]
fn indexed_window_caps_exact_probes_and_counts_offset_unread() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY NOT NULL, v INT NOT NULL)",
    );
    run(&mut db, "CREATE INDEX ix_t_v ON t (v)");
    for i in 0..200i64 {
        run(&mut db, &format!("INSERT INTO t VALUES ({i}, {})", i / 2));
    }
    // Exact range probe: the walk stops at the window's end.
    let r = rows(
        &mut db,
        "SELECT id FROM t WHERE id > 150 ORDER BY id LIMIT 5",
    );
    assert_eq!(
        r.rows,
        (151..=155).map(|i| vec![Value::Int(i)]).collect::<Vec<_>>()
    );
    // Tightest bound wins; the subsumed conjunct keeps the probe exact.
    let r = rows(
        &mut db,
        "SELECT id FROM t WHERE id > 150 AND id > 100 ORDER BY id LIMIT 2",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(151)], vec![Value::Int(152)]]);
    // A residual conjunct disables the cap: matches past the first
    // window stay visible.
    let r = rows(
        &mut db,
        "SELECT id FROM t WHERE id > 150 AND id % 10 = 0 ORDER BY id LIMIT 3",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(160)],
            vec![Value::Int(170)],
            vec![Value::Int(180)]
        ]
    );
    // OFFSET inside an exact range counts positions without decoding.
    let r = rows(
        &mut db,
        "SELECT id FROM t WHERE id >= 10 ORDER BY id LIMIT 2 OFFSET 3",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(13)], vec![Value::Int(14)]]);
    // Numeric equality with a differently-typed literal stays a result
    // even though the exact probe replaces the row filter.
    let r = rows(
        &mut db,
        "SELECT id FROM t WHERE v = 50.0 ORDER BY v LIMIT 5",
    );
    assert_eq!(r.rows.len(), 2);
}

#[test]
fn indexed_window_descending_walks_and_pure_skip() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY NOT NULL, v TEXT)",
    );
    for i in 0..100i64 {
        run(&mut db, &format!("INSERT INTO t VALUES ({i}, 'v{i}')"));
    }
    let r = rows(&mut db, "SELECT id FROM t ORDER BY id DESC LIMIT 3");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(99)],
            vec![Value::Int(98)],
            vec![Value::Int(97)]
        ]
    );
    let r = rows(
        &mut db,
        "SELECT id FROM t ORDER BY id DESC LIMIT 2 OFFSET 5",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(94)], vec![Value::Int(93)]]);
    // Keyset in descending order: the reverse range walk.
    let r = rows(
        &mut db,
        "SELECT id FROM t WHERE id < 50 ORDER BY id DESC LIMIT 3",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(49)],
            vec![Value::Int(48)],
            vec![Value::Int(47)]
        ]
    );
    let r = rows(
        &mut db,
        "SELECT id FROM t WHERE id < 50 ORDER BY id DESC LIMIT 2 OFFSET 3",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(46)], vec![Value::Int(45)]]);
    let r = rows(
        &mut db,
        "SELECT id FROM t WHERE id > 90 ORDER BY id DESC LIMIT 2 OFFSET 1",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(98)], vec![Value::Int(97)]]);
    // `LIMIT -1` (pure skip) keeps index order both ways.
    let r = rows(&mut db, "SELECT id FROM t ORDER BY id LIMIT -1 OFFSET 97");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(97)],
            vec![Value::Int(98)],
            vec![Value::Int(99)]
        ]
    );
    let r = rows(
        &mut db,
        "SELECT id FROM t ORDER BY id DESC LIMIT -1 OFFSET 97",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(2)],
            vec![Value::Int(1)],
            vec![Value::Int(0)]
        ]
    );
}

#[test]
fn indexed_window_composite_exact_and_residual() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, v TEXT)",
    );
    run(&mut db, "CREATE INDEX ix_ab ON t (a, b)");
    for i in 0..100i64 {
        run(
            &mut db,
            &format!("INSERT INTO t VALUES ({}, {}, 'x{i}')", i / 10, i % 10),
        );
    }
    // Exact prefix probe: only the window's rows are decoded.
    let r = rows(
        &mut db,
        "SELECT b FROM t WHERE a = 5 ORDER BY a, b LIMIT 3 OFFSET 2",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(2)],
            vec![Value::Int(3)],
            vec![Value::Int(4)]
        ]
    );
    // Residual conjunct on a non-leading column: candidates all scanned,
    // filter applied before OFFSET counts.
    let r = rows(
        &mut db,
        "SELECT b FROM t WHERE a = 5 AND b >= 5 ORDER BY a, b LIMIT 2",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(5)], vec![Value::Int(6)]]);
}

#[test]
fn indexed_window_descending_probe_eq_and_composite_prefix() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY NOT NULL, v INT NOT NULL)",
    );
    run(&mut db, "CREATE INDEX ix_v ON t (v)");
    run(
        &mut db,
        "INSERT INTO t VALUES (1, 2), (2, 2), (3, 1), (4, 3)",
    );
    // DESC window with an equality probe (Eq plan, reverse walk). Ties
    // on the probe key have unspecified order, so compare as a set.
    let r = rows(
        &mut db,
        "SELECT id FROM t WHERE v = 2 ORDER BY v DESC LIMIT 2",
    );
    let mut got: Vec<i64> = r.rows.iter().map(|row| row[0].as_i64().unwrap()).collect();
    got.sort_unstable();
    assert_eq!(got, vec![1, 2]);

    run(
        &mut db,
        "CREATE TABLE c (id INT PRIMARY KEY NOT NULL, a INT NOT NULL, b INT NOT NULL)",
    );
    run(&mut db, "CREATE INDEX ix_ab ON c (a, b)");
    run(
        &mut db,
        "INSERT INTO c VALUES (1, 1, 2), (2, 2, 1), (3, 1, 1), (4, 1, 3), (5, 2, 2)",
    );
    // Composite equality prefix in descending order (Prefix plan, reverse).
    let r = rows(
        &mut db,
        "SELECT id FROM c WHERE a = 1 ORDER BY a DESC, b DESC LIMIT 2",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(4)], vec![Value::Int(1)]]);
}

#[test]
fn order_by_composite_index_walk_and_mixed_direction_fallback() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, v TEXT)",
    );
    run(&mut db, "CREATE INDEX ix_ab ON t (a, b)");
    run(
        &mut db,
        "INSERT INTO t VALUES (1, 1, 'x'), (1, 2, 'y'), (2, 1, 'z'), (2, 2, 'w')",
    );
    let r = rows(&mut db, "SELECT v FROM t ORDER BY a, b LIMIT 3 OFFSET 1");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("y".into())],
            vec![Value::Str("z".into())],
            vec![Value::Str("w".into())]
        ]
    );
    let r = rows(&mut db, "SELECT v FROM t ORDER BY a DESC, b DESC LIMIT 2");
    assert_eq!(
        r.rows,
        vec![vec![Value::Str("w".into())], vec![Value::Str("z".into())]]
    );
    // Mixed directions cannot follow one tree order: generic sort path.
    let r = rows(&mut db, "SELECT v FROM t ORDER BY a ASC, b DESC LIMIT 2");
    assert_eq!(
        r.rows,
        vec![vec![Value::Str("y".into())], vec![Value::Str("x".into())]]
    );
}

#[test]
fn where_or_and_parentheses() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (n INT)");
    for i in 0..5 {
        run(&mut db, &format!("INSERT INTO t VALUES ({i})"));
    }
    let r = rows(
        &mut db,
        "SELECT n FROM t WHERE (n = 1 OR n = 3) AND n < 4 ORDER BY n",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(1)], vec![Value::Int(3)]]);
}

#[test]
fn where_string_comparison_and_not_in() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (s TEXT)");
    run(
        &mut db,
        "INSERT INTO t VALUES ('apple'), ('banana'), ('cherry')",
    );
    let r = rows(&mut db, "SELECT s FROM t WHERE s >= 'banana' ORDER BY s");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("banana".into())],
            vec![Value::Str("cherry".into())],
        ]
    );
    let r = rows(
        &mut db,
        "SELECT s FROM t WHERE s NOT IN ('apple', 'cherry')",
    );
    assert_eq!(r.rows, vec![vec![Value::Str("banana".into())]]);
}

#[test]
fn unary_minus_and_parenthesized_arithmetic() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (a INT)");
    run(&mut db, "INSERT INTO t VALUES (7)");
    let r = rows(&mut db, "SELECT -(a + 1), (a + 1) * 2 FROM t");
    assert_eq!(r.rows, vec![vec![Value::Int(-8), Value::Int(16)]]);
}

#[test]
fn limit_zero_and_offset_beyond_end() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (n INT)");
    for i in 0..5 {
        run(&mut db, &format!("INSERT INTO t VALUES ({i})"));
    }
    let r = rows(&mut db, "SELECT n FROM t ORDER BY n LIMIT 0");
    assert!(r.rows.is_empty());
    let r = rows(&mut db, "SELECT n FROM t ORDER BY n LIMIT 5 OFFSET 100");
    assert!(r.rows.is_empty());
}

#[test]
fn where_on_missing_column_is_empty_not_error() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (n INT)");
    run(&mut db, "INSERT INTO t VALUES (1)");
    let r = rows(&mut db, "SELECT n FROM t WHERE ghost = 1");
    assert!(r.rows.is_empty());
}

#[test]
fn boolean_literals_in_where() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (n INT)");
    for i in 0..4 {
        run(&mut db, &format!("INSERT INTO t VALUES ({i})"));
    }
    let r = rows(&mut db, "SELECT n FROM t WHERE TRUE AND n = 2");
    assert_eq!(r.rows, vec![vec![Value::Int(2)]]);
    let r = rows(&mut db, "SELECT n FROM t WHERE FALSE OR n = 3");
    assert_eq!(r.rows, vec![vec![Value::Int(3)]]);
    let r = rows(&mut db, "SELECT n FROM t WHERE FALSE");
    assert!(r.rows.is_empty());
}

// ---- set operations ------------------------------------------------

#[test]
fn union_column_count_mismatch_rejected() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE a (x INT, y INT)");
    run(&mut db, "CREATE TABLE b (x INT)");
    let e = db
        .execute("SELECT x, y FROM a UNION SELECT x FROM b")
        .unwrap_err();
    assert!(e.to_string().contains("different column counts"), "{e}");
}

// ---- DDL / constraints / system surfaces ---------------------------

#[test]
fn auto_increment_underscore_variant() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)",
    );
    run(&mut db, "INSERT INTO t (v) VALUES ('a'), ('b')");
    let r = rows(&mut db, "SELECT id FROM t ORDER BY id");
    assert_eq!(r.rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
}

#[test]
fn unique_column_allows_multiple_nulls() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY, email TEXT UNIQUE)",
    );
    run(
        &mut db,
        "INSERT INTO t VALUES (1, 'a@x'), (2, NULL), (3, NULL)",
    );
    let r = rows(&mut db, "SELECT id FROM t ORDER BY id");
    assert_eq!(r.rows.len(), 3);
}

#[test]
fn insert_column_subset_null_fills_the_rest() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (a INT, b TEXT)");
    run(&mut db, "INSERT INTO t (b) VALUES ('x')");
    let r = rows(&mut db, "SELECT a, b FROM t");
    assert_eq!(r.rows, vec![vec![Value::Null, Value::Str("x".into())]]);
}

#[test]
fn insert_boolean_and_null_literals_roundtrip() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (a INT, b INT, c INT)");
    run(&mut db, "INSERT INTO t VALUES (TRUE, NULL, FALSE)");
    let r = rows(&mut db, "SELECT a, b, c FROM t");
    assert_eq!(
        r.rows,
        vec![vec![Value::Bool(true), Value::Null, Value::Bool(false)]]
    );
}

#[test]
fn update_multiple_assignments() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (a INT, b TEXT)");
    run(&mut db, "INSERT INTO t VALUES (1, 'x')");
    match run(&mut db, "UPDATE t SET a = a + 1, b = 'z' WHERE a = 1") {
        ExecOutcome::Affected(1) => {}
        o => panic!("{o:?}"),
    }
    let r = rows(&mut db, "SELECT a, b FROM t");
    assert_eq!(r.rows, vec![vec![Value::Int(2), Value::Str("z".into())]]);
}

#[test]
fn pragma_is_accepted_noop() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (a INT)");
    match run(&mut db, "PRAGMA user_version") {
        ExecOutcome::Affected(0) => {}
        o => panic!("{o:?}"),
    }
}

#[test]
fn sqlite_master_lists_tables() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE alpha (x INT)");
    run(&mut db, "CREATE TABLE beta (y INT)");
    let r = rows(&mut db, "SELECT name FROM sqlite_master ORDER BY name");
    let names: Vec<String> = r
        .rows
        .iter()
        .map(|row| row[0].as_str().unwrap().into())
        .collect();
    assert_eq!(names, vec!["alpha", "beta"]);
}

#[test]
fn create_view_rejected() {
    // Plain views are supported now; the explicit-rejection surface that
    // remains is materialized / decorated views (same red line class:
    // never silently accept-and-ignore).
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT)");
    // CREATE MATERIALIZED VIEW has since been implemented: it
    // materializes now and refuses writes (REFRESH recomputes).
    run(&mut db, "CREATE MATERIALIZED VIEW v AS SELECT * FROM t");
    assert!(db.execute("INSERT INTO v VALUES (1)").is_err());
    assert!(db.execute("REFRESH MATERIALIZED VIEW v").is_ok());
}

#[test]
fn create_table_rejects_ignored_layout_clauses() {
    // These parsed and were dropped on the floor before (the same red
    // line the storage/partitioning guard exists for): inheriting
    // columns, rowid layout, Hive/Redshift placement/partitioning and
    // constraint decorations all change what the statement means.
    let mut db = Database::in_memory().unwrap();
    for sql in [
        "CREATE TABLE t (a INT) INHERITS (base)",
        "CREATE TABLE t (a INT) WITHOUT ROWID",
        "CREATE TABLE t (a INT) ON COMMIT DROP",
        "CREATE TABLE t (a INT) LOCATION '/x'",
        "CREATE TABLE t (a INT) STORED AS PARQUET",
        "CREATE TABLE t (a INT) CLUSTERED BY (a) INTO 4 BUCKETS",
        "CREATE TABLE t (a INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','",
        "CREATE TABLE t (a INT, UNIQUE NULLS NOT DISTINCT (a))",
        "CREATE TABLE t (a INT UNIQUE DEFERRABLE)",
        "CREATE TABLE t (a INT UNIQUE NOT ENFORCED)",
        "CREATE TABLE t (a INT PRIMARY KEY DEFERRABLE)",
        "CREATE TABLE t (a INT, PRIMARY KEY (a) INITIALLY DEFERRED)",
    ] {
        assert!(db.execute(sql).is_err(), "accepted silently: {sql}");
    }
    assert!(db.execute("SELECT * FROM t").is_err());
    run(&mut db, "CREATE TABLE p (id INT PRIMARY KEY)");
    let e = db
        .execute("CREATE TABLE c (pid INT REFERENCES p(id) MATCH FULL)")
        .unwrap_err();
    assert!(e.to_string().contains("MATCH FULL"), "{e}");
    // No-op spellings describe the behavior the engine already has.
    run(
        &mut db,
        "CREATE TABLE ok (a INT UNIQUE NOT DEFERRABLE, b INT, UNIQUE NULLS DISTINCT (b))",
    );
    run(
        &mut db,
        "CREATE TABLE ok2 (a INT, PRIMARY KEY (a) INITIALLY IMMEDIATE)",
    );
}

#[test]
fn qualified_wildcard_rejected() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (a INT)");
    assert!(db.execute("SELECT t.* FROM t").is_err());
}

#[test]
fn drop_index_lifecycle() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (a INT)");
    run(&mut db, "CREATE INDEX ia ON t (a)");
    assert_eq!(db.catalog()[0].indexes, vec!["ia".to_string()]);
    run(&mut db, "DROP INDEX ia");
    assert!(db.catalog()[0].indexes.is_empty());
    assert!(db.execute("DROP INDEX ia").is_err());
    run(&mut db, "DROP INDEX IF EXISTS ia"); // no error
}

#[test]
fn drop_index_recycles_json_path_tree() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE ev (id INT PRIMARY KEY, data TEXT)");
    run(
        &mut db,
        "INSERT INTO ev VALUES (1, '{\"k\":\"x\"}'), (2, '{\"k\":\"y\"}')",
    );
    run(
        &mut db,
        "CREATE INDEX data ON ev (JSON_EXTRACT(data, '$.k'))",
    );
    run(&mut db, "DROP INDEX data");
    // 残留路径树曾被误当 data 列的标量树:同列查询静默丢行。
    let r = rows(&mut db, "SELECT id FROM ev WHERE data = '{\"k\":\"x\"}'");
    assert_eq!(r.rows, vec![vec![Value::Int(1)]]);
    // 同名索引必须可重建(残留 root 会报 already in use)。
    run(
        &mut db,
        "CREATE INDEX data ON ev (JSON_EXTRACT(data, '$.k'))",
    );
}

#[test]
fn cte_named_like_a_table_still_reads_the_base_object() {
    // 授权 walker 曾把 CTE 自名遮蔽进 body,同名基表读完全逃过
    // stmt_read_targets。引擎语义:body(与非递归/anchor)先于 CTE
    // 入作用域,读的是基表。
    let sql =
        "WITH secret AS (SELECT * FROM secret UNION SELECT * FROM secret) SELECT * FROM secret";
    let mut stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
    let stmt = stmts.swap_remove(0);
    let objects = |n: &str| n == "secret";
    let targets = Database::stmt_read_targets(&stmt, &objects).unwrap();
    assert!(
        targets.iter().any(|t| t == "secret"),
        "same-named base reads must stay visible: {targets:?}"
    );
    // 纯 CTE 名(不撞表)仍被遮蔽——正常递归 CTE 不产生伪读目标。
    let sql = "WITH nums AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM nums) SELECT * FROM nums";
    let mut stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
    let stmt = stmts.swap_remove(0);
    let targets = Database::stmt_read_targets(&stmt, &|_| false).unwrap();
    assert!(
        !targets.iter().any(|t| t == "nums"),
        "a pure CTE self-reference is not a base read: {targets:?}"
    );
}

/// 第六轮审查回归:名为 `old` 的单列索引建在另一列上时,RENAME
/// COLUMN 把被改名列自己的约束树误判为"别人的树"而不随改名 rekey——
/// 主键树搁浅后唯一性完全失效(重复主键静默入库),副本回放端却强制
/// 唯一 → 集群分叉。
#[test]
fn rename_column_own_tree_survives_same_named_index_on_other_column() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (old INT PRIMARY KEY, other INT)");
    // 索引名叫 old,树在 root "other"(单列索引的 root = 列名)。
    run(&mut db, "CREATE INDEX old ON t (other)");
    run(&mut db, "INSERT INTO t VALUES (1, 5)");
    run(&mut db, "ALTER TABLE t RENAME COLUMN old TO z");
    // 主键树必须随改名生效:重复 z 仍被拒。
    assert!(db.execute("INSERT INTO t VALUES (1, 6)").is_err());
    run(&mut db, "INSERT INTO t VALUES (2, 6)");
    let r = rows(&mut db, "SELECT z, other FROM t ORDER BY z");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(1), Value::Int(5)],
            vec![Value::Int(2), Value::Int(6)]
        ]
    );
    // 索引 old(在 other 列上)不受影响。
    let r = rows(&mut db, "SELECT z FROM t WHERE other = 5");
    assert_eq!(r.rows, vec![vec![Value::Int(1)]]);
}

/// 第六轮审查回归:复合 ALTER 的 validate 未前置 apply 侧的 AddColumn
/// 拒绝(CHECK 引用未知列/generated 选项/回填值违反 CHECK),早前
/// 操作已独立落盘后语句才失败 → 半应用。
#[test]
fn composite_alter_validates_add_column_rejections_upfront() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (a INT PRIMARY KEY, b INT)");
    run(&mut db, "INSERT INTO t VALUES (1, 10)");
    let e = db
        .execute("ALTER TABLE t DROP COLUMN b, ADD COLUMN d INT CHECK (d > nosuchcol)")
        .unwrap_err()
        .to_string();
    assert!(e.contains("unknown column"), "{e}");
    // b 未被删(整条语句原子拒绝)。
    let r = rows(&mut db, "SELECT b FROM t");
    assert_eq!(r.rows, vec![vec![Value::Int(10)]]);
    let e = db
        .execute("ALTER TABLE t DROP COLUMN b, ADD COLUMN d INT GENERATED ALWAYS AS (1)")
        .unwrap_err()
        .to_string();
    assert!(e.contains("generated"), "{e}");
    assert_eq!(
        rows(&mut db, "SELECT b FROM t").rows,
        vec![vec![Value::Int(10)]]
    );
    let e = db
        .execute(
            "ALTER TABLE t ADD COLUMN e INT DEFAULT 1, ADD COLUMN d INT DEFAULT -5 CHECK (d > 0)",
        )
        .unwrap_err()
        .to_string();
    assert!(e.contains("CHECK"), "{e}");
    // e 未落盘:声明列仍只有 a/b(schemaless 未知列读 NULL,不能当
    // 存在性证明;用值数对不上来钉)。
    let e2 = db
        .execute("INSERT INTO t VALUES (2, 20, 3)")
        .unwrap_err()
        .to_string();
    assert!(e2.contains("column"), "{e2}");
}

/// 第六轮审查回归:FK 前向引用可构造声明级环,dump 回退按目录序输出
/// 导致恢复回放必败——CREATE 现在要求被引用表已存在(自引用除外)。
#[test]
fn create_rejects_forward_fk_references() {
    let mut db = Database::in_memory().unwrap();
    let e = db
        .execute("CREATE TABLE t1 (a INT PRIMARY KEY, b INT REFERENCES t2 (m))")
        .unwrap_err()
        .to_string();
    assert!(e.contains("does not exist"), "{e}");
    // 顺序建表与自引用不受影响。
    run(&mut db, "CREATE TABLE t2 (m INT PRIMARY KEY)");
    run(
        &mut db,
        "CREATE TABLE t3 (a INT PRIMARY KEY, b INT REFERENCES t2 (m))",
    );
    run(
        &mut db,
        "CREATE TABLE t4 (a INT PRIMARY KEY, b INT REFERENCES t4 (a))",
    );
    // 引用已存在表的未知列也被拒。
    let e = db
        .execute("CREATE TABLE t5 (a INT PRIMARY KEY, b INT REFERENCES t2 (nope))")
        .unwrap_err()
        .to_string();
    assert!(e.contains("unknown column"), "{e}");
}

/// 第六轮审查回归:嵌入式路径可对内部用户表做 DDL(CREATE INDEX/
/// ALTER/DROP),建出的索引不进 dump 却进 schema hash → 与集群摘要
/// 分叉。引擎层与网络层同拒。
#[test]
fn internal_tables_reject_engine_level_ddl() {
    let mut db = Database::in_memory().unwrap();
    assert!(db
        .execute("CREATE INDEX ix_u ON docsql_users (name)")
        .is_err());
    assert!(db
        .execute("ALTER TABLE docsql_users ADD COLUMN evil TEXT")
        .is_err());
    assert!(db.execute("DROP TABLE docsql_users").is_err());
    assert!(db
        .execute("ALTER TABLE _cluster_log ADD COLUMN x INT")
        .is_err());
}

#[test]
fn alter_rewrite_keeps_followup_metadata_ops() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY)");
    // ADD COLUMN b(带 DEFAULT 触发全表重写)之后的纯元数据 ADD COLUMN c
    // 曾随「rewrote 已保存」被静默丢弃。
    run(
        &mut db,
        "ALTER TABLE t ADD COLUMN b INT DEFAULT 7, ADD COLUMN c INT",
    );
    run(&mut db, "INSERT INTO t VALUES (2, 20, 30)");
    let r = rows(&mut db, "SELECT c FROM t");
    assert_eq!(r.rows, vec![vec![Value::Int(30)]]);
    // DROP COLUMN + ADD COLUMN 的混合形态同样收敛。
    run(
        &mut db,
        "ALTER TABLE t DROP COLUMN b, ADD COLUMN d INT CHECK (d >= 0)",
    );
    let m = db.execute("INSERT INTO t VALUES (3, -1)");
    assert!(m.is_err(), "CHECK on the trailing ADD COLUMN must persist");
}

#[test]
fn ctas_explicit_column_list_is_rejected() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE src (x INT)");
    let m = db
        .execute("CREATE TABLE ct (renamed INT) AS SELECT x FROM src")
        .expect_err("column list on CTAS must be refused, not ignored");
    assert!(m.to_string().contains("explicit column list"), "{m}");
}

#[test]
fn alter_rename_column_swap_via_vacated_name() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (a INT PRIMARY KEY)");
    // 校验器曾不随 rename 演化模拟 roots,换名组合被假拒绝。
    run(
        &mut db,
        "ALTER TABLE t RENAME COLUMN a TO b, RENAME COLUMN b TO a",
    );
    run(&mut db, "INSERT INTO t VALUES (1)");
    let r = rows(&mut db, "SELECT a FROM t");
    assert_eq!(r.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn is_write_statement_classification() {
    assert!(!Database::is_write_statement("SELECT 1"));
    assert!(!Database::is_write_statement("PRAGMA foo"));
    assert!(Database::is_write_statement("INSERT INTO t VALUES (1)"));
    assert!(Database::is_write_statement("UPDATE t SET a = 1"));
    assert!(Database::is_write_statement("DELETE FROM t"));
    assert!(Database::is_write_statement("CREATE TABLE t (a INT)"));
    assert!(Database::is_write_statement("DROP TABLE t"));
    // Unparseable input is conservatively treated as a write.
    assert!(Database::is_write_statement("SELEC nope"));
}

// ---- SQL completeness: silent-error fixes -------------------------

#[test]
fn distinct_dedups_rows_and_aggregate_inputs() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE dup (g TEXT)");
    run(&mut db, "INSERT INTO dup VALUES ('x'), ('x'), ('y')");
    let r = rows(&mut db, "SELECT DISTINCT g FROM dup ORDER BY g");
    assert_eq!(
        r.rows,
        vec![vec![Value::Str("x".into())], vec![Value::Str("y".into())]]
    );
    let r = rows(
        &mut db,
        "SELECT COUNT(DISTINCT g), SUM(DISTINCT 1) FROM dup",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(2), Value::Int(1)]]);
}

#[test]
fn from_less_where_filters() {
    let mut db = Database::in_memory().unwrap();
    let r = rows(&mut db, "SELECT 1 WHERE FALSE");
    assert!(r.rows.is_empty());
    let r = rows(&mut db, "SELECT 1 WHERE TRUE");
    assert_eq!(r.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn cte_with_and_chained_ctes() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (n INT)");
    run(&mut db, "INSERT INTO t VALUES (1), (2), (3), (4)");
    let r = rows(
        &mut db,
        "WITH big AS (SELECT n FROM t WHERE n > 2) SELECT n FROM big ORDER BY n",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(3)], vec![Value::Int(4)]]);
    // A later CTE can read an earlier one.
    let r = rows(
            &mut db,
            "WITH a AS (SELECT n FROM t), b AS (SELECT n FROM a WHERE n >= 3) SELECT COUNT(n) AS c FROM b",
        );
    assert_eq!(r.rows, vec![vec![Value::Int(2)]]);
}

#[test]
fn order_by_ordinal_and_nulls_position() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (s TEXT, n INT)");
    run(
        &mut db,
        "INSERT INTO t VALUES ('a', 10), ('b', 30), ('c', 20)",
    );
    // ORDER BY 2 = by n.
    let r = rows(&mut db, "SELECT s, n FROM t ORDER BY 2 DESC");
    assert_eq!(r.rows[0], vec![Value::Str("b".into()), Value::Int(30)]);
    // NULLS FIRST/LAST control placement.
    run(&mut db, "INSERT INTO t (s) VALUES ('z')");
    let r = rows(&mut db, "SELECT s FROM t ORDER BY n NULLS LAST");
    assert_eq!(r.rows[3], vec![Value::Str("z".into())]);
    let r = rows(&mut db, "SELECT s FROM t ORDER BY n NULLS FIRST LIMIT 1");
    assert_eq!(r.rows[0], vec![Value::Str("z".into())]);
}

#[test]
fn order_by_non_output_expression() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (g TEXT)");
    run(&mut db, "INSERT INTO t VALUES ('xx'), ('yyy'), ('y')");
    let r = rows(&mut db, "SELECT g FROM t ORDER BY LENGTH(g), g");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("y".into())],
            vec![Value::Str("xx".into())],
            vec![Value::Str("yyy".into())],
        ]
    );
}

// ---- SQL completeness: DML semantics ------------------------------

#[test]
fn create_table_as_select_copies_shape_and_rows() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE src (id INT, name TEXT)");
    run(
        &mut db,
        "INSERT INTO src VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    );
    run(
        &mut db,
        "CREATE TABLE dst AS SELECT id, name FROM src WHERE id < 3",
    );
    let r = rows(&mut db, "SELECT id, name FROM dst ORDER BY id");
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0], vec![Value::Int(1), Value::Str("a".into())]);
    // The new table is fully writable.
    run(&mut db, "INSERT INTO dst VALUES (9, 'nine')");
    let r = rows(&mut db, "SELECT COUNT(id) FROM dst");
    assert_eq!(r.rows, vec![vec![Value::Int(3)]]);
}

#[test]
fn on_conflict_do_nothing_and_or_ignore() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
    run(&mut db, "INSERT INTO t VALUES (1, 'a')");
    match run(
        &mut db,
        "INSERT INTO t VALUES (1, 'dup'), (2, 'new') ON CONFLICT DO NOTHING",
    ) {
        ExecOutcome::Affected(1) => {} // only the non-conflicting row lands
        o => panic!("{o:?}"),
    }
    let r = rows(&mut db, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(1), Value::Str("a".into())],
            vec![Value::Int(2), Value::Str("new".into())],
        ]
    );
    // SQLite spelling.
    run(&mut db, "INSERT OR IGNORE INTO t VALUES (2, 'ignored')");
    let r = rows(&mut db, "SELECT v FROM t WHERE id = 2");
    assert_eq!(r.rows, vec![vec![Value::Str("new".into())]]);
}

#[test]
fn replace_into_replaces_conflicting_rows() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
    run(&mut db, "INSERT INTO t VALUES (1, 'old'), (2, 'keep')");
    match run(&mut db, "REPLACE INTO t VALUES (1, 'new')") {
        ExecOutcome::Affected(1) => {}
        o => panic!("{o:?}"),
    }
    let r = rows(&mut db, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(1), Value::Str("new".into())],
            vec![Value::Int(2), Value::Str("keep".into())],
        ]
    );
}

#[test]
fn update_from_join_semantics() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT, v TEXT)");
    run(&mut db, "CREATE TABLE ext (id INT, v TEXT)");
    run(&mut db, "INSERT INTO t VALUES (1, 'x'), (2, 'y'), (3, 'z')");
    run(&mut db, "INSERT INTO ext VALUES (1, 'one'), (3, 'three')");
    match run(
        &mut db,
        "UPDATE t SET v = ext.v FROM ext WHERE t.id = ext.id",
    ) {
        ExecOutcome::Affected(2) => {}
        o => panic!("{o:?}"),
    }
    let r = rows(&mut db, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(1), Value::Str("one".into())],
            vec![Value::Int(2), Value::Str("y".into())],
            vec![Value::Int(3), Value::Str("three".into())],
        ]
    );
}

#[test]
fn delete_using_join_semantics() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT)");
    run(&mut db, "CREATE TABLE ext (id INT, drop INT)");
    run(&mut db, "INSERT INTO t VALUES (1), (2), (3)");
    run(&mut db, "INSERT INTO ext VALUES (1, 1), (3, 1)");
    match run(
        &mut db,
        "DELETE FROM t USING ext WHERE t.id = ext.id AND ext.drop = 1",
    ) {
        ExecOutcome::Affected(2) => {}
        o => panic!("{o:?}"),
    }
    let r = rows(&mut db, "SELECT id FROM t");
    assert_eq!(r.rows, vec![vec![Value::Int(2)]]);
}

#[test]
fn insert_into_select() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE src (n INT)");
    run(&mut db, "CREATE TABLE dst (n INT)");
    run(&mut db, "INSERT INTO src VALUES (1), (2), (3)");
    match run(&mut db, "INSERT INTO dst SELECT n FROM src WHERE n >= 2") {
        ExecOutcome::Affected(2) => {}
        o => panic!("{o:?}"),
    }
    let r = rows(&mut db, "SELECT n FROM dst ORDER BY n");
    assert_eq!(r.rows, vec![vec![Value::Int(2)], vec![Value::Int(3)]]);
}

#[test]
fn truncate_clears_rows_keeps_shape() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
    run(&mut db, "INSERT INTO t VALUES (1, 'a'), (2, 'b')");
    run(&mut db, "TRUNCATE TABLE t");
    let r = rows(&mut db, "SELECT id FROM t");
    assert!(r.rows.is_empty());
    // PK still enforced after truncation.
    run(&mut db, "INSERT INTO t VALUES (1, 'again')");
    let e = db.execute("INSERT INTO t VALUES (1, 'clash')").unwrap_err();
    assert!(e.to_string().contains("UNIQUE"), "{e}");
}

#[test]
fn drop_refuses_dangling_views_and_cascade_cleans_up() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (a INT)");
    run(&mut db, "CREATE VIEW v AS SELECT * FROM t");
    run(&mut db, "CREATE VIEW v2 AS SELECT * FROM v");
    // Without CASCADE the drop refuses instead of leaving broken views.
    let e = db.execute("DROP TABLE t").unwrap_err();
    assert!(e.to_string().contains("referenced by VIEW"), "{e}");
    let e = db.execute("DROP VIEW v").unwrap_err();
    assert!(e.to_string().contains("referenced by VIEW"), "{e}");
    // Sanity: nothing was dropped by the refused statements.
    run(&mut db, "SELECT * FROM v2");
    run(&mut db, "DROP TABLE t CASCADE");
    assert!(db.execute("SELECT * FROM v").is_err());
    assert!(db.execute("SELECT * FROM v2").is_err());
    // View-of-view cascade drops the whole chain.
    run(&mut db, "CREATE TABLE u (a INT)");
    run(&mut db, "CREATE VIEW w1 AS SELECT * FROM u");
    run(&mut db, "CREATE VIEW w2 AS SELECT * FROM w1");
    run(&mut db, "DROP VIEW w1 CASCADE");
    assert!(db.execute("SELECT * FROM w2").is_err());
    // IF EXISTS with a missing name is not blocked by anything.
    run(&mut db, "DROP TABLE IF EXISTS missing CASCADE");
}

#[test]
fn drop_table_cascade_removes_referencing_fks() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE p (id INT PRIMARY KEY)");
    run(&mut db, "CREATE TABLE c (pid INT REFERENCES p(id))");
    let e = db.execute("DROP TABLE p").unwrap_err();
    assert!(e.to_string().contains("FOREIGN KEY"), "{e}");
    assert!(db.execute("DROP TABLE p PURGE").is_err());
    run(&mut db, "DROP TABLE p CASCADE");
    // The child table stays; only its constraint on the vanished
    // parent goes (PostgreSQL CASCADE semantics).
    run(&mut db, "INSERT INTO c VALUES (99)");
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM c").rows[0][0],
        Value::Int(1)
    );
}

#[test]
fn truncate_cascade_truncates_referencing_tables() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE p (id INT PRIMARY KEY)");
    run(&mut db, "CREATE TABLE c (pid INT REFERENCES p(id))");
    run(&mut db, "CREATE TABLE g (cpid INT REFERENCES c(pid))");
    run(&mut db, "INSERT INTO p VALUES (1)");
    run(&mut db, "INSERT INTO c VALUES (1)");
    // Plain TRUNCATE still refuses to orphan child rows.
    assert!(db.execute("TRUNCATE TABLE p").is_err());
    run(&mut db, "TRUNCATE TABLE p CASCADE");
    for t in ["p", "c", "g"] {
        assert_eq!(
            rows(&mut db, &format!("SELECT COUNT(*) FROM {t}")).rows[0][0],
            Value::Int(0),
            "{t}"
        );
    }
    // Explicitly written options the engine cannot honor refuse loudly.
    assert!(db.execute("TRUNCATE TABLE p CONTINUE IDENTITY").is_err());
    assert!(db.execute("TRUNCATE TABLE ONLY p").is_err());
    assert!(db.execute("TRUNCATE TABLE p PARTITION (x)").is_err());
    assert!(db.execute("TRUNCATE TABLE p ON CLUSTER c").is_err());
    assert!(db.execute("TRUNCATE TABLE p RESTART IDENTITY").is_ok());
    run(&mut db, "TRUNCATE TABLE p, c"); // comma list still supported
}

#[test]
fn drop_view_cleans_stale_grants() {
    // A grant on a dropped view must not survive to a later view of the
    // same name (privilege persistence across DROP/CREATE).
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (a INT)");
    run(&mut db, "CREATE VIEW v AS SELECT * FROM t");
    run(&mut db, "CREATE USER u PASSWORD 'pw123456'");
    run(&mut db, "GRANT SELECT ON v TO u");
    let count = |db: &mut Database| -> i64 {
        match rows(db, "SELECT COUNT(*) FROM docsql_grants WHERE tbl = 'v'").rows[0][0] {
            Value::Int(n) => n,
            ref other => panic!("{other:?}"),
        }
    };
    assert_eq!(count(&mut db), 1);
    run(&mut db, "DROP VIEW v");
    assert_eq!(count(&mut db), 0);
}

// ---- SQL completeness: predicates & expressions -------------------

#[test]
fn is_null_and_is_not_null() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT, s TEXT)");
    run(
        &mut db,
        "INSERT INTO t (id, s) VALUES (1, 'a'), (2, NULL), (3, 'c')",
    );
    let r = rows(&mut db, "SELECT id FROM t WHERE s IS NULL");
    assert_eq!(r.rows, vec![vec![Value::Int(2)]]);
    let r = rows(&mut db, "SELECT id FROM t WHERE s IS NOT NULL ORDER BY id");
    assert_eq!(r.rows, vec![vec![Value::Int(1)], vec![Value::Int(3)]]);
}

#[test]
fn between_and_not_between() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (n INT)");
    run(&mut db, "INSERT INTO t VALUES (1), (5), (10), (20)");
    let r = rows(&mut db, "SELECT n FROM t WHERE n BETWEEN 5 AND 10");
    assert_eq!(r.rows, vec![vec![Value::Int(5)], vec![Value::Int(10)]]);
    let r = rows(
        &mut db,
        "SELECT n FROM t WHERE n NOT BETWEEN 5 AND 10 ORDER BY n",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(1)], vec![Value::Int(20)]]);
}

#[test]
fn like_patterns_and_escape() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (s TEXT)");
    run(
        &mut db,
        "INSERT INTO t VALUES ('apple'), ('maple'), ('apricot'), ('a%b')",
    );
    let r = rows(&mut db, "SELECT s FROM t WHERE s LIKE 'a%' ORDER BY s");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("a%b".into())],
            vec![Value::Str("apple".into())],
            vec![Value::Str("apricot".into())],
        ]
    );
    // '_' matches exactly one char.
    let r = rows(&mut db, "SELECT s FROM t WHERE s LIKE 'appl_'");
    assert_eq!(r.rows, vec![vec![Value::Str("apple".into())]]);
    // ESCAPE makes '%' literal.
    let r = rows(&mut db, "SELECT s FROM t WHERE s LIKE 'a!%b' ESCAPE '!'");
    assert_eq!(r.rows, vec![vec![Value::Str("a%b".into())]]);
    let r = rows(&mut db, "SELECT s FROM t WHERE s NOT LIKE 'a%' ORDER BY s");
    assert_eq!(r.rows, vec![vec![Value::Str("maple".into())]]);
}

#[test]
fn not_operator() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT)");
    run(&mut db, "INSERT INTO t VALUES (1), (2)");
    let r = rows(&mut db, "SELECT id FROM t WHERE NOT (id = 1)");
    assert_eq!(r.rows, vec![vec![Value::Int(2)]]);
}

#[test]
fn case_searched_and_simple() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (n INT)");
    run(&mut db, "INSERT INTO t VALUES (1), (5), (10)");
    let r = rows(
            &mut db,
            "SELECT CASE WHEN n >= 10 THEN 'big' WHEN n >= 5 THEN 'mid' ELSE 'small' END AS bucket FROM t",
        );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("small".into())],
            vec![Value::Str("mid".into())],
            vec![Value::Str("big".into())],
        ]
    );
    let r = rows(
            &mut db,
            "SELECT CASE n WHEN 1 THEN 'one' WHEN 5 THEN 'five' ELSE '?' END AS word FROM t WHERE n < 10",
        );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("one".into())],
            vec![Value::Str("five".into())],
        ]
    );
}

#[test]
fn cast_conversions() {
    let mut db = Database::in_memory().unwrap();
    let r = rows(&mut db, "SELECT CAST('42' AS INT) + 1, CAST(7 AS TEXT) || '!', CAST(4.0 AS TEXT), CAST(1 AS BOOLEAN), CAST('2.5' AS FLOAT)");
    assert_eq!(
        r.rows,
        vec![vec![
            Value::Int(43),
            Value::Str("7!".into()),
            Value::Str("4.0".into()),
            Value::Bool(true),
            Value::Float(2.5),
        ]]
    );
}

#[test]
fn scalar_function_library() {
    let mut db = Database::in_memory().unwrap();
    let r = rows(
            &mut db,
            "SELECT UPPER('aB'), LOWER('Cd'), LENGTH('héllo'), ABS(-5), ROUND(2.567, 2), COALESCE(NULL, 'd'), IFNULL(NULL, 9), NULLIF(1, 1), NULLIF(2, 1), SUBSTR('abcdef', 2, 3), TRIM('  x  '), CONCAT('a', 'b'), TYPEOF(1), TYPEOF('x'), TYPEOF(NULL)",
        );
    assert_eq!(
        r.rows,
        vec![vec![
            Value::Str("AB".into()),
            Value::Str("cd".into()),
            Value::Int(5),
            Value::Int(5),
            Value::Float(2.57),
            Value::Str("d".into()),
            Value::Int(9),
            Value::Null,
            Value::Int(2),
            Value::Str("bcd".into()),
            Value::Str("x".into()),
            Value::Str("ab".into()),
            Value::Str("integer".into()),
            Value::Str("text".into()),
            Value::Str("null".into()),
        ]]
    );
}

#[test]
fn string_concat_and_modulo() {
    let mut db = Database::in_memory().unwrap();
    let r = rows(&mut db, "SELECT 'a' || 'b' || 'c', 7 % 3, 7.5 % 2, 5 % 0");
    assert_eq!(
        r.rows,
        vec![vec![
            Value::Str("abc".into()),
            Value::Int(1),
            Value::Float(1.5),
            Value::Null,
        ]]
    );
}

// ---- SQL completeness: subqueries ----------------------------------

#[test]
fn in_subquery_predicate() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE a (n INT)");
    run(&mut db, "CREATE TABLE b (n INT)");
    run(&mut db, "INSERT INTO a VALUES (1), (2), (3)");
    run(&mut db, "INSERT INTO b VALUES (2), (3)");
    let r = rows(
        &mut db,
        "SELECT n FROM a WHERE n IN (SELECT n FROM b) ORDER BY n",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(2)], vec![Value::Int(3)]]);
    let r = rows(
        &mut db,
        "SELECT n FROM a WHERE n NOT IN (SELECT n FROM b) ORDER BY n",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn exists_predicates() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE a (n INT)");
    run(&mut db, "CREATE TABLE b (n INT)");
    run(&mut db, "INSERT INTO a VALUES (1)");
    run(&mut db, "INSERT INTO b VALUES (2)");
    let r = rows(
        &mut db,
        "SELECT n FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.n > 1)",
    );
    assert_eq!(r.rows.len(), 1);
    let r = rows(
        &mut db,
        "SELECT n FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.n > 99)",
    );
    assert_eq!(r.rows.len(), 1);
}

#[test]
fn scalar_subquery_in_where_and_projection() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (n INT)");
    run(&mut db, "INSERT INTO t VALUES (1), (5), (10)");
    let r = rows(&mut db, "SELECT n FROM t WHERE n = (SELECT MAX(n) FROM t)");
    assert_eq!(r.rows, vec![vec![Value::Int(10)]]);
    let r = rows(
        &mut db,
        "SELECT n, (SELECT MAX(n) FROM t) AS mx FROM t WHERE n = 1",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(1), Value::Int(10)]]);
}

#[test]
fn any_subquery_equality() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE a (n INT)");
    run(&mut db, "CREATE TABLE b (n INT)");
    run(&mut db, "INSERT INTO a VALUES (1), (2)");
    run(&mut db, "INSERT INTO b VALUES (2)");
    let r = rows(&mut db, "SELECT n FROM a WHERE n = ANY (SELECT n FROM b)");
    assert_eq!(r.rows, vec![vec![Value::Int(2)]]);
}

// ---- SQL completeness: set operations ------------------------------

#[test]
fn intersect_and_except_setops() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE a (n INT)");
    run(&mut db, "CREATE TABLE b (n INT)");
    run(&mut db, "INSERT INTO a VALUES (1), (2), (3)");
    run(&mut db, "INSERT INTO b VALUES (2), (3), (4)");
    let r = rows(
        &mut db,
        "SELECT n FROM a INTERSECT SELECT n FROM b ORDER BY n",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(2)], vec![Value::Int(3)]]);
    let r = rows(&mut db, "SELECT n FROM a EXCEPT SELECT n FROM b");
    assert_eq!(r.rows, vec![vec![Value::Int(1)]]);
    let r = rows(&mut db, "SELECT n FROM b EXCEPT SELECT n FROM a");
    assert_eq!(r.rows, vec![vec![Value::Int(4)]]);
    // EXCEPT ALL keeps multiplicity on the left: {1,1,2} minus the
    // right's single 2 leaves both 1s.
    run(&mut db, "CREATE TABLE c (n INT)");
    run(&mut db, "INSERT INTO c VALUES (1), (1), (2)");
    let r = rows(
        &mut db,
        "SELECT n FROM c EXCEPT ALL SELECT n FROM b ORDER BY n",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(1)], vec![Value::Int(1)]]);
}

#[test]
fn union_order_by_applies_to_whole_result() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE a (n INT)");
    run(&mut db, "CREATE TABLE b (n INT)");
    run(&mut db, "INSERT INTO a VALUES (3), (1)");
    run(&mut db, "INSERT INTO b VALUES (2)");
    let r = rows(
        &mut db,
        "SELECT n FROM a UNION ALL SELECT n FROM b ORDER BY n DESC",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(3)],
            vec![Value::Int(2)],
            vec![Value::Int(1)],
        ]
    );
}

#[test]
fn group_concat_aggregate() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (g TEXT, s TEXT)");
    run(
        &mut db,
        "INSERT INTO t VALUES ('x', 'b'), ('x', 'a'), ('y', 'c')",
    );
    let r = rows(
        &mut db,
        "SELECT g, GROUP_CONCAT(s) FROM t GROUP BY g ORDER BY g",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("x".into()), Value::Str("b,a".into())],
            vec![Value::Str("y".into()), Value::Str("c".into())],
        ]
    );
}

// ---- SQL completeness: DDL constraints ------------------------------

#[test]
fn rename_column_and_table() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT, v TEXT)");
    run(&mut db, "INSERT INTO t VALUES (1, 'a')");
    run(&mut db, "ALTER TABLE t RENAME COLUMN v TO name");
    let r = rows(&mut db, "SELECT id, name FROM t");
    assert_eq!(r.rows, vec![vec![Value::Int(1), Value::Str("a".into())]]);
    run(&mut db, "ALTER TABLE t RENAME TO t2");
    let r = rows(&mut db, "SELECT name FROM t2");
    assert_eq!(r.rows, vec![vec![Value::Str("a".into())]]);
    assert!(db.execute("SELECT * FROM t").is_err());
}

#[test]
fn column_default_on_insert_and_alter_backfill() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT, s TEXT DEFAULT 'seed', n INT DEFAULT 7)",
    );
    run(&mut db, "INSERT INTO t (id) VALUES (1)");
    let r = rows(&mut db, "SELECT s, n FROM t WHERE id = 1");
    assert_eq!(r.rows, vec![vec![Value::Str("seed".into()), Value::Int(7)]]);
    // ADD COLUMN with DEFAULT backfills existing rows.
    run(&mut db, "INSERT INTO t (id, s) VALUES (2, 'x')");
    run(&mut db, "ALTER TABLE t ADD COLUMN flag INT DEFAULT 1");
    let r = rows(&mut db, "SELECT flag FROM t ORDER BY id");
    assert_eq!(r.rows, vec![vec![Value::Int(1)], vec![Value::Int(1)]]);
    // Conventional option order `NOT NULL DEFAULT ...`: the NOT NULL
    // rule must be judged after all options are registered, not hit a
    // false "without a DEFAULT" while walking the options in order.
    run(
        &mut db,
        "ALTER TABLE t ADD COLUMN tag TEXT NOT NULL DEFAULT 'z'",
    );
    let r = rows(&mut db, "SELECT tag FROM t ORDER BY id");
    assert_eq!(
        r.rows,
        vec![vec![Value::Str("z".into())], vec![Value::Str("z".into())]]
    );
    run(&mut db, "INSERT INTO t (id) VALUES (3)");
    let r = rows(&mut db, "SELECT tag FROM t WHERE id = 3");
    assert_eq!(r.rows, vec![vec![Value::Str("z".into())]]);
    // ...while NOT NULL without a DEFAULT stays rejected.
    let e = db
        .execute("ALTER TABLE t ADD COLUMN bad INT NOT NULL")
        .unwrap_err();
    assert!(e.to_string().contains("without a DEFAULT"), "{e}");
}

#[test]
fn column_defaults_survive_reopen() {
    // Defaults live in the catalog (saved as SQL text); like every
    // TableMeta field they must round-trip the reopen (red line 9).
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("defaults.db");
    let mut db = Database::open(&path).unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY, s TEXT DEFAULT 'seed', n INT NOT NULL DEFAULT 7)",
    );
    run(&mut db, "INSERT INTO t (id) VALUES (1)");
    drop(db);
    let mut db = Database::open(&path).unwrap();
    // Still filling omitted columns after reopen...
    run(&mut db, "INSERT INTO t (id) VALUES (2)");
    let r = rows(&mut db, "SELECT s, n FROM t WHERE id = 2");
    assert_eq!(r.rows, vec![vec![Value::Str("seed".into()), Value::Int(7)]]);
    // ...and the tooling surface still reports the declarations.
    let cols = db
        .catalog()
        .into_iter()
        .find(|t| t.name == "t")
        .unwrap()
        .columns;
    assert_eq!(cols[1].default_value.as_deref(), Some("'seed'"));
    assert_eq!(cols[2].default_value.as_deref(), Some("7"));
    assert_eq!(cols[0].default_value, None);
}

#[test]
fn default_follows_rename_and_drop() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY, s TEXT DEFAULT 'x')",
    );
    // RENAME COLUMN carries the default to the new name.
    run(&mut db, "ALTER TABLE t RENAME COLUMN s TO note");
    run(&mut db, "INSERT INTO t (id) VALUES (1)");
    let r = rows(&mut db, "SELECT note FROM t WHERE id = 1");
    assert_eq!(r.rows, vec![vec![Value::Str("x".into())]]);
    let col = &db
        .catalog()
        .into_iter()
        .find(|t| t.name == "t")
        .unwrap()
        .columns[1];
    assert_eq!(col.name, "note");
    assert_eq!(col.default_value.as_deref(), Some("'x'"));
    // DROP COLUMN removes the default bookkeeping along with the column.
    run(&mut db, "ALTER TABLE t DROP COLUMN note");
    let t = db.catalog().into_iter().find(|t| t.name == "t").unwrap();
    assert_eq!(t.columns.len(), 1, "only id remains: {:?}", t.columns);
    assert_eq!(t.columns[0].name, "id");
    // Row 2's document carries no stale fill.
    run(&mut db, "INSERT INTO t (id) VALUES (2)");
    let r = rows(&mut db, "SELECT id FROM t WHERE id = 2");
    assert_eq!(r.rows, vec![vec![Value::Int(2)]]);
}

#[test]
fn guid_and_default_fill_writeback_together() {
    // Red line 11: the fanout/transaction buffer must carry the
    // canonical INSERT — generated GUIDs AND default-filled columns as
    // explicit values — so peers replay deterministically instead of
    // re-filling on their own.
    let mut db = Database::in_memory().unwrap();
    run(
            &mut db,
            "CREATE TABLE t (id GUID PRIMARY KEY AUTOINCREMENT, v TEXT DEFAULT 'd', n INT NOT NULL DEFAULT 5)",
        );
    run(&mut db, "INSERT INTO t (n) VALUES (7)");
    // Take the writeback before any other statement — it resets per
    // statement (take_resolved_sql is the server's fanout hook).
    let resolved = db.take_resolved_sql().expect("resolved insert text");
    let r = rows(&mut db, "SELECT id, v, n FROM t");
    let row = &r.rows[0];
    assert_eq!(&row[0].as_str().unwrap()[14..15], "7");
    assert_eq!(row[1], Value::Str("d".into()));
    assert_eq!(row[2], Value::Int(7));
    // The resolved writeback covers the generated id and the
    // default-filled column explicitly.
    assert!(
        resolved.contains("(\"id\", \"n\", \"v\")"),
        "explicit column list: {resolved}"
    );
    assert!(resolved.contains("'d'"), "default pinned: {resolved}");
    assert!(resolved.contains(" 7, 'd'"), "values pinned: {resolved}");
    // Consuming the writeback clears it.
    assert!(db.take_resolved_sql().is_none());
}

#[test]
fn check_constraint_enforced() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (n INT CHECK (n > 0))");
    let e = db.execute("INSERT INTO t VALUES (-1)").unwrap_err();
    assert!(e.to_string().contains("CHECK"), "{e}");
    run(&mut db, "INSERT INTO t VALUES (1)");
    let e = db.execute("UPDATE t SET n = -5").unwrap_err();
    assert!(e.to_string().contains("CHECK"), "{e}");
    // NULL is unknown -> passes.
    run(&mut db, "INSERT INTO t (n) VALUES (NULL)");
    // Table-level CHECK with a name works too.
    run(
        &mut db,
        "CREATE TABLE u (a INT, b INT, CONSTRAINT pos CHECK (a + b > 0))",
    );
    let e = db.execute("INSERT INTO u VALUES (-2, -3)").unwrap_err();
    assert!(e.to_string().contains("CHECK"), "{e}");
}

#[test]
fn foreign_key_enforced() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE parent (id INT PRIMARY KEY)");
    run(
        &mut db,
        "CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id))",
    );
    run(&mut db, "INSERT INTO parent VALUES (1)");
    run(&mut db, "INSERT INTO child VALUES (10, 1)"); // valid
    run(&mut db, "INSERT INTO child (id, pid) VALUES (11, NULL)"); // NULL passes
    let e = db.execute("INSERT INTO child VALUES (12, 99)").unwrap_err();
    assert!(e.to_string().contains("FOREIGN KEY"), "{e}");
    let e = db
        .execute("UPDATE child SET pid = 42 WHERE id = 10")
        .unwrap_err();
    assert!(e.to_string().contains("FOREIGN KEY"), "{e}");
    // Table-level FOREIGN KEY form.
    run(
        &mut db,
        "CREATE TABLE child2 (id INT, pid INT, FOREIGN KEY (pid) REFERENCES parent (id))",
    );
    let e = db.execute("INSERT INTO child2 VALUES (1, 5)").unwrap_err();
    assert!(e.to_string().contains("FOREIGN KEY"), "{e}");
}

/// 同语句行间引用与自引用 FK(与 SQLite/PG 立即 FK 对齐),以及自引用
/// 行的 dump 往返;引用方 FK 表名在 ALTER TABLE RENAME 时跟随改写。
#[test]
fn fk_intra_statement_self_reference_and_dump_roundtrip() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE emp (id INT PRIMARY KEY, mgr INT REFERENCES emp(id))",
    );
    // 自引用单行(1 的经理是自己)。
    run(&mut db, "INSERT INTO emp VALUES (1, 1)");
    // 行间引用:后行引用本语句的前行。
    run(&mut db, "INSERT INTO emp VALUES (2, 1), (3, 2)");
    // UPDATE 建自引用(此前 dump/restore 必败)。
    run(&mut db, "INSERT INTO emp VALUES (4, NULL)");
    run(&mut db, "UPDATE emp SET mgr = 3 WHERE id = 4");
    // dump 往返:前向引用行(引用同表更晚存储的行)按拓扑序出。
    let dump = db.dump_script().unwrap();
    let mut restored = Database::in_memory().unwrap();
    for stmt in crate::stmt::split_statements(&dump).unwrap() {
        run(&mut restored, &stmt);
    }
    let r = rows(&mut restored, "SELECT id, mgr FROM emp ORDER BY id");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(1), Value::Int(1)],
            vec![Value::Int(2), Value::Int(1)],
            vec![Value::Int(3), Value::Int(2)],
            vec![Value::Int(4), Value::Int(3)],
        ]
    );
    // 环(互为父子):dump 保持存储序,回放响亮报错,不静默丢行。
    // 单行 INSERT 的前向引用仍被拒(批内候选只覆盖同语句的行)。
    assert!(db.execute("INSERT INTO emp VALUES (5, 6)").is_err());
    run(&mut db, "INSERT INTO emp VALUES (5, NULL)");
    run(&mut db, "INSERT INTO emp VALUES (6, 5)");
    run(&mut db, "UPDATE emp SET mgr = 6 WHERE id = 5");
    let dump = db.dump_script().unwrap();
    let mut restored2 = Database::in_memory().unwrap();
    let mut failed = false;
    for stmt in crate::stmt::split_statements(&dump).unwrap() {
        if restored2.execute(&stmt).is_err() {
            failed = true;
        }
    }
    assert!(failed, "环引用的回放必须响亮失败");

    // RENAME 改写引用方 FK 表名:子表写路径不再瘫痪。
    let mut db2 = Database::in_memory().unwrap();
    run(&mut db2, "CREATE TABLE p (id INT PRIMARY KEY)");
    run(
        &mut db2,
        "CREATE TABLE c (id INT PRIMARY KEY, pid INT REFERENCES p(id))",
    );
    run(&mut db2, "INSERT INTO p VALUES (1)");
    run(&mut db2, "ALTER TABLE p RENAME TO p2");
    run(&mut db2, "INSERT INTO c VALUES (10, 1)"); // 曾报 table p does not exist
    let e = db2.execute("INSERT INTO c VALUES (11, 9)").unwrap_err();
    assert!(e.to_string().contains("FOREIGN KEY"), "{e}");
    // rename 后 dump 往返也保持引用一致。
    let dump = db2.dump_script().unwrap();
    let mut r2 = Database::in_memory().unwrap();
    for stmt in crate::stmt::split_statements(&dump).unwrap() {
        run(&mut r2, &stmt);
    }
    run(&mut r2, "INSERT INTO c VALUES (12, 1)");
}

/// 表级 FOREIGN KEY 列数不匹配必须报错(曾被 zip 静默截断);复合 FK
/// 显式拒绝(曾被静默拆成独立单列 FK,削弱为逐列判定)。
#[test]
fn fk_column_count_mismatch_rejected() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE p (a INT, b INT)");
    let e = db
        .execute("CREATE TABLE c (x INT, FOREIGN KEY (x) REFERENCES p (a, b))")
        .unwrap_err();
    assert!(e.to_string().contains("mismatch"), "{e}");
    let e = db
        .execute("CREATE TABLE c (x INT, y INT, FOREIGN KEY (x, y) REFERENCES p (a, b))")
        .unwrap_err();
    assert!(e.to_string().contains("composite FOREIGN KEY"), "{e}");
}

/// OR REPLACE 批内重复键:后者胜(曾整条报 UNIQUE)。
#[test]
fn insert_or_replace_within_batch_last_wins() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
    run(
        &mut db,
        "INSERT OR REPLACE INTO t VALUES (1, 'a'), (1, 'b')",
    );
    let r = rows(&mut db, "SELECT v FROM t");
    assert_eq!(r.rows, vec![vec![Value::Str("b".into())]]);
    // 与既有行冲突 + 批内冲突并存。
    run(
        &mut db,
        "INSERT OR REPLACE INTO t VALUES (1, 'c'), (2, 'x'), (2, 'y')",
    );
    let r = rows(&mut db, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(1), Value::Str("c".into())],
            vec![Value::Int(2), Value::Str("y".into())],
        ]
    );
}

/// MERGE 失败回滚后 SCOPE_IDENTITY 不留幻影 id。
#[test]
fn merge_rollback_leaves_no_phantom_identity() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT AUTOINCREMENT PRIMARY KEY, k INT)",
    );
    run(&mut db, "CREATE TABLE s (k INT)");
    run(&mut db, "INSERT INTO t VALUES (1, 1)");
    run(&mut db, "INSERT INTO s VALUES (1), (2)");
    // MERGE 插入的 not-matched 行带显式 id=1,撞唯一键 → 整条回滚;
    // 曾经循环内提前推进 last_insert_id,回滚后留下幻影。
    let e = db
        .execute("MERGE INTO t USING s ON t.k = s.k WHEN NOT MATCHED THEN INSERT VALUES (1, s.k)")
        .unwrap_err();
    assert!(e.to_string().contains("UNIQUE"), "{e}");
    assert_eq!(db.last_insert_id(), None, "回滚后不得有幻影 id");
    // 成功路径照常上报最大 id。
    run(
        &mut db,
        "MERGE INTO t USING s ON t.k = s.k WHEN NOT MATCHED THEN INSERT VALUES (100 + s.k, s.k)",
    );
    assert_eq!(db.last_insert_id(), Some(102));
}

/// INSERT ... SELECT 无列清单按位置映射(曾按查询输出名建幽灵键)。
#[test]
fn insert_select_positional_mapping() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE logs (ts INT, lvl TEXT)");
    run(&mut db, "CREATE TABLE staging (a INT, b TEXT)");
    run(&mut db, "INSERT INTO staging VALUES (1, 'x')");
    run(&mut db, "INSERT INTO logs SELECT * FROM staging");
    run(&mut db, "INSERT INTO logs SELECT 2, 'y'");
    run(&mut db, "INSERT INTO logs SELECT a AS ts2, b FROM staging");
    let r = rows(&mut db, "SELECT ts, lvl FROM logs");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(1), Value::Str("x".into())],
            vec![Value::Int(2), Value::Str("y".into())],
            vec![Value::Int(1), Value::Str("x".into())],
        ]
    );
}

#[test]
fn fk_parent_delete_and_key_update_enforced() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE parent (id INT PRIMARY KEY, name TEXT)",
    );
    run(
        &mut db,
        "CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id))",
    );
    run(&mut db, "INSERT INTO parent VALUES (1, 'a'), (2, 'b')");
    run(&mut db, "INSERT INTO child VALUES (10, 1)");
    // Deleting an unreferenced parent passes.
    run(&mut db, "DELETE FROM parent WHERE id = 2");
    // Deleting a referenced parent is refused loudly and transactionally.
    let e = db.execute("DELETE FROM parent WHERE id = 1").unwrap_err();
    assert!(
        e.to_string().contains("child.pid references parent.id"),
        "{e}"
    );
    // A child table with no rows cannot block anything.
    run(
        &mut db,
        "CREATE TABLE orphan (id INT PRIMARY KEY, pid INT REFERENCES parent(id))",
    );
    run(&mut db, "DELETE FROM child WHERE id = 10");
    run(&mut db, "DELETE FROM parent WHERE id = 1");
    run(&mut db, "INSERT INTO parent VALUES (1, 'a')");
    run(&mut db, "INSERT INTO orphan VALUES (20, 1)");
    // Key updates follow the same rule (the old key disappears).
    let e = db
        .execute("UPDATE parent SET id = 3 WHERE id = 1")
        .unwrap_err();
    assert!(
        e.to_string().contains("orphan.pid references parent.id"),
        "update must fail like a delete, got: {e}"
    );
    // A non-key update keeps the parent key (matches replacement_docs)
    // and passes; NULL children neither block nor count.
    run(&mut db, "INSERT INTO orphan VALUES (21, NULL)");
    run(&mut db, "UPDATE parent SET name = 'z' WHERE id = 1");
    // Removing the referencing rows first unblocks the parent delete.
    run(&mut db, "DELETE FROM orphan WHERE id IN (20, 21)");
    run(&mut db, "DELETE FROM parent WHERE id = 1");
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM parent").rows[0][0],
        Value::Int(0)
    );
}

#[test]
fn fk_parent_delete_multiple_children_references() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE parent (id INT PRIMARY KEY)");
    run(
        &mut db,
        "CREATE TABLE a (id INT PRIMARY KEY, pid INT REFERENCES parent(id))",
    );
    run(
        &mut db,
        "CREATE TABLE b (id INT PRIMARY KEY, pid INT REFERENCES parent(id))",
    );
    run(&mut db, "INSERT INTO parent VALUES (1)");
    run(&mut db, "INSERT INTO a VALUES (10, 1)");
    run(&mut db, "INSERT INTO b VALUES (20, 1)");
    let e = db.execute("DELETE FROM parent WHERE id = 1").unwrap_err();
    // The first offending child in catalog order is named.
    assert!(e.to_string().contains("references parent.id"), "{e}");
    run(&mut db, "DELETE FROM a WHERE id = 10");
    let e = db.execute("DELETE FROM parent WHERE id = 1").unwrap_err();
    assert!(e.to_string().contains("b.pid references parent.id"), "{e}");
}

#[test]
fn table_level_unique_and_pk_constraints() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (a INT, b INT, PRIMARY KEY (a), UNIQUE (b))",
    );
    run(&mut db, "INSERT INTO t VALUES (1, 10)");
    let e = db.execute("INSERT INTO t VALUES (2, 10)").unwrap_err();
    assert!(e.to_string().contains("UNIQUE"), "{e}");
    let e = db.execute("INSERT INTO t VALUES (1, 20)").unwrap_err();
    assert!(e.to_string().contains("UNIQUE"), "{e}");
}

// 待办:-9223372036854775808 目前被解析为浮点(一元负号 + 溢出字面量),
// 需要解析器层支持;恢复前保持 ignore。
#[test]
fn integer_boundary_values_roundtrip() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    run(
        &mut db,
        "INSERT INTO t VALUES (9223372036854775807, -9223372036854775808)",
    );
    run(&mut db, "INSERT INTO t VALUES (0, 0)");
    let r = rows(
        &mut db,
        "SELECT id, v FROM t WHERE id = 9223372036854775807",
    );
    assert_eq!(
        r.rows,
        vec![vec![Value::Int(i64::MAX), Value::Int(i64::MIN)]]
    );
    let r = rows(&mut db, "SELECT v + 1 FROM t WHERE id = 0");
    assert_eq!(r.rows, vec![vec![Value::Int(1)]]);
    let r = rows(&mut db, "SELECT id FROM t ORDER BY id");
    assert_eq!(
        r.rows,
        vec![vec![Value::Int(0)], vec![Value::Int(i64::MAX)]]
    );
}

#[test]
fn sum_integer_overflow_yields_null_not_wraparound() {
    // SUM must match the scalar `+` overflow semantics (NULL): a wrapped
    // negative would silently book a huge total as a loss.
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (a INT)");
    run(&mut db, "INSERT INTO t VALUES (9223372036854775807)");
    run(&mut db, "INSERT INTO t VALUES (1)");
    let r = rows(&mut db, "SELECT SUM(a) FROM t");
    assert_eq!(r.rows, vec![vec![Value::Null]], "got {:?}", r.rows);
    // In-range sums stay exact.
    let r = rows(&mut db, "SELECT SUM(a - 1) FROM t");
    assert_eq!(r.rows, vec![vec![Value::Int(i64::MAX - 1)]]);
}

#[test]
fn cast_out_of_range_to_int_errors_instead_of_zero_or_saturation() {
    let mut db = Database::in_memory().unwrap();
    // DECIMAL → INT used to unwrap_or(0): a huge amount silently became 0.
    let e = db
        .execute("SELECT CAST(CAST('1e28' AS DECIMAL) AS INT)")
        .unwrap_err();
    assert!(e.to_string().contains("out of range"), "{e}");
    // Float → INT used to saturate at i64::MAX.
    let e = db.execute("SELECT CAST(1e30 AS INT)").unwrap_err();
    assert!(e.to_string().contains("out of range"), "{e}");
    let e = db.execute("SELECT CAST(1e308 * 10 AS INT)").unwrap_err();
    assert!(e.to_string().contains("out of range"), "{e}");
    // In-range truncation and parsing keep working.
    let r = rows(&mut db, "SELECT CAST(3.7 AS INT), CAST('42' AS INT)");
    assert_eq!(r.rows, vec![vec![Value::Int(3), Value::Int(42)]]);
}

// ---- 精确 TIMESTAMP:类型、协变、探针、折叠 ----

#[test]
fn timestamp_literal_and_wire_roundtrip() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE ev (id INT PRIMARY KEY, at TIMESTAMP)",
    );
    run(
        &mut db,
        "INSERT INTO ev VALUES (1, TIMESTAMP '2026-09-15T08:30:00.123Z')",
    );
    let r = rows(&mut db, "SELECT at FROM ev");
    assert!(
        matches!(&r.rows[0][0], Value::Timestamp(ms) if *ms == 1_789_461_000_123),
        "TIMESTAMP 字面量必须存为毫秒: {:?}",
        r.rows[0][0]
    );
    // Wire format: {"$ts": millis} survives a JSON round-trip exactly.
    let wire = crate::json::to_string(&r.rows[0][0]);
    assert_eq!(wire, format!("{{\"$ts\":{}}}", 1_789_461_000_123i64));
    let back = crate::json::from_str(&wire).unwrap();
    assert_eq!(back, r.rows[0][0]);
    // Display is the canonical ISO text.
    assert_eq!(value_to_text(&r.rows[0][0]), "2026-09-15T08:30:00.123Z");
}

#[test]
fn timestamp_string_comparison_works_on_index_and_scan_paths() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE ev2 (id INT PRIMARY KEY, at TIMESTAMP)",
    );
    // id=1 有一棵 ts 索引,forces the indexed probe path; id=2/3 走同树。
    run(&mut db, "CREATE INDEX idx_ev2_at ON ev2 (at)");
    run(
        &mut db,
        "INSERT INTO ev2 VALUES (1, TIMESTAMP '2026-01-01T00:00:00Z')",
    );
    run(
        &mut db,
        "INSERT INTO ev2 VALUES (2, TIMESTAMP '2026-06-15T12:00:00Z')",
    );
    run(
        &mut db,
        "INSERT INTO ev2 VALUES (3, TIMESTAMP '2026-12-31T23:59:59.999Z')",
    );
    // String literal against the TIMESTAMP column: predicate coercion AND
    // the index-probe bound promotion must agree (same rows, both paths).
    let via_index = rows(
        &mut db,
        "SELECT id FROM ev2 WHERE at > '2026-06-01T00:00:00Z' ORDER BY id",
    );
    assert_eq!(
        via_index.rows,
        vec![vec![Value::Int(2)], vec![Value::Int(3)]],
        "索引探针的字符串下界必须按时间比较"
    );
    // Exact-equality probe with a string.
    let eq = rows(
        &mut db,
        "SELECT id FROM ev2 WHERE at = '2026-06-15T12:00:00Z'",
    );
    assert_eq!(eq.rows, vec![vec![Value::Int(2)]]);
    // Range with both bounds.
    let between = rows(
            &mut db,
            "SELECT id FROM ev2 WHERE at >= '2026-01-01T00:00:00Z' AND at < '2026-12-01T00:00:00Z' ORDER BY id",
        );
    assert_eq!(between.rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
    // Non-index path agrees (force the scan by filtering another column).
    let scan = rows(
        &mut db,
        "SELECT id FROM ev2 WHERE id + 0 > 0 AND at > '2026-06-01T00:00:00Z' ORDER BY id",
    );
    assert_eq!(scan.rows, vec![vec![Value::Int(2)], vec![Value::Int(3)]]);
    // DESC window (reverse probe) agrees too.
    let desc = rows(&mut db, "SELECT id FROM ev2 ORDER BY at DESC LIMIT 1");
    assert_eq!(desc.rows, vec![vec![Value::Int(3)]]);
    // Unparseable string vs TIMESTAMP: NULL (unknown), never a wrong row.
    let junk = rows(&mut db, "SELECT id FROM ev2 WHERE at > 'not-a-time'");
    assert!(junk.rows.is_empty(), "unparseable bound must match nothing");
}

/// 混合带(Str+Timestamp 同列)不可安全探测:探针结果曾依赖索引
/// 存在性 —— 保留字面量只探 Str 带,丢掉整个 Timestamp 带。必须回退
/// 通用扫描,给出与无索引时一致的行集。
#[test]
fn mixed_band_index_probe_falls_back_to_scan_semantics() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE mb (id INT PRIMARY KEY, ts TEXT)");
    run(&mut db, "CREATE INDEX i_mb_ts ON mb (ts)");
    run(
        &mut db,
        "INSERT INTO mb VALUES (1, TIMESTAMP '2021-01-01T00:00:00Z')",
    );
    run(&mut db, "INSERT INTO mb VALUES (2, 'zzz')");
    run(
        &mut db,
        "INSERT INTO mb VALUES (3, TIMESTAMP '2022-01-01T00:00:00Z')",
    );
    // 范围谓词:三行全部命中(cmp_coerced 逐行判定),不得只剩 Str 带。
    let r = rows(
        &mut db,
        "SELECT id FROM mb WHERE ts > '2020-01-01T00:00:00Z' ORDER BY id",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)]
        ],
        "混合带范围探针不得静默丢 Timestamp 行"
    );
    // 等值谓词:命中 Timestamp 行。
    let r = rows(
        &mut db,
        "SELECT id FROM mb WHERE ts = '2021-01-01T00:00:00Z'",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(1)]]);
    // UPDATE 快路径(同一探针)也不丢行。
    match run(
        &mut db,
        "UPDATE mb SET id = id WHERE ts > '2020-01-01T00:00:00Z'",
    ) {
        ExecOutcome::Affected(n) => assert_eq!(n, 3),
        other => panic!("expected affected, got {other:?}"),
    }
}

// ---- 谓词探针计划:IN / BETWEEN / LIKE 前缀(探针=超集,残余过滤兜底)----

#[test]
fn like_literal_prefix_and_prefix_successor() {
    // 纯函数:token 流 Literals* + 尾部单个 AnyRun 才有前缀。
    assert_eq!(
        crate::tsql::like_literal_prefix("abc%", None),
        Some("abc".to_string())
    );
    // 无尾部 %:精确匹配不走带探针;其余形状一律 None。
    assert_eq!(crate::tsql::like_literal_prefix("abc", None), None);
    assert_eq!(crate::tsql::like_literal_prefix("%abc", None), None);
    assert_eq!(crate::tsql::like_literal_prefix("a%b", None), None);
    assert_eq!(crate::tsql::like_literal_prefix("a_b%", None), None);
    assert_eq!(crate::tsql::like_literal_prefix("a[bc]%", None), None);
    assert_eq!(crate::tsql::like_literal_prefix("", None), None);
    // 转义在 tokenizer 内已解析:转义 % 是字面量成员。
    assert_eq!(
        crate::tsql::like_literal_prefix("a\\%b%", Some('\\')),
        Some("a%b".to_string())
    );
    assert_eq!(crate::tsql::like_literal_prefix("\\%", Some('\\')), None);
    // 前缀后继:末字符 +1;末位 char::MAX 无有限后继。
    assert_eq!(str_prefix_successor("abc"), Some("abd".to_string()));
    assert_eq!(str_prefix_successor("a"), Some("b".to_string()));
    assert_eq!(str_prefix_successor("\u{10FFFF}"), None);
    // 尾位 MAX 进位会把前缀自己裁掉(前缀 > 进位界),必须 None。
    assert_eq!(str_prefix_successor("ab\u{10FFFF}"), None);
}

/// 双生表差分骨架:同一行集上,索引列与无索引列的同一谓词必须同答
/// (结果不得依赖索引存在性,红线 #18)。
#[test]
fn in_between_like_probe_parity_with_scan() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE pt (id INT PRIMARY KEY NOT NULL, v INT, s TEXT, p INT, ps TEXT)",
    );
    run(&mut db, "CREATE INDEX ix_pt_v ON pt (v)");
    run(&mut db, "CREATE INDEX ix_pt_s ON pt (s)");
    for i in 1..=300 {
        let v = if i % 17 == 0 {
            "NULL".to_string()
        } else {
            format!("{}", i % 10)
        };
        let s = if i % 23 == 0 {
            "NULL".to_string()
        } else {
            format!("'name{}'", i % 10)
        };
        run(
            &mut db,
            &format!("INSERT INTO pt VALUES ({i}, {v}, {s}, {v}, {s})"),
        );
    }
    let parity = |db: &mut Database, pred_v: &str, pred_p: &str| {
        let a = rows(db, &format!("SELECT id FROM pt WHERE {pred_v} ORDER BY id")).rows;
        let b = rows(db, &format!("SELECT id FROM pt WHERE {pred_p} ORDER BY id")).rows;
        assert_eq!(a, b, "索引列({pred_v})与无索引列({pred_p})必须同答");
    };
    parity(&mut db, "v IN (3, 7)", "p IN (3, 7)");
    // 重复元素:探针整对去重,行不得翻倍。
    parity(&mut db, "v IN (3, 3, 7, 3)", "p IN (3, 3, 7, 3)");
    // cmp 判等的跨类型元素(Int/Float 同值)也不得重复计行。
    parity(&mut db, "v IN (3, 3.0)", "p IN (3, 3.0)");
    // NULL 元素永不匹配,不进探针。
    parity(&mut db, "v IN (3, NULL)", "p IN (3, NULL)");
    // 全 NULL 列表:谓词恒未知。
    parity(&mut db, "v IN (NULL)", "p IN (NULL)");
    parity(&mut db, "v BETWEEN 2 AND 4", "p BETWEEN 2 AND 4");
    parity(&mut db, "v BETWEEN 4 AND 2", "p BETWEEN 4 AND 2");
    parity(&mut db, "v NOT BETWEEN 2 AND 4", "p NOT BETWEEN 2 AND 4");
    parity(
        &mut db,
        "v > 2 AND v BETWEEN 1 AND 4",
        "p > 2 AND p BETWEEN 1 AND 4",
    );
    parity(&mut db, "s LIKE 'name3%'", "ps LIKE 'name3%'");
    // 中段 % / _ / 字符类 / ILIKE / NOT LIKE:保持全扫,结果同答。
    parity(&mut db, "s LIKE 'n%3%'", "ps LIKE 'n%3%'");
    parity(&mut db, "s LIKE 'name_'", "ps LIKE 'name_'");
    parity(&mut db, "s LIKE 'name[35]'", "ps LIKE 'name[35]'");
    parity(&mut db, "s ILIKE 'NAME3%'", "ps ILIKE 'NAME3%'");
    parity(&mut db, "s NOT LIKE 'name3%'", "ps NOT LIKE 'name3%'");
    // 转义前缀:字面量 % 进前缀。
    parity(
        &mut db,
        "s LIKE 'name\\_3%' ESCAPE '\\'",
        "ps LIKE 'name\\_3%' ESCAPE '\\'",
    );
    // IN 与等值/范围混合:计划只消费一臂,其余走残余过滤。
    parity(&mut db, "v IN (1, 2) AND v > 1", "p IN (1, 2) AND p > 1");
    parity(&mut db, "v = 3 AND v IN (3, 4)", "p = 3 AND p IN (3, 4)");
    parity(&mut db, "v IN (1, 2) OR v = 3", "p IN (1, 2) OR p = 3");
    // IN (SELECT …) 预 pass 改写成字面量 InList 后同路。
    parity(
        &mut db,
        "v IN (SELECT p FROM pt WHERE id <= 12)",
        "p IN (SELECT p FROM pt WHERE id <= 12)",
    );
    // 无下界范围:NULL 排在一切值之下,`v < k` 对 NULL 键为真,而树
    // 无 NULL 键——探针必须整体回退全扫(v 可空),不得静默丢行。
    parity(&mut db, "v < 3", "p < 3");
    parity(&mut db, "v <= 3", "p <= 3");
    parity(&mut db, "v < -100", "p < -100");
    // UPDATE/DELETE 快路径(index_probe_cx)同探针,影响行数同答。
    let affected = |db: &mut Database, sql: &str| match run(db, sql) {
        ExecOutcome::Affected(n) => n,
        other => panic!("expected affected, got {other:?}"),
    };
    let n_idx = affected(&mut db, "UPDATE pt SET v = v WHERE v BETWEEN 2 AND 3");
    let n_scan = affected(&mut db, "UPDATE pt SET p = p WHERE p BETWEEN 2 AND 3");
    assert_eq!(n_idx, n_scan);
    // 两条 DELETE 各自独立事务:p 镜像 v,同事务连跑会让第一条删掉
    // 第二条的匹配行。
    run(&mut db, "BEGIN");
    let n_idx = affected(&mut db, "DELETE FROM pt WHERE v IN (8, 9)");
    run(&mut db, "ROLLBACK");
    run(&mut db, "BEGIN");
    let n_scan = affected(&mut db, "DELETE FROM pt WHERE p IN (8, 9)");
    run(&mut db, "ROLLBACK");
    assert_eq!(n_idx, n_scan);
}

#[test]
fn unbounded_probe_never_drops_null_key_rows() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY, b INT, c INT, p INT)",
    );
    run(&mut db, "CREATE INDEX ix_bc ON t (b, c)");
    run(&mut db, "CREATE INDEX ix_p ON t (p)");
    run(
        &mut db,
        "INSERT INTO t VALUES (1, 5, NULL, NULL), (2, 5, 7, 2), (3, 6, NULL, NULL), (4, 7, 8, 8)",
    );
    // 复合部分前缀 + 未覆盖列 NULL:该行无树键,探针须回退全扫。
    let a = rows(&mut db, "SELECT id FROM t WHERE b = 5 ORDER BY id").rows;
    let expect = vec![vec![Value::Int(1)], vec![Value::Int(2)]];
    assert_eq!(a, expect, "复合前缀探针不得丢弃未覆盖列为 NULL 的行");
    // JSON 路径索引无下界范围:提取为 NULL 的行(`NULL < k` 为真)不得丢。
    run(&mut db, "CREATE TABLE j (id INT PRIMARY KEY, doc TEXT)");
    run(
        &mut db,
        "CREATE INDEX ix_age ON j (JSON_EXTRACT(doc, '$.age'))",
    );
    run(
        &mut db,
        "INSERT INTO j VALUES (1, '{\"age\":10}'), (2, 'not json'), (3, '{}')",
    );
    let a = rows(
        &mut db,
        "SELECT id FROM j WHERE JSON_EXTRACT(doc, '$.age') < 30 ORDER BY id",
    )
    .rows;
    let expect = vec![
        vec![Value::Int(1)],
        vec![Value::Int(2)],
        vec![Value::Int(3)],
    ];
    assert_eq!(a, expect, "路径范围探针不得丢弃提取为 NULL 的行");
}

#[test]
fn and_or_fold_unknown_predicates_per_row() {
    // Timestamp↔不可解析字符串(与 Timestamp↔数值)的比较是引擎唯一
    // 的 UNKNOWN 源;混进 AND/OR 时必须按三值逻辑逐行折叠(FALSE 支
    // 配 AND、TRUE 支配 OR、其余保持 unknown → WHERE 滤行),而不是
    // 让整条语句在第一行上就报 "AND/OR requires booleans"。
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE b3 (id INT PRIMARY KEY, ts TIMESTAMP, s TEXT)",
    );
    run(
        &mut db,
        "INSERT INTO b3 VALUES (1, TIMESTAMP '2026-01-01T00:00:00Z', 'zzz')",
    );
    run(
        &mut db,
        "INSERT INTO b3 VALUES (2, TIMESTAMP '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
    );
    // unknown AND true → unknown(滤行);可解析侧照常通过。
    let r = rows(&mut db, "SELECT id FROM b3 WHERE ts = s AND id = 1");
    assert!(r.rows.is_empty());
    let r = rows(&mut db, "SELECT id FROM b3 WHERE ts = s AND id = 2");
    assert_eq!(r.rows, vec![vec![Value::Int(2)]]);
    // unknown OR true → true;unknown OR false → unknown。
    let r = rows(
        &mut db,
        "SELECT id FROM b3 WHERE ts = s OR id = 1 ORDER BY id",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
    let r = rows(&mut db, "SELECT id FROM b3 WHERE ts = s OR id = 3");
    assert_eq!(r.rows, vec![vec![Value::Int(2)]]);
    // NOT(unknown) → unknown → 滤行。
    let r = rows(&mut db, "SELECT id FROM b3 WHERE NOT (ts = s)");
    assert!(r.rows.is_empty());
    // 数值形态的 unknown 同理。
    let r = rows(&mut db, "SELECT id FROM b3 WHERE ts > 0");
    assert!(r.rows.is_empty());
}

#[test]
fn in_list_null_elements_follow_total_order_semantics() {
    // NULL 是可比值:`x IN (…, NULL)` 对 x-NULL 行也为真、`x NOT IN
    // (NULL)` 与 `x <> NULL` 同答;唯一 unknown 源(Timestamp↔不可解
    // 析串)保持 unknown。可空列上的 IN(…, NULL) 不得走 exact 探针
    // (树无 NULL 键,会把 x-NULL 行整体丢掉)。
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE n9 (id INT PRIMARY KEY, v INT, p INT)",
    );
    run(&mut db, "CREATE INDEX ix_n9_v ON n9 (v)");
    run(
        &mut db,
        "INSERT INTO n9 VALUES (1, 3, 3), (2, NULL, NULL), (3, 7, 7)",
    );
    let r = rows(
        &mut db,
        "SELECT id FROM n9 WHERE v IN (3, NULL) ORDER BY id",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
    let b = rows(
        &mut db,
        "SELECT id FROM n9 WHERE p IN (3, NULL) ORDER BY id",
    );
    assert_eq!(r.rows, b.rows, "IN(含 NULL 元素)不得依赖索引存在性");
    let r = rows(
        &mut db,
        "SELECT id FROM n9 WHERE v NOT IN (NULL) ORDER BY id",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(1)], vec![Value::Int(3)]]);
    let b = rows(&mut db, "SELECT id FROM n9 WHERE v <> NULL ORDER BY id");
    assert_eq!(r.rows, b.rows, "NOT IN (NULL) 必须与 <> NULL 同答");
    // = ANY 与 = ALL 在单元素 NULL 集上同答(NULL = NULL 为真)。
    let a = rows(
        &mut db,
        "SELECT id FROM n9 WHERE v = ANY (SELECT NULL) ORDER BY id",
    );
    let l = rows(
        &mut db,
        "SELECT id FROM n9 WHERE v = ALL (SELECT NULL) ORDER BY id",
    );
    assert_eq!(a.rows, l.rows);
    assert_eq!(a.rows, vec![vec![Value::Int(2)]]);
    // Timestamp↔不可解析串元素保持 unknown(滤行,而不是误真/误假)。
    run(
        &mut db,
        "CREATE TABLE n9t (id INT PRIMARY KEY, ts TIMESTAMP, ps TIMESTAMP)",
    );
    run(&mut db, "CREATE INDEX ix_n9t_ts ON n9t (ts)");
    run(
            &mut db,
            "INSERT INTO n9t VALUES (1, TIMESTAMP '2026-01-01T00:00:00Z', TIMESTAMP '2026-01-01T00:00:00Z')",
        );
    let r = rows(&mut db, "SELECT id FROM n9t WHERE ts IN ('garbage')");
    assert!(r.rows.is_empty());
    let r = rows(&mut db, "SELECT id FROM n9t WHERE ts NOT IN ('garbage')");
    assert!(
        r.rows.is_empty(),
        "NOT IN 对 unknown 元素必须与 <> 同答(滤行)"
    );
}

#[test]
fn quantified_any_all_three_valued_logic() {
    // NULL itself is a comparable value in this engine (total order);
    // the only UNKNOWN source is Timestamp vs an unparseable string.
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, ts TIMESTAMP)");
    run(
        &mut db,
        "INSERT INTO t VALUES (1, TIMESTAMP '2026-06-01T00:00:00Z')",
    );
    run(&mut db, "CREATE TABLE s (x TEXT)");
    run(
        &mut db,
        "INSERT INTO s VALUES ('2025-01-01T00:00:00Z'), ('garbage')",
    );
    // ts > '2025…' TRUE, ts > 'garbage' UNKNOWN → ALL = UNKNOWN → the
    // row filters bare AND under NOT (the old FALSE fold let NOT admit
    // it — NOT FALSE became TRUE).
    let r = rows(&mut db, "SELECT id FROM t WHERE ts > ALL (SELECT x FROM s)").rows;
    assert!(
        r.is_empty(),
        "ALL with an UNKNOWN element is UNKNOWN: {r:?}"
    );
    let r = rows(
        &mut db,
        "SELECT id FROM t WHERE NOT (ts > ALL (SELECT x FROM s))",
    )
    .rows;
    assert!(r.is_empty(), "NOT UNKNOWN must not admit rows: {r:?}");
    // ANY with the same inputs: one TRUE → TRUE (standard).
    let r = rows(&mut db, "SELECT id FROM t WHERE ts > ANY (SELECT x FROM s)").rows;
    assert_eq!(r, vec![vec![Value::Int(1)]]);
    let r = rows(
        &mut db,
        "SELECT id FROM t WHERE NOT (ts > ANY (SELECT x FROM s))",
    )
    .rows;
    assert!(r.is_empty(), "NOT TRUE is FALSE");
    // All-UNKNOWN ANY = UNKNOWN (not FALSE): older row below proves it.
    run(&mut db, "DELETE FROM s");
    run(&mut db, "INSERT INTO s VALUES ('garbage'), ('junk')");
    let r = rows(&mut db, "SELECT id FROM t WHERE ts > ANY (SELECT x FROM s)").rows;
    assert!(r.is_empty(), "all-UNKNOWN ANY filters");
    let r = rows(
        &mut db,
        "SELECT id FROM t WHERE NOT (ts > ANY (SELECT x FROM s))",
    )
    .rows;
    assert!(
        r.is_empty(),
        "NOT UNKNOWN must not admit rows either: {r:?}"
    );
    // Two-valued shape stays intact without UNKNOWN elements.
    run(&mut db, "DELETE FROM s");
    run(
        &mut db,
        "INSERT INTO s VALUES ('2025-01-01T00:00:00Z'), ('2027-01-01T00:00:00Z')",
    );
    let r = rows(&mut db, "SELECT id FROM t WHERE ts > ALL (SELECT x FROM s)").rows;
    assert!(r.is_empty());
    let r = rows(&mut db, "SELECT id FROM t WHERE ts > ANY (SELECT x FROM s)").rows;
    assert_eq!(r, vec![vec![Value::Int(1)]]);
}

#[test]
fn hash_join_timestamp_string_coercion() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE a (id INT PRIMARY KEY, ts TIMESTAMP)");
    run(&mut db, "CREATE TABLE b (id INT PRIMARY KEY, s TEXT)");
    for i in 1..=200 {
        let day = (i - 1) % 28 + 1;
        let hour = (i - 1) / 28;
        let text = format!("2026-01-{day:02}T{hour:02}:00:00Z");
        run(
            &mut db,
            &format!("INSERT INTO a VALUES ({i}, TIMESTAMP '{text}')"),
        );
        if i % 2 == 0 {
            run(&mut db, &format!("INSERT INTO b VALUES ({i}, '{text}')"));
        }
    }
    run(&mut db, "INSERT INTO b VALUES (1, 'not a timestamp')");
    let inner = rows(&mut db, "SELECT COUNT(*) AS n FROM a JOIN b ON a.ts = b.s").rows;
    assert_eq!(
        inner[0][0],
        Value::Int(100),
        "哈希 join 桶不得拆散 Timestamp↔可解析字符串命中对"
    );
    let left = rows(
        &mut db,
        "SELECT COUNT(*) AS n FROM a LEFT JOIN b ON a.ts = b.s",
    )
    .rows;
    assert_eq!(
        left[0][0],
        Value::Int(200),
        "LEFT JOIN 不得把真实命中当未命中"
    );
}

#[test]
fn cte_named_like_a_column_is_not_recursive() {
    // 自引用检测只看 FROM/JOIN 表位:CTE 名撞上自己体内的列名/别名
    // (`WITH total AS (SELECT SUM(x) AS total …)`)不是递归——旧全文
    // 词扫描会误诊成递归,拒绝合法查询或让 UNION ALL 体白转满预算。
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE nums (n INT)");
    run(&mut db, "INSERT INTO nums VALUES (1), (2), (3)");
    // 列名撞名:非 UNION 体不得被当递归拒绝。
    let r = rows(
        &mut db,
        "WITH n AS (SELECT n FROM nums) SELECT COUNT(*) FROM n",
    );
    assert_eq!(r.rows[0][0], Value::Int(3));
    // 别名撞名 + UNION ALL:右臂并不引用 CTE,不得按递归去重——
    // 旧误诊走递归物化会按 UNION 去重丢掉重复行(COUNT=3)。
    let r = rows(
        &mut db,
        "WITH region AS (SELECT 1 AS region UNION ALL SELECT n FROM nums) \
             SELECT COUNT(*) FROM region",
    );
    assert_eq!(r.rows[0][0], Value::Int(4));
    // 派生表继承外层 CTE 作用域(旧代码在派生表内重建空 CTE 表,
    // `cte_src` 直接"不存在")。
    run(&mut db, "CREATE TABLE shadow_t (id INT)");
    run(&mut db, "INSERT INTO shadow_t VALUES (1), (2), (3), (4)");
    let r = rows(
        &mut db,
        "WITH cte_src AS (SELECT id FROM shadow_t WHERE id > 2) \
             SELECT COUNT(*) FROM (SELECT * FROM cte_src) d",
    );
    assert_eq!(r.rows[0][0], Value::Int(2));
    // 括号体自带 WITH 不再被丢弃。
    let r = rows(&mut db, "(WITH one AS (SELECT 7 AS v) SELECT v FROM one)");
    assert_eq!(r.rows[0][0], Value::Int(7));
}

#[test]
fn recursive_cte_union_dedupes_anchor_rows() {
    let mut db = Database::in_memory().unwrap();
    let r = rows(
            &mut db,
            "WITH c AS (SELECT n FROM (VALUES (1), (1), (2)) v(n) UNION SELECT n + 1 FROM c WHERE n < 2) \
             SELECT COUNT(*) AS n, SUM(n) AS s FROM c",
        )
        .rows;
    assert_eq!(
        r[0],
        vec![Value::Int(2), Value::Int(3)],
        "UNION 去重须覆盖 anchor 臂的重复行"
    );
}

#[test]
fn scalar_subquery_multiple_rows_is_an_error() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE e (v INT)");
    run(&mut db, "INSERT INTO e VALUES (10), (30)");
    let m = db
        .execute("SELECT 1 WHERE 1 = (SELECT v FROM e)")
        .expect_err("multi-row scalar subquery must be an error");
    assert!(m.to_string().contains("more than one row"), "{m}");
}

#[test]
fn in_between_like_probe_plans_in_explain() {
    let mut db = Database::in_memory().unwrap();
    run(
            &mut db,
            "CREATE TABLE pt (id INT PRIMARY KEY NOT NULL, v INT NOT NULL, s TEXT NOT NULL, p INT NOT NULL)",
        );
    run(&mut db, "CREATE INDEX ix_pt_v ON pt (v)");
    run(&mut db, "CREATE INDEX ix_pt_s ON pt (s)");
    for i in 1..=40 {
        run(
            &mut db,
            &format!(
                "INSERT INTO pt VALUES ({i}, {}, 'name{}', {})",
                i % 10,
                i % 10,
                i % 10
            ),
        );
    }
    let details = |db: &mut Database, sql: &str| -> Vec<String> {
        match run(db, sql) {
            ExecOutcome::Rows(r) => r
                .rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|v| v.as_str().unwrap_or_default().to_string())
                        .collect::<Vec<_>>()
                        .join(" | ")
                })
                .collect(),
            other => panic!("expected plan rows, got {other:?}"),
        }
    };
    let has = |d: &[String], needle: &str| d.iter().any(|l| l.contains(needle));
    // IN 多探针:PROBE 行 + 无残余(exact)。
    let d = details(&mut db, "EXPLAIN SELECT * FROM pt WHERE v IN (3, 7)");
    assert!(has(&d, "INDEX PROBE ON pt USING v"), "{d:?}");
    assert!(has(&d, "IN (3, 7)"), "{d:?}");
    assert!(!has(&d, "residual"), "{d:?}");
    // 大列表摘要显示。
    let big: Vec<String> = (0..30).map(|i| i.to_string()).collect();
    let d = details(
        &mut db,
        &format!("EXPLAIN SELECT * FROM pt WHERE v IN ({})", big.join(", ")),
    );
    assert!(has(&d, "+14 more"), "{d:?}");
    // 混合 conjunct:探针 + 残余。
    let d = details(
        &mut db,
        "EXPLAIN SELECT * FROM pt WHERE v IN (3, 7) AND p > 1",
    );
    assert!(has(&d, "INDEX PROBE"), "{d:?}");
    assert!(has(&d, "residual WHERE"), "{d:?}");
    // BETWEEN 闭区间。
    let d = details(&mut db, "EXPLAIN SELECT * FROM pt WHERE v BETWEEN 2 AND 4");
    assert!(has(&d, "INDEX PROBE ON pt USING v"), "{d:?}");
    assert!(has(&d, ">= 2"), "{d:?}");
    assert!(has(&d, "<= 4"), "{d:?}");
    // LIKE 前缀带。
    let d = details(&mut db, "EXPLAIN SELECT * FROM pt WHERE s LIKE 'name3%'");
    assert!(has(&d, "INDEX PROBE ON pt USING s"), "{d:?}");
    assert!(has(&d, ">= 'name3'"), "{d:?}");
    assert!(has(&d, "< 'name4'"), "{d:?}");
    // IN (SELECT …) 在执行期由预 pass 改写成字面量 InList 后走探针
    // (见 parity 测试);EXPLAIN 镜像不执行子查询,报告的是改写前
    // 形状 —— 这是镜像的既有边界,不是探针缺席。
    let d = details(
        &mut db,
        "EXPLAIN SELECT * FROM pt WHERE v IN (SELECT v FROM pt WHERE id <= 5)",
    );
    assert!(has(&d, "FULL HEAP SCAN ON pt"), "{d:?}");
    // 不消费的形状保持全扫:NOT IN / 中段 % / ILIKE / 非索引列。
    for sql in [
        "EXPLAIN SELECT * FROM pt WHERE v NOT IN (3, 7)",
        "EXPLAIN SELECT * FROM pt WHERE s LIKE 'n%3%'",
        "EXPLAIN SELECT * FROM pt WHERE s ILIKE 'NAME3%'",
        "EXPLAIN SELECT * FROM pt WHERE p IN (3, 7)",
    ] {
        let d = details(&mut db, sql);
        assert!(has(&d, "FULL HEAP SCAN ON pt"), "{sql} -> {d:?}");
    }
}

#[test]
fn in_between_probe_windows_asc_desc_exact_and_residual() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE wn (id INT PRIMARY KEY NOT NULL, v INT NOT NULL, p INT NOT NULL)",
    );
    run(&mut db, "CREATE INDEX ix_wn_v ON wn (v)");
    for i in 1..=200 {
        run(
            &mut db,
            &format!("INSERT INTO wn VALUES ({i}, {}, {})", i % 7, i % 7),
        );
    }
    let parity = |db: &mut Database, sql_v: &str, sql_p: &str| {
        let a = rows(db, sql_v).rows;
        let b = rows(db, sql_p).rows;
        assert_eq!(a, b, "窗口结果不得依赖索引存在性:\n{sql_v}\n{sql_p}");
    };
    // ASC/DESC 窗口 + In(exact:探针消费整个 WHERE,cap 截断)。
    parity(
        &mut db,
        "SELECT id FROM wn WHERE v IN (3, 5) ORDER BY id LIMIT 10",
        "SELECT id FROM wn WHERE p IN (3, 5) ORDER BY id LIMIT 10",
    );
    parity(
        &mut db,
        "SELECT id FROM wn WHERE v IN (3, 5) ORDER BY id DESC LIMIT 10 OFFSET 8",
        "SELECT id FROM wn WHERE p IN (3, 5) ORDER BY id DESC LIMIT 10 OFFSET 8",
    );
    // 树序窗口:ORDER BY 探针键自身。
    parity(
        &mut db,
        "SELECT id FROM wn WHERE v IN (3, 5) ORDER BY v LIMIT 12",
        "SELECT id FROM wn WHERE p IN (3, 5) ORDER BY p LIMIT 12",
    );
    // BETWEEN 闭区间 + DESC 窗口 + OFFSET。
    parity(
        &mut db,
        "SELECT id FROM wn WHERE v BETWEEN 2 AND 4 ORDER BY id DESC LIMIT 15 OFFSET 5",
        "SELECT id FROM wn WHERE p BETWEEN 2 AND 4 ORDER BY id DESC LIMIT 15 OFFSET 5",
    );
    // 非 exact(残余 conjunct 在场)窗口走全候选 + 逐行过滤。
    parity(
        &mut db,
        "SELECT id FROM wn WHERE v IN (1, 2) AND id % 3 = 0 ORDER BY id LIMIT 9",
        "SELECT id FROM wn WHERE p IN (1, 2) AND id % 3 = 0 ORDER BY id LIMIT 9",
    );
}

#[test]
fn like_prefix_probe_open_ended_and_class_fallback() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE ls (id INT PRIMARY KEY NOT NULL, s TEXT NOT NULL, ps TEXT NOT NULL)",
    );
    run(&mut db, "CREATE INDEX ix_ls_s ON ls (s)");
    run(
        &mut db,
        "INSERT INTO ls VALUES (1, 'x\u{10FFFF}a', 'x\u{10FFFF}a')",
    );
    run(
        &mut db,
        "INSERT INTO ls VALUES (2, 'x\u{10FFFF}b', 'x\u{10FFFF}b')",
    );
    run(&mut db, "INSERT INTO ls VALUES (3, 'ya', 'ya')");
    // 前缀止于 char::MAX:无有限后继 → 开放右界探针(残余过滤收尾),
    // 结果必须与无索引列一致。
    let parity = |db: &mut Database, sql_v: &str, sql_p: &str| {
        let a = rows(db, sql_v).rows;
        let b = rows(db, sql_p).rows;
        assert_eq!(a, b, "开放右界探针结果不得依赖索引存在性");
    };
    parity(
        &mut db,
        "SELECT id FROM ls WHERE s LIKE 'x\u{10FFFF}%'",
        "SELECT id FROM ls WHERE ps LIKE 'x\u{10FFFF}%'",
    );
}

#[test]
fn in_between_probes_respect_timestamp_band() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE tt (id INT PRIMARY KEY NOT NULL, ts TEXT, p TEXT)",
    );
    run(&mut db, "CREATE INDEX ix_tt_ts ON tt (ts)");
    let stamps = [
        "TIMESTAMP '2021-01-01T00:00:00Z'",
        "TIMESTAMP '2021-06-01T00:00:00Z'",
        "TIMESTAMP '2022-01-01T00:00:00Z'",
        "TIMESTAMP '2022-01-01T00:00:00Z'",
        "TIMESTAMP '2023-01-01T00:00:00Z'",
    ];
    for (i, st) in stamps.iter().enumerate() {
        let id = i + 1;
        run(
            &mut db,
            &format!("INSERT INTO tt VALUES ({id}, {st}, {st})"),
        );
    }
    // 纯 Timestamp 带上的 IN(可解析字符串元素真提升)与 BETWEEN
    // (字符串界提升):双生列差分。
    let parity = |db: &mut Database, sql_v: &str, sql_p: &str| {
        let a = rows(db, sql_v).rows;
        let b = rows(db, sql_p).rows;
        assert_eq!(
            a, b,
            "Timestamp 带探针结果不得依赖索引存在性:\n{sql_v}\n{sql_p}"
        );
    };
    parity(
            &mut db,
            "SELECT id FROM tt WHERE ts IN ('2021-06-01T00:00:00Z', '2023-01-01T00:00:00Z') ORDER BY id",
            "SELECT id FROM tt WHERE p IN ('2021-06-01T00:00:00Z', '2023-01-01T00:00:00Z') ORDER BY id",
        );
    parity(
            &mut db,
            "SELECT id FROM tt WHERE ts BETWEEN '2021-06-01T00:00:00Z' AND '2022-01-01T00:00:00Z' ORDER BY id",
            "SELECT id FROM tt WHERE p BETWEEN '2021-06-01T00:00:00Z' AND '2022-01-01T00:00:00Z' ORDER BY id",
        );
    // 不可解析字符串元素/前缀:探针回退全扫,残余过滤给出谓词语义
    // (对每个 Timestamp 键判未知 → 空结果)。
    parity(
        &mut db,
        "SELECT id FROM tt WHERE ts IN ('nope') ORDER BY id",
        "SELECT id FROM tt WHERE p IN ('nope') ORDER BY id",
    );
    parity(
        &mut db,
        "SELECT id FROM tt WHERE ts LIKE '2021%' ORDER BY id",
        "SELECT id FROM tt WHERE p LIKE '2021%' ORDER BY id",
    );
    assert!(rows(&mut db, "SELECT id FROM tt WHERE ts LIKE '2021%'")
        .rows
        .is_empty());
    // 混合带(Timestamp 与 Str 共存):整体回退,一行不丢。
    run(&mut db, "INSERT INTO tt VALUES (9, 'zzz', 'zzz')");
    parity(
        &mut db,
        "SELECT id FROM tt WHERE ts IN (TIMESTAMP '2021-01-01T00:00:00Z', 'zzz') ORDER BY id",
        "SELECT id FROM tt WHERE p IN (TIMESTAMP '2021-01-01T00:00:00Z', 'zzz') ORDER BY id",
    );
    let r = rows(
        &mut db,
        "SELECT id FROM tt WHERE ts IN (TIMESTAMP '2021-01-01T00:00:00Z', 'zzz') ORDER BY id",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(1)], vec![Value::Int(9)]]);
}

#[test]
fn numeric_bounds_on_timestamp_band_never_probe() {
    // 谓词侧 Timestamp↔数值比较恒为 NULL(行被排除),而 raw 全序里
    // Timestamp 排在数值之上——数值边界扫过 Timestamp 带会放进谓词
    // 必然排除的行。带型分析必须把数值边界拒下(回退全扫),同列
    // Timestamp/数值混合合取也不得被收紧吞并。
    let mut db = Database::in_memory().unwrap();
    run(
            &mut db,
            "CREATE TABLE tn (id INT PRIMARY KEY NOT NULL, ts TIMESTAMP NOT NULL, p TIMESTAMP NOT NULL)",
        );
    run(&mut db, "CREATE INDEX ix_tn_ts ON tn (ts)");
    run(
            &mut db,
            "INSERT INTO tn VALUES (1, TIMESTAMP '2026-01-01T00:00:00Z', TIMESTAMP '2026-01-01T00:00:00Z')",
        );
    run(
            &mut db,
            "INSERT INTO tn VALUES (2, TIMESTAMP '2026-06-01T00:00:00Z', TIMESTAMP '2026-06-01T00:00:00Z')",
        );
    let parity = |db: &mut Database, sql_v: &str, sql_p: &str| {
        let a = rows(db, sql_v).rows.clone();
        let b = rows(db, sql_p).rows.clone();
        assert_eq!(a, b, "数值边界结果不得依赖索引存在性:\n{sql_v}\n{sql_p}");
    };
    // 窗口路径(exact 探针会跳过残余过滤——修复前在这里返回行)。
    parity(
        &mut db,
        "SELECT id FROM tn WHERE ts > 0 ORDER BY ts LIMIT 1",
        "SELECT id FROM tn WHERE p > 0 ORDER BY p LIMIT 1",
    );
    parity(
        &mut db,
        "SELECT id FROM tn WHERE ts >= 0 ORDER BY ts DESC LIMIT 1",
        "SELECT id FROM tn WHERE p >= 0 ORDER BY p DESC LIMIT 1",
    );
    parity(
        &mut db,
        "SELECT id FROM tn WHERE ts < 999999999 ORDER BY ts LIMIT 1",
        "SELECT id FROM tn WHERE p < 999999999 ORDER BY p LIMIT 1",
    );
    // 无窗口的普通路径 + 同列混合合取(收紧不得吞掉数值项)。
    parity(
        &mut db,
        "SELECT id FROM tn WHERE ts > 0",
        "SELECT id FROM tn WHERE p > 0",
    );
    parity(
        &mut db,
        "SELECT id FROM tn WHERE ts > 0 AND ts > TIMESTAMP '2020-01-01T00:00:00Z'",
        "SELECT id FROM tn WHERE p > 0 AND p > TIMESTAMP '2020-01-01T00:00:00Z'",
    );
    // 正确答案本身:Timestamp 键对数值比较恒未知 → 0 行。
    assert!(rows(&mut db, "SELECT id FROM tn WHERE ts > 0")
        .rows
        .is_empty());
    // 数值带不受影响:整数边界照常走探针且答案正确。
    let r = rows(
        &mut db,
        "SELECT id FROM tn WHERE id > 1 ORDER BY id LIMIT 1",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(2)]]);
}

#[test]
fn in_probe_with_composite_prefix_and_duplicate_keys() {
    let mut db = Database::in_memory().unwrap();
    run(
            &mut db,
            "CREATE TABLE ci (id INT PRIMARY KEY NOT NULL, a INT NOT NULL, b INT NOT NULL, x INT NOT NULL, y INT NOT NULL)",
        );
    run(&mut db, "CREATE INDEX ix_ci_ab ON ci (a, b)");
    for i in 1..=60 {
        let (a, b) = (i % 3, i % 4);
        run(
            &mut db,
            &format!("INSERT INTO ci VALUES ({i}, {a}, {b}, {a}, {b})"),
        );
    }
    // 复合前缀 + IN:Prefix 计划消费 a=1,b IN 保留在残余过滤(非 exact),
    // 结果与无索引双生列一致。
    let a = rows(
        &mut db,
        "SELECT id FROM ci WHERE a = 1 AND b IN (2, 3) ORDER BY id",
    )
    .rows;
    let b = rows(
        &mut db,
        "SELECT id FROM ci WHERE x = 1 AND y IN (2, 3) ORDER BY id",
    )
    .rows;
    assert_eq!(a, b);
    assert_eq!(a.len(), 10); // a∈{1}, b∈{2,3}:每 12 个 id 命中 2 个 → 60/12*2
                             // 非唯一树同键多行:v IN 单值必须返回全部同键行(整对去重不丢行)。
    run(&mut db, "CREATE INDEX ix_ci_a ON ci (a)");
    let r = rows(&mut db, "SELECT COUNT(*) FROM ci WHERE a IN (1)").rows;
    assert_eq!(r, vec![vec![Value::Int(20)]]);
}

#[test]
fn timestamp_now_folds_to_literal_for_replication() {
    // 红线 #8:时间类非确定值必须定值回写。NOW() 在写语句里折叠为
    // 字面量,resolved SQL 与原始文本不再含 NOW(),副本重放零偏差。
    let mut a = Database::in_memory().unwrap();
    run(
        &mut a,
        "CREATE TABLE t3 (id INT PRIMARY KEY, at TIMESTAMP, who TEXT)",
    );
    let sql = "INSERT INTO t3 VALUES (1, NOW(), 'x')";
    a.execute(sql).unwrap();
    let resolved = a.take_resolved_sql().expect("NOW() 必须触发定值回写");
    assert!(
        !resolved.to_uppercase().contains("NOW("),
        "resolved 不得再含 NOW(): {resolved}"
    );
    assert!(
        resolved.contains("CAST(") && resolved.contains("AS TIMESTAMP"),
        "resolved 必须是 TIMESTAMP 字面量: {resolved}"
    );
    // The folded text replays EXACTLY on the replica.
    let mut b = Database::in_memory().unwrap();
    run(
        &mut b,
        "CREATE TABLE t3 (id INT PRIMARY KEY, at TIMESTAMP, who TEXT)",
    );
    b.execute(&resolved).unwrap();
    let ra = rows(&mut a, "SELECT at, who FROM t3");
    let rb = rows(&mut b, "SELECT at, who FROM t3");
    assert_eq!(ra.rows, rb.rows, "副本重放必须零偏差");
    assert!(b.take_resolved_sql().is_none(), "字面量重放不再折叠");
    // SYSDATE() (compat text form) folds to a string literal too.
    a.execute("INSERT INTO t3 VALUES (2, CAST('2026-01-01T00:00:00Z' AS TIMESTAMP), SYSDATE())")
        .unwrap();
    let resolved = a.take_resolved_sql().expect("SYSDATE() 必须折叠");
    assert!(
        !resolved.to_uppercase().contains("SYSDATE("),
        "resolved 不得再含 SYSDATE(): {resolved}"
    );
}

#[test]
fn timestamp_cast_and_arithmetic() {
    let mut db = Database::in_memory().unwrap();
    // CAST both directions.
    let r = rows(&mut db, "SELECT CAST('2026-01-01T00:00:00Z' AS TIMESTAMP)");
    assert!(matches!(&r.rows[0][0], Value::Timestamp(ms) if *ms == 1_767_225_600_000));
    let r = rows(
        &mut db,
        "SELECT CAST(CAST('2026-01-01T00:00:00Z' AS TIMESTAMP) AS INT)",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(1_767_225_600_000)]]);
    // Malformed CAST is a loud error (not NULL — CAST is explicit).
    assert!(db.execute("SELECT CAST('nope' AS TIMESTAMP)").is_err());
    // Arithmetic: + ms, - ms, difference of two instants.
    let r = rows(
        &mut db,
        "SELECT CAST('2026-01-01T00:00:00Z' AS TIMESTAMP) + 1500",
    );
    assert!(matches!(&r.rows[0][0], Value::Timestamp(ms) if *ms == 1_767_225_600_000 + 1500));
    let r = rows(
            &mut db,
            "SELECT CAST('2026-01-02T00:00:00Z' AS TIMESTAMP) - CAST('2026-01-01T00:00:00Z' AS TIMESTAMP)",
        );
    assert_eq!(r.rows, vec![vec![Value::Int(86_400_000)]]);
    // Timestamp + Timestamp is meaningless: NULL like other type errors
    // in the arithmetic path? No — an explicit error, matching arith's
    // non-numeric rule.
    assert!(db
            .execute(
                "SELECT CAST('2026-01-01T00:00:00Z' AS TIMESTAMP) + CAST('2026-01-01T00:00:00Z' AS TIMESTAMP)"
            )
            .is_err());
    // VALUE_LITERAL round-trip: stored Timestamps replay as TIMESTAMP
    // literals, never degrading to text.
    let lit = value_literal(&Value::Timestamp(1_767_225_600_000)).unwrap();
    assert_eq!(lit, "CAST('2026-01-01T00:00:00.000Z' AS TIMESTAMP)");
    let mut db2 = Database::in_memory().unwrap();
    run(&mut db2, "CREATE TABLE rt (v VALUE)");
    // (schemaless columns accept any scalar; the literal re-parses to
    // the same Timestamp)
    db2.execute(&format!("INSERT INTO rt VALUES ({lit})"))
        .unwrap();
    let r = rows(&mut db2, "SELECT v FROM rt");
    assert!(matches!(&r.rows[0][0], Value::Timestamp(ms) if *ms == 1_767_225_600_000));
}

// ---- 用户视图:CREATE VIEW / SELECT 穿透 / 写拒绝 / dump 往返 ----

#[test]
fn view_create_select_and_replace() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
    run(&mut db, "INSERT INTO t VALUES (1, 'a'), (2, 'b')");
    run(
        &mut db,
        "CREATE VIEW big AS SELECT id, v FROM t WHERE id > 1",
    );
    let r = rows(&mut db, "SELECT id, v FROM big ORDER BY id");
    assert_eq!(r.rows, vec![vec![Value::Int(2), Value::Str("b".into())]]);
    // WHERE over the view (outer filter composes with the body filter).
    let r = rows(&mut db, "SELECT id FROM big WHERE v = 'b'");
    assert_eq!(r.rows, vec![vec![Value::Int(2)]]);
    // OR REPLACE re-points the view.
    run(&mut db, "CREATE OR REPLACE VIEW big AS SELECT id FROM t");
    let r = rows(&mut db, "SELECT id FROM big ORDER BY id");
    assert_eq!(r.rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
    // View of a view.
    run(
        &mut db,
        "CREATE VIEW small AS SELECT id FROM big WHERE id = 1",
    );
    let r = rows(&mut db, "SELECT id FROM small");
    assert_eq!(r.rows, vec![vec![Value::Int(1)]]);
    // Body referencing a missing table fails at CREATE time.
    assert!(db.execute("CREATE VIEW bad AS SELECT * FROM nope").is_err());
    // Duplicate name without OR REPLACE errors.
    assert!(db.execute("CREATE VIEW small AS SELECT 1").is_err());
    assert!(db
        .execute("CREATE VIEW small IF NOT EXISTS AS SELECT 1")
        .is_ok());
    // Self-reference is impossible (name resolves only after creation).
    assert!(db
        .execute("CREATE OR REPLACE VIEW small AS SELECT * FROM small")
        .is_err());
}

#[test]
fn view_rejects_writes_and_schema_changes() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY)");
    run(&mut db, "CREATE VIEW v AS SELECT * FROM t");
    for sql in [
        "INSERT INTO v VALUES (1)",
        "UPDATE v SET id = 2",
        "DELETE FROM v",
        "TRUNCATE TABLE v",
        "ALTER TABLE v ADD COLUMN x INT",
        "CREATE INDEX iv ON v (id)",
    ] {
        assert!(db.execute(sql).is_err(), "{sql} must fail on a view");
    }
    // Wrong-kind drops are loud.
    assert!(db.execute("DROP TABLE v").is_err());
    run(&mut db, "DROP VIEW v");
    assert!(db.execute("DROP VIEW IF EXISTS v").is_ok());
    assert!(
        db.execute("SELECT * FROM v").is_err(),
        "dropped view is gone"
    );
    // A table named like a view cannot be DROP VIEW'd.
    assert!(db.execute("DROP VIEW t").is_err());
}

#[test]
fn view_cycle_via_or_replace_is_rejected() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE base (id INT)");
    run(&mut db, "CREATE VIEW a AS SELECT * FROM base");
    run(&mut db, "CREATE VIEW b AS SELECT * FROM a");
    // a→b→a through OR REPLACE used to create a catalog cycle that
    // stack-overflowed the process on the next SELECT; it must fail at
    // CREATE time and leave the catalog untouched.
    assert!(db
        .execute("CREATE OR REPLACE VIEW a AS SELECT * FROM b")
        .is_err());
    assert!(db.execute("SELECT * FROM a").is_ok(), "a still resolves");
    // Longer cycles are caught the same way.
    run(&mut db, "CREATE VIEW c AS SELECT * FROM b");
    assert!(db
        .execute("CREATE OR REPLACE VIEW a AS SELECT * FROM c")
        .is_err());
}

#[test]
fn view_expansion_depth_is_bounded() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE base (id INT)");
    run(&mut db, "INSERT INTO base VALUES (7)");
    run(&mut db, "CREATE VIEW v0 AS SELECT * FROM base");
    // Chains up to the budget build and select fine.
    for i in 1..=15 {
        run(
            &mut db,
            &format!("CREATE VIEW v{i} AS SELECT * FROM v{}", i - 1),
        );
    }
    let r = rows(&mut db, "SELECT id FROM v15");
    assert_eq!(r.rows, vec![vec![Value::Int(7)]]);
    // One more level is refused at CREATE: the dry-run (the view under
    // construction plus its body's chain) already exceeds the budget,
    // so a chain too deep to SELECT never enters the catalog.
    assert!(db.execute("CREATE VIEW v16 AS SELECT * FROM v15").is_err());
    // The guard itself is cycle defense for corrupt/legacy catalogs:
    // a self-referencing entry planted directly into the catalog (only
    // possible outside CREATE, which rejects cycles) errors loudly
    // instead of recursing until the stack dies.
    db.tables.insert(
        "loop".into(),
        std::sync::Arc::new(TableMeta {
            view_sql: Some("SELECT * FROM loop".into()),
            ..Default::default()
        }),
    );
    let err = db.execute("SELECT * FROM loop").unwrap_err();
    assert!(err.to_string().contains("too deep"), "{err}");
}

#[test]
fn view_count_and_order_window_expand_instead_of_empty_storage() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
    run(&mut db, "INSERT INTO t VALUES (1,'a'), (2,'b'), (3,'c')");
    run(
        &mut db,
        "CREATE VIEW big AS SELECT id, v FROM t WHERE id > 1",
    );
    // COUNT(*) used to answer 0 off the view's empty heap fast path.
    let r = rows(&mut db, "SELECT COUNT(*) FROM big");
    assert_eq!(r.rows[0][0], Value::Int(2));
    // ORDER BY + LIMIT used to return an empty window for the same
    // reason (the EF/console pagination shape).
    let r = rows(&mut db, "SELECT id FROM big ORDER BY id LIMIT 2");
    assert_eq!(r.rows, vec![vec![Value::Int(2)], vec![Value::Int(3)]]);
    let r = rows(&mut db, "SELECT id FROM big ORDER BY id DESC LIMIT 1");
    assert_eq!(r.rows, vec![vec![Value::Int(3)]]);
}

#[test]
fn merge_into_view_is_rejected() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    run(&mut db, "INSERT INTO t VALUES (1, 10)");
    run(&mut db, "CREATE TABLE src (id INT, v INT)");
    run(&mut db, "INSERT INTO src VALUES (9, 90)");
    run(&mut db, "CREATE VIEW vw AS SELECT * FROM t");
    // MERGE's INSERT branch used to bypass the view guard and write an
    // invisible row into the view's private empty heap.
    assert!(db
        .execute(
            "MERGE INTO vw USING src ON vw.id = src.id \
                 WHEN NOT MATCHED THEN INSERT VALUES (src.id, src.v)"
        )
        .is_err());
    let dump = db.dump_script().unwrap();
    assert!(!dump.contains("INSERT INTO vw"), "no zombie rows: {dump}");
}

#[test]
fn multi_target_ddl_is_atomic_on_failure() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE keep (id INT)");
    run(&mut db, "INSERT INTO keep VALUES (1)");
    run(&mut db, "CREATE VIEW v1 AS SELECT * FROM keep");
    run(&mut db, "CREATE VIEW v2 AS SELECT * FROM keep");
    // DROP VIEW with a missing tail: nothing is dropped (the first name
    // used to be dropped and committed before the error).
    assert!(db.execute("DROP VIEW v1, nope").is_err());
    assert!(db.execute("SELECT * FROM v1").is_ok(), "v1 survives");
    assert!(db.execute("DROP VIEW v2, keep").is_err());
    assert!(db.execute("SELECT * FROM v2").is_ok(), "v2 survives");
    // TRUNCATE with a missing tail: the first table keeps its rows.
    assert!(db.execute("TRUNCATE keep, missing").is_err());
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM keep").rows[0][0],
        Value::Int(1)
    );
    // DROP INDEX with a missing tail: the first index survives (the
    // in-memory catalog used to lose it without a journal entry).
    run(&mut db, "CREATE TABLE t2 (x INT)");
    run(&mut db, "CREATE INDEX ia ON keep (id)");
    run(&mut db, "CREATE INDEX ib ON t2 (x)");
    assert!(db.execute("DROP INDEX ia, nope").is_err());
    let r = rows(
        &mut db,
        "SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'ia'",
    );
    assert_eq!(r.rows.len(), 1, "ia survives");
    // Co-truncated FK pairs are legal: both tables empty in one
    // statement, no orphan check on the parent the child references.
    run(&mut db, "CREATE TABLE parent (id INT PRIMARY KEY)");
    run(
        &mut db,
        "CREATE TABLE child (pid INT, FOREIGN KEY (pid) REFERENCES parent (id))",
    );
    run(&mut db, "INSERT INTO parent VALUES (1)");
    run(&mut db, "INSERT INTO child VALUES (1)");
    run(&mut db, "TRUNCATE parent, child");
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM child").rows[0][0],
        Value::Int(0)
    );
}

#[test]
fn dump_orders_views_dependency_first_and_replays() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE base (id INT)");
    run(&mut db, "INSERT INTO base VALUES (1)");
    // `a` reads `z`: alphabetical dump order replayed CREATE a before z
    // existed, and the whole restore failed the dry-run.
    run(&mut db, "CREATE VIEW z AS SELECT * FROM base");
    run(&mut db, "CREATE VIEW a AS SELECT * FROM z");
    let dump = db.dump_script().unwrap();
    assert!(
        dump.find("CREATE VIEW \"z\"").unwrap() < dump.find("CREATE VIEW \"a\"").unwrap(),
        "{dump}"
    );
    let mut fresh = Database::in_memory().unwrap();
    for stmt in dump.split(";\n").filter(|x| !x.trim().is_empty()) {
        run(&mut fresh, stmt);
    }
    assert_eq!(
        rows(&mut fresh, "SELECT id FROM a").rows,
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn timestamp_parsing_rejects_multibyte_offsets_without_panicking() {
    // Four bytes, but not four digits: the old byte-length check sliced
    // into the middle of a multibyte char and panicked the executor.
    assert_eq!(
        crate::value::parse_timestamp_ms("2026-09-15T12:30:00+1中"),
        None
    );
    assert_eq!(
        crate::value::parse_timestamp_ms("2026-09-15T12:30:00+中1"),
        None
    );
    let mut db = Database::in_memory().unwrap();
    assert!(db
        .execute("SELECT CAST('2026-09-15T12:30:00+1中' AS TIMESTAMP)")
        .is_err());
    // The comparison-promotion path (formerly the same panic): the
    // predicate degrades to NULL, not a crash.
    run(&mut db, "CREATE TABLE t (id INT, ts TIMESTAMP)");
    run(
        &mut db,
        "INSERT INTO t VALUES (1, CAST('2026-09-15T12:30:00Z' AS TIMESTAMP))",
    );
    let r = rows(
        &mut db,
        "SELECT id FROM t WHERE ts > '2026-09-15T12:30:00+1中'",
    );
    assert!(r.rows.is_empty());
}

#[test]
fn timestamp_domain_is_enforced() {
    let mut db = Database::in_memory().unwrap();
    // Out-of-range instants have no canonical text form: admitting them
    // used to produce dumps the restore could not re-parse.
    assert!(db
        .execute("SELECT CAST(253402300800000 AS TIMESTAMP)")
        .is_err());
    assert!(db
        .execute("SELECT CAST(-62135596800001 AS TIMESTAMP)")
        .is_err());
    assert!(db
        .execute("INSERT INTO t VALUES (1, CAST(253402300800000 AS TIMESTAMP))")
        .is_err());
    // The exact boundaries are legal.
    let r = rows(
        &mut db,
        "SELECT CAST('0001-01-01T00:00:00.000Z' AS TIMESTAMP), \
                    CAST('9999-12-31T23:59:59.999Z' AS TIMESTAMP)",
    );
    assert_eq!(r.rows[0][0], Value::Timestamp(-62_135_596_800_000));
    assert_eq!(r.rows[0][1], Value::Timestamp(253_402_300_799_999));
    // Arithmetic past the domain degrades to NULL (SQLite overflow
    // style), never to an unrepresentable instant.
    let r = rows(
        &mut db,
        "SELECT CAST('9999-12-31T23:59:59.999Z' AS TIMESTAMP) + 1",
    );
    assert_eq!(r.rows[0][0], Value::Null);
    // The wire marker decodes only inside the domain; outside it stays
    // a plain object rather than a broken Timestamp.
    assert!(matches!(
        crate::json::from_str("{\"$ts\":0}").unwrap(),
        Value::Timestamp(0)
    ));
    assert!(matches!(
        crate::json::from_str("{\"$ts\":253402300800000}").unwrap(),
        Value::Object(_)
    ));
}

#[test]
fn default_now_resolves_per_insert_and_rewrites_the_journal_text() {
    let mut db = Database::in_memory().unwrap();
    // The DDL keeps its call text: folding it would freeze the CREATE
    // time into every future row.
    run(
        &mut db,
        "CREATE TABLE t (id INT, ts TIMESTAMP DEFAULT NOW())",
    );
    run(&mut db, "INSERT INTO t (id) VALUES (1)");
    let resolved = db.take_resolved_sql().unwrap();
    assert!(resolved.contains("AS TIMESTAMP"), "{resolved}");
    assert!(!resolved.contains("NOW()"), "{resolved}");
    // Insert-time semantics: a later row gets a later instant.
    std::thread::sleep(std::time::Duration::from_millis(3));
    run(&mut db, "INSERT INTO t (id) VALUES (2)");
    let _ = db.take_resolved_sql();
    let r = rows(&mut db, "SELECT ts FROM t ORDER BY id");
    match (&r.rows[0][0], &r.rows[1][0]) {
        (Value::Timestamp(a), Value::Timestamp(b)) => assert!(b > a),
        other => panic!("expected timestamps: {other:?}"),
    }
    // CHECK constraints must not carry wall-clock calls at all: every
    // node would evaluate a different instant.
    assert!(db
        .execute("CREATE TABLE bad (x TIMESTAMP, CHECK (x < NOW()))")
        .is_err());
    // NOW_MS() used to escape every wall-clock gate: the fold scanner
    // word-matches `now`/`current_timestamp` while the cheap prefilter
    // substring-matches `now` too — the fold branch ran, matched nothing,
    // and journaled the raw call for every replay peer to re-stamp.
    run(
        &mut db,
        "CREATE TABLE nm (id INT, ts TIMESTAMP DEFAULT NOW_MS())",
    );
    run(&mut db, "INSERT INTO nm (id) VALUES (1)");
    let resolved = db.take_resolved_sql().unwrap();
    assert!(resolved.contains("AS TIMESTAMP"), "{resolved}");
    assert!(!resolved.contains("NOW_MS()"), "{resolved}");
    run(&mut db, "INSERT INTO nm VALUES (2, NOW_MS())");
    let resolved = db.take_resolved_sql().unwrap();
    assert!(!resolved.contains("NOW_MS()"), "{resolved}");
    assert!(db
        .execute("CREATE TABLE badnm (x TIMESTAMP, CHECK (x < NOW_MS()))")
        .is_err());
    // MERGE has no canonical rewrite; applying a wall-clock DEFAULT
    // through it would silently diverge replicas — explicit refusal.
    run(&mut db, "CREATE TABLE src (id INT)");
    run(&mut db, "INSERT INTO src VALUES (5)");
    assert!(db
        .execute(
            "MERGE INTO t USING src ON t.id = src.id \
                 WHEN NOT MATCHED THEN INSERT (id) VALUES (src.id)"
        )
        .is_err());
}

#[test]
fn composite_index_timestamp_string_bounds_are_promoted() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE cx (ts TIMESTAMP, x INT, id INT PRIMARY KEY)",
    );
    run(&mut db, "CREATE INDEX ctsx ON cx (ts, x)");
    run(
        &mut db,
        "INSERT INTO cx VALUES (CAST('2026-06-01T00:00:00Z' AS TIMESTAMP), 9, 1)",
    );
    run(
        &mut db,
        "INSERT INTO cx VALUES (CAST('2026-07-01T00:00:00Z' AS TIMESTAMP), 8, 2)",
    );
    // Leading position, equality on both columns with a bare string:
    // the unpromoted Str prefix used to probe an empty band.
    let r = rows(
        &mut db,
        "SELECT id FROM cx WHERE ts = '2026-06-01T00:00:00Z' AND x = 9",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(1)]]);
    // Leading position, range bounds.
    let r = rows(
        &mut db,
        "SELECT id FROM cx WHERE ts >= '2026-06-01T00:00:00Z' \
             AND ts <= '2026-07-01T00:00:00Z' ORDER BY id",
    );
    assert_eq!(r.rows.len(), 2);
}

#[test]
fn timestamp_string_comparisons_are_consistent_across_predicates() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, ts TIMESTAMP)");
    run(
        &mut db,
        "INSERT INTO t VALUES (1, CAST('2026-06-01T00:00:00Z' AS TIMESTAMP))",
    );
    run(
        &mut db,
        "INSERT INTO t VALUES (2, CAST('2027-01-01T00:00:00Z' AS TIMESTAMP))",
    );
    // BETWEEN must agree with the equivalent >= AND <= (only the binop
    // funnel used to coerce timestamp strings).
    let between = rows(
        &mut db,
        "SELECT id FROM t WHERE ts BETWEEN '2026-01-01' AND '2027-06-01' ORDER BY id",
    );
    let ge_le = rows(
        &mut db,
        "SELECT id FROM t WHERE ts >= '2026-01-01' AND ts <= '2027-06-01' ORDER BY id",
    );
    assert_eq!(between.rows, ge_le.rows);
    assert_eq!(between.rows.len(), 2);
    // IN matches the same rows as =.
    let r = rows(
        &mut db,
        "SELECT id FROM t WHERE ts IN ('2026-06-01T00:00:00Z')",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(1)]]);
    // CASE compares like =.
    let r = rows(
        &mut db,
        "SELECT CASE ts WHEN '2026-06-01T00:00:00Z' THEN 'hit' ELSE 'miss' END AS r \
             FROM t WHERE id = 1",
    );
    assert_eq!(r.rows[0][0], Value::Str("hit".into()));
    // IS NOT DISTINCT FROM compares like =.
    let r = rows(
        &mut db,
        "SELECT id FROM t WHERE ts IS NOT DISTINCT FROM '2026-06-01T00:00:00Z'",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn subquery_inlining_carries_exact_scalar_literals() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE src (id INT PRIMARY KEY, amount DECIMAL, ts TIMESTAMP, data BLOB)",
    );
    run(
        &mut db,
        "INSERT INTO src VALUES (1, CAST('1.5' AS DECIMAL), \
             CAST('2026-06-01T00:00:00Z' AS TIMESTAMP), x'0a0b')",
    );
    // Scalar subqueries over DECIMAL/TIMESTAMP/BLOB columns used to
    // fail with "cannot inline value as literal".
    let r = rows(
        &mut db,
        "SELECT id FROM src WHERE amount = (SELECT amount FROM src WHERE id = 1)",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(1)]]);
    let r = rows(
        &mut db,
        "SELECT id FROM src WHERE ts = (SELECT ts FROM src WHERE id = 1)",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(1)]]);
    let r = rows(
        &mut db,
        "SELECT id FROM src WHERE data IN (SELECT data FROM src)",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn descending_index_creation_is_rejected() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT, v INT)");
    // DESC used to be silently ignored (an ASC tree was built); it must
    // be refused like every other unimplemented index option.
    assert!(db.execute("CREATE INDEX d ON t (v DESC)").is_err());
    assert!(db.execute("CREATE INDEX d ON t (v ASC)").is_ok());
}

#[test]
fn journal_trim_bytes_drops_oldest_entries() {
    let mut db = Database::in_memory().unwrap();
    for i in 0..50 {
        db.journal_append(&format!("-- entry {i} padding padding padding"))
            .unwrap();
    }
    // A tiny byte cap keeps only the newest entries' bytes.
    db.journal_trim_bytes(300).unwrap();
    let head = db.journal_head().unwrap();
    let oldest = db.journal_oldest().unwrap();
    assert!(oldest > 1, "oldest trimmed: {oldest}");
    assert_eq!(head, 50, "newest entries are never trimmed away");
    let entries = db.journal_entries_after(oldest - 1, 1000).unwrap();
    assert_eq!(entries.len() as u64, head - oldest + 1, "contiguous tail");
}

#[test]
fn view_survives_dump_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("view.db");
    {
        let mut db = Database::open(&path).unwrap();
        run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
        run(
            &mut db,
            "CREATE OR REPLACE VIEW v AS SELECT id FROM t WHERE id > 0",
        );
        run(&mut db, "INSERT INTO t VALUES (1, 'a')");
        // 视图指向的表不带 CASCADE 删除会被拒绝;CASCADE 连带删视图。
        let e = db.execute("DROP TABLE t").unwrap_err();
        assert!(e.to_string().contains("referenced by VIEW"), "{e}");
        run(&mut db, "DROP TABLE t CASCADE");
        assert!(db.execute("SELECT * FROM v").is_err());
        run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
        run(
            &mut db,
            "CREATE OR REPLACE VIEW v AS SELECT id FROM t WHERE id > 0",
        );
        run(&mut db, "INSERT INTO t VALUES (1, 'a')");
    }
    let script = {
        let mut db = Database::open(&path).unwrap();
        let dump = db.dump_script().unwrap();
        // Replay into a fresh database (backup restore semantics).
        let mut fresh = Database::open(&dir.path().join("restored.db")).unwrap();
        let batch = fresh.execute_batch(&dump);
        assert!(
            batch.error.is_none(),
            "dump replay failed: {:?}",
            batch.error
        );
        dump
    };
    assert!(
        script.contains("CREATE VIEW"),
        "dump 必须携带视图: {script}"
    );
    assert!(
        script.contains("DROP VIEW IF EXISTS"),
        "dump 必须先清视图: {script}"
    );
    // Reopened / replayed databases answer through the view.
    for p in ["view.db", "restored.db"] {
        let mut db = Database::open(&dir.path().join(p)).unwrap();
        let r = rows(&mut db, "SELECT id FROM v");
        assert_eq!(r.rows, vec![vec![Value::Int(1)]], "{p}");
    }
}

// ---- 覆盖率补强:TIMESTAMP/视图错误路径与边界 ----

#[test]
fn timestamp_arithmetic_and_cast_error_paths() {
    let mut db = Database::in_memory().unwrap();
    // Int + Timestamp(mirror)与 Timestamp - Int。
    let r = rows(
            &mut db,
            "SELECT 1000 + CAST('1970-01-01T00:00:00Z' AS TIMESTAMP),              CAST('1970-01-01T00:00:01Z' AS TIMESTAMP) - 1000",
        );
    assert!(matches!(&r.rows[0][0], Value::Timestamp(ms) if *ms == 1000));
    assert!(matches!(&r.rows[0][1], Value::Timestamp(ms) if *ms == 0));
    // 数值与 TIMESTAMP 比较无隐式语义 → NULL。
    let r = rows(
        &mut db,
        "SELECT CAST('2026-01-01T00:00:00Z' AS TIMESTAMP) = 1767225600000, \
                    CAST('2026-01-01T00:00:00Z' AS TIMESTAMP) > 3.5",
    );
    assert_eq!(r.rows, vec![vec![Value::Null, Value::Null]]);
    // 溢出折叠为 NULL(checked 算术)。
    let r = rows(
        &mut db,
        "SELECT CAST('2026-01-01T00:00:00Z' AS TIMESTAMP) + 9223372036854775807",
    );
    assert_eq!(r.rows, vec![vec![Value::Null]]);
    // 非法 CAST:字符串不解析、Float/Decimal/Blob 源未定义。
    assert!(db.execute("SELECT CAST('x' AS TIMESTAMP)").is_err());
    assert!(db.execute("SELECT CAST(1.5 AS TIMESTAMP)").is_err());
    assert!(db.execute("SELECT CAST(x'00' AS TIMESTAMP)").is_err());
    // TIMESTAMP → TEXT 走规范 ISO 形式(Display 权威)。
    let r = rows(
        &mut db,
        "SELECT CAST(CAST('2026-01-01T00:00:00Z' AS TIMESTAMP) AS TEXT)",
    );
    assert_eq!(
        r.rows,
        vec![vec![Value::Str("2026-01-01T00:00:00.000Z".into())]]
    );
}

#[test]
fn timestamp_typed_literal_variants() {
    let mut db = Database::in_memory().unwrap();
    // DATE '…' = 当日零点;DATETIME 别名;TIME 等未支持类型显式报错。
    let r = rows(&mut db, "SELECT DATE '2026-09-15'");
    assert!(matches!(&r.rows[0][0], Value::Timestamp(ms) if *ms == 1_789_430_400_000));
    let r = rows(&mut db, "SELECT DATETIME '2026-09-15T10:00:00Z'");
    assert!(matches!(&r.rows[0][0], Value::Timestamp(_)));
    assert!(db.execute("SELECT TIME '10:00:00'").is_err());
    // DEFAULT 位置也接受 TIMESTAMP 字面量(eval_const 路径)。
    run(&mut db, "CREATE TABLE ev4 (id INT PRIMARY KEY, at TIMESTAMP DEFAULT TIMESTAMP '2020-01-01T00:00:00Z')");
    run(&mut db, "INSERT INTO ev4 (id) VALUES (1)");
    let r = rows(&mut db, "SELECT at FROM ev4");
    assert!(matches!(&r.rows[0][0], Value::Timestamp(ms) if *ms == 1_577_836_800_000));
}

#[test]
fn timestamp_probe_mixed_band_needs_explicit_cast() {
    // 混合序带列(同列既有 Str 又有 Timestamp):字符串字面量边界不提升
    // (文档化边界)——显式 CAST 后按时间精确匹配。
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE mx (id INT PRIMARY KEY, at TIMESTAMP)",
    );
    run(&mut db, "CREATE INDEX idx_mx_at ON mx (at)");
    run(
        &mut db,
        "INSERT INTO mx VALUES (1, TIMESTAMP '2026-01-01T00:00:00Z')",
    );
    run(
        &mut db,
        "INSERT INTO mx VALUES (2, TIMESTAMP '2026-06-01T00:00:00Z')",
    );
    let r = rows(
        &mut db,
        "SELECT id FROM mx WHERE at = CAST('2026-06-01T00:00:00Z' AS TIMESTAMP)",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(2)]]);
}

#[test]
fn view_error_paths_and_read_through_writes() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE base_t (id INT PRIMARY KEY, v TEXT)");
    run(&mut db, "INSERT INTO base_t VALUES (1, 'x')");
    // 装饰/临时/保留名/列清单逐一显式拒绝(物化视图已升级为受支持)。
    assert!(
        db.execute("CREATE VIEW #tv AS SELECT 1").is_err(),
        "临时视图拒绝"
    );
    assert!(
        db.execute("CREATE VIEW v (a) AS SELECT 1").is_err(),
        "列清单拒绝"
    );
    assert!(
        db.execute("CREATE VIEW _cluster_log AS SELECT 1").is_err(),
        "引擎保留名拒绝"
    );
    assert!(
        db.execute("CREATE VIEW docsql_users AS SELECT 1").is_err(),
        "用户子系统保留名拒绝"
    );
    // 写语句经视图读源(读穿透):INSERT..SELECT 与 MERGE USING 都展开。
    run(&mut db, "CREATE VIEW vw AS SELECT id, v FROM base_t");
    run(&mut db, "CREATE TABLE dst (id INT PRIMARY KEY, v TEXT)");
    run(&mut db, "INSERT INTO dst SELECT * FROM vw");
    let r = rows(&mut db, "SELECT COUNT(*) FROM dst");
    assert_eq!(
        r.rows,
        vec![vec![Value::Int(1)]],
        "读穿透:视图作为 SELECT 源"
    );
    run(&mut db, "CREATE TABLE tgt (id INT PRIMARY KEY, v TEXT)");
    run(&mut db, "INSERT INTO tgt VALUES (1, 'x')");
    run(
        &mut db,
        "MERGE INTO tgt USING vw ON tgt.id = vw.id WHEN MATCHED THEN UPDATE SET v = vw.v",
    );
    // information_schema 照常工作(视图不产生列行,不炸页面)。
    let r = rows(&mut db, "SELECT COUNT(*) FROM information_schema.tables");
    assert!(!r.rows.is_empty());
}

// ---- 覆盖率补强:检查遍历器/分组形态/MERGE 分支/复合前缀探针 ----

#[test]
fn drop_column_check_walker_shapes() {
    // DROP COLUMN 走 expr_references_ident 的全部 AST 形态:
    // 每条 CHECK 用一种表达式,引用列被删即拒绝。
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE w (a INT, b INT, name TEXT, z INT,
             CHECK (a BETWEEN 0 AND 10),
             CHECK (a IN (1, 2, 3)),
             CHECK (LENGTH(name) > 0),
             CHECK (CASE WHEN a > 0 THEN 1 ELSE 0 END = 1),
             CHECK (-b < 5),
             CHECK (a + b > 0))",
    );
    for col in ["a", "b", "name"] {
        let e = db
            .execute(&format!("ALTER TABLE w DROP COLUMN {col}"))
            .unwrap_err();
        assert!(
            e.to_string().contains("references it"),
            "DROP {col} must report the CHECK reference: {e}"
        );
    }
    // 未被引用的列照常删除。
    run(&mut db, "ALTER TABLE w DROP COLUMN z");
}

#[test]
fn insert_null_walks_check_null_ref_shapes() {
    // INSERT 的 CHECK 求值走 expr_has_null_ref(CASE/BinaryOp/IsNull)。
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE n (v TEXT, CHECK (CASE WHEN v IS NOT NULL THEN 1 ELSE 0 END = 1))",
    );
    run(&mut db, "INSERT INTO n VALUES ('x')");
    // NULL 入 CASE → ELSE 0 = 1 为假:约束如实拒绝(NULL 语义走的是
    // 引用检查 expr_has_null_ref,不是免检)。
    assert!(db.execute("INSERT INTO n VALUES (NULL)").is_err());
}

#[test]
fn merge_clause_error_branches() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE m_t (id INT PRIMARY KEY, v TEXT)");
    run(&mut db, "CREATE TABLE m_s (id INT PRIMARY KEY, v TEXT)");
    run(&mut db, "INSERT INTO m_s VALUES (1, 'x')");
    // WHEN NOT MATCHED THEN UPDATE 拒绝。
    assert!(db
        .execute(
            "MERGE INTO m_t USING m_s ON m_t.id = m_s.id \
                 WHEN NOT MATCHED THEN UPDATE SET v = 'y'"
        )
        .is_err());
    // WHEN NOT MATCHED AND <谓词> 拒绝。
    assert!(db
        .execute(
            "MERGE INTO m_t USING m_s ON m_t.id = m_s.id \
                 WHEN NOT MATCHED AND m_s.v = 'x' THEN INSERT VALUES (1, 'x')"
        )
        .is_err());
    // MERGE INSERT ROW 拒绝。
    assert!(db
        .execute(
            "MERGE INTO m_t USING m_s ON m_t.id = m_s.id \
                 WHEN NOT MATCHED THEN INSERT ROW"
        )
        .is_err());
    // 重复 WHEN NOT MATCHED 拒绝。
    assert!(db
        .execute(
            "MERGE INTO m_t USING m_s ON m_t.id = m_s.id \
                 WHEN NOT MATCHED THEN INSERT VALUES (1, 'a') \
                 WHEN NOT MATCHED THEN INSERT VALUES (2, 'b')"
        )
        .is_err());
    // VALUES 行数非 1 拒绝。
    assert!(db
        .execute(
            "MERGE INTO m_t USING m_s ON m_t.id = m_s.id \
                 WHEN NOT MATCHED THEN INSERT VALUES (1, 'a'), (2, 'b')"
        )
        .is_err());
}

#[test]
fn having_and_group_expr_shapes() {
    // HAVING 引用检查(check_having_refs)与分组求值
    // (eval_group_expr)的函数/CASE/复合标识符/一元/CAST/嵌套形态。
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE emp (id INT, dept TEXT, n INT)");
    run(
        &mut db,
        "INSERT INTO emp VALUES (1, 'cs', 5), (2, 'cs', 3), (3, 'hr', 7)",
    );
    // 函数形态 + CAST 形态 + 一元形态。
    let r = rows(
        &mut db,
        "SELECT dept, COUNT(*) AS c FROM emp GROUP BY dept \
             HAVING LENGTH(dept) > 1 AND CAST(COUNT(*) AS TEXT) != '' AND -COUNT(*) < 0 \
             ORDER BY dept",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("cs".into()), Value::Int(2)],
            vec![Value::Str("hr".into()), Value::Int(1)]
        ]
    );
    // CASE 形态。
    let r = rows(
        &mut db,
        "SELECT dept FROM emp GROUP BY dept HAVING CASE WHEN COUNT(*) > 1 THEN 1 ELSE 0 END = 1",
    );
    assert_eq!(r.rows, vec![vec![Value::Str("cs".into())]]);
    // 复合标识符(别名限定)形态。
    let r = rows(
        &mut db,
        "SELECT e.dept FROM emp e GROUP BY e.dept HAVING e.dept = 'hr'",
    );
    assert_eq!(r.rows, vec![vec![Value::Str("hr".into())]]);
    // 未在 GROUP BY/聚合中的列 → 响亮报错。
    assert!(db
        .execute("SELECT dept FROM emp GROUP BY dept HAVING n > 0")
        .is_err());
}

#[test]
fn composite_index_prefix_probe_and_retain() {
    // 复合索引前缀探针:只按前导列等值(a=1)走 Prefix 扫描 + 逐键保留,
    // b 的取值不限制(前缀保留过滤必须留下全部 b 变体)。
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE cp (a INT, b INT, v TEXT)");
    run(&mut db, "CREATE INDEX idx_cp ON cp (a, b)");
    for i in 0..40 {
        run(
            &mut db,
            &format!("INSERT INTO cp VALUES (1, {}, 'r{i}')", i % 3),
        );
    }
    run(&mut db, "INSERT INTO cp VALUES (2, 9, 'other')");
    let r = rows(&mut db, "SELECT COUNT(*) FROM cp WHERE a = 1");
    assert_eq!(r.rows, vec![vec![Value::Int(40)]]);
    // 前缀 + 第二列范围:前缀探针 + 残余过滤。
    let r = rows(&mut db, "SELECT COUNT(*) FROM cp WHERE a = 1 AND b = 2");
    assert_eq!(r.rows, vec![vec![Value::Int(13)]]);
}

// ---- 银行场景合规:账务语义端到端 ----

fn ledger_db() -> Database {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE accounts (id INT PRIMARY KEY, balance DECIMAL NOT NULL)",
    );
    run(
        &mut db,
        "INSERT INTO accounts VALUES (1, CAST('1000.25' AS DECIMAL))",
    );
    run(
        &mut db,
        "INSERT INTO accounts VALUES (2, CAST('500.75' AS DECIMAL))",
    );
    db
}

fn total(db: &mut Database) -> String {
    let r = rows(db, "SELECT SUM(balance) FROM accounts");
    match &r.rows[0][0] {
        Value::Decimal(d) => d.to_string(),
        other => panic!("expected decimal total, got {other:?}"),
    }
}

#[test]
fn bank_ledger_transfer_is_atomic_and_exact() {
    let mut db = ledger_db();
    assert_eq!(total(&mut db), "1501.00", "期初合计必须精确");
    // 一笔转账:BEGIN 内两条 UPDATE,COMMIT 一次性落盘。
    run(&mut db, "BEGIN");
    run(
        &mut db,
        "UPDATE accounts SET balance = balance - CAST('0.10' AS DECIMAL) WHERE id = 1",
    );
    run(
        &mut db,
        "UPDATE accounts SET balance = balance + CAST('0.10' AS DECIMAL) WHERE id = 2",
    );
    run(&mut db, "COMMIT");
    assert_eq!(total(&mut db), "1501.00", "转账后合计必须分毫不差");
    let r = rows(&mut db, "SELECT balance FROM accounts WHERE id = 1");
    assert!(
        matches!(&r.rows[0][0], Value::Decimal(d) if d.to_string() == "1000.15"),
        "借方余额必须精确: {:?}",
        r.rows[0][0]
    );
}

#[test]
fn bank_ledger_rollback_and_failed_statement_preserve_invariant() {
    let mut db = ledger_db();
    // 显式回滚:中途两条 UPDATE 后 ROLLBACK,合计与逐户余额原封不动。
    run(&mut db, "BEGIN");
    run(
        &mut db,
        "UPDATE accounts SET balance = balance - CAST('250.005' AS DECIMAL) WHERE id = 1",
    );
    run(
        &mut db,
        "UPDATE accounts SET balance = balance + CAST('250.005' AS DECIMAL) WHERE id = 2",
    );
    run(&mut db, "ROLLBACK");
    assert_eq!(total(&mut db), "1501.00");
    let r = rows(&mut db, "SELECT balance FROM accounts WHERE id = 1");
    assert!(
        matches!(&r.rows[0][0], Value::Decimal(d) if d.to_string() == "1000.25"),
        "回滚后必须回到期初: {:?}",
        r.rows[0][0]
    );
    // 语句中途失败(NOT NULL 违例)在事务内只回滚该语句,显式
    // ROLLBACK 后整体原样 —— 不允许留下「转出一半」的状态。
    run(&mut db, "BEGIN");
    run(
        &mut db,
        "UPDATE accounts SET balance = balance - CAST('1.00' AS DECIMAL) WHERE id = 1",
    );
    assert!(db
        .execute("UPDATE accounts SET balance = NULL WHERE id = 2")
        .is_err());
    run(&mut db, "ROLLBACK");
    assert_eq!(total(&mut db), "1501.00");
}

#[test]
fn snapshot_reader_never_sees_a_torn_transfer() {
    // MVCC 一致性读:转账事务已提交后,此前建立的读快照必须看到
    // 完整的旧状态(两户同为旧值),新读才看到新状态 —— 任何时刻
    // 都不存在「转出已入账、转入未入账」的中间像。
    let mut db = ledger_db();
    let view = db.read_view();
    run(&mut db, "BEGIN");
    run(
        &mut db,
        "UPDATE accounts SET balance = balance - CAST('400.00' AS DECIMAL) WHERE id = 1",
    );
    run(
        &mut db,
        "UPDATE accounts SET balance = balance + CAST('400.00' AS DECIMAL) WHERE id = 2",
    );
    run(&mut db, "COMMIT");

    let old = match view.execute("SELECT id, balance FROM accounts ORDER BY id") {
        Ok(ExecOutcome::Rows(r)) => r,
        other => panic!("snapshot read failed: {other:?}"),
    };
    assert!(
        matches!(&old.rows[0][1], Value::Decimal(d) if d.to_string() == "1000.25")
            && matches!(&old.rows[1][1], Value::Decimal(d) if d.to_string() == "500.75"),
        "旧快照必须看到转账前的完整状态: {old:?}"
    );
    let fresh = rows(&mut db, "SELECT balance FROM accounts WHERE id = 2");
    assert!(
        matches!(&fresh.rows[0][0], Value::Decimal(d) if d.to_string() == "900.75"),
        "新读必须看到转账后状态: {:?}",
        fresh.rows[0][0]
    );
    assert_eq!(total(&mut db), "1501.00");
}

#[test]
fn killed_before_commit_transfer_never_lands() {
    // 崩溃原子性:BEGIN..COMMIT 之前的语句是 deferred 提交,进程被杀
    // (丢掉 Database 即模拟)后恢复,绝不能重放出转账的任意一侧。
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ledger-crash.db");
    {
        let mut db = Database::open(&path).unwrap();
        run(
            &mut db,
            "CREATE TABLE accounts (id INT PRIMARY KEY, balance DECIMAL NOT NULL)",
        );
        run(
            &mut db,
            "INSERT INTO accounts VALUES (1, CAST('1000.25' AS DECIMAL))",
        );
        run(
            &mut db,
            "INSERT INTO accounts VALUES (2, CAST('500.75' AS DECIMAL))",
        );
        run(&mut db, "BEGIN");
        run(
            &mut db,
            "UPDATE accounts SET balance = balance - CAST('400.00' AS DECIMAL) WHERE id = 1",
        );
        run(
            &mut db,
            "UPDATE accounts SET balance = balance + CAST('400.00' AS DECIMAL) WHERE id = 2",
        );
        // no COMMIT — kill -9
    }
    let mut db = Database::open(&path).unwrap();
    let r = rows(&mut db, "SELECT SUM(balance) FROM accounts");
    assert!(
        matches!(&r.rows[0][0], Value::Decimal(d) if d.to_string() == "1501.00"),
        "恢复后合计必须原样: {:?}",
        r.rows[0][0]
    );
    let r = rows(&mut db, "SELECT balance FROM accounts WHERE id = 1");
    assert!(
        matches!(&r.rows[0][0], Value::Decimal(d) if d.to_string() == "1000.25"),
        "被杀事务的借记绝不能落盘: {:?}",
        r.rows[0][0]
    );
}

#[test]
fn float_precision_and_negative_sorting() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v REAL)");
    run(&mut db, "INSERT INTO t VALUES (1, 0.1)");
    run(&mut db, "INSERT INTO t VALUES (2, 0.2)");
    run(&mut db, "INSERT INTO t VALUES (3, -2.5)");
    run(&mut db, "INSERT INTO t VALUES (4, -10.75)");
    // binary floating point: 0.1 + 0.2 must NOT equal 0.3
    let r = rows(&mut db, "SELECT 0.1 + 0.2 = 0.3 FROM t WHERE id = 1");
    assert_eq!(r.rows, vec![vec![Value::Bool(false)]]);
    let r = rows(&mut db, "SELECT v FROM t WHERE id = 3");
    assert_eq!(r.rows, vec![vec![Value::Float(-2.5)]]);
    let r = rows(&mut db, "SELECT v FROM t ORDER BY v");
    let vals: Vec<f64> = r
        .rows
        .iter()
        .filter_map(|row| match &row[0] {
            Value::Float(f) => Some(*f),
            Value::Int(i) => Some(*i as f64),
            _ => None,
        })
        .collect();
    assert_eq!(vals, vec![-10.75, -2.5, 0.1, 0.2]);
}

#[test]
fn long_string_value_survives() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    let long = "x".repeat(100_000) + "tail";
    run(&mut db, &format!("INSERT INTO t VALUES (1, '{}')", long));
    let r = rows(&mut db, "SELECT LENGTH(s) FROM t WHERE id = 1");
    let len = r.rows[0][0].as_i64().unwrap();
    assert!(len >= 100_000, "unexpected LENGTH {len}");
    let r = rows(&mut db, &format!("SELECT id FROM t WHERE s = '{}'", long));
    assert_eq!(r.rows.len(), 1);
    let r = rows(&mut db, "SELECT id FROM t WHERE s LIKE 'x%'");
    assert_eq!(r.rows.len(), 1);
}

#[test]
fn negative_primary_key_index_range() {
    let mut db = idx_db();
    for i in -10..10 {
        run(
            &mut db,
            &format!("INSERT INTO t (id, name, n) VALUES ({i}, 'n{i}', {i})"),
        );
    }
    let r = rows(&mut db, "SELECT id FROM t WHERE id < -7 ORDER BY id");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(-10)],
            vec![Value::Int(-9)],
            vec![Value::Int(-8)]
        ]
    );
    let r = rows(
        &mut db,
        "SELECT id FROM t WHERE id >= -1 AND id <= 1 ORDER BY id",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(-1)],
            vec![Value::Int(0)],
            vec![Value::Int(1)]
        ]
    );
    assert_eq!(
        rows(&mut db, "SELECT id FROM t WHERE id = -10").rows.len(),
        1
    );
}

#[test]
fn string_escape_and_multibyte_literals() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    run(&mut db, "INSERT INTO t VALUES (1, 'it''s a \\ test')");
    run(&mut db, "INSERT INTO t VALUES (2, '中文值「测试」')");
    let r = rows(&mut db, "SELECT s FROM t WHERE id = 1");
    assert_eq!(r.rows, vec![vec![Value::Str("it's a \\ test".into())]]);
    let r = rows(&mut db, "SELECT s FROM t WHERE id = 2");
    assert_eq!(r.rows, vec![vec![Value::Str("中文值「测试」".into())]]);
    let r = rows(&mut db, "SELECT id FROM t WHERE s = '中文值「测试」'");
    assert_eq!(r.rows.len(), 1);
}

#[test]
fn type_mismatch_insert_is_stored_verbatim() {
    // Schemaless by design: an INTEGER-declared column still accepts a
    // string literal. Pin that semantic so future changes are loud.
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    run(&mut db, "INSERT INTO t VALUES (1, 'not-a-number')");
    let r = rows(&mut db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r.rows, vec![vec![Value::Str("not-a-number".into())]]);
    assert_eq!(rows(&mut db, "SELECT id FROM t WHERE v = 0").rows.len(), 0);
}

#[test]
fn update_delete_zero_rows_reports_affected_zero() {
    let mut db = idx_db();
    insert_n(&mut db, 5);
    assert_eq!(
        db.execute("UPDATE t SET name = 'x' WHERE id = 999")
            .unwrap(),
        ExecOutcome::Affected(0)
    );
    assert_eq!(
        db.execute("DELETE FROM t WHERE id = 999").unwrap(),
        ExecOutcome::Affected(0)
    );
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM t").rows[0][0],
        Value::Int(5)
    );
}

#[test]
fn autoinc_after_explicit_large_id_skips_forward() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, v INT)",
    );
    run(&mut db, "INSERT INTO t (v) VALUES (1)");
    run(&mut db, "INSERT INTO t (id, v) VALUES (1000, 2)");
    run(&mut db, "INSERT INTO t (v) VALUES (3)");
    let r = rows(&mut db, "SELECT id FROM t ORDER BY id");
    let ids: Vec<i64> = r.rows.iter().filter_map(|row| row[0].as_i64()).collect();
    assert_eq!(ids, vec![1, 1000, 1001]);
}
