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

fn complex_db() -> Database {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE customers (id INT PRIMARY KEY, name TEXT, region TEXT)",
    );
    run(
            &mut db,
            "CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT, status TEXT, placed TEXT)",
        );
    run(
        &mut db,
        "CREATE TABLE items (id INT PRIMARY KEY, order_id INT, product TEXT, qty INT, price INT)",
    );
    run(
            &mut db,
            "INSERT INTO customers VALUES (1, 'Alice', 'east'), (2, 'Bob', 'west'), (3, 'Cara', 'east'), (4, 'Dan', NULL)",
        );
    run(
        &mut db,
        "INSERT INTO orders VALUES (100, 1, 250, 'paid', '2026-01-05'), \
             (101, 1, 90, 'open', '2026-01-07'), \
             (102, 2, 300, 'paid', '2026-01-09'), \
             (103, 3, 40, 'void', '2026-01-11'), \
             (104, 3, 120, 'paid', '2026-02-01'), \
             (105, 1, 60, 'paid', NULL)",
    );
    run(
        &mut db,
        "INSERT INTO items VALUES (1, 100, 'widget', 2, 50), (2, 100, 'gadget', 1, 150), \
             (3, 101, 'widget', 1, 90), (4, 102, 'gizmo', 3, 100), \
             (5, 103, 'widget', 1, 40), (6, 104, 'gadget', 2, 60), \
             (7, 105, 'widget', 3, 20)",
    );
    db
}

#[test]
fn complex_cte_join_group_having_order_limit_stack() {
    let mut db = complex_db();
    // Paid orders -> customers -> items, grouped by region, HAVING over
    // the join aggregate, ordered by alias, paged.
    let r = rows(
        &mut db,
        "WITH paid AS (
                 SELECT o.id AS order_id, o.customer_id AS cid
                 FROM orders o WHERE o.status = 'paid'
             )
             SELECT c.region, COUNT(*) AS line_items, SUM(i.qty * i.price) AS items_gmv
             FROM paid p
             JOIN customers c ON c.id = p.cid
             JOIN items i ON i.order_id = p.order_id
             GROUP BY c.region
             HAVING SUM(i.qty * i.price) > 300
             ORDER BY items_gmv DESC
             LIMIT 5",
    );
    // east: o100 (250) + o104 (120) + o105 (60), 2+1+1 item rows.
    // west: o102 (300) fails the HAVING.
    assert_eq!(r.columns, vec!["c.region", "line_items", "items_gmv"]);
    assert_eq!(
        r.rows,
        vec![vec![
            Value::Str("east".into()),
            Value::Int(4),
            Value::Int(430)
        ]]
    );
}

#[test]
fn complex_cte_joined_with_base_table_and_itself() {
    let mut db = complex_db();
    let r = rows(
        &mut db,
        "WITH east AS (SELECT id, name FROM customers WHERE region = 'east')
             SELECT e.name, o.amount
             FROM east e JOIN orders o ON o.customer_id = e.id
             ORDER BY o.id",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("Alice".into()), Value::Int(250)],
            vec![Value::Str("Alice".into()), Value::Int(90)],
            vec![Value::Str("Cara".into()), Value::Int(40)],
            vec![Value::Str("Cara".into()), Value::Int(120)],
            vec![Value::Str("Alice".into()), Value::Int(60)],
        ]
    );
    // A CTE can also be self-joined (pairwise same-customer paid orders).
    let r = rows(
        &mut db,
        "WITH paid AS (SELECT id, customer_id FROM orders WHERE status = 'paid')
             SELECT a.id, b.id
             FROM paid a JOIN paid b ON a.customer_id = b.customer_id AND a.id < b.id",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(100), Value::Int(105)]]);
}

#[test]
fn complex_cte_of_aggregates_joined_back() {
    let mut db = complex_db();
    let r = rows(
        &mut db,
        "WITH totals AS (
                 SELECT customer_id AS cid, SUM(amount) AS total
                 FROM orders GROUP BY customer_id
             )
             SELECT c.name, t.total
             FROM totals t JOIN customers c ON c.id = t.cid
             WHERE t.total > 100
             ORDER BY t.total DESC",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("Alice".into()), Value::Int(400)],
            vec![Value::Str("Bob".into()), Value::Int(300)],
            vec![Value::Str("Cara".into()), Value::Int(160)],
        ]
    );
}

#[test]
fn complex_derived_table_joined_and_aggregated() {
    let mut db = complex_db();
    let r = rows(
        &mut db,
        "SELECT c.name, d.total
             FROM customers c
             JOIN (SELECT customer_id AS cid, SUM(amount) AS total
                   FROM orders GROUP BY customer_id) d ON d.cid = c.id
             ORDER BY d.total DESC",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("Alice".into()), Value::Int(400)],
            vec![Value::Str("Bob".into()), Value::Int(300)],
            vec![Value::Str("Cara".into()), Value::Int(160)],
        ]
    );
}

#[test]
fn complex_nested_subqueries() {
    let mut db = complex_db();
    // Scalar inside scalar: biggest order -> its customer's name.
    let r = rows(
        &mut db,
        "SELECT name FROM customers WHERE id = (
                 SELECT customer_id FROM orders
                 WHERE amount = (SELECT MAX(amount) FROM orders))",
    );
    assert_eq!(r.rows, vec![vec![Value::Str("Bob".into())]]);
    // Scalar inside scalar, resolving through two hops to a region set.
    let r = rows(
        &mut db,
        "SELECT name FROM customers
             WHERE region = (SELECT region FROM customers
                             WHERE id = (SELECT customer_id FROM orders WHERE id = 101))
             ORDER BY name",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("Alice".into())],
            vec![Value::Str("Cara".into())]
        ]
    );
    // Aggregate inside IN-subquery: customers with an above-average order.
    let r = rows(
        &mut db,
        "SELECT name FROM customers
             WHERE id IN (SELECT customer_id FROM orders
                          WHERE amount > (SELECT AVG(amount) FROM orders))
             ORDER BY id",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("Alice".into())],
            vec![Value::Str("Bob".into())]
        ]
    );
    // NOT IN over a subquery excludes Cara (the void order).
    let r = rows(
        &mut db,
        "SELECT name FROM customers
             WHERE id NOT IN (SELECT customer_id FROM orders WHERE status = 'void')
             ORDER BY id",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("Alice".into())],
            vec![Value::Str("Bob".into())],
            vec![Value::Str("Dan".into())],
        ]
    );
}

#[test]
fn correlated_subqueries_evaluate_per_row() {
    // Correlated subqueries resolve their outer references per row
    // (they used to be rewritten uncorrelated — the outer column read
    // as NULL and the predicate quietly filtered everything out).
    let mut db = complex_db();
    // Scalar correlated: the customer whose max order is exactly 300.
    let r = rows(
        &mut db,
        "SELECT c.name FROM customers c WHERE 300 = (
                 SELECT MAX(o.amount) FROM orders o WHERE o.customer_id = c.id)",
    );
    assert_eq!(r.rows, vec![vec![Value::Str("Bob".into())]]);
    // EXISTS: everyone with at least one order.
    let r = rows(
        &mut db,
        "SELECT c.name FROM customers c WHERE EXISTS (
                 SELECT 1 FROM orders o WHERE o.customer_id = c.id) ORDER BY c.id",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("Alice".into())],
            vec![Value::Str("Bob".into())],
            vec![Value::Str("Cara".into())],
        ]
    );
    // NOT EXISTS anti-join: the customer without orders.
    let r = rows(
        &mut db,
        "SELECT c.name FROM customers c WHERE NOT EXISTS (
                 SELECT 1 FROM orders o WHERE o.customer_id = c.id)",
    );
    assert_eq!(r.rows, vec![vec![Value::Str("Dan".into())]]);
    // Correlated IN with arithmetic on the outer reference.
    let r = rows(
        &mut db,
        "SELECT c.name FROM customers c WHERE c.id NOT IN (
                 SELECT o.customer_id FROM orders o WHERE o.amount < c.id * 30) ORDER BY c.id",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("Alice".into())],
            vec![Value::Str("Bob".into())],
            vec![Value::Str("Dan".into())],
        ]
    );
    // The outer reference inside the IN-subquery binds per row: for
    // every non-'west' customer the inner predicate is false, and 'west'
    // never appears among the >200 order statuses.
    let r = rows(
        &mut db,
        "SELECT c.name FROM customers c WHERE c.region IN (
                 SELECT o.status FROM orders o WHERE o.amount > 200 AND c.region = 'west')",
    );
    assert!(r.rows.is_empty());
    // The same intent expressed decorrelated still works.
    let r = rows(
        &mut db,
        "SELECT name FROM customers
             WHERE id IN (SELECT customer_id FROM orders WHERE amount >= 120)
             ORDER BY id",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("Alice".into())],
            vec![Value::Str("Bob".into())],
            vec![Value::Str("Cara".into())],
        ]
    );
}

#[test]
fn complex_case_in_order_by_and_conditional_aggregates() {
    let mut db = complex_db();
    // NULL region sorts last via a CASE key.
    let r = rows(
        &mut db,
        "SELECT name FROM customers
             ORDER BY CASE WHEN region IS NULL THEN 1 ELSE 0 END, name",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("Alice".into())],
            vec![Value::Str("Bob".into())],
            vec![Value::Str("Cara".into())],
            vec![Value::Str("Dan".into())],
        ]
    );
    // PIVOT-style conditional aggregation.
    let r = rows(
        &mut db,
        "SELECT SUM(CASE WHEN status = 'paid' THEN amount ELSE 0 END) AS paid_sum, \
                    SUM(CASE WHEN status = 'open' THEN amount ELSE 0 END) AS open_sum \
             FROM orders",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(730), Value::Int(90)]]);
}

#[test]
fn complex_aggregates_over_joins_incl_zero_groups() {
    let mut db = complex_db();
    // Items GMV restricted to paid orders, computed across a 3-way join
    // shape (order side filtered, item side summed).
    let r = rows(
        &mut db,
        "SELECT SUM(i.qty * i.price)
             FROM orders o JOIN items i ON i.order_id = o.id
             WHERE o.status = 'paid'",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(730)]]);
    // LEFT JOIN keeps customers without orders: COUNT(inner) yields 0,
    // and DESC ordering puts them last deterministically.
    let r = rows(
        &mut db,
        "SELECT c.name, COUNT(o.id) AS orders
             FROM customers c LEFT JOIN orders o ON o.customer_id = c.id
             GROUP BY c.name
             ORDER BY orders DESC, c.name",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("Alice".into()), Value::Int(3)],
            vec![Value::Str("Cara".into()), Value::Int(2)],
            vec![Value::Str("Bob".into()), Value::Int(1)],
            vec![Value::Str("Dan".into()), Value::Int(0)],
        ]
    );
}

#[test]
fn complex_where_predicate_stack_over_join() {
    let mut db = complex_db();
    // Parenthesized OR over join keys + IN + NOT LIKE + IS NOT NULL, all
    // feeding a DISTINCT projection.
    let r = rows(
        &mut db,
        "SELECT DISTINCT c.region
             FROM customers c
             JOIN orders o ON o.customer_id = c.id
             WHERE (o.status = 'paid' OR o.amount BETWEEN 80 AND 120)
               AND c.region IN ('east', 'west')
               AND c.name NOT LIKE 'D%'
               AND o.placed IS NOT NULL
             ORDER BY 1",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("east".into())],
            vec![Value::Str("west".into())]
        ]
    );
}

#[test]
fn complex_setop_with_global_order_and_limit() {
    let mut db = complex_db();
    // ORDER BY + LIMIT apply to the whole UNION, not a branch.
    let r = rows(
        &mut db,
        "SELECT id FROM orders WHERE amount > 200
             UNION
             SELECT id FROM orders WHERE amount < 50
             ORDER BY id DESC LIMIT 2",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(103)], vec![Value::Int(102)]]);
}

#[test]
fn complex_grouped_paging_with_having_alias() {
    let mut db = complex_db();
    // HAVING accepts the output alias...
    let r = rows(
        &mut db,
        "SELECT customer_id, SUM(amount) AS total
             FROM orders GROUP BY customer_id
             HAVING total > 200
             ORDER BY total DESC",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(1), Value::Int(400)],
            vec![Value::Int(2), Value::Int(300)],
        ]
    );
    // ...and paging over grouped rows resolves ties by the second key.
    let r = rows(
        &mut db,
        "SELECT status, COUNT(*) AS n
             FROM orders GROUP BY status
             ORDER BY n DESC, status
             LIMIT 1 OFFSET 1",
    );
    // paid(3) is skipped by OFFSET; open and void tie at 1, 'open' first.
    assert_eq!(r.rows, vec![vec![Value::Str("open".into()), Value::Int(1)]]);
}

#[test]
fn complex_grouping_edges_error_explicitly() {
    let mut db = complex_db();
    // CASE as a grouped projection is not supported (searched or simple).
    for sql in [
        "SELECT CASE WHEN amount >= 200 THEN 'big' ELSE 'small' END AS bucket, COUNT(*) AS n \
             FROM orders GROUP BY bucket",
        "SELECT CASE status WHEN 'paid' THEN 1 ELSE 2 END AS code, COUNT(*) AS n \
             FROM orders GROUP BY code",
    ] {
        let e = db.execute(sql).unwrap_err();
        assert!(
            e.to_string().contains("unsupported aggregate projection"),
            "{sql}\n{e}"
        );
    }
    // Arithmetic over aggregates evaluates per group.
    let r = rows(&mut db, "SELECT MAX(amount) - MIN(amount) FROM orders");
    assert_eq!(r.rows, vec![vec![Value::Int(260)]]);
    // ORDER BY must use an alias, not a bare aggregate expression.
    let e = db
        .execute(
            "SELECT customer_id, SUM(amount) AS total FROM orders \
                 GROUP BY customer_id ORDER BY SUM(amount)",
        )
        .unwrap_err();
    assert!(e.to_string().contains("unknown ORDER BY key"), "{e}");
    // GROUP BY by alias or ordinal is not supported (unlike HAVING).
    for sql in [
        "SELECT status AS s, COUNT(*) FROM orders GROUP BY s",
        "SELECT status, COUNT(*) FROM orders GROUP BY 1",
    ] {
        let e = db.execute(sql).unwrap_err();
        assert!(
            e.to_string()
                .contains("must appear in GROUP BY or an aggregate"),
            "{sql}\n{e}"
        );
    }
}

// ---- rejoin repair: table digests + transactional wipe ----

fn digests_of(db: &mut Database) -> Vec<TableDigest> {
    db.digests().unwrap()
}

#[test]
fn digests_agree_regardless_of_row_order() {
    // Nodes apply the same logical rows in different arrival orders;
    // heap layout differs, the digest must not.
    let mut a = Database::in_memory().unwrap();
    let mut b = Database::in_memory().unwrap();
    {
        let pair: [(&mut Database, [i64; 3]); 2] = [(&mut a, [1, 2, 3]), (&mut b, [3, 1, 2])];
        for (db, order) in pair {
            run(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
            for id in order {
                run(db, &format!("INSERT INTO t VALUES ({id}, 'v{id}')"));
            }
        }
    }
    assert_eq!(digests_of(&mut a), digests_of(&mut b));
    // One changed row breaks the agreement (update path).
    run(&mut b, "UPDATE t SET v = 'x' WHERE id = 2");
    assert_ne!(digests_of(&mut a), digests_of(&mut b));
    // And the delete path moves it back only when mirrored.
    run(&mut b, "DELETE FROM t WHERE id = 2");
    assert_ne!(digests_of(&mut a), digests_of(&mut b));
    run(&mut a, "DELETE FROM t WHERE id = 2");
    assert_eq!(digests_of(&mut a), digests_of(&mut b));
}

#[test]
fn digests_reflect_schema_and_skip_pubsub() {
    let mut a = Database::in_memory().unwrap();
    let mut b = Database::in_memory().unwrap();
    run(&mut a, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
    run(&mut b, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT UNIQUE)");
    assert_ne!(digests_of(&mut a), digests_of(&mut b), "schema differs");
    // Column DEFAULT is part of the replayed DDL — it must show up.
    run(&mut a, "CREATE TABLE d (x TEXT DEFAULT ('n/a'))");
    run(&mut b, "CREATE TABLE d (x TEXT)");
    assert_ne!(digests_of(&mut a), digests_of(&mut b));
    // The pubsub system table is node-local queue state, never compared.
    a.ensure_pubsub_table().unwrap();
    let base = digests_of(&mut a);
    run(
        &mut a,
        "INSERT INTO _pubsub_messages (payload) VALUES ('m')",
    );
    assert_eq!(
        digests_of(&mut a),
        base,
        "pubsub rows must not move the digest"
    );
}

#[test]
fn wipe_user_tables_is_transactional() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE parent (id INT PRIMARY KEY)");
    run(
        &mut db,
        "CREATE TABLE child (id INT PRIMARY KEY, pid INT, \
             FOREIGN KEY (pid) REFERENCES parent (id))",
    );
    run(&mut db, "INSERT INTO parent VALUES (1)");
    run(&mut db, "INSERT INTO child VALUES (10, 1)");
    // DROP cannot take this state apart in any order (child references
    // parent); the wipe bypasses the FK guard.
    run(&mut db, "BEGIN");
    db.wipe_user_tables().unwrap();
    assert!(db.catalog().iter().all(|t| t.name == PUBSUB_TABLE));
    run(&mut db, "ROLLBACK");
    assert!(db.catalog().iter().any(|t| t.name == "parent"));
    let r = rows(&mut db, "SELECT COUNT(*) FROM child");
    assert_eq!(r.rows, vec![vec![Value::Int(1)]], "rollback restores data");
}

#[test]
fn snapshot_adoption_replaces_divergent_state() {
    // The repair flow: wipe + dump replay inside one transaction makes
    // the stale node digest-identical to the reference, and tables the
    // snapshot no longer contains are gone.
    let mut truth = Database::in_memory().unwrap();
    run(&mut truth, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
    run(&mut truth, "INSERT INTO t VALUES (1, 'a'), (2, 'b')");
    let script = truth.dump_script().unwrap();

    let mut stale = Database::in_memory().unwrap();
    run(&mut stale, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
    run(&mut stale, "CREATE TABLE extra (x INT)");
    run(
        &mut stale,
        "INSERT INTO t VALUES (1, 'stale'), (9, 'zombie')",
    );
    assert_ne!(digests_of(&mut truth), digests_of(&mut stale));

    run(&mut stale, "BEGIN");
    stale.wipe_user_tables().unwrap();
    let batch = stale.execute_batch(&script);
    assert!(batch.error.is_none(), "{:?}", batch.error);
    run(&mut stale, "COMMIT");
    assert_eq!(digests_of(&mut truth), digests_of(&mut stale));
    assert!(
        !stale.catalog().iter().any(|t| t.name == "extra"),
        "table absent from the snapshot must go"
    );
}

#[test]
fn catchup_journal_and_positions_round_trip() {
    let mut a = Database::in_memory().unwrap();
    let id = a.cluster_id().unwrap();
    assert!(!id.is_empty());
    assert_eq!(a.cluster_id().unwrap(), id, "identity is stable");
    let s1 = a.journal_append("INSERT INTO t VALUES (1)").unwrap();
    let s2 = a.journal_append("UPDATE t SET v = 2").unwrap();
    assert_eq!(s2, s1 + 1, "seqs are consecutive");
    assert_eq!(a.journal_head().unwrap(), s2);
    assert_eq!(a.journal_oldest().unwrap(), s1);
    let range = a.journal_range(s1 - 1, 10).unwrap();
    assert_eq!(range.len(), 2);
    assert_eq!(range[0].0, s1);
    assert!(range[0].1.contains("INSERT INTO t"));
    assert!(
        a.journal_range(s2, 10).unwrap().is_empty(),
        "range is exclusive of `after`"
    );
    // Positions round-trip per origin and advance in place.
    assert_eq!(a.position_get("origin-1").unwrap(), None);
    a.position_set("origin-1", 7).unwrap();
    a.position_set("origin-2", 9).unwrap();
    assert_eq!(a.position_get("origin-1").unwrap(), Some(7));
    a.position_set("origin-1", 8).unwrap();
    assert_eq!(a.position_get("origin-1").unwrap(), Some(8));
    a.positions_clear().unwrap();
    assert_eq!(a.position_get("origin-2").unwrap(), None);
    // Trim keeps exactly the newest `keep` entries.
    a.journal_trim(1).unwrap();
    assert_eq!(a.journal_head().unwrap(), s2);
    assert_eq!(a.journal_oldest().unwrap(), s2);
    assert_eq!(a.journal_range(0, 10).unwrap().len(), 1);
}

#[test]
fn system_tables_are_excluded_from_user_surfaces() {
    let mut db = Database::in_memory().unwrap();
    db.ensure_cluster_tables().unwrap();
    run(&mut db, "CREATE TABLE user_t (id INT)");
    db.journal_append("INSERT INTO user_t VALUES (1)").unwrap();
    let d = db.digests().unwrap();
    assert_eq!(d.len(), 1, "only user_t: {d:?}");
    let script = db.dump_script().unwrap();
    assert!(!script.contains("_cluster_log"), "{script}");
    assert!(!script.contains("_cluster_pos"), "{script}");
    run(&mut db, "BEGIN");
    db.wipe_user_tables().unwrap();
    run(&mut db, "COMMIT");
    assert!(
        db.catalog().iter().all(|t| is_system_table(&t.name)),
        "wipe keeps system tables, got {:?}",
        db.catalog()
    );
}

#[test]
fn journal_and_positions_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("j.db");
    let (id, seq) = {
        let mut db = Database::open(&path).unwrap();
        db.ensure_cluster_tables().unwrap();
        let id = db.cluster_id().unwrap();
        let seq = db.journal_append("INSERT INTO t VALUES (1)").unwrap();
        db.position_set("origin-x", 42).unwrap();
        (id, seq)
    };
    let mut db = Database::open(&path).unwrap();
    assert_eq!(db.cluster_id().unwrap(), id, "identity persists");
    assert_eq!(
        db.journal_append("INSERT INTO t VALUES (2)").unwrap(),
        seq + 1,
        "journal seq continues after reopen"
    );
    assert_eq!(db.position_get("origin-x").unwrap(), Some(42));
}

#[test]
fn write_unit_fuses_write_and_bookkeeping() {
    // The origin-side shape: user write + journal append in one unit,
    // durable together after `end` — and nothing in the data file
    // before it (deferred commits never touch it ahead of the fsync).
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("unit.db");
    let seq = {
        let mut db = Database::open(&path).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
        let file_len_before = std::fs::metadata(&path).unwrap().len();
        let mut unit = db.write_unit();
        unit.execute("INSERT INTO t VALUES (7)").unwrap();
        let s = unit.journal_append("INSERT INTO t VALUES (7)").unwrap();
        assert!(
            unit.has_pending_sync(),
            "statements inside the unit deferred their fsync"
        );
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            file_len_before,
            "no page image on disk before the unit's fsync"
        );
        // Reads see the unit's own writes through the buffer pool.
        assert!(matches!(
            unit.execute("SELECT id FROM t"),
            Ok(ExecOutcome::Rows(r)) if !r.rows.is_empty()
        ));
        unit.end().unwrap();
        assert!(
            std::fs::metadata(&path).unwrap().len() > file_len_before,
            "end flushed the deferred pages to the data file"
        );
        s
    };
    let mut db = Database::open(&path).unwrap();
    let rows = db.execute("SELECT id FROM t").unwrap();
    assert!(matches!(&rows, ExecOutcome::Rows(r) if !r.rows.is_empty()));
    assert_eq!(db.journal_head().unwrap(), seq);
}

#[test]
fn write_unit_receiver_side_position_fused() {
    // The receiver-side shape: replicated write + position update land
    // in one unit; after reopen both are present (atomic on crash, the
    // position can no longer lag the applied write).
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("recv.db");
    {
        let mut db = Database::open(&path).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
        let mut unit = db.write_unit();
        unit.execute("INSERT INTO t VALUES (1)").unwrap();
        unit.position_set("origin-a", 5).unwrap();
        unit.end().unwrap();
    }
    let mut db = Database::open(&path).unwrap();
    assert_eq!(db.position_get("origin-a").unwrap(), Some(5));
    assert!(matches!(
        db.execute("SELECT id FROM t"),
        Ok(ExecOutcome::Rows(r)) if !r.rows.is_empty()
    ));
}

#[test]
fn write_unit_batch_journals_in_one_sync() {
    // The COMMIT-drain shape: N journal appends inside one unit share a
    // single fsync; all N survive `end` in order.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("drain.db");
    let n = 50;
    let first = {
        let mut db = Database::open(&path).unwrap();
        let mut unit = db.write_unit();
        let mut s = None;
        for i in 0..n {
            let seq = unit
                .journal_append(&format!("INSERT INTO t VALUES ({i})"))
                .unwrap();
            s.get_or_insert(seq);
        }
        unit.end().unwrap();
        s.unwrap()
    };
    let mut db = Database::open(&path).unwrap();
    assert_eq!(db.journal_head().unwrap(), first + n as u64 - 1);
    let range = db.journal_range(first - 1, 1000).unwrap();
    assert_eq!(range.len(), n);
}

#[test]
fn dropped_write_unit_never_wedges_durability() {
    // A caller that forgets `end` only loses the fusion: the depth
    // clears on drop, so the next statement commits durably on its own
    // and flushes whatever the unit deferred before it.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("leak.db");
    {
        let mut db = Database::open(&path).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
        {
            let mut unit = db.write_unit();
            unit.execute("INSERT INTO t VALUES (1)").unwrap();
            assert!(unit.has_pending_sync());
        } // dropped without end
        db.execute("INSERT INTO t VALUES (2)").unwrap(); // durable commit
    }
    let mut db = Database::open(&path).unwrap();
    let out = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert!(
            matches!(&out, ExecOutcome::Rows(r) if r.rows[0][0].as_i64() == Some(2)),
            "both rows survive: the leaked unit's deferred pages were flushed by the next durable commit"
        );
}

#[test]
fn write_unit_defers_to_flusher_in_async_mode() {
    // Group commit owns the fsync in async-commit mode: a unit's `end`
    // must not sync the WAL itself (that would re-impose one fsync per
    // unit and defeat the flusher's batching), only group the commits.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("async-unit.db");
    {
        let mut db = Database::open(&path).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
        db.set_async_commit(true);
        let mut unit = db.write_unit();
        unit.execute("INSERT INTO t VALUES (1)").unwrap();
        let seq = unit.journal_append("INSERT INTO t VALUES (1)").unwrap();
        unit.end().unwrap();
        assert!(
            db.has_pending_sync(),
            "async mode: the flusher still owns the pending fsync"
        );
        db.sync_pending().unwrap(); // what the background task does
        assert!(!db.has_pending_sync());
        assert_eq!(db.journal_head().unwrap(), seq);
    }
    let mut db = Database::open(&path).unwrap();
    assert!(
        matches!(db.execute("SELECT id FROM t"), Ok(ExecOutcome::Rows(r)) if !r.rows.is_empty()),
        "durable after the flusher ran"
    );
}

#[test]
fn statement_deadline_fires_mid_join_and_clears() {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE a (id INT PRIMARY KEY)").unwrap();
    db.execute("CREATE TABLE b (id INT)").unwrap();
    let mut vals = String::from("INSERT INTO a VALUES (0)");
    for i in 1..400 {
        vals.push_str(&format!(", ({i})"));
    }
    db.execute(&vals).unwrap();
    let mut vals = String::from("INSERT INTO b VALUES (0)");
    for i in 1..400 {
        vals.push_str(&format!(", ({i})"));
    }
    db.execute(&vals).unwrap();

    // Deadline already in the past: the nested-loop join must abort
    // with the timeout error (row loops are the only practical
    // preemption point inside the synchronous engine), and — crucial —
    // the deadline is armed per statement: after clearing it, the same
    // join runs to completion.
    let expired = std::time::Instant::now() - std::time::Duration::from_secs(1);
    db.set_statement_deadline(Some(expired));
    let e = db
        .execute("SELECT COUNT(*) FROM a, b WHERE a.id + b.id < 0")
        .unwrap_err();
    assert!(
        e.to_string().contains("statement timeout"),
        "wrong error: {e}"
    );
    db.set_statement_deadline(None);
    assert!(
        matches!(
            db.execute("SELECT COUNT(*) FROM a, b WHERE a.id + b.id < 0"),
            Ok(ExecOutcome::Rows(_))
        ),
        "cleared deadline must not poison later statements"
    );
    // UPDATE matching loops honor the deadline too (plain table, no
    // index fast path: the deadline must fire in the row loop). Needs
    // more rows than the sampling batch — a 400-row UPDATE finishes
    // before the first clock sample by design.
    db.execute("CREATE TABLE c (id INT)").unwrap();
    let mut vals = String::from("INSERT INTO c VALUES (0)");
    for i in 1..2000 {
        vals.push_str(&format!(", ({i})"));
    }
    db.execute(&vals).unwrap();
    db.set_statement_deadline(Some(expired));
    let e = db.execute("UPDATE c SET id = id").unwrap_err();
    assert!(
        e.to_string().contains("statement timeout"),
        "wrong error: {e}"
    );
    db.set_statement_deadline(None);
}

#[test]
fn json_function_family_point_reads() {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE docs (id INT PRIMARY KEY, doc TEXT)")
        .unwrap();
    db.execute(
        r#"INSERT INTO docs VALUES (1, '{"name":"alice","tags":["a","b"],"nest":{"x":42}}')"#,
    )
    .unwrap();
    let q = |db: &mut Database, sql: &str| -> Value {
        match db.execute(sql).unwrap() {
            ExecOutcome::Rows(r) => r.rows[0][0].clone(),
            ExecOutcome::Affected(_) => panic!("expected rows"),
        }
    };
    // Path navigation: members, nested objects, array indexes.
    assert_eq!(
        q(
            &mut db,
            "SELECT JSON_EXTRACT(doc, '$.name') FROM docs WHERE id = 1"
        ),
        Value::Str("alice".into())
    );
    assert_eq!(
        q(
            &mut db,
            "SELECT JSON_EXTRACT(doc, '$.nest.x') FROM docs WHERE id = 1"
        ),
        Value::Int(42)
    );
    assert_eq!(
        q(
            &mut db,
            "SELECT JSON_EXTRACT(doc, '$.tags[1]') FROM docs WHERE id = 1"
        ),
        Value::Str("b".into())
    );
    // Misses and malformed input yield NULL (heterogeneous documents
    // must not abort the scan on one bad row).
    assert_eq!(
        q(
            &mut db,
            "SELECT JSON_EXTRACT(doc, '$.missing') FROM docs WHERE id = 1"
        ),
        Value::Null
    );
    // Objects and arrays stay structured.
    assert_eq!(
        q(
            &mut db,
            "SELECT JSON_EXTRACT(doc, '$.nest') FROM docs WHERE id = 1"
        ),
        crate::json::from_str(r#"{"x":42}"#).unwrap()
    );
    // JSON_TYPE names SQLite-style; two-arg form reads a path directly
    // (the unwrapping extract would hand type-of a plain scalar text).
    assert_eq!(
        q(&mut db, "SELECT JSON_TYPE(doc) FROM docs WHERE id = 1"),
        Value::Str("object".into())
    );
    assert_eq!(
        q(
            &mut db,
            "SELECT JSON_TYPE(doc, '$.tags[0]') FROM docs WHERE id = 1"
        ),
        Value::Str("text".into())
    );
    assert_eq!(
        q(
            &mut db,
            "SELECT JSON_TYPE(doc, '$.nest.x') FROM docs WHERE id = 1"
        ),
        Value::Str("integer".into())
    );
    // JSON_VALID.
    assert_eq!(
        q(&mut db, r#"SELECT JSON_VALID('{"a":1}')"#),
        Value::Bool(true)
    );
    assert_eq!(
        q(&mut db, "SELECT JSON_VALID('{\"a\":')"),
        Value::Bool(false)
    );
    // A stored non-JSON text row: NULL, not an error.
    db.execute("INSERT INTO docs VALUES (2, 'not json')")
        .unwrap();
    assert_eq!(
        q(
            &mut db,
            "SELECT JSON_EXTRACT(doc, '$.name') FROM docs WHERE id = 2"
        ),
        Value::Null
    );
}

#[test]
fn json_function_exception_paths() {
    let mut db = Database::in_memory().unwrap();
    let q = |db: &mut Database, sql: &str| -> Value {
        match db.execute(sql).unwrap() {
            ExecOutcome::Rows(r) => r.rows[0][0].clone(),
            ExecOutcome::Affected(_) => panic!("expected rows"),
        }
    };
    // Arity is checked before anything else: wrong argument counts are
    // caller bugs and surface as errors, not silent NULLs.
    for bad in [
        "SELECT JSON_EXTRACT()",
        "SELECT JSON_EXTRACT('{}', '$.a', 1)",
        "SELECT JSON_TYPE()",
        "SELECT JSON_TYPE('{}', '$', 1)",
    ] {
        let e = db.execute(bad).unwrap_err();
        assert!(
            e.to_string().contains("takes 1 or 2 arguments"),
            "{bad}: {e}"
        );
    }
    for bad in ["SELECT JSON_VALID()", "SELECT JSON_VALID('{}', 1)"] {
        let e = db.execute(bad).unwrap_err();
        assert!(e.to_string().contains("exactly 1"), "{bad}: {e}");
    }
    // A non-text path is a query bug (error); a non-text document,
    // NULL, or malformed JSON is data and answers NULL.
    let e = db
        .execute(r#"SELECT JSON_EXTRACT('{"a":1}', 5)"#)
        .unwrap_err();
    assert!(e.to_string().contains("path must be text"), "{e}");
    assert_eq!(q(&mut db, "SELECT JSON_EXTRACT(5, '$.a')"), Value::Null);
    assert_eq!(q(&mut db, "SELECT JSON_EXTRACT(NULL, '$.a')"), Value::Null);
    assert_eq!(q(&mut db, "SELECT JSON_TYPE('not json')"), Value::Null);
    assert_eq!(q(&mut db, "SELECT JSON_TYPE(NULL)"), Value::Null);
    assert_eq!(q(&mut db, "SELECT JSON_VALID(5)"), Value::Bool(false));
    assert_eq!(q(&mut db, "SELECT JSON_VALID(NULL)"), Value::Bool(false));

    // The path grammar refuses gracefully — every malformed or
    // non-matching path yields NULL, never an abort: missing '$',
    // empty member, unclosed bracket, non-numeric/negative/
    // out-of-range index, member on array or scalar, index on object.
    for path in [
        "a.b", "$.a..b", "$.a[1", "$.a[x]", "$.a[-1]", "$.a[5]", "$.a.b", "$.o[0]", "$.o.b.x",
        "$[0]",
    ] {
        let sql = format!(r#"SELECT JSON_EXTRACT('{{"a":[1,2],"o":{{"b":3}}}}', '{path}')"#);
        assert_eq!(q(&mut db, &sql), Value::Null, "path {path}");
        let sql = format!(r#"SELECT JSON_TYPE('{{"a":[1,2],"o":{{"b":3}}}}', '{path}')"#);
        assert_eq!(q(&mut db, &sql), Value::Null, "path {path} (type)");
    }

    // Heterogeneous documents scan to completion: the broken middle
    // row answers NULL in-line while its neighbors extract normally.
    db.execute("CREATE TABLE mixed (id INT PRIMARY KEY, doc TEXT)")
        .unwrap();
    db.execute(r#"INSERT INTO mixed VALUES (1, '{"a":1}'), (2, 'broken'), (3, '{"a":2}')"#)
        .unwrap();
    match db
        .execute("SELECT JSON_EXTRACT(doc, '$.a') FROM mixed ORDER BY id")
        .unwrap()
    {
        ExecOutcome::Rows(r) => {
            assert_eq!(r.rows.len(), 3);
            assert_eq!(r.rows[0][0], Value::Int(1));
            assert_eq!(r.rows[1][0], Value::Null);
            assert_eq!(r.rows[2][0], Value::Int(2));
        }
        ExecOutcome::Affected(_) => panic!("expected rows"),
    }
}

#[test]
fn fast_paths_reject_unsupported_table_factor_clauses() {
    // The count/order fast paths must not silently ignore per-factor
    // clauses that the general path rejects (TABLESAMPLE/alias column
    // lists/table functions).
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)");
    run(&mut db, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    for sql in [
        "SELECT COUNT(*) FROM t TABLESAMPLE (10)",
        "SELECT * FROM t AS q(a) ORDER BY id LIMIT 2",
        "SELECT * FROM t(1)",
        "SELECT id FROM t ORDER BY id LIMIT 2 OFFSET 1",
    ] {
        let out = db.execute(sql);
        match out {
            Err(_) => {}
            Ok(o) => {
                // The plain ORDER BY window is legitimately supported;
                // only the clause-bearing shapes must be rejected.
                assert!(
                    sql.contains("OFFSET"),
                    "clause silently ignored: {sql} -> {o:?}"
                );
            }
        }
    }
}

#[test]
fn composite_index_name_shadowing_a_column_still_probes_the_column() {
    // A composite index keyed by the index *name* must not hijack an
    // equality probe on a column that happens to share that name.
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (x INT NOT NULL, a INT, b INT)");
    run(&mut db, "CREATE INDEX x ON t (a, b)");
    run(&mut db, "INSERT INTO t VALUES (5, 1, 2)");
    assert_eq!(
        rows(&mut db, "SELECT a FROM t WHERE x = 5").rows,
        vec![vec![Value::Int(1)]]
    );
    run(&mut db, "DROP INDEX x");
    assert_eq!(
        rows(&mut db, "SELECT a FROM t WHERE x = 5").rows,
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn composite_index_create_probe_unique_and_maintenance() {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE t (a INT, b INT, tag TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10, 'x'), (1, 20, 'y'), (2, 10, 'z')")
        .unwrap();
    // Composite definition: full column list recorded, duplicate/reject
    // rules per column.
    db.execute("CREATE INDEX cpx ON t (a, b)").unwrap();
    assert!(db
        .execute("CREATE INDEX cpx2 ON t (a, a)")
        .unwrap_err()
        .to_string()
        .contains("duplicate column name: a"));
    assert!(db
        .execute("CREATE INDEX cpx3 ON t (a, nope)")
        .unwrap_err()
        .to_string()
        .contains("does not exist"));

    // Full-column equality probes the exact Array key.
    assert_eq!(
        rows(&mut db, "SELECT tag FROM t WHERE a = 1 AND b = 20").rows,
        vec![vec![Value::Str("y".into())]]
    );
    // Leading-column equality uses the prefix probe: both matching rows.
    let r = rows(&mut db, "SELECT tag FROM t WHERE a = 1 ORDER BY b");
    assert_eq!(
        r.rows,
        vec![vec![Value::Str("x".into())], vec![Value::Str("y".into())]]
    );
    // Non-leading column condition cannot use the tree but stays correct.
    assert_eq!(
        rows(&mut db, "SELECT tag FROM t WHERE b = 10 ORDER BY a").rows,
        vec![vec![Value::Str("x".into())], vec![Value::Str("z".into())]]
    );

    // Conflicting equalities on the same column must not become an
    // "exact" index window: `a = 1 AND a = 2` matches nothing, and a
    // prefix plan built from one value would return rows the residual
    // filter was skipped for.
    assert_eq!(
        rows(
            &mut db,
            "SELECT a, b FROM t WHERE a = 1 AND a = 2 ORDER BY a, b LIMIT 1"
        )
        .rows,
        Vec::<Vec<Value>>::new()
    );
    assert_eq!(
        rows(
            &mut db,
            "SELECT tag FROM t WHERE a = 1 AND a = 1 ORDER BY a, b"
        )
        .rows,
        vec![vec![Value::Str("x".into())], vec![Value::Str("y".into())]]
    );

    // Index maintenance through UPDATE and DELETE.
    db.execute("UPDATE t SET b = 99 WHERE a = 2 AND b = 10")
        .unwrap();
    assert_eq!(
        rows(&mut db, "SELECT tag FROM t WHERE a = 2 AND b = 99").rows,
        vec![vec![Value::Str("z".into())]]
    );
    db.execute("DELETE FROM t WHERE a = 1 AND b = 20").unwrap();
    assert_eq!(
        ExecOutcome::Rows(QueryResult {
            columns: vec!["n".into()],
            rows: vec![vec![Value::Int(2)]]
        }),
        db.execute("SELECT COUNT(*) AS n FROM t").unwrap()
    );
}

#[test]
fn composite_unique_index_enforces_and_allows_partial_keys() {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE t (a INT, tag TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a'), (1, 'b'), (2, 'a')")
        .unwrap();
    // (a, tag) pairs are unique; the single columns are NOT — the tree
    // (not meta.unique, which has no column-combination container)
    // enforces the composite uniqueness.
    db.execute("CREATE UNIQUE INDEX ux ON t (a, tag)").unwrap();
    let e = db.execute("INSERT INTO t VALUES (1, 'a')").unwrap_err();
    assert!(
        e.to_string().to_lowercase().contains("unique"),
        "wrong error: {e}"
    );
    // Distinct pairs sharing each single column value coexist.
    db.execute("INSERT INTO t VALUES (1, 'c'), (3, 'a')")
        .unwrap();
    // OR REPLACE displaces composite-unique conflicts too: the
    // documented contract is "ANY unique key" (sql-reference INSERT
    // table), matching SQLite — the probe used to cover only the
    // single-column constraint trees and errored instead.
    db.execute("INSERT OR REPLACE INTO t VALUES (1, 'a')")
        .unwrap();
    assert_eq!(
        ExecOutcome::Rows(QueryResult {
            columns: vec!["n".into()],
            rows: vec![vec![Value::Int(5)]]
        }),
        db.execute("SELECT COUNT(*) AS n FROM t").unwrap()
    );
    // NULL in any key column skips the whole key: duplicate NULL pairs
    // are allowed (composite rule: one NULL column skips the entire key).
    db.execute("INSERT INTO t VALUES (NULL, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (NULL, NULL)").unwrap();
    assert_eq!(
        ExecOutcome::Rows(QueryResult {
            columns: vec!["n".into()],
            rows: vec![vec![Value::Int(7)]]
        }),
        db.execute("SELECT COUNT(*) AS n FROM t").unwrap()
    );
    // Dropping the unique index lifts the enforcement.
    db.execute("DROP INDEX ux").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
}

#[test]
fn composite_index_survives_dump_catalog_and_renames() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cpx.db");
    {
        let mut db = Database::open(&path).unwrap();
        db.execute("CREATE TABLE t (a INT, b TEXT, v INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'x', 100), (1, 'y', 200), (2, 'x', 300)")
            .unwrap();
        db.execute("CREATE UNIQUE INDEX ux ON t (a, b)").unwrap();
        db.execute("CREATE INDEX iv ON t (v)").unwrap();
        // Catalog round-trip: composite def persists (4-element entry).
        let mut db2 = Database::open(&path).unwrap();
        assert!(matches!(
            db2.execute("SELECT v FROM t WHERE a = 1 AND b = 'y'"),
            Ok(ExecOutcome::Rows(_))
        ));
        assert!(db2.execute("INSERT INTO t VALUES (1, 'y', 999)").is_err());
    }
    // dump round-trip: full column list in the DDL, replay restores the
    // probe (and uniqueness).
    let dump = {
        let mut db = Database::open(&path).unwrap();
        db.dump_script().unwrap()
    };
    assert!(
        dump.contains("CREATE UNIQUE INDEX \"ux\" ON \"t\" (\"a\", \"b\")"),
        "composite DDL missing: {dump}"
    );
    let mut fresh = Database::in_memory().unwrap();
    for stmt in crate::stmt::split_statements(&dump).unwrap() {
        fresh.execute(&stmt).unwrap();
    }
    assert_eq!(
        ExecOutcome::Rows(QueryResult {
            columns: vec!["v".into()],
            rows: vec![vec![Value::Int(200)]]
        }),
        fresh
            .execute("SELECT v FROM t WHERE a = 1 AND b = 'y'")
            .unwrap()
    );
    // RENAME COLUMN carries the composite definition along.
    fresh
        .execute("ALTER TABLE t RENAME COLUMN b TO b2")
        .unwrap();
    assert_eq!(
        ExecOutcome::Rows(QueryResult {
            columns: vec!["v".into()],
            rows: vec![vec![Value::Int(300)]]
        }),
        fresh
            .execute("SELECT v FROM t WHERE a = 2 AND b2 = 'x'")
            .unwrap()
    );
}

#[test]
fn oversized_document_sql_roundtrip_and_update() {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE big (id INT PRIMARY KEY, blob TEXT)")
        .unwrap();
    // 9 KB document — over the legacy 4 KB page cap, stored via an
    // overflow chain.
    let payload = "z".repeat(9_000);
    db.execute(&format!("INSERT INTO big VALUES (1, '{payload}')"))
        .unwrap();
    assert_eq!(
        rows(&mut db, "SELECT LENGTH(blob) FROM big WHERE id = 1").rows,
        vec![vec![Value::Int(9_000)]]
    );
    // End-to-end content check: characters survive the chain.
    assert_eq!(
        rows(&mut db, "SELECT SUBSTR(blob, 1, 3) FROM big WHERE id = 1").rows,
        vec![vec![Value::Str("zzz".into())]]
    );
    // Shrinking an oversized document recycles its chain pages without
    // breaking later reads.
    db.execute("UPDATE big SET blob = 'small' WHERE id = 1")
        .unwrap();
    assert_eq!(
        rows(&mut db, "SELECT blob FROM big WHERE id = 1").rows,
        vec![vec![Value::Str("small".into())]]
    );
    // Growing again assembles a fresh overflow chain.
    let payload2 = "w".repeat(12_000);
    db.execute(&format!("UPDATE big SET blob = '{payload2}' WHERE id = 1"))
        .unwrap();
    assert_eq!(
        rows(&mut db, "SELECT LENGTH(blob) FROM big WHERE id = 1").rows,
        vec![vec![Value::Int(12_000)]]
    );
    // The oversized document participates in dumps like any other row
    // (the dump carries its full text).
    let dump = db.dump_script().unwrap();
    assert!(dump.contains(&payload2), "dump lost the oversized document");
}

#[test]
fn oversized_document_survives_index_maintenance() {
    // An oversized document on a table with indexes: locator semantics
    // (index entries point at the main-page slot) must hold through
    // updates that rewrite the overflow chain.
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, tag TEXT, blob TEXT)")
        .unwrap();
    db.execute("CREATE INDEX ix_tag ON t (tag)").unwrap();
    let payload = "z".repeat(9_000);
    db.execute(&format!("INSERT INTO t VALUES (1, 'k', '{payload}')"))
        .unwrap();
    // Probe via the single-column index, read the oversized column.
    assert_eq!(
        rows(&mut db, "SELECT LENGTH(blob) FROM t WHERE tag = 'k'").rows,
        vec![vec![Value::Int(9_000)]]
    );
    // Update shrinks it; the index entry for tag='k' stays valid.
    db.execute("UPDATE t SET blob = 'small' WHERE tag = 'k'")
        .unwrap();
    assert_eq!(
        rows(&mut db, "SELECT tag, blob FROM t WHERE tag = 'k'").rows,
        vec![vec![Value::Str("k".into()), Value::Str("small".into())]]
    );
}

#[test]
fn oracle_compat_dual_rownum_fetch_and_dictionary_views() {
    let mut db = Database::in_memory().unwrap();
    // DUAL: the one-row dummy table, case-insensitive.
    for name in ["DUAL", "dual", "Dual"] {
        assert_eq!(
            rows(&mut db, &format!("SELECT 1 AS one FROM {name}")).rows,
            vec![vec![Value::Int(1)]]
        );
    }
    // SELECT * FROM DUAL is a single empty row.
    assert_eq!(rows(&mut db, "SELECT * FROM DUAL").rows.len(), 1);

    // ROWNUM: numbering before WHERE (Oracle semantics) — take the
    // first 2 of the scanned rows via ROWNUM, not LIMIT.
    db.execute("CREATE TABLE r (n INT)").unwrap();
    db.execute("INSERT INTO r VALUES (10), (20), (30), (40), (50)")
        .unwrap();
    let r = rows(&mut db, "SELECT n FROM r WHERE ROWNUM <= 2 ORDER BY n");
    assert_eq!(r.rows, vec![vec![Value::Int(10)], vec![Value::Int(20)]]);
    // ROWNUM in the projection keeps the pre-WHERE numbering (Oracle:
    // filtered-out rows consume their numbers — rows 10 and 20 hold
    // ROWNUM 1 and 2 but are filtered away).
    let r = rows(&mut db, "SELECT ROWNUM, n FROM r WHERE n >= 30");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(3), Value::Int(30)],
            vec![Value::Int(4), Value::Int(40)],
            vec![Value::Int(5), Value::Int(50)],
        ]
    );
    // SELECT * must NOT surface the injected pseudo-column.
    let r = rows(&mut db, "SELECT * FROM r WHERE ROWNUM = 1");
    assert_eq!(r.columns, vec!["n"], "ROWNUM leaked into *");

    // FETCH FIRST (Oracle 12c / SQL standard).
    let r = rows(
        &mut db,
        "SELECT n FROM r ORDER BY n FETCH FIRST 2 ROWS ONLY",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(10)], vec![Value::Int(20)]]);
    // WITH TIES returns the whole tie run; unique keys mean exactly n.
    let r = rows(
        &mut db,
        "SELECT n FROM r ORDER BY n FETCH FIRST 2 ROWS WITH TIES",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(10)], vec![Value::Int(20)]]);
    // Without ORDER BY there is no peer definition.
    let e = db
        .execute("SELECT n FROM r FETCH FIRST 1 ROWS WITH TIES")
        .unwrap_err();
    assert!(e.to_string().contains("WITH TIES"), "{e}");
}

#[test]
fn oracle_dictionary_views_expose_catalog() {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE emp (id INT, name TEXT)").unwrap();
    db.execute("CREATE UNIQUE INDEX ux_emp ON emp (id)")
        .unwrap();
    db.execute("CREATE INDEX ix_name ON emp (name)").unwrap();

    let tables = rows(
        &mut db,
        "SELECT table_name FROM all_tables ORDER BY table_name",
    );
    assert_eq!(
        tables.rows,
        vec![vec![Value::Str("emp".into())]],
        "internal tables must not leak into ALL_TABLES"
    );
    // USER_* carries the same rows (single global namespace).
    assert_eq!(
        rows(
            &mut db,
            "SELECT table_name FROM user_tables ORDER BY table_name"
        )
        .rows,
        tables.rows
    );
    // Columns with ids, and indexes with uniqueness.
    let cols = rows(
            &mut db,
            "SELECT column_name, column_id FROM all_tab_columns WHERE table_name = 'emp' ORDER BY column_id",
        );
    assert_eq!(
        cols.rows,
        vec![
            vec![Value::Str("id".into()), Value::Int(1)],
            vec![Value::Str("name".into()), Value::Int(2)],
        ]
    );
    let idx = rows(
            &mut db,
            "SELECT index_name, uniqueness FROM all_indexes WHERE table_name = 'emp' ORDER BY index_name",
        );
    assert_eq!(
        idx.rows,
        vec![
            vec![Value::Str("ix_name".into()), Value::Str("NONUNIQUE".into())],
            vec![Value::Str("ux_emp".into()), Value::Str("UNIQUE".into())],
        ]
    );
}

#[test]
fn oracle_style_scalar_functions() {
    let mut db = Database::in_memory().unwrap();
    let q = |db: &mut Database, sql: &str| -> Value {
        match db.execute(sql).unwrap() {
            ExecOutcome::Rows(r) => r.rows[0][0].clone(),
            ExecOutcome::Affected(_) => panic!("expected rows"),
        }
    };
    // NULL-propagating and Oracle-null-comparison forms.
    assert_eq!(
        q(&mut db, "SELECT NVL(NULL, 'dflt')"),
        Value::Str("dflt".into())
    );
    assert_eq!(
        q(&mut db, "SELECT NVL('x', 'dflt')"),
        Value::Str("x".into())
    );
    assert_eq!(
        q(&mut db, "SELECT NVL2(NULL, 'a', 'b')"),
        Value::Str("b".into())
    );
    assert_eq!(
        q(&mut db, "SELECT NVL2('v', 'a', 'b')"),
        Value::Str("a".into())
    );
    // DECODE: NULL matches NULL (Oracle semantics), default optional.
    assert_eq!(
        q(&mut db, "SELECT DECODE(NULL, NULL, 'null-match', 'no')"),
        Value::Str("null-match".into())
    );
    assert_eq!(
        q(&mut db, "SELECT DECODE(2, 1, 'one', 2, 'two', 'other')"),
        Value::Str("two".into())
    );
    assert_eq!(
        q(&mut db, "SELECT DECODE(9, 1, 'one', 2, 'two')"),
        Value::Null
    );
    // String helpers.
    assert_eq!(
        q(&mut db, "SELECT INSTR('hello world', 'world')"),
        Value::Int(7)
    );
    assert_eq!(q(&mut db, "SELECT INSTR('hello', 'z')"), Value::Int(0));
    assert_eq!(
        q(&mut db, "SELECT LPAD('7', 3, '0')"),
        Value::Str("007".into())
    );
    assert_eq!(
        q(&mut db, "SELECT RPAD('ab', 4)"),
        Value::Str("ab  ".into())
    );
    // Truncating pads.
    assert_eq!(
        q(&mut db, "SELECT LPAD('abcdef', 3)"),
        Value::Str("abc".into())
    );
    // GREATEST/LEAST: NULL poisons, numeric/lexicographic order.
    assert_eq!(q(&mut db, "SELECT GREATEST(1, 5, 3)"), Value::Int(5));
    assert_eq!(
        q(&mut db, "SELECT LEAST('b', 'a', 'c')"),
        Value::Str("a".into())
    );
    assert_eq!(q(&mut db, "SELECT GREATEST(1, NULL)"), Value::Null);
    // Conversions.
    assert_eq!(q(&mut db, "SELECT TO_NUMBER('42') + 1"), Value::Int(43));
    // Oracle NUMBER semantics: non-integer text parses to exact DECIMAL.
    assert_eq!(
        q(&mut db, "SELECT TO_NUMBER('0.1') + TO_NUMBER('0.2')"),
        Value::Decimal("0.3".parse().unwrap())
    );
    assert_eq!(
        q(&mut db, "SELECT TO_CHAR(TO_NUMBER('1234.50'))"),
        Value::Str("1234.50".into())
    );
    assert!(matches!(
        db.execute("SELECT TO_NUMBER('not-a-number')"),
        Err(SqlError::Message(m)) if m.contains("invalid number")
    ));
    assert_eq!(
        q(&mut db, "SELECT TO_CHAR(123) || '!'"),
        Value::Str("123!".into())
    );
    // SYSDATE: fixed-shape UTC timestamp text with a sane year — the
    // old civil math returned year ~740000 and a shape-only assert
    // never noticed.
    match q(&mut db, "SELECT SYSDATE()") {
        Value::Str(s) => {
            assert!(s.len() >= 19 && s.contains('T'), "shape: {s}");
            let now_year = crate::now_ms() / 1000 / 86_400 / 366 + 1970;
            let y: i64 = s.get(..4).and_then(|p| p.parse().ok()).unwrap_or(-1);
            assert!(
                y.abs_diff(now_year as i64) <= 1,
                "SYSDATE year {y}, want ~{now_year}: {s}"
            );
        }
        other => panic!("SYSDATE must be a string, got {other:?}"),
    }
}

#[test]
fn format_timestamp_known_dates() {
    // 2026-09-13T00:00:00Z and the epoch.
    assert_eq!(
        crate::value::format_timestamp_ms(1_789_257_600_000),
        "2026-09-13T00:00:00.000Z"
    );
    assert_eq!(
        crate::value::format_timestamp_ms(0),
        "1970-01-01T00:00:00.000Z"
    );
    // 2000-02-29T23:59:59Z — leap day and second-of-day edge.
    assert_eq!(
        crate::value::format_timestamp_ms(951_868_799_000),
        "2000-02-29T23:59:59.000Z"
    );
}

#[test]
fn merge_into_upsert_over_arbitrary_match() {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE stock (sku INT PRIMARY KEY, qty INT)")
        .unwrap();
    db.execute("INSERT INTO stock VALUES (1, 10), (2, 20)")
        .unwrap();
    db.execute("CREATE TABLE feed (sku INT, qty INT)").unwrap();
    db.execute("INSERT INTO feed VALUES (1, 5), (3, 30)")
        .unwrap();

    // Oracle-style MERGE: matched → qty update, not-matched → insert.
    db.execute(
        "MERGE INTO stock USING feed ON stock.sku = feed.sku \
             WHEN MATCHED THEN UPDATE SET qty = feed.qty \
             WHEN NOT MATCHED THEN INSERT (sku, qty) VALUES (feed.sku, feed.qty)",
    )
    .unwrap();
    let r = rows(&mut db, "SELECT sku, qty FROM stock ORDER BY sku");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(1), Value::Int(5)],
            vec![Value::Int(2), Value::Int(20)],
            vec![Value::Int(3), Value::Int(30)],
        ]
    );

    // Cross-check the table-local translation on a second database.
    let mut db2 = Database::in_memory().unwrap();
    db2.execute("CREATE TABLE s (k INT, v TEXT)").unwrap();
    db2.execute("INSERT INTO s VALUES (1, 'a')").unwrap();
    db2.execute("CREATE TABLE src (k INT, v TEXT)").unwrap();
    db2.execute("INSERT INTO src VALUES (1, 'x'), (2, 'y')")
        .unwrap();
    db2.execute(
        "MERGE INTO s USING src ON s.k = src.k \
             WHEN MATCHED THEN UPDATE SET v = src.v \
             WHEN NOT MATCHED THEN INSERT (k, v) VALUES (src.k, src.v)",
    )
    .unwrap();
    assert_eq!(
        rows(&mut db2, "SELECT k, v FROM s ORDER BY k").rows,
        vec![
            vec![Value::Int(1), Value::Str("x".into())],
            vec![Value::Int(2), Value::Str("y".into())],
        ]
    );
}

#[test]
fn merge_rejects_double_match_and_keeps_row() {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    db.execute("CREATE TABLE s (id INT)").unwrap();
    // Two source rows matching one target row: ORA-30926-style refusal.
    db.execute("INSERT INTO s VALUES (1), (1)").unwrap();
    let e = db
        .execute(
            "MERGE INTO t USING s ON t.id = s.id \
                 WHEN MATCHED THEN UPDATE SET v = 'x'",
        )
        .unwrap_err();
    assert!(
        e.to_string().contains("multiple source rows matched"),
        "{e}"
    );
    // The target row is untouched.
    assert_eq!(
        rows(&mut db, "SELECT v FROM t WHERE id = 1").rows,
        vec![vec![Value::Str("a".into())]]
    );
}

#[test]
fn merge_using_derived_subquery() {
    // A derived-subquery USING source (no backing table) exercises the
    // subquery load path in exec_merge and the merge read-target
    // classification (the USING side is a read, not the target).
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    db.execute(
        "MERGE INTO t USING (SELECT 1 AS id, 'x' AS v UNION ALL SELECT 2, 'y') feed \
             ON t.id = feed.id \
             WHEN MATCHED THEN UPDATE SET v = feed.v \
             WHEN NOT MATCHED THEN INSERT (id, v) VALUES (feed.id, feed.v)",
    )
    .unwrap();
    assert_eq!(
        rows(&mut db, "SELECT id, v FROM t ORDER BY id").rows,
        vec![
            vec![Value::Int(1), Value::Str("x".into())],
            vec![Value::Int(2), Value::Str("y".into())],
        ]
    );
}

#[test]
fn stmt_read_targets_classification() {
    // Authorization taxonomy: SELECT walks the whole query graph; the
    // read side of INSERT .. SELECT, UPDATE .. FROM, DELETE .. USING and
    // MERGE USING resolve to the source tables (never the write target);
    // unrecognized statements classify as no reads (their write target
    // classification gates them instead).
    let parse = |sql: &str| {
        let mut stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        stmts.swap_remove(0)
    };
    let read = |sql: &str| Database::stmt_read_targets(&parse(sql), &|_| false).unwrap_or_default();

    assert_eq!(
        read("SELECT a.id FROM a JOIN b ON a.id = b.id"),
        vec!["a", "b"]
    );
    // Nested derived tables contribute their inside names.
    assert_eq!(read("SELECT * FROM (SELECT * FROM s) AS x"), vec!["s"]);
    assert_eq!(read("INSERT INTO t SELECT * FROM s"), vec!["s"]);
    assert_eq!(
        read("UPDATE t SET v = s.v FROM s WHERE t.id = s.id"),
        vec!["s"]
    );
    assert_eq!(read("DELETE FROM t USING s WHERE t.id = s.id"), vec!["s"]);
    assert_eq!(
        read("MERGE INTO t USING s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)"),
        vec!["s"]
    );
    // Subqueries inside the WHERE of a plain SELECT count too.
    assert_eq!(
        read("SELECT * FROM a WHERE id IN (SELECT b.id FROM b)"),
        vec!["a", "b"]
    );
    // Non-query statements carry no read targets.
    assert_eq!(read("CREATE TABLE t (id INT)"), Vec::<String>::new());
}

#[test]
fn stmt_read_targets_cover_expression_reads() {
    // The write-adjacent expression surfaces a write path actually
    // evaluates — UPDATE assignment subqueries, aggregate FILTER
    // conditions, MERGE WHEN predicates and CTAS/CREATE VIEW sources —
    // must classify as reads, or a granted write on one table leaks
    // data out of tables the user cannot SELECT (docsql_users hashes
    // being the prize).
    let parse = |sql: &str| {
        let mut stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        stmts.swap_remove(0)
    };
    let read = |sql: &str| Database::stmt_read_targets(&parse(sql), &|_| false).unwrap_or_default();

    // UPDATE: assignment-value subquery (exec_update substitutes and
    // evaluates these).
    assert_eq!(
        read("UPDATE t SET v = (SELECT pw FROM docsql_users LIMIT 1) WHERE id = 1"),
        vec!["docsql_users"]
    );
    // Aggregate FILTER carries a hidden subquery.
    assert_eq!(
        read("SELECT count(*) FILTER (WHERE EXISTS (SELECT 1 FROM secret)) FROM t"),
        vec!["secret", "t"]
    );
    // MERGE WHEN predicate subqueries.
    assert_eq!(
        read(
            "MERGE INTO t USING s ON t.id = s.id \
                 WHEN MATCHED AND t.id IN (SELECT id FROM audit) THEN UPDATE SET v = s.v"
        ),
        vec!["audit", "s"]
    );
    // CTAS and CREATE VIEW sources execute at creation time.
    assert_eq!(read("CREATE TABLE x AS SELECT * FROM src"), vec!["src"]);
    assert_eq!(read("CREATE VIEW v AS SELECT * FROM src"), vec!["src"]);
}

#[test]
fn lpad_rejects_astronomic_lengths() {
    // One SELECT used to be able to abort the whole node via
    // with_capacity(2^62).
    let mut db = Database::in_memory().unwrap();
    let e = db
        .execute("SELECT LPAD('a', 9223372036854775807)")
        .expect_err("must error");
    assert!(e.to_string().contains("maximum"), "{e}");
    // Normal padding is untouched, including truncation.
    let r = rows(&mut db, "SELECT LPAD('x', 3, 'ab'), RPAD('toolong', 4)");
    assert_eq!(
        r.rows,
        vec![vec![Value::Str("abx".into()), Value::Str("tool".into())]]
    );
}

#[test]
fn grouping_sets_product_is_capped() {
    // Three 12-element CUBEs multiply to 2^36 sets: the expansion must
    // refuse instead of allocating.
    let mut db = Database::in_memory().unwrap();
    let cols: Vec<String> = (1..=12).map(|i| format!("c{i} INT")).collect();
    run(&mut db, &format!("CREATE TABLE wide ({})", cols.join(", ")));
    let cube = format!(
        "CUBE({})",
        (1..=12)
            .map(|i| format!("c{i}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let sql = format!("SELECT c1 FROM wide GROUP BY {cube}, {cube}, {cube}");
    let e = db.execute(&sql).expect_err("must error");
    assert!(e.to_string().contains("grouping sets"), "{e}");
    // A single CUBE(12) stays under the cap.
    let vals: Vec<String> = (1..=12).map(|i| i.to_string()).collect();
    run(
        &mut db,
        &format!("INSERT INTO wide VALUES ({})", vals.join(", ")),
    );
    let r = rows(
        &mut db,
        &format!("SELECT count(*) FROM wide GROUP BY {cube}"),
    );
    assert_eq!(r.rows.len(), 4096);
}

#[test]
fn dictionaries_hide_internal_tables() {
    let mut db = Database::in_memory().unwrap();
    db.ensure_pubsub_table().unwrap();
    run(&mut db, "CREATE TABLE user_t (id INT)");
    for sql in [
        "SELECT name FROM sqlite_master WHERE type = 'table'",
        "SELECT table_name FROM information_schema.tables",
        "SELECT table_name FROM information_schema.columns WHERE column_name = 'payload'",
    ] {
        let r = rows(&mut db, sql);
        let hit = r.rows.iter().any(|row| {
            row.first()
                .is_some_and(|v| matches!(v, Value::Str(s) if s == PUBSUB_TABLE))
        });
        assert!(!hit, "{sql} must not list {PUBSUB_TABLE}");
    }
    let r = rows(
        &mut db,
        "SELECT name FROM sqlite_master WHERE type = 'table'",
    );
    assert_eq!(r.rows, vec![vec![Value::Str("user_t".into())]]);
}

#[test]
fn create_table_cannot_squat_system_names() {
    // A user-created table named like an engine system table would be
    // excluded from dumps/digests by name — a silent data-loss trap.
    let mut db = Database::in_memory().unwrap();
    for name in [
        PUBSUB_TABLE,
        CLUSTER_LOG_TABLE,
        CLUSTER_POS_TABLE,
        CLUSTER_ID_TABLE,
    ] {
        let e = db
            .execute(&format!("CREATE TABLE {name} (x INT)"))
            .err()
            .unwrap_or_else(|| panic!("{name} must be refused"));
        assert!(e.to_string().contains("reserved"), "{e}");
    }
    // The sanctioned internal path still works.
    db.ensure_cluster_tables().unwrap();
    db.ensure_pubsub_table().unwrap();
}

#[test]
fn stmt_write_targets_classification() {
    // The server's system-table gate classifies on this: every mutating
    // statement arm lists its target table (and any RENAME destination),
    // never the tables it merely reads — so a literal mentioning a
    // system table is not refused while a write aimed at one still is.
    let parse = |sql: &str| {
        let mut stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        stmts.swap_remove(0)
    };
    let write = |sql: &str| Database::stmt_write_targets(&parse(sql));

    assert_eq!(write("INSERT INTO t VALUES (1)"), vec!["t"]);
    assert_eq!(
        write("MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE"),
        vec!["t"]
    );
    assert_eq!(write("UPDATE t SET a = 1"), vec!["t"]);
    assert_eq!(write("DELETE FROM t WHERE a = 1"), vec!["t"]);
    assert_eq!(write("CREATE TABLE t (a INT)"), vec!["t"]);
    assert_eq!(write("CREATE VIEW v AS SELECT 1"), vec!["v"]);
    assert_eq!(write("CREATE INDEX i ON t (a)"), vec!["t"]);
    assert_eq!(write("DROP TABLE t"), vec!["t"]);
    assert_eq!(write("ALTER TABLE t RENAME TO t2"), vec!["t", "t2"]);
    assert_eq!(write("ALTER TABLE t ADD COLUMN c INT"), vec!["t"]);
    assert_eq!(write("TRUNCATE TABLE t"), vec!["t"]);
    // Reads and transaction control are not writes.
    assert_eq!(write("SELECT * FROM t"), Vec::<String>::new());
    assert_eq!(write("BEGIN"), Vec::<String>::new());
}

#[test]
fn correlated_subquery_ref_arms_resolve_per_row() {
    // Every expression shape the old rejection collector knew about
    // (Between / Like / Case / Cast / inner projection / inner HAVING)
    // now binds the enclosing row's values instead of refusing.
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE a (id INT PRIMARY KEY, pattern TEXT, low INT, high INT)",
    );
    run(&mut db, "CREATE TABLE b (x INT, y INT, z TEXT)");
    run(
        &mut db,
        "INSERT INTO a VALUES (1, '%z', 0, 9), (2, '%q', 5, 6)",
    );
    run(
        &mut db,
        "INSERT INTO b VALUES (1, 2, 'fizz'), (2, 8, 'seq')",
    );
    // Non-correlated subqueries still resolve fine (control).
    run(&mut db, "SELECT id FROM a WHERE id IN (SELECT x FROM b)");

    let cases: [(&str, Vec<i64>); 6] = [
            ("SELECT id FROM a WHERE id IN (SELECT x FROM b WHERE b.y BETWEEN a.low AND a.high)", vec![1]),
            ("SELECT id FROM a WHERE id IN (SELECT x FROM b WHERE b.z LIKE a.pattern)", vec![1, 2]),
            (
                "SELECT id FROM a WHERE id IN (SELECT x FROM b WHERE CASE WHEN b.y > 0 THEN a.id ELSE b.y END > 0)",
                vec![1, 2],
            ),
            (
                "SELECT id FROM a WHERE id IN (SELECT x FROM b WHERE CAST(a.low AS INT) > 0)",
                vec![2],
            ),
            ("SELECT id FROM a WHERE id IN (SELECT a.id FROM b)", vec![1, 2]),
            (
                "SELECT id FROM a WHERE id IN (SELECT x FROM b GROUP BY x HAVING SUM(b.y) > a.id)",
                vec![1, 2],
            ),
        ];
    for (sql, want) in cases {
        let mut r = rows(&mut db, sql);
        r.rows.sort_by(|a, b| {
            crate::value::Value::cmp_values(
                a.first().unwrap_or(&Value::Null),
                b.first().unwrap_or(&Value::Null),
            )
        });
        let got: Vec<i64> = r
            .rows
            .iter()
            .map(|row| match row.first() {
                Some(Value::Int(n)) => *n,
                other => panic!("{sql}: expected int, got {other:?}"),
            })
            .collect();
        assert_eq!(got, want, "{sql}");
    }
}

#[test]
fn correlated_subqueries_supported_surfaces() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE emp (id INT PRIMARY KEY, name TEXT, dept INT, salary INT)",
    );
    run(&mut db, "CREATE TABLE dept (id INT PRIMARY KEY, name TEXT)");
    run(&mut db, "CREATE TABLE nums (x INT)");
    run(
        &mut db,
        "INSERT INTO emp VALUES (1,'a',10,300),(2,'b',10,100),(3,'c',20,300),(4,'d',20,200)",
    );
    run(
        &mut db,
        "INSERT INTO dept VALUES (10,'eng'),(20,'ops'),(30,'hr')",
    );
    run(&mut db, "INSERT INTO nums VALUES (1),(2),(3)");

    // WHERE EXISTS matches the JOIN equivalent.
    let corr = rows(
        &mut db,
        "SELECT e.name FROM emp e
             WHERE EXISTS (SELECT 1 FROM dept d WHERE d.id = e.dept) ORDER BY e.id",
    );
    let join = rows(
        &mut db,
        "SELECT e.name FROM emp e JOIN dept d ON d.id = e.dept ORDER BY e.id",
    );
    assert_eq!(corr.rows, join.rows);
    // NOT EXISTS anti-join.
    let r = rows(
        &mut db,
        "SELECT e.name FROM emp e
             WHERE NOT EXISTS (SELECT 1 FROM dept d WHERE d.id = e.dept + 100)",
    );
    assert_eq!(r.rows.len(), 4);
    // max-per-group scalar projection (the classic correlated shape).
    let r = rows(
        &mut db,
        "SELECT d.name, (SELECT MAX(e.salary) FROM emp e WHERE e.dept = d.id) AS top
             FROM dept d ORDER BY d.id",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("eng".into()), Value::Int(300)],
            vec![Value::Str("ops".into()), Value::Int(300)],
            vec![Value::Str("hr".into()), Value::Null],
        ]
    );
    // ORDER BY a correlated key.
    let r = rows(
        &mut db,
        "SELECT e.name FROM emp e
             ORDER BY (SELECT COUNT(*) FROM emp e2 WHERE e2.salary < e.salary), e.id",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("b".into())],
            vec![Value::Str("d".into())],
            vec![Value::Str("a".into())],
            vec![Value::Str("c".into())],
        ]
    );
    // Inner alias shadows the outer one: `d` inside binds emp — the
    // predicate is row-independent, so every outer dept matches.
    let r = rows(
        &mut db,
        "SELECT d.name FROM dept d WHERE EXISTS (SELECT 1 FROM emp d WHERE d.dept = 20)",
    );
    assert_eq!(r.rows.len(), 3);
    // Uncorrelated regression: still evaluated exactly once.
    let r = rows(
        &mut db,
        "SELECT COUNT(*) FROM emp WHERE salary > (SELECT AVG(salary) FROM emp)",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(2)]]);

    // UPDATE with a correlated WHERE (slow path, bare table qualifier).
    run(
        &mut db,
        "UPDATE emp SET salary = salary + 1
             WHERE EXISTS (SELECT 1 FROM dept d WHERE d.id = emp.dept AND d.name = 'ops')",
    );
    let r = rows(
        &mut db,
        "SELECT salary FROM emp WHERE id IN (3, 4) ORDER BY id",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(301)], vec![Value::Int(201)]]);
    // UPDATE fast path: index probe on the equality conjunct, the
    // correlated EXISTS runs as the per-row residual.
    run(
        &mut db,
        "UPDATE emp SET salary = salary + 10
             WHERE id = 1 AND EXISTS (SELECT 1 FROM dept d WHERE d.id = emp.dept)",
    );
    let r = rows(&mut db, "SELECT salary FROM emp WHERE id = 1");
    assert_eq!(r.rows, vec![vec![Value::Int(310)]]);
    // UPDATE SET with a correlated scalar subquery.
    run(
        &mut db,
        "UPDATE dept SET name = (SELECT MAX(e.name) FROM emp e WHERE e.dept = dept.id)",
    );
    let r = rows(&mut db, "SELECT name FROM dept ORDER BY id");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("b".into())],
            vec![Value::Str("d".into())],
            vec![Value::Null],
        ]
    );
    // DELETE with a correlated WHERE: departments without employees.
    run(
        &mut db,
        "DELETE FROM dept WHERE NOT EXISTS (SELECT 1 FROM emp e WHERE e.dept = dept.id)",
    );
    let r = rows(&mut db, "SELECT COUNT(*) FROM dept");
    assert_eq!(r.rows, vec![vec![Value::Int(2)]]);

    // Surface boundaries stay loud: JOIN ON / HAVING / GROUP BY evaluate
    // outside a full row, correlation there keeps refusing.
    for sql in [
            "SELECT e.name FROM emp e JOIN dept d ON d.id = (SELECT MIN(x) FROM nums WHERE x < e.dept)",
            "SELECT dept FROM emp GROUP BY dept HAVING COUNT(*) > (SELECT COUNT(*) FROM nums WHERE x < emp.dept)",
            "SELECT dept FROM emp GROUP BY (SELECT MAX(x) FROM nums WHERE x < emp.dept)",
        ] {
            let e = db.execute(sql).unwrap_err();
            assert!(
                e.to_string().contains("correlated subqueries are not supported"),
                "{sql}: {e}"
            );
        }
}

#[test]
fn correlated_depth_guard_bounds() {
    // Eight levels of nested correlated evaluation are allowed; the
    // ninth refuses loudly instead of eating the thread stack.
    {
        let mut guards = Vec::new();
        for _ in 0..super::MAX_CORRELATED_DEPTH {
            guards.push(super::enter_correlated_eval().unwrap());
        }
        assert!(super::enter_correlated_eval().is_err());
    }
    // Guards released → the depth is back to zero.
    assert!(super::enter_correlated_eval().is_ok());
}

#[test]
fn newid_inside_subquery_of_write_rejected() {
    // A nondeterministic call inside a subquery of a write used to slip
    // past the NEWID gate (the expression walker did not recurse into
    // subqueries); every replaying peer would roll its own GUIDs and the
    // fan-out would silently diverge.
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    run(&mut db, "INSERT INTO t VALUES (1, 10), (2, 20)");
    for sql in [
            "UPDATE t SET v = v WHERE EXISTS (SELECT 1 FROM t t2 WHERE t2.id = t.id AND NEWID() IS NOT NULL)",
            "DELETE FROM t WHERE id IN (SELECT id FROM t WHERE NEWID() IS NOT NULL)",
        ] {
            let e = db.execute(sql).unwrap_err();
            assert!(e.to_string().contains("NEWID()"), "{sql}: {e}");
        }
    let r = rows(&mut db, "SELECT COUNT(*) FROM t");
    assert_eq!(r.rows, vec![vec![Value::Int(2)]]);
}

/// Regression: a derived table whose inner projection uses QUALIFIED column
/// names (`SELECT "p"."Id" …`) must expose bare output keys (`Id`). Kept
/// qualified, the outer reference `p0.Id` missed the direct key and fell
/// through `lookup_col`'s join suffix scan onto the RIGHT side's same-named
/// column (`c.Id`), so a LEFT JOIN could pair a post with another post's
/// comment. EF Core's `Include` emits exactly this shape.
#[test]
fn derived_table_qualified_projection_stays_bare_named() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE posts (id INT AUTOINCREMENT PRIMARY KEY, title VARCHAR)",
    );
    run(
        &mut db,
        "CREATE TABLE comments (id INT AUTOINCREMENT PRIMARY KEY, post_id INT, body VARCHAR)",
    );
    run(&mut db, "INSERT INTO posts (title) VALUES ('p1')");
    run(&mut db, "INSERT INTO posts (title) VALUES ('p2')");
    run(
        &mut db,
        "INSERT INTO comments (post_id, body) VALUES (1, 'c1')",
    );

    // The EF Include shape: derived table on the LEFT of a LEFT JOIN, no
    // comment for the selected post → right columns must be NULL, and the
    // left Id must be post 2 (not the comment's Id 1).
    let r = rows(
        &mut db,
        "SELECT p0.id, p0.title, c.id, c.body
           FROM (SELECT \"p\".\"id\", \"p\".\"title\" FROM posts AS \"p\"
                  WHERE \"p\".\"title\" = 'p2' LIMIT 1) AS p0
           LEFT JOIN comments AS c ON p0.id = c.post_id
           ORDER BY p0.id",
    );
    assert_eq!(
        r.rows,
        vec![vec![
            Value::Int(2),
            Value::Str("p2".into()),
            Value::Null,
            Value::Null
        ]]
    );

    // A matching child still pairs normally (the earlier false match was
    // only visible when Ids collided).
    let r = rows(
        &mut db,
        "SELECT p0.id, c.body
           FROM (SELECT \"p\".\"id\" FROM posts AS \"p\" WHERE \"p\".\"title\" = 'p1') AS p0
           LEFT JOIN comments AS c ON p0.id = c.post_id",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(1), Value::Str("c1".into())]]);

    // Bare (unqualified) reference to the derived output also resolves.
    let r = rows(
        &mut db,
        "SELECT id FROM (SELECT \"p\".\"id\" FROM posts AS \"p\"
                          WHERE \"p\".\"title\" = 'p2') AS p0",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(2)]]);
}

/// Same contract for CTE bodies: qualified inner projections are addressed
/// by their bare output name through the CTE alias.
#[test]
fn cte_qualified_projection_stays_bare_named() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE posts (id INT AUTOINCREMENT PRIMARY KEY, title VARCHAR)",
    );
    run(&mut db, "INSERT INTO posts (title) VALUES ('p1')");
    run(&mut db, "INSERT INTO posts (title) VALUES ('p2')");
    let r = rows(
        &mut db,
        "WITH recent AS (SELECT \"p\".\"id\", \"p\".\"title\" FROM posts AS \"p\"
                          WHERE \"p\".\"title\" = 'p2')
         SELECT r.id, r.title FROM recent AS r",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(2), Value::Str("p2".into())]]);
}
