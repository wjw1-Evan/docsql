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

fn str_col(r: &QueryResult, c: usize) -> Vec<String> {
    r.rows
        .iter()
        .map(|row| match &row[c] {
            Value::Str(s) => s.clone(),
            other => other.to_string(),
        })
        .collect()
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

/// Numeric column reader tolerant of Int/Float/Decimal (AVG returns
/// Float even over Int frames; SUM over Decimal stays Decimal).
fn num_col(r: &QueryResult, c: usize) -> Vec<f64> {
    r.rows
        .iter()
        .map(|row| match &row[c] {
            Value::Int(i) => *i as f64,
            Value::Float(f) => *f,
            Value::Decimal(d) => d.to_f64().unwrap_or(f64::NAN),
            other => panic!("expected number, got {other:?}"),
        })
        .collect()
}

fn opt_int_col(r: &QueryResult, c: usize) -> Vec<Option<i64>> {
    r.rows
        .iter()
        .map(|row| match &row[c] {
            Value::Int(i) => Some(*i),
            Value::Null => None,
            other => panic!("expected int?, got {other:?}"),
        })
        .collect()
}
fn opt_num_col(r: &QueryResult, c: usize) -> Vec<Option<f64>> {
    r.rows
        .iter()
        .map(|row| match &row[c] {
            Value::Int(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            Value::Decimal(d) => Some(d.to_f64().unwrap_or(f64::NAN)),
            Value::Null => None,
            other => panic!("expected number?, got {other:?}"),
        })
        .collect()
}

fn win_db() -> Database {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE e (id INT PRIMARY KEY, dept TEXT, name TEXT, salary INT)",
    );
    run(
        &mut db,
        "INSERT INTO e VALUES (1,'a','x',30),(2,'a','y',20),(3,'a','z',20),\
             (4,'b','p',10),(5,'b','q',40)",
    );
    db
}

// ===== Custom window frames (ROWS/RANGE/GROUPS BETWEEN … AND …) =====

#[test]
fn frame_rows_slides_every_aggregate() {
    let mut db = win_db();
    // Sorted by salary (ties by name): p(10), y(20), z(20), x(30), q(40).
    let r = rows(
        &mut db,
        "SELECT name, \
             SUM(salary) OVER (ORDER BY salary ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) s, \
             COUNT(*) OVER (ORDER BY salary ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) c, \
             AVG(salary) OVER (ORDER BY salary ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a, \
             MIN(salary) OVER (ORDER BY salary ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) mn, \
             MAX(salary) OVER (ORDER BY salary ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) mx \
             FROM e ORDER BY salary, name",
    );
    assert_eq!(str_col(&r, 0), vec!["p", "y", "z", "x", "q"]);
    assert_eq!(int_col(&r, 1), vec![10, 30, 40, 50, 70]);
    assert_eq!(int_col(&r, 2), vec![1, 2, 2, 2, 2]);
    // AVG over Int frames returns a Float: 10.0, 15.0, 20.0, 25.0, 35.0.
    assert_eq!(num_col(&r, 3), vec![10.0, 15.0, 20.0, 25.0, 35.0]);
    assert_eq!(int_col(&r, 4), vec![10, 10, 20, 20, 30]);
    assert_eq!(int_col(&r, 5), vec![10, 20, 20, 30, 40]);
}

#[test]
fn frame_rows_prefix_suffix_and_unbounded() {
    let mut db = win_db();
    // Running total under an explicit default spelling must equal the
    // implicit default frame.
    let r = rows(
        &mut db,
        "SELECT SUM(salary) OVER (ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING \
             AND CURRENT ROW) FROM e ORDER BY salary, name",
    );
    assert_eq!(int_col(&r, 0), vec![10, 30, 50, 80, 120]);
    // Suffix window: the low edge slides, the high edge pins to the end.
    let r = rows(
        &mut db,
        "SELECT SUM(salary) OVER (ORDER BY salary ROWS BETWEEN CURRENT ROW \
             AND UNBOUNDED FOLLOWING) FROM e ORDER BY salary, name",
    );
    assert_eq!(int_col(&r, 0), vec![120, 110, 90, 70, 40]);
    // Both edges offset from the current row; trailing rows frame
    // nothing and aggregate to NULL (COUNT stays 0).
    let r = rows(
        &mut db,
        "SELECT name, SUM(salary) OVER (ORDER BY salary, name ROWS BETWEEN 2 FOLLOWING \
             AND 3 FOLLOWING) s, COUNT(*) OVER (ORDER BY salary, name ROWS BETWEEN 2 FOLLOWING \
             AND 3 FOLLOWING) c FROM e ORDER BY salary, name",
    );
    assert_eq!(str_col(&r, 0), vec!["p", "y", "z", "x", "q"]);
    // p: y+z+x? no — positions 2..3 = z+x = 50; y: x+q = 70; z: q = 40;
    // x and q frame past the end: empty — SUM NULL, COUNT 0.
    assert_eq!(
        opt_int_col(&r, 1),
        vec![Some(50), Some(70), Some(40), None, None]
    );
    assert_eq!(int_col(&r, 2), vec![2, 2, 1, 0, 0]);
}

#[test]
fn frame_range_value_intervals_include_peers() {
    let mut db = win_db();
    // RANGE shifts the sort KEY, so peer rows always travel together.
    // Sorted salaries: 10, 20, 20, 30, 40.
    let r = rows(
        &mut db,
        "SELECT name, \
             SUM(salary) OVER (ORDER BY salary RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) s \
             FROM e ORDER BY salary, name",
    );
    assert_eq!(str_col(&r, 0), vec!["p", "y", "z", "x", "q"]);
    assert_eq!(int_col(&r, 1), vec![10, 40, 40, 30, 40]);
    // Explicit default spelling: running total with peers.
    let r = rows(
        &mut db,
        "SELECT SUM(salary) OVER (ORDER BY salary RANGE BETWEEN UNBOUNDED PRECEDING \
             AND CURRENT ROW) FROM e ORDER BY salary, name",
    );
    assert_eq!(int_col(&r, 0), vec![10, 50, 50, 80, 120]);
    // Interval that no key falls into: the empty frame aggregates to
    // NULL (COUNT 0).
    let r = rows(
        &mut db,
        "SELECT name, SUM(salary) OVER (ORDER BY salary RANGE BETWEEN 2 PRECEDING \
             AND 1 PRECEDING) s, COUNT(*) OVER (ORDER BY salary RANGE BETWEEN 2 PRECEDING \
             AND 1 PRECEDING) c FROM e ORDER BY salary, name",
    );
    // [k-2, k-1] misses every salary here: empty frame = SUM NULL, COUNT 0.
    assert_eq!(
        opt_int_col(&r, 1),
        vec![None, None, None, None, None],
        "{r:?}"
    );
    assert_eq!(int_col(&r, 2), vec![0, 0, 0, 0, 0]);
}

#[test]
fn frame_range_desc_and_wide_intervals() {
    let mut db = win_db();
    // DESC: `1 PRECEDING` means one unit LARGER (the sort direction
    // flips, not the arithmetic).
    let r = rows(
        &mut db,
        "SELECT name, SUM(salary) OVER (ORDER BY salary DESC RANGE BETWEEN 1 PRECEDING \
             AND CURRENT ROW) s FROM e ORDER BY salary DESC, name",
    );
    assert_eq!(str_col(&r, 0), vec!["q", "x", "y", "z", "p"]);
    assert_eq!(int_col(&r, 1), vec![40, 30, 40, 40, 10]);
    // A wide symmetric interval groups the whole neighborhood.
    let r = rows(
        &mut db,
        "SELECT name, SUM(salary) OVER (ORDER BY salary RANGE BETWEEN 5 PRECEDING \
             AND 5 FOLLOWING) s FROM e ORDER BY salary, name",
    );
    assert_eq!(int_col(&r, 1), vec![10, 40, 40, 30, 40]);
}

#[test]
fn frame_groups_counts_peer_groups() {
    let mut db = win_db();
    // Peer groups by salary: {10},{20,20},{30},{40}. One group each way.
    let r = rows(
        &mut db,
        "SELECT name, SUM(salary) OVER (ORDER BY salary GROUPS BETWEEN 1 PRECEDING \
             AND 1 FOLLOWING) s FROM e ORDER BY salary, name",
    );
    assert_eq!(str_col(&r, 0), vec!["p", "y", "z", "x", "q"]);
    assert_eq!(int_col(&r, 1), vec![50, 80, 80, 110, 70]);
    // CURRENT ROW start pins the frame to the own group.
    let r = rows(
        &mut db,
        "SELECT name, SUM(salary) OVER (ORDER BY salary GROUPS BETWEEN CURRENT ROW \
             AND 1 FOLLOWING) s FROM e ORDER BY salary, name",
    );
    // y/z ride their own group plus {30}: 20+20+30 = 70.
    assert_eq!(int_col(&r, 1), vec![50, 70, 70, 70, 40]);
}

#[test]
fn frame_first_last_value_walk_the_frame() {
    let mut db = win_db();
    let r = rows(
        &mut db,
        "SELECT name, salary, \
             FIRST_VALUE(salary) OVER (ORDER BY salary, name ROWS BETWEEN 1 PRECEDING \
             AND 1 FOLLOWING) f, \
             LAST_VALUE(salary) OVER (ORDER BY salary, name ROWS BETWEEN 1 PRECEDING \
             AND 1 FOLLOWING) l \
             FROM e ORDER BY salary, name",
    );
    assert_eq!(str_col(&r, 0), vec!["p", "y", "z", "x", "q"]);
    assert_eq!(int_col(&r, 2), vec![10, 10, 20, 20, 30]);
    assert_eq!(int_col(&r, 3), vec![20, 20, 30, 40, 40]);
    // The default frame keeps its historical RANGE semantics: LAST_VALUE
    // lands on the peer-run end, not the row.
    let r = rows(
        &mut db,
        "SELECT name, LAST_VALUE(salary) OVER (ORDER BY salary) FROM e \
             ORDER BY salary, name",
    );
    assert_eq!(int_col(&r, 1), vec![10, 20, 20, 30, 40]);
}

#[test]
fn frame_partitions_decimal_filter_distinct_and_concat() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (g TEXT, k INT, v DECIMAL)");
    // CAST literals: bare `1.0` parses as Float (documented boundary),
    // and this test exercises the DECIMAL exact-sum path.
    run(
        &mut db,
        "INSERT INTO t VALUES ('a',1,CAST('10.5' AS DECIMAL)),('a',2,CAST('20.0' AS DECIMAL)),\
             ('a',3,CAST('30.5' AS DECIMAL)),('b',1,CAST('1.0' AS DECIMAL)),\
             ('b',2,CAST('2.0' AS DECIMAL)),('b',3,CAST('3.0' AS DECIMAL))",
    );
    // Partitioned frames never leak across the boundary.
    let r = rows(
        &mut db,
        "SELECT g, k, SUM(v) OVER (PARTITION BY g ORDER BY k ROWS BETWEEN 1 PRECEDING \
             AND CURRENT ROW) s FROM t ORDER BY g, k",
    );
    assert_eq!(str_col(&r, 0), vec!["a", "a", "a", "b", "b", "b"]);
    // DECIMAL sums stay exact: 10.5 / 30.5 / 50.5 and 1.0 / 3.0 / 5.0.
    assert_eq!(num_col(&r, 2), vec![10.5, 30.5, 50.5, 1.0, 3.0, 5.0]);
    // FILTER rides the frame.
    let r = rows(
        &mut db,
        "SELECT k, SUM(v) FILTER (WHERE v > 1.0) OVER (ORDER BY k ROWS BETWEEN \
             UNBOUNDED PRECEDING AND CURRENT ROW) s FROM t WHERE g = 'b' ORDER BY k",
    );
    // k=1's own row is filtered out and nothing precedes it: NULL.
    assert_eq!(opt_num_col(&r, 1), vec![None, Some(2.0), Some(5.0)]);
    // DISTINCT and GROUP_CONCAT fold fresh per row (non-invertible).
    let r = rows(
        &mut db,
        "SELECT k, COUNT(DISTINCT k % 2) OVER (ORDER BY k ROWS BETWEEN 1 PRECEDING \
             AND 1 FOLLOWING) d, GROUP_CONCAT(k) OVER (ORDER BY k ROWS BETWEEN 1 PRECEDING \
             AND 1 FOLLOWING) gc FROM t WHERE g = 'b' ORDER BY k",
    );
    // k%2 over each row's frame: k=1 sees {1,0}, k=2 sees {1,0,1}, k=3 {0,1}.
    assert_eq!(int_col(&r, 1), vec![2, 2, 2]);
    assert_eq!(str_col(&r, 2), vec!["1,2", "1,2,3", "2,3"]);
}

#[test]
fn qualify_filters_window_results() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE emp (id INT PRIMARY KEY, dept TEXT, name TEXT, salary INT)",
    );
    run(
        &mut db,
        "INSERT INTO emp VALUES (1,'a','x',30),(2,'a','y',20),(3,'a','z',10),\
             (4,'b','p',40),(5,'b','q',50),(6,'b','r',45)",
    );
    // Top-1 per group: THE headline QUALIFY shape.
    let r = rows(
        &mut db,
        "SELECT dept, name, salary FROM emp \
             WHERE dept = 'b' \
             QUALIFY ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) = 1",
    );
    assert_eq!(
        r.rows,
        vec![vec![
            Value::Str("b".into()),
            Value::Str("q".into()),
            Value::Int(50)
        ]],
        "{r:?}"
    );
    // Mixed window + plain column conjunct.
    let r = rows(
        &mut db,
        "SELECT name, salary FROM emp \
             QUALIFY RANK() OVER (ORDER BY salary DESC) <= 2 AND name <> 'q' ORDER BY salary DESC",
    );
    // Ranks DESC: q=1, r=2, p=3 — the name filter drops q, r survives.
    assert_eq!(
        r.rows,
        vec![vec![Value::Str("r".into()), Value::Int(45)]],
        "{r:?}"
    );
    // LAG comparison: rows whose salary grew over the previous one.
    run(&mut db, "CREATE TABLE m (d TIMESTAMP, v INT)");
    run(
        &mut db,
        "INSERT INTO m VALUES (TIMESTAMP '2026-01-01 00:00:00', 10), \
             (TIMESTAMP '2026-01-01 00:01:00', 15), \
             (TIMESTAMP '2026-01-01 00:02:00', 12), \
             (TIMESTAMP '2026-01-01 00:03:00', 20)",
    );
    // Rows whose value grew over the previous one — including the first
    // row (its LAG is NULL; the COALESCE default v-1 is always smaller).
    // Only 12 (drop from 15) is excluded.
    let r = rows(
        &mut db,
        "SELECT v FROM m QUALIFY COALESCE(LAG(v) OVER (ORDER BY d), v - 1) < v ORDER BY d",
    );
    assert_eq!(int_col(&r, 0), vec![10, 15, 20], "{r:?}");
    // DISTINCT + QUALIFY interplay: qualify filters rows, DISTINCT then
    // dedupes the surviving projections.
    let r = rows(
            &mut db,
            "SELECT DISTINCT dept FROM emp QUALIFY ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) = 1",
        );
    let mut got: Vec<String> = str_col(&r, 0);
    got.sort();
    assert_eq!(got, vec!["a", "b"]);
    // Differential: QUALIFY vs the equivalent derived-table subquery.
    let r1 = rows(
            &mut db,
            "SELECT dept, name FROM emp QUALIFY ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) = 1 ORDER BY dept",
        );
    let r2 = rows(
        &mut db,
        "SELECT dept, name FROM (SELECT dept, name, \
             ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM emp) x \
             WHERE rn = 1 ORDER BY dept",
    );
    assert_eq!(r1.rows, r2.rows);
}

#[test]
fn qualify_gates_stay_loud() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (g TEXT, v INT)");
    run(&mut db, "INSERT INTO t VALUES ('a', 1), ('b', 2)");
    // QUALIFY + GROUP BY/aggregates (window-over-groups is unsupported;
    // the same wall applies).
    let e = db
        .execute(
            "SELECT g, COUNT(*) FROM t GROUP BY g \
                 QUALIFY ROW_NUMBER() OVER (ORDER BY g) = 1",
        )
        .unwrap_err()
        .to_string();
    assert!(e.contains("combined with"), "{e}");
    let e = db
        .execute("SELECT COUNT(*) FROM t QUALIFY COUNT(*) > 0")
        .unwrap_err()
        .to_string();
    assert!(e.contains("QUALIFY is not supported combined"), "{e}");
    // QUALIFY without FROM cannot parse in the generic dialect (`QUALIFY`
    // is read as a bare alias after `1`); the in-engine guard remains for
    // dialects that do parse it — exercised nowhere else.
    // Row-local evaluation keeps the surface honest: a subquery inside
    // QUALIFY rides the same machinery as WHERE (uncorrelated resolved).
    run(&mut db, "CREATE TABLE lim (n INT)");
    run(&mut db, "INSERT INTO lim VALUES (2)");
    let r = rows(
        &mut db,
        "SELECT v FROM t QUALIFY v >= (SELECT MAX(n) FROM lim) ORDER BY v",
    );
    assert_eq!(int_col(&r, 0), vec![2], "{r:?}");
}

#[test]
fn frame_range_interval_offsets_day_time_units() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE ev (ts TIMESTAMP, v INT)");
    run(
        &mut db,
        "INSERT INTO ev VALUES \
             (TIMESTAMP '2026-01-01 00:00:00', 1), \
             (TIMESTAMP '2026-01-01 00:00:30', 2), \
             (TIMESTAMP '2026-01-01 00:01:00', 4), \
             (TIMESTAMP '2026-01-01 00:06:00', 8), \
             (TIMESTAMP '2026-01-02 00:00:00', 16)",
    );
    // INTERVAL '90' SECOND ≡ 90000 ms: each row sums its own window.
    run(&mut db, "CREATE MATERIALIZED VIEW noop AS SELECT 1 AS x");
    let _ = db.execute("DROP MATERIALIZED VIEW noop");
    let r = rows(
        &mut db,
        "SELECT v, SUM(v) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '90' SECOND PRECEDING \
             AND CURRENT ROW) s FROM ev ORDER BY ts",
    );
    assert_eq!(int_col(&r, 0), vec![1, 2, 4, 8, 16]);
    assert_eq!(int_col(&r, 1), vec![1, 3, 7, 8, 16], "{r:?}");
    // DAY unit across the midnight boundary.
    let r = rows(
        &mut db,
        "SELECT v, SUM(v) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '1' DAY PRECEDING \
             AND CURRENT ROW) s FROM ev ORDER BY ts",
    );
    assert_eq!(int_col(&r, 1), vec![1, 3, 7, 15, 31]);
    // MINUTE unit, FOLLOWING side.
    let r = rows(
        &mut db,
        "SELECT v, SUM(v) OVER (ORDER BY ts RANGE BETWEEN CURRENT ROW \
             AND INTERVAL '1' MINUTE FOLLOWING) s FROM ev ORDER BY ts",
    );
    // 00:01's +1-minute window ends 00:02 — before the 00:06 row.
    assert_eq!(int_col(&r, 1), vec![7, 6, 4, 8, 16]);
    // Fractional seconds (0.5s = 500ms).
    let r = rows(
        &mut db,
        "SELECT v, COUNT(*) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '0.5' SECOND PRECEDING \
             AND CURRENT ROW) c FROM ev ORDER BY ts",
    );
    assert_eq!(int_col(&r, 1), vec![1, 1, 1, 1, 1]);
    // INTERVAL against a numeric key is a type error.
    run(&mut db, "CREATE TABLE n (k INT, v INT)");
    run(&mut db, "INSERT INTO n VALUES (1, 1), (2, 2)");
    let e = db
        .execute(
            "SELECT SUM(v) OVER (ORDER BY k RANGE BETWEEN INTERVAL '1' DAY PRECEDING \
                 AND CURRENT ROW) FROM n",
        )
        .unwrap_err()
        .to_string();
    assert!(e.contains("TIMESTAMP ORDER BY key"), "{e}");
    // YEAR/MONTH calendar units refused.
    let e = db
        .execute(
            "SELECT SUM(v) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '1' MONTH PRECEDING \
                 AND CURRENT ROW) FROM ev",
        )
        .unwrap_err()
        .to_string();
    assert!(e.contains("DAY/HOUR/MINUTE/SECOND"), "{e}");
    // Sub-millisecond interval refused.
    let e = db
        .execute(
            "SELECT SUM(v) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '0.0001' SECOND PRECEDING \
                 AND CURRENT ROW) FROM ev",
        )
        .unwrap_err()
        .to_string();
    assert!(e.contains("smaller than one millisecond"), "{e}");
}

#[test]
fn frame_range_timestamp_key_shifts_by_milliseconds() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE ev (ts TIMESTAMP, v INT)");
    run(
        &mut db,
        "INSERT INTO ev VALUES (TIMESTAMP '2026-01-01 00:00:00', 1), \
             (TIMESTAMP '2026-01-01 00:00:30', 2), \
             (TIMESTAMP '2026-01-01 00:01:00', 4), \
             (TIMESTAMP '2026-01-01 00:02:00', 8)",
    );
    // 60s = 60000 ms: each row sums its own 60-second window.
    let r = rows(
        &mut db,
        "SELECT v, SUM(v) OVER (ORDER BY ts RANGE BETWEEN 60000 PRECEDING \
             AND CURRENT ROW) s FROM ev ORDER BY ts",
    );
    assert_eq!(int_col(&r, 0), vec![1, 2, 4, 8]);
    assert_eq!(int_col(&r, 1), vec![1, 3, 7, 12]);
}

#[test]
fn frame_range_matches_manual_interval_scan() {
    // Differential: the RANGE bounds (binary search over the sorted
    // partition) must agree with a plain interval predicate over the
    // same rows — 60 deterministic rows with duplicate keys.
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, k INT, v INT)");
    let mut batch = Vec::new();
    for id in 1..=60i64 {
        batch.push(format!("({},{},{})", id, (id * 13) % 17, id));
    }
    run(
        &mut db,
        &format!("INSERT INTO t VALUES {}", batch.join(",")),
    );
    let r = rows(
        &mut db,
        "SELECT id, SUM(v) OVER (ORDER BY k RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING) w \
             FROM t ORDER BY id",
    );
    let r2 = rows(
        &mut db,
        "SELECT a.id, (SELECT SUM(b.v) FROM t b WHERE b.k >= a.k - 2 AND b.k <= a.k + 2) m \
             FROM t a ORDER BY a.id",
    );
    assert_eq!(r.rows, r2.rows, "window frame vs manual interval scan");
}

#[test]
fn frame_sum_overflow_degrades_to_fresh_folds() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    run(
        &mut db,
        "INSERT INTO t VALUES (1, 9223372036854775807), (2, 1), (3, -1), (4, 5)",
    );
    // Sliding SUM hits the Int overflow wall mid-window: the sticky-NULL
    // state cannot be inverted, so the affected rows refold their frames
    // fresh — and every value must equal the batch aggregate over the
    // same rows (the batch fold NULLs on the same overflow).
    let r = rows(
        &mut db,
        "SELECT id, SUM(v) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) s \
             FROM t ORDER BY id",
    );
    let got: Vec<Option<i64>> = r
        .rows
        .iter()
        .map(|row| match &row[1] {
            Value::Int(i) => Some(*i),
            Value::Null => None,
            other => panic!("expected int?, got {other:?}"),
        })
        .collect();
    // id1: [MAX] = MAX; id2: [MAX,1] overflows → NULL; id3: [MAX,1,-1]
    // overflows → NULL; id4: [1,-1,5] = 5.
    assert_eq!(
        got,
        vec![Some(9223372036854775807), None, None, Some(5),],
        "{r:?}"
    );
}

#[test]
fn frame_null_keys_and_partition_boundaries_stay_sane() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (k INT, v INT)");
    run(
        &mut db,
        "INSERT INTO t VALUES (NULL,1),(NULL,2),(1,4),(2,8)",
    );
    // NULL keys have no arithmetic: their frame is their peer run.
    let r = rows(
        &mut db,
        "SELECT k, v, SUM(v) OVER (ORDER BY k RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) s \
             FROM t ORDER BY k NULLS FIRST, v",
    );
    assert_eq!(int_col(&r, 1), vec![1, 2, 4, 8]);
    // k=1: [0,1] → 4; k=2: [1,2] → 4+8 = 12.
    assert_eq!(int_col(&r, 2), vec![3, 3, 4, 12]);
}

#[test]
fn window_row_number_rank_dense_rank() {
    let mut db = win_db();
    let r = rows(
        &mut db,
        "SELECT name, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn, \
             RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rk, \
             DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS dk \
             FROM e ORDER BY dept, rn",
    );
    assert_eq!(str_col(&r, 0), vec!["x", "y", "z", "q", "p"]);
    assert_eq!(int_col(&r, 1), vec![1, 2, 3, 1, 2]);
    // y/z tie: RANK shares 2 with a gap, DENSE_RANK shares 2 without one.
    assert_eq!(int_col(&r, 2), vec![1, 2, 2, 1, 2]);
    assert_eq!(int_col(&r, 3), vec![1, 2, 2, 1, 2]);
}

#[test]
fn window_ntile_uneven_buckets() {
    let mut db = win_db();
    let r = rows(
        &mut db,
        "SELECT name, NTILE(2) OVER (ORDER BY salary) AS b FROM e ORDER BY b, salary",
    );
    // 5 rows / 2 buckets: sizes 3+2.
    assert_eq!(str_col(&r, 0), vec!["p", "y", "z", "x", "q"]);
    assert_eq!(int_col(&r, 1), vec![1, 1, 1, 2, 2]);
}

#[test]
fn window_aggregate_partition_and_running() {
    let mut db = win_db();
    let r = rows(
        &mut db,
        "SELECT name, salary, \
             SUM(salary) OVER (PARTITION BY dept) AS dept_total, \
             SUM(salary) OVER (ORDER BY salary) AS running \
             FROM e ORDER BY salary",
    );
    assert_eq!(str_col(&r, 0), vec!["p", "y", "z", "x", "q"]);
    assert_eq!(int_col(&r, 1), vec![10, 20, 20, 30, 40]);
    // Whole-partition totals without ORDER BY.
    assert_eq!(int_col(&r, 2), vec![50, 70, 70, 70, 50]);
    // Default RANGE frame: running total includes the current peer run
    // (y and z tie at 20 and share one total); the last row's frame is
    // the whole partition.
    assert_eq!(int_col(&r, 3), vec![10, 50, 50, 80, 120]);
}

#[test]
fn window_count_star_and_lag_lead() {
    let mut db = win_db();
    let r = rows(
        &mut db,
        "SELECT name, COUNT(*) OVER () AS all_rows, \
             LAG(salary, 1, 0) OVER (ORDER BY salary) AS prev, \
             LEAD(salary, 2) OVER (ORDER BY salary) AS next2 \
             FROM e ORDER BY salary",
    );
    assert_eq!(int_col(&r, 1), vec![5, 5, 5, 5, 5]);
    // LAG with default 0 at the first row; LEAD offset 2 over the end.
    assert_eq!(int_col(&r, 2), vec![0, 10, 20, 20, 30]);
    // LEAD offset 2 over the end of the last two rows.
    let got: Vec<Option<i64>> = r
        .rows
        .iter()
        .map(|row| match &row[3] {
            Value::Int(i) => Some(*i),
            Value::Null => None,
            other => panic!("expected int or null, got {other:?}"),
        })
        .collect();
    assert_eq!(got, vec![Some(20), Some(30), Some(40), None, None]);
}

#[test]
fn window_first_last_value() {
    let mut db = win_db();
    let r = rows(
        &mut db,
        "SELECT name, FIRST_VALUE(name) OVER (ORDER BY salary) AS fv, \
             LAST_VALUE(name) OVER (ORDER BY salary) AS lv \
             FROM e ORDER BY salary",
    );
    assert_eq!(str_col(&r, 1), vec!["p", "p", "p", "p", "p"]);
    // Default frame ends at the current peer run: y/z share 'z'.
    assert_eq!(str_col(&r, 2), vec!["p", "z", "z", "x", "q"]);
}

#[test]
fn window_in_expression_and_distinct() {
    let mut db = win_db();
    // Window call wrapped in arithmetic.
    let r = rows(
        &mut db,
        "SELECT DISTINCT COUNT(*) OVER () + 100 AS c FROM e",
    );
    assert_eq!(int_col(&r, 0), vec![105]);
    // DISTINCT applies after the window: ties dedup.
    let r2 = rows(
        &mut db,
        "SELECT DISTINCT DENSE_RANK() OVER (ORDER BY salary) AS dk FROM e ORDER BY dk",
    );
    assert_eq!(int_col(&r2, 0), vec![1, 2, 3, 4]);
}

#[test]
fn window_star_does_not_leak_private_columns() {
    let mut db = win_db();
    let r = rows(
        &mut db,
        "SELECT *, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM e WHERE id = 1",
    );
    assert!(r.columns.iter().all(|c| !c.ends_with('$')));
    assert_eq!(r.columns.last().unwrap(), "rn");
    assert_eq!(int_col(&r, 4), vec![1]);
}

#[test]
fn window_order_by_alias_of_window_column() {
    let mut db = win_db();
    let r = rows(
        &mut db,
        "SELECT id, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn FROM e ORDER BY rn LIMIT 2",
    );
    assert_eq!(int_col(&r, 0), vec![5, 1]);
}

#[test]
fn window_rejections_stay_loud() {
    let mut db = win_db();
    // In WHERE (row-local context cannot see other rows).
    let e = db
        .execute("SELECT id FROM e WHERE ROW_NUMBER() OVER () > 1")
        .unwrap_err();
    assert!(e.to_string().contains("window functions (OVER)"), "{e}");
    // Combined with aggregates.
    let e = db
        .execute("SELECT dept, COUNT(*), ROW_NUMBER() OVER () FROM e GROUP BY dept")
        .unwrap_err();
    assert!(
        e.to_string()
            .contains("not supported combined with GROUP BY"),
        "{e}"
    );
    // A frame clause rides only FIRST/LAST_VALUE — ranking/offset
    // functions have frame-independent semantics and refuse one loudly.
    let e = db
        .execute(
            "SELECT ROW_NUMBER() OVER (ORDER BY salary \
                 ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM e",
        )
        .unwrap_err();
    assert!(e.to_string().contains("does not accept a frame"), "{e}");
    // A RANGE value offset shifts THE sort key: without exactly one
    // ORDER BY key there is nothing to shift.
    let e = db
        .execute("SELECT SUM(salary) OVER (RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM e")
        .unwrap_err();
    assert!(e.to_string().contains("exactly one ORDER BY key"), "{e}");
    // Frame start after frame end is refused (standard).
    let e = db
        .execute(
            "SELECT SUM(salary) OVER (ORDER BY salary \
                 ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) FROM e",
        )
        .unwrap_err();
    assert!(
        e.to_string().contains("start cannot be after its end"),
        "{e}"
    );
    // Named window (needs the WINDOW clause, which is refused).
    let e = db.execute("SELECT RANK() OVER (w) FROM e").unwrap_err();
    assert!(e.to_string().contains("WINDOW"), "{e}");
    // Unknown window function.
    let e = db.execute("SELECT FOO() OVER () FROM e").unwrap_err();
    assert!(e.to_string().contains("unknown window function"), "{e}");
    // IGNORE NULLS (standard position: after the argument list).
    let e = db
        .execute("SELECT FIRST_VALUE(name) IGNORE NULLS OVER (ORDER BY id) FROM e")
        .unwrap_err();
    assert!(e.to_string().contains("IGNORE NULLS"), "{e}");
    // ORDER BY with a window call stays rejected by the row-local
    // evaluator (windows are supported in the SELECT list only).
    let e = db
        .execute("SELECT id FROM e ORDER BY ROW_NUMBER() OVER ()")
        .unwrap_err();
    assert!(e.to_string().contains("window functions (OVER)"), "{e}");
}

#[test]
fn window_aggregate_filter_and_empty_input() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (v INT)");
    // Zero rows: every window value is NULL/empty, never an error.
    let r = rows(
        &mut db,
        "SELECT ROW_NUMBER() OVER (), SUM(v) OVER () FROM t",
    );
    assert!(r.rows.is_empty());
    run(&mut db, "INSERT INTO t VALUES (1), (2), (3)");
    let r = rows(
        &mut db,
        "SELECT v, SUM(v) FILTER (WHERE v > 1) OVER (ORDER BY v) FROM t",
    );
    // SUM over an empty frame is NULL, not 0.
    let got: Vec<Option<i64>> = r
        .rows
        .iter()
        .map(|row| match &row[1] {
            Value::Int(i) => Some(*i),
            Value::Null => None,
            other => panic!("expected int or null, got {other:?}"),
        })
        .collect();
    assert_eq!(got, vec![None, Some(2), Some(5)]);
    // FILTER excludes the row BEFORE its argument is evaluated (the
    // batch order): a bitwise op that only errors on text rows must
    // not fail the window — GROUP BY form succeeds, so must OVER.
    run(&mut db, "CREATE TABLE w (txt TEXT, n INT)");
    run(
        &mut db,
        "INSERT INTO w VALUES ('text-row', 1), ('2', 4), ('3', 8)",
    );
    let r = rows(
        &mut db,
        "SELECT SUM(n & 1) FILTER (WHERE n > 1) OVER (ORDER BY n) FROM w",
    );
    assert_eq!(r.rows.len(), 3);
}

#[test]
fn window_unsupported_clauses_are_loud() {
    // Silent-ignore red line: every clause the window evaluator does
    // not implement must refuse the statement, never narrow it.
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE e (id INT, name TEXT, salary INT)");
    run(&mut db, "INSERT INTO e VALUES (1, 'a', 10), (2, 'b', 20)");
    let mut refuses = |sql: &str, needle: &str| {
        let e = db.execute(sql).unwrap_err();
        assert!(e.to_string().contains(needle), "{sql} -> {e}");
    };
    // FILTER on a scalar window function is not defined.
    refuses(
        "SELECT RANK() FILTER (WHERE salary > 15) OVER (ORDER BY salary) FROM e",
        "FILTER",
    );
    // ORDER BY inside the aggregate's own argument list.
    refuses(
        "SELECT GROUP_CONCAT(name, ',' ORDER BY salary) FROM e",
        "ORDER BY inside aggregate",
    );
    refuses(
        "SELECT GROUP_CONCAT(name, ',' ORDER BY salary) OVER () FROM e",
        "ORDER BY inside aggregate",
    );
    // Unknown columns in window keys: silently NULL keys would collapse
    // the whole computation (RANK all 1s, totals over the partition).
    refuses(
        "SELECT RANK() OVER (ORDER BY salry) FROM e",
        "column salry does not exist",
    );
    refuses(
        "SELECT RANK() OVER (PARTITION BY dept) FROM e",
        "column dept does not exist",
    );
    refuses(
        "SELECT LAG(nmae) OVER (ORDER BY id) FROM e",
        "column nmae does not exist",
    );
}

#[test]
fn window_private_column_does_not_clobber_user_field() {
    // The `__w<i>$` injection used to overwrite a user column literally
    // named like the private one; the name is now collision-checked.
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (\"__w0$\" INT, v INT)");
    run(&mut db, "INSERT INTO t VALUES (7, 10), (8, 20)");
    let r = rows(&mut db, "SELECT * , ROW_NUMBER() OVER (ORDER BY v) FROM t");
    // Column order: user fields first, the window result appended last.
    assert!(matches!(&r.rows[0][0], Value::Int(7)), "{r:?}");
    assert!(matches!(&r.rows[1][0], Value::Int(8)), "{r:?}");
    assert!(matches!(&r.rows[0][2], Value::Int(1)), "{r:?}");
    assert!(matches!(&r.rows[1][2], Value::Int(2)), "{r:?}");
}

#[test]
fn window_aggregate_incremental_matches_batch_semantics() {
    // RunningAgg folds each frame incrementally; this dataset exercises
    // every tricky equivalence: mixed numeric types, NULLs, DISTINCT,
    // peer runs, the AVG exact-decimal mode flip, MIN ties and
    // STRING_AGG with an empty-string element.
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (k INT, v INT)");
    run(
        &mut db,
        "INSERT INTO t VALUES (1, NULL), (2, 2), (2, 2), (3, 5), (4, 9)",
    );
    // Peer run (k=2) shares one cumulative value.
    let r = rows(&mut db, "SELECT SUM(v) OVER (ORDER BY k) FROM t");
    let got: Vec<String> = r
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::Null => "null".into(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => format!("{f:?}"),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(got, vec!["null", "4", "4", "9", "18"], "{r:?}");
    // DISTINCT: duplicate 2 counts once from its first appearance.
    let r = rows(&mut db, "SELECT COUNT(DISTINCT v) OVER (ORDER BY k) FROM t");
    let got: Vec<i64> = r.rows.iter().map(|row| row[0].as_i64().unwrap()).collect();
    assert_eq!(got, vec![0, 1, 1, 2, 3], "{r:?}");
    // AVG flips into exact-decimal mode when a Decimal arrives later
    // in the frame: the buffered prefix converts once, exactly.
    run(&mut db, "CREATE TABLE d (k INT, v DECIMAL)");
    run(
        &mut db,
        "INSERT INTO d VALUES (1, 1), (2, 2), (3, CAST('1.5' AS DECIMAL))",
    );
    let r = rows(&mut db, "SELECT AVG(v) OVER (ORDER BY k) FROM d");
    let got: Vec<String> = r
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::Float(f) => format!("{f:?}"),
            Value::Decimal(d) => d.to_string(),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(got, vec!["1.0", "1.5", "1.5"], "{r:?}");
    // MIN keeps the FIRST value on ties (batch reduce semantics).
    run(&mut db, "CREATE TABLE m (k INT, v INT)");
    run(&mut db, "INSERT INTO m VALUES (1, 5), (2, 3), (3, 3)");
    let r = rows(&mut db, "SELECT MIN(v) OVER (ORDER BY k) FROM m");
    assert!(matches!(&r.rows[2][0], Value::Int(3)));
    // GROUP_CONCAT: an empty-string element still consumes a separator.
    run(&mut db, "CREATE TABLE g (k INT, v TEXT)");
    run(&mut db, "INSERT INTO g VALUES (1, 'a'), (2, ''), (3, 'b')");
    let r = rows(
        &mut db,
        "SELECT GROUP_CONCAT(v, '-') OVER (ORDER BY k) FROM g",
    );
    assert!(
        matches!(&r.rows[2][0], Value::Str(x) if x == "a--b"),
        "{r:?}"
    );
}

#[test]
fn window_query_respects_statement_deadline() {
    // The window loops sample the cooperative deadline (every 1024
    // ticks); an armed past deadline must abort a windowed SELECT over
    // a few thousand rows instead of running to completion.
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE big (v INT)");
    let mut vals = String::from("INSERT INTO big VALUES (0)");
    for i in 1..3000 {
        vals.push_str(&format!(", ({i})"));
    }
    run(&mut db, &vals);
    db.set_statement_deadline(Some(std::time::Instant::now()));
    let e = db
        .execute("SELECT SUM(v) OVER (ORDER BY v) FROM big")
        .unwrap_err();
    assert!(e.to_string().contains("statement timeout"), "{e}");
    // The deadline is per statement: cleared afterwards the same query
    // runs to completion.
    db.set_statement_deadline(None);
    let r = rows(&mut db, "SELECT SUM(v) OVER (ORDER BY v) FROM big");
    assert_eq!(r.rows.len(), 3000);
}
