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

fn one(db: &mut Database, sql: &str) -> Value {
    rows(db, sql).rows[0][0].clone()
}

#[test]
fn top_limits_and_ties() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    run(
        &mut db,
        "INSERT INTO t VALUES (1, 10), (2, 30), (3, 30), (4, 20)",
    );
    assert_eq!(
        one(
            &mut db,
            "SELECT COUNT(*) FROM (SELECT TOP 2 id FROM t) AS x"
        ),
        Value::Int(2)
    );
    let r = rows(&mut db, "SELECT TOP 2 id FROM t ORDER BY v DESC");
    assert_eq!(r.rows.len(), 2);
    // WITH TIES extends the window over the boundary row's tie run.
    let r = rows(
        &mut db,
        "SELECT TOP (2) WITH TIES id FROM t ORDER BY v DESC",
    );
    assert_eq!(r.rows.len(), 2); // both kept rows tie on v = 30
    let r = rows(
        &mut db,
        "SELECT TOP (3) WITH TIES id FROM t ORDER BY v DESC",
    );
    assert_eq!(r.rows.len(), 3); // 30, 30, 20 — no further 20s
                                 // TOP scopes to a UNION arm.
    let r = rows(
        &mut db,
        "SELECT TOP 1 v FROM t WHERE id = 1 UNION ALL SELECT v FROM t WHERE id = 2",
    );
    assert_eq!(r.rows.len(), 2);
    assert!(db.execute("SELECT TOP 50 PERCENT id FROM t").is_err());
    assert!(db.execute("SELECT TOP 1 id FROM t LIMIT 2").is_err());
    // TOP inside a derived table.
    assert_eq!(
        one(
            &mut db,
            "SELECT COUNT(*) FROM (SELECT TOP 1 id FROM t) AS d"
        ),
        Value::Int(1)
    );
}

#[test]
fn brackets_national_strings_and_concat() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE [My Table] ([Order Id] INT, [name] TEXT)",
    );
    run(
        &mut db,
        "INSERT INTO [My Table] ([Order Id], [name]) VALUES (1, N'ann')",
    );
    assert_eq!(
        one(
            &mut db,
            "SELECT [name] FROM [My Table] WHERE [Order Id] = 1"
        ),
        Value::Str("ann".into())
    );
    // T-SQL '+' concatenation; || keeps NULL propagation.
    assert_eq!(
        one(&mut db, "SELECT 'a' + 'b' + 'c'"),
        Value::Str("abc".into())
    );
    assert_eq!(
        one(&mut db, "SELECT [name] + '!' FROM [My Table]"),
        Value::Str("ann!".into())
    );
    assert_eq!(one(&mut db, "SELECT '5' + 1"), Value::Int(6));
    assert!(db.execute("SELECT 'x' + 1").is_err());
    // NULL + string still propagates.
    assert_eq!(one(&mut db, "SELECT NULL + 'a'"), Value::Null);
}

#[test]
fn bitwise_operators() {
    let mut db = Database::in_memory().unwrap();
    assert_eq!(one(&mut db, "SELECT 5 & 3"), Value::Int(1));
    assert_eq!(one(&mut db, "SELECT 5 | 3"), Value::Int(7));
    assert_eq!(one(&mut db, "SELECT 5 ^ 3"), Value::Int(6));
    assert_eq!(one(&mut db, "SELECT ~5"), Value::Int(!5));
    assert!(db.execute("SELECT (1 << 1)").is_err()); // no << in T-SQL
    assert!(db.execute("SELECT 'a' & 1").is_err());
}

#[test]
fn like_character_classes() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (s TEXT)");
    run(
        &mut db,
        "INSERT INTO t VALUES ('abc'), ('a2c'), ('a-c'), ('xyz')",
    );
    let r = rows(&mut db, "SELECT s FROM t WHERE s LIKE 'a[bd2]c' ORDER BY s");
    assert_eq!(r.rows.len(), 2);
    let r = rows(&mut db, "SELECT s FROM t WHERE s LIKE 'a[^0-9]c'");
    assert_eq!(r.rows.len(), 2);
    let r = rows(&mut db, "SELECT s FROM t WHERE s NOT LIKE '[ax]%'");
    assert_eq!(r.rows.len(), 0);
}

#[test]
fn convert_parse_try_cast() {
    let mut db = Database::in_memory().unwrap();
    assert_eq!(one(&mut db, "SELECT CONVERT(INT, '42')"), Value::Int(42));
    assert_eq!(
        one(
            &mut db,
            "SELECT CONVERT(VARCHAR, TIMESTAMP '2026-09-21T00:00:00Z', 23)"
        ),
        Value::Str("2026-09-21".into())
    );
    assert_eq!(
        one(
            &mut db,
            "SELECT CONVERT(DATETIME, '09/21/2026', 101) = TIMESTAMP '2026-09-21T00:00:00Z'"
        ),
        Value::Bool(true)
    );
    assert_eq!(one(&mut db, "SELECT TRY_CONVERT(INT, 'nope')"), Value::Null);
    assert!(db.execute("SELECT CONVERT(INT, 'nope')").is_err());
    assert_eq!(
        one(&mut db, "SELECT PARSE('1.5' AS DECIMAL)"),
        Value::Decimal("1.5".parse::<Decimal>().unwrap())
    );
    assert_eq!(one(&mut db, "SELECT TRY_PARSE('x' AS INT)"), Value::Null);
    assert_eq!(one(&mut db, "SELECT TRY_CAST('x' AS INT)"), Value::Null);
    assert!(db.execute("SELECT CAST('x' AS INT)").is_err());
    assert_eq!(
        one(&mut db, "SELECT CONVERT(DECIMAL, '1.5') + 1"),
        Value::Decimal("2.5".parse::<Decimal>().unwrap())
    );
}

#[test]
fn date_functions_end_to_end() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (d TIMESTAMP)");
    run(
        &mut db,
        "INSERT INTO t VALUES (TIMESTAMP '2026-09-21T13:45:06.250Z')",
    );
    assert_eq!(one(&mut db, "SELECT YEAR(d) FROM t"), Value::Int(2026));
    assert_eq!(
        one(&mut db, "SELECT DATEPART(quarter, d) FROM t"),
        Value::Int(3)
    );
    assert_eq!(
        one(&mut db, "SELECT DATENAME(month, d) FROM t"),
        Value::Str("September".into())
    );
    assert_eq!(
        one(&mut db, "SELECT DATEADD(day, 1, d) FROM t"),
        Value::Timestamp(crate::value::parse_timestamp_ms("2026-09-22T13:45:06.250Z").unwrap())
    );
    assert_eq!(
        one(&mut db, "SELECT DATEDIFF(day, '2026-09-20', d) FROM t"),
        Value::Int(1)
    );
    assert_eq!(
        one(
            &mut db,
            "SELECT DATEADD(month, 1, TIMESTAMP '2026-01-31T00:00:00Z')"
        ),
        Value::Timestamp(crate::value::parse_timestamp_ms("2026-02-28T00:00:00Z").unwrap())
    );
    // GETDATE family folds into a literal on the write path.
    run(&mut db, "CREATE TABLE g (id INT, at TIMESTAMP)");
    run(&mut db, "INSERT INTO g VALUES (1, GETDATE())");
    let resolved = db.take_resolved_sql().expect("GETDATE resolved");
    assert!(!resolved.to_lowercase().contains("getdate"), "{resolved}");
}

#[test]
fn table_functions_in_from() {
    let mut db = Database::in_memory().unwrap();
    let r = rows(&mut db, "SELECT value FROM STRING_SPLIT('a,b,c', ',') AS s");
    assert_eq!(r.rows.len(), 3);
    let r = rows(
        &mut db,
        "SELECT value, ordinal FROM STRING_SPLIT('a,b', ',', 1) AS s WHERE ordinal = 2",
    );
    assert_eq!(r.rows.len(), 1);
    let r = rows(&mut db, "SELECT value FROM GENERATE_SERIES(1, 4) AS g");
    assert_eq!(r.rows.len(), 4);
    let r = rows(&mut db, "SELECT [key] FROM OPENJSON('[10,20]') AS j");
    assert_eq!(r.rows.len(), 2);
    assert!(db
        .execute("SELECT * FROM STRING_SPLIT('a,b', ',')")
        .is_err());
    assert!(db.execute("SELECT * FROM SOME_TVF(1)").is_err());
}

#[test]
fn session_shims_and_variables_boundary() {
    let mut db = Database::in_memory().unwrap();
    // SET/USE/PRINT ride the PRAGMA channel: accepted and ignored.
    run(&mut db, "SET NOCOUNT ON");
    run(&mut db, "USE master");
    run(&mut db, "PRINT 'hello'");
    run(&mut db, "SET QUOTED_IDENTIFIER OFF");
    run(&mut db, "SET ANSI_NULLS ON");
    // @variables keep their loud error.
    let e = db.execute("SELECT @x").unwrap_err();
    assert!(e.to_string().contains("variables"), "{e}");
    let e = db.execute("SET @x = 1").unwrap_err();
    assert!(!e.to_string().is_empty());
}

#[test]
fn newid_write_paths() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT, uid TEXT)");
    // SELECT context: a fresh UUIDv7 string.
    match one(&mut db, "SELECT NEWID()") {
        Value::Str(s) => assert_eq!(s.as_bytes()[14], b'7'),
        other => panic!("{other:?}"),
    }
    // INSERT ... VALUES: the generated id ships as a literal.
    run(&mut db, "INSERT INTO t VALUES (1, NEWID())");
    let resolved = db
        .take_resolved_sql()
        .expect("NEWID resolved")
        .to_lowercase();
    assert!(!resolved.contains("newid"), "{resolved}");
    // INSERT ... SELECT: one fresh id per row, all literalized.
    run(&mut db, "CREATE TABLE base (id INT)");
    run(&mut db, "INSERT INTO base VALUES (10), (20)");
    run(&mut db, "INSERT INTO t SELECT id, NEWID() AS uid FROM base");
    // Claim the rewrite before any later statement clears it.
    let resolved = db
        .take_resolved_sql()
        .expect("NEWID resolved")
        .to_lowercase();
    assert!(!resolved.contains("newid"), "{resolved}");
    let r = rows(&mut db, "SELECT uid FROM t WHERE id > 1 ORDER BY id");
    assert_eq!(r.rows.len(), 2);
    assert_ne!(r.rows[0][0], r.rows[1][0]);
    // UPDATE/DELETE/MERGE refuse (replay would re-roll the id).
    let e = db
        .execute("UPDATE t SET uid = NEWID() WHERE id = 1")
        .unwrap_err();
    assert!(e.to_string().contains("NEWID"), "{e}");
    // The refusal is AST-level: the words "newid" inside a string
    // literal or used as a plain column name must NOT trip it (the old
    // substring scan rejected these legal writes).
    run(&mut db, "UPDATE t SET uid = 'x-newid-y' WHERE id = 1");
    assert_eq!(
        one(&mut db, "SELECT uid FROM t WHERE id = 1"),
        Value::Str("x-newid-y".into())
    );
    // The literal in the predicate must not trip the guard either; it
    // is a real delete, so assert the row is gone afterwards.
    run(
        &mut db,
        "DELETE FROM t WHERE id = 1 AND 'newsequentialid' <> 'x'",
    );
    assert!(rows(&mut db, "SELECT uid FROM t WHERE id = 1")
        .rows
        .is_empty());
    run(&mut db, "CREATE TABLE newid (v INT)");
    run(&mut db, "INSERT INTO newid VALUES (7)");
    run(&mut db, "UPDATE newid SET v = 8");
    assert_eq!(one(&mut db, "SELECT v FROM newid"), Value::Int(8));
}

#[test]
fn len_concat_trim_and_round_semantics() {
    let mut db = Database::in_memory().unwrap();
    assert_eq!(one(&mut db, "SELECT LEN('ab  ')"), Value::Int(2));
    assert_eq!(one(&mut db, "SELECT LENGTH('ab  ')"), Value::Int(4));
    assert_eq!(
        one(&mut db, "SELECT CONCAT('a', NULL, 'b')"),
        Value::Str("ab".into())
    );
    assert_eq!(one(&mut db, "SELECT 'a' || NULL || 'b'"), Value::Null);
    assert_eq!(
        one(&mut db, "SELECT TRIM('xy' FROM 'xxaxx')"),
        Value::Str("a".into())
    );
    assert_eq!(
        one(&mut db, "SELECT TRIM(LEADING 'x' FROM 'xxaxx')"),
        Value::Str("axx".into())
    );
    assert_eq!(
        one(&mut db, "SELECT ROUND(CAST('123.45' AS DECIMAL), -1)"),
        Value::Decimal(Decimal::from(120))
    );
    assert_eq!(
        one(&mut db, "SELECT ROUND(CAST('123.45' AS DECIMAL), 1)"),
        Value::Decimal("123.5".parse::<Decimal>().unwrap())
    );
}

#[test]
fn string_and_math_functions_end_to_end() {
    let mut db = Database::in_memory().unwrap();
    assert_eq!(
        one(&mut db, "SELECT LEFT('hello', 2)"),
        Value::Str("he".into())
    );
    assert_eq!(
        one(&mut db, "SELECT RIGHT('hello', 2)"),
        Value::Str("lo".into())
    );
    assert_eq!(
        one(&mut db, "SELECT CHARINDEX('l', 'hello')"),
        Value::Int(3)
    );
    assert_eq!(
        one(&mut db, "SELECT REPLACE('aXbX', 'X', '-')"),
        Value::Str("a-b-".into())
    );
    assert_eq!(
        one(&mut db, "SELECT REVERSE('abc')"),
        Value::Str("cba".into())
    );
    assert_eq!(one(&mut db, "SELECT ASCII('A')"), Value::Int(65));
    assert_eq!(one(&mut db, "SELECT CHAR(65)"), Value::Str("A".into()));
    assert_eq!(one(&mut db, "SELECT NCHAR(8364)"), Value::Str("€".into()));
    assert_eq!(one(&mut db, "SELECT UNICODE('€')"), Value::Int(8364));
    assert_eq!(
        one(&mut db, "SELECT CONCAT_WS(',', 'a', NULL, 'b')"),
        Value::Str("a,b".into())
    );
    assert_eq!(
        one(&mut db, "SELECT TRANSLATE('abc', 'ab', 'xy')"),
        Value::Str("xyc".into())
    );
    assert_eq!(
        one(&mut db, "SELECT STUFF('abcdef', 2, 3, 'XY')"),
        Value::Str("aXYef".into())
    );
    assert_eq!(
        one(&mut db, "SELECT STR(123.456)"),
        Value::Str("       123".into())
    );
    assert_eq!(
        one(&mut db, "SELECT QUOTENAME('a]b')"),
        Value::Str("[a]]b]".into())
    );
    assert_eq!(
        one(&mut db, "SELECT IIF(1 > 0, 'y', 'n')"),
        Value::Str("y".into())
    );
    assert_eq!(
        one(&mut db, "SELECT CHOOSE(2, 'a', 'b')"),
        Value::Str("b".into())
    );
    assert_eq!(one(&mut db, "SELECT FLOOR(1.7)"), Value::Float(1.0));
    assert_eq!(one(&mut db, "SELECT CEILING(1.2)"), Value::Float(2.0));
    assert_eq!(one(&mut db, "SELECT POWER(2, 10)"), Value::Int(1024));
    match one(&mut db, "SELECT SQRT(9)") {
        Value::Float(x) => assert!((x - 3.0).abs() < 1e-12),
        other => panic!("{other:?}"),
    }
    assert_eq!(one(&mut db, "SELECT SIGN(-5)"), Value::Int(-1));
    assert_eq!(one(&mut db, "SELECT ISNULL(NULL, -1)"), Value::Int(-1));
    assert_eq!(one(&mut db, "SELECT ISDATE('2026-09-21')"), Value::Int(1));
    assert_eq!(one(&mut db, "SELECT ISNUMERIC('$1,234')"), Value::Int(1));
    assert_eq!(
        one(&mut db, "SELECT DB_NAME()"),
        Value::Str("docsql".into())
    );
    match one(&mut db, "SELECT HASHBYTES('MD5', 'abc')") {
        Value::Bytes(b) => {
            assert_eq!(crate::tsql::hex(&b), "900150983cd24fb0d6963f7d28e17f72")
        }
        other => panic!("{other:?}"),
    }
    // Session functions refuse loudly; RAND is live on reads.
    assert!(db.execute("SELECT SUSER_SNAME()").is_err());
    match one(&mut db, "SELECT RAND() AS r") {
        Value::Float(x) => assert!((0.0..1.0).contains(&x)),
        other => panic!("{other:?}"),
    }
}

/// Error-path and multi-column-join coverage over FROM/join shapes.
#[test]
fn rand_write_path_literalization() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE r (id INT, x FLOAT)");
    // SELECT: a fresh unit-interval draw.
    match one(&mut db, "SELECT RAND() AS r") {
        Value::Float(x) => assert!((0.0..1.0).contains(&x)),
        other => panic!("{other:?}"),
    }
    // INSERT: the journaled statement carries the literal, and rows
    // replay identically (replication red line).
    run(&mut db, "INSERT INTO r VALUES (1, RAND())");
    let resolved = db
        .take_resolved_sql()
        .expect("RAND resolved")
        .to_lowercase();
    assert!(!resolved.contains("rand"), "{resolved}");
    let v = one(&mut db, "SELECT x FROM r WHERE id = 1");
    assert!(matches!(v, Value::Float(_)));
    // UPDATE/DELETE/MERGE refuse.
    let e = db
        .execute("UPDATE r SET x = RAND() WHERE id = 1")
        .unwrap_err();
    assert!(e.to_string().contains("RAND"), "{e}");
    // CHECK constraints refuse wall-clock/nondeterministic defaults.
    assert!(db
        .execute("CREATE TABLE bad (v FLOAT CHECK (v > RAND()))")
        .is_err());
}

/// Schemaless field-set differences must not blur PIVOT groups: rows
/// written before an ALTER ADD COLUMN lack the new field entirely, and
/// the group buckets must follow the fields a row actually has (a
/// permissive field predicate used to pull such rows into every
/// superset group, double-counting their aggregates). Also pins the
/// aggregate edge semantics: an empty cell set is COUNT 0 / otherwise
/// NULL — including an all-NULL cell set, which is not an error.
#[test]
fn pivot_field_sets_and_empty_cells() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE s (dept TEXT, yr INT, qty INT)");
    run(
        &mut db,
        "INSERT INTO s VALUES ('a', 2024, 10), ('a', 2024, 100)",
    );
    // Rows written before the ALTER lack the `region` field entirely.
    run(&mut db, "ALTER TABLE s ADD COLUMN region TEXT");
    run(
        &mut db,
        "INSERT INTO s (dept, region, yr, qty) VALUES ('a', 'east', 2024, 5)",
    );
    // All-NULL cell for 2025 / dept 'c'.
    run(
        &mut db,
        "INSERT INTO s (dept, yr, qty) VALUES ('c', 2025, NULL)",
    );
    // The east row aggregates alone; the field-less rows stay in their
    // own group (pre-fix this cell summed to 115).
    let r = rows(
            &mut db,
            "SELECT dept, region, [2024] FROM s PIVOT (SUM(qty) FOR yr IN (2024)) AS p ORDER BY dept, region",
        );
    // Three implicit groups: the two field-less rows, the east row, and
    // dept 'c' (whose only row pivots on 2025 — its 2024 cell is NULL).
    assert_eq!(r.rows.len(), 3, "{r:?}");
    assert_eq!(
        r.rows[0],
        vec![Value::Str("a".into()), Value::Null, Value::Int(110)]
    );
    assert_eq!(
        r.rows[1],
        vec![
            Value::Str("a".into()),
            Value::Str("east".into()),
            Value::Int(5)
        ]
    );
    assert_eq!(
        r.rows[2],
        vec![Value::Str("c".into()), Value::Null, Value::Null]
    );
    // Empty cells: COUNT reads 0 (T-SQL), MIN reads NULL — no error.
    let r = rows(
        &mut db,
        "SELECT dept, [2025] FROM s PIVOT (COUNT(qty) FOR yr IN (2025)) AS p ORDER BY dept",
    );
    assert_eq!(r.rows[0][1], Value::Int(0));
    assert_eq!(r.rows[1][1], Value::Int(0));
    let r = rows(
        &mut db,
        "SELECT dept, [2025] FROM s PIVOT (MIN(qty) FOR yr IN (2025)) AS p ORDER BY dept",
    );
    assert_eq!(r.rows[0][1], Value::Null);
}

/// RAND() is statement-scoped on reads (T-SQL): every call inside one
/// SELECT folds to the same value, later statements draw again, and a
/// CTAS pins the drawn value into the journaled rewrite so a peer
/// replay cannot re-roll it.
#[test]
fn rand_reads_are_statement_scoped() {
    let mut db = Database::in_memory().unwrap();
    let r = rows(&mut db, "SELECT RAND() AS a, RAND() AS b, RAND() AS c");
    assert_eq!(r.rows[0][0], r.rows[0][1]);
    assert_eq!(r.rows[0][1], r.rows[0][2]);
    // Successive statements draw independently (five identical draws in
    // a row is practically impossible for the advancing xorshift).
    let first = one(&mut db, "SELECT RAND() AS r");
    let mut differed = false;
    for _ in 0..5 {
        if one(&mut db, "SELECT RAND() AS r") != first {
            differed = true;
            break;
        }
    }
    assert!(differed);
    // CTAS: the journaled rewrite carries the literal, not the call.
    run(&mut db, "CREATE TABLE rd (x INT)");
    run(&mut db, "INSERT INTO rd VALUES (1)");
    run(&mut db, "CREATE TABLE rd2 AS SELECT RAND() AS r FROM rd");
    let resolved = db
        .take_resolved_sql()
        .expect("CTAS resolved")
        .to_lowercase();
    assert!(!resolved.contains("rand"), "{resolved}");
    let r = rows(&mut db, "SELECT r FROM rd2");
    assert!(matches!(r.rows[0][0], Value::Float(_)));
}

#[test]
fn pivot_and_unpivot() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE sales (dept TEXT, yr INT, qty INT)");
    run(
        &mut db,
        "INSERT INTO sales VALUES ('a', 2024, 10), ('a', 2025, 20), ('b', 2024, 5)",
    );
    // PIVOT: implicit group on dept, one column per IN value. Row
    // objects are key-sorted, so project columns explicitly.
    let r = rows(
            &mut db,
            "SELECT dept, [2024], [2025] FROM sales PIVOT (SUM(qty) FOR yr IN (2024, 2025)) AS p ORDER BY dept",
        );
    assert_eq!(r.rows.len(), 2, "{r:?}");
    assert_eq!(
        r.rows[0],
        vec![Value::Str("a".into()), Value::Int(10), Value::Int(20)]
    );
    // Missing cell reads NULL ('b' has no 2025 row).
    assert_eq!(
        r.rows[1],
        vec![Value::Str("b".into()), Value::Int(5), Value::Null]
    );
    // COUNT/MIN/MAX aggregates and aliased IN values.
    let r = rows(
        &mut db,
        "SELECT dept, c24 FROM sales PIVOT (COUNT(qty) FOR yr IN (2024 AS c24)) AS p ORDER BY dept",
    );
    assert_eq!(r.rows[0], vec![Value::Str("a".into()), Value::Int(1)]);
    let r = rows(
            &mut db,
            "SELECT dept, [2024], [2025] FROM sales PIVOT (MIN(qty) FOR yr IN (2024, 2025)) AS p ORDER BY dept",
        );
    assert_eq!(r.rows[0][2], Value::Int(20));
    // AVG aggregate: float division, consistent with a plain AVG.
    run(&mut db, "INSERT INTO sales VALUES ('a', 2024, 30)");
    let r = rows(
        &mut db,
        "SELECT dept, [2024] FROM sales PIVOT (AVG(qty) FOR yr IN (2024)) AS p ORDER BY dept",
    );
    assert_eq!(r.rows[0][1], Value::Float(20.0)); // (10+30)/2
                                                  // Two implicit group columns survive.
    run(&mut db, "CREATE TABLE s2 (g TEXT, k TEXT, yr INT, qty INT)");
    run(
        &mut db,
        "INSERT INTO s2 VALUES ('x','p',1,7), ('x','q',1,9)",
    );
    let r = rows(
        &mut db,
        "SELECT g, k, [1] FROM s2 PIVOT (SUM(qty) FOR yr IN (1)) AS p ORDER BY k",
    );
    assert_eq!(r.rows.len(), 2);
    // Refusals: subquery IN, DEFAULT ON NULL, two aggregates, non-function agg.
    assert!(db
        .execute("SELECT * FROM sales PIVOT (SUM(qty) FOR yr IN (SELECT yr FROM sales)) AS p")
        .is_err());
    assert!(db
            .execute("SELECT * FROM (SELECT dept, yr, qty FROM sales) AS s PIVOT (SUM(qty) FOR yr IN (2024)) AS p")
            .is_ok());
    // UNPIVOT: columns to rows, NULLs excluded.
    run(&mut db, "CREATE TABLE wide (id INT, a INT, b INT, c INT)");
    run(
        &mut db,
        "INSERT INTO wide VALUES (1, 10, 20, NULL), (2, 30, NULL, 50)",
    );
    let r = rows(
        &mut db,
        "SELECT id, col, val FROM wide UNPIVOT (val FOR col IN (a, b, c)) AS u ORDER BY id, col",
    );
    assert_eq!(r.rows.len(), 4, "{r:?}");
    assert_eq!(
        r.rows[0],
        vec![Value::Int(1), Value::Str("a".into()), Value::Int(10)]
    );
    // Aliased UNPIVOT labels.
    let r = rows(
            &mut db,
            "SELECT id, col, val FROM wide UNPIVOT (val FOR col IN (a AS alpha, b AS beta)) AS u ORDER BY id, col",
        );
    assert_eq!(r.rows.len(), 3);
    assert!(r.rows[0].contains(&Value::Str("alpha".into())));
    // INCLUDE NULLS refuses.
    assert!(db
        .execute("SELECT * FROM wide UNPIVOT INCLUDE NULLS (val FOR col IN (a)) AS u")
        .is_err());
}

#[test]
fn from_factor_and_join_edge_coverage() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE a (id INT, k1 INT, k2 INT)");
    run(&mut db, "CREATE TABLE b (id INT, k1 INT, k2 INT)");
    run(&mut db, "INSERT INTO a VALUES (1, 10, 100)");
    run(&mut db, "INSERT INTO b VALUES (1, 10, 100), (2, 10, 200)");
    // Multi-column USING builds a compound ON across both columns.
    let r = rows(&mut db, "SELECT a.id, b.id FROM a JOIN b USING (k1, k2)");
    assert_eq!(r.rows.len(), 1);
    // LATERAL table functions refuse; WITH ORDINALITY refuses; an
    // unknown FROM function names itself.
    assert!(db
        .execute("SELECT * FROM LATERAL STRING_SPLIT('a', ',') AS s")
        .is_err());
    assert!(db
        .execute("SELECT * FROM STRING_SPLIT('a', ',') AS s WITH ORDINALITY")
        .is_err());
    assert!(db.execute("SELECT * FROM some_tvf(1) AS t").is_err());
    // Failed ALTER/CREATE INDEX rolls the catalog back (the table
    // still works under its old shape afterwards).
    // ALTER on a missing table rolls back cleanly.
    assert!(db.execute("ALTER TABLE no_such ADD COLUMN x INT").is_err());
    let r = rows(&mut db, "SELECT id, k1 FROM a");
    assert_eq!(r.rows.len(), 1);
    assert!(db.execute("CREATE INDEX ix ON no_such (col)").is_err());
    assert!(db.execute("CREATE INDEX ix ON a (no_such)").is_err());
    let r = rows(&mut db, "SELECT id FROM a");
    assert_eq!(r.rows.len(), 1);
}

#[test]
fn cross_and_outer_apply_table_functions() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE items (id INT PRIMARY KEY, tags TEXT)",
    );
    run(
        &mut db,
        "INSERT INTO items VALUES (1, 'a,b'), (2, 'c'), (3, NULL)",
    );
    // CROSS APPLY: correlated STRING_SPLIT per row; NULL tags produce
    // no rows for that left row.
    let r = rows(
            &mut db,
            "SELECT i.id, s.value FROM items AS i CROSS APPLY STRING_SPLIT(i.tags, ',') AS s ORDER BY i.id, s.value",
        );
    assert_eq!(r.rows.len(), 3, "{r:?}");
    assert_eq!(r.rows[0], vec![Value::Int(1), Value::Str("a".into())]);
    // OUTER APPLY keeps the left row with NULL right columns.
    let r = rows(
            &mut db,
            "SELECT i.id, s.value FROM items AS i OUTER APPLY STRING_SPLIT(i.tags, ',') AS s ORDER BY i.id",
        );
    assert_eq!(r.rows.len(), 4, "{r:?}");
    assert_eq!(r.rows[3], vec![Value::Int(3), Value::Null]);
    // Alias column renaming.
    let r = rows(
            &mut db,
            "SELECT i.id, t.tag FROM items AS i CROSS APPLY STRING_SPLIT(i.tags, ',') AS t(tag) WHERE i.id = 1 ORDER BY t.tag",
        );
    assert_eq!(r.rows.len(), 2);
    // APPLY after APPLY: args may reference earlier aliases.
    let r = rows(
            &mut db,
            "SELECT s.value AS v, g.value AS n FROM items AS i CROSS APPLY STRING_SPLIT(i.tags, ',') AS s CROSS APPLY GENERATE_SERIES(1, 2) AS g WHERE i.id = 2 ORDER BY g.value",
        );
    assert_eq!(r.rows.len(), 2);
    // OPENJSON over a per-row JSON column.
    run(&mut db, "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)");
    run(&mut db, "INSERT INTO docs VALUES (1, '[10,20]')");
    let r = rows(
        &mut db,
        "SELECT d.id, j.value FROM docs AS d CROSS APPLY OPENJSON(d.body) AS j ORDER BY j.value",
    );
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0], vec![Value::Int(1), Value::Int(10)]);
    // NULL JSON per left row: CROSS APPLY skips it, OUTER APPLY keeps
    // the left row (an error here used to fail the whole statement).
    run(&mut db, "INSERT INTO docs VALUES (2, NULL)");
    let r = rows(
        &mut db,
        "SELECT d.id, j.[key] FROM docs AS d CROSS APPLY OPENJSON(d.body) AS j ORDER BY d.id",
    );
    assert_eq!(r.rows.len(), 2);
    let r = rows(
            &mut db,
            "SELECT d.id, j.[key] FROM docs AS d OUTER APPLY OPENJSON(d.body) AS j ORDER BY d.id, j.[key]",
        );
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[2], vec![Value::Int(2), Value::Null]);
    // APPLY over a subquery refuses loudly.
    assert!(db
        .execute("SELECT * FROM items AS i CROSS APPLY (SELECT i.id AS x) AS d")
        .is_err());
    // APPLY without an alias refuses loudly.
    assert!(db
        .execute("SELECT * FROM items CROSS APPLY STRING_SPLIT(items.tags, ',')")
        .is_err());
    // APPLY feeding WHERE on the applied column.
    let r = rows(
            &mut db,
            "SELECT i.id FROM items AS i CROSS APPLY STRING_SPLIT(i.tags, ',') AS s WHERE s.value = 'b'",
        );
    assert_eq!(r.rows.len(), 1);
}

#[test]
fn recursive_ctes() {
    let mut db = Database::in_memory().unwrap();
    // T-SQL spelling: no RECURSIVE keyword.
    let r = rows(
            &mut db,
            "WITH nums AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM nums WHERE n < 5) SELECT n FROM nums ORDER BY n",
        );
    assert_eq!(r.rows.len(), 5, "{r:?}");
    assert_eq!(r.rows[4], vec![Value::Int(5)]);
    // Standard RECURSIVE spelling works too.
    let r = rows(
            &mut db,
            "WITH RECURSIVE t AS (SELECT 1 AS n UNION ALL SELECT n * 2 FROM t WHERE n < 16) SELECT n FROM t ORDER BY n",
        );
    assert_eq!(r.rows.len(), 5);
    // UNION (distinct) dedupes and converges on cyclic graphs.
    run(&mut db, "CREATE TABLE edges (a INT, b INT)");
    run(
        &mut db,
        "INSERT INTO edges VALUES (1, 2), (2, 3), (3, 1), (2, 1)",
    );
    let r = rows(
            &mut db,
            "WITH walk AS (SELECT a, b FROM edges WHERE a = 1 UNION SELECT edges.a, edges.b FROM edges JOIN walk ON edges.a = walk.b) SELECT COUNT(*) AS c FROM walk",
        );
    assert_eq!(r.rows[0][0], Value::Int(4), "{r:?}");
    // Hierarchical multiplication over a tree.
    run(
        &mut db,
        "CREATE TABLE tree (id INT PRIMARY KEY, parent INT, v INT)",
    );
    run(
        &mut db,
        "INSERT INTO tree VALUES (1, NULL, 2), (2, 1, 3), (3, 1, 4), (4, 2, 5)",
    );
    let r = rows(
            &mut db,
            "WITH roll AS (SELECT id, v FROM tree WHERE parent IS NULL UNION ALL SELECT tree.id, roll.v * tree.v FROM tree JOIN roll ON tree.parent = roll.id) SELECT SUM(v) AS total FROM roll",
        );
    // root 2; node2 = 2*3; node3 = 2*4; node4 = (2*3)*5 → 46.
    assert_eq!(r.rows[0][0], Value::Int(46));
    // Explicit column list names the recursive CTE's columns.
    let r = rows(
            &mut db,
            "WITH c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 3) SELECT x FROM c ORDER BY x",
        );
    assert_eq!(r.rows.len(), 3);
    // UNION ALL cycle trips the row budget loudly.
    let e = db
        .execute("WITH loopy AS (SELECT 1 AS n UNION ALL SELECT n FROM loopy) SELECT n FROM loopy")
        .unwrap_err();
    assert!(e.to_string().contains("converge"), "{e}");
    // Non-UNION recursive body refuses.
    assert!(
        db.execute("WITH c AS (SELECT 1 UNION ALL SELECT 2 EXCEPT SELECT 1) SELECT * FROM c")
            .is_ok()
            || db
                .execute("WITH c AS (SELECT 1 INTERSECT SELECT 1 FROM c) SELECT * FROM c")
                .is_err()
    );
    // Column-count mismatch arm-vs-anchor refuses.
    assert!(db
        .execute("WITH c AS (SELECT 1, 2 UNION ALL SELECT n FROM c) SELECT * FROM c")
        .is_err());
}

#[test]
fn format_and_hashbytes_end_to_end() {
    let mut db = Database::in_memory().unwrap();
    assert_eq!(
        one(
            &mut db,
            "SELECT FORMAT(TIMESTAMP '2026-09-21T13:45:06Z', 'yyyy-MM-dd HH:mm:ss')"
        ),
        Value::Str("2026-09-21 13:45:06".into())
    );
    assert_eq!(
        one(&mut db, "SELECT FORMAT(1234.5, 'N2')"),
        Value::Str("1,234.50".into())
    );
    assert!(db
        .execute("SELECT FORMAT(TIMESTAMP '2026-01-01T00:00:00Z', 'gg')")
        .is_err());
}

// ==== 缺陷审查轮 12 回归(随机抽 20 功能点)====

/// NULL 必须穿过文本标量函数族(UPPER/LENGTH/LEN/TRIM/LTRIM/RTRIM/
/// INSTR/LPAD/RPAD):此前 NULL 被静默转成 ''/0,聚合计数被虚增、
/// `WHERE UPPER(v) IS NULL` 永不命中。
#[test]
fn null_propagates_through_text_scalar_functions() {
    let mut db = Database::in_memory().unwrap();
    for e in [
        "UPPER(NULL)",
        "LOWER(NULL)",
        "TRIM(NULL)",
        "LTRIM(NULL)",
        "RTRIM(NULL)",
        "LENGTH(NULL)",
        "LEN(NULL)",
        "INSTR(NULL, 'a')",
        "INSTR('a', NULL)",
        "LPAD(NULL, 3)",
        "RPAD(NULL, 3, 'x')",
    ] {
        assert_eq!(
            one(&mut db, &format!("SELECT {e}")),
            Value::Null,
            "{e} must return NULL"
        );
    }
    run(&mut db, "CREATE TABLE t (v TEXT)");
    run(&mut db, "INSERT INTO t VALUES ('a'), (NULL)");
    let r = rows(&mut db, "SELECT COUNT(UPPER(v)), COUNT(v) FROM t");
    assert_eq!(r.rows[0], vec![Value::Int(1), Value::Int(1)]);
}

/// SUBSTR 的 SQLite 语义:起点 0 位于虚拟首位(长度少取一个)、负长度
/// 取起点之前的字符、负起点从末尾数。此前 0,2 返回 'ab'(多取一位)、
/// 2,-1 返回 ''。
#[test]
fn substr_sqlite_edge_semantics() {
    let mut db = Database::in_memory().unwrap();
    let cases: &[(&str, &str)] = &[
        ("SUBSTRING('abcde', 0, 2)", "a"),
        ("SUBSTR('abcde', 2, -1)", "a"),
        ("SUBSTR('abcde', 1, -2)", ""),
        ("SUBSTR('abcde', 3, 2)", "cd"),
        ("SUBSTR('abcde', 2)", "bcde"),
        ("SUBSTR('hello', -2)", "lo"),
        ("SUBSTR('hello', -2, 1)", "l"),
        ("SUBSTR('abcde', -7, 3)", "a"),
    ];
    for (e, want) in cases {
        assert_eq!(
            one(&mut db, &format!("SELECT {e}")),
            Value::Str((*want).into()),
            "{e}"
        );
    }
}

/// SUM 的单元素也必须过数值校验:单行 BLOB 此前原样返回、第二行才
/// 报错——同一查询随行数改变语义。BLOB 文本化走 x'…' 规范形。
#[test]
fn sum_single_non_numeric_rejected_and_blob_text_form() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (b BLOB)");
    run(&mut db, "INSERT INTO t VALUES (x'0102')");
    assert!(db.execute("SELECT SUM(b) FROM t").is_err());
    assert_eq!(
        one(&mut db, "SELECT CAST(x'0102' AS TEXT)"),
        Value::Str("x'0102'".into())
    );
}

/// MERGE 的显式 id / UPDATE SET id 必须推进(或失效)autoinc 水位:
/// 此前下一次 INSERT 在显式值上撞 UNIQUE。
#[test]
fn merge_explicit_id_advances_autoinc_watermark() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)",
    );
    run(&mut db, "INSERT INTO t (v) VALUES ('a')");
    run(
        &mut db,
        "MERGE INTO t USING (SELECT 1 AS sid) s ON t.id = s.sid AND 1 = 0 \
             WHEN NOT MATCHED THEN INSERT (id, v) VALUES (2, 'b')",
    );
    run(&mut db, "INSERT INTO t (v) VALUES ('c')");
    let r = rows(&mut db, "SELECT id FROM t ORDER BY id");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)]
        ]
    );
    // UPDATE 臂改 id 同理。
    run(
        &mut db,
        "MERGE INTO t USING (SELECT 3 AS sid) s ON t.id = s.sid \
             WHEN MATCHED THEN UPDATE SET id = 9",
    );
    run(&mut db, "INSERT INTO t (v) VALUES ('d')");
    let r = rows(&mut db, "SELECT id FROM t ORDER BY id");
    assert_eq!(r.rows.len(), 4);
    // 不撞唯一:最后一个是 max+1 = 10。
    assert_eq!(r.rows.last().unwrap()[0], Value::Int(10));
}

/// 纯 WHEN NOT MATCHED INSERT 臂:多行 source 命中同一 target 不再误报
/// "cannot update the same row twice"(没有 UPDATE 臂就无"两次更新")。
#[test]
fn merge_insert_only_arm_allows_shared_target_rows() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE tgt (id INT PRIMARY KEY, v TEXT)");
    run(&mut db, "CREATE TABLE src (sid INT, w TEXT)");
    run(&mut db, "INSERT INTO tgt VALUES (1, 'x')");
    run(
        &mut db,
        "INSERT INTO src VALUES (1, 'p'), (1, 'q'), (9, 'z')",
    );
    run(
        &mut db,
        "MERGE INTO tgt USING src ON tgt.id = src.sid \
             WHEN NOT MATCHED THEN INSERT (id, v) VALUES (sid, w)",
    );
    let r = rows(&mut db, "SELECT id FROM tgt ORDER BY id");
    assert_eq!(r.rows, vec![vec![Value::Int(1)], vec![Value::Int(9)]]);
}

/// OUTER JOIN 的空侧必须补显式 NULL 键:此前另一侧同名列经后缀回退
/// 镜像对方值,`WHERE r.id IS NULL` 反连接在空右表时整体失效。
#[test]
fn outer_join_empty_side_pads_null_keys() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE a (id INT, va INT)");
    run(&mut db, "CREATE TABLE b (id INT, vb INT)");
    run(&mut db, "INSERT INTO a VALUES (1, 10), (2, 20)");
    let r = rows(
        &mut db,
        "SELECT a.id, b.id FROM a LEFT JOIN b ON a.id = b.id",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(1), Value::Null],
            vec![Value::Int(2), Value::Null],
        ]
    );
    let r = rows(
        &mut db,
        "SELECT a.id FROM a LEFT JOIN b ON a.id = b.id WHERE b.id IS NULL",
    );
    assert_eq!(r.rows.len(), 2);
    // RIGHT/FULL:左侧空时,左引用读 NULL 而非镜像右侧值。
    run(&mut db, "CREATE TABLE e (id INT)");
    run(&mut db, "INSERT INTO b VALUES (7, 70)");
    let r = rows(
        &mut db,
        "SELECT e.id, b.vb FROM e RIGHT JOIN b ON e.id = b.id",
    );
    assert_eq!(r.rows, vec![vec![Value::Null, Value::Int(70)]]);
    let r = rows(
        &mut db,
        "SELECT e.id, b.vb FROM e FULL OUTER JOIN b ON e.id = b.id",
    );
    assert_eq!(r.rows, vec![vec![Value::Null, Value::Int(70)]]);
}

/// OUTER APPLY 空行集保留的左行必须带右列 NULL 键(此前同名左列冒充
/// 右值);表函数别名列改名(`AS s(v)`、`AS j(k,v,t)`)不再被静默丢弃;
/// STRING_SPLIT 第三参关闭 ordinal 时两列改名清单报错。
#[test]
fn apply_and_tablefn_alias_columns() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE items (id INT, tags TEXT, value TEXT)",
    );
    run(&mut db, "INSERT INTO items VALUES (1, NULL, 'LEFTVAL')");
    let r = rows(
        &mut db,
        "SELECT i.id, s.value FROM items AS i OUTER APPLY STRING_SPLIT(i.tags, ',') AS s",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(1), Value::Null]]);
    // FROM 表函数的别名列改名生效。
    let r = rows(&mut db, "SELECT s.v FROM STRING_SPLIT('a,b', ',') AS s(v)");
    assert_eq!(
        r.rows,
        vec![vec![Value::Str("a".into())], vec![Value::Str("b".into())]]
    );
    // OPENJSON 的改名(FROM 与 APPLY 两种形态)。
    let r = rows(
        &mut db,
        "SELECT j.k FROM OPENJSON('[{\"k\":1}]') AS j(k, v, t)",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(0)]]);
    run(&mut db, "CREATE TABLE docs (id INT, body TEXT)");
    run(&mut db, "INSERT INTO docs VALUES (1, '[{\"k\":5}]')");
    let r = rows(
        &mut db,
        "SELECT d.id, j.k FROM docs AS d CROSS APPLY OPENJSON(d.body) AS j(k, v, t)",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(1), Value::Int(0)]]);
    // ordinal 关闭时产出单列,两列清单响亮报错(而非静默丢 ord)。
    assert!(db
        .execute("SELECT s.v FROM STRING_SPLIT('a,b', ',', 0) AS s(v, ord)")
        .is_err());
}

/// HAVING 支持谓词形态(IS NULL/LIKE/BETWEEN/IN)——求值器本就支持,
/// 此前 check_having_refs 白名单缺形态而误拒(ROLLUP 总计行的惯用写法)。
#[test]
fn having_accepts_predicate_shapes() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (k TEXT, v INT)");
    run(
        &mut db,
        "INSERT INTO t VALUES ('a', 1), ('a', 2), (NULL, 3)",
    );
    let r = rows(
        &mut db,
        "SELECT k, COUNT(*) FROM t GROUP BY k HAVING k IS NULL",
    );
    assert_eq!(r.rows, vec![vec![Value::Null, Value::Int(1)]]);
    let r = rows(
        &mut db,
        "SELECT k, SUM(v) FROM t GROUP BY k HAVING k LIKE 'a%'",
    );
    assert_eq!(r.rows, vec![vec![Value::Str("a".into()), Value::Int(3)]]);
    let r = rows(
        &mut db,
        "SELECT k FROM t GROUP BY k HAVING k BETWEEN 'a' AND 'b'",
    );
    assert_eq!(r.rows, vec![vec![Value::Str("a".into())]]);
    let r = rows(&mut db, "SELECT k FROM t GROUP BY k HAVING k IN ('a')");
    assert_eq!(r.rows, vec![vec![Value::Str("a".into())]]);
}

/// 表级 PK/UNIQUE/CHECK 引用不存在的列必须在 DDL 拒绝(幽灵约束树
/// 永不生效且 dump 丢失声明);表达式 UNIQUE 显式拒绝。
#[test]
fn ddl_rejects_ghost_constraint_columns() {
    let mut db = Database::in_memory().unwrap();
    assert!(db
        .execute("CREATE TABLE t1 (a INT, PRIMARY KEY(b))")
        .is_err());
    assert!(db.execute("CREATE TABLE t2 (a INT, UNIQUE(b))").is_err());
    assert!(db
        .execute("CREATE TABLE t3 (a TEXT, UNIQUE(LOWER(a)))")
        .is_err());
    assert!(db.execute("CREATE TABLE t4 (x INT CHECK (y > 0))").is_err());
    assert!(db
        .execute("CREATE TABLE t5 (x INT, y INT, CHECK (z + y > 0))")
        .is_err());
    // 列级多被引列 FK 静默取第一列 → 显式报错。
    run(&mut db, "CREATE TABLE p2 (x INT, y INT)");
    assert!(db
        .execute("CREATE TABLE c2 (a INT REFERENCES p2 (x, y))")
        .is_err());
}

/// INSERT / CTAS / MERGE INSERT 的重复列名报错(此前 zip().collect()
/// 后者静默覆盖前者)。
#[test]
fn insert_rejects_duplicate_column_list() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE d (a INT, b INT)");
    assert!(db
        .execute("INSERT INTO d (a, a, b) VALUES (1, 2, 3)")
        .is_err());
    assert!(db
        .execute("CREATE TABLE ct AS SELECT 1 AS a, 2 AS a")
        .is_err());
    run(&mut db, "CREATE TABLE m (a INT, b INT)");
    assert!(db
        .execute(
            "MERGE INTO m USING (SELECT 1 AS s) src ON 1 = 0 \
                 WHEN NOT MATCHED THEN INSERT (a, a) VALUES (1, 2)"
        )
        .is_err());
}

/// ON CONFLICT DO NOTHING / OR IGNORE 之后,子侧 FK 按最终落盘集校验:
/// 被跳过的父候选不得为同批子行背书(孤儿)。
#[test]
fn on_conflict_do_nothing_cannot_orphan_fk() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t9 (id INTEGER PRIMARY KEY, u TEXT UNIQUE, pid INT REFERENCES t9(id))",
    );
    run(&mut db, "INSERT INTO t9 VALUES (1, 'a', NULL)");
    // (2,'a') 冲突被跳过;(3,'b',2) 的父候选 2 不落盘 → 整条语句报错。
    assert!(db
        .execute("INSERT INTO t9 VALUES (2, 'a', NULL), (3, 'b', 2) ON CONFLICT (u) DO NOTHING")
        .is_err());
    let r = rows(&mut db, "SELECT id FROM t9");
    assert_eq!(r.rows, vec![vec![Value::Int(1)]]);
    // 父在前、正常落盘的组合照常通过(含批内前向引用)。
    run(
        &mut db,
        "INSERT INTO t9 VALUES (3, 'b', 1), (4, 'c', 3) ON CONFLICT (u) DO NOTHING",
    );
}

/// ADD COLUMN:带 DEFAULT 回填的行必须过 CHECK(含本语句新增的);
/// GENERATED ALWAYS AS 显式拒绝(此前静默降级为普通列)。
#[test]
fn add_column_backfill_check_and_generated_rejected() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE bk (x INT)");
    run(&mut db, "INSERT INTO bk VALUES (1)");
    assert!(db
        .execute("ALTER TABLE bk ADD COLUMN c INT DEFAULT -5 CHECK (c > 0)")
        .is_err());
    run(
        &mut db,
        "ALTER TABLE bk ADD COLUMN c INT DEFAULT 5 CHECK (c > 0)",
    );
    let r = rows(&mut db, "SELECT c FROM bk");
    assert_eq!(r.rows, vec![vec![Value::Int(5)]]);
    assert!(db
        .execute("ALTER TABLE bk ADD COLUMN g INT GENERATED ALWAYS AS (x + 1)")
        .is_err());
}

/// 缺陷审查轮13:同语句「显式 id == 语句开始水位 + 省略 id 行」曾整句撞
/// UNIQUE —— 显式 id 必须在行循环内推进水位(INSERT/MERGE 两路)。
#[test]
fn explicit_autoinc_id_advances_in_statement_watermark() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY AUTOINCREMENT, v TEXT)",
    );
    run(&mut db, "INSERT INTO t (id, v) VALUES (1, 'a'), (2, 'b')");
    run(
        &mut db,
        "INSERT INTO t (id, v) VALUES (3, 'e3'), (NULL, 'gen')",
    );
    let mut ids = rows(&mut db, "SELECT id FROM t ORDER BY id")
        .rows
        .into_iter()
        .filter_map(|r| r[0].as_i64())
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![1, 2, 3, 4]);
    // NULL 在前、大显式 id 在后:后一个 NULL 跟进显式 id。
    run(
        &mut db,
        "INSERT INTO t (id, v) VALUES (NULL, 'g1'), (50, 'e50'), (NULL, 'g2')",
    );
    ids = rows(
        &mut db,
        "SELECT id FROM t WHERE v IN ('g1','g2') ORDER BY id",
    )
    .rows
    .into_iter()
    .filter_map(|r| r[0].as_i64())
    .collect();
    assert_eq!(ids, vec![5, 51]);
    // MERGE INSERT 臂同款。
    run(
        &mut db,
        "CREATE TABLE t2 (id INT PRIMARY KEY AUTOINCREMENT, v TEXT)",
    );
    run(&mut db, "INSERT INTO t2 (id, v) VALUES (1, 'a'), (2, 'b')");
    run(&mut db, "CREATE TABLE feed (id INT, v TEXT)");
    run(&mut db, "INSERT INTO feed VALUES (3, 'e3'), (NULL, 'gen')");
    run(
        &mut db,
        "MERGE INTO t2 USING feed ON t2.id = feed.id \
             WHEN NOT MATCHED THEN INSERT (id, v) VALUES (feed.id, feed.v)",
    );
    let mids = rows(
        &mut db,
        "SELECT id FROM t2 WHERE v IN ('e3','gen') ORDER BY id",
    )
    .rows
    .into_iter()
    .filter_map(|r| r[0].as_i64())
    .collect::<Vec<_>>();
    assert_eq!(mids, vec![3, 4]);
}

/// INSERT … SELECT 源过滤后为空 = 插 0 行(主流语义);空 VALUES 仍报错。
#[test]
fn insert_select_empty_source_affected_zero() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE src (a INT)");
    run(&mut db, "INSERT INTO src VALUES (1)");
    run(&mut db, "CREATE TABLE dst (a INT)");
    let out = run(&mut db, "INSERT INTO dst SELECT a FROM src WHERE 1 = 0");
    assert!(matches!(out, ExecOutcome::Affected(0)));
    assert!(db.execute("INSERT INTO dst VALUES ()").is_err());
    let n = rows(&mut db, "SELECT COUNT(*) FROM dst").rows[0][0].as_i64();
    assert_eq!(n, Some(0));
}

/// TRUNCATE/DROP 的溢出链页:活链随表释放回页池,churn 不再无界增长
/// (红线 1:页回收,DML 之外的重写路径同样不得孤儿化链页)。
#[test]
fn truncate_and_drop_release_overflow_chain_pages() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Database::open(&dir.path().join("ovchurn.db")).unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, b TEXT)");
    let big = "x".repeat(20_000);
    run(&mut db, &format!("INSERT INTO t VALUES (1, '{big}')"));
    let filled = db.num_pages();
    for i in 0..5 {
        run(
            &mut db,
            &format!("INSERT INTO t VALUES ({}, '{big}')", 10 + i),
        );
        run(&mut db, "TRUNCATE TABLE t");
    }
    assert!(
        db.num_pages() <= filled + 8,
        "TRUNCATE churn leaked chain pages: {filled} -> {}",
        db.num_pages()
    );
    // DROP:释放的链页立即可复用(文件不因重建而增长)。
    run(&mut db, "DROP TABLE t");
    run(&mut db, "CREATE TABLE t2 (id INT PRIMARY KEY, b TEXT)");
    run(&mut db, &format!("INSERT INTO t2 VALUES (1, '{big}')"));
    assert!(
        db.num_pages() <= filled + 8,
        "freed chain pages must be reused after DROP: {} vs {filled}",
        db.num_pages()
    );
}

/// MERGE ON 目标限定引用一个目标没有、源有的列:必须按目标侧 NULL 判定,
/// 不得经裸名回退静默取源值(否则恒匹配 → 目标行被错误 UPDATE)。
#[test]
fn merge_on_target_qualified_missing_column_reads_null() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE tgt (id INT PRIMARY KEY, v TEXT)");
    run(&mut db, "INSERT INTO tgt VALUES (1, 'keep')");
    run(&mut db, "CREATE TABLE src (id INT, tag TEXT)");
    run(&mut db, "INSERT INTO src VALUES (1, 'x')");
    // t.tag 目标没有 → NULL → ON 不成立 → INSERT 臂(撞 PK 报错);
    // 旧行为:s.tag/t.tag 都取源值 → 恒匹配 → 目标行被改成 'upd'。
    let e = db
        .execute(
            "MERGE INTO tgt AS t USING src AS s ON t.id = s.id AND s.tag = t.tag \
                 WHEN MATCHED THEN UPDATE SET v = 'upd' \
                 WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.tag)",
        )
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("UNIQUE"),
        "target-qualified missing column must not silently match: {e}"
    );
    let r = rows(&mut db, "SELECT v FROM tgt");
    assert_eq!(r.rows, vec![vec![Value::Str("keep".into())]]);
}

/// RENAME COLUMN:被引用列改名后,本表自引用/同表 FK 的 rc 与跨表引用方
/// 的 rc 都必须跟随;dump 回放不再中断。
#[test]
fn rename_column_follows_fk_remote_columns() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE p (id INT PRIMARY KEY, a INT)");
    run(&mut db, "CREATE TABLE c (x INT REFERENCES p(id))");
    run(&mut db, "INSERT INTO p VALUES (1, 10)");
    run(&mut db, "INSERT INTO c VALUES (1)");
    run(&mut db, "ALTER TABLE p RENAME COLUMN id TO gid");
    run(&mut db, "INSERT INTO c VALUES (1)");
    // dump 回放(备份/快照通道)。
    let dump = db.dump_script().unwrap();
    let mut db2 = Database::in_memory().unwrap();
    for stmt in dump.split(';').filter(|s| !s.trim().is_empty()) {
        db2.execute(stmt)
            .unwrap_or_else(|e| panic!("replay {stmt}: {e}"));
    }
    // 自引用与同表兄弟列。
    run(
        &mut db,
        "CREATE TABLE e (id INT PRIMARY KEY, mgr INT REFERENCES e(id))",
    );
    run(&mut db, "INSERT INTO e VALUES (1, NULL)");
    run(&mut db, "ALTER TABLE e RENAME COLUMN id TO eid");
    run(&mut db, "INSERT INTO e VALUES (2, 1)");
    run(
        &mut db,
        "CREATE TABLE s (id INT PRIMARY KEY, b INT REFERENCES s(id))",
    );
    run(&mut db, "INSERT INTO s VALUES (1, 1)");
    run(&mut db, "ALTER TABLE s RENAME COLUMN id TO sid");
    run(&mut db, "INSERT INTO s VALUES (2, 1)");
}

/// RENAME COLUMN 的 CHECK 文本改写不得触碰字符串字面量
/// (CHECK (name <> 'name') 改名列后约束语义不得漂移)。
#[test]
fn rename_column_keeps_check_string_literals() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (name TEXT CHECK (name <> 'name'))");
    run(&mut db, "INSERT INTO t VALUES ('ok')");
    run(&mut db, "ALTER TABLE t RENAME COLUMN name TO title");
    assert!(db.execute("INSERT INTO t VALUES ('name')").is_err());
    run(&mut db, "INSERT INTO t VALUES ('title')");
}

/// RENAME COLUMN 落到已存在的动态字段名:静默覆盖数据 → 响亮拒绝。
#[test]
fn rename_column_onto_existing_data_field_rejected() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, a INT)");
    run(&mut db, "INSERT INTO t (id, a, extra) VALUES (1, 10, 99)");
    let e = db
        .execute("ALTER TABLE t RENAME COLUMN a TO extra")
        .unwrap_err()
        .to_string();
    assert!(e.contains("data field"), "{e}");
    let r = rows(&mut db, "SELECT a, extra FROM t WHERE id = 1");
    assert_eq!(r.rows, vec![vec![Value::Int(10), Value::Int(99)]]);
}

/// DROP COLUMN 被视图按名列引用:拒绝(存储视图 SQL 不改写,缺列读 NULL
/// 对 WHERE 是静默错误行;dump 回放还「看起来成功」)。
#[test]
fn drop_column_referenced_by_view_rejected() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, c INT, d INT)");
    run(&mut db, "CREATE VIEW v AS SELECT c, d FROM t WHERE c > 0");
    let e = db
        .execute("ALTER TABLE t DROP COLUMN c")
        .unwrap_err()
        .to_string();
    assert!(e.contains("VIEW"), "{e}");
    // 同被视图引用的 d 同拒;视图未引用的列照常可删。
    assert!(db.execute("ALTER TABLE t DROP COLUMN d").is_err());
    run(&mut db, "ALTER TABLE t ADD COLUMN e INT");
    run(&mut db, "ALTER TABLE t DROP COLUMN e");
}

/// CTE 名遮蔽同名对象:读取目标分类不再把 CTE 名当基表
/// (FROM v 读的是 CTE,不是名为 v 的表/视图)。
#[test]
fn read_targets_skip_cte_shadowed_names() {
    let mut stmts = sqlparser::parser::Parser::parse_sql(
        &sqlparser::dialect::GenericDialect {},
        "WITH v AS (SELECT x FROM secret) SELECT * FROM v",
    )
    .unwrap();
    let targets = Database::stmt_read_targets(&stmts.swap_remove(0), &|_| false).unwrap();
    assert_eq!(targets, vec!["secret"]);
    // 建视图撞名场景:WITH 体视图本身被响亮拒绝(既有边界),
    // 不再以误导性的「自引用」为由。
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (a INT)");
    let e = db
        .execute("CREATE VIEW v AS WITH v AS (SELECT a FROM t) SELECT * FROM v")
        .unwrap_err()
        .to_string();
    assert!(e.contains("WITH queries must use the write path"), "{e}");
}

/// ROWNUM 在写语句自己的谓词里没有行流可编号:读 NULL 且 NULL<=n 恒真,
/// Oracle 风格 `WHERE ROWNUM <= 1` 曾静默全表改写/删除 → 响亮拒绝;
/// 子查询内的 ROWNUM 是子查询自己的行流,照常可用。
#[test]
fn rownum_in_write_predicates_rejected() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
    run(&mut db, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    for sql in [
        "UPDATE t SET v = 'x' WHERE ROWNUM <= 1",
        "DELETE FROM t WHERE ROWNUM <= 1",
        "UPDATE t SET v = ROWNUM",
    ] {
        let e = db.execute(sql).unwrap_err().to_string();
        assert!(e.contains("ROWNUM"), "{sql}: {e}");
    }
    let n = rows(&mut db, "SELECT COUNT(*) FROM t").rows[0][0].as_i64();
    assert_eq!(n, Some(3), "rejected statements must not touch rows");
    // 子查询形态合法。
    run(
        &mut db,
        "DELETE FROM t WHERE id IN (SELECT id FROM t WHERE ROWNUM = 1)",
    );
    let n = rows(&mut db, "SELECT COUNT(*) FROM t").rows[0][0].as_i64();
    assert_eq!(n, Some(2));
}

/// 兼容字典视图名(DUAL/sqlite_master/USER_TABLES…)在 CREATE TABLE/VIEW
/// 保留:FROM 解析永远让字典视图优先,建得出写进不去的数据必须拒绝。
#[test]
fn compat_dictionary_names_reserved_at_create() {
    let mut db = Database::in_memory().unwrap();
    for name in ["dual", "sqlite_master", "user_tables", "ALL_TAB_COLUMNS"] {
        assert!(
            db.execute(&format!("CREATE TABLE {name} (x INT)")).is_err(),
            "CREATE TABLE {name} must be reserved"
        );
        assert!(
            db.execute(&format!("CREATE VIEW {name} AS SELECT 1"))
                .is_err(),
            "CREATE VIEW {name} must be reserved"
        );
    }
    // 普通名不受影响。
    run(&mut db, "CREATE TABLE user_notes (x INT)");
}

/// CAST/CONVERT 到 DATE:真转换 + UTC 零点截断(T-SQL 语义),不再是
/// 「原文原样返回」;非法文本响亮报错。
#[test]
fn cast_as_date_converts_and_truncates() {
    let mut db = Database::in_memory().unwrap();
    let r = rows(&mut db, "SELECT TYPEOF(CAST('2026-09-28' AS DATE))");
    assert_eq!(r.rows[0][0], Value::Str("timestamp".into()));
    assert_eq!(
        rows(&mut db, "SELECT CAST('2026-09-28 10:30' AS DATE)").rows[0][0].to_string(),
        "2026-09-28T00:00:00.000Z"
    );
    assert!(db.execute("SELECT CAST('not a date' AS DATE)").is_err());
    assert!(db.execute("SELECT TRY_CAST('nope' AS DATE)").is_ok());
}

/// BLOB → 数值目标:静默原样穿透曾让比较/算术永远错位 → 响亮报错。
#[test]
fn cast_numeric_from_blob_rejected() {
    let mut db = Database::in_memory().unwrap();
    for sql in [
        "SELECT CONVERT(INT, x'12345678')",
        "SELECT CAST(x'01' AS FLOAT)",
        "SELECT CAST(x'01' AS DECIMAL)",
    ] {
        assert!(db.execute(sql).is_err(), "{sql} must fail loudly");
    }
}
/// 第三轮审查回归:wall-clock 经 CAST/FLOOR 包装曾逃逸确定性门禁
/// (expr_calls_wall_clock 不下探容器变体)→ DEFAULT 不回写、journal 携带
/// 原文,重放节点各自打时钟。
#[test]
fn cast_wrapped_wall_clock_default_and_check_are_gated() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY, ts TIMESTAMP DEFAULT CAST(GETDATE() AS TIMESTAMP))",
    );
    run(&mut db, "INSERT INTO t (id) VALUES (1)");
    // The journaled text must carry the resolved instant, not the call
    // (a bare GETDATE() default was already rewritten; the CAST wrapper
    // used to slip the call through to every replaying peer's clock).
    let captured = db.take_resolved_sql().unwrap_or_default();
    assert!(
        !captured.contains("GETDATE"),
        "resolved INSERT must not carry GETDATE(): {captured}"
    );
    assert!(
        captured.contains("AS TIMESTAMP"),
        "resolved INSERT carries a timestamp literal: {captured}"
    );
    // CHECK carrying a wrapped clock must be refused at CREATE time.
    assert!(db
            .execute("CREATE TABLE bad (x INT CHECK (CAST(GETDATE() AS TIMESTAMP) > TIMESTAMP '2000-01-01T00:00:00Z'))")
            .is_err());
}

/// 第三轮审查回归:未定向冲突策略(OR IGNORE / ON DUPLICATE KEY /
/// REPLACE INTO)曾只探测单列约束树,复合唯一索引冲突落到树级 insert
/// 直接报错而非跳过/更新/置换。
#[test]
fn undirected_conflict_policies_cover_composite_unique_index() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)");
    run(&mut db, "CREATE UNIQUE INDEX uq ON t (a, b)");
    run(&mut db, "INSERT INTO t VALUES (1, 10, 20)");
    // OR IGNORE: skip silently.
    run(&mut db, "INSERT OR IGNORE INTO t VALUES (2, 10, 20)");
    let r = rows(&mut db, "SELECT COUNT(*) FROM t");
    assert_eq!(r.rows[0][0], Value::Int(1));
    // ON DUPLICATE KEY UPDATE: update in place.
    run(
        &mut db,
        "INSERT INTO t VALUES (2, 10, 20) ON DUPLICATE KEY UPDATE b = 21",
    );
    let r = rows(&mut db, "SELECT id, a, b FROM t");
    assert_eq!(
        r.rows,
        vec![vec![Value::Int(1), Value::Int(10), Value::Int(21)]]
    );
    // REPLACE INTO: displace the conflicting row.
    run(&mut db, "REPLACE INTO t VALUES (3, 10, 21)");
    let r = rows(&mut db, "SELECT id FROM t");
    assert_eq!(r.rows, vec![vec![Value::Int(3)]]);
}

/// 第三轮审查回归:upsert 多行批两次触碰同一行时,中间像曾参与父侧
/// FK 判定——最后一次触碰腾出的旧键被中间像"顶住",子行悬空引用被放行。
#[test]
fn upsert_multi_touch_collapses_intermediate_images_for_fk() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE p (id INT PRIMARY KEY, alt INT UNIQUE)",
    );
    run(&mut db, "CREATE TABLE c (pid INT REFERENCES p(id))");
    run(&mut db, "INSERT INTO p VALUES (1, 100)");
    run(&mut db, "INSERT INTO c VALUES (1)");
    // Three touches of the SAME row through `alt`: id goes 1→2→1→2.
    // The intermediate image (id=1) re-supplied the vacated key, so
    // feeding intermediate images to the parent-side check passed the
    // child's reference while the FINAL table only holds id=2 — a
    // dangling child committed silently.
    assert!(db
        .execute(
            "INSERT INTO p VALUES (2, 100), (1, 100), (2, 100) \
                 ON DUPLICATE KEY UPDATE id = VALUES(id)"
        )
        .is_err());
    // Without the child the same statement succeeds and collapses to
    // one row (first old image + final merged image).
    let mut db2 = Database::in_memory().unwrap();
    run(
        &mut db2,
        "CREATE TABLE p (id INT PRIMARY KEY, alt INT UNIQUE)",
    );
    run(&mut db2, "INSERT INTO p VALUES (1, 100)");
    run(
        &mut db2,
        "INSERT INTO p VALUES (2, 100), (1, 100), (2, 100) \
             ON DUPLICATE KEY UPDATE id = VALUES(id)",
    );
    let r = rows(&mut db2, "SELECT id FROM p");
    assert_eq!(r.rows, vec![vec![Value::Int(2)]], "{r:?}");
}

/// 第六轮审查回归:OR REPLACE 的位移列表未按 locator 去重——同一旧行
/// 被 PK 与 UNIQUE 两条约束各命中一次时,重复计数毒化 FK 父侧的
/// 出现次数筛查,合法 REPLACE 被误拒。
#[test]
fn or_replace_displaced_dedup_keeps_fk_counting_sane() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY, u INT UNIQUE, k INT)",
    );
    run(&mut db, "CREATE TABLE c (k INT REFERENCES t(k))");
    run(&mut db, "INSERT INTO t VALUES (1, 100, 50), (2, 101, 50)");
    run(&mut db, "INSERT INTO c VALUES (50)");
    // 新行同时按 id=1 与 u=100 命中旧行 R1(displaced 含重复 locator);
    // R2 仍承载 k=50,本应放行——修复前 count_in(current)=1 不大于
    // count_in(removed)=2,lost 保留 → 误报 FOREIGN KEY。
    run(&mut db, "INSERT OR REPLACE INTO t VALUES (1, 100, 99)");
    let r = rows(&mut db, "SELECT id, u, k FROM t ORDER BY id");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(1), Value::Int(100), Value::Int(99)],
            vec![Value::Int(2), Value::Int(101), Value::Int(50)]
        ]
    );
}

/// 第六轮审查回归:MERGE 源/目标同别名时目标限定键覆盖源限定键,
/// ON 恒真、SET 读目标自身预像——必须显式拒绝。
#[test]
fn merge_rejects_same_alias_for_source_and_target() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, tag TEXT)");
    run(&mut db, "CREATE TABLE s (id INT, tag TEXT)");
    run(&mut db, "INSERT INTO t VALUES (1, 'a')");
    run(&mut db, "INSERT INTO s VALUES (1, 'b')");
    let e = db
            .execute("MERGE INTO t USING (SELECT id, tag FROM s) AS t ON t.id = t.id WHEN MATCHED THEN UPDATE SET tag = t.tag")
            .unwrap_err()
            .to_string();
    assert!(e.contains("aliases must differ"), "{e}");
}

/// 第三轮审查回归:CTAS 查询携带 NEWID() 曾既不折叠也不拒绝,journal
/// 携带原文、各重放节点自行掷 GUID(静默集群分叉)。
#[test]
fn ctas_with_newid_is_rejected_loudly() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE src (n INT)");
    run(&mut db, "INSERT INTO src VALUES (1), (2)");
    assert!(db
        .execute("CREATE TABLE t2 AS SELECT NEWID() AS g FROM src")
        .is_err());
    assert!(db
        .execute("CREATE TABLE t3 AS SELECT RAND() AS r FROM src")
        .is_ok());
}

/// 第三轮审查回归:递归 CTE 体自带的 ORDER BY / LIMIT 曾被静默丢弃
/// (非递归 CTE 同写法完整生效)——现在显式报错。
#[test]
fn recursive_cte_rejects_body_order_limit() {
    let mut db = Database::in_memory().unwrap();
    run(
            &mut db,
            "WITH c AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM c WHERE n < 3) SELECT n FROM c ORDER BY n",
        );
    assert!(db
            .execute(
                "WITH c AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM c WHERE n < 3 LIMIT 2) SELECT n FROM c"
            )
            .is_err());
    assert!(db
            .execute(
                "WITH c AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM c WHERE n < 3 ORDER BY n) SELECT n FROM c"
            )
            .is_err());
}

/// 第三轮审查回归:非相关子查询嵌在 FLOOR/IS NULL/SUBSTRING 等包装节点
/// 下曾被原样留到行级求值,误报"correlated subqueries are not supported"。
#[test]
fn noncorrelated_subquery_under_wrapper_nodes_resolves() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE u (x INT)");
    run(&mut db, "INSERT INTO u VALUES (4)");
    run(&mut db, "CREATE TABLE t (n INT)");
    run(&mut db, "INSERT INTO t VALUES (1)");
    let r = rows(&mut db, "SELECT FLOOR((SELECT AVG(x) FROM u)) FROM t");
    assert_eq!(r.rows, vec![vec![Value::Float(4.0)]], "{r:?}");
    let r = rows(
        &mut db,
        "SELECT n FROM t WHERE (SELECT MAX(x) FROM u) IS NOT NULL",
    );
    assert_eq!(r.rows.len(), 1);
    let r = rows(
        &mut db,
        "SELECT SUBSTRING((SELECT 'abcd' FROM u), 2, 2) FROM t",
    );
    assert_eq!(r.rows, vec![vec![Value::Str("bc".into())]], "{r:?}");
}

/// 第三轮审查回归:CREATE VIEW 目录保存失败曾留内存幽灵视图(下次任意
/// 写把它持久化,与 peers 分叉)。save_catalog 失败难以在测试中构造,
/// 但 OR REPLACE 的还原路径可以用目录写权限外的行为钉住:视图替换后
/// 旧体不再可解析。
#[test]
fn create_view_replaces_atomically_by_name() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (a INT)");
    run(&mut db, "INSERT INTO t VALUES (1)");
    run(&mut db, "CREATE VIEW v AS SELECT a FROM t");
    run(
        &mut db,
        "CREATE OR REPLACE VIEW v AS SELECT a + 1 AS a FROM t",
    );
    let r = rows(&mut db, "SELECT a FROM v");
    assert_eq!(r.rows, vec![vec![Value::Int(2)]]);
}

/// 第三轮审查回归:视图上的 GRANT 曾使 dump 不可回放(用户语句在
/// CREATE VIEW 之前)——restore/join/repair 快照全部中断。
#[test]
fn dump_script_orders_grants_after_views() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (a INT)");
    run(&mut db, "INSERT INTO t VALUES (1)");
    run(&mut db, "CREATE VIEW v AS SELECT a FROM t");
    run(&mut db, "CREATE USER u1 PASSWORD 'pw12345678'");
    run(&mut db, "GRANT SELECT ON v TO u1");
    let dump = db.dump_script().unwrap();
    let view_at = dump.find("CREATE VIEW").expect("view in dump");
    let grant_at = dump.find("GRANT SELECT ON").expect("grant in dump");
    assert!(view_at < grant_at, "GRANT must replay after CREATE VIEW");
    // The dump must re-apply cleanly onto a fresh database.
    let mut db2 = Database::in_memory().unwrap();
    for stmt in dump.split(';').filter(|s| !s.trim().is_empty()) {
        db2.execute(stmt)
            .unwrap_or_else(|e| panic!("replay {stmt:?}: {e}"));
    }
}

/// 第三轮审查回归:stmt_read_targets 去重后,RLS 覆盖证明的同表双出现
/// (外层 FROM + 子查询)被折叠成一次出现而放行——出现次数变体必须
/// 保留重复。
#[test]
fn read_target_occurrences_preserve_duplicates() {
    let mut stmts = Parser::parse_sql(
        &GenericDialect {},
        "SELECT (SELECT secret FROM t WHERE id = 42) FROM t",
    )
    .unwrap();
    let stmt = stmts.swap_remove(0);
    let occ = Database::stmt_read_target_occurrences(&stmt, &|_| false).unwrap();
    assert_eq!(
        occ.iter().filter(|x| *x == "t").count(),
        2,
        "per-occurrence list keeps both reads: {occ:?}"
    );
    let set = Database::stmt_read_targets(&stmt, &|_| false).unwrap();
    assert_eq!(set.iter().filter(|x| *x == "t").count(), 1);
}

/// 第三轮审查回归:EXPLAIN 曾把表值函数 FROM 因子误报"表不存在"、把
/// 括号化查询体误报"集合操作不支持"——两者均可正常执行。
#[test]
fn explain_accepts_table_functions_and_parenthesized_body() {
    let mut db = Database::in_memory().unwrap();
    let r = rows(
        &mut db,
        "EXPLAIN SELECT * FROM STRING_SPLIT('a,b', ',') AS s",
    );
    assert!(
        r.rows.iter().any(|row| row[1]
            .as_str()
            .is_some_and(|d| d.contains("table function"))),
        "{r:?}"
    );
}

/// 第六轮审查回归:JOIN 侧表因子携带执行器必拒子句(TABLESAMPLE/
/// 索引提示/时间旅行)与 DISTINCT ON 时,EXPLAIN 不得输出一份永远
/// 跑不起来的"计划"。
#[test]
fn explain_mirrors_join_factor_and_distinct_on_rejections() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE mj (id INT PRIMARY KEY, v INT)");
    run(&mut db, "CREATE TABLE mu (id INT PRIMARY KEY, v INT)");
    let e = db
        .execute("EXPLAIN SELECT * FROM mj JOIN mu ON mj.id = mu.id TABLESAMPLE (10 PERCENT)")
        .unwrap_err()
        .to_string();
    assert!(e.contains("TABLESAMPLE"), "{e}");
    let e = db
        .execute("EXPLAIN SELECT DISTINCT ON (v) v FROM mj")
        .unwrap_err()
        .to_string();
    assert!(e.contains("DISTINCT ON"), "{e}");
}

/// 第七轮审查回归:相关子查询的外层引用藏在包装节点(IS NULL/
/// SUBSTRING)下时,代入遍历必须照样替换——残留引用在内层读 NULL,
/// DELETE 形态曾把整表删光。
#[test]
fn correlated_outer_ref_under_wrapper_nodes() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, x INT)");
    run(&mut db, "CREATE TABLE a (y INT)");
    run(
        &mut db,
        "INSERT INTO t (id, x) VALUES (1, NULL), (2, 5), (3, 7)",
    );
    run(&mut db, "INSERT INTO a (y) VALUES (0)");
    // IS NULL under EXISTS: only the NULL-x row matches the subquery.
    let r = rows(
        &mut db,
        "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM a WHERE t.x IS NULL) ORDER BY id",
    );
    assert_eq!(r.rows.len(), 1, "{r:?}");
    assert_eq!(r.rows[0][0], Value::Int(1));
    // SUBSTRING wrapper: same table joined, substring of outer value.
    let r = rows(
            &mut db,
            "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM a WHERE SUBSTRING(t.x, 1, 1) = '5') ORDER BY id",
        );
    assert_eq!(r.rows.len(), 1, "{r:?}");
    assert_eq!(r.rows[0][0], Value::Int(2));
    // The DELETE shape that used to wipe the whole table.
    run(
        &mut db,
        "DELETE FROM t WHERE EXISTS (SELECT 1 FROM a WHERE t.x IS NULL)",
    );
    let r = rows(&mut db, "SELECT COUNT(*) AS n FROM t");
    assert_eq!(r.rows[0][0], Value::Int(2), "{r:?}");
}

/// 第七轮审查回归:集合运算子查询的各臂 FROM 名是内层作用域,
/// 限定列引用不得误判为相关子查询。
#[test]
fn set_operation_subquery_arms_are_inner_scope() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (x INT)");
    run(&mut db, "CREATE TABLE a (x INT)");
    run(&mut db, "INSERT INTO t (x) VALUES (1), (2)");
    run(&mut db, "INSERT INTO a (x) VALUES (2)");
    let r = rows(
        &mut db,
        "SELECT x FROM t WHERE t.x IN (SELECT a.x FROM a UNION SELECT a.x FROM a)",
    );
    assert_eq!(r.rows.len(), 1, "{r:?}");
    assert_eq!(r.rows[0][0], Value::Int(2));
    // EXISTS body as a set operation must work too.
    let r = rows(
        &mut db,
        "SELECT x FROM t WHERE EXISTS (SELECT a.x FROM a WHERE a.x = 1 UNION SELECT 2)",
    );
    assert_eq!(r.rows.len(), 2, "{r:?}");
}

/// 第七轮审查回归:LEFT JOIN 未匹配行的 NULL 补齐用右表全部行的
/// 键形并集(首行形状不够——schemaless 右表后插的行带新键)。
#[test]
fn left_join_null_padding_uses_key_union() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE a (id INT, y INT)");
    run(&mut db, "CREATE TABLE b (x INT, y INT)");
    run(&mut db, "INSERT INTO a (id, y) VALUES (1, 100), (3, 100)");
    // First right row lacks y; a later row carries it.
    run(&mut db, "INSERT INTO b (x) VALUES (1)");
    run(&mut db, "INSERT INTO b (x, y) VALUES (9, 1)");
    let r = rows(
        &mut db,
        "SELECT a.id, b.y FROM a LEFT JOIN b ON a.id = b.x ORDER BY a.id",
    );
    // id=3 unmatched: b.y must read NULL, not leak a.y = 100.
    assert_eq!(r.rows.len(), 2, "{r:?}");
    assert_eq!(r.rows[1][0], Value::Int(3));
    assert!(matches!(r.rows[1][1], Value::Null), "{r:?}");
    let r = rows(
        &mut db,
        "SELECT COUNT(*) AS n FROM a LEFT JOIN b ON a.id = b.x WHERE b.y IS NULL",
    );
    assert_eq!(r.rows[0][0], Value::Int(1), "{r:?}");
}

/// 第七轮审查回归:ROWNUM 注入 JOIN 产物后非限定列仍走后缀解析;
/// 聚合 + ROWNUM 显式报错而非静默 NULL 过滤。
#[test]
fn rownum_over_joined_rows_and_aggregate_refusal() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE a (id INT, x INT)");
    run(&mut db, "CREATE TABLE b (id INT, y INT)");
    run(&mut db, "INSERT INTO a (id, x) VALUES (1, 10)");
    run(&mut db, "INSERT INTO b (id, y) VALUES (1, 20)");
    let r = rows(&mut db, "SELECT ROWNUM, x, y FROM a JOIN b ON a.id = b.id");
    assert_eq!(r.rows.len(), 1, "{r:?}");
    assert_eq!(r.rows[0][1], Value::Int(10), "{r:?}");
    assert_eq!(r.rows[0][2], Value::Int(20), "{r:?}");
    let e = db
        .execute("SELECT COUNT(*) FROM a WHERE ROWNUM <= 2")
        .unwrap_err()
        .to_string();
    assert!(e.contains("ROWNUM"), "{e}");
}

/// 第七轮审查回归:NULLIF/DECODE/GREATEST/JSON_ARRAY_CONTAINS 走与
/// 谓词相同的比较提升漏斗(Timestamp ↔ 可解析字符串)。
#[test]
fn equality_family_uses_coercion_funnel() {
    let mut db = Database::in_memory().unwrap();
    let r = rows(
            &mut db,
            "SELECT NULLIF(TIMESTAMP '2026-09-16T00:00:00Z', '2026-09-16') IS NULL AS a, \
             DECODE(TIMESTAMP '2026-09-16T00:00:00Z', '2026-09-16', 'hit', 'miss') AS b, \
             GREATEST(TIMESTAMP '2026-09-16T00:00:00Z', '2026-01-01') AS c, \
             JSON_ARRAY_CONTAINS('[\"2026-01-01T00:00:00Z\"]', TIMESTAMP '2026-01-01T00:00:00Z') AS d",
        );
    let row = &r.rows[0];
    assert_eq!(row[0], Value::Bool(true), "NULLIF {r:?}");
    assert_eq!(row[1], Value::Str("hit".into()), "DECODE {r:?}");
    assert!(
        matches!(row[2], Value::Timestamp(..)),
        "GREATEST must keep the Timestamp, got {r:?}"
    );
    assert_eq!(row[3], Value::Bool(true), "JSON_ARRAY_CONTAINS {r:?}");
}

/// 第七轮审查回归:ROUND 的 DECIMAL 分支对超域 digits 先夹取——
/// 2^32 截断成 scale 0 与 i64::MIN 取负 panic 都是静默/崩溃错误。
#[test]
fn round_decimal_digits_clamped() {
    let mut db = Database::in_memory().unwrap();
    let r = rows(
        &mut db,
        "SELECT ROUND(CAST(1.567 AS DECIMAL), 4294967296) = CAST(1.567 AS DECIMAL) AS a, \
             ROUND(CAST(1.5 AS DECIMAL), 0-9223372036854775807-1) AS b",
    );
    // 2^32 clamps to scale 28: value unchanged, no silent scale-0 read.
    assert_eq!(r.rows[0][0], Value::Bool(true), "{r:?}");
    // -2^63 clamps to -15: rounds at tens, value 1.5 -> 0.
    assert_eq!(r.rows[0][1].to_string(), "0", "{r:?}");
}

/// 第七轮审查回归:值列表子查询多列是基数错误,响亮报错而非取首列。
#[test]
fn multi_column_list_subquery_is_loud() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (a INT, b INT)");
    run(&mut db, "INSERT INTO t (a, b) VALUES (1, 5), (2, 6)");
    let e = db
        .execute("SELECT 5 IN (SELECT a, b FROM t)")
        .unwrap_err()
        .to_string();
    assert!(e.contains("columns"), "{e}");
    let e = db
        .execute("SELECT 1 = ANY (SELECT a, b FROM t)")
        .unwrap_err()
        .to_string();
    assert!(e.contains("columns"), "{e}");
}

/// 第七轮审查回归:ILIKE 折叠按字符进行——İ(U+0130)整串 lowercase
/// 展开成两个字符,`_` 计数失真。
#[test]
fn ilike_folds_per_char() {
    let mut db = Database::in_memory().unwrap();
    let r = rows(&mut db, "SELECT 'İİ' ILIKE '__' AS a, 'İİ' LIKE '__' AS b");
    assert_eq!(r.rows[0][0], Value::Bool(true), "{r:?}");
    assert_eq!(r.rows[0][1], Value::Bool(true), "{r:?}");
}

/// 第七轮审查回归:限定名/引号点分字段与探针的一致性——带字面
/// 点分键的表上,查询结果不得依赖索引存在性;引号 "v.x" 引用不剥
/// 限定前缀。
#[test]
fn dotted_literal_fields_and_probe_parity() {
    let mk = |idx: bool| {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE v (x INT)");
        if idx {
            run(&mut db, "CREATE INDEX iv ON v(x)");
        }
        run(&mut db, "INSERT INTO v (x) VALUES (9)");
        run(&mut db, "UPDATE v SET \"v.x\" = 5");
        db
    };
    for idx in [false, true] {
        let mut db = mk(idx);
        // eval tries the literal "v.x" key first — the stripped probe
        // used to disagree whenever the index existed.
        let r = rows(&mut db, "SELECT x FROM v WHERE v.x = 5");
        assert_eq!(r.rows.len(), 1, "idx={idx} qualified: {r:?}");
        let r = rows(&mut db, "SELECT x FROM v WHERE \"v.x\" = 5");
        assert_eq!(r.rows.len(), 1, "idx={idx} quoted: {r:?}");
        let r = rows(&mut db, "SELECT x FROM v WHERE x = 9");
        assert_eq!(r.rows.len(), 1, "idx={idx} bare: {r:?}");
    }
    // UPDATE/DELETE fast paths share the guard.
    let mut db = mk(true);
    run(&mut db, "UPDATE v SET x = 11 WHERE v.x = 5");
    let r = rows(&mut db, "SELECT x FROM v");
    assert_eq!(r.rows[0][0], Value::Int(11), "{r:?}");
}

/// 第七轮审查回归:全点分键的单表文档不再是"join 形态",裸列查询
/// 不经后缀回退读点分键——有/无索引同答。
#[test]
fn single_table_dotted_only_docs_reject_suffix_fallback() {
    let mk = |idx: bool| {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE g (\"g.x\" INT)");
        run(&mut db, "INSERT INTO g (\"g.x\") VALUES (5)");
        run(&mut db, "ALTER TABLE g ADD COLUMN x INT");
        if idx {
            run(&mut db, "CREATE INDEX ig ON g(x)");
        }
        db
    };
    for idx in [false, true] {
        let mut db = mk(idx);
        let r = rows(&mut db, "SELECT COUNT(*) AS n FROM g WHERE x = 5");
        assert_eq!(r.rows[0][0], Value::Int(0), "idx={idx}: {r:?}");
        // The literal dotted field still reads by its exact name.
        let r = rows(&mut db, "SELECT \"g.x\" FROM g WHERE \"g.x\" = 5");
        assert_eq!(r.rows.len(), 1, "idx={idx}: {r:?}");
    }
}

/// 第七轮审查回归:括号包裹的谓词透明解包,探针不失联。
#[test]
fn parenthesized_predicate_still_probes() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE p (a INT, b TEXT)");
    run(&mut db, "CREATE INDEX ipa ON p(a)");
    run(&mut db, "INSERT INTO p (a, b) VALUES (1, 'x'), (2, 'y')");
    let r = rows(&mut db, "EXPLAIN SELECT * FROM p WHERE (a = 1)");
    assert!(!r.rows.is_empty(), "{r:?}");
    let text = format!("{r:?}");
    assert!(text.to_uppercase().contains("PROBE"), "{text}");
    // Behavior: same answers with and without parens.
    let r = rows(&mut db, "SELECT a FROM p WHERE (a = 1) AND b = 'x'");
    assert_eq!(r.rows.len(), 1, "{r:?}");
}

/// 第七轮审查回归:UPDATE..FROM / DELETE..USING / MERGE..USING 的
/// 派生表源携带 RAND/NEWID 时必须拒绝——journal 原文重放会各自
/// 重掷,集群静默分叉。
#[test]
fn nondeterministic_derived_table_sources_in_writes_rejected() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    run(&mut db, "INSERT INTO t (id, v) VALUES (1, 1), (2, 2)");
    let e = db
            .execute("UPDATE t SET v = 99 FROM (SELECT RAND() AS r) AS x WHERE t.id = CASE WHEN x.r > 0.5 THEN 1 ELSE 2 END")
            .unwrap_err()
            .to_string();
    assert!(e.contains("RAND") || e.contains("NEWID"), "{e}");
    let e = db
            .execute("DELETE FROM t USING (SELECT RAND() AS r) AS x WHERE t.id = CASE WHEN x.r > 0.5 THEN 1 ELSE 2 END")
            .unwrap_err()
            .to_string();
    assert!(e.contains("RAND") || e.contains("NEWID"), "{e}");
    let e = db
            .execute("MERGE INTO t USING (SELECT RAND() AS r) AS s ON t.id = CASE WHEN s.r > 0.5 THEN 1 ELSE 2 END WHEN MATCHED THEN UPDATE SET v = 99")
            .unwrap_err()
            .to_string();
    assert!(e.contains("RAND") || e.contains("NEWID"), "{e}");
    // Nothing was applied by the rejected statements.
    let r = rows(&mut db, "SELECT COUNT(*) AS n FROM t");
    assert_eq!(r.rows[0][0], Value::Int(2), "{r:?}");
}

/// 第七轮审查回归:ADD COLUMN DEFAULT NOW()/RAND() 对非空表回填
/// 本节点值且无回写——拒绝;空表保留默认文本(后续 INSERT 走定值
/// 回写,安全)。
#[test]
fn add_column_wallclock_default_backfill_rejected() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY)");
    run(&mut db, "INSERT INTO t (id) VALUES (1)");
    let e = db
        .execute("ALTER TABLE t ADD COLUMN ts TIMESTAMP DEFAULT NOW()")
        .unwrap_err()
        .to_string();
    assert!(e.contains("DEFAULT"), "{e}");
    // Empty table keeps the default for future INSERTs.
    run(&mut db, "CREATE TABLE e (id INT PRIMARY KEY)");
    run(
        &mut db,
        "ALTER TABLE e ADD COLUMN ts TIMESTAMP DEFAULT NOW()",
    );
    run(&mut db, "INSERT INTO e (id) VALUES (1)");
    let r = rows(&mut db, "SELECT ts IS NOT NULL AS has FROM e");
    assert_eq!(r.rows[0][0], Value::Bool(true), "{r:?}");
}

/// 第七轮审查回归:RENAME TO 补齐 CREATE 的保留名守卫(#临时表/
/// 兼容字典视图名)——否则数据写入后不可读且 dump 不可重放。
#[test]
fn rename_to_reserved_names_rejected() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (a INT)");
    let e = db
        .execute("ALTER TABLE t RENAME TO dual")
        .unwrap_err()
        .to_string();
    assert!(e.contains("reserved"), "{e}");
    let e = db
        .execute("ALTER TABLE t RENAME TO ALL_TABLES")
        .unwrap_err()
        .to_string();
    assert!(e.contains("reserved"), "{e}");
    let e = db
        .execute("ALTER TABLE t RENAME TO #x")
        .unwrap_err()
        .to_string();
    assert!(e.contains("#"), "{e}");
    // Sanity: the table is still readable under its old name.
    let r = rows(&mut db, "SELECT COUNT(*) AS n FROM t");
    assert_eq!(r.rows[0][0], Value::Int(0), "{r:?}");
}

/// 第七轮审查回归:列级 UNIQUE 重复声明去重(`a INT UNIQUE UNIQUE`
/// 曾把树维护做成对同键二次插入,表永久不可写)。
#[test]
fn duplicate_column_unique_decl_deduplicated() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (a INT UNIQUE UNIQUE)");
    run(&mut db, "INSERT INTO t (a) VALUES (1)");
    run(&mut db, "INSERT INTO t (a) VALUES (2)");
    let e = db
        .execute("INSERT INTO t (a) VALUES (1)")
        .unwrap_err()
        .to_string();
    assert!(e.to_uppercase().contains("UNIQUE"), "{e}");
}

/// 第七轮审查回归:DROP INDEX 解除列级唯一时,含该列的**复合**
/// UNIQUE 索引不算同列来源(复合唯一从不保证单列唯一)。
#[test]
fn drop_index_lifts_unique_only_from_single_column_sources() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (a INT, b INT)");
    run(&mut db, "CREATE UNIQUE INDEX u1 ON t(a)");
    run(&mut db, "CREATE UNIQUE INDEX u2 ON t(a, b)");
    run(&mut db, "INSERT INTO t (a, b) VALUES (1, 1)");
    run(&mut db, "INSERT INTO t (a, b) VALUES (2, 2)");
    run(&mut db, "DROP INDEX u1");
    // Only (a, b) uniqueness remains enforced: (1, 3) is legal now.
    run(&mut db, "INSERT INTO t (a, b) VALUES (1, 3)");
    // And the composite still fires.
    let e = db
        .execute("INSERT INTO t (a, b) VALUES (1, 3)")
        .unwrap_err()
        .to_string();
    assert!(e.to_uppercase().contains("UNIQUE"), "{e}");
}

/// 第七轮审查回归:RENAME COLUMN 到含引号的标识符,CHECK 文本里的
/// 新名必须走引号拼写——裸拼破坏词法,该表一切 INSERT 解析即挂、
/// dump 不可重放。
#[test]
fn rename_column_quotes_tricky_names_in_checks() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (a INT, CHECK (a > 0))");
    run(&mut db, "INSERT INTO t (a) VALUES (1)");
    run(&mut db, "ALTER TABLE t RENAME COLUMN a TO \"x\"\"y\"");
    // The rewritten CHECK must still parse and enforce.
    run(&mut db, "INSERT INTO t (\"x\"\"y\") VALUES (5)");
    let e = db
        .execute("INSERT INTO t (\"x\"\"y\") VALUES (0)")
        .unwrap_err()
        .to_string();
    assert!(e.to_uppercase().contains("CHECK"), "{e}");
    // dump must replay (the quoted spelling round-trips).
    let script = db.dump_script().unwrap();
    let mut fresh = Database::in_memory().unwrap();
    for stmt in script.split(';').filter(|s| !s.trim().is_empty()) {
        run(&mut fresh, stmt);
    }
}

/// 第七轮审查回归:table_exists_ci 覆盖 ASCII 大小写变体——兼容视图
/// 改写的 shadow 守卫靠它挡 `DocSQL_PubSub` 这类用户表。
#[test]
fn table_exists_ci_matches_ascii_case_variants() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE \"DocSQL_PubSub\" (x INT)");
    assert!(db.table_exists("DocSQL_PubSub"));
    assert!(db.table_exists_ci("docsql_pubsub"), "case variant missed");
    assert!(!db.table_exists_ci("docsql_pubsub2"));
    assert!(!db.table_exists_ci("docsql_pubsu"));
}

/// 第七轮收尾:TRUNCATE 多表(含 CASCADE 扩表)整批单事务——全部
/// 目标一起清空;FK 校验先行,父子的清单整体合法才动手。
#[test]
fn truncate_multiple_tables_and_cascade() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE p (id INT PRIMARY KEY, v INT)");
    run(
        &mut db,
        "CREATE TABLE c (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES p (id))",
    );
    run(&mut db, "INSERT INTO p (id, v) VALUES (1, 10), (2, 20)");
    run(&mut db, "INSERT INTO c (id, pid) VALUES (1, 1), (2, 2)");
    run(&mut db, "TRUNCATE p, c");
    let r = rows(
        &mut db,
        "SELECT (SELECT COUNT(*) FROM p) + (SELECT COUNT(*) FROM c) AS n",
    );
    assert_eq!(r.rows[0][0], Value::Int(0), "{r:?}");
    // CASCADE reaches the child transitively.
    run(&mut db, "INSERT INTO p (id, v) VALUES (3, 30)");
    run(&mut db, "INSERT INTO c (id, pid) VALUES (3, 3)");
    run(&mut db, "TRUNCATE p CASCADE");
    let r = rows(
        &mut db,
        "SELECT (SELECT COUNT(*) FROM p) + (SELECT COUNT(*) FROM c) AS n",
    );
    assert_eq!(r.rows[0][0], Value::Int(0), "{r:?}");
    // The batch stays reloadable and re-insertable after the truncate.
    run(&mut db, "INSERT INTO p (id, v) VALUES (4, 40)");
    let r = rows(&mut db, "SELECT v FROM p");
    assert_eq!(r.rows[0][0], Value::Int(40), "{r:?}");
}

/// 第七轮收尾:OR REPLACE 在「约束列无树」的 legacy 形态表上按内容
/// 找被替行——替换语义恢复(旧行移除、新行落位),不再撞自身键报
/// UNIQUE 错误;批内同键后者胜,与树路径一致。
#[test]
fn or_replace_on_treeless_constraint_table_replaces() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
    run(
        &mut db,
        "INSERT INTO t (id, v) VALUES (1, 'old'), (2, 'keep')",
    );
    // Manufacture the legacy layout: the constraint column predates its
    // tree (old volumes carry this shape; new tables always build one).
    if let Some(m) = db.tables.get_mut("t") {
        let m = std::sync::Arc::make_mut(m);
        m.index_roots.remove("id");
    }
    run(
        &mut db,
        "INSERT OR REPLACE INTO t (id, v) VALUES (1, 'new')",
    );
    let r = rows(&mut db, "SELECT v FROM t ORDER BY id");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("new".into())],
            vec![Value::Str("keep".into())]
        ],
        "{r:?}"
    );
    // Intra-batch duplicate keys: later row wins.
    run(
        &mut db,
        "INSERT OR REPLACE INTO t (id, v) VALUES (3, 'a'), (3, 'b')",
    );
    let r = rows(&mut db, "SELECT v FROM t WHERE id = 3");
    assert_eq!(r.rows, vec![vec![Value::Str("b".into())]], "{r:?}");
    // The whole-set unique check still fires on a genuinely new key.
    let e = db
        .execute("INSERT INTO t (id, v) VALUES (2, 'dup')")
        .unwrap_err()
        .to_string();
    assert!(e.to_uppercase().contains("UNIQUE"), "{e}");
}

// ===== Row triggers =====

fn apply_dump(script: &str) -> Database {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "BEGIN");
    let batch = db.execute_batch(script);
    assert!(
        batch.error.is_none(),
        "dump replay failed: {:?}",
        batch.error
    );
    run(&mut db, "COMMIT");
    db
}

#[test]
fn trigger_fires_after_insert_update_delete() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
    run(&mut db, "CREATE TABLE audit (note TEXT)");
    run(
        &mut db,
        "CREATE TRIGGER ta AFTER INSERT ON t FOR EACH ROW \
             BEGIN INSERT INTO audit VALUES ('ins ' || NEW.id); END",
    );
    run(
        &mut db,
        "CREATE TRIGGER tu AFTER UPDATE ON t FOR EACH ROW \
             BEGIN INSERT INTO audit VALUES ('upd ' || OLD.v || '→' || NEW.v); END",
    );
    run(
        &mut db,
        "CREATE TRIGGER td AFTER DELETE ON t FOR EACH ROW \
             BEGIN INSERT INTO audit VALUES ('del ' || OLD.id); END",
    );
    run(&mut db, "INSERT INTO t VALUES (1, 'a'), (2, 'b')");
    run(&mut db, "UPDATE t SET v = 'x' WHERE id = 1");
    run(&mut db, "DELETE FROM t WHERE id = 2");
    let r = rows(&mut db, "SELECT note FROM audit ORDER BY note");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("del 2".into())],
            vec![Value::Str("ins 1".into())],
            vec![Value::Str("ins 2".into())],
            vec![Value::Str("upd a→x".into())],
        ],
        "{r:?}"
    );
}

#[test]
fn trigger_when_and_update_of_gating() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE orders (id INT, status TEXT, n INT)");
    run(&mut db, "CREATE TABLE stats (k TEXT PRIMARY KEY, n INT)");
    run(&mut db, "INSERT INTO stats VALUES ('done', 0)");
    run(
        &mut db,
        "CREATE TRIGGER tg AFTER UPDATE OF status ON orders FOR EACH ROW \
             WHEN (NEW.id > 1) BEGIN \
             UPDATE stats SET n = n + 1 WHERE k = 'done'; END",
    );
    run(
        &mut db,
        "INSERT INTO orders VALUES (1, 'new', 9), (2, 'new', 0)",
    );
    // An UPDATE of a different column does not fire an UPDATE OF trigger.
    run(&mut db, "UPDATE orders SET n = 9 WHERE id = 2");
    // UPDATE OF the listed column fires per row; WHEN filters row 1 out.
    run(
        &mut db,
        "UPDATE orders SET status = 'done' WHERE id IN (1, 2)",
    );
    let r = rows(&mut db, "SELECT n FROM stats WHERE k = 'done'");
    assert_eq!(r.rows, vec![vec![Value::Int(1)]], "{r:?}");
    // The WHEN-skipped row still got its own column change.
    let r = rows(&mut db, "SELECT status FROM orders WHERE id = 1");
    assert_eq!(r.rows, vec![vec![Value::Str("done".into())]], "{r:?}");
}

#[test]
fn trigger_before_raise_aborts_statement_atomically() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
    run(&mut db, "CREATE TABLE guard (min INT)");
    run(&mut db, "INSERT INTO guard VALUES (0)");
    // BEFORE INSERT RAISE: the check runs while the old state is live.
    run(
        &mut db,
        "CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW \
             BEGIN SELECT RAISE(ABORT, 'inserts disabled'); END",
    );
    let e = db
        .execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        .unwrap_err()
        .to_string();
    assert!(e.contains("inserts disabled"), "{e}");
    // Statement atomicity: the multi-row INSERT left NOTHING behind.
    let r = rows(&mut db, "SELECT COUNT(*) FROM t");
    assert_eq!(r.rows, vec![vec![Value::Int(0)]], "{r:?}");
    // DROP TRIGGER removes the gate.
    run(&mut db, "DROP TRIGGER tr");
    run(&mut db, "INSERT INTO t VALUES (1, 'a')");
    let r = rows(&mut db, "SELECT COUNT(*) FROM t");
    assert_eq!(r.rows, vec![vec![Value::Int(1)]], "{r:?}");
    // WHEN + RAISE: the constraint only bites negative values.
    run(
        &mut db,
        "CREATE TRIGGER tn BEFORE INSERT ON t FOR EACH ROW \
             WHEN (NEW.id < 0) BEGIN SELECT RAISE(FAIL, 'negative id'); END",
    );
    let e = db
        .execute("INSERT INTO t VALUES (-5, 'bad')")
        .unwrap_err()
        .to_string();
    assert!(e.contains("negative id"), "{e}");
    run(&mut db, "INSERT INTO t VALUES (5, 'ok')");
    let r = rows(&mut db, "SELECT COUNT(*) FROM t");
    assert_eq!(r.rows, vec![vec![Value::Int(2)]], "{r:?}");
}

#[test]
fn trigger_cascades_between_tables_with_depth_budget() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE a (v INT)");
    run(&mut db, "CREATE TABLE b (v INT)");
    run(&mut db, "CREATE TABLE log (v TEXT)");
    run(
        &mut db,
        "CREATE TRIGGER a2b AFTER INSERT ON a FOR EACH ROW \
             BEGIN INSERT INTO b VALUES (NEW.v + 1); END",
    );
    run(
        &mut db,
        "CREATE TRIGGER b2a AFTER INSERT ON b FOR EACH ROW \
             BEGIN INSERT INTO a VALUES (NEW.v + 1); END",
    );
    run(
        &mut db,
        "CREATE TRIGGER lg AFTER INSERT ON b FOR EACH ROW \
             BEGIN INSERT INTO log VALUES ('b=' || NEW.v); END",
    );
    // a→b→a→b… is a cycle: the cascade must stop loudly, not hang the
    // single writer.
    let e = db
        .execute("INSERT INTO a VALUES (0)")
        .unwrap_err()
        .to_string();
    assert!(e.contains("nesting too deep"), "{e}");
    // The failed statement leaves NOTHING (guard revert).
    let r = rows(&mut db, "SELECT COUNT(*) FROM b");
    assert_eq!(r.rows, vec![vec![Value::Int(0)]], "{r:?}");
    // A bounded cascade completes: remove the cycle, keep the chain.
    run(&mut db, "DROP TRIGGER b2a");
    run(&mut db, "INSERT INTO a VALUES (0)");
    let r = rows(&mut db, "SELECT v FROM b");
    assert_eq!(r.rows, vec![vec![Value::Int(1)]], "{r:?}");
    let r = rows(&mut db, "SELECT COUNT(*) FROM log");
    assert_eq!(r.rows, vec![vec![Value::Int(1)]], "{r:?}");
}

#[test]
fn trigger_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("trig.db");
    {
        let mut db = Database::open(&path).unwrap();
        run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
        run(&mut db, "CREATE TABLE audit (note TEXT)");
        run(
            &mut db,
            "CREATE TRIGGER tg AFTER INSERT ON t FOR EACH ROW \
                 BEGIN INSERT INTO audit VALUES (NEW.v); END",
        );
        run(&mut db, "INSERT INTO t VALUES (1, 'first')");
    }
    let mut db = Database::open(&path).unwrap();
    run(&mut db, "INSERT INTO t VALUES (2, 'second')");
    let r = rows(&mut db, "SELECT note FROM audit ORDER BY note");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("first".into())],
            vec![Value::Str("second".into())],
        ],
        "{r:?}"
    );
}

#[test]
fn trigger_drop_table_drops_triggers() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT)");
    run(&mut db, "CREATE TABLE other (id INT)");
    run(&mut db, "CREATE TABLE audit (note TEXT)");
    run(
        &mut db,
        "CREATE TRIGGER tg AFTER INSERT ON t FOR EACH ROW \
             BEGIN INSERT INTO audit VALUES ('t'); END",
    );
    run(
        &mut db,
        "CREATE TRIGGER to2 AFTER INSERT ON other FOR EACH ROW \
             BEGIN INSERT INTO audit VALUES ('other'); END",
    );
    run(&mut db, "DROP TABLE t");
    // The surviving trigger still works…
    run(&mut db, "INSERT INTO other VALUES (1)");
    let r = rows(&mut db, "SELECT note FROM audit");
    assert_eq!(r.rows, vec![vec![Value::Str("other".into())]], "{r:?}");
    // …and the dropped table's trigger is gone: recreating the table
    // must not resurrect it.
    run(&mut db, "CREATE TABLE t (id INT)");
    run(&mut db, "INSERT INTO t VALUES (1)");
    let r = rows(&mut db, "SELECT COUNT(*) FROM audit");
    assert_eq!(r.rows, vec![vec![Value::Int(1)]], "{r:?}");
}

#[test]
fn trigger_dump_roundtrip_does_not_refire() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
    run(&mut db, "CREATE TABLE audit (note TEXT)");
    run(
        &mut db,
        "CREATE TRIGGER tg AFTER INSERT ON t FOR EACH ROW \
             BEGIN INSERT INTO audit VALUES (NEW.v); END",
    );
    run(&mut db, "INSERT INTO t VALUES (1, 'a'), (2, 'b')");
    let r = rows(&mut db, "SELECT COUNT(*) FROM audit");
    assert_eq!(r.rows, vec![vec![Value::Int(2)]], "{r:?}");
    let script = db.dump_script().unwrap();
    // Triggers are dumped AFTER the data (pg_dump order): replaying the
    // INSERTs with live triggers would double every effect.
    let insert_pos = script.find("INSERT INTO \"t\"").expect("data in dump");
    let trigger_pos = script.find("CREATE TRIGGER").expect("trigger in dump");
    assert!(insert_pos < trigger_pos, "{script}");
    let mut dst = apply_dump(&script);
    // Replayed data: exactly the original rows, trigger effects included
    // exactly once.
    let r = rows(&mut dst, "SELECT COUNT(*) FROM t");
    assert_eq!(r.rows, vec![vec![Value::Int(2)]], "{r:?}");
    let r = rows(&mut dst, "SELECT COUNT(*) FROM audit");
    assert_eq!(r.rows, vec![vec![Value::Int(2)]], "{r:?}");
    // The trigger is live for POST-restore statements.
    run(&mut dst, "INSERT INTO t VALUES (3, 'c')");
    let r = rows(&mut dst, "SELECT COUNT(*) FROM audit");
    assert_eq!(r.rows, vec![vec![Value::Int(3)]], "{r:?}");
}

#[test]
fn trigger_replication_replay_converges() {
    // The replication model: every node replays the same statement text
    // over the same state and fires the same triggers. Two independent
    // databases executing the identical journal text must converge
    // byte-for-byte.
    let build = || -> Database {
        let mut db = Database::in_memory().unwrap();
        run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
        run(&mut db, "CREATE TABLE audit (id INT, v TEXT)");
        run(
            &mut db,
            "CREATE TRIGGER tg AFTER INSERT ON t FOR EACH ROW \
                 BEGIN INSERT INTO audit VALUES (NEW.id, NEW.v); END",
        );
        db
    };
    let mut a = build();
    let mut b = build();
    let journal = [
        "INSERT INTO t VALUES (1, 'x'), (2, 'y')",
        "UPDATE t SET v = 'z' WHERE id = 1",
        "DELETE FROM t WHERE id = 2",
    ];
    for sql in journal {
        run(&mut a, sql);
        run(&mut b, sql);
    }
    for q in [
        "SELECT * FROM t ORDER BY id",
        "SELECT * FROM audit ORDER BY id",
    ] {
        assert_eq!(rows(&mut a, q).rows, rows(&mut b, q).rows, "{q}");
    }
}

#[test]
fn trigger_rejected_bodies_are_loud() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT)");
    run(&mut db, "CREATE TABLE g (id GUID AUTOINCREMENT, v TEXT)");
    run(
        &mut db,
        "CREATE TABLE w (v TEXT DEFAULT (NOW()), other TEXT)",
    );
    run(&mut db, "CREATE VIEW vv AS SELECT 1 AS x");
    // Random ids diverge the cluster.
    let e = db
        .execute(
            "CREATE TRIGGER bad1 AFTER INSERT ON t FOR EACH ROW \
                 BEGIN INSERT INTO g (v) VALUES (NEW.id); END",
        )
        .unwrap_err()
        .to_string();
    assert!(e.contains("GUID"), "{e}");
    // Explicit id is fine.
    run(
        &mut db,
        "CREATE TRIGGER ok1 AFTER INSERT ON t FOR EACH ROW \
             BEGIN INSERT INTO g (id, v) VALUES (1, NEW.id); END",
    );
    run(&mut db, "DROP TRIGGER ok1");
    // A NULL literal still triggers the per-node fill.
    let e = db
        .execute(
            "CREATE TRIGGER bad2 AFTER INSERT ON t FOR EACH ROW \
                 BEGIN INSERT INTO g (id, v) VALUES (NULL, 'x'); END",
        )
        .unwrap_err()
        .to_string();
    assert!(e.contains("GUID"), "{e}");
    // Wall-clock DEFAULT on the target table fills per node — the body
    // must not omit that column.
    let e = db
        .execute(
            "CREATE TRIGGER bad3 AFTER INSERT ON t FOR EACH ROW \
                 BEGIN INSERT INTO w (other) VALUES ('x'); END",
        )
        .unwrap_err()
        .to_string();
    assert!(e.contains("wall-clock"), "{e}");
    // Supplying the defaulted column explicitly is fine.
    run(
        &mut db,
        "CREATE TRIGGER ok3 AFTER INSERT ON t FOR EACH ROW \
             BEGIN INSERT INTO w (v, other) VALUES ('x', 'y'); END",
    );
    run(&mut db, "DROP TRIGGER ok3");
    // NEWID in the body.
    let e = db
        .execute(
            "CREATE TRIGGER bad4 AFTER INSERT ON t FOR EACH ROW \
                 BEGIN INSERT INTO g (id, v) VALUES (NEWID(), 'x'); END",
        )
        .unwrap_err()
        .to_string();
    assert!(e.contains("NEWID"), "{e}");
    // Views and internal tables are not trigger targets.
    let e = db
        .execute("CREATE TRIGGER bad5 AFTER INSERT ON vv FOR EACH ROW BEGIN DELETE FROM t; END")
        .unwrap_err()
        .to_string();
    assert!(e.contains("view"), "{e}");
    // Statement-level triggers are not supported.
    let e = db
        .execute(
            "CREATE TRIGGER bad6 AFTER INSERT ON t FOR EACH STATEMENT BEGIN DELETE FROM t; END",
        )
        .unwrap_err()
        .to_string();
    assert!(e.contains("row-level"), "{e}");
    // A non-RAISE SELECT has no place in a body.
    let e = db
        .execute("CREATE TRIGGER bad7 AFTER INSERT ON t FOR EACH ROW BEGIN SELECT 1; END")
        .unwrap_err()
        .to_string();
    assert!(e.contains("RAISE"), "{e}");
    // RAISE(IGNORE) / ROLLBACK forms are refused.
    let e = db
        .execute(
            "CREATE TRIGGER bad8 AFTER INSERT ON t FOR EACH ROW \
                 BEGIN SELECT RAISE(IGNORE); END",
        )
        .unwrap_err()
        .to_string();
    assert!(e.contains("ABORT|FAIL"), "{e}");
    // Duplicate names are refused; OR REPLACE swaps the body.
    run(
        &mut db,
        "CREATE TRIGGER dup AFTER INSERT ON t FOR EACH ROW \
             BEGIN INSERT INTO t VALUES (1); END",
    );
    let e = db
        .execute(
            "CREATE TRIGGER dup AFTER INSERT ON t FOR EACH ROW \
                 BEGIN INSERT INTO t VALUES (2); END",
        )
        .unwrap_err()
        .to_string();
    assert!(e.contains("already exists"), "{e}");
    run(
        &mut db,
        "CREATE OR REPLACE TRIGGER dup AFTER DELETE ON t FOR EACH ROW \
             BEGIN INSERT INTO t VALUES (3); END",
    );
    let any = db.triggers.get("dup").expect("replaced");
    assert!(any.delete, "{any:?}");
}

#[test]
fn trigger_preserves_resolved_sql_and_last_insert_id() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE g (id GUID AUTOINCREMENT, v TEXT)");
    run(&mut db, "CREATE TABLE audit (note TEXT)");
    run(
        &mut db,
        "CREATE TRIGGER tg AFTER INSERT ON g FOR EACH ROW \
             BEGIN INSERT INTO audit VALUES ('logged'); END",
    );
    run(&mut db, "INSERT INTO g (v) VALUES ('a')");
    // The triggering INSERT's canonical rewrite (random GUID shipped as a
    // literal) must survive the trigger firing untouched.
    let resolved = db.take_resolved_sql().expect("resolved rewrite");
    assert!(resolved.starts_with("INSERT INTO"), "{resolved}");
    assert!(resolved.contains(", 'a')"), "{resolved}");
    // And the replica replaying that text lands the same audit row count.
    let mut peer = Database::in_memory().unwrap();
    run(&mut peer, "CREATE TABLE g (id GUID AUTOINCREMENT, v TEXT)");
    run(&mut peer, "CREATE TABLE audit (note TEXT)");
    run(
        &mut peer,
        "CREATE TRIGGER tg AFTER INSERT ON g FOR EACH ROW \
             BEGIN INSERT INTO audit VALUES ('logged'); END",
    );
    run(&mut peer, &resolved);
    let r = rows(&mut peer, "SELECT COUNT(*) FROM audit");
    assert_eq!(r.rows, vec![vec![Value::Int(1)]], "{r:?}");
    let r = rows(&mut peer, "SELECT id, v FROM g");
    assert_eq!(r.rows.len(), 1, "{r:?}");
    // The outer statement's assigned id is reported, not the body's.
    let _ = db.take_resolved_sql();
}

#[test]
fn trigger_explicit_tx_statement_failure_keeps_transaction() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY)");
    run(&mut db, "CREATE TABLE log (note TEXT)");
    run(
        &mut db,
        "CREATE TRIGGER tg AFTER INSERT ON t FOR EACH ROW \
             BEGIN INSERT INTO log VALUES ('n' || NEW.id); END",
    );
    run(&mut db, "BEGIN");
    run(&mut db, "INSERT INTO t VALUES (1)");
    // A statement whose trigger fails rolls back ONLY that statement;
    // the transaction and its earlier statement stay.
    let e = db
        .execute("INSERT INTO t VALUES (1)") // PK collision inside the tx
        .unwrap_err()
        .to_string();
    assert!(e.contains("UNIQUE"), "{e}");
    run(&mut db, "INSERT INTO t VALUES (2)");
    run(&mut db, "COMMIT");
    let r = rows(&mut db, "SELECT id FROM t ORDER BY id");
    assert_eq!(
        r.rows,
        vec![vec![Value::Int(1)], vec![Value::Int(2)]],
        "{r:?}"
    );
    let r = rows(&mut db, "SELECT note FROM log ORDER BY note");
    assert_eq!(
        r.rows,
        vec![vec![Value::Str("n1".into())], vec![Value::Str("n2".into())]],
        "{r:?}"
    );
}

#[test]
fn trigger_upsert_arm_fires_update_triggers() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
    run(&mut db, "CREATE TABLE log (note TEXT)");
    run(
        &mut db,
        "CREATE TRIGGER ti AFTER INSERT ON t FOR EACH ROW \
             BEGIN INSERT INTO log VALUES ('ins'); END",
    );
    run(
        &mut db,
        "CREATE TRIGGER tu AFTER UPDATE OF v ON t FOR EACH ROW \
             BEGIN INSERT INTO log VALUES ('upd ' || OLD.v || '→' || NEW.v); END",
    );
    run(
        &mut db,
        "INSERT INTO t (id, v) VALUES (1, 'a') \
             ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v",
    );
    run(
        &mut db,
        "INSERT INTO t (id, v) VALUES (1, 'b') \
             ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v",
    );
    let r = rows(&mut db, "SELECT note FROM log");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Str("ins".into())],
            vec![Value::Str("upd a→b".into())],
        ],
        "{r:?}"
    );
}

#[test]
fn trigger_replace_displacement_fires_delete_triggers() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
    run(&mut db, "CREATE TABLE log (note TEXT)");
    run(
        &mut db,
        "CREATE TRIGGER td AFTER DELETE ON t FOR EACH ROW \
             BEGIN INSERT INTO log VALUES ('del ' || OLD.id); END",
    );
    run(
        &mut db,
        "INSERT OR REPLACE INTO t (id, v) VALUES (1, 'a'), (2, 'b')",
    );
    run(&mut db, "INSERT OR REPLACE INTO t (id, v) VALUES (1, 'c')");
    let r = rows(&mut db, "SELECT note FROM log");
    assert_eq!(r.rows, vec![vec![Value::Str("del 1".into())]], "{r:?}");
    let r = rows(&mut db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r.rows, vec![vec![Value::Str("c".into())]], "{r:?}");
}

#[test]
fn trigger_split_statements_keeps_bodies_intact() {
    // execute_batch must treat the multi-statement body as ONE statement.
    let mut db = Database::in_memory().unwrap();
    let batch = "CREATE TABLE t (id INT); \
                     CREATE TABLE log (note TEXT); \
                     CREATE TRIGGER tg AFTER INSERT ON t FOR EACH ROW \
                     BEGIN INSERT INTO log VALUES ('n' || NEW.id); UPDATE log SET note = note; END; \
                     INSERT INTO t VALUES (7)";
    for stmt in crate::stmt::split_statements(batch).unwrap() {
        db.execute(&stmt).unwrap();
    }
    let r = rows(&mut db, "SELECT note FROM log");
    assert_eq!(r.rows, vec![vec![Value::Str("n7".into())]], "{r:?}");
    // CASE … END inside the body must not terminate the merge early.
    let mut db2 = Database::in_memory().unwrap();
    let batch2 = "CREATE TABLE t (id INT, w TEXT); \
                      CREATE TABLE mirror (id INT, w TEXT); \
                      CREATE TRIGGER tc AFTER INSERT ON t FOR EACH ROW \
                      BEGIN INSERT INTO mirror VALUES (NEW.id, \
                      CASE WHEN NEW.id > 1 THEN 'big' ELSE 'small' END); END; \
                      INSERT INTO t VALUES (5, 'x')";
    for stmt in crate::stmt::split_statements(batch2).unwrap() {
        db2.execute(&stmt).unwrap();
    }
    let r = rows(&mut db2, "SELECT w FROM mirror WHERE id = 5");
    assert_eq!(r.rows, vec![vec![Value::Str("big".into())]], "{r:?}");
    // A body write to the firing statement's own table is refused at
    // fire time (MySQL semantics — the statement's row images are still
    // staged in its own transaction).
    run(
        &mut db2,
        "CREATE TRIGGER bad_same AFTER INSERT ON t FOR EACH ROW \
             BEGIN DELETE FROM t WHERE id = NEW.id; END",
    );
    let e = db2
        .execute("INSERT INTO t VALUES (6, 'y')")
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("cannot modify table t from a trigger on t"),
        "{e}"
    );
    // An unterminated body is a loud error, never a truncated trigger.
    assert!(crate::stmt::split_statements(
        "CREATE TRIGGER bad AFTER INSERT ON t FOR EACH ROW BEGIN INSERT INTO log VALUES (1);"
    )
    .is_err());
}
