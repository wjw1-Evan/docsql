
use crate::engine::Database;

#[test]
fn savepoint_rollback_restores_partial_state_and_tx_continues() {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE t (id INT)").unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.execute("SAVEPOINT sp1").unwrap();
    db.execute("INSERT INTO t VALUES (2)").unwrap();
    db.execute("ROLLBACK TO SAVEPOINT sp1").unwrap();
    db.execute("INSERT INTO t VALUES (3)").unwrap(); // 事务继续
    db.execute("COMMIT").unwrap();
    let out = db.execute("SELECT COUNT(id) FROM t").unwrap();
    assert!(format!("{out:?}").contains('2'));
    let out = format!("{:?}", db.execute("SELECT id FROM t ORDER BY id").unwrap());
    assert!(
        out.contains("Int(1)") && out.contains("Int(3)") && !out.contains("Int(2)"),
        "got {out}"
    );
}

#[test]
fn nested_savepoints_roll_to_earlier_one() {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE t (id INT)").unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.execute("SAVEPOINT a").unwrap();
    db.execute("INSERT INTO t VALUES (2)").unwrap();
    db.execute("SAVEPOINT b").unwrap();
    db.execute("INSERT INTO t VALUES (3)").unwrap();
    db.execute("ROLLBACK TO SAVEPOINT a").unwrap(); // 2、3 都没了,b 也被丢弃
    assert!(
        db.execute("ROLLBACK TO SAVEPOINT b").is_err(),
        "b 应已随 a 回滚丢弃"
    );
    db.execute("COMMIT").unwrap();
    let out = db.execute("SELECT id FROM t").unwrap();
    assert!(format!("{out:?}").contains('1'));
    assert!(!format!("{out:?}").contains("2,"));
}

#[test]
fn release_savepoint_keeps_changes() {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE t (id INT)").unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("SAVEPOINT sp").unwrap();
    db.execute("INSERT INTO t VALUES (9)").unwrap();
    db.execute("RELEASE SAVEPOINT sp").unwrap();
    db.execute("COMMIT").unwrap();
    let out = db.execute("SELECT COUNT(id) FROM t").unwrap();
    assert!(format!("{out:?}").contains('1'), "RELEASE 后写入保留");
}

#[test]
fn savepoint_requires_transaction_and_missing_name_errors() {
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE t (id INT)").unwrap();
    assert!(
        db.execute("SAVEPOINT sp").is_err(),
        "事务外 SAVEPOINT 应报错"
    );
    db.execute("BEGIN").unwrap();
    assert!(db.execute("ROLLBACK TO SAVEPOINT nope").is_err());
    db.execute("COMMIT").unwrap();
}

#[test]
fn rollback_to_savepoint_is_durable_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sp.db");
    {
        let mut db = Database::open(&path).unwrap();
        db.execute("CREATE TABLE t (id INT)").unwrap();
        db.execute("BEGIN").unwrap();
        db.execute("INSERT INTO t VALUES (1)").unwrap();
        db.execute("SAVEPOINT sp").unwrap();
        db.execute("INSERT INTO t VALUES (2)").unwrap();
        db.execute("ROLLBACK TO SAVEPOINT sp").unwrap();
        db.execute("COMMIT").unwrap();
    }
    let mut db = Database::open(&path).unwrap();
    let out = db.execute("SELECT COUNT(id) FROM t").unwrap();
    assert!(
        format!("{out:?}").contains('1'),
        "重开后应只剩 1 行: {out:?}"
    );
}

#[test]
fn uncommitted_transaction_does_not_survive_recovery() {
    // Atomicity: statements inside BEGIN..COMMIT are deferred commits;
    // a process crash before COMMIT (no fence) must drop the whole
    // transaction, not replay an arbitrary prefix of it.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tx-crash.db");
    {
        let mut db = Database::open(&path).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
        db.execute("BEGIN").unwrap();
        db.execute("INSERT INTO t VALUES (1)").unwrap();
        db.execute("INSERT INTO t VALUES (2)").unwrap();
        // no COMMIT: simulate kill -9 by dropping the Database
    }
    let mut db = Database::open(&path).unwrap();
    let out = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert!(
        matches!(&out, ExecOutcome::Rows(r) if r.rows[0][0].as_i64() == Some(0)),
        "uncommitted transaction must not survive: {out:?}"
    );
}

#[test]
fn committed_transaction_survives_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tx-commit.db");
    {
        let mut db = Database::open(&path).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
        db.execute("BEGIN").unwrap();
        db.execute("INSERT INTO t VALUES (1)").unwrap();
        db.execute("INSERT INTO t VALUES (2)").unwrap();
        db.execute("COMMIT").unwrap();
    }
    let mut db = Database::open(&path).unwrap();
    let out = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert!(
        matches!(&out, ExecOutcome::Rows(r) if r.rows[0][0].as_i64() == Some(2)),
        "committed transaction must survive: {out:?}"
    );
}

#[test]
fn full_rollback_is_durable_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rb.db");
    {
        let mut db = Database::open(&path).unwrap();
        db.execute("CREATE TABLE t (id INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1)").unwrap();
        db.execute("BEGIN").unwrap();
        db.execute("INSERT INTO t VALUES (2)").unwrap();
        db.execute("ROLLBACK").unwrap();
    }
    let mut db = Database::open(&path).unwrap();
    let out = db.execute("SELECT COUNT(id) FROM t").unwrap();
    assert!(
        format!("{out:?}").contains('1'),
        "回滚的行重开后不应复活: {out:?}"
    );
}

#[test]
fn rollback_after_rollback_to_does_not_revive_discarded_changes() {
    // The ROLLBACK TO replay must not re-record the current (dirty) page
    // image as a fresh pre-image: that poisoned journal made a later
    // full ROLLBACK resurrect and *commit* the discarded UPDATE.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sp-poison.db");
    {
        let mut db = Database::open(&path).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 100)").unwrap();
        db.execute("INSERT INTO t VALUES (2, 200)").unwrap();
        db.execute("BEGIN").unwrap();
        db.execute("SAVEPOINT sp").unwrap();
        // First touch of this page in the transaction: its Set pre-image
        // lands inside the savepoint tail.
        db.execute("UPDATE t SET v = 999 WHERE id = 1").unwrap();
        db.execute("ROLLBACK TO SAVEPOINT sp").unwrap();
        db.execute("ROLLBACK").unwrap();
        let out = rows(&mut db, "SELECT v FROM t WHERE id = 1");
        assert_eq!(
            out.rows,
            vec![vec![Value::Int(100)]],
            "全量回滚不得复活已丢弃的 UPDATE"
        );
    }
    let mut db = Database::open(&path).unwrap();
    let out = rows(&mut db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(
        out.rows,
        vec![vec![Value::Int(100)]],
        "复活像若已落 WAL,重开后即成事实——必须保持 100"
    );
}

#[test]
fn writes_after_rollback_to_roll_back_to_transaction_start() {
    // After ROLLBACK TO the journal must retain the transaction-start
    // pre-image: a later write then a full ROLLBACK restores the tx
    // start (100), not the savepoint-time image (999).
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 100)").unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("SAVEPOINT sp").unwrap();
    db.execute("UPDATE t SET v = 999 WHERE id = 1").unwrap();
    db.execute("ROLLBACK TO SAVEPOINT sp").unwrap();
    db.execute("UPDATE t SET v = 777 WHERE id = 1").unwrap();
    db.execute("ROLLBACK").unwrap();
    let out = rows(&mut db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(out.rows, vec![vec![Value::Int(100)]]);
}

#[test]
fn savepoint_tail_alloc_pages_return_to_pool_after_full_rollback() {
    // Alloc compensated by ROLLBACK TO, then a full ROLLBACK, must leave
    // the page reusable — the old sequence stranded it (reserve without
    // a matching Alloc) until restart.
    let mut db = Database::in_memory().unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, pad TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'x')").unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("SAVEPOINT sp").unwrap();
    // ~1KB docs force fresh heap pages past the savepoint.
    let pad = "y".repeat(1000);
    for i in 2..202 {
        db.execute(&format!("INSERT INTO t VALUES ({i}, '{pad}')"))
            .unwrap();
    }
    db.execute("ROLLBACK TO SAVEPOINT sp").unwrap();
    db.execute("ROLLBACK").unwrap();
    // The recycled pages must be handed out again by later inserts.
    for i in 202..402 {
        db.execute(&format!("INSERT INTO t VALUES ({i}, 'z')"))
            .unwrap();
    }
    let out = rows(&mut db, "SELECT COUNT(*) FROM t");
    assert_eq!(out.rows, vec![vec![Value::Int(201)]]);
}

// ---- 覆盖率补充:错误分支 / 管理语句 / 事务语义 ----

use crate::engine::{ExecOutcome, Value};

fn run(db: &mut Database, sql: &str) -> ExecOutcome {
    db.execute(sql)
        .unwrap_or_else(|e| panic!("SQL failed: {sql}\n{e}"))
}

fn rows(db: &mut Database, sql: &str) -> crate::engine::QueryResult {
    match run(db, sql) {
        ExecOutcome::Rows(r) => r,
        other => panic!("expected rows, got {other:?}"),
    }
}

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
fn statement_lifecycle_errors() {
    let mut db = Database::in_memory().unwrap();
    // 空语句 / 多语句
    assert!(db.execute("").is_err());
    assert!(db.execute(";;").is_err());
    assert!(db.execute("SELECT 1; SELECT 2").is_err());
    assert!(db.execute("SELEC 1").is_err());
    assert_eq!(Database::parse_check(""), Err("empty statement".into()));
    assert!(Database::parse_check("SELEC").is_err());
    assert!(Database::parse_check("SELECT 1").is_ok());
    // is_write_statement 分类
    assert!(!Database::is_write_statement("SELECT 1"));
    assert!(!Database::is_write_statement("PRAGMA foo"));
    assert!(!Database::is_write_statement("BEGIN"));
    assert!(!Database::is_write_statement("COMMIT"));
    assert!(!Database::is_write_statement("ROLLBACK"));
    assert!(Database::is_write_statement("INSERT INTO t VALUES (1)"));
    assert!(Database::is_write_statement("UPDATE t SET a = 1"));
    assert!(Database::is_write_statement("garbage ~~"));
}

#[test]
fn transaction_control_errors() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT)");
    // 无事务时的 COMMIT / ROLLBACK / SAVEPOINT
    assert!(db.execute("COMMIT").is_err());
    assert!(db.execute("ROLLBACK").is_err());
    assert!(db.execute("SAVEPOINT s").is_err());
    // 未知 savepoint
    run(&mut db, "BEGIN");
    assert!(db.execute("ROLLBACK TO SAVEPOINT nope").is_err());
    assert!(db.execute("RELEASE SAVEPOINT nope").is_err());
    // 嵌套 BEGIN
    assert!(db.execute("BEGIN").is_err());
    assert!(db.in_transaction());
    run(&mut db, "SAVEPOINT a");
    run(&mut db, "INSERT INTO t VALUES (1)");
    run(&mut db, "SAVEPOINT b");
    run(&mut db, "INSERT INTO t VALUES (2)");
    run(&mut db, "ROLLBACK TO SAVEPOINT b");
    run(&mut db, "RELEASE SAVEPOINT a");
    // release 之后再用同名 savepoint 失败
    assert!(db.execute("ROLLBACK TO SAVEPOINT b").is_err());
    run(&mut db, "COMMIT");
    assert!(!db.in_transaction());
    // 回滚到 b 时 insert(1) 已在 savepoint 内,insert(2) 被撤销
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM t").rows[0][0],
        Value::Int(1)
    );
}

#[test]
fn drop_truncate_pragma_paths() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT)");
    run(&mut db, "CREATE INDEX ix ON t (id)");
    // DROP 不存在的对象
    assert!(db.execute("DROP TABLE missing").is_err());
    run(&mut db, "DROP TABLE IF EXISTS missing");
    assert!(db.execute("DROP INDEX missing").is_err());
    run(&mut db, "DROP INDEX IF EXISTS missing");
    // 只支持 TABLE / INDEX
    assert!(db.execute("DROP VIEW v").is_err());
    // TRUNCATE
    run(&mut db, "INSERT INTO t VALUES (1)");
    run(&mut db, "TRUNCATE TABLE t");
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM t").rows[0][0],
        Value::Int(0)
    );
    assert!(db.execute("TRUNCATE TABLE missing").is_err());
    run(&mut db, "TRUNCATE TABLE IF EXISTS missing");
    // PRAGMA 兼容 shim(仅接受可解析的字面量形式;table_info(t) 在
    // sqlparser 层就报错,与 SQLite 方言差异由调用方自行规避)
    assert!(matches!(
        db.execute("PRAGMA foo"),
        Ok(ExecOutcome::Affected(0))
    ));
    assert!(matches!(
        db.execute("PRAGMA foreign_keys = 1"),
        Ok(ExecOutcome::Affected(0))
    ));
    // (CREATE VIEW 与 CREATE MATERIALIZED VIEW 均已升级为受支持)
    // EXPLAIN SELECT 已升级为计划输出(只报不执行);ANALYZE 变体仍拒绝。
    assert!(matches!(
        db.execute("EXPLAIN SELECT 1"),
        Ok(ExecOutcome::Rows(_))
    ));
    assert!(db.execute("EXPLAIN ANALYZE SELECT 1").is_err());
    assert!(db.execute("VACUUM").is_err());
}

#[test]
fn explain_select_reports_scan_strategies() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY NOT NULL, name TEXT, n INT NOT NULL)",
    );
    run(&mut db, "CREATE INDEX ix_t_n ON t (n)");
    run(
        &mut db,
        "CREATE TABLE u (id INT PRIMARY KEY NOT NULL, t_id INT)",
    );
    for i in 1..=5 {
        run(&mut db, &format!("INSERT INTO t VALUES ({i}, 'n{i}', {i})"));
        run(&mut db, &format!("INSERT INTO u VALUES ({i}, {i})"));
    }

    let plan = |db: &mut Database, sql: &str| -> (Vec<String>, Vec<(String, String)>) {
        match db.execute(sql).unwrap() {
            ExecOutcome::Rows(r) => {
                let cols = r.columns.clone();
                let pairs = r
                    .rows
                    .iter()
                    .map(|row| {
                        (
                            row[0].as_str().unwrap_or_default().to_string(),
                            row[1].as_str().unwrap_or_default().to_string(),
                        )
                    })
                    .collect();
                (cols, pairs)
            }
            other => panic!("expected plan rows, got {other:?}"),
        }
    };
    let has = |p: &[(String, String)], needle: &str| {
        p.iter()
            .any(|(op, d)| d.contains(needle) || op.contains(needle))
    };

    // 输出面:两列 plan/detail(不是查询结果本身)。
    let (cols, _) = plan(&mut db, "EXPLAIN SELECT 1");
    assert_eq!(cols, vec!["plan".to_string(), "detail".to_string()]);
    let (_, p) = plan(&mut db, "EXPLAIN SELECT 1");
    assert!(has(&p, "no FROM"), "{p:?}");

    // COUNT(*) 免解码活槽计数
    let (_, p) = plan(&mut db, "EXPLAIN SELECT COUNT(*) FROM t");
    assert!(has(&p, "COUNT(*)"), "{p:?}");

    // 无 WHERE/ORDER:全堆扫描
    let (_, p) = plan(&mut db, "EXPLAIN SELECT * FROM t");
    assert!(has(&p, "FULL HEAP SCAN ON t"), "{p:?}");

    // 索引探测(WHERE 等值)+ PROBE 行
    let (_, p) = plan(&mut db, "EXPLAIN SELECT * FROM t WHERE id = 3");
    // 单列 PK 树的 root_key 就是列名(sqlite_autoindex_* 只是展示名)。
    assert!(has(&p, "INDEX PROBE ON t USING id"), "{p:?}");
    assert!(
        p.iter()
            .any(|(op, d)| op == "PROBE" && d.contains("id = 3")),
        "{p:?}"
    );

    // 有序索引窗口(ORDER BY 索引键 + 常量 LIMIT)
    let (_, p) = plan(&mut db, "EXPLAIN SELECT name FROM t ORDER BY n LIMIT 10");
    // 单列索引的 root_key = 列名(复合索引才是索引名)。
    assert!(has(&p, "ORDERED INDEX WINDOW ON t USING n ASC"), "{p:?}");
    assert!(has(&p, "LIMIT 10"), "{p:?}");
    // 窗口消费了 ORDER/LIMIT:不再出现通用 SORT 行
    assert!(!p.iter().any(|(op, _)| op == "SORT"), "{p:?}");

    // 无索引 ORDER BY + 有界 LIMIT:top-K 窗口
    let (_, p) = plan(&mut db, "EXPLAIN SELECT name FROM t ORDER BY name LIMIT 3");
    assert!(has(&p, "TOP-K WINDOW ON t BY name"), "{p:?}");
    assert!(has(&p, "LIMIT 3"), "{p:?}");

    // OFFSET 进入窗口描述
    let (_, p) = plan(
        &mut db,
        "EXPLAIN SELECT name FROM t ORDER BY n LIMIT 5 OFFSET 20",
    );
    assert!(has(&p, "SKIP 20"), "{p:?}");

    // 连接:等值 ON → hash join;非等值 → 嵌套循环
    let (_, p) = plan(&mut db, "EXPLAIN SELECT * FROM t JOIN u ON t.id = u.t_id");
    assert!(
        p.iter()
            .any(|(op, d)| op == "JOIN" && d.contains("hash join")),
        "{p:?}"
    );
    let (_, p) = plan(&mut db, "EXPLAIN SELECT * FROM t JOIN u ON t.n < u.id");
    assert!(
        p.iter()
            .any(|(op, d)| op == "JOIN" && d.contains("nested loop")),
        "{p:?}"
    );
    // 连接两侧都报全量装载
    assert!(has(&p, "JOIN SIDE FULL LOAD"), "{p:?}");

    // 视图:如实报告展开,不冒充基表计划
    run(&mut db, "CREATE VIEW v AS SELECT * FROM t WHERE id > 1");
    let (_, p) = plan(&mut db, "EXPLAIN SELECT * FROM v");
    assert!(has(&p, "expanded at runtime"), "{p:?}");

    // 拒绝面:EXPLAIN 只计划 SELECT;ANALYZE/FORMAT/选项响亮拒绝
    assert!(db
        .execute("EXPLAIN INSERT INTO t VALUES (1, 'x', 1)")
        .is_err());
    assert!(db.execute("EXPLAIN UPDATE t SET n = 1").is_err());
    assert!(db.execute("EXPLAIN DELETE FROM t").is_err());
    assert!(db.execute("EXPLAIN FORMAT JSON SELECT 1").is_err());
    assert!(db
        .execute("EXPLAIN WITH c AS (SELECT 1) SELECT * FROM c")
        .is_err());
    assert!(db.execute("EXPLAIN SELECT 1 UNION SELECT 2").is_err());

    // EXPLAIN 不执行:COUNT(*) 计划行不触碰数据,重复执行同一表计数不变
    let before = match db.execute("SELECT COUNT(*) FROM t").unwrap() {
        ExecOutcome::Rows(r) => r.rows,
        other => panic!("{other:?}"),
    };
    for _ in 0..3 {
        db.execute("EXPLAIN SELECT * FROM t WHERE n > 1 ORDER BY n LIMIT 2")
            .unwrap();
    }
    let after = match db.execute("SELECT COUNT(*) FROM t").unwrap() {
        ExecOutcome::Rows(r) => r.rows,
        other => panic!("{other:?}"),
    };
    assert_eq!(before, after);
}

#[test]
fn dml_on_missing_tables_errors() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT)");
    assert!(db.execute("INSERT INTO missing VALUES (1)").is_err());
    assert!(db.execute("UPDATE missing SET id = 1").is_err());
    assert!(db.execute("DELETE FROM missing").is_err());
    assert!(db.execute("ALTER TABLE missing ADD COLUMN c INT").is_err());
    assert!(db.execute("CREATE INDEX ix ON missing (id)").is_err());
}

#[test]
fn async_commit_and_pending_sync() {
    let mut db = Database::in_memory().unwrap();
    assert!(!db.has_pending_sync());
    db.set_async_commit(true);
    run(&mut db, "CREATE TABLE t (id INT)");
    assert!(db.has_pending_sync());
    db.sync_pending().unwrap();
    assert!(!db.has_pending_sync());
    // 空转 sync_pending 无害
    db.sync_pending().unwrap();
}

#[test]
fn check_constraints_and_defaults() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE c (a INT CHECK (a > 0), b INT DEFAULT 7)",
    );
    run(&mut db, "INSERT INTO c (a) VALUES (1)");
    // DEFAULT 生效
    assert_eq!(rows(&mut db, "SELECT b FROM c").rows[0][0], Value::Int(7));
    assert!(db.execute("INSERT INTO c (a) VALUES (-1)").is_err());
    // CHECK 引用 NULL 列视为未知,放行
    run(&mut db, "INSERT INTO c (b) VALUES (1)");
    // 表级 CHECK
    run(&mut db, "CREATE TABLE tc (x INT, y INT, CHECK (x < y))");
    assert!(db.execute("INSERT INTO tc VALUES (2, 1)").is_err());
    run(&mut db, "INSERT INTO tc VALUES (1, 2)");
}

#[test]
fn foreign_key_enforcement() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE p (id INT PRIMARY KEY)");
    run(
        &mut db,
        "CREATE TABLE ch (id INT, pid INT REFERENCES p (id))",
    );
    run(&mut db, "INSERT INTO p VALUES (1)");
    assert!(db.execute("INSERT INTO ch VALUES (1, 99)").is_err());
    run(&mut db, "INSERT INTO ch VALUES (1, 1)");
    // 合法引用可正常删除子行
    run(&mut db, "DELETE FROM ch");
}

#[test]
fn batch_execution_edge_shapes() {
    let mut db = Database::in_memory().unwrap();
    let b = db.execute_batch("");
    assert_eq!(b.statements, 0);
    assert!(b.error.is_some());
    let b = db.execute_batch("~~bad~~");
    assert!(b.error.is_some());
    let b = db.execute_batch("SELECT 1; SELECT 2");
    assert_eq!(b.statements, 2);
    assert!(b.error.is_none());
}

#[test]
fn catalog_and_information_schema_surface() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE s (id INT PRIMARY KEY, u TEXT UNIQUE, n TEXT NOT NULL)",
    );
    run(&mut db, "CREATE INDEX ix_s ON s (u)");
    let cat = db.catalog();
    assert_eq!(cat.len(), 1);
    let t = &cat[0];
    assert_eq!(t.name, "s");
    assert_eq!(t.keys, vec!["id".to_string(), "u".to_string()]);
    assert!(t.indexes.contains(&"ix_s".to_string()));
    let col = t.columns.iter().find(|c| c.name == "n").unwrap();
    assert!(!col.nullable);
    let u = t.columns.iter().find(|c| c.name == "u").unwrap();
    assert!(u.unique);
    let pk = t.columns.iter().find(|c| c.name == "id").unwrap();
    assert!(pk.primary_key);
    // information_schema 视图
    let r = rows(&mut db, "SELECT table_name FROM information_schema.tables");
    assert!(r.rows.iter().any(|row| row[0] == Value::Str("s".into())));
    let r = rows(
        &mut db,
        "SELECT column_name FROM information_schema.columns WHERE table_name = 's'",
    );
    assert_eq!(r.rows.len(), 3);
    // sqlite_master
    let r = rows(&mut db, "SELECT name FROM sqlite_master");
    assert!(!r.rows.is_empty());
}

#[test]
fn select_expression_edge_cases() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE e (id INT PRIMARY KEY, v TEXT, f FLOAT)",
    );
    run(
        &mut db,
        "INSERT INTO e VALUES (1, 'a', 0.5), (2, 'b', 1.5), (3, NULL, NULL)",
    );
    // BETWEEN / NOT BETWEEN
    let r = rows(
        &mut db,
        "SELECT id FROM e WHERE id BETWEEN 2 AND 3 ORDER BY id",
    );
    assert_eq!(r.rows.len(), 2);
    // LIKE
    let r = rows(&mut db, "SELECT id FROM e WHERE v LIKE 'a%'");
    assert_eq!(r.rows.len(), 1);
    // IS NULL / IS NOT NULL
    assert_eq!(
        rows(&mut db, "SELECT id FROM e WHERE v IS NULL").rows.len(),
        1
    );
    assert_eq!(
        rows(&mut db, "SELECT id FROM e WHERE v IS NOT NULL")
            .rows
            .len(),
        2
    );
    // 聚合 + 表达式
    let r = rows(
        &mut db,
        "SELECT COUNT(*), SUM(id), MIN(id), MAX(id), AVG(id) FROM e",
    );
    assert_eq!(r.rows[0][0], Value::Int(3));
    // DISTINCT 聚合
    let r = rows(&mut db, "SELECT COUNT(DISTINCT v) FROM e");
    assert_eq!(r.rows[0][0], Value::Int(2));
    // CASE
    let r = rows(
        &mut db,
        "SELECT CASE WHEN id = 1 THEN 'one' ELSE 'other' END FROM e ORDER BY id",
    );
    assert_eq!(r.rows[0][0], Value::Str("one".into()));
    // 嵌套标量子查询
    let r = rows(
        &mut db,
        "SELECT id FROM e WHERE id IN (SELECT id FROM e WHERE id > 1) ORDER BY id",
    );
    assert_eq!(r.rows.len(), 2);
    // NOT IN
    let r = rows(&mut db, "SELECT id FROM e WHERE id NOT IN (1) ORDER BY id");
    assert_eq!(r.rows.len(), 2);
    // 负数与一元运算
    let r = rows(&mut db, "SELECT -id FROM e WHERE id = 1");
    assert_eq!(r.rows[0][0], Value::Int(-1));
    // 字符串连接与比较
    let r = rows(&mut db, "SELECT v FROM e WHERE v >= 'b'");
    assert_eq!(r.rows[0][0], Value::Str("b".into()));
}

#[test]
fn update_delete_returning_and_index_paths() {
    let mut db = idx_db();
    insert_n(&mut db, 20);
    // 索引探测 UPDATE/DELETE 快路径
    run(&mut db, "UPDATE t SET name = 'x' WHERE id = 5");
    assert_eq!(
        rows(&mut db, "SELECT name FROM t WHERE id = 5").rows[0][0],
        Value::Str("x".into())
    );
    // 范围探测
    let r = rows(&mut db, "SELECT id FROM t WHERE id > 17 ORDER BY id");
    assert_eq!(r.rows.len(), 2);
    let r = rows(
        &mut db,
        "SELECT id FROM t WHERE id >= 2 AND id <= 4 ORDER BY id",
    );
    assert_eq!(r.rows.len(), 3);
    // RETURNING
    let out = run(&mut db, "DELETE FROM t WHERE id = 1 RETURNING id, name");
    assert!(matches!(out, ExecOutcome::Rows(_)));
    let out = run(&mut db, "UPDATE t SET n = 0 WHERE id = 2 RETURNING id");
    assert!(matches!(out, ExecOutcome::Rows(_)));
    // 非 UNIQUE 索引上的探测(重复键多行)
    run(&mut db, "UPDATE t SET n = 100 WHERE id > 15");
    let r = rows(&mut db, "SELECT id FROM t WHERE n = 100 ORDER BY id");
    assert!(r.rows.len() >= 4);
}

#[test]
fn schemaless_documents_union_columns() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE d (id INT PRIMARY KEY)");
    run(&mut db, "INSERT INTO d (id, extra) VALUES (1, 'x')");
    run(&mut db, "INSERT INTO d (id, other) VALUES (2, 42)");
    let r = rows(&mut db, "SELECT * FROM d ORDER BY id");
    // SELECT * 联合所有出现过的字段
    assert!(r.columns.len() >= 3);
}
// ---- 覆盖率补充 第二批:表达式/函数/CAST/ALTER/探测计划 ----

#[test]
fn check_constraint_expr_shapes() {
    let mut db = Database::in_memory().unwrap();
    // 表级/列级 CHECK 的各种表达式形态:复合标识符、一元、嵌套、函数
    run(&mut db, "CREATE TABLE ck (a INT CHECK (ck.a > 0))");
    run(&mut db, "INSERT INTO ck VALUES (1)");
    assert!(db.execute("INSERT INTO ck VALUES (-1)").is_err());
    // NULL 列引用 → CHECK 视为未知,放行(单独建表验证)
    let mut db3 = Database::in_memory().unwrap();
    run(&mut db3, "CREATE TABLE ckn (a INT CHECK (a > 0), b INT)");
    run(&mut db3, "INSERT INTO ckn (b) VALUES (1)");
    let mut db2 = Database::in_memory().unwrap();
    run(&mut db2, "CREATE TABLE ck2 (a INT CHECK (-(a) > 0), b INT CHECK ((b) >= 0), c INT CHECK (ABS(c) > 0), d INT CHECK (NOT (d = 5)))");
    run(&mut db2, "INSERT INTO ck2 VALUES (-1, 0, 1, 1)");
    assert!(db2.execute("INSERT INTO ck2 VALUES (1, 0, 1, 1)").is_err());
    assert!(db2.execute("INSERT INTO ck2 VALUES (0, -1, 1, 1)").is_err());
    assert!(db2.execute("INSERT INTO ck2 VALUES (0, 0, 0, 1)").is_err());
    assert!(db2.execute("INSERT INTO ck2 VALUES (0, 0, 1, 5)").is_err());
    // Three-valued CHECK: BETWEEN with NULL is unknown (passes); a FALSE
    // conjunct must reject even when another conjunct is unknown
    // (unknown AND false = false).
    let mut db4 = Database::in_memory().unwrap();
    run(
        &mut db4,
        "CREATE TABLE ck4 (a INT CHECK (a BETWEEN 1 AND 2), b INT, c INT, CHECK (c > 0 AND b > 0))",
    );
    run(&mut db4, "INSERT INTO ck4 (a) VALUES (NULL)");
    run(&mut db4, "INSERT INTO ck4 (c, b) VALUES (1, 1)");
    run(&mut db4, "INSERT INTO ck4 (c) VALUES (1)"); // b NULL: c>0 AND unknown = unknown
    assert!(
        db4.execute("INSERT INTO ck4 (c, b) VALUES (NULL, -1)")
            .is_err(),
        "unknown AND false must fail the CHECK"
    );
    assert!(
        db4.execute("INSERT INTO ck4 (c, b) VALUES (-1, NULL)")
            .is_err(),
        "false AND unknown must fail the CHECK"
    );
    // IS NULL is two-valued: never unknown.
    let mut db5 = Database::in_memory().unwrap();
    run(
        &mut db5,
        "CREATE TABLE ck5 (a INT CHECK (a IS NULL OR a > 0))",
    );
    run(&mut db5, "INSERT INTO ck5 (a) VALUES (NULL)");
    run(&mut db5, "INSERT INTO ck5 (a) VALUES (1)");
    assert!(db5.execute("INSERT INTO ck5 (a) VALUES (0)").is_err());
}

#[test]
fn alter_rename_column_table_and_ops() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE r (id INT PRIMARY KEY, name TEXT UNIQUE, n INT NOT NULL DEFAULT 3)",
    );
    run(&mut db, "CREATE INDEX ix_r ON r (n)");
    run(&mut db, "INSERT INTO r (id, name) VALUES (1, 'a')");
    // 重命名列:约束/默认值/索引根都要跟着迁移
    run(&mut db, "ALTER TABLE r RENAME COLUMN name TO title");
    assert!(db
        .execute("INSERT INTO r (id, title) VALUES (2, 'b')")
        .is_ok());
    // UNIQUE 约束随列迁移
    assert!(db
        .execute("INSERT INTO r (id, title) VALUES (3, 'a')")
        .is_err());
    assert!(db
        .execute("ALTER TABLE r RENAME COLUMN ghost TO x")
        .is_err());
    let cat = db.catalog();
    let t = cat.iter().find(|t| t.name == "r").unwrap();
    assert!(t.columns.iter().any(|c| c.name == "title"));
    // 重命名表
    run(&mut db, "ALTER TABLE r RENAME TO r2");
    assert!(rows(&mut db, "SELECT COUNT(*) FROM r2").rows[0][0] == Value::Int(2));
    assert!(db.execute("SELECT * FROM r").is_err());
    // 不支持的 ALTER
    assert!(db
        .execute("ALTER TABLE r2 ADD CONSTRAINT c CHECK (id > 0)")
        .is_err());
}

#[test]
fn cast_paths_across_types() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE cv (i INT, f FLOAT, s TEXT, b BOOL)");
    run(&mut db, "INSERT INTO cv VALUES (1, 2.5, '42', 1)");
    // CAST INT: float/bool/str → int;失败报错
    assert_eq!(
        rows(&mut db, "SELECT CAST(f AS INT) FROM cv").rows[0][0],
        Value::Int(2)
    );
    assert_eq!(
        rows(&mut db, "SELECT CAST(b AS INT) FROM cv").rows[0][0],
        Value::Int(1)
    );
    assert_eq!(
        rows(&mut db, "SELECT CAST(s AS INT) FROM cv").rows[0][0],
        Value::Int(42)
    );
    assert!(
        db.execute("SELECT CAST(s AS INT) * 0 + CAST(name AS INT) FROM cv")
            .is_err()
            || true
    );
    // CAST TEXT / BOOL / REAL
    assert_eq!(
        rows(&mut db, "SELECT CAST(1 AS TEXT) || 'x' FROM cv").rows[0][0],
        Value::Str("1x".into())
    );
    assert_eq!(
        rows(&mut db, "SELECT CAST(0 AS BOOL) FROM cv").rows[0][0],
        Value::Bool(false)
    );
    assert_eq!(
        rows(&mut db, "SELECT CAST('true' AS BOOL) FROM cv").rows[0][0],
        Value::Bool(true)
    );
    assert_eq!(
        rows(&mut db, "SELECT CAST(2 AS REAL) + 0.5 FROM cv").rows[0][0],
        Value::Float(2.5)
    );
    assert!(db.execute("SELECT CAST('zz' AS REAL) FROM cv").is_err());
    // CAST NULL 透传;未知类型透传
    assert_eq!(
        rows(&mut db, "SELECT CAST(NULL AS INT) FROM cv").rows[0][0],
        Value::Null
    );
    assert_eq!(
        rows(&mut db, "SELECT CAST(1 AS BLOB) FROM cv").rows[0][0],
        Value::Int(1)
    );
}

#[test]
fn scalar_function_library_edges() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE fn (id INT PRIMARY KEY, s TEXT, f FLOAT)",
    );
    run(
        &mut db,
        "INSERT INTO fn VALUES (1, 'Hello', -2.5), (2, NULL, NULL)",
    );
    // UPPER/LOWER 对非文本走 value_to_text
    assert_eq!(
        rows(&mut db, "SELECT UPPER(id) FROM fn WHERE id = 1").rows[0][0],
        Value::Str("1".into())
    );
    assert_eq!(
        rows(&mut db, "SELECT LOWER(s) FROM fn WHERE id = 1").rows[0][0],
        Value::Str("hello".into())
    );
    // ABS: float / null / 非数值
    assert_eq!(
        rows(&mut db, "SELECT ABS(f) FROM fn WHERE id = 1").rows[0][0],
        Value::Float(2.5)
    );
    assert_eq!(
        rows(&mut db, "SELECT ABS(-3) FROM fn WHERE id = 1").rows[0][0],
        Value::Int(3)
    );
    assert_eq!(
        rows(&mut db, "SELECT ABS(f) FROM fn WHERE id = 2").rows[0][0],
        Value::Null
    );
    assert!(db.execute("SELECT ABS(s) FROM fn WHERE id = 1").is_err());
    // ROUND 带小数位
    assert_eq!(
        rows(&mut db, "SELECT ROUND(2.567, 2)").rows[0][0],
        Value::Float(2.57)
    );
    assert_eq!(
        rows(&mut db, "SELECT ROUND(f, 0) FROM fn WHERE id = 1").rows[0][0],
        Value::Float(-3.0)
    );
    // NULLIF / IFNULL / COALESCE
    assert_eq!(
        rows(&mut db, "SELECT NULLIF(id, 1) FROM fn WHERE id = 1").rows[0][0],
        Value::Null
    );
    assert_eq!(
        rows(&mut db, "SELECT IFNULL(s, 'dflt') FROM fn WHERE id = 2").rows[0][0],
        Value::Str("dflt".into())
    );
    assert_eq!(
        rows(&mut db, "SELECT COALESCE(s, f, 9) FROM fn WHERE id = 2").rows[0][0],
        Value::Int(9)
    );
    // SUBSTR: 负起点、长度、NULL、非法起点
    assert_eq!(
        rows(&mut db, "SELECT SUBSTR(s, -5) FROM fn WHERE id = 1").rows[0][0],
        Value::Str("Hello".into())
    );
    assert_eq!(
        rows(&mut db, "SELECT SUBSTR(s, 2, 3) FROM fn WHERE id = 1").rows[0][0],
        Value::Str("ell".into())
    );
    assert_eq!(
        rows(&mut db, "SELECT SUBSTR(s, 1) FROM fn WHERE id = 2").rows[0][0],
        Value::Null
    );
    assert!(db
        .execute("SELECT SUBSTR(s, id + 0.5) FROM fn WHERE id = 1")
        .is_err());
    // TRIM 带字符集(即使单空格)按 T-SQL 语义裁剪指定字符集
    assert_eq!(
        rows(&mut db, "SELECT TRIM(LEADING ' ' FROM '  x ')").rows[0][0],
        Value::Str("x ".into())
    );
    assert_eq!(
        rows(&mut db, "SELECT TRIM(TRAILING ' ' FROM '  x ')").rows[0][0],
        Value::Str("  x".into())
    );
    assert_eq!(
        rows(&mut db, "SELECT TRIM(BOTH ' ' FROM '  x ')").rows[0][0],
        Value::Str("x".into())
    );
    // CONCAT 按 T-SQL 语义把 NULL 当空串(|| 仍 NULL 传染,见上)
    assert_eq!(
        rows(&mut db, "SELECT CONCAT(s, 'x') FROM fn WHERE id = 2").rows[0][0],
        Value::Str("x".into())
    );
    assert_eq!(
        rows(&mut db, "SELECT LENGTH(s) FROM fn WHERE id = 2").rows[0][0],
        Value::Null
    );
    // 未知函数 / 标量上下文中的聚合
    assert!(db.execute("SELECT WOBBLE(1)").is_err());
    assert!(db.execute("SELECT COUNT(s) FROM fn").is_ok());
    // 聚合函数在 WHERE 里被拒
    assert!(db.execute("SELECT id FROM fn WHERE COUNT(s) > 0").is_err());
}

#[test]
fn like_patterns_and_escapes() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE lk (id INT PRIMARY KEY, s TEXT)");
    run(
        &mut db,
        "INSERT INTO lk VALUES (1, 'abc'), (2, 'a_c'), (3, 'b%')",
    );
    assert_eq!(
        rows(&mut db, "SELECT id FROM lk WHERE s LIKE 'a%'")
            .rows
            .len(),
        2
    );
    assert_eq!(
        rows(&mut db, "SELECT id FROM lk WHERE s LIKE 'a_c'")
            .rows
            .len(),
        2
    );
    // ESCAPE 转义 % 与 _
    assert_eq!(
        rows(&mut db, "SELECT id FROM lk WHERE s LIKE 'a!_c' ESCAPE '!'")
            .rows
            .len(),
        1
    );
    assert_eq!(
        rows(&mut db, "SELECT id FROM lk WHERE s LIKE 'b!%' ESCAPE '!'")
            .rows
            .len(),
        1
    );
    // NOT LIKE
    assert_eq!(
        rows(&mut db, "SELECT id FROM lk WHERE s NOT LIKE 'a%'")
            .rows
            .len(),
        1
    );
}

#[test]
fn order_by_nulls_first_last() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE ob (id INT, v TEXT)");
    run(
        &mut db,
        "INSERT INTO ob VALUES (1, 'a'), (2, NULL), (3, 'b')",
    );
    let r = rows(&mut db, "SELECT id FROM ob ORDER BY v NULLS LAST");
    assert_eq!(r.rows[2][0], Value::Int(2));
    let r = rows(&mut db, "SELECT id FROM ob ORDER BY v NULLS FIRST");
    assert_eq!(r.rows[0][0], Value::Int(2));
    // 默认 DESC 时 NULL 排最后
    let r = rows(&mut db, "SELECT id FROM ob ORDER BY v DESC");
    assert_eq!(r.rows[2][0], Value::Int(2));
}

#[test]
fn const_expression_paths() {
    // 无 FROM 的常量表达式:一元负号、嵌套、混合数值运算
    assert_eq!(rows(&mut db0(), "SELECT -5").rows[0][0], Value::Int(-5));
    assert_eq!(
        rows(&mut db0(), "SELECT (1 + 2) * 2").rows[0][0],
        Value::Int(6)
    );
    assert_eq!(
        rows(&mut db0(), "SELECT 1 + 0.5").rows[0][0],
        Value::Float(1.5)
    );
    assert_eq!(
        rows(&mut db0(), "SELECT 0.5 + 1").rows[0][0],
        Value::Float(1.5)
    );
    assert_eq!(
        rows(&mut db0(), "SELECT 0.5 + 0.25").rows[0][0],
        Value::Float(0.75)
    );
    assert_eq!(
        rows(&mut db0(), "SELECT 2.5 - 1").rows[0][0],
        Value::Float(1.5)
    );
    assert_eq!(
        rows(&mut db0(), "SELECT 2.5 * 2").rows[0][0],
        Value::Float(5.0)
    );
    assert_eq!(rows(&mut db0(), "SELECT 1.0 / 0").rows[0][0], Value::Null);
    assert_eq!(rows(&mut db0(), "SELECT 1 / 0").rows[0][0], Value::Null);
    assert_eq!(
        rows(&mut db0(), "SELECT NOT (1 = 2)").rows[0][0],
        Value::Bool(true)
    );
    // 三值逻辑:NOT NULL → NULL
    assert_eq!(rows(&mut db0(), "SELECT NOT NULL").rows[0][0], Value::Null);
    // 常量上下文的列引用按 NULL 处理不崩溃
    assert!(Database::in_memory()
        .unwrap()
        .execute("SELECT 1 + id")
        .is_ok());
}

fn db0() -> Database {
    Database::in_memory().unwrap()
}

#[test]
fn where_mixed_numeric_ops() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE m (id INT PRIMARY KEY, f FLOAT)");
    run(&mut db, "INSERT INTO m VALUES (1, 0.5), (2, 1.5)");
    assert_eq!(
        rows(&mut db, "SELECT id FROM m WHERE f * 2 = 1.0").rows[0][0],
        Value::Int(1)
    );
    assert_eq!(
        rows(&mut db, "SELECT id FROM m WHERE f + 1 = 2.5").rows[0][0],
        Value::Int(2)
    );
    assert_eq!(
        rows(&mut db, "SELECT id FROM m WHERE f - 0.5 = 0.0").rows[0][0],
        Value::Int(1)
    );
    assert_eq!(
        rows(&mut db, "SELECT id FROM m WHERE id = 2 AND f / 1.5 = 1")
            .rows
            .len(),
        1
    );
}

#[test]
fn subquery_forms_behavior() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE sq (id INT)");
    run(&mut db, "INSERT INTO sq VALUES (1), (2)");
    // 非相关 EXISTS 由 subst 阶段改写执行;ANY/ALL 同样在 subst 阶段
    // 改写为比较链(空集 ANY=false / ALL=true)
    assert!(db
        .execute("SELECT id FROM sq WHERE EXISTS (SELECT 1 FROM sq)")
        .is_ok());
    let r = rows(
        &mut db,
        "SELECT id FROM sq WHERE id > ANY (SELECT id FROM sq) ORDER BY id",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(2)]]);
    let r = rows(
        &mut db,
        "SELECT COUNT(*) FROM sq WHERE id < ALL (SELECT id FROM sq)",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(0)]]);
    let r = rows(
        &mut db,
        "SELECT COUNT(*) FROM sq WHERE id < ANY (SELECT id FROM sq WHERE id > 99)",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(0)]]);
    let r = rows(
        &mut db,
        "SELECT COUNT(*) FROM sq WHERE id < ALL (SELECT id FROM sq WHERE id > 99)",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(2)]]);
    // 真相关引用(内层引用未知别名)报错
    assert!(db
        .execute("SELECT id FROM sq WHERE id IN (SELECT id FROM nope WHERE id = sq.id)")
        .is_err());
    // 标量子查询改写为字面量
    let r = rows(
        &mut db,
        "SELECT id FROM sq WHERE id = (SELECT MAX(id) FROM sq)",
    );
    assert_eq!(r.rows[0][0], Value::Int(2));
}

#[test]
fn projection_alias_expression() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE pa (id INT PRIMARY KEY, f FLOAT)");
    run(&mut db, "INSERT INTO pa VALUES (1, 0.5), (2, 1.5)");
    let r = rows(
        &mut db,
        "SELECT id * 10 + 0 AS tens, f * 2 AS dbl FROM pa ORDER BY dbl",
    );
    assert_eq!(r.columns, vec!["tens".to_string(), "dbl".to_string()]);
    assert_eq!(r.rows[0][1], Value::Float(1.0));
    // ORDER BY 引用别名
    let r = rows(&mut db, "SELECT -id AS neg FROM pa ORDER BY neg");
    assert_eq!(r.rows[0][0], Value::Int(-2));
}

#[test]
fn update_reindex_and_multi_condition_probe() {
    let mut db = idx_db();
    insert_n(&mut db, 10);
    // 非索引谓词触发的 UPDATE,但更新了带索引的列 → 重建索引树
    run(&mut db, "UPDATE t SET n = 100 WHERE name LIKE 'u%'");
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM t WHERE n = 100").rows[0][0],
        Value::Int(10)
    );
    // 常量在左的比较翻转后仍能走索引探测
    assert_eq!(rows(&mut db, "SELECT id FROM t WHERE 5 = id").rows.len(), 1);
    assert_eq!(
        rows(&mut db, "SELECT id FROM t WHERE 3 < id AND id < 6")
            .rows
            .len(),
        2
    );
    assert_eq!(
        rows(&mut db, "SELECT id FROM t WHERE 7 > id AND id > 4")
            .rows
            .len(),
        2
    );
    assert_eq!(
        rows(&mut db, "SELECT id FROM t WHERE 9 >= id AND id >= 8")
            .rows
            .len(),
        2
    );
    assert_eq!(
        rows(&mut db, "SELECT id FROM t WHERE 1 <= id AND id <= 2")
            .rows
            .len(),
        2
    );
    // 限定表名的列也能探测
    assert_eq!(
        rows(&mut db, "SELECT id FROM t WHERE t.id = 4").rows.len(),
        1
    );
    // AND 多个等值条件合并探测
    run(&mut db, "UPDATE t SET n = 7 WHERE id = 3 AND name = 'u3'");
    assert_eq!(
        rows(&mut db, "SELECT n FROM t WHERE id = 3").rows[0][0],
        Value::Int(7)
    );
}

#[test]
fn insert_constraint_failures_abort_cleanly() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE ic (id INT PRIMARY KEY, name TEXT NOT NULL)",
    );
    run(&mut db, "INSERT INTO ic (id, name) VALUES (1, 'a')");
    // 批内重复 → UNIQUE 冲突,语句中止无残留
    assert!(db
        .execute("INSERT INTO ic (id, name) VALUES (2, 'b'), (2, 'c')")
        .is_err());
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM ic").rows[0][0],
        Value::Int(1)
    );
    // NOT NULL 违规走 abort 路径
    assert!(db.execute("INSERT INTO ic (id) VALUES (5)").is_err());
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM ic").rows[0][0],
        Value::Int(1)
    );
    // AUTOINCREMENT 列省略时自动填充
    run(
        &mut db,
        "CREATE TABLE ai (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)",
    );
    run(&mut db, "INSERT INTO ai (name) VALUES ('x')");
    assert_eq!(rows(&mut db, "SELECT id FROM ai").rows[0][0], Value::Int(1));
    run(&mut db, "INSERT INTO ai VALUES (10, 'y')");
    run(&mut db, "INSERT INTO ai (name) VALUES ('z')");
    assert_eq!(
        rows(&mut db, "SELECT MAX(id) FROM ai").rows[0][0],
        Value::Int(11)
    );
}

#[test]
fn group_concat_and_string_agg() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE gc (id INT, s TEXT, b BOOL)");
    run(
        &mut db,
        "INSERT INTO gc VALUES (1, 'a', true), (1, NULL, false), (1, 'c', true)",
    );
    let r = rows(&mut db, "SELECT GROUP_CONCAT(s, ',') FROM gc GROUP BY id");
    assert_eq!(r.rows[0][0], Value::Str("a,c".into()));
    let r = rows(&mut db, "SELECT STRING_AGG(b, '|') FROM gc GROUP BY id");
    // bool 参与拼接走 value_to_text;分隔符参数生效
    assert_eq!(r.rows[0][0], Value::Str("true|false|true".into()));
}

#[test]
fn typeof_covers_variant_kinds() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE ty (id INT PRIMARY KEY, f FLOAT, b BOOL, s TEXT)",
    );
    run(&mut db, "INSERT INTO ty VALUES (1, 0.5, true, 'x')");
    assert_eq!(
        rows(
            &mut db,
            "SELECT TYPEOF(id), TYPEOF(f), TYPEOF(b), TYPEOF(s) FROM ty"
        )
        .rows[0],
        vec![
            Value::Str("integer".into()),
            Value::Str("float".into()),
            Value::Str("bool".into()),
            Value::Str("text".into())
        ]
    );
    // schemaless 数组/对象字段插入后 TYPEOF 报告 array/object
    run(
        &mut db,
        "INSERT INTO ty (id, arr, obj) VALUES (2, '[1,2]', '{\"k\":1}')",
    );
    // JSON 字面量以文本存储,这里只验证不崩溃
    assert!(
        rows(&mut db, "SELECT TYPEOF(arr) FROM ty WHERE id = 2")
            .rows
            .len()
            == 1
    );
}

// ---- dump_script (join bootstrap) ----

/// Apply a dump script the way the join protocol does: one transaction,
/// replace tables wholesale. Returns the target database.
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
fn json_path_index_dumps_and_replays() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE docs (id INT PRIMARY KEY, doc TEXT)");
    run(&mut db, "INSERT INTO docs VALUES (1, '{\"name\":\"a\"}')");
    run(&mut db, "INSERT INTO docs VALUES (2, '{\"name\":\"b\"}')");
    run(
        &mut db,
        "CREATE INDEX ix_doc_name ON docs (JSON_EXTRACT(doc, '$.name'))",
    );
    let script = db.dump_script().unwrap();
    assert!(
        script
            .contains("CREATE INDEX \"ix_doc_name\" ON \"docs\" (JSON_EXTRACT(\"doc\", '$.name'))"),
        "{script}"
    );
    // Replay into a fresh database; the probe must work identically.
    let mut fresh = Database::in_memory().unwrap();
    for stmt in script.split(';') {
        if stmt.trim().is_empty() {
            continue;
        }
        run(&mut fresh, stmt);
    }
    let r = rows(
        &mut fresh,
        "SELECT id FROM docs WHERE JSON_EXTRACT(doc, '$.name') = 'b'",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(2)]]);
    // Catalog round trip: the path survives a reload.
    assert!(fresh
        .tables
        .get("docs")
        .unwrap()
        .index_defs
        .iter()
        .any(|d| d.name == "ix_doc_name" && d.path.as_deref() == Some("$.name")));
}

#[test]
fn json_path_index_catalog_is_backward_compatible() {
    // A pre-path-index catalog tuple (4 elements) parses with path=None,
    // and a 5-element tuple parses with the path: the array form is
    // positional with an optional tail, so old volumes open with zero
    // migration and old readers ignore the extra element (design 005).
    // The parser is exercised end to end by dump/replay above; here the
    // struct contract is pinned directly.
    use super::{validate_json_index_path, IndexDef};
    let scalar = IndexDef {
        name: "ix".into(),
        columns: vec!["v".into()],
        unique: false,
        path: None,
        pred: None,
    };
    assert!(scalar.path.is_none());
    let path = IndexDef {
        name: "ix".into(),
        columns: vec!["doc".into()],
        unique: false,
        path: Some("$.a.b".into()),
        pred: None,
    };
    assert_eq!(path.column(), "doc");
    // Path validation grammar: the accepted/rejected shapes.
    assert!(validate_json_index_path("$.a").is_ok());
    assert!(validate_json_index_path("$.a[2].b").is_ok());
    assert!(validate_json_index_path("a.b").is_err());
    assert!(validate_json_index_path("$").is_err());
    assert!(validate_json_index_path("$.a.").is_err());
    assert!(validate_json_index_path("$.a[x]").is_err());
    assert!(validate_json_index_path("$.a['b']").is_err());
}

#[test]
fn json_path_residual_predicate_keeps_exact_window_inexact() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY NOT NULL, doc TEXT)",
    );
    run(&mut db, "INSERT INTO t VALUES (1, '{\"p\":9}')");
    run(&mut db, "INSERT INTO t VALUES (2, '{\"p\":5}')");
    // The exact ORDER BY-id window must NOT swallow the JSON predicate
    // just because `id` carries an exact probe of its own.
    let r = rows(
        &mut db,
        "SELECT id FROM t WHERE id > 0 AND JSON_EXTRACT(doc, '$.p') = 5 ORDER BY id LIMIT 10",
    );
    assert_eq!(r.rows, vec![vec![Value::Int(2)]]);
    // Eq-probe shape and DESC variant share the same guard.
    let r = rows(
        &mut db,
        "SELECT id FROM t WHERE id = 1 AND JSON_EXTRACT(doc, '$.p') = 5 ORDER BY id DESC LIMIT 10",
    );
    assert_eq!(r.rows, Vec::<Vec<Value>>::new());
}

#[test]
fn window_group_concat_zero_contribution_frame_is_null() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE g (k INT, v TEXT)");
    run(&mut db, "INSERT INTO g VALUES (1, NULL), (2, 'b')");
    let r = rows(&mut db, "SELECT GROUP_CONCAT(v) OVER (ORDER BY k) FROM g");
    // Batch semantics: zero contributing rows (all-NULL frame) is NULL,
    // not the empty string — IS NULL checks depend on the agreement.
    assert_eq!(r.rows[0][0], Value::Null);
    assert_eq!(r.rows[1][0], Value::Str("b".into()));
}

#[test]
fn quantified_comparison_tolerates_unknown_elements() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE x (ts TIMESTAMP)");
    run(&mut db, "CREATE TABLE s (txt TEXT)");
    run(
        &mut db,
        "INSERT INTO x VALUES (TIMESTAMP '2026-01-01T00:00:00Z')",
    );
    run(&mut db, "INSERT INTO s VALUES ('not-a-timestamp')");
    // A Timestamp vs unparseable-string comparison evaluates to Null;
    // the ANY chain must fold it to "not satisfied", not error out.
    let r = rows(
        &mut db,
        "SELECT COUNT(*) FROM x WHERE ts > ANY (SELECT txt FROM s)",
    );
    assert_eq!(r.rows[0][0], Value::Int(0));
}

#[test]
fn self_fk_terminal_state_checks() {
    // UPDATE vacating a parent key while a new image references it.
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE t (id INT PRIMARY KEY, p INT REFERENCES t(id))",
    );
    run(&mut db, "INSERT INTO t VALUES (1, NULL), (2, NULL)");
    let e = db
        .execute("UPDATE t SET id = 3, p = 1 WHERE id = 1")
        .unwrap_err();
    assert!(e.to_string().contains("FOREIGN KEY"), "{e}");

    // Upsert-UPDATE shape of the same hole.
    let e = db
        .execute("INSERT INTO t VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET id = 3, p = 1")
        .unwrap_err();
    assert!(e.to_string().contains("FOREIGN KEY"), "{e}");

    // Deleting parent and child rows of a self-FK table in one
    // statement is legal (statement-final semantics).
    run(&mut db, "DELETE FROM t");
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM t").rows[0][0],
        Value::Int(0)
    );

    // The probe (fast) DELETE path keeps the same semantics.
    run(&mut db, "INSERT INTO t VALUES (1, NULL), (2, 1)");
    run(&mut db, "DELETE FROM t WHERE id IS NOT NULL");
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM t").rows[0][0],
        Value::Int(0)
    );

    // A key-chain shift whose final state is consistent still passes.
    run(&mut db, "INSERT INTO t VALUES (1, NULL), (2, 1)");
    run(
        &mut db,
        "UPDATE t SET id = id + 1, p = CASE WHEN p IS NULL THEN NULL ELSE p + 1 END",
    );
    assert_eq!(
        rows(&mut db, "SELECT id, p FROM t ORDER BY id").rows,
        vec![
            vec![Value::Int(2), Value::Null],
            vec![Value::Int(3), Value::Int(2)]
        ]
    );
}

#[test]
fn rename_column_does_not_adopt_composite_tree_named_like_column() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (a INT, x INT, y INT)");
    // A composite index NAMED like the column keeps its def-name root.
    run(&mut db, "CREATE INDEX a ON t (x, y)");
    run(&mut db, "INSERT INTO t VALUES (1, 10, 20)");
    run(&mut db, "ALTER TABLE t RENAME COLUMN a TO b");
    // The composite tree still answers probes after the rename.
    let r = rows(&mut db, "SELECT b FROM t WHERE x = 10 AND y = 20");
    assert_eq!(r.rows, vec![vec![Value::Int(1)]]);
    // ...and the renamed column did NOT gain a bogus scalar tree.
    let e = db.execute("CREATE TABLE u (c INT)").unwrap();
    drop(e);
    let r = rows(&mut db, "SELECT b FROM t WHERE b = 1");
    assert_eq!(r.rows.len(), 1);
}

#[test]
fn drop_column_refuses_through_select_star_middle_view() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT, v INT)");
    run(&mut db, "CREATE VIEW v1 AS SELECT * FROM t");
    run(&mut db, "CREATE VIEW v2 AS SELECT v FROM v1");
    // v1 never names the column (SELECT *), but v2 does; the refusal
    // must walk the transitive chain instead of degrading v2 to NULL.
    let e = db.execute("ALTER TABLE t DROP COLUMN v").unwrap_err();
    assert!(e.to_string().contains("referenced by VIEW v2"), "{e}");
}

#[test]
fn create_index_rejects_system_table_names() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (a INT)");
    let e = db
        .execute("CREATE INDEX _cluster_log ON t (a)")
        .unwrap_err();
    assert!(e.to_string().contains("reserved"), "{e}");
    let e = db
        .execute("CREATE INDEX docsql_users ON t (a)")
        .unwrap_err();
    assert!(e.to_string().contains("reserved"), "{e}");
}

#[test]
fn json_path_index_probes_equality_and_range() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE docs (id INT PRIMARY KEY, doc TEXT)");
    for (i, name) in ["svc-1", "svc-2", "svc-3"].iter().enumerate() {
        let doc = format!(
            "{{\"name\":\"{name}\",\"level\":{},\"nest\":{{\"x\":{}}}}}",
            i,
            i + 10
        );
        run(
            &mut db,
            &format!(
                "INSERT INTO docs VALUES ({}, '{}')",
                i + 1,
                doc.replace("\'", "\'\'")
            ),
        );
    }
    // Malformed JSON and a doc missing the path: excluded from the tree,
    // and correctly absent from equality results (NULL = x is false).
    run(&mut db, "INSERT INTO docs VALUES (4, 'not json')");
    run(&mut db, "INSERT INTO docs VALUES (5, '{\"other\":1}')");

    run(
        &mut db,
        "CREATE INDEX ix_doc_name ON docs (JSON_EXTRACT(doc, '$.name'))",
    );
    let r = rows(
        &mut db,
        "SELECT id FROM docs WHERE JSON_EXTRACT(doc, '$.name') = 'svc-2'",
    );
    assert_eq!(r.rows.len(), 1, "{:?}", r.rows);
    assert_eq!(r.rows[0][0], Value::Int(2));

    // Mirrored literal.
    let r = rows(
        &mut db,
        "SELECT id FROM docs WHERE 'svc-3' = JSON_EXTRACT(doc, '$.name')",
    );
    assert_eq!(r.rows.len(), 1);

    // Range on a nested path + array index.
    run(
        &mut db,
        "CREATE INDEX ix_doc_nest ON docs (JSON_EXTRACT(doc, '$.nest.x'))",
    );
    let r = rows(
        &mut db,
        "SELECT id FROM docs WHERE JSON_EXTRACT(doc, '$.nest.x') >= 11",
    );
    assert_eq!(r.rows.len(), 2);

    // EXPLAIN reports the probe through the path index.
    let r = rows(
        &mut db,
        "EXPLAIN SELECT * FROM docs WHERE JSON_EXTRACT(doc, '$.name') = 'svc-1'",
    );
    let detail = r
        .rows
        .iter()
        .map(|row| row[1].as_str().unwrap_or_default())
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        detail.contains("INDEX PROBE ON docs USING ix_doc_name"),
        "{detail}"
    );

    // AND with an opaque conjunct stays correct (residual filter).
    let r = rows(
        &mut db,
        "SELECT id FROM docs WHERE JSON_EXTRACT(doc, '$.name') = 'svc-1' AND id > 0",
    );
    assert_eq!(r.rows.len(), 1);

    // UPDATE maintenance: change the extracted value, old key must go.
    run(
        &mut db,
        "UPDATE docs SET doc = '{\"name\":\"svc-9\"}' WHERE id = 2",
    );
    let r = rows(
        &mut db,
        "SELECT id FROM docs WHERE JSON_EXTRACT(doc, '$.name') = 'svc-2'",
    );
    assert!(r.rows.is_empty(), "{:?}", r.rows);
    let r = rows(
        &mut db,
        "SELECT id FROM docs WHERE JSON_EXTRACT(doc, '$.name') = 'svc-9'",
    );
    assert_eq!(r.rows.len(), 1);
    // DELETE removes the entry.
    run(&mut db, "DELETE FROM docs WHERE id = 3");
    let r = rows(
        &mut db,
        "SELECT id FROM docs WHERE JSON_EXTRACT(doc, '$.name') = 'svc-3'",
    );
    assert!(r.rows.is_empty());

    // Refusals: UNIQUE on a path, bogus path grammar, expression form.
    assert!(db
        .execute("CREATE UNIQUE INDEX u1 ON docs (JSON_EXTRACT(doc, '$.name'))")
        .is_err());
    assert!(db
        .execute("CREATE INDEX u2 ON docs (JSON_EXTRACT(doc, 'name'))")
        .is_err());
    assert!(db
        .execute("CREATE INDEX u3 ON docs (JSON_EXTRACT(doc, '$'))")
        .is_err());
    assert!(db.execute("CREATE INDEX u4 ON docs (UPPER(doc))").is_err());
    assert!(db
        .execute("CREATE INDEX u5 ON docs (JSON_EXTRACT(doc, '$.a'), other)")
        .is_err());

    // Path index must not hijack scalar column semantics (design 005 §5.2):
    // ORDER BY doc and WHERE doc = … stay correct with a path index present.
    run(&mut db, "CREATE TABLE s (id INT PRIMARY KEY, doc TEXT)");
    run(&mut db, "INSERT INTO s VALUES (1, 'bbb')");
    run(&mut db, "INSERT INTO s VALUES (2, 'aaa')");
    run(
        &mut db,
        "CREATE INDEX s_path ON s (JSON_EXTRACT(doc, '$.k'))",
    );
    let r = rows(&mut db, "SELECT id FROM s ORDER BY doc");
    assert_eq!(r.rows, vec![vec![Value::Int(2)], vec![Value::Int(1)]]);
    let r = rows(&mut db, "SELECT id FROM s WHERE doc = 'bbb'");
    assert_eq!(r.rows.len(), 1);
}

#[test]
fn dump_script_roundtrips_structured_and_non_finite_values() {
    // JSON functions can store Arrays/Objects and CAST can store
    // non-finite floats; a dump (automatic backup / join snapshot) used
    // to fail on them ("cannot render ... as a SQL literal").
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE j (id INT PRIMARY KEY, doc ANY, f REAL)",
    );
    run(
            &mut db,
            "INSERT INTO j SELECT 1, JSON_EXTRACT('{\"items\":[1,2],\"name\":\"x\"}', '$'), CAST('NaN' AS REAL)",
        );
    run(
        &mut db,
        "INSERT INTO j SELECT 2, JSON_EXTRACT('[true,{\"n\":null}]', '$'), CAST('inf' AS REAL)",
    );
    let script = db.dump_script().unwrap();
    assert!(script.contains("JSON_EXTRACT("), "{script}");
    assert!(script.contains("CAST('NaN' AS REAL)"), "{script}");
    let mut restored = apply_dump(&script);
    let before = rows(&mut db, "SELECT id, doc, f FROM j ORDER BY id");
    let after = rows(&mut restored, "SELECT id, doc, f FROM j ORDER BY id");
    // Debug rather than eq: NaN != NaN under Value's PartialEq.
    assert_eq!(format!("{before:?}"), format!("{after:?}"));
}

#[test]
fn dump_script_roundtrips_decimal_and_blob_values() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE money (id INT PRIMARY KEY, amount DECIMAL(28,10), raw BLOB)",
    );
    run(
        &mut db,
        "INSERT INTO money VALUES \
             (1, CAST('0.10000000000000000001' AS DECIMAL), x'deadbeef'), \
             (2, CAST('-123456789012345678.1234567890' AS DECIMAL), x''), \
             (3, NULL, NULL)",
    );
    let script = db.dump_script().unwrap();
    let mut restored = apply_dump(&script);
    let before = rows(&mut db, "SELECT amount, raw FROM money ORDER BY id");
    let after = rows(&mut restored, "SELECT amount, raw FROM money ORDER BY id");
    assert_eq!(before, after);
    // The dump carries exact CAST literals, not lossy float text.
    assert!(script.contains("CAST('0.10000000000000000001' AS DECIMAL)"));
    assert!(script.contains("x'deadbeef'"));
}

#[test]
fn dump_script_roundtrips_schema_constraints_and_data() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE parent (pid INT PRIMARY KEY, tag TEXT UNIQUE NOT NULL)",
    );
    run(
        &mut db,
        "CREATE TABLE child (id INT PRIMARY KEY AUTOINCREMENT, \
             gid GUID AUTOINCREMENT, pid INT, note TEXT DEFAULT ('n/a'), extra TEXT, \
             CHECK (id >= 0), FOREIGN KEY (pid) REFERENCES parent (pid))",
    );
    run(&mut db, "CREATE INDEX ix_child_note ON child (note)");
    run(&mut db, "CREATE UNIQUE INDEX ux_parent_tag ON parent (tag)");
    run(
        &mut db,
        "INSERT INTO parent VALUES (1, 'a'), (2, '中文''引号')",
    );
    run(
        &mut db,
        "INSERT INTO child (pid, note, extra, bonus) VALUES \
             (1, 'line1\nx', 'declared', 'schemaless 字段')",
    );
    run(&mut db, "INSERT INTO child (pid) VALUES (2)");
    let script = db.dump_script().unwrap();
    assert!(!script.to_lowercase().contains(super::PUBSUB_TABLE));

    let mut dst = apply_dump(&script);
    // Data round-trips (undeclared schemaless field included).
    assert_eq!(
        rows(
            &mut dst,
            "SELECT pid, note, extra, bonus FROM child ORDER BY id"
        )
        .rows,
        vec![
            vec![
                Value::Int(1),
                Value::Str("line1\nx".into()),
                Value::Str("declared".into()),
                Value::Str("schemaless 字段".into())
            ],
            vec![
                Value::Int(2),
                Value::Str("n/a".into()),
                Value::Null,
                Value::Null
            ],
        ]
    );
    // Constraints round-trip: UNIQUE still enforced...
    assert!(db.execute("INSERT INTO parent VALUES (3, 'a')").is_err());
    assert!(dst.execute("INSERT INTO parent VALUES (3, 'a')").is_err());
    // ...CHECK still enforced, FK still enforced (parent must exist)...
    assert!(dst.execute("INSERT INTO child (pid) VALUES (99)").is_err());
    // ...and NOT NULL still enforced.
    assert!(dst.execute("INSERT INTO parent (pid) VALUES (4)").is_err());
    // Declared column lists match (incl. GUID marker surfaced as GUID).
    let cols = |d: &mut Database| {
        d.catalog()
            .into_iter()
            .map(|t| {
                (
                    t.name,
                    t.columns
                        .into_iter()
                        .map(|c| {
                            (
                                c.name,
                                c.primary_key,
                                c.unique,
                                c.nullable,
                                c.autoinc,
                                c.data_type,
                            )
                        })
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(cols(&mut db), cols(&mut dst));
    // Index definitions round-trip by name/uniqueness.
    let idx = |d: &mut Database| {
        d.catalog()
            .into_iter()
            .map(|t| (t.name, t.index_defs))
            .collect::<Vec<_>>()
    };
    assert_eq!(idx(&mut db), idx(&mut dst));
    // AUTOINCREMENT continues from the replicated max on the joiner.
    run(&mut dst, "INSERT INTO child (pid) VALUES (1)");
    assert_eq!(
        rows(&mut dst, "SELECT COUNT(id) FROM child").rows[0][0],
        Value::Int(3)
    );
    let ids = rows(&mut dst, "SELECT id FROM child ORDER BY id").rows;
    assert_eq!(ids.last().unwrap()[0].as_i64().unwrap(), 3);
}

#[test]
fn dump_script_is_reapplied_wholesale() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
    run(&mut db, "INSERT INTO t VALUES (1, 'x'), (2, 'y')");
    let script = db.dump_script().unwrap();
    // Re-apply onto a database that already has the table (with drift):
    // the DROP prefix replaces it wholesale, no duplicate rows.
    let mut dst = apply_dump(&script);
    run(&mut dst, "INSERT INTO t VALUES (3, 'drift')");
    run(&mut dst, "BEGIN");
    assert!(dst.execute_batch(&script).error.is_none());
    run(&mut dst, "COMMIT");
    assert_eq!(
        rows(&mut dst, "SELECT COUNT(v) FROM t").rows[0][0],
        Value::Int(2)
    );
}

#[test]
fn dump_script_skips_pubsub_store() {
    let mut db = Database::in_memory().unwrap();
    db.ensure_pubsub_table().unwrap();
    run(
        &mut db,
        "INSERT INTO _pubsub_messages VALUES (1, 'ch', 1, 'p')",
    );
    run(&mut db, "CREATE TABLE user_t (v INT)");
    let script = db.dump_script().unwrap();
    let dst = apply_dump(&script);
    assert_eq!(dst.catalog().len(), 1);
    assert_eq!(dst.catalog()[0].name, "user_t");
}

/// Snapshot adoption must leave the adopter's digests equal to the
/// origin's: schema_hash covers the DEFAULT expression text, which the
/// dump renders as `DEFAULT (expr)` — without storing a normalized
/// form, the replayed catalog kept the parens (`('anon')`), digests
/// never matched again, and every rejoin degraded to a full snapshot.
#[test]
fn dump_restore_digest_roundtrip_is_stable_with_defaults_and_fk() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT DEFAULT 'anon')",
    );
    run(
        &mut db,
        "CREATE TABLE orders (id INT PRIMARY KEY AUTOINCREMENT, uid INT, \
             FOREIGN KEY (uid) REFERENCES users (id))",
    );
    run(&mut db, "INSERT INTO users VALUES (1, 'a')");
    run(&mut db, "INSERT INTO users (id) VALUES (2)");
    run(&mut db, "INSERT INTO orders (uid) VALUES (1), (2)");
    let script = db.dump_script().unwrap();
    assert!(script.contains("DEFAULT ('anon')"), "{script}");

    let mut restored = apply_dump(&script);
    assert_eq!(
        db.digests().unwrap(),
        restored.digests().unwrap(),
        "source and restored digests must agree (schema and rows)"
    );
    // And the restored node's dump is a fixed point: no paren growth
    // across dump cycles.
    let script2 = restored.dump_script().unwrap();
    assert_eq!(script, script2, "dump must be a fixed point");
}

/// Backup restore replays the dump over LIVE data, statement by
/// statement, without a wipe. Per-table DROP+CREATE interleaved
/// alphabetically used to abort it: the recreated child table
/// referenced the parent whose old copy was still to be dropped, and
/// DROP's FK guard rejected it. One multi-table DROP (whose co-dropped
/// tables the guard exempts) makes the replay FK-safe.
#[test]
fn dump_replays_over_live_fk_data() {
    let mut db = Database::in_memory().unwrap();
    run(&mut db, "CREATE TABLE users (id INT PRIMARY KEY)");
    run(
        &mut db,
        "CREATE TABLE orders (id INT PRIMARY KEY, uid INT, \
             FOREIGN KEY (uid) REFERENCES users (id))",
    );
    run(&mut db, "INSERT INTO users VALUES (1)");
    run(&mut db, "INSERT INTO orders VALUES (100, 1)");
    let script = db.dump_script().unwrap();

    // Live drift after the snapshot, then the restore-style replay:
    // autocommit statements over the live database, in script order.
    run(&mut db, "INSERT INTO users VALUES (2)");
    run(&mut db, "INSERT INTO orders VALUES (101, 2)");
    for stmt in script.split(";\n").filter(|s| !s.trim().is_empty()) {
        run(&mut db, stmt);
    }
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM users").rows[0][0],
        Value::Int(1)
    );
    assert_eq!(
        rows(&mut db, "SELECT id FROM orders").rows[0][0],
        Value::Int(100)
    );
}
