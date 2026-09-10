//! 写入路径微基准:拆解单条 INSERT / UPDATE 的成本构成(fsync、事务批量、索引维护)。
//! 运行:cargo run -p docsql-core --example bench_write --release

use docsql_core::engine::{Database, ExecOutcome};
use std::time::Instant;

fn now_us(t: Instant) -> f64 {
    t.elapsed().as_secs_f64() * 1e6
}

fn main() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Database::open(&dir.path().join("write.db")).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, v INT)")
        .unwrap();

    // 1) autocommit 单条 INSERT(默认持久:每语句一次 WAL fsync)
    const N: i64 = 5_000;
    let t = Instant::now();
    for i in 0..N {
        db.execute(&format!(
            "INSERT INTO t VALUES ({i}, 'name{i}', {})",
            i % 100
        ))
        .unwrap();
    }
    let e = now_us(t);
    println!(
        "autocommit 插入 {N:>6} 条: {e:10.1} µs ({:8.1} µs/条, {:8.0} 条/秒)",
        e / N as f64,
        N as f64 / (e / 1e6),
    );

    // 2) 显式事务批量 INSERT(整批一次 fsync)
    const M: i64 = 20_000;
    let t = Instant::now();
    db.execute("BEGIN").unwrap();
    for i in N..N + M {
        db.execute(&format!(
            "INSERT INTO t VALUES ({i}, 'name{i}', {})",
            i % 100
        ))
        .unwrap();
    }
    db.execute("COMMIT").unwrap();
    let e = now_us(t);
    println!(
        "事务批量插入 {M:>6} 条: {e:10.1} µs ({:8.1} µs/条, {:8.0} 条/秒)",
        e / M as f64,
        M as f64 / (e / 1e6),
    );

    // 3) autocommit UPDATE 走主键(索引探针 + 原地更新 + fsync)
    let t = Instant::now();
    for i in 0..N {
        db.execute(&format!("UPDATE t SET v = v + 1 WHERE id = {i}"))
            .unwrap();
    }
    let e = now_us(t);
    println!(
        "autocommit 更新 {N:>6} 条: {e:10.1} µs ({:8.1} µs/条, {:8.0} 条/秒)",
        e / N as f64,
        N as f64 / (e / 1e6),
    );

    // 4) async_commit 模式(批量组刷,无每语句 fsync)
    const A0: i64 = N + M; // 25_000 起,避开已占用的 id 区间
    db.set_async_commit(true);
    let t = Instant::now();
    for i in A0..A0 + N {
        db.execute(&format!("INSERT INTO t VALUES ({i}, 'x{i}', 1)"))
            .unwrap();
    }
    let e = now_us(t);
    db.sync_pending().unwrap();
    println!(
        "异步提交插入 {N:>6} 条: {e:10.1} µs ({:8.1} µs/条, {:8.0} 条/秒)",
        e / N as f64,
        N as f64 / (e / 1e6),
    );
    db.set_async_commit(false);

    // 4b) 集群写路径的复制记账:未融合时每条写 = 用户写 fsync + journal
    // fsync;融合写单元把两笔并成一次 fsync。用独立的新库测,避免上方
    // 30k 行大库的 WAL checkpoint(sync_all 整库)摊销淹没逐条 fsync 差异。
    {
        const C: i64 = 3_000;
        let dir2 = tempfile::tempdir().unwrap();
        let mut db2 = Database::open(&dir2.path().join("cluster.db")).unwrap();
        db2.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, v INT)")
            .unwrap();
        db2.ensure_cluster_tables().unwrap();
        // 预热:journal 表建好、journal_next 缓存住
        db2.journal_append("INSERT INTO t VALUES (-1, 'warm', 1)")
            .unwrap();
        // 未融合:两条独立 autocommit
        let t = Instant::now();
        for i in 0..C {
            let sql = format!("INSERT INTO t VALUES ({i}, 'c{i}', 1)");
            db2.execute(&sql).unwrap();
            db2.journal_append(&sql).unwrap();
        }
        let unfused = now_us(t);
        // 融合:一个写单元内 用户写 + journal,结束一次 fsync
        let t = Instant::now();
        for i in C..2 * C {
            let sql = format!("INSERT INTO t VALUES ({i}, 'c{i}', 1)");
            let mut unit = db2.write_unit();
            unit.execute(&sql).unwrap();
            unit.journal_append(&sql).unwrap();
            unit.end().unwrap();
        }
        let fused = now_us(t);
        println!(
            "集群写记账({C} 条): 未融合 {unfused:9.1} µs ({:7.1} µs/条) vs 融合 {fused:9.1} µs ({:7.1} µs/条, {:.2}×)",
            unfused / C as f64,
            fused / C as f64,
            unfused / fused,
        );
    }

    // 5) 点查随表规模伸缩(30k → 300k 行;旧 catalog 单页格式在 ~10 万行即封顶)
    for (label, total) in [("30k 行", 30_000i64), ("300k 行", 300_000)] {
        if total > A0 + N {
            db.execute("BEGIN").unwrap();
            for i in A0 + N..total {
                db.execute(&format!("INSERT INTO t VALUES ({i}, 'x{i}', 1)"))
                    .unwrap();
            }
            db.execute("COMMIT").unwrap();
        }
        let t = Instant::now();
        for i in 0..20_000i64 {
            let hit = db
                .execute(&format!("SELECT name FROM t WHERE id = {}", i % total))
                .unwrap();
            assert!(matches!(hit, ExecOutcome::Rows(r) if !r.rows.is_empty()));
        }
        let e = now_us(t);
        println!(
            "点查({label:>6}) 20000 次: {e:10.1} µs ({:8.1} µs/次)",
            e / 20_000.0,
        );
    }

    // 6) 全表过滤(此时表已含 30 万行,看扫描吞吐)
    {
        let total: i64 = 300_000;
        let t = Instant::now();
        let out = db
            .execute(&format!("SELECT COUNT(*) FROM t WHERE v < {}", i64::MAX))
            .unwrap();
        let e = now_us(t);
        let rows: i64 = match out {
            ExecOutcome::Rows(r) => r.rows[0][0].as_i64().unwrap(),
            _ => unreachable!(),
        };
        assert_eq!(rows, total);
        println!(
            "全表过滤(300k 行) {total:>6} 行: {e:9.1} µs ({:5.2} µs/行)",
            e / total as f64,
        );
    }
}
