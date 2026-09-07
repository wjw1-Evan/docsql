//! docsql 性能基准:插入/点查/索引查/更新/删除/扫描吞吐。
//! 运行:cargo run -p docsql-core --example bench

use docsql_core::engine::Database;
use std::time::Instant;

fn ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

fn main() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Database::open(&dir.path().join("bench.db")).unwrap();

    db.execute("CREATE TABLE bench (id INT PRIMARY KEY, name TEXT, v INT)")
        .unwrap();
    db.execute("CREATE INDEX idx_bench_v ON bench (v)").unwrap();

    const N: usize = 20_000;

    // 1) 逐条插入(每条一个事务 = 每条一次 fsync)
    let t = Instant::now();
    for i in 0..N as i64 {
        db.execute(&format!(
            "INSERT INTO bench (id, name, v) VALUES ({i}, 'name{i}', {})",
            i % 100
        ))
        .unwrap();
    }
    let single_ms = ms(t);
    println!(
        "逐条插入 {N:>6} 行: {single_ms:8.1} ms  ({:.0} 行/秒)",
        N as f64 / single_ms * 1000.0
    );

    // 2) 单事务批量插入
    db.execute("DELETE FROM bench").unwrap();
    let t = Instant::now();
    db.execute("BEGIN").unwrap();
    for i in 0..N as i64 {
        db.execute(&format!(
            "INSERT INTO bench (id, name, v) VALUES ({i}, 'name{i}', {})",
            i % 100
        ))
        .unwrap();
    }
    db.execute("COMMIT").unwrap();
    let batch_ms = ms(t);
    println!(
        "批量插入 {N:>6} 行(1 事务): {batch_ms:8.1} ms  ({:.0} 行/秒)",
        N as f64 / batch_ms * 1000.0
    );

    // 3) 主键点查
    let t = Instant::now();
    for i in 0..N as i64 {
        db.execute(&format!("SELECT name FROM bench WHERE id = {i}"))
            .unwrap();
    }
    let pk_ms = ms(t);
    println!(
        "主键点查 {N:>6} 次: {pk_ms:8.1} ms  ({:.0} 次/秒)",
        N as f64 / pk_ms * 1000.0
    );

    // 4) 二级索引等值查
    let t = Instant::now();
    for i in 0..N as i64 {
        db.execute(&format!("SELECT id FROM bench WHERE v = {}", i % 100))
            .unwrap();
    }
    let idx_ms = ms(t);
    println!(
        "索引等值 {N:>6} 次: {idx_ms:8.1} ms  ({:.0} 次/秒)",
        N as f64 / idx_ms * 1000.0
    );

    // 5) 更新
    let t = Instant::now();
    db.execute("BEGIN").unwrap();
    for i in 0..N as i64 {
        db.execute(&format!("UPDATE bench SET v = v + 1 WHERE id = {i}"))
            .unwrap();
    }
    db.execute("COMMIT").unwrap();
    let upd_ms = ms(t);
    println!(
        "批量更新 {N:>6} 行: {upd_ms:8.1} ms  ({:.0} 行/秒)",
        N as f64 / upd_ms * 1000.0
    );

    // 6) 全表扫描 + 聚合
    let t = Instant::now();
    db.execute("SELECT COUNT(*), MAX(v) FROM bench").unwrap();
    println!("全表聚合 {N:>6} 行: {:8.1} ms", ms(t));

    // 7) 删除
    let t = Instant::now();
    db.execute("BEGIN").unwrap();
    for i in 0..N as i64 {
        db.execute(&format!("DELETE FROM bench WHERE id = {i}"))
            .unwrap();
    }
    db.execute("COMMIT").unwrap();
    let del_ms = ms(t);
    println!(
        "批量删除 {N:>6} 行: {del_ms:8.1} ms  ({:.0} 行/秒)",
        N as f64 / del_ms * 1000.0
    );

    println!("\n--- 汇总 ---");
    println!(
        "单事务写吞吐 {} 行/秒 | 批量写 {} 行/秒 | 点查 {} 次/秒",
        N as f64 / single_ms * 1000.0,
        N as f64 / batch_ms * 1000.0,
        N as f64 / pk_ms * 1000.0
    );
}
