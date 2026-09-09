//! 微基准:拆解单条点查的成本构成(纯解析 / 解析+规划+执行)。
//! 运行:cargo run -p docsql-core --example bench_micro --release

use docsql_core::engine::Database;
use std::time::Instant;

fn main() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Database::open(&dir.path().join("micro.db")).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, v INT)")
        .unwrap();
    const N: i64 = 20_000;
    for i in 0..N {
        db.execute(&format!(
            "INSERT INTO t VALUES ({i}, 'name{i}', {})",
            i % 100
        ))
        .unwrap();
    }

    // 1) 纯解析(parse_check 是静态函数,不触碰引擎)
    let t = Instant::now();
    for i in 0..N {
        assert!(Database::parse_check(&format!("SELECT name FROM t WHERE id = {i}")).is_ok());
    }
    let p = t.elapsed().as_secs_f64() * 1e6;
    println!(
        "纯解析      {N:>6} 次: {p:9.1} µs ({:7.1} µs/次)",
        p / N as f64
    );

    // 2) 主键点查(解析+执行)
    let t = Instant::now();
    for i in 0..N {
        db.execute(&format!("SELECT name FROM t WHERE id = {i}"))
            .unwrap();
    }
    let e = t.elapsed().as_secs_f64() * 1e6;
    println!(
        "主键点查    {N:>6} 次: {e:9.1} µs ({:7.1} µs/次)",
        e / N as f64
    );

    // 3) 预解析过的同一条语句反复执行(排除 format 的干扰,衡量引擎开销下限)
    let t = Instant::now();
    for _ in 0..N {
        db.execute("SELECT name FROM t WHERE id = 42").unwrap();
    }
    let e2 = t.elapsed().as_secs_f64() * 1e6;
    println!(
        "同语句点查  {N:>6} 次: {e2:9.1} µs ({:7.1} µs/次)",
        e2 / N as f64
    );

    // 4) 全表扫描一次(作为 UPDATE 快路径是否扫描的对照)
    let t = Instant::now();
    db.execute("SELECT COUNT(*) FROM t").unwrap();
    println!("全表聚合一次: {:9.1} µs", t.elapsed().as_secs_f64() * 1e6);
}
