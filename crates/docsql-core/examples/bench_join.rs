//! JOIN 路径微基准:等值连接(hash join)与非等值回退(嵌套循环)的成本对照。
//! 运行:cargo run -p docsql-core --example bench_join --release

use docsql_core::engine::{Database, ExecOutcome};
use std::time::Instant;

fn now_ms(t: Instant) -> f64 {
    t.elapsed().as_secs_f64() * 1e3
}

fn count(db: &mut Database, sql: &str) -> i64 {
    match db.execute(sql).unwrap() {
        ExecOutcome::Rows(r) => r.rows[0][0].as_i64().unwrap(),
        _ => panic!("expected rows"),
    }
}

fn main() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Database::open(&dir.path().join("join.db")).unwrap();
    db.execute("CREATE TABLE a (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    db.execute("CREATE TABLE b (id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("BEGIN").unwrap();
    let n: i64 = 3000;
    for i in 0..n {
        db.execute(&format!("INSERT INTO a VALUES ({i}, 'a{i}')"))
            .unwrap();
        // b 的键域偏移一半:一半匹配、一半是 b 独有(测 LEFT/RIGHT 补 NULL)
        let j = i + n / 2;
        db.execute(&format!("INSERT INTO b VALUES ({j}, 'b{j}')"))
            .unwrap();
    }
    db.execute("COMMIT").unwrap();

    // 1) 等值 INNER JOIN(主键列,全限定别名)
    let t = Instant::now();
    let rows = count(&mut db, "SELECT COUNT(*) FROM a JOIN b ON a.id = b.id");
    let e = now_ms(t);
    println!(
        "等值 JOIN {n}×{n} (匹配 {rows} 行): {e:9.1} ms ({:9.0} 万对/秒)",
        (n * n) as f64 / e / 10.0
    );

    // 2) 等值 LEFT JOIN(含未匹配补 NULL)
    let t = Instant::now();
    let rows = count(&mut db, "SELECT COUNT(*) FROM a LEFT JOIN b ON a.id = b.id");
    let e = now_ms(t);
    println!("等值 LEFT JOIN (结果 {rows} 行): {e:9.1} ms");

    // 3) 复合等值(两列同时相等)
    db.execute("CREATE TABLE c (id INT, v TEXT)").unwrap();
    db.execute("BEGIN").unwrap();
    for i in 0..n {
        db.execute(&format!(
            "INSERT INTO c VALUES ({}, 'a{}')",
            i % 100,
            i % 97
        ))
        .unwrap();
    }
    db.execute("COMMIT").unwrap();
    let t = Instant::now();
    let rows = count(
        &mut db,
        "SELECT COUNT(*) FROM a JOIN c ON a.id = c.id AND a.v = c.v",
    );
    let e = now_ms(t);
    println!("复合等值 JOIN (结果 {rows} 行): {e:9.1} ms");

    // 4) 非等值 JOIN(回退嵌套循环,规模缩到 500×500 代表性测)
    let t = Instant::now();
    let rows = count(
        &mut db,
        "SELECT COUNT(*) FROM a x JOIN b y ON x.id < 200 AND y.id < 200",
    );
    let e = now_ms(t);
    println!("非等值 JOIN 200×200 (结果 {rows} 行): {e:9.1} ms");
}
