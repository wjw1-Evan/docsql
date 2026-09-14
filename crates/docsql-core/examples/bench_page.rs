//! 分页窗口基准:默认 50 万行下的首屏/深偏移/keyset(双向)/索引序对照。
//! 运行:cargo run -p docsql-core --example bench_page --release [行数]

use docsql_core::engine::Database;
use std::time::Instant;

fn bench(label: &str, iters: usize, mut f: impl FnMut()) {
    f(); // warm page cache / once-only paths
    let mut times = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t = Instant::now();
        f();
        times.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "{label:<44} median {:9.3} ms  mean {:9.3} ms",
        times[times.len() / 2],
        times.iter().sum::<f64>() / times.len() as f64
    );
}

fn main() {
    let n: i64 = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(500_000);
    let dir = tempfile::tempdir().unwrap();
    let mut db = Database::open(&dir.path().join("page.db")).unwrap();
    db.execute(
        "CREATE TABLE page_t (id INT PRIMARY KEY NOT NULL, name TEXT NOT NULL, v INT NOT NULL)",
    )
    .unwrap();
    db.execute("CREATE INDEX idx_page_v ON page_t (v)").unwrap();

    // Load with group commit: the write path is not what this benchmark
    // measures and per-row fsync would dominate the setup.
    let t = Instant::now();
    db.set_async_commit(true);
    const BATCH: i64 = 10_000;
    let mut i = 0i64;
    while i < n {
        db.execute("BEGIN").unwrap();
        for j in 0..BATCH.min(n - i) {
            let id = i + j;
            db.execute(&format!(
                "INSERT INTO page_t VALUES ({id}, 'name{id}', {})",
                id % 1000
            ))
            .unwrap();
        }
        db.execute("COMMIT").unwrap();
        i += BATCH;
    }
    db.set_async_commit(false);
    println!("装载 {n} 行: {:.1} s\n", t.elapsed().as_secs_f64());

    const PAGE: i64 = 20;
    let mid = n / 2;
    let last = n - PAGE;
    bench(" 1 主键序 首屏", 50, || {
        db.execute(&format!(
            "SELECT id, name FROM page_t ORDER BY id LIMIT {PAGE}"
        ))
        .unwrap();
    });
    bench(" 2 主键序 OFFSET 半表", 20, || {
        db.execute(&format!(
            "SELECT id, name FROM page_t ORDER BY id LIMIT {PAGE} OFFSET {mid}"
        ))
        .unwrap();
    });
    bench(" 3 主键序 尾页", 20, || {
        db.execute(&format!(
            "SELECT id, name FROM page_t ORDER BY id LIMIT {PAGE} OFFSET {last}"
        ))
        .unwrap();
    });
    bench(" 4 keyset id > 半表", 50, || {
        db.execute(&format!(
            "SELECT id, name FROM page_t WHERE id > {mid} ORDER BY id LIMIT {PAGE}"
        ))
        .unwrap();
    });
    bench(" 5 keyset id > 尾声", 50, || {
        let deep = n - 100;
        db.execute(&format!(
            "SELECT id, name FROM page_t WHERE id > {deep} ORDER BY id LIMIT {PAGE}"
        ))
        .unwrap();
    });
    bench(" 6 主键序 DESC 首屏", 50, || {
        db.execute(&format!(
            "SELECT id, name FROM page_t ORDER BY id DESC LIMIT {PAGE}"
        ))
        .unwrap();
    });
    bench(" 7 主键序 DESC OFFSET 半表", 20, || {
        db.execute(&format!(
            "SELECT id, name FROM page_t ORDER BY id DESC LIMIT {PAGE} OFFSET {mid}"
        ))
        .unwrap();
    });
    bench(" 8 keyset DESC id < 半表", 50, || {
        db.execute(&format!(
            "SELECT id, name FROM page_t WHERE id < {mid} ORDER BY id DESC LIMIT {PAGE}"
        ))
        .unwrap();
    });
    bench(" 9 二级索引序 首屏", 50, || {
        db.execute(&format!(
            "SELECT id, name FROM page_t ORDER BY v LIMIT {PAGE}"
        ))
        .unwrap();
    });
    bench("10 二级索引序 OFFSET 半表", 20, || {
        db.execute(&format!(
            "SELECT id, name FROM page_t ORDER BY v LIMIT {PAGE} OFFSET {mid}"
        ))
        .unwrap();
    });
    bench("11 残余过滤 keyset(id%7=0)", 20, || {
        db.execute(&format!(
            "SELECT id, name FROM page_t WHERE id > 100 AND id % 7 = 0 ORDER BY id LIMIT {PAGE}"
        ))
        .unwrap();
    });
    bench("12 无索引排序 ORDER BY name", 5, || {
        db.execute(&format!(
            "SELECT id, name FROM page_t ORDER BY name LIMIT {PAGE}"
        ))
        .unwrap();
    });
    bench("13 无索引排序 OFFSET 半表", 5, || {
        db.execute(&format!(
            "SELECT id, name FROM page_t ORDER BY name LIMIT {PAGE} OFFSET {mid}"
        ))
        .unwrap();
    });
    bench("14 COUNT(*)", 3, || {
        db.execute("SELECT COUNT(*) FROM page_t").unwrap();
    });
}
