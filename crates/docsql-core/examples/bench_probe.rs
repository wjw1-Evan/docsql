//! 谓词探针基准:IN 列表 / BETWEEN / LIKE 前缀在索引列上的探针路径,
//! 对照无索引列上的全表扫描基线。运行:
//! cargo run -p docsql-core --example bench_probe --release [行数]

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
        "{label:<52} median {:9.3} ms  mean {:9.3} ms",
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
    let mut db = Database::open(&dir.path().join("probe.db")).unwrap();
    db.execute("CREATE TABLE probe_t (id INT PRIMARY KEY NOT NULL, name TEXT NOT NULL, v INT NOT NULL, plain INT NOT NULL)").unwrap();
    db.execute("CREATE INDEX idx_probe_v ON probe_t (v)")
        .unwrap();
    db.execute("CREATE INDEX idx_probe_name ON probe_t (name)")
        .unwrap();

    // Group-commit load: the write path is not what this benchmark measures.
    let t = Instant::now();
    db.set_async_commit(true);
    const BATCH: i64 = 10_000;
    let mut i = 0i64;
    while i < n {
        db.execute("BEGIN").unwrap();
        for j in 0..BATCH.min(n - i) {
            let id = i + j;
            db.execute(&format!(
                "INSERT INTO probe_t VALUES ({id}, 'name{id}', {}, {})",
                id % 1000,
                id % 1000
            ))
            .unwrap();
        }
        db.execute("COMMIT").unwrap();
        i += BATCH;
    }
    db.set_async_commit(false);
    println!("装载 {n} 行: {:.1} s\n", t.elapsed().as_secs_f64());

    // 稠密 IN(键聚在窄带):v ∈ {17,233,571},各约 500 行,共 ~1500 行。
    bench(" 1 IN 稠密(v IN 17,233,571)", 20, || {
        db.execute("SELECT id, name FROM probe_t WHERE v IN (17, 233, 571)")
            .unwrap();
    });
    // 稀疏 IN(键散布全空间):多探针 O(k log n),范围折叠会退化全表。
    let ids = [7i64, 199_999, 345_678, 499_990];
    let id_list = ids.iter().map(|i| i.to_string()).collect::<Vec<_>>();
    bench(" 2 IN 稀疏(id IN 4 个散点)", 20, || {
        db.execute(&format!(
            "SELECT id, name FROM probe_t WHERE id IN ({})",
            id_list.join(", ")
        ))
        .unwrap();
    });
    // IN 子查询(预 pass 改写成 InList 字面量后同路)。
    bench(" 3 IN (SELECT …) 改写", 5, || {
        db.execute("SELECT id FROM probe_t WHERE v IN (SELECT v FROM probe_t WHERE id < 5)")
            .unwrap();
    });
    // 闭区间:两端含入。
    bench(" 4 BETWEEN (v BETWEEN 200 AND 209)", 20, || {
        db.execute("SELECT id, name FROM probe_t WHERE v BETWEEN 200 AND 209")
            .unwrap();
    });
    // 前缀 LIKE:name1234xx 命中 100 行(name12340..name123499)。
    bench(" 5 LIKE 前缀 (name LIKE 'name1234%')", 20, || {
        db.execute("SELECT id FROM probe_t WHERE name LIKE 'name1234%'")
            .unwrap();
    });
    // 无通配符前缀 `_`/中段 `%`:保持全扫(回归对照,应与旧路径同价)。
    bench(" 6 LIKE 中段% 全扫(对照)", 3, || {
        db.execute("SELECT id FROM probe_t WHERE name LIKE 'name1%4%'")
            .unwrap();
    });
    // 非索引列同名谓词:全扫基线(新旧同价)。
    bench(" 7 全扫基线(plain IN 17,233,571)", 3, || {
        db.execute("SELECT id, name FROM probe_t WHERE plain IN (17, 233, 571)")
            .unwrap();
    });
    bench(" 8 全扫基线(plain BETWEEN 200 AND 209)", 3, || {
        db.execute("SELECT id, name FROM probe_t WHERE plain BETWEEN 200 AND 209")
            .unwrap();
    });
    // 探针 + 窗口:ORDER BY 索引键 + LIMIT,exact 截断。
    bench(" 9 IN + ORDER BY id LIMIT 20", 20, || {
        db.execute("SELECT id, name FROM probe_t WHERE v IN (17, 233, 571) ORDER BY id LIMIT 20")
            .unwrap();
    });
    bench("10 BETWEEN + ORDER BY id DESC LIMIT 20", 20, || {
        db.execute(
            "SELECT id, name FROM probe_t WHERE v BETWEEN 200 AND 209 ORDER BY id DESC LIMIT 20",
        )
        .unwrap();
    });
}
