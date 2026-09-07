// 自动提交写(每条语句独立提交):逐条 fsync vs 异步组提交
use docsql_core::engine::Database;
use std::time::Instant;

fn run(async_mode: bool, label: &str) {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Database::open(&dir.path().join("b.db")).unwrap();
    db.set_async_commit(async_mode);
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    const N: usize = 20_000;
    let t = Instant::now();
    for i in 0..N as i64 {
        db.execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .unwrap();
    }
    let ms = t.elapsed().as_secs_f64() * 1000.0;
    db.sync_pending().unwrap();
    println!(
        "{label}: {ms:8.1} ms  ({:.0} 行/秒)",
        N as f64 / ms * 1000.0
    );
}

fn main() {
    run(false, "自动提交(逐条 fsync)");
    run(true, "自动提交(异步组提交)");
    run(false, "自动提交(逐条 fsync)");
    run(true, "自动提交(异步组提交)");
}
