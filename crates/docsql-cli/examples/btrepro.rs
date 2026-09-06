// Minimal repro with op tracing for the Duplicate bug.
fn main() {
    let dir = tempfile::tempdir().unwrap();
    let mut pager = docsql_core::pager::Pager::open(&dir.path().join("bt.db")).unwrap();
    use docsql_core::btree::BTree;
    use docsql_core::value::Value;
    let mut tx = pager.begin_tx();
    let mut tree = BTree::create(&mut pager, &mut tx).unwrap();
    let mut model = std::collections::BTreeMap::<i64, u64>::new();
    let mut seed: u64 = 0x2545F4914F6CDD1D;
    let mut next = || {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        seed >> 33
    };
    for op in 0..2000 {
        let r = next();
        let k = (r % 500) as i64;
        match r % 3 {
            0 | 1 => {
                let v = (r >> 16) % 10_000;
                match tree.insert(&mut pager, &mut tx, Value::Int(k), v, true) {
                    Ok(_) => {
                        model.insert(k, v);
                    }
                    Err(e) => {
                        println!("op {op} INSERT k={k} v={v} failed: {e}");
                        println!("model has k? {}", model.contains_key(&k));
                        // dump leaves
                        let all = tree.scan(&mut pager, &tx).unwrap();
                        let dups: Vec<&(Value, u64)> = all
                            .iter()
                            .filter(|(kv, _)| kv.as_i64() == Some(k))
                            .collect();
                        println!("copies of k in tree: {}", dups.len());
                        return;
                    }
                }
            }
            _ => {
                let got = tree.delete(&mut pager, &mut tx, &Value::Int(k)).unwrap();
                let expect = model.remove(&k).is_some();
                if got != expect {
                    println!("op {op} DELETE k={k} got={got} expect={expect}");
                    return;
                }
            }
        }
    }
    println!("all 2000 ops OK");
}
