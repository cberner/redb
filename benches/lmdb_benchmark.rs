use tempfile::TempDir;

use rand::Rng;
use std::path::Path;
use std::time::SystemTime;

/// Returns pairs of key, value
fn gen_data(count: usize, key_size: usize, value_size: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut pairs = vec![];

    for _ in 0..count {
        let key: Vec<u8> = (0..key_size).map(|_| rand::thread_rng().gen()).collect();
        let value: Vec<u8> = (0..value_size).map(|_| rand::thread_rng().gen()).collect();
        pairs.push((key, value));
    }

    pairs
}

fn lmdb_zero_bench(path: &str) {
    let env = unsafe {
        lmdb_zero::EnvBuilder::new()
            .unwrap()
            .open(path, lmdb_zero::open::Flags::empty(), 0o600)
            .unwrap()
    };
    unsafe {
        env.set_mapsize(4096 * 1024 * 1024).unwrap();
    }

    let elements = 10_000_000;

    let pairs = gen_data(1000, 16, 2000);

    let db =
        lmdb_zero::Database::open(&env, None, &lmdb_zero::DatabaseOptions::defaults()).unwrap();
    {
        let start = SystemTime::now();
        let txn = lmdb_zero::WriteTransaction::new(&env).unwrap();
        {
            let mut access = txn.access();
            for i in 0..elements {
                let (key, value) = &pairs[i % pairs.len()];
                access
                    .put(&db, key, value, lmdb_zero::put::Flags::empty())
                    .unwrap();
            }
        }
        txn.commit().unwrap();

        let end = SystemTime::now();
        let duration = end.duration_since(start).unwrap();
        println!(
            "lmdb-zero: Loaded {} items in {}ms",
            elements,
            duration.as_millis()
        )
    }
}

fn lmdb_rkv_bench(path: &Path) {
    use lmdb::Transaction;
    let env = lmdb::Environment::new().open(path).unwrap();
    env.set_map_size(4096 * 1024 * 1024).unwrap();

    let elements = 10_000_000;

    let pairs = gen_data(1000, 16, 2000);

    let db = env.open_db(None).unwrap();
    let start = SystemTime::now();
    let mut txn = env.begin_rw_txn().unwrap();
    {
        for i in 0..elements {
            let (key, value) = &pairs[i % pairs.len()];
            txn.put(db, key, value, lmdb::WriteFlags::empty()).unwrap();
        }
    }
    txn.commit().unwrap();

    let end = SystemTime::now();
    let duration = end.duration_since(start).unwrap();
    println!(
        "lmdb-rkv: Loaded {} items in {}ms",
        elements,
        duration.as_millis()
    )
}

fn main() {
    let tmpfile: TempDir = tempfile::tempdir().unwrap();
    let path = tmpfile.path().to_str().unwrap();
    lmdb_zero_bench(path);
    lmdb_rkv_bench(tmpfile.path());
}
