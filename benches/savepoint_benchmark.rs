#![allow(dead_code)]

use std::env::current_dir;
use tempfile::NamedTempFile;

use rand::Rng;
use redb::{Database, TableDefinition};
use std::time::{Duration, Instant};

const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("x");

const VALUE_SIZE: usize = 3_000;
const SAVEPOINT_WINDOW: usize = 10;

struct Timing {
    insert: Duration,
    savepoint_creation: Duration,
    savepoint_restore: Duration,
}

/// Returns pairs of key, value
fn random_data(count: usize, key_size: usize, value_size: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut pairs = vec![];

    for _ in 0..count {
        let key: Vec<u8> = (0..key_size).map(|_| rand::rng().random()).collect();
        let value: Vec<u8> = (0..value_size).map(|_| rand::rng().random()).collect();
        pairs.push((key, value));
    }

    pairs
}

fn benchmark(db: &Database, insertions: usize) -> Timing {
    let mut pairs = random_data(insertions, 24, VALUE_SIZE);
    let mut written = 0;

    let mut total_savepoint_creation = Duration::from_micros(0);
    let mut total_insert = Duration::from_micros(0);
    let mut first_savepoint = None;
    for _ in 0..SAVEPOINT_WINDOW {
        let txn = db.begin_write().unwrap();
        let mut table = txn.open_table(TABLE).unwrap();
        let start = Instant::now();
        {
            for _ in 0..(insertions / SAVEPOINT_WINDOW) {
                let len = pairs.len();
                let (key, value) = &mut pairs[written % len];
                key[16..].copy_from_slice(&(written as u64).to_le_bytes());
                table.insert(key.as_slice(), value.as_slice()).unwrap();
                written += 1;
            }
        }
        let end = Instant::now();
        total_insert += end - start;
        drop(table);
        txn.commit().unwrap();

        let txn = db.begin_write().unwrap();
        let start = Instant::now();
        let savepoint_id = txn.persistent_savepoint().unwrap();
        if first_savepoint.is_none() {
            first_savepoint = Some(savepoint_id);
        }
        let end = Instant::now();
        total_savepoint_creation += end - start;
        txn.commit().unwrap();
    }

    let mut txn = db.begin_write().unwrap();
    let savepoint = txn
        .get_persistent_savepoint(first_savepoint.unwrap())
        .unwrap();
    let start = Instant::now();
    txn.restore_savepoint(&savepoint).unwrap();
    let end = Instant::now();
    let restore_duration = end - start;
    txn.abort().unwrap();

    let txn = db.begin_write().unwrap();
    for id in txn.list_persistent_savepoints().unwrap() {
        txn.delete_persistent_savepoint(id).unwrap();
    }
    txn.commit().unwrap();

    Timing {
        insert: total_insert / insertions as u32,
        savepoint_creation: total_savepoint_creation / SAVEPOINT_WINDOW as u32,
        savepoint_restore: restore_duration,
    }
}

fn main() {
    let tmpfile: NamedTempFile = NamedTempFile::new_in(current_dir().unwrap()).unwrap();
    let db = Database::builder().create(tmpfile.path()).unwrap();

    let mut table = comfy_table::Table::new();
    table.set_width(100);
    table.set_header([
        "DB size",
        "insert()",
        "persistent_savepoint()",
        "restore_savepoint()",
    ]);
    for inserts in [
        10_000, 20_000, 40_000, 80_000, 160_000, 320_000, 640_000, 1_280_000, 2_560_000, 5_120_000,
    ] {
        let timing = benchmark(&db, inserts);
        let len = tmpfile.as_file().metadata().unwrap().len();
        let row = vec![
            format!("{}MiB", len / 1024 / 1024),
            format!("{}ns", timing.insert.as_nanos()),
            format!("{}us", timing.savepoint_creation.as_micros()),
            format!("{}us", timing.savepoint_restore.as_micros()),
        ];
        table.add_row(row);
    }

    println!();
    println!("{table}");
}
