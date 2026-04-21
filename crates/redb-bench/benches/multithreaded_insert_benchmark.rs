use std::env::current_dir;
use std::{fs, process, thread};
use tempfile::NamedTempFile;

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use redb::{Database, ReadableDatabase, ReadableTableMetadata, TableDefinition};
use std::time::Instant;

const ELEMENTS: u64 = 1_000_000;
const RNG_SEED: u64 = 3;
const THREAD_COUNTS: &[usize] = &[1, 2, 4, 8, 16, 32];

#[inline(never)]
fn benchmark(values: &[u128], num_threads: usize) {
    assert_eq!(values.len() as u64, ELEMENTS);
    assert_eq!(ELEMENTS % num_threads as u64, 0);
    let elements_per_thread = (ELEMENTS / num_threads as u64) as usize;

    let tmpfile: NamedTempFile = NamedTempFile::new_in(current_dir().unwrap()).unwrap();
    let db = Database::builder().create(tmpfile.path()).unwrap();

    let table_names: Vec<String> = (0..num_threads).map(|i| format!("table_{i}")).collect();
    let table_defs: Vec<TableDefinition<u128, u128>> = table_names
        .iter()
        .map(|n| TableDefinition::new(n.as_str()))
        .collect();

    let start = Instant::now();
    let write_txn = db.begin_write().unwrap();
    {
        let mut tables: Vec<_> = table_defs
            .iter()
            .map(|def| write_txn.open_table(*def).unwrap())
            .collect();

        thread::scope(|s| {
            for (i, table) in tables.iter_mut().enumerate() {
                let chunk = &values[i * elements_per_thread..(i + 1) * elements_per_thread];
                s.spawn(move || {
                    for value in chunk.iter() {
                        table.insert(value, value).unwrap();
                    }
                });
            }
        });
    }
    write_txn.commit().unwrap();
    let duration = start.elapsed();
    println!(
        "{} threaded load: {} pairs in {}ms",
        num_threads,
        ELEMENTS,
        duration.as_millis()
    );

    let read_txn = db.begin_read().unwrap();
    for def in &table_defs {
        let table = read_txn.open_table(*def).unwrap();
        assert_eq!(table.len().unwrap(), elements_per_thread as u64);
    }
}

// TODO: multi-threaded inserts are slower. Probably due to lock contention checking dirty pages
fn main() {
    let mut rng = StdRng::seed_from_u64(RNG_SEED);
    let mut values = vec![];
    for _ in 0..ELEMENTS {
        values.push(rng.random());
    }

    let tmpdir = current_dir().unwrap().join(".benchmark");
    fs::create_dir(&tmpdir).unwrap();

    let tmpdir2 = tmpdir.clone();
    ctrlc::set_handler(move || {
        fs::remove_dir_all(&tmpdir2).unwrap();
        process::exit(1);
    })
    .unwrap();

    for &num_threads in THREAD_COUNTS {
        benchmark(&values, num_threads);
    }

    fs::remove_dir_all(&tmpdir).unwrap();
}
