use std::env::current_dir;
use std::{fs, process, thread};
use tempfile::NamedTempFile;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use redb::{Database, ReadableTableMetadata, TableDefinition};
use std::time::Instant;

const ELEMENTS: u64 = 1_000_000;
const RNG_SEED: u64 = 3;

const TABLE1: TableDefinition<u128, u128> = TableDefinition::new("x");
const TABLE2: TableDefinition<u128, u128> = TableDefinition::new("y");

#[inline(never)]
fn single_threaded(values: &[u128]) {
    let tmpfile: NamedTempFile = NamedTempFile::new_in(current_dir().unwrap()).unwrap();
    let db = Database::builder().create(tmpfile.path()).unwrap();

    let start = Instant::now();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table1 = write_txn.open_table(TABLE1).unwrap();
        let mut table2 = write_txn.open_table(TABLE2).unwrap();

        for value in values.iter() {
            table1.insert(value, value).unwrap();
            table2.insert(value, value).unwrap();
        }
    }
    write_txn.commit().unwrap();
    let end = Instant::now();
    let duration = end - start;
    println!(
        "single threaded load:  {} pairs in {}ms",
        2 * ELEMENTS,
        duration.as_millis()
    );
    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE1).unwrap();
    assert_eq!(table.len().unwrap(), ELEMENTS);
    let table = read_txn.open_table(TABLE2).unwrap();
    assert_eq!(table.len().unwrap(), ELEMENTS);
}

#[inline(never)]
fn multi_threaded(values: &[u128]) {
    let tmpfile: NamedTempFile = NamedTempFile::new_in(current_dir().unwrap()).unwrap();
    let db = Database::builder().create(tmpfile.path()).unwrap();

    let start = Instant::now();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table1 = write_txn.open_table(TABLE1).unwrap();
        let mut table2 = write_txn.open_table(TABLE2).unwrap();

        thread::scope(|s| {
            s.spawn(|| {
                for value in values.iter() {
                    table1.insert(value, value).unwrap();
                }
            });
            s.spawn(|| {
                for value in values.iter() {
                    table2.insert(value, value).unwrap();
                }
            });
        });
    }
    write_txn.commit().unwrap();
    let end = Instant::now();
    let duration = end - start;
    println!(
        "2 threaded load:  {} pairs in {}ms",
        2 * ELEMENTS,
        duration.as_millis()
    );
    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE1).unwrap();
    assert_eq!(table.len().unwrap(), ELEMENTS);
    let table = read_txn.open_table(TABLE2).unwrap();
    assert_eq!(table.len().unwrap(), ELEMENTS);
}

// TODO: multi-threaded inserts are slower. Probably due to lock contention checking dirty pages
fn main() {
    let mut rng = StdRng::seed_from_u64(RNG_SEED);
    let mut values = vec![];
    for _ in 0..ELEMENTS {
        values.push(rng.gen());
    }

    let tmpdir = current_dir().unwrap().join(".benchmark");
    fs::create_dir(&tmpdir).unwrap();

    let tmpdir2 = tmpdir.clone();
    ctrlc::set_handler(move || {
        fs::remove_dir_all(&tmpdir2).unwrap();
        process::exit(1);
    })
    .unwrap();

    single_threaded(&values);

    multi_threaded(&values);

    fs::remove_dir_all(&tmpdir).unwrap();
}
