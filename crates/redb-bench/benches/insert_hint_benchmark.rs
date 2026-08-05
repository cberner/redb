#![allow(dead_code)]

use tempfile::NamedTempFile;

mod benchmark_dir;
use benchmark_dir::benchmark_dir;

use rand::RngExt;
use redb::{
    Database, InsertHint, ReadableDatabase, ReadableTableMetadata, TableDefinition, TableStats,
};
use std::time::{Duration, Instant};

const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("x");

const ELEMENTS: usize = 1_000_000;
const KEY_SIZE: usize = 24;
const VALUE_SIZE: usize = 150;

struct Timing {
    load: Duration,
    stats: TableStats,
    size: u64,
}

fn random_keys() -> Vec<[u8; KEY_SIZE]> {
    let mut rng = rand::rng();
    (0..ELEMENTS)
        .map(|_| {
            let mut key = [0u8; KEY_SIZE];
            rng.fill(&mut key[..]);
            key
        })
        .collect()
}

fn benchmark(keys: &[[u8; KEY_SIZE]], hint: Option<InsertHint>) -> Timing {
    let tmpfile: NamedTempFile = NamedTempFile::new_in(benchmark_dir()).unwrap();
    let db = Database::builder().create(tmpfile.path()).unwrap();
    let value = vec![0u8; VALUE_SIZE];

    let start = Instant::now();
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        for key in keys {
            match hint {
                Some(hint) => table
                    .insert_with_hint(key.as_slice(), value.as_slice(), hint)
                    .unwrap(),
                None => table.insert(key.as_slice(), value.as_slice()).unwrap(),
            };
        }
    }
    txn.commit().unwrap();
    let load = start.elapsed();

    let txn = db.begin_read().unwrap();
    let stats = txn.open_table(TABLE).unwrap().stats().unwrap();
    let size = tmpfile.as_file().metadata().unwrap().len();

    Timing { load, stats, size }
}

// Share of the table's bytes that hold user data, rather than padding left
// behind by splitting
fn data_percent(stats: &TableStats) -> f64 {
    let total = stats.stored_bytes() + stats.metadata_bytes() + stats.fragmented_bytes();
    100.0 * stats.stored_bytes() as f64 / total as f64
}

fn main() {
    let random = random_keys();
    let mut ascending = random.clone();
    ascending.sort_unstable();

    let mut table = comfy_table::Table::new();
    table.set_width(100);
    table.set_header(["", "load", "leaf pages", "data", "DB size"]);
    for (name, keys, hint) in [
        ("random order", &random, None),
        ("ascending order", &ascending, None),
        (
            "ascending order, Append hint",
            &ascending,
            Some(InsertHint::Append),
        ),
    ] {
        let timing = benchmark(keys, hint);
        table.add_row(vec![
            name.to_string(),
            format!("{}ms", timing.load.as_millis()),
            format!("{}", timing.stats.leaf_pages()),
            format!("{:.1}%", data_percent(&timing.stats)),
            format!("{}MiB", timing.size / 1024 / 1024),
        ]);
    }

    println!();
    println!("{table}");
}
