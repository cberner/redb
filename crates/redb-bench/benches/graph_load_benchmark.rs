#![allow(dead_code)]

use std::time::{Duration, Instant};

use redb::{
    Database, InsertHint, ReadableDatabase, ReadableTableMetadata, TableDefinition, TableStats,
    WriteTransaction,
};
use tempfile::NamedTempFile;

mod benchmark_dir;
use benchmark_dir::benchmark_dir;

mod common;
use common::*;

const VERTICES: u64 = 1_000_000;
const EDGES: usize = 8_000_000;
const RNG_SEED: u64 = 3;

// Mirrors a graph bulk load: a vertex table plus forward and reverse adjacency
// tables, all built in one transaction. Edge keys are (src, dst) big endian, so
// key order is adjacency order.
const NODES: TableDefinition<&[u8], u64> = TableDefinition::new("nodes");
const FORWARD: TableDefinition<&[u8], u64> = TableDefinition::new("forward");
const REVERSE: TableDefinition<&[u8], u64> = TableDefinition::new("reverse");

fn edge_key(src: u64, dst: u64) -> [u8; 16] {
    let mut key = [0u8; 16];
    key[..8].copy_from_slice(&src.to_be_bytes());
    key[8..].copy_from_slice(&dst.to_be_bytes());
    key
}

fn node_key(id: u64) -> [u8; 8] {
    id.to_be_bytes()
}

// Deduplicated so every arm writes the same set of keys. The insert path would
// silently overwrite a repeated edge, while a bottom-up build rejects it.
fn generate_edges() -> Vec<(u64, u64)> {
    let mut rng = fastrand::Rng::with_seed(RNG_SEED);
    let mut edges: Vec<(u64, u64)> = (0..EDGES)
        .map(|_| (rng.u64(0..VERTICES), rng.u64(0..VERTICES)))
        .collect();
    edges.sort_unstable();
    edges.dedup();
    edges
}

struct Arm {
    load: Duration,
    size: u64,
    compacted: u64,
    nodes: TableStats,
    forward: TableStats,
    reverse: TableStats,
}

fn write_stream<F>(
    txn: &WriteTransaction,
    table: TableDefinition<&[u8], u64>,
    hinted: bool,
    mut fill: F,
) where
    F: FnMut(&mut dyn FnMut(&[u8], u64)),
{
    let mut table = txn.open_table(table).unwrap();
    fill(&mut |key, value| {
        if hinted {
            table
                .insert_with_hint(key, value, InsertHint::Append)
                .unwrap();
        } else {
            table.insert(key, value).unwrap();
        }
    });
}

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    // Insert in arrival order, as an unsorted producer would
    Arrival,
    // Sort first, then insert through the normal write path
    Sorted,
    // Sort first, then insert with the append hint
    SortedHinted,
}

fn run(edges: &[(u64, u64)], mode: Mode) -> Arm {
    let sorted = mode != Mode::Arrival;
    let mut forward: Vec<[u8; 16]> = edges.iter().map(|(s, d)| edge_key(*s, *d)).collect();
    let mut reverse: Vec<[u8; 16]> = edges.iter().map(|(s, d)| edge_key(*d, *s)).collect();
    let mut nodes: Vec<u64> = (0..VERTICES).collect();

    if sorted {
        forward.sort_unstable();
        reverse.sort_unstable();
    } else {
        // Same data, arrival order instead of key order.
        let mut rng = fastrand::Rng::with_seed(RNG_SEED + 1);
        rng.shuffle(&mut forward);
        rng.shuffle(&mut reverse);
        rng.shuffle(&mut nodes);
    }

    let file: NamedTempFile = NamedTempFile::new_in(benchmark_dir()).unwrap();
    let mut db = Database::builder()
        .set_cache_size(CACHE_SIZE)
        .create(file.path())
        .unwrap();

    let start = Instant::now();
    let txn = db.begin_write().unwrap();
    let hinted = mode == Mode::SortedHinted;
    write_stream(&txn, NODES, hinted, |insert| {
        for id in &nodes {
            insert(node_key(*id).as_slice(), *id);
        }
    });
    write_stream(&txn, FORWARD, hinted, |insert| {
        for key in &forward {
            insert(key.as_slice(), 1);
        }
    });
    write_stream(&txn, REVERSE, hinted, |insert| {
        for key in &reverse {
            insert(key.as_slice(), 1);
        }
    });
    txn.commit().unwrap();
    let load = start.elapsed();

    let size = database_size(file.path());
    let read = db.begin_read().unwrap();
    let stats = (
        read.open_table(NODES).unwrap().stats().unwrap(),
        read.open_table(FORWARD).unwrap().stats().unwrap(),
        read.open_table(REVERSE).unwrap().stats().unwrap(),
    );
    drop(read);

    db.compact().unwrap();
    let compacted = database_size(file.path());

    Arm {
        load,
        size,
        compacted,
        nodes: stats.0,
        forward: stats.1,
        reverse: stats.2,
    }
}

// Share of the table's bytes that hold user data. Independent of leaf size, so
// it stays comparable when a build packs leaves larger than one page.
fn fill_percent(stats: &TableStats) -> f64 {
    let total = stats.stored_bytes() + stats.metadata_bytes() + stats.fragmented_bytes();
    if total == 0 {
        return 0.0;
    }
    100.0 * stats.stored_bytes() as f64 / total as f64
}

fn describe(label: &str, arm: &Arm) {
    println!(
        "{label}: {}ms, {} uncompacted, {} compacted",
        arm.load.as_millis(),
        mib(arm.size),
        mib(arm.compacted)
    );
    for (name, stats) in [
        ("nodes", &arm.nodes),
        ("forward", &arm.forward),
        ("reverse", &arm.reverse),
    ] {
        println!(
            "    {name:>8}: {:>9} leaf pages, {:>5.1}% data, {} fragmented",
            stats.leaf_pages(),
            fill_percent(stats),
            mib(stats.fragmented_bytes())
        );
    }
}

fn mib(bytes: u64) -> String {
    format!("{:.2} MiB", bytes as f64 / 1024.0 / 1024.0)
}

fn main() {
    let _ = env_logger::try_init();
    let edges = generate_edges();
    println!(
        "{VERTICES} vertices, {} unique edges, forward + reverse adjacency",
        edges.len()
    );
    let arrival = run(&edges, Mode::Arrival);
    describe("        arrival order", &arrival);
    let sorted = run(&edges, Mode::Sorted);
    describe("         sorted order", &sorted);
    let hinted = run(&edges, Mode::SortedHinted);
    describe("  sorted order + hint", &hinted);

    for (label, arm) in [("sorted", &sorted), ("hinted", &hinted)] {
        println!(
            "{label} vs arrival: {:.3}x size, {:.3}x load",
            arm.compacted as f64 / arrival.compacted as f64,
            arm.load.as_secs_f64() / arrival.load.as_secs_f64(),
        );
    }
}
