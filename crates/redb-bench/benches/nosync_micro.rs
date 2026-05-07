// Focused micro-benchmark for measuring the nosync write throughput.
// Runs the relevant portion of the redb_benchmark a few times so we can compare
// before/after changes to the cached file's writeback behavior.

use std::env::current_dir;
use std::fs;
use std::time::Instant;

use redb::{Durability, ReadableDatabase, ReadableTableMetadata, TableDefinition};
use tempfile::NamedTempFile;

const X: TableDefinition<&[u8], &[u8]> = TableDefinition::new("x");

const KEY_SIZE: usize = 24;
const VALUE_SIZE: usize = 150;
const RNG_SEED: u64 = 3;
const BULK_ELEMENTS: usize = 1_000_000;
const NOSYNC_WRITES: usize = 50_000;
const ITERATIONS: usize = 3;

const CACHE_SIZE: usize = 4 * 1_024 * 1_024 * 1_024; // 4GB

fn random_pair(rng: &mut fastrand::Rng) -> ([u8; KEY_SIZE], Vec<u8>) {
    let mut key = [0u8; KEY_SIZE];
    rng.fill(&mut key);
    let mut value = vec![0u8; VALUE_SIZE];
    rng.fill(&mut value);
    (key, value)
}

#[cfg(target_os = "linux")]
fn read_io_counters() -> (u64, u64) {
    // (write_bytes, write_syscalls)
    let s = fs::read_to_string("/proc/self/io").unwrap_or_default();
    let mut wbytes = 0u64;
    let mut wsyscalls = 0u64;
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("write_bytes:") {
            wbytes = rest.trim().parse().unwrap_or(0);
        }
        if let Some(rest) = line.strip_prefix("syscw:") {
            wsyscalls = rest.trim().parse().unwrap_or(0);
        }
    }
    (wbytes, wsyscalls)
}

#[cfg(not(target_os = "linux"))]
fn read_io_counters() -> (u64, u64) {
    (0, 0)
}

fn run_once(rng_offset: u64) -> u128 {
    let tmpdir = current_dir().unwrap().join(".nosync_micro");
    let _ = fs::remove_dir_all(&tmpdir);
    fs::create_dir(&tmpdir).unwrap();
    let tmpfile = NamedTempFile::new_in(&tmpdir).unwrap();
    let db = redb::Database::builder()
        .set_cache_size(CACHE_SIZE)
        .create(tmpfile.path())
        .unwrap();
    {
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(X).unwrap();
            let mut rng = fastrand::Rng::with_seed(RNG_SEED);
            for _ in 0..BULK_ELEMENTS {
                let (k, v) = random_pair(&mut rng);
                t.insert(k.as_slice(), v.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
    }

    // Baseline: empty nosync transactions (no inserts) to isolate per-commit overhead.
    let baseline_start = Instant::now();
    for _ in 0..NOSYNC_WRITES {
        let mut txn = db.begin_write().unwrap();
        txn.set_durability(Durability::None).unwrap();
        txn.commit().unwrap();
    }
    let baseline_elapsed = baseline_start.elapsed().as_millis();
    println!("  -> empty nosync commits: {baseline_elapsed}ms");

    let (wbytes_before, wsys_before) = read_io_counters();
    let mut rng = fastrand::Rng::with_seed(RNG_SEED.wrapping_add(rng_offset));
    let start = Instant::now();
    for _ in 0..NOSYNC_WRITES {
        let mut txn = db.begin_write().unwrap();
        txn.set_durability(Durability::None).unwrap();
        {
            let mut t = txn.open_table(X).unwrap();
            let (k, v) = random_pair(&mut rng);
            t.insert(k.as_slice(), v.as_slice()).unwrap();
        }
        txn.commit().unwrap();
    }
    let elapsed = start.elapsed().as_millis();
    let (wbytes_after, wsys_after) = read_io_counters();
    println!(
        "  -> nosync I/O: write_bytes={} write_syscalls={}",
        wbytes_after - wbytes_before,
        wsys_after - wsys_before,
    );

    let r = db.begin_read().unwrap();
    let _ = r.open_table(X).unwrap().len().unwrap();
    drop(r);
    drop(db);
    fs::remove_dir_all(&tmpdir).unwrap();
    elapsed
}

fn main() {
    let mut runs = Vec::new();
    for i in 0..ITERATIONS {
        let ms = run_once(i as u64);
        println!("iter {i}: nosync writes={NOSYNC_WRITES} in {ms}ms");
        runs.push(ms);
    }
    runs.sort();
    let min = runs.first().copied().unwrap_or(0);
    let max = runs.last().copied().unwrap_or(0);
    let median = runs[runs.len() / 2];
    println!(
        "min={min}ms median={median}ms max={max}ms over {} runs",
        runs.len()
    );
}
