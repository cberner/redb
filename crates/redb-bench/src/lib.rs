//! Shared benchmark harness: the BenchDatabase trait family that each storage engine
//! implements, the workloads that drive them, and the result formatting.
//!
//! The redb adapter lives here; adapters for the other engines live in redb-bench-compare,
//! which keeps their heavyweight native dependencies out of this crate's build.

use redb::{
    AccessGuard, Durability, ReadableDatabase, ReadableTable, ReadableTableMetadata,
    TableDefinition,
};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

pub fn benchmark_dir() -> PathBuf {
    std::env::var_os("REDB_BENCHMARK_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::current_dir().unwrap())
}

#[allow(dead_code)]
const X: TableDefinition<&[u8], &[u8]> = TableDefinition::new("x");

const READ_ITERATIONS: usize = 3;
const BULK_ELEMENTS: usize = 5_000_000;
const SORTED_ELEMENTS: usize = 1_000_000;
const INDIVIDUAL_WRITES: usize = 1_000;
const NOSYNC_WRITES: usize = 50_000;
const BATCH_WRITES: usize = 100;
const BATCH_SIZE: usize = 1000;
const SCAN_ITERATIONS: usize = 3;
const NUM_READS: usize = 1_000_000;
const NUM_SCANS: usize = 500_000;
const SCAN_LEN: usize = 10;
const POP_REMOVALS: usize = 500_000;
const POP_SAMPLE_REMOVALS: usize = 5_000;
const SLOW_POP_SAMPLE_LIMIT: Duration = Duration::from_secs(1);
const KEY_SIZE: usize = 24;
const VALUE_SIZE: usize = 150;
const RNG_SEED: u64 = 3;

pub const CACHE_SIZE: usize = 4 * 1_024 * 1_024 * 1_024; // 4GB

/// Returns pairs of key, value
fn random_pair(rng: &mut fastrand::Rng) -> ([u8; KEY_SIZE], Vec<u8>) {
    let mut key = [0u8; KEY_SIZE];
    rng.fill(&mut key);
    let mut value = vec![0u8; VALUE_SIZE];
    rng.fill(&mut value);

    (key, value)
}

fn make_rng() -> fastrand::Rng {
    fastrand::Rng::with_seed(RNG_SEED)
}

fn make_rng_shards(shards: usize, elements: usize) -> Vec<fastrand::Rng> {
    let mut rngs = vec![];
    let elements_per_shard = elements / shards;
    for i in 0..shards {
        let mut rng = make_rng();
        for _ in 0..(i * elements_per_shard) {
            random_pair(&mut rng);
        }
        rngs.push(rng);
    }

    rngs
}

// Middle timing of the sorted samples, so that a cold cache on the first iteration, or a
// stray outlier, does not skew the reported rate. With an even number of samples this is
// the upper of the two middle ones.
fn median_duration(durations: &mut [Duration]) -> Duration {
    durations.sort_unstable();
    durations[durations.len() / 2]
}

#[inline(never)]
fn nosync_writes<T: BenchDatabase + Send + Sync>(
    connection: &T::C<'_>,
    rng: &mut fastrand::Rng,
) -> ResultType {
    let start = Instant::now();
    {
        for _ in 0..NOSYNC_WRITES {
            let mut txn = connection.write_transaction();
            let mut inserter = txn.get_inserter();
            let (key, value) = random_pair(rng);
            inserter.insert(&key, &value).unwrap();
            drop(inserter);
            txn.commit().unwrap();
        }
    }

    let end = Instant::now();
    let duration = end - start;
    let result = ResultType::txns(NOSYNC_WRITES, duration);
    println!(
        "{}: Wrote {} individual items in {}ms ({}), with nosync",
        T::db_type_name(),
        NOSYNC_WRITES,
        duration.as_millis(),
        result.with_unit()
    );

    result
}

pub fn benchmark<T: BenchDatabase + Send + Sync>(
    mut db: T,
    path: &Path,
) -> Vec<(String, ResultType)> {
    let mut rng = make_rng();
    let mut results = Vec::new();
    let mut connection = db.connect();

    let start = Instant::now();
    let mut txn = connection.write_transaction();
    let mut inserter = txn.get_inserter();
    {
        for _ in 0..BULK_ELEMENTS {
            let (key, value) = random_pair(&mut rng);
            inserter.insert(&key, &value).unwrap();
        }
    }
    drop(inserter);
    txn.commit().unwrap();

    let end = Instant::now();
    let duration = end - start;
    let result = ResultType::keys(BULK_ELEMENTS, duration);
    println!(
        "{}: Bulk loaded {} items in {}ms ({})",
        T::db_type_name(),
        BULK_ELEMENTS,
        duration.as_millis(),
        result.with_unit()
    );
    results.push(("bulk load".to_string(), result));

    let start = Instant::now();
    {
        for _ in 0..INDIVIDUAL_WRITES {
            let mut txn = connection.write_transaction();
            let mut inserter = txn.get_inserter();
            let (key, value) = random_pair(&mut rng);
            inserter.insert(&key, &value).unwrap();
            drop(inserter);
            txn.commit().unwrap();
        }
    }

    let end = Instant::now();
    let duration = end - start;
    let result = ResultType::txns(INDIVIDUAL_WRITES, duration);
    println!(
        "{}: Wrote {} individual items in {}ms ({})",
        T::db_type_name(),
        INDIVIDUAL_WRITES,
        duration.as_millis(),
        result.with_unit()
    );
    results.push(("individual writes".to_string(), result));

    let start = Instant::now();
    {
        for _ in 0..BATCH_WRITES {
            let mut txn = connection.write_transaction();
            let mut inserter = txn.get_inserter();
            for _ in 0..BATCH_SIZE {
                let (key, value) = random_pair(&mut rng);
                inserter.insert(&key, &value).unwrap();
            }
            drop(inserter);
            txn.commit().unwrap();
        }
    }

    let end = Instant::now();
    let duration = end - start;
    // Batches are large enough that the rate of keys written, rather than of commits, is
    // what the phase measures
    let result = ResultType::keys(BATCH_WRITES * BATCH_SIZE, duration);
    println!(
        "{}: Wrote {} batches of {} items in {}ms ({})",
        T::db_type_name(),
        BATCH_WRITES,
        BATCH_SIZE,
        duration.as_millis(),
        result.with_unit()
    );
    results.push(("small batch writes".to_string(), result));
    // Sorted inserts have to run last, to keep the size measurements comparable, but belong
    // with the other write phases in the results
    let sorted_inserts_row = results.len();

    if connection.set_sync(false) {
        let result = nosync_writes::<T>(&connection, &mut rng);
        results.push(("nosync writes".to_string(), result));
    } else {
        // Still perform the writes to make sure that future benchmarks aren't skewed
        let mut txn = connection.write_transaction();
        let mut inserter = txn.get_inserter();
        for _ in 0..NOSYNC_WRITES {
            let (key, value) = random_pair(&mut rng);
            inserter.insert(&key, &value).unwrap();
        }
        drop(inserter);
        txn.commit().unwrap();
        results.push(("nosync writes".to_string(), ResultType::NA));
    }
    connection.set_sync(true);

    let elements = BULK_ELEMENTS + INDIVIDUAL_WRITES + BATCH_SIZE * BATCH_WRITES + NOSYNC_WRITES;
    let txn = connection.read_transaction();
    {
        {
            let start = Instant::now();
            let len = txn.get_reader().len();
            assert_eq!(len, elements as u64);
            let end = Instant::now();
            let duration = end - start;
            let result = ResultType::Latency(duration);
            println!("{}: len() in {}", T::db_type_name(), result.with_unit());
            results.push(("len()".to_string(), result));
        }

        let mut read_durations = [Duration::ZERO; READ_ITERATIONS];
        for read_duration in &mut read_durations {
            let mut rng = make_rng();
            let start = Instant::now();
            let mut checksum = 0u64;
            let mut expected_checksum = 0u64;
            let mut reader = txn.get_reader();
            for _ in 0..NUM_READS {
                let (key, value) = random_pair(&mut rng);
                let result = reader.get(&key).unwrap();
                checksum += result.as_ref()[0] as u64;
                expected_checksum += value[0] as u64;
            }
            assert_eq!(checksum, expected_checksum);
            *read_duration = start.elapsed();
        }
        let duration = median_duration(&mut read_durations);
        let result = ResultType::keys(NUM_READS, duration);
        println!(
            "{}: Random read {} items in {}ms ({}), median of {} runs",
            T::db_type_name(),
            NUM_READS,
            duration.as_millis(),
            result.with_unit(),
            READ_ITERATIONS
        );
        results.push(("random reads".to_string(), result));

        let mut scan_durations = [Duration::ZERO; SCAN_ITERATIONS];
        for scan_duration in &mut scan_durations {
            let mut rng = make_rng();
            let start = Instant::now();
            let mut reader = txn.get_reader();
            let mut value_sum = 0;
            for _ in 0..NUM_SCANS {
                let (key, _value) = random_pair(&mut rng);
                let mut iter = reader.range_from(&key);
                for _ in 0..SCAN_LEN {
                    if let Some((_, value)) = iter.next() {
                        value_sum += value.as_ref()[0];
                    } else {
                        break;
                    }
                }
            }
            assert!(value_sum > 0);
            *scan_duration = start.elapsed();
        }
        // Rated per range read, rather than per key, because the keys after the first
        // are reached by stepping the iterator rather than by a lookup
        let duration = median_duration(&mut scan_durations);
        let result = ResultType::scans(NUM_SCANS, duration);
        println!(
            "{}: Random range read {} x {} elements in {}ms ({}), median of {} runs",
            T::db_type_name(),
            NUM_SCANS,
            SCAN_LEN,
            duration.as_millis(),
            result.with_unit(),
            SCAN_ITERATIONS
        );
        results.push(("random range reads".to_string(), result));
    }
    drop(txn);

    for num_threads in [4, 8, 16, 32] {
        let barrier = Arc::new(std::sync::Barrier::new(num_threads));
        let mut rngs = make_rng_shards(num_threads, elements);
        let start = Instant::now();

        thread::scope(|s| {
            for _ in 0..num_threads {
                let barrier = barrier.clone();
                let connection = db.connect();
                let rng = rngs.pop().unwrap();
                s.spawn(move || {
                    barrier.wait();
                    let txn = connection.read_transaction();
                    let mut checksum = 0u64;
                    let mut expected_checksum = 0u64;
                    let mut reader = txn.get_reader();
                    let mut rng = rng.clone();
                    for _ in 0..(elements / num_threads) {
                        let (key, value) = random_pair(&mut rng);
                        let result = reader.get(&key).unwrap();
                        checksum += result.as_ref()[0] as u64;
                        expected_checksum += value[0] as u64;
                    }
                    assert_eq!(checksum, expected_checksum);
                });
            }
        });

        let end = Instant::now();
        let duration = end - start;
        let result = ResultType::keys(elements, duration);
        println!(
            "{}: Random read ({} threads) {} items in {}ms ({})",
            T::db_type_name(),
            num_threads,
            elements,
            duration.as_millis(),
            result.with_unit()
        );
        results.push((format!("random reads ({num_threads} threads)"), result));
    }

    let start = Instant::now();
    let deletes = elements / 2;
    {
        let mut rng = make_rng();
        let mut txn = connection.write_transaction();
        let mut inserter = txn.get_inserter();
        for _ in 0..deletes {
            let (key, _value) = random_pair(&mut rng);
            inserter.remove(&key).unwrap();
        }
        drop(inserter);
        txn.commit().unwrap();
    }

    let end = Instant::now();
    let duration = end - start;
    let result = ResultType::keys(deletes, duration);
    println!(
        "{}: Removed {} items in {}ms ({})",
        T::db_type_name(),
        deletes,
        duration.as_millis(),
        result.with_unit()
    );
    results.push(("removals".to_string(), result));

    // Retain benchmark: drop every other entry via a predicate, commit the transaction, and
    // then repopulate with the same number of random entries so downstream phases (uncompacted
    // size, compaction) see a table of similar shape.
    let start = Instant::now();
    let removed = {
        let mut txn = connection.write_transaction();
        let mut inserter = txn.get_inserter();
        let mut counter: u64 = 0;
        let removed = inserter
            .retain(|_, _| {
                let keep = counter.is_multiple_of(2);
                counter += 1;
                keep
            })
            .unwrap();
        drop(inserter);
        txn.commit().unwrap();
        removed
    };
    let end = Instant::now();
    let duration = end - start;
    let result = ResultType::keys(removed as usize, duration);
    println!(
        "{}: Retain removed {} items in {}ms ({})",
        T::db_type_name(),
        removed,
        duration.as_millis(),
        result.with_unit()
    );
    results.push(("retain".to_string(), result));

    // Repopulate with `removed` fresh random entries so subsequent phases operate on a
    // table comparable in size to the pre-retain state.
    {
        let mut txn = connection.write_transaction();
        let mut inserter = txn.get_inserter();
        for _ in 0..removed {
            let (key, value) = random_pair(&mut rng);
            inserter.insert(&key, &value).unwrap();
        }
        drop(inserter);
        txn.commit().unwrap();
    }

    // Extract_if benchmark: walk a range covering ~1/3 of the keyspace and remove every other
    // entry via a predicate. Keys are random uniform over [0, 2^192), so a first-byte range of
    // [0x55, 0xAA) covers ~33% of all keys. After timing, repopulate with the same number of
    // fresh random entries so downstream phases see a table of similar shape.
    let extract_start: [u8; KEY_SIZE] = [0x55; KEY_SIZE];
    let extract_end: [u8; KEY_SIZE] = [0xAA; KEY_SIZE];
    let start = Instant::now();
    let extracted = {
        let mut txn = connection.write_transaction();
        let mut inserter = txn.get_inserter();
        let mut counter: u64 = 0;
        let mut extract_iter = inserter
            .extract_if(
                (
                    Bound::Included(&extract_start[..]),
                    Bound::Excluded(&extract_end[..]),
                ),
                |_, _| {
                    let extract = counter % 2 == 1;
                    counter += 1;
                    extract
                },
            )
            .unwrap();
        let mut extracted = 0u64;
        while let Some((_key, _value)) = extract_iter.next() {
            extracted += 1;
        }
        drop(extract_iter);
        drop(inserter);
        txn.commit().unwrap();
        extracted
    };
    let end = Instant::now();
    let duration = end - start;
    let result = ResultType::keys(extracted as usize, duration);
    println!(
        "{}: extract_if removed {} items in {}ms ({})",
        T::db_type_name(),
        extracted,
        duration.as_millis(),
        result.with_unit()
    );
    results.push(("extract_if".to_string(), result));

    {
        let mut txn = connection.write_transaction();
        let mut inserter = txn.get_inserter();
        for _ in 0..extracted {
            let (key, value) = random_pair(&mut rng);
            inserter.insert(&key, &value).unwrap();
        }
        drop(inserter);
        txn.commit().unwrap();
    }

    let pop_table_len = {
        let txn = connection.read_transaction();
        txn.get_reader().len() as usize
    };
    let (timed_pops, applied_pops, duration) = {
        let mut txn = connection.write_transaction();
        let mut inserter = txn.get_inserter();
        let start = Instant::now();
        let mut duration = None;
        let mut timed_pops = 0u64;
        for i in 0..POP_REMOVALS {
            if i % 2 == 0 {
                inserter.pop_first()
            } else {
                inserter.pop_last()
            }
            .unwrap()
            .unwrap();
            timed_pops += 1;
            // Some backends are very inefficient at a BtreeMap-style pop API. If the first
            // 1% already takes more than a second, estimate the full benchmark from that sample.
            if i + 1 == POP_SAMPLE_REMOVALS {
                let sample_duration = start.elapsed();
                if sample_duration > SLOW_POP_SAMPLE_LIMIT {
                    duration = Some(
                        sample_duration
                            .checked_mul((POP_REMOVALS / POP_SAMPLE_REMOVALS) as u32)
                            .unwrap_or(Duration::MAX),
                    );
                    break;
                }
            }
        }
        let duration = duration.unwrap_or_else(|| start.elapsed());
        if timed_pops != POP_REMOVALS as u64 {
            let timed_pops = timed_pops as usize;
            let remaining_front_pops = POP_REMOVALS.div_ceil(2) - timed_pops.div_ceil(2);
            let remaining_back_pops = POP_REMOVALS / 2 - timed_pops / 2;
            let remaining_len = pop_table_len - timed_pops;
            let mut index = 0usize;
            let removed = inserter
                .retain(|_, _| {
                    // Apply the same edge removals as the remaining alternating pop calls,
                    // without timing a different API as part of the pop benchmark.
                    let keep = index >= remaining_front_pops
                        && index < remaining_len - remaining_back_pops;
                    index += 1;
                    keep
                })
                .unwrap();
            assert_eq!(index, remaining_len);
            assert_eq!(removed as usize, remaining_front_pops + remaining_back_pops);
        }
        drop(inserter);
        txn.commit().unwrap();
        let applied_pops = if timed_pops == POP_REMOVALS as u64 {
            timed_pops
        } else {
            POP_REMOVALS as u64
        };
        (timed_pops, applied_pops, duration)
    };
    // `duration` covers POP_REMOVALS pops in both cases: it is extrapolated from the sample
    // when the benchmark stopped early
    let result = ResultType::keys(POP_REMOVALS, duration);
    if timed_pops == POP_REMOVALS as u64 {
        println!(
            "{}: Popped {} items in {}ms ({})",
            T::db_type_name(),
            timed_pops,
            duration.as_millis(),
            result.with_unit()
        );
    } else {
        println!(
            "{}: Popped {} sampled items, estimated {} items in {}ms ({})",
            T::db_type_name(),
            timed_pops,
            POP_REMOVALS,
            duration.as_millis(),
            result.with_unit()
        );
    }
    results.push(("pop".to_string(), result));

    let mut txn = connection.write_transaction();
    let mut inserter = txn.get_inserter();
    for _ in 0..applied_pops {
        let (key, value) = random_pair(&mut rng);
        inserter.insert(&key, &value).unwrap();
    }
    drop(inserter);
    txn.commit().unwrap();

    let uncompacted_size = database_size(path);
    results.push((
        "uncompacted size".to_string(),
        ResultType::SizeInBytes(uncompacted_size),
    ));
    let start = Instant::now();
    drop(connection);
    if db.compact() {
        let end = Instant::now();
        let duration = end - start;
        println!(
            "{}: Compacted in {}ms",
            T::db_type_name(),
            duration.as_millis()
        );
        let compacted_size = database_size(path);
        results.push((
            "compacted size".to_string(),
            ResultType::SizeInBytes(compacted_size),
        ));
    } else {
        results.push(("compacted size".to_string(), ResultType::NA));
    }

    // Sorted inserts: pairs whose keys ascend, all past every existing key, loaded
    // through the fastest sorted-insert path the database offers. The keys extend the
    // largest possible random key by a suffix so they sort past every existing entry,
    // and the pairs are precomputed so the timing covers only the load. Runs after the
    // size measurements so those stay comparable with historical results, but is reported
    // among the other write phases.
    let sorted_pairs: Vec<([u8; KEY_SIZE + 8], Vec<u8>)> = (0..SORTED_ELEMENTS as u64)
        .map(|i| {
            let mut key = [0xFFu8; KEY_SIZE + 8];
            key[KEY_SIZE..].copy_from_slice(&i.to_be_bytes());
            let mut value = vec![0u8; VALUE_SIZE];
            rng.fill(&mut value);
            (key, value)
        })
        .collect();
    let connection = db.connect();
    let start = Instant::now();
    let mut txn = connection.write_transaction();
    let mut inserter = txn.get_inserter();
    inserter
        .insert_sorted(
            sorted_pairs
                .iter()
                .map(|(key, value)| (key.as_slice(), value.as_slice())),
        )
        .unwrap();
    drop(inserter);
    txn.commit().unwrap();
    let duration = start.elapsed();
    let result = ResultType::keys(SORTED_ELEMENTS, duration);
    println!(
        "{}: Loaded {} sorted items in {}ms ({})",
        T::db_type_name(),
        SORTED_ELEMENTS,
        duration.as_millis(),
        result.with_unit()
    );
    results.insert(sorted_inserts_row, ("sorted inserts".to_string(), result));

    results
}

fn database_size(path: &Path) -> u64 {
    let mut size = 0u64;
    for result in walkdir::WalkDir::new(path) {
        let entry = result.unwrap();
        size += entry.metadata().unwrap().len();
    }
    size
}

#[derive(Copy, Clone)]
pub enum ThroughputUnit {
    /// A single key/value pair inserted, read, or removed
    Key,
    /// A single committed write transaction
    Transaction,
    /// A single range read, which visits several keys
    Scan,
}

impl ThroughputUnit {
    fn abbreviation(self) -> &'static str {
        match self {
            ThroughputUnit::Key => "key/s",
            ThroughputUnit::Transaction => "txn/s",
            ThroughputUnit::Scan => "scan/s",
        }
    }
}

#[derive(Copy, Clone)]
pub enum ResultType {
    /// `count` units of work in `duration`. Reported as a rate, so that phases which do
    /// different amounts of work can still be compared against each other
    Throughput {
        count: u64,
        duration: Duration,
        unit: ThroughputUnit,
    },
    /// Time taken by a phase that is a single call rather than a loop
    Latency(Duration),
    SizeInBytes(u64),
    NA,
}

impl ResultType {
    pub fn keys(count: usize, duration: Duration) -> Self {
        ResultType::Throughput {
            count: count as u64,
            duration,
            unit: ThroughputUnit::Key,
        }
    }

    pub fn txns(count: usize, duration: Duration) -> Self {
        ResultType::Throughput {
            count: count as u64,
            duration,
            unit: ThroughputUnit::Transaction,
        }
    }

    pub fn scans(count: usize, duration: Duration) -> Self {
        ResultType::Throughput {
            count: count as u64,
            duration,
            unit: ThroughputUnit::Scan,
        }
    }

    fn rate(&self) -> f64 {
        match self {
            ResultType::Throughput {
                count, duration, ..
            } => *count as f64 / duration.as_secs_f64(),
            _ => 0.0,
        }
    }

    // Unit to name in the row title, or None when the value already carries its own unit
    fn unit_label(&self) -> Option<&'static str> {
        match self {
            ResultType::Throughput { unit, .. } => Some(unit.abbreviation()),
            ResultType::Latency(_) | ResultType::SizeInBytes(_) | ResultType::NA => None,
        }
    }

    // Higher is better for throughput, lower is better for latency and size. N/A is never
    // better, and a row never mixes result kinds
    fn is_better_than(&self, other: &ResultType) -> bool {
        match (self, other) {
            (ResultType::Throughput { .. }, ResultType::Throughput { .. }) => {
                self.rate() > other.rate()
            }
            (ResultType::Latency(a), ResultType::Latency(b)) => a < b,
            (ResultType::SizeInBytes(a), ResultType::SizeInBytes(b)) => a < b,
            _ => false,
        }
    }

    // Same as `Display`, but names the unit. Used for the per-phase output, which has no
    // row title to carry it
    pub fn with_unit(&self) -> String {
        match self {
            ResultType::Throughput { unit, .. } => format!("{self} {}", unit.abbreviation()),
            _ => self.to_string(),
        }
    }
}

impl std::fmt::Display for ResultType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use byte_unit::{Byte, UnitType};

        match self {
            ResultType::NA => write!(f, "N/A"),
            ResultType::Throughput { .. } => write!(f, "{}", format_rate(self.rate())),
            ResultType::Latency(d) => write!(f, "{}", format_duration(*d)),
            ResultType::SizeInBytes(s) => {
                let b = Byte::from_u64(*s).get_appropriate_unit(UnitType::Binary);
                write!(f, "{b:.2}")
            }
        }
    }
}

// Three significant digits with an SI suffix, e.g. "938", "9.20K", "293K", "1.09M", so that
// the results table stays narrow enough to read
fn format_rate(rate: f64) -> String {
    const SUFFIXES: [&str; 4] = ["", "K", "M", "G"];

    let mut value = rate;
    let mut suffix = 0;
    while value >= 1000.0 && suffix + 1 < SUFFIXES.len() {
        value /= 1000.0;
        suffix += 1;
    }
    let precision = if value < 10.0 {
        2
    } else if value < 100.0 {
        1
    } else {
        0
    };
    let suffix = SUFFIXES[suffix];

    format!("{value:.precision$}{suffix}")
}

// Rounded to the nearest millisecond
fn format_duration(duration: Duration) -> String {
    let millis = (duration.as_nanos() + 500_000) / 1_000_000;
    format!("{millis}ms")
}

// Prints `results` as a markdown table with one column per database, marking the best result
// in each row. Rates are named once in the row title, rather than repeated in every cell
// where they would make the table too wide to read; times and sizes carry their own unit.
pub fn print_results_table(results: &[(&str, Vec<(String, ResultType)>)]) {
    let (_, first) = results.first().expect("no results to print");

    let mut table = comfy_table::Table::new();
    table.load_preset(comfy_table::presets::ASCII_MARKDOWN);
    table.set_width(100);
    let mut header = vec![""];
    header.extend(results.iter().map(|(name, _)| *name));
    table.set_header(header);

    for (i, (name, _)) in first.iter().enumerate() {
        let row: Vec<&ResultType> = results.iter().map(|(_, r)| &r[i].1).collect();

        // A database that doesn't support the phase reports N/A, so take the unit from the
        // first one that ran it
        let label = match row.iter().find_map(|result| result.unit_label()) {
            Some(unit) => format!("{name} ({unit})"),
            None => name.clone(),
        };

        // Nothing to compare against when only one database was benchmarked
        let mut best: Option<usize> = None;
        if results.len() > 1 {
            for (j, result) in row.iter().enumerate() {
                if matches!(result, ResultType::NA) {
                    continue;
                }
                if best.is_none_or(|previous| result.is_better_than(row[previous])) {
                    best = Some(j);
                }
            }
        }

        // Anything that renders the same as the best result is a tie at the precision shown,
        // so highlight all of them rather than picking one arbitrarily
        let best = best.map(|j| row[j].to_string());
        let mut cells = vec![label];
        cells.extend(row.iter().map(|result| {
            let value = result.to_string();
            if best.as_deref() == Some(value.as_str()) {
                format!("**{value}**")
            } else {
                value
            }
        }));
        table.add_row(cells);
    }

    println!();
    println!("{table}");
}

pub trait BenchDatabase {
    type C<'db>: BenchDatabaseConnection
    where
        Self: 'db;

    fn db_type_name() -> &'static str;

    fn connect(&self) -> Self::C<'_>;

    // Returns a boolean indicating whether compaction is supported
    fn compact(&mut self) -> bool {
        false
    }
}

pub trait BenchDatabaseConnection: Send {
    type W<'db>: BenchWriteTransaction
    where
        Self: 'db;
    type R<'db>: BenchReadTransaction
    where
        Self: 'db;

    // Returns a boolean indicating whether the database supports changing the synchronization mode
    fn set_sync(&mut self, _sync: bool) -> bool {
        false
    }

    fn write_transaction(&self) -> Self::W<'_>;

    fn read_transaction(&self) -> Self::R<'_>;
}

pub trait BenchWriteTransaction {
    type W<'txn>: BenchInserter
    where
        Self: 'txn;

    fn get_inserter(&mut self) -> Self::W<'_>;

    #[allow(clippy::result_unit_err)]
    fn commit(self) -> Result<(), ()>;
}

pub type BenchPopEntry<'out, T> = (
    <T as BenchInserter>::Output<'out>,
    <T as BenchInserter>::Output<'out>,
);

pub type BenchPopResult<'out, T> = Result<Option<BenchPopEntry<'out, T>>, ()>;

pub type BenchExtractIfResult<'out, T, F> =
    Result<<T as BenchInserter>::ExtractIfIterator<'out, F>, ()>;

pub trait BenchInserter {
    type Output<'out>: AsRef<[u8]> + 'out
    where
        Self: 'out;
    type ExtractIfIterator<'out, F>: BenchIterator
    where
        Self: 'out,
        F: for<'f> FnMut(&'f [u8], &'f [u8]) -> bool + 'out;

    #[allow(clippy::result_unit_err)]
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()>;

    // Inserts pairs whose keys are strictly ascending and greater than every key
    // already in the table, through the fastest sorted-insert path the database
    // offers. Databases without one fall back to ordinary inserts.
    #[allow(clippy::result_unit_err)]
    fn insert_sorted<'i>(
        &mut self,
        pairs: impl Iterator<Item = (&'i [u8], &'i [u8])>,
    ) -> Result<(), ()> {
        for (key, value) in pairs {
            self.insert(key, value)?;
        }
        Ok(())
    }

    #[allow(clippy::result_unit_err)]
    fn remove(&mut self, key: &[u8]) -> Result<(), ()>;

    #[allow(clippy::result_unit_err)]
    fn pop_first(&mut self) -> BenchPopResult<'_, Self>;

    #[allow(clippy::result_unit_err)]
    fn pop_last(&mut self) -> BenchPopResult<'_, Self>;

    // Applies `predicate` to every entry in sorted key order; entries for which it returns
    // `false` are removed.
    // Returns the number of entries removed.
    #[allow(clippy::result_unit_err)]
    fn retain<F: FnMut(&[u8], &[u8]) -> bool>(&mut self, predicate: F) -> Result<u64, ()>;

    // Applies `predicate` to every entry within `range`; entries for which it returns `true`
    // are removed (extracted). Modeled after `std::collections::BTreeMap::extract_if`.
    #[allow(clippy::result_unit_err)]
    fn extract_if<'a, F>(
        &'a mut self,
        range: (Bound<&[u8]>, Bound<&[u8]>),
        predicate: F,
    ) -> BenchExtractIfResult<'a, Self, F>
    where
        F: for<'f> FnMut(&'f [u8], &'f [u8]) -> bool + 'a;
}

pub trait BenchReadTransaction {
    type T<'txn>: BenchReader
    where
        Self: 'txn;

    fn get_reader(&self) -> Self::T<'_>;
}

#[allow(clippy::len_without_is_empty)]
pub trait BenchReader {
    type Output<'out>: AsRef<[u8]> + 'out
    where
        Self: 'out;
    type Iterator<'out>: BenchIterator
    where
        Self: 'out;

    fn get<'a>(&'a mut self, key: &[u8]) -> Option<Self::Output<'a>>;

    fn range_from<'a>(&'a mut self, start: &'a [u8]) -> Self::Iterator<'a>;

    fn len(&mut self) -> u64;
}

pub trait BenchIterator {
    type Output<'out>: AsRef<[u8]> + 'out
    where
        Self: 'out;

    fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)>;
}

pub struct VecBenchIterator<T> {
    inner: std::vec::IntoIter<(T, T)>,
}

impl<T> VecBenchIterator<T> {
    pub fn new(entries: Vec<(T, T)>) -> Self {
        Self {
            inner: entries.into_iter(),
        }
    }
}

impl<T: AsRef<[u8]>> BenchIterator for VecBenchIterator<T> {
    type Output<'out>
        = T
    where
        Self: 'out;

    fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)> {
        self.inner.next()
    }
}

pub struct RedbBenchDatabase<'a> {
    db: &'a mut redb::Database,
}

impl<'a> RedbBenchDatabase<'a> {
    #[allow(dead_code)]
    pub fn new(db: &'a mut redb::Database) -> Self {
        RedbBenchDatabase { db }
    }
}

impl BenchDatabase for RedbBenchDatabase<'_> {
    type C<'db>
        = RedbBenchDatabaseConnection<'db>
    where
        Self: 'db;

    fn db_type_name() -> &'static str {
        "redb"
    }

    fn connect(&self) -> Self::C<'_> {
        RedbBenchDatabaseConnection {
            db: self.db,
            sync: true,
        }
    }

    fn compact(&mut self) -> bool {
        self.db.compact().unwrap();
        true
    }
}

pub struct RedbBenchDatabaseConnection<'a> {
    db: &'a redb::Database,
    sync: bool,
}

impl BenchDatabaseConnection for RedbBenchDatabaseConnection<'_> {
    type W<'db>
        = RedbBenchWriteTransaction
    where
        Self: 'db;
    type R<'db>
        = RedbBenchReadTransaction
    where
        Self: 'db;

    fn set_sync(&mut self, sync: bool) -> bool {
        self.sync = sync;
        true
    }

    fn write_transaction(&self) -> Self::W<'_> {
        let mut txn = self.db.begin_write().unwrap();
        if !self.sync {
            txn.set_durability(Durability::None).unwrap();
        }
        RedbBenchWriteTransaction { txn }
    }

    fn read_transaction(&self) -> Self::R<'_> {
        let txn = self.db.begin_read().unwrap();
        RedbBenchReadTransaction { txn }
    }
}

pub struct RedbBenchReadTransaction {
    txn: redb::ReadTransaction,
}

impl BenchReadTransaction for RedbBenchReadTransaction {
    type T<'txn>
        = RedbBenchReader
    where
        Self: 'txn;

    fn get_reader(&self) -> Self::T<'_> {
        let table = self.txn.open_table(X).unwrap();
        RedbBenchReader { table }
    }
}

pub struct RedbBenchReader {
    table: redb::ReadOnlyTable<&'static [u8], &'static [u8]>,
}

impl BenchReader for RedbBenchReader {
    type Output<'out>
        = RedbAccessGuard<'out>
    where
        Self: 'out;
    type Iterator<'out>
        = RedbBenchIterator<'out>
    where
        Self: 'out;

    fn get<'a>(&'a mut self, key: &[u8]) -> Option<Self::Output<'a>> {
        // Explicit trait call, so that the benchmark measures the lifetime-bound accessor
        // rather than the inherent reference-counted one that shadows it
        ReadableTable::get(&self.table, key)
            .unwrap()
            .map(RedbAccessGuard::new)
    }

    fn range_from<'a>(&'a mut self, key: &'a [u8]) -> Self::Iterator<'a> {
        // Explicit trait call, so that the benchmark measures the lifetime-bound accessor
        // rather than the inherent reference-counted one that shadows it
        let iter = ReadableTable::range(&self.table, key..).unwrap();
        RedbBenchIterator { iter }
    }

    fn len(&mut self) -> u64 {
        self.table.len().unwrap()
    }
}

pub struct RedbBenchIterator<'a> {
    iter: redb::Range<'a, &'static [u8], &'static [u8]>,
}

impl BenchIterator for RedbBenchIterator<'_> {
    type Output<'a>
        = RedbAccessGuard<'a>
    where
        Self: 'a;

    fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)> {
        self.iter.next().map(|item| {
            let (k, v) = item.unwrap();
            (RedbAccessGuard::new(k), RedbAccessGuard::new(v))
        })
    }
}

pub struct RedbExtractIfIterator<'a, F: for<'f> FnMut(&'f [u8], &'f [u8]) -> bool> {
    iter: redb::ExtractIf<'a, &'static [u8], &'static [u8], F>,
}

impl<F: for<'f> FnMut(&'f [u8], &'f [u8]) -> bool> BenchIterator for RedbExtractIfIterator<'_, F> {
    type Output<'a>
        = RedbAccessGuard<'a>
    where
        Self: 'a;

    fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)> {
        self.iter.next().map(|item| {
            let (k, v) = item.unwrap();
            (RedbAccessGuard::new(k), RedbAccessGuard::new(v))
        })
    }
}

pub struct RedbAccessGuard<'a> {
    inner: AccessGuard<'a, &'static [u8]>,
}

impl<'a> RedbAccessGuard<'a> {
    fn new(inner: AccessGuard<'a, &'static [u8]>) -> Self {
        Self { inner }
    }
}

impl AsRef<[u8]> for RedbAccessGuard<'_> {
    fn as_ref(&self) -> &[u8] {
        self.inner.value()
    }
}

pub struct RedbBenchWriteTransaction {
    txn: redb::WriteTransaction,
}

impl BenchWriteTransaction for RedbBenchWriteTransaction {
    type W<'txn>
        = RedbBenchInserter<'txn>
    where
        Self: 'txn;

    fn get_inserter(&mut self) -> Self::W<'_> {
        let table = self.txn.open_table(X).unwrap();
        RedbBenchInserter { table }
    }

    fn commit(self) -> Result<(), ()> {
        self.txn.commit().map_err(|_| ())
    }
}

pub struct RedbBenchInserter<'txn> {
    table: redb::Table<'txn, &'static [u8], &'static [u8]>,
}

impl BenchInserter for RedbBenchInserter<'_> {
    type Output<'out>
        = RedbAccessGuard<'out>
    where
        Self: 'out;
    type ExtractIfIterator<'out, F>
        = RedbExtractIfIterator<'out, F>
    where
        Self: 'out,
        F: for<'f> FnMut(&'f [u8], &'f [u8]) -> bool + 'out;

    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        self.table.insert(key, value).map(|_| ()).map_err(|_| ())
    }

    fn insert_sorted<'i>(
        &mut self,
        pairs: impl Iterator<Item = (&'i [u8], &'i [u8])>,
    ) -> Result<(), ()> {
        // redb's appending cursor: buffers the inserts and splices packed leaves into
        // the tree, instead of descending it for each pair
        let mut cursor = self
            .table
            .upper_bound_mut(Bound::<&[u8]>::Unbounded)
            .map_err(|_| ())?;
        for (key, value) in pairs {
            cursor.insert_before(key, value).map_err(|_| ())?;
        }
        cursor.close().map_err(|_| ())
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), ()> {
        self.table.remove(key).map(|_| ()).map_err(|_| ())
    }

    fn pop_first(&mut self) -> BenchPopResult<'_, Self> {
        self.table
            .pop_first()
            .map(|entry| {
                entry.map(|(key, value)| (RedbAccessGuard::new(key), RedbAccessGuard::new(value)))
            })
            .map_err(|_| ())
    }

    fn pop_last(&mut self) -> BenchPopResult<'_, Self> {
        self.table
            .pop_last()
            .map(|entry| {
                entry.map(|(key, value)| (RedbAccessGuard::new(key), RedbAccessGuard::new(value)))
            })
            .map_err(|_| ())
    }

    fn retain<F: FnMut(&[u8], &[u8]) -> bool>(&mut self, mut predicate: F) -> Result<u64, ()> {
        let mut removed = 0u64;
        self.table
            .retain(|k, v| {
                let keep = predicate(k, v);
                if !keep {
                    removed += 1;
                }
                keep
            })
            .map_err(|_| ())?;
        Ok(removed)
    }

    fn extract_if<'a, F>(
        &'a mut self,
        range: (Bound<&[u8]>, Bound<&[u8]>),
        predicate: F,
    ) -> BenchExtractIfResult<'a, Self, F>
    where
        F: for<'f> FnMut(&'f [u8], &'f [u8]) -> bool + 'a,
    {
        // redb has a native cursor-based extract_from_if that removes entries lazily as the
        // returned iterator is consumed.
        let iter = self
            .table
            .extract_from_if(range, predicate)
            .map_err(|_| ())?;
        Ok(RedbExtractIfIterator { iter })
    }
}
