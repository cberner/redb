//! Encryption Overhead Benchmark
//! 
//! Compares redbx (encrypted) vs redb (unencrypted) performance
//! to measure the overhead of encryption.

use std::time::Instant;
use tempfile::NamedTempFile;
use comfy_table::{Table, Cell, presets::UTF8_FULL};
use redbx_bench::*;
use redb::ReadableDatabase as RedbReadableDatabase;
use redbx::ReadableDatabase;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = env_logger::try_init();
    
    println!("🔐 Encryption Overhead Benchmark");
    println!("Comparing redbx (encrypted) vs redb (unencrypted)");
    println!("================================================\n");

    // Benchmark redb (unencrypted)
    println!("📊 Benchmarking redb (unencrypted)...");
    let redb_results = benchmark_redb()?;

    // Benchmark redbx (encrypted)  
    println!("\n📊 Benchmarking redbx (encrypted)...");
    let redbx_results = benchmark_redbx()?;

    // Compare results
    compare_results(redb_results, redbx_results);

    Ok(())
}

fn benchmark_redb() -> Result<Vec<BenchmarkResult>, Box<dyn std::error::Error>> {
    let mut results = Vec::new();
    let mut rng = make_rng();
    
    let tmpfile = NamedTempFile::new()?;
    let db = redb::Database::create(tmpfile.path())?;
    let table_def: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new(TABLE_NAME);

    // Bulk load test
    let start = Instant::now();
    {
        let write_txn = db.begin_write()?;
        {
            let mut table = write_txn.open_table(table_def)?;
            for _ in 0..BULK_ELEMENTS {
                let (key, value) = random_pair(&mut rng);
                table.insert(&key[..], &value[..])?;
            }
        }
        write_txn.commit()?;
    }
    let duration = start.elapsed();
    println!("  Bulk load: {} items in {}", BULK_ELEMENTS, format_duration(duration));
    results.push(BenchmarkResult {
        name: "Bulk Load".to_string(),
        duration,
    });

    // Individual writes test
    let start = Instant::now();
    for _ in 0..INDIVIDUAL_WRITES {
        let write_txn = db.begin_write()?;
        {
            let mut table = write_txn.open_table(table_def)?;
            let (key, value) = random_pair(&mut rng);
            table.insert(&key[..], &value[..])?;
        }
        write_txn.commit()?;
    }
    let duration = start.elapsed();
    println!("  Individual writes: {} items in {}", INDIVIDUAL_WRITES, format_duration(duration));
    results.push(BenchmarkResult {
        name: "Individual Writes".to_string(),
        duration,
    });

    // Batch writes test
    let start = Instant::now();
    for _ in 0..BATCH_WRITES {
        let write_txn = db.begin_write()?;
        {
            let mut table = write_txn.open_table(table_def)?;
            for _ in 0..BATCH_SIZE {
                let (key, value) = random_pair(&mut rng);
                table.insert(&key[..], &value[..])?;
            }
        }
        write_txn.commit()?;
    }
    let duration = start.elapsed();
    println!("  Batch writes: {} batches of {} items in {}", BATCH_WRITES, BATCH_SIZE, format_duration(duration));
    results.push(BenchmarkResult {
        name: "Batch Writes".to_string(),
        duration,
    });

    // Random reads test
    rng = make_rng(); // Reset for consistent reads
    let start = Instant::now();
    let mut found = 0;
    {
        let read_txn = db.begin_read()?;
        let table = read_txn.open_table(table_def)?;
        for _ in 0..NUM_READS {
            let (key, _) = random_pair(&mut rng);
            if table.get(&key[..])?.is_some() {
                found += 1;
            }
        }
    }
    let duration = start.elapsed();
    println!("  Random reads: {}/{} found in {}", found, NUM_READS, format_duration(duration));
    results.push(BenchmarkResult {
        name: "Random Reads".to_string(),
        duration,
    });

    // Range scans test
    rng = make_rng(); // Reset for consistent scans
    let start = Instant::now();
    let mut total_scanned = 0;
    {
        let read_txn = db.begin_read()?;
        let table = read_txn.open_table(table_def)?;
        for _ in 0..NUM_SCANS {
            let (key, _) = random_pair(&mut rng);
            let mut iter = table.range(&key[..]..).unwrap();
            for _ in 0..SCAN_LEN {
                if iter.next().is_some() {
                    total_scanned += 1;
                } else {
                    break;
                }
            }
        }
    }
    let duration = start.elapsed();
    println!("  Range scans: {} total items scanned in {}", total_scanned, format_duration(duration));
    results.push(BenchmarkResult {
        name: "Range Scans".to_string(),
        duration,
    });

    Ok(results)
}

fn benchmark_redbx() -> Result<Vec<BenchmarkResult>, Box<dyn std::error::Error>> {
    let mut results = Vec::new();
    let mut rng = make_rng();
    
    let tmpfile = NamedTempFile::new()?;
    let db = redbx::Database::create(tmpfile.path(), "benchmark_password")?;
    let table_def: redbx::TableDefinition<&[u8], &[u8]> = redbx::TableDefinition::new(TABLE_NAME);

    // Bulk load test
    let start = Instant::now();
    {
        let write_txn = db.begin_write()?;
        {
            let mut table = write_txn.open_table(table_def)?;
            for _ in 0..BULK_ELEMENTS {
                let (key, value) = random_pair(&mut rng);
                table.insert(&key[..], &value[..])?;
            }
        }
        write_txn.commit()?;
    }
    let duration = start.elapsed();
    println!("  Bulk load: {} items in {}", BULK_ELEMENTS, format_duration(duration));
    results.push(BenchmarkResult {
        name: "Bulk Load".to_string(),
        duration,
    });

    // Individual writes test
    let start = Instant::now();
    for _ in 0..INDIVIDUAL_WRITES {
        let write_txn = db.begin_write()?;
        {
            let mut table = write_txn.open_table(table_def)?;
            let (key, value) = random_pair(&mut rng);
            table.insert(&key[..], &value[..])?;
        }
        write_txn.commit()?;
    }
    let duration = start.elapsed();
    println!("  Individual writes: {} items in {}", INDIVIDUAL_WRITES, format_duration(duration));
    results.push(BenchmarkResult {
        name: "Individual Writes".to_string(),
        duration,
    });

    // Batch writes test
    let start = Instant::now();
    for _ in 0..BATCH_WRITES {
        let write_txn = db.begin_write()?;
        {
            let mut table = write_txn.open_table(table_def)?;
            for _ in 0..BATCH_SIZE {
                let (key, value) = random_pair(&mut rng);
                table.insert(&key[..], &value[..])?;
            }
        }
        write_txn.commit()?;
    }
    let duration = start.elapsed();
    println!("  Batch writes: {} batches of {} items in {}", BATCH_WRITES, BATCH_SIZE, format_duration(duration));
    results.push(BenchmarkResult {
        name: "Batch Writes".to_string(),
        duration,
    });

    // Random reads test
    rng = make_rng(); // Reset for consistent reads
    let start = Instant::now();
    let mut found = 0;
    {
        let read_txn = db.begin_read()?;
        let table = read_txn.open_table(table_def)?;
        for _ in 0..NUM_READS {
            let (key, _) = random_pair(&mut rng);
            if table.get(&key[..])?.is_some() {
                found += 1;
            }
        }
    }
    let duration = start.elapsed();
    println!("  Random reads: {}/{} found in {}", found, NUM_READS, format_duration(duration));
    results.push(BenchmarkResult {
        name: "Random Reads".to_string(),
        duration,
    });

    // Range scans test
    rng = make_rng(); // Reset for consistent scans
    let start = Instant::now();
    let mut total_scanned = 0;
    {
        let read_txn = db.begin_read()?;
        let table = read_txn.open_table(table_def)?;
        for _ in 0..NUM_SCANS {
            let (key, _) = random_pair(&mut rng);
            let mut iter = table.range(&key[..]..).unwrap();
            for _ in 0..SCAN_LEN {
                if iter.next().is_some() {
                    total_scanned += 1;
                } else {
                    break;
                }
            }
        }
    }
    let duration = start.elapsed();
    println!("  Range scans: {} total items scanned in {}", total_scanned, format_duration(duration));
    results.push(BenchmarkResult {
        name: "Range Scans".to_string(),
        duration,
    });

    Ok(results)
}

fn compare_results(redb_results: Vec<BenchmarkResult>, redbx_results: Vec<BenchmarkResult>) {
    let mut table = Table::new();
    table.load_preset(UTF8_FULL);
    
    table.set_header(vec![
        "Test", 
        "redb (unencrypted)", 
        "redbx (encrypted)", 
        "Overhead", 
        "Status"
    ]);
    
    let mut total_overhead = 0.0;
    let mut count = 0;

    for (redb_result, redbx_result) in redb_results.iter().zip(redbx_results.iter()) {
        let overhead = calculate_overhead(redb_result.duration, redbx_result.duration);
        
        let status = if overhead <= 20.0 {
            "✅ GOOD"
        } else if overhead <= 40.0 {
            "⚠️  WARNING"
        } else {
            "❌ CRITICAL"
        };

        table.add_row(vec![
            Cell::new(&redb_result.name),
            Cell::new(&format_duration(redb_result.duration)),
            Cell::new(&format_duration(redbx_result.duration)),
            Cell::new(&format!("{:.1}%", overhead)),
            Cell::new(status),
        ]);

        total_overhead += overhead;
        count += 1;
    }

    println!("\n📈 Encryption Overhead Results:");
    println!("{}", table);

    // Summary
    if count > 0 {
        let avg_overhead = total_overhead / count as f64;
        println!("\n📊 Summary:");
        println!("Average encryption overhead: {:.1}%", avg_overhead);
        
        if avg_overhead <= 20.0 {
            println!("✅ Performance is within acceptable limits (≤20% overhead)");
        } else if avg_overhead <= 40.0 {
            println!("⚠️  Performance overhead is high but manageable (≤40% overhead)");
        } else {
            println!("❌ Performance overhead is too high (>40% overhead)");
        }
    }
}