//! Common utilities for redbx benchmarks

use std::time::Duration;

pub const TABLE_NAME: &str = "benchmark_data";
pub const BULK_ELEMENTS: usize = 5_000_000;
pub const INDIVIDUAL_WRITES: usize = 1_000;
pub const BATCH_WRITES: usize = 100;
pub const BATCH_SIZE: usize = 1000;
pub const NUM_READS: usize = 1_000_000;
pub const NUM_SCANS: usize = 500_000;
pub const SCAN_LEN: usize = 10;
pub const KEY_SIZE: usize = 24;
pub const VALUE_SIZE: usize = 150;
pub const RNG_SEED: u64 = 3;

#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    pub name: String,
    pub duration: Duration,
}

/// Generate random key-value pair for benchmarking
pub fn random_pair(rng: &mut fastrand::Rng) -> ([u8; KEY_SIZE], Vec<u8>) {
    let mut key = [0u8; KEY_SIZE];
    rng.fill(&mut key);
    let mut value = vec![0u8; VALUE_SIZE];
    rng.fill(&mut value);
    (key, value)
}

/// Create seeded RNG for reproducible benchmarks
pub fn make_rng() -> fastrand::Rng {
    fastrand::Rng::with_seed(RNG_SEED)
}

/// Calculate overhead percentage
pub fn calculate_overhead(baseline: Duration, encrypted: Duration) -> f64 {
    let baseline_nanos = baseline.as_nanos() as f64;
    let encrypted_nanos = encrypted.as_nanos() as f64;
    
    if baseline_nanos == 0.0 {
        0.0
    } else {
        ((encrypted_nanos - baseline_nanos) / baseline_nanos) * 100.0
    }
}

/// Format duration in a human-readable way
pub fn format_duration(duration: Duration) -> String {
    let millis = duration.as_millis();
    if millis >= 1000 {
        format!("{:.2}s", duration.as_secs_f64())
    } else {
        format!("{}ms", millis)
    }
}