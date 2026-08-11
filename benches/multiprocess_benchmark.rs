use redb::{Database, ReadableDatabase};
use std::hint::black_box;
use std::time::{Duration, Instant};

const READ_ITERATIONS: u32 = 20_000;
const ABORTED_WRITE_ITERATIONS: u32 = 2_000;
const COMMITTED_WRITE_ITERATIONS: u32 = 100;

fn time(iterations: u32, mut operation: impl FnMut()) -> Duration {
    let start = Instant::now();
    for _ in 0..iterations {
        operation();
    }
    start.elapsed()
}

fn read_transactions(db: &Database) -> Duration {
    time(READ_ITERATIONS, || {
        black_box(db.begin_read().unwrap());
    })
}

fn aborted_write_transactions(db: &Database) -> Duration {
    time(ABORTED_WRITE_ITERATIONS, || {
        db.begin_write().unwrap().abort().unwrap();
    })
}

fn committed_write_transactions(db: &Database) -> Duration {
    time(COMMITTED_WRITE_ITERATIONS, || {
        db.begin_write().unwrap().commit().unwrap();
    })
}

fn nanos_per_iteration(duration: Duration, iterations: u32) -> u128 {
    duration.as_nanos() / u128::from(iterations)
}

fn print_result(
    name: &str,
    single: Duration,
    single_writer: Duration,
    multiple_writers: Duration,
    iterations: u32,
) {
    let single = nanos_per_iteration(single, iterations);
    let single_writer = nanos_per_iteration(single_writer, iterations);
    let multiple_writers = nanos_per_iteration(multiple_writers, iterations);
    println!(
        "{name:28} file={single:>10} ns/op  single-writer={single_writer:>10} ns/op ({:.2}x)  multiple-writers={multiple_writers:>10} ns/op ({:.2}x)",
        single_writer as f64 / single as f64,
        multiple_writers as f64 / single as f64,
    );
}

fn main() {
    let single_file = tempfile::NamedTempFile::new().unwrap();
    let single = Database::create(single_file.path()).unwrap();

    let multiprocess_root = tempfile::tempdir().unwrap();
    let single_writer =
        Database::create_multiprocess(multiprocess_root.path().join("database")).unwrap();
    single_writer.begin_write().unwrap().abort().unwrap();

    let multiple_writers_root = tempfile::tempdir().unwrap();
    let mut builder = Database::builder();
    builder.set_multiprocess_multiple_writers(true);
    let multiple_writers = builder
        .create_multiprocess(multiple_writers_root.path().join("database"))
        .unwrap();
    multiple_writers.begin_write().unwrap().abort().unwrap();

    print_result(
        "begin/drop read",
        read_transactions(&single),
        read_transactions(&single_writer),
        read_transactions(&multiple_writers),
        READ_ITERATIONS,
    );
    print_result(
        "begin/abort write",
        aborted_write_transactions(&single),
        aborted_write_transactions(&single_writer),
        aborted_write_transactions(&multiple_writers),
        ABORTED_WRITE_ITERATIONS,
    );
    print_result(
        "empty committed write",
        committed_write_transactions(&single),
        committed_write_transactions(&single_writer),
        committed_write_transactions(&multiple_writers),
        COMMITTED_WRITE_ITERATIONS,
    );
}
