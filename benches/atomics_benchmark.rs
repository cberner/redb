use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};
use std::thread;
use std::time::SystemTime;

const ITERATIONS: usize = 1000 * 1000;

fn baseline(num_threads: usize) {
    let start = SystemTime::now();
    for _ in 0..num_threads {
        thread::scope(|s| {
            s.spawn(|| {
                let mut value = 0u64;
                for _ in 0..ITERATIONS {
                    let value = black_box(&mut value);
                    *value += 1;
                }
                for _ in 0..ITERATIONS {
                    let value = black_box(&mut value);
                    *value -= 1;
                }
            });
        });
    }

    let end = SystemTime::now();
    let duration = end.duration_since(start).unwrap();
    println!(
        "baseline (NOT atomic) ({} threads): {} ops in {}ms",
        num_threads,
        2 * ITERATIONS,
        duration.as_millis(),
    );
}

fn atomics(num_threads: usize) {
    let start = SystemTime::now();
    let value = AtomicU64::new(0);
    for _ in 0..num_threads {
        thread::scope(|s| {
            s.spawn(|| {
                for _ in 0..ITERATIONS {
                    let value = black_box(&value);
                    value.fetch_add(1, Ordering::Release);
                }
                for _ in 0..ITERATIONS {
                    let value = black_box(&value);
                    value.fetch_sub(1, Ordering::Release);
                }
            });
        });
    }
    assert_eq!(0, value.load(Ordering::Acquire));

    let end = SystemTime::now();
    let duration = end.duration_since(start).unwrap();
    println!(
        "atomics ({} threads): {} ops in {}ms",
        num_threads,
        2 * ITERATIONS,
        duration.as_millis(),
    );
}

fn mutex(num_threads: usize) {
    let start = SystemTime::now();
    let value = Mutex::new(0u64);
    for _ in 0..num_threads {
        thread::scope(|s| {
            s.spawn(|| {
                for _ in 0..ITERATIONS {
                    let value = black_box(&value);
                    *value.lock().unwrap() += 1;
                }
                for _ in 0..ITERATIONS {
                    let value = black_box(&value);
                    *value.lock().unwrap() -= 1;
                }
            });
        });
    }
    assert_eq!(0u64, *value.lock().unwrap());

    let end = SystemTime::now();
    let duration = end.duration_since(start).unwrap();
    println!(
        "mutex ({} threads): {} ops in {}ms",
        num_threads,
        2 * ITERATIONS,
        duration.as_millis(),
    );
}

fn rw_lock(num_threads: usize) {
    let start = SystemTime::now();
    let value = RwLock::new(0u64);
    for _ in 0..num_threads {
        thread::scope(|s| {
            s.spawn(|| {
                for _ in 0..ITERATIONS {
                    let value = black_box(&value);
                    *value.write().unwrap() += 1;
                }
                for _ in 0..ITERATIONS {
                    let value = black_box(&value);
                    *value.write().unwrap() -= 1;
                }
            });
        });
    }
    assert_eq!(0u64, *value.read().unwrap());

    let end = SystemTime::now();
    let duration = end.duration_since(start).unwrap();
    println!(
        "rwlock ({} threads): {} ops in {}ms",
        num_threads,
        2 * ITERATIONS,
        duration.as_millis(),
    );
}

fn main() {
    for threads in [1, 2, 4, 8, 16, 32, 64] {
        baseline(threads);
        atomics(threads);
        mutex(threads);
        rw_lock(threads);
        println!();
    }
}
