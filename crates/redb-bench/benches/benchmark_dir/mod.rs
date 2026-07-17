use std::path::PathBuf;

pub fn benchmark_dir() -> PathBuf {
    std::env::var_os("REDB_BENCHMARK_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::current_dir().unwrap())
}
