use std::env::current_dir;
use std::{fs, process};
use tempfile::NamedTempFile;

#[expect(dead_code)]
mod common;
use common::*;

fn main() {
    let _ = env_logger::try_init();
    let tmpdir = current_dir().unwrap().join(".benchmark");
    fs::create_dir(&tmpdir).unwrap();

    let tmpdir2 = tmpdir.clone();
    ctrlc::set_handler(move || {
        fs::remove_dir_all(&tmpdir2).unwrap();
        process::exit(1);
    })
    .unwrap();

    let tmpfile: NamedTempFile = NamedTempFile::new_in(&tmpdir).unwrap();
    let mut db = redb::Database::builder()
        .set_cache_size(CACHE_SIZE)
        .create(tmpfile.path())
        .unwrap();
    let table = RedbBenchDatabase::new(&mut db);
    benchmark(table, tmpfile.path());

    fs::remove_dir_all(&tmpdir).unwrap();
}
