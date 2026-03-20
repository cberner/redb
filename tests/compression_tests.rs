#![cfg(feature = "compression")]

use shodh_redb::{
    Builder, CompressionConfig, Database, ReadableDatabase, ReadableTableMetadata, TableDefinition,
};

const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("data");
const STR_TABLE: TableDefinition<&str, &str> = TableDefinition::new("strings");

fn create_tempfile() -> tempfile::NamedTempFile {
    if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/tmp").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    }
}

fn compressible_value(size: usize) -> Vec<u8> {
    let mut v = vec![0u8; size];
    for (i, b) in v.iter_mut().enumerate() {
        *b = (i % 64) as u8;
    }
    v
}

#[cfg(feature = "compression_lz4")]
mod lz4 {
    use super::*;

    #[test]
    fn insert_read_verify() {
        let tmpfile = create_tempfile();
        let db = Builder::new()
            .set_compression(CompressionConfig::Lz4)
            .create(tmpfile.path())
            .unwrap();

        let value = compressible_value(4096);

        {
            let wtx = db.begin_write().unwrap();
            {
                let mut table = wtx.open_table(TABLE).unwrap();
                for i in 0..100 {
                    table.insert(i, value.as_slice()).unwrap();
                }
            }
            wtx.commit().unwrap();
        }

        // Read back and verify
        let rtx = db.begin_read().unwrap();
        let table = rtx.open_table(TABLE).unwrap();
        for i in 0..100 {
            let got = table.get(i).unwrap().unwrap();
            assert_eq!(got.value(), value.as_slice(), "mismatch at key {i}");
        }
        assert_eq!(table.len().unwrap(), 100);
    }

    #[test]
    fn reopen_compressed_db() {
        let tmpfile = create_tempfile();
        let value = compressible_value(2048);

        // Create and populate
        {
            let db = Builder::new()
                .set_compression(CompressionConfig::Lz4)
                .create(tmpfile.path())
                .unwrap();
            let wtx = db.begin_write().unwrap();
            {
                let mut table = wtx.open_table(TABLE).unwrap();
                for i in 0..50 {
                    table.insert(i, value.as_slice()).unwrap();
                }
            }
            wtx.commit().unwrap();
        }

        // Reopen with compression and verify
        {
            let db = Builder::new()
                .set_compression(CompressionConfig::Lz4)
                .open(tmpfile.path())
                .unwrap();
            let rtx = db.begin_read().unwrap();
            let table = rtx.open_table(TABLE).unwrap();
            for i in 0..50 {
                let got = table.get(i).unwrap().unwrap();
                assert_eq!(got.value(), value.as_slice(), "mismatch at key {i}");
            }
        }
    }

    #[test]
    fn compact_with_compression() {
        let tmpfile = create_tempfile();
        let big_value = compressible_value(100 * 1024);
        let definition: TableDefinition<u32, &[u8]> = TableDefinition::new("compact_test");

        let db = Builder::new()
            .set_compression(CompressionConfig::Lz4)
            .create(tmpfile.path())
            .unwrap();

        // Insert many values
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(definition).unwrap();
            for i in 0..50 {
                table.insert(i, big_value.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();

        // Delete most
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(definition).unwrap();
            for i in 0..45 {
                table.remove(i).unwrap();
            }
        }
        txn.commit().unwrap();

        // Trigger compaction commit
        let txn = db.begin_write().unwrap();
        txn.commit().unwrap();

        drop(db);
        let file_size_before = tmpfile.as_file().metadata().unwrap().len();

        let mut db = Builder::new()
            .set_compression(CompressionConfig::Lz4)
            .open(tmpfile.path())
            .unwrap();
        db.compact().unwrap();
        drop(db);

        let file_size_after = tmpfile.as_file().metadata().unwrap().len();
        assert!(
            file_size_after < file_size_before,
            "compaction should reduce file size: before={file_size_before}, after={file_size_after}"
        );

        // Verify remaining data survives compaction
        let db = Builder::new()
            .set_compression(CompressionConfig::Lz4)
            .open(tmpfile.path())
            .unwrap();
        let rtx = db.begin_read().unwrap();
        let table = rtx.open_table(definition).unwrap();
        assert_eq!(table.len().unwrap(), 5);
        for i in 45..50 {
            let got = table.get(i).unwrap().unwrap();
            assert_eq!(got.value(), big_value.as_slice());
        }
    }

    #[test]
    fn string_table_with_compression() {
        let tmpfile = create_tempfile();
        let db = Builder::new()
            .set_compression(CompressionConfig::Lz4)
            .create(tmpfile.path())
            .unwrap();

        let wtx = db.begin_write().unwrap();
        {
            let mut table = wtx.open_table(STR_TABLE).unwrap();
            table.insert("hello", "world").unwrap();
            table
                .insert("key", "a]long value that should be compressible if repeated enough times in the page buffer to hit the ratio threshold")
                .unwrap();
        }
        wtx.commit().unwrap();

        let rtx = db.begin_read().unwrap();
        let table = rtx.open_table(STR_TABLE).unwrap();
        assert_eq!(table.get("hello").unwrap().unwrap().value(), "world");
    }

    #[test]
    fn mixed_value_sizes() {
        let tmpfile = create_tempfile();
        let db = Builder::new()
            .set_compression(CompressionConfig::Lz4)
            .create(tmpfile.path())
            .unwrap();

        let wtx = db.begin_write().unwrap();
        {
            let mut table = wtx.open_table(TABLE).unwrap();
            // Small values (inline in leaf, may not compress)
            for i in 0..50 {
                table.insert(i, &[i as u8; 8][..]).unwrap();
            }
            // Large compressible values
            for i in 50..100 {
                let val = compressible_value(8192);
                table.insert(i, val.as_slice()).unwrap();
            }
        }
        wtx.commit().unwrap();

        let rtx = db.begin_read().unwrap();
        let table = rtx.open_table(TABLE).unwrap();
        for i in 0..50 {
            let got = table.get(i).unwrap().unwrap();
            assert_eq!(got.value(), &[i as u8; 8]);
        }
        for i in 50..100 {
            let got = table.get(i).unwrap().unwrap();
            let expected = compressible_value(8192);
            assert_eq!(got.value(), expected.as_slice());
        }
    }

    #[test]
    fn delete_and_reinsert() {
        let tmpfile = create_tempfile();
        let db = Builder::new()
            .set_compression(CompressionConfig::Lz4)
            .create(tmpfile.path())
            .unwrap();

        let value1 = compressible_value(1024);
        let mut value2 = compressible_value(2048);
        value2[0] = 0xFF;

        // Insert
        let wtx = db.begin_write().unwrap();
        {
            let mut table = wtx.open_table(TABLE).unwrap();
            for i in 0..20 {
                table.insert(i, value1.as_slice()).unwrap();
            }
        }
        wtx.commit().unwrap();

        // Delete half, reinsert with different value
        let wtx = db.begin_write().unwrap();
        {
            let mut table = wtx.open_table(TABLE).unwrap();
            for i in 0..10 {
                table.remove(i).unwrap();
            }
            for i in 0..10 {
                table.insert(i, value2.as_slice()).unwrap();
            }
        }
        wtx.commit().unwrap();

        // Verify
        let rtx = db.begin_read().unwrap();
        let table = rtx.open_table(TABLE).unwrap();
        for i in 0..10 {
            assert_eq!(table.get(i).unwrap().unwrap().value(), value2.as_slice());
        }
        for i in 10..20 {
            assert_eq!(table.get(i).unwrap().unwrap().value(), value1.as_slice());
        }
    }

    #[test]
    fn read_only_compressed_db() {
        let tmpfile = create_tempfile();
        let value = compressible_value(4096);

        // Create compressed DB
        {
            let db = Builder::new()
                .set_compression(CompressionConfig::Lz4)
                .create(tmpfile.path())
                .unwrap();
            let wtx = db.begin_write().unwrap();
            {
                let mut table = wtx.open_table(TABLE).unwrap();
                for i in 0..25 {
                    table.insert(i, value.as_slice()).unwrap();
                }
            }
            wtx.commit().unwrap();
        }

        // Open read-only
        let db = Builder::new()
            .set_compression(CompressionConfig::Lz4)
            .open_read_only(tmpfile.path())
            .unwrap();
        let rtx = db.begin_read().unwrap();
        let table = rtx.open_table(TABLE).unwrap();
        assert_eq!(table.len().unwrap(), 25);
        for i in 0..25 {
            assert_eq!(table.get(i).unwrap().unwrap().value(), value.as_slice());
        }
    }

    #[test]
    fn multiple_tables_compressed() {
        let tmpfile = create_tempfile();
        let table_a: TableDefinition<u64, &[u8]> = TableDefinition::new("table_a");
        let table_b: TableDefinition<u64, &[u8]> = TableDefinition::new("table_b");

        let db = Builder::new()
            .set_compression(CompressionConfig::Lz4)
            .create(tmpfile.path())
            .unwrap();

        let val_a = compressible_value(2048);
        let mut val_b = compressible_value(4096);
        val_b[10] = 0xAA;

        let wtx = db.begin_write().unwrap();
        {
            let mut ta = wtx.open_table(table_a).unwrap();
            let mut tb = wtx.open_table(table_b).unwrap();
            for i in 0..30 {
                ta.insert(i, val_a.as_slice()).unwrap();
                tb.insert(i, val_b.as_slice()).unwrap();
            }
        }
        wtx.commit().unwrap();

        let rtx = db.begin_read().unwrap();
        let ta = rtx.open_table(table_a).unwrap();
        let tb = rtx.open_table(table_b).unwrap();
        for i in 0..30 {
            assert_eq!(ta.get(i).unwrap().unwrap().value(), val_a.as_slice());
            assert_eq!(tb.get(i).unwrap().unwrap().value(), val_b.as_slice());
        }
    }

    #[test]
    fn large_value_round_trip() {
        // Test with values much larger than a page to exercise multi-page allocation
        let tmpfile = create_tempfile();
        let db = Builder::new()
            .set_compression(CompressionConfig::Lz4)
            .create(tmpfile.path())
            .unwrap();

        let large_value = compressible_value(256 * 1024); // 256 KB

        let wtx = db.begin_write().unwrap();
        {
            let mut table = wtx.open_table(TABLE).unwrap();
            for i in 0..10 {
                table.insert(i, large_value.as_slice()).unwrap();
            }
        }
        wtx.commit().unwrap();

        let rtx = db.begin_read().unwrap();
        let table = rtx.open_table(TABLE).unwrap();
        for i in 0..10 {
            let got = table.get(i).unwrap().unwrap();
            assert_eq!(got.value(), large_value.as_slice(), "mismatch at key {i}");
        }
    }
}

#[cfg(feature = "compression_zstd")]
mod zstd {
    use super::*;

    #[test]
    fn insert_read_verify() {
        let tmpfile = create_tempfile();
        let db = Builder::new()
            .set_compression(CompressionConfig::Zstd { level: 3 })
            .create(tmpfile.path())
            .unwrap();

        let value = compressible_value(4096);

        let wtx = db.begin_write().unwrap();
        {
            let mut table = wtx.open_table(TABLE).unwrap();
            for i in 0..100 {
                table.insert(i, value.as_slice()).unwrap();
            }
        }
        wtx.commit().unwrap();

        let rtx = db.begin_read().unwrap();
        let table = rtx.open_table(TABLE).unwrap();
        for i in 0..100 {
            assert_eq!(
                table.get(i).unwrap().unwrap().value(),
                value.as_slice(),
                "mismatch at key {i}"
            );
        }
    }

    #[test]
    fn reopen_compressed_db() {
        let tmpfile = create_tempfile();
        let value = compressible_value(2048);

        {
            let db = Builder::new()
                .set_compression(CompressionConfig::Zstd { level: 3 })
                .create(tmpfile.path())
                .unwrap();
            let wtx = db.begin_write().unwrap();
            {
                let mut table = wtx.open_table(TABLE).unwrap();
                for i in 0..50 {
                    table.insert(i, value.as_slice()).unwrap();
                }
            }
            wtx.commit().unwrap();
        }

        {
            let db = Builder::new()
                .set_compression(CompressionConfig::Zstd { level: 3 })
                .open(tmpfile.path())
                .unwrap();
            let rtx = db.begin_read().unwrap();
            let table = rtx.open_table(TABLE).unwrap();
            for i in 0..50 {
                assert_eq!(
                    table.get(i).unwrap().unwrap().value(),
                    value.as_slice(),
                    "mismatch at key {i}"
                );
            }
        }
    }

    #[test]
    fn zstd_levels() {
        // Verify multiple zstd compression levels work
        for level in [1, 3, 6, 9] {
            let tmpfile = create_tempfile();
            let value = compressible_value(4096);

            let db = Builder::new()
                .set_compression(CompressionConfig::Zstd { level })
                .create(tmpfile.path())
                .unwrap();

            let wtx = db.begin_write().unwrap();
            {
                let mut table = wtx.open_table(TABLE).unwrap();
                for i in 0..20 {
                    table.insert(i, value.as_slice()).unwrap();
                }
            }
            wtx.commit().unwrap();

            let rtx = db.begin_read().unwrap();
            let table = rtx.open_table(TABLE).unwrap();
            for i in 0..20 {
                assert_eq!(
                    table.get(i).unwrap().unwrap().value(),
                    value.as_slice(),
                    "mismatch at key {i} with level {level}"
                );
            }
        }
    }
}

#[test]
fn no_compression_backward_compat() {
    // A database created without compression should open fine
    // with a builder that has compression set (it reads None from the header)
    let tmpfile = create_tempfile();
    let value = compressible_value(1024);

    // Create without compression (V3)
    {
        let db = Database::create(tmpfile.path()).unwrap();
        let wtx = db.begin_write().unwrap();
        {
            let mut table = wtx.open_table(TABLE).unwrap();
            for i in 0..10 {
                table.insert(i, value.as_slice()).unwrap();
            }
        }
        wtx.commit().unwrap();
    }

    // Open with default builder (no compression) -- should work
    {
        let db = Database::open(tmpfile.path()).unwrap();
        let rtx = db.begin_read().unwrap();
        let table = rtx.open_table(TABLE).unwrap();
        for i in 0..10 {
            assert_eq!(table.get(i).unwrap().unwrap().value(), value.as_slice());
        }
    }
}

#[test]
fn default_compression_is_none() {
    let config = CompressionConfig::default();
    assert_eq!(config, CompressionConfig::None);
}

#[cfg(feature = "compression_lz4")]
#[test]
fn savepoint_with_compression() {
    let tmpfile = create_tempfile();
    let definition: TableDefinition<u32, &str> = TableDefinition::new("sp_test");
    let db = Builder::new()
        .set_compression(CompressionConfig::Lz4)
        .create(tmpfile.path())
        .unwrap();

    // Insert initial data
    let wtx = db.begin_write().unwrap();
    {
        let mut table = wtx.open_table(definition).unwrap();
        table.insert(&0, "hello").unwrap();
    }
    wtx.commit().unwrap();

    // Create savepoint, then modify
    let wtx = db.begin_write().unwrap();
    let savepoint = wtx.ephemeral_savepoint().unwrap();
    {
        let mut table = wtx.open_table(definition).unwrap();
        table.remove(&0).unwrap();
    }
    wtx.commit().unwrap();

    // Restore savepoint in new transaction
    let mut wtx = db.begin_write().unwrap();
    wtx.restore_savepoint(&savepoint).unwrap();
    wtx.commit().unwrap();

    // Original data should be back
    let rtx = db.begin_read().unwrap();
    let table = rtx.open_table(definition).unwrap();
    assert_eq!(table.get(&0).unwrap().unwrap().value(), "hello");
}

#[cfg(feature = "compression_lz4")]
#[test]
fn range_query_with_compression() {
    let tmpfile = create_tempfile();
    let db = Builder::new()
        .set_compression(CompressionConfig::Lz4)
        .create(tmpfile.path())
        .unwrap();
    let value = compressible_value(1024);

    let wtx = db.begin_write().unwrap();
    {
        let mut table = wtx.open_table(TABLE).unwrap();
        for i in 0..100 {
            table.insert(i, value.as_slice()).unwrap();
        }
    }
    wtx.commit().unwrap();

    let rtx = db.begin_read().unwrap();
    let table = rtx.open_table(TABLE).unwrap();
    let range: Vec<_> = table.range(10..20).unwrap().collect();
    assert_eq!(range.len(), 10);
    for (i, entry) in range.iter().enumerate() {
        let entry = entry.as_ref().unwrap();
        assert_eq!(entry.0.value(), (10 + i) as u64);
        assert_eq!(entry.1.value(), value.as_slice());
    }
}

#[cfg(feature = "compression_lz4")]
#[test]
fn non_durable_commit_with_compression() {
    use shodh_redb::Durability;

    let tmpfile = create_tempfile();
    let db = Builder::new()
        .set_compression(CompressionConfig::Lz4)
        .create(tmpfile.path())
        .unwrap();
    let value = compressible_value(2048);

    // Non-durable write
    let mut wtx = db.begin_write().unwrap();
    {
        let mut table = wtx.open_table(TABLE).unwrap();
        for i in 0..20 {
            table.insert(i, value.as_slice()).unwrap();
        }
    }
    wtx.set_durability(Durability::None).unwrap();
    wtx.commit().unwrap();

    // Should still be readable in same session
    let rtx = db.begin_read().unwrap();
    let table = rtx.open_table(TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 20);
}

#[cfg(feature = "compression_lz4")]
#[test]
fn embedding_vector_simulation() {
    // Simulate shodh-memory's use case: 384-dim float32 embedding vectors
    let tmpfile = create_tempfile();
    let db = Builder::new()
        .set_compression(CompressionConfig::Lz4)
        .create(tmpfile.path())
        .unwrap();

    let dim = 384;
    let vector_bytes = dim * 4; // 1536 bytes per vector

    // Create a "typical" embedding: many values near zero with some variation
    let mut embedding = vec![0u8; vector_bytes];
    for i in 0..dim {
        let val: f32 = ((i as f32 * 0.01).sin() * 0.1) + 0.001;
        embedding[i * 4..(i + 1) * 4].copy_from_slice(&val.to_le_bytes());
    }

    let wtx = db.begin_write().unwrap();
    {
        let mut table = wtx.open_table(TABLE).unwrap();
        for i in 0..500 {
            table.insert(i, embedding.as_slice()).unwrap();
        }
    }
    wtx.commit().unwrap();

    // Verify all vectors survive round-trip
    let rtx = db.begin_read().unwrap();
    let table = rtx.open_table(TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 500);
    for i in 0..500 {
        let got = table.get(i).unwrap().unwrap();
        assert_eq!(got.value(), embedding.as_slice(), "vector {i} corrupted");
    }
}
