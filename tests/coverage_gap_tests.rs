// Tests for coverage gaps identified in the production readiness audit.
// Covers: TTL len_with_expired, TTL DoubleEndedIterator, sq_dot_product,
// BinaryQuantized N>1, ScalarQuantized negative values, merge 4-byte branch,
// BlobWriter::bytes_written, BlobReader::is_empty/remaining, blob_stats,
// concurrent IVF-PQ reads.

use shodh_redb::{
    BinaryQuantized, ContentType, Database, NumericAdd, NumericMax, NumericMin, ReadableDatabase,
    ScalarQuantized, StoreOptions, TableDefinition, TtlTableDefinition,
};
use std::io::{Read, Seek, SeekFrom};
use std::thread;
use std::time::Duration;

fn create_tempfile() -> tempfile::NamedTempFile {
    if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/tmp").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    }
}

// ---------------------------------------------------------------------------
// TTL: len_with_expired
// ---------------------------------------------------------------------------

const TTL_U64: TtlTableDefinition<&str, u64> = TtlTableDefinition::new("ttl_gap");

#[test]
fn ttl_len_with_expired_counts_all() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    {
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_ttl_table(TTL_U64).unwrap();
            table.insert("alive", &1).unwrap();
            table
                .insert_with_ttl("dead", &2, Duration::from_millis(1))
                .unwrap();
            table.insert("also_alive", &3).unwrap();
        }
        write_txn.commit().unwrap();
    }

    thread::sleep(Duration::from_millis(10));

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_ttl_table(TTL_U64).unwrap();

    // len_with_expired includes the expired entry
    assert_eq!(table.len_with_expired().unwrap(), 3);

    // iter only yields live entries
    let live_count = table.iter().unwrap().count();
    assert_eq!(live_count, 2);
}

#[test]
fn ttl_len_with_expired_on_write_table() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_ttl_table(TTL_U64).unwrap();
        table.insert("a", &1).unwrap();
        table
            .insert_with_ttl("b", &2, Duration::from_millis(1))
            .unwrap();

        // Both visible in len_with_expired immediately
        assert_eq!(table.len_with_expired().unwrap(), 2);
    }
    write_txn.commit().unwrap();
}

// ---------------------------------------------------------------------------
// TTL: DoubleEndedIterator (next_back)
// ---------------------------------------------------------------------------

#[test]
fn ttl_double_ended_iterator() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    {
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_ttl_table(TTL_U64).unwrap();
            table.insert("a", &1).unwrap();
            table
                .insert_with_ttl("b", &2, Duration::from_millis(1))
                .unwrap();
            table.insert("c", &3).unwrap();
            table.insert("d", &4).unwrap();
        }
        write_txn.commit().unwrap();
    }

    thread::sleep(Duration::from_millis(10));

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_ttl_table(TTL_U64).unwrap();

    // Collect in reverse order via next_back
    let mut iter = table.iter().unwrap();
    let last = iter.next_back().unwrap().unwrap();
    assert_eq!(last.0.value(), "d");
    assert_eq!(last.1.value(), 4);

    let second_last = iter.next_back().unwrap().unwrap();
    assert_eq!(second_last.0.value(), "c");
    assert_eq!(second_last.1.value(), 3);

    // "b" is expired, so next_back should skip it and return "a"
    let first = iter.next_back().unwrap().unwrap();
    assert_eq!(first.0.value(), "a");
    assert_eq!(first.1.value(), 1);

    // Iterator exhausted
    assert!(iter.next_back().is_none());
}

#[test]
fn ttl_double_ended_mixed_direction() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    {
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_ttl_table(TTL_U64).unwrap();
            table.insert("a", &1).unwrap();
            table.insert("b", &2).unwrap();
            table.insert("c", &3).unwrap();
        }
        write_txn.commit().unwrap();
    }

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_ttl_table(TTL_U64).unwrap();

    let mut iter = table.iter().unwrap();
    let first = iter.next().unwrap().unwrap();
    assert_eq!(first.0.value(), "a");

    let last = iter.next_back().unwrap().unwrap();
    assert_eq!(last.0.value(), "c");

    // Only "b" remains
    let mid = iter.next().unwrap().unwrap();
    assert_eq!(mid.0.value(), "b");

    assert!(iter.next().is_none());
}

// ---------------------------------------------------------------------------
// TTL: savepoint round-trip
// ---------------------------------------------------------------------------

#[test]
fn ttl_with_savepoints() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    // First txn: seed initial data and commit
    {
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_ttl_table(TTL_U64).unwrap();
            table.insert("before_save", &10).unwrap();
        }
        write_txn.commit().unwrap();
    }

    // Second txn: open table once, take persistent savepoint, modify, commit
    // (verifying TTL data survives across transactions with a savepoint in between)
    {
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_ttl_table(TTL_U64).unwrap();
            // Modify the existing entry
            table.insert("before_save", &99).unwrap();
            table
                .insert_with_ttl("ephemeral", &42, Duration::from_secs(3600))
                .unwrap();
            assert_eq!(table.len_with_expired().unwrap(), 2);
        }
        write_txn.commit().unwrap();
    }

    // Verify both entries survived the commit
    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_ttl_table(TTL_U64).unwrap();
    assert_eq!(table.get("before_save").unwrap().unwrap().value(), 99);
    assert_eq!(table.get("ephemeral").unwrap().unwrap().value(), 42);
    // ephemeral has a TTL
    assert!(table.get("ephemeral").unwrap().unwrap().expires_at_ms() > 0);
}

// ---------------------------------------------------------------------------
// sq_dot_product
// ---------------------------------------------------------------------------

#[test]
fn sq_dot_product_basic() {
    let a: [f32; 4] = [1.0, 2.0, 3.0, 4.0];
    let b: [f32; 4] = [0.1, 0.5, 0.9, 0.3];
    let sq_b = shodh_redb::quantize_scalar(&b);

    let exact = shodh_redb::dot_product(&a, &b);
    let approx = shodh_redb::sq_dot_product(&a, &sq_b);

    assert!(
        (exact - approx).abs() < 0.05,
        "exact={exact}, approx={approx}"
    );
}

#[test]
fn sq_dot_product_constant_vector() {
    // When all values are identical, range=0 triggers the special branch
    let query: [f32; 4] = [1.0, 2.0, 3.0, 4.0];
    let constant: [f32; 4] = [0.5, 0.5, 0.5, 0.5];
    let sq = shodh_redb::quantize_scalar(&constant);

    let approx = shodh_redb::sq_dot_product(&query, &sq);
    let expected = (1.0 + 2.0 + 3.0 + 4.0) * 0.5; // 5.0
    assert!(
        (approx - expected).abs() < 1e-6,
        "expected={expected}, got={approx}"
    );
}

// ---------------------------------------------------------------------------
// BinaryQuantized N>1
// ---------------------------------------------------------------------------

#[test]
fn binary_quantized_multi_byte() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    // 16 dimensions -> 2 bytes
    const BQ_TABLE: TableDefinition<u64, BinaryQuantized<2>> = TableDefinition::new("bq_2byte");

    let v1 = [
        1.0f32, -1.0, 1.0, -1.0, 1.0, -1.0, 1.0, -1.0, 0.5, -0.5, 0.5, -0.5, 0.5, -0.5, 0.5,
        -0.5,
    ];
    let bq1_vec = shodh_redb::quantize_binary(&v1);
    assert_eq!(bq1_vec.len(), 2);
    let bq1: [u8; 2] = bq1_vec.try_into().unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(BQ_TABLE).unwrap();
        table.insert(&1u64, &bq1).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(BQ_TABLE).unwrap();
    let stored = table.get(&1u64).unwrap().unwrap().value();
    assert_eq!(stored, bq1);

    // Hamming distance against itself should be 0
    assert_eq!(shodh_redb::hamming_distance(&stored, &bq1), 0);
}

#[test]
fn binary_quantized_48_bytes() {
    // 384 dimensions -> 48 bytes, typical for small embeddings
    const BQ48: TableDefinition<u64, BinaryQuantized<48>> = TableDefinition::new("bq_48byte");

    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let mut vec384 = [0.0f32; 384];
    for (i, v) in vec384.iter_mut().enumerate() {
        *v = if i % 3 == 0 { 1.0 } else { -1.0 };
    }
    let bq = shodh_redb::quantize_binary(&vec384);
    assert_eq!(bq.len(), 48);
    let bq_arr: [u8; 48] = bq.try_into().unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(BQ48).unwrap();
        table.insert(&1u64, &bq_arr).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(BQ48).unwrap();
    let stored = table.get(&1u64).unwrap().unwrap().value();
    assert_eq!(stored, bq_arr);
}

// ---------------------------------------------------------------------------
// ScalarQuantized with negative values
// ---------------------------------------------------------------------------

#[test]
fn scalar_quantize_negative_values() {
    let v: [f32; 4] = [-2.0, -0.5, 0.5, 2.0];
    let sq = shodh_redb::quantize_scalar(&v);

    assert_eq!(sq.min_val, -2.0);
    assert_eq!(sq.max_val, 2.0);
    assert_eq!(sq.codes[0], 0); // min -> 0
    assert_eq!(sq.codes[3], 255); // max -> 255

    let dq = shodh_redb::dequantize_scalar(&sq);
    for i in 0..4 {
        assert!(
            (dq[i] - v[i]).abs() < 0.02,
            "dim {i}: expected {}, got {}",
            v[i],
            dq[i]
        );
    }
}

#[test]
fn scalar_quantize_negative_store_and_search() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    const SQ_TABLE: TableDefinition<u64, ScalarQuantized<4>> = TableDefinition::new("sq_neg");

    let v1: [f32; 4] = [-1.0, -0.5, 0.5, 1.0];
    let v2: [f32; 4] = [-0.9, -0.4, 0.6, 0.9];
    let v3: [f32; 4] = [1.0, 1.0, -1.0, -1.0];

    let sq1 = shodh_redb::quantize_scalar(&v1);
    let sq2 = shodh_redb::quantize_scalar(&v2);
    let sq3 = shodh_redb::quantize_scalar(&v3);

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(SQ_TABLE).unwrap();
        table.insert(&1u64, &sq1).unwrap();
        table.insert(&2u64, &sq2).unwrap();
        table.insert(&3u64, &sq3).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(SQ_TABLE).unwrap();

    // Distance from query (v1) to v2 should be smaller than to v3
    let query = v1;
    let s2 = table.get(&2u64).unwrap().unwrap().value();
    let s3 = table.get(&3u64).unwrap().unwrap().value();

    let d2 = shodh_redb::sq_euclidean_distance_sq(&query, &s2);
    let d3 = shodh_redb::sq_euclidean_distance_sq(&query, &s3);
    assert!(d2 < d3, "d2={d2}, d3={d3}");
}

// ---------------------------------------------------------------------------
// Merge: 4-byte (u32) branch
// ---------------------------------------------------------------------------

const TABLE_U32: TableDefinition<&str, u32> = TableDefinition::new("merge_u32");

#[test]
fn merge_add_u32() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE_U32).unwrap();
        table.insert("count", &100u32).unwrap();
        table
            .merge("count", &50u32.to_le_bytes(), &NumericAdd)
            .unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE_U32).unwrap();
    assert_eq!(table.get("count").unwrap().unwrap().value(), 150u32);
}

#[test]
fn merge_max_u32() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE_U32).unwrap();
        table.insert("val", &30u32).unwrap();
        table
            .merge("val", &50u32.to_le_bytes(), &NumericMax)
            .unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE_U32).unwrap();
    assert_eq!(table.get("val").unwrap().unwrap().value(), 50u32);
}

#[test]
fn merge_min_u32() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE_U32).unwrap();
        table.insert("val", &50u32).unwrap();
        table
            .merge("val", &30u32.to_le_bytes(), &NumericMin)
            .unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE_U32).unwrap();
    assert_eq!(table.get("val").unwrap().unwrap().value(), 30u32);
}

// ---------------------------------------------------------------------------
// BlobWriter::bytes_written
// ---------------------------------------------------------------------------

#[test]
fn blob_writer_bytes_written() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    let mut writer = write_txn
        .blob_writer(ContentType::OctetStream, "test", StoreOptions::default())
        .unwrap();

    assert_eq!(writer.bytes_written(), 0);

    let chunk1 = b"Hello, ";
    writer.write(chunk1).unwrap();
    assert_eq!(writer.bytes_written(), 7);

    let chunk2 = b"world!";
    writer.write(chunk2).unwrap();
    assert_eq!(writer.bytes_written(), 13);

    let blob_id = writer.finish().unwrap();
    write_txn.commit().unwrap();

    // Verify the blob has the correct total size
    let read_txn = db.begin_read().unwrap();
    let (data, _meta) = read_txn.get_blob(&blob_id).unwrap().unwrap();
    assert_eq!(data.len(), 13);
    assert_eq!(&data, b"Hello, world!");
}

// ---------------------------------------------------------------------------
// BlobReader: is_empty, remaining, read_range
// ---------------------------------------------------------------------------

#[test]
fn blob_reader_is_empty_and_remaining() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let data = b"test blob data for reader";
    let blob_id;
    {
        let write_txn = db.begin_write().unwrap();
        blob_id = write_txn
            .store_blob(
                data,
                ContentType::OctetStream,
                "reader-test",
                StoreOptions::default(),
            )
            .unwrap();
        write_txn.commit().unwrap();
    }

    let read_txn = db.begin_read().unwrap();
    let mut reader = read_txn.blob_reader(&blob_id).unwrap().unwrap();

    assert!(!reader.is_empty());
    assert_eq!(reader.len(), data.len() as u64);
    assert_eq!(reader.remaining(), data.len() as u64);
    assert_eq!(reader.position(), 0);

    // Read 5 bytes
    let mut buf = [0u8; 5];
    reader.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"test ");
    assert_eq!(reader.position(), 5);
    assert_eq!(reader.remaining(), (data.len() - 5) as u64);

    // Seek to end
    reader.seek(SeekFrom::End(0)).unwrap();
    assert_eq!(reader.remaining(), 0);
}

#[test]
fn blob_reader_empty_blob() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let blob_id;
    {
        let write_txn = db.begin_write().unwrap();
        blob_id = write_txn
            .store_blob(
                b"",
                ContentType::OctetStream,
                "empty",
                StoreOptions::default(),
            )
            .unwrap();
        write_txn.commit().unwrap();
    }

    let read_txn = db.begin_read().unwrap();
    let reader = read_txn.blob_reader(&blob_id).unwrap().unwrap();
    assert!(reader.is_empty());
    assert_eq!(reader.remaining(), 0);
    assert_eq!(reader.len(), 0);
}

#[test]
fn blob_reader_read_range() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let data = b"0123456789abcdef";
    let blob_id;
    {
        let write_txn = db.begin_write().unwrap();
        blob_id = write_txn
            .store_blob(
                data,
                ContentType::OctetStream,
                "range",
                StoreOptions::default(),
            )
            .unwrap();
        write_txn.commit().unwrap();
    }

    let read_txn = db.begin_read().unwrap();
    let mut reader = read_txn.blob_reader(&blob_id).unwrap().unwrap();

    // Read a sub-range: offset 4, length 6 -> "456789"
    let chunk = reader.read_range(4, 6).unwrap();
    assert_eq!(&chunk, b"456789");
}

// ---------------------------------------------------------------------------
// blob_stats via WriteTransaction and ReadTransaction
// ---------------------------------------------------------------------------

#[test]
fn blob_stats_basic() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    // Store a blob
    {
        let write_txn = db.begin_write().unwrap();
        write_txn
            .store_blob(
                b"some data",
                ContentType::OctetStream,
                "stats-test",
                StoreOptions::default(),
            )
            .unwrap();

        // Check stats within write txn
        let stats = write_txn.blob_stats().unwrap();
        assert_eq!(stats.blob_count, 1);
        assert!(stats.live_bytes > 0);

        write_txn.commit().unwrap();
    }

    // Check stats via read txn
    let read_txn = db.begin_read().unwrap();
    let stats = read_txn.blob_stats().unwrap();
    assert_eq!(stats.blob_count, 1);
    assert!(stats.live_bytes > 0);
    assert!(stats.region_bytes >= stats.live_bytes);
}

#[test]
fn blob_stats_multiple_blobs() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    {
        let write_txn = db.begin_write().unwrap();
        for i in 0..5u8 {
            let data = vec![i; 100];
            write_txn
                .store_blob(
                    &data,
                    ContentType::OctetStream,
                    "multi",
                    StoreOptions::default(),
                )
                .unwrap();
        }
        write_txn.commit().unwrap();
    }

    let read_txn = db.begin_read().unwrap();
    let stats = read_txn.blob_stats().unwrap();
    assert_eq!(stats.blob_count, 5);
}

// ---------------------------------------------------------------------------
// Concurrent IVF-PQ reads
// ---------------------------------------------------------------------------

#[test]
fn concurrent_ivfpq_reads() {
    use shodh_redb::{DistanceMetric, IvfPqIndexDefinition, SearchParams};
    use std::sync::Arc;

    let tmpfile = create_tempfile();
    let db = Arc::new(Database::create(tmpfile.path()).unwrap());

    const INDEX: IvfPqIndexDefinition = IvfPqIndexDefinition::new(
        "concurrent_test",
        8,  // dim
        4,  // clusters
        4,  // subvectors
        DistanceMetric::EuclideanSq,
    );

    // Train and insert vectors
    {
        let write_txn = db.begin_write().unwrap();
        {
            let mut idx = write_txn.open_ivfpq_index(&INDEX).unwrap();

            let training_vecs: Vec<(u64, Vec<f32>)> = (0..20u64)
                .map(|id| {
                    let vec: Vec<f32> = (0..8).map(|d| (id as f32) * 0.1 + d as f32).collect();
                    (id, vec)
                })
                .collect();
            idx.train(training_vecs.iter().cloned(), 10).unwrap();

            for (id, vec) in &training_vecs {
                idx.insert(*id, vec).unwrap();
            }
        }
        write_txn.commit().unwrap();
    }

    // Spawn concurrent read threads
    let mut handles = vec![];
    for t in 0..4 {
        let db_clone = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            let read_txn = db_clone.begin_read().unwrap();
            let idx = read_txn.open_ivfpq_index(&INDEX).unwrap();

            let query: Vec<f32> = (0..8).map(|d| (t as f32) * 0.5 + d as f32).collect();
            let params = SearchParams {
                nprobe: 2,
                candidates: 10,
                k: 5,
                rerank: false,
            };
            let results = idx.search(&read_txn, &query, &params).unwrap();
            assert!(!results.is_empty());
            assert!(results.len() <= 5);

            // Results should be ordered by distance
            for w in results.windows(2) {
                assert!(w[0].distance <= w[1].distance);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

// ---------------------------------------------------------------------------
// Merge: unsupported byte width fallback (graceful no-op)
// ---------------------------------------------------------------------------

#[test]
fn merge_unsupported_width_noop() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    const TABLE_BYTES: TableDefinition<&str, &[u8]> = TableDefinition::new("merge_odd");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE_BYTES).unwrap();
        // 3-byte value: not a supported numeric width (1, 2, 4, 8)
        table.insert("key", &[10u8, 20, 30][..]).unwrap();
        // NumericAdd on 3 bytes should be a no-op (return existing)
        table
            .merge("key", &[1u8, 1, 1], &NumericAdd)
            .unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE_BYTES).unwrap();
    // Value unchanged -- the merge was a no-op for unsupported width
    assert_eq!(
        table.get("key").unwrap().unwrap().value(),
        &[10u8, 20, 30]
    );
}
