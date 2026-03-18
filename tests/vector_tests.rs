use redb::{
    BinaryQuantized, Database, DistanceMetric, DynVec, FixedVec, ReadableDatabase, ReadableTable,
    ReadableTableMetadata, ScalarQuantized, TableDefinition,
};

const TABLE_VEC4: TableDefinition<u64, FixedVec<4>> = TableDefinition::new("vectors_4d");
const TABLE_VEC384: TableDefinition<u64, FixedVec<384>> = TableDefinition::new("vectors_384d");
const TABLE_DYN: TableDefinition<u64, DynVec> = TableDefinition::new("vectors_dyn");

fn create_tempfile() -> tempfile::NamedTempFile {
    if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/tmp").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    }
}

#[test]
fn fixed_vec_insert_and_get() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let input: [f32; 4] = [1.0, -2.5, 3.125, 0.0];

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE_VEC4).unwrap();
        table.insert(&1u64, &input).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE_VEC4).unwrap();
    let stored = table.get(&1u64).unwrap().unwrap().value();
    assert_eq!(stored, input);
}

#[test]
fn fixed_vec_large_dimension() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let mut embedding = [0.0f32; 384];
    for (i, val) in embedding.iter_mut().enumerate() {
        *val = (i as f32) * 0.001;
    }

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE_VEC384).unwrap();
        table.insert(&42u64, &embedding).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE_VEC384).unwrap();
    let stored = table.get(&42u64).unwrap().unwrap().value();
    for i in 0..384 {
        assert!(
            (stored[i] - embedding[i]).abs() < f32::EPSILON,
            "mismatch at index {i}"
        );
    }
}

#[test]
fn fixed_vec_insert_reserve() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let values: [f32; 4] = [10.0, 20.0, 30.0, 40.0];

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE_VEC4).unwrap();
        let mut guard = table.insert_reserve(&1u64, 4 * 4).unwrap();
        let buf: &mut [u8] = guard.as_mut();
        redb::write_f32_le(buf, &values);
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE_VEC4).unwrap();
    let stored = table.get(&1u64).unwrap().unwrap().value();
    assert_eq!(stored, values);
}

#[test]
fn fixed_vec_overwrites() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE_VEC4).unwrap();
        table.insert(&1u64, &[1.0f32, 2.0, 3.0, 4.0]).unwrap();
        let old = table.insert(&1u64, &[5.0f32, 6.0, 7.0, 8.0]).unwrap();
        assert_eq!(old.unwrap().value(), [1.0f32, 2.0, 3.0, 4.0]);
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE_VEC4).unwrap();
    assert_eq!(
        table.get(&1u64).unwrap().unwrap().value(),
        [5.0f32, 6.0, 7.0, 8.0]
    );
}

#[test]
fn fixed_vec_remove() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE_VEC4).unwrap();
        table.insert(&1u64, &[1.0f32, 2.0, 3.0, 4.0]).unwrap();
        let removed = table.remove(&1u64).unwrap();
        assert!(removed.is_some());
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE_VEC4).unwrap();
    assert!(table.get(&1u64).unwrap().is_none());
}

#[test]
fn dyn_vec_insert_and_get() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let input: Vec<f32> = vec![1.0, -2.5, 3.125];

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE_DYN).unwrap();
        table.insert(&1u64, &input).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE_DYN).unwrap();
    let stored = table.get(&1u64).unwrap().unwrap().value();
    assert_eq!(stored, input);
}

#[test]
fn dyn_vec_different_dimensions() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let vec3: Vec<f32> = vec![1.0, 2.0, 3.0];
    let vec5: Vec<f32> = vec![10.0, 20.0, 30.0, 40.0, 50.0];

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE_DYN).unwrap();
        table.insert(&1u64, &vec3).unwrap();
        table.insert(&2u64, &vec5).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE_DYN).unwrap();
    assert_eq!(table.get(&1u64).unwrap().unwrap().value(), vec3);
    assert_eq!(table.get(&2u64).unwrap().unwrap().value(), vec5);
}

#[test]
fn dot_product_basic() {
    let a = [1.0f32, 2.0, 3.0];
    let b = [4.0f32, 5.0, 6.0];
    // 1*4 + 2*5 + 3*6 = 4 + 10 + 18 = 32
    let result = redb::dot_product(&a, &b);
    assert!((result - 32.0).abs() < f32::EPSILON);
}

#[test]
fn cosine_similarity_basic() {
    let a = [1.0f32, 0.0, 0.0];
    let b = [1.0f32, 0.0, 0.0];
    let result = redb::cosine_similarity(&a, &b);
    assert!((result - 1.0).abs() < 1e-6);

    // Orthogonal vectors
    let c = [0.0f32, 1.0, 0.0];
    let result = redb::cosine_similarity(&a, &c);
    assert!(result.abs() < 1e-6);

    // Zero vector
    let zero = [0.0f32, 0.0, 0.0];
    let result = redb::cosine_similarity(&a, &zero);
    assert_eq!(result, 0.0);
}

#[test]
fn euclidean_distance_basic() {
    let a = [1.0f32, 2.0, 3.0];
    let b = [4.0f32, 6.0, 3.0];
    // (4-1)^2 + (6-2)^2 + (3-3)^2 = 9 + 16 + 0 = 25
    let result = redb::euclidean_distance_sq(&a, &b);
    assert!((result - 25.0).abs() < f32::EPSILON);
}

#[test]
fn manhattan_distance_basic() {
    let a = [1.0f32, 2.0, 3.0];
    let b = [4.0f32, 6.0, 3.0];
    // |4-1| + |6-2| + |3-3| = 3 + 4 + 0 = 7
    let result = redb::manhattan_distance(&a, &b);
    assert!((result - 7.0).abs() < f32::EPSILON);
}

#[test]
fn distance_with_stored_vectors() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let v1: [f32; 4] = [1.0, 0.0, 0.0, 0.0];
    let v2: [f32; 4] = [0.0, 1.0, 0.0, 0.0];
    let v3: [f32; 4] = [1.0, 1.0, 0.0, 0.0];

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE_VEC4).unwrap();
        table.insert(&1u64, &v1).unwrap();
        table.insert(&2u64, &v2).unwrap();
        table.insert(&3u64, &v3).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE_VEC4).unwrap();

    let query = [1.0f32, 0.0, 0.0, 0.0];
    let stored1 = table.get(&1u64).unwrap().unwrap().value();
    let stored2 = table.get(&2u64).unwrap().unwrap().value();
    let stored3 = table.get(&3u64).unwrap().unwrap().value();

    let sim1 = redb::cosine_similarity(&query, &stored1);
    let sim2 = redb::cosine_similarity(&query, &stored2);
    let sim3 = redb::cosine_similarity(&query, &stored3);

    // v1 is identical to query (sim=1.0), v2 is orthogonal (sim=0.0), v3 is in between
    assert!((sim1 - 1.0).abs() < 1e-6);
    assert!(sim2.abs() < 1e-6);
    assert!(sim3 > 0.5 && sim3 < 1.0);
}

#[test]
fn fixed_vec_multiple_tables() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    const TABLE_A: TableDefinition<u64, FixedVec<4>> = TableDefinition::new("table_a");
    const TABLE_B: TableDefinition<u64, FixedVec<8>> = TableDefinition::new("table_b");

    let write_txn = db.begin_write().unwrap();
    {
        let mut ta = write_txn.open_table(TABLE_A).unwrap();
        ta.insert(&1u64, &[1.0f32, 2.0, 3.0, 4.0]).unwrap();
    }
    {
        let mut tb = write_txn.open_table(TABLE_B).unwrap();
        tb.insert(&1u64, &[1.0f32, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0])
            .unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let ta = read_txn.open_table(TABLE_A).unwrap();
    let tb = read_txn.open_table(TABLE_B).unwrap();
    assert_eq!(ta.get(&1u64).unwrap().unwrap().value().len(), 4);
    assert_eq!(tb.get(&1u64).unwrap().unwrap().value().len(), 8);
}

#[test]
fn fixed_vec_range_scan() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE_VEC4).unwrap();
        for i in 0..10u64 {
            let vec = [i as f32, (i * 2) as f32, (i * 3) as f32, (i * 4) as f32];
            table.insert(&i, &vec).unwrap();
        }
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE_VEC4).unwrap();

    // Range scan keys 3..7
    let entries: Vec<(u64, [f32; 4])> = table
        .range(3u64..7u64)
        .unwrap()
        .map(|r| {
            let (k, v) = r.unwrap();
            (k.value(), v.value())
        })
        .collect();

    assert_eq!(entries.len(), 4);
    assert_eq!(entries[0].0, 3);
    assert_eq!(entries[0].1, [3.0, 6.0, 9.0, 12.0]);
    assert_eq!(entries[3].0, 6);
    assert_eq!(entries[3].1, [6.0, 12.0, 18.0, 24.0]);

    assert_eq!(table.len().unwrap(), 10);
}

#[test]
fn cosine_distance_basic() {
    let a = [1.0f32, 0.0, 0.0];
    let b = [1.0f32, 0.0, 0.0];
    // Identical vectors: distance = 0
    assert!(redb::cosine_distance(&a, &b).abs() < 1e-6);

    // Orthogonal vectors: distance = 1
    let c = [0.0f32, 1.0, 0.0];
    assert!((redb::cosine_distance(&a, &c) - 1.0).abs() < 1e-6);

    // Opposite vectors: distance = 2
    let d = [-1.0f32, 0.0, 0.0];
    assert!((redb::cosine_distance(&a, &d) - 2.0).abs() < 1e-6);
}

#[test]
fn l2_norm_and_normalize() {
    let v = [3.0f32, 4.0];
    assert!((redb::l2_norm(&v) - 5.0).abs() < 1e-6);

    let mut w = [3.0f32, 4.0];
    redb::l2_normalize(&mut w);
    assert!((w[0] - 0.6).abs() < 1e-6);
    assert!((w[1] - 0.8).abs() < 1e-6);
    // Normalized vector has unit norm
    assert!((redb::l2_norm(&w) - 1.0).abs() < 1e-6);

    // Zero vector is unchanged
    let mut z = [0.0f32, 0.0];
    redb::l2_normalize(&mut z);
    assert_eq!(z, [0.0, 0.0]);
}

#[test]
fn l2_normalized_returns_copy() {
    let v = vec![3.0f32, 4.0];
    let normed = redb::l2_normalized(&v);
    assert!((normed[0] - 0.6).abs() < 1e-6);
    assert!((normed[1] - 0.8).abs() < 1e-6);
    // Original is unchanged
    assert_eq!(v, vec![3.0, 4.0]);
}

#[test]
fn normalized_dot_equals_cosine() {
    let a = [1.0f32, 2.0, 3.0, 4.0];
    let b = [5.0f32, 6.0, 7.0, 8.0];

    let cosine = redb::cosine_similarity(&a, &b);

    let na = redb::l2_normalized(&a);
    let nb = redb::l2_normalized(&b);
    let dot = redb::dot_product(&na, &nb);

    // After normalization, dot product == cosine similarity
    assert!((dot - cosine).abs() < 1e-5);
}

#[test]
fn hamming_distance_basic() {
    let a: [u8; 4] = [0b1010_1010, 0b0000_0000, 0b1111_1111, 0b0000_0000];
    let b: [u8; 4] = [0b1010_1010, 0b1111_1111, 0b1111_1111, 0b0000_0000];
    // Only second byte differs: 8 bits
    assert_eq!(redb::hamming_distance(&a, &b), 8);

    // Identical
    assert_eq!(redb::hamming_distance(&a, &a), 0);

    // All bits differ in one byte
    let c: [u8; 1] = [0b0000_0000];
    let d: [u8; 1] = [0b1111_1111];
    assert_eq!(redb::hamming_distance(&c, &d), 8);
}

// ---------------------------------------------------------------------------
// Binary quantization
// ---------------------------------------------------------------------------

#[test]
fn quantize_binary_basic() {
    // 8 dims -> 1 byte
    let v = [1.0f32, -0.5, 0.3, -0.1, 0.0, 0.7, -0.2, 0.9];
    let bq = redb::quantize_binary(&v);
    // positive bits: [1,0,1,0, 0,1,0,1] = 0b10100101 = 0xA5
    assert_eq!(bq.len(), 1);
    assert_eq!(bq[0], 0xA5);
}

#[test]
fn quantize_binary_non_multiple_of_8() {
    // 5 dims -> 1 byte, last 3 bits unused (0)
    let v = [1.0f32, 1.0, 1.0, 1.0, 1.0];
    let bq = redb::quantize_binary(&v);
    assert_eq!(bq.len(), 1);
    // bits: [1,1,1,1,1,0,0,0] = 0b11111000 = 0xF8
    assert_eq!(bq[0], 0xF8);
}

#[test]
fn binary_quantized_store_and_search() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    // 8 dimensions -> 1 byte binary
    const BQ_TABLE: TableDefinition<u64, BinaryQuantized<1>> = TableDefinition::new("bq_vectors");

    let v1 = [1.0f32, -1.0, 1.0, -1.0, 1.0, -1.0, 1.0, -1.0];
    let v2 = [1.0f32, 1.0, 1.0, 1.0, -1.0, -1.0, -1.0, -1.0];
    let bq1: [u8; 1] = redb::quantize_binary(&v1).try_into().unwrap();
    let bq2: [u8; 1] = redb::quantize_binary(&v2).try_into().unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(BQ_TABLE).unwrap();
        table.insert(&1u64, &bq1).unwrap();
        table.insert(&2u64, &bq2).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(BQ_TABLE).unwrap();
    let stored1 = table.get(&1u64).unwrap().unwrap().value();
    let stored2 = table.get(&2u64).unwrap().unwrap().value();

    // Query matches v1 pattern
    let query = redb::quantize_binary(&v1);
    let d1 = redb::hamming_distance(&stored1, &query);
    let d2 = redb::hamming_distance(&stored2, &query);
    assert_eq!(d1, 0); // exact match
    assert!(d2 > 0); // different
}

// ---------------------------------------------------------------------------
// Scalar quantization
// ---------------------------------------------------------------------------

#[test]
fn scalar_quantize_roundtrip() {
    let v: [f32; 4] = [0.0, 0.5, 1.0, 0.25];
    let sq = redb::quantize_scalar(&v);
    assert_eq!(sq.min_val, 0.0);
    assert_eq!(sq.max_val, 1.0);
    assert_eq!(sq.codes[0], 0); // min -> 0
    assert_eq!(sq.codes[2], 255); // max -> 255

    let dq = redb::dequantize_scalar(&sq);
    for i in 0..4 {
        assert!(
            (dq[i] - v[i]).abs() < 0.005,
            "dim {i}: expected {}, got {}",
            v[i],
            dq[i]
        );
    }
}

#[test]
fn scalar_quantize_constant_vector() {
    let v: [f32; 4] = [0.5, 0.5, 0.5, 0.5];
    let sq = redb::quantize_scalar(&v);
    let dq = sq.dequantize();
    for val in &dq {
        assert!((val - 0.5).abs() < f32::EPSILON);
    }
}

#[test]
fn scalar_quantized_store_and_retrieve() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    const SQ_TABLE: TableDefinition<u64, ScalarQuantized<4>> = TableDefinition::new("sq_vectors");

    let original = [0.1f32, 0.5, 0.9, 0.3];
    let sq = redb::quantize_scalar(&original);

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(SQ_TABLE).unwrap();
        table.insert(&1u64, &sq).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(SQ_TABLE).unwrap();
    let stored = table.get(&1u64).unwrap().unwrap().value();
    let recovered = stored.dequantize();
    for i in 0..4 {
        assert!(
            (recovered[i] - original[i]).abs() < 0.005,
            "dim {i}: expected {}, got {}",
            original[i],
            recovered[i]
        );
    }
}

#[test]
fn sq_distance_approximation() {
    let a: [f32; 4] = [0.1, 0.5, 0.9, 0.3];
    let b: [f32; 4] = [0.2, 0.4, 0.8, 0.6];
    let sq_b = redb::quantize_scalar(&b);

    let exact_dist = redb::euclidean_distance_sq(&a, &b);
    let approx_dist = redb::sq_euclidean_distance_sq(&a, &sq_b);

    // Approximate distance should be close to exact (within quantization error)
    assert!(
        (exact_dist - approx_dist).abs() < 0.01,
        "exact={exact_dist}, approx={approx_dist}"
    );
}

// ---------------------------------------------------------------------------
// DistanceMetric enum
// ---------------------------------------------------------------------------

#[test]
fn distance_metric_compute() {
    let a = [1.0f32, 0.0, 0.0];
    let b = [0.0f32, 1.0, 0.0];

    // Cosine: orthogonal -> distance = 1.0
    let d = DistanceMetric::Cosine.compute(&a, &b);
    assert!((d - 1.0).abs() < 1e-6);

    // EuclideanSq: sqrt(2)^2 = 2.0
    let d = DistanceMetric::EuclideanSq.compute(&a, &b);
    assert!((d - 2.0).abs() < 1e-6);

    // DotProduct: -(1*0 + 0*1 + 0*0) = 0
    let d = DistanceMetric::DotProduct.compute(&a, &b);
    assert!(d.abs() < 1e-6);

    // Manhattan: |1-0| + |0-1| + |0-0| = 2
    let d = DistanceMetric::Manhattan.compute(&a, &b);
    assert!((d - 2.0).abs() < 1e-6);
}

#[test]
fn distance_metric_display() {
    assert_eq!(format!("{}", DistanceMetric::Cosine), "cosine");
    assert_eq!(format!("{}", DistanceMetric::EuclideanSq), "euclidean_sq");
    assert_eq!(format!("{}", DistanceMetric::DotProduct), "dot_product");
    assert_eq!(format!("{}", DistanceMetric::Manhattan), "manhattan");
}

// ---------------------------------------------------------------------------
// Top-K nearest neighbor scan
// ---------------------------------------------------------------------------

#[test]
fn nearest_k_basic() {
    let vectors: Vec<(u64, Vec<f32>)> = vec![
        (1, vec![1.0, 0.0, 0.0]),
        (2, vec![0.0, 1.0, 0.0]),
        (3, vec![0.7, 0.7, 0.0]),
        (4, vec![-1.0, 0.0, 0.0]),
    ];

    let query = [1.0f32, 0.0, 0.0];
    let results = redb::nearest_k(vectors.into_iter(), &query, 2, |a, b| {
        DistanceMetric::Cosine.compute(a, b)
    });

    assert_eq!(results.len(), 2);
    // Closest should be key=1 (identical), then key=3 (similar direction)
    assert_eq!(results[0].key, 1);
    assert_eq!(results[1].key, 3);
    assert!(results[0].distance < results[1].distance);
}

#[test]
fn nearest_k_fixed_with_stored_vectors() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let vectors: Vec<(u64, [f32; 4])> = vec![
        (1, [1.0, 0.0, 0.0, 0.0]),
        (2, [0.0, 1.0, 0.0, 0.0]),
        (3, [0.9, 0.1, 0.0, 0.0]),
        (4, [0.5, 0.5, 0.5, 0.5]),
        (5, [-1.0, 0.0, 0.0, 0.0]),
    ];

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE_VEC4).unwrap();
        for (k, v) in &vectors {
            table.insert(k, v).unwrap();
        }
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE_VEC4).unwrap();

    let query = [1.0f32, 0.0, 0.0, 0.0];
    let results = redb::nearest_k_fixed(
        table.iter().unwrap().map(|r| {
            let (k, v) = r.unwrap();
            (k.value(), v.value())
        }),
        &query,
        3,
        |a, b| DistanceMetric::Cosine.compute(a, b),
    );

    assert_eq!(results.len(), 3);
    // Key 1 is exact match (dist ~0), key 3 is next closest, then key 4
    assert_eq!(results[0].key, 1);
    assert!(results[0].distance < 1e-6);
    assert_eq!(results[1].key, 3);
    assert!(results[1].distance < results[2].distance);
}

#[test]
fn nearest_k_zero_returns_empty() {
    let vectors: Vec<(u64, Vec<f32>)> = vec![(1, vec![1.0, 0.0])];
    let query = [1.0f32, 0.0];
    let results = redb::nearest_k(vectors.into_iter(), &query, 0, |a, b| {
        redb::euclidean_distance_sq(a, b)
    });
    assert!(results.is_empty());
}

#[test]
fn nearest_k_more_than_available() {
    let vectors: Vec<(u64, Vec<f32>)> = vec![(1, vec![1.0, 0.0]), (2, vec![0.0, 1.0])];
    let query = [1.0f32, 0.0];
    let results = redb::nearest_k(vectors.into_iter(), &query, 100, |a, b| {
        redb::euclidean_distance_sq(a, b)
    });
    // Should return all available (2), not panic
    assert_eq!(results.len(), 2);
}
