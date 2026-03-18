use redb::{
    Database, DynVec, FixedVec, ReadableDatabase, ReadableTableMetadata, TableDefinition,
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
