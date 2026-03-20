use shodh_redb::{
    Database, DistanceMetric, IvfPqIndexDefinition, ReadableDatabase, SearchParams,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn create_tempfile() -> tempfile::NamedTempFile {
    if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/tmp").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    }
}

/// Deterministic pseudo-random f32 vector from a seed.
fn random_vector(seed: u64, dim: usize) -> Vec<f32> {
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    (0..dim)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            // Map to [-1, 1]
            ((state as f64) / (u64::MAX as f64) * 2.0 - 1.0) as f32
        })
        .collect()
}

/// Brute-force k-nearest neighbors for recall benchmarking.
fn brute_force_knn(
    query: &[f32],
    vectors: &[(u64, Vec<f32>)],
    k: usize,
    metric: DistanceMetric,
) -> Vec<u64> {
    let mut dists: Vec<(u64, f32)> = vectors
        .iter()
        .map(|(id, v)| (*id, metric.compute(query, v)))
        .collect();
    dists.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    dists.into_iter().take(k).map(|(id, _)| id).collect()
}

// ---------------------------------------------------------------------------
// Index definitions used across tests
// ---------------------------------------------------------------------------

const INDEX_8D: IvfPqIndexDefinition = IvfPqIndexDefinition::new(
    "test_8d",
    8,   // dim
    4,   // clusters
    2,   // subvectors (sub_dim = 4)
    DistanceMetric::EuclideanSq,
)
.with_raw_vectors()
.with_nprobe(4);

const INDEX_128D: IvfPqIndexDefinition = IvfPqIndexDefinition::new(
    "test_128d",
    128,  // dim
    16,   // clusters
    16,   // subvectors (sub_dim = 8)
    DistanceMetric::EuclideanSq,
)
.with_raw_vectors()
.with_nprobe(10);

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn train_insert_search_basic() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    // Training vectors: 8 vectors of dim 8
    let training: Vec<(u64, Vec<f32>)> = (0..8)
        .map(|i| (i, random_vector(i + 100, 8)))
        .collect();

    let write_txn = db.begin_write().unwrap();
    {
        let mut idx = write_txn.open_ivfpq_index(&INDEX_8D).unwrap();
        idx.train(training.iter().map(|(id, v)| (*id, v.clone())), 25)
            .unwrap();

        // Insert the training vectors
        for (id, vec) in &training {
            idx.insert(*id, vec).unwrap();
        }
    }
    write_txn.commit().unwrap();

    // Search from a read transaction
    let read_txn = db.begin_read().unwrap();
    let idx = read_txn.open_ivfpq_index(&INDEX_8D).unwrap();

    let results = idx
        .search(
            &read_txn,
            &training[0].1,
            &SearchParams::top_k(3),
        )
        .unwrap();

    assert!(!results.is_empty());
    assert!(results.len() <= 3);
    // The query vector itself should be the closest match
    assert_eq!(results[0].key, training[0].0);
}

#[test]
fn search_within_write_transaction() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let vectors: Vec<(u64, Vec<f32>)> = (0..20)
        .map(|i| (i, random_vector(i + 200, 8)))
        .collect();

    let write_txn = db.begin_write().unwrap();
    {
        let mut idx = write_txn.open_ivfpq_index(&INDEX_8D).unwrap();
        idx.train(vectors.iter().map(|(id, v)| (*id, v.clone())), 25)
            .unwrap();
        for (id, vec) in &vectors {
            idx.insert(*id, vec).unwrap();
        }

        // Search within the same write transaction
        let results = idx
            .search(&vectors[5].1, &SearchParams::top_k(5))
            .unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].key, vectors[5].0);
    }
    write_txn.commit().unwrap();
}

#[test]
fn persistence_across_transactions() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let vectors: Vec<(u64, Vec<f32>)> = (0..16)
        .map(|i| (i, random_vector(i + 300, 8)))
        .collect();

    // Train and insert in one transaction
    let write_txn = db.begin_write().unwrap();
    {
        let mut idx = write_txn.open_ivfpq_index(&INDEX_8D).unwrap();
        idx.train(vectors.iter().map(|(id, v)| (*id, v.clone())), 25)
            .unwrap();
        for (id, vec) in &vectors {
            idx.insert(*id, vec).unwrap();
        }
    }
    write_txn.commit().unwrap();

    // Insert more in a second transaction
    let extra: Vec<(u64, Vec<f32>)> = (100..108)
        .map(|i| (i, random_vector(i + 400, 8)))
        .collect();

    let write_txn2 = db.begin_write().unwrap();
    {
        let mut idx = write_txn2.open_ivfpq_index(&INDEX_8D).unwrap();
        assert_eq!(idx.config().num_vectors, 16);
        for (id, vec) in &extra {
            idx.insert(*id, vec).unwrap();
        }
        assert_eq!(idx.config().num_vectors, 24);
    }
    write_txn2.commit().unwrap();

    // Verify all vectors are searchable
    let read_txn = db.begin_read().unwrap();
    let idx = read_txn.open_ivfpq_index(&INDEX_8D).unwrap();
    assert_eq!(idx.config().num_vectors, 24);

    let results = idx
        .search(&read_txn, &extra[0].1, &SearchParams::top_k(1))
        .unwrap();
    assert_eq!(results[0].key, extra[0].0);
}

#[test]
fn insert_batch() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let vectors: Vec<(u64, Vec<f32>)> = (0..32)
        .map(|i| (i, random_vector(i + 500, 8)))
        .collect();

    let write_txn = db.begin_write().unwrap();
    {
        let mut idx = write_txn.open_ivfpq_index(&INDEX_8D).unwrap();
        idx.train(vectors.iter().map(|(id, v)| (*id, v.clone())), 25)
            .unwrap();

        let count = idx
            .insert_batch(vectors.iter().map(|(id, v)| (*id, v.clone())))
            .unwrap();
        assert_eq!(count, 32);
        assert_eq!(idx.config().num_vectors, 32);
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let idx = read_txn.open_ivfpq_index(&INDEX_8D).unwrap();
    let results = idx
        .search(&read_txn, &vectors[15].1, &SearchParams::top_k(1))
        .unwrap();
    assert_eq!(results[0].key, 15);
}

#[test]
fn remove_vector() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let vectors: Vec<(u64, Vec<f32>)> = (0..12)
        .map(|i| (i, random_vector(i + 600, 8)))
        .collect();

    let write_txn = db.begin_write().unwrap();
    {
        let mut idx = write_txn.open_ivfpq_index(&INDEX_8D).unwrap();
        idx.train(vectors.iter().map(|(id, v)| (*id, v.clone())), 25)
            .unwrap();
        for (id, vec) in &vectors {
            idx.insert(*id, vec).unwrap();
        }
        assert_eq!(idx.config().num_vectors, 12);

        // Remove vector 5
        let removed = idx.remove(5).unwrap();
        assert!(removed);
        assert_eq!(idx.config().num_vectors, 11);

        // Removing again returns false
        let removed_again = idx.remove(5).unwrap();
        assert!(!removed_again);
        assert_eq!(idx.config().num_vectors, 11);
    }
    write_txn.commit().unwrap();

    // Verify the removed vector doesn't appear in top results when queried
    let read_txn = db.begin_read().unwrap();
    let idx = read_txn.open_ivfpq_index(&INDEX_8D).unwrap();
    assert_eq!(idx.config().num_vectors, 11);

    let results = idx
        .search(&read_txn, &vectors[5].1, &SearchParams::top_k(11))
        .unwrap();
    for r in &results {
        assert_ne!(r.key, 5, "removed vector should not appear in results");
    }
}

#[test]
fn reranking_improves_accuracy() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let vectors: Vec<(u64, Vec<f32>)> = (0..50)
        .map(|i| (i, random_vector(i + 700, 8)))
        .collect();

    let write_txn = db.begin_write().unwrap();
    {
        let mut idx = write_txn.open_ivfpq_index(&INDEX_8D).unwrap();
        idx.train(vectors.iter().map(|(id, v)| (*id, v.clone())), 25)
            .unwrap();
        for (id, vec) in &vectors {
            idx.insert(*id, vec).unwrap();
        }
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let idx = read_txn.open_ivfpq_index(&INDEX_8D).unwrap();

    let query = &vectors[10].1;

    // With reranking (exact distances)
    let params_rerank = SearchParams {
        nprobe: 4,
        candidates: 50,
        k: 5,
        rerank: true,
    };
    let reranked = idx.search(&read_txn, query, &params_rerank).unwrap();

    // Without reranking (PQ-approximate distances)
    let params_no_rerank = SearchParams {
        nprobe: 4,
        candidates: 50,
        k: 5,
        rerank: false,
    };
    let approx = idx.search(&read_txn, query, &params_no_rerank).unwrap();

    // Both should find the exact match at position 0
    assert_eq!(reranked[0].key, 10);
    assert_eq!(approx[0].key, 10);

    // Reranked distances should be exact (rerank uses raw vectors)
    let exact_self_dist =
        DistanceMetric::EuclideanSq.compute(query, &vectors[reranked[0].key as usize].1);
    assert!(
        (reranked[0].distance - exact_self_dist).abs() < 1e-5,
        "reranked distance should be exact"
    );
}

#[test]
fn abort_transaction_leaves_index_unchanged() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let vectors: Vec<(u64, Vec<f32>)> = (0..10)
        .map(|i| (i, random_vector(i + 800, 8)))
        .collect();

    // Train and insert, then commit
    let write_txn = db.begin_write().unwrap();
    {
        let mut idx = write_txn.open_ivfpq_index(&INDEX_8D).unwrap();
        idx.train(vectors.iter().map(|(id, v)| (*id, v.clone())), 25)
            .unwrap();
        for (id, vec) in &vectors {
            idx.insert(*id, vec).unwrap();
        }
    }
    write_txn.commit().unwrap();

    // Start another transaction, insert more, but abort (drop without commit)
    {
        let write_txn2 = db.begin_write().unwrap();
        let mut idx = write_txn2.open_ivfpq_index(&INDEX_8D).unwrap();
        idx.insert(999, &random_vector(999, 8)).unwrap();
        assert_eq!(idx.config().num_vectors, 11);
        // Drop without commit — transaction aborts
    }

    // Verify the aborted insert is not visible
    let read_txn = db.begin_read().unwrap();
    let idx = read_txn.open_ivfpq_index(&INDEX_8D).unwrap();
    assert_eq!(idx.config().num_vectors, 10);
}

#[test]
fn multiple_indices_same_database() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    const INDEX_A: IvfPqIndexDefinition = IvfPqIndexDefinition::new(
        "index_a",
        8, 4, 2,
        DistanceMetric::EuclideanSq,
    )
    .with_raw_vectors()
    .with_nprobe(4);

    const INDEX_B: IvfPqIndexDefinition = IvfPqIndexDefinition::new(
        "index_b",
        8, 4, 2,
        DistanceMetric::EuclideanSq,
    )
    .with_raw_vectors()
    .with_nprobe(4);

    let vecs_a: Vec<(u64, Vec<f32>)> = (0..10)
        .map(|i| (i, random_vector(i + 1000, 8)))
        .collect();
    let vecs_b: Vec<(u64, Vec<f32>)> = (0..10)
        .map(|i| (i + 100, random_vector(i + 2000, 8)))
        .collect();

    let write_txn = db.begin_write().unwrap();
    {
        let mut idx_a = write_txn.open_ivfpq_index(&INDEX_A).unwrap();
        idx_a
            .train(vecs_a.iter().map(|(id, v)| (*id, v.clone())), 25)
            .unwrap();
        for (id, vec) in &vecs_a {
            idx_a.insert(*id, vec).unwrap();
        }
    }
    {
        let mut idx_b = write_txn.open_ivfpq_index(&INDEX_B).unwrap();
        idx_b
            .train(vecs_b.iter().map(|(id, v)| (*id, v.clone())), 25)
            .unwrap();
        for (id, vec) in &vecs_b {
            idx_b.insert(*id, vec).unwrap();
        }
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let idx_a = read_txn.open_ivfpq_index(&INDEX_A).unwrap();
    let idx_b = read_txn.open_ivfpq_index(&INDEX_B).unwrap();

    assert_eq!(idx_a.config().num_vectors, 10);
    assert_eq!(idx_b.config().num_vectors, 10);

    // Index A should find its own vectors
    let r_a = idx_a
        .search(&read_txn, &vecs_a[0].1, &SearchParams::top_k(1))
        .unwrap();
    assert_eq!(r_a[0].key, vecs_a[0].0);

    // Index B should find its own vectors
    let r_b = idx_b
        .search(&read_txn, &vecs_b[0].1, &SearchParams::top_k(1))
        .unwrap();
    assert_eq!(r_b[0].key, vecs_b[0].0);
}

#[test]
fn untrained_index_errors_on_insert() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    let mut idx = write_txn.open_ivfpq_index(&INDEX_8D).unwrap();

    let result = idx.insert(1, &[1.0; 8]);
    assert!(result.is_err(), "insert on untrained index should error");
}

#[test]
fn wrong_dimension_errors() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let training: Vec<(u64, Vec<f32>)> = (0..8)
        .map(|i| (i, random_vector(i + 900, 8)))
        .collect();

    let write_txn = db.begin_write().unwrap();
    let mut idx = write_txn.open_ivfpq_index(&INDEX_8D).unwrap();
    idx.train(training.iter().map(|(id, v)| (*id, v.clone())), 25)
        .unwrap();

    // Wrong dimension on insert
    let result = idx.insert(99, &[1.0; 4]);
    assert!(result.is_err(), "wrong dim insert should error");

    // Wrong dimension on search
    let result = idx.search(&[1.0; 4], &SearchParams::top_k(1));
    assert!(result.is_err(), "wrong dim search should error");
}

#[test]
fn duplicate_insert_upsert_semantics() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let vectors: Vec<(u64, Vec<f32>)> = (0..10)
        .map(|i| (i, random_vector(i + 1700, 8)))
        .collect();

    let write_txn = db.begin_write().unwrap();
    {
        let mut idx = write_txn.open_ivfpq_index(&INDEX_8D).unwrap();
        idx.train(vectors.iter().map(|(id, v)| (*id, v.clone())), 25)
            .unwrap();
        for (id, vec) in &vectors {
            idx.insert(*id, vec).unwrap();
        }
        assert_eq!(idx.config().num_vectors, 10);

        // Re-insert vector 3 with a different vector -- should not increase count
        let new_vec = random_vector(9999, 8);
        idx.insert(3, &new_vec).unwrap();
        assert_eq!(idx.config().num_vectors, 10, "duplicate insert should not increase count");

        // Search should find the updated vector
        let results = idx.search(&new_vec, &SearchParams::top_k(1)).unwrap();
        assert_eq!(results[0].key, 3);
    }
    write_txn.commit().unwrap();
}

#[test]
fn write_txn_search_without_insert() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let vectors: Vec<(u64, Vec<f32>)> = (0..10)
        .map(|i| (i, random_vector(i + 1800, 8)))
        .collect();

    // Train and insert in first txn
    let write_txn = db.begin_write().unwrap();
    {
        let mut idx = write_txn.open_ivfpq_index(&INDEX_8D).unwrap();
        idx.train(vectors.iter().map(|(id, v)| (*id, v.clone())), 25)
            .unwrap();
        for (id, vec) in &vectors {
            idx.insert(*id, vec).unwrap();
        }
    }
    write_txn.commit().unwrap();

    // Open a new write txn and search WITHOUT inserting first (C1 regression test)
    let write_txn2 = db.begin_write().unwrap();
    {
        let mut idx = write_txn2.open_ivfpq_index(&INDEX_8D).unwrap();
        let results = idx.search(&vectors[0].1, &SearchParams::top_k(1)).unwrap();
        assert_eq!(results[0].key, 0);
    }
    write_txn2.commit().unwrap();
}

#[test]
fn search_k_larger_than_index_size() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let vectors: Vec<(u64, Vec<f32>)> = (0..5)
        .map(|i| (i, random_vector(i + 1100, 8)))
        .collect();

    let write_txn = db.begin_write().unwrap();
    {
        let mut idx = write_txn.open_ivfpq_index(&INDEX_8D).unwrap();
        idx.train(vectors.iter().map(|(id, v)| (*id, v.clone())), 25)
            .unwrap();
        for (id, vec) in &vectors {
            idx.insert(*id, vec).unwrap();
        }
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let idx = read_txn.open_ivfpq_index(&INDEX_8D).unwrap();

    // Ask for 100 results but only 5 vectors exist
    let results = idx
        .search(&read_txn, &vectors[0].1, &SearchParams::top_k(100))
        .unwrap();
    assert_eq!(results.len(), 5);
}

#[test]
fn cosine_metric() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    const COSINE_INDEX: IvfPqIndexDefinition = IvfPqIndexDefinition::new(
        "cosine_test",
        8, 4, 2,
        DistanceMetric::Cosine,
    )
    .with_raw_vectors()
    .with_nprobe(4);

    let vectors: Vec<(u64, Vec<f32>)> = (0..20)
        .map(|i| (i, random_vector(i + 1200, 8)))
        .collect();

    let write_txn = db.begin_write().unwrap();
    {
        let mut idx = write_txn.open_ivfpq_index(&COSINE_INDEX).unwrap();
        idx.train(vectors.iter().map(|(id, v)| (*id, v.clone())), 25)
            .unwrap();
        for (id, vec) in &vectors {
            idx.insert(*id, vec).unwrap();
        }
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let idx = read_txn.open_ivfpq_index(&COSINE_INDEX).unwrap();

    let results = idx
        .search(&read_txn, &vectors[0].1, &SearchParams::top_k(3))
        .unwrap();
    assert!(!results.is_empty());
    // Self-match should be closest
    assert_eq!(results[0].key, 0);
}

#[test]
fn dotproduct_metric() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    const DP_INDEX: IvfPqIndexDefinition = IvfPqIndexDefinition::new(
        "dp_test",
        8, 4, 2,
        DistanceMetric::DotProduct,
    )
    .with_raw_vectors()
    .with_nprobe(4);

    let vectors: Vec<(u64, Vec<f32>)> = (0..20)
        .map(|i| (i, random_vector(i + 1300, 8)))
        .collect();

    let write_txn = db.begin_write().unwrap();
    {
        let mut idx = write_txn.open_ivfpq_index(&DP_INDEX).unwrap();
        idx.train(vectors.iter().map(|(id, v)| (*id, v.clone())), 25)
            .unwrap();
        for (id, vec) in &vectors {
            idx.insert(*id, vec).unwrap();
        }
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let idx = read_txn.open_ivfpq_index(&DP_INDEX).unwrap();

    let results = idx
        .search(&read_txn, &vectors[0].1, &SearchParams::top_k(3))
        .unwrap();
    assert!(!results.is_empty());
    assert_eq!(results[0].key, 0);
}

#[test]
fn manhattan_metric() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    const MANH_INDEX: IvfPqIndexDefinition = IvfPqIndexDefinition::new(
        "manhattan_test",
        8, 4, 2,
        DistanceMetric::Manhattan,
    )
    .with_raw_vectors()
    .with_nprobe(4);

    let vectors: Vec<(u64, Vec<f32>)> = (0..20)
        .map(|i| (i, random_vector(i + 1400, 8)))
        .collect();

    let write_txn = db.begin_write().unwrap();
    {
        let mut idx = write_txn.open_ivfpq_index(&MANH_INDEX).unwrap();
        idx.train(vectors.iter().map(|(id, v)| (*id, v.clone())), 25)
            .unwrap();
        for (id, vec) in &vectors {
            idx.insert(*id, vec).unwrap();
        }
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let idx = read_txn.open_ivfpq_index(&MANH_INDEX).unwrap();

    let results = idx
        .search(&read_txn, &vectors[0].1, &SearchParams::top_k(3))
        .unwrap();
    assert!(!results.is_empty());
    assert_eq!(results[0].key, 0);
}

#[test]
fn recall_benchmark_128d() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let num_vectors = 1000;
    let dim = 128;
    let k = 10;

    let vectors: Vec<(u64, Vec<f32>)> = (0..num_vectors)
        .map(|i| (i, random_vector(i + 5000, dim)))
        .collect();

    let write_txn = db.begin_write().unwrap();
    {
        let mut idx = write_txn.open_ivfpq_index(&INDEX_128D).unwrap();
        idx.train(vectors.iter().map(|(id, v)| (*id, v.clone())), 25)
            .unwrap();
        let count = idx
            .insert_batch(vectors.iter().map(|(id, v)| (*id, v.clone())))
            .unwrap();
        assert_eq!(count, num_vectors);
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let idx = read_txn.open_ivfpq_index(&INDEX_128D).unwrap();

    // Run recall test over 50 random queries
    let num_queries = 50;
    let mut total_recall = 0.0f64;

    for q in 0..num_queries {
        let query = &vectors[q as usize].1;
        let gt = brute_force_knn(query, &vectors, k, DistanceMetric::EuclideanSq);

        let params = SearchParams {
            nprobe: 14,
            candidates: 200,
            k,
            rerank: true,
        };
        let results = idx.search(&read_txn, query, &params).unwrap();
        let result_ids: Vec<u64> = results.iter().map(|r| r.key).collect();

        let hits = gt.iter().filter(|id| result_ids.contains(id)).count();
        total_recall += hits as f64 / k as f64;
    }

    let avg_recall = total_recall / num_queries as f64;
    assert!(
        avg_recall > 0.85,
        "recall@{k} = {avg_recall:.3} — expected > 0.85 with nprobe=14 and reranking"
    );
}

#[test]
fn index_without_raw_vectors() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    // No .with_raw_vectors() — PQ-only distance
    const NO_RAW_INDEX: IvfPqIndexDefinition = IvfPqIndexDefinition::new(
        "no_raw",
        8, 4, 2,
        DistanceMetric::EuclideanSq,
    )
    .with_nprobe(4);

    let vectors: Vec<(u64, Vec<f32>)> = (0..20)
        .map(|i| (i, random_vector(i + 1500, 8)))
        .collect();

    let write_txn = db.begin_write().unwrap();
    {
        let mut idx = write_txn.open_ivfpq_index(&NO_RAW_INDEX).unwrap();
        assert!(!idx.config().store_raw_vectors);

        idx.train(vectors.iter().map(|(id, v)| (*id, v.clone())), 25)
            .unwrap();
        for (id, vec) in &vectors {
            idx.insert(*id, vec).unwrap();
        }
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let idx = read_txn.open_ivfpq_index(&NO_RAW_INDEX).unwrap();

    // Search with rerank=true but no raw vectors — should fall back to PQ distances
    let params = SearchParams {
        nprobe: 4,
        candidates: 20,
        k: 3,
        rerank: true,
    };
    let results = idx.search(&read_txn, &vectors[0].1, &params).unwrap();
    assert!(!results.is_empty());
    // Self-match should still be closest even with PQ approximation
    assert_eq!(results[0].key, 0);
}

#[test]
fn config_persists_correctly() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let training: Vec<(u64, Vec<f32>)> = (0..8)
        .map(|i| (i, random_vector(i + 1900, 8)))
        .collect();

    let write_txn = db.begin_write().unwrap();
    {
        let mut idx = write_txn.open_ivfpq_index(&INDEX_8D).unwrap();
        let cfg = idx.config();
        assert_eq!(cfg.dim, 8);
        assert_eq!(cfg.num_clusters, 4);
        assert_eq!(cfg.num_subvectors, 2);
        assert_eq!(cfg.num_codewords, 256);
        assert_eq!(cfg.metric, DistanceMetric::EuclideanSq);
        assert!(cfg.store_raw_vectors);
        assert_eq!(cfg.default_nprobe, 4);
        assert_eq!(cfg.num_vectors, 0);

        // Train so centroids/codebooks exist for read-only open
        idx.train(training.iter().map(|(id, v)| (*id, v.clone())), 25)
            .unwrap();
    }
    write_txn.commit().unwrap();

    // Re-open read-only and verify config persists
    let read_txn = db.begin_read().unwrap();
    let idx = read_txn.open_ivfpq_index(&INDEX_8D).unwrap();
    let cfg = idx.config();
    assert_eq!(cfg.dim, 8);
    assert_eq!(cfg.num_clusters, 4);
    assert!(cfg.store_raw_vectors);
}

#[test]
fn database_reopen_persistence() {
    let tmpfile = create_tempfile();

    let vectors: Vec<(u64, Vec<f32>)> = (0..10)
        .map(|i| (i, random_vector(i + 1600, 8)))
        .collect();

    // Create, train, insert, close
    {
        let db = Database::create(tmpfile.path()).unwrap();
        let write_txn = db.begin_write().unwrap();
        {
            let mut idx = write_txn.open_ivfpq_index(&INDEX_8D).unwrap();
            idx.train(vectors.iter().map(|(id, v)| (*id, v.clone())), 25)
                .unwrap();
            for (id, vec) in &vectors {
                idx.insert(*id, vec).unwrap();
            }
        }
        write_txn.commit().unwrap();
    }

    // Reopen database and verify data persists
    {
        let db = Database::open(tmpfile.path()).unwrap();
        let read_txn = db.begin_read().unwrap();
        let idx = read_txn.open_ivfpq_index(&INDEX_8D).unwrap();
        assert_eq!(idx.config().num_vectors, 10);

        let results = idx
            .search(&read_txn, &vectors[3].1, &SearchParams::top_k(1))
            .unwrap();
        assert_eq!(results[0].key, 3);
    }
}

/// Regression test: training with fewer vectors than num_clusters.
///
/// kmeans clamps k to min(num_clusters, n). The config must be updated to
/// reflect the actual number of centroids, otherwise a subsequent open will
/// fail trying to read centroid rows that don't exist.
#[test]
fn train_fewer_vectors_than_clusters() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    // Index requests 4 clusters, but we only provide 2 training vectors.
    let vectors: Vec<(u64, Vec<f32>)> = (0..2)
        .map(|i| (i, random_vector(i + 2000, 8)))
        .collect();

    let write_txn = db.begin_write().unwrap();
    {
        let mut idx = write_txn.open_ivfpq_index(&INDEX_8D).unwrap();
        idx.train(vectors.iter().map(|(id, v)| (*id, v.clone())), 25)
            .unwrap();

        // Config should have been clamped to the actual number of centroids.
        assert_eq!(idx.config().num_clusters, 2);

        for (id, vec) in &vectors {
            idx.insert(*id, vec).unwrap();
        }
    }
    write_txn.commit().unwrap();

    // Read-only open must succeed (previously would fail reading missing centroids).
    let read_txn = db.begin_read().unwrap();
    let idx = read_txn.open_ivfpq_index(&INDEX_8D).unwrap();
    assert_eq!(idx.config().num_clusters, 2);
    assert_eq!(idx.config().num_vectors, 2);

    let results = idx
        .search(&read_txn, &vectors[0].1, &SearchParams::top_k(2))
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].key, 0);
}

/// Regression test: re-training restores the definition's original cluster count
/// and clears stale postings/assignments from the prior cycle.
#[test]
fn retrain_restores_cluster_count_and_clears_data() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    // First train with only 2 vectors → clamped to 2 clusters.
    let small_set: Vec<(u64, Vec<f32>)> = (0..2)
        .map(|i| (i, random_vector(i + 3000, 8)))
        .collect();

    let write_txn = db.begin_write().unwrap();
    {
        let mut idx = write_txn.open_ivfpq_index(&INDEX_8D).unwrap();
        idx.train(small_set.iter().map(|(id, v)| (*id, v.clone())), 25)
            .unwrap();
        assert_eq!(idx.config().num_clusters, 2);

        for (id, vec) in &small_set {
            idx.insert(*id, vec).unwrap();
        }
        assert_eq!(idx.config().num_vectors, 2);
    }
    write_txn.commit().unwrap();

    // Re-train with 20 vectors → should use the definition's 4 clusters, not 2.
    let large_set: Vec<(u64, Vec<f32>)> = (0..20)
        .map(|i| (i, random_vector(i + 3100, 8)))
        .collect();

    let write_txn2 = db.begin_write().unwrap();
    {
        let mut idx = write_txn2.open_ivfpq_index(&INDEX_8D).unwrap();
        idx.train(large_set.iter().map(|(id, v)| (*id, v.clone())), 25)
            .unwrap();

        // Cluster count should be restored to the definition's 4.
        assert_eq!(idx.config().num_clusters, 4);
        // Old vectors should be cleared.
        assert_eq!(idx.config().num_vectors, 0);

        // Re-insert with the new training.
        for (id, vec) in &large_set {
            idx.insert(*id, vec).unwrap();
        }
        assert_eq!(idx.config().num_vectors, 20);
    }
    write_txn2.commit().unwrap();

    // Verify search works correctly after re-training.
    let read_txn = db.begin_read().unwrap();
    let idx = read_txn.open_ivfpq_index(&INDEX_8D).unwrap();
    assert_eq!(idx.config().num_clusters, 4);
    assert_eq!(idx.config().num_vectors, 20);

    let results = idx
        .search(&read_txn, &large_set[5].1, &SearchParams::top_k(1))
        .unwrap();
    assert_eq!(results[0].key, 5);
}

/// Regression test: NaN and Inf vectors are rejected on insert.
#[test]
fn nan_inf_vectors_rejected() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let training: Vec<(u64, Vec<f32>)> = (0..8)
        .map(|i| (i, random_vector(i + 4000, 8)))
        .collect();

    let write_txn = db.begin_write().unwrap();
    {
        let mut idx = write_txn.open_ivfpq_index(&INDEX_8D).unwrap();
        idx.train(training.iter().map(|(id, v)| (*id, v.clone())), 25)
            .unwrap();

        // NaN should be rejected.
        let nan_vec = vec![f32::NAN; 8];
        assert!(idx.insert(100, &nan_vec).is_err());

        // Inf should be rejected.
        let inf_vec = vec![f32::INFINITY; 8];
        assert!(idx.insert(101, &inf_vec).is_err());

        // Negative Inf should be rejected.
        let neg_inf_vec = vec![f32::NEG_INFINITY; 8];
        assert!(idx.insert(102, &neg_inf_vec).is_err());

        // Valid vectors should still work.
        idx.insert(0, &training[0].1).unwrap();
        assert_eq!(idx.config().num_vectors, 1);
    }
    write_txn.commit().unwrap();
}
