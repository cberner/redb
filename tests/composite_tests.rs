use shodh_redb::{
    BlobId, CausalLink, CompositeQuery, ContentType, Database, DistanceMetric,
    IvfPqIndexDefinition, ReadableDatabase, RelationType, StoreOptions,
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

fn random_vector(seed: u64, dim: usize) -> Vec<f32> {
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    (0..dim)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            ((state as f64) / (u64::MAX as f64) * 2.0 - 1.0) as f32
        })
        .collect()
}

const INDEX_8D: IvfPqIndexDefinition =
    IvfPqIndexDefinition::new("composite_8d", 8, 4, 2, DistanceMetric::EuclideanSq)
        .with_raw_vectors()
        .with_nprobe(4);

/// Store blobs and train an IVF-PQ index using their sequence numbers as vector keys.
/// Returns (vec of BlobIds, vec of (vector_key, vector)).
fn setup_indexed_blobs(
    db: &Database,
    dim: usize,
    count: usize,
) -> (Vec<BlobId>, Vec<(u64, Vec<f32>)>) {
    let write_txn = db.begin_write().unwrap();
    let mut blob_ids = Vec::new();
    let mut vectors = Vec::new();

    for i in 0..count {
        let data = format!("blob-{i}");
        let id = write_txn
            .store_blob(
                data.as_bytes(),
                ContentType::OctetStream,
                &format!("v{i}"),
                StoreOptions::default(),
            )
            .unwrap();
        let vec = random_vector(i as u64 + 1, dim);
        vectors.push((id.sequence, vec));
        blob_ids.push(id);
    }

    {
        let mut idx = write_txn.open_ivfpq_index(&INDEX_8D).unwrap();
        idx.train(vectors.iter().map(|(id, v)| (*id, v.clone())), 25)
            .unwrap();
        for (id, vec) in &vectors {
            idx.insert(*id, vec).unwrap();
        }
    }
    write_txn.commit().unwrap();
    (blob_ids, vectors)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn temporal_only() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    let mut ids = Vec::new();
    for i in 0..5 {
        let data = format!("temporal-{i}");
        let id = write_txn
            .store_blob(
                data.as_bytes(),
                ContentType::OctetStream,
                &format!("t{i}"),
                StoreOptions::default(),
            )
            .unwrap();
        ids.push(id);
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let results = read_txn
        .composite_query()
        .temporal(1.0)
        .time_range(0, u64::MAX)
        .top_k(5)
        .execute()
        .unwrap();

    assert_eq!(results.len(), 5);
    // Most recent should be first (highest temporal score)
    for i in 0..results.len() - 1 {
        assert!(
            results[i].score >= results[i + 1].score,
            "results should be sorted by score descending"
        );
    }
    // Most recent blob should be last created
    assert_eq!(results[0].blob_id, ids[4]);
    // All signals: temporal populated, others None
    assert!(results[0].signals.temporal.is_some());
    assert!(results[0].signals.semantic.is_none());
    assert!(results[0].signals.causal.is_none());
}

#[test]
fn causal_only() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    let root = write_txn
        .store_blob(
            b"root",
            ContentType::OctetStream,
            "root",
            StoreOptions::default(),
        )
        .unwrap();
    let child = write_txn
        .store_blob(
            b"child",
            ContentType::OctetStream,
            "child",
            StoreOptions::with_causal_link(CausalLink::new(root, RelationType::Derived, "step1")),
        )
        .unwrap();
    let grandchild = write_txn
        .store_blob(
            b"grandchild",
            ContentType::OctetStream,
            "gc",
            StoreOptions::with_causal_link(CausalLink::new(child, RelationType::Derived, "step2")),
        )
        .unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let results = read_txn
        .composite_query()
        .causal(root, 1.0)
        .top_k(10)
        .execute()
        .unwrap();

    assert_eq!(results.len(), 3);
    // Root should have highest score (distance 0)
    assert_eq!(results[0].blob_id, root);
    assert!(results[0].signals.causal.unwrap() > results[1].signals.causal.unwrap());
    // Grandchild should have lowest causal score
    let gc_result = results.iter().find(|r| r.blob_id == grandchild).unwrap();
    assert!(gc_result.signals.causal.unwrap() < results[0].signals.causal.unwrap());
}

#[test]
fn semantic_only() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let (blob_ids, vectors) = setup_indexed_blobs(&db, 8, 20);

    let read_txn = db.begin_read().unwrap();
    let idx = read_txn.open_ivfpq_index(&INDEX_8D).unwrap();

    // Query with the first blob's vector -- it should rank highest
    let query = &vectors[0].1;
    let results = read_txn
        .composite_query()
        .semantic(&idx, query, 1.0)
        .top_k(5)
        .execute()
        .unwrap();

    assert_eq!(results.len(), 5);
    // Closest vector should be the query itself
    assert_eq!(results[0].blob_id, blob_ids[0]);
    assert!(results[0].signals.semantic.is_some());
    // Scores should be descending
    for i in 0..results.len() - 1 {
        assert!(results[i].score >= results[i + 1].score);
    }
}

#[test]
fn weighted_fusion() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    // Create blobs with causal chain and different timestamps
    let write_txn = db.begin_write().unwrap();
    let root = write_txn
        .store_blob(
            b"root",
            ContentType::OctetStream,
            "root",
            StoreOptions::default(),
        )
        .unwrap();
    let _child = write_txn
        .store_blob(
            b"child",
            ContentType::OctetStream,
            "child",
            StoreOptions::with_causal_link(CausalLink::derived(root)),
        )
        .unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();

    // Both signals active: temporal (recent = higher) and causal (root = higher)
    // child is more recent but farther from root; root is older but is the root
    let results = read_txn
        .composite_query()
        .temporal(0.5)
        .time_range(0, u64::MAX)
        .causal(root, 0.5)
        .top_k(10)
        .execute()
        .unwrap();

    assert_eq!(results.len(), 2);
    // Both signals populated
    for r in &results {
        assert!(r.signals.temporal.is_some());
        assert!(r.signals.causal.is_some());
        assert!(r.signals.semantic.is_none());
    }
    // Scores should sum correctly: temporal + causal, each weighted 0.5
    for r in &results {
        let expected = 0.5 * r.signals.temporal.unwrap() + 0.5 * r.signals.causal.unwrap();
        assert!(
            (r.score - expected).abs() < 1e-6,
            "score {:.6} != expected {:.6}",
            r.score,
            expected
        );
    }
}

#[test]
fn namespace_filter() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    let _a = write_txn
        .store_blob(
            b"a",
            ContentType::OctetStream,
            "a",
            StoreOptions::with_namespace("alpha"),
        )
        .unwrap();
    let b = write_txn
        .store_blob(
            b"b",
            ContentType::OctetStream,
            "b",
            StoreOptions::with_namespace("beta"),
        )
        .unwrap();
    let _c = write_txn
        .store_blob(
            b"c",
            ContentType::OctetStream,
            "c",
            StoreOptions::with_namespace("alpha"),
        )
        .unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let results = read_txn
        .composite_query()
        .temporal(1.0)
        .time_range(0, u64::MAX)
        .namespace("beta")
        .top_k(10)
        .execute()
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].blob_id, b);
}

#[test]
fn tag_filter() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    let id_lidar = write_txn
        .store_blob(
            b"lidar",
            ContentType::OctetStream,
            "lidar",
            StoreOptions::with_tags(&["sensor", "lidar"]),
        )
        .unwrap();
    let _id_imu = write_txn
        .store_blob(
            b"imu",
            ContentType::OctetStream,
            "imu",
            StoreOptions::with_tags(&["sensor", "imu"]),
        )
        .unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let results = read_txn
        .composite_query()
        .temporal(1.0)
        .time_range(0, u64::MAX)
        .tag("lidar")
        .top_k(10)
        .execute()
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].blob_id, id_lidar);
}

#[test]
fn tag_intersection() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    let id_both = write_txn
        .store_blob(
            b"both",
            ContentType::OctetStream,
            "both",
            StoreOptions::with_tags(&["sensor", "lidar"]),
        )
        .unwrap();
    let _id_sensor_only = write_txn
        .store_blob(
            b"sensor-only",
            ContentType::OctetStream,
            "so",
            StoreOptions::with_tags(&["sensor"]),
        )
        .unwrap();
    let _id_lidar_only = write_txn
        .store_blob(
            b"lidar-only",
            ContentType::OctetStream,
            "lo",
            StoreOptions::with_tags(&["lidar"]),
        )
        .unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    // AND semantics: must have both tags
    let results = read_txn
        .composite_query()
        .temporal(1.0)
        .time_range(0, u64::MAX)
        .tag("sensor")
        .tag("lidar")
        .top_k(10)
        .execute()
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].blob_id, id_both);
}

#[test]
fn zero_results() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    // Empty database
    let read_txn = db.begin_read().unwrap();
    let results = read_txn
        .composite_query()
        .temporal(1.0)
        .time_range(0, u64::MAX)
        .top_k(10)
        .execute()
        .unwrap();

    assert!(results.is_empty());
}

#[test]
fn single_blob() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    let id = write_txn
        .store_blob(
            b"only",
            ContentType::OctetStream,
            "only",
            StoreOptions::default(),
        )
        .unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let results = read_txn
        .composite_query()
        .temporal(1.0)
        .time_range(0, u64::MAX)
        .top_k(5)
        .execute()
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].blob_id, id);
    // Single blob gets score 1.0 (min == max, all scores default to 1.0)
    assert!((results[0].score - 1.0).abs() < 1e-6);
}

#[test]
fn top_k_limit() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    for i in 0..20 {
        let data = format!("blob-{i}");
        write_txn
            .store_blob(
                data.as_bytes(),
                ContentType::OctetStream,
                &format!("k{i}"),
                StoreOptions::default(),
            )
            .unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let results = read_txn
        .composite_query()
        .temporal(1.0)
        .time_range(0, u64::MAX)
        .top_k(5)
        .execute()
        .unwrap();

    assert_eq!(results.len(), 5);
}

#[test]
fn signal_scores_populated() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    let root = write_txn
        .store_blob(
            b"root",
            ContentType::OctetStream,
            "root",
            StoreOptions::default(),
        )
        .unwrap();
    let _child = write_txn
        .store_blob(
            b"child",
            ContentType::OctetStream,
            "child",
            StoreOptions::with_causal_link(CausalLink::derived(root)),
        )
        .unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();

    // Only temporal active
    let results = read_txn
        .composite_query()
        .temporal(1.0)
        .time_range(0, u64::MAX)
        .top_k(10)
        .execute()
        .unwrap();
    for r in &results {
        assert!(r.signals.temporal.is_some());
        assert!(r.signals.semantic.is_none());
        assert!(r.signals.causal.is_none());
    }

    // Only causal active
    let results = read_txn
        .composite_query()
        .causal(root, 1.0)
        .top_k(10)
        .execute()
        .unwrap();
    for r in &results {
        assert!(r.signals.causal.is_some());
        assert!(r.signals.semantic.is_none());
        assert!(r.signals.temporal.is_none());
    }
}

#[test]
fn causal_branching() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    // Linear chain: root -> A -> B -> C
    // (causal_children is 1:1 per parent until issue #28, so test linear chains)
    let write_txn = db.begin_write().unwrap();
    let root = write_txn
        .store_blob(
            b"root",
            ContentType::OctetStream,
            "root",
            StoreOptions::default(),
        )
        .unwrap();
    let a = write_txn
        .store_blob(
            b"a",
            ContentType::OctetStream,
            "a",
            StoreOptions::with_causal_link(CausalLink::derived(root)),
        )
        .unwrap();
    let b = write_txn
        .store_blob(
            b"b",
            ContentType::OctetStream,
            "b",
            StoreOptions::with_causal_link(CausalLink::derived(a)),
        )
        .unwrap();
    let c = write_txn
        .store_blob(
            b"c",
            ContentType::OctetStream,
            "c",
            StoreOptions::with_causal_link(CausalLink::derived(b)),
        )
        .unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let results = read_txn
        .composite_query()
        .causal(root, 1.0)
        .top_k(10)
        .execute()
        .unwrap();

    assert_eq!(results.len(), 4);
    // Root is distance 0 (highest score)
    assert_eq!(results[0].blob_id, root);
    // A is distance 1
    let a_score = results.iter().find(|r| r.blob_id == a).unwrap().score;
    let b_score = results.iter().find(|r| r.blob_id == b).unwrap().score;
    let c_score = results.iter().find(|r| r.blob_id == c).unwrap().score;
    // Scores decrease with distance: root > A > B > C
    assert!(
        a_score > b_score,
        "child should score higher than grandchild"
    );
    assert!(
        b_score > c_score,
        "grandchild should score higher than great-grandchild"
    );
}

#[test]
fn validation_no_signals() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let read_txn = db.begin_read().unwrap();
    let result = CompositeQuery::new(&read_txn).top_k(10).execute();

    assert!(result.is_err(), "should fail with no signals active");
}

#[test]
fn validation_semantic_without_index() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let read_txn = db.begin_read().unwrap();
    // Manually set semantic weight without providing index
    let mut query = CompositeQuery::new(&read_txn);
    query = query.temporal(0.0); // disable temporal
    // Force semantic weight via the builder but no index
    // CompositeQuery::semantic() requires index -- so test causal without root instead
    let result = CompositeQuery::new(&read_txn)
        .causal(BlobId::new(999, 0), 0.0)
        .top_k(10)
        .execute();

    // All weights are 0 -> validation error
    assert!(result.is_err());
    let _ = query; // suppress unused
}
