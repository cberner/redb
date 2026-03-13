use redb::{
    BlobId, CausalLink, ContentType, Database, ReadableDatabase, RelationType, StorageError,
    StoreOptions,
};
use std::io::{Read, Seek, SeekFrom};

fn create_tempfile() -> tempfile::NamedTempFile {
    if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/tmp").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    }
}

#[test]
fn store_and_get_blob() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let data = b"Hello, blob store!";
    let blob_id;

    {
        let write_txn = db.begin_write().unwrap();
        blob_id = write_txn
            .store_blob(
                data,
                ContentType::OctetStream,
                "test-blob",
                StoreOptions::default(),
            )
            .unwrap();
        write_txn.commit().unwrap();
    }

    // Read back via ReadTransaction
    let read_txn = db.begin_read().unwrap();
    let (retrieved_data, meta) = read_txn.get_blob(&blob_id).unwrap().unwrap();

    assert_eq!(retrieved_data, data);
    assert_eq!(meta.blob_ref.length, data.len() as u64);
    assert_eq!(
        ContentType::from_byte(meta.blob_ref.content_type),
        ContentType::OctetStream
    );
    assert_eq!(meta.label_str(), "test-blob");
    assert!(meta.causal_parent.is_none());
}

#[test]
fn store_blob_in_write_txn_and_read_back() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let data = b"read within same write txn";

    let write_txn = db.begin_write().unwrap();
    let blob_id = write_txn
        .store_blob(
            data,
            ContentType::Metadata,
            "inline-read",
            StoreOptions::default(),
        )
        .unwrap();

    // Read back within the same write transaction
    let (retrieved_data, meta) = write_txn.get_blob(&blob_id).unwrap().unwrap();
    assert_eq!(retrieved_data, data.as_slice());
    assert_eq!(meta.label_str(), "inline-read");

    write_txn.commit().unwrap();
}

#[test]
fn get_blob_meta_only() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let data = vec![0u8; 1024];
    let blob_id;

    {
        let write_txn = db.begin_write().unwrap();
        blob_id = write_txn
            .store_blob(
                &data,
                ContentType::ImagePng,
                "image",
                StoreOptions::default(),
            )
            .unwrap();
        write_txn.commit().unwrap();
    }

    let read_txn = db.begin_read().unwrap();
    let meta = read_txn.get_blob_meta(&blob_id).unwrap().unwrap();
    assert_eq!(meta.blob_ref.length, 1024);
    assert_eq!(
        ContentType::from_byte(meta.blob_ref.content_type),
        ContentType::ImagePng
    );
}

#[test]
fn get_nonexistent_blob() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    // Store one blob so system tables exist
    {
        let write_txn = db.begin_write().unwrap();
        write_txn
            .store_blob(b"x", ContentType::OctetStream, "", StoreOptions::default())
            .unwrap();
        write_txn.commit().unwrap();
    }

    let read_txn = db.begin_read().unwrap();
    let fake_id = BlobId::new(99999, 0);
    assert!(read_txn.get_blob(&fake_id).unwrap().is_none());
    assert!(read_txn.get_blob_meta(&fake_id).unwrap().is_none());
}

#[test]
fn get_blob_no_system_tables() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    // No blobs stored, system tables don't exist
    let read_txn = db.begin_read().unwrap();
    let fake_id = BlobId::new(0, 0);
    assert!(read_txn.get_blob(&fake_id).unwrap().is_none());
    assert!(read_txn.get_blob_meta(&fake_id).unwrap().is_none());
}

#[test]
fn multiple_blobs_sequential() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    let id1 = write_txn
        .store_blob(
            b"first",
            ContentType::OctetStream,
            "a",
            StoreOptions::default(),
        )
        .unwrap();
    let id2 = write_txn
        .store_blob(
            b"second",
            ContentType::AudioWav,
            "b",
            StoreOptions::default(),
        )
        .unwrap();
    let id3 = write_txn
        .store_blob(
            b"third",
            ContentType::VideoMp4,
            "c",
            StoreOptions::default(),
        )
        .unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let (d1, _) = read_txn.get_blob(&id1).unwrap().unwrap();
    let (d2, _) = read_txn.get_blob(&id2).unwrap().unwrap();
    let (d3, _) = read_txn.get_blob(&id3).unwrap().unwrap();
    assert_eq!(d1, b"first");
    assert_eq!(d2, b"second");
    assert_eq!(d3, b"third");

    // Sequence numbers should be monotonic
    assert!(id1.sequence < id2.sequence);
    assert!(id2.sequence < id3.sequence);
}

#[test]
fn delete_blob() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let blob_id;
    {
        let write_txn = db.begin_write().unwrap();
        blob_id = write_txn
            .store_blob(
                b"to-delete",
                ContentType::OctetStream,
                "del",
                StoreOptions::default(),
            )
            .unwrap();
        write_txn.commit().unwrap();
    }

    {
        let write_txn = db.begin_write().unwrap();
        assert!(write_txn.delete_blob(&blob_id).unwrap());
        // Double delete returns false
        assert!(!write_txn.delete_blob(&blob_id).unwrap());
        write_txn.commit().unwrap();
    }

    let read_txn = db.begin_read().unwrap();
    assert!(read_txn.get_blob(&blob_id).unwrap().is_none());
}

#[test]
fn blob_survives_reopen() {
    let tmpfile = create_tempfile();
    let blob_id;

    {
        let db = Database::create(tmpfile.path()).unwrap();
        let write_txn = db.begin_write().unwrap();
        blob_id = write_txn
            .store_blob(
                b"persistent",
                ContentType::Embedding,
                "embed",
                StoreOptions::default(),
            )
            .unwrap();
        write_txn.commit().unwrap();
    }

    // Reopen
    let db = Database::create(tmpfile.path()).unwrap();
    let read_txn = db.begin_read().unwrap();
    let (data, meta) = read_txn.get_blob(&blob_id).unwrap().unwrap();
    assert_eq!(data, b"persistent");
    assert_eq!(meta.label_str(), "embed");
}

#[test]
fn blob_abort_invisible() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    // Store a committed blob first
    {
        let write_txn = db.begin_write().unwrap();
        write_txn
            .store_blob(
                b"committed",
                ContentType::OctetStream,
                "ok",
                StoreOptions::default(),
            )
            .unwrap();
        write_txn.commit().unwrap();
    }

    // Store but abort
    let aborted_id;
    {
        let write_txn = db.begin_write().unwrap();
        aborted_id = write_txn
            .store_blob(
                b"aborted",
                ContentType::OctetStream,
                "nope",
                StoreOptions::default(),
            )
            .unwrap();
        // Drop without commit = abort
        drop(write_txn);
    }

    let read_txn = db.begin_read().unwrap();
    assert!(read_txn.get_blob(&aborted_id).unwrap().is_none());
}

#[test]
fn temporal_range_query() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    let id1 = write_txn
        .store_blob(
            b"a",
            ContentType::OctetStream,
            "t1",
            StoreOptions::default(),
        )
        .unwrap();
    let id2 = write_txn
        .store_blob(
            b"b",
            ContentType::OctetStream,
            "t2",
            StoreOptions::default(),
        )
        .unwrap();
    let id3 = write_txn
        .store_blob(
            b"c",
            ContentType::OctetStream,
            "t3",
            StoreOptions::default(),
        )
        .unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();

    // Get all blobs timestamps
    let m1 = read_txn.get_blob_meta(&id1).unwrap().unwrap();
    let m2 = read_txn.get_blob_meta(&id2).unwrap().unwrap();
    let m3 = read_txn.get_blob_meta(&id3).unwrap().unwrap();

    // Timestamps should be monotonically non-decreasing
    assert!(m1.wall_clock_ns <= m2.wall_clock_ns);
    assert!(m2.wall_clock_ns <= m3.wall_clock_ns);

    // Query the full range
    let results = read_txn
        .blobs_in_time_range(m1.wall_clock_ns, m3.wall_clock_ns)
        .unwrap();
    assert_eq!(results.len(), 3);

    // Results should be in temporal order
    for i in 0..results.len() - 1 {
        assert!(results[i].0.wall_clock_ns <= results[i + 1].0.wall_clock_ns);
    }
}

#[test]
fn temporal_range_empty() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    // No blobs stored
    let read_txn = db.begin_read().unwrap();
    let results = read_txn.blobs_in_time_range(0, u64::MAX).unwrap();
    assert!(results.is_empty());
}

#[test]
fn blobs_near() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    let id1 = write_txn
        .store_blob(
            b"sensor1",
            ContentType::SensorImu,
            "imu",
            StoreOptions::default(),
        )
        .unwrap();
    write_txn
        .store_blob(
            b"sensor2",
            ContentType::ImageJpeg,
            "cam",
            StoreOptions::default(),
        )
        .unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    // Use a very wide window to ensure both blobs are found
    let results = read_txn.blobs_near(&id1, 10_000_000_000).unwrap();
    assert!(results.len() >= 2);
}

#[test]
fn causal_chain() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();

    // Build a chain: root -> child -> grandchild
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
            StoreOptions::with_causal_link(CausalLink::new(
                root,
                RelationType::Derived,
                "processed",
            )),
        )
        .unwrap();
    let grandchild = write_txn
        .store_blob(
            b"grandchild",
            ContentType::OctetStream,
            "grandchild",
            StoreOptions::with_causal_link(CausalLink::new(
                child,
                RelationType::Supports,
                "evidence",
            )),
        )
        .unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();

    // Traverse from grandchild backwards
    let chain = read_txn.causal_chain(&grandchild, 10).unwrap();
    assert_eq!(chain.len(), 3);
    assert_eq!(chain[0].0, grandchild);
    assert_eq!(chain[1].0, child);
    assert_eq!(chain[2].0, root);

    // Edge metadata should be present
    let edge_gc = chain[0].2.as_ref().unwrap();
    assert_eq!(edge_gc.relation, RelationType::Supports);
    assert_eq!(edge_gc.context_str(), "evidence");

    let edge_c = chain[1].2.as_ref().unwrap();
    assert_eq!(edge_c.relation, RelationType::Derived);
    assert_eq!(edge_c.context_str(), "processed");

    // Root has no incoming edge
    assert!(chain[2].2.is_none());

    // Max hops = 1: only gets grandchild + child
    let short_chain = read_txn.causal_chain(&grandchild, 1).unwrap();
    assert_eq!(short_chain.len(), 2);
    assert_eq!(short_chain[0].0, grandchild);
    assert_eq!(short_chain[1].0, child);
}

#[test]
fn causal_children() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    let parent = write_txn
        .store_blob(
            b"parent",
            ContentType::OctetStream,
            "p",
            StoreOptions::default(),
        )
        .unwrap();
    let child = write_txn
        .store_blob(
            b"child",
            ContentType::OctetStream,
            "c",
            StoreOptions::with_causal_link(CausalLink::new(
                parent,
                RelationType::Contradicts,
                "revised output",
            )),
        )
        .unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let children = read_txn.causal_children(&parent).unwrap();
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].child, child);
    assert_eq!(children[0].relation, RelationType::Contradicts);
    assert_eq!(children[0].context_str(), "revised output");

    // No children for the child
    let grandchildren = read_txn.causal_children(&child).unwrap();
    assert!(grandchildren.is_empty());
}

#[test]
fn causal_path_found() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    let a = write_txn
        .store_blob(b"a", ContentType::OctetStream, "a", StoreOptions::default())
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

    // Path from a to c
    let path = read_txn.causal_path(&a, &c, 10).unwrap().unwrap();
    assert_eq!(path.len(), 3);
    assert_eq!(path[0].0, a);
    assert!(path[0].1.is_none()); // from endpoint has no incoming edge
    assert_eq!(path[1].0, b);
    assert!(path[1].1.is_some());
    assert_eq!(path[2].0, c);
    assert!(path[2].1.is_some());

    // Path from a to a (trivial)
    let self_path = read_txn.causal_path(&a, &a, 10).unwrap().unwrap();
    assert_eq!(self_path.len(), 1);
    assert_eq!(self_path[0].0, a);
}

#[test]
fn causal_path_not_found() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    let a = write_txn
        .store_blob(b"a", ContentType::OctetStream, "a", StoreOptions::default())
        .unwrap();
    let b = write_txn
        .store_blob(b"b", ContentType::OctetStream, "b", StoreOptions::default())
        .unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    // No causal link between a and b
    assert!(read_txn.causal_path(&a, &b, 10).unwrap().is_none());
}

#[test]
fn large_blob() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    // 1MB blob
    let data: Vec<u8> = (0..1_048_576).map(|i| (i % 256) as u8).collect();

    let blob_id;
    {
        let write_txn = db.begin_write().unwrap();
        blob_id = write_txn
            .store_blob(
                &data,
                ContentType::PointCloudLas,
                "lidar",
                StoreOptions::default(),
            )
            .unwrap();
        write_txn.commit().unwrap();
    }

    let read_txn = db.begin_read().unwrap();
    let (retrieved, meta) = read_txn.get_blob(&blob_id).unwrap().unwrap();
    assert_eq!(retrieved.len(), 1_048_576);
    assert_eq!(retrieved, data);
    assert_eq!(
        ContentType::from_byte(meta.blob_ref.content_type),
        ContentType::PointCloudLas
    );
}

#[test]
fn hlc_monotonicity_across_blobs() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    let mut ids = Vec::new();
    for i in 0..10 {
        let id = write_txn
            .store_blob(
                format!("blob-{i}").as_bytes(),
                ContentType::OctetStream,
                &format!("b{i}"),
                StoreOptions::default(),
            )
            .unwrap();
        ids.push(id);
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let mut prev_hlc = 0u64;
    for id in &ids {
        let meta = read_txn.get_blob_meta(id).unwrap().unwrap();
        assert!(meta.hlc > prev_hlc, "HLC must be strictly monotonic");
        prev_hlc = meta.hlc;
    }
}

#[test]
fn blob_checksum_stored() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let data = b"checksum test data";
    let blob_id;
    {
        let write_txn = db.begin_write().unwrap();
        blob_id = write_txn
            .store_blob(
                data,
                ContentType::OctetStream,
                "ck",
                StoreOptions::default(),
            )
            .unwrap();
        write_txn.commit().unwrap();
    }

    let read_txn = db.begin_read().unwrap();
    let meta = read_txn.get_blob_meta(&blob_id).unwrap().unwrap();
    // Checksum should be non-zero for non-empty data
    assert_ne!(meta.blob_ref.checksum, 0);
}

#[test]
fn content_type_variants() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let types = [
        ContentType::OctetStream,
        ContentType::ImagePng,
        ContentType::ImageJpeg,
        ContentType::AudioWav,
        ContentType::AudioOgg,
        ContentType::VideoMp4,
        ContentType::PointCloudLas,
        ContentType::SensorImu,
        ContentType::Embedding,
        ContentType::Metadata,
    ];

    let write_txn = db.begin_write().unwrap();
    let ids: Vec<_> = types
        .iter()
        .enumerate()
        .map(|(i, ct)| {
            write_txn
                .store_blob(
                    format!("data-{i}").as_bytes(),
                    *ct,
                    &format!("ct-{i}"),
                    StoreOptions::default(),
                )
                .unwrap()
        })
        .collect();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    for (i, id) in ids.iter().enumerate() {
        let meta = read_txn.get_blob_meta(id).unwrap().unwrap();
        assert_eq!(ContentType::from_byte(meta.blob_ref.content_type), types[i]);
    }
}

#[test]
fn blob_label_truncation() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    // Label longer than 63 bytes should be truncated
    let long_label = "a".repeat(100);

    let write_txn = db.begin_write().unwrap();
    let blob_id = write_txn
        .store_blob(
            b"x",
            ContentType::OctetStream,
            &long_label,
            StoreOptions::default(),
        )
        .unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let meta = read_txn.get_blob_meta(&blob_id).unwrap().unwrap();
    assert_eq!(meta.label_str().len(), 63);
    assert_eq!(meta.label_str(), &long_label[..63]);
}

#[test]
fn multiple_transactions_blob_state() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    // First transaction
    let id1;
    {
        let write_txn = db.begin_write().unwrap();
        id1 = write_txn
            .store_blob(
                b"first-txn",
                ContentType::OctetStream,
                "t1",
                StoreOptions::default(),
            )
            .unwrap();
        write_txn.commit().unwrap();
    }

    // Second transaction
    let id2;
    {
        let write_txn = db.begin_write().unwrap();
        id2 = write_txn
            .store_blob(
                b"second-txn",
                ContentType::OctetStream,
                "t2",
                StoreOptions::default(),
            )
            .unwrap();
        write_txn.commit().unwrap();
    }

    // Both should be readable
    let read_txn = db.begin_read().unwrap();
    let (d1, _) = read_txn.get_blob(&id1).unwrap().unwrap();
    let (d2, _) = read_txn.get_blob(&id2).unwrap().unwrap();
    assert_eq!(d1, b"first-txn");
    assert_eq!(d2, b"second-txn");

    // Sequence numbers across transactions should be monotonic
    assert!(id1.sequence < id2.sequence);
}

// ---------------------------------------------------------------------------
// Streaming blob writer tests
// ---------------------------------------------------------------------------

#[test]
fn streaming_blob_basic() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let data = b"hello streaming world!";
    let write_txn = db.begin_write().unwrap();
    let blob_id = {
        let mut writer = write_txn
            .blob_writer(ContentType::OctetStream, "basic", StoreOptions::default())
            .unwrap();
        writer.write(&data[..5]).unwrap();
        writer.write(&data[5..14]).unwrap();
        writer.write(&data[14..]).unwrap();
        writer.finish().unwrap()
    };
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let (read_data, meta) = read_txn.get_blob(&blob_id).unwrap().unwrap();
    assert_eq!(read_data, data);
    assert_eq!(meta.blob_ref.length, data.len() as u64);
}

#[test]
fn streaming_blob_large() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    // 4 MB in 64 KB chunks — exercises the streaming hasher's large mode
    let total = 4 * 1024 * 1024;
    let chunk_size = 64 * 1024;
    let full_data: Vec<u8> = (0..total).map(|i| (i % 251) as u8).collect();

    let write_txn = db.begin_write().unwrap();
    let blob_id = {
        let mut writer = write_txn
            .blob_writer(ContentType::PointCloudLas, "lidar", StoreOptions::default())
            .unwrap();
        for chunk in full_data.chunks(chunk_size) {
            writer.write(chunk).unwrap();
        }
        writer.finish().unwrap()
    };
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let (read_data, meta) = read_txn.get_blob(&blob_id).unwrap().unwrap();
    assert_eq!(read_data.len(), total);
    assert_eq!(read_data, full_data);
    assert_eq!(meta.blob_ref.length, total as u64);

    // The checksum should match what store_blob would compute
    // (implicitly verified by get_blob's checksum validation)
}

#[test]
fn streaming_blob_small() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    // < 240 bytes: exercises the streaming hasher's small/one-shot path
    let data = b"tiny";
    let write_txn = db.begin_write().unwrap();
    let blob_id = {
        let mut writer = write_txn
            .blob_writer(ContentType::Metadata, "small", StoreOptions::default())
            .unwrap();
        writer.write(data).unwrap();
        writer.finish().unwrap()
    };
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let (read_data, _) = read_txn.get_blob(&blob_id).unwrap().unwrap();
    assert_eq!(read_data, data);
}

#[test]
fn streaming_blob_io_write_trait() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let data = b"data via std::io::Write trait";
    let write_txn = db.begin_write().unwrap();
    let blob_id = {
        let mut writer = write_txn
            .blob_writer(
                ContentType::OctetStream,
                "io_write",
                StoreOptions::default(),
            )
            .unwrap();
        // Use std::io::Write::write_all
        std::io::Write::write_all(&mut writer, data).unwrap();
        writer.finish().unwrap()
    };
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let (read_data, _) = read_txn.get_blob(&blob_id).unwrap().unwrap();
    assert_eq!(read_data, data);
}

#[test]
fn streaming_blob_abort() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    // Write some data then drop without finish
    let write_txn = db.begin_write().unwrap();
    {
        let mut writer = write_txn
            .blob_writer(ContentType::OctetStream, "aborted", StoreOptions::default())
            .unwrap();
        writer.write(b"partial data").unwrap();
        // drop without finish
    }
    // After drop, we should be able to create a new writer
    let blob_id = {
        let mut writer = write_txn
            .blob_writer(ContentType::OctetStream, "real", StoreOptions::default())
            .unwrap();
        writer.write(b"actual data").unwrap();
        writer.finish().unwrap()
    };
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let (data, _) = read_txn.get_blob(&blob_id).unwrap().unwrap();
    assert_eq!(data, b"actual data");
}

#[test]
fn streaming_blob_mixed_with_store_blob() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();

    // store_blob first
    let id1 = write_txn
        .store_blob(
            b"one-shot",
            ContentType::OctetStream,
            "first",
            StoreOptions::default(),
        )
        .unwrap();

    // Then streaming
    let id2 = {
        let mut writer = write_txn
            .blob_writer(
                ContentType::OctetStream,
                "streaming",
                StoreOptions::default(),
            )
            .unwrap();
        writer.write(b"streamed").unwrap();
        writer.finish().unwrap()
    };

    // Then store_blob again
    let id3 = write_txn
        .store_blob(
            b"another",
            ContentType::OctetStream,
            "third",
            StoreOptions::default(),
        )
        .unwrap();

    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let (d1, _) = read_txn.get_blob(&id1).unwrap().unwrap();
    let (d2, _) = read_txn.get_blob(&id2).unwrap().unwrap();
    let (d3, _) = read_txn.get_blob(&id3).unwrap().unwrap();
    assert_eq!(d1, b"one-shot");
    assert_eq!(d2, b"streamed");
    assert_eq!(d3, b"another");

    // Sequence numbers should be monotonically increasing
    assert!(id1.sequence < id2.sequence);
    assert!(id2.sequence < id3.sequence);
}

#[test]
fn streaming_blob_empty() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    let blob_id = {
        let writer = write_txn
            .blob_writer(ContentType::OctetStream, "empty", StoreOptions::default())
            .unwrap();
        // finish immediately without writing anything
        writer.finish().unwrap()
    };
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let (data, meta) = read_txn.get_blob(&blob_id).unwrap().unwrap();
    assert!(data.is_empty());
    assert_eq!(meta.blob_ref.length, 0);
}

#[test]
fn streaming_blob_survives_reopen() {
    let tmpfile = create_tempfile();

    let blob_id;
    let data = b"persistent streaming blob";
    {
        let db = Database::create(tmpfile.path()).unwrap();
        let write_txn = db.begin_write().unwrap();
        blob_id = {
            let mut writer = write_txn
                .blob_writer(ContentType::ImagePng, "persistent", StoreOptions::default())
                .unwrap();
            writer.write(data).unwrap();
            writer.finish().unwrap()
        };
        write_txn.commit().unwrap();
    }

    // Reopen and verify
    {
        let db = Database::create(tmpfile.path()).unwrap();
        let read_txn = db.begin_read().unwrap();
        let (read_data, _) = read_txn.get_blob(&blob_id).unwrap().unwrap();
        assert_eq!(read_data, data);
    }
}

#[test]
fn streaming_blob_concurrent_writer_rejected() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    let mut writer = write_txn
        .blob_writer(ContentType::OctetStream, "first", StoreOptions::default())
        .unwrap();
    writer.write(b"data").unwrap();

    // Attempting a second writer should fail
    {
        let result =
            write_txn.blob_writer(ContentType::OctetStream, "second", StoreOptions::default());
        assert!(matches!(result, Err(StorageError::BlobWriterActive)));
    }

    // store_blob should also fail while writer is active
    {
        let result = write_txn.store_blob(
            b"data",
            ContentType::OctetStream,
            "blocked",
            StoreOptions::default(),
        );
        assert!(matches!(result, Err(StorageError::BlobWriterActive)));
    }

    // After finishing the writer, operations should succeed
    writer.finish().unwrap();
    let _id = write_txn
        .store_blob(
            b"ok",
            ContentType::OctetStream,
            "unblocked",
            StoreOptions::default(),
        )
        .unwrap();
    write_txn.commit().unwrap();
}

#[test]
fn streaming_blob_checksum_matches_oneshot() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let data: Vec<u8> = (0..10_000).map(|i| (i % 251) as u8).collect();

    let write_txn = db.begin_write().unwrap();

    // Store via one-shot
    let id_oneshot = write_txn
        .store_blob(
            &data,
            ContentType::OctetStream,
            "oneshot",
            StoreOptions::default(),
        )
        .unwrap();

    // Store via streaming (byte-at-a-time for maximum stress)
    let id_streaming = {
        let mut writer = write_txn
            .blob_writer(
                ContentType::OctetStream,
                "streaming",
                StoreOptions::default(),
            )
            .unwrap();
        for byte in &data {
            writer.write(std::slice::from_ref(byte)).unwrap();
        }
        writer.finish().unwrap()
    };

    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let (_, meta_oneshot) = read_txn.get_blob(&id_oneshot).unwrap().unwrap();
    let (_, meta_streaming) = read_txn.get_blob(&id_streaming).unwrap().unwrap();

    // Checksums should be identical
    assert_eq!(
        meta_oneshot.blob_ref.checksum,
        meta_streaming.blob_ref.checksum
    );
    // Content prefix hashes should be identical
    assert_eq!(
        id_oneshot.content_prefix_hash,
        id_streaming.content_prefix_hash
    );
}

// ---------------------------------------------------------------------------
// Causal edge metadata (Cmeta) tests
// ---------------------------------------------------------------------------

#[test]
fn causal_edge_relation_types() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let relations = [
        RelationType::Derived,
        RelationType::Similar,
        RelationType::Contradicts,
        RelationType::Supports,
        RelationType::Supersedes,
    ];

    let write_txn = db.begin_write().unwrap();
    let mut pairs = Vec::new();
    for rel in &relations {
        let parent = write_txn
            .store_blob(
                b"parent",
                ContentType::OctetStream,
                "p",
                StoreOptions::default(),
            )
            .unwrap();
        let child = write_txn
            .store_blob(
                b"child",
                ContentType::OctetStream,
                "c",
                StoreOptions::with_causal_link(CausalLink::new(parent, *rel, rel.label())),
            )
            .unwrap();
        pairs.push((parent, child, *rel));
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    for (parent, child, expected_rel) in &pairs {
        let edges = read_txn.causal_children(parent).unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].child, *child);
        assert_eq!(edges[0].relation, *expected_rel);
        assert_eq!(edges[0].context_str(), expected_rel.label());
    }
}

#[test]
fn causal_edge_context_string() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();

    // Context up to 62 bytes should be preserved
    let ctx = "inference result contradicts calibration baseline v2.3";
    let parent = write_txn
        .store_blob(b"p", ContentType::OctetStream, "p", StoreOptions::default())
        .unwrap();
    let _child = write_txn
        .store_blob(
            b"c",
            ContentType::OctetStream,
            "c",
            StoreOptions::with_causal_link(CausalLink::new(parent, RelationType::Contradicts, ctx)),
        )
        .unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let edges = read_txn.causal_children(&parent).unwrap();
    assert_eq!(edges[0].context_str(), ctx);

    // Context > 62 bytes should be truncated
    drop(read_txn);
    let write_txn2 = db.begin_write().unwrap();
    let long_ctx = "x".repeat(200);
    let p2 = write_txn2
        .store_blob(
            b"p2",
            ContentType::OctetStream,
            "p2",
            StoreOptions::default(),
        )
        .unwrap();
    write_txn2
        .store_blob(
            b"c2",
            ContentType::OctetStream,
            "c2",
            StoreOptions::with_causal_link(CausalLink::new(p2, RelationType::Derived, &long_ctx)),
        )
        .unwrap();
    write_txn2.commit().unwrap();

    let read_txn2 = db.begin_read().unwrap();
    let edges2 = read_txn2.causal_children(&p2).unwrap();
    assert_eq!(edges2[0].context_str().len(), 62);
}

#[test]
fn causal_chain_with_mixed_relations() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();

    let a = write_txn
        .store_blob(b"a", ContentType::OctetStream, "a", StoreOptions::default())
        .unwrap();
    let b = write_txn
        .store_blob(
            b"b",
            ContentType::OctetStream,
            "b",
            StoreOptions::with_causal_link(CausalLink::new(a, RelationType::Derived, "step 1")),
        )
        .unwrap();
    let c = write_txn
        .store_blob(
            b"c",
            ContentType::OctetStream,
            "c",
            StoreOptions::with_causal_link(CausalLink::new(b, RelationType::Contradicts, "step 2")),
        )
        .unwrap();
    let d = write_txn
        .store_blob(
            b"d",
            ContentType::OctetStream,
            "d",
            StoreOptions::with_causal_link(CausalLink::new(c, RelationType::Supersedes, "step 3")),
        )
        .unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let chain = read_txn.causal_chain(&d, 10).unwrap();
    assert_eq!(chain.len(), 4);

    // d's edge: c->d with Supersedes
    let e_d = chain[0].2.as_ref().unwrap();
    assert_eq!(e_d.relation, RelationType::Supersedes);
    assert_eq!(e_d.context_str(), "step 3");

    // c's edge: b->c with Contradicts
    let e_c = chain[1].2.as_ref().unwrap();
    assert_eq!(e_c.relation, RelationType::Contradicts);
    assert_eq!(e_c.context_str(), "step 2");

    // b's edge: a->b with Derived
    let e_b = chain[2].2.as_ref().unwrap();
    assert_eq!(e_b.relation, RelationType::Derived);
    assert_eq!(e_b.context_str(), "step 1");

    // a is root, no edge
    assert!(chain[3].2.is_none());
}

#[test]
fn causal_edge_with_streaming_writer() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    let parent = write_txn
        .store_blob(
            b"parent",
            ContentType::OctetStream,
            "p",
            StoreOptions::default(),
        )
        .unwrap();

    let child = {
        let mut writer = write_txn
            .blob_writer(
                ContentType::OctetStream,
                "streamed-child",
                StoreOptions::with_causal_link(CausalLink::new(
                    parent,
                    RelationType::Similar,
                    "augmented version",
                )),
            )
            .unwrap();
        writer.write(b"streamed child data").unwrap();
        writer.finish().unwrap()
    };
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let edges = read_txn.causal_children(&parent).unwrap();
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].child, child);
    assert_eq!(edges[0].relation, RelationType::Similar);
    assert_eq!(edges[0].context_str(), "augmented version");
}

// ---------------------------------------------------------------------------
// Tag and namespace tests (#43)
// ---------------------------------------------------------------------------

#[test]
fn blob_tags_basic() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    let id = write_txn
        .store_blob(
            b"tagged data",
            ContentType::OctetStream,
            "tagged",
            StoreOptions::with_tags(&["sensor", "imu", "calibration"]),
        )
        .unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();

    // Query by tag
    let by_sensor = read_txn.blobs_by_tag("sensor").unwrap();
    assert_eq!(by_sensor.len(), 1);
    assert_eq!(by_sensor[0], id);

    let by_imu = read_txn.blobs_by_tag("imu").unwrap();
    assert_eq!(by_imu.len(), 1);

    // Nonexistent tag returns empty
    let by_missing = read_txn.blobs_by_tag("nonexistent").unwrap();
    assert!(by_missing.is_empty());

    // Read back tags
    let tags = read_txn.blob_tags(&id).unwrap();
    assert_eq!(tags.len(), 3);
    assert!(tags.contains(&"sensor".to_string()));
    assert!(tags.contains(&"imu".to_string()));
    assert!(tags.contains(&"calibration".to_string()));
}

#[test]
fn blob_tags_multiple_blobs() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    let id1 = write_txn
        .store_blob(
            b"blob1",
            ContentType::OctetStream,
            "b1",
            StoreOptions::with_tags(&["shared", "first"]),
        )
        .unwrap();
    let id2 = write_txn
        .store_blob(
            b"blob2",
            ContentType::OctetStream,
            "b2",
            StoreOptions::with_tags(&["shared", "second"]),
        )
        .unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let shared = read_txn.blobs_by_tag("shared").unwrap();
    assert_eq!(shared.len(), 2);
    assert!(shared.contains(&id1));
    assert!(shared.contains(&id2));

    let first_only = read_txn.blobs_by_tag("first").unwrap();
    assert_eq!(first_only.len(), 1);
    assert_eq!(first_only[0], id1);
}

#[test]
fn blob_tags_max_eight() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let tags: Vec<String> = (0..12).map(|i| format!("tag{i}")).collect();
    let tag_refs: Vec<&str> = tags.iter().map(|s| s.as_str()).collect();

    let write_txn = db.begin_write().unwrap();
    let id = write_txn
        .store_blob(
            b"over-tagged",
            ContentType::OctetStream,
            "many",
            StoreOptions::with_tags(&tag_refs),
        )
        .unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let stored_tags = read_txn.blob_tags(&id).unwrap();
    // Only first 8 should be stored
    assert_eq!(stored_tags.len(), 8);
    for i in 0..8 {
        assert!(stored_tags.contains(&format!("tag{i}")));
    }
    // Tags beyond 8 should not be indexed
    let tag8 = read_txn.blobs_by_tag("tag8").unwrap();
    assert!(tag8.is_empty());
}

#[test]
fn blob_tags_delete_cleanup() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let blob_id;
    {
        let write_txn = db.begin_write().unwrap();
        blob_id = write_txn
            .store_blob(
                b"deleteme",
                ContentType::OctetStream,
                "del",
                StoreOptions::with_tags(&["cleanup", "temp"]),
            )
            .unwrap();
        write_txn.commit().unwrap();
    }

    // Verify tags exist
    {
        let read_txn = db.begin_read().unwrap();
        assert_eq!(read_txn.blobs_by_tag("cleanup").unwrap().len(), 1);
    }

    // Delete the blob
    {
        let write_txn = db.begin_write().unwrap();
        assert!(write_txn.delete_blob(&blob_id).unwrap());
        write_txn.commit().unwrap();
    }

    // Tags should be cleaned up
    let read_txn = db.begin_read().unwrap();
    assert!(read_txn.blobs_by_tag("cleanup").unwrap().is_empty());
    assert!(read_txn.blobs_by_tag("temp").unwrap().is_empty());
    assert!(read_txn.blob_tags(&blob_id).unwrap().is_empty());
}

#[test]
fn blob_namespace_basic() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    let id1 = write_txn
        .store_blob(
            b"session-data",
            ContentType::OctetStream,
            "s1",
            StoreOptions::with_namespace("session-abc"),
        )
        .unwrap();
    let id2 = write_txn
        .store_blob(
            b"other-data",
            ContentType::OctetStream,
            "s2",
            StoreOptions::with_namespace("session-xyz"),
        )
        .unwrap();
    let _id3 = write_txn
        .store_blob(
            b"no-ns",
            ContentType::OctetStream,
            "s3",
            StoreOptions::default(),
        )
        .unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();

    // Query by namespace
    let abc_blobs = read_txn.blobs_in_namespace("session-abc").unwrap();
    assert_eq!(abc_blobs.len(), 1);
    assert_eq!(abc_blobs[0].0, id1);

    let xyz_blobs = read_txn.blobs_in_namespace("session-xyz").unwrap();
    assert_eq!(xyz_blobs.len(), 1);
    assert_eq!(xyz_blobs[0].0, id2);

    // Lookup namespace for a blob
    assert_eq!(
        read_txn.blob_namespace(&id1).unwrap(),
        Some("session-abc".to_string())
    );
    assert_eq!(
        read_txn.blob_namespace(&id2).unwrap(),
        Some("session-xyz".to_string())
    );

    // Blob without namespace
    assert!(read_txn.blob_namespace(&_id3).unwrap().is_none());
}

#[test]
fn blob_namespace_filtered_temporal() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    let _a = write_txn
        .store_blob(
            b"a",
            ContentType::OctetStream,
            "a",
            StoreOptions::with_namespace("ns1"),
        )
        .unwrap();
    let _b = write_txn
        .store_blob(
            b"b",
            ContentType::OctetStream,
            "b",
            StoreOptions::with_namespace("ns2"),
        )
        .unwrap();
    let _c = write_txn
        .store_blob(
            b"c",
            ContentType::OctetStream,
            "c",
            StoreOptions::with_namespace("ns1"),
        )
        .unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();

    // All blobs in time range
    let all = read_txn.blobs_in_time_range(0, u64::MAX).unwrap();
    assert_eq!(all.len(), 3);

    // Filtered by ns1
    let ns1 = read_txn
        .blobs_in_time_range_ns(0, u64::MAX, Some("ns1"))
        .unwrap();
    assert_eq!(ns1.len(), 2);

    // Filtered by ns2
    let ns2 = read_txn
        .blobs_in_time_range_ns(0, u64::MAX, Some("ns2"))
        .unwrap();
    assert_eq!(ns2.len(), 1);

    // No namespace filter returns all
    let none = read_txn.blobs_in_time_range_ns(0, u64::MAX, None).unwrap();
    assert_eq!(none.len(), 3);
}

#[test]
fn blob_namespace_delete_cleanup() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let blob_id;
    {
        let write_txn = db.begin_write().unwrap();
        blob_id = write_txn
            .store_blob(
                b"ns-delete",
                ContentType::OctetStream,
                "nsd",
                StoreOptions::with_namespace("cleanup-ns"),
            )
            .unwrap();
        write_txn.commit().unwrap();
    }

    // Verify namespace exists
    {
        let read_txn = db.begin_read().unwrap();
        assert_eq!(read_txn.blobs_in_namespace("cleanup-ns").unwrap().len(), 1);
    }

    // Delete
    {
        let write_txn = db.begin_write().unwrap();
        write_txn.delete_blob(&blob_id).unwrap();
        write_txn.commit().unwrap();
    }

    let read_txn = db.begin_read().unwrap();
    assert!(
        read_txn
            .blobs_in_namespace("cleanup-ns")
            .unwrap()
            .is_empty()
    );
    assert!(read_txn.blob_namespace(&blob_id).unwrap().is_none());
}

#[test]
fn blob_tags_and_namespace_combined() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    let id = write_txn
        .store_blob(
            b"full-featured",
            ContentType::Embedding,
            "embed",
            StoreOptions {
                causal_link: None,
                namespace: Some("ml-pipeline".to_string()),
                tags: vec!["embedding".to_string(), "v2".to_string()],
            },
        )
        .unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();

    // Tags work
    let by_tag = read_txn.blobs_by_tag("embedding").unwrap();
    assert_eq!(by_tag.len(), 1);
    assert_eq!(by_tag[0], id);

    // Namespace works
    let by_ns = read_txn.blobs_in_namespace("ml-pipeline").unwrap();
    assert_eq!(by_ns.len(), 1);
    assert_eq!(by_ns[0].0, id);

    // Both readable
    let tags = read_txn.blob_tags(&id).unwrap();
    assert_eq!(tags.len(), 2);
    assert_eq!(
        read_txn.blob_namespace(&id).unwrap(),
        Some("ml-pipeline".to_string())
    );
}

#[test]
fn blob_tags_with_streaming_writer() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    let blob_id = {
        let mut writer = write_txn
            .blob_writer(
                ContentType::OctetStream,
                "streamed-tags",
                StoreOptions::with_tags(&["streamed", "sensor"]),
            )
            .unwrap();
        writer.write(b"streamed data with tags").unwrap();
        writer.finish().unwrap()
    };
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let by_tag = read_txn.blobs_by_tag("streamed").unwrap();
    assert_eq!(by_tag.len(), 1);
    assert_eq!(by_tag[0], blob_id);

    let tags = read_txn.blob_tags(&blob_id).unwrap();
    assert_eq!(tags.len(), 2);
}

// ── Partial / Range Blob Read Tests ──────────────────────────────────────────

#[test]
fn blob_range_read_basic() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let data: Vec<u8> = (0..=255).cycle().take(1024).collect();
    let blob_id;
    {
        let write_txn = db.begin_write().unwrap();
        blob_id = write_txn
            .store_blob(
                &data,
                ContentType::OctetStream,
                "range-test",
                StoreOptions::default(),
            )
            .unwrap();
        write_txn.commit().unwrap();
    }

    let read_txn = db.begin_read().unwrap();

    // Read a middle slice
    let slice = read_txn
        .read_blob_range(&blob_id, 100, 200)
        .unwrap()
        .unwrap();
    assert_eq!(slice.len(), 200);
    assert_eq!(slice, &data[100..300]);
}

#[test]
fn blob_range_read_full() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let data = b"complete blob data for full range read test";
    let blob_id;
    {
        let write_txn = db.begin_write().unwrap();
        blob_id = write_txn
            .store_blob(
                data,
                ContentType::OctetStream,
                "full",
                StoreOptions::default(),
            )
            .unwrap();
        write_txn.commit().unwrap();
    }

    let read_txn = db.begin_read().unwrap();
    let full_range = read_txn
        .read_blob_range(&blob_id, 0, data.len() as u64)
        .unwrap()
        .unwrap();
    let (full_get, _) = read_txn.get_blob(&blob_id).unwrap().unwrap();

    assert_eq!(full_range, full_get);
    assert_eq!(full_range, data);
}

#[test]
fn blob_range_read_start() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let data = b"HEADER_DATA_REST_OF_BLOB_CONTENT";
    let blob_id;
    {
        let write_txn = db.begin_write().unwrap();
        blob_id = write_txn
            .store_blob(
                data,
                ContentType::OctetStream,
                "start",
                StoreOptions::default(),
            )
            .unwrap();
        write_txn.commit().unwrap();
    }

    let read_txn = db.begin_read().unwrap();
    let first_11 = read_txn.read_blob_range(&blob_id, 0, 11).unwrap().unwrap();
    assert_eq!(&first_11, b"HEADER_DATA");
}

#[test]
fn blob_range_read_end() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let data = b"beginning_TAIL_BYTES";
    let blob_id;
    {
        let write_txn = db.begin_write().unwrap();
        blob_id = write_txn
            .store_blob(
                data,
                ContentType::OctetStream,
                "end",
                StoreOptions::default(),
            )
            .unwrap();
        write_txn.commit().unwrap();
    }

    let read_txn = db.begin_read().unwrap();
    let last_10 = read_txn
        .read_blob_range(&blob_id, data.len() as u64 - 10, 10)
        .unwrap()
        .unwrap();
    assert_eq!(&last_10, b"TAIL_BYTES");
}

#[test]
fn blob_range_read_out_of_bounds() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let data = b"short";
    let blob_id;
    {
        let write_txn = db.begin_write().unwrap();
        blob_id = write_txn
            .store_blob(
                data,
                ContentType::OctetStream,
                "oob",
                StoreOptions::default(),
            )
            .unwrap();
        write_txn.commit().unwrap();
    }

    let read_txn = db.begin_read().unwrap();
    let result = read_txn.read_blob_range(&blob_id, 3, 10);
    assert!(result.is_err());
    match result.unwrap_err() {
        StorageError::BlobRangeOutOfBounds {
            blob_length,
            requested_offset,
            requested_length,
        } => {
            assert_eq!(blob_length, 5);
            assert_eq!(requested_offset, 3);
            assert_eq!(requested_length, 10);
        }
        other => panic!("Expected BlobRangeOutOfBounds, got: {other}"),
    }
}

#[test]
fn blob_range_read_zero_length() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let data = b"nonempty";
    let blob_id;
    {
        let write_txn = db.begin_write().unwrap();
        blob_id = write_txn
            .store_blob(
                data,
                ContentType::OctetStream,
                "zero",
                StoreOptions::default(),
            )
            .unwrap();
        write_txn.commit().unwrap();
    }

    let read_txn = db.begin_read().unwrap();
    let empty = read_txn.read_blob_range(&blob_id, 0, 0).unwrap().unwrap();
    assert!(empty.is_empty());
}

#[test]
fn blob_range_read_nonexistent() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    // Store one blob so blob tables exist
    {
        let write_txn = db.begin_write().unwrap();
        write_txn
            .store_blob(b"x", ContentType::OctetStream, "x", StoreOptions::default())
            .unwrap();
        write_txn.commit().unwrap();
    }

    let fake_id = BlobId::new(999_999, 0);
    let read_txn = db.begin_read().unwrap();
    let result = read_txn.read_blob_range(&fake_id, 0, 1).unwrap();
    assert!(result.is_none());
}

#[test]
fn blob_reader_seek_and_read() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let data: Vec<u8> = (0u8..=255).cycle().take(512).collect();
    let blob_id;
    {
        let write_txn = db.begin_write().unwrap();
        blob_id = write_txn
            .store_blob(
                &data,
                ContentType::OctetStream,
                "seek",
                StoreOptions::default(),
            )
            .unwrap();
        write_txn.commit().unwrap();
    }

    let read_txn = db.begin_read().unwrap();
    let mut reader = read_txn.blob_reader(&blob_id).unwrap().unwrap();

    assert_eq!(reader.len(), 512);
    assert_eq!(reader.position(), 0);

    // Read first 10 bytes
    let mut buf = [0u8; 10];
    reader.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, &data[..10]);
    assert_eq!(reader.position(), 10);

    // Seek to position 200
    reader.seek(SeekFrom::Start(200)).unwrap();
    assert_eq!(reader.position(), 200);

    reader.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, &data[200..210]);

    // Seek from end
    reader.seek(SeekFrom::End(-20)).unwrap();
    assert_eq!(reader.position(), 492);

    let mut tail = [0u8; 20];
    reader.read_exact(&mut tail).unwrap();
    assert_eq!(&tail, &data[492..512]);

    // Seek from current
    reader.seek(SeekFrom::Start(100)).unwrap();
    reader.seek(SeekFrom::Current(50)).unwrap();
    assert_eq!(reader.position(), 150);

    reader.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, &data[150..160]);

    // EOF: read returns 0
    reader.seek(SeekFrom::Start(512)).unwrap();
    let n = reader.read(&mut buf).unwrap();
    assert_eq!(n, 0);
}

#[test]
fn blob_reader_read_sequential() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let data: Vec<u8> = (0..=255).cycle().take(1000).collect();
    let blob_id;
    {
        let write_txn = db.begin_write().unwrap();
        blob_id = write_txn
            .store_blob(
                &data,
                ContentType::OctetStream,
                "seq",
                StoreOptions::default(),
            )
            .unwrap();
        write_txn.commit().unwrap();
    }

    let read_txn = db.begin_read().unwrap();
    let mut reader = read_txn.blob_reader(&blob_id).unwrap().unwrap();

    // Read entire blob in 64-byte chunks
    let mut result = Vec::new();
    let mut buf = [0u8; 64];
    loop {
        let n = reader.read(&mut buf).unwrap();
        if n == 0 {
            break;
        }
        result.extend_from_slice(&buf[..n]);
    }
    assert_eq!(result, data);
}

#[test]
fn blob_range_read_streaming_writer() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let blob_id;
    {
        let write_txn = db.begin_write().unwrap();
        let mut writer = write_txn
            .blob_writer(
                ContentType::OctetStream,
                "stream-range",
                StoreOptions::default(),
            )
            .unwrap();
        // Write in 3 chunks
        writer.write(b"AAAAAAAAAA").unwrap(); // 10 bytes of 'A'
        writer.write(b"BBBBBBBBBB").unwrap(); // 10 bytes of 'B'
        writer.write(b"CCCCCCCCCC").unwrap(); // 10 bytes of 'C'
        blob_id = writer.finish().unwrap();
        write_txn.commit().unwrap();
    }

    let read_txn = db.begin_read().unwrap();

    // Range read spanning chunk boundaries
    let slice = read_txn.read_blob_range(&blob_id, 5, 20).unwrap().unwrap();
    assert_eq!(slice.len(), 20);
    assert_eq!(&slice[..5], b"AAAAA"); // last 5 of first chunk
    assert_eq!(&slice[5..15], b"BBBBBBBBBB"); // entire second chunk
    assert_eq!(&slice[15..20], b"CCCCC"); // first 5 of third chunk
}

#[test]
fn blob_range_read_within_write_txn() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    let data = b"read range within uncommitted txn";
    let blob_id = write_txn
        .store_blob(
            data,
            ContentType::OctetStream,
            "write-range",
            StoreOptions::default(),
        )
        .unwrap();

    // Range read within the same write transaction
    let slice = write_txn.read_blob_range(&blob_id, 5, 5).unwrap().unwrap();
    assert_eq!(&slice, b"range");

    // BlobReader within write txn
    let mut reader = write_txn.blob_reader(&blob_id).unwrap().unwrap();
    let mut buf = [0u8; 4];
    reader.seek(SeekFrom::Start(0)).unwrap();
    reader.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"read");

    write_txn.commit().unwrap();
}

#[test]
fn blob_reader_nonexistent() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    {
        let write_txn = db.begin_write().unwrap();
        write_txn
            .store_blob(b"x", ContentType::OctetStream, "x", StoreOptions::default())
            .unwrap();
        write_txn.commit().unwrap();
    }

    let fake_id = BlobId::new(999_999, 0);
    let read_txn = db.begin_read().unwrap();
    let result = read_txn.blob_reader(&fake_id).unwrap();
    assert!(result.is_none());
}

#[test]
fn blob_reader_read_range_method() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let data: Vec<u8> = (0u8..=255).cycle().take(500).collect();
    let blob_id;
    {
        let write_txn = db.begin_write().unwrap();
        blob_id = write_txn
            .store_blob(
                &data,
                ContentType::OctetStream,
                "rr",
                StoreOptions::default(),
            )
            .unwrap();
        write_txn.commit().unwrap();
    }

    let read_txn = db.begin_read().unwrap();
    let mut reader = read_txn.blob_reader(&blob_id).unwrap().unwrap();

    let slice = reader.read_range(100, 50).unwrap();
    assert_eq!(slice, &data[100..150]);
    assert_eq!(reader.position(), 150);

    // Out-of-bounds read_range
    let err = reader.read_range(490, 20);
    assert!(err.is_err());
}
