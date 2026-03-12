use redb::{BlobId, ContentType, Database, ReadableDatabase, StorageError};

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
            .store_blob(data, ContentType::OctetStream, "test-blob", None)
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
        .store_blob(data, ContentType::Metadata, "inline-read", None)
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
            .store_blob(&data, ContentType::ImagePng, "image", None)
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
            .store_blob(b"x", ContentType::OctetStream, "", None)
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
        .store_blob(b"first", ContentType::OctetStream, "a", None)
        .unwrap();
    let id2 = write_txn
        .store_blob(b"second", ContentType::AudioWav, "b", None)
        .unwrap();
    let id3 = write_txn
        .store_blob(b"third", ContentType::VideoMp4, "c", None)
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
            .store_blob(b"to-delete", ContentType::OctetStream, "del", None)
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
            .store_blob(b"persistent", ContentType::Embedding, "embed", None)
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
            .store_blob(b"committed", ContentType::OctetStream, "ok", None)
            .unwrap();
        write_txn.commit().unwrap();
    }

    // Store but abort
    let aborted_id;
    {
        let write_txn = db.begin_write().unwrap();
        aborted_id = write_txn
            .store_blob(b"aborted", ContentType::OctetStream, "nope", None)
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
        .store_blob(b"a", ContentType::OctetStream, "t1", None)
        .unwrap();
    let id2 = write_txn
        .store_blob(b"b", ContentType::OctetStream, "t2", None)
        .unwrap();
    let id3 = write_txn
        .store_blob(b"c", ContentType::OctetStream, "t3", None)
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
        .store_blob(b"sensor1", ContentType::SensorImu, "imu", None)
        .unwrap();
    write_txn
        .store_blob(b"sensor2", ContentType::ImageJpeg, "cam", None)
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
        .store_blob(b"root", ContentType::OctetStream, "root", None)
        .unwrap();
    let child = write_txn
        .store_blob(b"child", ContentType::OctetStream, "child", Some(root))
        .unwrap();
    let grandchild = write_txn
        .store_blob(
            b"grandchild",
            ContentType::OctetStream,
            "grandchild",
            Some(child),
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
        .store_blob(b"parent", ContentType::OctetStream, "p", None)
        .unwrap();
    let child = write_txn
        .store_blob(b"child", ContentType::OctetStream, "c", Some(parent))
        .unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let children = read_txn.causal_children(&parent).unwrap();
    assert_eq!(children.len(), 1);
    assert_eq!(children[0], child);

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
        .store_blob(b"a", ContentType::OctetStream, "a", None)
        .unwrap();
    let b = write_txn
        .store_blob(b"b", ContentType::OctetStream, "b", Some(a))
        .unwrap();
    let c = write_txn
        .store_blob(b"c", ContentType::OctetStream, "c", Some(b))
        .unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();

    // Path from a to c
    let path = read_txn.causal_path(&a, &c, 10).unwrap().unwrap();
    assert_eq!(path, vec![a, b, c]);

    // Path from a to a (trivial)
    let self_path = read_txn.causal_path(&a, &a, 10).unwrap().unwrap();
    assert_eq!(self_path, vec![a]);
}

#[test]
fn causal_path_not_found() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    let a = write_txn
        .store_blob(b"a", ContentType::OctetStream, "a", None)
        .unwrap();
    let b = write_txn
        .store_blob(b"b", ContentType::OctetStream, "b", None)
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
            .store_blob(&data, ContentType::PointCloudLas, "lidar", None)
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
                None,
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
            .store_blob(data, ContentType::OctetStream, "ck", None)
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
                    None,
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
        .store_blob(b"x", ContentType::OctetStream, &long_label, None)
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
            .store_blob(b"first-txn", ContentType::OctetStream, "t1", None)
            .unwrap();
        write_txn.commit().unwrap();
    }

    // Second transaction
    let id2;
    {
        let write_txn = db.begin_write().unwrap();
        id2 = write_txn
            .store_blob(b"second-txn", ContentType::OctetStream, "t2", None)
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
            .blob_writer(ContentType::OctetStream, "basic", None)
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
            .blob_writer(ContentType::PointCloudLas, "lidar", None)
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
            .blob_writer(ContentType::Metadata, "small", None)
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
            .blob_writer(ContentType::OctetStream, "io_write", None)
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
            .blob_writer(ContentType::OctetStream, "aborted", None)
            .unwrap();
        writer.write(b"partial data").unwrap();
        // drop without finish
    }
    // After drop, we should be able to create a new writer
    let blob_id = {
        let mut writer = write_txn
            .blob_writer(ContentType::OctetStream, "real", None)
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
        .store_blob(b"one-shot", ContentType::OctetStream, "first", None)
        .unwrap();

    // Then streaming
    let id2 = {
        let mut writer = write_txn
            .blob_writer(ContentType::OctetStream, "streaming", None)
            .unwrap();
        writer.write(b"streamed").unwrap();
        writer.finish().unwrap()
    };

    // Then store_blob again
    let id3 = write_txn
        .store_blob(b"another", ContentType::OctetStream, "third", None)
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
            .blob_writer(ContentType::OctetStream, "empty", None)
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
                .blob_writer(ContentType::ImagePng, "persistent", None)
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
        .blob_writer(ContentType::OctetStream, "first", None)
        .unwrap();
    writer.write(b"data").unwrap();

    // Attempting a second writer should fail
    {
        let result = write_txn.blob_writer(ContentType::OctetStream, "second", None);
        assert!(matches!(result, Err(StorageError::BlobWriterActive)));
    }

    // store_blob should also fail while writer is active
    {
        let result = write_txn.store_blob(b"data", ContentType::OctetStream, "blocked", None);
        assert!(matches!(result, Err(StorageError::BlobWriterActive)));
    }

    // After finishing the writer, operations should succeed
    writer.finish().unwrap();
    let _id = write_txn
        .store_blob(b"ok", ContentType::OctetStream, "unblocked", None)
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
        .store_blob(&data, ContentType::OctetStream, "oneshot", None)
        .unwrap();

    // Store via streaming (byte-at-a-time for maximum stress)
    let id_streaming = {
        let mut writer = write_txn
            .blob_writer(ContentType::OctetStream, "streaming", None)
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
