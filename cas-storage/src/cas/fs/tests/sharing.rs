use super::*;

/// The configured stripe count reaches the stripes.
///
/// Pins the plumbing the config knob added: `stripe_count` travels from
/// `single_namespace` into `SharedBlockStore::new` and on into `Stripes`.
/// Observed through behaviour rather than a length accessor -- with one
/// stripe every block must resolve to the same lock, and with many, two
/// ids two apart must not. A knob that was accepted and dropped on the
/// floor would pass every config test and fail this one.
#[test]
fn the_configured_stripe_count_reaches_the_stripes() {
    let one = tempdir().unwrap();
    let fs = CasFS::single_namespace(
        one.path().to_path_buf(),
        one.path().to_path_buf(),
        METRICS.clone(),
        StoreOptions {
            stripe_count: Some(1),
            ..test_options()
        },
    )
    .unwrap();

    let id = |b0: u8, b1: u8| {
        let mut bytes = [0u8; crate::metastore::BLOCKID_SIZE];
        bytes[0] = b0;
        bytes[1] = b1;
        crate::metastore::BlockId::from(bytes)
    };

    // One stripe: everything collides, by construction.
    assert!(Arc::ptr_eq(
        &fs.shared.stripes().for_hash(&id(0x00, 0x01)),
        &fs.shared.stripes().for_hash(&id(0xff, 0xfe))
    ));
    drop(fs);

    // The default is not 1, so the same two ids must now part ways --
    // otherwise the assertion above would hold for any count and prove
    // nothing.
    let many = tempdir().unwrap();
    let fs = CasFS::single_namespace(
        many.path().to_path_buf(),
        many.path().to_path_buf(),
        METRICS.clone(),
        test_options(),
    )
    .unwrap();
    assert!(!Arc::ptr_eq(
        &fs.shared.stripes().for_hash(&id(0x00, 0x01)),
        &fs.shared.stripes().for_hash(&id(0xff, 0xfe))
    ));
}

/// ADR 0006: two namespaces over one `SharedBlockStore` resolve one
/// stripe set and ONE disk path per block. Cross-namespace dedup must
/// bump the shared record and never write a second file.
#[tokio::test]
async fn test_two_namespaces_share_stripes_and_block_paths() {
    let dir = tempdir().unwrap();
    let shared = Arc::new(
        crate::cas::SharedBlockStore::new(
            dir.path().join("meta/blocks"),
            dir.path().join("blocks"),
            test_options(),
        )
        .unwrap(),
    );
    let ns = |name: &str| {
        CasFS::new(
            dir.path().join("meta").join(name),
            shared.clone(),
            METRICS.clone(),
            test_options(),
        )
        .unwrap()
    };
    let alice = ns("alice");
    let bob = ns("bob");
    alice.create_bucket("b").unwrap();
    bob.create_bucket("b").unwrap();

    let data = b"cross namespace dedup payload".repeat(40).to_vec();
    let id = alice.hasher().hash(&data);

    // Same stripe object from both namespaces.
    assert!(Arc::ptr_eq(
        &alice.shared.stripes().for_hash(&id),
        &bob.shared.stripes().for_hash(&id)
    ));

    let put = |data: Vec<u8>| {
        let len = data.len();
        let stream = AsyncByteStream::new(stream::once(async move { Ok(Bytes::from(data)) }));
        (stream, len)
    };
    let (stream, len) = put(data.clone());
    let obj_a = alice
        .store_single_object_and_meta("b", "k", stream, len)
        .await
        .unwrap();
    let (stream, len) = put(data.clone());
    let obj_b = bob
        .store_single_object_and_meta("b", "k", stream, len)
        .await
        .unwrap();
    assert_eq!(obj_a.blocks(), obj_b.blocks());

    // One record, rc 2, one file at one path derived from ONE root.
    let block = shared
        .block_tree()
        .get_block(id.as_slice())
        .unwrap()
        .unwrap();
    assert_eq!(block.rc(), 2, "dedup must bump the shared record");
    let path_a = block.disk_path(&id, alice.fs_root().clone());
    let path_b = block.disk_path(&id, bob.fs_root().clone());
    assert_eq!(path_a, path_b, "both namespaces derive the same path");
    assert!(path_a.is_file());
}

/// ADR 0006 orphan healing, end to end: a file named like the block,
/// hand-planted on the id's directory chain at a non-policy depth (crash
/// residue of an earlier attempt), is adopted by a later PUT -- the
/// record stores the orphan's depth and the file ends up with the real
/// bytes at that same path.
#[tokio::test]
async fn test_orphan_block_file_is_healed_in_place() {
    let (fs, _dir) = setup_test_fs(StorageEngine::Fjall, Hasher::Blake3W32);
    const BUCKET: &str = "test-bucket";
    fs.create_bucket(BUCKET).unwrap();

    let data = b"orphan heal payload".repeat(64).to_vec();
    let id = fs.hasher().hash(&data);

    // Plant garbage at depth 2 on the id's chain, as a torn write would.
    let orphan_path = crate::metastore::block_disk_path(&id, 2, fs.fs_root().clone());
    std::fs::create_dir_all(orphan_path.parent().unwrap()).unwrap();
    std::fs::write(&orphan_path, b"torn garbage").unwrap();

    let len = data.len();
    let stream = AsyncByteStream::new(stream::once(async move { Ok(Bytes::from(data.clone())) }));
    let obj = fs
        .store_single_object_and_meta(BUCKET, "healed", stream, len)
        .await
        .unwrap();
    assert_eq!(obj.blocks(), [id]);

    let block = fs
        .shared
        .block_tree()
        .get_block(id.as_slice())
        .unwrap()
        .unwrap();
    assert_eq!(block.depth(), 2, "record must adopt the orphan's depth");
    let bytes = std::fs::read(&orphan_path).unwrap();
    assert_eq!(
        fs.hasher().hash(&bytes),
        id,
        "the orphan path must now hold the real block bytes"
    );
}

/// The per-bucket usage ledger: every object write and every delete moves
/// it, and the number it lands on is the sum of the records' sizes.
///
/// LOGICAL bytes, deliberately: the same content stored in two buckets
/// costs one set of blocks on disk and counts in full against both, since
/// either of them can be the holder that keeps it alive. It is what
/// respcas spends a namespace quota in (`max_size`).
#[tokio::test]
async fn the_usage_counter_follows_the_records_it_counts() {
    let (fs, _dir) = setup_test_fs(StorageEngine::Fjall, Hasher::Blake3W32);
    fs.create_bucket("b").unwrap();
    assert_eq!(usage(&fs, "b"), Some(0), "a fresh bucket counts nothing");

    // A store adds its size, inline or block-backed alike.
    fs.store_inlined_object("b", "small", vec![7u8; 40])
        .await
        .unwrap();
    assert_eq!(usage(&fs, "b"), Some(40));

    let big = put_blocks(&fs, "b", "big", vec![9u8; 3 * BLOCK_SIZE]).await;
    assert_eq!(big.size(), 3 * BLOCK_SIZE as u64);
    assert_eq!(usage(&fs, "b"), Some(40 + 3 * BLOCK_SIZE as u64));

    // An overwrite applies the difference, in both directions.
    fs.store_inlined_object("b", "small", vec![7u8; 100])
        .await
        .unwrap();
    assert_eq!(usage(&fs, "b"), Some(100 + 3 * BLOCK_SIZE as u64));
    fs.store_inlined_object("b", "big", vec![7u8; 10])
        .await
        .unwrap();
    assert_eq!(
        usage(&fs, "b"),
        Some(110),
        "the blocks it displaced are gone"
    );

    // A delete gives the bytes back, and deleting nothing changes nothing.
    assert!(fs.delete_object("b", "small").await.unwrap());
    assert_eq!(usage(&fs, "b"), Some(10));
    assert!(!fs.delete_object("b", "small").await.unwrap());
    assert_eq!(usage(&fs, "b"), Some(10));
    assert!(fs.delete_object("b", "big").await.unwrap());
    assert_eq!(usage(&fs, "b"), Some(0), "an emptied bucket counts nothing");

    // A dropped bucket takes its counter with it: the name is free, and
    // so is the budget of whatever is created under it next.
    fs.store_inlined_object("b", "again", vec![1u8; 64])
        .await
        .unwrap();
    assert_eq!(usage(&fs, "b"), Some(64));
    fs.namespace_meta_store().drop_bucket("b").unwrap();
    assert_eq!(usage(&fs, "b"), None);
}

/// A cross-namespace clone (ADR 0014) is a store on the ledger: the
/// destination is charged the full logical size, however few bytes moved.
#[tokio::test]
async fn a_clone_charges_the_destination_in_full() {
    let (fs, _dir) = setup_test_fs(StorageEngine::Fjall, Hasher::Blake3W32);
    fs.create_bucket("source").unwrap();
    fs.create_bucket("dest").unwrap();

    let obj = put_blocks(&fs, "source", "k", vec![3u8; 2 * BLOCK_SIZE]).await;
    assert_eq!(usage(&fs, "source"), Some(obj.size()));
    assert_eq!(usage(&fs, "dest"), Some(0));

    fs.clone_object_by_reference("source", b"k", "dest", b"k")
        .await
        .unwrap()
        .expect("the source is there");

    assert_eq!(
        usage(&fs, "dest"),
        Some(obj.size()),
        "a reference to content is a record of that size in this namespace"
    );
    assert_eq!(
        usage(&fs, "source"),
        Some(obj.size()),
        "and the source is unmoved"
    );

    // The source letting go leaves the clone's charge exactly where it is.
    fs.delete_object("source", "k").await.unwrap();
    assert_eq!(usage(&fs, "source"), Some(0));
    assert_eq!(usage(&fs, "dest"), Some(obj.size()));
}
