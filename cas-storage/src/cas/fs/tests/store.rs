use super::*;

#[tokio::test]
async fn test_store_object_write_failure() {
    for hasher in TEST_WIDTHS {
        let (fs, _dir) = setup_failing_write_fs(hasher);
        do_test_store_object_write_failure(fs).await;
    }
}

async fn do_test_store_object_write_failure(fs: CasFS) {
    let bucket_name = "test_bucket";
    let key = "test_key";
    fs.create_bucket(bucket_name).unwrap();

    let test_data = b"test data".repeat(100);
    let stream = AsyncByteStream::new(stream::once(async move { Ok(Bytes::from(test_data)) }));

    let result = fs.store_object(bucket_name, key, stream).await;
    assert!(result.is_err());

    // Verify the error
    let err = result.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::Other);
    assert_eq!(err.to_string(), "Mock write failure");

    // File-first: a failed disk write means NO record was ever
    // attempted -- nothing to clean up, nothing left behind.
    let block_tree = fs.shared.block_tree();
    assert_eq!(block_tree.len().unwrap(), 0);

    // Verify object metadata was not created
    assert!(!fs.key_exists(bucket_name, key).unwrap());
}

#[tokio::test]
async fn test_store_object() {
    for (engine, hasher) in matrix() {
        let (fs, _dir) = setup_test_fs(engine, hasher);
        do_test_store_object(fs).await;
    }
}

async fn do_test_store_object(fs: CasFS) {
    const BUCKET_NAME: &str = "test_bucket";
    const KEY1: &str = "test_key1";
    const KEY2: &str = "test_key2";
    fs.create_bucket(BUCKET_NAME).unwrap();

    // Create ByteStream from test data
    let test_data = b"long test data".repeat(100).to_vec();
    let test_data_2 = test_data.clone();
    let test_data_len = test_data.len();
    let stream = AsyncByteStream::new(stream::once(
        async move { Ok(Bytes::from(test_data.clone())) },
    ));

    // Store object
    let obj = fs
        .store_single_object_and_meta(BUCKET_NAME, KEY1, stream, test_data_len)
        .await
        .unwrap();

    // Verify results
    assert_eq!(obj.size(), test_data_len as u64);
    assert_eq!(obj.blocks().len(), 1);

    // Verify block was stored and its file sits at the derived path
    let block_tree = fs.shared.block_tree();
    assert!(block_tree.len().unwrap() > 0);
    let stored_block = block_tree
        .get_block(obj.blocks()[0].as_slice())
        .unwrap()
        .unwrap();
    assert_eq!(stored_block.size(), test_data_len);
    assert_eq!(stored_block.rc(), 1);
    assert!(
        stored_block
            .disk_path(&obj.blocks()[0], fs.fs_root().clone())
            .is_file(),
        "block file must exist at the depth-derived path"
    );

    // Store the same data again with different key
    // - The same block should be returned
    // - The refcount should be increased

    let stream = AsyncByteStream::new(stream::once(
        async move { Ok(Bytes::from(test_data_2.clone())) },
    ));

    let new_obj = fs
        .store_single_object_and_meta(BUCKET_NAME, KEY2, stream, test_data_len)
        .await
        .unwrap();

    assert_eq!(new_obj.blocks(), obj.blocks());

    let stored_block = block_tree
        .get_block(new_obj.blocks()[0].as_slice())
        .unwrap()
        .unwrap();
    assert_eq!(stored_block.rc(), 2);
}

/// The well known MD5 of zero bytes, which is the ETag S3 clients expect
/// for an empty object.
const EMPTY_MD5: &str = "d41d8cd98f00b204e9800998ecf8427e";

#[tokio::test]
async fn test_store_empty_object() {
    for (engine, hasher) in matrix() {
        let (fs, _dir) = setup_test_fs(engine, hasher);
        do_test_store_empty_object(fs).await;
    }
}

async fn do_test_store_empty_object(fs: CasFS) {
    const BUCKET_NAME: &str = "test_bucket";
    const KEY: &str = "empty";
    fs.create_bucket(BUCKET_NAME).unwrap();

    let stream = AsyncByteStream::new(stream::empty());
    let obj = fs
        .store_single_object_and_meta(BUCKET_NAME, KEY, stream, 0)
        .await
        .unwrap();

    assert_eq!(obj.size(), 0);
    assert!(obj.blocks().is_empty());
    // The empty object is not stored, but it still hashes to the MD5 of
    // no bytes rather than to a zero sentinel.
    assert_eq!(obj.format_e_tag(), EMPTY_MD5);
}

#[tokio::test]
async fn test_store_inlined_object() {
    for (engine, hasher) in matrix() {
        let (fs, _dir) = setup_test_fs(engine, hasher);
        do_test_store_inlined_object(fs).await;
    }
}

async fn do_test_store_inlined_object(fs: CasFS) {
    let bucket_name = "test_bucket";
    let key = "test_key1";
    fs.create_bucket(bucket_name).unwrap();

    let small_data = b"small test data".to_vec();
    let obj_meta = fs
        .store_inlined_object(bucket_name, key, small_data.clone())
        .await
        .unwrap();

    // Verify inlined data
    assert_eq!(obj_meta.size(), small_data.len() as u64);
    assert_eq!(obj_meta.inlined().unwrap(), &small_data);
}

#[tokio::test]
async fn test_store_object_refcount() {
    for (engine, hasher) in matrix() {
        let (fs, _dir) = setup_test_fs(engine, hasher);
        do_test_store_object_refcount(fs).await;
    }
}

async fn do_test_store_object_refcount(fs: CasFS) {
    let bucket_name = "test_bucket";
    let key1 = "test_key1";
    let key2 = "test_key2";
    fs.create_bucket(bucket_name).unwrap();

    // Create ByteStream from test data
    let test_data = b"long test data".repeat(100).to_vec();
    let test_data_len = test_data.len();
    let test_data_2 = test_data.clone();
    let test_data_3 = test_data.clone();
    let stream = AsyncByteStream::new(stream::once(
        async move { Ok(Bytes::from(test_data.clone())) },
    ));

    // Store object
    let obj = fs
        .store_single_object_and_meta(bucket_name, key1, stream, test_data_len)
        .await
        .unwrap();

    // Initial refcount must be 1
    let block_tree = fs.shared.block_tree();
    for id in obj.blocks() {
        let block = block_tree.get_block(id.as_slice()).unwrap().unwrap();
        assert_eq!(block.rc(), 1);
    }

    {
        // Re-PUT with the SAME key. Two rules meet here and cancel:
        // every dedup hit bumps (ADR 0006, the key_has_block skip is
        // gone), and the overwrite releases the record it displaced
        // (ADR 0008). Bump to 2, release back to 1 -- one object, one
        // reference, exactly the truth. Before 0008 this settled at 2
        // and waited for fsck.
        let stream =
            AsyncByteStream::new(stream::once(
                async move { Ok(Bytes::from(test_data_2.clone())) },
            ));

        let new_obj = fs
            .store_single_object_and_meta(bucket_name, key1, stream, test_data_len)
            .await
            .unwrap();

        assert_eq!(new_obj.blocks(), obj.blocks());

        let stored_block = block_tree
            .get_block(new_obj.blocks()[0].as_slice())
            .unwrap()
            .unwrap();
        assert_eq!(
            stored_block.rc(),
            1,
            "bump for the new object, release for the replaced one"
        );
    }
    {
        // A SECOND key referencing the same content bumps and displaces
        // nothing: two objects, two references.
        let stream =
            AsyncByteStream::new(stream::once(
                async move { Ok(Bytes::from(test_data_3.clone())) },
            ));

        let new_obj = fs
            .store_single_object_and_meta(bucket_name, key2, stream, test_data_len)
            .await
            .unwrap();

        assert_eq!(new_obj.blocks(), obj.blocks());

        let stored_block = block_tree
            .get_block(new_obj.blocks()[0].as_slice())
            .unwrap()
            .unwrap();
        assert_eq!(stored_block.rc(), 2, "two live objects, two references");
    }
}

#[tokio::test]
async fn test_block_files_hash_to_their_address() {
    for (engine, hasher) in matrix() {
        let (fs, _dir) = setup_test_fs(engine, hasher);
        do_test_block_files_hash_to_their_address(fs, hasher).await;
    }
}

/// End-to-end guard against a write path that hashes with anything but the
/// store's own hasher: write an object the normal way, then re-hash each
/// block file straight off disk and demand the address back, byte for
/// byte.
async fn do_test_block_files_hash_to_their_address(fs: CasFS, hasher: Hasher) {
    const BUCKET: &str = "test-bucket";
    const KEY: &str = "multi/block";
    fs.create_bucket(BUCKET).unwrap();

    let data = multi_block_data();
    let len = data.len();
    let stream = AsyncByteStream::new(stream::once(async move { Ok(Bytes::from(data)) }));
    let obj = fs
        .store_single_object_and_meta(BUCKET, KEY, stream, len)
        .await
        .unwrap();
    assert!(obj.blocks().len() > 1, "test data must span several blocks");

    let block_tree = fs.shared.block_tree();
    for id in obj.blocks() {
        assert_eq!(id.len(), hasher.width() as usize);
        let block = block_tree.get_block(id.as_slice()).unwrap().unwrap();
        let path = block.disk_path(id, fs.fs_root().clone());
        let on_disk = std::fs::read(&path).unwrap();
        assert_eq!(on_disk.len(), block.size());
        assert_eq!(
            hasher.hash(&on_disk).as_slice(),
            id.as_slice(),
            "block file {} does not hash to its address {}",
            path.display(),
            id.to_hex()
        );
    }
}
