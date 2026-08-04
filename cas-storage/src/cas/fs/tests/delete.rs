use super::*;

#[tokio::test]
async fn test_store_and_delete_object() {
    for (engine, hasher) in matrix() {
        let (fs, _dir) = setup_test_fs(engine, hasher);
        do_test_store_and_delete_object(fs).await;
    }
}

// test store and delete object
// - store an object
// - delete the object
async fn do_test_store_and_delete_object(fs: CasFS) {
    let bucket_name = "test-bucket";
    let key = "test/key";

    // Create bucket
    fs.create_bucket(bucket_name).unwrap();

    // Create test data and stream
    let test_data = b"test data".to_vec();
    let test_data_len = test_data.len();
    let stream = AsyncByteStream::new(stream::once(async move { Ok(Bytes::from(test_data)) }));

    // Store object
    let obj = fs
        .store_single_object_and_meta(bucket_name, key, stream, test_data_len)
        .await
        .unwrap();

    // Verify object exists
    let exists = fs.key_exists(bucket_name, key).unwrap();
    assert!(exists);

    // verify blocks and their files exist
    let block_tree = fs.shared.block_tree();
    let mut stored_paths = Vec::new();
    for id in obj.blocks() {
        let block = block_tree.get_block(id.as_slice()).unwrap().unwrap();
        let path = block.disk_path(id, fs.fs_root().clone());
        assert!(path.is_file(), "block file must exist before delete");
        stored_paths.push(path);
    }

    // Delete object
    fs.delete_object(bucket_name, key).await.unwrap();

    // Verify object no longer exists
    let exists = fs.key_exists(bucket_name, key).unwrap();
    assert!(!exists);

    // Verify blocks were cleaned up
    let block_tree = fs.shared.block_tree();
    for id in obj.blocks() {
        assert!(block_tree.get_block(id.as_slice()).unwrap().is_none());
    }
    // Verify the files are gone too
    for path in stored_paths {
        assert!(!path.exists(), "block file must be unlinked by delete");
    }
}

#[tokio::test]
async fn test_store_and_delete_object_with_refcount_same_blocks_diffkey() {
    for (engine, hasher) in matrix() {
        let (fs, _dir) = setup_test_fs(engine, hasher);
        do_test_store_and_delete_object_with_refcount_same_blocks_diffkey(fs).await;
    }
}

// Test storing and deleting an object with refcount
// - store object
//       refcount == 1
// - store object again with differrent key
//      refcount == 2
// - delete the first object
// - check block/disk/whatever is still there
// - delete the second object
// - check block/disk/whatever should be gone
async fn do_test_store_and_delete_object_with_refcount_same_blocks_diffkey(fs: CasFS) {
    let bucket = "test-bucket";
    let key1 = "test/key1";
    let key2 = "test/key2";

    // Create bucket
    fs.create_bucket(bucket).unwrap();

    // Create test data
    let test_data = b"test data".to_vec();
    let test_data_len = test_data.len();
    let test_data2 = test_data.clone();
    let stream1 = AsyncByteStream::new(stream::once(async move { Ok(Bytes::from(test_data)) }));

    // Store first object
    let obj1 = fs
        .store_single_object_and_meta(bucket, key1, stream1, test_data_len)
        .await
        .unwrap();
    // Verify blocks  exist with rc=1
    let block_tree = fs.shared.block_tree();
    for id in obj1.blocks() {
        let block = block_tree.get_block(id.as_slice()).unwrap().unwrap();
        assert_eq!(block.rc(), 1);
    }

    // Store same data with different key

    let stream2 = AsyncByteStream::new(stream::once(async move { Ok(Bytes::from(test_data2)) }));

    let obj2 = fs
        .store_single_object_and_meta(bucket, key2, stream2, test_data_len)
        .await
        .unwrap();

    // Verify both objects share same blocks
    assert_eq!(obj1.blocks(), obj2.blocks());
    assert_eq!(obj1.hash(), obj2.hash());
    // Verify blocks  exist with rc=2
    let block_tree = fs.shared.block_tree();
    for id in obj2.blocks() {
        let block = block_tree.get_block(id.as_slice()).unwrap().unwrap();
        assert_eq!(block.rc(), 2);
    }

    // Delete first object
    fs.delete_object(bucket, key1).await.unwrap();

    // Verify blocks still exist
    let block_tree = fs.shared.block_tree();
    for id in obj1.blocks() {
        let block = block_tree.get_block(id.as_slice()).unwrap().unwrap();
        assert_eq!(block.rc(), 1);
    }

    // Delete second object
    fs.delete_object(bucket, key2).await.unwrap();

    // Verify blocks are gone
    for id in obj1.blocks() {
        assert!(block_tree.get_block(id.as_slice()).unwrap().is_none());
    }
}

#[tokio::test]
async fn test_same_key_rewrite_then_delete_frees_the_block() {
    for (engine, hasher) in matrix() {
        let (fs, _dir) = setup_test_fs(engine, hasher);
        do_test_same_key_rewrite_then_delete_frees_the_block(fs).await;
    }
}

// Store the same content twice under ONE key, then delete the key.
//
// The full lifecycle of the rule pair: every dedup hit bumps (ADR 0006,
// the key_has_block skip is gone) and every overwrite releases what it
// displaced (ADR 0008). The re-PUT bumps to 2 and releases back to 1,
// so the single DELETE that follows takes the LAST reference -- record
// removed, file unlinked, nothing left for fsck to reconcile.
//
// Both halves are load-bearing. Without the bump the re-PUT would
// under-count, which is the pre-0006 behaviour that lost data in the
// multipart trace. Without the release the block would survive this
// DELETE at rc 1 with no holder: the leak ADR 0008 closed.
async fn do_test_same_key_rewrite_then_delete_frees_the_block(fs: CasFS) {
    let bucket = "test-bucket";
    let key1 = "test/key1";

    // Create bucket
    fs.create_bucket(bucket).unwrap();

    // Create test data
    let test_data = b"test data".to_vec();
    let test_data_len = test_data.len();
    let test_data2 = test_data.clone();
    let stream1 = AsyncByteStream::new(stream::once(async move { Ok(Bytes::from(test_data)) }));

    // Store first object
    let obj1 = fs
        .store_single_object_and_meta(bucket, key1, stream1, test_data_len)
        .await
        .unwrap();
    // Verify blocks  exist with rc=1
    let block_tree = fs.shared.block_tree();
    for id in obj1.blocks() {
        let block = block_tree.get_block(id.as_slice()).unwrap().unwrap();
        assert_eq!(block.rc(), 1);
    }

    // Store same data with same key

    let stream2 = AsyncByteStream::new(stream::once(async move { Ok(Bytes::from(test_data2)) }));

    let obj2 = fs
        .store_single_object_and_meta(bucket, key1, stream2, test_data_len)
        .await
        .unwrap();

    // Verify both objects share same blocks
    assert_eq!(obj1.blocks(), obj2.blocks());
    assert_eq!(obj1.hash(), obj2.hash());
    // The re-PUT bumped the rc to 2 and the overwrite released the
    // record it displaced, taking it back to 1: one live object, one
    // reference.
    let block_tree = fs.shared.block_tree();
    for id in obj2.blocks() {
        let block = block_tree.get_block(id.as_slice()).unwrap().unwrap();
        assert_eq!(block.rc(), 1, "bump then release nets to nothing");
    }

    // Delete object
    fs.delete_object(bucket, key1).await.unwrap();

    // That was the last reference: record gone, file unlinked. Nothing
    // survives for fsck to collect.
    for id in obj1.blocks() {
        assert!(
            block_tree.get_block(id.as_slice()).unwrap().is_none(),
            "the last reference is gone, so the record must be too"
        );
        assert!(
            !crate::metastore::block_disk_path(id, 1, fs.fs_root().clone()).exists(),
            "the last release must unlink the file"
        );
    }
}

/// DELETE is idempotent: the second delete of one key finds no object
/// record (the atomic take-object pair removed it) and does nothing --
/// in particular it must NOT decrement any block a still-live object
/// holds.
#[tokio::test]
async fn test_double_delete_same_key_is_idempotent() {
    let (fs, _dir) = setup_test_fs(StorageEngine::Fjall, Hasher::Blake3W32);
    let bucket = "test-bucket";
    fs.create_bucket(bucket).unwrap();

    let data = b"double delete payload".repeat(50).to_vec();
    let make_stream = |data: Vec<u8>| {
        let len = data.len();
        (
            AsyncByteStream::new(stream::once(async move { Ok(Bytes::from(data)) })),
            len,
        )
    };

    // Two keys share the block: rc == 2.
    let (stream, len) = make_stream(data.clone());
    let obj = fs
        .store_single_object_and_meta(bucket, "a", stream, len)
        .await
        .unwrap();
    let (stream, len) = make_stream(data.clone());
    fs.store_single_object_and_meta(bucket, "b", stream, len)
        .await
        .unwrap();
    let id = obj.blocks()[0];
    let block_tree = fs.shared.block_tree();
    assert_eq!(
        block_tree.get_block(id.as_slice()).unwrap().unwrap().rc(),
        2
    );

    // Delete key "a" twice. The first decrements; the second is a no-op.
    fs.delete_object(bucket, "a").await.unwrap();
    fs.delete_object(bucket, "a").await.unwrap();

    let block = block_tree.get_block(id.as_slice()).unwrap().unwrap();
    assert_eq!(block.rc(), 1, "the double DELETE must not double-decrement");
    assert!(
        block.disk_path(&id, fs.fs_root().clone()).is_file(),
        "the block key \"b\" references must survive"
    );
}
