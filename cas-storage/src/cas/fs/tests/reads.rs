use super::*;
use crate::cas::block_stream::BlockStream;
use crate::cas::range_request::RangeRequest;
use futures::StreamExt;

#[tokio::test]
async fn test_verify_on_read_catches_corruption() {
    // One width is enough here: what is under test is the read-side check,
    // not the address width, which the matrix above already covers.
    for engine in TEST_ENGINES {
        let (fs, _dir) = setup_test_fs_verifying(engine, Hasher::Blake3W32, true);
        assert!(fs.verify_on_read());
        do_test_verify_on_read_catches_corruption(fs).await;
    }
}

async fn do_test_verify_on_read_catches_corruption(fs: CasFS) {
    const BUCKET: &str = "test-bucket";
    const KEY: &str = "corrupt/me";
    fs.create_bucket(BUCKET).unwrap();

    let data = multi_block_data();
    let original = data.clone();
    let len = data.len();
    let stream = AsyncByteStream::new(stream::once(async move { Ok(Bytes::from(data)) }));
    let obj = fs
        .store_single_object_and_meta(BUCKET, KEY, stream, len)
        .await
        .unwrap();

    // An untouched object reads back clean with verification on.
    assert_eq!(
        read_whole_object(&fs, BUCKET, KEY, true).await.unwrap(),
        original
    );

    // Flip a byte in the second block's file, behind the store's back.
    let victim = obj.blocks()[1];
    let block = fs
        .shared
        .block_tree()
        .get_block(victim.as_slice())
        .unwrap()
        .unwrap();
    let path = block.disk_path(&victim, fs.fs_root().clone());
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[0] ^= 0xff;
    std::fs::write(&path, &bytes).unwrap();

    // Verification on: the read fails, naming the block and its file.
    let err = read_whole_object(&fs, BUCKET, KEY, fs.verify_on_read())
        .await
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    let msg = err.to_string();
    assert!(
        msg.contains(&victim.to_hex()),
        "error must name the block: {msg}"
    );
    assert!(
        msg.contains(&path.display().to_string()),
        "error must name the block file: {msg}"
    );

    // Verification off: the same read serves the changed bytes, no error.
    let served = read_whole_object(&fs, BUCKET, KEY, false).await.unwrap();
    assert_eq!(served.len(), original.len());
    assert_ne!(served, original);
}

#[tokio::test]
async fn test_ranged_read_boundaries() {
    for (engine, hasher) in matrix() {
        let (fs, _dir) = setup_test_fs(engine, hasher);
        do_test_ranged_read_boundaries(fs).await;
    }
}

/// A ranged `BlockStream` must serve exactly the requested window. The
/// interesting cases are the historic off-by-one victims: an inclusive
/// end on the last byte of a block, and one on the first byte of the
/// block after it -- the latter used to lose its final byte to an exit
/// condition that treated the inclusive end as exclusive.
async fn do_test_ranged_read_boundaries(fs: CasFS) {
    const BUCKET: &str = "test-bucket";
    const KEY: &str = "ranged/block";
    fs.create_bucket(BUCKET).unwrap();

    let data = multi_block_data();
    let len = data.len();
    let payload = data.clone();
    let stream = AsyncByteStream::new(stream::once(async move { Ok(Bytes::from(payload)) }));
    fs.store_single_object_and_meta(BUCKET, KEY, stream, len)
        .await
        .unwrap();

    let block = BLOCK_SIZE as u64;
    let cases = [
        (0, 4095),                           // prefix
        (block - 100, block - 1),            // ends on a block's last byte
        (block - 100, block),                // ends on the next block's first byte
        (block + 10, block + 200),           // inside a later block
        (len as u64 - 4096, len as u64 - 1), // tail
    ];
    for (start, end) in cases {
        let served = read_range(&fs, BUCKET, KEY, start, end).await.unwrap();
        let want = &data[start as usize..=end as usize];
        assert_eq!(
            served.len(),
            want.len(),
            "range {start}-{end} must serve exactly its window's length"
        );
        assert!(
            served.as_slice() == want,
            "range {start}-{end} served the right length but the wrong bytes"
        );
    }
}

/// Reads a byte window the way a ranged S3 GET does, end inclusive.
async fn read_range(
    fs: &CasFS,
    bucket: &str,
    key: &str,
    start: u64,
    end: u64,
) -> io::Result<Vec<u8>> {
    let (_obj, paths) = fs.get_object_paths(bucket, key).unwrap().unwrap();
    let size: usize = paths.iter().map(|(_, size)| size).sum();
    let mut stream = BlockStream::new(
        paths,
        size,
        RangeRequest::new_range(start, end),
        METRICS.clone(),
    );
    let mut out = Vec::new();
    while let Some(chunk) = stream.next().await {
        out.extend_from_slice(&chunk?);
    }
    Ok(out)
}

/// Reads an object the way the S3 GET path does: block paths out of the
/// metadata, a `BlockStream` over them, verification attached when asked.
async fn read_whole_object(
    fs: &CasFS,
    bucket: &str,
    key: &str,
    verify: bool,
) -> io::Result<Vec<u8>> {
    let (obj, paths) = fs.get_object_paths(bucket, key).unwrap().unwrap();
    let size: usize = paths.iter().map(|(_, size)| size).sum();
    let mut stream = BlockStream::new(paths, size, RangeRequest::All, METRICS.clone());
    if verify {
        stream = stream.verified(fs.hasher(), obj.blocks().to_vec());
    }
    let mut out = Vec::with_capacity(size);
    while let Some(chunk) = stream.next().await {
        out.extend_from_slice(&chunk?);
    }
    Ok(out)
}
