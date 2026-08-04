use super::*;

/// Records appear in batch-sized groups, and never before their files.
///
/// This is the ADR's claim made observable. An 8-block object at a cap of
/// 4 lands as two batches, and at every rename the tree holds a multiple
/// of the cap: 0 for all four renames of the first batch (its records do
/// not exist yet), 4 for all four of the second. A per-block protocol
/// would count 0,1,2,3,4,5,6,7 instead, and a batch that committed before
/// renaming would count 4,4,4,4,8,8,8,8.
#[tokio::test]
async fn records_commit_once_per_batch_and_never_before_the_files_land() {
    const CAP: usize = 4;
    const BLOCKS: usize = 8;

    let dir = tempdir().unwrap();
    let ops = CommitObservingOps::new();
    let (shared, fs) = store_with_cap(dir.path(), Some(CAP), Some(ops.clone()));
    ops.watch(shared.block_tree());

    put(&fs, "big", distinct_blocks("batched", BLOCKS)).await;

    assert_eq!(
        ops.observations(),
        vec![0, 0, 0, 0, 4, 4, 4, 4],
        "records must become visible one batch at a time, after the renames"
    );
    assert_eq!(shared.block_tree().len().unwrap(), BLOCKS);
}

/// The cap does not change what the store ends up holding: a request
/// split into many batches, into one batch, or into one block per batch
/// all leave the same object with the same blocks.
#[tokio::test]
async fn the_cap_changes_the_batching_and_nothing_else() {
    let content = distinct_blocks("cap-invariance", 5);

    let mut ids_per_cap = Vec::new();
    for cap in [1usize, 2, 5, 64] {
        let dir = tempdir().unwrap();
        let (shared, fs) = store_with_cap(dir.path(), Some(cap), None);
        let obj = put(&fs, "same", content.clone()).await;

        assert_eq!(obj.blocks().len(), 5, "cap {cap}");
        assert_eq!(shared.block_tree().len().unwrap(), 5, "cap {cap}");
        for id in obj.blocks() {
            let block = shared
                .block_tree()
                .get_block(id.as_slice())
                .unwrap()
                .unwrap_or_else(|| panic!("cap {cap}: every block must be recorded"));
            assert_eq!(block.rc(), 1, "cap {cap}");
            let path = block.disk_path(id, fs.fs_root().clone());
            assert_eq!(
                shared.hasher().hash(&std::fs::read(&path).unwrap()),
                *id,
                "cap {cap}: the file is the block it is named after"
            );
        }
        assert_eq!(
            std::fs::read_dir(fs.fs_root().join(".tmp"))
                .unwrap()
                .count(),
            0,
            "cap {cap}: no temp residue"
        );
        ids_per_cap.push(obj.blocks().to_vec());
    }

    for ids in &ids_per_cap {
        assert_eq!(
            ids, &ids_per_cap[0],
            "the block list cannot depend on the cap"
        );
    }
}

/// A block appearing twice in ONE request is one insert plus one bump --
/// the accumulator dedups against itself, exactly as two requests dedup
/// against committed state.
#[tokio::test]
async fn a_block_repeated_in_one_request_is_one_file_and_two_references() {
    let dir = tempdir().unwrap();
    let ops = CommitObservingOps::new();
    let (shared, fs) = store_with_cap(dir.path(), None, Some(ops.clone()));

    // Two identical blocks around one distinct one: the repeat is not
    // adjacent, so a "same as the last block" check would miss it.
    let repeated = distinct_blocks("repeated", 1);
    let other = distinct_blocks("other", 1);
    let mut content = repeated.clone();
    content.extend_from_slice(&other);
    content.extend_from_slice(&repeated);

    let obj = put(&fs, "twice", content).await;

    assert_eq!(obj.blocks().len(), 3, "three occurrences in the block list");
    assert_eq!(obj.blocks()[0], obj.blocks()[2], "the first and last agree");
    assert_eq!(
        ops.writes.load(Ordering::SeqCst),
        2,
        "one file per distinct block, however often it occurs"
    );

    let repeated_id = obj.blocks()[0];
    assert_eq!(
        shared
            .block_tree()
            .get_block(repeated_id.as_slice())
            .unwrap()
            .unwrap()
            .rc(),
        2,
        "every occurrence holds its own reference"
    );
    assert_eq!(
        shared
            .block_tree()
            .get_block(obj.blocks()[1].as_slice())
            .unwrap()
            .unwrap()
            .rc(),
        1
    );

    // And the lifecycle closes exactly: deleting the object drops both.
    fs.delete_object(BUCKET, "twice").await.unwrap();
    assert!(
        shared
            .block_tree()
            .get_block(repeated_id.as_slice())
            .unwrap()
            .is_none(),
        "two references in, two references out"
    );
}

/// The one-block PUT is a one-block batch: today's shape, unchanged.
#[tokio::test]
async fn a_single_block_put_is_a_one_block_batch() {
    let dir = tempdir().unwrap();
    let ops = CommitObservingOps::new();
    let (shared, fs) = store_with_cap(dir.path(), None, Some(ops.clone()));
    ops.watch(shared.block_tree());

    let obj = put(&fs, "small", b"one small block".repeat(64).to_vec()).await;

    assert_eq!(obj.blocks().len(), 1);
    assert_eq!(
        ops.observations(),
        vec![0],
        "one rename, and no record existed when it happened"
    );
    assert_eq!(ops.writes.load(Ordering::SeqCst), 1);
}

/// A few hundred MiB through the real write path at real `fsync`
/// durability, one cap against another. Ignored by default.
///
/// NOT the acceptance benchmark -- that is the 16 GiB A/B on the rig
/// (`tests/real/tools/durability-bench.sh`), which owns the regression
/// floor and the hardware it means anything on. This is the smoke check
/// that says the batch path works end to end under real syncs and that a
/// bigger cap does what it is for, in seconds rather than hours:
///
/// ```text
/// cargo test -p cas-storage --release -- --ignored --nocapture batch_smoke
/// ```
///
/// A cap of 1 is the pre-ADR-0010 cadence (one commit and one journal
/// fsync per block), so the two rows are the change this ADR is about.
///
/// The store goes under `target/`, deliberately, and NOT in `$TMPDIR`:
/// `/tmp` is tmpfs on most Linux boxes, where fsync costs nothing and
/// both rows come back identical and meaningless. A benchmark that
/// silently measures a RAM disk is worse than no benchmark.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "writes a few hundred MiB with real fsyncs; run explicitly"]
async fn batch_smoke_ab() {
    /// Blocks per object, so 256 MiB per row at the 1 MiB block size.
    const BLOCKS: usize = 256;

    let scratch = Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/batch-smoke");
    std::fs::create_dir_all(&scratch).unwrap();

    for cap in [1usize, 64] {
        let dir = tempfile::Builder::new()
            .prefix("ab-")
            .tempdir_in(&scratch)
            .unwrap();
        // Fsync, not Buffer: the whole point is to pay the real syncs.
        let opts = StoreOptions {
            inline_metadata_size: Some(1),
            durability: Durability::Fsync,
            max_blocks_per_commit: Some(cap),
            ..StoreOptions::default()
        };
        let shared = Arc::new(
            SharedBlockStore::new(
                dir.path().join("meta/blocks"),
                dir.path().join("blocks"),
                opts,
            )
            .unwrap(),
        );
        let fs = CasFS::new(
            dir.path().join("meta/ns"),
            shared.clone(),
            SharedMetrics::default(),
            opts,
        )
        .unwrap();
        fs.create_bucket(BUCKET).unwrap();

        // Distinct content per row: no row may dedup against another's.
        let content = distinct_blocks(&format!("smoke-{cap}"), BLOCKS);
        let bytes = content.len();

        let started = std::time::Instant::now();
        let obj = put(&fs, "giant", content).await;
        let elapsed = started.elapsed();

        assert_eq!(obj.blocks().len(), BLOCKS);
        assert_eq!(shared.block_tree().len().unwrap(), BLOCKS);
        #[allow(clippy::cast_precision_loss)] // a printed rate, not a value
        let rate = (bytes as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64();
        println!(
            "max_blocks_per_commit={cap:>3}: {:>4} MiB in {:>6.2}s = {rate:>7.1} MiB/s",
            bytes / (1024 * 1024),
            elapsed.as_secs_f64(),
        );

        // Correctness first, speed second: every block readable and
        // exactly what it claims to be.
        for id in obj.blocks() {
            let block = shared
                .block_tree()
                .get_block(id.as_slice())
                .unwrap()
                .unwrap();
            let path = block.disk_path(id, fs.fs_root().clone());
            assert_eq!(shared.hasher().hash(&std::fs::read(&path).unwrap()), *id);
        }
    }
}

/// The batch is per REQUEST, not per object: a multipart part is its own
/// durability unit, and two parts of one upload commit separately.
#[tokio::test]
async fn each_part_upload_is_its_own_batch() {
    let dir = tempdir().unwrap();
    let ops = CommitObservingOps::new();
    let (shared, fs) = store_with_cap(dir.path(), Some(64), Some(ops.clone()));
    ops.watch(shared.block_tree());

    for part in 0..2 {
        let data = distinct_blocks(&format!("part-{part}"), 2);
        let stream =
            AsyncByteStream::new(futures::stream::once(async move { Ok(Bytes::from(data)) }));
        store_object(&fs, BUCKET, b"multi", stream).await.unwrap();
    }

    assert_eq!(
        ops.observations(),
        vec![0, 0, 2, 2],
        "each request commits its own blocks, as one group"
    );
}
