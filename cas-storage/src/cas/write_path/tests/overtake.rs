use super::*;

/// Two concurrent batches both containing block X, both new (ADR 0010's
/// "who wins?").
///
/// Both stage their own temp file -- neither saw the other's record,
/// because neither had committed one. The stripe serializes them at the
/// transaction: the first insert wins, the second's in-tx decision sees
/// the committed record and becomes a bump, and its surplus temp file is
/// dropped rather than renamed over a live block. At quiesce: one insert,
/// one bump, rc exactly 2, one file, no residue.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_concurrent_batches_over_one_new_block_insert_once_and_bump_once() {
    const ROUNDS: usize = 30;
    const CAP: usize = 4;

    let dir = tempdir().unwrap();
    let (shared, fs) = store_with_cap(dir.path(), Some(CAP), None);
    let fs = Arc::new(fs);

    for round in 0..ROUNDS {
        // The shared block sits among distinct ones, so each request is a
        // real batch and the shared block is not its only member.
        let shared_block = distinct_blocks(&format!("shared-{round}"), 1);
        let mut left = distinct_blocks(&format!("left-{round}"), 2);
        left.extend_from_slice(&shared_block);
        let mut right = distinct_blocks(&format!("right-{round}"), 2);
        right.extend_from_slice(&shared_block);

        let id = shared.hasher().hash(&shared_block);

        let a = {
            let fs = fs.clone();
            tokio::spawn(async move { put(&fs, &format!("a-{round}"), left).await })
        };
        let b = {
            let fs = fs.clone();
            tokio::spawn(async move { put(&fs, &format!("b-{round}"), right).await })
        };
        a.await.unwrap();
        b.await.unwrap();

        let block = shared
            .block_tree()
            .get_block(id.as_slice())
            .unwrap()
            .expect("the contended block must have exactly one record");
        assert_eq!(block.rc(), 2, "round {round}: one insert plus one bump");

        // Exactly one file, at the depth the record names, with the right
        // bytes -- and no second copy at any other depth.
        let path = block.disk_path(&id, fs.fs_root().clone());
        assert_eq!(
            shared.hasher().hash(&std::fs::read(&path).unwrap()),
            id,
            "round {round}: the surviving file is the block"
        );
        for depth in 1..=4u8 {
            if depth != block.depth() {
                assert!(
                    !block_disk_path(&id, depth, fs.fs_root().clone()).exists(),
                    "round {round}: the loser must not leave an off-depth copy"
                );
            }
        }
        assert_eq!(
            std::fs::read_dir(fs.fs_root().join(".tmp"))
                .unwrap()
                .count(),
            0,
            "round {round}: the surplus temp file must be removed"
        );
    }
}

/// A request whose dedup lookup is overtaken: the record it deduped
/// against is deleted before its batch commits.
///
/// That lookup is the one thing in a batch that can go stale, and this is
/// the shape that makes it go stale on purpose. The batch has to notice
/// under the stripe and write the block after all, rather than committing
/// a record whose file was just unlinked. Run as a storm because the
/// window is small; a wrong implementation shows up as a record with no
/// file, which the read-back catches.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dedup_hit_overtaken_by_a_delete_still_lands_its_bytes() {
    const ROUNDS: usize = 40;

    let dir = tempdir().unwrap();
    let (shared, fs) = store_with_cap(dir.path(), None, None);
    let fs = Arc::new(fs);

    for round in 0..ROUNDS {
        let content = distinct_blocks(&format!("overtaken-{round}"), 1);
        let id = shared.hasher().hash(&content);

        // The reference the racing DELETE will take: the last one, so the
        // record and the file both go.
        put(&fs, &format!("seed-{round}"), content.clone()).await;

        let deleter = {
            let fs = fs.clone();
            tokio::spawn(async move { fs.delete_object(BUCKET, &format!("seed-{round}")).await })
        };
        let writer = {
            let fs = fs.clone();
            let content = content.clone();
            tokio::spawn(async move { put(&fs, &format!("writer-{round}"), content).await })
        };
        deleter.await.unwrap().unwrap();
        writer.await.unwrap();

        // Whichever order they landed in, the writer's object is
        // readable: its record exists and its file is there, whole.
        let block = shared
            .block_tree()
            .get_block(id.as_slice())
            .unwrap()
            .unwrap_or_else(|| panic!("round {round}: the writer's block must be recorded"));
        let path = block.disk_path(&id, fs.fs_root().clone());
        let bytes = std::fs::read(&path).unwrap_or_else(|e| {
            panic!("round {round}: a committed record must have its file: {e}")
        });
        assert_eq!(
            shared.hasher().hash(&bytes),
            id,
            "round {round}: complete file, never partial"
        );

        fs.delete_object(BUCKET, &format!("writer-{round}"))
            .await
            .unwrap();
    }
}

/// Ops whose exclusive-create write signals entry and waits for a
/// release, so a test can park a request between its dedup lookup and its
/// batch commit -- the one window in which that lookup can go stale.
#[derive(Debug)]
struct GatedStageOps {
    real: RealDiskOps,
    entered: std::sync::mpsc::Sender<()>,
    release: StdMutex<std::sync::mpsc::Receiver<()>>,
}

impl BlockDiskOps for GatedStageOps {
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.real.create_dir_all(path)
    }
    fn write_new_file(&self, path: &Path, contents: &[u8]) -> io::Result<()> {
        self.entered.send(()).ok();
        self.release.lock().unwrap().recv().ok();
        self.real.write_new_file(path, contents)
    }
    fn fsync_file(&self, path: &Path, data_only: bool) -> io::Result<()> {
        self.real.fsync_file(path, data_only)
    }
    fn fsync_dir(&self, path: &Path) -> io::Result<()> {
        self.real.fsync_dir(path)
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.real.rename(from, to)
    }
    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.real.remove_file(path)
    }
    fn list_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        self.real.list_dir(path)
    }
    fn device_of(&self, path: &Path) -> io::Result<Option<u64>> {
        self.real.device_of(path)
    }
}

/// The overtake, arranged rather than raced: the deleted-underneath case,
/// deterministically.
///
/// A request of two blocks, `[X, N]`. X dedup-hits a committed record, so
/// no file is written for it and only its bytes are held. N is new, so it
/// stages -- and the gate parks the request right there, holding no
/// stripes, which is exactly what lets the DELETE through. The DELETE
/// takes X's last reference: record removed, file unlinked. Then the batch
/// resumes.
///
/// What must happen: the batch notices under X's stripe that the record it
/// deduped against is gone, writes X from the bytes it kept, and commits a
/// record whose file is really there. What must NOT happen: a record for X
/// with no file, which is silent data loss dressed as a successful PUT.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dedup_hit_deleted_mid_batch_is_written_from_the_bytes_it_kept() {
    let dir = tempdir().unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let ops = Arc::new(GatedStageOps {
        real: RealDiskOps,
        entered: entered_tx,
        release: StdMutex::new(release_rx),
    });
    let (shared, fs) = store_with_cap(dir.path(), None, Some(ops));
    let fs = Arc::new(fs);

    let x = distinct_blocks("deduped-away", 1);
    let n = distinct_blocks("brand-new", 1);
    let x_id = shared.hasher().hash(&x);

    // Seed the record the request will dedup against, letting its own
    // single staged write through the gate.
    let seed = {
        let fs = fs.clone();
        let x = x.clone();
        tokio::spawn(async move { put(&fs, "seed", x).await })
    };
    entered_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("the seed must stage its block");
    release_tx.send(()).unwrap();
    seed.await.unwrap();
    assert_eq!(
        shared
            .block_tree()
            .get_block(x_id.as_slice())
            .unwrap()
            .unwrap()
            .rc(),
        1
    );

    // The request: X dedup-hits (no write), N stages and parks.
    let mut content = x.clone();
    content.extend_from_slice(&n);
    let writer = {
        let fs = fs.clone();
        tokio::spawn(async move { put(&fs, "writer", content).await })
    };
    entered_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("the request must reach the new block's stage");

    // Parked with no stripes held, so the DELETE goes through and takes
    // X's last reference with it.
    fs.delete_object(BUCKET, "seed").await.unwrap();
    assert!(
        shared
            .block_tree()
            .get_block(x_id.as_slice())
            .unwrap()
            .is_none(),
        "the premise: the record the request deduped against is gone"
    );

    // The batch resumes, and has to notice. Two tokens: one frees the
    // parked stage of N, the second is consumed by the write of X that
    // the batch is obliged to perform now. That second token being
    // NEEDED is itself the assertion -- without the fallback the request
    // would sail through on one, and commit a record for a block whose
    // file it never wrote.
    release_tx.send(()).unwrap();
    release_tx.send(()).unwrap();
    writer.await.unwrap();

    let block = shared
        .block_tree()
        .get_block(x_id.as_slice())
        .unwrap()
        .expect("the request's own reference must have recreated the record");
    assert_eq!(block.rc(), 1, "one holder: the request that survived");
    let path = block.disk_path(&x_id, fs.fs_root().clone());
    let bytes = std::fs::read(&path)
        .expect("a committed record must have its file -- this is the loss case");
    assert_eq!(
        shared.hasher().hash(&bytes),
        x_id,
        "and the file must be the block it is named after"
    );
    assert_eq!(
        std::fs::read_dir(fs.fs_root().join(".tmp"))
            .unwrap()
            .count(),
        0,
        "no temp residue"
    );
}

/// A failed request leaves no temp residue: everything it staged is
/// unlinked on the way out, and nothing was renamed.
#[tokio::test]
async fn a_request_that_fails_mid_stream_discards_what_it_staged() {
    let dir = tempdir().unwrap();
    let (shared, fs) = store_with_cap(dir.path(), Some(64), None);

    // Enough blocks to fill a batch's worth of staging, then an error
    // before the request end that would have flushed it.
    let good = distinct_blocks("doomed", 3);
    let stream = AsyncByteStream::new(futures::stream::iter(vec![
        Ok(Bytes::from(good)),
        Err(io::Error::other("the client went away")),
    ]));

    let err = store_object(&fs, BUCKET, b"never", stream)
        .await
        .expect_err("a stream error must fail the request");
    assert!(err.to_string().contains("the client went away"), "{err}");

    assert_eq!(
        shared.block_tree().len().unwrap(),
        0,
        "a request that never acked records nothing"
    );
    assert_eq!(
        std::fs::read_dir(fs.fs_root().join(".tmp"))
            .unwrap()
            .count(),
        0,
        "and leaves no temp residue behind"
    );
}
