use super::*;

/// Counts the two file operations the block protocol distinguishes:
/// exclusive-create writes (a staged temp file, or the overtake rewrite) and
/// renames (a file PLACED at its final path). Everything else is real.
///
/// The two are counted apart because only one of them is deterministic. A
/// write is spent on a bet -- the unstriped dedup lookup -- that a racing
/// writer can lose; a rename happens under the block's stripe, after the
/// in-transaction decision, and so happens exactly once per block that this
/// store did not already have. See
/// [`n_concurrent_puts_of_one_new_block`].
#[derive(Debug)]
struct CountingOps {
    writes: AtomicUsize,
    lands: AtomicUsize,
    real: RealDiskOps,
}

impl CountingOps {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            writes: AtomicUsize::new(0),
            lands: AtomicUsize::new(0),
            real: RealDiskOps,
        })
    }
}

impl BlockDiskOps for CountingOps {
    fn create_dir_all(&self, path: &std::path::Path) -> std::io::Result<()> {
        self.real.create_dir_all(path)
    }
    fn write_new_file(&self, path: &std::path::Path, contents: &[u8]) -> std::io::Result<()> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        self.real.write_new_file(path, contents)
    }
    fn fsync_file(&self, path: &std::path::Path, data_only: bool) -> std::io::Result<()> {
        self.real.fsync_file(path, data_only)
    }
    fn fsync_dir(&self, path: &std::path::Path) -> std::io::Result<()> {
        self.real.fsync_dir(path)
    }
    fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
        self.lands.fetch_add(1, Ordering::SeqCst);
        self.real.rename(from, to)
    }
    fn remove_file(&self, path: &std::path::Path) -> std::io::Result<()> {
        self.real.remove_file(path)
    }
    fn list_dir(&self, path: &std::path::Path) -> std::io::Result<Vec<std::path::PathBuf>> {
        self.real.list_dir(path)
    }
    fn device_of(&self, path: &std::path::Path) -> std::io::Result<Option<u64>> {
        self.real.device_of(path)
    }
}

/// N concurrent PUTs of one brand-new block: exactly one file is PLACED,
/// at quiesce rc == N, and the wasted work is bounded by one staged temp
/// file per writer.
///
/// # Why the write count is a bound and the placement count is not
///
/// The dedup lookup that decides whether to spend a file write is
/// deliberately unstriped and outside any transaction (`resolve_chunk`):
/// it decides only whether to spend a 1 MiB write, and the authoritative
/// insert-vs-bump decision is remade later, inside the batch's
/// transaction, under the block's stripe, against whatever state is
/// current then. So a writer whose lookup runs before the winner's
/// transaction commits misses, stages a temp file of its own -- and then,
/// under the stripe, finds the record live, discards that temp file
/// UNRENAMED, and bumps. ADR 0010 names this interleaving in as many words
/// ("Two concurrent batches both contain block X, both new"): the loser's
/// file is surplus, and dropping it is the whole compensation.
///
/// The write count is therefore a race outcome -- one write per writer
/// whose lookup lost -- bounded by one per request, because a request's
/// entry for a block either stages (and never rewrites) or dedups (and
/// rewrites only if the record it deduped against is gone by commit time,
/// which needs a concurrent DELETE this test does not have). Asserting
/// `== 1` was asserting a scheduler outcome: at idle it never missed, but
/// with 32 busy loops on 32 cores an instrumented replay of this exact
/// scenario spent a second write in 31 of 1500 iterations (2%, 29 of them
/// at two writes and one at three) -- which is why the pre-push hook, whose
/// clippy build saturates the cores just before the tests run, was the best
/// reproducer in the tree.
///
/// What is NOT a race, and is asserted exactly: rc == N, and one file
/// placed. Both hold because every rc mutation and every rename happens
/// under this block's one stripe, so the writers are a queue -- the first
/// lands the file and inserts at rc 1, and each one after it sees the
/// committed record and bumps. All 31 double-write iterations landed
/// exactly one file and finished at rc exactly 16: the surplus is a wasted
/// temp write, never a wasted or a missing reference.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn n_concurrent_puts_of_one_new_block() {
    const N: usize = 16;
    let dir = tempdir().unwrap();
    let ops = CountingOps::new();
    let (shared, namespaces) = store_with_namespaces(dir.path(), Some(ops.clone()), N);

    let data = b"one block, many writers".repeat(100).to_vec();
    let id = shared.hasher().hash(&data);

    let mut tasks = Vec::new();
    for (i, fs) in namespaces.iter().enumerate() {
        fs.create_bucket("b").unwrap();
        let fs = fs.clone();
        let data = data.clone();
        tasks.push(tokio::spawn(async move {
            put(&fs, "b", &format!("k-{i}"), data).await;
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }

    // The exact half first, so a run that breaks the accounting says so
    // rather than being pre-empted by the bounded half.
    let block = shared
        .block_tree()
        .get_block(id.as_slice())
        .unwrap()
        .expect("record exists");
    assert_eq!(block.rc(), N, "exact rc at quiesce");
    assert!(
        block
            .disk_path(&id, namespaces[0].fs_root().clone())
            .is_file()
    );
    assert_eq!(
        ops.lands.load(Ordering::SeqCst),
        1,
        "exactly one file placed for one block, however many writers"
    );

    // The bounded half: surplus staged files are allowed, and bounded.
    let writes = ops.writes.load(Ordering::SeqCst);
    assert!(
        (1..=N).contains(&writes),
        "one write per writer that lost the dedup lookup, at most: got {writes}"
    );
}

/// PUT/DELETE storm on one shared block from two tasks (the defect-5
/// interleaving: dedup bumps racing last-ref deletes). At quiesce nothing
/// references the block: record gone, file gone -- and no iteration ever
/// saw a torn state (a GET after every PUT succeeds).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn put_storm_vs_delete_last_ref_loop() {
    let dir = tempdir().unwrap();
    let (shared, namespaces) = store_with_namespaces(dir.path(), None, 2);
    let data = b"contended block".repeat(200).to_vec();
    let id = shared.hasher().hash(&data);

    let mut tasks = Vec::new();
    for fs in &namespaces {
        fs.create_bucket("b").unwrap();
        let fs = fs.clone();
        let data = data.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..STORM_ITERATIONS {
                let key = format!("k-{i}");
                put(&fs, "b", &key, data.clone()).await;
                // The object must be complete and readable between its PUT
                // and its DELETE, whatever the other task does to the
                // shared block: never a record without a complete file.
                let (_, paths) = fs.get_object_paths("b", &key).unwrap().expect("just PUT");
                for (path, size) in paths {
                    let bytes = std::fs::read(&path).expect("live block file must exist");
                    assert_eq!(bytes.len(), size, "complete file, never partial");
                }
                fs.delete_object("b", &key).await.unwrap();
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }

    // Quiesce: every reference deleted; exact accounting demands the
    // record is gone and the file unlinked.
    assert!(
        shared
            .block_tree()
            .get_block(id.as_slice())
            .unwrap()
            .is_none(),
        "no reference left, record must be gone"
    );
    assert!(
        !crate::metastore::block_disk_path(&id, 1, namespaces[0].fs_root().clone()).exists(),
        "no reference left, file must be unlinked"
    );
}

/// Dedup-bump vs delete tight loop around a pinned live reference: after
/// the storm the pinned key still reads back byte-for-byte and the rc is
/// exactly 1. Any torn bump or decrement shows up as a wrong rc.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dedup_bump_vs_delete_keeps_exact_rc() {
    let dir = tempdir().unwrap();
    let (shared, namespaces) = store_with_namespaces(dir.path(), None, 2);
    let data = b"pinned content".repeat(300).to_vec();
    let id = shared.hasher().hash(&data);

    namespaces[0].create_bucket("b").unwrap();
    namespaces[1].create_bucket("b").unwrap();
    put(&namespaces[0], "b", "pinned", data.clone()).await;

    let mut tasks = Vec::new();
    for fs in &namespaces {
        let fs = fs.clone();
        let data = data.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..STORM_ITERATIONS {
                let key = format!("churn-{i}");
                put(&fs, "b", &key, data.clone()).await;
                fs.delete_object("b", &key).await.unwrap();
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }

    let block = shared
        .block_tree()
        .get_block(id.as_slice())
        .unwrap()
        .expect("pinned reference keeps the record alive");
    assert_eq!(block.rc(), 1, "exact rc after the churn");
    let path = block.disk_path(&id, namespaces[0].fs_root().clone());
    assert_eq!(
        shared.hasher().hash(&std::fs::read(&path).unwrap()),
        id,
        "pinned block still reads back byte-for-byte"
    );
}

/// Concurrent double-DELETE of one key: exactly one of the deletes
/// decrements (take-object is an atomic pair), so the sibling key's
/// reference survives with rc exactly 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_double_delete_decrements_once() {
    let dir = tempdir().unwrap();
    let (shared, namespaces) = store_with_namespaces(dir.path(), None, 1);
    let fs = namespaces[0].clone();
    fs.create_bucket("b").unwrap();

    let data = b"double delete race".repeat(150).to_vec();
    let id = shared.hasher().hash(&data);
    put(&fs, "b", "victim", data.clone()).await;
    put(&fs, "b", "survivor", data.clone()).await;
    assert_eq!(
        shared
            .block_tree()
            .get_block(id.as_slice())
            .unwrap()
            .unwrap()
            .rc(),
        2
    );

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let fs = fs.clone();
        tasks.push(tokio::spawn(async move {
            fs.delete_object("b", "victim").await.unwrap();
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }

    let block = shared
        .block_tree()
        .get_block(id.as_slice())
        .unwrap()
        .expect("survivor's reference must hold the record");
    assert_eq!(block.rc(), 1, "eight racing DELETEs decrement exactly once");
    assert!(
        block.disk_path(&id, fs.fs_root().clone()).is_file(),
        "survivor's block file must not be unlinked"
    );
}

/// K concurrent PUTs onto a degraded record (ADR 0005): the degraded flag
/// makes the record absent for dedup, so the writers race to be the one
/// that heals it. Whoever wins writes the file and clears the flag; the
/// rest dedup-bump the healed record. At quiesce the flag is clear, the
/// file is present and hash-valid at the depth the record now names, and
/// rc is exactly the planted holders plus one per PUT -- one heal, K-1
/// bumps, never a second heal that would restart the count.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn degraded_record_heals_once_under_concurrent_puts() {
    /// Concurrent writers of the same content.
    const K: usize = 8;
    /// Holders the degraded record still accounts for.
    const N: usize = 3;

    let dir = tempdir().unwrap();
    let (shared, namespaces) = store_with_namespaces(dir.path(), None, K);
    for fs in &namespaces {
        fs.create_bucket("b").unwrap();
    }

    for iteration in 0..STORM_ITERATIONS {
        // Fresh content per iteration: each one is a fresh race.
        let data = format!("degraded heal {iteration} ")
            .repeat(64)
            .into_bytes();
        let id = shared.hasher().hash(&data);
        crate::cas::crash_fixtures::plant_degraded_record(&shared, id, 1, N, data.len());

        let mut tasks = Vec::new();
        for (i, fs) in namespaces.iter().enumerate() {
            let fs = fs.clone();
            let data = data.clone();
            tasks.push(tokio::spawn(async move {
                put(&fs, "b", &format!("k-{iteration}-{i}"), data).await;
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }

        let block = shared
            .block_tree()
            .get_block(id.as_slice())
            .unwrap()
            .expect("the record survives the heal");
        assert!(!block.is_degraded(), "the heal must clear the flag");
        assert_eq!(block.rc(), N + K, "one heal plus K-1 dedup bumps");

        let path = block.disk_path(&id, namespaces[0].fs_root().clone());
        let bytes = std::fs::read(&path).expect("the healed record must have its file");
        assert_eq!(bytes.len(), data.len(), "complete file, never partial");
        assert_eq!(
            shared.hasher().hash(&bytes),
            id,
            "the healed file is the block it is named after"
        );
    }
}
