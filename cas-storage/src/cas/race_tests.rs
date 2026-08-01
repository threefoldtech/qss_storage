//! Race-stress and cancellation tests for the ADR 0006 block protocol
//! (plan component 9).
//!
//! These tests are written against the INVARIANTS, not the
//! implementation: never a record without a complete file, never an
//! unlinked live block, exact refcounts at quiesce. Iteration counts are
//! sized for CI; when hunting a suspected race, raise `STORM_ITERATIONS`
//! and run under `--release` locally.

use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use futures::stream;
use tempfile::tempdir;

use super::block_disk::{BlockDiskOps, RealDiskOps};
use super::byte_stream::AsyncByteStream;
use super::delete_path::release_blocks;
use super::fs::CasFS;
use super::shared_block_store::SharedBlockStore;
use crate::metastore::{BlockId, Durability};
use crate::metrics::SharedMetrics;

const STORM_ITERATIONS: usize = 60;

/// One store, `n` namespaces over it. Returns the shared store handle and
/// the namespaces.
fn store_with_namespaces(
    dir: &std::path::Path,
    ops: Option<Arc<dyn BlockDiskOps>>,
    n: usize,
) -> (Arc<SharedBlockStore>, Vec<Arc<CasFS>>) {
    let mut shared = SharedBlockStore::new(
        dir.join("meta/blocks"),
        dir.join("blocks"),
        super::StorageEngine::Fjall,
        Some(1),
        Some(Durability::Buffer),
        None,
        None,
        None,
        None,
    )
    .unwrap();
    if let Some(ops) = ops {
        shared.set_disk_ops(ops);
    }
    let shared = Arc::new(shared);
    let namespaces = (0..n)
        .map(|i| {
            Arc::new(
                CasFS::new(
                    dir.join(format!("meta/ns-{i}")),
                    shared.clone(),
                    SharedMetrics::default(),
                    super::StorageEngine::Fjall,
                    Some(1),
                    Some(Durability::Buffer),
                    false,
                )
                .unwrap(),
            )
        })
        .collect();
    (shared, namespaces)
}

fn byte_stream(data: Vec<u8>) -> (AsyncByteStream, usize) {
    let len = data.len();
    (
        AsyncByteStream::new(stream::once(async move { Ok(Bytes::from(data)) })),
        len,
    )
}

async fn put(fs: &CasFS, bucket: &str, key: &str, data: Vec<u8>) {
    let (stream, len) = byte_stream(data);
    fs.store_single_object_and_meta(bucket, key, stream, len)
        .await
        .unwrap();
}

/// The invariant every release leg below shares: the record carries exactly
/// the expected count, and the block file exists IFF something still
/// references it. Anything else is a torn rc -- a leaked file under a dead
/// record, or a live record whose bytes were unlinked out from under it.
fn assert_block_state(
    shared: &SharedBlockStore,
    id: BlockId,
    expected_rc: usize,
    root: &std::path::Path,
) {
    match shared.block_tree().get_block(id.as_slice()).unwrap() {
        Some(block) => {
            assert_eq!(block.rc(), expected_rc, "exact rc at quiesce");
            assert!(expected_rc > 0, "a live record must carry a reference");
            let path = block.disk_path(&id, root.to_path_buf());
            let bytes = std::fs::read(&path).expect("a referenced block must keep its file");
            assert_eq!(
                shared.hasher().hash(&bytes),
                id,
                "the file is the block it is named after"
            );
        }
        None => {
            assert_eq!(
                expected_rc, 0,
                "the record vanished with references outstanding"
            );
            assert!(
                !crate::metastore::block_disk_path(&id, 1, root.to_path_buf()).exists(),
                "the last release must unlink the file"
            );
        }
    }
}

/// One release racing K concurrent PUTs of the same content. The release's
/// striped decrement and each PUT's striped bump serialize on the one
/// stripe, so the arithmetic is exact whatever the interleaving: one seed
/// reference, plus K, minus the one released, is K.
///
/// The extreme interleaving is the interesting one: if the release lands
/// first it takes the LAST reference, removing the record and unlinking the
/// file inside its stripe hold -- and the PUTs that follow must then rebuild
/// the block from rc 1 rather than resurrecting a half-dead record.
///
/// (The seed PUT stands in for whichever record held that reference; a real
/// caller -- abort, from component 4 -- removes its part record before
/// calling. This arm pins the rc protocol, not the caller's ordering.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn release_blocks_vs_concurrent_puts_keeps_exact_rc() {
    /// Concurrent writers of the same content, one per namespace.
    const K: usize = 8;

    let dir = tempdir().unwrap();
    let (shared, namespaces) = store_with_namespaces(dir.path(), None, K);
    let metrics = SharedMetrics::default();
    for fs in &namespaces {
        fs.create_bucket("b").unwrap();
    }

    for iteration in 0..STORM_ITERATIONS {
        // Fresh content per iteration: each one is a fresh race.
        let data = format!("release race {iteration} ").repeat(64).into_bytes();
        let id = shared.hasher().hash(&data);

        // Seed the reference the release will drop.
        put(
            &namespaces[0],
            "b",
            &format!("seed-{iteration}"),
            data.clone(),
        )
        .await;

        let mut tasks = Vec::new();
        {
            let shared = shared.clone();
            let metrics = metrics.clone();
            tasks.push(tokio::spawn(async move {
                release_blocks(&shared, &metrics, &[id]).await;
            }));
        }
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

        assert_block_state(&shared, id, K, namespaces[0].fs_root());
    }
}

/// The same arithmetic in both orders, run sequentially so the interleaving
/// is not left to the scheduler: release-then-PUTs and PUTs-then-release
/// must land on the identical final state. The first order also pins the
/// rc-zero side of the invariant -- releasing the last reference removes the
/// record AND unlinks the file, before the PUTs rebuild it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn release_blocks_lands_the_same_state_in_either_order() {
    const K: usize = 4;

    let dir = tempdir().unwrap();
    let (shared, namespaces) = store_with_namespaces(dir.path(), None, K);
    let metrics = SharedMetrics::default();
    for fs in &namespaces {
        fs.create_bucket("b").unwrap();
    }

    for release_first in [true, false] {
        let leg = if release_first {
            "release-first"
        } else {
            "puts-first"
        };
        let data = format!("order {leg} ").repeat(64).into_bytes();
        let id = shared.hasher().hash(&data);

        put(&namespaces[0], "b", &format!("seed-{leg}"), data.clone()).await;
        assert_block_state(&shared, id, 1, namespaces[0].fs_root());

        if release_first {
            release_blocks(&shared, &metrics, &[id]).await;
            // That was the last reference: nothing left, file included.
            assert_block_state(&shared, id, 0, namespaces[0].fs_root());
        }

        for (i, fs) in namespaces.iter().enumerate() {
            put(fs, "b", &format!("k-{leg}-{i}"), data.clone()).await;
        }

        if !release_first {
            release_blocks(&shared, &metrics, &[id]).await;
        }

        assert_block_state(&shared, id, K, namespaces[0].fs_root());
    }
}

/// An overwrite storm on ONE key with the SAME content (ADR 0008).
///
/// Two rules meet on every write and cancel: each dedup hit bumps the shared
/// block (ADR 0006) and each overwrite releases the record it displaced. The
/// transactions serialize, so whatever the interleaving the storm is a chain
/// -- each writer displaces exactly the record before it and releases
/// exactly that -- and the count lands on the survivor's occurrences.
///
/// The pinned sibling reference makes both failure directions visible in one
/// assertion: a release that never happened shows up as an rc above the
/// truth, a release that happened twice as a record freed while the pinned
/// object still names it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overwrite_storm_on_one_key_keeps_exact_rc() {
    /// Concurrent overwriters of one key.
    const K: usize = 8;

    let dir = tempdir().unwrap();
    let (shared, namespaces) = store_with_namespaces(dir.path(), None, 1);
    let fs = namespaces[0].clone();
    fs.create_bucket("b").unwrap();

    let data = b"one key, many overwrites".repeat(100).to_vec();
    let id = shared.hasher().hash(&data);

    // The reference the storm never displaces.
    put(&fs, "b", "pinned", data.clone()).await;
    assert_block_state(&shared, id, 1, fs.fs_root());

    let mut tasks = Vec::new();
    for _ in 0..K {
        let fs = fs.clone();
        let data = data.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..STORM_ITERATIONS {
                put(&fs, "b", "k", data.clone()).await;
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }

    // K * STORM_ITERATIONS writes of one key leave exactly one object, and
    // exactly its occurrences on top of the pinned one.
    assert_block_state(&shared, id, 2, fs.fs_root());

    // And the whole lifecycle closes: no residue for fsck to collect.
    fs.delete_object("b", "k").await.unwrap();
    assert_block_state(&shared, id, 1, fs.fs_root());
    fs.delete_object("b", "pinned").await.unwrap();
    assert_block_state(&shared, id, 0, fs.fs_root());
}

/// The same storm with DISTINCT content per writer, where every overwrite
/// takes a block's last reference rather than cancelling against a bump.
///
/// Exactly one object survives each round, holding exactly one reference;
/// every content it replaced is gone, record and file. A missed release
/// leaves a record behind, a double release takes the survivor's own block
/// -- the round asserts against both.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_overwrite_storm_of_distinct_content_leaves_only_the_survivor() {
    /// Concurrent overwriters, each with its own content.
    const K: usize = 6;

    let dir = tempdir().unwrap();
    let (shared, namespaces) = store_with_namespaces(dir.path(), None, 1);
    let fs = namespaces[0].clone();
    fs.create_bucket("b").unwrap();

    for iteration in 0..STORM_ITERATIONS {
        let contents: Vec<Vec<u8>> = (0..K)
            .map(|writer| {
                format!("overwrite {iteration}-{writer} ")
                    .repeat(64)
                    .into_bytes()
            })
            .collect();
        let ids: Vec<BlockId> = contents.iter().map(|c| shared.hasher().hash(c)).collect();

        let mut tasks = Vec::new();
        for data in contents.iter().cloned() {
            let fs = fs.clone();
            tasks.push(tokio::spawn(async move { put(&fs, "b", "k", data).await }));
        }
        for t in tasks {
            t.await.unwrap();
        }

        let survivor = fs
            .get_object_meta("b", "k")
            .unwrap()
            .expect("one object always survives an overwrite storm");
        assert_eq!(survivor.blocks().len(), 1, "single-block fixture");
        let alive = survivor.blocks()[0];
        for id in &ids {
            assert_block_state(&shared, *id, usize::from(*id == alive), fs.fs_root());
        }

        // The survivor's own reference goes the same way, leaving the store
        // clean for the next round.
        fs.delete_object("b", "k").await.unwrap();
        for id in &ids {
            assert_block_state(&shared, *id, 0, fs.fs_root());
        }
    }
}

/// Overwrite racing DELETE of one key.
///
/// Both operations take the record they act on out of a single transaction
/// -- `take_object` for the delete, `replace_object` for the overwrite -- so
/// whichever order they land in, each record is released exactly once by
/// exactly the caller that removed it. No new analysis: this is take/replace
/// symmetry, and the pinned reference is what would catch it failing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overwrite_racing_delete_releases_each_record_once() {
    let dir = tempdir().unwrap();
    let (shared, namespaces) = store_with_namespaces(dir.path(), None, 1);
    let fs = namespaces[0].clone();
    fs.create_bucket("b").unwrap();

    let data = b"overwritten and deleted".repeat(120).to_vec();
    let id = shared.hasher().hash(&data);
    put(&fs, "b", "pinned", data.clone()).await;

    let overwriter = {
        let fs = fs.clone();
        let data = data.clone();
        tokio::spawn(async move {
            for _ in 0..STORM_ITERATIONS {
                put(&fs, "b", "k", data.clone()).await;
            }
        })
    };
    let deleter = {
        let fs = fs.clone();
        tokio::spawn(async move {
            for _ in 0..STORM_ITERATIONS {
                fs.delete_object("b", "k").await.unwrap();
            }
        })
    };
    overwriter.await.unwrap();
    deleter.await.unwrap();

    // Whatever the interleaving left behind, one final delete quiesces the
    // key -- and then only the pinned reference may remain.
    fs.delete_object("b", "k").await.unwrap();
    assert_block_state(&shared, id, 1, fs.fs_root());
}

/// The reader race an overwrite inherits IS the delete race, unchanged.
///
/// A reader resolves its block list from the object record and then reads the
/// files. If the last reference to one of those blocks goes in between, the
/// file is unlinked underneath it -- and that is true whether the reference
/// went because the key was DELETED or because it was OVERWRITTEN. POSIX
/// keeps an already-open file alive through the unlink, so a reader holding
/// its handles reads through to completion; a reader that opens AFTER the
/// unlink fails loudly rather than being served anything.
///
/// Both legs do the same thing to the same object, one by overwriting the key
/// and one by deleting it, and assert the identical outcome. That is the
/// equivalence ADR 0008 rests on: the overwrite opens no new window, it
/// reaches the same release through the same primitive one commit later.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_overwrite_races_a_reader_exactly_as_a_delete_does() {
    for overwrite in [true, false] {
        let leg = if overwrite { "overwrite" } else { "delete" };
        let dir = tempdir().unwrap();
        let (shared, namespaces) = store_with_namespaces(dir.path(), None, 1);
        let fs = namespaces[0].clone();
        fs.create_bucket("b").unwrap();

        let data = format!("read me while it goes {leg} ")
            .repeat(64)
            .into_bytes();
        let id = shared.hasher().hash(&data);
        put(&fs, "b", "k", data.clone()).await;

        // The reader resolves its paths and OPENS them, then stops. The
        // record it read is about to stop being the visible one.
        let (_, paths) = fs.get_object_paths("b", "k").unwrap().expect("just PUT");
        let mut open: Vec<std::fs::File> = paths
            .iter()
            .map(|(path, _)| std::fs::File::open(path).expect("a live block file opens"))
            .collect();

        if overwrite {
            put(&fs, "b", "k", b"entirely other bytes".repeat(64).to_vec()).await;
        } else {
            fs.delete_object("b", "k").await.unwrap();
        }

        // Either way the old object's last reference is gone: record
        // removed, file unlinked.
        assert_block_state(&shared, id, 0, fs.fs_root());

        // The handles opened before it went still serve the old bytes, whole.
        let mut read_back = Vec::new();
        for file in &mut open {
            let mut buf = Vec::new();
            file.read_to_end(&mut buf)
                .expect("an already-open handle survives the unlink");
            read_back.extend_from_slice(&buf);
        }
        assert_eq!(read_back, data, "{leg}: an open stream reads through");
        assert_eq!(
            shared.hasher().hash(&read_back),
            id,
            "{leg}: and reads back the block it opened"
        );

        // Opening after the unlink fails loudly. Never wrong bytes.
        for (path, _) in &paths {
            let err = std::fs::File::open(path).expect_err("open-after-unlink must fail");
            assert_eq!(
                err.kind(),
                std::io::ErrorKind::NotFound,
                "{leg}: the failure must say the file is gone"
            );
        }
    }
}

/// Counts exclusive-create file writes; everything else is real.
#[derive(Debug)]
struct CountingOps {
    writes: AtomicUsize,
    real: RealDiskOps,
}

impl CountingOps {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            writes: AtomicUsize::new(0),
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

/// N concurrent PUTs of one brand-new block: exactly one file write hits
/// the disk (the stripe serializes; every later writer sees the record and
/// bumps), and at quiesce rc == N.
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

    assert_eq!(
        ops.writes.load(Ordering::SeqCst),
        1,
        "exactly one file write for one block, however many writers"
    );
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
        super::crash_fixtures::plant_degraded_record(&shared, id, 1, N, data.len());

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

/// Ops whose exclusive-create write signals entry and then waits for a
/// release, so a test can cancel the awaiting future while the blocking
/// closure is provably mid-protocol.
#[derive(Debug)]
struct GatedWriteOps {
    real: RealDiskOps,
    entered: std::sync::mpsc::Sender<()>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}

impl BlockDiskOps for GatedWriteOps {
    fn create_dir_all(&self, path: &std::path::Path) -> std::io::Result<()> {
        self.real.create_dir_all(path)
    }
    fn write_new_file(&self, path: &std::path::Path, contents: &[u8]) -> std::io::Result<()> {
        self.entered.send(()).ok();
        self.release.lock().unwrap().recv().ok();
        self.real.write_new_file(path, contents)
    }
    fn fsync_file(&self, path: &std::path::Path, data_only: bool) -> std::io::Result<()> {
        self.real.fsync_file(path, data_only)
    }
    fn fsync_dir(&self, path: &std::path::Path) -> std::io::Result<()> {
        self.real.fsync_dir(path)
    }
    fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
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

/// Cancel a PUT while its blocking closure is staging a block's temp file
/// (ADR 0010).
///
/// Staging is deliberately OUTSIDE the uncancellable window: no stripe is
/// held, nothing has been renamed, and no record exists, so a request that
/// dies here has changed nothing a reader or another writer can observe. The
/// residue is a temp file -- residue class 3, collected wholesale at the next
/// store open -- and specifically NOT a committed record, which is what the
/// per-block protocol used to leave here and which fsck had to reconcile
/// against the rc.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_put_cancelled_while_staging_leaves_only_temp_residue() {
    let dir = tempdir().unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let ops = Arc::new(GatedWriteOps {
        real: RealDiskOps,
        entered: entered_tx,
        release: std::sync::Mutex::new(release_rx),
    });
    let (shared, namespaces) = store_with_namespaces(dir.path(), Some(ops), 1);
    let fs = namespaces[0].clone();
    fs.create_bucket("b").unwrap();

    let data = b"cancelled mid stage".repeat(120).to_vec();
    let id = shared.hasher().hash(&data);

    let putter = {
        let fs = fs.clone();
        let data = data.clone();
        tokio::spawn(async move { put(&fs, "b", "k", data).await })
    };

    // The closure is provably inside write_new_file: cancel the request
    // future now, then let the write finish.
    entered_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("the staging closure must reach the write");
    putter.abort();
    let _ = putter.await;
    release_tx.send(()).unwrap();

    // Give the detached staging closure time to finish its write. Whatever
    // it does, it cannot produce a record: the batch that would have
    // committed one died with the request.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(
        shared
            .block_tree()
            .get_block(id.as_slice())
            .unwrap()
            .is_none(),
        "a cancelled stage must never leave a block record"
    );
    assert!(
        !crate::metastore::block_disk_path(&id, 1, fs.fs_root().clone()).exists(),
        "nothing was renamed, so no file is at a final path"
    );
    assert!(!fs.key_exists("b", "k").unwrap());

    // A retry writes the block properly: rc 1, not 2, because nothing was
    // ever recorded to bump. It stages a file of its own, so the gate needs
    // one more token -- proof in itself that the first attempt recorded
    // nothing to dedup against.
    release_tx.send(()).unwrap();
    put(&fs, "b", "k", data.clone()).await;
    let block = shared
        .block_tree()
        .get_block(id.as_slice())
        .unwrap()
        .expect("the retry records the block");
    assert_eq!(block.rc(), 1, "the retry writes, it does not bump a ghost");
    let path = block.disk_path(&id, fs.fs_root().clone());
    assert_eq!(
        shared.hasher().hash(&std::fs::read(&path).unwrap()),
        id,
        "the retry's file is complete"
    );
}

/// Ops that gate the RENAME: lets a test cancel a PUT while its batch is
/// provably inside the commit closure, stripes held, files landing.
#[derive(Debug)]
struct GatedRenameOps {
    real: RealDiskOps,
    entered: std::sync::mpsc::Sender<()>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}

impl BlockDiskOps for GatedRenameOps {
    fn create_dir_all(&self, path: &std::path::Path) -> std::io::Result<()> {
        self.real.create_dir_all(path)
    }
    fn write_new_file(&self, path: &std::path::Path, contents: &[u8]) -> std::io::Result<()> {
        self.real.write_new_file(path, contents)
    }
    fn fsync_file(&self, path: &std::path::Path, data_only: bool) -> std::io::Result<()> {
        self.real.fsync_file(path, data_only)
    }
    fn fsync_dir(&self, path: &std::path::Path) -> std::io::Result<()> {
        self.real.fsync_dir(path)
    }
    fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
        self.entered.send(()).ok();
        self.release.lock().unwrap().recv().ok();
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

/// Cancel a PUT once its batch has entered the commit closure (ADR 0010).
///
/// This is the uncancellable window, and it is uncancellable for the same
/// reason it always was: the stripes are OWNED by the blocking closure, so
/// the closure runs to completion detached and releases them itself. The
/// residue is therefore a COMPLETE record+file pair -- never a rename
/// without its record, never a record without a complete file -- and a retry
/// simply dedup-bumps it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_put_cancelled_inside_the_commit_still_lands_whole() {
    let dir = tempdir().unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let ops = Arc::new(GatedRenameOps {
        real: RealDiskOps,
        entered: entered_tx,
        release: std::sync::Mutex::new(release_rx),
    });
    let (shared, namespaces) = store_with_namespaces(dir.path(), Some(ops), 1);
    let fs = namespaces[0].clone();
    fs.create_bucket("b").unwrap();

    let data = b"cancelled mid commit".repeat(120).to_vec();
    let id = shared.hasher().hash(&data);

    let putter = {
        let fs = fs.clone();
        let data = data.clone();
        tokio::spawn(async move { put(&fs, "b", "k", data).await })
    };

    entered_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("the commit closure must reach the rename");
    putter.abort();
    let _ = putter.await;
    release_tx.send(()).unwrap();

    // The detached closure completes: record and file both exist, no temp
    // residue. Poll briefly -- completion is asynchronous to the abort.
    let block = {
        let mut waited = 0u64;
        loop {
            if let Some(b) = shared.block_tree().get_block(id.as_slice()).unwrap() {
                break b;
            }
            waited += 50;
            assert!(waited < 10_000, "detached closure must finish the commit");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    };
    assert_eq!(block.rc(), 1);
    let path = block.disk_path(&id, fs.fs_root().clone());
    assert_eq!(
        shared.hasher().hash(&std::fs::read(&path).unwrap()),
        id,
        "the file at the final path is complete"
    );
    let tmp_entries = std::fs::read_dir(fs.fs_root().join(".tmp"))
        .unwrap()
        .count();
    assert_eq!(tmp_entries, 0, "no temp residue after the closure finished");

    // The object never landed (the request died), so the record is
    // leak-class residue: a retry dedup-bumps it rather than rewriting.
    assert!(!fs.key_exists("b", "k").unwrap());
    put(&fs, "b", "k", data.clone()).await;
    assert_eq!(
        shared
            .block_tree()
            .get_block(id.as_slice())
            .unwrap()
            .unwrap()
            .rc(),
        2,
        "the retry bumps the healed record"
    );
}

/// Ops that gate the UNLINK: lets a test cancel a DELETE while its
/// blocking closure is about to unlink, then race a PUT of the same
/// block against the detached closure.
#[derive(Debug)]
struct GatedUnlinkOps {
    real: RealDiskOps,
    entered: std::sync::mpsc::Sender<()>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}

impl BlockDiskOps for GatedUnlinkOps {
    fn create_dir_all(&self, path: &std::path::Path) -> std::io::Result<()> {
        self.real.create_dir_all(path)
    }
    fn write_new_file(&self, path: &std::path::Path, contents: &[u8]) -> std::io::Result<()> {
        self.real.write_new_file(path, contents)
    }
    fn fsync_file(&self, path: &std::path::Path, data_only: bool) -> std::io::Result<()> {
        self.real.fsync_file(path, data_only)
    }
    fn fsync_dir(&self, path: &std::path::Path) -> std::io::Result<()> {
        self.real.fsync_dir(path)
    }
    fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
        self.real.rename(from, to)
    }
    fn remove_file(&self, path: &std::path::Path) -> std::io::Result<()> {
        // Only block-file unlinks are gated; the open-time temp purge
        // calls list_dir first and finds nothing, so this only fires on
        // the delete path.
        self.entered.send(()).ok();
        self.release.lock().unwrap().recv().ok();
        self.real.remove_file(path)
    }
    fn list_dir(&self, path: &std::path::Path) -> std::io::Result<Vec<std::path::PathBuf>> {
        self.real.list_dir(path)
    }
    fn device_of(&self, path: &std::path::Path) -> std::io::Result<Option<u64>> {
        self.real.device_of(path)
    }
}

/// The guard-in-closure property: a cancelled last-ref DELETE cannot
/// unlink a block a subsequent PUT recreated. The DELETE's unlink runs
/// inside the stripe hold, so the PUT (which needs the same stripe)
/// cannot interleave between the record removal and the unlink -- the
/// recreate strictly follows the completed delete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_delete_cannot_unlink_a_recreated_block() {
    let dir = tempdir().unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let ops = Arc::new(GatedUnlinkOps {
        real: RealDiskOps,
        entered: entered_tx,
        release: std::sync::Mutex::new(release_rx),
    });
    let (shared, namespaces) = store_with_namespaces(dir.path(), Some(ops), 1);
    let fs = namespaces[0].clone();
    fs.create_bucket("b").unwrap();

    let data = b"delete then recreate".repeat(130).to_vec();
    let id = shared.hasher().hash(&data);
    put(&fs, "b", "k", data.clone()).await;

    // Start the last-ref DELETE and cancel it while its closure is parked
    // at the unlink, holding the stripe.
    let deleter = {
        let fs = fs.clone();
        tokio::spawn(async move { fs.delete_object("b", "k").await.unwrap() })
    };
    entered_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("the delete closure must reach the unlink");
    deleter.abort();
    let _ = deleter.await;

    // Recreate the block. The PUT must queue on the stripe until the
    // detached delete closure finishes its unlink and releases the guard.
    let putter = {
        let fs = fs.clone();
        let data = data.clone();
        tokio::spawn(async move { put(&fs, "b", "k2", data).await })
    };
    // Give the PUT time to reach the stripe, then let the unlink proceed.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    release_tx.send(()).unwrap();
    putter.await.unwrap();

    // The recreated block is intact: the cancelled DELETE's unlink
    // happened strictly before the PUT's write, never after it.
    let block = shared
        .block_tree()
        .get_block(id.as_slice())
        .unwrap()
        .expect("recreated record");
    assert_eq!(block.rc(), 1);
    let path = block.disk_path(&id, fs.fs_root().clone());
    assert_eq!(
        shared.hasher().hash(&std::fs::read(&path).unwrap()),
        id,
        "the recreated block file must survive the cancelled delete"
    );
}
