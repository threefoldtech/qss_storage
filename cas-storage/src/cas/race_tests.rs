//! Race-stress and cancellation tests for the ADR 0006 block protocol
//! (plan component 9).
//!
//! These tests are written against the INVARIANTS, not the
//! implementation: never a record without a complete file, never an
//! unlinked live block, exact refcounts at quiesce. Iteration counts are
//! sized for CI; when hunting a suspected race, raise `STORM_ITERATIONS`
//! and run under `--release` locally.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use futures::stream;
use tempfile::tempdir;

use super::block_disk::{BlockDiskOps, RealDiskOps};
use super::byte_stream::AsyncByteStream;
use super::fs::CasFS;
use super::shared_block_store::SharedBlockStore;
use crate::metastore::Durability;
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

/// Cancel a PUT while its blocking closure is mid-write. The closure runs
/// to completion detached, so the residue is a COMPLETE record+file pair
/// -- never a temp at a final path, never a record without a complete
/// file -- and a retry simply dedup-bumps it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_put_leaves_record_plus_file_or_nothing() {
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

    let data = b"cancelled mid write".repeat(120).to_vec();
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
        .expect("the blocking closure must reach the write");
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
            assert!(waited < 10_000, "detached closure must finish the insert");
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
