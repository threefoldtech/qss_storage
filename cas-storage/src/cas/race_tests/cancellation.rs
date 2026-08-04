use super::*;

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
