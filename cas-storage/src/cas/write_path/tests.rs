use super::*;
use crate::cas::block_disk::{BlockDiskOps, RealDiskOps};
use crate::cas::fs::BLOCK_SIZE;
use crate::cas::fs::CasFS;
use crate::cas::shared_block_store::SharedBlockStore;
use crate::metastore::BlockId;
use crate::metastore::{BlockTree, Durability, block_disk_path};
use crate::metrics::SharedMetrics;
use crate::store_options::StoreOptions;
use bytes::Bytes;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use tempfile::tempdir;

const BUCKET: &str = "b";

/// One store with one namespace at a chosen batch cap.
fn store_with_cap(
    dir: &Path,
    cap: Option<usize>,
    ops: Option<Arc<dyn BlockDiskOps>>,
) -> (Arc<SharedBlockStore>, CasFS) {
    store_with_cap_and_station(dir, cap, ops, None)
}

/// The same, with a commit station (ADR 0011) if one is asked for.
fn store_with_cap_and_station(
    dir: &Path,
    cap: Option<usize>,
    ops: Option<Arc<dyn BlockDiskOps>>,
    group_commit: Option<crate::cas::GroupCommit>,
) -> (Arc<SharedBlockStore>, CasFS) {
    let opts = StoreOptions {
        inline_metadata_size: Some(1),
        durability: Durability::Buffer,
        max_blocks_per_commit: cap,
        group_commit,
        ..StoreOptions::default()
    };
    let mut shared =
        SharedBlockStore::new(dir.join("meta/blocks"), dir.join("blocks"), opts).unwrap();
    if let Some(ops) = ops {
        shared.set_disk_ops(ops);
    }
    let shared = Arc::new(shared);
    let fs = CasFS::new(
        dir.join("meta/ns"),
        shared.clone(),
        SharedMetrics::default(),
        opts,
    )
    .unwrap();
    fs.create_bucket(BUCKET).unwrap();
    (shared, fs)
}

/// `n` blocks' worth of content, every block distinct.
fn distinct_blocks(tag: &str, n: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(n * BLOCK_SIZE);
    for block in 0..n {
        let filler = format!("{tag}-block-{block}-");
        let mut one = filler.repeat(BLOCK_SIZE / filler.len() + 1);
        one.truncate(BLOCK_SIZE);
        out.extend_from_slice(one.as_bytes());
    }
    out
}

async fn put(fs: &CasFS, key: &str, data: Vec<u8>) -> Object {
    let len = data.len();
    let stream = AsyncByteStream::new(futures::stream::once(async move { Ok(Bytes::from(data)) }));
    fs.store_single_object_and_meta(BUCKET, key, stream, len)
        .await
        .unwrap()
}

/// Reads the block record count at each rename, so a test can see exactly
/// when records became visible relative to the files landing.
#[derive(Debug)]
struct CommitObservingOps {
    real: RealDiskOps,
    /// Set once the store exists; `rename` reads through it.
    tree: StdMutex<Option<Arc<BlockTree>>>,
    /// Record count observed at each rename, in call order.
    at_rename: StdMutex<Vec<usize>>,
    writes: AtomicUsize,
}

impl CommitObservingOps {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            real: RealDiskOps,
            tree: StdMutex::new(None),
            at_rename: StdMutex::new(Vec::new()),
            writes: AtomicUsize::new(0),
        })
    }

    fn watch(&self, tree: Arc<BlockTree>) {
        *self.tree.lock().unwrap() = Some(tree);
    }

    fn observations(&self) -> Vec<usize> {
        self.at_rename.lock().unwrap().clone()
    }
}

impl BlockDiskOps for CommitObservingOps {
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.real.create_dir_all(path)
    }
    fn write_new_file(&self, path: &Path, contents: &[u8]) -> io::Result<()> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        self.real.write_new_file(path, contents)
    }
    fn fsync_file(&self, path: &Path, data_only: bool) -> io::Result<()> {
        self.real.fsync_file(path, data_only)
    }
    fn fsync_dir(&self, path: &Path) -> io::Result<()> {
        self.real.fsync_dir(path)
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let observed = self
            .tree
            .lock()
            .unwrap()
            .as_ref()
            .map(|tree| tree.len().unwrap());
        if let Some(count) = observed {
            self.at_rename.lock().unwrap().push(count);
        }
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

mod batching;
mod overtake;

/// The commit station (ADR 0011): strangers share a flush.
///
/// Every test here turns the station on and drives it through the real
/// write path -- `store_object` and `store_single_object_and_meta` -- so
/// nothing is asserted about a function that production does not call.
///
/// # How a group is forced
///
/// Group formation is natural batching: whatever queued while the
/// previous group was committing. That is by definition timing-dependent,
/// so tests that need SEVERAL members in ONE group set a
/// `group_commit_window` and rely on the timer -- the only deterministic
/// grouping the design offers. The window is the thing under test in
/// those cases anyway. Tests about a LONE request use window zero, which
/// is the shipped default and the one an operator gets.
mod station;
