mod delete;
mod overwrite;
mod reads;
mod sharing;
mod store;

use super::AsyncByteStream;
use super::*;
use crate::hasher::Hasher;
use crate::metastore::Durability;
use crate::store_options::StoreOptions;
use bytes::Bytes;
use futures::stream;
use std::sync::LazyLock;
use tempfile::tempdir;

const TEST_ENGINES: [StorageEngine; 1] = [StorageEngine::Fjall];

/// Both block address widths a store can be created with. Blocks written
/// under one are not addressable under the other, so every behaviour below
/// is checked at both.
const TEST_WIDTHS: [Hasher; 2] = [Hasher::Blake3W16, Hasher::Blake3W32];

/// The full backend x address-width matrix every test runs over.
fn matrix() -> Vec<(StorageEngine, Hasher)> {
    TEST_ENGINES
        .iter()
        .flat_map(|engine| TEST_WIDTHS.iter().map(move |hasher| (*engine, *hasher)))
        .collect()
}

static METRICS: LazyLock<SharedMetrics> = LazyLock::new(SharedMetrics::default);

/// How every store in this module is opened: inline nothing (so the block
/// path is always the path under test) and leave the flush to the page
/// cache (tests are not crash tests).
fn test_options() -> StoreOptions {
    StoreOptions {
        inline_metadata_size: Some(1),
        durability: Durability::Buffer,
        ..StoreOptions::default()
    }
}

fn setup_test_fs(storage_engine: StorageEngine, hasher: Hasher) -> (CasFS, tempfile::TempDir) {
    setup_test_fs_verifying(storage_engine, hasher, false)
}

fn setup_test_fs_verifying(
    storage_engine: StorageEngine,
    hasher: Hasher,
    verify_on_read: bool,
) -> (CasFS, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let meta_path = dir.path().join("meta");
    let metrics = METRICS.clone();

    let fs = CasFS::single_namespace(
        dir.path().to_path_buf(),
        meta_path,
        metrics,
        StoreOptions {
            metadata_db: storage_engine,
            hasher,
            verify_on_read,
            ..test_options()
        },
    )
    .unwrap();
    assert_eq!(fs.hasher(), hasher, "store must open with the asked hasher");
    (fs, dir)
}

/// Disk ops whose file writes always fail; everything else is real.
#[derive(Debug)]
struct FailingWriteOps;

impl crate::cas::block_disk::BlockDiskOps for FailingWriteOps {
    fn create_dir_all(&self, path: &std::path::Path) -> io::Result<()> {
        crate::cas::block_disk::RealDiskOps.create_dir_all(path)
    }

    fn write_new_file(&self, _path: &std::path::Path, _contents: &[u8]) -> io::Result<()> {
        Err(io::Error::other("Mock write failure"))
    }

    fn fsync_file(&self, path: &std::path::Path, data_only: bool) -> io::Result<()> {
        crate::cas::block_disk::RealDiskOps.fsync_file(path, data_only)
    }

    fn fsync_dir(&self, path: &std::path::Path) -> io::Result<()> {
        crate::cas::block_disk::RealDiskOps.fsync_dir(path)
    }

    fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> io::Result<()> {
        crate::cas::block_disk::RealDiskOps.rename(from, to)
    }

    fn remove_file(&self, path: &std::path::Path) -> io::Result<()> {
        crate::cas::block_disk::RealDiskOps.remove_file(path)
    }

    fn list_dir(&self, path: &std::path::Path) -> io::Result<Vec<PathBuf>> {
        crate::cas::block_disk::RealDiskOps.list_dir(path)
    }

    fn device_of(&self, path: &std::path::Path) -> io::Result<Option<u64>> {
        crate::cas::block_disk::RealDiskOps.device_of(path)
    }
}

/// A CasFS whose block file writes fail (the mock seam the plan calls
/// the injection point -- it must survive every protocol change).
fn setup_failing_write_fs(hasher: Hasher) -> (CasFS, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let opts = StoreOptions {
        hasher,
        ..test_options()
    };
    let mut shared = crate::cas::SharedBlockStore::new(
        dir.path().join("meta/blocks"),
        dir.path().join("blocks"),
        opts,
    )
    .unwrap();
    shared.set_disk_ops(Arc::new(FailingWriteOps));
    let fs = CasFS::new(
        dir.path().join("meta"),
        Arc::new(shared),
        METRICS.clone(),
        opts,
    )
    .unwrap();
    (fs, dir)
}

/// Store `data` as a block-backed object under `key`.
async fn put_blocks(fs: &CasFS, bucket: &str, key: &str, data: Vec<u8>) -> Object {
    let len = data.len();
    let stream = AsyncByteStream::new(stream::once(async move { Ok(Bytes::from(data)) }));
    fs.store_single_object_and_meta(bucket, key, stream, len)
        .await
        .unwrap()
}

/// The rc a block record carries, or `None` if the record is gone.
fn rc_of(fs: &CasFS, id: &BlockId) -> Option<usize> {
    fs.shared
        .block_tree()
        .get_block(id.as_slice())
        .unwrap()
        .map(|block| block.rc())
}

/// The bucket's usage counter, in logical bytes.
fn usage(fs: &CasFS, bucket: &str) -> Option<u64> {
    fs.namespace_meta_store().bucket_usage(bucket).unwrap()
}

/// Payload spanning several blocks, with a partial last one. The period is
/// coprime with the block size, so no two blocks come out identical and
/// deduplication does not collapse them.
#[allow(clippy::cast_possible_truncation)] // the modulus bounds the cast
fn multi_block_data() -> Vec<u8> {
    (0..BLOCK_SIZE * 2 + 4096)
        .map(|i| (i % 251) as u8)
        .collect()
}
