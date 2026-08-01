use std::str::FromStr;
use std::sync::Arc;
use std::{io, path::PathBuf};

use super::multipart::{MultiPart, part_key};
use super::shared_block_store::SharedBlockStore;
use super::uploads::UploadClaim;
use crate::metrics::SharedMetrics;

use crate::metastore::{
    BlockId, BlockTree, BucketMeta, ContentHash, Durability, FjallStore, HeaderSpec, MetaError,
    MetaStore, MetaTreeExt, Object, ObjectData, UploadRecord,
};

use super::byte_stream::AsyncByteStream;

pub const BLOCK_SIZE: usize = 1 << 20; // Supposedly 1 MiB

/// One namespace's view of a store: its own metadata DB, plus the block store
/// it shares with every sibling namespace.
///
/// A clone shares the same `SharedBlockStore` `Arc` -- one stripe set, one
/// blocks DB, one blocks root -- so ADR 0006's one-store-one-instance rule
/// survives cloning; that is what lets the stale-upload GC hold its own handle
/// beside the S3 service (ADR 0003).
#[derive(Clone)]
pub struct CasFS {
    pub(super) namespace: MetaStore,
    pub(super) shared: Arc<SharedBlockStore>,
    pub(super) metrics: SharedMetrics,
    pub(super) verify_on_read: bool,
}

/// Which metadata database backend a store uses.
///
/// Deserialized through [`FromStr`] (`try_from = "String"`) so the config file
/// spelling is exactly the CLI flag spelling: `fjall`.
///
/// A single variant since ADR 0007 removed the non-transactional backend;
/// the enum survives as the config/CLI surface, and [`FromStr`] rejects the
/// removed value with the migration path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(try_from = "String")]
pub enum StorageEngine {
    // fjall with transactions support
    Fjall,
}

impl FromStr for StorageEngine {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "fjall" => Ok(StorageEngine::Fjall),
            "fjall_notx" => Err(
                "the fjall_notx backend was removed (ADR 0007); use fjall with \
                 durability = \"buffer\" for the fast tier"
                    .to_string(),
            ),
            _ => Err(format!("unknown storage engine: {s} (expected fjall)")),
        }
    }
}

impl TryFrom<String> for StorageEngine {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl std::fmt::Display for StorageEngine {
    /// Writes the spelling [`FromStr`] accepts, so a value read from a config
    /// file round-trips through a log line unchanged.
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let name = match self {
            StorageEngine::Fjall => "fjall",
        };
        f.write_str(name)
    }
}

pub type ObjectPaths = (Object, Vec<(PathBuf, usize)>);

impl CasFS {
    /// Build a `CasFS` for one namespace, sharing a block/multipart store
    /// across namespaces via `shared`.
    ///
    /// Layout on disk:
    ///   `namespace_meta_path/db/` - this namespace's metadata DB
    ///   (the shared DB and the block data files live wherever
    ///   `SharedBlockStore::new` was given -- the blocks root is the store's,
    ///   not this namespace's, so cross-namespace dedup always resolves to
    ///   one set of files)
    ///
    /// The namespace DB is headered like every other store. Its header takes
    /// the hash the shared block store already carries, so the two DBs of one
    /// deployment can never disagree about how blocks are addressed.
    ///
    /// # Errors
    ///
    /// [`MetaError::Header`] if the namespace DB exists but its header is
    /// missing or unacceptable; see [`MetaStore::open_or_create`].
    ///
    /// [`MetaError::StoreLocked`] if another process already holds the
    /// namespace DB. Routine rather than exceptional -- it is what an offline
    /// tool meets when the daemon is running -- so it is a value, not a panic.
    ///
    /// `verify_on_read` turns on block verification on read; see
    /// [`CasFS::verify_on_read`] for what it does and does not cover. It is a
    /// constructor parameter until the config file gives it a home.
    pub fn new(
        mut namespace_meta_path: PathBuf,
        shared: Arc<SharedBlockStore>,
        metrics: SharedMetrics,
        storage_engine: StorageEngine,
        inlined_metadata_size: Option<usize>,
        durability: Option<Durability>,
        verify_on_read: bool,
    ) -> Result<Self, MetaError> {
        namespace_meta_path.push("db");

        // Canonicalize to eliminate getcwd() syscalls in async operations
        std::fs::create_dir_all(&namespace_meta_path).ok();
        namespace_meta_path = namespace_meta_path
            .canonicalize()
            .unwrap_or(namespace_meta_path);

        let spec = HeaderSpec::from(shared.hasher());
        let (namespace, _header) = match storage_engine {
            StorageEngine::Fjall => {
                MetaStore::open_or_create(namespace_meta_path, inlined_metadata_size, spec, |p| {
                    FjallStore::new(p, inlined_metadata_size, durability)
                })?
            }
        };

        Ok(Self {
            namespace,
            shared,
            metrics,
            verify_on_read,
        })
    }

    /// Convenience constructor for single-namespace consumers (CLI ops,
    /// tests, third-party library users who only need one namespace).
    ///
    /// Builds a dedicated `SharedBlockStore` with its blocks DB at
    /// `meta_path/blocks/.db/` and its block data files at `root/blocks/`,
    /// and returns a `CasFS` whose namespace metadata lives at
    /// `meta_path/db/`.
    ///
    /// Two headered DBs are involved: the blocks DB, whose header names the
    /// block hash, and the namespace DB, which inherits it. `spec` applies
    /// only to DBs that are created now; `None` takes
    /// [`HeaderSpec::default`].
    ///
    /// `verify_on_read` is passed straight to [`CasFS::new`], and
    /// `stripe_count` and `max_blocks_per_commit` straight to
    /// [`SharedBlockStore::new`] (`None` takes the built-in defaults;
    /// `config::DEFAULT_STRIPE_COUNT` and
    /// `config::DEFAULT_MAX_BLOCKS_PER_COMMIT` name them).
    ///
    /// # One process, one store
    ///
    /// Every call mints a PRIVATE `SharedBlockStore` -- with its own stripe
    /// set and placement state. A process must not open the same on-disk
    /// store through this twice: two instances would not share stripes, and
    /// the per-block serialization that the write and delete protocols rely
    /// on (ADR 0006) would silently not hold between them. Open one `CasFS`
    /// per store, or build one `SharedBlockStore` and hand it to
    /// [`CasFS::new`] per namespace.
    #[allow(clippy::too_many_arguments)]
    pub fn single_namespace(
        root: PathBuf,
        meta_path: PathBuf,
        metrics: SharedMetrics,
        storage_engine: StorageEngine,
        inlined_metadata_size: Option<usize>,
        durability: Option<Durability>,
        spec: Option<HeaderSpec>,
        verify_on_read: bool,
        stripe_count: Option<usize>,
        max_blocks_per_commit: Option<usize>,
    ) -> Result<Self, MetaError> {
        let shared = Arc::new(SharedBlockStore::new(
            meta_path.join("blocks"),
            root.join("blocks"),
            storage_engine,
            inlined_metadata_size,
            durability,
            spec,
            stripe_count,
            max_blocks_per_commit,
        )?);
        Self::new(
            meta_path,
            shared,
            metrics,
            storage_engine,
            inlined_metadata_size,
            durability,
            verify_on_read,
        )
    }

    /// The hash function this filesystem addresses blocks with, taken from the
    /// block store's header at open.
    pub fn hasher(&self) -> crate::hasher::Hasher {
        self.shared.hasher()
    }

    /// Whether reads re-hash each block and refuse to serve one whose bytes no
    /// longer hash to its address. Off by default: it costs a full buffer plus
    /// a hash per block.
    ///
    /// # Limitation
    ///
    /// Whole blocks only. A range request is served from the same block files
    /// without verification, because a partial block cannot be re-hashed
    /// against a whole-block address. Callers that need the guarantee must ask
    /// for the whole object.
    pub fn verify_on_read(&self) -> bool {
        self.verify_on_read
    }

    /// Root directory of the block data files -- the store's, shared by
    /// every namespace.
    pub fn fs_root(&self) -> &PathBuf {
        self.shared.blocks_root()
    }

    /// This namespace's own metadata store: the DB holding its bucket trees.
    ///
    /// Distinct from the shared block store's metadata (`_BLOCKS`,
    /// `_MULTIPART_PARTS`), which lives in a different database. Tools that
    /// walk the namespace's holders -- fsck (ADR 0005) -- start here.
    pub fn namespace_meta_store(&self) -> &MetaStore {
        &self.namespace
    }

    /// The block store this namespace shares with its siblings.
    pub fn shared_block_store(&self) -> &Arc<SharedBlockStore> {
        &self.shared
    }

    pub fn max_inlined_data_length(&self) -> usize {
        self.namespace.max_inlined_data_length()
    }

    pub fn get_bucket(
        &self,
        bucket_name: &str,
    ) -> Result<Arc<dyn MetaTreeExt + Send + Sync>, MetaError> {
        super::buckets::get_bucket(self, bucket_name)
    }

    /// Open the tree containing the block map.
    pub fn block_tree(&self) -> Result<Arc<BlockTree>, MetaError> {
        Ok(self.shared.block_tree())
    }

    /// Check if a bucket with a given name exists.
    pub fn bucket_exists(&self, bucket_name: &str) -> Result<bool, MetaError> {
        super::buckets::bucket_exists(self, bucket_name)
    }

    /// Write an object record under `key`, releasing the blocks of whatever
    /// object it replaced (ADR 0008).
    ///
    /// The single entry point for every object-record write: PUT, the inline
    /// path, and `CompleteMultipartUpload`. An overwrite ends the replaced
    /// object's life exactly as a DELETE does, and pays the same cost -- one
    /// striped decrement per displaced occurrence. See
    /// [`write_path::create_object_meta`](super::write_path::create_object_meta)
    /// for the ordering rule (new record commits first) and why the reader
    /// race it inherits is the delete race and not a new one.
    pub async fn create_object_meta(
        &self,
        bucket_name: &str,
        key: &str,
        size: u64,
        hash: ContentHash,
        object_data: ObjectData,
    ) -> Result<Object, MetaError> {
        super::write_path::create_object_meta(self, bucket_name, key, size, hash, object_data).await
    }

    // get meta object from the DB
    pub fn get_object_meta(
        &self,
        bucket_name: &str,
        key: &str,
    ) -> Result<Option<Object>, MetaError> {
        super::read_path::get_object_meta(self, bucket_name, key)
    }

    pub fn get_object_paths(
        &self,
        bucket_name: &str,
        key: &str,
    ) -> Result<Option<ObjectPaths>, MetaError> {
        super::read_path::get_object_paths(self, bucket_name, key)
    }

    // create and insert a new  bucket
    pub fn create_bucket(&self, bucket_name: &str) -> Result<(), MetaError> {
        super::buckets::create_bucket(self, bucket_name)
    }

    /// Remove a bucket and its associated metadata.
    // TODO: this is very much not optimal
    pub async fn bucket_delete(&self, bucket_name: &str) -> Result<(), MetaError> {
        super::delete_path::bucket_delete(self, bucket_name).await
    }

    /// Record a new in-flight multipart upload (ADR 0003). Written by
    /// `CreateMultipartUpload` once the bucket check passes.
    pub fn create_upload(&self, bucket: &str, key: &str, upload_id: &str) -> Result<(), MetaError> {
        super::uploads::create_upload(self, bucket, key, upload_id)
    }

    /// Read an upload record without claiming it: the non-atomic existence
    /// check `UploadPart` makes before it streams any block. See
    /// [`uploads::get_upload`](super::uploads::get_upload) for what this
    /// deliberately does not guarantee.
    pub fn get_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<Option<UploadRecord>, MetaError> {
        super::uploads::get_upload(self, bucket, key, upload_id)
    }

    /// Claim an upload: the atomic read+remove that decides complete versus
    /// abort. `None` means another caller already won, and the caller answers
    /// `NoSuchUpload`.
    pub fn claim_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<Option<UploadRecord>, MetaError> {
        super::uploads::claim_upload(self, bucket, key, upload_id)
    }

    /// Claim an upload together with the parts a `CompleteMultipartUpload`
    /// names: one transaction over both trees, so a named part record never
    /// outlives its upload record (ADR 0003 amendment).
    ///
    /// The returned parts are the values that transaction read, and the object
    /// must be built from them. See
    /// [`uploads::claim_upload_with_parts`](super::uploads::claim_upload_with_parts)
    /// for what each outcome means and what a crash after it leaves behind.
    pub fn claim_upload_with_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_numbers: &[i64],
    ) -> Result<UploadClaim, MetaError> {
        super::uploads::claim_upload_with_parts(self, bucket, key, upload_id, part_numbers)
    }

    /// Abort an upload: claim it, then remove each part record and drop the
    /// block references it held (ADR 0003).
    ///
    /// `Some(parts reaped)` when this caller won the claim, `None` when it
    /// lost -- the `NoSuchUpload` case, whether the upload was completed,
    /// aborted a moment earlier, or never existed. `AbortMultipartUpload` and
    /// the stale-upload GC are both callers; the GC is just another client of
    /// the claim.
    pub async fn abort_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<Option<usize>, MetaError> {
        super::uploads::abort_upload(self, bucket, key, upload_id).await
    }

    /// Claim one part record by the storage key it is filed under, and drop
    /// the block references it held (ADR 0003).
    ///
    /// The take IS the claim: `Ok(None)` means another reaper -- a client's
    /// abort, a GC sweep -- won it and already released its blocks, which is
    /// a no-op for this caller and not an error. See
    /// [`uploads::reap_part`](super::uploads::reap_part) for why the key is
    /// passed in rather than rebuilt, and why the record leaves before its
    /// blocks do.
    ///
    /// Crate-internal: the daemon reaps through [`Self::abort_upload`] or the
    /// GC sweep, and the only caller that names a single raw key is fsck's
    /// `--repair`, which reaps the orphan parts its multipart pass found.
    pub(crate) async fn reap_part(
        &self,
        storage_key: &[u8],
    ) -> Result<Option<MultiPart>, MetaError> {
        super::uploads::reap_part(self, storage_key).await
    }

    /// Every in-flight upload record in the store, decoded, unsorted and
    /// unfiltered: `ListMultipartUploads` and the GC's TTL sweep each apply
    /// their own (in-memory) selection.
    pub fn list_uploads(&self) -> Result<Vec<UploadRecord>, MetaError> {
        super::uploads::list_uploads(self)
    }

    /// Every part record of one upload, ascending by part number.
    pub fn upload_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<Vec<MultiPart>, MetaError> {
        super::uploads::upload_parts(self, bucket, key, upload_id)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn insert_multipart_part(
        &self,
        bucket: String,
        key: String,
        size: usize,
        part_number: i64,
        upload_id: String,
        hash: ContentHash,
        blocks: Vec<BlockId>,
    ) -> Result<(), MetaError> {
        let mp_map = self.shared.multipart_tree();
        let storage_key = part_key(&bucket, &key, &upload_id, part_number);

        tracing::debug!(
            bucket = %bucket,
            key = %key,
            upload_id = %upload_id,
            part_number = part_number,
            size = size,
            blocks = blocks.len(),
            "CasFS: insert_multipart_part"
        );

        let mp = MultiPart::new(size, part_number, bucket, key, upload_id, hash, blocks);

        mp_map.insert(&storage_key, mp)?;
        Ok(())
    }

    pub fn get_multipart_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i64,
    ) -> Result<Option<MultiPart>, MetaError> {
        let mp_map = self.shared.multipart_tree();
        let storage_key = part_key(bucket, key, upload_id, part_number);

        tracing::debug!(
            bucket = %bucket,
            key = %key,
            upload_id = %upload_id,
            part_number = part_number,
            "CasFS: get_multipart_part"
        );

        let result = mp_map.get_multipart_part(&storage_key);

        if let Ok(Some(ref mp)) = result {
            tracing::debug!(
                bucket = %bucket,
                key = %key,
                upload_id = %upload_id,
                part_number = part_number,
                blocks = mp.blocks().len(),
                "CasFS: get_multipart_part found"
            );
        }

        result
    }

    pub fn remove_multipart_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i64,
    ) -> Result<(), MetaError> {
        let mp_map = self.shared.multipart_tree();
        let storage_key = part_key(bucket, key, upload_id, part_number);

        tracing::debug!(
            bucket = %bucket,
            key = %key,
            upload_id = %upload_id,
            part_number = part_number,
            "CasFS: remove_multipart_part"
        );

        mp_map.remove(&storage_key)
    }

    pub fn key_exists(&self, bucket: &str, key: &str) -> Result<bool, MetaError> {
        super::buckets::key_exists(self, bucket, key)
    }

    /// Get a list of all buckets in the system.
    pub fn list_buckets(&self) -> Result<Vec<BucketMeta>, MetaError> {
        super::buckets::list_buckets(self)
    }

    /// Delete an object from a bucket.
    /// it also delete keys under it's tree
    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), MetaError> {
        super::delete_path::delete_object(self, bucket, key).await
    }

    // convenient function to store an object to disk and then store it's metada
    pub async fn store_single_object_and_meta(
        &self,
        bucket_name: &str,
        key: &str,
        data: AsyncByteStream,
        len: usize,
    ) -> io::Result<Object> {
        super::write_path::store_single_object_and_meta(self, bucket_name, key, data, len).await
    }

    /// Save the stream of bytes to disk.
    ///
    /// The data is streamed in chunks, and each chunk is hashed and stored on disk.
    /// The hash of each chunk is used as a key to store the data in the database.
    ///
    /// A list of block ID's used as keys for the data blocks is
    /// returned, along with the hash of the full byte stream, and the length of the stream.
    pub async fn store_object(
        &self,
        bucket_name: &str,
        key: &str,
        data: AsyncByteStream,
    ) -> io::Result<(Vec<BlockId>, ContentHash, u64)> {
        super::write_path::store_object(self, bucket_name, key, data).await
    }

    /// Store an object inlined in its own metadata record.
    ///
    /// Inline objects hold no block references, but the write still releases
    /// what it replaced: an inline write over a BLOCK-BACKED object is the
    /// case where the new record names nothing, so the release is the only
    /// thing between the overwrite and a permanent leak (ADR 0008).
    pub async fn store_inlined_object(
        &self,
        bucket_name: &str,
        key: &str,
        data: Vec<u8>,
    ) -> Result<Object, MetaError> {
        super::write_path::store_inlined_object(self, bucket_name, key, data).await
    }
}

#[cfg(test)]
mod tests {
    use super::AsyncByteStream;
    use super::*;
    use crate::cas::block_stream::BlockStream;
    use crate::cas::range_request::RangeRequest;
    use crate::hasher::Hasher;
    use bytes::Bytes;
    use futures::{StreamExt, stream};
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
            storage_engine,
            Some(1),
            Some(Durability::Buffer),
            Some(HeaderSpec::from(hasher)),
            verify_on_read,
            None,
            None,
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
        let mut shared = crate::cas::SharedBlockStore::new(
            dir.path().join("meta/blocks"),
            dir.path().join("blocks"),
            StorageEngine::Fjall,
            Some(1),
            Some(Durability::Buffer),
            Some(HeaderSpec::from(hasher)),
            None,
            None,
        )
        .unwrap();
        shared.set_disk_ops(Arc::new(FailingWriteOps));
        let fs = CasFS::new(
            dir.path().join("meta"),
            Arc::new(shared),
            METRICS.clone(),
            StorageEngine::Fjall,
            Some(1),
            Some(Durability::Buffer),
            false,
        )
        .unwrap();
        (fs, dir)
    }

    #[tokio::test]
    async fn test_store_object_write_failure() {
        for hasher in TEST_WIDTHS {
            let (fs, _dir) = setup_failing_write_fs(hasher);
            do_test_store_object_write_failure(fs).await;
        }
    }

    async fn do_test_store_object_write_failure(fs: CasFS) {
        let bucket_name = "test_bucket";
        let key = "test_key";
        fs.create_bucket(bucket_name).unwrap();

        let test_data = b"test data".repeat(100);
        let stream = AsyncByteStream::new(stream::once(async move { Ok(Bytes::from(test_data)) }));

        let result = fs.store_object(bucket_name, key, stream).await;
        assert!(result.is_err());

        // Verify the error
        let err = result.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::Other);
        assert_eq!(err.to_string(), "Mock write failure");

        // File-first: a failed disk write means NO record was ever
        // attempted -- nothing to clean up, nothing left behind.
        let block_tree = fs.shared.block_tree();
        assert_eq!(block_tree.len().unwrap(), 0);

        // Verify object metadata was not created
        assert!(!fs.key_exists(bucket_name, key).unwrap());
    }

    #[tokio::test]
    async fn test_store_object() {
        for (engine, hasher) in matrix() {
            let (fs, _dir) = setup_test_fs(engine, hasher);
            do_test_store_object(fs).await;
        }
    }

    async fn do_test_store_object(fs: CasFS) {
        const BUCKET_NAME: &str = "test_bucket";
        const KEY1: &str = "test_key1";
        const KEY2: &str = "test_key2";
        fs.create_bucket(BUCKET_NAME).unwrap();

        // Create ByteStream from test data
        let test_data = b"long test data".repeat(100).to_vec();
        let test_data_2 = test_data.clone();
        let test_data_len = test_data.len();
        let stream =
            AsyncByteStream::new(stream::once(
                async move { Ok(Bytes::from(test_data.clone())) },
            ));

        // Store object
        let obj = fs
            .store_single_object_and_meta(BUCKET_NAME, KEY1, stream, test_data_len)
            .await
            .unwrap();

        // Verify results
        assert_eq!(obj.size(), test_data_len as u64);
        assert_eq!(obj.blocks().len(), 1);

        // Verify block was stored and its file sits at the derived path
        let block_tree = fs.shared.block_tree();
        assert!(block_tree.len().unwrap() > 0);
        let stored_block = block_tree
            .get_block(obj.blocks()[0].as_slice())
            .unwrap()
            .unwrap();
        assert_eq!(stored_block.size(), test_data_len);
        assert_eq!(stored_block.rc(), 1);
        assert!(
            stored_block
                .disk_path(&obj.blocks()[0], fs.fs_root().clone())
                .is_file(),
            "block file must exist at the depth-derived path"
        );

        // Store the same data again with different key
        // - The same block should be returned
        // - The refcount should be increased

        let stream =
            AsyncByteStream::new(stream::once(
                async move { Ok(Bytes::from(test_data_2.clone())) },
            ));

        let new_obj = fs
            .store_single_object_and_meta(BUCKET_NAME, KEY2, stream, test_data_len)
            .await
            .unwrap();

        assert_eq!(new_obj.blocks(), obj.blocks());

        let stored_block = block_tree
            .get_block(new_obj.blocks()[0].as_slice())
            .unwrap()
            .unwrap();
        assert_eq!(stored_block.rc(), 2);
    }

    /// The configured stripe count reaches the stripes.
    ///
    /// Pins the plumbing the config knob added: `stripe_count` travels from
    /// `single_namespace` into `SharedBlockStore::new` and on into `Stripes`.
    /// Observed through behaviour rather than a length accessor -- with one
    /// stripe every block must resolve to the same lock, and with many, two
    /// ids two apart must not. A knob that was accepted and dropped on the
    /// floor would pass every config test and fail this one.
    #[test]
    fn the_configured_stripe_count_reaches_the_stripes() {
        let one = tempdir().unwrap();
        let fs = CasFS::single_namespace(
            one.path().to_path_buf(),
            one.path().to_path_buf(),
            METRICS.clone(),
            StorageEngine::Fjall,
            Some(1),
            Some(Durability::Buffer),
            None,
            false,
            Some(1),
            None,
        )
        .unwrap();

        let id = |b0: u8, b1: u8| {
            let mut bytes = [0u8; crate::metastore::BLOCKID_SIZE];
            bytes[0] = b0;
            bytes[1] = b1;
            crate::metastore::BlockId::from(bytes)
        };

        // One stripe: everything collides, by construction.
        assert!(Arc::ptr_eq(
            &fs.shared.stripes().for_hash(&id(0x00, 0x01)),
            &fs.shared.stripes().for_hash(&id(0xff, 0xfe))
        ));
        drop(fs);

        // The default is not 1, so the same two ids must now part ways --
        // otherwise the assertion above would hold for any count and prove
        // nothing.
        let many = tempdir().unwrap();
        let fs = CasFS::single_namespace(
            many.path().to_path_buf(),
            many.path().to_path_buf(),
            METRICS.clone(),
            StorageEngine::Fjall,
            Some(1),
            Some(Durability::Buffer),
            None,
            false,
            None,
            None,
        )
        .unwrap();
        assert!(!Arc::ptr_eq(
            &fs.shared.stripes().for_hash(&id(0x00, 0x01)),
            &fs.shared.stripes().for_hash(&id(0xff, 0xfe))
        ));
    }

    /// ADR 0006: two namespaces over one `SharedBlockStore` resolve one
    /// stripe set and ONE disk path per block. Cross-namespace dedup must
    /// bump the shared record and never write a second file.
    #[tokio::test]
    async fn test_two_namespaces_share_stripes_and_block_paths() {
        let dir = tempdir().unwrap();
        let shared = Arc::new(
            crate::cas::SharedBlockStore::new(
                dir.path().join("meta/blocks"),
                dir.path().join("blocks"),
                StorageEngine::Fjall,
                Some(1),
                Some(Durability::Buffer),
                None,
                None,
                None,
            )
            .unwrap(),
        );
        let ns = |name: &str| {
            CasFS::new(
                dir.path().join("meta").join(name),
                shared.clone(),
                METRICS.clone(),
                StorageEngine::Fjall,
                Some(1),
                Some(Durability::Buffer),
                false,
            )
            .unwrap()
        };
        let alice = ns("alice");
        let bob = ns("bob");
        alice.create_bucket("b").unwrap();
        bob.create_bucket("b").unwrap();

        let data = b"cross namespace dedup payload".repeat(40).to_vec();
        let id = alice.hasher().hash(&data);

        // Same stripe object from both namespaces.
        assert!(Arc::ptr_eq(
            &alice.shared.stripes().for_hash(&id),
            &bob.shared.stripes().for_hash(&id)
        ));

        let put = |data: Vec<u8>| {
            let len = data.len();
            let stream = AsyncByteStream::new(stream::once(async move { Ok(Bytes::from(data)) }));
            (stream, len)
        };
        let (stream, len) = put(data.clone());
        let obj_a = alice
            .store_single_object_and_meta("b", "k", stream, len)
            .await
            .unwrap();
        let (stream, len) = put(data.clone());
        let obj_b = bob
            .store_single_object_and_meta("b", "k", stream, len)
            .await
            .unwrap();
        assert_eq!(obj_a.blocks(), obj_b.blocks());

        // One record, rc 2, one file at one path derived from ONE root.
        let block = shared
            .block_tree()
            .get_block(id.as_slice())
            .unwrap()
            .unwrap();
        assert_eq!(block.rc(), 2, "dedup must bump the shared record");
        let path_a = block.disk_path(&id, alice.fs_root().clone());
        let path_b = block.disk_path(&id, bob.fs_root().clone());
        assert_eq!(path_a, path_b, "both namespaces derive the same path");
        assert!(path_a.is_file());
    }

    /// ADR 0006 orphan healing, end to end: a file named like the block,
    /// hand-planted on the id's directory chain at a non-policy depth (crash
    /// residue of an earlier attempt), is adopted by a later PUT -- the
    /// record stores the orphan's depth and the file ends up with the real
    /// bytes at that same path.
    #[tokio::test]
    async fn test_orphan_block_file_is_healed_in_place() {
        let (fs, _dir) = setup_test_fs(StorageEngine::Fjall, Hasher::Blake3W32);
        const BUCKET: &str = "test-bucket";
        fs.create_bucket(BUCKET).unwrap();

        let data = b"orphan heal payload".repeat(64).to_vec();
        let id = fs.hasher().hash(&data);

        // Plant garbage at depth 2 on the id's chain, as a torn write would.
        let orphan_path = crate::metastore::block_disk_path(&id, 2, fs.fs_root().clone());
        std::fs::create_dir_all(orphan_path.parent().unwrap()).unwrap();
        std::fs::write(&orphan_path, b"torn garbage").unwrap();

        let len = data.len();
        let stream =
            AsyncByteStream::new(stream::once(async move { Ok(Bytes::from(data.clone())) }));
        let obj = fs
            .store_single_object_and_meta(BUCKET, "healed", stream, len)
            .await
            .unwrap();
        assert_eq!(obj.blocks(), [id]);

        let block = fs
            .shared
            .block_tree()
            .get_block(id.as_slice())
            .unwrap()
            .unwrap();
        assert_eq!(block.depth(), 2, "record must adopt the orphan's depth");
        let bytes = std::fs::read(&orphan_path).unwrap();
        assert_eq!(
            fs.hasher().hash(&bytes),
            id,
            "the orphan path must now hold the real block bytes"
        );
    }

    /// The well known MD5 of zero bytes, which is the ETag S3 clients expect
    /// for an empty object.
    const EMPTY_MD5: &str = "d41d8cd98f00b204e9800998ecf8427e";

    #[tokio::test]
    async fn test_store_empty_object() {
        for (engine, hasher) in matrix() {
            let (fs, _dir) = setup_test_fs(engine, hasher);
            do_test_store_empty_object(fs).await;
        }
    }

    async fn do_test_store_empty_object(fs: CasFS) {
        const BUCKET_NAME: &str = "test_bucket";
        const KEY: &str = "empty";
        fs.create_bucket(BUCKET_NAME).unwrap();

        let stream = AsyncByteStream::new(stream::empty());
        let obj = fs
            .store_single_object_and_meta(BUCKET_NAME, KEY, stream, 0)
            .await
            .unwrap();

        assert_eq!(obj.size(), 0);
        assert!(obj.blocks().is_empty());
        // The empty object is not stored, but it still hashes to the MD5 of
        // no bytes rather than to a zero sentinel.
        assert_eq!(obj.format_e_tag(), EMPTY_MD5);
    }

    #[tokio::test]
    async fn test_store_inlined_object() {
        for (engine, hasher) in matrix() {
            let (fs, _dir) = setup_test_fs(engine, hasher);
            do_test_store_inlined_object(fs).await;
        }
    }

    async fn do_test_store_inlined_object(fs: CasFS) {
        let bucket_name = "test_bucket";
        let key = "test_key1";
        fs.create_bucket(bucket_name).unwrap();

        let small_data = b"small test data".to_vec();
        let obj_meta = fs
            .store_inlined_object(bucket_name, key, small_data.clone())
            .await
            .unwrap();

        // Verify inlined data
        assert_eq!(obj_meta.size(), small_data.len() as u64);
        assert_eq!(obj_meta.inlined().unwrap(), &small_data);
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

    /// An INLINE write replacing a BLOCK-BACKED object: the case ADR 0008
    /// singles out. The new record names no blocks at all, so the release is
    /// the only thing standing between the overwrite and a permanent leak --
    /// nothing that survives could ever name those references again.
    #[tokio::test]
    async fn inline_over_block_backed_releases_the_replaced_blocks() {
        for (engine, hasher) in matrix() {
            let (fs, _dir) = setup_test_fs(engine, hasher);
            fs.create_bucket("b").unwrap();

            let obj = put_blocks(&fs, "b", "k", b"block backed payload".repeat(60).to_vec()).await;
            let id = obj.blocks()[0];
            assert_eq!(rc_of(&fs, &id), Some(1));

            fs.store_inlined_object("b", "k", b"now inline".to_vec())
                .await
                .unwrap();

            assert_eq!(
                rc_of(&fs, &id),
                None,
                "the inline record names nothing, so the last reference is gone"
            );
            assert!(
                !crate::metastore::block_disk_path(&id, 1, fs.fs_root().clone()).exists(),
                "the last release must unlink the file"
            );
            assert_eq!(
                fs.get_object_meta("b", "k").unwrap().unwrap().inlined(),
                Some(&b"now inline".to_vec()),
                "the inline object is the one that survives"
            );
        }
    }

    /// Inline over inline: neither record holds a reference, so there is
    /// nothing to release and the overwrite is a plain record replacement.
    /// This is the shape respd's `set` has (through its own tree, not this
    /// path -- respd carries no block store at all).
    #[tokio::test]
    async fn inline_over_inline_releases_nothing() {
        let (fs, _dir) = setup_test_fs(StorageEngine::Fjall, Hasher::Blake3W32);
        fs.create_bucket("b").unwrap();

        fs.store_inlined_object("b", "k", b"first".to_vec())
            .await
            .unwrap();
        fs.store_inlined_object("b", "k", b"second".to_vec())
            .await
            .unwrap();

        assert_eq!(
            fs.get_object_meta("b", "k").unwrap().unwrap().inlined(),
            Some(&b"second".to_vec())
        );
        assert_eq!(
            fs.shared.block_tree().len().unwrap(),
            0,
            "an inline object never touched a block record"
        );
    }

    /// Block-backed over inline: the displaced record holds no references,
    /// so the release is a no-op and the new object's block sits at exactly
    /// one.
    #[tokio::test]
    async fn block_backed_over_inline_releases_nothing() {
        let (fs, _dir) = setup_test_fs(StorageEngine::Fjall, Hasher::Blake3W32);
        fs.create_bucket("b").unwrap();

        fs.store_inlined_object("b", "k", b"inline first".to_vec())
            .await
            .unwrap();
        let obj = put_blocks(&fs, "b", "k", b"blocks second".repeat(60).to_vec()).await;

        assert_eq!(rc_of(&fs, &obj.blocks()[0]), Some(1));
    }

    /// An overwrite with DIFFERENT content: the old block loses its only
    /// holder and goes, the new one arrives at one. The bump-and-release
    /// arithmetic has no shared block to cancel against here, so both
    /// directions are visible in one test.
    #[tokio::test]
    async fn overwrite_with_new_content_drops_the_old_block_and_keeps_the_new() {
        let (fs, _dir) = setup_test_fs(StorageEngine::Fjall, Hasher::Blake3W32);
        fs.create_bucket("b").unwrap();

        let old = put_blocks(&fs, "b", "k", b"the old bytes".repeat(60).to_vec()).await;
        let new = put_blocks(&fs, "b", "k", b"the new bytes".repeat(60).to_vec()).await;
        let (old_id, new_id) = (old.blocks()[0], new.blocks()[0]);
        assert_ne!(old_id, new_id, "the test needs distinct content");

        assert_eq!(rc_of(&fs, &old_id), None, "-1 for the dropped block");
        assert!(!crate::metastore::block_disk_path(&old_id, 1, fs.fs_root().clone()).exists());
        assert_eq!(rc_of(&fs, &new_id), Some(1), "+1 for the added block");
    }

    /// An overwrite whose new object SHARES a block with the one it replaces
    /// nets to no change: the write bumped the shared block (every dedup hit
    /// bumps, ADR 0006) and the release dropped the old occurrence. The
    /// unshared halves move by exactly one each.
    #[tokio::test]
    async fn overwrite_sharing_a_block_nets_to_no_change() {
        let (fs, _dir) = setup_test_fs(StorageEngine::Fjall, Hasher::Blake3W32);
        fs.create_bucket("b").unwrap();

        // Two objects of two blocks each, sharing their first block. The
        // block size is 1 MiB, so a chunk boundary needs that much data.
        let shared_half = b"shared prefix block ".repeat(60_000);
        let old_half = b"old suffix block ".repeat(60_000);
        let new_half = b"new suffix block ".repeat(60_000);

        let old = put_blocks(
            &fs,
            "b",
            "k",
            [shared_half.clone(), old_half].concat().to_vec(),
        )
        .await;
        assert!(old.blocks().len() > 1, "the fixture needs two blocks");
        let shared_id = old.blocks()[0];
        let old_tail = *old.blocks().last().unwrap();
        assert_eq!(rc_of(&fs, &shared_id), Some(1));

        let new = put_blocks(&fs, "b", "k", [shared_half, new_half].concat().to_vec()).await;
        assert_eq!(
            new.blocks()[0],
            shared_id,
            "the fixture needs a shared head"
        );
        let new_tail = *new.blocks().last().unwrap();
        assert_ne!(new_tail, old_tail, "the fixture needs a changed tail");

        assert_eq!(
            rc_of(&fs, &shared_id),
            Some(1),
            "bump and release cancel on the shared block"
        );
        assert_eq!(
            rc_of(&fs, &old_tail),
            None,
            "the dropped tail loses its one"
        );
        assert_eq!(rc_of(&fs, &new_tail), Some(1), "the added tail gains one");
    }

    #[tokio::test]
    async fn test_store_object_refcount() {
        for (engine, hasher) in matrix() {
            let (fs, _dir) = setup_test_fs(engine, hasher);
            do_test_store_object_refcount(fs).await;
        }
    }

    async fn do_test_store_object_refcount(fs: CasFS) {
        let bucket_name = "test_bucket";
        let key1 = "test_key1";
        let key2 = "test_key2";
        fs.create_bucket(bucket_name).unwrap();

        // Create ByteStream from test data
        let test_data = b"long test data".repeat(100).to_vec();
        let test_data_len = test_data.len();
        let test_data_2 = test_data.clone();
        let test_data_3 = test_data.clone();
        let stream =
            AsyncByteStream::new(stream::once(
                async move { Ok(Bytes::from(test_data.clone())) },
            ));

        // Store object
        let obj = fs
            .store_single_object_and_meta(bucket_name, key1, stream, test_data_len)
            .await
            .unwrap();

        // Initial refcount must be 1
        let block_tree = fs.shared.block_tree();
        for id in obj.blocks() {
            let block = block_tree.get_block(id.as_slice()).unwrap().unwrap();
            assert_eq!(block.rc(), 1);
        }

        {
            // Re-PUT with the SAME key. Two rules meet here and cancel:
            // every dedup hit bumps (ADR 0006, the key_has_block skip is
            // gone), and the overwrite releases the record it displaced
            // (ADR 0008). Bump to 2, release back to 1 -- one object, one
            // reference, exactly the truth. Before 0008 this settled at 2
            // and waited for fsck.
            let stream =
                AsyncByteStream::new(stream::once(
                    async move { Ok(Bytes::from(test_data_2.clone())) },
                ));

            let new_obj = fs
                .store_single_object_and_meta(bucket_name, key1, stream, test_data_len)
                .await
                .unwrap();

            assert_eq!(new_obj.blocks(), obj.blocks());

            let stored_block = block_tree
                .get_block(new_obj.blocks()[0].as_slice())
                .unwrap()
                .unwrap();
            assert_eq!(
                stored_block.rc(),
                1,
                "bump for the new object, release for the replaced one"
            );
        }
        {
            // A SECOND key referencing the same content bumps and displaces
            // nothing: two objects, two references.
            let stream =
                AsyncByteStream::new(stream::once(
                    async move { Ok(Bytes::from(test_data_3.clone())) },
                ));

            let new_obj = fs
                .store_single_object_and_meta(bucket_name, key2, stream, test_data_len)
                .await
                .unwrap();

            assert_eq!(new_obj.blocks(), obj.blocks());

            let stored_block = block_tree
                .get_block(new_obj.blocks()[0].as_slice())
                .unwrap()
                .unwrap();
            assert_eq!(stored_block.rc(), 2, "two live objects, two references");
        }
    }

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

        let stream2 =
            AsyncByteStream::new(stream::once(async move { Ok(Bytes::from(test_data2)) }));

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

        let stream2 =
            AsyncByteStream::new(stream::once(async move { Ok(Bytes::from(test_data2)) }));

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

    /// Payload spanning several blocks, with a partial last one. The period is
    /// coprime with the block size, so no two blocks come out identical and
    /// deduplication does not collapse them.
    #[allow(clippy::cast_possible_truncation)] // the modulus bounds the cast
    fn multi_block_data() -> Vec<u8> {
        (0..BLOCK_SIZE * 2 + 4096)
            .map(|i| (i % 251) as u8)
            .collect()
    }

    #[tokio::test]
    async fn test_block_files_hash_to_their_address() {
        for (engine, hasher) in matrix() {
            let (fs, _dir) = setup_test_fs(engine, hasher);
            do_test_block_files_hash_to_their_address(fs, hasher).await;
        }
    }

    /// End-to-end guard against a write path that hashes with anything but the
    /// store's own hasher: write an object the normal way, then re-hash each
    /// block file straight off disk and demand the address back, byte for
    /// byte.
    async fn do_test_block_files_hash_to_their_address(fs: CasFS, hasher: Hasher) {
        const BUCKET: &str = "test-bucket";
        const KEY: &str = "multi/block";
        fs.create_bucket(BUCKET).unwrap();

        let data = multi_block_data();
        let len = data.len();
        let stream = AsyncByteStream::new(stream::once(async move { Ok(Bytes::from(data)) }));
        let obj = fs
            .store_single_object_and_meta(BUCKET, KEY, stream, len)
            .await
            .unwrap();
        assert!(obj.blocks().len() > 1, "test data must span several blocks");

        let block_tree = fs.shared.block_tree();
        for id in obj.blocks() {
            assert_eq!(id.len(), hasher.width() as usize);
            let block = block_tree.get_block(id.as_slice()).unwrap().unwrap();
            let path = block.disk_path(id, fs.fs_root().clone());
            let on_disk = std::fs::read(&path).unwrap();
            assert_eq!(on_disk.len(), block.size());
            assert_eq!(
                hasher.hash(&on_disk).as_slice(),
                id.as_slice(),
                "block file {} does not hash to its address {}",
                path.display(),
                id.to_hex()
            );
        }
    }

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
}
