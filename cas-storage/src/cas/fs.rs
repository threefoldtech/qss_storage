use std::str::FromStr;
use std::sync::Arc;
use std::{io, path::PathBuf};

use super::async_fs::{AsyncFileSystem, RealAsyncFs};
use super::multipart::MultiPart;
use super::shared_block_store::SharedBlockStore;
use crate::metrics::SharedMetrics;

use crate::metastore::{
    BaseMetaTree, BlockId, BlockTree, BucketMeta, ContentHash, Durability, FjallStore,
    FjallStoreNotx, HeaderSpec, MetaError, MetaStore, MetaTreeExt, Object, ObjectData,
};

use super::byte_stream::AsyncByteStream;

pub const BLOCK_SIZE: usize = 1 << 20; // Supposedly 1 MiB

pub struct CasFS {
    pub(super) async_fs: Box<dyn AsyncFileSystem>,
    pub(super) namespace: MetaStore,
    pub(super) shared: Arc<SharedBlockStore>,
    pub(super) root: PathBuf,
    pub(super) metrics: SharedMetrics,
    pub(super) verify_on_read: bool,
}

/// Which metadata database backend a store uses.
///
/// Deserialized through [`FromStr`] (`try_from = "String"`) so the config file
/// spelling is exactly the CLI flag spelling: `fjall` or `fjall_notx`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(try_from = "String")]
pub enum StorageEngine {
    // fjall with transactions support
    Fjall,

    // fjall without transactions support.
    // we implement the rollback logic in our own code
    FjallNotx,
}

impl FromStr for StorageEngine {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "fjall" => Ok(StorageEngine::Fjall),
            "fjall_notx" => Ok(StorageEngine::FjallNotx),
            _ => Err(format!(
                "unknown storage engine: {s} (expected fjall or fjall_notx)"
            )),
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
            StorageEngine::FjallNotx => "fjall_notx",
        };
        f.write_str(name)
    }
}

pub type ObjectPaths = (Object, Vec<(PathBuf, usize)>);

/// Storage key of one part of a multipart upload.
fn part_key(bucket: &str, key: &str, upload_id: &str, part_number: i64) -> String {
    format!("{bucket}-{key}-{upload_id}-{part_number}")
}

impl CasFS {
    /// Build a `CasFS` for one namespace, sharing a block/path/multipart
    /// store across namespaces via `shared`.
    ///
    /// Layout on disk:
    ///   `root/blocks/` - block data files
    ///   `namespace_meta_path/db/` - this namespace's metadata DB
    ///   (the shared DB lives wherever `SharedBlockStore::new` was given)
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
    /// `verify_on_read` turns on block verification on read; see
    /// [`CasFS::verify_on_read`] for what it does and does not cover. It is a
    /// constructor parameter until the config file gives it a home.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        mut root: PathBuf,
        mut namespace_meta_path: PathBuf,
        shared: Arc<SharedBlockStore>,
        metrics: SharedMetrics,
        storage_engine: StorageEngine,
        inlined_metadata_size: Option<usize>,
        durability: Option<Durability>,
        verify_on_read: bool,
    ) -> Result<Self, MetaError> {
        namespace_meta_path.push("db");
        root.push("blocks");

        // Canonicalize both paths to eliminate getcwd() syscalls in async operations
        // This is critical for performance as it avoids repeated getcwd() on every file op
        std::fs::create_dir_all(&root).ok();
        root = root.canonicalize().unwrap_or(root);

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
            StorageEngine::FjallNotx => {
                MetaStore::open_or_create(namespace_meta_path, inlined_metadata_size, spec, |p| {
                    FjallStoreNotx::new(p, inlined_metadata_size)
                })?
            }
        };

        Ok(Self {
            async_fs: Box::new(RealAsyncFs),
            namespace,
            shared,
            root,
            metrics,
            verify_on_read,
        })
    }

    /// Convenience constructor for single-namespace consumers (CLI ops,
    /// tests, third-party library users who only need one namespace).
    ///
    /// Builds a dedicated `SharedBlockStore` at `meta_path.join("blocks")`
    /// and returns a `CasFS` whose namespace metadata lives at
    /// `meta_path/db/`.
    ///
    /// Two headered DBs are involved: the blocks DB, whose header names the
    /// block hash, and the namespace DB, which inherits it. `spec` applies
    /// only to DBs that are created now; `None` takes
    /// [`HeaderSpec::default`].
    ///
    /// `verify_on_read` is passed straight to [`CasFS::new`].
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
    ) -> Result<Self, MetaError> {
        let shared = Arc::new(SharedBlockStore::new(
            meta_path.join("blocks"),
            storage_engine,
            inlined_metadata_size,
            durability,
            spec,
        )?);
        Self::new(
            root,
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

    pub(super) fn path_tree(&self) -> Arc<dyn BaseMetaTree> {
        self.shared.path_tree()
    }

    pub fn fs_root(&self) -> &PathBuf {
        &self.root
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

    // create a meta object and insert it into the database
    pub fn create_object_meta(
        &self,
        bucket_name: &str,
        key: &str,
        size: u64,
        hash: ContentHash,
        object_data: ObjectData,
    ) -> Result<Object, MetaError> {
        let obj_meta = Object::new(size, hash, object_data);
        self.namespace
            .insert_meta(bucket_name, key, obj_meta.to_vec())?;
        Ok(obj_meta)
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
            "CasFS: insert_multipart_part storage_key={}, size={}, blocks={}",
            storage_key,
            size,
            blocks.len()
        );

        let mp = MultiPart::new(size, part_number, bucket, key, upload_id, hash, blocks);

        mp_map.insert(storage_key.as_bytes(), mp)?;
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
        let part_key = part_key(bucket, key, upload_id, part_number);

        tracing::debug!("CasFS: get_multipart_part storage_key={}", part_key);

        let result = mp_map.get_multipart_part(part_key.as_bytes());

        if let Ok(Some(ref mp)) = result {
            tracing::debug!(
                "CasFS: get_multipart_part found storage_key={}, blocks={}",
                part_key,
                mp.blocks().len()
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
        let part_key = part_key(bucket, key, upload_id, part_number);

        tracing::debug!("CasFS: remove_multipart_part storage_key={}", part_key);

        mp_map.remove(part_key.as_bytes())
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

    // Store an object inlined in the metadata.
    pub fn store_inlined_object(
        &self,
        bucket_name: &str,
        key: &str,
        data: Vec<u8>,
    ) -> Result<Object, MetaError> {
        super::write_path::store_inlined_object(self, bucket_name, key, data)
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

    const TEST_ENGINES: [StorageEngine; 2] = [StorageEngine::Fjall, StorageEngine::FjallNotx];

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
        )
        .unwrap();
        assert_eq!(fs.hasher(), hasher, "store must open with the asked hasher");
        (fs, dir)
    }

    #[derive(Debug)]
    struct MockFs {
        should_fail_write: bool,
    }

    impl MockFs {
        fn new() -> Self {
            Self {
                should_fail_write: false,
            }
        }
    }

    impl AsyncFileSystem for MockFs {
        fn create_dir_all(&self, _path: &std::path::Path) -> std::io::Result<()> {
            Ok(())
        }

        fn write(&self, _path: &std::path::Path, _contents: &[u8]) -> std::io::Result<()> {
            if !self.should_fail_write {
                Err(std::io::Error::other("Mock write failure"))
            } else {
                Ok(())
            }
        }
    }

    impl CasFS {
        #[cfg(test)]
        fn with_mock_fs(mut self) -> (Self, MockFs) {
            // Changed return type
            let mock_fs = MockFs::new();
            self.async_fs = Box::new(mock_fs.clone()); // Implement Clone for MockFs
            (self, mock_fs)
        }
    }

    // Add Clone implementation for MockFs
    impl Clone for MockFs {
        fn clone(&self) -> Self {
            Self {
                should_fail_write: self.should_fail_write,
            }
        }
    }

    #[tokio::test]
    async fn test_store_object_write_failure() {
        for (engine, hasher) in matrix() {
            let (fs, _dir) = setup_test_fs(engine, hasher);
            let (fs, _mock) = fs.with_mock_fs();
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

        // Verify no blocks were stored in metadata
        // the block must be rolled back
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

        // Verify block & path was stored
        let block_tree = fs.shared.block_tree();
        assert!(block_tree.len().unwrap() > 0);
        let stored_block = block_tree
            .get_block(obj.blocks()[0].as_slice())
            .unwrap()
            .unwrap();
        assert_eq!(stored_block.size(), test_data_len);
        assert_eq!(stored_block.rc(), 1);
        assert!(fs.path_tree().contains_key(stored_block.path()).unwrap());

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
            do_test_store_inlined_object(fs);
        }
    }

    fn do_test_store_inlined_object(fs: CasFS) {
        let bucket_name = "test_bucket";
        let key = "test_key1";
        fs.create_bucket(bucket_name).unwrap();

        let small_data = b"small test data".to_vec();
        let obj_meta = fs
            .store_inlined_object(bucket_name, key, small_data.clone())
            .unwrap();

        // Verify inlined data
        assert_eq!(obj_meta.size(), small_data.len() as u64);
        assert_eq!(obj_meta.inlined().unwrap(), &small_data);
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
            // Test using  the same key
            // Refcount must not be increased

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
            assert_eq!(stored_block.rc(), 1);
        }
        {
            // Test  using a new key
            // Refcount must be increased
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
            assert_eq!(stored_block.rc(), 2);
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

        // verify blocks and path exist
        let block_tree = fs.shared.block_tree();
        let mut stored_paths = Vec::new();
        for id in obj.blocks() {
            let block = block_tree.get_block(id.as_slice()).unwrap().unwrap();
            assert!(fs.path_tree().contains_key(block.path()).unwrap());
            stored_paths.push(block.path().to_vec());
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
        // Verify paths were cleaned up
        for path in stored_paths {
            assert!(!fs.path_tree().contains_key(&path).unwrap());
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
    async fn test_store_and_delete_object_with_refcount_same_blocks_samekey() {
        for (engine, hasher) in matrix() {
            let (fs, _dir) = setup_test_fs(engine, hasher);
            do_test_store_and_delete_object_with_refcount_same_blocks_samekey(fs).await;
        }
    }

    // Test storing and deleting an object with refcount
    // - store object
    //       refcount == 1
    // - store object again with differrent key
    //      refcount == 1
    // - delete the object
    // - check block/disk/whatever should be gone
    async fn do_test_store_and_delete_object_with_refcount_same_blocks_samekey(fs: CasFS) {
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
        // Verify blocks  exist with rc=1
        let block_tree = fs.shared.block_tree();
        for id in obj2.blocks() {
            let block = block_tree.get_block(id.as_slice()).unwrap().unwrap();
            assert_eq!(block.rc(), 1);
        }

        // Delete object
        fs.delete_object(bucket, key1).await.unwrap();

        // Verify blocks are gone
        for id in obj1.blocks() {
            assert!(block_tree.get_block(id.as_slice()).unwrap().is_none());
        }
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
            let path = block.disk_path(fs.fs_root().clone());
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
        let path = block.disk_path(fs.fs_root().clone());
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
