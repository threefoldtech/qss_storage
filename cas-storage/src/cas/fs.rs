use std::str::FromStr;
use std::sync::Arc;
use std::{io, path::PathBuf};

use super::async_fs::{AsyncFileSystem, RealAsyncFs};
use super::multipart::MultiPart;
use super::shared_block_store::SharedBlockStore;
use crate::metrics::SharedMetrics;

use crate::metastore::{
    BaseMetaTree, BlockID, BlockTree, BucketMeta, Durability, FjallStore, FjallStoreNotx,
    MetaError, MetaStore, MetaTreeExt, Object, ObjectData,
};

use super::byte_stream::AsyncByteStream;

pub const BLOCK_SIZE: usize = 1 << 20; // Supposedly 1 MiB

pub struct CasFS {
    pub(super) async_fs: Box<dyn AsyncFileSystem>,
    pub(super) namespace: MetaStore,
    pub(super) shared: Arc<SharedBlockStore>,
    pub(super) root: PathBuf,
    pub(super) metrics: SharedMetrics,
}

#[derive(Debug, Clone, Copy)]
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
            _ => Err(format!("Unknown storage engine: {s}")),
        }
    }
}

pub type ObjectPaths = (Object, Vec<(PathBuf, usize)>);

impl CasFS {
    /// Build a `CasFS` for one namespace, sharing a block/path/multipart
    /// store across namespaces via `shared`.
    ///
    /// Layout on disk:
    ///   `root/blocks/` - block data files
    ///   `namespace_meta_path/db/` - this namespace's metadata DB
    ///   (the shared DB lives wherever `SharedBlockStore::new` was given)
    pub fn new(
        mut root: PathBuf,
        mut namespace_meta_path: PathBuf,
        shared: Arc<SharedBlockStore>,
        metrics: SharedMetrics,
        storage_engine: StorageEngine,
        inlined_metadata_size: Option<usize>,
        durability: Option<Durability>,
    ) -> Self {
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

        let namespace = match storage_engine {
            StorageEngine::Fjall => {
                let store = FjallStore::new(namespace_meta_path, inlined_metadata_size, durability);
                MetaStore::new(store, inlined_metadata_size)
            }
            StorageEngine::FjallNotx => {
                let store = FjallStoreNotx::new(namespace_meta_path, inlined_metadata_size);
                MetaStore::new(store, inlined_metadata_size)
            }
        };

        Self {
            async_fs: Box::new(RealAsyncFs),
            namespace,
            shared,
            root,
            metrics,
        }
    }

    /// Convenience constructor for single-namespace consumers (CLI ops,
    /// tests, third-party library users who only need one namespace).
    ///
    /// Builds a dedicated `SharedBlockStore` at `meta_path.join("blocks")`
    /// and returns a `CasFS` whose namespace metadata lives at
    /// `meta_path/db/`.
    pub fn single_namespace(
        root: PathBuf,
        meta_path: PathBuf,
        metrics: SharedMetrics,
        storage_engine: StorageEngine,
        inlined_metadata_size: Option<usize>,
        durability: Option<Durability>,
    ) -> Result<Self, MetaError> {
        let shared = Arc::new(SharedBlockStore::new(
            meta_path.join("blocks"),
            storage_engine,
            inlined_metadata_size,
            durability,
        )?);
        Ok(Self::new(
            root,
            meta_path,
            shared,
            metrics,
            storage_engine,
            inlined_metadata_size,
            durability,
        ))
    }

    pub(super) fn path_tree(&self) -> Result<Arc<dyn BaseMetaTree>, MetaError> {
        Ok(self.shared.path_tree())
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
        hash: BlockID,
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

    fn part_key(&self, bucket: &str, key: &str, upload_id: &str, part_number: i64) -> String {
        format!("{bucket}-{key}-{upload_id}-{part_number}")
    }

    #[allow(clippy::too_many_arguments)]
    pub fn insert_multipart_part(
        &self,
        bucket: String,
        key: String,
        size: usize,
        part_number: i64,
        upload_id: String,
        hash: BlockID,
        blocks: Vec<BlockID>,
    ) -> Result<(), MetaError> {
        let mp_map = self.shared.multipart_tree();
        let storage_key = self.part_key(&bucket, &key, &upload_id, part_number);

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
        let part_key = self.part_key(bucket, key, upload_id, part_number);

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
        let part_key = self.part_key(bucket, key, upload_id, part_number);

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
    ) -> io::Result<(Vec<BlockID>, BlockID, u64)> {
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
    use bytes::Bytes;
    use futures::stream;
    use once_cell::sync::Lazy;
    use tempfile::tempdir;

    const TEST_ENGINES: [StorageEngine; 2] = [StorageEngine::Fjall, StorageEngine::FjallNotx];

    static METRICS: Lazy<SharedMetrics> = Lazy::new(SharedMetrics::default);

    fn setup_test_fs(storage_engine: StorageEngine) -> (CasFS, tempfile::TempDir) {
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
        )
        .unwrap();
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
        for engine in TEST_ENGINES {
            let (fs, _dir) = setup_test_fs(engine);
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
        for engine in TEST_ENGINES {
            let (fs, _dir) = setup_test_fs(engine);
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
        let stored_block = block_tree.get_block(&obj.blocks()[0]).unwrap().unwrap();
        assert_eq!(stored_block.size(), test_data_len);
        assert_eq!(stored_block.rc(), 1);
        assert!(
            fs.path_tree()
                .unwrap()
                .contains_key(stored_block.path())
                .unwrap()
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

        let stored_block = block_tree.get_block(&new_obj.blocks()[0]).unwrap().unwrap();
        assert_eq!(stored_block.rc(), 2);
    }

    #[tokio::test]
    async fn test_store_inlined_object() {
        for engine in TEST_ENGINES {
            let (fs, _dir) = setup_test_fs(engine);
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
            .unwrap();

        // Verify inlined data
        assert_eq!(obj_meta.size(), small_data.len() as u64);
        assert_eq!(obj_meta.inlined().unwrap(), &small_data);
    }

    #[tokio::test]
    async fn test_store_object_refcount() {
        for engine in TEST_ENGINES {
            let (fs, _dir) = setup_test_fs(engine);
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
            let block = block_tree.get_block(id).unwrap().unwrap();
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

            let stored_block = block_tree.get_block(&new_obj.blocks()[0]).unwrap().unwrap();
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

            let stored_block = block_tree.get_block(&new_obj.blocks()[0]).unwrap().unwrap();
            assert_eq!(stored_block.rc(), 2);
        }
    }

    #[tokio::test]
    async fn test_store_and_delete_object() {
        for engine in TEST_ENGINES {
            let (fs, _dir) = setup_test_fs(engine);
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
            let block = block_tree.get_block(id).unwrap().unwrap();
            assert!(fs.path_tree().unwrap().contains_key(block.path()).unwrap());
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
            assert!(block_tree.get_block(id).unwrap().is_none());
        }
        // Verify paths were cleaned up
        for path in stored_paths {
            assert!(!fs.path_tree().unwrap().contains_key(&path).unwrap());
        }
    }

    #[tokio::test]
    async fn test_store_and_delete_object_with_refcount_same_blocks_diffkey() {
        for engine in TEST_ENGINES {
            let (fs, _dir) = setup_test_fs(engine);
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
            let block = block_tree.get_block(id).unwrap().unwrap();
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
            let block = block_tree.get_block(id).unwrap().unwrap();
            assert_eq!(block.rc(), 2);
        }

        // Delete first object
        fs.delete_object(bucket, key1).await.unwrap();

        // Verify blocks still exist
        let block_tree = fs.shared.block_tree();
        for id in obj1.blocks() {
            let block = block_tree.get_block(id).unwrap().unwrap();
            assert_eq!(block.rc(), 1);
        }

        // Delete second object
        fs.delete_object(bucket, key2).await.unwrap();

        // Verify blocks are gone
        for id in obj1.blocks() {
            assert!(block_tree.get_block(id).unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn test_store_and_delete_object_with_refcount_same_blocks_samekey() {
        for engine in TEST_ENGINES {
            let (fs, _dir) = setup_test_fs(engine);
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
            let block = block_tree.get_block(id).unwrap().unwrap();
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
            let block = block_tree.get_block(id).unwrap().unwrap();
            assert_eq!(block.rc(), 1);
        }

        // Delete object
        fs.delete_object(bucket, key1).await.unwrap();

        // Verify blocks are gone
        for id in obj1.blocks() {
            assert!(block_tree.get_block(id).unwrap().is_none());
        }
    }
}
