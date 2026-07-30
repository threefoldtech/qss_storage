use std::convert::TryFrom;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;

use super::store_header::{self, HeaderSpec, StoreHeader, StoreHeaderError, StoreInit};
use super::{BaseMetaTree, Block, BlockId, BucketMeta, MetaError, MetaTreeExt, Object, Store};

/// `MetaStore` is a struct that provides methods to interact with the metadata store.
///
/// It uses a Store implementation to handle the low-level storage operations.
/// The MetaStore provides higher-level operations for buckets, blocks, paths, and objects.
/// It serves as the main entry point for interacting with the metadata storage layer.
#[derive(Clone)]
pub struct MetaStore {
    store: Arc<dyn Store>,
    inlined_metadata_size: usize,
}

/// Default tree names used by the MetaStore
/// These constants define the names of the special trees used internally
const DEFAULT_BUCKET_TREE: &str = "_BUCKETS";
const DEFAULT_BLOCK_TREE: &str = "_BLOCKS";
const DEFAULT_PATH_TREE: &str = "_PATHS";

impl MetaStore {
    /// Creates a new MetaStore instance with the given store implementation.
    ///
    /// This constructor neither writes nor checks the store header, so it will
    /// happily wrap a database of any vintage. Production code opens stores
    /// with [`MetaStore::open_or_create`] instead; this one stays for tests,
    /// benchmarks and tools that deliberately want the unvalidated view.
    ///
    /// # Arguments
    /// * `store` - The storage backend implementation
    /// * `inlined_metadata_size` - Optional size limit for inlined metadata. If None, a default value is used.
    ///
    /// # Returns
    /// A new MetaStore instance
    pub fn new(store: impl Store + 'static, inlined_metadata_size: Option<usize>) -> Self {
        const DEFAULT_INLINED_METADATA_SIZE: usize = 1; // setting very low will practically disable it by default

        Self {
            store: Arc::new(store),
            inlined_metadata_size: inlined_metadata_size.unwrap_or(DEFAULT_INLINED_METADATA_SIZE),
        }
    }

    /// Opens the store at `db_path`, creating it with a fresh QSST header if
    /// there is nothing there yet.
    ///
    /// `build` is handed the path and must open the backing database. It runs
    /// *after* the create-or-open decision has been made, which is the whole
    /// point of taking a closure: opening a fjall database creates the
    /// directory and its partitions, so once `build` has run the question
    /// "was there a store here?" can no longer be answered.
    ///
    /// The rule is: a path that does not exist, or an empty directory, is a
    /// new store and gets `spec`'s hash written into its header plus a sidecar
    /// copy next to the db directory. Anything else is an existing store, and
    /// its header decides -- `spec` is ignored, because its blocks are already
    /// addressed by whatever the header says.
    ///
    /// # Errors
    ///
    /// [`MetaError::Header`] if the store has no header (it predates the
    /// format), or one this build refuses: foreign magic, an unsupported
    /// version, or a hash it does not have. There is no fallback; see
    /// [`super::store_header`].
    pub fn open_or_create<S: Store + 'static>(
        db_path: PathBuf,
        inlined_metadata_size: Option<usize>,
        spec: HeaderSpec,
        build: impl FnOnce(PathBuf) -> S,
    ) -> Result<(Self, StoreHeader), MetaError> {
        let init = store_header::classify_db_dir(&db_path)?;

        let meta = Self::new(build(db_path.clone()), inlined_metadata_size);

        let header = match init {
            StoreInit::Create => {
                let header =
                    StoreHeader::create(spec).map_err(|e| MetaError::header(&db_path, e))?;
                store_header::write_header(&*meta.store, &header)?;
                store_header::write_sidecar(&db_path, &header);
                tracing::debug!(
                    "created QSST store at {} (version {}, algo {}, width {})",
                    db_path.display(),
                    header.version(),
                    header.hash_algo(),
                    header.hash_width()
                );
                header
            }
            StoreInit::Open => store_header::read_header(&*meta.store, &db_path)?
                .ok_or_else(|| MetaError::header(&db_path, StoreHeaderError::Missing))?,
        };

        Ok((meta, header))
    }

    /// Returns the maximum length of the data that can be inlined in the metadata object.
    ///
    /// Inlining small data directly in metadata can improve performance by reducing the number
    /// of storage operations needed for small objects.
    ///
    /// # Returns
    /// The maximum number of bytes that can be inlined
    pub fn max_inlined_data_length(&self) -> usize {
        if self.inlined_metadata_size < Object::minimum_inline_metadata_size() {
            return 0;
        }
        self.inlined_metadata_size - Object::minimum_inline_metadata_size()
    }

    /// Returns a reference to the underlying store.
    ///
    /// This is used for creating additional stores that share the same storage backend,
    /// such as UserStore in multi-user mode.
    ///
    /// # Returns
    /// An Arc reference to the underlying Store implementation
    pub fn get_underlying_store(&self) -> Arc<dyn Store> {
        Arc::clone(&self.store)
    }

    /// Returns the tree which contains all the buckets.
    ///
    /// This tree is used to store the bucket lists and provide
    /// the CRUD operations for the bucket list.
    ///
    /// # Returns
    /// A tree with extended functionality for bucket operations or an error
    pub fn get_allbuckets_tree(&self) -> Result<Arc<dyn MetaTreeExt + Send + Sync>, MetaError> {
        self.store.tree_ext_open(DEFAULT_BUCKET_TREE)
    }

    /// Returns the tree for a specific bucket with extended methods.
    ///
    /// This tree provides additional methods for the bucket like range queries and listing operations.
    ///
    /// # Arguments
    /// * `name` - The name of the bucket
    ///
    /// # Returns
    /// A tree with extended functionality for the specified bucket or an error
    pub fn get_bucket_ext(
        &self,
        name: &str,
    ) -> Result<Arc<dyn MetaTreeExt + Send + Sync>, MetaError> {
        self.store.tree_ext_open(name)
    }

    /// Returns the block metadata tree.
    ///
    /// This tree is used to store the data block metadata, including reference counts
    /// and other block-specific information.
    ///
    /// # Returns
    /// A BlockTree instance or an error
    pub fn get_block_tree(&self) -> Result<BlockTree, MetaError> {
        let tree = self.store.tree_ext_open(DEFAULT_BLOCK_TREE)?;
        Ok(BlockTree { tree })
    }

    /// Returns a tree with the given name.
    ///
    /// This is typically used when the application needs to store custom metadata
    /// for a specific purpose outside the standard bucket/object model.
    ///
    /// # Arguments
    /// * `name` - The name of the tree to open
    ///
    /// # Returns
    /// A tree instance or an error
    pub fn get_tree(&self, name: &str) -> Result<Arc<dyn BaseMetaTree>, MetaError> {
        self.store.tree_open(name)
    }

    /// Returns the path metadata tree.
    ///
    /// This tree is used to store file path metadata and path-related information.
    ///
    /// # Returns
    /// A tree instance or an error
    pub fn get_path_tree(&self) -> Result<Arc<dyn BaseMetaTree>, MetaError> {
        self.store.tree_open(DEFAULT_PATH_TREE)
    }

    /// Checks if a bucket with the given name exists.
    ///
    /// # Arguments
    /// * `bucket_name` - The name of the bucket to check
    ///
    /// # Returns
    /// `true` if the bucket exists, `false` otherwise, or an error
    pub fn bucket_exists(&self, bucket_name: &str) -> Result<bool, MetaError> {
        self.store.tree_exists(bucket_name)
    }

    /// Deletes the bucket with the given name.
    ///
    /// If the bucket doesn't exist, this operation is a no-op and returns success.
    ///
    /// # Arguments
    /// * `name` - The name of the bucket to delete
    ///
    /// # Returns
    /// Success or an error if the deletion fails
    pub fn drop_bucket(&self, name: &str) -> Result<(), MetaError> {
        if self.bucket_exists(name)? {
            self.store.tree_delete(name)
        } else {
            Ok(())
        }
    }

    /// Inserts a raw representation of a bucket into the meta store.
    ///
    /// This method both adds the bucket metadata to the buckets tree and
    /// creates the bucket's own tree if it doesn't already exist.
    ///
    /// # Arguments
    /// * `bucket_name` - The name of the bucket
    /// * `raw_bucket` - The serialized bucket metadata
    ///
    /// # Returns
    /// Success or an error if the insertion fails
    ///
    /// # Errors
    ///
    /// [`MetaError::ReservedBucketName`] for a name starting with `_`. A
    /// bucket becomes a tree of its own name, so such a bucket would collide
    /// with the store's internal trees (`_STORE_HEADER`, `_BUCKETS`,
    /// `_BLOCKS`, `_PATHS`, `_MULTIPART_PARTS`) and hand a client the store's
    /// own bookkeeping. S3 bucket naming forbids these names anyway; this is
    /// the store enforcing it for every caller, respd included.
    pub fn insert_bucket(&self, bucket_name: &str, raw_bucket: Vec<u8>) -> Result<(), MetaError> {
        if bucket_name.starts_with('_') {
            return Err(MetaError::ReservedBucketName(bucket_name.to_string()));
        }

        // Insert the bucket metadata into the buckets tree
        let buckets = self.store.tree_open(DEFAULT_BUCKET_TREE)?;
        buckets.insert(bucket_name.as_bytes(), raw_bucket)?;

        // Create the bucket tree if it doesn't exist
        self.store.tree_open(bucket_name)?;

        Ok(())
    }

    /// Returns a list of all buckets in the system.
    ///
    /// # Returns
    /// A vector of BucketMeta objects or an error
    ///
    /// # Note
    /// This method currently loads all buckets into memory at once.
    /// TODO: This should be paginated and return a stream for better scalability.
    ///
    /// A record that fails to decode fails the whole listing: silently dropping
    /// it would report a bucket as gone while its tree and data are still there.
    pub fn list_buckets(&self) -> Result<Vec<BucketMeta>, MetaError> {
        let bucket = self.get_allbuckets_tree()?;
        bucket
            .iter_all()
            .map(|result| {
                let (_, value) = result?;
                // Just return the BucketMeta without the key
                BucketMeta::try_from(&*value).map_err(MetaError::from)
            })
            .collect()
    }

    /// Inserts a metadata Object into the specified bucket.
    ///
    /// # Arguments
    /// * `bucket_name` - The name of the bucket
    /// * `key` - The key to associate with the object
    /// * `raw_obj` - The serialized object metadata
    ///
    /// # Returns
    /// Success or an error if the insertion fails
    pub fn insert_meta(
        &self,
        bucket_name: &str,
        key: &str,
        raw_obj: Vec<u8>,
    ) -> Result<(), MetaError> {
        let bucket = self.get_bucket_ext(bucket_name)?;
        bucket.insert(key.as_bytes(), raw_obj)
    }

    /// Retrieves the Object metadata for the given bucket and key.
    ///
    /// This method returns the deserialized Object struct instead of raw bytes
    /// for better performance and convenience.
    ///
    /// # Arguments
    /// * `bucket_name` - The name of the bucket
    /// * `key` - The key to look up
    ///
    /// # Returns
    /// The Object if found, None if the key doesn't exist, or an error
    pub fn get_meta(&self, bucket_name: &str, key: &str) -> Result<Option<Object>, MetaError> {
        let bucket = self.get_bucket_ext(bucket_name)?;
        match bucket.get(key.as_bytes())? {
            Some(data) => {
                let obj = Object::try_from(&*data)?;
                Ok(Some(obj))
            }
            None => Ok(None),
        }
    }

    /// Deletes an object from a bucket and manages its associated blocks.
    ///
    /// This method performs the following operations:
    /// 1. Retrieves the object metadata from the bucket
    /// 2. Removes the object from the bucket
    /// 3. For each block in the object:
    ///    - Decrements its reference count
    ///    - If the reference count reaches zero, marks the block for deletion
    /// 4. Returns the list of blocks that should be physically deleted from storage
    ///
    /// # Arguments
    /// * `bucket` - The name of the bucket containing the object
    /// * `key` - The key of the object to delete
    ///
    /// # Returns
    /// A vector of Block objects that should be physically deleted, or an error
    ///
    /// # Note
    /// This method currently handles reference counting and block management directly.
    /// In the future, these operations should be abstracted into a transaction system.
    /// Delete an object from a bucket and decrement refcounts on its blocks.
    ///
    /// The bucket tree lives in this `MetaStore`; the block tree is passed
    /// explicitly because in multi-namespace deployments it lives in a
    /// separate `SharedBlockStore` (see `CasFS::new`). For single-namespace
    /// use, pass `self.get_block_tree()?`.
    pub fn delete_object(
        &self,
        bucket: &str,
        key: &str,
        block_tree: &BlockTree,
    ) -> Result<Vec<Block>, MetaError> {
        let bucket_tree = self.get_bucket_ext(bucket)?;

        // Get the object metadata
        let raw_object = match bucket_tree.get(key.as_bytes())? {
            Some(o) => o,
            None => return Ok(vec![]),
        };

        let obj = Object::try_from(&*raw_object)?;
        let mut to_delete: Vec<Block> = Vec::with_capacity(obj.blocks().len());

        tracing::debug!(
            bucket = bucket,
            key = key,
            block_count = obj.blocks().len(),
            "Deleting object"
        );

        // Delete the object from the bucket
        bucket_tree.remove(key.as_bytes())?;

        // Process all blocks in the object
        for block_id in obj.blocks() {
            match block_tree.get(block_id.as_slice())? {
                Some(block_data) => {
                    let mut block = Block::try_from(&*block_data)?;

                    // If this is the last reference to the block, delete it
                    if block.rc() == 1 {
                        tracing::debug!(
                            block_hash = %block_id.to_hex(),
                            rc = block.rc(),
                            "Block rc==1: deleting block and marking for file deletion"
                        );
                        block_tree.remove(block_id.as_slice())?;
                        to_delete.push(block);
                    } else {
                        // Otherwise decrement the reference count
                        let old_rc = block.rc();
                        block.decrement_refcount();
                        let new_rc = block.rc();
                        tracing::debug!(
                            block_hash = %block_id.to_hex(),
                            old_rc = old_rc,
                            new_rc = new_rc,
                            "Block rc>1: decrementing refcount"
                        );
                        block_tree.insert(block_id.as_slice(), block.to_vec())?;
                    }
                }
                None => {
                    tracing::warn!(
                        block_hash = %block_id.to_hex(),
                        "Block not found in tree during deletion"
                    );
                    continue; // Block not found, skip it
                }
            }
        }

        tracing::debug!(
            blocks_to_delete = to_delete.len(),
            "Finished processing object deletion"
        );

        Ok(to_delete)
    }

    /// Begins a new transaction for atomic operations.
    ///
    /// # Returns
    /// A new Transaction object
    pub fn begin_transaction(&self) -> Transaction {
        self.store.begin_transaction()
    }

    // ---- tfstor-extension: BEGIN ----
    /// Returns the total number of keys in the bucket tree.
    ///
    /// This is primarily used for monitoring and debugging purposes.
    ///
    /// # Returns
    /// The number of keys in the bucket tree, or the backend error that
    /// prevented counting them.
    ///
    /// Upstream returns a bare `usize` and `unwrap()`s the store call, turning
    /// any backend error into a panic in a debugging helper.
    pub fn num_keys(&self) -> Result<usize, MetaError> {
        self.store.num_keys(DEFAULT_BUCKET_TREE)
    }
    // ---- tfstor-extension: END ----

    /// Returns the total disk space used by the metadata store.
    ///
    /// # Returns
    /// The disk space usage in bytes
    pub fn disk_space(&self) -> u64 {
        self.store.disk_space()
    }
}

impl Debug for MetaStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaStore")
            .field("store", &"<Store>")
            .field("bucket_tree_name", &DEFAULT_BUCKET_TREE)
            .field("block_tree_name", &DEFAULT_BLOCK_TREE)
            .field("path_tree_name", &DEFAULT_PATH_TREE)
            .field("inlined_metadata_size", &self.inlined_metadata_size)
            .finish()
    }
}

/// `BlockTree` provides specialized operations for working with block metadata.
///
/// This struct wraps a MetaTreeExt and provides methods specific to block operations,
/// such as retrieving and manipulating block metadata.
#[derive(Clone)]
pub struct BlockTree {
    tree: Arc<dyn MetaTreeExt + Send + Sync>,
}

impl Debug for BlockTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockTree").finish()
    }
}

impl BlockTree {
    /// Retrieves a Block object for the given key.
    ///
    /// This method deserializes the raw block data into a Block struct.
    ///
    /// # Arguments
    /// * `key` - The key (typically a block hash) to look up
    ///
    /// # Returns
    /// The Block if found, None if the key doesn't exist, or an error
    pub fn get_block(&self, key: &[u8]) -> Result<Option<Block>, MetaError> {
        match self.tree.get(key)? {
            Some(data) => {
                let block = Block::try_from(&*data)?;
                Ok(Some(block))
            }
            None => Ok(None),
        }
    }

    /// Returns the number of blocks in the tree.
    ///
    /// This method is only available in test builds.
    ///
    /// # Returns
    /// The number of blocks or an error
    #[cfg(test)]
    pub fn len(&self) -> Result<usize, MetaError> {
        self.tree.len()
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> Result<bool, MetaError> {
        self.len().map(|n| n == 0)
    }

    /// Removes a block from the tree.
    ///
    /// # Arguments
    /// * `key` - The key of the block to remove
    ///
    /// # Returns
    /// Success or an error if the removal fails
    pub fn remove(&self, key: &[u8]) -> Result<(), MetaError> {
        self.tree.remove(key)
    }

    /// Inserts a block into the tree.
    ///
    /// # Arguments
    /// * `key` - The key to associate with the block
    /// * `value` - The serialized block data
    ///
    /// # Returns
    /// Success or an error if the insertion fails
    fn insert(&self, key: &[u8], value: Vec<u8>) -> Result<(), MetaError> {
        self.tree.insert(key, value)
    }

    /// Retrieves the raw block data for the given key.
    ///
    /// # Arguments
    /// * `key` - The key to look up
    ///
    /// # Returns
    /// The raw block data if found, None if the key doesn't exist, or an error
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MetaError> {
        self.tree.get(key)
    }

    /// Returns an iterator over all blocks in the tree.
    ///
    /// # Returns
    /// An iterator yielding (BlockId, Block) tuples
    pub fn iter_all(&self) -> Box<dyn Iterator<Item = Result<(BlockId, Block), MetaError>> + '_> {
        Box::new(self.tree.iter_all().map(|result| match result {
            Ok((key, value)) => {
                // The key *is* the block address, at whatever width the store
                // wrote it (16 or 32 bytes); anything else is a foreign key in
                // the block tree.
                let block_id = BlockId::from_slice(&key)
                    .map_err(|e| MetaError::OtherDBError(format!("Malformed block key: {e}")))?;
                // Deserialize the block
                Block::try_from(&*value)
                    .map(|block| (block_id, block))
                    .map_err(MetaError::from)
            }
            Err(e) => Err(e),
        }))
    }
}

/// Represents a database transaction that can be committed or rolled back.
///
/// It provides methods for writing blocks to the database and managing
/// the lifecycle of a transaction.
pub struct Transaction {
    // The backend storage implementation
    backend: Box<dyn TransactionBackend>,
}

impl Transaction {
    /// Creates a new Transaction with the given backend.
    ///
    /// # Arguments
    /// * `backend` - The transaction backend implementation
    ///
    /// # Returns
    /// A new Transaction instance
    pub(crate) fn new(backend: Box<dyn TransactionBackend>) -> Self {
        Self { backend }
    }

    /// Commits the transaction, making all changes permanent.
    ///
    /// # Returns
    /// Success or an error if the commit fails
    pub fn commit(mut self) -> Result<(), MetaError> {
        self.backend.commit()
    }

    /// Rolls back the transaction, discarding all changes.
    ///
    /// This method is called when the transaction should be aborted.
    pub fn rollback(mut self) {
        // Call the backend's rollback method for cleanup
        self.backend.rollback();
    }

    /// Writes a block to the database, handling reference counting and path creation.
    ///
    /// This method either creates a new block or updates an existing one's reference count.
    /// For new blocks, it also creates the necessary path entries.
    ///
    /// # Arguments
    /// * `block_hash` - The hash of the block to write
    /// * `data_len` - The length of the block data
    /// * `key_has_block` - Whether the key already has this block
    ///
    /// # Returns
    /// A tuple containing:
    /// * A boolean indicating whether the block was newly created
    /// * The Block object
    pub fn write_block(
        &mut self,
        block_hash: BlockId,
        data_len: usize,
        key_has_block: bool,
    ) -> Result<(bool, Block), MetaError> {
        // Check if the block already exists
        match self
            .backend
            .get(DEFAULT_BLOCK_TREE, block_hash.as_slice())?
        {
            // Block exists
            Some(block_data) => {
                let mut block = Block::try_from(&*block_data as &[u8])?;

                // If the key doesn't have this block, increment the reference count
                if !key_has_block {
                    let old_rc = block.rc();
                    block.increment_refcount();
                    let new_rc = block.rc();
                    tracing::debug!(
                        block_hash = %block_hash.to_hex(),
                        old_rc = old_rc,
                        new_rc = new_rc,
                        key_has_block = key_has_block,
                        "Block exists: incrementing refcount"
                    );
                    self.backend.insert(
                        DEFAULT_BLOCK_TREE,
                        block_hash.as_slice(),
                        block.to_vec(),
                    )?;
                } else {
                    tracing::debug!(
                        block_hash = %block_hash.to_hex(),
                        rc = block.rc(),
                        key_has_block = key_has_block,
                        "Block exists: NOT incrementing (key already has it)"
                    );
                }

                Ok((false, block))
            }
            // Block doesn't exist, create it
            None => {
                // Find the shortest prefix of the hash that is not already
                // claimed by a different block; that prefix becomes this
                // block's on-disk path. The full-width prefix is part of the
                // search (it was not, which is how prefix exhaustion used to
                // leave idx at 0 and write an empty path that later panicked
                // in Block::disk_path).
                let width = block_hash.len();
                let mut idx = 0;
                for index in 1..=width {
                    match self
                        .backend
                        .get(DEFAULT_PATH_TREE, &block_hash.as_slice()[..index])
                    {
                        Ok(Some(existing)) => {
                            // The full-width key can only be held by this very
                            // hash, since a path entry stores the hash that
                            // owns it. Then the path is already ours to use.
                            if index == width && existing == block_hash.as_slice() {
                                idx = index;
                            }
                            continue;
                        }
                        Ok(None) => {
                            idx = index;
                            break;
                        }
                        Err(e) => return Err(MetaError::OtherDBError(e.to_string())),
                    }
                }

                if idx == 0 {
                    // Every prefix up to and including the full hash is taken
                    // by another hash. For a real hash this cannot happen: the
                    // full-width prefix is the hash itself, so a different hash
                    // holding it would be a hash collision. Refuse rather than
                    // write a zero-length path.
                    return Err(MetaError::OtherDBError(format!(
                        "path tree exhausted for block {}: every prefix is taken by another hash",
                        block_hash.to_hex()
                    )));
                }

                let path = block_hash.as_slice()[..idx].to_vec();

                // insert this new path
                self.backend
                    .insert(DEFAULT_PATH_TREE, &path, block_hash.as_slice().to_vec())?;

                // insert this new block
                let block = Block::new(data_len, path);

                tracing::debug!(
                    block_hash = %block_hash.to_hex(),
                    rc = block.rc(),
                    data_len = data_len,
                    key_has_block = key_has_block,
                    "Creating new block with rc=1"
                );

                self.backend
                    .insert(DEFAULT_BLOCK_TREE, block_hash.as_slice(), block.to_vec())?;

                Ok((true, block))
            }
        }
    }
}

/// Abstracts the storage backend operations needed by Transaction.
///
/// This trait defines the interface that any storage backend must implement
/// to support transactions in the metadata store.
pub(crate) trait TransactionBackend: Send + Sync {
    /// Commits the transaction, making all changes permanent.
    ///
    /// # Returns
    /// Success or an error if the commit fails
    fn commit(&mut self) -> Result<(), MetaError>;

    /// Rolls back the transaction, discarding all changes.
    fn rollback(&mut self);

    /// Retrieves a value from the specified tree.
    ///
    /// # Arguments
    /// * `tree_name` - The name of the tree to query
    /// * `key` - The key to look up
    ///
    /// # Returns
    /// The value if found, None if the key doesn't exist, or an error
    fn get(&mut self, tree_name: &str, key: &[u8]) -> Result<Option<Vec<u8>>, MetaError>;

    /// Inserts a value into the specified tree.
    ///
    /// # Arguments
    /// * `tree_name` - The name of the tree
    /// * `key` - The key to associate with the value
    /// * `data` - The value to insert
    ///
    /// # Returns
    /// Success or an error if the insertion fails
    fn insert(&mut self, tree_name: &str, key: &[u8], data: Vec<u8>) -> Result<(), MetaError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metastore::{BLOCKID_SIZE, FjallStore};
    use tempfile::{TempDir, tempdir};

    fn test_store() -> (MetaStore, TempDir) {
        let dir = tempdir().unwrap();
        let store = FjallStore::new(dir.path().to_path_buf(), Some(1), None);
        (MetaStore::new(store, None), dir)
    }

    /// Claims every prefix of `hash` of length `1..=upto` in the path tree for
    /// a *different* block, which is what a run of unlucky collisions would
    /// leave behind.
    fn claim_prefixes(meta: &MetaStore, hash: &BlockId, upto: usize) {
        let path_tree = meta.get_path_tree().unwrap();
        let squatter = BlockId::from([0xffu8; BLOCKID_SIZE]);
        for index in 1..=upto {
            path_tree
                .insert(&hash.as_slice()[..index], squatter.as_slice().to_vec())
                .unwrap();
        }
    }

    /// Regression: with every prefix shorter than the full hash taken, the
    /// block used to be written with a zero-length path, which then panicked
    /// in `Block::disk_path`. It must fall back to the full-width path.
    #[test]
    fn write_block_falls_back_to_full_width_path() {
        let (meta, dir) = test_store();
        let hash = BlockId::from([0xaau8; BLOCKID_SIZE]);
        claim_prefixes(&meta, &hash, BLOCKID_SIZE - 1);

        let mut tx = meta.begin_transaction();
        let (new, block) = tx.write_block(hash, 42, false).unwrap();
        tx.commit().unwrap();

        assert!(new);
        assert!(!block.path().is_empty(), "block path must never be empty");
        assert_eq!(block.path(), hash.as_slice());
        // The path is usable: this panics on an empty path.
        let _ = block.disk_path(dir.path().to_path_buf());
    }

    /// If even the full-width key is held by a different hash -- impossible for
    /// a real hash, since that key *is* the hash -- writing must fail loudly
    /// instead of storing an empty path.
    #[test]
    fn write_block_refuses_when_every_prefix_is_taken() {
        let (meta, _dir) = test_store();
        let hash = BlockId::from([0xaau8; BLOCKID_SIZE]);
        claim_prefixes(&meta, &hash, BLOCKID_SIZE);

        let mut tx = meta.begin_transaction();
        let err = tx.write_block(hash, 42, false).unwrap_err();
        tx.rollback();

        match err {
            MetaError::OtherDBError(msg) => assert!(
                msg.contains("path tree exhausted"),
                "unexpected message: {msg}"
            ),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    /// A bucket becomes a tree of its own name, so a bucket named like one of
    /// the store's internal trees would hand a client the store's bookkeeping.
    /// Creation must refuse the whole `_` namespace, and refuse it before
    /// anything is written.
    #[test]
    fn insert_bucket_refuses_reserved_names() {
        let (meta, _dir) = test_store();

        for name in [
            "_STORE_HEADER",
            "_BLOCKS",
            "_PATHS",
            "_MULTIPART_PARTS",
            "_",
        ] {
            let raw = BucketMeta::new(name.to_string()).to_vec();
            match meta.insert_bucket(name, raw).unwrap_err() {
                MetaError::ReservedBucketName(refused) => assert_eq!(refused, name),
                other => panic!("unexpected error for {name}: {other:?}"),
            }
        }

        // The refusal is not a side effect of the name existing already: a
        // name nothing internal uses is refused just the same, and no tree is
        // left behind.
        let raw = BucketMeta::new("_private".to_string()).to_vec();
        assert!(meta.insert_bucket("_private", raw).is_err());
        assert!(!meta.bucket_exists("_private").unwrap());

        // An ordinary name is unaffected.
        let raw = BucketMeta::new("photos".to_string()).to_vec();
        meta.insert_bucket("photos", raw).unwrap();
        assert!(meta.bucket_exists("photos").unwrap());
    }

    /// The ordinary case: a fresh hash takes the one-byte prefix, and a second
    /// hash sharing that first byte takes the two-byte prefix.
    #[test]
    fn write_block_uses_shortest_free_prefix() {
        let (meta, _dir) = test_store();
        let first = BlockId::from([0x11u8; BLOCKID_SIZE]);
        let mut second_bytes = [0x11u8; BLOCKID_SIZE];
        second_bytes[1] = 0x22;
        let second = BlockId::from(second_bytes);

        let mut tx = meta.begin_transaction();
        let (_, first_block) = tx.write_block(first, 1, false).unwrap();
        tx.commit().unwrap();
        assert_eq!(first_block.path(), &[0x11]);

        let mut tx = meta.begin_transaction();
        let (_, second_block) = tx.write_block(second, 1, false).unwrap();
        tx.commit().unwrap();
        assert_eq!(second_block.path(), &[0x11, 0x22]);
    }
}
