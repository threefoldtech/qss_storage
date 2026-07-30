use std::convert::TryFrom;
use std::fmt::Debug;
use std::sync::Arc;

use super::{
    BLOCKID_SIZE, BaseMetaTree, Block, BlockID, BucketMeta, MetaError, MetaTreeExt, Object, Store,
};

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
    pub fn insert_bucket(&self, bucket_name: &str, raw_bucket: Vec<u8>) -> Result<(), MetaError> {
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
            match block_tree.get(block_id)? {
                Some(block_data) => {
                    let mut block = Block::try_from(&*block_data)?;

                    // If this is the last reference to the block, delete it
                    if block.rc() == 1 {
                        tracing::debug!(
                            block_hash = %hex::encode(block_id),
                            rc = block.rc(),
                            "Block rc==1: deleting block and marking for file deletion"
                        );
                        block_tree.remove(block_id)?;
                        to_delete.push(block);
                    } else {
                        // Otherwise decrement the reference count
                        let old_rc = block.rc();
                        block.decrement_refcount();
                        let new_rc = block.rc();
                        tracing::debug!(
                            block_hash = %hex::encode(block_id),
                            old_rc = old_rc,
                            new_rc = new_rc,
                            "Block rc>1: decrementing refcount"
                        );
                        block_tree.insert(block_id, block.to_vec())?;
                    }
                }
                None => {
                    tracing::warn!(
                        block_hash = %hex::encode(block_id),
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
    /// An iterator yielding (BlockID, Block) tuples
    pub fn iter_all(&self) -> Box<dyn Iterator<Item = Result<(BlockID, Block), MetaError>> + '_> {
        use crate::metastore::block::BLOCKID_SIZE;

        Box::new(self.tree.iter_all().map(|result| match result {
            Ok((key, value)) => {
                // Parse the block ID from the key
                let block_id: BlockID = if key.len() >= BLOCKID_SIZE {
                    let mut id = [0u8; BLOCKID_SIZE];
                    id.copy_from_slice(&key[..BLOCKID_SIZE]);
                    id
                } else {
                    return Err(MetaError::OtherDBError("Malformed block key".to_string()));
                };
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
        block_hash: BlockID,
        data_len: usize,
        key_has_block: bool,
    ) -> Result<(bool, Block), MetaError> {
        // Check if the block already exists
        match self.backend.get(DEFAULT_BLOCK_TREE, &block_hash)? {
            // Block exists
            Some(block_data) => {
                let mut block = Block::try_from(&*block_data as &[u8])?;

                // If the key doesn't have this block, increment the reference count
                if !key_has_block {
                    let old_rc = block.rc();
                    block.increment_refcount();
                    let new_rc = block.rc();
                    tracing::debug!(
                        block_hash = %hex::encode(block_hash),
                        old_rc = old_rc,
                        new_rc = new_rc,
                        key_has_block = key_has_block,
                        "Block exists: incrementing refcount"
                    );
                    self.backend
                        .insert(DEFAULT_BLOCK_TREE, &block_hash, block.to_vec())?;
                } else {
                    tracing::debug!(
                        block_hash = %hex::encode(block_hash),
                        rc = block.rc(),
                        key_has_block = key_has_block,
                        "Block exists: NOT incrementing (key already has it)"
                    );
                }

                Ok((false, block))
            }
            // Block doesn't exist, create it
            None => {
                let mut idx = 0;
                for index in 1..BLOCKID_SIZE {
                    match self.backend.get(DEFAULT_PATH_TREE, &block_hash[..index]) {
                        Ok(Some(_)) => continue,
                        Ok(None) => {
                            idx = index;
                            break;
                        }
                        Err(e) => return Err(MetaError::OtherDBError(e.to_string())),
                    }
                }

                // insert this new path
                self.backend
                    .insert(DEFAULT_PATH_TREE, &block_hash[..idx], block_hash.to_vec())?;

                // insert this new block
                let block = Block::new(data_len, block_hash[..idx].to_vec());

                tracing::debug!(
                    block_hash = %hex::encode(block_hash),
                    rc = block.rc(),
                    data_len = data_len,
                    key_has_block = key_has_block,
                    "Creating new block with rc=1"
                );

                self.backend
                    .insert(DEFAULT_BLOCK_TREE, &block_hash, block.to_vec())?;

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
