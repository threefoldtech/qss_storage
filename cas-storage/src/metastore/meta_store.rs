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

/// Tree holding the in-flight multipart part records, in the shared blocks
/// DB next to `_BLOCKS`.
///
/// Public because it is a reference holder: ADR 0005's recount must walk it
/// unconditionally (a live upload's parts are the only thing referencing its
/// blocks until the upload completes), so the name is part of the store's
/// contract rather than a literal each caller spells for itself.
pub const MULTIPART_PARTS_TREE: &str = "_MULTIPART_PARTS";

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

    /// Returns the name of every tree in the store, reserved `_`-prefixed
    /// trees included.
    ///
    /// The store itself is the authority on which trees exist. ADR 0005's
    /// holder enumeration starts here rather than at [`Self::list_buckets`],
    /// because a bucket whose teardown crashed after its `_BUCKETS` row was
    /// removed still has an object tree, and that tree still holds block
    /// references.
    ///
    /// # Returns
    /// Every tree name, in no particular order, or an error
    pub fn list_trees(&self) -> Result<Vec<String>, MetaError> {
        self.store.list_trees()
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
    /// `_BLOCKS`, `_MULTIPART_PARTS`) and hand a client the store's
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

/// How a striped block decrement resolved; see
/// [`Transaction::decrement_block_rc`].
#[derive(Debug)]
pub enum BlockDecrement {
    /// No record for the block existed. DELETE logs and moves on: the
    /// reference the object held was already unaccounted.
    Missing,
    /// The reference count was above one and has been decremented; the
    /// block (and its file) live on.
    Decremented(Block),
    /// This was the last reference: the record has been removed in this
    /// transaction. After committing, the caller must unlink the file at
    /// the block's recorded depth -- inside the same stripe hold.
    Removed(Block),
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

    /// The dedup half of the ADR 0006 write protocol: if a record for
    /// `block_hash` exists, bump its reference count by one INSIDE this
    /// transaction and return the updated block; otherwise return `None`
    /// and change nothing.
    ///
    /// The existence check and the rc mutation are one transactional
    /// read-modify-write (hard rule 2) -- never split this into a pre-tx
    /// read with a blind write. Every dedup hit bumps: the old
    /// `key_has_block` skip undercounted references and is gone (ADR 0006).
    ///
    /// A record flagged degraded reports as absent (`None`): its file is
    /// gone, so deduplicating against it would commit another damaged
    /// object (ADR 0005). The caller falls through to the insert path,
    /// which heals the record instead.
    ///
    /// The caller must hold the block's stripe (hard rule 1).
    pub fn bump_block_rc(&mut self, block_hash: BlockId) -> Result<Option<Block>, MetaError> {
        match self
            .backend
            .get(DEFAULT_BLOCK_TREE, block_hash.as_slice())?
        {
            None => Ok(None),
            Some(block_data) => {
                let mut block = Block::try_from(&*block_data as &[u8])?;
                if block.is_degraded() {
                    tracing::debug!(
                        block_hash = %block_hash.to_hex(),
                        rc = block.rc(),
                        "Block record is degraded: absent for dedup, heal on insert"
                    );
                    return Ok(None);
                }
                let old_rc = block.rc();
                block.increment_refcount();
                tracing::debug!(
                    block_hash = %block_hash.to_hex(),
                    old_rc = old_rc,
                    new_rc = block.rc(),
                    "Block exists: incrementing refcount"
                );
                self.backend
                    .insert(DEFAULT_BLOCK_TREE, block_hash.as_slice(), block.to_vec())?;
                Ok(Some(block))
            }
        }
    }

    /// The DELETE-side object step (ADR 0006): reads AND removes the object
    /// record for `key` inside this transaction, so read+remove commit as
    /// one atomic pair -- that is what defeats a concurrent double-DELETE
    /// of one key double-decrementing block refcounts (verified defect 5's
    /// sibling).
    ///
    /// Returns the removed object, or `None` (and no change) if the key was
    /// absent -- DELETE is idempotent.
    pub fn take_object(&mut self, bucket: &str, key: &str) -> Result<Option<Object>, MetaError> {
        let Some(raw) = self.backend.get(bucket, key.as_bytes())? else {
            return Ok(None);
        };
        let obj = Object::try_from(&*raw)?;
        self.backend.remove(bucket, key.as_bytes())?;
        Ok(Some(obj))
    }

    /// The DELETE-side block step (ADR 0006): re-checks the record and
    /// applies one reference decrement inside this transaction.
    ///
    /// The re-check matters: between the object removal and this call,
    /// other writers may have bumped or even removed-and-recreated the
    /// record, so the caller's earlier knowledge is stale. Under the stripe
    /// this read-modify-write is race-free (hard rules 1 and 2).
    ///
    /// On [`BlockDecrement::Removed`] the caller must commit FIRST and then
    /// unlink the file at the returned block's depth, all inside the same
    /// stripe hold and the same blocking closure (hard rule 4) -- a
    /// cancellable await between decrement and unlink is how a detached
    /// unlink once deleted a freshly rewritten block.
    ///
    /// A degraded record (ADR 0005) needs no special case: it accounts for
    /// real holders, so it decrements like any other, and the unlink that
    /// follows its last reference tolerates ENOENT -- having no file is
    /// exactly what degraded means.
    pub fn decrement_block_rc(&mut self, block_hash: BlockId) -> Result<BlockDecrement, MetaError> {
        let Some(raw) = self
            .backend
            .get(DEFAULT_BLOCK_TREE, block_hash.as_slice())?
        else {
            return Ok(BlockDecrement::Missing);
        };
        let mut block = Block::try_from(&*raw)?;

        if block.rc() == 1 {
            tracing::debug!(
                block_hash = %block_hash.to_hex(),
                "Block rc==1: removing record; caller unlinks the file"
            );
            self.backend
                .remove(DEFAULT_BLOCK_TREE, block_hash.as_slice())?;
            return Ok(BlockDecrement::Removed(block));
        }

        let old_rc = block.rc();
        block.decrement_refcount();
        tracing::debug!(
            block_hash = %block_hash.to_hex(),
            old_rc = old_rc,
            new_rc = block.rc(),
            "Block rc>1: decrementing refcount"
        );
        self.backend
            .insert(DEFAULT_BLOCK_TREE, block_hash.as_slice(), block.to_vec())?;
        Ok(BlockDecrement::Decremented(block))
    }

    /// The new-block half of the ADR 0006 write protocol: write the record
    /// for `block_hash`, whose file is now durable at `depth`.
    ///
    /// Callable only after the block's file is durable at its final path
    /// (hard rule 5) and only while holding the block's stripe (hard
    /// rule 1). Under the stripe there are exactly two states to meet,
    /// because only stripe holders write block records and
    /// [`bump_block_rc`](Self::bump_block_rc) has just reported this one
    /// absent-or-degraded:
    ///
    /// - no record: insert a fresh one at rc = 1;
    /// - a degraded record (ADR 0005): the file we just wrote is the heal.
    ///   Clear the flag, take the depth the file actually landed at -- the
    ///   heal's placement need not match the depth the dead record named,
    ///   and the record must follow the file -- and add our own reference
    ///   to the holders it was keeping accounted for.
    pub fn insert_new_block(
        &mut self,
        block_hash: BlockId,
        data_len: usize,
        depth: u8,
    ) -> Result<Block, MetaError> {
        let present = match self
            .backend
            .get(DEFAULT_BLOCK_TREE, block_hash.as_slice())?
        {
            Some(raw) => Some(Block::try_from(&*raw)?),
            None => None,
        };

        let block = match present {
            None => {
                tracing::debug!(
                    block_hash = %block_hash.to_hex(),
                    data_len = data_len,
                    depth = depth,
                    "Creating new block with rc=1"
                );
                Block::new(data_len, depth)
            }
            Some(mut present) => {
                debug_assert!(
                    present.is_degraded(),
                    "a live record under our own stripe hold: the bump would have taken it"
                );
                debug_assert_eq!(
                    present.size(),
                    data_len,
                    "same block id, same content, same size"
                );
                tracing::debug!(
                    block_hash = %block_hash.to_hex(),
                    rc = present.rc(),
                    old_depth = present.depth(),
                    depth = depth,
                    "Healing degraded block: clearing the flag and adding our reference"
                );
                // Only the degraded bit moves; the reserved bits are not
                // this build's to interpret, so they are carried over.
                present.set_degraded(false);
                Block::from_parts(data_len, depth, present.rc() + 1, present.flags())
            }
        };

        self.backend
            .insert(DEFAULT_BLOCK_TREE, block_hash.as_slice(), block.to_vec())?;

        Ok(block)
    }

    /// Writes `block`'s record for `block_hash` verbatim, replacing whatever
    /// is there.
    ///
    /// The raw record write fsck's repair actions and the crash fixtures
    /// need: rc, depth and flags are whatever the caller states, so none of
    /// the protocol's accounting rules apply. Daemon paths use the striped
    /// read-modify-writes above instead.
    ///
    /// Test-gated until fsck's repair actions land (ADR 0005 component 5);
    /// today the crash fixtures are its only caller.
    #[cfg(test)]
    pub(crate) fn put_block_record(
        &mut self,
        block_hash: BlockId,
        block: &Block,
    ) -> Result<(), MetaError> {
        self.backend
            .insert(DEFAULT_BLOCK_TREE, block_hash.as_slice(), block.to_vec())
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

    /// Removes a key from the specified tree. Removing an absent key is not
    /// an error; the transaction's atomicity is what callers rely on.
    fn remove(&mut self, tree_name: &str, key: &[u8]) -> Result<(), MetaError>;
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

    /// A record fsck marked degraded: holders still counted, bytes gone.
    fn degraded_record(size: usize, depth: u8, rc: usize) -> Block {
        let mut block = Block::from_parts(size, depth, rc, 0);
        block.set_degraded(true);
        block
    }

    /// A new block records the depth the caller chose; the derived disk path
    /// follows it.
    #[test]
    fn insert_new_block_records_the_given_depth() {
        let (meta, dir) = test_store();
        let hash = BlockId::from([0xaau8; BLOCKID_SIZE]);

        let mut tx = meta.begin_transaction();
        let block = tx.insert_new_block(hash, 42, 3).unwrap();
        tx.commit().unwrap();

        assert_eq!(block.rc(), 1);
        assert_eq!(block.depth(), 3);
        assert_eq!(
            block.disk_path(&hash, dir.path().to_path_buf()),
            crate::metastore::block_disk_path(&hash, 3, dir.path().to_path_buf())
        );
    }

    /// A dedup bump increments rc and keeps the ORIGINAL depth -- that is
    /// where the file is.
    #[test]
    fn bump_block_rc_hits_and_keeps_the_recorded_depth() {
        let (meta, _dir) = test_store();
        let hash = BlockId::from([0xabu8; BLOCKID_SIZE]);

        // No record yet: the bump reports a miss and mutates nothing.
        let mut tx = meta.begin_transaction();
        assert!(tx.bump_block_rc(hash).unwrap().is_none());
        tx.rollback();

        let mut tx = meta.begin_transaction();
        tx.insert_new_block(hash, 42, 2).unwrap();
        tx.commit().unwrap();

        let mut tx = meta.begin_transaction();
        let block = tx.bump_block_rc(hash).unwrap().expect("record exists");
        tx.commit().unwrap();

        assert_eq!(block.rc(), 2);
        assert_eq!(block.depth(), 2, "dedup must not move the block");
    }

    /// A degraded record is absent for dedup: the bump reports a miss and
    /// mutates nothing, so the caller writes the file and heals it.
    #[test]
    fn bump_block_rc_treats_a_degraded_record_as_absent() {
        let (meta, _dir) = test_store();
        let hash = BlockId::from([0xadu8; BLOCKID_SIZE]);

        let mut tx = meta.begin_transaction();
        tx.put_block_record(hash, &degraded_record(42, 2, 3))
            .unwrap();
        tx.commit().unwrap();

        let mut tx = meta.begin_transaction();
        assert!(tx.bump_block_rc(hash).unwrap().is_none());
        tx.commit().unwrap();

        let block = meta
            .get_block_tree()
            .unwrap()
            .get_block(hash.as_slice())
            .unwrap()
            .unwrap();
        assert_eq!(block.rc(), 3, "a reported miss must not bump");
        assert!(block.is_degraded());
    }

    /// The insert path heals a degraded record: the flag clears, the record
    /// follows the file to the depth it was written at, and the writer's own
    /// reference is added to the holders already counted.
    #[test]
    fn insert_new_block_heals_a_degraded_record() {
        let (meta, _dir) = test_store();
        let hash = BlockId::from([0xaeu8; BLOCKID_SIZE]);

        let mut tx = meta.begin_transaction();
        tx.put_block_record(hash, &degraded_record(42, 1, 4))
            .unwrap();
        tx.commit().unwrap();

        let mut tx = meta.begin_transaction();
        let healed = tx.insert_new_block(hash, 42, 3).unwrap();
        tx.commit().unwrap();

        assert!(!healed.is_degraded());
        assert_eq!(healed.rc(), 5, "four holders plus the healing writer");
        assert_eq!(healed.depth(), 3, "the record follows the new file");

        let stored = meta
            .get_block_tree()
            .unwrap()
            .get_block(hash.as_slice())
            .unwrap()
            .unwrap();
        assert_eq!(stored.rc(), 5);
        assert_eq!(stored.depth(), 3);
        assert!(!stored.is_degraded());
    }

    /// A bump inside a rolled-back transaction leaves the record untouched:
    /// the RMW is transactional, not a blind write.
    #[test]
    fn bump_block_rc_rolls_back_with_the_transaction() {
        let (meta, _dir) = test_store();
        let hash = BlockId::from([0xacu8; BLOCKID_SIZE]);

        let mut tx = meta.begin_transaction();
        tx.insert_new_block(hash, 42, 1).unwrap();
        tx.commit().unwrap();

        let mut tx = meta.begin_transaction();
        tx.bump_block_rc(hash).unwrap().expect("record exists");
        tx.rollback();

        let block = meta
            .get_block_tree()
            .unwrap()
            .get_block(hash.as_slice())
            .unwrap()
            .unwrap();
        assert_eq!(block.rc(), 1, "rolled-back bump must not persist");
    }

    /// A bucket becomes a tree of its own name, so a bucket named like one of
    /// the store's internal trees would hand a client the store's bookkeeping.
    /// Creation must refuse the whole `_` namespace, and refuse it before
    /// anything is written.
    #[test]
    fn insert_bucket_refuses_reserved_names() {
        let (meta, _dir) = test_store();

        for name in [
            store_header::STORE_HEADER_TREE,
            DEFAULT_BLOCK_TREE,
            MULTIPART_PARTS_TREE,
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

    /// The holder enumeration ADR 0005 recounts from: every bucket tree and
    /// every reserved tree the store keeps must be named, because a bucket
    /// whose `_BUCKETS` row is gone still holds block references through its
    /// object tree.
    #[test]
    fn list_trees_names_bucket_and_reserved_trees() {
        let dir = tempdir().unwrap();
        let (meta, _header) =
            MetaStore::open_or_create(dir.path().join("db"), Some(1), HeaderSpec::default(), |p| {
                FjallStore::new(p, Some(1), None)
            })
            .unwrap();

        for name in ["photos", "videos"] {
            meta.insert_bucket(name, BucketMeta::new(name.to_string()).to_vec())
                .unwrap();
        }
        // The shared trees exist from the moment they are opened.
        meta.get_block_tree().unwrap();
        meta.get_tree(MULTIPART_PARTS_TREE).unwrap();

        let trees = meta.list_trees().unwrap();
        for expected in [
            "photos",
            "videos",
            store_header::STORE_HEADER_TREE,
            DEFAULT_BUCKET_TREE,
            DEFAULT_BLOCK_TREE,
            MULTIPART_PARTS_TREE,
        ] {
            assert!(
                trees.iter().any(|name| name == expected),
                "{expected} missing from {trees:?}"
            );
        }

        // A half-deleted bucket -- its row removed, its tree left behind --
        // is exactly what the listing must keep showing.
        let buckets = meta.get_allbuckets_tree().unwrap();
        buckets.remove(b"videos").unwrap();
        assert!(
            meta.list_trees()
                .unwrap()
                .iter()
                .any(|name| name == "videos"),
            "the tree outlives its _BUCKETS row"
        );
    }

    /// Two hashes sharing a leading byte can both live at depth 1: their
    /// full-id filenames can never collide, so no allocator is involved.
    #[test]
    fn shared_prefix_needs_no_allocation() {
        let (meta, dir) = test_store();
        let first = BlockId::from([0x11u8; BLOCKID_SIZE]);
        let mut second_bytes = [0x11u8; BLOCKID_SIZE];
        second_bytes[1] = 0x22;
        let second = BlockId::from(second_bytes);

        let mut tx = meta.begin_transaction();
        let first_block = tx.insert_new_block(first, 1, 1).unwrap();
        tx.commit().unwrap();

        let mut tx = meta.begin_transaction();
        let second_block = tx.insert_new_block(second, 1, 1).unwrap();
        tx.commit().unwrap();

        let p1 = first_block.disk_path(&first, dir.path().to_path_buf());
        let p2 = second_block.disk_path(&second, dir.path().to_path_buf());
        assert_eq!(p1.parent(), p2.parent(), "same depth-1 dir");
        assert_ne!(p1, p2, "full-id names never collide");
    }
}
