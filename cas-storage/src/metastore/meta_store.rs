use std::sync::Arc;

use super::{Block, MetaError, MetaTreeExt, Store};

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

/// Tree holding the block records, in the shared blocks DB.
///
/// Public for the same reason as [`MULTIPART_PARTS_TREE`]: ADR 0005's
/// walkers and repairs address this tree by name, so the name is part of
/// the store's contract.
pub const DEFAULT_BLOCK_TREE: &str = "_BLOCKS";

/// Tree holding the in-flight multipart part records, in the shared blocks
/// DB next to `_BLOCKS`.
///
/// Public because it is a reference holder: ADR 0005's recount must walk it
/// unconditionally (a live upload's parts are the only thing referencing its
/// blocks until the upload completes), so the name is part of the store's
/// contract rather than a literal each caller spells for itself.
pub const MULTIPART_PARTS_TREE: &str = "_MULTIPART_PARTS";

/// Tree holding the in-flight multipart UPLOAD records, in the shared blocks
/// DB next to `_MULTIPART_PARTS`.
///
/// Public because its records are the lifecycle authority ADR 0003 gives
/// multipart: an upload exists iff its record does, complete and abort race
/// for it through [`Transaction::take_upload`], and both the stale-upload GC
/// and fsck address the tree by name.
pub const UPLOADS_TREE: &str = "_UPLOADS";

/// Tree holding the per-bucket usage counter: the logical bytes its records
/// add up to, one 8-byte little-endian row per bucket name.
///
/// Lives in the namespace database beside `_BUCKETS` and the bucket trees
/// themselves, which is what lets one transaction move a record and its
/// bucket's counter together. Reserved-prefixed, so no bucket can collide
/// with it and fsck's holder walk skips it like every other `_` tree.
///
/// A row of its own rather than a field of the bucket record: the counter
/// moves on every write, and a read-modify-write of the whole metadata
/// record would put a namespace's password and flags in the path of every
/// SET -- where a configuration change landing between the read and the
/// write would be silently overwritten.
const DEFAULT_USAGE_TREE: &str = "_USAGE";

/// Reads a usage counter row.
///
/// The width is exact: a row of any other length is damage, not a variant,
/// and answering a quota from a misread number is worse than refusing to.
fn decode_usage(bucket: &str, raw: &[u8]) -> Result<u64, MetaError> {
    let bytes: [u8; 8] = raw.try_into().map_err(|_| {
        MetaError::OtherDBError(format!(
            "the usage counter of {bucket} is {} bytes, not 8",
            raw.len()
        ))
    })?;
    Ok(u64::from_le_bytes(bytes))
}

/// `BlockTree` provides specialized operations for working with block metadata.
///
/// This struct wraps a MetaTreeExt and provides methods specific to block operations,
/// such as retrieving and manipulating block metadata.
#[derive(Clone)]
pub struct BlockTree {
    tree: Arc<dyn MetaTreeExt + Send + Sync>,
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

mod block_tree;
mod ops;
#[cfg(test)]
mod tests;
mod transaction;
