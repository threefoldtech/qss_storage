use std::str::FromStr;
use std::{fmt::Debug, sync::Arc};

use super::{MetaError, Transaction, object::Object};

/// `BaseMetaTree` defines the core operations for a metadata tree storage.
///
/// This trait provides the fundamental operations needed to interact with a key-value
/// storage system, including inserting, removing, and retrieving values.
pub trait BaseMetaTree: Send + Sync {
    /// Inserts a key-value pair into the tree.
    ///
    /// # Arguments
    /// * `key` - The key as a byte slice
    /// * `value` - The value as a vector of bytes
    ///
    /// # Returns
    /// * `Result<(), MetaError>` - Success or an error if the insertion fails
    fn insert(&self, key: &[u8], value: Vec<u8>) -> Result<(), MetaError>;

    /// Removes a key and its associated value from the tree.
    ///
    /// # Arguments
    /// * `key` - The key to remove as a byte slice
    ///
    /// # Returns
    /// * `Result<bool, MetaError>` - Whether the key existed (best-effort
    ///   under concurrent writers), or an error if the removal fails. The
    ///   answer is what Redis DEL semantics count.
    fn remove(&self, key: &[u8]) -> Result<bool, MetaError>;

    /// Checks if a key exists in the tree.
    ///
    /// # Arguments
    /// * `key` - The key to check as a byte slice
    ///
    /// # Returns
    /// * `Result<bool, MetaError>` - True if the key exists, false otherwise, or an error
    fn contains_key(&self, key: &[u8]) -> Result<bool, MetaError>;

    /// Retrieves a value for the given key.
    ///
    /// # Arguments
    /// * `key` - The key to look up as a byte slice
    ///
    /// # Returns
    /// * `Result<Option<Vec<u8>>, MetaError>` - The value if found, None if the key doesn't exist, or an error
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MetaError>;

    // ---- tfstor-extension: BEGIN ----
    // Upstream marks `len`/`is_empty` as `#[cfg(test)]`. We need them at
    // runtime for respd's LENGTH (key size) and DBSIZE (namespace size)
    // commands. Drop these markers and the cfg gate once upstreamed.
    /// Returns the number of key-value pairs in the tree.
    fn len(&self) -> Result<usize, MetaError>;

    /// Returns true if the tree contains no key-value pairs.
    fn is_empty(&self) -> Result<bool, MetaError> {
        self.len().map(|n| n == 0)
    }
    // ---- tfstor-extension: END ----
}

/// Type alias for a boxed iterator over key-value pairs.
pub type KeyValuePairs = Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), MetaError>> + Send>;

/// `MetaTreeExt` extends the `BaseMetaTree` with additional operations.
///
/// This trait provides more advanced functionality like iteration and filtering
/// on top of the basic tree operations.
pub trait MetaTreeExt: BaseMetaTree {
    /// Iterates over all key-value pairs in the tree.
    ///
    /// # Returns
    /// * `KeyValuePairs` - A boxed iterator over all key-value pairs
    fn iter_all(&self) -> KeyValuePairs;

    // ---- tfstor-extension: BEGIN ----
    // respd needs forward iteration from an arbitrary key (SCAN cursor) and
    // backward iteration from an arbitrary key (RSCAN). Upstream's iter_all
    // is a strict subset (iter_kv(None) == iter_all()).

    /// Iterates forward over key-value pairs starting strictly after `start_after`.
    /// If `start_after` is None, behaves like `iter_all()`.
    fn iter_kv(&self, start_after: Option<Vec<u8>>) -> KeyValuePairs;

    /// Iterates backward over key-value pairs starting strictly before `start_key`.
    /// If `start_key` is None, starts from the end of the tree.
    fn iter_kv_backward(&self, start_key: Option<Vec<u8>>) -> KeyValuePairs;
    // ---- tfstor-extension: END ----

    /// Filters and iterates over a range of keys with optional filtering parameters.
    ///
    /// # Arguments
    /// * `start_after` - Optional string to start iteration after
    /// * `prefix` - Optional prefix to filter keys
    /// * `continuation_token` - Optional token for pagination
    ///
    /// # Returns
    /// * A boxed iterator yielding key-value pairs as (String, Object) tuples
    fn range_filter<'a>(
        &'a self,
        start_after: Option<String>,
        prefix: Option<String>,
        continuation_token: Option<String>,
    ) -> Box<dyn Iterator<Item = (String, Object)> + 'a>;
}

/// `Store` represents a storage backend for metadata trees.
///
/// This trait defines operations for managing multiple metadata trees,
/// including creating, opening, and deleting trees, as well as transaction support.
pub trait Store: Send + Sync + Debug + 'static {
    /// Opens a tree with the given name, creating it if it doesn't exist.
    ///
    /// # Arguments
    /// * `name` - The name of the tree to open
    ///
    /// # Returns
    /// * `Result<Box<dyn BaseMetaTree>, MetaError>` - A boxed trait object implementing BaseMetaTree or an error
    fn tree_open(&self, name: &str) -> Result<Arc<dyn BaseMetaTree>, MetaError>;

    /// Opens a tree with extended functionality.
    ///
    /// # Arguments
    /// * `name` - The name of the tree to open
    ///
    /// # Returns
    /// * `Result<Box<dyn MetaTreeExt + Send + Sync>, MetaError>` - A boxed trait object implementing MetaTreeExt or an error
    fn tree_ext_open(&self, name: &str) -> Result<Arc<dyn MetaTreeExt + Send + Sync>, MetaError>;

    /// Checks if a tree with the given name exists.
    ///
    /// # Arguments
    /// * `name` - The name of the tree to check
    ///
    /// # Returns
    /// * `Result<bool, MetaError>` - True if the tree exists, false otherwise, or an error
    fn tree_exists(&self, name: &str) -> Result<bool, MetaError>;

    /// Lists the name of every tree in the store, reserved `_`-prefixed
    /// trees included, in no particular order.
    ///
    /// This is what makes the closed-holder-set rule of ADR 0005
    /// enforceable: fsck enumerates reference holders from the store's own
    /// trees rather than from the `_BUCKETS` rows, because a half-deleted
    /// bucket's object tree still holds refcounts after its row is gone.
    ///
    /// # Returns
    /// * `Result<Vec<String>, MetaError>` - Every tree name, or an error
    fn list_trees(&self) -> Result<Vec<String>, MetaError>;

    /// Deletes the tree with the given name.
    ///
    /// # Arguments
    /// * `name` - The name of the tree to delete
    ///
    /// # Returns
    /// * `Result<(), MetaError>` - Success or an error if the deletion fails
    fn tree_delete(&self, name: &str) -> Result<(), MetaError>;

    /// Begins a new transaction.
    ///
    /// # Returns
    /// * `Transaction` - A new transaction object
    fn begin_transaction(&self) -> Transaction;

    /// Returns the number of keys in the specified tree.
    ///
    /// # Arguments
    /// * `tree_name` - The name of the tree to count keys in
    ///
    /// # Returns
    /// * `Result<usize, MetaError>` - The number of keys or an error
    fn num_keys(&self, tree_name: &str) -> Result<usize, MetaError>;

    /// Returns the total disk space used by the storage.
    ///
    /// # Returns
    /// * `u64` - The disk space usage in bytes
    fn disk_space(&self) -> u64;
}

/// `Durability` defines the durability guarantees for storage operations.
///
/// Two levels, and only two (ADR 0010): `fsync`, where everything an ack
/// covers is on stable storage before the ack goes out, and `buffer`, where
/// nothing is flushed and the page cache decides.
///
/// The `try_from = "String"` deserialization routes the config file through
/// the same [`FromStr`] the CLI flag uses, so `durability = "fsync"` in
/// `qss_storage.toml` and `--durability fsync` cannot drift apart, and a
/// typo is reported with the same message in both places.
///
/// # `fdatasync` was a level and is not one any more
///
/// ADR 0010 removed it with no compatibility alias. After the sync boundary
/// moved from the block to the ack, the two syscalls' cost difference is
/// paid once per request instead of twice per MiB, and on the append-only
/// journal even fdatasync must flush the size metadata -- the two levels
/// became indistinguishable in both speed and crash safety, and a knob
/// implying a dead tradeoff misleads whoever picks it. The parser refusing
/// the name, with the two valid ones in the message, IS the migration.
///
/// The fdatasync SYSCALL is untouched: it is the internal primitive the
/// batch uses for block files, where the per-batch directory fsync carries
/// the rename durability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(try_from = "String")]
pub enum Durability {
    /// Data is buffered in memory and will be written to disk later.
    /// This provides the highest performance but lowest durability.
    Buffer,

    /// Everything a request acknowledges is flushed to stable storage before
    /// the acknowledgement: block files (fdatasync plus the directory fsync
    /// that makes their renames durable) and then the metadata journal
    /// (fjall `SyncAll`), once per batch.
    Fsync,
}

impl FromStr for Durability {
    type Err = String;

    /// Converts a string to a `Durability` enum value.
    ///
    /// # Arguments
    /// * `s` - The string to parse
    ///
    /// # Returns
    /// * `Result<Self, Self::Err>` - The parsed durability level or an error
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "buffer" => Ok(Durability::Buffer),
            "fsync" => Ok(Durability::Fsync),
            "fdatasync" => Err(
                "the fdatasync durability level was removed (ADR 0010): the sync boundary \
                 is the request ack now, which makes it indistinguishable from fsync in \
                 both speed and crash safety -- use fsync or buffer"
                    .to_string(),
            ),
            _ => Err(format!(
                "unknown durability option: {s} (expected buffer or fsync)"
            )),
        }
    }
}

impl TryFrom<String> for Durability {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl std::fmt::Display for Durability {
    /// Writes the spelling [`FromStr`] accepts, so a value read from a config
    /// file round-trips through a log line unchanged.
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let name = match self {
            Durability::Buffer => "buffer",
            Durability::Fsync => "fsync",
        };
        f.write_str(name)
    }
}
