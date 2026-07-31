use std::path::PathBuf;
use std::sync::Arc;

use crate::hasher::Hasher;
use crate::metastore::{
    BlockTree, Durability, FjallStore, HeaderSpec, MetaError, MetaStore, StoreHeader,
};

use super::{StorageEngine, multipart::MultiPartTree};

/// SharedBlockStore manages the shared block metadata (the _BLOCKS and
/// _MULTIPART_PARTS trees) that is accessed by all users for block
/// refcounting and multipart uploads.
///
/// This is created once at startup and shared across all CasFS instances.
pub struct SharedBlockStore {
    meta_store: Arc<MetaStore>,
    block_tree: Arc<BlockTree>,
    multipart_tree: Arc<MultiPartTree>,
    header: StoreHeader,
    hasher: Hasher,
}

impl SharedBlockStore {
    /// Create a new SharedBlockStore, or open an existing one.
    ///
    /// The block DB is the store whose header decides how blocks are
    /// addressed, so this is where the [`Hasher`] comes from. A store whose
    /// header names a hash this build does not have is refused here rather
    /// than mis-addressed later.
    ///
    /// # Arguments
    /// * `path` - Path to the shared block metadata DB (e.g., /meta_root/blocks/db)
    /// * `storage_engine` - Storage engine
    /// * `inlined_metadata_size` - Maximum size for inlined metadata
    /// * `durability` - Durability level for transactions
    /// * `spec` - Hash written into the header of a *new* store; ignored when
    ///   an existing store is opened. `None` takes [`HeaderSpec::default`].
    pub fn new(
        mut path: PathBuf,
        storage_engine: StorageEngine,
        inlined_metadata_size: Option<usize>,
        durability: Option<Durability>,
        spec: Option<HeaderSpec>,
    ) -> Result<Self, MetaError> {
        path.push("db");

        // Canonicalize path to eliminate getcwd() syscalls in async operations
        // This is critical for performance as it avoids repeated getcwd() on every file op
        std::fs::create_dir_all(&path).ok();
        path = path.canonicalize().unwrap_or(path);

        let spec = spec.unwrap_or_default();
        let (meta_store, header) = match storage_engine {
            StorageEngine::Fjall => {
                MetaStore::open_or_create(path, inlined_metadata_size, spec, |p| {
                    FjallStore::new(p, inlined_metadata_size, durability)
                })?
            }
        };

        let block_tree = meta_store.get_block_tree()?;
        let multipart_tree_base = meta_store.get_tree("_MULTIPART_PARTS")?;
        let multipart_tree = MultiPartTree::new(multipart_tree_base);

        Ok(Self {
            meta_store: Arc::new(meta_store),
            block_tree: Arc::new(block_tree),
            multipart_tree: Arc::new(multipart_tree),
            header,
            hasher: header.hasher(),
        })
    }

    /// The hash function this store's blocks are addressed by, as recorded in
    /// its header at creation.
    pub fn hasher(&self) -> Hasher {
        self.hasher
    }

    /// The store header, for tools that report on it.
    pub fn header(&self) -> StoreHeader {
        self.header
    }

    /// Get a reference to the shared block tree
    pub fn block_tree(&self) -> Arc<BlockTree> {
        Arc::clone(&self.block_tree)
    }

    /// Get a reference to the shared multipart tree
    pub fn multipart_tree(&self) -> Arc<MultiPartTree> {
        Arc::clone(&self.multipart_tree)
    }

    /// Get a reference to the shared meta store
    /// This is used for creating transactions that write to shared metadata
    pub fn meta_store(&self) -> Arc<MetaStore> {
        Arc::clone(&self.meta_store)
    }
}
