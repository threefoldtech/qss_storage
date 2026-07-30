use std::path::PathBuf;
use std::sync::Arc;

use crate::metastore::{
    BaseMetaTree, BlockTree, Durability, FjallStore, FjallStoreNotx, MetaError, MetaStore,
};

use super::{StorageEngine, multipart::MultiPartTree};

/// SharedBlockStore manages the shared block metadata (_BLOCKS, _PATHS, and _MULTIPART_PARTS trees)
/// that is accessed by all users for block refcounting, path allocation, and multipart uploads.
///
/// This is created once at startup and shared across all CasFS instances.
pub struct SharedBlockStore {
    meta_store: Arc<MetaStore>,
    block_tree: Arc<BlockTree>,
    path_tree: Arc<dyn BaseMetaTree>,
    multipart_tree: Arc<MultiPartTree>,
}

impl SharedBlockStore {
    /// Create a new SharedBlockStore
    ///
    /// # Arguments
    /// * `path` - Path to the shared block metadata DB (e.g., /meta_root/blocks/db)
    /// * `storage_engine` - Storage engine (Fjall or FjallNotx)
    /// * `inlined_metadata_size` - Maximum size for inlined metadata
    /// * `durability` - Durability level for transactions
    pub fn new(
        mut path: PathBuf,
        storage_engine: StorageEngine,
        inlined_metadata_size: Option<usize>,
        durability: Option<Durability>,
    ) -> Result<Self, MetaError> {
        path.push("db");

        // Canonicalize path to eliminate getcwd() syscalls in async operations
        // This is critical for performance as it avoids repeated getcwd() on every file op
        std::fs::create_dir_all(&path).ok();
        path = path.canonicalize().unwrap_or(path);

        let meta_store = match storage_engine {
            StorageEngine::Fjall => {
                let store = FjallStore::new(path, inlined_metadata_size, durability);
                MetaStore::new(store, inlined_metadata_size)
            }
            StorageEngine::FjallNotx => {
                tracing::warn!(
                    "Using fjall_notx for shared block metadata in multi-user mode. \
                     Note: fjall_notx has weaker consistency guarantees: \
                     (1) transactions are not atomic - blocks visible before commit, \
                     (2) rollback uses best-effort cleanup, not true rollback, \
                     (3) durability parameter is ignored. \
                     For production multi-user deployments, consider using 'fjall' instead."
                );
                let store = FjallStoreNotx::new(path, inlined_metadata_size);
                MetaStore::new(store, inlined_metadata_size)
            }
        };

        let block_tree = meta_store.get_block_tree()?;
        let path_tree = meta_store.get_path_tree()?;
        let multipart_tree_base = meta_store.get_tree("_MULTIPART_PARTS")?;
        let multipart_tree = MultiPartTree::new(multipart_tree_base);

        Ok(Self {
            meta_store: Arc::new(meta_store),
            block_tree: Arc::new(block_tree),
            path_tree,
            multipart_tree: Arc::new(multipart_tree),
        })
    }

    /// Get a reference to the shared block tree
    pub fn block_tree(&self) -> Arc<BlockTree> {
        Arc::clone(&self.block_tree)
    }

    /// Get a reference to the shared path tree
    pub fn path_tree(&self) -> Arc<dyn BaseMetaTree> {
        Arc::clone(&self.path_tree)
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
