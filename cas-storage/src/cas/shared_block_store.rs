use std::path::PathBuf;
use std::sync::Arc;

use crate::hasher::Hasher;
use crate::metastore::{
    BlockTree, Durability, FjallStore, HeaderSpec, MULTIPART_PARTS_TREE, MetaError, MetaStore,
    StoreHeader,
};

use super::block_disk::{AtomicBlockWriter, BlockDiskOps, RealDiskOps};
use super::placement::BlockPlacement;
use super::stripes::{DEFAULT_STRIPE_COUNT, Stripes};
use super::{StorageEngine, multipart::MultiPartTree};

/// SharedBlockStore manages everything block-scoped that must be one per
/// store, not one per namespace: the shared block metadata (the _BLOCKS and
/// _MULTIPART_PARTS trees), the blocks file root on disk, the per-block
/// stripe set, and the depth placement state.
///
/// This is created once at startup and shared across all CasFS instances.
/// The blocks root living HERE is load-bearing (ADR 0006): block records are
/// shared across namespaces, so if two namespaces derived file paths from
/// different roots, a dedup hit in one would point at a file only the other
/// can see. One store, one root.
pub struct SharedBlockStore {
    meta_store: Arc<MetaStore>,
    block_tree: Arc<BlockTree>,
    multipart_tree: Arc<MultiPartTree>,
    header: StoreHeader,
    hasher: Hasher,
    /// Root directory of the block data files.
    blocks_root: PathBuf,
    /// Fanout-depth placement for new block files.
    placement: BlockPlacement,
    /// Per-block-hash lock stripes; every `_BLOCKS` mutation runs under one.
    stripes: Stripes,
    /// The atomic temp+fsync+rename writer (open duties already run).
    disk_writer: AtomicBlockWriter,
    /// The low-level disk ops the writer drives; swapped by tests.
    disk_ops: Arc<dyn BlockDiskOps>,
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
    /// * `blocks_root` - Root directory for the block data files, shared by
    ///   every namespace of this store
    /// * `storage_engine` - Storage engine
    /// * `inlined_metadata_size` - Maximum size for inlined metadata
    /// * `durability` - Durability level for transactions
    /// * `spec` - Hash written into the header of a *new* store; ignored when
    ///   an existing store is opened. `None` takes [`HeaderSpec::default`].
    /// * `stripe_count` - Number of block lock stripes; `None` takes
    ///   [`DEFAULT_STRIPE_COUNT`]. Sizing rule in `cas::stripes`.
    pub fn new(
        mut path: PathBuf,
        mut blocks_root: PathBuf,
        storage_engine: StorageEngine,
        inlined_metadata_size: Option<usize>,
        durability: Option<Durability>,
        spec: Option<HeaderSpec>,
        stripe_count: Option<usize>,
    ) -> Result<Self, MetaError> {
        path.push("db");

        // Canonicalize path to eliminate getcwd() syscalls in async operations
        // This is critical for performance as it avoids repeated getcwd() on every file op
        std::fs::create_dir_all(&path).ok();
        path = path.canonicalize().unwrap_or(path);

        std::fs::create_dir_all(&blocks_root).ok();
        blocks_root = blocks_root.canonicalize().unwrap_or(blocks_root);

        // Store-open duties for the block file tree: create blocks/ and
        // blocks/.tmp, purge temp residue, refuse a temp dir on another
        // filesystem, fsync per durability (ADR 0006 component 4).
        let disk_ops: Arc<dyn BlockDiskOps> = Arc::new(RealDiskOps);
        let disk_writer = AtomicBlockWriter::open(
            &*disk_ops,
            blocks_root.clone(),
            durability.unwrap_or(Durability::Fsync),
        )
        .map_err(|e| MetaError::OtherDBError(format!("opening the blocks root: {e}")))?;

        let spec = spec.unwrap_or_default();
        let (meta_store, header) = match storage_engine {
            StorageEngine::Fjall => {
                MetaStore::open_or_create(path, inlined_metadata_size, spec, |p| {
                    FjallStore::new(p, inlined_metadata_size, durability)
                })?
            }
        };

        let block_tree = meta_store.get_block_tree()?;
        let multipart_tree_base = meta_store.get_tree(MULTIPART_PARTS_TREE)?;
        let multipart_tree = MultiPartTree::new(multipart_tree_base);

        Ok(Self {
            meta_store: Arc::new(meta_store),
            block_tree: Arc::new(block_tree),
            multipart_tree: Arc::new(multipart_tree),
            header,
            hasher: header.hasher(),
            placement: BlockPlacement::new(blocks_root.clone()),
            blocks_root,
            stripes: Stripes::new(stripe_count.unwrap_or(DEFAULT_STRIPE_COUNT)),
            disk_writer,
            disk_ops,
        })
    }

    /// Root directory of the block data files. One per store: every
    /// namespace derives block file paths from this root and no other.
    pub fn blocks_root(&self) -> &PathBuf {
        &self.blocks_root
    }

    /// The store's depth placement state for new block files.
    pub(super) fn placement(&self) -> &BlockPlacement {
        &self.placement
    }

    /// The store's per-block lock stripes.
    pub(super) fn stripes(&self) -> &Stripes {
        &self.stripes
    }

    /// The store's atomic block file writer.
    pub(super) fn disk_writer(&self) -> &AtomicBlockWriter {
        &self.disk_writer
    }

    /// The low-level disk ops handle (a seam: tests swap in a failer).
    pub(super) fn disk_ops(&self) -> Arc<dyn BlockDiskOps> {
        Arc::clone(&self.disk_ops)
    }

    /// Test-only: substitute the disk ops before the store is shared.
    #[cfg(test)]
    pub(super) fn set_disk_ops(&mut self, ops: Arc<dyn BlockDiskOps>) {
        self.disk_ops = ops;
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
