use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use crate::hasher::Hasher;
use crate::metastore::{
    BlockTree, Durability, FjallStore, HeaderSpec, MULTIPART_PARTS_TREE, MetaError, MetaStore,
    MetaTreeExt, StoreHeader, UPLOADS_TREE,
};

use super::block_disk::{AtomicBlockWriter, BLOCKS_DB_DIR_NAME, BlockDiskOps, RealDiskOps};
use super::group_commit::{CommitStation, GroupCommit, GroupCommitStats};
use super::placement::BlockPlacement;
use super::stripes::{DEFAULT_STRIPE_COUNT, Stripes};
use super::write_path::DEFAULT_MAX_BLOCKS_PER_COMMIT;
use super::{StorageEngine, multipart::MultiPartTree};

/// SharedBlockStore manages everything block-scoped that must be one per
/// store, not one per namespace: the shared block metadata (the _BLOCKS,
/// _MULTIPART_PARTS and _UPLOADS trees), the blocks file root on disk, the
/// per-block stripe set, and the depth placement state.
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
    /// The in-flight upload records; the ext handle because they are scanned.
    uploads_tree: Arc<dyn MetaTreeExt + Send + Sync>,
    header: StoreHeader,
    hasher: Hasher,
    /// Root directory of the block data files.
    blocks_root: PathBuf,
    /// Fanout-depth placement for new block files.
    placement: BlockPlacement,
    /// Per-block-hash lock stripes; every `_BLOCKS` mutation runs under one.
    stripes: Stripes,
    /// Most block records one transaction carries (ADR 0010).
    max_blocks_per_commit: usize,
    /// How this process runs cross-request group commit (ADR 0011), or
    /// `None` for the ADR 0010 write path.
    group_commit: Option<GroupCommit>,
    /// The station itself, built on the first flush that wants one.
    ///
    /// Lazy because a station needs a tokio runtime to live in, and a store
    /// is also opened by tools that have none (fsck, the inspect
    /// subcommands). Building it eagerly in [`SharedBlockStore::new`] would
    /// make `group_commit = true` in a shared config file crash every offline
    /// tool in the deployment.
    station: OnceLock<CommitStation>,
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
    /// * `path` - Path to the shared block metadata DB (e.g., /meta_root/blocks/.db)
    /// * `blocks_root` - Root directory for the block data files, shared by
    ///   every namespace of this store
    /// * `storage_engine` - Storage engine
    /// * `inlined_metadata_size` - Maximum size for inlined metadata
    /// * `durability` - Durability level for transactions
    /// * `spec` - Hash written into the header of a *new* store; ignored when
    ///   an existing store is opened. `None` takes [`HeaderSpec::default`].
    /// * `stripe_count` - Number of block lock stripes; `None` takes
    ///   [`DEFAULT_STRIPE_COUNT`]. Sizing rule in `cas::stripes`.
    /// * `max_blocks_per_commit` - Most block records one transaction carries
    ///   (ADR 0010); `None` takes [`DEFAULT_MAX_BLOCKS_PER_COMMIT`]. Like the
    ///   stripe count, a property of this process and not of the store.
    /// * `group_commit` - `Some` to merge the closing step of concurrent
    ///   requests through a commit station (ADR 0011), `None` for the ADR
    ///   0010 write path unchanged. Default off; the station is bounded by
    ///   the same `max_blocks_per_commit`.
    ///
    /// # Errors
    ///
    /// [`MetaError::StoreLocked`] if another process holds the blocks DB, and
    /// [`MetaError::Header`] if its header is missing or unacceptable.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        mut path: PathBuf,
        mut blocks_root: PathBuf,
        storage_engine: StorageEngine,
        inlined_metadata_size: Option<usize>,
        durability: Option<Durability>,
        spec: Option<HeaderSpec>,
        stripe_count: Option<usize>,
        max_blocks_per_commit: Option<usize>,
        group_commit: Option<GroupCommit>,
    ) -> Result<Self, MetaError> {
        // A store from before the .db rename has its database at
        // <blocks>/db, a name that doubles as the 0xdb fanout slot.
        // Opening would mint a fresh empty database at .db and silently
        // shadow every record of the old store, so it is refused with the
        // migration spelled out. The `version` file is fjall's own and
        // never a block name, so it tells a legacy database apart from a
        // legitimate 0xdb fanout directory.
        let legacy = path.join("db");
        if legacy.join("version").is_file() && !path.join(BLOCKS_DB_DIR_NAME).exists() {
            return Err(MetaError::OtherDBError(format!(
                "legacy blocks database at {}: this build keeps it at {} -- \
                 stop every daemon, move fjall's own files (everything NOT \
                 named as full-hex block or two-hex directory) there, and \
                 leave any hex-named entries where they are",
                legacy.display(),
                path.join(BLOCKS_DB_DIR_NAME).display(),
            )));
        }

        path.push(BLOCKS_DB_DIR_NAME);

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
        let multipart_tree_base = meta_store.get_tree_ext(MULTIPART_PARTS_TREE)?;
        let multipart_tree = MultiPartTree::new(multipart_tree_base);
        let uploads_tree = meta_store.get_tree_ext(UPLOADS_TREE)?;

        Ok(Self {
            meta_store: Arc::new(meta_store),
            block_tree: Arc::new(block_tree),
            multipart_tree: Arc::new(multipart_tree),
            uploads_tree,
            header,
            hasher: header.hasher(),
            placement: BlockPlacement::new(blocks_root.clone()),
            blocks_root,
            stripes: Stripes::new(stripe_count.unwrap_or(DEFAULT_STRIPE_COUNT)),
            // Clamped rather than refused: the config and CLI layers already
            // reject zero with a message naming the setting, and a store
            // built in code should not be able to wedge the write path with
            // a batch that can never close.
            max_blocks_per_commit: max_blocks_per_commit
                .unwrap_or(DEFAULT_MAX_BLOCKS_PER_COMMIT)
                .max(1),
            group_commit,
            station: OnceLock::new(),
            disk_writer,
            disk_ops,
        })
    }

    /// Most block records this process puts in one transaction (ADR 0010).
    pub(super) fn max_blocks_per_commit(&self) -> usize {
        self.max_blocks_per_commit
    }

    /// This store's commit station, started on first use, or `None` when no
    /// group commit was configured (ADR 0011).
    ///
    /// Called from the write path's flush, which is always inside a tokio
    /// runtime -- which is exactly why the station is built here and not in
    /// [`SharedBlockStore::new`], where an offline tool would have to spawn a
    /// committer task with no runtime to spawn it into.
    pub(super) fn commit_station(self: &Arc<Self>) -> Option<&CommitStation> {
        let options = self.group_commit?;
        Some(
            self.station
                .get_or_init(|| CommitStation::start(self, options, self.max_blocks_per_commit)),
        )
    }

    /// What this store's commit station has done, or `None` if it has none
    /// (either group commit is off, or nothing has flushed yet).
    ///
    /// `members / groups` is the mean group size, and `groups` is the number
    /// of write-path persists the blocks DB paid.
    pub fn group_commit_stats(&self) -> Option<GroupCommitStats> {
        self.station.get().map(CommitStation::stats)
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

    /// Get a reference to the shared upload records (`_UPLOADS`).
    ///
    /// The extended handle rather than the base one: both consumers of this
    /// tree scan it -- `ListMultipartUploads` and the stale-upload GC sweep
    /// (ADR 0003) -- and iteration lives on [`MetaTreeExt`]. Point reads
    /// reconstruct their key, so the scans decode record VALUES and never
    /// parse a key.
    pub fn uploads_tree(&self) -> Arc<dyn MetaTreeExt + Send + Sync> {
        Arc::clone(&self.uploads_tree)
    }

    /// Get a reference to the shared meta store
    /// This is used for creating transactions that write to shared metadata
    pub fn meta_store(&self) -> Arc<MetaStore> {
        Arc::clone(&self.meta_store)
    }
}
