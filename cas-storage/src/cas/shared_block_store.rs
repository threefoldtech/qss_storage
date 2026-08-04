use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use crate::hasher::Hasher;
use crate::metastore::{
    BlockTree, FjallStore, MULTIPART_PARTS_TREE, MetaError, MetaStore, MetaTreeExt, StoreHeader,
    StoreId, StorePairingMismatch, UPLOADS_TREE, store_header,
};
use crate::store_options::StoreOptions;

use super::block_disk::{
    AtomicBlockWriter, BLOCKS_DB_DIR_NAME, BlockDiskOps, RealDiskOps, StoreIdMarker,
};
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
    /// * `opts` - How this process opens the store: backend, durability,
    ///   inline threshold, the hash a *new* store's header gets, the stripe
    ///   count, the batch cap (ADR 0010) and the commit station (ADR 0011).
    ///   [`StoreOptions::default`] is "no opinion"; see the field docs there
    ///   for which knobs describe the store on disk and which describe only
    ///   this process. `verify_on_read` is read by [`CasFS`](super::CasFS)
    ///   and ignored here -- the block store serves bytes, it does not read
    ///   objects.
    ///
    /// # Errors
    ///
    /// [`MetaError::StoreLocked`] if another process holds the blocks DB, and
    /// [`MetaError::Header`] if its header is missing or unacceptable.
    pub fn new(
        mut path: PathBuf,
        mut blocks_root: PathBuf,
        opts: StoreOptions,
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

        // The directory the blocks DATABASE lives in, which is also where its
        // header sidecar belongs -- `<meta_root>/blocks/`, and not the blocks
        // ROOT, which a meta-root/data-root split (ADR 0012) puts somewhere
        // else entirely.
        let store_dir = path.clone();

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
        let disk_writer = AtomicBlockWriter::open(&*disk_ops, blocks_root.clone(), opts.durability)
            .map_err(|e| MetaError::OtherDBError(format!("opening the blocks root: {e}")))?;

        let inlined_metadata_size = opts.inline_metadata_size;
        let (meta_store, header) = match opts.metadata_db {
            StorageEngine::Fjall => MetaStore::open_or_create(
                path.clone(),
                inlined_metadata_size,
                opts.header_spec(),
                |p| FjallStore::new(p, inlined_metadata_size, Some(opts.durability)),
            )?,
        };

        // ADR 0012: the two halves of the store name each other, or this is
        // not a store, it is two halves of two.
        let header = pair_the_roots(
            &meta_store,
            &path,
            &store_dir,
            &blocks_root,
            header,
            &disk_writer,
            &*disk_ops,
        )?;

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
            stripes: Stripes::new(opts.stripe_count.unwrap_or(DEFAULT_STRIPE_COUNT)),
            // Clamped rather than refused: the config and CLI layers already
            // reject zero with a message naming the setting, and a store
            // built in code should not be able to wedge the write path with
            // a batch that can never close.
            max_blocks_per_commit: opts
                .max_blocks_per_commit
                .unwrap_or(DEFAULT_MAX_BLOCKS_PER_COMMIT)
                .max(1),
            group_commit: opts.group_commit,
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

    /// This store's pairing identity (ADR 0012). Always present after a
    /// successful open: a store without one is adopted on the way through.
    pub fn store_id(&self) -> Option<StoreId> {
        self.header.store_id()
    }
}

/// Decides what the blocks database and the blocks root have to say to each
/// other, and makes it so (ADR 0012).
///
/// Six states, one refusal:
///
/// | header | marker | what happens |
/// |--------|--------|--------------|
/// | id     | same   | nothing: this is a paired store |
/// | id     | other  | REFUSED: two halves of two stores |
/// | id     | none   | the marker is written (a crash landed between the two adoption writes) |
/// | id     | junk   | the marker is rewritten: damage names no store |
/// | none   | none   | adoption: an id is minted, header first, then marker |
/// | none   | id     | the root's id is taken into the header: the marker outlived the header write |
///
/// The write order is header first, always. It is the ordering that makes
/// the half-adopted state repairable without judgement: a header with an id
/// and a root without one has exactly one completion, and it is the one this
/// function performs on the next open.
///
/// The last row is the one the ADR's "header is authoritative" does not
/// cover, because a header with no id is not authoritative about anything.
/// Taking the marker's id is the choice that destroys nothing: minting a new
/// one would overwrite the identity of whatever store that blocks root
/// really belongs to, and that store's own database would then be the one
/// refused.
fn pair_the_roots(
    meta_store: &MetaStore,
    db_path: &Path,
    store_dir: &Path,
    blocks_root: &Path,
    header: StoreHeader,
    writer: &AtomicBlockWriter,
    ops: &dyn BlockDiskOps,
) -> Result<StoreHeader, MetaError> {
    let write_marker = |id: StoreId| -> Result<(), MetaError> {
        writer.write_store_id_marker(ops, id).map_err(|e| {
            MetaError::OtherDBError(format!(
                "writing the store id marker in {}: {e}",
                blocks_root.display()
            ))
        })
    };
    let adopt = |id: StoreId| -> Result<StoreHeader, MetaError> {
        store_header::adopt_store_id(&*meta_store.get_underlying_store(), store_dir, header, id)
    };

    match (header.store_id(), writer.store_id_marker()) {
        (Some(header_id), StoreIdMarker::Present(marker_id)) if header_id == *marker_id => {
            Ok(header)
        }
        (Some(header_id), StoreIdMarker::Present(marker_id)) => {
            Err(MetaError::StorePairing(Box::new(StorePairingMismatch {
                db_path: db_path.display().to_string(),
                header_id,
                blocks_root: blocks_root.display().to_string(),
                marker_id: *marker_id,
            })))
        }
        (Some(header_id), StoreIdMarker::Absent) => {
            write_marker(header_id)?;
            tracing::info!(
                store_id = %header_id,
                blocks_root = %blocks_root.display(),
                "adopted the blocks root into store {header_id}: it carried no store id marker"
            );
            Ok(header)
        }
        (Some(header_id), StoreIdMarker::Unreadable(found)) => {
            write_marker(header_id)?;
            tracing::warn!(
                store_id = %header_id,
                blocks_root = %blocks_root.display(),
                found = %found,
                "the blocks root's store id marker was not an id; rewrote it from the header"
            );
            Ok(header)
        }
        (None, StoreIdMarker::Present(marker_id)) => {
            let marker_id = *marker_id;
            let adopted = adopt(marker_id)?;
            tracing::info!(
                store_id = %marker_id,
                db = %db_path.display(),
                "the blocks database had no store id; took the one its blocks root carries"
            );
            Ok(adopted)
        }
        (None, marker) => {
            // A store from before ADR 0012 (or one whose marker is damage).
            // Header first, then marker: the order the repair rule assumes.
            let minted = StoreId::generate();
            let adopted = adopt(minted)?;
            write_marker(minted)?;
            tracing::info!(
                store_id = %minted,
                db = %db_path.display(),
                blocks_root = %blocks_root.display(),
                "adopted a store that predates ADR 0012: minted store id {minted}{}",
                match marker {
                    StoreIdMarker::Unreadable(found) => format!(" over an unreadable marker ({found})"),
                    _ => String::new(),
                }
            );
            Ok(adopted)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::block_disk::STORE_ID_MARKER_NAME;
    use crate::cas::crash_fixtures::{
        plant_foreign_store_id_marker, plant_unreadable_store_id_marker, remove_store_id_marker,
        strip_store_id,
    };
    use crate::metastore::Durability;
    use tempfile::{TempDir, tempdir};

    /// Opens (creating the first time) a store whose two roots are wherever
    /// the caller put them -- the ADR 0012 shape, where "wherever" is two
    /// different disks.
    fn open(meta_root: &Path, fs_root: &Path) -> Result<SharedBlockStore, MetaError> {
        SharedBlockStore::new(
            meta_root.join("blocks"),
            fs_root.join("blocks"),
            StoreOptions {
                inline_metadata_size: Some(1),
                durability: Durability::Buffer,
                ..StoreOptions::default()
            },
        )
    }

    /// The blocks database path the store builds under a meta root.
    fn blocks_db(meta_root: &Path) -> PathBuf {
        meta_root.join("blocks").join(BLOCKS_DB_DIR_NAME)
    }

    /// What the blocks root's marker says, read straight off the disk.
    fn marker_of(fs_root: &Path) -> Option<StoreId> {
        let raw =
            std::fs::read_to_string(fs_root.join("blocks").join(STORE_ID_MARKER_NAME)).ok()?;
        StoreId::parse_hex(&raw)
    }

    /// The error of an open that had to fail. (`expect_err` wants a `Debug`
    /// store, and a block store is not a thing to print.)
    fn refusal(result: Result<SharedBlockStore, MetaError>, why: &str) -> MetaError {
        match result {
            Ok(_) => panic!("{why}"),
            Err(e) => e,
        }
    }

    /// A created store, closed again, and the id it minted.
    fn make_store(meta_root: &Path, fs_root: &Path) -> StoreId {
        let store = open(meta_root, fs_root).expect("a fresh store must open");
        store.store_id().expect("a created store has an id")
    }

    /// Creation mints the identity and writes both halves of it -- with the
    /// two roots as far apart as the ADR's deployment puts them.
    #[test]
    fn a_new_store_pairs_its_two_roots() {
        let meta = tempdir().unwrap();
        let fs = tempdir().unwrap();

        let id = make_store(meta.path(), fs.path());

        assert_eq!(
            marker_of(fs.path()),
            Some(id),
            "the marker copies the header"
        );
        assert!(
            blocks_db(meta.path()).is_dir(),
            "the database is under the meta root, not the data root"
        );
        assert!(
            !fs.path().join("blocks").join(BLOCKS_DB_DIR_NAME).exists(),
            "nothing of the database lives on the data root"
        );

        // And the pairing is stable: reopening changes neither half.
        let reopened = open(meta.path(), fs.path()).unwrap();
        assert_eq!(reopened.store_id(), Some(id));
        assert_eq!(marker_of(fs.path()), Some(id));
    }

    /// A store from before ADR 0012 -- no id anywhere -- is ADOPTED at first
    /// open, not refused. The asymmetry with the blocks/.db migration
    /// refusal is deliberate: adoption shadows nothing.
    #[test]
    fn a_legacy_store_is_adopted_at_first_open() {
        let dir = tempdir().unwrap();
        make_store(dir.path(), dir.path());

        // Rewind to the pre-0012 shape: no id in the header, no marker.
        strip_store_id(&blocks_db(dir.path()));
        remove_store_id_marker(&dir.path().join("blocks"));

        let id = {
            let adopted = open(dir.path(), dir.path()).expect("an old store still opens");
            let id = adopted.store_id().expect("adoption mints an id");
            assert_eq!(marker_of(dir.path()), Some(id), "both halves were written");
            id
        };

        // Adoption happens once: the second open finds a paired store.
        let reopened = open(dir.path(), dir.path()).unwrap();
        assert_eq!(reopened.store_id(), Some(id));
    }

    /// The crash between the two adoption writes: the header has the id, the
    /// marker never landed. The header is authoritative, so the next open
    /// completes the adoption by writing the marker -- same id, no new one.
    #[test]
    fn a_half_adopted_store_completes_at_the_next_open() {
        let meta = tempdir().unwrap();
        let fs = tempdir().unwrap();
        let id = make_store(meta.path(), fs.path());

        remove_store_id_marker(&fs.path().join("blocks"));
        assert_eq!(marker_of(fs.path()), None, "the crash fixture: no marker");

        let completed = open(meta.path(), fs.path()).expect("half adoption is not a refusal");
        assert_eq!(completed.store_id(), Some(id), "no id was re-minted");
        assert_eq!(marker_of(fs.path()), Some(id));
    }

    /// A marker that is not an id claims nothing, so there is nothing to
    /// compare it against: the header wins and it is rewritten.
    #[test]
    fn an_unreadable_marker_is_rewritten_from_the_header() {
        let meta = tempdir().unwrap();
        let fs = tempdir().unwrap();
        let id = make_store(meta.path(), fs.path());

        plant_unreadable_store_id_marker(&fs.path().join("blocks"), b"half a line, no ne");

        let healed = open(meta.path(), fs.path()).expect("damage is not a mispairing");
        assert_eq!(healed.store_id(), Some(id));
        assert_eq!(marker_of(fs.path()), Some(id));
    }

    /// The other half-adopted direction: the marker survived and the header
    /// write did not. The root's claim is taken into the header rather than
    /// overwritten -- minting a new id here would erase the identity of
    /// whatever store that root really belongs to.
    #[test]
    fn a_header_without_an_id_takes_the_markers() {
        let meta = tempdir().unwrap();
        let fs = tempdir().unwrap();
        let id = make_store(meta.path(), fs.path());

        strip_store_id(&blocks_db(meta.path()));

        let healed = open(meta.path(), fs.path()).expect("a lost header write is repairable");
        assert_eq!(healed.store_id(), Some(id), "the marker's id was adopted");
        assert_eq!(
            marker_of(fs.path()),
            Some(id),
            "the marker was not rewritten"
        );
    }

    /// Two stores, so their halves can be crossed.
    struct TwoStores {
        meta_a: TempDir,
        fs_a: TempDir,
        meta_b: TempDir,
        fs_b: TempDir,
        id_a: StoreId,
        id_b: StoreId,
    }

    fn two_stores() -> TwoStores {
        let (meta_a, fs_a, meta_b, fs_b) = (
            tempdir().unwrap(),
            tempdir().unwrap(),
            tempdir().unwrap(),
            tempdir().unwrap(),
        );
        let id_a = make_store(meta_a.path(), fs_a.path());
        let id_b = make_store(meta_b.path(), fs_b.path());
        assert_ne!(id_a, id_b);
        TwoStores {
            meta_a,
            fs_a,
            meta_b,
            fs_b,
            id_a,
            id_b,
        }
    }

    /// The refusal, in both directions: the fast disk's database with the
    /// wrong data directory, and the right data directory with the wrong
    /// database. Both name both stores and both paths, because nothing in
    /// the process knows which of the two paths the operator got wrong.
    #[test]
    fn a_mispaired_store_is_refused_in_both_directions() {
        let s = two_stores();

        for (meta, fs, header_id, marker_id) in [
            (s.meta_a.path(), s.fs_b.path(), s.id_a, s.id_b),
            (s.meta_b.path(), s.fs_a.path(), s.id_b, s.id_a),
        ] {
            let err = refusal(open(meta, fs), "a mispaired store must be refused");
            assert!(
                matches!(err, MetaError::StorePairing { .. }),
                "the refusal has its own class: {err:?}"
            );
            let msg = err.to_string();
            assert!(msg.contains(&header_id.to_hex()), "{msg}");
            assert!(msg.contains(&marker_id.to_hex()), "{msg}");
            let db = blocks_db(meta).canonicalize().unwrap();
            let root = fs.join("blocks").canonicalize().unwrap();
            assert!(msg.contains(&db.display().to_string()), "{msg}");
            assert!(msg.contains(&root.display().to_string()), "{msg}");
            assert!(
                msg.contains("--re-pair"),
                "the way out is in the message: {msg}"
            );
        }

        // Nothing was written by the refusals: both stores still pair.
        assert_eq!(marker_of(s.fs_a.path()), Some(s.id_a));
        assert_eq!(marker_of(s.fs_b.path()), Some(s.id_b));
        assert_eq!(
            open(s.meta_a.path(), s.fs_a.path()).unwrap().store_id(),
            Some(s.id_a)
        );
        assert_eq!(
            open(s.meta_b.path(), s.fs_b.path()).unwrap().store_id(),
            Some(s.id_b)
        );
    }

    /// The failed-mount case the ADR names: a database pointed at a blocks
    /// root some other store already claimed. Refused even though the root
    /// holds no blocks at all -- the claim is what is compared, not the
    /// contents.
    #[test]
    fn an_empty_but_claimed_blocks_root_is_refused() {
        let meta = tempdir().unwrap();
        let fs = tempdir().unwrap();
        let id = make_store(meta.path(), fs.path());

        let elsewhere = tempdir().unwrap();
        let stranger =
            plant_foreign_store_id_marker(&elsewhere.path().join("blocks"), StoreId::generate());

        let err = refusal(
            open(meta.path(), elsewhere.path()),
            "a claimed root is not ours to open",
        );
        let msg = err.to_string();
        assert!(msg.contains(&id.to_hex()), "{msg}");
        assert!(msg.contains(&stranger.to_hex()), "{msg}");
    }
}
