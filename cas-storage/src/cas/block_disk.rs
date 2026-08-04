//! The atomic block file writer (ADR 0006 component 4).
//!
//! Blocks reach their final path by write-temp + fsync + rename, never by
//! writing in place. The invariant this buys (hard rule 5): a block record
//! is committed only after its file is durable at its final path --
//! crash-true at `Fsync`, ordering-true at `Buffer`.
//!
//! Two layers live here:
//!
//! - [`BlockDiskOps`]: the low-level, mockable seam -- one method per
//!   syscall-shaped operation. Tests substitute a recorder/failer.
//! - [`AtomicBlockWriter`]: the protocol over those ops -- temp naming with
//!   a process-wide nonce, the durability-gated fsync plan, the
//!   known-durable directory cache, rename-over, unlink, and the
//!   store-open duties (create dirs, purge temp residue, same-device
//!   check).
//!
//! Everything here is synchronous on purpose, with one exception: the write
//! and delete paths call it from inside `spawn_blocking` closures that own
//! the stripe guards, so no executor thread ever parks on disk I/O. The
//! exception is [`AtomicBlockWriter::sync_batch`] (ADR 0010), which fans a
//! batch's file syncs out across the blocking pool and is therefore `async`
//! itself; the fan-out primitive is deliberately hidden behind that one
//! method, so replacing it (io_uring, say) touches no caller.
//!
//! # The batch (ADR 0010)
//!
//! One request is one durability unit, so the per-block sequence splits into
//! three pieces the write path drives in order:
//!
//! 1. [`stage_block`](AtomicBlockWriter::stage_block) -- per block, as its
//!    bytes arrive: make the fanout dir, write the temp file. No stripe is
//!    needed; a temp name is unique per attempt and no reader can see it.
//! 2. [`sync_batch`](AtomicBlockWriter::sync_batch) -- once per batch, still
//!    without stripes: fdatasync every staged file, concurrently.
//! 3. [`land_batch`](AtomicBlockWriter::land_batch) -- once per batch, under
//!    the batch's stripes: rename every staged file into place, then fsync
//!    the set of directories the renames touched, each exactly once.
//!
//! Only after step 3 may the records commit, which is ADR 0006's file-first
//! invariant unchanged -- the batch widens it from one block to N, it does
//! not reorder it.

mod store_id;

use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::metastore::{BlockId, Durability, StoreId, block_disk_path};

pub(crate) use store_id::{StoreIdMarker, read_store_id_marker, rewrite_store_id_marker};

/// Name of the temp directory under the blocks root. Crash residue in here
/// is garbage by definition and is purged once at store open -- never by a
/// runtime sweeper, which could race an in-flight write.
pub(crate) const TMP_DIR_NAME: &str = ".tmp";

/// Name of the quarantine directory under the blocks root: where fsck moves
/// corrupt block files and foreign files instead of deleting them (ADR
/// 0005). Nothing in the write or read path ever looks in here; it exists so
/// the scrub walkers know to skip it and so repair has one agreed
/// destination.
pub(crate) const QUARANTINE_DIR_NAME: &str = ".quarantine";

/// Name of the shared blocks database directory under the blocks root.
///
/// Dot-prefixed like the other reserved names so it can never be a fanout
/// directory, whose names are two hex characters. The old bare "db" was
/// also the 0xdb fanout slot: blocks whose hash began with 0xdb landed
/// inside the database directory, and the scrub had to skip that whole
/// subtree -- one block in 256 invisible to every fsck pass.
pub const BLOCKS_DB_DIR_NAME: &str = ".db";

/// Name of the pairing marker under the blocks root (ADR 0012): the store id
/// this tree belongs to, as lowercase hex.
///
/// Dot-prefixed like every other reserved name, so it can never be mistaken
/// for a fanout directory or a block. It is the fs half of the pairing
/// identity whose meta half lives in the store header, and it is written
/// once -- by adoption or at creation -- and never again by the daemon.
pub const STORE_ID_MARKER_NAME: &str = ".store-id";

/// Process-wide temp-name nonce. Uniqueness per ATTEMPT is load-bearing: a
/// cancelled attempt's detached writer must never share a temp inode with a
/// retry of the same block, or the retry could rename a half-written file
/// into place. `O_CREAT|O_EXCL` turns any collision into a loud error
/// instead of silent inode sharing.
static TEMP_NONCE: AtomicU64 = AtomicU64::new(0);

/// The low-level disk operations the atomic writer performs.
///
/// This is the injection seam the old `AsyncFileSystem` trait used to be:
/// tests substitute an implementation that records calls or fails them.
pub(super) trait BlockDiskOps: Send + Sync + std::fmt::Debug {
    fn create_dir_all(&self, path: &Path) -> io::Result<()>;

    /// Creates `path` with `O_CREAT|O_EXCL` and writes all of `contents`.
    /// An existing file is a loud error, never reused.
    fn write_new_file(&self, path: &Path, contents: &[u8]) -> io::Result<()>;

    /// Fsyncs a file: full fsync when `data_only` is false, fdatasync when
    /// true.
    fn fsync_file(&self, path: &Path, data_only: bool) -> io::Result<()>;

    /// Fsyncs a directory (always a full fsync; directory fdatasync is not
    /// a meaningful operation).
    fn fsync_dir(&self, path: &Path) -> io::Result<()>;

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;

    /// Removes a file; a file that is already gone is `Ok(())`.
    fn remove_file(&self, path: &Path) -> io::Result<()>;

    /// Entries of a directory, for the open-time temp purge.
    fn list_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>>;

    /// Device id of the filesystem holding `path`; `None` where the
    /// platform cannot say (the same-device check is then skipped).
    fn device_of(&self, path: &Path) -> io::Result<Option<u64>>;
}

/// The real thing: std::fs against the local filesystem.
#[derive(Debug)]
pub(super) struct RealDiskOps;

impl BlockDiskOps for RealDiskOps {
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        fs::create_dir_all(path)
    }

    fn write_new_file(&self, path: &Path, contents: &[u8]) -> io::Result<()> {
        use std::io::Write;
        let mut f = fs::OpenOptions::new()
            .write(true)
            .create_new(true) // O_CREAT | O_EXCL
            .open(path)?;
        f.write_all(contents)
    }

    fn fsync_file(&self, path: &Path, data_only: bool) -> io::Result<()> {
        let f = fs::File::open(path)?;
        if data_only {
            f.sync_data()
        } else {
            f.sync_all()
        }
    }

    fn fsync_dir(&self, path: &Path) -> io::Result<()> {
        fs::File::open(path)?.sync_all()
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        fs::rename(from, to)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        match fs::remove_file(path) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        }
    }

    fn list_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        fs::read_dir(path)?
            .map(|entry| entry.map(|e| e.path()))
            .collect()
    }

    #[cfg(unix)]
    fn device_of(&self, path: &Path) -> io::Result<Option<u64>> {
        use std::os::unix::fs::MetadataExt;
        Ok(Some(fs::metadata(path)?.dev()))
    }

    #[cfg(not(unix))]
    fn device_of(&self, _path: &Path) -> io::Result<Option<u64>> {
        Ok(None)
    }
}

/// A block whose bytes are written to a temp file and nothing more (ADR
/// 0010).
///
/// Produced by [`AtomicBlockWriter::stage_block`], consumed by
/// [`land_batch`](AtomicBlockWriter::land_batch) or
/// [`discard_staged`](AtomicBlockWriter::discard_staged). It is a promise
/// about disk state and nothing else: the bytes exist under a name only this
/// attempt knows, at a depth already chosen, so landing it is a rename and a
/// directory sync -- no decision left to make.
#[derive(Debug)]
pub(crate) struct StagedBlock {
    /// Fanout depth `final_path` was derived at, which the block's record
    /// must name -- the record follows the file, never the other way round.
    pub(super) depth: u8,
    /// `.tmp/<hex id>-<nonce>`: unique per attempt.
    temp_path: PathBuf,
    /// Where the rename puts it.
    final_path: PathBuf,
    /// `final_path`'s parent, which the batch's directory sync covers.
    fanout_dir: PathBuf,
}

impl StagedBlock {
    /// The temp file the batch sync must flush.
    pub(super) fn temp_path(&self) -> &Path {
        &self.temp_path
    }
}

/// The atomic write protocol over a [`BlockDiskOps`].
///
/// One per store (it lives on `SharedBlockStore`); the known-durable cache
/// and the temp dir are store-wide state.
#[derive(Debug)]
pub(super) struct AtomicBlockWriter {
    root: PathBuf,
    tmp: PathBuf,
    durability: Durability,
    /// Directories known fsynced since open. Entries are only added AFTER a
    /// successful fsync, so membership is a durable-on-crash claim (at
    /// `Buffer` the set is never consulted -- nothing is claimed durable).
    known_durable: Mutex<HashSet<PathBuf>>,
    /// What the root's `.store-id` said at open (ADR 0012). Read here
    /// because open is the only moment it can be read before anything has
    /// been written; compared against the header by `SharedBlockStore::new`,
    /// which is the only place that knows both halves.
    store_id_marker: StoreIdMarker,
}

impl AtomicBlockWriter {
    /// Store-open duties, then the writer.
    ///
    /// - creates `root` and `root/.tmp`;
    /// - fsyncs both and the parent of `root` (per durability);
    /// - purges `root/.tmp/*` -- crash residue is garbage by definition;
    /// - same-device check: `root` and `root/.tmp` must be on one
    ///   filesystem, or the rename in [`write_block`](Self::write_block)
    ///   could not be atomic. Refused loudly at open; a runtime `EXDEV`
    ///   (someone mounted over a fanout dir while running) is logged as a
    ///   store-level fault when the rename fails.
    /// - reads the `.store-id` marker (ADR 0012) and hands it up: the
    ///   comparison against the header belongs to `SharedBlockStore::new`,
    ///   which opens the database this root claims to belong to.
    pub fn open(ops: &dyn BlockDiskOps, root: PathBuf, durability: Durability) -> io::Result<Self> {
        let tmp = root.join(TMP_DIR_NAME);
        ops.create_dir_all(&root)?;
        ops.create_dir_all(&tmp)?;

        // Purge temp residue before anything else can collide with it.
        for stale in ops.list_dir(&tmp)? {
            ops.remove_file(&stale)?;
        }

        if let (Some(root_dev), Some(tmp_dev)) = (ops.device_of(&root)?, ops.device_of(&tmp)?)
            && root_dev != tmp_dev
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "blocks root {} (device {root_dev}) and temp dir {} (device {tmp_dev}) \
                     are on different filesystems; block renames would not be atomic",
                    root.display(),
                    tmp.display(),
                ),
            ));
        }

        let store_id_marker = read_store_id_marker(&root)?;

        let writer = Self {
            root,
            tmp,
            durability,
            known_durable: Mutex::new(HashSet::new()),
            store_id_marker,
        };

        if writer.syncs() {
            ops.fsync_dir(&writer.tmp)?;
            ops.fsync_dir(&writer.root)?;
            if let Some(parent) = writer.root.parent() {
                ops.fsync_dir(parent)?;
            }
            let mut durable = writer.known_durable.lock().expect("durable set poisoned");
            durable.insert(writer.tmp.clone());
            durable.insert(writer.root.clone());
        }

        Ok(writer)
    }

    /// Whether this durability level fsyncs at all.
    fn syncs(&self) -> bool {
        !matches!(self.durability, Durability::Buffer)
    }

    /// What the blocks root claimed about its store at open (ADR 0012).
    pub fn store_id_marker(&self) -> &StoreIdMarker {
        &self.store_id_marker
    }

    /// Writes (or rewrites) the blocks root's pairing marker.
    ///
    /// Called only by `SharedBlockStore::new`, and only after the header
    /// side of the identity is durable: header first, marker second, so the
    /// crash window leaves the state a reopen can complete without judging.
    pub fn write_store_id_marker(&self, ops: &dyn BlockDiskOps, id: StoreId) -> io::Result<()> {
        store_id::write_marker(ops, &self.root, &self.tmp, id)
    }

    /// Writes `bytes` as block `id` at fanout `depth` and returns the final
    /// path. The ADR 0006 per-block sequence:
    ///
    /// 1. `create_dir_all` the fanout dir;
    /// 2. exclusive-create `.tmp/<hex id>-<nonce>` and write all bytes;
    /// 3. fdatasync the temp file;
    /// 4. fsync the fanout chain deepest-up until a known-durable ancestor
    ///    (directories always get full fsync);
    /// 5. rename over the final path -- unconditionally: if an orphan or a
    ///    concurrent writer's identical file is there, rename-over installs
    ///    identical bytes and heals in place;
    /// 6. fsync the fanout dir the file landed in.
    ///
    /// `Buffer` skips steps 3, 4 and 6. The temp file is unlinked on any
    /// failure after it was created.
    pub fn write_block(
        &self,
        ops: &dyn BlockDiskOps,
        id: &BlockId,
        depth: u8,
        bytes: &[u8],
    ) -> io::Result<PathBuf> {
        let final_path = block_disk_path(id, depth, self.root.clone());
        let fanout_dir = final_path
            .parent()
            .expect("block paths always have a parent dir")
            .to_path_buf();

        ops.create_dir_all(&fanout_dir)?;

        let nonce = TEMP_NONCE.fetch_add(1, Ordering::Relaxed);
        let temp_path = self.tmp.join(format!("{}-{nonce}", id.to_hex()));

        let attempt = (|| -> io::Result<()> {
            ops.write_new_file(&temp_path, bytes)?;

            if self.syncs() {
                // fdatasync, always (ADR 0010): the file was just created and
                // fully written, so data+size is everything that matters, and
                // the directory fsync below carries the rename's durability.
                ops.fsync_file(&temp_path, true)?;
                self.fsync_chain_deepest_up(ops, &fanout_dir)?;
            }

            if let Err(e) = ops.rename(&temp_path, &final_path) {
                if e.kind() == io::ErrorKind::CrossesDevices {
                    tracing::error!(
                        from = %temp_path.display(),
                        to = %final_path.display(),
                        "EXDEV on block rename: blocks root and temp dir no longer share \
                         a filesystem -- store-level fault"
                    );
                }
                return Err(e);
            }

            if self.syncs() {
                ops.fsync_dir(&fanout_dir)?;
            }
            Ok(())
        })();

        match attempt {
            Ok(()) => Ok(final_path),
            Err(e) => {
                // Best effort: the temp is garbage either way; open-time
                // purge collects it if this unlink loses too.
                let _ = ops.remove_file(&temp_path);
                Err(e)
            }
        }
    }

    /// Unlinks the block file for `id` at `depth`. A file that is already
    /// gone is success -- unlink is the idempotent tail of DELETE.
    pub fn unlink_block(&self, ops: &dyn BlockDiskOps, id: &BlockId, depth: u8) -> io::Result<()> {
        ops.remove_file(&block_disk_path(id, depth, self.root.clone()))
    }

    /// Batch step 1: writes `bytes` to a temp file for block `id` at fanout
    /// `depth`, and stops there.
    ///
    /// Steps 1 and 2 of [`write_block`](Self::write_block) -- make the fanout
    /// directory, exclusive-create the temp file, write it all -- and none of
    /// the rest. Nothing is synced, nothing is renamed, so nothing a reader
    /// or another writer can see has changed: the returned [`StagedBlock`] is
    /// entirely this request's, until its batch lands it.
    ///
    /// The temp file is unlinked if the write itself fails, so a failed stage
    /// leaves nothing behind. A stage that succeeds and is then abandoned is
    /// the caller's to [`discard`](Self::discard_staged).
    pub fn stage_block(
        &self,
        ops: &dyn BlockDiskOps,
        id: &BlockId,
        depth: u8,
        bytes: &[u8],
    ) -> io::Result<StagedBlock> {
        let final_path = block_disk_path(id, depth, self.root.clone());
        let fanout_dir = final_path
            .parent()
            .expect("block paths always have a parent dir")
            .to_path_buf();

        ops.create_dir_all(&fanout_dir)?;

        let nonce = TEMP_NONCE.fetch_add(1, Ordering::Relaxed);
        let temp_path = self.tmp.join(format!("{}-{nonce}", id.to_hex()));

        if let Err(e) = ops.write_new_file(&temp_path, bytes) {
            // Best effort: the temp is garbage either way, and the open-time
            // purge collects it if this unlink loses too.
            let _ = ops.remove_file(&temp_path);
            return Err(e);
        }

        Ok(StagedBlock {
            depth,
            temp_path,
            final_path,
            fanout_dir,
        })
    }

    /// Batch step 2: fdatasyncs every staged file, concurrently.
    ///
    /// The one async method here, and the one place the batch's concurrency
    /// primitive lives: today a `spawn_blocking` per file joined as a set,
    /// tomorrow whatever is faster. Callers see "these files are on stable
    /// storage when this returns" and nothing else.
    ///
    /// No stripe is held across this, on purpose (ADR 0010): temp files are
    /// per-attempt-unique and invisible, so the syncs -- the expensive part
    /// -- need no serialization at all, and stripe hold time stays down to
    /// the renames plus the one transaction.
    ///
    /// At `Buffer` durability this is a no-op, as every other sync is.
    pub async fn sync_batch(
        &self,
        ops: Arc<dyn BlockDiskOps>,
        temp_paths: Vec<PathBuf>,
    ) -> io::Result<()> {
        if !self.syncs() || temp_paths.is_empty() {
            return Ok(());
        }

        let mut tasks = Vec::with_capacity(temp_paths.len());
        for path in temp_paths {
            let ops = Arc::clone(&ops);
            tasks.push(tokio::task::spawn_blocking(move || {
                // fdatasync: the file was created and fully written by this
                // batch, so data + size is all there is, and the directory
                // fsync in land_batch carries the rename's durability.
                ops.fsync_file(&path, true)
            }));
        }

        // Every task is awaited even after one fails: they are already
        // running, and abandoning them would leave syncs in flight against
        // files a failure path is about to unlink.
        let mut first_error = None;
        for task in tasks {
            let result = match task.await {
                Ok(result) => result,
                Err(join_err) => Err(io::Error::other(format!(
                    "block sync task did not complete: {join_err}"
                ))),
            };
            if let Err(e) = result
                && first_error.is_none()
            {
                first_error = Some(e);
            }
        }
        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Batch step 3: renames every staged file into place, then fsyncs each
    /// directory the batch touched exactly once.
    ///
    /// Called with the batch's stripes held, immediately before the one
    /// transaction that records the blocks. Synchronous: it runs inside the
    /// same blocking closure as the transaction, which is what makes the
    /// rename-through-commit stretch uncancellable.
    ///
    /// The directory set is COMPUTED, not assumed. A 16 MiB part usually
    /// touches 16 distinct fanout directories and each is synced once; any
    /// ancestor of one of them that is not yet known durable is synced too,
    /// deepest first, so a directory's own entry in its parent is durable
    /// before the batch's records exist. `Buffer` skips all of it.
    pub fn land_batch(&self, ops: &dyn BlockDiskOps, staged: &[&StagedBlock]) -> io::Result<()> {
        for block in staged {
            if let Err(e) = ops.rename(&block.temp_path, &block.final_path) {
                if e.kind() == io::ErrorKind::CrossesDevices {
                    tracing::error!(
                        from = %block.temp_path.display(),
                        to = %block.final_path.display(),
                        "EXDEV on block rename: blocks root and temp dir no longer share \
                         a filesystem -- store-level fault"
                    );
                }
                return Err(e);
            }
        }

        if !self.syncs() {
            return Ok(());
        }

        // Landing directories always get a sync (a rename just added an entry
        // to each); their ancestors only until one already known durable.
        let mut chain: Vec<PathBuf> = Vec::new();
        let mut planned: HashSet<PathBuf> = HashSet::new();
        {
            let durable = self.known_durable.lock().expect("durable set poisoned");
            for block in staged {
                let mut cursor = block.fanout_dir.as_path();
                if planned.insert(cursor.to_path_buf()) {
                    chain.push(cursor.to_path_buf());
                }
                while let Some(parent) = cursor.parent() {
                    if cursor == self.root || durable.contains(parent) {
                        break;
                    }
                    if !planned.insert(parent.to_path_buf()) {
                        break;
                    }
                    chain.push(parent.to_path_buf());
                    cursor = parent;
                }
            }
        }

        // Deepest first, so a directory is durable before the entry naming it
        // in its parent is.
        chain.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
        for dir in &chain {
            ops.fsync_dir(dir)?;
        }
        self.known_durable
            .lock()
            .expect("durable set poisoned")
            .extend(chain);
        Ok(())
    }

    /// Drops a staged block that will not be landed: its temp file is
    /// removed.
    ///
    /// Two callers, both routine. A batch that fails before landing discards
    /// everything it staged. A batch whose block turned out to be a dedup hit
    /// at commit time -- another writer got there first -- discards that one
    /// block's surplus file, because the live record's own file is already at
    /// the final path and identical.
    ///
    /// Best effort by design: the temp file is garbage whatever happens to
    /// this unlink, and the store-open purge is the backstop.
    pub fn discard_staged(&self, ops: &dyn BlockDiskOps, block: &StagedBlock) {
        if let Err(e) = ops.remove_file(&block.temp_path) {
            tracing::warn!(
                path = %block.temp_path.display(),
                error = %e,
                "Could not remove a staged block's temp file; the next store open will"
            );
        }
    }

    /// Fsyncs `dir` and its ancestors (deepest first) up to the first
    /// ancestor already known durable, then records them. The blocks root
    /// is inserted at open, so the walk always terminates there.
    fn fsync_chain_deepest_up(&self, ops: &dyn BlockDiskOps, dir: &Path) -> io::Result<()> {
        let mut chain = Vec::new();
        {
            let durable = self.known_durable.lock().expect("durable set poisoned");
            let mut cursor = dir;
            while !durable.contains(cursor) {
                chain.push(cursor.to_path_buf());
                match cursor.parent() {
                    Some(parent) if cursor != self.root => cursor = parent,
                    _ => break,
                }
            }
        }
        for dir in &chain {
            ops.fsync_dir(dir)?;
        }
        let mut durable = self.known_durable.lock().expect("durable set poisoned");
        durable.extend(chain);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
