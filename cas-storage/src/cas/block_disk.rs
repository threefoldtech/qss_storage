//! The atomic block file writer (ADR 0006 component 4).
//!
//! Blocks reach their final path by write-temp + fsync + rename, never by
//! writing in place. The invariant this buys (hard rule 5): a block record
//! is committed only after its file is durable at its final path --
//! crash-true at `Fsync`/`Fdatasync`, ordering-true at `Buffer`.
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
//! Everything here is synchronous on purpose: the write and delete paths
//! call it from inside `spawn_blocking` closures that own the block's
//! stripe guard, so no executor thread ever parks on disk I/O.

use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::metastore::{BlockId, Durability, block_disk_path};

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

        let writer = Self {
            root,
            tmp,
            durability,
            known_durable: Mutex::new(HashSet::new()),
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

    /// Writes `bytes` as block `id` at fanout `depth` and returns the final
    /// path. The ADR 0006 per-block sequence:
    ///
    /// 1. `create_dir_all` the fanout dir;
    /// 2. exclusive-create `.tmp/<hex id>-<nonce>` and write all bytes;
    /// 3. fsync the temp file (full at `Fsync`, fdatasync at `Fdatasync`);
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
                ops.fsync_file(&temp_path, matches!(self.durability, Durability::Fdatasync))?;
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
mod tests {
    use super::*;
    use crate::metastore::BLOCKID_SIZE;
    use faster_hex::hex_string;
    use std::sync::Mutex as StdMutex;
    use tempfile::tempdir;

    fn test_id() -> BlockId {
        let mut bytes = [0x42u8; BLOCKID_SIZE];
        bytes[0] = 0xab;
        bytes[1] = 0x01;
        BlockId::from(bytes)
    }

    /// Records every call; optionally fails writes.
    #[derive(Debug, Default)]
    struct RecordingOps {
        log: StdMutex<Vec<String>>,
        fail_writes: bool,
        real: Option<RealDiskOps>,
    }

    impl RecordingOps {
        fn recording_over_real() -> Self {
            Self {
                log: StdMutex::new(Vec::new()),
                fail_writes: false,
                real: Some(RealDiskOps),
            }
        }

        fn log(&self, entry: String) {
            self.log.lock().unwrap().push(entry);
        }

        fn entries(&self) -> Vec<String> {
            self.log.lock().unwrap().clone()
        }

        fn count(&self, prefix: &str) -> usize {
            self.entries()
                .iter()
                .filter(|e| e.starts_with(prefix))
                .count()
        }
    }

    impl BlockDiskOps for RecordingOps {
        fn create_dir_all(&self, path: &Path) -> io::Result<()> {
            self.log(format!("mkdir {}", path.display()));
            self.real
                .as_ref()
                .map(|r| r.create_dir_all(path))
                .unwrap_or(Ok(()))
        }

        fn write_new_file(&self, path: &Path, contents: &[u8]) -> io::Result<()> {
            if self.fail_writes {
                return Err(io::Error::other("injected write failure"));
            }
            self.log(format!("write {}", path.display()));
            self.real
                .as_ref()
                .map(|r| r.write_new_file(path, contents))
                .unwrap_or(Ok(()))
        }

        fn fsync_file(&self, path: &Path, data_only: bool) -> io::Result<()> {
            let mode = if data_only { "fdatasync" } else { "fsync" };
            self.log(format!("fsync-file:{mode} {}", path.display()));
            self.real
                .as_ref()
                .map(|r| r.fsync_file(path, data_only))
                .unwrap_or(Ok(()))
        }

        fn fsync_dir(&self, path: &Path) -> io::Result<()> {
            self.log(format!("fsync-dir {}", path.display()));
            self.real
                .as_ref()
                .map(|r| r.fsync_dir(path))
                .unwrap_or(Ok(()))
        }

        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            self.log(format!("rename {} -> {}", from.display(), to.display()));
            self.real
                .as_ref()
                .map(|r| r.rename(from, to))
                .unwrap_or(Ok(()))
        }

        fn remove_file(&self, path: &Path) -> io::Result<()> {
            self.log(format!("unlink {}", path.display()));
            self.real
                .as_ref()
                .map(|r| r.remove_file(path))
                .unwrap_or(Ok(()))
        }

        fn list_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
            self.real
                .as_ref()
                .map(|r| r.list_dir(path))
                .unwrap_or_else(|| Ok(Vec::new()))
        }

        fn device_of(&self, path: &Path) -> io::Result<Option<u64>> {
            self.real
                .as_ref()
                .map(|r| r.device_of(path))
                .unwrap_or(Ok(None))
        }
    }

    fn open_writer(
        ops: &RecordingOps,
        durability: Durability,
    ) -> (AtomicBlockWriter, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let writer = AtomicBlockWriter::open(ops, dir.path().join("blocks"), durability).unwrap();
        (writer, dir)
    }

    /// The exact fsync set per durability level (component 4 acceptance).
    #[test]
    fn buffer_skips_every_fsync() {
        let ops = RecordingOps::recording_over_real();
        let (writer, _dir) = open_writer(&ops, Durability::Buffer);
        writer.write_block(&ops, &test_id(), 2, b"payload").unwrap();

        assert_eq!(
            ops.count("fsync"),
            0,
            "Buffer must never fsync: {:?}",
            ops.entries()
        );
        assert_eq!(ops.count("rename"), 1, "the rename still happens");
    }

    #[test]
    fn fsync_level_syncs_file_new_dirs_and_parent() {
        let ops = RecordingOps::recording_over_real();
        let (writer, _dir) = open_writer(&ops, Durability::Fsync);
        let before_write = ops.entries().len();
        writer.write_block(&ops, &test_id(), 2, b"payload").unwrap();
        let during = ops.entries()[before_write..].to_vec();

        // The file, full fsync.
        assert_eq!(
            during
                .iter()
                .filter(|e| e.starts_with("fsync-file:fsync "))
                .count(),
            1,
            "{during:?}"
        );
        // The two new fanout dirs (ab/ and ab/01/), deepest-up, plus the
        // post-rename fsync of the landing dir.
        assert_eq!(
            during.iter().filter(|e| e.starts_with("fsync-dir")).count(),
            3,
            "{during:?}"
        );
        // Order: file, chain, rename, parent.
        let rename_pos = during.iter().position(|e| e.starts_with("rename")).unwrap();
        let first_fsync = during.iter().position(|e| e.starts_with("fsync")).unwrap();
        let last_fsync = during
            .iter()
            .rposition(|e| e.starts_with("fsync-dir"))
            .unwrap();
        assert!(
            first_fsync < rename_pos,
            "file+chain fsyncs precede the rename: {during:?}"
        );
        assert!(
            last_fsync > rename_pos,
            "the landing dir is fsynced after the rename: {during:?}"
        );
    }

    #[test]
    fn fdatasync_uses_data_only_for_the_file_and_full_for_dirs() {
        let ops = RecordingOps::recording_over_real();
        let (writer, _dir) = open_writer(&ops, Durability::Fdatasync);
        writer.write_block(&ops, &test_id(), 1, b"payload").unwrap();

        assert_eq!(ops.count("fsync-file:fdatasync"), 1, "{:?}", ops.entries());
        assert_eq!(ops.count("fsync-file:fsync "), 0, "{:?}", ops.entries());
        assert!(ops.count("fsync-dir") >= 1, "dirs always get full fsync");
    }

    /// The known-durable cache: a second write into the same fanout dir
    /// re-fsyncs only the landing dir (post-rename), not the whole chain.
    #[test]
    fn known_durable_dirs_are_not_re_fsynced() {
        let ops = RecordingOps::recording_over_real();
        let (writer, _dir) = open_writer(&ops, Durability::Fsync);
        writer.write_block(&ops, &test_id(), 2, b"payload").unwrap();
        let after_first = ops.entries().len();

        let mut second = [0x42u8; BLOCKID_SIZE];
        second[0] = 0xab;
        second[1] = 0x01;
        second[2] = 0x99;
        writer
            .write_block(&ops, &BlockId::from(second), 2, b"other")
            .unwrap();
        let during = ops.entries()[after_first..].to_vec();

        assert_eq!(
            during.iter().filter(|e| e.starts_with("fsync-dir")).count(),
            1,
            "only the post-rename landing-dir fsync remains: {during:?}"
        );
    }

    #[test]
    fn open_purges_temp_residue() {
        let dir = tempdir().unwrap();
        let root = dir.path().join("blocks");
        let tmp = root.join(TMP_DIR_NAME);
        std::fs::create_dir_all(&tmp).unwrap();
        std::fs::write(tmp.join("deadbeef-0"), b"crash residue").unwrap();
        std::fs::write(tmp.join("cafebabe-7"), b"more residue").unwrap();

        let _writer = AtomicBlockWriter::open(&RealDiskOps, root, Durability::Buffer).unwrap();

        assert_eq!(
            std::fs::read_dir(&tmp).unwrap().count(),
            0,
            "temp residue must be purged at open"
        );
    }

    #[test]
    fn failed_write_leaves_no_temp_behind() {
        let real = RecordingOps::recording_over_real();
        let (writer, dir) = open_writer(&real, Durability::Buffer);
        let failing = RecordingOps {
            fail_writes: true,
            ..RecordingOps::recording_over_real()
        };

        let err = writer
            .write_block(&failing, &test_id(), 1, b"payload")
            .unwrap_err();
        assert_eq!(err.to_string(), "injected write failure");
        assert_eq!(
            std::fs::read_dir(dir.path().join("blocks").join(TMP_DIR_NAME))
                .unwrap()
                .count(),
            0
        );
    }

    #[test]
    fn write_then_unlink_round_trip() {
        let ops = RealDiskOps;
        let dir = tempdir().unwrap();
        let writer =
            AtomicBlockWriter::open(&ops, dir.path().join("blocks"), Durability::Fsync).unwrap();
        let id = test_id();

        let path = writer.write_block(&ops, &id, 3, b"the payload").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"the payload");
        assert_eq!(path, block_disk_path(&id, 3, dir.path().join("blocks")));

        writer.unlink_block(&ops, &id, 3).unwrap();
        assert!(!path.exists());
        // Unlinking a block that is already gone is Ok -- idempotent tail.
        writer.unlink_block(&ops, &id, 3).unwrap();
    }

    /// Rename-over heals an orphan unconditionally: whatever bytes sat at
    /// the final path are replaced, never compared-and-skipped.
    #[test]
    fn rename_over_replaces_an_orphan() {
        let ops = RealDiskOps;
        let dir = tempdir().unwrap();
        let root = dir.path().join("blocks");
        let writer = AtomicBlockWriter::open(&ops, root.clone(), Durability::Buffer).unwrap();
        let id = test_id();

        let final_path = block_disk_path(&id, 2, root);
        std::fs::create_dir_all(final_path.parent().unwrap()).unwrap();
        std::fs::write(&final_path, b"torn orphan bytes").unwrap();

        writer.write_block(&ops, &id, 2, b"real bytes").unwrap();
        assert_eq!(std::fs::read(&final_path).unwrap(), b"real bytes");
    }

    #[test]
    fn temp_names_are_unique_per_attempt() {
        let nonce_a = TEMP_NONCE.fetch_add(1, Ordering::Relaxed);
        let nonce_b = TEMP_NONCE.fetch_add(1, Ordering::Relaxed);
        assert_ne!(nonce_a, nonce_b);

        // And the exclusive create refuses a collision loudly.
        let dir = tempdir().unwrap();
        let path = dir.path().join("x");
        RealDiskOps.write_new_file(&path, b"a").unwrap();
        let err = RealDiskOps.write_new_file(&path, b"b").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
    }

    fn hex_of(byte: u8) -> String {
        hex_string(&[byte])
    }

    /// The fanout chain fsync happens deepest-up.
    #[test]
    fn chain_fsync_order_is_deepest_first() {
        let ops = RecordingOps::recording_over_real();
        let (writer, dir) = open_writer(&ops, Durability::Fsync);
        writer.write_block(&ops, &test_id(), 2, b"payload").unwrap();

        let root = dir.path().join("blocks");
        let level1 = root.join(hex_of(0xab));
        let level2 = level1.join(hex_of(0x01));
        let entries = ops.entries();
        let pos = |p: &Path| {
            entries
                .iter()
                .position(|e| *e == format!("fsync-dir {}", p.display()))
                .unwrap_or_else(|| panic!("no fsync of {}: {entries:?}", p.display()))
        };
        assert!(
            pos(&level2) < pos(&level1),
            "deepest dir must be fsynced first: {entries:?}"
        );
    }
}
