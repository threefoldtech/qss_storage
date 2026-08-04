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

    // The file: fdatasync, always (ADR 0010). Its directory entry is made
    // durable by the dir fsyncs, not by a full file fsync.
    assert_eq!(
        during
            .iter()
            .filter(|e| e.starts_with("fsync-file:fdatasync "))
            .count(),
        1,
        "{during:?}"
    );
    assert_eq!(
        during
            .iter()
            .filter(|e| e.starts_with("fsync-file:fsync "))
            .count(),
        0,
        "no block file ever gets a full fsync: {during:?}"
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

/// Block files get data-only syncs and directories get full ones -- the
/// split ADR 0010 settled on, now that there is no level to choose it.
#[test]
fn block_files_are_data_only_and_dirs_are_full() {
    let ops = RecordingOps::recording_over_real();
    let (writer, _dir) = open_writer(&ops, Durability::Fsync);
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

/// The marker round-trips through the atomic protocol: temp file, fsync,
/// rename, and nothing left in `.tmp`.
#[test]
fn the_store_id_marker_round_trips_through_temp_and_rename() {
    let ops = RecordingOps::recording_over_real();
    let (writer, dir) = open_writer(&ops, Durability::Buffer);
    let root = dir.path().join("blocks");
    assert_eq!(*writer.store_id_marker(), StoreIdMarker::Absent);

    let id = StoreId::generate();
    writer.write_store_id_marker(&ops, id).unwrap();

    let marker = root.join(STORE_ID_MARKER_NAME);
    assert_eq!(
        std::fs::read_to_string(&marker).unwrap(),
        format!("{}\n", id.to_hex())
    );
    assert_eq!(
        read_store_id_marker(&root).unwrap(),
        StoreIdMarker::Present(id)
    );
    assert_eq!(
        std::fs::read_dir(root.join(TMP_DIR_NAME)).unwrap().count(),
        0,
        "the marker's temp file is renamed away, not left behind"
    );
    // Written the way blocks are: exclusive create, sync, rename, dirsync
    // -- even at Buffer, because this one is identity, not data.
    assert!(ops.count("rename") >= 1, "{:?}", ops.entries());
    assert_eq!(ops.count("fsync-file:fsync "), 1, "{:?}", ops.entries());

    // A reopen sees it, and rewriting it is not an error.
    let reopened = AtomicBlockWriter::open(&ops, root.clone(), Durability::Buffer).unwrap();
    assert_eq!(*reopened.store_id_marker(), StoreIdMarker::Present(id));
    let second = StoreId::generate();
    reopened.write_store_id_marker(&ops, second).unwrap();
    assert_eq!(
        read_store_id_marker(&root).unwrap(),
        StoreIdMarker::Present(second)
    );
}

/// A marker that is not an id names no store: it is damage, reported as
/// such rather than parsed into a claim.
#[test]
fn an_unparseable_marker_is_not_a_claim() {
    let dir = tempdir().unwrap();
    let root = dir.path().join("blocks");
    std::fs::create_dir_all(&root).unwrap();

    for junk in [
        &b""[..],
        b"not hex at all\n",
        b"deadbeef\n",
        &[0xffu8, 0xfe][..],
    ] {
        std::fs::write(root.join(STORE_ID_MARKER_NAME), junk).unwrap();
        assert!(
            matches!(
                read_store_id_marker(&root).unwrap(),
                StoreIdMarker::Unreadable(_)
            ),
            "{junk:?} is not a store id"
        );
    }
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
