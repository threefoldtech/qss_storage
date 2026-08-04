use super::*;
use tempfile::tempdir;

crate::metastore::stores::test_utils::backend_test_battery!(FjallStore, || {
    let dir = tempdir().unwrap();
    let store = FjallStore::new(dir.path().to_path_buf(), Some(1), None).unwrap();
    (store, dir)
});

/// Lock contention is a value, not a panic.
///
/// fjall locks the database directory with `std::fs::File::try_lock`,
/// which is `flock` on Linux -- the lock belongs to the open file
/// description, so a second open contends even from inside one process.
/// That is what makes this testable here rather than only across
/// processes.
///
/// This open used to `.unwrap()`. The tools' exit-code contracts depend on
/// the error arriving as a value: fsck answers a locked store with exit 3
/// (`docs/fsck.md`, ADR 0005), and it cannot do that from a panic.
#[test]
fn a_second_open_reports_the_lock_instead_of_panicking() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();

    let _held = FjallStore::new(path.clone(), Some(1), None).expect("the first open must win");

    let err = FjallStore::new(path.clone(), Some(1), None)
        .expect_err("a second open of a held database must fail");

    assert!(
        matches!(err, MetaError::StoreLocked(ref p) if p == &path.display().to_string()),
        "expected StoreLocked naming {}, got {err:?}",
        path.display()
    );

    // The message is what an operator reads off a terminal, so it has to
    // say what to do about it rather than just name a condition.
    let msg = err.to_string();
    assert!(msg.contains("locked by another process"), "{msg}");
    assert!(msg.contains("daemon"), "{msg}");
}

/// Every byte of every journal file of the store at `dir`, read through
/// the filesystem -- what the KERNEL has, not what the process thinks it
/// wrote. A record still in fjall's `BufWriter` is not in here.
fn journal_bytes(dir: &std::path::Path) -> Vec<u8> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|ext| ext == "jnl") {
            out.extend_from_slice(&std::fs::read(&path).unwrap());
        }
    }
    out
}

/// A bare tree write is on the kernel's side of the buffer when it
/// returns, at both durability levels.
///
/// The ADR 0011 rider's pin at the layer the rider changed. `insert` and
/// `remove` are the ack-carrying non-transactional surface --
/// `CreateBucket`, `CreateMultipartUpload`, `UploadPart`, respcas's `SET`
/// and `DEL` -- and what a client is told about them has to be true of
/// the file on disk, not of a userspace buffer.
///
/// Observable in-process precisely because `buffer` is a `write` and not
/// an fsync: nothing has to crash for the bytes to be visible through the
/// filesystem. The fsync half of `Fsync` is NOT observable this way and
/// is pinned separately, by the mode assertion below.
#[test]
fn a_bare_tree_write_reaches_the_kernel_before_it_returns() {
    for durability in [Durability::Buffer, Durability::Fsync] {
        let dir = tempdir().unwrap();
        let store = FjallStore::new(dir.path().to_path_buf(), Some(1), Some(durability))
            .expect("the store must open");
        let tree = store.tree_open("acked").unwrap();

        tree.insert(b"a-key-a-client-was-told-about", b"value".to_vec())
            .unwrap();

        let journal = journal_bytes(dir.path());
        let needle = b"a-key-a-client-was-told-about";
        assert!(
            journal.windows(needle.len()).any(|w| w == needle),
            "{durability}: an acked bare write is not in the journal on \
             disk -- it is in a userspace buffer a kill would take"
        );

        // The same for the removal side: a DEL a client was told
        // succeeded must not come back.
        tree.insert(b"a-key-that-goes-away", b"value".to_vec())
            .unwrap();
        tree.remove(b"a-key-that-goes-away").unwrap();
        let journal = journal_bytes(dir.path());
        let needle = b"a-key-that-goes-away";
        assert!(
            journal.windows(needle.len()).any(|w| w == needle),
            "{durability}: the removal is not in the journal on disk"
        );

        // The recoverable class (ADR 0013) skips the explicit persist,
        // which makes THIS assertion its load-bearing dependency: the
        // bytes must be kernel-visible purely through fjall's internal
        // Buffer-level write, or a kill -9 could take an acked
        // UploadPart ETag. If a fjall upgrade ever fails this, the
        // recoverable class needs an explicit persist(Buffer) back.
        for relaxed in [MULTIPART_PARTS_TREE, UPLOADS_TREE] {
            let tree = store.tree_open(relaxed).unwrap();
            let key = format!("an-acked-part-record-in-{relaxed}");
            tree.insert(key.as_bytes(), b"value".to_vec()).unwrap();
            let journal = journal_bytes(dir.path());
            assert!(
                journal.windows(key.len()).any(|w| w == key.as_bytes()),
                "{durability}: an acked {relaxed} write is not in the \
                 journal on disk despite fjall's internal Buffer persist \
                 -- the ADR 0013 recoverable class just lost its kill -9 \
                 safety"
            );
        }
    }
}

/// The ADR 0013 contract table: exactly the two multipart state trees
/// are recoverable-class, everything else -- bucket trees, respcas
/// namespaces, anything future -- persists per ack.
#[test]
fn the_ack_persist_table_is_exactly_the_multipart_state_trees() {
    assert_eq!(
        ack_persist_for(MULTIPART_PARTS_TREE),
        AckPersist::Recoverable
    );
    assert_eq!(ack_persist_for(UPLOADS_TREE), AckPersist::Recoverable);
    for contract in ["some-bucket", "_BLOCKS", "acked", "respcas-ns"] {
        assert_eq!(
            ack_persist_for(contract),
            AckPersist::Contract,
            "{contract} must persist per ack: its loss would be silent"
        );
    }
}

/// The configured level really is the persist mode the store applies.
///
/// The half of the contract no in-process assertion can observe: an
/// fsync leaves nothing behind to look at, so what is pinned is that
/// `fsync` plumbs through to fjall's `SyncAll` and `buffer` to its
/// `Buffer`. Beyond this line the trust boundary is fjall's journal
/// writer, which flushes its `BufWriter` and then applies the mode --
/// `SyncAll` fsyncs, `Buffer` returns.
#[test]
fn the_configured_durability_is_the_persist_mode() {
    for (durability, expected) in [
        (Durability::Buffer, fjall::PersistMode::Buffer),
        (Durability::Fsync, fjall::PersistMode::SyncAll),
    ] {
        let dir = tempdir().unwrap();
        let store = FjallStore::new(dir.path().to_path_buf(), Some(1), Some(durability)).unwrap();
        assert_eq!(store.db().durability(), expected, "{durability}");
    }
}

/// Dropping the holder releases the lock, so the refusal above is about
/// contention and not about the store being permanently unusable.
#[test]
fn the_lock_is_released_when_the_store_is_dropped() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();

    let held = FjallStore::new(path.clone(), Some(1), None).unwrap();
    assert!(FjallStore::new(path.clone(), Some(1), None).is_err());

    drop(held);
    FjallStore::new(path, Some(1), None).expect("the lock must be released on drop");
}
