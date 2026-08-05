use super::*;
use crate::metastore::store_header::{self, HeaderSpec};
use crate::metastore::{BLOCKID_SIZE, BlockId, BucketMeta, FjallStore, Object};
use tempfile::{TempDir, tempdir};

fn test_store() -> (MetaStore, TempDir) {
    let dir = tempdir().unwrap();
    let store = FjallStore::new(dir.path().to_path_buf(), Some(1), None).unwrap();
    (MetaStore::new(store, None), dir)
}

/// A record fsck marked degraded: holders still counted, bytes gone.
fn degraded_record(size: usize, depth: u8, rc: usize) -> Block {
    let mut block = Block::from_parts(size, depth, rc, 0);
    block.set_degraded(true);
    block
}

/// A new block records the depth the caller chose; the derived disk path
/// follows it.
#[test]
fn insert_new_block_records_the_given_depth() {
    let (meta, dir) = test_store();
    let hash = BlockId::from([0xaau8; BLOCKID_SIZE]);

    let mut tx = meta.begin_transaction();
    let block = tx.insert_new_block(hash, 42, 3).unwrap();
    tx.commit().unwrap();

    assert_eq!(block.rc(), 1);
    assert_eq!(block.depth(), 3);
    assert_eq!(
        block.disk_path(&hash, dir.path().to_path_buf()),
        crate::metastore::block_disk_path(&hash, 3, dir.path().to_path_buf())
    );
}

/// A dedup bump increments rc and keeps the ORIGINAL depth -- that is
/// where the file is.
#[test]
fn bump_block_rc_hits_and_keeps_the_recorded_depth() {
    let (meta, _dir) = test_store();
    let hash = BlockId::from([0xabu8; BLOCKID_SIZE]);

    // No record yet: the bump reports a miss and mutates nothing.
    let mut tx = meta.begin_transaction();
    assert!(tx.bump_block_rc(hash).unwrap().is_none());
    tx.rollback();

    let mut tx = meta.begin_transaction();
    tx.insert_new_block(hash, 42, 2).unwrap();
    tx.commit().unwrap();

    let mut tx = meta.begin_transaction();
    let block = tx.bump_block_rc(hash).unwrap().expect("record exists");
    tx.commit().unwrap();

    assert_eq!(block.rc(), 2);
    assert_eq!(block.depth(), 2, "dedup must not move the block");
}

/// A degraded record is absent for dedup: the bump reports a miss and
/// mutates nothing, so the caller writes the file and heals it.
#[test]
fn bump_block_rc_treats_a_degraded_record_as_absent() {
    let (meta, _dir) = test_store();
    let hash = BlockId::from([0xadu8; BLOCKID_SIZE]);

    let mut tx = meta.begin_transaction();
    tx.put_block_record(hash, &degraded_record(42, 2, 3))
        .unwrap();
    tx.commit().unwrap();

    let mut tx = meta.begin_transaction();
    assert!(tx.bump_block_rc(hash).unwrap().is_none());
    tx.commit().unwrap();

    let block = meta
        .get_block_tree()
        .unwrap()
        .get_block(hash.as_slice())
        .unwrap()
        .unwrap();
    assert_eq!(block.rc(), 3, "a reported miss must not bump");
    assert!(block.is_degraded());
}

/// The insert path heals a degraded record: the flag clears, the record
/// follows the file to the depth it was written at, and the writer's own
/// reference is added to the holders already counted.
#[test]
fn insert_new_block_heals_a_degraded_record() {
    let (meta, _dir) = test_store();
    let hash = BlockId::from([0xaeu8; BLOCKID_SIZE]);

    let mut tx = meta.begin_transaction();
    tx.put_block_record(hash, &degraded_record(42, 1, 4))
        .unwrap();
    tx.commit().unwrap();

    let mut tx = meta.begin_transaction();
    let healed = tx.insert_new_block(hash, 42, 3).unwrap();
    tx.commit().unwrap();

    assert!(!healed.is_degraded());
    assert_eq!(healed.rc(), 5, "four holders plus the healing writer");
    assert_eq!(healed.depth(), 3, "the record follows the new file");

    let stored = meta
        .get_block_tree()
        .unwrap()
        .get_block(hash.as_slice())
        .unwrap()
        .unwrap();
    assert_eq!(stored.rc(), 5);
    assert_eq!(stored.depth(), 3);
    assert!(!stored.is_degraded());
}

/// A bump inside a rolled-back transaction leaves the record untouched:
/// the RMW is transactional, not a blind write.
#[test]
fn bump_block_rc_rolls_back_with_the_transaction() {
    let (meta, _dir) = test_store();
    let hash = BlockId::from([0xacu8; BLOCKID_SIZE]);

    let mut tx = meta.begin_transaction();
    tx.insert_new_block(hash, 42, 1).unwrap();
    tx.commit().unwrap();

    let mut tx = meta.begin_transaction();
    tx.bump_block_rc(hash).unwrap().expect("record exists");
    tx.rollback();

    let block = meta
        .get_block_tree()
        .unwrap()
        .get_block(hash.as_slice())
        .unwrap()
        .unwrap();
    assert_eq!(block.rc(), 1, "rolled-back bump must not persist");
}

/// A bucket becomes a tree of its own name, so a bucket named like one of
/// the store's internal trees would hand a client the store's bookkeeping.
/// Creation must refuse the whole `_` namespace, and refuse it before
/// anything is written.
#[test]
fn insert_bucket_refuses_reserved_names() {
    let (meta, _dir) = test_store();

    for name in [
        store_header::STORE_HEADER_TREE,
        DEFAULT_BLOCK_TREE,
        MULTIPART_PARTS_TREE,
        UPLOADS_TREE,
        "_",
    ] {
        let raw = BucketMeta::new(name.to_string()).to_vec();
        match meta.insert_bucket(name, raw).unwrap_err() {
            MetaError::ReservedBucketName(refused) => assert_eq!(refused, name),
            other => panic!("unexpected error for {name}: {other:?}"),
        }
    }

    // The refusal is not a side effect of the name existing already: a
    // name nothing internal uses is refused just the same, and no tree is
    // left behind.
    let raw = BucketMeta::new("_private".to_string()).to_vec();
    assert!(meta.insert_bucket("_private", raw).is_err());
    assert!(!meta.bucket_exists("_private").unwrap());

    // An ordinary name is unaffected.
    let raw = BucketMeta::new("photos".to_string()).to_vec();
    meta.insert_bucket("photos", raw).unwrap();
    assert!(meta.bucket_exists("photos").unwrap());
}

/// The name is claimed once: a second insert-if-absent is refused, and
/// the record the first one wrote is left exactly as it is.
#[test]
fn insert_bucket_if_absent_claims_a_name_once() {
    let (meta, _dir) = test_store();

    assert!(
        meta.insert_bucket_if_absent("photos", b"the first record".to_vec())
            .unwrap(),
        "a free name is claimed"
    );
    assert!(meta.bucket_exists("photos").unwrap(), "and its tree exists");

    assert!(
        !meta
            .insert_bucket_if_absent("photos", b"a record that must not land".to_vec())
            .unwrap(),
        "a taken name is refused"
    );
    let stored = meta
        .get_allbuckets_tree()
        .unwrap()
        .get(b"photos")
        .unwrap()
        .expect("the record is there");
    assert_eq!(
        stored, b"the first record",
        "the refusal wrote nothing over what the winner stored"
    );

    // Reserved names are refused before anything is written, exactly as
    // the plain insert refuses them.
    match meta
        .insert_bucket_if_absent("_private", b"x".to_vec())
        .unwrap_err()
    {
        MetaError::ReservedBucketName(name) => assert_eq!(name, "_private"),
        other => panic!("unexpected error: {other:?}"),
    }
    assert!(!meta.bucket_exists("_private").unwrap());
}

/// The usage counter is transactional: a delta that was rolled back never
/// happened, and a decrement past zero clamps rather than wrapping.
#[test]
fn usage_deltas_commit_and_roll_back_with_their_transaction() {
    let (meta, _dir) = test_store();
    meta.insert_bucket("photos", BucketMeta::new("photos".to_string()).to_vec())
        .unwrap();
    assert_eq!(meta.bucket_usage("photos").unwrap(), Some(0));

    let mut tx = meta.begin_transaction();
    assert_eq!(tx.add_bucket_usage("photos", 4096).unwrap(), 4096);
    tx.commit().unwrap();
    assert_eq!(meta.bucket_usage("photos").unwrap(), Some(4096));

    // Rolled back: the record it would have accounted for did not land
    // either, so neither may the number.
    let mut tx = meta.begin_transaction();
    tx.add_bucket_usage("photos", 1_000_000).unwrap();
    tx.rollback();
    assert_eq!(meta.bucket_usage("photos").unwrap(), Some(4096));

    // Down, and then past the floor.
    let mut tx = meta.begin_transaction();
    assert_eq!(tx.add_bucket_usage("photos", -96).unwrap(), 4000);
    assert_eq!(
        tx.add_bucket_usage("photos", -1_000_000).unwrap(),
        0,
        "a counter that would go negative is clamped, not wrapped"
    );
    tx.commit().unwrap();
    assert_eq!(meta.bucket_usage("photos").unwrap(), Some(0));

    // A bucket nothing ever accounted has no counter at all, which is not
    // the same answer as zero.
    assert_eq!(meta.bucket_usage("never-existed").unwrap(), None);
}

/// The holder enumeration ADR 0005 recounts from: every bucket tree and
/// every reserved tree the store keeps must be named, because a bucket
/// whose `_BUCKETS` row is gone still holds block references through its
/// object tree.
#[test]
fn list_trees_names_bucket_and_reserved_trees() {
    let dir = tempdir().unwrap();
    let (meta, _header) =
        MetaStore::open_or_create(dir.path().join("db"), Some(1), HeaderSpec::default(), |p| {
            FjallStore::new(p, Some(1), None)
        })
        .unwrap();

    for name in ["photos", "videos"] {
        meta.insert_bucket(name, BucketMeta::new(name.to_string()).to_vec())
            .unwrap();
    }
    // The shared trees exist from the moment they are opened.
    meta.get_block_tree().unwrap();
    meta.get_tree(MULTIPART_PARTS_TREE).unwrap();
    meta.get_tree(UPLOADS_TREE).unwrap();

    let trees = meta.list_trees().unwrap();
    for expected in [
        "photos",
        "videos",
        store_header::STORE_HEADER_TREE,
        DEFAULT_BUCKET_TREE,
        DEFAULT_BLOCK_TREE,
        MULTIPART_PARTS_TREE,
        UPLOADS_TREE,
    ] {
        assert!(
            trees.iter().any(|name| name == expected),
            "{expected} missing from {trees:?}"
        );
    }

    // A half-deleted bucket -- its row removed, its tree left behind --
    // is exactly what the listing must keep showing.
    let buckets = meta.get_allbuckets_tree().unwrap();
    buckets.remove(b"videos").unwrap();
    assert!(
        meta.list_trees()
            .unwrap()
            .iter()
            .any(|name| name == "videos"),
        "the tree outlives its _BUCKETS row"
    );
}

/// An object record naming `blocks`, at a hash derived from `tag` so two
/// fixtures are never mistaken for each other.
fn object(tag: u8, blocks: Vec<BlockId>) -> Object {
    Object::new(
        1024,
        crate::metastore::ContentHash::from([tag; 16]),
        crate::metastore::ObjectData::SinglePart { blocks },
    )
}

fn stored_object(meta: &MetaStore, bucket: &str, key: &str) -> Option<Object> {
    meta.get_meta(bucket, key.as_bytes()).unwrap()
}

/// A replace onto a free key displaces nothing, and the record it wrote
/// is the one that reads back.
#[test]
fn replace_object_on_a_free_key_displaces_nothing() {
    let (meta, _dir) = test_store();
    let fresh = object(0x01, vec![BlockId::from([0x11u8; BLOCKID_SIZE])]);

    let mut tx = meta.begin_transaction();
    let displaced = tx.replace_object("photos", b"a", fresh.to_vec()).unwrap();
    tx.commit().unwrap();

    assert!(displaced.is_none(), "nothing was there to displace");
    assert_eq!(
        stored_object(&meta, "photos", "a").unwrap().blocks(),
        fresh.blocks()
    );
}

/// The point of the primitive: the replace hands back the record it
/// overwrote, which is the list of references the caller must release.
#[test]
fn replace_object_returns_what_it_overwrote() {
    let (meta, _dir) = test_store();
    let old = object(0x01, vec![BlockId::from([0x11u8; BLOCKID_SIZE])]);
    let new = object(0x02, vec![BlockId::from([0x22u8; BLOCKID_SIZE])]);

    let mut tx = meta.begin_transaction();
    tx.replace_object("photos", b"a", old.to_vec()).unwrap();
    tx.commit().unwrap();

    let mut tx = meta.begin_transaction();
    let displaced = tx.replace_object("photos", b"a", new.to_vec()).unwrap();
    tx.commit().unwrap();

    assert_eq!(
        displaced.expect("the old record").blocks(),
        old.blocks(),
        "the displaced record is the caller's to release"
    );
    assert_eq!(
        stored_object(&meta, "photos", "a").unwrap().blocks(),
        new.blocks(),
        "the new record is the visible one"
    );
}

/// Sequential overwrites of one key each displace exactly the previous
/// record -- never the original twice, never one of them not at all.
/// This is what makes each writer's release exact under concurrency:
/// fjall's single-writer transaction turns any interleaving into this
/// sequence.
#[test]
fn sequential_replaces_each_displace_the_previous_record() {
    let (meta, _dir) = test_store();

    let mut previous: Option<Object> = None;
    for tag in 1u8..=4 {
        let next = object(tag, vec![BlockId::from([tag; BLOCKID_SIZE])]);
        let mut tx = meta.begin_transaction();
        let displaced = tx.replace_object("photos", b"a", next.to_vec()).unwrap();
        tx.commit().unwrap();

        match (&previous, &displaced) {
            (None, None) => {}
            (Some(prev), Some(got)) => assert_eq!(
                got.blocks(),
                prev.blocks(),
                "each replace displaces exactly the previous record"
            ),
            other => panic!("displacement mismatch at tag {tag}: {other:?}"),
        }
        previous = Some(next);
    }

    assert_eq!(
        stored_object(&meta, "photos", "a").unwrap().blocks(),
        &[BlockId::from([4u8; BLOCKID_SIZE])],
        "the last writer's record survives"
    );
}

/// A rolled-back replace writes nothing: the read and the write are one
/// transaction, not a read followed by a blind insert.
#[test]
fn replace_object_rolls_back_with_the_transaction() {
    let (meta, _dir) = test_store();
    let old = object(0x01, vec![BlockId::from([0x11u8; BLOCKID_SIZE])]);
    let new = object(0x02, vec![BlockId::from([0x22u8; BLOCKID_SIZE])]);

    let mut tx = meta.begin_transaction();
    tx.replace_object("photos", b"a", old.to_vec()).unwrap();
    tx.commit().unwrap();

    let mut tx = meta.begin_transaction();
    tx.replace_object("photos", b"a", new.to_vec()).unwrap();
    tx.rollback();

    assert_eq!(
        stored_object(&meta, "photos", "a").unwrap().blocks(),
        old.blocks(),
        "a rolled-back replace leaves the original record"
    );
}

/// A displaced record that will not decode fails the call and writes
/// nothing. Overwriting it would strand every reference it names, with
/// no holder left able to name them again.
#[test]
fn replace_object_refuses_to_overwrite_an_undecodable_record() {
    let (meta, _dir) = test_store();
    meta.get_bucket_ext("photos")
        .unwrap()
        .insert(b"a", vec![0xffu8; 3])
        .unwrap();

    let new = object(0x02, vec![BlockId::from([0x22u8; BLOCKID_SIZE])]);
    let mut tx = meta.begin_transaction();
    assert!(tx.replace_object("photos", b"a", new.to_vec()).is_err());
    tx.rollback();

    assert_eq!(
        meta.get_bucket_ext("photos")
            .unwrap()
            .get(b"a")
            .unwrap()
            .unwrap(),
        vec![0xffu8; 3],
        "the unreadable record is left exactly as found"
    );
}

/// Two hashes sharing a leading byte can both live at depth 1: their
/// full-id filenames can never collide, so no allocator is involved.
#[test]
fn shared_prefix_needs_no_allocation() {
    let (meta, dir) = test_store();
    let first = BlockId::from([0x11u8; BLOCKID_SIZE]);
    let mut second_bytes = [0x11u8; BLOCKID_SIZE];
    second_bytes[1] = 0x22;
    let second = BlockId::from(second_bytes);

    let mut tx = meta.begin_transaction();
    let first_block = tx.insert_new_block(first, 1, 1).unwrap();
    tx.commit().unwrap();

    let mut tx = meta.begin_transaction();
    let second_block = tx.insert_new_block(second, 1, 1).unwrap();
    tx.commit().unwrap();

    let p1 = first_block.disk_path(&first, dir.path().to_path_buf());
    let p2 = second_block.disk_path(&second, dir.path().to_path_buf());
    assert_eq!(p1.parent(), p2.parent(), "same depth-1 dir");
    assert_ne!(p1, p2, "full-id names never collide");
}
