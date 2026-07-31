//! Crash-window fixture helpers for ADR 0005 (fsck) -- built now, next to
//! the protocol that produces them (ADR 0006 plan component 9).
//!
//! Each helper constructs one residue class the file-first protocol can
//! leave behind, exactly as a crash or cancellation would, and each has a
//! test pinning that the construction really is what fsck will meet. The
//! fsck passes (ADR 0005) will drive their detection and collection
//! against these constructors.

use std::path::Path;

use crate::metastore::{Block, BlockId, ContentHash, MetaStore, block_disk_path};

/// Residue class 1: an orphan block file without a record -- the crash
/// window between the rename and the record commit, or a cancelled PUT
/// whose insert never ran. Heal path: a later PUT of the same block
/// adopts it in place; fsck collects it otherwise.
pub(crate) fn plant_orphan_file(blocks_root: &Path, id: &BlockId, depth: u8, bytes: &[u8]) {
    let path = block_disk_path(id, depth, blocks_root.to_path_buf());
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, bytes).unwrap();
}

/// Residue class 2: an orphan at a NON-policy depth (depth-change
/// residue): a file named `<id>` at depth `d1` while the live record says
/// `d2` -- the store once placed the block deeper or shallower than the
/// current policy would. The record's depth wins for GET/DELETE; the
/// off-depth file is unreferenced residue.
pub(crate) fn plant_off_depth_duplicate(blocks_root: &Path, id: &BlockId, depth: u8, bytes: &[u8]) {
    plant_orphan_file(blocks_root, id, depth, bytes);
}

/// Residue class 3: `.tmp` residue -- a temp file whose writer died
/// before the rename. Collected wholesale at store open; fsck never has
/// to reason about it beyond "everything in .tmp is garbage".
pub(crate) fn plant_tmp_residue(blocks_root: &Path, id: &BlockId, nonce: u64) {
    let tmp = blocks_root.join(super::block_disk::TMP_DIR_NAME);
    std::fs::create_dir_all(&tmp).unwrap();
    std::fs::write(tmp.join(format!("{}-{nonce}", id.to_hex())), b"torn temp").unwrap();
}

/// Residue class 4: a Buffer-mode dangling record -- a committed block
/// record whose file vanished with the page cache (durability=buffer
/// fsyncs nothing, so power loss can keep the record and drop the file).
/// This is the one residue class where a GET fails; fsck's recount must
/// treat it as loss-of-file, not loss-of-reference.
pub(crate) fn plant_dangling_record(
    shared: &super::shared_block_store::SharedBlockStore,
    id: BlockId,
    depth: u8,
) {
    let mut tx = shared.meta_store().begin_transaction();
    tx.insert_new_block(id, 1234, depth).unwrap();
    tx.commit().unwrap();
    // No file is written: that is the point.
}

/// Residue class 5: a degraded record -- what fsck's repair leaves behind
/// for a block whose bytes are unrecoverable (ADR 0005). `rc` accounts for
/// the holders that still reference the id, and `size` is the block's real
/// size, since the record's own size field is what a heal must agree with.
/// The write path treats it as absent for dedup, so the next PUT of the
/// same content rewrites the file and clears the flag.
pub(crate) fn plant_degraded_record(
    shared: &super::shared_block_store::SharedBlockStore,
    id: BlockId,
    depth: u8,
    rc: usize,
    size: usize,
) {
    let mut block = Block::from_parts(size, depth, rc, 0);
    block.set_degraded(true);
    let mut tx = shared.meta_store().begin_transaction();
    tx.put_block_record(id, &block).unwrap();
    tx.commit().unwrap();
    // No file either: degraded means the bytes are gone.
}

/// Rewrites an existing record's rc to exactly `rc`, leaving size, depth
/// and flags alone. The shared half of the two rc-residue constructors
/// below; goes through [`Transaction::put_block_record`], which is the raw
/// write fsck's repair uses too.
fn set_rc(shared: &super::shared_block_store::SharedBlockStore, id: BlockId, rc: usize) {
    let record = shared
        .block_tree()
        .get_block(id.as_slice())
        .unwrap()
        .expect("the record to adjust must exist");
    let adjusted = Block::from_parts(record.size(), record.depth(), rc, record.flags());
    let mut tx = shared.meta_store().begin_transaction();
    tx.put_block_record(id, &adjusted).unwrap();
    tx.commit().unwrap();
}

/// Residue class 6: an inflated refcount -- the leak direction. Produced by
/// a DELETE that crashed after removing the object record, and by an
/// overwrite that crashed after committing the new record and before
/// releasing the old one's blocks (ADR 0008 -- a SUCCESSFUL overwrite
/// leaves nothing here, it releases what it displaced). Costs space, never
/// data; fsck's recount lowers it to the walked truth.
pub(crate) fn plant_inflated_rc(
    shared: &super::shared_block_store::SharedBlockStore,
    id: BlockId,
    extra: usize,
) {
    let rc = shared
        .block_tree()
        .get_block(id.as_slice())
        .unwrap()
        .expect("the record to inflate must exist")
        .rc();
    set_rc(shared, id, rc + extra);
}

/// Residue class 7: a deflated refcount -- the loss direction, and the one
/// no protocol path produces: it means a reference was dropped without its
/// holder going away, so the next DELETE of one holder frees a block the
/// others still read. fsck reports it CRITICAL and raises it back to the
/// walked truth.
pub(crate) fn plant_deflated_rc(
    shared: &super::shared_block_store::SharedBlockStore,
    id: BlockId,
    missing: usize,
) {
    let rc = shared
        .block_tree()
        .get_block(id.as_slice())
        .unwrap()
        .expect("the record to deflate must exist")
        .rc();
    set_rc(shared, id, rc.saturating_sub(missing));
}

/// Residue class 8: silent corruption -- a block file whose bytes changed
/// under a name that still claims their old hash. Same name, same length,
/// so only a re-hash sees it: the shape the `--scrub` pass exists for.
pub(crate) fn plant_bit_flip(blocks_root: &Path, id: &BlockId, depth: u8) {
    let path = block_disk_path(id, depth, blocks_root.to_path_buf());
    let mut bytes = std::fs::read(&path).expect("the block file to rot must exist");
    assert!(!bytes.is_empty(), "an empty block cannot be bit-flipped");
    bytes[0] ^= 0x01;
    std::fs::write(&path, &bytes).unwrap();
}

/// Residue class 9: a half-deleted bucket -- `bucket_delete` removes the
/// `_BUCKETS` row before tearing the objects down, so a crash mid-loop
/// strands an object tree that no listing names and whose records still
/// hold block references. fsck resumes the teardown.
pub(crate) fn plant_half_deleted_bucket(namespace: &MetaStore, bucket: &str) {
    namespace
        .get_allbuckets_tree()
        .unwrap()
        .remove(bucket.as_bytes())
        .unwrap();
}

/// An upload record with a caller-chosen creation time (ADR 0003): the
/// live half of the multipart shapes, and the only way to test anything
/// that ages uploads -- fsck's reported age, the GC's TTL -- without
/// sleeping through a TTL.
///
/// `created_at` is a Unix timestamp in seconds, as the record stores it:
/// `Utc::now().timestamp() - n` is an upload that started `n` seconds ago.
/// Planted through the same key and the same tree
/// [`CasFS::create_upload`](super::CasFS::create_upload) writes, so
/// everything that reads an upload record finds this one.
pub(crate) fn plant_upload_record(
    fs: &super::CasFS,
    bucket: &str,
    key: &str,
    upload_id: &str,
    created_at: i64,
) {
    let record = crate::metastore::UploadRecord::with_created_at(
        created_at,
        bucket.to_string(),
        key.to_string(),
        upload_id.to_string(),
    );
    fs.shared_block_store()
        .uploads_tree()
        .insert(
            &super::uploads::upload_key(bucket, key, upload_id),
            record.to_vec(),
        )
        .unwrap();
}

/// Residue class 10: a stale part record -- a part of an upload that was
/// never completed or aborted. Its blocks stay referenced (part records are
/// holders like any other).
///
/// With no upload record beside it this plants an ORPHAN part: the shape
/// ADR 0003's GC and fsck's `orphan_part` finding are about, which
/// `--repair` reaps. Plant [`plant_upload_record`] for the same triple
/// first and the part belongs to a live in-flight upload instead, which
/// fsck reports (`multipart_upload`) and never touches.
pub(crate) fn plant_stale_part(
    fs: &super::CasFS,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i64,
    blocks: Vec<BlockId>,
) {
    fs.insert_multipart_part(
        bucket.to_string(),
        key.to_string(),
        1024,
        part_number,
        upload_id.to_string(),
        ContentHash::from([5u8; 16]),
        blocks,
    )
    .unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::shared_block_store::SharedBlockStore;
    use crate::cas::{CasFS, StorageEngine};
    use crate::metastore::Durability;
    use crate::metrics::SharedMetrics;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn fixture_store(dir: &Path) -> (Arc<SharedBlockStore>, CasFS) {
        let shared = Arc::new(
            SharedBlockStore::new(
                dir.join("meta/blocks"),
                dir.join("blocks"),
                StorageEngine::Fjall,
                Some(1),
                Some(Durability::Buffer),
                None,
                None,
            )
            .unwrap(),
        );
        let fs = CasFS::new(
            dir.join("meta"),
            shared.clone(),
            SharedMetrics::default(),
            StorageEngine::Fjall,
            Some(1),
            Some(Durability::Buffer),
            false,
        )
        .unwrap();
        (shared, fs)
    }

    fn some_id(seed: u8) -> BlockId {
        BlockId::from([seed; crate::metastore::BLOCKID_SIZE])
    }

    /// Stores `data` as one object and returns its single block id: the
    /// live shape the rc and corruption fixtures damage.
    async fn put(fs: &CasFS, bucket: &str, key: &str, data: Vec<u8>) -> BlockId {
        let id = fs.hasher().hash(&data);
        let len = data.len();
        let stream = crate::cas::AsyncByteStream::new(futures::stream::once(async move {
            Ok(bytes::Bytes::from(data))
        }));
        fs.store_single_object_and_meta(bucket, key, stream, len)
            .await
            .unwrap();
        id
    }

    /// Orphan file: on disk, not in the tree. The shape fsck's
    /// orphan-collection pass starts from.
    #[test]
    fn orphan_file_is_file_without_record() {
        let dir = tempdir().unwrap();
        let (shared, fs) = fixture_store(dir.path());
        let id = some_id(0x11);

        plant_orphan_file(fs.fs_root(), &id, 2, b"orphan bytes");

        let path = block_disk_path(&id, 2, fs.fs_root().clone());
        assert!(path.is_file());
        assert!(
            shared
                .block_tree()
                .get_block(id.as_slice())
                .unwrap()
                .is_none(),
            "an orphan has no record"
        );
    }

    /// Off-depth duplicate: record says d2, an unreferenced file sits at
    /// d1. GET follows the record; the d1 file is residue.
    #[tokio::test]
    async fn off_depth_duplicate_leaves_the_record_authoritative() {
        let dir = tempdir().unwrap();
        let (shared, fs) = fixture_store(dir.path());
        fs.create_bucket("b").unwrap();

        let data = b"real content".repeat(64).to_vec();
        let id = shared.hasher().hash(&data);
        let len = data.len();
        let stream = crate::cas::AsyncByteStream::new(futures::stream::once(async move {
            Ok(bytes::Bytes::from(data))
        }));
        fs.store_single_object_and_meta("b", "k", stream, len)
            .await
            .unwrap();
        let record = shared
            .block_tree()
            .get_block(id.as_slice())
            .unwrap()
            .unwrap();

        // Plant the duplicate at a depth the record does NOT name.
        let off_depth = record.depth() + 1;
        plant_off_depth_duplicate(fs.fs_root(), &id, off_depth, b"stale duplicate");

        assert!(block_disk_path(&id, off_depth, fs.fs_root().clone()).is_file());
        let live = block_disk_path(&id, record.depth(), fs.fs_root().clone());
        assert_eq!(
            shared.hasher().hash(&std::fs::read(&live).unwrap()),
            id,
            "the recorded depth still resolves to the real bytes"
        );
    }

    /// Temp residue: constructed exactly where a torn writer leaves it,
    /// and the store-open purge collects it.
    #[test]
    fn tmp_residue_is_purged_at_open() {
        let dir = tempdir().unwrap();
        {
            let (_shared, fs) = fixture_store(dir.path());
            plant_tmp_residue(fs.fs_root(), &some_id(0x22), 7);
            assert_eq!(
                std::fs::read_dir(fs.fs_root().join(crate::cas::block_disk::TMP_DIR_NAME))
                    .unwrap()
                    .count(),
                1
            );
        }
        // Reopen: the open-time purge must collect it.
        let (_shared, fs) = fixture_store(dir.path());
        assert_eq!(
            std::fs::read_dir(fs.fs_root().join(crate::cas::block_disk::TMP_DIR_NAME))
                .unwrap()
                .count(),
            0,
            "temp residue survives only until the next open"
        );
    }

    /// Dangling record: in the tree, not on disk. The Buffer-mode
    /// power-loss shape; fsck's recount pass must classify it.
    #[test]
    fn dangling_record_is_record_without_file() {
        let dir = tempdir().unwrap();
        let (shared, fs) = fixture_store(dir.path());
        let id = some_id(0x33);

        plant_dangling_record(&shared, id, 1);

        let block = shared
            .block_tree()
            .get_block(id.as_slice())
            .unwrap()
            .expect("the record exists");
        assert!(
            !block.disk_path(&id, fs.fs_root().clone()).exists(),
            "a dangling record has no file"
        );
    }

    /// Degraded record: flagged, still counting its holders, no file. The
    /// state fsck's repair produces and the write path heals.
    #[test]
    fn degraded_record_is_flagged_and_fileless() {
        let dir = tempdir().unwrap();
        let (shared, fs) = fixture_store(dir.path());
        let id = some_id(0x44);

        plant_degraded_record(&shared, id, 2, 3, 4096);

        let block = shared
            .block_tree()
            .get_block(id.as_slice())
            .unwrap()
            .expect("the record exists");
        assert!(block.is_degraded(), "the flag is what makes it degraded");
        assert_eq!(block.rc(), 3, "the holders stay accounted for");
        assert_eq!(block.size(), 4096);
        assert_eq!(block.depth(), 2);
        assert!(
            !block.disk_path(&id, fs.fs_root().clone()).exists(),
            "a degraded record has no file"
        );
    }

    /// Inflated and deflated rc: only the count moves. Everything else the
    /// record says stays true, which is what makes the recount's diff the
    /// only evidence fsck has.
    #[tokio::test]
    async fn rc_residue_moves_the_count_and_nothing_else() {
        let dir = tempdir().unwrap();
        let (shared, fs) = fixture_store(dir.path());
        fs.create_bucket("b").unwrap();
        let id = put(&fs, "b", "k", b"a real block".repeat(20).to_vec()).await;

        let before = shared
            .block_tree()
            .get_block(id.as_slice())
            .unwrap()
            .unwrap();
        assert_eq!(before.rc(), 1);

        plant_inflated_rc(&shared, id, 4);
        let inflated = shared
            .block_tree()
            .get_block(id.as_slice())
            .unwrap()
            .unwrap();
        assert_eq!(inflated.rc(), 5, "one real holder, four leaked");
        assert_eq!(inflated.size(), before.size());
        assert_eq!(inflated.depth(), before.depth());
        assert!(!inflated.is_degraded());

        plant_deflated_rc(&shared, id, 5);
        let deflated = shared
            .block_tree()
            .get_block(id.as_slice())
            .unwrap()
            .unwrap();
        assert_eq!(deflated.rc(), 0, "a reference dropped without its holder");
        assert!(
            deflated.disk_path(&id, fs.fs_root().clone()).is_file(),
            "the file is untouched: only the accounting is damaged"
        );
    }

    /// A bit flip keeps the name and the length, so nothing short of a
    /// re-hash can tell: the whole reason the corruption scrub reads bytes.
    #[tokio::test]
    async fn bit_flip_keeps_the_name_and_the_length() {
        let dir = tempdir().unwrap();
        let (shared, fs) = fixture_store(dir.path());
        fs.create_bucket("b").unwrap();
        let id = put(&fs, "b", "k", b"bytes that rot".repeat(20).to_vec()).await;
        let depth = shared
            .block_tree()
            .get_block(id.as_slice())
            .unwrap()
            .unwrap()
            .depth();
        let path = block_disk_path(&id, depth, fs.fs_root().clone());
        let before = std::fs::read(&path).unwrap();

        plant_bit_flip(fs.fs_root(), &id, depth);

        let after = std::fs::read(&path).unwrap();
        assert_eq!(after.len(), before.len(), "same length");
        assert_ne!(after, before, "different bytes");
        assert_ne!(
            shared.hasher().hash(&after),
            id,
            "the file no longer hashes to the name it is filed under"
        );
    }

    /// A half-deleted bucket: the row is gone, the tree and its records are
    /// not, and the objects still hold their block references.
    #[tokio::test]
    async fn half_deleted_bucket_keeps_its_tree_and_its_references() {
        let dir = tempdir().unwrap();
        let (shared, fs) = fixture_store(dir.path());
        fs.create_bucket("doomed").unwrap();
        let id = put(&fs, "doomed", "k", b"still referenced".repeat(8).to_vec()).await;

        plant_half_deleted_bucket(fs.namespace_meta_store(), "doomed");

        assert!(
            !fs.list_buckets()
                .unwrap()
                .iter()
                .any(|b| b.name() == "doomed"),
            "no listing names it any more"
        );
        assert!(
            fs.namespace_meta_store()
                .list_trees()
                .unwrap()
                .iter()
                .any(|t| t == "doomed"),
            "the object tree outlives the row"
        );
        assert_eq!(
            shared
                .block_tree()
                .get_block(id.as_slice())
                .unwrap()
                .unwrap()
                .rc(),
            1,
            "the stranded object still holds its reference"
        );
    }

    /// A stale part record holds its blocks like any other holder: that is
    /// why the recount counts part records unconditionally. On its own it is
    /// an ORPHAN part -- no upload record names it -- which is what makes it
    /// reapable; with an upload record beside it, it is a live upload's part.
    #[test]
    fn stale_part_record_holds_its_blocks_and_is_an_orphan_alone() {
        let dir = tempdir().unwrap();
        let (_shared, fs) = fixture_store(dir.path());
        fs.create_bucket("b").unwrap();
        let held = some_id(0x55);

        plant_stale_part(&fs, "b", "big", "u-1", 1, vec![held]);

        let part = fs
            .get_multipart_part("b", "big", "u-1", 1)
            .unwrap()
            .unwrap();
        assert_eq!(part.blocks(), &[held]);
        assert!(
            fs.get_upload("b", "big", "u-1").unwrap().is_none(),
            "a stale part alone is an orphan: nothing owns it"
        );

        plant_upload_record(&fs, "b", "big", "u-1", 1_700_000_000);
        assert!(
            fs.get_upload("b", "big", "u-1").unwrap().is_some(),
            "with the upload record, the same part belongs to a live upload"
        );
    }

    /// A planted upload record reads back through the daemon's own point
    /// read, carrying the age it was given: the fixture everything that
    /// ages an upload is tested against.
    #[test]
    fn planted_upload_record_carries_its_chosen_age() {
        let dir = tempdir().unwrap();
        let (_shared, fs) = fixture_store(dir.path());
        let week = 7 * 24 * 60 * 60;
        let started = chrono::Utc::now().timestamp() - week;

        plant_upload_record(&fs, "b", "big", "u-1", started);

        let record = fs.get_upload("b", "big", "u-1").unwrap().expect("planted");
        assert_eq!(record.created_at(), started);
        assert_eq!(record.bucket(), "b");
        assert_eq!(record.key(), "big");
        assert_eq!(record.upload_id(), "u-1");
        // And the listing sees it: the GC's TTL sweep reads it that way.
        assert_eq!(fs.list_uploads().unwrap().len(), 1);
    }
}
