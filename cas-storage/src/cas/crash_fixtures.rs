//! Crash-window fixture helpers for ADR 0005 (fsck) -- built now, next to
//! the protocol that produces them (ADR 0006 plan component 9).
//!
//! Each helper constructs one residue class the file-first protocol can
//! leave behind, exactly as a crash or cancellation would, and each has a
//! test pinning that the construction really is what fsck will meet. The
//! fsck passes (ADR 0005) will drive their detection and collection
//! against these constructors.

use std::path::Path;

use crate::metastore::{BlockId, block_disk_path};

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
}
