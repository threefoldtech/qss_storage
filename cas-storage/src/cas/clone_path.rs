//! Copying a record from one bucket into another by REFERENCE, without
//! moving a byte (ADR 0014).
//!
//! What it is for: two content-addressed namespaces asked to hold the same
//! content. The bytes are already in the store, verified against their
//! address when they were first written, so the second namespace needs a
//! record of its own and one more reference per block -- not another copy of
//! the data, and not another verification of bytes that are about to be
//! discarded.
//!
//! # Every namespace holds its own references
//!
//! The clone takes a reference per block OCCURRENCE, exactly as a write of
//! the same content would have. That is what makes a later DELETE in one
//! namespace unable to strand another: each holder accounts for itself, and
//! the last one to let go is the one that frees the block.
//!
//! # The ordering, and why it is the opposite of the delete side's
//!
//! References are taken BEFORE the record that will hold them is written.
//! The delete side is the mirror image -- record out first, references after
//! (see [`release_blocks`](super::delete_path::release_blocks)) -- and both
//! orderings are chosen for the same reason: a crash in the middle must
//! leave an over-count, never an under-count. Here:
//!
//! - crash after the bumps, before the record: rc exceeds the walked
//!   holders. Leakage, INFO in fsck, collected by the next recount.
//! - the reverse order would publish a record naming blocks it has not
//!   referenced yet, and a concurrent DELETE taking the last true reference
//!   would unlink the file under it. That is loss.
//!
//! # The clone-versus-DELETE race
//!
//! ADR 0014 asks for the record insert and the rc bumps in ONE transaction,
//! on the grounds that it is the same guarantee class as ADR 0003's
//! `claim_upload_with_parts`. It cannot be one transaction here, and the
//! difference is not a shortcut: `_UPLOADS` and `_MULTIPART_PARTS` live in
//! the same database, whereas an object record lives in the namespace
//! database and `_BLOCKS` lives in the shared blocks database. No
//! transaction spans two fjall databases.
//!
//! The outcome the ADR specifies is delivered by the ordering plus the
//! stripes instead, and it is the same outcome:
//!
//! - the clone's bump lands first: rc is at least two when the DELETE's
//!   decrement arrives, so the block survives and the clone's record is
//!   truthful;
//! - the DELETE's last decrement lands first: the record and the file are
//!   gone, this clone's bump finds nothing under the block's stripe, and the
//!   clone REFUSES -- it returns `None`, having released whatever it had
//!   already taken, and the caller falls through to the ordinary write path
//!   with the bytes it still holds.
//!
//! There is no interleaving in which a record is left naming a dead block,
//! because every bump and every decrement of one block is serialized by that
//! block's stripe, and the record is written only after every bump has
//! succeeded.

use super::delete_path::release_blocks;
use super::fs::CasFS;
use super::object_key;
use super::write_path::create_object_meta;
use crate::metastore::{BlockId, MetaError, Object, ObjectData};

/// Writes `dest_bucket`/`dest_key` as a copy of `source_bucket`/`source_key`,
/// taking a reference to the source's blocks rather than storing bytes.
///
/// `Ok(None)` means the clone did not happen and nothing was written: either
/// the source record is gone, or a block it names was freed by a concurrent
/// DELETE while this call was taking its references. Both mean the same
/// thing to a caller -- the content is not there after all, store it the
/// ordinary way.
///
/// The source must be a record whose content was verified against its key
/// (a Cas namespace's, ADR 0014). Nothing here can check that, and it is the
/// caller's obligation: cloning from a user-keyed record would file bytes
/// under an address they may not hash to.
///
/// The destination write goes through
/// [`create_object_meta`](super::write_path::create_object_meta) like every
/// other object write, so an occupied destination key is displaced and
/// released exactly as an overwrite is (ADR 0008). Two concurrent clones of
/// one key therefore leave rc exact: the second displaces the first's record
/// and releases what it held.
pub(super) async fn clone_object_by_reference(
    fs: &CasFS,
    source_bucket: &str,
    source_key: &[u8],
    dest_bucket: &str,
    dest_key: &[u8],
) -> Result<Option<Object>, MetaError> {
    let Some(source) = fs
        .namespace_meta_store()
        .get_meta(source_bucket, source_key)?
    else {
        return Ok(None);
    };

    let size = source.size();
    let hash = *source.hash();

    let data = match source.data() {
        // Inline records hold bytes, not references: the copy is the record.
        // The bytes copied are the ones that passed verification when they
        // were first written, which is the whole point of cloning rather
        // than trusting what a client has just sent.
        ObjectData::Inline { data } => ObjectData::Inline { data: data.clone() },
        ObjectData::SinglePart { blocks } => {
            let blocks = blocks.clone();
            if !acquire_references(fs, &blocks).await? {
                return Ok(None);
            }
            ObjectData::SinglePart { blocks }
        }
        ObjectData::MultiPart { blocks, parts } => {
            let blocks = blocks.clone();
            if !acquire_references(fs, &blocks).await? {
                return Ok(None);
            }
            ObjectData::MultiPart {
                blocks,
                parts: *parts,
            }
        }
    };

    tracing::debug!(
        source_bucket = %source_bucket,
        source_key = %object_key(source_key),
        bucket = %dest_bucket,
        key = %object_key(dest_key),
        size = size,
        "Cloning a record by reference: no bytes move"
    );

    let obj = create_object_meta(fs, dest_bucket, dest_key, size, hash, data).await?;
    Ok(Some(obj))
}

/// Takes one reference per occurrence in `blocks`, each under its own stripe.
///
/// `false` means a block was not there to reference (freed, or degraded --
/// which reads as absent for exactly this reason: its file is gone). Every
/// reference this call had already taken is released before it returns, so a
/// refusal leaves the store as it found it.
///
/// The mirror of [`release_blocks`](super::delete_path::release_blocks), and
/// deliberately built the same way: one blocking closure per block, holding
/// that block's stripe across the read-modify-write, never an await inside
/// it (ADR 0006 hard rules 1 to 4).
async fn acquire_references(fs: &CasFS, blocks: &[BlockId]) -> Result<bool, MetaError> {
    let mut taken: Vec<BlockId> = Vec::with_capacity(blocks.len());

    for block_id in blocks {
        let stripe_guard = fs.shared.stripes().for_hash(block_id).lock_owned().await;
        let shared = fs.shared.clone();
        let id = *block_id;

        let joined = tokio::task::spawn_blocking(move || {
            let _stripe = stripe_guard;
            let mut tx = shared.meta_store().begin_transaction();
            match tx.bump_block_rc(id) {
                Ok(Some(_)) => tx.commit().map(|()| true),
                Ok(None) => {
                    tx.rollback();
                    Ok(false)
                }
                Err(e) => {
                    tx.rollback();
                    Err(e)
                }
            }
        })
        .await;

        let bumped = match joined {
            Ok(result) => result?,
            Err(join_err) => {
                // The closure did not run to completion, so this block's
                // reference is not accounted for. Give back what is held and
                // refuse, rather than write a record on a guess.
                release_blocks(&fs.shared, &fs.metrics, &taken).await;
                return Err(MetaError::OtherDBError(format!(
                    "block reference task did not complete: {join_err}"
                )));
            }
        };

        if !bumped {
            tracing::debug!(
                block_hash = %id.to_hex(),
                "The block a clone was taking a reference to is gone; the caller stores the bytes"
            );
            release_blocks(&fs.shared, &fs.metrics, &taken).await;
            return Ok(false);
        }
        taken.push(id);
    }

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::byte_stream::AsyncByteStream;
    use crate::cas::fs::BLOCK_SIZE;
    use crate::metastore::{ContentHash, Durability, MAX_BLOCKID_SIZE};
    use crate::metrics::SharedMetrics;
    use crate::store_options::StoreOptions;
    use bytes::Bytes;
    use tempfile::{TempDir, tempdir};

    const SOURCE: &str = "source-ns";
    const DEST: &str = "dest-ns";

    /// One store with the two buckets a clone runs between -- respcas's
    /// shape, where a namespace is a bucket of one metadata store.
    fn test_fs() -> (CasFS, TempDir) {
        let dir = tempdir().unwrap();
        let fs = CasFS::single_namespace(
            dir.path().to_path_buf(),
            dir.path().join("meta"),
            SharedMetrics::default(),
            StoreOptions {
                inline_metadata_size: Some(1),
                durability: Durability::Buffer,
                ..StoreOptions::default()
            },
        )
        .unwrap();
        fs.create_bucket(SOURCE).unwrap();
        fs.create_bucket(DEST).unwrap();
        (fs, dir)
    }

    async fn put(fs: &CasFS, bucket: &str, key: &[u8], data: Vec<u8>) -> Object {
        let len = data.len();
        let stream =
            AsyncByteStream::new(futures::stream::once(async move { Ok(Bytes::from(data)) }));
        fs.store_single_object_and_meta(bucket, key, stream, len)
            .await
            .unwrap()
    }

    fn rc_of(fs: &CasFS, id: &BlockId) -> Option<usize> {
        fs.shared
            .block_tree()
            .get_block(id.as_slice())
            .unwrap()
            .map(|block| block.rc())
    }

    /// Every block file under the store's root, so a test can say "no bytes
    /// moved" rather than assume it.
    fn block_files(fs: &CasFS) -> usize {
        fn walk(dir: &std::path::Path, count: &mut usize) {
            for entry in std::fs::read_dir(dir).unwrap().flatten() {
                let path = entry.path();
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if path.is_dir() {
                    // Skip the store's own database and its temp dir.
                    if !name.starts_with('.') {
                        walk(&path, count);
                    }
                } else if !name.starts_with('.') {
                    *count += 1;
                }
            }
        }
        let mut count = 0;
        walk(fs.fs_root(), &mut count);
        count
    }

    /// Content big enough to be several distinct blocks.
    fn multi_block(tag: u8) -> Vec<u8> {
        let mut data = Vec::with_capacity(3 * BLOCK_SIZE);
        for block in 0..3u8 {
            data.extend(std::iter::repeat_n(tag ^ block, BLOCK_SIZE));
        }
        data
    }

    /// The clone names the same blocks, counts one reference per occurrence,
    /// and writes no file.
    #[tokio::test]
    async fn a_clone_takes_a_reference_per_occurrence_and_moves_no_bytes() {
        let (fs, _dir) = test_fs();
        let source = put(&fs, SOURCE, b"k", multi_block(0x10)).await;
        let files_before = block_files(&fs);
        for id in source.blocks() {
            assert_eq!(rc_of(&fs, id), Some(1));
        }

        let cloned = clone_object_by_reference(&fs, SOURCE, b"k", DEST, b"k")
            .await
            .unwrap()
            .expect("the source is there, so the clone lands");

        assert_eq!(cloned.blocks(), source.blocks(), "same blocks, in order");
        assert_eq!(cloned.size(), source.size());
        assert_eq!(cloned.hash(), source.hash());
        for id in source.blocks() {
            assert_eq!(rc_of(&fs, id), Some(2), "one reference per holder");
        }
        assert_eq!(
            block_files(&fs),
            files_before,
            "a clone by reference writes no block file"
        );

        // And the destination reads back as a whole object.
        let (_, paths) = fs
            .get_object_paths(DEST, b"k")
            .unwrap()
            .expect("the destination record resolves");
        assert_eq!(paths.len(), source.blocks().len());
        for (path, _) in &paths {
            assert!(path.exists(), "every block the clone names is on disk");
        }
    }

    /// An inline record holds bytes rather than references: the clone copies
    /// the bytes that were verified when they were written, and takes no
    /// reference because there is none to take.
    #[tokio::test]
    async fn a_clone_of_an_inline_record_copies_its_verified_bytes() {
        let (fs, _dir) = test_fs();
        let payload = b"small enough to live in its own record".to_vec();
        fs.store_inlined_object(SOURCE, b"k", payload.clone())
            .await
            .unwrap();

        clone_object_by_reference(&fs, SOURCE, b"k", DEST, b"k")
            .await
            .unwrap()
            .expect("an inline source clones");

        let cloned = fs.get_object_meta(DEST, b"k").unwrap().unwrap();
        assert_eq!(cloned.inlined(), Some(&payload));
        assert!(
            fs.shared.block_tree().is_empty().unwrap(),
            "an inline record references no block"
        );
    }

    /// The property the whole design is for: a DELETE in one namespace
    /// leaves the other namespace's copy readable, because each holds its
    /// own reference.
    #[tokio::test]
    async fn deleting_the_source_leaves_the_clone_readable() {
        let (fs, _dir) = test_fs();
        let source = put(&fs, SOURCE, b"k", multi_block(0x20)).await;
        clone_object_by_reference(&fs, SOURCE, b"k", DEST, b"k")
            .await
            .unwrap()
            .unwrap();

        fs.delete_object(SOURCE, b"k").await.unwrap();

        assert!(fs.get_object_meta(SOURCE, b"k").unwrap().is_none());
        let (_, paths) = fs
            .get_object_paths(DEST, b"k")
            .unwrap()
            .expect("the clone survives its source");
        for (path, _) in &paths {
            assert!(path.exists(), "the blocks are still there for the clone");
        }
        for id in source.blocks() {
            assert_eq!(rc_of(&fs, id), Some(1), "one holder left, counted once");
        }
    }

    /// A source naming a block that is not in the store cannot be cloned:
    /// the call refuses, writes no record, and gives back every reference it
    /// had already taken.
    #[tokio::test]
    async fn a_clone_refuses_when_a_block_is_gone_and_keeps_nothing() {
        let (fs, _dir) = test_fs();
        let real = put(&fs, SOURCE, b"real", multi_block(0x30)).await;
        let live = real.blocks()[0];
        let missing = BlockId::from([0xEEu8; MAX_BLOCKID_SIZE]);

        // A record naming a live block first and a dead one second: the
        // clone gets one reference in before it meets the refusal.
        fs.create_object_meta(
            SOURCE,
            b"torn",
            2048,
            ContentHash::from([7u8; 16]),
            ObjectData::SinglePart {
                blocks: vec![live, missing],
            },
        )
        .await
        .unwrap();
        let live_rc = rc_of(&fs, &live).expect("the live block has a record");

        let outcome = clone_object_by_reference(&fs, SOURCE, b"torn", DEST, b"torn")
            .await
            .unwrap();

        assert!(outcome.is_none(), "the caller is told to store the bytes");
        assert!(
            fs.get_object_meta(DEST, b"torn").unwrap().is_none(),
            "a refused clone writes no record"
        );
        assert_eq!(
            rc_of(&fs, &live),
            Some(live_rc),
            "the reference taken before the refusal was given back"
        );
    }

    /// A source that is not there at all is the same answer, reached one
    /// step earlier.
    #[tokio::test]
    async fn a_clone_of_a_missing_source_refuses() {
        let (fs, _dir) = test_fs();
        assert!(
            clone_object_by_reference(&fs, SOURCE, b"nothing", DEST, b"k")
                .await
                .unwrap()
                .is_none()
        );
        assert!(fs.get_object_meta(DEST, b"k").unwrap().is_none());
    }

    /// A clone onto an occupied key displaces what was there and releases
    /// it, exactly as an overwrite does (ADR 0008) -- so cloning the same
    /// content twice leaves rc at two, not three.
    #[tokio::test]
    async fn a_clone_over_an_existing_record_releases_what_it_displaced() {
        let (fs, _dir) = test_fs();
        let source = put(&fs, SOURCE, b"k", multi_block(0x40)).await;

        for _ in 0..2 {
            clone_object_by_reference(&fs, SOURCE, b"k", DEST, b"k")
                .await
                .unwrap()
                .expect("both clones land");
        }

        for id in source.blocks() {
            assert_eq!(rc_of(&fs, id), Some(2), "two holders, whatever the retries");
        }
    }
}
