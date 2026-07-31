//! The multipart upload lifecycle above the store (ADR 0003): how an upload
//! addresses its record in `_UPLOADS`, and the three operations every caller
//! shares -- the S3 handlers, and the stale-upload GC, which is just another
//! client of the same claim.
//!
//! The record itself is a metastore record ([`UploadRecord`]), because the
//! claim decodes it inside a transaction.

use crate::metastore::{MetaError, UploadRecord, codec::put_len};

use super::delete_path::release_blocks;
use super::fs::CasFS;
use super::multipart::{MultiPart, part_key, part_prefix};

/// Storage key of one multipart upload record in `_UPLOADS`:
///
/// ```text
/// bucket_len u64 | bucket | key_len u64 | key | upload_id
/// ```
///
/// Length-prefixed rather than joined with a separator, because bucket names
/// and keys may contain whichever separator one picks: the prefixes make the
/// triple unambiguous, so two different uploads can never collide on one key.
/// The trailing upload_id needs no length of its own -- nothing parses these
/// bytes back. Point reads rebuild the key they wrote, and scans decode
/// record VALUES (ADR 0003), which is also why on-disk key order does not
/// matter here: listings sort in memory.
pub(crate) fn upload_key(bucket: &str, key: &str, upload_id: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + bucket.len() + 8 + key.len() + upload_id.len());
    put_len(&mut out, bucket.len());
    out.extend_from_slice(bucket.as_bytes());
    put_len(&mut out, key.len());
    out.extend_from_slice(key.as_bytes());
    out.extend_from_slice(upload_id.as_bytes());
    out
}

/// Records a new in-flight upload, stamped with the current time.
///
/// Writing this record is what brings the upload into existence: until it
/// lands, `upload_part` refuses the id, and once it is gone the upload is
/// over.
pub(super) fn create_upload(
    fs: &CasFS,
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> Result<(), MetaError> {
    let record = UploadRecord::new(bucket.to_string(), key.to_string(), upload_id.to_string());
    fs.shared
        .uploads_tree()
        .insert(&upload_key(bucket, key, upload_id), record.to_vec())
}

/// Reads the upload record without claiming it: the existence check
/// `upload_part` makes at entry.
///
/// Deliberately NOT atomic against a concurrent complete or abort. A part
/// that passes this check and lands after another caller claimed the record
/// becomes an orphan part -- refcounted blocks held by a part record whose
/// upload is gone -- which the GC reaps within one sweep (ADR 0003: leakage
/// bounded by the interval, loss impossible). Anything that must exclude the
/// other lifecycle operations takes [`claim_upload`] instead.
pub(super) fn get_upload(
    fs: &CasFS,
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> Result<Option<UploadRecord>, MetaError> {
    let Some(raw) = fs
        .shared
        .uploads_tree()
        .get(&upload_key(bucket, key, upload_id))?
    else {
        return Ok(None);
    };
    Ok(Some(UploadRecord::try_from(&*raw)?))
}

/// Claims the upload: the atomic read+remove that decides complete versus
/// abort (ADR 0003).
///
/// `Some` means the caller owns the upload and may proceed; `None` means
/// somebody else already claimed it -- or it never existed -- and the caller
/// answers `NoSuchUpload`. The transaction runs on the SHARED store, where
/// `_UPLOADS` lives, not on the namespace.
pub(super) fn claim_upload(
    fs: &CasFS,
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> Result<Option<UploadRecord>, MetaError> {
    let mut tx = fs.shared.meta_store().begin_transaction();
    match tx.take_upload(&upload_key(bucket, key, upload_id)) {
        Ok(record) => {
            tx.commit()?;
            Ok(record)
        }
        Err(e) => {
            tx.rollback();
            Err(e)
        }
    }
}

/// Every upload record in the store, decoded.
///
/// Value-driven (hard rule 4): the scan decodes VALUES and never parses a
/// key, so the triple it reports is the one the record carries. Filtering by
/// bucket, prefix and markers, and the S3 ordering, are the caller's -- they
/// happen in memory, which is what buys the freedom to key `_UPLOADS` for
/// point reads instead of for collation (ADR 0003). The set is bounded by the
/// GC's TTL.
///
/// A store or decode error ends the listing rather than silently shortening
/// it: an upload that exists but cannot be read is not the same answer as no
/// upload.
pub(super) fn list_uploads(fs: &CasFS) -> Result<Vec<UploadRecord>, MetaError> {
    fs.shared
        .uploads_tree()
        .iter_all()
        .map(|item| {
            let (_, raw) = item?;
            UploadRecord::try_from(&*raw).map_err(MetaError::from)
        })
        .collect()
}

/// Every part record of one upload, ascending by part number.
///
/// The read side of the same prefix scan abort uses; `ListParts` sorts and
/// paginates the result in memory.
pub(super) fn upload_parts(
    fs: &CasFS,
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> Result<Vec<MultiPart>, MetaError> {
    let tree = fs.shared.multipart_tree();
    tree.parts_of(&part_prefix(bucket, key, upload_id))
        .collect()
}

/// Drops one part record and the block references it held.
///
/// # Record first, blocks second
///
/// The removal happens BEFORE the release, and a failed removal releases
/// nothing (hard rule 2, ADR 0003 decision 6). A crash -- or an error --
/// between the two leaves a block whose rc exceeds its walked holders: an
/// over-count, which fsck reports INFO and the next recount collects. The
/// reverse order leaves a part record still claiming references that were
/// already dropped, which fsck must read as a loss-shaped under-count and
/// `--repair` would "fix" by raising the rc back, leaking the blocks
/// permanently. Wrong in the recoverable direction only.
///
/// `storage_key` is passed in rather than rebuilt from the record, because the
/// GC's orphan sweep reaps records whose key is not reconstructible at all --
/// the legacy dash-joined ones (ADR 0003: the first sweep IS the migration).
/// Rebuilding the key there would remove nothing and then release blocks the
/// surviving record still claims: precisely the forbidden order.
///
/// Returns whether the record was removed (and hence its blocks released).
pub(super) async fn reap_part(fs: &CasFS, storage_key: &[u8], part: &MultiPart) -> bool {
    if let Err(e) = fs.shared.multipart_tree().remove(storage_key) {
        tracing::error!(
            bucket = %part.bucket(),
            key = %part.key(),
            upload_id = %part.upload_id(),
            part_number = part.part_number(),
            error = %e,
            "Could not remove part record; its blocks stay referenced"
        );
        return false;
    }
    release_blocks(&fs.shared, &fs.metrics, part.blocks()).await;
    true
}

/// Aborts an upload: claim it, then reap every part it holds.
///
/// `Some(parts reaped)` means this caller won the claim; `None` means it lost
/// -- somebody else completed or aborted the upload first, or it never existed
/// -- and the caller answers `NoSuchUpload`. That is the whole of the
/// complete-versus-abort rule (ADR 0003 decision 2): the claim is the only
/// linearization point, and both sides of every race read it the same way.
///
/// The client handler and the stale-upload GC call this same function; the GC
/// is just another client of the claim.
///
/// # A crash after the claim
///
/// The upload record is gone before the first part is, so a crash mid-reap
/// leaves part records nothing can name an upload for: orphan parts. They hold
/// their blocks until the GC's orphan sweep removes them, so the residue is
/// bounded leakage, never loss.
pub(super) async fn abort_upload(
    fs: &CasFS,
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> Result<Option<usize>, MetaError> {
    if claim_upload(fs, bucket, key, upload_id)?.is_none() {
        return Ok(None);
    }

    // Collected before the first removal: the scan and the removals hit the
    // same tree, and the reaping awaits (release_blocks blocks on the stripe),
    // so this keeps no store iterator open across them. Parts per upload are
    // capped at 10k by S3.
    let tree = fs.shared.multipart_tree();
    let parts: Vec<MultiPart> = tree
        .parts_of(&part_prefix(bucket, key, upload_id))
        .filter_map(|part| match part {
            Ok(part) => Some(part),
            // One unreadable record must not strand every other part's
            // blocks: skip it and carry on. What is left behind is an orphan
            // part for the GC, the same residue a crash here would leave.
            Err(e) => {
                tracing::error!(
                    bucket = %bucket,
                    key = %key,
                    upload_id = %upload_id,
                    error = %e,
                    "Could not read a part record while aborting; skipping it"
                );
                None
            }
        })
        .collect();

    let mut reaped = 0;
    for part in &parts {
        // Rebuilt from the arguments the scan matched on, so this is the key
        // that record was written under -- `insert_multipart_part` builds it
        // the same way.
        let storage_key = part_key(bucket, key, upload_id, part.part_number());
        if reap_part(fs, &storage_key, part).await {
            reaped += 1;
        }
    }

    tracing::debug!(
        bucket = %bucket,
        key = %key,
        upload_id = %upload_id,
        parts = parts.len(),
        reaped = reaped,
        "CasFS: abort_upload"
    );
    Ok(Some(reaped))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::{AsyncByteStream, CasFS, StorageEngine};
    use crate::metastore::{BlockId, Durability};
    use crate::metrics::SharedMetrics;
    use tempfile::{TempDir, tempdir};

    const BUCKET: &str = "test-bucket";
    const KEY: &str = "test/key";

    fn test_fs() -> (CasFS, TempDir) {
        let dir = tempdir().unwrap();
        let fs = CasFS::single_namespace(
            dir.path().to_path_buf(),
            dir.path().join("meta"),
            SharedMetrics::default(),
            StorageEngine::Fjall,
            Some(1),
            Some(Durability::Buffer),
            None,
            false,
        )
        .unwrap();
        (fs, dir)
    }

    /// The ambiguity the dash-joined keys had: with a separator, a bucket or
    /// key containing it makes two different uploads share one key. Length
    /// prefixes make every triple its own key.
    #[test]
    fn upload_keys_are_unambiguous() {
        let split_one = upload_key("a-b", "c", "u");
        let split_other = upload_key("a", "b-c", "u");
        assert_ne!(split_one, split_other);

        // The same triple always encodes to the same bytes -- point reads
        // depend on rebuilding exactly what was written.
        assert_eq!(upload_key(BUCKET, KEY, "u1"), upload_key(BUCKET, KEY, "u1"));
        assert_ne!(upload_key(BUCKET, KEY, "u1"), upload_key(BUCKET, KEY, "u2"));
    }

    /// Create, read back, claim: the record carries the triple it was made
    /// for, and the claim both returns it and takes it away.
    #[test]
    fn create_get_claim_round_trip() {
        let (fs, _dir) = test_fs();
        let before = chrono::Utc::now().timestamp();
        fs.create_upload(BUCKET, KEY, "u1").unwrap();
        let after = chrono::Utc::now().timestamp();

        let record = fs.get_upload(BUCKET, KEY, "u1").unwrap().expect("created");
        assert_eq!(record.bucket(), BUCKET);
        assert_eq!(record.key(), KEY);
        assert_eq!(record.upload_id(), "u1");
        assert!(
            (before..=after).contains(&record.created_at()),
            "created_at {} outside [{before}, {after}]",
            record.created_at()
        );

        let claimed = fs.claim_upload(BUCKET, KEY, "u1").unwrap().expect("claim");
        assert_eq!(claimed.upload_id(), "u1");
        assert!(
            fs.get_upload(BUCKET, KEY, "u1").unwrap().is_none(),
            "the claim removes the record"
        );
    }

    /// The rule complete and abort rest on: two claims of one upload, exactly
    /// one winner. Sequential transactions are the whole test -- the claim is
    /// atomic within one, so if the second could still see the record no
    /// amount of concurrency would help.
    #[test]
    fn claiming_twice_yields_exactly_one_winner() {
        let (fs, _dir) = test_fs();
        fs.create_upload(BUCKET, KEY, "u1").unwrap();

        let first = fs.claim_upload(BUCKET, KEY, "u1").unwrap();
        let second = fs.claim_upload(BUCKET, KEY, "u1").unwrap();

        assert!(first.is_some(), "the first claim wins");
        assert!(second.is_none(), "the loser sees no record: NoSuchUpload");
    }

    /// Claiming an id that was never created is the loser's case too, not an
    /// error: abort of an unknown upload answers NoSuchUpload from this None.
    #[test]
    fn claiming_an_unknown_upload_is_none() {
        let (fs, _dir) = test_fs();
        assert!(fs.get_upload(BUCKET, KEY, "never").unwrap().is_none());
        assert!(fs.claim_upload(BUCKET, KEY, "never").unwrap().is_none());
    }

    /// S3 allows any number of concurrent uploads of one key. They are
    /// separate records, and claiming one leaves the others alone.
    #[test]
    fn two_uploads_of_one_key_coexist() {
        let (fs, _dir) = test_fs();
        fs.create_upload(BUCKET, KEY, "u1").unwrap();
        fs.create_upload(BUCKET, KEY, "u2").unwrap();

        assert!(fs.claim_upload(BUCKET, KEY, "u1").unwrap().is_some());

        let survivor = fs
            .get_upload(BUCKET, KEY, "u2")
            .unwrap()
            .expect("untouched");
        assert_eq!(survivor.upload_id(), "u2");
        assert!(fs.claim_upload(BUCKET, KEY, "u2").unwrap().is_some());
    }

    /// Stores one part the way `UploadPart` does -- blocks first (which bumps
    /// their refcounts), then the part record -- and returns its block ids.
    async fn put_part(
        fs: &CasFS,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i64,
        data: Vec<u8>,
    ) -> Vec<BlockId> {
        let stream = AsyncByteStream::new(futures::stream::once(async move {
            Ok(bytes::Bytes::from(data))
        }));
        let (blocks, hash, size) = fs.store_object(bucket, key, stream).await.unwrap();
        fs.insert_multipart_part(
            bucket.to_string(),
            key.to_string(),
            size as usize,
            part_number,
            upload_id.to_string(),
            hash,
            blocks.clone(),
        )
        .unwrap();
        blocks
    }

    /// Payload big enough to be a real block, unique per `tag` so no two
    /// parts of a test dedup onto each other.
    fn part_data(tag: u8) -> Vec<u8> {
        std::iter::repeat_n(tag, 4096).collect()
    }

    /// The refcount of one block, or `None` once its record is gone.
    fn rc_of(fs: &CasFS, id: &BlockId) -> Option<usize> {
        fs.shared_block_store()
            .block_tree()
            .get_block(id.as_slice())
            .unwrap()
            .map(|block| block.rc())
    }

    /// Whether the block's file is on disk, following the record's own depth.
    fn block_file_exists(fs: &CasFS, id: &BlockId) -> bool {
        fs.shared_block_store()
            .block_tree()
            .get_block(id.as_slice())
            .unwrap()
            .is_some_and(|block| block.disk_path(id, fs.fs_root().clone()).is_file())
    }

    /// Abort reaps every part of the upload it claimed: records gone, the
    /// references they held dropped, and the block files with them.
    #[tokio::test]
    async fn abort_reaps_parts_records_and_blocks() {
        let (fs, _dir) = test_fs();
        fs.create_bucket(BUCKET).unwrap();
        fs.create_upload(BUCKET, KEY, "u1").unwrap();

        let first = put_part(&fs, BUCKET, KEY, "u1", 1, part_data(0xa1)).await;
        let second = put_part(&fs, BUCKET, KEY, "u1", 2, part_data(0xa2)).await;
        let held: Vec<BlockId> = first.iter().chain(second.iter()).copied().collect();
        for id in &held {
            assert_eq!(rc_of(&fs, id), Some(1), "the part holds its block");
            assert!(block_file_exists(&fs, id));
        }

        assert_eq!(fs.abort_upload(BUCKET, KEY, "u1").await.unwrap(), Some(2));

        assert!(fs.get_upload(BUCKET, KEY, "u1").unwrap().is_none());
        assert!(fs.upload_parts(BUCKET, KEY, "u1").unwrap().is_empty());
        for id in &held {
            assert_eq!(rc_of(&fs, id), None, "the last reference is gone");
            assert!(!block_file_exists(&fs, id), "and so is the file");
        }
    }

    /// Double abort, and complete-versus-abort, are one rule: whoever claims
    /// the record first wins, and the loser is told `NoSuchUpload`.
    #[tokio::test]
    async fn aborting_twice_has_exactly_one_winner() {
        let (fs, _dir) = test_fs();
        fs.create_bucket(BUCKET).unwrap();
        fs.create_upload(BUCKET, KEY, "u1").unwrap();
        put_part(&fs, BUCKET, KEY, "u1", 1, part_data(0xb1)).await;

        assert_eq!(fs.abort_upload(BUCKET, KEY, "u1").await.unwrap(), Some(1));
        assert_eq!(
            fs.abort_upload(BUCKET, KEY, "u1").await.unwrap(),
            None,
            "the second abort lost the claim"
        );
        assert_eq!(
            fs.abort_upload(BUCKET, KEY, "never").await.unwrap(),
            None,
            "and so does an id that never existed"
        );
    }

    /// The prefix scan is exact: aborting one upload of a key leaves the
    /// other upload of that same key, parts and references intact.
    #[tokio::test]
    async fn abort_leaves_a_sibling_upload_alone() {
        let (fs, _dir) = test_fs();
        fs.create_bucket(BUCKET).unwrap();
        fs.create_upload(BUCKET, KEY, "u1").unwrap();
        fs.create_upload(BUCKET, KEY, "u2").unwrap();

        let doomed = put_part(&fs, BUCKET, KEY, "u1", 1, part_data(0xc1)).await;
        let survivor = put_part(&fs, BUCKET, KEY, "u2", 1, part_data(0xc2)).await;

        assert_eq!(fs.abort_upload(BUCKET, KEY, "u1").await.unwrap(), Some(1));

        for id in &doomed {
            assert_eq!(rc_of(&fs, id), None);
        }
        assert_eq!(fs.upload_parts(BUCKET, KEY, "u2").unwrap().len(), 1);
        for id in &survivor {
            assert_eq!(rc_of(&fs, id), Some(1), "u2 still holds its block");
            assert!(block_file_exists(&fs, id));
        }
        assert!(fs.get_upload(BUCKET, KEY, "u2").unwrap().is_some());
    }

    /// A block a live object also references survives the abort: the release
    /// drops the part's OCCURRENCE, not the block.
    #[tokio::test]
    async fn abort_only_drops_its_own_occurrence() {
        let (fs, _dir) = test_fs();
        fs.create_bucket(BUCKET).unwrap();
        fs.create_upload(BUCKET, KEY, "u1").unwrap();

        // The object and the part carry identical content, so they dedup onto
        // one block record with two references.
        let data = part_data(0xd1);
        let len = data.len();
        let object_data = data.clone();
        let stream = AsyncByteStream::new(futures::stream::once(async move {
            Ok(bytes::Bytes::from(object_data))
        }));
        let object = fs
            .store_single_object_and_meta(BUCKET, "live", stream, len)
            .await
            .unwrap();
        let shared_blocks = put_part(&fs, BUCKET, KEY, "u1", 1, data).await;
        assert_eq!(object.blocks(), shared_blocks.as_slice());
        for id in &shared_blocks {
            assert_eq!(rc_of(&fs, id), Some(2));
        }

        assert_eq!(fs.abort_upload(BUCKET, KEY, "u1").await.unwrap(), Some(1));

        for id in &shared_blocks {
            assert_eq!(rc_of(&fs, id), Some(1), "the object's reference remains");
            assert!(block_file_exists(&fs, id), "and its bytes with it");
        }
    }

    /// The race window ADR 0003 accepts: a part that passed its existence
    /// check before the claim lands after the abort has finished. Nothing is
    /// lost -- the part record holds its blocks, alone -- and nothing owns
    /// the upload any more, which is exactly the orphan the GC reaps.
    #[tokio::test]
    async fn a_part_landing_after_the_abort_is_an_orphan() {
        let (fs, _dir) = test_fs();
        fs.create_bucket(BUCKET).unwrap();
        fs.create_upload(BUCKET, KEY, "u1").unwrap();

        // The in-flight part's existence check passes here...
        assert!(fs.get_upload(BUCKET, KEY, "u1").unwrap().is_some());
        assert_eq!(fs.abort_upload(BUCKET, KEY, "u1").await.unwrap(), Some(0));
        // ...and its blocks and record land only now.
        let late = put_part(&fs, BUCKET, KEY, "u1", 7, part_data(0xe1)).await;

        assert!(
            fs.get_upload(BUCKET, KEY, "u1").unwrap().is_none(),
            "no upload record owns the late part"
        );
        let parts = fs.upload_parts(BUCKET, KEY, "u1").unwrap();
        assert_eq!(parts.len(), 1, "the part record survives as an orphan");
        assert_eq!(parts[0].part_number(), 7);
        for id in &late {
            assert_eq!(
                rc_of(&fs, id),
                Some(1),
                "held by the orphan part record and nothing else"
            );
            assert!(block_file_exists(&fs, id), "leakage, never loss");
        }
    }

    /// The listing surface `ListMultipartUploads` reads: every record in the
    /// store, decoded from its VALUE, with claims removing them as they go.
    #[test]
    fn list_uploads_reports_every_record() {
        let (fs, _dir) = test_fs();
        fs.create_upload(BUCKET, KEY, "u1").unwrap();
        fs.create_upload(BUCKET, "other/key", "u2").unwrap();
        fs.create_upload("other-bucket", KEY, "u3").unwrap();

        let mut listed: Vec<(String, String, String)> = fs
            .list_uploads()
            .unwrap()
            .iter()
            .map(|u| {
                (
                    u.bucket().to_string(),
                    u.key().to_string(),
                    u.upload_id().to_string(),
                )
            })
            .collect();
        listed.sort();
        assert_eq!(
            listed,
            [
                (
                    "other-bucket".to_string(),
                    KEY.to_string(),
                    "u3".to_string()
                ),
                (
                    BUCKET.to_string(),
                    "other/key".to_string(),
                    "u2".to_string()
                ),
                (BUCKET.to_string(), KEY.to_string(), "u1".to_string()),
            ]
        );

        fs.claim_upload(BUCKET, KEY, "u1").unwrap().unwrap();
        assert_eq!(fs.list_uploads().unwrap().len(), 2);
    }
}
