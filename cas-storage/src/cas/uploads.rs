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

/// What [`claim_upload_with_parts`] found.
///
/// Three outcomes because complete has three answers, and the middle one is
/// not an error: losing the claim is the ordinary end of a race.
#[derive(Debug)]
pub enum UploadClaim {
    /// The upload record and every named part were taken together. The caller
    /// now owns their blocks and must account for them -- by minting the
    /// object that inherits them, or by releasing them.
    Claimed {
        /// The upload record this claim removed.
        record: UploadRecord,
        /// The named parts, in the order they were asked for, as the claiming
        /// transaction read them.
        parts: Vec<MultiPart>,
    },
    /// No upload record: somebody else completed or aborted it, or it never
    /// existed. `NoSuchUpload`. Nothing was removed.
    NoUpload,
    /// A named part record was not there. Nothing was removed -- the upload
    /// survives and the client can retry a corrected complete.
    MissingPart(i64),
}

/// Complete's claim: takes the upload record AND every part it names, in one
/// transaction (ADR 0003 amendment).
///
/// # Why the parts are in the claim
///
/// `complete` does not release the parts' blocks -- it hands them to the
/// object it mints, which inherits the references the part records held. So
/// between "the upload record is gone" and "the part records are gone" those
/// blocks have two claimants on paper and one in truth, and anything that
/// reaps orphan parts in that window (the GC's phase 2, by construction)
/// would release references the new object owns. That is loss, and no amount
/// of narrowing the window makes it not loss.
///
/// `_UPLOADS` and `_MULTIPART_PARTS` live in the same shared database, so one
/// transaction spans both and the window does not exist: a named part record
/// disappears at the same instant as its upload record. Nothing can observe
/// an inheritable part as an orphan.
///
/// # What the caller must know
///
/// - The parts returned are the ones the CLAIMING transaction read, not
///   whatever an earlier validation pass saw. They are the authoritative
///   values, and the object must be built from them.
/// - Every failure rolls back whole: a claim that does not return
///   [`UploadClaim::Claimed`] has removed nothing, so a rejected complete
///   leaves the upload intact and retryable.
/// - A crash after this commits and before the object record exists leaves
///   blocks with rc but no holder: over-counts, which the recount collects.
///   Leakage, never loss -- the same trade every other step here makes.
/// - Parts the client did NOT name survive the claim. Their references were
///   never inherited, so they are true orphans and the GC reaps them.
///
/// Synchronous on purpose: the transaction holds the backend's single-writer
/// guard, which must not be held across an `.await`.
pub(super) fn claim_upload_with_parts(
    fs: &CasFS,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_numbers: &[i64],
) -> Result<UploadClaim, MetaError> {
    let mut tx = fs.shared.meta_store().begin_transaction();

    // The upload record first: it is the linearization point, and taking it
    // is what makes this caller the one that completes.
    let record = match tx.take_upload(&upload_key(bucket, key, upload_id)) {
        Ok(Some(record)) => record,
        Ok(None) => {
            tx.rollback();
            return Ok(UploadClaim::NoUpload);
        }
        Err(e) => {
            tx.rollback();
            return Err(e);
        }
    };

    // Then every named part. S3 caps an upload at 10k parts, so this is a
    // bounded number of point operations in one transaction -- the same reads
    // and removes complete always did, now as one atomic step.
    let mut parts = Vec::with_capacity(part_numbers.len());
    for &part_number in part_numbers {
        let raw = match tx.take_part(&part_key(bucket, key, upload_id, part_number)) {
            Ok(Some(raw)) => raw,
            Ok(None) => {
                tx.rollback();
                return Ok(UploadClaim::MissingPart(part_number));
            }
            Err(e) => {
                tx.rollback();
                return Err(e);
            }
        };
        match MultiPart::try_from(&*raw) {
            Ok(part) => parts.push(part),
            Err(e) => {
                tx.rollback();
                return Err(MetaError::from(e));
            }
        }
    }

    tx.commit()?;
    Ok(UploadClaim::Claimed { record, parts })
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

/// Claims one part record and drops the block references it held.
///
/// # The removal IS the claim
///
/// The record is read AND removed in one transaction ([`Transaction::take_part`]),
/// and the blocks released are the ones that transaction read. Two reapers
/// racing over one part -- a client's abort against a GC sweep, or two sweeps
/// -- therefore cannot both release it: exactly one take returns the record,
/// the other is told it was already gone. Without the atomic pair, both would
/// read the same blocks and both would decrement them, which is a
/// double-release: loss, not leakage.
///
/// # Record first, blocks second
///
/// The take COMMITS before the release begins, and a failed take releases
/// nothing (hard rule 2, ADR 0003 decision 6). A crash -- or an error --
/// between the two leaves a block whose rc exceeds its walked holders: an
/// over-count, which fsck reports INFO and the next recount collects. The
/// reverse order leaves a part record still claiming references that were
/// already dropped, which fsck must read as a loss-shaped under-count and
/// `--repair` would "fix" by raising the rc back, leaking the blocks
/// permanently. Wrong in the recoverable direction only.
///
/// The transaction is opened and committed without an `.await` in between:
/// the backend's transaction holds a single-writer guard, and holding one
/// across a suspension point is what `FjallTransaction`'s safety note
/// forbids. The release, which does await, happens after the commit.
///
/// `storage_key` is passed in rather than rebuilt from the record, because the
/// GC's orphan sweep reaps records whose key is not reconstructible at all --
/// the legacy dash-joined ones (ADR 0003: the first sweep IS the migration).
/// Rebuilding the key there would remove nothing and then release blocks the
/// surviving record still claims: precisely the forbidden order.
///
/// `Ok(Some(part))` means this caller won the take and released its blocks;
/// `Ok(None)` that another reaper got there first and there was nothing left
/// to do.
pub(super) async fn reap_part(
    fs: &CasFS,
    storage_key: &[u8],
) -> Result<Option<MultiPart>, MetaError> {
    // Scoped so the transaction -- and its writer guard -- is gone before the
    // release below awaits.
    let part = {
        let mut tx = fs.shared.meta_store().begin_transaction();
        let raw = match tx.take_part(storage_key) {
            Ok(Some(raw)) => raw,
            Ok(None) => {
                tx.rollback();
                return Ok(None);
            }
            Err(e) => {
                tx.rollback();
                return Err(e);
            }
        };
        // An undecodable record names no blocks: rolling back leaves it in
        // place rather than stranding whatever it referenced. fsck's problem.
        let part = match MultiPart::try_from(&*raw) {
            Ok(part) => part,
            Err(e) => {
                tx.rollback();
                return Err(MetaError::from(e));
            }
        };
        tx.commit()?;
        part
    };

    release_blocks(&fs.shared, &fs.metrics, part.blocks()).await;
    Ok(Some(part))
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
        match reap_part(fs, &storage_key).await {
            Ok(Some(_)) => reaped += 1,
            // Another reaper took this part between the scan and here (a GC
            // sweep, since this caller holds the upload claim). It released
            // the blocks; there is nothing left for this loop to do.
            Ok(None) => {}
            // One unreapable part must not strand the rest: its blocks stay
            // referenced by a record that survives, which is the over-count
            // direction, and the next sweep retries.
            Err(e) => tracing::error!(
                bucket = %bucket,
                key = %key,
                upload_id = %upload_id,
                part_number = part.part_number(),
                error = %e,
                "Could not reap a part record; its blocks stay referenced"
            ),
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
    use crate::cas::{AsyncByteStream, CasFS};
    use crate::metastore::{BlockId, Durability};
    use crate::metrics::SharedMetrics;
    use crate::store_options::StoreOptions;
    use tempfile::{TempDir, tempdir};

    const BUCKET: &str = "test-bucket";
    const KEY: &str = "test/key";

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

    /// Complete's claim takes the upload record and every NAMED part in one
    /// step, and returns the values that transaction read. A part the client
    /// did not name is left alone -- as an orphan, since its upload record is
    /// gone, which is exactly what it is: nothing inherited its blocks.
    #[tokio::test]
    async fn claiming_with_parts_takes_the_upload_and_the_named_parts() {
        let (fs, _dir) = test_fs();
        fs.create_bucket(BUCKET).unwrap();
        fs.create_upload(BUCKET, KEY, "u1").unwrap();

        put_part(&fs, BUCKET, KEY, "u1", 1, part_data(0xf1)).await;
        put_part(&fs, BUCKET, KEY, "u1", 2, part_data(0xf2)).await;
        let unnamed = put_part(&fs, BUCKET, KEY, "u1", 3, part_data(0xf3)).await;

        let claimed = fs
            .claim_upload_with_parts(BUCKET, KEY, "u1", &[1, 2])
            .unwrap();
        let UploadClaim::Claimed { record, parts } = claimed else {
            panic!("the claim must win: {claimed:?}");
        };

        assert_eq!(record.upload_id(), "u1");
        assert_eq!(
            parts.iter().map(|p| p.part_number()).collect::<Vec<_>>(),
            [1, 2],
            "the parts come back in the order they were asked for"
        );

        // The upload record and the named parts left together: there is no
        // ordering between them to observe, which is the whole point.
        assert!(fs.get_upload(BUCKET, KEY, "u1").unwrap().is_none());
        let left = fs.upload_parts(BUCKET, KEY, "u1").unwrap();
        assert_eq!(
            left.iter().map(|p| p.part_number()).collect::<Vec<_>>(),
            [3],
            "only the part nobody named survives"
        );
        for id in &unnamed {
            assert_eq!(
                rc_of(&fs, id),
                Some(1),
                "and it still holds its own blocks, alone"
            );
        }
    }

    /// Claiming an upload that is gone changes nothing and says so: the
    /// `NoSuchUpload` case, whether it was completed, aborted or never made.
    #[tokio::test]
    async fn claiming_with_parts_reports_a_missing_upload() {
        let (fs, _dir) = test_fs();
        fs.create_bucket(BUCKET).unwrap();

        // A part record with no upload record: the claim must not take it.
        let orphan = put_part(&fs, BUCKET, KEY, "never", 1, part_data(0xf4)).await;

        let claimed = fs
            .claim_upload_with_parts(BUCKET, KEY, "never", &[1])
            .unwrap();
        assert!(matches!(claimed, UploadClaim::NoUpload), "{claimed:?}");

        assert_eq!(fs.upload_parts(BUCKET, KEY, "never").unwrap().len(), 1);
        for id in &orphan {
            assert_eq!(rc_of(&fs, id), Some(1));
        }
    }

    /// A named part that is not there rolls the whole claim back: the upload
    /// record and every other part survive, so the client can retry a
    /// corrected complete. A half-claimed upload would be unrecoverable.
    #[tokio::test]
    async fn a_missing_named_part_rolls_the_whole_claim_back() {
        let (fs, _dir) = test_fs();
        fs.create_bucket(BUCKET).unwrap();
        fs.create_upload(BUCKET, KEY, "u1").unwrap();
        let held = put_part(&fs, BUCKET, KEY, "u1", 1, part_data(0xf5)).await;

        let claimed = fs
            .claim_upload_with_parts(BUCKET, KEY, "u1", &[1, 2])
            .unwrap();
        assert!(
            matches!(claimed, UploadClaim::MissingPart(2)),
            "{claimed:?}"
        );

        assert!(
            fs.get_upload(BUCKET, KEY, "u1").unwrap().is_some(),
            "the upload survives a rejected claim"
        );
        assert_eq!(
            fs.upload_parts(BUCKET, KEY, "u1").unwrap().len(),
            1,
            "and so does the part that WAS there"
        );
        for id in &held {
            assert_eq!(rc_of(&fs, id), Some(1));
        }

        // Retrying with what really exists then works.
        assert!(matches!(
            fs.claim_upload_with_parts(BUCKET, KEY, "u1", &[1]).unwrap(),
            UploadClaim::Claimed { .. }
        ));
    }

    /// The reap is a take: the record and its blocks leave together, so two
    /// reapers over one part -- a client's abort against a GC sweep -- release
    /// it exactly ONCE. A split read-then-remove lets both read the same
    /// blocks and both decrement them, which takes a block below its real
    /// holder count: loss, not leakage.
    ///
    /// The part shares its content with a live object, so the block carries
    /// two references: one release leaves rc 1 and the file on disk, a double
    /// release takes the record to zero and unlinks bytes the object still
    /// needs. That is the difference this test can see.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_reapers_release_one_part_exactly_once() {
        /// Fresh content per iteration, so every round is its own race.
        const STORM: usize = 40;

        let (fs, _dir) = test_fs();
        fs.create_bucket(BUCKET).unwrap();
        let fs = std::sync::Arc::new(fs);

        for iteration in 0..STORM {
            let data: Vec<u8> = format!("reap race {iteration} ")
                .repeat(256)
                .into_bytes()
                .into_iter()
                .collect();
            let upload_id = format!("u{iteration}");

            // The object's reference, then the part's: rc 2 on one block.
            let len = data.len();
            let object_data = data.clone();
            let stream = AsyncByteStream::new(futures::stream::once(async move {
                Ok(bytes::Bytes::from(object_data))
            }));
            let object = fs
                .store_single_object_and_meta(BUCKET, &format!("live-{iteration}"), stream, len)
                .await
                .unwrap();
            let blocks = put_part(&fs, BUCKET, KEY, &upload_id, 1, data).await;
            assert_eq!(object.blocks(), blocks.as_slice());
            for id in &blocks {
                assert_eq!(rc_of(&fs, id), Some(2), "object plus part");
            }

            let storage_key = part_key(BUCKET, KEY, &upload_id, 1);
            let reapers: Vec<_> = (0..2)
                .map(|_| {
                    let fs = fs.clone();
                    let storage_key = storage_key.clone();
                    tokio::spawn(async move { reap_part(&fs, &storage_key).await })
                })
                .collect();

            let mut winners = 0;
            for reaper in reapers {
                if reaper.await.unwrap().unwrap().is_some() {
                    winners += 1;
                }
            }
            assert_eq!(winners, 1, "the take has exactly one winner");

            for id in &blocks {
                assert_eq!(
                    rc_of(&fs, id),
                    Some(1),
                    "released exactly once: the object's reference survives"
                );
                assert!(block_file_exists(&fs, id), "and its bytes with it");
            }
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
