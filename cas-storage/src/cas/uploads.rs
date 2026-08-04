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
mod tests;
