//! The multipart upload lifecycle above the store (ADR 0003): how an upload
//! addresses its record in `_UPLOADS`, and the three operations every caller
//! shares -- the S3 handlers, and the stale-upload GC, which is just another
//! client of the same claim.
//!
//! The record itself is a metastore record ([`UploadRecord`]), because the
//! claim decodes it inside a transaction.

use crate::metastore::{MetaError, UploadRecord, codec::put_len};

use super::fs::CasFS;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::{CasFS, StorageEngine};
    use crate::metastore::Durability;
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
}
