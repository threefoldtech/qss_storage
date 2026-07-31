use chrono::Utc;

use super::{
    FsError,
    codec::{Reader, put_len},
};

/// One in-flight multipart upload, as recorded in
/// [`UPLOADS_TREE`](super::UPLOADS_TREE).
///
/// The record's existence IS the upload's existence (ADR 0003): `upload_part`
/// refuses an id that has none, and complete and abort both start by claiming
/// it with [`Transaction::take_upload`](super::Transaction::take_upload), so
/// exactly one of them can win.
///
/// `created_at` is the wall clock at creation, and it is what the stale-upload
/// GC ages against. TTLs of days make skew irrelevant and monotonicity is not
/// required; a store carried to a machine with a wildly wrong clock can expire
/// uploads early, which is accepted (ADR 0003).
///
/// The record lives here rather than beside the upload code in `cas` because
/// `take_upload` decodes it, and the metastore layer names no type from above
/// it.
#[derive(Debug)]
pub struct UploadRecord {
    /// Creation time as a Unix timestamp (seconds since epoch)
    created_at: i64,
    /// Bucket the completed object will land in
    bucket: String,
    /// Key the completed object will land under
    key: String,
    /// Upload id handed to the client
    upload_id: String,
}

impl UploadRecord {
    /// Creates a record for an upload starting now.
    pub fn new(bucket: String, key: String, upload_id: String) -> Self {
        Self::with_created_at(Utc::now().timestamp(), bucket, key, upload_id)
    }

    /// The same record with a caller-chosen creation time.
    ///
    /// Crate-internal on purpose: everything that ages uploads -- the GC's TTL
    /// sweep, fsck's age reporting -- can only be tested against records that
    /// are already old, and sleeping through a TTL is not a test. Daemon paths
    /// take [`Self::new`], which is the only clock read.
    pub(crate) fn with_created_at(
        created_at: i64,
        bucket: String,
        key: String,
        upload_id: String,
    ) -> Self {
        Self {
            created_at,
            bucket,
            key,
            upload_id,
        }
    }

    /// Creation time as a Unix timestamp (seconds since epoch).
    ///
    /// Seconds rather than a `SystemTime`, because every consumer subtracts it
    /// from now: a TTL comparison and a reported age.
    pub fn created_at(&self) -> i64 {
        self.created_at
    }

    /// Bucket the completed object will land in.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Key the completed object will land under.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Upload id this record was created for.
    pub fn upload_id(&self) -> &str {
        &self.upload_id
    }

    /// Serializes the record to a byte vector.
    pub fn to_vec(&self) -> Vec<u8> {
        self.into()
    }
}

/// Serializes UploadRecord (format v1):
///
/// ```text
/// created_at i64 | bucket_len u64 | bucket | key_len u64 | key |
/// upload_id_len u64 | upload_id
/// ```
impl From<&UploadRecord> for Vec<u8> {
    fn from(u: &UploadRecord) -> Self {
        let mut out =
            Vec::with_capacity(8 + 8 + u.bucket.len() + 8 + u.key.len() + 8 + u.upload_id.len());
        out.extend_from_slice(&u.created_at.to_le_bytes());
        put_len(&mut out, u.bucket.len());
        out.extend_from_slice(u.bucket.as_bytes());
        put_len(&mut out, u.key.len());
        out.extend_from_slice(u.key.as_bytes());
        put_len(&mut out, u.upload_id.len());
        out.extend_from_slice(u.upload_id.as_bytes());
        out
    }
}

/// Deserializes UploadRecord from the layout above, with an exact length
/// check.
///
/// The three string fields come back off disk, where the "only valid strings
/// were ever written" invariant can be broken by corruption or a truncated
/// write, so `Reader::utf8` validates them rather than assuming.
impl TryFrom<&[u8]> for UploadRecord {
    type Error = FsError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let mut r = Reader::new("UploadRecord", value);
        let created_at = r.i64("created_at")?;
        let bucket_len = r.len("bucket_len")?;
        let bucket = r.utf8("bucket", bucket_len)?;
        let key_len = r.len("key_len")?;
        let key = r.utf8("key", key_len)?;
        let upload_id_len = r.len("upload_id_len")?;
        let upload_id = r.utf8("upload_id", upload_id_len)?;
        r.finish()?;

        Ok(UploadRecord {
            created_at,
            bucket,
            key,
            upload_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Upload of key "key" in bucket "bucket" under id "u1" (format v1).
    #[rustfmt::skip]
    const GOLDEN_NAMED: &[u8] = &[
        // created_at = 0x0102030405060708
        0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
        // bucket_len = 6
        0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // bucket = "bucket"
        0x62, 0x75, 0x63, 0x6b, 0x65, 0x74,
        // key_len = 3
        0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // key = "key"
        0x6b, 0x65, 0x79,
        // upload_id_len = 2
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // upload_id = "u1"
        0x75, 0x31,
    ];

    /// Offsets into `GOLDEN_NAMED`, used to build malformed variants.
    const NAMED_BUCKET_LEN_AT: usize = 8;
    const NAMED_BUCKET_AT: usize = 16;
    const NAMED_KEY_LEN_AT: usize = 22;
    const NAMED_KEY_AT: usize = 30;
    const NAMED_UPLOAD_ID_LEN_AT: usize = 33;
    const NAMED_UPLOAD_ID_AT: usize = 41;

    /// All three strings empty and a negative created_at: the shortest legal
    /// record (32 bytes), and a pin on the sign of the timestamp field.
    #[rustfmt::skip]
    const GOLDEN_EMPTY: &[u8] = &[
        // created_at = -1
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        // bucket_len = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // key_len = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // upload_id_len = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];

    fn golden_uploads() -> Vec<(&'static str, UploadRecord, &'static [u8])> {
        vec![
            (
                "named",
                UploadRecord::with_created_at(
                    0x0102_0304_0506_0708,
                    "bucket".to_string(),
                    "key".to_string(),
                    "u1".to_string(),
                ),
                GOLDEN_NAMED,
            ),
            (
                "empty",
                UploadRecord::with_created_at(-1, String::new(), String::new(), String::new()),
                GOLDEN_EMPTY,
            ),
        ]
    }

    /// Copies `src` with the bytes at `at` replaced by `patch`.
    fn patched(src: &[u8], at: usize, patch: &[u8]) -> Vec<u8> {
        let mut out = src.to_vec();
        out[at..at + patch.len()].copy_from_slice(patch);
        out
    }

    /// Format v1 pin: serialization must produce exactly these bytes.
    #[test]
    fn golden_serialization() {
        for (label, record, expected) in golden_uploads() {
            assert_eq!(record.to_vec(), expected, "golden mismatch for {label}");
        }
    }

    /// Format v1 pin: the same bytes must decode to the same fields.
    #[test]
    fn golden_deserialization() {
        for (label, record, expected) in golden_uploads() {
            let decoded =
                UploadRecord::try_from(expected).unwrap_or_else(|e| panic!("{label}: {e}"));
            assert_eq!(
                decoded.created_at(),
                record.created_at(),
                "{label} created_at"
            );
            assert_eq!(decoded.bucket(), record.bucket(), "{label} bucket");
            assert_eq!(decoded.key(), record.key(), "{label} key");
            assert_eq!(decoded.upload_id(), record.upload_id(), "{label} upload_id");
        }
    }

    /// The clock read lives in `new` alone, so a record made now carries a
    /// timestamp between the two readings around it.
    #[test]
    fn new_stamps_the_current_time() {
        let before = Utc::now().timestamp();
        let record = UploadRecord::new("b".to_string(), "k".to_string(), "u".to_string());
        let after = Utc::now().timestamp();
        assert!(
            (before..=after).contains(&record.created_at()),
            "created_at {} outside [{before}, {after}]",
            record.created_at()
        );
    }

    #[test]
    fn malformed_upload_records() {
        // Truncated at every field boundary, and inside every field.
        for cut in [0usize, 4, 8, 12, 16, 21, 22, 26, 30, 32, 33, 40, 41, 42] {
            assert!(
                matches!(
                    UploadRecord::try_from(&GOLDEN_NAMED[..cut]),
                    Err(FsError::Truncated {
                        record: "UploadRecord",
                        ..
                    })
                ),
                "expected Truncated when cut at {cut}"
            );
        }

        // One byte too many.
        for golden in [GOLDEN_NAMED, GOLDEN_EMPTY] {
            let mut extra = golden.to_vec();
            extra.push(0);
            assert_eq!(
                UploadRecord::try_from(extra.as_slice()).unwrap_err(),
                FsError::TrailingBytes {
                    record: "UploadRecord",
                    extra: 1
                }
            );
        }

        // A last field shorter than the record leaves trailing bytes.
        let short = patched(GOLDEN_NAMED, NAMED_UPLOAD_ID_LEN_AT, &1u64.to_le_bytes());
        assert_eq!(
            UploadRecord::try_from(short.as_slice()).unwrap_err(),
            FsError::TrailingBytes {
                record: "UploadRecord",
                extra: 1
            }
        );

        // Lengths nothing could satisfy must not overflow the offset maths.
        // The value does not fit a 32 bit usize and overflows the offset sum
        // on a 64 bit one, so it is reported either way.
        for at in [
            NAMED_BUCKET_LEN_AT,
            NAMED_KEY_LEN_AT,
            NAMED_UPLOAD_ID_LEN_AT,
        ] {
            let raw = patched(GOLDEN_NAMED, at, &u64::MAX.to_le_bytes());
            assert!(
                matches!(
                    UploadRecord::try_from(raw.as_slice()),
                    Err(FsError::LengthOverflow {
                        record: "UploadRecord",
                        ..
                    })
                ),
                "absurd length at {at} must not panic"
            );
        }

        // Strings that are not UTF-8.
        for (at, field) in [
            (NAMED_BUCKET_AT, "bucket"),
            (NAMED_KEY_AT, "key"),
            (NAMED_UPLOAD_ID_AT, "upload_id"),
        ] {
            let raw = patched(GOLDEN_NAMED, at, &[0xff]);
            assert_eq!(
                UploadRecord::try_from(raw.as_slice()).unwrap_err(),
                FsError::InvalidUtf8 {
                    record: "UploadRecord",
                    field
                }
            );
        }
    }
}
