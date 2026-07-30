use std::{
    convert::TryFrom,
    time::{SystemTime, UNIX_EPOCH},
};

use chrono::Utc;

use super::{
    FsError,
    codec::{Reader, put_len},
};

/// `BucketMeta` represents metadata for a storage bucket.
///
/// This struct stores essential information about a bucket, including:
/// - Creation time (ctime) as a Unix timestamp
/// - The bucket name as a string
///
/// BucketMeta is used to track and manage buckets in the storage system.
#[derive(Debug)]
pub struct BucketMeta {
    /// Creation time as a Unix timestamp (seconds since epoch)
    ctime: i64,
    /// Name of the bucket
    name: String,
}

impl BucketMeta {
    /// Creates a new BucketMeta with the given name and current timestamp.
    ///
    /// # Arguments
    /// * `name` - The name of the bucket
    ///
    /// # Returns
    /// A new BucketMeta instance with the current time as creation time
    pub fn new(name: String) -> Self {
        Self {
            ctime: Utc::now().timestamp(),
            name,
        }
    }

    /// Returns the creation time of the bucket as a SystemTime.
    ///
    /// # Returns
    /// The creation time as a SystemTime instance
    pub fn ctime(&self) -> SystemTime {
        UNIX_EPOCH + std::time::Duration::from_secs(self.ctime as u64)
    }

    /// Returns the name of the bucket.
    ///
    /// # Returns
    /// A string slice containing the bucket name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Serializes the bucket metadata to a byte vector.
    ///
    /// # Returns
    /// A vector of bytes representing the serialized bucket metadata
    pub fn to_vec(&self) -> Vec<u8> {
        self.into()
    }
}

/// Serializes BucketMeta (format v1):
///
/// ```text
/// ctime i64 | name_len u64 | name[name_len]
/// ```
impl From<&BucketMeta> for Vec<u8> {
    fn from(b: &BucketMeta) -> Self {
        let mut out = Vec::with_capacity(8 + 8 + b.name.len());
        out.extend_from_slice(&b.ctime.to_le_bytes());
        put_len(&mut out, b.name.len());
        out.extend_from_slice(b.name.as_bytes());
        out
    }
}

/// Deserializes BucketMeta from the layout above, with an exact length check.
impl TryFrom<&[u8]> for BucketMeta {
    type Error = FsError;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let mut r = Reader::new("BucketMeta", value);
        let ctime = r.i64("ctime")?;
        let name_len = r.len("name_len")?;
        // ---- tfstor-extension: BEGIN ----
        // Upstream used `String::from_utf8_unchecked` here, arguing that only
        // valid strings are ever written. That covers the write path only:
        // these bytes come back from a database file, where the invariant can
        // be broken by corruption, a truncated write, or a format change. The
        // cost of validating a bucket name is irrelevant next to undefined
        // behaviour, so `Reader::utf8` validates and reports a malformed
        // record instead.
        let name = r.utf8("name", name_len)?;
        // ---- tfstor-extension: END ----
        r.finish()?;

        Ok(BucketMeta { ctime, name })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// BucketMeta for the bucket named "bucket" (format v1).
    #[rustfmt::skip]
    const GOLDEN_NAMED: &[u8] = &[
        // ctime = 0x0102030405060708
        0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
        // name_len = 6
        0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // name = "bucket"
        0x62, 0x75, 0x63, 0x6b, 0x65, 0x74,
    ];

    /// BucketMeta with an empty name and a negative ctime: the shortest legal
    /// record (16 bytes), and a pin on the sign of the timestamp field.
    #[rustfmt::skip]
    const GOLDEN_EMPTY_NAME: &[u8] = &[
        // ctime = -1
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        // name_len = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];

    fn golden_buckets() -> Vec<(&'static str, BucketMeta, &'static [u8])> {
        vec![
            (
                "named",
                BucketMeta {
                    ctime: 0x0102_0304_0506_0708,
                    name: "bucket".to_string(),
                },
                GOLDEN_NAMED,
            ),
            (
                "empty_name",
                BucketMeta {
                    ctime: -1,
                    name: String::new(),
                },
                GOLDEN_EMPTY_NAME,
            ),
        ]
    }

    /// Format v1 pin: serialization must produce exactly these bytes.
    #[test]
    fn golden_serialization() {
        for (name, bucket, expected) in golden_buckets() {
            assert_eq!(bucket.to_vec(), expected, "golden mismatch for {name}");
        }
    }

    /// Format v1 pin: the same bytes must decode to the same fields.
    #[test]
    fn golden_deserialization() {
        for (label, bucket, expected) in golden_buckets() {
            let decoded = BucketMeta::try_from(expected).unwrap_or_else(|e| panic!("{label}: {e}"));
            assert_eq!(decoded.ctime, bucket.ctime, "{label} ctime");
            assert_eq!(decoded.name(), bucket.name(), "{label} name");
        }
    }

    #[test]
    fn malformed_bucket_meta_records() {
        // Truncated at every field boundary.
        for cut in [0usize, 4, 8, 12, 15, 16, 21] {
            assert!(
                matches!(
                    BucketMeta::try_from(&GOLDEN_NAMED[..cut]),
                    Err(FsError::Truncated {
                        record: "BucketMeta",
                        ..
                    })
                ),
                "expected Truncated when cut at {cut}"
            );
        }

        // One byte too many.
        let mut extra = GOLDEN_NAMED.to_vec();
        extra.push(0);
        assert_eq!(
            BucketMeta::try_from(extra.as_slice()).unwrap_err(),
            FsError::TrailingBytes {
                record: "BucketMeta",
                extra: 1
            }
        );

        // A name shorter than the record leaves trailing bytes.
        let mut short_name = GOLDEN_NAMED.to_vec();
        short_name[8] = 0x05;
        assert_eq!(
            BucketMeta::try_from(short_name.as_slice()).unwrap_err(),
            FsError::TrailingBytes {
                record: "BucketMeta",
                extra: 1
            }
        );

        // A length nothing could satisfy must not overflow the offset maths.
        // The value does not fit a 32 bit usize and overflows the offset sum
        // on a 64 bit one, so it is reported either way.
        let mut huge = GOLDEN_NAMED.to_vec();
        huge[8..16].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(matches!(
            BucketMeta::try_from(huge.as_slice()),
            Err(FsError::LengthOverflow {
                record: "BucketMeta",
                ..
            })
        ));

        // A name that is not UTF-8.
        let mut bad_utf8 = GOLDEN_NAMED.to_vec();
        bad_utf8[16] = 0xff;
        assert_eq!(
            BucketMeta::try_from(bad_utf8.as_slice()).unwrap_err(),
            FsError::InvalidUtf8 {
                record: "BucketMeta",
                field: "name"
            }
        );
    }
}
