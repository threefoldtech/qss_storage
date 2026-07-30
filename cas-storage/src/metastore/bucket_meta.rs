use std::{
    convert::{TryFrom, TryInto},
    time::{SystemTime, UNIX_EPOCH},
};

use chrono::Utc;

use super::{FsError, PTR_SIZE};

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

/// Implements serialization of BucketMeta to a byte vector.
///
/// The serialized format includes:
/// - 8 bytes for the creation time (i64)
/// - PTR_SIZE bytes for the length of the name
/// - The name bytes
impl From<&BucketMeta> for Vec<u8> {
    fn from(b: &BucketMeta) -> Self {
        let mut out = Vec::with_capacity(8 + PTR_SIZE + b.name.len());
        out.extend_from_slice(&b.ctime.to_le_bytes());
        out.extend_from_slice(&b.name.len().to_le_bytes());
        out.extend_from_slice(b.name.as_bytes());
        out
    }
}

/// Implements deserialization of BucketMeta from a byte slice.
///
/// This implementation validates the input format and extracts the creation time and name.
impl TryFrom<&[u8]> for BucketMeta {
    type Error = FsError;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() < 8 + PTR_SIZE {
            return Err(FsError::MalformedObject);
        }
        let name_len = usize::from_le_bytes(value[8..8 + PTR_SIZE].try_into().unwrap());
        if value.len() != 8 + PTR_SIZE + name_len {
            return Err(FsError::MalformedObject);
        }
        Ok(BucketMeta {
            ctime: i64::from_le_bytes(value[..8].try_into().unwrap()),
            // SAFETY: this is safe because we only store valid strings in the first place.
            name: unsafe { String::from_utf8_unchecked(value[8 + PTR_SIZE..].to_vec()) },
        })
    }
}
