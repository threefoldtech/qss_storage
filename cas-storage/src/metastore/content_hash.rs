use faster_hex::hex_string;

/// Size of a content hash in bytes (16, the width of an MD5 digest).
///
/// Deliberately separate from `BLOCKID_SIZE`: the two happen to be equal
/// today, but a block address width change must not move the ETag field.
pub const CONTENT_HASH_SIZE: usize = 16;

/// MD5 digest of an object's (or multipart part's) full content -- the source
/// of the S3 ETag.
///
/// This is never a block address. Block addressing uses `BlockID`; mixing the
/// two silently corrupts either the ETag a client sees or the location a block
/// is read from, which is why they are distinct types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentHash(pub [u8; CONTENT_HASH_SIZE]);

impl ContentHash {
    /// Returns the digest as a byte slice.
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// Returns the digest as lowercase hex, the form an ETag is rendered in.
    pub fn to_hex(&self) -> String {
        hex_string(&self.0)
    }
}

impl From<[u8; CONTENT_HASH_SIZE]> for ContentHash {
    fn from(bytes: [u8; CONTENT_HASH_SIZE]) -> Self {
        Self(bytes)
    }
}

impl From<ContentHash> for [u8; CONTENT_HASH_SIZE] {
    fn from(hash: ContentHash) -> Self {
        hash.0
    }
}

impl AsRef<[u8]> for ContentHash {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
