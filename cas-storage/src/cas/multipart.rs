use std::{convert::TryFrom, sync::Arc};

use crate::metastore::{
    BaseMetaTree, BlockId, CONTENT_HASH_SIZE, ContentHash, FsError, MetaError,
    codec::{Reader, id_list_len, put_id_list, put_len},
};

#[derive(Debug)]
pub struct MultiPart {
    size: usize,
    part_number: i64,
    bucket: String,
    key: String,
    upload_id: String,
    hash: ContentHash,
    blocks: Vec<BlockId>,
}

impl MultiPart {
    pub fn new(
        size: usize,
        part_number: i64,
        bucket: String,
        key: String,
        upload_id: String,
        hash: ContentHash,
        blocks: Vec<BlockId>,
    ) -> Self {
        Self {
            size,
            part_number,
            bucket,
            key,
            upload_id,
            hash,
            blocks,
        }
    }

    pub fn blocks(&self) -> &[BlockId] {
        &self.blocks
    }

    /// MD5 digest of this part's content -- the part's ETag, and one input to
    /// the completed object's multipart ETag.
    pub fn hash(&self) -> ContentHash {
        self.hash
    }

    /// Size of this part in bytes.
    pub fn size(&self) -> usize {
        self.size
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.into()
    }
}

/// Serializes a multipart part record (format v1):
///
/// ```text
/// size u64 | part_number i64 | bucket_len u64 | bucket | key_len u64 | key |
/// upload_len u64 | upload_id | hash [CONTENT_HASH_SIZE] |
/// id_width u8 | count u64 | ids[count * id_width]
/// ```
impl From<&MultiPart> for Vec<u8> {
    fn from(mp: &MultiPart) -> Self {
        let mut out = Vec::with_capacity(
            8 + 8
                + 8
                + mp.bucket.len()
                + 8
                + mp.key.len()
                + 8
                + mp.upload_id.len()
                + CONTENT_HASH_SIZE
                + id_list_len(&mp.blocks),
        );

        put_len(&mut out, mp.size);
        out.extend_from_slice(&mp.part_number.to_le_bytes());
        put_len(&mut out, mp.bucket.len());
        out.extend_from_slice(mp.bucket.as_bytes());
        put_len(&mut out, mp.key.len());
        out.extend_from_slice(mp.key.as_bytes());
        put_len(&mut out, mp.upload_id.len());
        out.extend_from_slice(mp.upload_id.as_bytes());
        out.extend_from_slice(mp.hash.as_slice());
        put_id_list(&mut out, &mp.blocks);

        out
    }
}

/// Deserializes a part record from the layout above, with an exact length
/// check.
///
/// The block list length is derived from the width byte and the count, so
/// trailing garbage is reported rather than absorbed as extra blocks.
impl TryFrom<&[u8]> for MultiPart {
    type Error = FsError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let mut r = Reader::new("MultiPart", value);

        let size = r.len("size")?;
        let part_number = r.i64("part_number")?;

        // ---- tfstor-extension: BEGIN ----
        // Upstream used `String::from_utf8_unchecked` for the three string fields
        // below, on the grounds that only valid strings are ever inserted. That
        // argument covers the write path, not the read path: these bytes come
        // back off disk, where corruption, a truncated write or a format change
        // breaks the invariant, and the penalty for breaking it is undefined
        // behaviour rather than an error. Bucket names, keys and upload IDs are
        // short, so the validation in `Reader::utf8` costs nothing worth having.
        let bucket_len = r.len("bucket_len")?;
        let bucket = r.utf8("bucket", bucket_len)?;
        let key_len = r.len("key_len")?;
        let key = r.utf8("key", key_len)?;
        let upload_len = r.len("upload_len")?;
        let upload_id = r.utf8("upload_id", upload_len)?;
        // ---- tfstor-extension: END ----

        let hash = ContentHash(r.array::<CONTENT_HASH_SIZE>("hash")?);
        let blocks = r.id_list()?;
        r.finish()?;

        Ok(MultiPart {
            size,
            part_number,
            bucket,
            key,
            upload_id,
            hash,
            blocks,
        })
    }
}

pub struct MultiPartTree {
    tree: Arc<dyn BaseMetaTree>,
}
// Implement Debug manually
impl std::fmt::Debug for MultiPartTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiPartTree")
            .field("tree", &"<BaseMetaTree>")
            .finish()
    }
}
impl MultiPartTree {
    pub fn new(tree: Arc<dyn BaseMetaTree>) -> Self {
        Self { tree }
    }

    pub fn insert(&self, key: &[u8], mp: MultiPart) -> Result<(), MetaError> {
        self.tree.insert(key, mp.to_vec())
    }

    pub fn remove(&self, key: &[u8]) -> Result<(), MetaError> {
        self.tree.remove(key)
    }

    pub fn get_multipart_part(&self, key: &[u8]) -> Result<Option<MultiPart>, MetaError> {
        let value = match self.tree.get(key) {
            Ok(Some(v)) => v,
            Ok(None) => return Ok(None),
            Err(e) => return Err(e),
        };
        let mp = MultiPart::try_from(value.as_ref())?;
        Ok(Some(mp))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metastore::{BLOCKID_SIZE, MAX_BLOCKID_SIZE};

    /// Part record with one 16 byte block id (format v1).
    #[rustfmt::skip]
    const GOLDEN_W16: &[u8] = &[
        // size = 9
        0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // part_number = 1
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // bucket_len = 1
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // bucket = "b"
        0x62,
        // key_len = 1
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // key = "k"
        0x6b,
        // upload_len = 1
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // upload_id = "u"
        0x75,
        // hash (16 bytes)
        0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
        0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
        // id_width = 16
        0x10,
        // count = 1
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // ids
        0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb,
        0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb,
    ];

    /// Offsets into `GOLDEN_W16`, used to build malformed variants.
    const W16_BUCKET_LEN_AT: usize = 16;
    const W16_BUCKET_AT: usize = 24;
    const W16_KEY_AT: usize = 33;
    const W16_UPLOAD_AT: usize = 42;
    const W16_WIDTH_AT: usize = 59;
    const W16_COUNT_AT: usize = 60;
    /// Offset of the count field in `GOLDEN_EMPTY` (no string payloads).
    const EMPTY_COUNT_AT: usize = 57;

    /// Part record with two 32 byte block ids.
    #[rustfmt::skip]
    const GOLDEN_W32: &[u8] = &[
        // size = 1024
        0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // part_number = 3
        0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // bucket_len = 2
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // bucket = "bk"
        0x62, 0x6b,
        // key_len = 2
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // key = "ky"
        0x6b, 0x79,
        // upload_len = 2
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // upload_id = "up"
        0x75, 0x70,
        // hash (16 bytes)
        0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
        0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
        // id_width = 32
        0x20,
        // count = 2
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // ids
        0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02,
        0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02,
        0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02,
        0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02,
        0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03,
        0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03,
        0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03,
        0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03,
    ];

    /// Part record with empty strings and no blocks: width 0, count 0, and the
    /// shortest legal record (65 bytes).
    #[rustfmt::skip]
    const GOLDEN_EMPTY: &[u8] = &[
        // size = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // part_number = -1
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        // bucket_len = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // key_len = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // upload_len = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // hash (16 bytes)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // id_width = 0 (legal only for an empty list)
        0x00,
        // count = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];

    fn golden_parts() -> Vec<(&'static str, MultiPart, &'static [u8])> {
        vec![
            (
                "w16",
                MultiPart {
                    size: 9,
                    part_number: 1,
                    bucket: "b".to_string(),
                    key: "k".to_string(),
                    upload_id: "u".to_string(),
                    hash: ContentHash([0xaa; CONTENT_HASH_SIZE]),
                    blocks: vec![BlockId::from([0xbb; BLOCKID_SIZE])],
                },
                GOLDEN_W16,
            ),
            (
                "w32",
                MultiPart {
                    size: 1024,
                    part_number: 3,
                    bucket: "bk".to_string(),
                    key: "ky".to_string(),
                    upload_id: "up".to_string(),
                    hash: ContentHash([0x01; CONTENT_HASH_SIZE]),
                    blocks: vec![
                        BlockId::from([0x02; MAX_BLOCKID_SIZE]),
                        BlockId::from([0x03; MAX_BLOCKID_SIZE]),
                    ],
                },
                GOLDEN_W32,
            ),
            (
                "empty",
                MultiPart {
                    size: 0,
                    part_number: -1,
                    bucket: String::new(),
                    key: String::new(),
                    upload_id: String::new(),
                    hash: ContentHash([0x00; CONTENT_HASH_SIZE]),
                    blocks: vec![],
                },
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
        for (name, part, expected) in golden_parts() {
            assert_eq!(part.to_vec(), expected, "golden mismatch for {name}");
        }
    }

    /// Format v1 pin: the same bytes must decode to the same fields.
    #[test]
    fn golden_deserialization() {
        for (name, part, expected) in golden_parts() {
            let decoded = MultiPart::try_from(expected).unwrap_or_else(|e| panic!("{name}: {e}"));
            assert_eq!(decoded.size, part.size, "{name} size");
            assert_eq!(decoded.part_number, part.part_number, "{name} part_number");
            assert_eq!(decoded.bucket, part.bucket, "{name} bucket");
            assert_eq!(decoded.key, part.key, "{name} key");
            assert_eq!(decoded.upload_id, part.upload_id, "{name} upload_id");
            assert_eq!(decoded.hash, part.hash, "{name} hash");
            assert_eq!(decoded.blocks, part.blocks, "{name} blocks");
        }
    }

    #[test]
    fn malformed_multipart_records() {
        // Truncated at every field boundary.
        for cut in [0usize, 8, 16, 24, 25, 33, 34, 42, 43, 59, 60, 68, 83] {
            assert!(
                matches!(
                    MultiPart::try_from(&GOLDEN_W16[..cut]),
                    Err(FsError::Truncated {
                        record: "MultiPart",
                        ..
                    })
                ),
                "expected Truncated when cut at {cut}"
            );
        }

        // One byte too many: the old decoder read the id list as "whatever is
        // left", so trailing garbage became extra blocks. It is now reported.
        for golden in [GOLDEN_W16, GOLDEN_W32, GOLDEN_EMPTY] {
            let mut extra = golden.to_vec();
            extra.push(0);
            assert_eq!(
                MultiPart::try_from(extra.as_slice()).unwrap_err(),
                FsError::TrailingBytes {
                    record: "MultiPart",
                    extra: 1
                }
            );
        }
        // Sixteen extra bytes -- a whole spurious id -- are rejected too.
        let mut extra_id = GOLDEN_W16.to_vec();
        extra_id.extend_from_slice(&[0xcc; BLOCKID_SIZE]);
        assert_eq!(
            MultiPart::try_from(extra_id.as_slice()).unwrap_err(),
            FsError::TrailingBytes {
                record: "MultiPart",
                extra: 16
            }
        );

        // An id width that is neither 16 nor 32.
        for bad in [1u8, 17, 33, 255] {
            let raw = patched(GOLDEN_W16, W16_WIDTH_AT, &[bad]);
            assert_eq!(
                MultiPart::try_from(raw.as_slice()).unwrap_err(),
                FsError::InvalidIdWidth(bad)
            );
        }

        // Width 0 with a non-empty list.
        let raw = patched(GOLDEN_W16, W16_WIDTH_AT, &[0]);
        assert_eq!(
            MultiPart::try_from(raw.as_slice()).unwrap_err(),
            FsError::InvalidIdWidth(0)
        );
        let raw = patched(GOLDEN_EMPTY, EMPTY_COUNT_AT, &1u64.to_le_bytes());
        assert_eq!(
            MultiPart::try_from(raw.as_slice()).unwrap_err(),
            FsError::InvalidIdWidth(0)
        );

        // Counts and lengths nothing could satisfy must not overflow the
        // offset maths.
        for at in [W16_BUCKET_LEN_AT, W16_COUNT_AT] {
            let raw = patched(GOLDEN_W16, at, &u64::MAX.to_le_bytes());
            assert!(
                matches!(
                    MultiPart::try_from(raw.as_slice()),
                    Err(FsError::LengthOverflow {
                        record: "MultiPart",
                        ..
                    })
                ),
                "absurd length at {at} must not panic"
            );
        }

        // Strings that are not UTF-8.
        for (at, field) in [
            (W16_BUCKET_AT, "bucket"),
            (W16_KEY_AT, "key"),
            (W16_UPLOAD_AT, "upload_id"),
        ] {
            let raw = patched(GOLDEN_W16, at, &[0xff]);
            assert_eq!(
                MultiPart::try_from(raw.as_slice()).unwrap_err(),
                FsError::InvalidUtf8 {
                    record: "MultiPart",
                    field
                }
            );
        }
    }
}
