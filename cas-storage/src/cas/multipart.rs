use std::{convert::TryFrom, sync::Arc};

use crate::metastore::{
    BlockId, CONTENT_HASH_SIZE, ContentHash, FsError, MetaError, MetaTreeExt,
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

    /// Bucket of the object this part belongs to.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Key of the object this part belongs to.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Upload this part belongs to.
    pub fn upload_id(&self) -> &str {
        &self.upload_id
    }

    /// Position of this part within its upload.
    pub fn part_number(&self) -> i64 {
        self.part_number
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

/// Storage key of one part of a multipart upload, in `_MULTIPART_PARTS`:
///
/// ```text
/// bucket_len u64 | bucket | key_len u64 | key | upload_id_len u64 |
/// upload_id | part_number u64 BE
/// ```
///
/// Length-prefixed rather than joined with `-`, because bucket names and
/// keys may contain any separator one might pick: the prefixes make the
/// encoding injective, so two different uploads can never collide on one
/// key, and no upload's key can start with another's prefix (the
/// `upload_id_len` field forces the triples to match).
///
/// The `part_number` tail is UNSIGNED BIG-ENDIAN, and that is the only field
/// whose byte order is load-bearing: it makes the store's own key order the
/// numeric part order, so every part of one upload lies contiguously under
/// [`part_prefix`] and enumerating them is a plain forward scan
/// ([`MultiPartTree::parts_of`], ADR 0003). Little-endian would scatter part
/// 256 among the low numbers; the length fields keep the codec's
/// little-endian house rule because nothing orders by them. S3 part numbers
/// run 1..=10000, so the unsigned cast never meets a negative in practice --
/// one would merely sort after every real part, not collide with it.
#[allow(clippy::cast_sign_loss)]
pub(crate) fn part_key(bucket: &str, key: &str, upload_id: &str, part_number: i64) -> Vec<u8> {
    let mut out = part_prefix(bucket, key, upload_id);
    out.extend_from_slice(&(part_number as u64).to_be_bytes());
    out
}

/// The byte prefix every part of one upload shares: [`part_key`] without its
/// `part_number` tail.
///
/// This is what a per-upload scan positions on. Nothing parses these bytes
/// back -- point reads rebuild the key they wrote and scans decode record
/// VALUES (hard rule 4) -- so the encoding only has to be injective and
/// order-preserving in the tail, which it is.
pub(crate) fn part_prefix(bucket: &str, key: &str, upload_id: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + bucket.len() + 8 + key.len() + 8 + upload_id.len());
    put_len(&mut out, bucket.len());
    out.extend_from_slice(bucket.as_bytes());
    put_len(&mut out, key.len());
    out.extend_from_slice(key.as_bytes());
    put_len(&mut out, upload_id.len());
    out.extend_from_slice(upload_id.as_bytes());
    out
}

pub struct MultiPartTree {
    tree: Arc<dyn MetaTreeExt + Send + Sync>,
}
// Implement Debug manually
impl std::fmt::Debug for MultiPartTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiPartTree")
            .field("tree", &"<MetaTreeExt>")
            .finish()
    }
}
impl MultiPartTree {
    /// Wraps the `_MULTIPART_PARTS` tree.
    ///
    /// The extended handle rather than the base one: per-upload enumeration
    /// ([`Self::parts_of`]) is a range scan, and scans live on
    /// [`MetaTreeExt`].
    pub fn new(tree: Arc<dyn MetaTreeExt + Send + Sync>) -> Self {
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

    /// Every part record carrying `prefix` -- one upload's parts, in
    /// ascending part_number order (the big-endian tail of [`part_key`] is
    /// what makes key order numeric order).
    ///
    /// The scan starts strictly after `prefix` itself and stops at the first
    /// key that does not carry it. Nothing is skipped by starting there,
    /// because every part key is the prefix plus an 8 byte tail and so sorts
    /// after it; stopping at the first non-match is exact, because the store
    /// iterates in key order and the encoding is prefix-injective.
    ///
    /// Records are decoded from VALUES, never from keys (hard rule 4). A
    /// legacy dash-format record is therefore unreachable here -- no prefix
    /// matches it -- which is precisely what leaves it to the GC's
    /// value-driven orphan sweep (ADR 0003: the first sweep IS the
    /// migration).
    pub fn parts_of(
        &self,
        prefix: &[u8],
    ) -> Box<dyn Iterator<Item = Result<MultiPart, MetaError>> + Send> {
        let prefix = prefix.to_vec();
        Box::new(
            self.tree
                .iter_kv(Some(prefix.clone()))
                .take_while(move |item| match item {
                    Ok((key, _)) => key.starts_with(&prefix),
                    // A store error must reach the caller, not silently end
                    // the scan short of the upload's remaining parts.
                    Err(_) => true,
                })
                .map(|item| {
                    let (_, raw) = item?;
                    MultiPart::try_from(&*raw).map_err(MetaError::from)
                }),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metastore::{
        BLOCKID_SIZE, FjallStore, MAX_BLOCKID_SIZE, MULTIPART_PARTS_TREE, MetaStore,
    };
    use tempfile::{TempDir, tempdir};

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

    /// The parts tree, plus the raw handle underneath it for planting
    /// records the typed surface can no longer write.
    fn test_tree() -> (MultiPartTree, Arc<dyn MetaTreeExt + Send + Sync>, TempDir) {
        let dir = tempdir().unwrap();
        let meta = MetaStore::new(
            FjallStore::new(dir.path().to_path_buf(), Some(1), None).unwrap(),
            None,
        );
        let raw = meta.get_tree_ext(MULTIPART_PARTS_TREE).unwrap();
        (MultiPartTree::new(Arc::clone(&raw)), raw, dir)
    }

    fn part(bucket: &str, key: &str, upload_id: &str, part_number: i64) -> MultiPart {
        MultiPart::new(
            1024,
            part_number,
            bucket.to_string(),
            key.to_string(),
            upload_id.to_string(),
            ContentHash([0x11; CONTENT_HASH_SIZE]),
            vec![BlockId::from([0x22; BLOCKID_SIZE])],
        )
    }

    fn insert(tree: &MultiPartTree, bucket: &str, key: &str, upload_id: &str, part_number: i64) {
        tree.insert(
            &part_key(bucket, key, upload_id, part_number),
            part(bucket, key, upload_id, part_number),
        )
        .unwrap();
    }

    fn part_numbers(tree: &MultiPartTree, prefix: &[u8]) -> Vec<i64> {
        tree.parts_of(prefix)
            .map(|part| part.unwrap().part_number())
            .collect()
    }

    /// The ambiguity the dash-joined keys had: with a separator, a bucket,
    /// key or upload id containing it makes two different parts share one
    /// key. Length prefixes make every triple its own key, and keep one
    /// upload's prefix from reaching into another's.
    #[test]
    fn part_keys_are_unambiguous() {
        // The pair that collided under `{bucket}-{key}-{upload}-{n}`.
        assert_ne!(part_key("a-b", "c", "u", 1), part_key("a", "b-c", "u", 1));
        assert_ne!(part_key("a", "b-u", "x", 1), part_key("a", "b", "u-x", 1));

        // The same triple always encodes to the same bytes: point reads
        // depend on rebuilding exactly what was written.
        assert_eq!(part_key("b", "k", "u1", 3), part_key("b", "k", "u1", 3));
        assert_ne!(part_key("b", "k", "u1", 3), part_key("b", "k", "u1", 4));

        // A part key is its upload's prefix plus the 8 byte tail.
        let prefix = part_prefix("b", "k", "u1");
        let key = part_key("b", "k", "u1", 7);
        assert!(key.starts_with(&prefix));
        assert_eq!(key.len(), prefix.len() + 8);
        assert_eq!(key[prefix.len()..], 7u64.to_be_bytes());

        // No other upload's key carries this prefix -- not even one whose id
        // extends it, which a bare concatenation would have let through.
        for foreign in [
            part_key("b", "k", "u10", 7),
            part_key("b", "k1", "u1", 7),
            part_key("b1", "k", "u1", 7),
        ] {
            assert!(!foreign.starts_with(&prefix));
        }
    }

    /// A prefix scan returns exactly one upload's parts, in ascending
    /// part_number order, whatever order they were written in.
    #[test]
    fn prefix_scan_yields_one_uploads_parts_in_order() {
        let (tree, _raw, _dir) = test_tree();

        for number in [3, 1, 2] {
            insert(&tree, "b", "k", "u1", number);
        }
        // Neighbours that must not appear: same key different upload, same
        // upload id under another key, another bucket.
        insert(&tree, "b", "k", "u2", 1);
        insert(&tree, "b", "k2", "u1", 1);
        insert(&tree, "b2", "k", "u1", 1);

        assert_eq!(part_numbers(&tree, &part_prefix("b", "k", "u1")), [1, 2, 3]);
        assert_eq!(part_numbers(&tree, &part_prefix("b", "k", "u2")), [1]);
        assert!(
            tree.parts_of(&part_prefix("b", "k", "gone"))
                .next()
                .is_none(),
            "an upload with no parts scans empty"
        );

        // The scan decodes values, so every record really is this upload's.
        for part in tree.parts_of(&part_prefix("b", "k", "u1")) {
            let part = part.unwrap();
            assert_eq!(
                (part.bucket(), part.key(), part.upload_id()),
                ("b", "k", "u1")
            );
        }
    }

    /// The tail is unsigned big-endian, so key order IS numeric order across
    /// a byte boundary. Little-endian would sort 256 before 255.
    #[test]
    fn part_number_order_survives_the_byte_boundary() {
        let (tree, _raw, _dir) = test_tree();

        for number in [257, 255, 1, 256] {
            insert(&tree, "b", "k", "u1", number);
        }

        assert_eq!(
            part_numbers(&tree, &part_prefix("b", "k", "u1")),
            [1, 255, 256, 257]
        );
    }

    /// Hard rule 4: a legacy dash-keyed record is invisible to every prefix
    /// scan -- no key format bridges the two -- while staying perfectly
    /// visible to the value-driven walk that the GC's orphan sweep and fsck
    /// use. That gap IS the migration: the sweep reaps it.
    #[test]
    fn a_legacy_dash_keyed_record_is_invisible_to_prefix_scans() {
        let (tree, raw, _dir) = test_tree();

        raw.insert(b"b-k-u1-1", part("b", "k", "u1", 1).to_vec())
            .unwrap();
        insert(&tree, "b", "k", "u1", 2);

        assert_eq!(
            part_numbers(&tree, &part_prefix("b", "k", "u1")),
            [2],
            "only the re-keyed part is reachable by prefix"
        );

        // Both records are still there, and the value walk sees both.
        assert_eq!(raw.len().unwrap(), 2);
        let mut walked: Vec<i64> = raw
            .iter_all()
            .map(|item| {
                let (_, value) = item.unwrap();
                MultiPart::try_from(&*value).unwrap().part_number()
            })
            .collect();
        walked.sort_unstable();
        assert_eq!(walked, [1, 2], "the value-driven walk still reaches both");
    }
}
