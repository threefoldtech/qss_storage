use faster_hex::hex_string;
use std::{convert::TryFrom, path::PathBuf};

use super::{
    FsError,
    codec::{Reader, put_len},
};

/// Narrow block address width in bytes: BLAKE3 truncated to 128 bits.
///
/// The width a store addresses blocks at is a per-store choice recorded in its
/// header, so this is the narrow end of the range rather than the one true
/// width; `BlockId` carries its own width alongside its bytes.
pub const BLOCKID_SIZE: usize = 16;

/// The widest block address `BlockId` can hold (a 256 bit hash).
pub const MAX_BLOCKID_SIZE: usize = 32;

/// Address of a data block: the hash of the block's contents, at the width the
/// store that produced it uses (16 or 32 bytes).
///
/// A `BlockId` is never a content hash of a whole object -- that is
/// `ContentHash`, the ETag source. This type only ever addresses a block.
///
/// The width is a runtime property of the store, not a compile-time constant,
/// so the id carries its length instead of being a fixed-size array.
///
/// # Invariant
///
/// `bytes[len..]` is always zero. Every constructor establishes this, and the
/// fields are private so nothing else can break it. That is what makes the
/// derived comparisons agree with `as_slice()`:
///
/// - `PartialEq`/`Hash`: two ids are equal exactly when their significant
///   bytes and widths match, because the padding contributes nothing.
/// - `Ord`: comparing the padded arrays (then the lengths, as the derive does)
///   yields the same order as comparing the `as_slice()` byte strings. Where
///   the significant bytes of the shorter id are a prefix of the longer one,
///   the longer id's remaining bytes are compared against zeros -- non-zero
///   sorts the longer id after, all-zero falls through to the length
///   comparison, which also sorts the shorter id first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BlockId {
    /// Padded storage; only the first `len` bytes are significant.
    bytes: [u8; MAX_BLOCKID_SIZE],
    /// Number of significant bytes: 16 or 32.
    len: u8,
}

impl BlockId {
    /// Builds a block id from raw bytes.
    ///
    /// # Arguments
    /// * `bytes` - The address bytes; must be 16 or 32 bytes long
    ///
    /// # Returns
    /// The block id, or `FsError::InvalidIdWidth` for any other length
    pub fn from_slice(bytes: &[u8]) -> Result<Self, FsError> {
        match bytes.len() {
            BLOCKID_SIZE | MAX_BLOCKID_SIZE => {
                let mut buf = [0u8; MAX_BLOCKID_SIZE];
                buf[..bytes.len()].copy_from_slice(bytes);
                Ok(Self {
                    bytes: buf,
                    // the match arm bounds the length to 16 or 32
                    #[allow(clippy::cast_possible_truncation)]
                    len: bytes.len() as u8,
                })
            }
            other => Err(FsError::InvalidIdWidth(
                u8::try_from(other).unwrap_or(u8::MAX),
            )),
        }
    }

    /// Returns the significant bytes of the address.
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }

    /// Returns the width of the address in bytes (16 or 32).
    pub fn len(&self) -> usize {
        self.len as usize
    }

    /// Always false: a block id is 16 or 32 bytes wide, never empty.
    ///
    /// Present because `len()` without `is_empty()` is a clippy lint.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the address as lowercase hex, the form used in logs.
    pub fn to_hex(&self) -> String {
        hex_string(self.as_slice())
    }
}

impl From<[u8; BLOCKID_SIZE]> for BlockId {
    // both widths are compile-time constants well below 256
    #[allow(clippy::cast_possible_truncation)]
    fn from(bytes: [u8; BLOCKID_SIZE]) -> Self {
        let mut buf = [0u8; MAX_BLOCKID_SIZE];
        buf[..BLOCKID_SIZE].copy_from_slice(&bytes);
        Self {
            bytes: buf,
            len: BLOCKID_SIZE as u8,
        }
    }
}

impl From<[u8; MAX_BLOCKID_SIZE]> for BlockId {
    // both widths are compile-time constants well below 256
    #[allow(clippy::cast_possible_truncation)]
    fn from(bytes: [u8; MAX_BLOCKID_SIZE]) -> Self {
        Self {
            bytes,
            len: MAX_BLOCKID_SIZE as u8,
        }
    }
}

/// Builds the on-disk path of a block: the ADR 0006 layout.
///
/// ```text
/// <root>/<hex b0>/.../<hex b(depth-1)>/<full-hex id>
/// ```
///
/// Directory names are single hex bytes of the id's leading bytes (2 chars
/// each); the filename is the full-width lowercase hex of the id (32 or 64
/// chars), so dir names and file names can never collide. A file's NAME
/// identifies its block, so any depth is correct -- the depth choice is
/// performance placement only, and this function is pure: same id + depth =
/// same path, always.
///
/// `depth` is clamped to `1..=id.len()`. A corrupt record depth therefore
/// yields a well-formed path that simply is not occupied, and the read
/// surfaces "block file missing" instead of a panic or an aliased path.
pub fn block_disk_path(id: &BlockId, depth: u8, mut root: PathBuf) -> PathBuf {
    let depth = (depth as usize).clamp(1, id.len());
    for byte in &id.as_slice()[..depth] {
        root.push(hex_string(&[*byte]));
    }
    root.push(id.to_hex());
    root
}

/// `Block` represents metadata about a stored data block in the content-addressable storage system.
///
/// Each Block contains:
/// - The size of the actual data
/// - The fanout depth its file was placed at (the path itself is derived:
///   see [`block_disk_path`])
/// - A reference count (rc) tracking how many objects reference this block
#[derive(Debug)]
pub struct Block {
    /// Size of the block data in bytes
    size: usize,
    /// Fanout depth of the block file, `1..=width(id)`. Pure placement: the
    /// file's name is the full id at every depth.
    depth: u8,
    /// Reference count - how many objects reference this block
    rc: usize,
}

/// Serializes a Block (format v2):
///
/// ```text
/// size u64 | depth u8 | rc u64
/// ```
///
/// Format v1 stored a variable-length path allocated from the `_PATHS` tree;
/// ADR 0006 replaced that with the derived path scheme, and the stored path
/// bytes with the one-byte depth. The store header version gates the break.
impl From<&Block> for Vec<u8> {
    fn from(b: &Block) -> Self {
        let mut out = Vec::with_capacity(8 + 1 + 8);

        put_len(&mut out, b.size);
        out.push(b.depth);
        put_len(&mut out, b.rc);
        out
    }
}

/// Deserializes a Block from the layout above, with an exact length check.
impl TryFrom<&[u8]> for Block {
    type Error = FsError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let mut r = Reader::new("Block", value);
        let size = r.len("size")?;
        let depth = r.u8("depth")?;
        let rc = r.len("rc")?;
        r.finish()?;

        Ok(Block { size, depth, rc })
    }
}

impl Block {
    /// Creates a new Block with the specified size and fanout depth,
    /// initializing the reference count to 1
    ///
    /// # Arguments
    /// * `size` - The size of the block data in bytes
    /// * `depth` - The fanout depth the block file was placed at
    ///
    /// # Returns
    /// A new Block instance with reference count set to 1
    pub fn new(size: usize, depth: u8) -> Self {
        Self { size, depth, rc: 1 }
    }

    /// Returns the size of the block data in bytes
    pub fn size(&self) -> usize {
        self.size
    }

    /// Returns the fanout depth the block file was placed at
    pub fn depth(&self) -> u8 {
        self.depth
    }

    /// Constructs the full filesystem path to the block from its recorded
    /// depth. GET and DELETE use this -- they never probe.
    ///
    /// # Arguments
    /// * `id` - The block's address (the tree key this record was read under)
    /// * `root` - The root directory where blocks are stored
    ///
    /// # Returns
    /// The complete filesystem path to the block
    pub fn disk_path(&self, id: &BlockId, root: PathBuf) -> PathBuf {
        block_disk_path(id, self.depth, root)
    }

    /// Returns the current reference count of the block
    pub fn rc(&self) -> usize {
        self.rc
    }

    /// Increments the reference count of the block
    ///
    /// This is called when a new object references this block
    pub fn increment_refcount(&mut self) {
        self.rc += 1
    }

    /// Decrements the reference count of the block
    ///
    /// This is called when an object that referenced this block is deleted
    pub fn decrement_refcount(&mut self) {
        self.rc -= 1
    }

    /// Serializes the block to a byte vector
    ///
    /// # Returns
    /// A vector of bytes representing the serialized block
    pub fn to_vec(&self) -> Vec<u8> {
        self.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equality_follows_significant_bytes() {
        let a = BlockId::from([7u8; BLOCKID_SIZE]);
        let b = BlockId::from_slice(&[7u8; BLOCKID_SIZE]).unwrap();
        assert_eq!(a, b);
        assert_eq!(a.as_slice(), b.as_slice());

        let mut other = [7u8; BLOCKID_SIZE];
        other[15] = 8;
        let c = BlockId::from(other);
        assert_ne!(a, c);
    }

    #[test]
    fn same_prefix_different_width_is_not_equal() {
        // A 32 byte id whose first 16 bytes match a 16 byte id, with the rest
        // zero: the padded arrays are identical, only the width differs.
        let narrow = BlockId::from([1u8; BLOCKID_SIZE]);
        let mut wide_bytes = [0u8; MAX_BLOCKID_SIZE];
        wide_bytes[..BLOCKID_SIZE].copy_from_slice(&[1u8; BLOCKID_SIZE]);
        let wide = BlockId::from(wide_bytes);

        assert_ne!(narrow, wide);
        assert_ne!(narrow.as_slice(), wide.as_slice());
        // Ordering agrees with the byte strings: the prefix sorts first.
        assert!(narrow < wide);
        assert!(narrow.as_slice() < wide.as_slice());
    }

    #[test]
    fn ordering_matches_slice_ordering() {
        let small = BlockId::from([0x01u8; BLOCKID_SIZE]);
        let big = BlockId::from([0xffu8; MAX_BLOCKID_SIZE]);
        assert!(small < big);
        assert!(small.as_slice() < big.as_slice());

        let wide_small = BlockId::from([0x01u8; MAX_BLOCKID_SIZE]);
        let narrow_big = BlockId::from([0xffu8; BLOCKID_SIZE]);
        assert!(wide_small < narrow_big);
        assert!(wide_small.as_slice() < narrow_big.as_slice());
    }

    #[test]
    fn padding_is_zero_for_both_widths() {
        let narrow = BlockId::from_slice(&[0xabu8; BLOCKID_SIZE]).unwrap();
        assert_eq!(narrow.len(), BLOCKID_SIZE);
        assert!(!narrow.is_empty());
        assert_eq!(narrow.as_slice(), &[0xabu8; BLOCKID_SIZE]);
        assert!(narrow.bytes[BLOCKID_SIZE..].iter().all(|b| *b == 0));

        let wide = BlockId::from_slice(&[0xcdu8; MAX_BLOCKID_SIZE]).unwrap();
        assert_eq!(wide.len(), MAX_BLOCKID_SIZE);
        assert_eq!(wide.as_slice(), &[0xcdu8; MAX_BLOCKID_SIZE]);

        // The From impl pads too.
        let from_array = BlockId::from([0xabu8; BLOCKID_SIZE]);
        assert_eq!(from_array, narrow);
        assert!(from_array.bytes[BLOCKID_SIZE..].iter().all(|b| *b == 0));
    }

    #[test]
    fn from_slice_rejects_other_widths() {
        for width in [0usize, 1, 15, 17, 31, 33, 64] {
            let err = BlockId::from_slice(&vec![0u8; width]).unwrap_err();
            let expected = u8::try_from(width).unwrap_or(u8::MAX);
            assert_eq!(err, FsError::InvalidIdWidth(expected));
        }
    }

    #[test]
    fn hex_rendering_covers_only_significant_bytes() {
        let narrow = BlockId::from([0u8; BLOCKID_SIZE]);
        assert_eq!(narrow.to_hex().len(), BLOCKID_SIZE * 2);
        let wide = BlockId::from([0u8; MAX_BLOCKID_SIZE]);
        assert_eq!(wide.to_hex().len(), MAX_BLOCKID_SIZE * 2);
    }

    /// Block at fanout depth 2 (format v2).
    #[rustfmt::skip]
    const GOLDEN_DEPTH_2: &[u8] = &[
        // size = 4096
        0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // depth = 2
        0x02,
        // rc = 3
        0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];

    /// Block at the deepest fanout a 32 byte id allows.
    #[rustfmt::skip]
    const GOLDEN_DEPTH_MAX: &[u8] = &[
        // size = 0x01020304
        0x04, 0x03, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00,
        // depth = 32
        0x20,
        // rc = 10
        0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];

    fn golden_blocks() -> Vec<(&'static str, Block, &'static [u8])> {
        vec![
            (
                "depth_2",
                Block {
                    size: 4096,
                    depth: 2,
                    rc: 3,
                },
                GOLDEN_DEPTH_2,
            ),
            (
                "depth_max",
                Block {
                    size: 0x0102_0304,
                    depth: MAX_BLOCKID_SIZE as u8,
                    rc: 10,
                },
                GOLDEN_DEPTH_MAX,
            ),
        ]
    }

    /// Format v2 pin: serialization must produce exactly these bytes.
    #[test]
    fn golden_serialization() {
        for (name, block, expected) in golden_blocks() {
            assert_eq!(block.to_vec(), expected, "golden mismatch for {name}");
        }
    }

    /// Format v2 pin: the same bytes must decode to the same fields.
    #[test]
    fn golden_deserialization() {
        for (name, block, expected) in golden_blocks() {
            let decoded = Block::try_from(expected).unwrap_or_else(|e| panic!("{name}: {e}"));
            assert_eq!(decoded.size(), block.size(), "{name} size");
            assert_eq!(decoded.depth(), block.depth(), "{name} depth");
            assert_eq!(decoded.rc(), block.rc(), "{name} rc");
        }
    }

    #[test]
    fn malformed_block_records() {
        // Truncated at every field boundary.
        for cut in [0usize, 1, 7, 8, 9, 16] {
            assert!(
                matches!(
                    Block::try_from(&GOLDEN_DEPTH_2[..cut]),
                    Err(FsError::Truncated {
                        record: "Block",
                        ..
                    })
                ),
                "expected Truncated when cut at {cut}"
            );
        }

        // One byte too many.
        let mut extra = GOLDEN_DEPTH_2.to_vec();
        extra.push(0);
        assert_eq!(
            Block::try_from(extra.as_slice()).unwrap_err(),
            FsError::TrailingBytes {
                record: "Block",
                extra: 1
            }
        );
    }

    /// The path scheme: dirs are single hex bytes of the id's prefix, the
    /// filename is the full-width hex of the id.
    #[test]
    fn disk_path_layout() {
        let mut bytes = [0u8; BLOCKID_SIZE];
        bytes[0] = 0xab;
        bytes[1] = 0x01;
        bytes[2] = 0xff;
        let id = BlockId::from(bytes);
        let hex = id.to_hex();

        let d1 = block_disk_path(&id, 1, PathBuf::from("/data/blocks"));
        assert_eq!(d1, PathBuf::from(format!("/data/blocks/ab/{hex}")));

        let d3 = block_disk_path(&id, 3, PathBuf::from("/data/blocks"));
        assert_eq!(d3, PathBuf::from(format!("/data/blocks/ab/01/ff/{hex}")));

        // Any depth locates the same block by name: only the dir chain moves.
        assert_eq!(d1.file_name(), d3.file_name());
    }

    /// A depth of 0 (which no writer produces) and a depth beyond the id
    /// width (a corrupt record) both clamp to a well-formed path instead of
    /// panicking or aliasing another block's path.
    #[test]
    fn disk_path_clamps_out_of_range_depths() {
        let id = BlockId::from([0x11u8; BLOCKID_SIZE]);
        let root = PathBuf::from("/r");

        assert_eq!(
            block_disk_path(&id, 0, root.clone()),
            block_disk_path(&id, 1, root.clone())
        );
        assert_eq!(
            block_disk_path(&id, u8::MAX, root.clone()),
            block_disk_path(&id, BLOCKID_SIZE as u8, root)
        );
    }
}
