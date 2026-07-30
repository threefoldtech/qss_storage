use faster_hex::hex_string;
use std::{
    convert::{TryFrom, TryInto},
    path::PathBuf,
};

use super::{FsError, PTR_SIZE};

/// Size of a block identifier in bytes (16 bytes, equivalent to an MD5 hash)
///
/// This is the width every block address is built at today. The width becomes
/// a per-store runtime choice later, at which point this constant disappears;
/// `BlockId` already carries its own width so that change does not ripple.
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
    fn from(bytes: [u8; MAX_BLOCKID_SIZE]) -> Self {
        Self {
            bytes,
            len: MAX_BLOCKID_SIZE as u8,
        }
    }
}

/// `Block` represents metadata about a stored data block in the content-addressable storage system.
///
/// Each Block contains:
/// - The size of the actual data
/// - A path to locate the block in the storage hierarchy
/// - A reference count (rc) tracking how many objects reference this block
///
/// The path is stored as a variable-length byte array, which could be optimized in the future.
// TODO: this can be optimized by making path a `[u8;BLOCKID_SIZE]` and keeping track of a len u8
#[derive(Debug)]
pub struct Block {
    /// Size of the block data in bytes
    size: usize,
    /// Path to the block in the storage hierarchy
    path: Vec<u8>,
    /// Reference count - how many objects reference this block
    rc: usize,
}

/// Implements serialization of a Block to a byte vector
impl From<&Block> for Vec<u8> {
    fn from(b: &Block) -> Self {
        // NOTE: we encode the lenght of the vector as a single byte, since it can only be 16 bytes
        // long.
        let mut out = Vec::with_capacity(2 * PTR_SIZE + b.path.len() + 1);

        out.extend_from_slice(&b.size.to_le_bytes());
        out.extend_from_slice(&(b.path.len() as u8).to_le_bytes());
        out.extend_from_slice(&b.path);
        out.extend_from_slice(&b.rc.to_le_bytes());
        out
    }
}

/// Implements deserialization of a Block from a byte slice
impl TryFrom<&[u8]> for Block {
    type Error = FsError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() < PTR_SIZE + 1 {
            return Err(FsError::Truncated {
                record: "Block",
                needed: PTR_SIZE + 1,
                got: value.len(),
            });
        }
        let size = usize::from_le_bytes(value[..PTR_SIZE].try_into().unwrap());

        let vec_size =
            u8::from_le_bytes(value[PTR_SIZE..PTR_SIZE + 1].try_into().unwrap()) as usize;
        if value.len() < PTR_SIZE + 1 + vec_size {
            return Err(FsError::Truncated {
                record: "Block",
                needed: PTR_SIZE + 1 + vec_size,
                got: value.len(),
            });
        }
        let path = value[PTR_SIZE + 1..PTR_SIZE + 1 + vec_size].to_vec();

        if value.len() != PTR_SIZE * 2 + 1 + vec_size {
            return Err(FsError::TrailingBytes {
                record: "Block",
                extra: value.len().abs_diff(PTR_SIZE * 2 + 1 + vec_size),
            });
        }

        Ok(Block {
            size,
            path,
            rc: usize::from_le_bytes(value[PTR_SIZE + 1 + vec_size..].try_into().unwrap()),
        })
    }
}

impl Block {
    /// Creates a new Block with the specified size and path, initializing the reference count to 1
    ///
    /// # Arguments
    /// * `size` - The size of the block data in bytes
    /// * `path` - The path to locate the block in the storage hierarchy
    ///
    /// # Returns
    /// A new Block instance with reference count set to 1
    pub fn new(size: usize, path: Vec<u8>) -> Self {
        Self { size, path, rc: 1 }
    }

    /// Returns the size of the block data in bytes
    pub fn size(&self) -> usize {
        self.size
    }

    /// Returns a reference to the path of the block
    pub fn path(&self) -> &[u8] {
        &self.path
    }

    /// Constructs the full filesystem path to the block
    ///
    /// This method converts the internal path representation to a filesystem path
    /// by creating a directory hierarchy based on the block's path bytes.
    ///
    /// # Arguments
    /// * `root` - The root directory where blocks are stored
    ///
    /// # Returns
    /// The complete filesystem path to the block
    pub fn disk_path(&self, mut root: PathBuf) -> PathBuf {
        // path has at least len 1
        let dirs = &self.path[..self.path.len() - 1];
        for byte in dirs {
            root.push(hex_string(&[*byte]));
        }
        root.push(format!(
            "_{}",
            hex_string(&[self.path[self.path.len() - 1]])
        ));
        root
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
}
