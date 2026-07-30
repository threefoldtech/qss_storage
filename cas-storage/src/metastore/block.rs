use faster_hex::hex_string;
use std::{
    convert::{TryFrom, TryInto},
    path::PathBuf,
};

use super::{FsError, PTR_SIZE};

/// Size of a block identifier in bytes (16 bytes, equivalent to an MD5 hash)
pub const BLOCKID_SIZE: usize = 16;

/// Type alias for a block identifier, represented as a fixed-size byte array
///
/// BlockID is used throughout the system to uniquely identify data blocks
pub type BlockID = [u8; BLOCKID_SIZE];

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
            return Err(FsError::MalformedObject);
        }
        let size = usize::from_le_bytes(value[..PTR_SIZE].try_into().unwrap());

        let vec_size =
            u8::from_le_bytes(value[PTR_SIZE..PTR_SIZE + 1].try_into().unwrap()) as usize;
        if value.len() < PTR_SIZE + 1 + vec_size {
            return Err(FsError::MalformedObject);
        }
        let path = value[PTR_SIZE + 1..PTR_SIZE + 1 + vec_size].to_vec();

        if value.len() != PTR_SIZE * 2 + 1 + vec_size {
            return Err(FsError::MalformedObject);
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
