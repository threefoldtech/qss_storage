use std::{convert::TryFrom, time::SystemTime, time::UNIX_EPOCH};

use chrono::{SecondsFormat, TimeZone, Utc};

use super::{
    BlockId, CONTENT_HASH_SIZE, ContentHash, FsError,
    codec::{Reader, id_list_len, put_id_list, put_len},
};

/// Represents an object in the storage system with its metadata and content (for Inline objects).
///
/// An Object is the primary entity stored in the system and can be one of three types:
/// - Single part: A regular object with one or more blocks
/// - Multipart: An object composed of multiple parts uploaded separately
/// - Inline: A small object with its data stored directly in the metadata
///
/// Each object contains metadata such as size, creation time, and a unique hash,
/// along with either references to data blocks or the inline data itself.
#[derive(Debug)]
pub struct Object {
    /// The type of the object (Single, Multipart, or Inline)
    object_type: ObjectType,
    /// Total size of the object in bytes
    size: u64,
    /// Creation time as a Unix timestamp (seconds since epoch)
    ctime: i64,
    /// MD5 digest of the object's content (the S3 ETag source)
    hash: ContentHash,
    /// The actual data or references to data blocks
    data: ObjectData,
}

/// Represents the different ways object data can be stored.
///
/// This enum allows the system to handle different storage strategies
/// based on object size and upload method.
#[derive(Debug)]
pub enum ObjectData {
    /// The object is stored inline in the metadata.
    ///
    /// Used for small objects where it's more efficient to store the data
    /// directly in the metadata rather than as separate blocks.
    Inline {
        /// The actual object data
        data: Vec<u8>,
    },

    /// The object is a single part object, and the blocks are stored separately.
    ///
    /// Used for regular objects that are uploaded in a single operation.
    SinglePart {
        /// References to the data blocks that make up the object
        blocks: Vec<BlockId>,
    },

    /// The object is a multipart object, with blocks stored separately.
    ///
    /// Used for objects that are uploaded in multiple parts, typically for
    /// large objects or when resumable uploads are needed.
    MultiPart {
        /// References to the data blocks that make up the object
        blocks: Vec<BlockId>,
        /// The number of parts uploaded for this object
        /// Required for proper ETag calculation and verification
        parts: usize,
    },
}

/// Defines the type of an object in the storage system.
///
/// This enum is used to distinguish between different object storage strategies.
#[derive(Debug, PartialEq, Clone, Copy)]
#[repr(u8)]
pub enum ObjectType {
    /// A regular object uploaded in a single operation
    Single = 0,
    /// An object uploaded in multiple parts
    Multipart = 1,
    /// A small object with data stored directly in the metadata
    Inline = 2,
}

impl ObjectType {
    /// Converts the ObjectType to its u8 representation for serialization.
    ///
    /// # Returns
    /// The numeric representation of the object type
    fn as_u8(&self) -> u8 {
        match self {
            ObjectType::Single => 0,
            ObjectType::Multipart => 1,
            ObjectType::Inline => 2,
        }
    }
}

impl Object {
    /// Creates a new Object with the specified properties.
    ///
    /// The object_type is automatically determined based on the provided object_data.
    ///
    /// # Arguments
    /// * `size` - Total size of the object in bytes
    /// * `hash` - MD5 content digest of the object (the ETag source)
    /// * `object_data` - The data storage strategy and content/references
    ///
    /// # Returns
    /// A new Object instance
    pub fn new(size: u64, hash: ContentHash, object_data: ObjectData) -> Self {
        let object_type = match &object_data {
            ObjectData::SinglePart { .. } => ObjectType::Single,
            ObjectData::MultiPart { .. } => ObjectType::Multipart,
            ObjectData::Inline { .. } => ObjectType::Inline,
        };
        Self {
            object_type,
            size,
            ctime: Utc::now().timestamp(),
            hash,
            data: object_data,
        }
    }

    /// Returns the size of an Inline object record that carries no data.
    ///
    /// An Inline object is `header | data_len u64 | data`, so its record is
    /// this many bytes plus the payload. Subtracting it from the configured
    /// inlined metadata budget gives the largest payload that still fits --
    /// see `MetaStore::max_inlined_data_length`.
    ///
    /// # Returns
    /// The minimum number of bytes required for inline metadata (41)
    pub fn minimum_inline_metadata_size() -> usize {
        OBJECT_HEADER_SIZE + 8 // header + data_len field
    }

    /// Serializes the object to a byte vector.
    ///
    /// # Returns
    /// A vector of bytes representing the serialized object
    pub fn to_vec(&self) -> Vec<u8> {
        self.into()
    }

    /// Formats the object's ETag (Entity Tag) according to S3 conventions.
    ///
    /// For multipart objects, the ETag includes the part count.
    ///
    /// # Returns
    /// A formatted ETag string
    pub fn format_e_tag(&self) -> String {
        if let ObjectData::MultiPart { parts, .. } = &self.data {
            format!("{}-{}", self.hash.to_hex(), parts)
        } else {
            self.hash.to_hex()
        }
    }

    /// Returns the MD5 content digest of the object.
    ///
    /// # Returns
    /// A reference to the object's ContentHash (the ETag source)
    pub fn hash(&self) -> &ContentHash {
        &self.hash
    }

    /// Updates the object's creation time to the current time.
    ///
    /// This is typically used when an object is modified.
    pub fn touch(&mut self) {
        self.ctime = Utc::now().timestamp();
    }

    /// Returns the total size of the object in bytes.
    ///
    /// # Returns
    /// The object size
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Returns a slice of all block IDs that make up the object.
    ///
    /// For inline objects, this returns an empty slice.
    ///
    /// # Returns
    /// A slice of block ids
    pub fn blocks(&self) -> &[BlockId] {
        match &self.data {
            ObjectData::SinglePart { blocks } => blocks,
            ObjectData::MultiPart { blocks, .. } => blocks,
            ObjectData::Inline { .. } => &[],
        }
    }

    /// Checks if the object contains a specific block.
    ///
    /// # Arguments
    /// * `block` - The block ID to check for
    ///
    /// # Returns
    /// `true` if the object contains the block, `false` otherwise
    pub fn has_block(&self, block: &BlockId) -> bool {
        match &self.data {
            ObjectData::SinglePart { blocks } => blocks.contains(block),
            ObjectData::MultiPart { blocks, .. } => blocks.contains(block),
            ObjectData::Inline { .. } => false,
        }
    }

    /// Returns the last modification time of the object as a SystemTime.
    ///
    /// # Returns
    /// The last modification time
    pub fn last_modified(&self) -> SystemTime {
        UNIX_EPOCH + std::time::Duration::from_secs(self.ctime as u64)
    }

    /// Formats the creation time as an RFC3339 string.
    ///
    /// # Returns
    /// A formatted timestamp string
    pub fn format_ctime(&self) -> String {
        Utc.timestamp_opt(self.ctime, 0)
            .unwrap()
            .to_rfc3339_opts(SecondsFormat::Secs, true)
    }

    /// Calculates the number of bytes this object would take up in serialized form.
    ///
    /// This is used to allocate the right amount of memory for serialization.
    ///
    /// # Returns
    /// The number of bytes needed for serialization
    fn num_bytes(&self) -> usize {
        OBJECT_HEADER_SIZE
            + match &self.data {
                ObjectData::SinglePart { blocks } => id_list_len(blocks),
                // parts u64, then the id list.
                ObjectData::MultiPart { blocks, .. } => 8 + id_list_len(blocks),
                ObjectData::Inline { data } => 8 + data.len(),
            }
    }

    /// Checks if the object is stored inline.
    ///
    /// # Returns
    /// `true` if the object is stored inline, `false` otherwise
    pub fn is_inlined(&self) -> bool {
        matches!(&self.data, ObjectData::Inline { .. })
    }

    /// Returns the inline data if the object is stored inline.
    ///
    /// # Returns
    /// Some(&Vec<u8>) if the object is inline, None otherwise
    pub fn inlined(&self) -> Option<&Vec<u8>> {
        match &self.data {
            ObjectData::Inline { data } => Some(data),
            _ => None,
        }
    }

    /// Returns the object type.
    ///
    /// # Returns
    /// The ObjectType of this object
    pub fn object_type(&self) -> ObjectType {
        self.object_type
    }

    /// Returns a reference to the object data.
    ///
    /// # Returns
    /// A reference to the ObjectData enum
    pub fn data(&self) -> &ObjectData {
        &self.data
    }
}

/// Serializes an Object (format v1):
///
/// ```text
/// type u8 | size u64 | ctime i64 | hash [CONTENT_HASH_SIZE]
///   Inline:     data_len u64 | data
///   SinglePart: id_width u8 | count u64 | ids[count * id_width]
///   MultiPart:  parts u64 | id_width u8 | count u64 | ids
/// ```
impl From<&Object> for Vec<u8> {
    fn from(o: &Object) -> Self {
        let mut raw_data = Vec::with_capacity(o.num_bytes());

        // Write header fields
        raw_data.push(o.object_type.as_u8());
        raw_data.extend_from_slice(&o.size.to_le_bytes());
        raw_data.extend_from_slice(&o.ctime.to_le_bytes());
        raw_data.extend_from_slice(o.hash.as_slice());

        // Write variant-specific data
        match &o.data {
            ObjectData::SinglePart { blocks } => put_id_list(&mut raw_data, blocks),
            ObjectData::MultiPart { blocks, parts } => {
                put_len(&mut raw_data, *parts);
                put_id_list(&mut raw_data, blocks);
            }
            ObjectData::Inline { data } => {
                put_len(&mut raw_data, data.len());
                raw_data.extend_from_slice(data);
            }
        }

        raw_data
    }
}

/// Size of the fields every Object record starts with:
/// `type u8 | size u64 | ctime i64 | hash [CONTENT_HASH_SIZE]`.
const OBJECT_HEADER_SIZE: usize = 1 + 8 + 8 + CONTENT_HASH_SIZE;

/// Returns the length of the shortest valid Object record.
///
/// That is an Inline object with no data: the header plus an empty `data_len`
/// field (41 bytes). A SinglePart with an empty block list is one byte longer
/// (42: the header, the width byte and the count), so 41 is the floor for the
/// record as a whole.
fn minimum_raw_object_size() -> usize {
    OBJECT_HEADER_SIZE + 8
}

/// Deserializes an Object from the layout above, with an exact length check.
impl TryFrom<&[u8]> for Object {
    type Error = FsError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() < minimum_raw_object_size() {
            return Err(FsError::Truncated {
                record: "Object",
                needed: minimum_raw_object_size(),
                got: value.len(),
            });
        }

        let mut r = Reader::new("Object", value);

        let object_type = match r.u8("object_type")? {
            0 => ObjectType::Single,
            1 => ObjectType::Multipart,
            2 => ObjectType::Inline,
            t => return Err(FsError::UnknownObjectType(t)),
        };
        let size = r.u64("size")?;
        let ctime = r.i64("ctime")?;
        let e_tag = ContentHash(r.array::<CONTENT_HASH_SIZE>("hash")?);

        let data = match object_type {
            ObjectType::Single => ObjectData::SinglePart {
                blocks: r.id_list()?,
            },
            ObjectType::Multipart => {
                let parts = r.len("parts")?;
                let blocks = r.id_list()?;
                ObjectData::MultiPart { blocks, parts }
            }
            ObjectType::Inline => {
                let data_len = r.len("data_len")?;
                ObjectData::Inline {
                    data: r.bytes("data", data_len)?.to_vec(),
                }
            }
        };
        r.finish()?;

        Ok(Self {
            object_type,
            size,
            ctime,
            hash: e_tag,
            data,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metastore::{BLOCKID_SIZE, MAX_BLOCKID_SIZE};

    /// Fixed ctime for the golden vectors: an asymmetric value, so a record
    /// written big-endian by mistake could not pass.
    const CTIME: i64 = 0x0102_0304_0506_0708;

    /// Object, Inline with 5 bytes of data (format v1).
    #[rustfmt::skip]
    const GOLDEN_INLINE: &[u8] = &[
        // type = Inline (2)
        0x02,
        // size = 5
        0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // ctime = 0x0102030405060708
        0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
        // hash (16 bytes)
        0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77,
        0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77,
        // data_len = 5
        0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // data
        0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
    ];

    /// Object, SinglePart with two 16 byte block ids.
    #[rustfmt::skip]
    const GOLDEN_SINGLE_W16: &[u8] = &[
        // type = Single (0)
        0x00,
        // size = 1024
        0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // ctime
        0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
        // hash (16 bytes)
        0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,
        0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,
        // id_width = 16
        0x10,
        // count = 2
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // ids
        0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22,
        0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22,
        0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33,
        0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33,
    ];

    /// Offsets into `GOLDEN_SINGLE_W16`, used to build malformed variants.
    const SINGLE_W16_WIDTH_AT: usize = 33;
    const SINGLE_W16_COUNT_AT: usize = 34;

    /// Object, SinglePart with one 32 byte block id.
    #[rustfmt::skip]
    const GOLDEN_SINGLE_W32: &[u8] = &[
        // type = Single (0)
        0x00,
        // size = 2
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // ctime
        0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
        // hash (16 bytes)
        0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,
        0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,
        // id_width = 32
        0x20,
        // count = 1
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // ids
        0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
        0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
        0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
        0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
    ];

    /// Object, SinglePart with an empty block list: width 0, count 0. This is
    /// also the smallest SinglePart record (42 bytes).
    #[rustfmt::skip]
    const GOLDEN_SINGLE_EMPTY: &[u8] = &[
        // type = Single (0)
        0x00,
        // size = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // ctime
        0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
        // hash (16 bytes)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // id_width = 0 (legal only for an empty list)
        0x00,
        // count = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];

    /// Object, MultiPart with two 16 byte block ids. `parts` precedes the list.
    #[rustfmt::skip]
    const GOLDEN_MULTI_W16: &[u8] = &[
        // type = Multipart (1)
        0x01,
        // size = 8192
        0x00, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // ctime
        0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
        // hash (16 bytes)
        0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
        0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
        // parts = 2
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // id_width = 16
        0x10,
        // count = 2
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // ids
        0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66,
        0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66,
        0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77,
        0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77,
    ];

    /// Object, MultiPart with one 32 byte block id.
    #[rustfmt::skip]
    const GOLDEN_MULTI_W32: &[u8] = &[
        // type = Multipart (1)
        0x01,
        // size = 1
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // ctime
        0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
        // hash (16 bytes)
        0x99, 0x99, 0x99, 0x99, 0x99, 0x99, 0x99, 0x99,
        0x99, 0x99, 0x99, 0x99, 0x99, 0x99, 0x99, 0x99,
        // parts = 7
        0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // id_width = 32
        0x20,
        // count = 1
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // ids
        0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88,
        0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88,
        0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88,
        0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88,
    ];

    fn object(size: u64, hash: u8, data: ObjectData) -> Object {
        Object {
            object_type: match &data {
                ObjectData::SinglePart { .. } => ObjectType::Single,
                ObjectData::MultiPart { .. } => ObjectType::Multipart,
                ObjectData::Inline { .. } => ObjectType::Inline,
            },
            size,
            ctime: CTIME,
            hash: ContentHash([hash; CONTENT_HASH_SIZE]),
            data,
        }
    }

    /// The golden fixtures, paired with the bytes they must serialize to.
    fn golden_objects() -> Vec<(&'static str, Object, &'static [u8])> {
        vec![
            (
                "inline",
                object(
                    5,
                    0x77,
                    ObjectData::Inline {
                        data: vec![0x0a, 0x0b, 0x0c, 0x0d, 0x0e],
                    },
                ),
                GOLDEN_INLINE,
            ),
            (
                "single_w16",
                object(
                    1024,
                    0x11,
                    ObjectData::SinglePart {
                        blocks: vec![
                            BlockId::from([0x22; BLOCKID_SIZE]),
                            BlockId::from([0x33; BLOCKID_SIZE]),
                        ],
                    },
                ),
                GOLDEN_SINGLE_W16,
            ),
            (
                "single_w32",
                object(
                    2,
                    0x11,
                    ObjectData::SinglePart {
                        blocks: vec![BlockId::from([0x44; MAX_BLOCKID_SIZE])],
                    },
                ),
                GOLDEN_SINGLE_W32,
            ),
            (
                "single_empty",
                object(0, 0x00, ObjectData::SinglePart { blocks: vec![] }),
                GOLDEN_SINGLE_EMPTY,
            ),
            (
                "multi_w16",
                object(
                    8192,
                    0x55,
                    ObjectData::MultiPart {
                        blocks: vec![
                            BlockId::from([0x66; BLOCKID_SIZE]),
                            BlockId::from([0x77; BLOCKID_SIZE]),
                        ],
                        parts: 2,
                    },
                ),
                GOLDEN_MULTI_W16,
            ),
            (
                "multi_w32",
                object(
                    1,
                    0x99,
                    ObjectData::MultiPart {
                        blocks: vec![BlockId::from([0x88; MAX_BLOCKID_SIZE])],
                        parts: 7,
                    },
                ),
                GOLDEN_MULTI_W32,
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
        for (name, obj, expected) in golden_objects() {
            assert_eq!(obj.to_vec(), expected, "golden mismatch for {name}");
        }
    }

    /// Format v1 pin: the same bytes must decode to the same fields.
    #[test]
    fn golden_deserialization() {
        for (name, obj, expected) in golden_objects() {
            let decoded = Object::try_from(expected).unwrap_or_else(|e| panic!("{name}: {e}"));
            assert_eq!(decoded.object_type, obj.object_type, "{name} type");
            assert_eq!(decoded.size, obj.size, "{name} size");
            assert_eq!(decoded.ctime, CTIME, "{name} ctime");
            assert_eq!(decoded.hash, obj.hash, "{name} hash");
            match (&obj.data, &decoded.data) {
                (ObjectData::Inline { data: a }, ObjectData::Inline { data: b }) => {
                    assert_eq!(a, b, "{name} data")
                }
                (ObjectData::SinglePart { blocks: a }, ObjectData::SinglePart { blocks: b }) => {
                    assert_eq!(a, b, "{name} blocks")
                }
                (
                    ObjectData::MultiPart {
                        blocks: a,
                        parts: pa,
                    },
                    ObjectData::MultiPart {
                        blocks: b,
                        parts: pb,
                    },
                ) => {
                    assert_eq!(a, b, "{name} blocks");
                    assert_eq!(pa, pb, "{name} parts");
                }
                _ => panic!("{name}: variant mismatch after deserialization"),
            }
        }
    }

    fn create_test_objects() -> Vec<(ObjectType, Object)> {
        vec![
            (
                ObjectType::Single,
                Object::new(
                    1024,
                    ContentHash([1; CONTENT_HASH_SIZE]),
                    ObjectData::SinglePart {
                        blocks: vec![
                            BlockId::from([2; BLOCKID_SIZE]),
                            BlockId::from([3; BLOCKID_SIZE]),
                        ],
                    },
                ),
            ),
            (
                ObjectType::Multipart,
                Object::new(
                    2048,
                    ContentHash([4; CONTENT_HASH_SIZE]),
                    ObjectData::MultiPart {
                        blocks: vec![
                            BlockId::from([5; MAX_BLOCKID_SIZE]),
                            BlockId::from([6; MAX_BLOCKID_SIZE]),
                        ],
                        parts: 2,
                    },
                ),
            ),
            (
                ObjectType::Inline,
                Object::new(
                    5,
                    ContentHash([7; CONTENT_HASH_SIZE]),
                    ObjectData::Inline {
                        data: vec![1, 2, 3, 4, 5],
                    },
                ),
            ),
        ]
    }

    #[test]
    fn test_object_serialization() {
        for (expected_type, obj) in create_test_objects() {
            let serialized: Vec<u8> = (&obj).into();
            assert!(serialized.len() >= minimum_raw_object_size());
            assert_eq!(serialized[0], expected_type as u8);
        }
    }

    #[test]
    fn test_object_deserialization() {
        for (expected_type, obj) in create_test_objects() {
            let serialized: Vec<u8> = (&obj).into();
            let deserialized = Object::try_from(serialized.as_slice()).unwrap();

            assert_eq!(deserialized.object_type, expected_type);
            assert_eq!(deserialized.size, obj.size);
            assert_eq!(deserialized.ctime, obj.ctime);
            assert_eq!(deserialized.hash, obj.hash);

            match (obj.data, deserialized.data) {
                (ObjectData::SinglePart { blocks: b1 }, ObjectData::SinglePart { blocks: b2 }) => {
                    assert_eq!(b1, b2);
                }
                (
                    ObjectData::MultiPart {
                        blocks: b1,
                        parts: p1,
                    },
                    ObjectData::MultiPart {
                        blocks: b2,
                        parts: p2,
                    },
                ) => {
                    assert_eq!(b1, b2);
                    assert_eq!(p1, p2);
                }
                (ObjectData::Inline { data: d1 }, ObjectData::Inline { data: d2 }) => {
                    assert_eq!(d1, d2);
                }
                _ => panic!("Object type mismatch after deserialization"),
            }
        }
    }

    #[test]
    fn test_malformed_input() {
        // Shorter than the smallest possible record.
        assert!(matches!(
            Object::try_from(&[0u8; 15][..]),
            Err(FsError::Truncated {
                record: "Object",
                got: 15,
                ..
            })
        ));
        assert!(matches!(
            Object::try_from(&[2u8; 40][..]),
            Err(FsError::Truncated {
                record: "Object",
                needed: 41,
                got: 40,
            })
        ));

        // Unknown type discriminants.
        for bad in [3u8, 255] {
            assert_eq!(
                Object::try_from(patched(GOLDEN_SINGLE_W16, 0, &[bad]).as_slice()).unwrap_err(),
                FsError::UnknownObjectType(bad)
            );
        }

        // Truncated at every field boundary of a SinglePart record.
        for cut in [1usize, 9, 17, 33, 34, 42, 57, 73] {
            assert!(
                matches!(
                    Object::try_from(&GOLDEN_SINGLE_W16[..cut]),
                    Err(FsError::Truncated {
                        record: "Object",
                        ..
                    })
                ),
                "expected Truncated when cut at {cut}"
            );
        }

        // Truncated in the tail of the other two variants.
        for golden in [GOLDEN_INLINE, GOLDEN_MULTI_W16] {
            assert!(matches!(
                Object::try_from(&golden[..golden.len() - 1]),
                Err(FsError::Truncated {
                    record: "Object",
                    ..
                })
            ));
        }

        // One byte too many, for every variant.
        for golden in [
            GOLDEN_INLINE,
            GOLDEN_SINGLE_W16,
            GOLDEN_SINGLE_EMPTY,
            GOLDEN_MULTI_W16,
        ] {
            let mut extra = golden.to_vec();
            extra.push(0);
            assert_eq!(
                Object::try_from(extra.as_slice()).unwrap_err(),
                FsError::TrailingBytes {
                    record: "Object",
                    extra: 1
                }
            );
        }

        // An id width that is neither 16 nor 32.
        for bad in [1u8, 15, 17, 31, 33, 255] {
            let raw = patched(GOLDEN_SINGLE_W16, SINGLE_W16_WIDTH_AT, &[bad]);
            assert_eq!(
                Object::try_from(raw.as_slice()).unwrap_err(),
                FsError::InvalidIdWidth(bad)
            );
        }

        // Width 0 is legal only for an empty list.
        let raw = patched(GOLDEN_SINGLE_W16, SINGLE_W16_WIDTH_AT, &[0]);
        assert_eq!(
            Object::try_from(raw.as_slice()).unwrap_err(),
            FsError::InvalidIdWidth(0)
        );
        let raw = patched(GOLDEN_SINGLE_EMPTY, SINGLE_W16_WIDTH_AT, &[0]);
        let raw = patched(&raw, SINGLE_W16_COUNT_AT, &3u64.to_le_bytes());
        assert_eq!(
            Object::try_from(raw.as_slice()).unwrap_err(),
            FsError::InvalidIdWidth(0)
        );

        // A count nothing could satisfy must not overflow the offset maths.
        // The value does not fit a 32 bit usize and overflows the derived
        // total on a 64 bit one, so it is reported either way.
        let raw = patched(
            GOLDEN_SINGLE_W16,
            SINGLE_W16_COUNT_AT,
            &u64::MAX.to_le_bytes(),
        );
        assert_eq!(
            Object::try_from(raw.as_slice()).unwrap_err(),
            FsError::LengthOverflow {
                record: "Object",
                field: "id_count"
            }
        );

        // Same for an inline data length.
        let raw = patched(GOLDEN_INLINE, 33, &u64::MAX.to_le_bytes());
        assert_eq!(
            Object::try_from(raw.as_slice()).unwrap_err(),
            FsError::LengthOverflow {
                record: "Object",
                field: "data"
            }
        );

        // `parts` describes nothing on disk, so an absurd value is not a
        // decode error on a 64 bit host -- but it must not panic, and on a
        // host where it does not fit a usize it must be reported.
        let raw = patched(GOLDEN_MULTI_W16, 33, &u64::MAX.to_le_bytes());
        match Object::try_from(raw.as_slice()) {
            Ok(obj) => assert!(matches!(
                obj.data,
                ObjectData::MultiPart { parts, .. } if parts == usize::MAX
            )),
            Err(e) => assert!(matches!(
                e,
                FsError::LengthOverflow {
                    record: "Object",
                    field: "parts"
                }
            )),
        }
    }

    #[test]
    fn test_size_calculation() {
        for (_, obj) in create_test_objects() {
            let serialized: Vec<u8> = (&obj).into();
            assert_eq!(
                serialized.len(),
                obj.num_bytes(),
                "Size mismatch for {:?} object: expected {}, got {}",
                obj.object_type,
                obj.num_bytes(),
                serialized.len()
            );
        }
        for (name, obj, expected) in golden_objects() {
            assert_eq!(obj.num_bytes(), expected.len(), "num_bytes for {name}");
        }
    }

    /// The two size floors the inline threshold is built on.
    #[test]
    fn minimum_sizes_match_the_layout() {
        // header (1 + 8 + 8 + 16) + data_len (8)
        assert_eq!(Object::minimum_inline_metadata_size(), 41);
        // The shortest record overall is an Inline object with no data.
        assert_eq!(minimum_raw_object_size(), 41);
        assert_eq!(GOLDEN_INLINE.len(), 41 + 5);
        // A SinglePart with an empty list is one byte longer: it carries a
        // width byte and an 8 byte count instead of an 8 byte data length.
        assert_eq!(GOLDEN_SINGLE_EMPTY.len(), 42);
    }
}
