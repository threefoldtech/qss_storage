use std::{
    convert::{TryFrom, TryInto},
    time::SystemTime,
    time::UNIX_EPOCH,
};

use chrono::{SecondsFormat, TimeZone, Utc};
use faster_hex::hex_string;

use super::{BLOCKID_SIZE, BlockID, FsError, PTR_SIZE};

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
    /// Unique hash identifier for the object
    hash: BlockID,
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
        blocks: Vec<BlockID>,
    },

    /// The object is a multipart object, with blocks stored separately.
    ///
    /// Used for objects that are uploaded in multiple parts, typically for
    /// large objects or when resumable uploads are needed.
    MultiPart {
        /// References to the data blocks that make up the object
        blocks: Vec<BlockID>,
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
    /// * `hash` - Unique hash identifier for the object
    /// * `object_data` - The data storage strategy and content/references
    ///
    /// # Returns
    /// A new Object instance
    pub fn new(size: u64, hash: BlockID, object_data: ObjectData) -> Self {
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

    /// Returns the minimum size needed for inline metadata storage.
    ///
    /// This is used to determine if an object can be stored inline.
    ///
    /// # Returns
    /// The minimum number of bytes required for inline metadata
    pub fn minimum_inline_metadata_size() -> usize {
        minimum_raw_object_size() + PTR_SIZE // size of common fields + size of data_len field
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
            format!("{}-{}", hex_string(&self.hash), parts)
        } else {
            hex_string(&self.hash)
        }
    }

    /// Returns the unique hash identifier of the object.
    ///
    /// # Returns
    /// A reference to the object's BlockID (hash)
    pub fn hash(&self) -> &BlockID {
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
    /// A slice of BlockIDs
    pub fn blocks(&self) -> &[BlockID] {
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
    pub fn has_block(&self, block: &BlockID) -> bool {
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
        let mandatory_fields_size = 17 + BLOCKID_SIZE;
        match &self.data {
            ObjectData::SinglePart { blocks } => {
                mandatory_fields_size + PTR_SIZE + (blocks.len() * BLOCKID_SIZE)
            }
            ObjectData::MultiPart { blocks, .. } => {
                mandatory_fields_size + PTR_SIZE + (blocks.len() * BLOCKID_SIZE) + PTR_SIZE
            }
            ObjectData::Inline { data } => mandatory_fields_size + PTR_SIZE + data.len(),
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

/// Implements serialization of an Object to a byte vector.
///
/// The serialization format includes:
/// - 1 byte for object type
/// - 8 bytes for size
/// - 8 bytes for creation time
/// - BLOCKID_SIZE bytes for hash
/// - Variant-specific data based on the object type
impl From<&Object> for Vec<u8> {
    fn from(o: &Object) -> Self {
        let mut raw_data = Vec::with_capacity(o.num_bytes());

        // Write header fields
        raw_data.extend_from_slice(&o.object_type.as_u8().to_le_bytes());
        raw_data.extend_from_slice(&o.size.to_le_bytes());
        raw_data.extend_from_slice(&o.ctime.to_le_bytes());
        raw_data.extend_from_slice(&o.hash);

        // Write variant-specific data
        match &o.data {
            ObjectData::SinglePart { blocks } | ObjectData::MultiPart { blocks, .. } => {
                // Write blocks for both Single and MultiPart
                raw_data.extend_from_slice(&blocks.len().to_le_bytes());
                blocks
                    .iter()
                    .for_each(|block| raw_data.extend_from_slice(block));

                // Write parts count for MultiPart only
                if let ObjectData::MultiPart { parts, .. } = &o.data {
                    raw_data.extend_from_slice(&parts.to_le_bytes());
                }
            }
            ObjectData::Inline { data } => {
                raw_data.extend_from_slice(&(data.len() as u64).to_le_bytes());
                raw_data.extend_from_slice(data);
            }
        }

        raw_data
    }
}

/// Returns the minimum size required for the common fields of an Object.
///
/// This includes the object type, size, creation time, hash, and a pointer to data.
///
/// # Returns
/// The minimum number of bytes needed for the common fields
fn minimum_raw_object_size() -> usize {
    17 + BLOCKID_SIZE + PTR_SIZE
}

/// Implements deserialization of an Object from a byte slice.
///
/// This implementation validates the input format and extracts all object fields.
impl TryFrom<&[u8]> for Object {
    type Error = FsError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() < minimum_raw_object_size() {
            return Err(FsError::MalformedObject);
        }

        // object type: 1 byte
        let mut pos = 0;

        let object_type = u8::from_le_bytes(value[pos..pos + 1].try_into().unwrap());
        let object_type = match object_type {
            0 => ObjectType::Single,
            1 => ObjectType::Multipart,
            2 => ObjectType::Inline,
            _ => return Err(FsError::MalformedObject),
        };
        pos += 1;

        // size: 8 bytes
        let size = u64::from_le_bytes(value[pos..pos + 8].try_into().unwrap());
        pos += 8;

        // ctime: 8bytes
        let ctime = i64::from_le_bytes(value[pos..pos + 8].try_into().unwrap());
        pos += 8;

        // etag: BLOCKID_SIZE bytes
        let e_tag = value[pos..pos + BLOCKID_SIZE].try_into().unwrap();
        pos += BLOCKID_SIZE;

        let data = match object_type {
            ObjectType::Single | ObjectType::Multipart => {
                // block_len : PTR_SIZE bytes
                let block_len =
                    usize::from_le_bytes(value[pos..pos + PTR_SIZE].try_into().unwrap());
                pos += PTR_SIZE;

                // check the expected length
                let mut expected_len = pos + block_len * BLOCKID_SIZE;
                if object_type == ObjectType::Multipart {
                    expected_len += PTR_SIZE;
                }
                if value.len() != expected_len {
                    return Err(FsError::MalformedObject);
                }

                let mut blocks = Vec::with_capacity(block_len);

                // blocks: BLOCKID_SIZE * block_len bytes
                for chunk in value[pos..pos + (BLOCKID_SIZE * block_len)].chunks_exact(BLOCKID_SIZE)
                {
                    blocks.push(chunk.try_into().unwrap());
                }
                pos += BLOCKID_SIZE * block_len;

                if object_type == ObjectType::Single {
                    ObjectData::SinglePart { blocks }
                } else {
                    let parts =
                        usize::from_le_bytes(value[pos..pos + PTR_SIZE].try_into().unwrap());
                    ObjectData::MultiPart { blocks, parts }
                }
            }
            ObjectType::Inline => {
                // data_len: PTR_SIZE bytes
                let data_len = u64::from_le_bytes(value[pos..pos + PTR_SIZE].try_into().unwrap());
                pos += PTR_SIZE;

                // check the expected length
                let expected_len = pos + data_len as usize;
                if value.len() != expected_len {
                    return Err(FsError::MalformedObject);
                }

                // data: data_len bytes
                let data = value[pos..pos + data_len as usize].to_vec();
                ObjectData::Inline { data }
            }
        };
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

    fn create_test_objects() -> Vec<(ObjectType, Object)> {
        vec![
            (
                ObjectType::Single,
                Object::new(
                    1024,
                    [1; BLOCKID_SIZE],
                    ObjectData::SinglePart {
                        blocks: vec![[2; BLOCKID_SIZE], [3; BLOCKID_SIZE]],
                    },
                ),
            ),
            (
                ObjectType::Multipart,
                Object::new(
                    2048,
                    [4; BLOCKID_SIZE],
                    ObjectData::MultiPart {
                        blocks: vec![[5; BLOCKID_SIZE], [6; BLOCKID_SIZE]],
                        parts: 2,
                    },
                ),
            ),
            (
                ObjectType::Inline,
                Object::new(
                    5,
                    [7; BLOCKID_SIZE],
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
        // Test too short input
        assert!(matches!(
            Object::try_from(&[0u8; 15][..]),
            Err(FsError::MalformedObject)
        ));

        // Test invalid object type
        let mut bad_type = Vec::from(&create_test_objects()[0].1);
        bad_type[0] = 255;
        assert!(matches!(
            Object::try_from(bad_type.as_slice()),
            Err(FsError::MalformedObject)
        ));

        // Test incorrect length for blocks
        let mut bad_blocks = Vec::from(&create_test_objects()[0].1);
        bad_blocks.truncate(bad_blocks.len() - 1);
        assert!(matches!(
            Object::try_from(bad_blocks.as_slice()),
            Err(FsError::MalformedObject)
        ));
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
    }
}
