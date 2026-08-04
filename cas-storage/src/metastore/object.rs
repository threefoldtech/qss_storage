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
mod tests;
