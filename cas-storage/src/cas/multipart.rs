use std::{
    convert::{TryFrom, TryInto},
    sync::Arc,
};

use crate::metastore::{BLOCKID_SIZE, BaseMetaTree, BlockID, FsError, MetaError, PTR_SIZE};

#[derive(Debug)]
pub struct MultiPart {
    size: usize,
    part_number: i64,
    bucket: String,
    key: String,
    upload_id: String,
    hash: BlockID,
    blocks: Vec<BlockID>,
}

impl MultiPart {
    pub fn new(
        size: usize,
        part_number: i64,
        bucket: String,
        key: String,
        upload_id: String,
        hash: BlockID,
        blocks: Vec<BlockID>,
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

    pub fn blocks(&self) -> &[BlockID] {
        &self.blocks
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.into()
    }
}

impl From<&MultiPart> for Vec<u8> {
    fn from(mp: &MultiPart) -> Self {
        let mut out = Vec::with_capacity(
            5 * PTR_SIZE
                + 8
                + mp.bucket.len()
                + mp.key.len()
                + mp.upload_id.len()
                + (1 + mp.blocks.len()) * BLOCKID_SIZE,
        );

        out.extend_from_slice(&mp.size.to_le_bytes());
        out.extend_from_slice(&mp.part_number.to_le_bytes());
        out.extend_from_slice(&mp.bucket.len().to_le_bytes());
        out.extend_from_slice(mp.bucket.as_bytes());
        out.extend_from_slice(&mp.key.len().to_le_bytes());
        out.extend_from_slice(mp.key.as_bytes());
        out.extend_from_slice(&mp.upload_id.len().to_le_bytes());
        out.extend_from_slice(mp.upload_id.as_bytes());
        out.extend_from_slice(&mp.hash);
        out.extend_from_slice(&mp.blocks.len().to_le_bytes());
        for block in &mp.blocks {
            out.extend_from_slice(block);
        }

        out
    }
}

impl TryFrom<&[u8]> for MultiPart {
    type Error = FsError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() < 5 * PTR_SIZE + 8 + BLOCKID_SIZE {
            return Err(FsError::MalformedObject);
        }

        let bucket_len =
            usize::from_le_bytes(value[8 + PTR_SIZE..8 + 2 * PTR_SIZE].try_into().unwrap());
        if value.len() < 8 + 3 * PTR_SIZE + bucket_len {
            return Err(FsError::MalformedObject);
        }
        // ---- tfstor-extension: BEGIN ----
        // Upstream used `String::from_utf8_unchecked` for the three string fields
        // below, on the grounds that only valid strings are ever inserted. That
        // argument covers the write path, not the read path: these bytes come
        // back off disk, where corruption, a truncated write or a format change
        // breaks the invariant, and the penalty for breaking it is undefined
        // behaviour rather than an error. Bucket names, keys and upload IDs are
        // short, so validation costs nothing worth having.
        let bucket =
            String::from_utf8(value[8 + 2 * PTR_SIZE..8 + 2 * PTR_SIZE + bucket_len].to_vec())
                .map_err(|_| FsError::MalformedObject)?;
        // ---- tfstor-extension: END ----

        let key_len = usize::from_le_bytes(
            value[8 + 2 * PTR_SIZE + bucket_len..8 + 3 * PTR_SIZE + bucket_len]
                .try_into()
                .unwrap(),
        );
        if value.len() < 8 + 4 * PTR_SIZE + bucket_len + key_len {
            return Err(FsError::MalformedObject);
        }
        // ---- tfstor-extension: BEGIN ----
        let key = String::from_utf8(
            value[8 + 3 * PTR_SIZE + bucket_len..8 + 3 * PTR_SIZE + bucket_len + key_len].to_vec(),
        )
        .map_err(|_| FsError::MalformedObject)?;
        // ---- tfstor-extension: END ----

        let upload_id_len = usize::from_le_bytes(
            value[8 + 3 * PTR_SIZE + bucket_len + key_len..8 + 4 * PTR_SIZE + bucket_len + key_len]
                .try_into()
                .unwrap(),
        );
        if value.len() < 8 + 5 * PTR_SIZE + bucket_len + key_len + upload_id_len + BLOCKID_SIZE {
            return Err(FsError::MalformedObject);
        }
        // ---- tfstor-extension: BEGIN ----
        let upload_id = String::from_utf8(
            value[8 + 4 * PTR_SIZE + bucket_len + key_len
                ..8 + 4 * PTR_SIZE + bucket_len + key_len + upload_id_len]
                .to_vec(),
        )
        .map_err(|_| FsError::MalformedObject)?;
        // ---- tfstor-extension: END ----

        let block_len = usize::from_le_bytes(
            value[8 + 4 * PTR_SIZE + bucket_len + key_len + upload_id_len + BLOCKID_SIZE
                ..8 + 5 * PTR_SIZE + bucket_len + key_len + upload_id_len + BLOCKID_SIZE]
                .try_into()
                .unwrap(),
        );
        if value.len()
            < 8 + 5 * PTR_SIZE
                + bucket_len
                + key_len
                + upload_id_len
                + (1 + block_len) * BLOCKID_SIZE
        {
            return Err(FsError::MalformedObject);
        }
        let mut blocks = Vec::with_capacity(block_len);
        for chunk in value[8 + 5 * PTR_SIZE + bucket_len + key_len + upload_id_len + BLOCKID_SIZE..]
            .chunks_exact(BLOCKID_SIZE)
        {
            blocks.push(chunk.try_into().unwrap());
        }

        Ok(MultiPart {
            size: usize::from_le_bytes(value[..PTR_SIZE].try_into().unwrap()),
            part_number: i64::from_le_bytes(value[PTR_SIZE..8 + PTR_SIZE].try_into().unwrap()),
            bucket,
            key,
            upload_id,
            hash: value[8 + 4 * PTR_SIZE + bucket_len + key_len + upload_id_len
                ..8 + 4 * PTR_SIZE + bucket_len + key_len + upload_id_len + BLOCKID_SIZE]
                .try_into()
                .unwrap(),
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
        let mp = MultiPart::try_from(value.as_ref()).expect("Corrupted multipart data");
        Ok(Some(mp))
    }
}
