use std::fmt::Debug;

use super::{Block, BlockTree, MetaError};
use crate::metastore::BlockId;

impl Debug for BlockTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockTree").finish()
    }
}

impl BlockTree {
    /// Retrieves a Block object for the given key.
    ///
    /// This method deserializes the raw block data into a Block struct.
    ///
    /// # Arguments
    /// * `key` - The key (typically a block hash) to look up
    ///
    /// # Returns
    /// The Block if found, None if the key doesn't exist, or an error
    pub fn get_block(&self, key: &[u8]) -> Result<Option<Block>, MetaError> {
        match self.tree.get(key)? {
            Some(data) => {
                let block = Block::try_from(&*data)?;
                Ok(Some(block))
            }
            None => Ok(None),
        }
    }

    /// Returns the number of blocks in the tree.
    ///
    /// This method is only available in test builds.
    ///
    /// # Returns
    /// The number of blocks or an error
    #[cfg(test)]
    pub fn len(&self) -> Result<usize, MetaError> {
        self.tree.len()
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> Result<bool, MetaError> {
        self.len().map(|n| n == 0)
    }

    /// Returns an iterator over the tree's raw, undecoded records.
    ///
    /// fsck's record walker needs this rather than [`Self::iter_all`]: that
    /// one folds a decode failure into an error which no longer names the
    /// record it came from, and a finding that cannot name the damaged
    /// record is not actionable (ADR 0005).
    ///
    /// # Returns
    /// An iterator yielding raw (key, value) pairs
    pub fn iter_raw(&self) -> crate::metastore::KeyValuePairs {
        self.tree.iter_all()
    }

    /// Returns an iterator over all blocks in the tree.
    ///
    /// # Returns
    /// An iterator yielding (BlockId, Block) tuples
    pub fn iter_all(&self) -> Box<dyn Iterator<Item = Result<(BlockId, Block), MetaError>> + '_> {
        Box::new(self.tree.iter_all().map(|result| match result {
            Ok((key, value)) => {
                // The key *is* the block address, at whatever width the store
                // wrote it (16 or 32 bytes); anything else is a foreign key in
                // the block tree.
                let block_id = BlockId::from_slice(&key)
                    .map_err(|e| MetaError::OtherDBError(format!("Malformed block key: {e}")))?;
                // Deserialize the block
                Block::try_from(&*value)
                    .map(|block| (block_id, block))
                    .map_err(MetaError::from)
            }
            Err(e) => Err(e),
        }))
    }
}
