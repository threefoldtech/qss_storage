//! The record walk: every block record in `_BLOCKS`, decoded.
//!
//! Unlike the holder walk this one reports rather than refuses. The
//! difference is what an undecodable record costs: a holder record that will
//! not decode hides references, and a recount that cannot see a reference
//! can free live data. A block record that will not decode hides only
//! itself -- the block it names is already unusable, and every holder of it
//! is still counted by the holder walk. So it is a CRITICAL finding, and the
//! walk goes on.

use std::collections::HashMap;

use crate::metastore::{Block, BlockId, MetaError};

use super::ScrubContext;
use super::findings::{Finding, FindingClass};

/// What the record walk found: the records it could decode, and a finding
/// for each one it could not.
#[derive(Debug, Default)]
pub struct RecordWalk {
    /// Every decodable record, by block id.
    pub records: HashMap<BlockId, Block>,
    /// One CRITICAL finding per undecodable record.
    pub findings: Vec<Finding>,
}

/// Walks `_BLOCKS` end to end.
///
/// Reads the tree raw and decodes here, so a failure names the record it
/// came from: the typed iterator folds the key away on error.
///
/// # Errors
///
/// [`MetaError`] if the tree will not open, or if a read step fails. A
/// backend that cannot be read is not a finding -- there is no trustworthy
/// record set to report on.
pub fn walk_records(ctx: &ScrubContext) -> Result<RecordWalk, MetaError> {
    let tree = ctx.shared().block_tree();
    let mut walk = RecordWalk::default();

    for item in tree.iter_raw() {
        let (key, raw) = item?;

        let id = match BlockId::from_slice(&key) {
            Ok(id) => id,
            Err(e) => {
                // The key is the address; a key of the wrong width is a
                // record that names no block this store can address.
                walk.findings.push(Finding::new(
                    FindingClass::UndecodableBlockRecord,
                    format!(
                        "block key {} is not an address of this store: {e}",
                        faster_hex::hex_string(&key)
                    ),
                ));
                continue;
            }
        };

        match Block::try_from(&*raw) {
            Ok(block) => {
                walk.records.insert(id, block);
            }
            Err(e) => {
                walk.findings.push(
                    Finding::new(
                        FindingClass::UndecodableBlockRecord,
                        format!("block record does not decode: {e}"),
                    )
                    .with_block(&id),
                );
            }
        }
    }

    Ok(walk)
}
