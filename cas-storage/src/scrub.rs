//! Offline scrub of a store: the walkers and the findings model of ADR 0005.
//!
//! Library code, all synchronous, no policy. Three walkers answer the three
//! questions a reconciliation needs, each from one source of truth:
//!
//! - [`holders`]: who references which block, per occurrence (the holder
//!   records);
//! - [`records`]: what `_BLOCKS` says (the block records);
//! - [`disk`]: what is actually on disk (the block files).
//!
//! Comparing those three is the passes' job (ADR 0005 component 4), not
//! this module's. What lives here is the enumeration, and one rule it
//! enforces on its own: the holder walk refuses on any incompleteness,
//! because a recount over a partial holder set that then repairs frees live
//! blocks. Everything else reports.
//!
//! The walkers live in the library rather than in the fsck binary so that
//! the daemon's own store can be checked by the same code the tool uses,
//! and so a future online mode can wrap them.

pub mod disk;
pub mod findings;
pub mod holders;
pub mod records;

use std::path::Path;

use crate::cas::SharedBlockStore;
use crate::metastore::MetaStore;

pub use disk::{BlockFile, DiskWalk, ForeignPath, walk_disk};
pub use findings::{Finding, FindingClass, HolderRef, Severity};
pub use holders::{ExpectedCounts, HolderEnumerationError, expected_counts, holders_of};
pub use records::{RecordWalk, walk_records};

/// What a walker needs from an opened store.
///
/// The two halves are separate databases and neither can see the other's
/// records: the namespace DB holds the bucket trees (the object holders),
/// the shared block store holds `_BLOCKS`, `_MULTIPART_PARTS` and the block
/// files. A scrub needs both, so it takes both rather than guessing one
/// from the other.
///
/// Borrowed, not owned: the caller keeps the handles it opened, and the
/// exclusivity that makes an offline scrub meaningful comes from fjall's
/// LOCK on those handles, not from anything here.
pub struct ScrubContext<'a> {
    namespace: &'a MetaStore,
    shared: &'a SharedBlockStore,
}

impl<'a> ScrubContext<'a> {
    /// Builds a context over an opened namespace and its block store.
    pub fn new(namespace: &'a MetaStore, shared: &'a SharedBlockStore) -> Self {
        Self { namespace, shared }
    }

    /// The namespace metadata store: bucket trees live here.
    pub fn namespace(&self) -> &MetaStore {
        self.namespace
    }

    /// The shared block store: `_BLOCKS`, `_MULTIPART_PARTS`, block files.
    pub fn shared(&self) -> &SharedBlockStore {
        self.shared
    }

    /// Root directory of the block data files.
    pub fn blocks_root(&self) -> &Path {
        self.shared.blocks_root()
    }

    /// Width in bytes of the addresses this store files its blocks under,
    /// from its header. A file name is twice this many hex characters.
    pub fn id_width(&self) -> usize {
        self.shared.hasher().width() as usize
    }
}

#[cfg(test)]
mod tests;
