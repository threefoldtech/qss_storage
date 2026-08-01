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
//! [`passes`] compares those three and says what the differences mean;
//! [`engine::run`] walks once, runs the passes and returns a [`report`].
//! Everything up to there only ever reads; [`mod@repair`] is the one module
//! that mutates, and it runs after a report has been emitted, never before.
//!
//! One rule is enforced below the passes: the holder walk refuses on any
//! incompleteness, because a recount over a partial holder set that then
//! repairs frees live blocks. Everything else reports.
//!
//! The walkers live in the library rather than in the fsck binary so that
//! the daemon's own store can be checked by the same code the tool uses,
//! and so a future online mode can wrap them.

pub mod disk;
pub mod engine;
pub mod findings;
pub mod holders;
pub mod pairing;
pub mod passes;
pub mod records;
pub mod repair;
pub mod report;

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::cas::{BLOCKS_DB_DIR_NAME, SharedBlockStore};
use crate::metastore::MetaStore;
use crate::metastore::store_header::STORE_HEADER_SIDECAR;

pub use disk::{BlockFile, DiskWalk, ForeignPath, walk_disk};
pub use engine::{ScrubError, ScrubOptions, run};
pub use findings::{Finding, FindingClass, HolderRef, Severity};
pub use holders::{ExpectedCounts, HolderEnumerationError, expected_counts, holders_of};
pub use pairing::{Pairing, PairingError, RePair, re_pair};
pub use records::{RecordWalk, walk_records};
pub use repair::{
    RepairAction, RepairContext, RepairCounts, RepairError, RepairOutcome, RepairStatus,
    RepairSummary, repair,
};
pub use report::{Pass, Report, StoreRef, Summary, exit_code};

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
    meta_root: Option<PathBuf>,
}

impl<'a> ScrubContext<'a> {
    /// Builds a context over an opened namespace and its block store.
    pub fn new(namespace: &'a MetaStore, shared: &'a SharedBlockStore) -> Self {
        Self {
            namespace,
            shared,
            meta_root: None,
        }
    }

    /// Records the metadata root, for the report to name.
    ///
    /// Optional because nothing in a walk needs it: the databases are
    /// already open. It is the opener (the fsck binary) that knows which
    /// path it was told to open, and an operator running several stores
    /// needs the report to say which one this is.
    #[must_use]
    pub fn with_meta_root(mut self, meta_root: impl Into<PathBuf>) -> Self {
        self.meta_root = Some(meta_root.into());
        self
    }

    /// The metadata root, if the opener supplied it.
    pub fn meta_root(&self) -> Option<&Path> {
        self.meta_root.as_deref()
    }

    /// Paths that are the store's own metadata rather than block data, in the
    /// canonical form the disk walk compares against.
    ///
    /// The two databases of a store sit at `<meta_root>/db` and
    /// `<meta_root>/blocks/.db`, each with a header sidecar next to it -- and
    /// the tools default to one root for both halves (`--meta-root .
    /// --fs-root .`), which puts the block database and its sidecar *inside*
    /// `<fs_root>/blocks`, the tree [`walk_disk`] walks. Reporting them
    /// foreign would be wrong, and a `--repair` acting on that finding would
    /// rename the live database into quarantine. (`.db` is also skipped by
    /// name as a reserved entry, like `.tmp`; the sidecar is only known
    /// here.)
    ///
    /// Derived from the meta root, so a caller that did not supply one gets
    /// nothing skipped: only the opener knows which paths it opened.
    /// Non-existent candidates drop out here, so the walk compares against
    /// paths that are really there.
    pub fn store_own_paths(&self) -> HashSet<PathBuf> {
        let Some(meta_root) = &self.meta_root else {
            return HashSet::new();
        };
        [
            meta_root.join("db"),
            meta_root.join(STORE_HEADER_SIDECAR),
            meta_root.join("blocks").join(BLOCKS_DB_DIR_NAME),
            meta_root.join("blocks").join(STORE_HEADER_SIDECAR),
        ]
        .into_iter()
        .filter_map(|path| std::fs::canonicalize(path).ok())
        .collect()
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
