//! Repair: what `--repair` does about what the scrub found (ADR 0005).
//!
//! Everything in this module mutates the store. Four rules hold it
//! together, and every action below is written to keep them:
//!
//! - **Report first.** The report is produced and emitted before any of
//!   this runs. [`repair`] takes it as an input and never widens its
//!   verdict on its own.
//! - **Idempotent.** Each action tolerates having already been applied:
//!   rc is *set* to a value rather than adjusted by a delta, unlinks and
//!   renames tolerate ENOENT, the degraded flag is set rather than
//!   toggled. A `--repair` that is killed halfway is re-run, not
//!   recovered.
//! - **Loss-never.** An rc only ever moves to a value derived from a
//!   complete holder walk, so every rc-mutating action is gated on
//!   [`Report::ran`](crate::scrub::report::Report::ran)`(`[`Pass::Recount`](crate::scrub::report::Pass::Recount)`)`.
//!   The recount's absence means the holder set could not be closed, and a
//!   count over a partial holder set would authorise freeing blocks that
//!   are still referenced.
//! - **Quarantine, never delete**, for corrupt blocks and foreign files.
//!   Only files nothing can reference -- orphans and off-depth duplicates
//!   -- are deleted.
//!
//! **No stripes are taken, and that is deliberate.** The stripes serialize
//! concurrent daemon writers; fsck holds the store's fjall LOCK, so there
//! is no second writer to serialize against and every walk it did is still
//! true when it acts on it. Record mutations still go through transactions:
//! atomicity against a crash is a different property from exclusion against
//! a peer, and only the first one is free.
//!
//! **What the planner takes from the report.** Exactly two things: the
//! recount gate above, and the corruption verdicts (re-hashing the store is
//! the hours-long pass, and this will not repeat it). Every other subject
//! is re-derived from a fresh walk, typed. A finding renders its paths
//! lossily because it has to serialize, which is right for a document and
//! wrong for a rename; and re-deriving means repair only ever acts on
//! residue it can still see for itself.

use std::fmt::{self, Display, Formatter};
use std::io;
use std::path::{Path, PathBuf};

use crate::cas::{CasFS, SharedBlockStore};
use crate::metastore::{BlockId, MetaError, MetaStore};

use super::ScrubContext;
use super::engine::ScrubError;
use super::holders::HolderEnumerationError;

mod actions;
mod plan;
mod summary;

pub use plan::repair;
pub use summary::{RepairCounts, RepairOutcome, RepairStatus, RepairSummary};

use actions::{
    Done, adopt_orphan, delete_unreferenced, mark_degraded_action, quarantine_corrupt,
    quarantine_foreign, reap_orphan_part, resume_bucket_teardown, set_rc,
};

/// Schema version of the repair document. Bumped only by a breaking change
/// to the field names or their meaning.
pub const REPAIR_VERSION: u32 = 1;

/// What a repair run may touch: one opened store, through the handles the
/// daemon itself uses.
///
/// Takes the whole [`CasFS`] rather than the scrub's two borrowed halves
/// because one action -- resuming a half-deleted bucket's teardown -- must
/// delete objects through the daemon's own striped delete path, and that
/// path lives on `CasFS`. It is also why [`repair`] is async.
pub struct RepairContext<'a> {
    fs: &'a CasFS,
    meta_root: Option<PathBuf>,
}

impl<'a> RepairContext<'a> {
    /// Builds a repair context over an opened store.
    pub fn new(fs: &'a CasFS) -> Self {
        Self {
            fs,
            meta_root: None,
        }
    }

    /// Records the metadata root, so the post-repair report can name the
    /// store it is about. See [`ScrubContext::with_meta_root`].
    #[must_use]
    pub fn with_meta_root(mut self, meta_root: impl Into<PathBuf>) -> Self {
        self.meta_root = Some(meta_root.into());
        self
    }

    /// The read-only view of the same store, for the walks and the passes.
    pub fn scrub_context(&self) -> ScrubContext<'_> {
        let ctx = ScrubContext::new(self.fs.namespace_meta_store(), self.fs.shared_block_store());
        match &self.meta_root {
            Some(root) => ctx.with_meta_root(root.clone()),
            None => ctx,
        }
    }

    /// The namespace metadata store: bucket trees live here.
    fn namespace(&self) -> &MetaStore {
        self.fs.namespace_meta_store()
    }

    /// The shared block store: `_BLOCKS`, `_MULTIPART_PARTS`, block files.
    fn shared(&self) -> &SharedBlockStore {
        self.fs.shared_block_store()
    }

    /// Root directory of the block data files.
    fn blocks_root(&self) -> &Path {
        self.fs.fs_root()
    }
}

/// A repair that could not run at all. The binary reports this and exits 3.
#[derive(Debug)]
pub enum RepairError {
    /// The post-repair passes could not be run.
    Scrub(ScrubError),
    /// The holder set would not close for the recount the repair was
    /// authorised to apply. The pre-repair report closed it one moment
    /// earlier, so this means the store changed under the run: re-run fsck.
    Holders(HolderEnumerationError),
    /// The metadata store would not answer.
    Store(MetaError),
    /// The blocks root would not walk.
    Disk(io::Error),
}

impl Display for RepairError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            RepairError::Scrub(e) => write!(f, "the post-repair scrub could not run: {e}"),
            RepairError::Holders(e) => write!(
                f,
                "{e}. The report this repair was authorised by did close the holder set, so the \
                 store changed under the run: re-run fsck"
            ),
            RepairError::Store(e) => write!(f, "the metadata store could not be read: {e}"),
            RepairError::Disk(e) => write!(f, "the blocks root could not be walked: {e}"),
        }
    }
}

impl std::error::Error for RepairError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            RepairError::Scrub(e) => Some(e),
            RepairError::Holders(e) => Some(e),
            RepairError::Store(e) => Some(e),
            RepairError::Disk(e) => Some(e),
        }
    }
}

/// One repair, named by the finding class it answers.
///
/// An enum rather than one type per class implementing a shared trait: the
/// bucket teardown is async (it drives the daemon's delete path), and a
/// `dyn` trait cannot carry a native `async fn`. The variants are the
/// classes; the arms of [`Self::apply`] are the actions.
///
/// Declaration order is application order, and the order is load-bearing:
///
/// 1. corrupt files are quarantined before anything can delete them;
/// 2. adoption runs before the recount so a record's rc is set against the
///    depth it ends up naming;
/// 3. the recount sets every rc, freeing what nothing holds;
/// 4. what is still fileless afterwards is marked degraded;
/// 5. unreferenced files go last, so they are removed once nothing above
///    can still want them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepairAction {
    /// Resume a bucket teardown that crashed after its `_BUCKETS` row was
    /// removed: delete every object through the striped delete path, then
    /// drop the tree.
    ResumeBucketTeardown {
        /// Name of the stranded object tree.
        bucket: String,
    },
    /// Reap a part record whose upload record does not exist: take the
    /// record and release the blocks it held, through the daemon's own
    /// take-style primitive (ADR 0003).
    ReapOrphanPart {
        /// Bucket the dead upload targeted.
        bucket: String,
        /// Key the dead upload targeted.
        key: String,
        /// Upload the part belonged to.
        upload_id: String,
        /// Position of the part within that upload.
        part_number: i64,
        /// The key the record is actually filed under. Carried whole rather
        /// than rebuilt: a legacy dash-joined key has no other address, and
        /// a rebuilt key would remove nothing and then release blocks the
        /// surviving record still claims.
        storage_key: Vec<u8>,
    },
    /// Move a block file whose bytes do not hash to its own name out of the
    /// blocks root, and mark its record degraded so future PUTs heal
    /// instead of deduplicating against a poisoned record.
    QuarantineCorrupt {
        /// The block the file claims to be.
        block: BlockId,
        /// Where the file is now.
        path: PathBuf,
    },
    /// Point a record at the copy of its block that is on disk at another
    /// depth -- after re-hashing that copy, because fsck does not have the
    /// writer's bytes and must verify before trusting them.
    AdoptOrphan {
        /// The record's block.
        block: BlockId,
        /// The depth the record names, where no file is.
        record_depth: u8,
        /// Every copy of the id on disk, shallowest first.
        candidates: Vec<(u8, PathBuf)>,
    },
    /// Set a record's rc to the walked holder count. Zero means free: the
    /// record is removed and its file unlinked, exactly as the last
    /// decrement would have done.
    SetRc {
        /// The record's block.
        block: BlockId,
        /// What the record said when this was planned.
        from: u64,
        /// What the holder walk counted.
        to: u64,
    },
    /// Flag a record whose bytes are gone, keeping its accounting honest
    /// while stopping it from poisoning dedup.
    MarkDegraded {
        /// The record's block.
        block: BlockId,
    },
    /// Delete a block file no record names.
    DeleteOrphan {
        /// The block the file names itself with.
        block: BlockId,
        /// Where it is.
        path: PathBuf,
    },
    /// Delete a copy of a block at a depth its record does not name, while
    /// the record's own file is where it says.
    DeleteOffDepth {
        /// The block the file names itself with.
        block: BlockId,
        /// Where it is.
        path: PathBuf,
    },
    /// Move something that is not part of this store's layout out of the
    /// blocks root. Never deleted: fsck did not put it there and does not
    /// know what it is.
    QuarantineForeign {
        /// Where it is.
        path: PathBuf,
    },
}

impl RepairAction {
    /// The name this action carries in the summary. Part of the tool's
    /// scripting contract, like a finding's class.
    pub fn kind(&self) -> &'static str {
        match self {
            RepairAction::ResumeBucketTeardown { .. } => "resume_bucket_teardown",
            RepairAction::ReapOrphanPart { .. } => "reap_orphan_part",
            RepairAction::QuarantineCorrupt { .. } => "quarantine_corrupt",
            RepairAction::AdoptOrphan { .. } => "adopt_orphan",
            RepairAction::SetRc { .. } => "set_rc",
            RepairAction::MarkDegraded { .. } => "mark_degraded",
            RepairAction::DeleteOrphan { .. } => "delete_orphan",
            RepairAction::DeleteOffDepth { .. } => "delete_off_depth",
            RepairAction::QuarantineForeign { .. } => "quarantine_foreign",
        }
    }

    /// Applies this action to `ctx`.
    ///
    /// Usually one outcome; the bucket teardown adds one per object key it
    /// could not act on. Never returns an error: a failed action is an
    /// outcome, because the run must go on to the actions that can still
    /// succeed and the post-repair report is what judges the result.
    pub async fn apply(&self, ctx: &RepairContext<'_>) -> Vec<RepairOutcome> {
        let mut outcomes = Vec::new();
        let result = match self {
            RepairAction::ResumeBucketTeardown { bucket } => {
                resume_bucket_teardown(ctx, bucket, &mut outcomes).await
            }
            RepairAction::ReapOrphanPart {
                bucket,
                key,
                upload_id,
                part_number,
                storage_key,
            } => reap_orphan_part(ctx, bucket, key, upload_id, *part_number, storage_key).await,
            RepairAction::QuarantineCorrupt { block, path } => quarantine_corrupt(ctx, block, path),
            RepairAction::AdoptOrphan {
                block,
                record_depth,
                candidates,
            } => adopt_orphan(ctx, block, *record_depth, candidates),
            RepairAction::SetRc { block, from, to } => set_rc(ctx, block, *from, *to),
            RepairAction::MarkDegraded { block } => mark_degraded_action(ctx, block),
            RepairAction::DeleteOrphan { path, .. } | RepairAction::DeleteOffDepth { path, .. } => {
                delete_unreferenced(path)
            }
            RepairAction::QuarantineForeign { path } => quarantine_foreign(ctx, path),
        };

        let mut outcome = match result {
            Ok(Done::Applied(detail)) => {
                RepairOutcome::new(self.kind(), RepairStatus::Applied, detail)
            }
            Ok(Done::Skipped(detail)) => {
                RepairOutcome::new(self.kind(), RepairStatus::Skipped, detail)
            }
            Err(detail) => RepairOutcome::new(self.kind(), RepairStatus::Failed, detail),
        };
        if let Some(block) = self.block() {
            outcome = outcome.with_block(&block);
        }
        if let Some(path) = self.path() {
            outcome = outcome.with_path(path);
        }
        if let Some(key) = self.storage_key() {
            outcome = outcome.with_storage_key(key);
        }

        outcomes.push(outcome);
        outcomes
    }

    /// The block this action is about, when it is about one.
    fn block(&self) -> Option<BlockId> {
        match self {
            RepairAction::QuarantineCorrupt { block, .. }
            | RepairAction::AdoptOrphan { block, .. }
            | RepairAction::SetRc { block, .. }
            | RepairAction::MarkDegraded { block }
            | RepairAction::DeleteOrphan { block, .. }
            | RepairAction::DeleteOffDepth { block, .. } => Some(*block),
            RepairAction::ResumeBucketTeardown { .. }
            | RepairAction::ReapOrphanPart { .. }
            | RepairAction::QuarantineForeign { .. } => None,
        }
    }

    /// The path this action is about, when it is about one.
    fn path(&self) -> Option<&Path> {
        match self {
            RepairAction::QuarantineCorrupt { path, .. }
            | RepairAction::DeleteOrphan { path, .. }
            | RepairAction::DeleteOffDepth { path, .. }
            | RepairAction::QuarantineForeign { path } => Some(path),
            RepairAction::ResumeBucketTeardown { .. }
            | RepairAction::ReapOrphanPart { .. }
            | RepairAction::AdoptOrphan { .. }
            | RepairAction::SetRc { .. }
            | RepairAction::MarkDegraded { .. } => None,
        }
    }

    /// The raw record key this action is about, when it is about one. Only
    /// the orphan-part reap is: every other subject is addressed by a block
    /// address or a filesystem path.
    fn storage_key(&self) -> Option<&[u8]> {
        match self {
            RepairAction::ReapOrphanPart { storage_key, .. } => Some(storage_key),
            RepairAction::ResumeBucketTeardown { .. }
            | RepairAction::QuarantineCorrupt { .. }
            | RepairAction::AdoptOrphan { .. }
            | RepairAction::SetRc { .. }
            | RepairAction::MarkDegraded { .. }
            | RepairAction::DeleteOrphan { .. }
            | RepairAction::DeleteOffDepth { .. }
            | RepairAction::QuarantineForeign { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests;
