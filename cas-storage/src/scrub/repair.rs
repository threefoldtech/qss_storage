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
//!   [`Report::ran`]`(`[`Pass::Recount`]`)`. The recount's absence means
//!   the holder set could not be closed, and a count over a partial holder
//!   set would authorise freeing blocks that are still referenced.
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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::{self, Display, Formatter};
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::cas::block_disk::QUARANTINE_DIR_NAME;
use crate::cas::{CasFS, SharedBlockStore};
use crate::metastore::{Block, BlockId, MetaError, MetaStore, block_disk_path};

use super::ScrubContext;
use super::disk::{DiskWalk, walk_disk};
use super::engine::{self, ScrubError, ScrubOptions};
use super::findings::{Finding, FindingClass, Severity};
use super::holders::{HolderEnumerationError, expected_counts};
use super::records::walk_records;
use super::report::{Pass, Report};

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

/// How one action ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RepairStatus {
    /// The store changed as the action intended.
    Applied,
    /// Nothing to do: the residue was already gone, or a rule forbade the
    /// action. The reason is in the outcome's detail.
    Skipped,
    /// The action was attempted and did not take. Its subject is still
    /// damaged, and the post-repair report will say so.
    Failed,
}

impl RepairStatus {
    /// The name this status carries in both renderings.
    pub fn as_str(self) -> &'static str {
        match self {
            RepairStatus::Applied => "applied",
            RepairStatus::Skipped => "skipped",
            RepairStatus::Failed => "failed",
        }
    }
}

/// What one action did, for the operator and for scripts.
///
/// Same conventions as [`Finding`]: owned, `Serialize`, snake_case names,
/// paths rendered lossily because a report must always serialize.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RepairOutcome {
    /// Which action this was, as [`RepairAction::kind`] names it.
    pub action: &'static str,
    /// How it ended.
    pub status: RepairStatus,
    /// How much an operator should care. Applied and skipped work is
    /// [`Severity::Info`]; a partial action is [`Severity::Warn`]; a
    /// failure is [`Severity::Critical`].
    pub severity: Severity,
    /// The block this was about, as lowercase hex, when it was about one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block: Option<String>,
    /// The path this was about, rendered lossily, when it was about one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// What happened, in words: the numbers moved, or the reason nothing
    /// moved. Scripts key off `action` and `status`.
    pub detail: String,
}

impl RepairOutcome {
    /// An outcome of `action`, at the severity its status implies.
    fn new(action: &'static str, status: RepairStatus, detail: impl Into<String>) -> Self {
        let severity = match status {
            RepairStatus::Applied | RepairStatus::Skipped => Severity::Info,
            RepairStatus::Failed => Severity::Critical,
        };
        Self {
            action,
            status,
            severity,
            block: None,
            path: None,
            detail: detail.into(),
        }
    }

    /// Names the block this outcome is about.
    #[must_use]
    fn with_block(mut self, id: &BlockId) -> Self {
        self.block = Some(id.to_hex());
        self
    }

    /// Names the path this outcome is about.
    #[must_use]
    fn with_path(mut self, path: &Path) -> Self {
        self.path = Some(path.to_string_lossy().into_owned());
        self
    }

    /// Overrides the severity the status implies.
    #[must_use]
    fn at(mut self, severity: Severity) -> Self {
        self.severity = severity;
        self
    }
}

/// How many outcomes of each status.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct RepairCounts {
    /// Actions that changed the store.
    pub applied: usize,
    /// Actions that had nothing to do, or were not allowed to.
    pub skipped: usize,
    /// Actions that did not take.
    pub failed: usize,
    /// All of the above.
    pub total: usize,
}

/// One `--repair` run: what it did, and what the store looked like
/// afterwards.
///
/// The exit code of the run is the post-repair report's
/// ([`Self::exit_code`]): a repair that did not finish leaves its subject
/// standing, and hard rule 2 turns that into a CRITICAL finding there.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RepairSummary {
    /// Schema version of this document.
    pub version: u32,
    /// Every action, in the order it was applied.
    pub outcomes: Vec<RepairOutcome>,
    /// Counts by status.
    pub counts: RepairCounts,
    /// The report the passes produced after the last action.
    pub report: Report,
}

impl RepairSummary {
    /// Assembles a summary, deriving the counts from the outcomes.
    fn new(outcomes: Vec<RepairOutcome>, report: Report) -> Self {
        let mut counts = RepairCounts::default();
        for outcome in &outcomes {
            match outcome.status {
                RepairStatus::Applied => counts.applied += 1,
                RepairStatus::Skipped => counts.skipped += 1,
                RepairStatus::Failed => counts.failed += 1,
            }
            counts.total += 1;
        }
        Self {
            version: REPAIR_VERSION,
            outcomes,
            counts,
            report,
        }
    }

    /// What the process should exit with after this run.
    pub fn exit_code(&self) -> u8 {
        self.report.exit_code
    }

    /// The summary as an operator reads it: the actions, then the report
    /// the store produced once they were done.
    pub fn render_text(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!("qss-storage fsck repair (v{})\n\n", self.version));

        if self.outcomes.is_empty() {
            out.push_str("nothing to repair\n\n");
        }
        for outcome in &self.outcomes {
            out.push_str(&format!(
                "{} {}",
                outcome.status.as_str().to_uppercase(),
                outcome.action
            ));
            if let Some(block) = &outcome.block {
                out.push_str(&format!(" block={block}"));
            }
            out.push('\n');
            if let Some(path) = &outcome.path {
                out.push_str(&format!("    path: {path}\n"));
            }
            out.push_str(&format!("    {}\n", outcome.detail));
        }

        out.push_str(&format!(
            "\n{} applied, {} skipped, {} failed ({} action(s))\n\n",
            self.counts.applied, self.counts.skipped, self.counts.failed, self.counts.total
        ));
        out.push_str(&self.report.render_text());
        out
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

/// What one action's implementation reports back, before it is dressed as
/// an outcome. `Err` is a failure, carrying what went wrong.
type ActionResult = Result<Done, String>;

/// The two ways an action can succeed.
enum Done {
    /// The store changed.
    Applied(String),
    /// It did not need to.
    Skipped(String),
}

impl RepairAction {
    /// The name this action carries in the summary. Part of the tool's
    /// scripting contract, like a finding's class.
    pub fn kind(&self) -> &'static str {
        match self {
            RepairAction::ResumeBucketTeardown { .. } => "resume_bucket_teardown",
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
            RepairAction::ResumeBucketTeardown { .. } | RepairAction::QuarantineForeign { .. } => {
                None
            }
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
            | RepairAction::AdoptOrphan { .. }
            | RepairAction::SetRc { .. }
            | RepairAction::MarkDegraded { .. } => None,
        }
    }
}

// ---------------------------------------------------------------------
// The actions
// ---------------------------------------------------------------------

/// Sets a record's rc to `to`, or frees the block when `to` is zero.
///
/// Reads the record at apply time rather than trusting the plan: the value
/// is a target, not a delta, so re-applying it is a no-op and an rc that
/// already reads `to` is skipped rather than rewritten.
fn set_rc(ctx: &RepairContext<'_>, block: &BlockId, from: u64, to: u64) -> ActionResult {
    let shared = ctx.shared();
    let Some(record) = shared
        .block_tree()
        .get_block(block.as_slice())
        .map_err(|e| format!("the record could not be read: {e}"))?
    else {
        return Ok(Done::Skipped(
            "the record is gone, so there is no rc to set".to_string(),
        ));
    };

    let current = record.rc() as u64;
    if current == to {
        return Ok(Done::Skipped(format!("rc already reads {to}")));
    }

    if to == 0 {
        // Nothing holds this block. The protocol has no rc=0 state -- the
        // last decrement removes the record and unlinks the file -- so this
        // does the same, and in the same order: record first, file second,
        // so a crash in between leaves an orphan file (leakage) rather than
        // a record with no bytes (loss).
        let path = block_disk_path(block, record.depth(), ctx.blocks_root().to_path_buf());
        let mut tx = shared.meta_store().begin_transaction();
        if let Err(e) = tx.remove_block_record(*block) {
            tx.rollback();
            return Err(format!("the record could not be removed: {e}"));
        }
        tx.commit()
            .map_err(|e| format!("the record removal would not commit: {e}"))?;
        unlink_tolerant(&path)?;
        return Ok(Done::Applied(format!(
            "rc {current} -> 0 (planned from {from}): nothing holds this block, so the record is \
             removed and {} unlinked",
            path.display()
        )));
    }

    let rc = usize::try_from(to).map_err(|_| format!("holder count {to} does not fit an rc"))?;
    // Only the count moves: size, depth and the flags byte (whose reserved
    // bits are not this build's to interpret) are carried over.
    let updated = Block::from_parts(record.size(), record.depth(), rc, record.flags());
    let mut tx = shared.meta_store().begin_transaction();
    if let Err(e) = tx.put_block_record(*block, &updated) {
        tx.rollback();
        return Err(format!("the record could not be written: {e}"));
    }
    tx.commit()
        .map_err(|e| format!("the rc write would not commit: {e}"))?;

    Ok(Done::Applied(format!(
        "rc {current} -> {to} (planned from {from})"
    )))
}

/// Points a record at a copy of its block found at another depth.
///
/// Hard rule 3: the candidate is re-hashed first. fsck does not have the
/// writer's bytes, so unlike the write path's heal it cannot assume a file
/// under an address holds that address's content. A candidate that does not
/// hash is corrupt residue and is quarantined instead; if none of them
/// hash, the record is marked degraded, which is where it would have ended
/// up with no candidates at all.
///
/// Once a candidate is adopted the remaining copies are unreferenced
/// duplicates of it, and go the way of any off-depth duplicate.
fn adopt_orphan(
    ctx: &RepairContext<'_>,
    block: &BlockId,
    record_depth: u8,
    candidates: &[(u8, PathBuf)],
) -> ActionResult {
    let shared = ctx.shared();
    let Some(record) = shared
        .block_tree()
        .get_block(block.as_slice())
        .map_err(|e| format!("the record could not be read: {e}"))?
    else {
        return Ok(Done::Skipped(
            "the record is gone, so there is nothing to adopt for".to_string(),
        ));
    };
    if block_disk_path(block, record.depth(), ctx.blocks_root().to_path_buf()).is_file() {
        return Ok(Done::Skipped(format!(
            "the record's own file is at depth {} after all",
            record.depth()
        )));
    }

    let mut quarantined: Vec<String> = Vec::new();
    for (index, (depth, path)) in candidates.iter().enumerate() {
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(format!(
                    "candidate {} could not be read: {e}",
                    path.display()
                ));
            }
        };

        let actual = shared.hasher().hash(&bytes);
        if actual != *block {
            let dest = quarantine_file(ctx, path)?;
            quarantined.push(format!(
                "{} (hashes to {}) -> {}",
                path.display(),
                actual.to_hex(),
                dest.display()
            ));
            continue;
        }

        // Verified. The record follows the file: depth is placement, and
        // rc, size and flags are the record's own accounting.
        let adopted = Block::from_parts(record.size(), *depth, record.rc(), record.flags());
        let mut tx = shared.meta_store().begin_transaction();
        if let Err(e) = tx.put_block_record(*block, &adopted) {
            tx.rollback();
            return Err(format!("the adopted record could not be written: {e}"));
        }
        tx.commit()
            .map_err(|e| format!("the adoption would not commit: {e}"))?;

        // Anything still standing is now a duplicate of the adopted file.
        let mut deleted = 0;
        for (_, other) in &candidates[index + 1..] {
            unlink_tolerant(other)?;
            deleted += 1;
        }

        let mut detail = format!(
            "re-hashed {} and it matches: the record moves from depth {record_depth} to {depth}",
            path.display()
        );
        if deleted > 0 {
            detail.push_str(&format!(", {deleted} further copy/copies deleted"));
        }
        if !quarantined.is_empty() {
            detail.push_str(&format!(", after quarantining {}", quarantined.join(", ")));
        }
        return Ok(Done::Applied(detail));
    }

    if quarantined.is_empty() {
        return Ok(Done::Skipped(
            "no candidate is on disk any more; the record is left to the degraded marking"
                .to_string(),
        ));
    }

    // Every candidate was corrupt: the bytes really are gone.
    mark_degraded(ctx, block)?;
    Ok(Done::Applied(format!(
        "no candidate hashes to this address, so none was adopted: quarantined {} and marked the \
         record degraded",
        quarantined.join(", ")
    )))
}

/// Moves a corrupt block file out of the blocks root and marks its record
/// degraded.
///
/// The two halves are one action because either alone is worse than
/// neither: a quarantined file with a live record leaves dedup pointing at
/// nothing, and a degraded record over corrupt bytes leaves the bytes where
/// a reader can still find them.
fn quarantine_corrupt(ctx: &RepairContext<'_>, block: &BlockId, path: &Path) -> ActionResult {
    if !path.exists() {
        // Still make sure the record says what it should: the file may have
        // been moved by an earlier, interrupted run.
        let flagged = mark_degraded(ctx, block)?;
        return Ok(Done::Skipped(format!(
            "the file is already gone from {}{}",
            path.display(),
            if flagged {
                "; its record is now marked degraded"
            } else {
                ""
            }
        )));
    }

    let dest = quarantine_file(ctx, path)?;
    mark_degraded(ctx, block)?;
    Ok(Done::Applied(format!(
        "moved to {} and the record marked degraded, so the next PUT of this content heals it \
         instead of deduplicating against bytes that do not hash",
        dest.display()
    )))
}

/// Flags a record whose bytes are gone (the action; see [`mark_degraded`]).
fn mark_degraded_action(ctx: &RepairContext<'_>, block: &BlockId) -> ActionResult {
    if mark_degraded(ctx, block)? {
        Ok(Done::Applied(
            "record flagged degraded: its holders stay accounted for, and the next PUT of this \
             content writes the file and clears the flag"
                .to_string(),
        ))
    } else {
        Ok(Done::Skipped(
            "the record is gone or already degraded".to_string(),
        ))
    }
}

/// Deletes a block file nothing references. ENOENT is success, which is
/// what makes a killed repair re-runnable.
fn delete_unreferenced(path: &Path) -> ActionResult {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(Done::Applied(format!("deleted {}", path.display()))),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            Ok(Done::Skipped("already gone".to_string()))
        }
        Err(e) => Err(format!("could not delete {}: {e}", path.display())),
    }
}

/// Moves something that is not part of the layout into quarantine.
fn quarantine_foreign(ctx: &RepairContext<'_>, path: &Path) -> ActionResult {
    if !path.exists() {
        return Ok(Done::Skipped("already gone".to_string()));
    }
    let dest = quarantine_file(ctx, path)?;
    Ok(Done::Applied(format!(
        "moved out of the blocks root to {}; fsck did not put it there and does not delete it",
        dest.display()
    )))
}

/// Resumes a bucket teardown: every object through the daemon's striped
/// delete path, then the tree.
///
/// Deleting through `delete_object` rather than dropping the tree outright
/// is what keeps the refcounts honest as it goes -- the same decrements the
/// crashed teardown would have made.
///
/// An object key that is not UTF-8 cannot go through that path (it takes
/// `&str`, and the daemon's own loop asserts it). fsck must not panic on
/// one, so the key is reported WARN and skipped; the tree is still dropped,
/// because the `_BUCKETS` row is already gone and leaving the tree would
/// strand it forever. The recount that follows this action reconciles the
/// references the skipped object was holding.
async fn resume_bucket_teardown(
    ctx: &RepairContext<'_>,
    bucket: &str,
    extra: &mut Vec<RepairOutcome>,
) -> ActionResult {
    let namespace = ctx.namespace();
    let tree = namespace
        .get_bucket_ext(bucket)
        .map_err(|e| format!("the object tree would not open: {e}"))?;

    // The keys are collected first: the delete path removes records from
    // the tree being iterated, and this store makes no promise about what
    // an iterator does then.
    let mut keys: Vec<Vec<u8>> = Vec::new();
    for item in tree.iter_all() {
        let (key, _) = item.map_err(|e| format!("the object tree would not iterate: {e}"))?;
        keys.push(key);
    }

    let mut deleted = 0usize;
    let mut skipped = 0usize;
    for key in keys {
        match std::str::from_utf8(&key) {
            Ok(key) => {
                ctx.fs
                    .delete_object(bucket, key)
                    .await
                    .map_err(|e| format!("object {bucket}/{key} would not delete: {e}"))?;
                deleted += 1;
            }
            Err(_) => {
                skipped += 1;
                extra.push(
                    RepairOutcome::new(
                        "resume_bucket_teardown",
                        RepairStatus::Skipped,
                        format!(
                            "object key {} in {bucket} is not UTF-8, so the striped delete path \
                             cannot name it: the record goes with the tree and the recount \
                             reconciles its block references",
                            String::from_utf8_lossy(&key)
                        ),
                    )
                    .at(Severity::Warn),
                );
            }
        }
    }

    namespace
        .drop_bucket(bucket)
        .map_err(|e| format!("the object tree would not drop: {e}"))?;

    Ok(Done::Applied(format!(
        "teardown resumed: {deleted} object(s) deleted through the striped path, {skipped} \
         skipped, tree dropped"
    )))
}

// ---------------------------------------------------------------------
// Shared primitives
// ---------------------------------------------------------------------

/// Sets the degraded flag on a record, if there is one and it is not set
/// already. Returns whether anything changed.
fn mark_degraded(ctx: &RepairContext<'_>, block: &BlockId) -> Result<bool, String> {
    let shared = ctx.shared();
    let Some(record) = shared
        .block_tree()
        .get_block(block.as_slice())
        .map_err(|e| format!("the record could not be read: {e}"))?
    else {
        return Ok(false);
    };
    if record.is_degraded() {
        return Ok(false);
    }

    let mut flagged = Block::from_parts(record.size(), record.depth(), record.rc(), record.flags());
    flagged.set_degraded(true);
    let mut tx = shared.meta_store().begin_transaction();
    if let Err(e) = tx.put_block_record(*block, &flagged) {
        tx.rollback();
        return Err(format!("the degraded flag could not be written: {e}"));
    }
    tx.commit()
        .map_err(|e| format!("the degraded flag would not commit: {e}"))?;
    Ok(true)
}

/// Unlinks a path, treating "already gone" as success.
fn unlink_tolerant(path: &Path) -> Result<(), String> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(format!("could not unlink {}: {e}", path.display())),
    }
}

/// Moves `path` into `blocks/.quarantine/` and returns where it went.
///
/// A filesystem quarantine rather than a DB tree (ADR 0005): it is visible
/// with `ls`, it survives metadata damage, and a block file's full-id name
/// stays self-identifying wherever it sits. The directory is created and
/// fsynced on use -- `create_dir_all` is a no-op after the first time -- and
/// a name already taken gets a numeric suffix rather than overwriting what
/// an earlier run set aside.
fn quarantine_file(ctx: &RepairContext<'_>, path: &Path) -> Result<PathBuf, String> {
    let dir = ctx.blocks_root().join(QUARANTINE_DIR_NAME);
    std::fs::create_dir_all(&dir)
        .map_err(|e| format!("the quarantine directory could not be created: {e}"))?;
    fsync_dir(&dir)?;

    let base = path.file_name().unwrap_or_else(|| "unnamed".as_ref());
    let mut dest = dir.join(base);
    let mut collision = 0u32;
    while dest.exists() {
        collision += 1;
        let mut name = base.to_os_string();
        name.push(format!(".{collision}"));
        dest = dir.join(name);
    }

    std::fs::rename(path, &dest).map_err(|e| {
        format!(
            "could not move {} to {}: {e}",
            path.display(),
            dest.display()
        )
    })?;
    fsync_dir(&dir)?;
    Ok(dest)
}

/// Fsyncs a directory, so a quarantine survives the power cut that a
/// corruption finding often precedes.
fn fsync_dir(dir: &Path) -> Result<(), String> {
    std::fs::File::open(dir)
        .and_then(|f| f.sync_all())
        .map_err(|e| format!("could not fsync {}: {e}", dir.display()))
}

// ---------------------------------------------------------------------
// The engine
// ---------------------------------------------------------------------

/// Applies what the report found, then checks its work.
///
/// Two planning rounds, because one action changes what the others must do:
/// resuming a half-deleted bucket's teardown removes object records, so
/// every rc below it is counted after the teardowns are done, never before.
///
/// `options` decides the post-repair passes; pass the same ones the report
/// was produced with, or the check will not look at what the repair
/// touched.
///
/// # Errors
///
/// [`RepairError`] if the store could not be walked or the post-repair
/// passes could not run. A failed individual action is not an error: it is
/// an outcome, and the post-repair report carries its consequence.
pub async fn repair(
    ctx: &RepairContext<'_>,
    report: &Report,
    options: &ScrubOptions,
) -> Result<RepairSummary, RepairError> {
    let mut outcomes: Vec<RepairOutcome> = Vec::new();

    for action in plan_teardowns(ctx, report, &mut outcomes)? {
        outcomes.extend(action.apply(ctx).await);
    }

    for action in plan_repairs(ctx, report, &mut outcomes)? {
        outcomes.extend(action.apply(ctx).await);
    }

    // Hard rule 2: the passes run again over the repaired store, and
    // anything they still find that this engine claims to repair means an
    // action did not take.
    let post = engine::run(&ctx.scrub_context(), options).map_err(RepairError::Scrub)?;
    Ok(RepairSummary::new(outcomes, flag_unfinished(post)))
}

/// Round one: the half-deleted buckets.
///
/// Gated on the recount for the same reason [`RepairAction::SetRc`] is --
/// the teardown drives refcounts down through the delete path, and no
/// refcount may move while the holder set is unclosed.
fn plan_teardowns(
    ctx: &RepairContext<'_>,
    report: &Report,
    outcomes: &mut Vec<RepairOutcome>,
) -> Result<Vec<RepairAction>, RepairError> {
    if !report.ran(Pass::BucketIntegrity) {
        return Ok(Vec::new());
    }

    let stranded = stranded_trees(ctx.namespace()).map_err(RepairError::Store)?;
    if stranded.is_empty() {
        return Ok(Vec::new());
    }

    if !report.ran(Pass::Recount) {
        outcomes.push(
            RepairOutcome::new(
                "resume_bucket_teardown",
                RepairStatus::Skipped,
                format!(
                    "{} half-deleted bucket(s) left alone: the report's holder set was not \
                     closed, and a teardown decrements refcounts",
                    stranded.len()
                ),
            )
            .at(Severity::Warn),
        );
        return Ok(Vec::new());
    }

    Ok(stranded
        .into_iter()
        .map(|bucket| RepairAction::ResumeBucketTeardown { bucket })
        .collect())
}

/// Object trees with no `_BUCKETS` row, sorted. The same derivation
/// `passes::bucket_integrity` makes; repeated here typed, because the
/// finding it produces states the tree name in prose.
fn stranded_trees(namespace: &MetaStore) -> Result<Vec<String>, MetaError> {
    let named: HashSet<String> = namespace
        .list_buckets()?
        .into_iter()
        .map(|meta| meta.name().to_string())
        .collect();
    let mut stranded: Vec<String> = namespace
        .list_trees()?
        .into_iter()
        .filter(|tree| !tree.starts_with('_') && !named.contains(tree))
        .collect();
    stranded.sort();
    Ok(stranded)
}

/// Round two: everything else, planned against the store the teardowns
/// left behind, in application order.
fn plan_repairs(
    ctx: &RepairContext<'_>,
    report: &Report,
    outcomes: &mut Vec<RepairOutcome>,
) -> Result<Vec<RepairAction>, RepairError> {
    let scrub = ctx.scrub_context();
    let records = walk_records(&scrub).map_err(RepairError::Store)?;
    let disk = walk_disk(&scrub).map_err(RepairError::Disk)?;
    let root = ctx.blocks_root().to_path_buf();

    let mut actions = Vec::new();

    // 1. The corruption verdicts, from the report: this planner will not
    //    re-hash the store.
    actions.extend(plan_corrupt(report, &disk, outcomes));

    // 2. Records whose file is not where they say. Which of the two
    //    treatments each gets is the dangling sweep's classification,
    //    re-derived typed.
    let mut on_disk: HashMap<BlockId, Vec<u8>> = HashMap::new();
    for file in &disk.files {
        on_disk.entry(file.id).or_default().push(file.depth);
    }
    let mut adoptions = Vec::new();
    let mut degradings = Vec::new();
    for (id, block) in &records.records {
        if block.is_degraded() || block_disk_path(id, block.depth(), root.clone()).is_file() {
            continue;
        }
        let mut depths: Vec<u8> = on_disk
            .get(id)
            .map(|depths| {
                depths
                    .iter()
                    .copied()
                    .filter(|d| *d != block.depth())
                    .collect()
            })
            .unwrap_or_default();
        depths.sort_unstable();

        if depths.is_empty() {
            degradings.push(RepairAction::MarkDegraded { block: *id });
        } else {
            adoptions.push(RepairAction::AdoptOrphan {
                block: *id,
                record_depth: block.depth(),
                candidates: depths
                    .into_iter()
                    .map(|depth| (depth, block_disk_path(id, depth, root.clone())))
                    .collect(),
            });
        }
    }
    sort_by_block(&mut adoptions);
    sort_by_block(&mut degradings);
    actions.extend(adoptions);

    // 3. The recount, gated. Hard rule 1: with the holder set unclosed, no
    //    refcount may move in either direction.
    if report.ran(Pass::Recount) {
        let expected = expected_counts(&scrub).map_err(RepairError::Holders)?;
        let mut set_rcs = Vec::new();
        for (id, block) in &records.records {
            let holders = expected.get(id).copied().unwrap_or(0);
            let rc = block.rc() as u64;
            if rc != holders {
                set_rcs.push(RepairAction::SetRc {
                    block: *id,
                    from: rc,
                    to: holders,
                });
            }
        }
        sort_by_block(&mut set_rcs);
        actions.extend(set_rcs);
    } else {
        outcomes.push(
            RepairOutcome::new(
                "set_rc",
                RepairStatus::Skipped,
                "no refcount was touched: the report did not run the recount, so the holder set \
                 was not closed, and a count over a partial holder set would authorise freeing \
                 blocks that are still referenced",
            )
            .at(Severity::Warn),
        );
    }

    // 4. What is still fileless is unrecoverable.
    actions.extend(degradings);

    // 5. Files nothing can reference. Last, so that a file some action
    //    above still wanted is gone by the time this deletes it.
    let present: HashSet<(BlockId, u8)> = disk
        .files
        .iter()
        .map(|file| (file.id, file.depth))
        .collect();
    for file in &disk.files {
        match records.records.get(&file.id) {
            None => actions.push(RepairAction::DeleteOrphan {
                block: file.id,
                path: file.path.clone(),
            }),
            Some(block)
                if block.depth() != file.depth && present.contains(&(file.id, block.depth())) =>
            {
                actions.push(RepairAction::DeleteOffDepth {
                    block: file.id,
                    path: file.path.clone(),
                });
            }
            // Either the record's own file, or the adoption candidate
            // handled above -- one file, one action.
            Some(_) => {}
        }
    }
    for foreign in &disk.foreign {
        actions.push(RepairAction::QuarantineForeign {
            path: foreign.path.clone(),
        });
    }

    Ok(actions)
}

/// The corrupt files the report named, resolved back to the walk's typed
/// paths.
///
/// A finding renders its path lossily; the pair (block, rendered path) is
/// still exact for a block file, because every path in one walk is unique
/// and a block file's own name is hex. A finding whose file the walk no
/// longer shows is reported skipped rather than guessed at.
fn plan_corrupt(
    report: &Report,
    disk: &DiskWalk,
    outcomes: &mut Vec<RepairOutcome>,
) -> Vec<RepairAction> {
    let mut actions = Vec::new();
    for finding in report
        .findings
        .iter()
        .filter(|f| f.class == FindingClass::CorruptBlock)
    {
        let (Some(hex), Some(path)) = (finding.block.as_deref(), finding.path.as_deref()) else {
            continue;
        };
        match disk
            .files
            .iter()
            .find(|file| file.id.to_hex() == hex && file.path.to_string_lossy() == path)
        {
            Some(file) => actions.push(RepairAction::QuarantineCorrupt {
                block: file.id,
                path: file.path.clone(),
            }),
            None => outcomes.push(RepairOutcome::new(
                "quarantine_corrupt",
                RepairStatus::Skipped,
                format!("the report named a corrupt file at {path}, which is no longer there"),
            )),
        }
    }
    actions
}

/// Orders actions by block address, so two runs over one store plan and
/// report in the same order. The walks hand back hash maps.
fn sort_by_block(actions: &mut [RepairAction]) {
    actions.sort_by_key(|action| action.block().map(|id| id.to_hex()));
}

/// Whether this engine claims to repair a finding of this class.
///
/// The post-repair check is exactly this list: a class nobody repairs
/// (a holder pointing at a block with no record, a record that will not
/// decode, a size mismatch) is expected to survive and says nothing about
/// whether the repair worked.
fn is_repairable(class: FindingClass) -> bool {
    match class {
        FindingClass::RefcountOverCount
        | FindingClass::RefcountUnderCount
        | FindingClass::OrphanFile
        | FindingClass::OffDepthFile
        | FindingClass::ForeignFile
        | FindingClass::DanglingRecord
        | FindingClass::AdoptableDanglingRecord
        | FindingClass::CorruptBlock
        | FindingClass::HalfDeletedBucket => true,
        FindingClass::MissingBlockRecord
        | FindingClass::UndecodableBlockRecord
        | FindingClass::SizeMismatch
        | FindingClass::DegradedRecord
        | FindingClass::MultipartUpload
        | FindingClass::HolderEnumerationFailed
        | FindingClass::PostRepairRecountDirty => false,
    }
}

/// Hard rule 2: anything repairable still standing after `--repair` makes
/// the run CRITICAL, whatever that finding's own severity is. A leftover
/// orphan file is INFO on its own; a leftover orphan file after a repair
/// that said it would delete it is a repair that did not work.
fn flag_unfinished(post: Report) -> Report {
    let mut left: BTreeMap<FindingClass, usize> = BTreeMap::new();
    for finding in &post.findings {
        if is_repairable(finding.class) {
            *left.entry(finding.class).or_default() += 1;
        }
    }
    if left.is_empty() {
        return post;
    }

    let mut findings = post.findings;
    for (class, count) in left {
        findings.push(Finding::new(
            FindingClass::PostRepairRecountDirty,
            format!(
                "{count} {} finding(s) survived --repair, which claims to repair that class: the \
                 repair did not finish, and no report of this store is clean until it does",
                class.as_str()
            ),
        ));
    }
    Report::new(post.store, post.passes_run, findings)
}

#[cfg(test)]
mod tests;
