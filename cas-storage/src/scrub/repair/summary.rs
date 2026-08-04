//! What one repair action did ([`RepairOutcome`]), and the document a whole
//! `--repair` run leaves behind ([`RepairSummary`]).

use std::path::Path;

use serde::Serialize;

use crate::metastore::BlockId;
use crate::scrub::findings::Severity;
use crate::scrub::report::Report;

use super::REPAIR_VERSION;

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
/// Same conventions as [`Finding`](crate::scrub::findings::Finding): owned,
/// `Serialize`, snake_case names, paths rendered lossily because a report
/// must always serialize.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RepairOutcome {
    /// Which action this was, as [`RepairAction::kind`](super::RepairAction::kind) names it.
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
    pub(super) fn new(
        action: &'static str,
        status: RepairStatus,
        detail: impl Into<String>,
    ) -> Self {
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
    pub(super) fn with_block(mut self, id: &BlockId) -> Self {
        self.block = Some(id.to_hex());
        self
    }

    /// Names the path this outcome is about.
    #[must_use]
    pub(super) fn with_path(mut self, path: &Path) -> Self {
        self.path = Some(path.to_string_lossy().into_owned());
        self
    }

    /// Names the raw record key this outcome is about, in the same field a
    /// path goes in and by the same lossless rule a finding uses
    /// ([`Finding::with_storage_key`](crate::scrub::findings::Finding::with_storage_key)):
    /// the two documents must name one record identically, or nothing can
    /// join a repaired record to the finding that reported it.
    #[must_use]
    pub(super) fn with_storage_key(mut self, key: &[u8]) -> Self {
        self.path = Some(crate::scrub::findings::render_storage_key(key));
        self
    }

    /// Overrides the severity the status implies.
    #[must_use]
    pub(super) fn at(mut self, severity: Severity) -> Self {
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
    pub(super) fn new(outcomes: Vec<RepairOutcome>, report: Report) -> Self {
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
