//! What a scrub run produces: an ordered set of findings, what it looked
//! at, and the number the shell sees.
//!
//! The JSON shape is the tool's scripting contract, so it is spelled out
//! here rather than assembled ad hoc:
//!
//! ```json
//! {
//!   "version": 1,
//!   "store": { "blocks_root": "...", "meta_root": "..." },
//!   "passes_run": ["recount", "disk_sweep"],
//!   "findings": [ ... ],
//!   "summary": { "info": 3, "warn": 1, "critical": 0, "total": 4 },
//!   "exit_code": 1
//! }
//! ```
//!
//! `version` is this document's schema version, not the store's. Adding a
//! field is not a bump; changing or removing one is.

use std::fmt::{self, Display, Formatter};

use serde::Serialize;

use super::findings::{Finding, Severity};

/// Schema version of the report document. Bumped only by a breaking change
/// to the field names or their meaning.
pub const REPORT_VERSION: u32 = 1;

/// Exit code of a scrub run, as the shell sees it.
///
/// Note that 3 (could-not-run) never appears in a report: if the store
/// could not be opened or walked there is no report to put it in, so the
/// binary produces it directly.
pub mod exit_code {
    /// Nothing found, or nothing above INFO.
    pub const CLEAN: u8 = 0;
    /// At least one WARN, no CRITICAL.
    pub const WARN: u8 = 1;
    /// At least one CRITICAL.
    pub const CRITICAL: u8 = 2;
    /// The scrub could not run at all. Never appears inside a report.
    pub const COULD_NOT_RUN: u8 = 3;
}

/// Which pass produced findings in this run.
///
/// A pass that did not run is absent -- that is how a consumer tells "the
/// recount found nothing" from "the recount refused because the holder set
/// was not closed".
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Pass {
    /// Holder references versus `_BLOCKS`.
    Recount,
    /// Block files versus the records that should name them.
    DiskSweep,
    /// Records whose file is not where they say.
    DanglingSweep,
    /// Re-hash of every block file.
    CorruptionScrub,
    /// In-flight multipart uploads.
    MultipartReport,
    /// Object trees with no bucket row.
    BucketIntegrity,
}

impl Pass {
    /// The name this pass carries in both renderings.
    pub fn as_str(self) -> &'static str {
        match self {
            Pass::Recount => "recount",
            Pass::DiskSweep => "disk_sweep",
            Pass::DanglingSweep => "dangling_sweep",
            Pass::CorruptionScrub => "corruption_scrub",
            Pass::MultipartReport => "multipart_report",
            Pass::BucketIntegrity => "bucket_integrity",
        }
    }
}

impl Display for Pass {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Which store was scrubbed.
///
/// Paths are rendered lossily, for the same reason findings' are: a report
/// that fails to serialize because a path is odd is worse than a report
/// with an odd-looking path in it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StoreRef {
    /// Root of the block data files.
    pub blocks_root: String,
    /// Root the metadata databases live under, when the caller knows it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta_root: Option<String>,
}

/// How many findings of each severity.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct Summary {
    /// Expected leakage and information.
    pub info: usize,
    /// Inconsistencies.
    pub warn: usize,
    /// Loss or loss-risk.
    pub critical: usize,
    /// All of the above.
    pub total: usize,
}

/// One scrub run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Report {
    /// Schema version of this document.
    pub version: u32,
    /// The store this is about.
    pub store: StoreRef,
    /// Passes that actually ran, in the order they ran.
    pub passes_run: Vec<Pass>,
    /// Everything found, worst first.
    pub findings: Vec<Finding>,
    /// Counts by severity.
    pub summary: Summary,
    /// What the process should exit with.
    pub exit_code: u8,
}

impl Report {
    /// Builds a report from a run's raw output, sorting the findings and
    /// deriving the summary and exit code from them.
    ///
    /// Ordering is CRITICAL first, then WARN, then INFO; within a severity,
    /// by class, then block, then path. The tiebreakers are there so two
    /// runs over an unchanged store produce byte-identical reports -- a
    /// report that reshuffles itself cannot be diffed.
    pub fn new(store: StoreRef, passes_run: Vec<Pass>, mut findings: Vec<Finding>) -> Self {
        findings.sort_by(|a, b| {
            b.severity
                .cmp(&a.severity)
                .then_with(|| a.class.cmp(&b.class))
                .then_with(|| a.block.cmp(&b.block))
                .then_with(|| a.path.cmp(&b.path))
                .then_with(|| a.evidence.cmp(&b.evidence))
        });

        let mut summary = Summary::default();
        for finding in &findings {
            match finding.severity {
                Severity::Info => summary.info += 1,
                Severity::Warn => summary.warn += 1,
                Severity::Critical => summary.critical += 1,
            }
            summary.total += 1;
        }

        let exit_code = if summary.critical > 0 {
            exit_code::CRITICAL
        } else if summary.warn > 0 {
            exit_code::WARN
        } else {
            exit_code::CLEAN
        };

        Self {
            version: REPORT_VERSION,
            store,
            passes_run,
            findings,
            summary,
            exit_code,
        }
    }

    /// Whether a pass ran in this scrub.
    ///
    /// Repair consults this: with the recount absent, the holder set was
    /// not closed and no refcount may be touched (ADR 0005 hard rule 1).
    pub fn ran(&self, pass: Pass) -> bool {
        self.passes_run.contains(&pass)
    }

    /// The report as an operator reads it.
    pub fn render_text(&self) -> String {
        let mut out = String::new();

        out.push_str(&format!("qss-storage fsck report (v{})\n", self.version));
        out.push_str(&format!("store: {}\n", self.store.blocks_root));
        if let Some(meta_root) = &self.store.meta_root {
            out.push_str(&format!("meta:  {meta_root}\n"));
        }
        out.push_str(&format!(
            "passes: {}\n\n",
            self.passes_run
                .iter()
                .map(|p| p.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));

        if self.findings.is_empty() {
            out.push_str("no findings\n\n");
        }
        for finding in &self.findings {
            let severity = match finding.severity {
                Severity::Info => "INFO",
                Severity::Warn => "WARN",
                Severity::Critical => "CRITICAL",
            };
            out.push_str(&format!("{severity} {}", finding.class.as_str()));
            if let Some(block) = &finding.block {
                out.push_str(&format!(" block={block}"));
            }
            out.push('\n');
            if let Some(path) = &finding.path {
                out.push_str(&format!("    path: {path}\n"));
            }
            out.push_str(&format!("    {}\n", finding.evidence));
            for holder in &finding.holders {
                out.push_str(&format!("    holder: {holder}\n"));
            }
            out.push('\n');
        }

        out.push_str(&format!(
            "{} critical, {} warn, {} info ({} finding(s)); exit {}\n",
            self.summary.critical,
            self.summary.warn,
            self.summary.info,
            self.summary.total,
            self.exit_code
        ));
        out
    }
}

impl Display for Report {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.write_str(&self.render_text())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scrub::findings::{FindingClass, HolderRef};

    fn store_ref() -> StoreRef {
        StoreRef {
            blocks_root: "/data/blocks".to_string(),
            meta_root: Some("/data/meta".to_string()),
        }
    }

    fn finding(class: FindingClass, evidence: &str) -> Finding {
        Finding::new(class, evidence)
    }

    #[test]
    fn findings_sort_worst_first_and_deterministically() {
        let findings = vec![
            finding(FindingClass::OrphanFile, "b"),
            finding(FindingClass::DanglingRecord, "gone"),
            finding(FindingClass::ForeignFile, "junk"),
            finding(FindingClass::OrphanFile, "a"),
            finding(FindingClass::RefcountUnderCount, "under"),
        ];
        let report = Report::new(store_ref(), vec![Pass::Recount], findings);

        let order: Vec<Severity> = report.findings.iter().map(|f| f.severity).collect();
        assert_eq!(
            order,
            vec![
                Severity::Critical,
                Severity::Critical,
                Severity::Warn,
                Severity::Info,
                Severity::Info
            ]
        );
        // Same severity, same class: the tiebreaker keeps the order stable.
        assert_eq!(report.findings[3].evidence, "a");
        assert_eq!(report.findings[4].evidence, "b");
    }

    #[test]
    fn the_summary_and_exit_code_follow_the_worst_finding() {
        let clean = Report::new(store_ref(), vec![], vec![]);
        assert_eq!(clean.exit_code, exit_code::CLEAN);
        assert_eq!(clean.summary, Summary::default());

        let info_only = Report::new(
            store_ref(),
            vec![],
            vec![finding(FindingClass::OrphanFile, "x")],
        );
        assert_eq!(info_only.exit_code, exit_code::CLEAN, "INFO alone is clean");
        assert_eq!(info_only.summary.info, 1);
        assert_eq!(info_only.summary.total, 1);

        let warned = Report::new(
            store_ref(),
            vec![],
            vec![
                finding(FindingClass::OrphanFile, "x"),
                finding(FindingClass::ForeignFile, "y"),
            ],
        );
        assert_eq!(warned.exit_code, exit_code::WARN);
        assert_eq!(warned.summary.warn, 1);

        let critical = Report::new(
            store_ref(),
            vec![],
            vec![
                finding(FindingClass::ForeignFile, "y"),
                finding(FindingClass::DanglingRecord, "z"),
            ],
        );
        assert_eq!(critical.exit_code, exit_code::CRITICAL);
        assert_eq!(critical.summary.critical, 1);
        assert_eq!(critical.summary.warn, 1);
    }

    #[test]
    fn a_pass_that_did_not_run_is_absent() {
        let report = Report::new(store_ref(), vec![Pass::DiskSweep], vec![]);
        assert!(report.ran(Pass::DiskSweep));
        assert!(
            !report.ran(Pass::Recount),
            "absence is how repair learns the holder set was not closed"
        );
    }

    #[test]
    fn the_text_render_shows_evidence_holders_and_a_footer() {
        let report = Report::new(
            store_ref(),
            vec![Pass::Recount, Pass::DanglingSweep],
            vec![
                finding(FindingClass::DanglingRecord, "the bytes are gone").with_holders(vec![
                    HolderRef::Object {
                        bucket: "photos".to_string(),
                        key: "cat.png".to_string(),
                    },
                ]),
                finding(FindingClass::OrphanFile, "nobody's block"),
            ],
        );

        let text = report.render_text();
        assert!(text.contains("passes: recount, dangling_sweep"), "{text}");
        assert!(text.contains("CRITICAL dangling_record"), "{text}");
        assert!(text.contains("    the bytes are gone"), "{text}");
        assert!(text.contains("    holder: object photos/cat.png"), "{text}");
        assert!(text.contains("INFO orphan_file"), "{text}");
        assert!(
            text.contains("1 critical, 0 warn, 1 info (2 finding(s)); exit 2"),
            "{text}"
        );
        // The Display impl is the same rendering.
        assert_eq!(text, report.to_string());
    }

    #[test]
    fn an_empty_report_says_so() {
        let text = Report::new(store_ref(), vec![Pass::Recount], vec![]).render_text();
        assert!(text.contains("no findings"), "{text}");
        assert!(text.contains("0 critical, 0 warn, 0 info"), "{text}");
    }
}
