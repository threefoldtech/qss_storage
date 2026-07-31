//! Running a scrub: walk once, hand the results to every pass, collect the
//! report.
//!
//! Two kinds of failure, and the difference is the whole design:
//!
//! - the store cannot be walked at all (the block records will not read,
//!   the blocks root will not list, `_BUCKETS` will not decode). There is
//!   no trustworthy report to write, so this is a [`ScrubError`] and the
//!   binary exits 3.
//! - the holder set cannot be closed. That is a CRITICAL *finding*, per
//!   the ADR's severity table: the recount does not run and is absent from
//!   `passes_run`, so repair can see that no refcount may be touched, but
//!   everything the other passes can still say gets said.

use std::fmt::{self, Display, Formatter};
use std::io;

use crate::metastore::MetaError;

use super::disk::walk_disk;
use super::findings::{Finding, FindingClass};
use super::holders::expected_counts;
use super::passes;
use super::records::walk_records;
use super::report::{Pass, Report, StoreRef};
use super::{ScrubContext, holders::HolderEnumerationError};

/// Which passes to run.
///
/// Everything except the corruption scrub is always on: they are all
/// metadata-bound and finish in one pass over the store. The scrub reads
/// every byte of every block, which is hours on a real store, so it is
/// asked for explicitly.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScrubOptions {
    /// Re-hash every block file against its own name.
    pub scrub: bool,
}

impl ScrubOptions {
    /// Metadata passes only: the default.
    pub fn metadata_only() -> Self {
        Self { scrub: false }
    }

    /// Everything, including the disk-bound re-hash.
    pub fn full() -> Self {
        Self { scrub: true }
    }
}

/// A scrub that could not run. The binary reports this and exits 3.
#[derive(Debug)]
pub enum ScrubError {
    /// The block records could not be read.
    Records(MetaError),
    /// The blocks root could not be walked.
    Disk(io::Error),
    /// The bucket list or tree list could not be read.
    Buckets(MetaError),
}

impl Display for ScrubError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            ScrubError::Records(e) => write!(f, "the block records could not be read: {e}"),
            ScrubError::Disk(e) => write!(f, "the blocks root could not be walked: {e}"),
            ScrubError::Buckets(e) => write!(f, "the bucket list could not be read: {e}"),
        }
    }
}

impl std::error::Error for ScrubError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ScrubError::Records(e) | ScrubError::Buckets(e) => Some(e),
            ScrubError::Disk(e) => Some(e),
        }
    }
}

/// Renders a refused holder enumeration as the finding the report carries.
fn refusal_finding(e: &HolderEnumerationError) -> Finding {
    Finding::new(
        FindingClass::HolderEnumerationFailed,
        format!(
            "{e}. No recount was run: a count over a partial holder set would authorise freeing \
             blocks that are still referenced"
        ),
    )
}

/// Runs a scrub and returns its report.
///
/// The store is walked once: one record walk powers both the recount and
/// the dangling sweep, and one disk walk powers the disk sweep, the
/// dangling sweep's adoption check and (if asked) the corruption scrub.
///
/// # Errors
///
/// [`ScrubError`] if the store could not be walked. A refused holder
/// enumeration is not an error here -- it is a CRITICAL finding.
pub fn run(ctx: &ScrubContext, options: &ScrubOptions) -> Result<Report, ScrubError> {
    let mut findings: Vec<Finding> = Vec::new();
    let mut passes_run: Vec<Pass> = Vec::new();

    // The three walks, once each.
    let records = walk_records(ctx).map_err(ScrubError::Records)?;
    findings.extend(records.findings.iter().cloned());
    let disk = walk_disk(ctx).map_err(ScrubError::Disk)?;

    match expected_counts(ctx) {
        Ok(expected) => {
            passes_run.push(Pass::Recount);
            findings.extend(passes::recount(&expected, &records));
        }
        Err(e) => findings.push(refusal_finding(&e)),
    }

    passes_run.push(Pass::DiskSweep);
    findings.extend(passes::disk_sweep(&disk, &records));

    passes_run.push(Pass::DanglingSweep);
    findings.extend(passes::dangling_sweep(ctx, &records, &disk));

    if options.scrub {
        passes_run.push(Pass::CorruptionScrub);
        findings.extend(passes::corruption_scrub(ctx, &disk));
    }

    // The part tree is its own tree: it can be readable when a bucket tree
    // is not, so this runs whatever the holder walk did.
    match passes::multipart_report(ctx) {
        Ok(uploads) => {
            passes_run.push(Pass::MultipartReport);
            findings.extend(uploads);
        }
        Err(e) => findings.push(refusal_finding(&e)),
    }

    passes_run.push(Pass::BucketIntegrity);
    findings.extend(passes::bucket_integrity(ctx).map_err(ScrubError::Buckets)?);

    Ok(Report::new(
        StoreRef {
            blocks_root: ctx.blocks_root().to_string_lossy().into_owned(),
            meta_root: ctx.meta_root().map(|p| p.to_string_lossy().into_owned()),
        },
        passes_run,
        findings,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::crash_fixtures::{
        plant_dangling_record, plant_degraded_record, plant_orphan_file,
    };
    use crate::metastore::ContentHash;
    use crate::scrub::findings::Severity;
    use crate::scrub::report::exit_code;
    use crate::scrub::tests::{plant_object, put, store, synthetic_id};
    use tempfile::tempdir;

    fn classes(report: &Report, class: FindingClass) -> Vec<&Finding> {
        report
            .findings
            .iter()
            .filter(|f| f.class == class)
            .collect()
    }

    /// A store with one residue of every class the passes can see, run end
    /// to end: the report must find each of them, order them worst first,
    /// and exit 2.
    #[tokio::test]
    async fn a_store_with_one_of_everything_reports_all_of_it() {
        let dir = tempdir().unwrap();
        let (shared, fs) = store(&dir);
        fs.create_bucket("photos").unwrap();
        fs.create_bucket("stranded").unwrap();

        // Healthy: one object, one block, rc 1, file present.
        let healthy = put(&fs, "photos", "ok", b"a healthy block".repeat(20).to_vec()).await;

        // INFO: a record nothing references (cancelled PUT residue).
        let leaked = synthetic_id(0x01);
        plant_dangling_record(&shared, leaked, 1);
        plant_orphan_file(fs.fs_root(), &leaked, 1, &vec![0u8; 1234]);

        // INFO: an orphan file with no record at all.
        let orphan = synthetic_id(0x02);
        plant_orphan_file(fs.fs_root(), &orphan, 1, b"nobody's bytes");

        // INFO: a second copy of the healthy block at another depth.
        plant_orphan_file(fs.fs_root(), &healthy, 4, b"stale copy");

        // INFO: a degraded record, known damage awaiting heal.
        let degraded = synthetic_id(0x03);
        plant_degraded_record(&shared, degraded, 1, 2, 512);

        // INFO: an adoptable dangling record.
        let adoptable = synthetic_id(0x04);
        plant_dangling_record(&shared, adoptable, 1);
        plant_orphan_file(fs.fs_root(), &adoptable, 2, b"findable elsewhere");

        // CRITICAL: an unrecoverable dangling record with a live holder.
        let gone = synthetic_id(0x05);
        plant_dangling_record(&shared, gone, 1);
        plant_object(&fs, "photos", "damaged", vec![gone]);

        // CRITICAL: a holder pointing at a block with no record.
        let no_record = synthetic_id(0x06);
        plant_object(&fs, "photos", "points-nowhere", vec![no_record]);

        // WARN: a foreign file.
        std::fs::write(fs.fs_root().join("README"), b"not a block").unwrap();

        // WARN: an object tree whose bucket row is gone.
        put(&fs, "stranded", "k", b"still referenced".to_vec()).await;
        fs.namespace_meta_store()
            .get_allbuckets_tree()
            .unwrap()
            .remove(b"stranded")
            .unwrap();

        // INFO: an in-flight upload.
        fs.insert_multipart_part(
            "photos".to_string(),
            "big".to_string(),
            2048,
            1,
            "u-1".to_string(),
            ContentHash::from([9u8; 16]),
            vec![synthetic_id(0x07)],
        )
        .unwrap();

        let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared)
            .with_meta_root(dir.path().join("meta"));
        let report = run(&ctx, &ScrubOptions::metadata_only()).unwrap();

        // Every pass but the scrub ran.
        assert_eq!(
            report.passes_run,
            vec![
                Pass::Recount,
                Pass::DiskSweep,
                Pass::DanglingSweep,
                Pass::MultipartReport,
                Pass::BucketIntegrity
            ]
        );

        assert_eq!(classes(&report, FindingClass::DanglingRecord).len(), 1);
        assert_eq!(
            classes(&report, FindingClass::AdoptableDanglingRecord).len(),
            1
        );
        assert_eq!(classes(&report, FindingClass::DegradedRecord).len(), 1);
        // Two holders point at blocks with no record: the object planted
        // above, and the in-flight upload's part -- part records hold
        // references like any other holder, which is the point of counting
        // them unconditionally.
        let missing = classes(&report, FindingClass::MissingBlockRecord);
        assert_eq!(missing.len(), 2, "{missing:#?}");
        // One artifact, one finding: only the healthy block's stale copy is
        // an off-depth duplicate. The adoption candidate's file sits at the
        // wrong depth too, but its record has no file of its own, so it is
        // reported once -- as the candidate to adopt, not as a duplicate.
        let off_depth = classes(&report, FindingClass::OffDepthFile);
        assert_eq!(off_depth.len(), 1, "{off_depth:#?}");
        assert_eq!(
            off_depth[0].block.as_deref(),
            Some(healthy.to_hex().as_str())
        );
        assert_eq!(classes(&report, FindingClass::ForeignFile).len(), 1);
        assert_eq!(classes(&report, FindingClass::HalfDeletedBucket).len(), 1);
        assert_eq!(classes(&report, FindingClass::MultipartUpload).len(), 1);
        // The orphan file, plus the healthy block's stale copy is off-depth,
        // not an orphan.
        let orphans = classes(&report, FindingClass::OrphanFile);
        assert_eq!(orphans.len(), 1, "{orphans:#?}");

        // The multipart part is a holder of a block with no record, and the
        // leaked record has no holder: both directions of the recount.
        assert!(!classes(&report, FindingClass::RefcountOverCount).is_empty());

        // Worst first, and the exit code follows the worst.
        let severities: Vec<Severity> = report.findings.iter().map(|f| f.severity).collect();
        let mut sorted = severities.clone();
        sorted.sort_by(|a, b| b.cmp(a));
        assert_eq!(severities, sorted, "findings must be ordered worst first");
        assert_eq!(report.exit_code, exit_code::CRITICAL);
        assert_eq!(
            report.summary.total,
            report.summary.info + report.summary.warn + report.summary.critical
        );
        assert!(report.summary.critical >= 2);

        // The store the report is about is the store we scrubbed.
        assert_eq!(
            report.store.blocks_root,
            fs.fs_root().to_string_lossy().into_owned()
        );
        assert!(report.store.meta_root.is_some());
    }

    /// A healthy store: nothing to say, exit 0.
    #[tokio::test]
    async fn a_healthy_store_exits_clean() {
        let dir = tempdir().unwrap();
        let (shared, fs) = store(&dir);
        fs.create_bucket("b").unwrap();
        put(&fs, "b", "one", b"first block".repeat(20).to_vec()).await;
        put(&fs, "b", "two", b"second block".repeat(20).to_vec()).await;
        // The same content again: a dedup hit, rc 2, still exact.
        put(&fs, "b", "three", b"first block".repeat(20).to_vec()).await;

        let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
        let report = run(&ctx, &ScrubOptions::full()).unwrap();

        assert!(report.findings.is_empty(), "{}", report.render_text());
        assert_eq!(report.exit_code, exit_code::CLEAN);
        assert!(report.ran(Pass::CorruptionScrub), "--scrub was asked for");
    }

    /// The corruption scrub is opt-in: the same rotted store is silent
    /// without it and CRITICAL with it.
    #[tokio::test]
    async fn the_corruption_scrub_is_the_only_optional_pass() {
        let dir = tempdir().unwrap();
        let (shared, fs) = store(&dir);
        fs.create_bucket("b").unwrap();
        put(&fs, "b", "k", b"bytes that rot".repeat(20).to_vec()).await;

        // Same name, same length, different bytes: only a re-hash sees it.
        let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
        let path = walk_disk(&ctx).unwrap().files[0].path.clone();
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[3] ^= 0xff;
        std::fs::write(&path, &bytes).unwrap();

        let quiet = run(&ctx, &ScrubOptions::metadata_only()).unwrap();
        assert!(quiet.findings.is_empty(), "{}", quiet.render_text());
        assert!(!quiet.ran(Pass::CorruptionScrub));
        assert_eq!(quiet.exit_code, exit_code::CLEAN);

        let loud = run(&ctx, &ScrubOptions::full()).unwrap();
        assert_eq!(classes(&loud, FindingClass::CorruptBlock).len(), 1);
        assert_eq!(loud.exit_code, exit_code::CRITICAL);
    }

    /// A holder set that cannot be closed: the recount does not run, is
    /// absent from passes_run, and the refusal is itself CRITICAL. The
    /// other passes still report.
    #[tokio::test]
    async fn a_refused_holder_set_skips_the_recount_but_not_the_report() {
        let dir = tempdir().unwrap();
        let (shared, fs) = store(&dir);
        fs.create_bucket("b").unwrap();
        put(&fs, "b", "k", b"a real object".repeat(10).to_vec()).await;
        std::fs::write(fs.fs_root().join("README"), b"foreign").unwrap();

        fs.get_bucket("b")
            .unwrap()
            .insert(b"broken", vec![0xffu8; 8])
            .unwrap();

        let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
        let report = run(&ctx, &ScrubOptions::metadata_only()).unwrap();

        assert!(
            !report.ran(Pass::Recount),
            "no recount over a partial holder set"
        );
        assert!(report.ran(Pass::DiskSweep), "the disk pass is unaffected");
        let refusal = classes(&report, FindingClass::HolderEnumerationFailed);
        assert_eq!(refusal.len(), 1);
        assert_eq!(refusal[0].severity, Severity::Critical);
        assert!(refusal[0].evidence.contains("broken"), "{:?}", refusal[0]);
        assert_eq!(report.exit_code, exit_code::CRITICAL);
        // The foreign file was still found.
        assert_eq!(classes(&report, FindingClass::ForeignFile).len(), 1);
    }

    /// The JSON is the scripting contract: the documented top-level fields,
    /// with the documented names.
    #[tokio::test]
    async fn the_report_serializes_to_the_documented_schema() {
        let dir = tempdir().unwrap();
        let (shared, fs) = store(&dir);
        fs.create_bucket("b").unwrap();
        let gone = synthetic_id(0x21);
        plant_dangling_record(&shared, gone, 1);
        plant_object(&fs, "b", "damaged", vec![gone]);
        std::fs::write(fs.fs_root().join("README"), b"foreign").unwrap();

        let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared)
            .with_meta_root(dir.path().join("meta"));
        let report = run(&ctx, &ScrubOptions::metadata_only()).unwrap();

        let json: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&report).unwrap()).unwrap();

        assert_eq!(json["version"], 1);
        assert_eq!(json["exit_code"], 2);
        assert!(json["store"]["blocks_root"].is_string());
        assert!(json["store"]["meta_root"].is_string());
        assert_eq!(json["passes_run"][0], "recount");
        assert_eq!(
            json["summary"]["total"],
            serde_json::json!(report.summary.total)
        );
        assert!(json["summary"]["critical"].as_u64().unwrap() >= 1);

        let first = &json["findings"][0];
        assert_eq!(first["severity"], "critical");
        assert_eq!(first["class"], "dangling_record");
        assert_eq!(first["block"], gone.to_hex());
        assert!(
            first["evidence"]
                .as_str()
                .unwrap()
                .contains("poisons dedup")
        );
        assert_eq!(first["holders"][0]["kind"], "object");
        assert_eq!(first["holders"][0]["key"], "damaged");

        // The text render carries the same facts.
        let text = report.render_text();
        assert!(text.contains("CRITICAL dangling_record"), "{text}");
        assert!(text.contains("holder: object b/damaged"), "{text}");
        assert!(text.contains("exit 2"), "{text}");
    }
}
