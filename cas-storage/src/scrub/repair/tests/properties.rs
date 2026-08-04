use tempfile::tempdir;

use super::*;
use crate::cas::crash_fixtures::{
    plant_dangling_record, plant_half_deleted_bucket, plant_inflated_rc, plant_orphan_file,
    plant_stale_part, plant_upload_record,
};
use crate::scrub::ScrubContext;
use crate::scrub::disk::walk_disk;
use crate::scrub::records::walk_records;

/// One record, as a comparable value: address, rc, depth, size, degraded.
type RecordState = (String, usize, u8, usize, bool);

/// One block file, as a comparable value: path and size.
type FileState = (String, u64);

/// Every record and every block file: the end state two runs of an
/// idempotent repair must agree on.
fn snapshot(fs: &CasFS) -> (Vec<RecordState>, Vec<FileState>) {
    let shared = fs.shared_block_store();
    let ctx = ScrubContext::new(fs.namespace_meta_store(), shared);

    let mut records: Vec<_> = walk_records(&ctx)
        .unwrap()
        .records
        .iter()
        .map(|(id, block)| {
            (
                id.to_hex(),
                block.rc(),
                block.depth(),
                block.size(),
                block.is_degraded(),
            )
        })
        .collect();
    records.sort();

    let mut files: Vec<_> = walk_disk(&ctx)
        .unwrap()
        .files
        .iter()
        .map(|file| (file.path.to_string_lossy().into_owned(), file.size))
        .collect();
    files.sort();

    (records, files)
}

/// The reap is rc-consequential, so it is refused on exactly the ground
/// every other rc mutation is: an unclosed holder set. The record and the
/// references it holds are left exactly as they were found.
#[tokio::test]
async fn an_unclosed_holder_set_refuses_the_orphan_part_reap() {
    let dir = tempdir().unwrap();
    let (_shared, fs) = store(&dir);
    fs.create_bucket("b").unwrap();

    let held = put_part(
        &fs,
        "b",
        "big",
        "gone",
        1,
        b"an abandoned part".repeat(64).to_vec(),
    )
    .await;
    // One object record that will not decode: the holder set cannot close.
    fs.get_bucket("b")
        .unwrap()
        .insert(b"broken", vec![0xffu8; 8])
        .unwrap();

    let ctx = RepairContext::new(&fs);
    let report = engine::run(&ctx.scrub_context(), &ScrubOptions::metadata_only()).unwrap();
    assert!(!report.ran(Pass::Recount), "the premise of this test");
    assert!(
        report.ran(Pass::MultipartReport),
        "the classification itself is fine: the part records decode"
    );
    let summary = repair(&ctx, &report, &ScrubOptions::metadata_only())
        .await
        .unwrap();

    let refused = of_kind(&summary, "reap_orphan_part");
    assert_eq!(refused.len(), 1, "{:#?}", summary.outcomes);
    assert_eq!(refused[0].status, RepairStatus::Skipped);
    assert_eq!(refused[0].severity, Severity::Warn);
    assert!(refused[0].detail.contains("holder set"), "{refused:#?}");

    assert_eq!(
        fs.upload_parts("b", "big", "gone").unwrap().len(),
        1,
        "the record stands"
    );
    for id in &held {
        assert_eq!(rc(&fs, *id), 1, "and so does the reference it holds");
        assert!(recorded_path(&fs, *id).is_file());
    }
    assert_eq!(summary.exit_code(), exit_code::CRITICAL);
}

/// Hard rule 1: with the holder set unclosed, no refcount may move -- in
/// either direction, by any action. The report says so by leaving the
/// recount out of `passes_run`, and every rc-mutating action reads that.
#[tokio::test]
async fn an_unclosed_holder_set_refuses_every_rc_mutation() {
    let dir = tempdir().unwrap();
    let (shared, fs) = store(&dir);
    fs.create_bucket("b").unwrap();
    fs.create_bucket("stranded").unwrap();

    let id = put(&fs, "b", "k", b"a real block".repeat(20).to_vec()).await;
    plant_inflated_rc(&shared, id, 3);
    put(&fs, "stranded", "k", b"still referenced".repeat(8).to_vec()).await;
    plant_half_deleted_bucket(fs.namespace_meta_store(), "stranded");
    // One object record that will not decode: the holder set cannot close.
    fs.get_bucket("b")
        .unwrap()
        .insert(b"broken", vec![0xffu8; 8])
        .unwrap();

    let ctx = RepairContext::new(&fs);
    let report = engine::run(&ctx.scrub_context(), &ScrubOptions::metadata_only()).unwrap();
    assert!(!report.ran(Pass::Recount), "the premise of this test");
    let summary = repair(&ctx, &report, &ScrubOptions::metadata_only())
        .await
        .unwrap();

    assert_eq!(rc(&fs, id), 4, "the inflated rc is left exactly as it was");
    let refused = of_kind(&summary, "set_rc");
    assert_eq!(refused.len(), 1, "{:#?}", summary.outcomes);
    assert_eq!(refused[0].status, RepairStatus::Skipped);
    assert_eq!(refused[0].severity, Severity::Warn);
    assert!(refused[0].detail.contains("holder set"), "{refused:#?}");

    // The teardown is an rc mutation too: it decrements through the delete
    // path, so it is refused on the same ground.
    let teardown = of_kind(&summary, "resume_bucket_teardown");
    assert_eq!(teardown.len(), 1);
    assert_eq!(teardown[0].status, RepairStatus::Skipped);
    assert!(
        fs.namespace_meta_store()
            .list_trees()
            .unwrap()
            .iter()
            .any(|tree| tree == "stranded"),
        "the stranded tree is left standing"
    );
    assert_eq!(summary.exit_code(), exit_code::CRITICAL);
}

/// Idempotency: the second run of a repair finds nothing to do, and the
/// store it leaves is identical to the one the first run left. This is what
/// makes "re-run fsck" the whole recovery procedure.
#[tokio::test]
async fn applying_the_full_repair_twice_changes_nothing() {
    let dir = tempdir().unwrap();
    let (shared, fs) = store(&dir);
    fs.create_bucket("b").unwrap();
    fs.create_bucket("stranded").unwrap();

    let live = put(&fs, "b", "k", b"a live block".repeat(20).to_vec()).await;
    plant_inflated_rc(&shared, live, 2);
    plant_orphan_file(fs.fs_root(), &live, 5, b"a stale copy");
    plant_orphan_file(fs.fs_root(), &synthetic_id(0x04), 1, b"nobody's bytes");
    let gone = synthetic_id(0x05);
    plant_dangling_record(&shared, gone, 1);
    plant_object(&fs, "b", "damaged", vec![gone]).await;
    let adoptable = put(&fs, "b", "moved", b"content that moved".repeat(20).to_vec()).await;
    let depth = record(&fs, adoptable).unwrap().depth();
    misplace_block_file(fs.fs_root(), &adoptable, depth, depth + 2);
    std::fs::write(fs.fs_root().join("NOTES"), b"foreign").unwrap();
    put(&fs, "stranded", "k", b"still referenced".repeat(8).to_vec()).await;
    plant_half_deleted_bucket(fs.namespace_meta_store(), "stranded");
    // Both multipart shapes: a part no upload record owns (reaped) and one
    // a live upload does (reported, and left alone by both runs).
    plant_stale_part(&fs, "b", "big", "u-1", 1, vec![live]);
    plant_upload_record(&fs, "b", "big", "u-2", chrono::Utc::now().timestamp() - 60);
    plant_stale_part(&fs, "b", "big", "u-2", 1, vec![live]);

    let first = report_and_repair(&fs, ScrubOptions::full()).await;
    assert!(first.counts.applied > 0, "there was work to do");
    assert_eq!(first.exit_code(), exit_code::CLEAN, "{}", first.report);
    let after_first = snapshot(&fs);

    let second = report_and_repair(&fs, ScrubOptions::full()).await;

    assert!(
        second.outcomes.is_empty(),
        "a repaired store plans no actions: {:#?}",
        second.outcomes
    );
    assert_eq!(after_first, snapshot(&fs), "the end state is a fixed point");
    assert_eq!(second.exit_code(), exit_code::CLEAN, "{}", second.report);
}

/// Kill-mid-repair: a run that died after some actions leaves a store the
/// next run finishes. Applying a strict subset by hand is that crash, and
/// the full repair afterwards must still come back clean.
#[tokio::test]
async fn a_repair_that_died_halfway_is_finished_by_the_next_run() {
    let dir = tempdir().unwrap();
    let (shared, fs) = store(&dir);
    fs.create_bucket("b").unwrap();

    let live = put(&fs, "b", "k", b"a live block".repeat(20).to_vec()).await;
    plant_inflated_rc(&shared, live, 4);
    let orphan = synthetic_id(0x06);
    plant_orphan_file(fs.fs_root(), &orphan, 1, b"nobody's bytes");
    let gone = synthetic_id(0x07);
    plant_dangling_record(&shared, gone, 1);
    plant_object(&fs, "b", "damaged", vec![gone]).await;
    std::fs::write(fs.fs_root().join("NOTES"), b"foreign").unwrap();

    // The strict subset the killed run got through.
    {
        let ctx = RepairContext::new(&fs);
        for action in [
            RepairAction::QuarantineForeign {
                path: fs.fs_root().join("NOTES"),
            },
            RepairAction::MarkDegraded { block: gone },
        ] {
            let outcomes = action.apply(&ctx).await;
            assert_eq!(outcomes[0].status, RepairStatus::Applied, "{outcomes:#?}");
        }
    }

    let summary = report_and_repair(&fs, ScrubOptions::metadata_only()).await;

    assert_eq!(rc(&fs, live), 1, "the run picked up where it stopped");
    assert!(!block_disk_path(&orphan, 1, fs.fs_root().clone()).exists());
    assert!(record(&fs, gone).unwrap().is_degraded());
    assert!(quarantine_dir(&fs).join("NOTES").is_file());
    assert_eq!(
        std::fs::read_dir(quarantine_dir(&fs)).unwrap().count(),
        1,
        "the already-quarantined file was not quarantined twice"
    );
    assert_eq!(summary.exit_code(), exit_code::CLEAN, "{}", summary.report);
}

/// Hard rule 2: a class this engine claims to repair that is still standing
/// afterwards is CRITICAL, whatever that finding's own severity is. An
/// orphan file is INFO; an orphan file that survived a repair is a repair
/// that did not work.
#[tokio::test]
async fn a_repairable_finding_that_survives_the_repair_is_critical() {
    let dir = tempdir().unwrap();
    let (_shared, fs) = store(&dir);
    let ctx = RepairContext::new(&fs);
    let report = engine::run(&ctx.scrub_context(), &ScrubOptions::metadata_only()).unwrap();

    let dirty = flag_unfinished(Report::new(
        report.store.clone(),
        report.passes_run.clone(),
        vec![
            Finding::new(FindingClass::OrphanFile, "an orphan nobody deleted"),
            Finding::new(FindingClass::MissingBlockRecord, "not repairable by anyone"),
        ],
    ));

    assert_eq!(dirty.exit_code, exit_code::CRITICAL);
    let flagged: Vec<_> = dirty
        .findings
        .iter()
        .filter(|f| f.class == FindingClass::PostRepairRecountDirty)
        .collect();
    assert_eq!(flagged.len(), 1, "one per surviving class: {flagged:#?}");
    assert!(flagged[0].evidence.contains("orphan_file"), "{flagged:#?}");

    // A store with only unrepairable findings left is not called dirty:
    // nothing claimed to repair them.
    let untouched = flag_unfinished(Report::new(
        report.store,
        report.passes_run,
        vec![Finding::new(FindingClass::MissingBlockRecord, "still gone")],
    ));
    assert!(
        !untouched
            .findings
            .iter()
            .any(|f| f.class == FindingClass::PostRepairRecountDirty),
        "{untouched}"
    );
}

/// The summary is a machine contract like the report is: documented field
/// names, statuses that match their `as_str`, and the post-repair report
/// nested whole.
#[tokio::test]
async fn the_summary_serializes_to_the_documented_shape() {
    let dir = tempdir().unwrap();
    let (_shared, fs) = store(&dir);
    std::fs::write(fs.fs_root().join("README"), b"foreign").unwrap();

    let summary = report_and_repair(&fs, ScrubOptions::metadata_only()).await;
    let json: serde_json::Value =
        serde_json::from_str(&serde_json::to_string(&summary).unwrap()).unwrap();

    assert_eq!(json["version"], REPAIR_VERSION);
    assert_eq!(json["counts"]["applied"], 1);
    assert_eq!(json["counts"]["total"], 1);
    let outcome = &json["outcomes"][0];
    assert_eq!(outcome["action"], "quarantine_foreign");
    assert_eq!(outcome["status"], "applied");
    assert_eq!(outcome["severity"], "info");
    assert!(outcome["path"].as_str().unwrap().ends_with("README"));
    assert!(outcome["detail"].as_str().unwrap().contains("quarantine"));
    // A block-less outcome carries no empty key.
    assert!(outcome.get("block").is_none(), "{outcome}");
    // The post-repair report is the same document the scrub emits.
    assert_eq!(json["report"]["version"], 1);
    assert_eq!(json["report"]["exit_code"], 0);

    for status in [
        RepairStatus::Applied,
        RepairStatus::Skipped,
        RepairStatus::Failed,
    ] {
        assert_eq!(
            serde_json::to_value(status).unwrap(),
            serde_json::Value::String(status.as_str().to_string())
        );
    }

    let text = summary.render_text();
    assert!(text.contains("APPLIED quarantine_foreign"), "{text}");
    assert!(text.contains("1 applied, 0 skipped, 0 failed"), "{text}");
    assert!(text.contains("no findings"), "{text}");
}
