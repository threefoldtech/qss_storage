//! Repair tests: every action against the residue it exists for, plus the
//! three properties the ADR demands of the set as a whole -- idempotent,
//! re-runnable after a kill, and refusing every rc mutation when the holder
//! set was not closed.

use std::path::{Path, PathBuf};

use tempfile::tempdir;

use super::*;
use crate::cas::crash_fixtures::{
    plant_bit_flip, plant_dangling_record, plant_deflated_rc, plant_half_deleted_bucket,
    plant_inflated_rc, plant_orphan_file, plant_stale_part,
};
use crate::metastore::{ContentHash, Object, ObjectData};
use crate::scrub::report::exit_code;
use crate::scrub::tests::{plant_object, put, store, synthetic_id};

/// The tool's workflow: report first, repair second, check third.
async fn report_and_repair(fs: &CasFS, options: ScrubOptions) -> RepairSummary {
    let ctx = RepairContext::new(fs);
    let report = engine::run(&ctx.scrub_context(), &options).unwrap();
    repair(&ctx, &report, &options).await.unwrap()
}

/// The record for `id`, or `None` if there is none.
fn record(fs: &CasFS, id: BlockId) -> Option<Block> {
    fs.shared_block_store()
        .block_tree()
        .get_block(id.as_slice())
        .unwrap()
}

/// The rc the record for `id` states; panics if there is no record.
fn rc(fs: &CasFS, id: BlockId) -> usize {
    record(fs, id).expect("the record must exist").rc()
}

/// Where the record for `id` says its file is.
fn recorded_path(fs: &CasFS, id: BlockId) -> PathBuf {
    let depth = record(fs, id).expect("the record must exist").depth();
    block_disk_path(&id, depth, fs.fs_root().clone())
}

fn quarantine_dir(fs: &CasFS) -> PathBuf {
    fs.fs_root().join(QUARANTINE_DIR_NAME)
}

/// Moves a block file from one fanout depth to another: the placement-drift
/// shape that leaves a record pointing where its bytes no longer are.
fn misplace_block_file(root: &Path, id: &BlockId, from: u8, to: u8) {
    let source = block_disk_path(id, from, root.to_path_buf());
    let dest = block_disk_path(id, to, root.to_path_buf());
    std::fs::create_dir_all(dest.parent().unwrap()).unwrap();
    std::fs::rename(&source, &dest).unwrap();
}

/// One record, as a comparable value: address, rc, depth, size, degraded.
type RecordState = (String, usize, u8, usize, bool);

/// One block file, as a comparable value: path and size.
type FileState = (String, u64);

/// Every record and every block file: the end state two runs of an
/// idempotent repair must agree on.
fn snapshot(fs: &CasFS) -> (Vec<RecordState>, Vec<FileState>) {
    let shared = fs.shared_block_store();
    let ctx = ScrubContext::new(fs.namespace_meta_store(), shared);

    let mut records: Vec<_> = crate::scrub::records::walk_records(&ctx)
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

    let mut files: Vec<_> = crate::scrub::disk::walk_disk(&ctx)
        .unwrap()
        .files
        .iter()
        .map(|file| (file.path.to_string_lossy().into_owned(), file.size))
        .collect();
    files.sort();

    (records, files)
}

/// The outcomes of one kind, for asserting on what an action said.
fn of_kind<'a>(summary: &'a RepairSummary, kind: &str) -> Vec<&'a RepairOutcome> {
    summary
        .outcomes
        .iter()
        .filter(|outcome| outcome.action == kind)
        .collect()
}

/// The recount, both directions. Raising an under-count is deliberate (ADR
/// 0005): the CRITICAL finding and the nonzero exit of the report that
/// authorised it stay, but leaving a known under-count in place leaves a
/// premature free armed.
#[tokio::test]
async fn set_rc_lowers_a_leak_and_raises_an_under_count() {
    let dir = tempdir().unwrap();
    let (shared, fs) = store(&dir);
    fs.create_bucket("b").unwrap();

    let leaked = put(&fs, "b", "one", b"a leaked block".repeat(20).to_vec()).await;
    let under = put(
        &fs,
        "b",
        "two",
        b"an under-counted block".repeat(20).to_vec(),
    )
    .await;
    plant_inflated_rc(&shared, leaked, 3);
    plant_deflated_rc(&shared, under, 1);

    let summary = report_and_repair(&fs, ScrubOptions::metadata_only()).await;

    assert_eq!(rc(&fs, leaked), 1, "lowered to the walked truth");
    assert_eq!(rc(&fs, under), 1, "raised to the walked truth");
    assert!(recorded_path(&fs, leaked).is_file(), "both files stay");
    assert!(recorded_path(&fs, under).is_file());

    let set_rcs = of_kind(&summary, "set_rc");
    assert_eq!(set_rcs.len(), 2, "{:#?}", summary.outcomes);
    assert!(set_rcs.iter().all(|o| o.status == RepairStatus::Applied));
    assert!(
        set_rcs.iter().any(|o| o.detail.contains("rc 4 -> 1")),
        "{set_rcs:#?}"
    );
    assert!(
        set_rcs.iter().any(|o| o.detail.contains("rc 0 -> 1")),
        "{set_rcs:#?}"
    );

    assert_eq!(summary.exit_code(), exit_code::CLEAN, "{}", summary.report);
}

/// A recount of zero means the block is free. The protocol has no rc=0
/// state, so the record goes and the file goes with it -- otherwise the
/// next decrement of that record would underflow, and `blocks/` would only
/// ever grow.
#[tokio::test]
async fn a_record_nothing_holds_is_freed_with_its_file() {
    let dir = tempdir().unwrap();
    let (shared, fs) = store(&dir);
    let leaked = synthetic_id(0x01);
    plant_dangling_record(&shared, leaked, 1);
    plant_orphan_file(fs.fs_root(), &leaked, 1, &vec![7u8; 1234]);
    let path = block_disk_path(&leaked, 1, fs.fs_root().clone());
    // The same shape without a file: freeing beats marking it degraded,
    // because a degraded record with no holder would sit there forever.
    let fileless = synthetic_id(0x11);
    plant_dangling_record(&shared, fileless, 1);

    let summary = report_and_repair(&fs, ScrubOptions::metadata_only()).await;

    assert!(record(&fs, leaked).is_none(), "the record is gone");
    assert!(!path.exists(), "and so are its bytes");
    assert!(record(&fs, fileless).is_none(), "freed, not degraded");
    assert!(
        of_kind(&summary, "set_rc")
            .iter()
            .all(|o| o.detail.contains("rc 1 -> 0")),
        "{:#?}",
        summary.outcomes
    );
    assert_eq!(
        of_kind(&summary, "mark_degraded")[0].status,
        RepairStatus::Skipped,
        "the record it would have flagged is already freed"
    );
    assert_eq!(summary.exit_code(), exit_code::CLEAN, "{}", summary.report);
}

/// Files nothing can reference are deleted; the live block is not touched.
#[tokio::test]
async fn orphan_and_off_depth_files_are_deleted() {
    let dir = tempdir().unwrap();
    let (_shared, fs) = store(&dir);
    fs.create_bucket("b").unwrap();

    let live = put(&fs, "b", "k", b"a live block".repeat(30).to_vec()).await;
    let live_path = recorded_path(&fs, live);
    let off_depth = block_disk_path(&live, 4, fs.fs_root().clone());
    plant_orphan_file(fs.fs_root(), &live, 4, b"a stale copy");

    let orphan = synthetic_id(0x02);
    let orphan_path = block_disk_path(&orphan, 1, fs.fs_root().clone());
    plant_orphan_file(fs.fs_root(), &orphan, 1, b"nobody's bytes");

    let summary = report_and_repair(&fs, ScrubOptions::metadata_only()).await;

    assert!(!orphan_path.exists(), "the orphan is gone");
    assert!(!off_depth.exists(), "so is the off-depth duplicate");
    assert!(live_path.is_file(), "the referenced block is untouched");
    assert_eq!(rc(&fs, live), 1);
    assert_eq!(of_kind(&summary, "delete_orphan").len(), 1);
    assert_eq!(of_kind(&summary, "delete_off_depth").len(), 1);
    assert_eq!(summary.exit_code(), exit_code::CLEAN, "{}", summary.report);
}

/// Quarantine, never delete (ADR 0005 hard rule 4): fsck did not put a
/// foreign file there and does not know what it is. A name already taken in
/// quarantine gets a suffix rather than overwriting what an earlier run set
/// aside.
#[tokio::test]
async fn a_foreign_file_is_quarantined_not_deleted() {
    let dir = tempdir().unwrap();
    let (_shared, fs) = store(&dir);
    std::fs::write(fs.fs_root().join("README"), b"someone else's notes").unwrap();

    let summary = report_and_repair(&fs, ScrubOptions::metadata_only()).await;

    assert!(!fs.fs_root().join("README").exists());
    assert_eq!(
        std::fs::read(quarantine_dir(&fs).join("README")).unwrap(),
        b"someone else's notes"
    );
    assert_eq!(of_kind(&summary, "quarantine_foreign").len(), 1);
    assert_eq!(summary.exit_code(), exit_code::CLEAN, "{}", summary.report);

    // A second file of the same name does not overwrite the first.
    std::fs::write(fs.fs_root().join("README"), b"a different README").unwrap();
    report_and_repair(&fs, ScrubOptions::metadata_only()).await;
    assert_eq!(
        std::fs::read(quarantine_dir(&fs).join("README")).unwrap(),
        b"someone else's notes"
    );
    assert_eq!(
        std::fs::read(quarantine_dir(&fs).join("README.1")).unwrap(),
        b"a different README"
    );
}

/// A block whose bytes do not hash to its name: the file is set aside and
/// the record is marked degraded, so the next PUT of that content writes it
/// fresh instead of deduplicating against a poisoned record.
#[tokio::test]
async fn a_corrupt_block_is_quarantined_and_its_record_marked_degraded() {
    let dir = tempdir().unwrap();
    let (_shared, fs) = store(&dir);
    fs.create_bucket("b").unwrap();

    let id = put(&fs, "b", "k", b"bytes that rot".repeat(20).to_vec()).await;
    let depth = record(&fs, id).unwrap().depth();
    let path = recorded_path(&fs, id);
    plant_bit_flip(fs.fs_root(), &id, depth);

    let summary = report_and_repair(&fs, ScrubOptions::full()).await;

    assert!(!path.exists(), "the corrupt file left the blocks root");
    assert!(
        quarantine_dir(&fs).join(id.to_hex()).is_file(),
        "and it is in quarantine under its own name, not deleted"
    );
    let record = record(&fs, id).expect("the record survives the block");
    assert!(record.is_degraded());
    assert_eq!(record.rc(), 1, "its holder stays accounted for");
    assert_eq!(of_kind(&summary, "quarantine_corrupt").len(), 1);
    assert_eq!(
        summary.exit_code(),
        exit_code::CLEAN,
        "documented damage is INFO: {}",
        summary.report
    );
}

/// Adoption verifies before it trusts (hard rule 3): fsck does not have the
/// writer's bytes, so a file found under an address is re-hashed before any
/// record is pointed at it.
#[tokio::test]
async fn adoption_re_hashes_the_candidate_before_pointing_a_record_at_it() {
    let dir = tempdir().unwrap();
    let (_shared, fs) = store(&dir);
    fs.create_bucket("b").unwrap();

    let data = b"content that moved".repeat(20).to_vec();
    let id = put(&fs, "b", "k", data.clone()).await;
    let from = record(&fs, id).unwrap().depth();
    let to = from + 1;
    misplace_block_file(fs.fs_root(), &id, from, to);

    let summary = report_and_repair(&fs, ScrubOptions::metadata_only()).await;

    let adopted = record(&fs, id).expect("the record survives");
    assert_eq!(adopted.depth(), to, "the record follows the file");
    assert_eq!(adopted.rc(), 1, "rc is not adoption's business");
    assert!(!adopted.is_degraded());
    assert_eq!(std::fs::read(recorded_path(&fs, id)).unwrap(), data);
    assert!(
        of_kind(&summary, "adopt_orphan")[0]
            .detail
            .contains("re-hashed"),
        "{:#?}",
        summary.outcomes
    );
    assert_eq!(summary.exit_code(), exit_code::CLEAN, "{}", summary.report);
}

/// A candidate that does not hash to the address it sits under is corrupt
/// residue, not the missing block: it is quarantined and the record is
/// marked degraded, which is where a record with no candidate at all ends
/// up.
#[tokio::test]
async fn an_adoption_candidate_that_does_not_hash_goes_to_quarantine() {
    let dir = tempdir().unwrap();
    let (_shared, fs) = store(&dir);
    fs.create_bucket("b").unwrap();

    let id = put(&fs, "b", "k", b"the real content".repeat(20).to_vec()).await;
    let depth = record(&fs, id).unwrap().depth();
    // The record's own bytes vanish, and something else takes their name at
    // another depth.
    std::fs::remove_file(recorded_path(&fs, id)).unwrap();
    plant_orphan_file(fs.fs_root(), &id, depth + 1, b"not what this address names");

    let summary = report_and_repair(&fs, ScrubOptions::metadata_only()).await;

    let record = record(&fs, id).expect("the record survives");
    assert!(record.is_degraded(), "the bytes really are gone");
    assert_eq!(record.rc(), 1, "the damaged object stays accounted for");
    assert_eq!(
        std::fs::read(quarantine_dir(&fs).join(id.to_hex())).unwrap(),
        b"not what this address names",
        "the impostor is set aside, not adopted and not deleted"
    );
    let outcome = of_kind(&summary, "adopt_orphan")[0];
    assert_eq!(outcome.status, RepairStatus::Applied);
    assert!(outcome.detail.contains("hashes to"), "{outcome:#?}");
    assert_eq!(summary.exit_code(), exit_code::CLEAN, "{}", summary.report);
}

/// A record whose bytes are nowhere is marked degraded rather than removed:
/// removing it would let a later PUT heal the block at rc=1 while the old
/// holders still reference it, and the lie would break with no operation on
/// them.
#[tokio::test]
async fn an_unrecoverable_record_is_marked_degraded_not_removed() {
    let dir = tempdir().unwrap();
    let (shared, fs) = store(&dir);
    fs.create_bucket("b").unwrap();

    let gone = synthetic_id(0x03);
    plant_dangling_record(&shared, gone, 1);
    plant_object(&fs, "b", "damaged", vec![gone, gone]);

    let summary = report_and_repair(&fs, ScrubOptions::metadata_only()).await;

    let record = record(&fs, gone).expect("the record must survive");
    assert!(record.is_degraded());
    assert_eq!(record.rc(), 2, "both occurrences stay counted");
    assert_eq!(of_kind(&summary, "mark_degraded").len(), 1);
    assert_eq!(summary.exit_code(), exit_code::CLEAN, "{}", summary.report);
}

/// Resuming a teardown deletes through the daemon's own striped path, so
/// every block the stranded objects held is decremented exactly as the
/// crashed teardown would have decremented it.
#[tokio::test]
async fn a_half_deleted_bucket_teardown_is_resumed() {
    let dir = tempdir().unwrap();
    let (_shared, fs) = store(&dir);
    fs.create_bucket("kept").unwrap();
    fs.create_bucket("doomed").unwrap();

    let shared_block = put(&fs, "kept", "k", b"deduplicated bytes".repeat(20).to_vec()).await;
    put(
        &fs,
        "doomed",
        "same",
        b"deduplicated bytes".repeat(20).to_vec(),
    )
    .await;
    let only_doomed = put(
        &fs,
        "doomed",
        "own",
        b"only this bucket".repeat(20).to_vec(),
    )
    .await;
    assert_eq!(rc(&fs, shared_block), 2, "one block, two objects");
    let only_path = recorded_path(&fs, only_doomed);

    plant_half_deleted_bucket(fs.namespace_meta_store(), "doomed");

    let summary = report_and_repair(&fs, ScrubOptions::metadata_only()).await;

    assert!(
        !fs.namespace_meta_store()
            .list_trees()
            .unwrap()
            .iter()
            .any(|tree| tree == "doomed"),
        "the stranded tree is gone"
    );
    assert_eq!(rc(&fs, shared_block), 1, "the surviving holder keeps it");
    assert!(recorded_path(&fs, shared_block).is_file());
    assert!(record(&fs, only_doomed).is_none(), "nothing holds it now");
    assert!(!only_path.exists());
    assert_eq!(of_kind(&summary, "resume_bucket_teardown").len(), 1);
    assert_eq!(summary.exit_code(), exit_code::CLEAN, "{}", summary.report);
}

/// An object key that is not UTF-8 cannot go through the daemon's delete
/// path, which takes a `&str` and asserts it. fsck must report it and carry
/// on, never panic: the recount that follows reconciles what that object
/// was holding.
#[tokio::test]
async fn a_non_utf8_object_key_is_warned_about_not_panicked_on() {
    let dir = tempdir().unwrap();
    let (_shared, fs) = store(&dir);
    fs.create_bucket("doomed").unwrap();

    let id = put(
        &fs,
        "doomed",
        "readable",
        b"shared by both records".repeat(8).to_vec(),
    )
    .await;
    let path = recorded_path(&fs, id);
    // A second holder of the same block, under a key no `&str` can name.
    let object = Object::new(
        64,
        ContentHash::from([7u8; 16]),
        ObjectData::SinglePart { blocks: vec![id] },
    );
    fs.get_bucket("doomed")
        .unwrap()
        .insert(&[0xff, 0xfe], object.to_vec())
        .unwrap();

    plant_half_deleted_bucket(fs.namespace_meta_store(), "doomed");

    let summary = report_and_repair(&fs, ScrubOptions::metadata_only()).await;

    let warned: Vec<_> = of_kind(&summary, "resume_bucket_teardown")
        .into_iter()
        .filter(|outcome| outcome.severity == Severity::Warn)
        .collect();
    assert_eq!(warned.len(), 1, "{:#?}", summary.outcomes);
    assert!(warned[0].detail.contains("not UTF-8"), "{warned:#?}");
    assert_eq!(warned[0].status, RepairStatus::Skipped);

    assert!(
        !fs.namespace_meta_store()
            .list_trees()
            .unwrap()
            .iter()
            .any(|tree| tree == "doomed"),
        "the teardown still finishes: the _BUCKETS row was already gone"
    );
    assert!(record(&fs, id).is_none(), "no holder is left");
    assert!(!path.exists());
    assert_eq!(summary.exit_code(), exit_code::CLEAN, "{}", summary.report);
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
    plant_object(&fs, "b", "damaged", vec![gone]);
    let adoptable = put(&fs, "b", "moved", b"content that moved".repeat(20).to_vec()).await;
    let depth = record(&fs, adoptable).unwrap().depth();
    misplace_block_file(fs.fs_root(), &adoptable, depth, depth + 2);
    std::fs::write(fs.fs_root().join("NOTES"), b"foreign").unwrap();
    put(&fs, "stranded", "k", b"still referenced".repeat(8).to_vec()).await;
    plant_half_deleted_bucket(fs.namespace_meta_store(), "stranded");
    plant_stale_part(&fs, "b", "big", "u-1", 1, vec![live]);

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
    plant_object(&fs, "b", "damaged", vec![gone]);
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
