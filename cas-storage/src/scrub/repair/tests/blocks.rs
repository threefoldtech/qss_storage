use tempfile::tempdir;

use super::*;
use crate::cas::crash_fixtures::{
    plant_bit_flip, plant_dangling_record, plant_deflated_rc, plant_inflated_rc, plant_orphan_file,
};
use crate::scrub::tests::{plant_object, put, store, synthetic_id};

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
    plant_object(&fs, "b", "damaged", vec![gone, gone]).await;

    let summary = report_and_repair(&fs, ScrubOptions::metadata_only()).await;

    let record = record(&fs, gone).expect("the record must survive");
    assert!(record.is_degraded());
    assert_eq!(record.rc(), 2, "both occurrences stay counted");
    assert_eq!(of_kind(&summary, "mark_degraded").len(), 1);
    assert_eq!(summary.exit_code(), exit_code::CLEAN, "{}", summary.report);
}
