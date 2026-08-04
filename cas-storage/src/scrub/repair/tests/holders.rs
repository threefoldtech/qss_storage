use tempfile::tempdir;

use super::*;
use crate::cas::crash_fixtures::{plant_half_deleted_bucket, plant_upload_record};
use crate::metastore::{ContentHash, Object, ObjectData};

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

/// The orphan-part reap, end to end (ADR 0003 decision 9): the record goes
/// through the daemon's take-style primitive, the references it held are
/// released, and the block files go with them when nothing else holds
/// them. A live upload's part -- one whose upload record exists -- is
/// reported and left exactly where it is.
#[tokio::test]
async fn an_orphan_part_is_reaped_and_a_live_uploads_part_is_not() {
    let dir = tempdir().unwrap();
    let (_shared, fs) = store(&dir);
    fs.create_bucket("b").unwrap();

    // A live upload, an hour old: the upload record is what makes it live.
    plant_upload_record(
        &fs,
        "b",
        "big",
        "live",
        chrono::Utc::now().timestamp() - 60 * 60,
    );
    let live = put_part(
        &fs,
        "b",
        "big",
        "live",
        1,
        b"a live part".repeat(64).to_vec(),
    )
    .await;

    // An orphan: no upload record, its blocks held by the part record alone.
    let doomed = put_part(
        &fs,
        "b",
        "big",
        "gone",
        2,
        b"an abandoned part".repeat(64).to_vec(),
    )
    .await;

    // And an orphan whose content a live object shares: the release drops
    // the part's OCCURRENCE, and the object keeps the block.
    let data = b"content two holders share".repeat(64).to_vec();
    let object = put(&fs, "b", "object", data.clone()).await;
    let shared_blocks = put_part(&fs, "b", "big", "gone", 3, data).await;
    assert_eq!(
        shared_blocks,
        vec![object],
        "the premise: one block, two holders"
    );
    assert_eq!(rc(&fs, object), 2);

    let summary = report_and_repair(&fs, ScrubOptions::metadata_only()).await;

    let reaped = of_kind(&summary, "reap_orphan_part");
    assert_eq!(reaped.len(), 2, "{:#?}", summary.outcomes);
    assert!(reaped.iter().all(|o| o.status == RepairStatus::Applied));
    assert!(
        reaped.iter().any(|o| o.detail.contains("upload gone")),
        "{reaped:#?}"
    );
    // The outcome names the record by the key it was filed under.
    assert!(reaped.iter().all(|o| o.path.is_some()), "{reaped:#?}");

    for id in &doomed {
        assert!(record(&fs, *id).is_none(), "the last reference is gone");
        assert!(
            !block_disk_path(id, 1, fs.fs_root().clone()).exists(),
            "and so is the file"
        );
    }
    assert_eq!(rc(&fs, object), 1, "the object keeps the block it shares");
    assert!(recorded_path(&fs, object).is_file());

    assert_eq!(
        fs.upload_parts("b", "big", "live").unwrap().len(),
        1,
        "the live upload's part is untouched"
    );
    for id in &live {
        assert_eq!(rc(&fs, *id), 1);
        assert!(recorded_path(&fs, *id).is_file());
    }

    // Clean afterwards: no orphan_part survives, and the live upload is
    // still reported as the INFO it is.
    assert_eq!(summary.exit_code(), exit_code::CLEAN, "{}", summary.report);
    assert!(
        !summary
            .report
            .findings
            .iter()
            .any(|f| f.class == FindingClass::OrphanPart),
        "{}",
        summary.report
    );
    assert_eq!(
        summary
            .report
            .findings
            .iter()
            .filter(|f| f.class == FindingClass::MultipartUpload)
            .count(),
        1,
        "{}",
        summary.report
    );
}
