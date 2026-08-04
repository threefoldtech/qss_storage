use chrono::Utc;

use super::multipart::humanize_age;
use super::*;
use crate::cas::multipart::MultiPart;
use crate::metastore::{MULTIPART_PARTS_TREE, UPLOADS_TREE};
use crate::scrub::disk::walk_disk;
use crate::scrub::holders::HolderEnumerationError;
use crate::scrub::records::walk_records;
use crate::scrub::tests::{plant_object, put, store, synthetic_id};
use crate::scrub::{Severity, holders::expected_counts};

use crate::cas::crash_fixtures::{
    plant_dangling_record, plant_degraded_record, plant_orphan_file, plant_upload_record,
};
use tempfile::tempdir;

/// Finds the one finding of `class`, or fails loudly.
fn only(findings: &[Finding], class: FindingClass) -> &Finding {
    let mut hits = findings.iter().filter(|f| f.class == class);
    let first = hits
        .next()
        .unwrap_or_else(|| panic!("no {class:?} in {findings:#?}"));
    assert!(hits.next().is_none(), "more than one {class:?}");
    first
}

#[tokio::test]
async fn recount_reports_both_directions_and_the_missing_record() {
    let dir = tempdir().unwrap();
    let (shared, fs) = store(&dir);
    fs.create_bucket("b").unwrap();

    // Two objects, one block: rc 2, holders 2. Clean.
    let clean = put(&fs, "b", "one", b"clean block".repeat(20).to_vec()).await;
    put(&fs, "b", "two", b"clean block".repeat(20).to_vec()).await;

    // A record nothing references: the cancelled-PUT shape.
    let leaked = synthetic_id(0x91);
    plant_dangling_record(&shared, leaked, 1);

    // A holder referencing a block that has no record at all.
    let vanished = synthetic_id(0x92);
    plant_object(&fs, "b", "points-nowhere", vec![vanished]).await;

    // An under-count: three holders, a record that says one.
    let under = synthetic_id(0x93);
    plant_dangling_record(&shared, under, 1);
    plant_object(&fs, "b", "u1", vec![under, under, under]).await;

    let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
    let records = walk_records(&ctx).unwrap();
    let expected = expected_counts(&ctx).unwrap();
    let findings = recount(&expected, &records);

    assert!(
        !findings
            .iter()
            .any(|f| f.block.as_deref() == Some(clean.to_hex().as_str())),
        "an exact match is not a finding: {findings:#?}"
    );

    let over = only(&findings, FindingClass::RefcountOverCount);
    assert_eq!(over.block.as_deref(), Some(leaked.to_hex().as_str()));
    assert_eq!(over.severity, Severity::Info);

    let missing = only(&findings, FindingClass::MissingBlockRecord);
    assert_eq!(missing.block.as_deref(), Some(vanished.to_hex().as_str()));
    assert_eq!(missing.severity, Severity::Critical);

    let under_finding = only(&findings, FindingClass::RefcountUnderCount);
    assert_eq!(
        under_finding.block.as_deref(),
        Some(under.to_hex().as_str())
    );
    assert_eq!(under_finding.severity, Severity::Critical);
    assert!(under_finding.evidence.contains("rc 1"), "{under_finding:?}");
}

#[tokio::test]
async fn disk_sweep_separates_orphans_off_depth_and_size_damage() {
    let dir = tempdir().unwrap();
    let (shared, fs) = store(&dir);
    fs.create_bucket("b").unwrap();

    let live = put(&fs, "b", "k", b"a live block".repeat(30).to_vec()).await;
    // A second copy of the live block at a depth the record does not name.
    plant_orphan_file(fs.fs_root(), &live, 3, b"stale copy");
    // A file no record mentions.
    let orphan = synthetic_id(0xa1);
    plant_orphan_file(fs.fs_root(), &orphan, 1, b"nobody's block");
    // Something that is not part of the layout.
    std::fs::write(fs.fs_root().join("NOTES.txt"), b"hello").unwrap();

    let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
    let records = walk_records(&ctx).unwrap();
    let findings = disk_sweep(&walk_disk(&ctx).unwrap(), &records);

    let off = only(&findings, FindingClass::OffDepthFile);
    assert_eq!(off.block.as_deref(), Some(live.to_hex().as_str()));
    assert_eq!(off.severity, Severity::Info);

    let orphan_finding = only(&findings, FindingClass::OrphanFile);
    assert_eq!(
        orphan_finding.block.as_deref(),
        Some(orphan.to_hex().as_str())
    );

    let foreign = only(&findings, FindingClass::ForeignFile);
    assert_eq!(foreign.severity, Severity::Warn);
    assert!(foreign.path.as_ref().unwrap().ends_with("NOTES.txt"));

    // No size damage yet: truncate the live file and the pass must see it.
    assert!(
        !findings
            .iter()
            .any(|f| f.class == FindingClass::SizeMismatch)
    );
    let live_path = records.records[&live].disk_path(&live, fs.fs_root().clone());
    std::fs::write(&live_path, b"short").unwrap();
    let findings = disk_sweep(&walk_disk(&ctx).unwrap(), &records);
    let size = only(&findings, FindingClass::SizeMismatch);
    assert_eq!(size.severity, Severity::Critical);
    assert!(size.evidence.contains("5 bytes"), "{size:?}");
}

/// The classification the repair depends on: a record whose id is on
/// disk somewhere is adoptable, one whose id is nowhere is not.
#[tokio::test]
async fn adoptable_and_unrecoverable_dangling_records_are_different_classes() {
    let dir = tempdir().unwrap();
    let (shared, fs) = store(&dir);
    fs.create_bucket("b").unwrap();

    // Record says depth 1; the bytes are actually at depth 2.
    let adoptable = synthetic_id(0xb1);
    plant_dangling_record(&shared, adoptable, 1);
    plant_orphan_file(fs.fs_root(), &adoptable, 2, b"the missing bytes");

    // Record says depth 1 and nothing is anywhere.
    let gone = synthetic_id(0xb2);
    plant_dangling_record(&shared, gone, 1);
    plant_object(&fs, "b", "damaged", vec![gone]).await;

    let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
    let records = walk_records(&ctx).unwrap();
    let findings = dangling_sweep(&ctx, &records, &walk_disk(&ctx).unwrap());

    let candidate = only(&findings, FindingClass::AdoptableDanglingRecord);
    assert_eq!(
        candidate.block.as_deref(),
        Some(adoptable.to_hex().as_str())
    );
    assert_eq!(candidate.severity, Severity::Info);
    assert!(candidate.evidence.contains("depth 2"), "{candidate:?}");

    let dead = only(&findings, FindingClass::DanglingRecord);
    assert_eq!(dead.block.as_deref(), Some(gone.to_hex().as_str()));
    assert_eq!(dead.severity, Severity::Critical);
    assert!(dead.evidence.contains("poisons dedup"), "{dead:?}");
    assert_eq!(
        dead.holders,
        vec![crate::scrub::HolderRef::Object {
            bucket: "b".to_string(),
            key: "damaged".to_string(),
        }],
        "the blast radius must name the damaged object"
    );
}

/// One file, one finding. A misplaced file is an off-depth duplicate
/// only while the record's own file is there to make it redundant; with
/// the record's file gone, the same file is the adoption candidate and
/// the disk sweep says nothing about it.
#[test]
fn a_misplaced_file_is_reported_once_by_whichever_pass_owns_it() {
    let dir = tempdir().unwrap();
    let (shared, fs) = store(&dir);
    let id = synthetic_id(0xe1);

    // Record at depth 1 with no file of its own; the bytes are at 2.
    plant_dangling_record(&shared, id, 1);
    plant_orphan_file(fs.fs_root(), &id, 2, b"the only copy");

    let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
    let records = walk_records(&ctx).unwrap();
    let disk = walk_disk(&ctx).unwrap();

    let sweep = disk_sweep(&disk, &records);
    assert!(
        sweep.is_empty(),
        "the candidate belongs to the dangling sweep: {sweep:#?}"
    );
    let dangling = dangling_sweep(&ctx, &records, &disk);
    assert_eq!(dangling.len(), 1, "{dangling:#?}");
    only(&dangling, FindingClass::AdoptableDanglingRecord);

    // Give the record its own file back: now the depth-2 copy really is
    // a redundant duplicate, and the disk sweep owns it.
    plant_orphan_file(fs.fs_root(), &id, 1, b"the only copy");
    let disk = walk_disk(&ctx).unwrap();

    let sweep = disk_sweep(&disk, &records);
    let off = only(&sweep, FindingClass::OffDepthFile);
    assert_eq!(off.severity, Severity::Info);
    assert!(off.path.as_ref().unwrap().contains("/e1/e1/"), "{off:?}");
    assert!(
        dangling_sweep(&ctx, &records, &disk).is_empty(),
        "the record's file is present, so nothing dangles"
    );
}

/// A degraded record is known damage: INFO, and never counted as a
/// fresh dangling discovery.
#[test]
fn a_degraded_record_is_reported_as_known_damage() {
    let dir = tempdir().unwrap();
    let (shared, fs) = store(&dir);
    let degraded = synthetic_id(0xc1);
    plant_degraded_record(&shared, degraded, 1, 3, 4096);

    let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
    let records = walk_records(&ctx).unwrap();
    let findings = dangling_sweep(&ctx, &records, &walk_disk(&ctx).unwrap());

    assert_eq!(findings.len(), 1, "{findings:#?}");
    let finding = only(&findings, FindingClass::DegradedRecord);
    assert_eq!(finding.severity, Severity::Info);
    assert!(finding.evidence.contains("rc 3"), "{finding:?}");
}

#[tokio::test]
async fn the_corruption_scrub_catches_a_flipped_bit() {
    let dir = tempdir().unwrap();
    let (shared, fs) = store(&dir);
    fs.create_bucket("b").unwrap();
    let id = put(&fs, "b", "k", b"bytes that will rot".repeat(10).to_vec()).await;

    let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
    let disk = walk_disk(&ctx).unwrap();
    assert!(
        corruption_scrub(&ctx, &disk).is_empty(),
        "an intact store scrubs clean"
    );

    // Flip a bit under the same name and length: only a re-hash sees it.
    let path = &disk.files[0].path;
    let mut bytes = std::fs::read(path).unwrap();
    bytes[0] ^= 0x01;
    std::fs::write(path, &bytes).unwrap();

    let findings = corruption_scrub(&ctx, &walk_disk(&ctx).unwrap());
    let corrupt = only(&findings, FindingClass::CorruptBlock);
    assert_eq!(corrupt.severity, Severity::Critical);
    assert_eq!(corrupt.block.as_deref(), Some(id.to_hex().as_str()));
    assert!(corrupt.evidence.contains("hash to"), "{corrupt:?}");
}

/// Seconds ago, as a `created_at` a planted upload record carries.
fn started_ago(seconds: i64) -> i64 {
    Utc::now().timestamp() - seconds
}

/// Plants one part record of `upload_id`, naming one synthetic block.
fn plant_part(fs: &crate::cas::CasFS, key: &str, upload_id: &str, part_number: i64, size: usize) {
    fs.insert_multipart_part(
        "b".to_string(),
        key.to_string(),
        size,
        part_number,
        upload_id.to_string(),
        crate::metastore::ContentHash::from([1u8; 16]),
        vec![synthetic_id(0xd0u8.wrapping_add(part_number as u8))],
    )
    .unwrap();
}

#[test]
fn the_multipart_pass_groups_by_upload_and_reports_its_age() {
    let dir = tempdir().unwrap();
    let (shared, fs) = store(&dir);
    fs.create_bucket("b").unwrap();

    plant_upload_record(&fs, "b", "big", "u-1", started_ago(3 * 24 * 60 * 60));
    plant_part(&fs, "big", "u-1", 1, 1024);
    plant_part(&fs, "big", "u-1", 2, 512);
    // A second upload of the same key is a different unit of work, and
    // ages on its own clock.
    plant_upload_record(&fs, "b", "big", "u-2", started_ago(5 * 60 * 60));
    plant_part(&fs, "big", "u-2", 1, 64);
    // An upload nobody has sent a part for yet is still an upload.
    plant_upload_record(&fs, "b", "empty", "u-3", started_ago(90));

    let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
    let findings = multipart_report(&ctx).unwrap();

    assert_eq!(findings.len(), 3, "{findings:#?}");
    assert!(
        findings
            .iter()
            .all(|f| f.class == FindingClass::MultipartUpload && f.severity == Severity::Info)
    );

    let first = &findings[0];
    assert!(first.evidence.contains("u-1"), "{first:?}");
    assert!(first.evidence.contains("2 part(s)"), "{first:?}");
    assert!(first.evidence.contains("1536 byte(s)"), "{first:?}");
    assert!(
        first.evidence.contains("started 3 day(s) ago"),
        "the age ADR 0003 promised: {first:?}"
    );

    let second = &findings[1];
    assert!(second.evidence.contains("u-2"), "{second:?}");
    assert!(
        second.evidence.contains("started 5 hour(s) ago"),
        "an upload younger than a day reports hours: {second:?}"
    );

    // Sorted by (bucket, key, upload_id), so b/empty comes after b/big:
    // the upload nobody has uploaded a part for is reported all the same.
    let third = &findings[2];
    assert!(third.evidence.contains("u-3"), "{third:?}");
    assert!(
        third.evidence.contains("0 part(s), 0 byte(s)"),
        "an upload with no parts is reported on its age alone: {third:?}"
    );
    assert!(
        third.evidence.contains("started 1 minute(s) ago"),
        "{third:?}"
    );
}

/// The classification `--repair` acts on: the upload record decides. A
/// part whose triple has one belongs to a live upload and is reported
/// with it; a part whose triple has none is an orphan, reported on its
/// own and carrying the raw key its reaping needs.
#[test]
fn parts_without_an_upload_record_are_orphans_not_uploads() {
    let dir = tempdir().unwrap();
    let (shared, fs) = store(&dir);
    fs.create_bucket("b").unwrap();

    plant_upload_record(&fs, "b", "big", "live", started_ago(60 * 60));
    plant_part(&fs, "big", "live", 1, 4096);
    // Same bucket and key, no upload record: the abort-race residue.
    plant_part(&fs, "big", "gone", 7, 2048);

    let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
    let findings = multipart_report(&ctx).unwrap();

    assert_eq!(findings.len(), 2, "{findings:#?}");
    let live = only(&findings, FindingClass::MultipartUpload);
    assert!(live.evidence.contains("upload live"), "{live:?}");
    assert!(live.evidence.contains("1 part(s)"), "{live:?}");

    let orphan = only(&findings, FindingClass::OrphanPart);
    assert_eq!(orphan.severity, Severity::Info);
    assert!(orphan.evidence.contains("part 7"), "{orphan:?}");
    assert!(orphan.evidence.contains("upload gone"), "{orphan:?}");
    assert!(orphan.evidence.contains("2048 byte(s)"), "{orphan:?}");
    assert!(
        orphan.evidence.contains("upload record does not exist"),
        "{orphan:?}"
    );
    // The raw storage key, so an operator can find the record and repair
    // can name what it reaped. An ADR 0003 key is length-prefixed, so it
    // is not printable text and renders as hex.
    let rendered = orphan.path.as_deref().expect("the key is carried");
    assert_eq!(
        rendered,
        faster_hex::hex_string(&crate::cas::multipart::part_key("b", "big", "gone", 7)),
        "{orphan:?}"
    );
}

/// A legacy dash-keyed record can have no upload record at all -- no
/// point read can address it -- so it is an orphan by construction, and
/// its key, being printable text, is carried as itself.
#[test]
fn a_legacy_dash_keyed_part_is_an_orphan_with_a_readable_key() {
    let dir = tempdir().unwrap();
    let (shared, fs) = store(&dir);
    fs.create_bucket("b").unwrap();

    let record = MultiPart::new(
        1024,
        1,
        "b".to_string(),
        "big".to_string(),
        "u-legacy".to_string(),
        crate::metastore::ContentHash::from([7u8; 16]),
        vec![synthetic_id(0xd9)],
    );
    shared
        .meta_store()
        .get_tree_ext(MULTIPART_PARTS_TREE)
        .unwrap()
        .insert(b"b-big-u-legacy-1", record.to_vec())
        .unwrap();

    let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
    let findings = multipart_report(&ctx).unwrap();

    let orphan = only(&findings, FindingClass::OrphanPart);
    assert_eq!(orphan.path.as_deref(), Some("b-big-u-legacy-1"));
}

/// An upload record that does not decode takes the pass down, exactly as
/// an undecodable part record does: without the full set of upload
/// records a live part cannot be told from an orphan, and reaping a live
/// part is loss.
#[test]
fn an_undecodable_upload_record_refuses_the_pass() {
    let dir = tempdir().unwrap();
    let (shared, fs) = store(&dir);
    fs.create_bucket("b").unwrap();
    plant_part(&fs, "big", "u-1", 1, 128);

    shared
        .uploads_tree()
        .insert(b"not-a-real-key", vec![0xffu8; 3])
        .unwrap();

    let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
    match multipart_report(&ctx).unwrap_err() {
        HolderEnumerationError::UndecodableRecord { tree, .. } => {
            assert_eq!(tree, UPLOADS_TREE);
        }
        other => panic!("expected an undecodable-record refusal, got {other:?}"),
    }
}

/// Ages read as a person reads them, and a clock that went backwards is
/// shown as the skew it is rather than as a plausible zero.
#[test]
fn ages_are_rendered_in_the_coarsest_unit_that_fits() {
    assert_eq!(humanize_age(0), "0 second(s)");
    assert_eq!(humanize_age(59), "59 second(s)");
    assert_eq!(humanize_age(60), "1 minute(s)");
    assert_eq!(humanize_age(59 * 60), "59 minute(s)");
    assert_eq!(humanize_age(60 * 60), "1 hour(s)");
    assert_eq!(humanize_age(47 * 60 * 60), "1 day(s)");
    assert_eq!(humanize_age(-90), "-1 minute(s)");
    // An absurd timestamp must not panic the report that carries it.
    assert!(humanize_age(i64::MIN).starts_with('-'));
}

#[tokio::test]
async fn bucket_integrity_finds_a_tree_whose_row_is_gone() {
    let dir = tempdir().unwrap();
    let (shared, fs) = store(&dir);
    fs.create_bucket("kept").unwrap();
    fs.create_bucket("halfdeleted").unwrap();
    put(&fs, "halfdeleted", "k", b"still referenced".to_vec()).await;

    let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
    assert!(
        bucket_integrity(&ctx).unwrap().is_empty(),
        "both buckets have rows"
    );

    // Remove the row the way a crashed teardown would, leaving the tree.
    fs.namespace_meta_store()
        .get_allbuckets_tree()
        .unwrap()
        .remove(b"halfdeleted")
        .unwrap();

    let findings = bucket_integrity(&ctx).unwrap();
    let finding = only(&findings, FindingClass::HalfDeletedBucket);
    assert_eq!(finding.severity, Severity::Warn);
    assert!(finding.evidence.contains("halfdeleted"), "{finding:?}");
    assert!(
        finding.evidence.contains("1 object record"),
        "the stranded objects are sized: {finding:?}"
    );
}
