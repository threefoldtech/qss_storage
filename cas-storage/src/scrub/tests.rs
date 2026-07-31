//! Walker tests, driven against real stores: normal PUTs for the live
//! shapes, the ADR 0006 crash fixtures for the damaged ones.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use tempfile::{TempDir, tempdir};

use crate::cas::crash_fixtures::{plant_degraded_record, plant_orphan_file};
use crate::cas::{AsyncByteStream, CasFS, SharedBlockStore, StorageEngine};
use crate::metastore::{
    BlockId, ContentHash, DEFAULT_BLOCK_TREE, Durability, MAX_BLOCKID_SIZE, ObjectData,
    block_disk_path,
};
use crate::metrics::SharedMetrics;

use super::findings::{FindingClass, HolderRef, Severity};
use super::holders::{HolderEnumerationError, expected_counts, holders_of};
use super::records::walk_records;
use super::{ScrubContext, disk::walk_disk};

/// One store: a shared block store and a single namespace over it.
pub(super) fn store(dir: &TempDir) -> (Arc<SharedBlockStore>, CasFS) {
    let path = dir.path();
    let shared = Arc::new(
        SharedBlockStore::new(
            path.join("meta/blocks"),
            path.join("blocks"),
            StorageEngine::Fjall,
            Some(1),
            Some(Durability::Buffer),
            None,
            None,
        )
        .unwrap(),
    );
    let fs = CasFS::new(
        path.join("meta/ns"),
        shared.clone(),
        SharedMetrics::default(),
        StorageEngine::Fjall,
        Some(1),
        Some(Durability::Buffer),
        false,
    )
    .unwrap();
    (shared, fs)
}

/// Stores `data` under `bucket`/`key` and returns its (single) block id.
pub(super) async fn put(fs: &CasFS, bucket: &str, key: &str, data: Vec<u8>) -> BlockId {
    let id = fs.hasher().hash(&data);
    let len = data.len();
    let stream = AsyncByteStream::new(futures::stream::once(async move {
        Ok(bytes::Bytes::from(data))
    }));
    fs.store_single_object_and_meta(bucket, key, stream, len)
        .await
        .unwrap();
    id
}

/// Writes an object record naming exactly `blocks`, without writing any of
/// them: the counting rule is about the record, not the bytes.
pub(super) fn plant_object(fs: &CasFS, bucket: &str, key: &str, blocks: Vec<BlockId>) {
    fs.create_object_meta(
        bucket,
        key,
        1024,
        ContentHash::from([7u8; 16]),
        ObjectData::SinglePart { blocks },
    )
    .unwrap();
}

pub(super) fn synthetic_id(seed: u8) -> BlockId {
    BlockId::from([seed; MAX_BLOCKID_SIZE])
}

/// A store nothing has been written to has nothing to say -- and says it
/// without erroring on the `.tmp` directory store-open leaves behind.
#[test]
fn an_empty_store_walks_clean() {
    let dir = tempdir().unwrap();
    let (shared, fs) = store(&dir);
    let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);

    assert!(expected_counts(&ctx).unwrap().is_empty());

    let records = walk_records(&ctx).unwrap();
    assert!(records.records.is_empty());
    assert!(records.findings.is_empty());

    let disk = walk_disk(&ctx).unwrap();
    assert!(disk.files.is_empty());
    assert!(
        disk.foreign.is_empty(),
        "the store's own .tmp is not foreign: {:?}",
        disk.foreign
    );
}

/// The counting rule of docs/refcount.md: per occurrence. A block in two
/// objects counts twice, and a block listed twice in ONE object also counts
/// twice -- that is what the write path's every-hit-bumps produces.
#[tokio::test]
async fn expected_counts_count_every_occurrence() {
    let dir = tempdir().unwrap();
    let (shared, fs) = store(&dir);
    fs.create_bucket("photos").unwrap();
    fs.create_bucket("videos").unwrap();

    let shared_data = b"deduplicated across two objects".repeat(8).to_vec();
    let id = put(&fs, "photos", "a", shared_data.clone()).await;
    // Same content in another bucket: one block, two references.
    put(&fs, "videos", "b", shared_data).await;

    // A record that names the same block twice, plus a distinct one.
    let twice = synthetic_id(0x41);
    let once = synthetic_id(0x42);
    plant_object(&fs, "photos", "doubled", vec![twice, twice, once]);

    // An inline object holds bytes, not references: it must count nothing.
    fs.store_inlined_object("photos", "small", b"inline".to_vec())
        .unwrap();

    let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
    let counts = expected_counts(&ctx).unwrap();

    assert_eq!(counts.get(&id), Some(&2), "one block, two objects");
    assert_eq!(counts.get(&twice), Some(&2), "twice in one object is two");
    assert_eq!(counts.get(&once), Some(&1));
    assert_eq!(
        counts.len(),
        3,
        "the inline object adds nothing: {counts:?}"
    );
}

/// Part records are holders too, and unconditionally: until ADR 0003 lands
/// they are the only thing referencing an in-flight upload's blocks.
#[test]
fn multipart_part_records_are_counted_as_holders() {
    let dir = tempdir().unwrap();
    let (shared, fs) = store(&dir);
    fs.create_bucket("b").unwrap();

    let first = synthetic_id(0x51);
    let second = synthetic_id(0x52);
    fs.insert_multipart_part(
        "b".to_string(),
        "big".to_string(),
        2048,
        1,
        "upload-1".to_string(),
        ContentHash::from([1u8; 16]),
        vec![first, second],
    )
    .unwrap();
    fs.insert_multipart_part(
        "b".to_string(),
        "big".to_string(),
        2048,
        2,
        "upload-1".to_string(),
        ContentHash::from([2u8; 16]),
        vec![second],
    )
    .unwrap();

    let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
    let counts = expected_counts(&ctx).unwrap();

    assert_eq!(counts.get(&first), Some(&1));
    assert_eq!(counts.get(&second), Some(&2), "referenced by both parts");
}

/// The closed-holder-set refusal: one object record that will not decode
/// takes the whole enumeration down, naming the tree and key so an operator
/// can go look. A count over a partial holder set would authorise freeing
/// live blocks.
#[tokio::test]
async fn an_undecodable_object_record_refuses_the_enumeration() {
    let dir = tempdir().unwrap();
    let (shared, fs) = store(&dir);
    fs.create_bucket("photos").unwrap();
    put(&fs, "photos", "good", b"a real object".repeat(4).to_vec()).await;

    // Raw garbage where an object record belongs.
    fs.get_bucket("photos")
        .unwrap()
        .insert(b"broken", vec![0xffu8; 8])
        .unwrap();

    let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
    match expected_counts(&ctx).unwrap_err() {
        HolderEnumerationError::UndecodableRecord { tree, key, .. } => {
            assert_eq!(tree, "photos");
            assert_eq!(key, "broken");
        }
        other => panic!("expected an undecodable-record refusal, got {other:?}"),
    }

    // The blast-radius walk refuses on the same ground -- it is the same
    // enumeration, so it cannot be used to sidestep the refusal.
    let wanted: HashSet<BlockId> = [synthetic_id(1)].into_iter().collect();
    assert!(holders_of(&ctx, &wanted).is_err());

    // The message names both, because that is what the operator needs.
    let msg = expected_counts(&ctx).unwrap_err().to_string();
    assert!(msg.contains("photos"), "{msg}");
    assert!(msg.contains("broken"), "{msg}");
}

/// The blast radius: every holder of a damaged block, objects and parts
/// alike. A holder that references the block twice is still one holder.
#[tokio::test]
async fn holders_of_names_every_holder_once() {
    let dir = tempdir().unwrap();
    let (shared, fs) = store(&dir);
    fs.create_bucket("photos").unwrap();

    let data = b"shared bytes".repeat(16).to_vec();
    let id = put(&fs, "photos", "live", data).await;
    plant_object(&fs, "photos", "doubled", vec![id, id]);
    fs.insert_multipart_part(
        "photos".to_string(),
        "upload-target".to_string(),
        512,
        3,
        "u-9".to_string(),
        ContentHash::from([3u8; 16]),
        vec![id],
    )
    .unwrap();

    // A block nothing references at all.
    let lonely = synthetic_id(0x77);

    let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
    let wanted: HashSet<BlockId> = [id, lonely].into_iter().collect();
    let holders = holders_of(&ctx, &wanted).unwrap();

    let mut for_id = holders.get(&id).expect("the block has holders").clone();
    assert_eq!(for_id.len(), 3, "two objects and one part: {for_id:?}");
    for_id.sort_by_key(|h| format!("{h:?}"));

    assert!(for_id.contains(&HolderRef::Object {
        bucket: "photos".to_string(),
        key: "live".to_string(),
    }));
    assert!(
        for_id.contains(&HolderRef::Object {
            bucket: "photos".to_string(),
            key: "doubled".to_string(),
        }),
        "a holder that references the block twice appears once"
    );
    assert!(for_id.contains(&HolderRef::Part {
        bucket: "photos".to_string(),
        key: "upload-target".to_string(),
        upload_id: "u-9".to_string(),
        part_number: 3,
    }));

    assert!(
        !holders.contains_key(&lonely),
        "a block with no holders is absent, not empty"
    );
    // Asking for nothing walks nothing.
    assert!(holders_of(&ctx, &HashSet::new()).unwrap().is_empty());
}

/// Record damage is a finding, not a refusal: the block is already unusable
/// and its holders are still counted by the holder walk. The finding must
/// name the record, which is why the walker reads the tree raw.
#[tokio::test]
async fn the_record_walk_reports_damage_and_keeps_going() {
    let dir = tempdir().unwrap();
    let (shared, fs) = store(&dir);
    fs.create_bucket("b").unwrap();
    let good = put(&fs, "b", "k", b"a real block".repeat(20).to_vec()).await;

    // A record that does not decode, under a well-formed address.
    let broken = synthetic_id(0x61);
    shared
        .meta_store()
        .get_tree(DEFAULT_BLOCK_TREE)
        .unwrap()
        .insert(broken.as_slice(), vec![0x01u8; 3])
        .unwrap();
    // A key that is not an address at all.
    shared
        .meta_store()
        .get_tree(DEFAULT_BLOCK_TREE)
        .unwrap()
        .insert(b"not-an-address", vec![0u8; 18])
        .unwrap();

    let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
    let walk = walk_records(&ctx).unwrap();

    assert_eq!(walk.records.len(), 1, "the good record still decodes");
    assert!(walk.records.contains_key(&good));

    assert_eq!(walk.findings.len(), 2);
    for finding in &walk.findings {
        assert_eq!(finding.severity, Severity::Critical);
        assert_eq!(finding.class, FindingClass::UndecodableBlockRecord);
    }
    assert!(
        walk.findings
            .iter()
            .any(|f| f.block.as_deref() == Some(broken.to_hex().as_str())),
        "the damaged record must be named: {:?}",
        walk.findings
    );
}

/// The disk walker's acceptance rule, stated as a round trip: for every file
/// it yields, the pure path builder rebuilds the path it was found at.
#[test]
fn disk_walk_depth_round_trips_with_block_disk_path() {
    let dir = tempdir().unwrap();
    let (shared, fs) = store(&dir);
    let root: &Path = fs.fs_root();

    for depth in 1u8..=4 {
        let id = synthetic_id(0x10 + depth);
        plant_orphan_file(root, &id, depth, b"orphan bytes");
    }

    let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
    let walk = walk_disk(&ctx).unwrap();

    assert_eq!(walk.files.len(), 4);
    assert!(walk.foreign.is_empty(), "{:?}", walk.foreign);
    for file in &walk.files {
        assert_eq!(
            file.path,
            block_disk_path(&file.id, file.depth, root.to_path_buf()),
            "the walker's (id, depth) must rebuild the path it came from"
        );
        assert_eq!(file.size, b"orphan bytes".len() as u64);
    }

    let mut depths: Vec<u8> = walk.files.iter().map(|f| f.depth).collect();
    depths.sort_unstable();
    assert_eq!(depths, vec![1, 2, 3, 4]);
}

/// One id at two depths is two files, both reported: which of them (if
/// either) the record names is a pass's judgment, not the walker's.
#[test]
fn disk_walk_yields_both_copies_of_a_block_at_two_depths() {
    let dir = tempdir().unwrap();
    let (shared, fs) = store(&dir);
    let id = synthetic_id(0x33);

    plant_orphan_file(fs.fs_root(), &id, 1, b"shallow");
    plant_orphan_file(fs.fs_root(), &id, 3, b"deeper copy");

    let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
    let walk = walk_disk(&ctx).unwrap();

    assert_eq!(walk.files.len(), 2);
    assert!(walk.files.iter().all(|f| f.id == id));
    let mut depths: Vec<u8> = walk.files.iter().map(|f| f.depth).collect();
    depths.sort_unstable();
    assert_eq!(depths, vec![1, 3]);
}

/// Everything the layout does not produce is foreign -- reported, never
/// mistaken for a block, and (per the ADR) quarantined rather than deleted.
#[test]
fn disk_walk_refuses_everything_that_is_not_this_layout() {
    let dir = tempdir().unwrap();
    let (shared, fs) = store(&dir);
    let root = fs.fs_root().clone();
    // An address whose hex has letters in it, so the uppercase case below
    // really is a different name.
    let id = synthetic_id(0xab);
    let hex = id.to_hex();

    // A well-formed block, to prove the rejections are selective.
    plant_orphan_file(&root, &id, 2, b"the real one");

    // A valid name directly under the root: no record can ever name it,
    // because a derived path is at least one fanout level deep.
    std::fs::write(root.join(&hex), b"unreachable").unwrap();
    // A directory that is not a fanout level.
    std::fs::create_dir_all(root.join("zz")).unwrap();
    std::fs::create_dir_all(root.join("abc")).unwrap();
    // A valid name under someone else's chain.
    let wrong_chain = root.join("cd");
    std::fs::create_dir_all(&wrong_chain).unwrap();
    std::fs::write(wrong_chain.join(&hex), b"misfiled").unwrap();
    // Uppercase hex is not what the writer produces.
    std::fs::write(root.join("ab").join(hex.to_uppercase()), b"shouty").unwrap();
    // A name that is not hex at all.
    std::fs::write(root.join("ab").join("README"), b"notes").unwrap();

    let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
    let walk = walk_disk(&ctx).unwrap();

    assert_eq!(walk.files.len(), 1, "only the real block: {:?}", walk.files);
    assert_eq!(walk.files[0].id, id);
    assert_eq!(walk.files[0].depth, 2);

    let foreign: Vec<String> = walk
        .foreign
        .iter()
        .map(|f| f.path.strip_prefix(&root).unwrap().display().to_string())
        .collect();
    assert_eq!(foreign.len(), 6, "{foreign:?}");
    for expected in [
        hex.clone(),
        "zz".to_string(),
        "abc".to_string(),
        format!("cd/{hex}"),
        format!("ab/{}", hex.to_uppercase()),
        "ab/README".to_string(),
    ] {
        assert!(
            foreign.contains(&expected),
            "{expected} missing: {foreign:?}"
        );
    }

    // Every foreign entry renders as a WARN finding carrying its path.
    for entry in &walk.foreign {
        let finding = entry.to_finding();
        assert_eq!(finding.severity, Severity::Warn);
        assert_eq!(finding.class, FindingClass::ForeignFile);
        assert!(finding.path.is_some());
    }
}

/// `.tmp` and `.quarantine` are the store's own, at the top level only. The
/// same names one level down are not fanout directories and are reported.
#[test]
fn tmp_and_quarantine_are_skipped_only_at_the_root() {
    let dir = tempdir().unwrap();
    let (shared, fs) = store(&dir);
    let root = fs.fs_root().clone();

    // Store-open already created .tmp; give both something to hide.
    std::fs::create_dir_all(root.join(".tmp")).unwrap();
    std::fs::write(root.join(".tmp").join("half-written"), b"torn").unwrap();
    std::fs::create_dir_all(root.join(".quarantine")).unwrap();
    std::fs::write(root.join(".quarantine").join("corrupt-block"), b"bad").unwrap();

    // The same names nested under a fanout directory are just junk.
    std::fs::create_dir_all(root.join("ab").join(".tmp")).unwrap();
    std::fs::create_dir_all(root.join("ab").join(".quarantine")).unwrap();

    let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
    let walk = walk_disk(&ctx).unwrap();

    assert!(walk.files.is_empty());
    let foreign: Vec<String> = walk
        .foreign
        .iter()
        .map(|f| f.path.strip_prefix(&root).unwrap().display().to_string())
        .collect();
    assert_eq!(foreign.len(), 2, "{foreign:?}");
    assert!(foreign.contains(&"ab/.tmp".to_string()), "{foreign:?}");
    assert!(
        foreign.contains(&"ab/.quarantine".to_string()),
        "{foreign:?}"
    );
}

/// The two fileless residue classes seen through the walkers: both are
/// records the record walk decodes and the disk walk knows nothing about.
/// Telling them apart is a pass's job; producing the raw material is this
/// component's.
#[test]
fn fileless_records_show_up_in_the_record_walk_only() {
    let dir = tempdir().unwrap();
    let (shared, fs) = store(&dir);

    let dangling = synthetic_id(0x81);
    let degraded = synthetic_id(0x82);
    crate::cas::crash_fixtures::plant_dangling_record(&shared, dangling, 1);
    plant_degraded_record(&shared, degraded, 2, 4, 4096);

    let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
    let walk = walk_records(&ctx).unwrap();
    assert!(walk.findings.is_empty(), "both records decode fine");
    assert_eq!(walk.records.len(), 2);
    assert!(!walk.records[&dangling].is_degraded());
    let degraded_record = &walk.records[&degraded];
    assert!(degraded_record.is_degraded());
    assert_eq!(degraded_record.rc(), 4, "its holders stay counted");

    assert!(
        walk_disk(&ctx).unwrap().files.is_empty(),
        "neither residue class has a file"
    );
}
