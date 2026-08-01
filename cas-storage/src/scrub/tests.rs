//! Walker tests, driven against real stores: normal PUTs for the live
//! shapes, the ADR 0006 crash fixtures for the damaged ones.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use tempfile::{TempDir, tempdir};

use crate::cas::crash_fixtures::{plant_degraded_record, plant_orphan_file};
use crate::cas::{
    AsyncByteStream, BLOCKS_DB_DIR_NAME, CasFS, STORE_ID_MARKER_NAME, SharedBlockStore,
    StorageEngine,
};
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
///
/// Every caller plants on a free key, so the write displaces nothing and
/// releases nothing (ADR 0008) -- planting over a live record would drop
/// that record's references, which is not what a fixture is for.
pub(super) async fn plant_object(fs: &CasFS, bucket: &str, key: &str, blocks: Vec<BlockId>) {
    fs.create_object_meta(
        bucket,
        key,
        1024,
        ContentHash::from([7u8; 16]),
        ObjectData::SinglePart { blocks },
    )
    .await
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
    plant_object(&fs, "photos", "doubled", vec![twice, twice, once]).await;

    // An inline object holds bytes, not references: it must count nothing.
    fs.store_inlined_object("photos", "small", b"inline".to_vec())
        .await
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
    plant_object(&fs, "photos", "doubled", vec![id, id]).await;
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

/// The store's own entries -- `.tmp`, `.quarantine`, `.db` and the ADR 0012
/// `.store-id` marker -- are skipped at the top level, and nowhere else. The
/// same names one level down are not fanout directories and are reported.
///
/// The exact skip table is the assertion: everything else in the fanout is
/// block-shaped or foreign, so a fifth reserved name added without a thought
/// would fail here.
#[test]
fn the_stores_own_entries_are_skipped_only_at_the_root() {
    let dir = tempdir().unwrap();
    let (shared, fs) = store(&dir);
    let root = fs.fs_root().clone();

    // Store-open already created .tmp and .store-id; give each of the four
    // something to hide, and check the marker really is there to be skipped.
    std::fs::create_dir_all(root.join(".tmp")).unwrap();
    std::fs::write(root.join(".tmp").join("half-written"), b"torn").unwrap();
    std::fs::create_dir_all(root.join(".quarantine")).unwrap();
    std::fs::write(root.join(".quarantine").join("corrupt-block"), b"bad").unwrap();
    assert!(
        root.join(STORE_ID_MARKER_NAME).is_file(),
        "the store wrote its pairing marker at open"
    );
    assert!(
        root.join(BLOCKS_DB_DIR_NAME).exists() || dir.path().join("meta/blocks").is_dir(),
        "the database is somewhere: under blocks/ on one root, under the meta root on two"
    );

    // The same names nested under a fanout directory are just junk.
    std::fs::create_dir_all(root.join("ab").join(".tmp")).unwrap();
    std::fs::create_dir_all(root.join("ab").join(".quarantine")).unwrap();
    std::fs::create_dir_all(root.join("ab").join(BLOCKS_DB_DIR_NAME)).unwrap();
    std::fs::write(root.join("ab").join(STORE_ID_MARKER_NAME), b"not mine").unwrap();

    let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared);
    let walk = walk_disk(&ctx).unwrap();

    assert!(walk.files.is_empty());
    let mut foreign: Vec<String> = walk
        .foreign
        .iter()
        .map(|f| f.path.strip_prefix(&root).unwrap().display().to_string())
        .collect();
    foreign.sort();
    assert_eq!(
        foreign,
        vec![
            "ab/.db".to_string(),
            "ab/.quarantine".to_string(),
            "ab/.store-id".to_string(),
            "ab/.tmp".to_string(),
        ],
        "exactly the four names, and only below the root"
    );
}

/// The layout the tools ship with -- one root for both halves of the store
/// (`--meta-root .` and `--fs-root .`) -- puts the shared block database and
/// its header sidecar INSIDE the blocks root, where the disk walk runs. They
/// are the store's own files: reporting them foreign would be wrong, and a
/// repair acting on that finding would rename the live database into
/// quarantine.
#[test]
fn the_stores_own_database_inside_the_blocks_root_is_not_foreign() {
    let dir = tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let fs = CasFS::single_namespace(
        root.clone(),
        root.clone(),
        SharedMetrics::default(),
        StorageEngine::Fjall,
        Some(1),
        Some(Durability::Buffer),
        None,
        false,
        None,
        None,
        None,
    )
    .unwrap();
    let blocks_db = root.join("blocks").join(BLOCKS_DB_DIR_NAME);
    assert!(blocks_db.is_dir(), "the premise: the DB is under blocks/");

    let ctx = ScrubContext::new(fs.namespace_meta_store(), fs.shared_block_store())
        .with_meta_root(root.clone());
    let walk = walk_disk(&ctx).unwrap();
    assert!(walk.files.is_empty());
    assert!(
        walk.foreign.is_empty(),
        "the store's own metadata is not residue: {:?}",
        walk.foreign
    );

    // The rule is exactly the opener's knowledge: a context that was not told
    // which meta root it opened cannot know, and says so by reporting them.
    let blind = ScrubContext::new(fs.namespace_meta_store(), fs.shared_block_store());
    assert!(
        !walk_disk(&blind).unwrap().foreign.is_empty(),
        "without a meta root there is nothing to compare against"
    );
}

/// A block whose hash begins with 0xdb fans out to `blocks/db` -- the name
/// the shared database itself used to squat, which forced the walk to skip
/// that whole subtree: one block in 256 invisible to every pass. The
/// database lives at `blocks/.db` now, and `db` is a fanout directory like
/// any other.
#[test]
fn the_db_fanout_directory_is_walked_like_any_other() {
    let dir = tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let fs = CasFS::single_namespace(
        root.clone(),
        root.clone(),
        SharedMetrics::default(),
        StorageEngine::Fjall,
        Some(1),
        Some(Durability::Buffer),
        None,
        false,
        None,
        None,
        None,
    )
    .unwrap();

    let id = synthetic_id(0xdb);
    plant_orphan_file(fs.fs_root(), &id, 1, b"lives in the db fanout dir");

    let ctx = ScrubContext::new(fs.namespace_meta_store(), fs.shared_block_store())
        .with_meta_root(root.clone());
    let walk = walk_disk(&ctx).unwrap();
    assert!(
        walk.files.iter().any(|f| f.id == id),
        "the 0xdb block must be visible to the walk: {:?}",
        walk.files
    );
    assert!(
        walk.foreign.is_empty(),
        "nothing planted here is foreign: {:?}",
        walk.foreign
    );
}

/// A store from before the `.db` rename still has its database at
/// `blocks/db`. Opening it must refuse with the migration spelled out --
/// not mint a fresh empty database at `.db` and silently shadow every
/// record of the old store.
#[test]
fn a_legacy_blocks_db_is_refused_not_shadowed() {
    let dir = tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let legacy = root.join("blocks").join("db");
    std::fs::create_dir_all(&legacy).unwrap();
    // fjall's own file, never a block name: what marks the dir a database.
    std::fs::write(legacy.join("version"), b"2").unwrap();

    let Err(err) = CasFS::single_namespace(
        root.clone(),
        root.clone(),
        SharedMetrics::default(),
        StorageEngine::Fjall,
        Some(1),
        Some(Durability::Buffer),
        None,
        false,
        None,
        None,
        None,
    ) else {
        panic!("a legacy store must be refused");
    };
    let msg = format!("{err}");
    assert!(
        msg.contains("legacy blocks database"),
        "the refusal must say what this is: {msg}"
    );
    assert!(
        !root.join("blocks").join(BLOCKS_DB_DIR_NAME).exists(),
        "the refusal must not have created a shadowing database"
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

/// A batch killed between its directory sync and its commit (ADR 0010).
///
/// The window the batch widened: every file of the batch is durable at its
/// final path, and not one of them has a record, because the records were
/// going to commit together and never did. The claim under test is that this
/// is the SAME residue as before, only more of it -- so fsck must report each
/// file as `orphan_file` (residue class 1) and nothing else, with no new
/// class and no critical finding, and a later PUT of the same content must
/// adopt each file in place rather than write a second copy somewhere else.
///
/// A batch cap's worth of blocks, because "up to `max_blocks_per_commit`
/// orphans" is exactly what the ADR promises a single kill can leave.
#[tokio::test]
async fn a_batch_killed_before_its_commit_is_a_batch_of_ordinary_orphans() {
    /// The default cap: the widest residue a single kill may leave.
    const BATCH: usize = crate::config::DEFAULT_MAX_BLOCKS_PER_COMMIT;

    let dir = tempdir().unwrap();
    let (shared, fs) = store(&dir);
    fs.create_bucket("b").unwrap();

    // The batch as it was on disk when the process died: files renamed into
    // place at the depths the placement policy would pick, records absent.
    let batch: Vec<(BlockId, u8, Vec<u8>)> = (0..BATCH)
        .map(|i| {
            let bytes = format!("killed batch block {i} ").repeat(16).into_bytes();
            let id = shared.hasher().hash(&bytes);
            // Depth 1 for the first half and 2 for the rest: a real batch
            // fans out, and the heal has to follow each file to where it is.
            let depth = if i % 2 == 0 { 1 } else { 2 };
            (id, depth, bytes)
        })
        .collect();
    crate::cas::crash_fixtures::plant_killed_batch(fs.fs_root(), &batch);

    let ctx = ScrubContext::new(fs.namespace_meta_store(), &shared).with_meta_root(dir.path());
    let report = super::engine::run(&ctx, &super::engine::ScrubOptions::full()).unwrap();

    // Class 1, once per file, and nothing else at all.
    let orphans: Vec<_> = report
        .findings
        .iter()
        .filter(|f| f.class == FindingClass::OrphanFile)
        .collect();
    assert_eq!(
        orphans.len(),
        BATCH,
        "every file of the killed batch is one orphan: {}",
        report.render_text()
    );
    assert_eq!(
        report.findings.len(),
        BATCH,
        "a killed batch produces no finding of any other class: {}",
        report.render_text()
    );
    assert!(
        report
            .findings
            .iter()
            .all(|f| f.severity != Severity::Critical),
        "orphan files are leakage, never loss: {}",
        report.render_text()
    );

    // Every planted file is named by exactly one finding.
    let reported: HashSet<String> = orphans.iter().filter_map(|f| f.block.clone()).collect();
    for (id, _, _) in &batch {
        assert!(
            reported.contains(&id.to_hex()),
            "fsck must name the orphan {}",
            id.to_hex()
        );
    }

    // The heal: re-PUT the same content. Each block adopts the file already
    // sitting on its directory chain -- same depth, same inode path -- rather
    // than placing a second copy the scrub would then call an off-depth
    // duplicate.
    for (i, (id, depth, bytes)) in batch.iter().enumerate() {
        put(&fs, "b", &format!("retry-{i}"), bytes.clone()).await;

        let record = shared
            .block_tree()
            .get_block(id.as_slice())
            .unwrap()
            .expect("the retry records the block");
        assert_eq!(
            record.depth(),
            *depth,
            "the retry must adopt the orphan where it lies, not move it"
        );
        assert_eq!(record.rc(), 1, "one holder: the object that just landed");
        assert_eq!(
            shared
                .hasher()
                .hash(&std::fs::read(block_disk_path(id, *depth, fs.fs_root().clone())).unwrap()),
            *id,
            "the adopted file is the block it is named after"
        );
    }

    // And the store is clean again: the heal left nothing for fsck to say.
    let healed = super::engine::run(&ctx, &super::engine::ScrubOptions::full()).unwrap();
    assert!(
        healed.findings.is_empty(),
        "a re-PUT of every block heals the whole batch: {}",
        healed.render_text()
    );
}
