//! The stale-upload garbage collector (ADR 0003 decision 8).
//!
//! One sweep, two phases, both value-driven (hard rule 4):
//!
//! 1. **Aged uploads.** Every `_UPLOADS` record older than the TTL is aborted
//!    through [`CasFS::abort_upload`] -- the same claim, the same per-part
//!    reap, the same code the client's `AbortMultipartUpload` runs. The GC is
//!    not a second abort implementation; it is another caller of the claim.
//! 2. **Orphan parts.** Every part record whose upload record no longer
//!    exists holds block references nothing will ever complete or abort: the
//!    residue of a crash mid-abort, of the accepted upload_part-versus-abort
//!    race, and of every legacy dash-keyed record written before ADR 0003
//!    re-keyed the tree. Each is removed and its blocks released.
//!
//! Phase 2 is also the migration: a dash-keyed record is unreachable by any
//! prefix scan and can have no upload record, so the first sweep reaps it.
//! That is why the reap takes the ITERATED key rather than rebuilding one
//! from the record -- a rebuilt key would remove nothing and then release
//! blocks the surviving record still claims, which is the forbidden order
//! (hard rule 2).
//!
//! # A sweep never aborts
//!
//! Every failure is logged, counted in [`SweepStats::errors`], and stepped
//! over. A store error on one record must not strand every later one, and
//! there is no caller-level recovery to return an error to: the next sweep
//! sees whatever this one could not do.
//!
//! # The window this cannot close
//!
//! `complete_multipart_upload` claims the upload record, creates the object
//! from its parts' blocks -- the object INHERITS those references -- and only
//! then removes the part records. A sweep landing between the claim and the
//! removals sees part records with no upload record and cannot tell them from
//! a crashed abort's residue. Reaping one there would drop references the new
//! object holds: loss, not leakage. The window is the few milliseconds of one
//! complete against a sweep that runs hourly at most, and closing it needs a
//! part-level claim in `complete` (ADR 0003's territory, not this task's), so
//! it is recorded here rather than papered over.

use std::time::Duration;

use chrono::Utc;

use super::fs::CasFS;
use super::multipart::MultiPart;
use super::uploads::reap_part;
use crate::metastore::MULTIPART_PARTS_TREE;

/// What one sweep did.
///
/// Counts, not outcomes: the caller (the s3cas daemon task) feeds them to its
/// prometheus counters. `errors` is the number of records the sweep stepped
/// over -- each already logged where it happened, with the context this
/// struct deliberately does not carry.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SweepStats {
    /// Aged upload records claimed and aborted by this sweep.
    pub uploads_reaped: u64,
    /// Part records with no upload record, removed with their references.
    pub orphan_parts_reaped: u64,
    /// Records the sweep could not read, decode or reap.
    pub errors: u64,
}

/// Reaps everything ADR 0003 leaves for the GC: uploads older than `ttl`, and
/// part records no upload record owns.
///
/// # `ttl` is taken literally
///
/// An upload is stale when it is strictly older than `ttl`, so `ttl` of zero
/// makes every upload in the store stale, including one started a second ago.
/// "0 disables the GC" is the DAEMON's rule -- s3cas never spawns the task for
/// a zero TTL -- not this function's, which has no way to distinguish
/// "disabled" from "reap everything" and does not guess.
pub async fn sweep_stale_uploads(fs: &CasFS, ttl: Duration) -> SweepStats {
    let mut stats = SweepStats::default();

    // Wall clock, matching the wall clock `UploadRecord::new` stamped. Skew
    // and non-monotonicity are accepted at TTLs of days (ADR 0003).
    let ttl_secs = i64::try_from(ttl.as_secs()).unwrap_or(i64::MAX);
    let cutoff = Utc::now().timestamp().saturating_sub(ttl_secs);

    sweep_aged_uploads(fs, cutoff, &mut stats).await;
    sweep_orphan_parts(fs, &mut stats).await;

    tracing::debug!(
        uploads_reaped = stats.uploads_reaped,
        orphan_parts_reaped = stats.orphan_parts_reaped,
        errors = stats.errors,
        "stale-upload sweep finished"
    );
    stats
}

/// Phase 1: abort every upload record created at or before `cutoff`.
///
/// The listing is taken first and the aborts run against it, so no store
/// iterator is open while the tree is being written -- and a record created
/// after the listing is by construction younger than the cutoff anyway.
async fn sweep_aged_uploads(fs: &CasFS, cutoff: i64, stats: &mut SweepStats) {
    let records = match fs.list_uploads() {
        Ok(records) => records,
        Err(e) => {
            // The age phase is lost for this sweep; the orphan phase still
            // runs, and the next sweep retries.
            tracing::error!(error = %e, "Could not list uploads; skipping the age phase");
            stats.errors += 1;
            return;
        }
    };

    for record in records {
        if record.created_at() >= cutoff {
            continue;
        }

        match fs
            .abort_upload(record.bucket(), record.key(), record.upload_id())
            .await
        {
            Ok(Some(parts)) => {
                stats.uploads_reaped += 1;
                tracing::info!(
                    bucket = %record.bucket(),
                    key = %record.key(),
                    upload_id = %record.upload_id(),
                    created_at = record.created_at(),
                    parts = parts,
                    "Reaped a stale multipart upload"
                );
            }
            // Not an error: a client's own abort, or a complete, won the
            // claim between the listing and here. The upload is over either
            // way, which is all the GC wanted.
            Ok(None) => tracing::debug!(
                bucket = %record.bucket(),
                key = %record.key(),
                upload_id = %record.upload_id(),
                "Stale upload was already claimed by a client; nothing to reap"
            ),
            Err(e) => {
                stats.errors += 1;
                tracing::error!(
                    bucket = %record.bucket(),
                    key = %record.key(),
                    upload_id = %record.upload_id(),
                    error = %e,
                    "Could not abort a stale upload; leaving it for the next sweep"
                );
            }
        }
    }
}

/// Phase 2: remove every part record whose upload record does not exist, and
/// release the blocks it held.
///
/// The parts tree is walked through the ext handle rather than
/// [`MultiPartTree::parts_of`](super::multipart::MultiPartTree::parts_of):
/// this is the one caller that needs the raw KEYS as well as the values,
/// because a legacy dash-keyed record cannot be addressed any other way.
///
/// Candidates are collected before the first removal, so the store iterator
/// is closed before anything writes to the tree it came from and no `.await`
/// happens while it is open. Only orphans are collected, not the whole tree,
/// so the memory this costs is the residue, not the working set.
async fn sweep_orphan_parts(fs: &CasFS, stats: &mut SweepStats) {
    let tree = match fs.shared.meta_store().get_tree_ext(MULTIPART_PARTS_TREE) {
        Ok(tree) => tree,
        Err(e) => {
            tracing::error!(error = %e, "Could not open the parts tree; skipping the orphan phase");
            stats.errors += 1;
            return;
        }
    };

    let mut orphans: Vec<(Vec<u8>, MultiPart)> = Vec::new();
    for item in tree.iter_all() {
        let (storage_key, raw) = match item {
            Ok(pair) => pair,
            Err(e) => {
                tracing::error!(error = %e, "Could not read a part record; skipping it");
                stats.errors += 1;
                continue;
            }
        };

        let part = match MultiPart::try_from(&*raw) {
            Ok(part) => part,
            // An undecodable record names no blocks, so there is nothing safe
            // to release: removing it would strand its references forever.
            // fsck's business, not the GC's.
            Err(e) => {
                tracing::error!(
                    key = %String::from_utf8_lossy(&storage_key),
                    error = %e,
                    "Could not decode a part record; leaving it for fsck"
                );
                stats.errors += 1;
                continue;
            }
        };

        match fs.get_upload(part.bucket(), part.key(), part.upload_id()) {
            // A live upload owns this part: complete or abort will deal with
            // it, and the GC must not touch it.
            Ok(Some(_)) => {}
            Ok(None) => orphans.push((storage_key, part)),
            // "Cannot tell" is not "no upload record". Skip it: a sweep that
            // reaps on a failed read releases blocks a live upload holds.
            Err(e) => {
                stats.errors += 1;
                tracing::error!(
                    bucket = %part.bucket(),
                    key = %part.key(),
                    upload_id = %part.upload_id(),
                    error = %e,
                    "Could not check the upload record of a part; skipping it"
                );
            }
        }
    }

    for (storage_key, part) in orphans {
        if reap_part(fs, &storage_key, &part).await {
            stats.orphan_parts_reaped += 1;
            tracing::info!(
                bucket = %part.bucket(),
                key = %part.key(),
                upload_id = %part.upload_id(),
                part_number = part.part_number(),
                "Reaped an orphan part record"
            );
        } else {
            // reap_part already logged why; its blocks stay referenced.
            stats.errors += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::uploads::upload_key;
    use crate::cas::{AsyncByteStream, StorageEngine};
    use crate::metastore::{BlockId, ContentHash, Durability, UploadRecord};
    use crate::metrics::SharedMetrics;
    use tempfile::{TempDir, tempdir};

    const BUCKET: &str = "test-bucket";
    const KEY: &str = "test/key";
    const TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);

    fn test_fs() -> (CasFS, TempDir) {
        let dir = tempdir().unwrap();
        let fs = CasFS::single_namespace(
            dir.path().to_path_buf(),
            dir.path().join("meta"),
            SharedMetrics::default(),
            StorageEngine::Fjall,
            Some(1),
            Some(Durability::Buffer),
            None,
            false,
        )
        .unwrap();
        fs.create_bucket(BUCKET).unwrap();
        (fs, dir)
    }

    /// Plants an upload record with a chosen age, the way a store that has
    /// been running for a week would hold it. Sleeping through a TTL is not a
    /// test, which is what the backdating constructor exists for.
    fn plant_aged_upload(fs: &CasFS, key: &str, upload_id: &str, age: Duration) {
        let created_at = Utc::now().timestamp() - age.as_secs() as i64;
        let record = UploadRecord::with_created_at(
            created_at,
            BUCKET.to_string(),
            key.to_string(),
            upload_id.to_string(),
        );
        fs.shared
            .uploads_tree()
            .insert(&upload_key(BUCKET, key, upload_id), record.to_vec())
            .unwrap();
    }

    /// Payload big enough to be a real block, unique per `tag` so no two
    /// parts of a test dedup onto each other.
    fn part_data(tag: u8) -> Vec<u8> {
        std::iter::repeat_n(tag, 4096).collect()
    }

    /// Stores one part the way `UploadPart` does -- blocks first (which bumps
    /// their refcounts), then the part record -- and returns its block ids.
    async fn put_part(
        fs: &CasFS,
        key: &str,
        upload_id: &str,
        part_number: i64,
        tag: u8,
    ) -> Vec<BlockId> {
        let data = part_data(tag);
        let stream = AsyncByteStream::new(futures::stream::once(async move {
            Ok(bytes::Bytes::from(data))
        }));
        let (blocks, hash, size) = fs.store_object(BUCKET, key, stream).await.unwrap();
        fs.insert_multipart_part(
            BUCKET.to_string(),
            key.to_string(),
            size as usize,
            part_number,
            upload_id.to_string(),
            hash,
            blocks.clone(),
        )
        .unwrap();
        blocks
    }

    /// The refcount of one block, or `None` once its record is gone.
    fn rc_of(fs: &CasFS, id: &BlockId) -> Option<usize> {
        fs.shared_block_store()
            .block_tree()
            .get_block(id.as_slice())
            .unwrap()
            .map(|block| block.rc())
    }

    /// Whether the block's file is on disk, following the record's own depth.
    fn block_file_exists(fs: &CasFS, id: &BlockId) -> bool {
        fs.shared_block_store()
            .block_tree()
            .get_block(id.as_slice())
            .unwrap()
            .is_some_and(|block| block.disk_path(id, fs.fs_root().clone()).is_file())
    }

    /// How many part records the store holds, whatever their key format.
    fn part_count(fs: &CasFS) -> usize {
        fs.shared
            .meta_store()
            .get_tree_ext(MULTIPART_PARTS_TREE)
            .unwrap()
            .iter_all()
            .count()
    }

    /// The TTL rule: an upload past it is claimed and reaped with its parts
    /// and their references; one inside it is not touched at all.
    #[tokio::test]
    async fn aged_uploads_are_reaped_and_fresh_ones_are_not() {
        let (fs, _dir) = test_fs();

        plant_aged_upload(&fs, KEY, "old", Duration::from_secs(30 * 24 * 60 * 60));
        let doomed = put_part(&fs, KEY, "old", 1, 0xa1).await;

        fs.create_upload(BUCKET, KEY, "new").unwrap();
        let fresh = put_part(&fs, KEY, "new", 1, 0xa2).await;

        let stats = sweep_stale_uploads(&fs, TTL).await;

        assert_eq!(
            stats,
            SweepStats {
                uploads_reaped: 1,
                orphan_parts_reaped: 0,
                errors: 0
            }
        );
        assert!(fs.get_upload(BUCKET, KEY, "old").unwrap().is_none());
        assert!(fs.upload_parts(BUCKET, KEY, "old").unwrap().is_empty());
        for id in &doomed {
            assert_eq!(rc_of(&fs, id), None, "the stale upload's blocks are freed");
            assert!(!block_file_exists(&fs, id));
        }

        assert!(
            fs.get_upload(BUCKET, KEY, "new").unwrap().is_some(),
            "an upload inside the TTL survives"
        );
        assert_eq!(fs.upload_parts(BUCKET, KEY, "new").unwrap().len(), 1);
        for id in &fresh {
            assert_eq!(rc_of(&fs, id), Some(1));
            assert!(block_file_exists(&fs, id));
        }
    }

    /// Phase 2, both shapes at once: a part record left behind by a finished
    /// upload, and a legacy dash-keyed record no prefix scan can reach. Both
    /// are reaped from the key the walk yielded, and both drop their blocks.
    #[tokio::test]
    async fn orphan_parts_are_reaped_including_a_legacy_dash_keyed_record() {
        let (fs, _dir) = test_fs();

        // An orphan of the new key format: the part outlived its upload
        // record (a crash mid-abort, or the accepted upload_part race).
        let orphan = put_part(&fs, KEY, "gone", 1, 0xb1).await;

        // A legacy record: written under the pre-ADR-0003 dash key, which no
        // point read and no prefix scan can address. Only the value-driven
        // walk sees it -- reaching it at all IS the migration.
        let legacy_data = part_data(0xb2);
        let legacy_len = legacy_data.len();
        let stream = AsyncByteStream::new(futures::stream::once(async move {
            Ok(bytes::Bytes::from(legacy_data))
        }));
        let (legacy_blocks, _, _) = fs.store_object(BUCKET, KEY, stream).await.unwrap();
        let legacy_record = MultiPart::new(
            legacy_len,
            1,
            BUCKET.to_string(),
            KEY.to_string(),
            "legacy".to_string(),
            ContentHash::from([7u8; 16]),
            legacy_blocks.clone(),
        );
        fs.shared
            .meta_store()
            .get_tree_ext(MULTIPART_PARTS_TREE)
            .unwrap()
            .insert(
                format!("{BUCKET}-{KEY}-legacy-1").as_bytes(),
                legacy_record.to_vec(),
            )
            .unwrap();

        for id in orphan.iter().chain(legacy_blocks.iter()) {
            assert_eq!(rc_of(&fs, id), Some(1), "the part record holds its block");
        }

        let stats = sweep_stale_uploads(&fs, TTL).await;

        assert_eq!(
            stats,
            SweepStats {
                uploads_reaped: 0,
                orphan_parts_reaped: 2,
                errors: 0
            }
        );
        assert_eq!(part_count(&fs), 0, "both records are gone");
        for id in orphan.iter().chain(legacy_blocks.iter()) {
            assert_eq!(rc_of(&fs, id), None, "and so are their references");
            assert!(!block_file_exists(&fs, id), "and their files");
        }
    }

    /// The guard phase 2 rests on: a part whose upload record exists belongs
    /// to a live upload, however many parts or uploads sit beside it, and the
    /// sweep must leave it exactly as it found it.
    #[tokio::test]
    async fn a_live_uploads_parts_are_not_touched() {
        let (fs, _dir) = test_fs();

        fs.create_upload(BUCKET, KEY, "live").unwrap();
        let first = put_part(&fs, KEY, "live", 1, 0xc1).await;
        let second = put_part(&fs, KEY, "live", 2, 0xc2).await;
        // An orphan beside it, so the sweep really did run its phase 2.
        let orphan = put_part(&fs, "other/key", "gone", 1, 0xc3).await;

        let stats = sweep_stale_uploads(&fs, TTL).await;
        assert_eq!(stats.orphan_parts_reaped, 1);
        assert_eq!(stats.uploads_reaped, 0);
        assert_eq!(stats.errors, 0);

        assert_eq!(fs.upload_parts(BUCKET, KEY, "live").unwrap().len(), 2);
        for id in first.iter().chain(second.iter()) {
            assert_eq!(rc_of(&fs, id), Some(1), "the live upload keeps its blocks");
            assert!(block_file_exists(&fs, id));
        }
        for id in &orphan {
            assert_eq!(rc_of(&fs, id), None);
        }
    }

    /// A block a live object also references survives the orphan sweep: the
    /// release drops the part's OCCURRENCE, not the block.
    #[tokio::test]
    async fn the_orphan_sweep_only_drops_its_own_occurrence() {
        let (fs, _dir) = test_fs();

        // Object and orphan part carry identical content, so they dedup onto
        // one block record with two references.
        let data = part_data(0xd1);
        let len = data.len();
        let object_data = data.clone();
        let stream = AsyncByteStream::new(futures::stream::once(async move {
            Ok(bytes::Bytes::from(object_data))
        }));
        let object = fs
            .store_single_object_and_meta(BUCKET, "live", stream, len)
            .await
            .unwrap();
        let shared_blocks = put_part(&fs, KEY, "gone", 1, 0xd1).await;
        assert_eq!(object.blocks(), shared_blocks.as_slice());
        for id in &shared_blocks {
            assert_eq!(rc_of(&fs, id), Some(2));
        }

        assert_eq!(sweep_stale_uploads(&fs, TTL).await.orphan_parts_reaped, 1);

        for id in &shared_blocks {
            assert_eq!(rc_of(&fs, id), Some(1), "the object's reference remains");
            assert!(block_file_exists(&fs, id), "and its bytes with it");
        }
    }

    /// A sweep never aborts: an undecodable record is counted and stepped
    /// over, the records after it are still reaped, and the unreadable one is
    /// left in place -- its blocks cannot be named, so removing it would
    /// strand them.
    #[tokio::test]
    async fn an_undecodable_record_is_counted_and_stepped_over() {
        let (fs, _dir) = test_fs();

        fs.shared
            .meta_store()
            .get_tree_ext(MULTIPART_PARTS_TREE)
            .unwrap()
            .insert(b"garbage-key", b"not a part record".to_vec())
            .unwrap();
        let orphan = put_part(&fs, KEY, "gone", 1, 0xe1).await;

        let stats = sweep_stale_uploads(&fs, TTL).await;

        assert_eq!(stats.orphan_parts_reaped, 1, "the good record is reaped");
        assert_eq!(stats.errors, 1, "the bad one is counted");
        assert_eq!(part_count(&fs), 1, "and left for fsck");
        for id in &orphan {
            assert_eq!(rc_of(&fs, id), None);
        }
    }

    /// Empty store, empty sweep: no uploads, no parts, no errors, no panic.
    #[tokio::test]
    async fn an_empty_store_sweeps_clean() {
        let (fs, _dir) = test_fs();
        assert_eq!(sweep_stale_uploads(&fs, TTL).await, SweepStats::default());
    }
}
