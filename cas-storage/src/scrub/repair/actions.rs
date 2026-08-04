//! The individual repair actions, and the filesystem primitives they share.

use std::io;
use std::path::{Path, PathBuf};

use crate::cas::block_disk::QUARANTINE_DIR_NAME;
use crate::metastore::{Block, BlockId, block_disk_path};
use crate::scrub::findings::Severity;

use super::RepairContext;
use super::summary::{RepairOutcome, RepairStatus};

/// What one action's implementation reports back, before it is dressed as
/// an outcome. `Err` is a failure, carrying what went wrong.
pub(super) type ActionResult = Result<Done, String>;

/// The two ways an action can succeed.
pub(super) enum Done {
    /// The store changed.
    Applied(String),
    /// It did not need to.
    Skipped(String),
}

// ---------------------------------------------------------------------
// The actions
// ---------------------------------------------------------------------

/// Sets a record's rc to `to`, or frees the block when `to` is zero.
///
/// Reads the record at apply time rather than trusting the plan: the value
/// is a target, not a delta, so re-applying it is a no-op and an rc that
/// already reads `to` is skipped rather than rewritten.
pub(super) fn set_rc(ctx: &RepairContext<'_>, block: &BlockId, from: u64, to: u64) -> ActionResult {
    let shared = ctx.shared();
    let Some(record) = shared
        .block_tree()
        .get_block(block.as_slice())
        .map_err(|e| format!("the record could not be read: {e}"))?
    else {
        return Ok(Done::Skipped(
            "the record is gone, so there is no rc to set".to_string(),
        ));
    };

    let current = record.rc() as u64;
    if current == to {
        return Ok(Done::Skipped(format!("rc already reads {to}")));
    }

    if to == 0 {
        // Nothing holds this block. The protocol has no rc=0 state -- the
        // last decrement removes the record and unlinks the file -- so this
        // does the same, and in the same order: record first, file second,
        // so a crash in between leaves an orphan file (leakage) rather than
        // a record with no bytes (loss).
        let path = block_disk_path(block, record.depth(), ctx.blocks_root().to_path_buf());
        let mut tx = shared.meta_store().begin_transaction();
        if let Err(e) = tx.remove_block_record(*block) {
            tx.rollback();
            return Err(format!("the record could not be removed: {e}"));
        }
        tx.commit()
            .map_err(|e| format!("the record removal would not commit: {e}"))?;
        unlink_tolerant(&path)?;
        return Ok(Done::Applied(format!(
            "rc {current} -> 0 (planned from {from}): nothing holds this block, so the record is \
             removed and {} unlinked",
            path.display()
        )));
    }

    let rc = usize::try_from(to).map_err(|_| format!("holder count {to} does not fit an rc"))?;
    // Only the count moves: size, depth and the flags byte (whose reserved
    // bits are not this build's to interpret) are carried over.
    let updated = Block::from_parts(record.size(), record.depth(), rc, record.flags());
    let mut tx = shared.meta_store().begin_transaction();
    if let Err(e) = tx.put_block_record(*block, &updated) {
        tx.rollback();
        return Err(format!("the record could not be written: {e}"));
    }
    tx.commit()
        .map_err(|e| format!("the rc write would not commit: {e}"))?;

    Ok(Done::Applied(format!(
        "rc {current} -> {to} (planned from {from})"
    )))
}

/// Points a record at a copy of its block found at another depth.
///
/// Hard rule 3: the candidate is re-hashed first. fsck does not have the
/// writer's bytes, so unlike the write path's heal it cannot assume a file
/// under an address holds that address's content. A candidate that does not
/// hash is corrupt residue and is quarantined instead; if none of them
/// hash, the record is marked degraded, which is where it would have ended
/// up with no candidates at all.
///
/// Once a candidate is adopted the remaining copies are unreferenced
/// duplicates of it, and go the way of any off-depth duplicate.
pub(super) fn adopt_orphan(
    ctx: &RepairContext<'_>,
    block: &BlockId,
    record_depth: u8,
    candidates: &[(u8, PathBuf)],
) -> ActionResult {
    let shared = ctx.shared();
    let Some(record) = shared
        .block_tree()
        .get_block(block.as_slice())
        .map_err(|e| format!("the record could not be read: {e}"))?
    else {
        return Ok(Done::Skipped(
            "the record is gone, so there is nothing to adopt for".to_string(),
        ));
    };
    if block_disk_path(block, record.depth(), ctx.blocks_root().to_path_buf()).is_file() {
        return Ok(Done::Skipped(format!(
            "the record's own file is at depth {} after all",
            record.depth()
        )));
    }

    let mut quarantined: Vec<String> = Vec::new();
    for (index, (depth, path)) in candidates.iter().enumerate() {
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(format!(
                    "candidate {} could not be read: {e}",
                    path.display()
                ));
            }
        };

        let actual = shared.hasher().hash(&bytes);
        if actual != *block {
            let dest = quarantine_file(ctx, path)?;
            quarantined.push(format!(
                "{} (hashes to {}) -> {}",
                path.display(),
                actual.to_hex(),
                dest.display()
            ));
            continue;
        }

        // Verified. The record follows the file: depth is placement, and
        // rc, size and flags are the record's own accounting.
        let adopted = Block::from_parts(record.size(), *depth, record.rc(), record.flags());
        let mut tx = shared.meta_store().begin_transaction();
        if let Err(e) = tx.put_block_record(*block, &adopted) {
            tx.rollback();
            return Err(format!("the adopted record could not be written: {e}"));
        }
        tx.commit()
            .map_err(|e| format!("the adoption would not commit: {e}"))?;

        // Anything still standing is now a duplicate of the adopted file.
        let mut deleted = 0;
        for (_, other) in &candidates[index + 1..] {
            unlink_tolerant(other)?;
            deleted += 1;
        }

        let mut detail = format!(
            "re-hashed {} and it matches: the record moves from depth {record_depth} to {depth}",
            path.display()
        );
        if deleted > 0 {
            detail.push_str(&format!(", {deleted} further copy/copies deleted"));
        }
        if !quarantined.is_empty() {
            detail.push_str(&format!(", after quarantining {}", quarantined.join(", ")));
        }
        return Ok(Done::Applied(detail));
    }

    if quarantined.is_empty() {
        return Ok(Done::Skipped(
            "no candidate is on disk any more; the record is left to the degraded marking"
                .to_string(),
        ));
    }

    // Every candidate was corrupt: the bytes really are gone.
    mark_degraded(ctx, block)?;
    Ok(Done::Applied(format!(
        "no candidate hashes to this address, so none was adopted: quarantined {} and marked the \
         record degraded",
        quarantined.join(", ")
    )))
}

/// Moves a corrupt block file out of the blocks root and marks its record
/// degraded.
///
/// The two halves are one action because either alone is worse than
/// neither: a quarantined file with a live record leaves dedup pointing at
/// nothing, and a degraded record over corrupt bytes leaves the bytes where
/// a reader can still find them.
pub(super) fn quarantine_corrupt(
    ctx: &RepairContext<'_>,
    block: &BlockId,
    path: &Path,
) -> ActionResult {
    if !path.exists() {
        // Still make sure the record says what it should: the file may have
        // been moved by an earlier, interrupted run.
        let flagged = mark_degraded(ctx, block)?;
        return Ok(Done::Skipped(format!(
            "the file is already gone from {}{}",
            path.display(),
            if flagged {
                "; its record is now marked degraded"
            } else {
                ""
            }
        )));
    }

    let dest = quarantine_file(ctx, path)?;
    mark_degraded(ctx, block)?;
    Ok(Done::Applied(format!(
        "moved to {} and the record marked degraded, so the next PUT of this content heals it \
         instead of deduplicating against bytes that do not hash",
        dest.display()
    )))
}

/// Flags a record whose bytes are gone (the action; see [`mark_degraded`]).
pub(super) fn mark_degraded_action(ctx: &RepairContext<'_>, block: &BlockId) -> ActionResult {
    if mark_degraded(ctx, block)? {
        Ok(Done::Applied(
            "record flagged degraded: its holders stay accounted for, and the next PUT of this \
             content writes the file and clears the flag"
                .to_string(),
        ))
    } else {
        Ok(Done::Skipped(
            "the record is gone or already degraded".to_string(),
        ))
    }
}

/// Deletes a block file nothing references. ENOENT is success, which is
/// what makes a killed repair re-runnable.
pub(super) fn delete_unreferenced(path: &Path) -> ActionResult {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(Done::Applied(format!("deleted {}", path.display()))),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            Ok(Done::Skipped("already gone".to_string()))
        }
        Err(e) => Err(format!("could not delete {}: {e}", path.display())),
    }
}

/// Moves something that is not part of the layout into quarantine.
pub(super) fn quarantine_foreign(ctx: &RepairContext<'_>, path: &Path) -> ActionResult {
    if !path.exists() {
        return Ok(Done::Skipped("already gone".to_string()));
    }
    let dest = quarantine_file(ctx, path)?;
    Ok(Done::Applied(format!(
        "moved out of the blocks root to {}; fsck did not put it there and does not delete it",
        dest.display()
    )))
}

/// Resumes a bucket teardown: every object through the daemon's striped
/// delete path, then the tree.
///
/// Deleting through `delete_object` rather than dropping the tree outright
/// is what keeps the refcounts honest as it goes -- the same decrements the
/// crashed teardown would have made.
///
/// An object key that is not UTF-8 cannot go through that path (it takes
/// `&str`, and the daemon's own loop asserts it). fsck must not panic on
/// one, so the key is reported WARN and skipped; the tree is still dropped,
/// because the `_BUCKETS` row is already gone and leaving the tree would
/// strand it forever. The recount that follows this action reconciles the
/// references the skipped object was holding.
pub(super) async fn resume_bucket_teardown(
    ctx: &RepairContext<'_>,
    bucket: &str,
    extra: &mut Vec<RepairOutcome>,
) -> ActionResult {
    let namespace = ctx.namespace();
    let tree = namespace
        .get_bucket_ext(bucket)
        .map_err(|e| format!("the object tree would not open: {e}"))?;

    // The keys are collected first: the delete path removes records from
    // the tree being iterated, and this store makes no promise about what
    // an iterator does then.
    let mut keys: Vec<Vec<u8>> = Vec::new();
    for item in tree.iter_all() {
        let (key, _) = item.map_err(|e| format!("the object tree would not iterate: {e}"))?;
        keys.push(key);
    }

    let mut deleted = 0usize;
    let mut skipped = 0usize;
    for key in keys {
        match std::str::from_utf8(&key) {
            Ok(key) => {
                ctx.fs
                    .delete_object(bucket, key)
                    .await
                    .map_err(|e| format!("object {bucket}/{key} would not delete: {e}"))?;
                deleted += 1;
            }
            Err(_) => {
                skipped += 1;
                extra.push(
                    RepairOutcome::new(
                        "resume_bucket_teardown",
                        RepairStatus::Skipped,
                        format!(
                            "object key {} in {bucket} is not UTF-8, so the striped delete path \
                             cannot name it: the record goes with the tree and the recount \
                             reconciles its block references",
                            String::from_utf8_lossy(&key)
                        ),
                    )
                    .at(Severity::Warn),
                );
            }
        }
    }

    namespace
        .drop_bucket(bucket)
        .map_err(|e| format!("the object tree would not drop: {e}"))?;

    Ok(Done::Applied(format!(
        "teardown resumed: {deleted} object(s) deleted through the striped path, {skipped} \
         skipped, tree dropped"
    )))
}

/// Reaps a part record no upload record owns: the record and the block
/// references it held leave together.
///
/// Goes through the daemon's own primitive
/// ([`CasFS::reap_part`](crate::cas::CasFS::reap_part)) rather than
/// removing the record here and letting the closing recount collect the
/// references. Two reasons, and the second is why this is not a
/// `tx.remove()`:
///
/// - the take IS the claim. `Ok(None)` -- the record was already gone --
///   is a skip, not a failure, which is what makes a killed `--repair`
///   re-runnable over the parts it did get to;
/// - the release drops exactly the occurrences that record held, per
///   occurrence, through the striped decrement. The recount that closes
///   the run then validates the result rather than being the thing that
///   produces it.
pub(super) async fn reap_orphan_part(
    ctx: &RepairContext<'_>,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i64,
    storage_key: &[u8],
) -> ActionResult {
    match ctx.fs.reap_part(storage_key).await {
        Ok(Some(part)) => Ok(Done::Applied(format!(
            "part {part_number} of upload {upload_id} ({bucket}/{key}) removed, and the {} block \
             reference(s) it held released",
            part.blocks().len()
        ))),
        Ok(None) => Ok(Done::Skipped(format!(
            "part {part_number} of upload {upload_id} ({bucket}/{key}) is already gone, so there \
             was nothing to release"
        ))),
        Err(e) => Err(format!(
            "part {part_number} of upload {upload_id} ({bucket}/{key}) could not be reaped: {e}. \
             Its blocks stay referenced by the record that survives, which is the over-count \
             direction, and the next run retries"
        )),
    }
}

// ---------------------------------------------------------------------
// Shared primitives
// ---------------------------------------------------------------------

/// Sets the degraded flag on a record, if there is one and it is not set
/// already. Returns whether anything changed.
fn mark_degraded(ctx: &RepairContext<'_>, block: &BlockId) -> Result<bool, String> {
    let shared = ctx.shared();
    let Some(record) = shared
        .block_tree()
        .get_block(block.as_slice())
        .map_err(|e| format!("the record could not be read: {e}"))?
    else {
        return Ok(false);
    };
    if record.is_degraded() {
        return Ok(false);
    }

    let mut flagged = Block::from_parts(record.size(), record.depth(), record.rc(), record.flags());
    flagged.set_degraded(true);
    let mut tx = shared.meta_store().begin_transaction();
    if let Err(e) = tx.put_block_record(*block, &flagged) {
        tx.rollback();
        return Err(format!("the degraded flag could not be written: {e}"));
    }
    tx.commit()
        .map_err(|e| format!("the degraded flag would not commit: {e}"))?;
    Ok(true)
}

/// Unlinks a path, treating "already gone" as success.
fn unlink_tolerant(path: &Path) -> Result<(), String> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(format!("could not unlink {}: {e}", path.display())),
    }
}

/// Moves `path` into `blocks/.quarantine/` and returns where it went.
///
/// A filesystem quarantine rather than a DB tree (ADR 0005): it is visible
/// with `ls`, it survives metadata damage, and a block file's full-id name
/// stays self-identifying wherever it sits. The directory is created and
/// fsynced on use -- `create_dir_all` is a no-op after the first time -- and
/// a name already taken gets a numeric suffix rather than overwriting what
/// an earlier run set aside.
fn quarantine_file(ctx: &RepairContext<'_>, path: &Path) -> Result<PathBuf, String> {
    let dir = ctx.blocks_root().join(QUARANTINE_DIR_NAME);
    std::fs::create_dir_all(&dir)
        .map_err(|e| format!("the quarantine directory could not be created: {e}"))?;
    fsync_dir(&dir)?;

    let base = path.file_name().unwrap_or_else(|| "unnamed".as_ref());
    let mut dest = dir.join(base);
    let mut collision = 0u32;
    while dest.exists() {
        collision += 1;
        let mut name = base.to_os_string();
        name.push(format!(".{collision}"));
        dest = dir.join(name);
    }

    std::fs::rename(path, &dest).map_err(|e| {
        format!(
            "could not move {} to {}: {e}",
            path.display(),
            dest.display()
        )
    })?;
    fsync_dir(&dir)?;
    Ok(dest)
}

/// Fsyncs a directory, so a quarantine survives the power cut that a
/// corruption finding often precedes.
fn fsync_dir(dir: &Path) -> Result<(), String> {
    std::fs::File::open(dir)
        .and_then(|f| f.sync_all())
        .map_err(|e| format!("could not fsync {}: {e}", dir.display()))
}
