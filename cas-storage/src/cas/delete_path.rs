//! The DELETE side of the ADR 0006 block protocol.
//!
//! Two steps, two databases:
//!
//! 1. The object record is read AND removed in one namespace-DB
//!    transaction (`take_object`) -- the atomic pair that makes DELETE
//!    idempotent and defeats concurrent double-DELETE double-decrements.
//! 2. Each block occurrence is decremented under its stripe, one at a
//!    time: re-check + decrement/remove + unlink share one stripe hold
//!    inside one `spawn_blocking` closure (hard rule 4 -- a cancellable
//!    await between decrement and unlink is how a detached unlink once
//!    deleted a freshly rewritten block). That loop is [`release_blocks`],
//!    which multipart abort and the stale-upload GC (ADR 0003) call with
//!    the blocks of a part record they have just removed.
//!
//! Ordering object-removal-then-decrements means a crash between the two
//! steps leaves rc over-counts: leakage for ADR 0005 to reconcile, never
//! loss. Per-block failures are logged and the loop continues -- a failed
//! decrement or unlink strands at most one block (leak class), while
//! aborting the loop would strand every remaining one.

use std::sync::Arc;

use super::fs::CasFS;
use super::object_key;
use super::shared_block_store::SharedBlockStore;
use crate::metastore::{BlockDecrement, BlockId, MetaError};
use crate::metrics::SharedMetrics;

/// One block-occurrence decrement under the stripe, on a blocking thread.
///
/// Commit order inside the removal branch: record first, unlink second.
/// If the unlink is lost (crash after commit), the residue is an orphan
/// file that the write path heals in place and fsck can collect; unlink
/// maps ENOENT to Ok, so replaying it is harmless.
fn decrement_one_block(
    shared: Arc<SharedBlockStore>,
    _stripe_guard: tokio::sync::OwnedMutexGuard<()>,
    block_id: BlockId,
) -> Result<(), MetaError> {
    let mut tx = shared.meta_store().begin_transaction();
    match tx.decrement_block_rc(block_id) {
        Err(e) => {
            tx.rollback();
            Err(e)
        }
        Ok(BlockDecrement::Missing) => {
            tx.rollback();
            tracing::warn!(
                block_hash = %block_id.to_hex(),
                "Block not found in tree during deletion"
            );
            Ok(())
        }
        Ok(BlockDecrement::Decremented(_)) => tx.commit(),
        Ok(BlockDecrement::Removed(block)) => {
            tx.commit()?;
            shared
                .disk_writer()
                .unlink_block(&*shared.disk_ops(), &block_id, block.depth())
                .map_err(|e| MetaError::OtherDBError(format!("unlinking block file: {e}")))
        }
    }
}

/// Drops one reference per entry of `blocks`: ADR 0006's delete-side
/// primitive, with a name.
///
/// Object delete releases the blocks of an object it has taken; from ADR
/// 0003 on, multipart abort and the stale-upload GC release the blocks of a
/// part record they have removed. All three are the same operation over an
/// explicit list, which is why this is a factoring of `delete_object`'s loop
/// rather than a second implementation -- there is exactly one place where
/// references are dropped.
///
/// # The caller must have removed the owning record FIRST
///
/// Record first, release second -- never the reverse (hard rule 2). A crash
/// between the two then leaves a block whose rc exceeds its walked holders:
/// an over-count, which fsck reports INFO and the next recount collects. The
/// reverse order leaves a record still claiming references that were already
/// dropped, so fsck's recount sees holders exceeding rc -- a loss-shaped
/// under-count it must call CRITICAL, and one that `--repair` would "fix" by
/// raising the rc back, leaking those blocks permanently. The ordering keeps
/// the accounting wrong in the only direction that is recoverable.
///
/// Per OCCURRENCE: a list may name one block several times (a multipart
/// object commonly does), and each occurrence holds its own reference.
///
/// Never aborts the loop. A failed decrement or unlink strands at most one
/// block (leak class); giving up would strand every remaining one. Failures
/// are logged, not returned -- there is no caller-level recovery for them.
pub(crate) async fn release_blocks(
    shared: &Arc<SharedBlockStore>,
    metrics: &SharedMetrics,
    blocks: &[BlockId],
) {
    for block_id in blocks {
        let stripe_guard = shared.stripes().for_hash(block_id).lock_owned().await;
        let shared = shared.clone();
        let metrics_for_task = metrics.clone();
        let id = *block_id;

        // Same in-flight gauge as the write side; see store_object.
        metrics.block_disk_op_started();
        let joined = tokio::task::spawn_blocking(move || {
            let result = decrement_one_block(shared, stripe_guard, id);
            metrics_for_task.block_disk_op_finished();
            result
        })
        .await;

        // Log and continue with the remaining blocks -- never abort the
        // loop, never panic. The failed occurrence leaks (over-count or
        // stale file); the remaining ones still get their decrements.
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::error!(
                    block_hash = %id.to_hex(),
                    error = %e,
                    "Block decrement failed; continuing with remaining blocks"
                );
            }
            Err(join_err) => {
                tracing::error!(
                    block_hash = %id.to_hex(),
                    error = %join_err,
                    "Block decrement task did not complete; continuing with remaining blocks"
                );
            }
        }
    }
}

#[tracing::instrument(skip(fs, key), fields(bucket = %bucket, key = %object_key(key), blocks_deleted))]
pub(super) async fn delete_object(fs: &CasFS, bucket: &str, key: &[u8]) -> Result<bool, MetaError> {
    // Step 1: atomically take the object record out of the namespace DB, and
    // give its logical bytes back to the bucket's usage counter in the same
    // transaction. An absent key deletes nothing and is not an error
    // (idempotent), and moves no counter.
    let mut tx = fs.namespace.begin_transaction();
    let taken = tx.take_object(bucket, key).and_then(|obj| {
        if let Some(obj) = &obj {
            tx.add_bucket_usage(bucket, -(obj.size() as i64))?;
        }
        Ok(obj)
    });
    let obj = match taken {
        Ok(obj) => {
            tx.commit()?;
            obj
        }
        Err(e) => {
            tx.rollback();
            return Err(e);
        }
    };
    let Some(obj) = obj else {
        tracing::Span::current().record("blocks_deleted", 0);
        return Ok(false);
    };

    tracing::Span::current().record("blocks_deleted", obj.blocks().len());

    // Step 2: the object record is gone, so its references may be dropped --
    // record first, release second (see release_blocks).
    release_blocks(&fs.shared, &fs.metrics, obj.blocks()).await;

    Ok(true)
}

#[tracing::instrument(skip(fs), fields(bucket = %bucket_name, objects_deleted))]
pub(super) async fn bucket_delete(fs: &CasFS, bucket_name: &str) -> Result<(), MetaError> {
    let bmt = fs.namespace.get_allbuckets_tree()?;
    bmt.remove(bucket_name.as_bytes())?;

    let bucket = fs.namespace.get_bucket_ext(bucket_name)?;
    let mut object_count = 0;
    for key_val in bucket.iter_all() {
        let (key, _) = key_val?;
        delete_object(fs, bucket_name, &key).await?;
        object_count += 1;
    }

    tracing::Span::current().record("objects_deleted", object_count);

    fs.namespace.drop_bucket(bucket_name)?;
    Ok(())
}
