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
//!    deleted a freshly rewritten block).
//!
//! Ordering object-removal-then-decrements means a crash between the two
//! steps leaves rc over-counts: leakage for ADR 0005 to reconcile, never
//! loss. Per-block failures are logged and the loop continues -- a failed
//! decrement or unlink strands at most one block (leak class), while
//! aborting the loop would strand every remaining one.

use std::sync::Arc;

use super::fs::CasFS;
use super::shared_block_store::SharedBlockStore;
use crate::metastore::{BlockDecrement, BlockId, MetaError};

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

#[tracing::instrument(skip(fs), fields(bucket = %bucket, key = %key, blocks_deleted))]
pub(super) async fn delete_object(fs: &CasFS, bucket: &str, key: &str) -> Result<(), MetaError> {
    // Step 1: atomically take the object record out of the namespace DB.
    // An absent key deletes nothing and is not an error (idempotent).
    let mut tx = fs.namespace.begin_transaction();
    let obj = match tx.take_object(bucket, key) {
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
        return Ok(());
    };

    tracing::Span::current().record("blocks_deleted", obj.blocks().len());

    // Step 2: per block OCCURRENCE (a multipart object may list one block
    // several times, and each occurrence holds one reference), one stripe
    // at a time.
    for block_id in obj.blocks() {
        let stripe_guard = fs.shared.stripes().for_hash(block_id).lock_owned().await;
        let shared = fs.shared.clone();
        let metrics = fs.metrics.clone();
        let id = *block_id;

        // Same in-flight gauge as the write side; see store_object.
        fs.metrics.block_disk_op_started();
        let joined = tokio::task::spawn_blocking(move || {
            let result = decrement_one_block(shared, stripe_guard, id);
            metrics.block_disk_op_finished();
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

    Ok(())
}

#[tracing::instrument(skip(fs), fields(bucket = %bucket_name, objects_deleted))]
pub(super) async fn bucket_delete(fs: &CasFS, bucket_name: &str) -> Result<(), MetaError> {
    let bmt = fs.namespace.get_allbuckets_tree()?;
    bmt.remove(bucket_name.as_bytes())?;

    let bucket = fs.namespace.get_bucket_ext(bucket_name)?;
    let mut object_count = 0;
    for key_val in bucket.iter_all() {
        let (key, _) = key_val?;
        delete_object(
            fs,
            bucket_name,
            std::str::from_utf8(&key).expect("keys are valid utf-8"),
        )
        .await?;
        object_count += 1;
    }

    tracing::Span::current().record("objects_deleted", object_count);

    fs.namespace.drop_bucket(bucket_name)?;
    Ok(())
}
