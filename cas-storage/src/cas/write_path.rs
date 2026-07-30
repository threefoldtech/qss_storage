use std::io;
use std::sync::Arc;

use super::buffered_byte_stream::BufferedByteStream;
use super::byte_stream::AsyncByteStream;
use super::fs::CasFS;
use crate::metastore::{BlockID, ContentHash, MetaError, Object, ObjectData};
use crate::metrics::SharedMetrics;
use faster_hex::hex_string;
use futures::{
    channel::mpsc::unbounded,
    sink::SinkExt,
    stream,
    stream::{StreamExt, TryStreamExt},
};
use md5::{Digest, Md5};

/// RAII guard for a single in-flight block write.
///
/// Constructed via `new_pending`, which increments the `block_pending`
/// metric. Exactly one terminal method -- `.written()` or `.failed()`
/// -- must be called before the guard drops, or `Drop` reports the
/// block as dropped. The compiler enforces this via `#[must_use]`.
///
/// The `ignored` case (block already exists, no disk write needed) is
/// not modelled here on purpose -- just call `metrics.block_ignored()`
/// directly, because there is no `Pending` state to transition from.
///
/// Owned (not borrowed) `SharedMetrics` so the guard can live across
/// `.await` inside `store_object`'s per-chunk closure without borrow-
/// checker gymnastics. `SharedMetrics` is `Arc`-backed, so this is
/// just another refcount clone.
#[must_use = "a BlockWriteGuard must be resolved with .written() or .failed()"]
pub(super) struct BlockWriteGuard {
    metrics: SharedMetrics,
    state: GuardState,
}

enum GuardState {
    Pending,
    Resolved,
}

impl BlockWriteGuard {
    pub fn new_pending(metrics: SharedMetrics) -> Self {
        metrics.block_pending();
        Self {
            metrics,
            state: GuardState::Pending,
        }
    }

    pub fn written(mut self, _size: usize) {
        self.state = GuardState::Resolved;
        self.metrics.block_written();
    }

    pub fn failed(mut self) {
        self.state = GuardState::Resolved;
        self.metrics.block_write_error();
    }
}

impl Drop for BlockWriteGuard {
    fn drop(&mut self) {
        if matches!(self.state, GuardState::Pending) {
            self.metrics.blocks_dropped(1);
        }
    }
}

#[tracing::instrument(skip(fs, data), fields(bucket = %bucket_name, key = %key, size, blocks))]
pub(super) async fn store_object(
    fs: &CasFS,
    bucket_name: &str,
    key: &str,
    data: AsyncByteStream,
) -> io::Result<(Vec<BlockID>, ContentHash, u64)> {
    let old_obj_meta = match fs.get_object_meta(bucket_name, key) {
        Ok(Some(obj_meta)) => Some(obj_meta),
        _ => None,
    };
    let old_obj_meta = Arc::new(old_obj_meta);

    let (tx, rx) = unbounded();
    let mut content_hash = Md5::new();
    let data = BufferedByteStream::new(data);
    let mut size = 0;
    data.map(|res| match res {
        Ok(buffers) => buffers.into_iter().map(Ok).collect(),
        Err(e) => vec![Err(e)],
    })
    .map(stream::iter)
    .flatten()
    .inspect(|maybe_bytes| {
        if let Ok(bytes) = maybe_bytes {
            content_hash.update(bytes);
            size += bytes.len() as u64;
            fs.metrics.bytes_received(bytes.len());
        }
    })
    .zip(stream::repeat((tx, old_obj_meta)))
    .enumerate()
    .for_each(
        |(idx, (maybe_chunk, (mut tx, old_obj_meta)))| async move {
            if let Err(e) = maybe_chunk {
                if let Err(e) = tx
                    .send(Err(std::io::Error::new(e.kind(), e.to_string())))
                    .await
                {
                    tracing::error!(error = %e, "Could not convey result");
                }
                return;
            }
            // unwrap is safe as we checked that there is no error above
            let bytes: Vec<u8> = maybe_chunk.unwrap();
            let mut hasher = Md5::new();
            hasher.update(&bytes);
            let block_hash: BlockID = hasher.finalize().into();
            let data_len = bytes.len();

            // check if this key already has this block
            let key_has_block = if let Some(obj) = old_obj_meta.as_ref() {
                obj.has_block(&block_hash)
            } else {
                false
            };

            // begin the transaction
            // there are two main things we need to do here:
            // 1. write the meta to the database
            //      - if the block already exists, we don't need to write it to the storage
            //      - if the block does not exist, we need to write it to the storage
            // 2. write the actual block to disk
            //
            // we commit the meta database transaction BEFORE writing the block to disk
            // to avoid holding the lock during slow I/O operations.
            //
            // IMPORTANT: In multi-user mode, use shared MetaStore for block transactions
            // to ensure blocks are written to the shared _BLOCKS tree, not user-specific tree
            let mut store_tx = fs.shared.meta_store().begin_transaction();
            let write_meta_result = store_tx.write_block(block_hash, data_len, key_has_block);

            let block = match write_meta_result {
                Err(e) => {
                    if let Err(e) = tx.unbounded_send(Err(e.into())) {
                        tracing::error!(error = %e, "Could not send transaction error");
                    }
                    return;
                }
                Ok((false, _)) => {
                    // the block already exists, no need to write it to the storage.
                    // No guard: we never transitioned to Pending.
                    fs.metrics.block_ignored();

                    tracing::debug!(target: "cas_storage::locks", "Committing metadata transaction (block exists)");
                    Box::new(store_tx).commit().unwrap();

                    if let Err(e) = tx.unbounded_send(Ok((idx, block_hash))) {
                        tracing::error!(error = %e, "Could not send block id");
                    }
                    return;
                }
                Ok((true, block)) => {
                    // COMMIT IMMEDIATELY to release lock
                    tracing::debug!(target: "cas_storage::locks", "Committing metadata transaction (new block)");
                    Box::new(store_tx).commit().unwrap();

                    block
                }
            };

            // From here on we have a new block to write to disk. The
            // guard tracks the Pending -> Written / Failed / Dropped
            // transition; if we return without resolving it, Drop
            // reports the block as dropped.
            let guard = BlockWriteGuard::new_pending(fs.metrics.clone());

            // write the actual block to disk
            // if the disk operation fails, we must manually rollback (compensating transaction)
            let block_path = block.disk_path(fs.fs_root().clone());

            // Helper to cleanup on failure
            let cleanup_on_failure = || {
                // We need to delete the block we just added.
                // Since we just added it with rc=1, we can just delete it.
                // We accept potential data leakage here if this cleanup fails,
                // as per the design principles (leakage is better than data loss).
                let tree = fs.shared.block_tree();
                if let Err(e) = tree.remove(&block_hash) {
                    tracing::warn!(block = %hex_string(&block_hash), error = %e, "Failed to cleanup orphan block metadata");
                } else {
                    tracing::debug!(block = %hex_string(&block_hash), "Cleaned up orphan block metadata");
                }
            };

            if let Err(e) = fs.async_fs.create_dir_all(block_path.parent().unwrap()) {
                cleanup_on_failure();
                if let Err(e) = tx.unbounded_send(Err(e)) {
                    tracing::error!(error = %e, "Could not send path create error");
                }
                guard.failed();
                return;
            }
            if let Err(e) = fs.async_fs.write(&block_path, &bytes) {
                cleanup_on_failure();
                if let Err(e) = tx.unbounded_send(Err(e)) {
                    tracing::error!(error = %e, "Could not send block write error");
                }
                guard.failed();
                return;
            }

            guard.written(bytes.len());

            if let Err(e) = tx.unbounded_send(Ok((idx, block_hash))) {
                tracing::error!(error = %e, "Could not send block id");
            }
        },
    )
    .await;

    let mut ids = rx.try_collect::<Vec<(usize, BlockID)>>().await?;
    // Make sure the chunks are in the proper order
    ids.sort_by_key(|a| a.0);

    let blocks: Vec<BlockID> = ids.into_iter().map(|(_, id)| id).collect();

    tracing::Span::current().record("size", size);
    tracing::Span::current().record("blocks", blocks.len());

    Ok((blocks, ContentHash(content_hash.finalize().into()), size))
}

pub(super) async fn store_single_object_and_meta(
    fs: &CasFS,
    bucket_name: &str,
    key: &str,
    data: AsyncByteStream,
    len: usize,
) -> io::Result<Object> {
    let (blocks, content_hash, size) = if len > 0 {
        store_object(fs, bucket_name, key, data).await?
    } else {
        tracing::warn!(%key, "Skipping store for empty blob");
        // An empty object still has a content hash: the MD5 of no bytes,
        // which is the ETag d41d8cd98f00b204e9800998ecf8427e that clients
        // expect for a zero-length object.
        (Vec::new(), ContentHash(Md5::digest(b"").into()), 0)
    };
    let obj = fs
        .create_object_meta(
            bucket_name,
            key,
            size,
            content_hash,
            ObjectData::SinglePart { blocks },
        )
        .unwrap();
    Ok(obj)
}

pub(super) fn store_inlined_object(
    fs: &CasFS,
    bucket_name: &str,
    key: &str,
    data: Vec<u8>,
) -> Result<Object, MetaError> {
    let content_hash = ContentHash(Md5::digest(&data).into());
    let size = data.len() as u64;
    fs.create_object_meta(
        bucket_name,
        key,
        size,
        content_hash,
        ObjectData::Inline { data },
    )
}
