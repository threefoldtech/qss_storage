use std::io;
use std::sync::Arc;

use super::buffered_byte_stream::BufferedByteStream;
use super::byte_stream::AsyncByteStream;
use super::fs::CasFS;
use super::shared_block_store::SharedBlockStore;
use crate::metastore::{BlockId, ContentHash, MetaError, Object, ObjectData};
use crate::metrics::SharedMetrics;
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

/// One block through the ADR 0006 file-first protocol.
///
/// Runs on a blocking thread; `_stripe_guard` is the block's stripe, owned
/// by this function for its whole extent. That single fact carries three
/// properties at once:
///
/// - every fjall commit (and its journal fsync at Fsync durability) runs
///   off the executor;
/// - rename + insert are uncancellable as a unit -- cancelling the request
///   drops nothing mid-protocol, the detached closure runs to completion
///   and only then releases the stripe;
/// - the `Send`-soundness rule on `FjallTransaction` holds trivially: each
///   transaction begins and commits on this one thread, no await anywhere
///   between (hard rule 3).
///
/// Lock order is stripe first, fjall second; fjall is the leaf lock (hard
/// rule 6), and no fjall guard is held during the disk I/O between the two
/// transactions.
///
/// Residue of a cancelled request: a completed bump (rc over-count) or a
/// completed insert (record+file with no object) -- leak class for ADR
/// 0005 to reconcile, never a torn state.
fn write_one_block(
    shared: Arc<SharedBlockStore>,
    metrics: SharedMetrics,
    _stripe_guard: tokio::sync::OwnedMutexGuard<()>,
    block_hash: BlockId,
    bytes: &[u8],
) -> io::Result<()> {
    // Dedup check and rc bump: one transactional RMW under the stripe
    // (hard rule 2). EVERY hit bumps -- the key_has_block skip is gone.
    let mut store_tx = shared.meta_store().begin_transaction();
    match store_tx.bump_block_rc(block_hash) {
        Err(e) => {
            store_tx.rollback();
            return Err(e.into());
        }
        Ok(Some(_)) => {
            tracing::debug!(target: "cas_storage::locks", "Committing dedup rc bump");
            store_tx.commit()?;
            // Dedup hit: no disk write, no guard was ever Pending.
            metrics.block_ignored();
            return Ok(());
        }
        // Release the fjall writer before any disk I/O.
        Ok(None) => store_tx.rollback(),
    }

    // New block: the file reaches its final path durably BEFORE the record
    // is committed (hard rule 5). The guard tracks Pending -> Written /
    // Failed; a panic in here surfaces as Dropped.
    let write_guard = BlockWriteGuard::new_pending(metrics);

    // Depth: probe the id's dir chain for an orphan to heal in place,
    // else the placement policy. Only the new-block path pays for this.
    let depth = shared.placement().choose_depth(&block_hash);

    let attempt = (|| -> io::Result<()> {
        shared
            .disk_writer()
            .write_block(&*shared.disk_ops(), &block_hash, depth, bytes)?;

        let mut store_tx = shared.meta_store().begin_transaction();
        if let Err(e) = store_tx.insert_new_block(block_hash, bytes.len(), depth) {
            store_tx.rollback();
            return Err(e.into());
        }
        tracing::debug!(target: "cas_storage::locks", "Committing new block record");
        store_tx.commit()?;
        Ok(())
    })();

    match attempt {
        Ok(()) => {
            write_guard.written(bytes.len());
            Ok(())
        }
        Err(e) => {
            // File-first means there is nothing to compensate: either the
            // file write failed (no record was attempted) or the record
            // commit failed (the file at its final path is orphan residue
            // that a retry heals in place and fsck can collect).
            write_guard.failed();
            Err(e)
        }
    }
}

#[tracing::instrument(skip(fs, data), fields(bucket = %bucket_name, key = %key, size, blocks))]
pub(super) async fn store_object(
    fs: &CasFS,
    bucket_name: &str,
    key: &str,
    data: AsyncByteStream,
) -> io::Result<(Vec<BlockId>, ContentHash, u64)> {
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
    .zip(stream::repeat(tx))
    .enumerate()
    .for_each(|(idx, (maybe_chunk, mut tx))| async move {
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
        // Block addresses come from the store's own hasher, which is
        // fixed at store creation and read back from the header. MD5
        // below is the object ETag and a different thing entirely.
        let block_hash = fs.shared.hasher().hash(&bytes);

        // Stripe first (hard rule 6: fjall is the leaf lock). The owned
        // guard moves INTO the blocking closure, so cancelling this future
        // past this point abandons a closure that still runs to completion
        // and releases the stripe itself.
        let stripe_guard = fs.shared.stripes().for_hash(&block_hash).lock_owned().await;

        let shared = fs.shared.clone();
        let metrics = fs.metrics.clone();
        let joined = tokio::task::spawn_blocking(move || {
            write_one_block(shared, metrics, stripe_guard, block_hash, &bytes)
        })
        .await;

        let result = match joined {
            Ok(res) => res,
            // The closure panicked; the guard was dropped with it, and
            // BlockWriteGuard's Drop already counted the block as dropped.
            Err(join_err) => Err(io::Error::other(format!(
                "block write task did not complete: {join_err}"
            ))),
        };

        match result {
            Ok(()) => {
                if let Err(e) = tx.unbounded_send(Ok((idx, block_hash))) {
                    tracing::error!(error = %e, "Could not send block id");
                }
            }
            Err(e) => {
                if let Err(e) = tx.unbounded_send(Err(e)) {
                    tracing::error!(error = %e, "Could not send block write error");
                }
            }
        }
    })
    .await;

    let mut ids = rx.try_collect::<Vec<(usize, BlockId)>>().await?;
    // Make sure the chunks are in the proper order
    ids.sort_by_key(|a| a.0);

    let blocks: Vec<BlockId> = ids.into_iter().map(|(_, id)| id).collect();

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
