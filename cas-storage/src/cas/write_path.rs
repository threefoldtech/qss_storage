use std::io;
use std::sync::Arc;

use super::buffered_byte_stream::BufferedByteStream;
use super::byte_stream::AsyncByteStream;
use super::delete_path::release_blocks;
use super::fs::CasFS;
use super::shared_block_store::SharedBlockStore;
use crate::metastore::{BlockId, ContentHash, MetaError, MetaStore, Object, ObjectData};
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
        // In-flight gauge: started at submission, finished as the closure's
        // last act -- the closure always runs to completion, so the pair
        // balances even when this future is cancelled at the await below.
        fs.metrics.block_disk_op_started();
        let joined = tokio::task::spawn_blocking(move || {
            let result = write_one_block(shared, metrics.clone(), stripe_guard, block_hash, &bytes);
            metrics.block_disk_op_finished();
            result
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
        .await
        .unwrap();
    Ok(obj)
}

/// The replace transaction, on its own and synchronous.
///
/// Separate from [`create_object_meta`] on purpose: no `Transaction` may be
/// live across an await (ADR 0006 hard rule 3, the `Send`-soundness rule on
/// `FjallTransaction`), and no task may hold the fjall guard while it
/// acquires a stripe (hard rule 6, fjall is the leaf lock). Keeping the
/// whole transaction inside a non-async function makes both true by
/// construction rather than by inspection of an async frame.
fn replace_object_record(
    namespace: &MetaStore,
    bucket_name: &str,
    key: &str,
    raw_obj: Vec<u8>,
) -> Result<Option<Object>, MetaError> {
    let mut tx = namespace.begin_transaction();
    match tx.replace_object(bucket_name, key, raw_obj) {
        Ok(displaced) => {
            tx.commit()?;
            Ok(displaced)
        }
        Err(e) => {
            tx.rollback();
            Err(e)
        }
    }
}

/// Write an object record, releasing whatever object it replaced (ADR
/// 0008).
///
/// Every write of an object record goes through here: the PUT path, the
/// inline path, and `CompleteMultipartUpload`. If the key was occupied, the
/// displaced record's block references are dropped -- one per occurrence,
/// through the same [`release_blocks`] primitive `delete_object` and
/// multipart abort use, because an overwrite ends an object's life exactly
/// as a DELETE does.
///
/// # New record first, release second
///
/// The replace commits before a single reference is dropped. A crash in
/// between leaves the displaced blocks over-counted: leakage, INFO, and the
/// next recount collects it (ADR 0005). The reverse order would drop
/// references while the OLD record is still the visible one, so a reader
/// resolving that record races an unlink against nothing -- loss. Same
/// argument, same direction, as ADR 0003's abort loop.
///
/// # The reader race is the delete race, unchanged
///
/// A reader that resolved its block list from the record this call
/// displaces can have a block unlinked mid-read. That is EXACTLY the
/// delete-versus-reader race, which exists already and is already accepted:
/// POSIX fd semantics keep an already-open stream alive through the unlink,
/// and an open-after-unlink fails loudly rather than serving wrong bytes.
/// The overwrite opens no new window -- it reaches the same release, by the
/// same primitive, one commit later.
///
/// # Dedup arithmetic
///
/// When old and new share a block, the new write already bumped it (every
/// dedup hit bumps, ADR 0006) and this release drops the old occurrence:
/// net unchanged. Per dropped block -1, per added block +1. Exactly the
/// truth, with no special case for the shared ones.
pub(super) async fn create_object_meta(
    fs: &CasFS,
    bucket_name: &str,
    key: &str,
    size: u64,
    hash: ContentHash,
    object_data: ObjectData,
) -> Result<Object, MetaError> {
    let obj_meta = Object::new(size, hash, object_data);
    let displaced = replace_object_record(&fs.namespace, bucket_name, key, obj_meta.to_vec())?;

    // Committed. Only now may the replaced object's references go.
    if let Some(old) = displaced {
        let blocks = old.blocks();
        if !blocks.is_empty() {
            tracing::debug!(
                bucket = %bucket_name,
                key = %key,
                blocks = blocks.len(),
                "Overwrite: releasing the replaced object's blocks"
            );
            release_blocks(&fs.shared, &fs.metrics, blocks).await;
        }
    }

    Ok(obj_meta)
}

/// The inline write: the object's bytes live in its own record, so it holds
/// no block references at all.
///
/// It still goes through [`create_object_meta`], and that matters most in
/// the case that looks like it should not need it -- an inline write
/// REPLACING a block-backed object. The new record names no blocks, so
/// without the release the replaced object's occurrences would have no
/// holder and no collector short of fsck. Inline over inline releases
/// nothing, because there was nothing to release.
pub(super) async fn store_inlined_object(
    fs: &CasFS,
    bucket_name: &str,
    key: &str,
    data: Vec<u8>,
) -> Result<Object, MetaError> {
    let content_hash = ContentHash(Md5::digest(&data).into());
    let size = data.len() as u64;
    create_object_meta(
        fs,
        bucket_name,
        key,
        size,
        content_hash,
        ObjectData::Inline { data },
    )
    .await
}
