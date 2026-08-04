//! The PUT side of the block protocol: ADR 0006's file-first ordering, at
//! ADR 0010's batch width.
//!
//! # One request, one durability unit
//!
//! Before ADR 0010 every block paid its own fsync and its own transaction:
//! two-plus flushes per MiB, the metadata half serialized behind fjall's
//! single-writer lock. The durability CONTRACT never bound there, though --
//! what a client can observe, and what the campaign verifies, is the
//! acknowledgement. So the boundary moved to the ack, and a request's blocks
//! become one group:
//!
//! ```text
//! chunk -> hash -> dedup lookup -> [temp write]  (per block, as it arrives)
//!
//!    at the cap, and again at the request's end:
//!      fdatasync xN (concurrent, no stripes)
//!      -> stripes(sorted) -> rename xN -> dirsync (each dir once)
//!      -> ONE transaction: every insert and bump -> ONE persist
//!      -> release stripes -> ack
//! ```
//!
//! Nothing about the ORDERING moved. A block record still becomes readable
//! only after its file is durable at its final path (hard rule 5), the rc
//! arithmetic is still the same two transactional primitives (hard rule 2),
//! and stripes are still taken before fjall and never after (hard rule 6) --
//! there are simply N of them, taken in one sorted acquisition.
//!
//! # What a crash leaves
//!
//! A kill between the batch's directory sync and its commit leaves up to
//! `max_blocks_per_commit` block files with no record: residue class 1 (ADR
//! 0005), which fsck collects and which a later PUT of the same content
//! adopts in place. The batch changed the residue's SIZE, not its class,
//! because the file-first ordering is preserved wholesale.
//!
//! # What a mid-request crash means for a client
//!
//! Nothing new. A part whose upload died is a part the client never got an
//! ETag for, so it re-uploads it; the batches that did commit dedup-hit on
//! the retry, and the orphans of the batch that did not are healed in place
//! by the same rewrite. That is today's retry story, at batch width.

use std::io;

use super::buffered_byte_stream::BufferedByteStream;
use super::byte_stream::AsyncByteStream;
use super::delete_path::release_blocks;
use super::fs::CasFS;
use super::object_key;
use crate::metastore::{BlockId, ContentHash, MetaError, MetaStore, Object, ObjectData};
use futures::stream::StreamExt;
use md5::{Digest, Md5};

mod batch;
pub(crate) use batch::{
    BatchEntry, BlockWriteGuard, DEFAULT_MAX_BLOCKS_PER_COMMIT, FlushOutcome, Payload,
    abandon_after_landing, has_live_record, record_entry,
};
use batch::{BlockBatch, abandon, accumulate, flush_batch};

#[tracing::instrument(skip(fs, key, data), fields(bucket = %bucket_name, key = %object_key(key), size, blocks))]
pub(super) async fn store_object(
    fs: &CasFS,
    bucket_name: &str,
    key: &[u8],
    data: AsyncByteStream,
) -> io::Result<(Vec<BlockId>, ContentHash, u64)> {
    let mut content_hash = Md5::new();
    let mut size = 0u64;
    // Every occurrence, in chunk order: this is the object's block list, and
    // a repeated block appears once per occurrence in it.
    let mut blocks: Vec<BlockId> = Vec::new();
    let mut batch = BlockBatch::new(fs.shared.max_blocks_per_commit());

    let mut stream = BufferedByteStream::new(data);
    while let Some(item) = stream.next().await {
        let buffers = match item {
            Ok(buffers) => buffers,
            Err(e) => {
                abandon(&fs.shared, batch.take());
                return Err(io::Error::new(e.kind(), e.to_string()));
            }
        };

        for bytes in buffers {
            content_hash.update(&bytes);
            size += bytes.len() as u64;
            fs.metrics.bytes_received(bytes.len());

            // Block addresses come from the store's own hasher, which is
            // fixed at store creation and read back from the header. MD5
            // above is the object ETag and a different thing entirely.
            let block_hash = fs.shared.hasher().hash(&bytes);
            blocks.push(block_hash);

            if let Err(e) = accumulate(fs, &mut batch, block_hash, bytes).await {
                abandon(&fs.shared, batch.take());
                return Err(e);
            }

            // The cap closes a batch mid-request; the request's end closes
            // the last one, which is the batch the ack waits on.
            if batch.is_full() {
                flush_batch(fs, &mut batch).await?;
            }
        }
    }

    flush_batch(fs, &mut batch).await?;

    tracing::Span::current().record("size", size);
    tracing::Span::current().record("blocks", blocks.len());

    Ok((blocks, ContentHash(content_hash.finalize().into()), size))
}

pub(super) async fn store_single_object_and_meta(
    fs: &CasFS,
    bucket_name: &str,
    key: &[u8],
    data: AsyncByteStream,
    len: usize,
) -> io::Result<Object> {
    let (blocks, content_hash, size) = if len > 0 {
        store_object(fs, bucket_name, key, data).await?
    } else {
        tracing::warn!(key = %object_key(key), "Skipping store for empty blob");
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
///
/// The bucket's usage counter moves in the same transaction, by the
/// difference between what is written and what it displaced -- so the ledger
/// is exact over any interleaving of writers (the replace serializes them)
/// and cannot survive a rollback that the record did not.
fn replace_object_record(
    namespace: &MetaStore,
    bucket_name: &str,
    key: &[u8],
    raw_obj: Vec<u8>,
    size: u64,
) -> Result<Option<Object>, MetaError> {
    let mut tx = namespace.begin_transaction();
    let outcome = tx
        .replace_object(bucket_name, key, raw_obj)
        .and_then(|displaced| {
            let previous = displaced.as_ref().map_or(0, Object::size);
            let delta = size as i64 - previous as i64;
            tx.add_bucket_usage(bucket_name, delta)?;
            Ok(displaced)
        });

    match outcome {
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
    key: &[u8],
    size: u64,
    hash: ContentHash,
    object_data: ObjectData,
) -> Result<Object, MetaError> {
    let obj_meta = Object::new(size, hash, object_data);
    let displaced =
        replace_object_record(&fs.namespace, bucket_name, key, obj_meta.to_vec(), size)?;

    // Committed. Only now may the replaced object's references go.
    if let Some(old) = displaced {
        let blocks = old.blocks();
        if !blocks.is_empty() {
            tracing::debug!(
                bucket = %bucket_name,
                key = %object_key(key),
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
    key: &[u8],
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

#[cfg(test)]
mod tests;
