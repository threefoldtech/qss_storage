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

use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use super::block_disk::StagedBlock;
use super::buffered_byte_stream::BufferedByteStream;
use super::byte_stream::AsyncByteStream;
use super::delete_path::release_blocks;
use super::fs::CasFS;
use super::shared_block_store::SharedBlockStore;
use crate::metastore::{
    BlockId, ContentHash, MetaError, MetaStore, Object, ObjectData, Transaction,
};
use crate::metrics::SharedMetrics;
use futures::stream::StreamExt;
use md5::{Digest, Md5};

/// Block records one transaction carries when nothing configures a cap (ADR
/// 0010): 64 blocks, so 64 MiB at the 1 MiB block size.
///
/// It bounds three things at once -- transaction size, stripe hold time, and
/// the orphan count a single kill can leave -- and none of them is a format,
/// so it is a config (`store.max_blocks_per_commit`) and not a constant to
/// argue about. `config::DEFAULT_MAX_BLOCKS_PER_COMMIT` re-exports this
/// rather than repeating the number.
pub(crate) const DEFAULT_MAX_BLOCKS_PER_COMMIT: usize = 64;

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

/// What one distinct block of a batch still needs doing to it.
enum Payload {
    /// Its bytes are in a temp file already: the batch syncs, renames and
    /// records it.
    Staged(StagedBlock),
    /// A committed, healthy record existed when the chunk arrived, so no
    /// file was written -- that is dedup earning its keep, and the reason a
    /// re-upload of existing content costs no disk writes.
    ///
    /// The bytes are held until the batch commits, and only until then,
    /// because that lookup can still be overtaken: a concurrent DELETE may
    /// take the record's last reference before this batch reaches its
    /// transaction, and then the block has to be written after all. Rare,
    /// but it is the difference between a correct store and one that commits
    /// a record whose file was just unlinked. The batch cap is what bounds
    /// the memory this costs.
    Deduped(Vec<u8>),
}

/// One distinct block id in the accumulator, with every reference this
/// request adds to it.
struct BatchEntry {
    id: BlockId,
    /// Length of the block, for the record.
    len: usize,
    /// References this request adds: one per occurrence in the object. A
    /// block naming itself twice in one part holds two references, exactly
    /// as it would across two requests.
    occurrences: usize,
    payload: Payload,
    /// Fanout depth, once a file for the block has been placed by THIS
    /// request. `None` while the block is a dedup hit whose file is already
    /// on disk under some other depth, which the live record names.
    depth: Option<u8>,
    /// Resolved when the entry is accounted for, or on the way out.
    guard: Option<BlockWriteGuard>,
}

/// The blocks of the request that have not been committed yet (ADR 0010).
///
/// Dedup happens against committed state AND against this: a block that
/// appears twice in one request is one insert plus one bump, which is what
/// it would be across two requests. That is the whole reason the accumulator
/// is keyed by block id instead of being a list.
struct BlockBatch {
    entries: Vec<BatchEntry>,
    /// Block id to its slot in `entries`; the accumulator's own dedup index.
    seen: HashMap<BlockId, usize>,
    cap: usize,
}

impl BlockBatch {
    fn new(cap: usize) -> Self {
        Self {
            entries: Vec::new(),
            seen: HashMap::new(),
            cap: cap.max(1),
        }
    }

    /// Counts another occurrence of a block the batch already holds, and
    /// says whether it did. `false` means the caller must resolve the block
    /// against committed state and add it.
    fn add_occurrence(&mut self, id: BlockId) -> bool {
        match self.seen.get(&id) {
            Some(&slot) => {
                self.entries[slot].occurrences += 1;
                true
            }
            None => false,
        }
    }

    fn push(&mut self, entry: BatchEntry) {
        self.seen.insert(entry.id, self.entries.len());
        self.entries.push(entry);
    }

    fn is_full(&self) -> bool {
        self.entries.len() >= self.cap
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Empties the accumulator, handing over what it held.
    fn take(&mut self) -> Vec<BatchEntry> {
        self.seen.clear();
        std::mem::take(&mut self.entries)
    }
}

/// Abandons everything a batch staged: temp files unlinked, guards resolved
/// as failures.
///
/// Called on any error before the commit. Nothing has been renamed yet at
/// that point, so there is no half-applied state to compensate -- this is
/// housekeeping, and the store-open temp purge is its backstop.
fn abandon(shared: &SharedBlockStore, entries: Vec<BatchEntry>) {
    let ops = shared.disk_ops();
    for mut entry in entries {
        if let Payload::Staged(staged) = &entry.payload {
            shared.disk_writer().discard_staged(&*ops, staged);
        }
        if let Some(guard) = entry.guard.take() {
            guard.failed();
        }
    }
}

/// What the blocking half of a flush did, so the async half can move the
/// metrics without holding anything across the commit.
#[derive(Default)]
struct FlushOutcome {
    /// Blocks whose file this batch put on disk.
    written: usize,
    /// Blocks that deduped against a record already there.
    ignored: usize,
}

/// Resolves one arriving chunk into the batch: an occurrence of something it
/// already holds, a dedup hit against committed state, or a staged file.
///
/// The dedup lookup and the staging share one blocking closure, because both
/// touch disk -- the lookup reads the block tree, and `choose_depth` stats
/// the id's directory chain -- and neither belongs on an executor thread.
async fn accumulate(
    fs: &CasFS,
    batch: &mut BlockBatch,
    id: BlockId,
    bytes: Vec<u8>,
) -> io::Result<()> {
    if batch.add_occurrence(id) {
        return Ok(());
    }

    let len = bytes.len();
    let shared = fs.shared.clone();
    let metrics = fs.metrics.clone();
    fs.metrics.block_disk_op_started();
    let joined = tokio::task::spawn_blocking(move || {
        let outcome = resolve_chunk(&shared, id, bytes);
        metrics.block_disk_op_finished();
        outcome
    })
    .await;

    let payload = match joined {
        Ok(Ok(payload)) => payload,
        Ok(Err(e)) => return Err(e),
        Err(join_err) => {
            return Err(io::Error::other(format!(
                "block stage task did not complete: {join_err}"
            )));
        }
    };

    let entry = match payload {
        Payload::Staged(staged) => BatchEntry {
            id,
            len,
            occurrences: 1,
            depth: Some(staged.depth),
            // Pending from the moment its bytes hit the disk until the batch
            // that owns it commits or fails.
            guard: Some(BlockWriteGuard::new_pending(fs.metrics.clone())),
            payload: Payload::Staged(staged),
        },
        deduped @ Payload::Deduped(_) => BatchEntry {
            id,
            len,
            occurrences: 1,
            depth: None,
            // No disk write to track: a dedup hit was never Pending.
            guard: None,
            payload: deduped,
        },
    };
    batch.push(entry);
    Ok(())
}

/// The dedup lookup plus, if it misses, the temp write. Runs on a blocking
/// thread.
///
/// The lookup is a plain point read, not a transaction: it decides only
/// whether to spend a 1 MiB write, and the authoritative insert-vs-bump
/// decision is made later inside the batch's transaction, under the block's
/// stripe, against whatever state is current then. A degraded record (ADR
/// 0005) reads as absent, exactly as `bump_block_rc` treats it, so the write
/// that heals it still happens.
fn resolve_chunk(
    shared: &Arc<SharedBlockStore>,
    id: BlockId,
    bytes: Vec<u8>,
) -> io::Result<Payload> {
    if has_live_record(shared, &id)? {
        return Ok(Payload::Deduped(bytes));
    }

    // Depth: probe the id's dir chain for an orphan to heal in place, else
    // the placement policy. Only the new-block path pays for this.
    let depth = shared.placement().choose_depth(&id);
    let staged = shared
        .disk_writer()
        .stage_block(&*shared.disk_ops(), &id, depth, &bytes)?;
    Ok(Payload::Staged(staged))
}

/// Whether a committed, usable record exists for `id`.
///
/// Degraded records (ADR 0005) count as absent: their file is gone, so
/// deduplicating against one would commit another damaged object.
fn has_live_record(shared: &SharedBlockStore, id: &BlockId) -> io::Result<bool> {
    let record = shared
        .block_tree()
        .get_block(id.as_slice())
        .map_err(|e| io::Error::other(format!("reading the block record: {e}")))?;
    Ok(record.is_some_and(|block| !block.is_degraded()))
}

/// Closes a batch: data durable, then records committed, then the stripes go.
///
/// The three phases in the order the ADR names them, and the reason for the
/// order in one line each:
///
/// 1. **Sync, unstriped.** Temp files are per-attempt-unique and no reader
///    can name them, so the expensive part -- N concurrent fdatasyncs --
///    needs no serialization at all.
/// 2. **Stripes, sorted.** Every block the batch touches, in one acquisition
///    ordered by stripe, which is what rules ABBA out between two batches
///    that overlap.
/// 3. **Rename, dirsync, one transaction, one persist** -- all inside a
///    single blocking closure that OWNS the stripes. That is what makes the
///    stretch uncancellable (hard rule 4) and what keeps the fjall
///    transaction on one thread with no await inside it (hard rule 3).
async fn flush_batch(fs: &CasFS, batch: &mut BlockBatch) -> io::Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let entries = batch.take();

    let temp_paths: Vec<std::path::PathBuf> = entries
        .iter()
        .filter_map(|entry| match &entry.payload {
            Payload::Staged(staged) => Some(staged.temp_path().to_path_buf()),
            Payload::Deduped(_) => None,
        })
        .collect();

    if let Err(e) = fs
        .shared
        .disk_writer()
        .sync_batch(fs.shared.disk_ops(), temp_paths)
        .await
    {
        abandon(&fs.shared, entries);
        return Err(e);
    }

    let ids: Vec<BlockId> = entries.iter().map(|entry| entry.id).collect();
    let stripes = fs.shared.stripes().lock_batch(&ids).await;

    let shared = fs.shared.clone();
    let metrics = fs.metrics.clone();
    // In-flight gauge: started at submission, finished as the closure's last
    // act -- the closure always runs to completion, so the pair balances even
    // when this future is cancelled at the await below.
    fs.metrics.block_disk_op_started();
    let joined = tokio::task::spawn_blocking(move || {
        let outcome = commit_batch(&shared, &metrics, stripes, entries);
        metrics.block_disk_op_finished();
        outcome
    })
    .await;

    let outcome = match joined {
        Ok(result) => result?,
        // The closure panicked; every guard went with it, and
        // BlockWriteGuard's Drop counted its block as dropped.
        Err(join_err) => {
            return Err(io::Error::other(format!(
                "block batch task did not complete: {join_err}"
            )));
        }
    };

    for _ in 0..outcome.ignored {
        fs.metrics.block_ignored();
    }
    tracing::debug!(
        written = outcome.written,
        deduped = outcome.ignored,
        "Batch committed"
    );
    Ok(())
}

/// The blocking half of a flush: the stripes are held, the files land, the
/// records commit, and only then does the guard drop.
///
/// `_stripes` is owned here for exactly that reason. Cancelling the request
/// past the point this closure was submitted abandons a closure that still
/// runs to completion -- so a batch is never half-applied because a client
/// hung up, and the stripes are released by the code that took them.
fn commit_batch(
    shared: &Arc<SharedBlockStore>,
    metrics: &SharedMetrics,
    _stripes: super::stripes::BatchStripeGuard,
    mut entries: Vec<BatchEntry>,
) -> io::Result<FlushOutcome> {
    let ops = shared.disk_ops();

    // Under the stripes the block tree is stable for these ids: only stripe
    // holders mutate block records, and this batch holds every stripe it
    // names. So one read per entry decides the file work, and the
    // transaction below re-derives the same answer through the rc primitives.
    let mut live = Vec::with_capacity(entries.len());
    for entry in &entries {
        live.push(has_live_record(shared, &entry.id)?);
    }

    // The rare overtake: a block that deduped against a record which has
    // since gone (a concurrent DELETE took its last reference, unlinking the
    // file with it). The bytes were kept for exactly this, so the block is
    // written the single-block way, right here, under its own stripe.
    for (entry, live) in entries.iter_mut().zip(live.iter().copied()) {
        if live {
            continue;
        }
        let Payload::Deduped(bytes) = &entry.payload else {
            continue;
        };
        tracing::debug!(
            block_hash = %entry.id.to_hex(),
            "The record this block deduped against is gone; writing it after all"
        );
        let depth = shared.placement().choose_depth(&entry.id);
        shared
            .disk_writer()
            .write_block(&*ops, &entry.id, depth, bytes)?;
        entry.depth = Some(depth);
        entry.guard = Some(BlockWriteGuard::new_pending(metrics.clone()));
    }

    // Staged files whose block turned out to be live are surplus: another
    // writer landed identical bytes first, and its record names its own
    // depth. Dropping the temp file is the whole compensation -- nothing was
    // renamed, so nothing is off-depth residue.
    let mut to_land = Vec::with_capacity(entries.len());
    for (entry, live) in entries.iter().zip(live.iter().copied()) {
        if let Payload::Staged(staged) = &entry.payload {
            if live {
                shared.disk_writer().discard_staged(&*ops, staged);
            } else {
                to_land.push(staged);
            }
        }
    }

    // Files durable at their final paths, directories synced -- before a
    // single record exists (hard rule 5, ADR 0006, unchanged at width N).
    shared.disk_writer().land_batch(&*ops, &to_land)?;

    let mut outcome = FlushOutcome::default();
    let mut tx = shared.meta_store().begin_transaction();
    for entry in &entries {
        match record_entry(&mut tx, entry) {
            Ok(inserted) => {
                if inserted {
                    outcome.written += 1;
                } else {
                    outcome.ignored += 1;
                }
            }
            Err(e) => {
                tx.rollback();
                // The renames already happened, so the residue is orphan
                // block files at their final paths: residue class 1, healed
                // in place by a retry and collected by fsck. Nothing is torn.
                abandon_after_landing(entries);
                return Err(e.into());
            }
        }
    }
    tracing::debug!(target: "cas_storage::locks", blocks = entries.len(), "Committing block batch");
    if let Err(e) = tx.commit() {
        abandon_after_landing(entries);
        return Err(e.into());
    }

    for mut entry in entries {
        if let Some(guard) = entry.guard.take() {
            guard.written(entry.len);
        }
    }
    Ok(outcome)
}

/// Resolves the guards of a batch whose records did not commit.
///
/// Separate from [`abandon`] because by this point the files have been
/// renamed into place: there is nothing left to unlink, only the metric to
/// tell the truth about.
fn abandon_after_landing(entries: Vec<BatchEntry>) {
    for mut entry in entries {
        if let Some(guard) = entry.guard.take() {
            guard.failed();
        }
    }
}

/// One entry's records, inside the batch's transaction. Returns whether the
/// block was inserted (as opposed to deduping onto an existing record).
///
/// The insert-vs-bump decision is made HERE, against current state, through
/// the same two primitives the single-block path used -- that is what keeps
/// ADR 0008's rc exactness a property of the primitives rather than of the
/// caller. Every occurrence past the first is a bump, because every
/// reference is a reference.
fn record_entry(tx: &mut Transaction, entry: &BatchEntry) -> Result<bool, MetaError> {
    let mut remaining = entry.occurrences;
    let inserted = if tx.bump_block_rc(entry.id)?.is_none() {
        let depth = entry.depth.expect(
            "a block with no live record has had a file placed by this batch, \
             so its depth is known",
        );
        tx.insert_new_block(entry.id, entry.len, depth)?;
        remaining -= 1;
        true
    } else {
        remaining -= 1;
        false
    };

    for _ in 0..remaining {
        // Reads its own uncommitted write, so the count is exact whether the
        // first occurrence inserted or bumped.
        tx.bump_block_rc(entry.id)?;
    }
    Ok(inserted)
}

#[tracing::instrument(skip(fs, data), fields(bucket = %bucket_name, key = %key, size, blocks))]
pub(super) async fn store_object(
    fs: &CasFS,
    bucket_name: &str,
    key: &str,
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
