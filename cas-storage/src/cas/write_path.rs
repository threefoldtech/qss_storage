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
pub(super) enum Payload {
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
pub(super) struct BatchEntry {
    pub(super) id: BlockId,
    /// Length of the block, for the record.
    pub(super) len: usize,
    /// References this request adds: one per occurrence in the object. A
    /// block naming itself twice in one part holds two references, exactly
    /// as it would across two requests.
    pub(super) occurrences: usize,
    pub(super) payload: Payload,
    /// Fanout depth, once a file for the block has been placed by THIS
    /// request. `None` while the block is a dedup hit whose file is already
    /// on disk under some other depth, which the live record names.
    pub(super) depth: Option<u8>,
    /// Resolved when the entry is accounted for, or on the way out.
    pub(super) guard: Option<BlockWriteGuard>,
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
pub(super) struct FlushOutcome {
    /// Blocks whose file this batch put on disk.
    pub(super) written: usize,
    /// Blocks that deduped against a record already there.
    pub(super) ignored: usize,
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
pub(super) fn has_live_record(shared: &SharedBlockStore, id: &BlockId) -> io::Result<bool> {
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
///
/// # The one branch ADR 0011 added
///
/// Stage 3 is either done here, by this request, exactly as ADR 0010 wrote
/// it -- or handed to the store's commit station, which does the same three
/// things for several requests at once and wakes each with its own outcome.
/// Stages 1 and 2 are identical either way, which is the promise ADR 0010
/// made when it shaped the batch API ("that ADR is additive and touches no
/// callers"). With no station configured, the code below this branch is the
/// 0010 close byte for byte.
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

    let outcome = match fs.shared.commit_station() {
        // Sealed: its data is durable and it is nobody's request any more
        // until the committer says so. Parking here is cancel-safe -- the
        // group commits whatever happens to this future, because the station
        // owns the batch now.
        Some(station) => station.close(entries, fs.metrics.clone()).await?,
        None => close_alone(fs, entries).await?,
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

/// The ADR 0010 close: this request's batch, closed by this request.
///
/// What `flush_batch` did in full before ADR 0011 split the last step out,
/// and what it still does whenever no commit station is configured. The
/// station's degrade path does NOT come back through here -- a degraded
/// member replays its transaction under the group's stripes, which it
/// already holds (see `group_commit::close_group`).
async fn close_alone(fs: &CasFS, entries: Vec<BatchEntry>) -> io::Result<FlushOutcome> {
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

    match joined {
        Ok(result) => result,
        // The closure panicked; every guard went with it, and
        // BlockWriteGuard's Drop counted its block as dropped.
        Err(join_err) => Err(io::Error::other(format!(
            "block batch task did not complete: {join_err}"
        ))),
    }
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
pub(super) fn abandon_after_landing(entries: Vec<BatchEntry>) {
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
///
/// # Why this is also the CROSS-MEMBER merge (ADR 0011)
///
/// Nothing here knows whose entry it is holding. Call it for member A's
/// entry for block X and then for member B's entry for the same X inside one
/// transaction, and the second call's `bump_block_rc` reads the first call's
/// uncommitted insert: one insert, one bump, rc exact. That is why the group
/// closer merges strangers by doing nothing special -- the merge is a
/// property of the primitives, which is the same reason it was already a
/// property within one request.
pub(super) fn record_entry(tx: &mut Transaction, entry: &BatchEntry) -> Result<bool, MetaError> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::StorageEngine;
    use crate::cas::block_disk::{BlockDiskOps, RealDiskOps};
    use crate::cas::fs::BLOCK_SIZE;
    use crate::metastore::{BlockTree, Durability, block_disk_path};
    use bytes::Bytes;
    use std::path::{Path, PathBuf};
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::tempdir;

    const BUCKET: &str = "b";

    /// One store with one namespace at a chosen batch cap.
    fn store_with_cap(
        dir: &Path,
        cap: Option<usize>,
        ops: Option<Arc<dyn BlockDiskOps>>,
    ) -> (Arc<SharedBlockStore>, CasFS) {
        store_with_cap_and_station(dir, cap, ops, None)
    }

    /// The same, with a commit station (ADR 0011) if one is asked for.
    fn store_with_cap_and_station(
        dir: &Path,
        cap: Option<usize>,
        ops: Option<Arc<dyn BlockDiskOps>>,
        group_commit: Option<crate::cas::GroupCommit>,
    ) -> (Arc<SharedBlockStore>, CasFS) {
        let mut shared = SharedBlockStore::new(
            dir.join("meta/blocks"),
            dir.join("blocks"),
            StorageEngine::Fjall,
            Some(1),
            Some(Durability::Buffer),
            None,
            None,
            cap,
            group_commit,
        )
        .unwrap();
        if let Some(ops) = ops {
            shared.set_disk_ops(ops);
        }
        let shared = Arc::new(shared);
        let fs = CasFS::new(
            dir.join("meta/ns"),
            shared.clone(),
            SharedMetrics::default(),
            StorageEngine::Fjall,
            Some(1),
            Some(Durability::Buffer),
            false,
        )
        .unwrap();
        fs.create_bucket(BUCKET).unwrap();
        (shared, fs)
    }

    /// `n` blocks' worth of content, every block distinct.
    fn distinct_blocks(tag: &str, n: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(n * BLOCK_SIZE);
        for block in 0..n {
            let filler = format!("{tag}-block-{block}-");
            let mut one = filler.repeat(BLOCK_SIZE / filler.len() + 1);
            one.truncate(BLOCK_SIZE);
            out.extend_from_slice(one.as_bytes());
        }
        out
    }

    async fn put(fs: &CasFS, key: &str, data: Vec<u8>) -> Object {
        let len = data.len();
        let stream =
            AsyncByteStream::new(futures::stream::once(async move { Ok(Bytes::from(data)) }));
        fs.store_single_object_and_meta(BUCKET, key, stream, len)
            .await
            .unwrap()
    }

    /// Reads the block record count at each rename, so a test can see exactly
    /// when records became visible relative to the files landing.
    #[derive(Debug)]
    struct CommitObservingOps {
        real: RealDiskOps,
        /// Set once the store exists; `rename` reads through it.
        tree: StdMutex<Option<Arc<BlockTree>>>,
        /// Record count observed at each rename, in call order.
        at_rename: StdMutex<Vec<usize>>,
        writes: AtomicUsize,
    }

    impl CommitObservingOps {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                real: RealDiskOps,
                tree: StdMutex::new(None),
                at_rename: StdMutex::new(Vec::new()),
                writes: AtomicUsize::new(0),
            })
        }

        fn watch(&self, tree: Arc<BlockTree>) {
            *self.tree.lock().unwrap() = Some(tree);
        }

        fn observations(&self) -> Vec<usize> {
            self.at_rename.lock().unwrap().clone()
        }
    }

    impl BlockDiskOps for CommitObservingOps {
        fn create_dir_all(&self, path: &Path) -> io::Result<()> {
            self.real.create_dir_all(path)
        }
        fn write_new_file(&self, path: &Path, contents: &[u8]) -> io::Result<()> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            self.real.write_new_file(path, contents)
        }
        fn fsync_file(&self, path: &Path, data_only: bool) -> io::Result<()> {
            self.real.fsync_file(path, data_only)
        }
        fn fsync_dir(&self, path: &Path) -> io::Result<()> {
            self.real.fsync_dir(path)
        }
        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            let observed = self
                .tree
                .lock()
                .unwrap()
                .as_ref()
                .map(|tree| tree.len().unwrap());
            if let Some(count) = observed {
                self.at_rename.lock().unwrap().push(count);
            }
            self.real.rename(from, to)
        }
        fn remove_file(&self, path: &Path) -> io::Result<()> {
            self.real.remove_file(path)
        }
        fn list_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
            self.real.list_dir(path)
        }
        fn device_of(&self, path: &Path) -> io::Result<Option<u64>> {
            self.real.device_of(path)
        }
    }

    /// Records appear in batch-sized groups, and never before their files.
    ///
    /// This is the ADR's claim made observable. An 8-block object at a cap of
    /// 4 lands as two batches, and at every rename the tree holds a multiple
    /// of the cap: 0 for all four renames of the first batch (its records do
    /// not exist yet), 4 for all four of the second. A per-block protocol
    /// would count 0,1,2,3,4,5,6,7 instead, and a batch that committed before
    /// renaming would count 4,4,4,4,8,8,8,8.
    #[tokio::test]
    async fn records_commit_once_per_batch_and_never_before_the_files_land() {
        const CAP: usize = 4;
        const BLOCKS: usize = 8;

        let dir = tempdir().unwrap();
        let ops = CommitObservingOps::new();
        let (shared, fs) = store_with_cap(dir.path(), Some(CAP), Some(ops.clone()));
        ops.watch(shared.block_tree());

        put(&fs, "big", distinct_blocks("batched", BLOCKS)).await;

        assert_eq!(
            ops.observations(),
            vec![0, 0, 0, 0, 4, 4, 4, 4],
            "records must become visible one batch at a time, after the renames"
        );
        assert_eq!(shared.block_tree().len().unwrap(), BLOCKS);
    }

    /// The cap does not change what the store ends up holding: a request
    /// split into many batches, into one batch, or into one block per batch
    /// all leave the same object with the same blocks.
    #[tokio::test]
    async fn the_cap_changes_the_batching_and_nothing_else() {
        let content = distinct_blocks("cap-invariance", 5);

        let mut ids_per_cap = Vec::new();
        for cap in [1usize, 2, 5, 64] {
            let dir = tempdir().unwrap();
            let (shared, fs) = store_with_cap(dir.path(), Some(cap), None);
            let obj = put(&fs, "same", content.clone()).await;

            assert_eq!(obj.blocks().len(), 5, "cap {cap}");
            assert_eq!(shared.block_tree().len().unwrap(), 5, "cap {cap}");
            for id in obj.blocks() {
                let block = shared
                    .block_tree()
                    .get_block(id.as_slice())
                    .unwrap()
                    .unwrap_or_else(|| panic!("cap {cap}: every block must be recorded"));
                assert_eq!(block.rc(), 1, "cap {cap}");
                let path = block.disk_path(id, fs.fs_root().clone());
                assert_eq!(
                    shared.hasher().hash(&std::fs::read(&path).unwrap()),
                    *id,
                    "cap {cap}: the file is the block it is named after"
                );
            }
            assert_eq!(
                std::fs::read_dir(fs.fs_root().join(".tmp"))
                    .unwrap()
                    .count(),
                0,
                "cap {cap}: no temp residue"
            );
            ids_per_cap.push(obj.blocks().to_vec());
        }

        for ids in &ids_per_cap {
            assert_eq!(
                ids, &ids_per_cap[0],
                "the block list cannot depend on the cap"
            );
        }
    }

    /// A block appearing twice in ONE request is one insert plus one bump --
    /// the accumulator dedups against itself, exactly as two requests dedup
    /// against committed state.
    #[tokio::test]
    async fn a_block_repeated_in_one_request_is_one_file_and_two_references() {
        let dir = tempdir().unwrap();
        let ops = CommitObservingOps::new();
        let (shared, fs) = store_with_cap(dir.path(), None, Some(ops.clone()));

        // Two identical blocks around one distinct one: the repeat is not
        // adjacent, so a "same as the last block" check would miss it.
        let repeated = distinct_blocks("repeated", 1);
        let other = distinct_blocks("other", 1);
        let mut content = repeated.clone();
        content.extend_from_slice(&other);
        content.extend_from_slice(&repeated);

        let obj = put(&fs, "twice", content).await;

        assert_eq!(obj.blocks().len(), 3, "three occurrences in the block list");
        assert_eq!(obj.blocks()[0], obj.blocks()[2], "the first and last agree");
        assert_eq!(
            ops.writes.load(Ordering::SeqCst),
            2,
            "one file per distinct block, however often it occurs"
        );

        let repeated_id = obj.blocks()[0];
        assert_eq!(
            shared
                .block_tree()
                .get_block(repeated_id.as_slice())
                .unwrap()
                .unwrap()
                .rc(),
            2,
            "every occurrence holds its own reference"
        );
        assert_eq!(
            shared
                .block_tree()
                .get_block(obj.blocks()[1].as_slice())
                .unwrap()
                .unwrap()
                .rc(),
            1
        );

        // And the lifecycle closes exactly: deleting the object drops both.
        fs.delete_object(BUCKET, "twice").await.unwrap();
        assert!(
            shared
                .block_tree()
                .get_block(repeated_id.as_slice())
                .unwrap()
                .is_none(),
            "two references in, two references out"
        );
    }

    /// The one-block PUT is a one-block batch: today's shape, unchanged.
    #[tokio::test]
    async fn a_single_block_put_is_a_one_block_batch() {
        let dir = tempdir().unwrap();
        let ops = CommitObservingOps::new();
        let (shared, fs) = store_with_cap(dir.path(), None, Some(ops.clone()));
        ops.watch(shared.block_tree());

        let obj = put(&fs, "small", b"one small block".repeat(64).to_vec()).await;

        assert_eq!(obj.blocks().len(), 1);
        assert_eq!(
            ops.observations(),
            vec![0],
            "one rename, and no record existed when it happened"
        );
        assert_eq!(ops.writes.load(Ordering::SeqCst), 1);
    }

    /// Two concurrent batches both containing block X, both new (ADR 0010's
    /// "who wins?").
    ///
    /// Both stage their own temp file -- neither saw the other's record,
    /// because neither had committed one. The stripe serializes them at the
    /// transaction: the first insert wins, the second's in-tx decision sees
    /// the committed record and becomes a bump, and its surplus temp file is
    /// dropped rather than renamed over a live block. At quiesce: one insert,
    /// one bump, rc exactly 2, one file, no residue.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_concurrent_batches_over_one_new_block_insert_once_and_bump_once() {
        const ROUNDS: usize = 30;
        const CAP: usize = 4;

        let dir = tempdir().unwrap();
        let (shared, fs) = store_with_cap(dir.path(), Some(CAP), None);
        let fs = Arc::new(fs);

        for round in 0..ROUNDS {
            // The shared block sits among distinct ones, so each request is a
            // real batch and the shared block is not its only member.
            let shared_block = distinct_blocks(&format!("shared-{round}"), 1);
            let mut left = distinct_blocks(&format!("left-{round}"), 2);
            left.extend_from_slice(&shared_block);
            let mut right = distinct_blocks(&format!("right-{round}"), 2);
            right.extend_from_slice(&shared_block);

            let id = shared.hasher().hash(&shared_block);

            let a = {
                let fs = fs.clone();
                tokio::spawn(async move { put(&fs, &format!("a-{round}"), left).await })
            };
            let b = {
                let fs = fs.clone();
                tokio::spawn(async move { put(&fs, &format!("b-{round}"), right).await })
            };
            a.await.unwrap();
            b.await.unwrap();

            let block = shared
                .block_tree()
                .get_block(id.as_slice())
                .unwrap()
                .expect("the contended block must have exactly one record");
            assert_eq!(block.rc(), 2, "round {round}: one insert plus one bump");

            // Exactly one file, at the depth the record names, with the right
            // bytes -- and no second copy at any other depth.
            let path = block.disk_path(&id, fs.fs_root().clone());
            assert_eq!(
                shared.hasher().hash(&std::fs::read(&path).unwrap()),
                id,
                "round {round}: the surviving file is the block"
            );
            for depth in 1..=4u8 {
                if depth != block.depth() {
                    assert!(
                        !block_disk_path(&id, depth, fs.fs_root().clone()).exists(),
                        "round {round}: the loser must not leave an off-depth copy"
                    );
                }
            }
            assert_eq!(
                std::fs::read_dir(fs.fs_root().join(".tmp"))
                    .unwrap()
                    .count(),
                0,
                "round {round}: the surplus temp file must be removed"
            );
        }
    }

    /// A request whose dedup lookup is overtaken: the record it deduped
    /// against is deleted before its batch commits.
    ///
    /// That lookup is the one thing in a batch that can go stale, and this is
    /// the shape that makes it go stale on purpose. The batch has to notice
    /// under the stripe and write the block after all, rather than committing
    /// a record whose file was just unlinked. Run as a storm because the
    /// window is small; a wrong implementation shows up as a record with no
    /// file, which the read-back catches.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_dedup_hit_overtaken_by_a_delete_still_lands_its_bytes() {
        const ROUNDS: usize = 40;

        let dir = tempdir().unwrap();
        let (shared, fs) = store_with_cap(dir.path(), None, None);
        let fs = Arc::new(fs);

        for round in 0..ROUNDS {
            let content = distinct_blocks(&format!("overtaken-{round}"), 1);
            let id = shared.hasher().hash(&content);

            // The reference the racing DELETE will take: the last one, so the
            // record and the file both go.
            put(&fs, &format!("seed-{round}"), content.clone()).await;

            let deleter = {
                let fs = fs.clone();
                tokio::spawn(
                    async move { fs.delete_object(BUCKET, &format!("seed-{round}")).await },
                )
            };
            let writer = {
                let fs = fs.clone();
                let content = content.clone();
                tokio::spawn(async move { put(&fs, &format!("writer-{round}"), content).await })
            };
            deleter.await.unwrap().unwrap();
            writer.await.unwrap();

            // Whichever order they landed in, the writer's object is
            // readable: its record exists and its file is there, whole.
            let block = shared
                .block_tree()
                .get_block(id.as_slice())
                .unwrap()
                .unwrap_or_else(|| panic!("round {round}: the writer's block must be recorded"));
            let path = block.disk_path(&id, fs.fs_root().clone());
            let bytes = std::fs::read(&path).unwrap_or_else(|e| {
                panic!("round {round}: a committed record must have its file: {e}")
            });
            assert_eq!(
                shared.hasher().hash(&bytes),
                id,
                "round {round}: complete file, never partial"
            );

            fs.delete_object(BUCKET, &format!("writer-{round}"))
                .await
                .unwrap();
        }
    }

    /// Ops whose exclusive-create write signals entry and waits for a
    /// release, so a test can park a request between its dedup lookup and its
    /// batch commit -- the one window in which that lookup can go stale.
    #[derive(Debug)]
    struct GatedStageOps {
        real: RealDiskOps,
        entered: std::sync::mpsc::Sender<()>,
        release: StdMutex<std::sync::mpsc::Receiver<()>>,
    }

    impl BlockDiskOps for GatedStageOps {
        fn create_dir_all(&self, path: &Path) -> io::Result<()> {
            self.real.create_dir_all(path)
        }
        fn write_new_file(&self, path: &Path, contents: &[u8]) -> io::Result<()> {
            self.entered.send(()).ok();
            self.release.lock().unwrap().recv().ok();
            self.real.write_new_file(path, contents)
        }
        fn fsync_file(&self, path: &Path, data_only: bool) -> io::Result<()> {
            self.real.fsync_file(path, data_only)
        }
        fn fsync_dir(&self, path: &Path) -> io::Result<()> {
            self.real.fsync_dir(path)
        }
        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            self.real.rename(from, to)
        }
        fn remove_file(&self, path: &Path) -> io::Result<()> {
            self.real.remove_file(path)
        }
        fn list_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
            self.real.list_dir(path)
        }
        fn device_of(&self, path: &Path) -> io::Result<Option<u64>> {
            self.real.device_of(path)
        }
    }

    /// The overtake, arranged rather than raced: the deleted-underneath case,
    /// deterministically.
    ///
    /// A request of two blocks, `[X, N]`. X dedup-hits a committed record, so
    /// no file is written for it and only its bytes are held. N is new, so it
    /// stages -- and the gate parks the request right there, holding no
    /// stripes, which is exactly what lets the DELETE through. The DELETE
    /// takes X's last reference: record removed, file unlinked. Then the batch
    /// resumes.
    ///
    /// What must happen: the batch notices under X's stripe that the record it
    /// deduped against is gone, writes X from the bytes it kept, and commits a
    /// record whose file is really there. What must NOT happen: a record for X
    /// with no file, which is silent data loss dressed as a successful PUT.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_dedup_hit_deleted_mid_batch_is_written_from_the_bytes_it_kept() {
        let dir = tempdir().unwrap();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let ops = Arc::new(GatedStageOps {
            real: RealDiskOps,
            entered: entered_tx,
            release: StdMutex::new(release_rx),
        });
        let (shared, fs) = store_with_cap(dir.path(), None, Some(ops));
        let fs = Arc::new(fs);

        let x = distinct_blocks("deduped-away", 1);
        let n = distinct_blocks("brand-new", 1);
        let x_id = shared.hasher().hash(&x);

        // Seed the record the request will dedup against, letting its own
        // single staged write through the gate.
        let seed = {
            let fs = fs.clone();
            let x = x.clone();
            tokio::spawn(async move { put(&fs, "seed", x).await })
        };
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("the seed must stage its block");
        release_tx.send(()).unwrap();
        seed.await.unwrap();
        assert_eq!(
            shared
                .block_tree()
                .get_block(x_id.as_slice())
                .unwrap()
                .unwrap()
                .rc(),
            1
        );

        // The request: X dedup-hits (no write), N stages and parks.
        let mut content = x.clone();
        content.extend_from_slice(&n);
        let writer = {
            let fs = fs.clone();
            tokio::spawn(async move { put(&fs, "writer", content).await })
        };
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("the request must reach the new block's stage");

        // Parked with no stripes held, so the DELETE goes through and takes
        // X's last reference with it.
        fs.delete_object(BUCKET, "seed").await.unwrap();
        assert!(
            shared
                .block_tree()
                .get_block(x_id.as_slice())
                .unwrap()
                .is_none(),
            "the premise: the record the request deduped against is gone"
        );

        // The batch resumes, and has to notice. Two tokens: one frees the
        // parked stage of N, the second is consumed by the write of X that
        // the batch is obliged to perform now. That second token being
        // NEEDED is itself the assertion -- without the fallback the request
        // would sail through on one, and commit a record for a block whose
        // file it never wrote.
        release_tx.send(()).unwrap();
        release_tx.send(()).unwrap();
        writer.await.unwrap();

        let block = shared
            .block_tree()
            .get_block(x_id.as_slice())
            .unwrap()
            .expect("the request's own reference must have recreated the record");
        assert_eq!(block.rc(), 1, "one holder: the request that survived");
        let path = block.disk_path(&x_id, fs.fs_root().clone());
        let bytes = std::fs::read(&path)
            .expect("a committed record must have its file -- this is the loss case");
        assert_eq!(
            shared.hasher().hash(&bytes),
            x_id,
            "and the file must be the block it is named after"
        );
        assert_eq!(
            std::fs::read_dir(fs.fs_root().join(".tmp"))
                .unwrap()
                .count(),
            0,
            "no temp residue"
        );
    }

    /// A failed request leaves no temp residue: everything it staged is
    /// unlinked on the way out, and nothing was renamed.
    #[tokio::test]
    async fn a_request_that_fails_mid_stream_discards_what_it_staged() {
        let dir = tempdir().unwrap();
        let (shared, fs) = store_with_cap(dir.path(), Some(64), None);

        // Enough blocks to fill a batch's worth of staging, then an error
        // before the request end that would have flushed it.
        let good = distinct_blocks("doomed", 3);
        let stream = AsyncByteStream::new(futures::stream::iter(vec![
            Ok(Bytes::from(good)),
            Err(io::Error::other("the client went away")),
        ]));

        let err = store_object(&fs, BUCKET, "never", stream)
            .await
            .expect_err("a stream error must fail the request");
        assert!(err.to_string().contains("the client went away"), "{err}");

        assert_eq!(
            shared.block_tree().len().unwrap(),
            0,
            "a request that never acked records nothing"
        );
        assert_eq!(
            std::fs::read_dir(fs.fs_root().join(".tmp"))
                .unwrap()
                .count(),
            0,
            "and leaves no temp residue behind"
        );
    }

    /// A few hundred MiB through the real write path at real `fsync`
    /// durability, one cap against another. Ignored by default.
    ///
    /// NOT the acceptance benchmark -- that is the 16 GiB A/B on the rig
    /// (`tests/real/tools/durability-bench.sh`), which owns the regression
    /// floor and the hardware it means anything on. This is the smoke check
    /// that says the batch path works end to end under real syncs and that a
    /// bigger cap does what it is for, in seconds rather than hours:
    ///
    /// ```text
    /// cargo test -p cas-storage --release -- --ignored --nocapture batch_smoke
    /// ```
    ///
    /// A cap of 1 is the pre-ADR-0010 cadence (one commit and one journal
    /// fsync per block), so the two rows are the change this ADR is about.
    ///
    /// The store goes under `target/`, deliberately, and NOT in `$TMPDIR`:
    /// `/tmp` is tmpfs on most Linux boxes, where fsync costs nothing and
    /// both rows come back identical and meaningless. A benchmark that
    /// silently measures a RAM disk is worse than no benchmark.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "writes a few hundred MiB with real fsyncs; run explicitly"]
    async fn batch_smoke_ab() {
        /// Blocks per object, so 256 MiB per row at the 1 MiB block size.
        const BLOCKS: usize = 256;

        let scratch = Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/batch-smoke");
        std::fs::create_dir_all(&scratch).unwrap();

        for cap in [1usize, 64] {
            let dir = tempfile::Builder::new()
                .prefix("ab-")
                .tempdir_in(&scratch)
                .unwrap();
            // Fsync, not Buffer: the whole point is to pay the real syncs.
            let shared = Arc::new(
                SharedBlockStore::new(
                    dir.path().join("meta/blocks"),
                    dir.path().join("blocks"),
                    StorageEngine::Fjall,
                    Some(1),
                    Some(Durability::Fsync),
                    None,
                    None,
                    Some(cap),
                    None,
                )
                .unwrap(),
            );
            let fs = CasFS::new(
                dir.path().join("meta/ns"),
                shared.clone(),
                SharedMetrics::default(),
                StorageEngine::Fjall,
                Some(1),
                Some(Durability::Fsync),
                false,
            )
            .unwrap();
            fs.create_bucket(BUCKET).unwrap();

            // Distinct content per row: no row may dedup against another's.
            let content = distinct_blocks(&format!("smoke-{cap}"), BLOCKS);
            let bytes = content.len();

            let started = std::time::Instant::now();
            let obj = put(&fs, "giant", content).await;
            let elapsed = started.elapsed();

            assert_eq!(obj.blocks().len(), BLOCKS);
            assert_eq!(shared.block_tree().len().unwrap(), BLOCKS);
            #[allow(clippy::cast_precision_loss)] // a printed rate, not a value
            let rate = (bytes as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64();
            println!(
                "max_blocks_per_commit={cap:>3}: {:>4} MiB in {:>6.2}s = {rate:>7.1} MiB/s",
                bytes / (1024 * 1024),
                elapsed.as_secs_f64(),
            );

            // Correctness first, speed second: every block readable and
            // exactly what it claims to be.
            for id in obj.blocks() {
                let block = shared
                    .block_tree()
                    .get_block(id.as_slice())
                    .unwrap()
                    .unwrap();
                let path = block.disk_path(id, fs.fs_root().clone());
                assert_eq!(shared.hasher().hash(&std::fs::read(&path).unwrap()), *id);
            }
        }
    }

    /// The batch is per REQUEST, not per object: a multipart part is its own
    /// durability unit, and two parts of one upload commit separately.
    #[tokio::test]
    async fn each_part_upload_is_its_own_batch() {
        let dir = tempdir().unwrap();
        let ops = CommitObservingOps::new();
        let (shared, fs) = store_with_cap(dir.path(), Some(64), Some(ops.clone()));
        ops.watch(shared.block_tree());

        for part in 0..2 {
            let data = distinct_blocks(&format!("part-{part}"), 2);
            let stream =
                AsyncByteStream::new(futures::stream::once(async move { Ok(Bytes::from(data)) }));
            store_object(&fs, BUCKET, "multi", stream).await.unwrap();
        }

        assert_eq!(
            ops.observations(),
            vec![0, 0, 2, 2],
            "each request commits its own blocks, as one group"
        );
    }

    /// The commit station (ADR 0011): strangers share a flush.
    ///
    /// Every test here turns the station on and drives it through the real
    /// write path -- `store_object` and `store_single_object_and_meta` -- so
    /// nothing is asserted about a function that production does not call.
    ///
    /// # How a group is forced
    ///
    /// Group formation is natural batching: whatever queued while the
    /// previous group was committing. That is by definition timing-dependent,
    /// so tests that need SEVERAL members in ONE group set a
    /// `group_commit_window` and rely on the timer -- the only deterministic
    /// grouping the design offers. The window is the thing under test in
    /// those cases anyway. Tests about a LONE request use window zero, which
    /// is the shipped default and the one an operator gets.
    mod station {
        use super::*;
        use crate::cas::GroupCommit;
        use std::sync::atomic::AtomicBool;
        use std::time::Duration;

        /// Long enough that every member of a test group has queued before
        /// the deadline fires, short enough not to slow the suite down.
        const WINDOW: Duration = Duration::from_millis(500);

        /// A store whose station gathers for [`WINDOW`] before committing.
        fn windowed_store(
            dir: &Path,
            cap: Option<usize>,
            ops: Option<Arc<dyn BlockDiskOps>>,
        ) -> (Arc<SharedBlockStore>, Arc<CasFS>) {
            let (shared, fs) =
                store_with_cap_and_station(dir, cap, ops, Some(GroupCommit { window: WINDOW }));
            (shared, Arc::new(fs))
        }

        async fn try_put(fs: &CasFS, key: &str, data: Vec<u8>) -> io::Result<Object> {
            let len = data.len();
            let stream =
                AsyncByteStream::new(futures::stream::once(async move { Ok(Bytes::from(data)) }));
            fs.store_single_object_and_meta(BUCKET, key, stream, len)
                .await
        }

        /// Everything a healthy store must NOT have on disk after a batch:
        /// no temp residue, and no copy of a block at a depth its record does
        /// not name.
        fn assert_no_residue(shared: &SharedBlockStore, fs: &CasFS, id: &BlockId) {
            let block = shared
                .block_tree()
                .get_block(id.as_slice())
                .unwrap()
                .expect("the block must have a record");
            for depth in 1..=4u8 {
                if depth != block.depth() {
                    assert!(
                        !block_disk_path(id, depth, fs.fs_root().clone()).exists(),
                        "a copy at depth {depth} is off-depth residue"
                    );
                }
            }
            assert_eq!(
                std::fs::read_dir(fs.fs_root().join(".tmp"))
                    .unwrap()
                    .count(),
                0,
                "no temp residue"
            );
        }

        /// A lone request pays nothing for a station that is idle.
        ///
        /// This is the ADR's first promise and the reason group commit can be
        /// default-safe: natural batching merges only what was ALREADY
        /// waiting behind an in-flight commit, so a request that finds the
        /// committer idle commits immediately. At `group_commit_window = 0`,
        /// the shipped default, there is no timer to wait on at all.
        ///
        /// The contrast is the assertion that gives the first half its
        /// meaning: the SAME lone PUT against a station with a one-second
        /// window really does wait. Without that row, "fast" would prove
        /// nothing about whether a timer exists.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn a_lone_request_does_not_wait_at_window_zero() {
            let content = distinct_blocks("lonely", 1);

            let quick = tempdir().unwrap();
            let (shared, fs) = store_with_cap_and_station(
                quick.path(),
                None,
                None,
                Some(GroupCommit {
                    window: Duration::ZERO,
                }),
            );
            let started = std::time::Instant::now();
            put(&fs, "alone", content.clone()).await;
            let idle = started.elapsed();

            assert!(
                idle < Duration::from_millis(500),
                "an idle committer must take a lone batch at once, took {idle:?}"
            );
            let stats = shared
                .group_commit_stats()
                .expect("the station must have run");
            assert_eq!(stats.groups, 1, "one group");
            assert_eq!(stats.members, 1, "of one member");
            assert_eq!(stats.mean_group_size(), Some(1.0));

            // The same request against a window, which is what makes the
            // measurement above mean something.
            let slow = tempdir().unwrap();
            let (_, fs) = store_with_cap_and_station(
                slow.path(),
                None,
                None,
                Some(GroupCommit {
                    window: Duration::from_secs(1),
                }),
            );
            let started = std::time::Instant::now();
            put(&fs, "alone", content).await;
            let waited = started.elapsed();
            assert!(
                waited >= Duration::from_millis(900),
                "a window is a real wait, or the row above proves nothing: {waited:?}"
            );
        }

        /// Two strangers in one group, both carrying the same new block X:
        /// one insert, one bump, rc exactly 2, one file, no residue.
        ///
        /// The ADR's cross-member question, pinned. Under ADR 0010 these two
        /// requests would serialize on X's stripe and the second's in-tx
        /// decision would see the first's COMMITTED record; here they are in
        /// one transaction, and the second's `bump_block_rc` has to read the
        /// first's UNCOMMITTED insert instead. Same answer, different
        /// mechanism -- which is exactly why it needs its own test.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn two_members_of_one_group_over_one_new_block_insert_once_and_bump_once() {
            const ROUNDS: usize = 10;

            let dir = tempdir().unwrap();
            let (shared, fs) = windowed_store(dir.path(), Some(64), None);

            for round in 0..ROUNDS {
                let shared_block = distinct_blocks(&format!("group-shared-{round}"), 1);
                let mut left = distinct_blocks(&format!("group-left-{round}"), 1);
                left.extend_from_slice(&shared_block);
                let mut right = distinct_blocks(&format!("group-right-{round}"), 1);
                right.extend_from_slice(&shared_block);
                let id = shared.hasher().hash(&shared_block);

                let a = {
                    let fs = fs.clone();
                    tokio::spawn(async move { put(&fs, &format!("a-{round}"), left).await })
                };
                let b = {
                    let fs = fs.clone();
                    tokio::spawn(async move { put(&fs, &format!("b-{round}"), right).await })
                };
                a.await.unwrap();
                b.await.unwrap();

                let block = shared
                    .block_tree()
                    .get_block(id.as_slice())
                    .unwrap()
                    .expect("round {round}: the shared block must have one record");
                assert_eq!(
                    block.rc(),
                    2,
                    "round {round}: one insert plus one bump, across two members"
                );
                let path = block.disk_path(&id, fs.fs_root().clone());
                assert_eq!(
                    shared.hasher().hash(&std::fs::read(&path).unwrap()),
                    id,
                    "round {round}: the surviving file is the block"
                );
                assert_no_residue(&shared, &fs, &id);
            }

            let stats = shared.group_commit_stats().unwrap();
            assert!(
                stats.largest >= 2,
                "the window must actually have merged strangers: {stats:?}"
            );
            assert_eq!(stats.degraded, 0, "nothing here should have failed");
        }

        /// One poisoned member fails; every stranger in its group acks.
        ///
        /// The poison is a block record that will not decode, planted while
        /// the group is gathering -- so the member reaches the committer
        /// healthy and dies inside the SHARED transaction, which is precisely
        /// the case the ADR's degrade path exists for. What must happen: the
        /// group transaction rolls back, every member is replayed on its own,
        /// and only the member that named the corrupt record fails.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn a_poisoned_member_fails_alone_and_its_group_acks() {
            const STRANGERS: usize = 3;

            let dir = tempdir().unwrap();
            let (shared, fs) = windowed_store(dir.path(), Some(64), None);

            let poisoned_content = distinct_blocks("poisoned", 1);
            let poisoned_id = shared.hasher().hash(&poisoned_content);

            let mut handles = Vec::new();
            handles.push({
                let fs = fs.clone();
                tokio::spawn(async move { try_put(&fs, "poisoned", poisoned_content).await })
            });
            for i in 0..STRANGERS {
                let fs = fs.clone();
                let content = distinct_blocks(&format!("innocent-{i}"), 1);
                handles.push(tokio::spawn(async move {
                    try_put(&fs, &format!("innocent-{i}"), content).await
                }));
            }

            // Every member has staged, synced and queued by now, and the
            // committer is waiting out its window. Garbage where the poisoned
            // member's block record belongs: its `bump_block_rc` cannot
            // decode it, which fails the transaction the whole group shares.
            tokio::time::sleep(WINDOW / 5).await;
            shared
                .meta_store()
                .get_tree(crate::metastore::DEFAULT_BLOCK_TREE)
                .unwrap()
                .insert(poisoned_id.as_slice(), vec![0xffu8; 3])
                .unwrap();

            let mut results = Vec::new();
            for handle in handles {
                results.push(handle.await.unwrap());
            }

            assert!(
                results[0].is_err(),
                "the member whose record will not decode must fail"
            );
            for (i, result) in results[1..].iter().enumerate() {
                assert!(
                    result.is_ok(),
                    "stranger {i} must ack anyway: {:?}",
                    result.as_ref().err()
                );
            }

            let stats = shared.group_commit_stats().unwrap();
            assert_eq!(
                stats.degraded, 1,
                "exactly one group degraded to per-member replay: {stats:?}"
            );

            // And the strangers' objects are really there, not merely acked.
            for i in 0..STRANGERS {
                let obj = fs
                    .get_object_meta(BUCKET, &format!("innocent-{i}"))
                    .unwrap()
                    .expect("an acked object must have its record");
                for id in obj.blocks() {
                    let block = shared
                        .block_tree()
                        .get_block(id.as_slice())
                        .unwrap()
                        .expect("and its blocks must be recorded");
                    assert_eq!(block.rc(), 1);
                    let path = block.disk_path(id, fs.fs_root().clone());
                    assert_eq!(shared.hasher().hash(&std::fs::read(&path).unwrap()), *id);
                }
            }
            // The poisoned member acked nothing.
            assert!(
                fs.get_object_meta(BUCKET, "poisoned").unwrap().is_none(),
                "a request that failed must not have written an object record"
            );
        }

        /// Renames everything for real, then dies once, on the Nth one.
        ///
        /// The kill fixture: a group whose files have all landed and whose
        /// transaction never ran. Arming it for exactly one rename lets the
        /// same store be used afterwards for the heal, which is the half of
        /// residue class 1 that matters.
        #[derive(Debug)]
        struct DieAfterRenamesOps {
            real: RealDiskOps,
            die_on: usize,
            seen: AtomicUsize,
            armed: AtomicBool,
        }

        impl DieAfterRenamesOps {
            fn arm(die_on: usize) -> Arc<Self> {
                Arc::new(Self {
                    real: RealDiskOps,
                    die_on,
                    seen: AtomicUsize::new(0),
                    armed: AtomicBool::new(true),
                })
            }
        }

        impl BlockDiskOps for DieAfterRenamesOps {
            fn create_dir_all(&self, path: &Path) -> io::Result<()> {
                self.real.create_dir_all(path)
            }
            fn write_new_file(&self, path: &Path, contents: &[u8]) -> io::Result<()> {
                self.real.write_new_file(path, contents)
            }
            fn fsync_file(&self, path: &Path, data_only: bool) -> io::Result<()> {
                self.real.fsync_file(path, data_only)
            }
            fn fsync_dir(&self, path: &Path) -> io::Result<()> {
                self.real.fsync_dir(path)
            }
            fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
                self.real.rename(from, to)?;
                let seen = self.seen.fetch_add(1, Ordering::SeqCst) + 1;
                assert!(
                    !(seen >= self.die_on && self.armed.swap(false, Ordering::SeqCst)),
                    "kill -9 between the group's last rename and its commit"
                );
                Ok(())
            }
            fn remove_file(&self, path: &Path) -> io::Result<()> {
                self.real.remove_file(path)
            }
            fn list_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
                self.real.list_dir(path)
            }
            fn device_of(&self, path: &Path) -> io::Result<Option<u64>> {
                self.real.device_of(path)
            }
        }

        /// A kill at group width leaves residue class 1 and nothing else, and
        /// a retry heals it in place.
        ///
        /// The ADR's crash question: "up to `max_blocks_per_commit` orphan
        /// block files -- the SAME bound and the SAME class as 0010, because
        /// the group is capped by the same knob. The difference is
        /// provenance, which fsck does not care about."
        ///
        /// So the fixture kills the committer between the group's last rename
        /// and its transaction, and the assertions are about the CLASS of
        /// what is left: files at their final paths with no records (class
        /// 1), no temp files, no off-depth copies (class 2), no record
        /// without a file (which would be loss, not residue), and a bound of
        /// one cap's worth.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn a_kill_at_group_width_leaves_class_one_residue_that_a_retry_heals() {
            const MEMBERS: usize = 3;
            const BLOCKS_EACH: usize = 2;
            const CAP: usize = 64;

            let dir = tempdir().unwrap();
            let ops = DieAfterRenamesOps::arm(MEMBERS * BLOCKS_EACH);
            let (shared, fs) = windowed_store(dir.path(), Some(CAP), Some(ops.clone()));

            let contents: Vec<Vec<u8>> = (0..MEMBERS)
                .map(|i| distinct_blocks(&format!("killed-{i}"), BLOCKS_EACH))
                .collect();
            let ids: Vec<Vec<BlockId>> = contents
                .iter()
                .map(|content| {
                    content
                        .chunks(BLOCK_SIZE)
                        .map(|chunk| shared.hasher().hash(chunk))
                        .collect()
                })
                .collect();

            let mut handles = Vec::new();
            for (i, content) in contents.iter().enumerate() {
                let fs = fs.clone();
                let content = content.clone();
                handles.push(tokio::spawn(async move {
                    try_put(&fs, &format!("killed-{i}"), content).await
                }));
            }
            for handle in handles {
                assert!(
                    handle.await.unwrap().is_err(),
                    "a group that died before its commit acks nobody"
                );
            }

            // Residue class 1: every file at its final path, not one record.
            let orphans: Vec<&BlockId> = ids.iter().flatten().collect();
            assert_eq!(orphans.len(), MEMBERS * BLOCKS_EACH);
            assert!(
                orphans.len() <= CAP,
                "the residue a single kill leaves is bounded by the cap"
            );
            assert_eq!(
                shared.block_tree().len().unwrap(),
                0,
                "not one record may exist: the transaction never ran"
            );
            let mut found = 0;
            for id in &orphans {
                let mut at_depth = 0;
                for depth in 1..=4u8 {
                    if block_disk_path(id, depth, fs.fs_root().clone()).exists() {
                        at_depth += 1;
                        found += 1;
                    }
                }
                assert!(
                    at_depth <= 1,
                    "an orphan must exist at ONE depth, never several (class 2 residue)"
                );
            }
            assert_eq!(
                found,
                orphans.len(),
                "every landed file is an orphan, and every orphan is a landed file"
            );
            assert_eq!(
                std::fs::read_dir(fs.fs_root().join(".tmp"))
                    .unwrap()
                    .count(),
                0,
                "no temp residue: the group renamed everything it staged"
            );

            // The heal: the same content, written again by a client that
            // retried. The orphans are adopted in place -- rename-over
            // installs identical bytes -- and the store comes out exact.
            for (i, content) in contents.iter().enumerate() {
                try_put(&fs, &format!("healed-{i}"), content.clone())
                    .await
                    .expect("the retry must succeed");
            }
            assert_eq!(
                shared.block_tree().len().unwrap(),
                MEMBERS * BLOCKS_EACH,
                "every orphan is now a recorded block"
            );
            for id in &orphans {
                let block = shared
                    .block_tree()
                    .get_block(id.as_slice())
                    .unwrap()
                    .expect("healed");
                assert_eq!(block.rc(), 1, "one holder: the retry");
                assert_no_residue(&shared, &fs, id);
                let path = block.disk_path(id, fs.fs_root().clone());
                assert_eq!(shared.hasher().hash(&std::fs::read(&path).unwrap()), **id);
            }
        }

        /// The workload the ADR is FOR: many small concurrent PUTs, all
        /// acked, all readable, refcounts exact.
        ///
        /// Deliberately at window zero -- the shipped default -- so this is
        /// natural batching and nothing else. It does not assert a group size
        /// (that would be asserting the scheduler); it asserts that whatever
        /// grouping happened, the store is exactly what a store without a
        /// station would hold.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn a_flood_of_small_puts_through_the_station_is_exact() {
            const WRITERS: usize = 24;

            let dir = tempdir().unwrap();
            let (shared, fs) = store_with_cap_and_station(
                dir.path(),
                Some(64),
                None,
                Some(GroupCommit {
                    window: Duration::ZERO,
                }),
            );
            let fs = Arc::new(fs);

            // Half the writers share one block, so the cross-member merge is
            // exercised by whatever groups the scheduler happens to form.
            let shared_block = b"a block every other writer carries".repeat(64).to_vec();
            let shared_id = shared.hasher().hash(&shared_block);

            let mut handles = Vec::new();
            for i in 0..WRITERS {
                let fs = fs.clone();
                let content = if i % 2 == 0 {
                    shared_block.clone()
                } else {
                    format!("small object {i} ").repeat(64).into_bytes()
                };
                handles.push(tokio::spawn(async move {
                    put(&fs, &format!("small-{i}"), content).await
                }));
            }
            for handle in handles {
                handle.await.unwrap();
            }

            // Every object readable, every block exactly as recorded.
            for i in 0..WRITERS {
                let obj = fs
                    .get_object_meta(BUCKET, &format!("small-{i}"))
                    .unwrap()
                    .expect("every acked PUT must have its record");
                for id in obj.blocks() {
                    let block = shared
                        .block_tree()
                        .get_block(id.as_slice())
                        .unwrap()
                        .expect("and every block of it must be recorded");
                    let path = block.disk_path(id, fs.fs_root().clone());
                    assert_eq!(shared.hasher().hash(&std::fs::read(&path).unwrap()), *id);
                }
            }

            // The contended block: one record, one file, one reference per
            // writer that named it -- however the groups fell.
            let block = shared
                .block_tree()
                .get_block(shared_id.as_slice())
                .unwrap()
                .expect("the shared block must have exactly one record");
            assert_eq!(
                block.rc(),
                WRITERS / 2,
                "one reference per holder, no more and no fewer"
            );
            assert_no_residue(&shared, &fs, &shared_id);

            let stats = shared.group_commit_stats().unwrap();
            assert_eq!(stats.degraded, 0, "nothing should have failed: {stats:?}");
            assert!(stats.groups > 0);

            // And the lifecycle still closes exactly: deleting every holder
            // takes the block with it.
            for i in (0..WRITERS).step_by(2) {
                fs.delete_object(BUCKET, &format!("small-{i}"))
                    .await
                    .unwrap();
            }
            assert!(
                shared
                    .block_tree()
                    .get_block(shared_id.as_slice())
                    .unwrap()
                    .is_none(),
                "as many references out as went in"
            );
        }

        /// The cap bounds a GROUP, not just a batch: members past it form the
        /// next group rather than widening this one.
        ///
        /// This is ADR 0011's review ask 2 made observable -- "the
        /// transaction does not get bigger, its BOUND is unchanged". With a
        /// cap of 2 and four one-block members gathered inside one window, no
        /// group may carry more than two of them.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn a_group_never_carries_more_than_the_cap() {
            const MEMBERS: usize = 4;
            const CAP: usize = 2;

            let dir = tempdir().unwrap();
            let (shared, fs) = windowed_store(dir.path(), Some(CAP), None);

            let mut handles = Vec::new();
            for i in 0..MEMBERS {
                let fs = fs.clone();
                let content = distinct_blocks(&format!("capped-{i}"), 1);
                handles.push(tokio::spawn(async move {
                    put(&fs, &format!("capped-{i}"), content).await
                }));
            }
            for handle in handles {
                handle.await.unwrap();
            }

            let stats = shared.group_commit_stats().unwrap();
            assert!(
                stats.largest <= CAP as u64,
                "a group carried more blocks than the cap: {stats:?}"
            );
            assert_eq!(
                stats.members, MEMBERS as u64,
                "every member was committed exactly once: {stats:?}"
            );
            assert!(
                stats.groups >= 2,
                "four members at a cap of two cannot be one group: {stats:?}"
            );
            assert_eq!(shared.block_tree().len().unwrap(), MEMBERS);
        }
    }
}
