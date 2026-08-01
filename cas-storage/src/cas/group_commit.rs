//! The commit station: strangers share a flush (ADR 0011).
//!
//! ADR 0010 moved the sync boundary from the block to the ack, and one
//! request became one durability unit. What it left behind is flush COUNT: a
//! 4 KiB PUT still pays a whole journal persist, and a thousand concurrent
//! small writers are a thousand persists a second queued behind fjall's
//! single-writer lock. This module merges the CLOSING step of concurrent
//! requests -- and only the closing step -- into one transaction with one
//! persist.
//!
//! ```text
//! request A: chunk -> stage -> fdatasync xN --+
//! request B: chunk -> stage -> fdatasync xN --+--> station queue
//! request C: chunk -> stage -> fdatasync xN --+        |
//!                                                      v
//!                        committer: stripes(union, sorted)
//!                          -> re-check dedup (incl. cross-member)
//!                          -> rename all -> dirsync (union, once each)
//!                          -> ONE tx: everyone's inserts+bumps
//!                          -> ONE persist -> release -> wake A, B, C
//! ```
//!
//! # Natural batching, not a mandatory timer
//!
//! While one group commits, arriving batches queue; when the commit returns,
//! the committer takes everything queued as the next group. An uncontended
//! request finds the committer idle and commits IMMEDIATELY, so group commit
//! adds nothing to a lone ack -- by construction, not by tuning. The optional
//! [`GroupCommit::window`] is an extra bounded wait measured from the FIRST
//! member entering an empty queue, for operators who measured and want bigger
//! groups than commit duration alone accumulates. Zero means the timer does
//! not exist.
//!
//! # The cap is the cap
//!
//! A group never carries more than `max_blocks_per_commit` blocks -- the same
//! bound one large request already fills under ADR 0010, and still the one
//! number bounding transaction size, stripe hold time and the orphan count a
//! single kill can leave. Grouping does not make the transaction bigger; it
//! lets strangers fill a cap a small request would have wasted. Queued
//! batches beyond the cap are the next group.
//!
//! # A stranger's error never fails your ack
//!
//! If the group transaction errors it rolls back, and each member replays as
//! its own transaction under the stripes the group already holds. One bad
//! member fails one request; the rest commit. The degradation is per-group,
//! never sticky: the next group merges again.
//!
//! # What is NOT here
//!
//! Nothing about ordering moved. Files are still durable at their final paths
//! before any record exists (ADR 0006, hard rule 5), stripes are still taken
//! before fjall and never after (hard rule 6), the transaction still runs in
//! one blocking closure that owns the stripes with no await inside it (hard
//! rules 3 and 4), and the rc arithmetic is still the same two primitives
//! (ADR 0008). A group is a wider batch, not a different protocol -- which is
//! also why a kill inside one leaves residue class 1 and nothing new.

use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use super::block_disk::StagedBlock;
use super::shared_block_store::SharedBlockStore;
use super::stripes::BatchStripeGuard;
use super::write_path::{
    BatchEntry, BlockWriteGuard, FlushOutcome, Payload, abandon_after_landing, has_live_record,
    record_entry,
};
use crate::metastore::BlockId;
use crate::metrics::SharedMetrics;

/// How this process runs its commit station (ADR 0011).
///
/// Its mere existence is the switch: `Some` builds a station, `None` leaves
/// the ADR 0010 write path untouched. There is no third state and no
/// auto-enable -- the owner's ruling is that grouping strangers is opted
/// into, never sprung on an operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupCommit {
    /// Extra bounded wait after the FIRST member of a group arrives.
    ///
    /// [`Duration::ZERO`] -- the default -- means the timer does not exist,
    /// and groups are exactly what natural batching delivered. Anything else
    /// trades lone-ack latency for group size.
    pub window: Duration,
}

/// What a station has done, read off at a moment in time.
///
/// The ADR asks for a group-size histogram or, at minimum, a counter pair
/// that yields the mean. This is that pair ([`groups`](Self::groups) and
/// [`members`](Self::members)) plus the largest group seen and the number of
/// groups that had to degrade -- the numbers that say whether the merge is
/// working and whether it is costing anybody.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GroupCommitStats {
    /// Groups committed. At `fsync` durability this is also the number of
    /// journal persists the blocks DB paid on the write path, which is the
    /// number ADR 0011 exists to bring down.
    pub groups: u64,
    /// Members across all groups. `members / groups` is the mean group size;
    /// pinned at 1 under load, the window knob is what the ADR points at.
    pub members: u64,
    /// Largest group observed.
    pub largest: u64,
    /// Groups whose shared transaction failed and were replayed member by
    /// member. Nonzero is not an error -- it is the isolation working -- but
    /// sustained growth means something is failing every group it touches.
    pub degraded: u64,
}

impl GroupCommitStats {
    /// Mean group size, or `None` before the first group.
    #[must_use]
    pub fn mean_group_size(&self) -> Option<f64> {
        #[allow(clippy::cast_precision_loss)] // a reported ratio, not a value
        (self.groups > 0).then(|| self.members as f64 / self.groups as f64)
    }
}

/// The live counters behind [`GroupCommitStats`].
#[derive(Debug, Default)]
pub(super) struct StationStats {
    groups: AtomicU64,
    members: AtomicU64,
    largest: AtomicU64,
    degraded: AtomicU64,
}

impl StationStats {
    fn record(&self, members: u64) {
        self.groups.fetch_add(1, Ordering::Relaxed);
        self.members.fetch_add(members, Ordering::Relaxed);
        self.largest.fetch_max(members, Ordering::Relaxed);
    }

    fn record_degraded(&self) {
        self.degraded.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn snapshot(&self) -> GroupCommitStats {
        GroupCommitStats {
            groups: self.groups.load(Ordering::Relaxed),
            members: self.members.load(Ordering::Relaxed),
            largest: self.largest.load(Ordering::Relaxed),
            degraded: self.degraded.load(Ordering::Relaxed),
        }
    }
}

/// One request's sealed batch, waiting for the committer.
///
/// "Sealed" is the whole point: its files are staged and fdatasynced, its
/// dedup decisions are made, and it belongs to the station rather than to the
/// request that produced it. The request has nothing left to do but park.
struct SealedBatch {
    entries: Vec<BatchEntry>,
    /// The submitting request's metrics, so a block the group has to write
    /// after all is counted against the request that owns it and not against
    /// whoever happened to be first in the group.
    metrics: SharedMetrics,
    /// Where this member's own outcome goes. A dropped receiver (the client
    /// hung up) is not an error: the batch commits regardless, which is what
    /// makes the stretch uncancellable.
    reply: oneshot::Sender<io::Result<FlushOutcome>>,
}

impl SealedBatch {
    fn fail(self, e: io::Error) {
        abandon_after_landing(self.entries);
        let _ = self.reply.send(Err(e));
    }
}

/// The per-store commit station: a queue, and one committer draining it.
///
/// Built lazily, on the first flush that wants it, because a station needs a
/// tokio runtime to live in and a `SharedBlockStore` is also opened by tools
/// that have none (fsck, the inspect subcommands). Nothing about the store on
/// disk changes either way.
pub(super) struct CommitStation {
    submit: mpsc::UnboundedSender<SealedBatch>,
    stats: Arc<StationStats>,
}

impl CommitStation {
    /// Starts a station for `store`, with its committer running as a task.
    ///
    /// The committer holds a `Weak`, never an `Arc`: a strong reference from
    /// the task the store spawned back to the store would be a cycle nothing
    /// collects, and the store would outlive every process that opened it.
    /// When the store drops, the queue closes and the committer stops.
    pub(super) fn start(store: &Arc<SharedBlockStore>, options: GroupCommit, cap: usize) -> Self {
        let (submit, queue) = mpsc::unbounded_channel();
        let stats = Arc::new(StationStats::default());
        let committer = Committer {
            store: Arc::downgrade(store),
            queue,
            window: options.window,
            cap: cap.max(1),
            stats: Arc::clone(&stats),
        };
        tokio::spawn(committer.run());
        tracing::debug!(
            window_us = options.window.as_micros(),
            cap,
            "Commit station started"
        );
        Self { submit, stats }
    }

    /// Hands a sealed batch over and parks until the committer says what
    /// happened to it.
    ///
    /// Cancelling this future abandons the wait, not the work: the batch is
    /// the station's now and commits either way, so a client that hangs up
    /// mid-ack leaves a committed batch rather than a half-applied one --
    /// the same property the ADR 0010 close gets from owning its stripes
    /// inside a blocking closure.
    pub(super) async fn close(
        &self,
        entries: Vec<BatchEntry>,
        metrics: SharedMetrics,
    ) -> io::Result<FlushOutcome> {
        let (reply, parked) = oneshot::channel();
        let sealed = SealedBatch {
            entries,
            metrics,
            reply,
        };
        if let Err(returned) = self.submit.send(sealed) {
            // The committer is gone, which means the store is going away.
            // Nothing was renamed, so the staged files are the caller's
            // problem exactly as they are on any pre-commit failure.
            let batch = returned.0;
            abandon_after_landing(batch.entries);
            return Err(io::Error::other(
                "the commit station is gone; the store is shutting down",
            ));
        }
        match parked.await {
            Ok(outcome) => outcome,
            // The committer dropped the reply without sending: it panicked,
            // or the store went away mid-group.
            Err(_) => Err(io::Error::other(
                "the commit station dropped a batch without an outcome",
            )),
        }
    }

    pub(super) fn stats(&self) -> GroupCommitStats {
        self.stats.snapshot()
    }
}

/// The committer loop.
struct Committer {
    store: std::sync::Weak<SharedBlockStore>,
    queue: mpsc::UnboundedReceiver<SealedBatch>,
    window: Duration,
    cap: usize,
    stats: Arc<StationStats>,
}

impl Committer {
    /// Take a group, commit it, repeat, until the store goes away.
    async fn run(mut self) {
        // A member that would have pushed the previous group past the cap
        // waits here and leads the next one. This is the only reason the
        // gathering loops can stop mid-drain without losing anybody: an
        // unbounded receiver has no way to un-receive.
        let mut carried: Option<SealedBatch> = None;

        loop {
            let first = match carried.take() {
                Some(batch) => batch,
                None => match self.queue.recv().await {
                    Some(batch) => batch,
                    // Every sender is gone: the store dropped.
                    None => break,
                },
            };

            let Some(store) = self.store.upgrade() else {
                first.fail(io::Error::other(
                    "the store was dropped before this batch committed",
                ));
                break;
            };

            let mut group = vec![first];
            let mut blocks = group[0].entries.len();

            // The window, if the operator asked for one: measured from the
            // first member entering an empty queue, so groups close at the
            // deadline or the cap, whichever comes first, and never grow
            // without bound under steady load.
            if !self.window.is_zero() {
                let deadline = tokio::time::Instant::now() + self.window;
                while blocks < self.cap {
                    match tokio::time::timeout_at(deadline, self.queue.recv()).await {
                        Ok(Some(next)) => {
                            if blocks + next.entries.len() > self.cap {
                                carried = Some(next);
                                break;
                            }
                            blocks += next.entries.len();
                            group.push(next);
                        }
                        // Queue closed, or the deadline arrived.
                        Ok(None) | Err(_) => break,
                    }
                }
            }

            // Natural batching: whatever queued while the previous group was
            // committing, and not one microsecond of waiting for more.
            while carried.is_none() && blocks < self.cap {
                match self.queue.try_recv() {
                    Ok(next) => {
                        if blocks + next.entries.len() > self.cap {
                            carried = Some(next);
                            break;
                        }
                        blocks += next.entries.len();
                        group.push(next);
                    }
                    Err(_) => break,
                }
            }

            self.commit_group(&store, group, blocks).await;
        }

        tracing::debug!("Commit station stopped");
    }

    /// One group: stripes, close, wake.
    async fn commit_group(
        &self,
        store: &Arc<SharedBlockStore>,
        group: Vec<SealedBatch>,
        blocks: usize,
    ) {
        let members = group.len() as u64;
        self.stats.record(members);
        group[0].metrics.group_committed(members);

        // The union, sorted by stripe index and deduplicated by the same
        // acquisition ADR 0010 built -- at group width instead of batch
        // width, which is the only difference.
        let ids: Vec<BlockId> = group
            .iter()
            .flat_map(|member| member.entries.iter().map(|entry| entry.id))
            .collect();
        let stripes = store.stripes().lock_batch(&ids).await;

        let mut entries = Vec::with_capacity(group.len());
        let mut metrics = Vec::with_capacity(group.len());
        let mut replies = Vec::with_capacity(group.len());
        for member in group {
            entries.push(member.entries);
            metrics.push(member.metrics);
            replies.push(member.reply);
        }

        // The in-flight gauge is charged to the first member's collector;
        // every collector in one process is the same handle in practice, and
        // a group is one blocking submission however many requests it serves.
        let gauge = metrics[0].clone();
        let shared = Arc::clone(store);
        let stats = Arc::clone(&self.stats);
        gauge.block_disk_op_started();
        let joined = tokio::task::spawn_blocking(move || {
            let outcomes = close_group(&shared, &metrics, stripes, entries, &stats);
            gauge.block_disk_op_finished();
            outcomes
        })
        .await;

        match joined {
            Ok(outcomes) => {
                for (reply, outcome) in replies.into_iter().zip(outcomes) {
                    let _ = reply.send(outcome);
                }
                tracing::debug!(members, blocks, "Group committed");
            }
            // The closure panicked. Every guard went with it and
            // BlockWriteGuard's Drop counted its blocks as dropped; all this
            // can still do is stop the members waiting forever.
            Err(join_err) => {
                for reply in replies {
                    let _ = reply.send(Err(io::Error::other(format!(
                        "the group commit task did not complete: {join_err}"
                    ))));
                }
            }
        }
    }
}

/// The group closer: the ADR 0010 close, generalized from one batch to
/// several, with the cross-member merge in the middle of it.
///
/// The stripes are held for all of it and released when this returns, which
/// is what keeps the rename-through-persist stretch uncancellable (hard rule
/// 4) and the transaction on one thread with no await inside it (hard rule
/// 3). One outcome comes back per member, in the order they were given.
///
/// # The cross-member merge
///
/// Two members both staging a new block X become one insert and one bump. It
/// takes two rules and no cleverness:
///
/// - Only the FIRST member's staged file for X is renamed into place. The
///   others' are discarded, never renamed over -- renaming would replace a
///   live inode with identical bytes for nothing, and if the winner chose a
///   different fanout depth it would leave an off-depth duplicate (residue
///   class 2) behind.
/// - Every member's entry goes through [`record_entry`] in the same order,
///   inside one transaction, so the second call's `bump_block_rc` reads the
///   first's uncommitted insert and becomes the bump. ADR 0008's exactness
///   stays a property of the primitives, exactly as within one request.
fn close_group(
    shared: &Arc<SharedBlockStore>,
    metrics: &[SharedMetrics],
    _stripes: BatchStripeGuard,
    mut members: Vec<Vec<BatchEntry>>,
    stats: &StationStats,
) -> Vec<io::Result<FlushOutcome>> {
    let ops = shared.disk_ops();

    // Under the stripes the block tree is stable for every id in the group,
    // so one read per DISTINCT id decides the file work for all of them.
    //
    // A read that FAILS is recorded as "no live record" rather than failing
    // anybody here. The transaction below reads the same record through the
    // same decode and will fail on it too, and failing there is what routes
    // the member to the degrade path and isolates it. Being wrong in this
    // direction costs a rename-over of identical bytes; being wrong the other
    // way would commit a record whose file is gone.
    let mut live: HashMap<BlockId, bool> = HashMap::new();
    for entries in &members {
        for entry in entries {
            if let std::collections::hash_map::Entry::Vacant(slot) = live.entry(entry.id) {
                let answer = has_live_record(shared, &entry.id).unwrap_or_else(|e| {
                    tracing::warn!(
                        block_hash = %entry.id.to_hex(),
                        error = %e,
                        "Could not read a block record under its stripe; \
                         the transaction decides"
                    );
                    false
                });
                slot.insert(answer);
            }
        }
    }

    // Which ids this group has already placed a file for. Both loops below
    // consult it, so a block named by three members is written once whether
    // it arrived staged or as an overtaken dedup hit.
    let mut placed: HashMap<BlockId, u8> = HashMap::new();

    // The rare overtake, at group width: a block that deduped against a
    // record which has since gone (a concurrent DELETE took its last
    // reference and unlinked the file). The bytes were kept for exactly this.
    for (index, entries) in members.iter_mut().enumerate() {
        for entry in entries.iter_mut() {
            if live[&entry.id] {
                continue;
            }
            let Payload::Deduped(bytes) = &entry.payload else {
                continue;
            };
            if let Some(&depth) = placed.get(&entry.id) {
                // Another member of this group already wrote it; this entry
                // only needs to know where it went.
                entry.depth = Some(depth);
                continue;
            }
            tracing::debug!(
                block_hash = %entry.id.to_hex(),
                "The record this block deduped against is gone; writing it after all"
            );
            let depth = shared.placement().choose_depth(&entry.id);
            if let Err(e) = shared
                .disk_writer()
                .write_block(&*ops, &entry.id, depth, bytes)
            {
                // The file for this block does not exist, so no member of the
                // group may record it. Marking it live would be a lie; the
                // transaction is left to fail on it, which fails exactly the
                // members that named it.
                tracing::error!(
                    block_hash = %entry.id.to_hex(),
                    error = %e,
                    "Could not rewrite an overtaken dedup hit"
                );
                continue;
            }
            entry.depth = Some(depth);
            entry.guard = Some(BlockWriteGuard::new_pending(metrics[index].clone()));
            placed.insert(entry.id, depth);
        }
    }

    // Staged files: one lands per distinct new block, the rest are surplus.
    // Surplus is either a block another writer committed before this group
    // reached its stripes, or one a stranger IN this group staged too.
    let mut to_land: Vec<&StagedBlock> = Vec::new();
    for entries in &members {
        for entry in entries {
            let Payload::Staged(staged) = &entry.payload else {
                continue;
            };
            if live[&entry.id] || placed.contains_key(&entry.id) {
                shared.disk_writer().discard_staged(&*ops, staged);
            } else {
                placed.insert(entry.id, staged.depth);
                to_land.push(staged);
            }
        }
    }

    // Files durable at their final paths, every touched directory synced once
    // -- before a single record exists (hard rule 5, ADR 0006, unchanged at
    // group width). A failure here is a store-level fault, not one member's
    // fault, so it fails the group: there is no isolation to offer when the
    // filesystem stopped answering.
    if let Err(e) = shared.disk_writer().land_batch(&*ops, &to_land) {
        let message = format!("landing the group's block files: {e}");
        return members
            .into_iter()
            .map(|entries| {
                abandon_after_landing(entries);
                Err(io::Error::new(e.kind(), message.clone()))
            })
            .collect();
    }

    // ONE transaction carrying every member's inserts and bumps, ONE persist.
    let mut tx = shared.meta_store().begin_transaction();
    let mut outcomes = Vec::with_capacity(members.len());
    let mut failure = None;
    'group: for entries in &members {
        let mut outcome = FlushOutcome::default();
        for entry in entries {
            match record_entry(&mut tx, entry) {
                Ok(true) => outcome.written += 1,
                Ok(false) => outcome.ignored += 1,
                Err(e) => {
                    failure = Some(e);
                    break 'group;
                }
            }
        }
        outcomes.push(outcome);
    }

    if failure.is_none() {
        tracing::debug!(
            target: "cas_storage::locks",
            members = members.len(),
            "Committing block group"
        );
        match tx.commit() {
            Ok(()) => {
                for entries in members {
                    resolve_landed(entries);
                }
                return outcomes.into_iter().map(Ok).collect();
            }
            Err(e) => failure = Some(e),
        }
    } else {
        tx.rollback();
    }

    // Degraded: the shared transaction is gone and every member gets its own.
    // The renames already happened, so this is a transaction retry and
    // nothing else -- and it is STRICTLY DIRECT, never a second trip through
    // the station. The stripes the group holds are a superset of what each
    // member needs, so no lock is taken or released here.
    let reason = failure.expect("the degrade path runs only after a failure");
    tracing::warn!(
        members = members.len(),
        error = %reason,
        "The group transaction failed; replaying each member on its own"
    );
    stats.record_degraded();
    metrics[0].group_commit_degraded();
    members
        .into_iter()
        .map(|entries| replay_alone(shared, entries))
        .collect()
}

/// One member, one transaction, after the group's failed.
///
/// The files are already at their final paths, so a member that fails here
/// leaves orphan block files: residue class 1, which fsck collects and a
/// later PUT of the same content heals in place. Exactly what a kill between
/// the dirsync and the commit leaves, which is the point -- the degrade path
/// invents no new residue.
fn replay_alone(
    shared: &Arc<SharedBlockStore>,
    entries: Vec<BatchEntry>,
) -> io::Result<FlushOutcome> {
    let mut tx = shared.meta_store().begin_transaction();
    let mut outcome = FlushOutcome::default();
    for entry in &entries {
        match record_entry(&mut tx, entry) {
            Ok(true) => outcome.written += 1,
            Ok(false) => outcome.ignored += 1,
            Err(e) => {
                tx.rollback();
                abandon_after_landing(entries);
                return Err(e.into());
            }
        }
    }
    if let Err(e) = tx.commit() {
        abandon_after_landing(entries);
        return Err(e.into());
    }
    resolve_landed(entries);
    Ok(outcome)
}

/// Resolves the guards of a member whose records committed.
fn resolve_landed(entries: Vec<BatchEntry>) {
    for mut entry in entries {
        if let Some(guard) = entry.guard.take() {
            guard.written(entry.len);
        }
    }
}
