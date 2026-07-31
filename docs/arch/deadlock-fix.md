# The fjall write-lock deadlock

Status: HISTORICAL. Written 2026-07-30 from primary sources; as of
2026-07-31 the write path this document ends on (commit-before-I/O with a
compensating delete) has itself been replaced by ADR 0006's file-first
protocol: every block now runs its dedup RMW, disk write, and record
insert inside one `spawn_blocking` closure that owns the block's stripe
lock, the record commits only after the file is durable at its final
path, and `cleanup_on_failure`, the sync `AsyncFileSystem` seam, and the
commit `unwrap()`s described below are all gone. The deadlock analysis
and the trade-off account remain accurate history and explain WHY the
current design looks the way it does.

An earlier version of this
document was a reconstruction; it has been superseded by this one after the
pre-fix code was found intact in the imported upstream history. The commit
hash `c5f9cc9` that `async_fs.rs` used to cite remains unrecoverable (it
exists in no reachable history), but the *code state it changed* is fully
visible at `7f20502:src/cas/fs.rs` (upstream, 2025-03-06), so nothing of
substance is lost.

This document explains three things: the bug, the fix that shipped, and --
the part easiest to miss -- what the fix traded away. The trade is why
`docs/adr/0005-fsck-scrub-reconciliation.md` exists.

## The invariant upstream wanted

The block write path touches two resources per block:

1. a record in the metadata store (block hash, size, refcount);
2. the block file on disk.

Upstream wanted them atomic: *the record exists if and only if the file
landed*. The pre-fix code delivered exactly that, transactionally:

```text
// 7f20502:src/cas/fs.rs (condensed)
let mut store_tx = self.meta_store.begin_transaction();   // takes the writer lock
let block = store_tx.write_block(hash, len, reused)?;
self.async_fs.create_dir_all(parent).await?;              // <-- await, lock held
self.async_fs.write(&block_path, &bytes).await?;          // <-- await, lock held
//   on disk error:  store_tx.rollback()   -> record never existed
//   on disk success: store_tx.commit()    -> record and file both exist
```

`AsyncFileSystem` was an `async` trait backed by the `async-fs` crate. On
disk failure the transaction rolled back; a dangling record (metadata naming
a file that never landed) was impossible except in a process crash between
the file write and the commit.

Correct semantics. The problem is the two `.await`s in the middle.

## The deadlock, precisely

On the transactional backend (`FjallStore`, the default per
`DEFAULT_METADATA_DB`), `begin_transaction` acquires fjall's single-writer
lock -- concretely a `std::sync::MutexGuard` (see the Send/Sync commentary
in `metastore/stores/fjall.rs`). A std mutex blocks the OS thread; it does
not yield to the async runtime.

The sequence, on a tokio runtime with N worker threads:

1. Task A begins a transaction, holds the writer guard, and suspends at the
   disk-write await. The guard stays held across the suspension.
2. Concurrent PUTs on other connections reach `begin_transaction` and block
   their worker threads *inside the mutex acquire*. Each one parks an OS
   thread the runtime can never reclaim.
3. The disk I/O completes on async-fs's own I/O thread pool and wakes
   task A. But waking only queues the task -- resuming it requires a free
   tokio worker.
4. Once all N workers are parked in step 2, there is no thread left to poll
   task A. The holder of the lock can never run again; the lock is never
   released; every thread is blocked on it. Total, permanent deadlock.

Note what it is not: not lock ordering (there is one lock), not a missed
unlock, not fjall misbehaving. It is the classic sync-lock-across-await
hazard, made deterministic by a bounded worker pool. PUT concurrency at or
above the worker count reproduces it reliably.

The non-transactional backend (`FjallStoreNotx`) never had the hazard:
writes land in the keyspace immediately, there is no write lock, and
rollback is hand-rolled compensation (`FjallNoTransaction` remembers its
inserts and undoes them). Only the default backend deadlocked.

## The fix that shipped

Two coordinated changes, both visible in today's `cas/write_path.rs` and
`cas/async_fs.rs`:

1. **Commit before I/O.** The transaction commits immediately after
   `write_block` decides whether the block is new -- before any disk I/O.
   The writer lock is held only across pure in-memory/DB work and is
   released before the file write starts. The inline comments ("we commit
   the meta database transaction BEFORE writing the block to disk...",
   "COMMIT IMMEDIATELY to release lock") are this half.
2. **A synchronous write seam.** `AsyncFileSystem` became a plain-`fn`
   trait backed by `std::fs`. With no await point anywhere near the
   remaining critical sections, the hazard is gone from the type system
   rather than policed by convention. The trait survives as a mocking seam
   for `test_store_object_write_failure`.

## What the fix traded away

The pre-fix design was *transactionally correct and concurrently broken*.
The fix made it *concurrently correct and transactionally weaker*, and the
weakening is easy to under-state:

- **Before**: a failed disk write rolled the record back. Dangling records
  required a crash in a narrow window.
- **After**: the record is already committed when the disk write starts. A
  failed disk write triggers metrics (`block_write_error`, the
  `BlockWriteGuard` state machine) and a best-effort compensating delete
  (`cleanup_on_failure`, `write_path.rs:184-195`, in-tree since `e349d9d`)
  that removes the just-committed record. The compensation is unserialized
  and unconditional, so it narrows the dangling-record window without
  closing it: it is skipped on crash or panic, its own failure is
  warn-and-continue, and under concurrency it can remove a record a
  same-content PUT just dedup-bumped -- a loss-shaped race analyzed as
  defect 1 of ADR 0006. Where a dangling record survives, a subsequent
  GET of that object fails with an I/O error.

So the dangling-record state is no longer a *crash* window; it is an
*every-failed-write* window. This is the accepted cost of the deadlock fix,
and it is the direct motivation for the dangling-record pass in ADR 0005
(fsck), which is the reconciliation this path now requires.

The refcount contract (`docs/refcount.md`: leakage allowed, loss never)
still holds -- a dangling record is leakage-shaped, not loss-shaped -- but
the fix moved a failure class from "impossible" to "counted and ignored".

## Roads not taken (and why)

- **Commit-after-write with a sync disk write** (hold the lock over
  `std::fs::write`): correct and deadlock-free, but serializes every block
  write in the store behind the single writer lock. Throughput becomes one
  block per disk-write latency, store-wide. Rightly rejected.
- **`spawn_blocking` for the disk write, transaction held**: still awaits
  (on the join handle) under the guard. Same deadlock. Not a fix at all.
- **Compensating delete on failure**: keep commit-before-I/O, and on disk
  failure remove/decrement the just-committed record. A version of this
  shipped in `e349d9d` as `cleanup_on_failure` (write_path.rs:184-195),
  predating this document's rewrite, which wrongly described it as not
  done. The shipped form is an unconditional out-of-tx remove, and ADR
  0006's review found it is itself a loss bug under concurrency (it can
  delete a record a concurrent dedup-hit PUT now depends on). A corrected
  form would decrement-or-remove-if-rc==1 inside one transaction -- but
  ADR 0006's file-first protocol removes the need for compensation
  entirely, so that repair is moot.
- **Two-phase records** (commit `pending`, write disk, commit `finalize`):
  the rigorous version of the previous point; failures leave a
  self-describing pending record fsck can treat distinctly. Costs a second
  transaction per block.

## Residual costs in the shipped design (since resolved)

Both residual costs this section used to list were closed by ADR 0006's
implementation:

- The 1 MiB `std::fs::write` on a tokio worker: all block disk I/O now
  runs in `spawn_blocking` closures, off the executor, with fjall commits
  (and their journal fsyncs) alongside.
- The `commit().unwrap()`s: replaced by error mapping into the write
  path's error channel before the protocol change landed (plan
  component 1).

## Pointers

- Pre-fix code: `git show 7f20502:src/cas/fs.rs` (search
  `begin_transaction`).
- Current code: `cas-storage/src/cas/write_path.rs` (`write_one_block`,
  the file-first protocol under the stripe),
  `cas-storage/src/cas/block_disk.rs` (the atomic temp+fsync+rename
  writer and its mockable ops seam -- the honest successor of the old
  `async_fs.rs`), `cas-storage/src/cas/stripes.rs` (the per-block locks),
  `cas-storage/src/metastore/stores/fjall.rs` (the writer-lock guard).
  The non-transactional backend this document contrasts against was
  removed outright by ADR 0007.
- Consequences and reconciliation: `docs/refcount.md`,
  `docs/adr/0005-fsck-scrub-reconciliation.md`.
- Successor design: `docs/adr/0006-block-write-protocol.md` -- the
  file-first, per-block-striped protocol that closed the dangling-record,
  partial-read, and durability gaps this document describes, and retired
  the "compensating delete" and "two-phase records" roads above. Landed
  2026-07-31.
