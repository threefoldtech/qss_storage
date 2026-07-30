# The Fjall write-lock deadlock, and why the disk-write seam is synchronous

Status: reconstructed 2026-07-30. The original document this path pointed at
(`docs/arch/deadlock-fix.md`, cited by `cas-storage/src/cas/async_fs.rs`
together with a commit `c5f9cc9`) was never committed to any branch of this
repository or of the retired tfstor checkout. This is a reconstruction from
the code that embodies the fix; the referenced commit hash is unrecoverable.

## The problem

The block write path (`cas/write_path.rs::store_object`) interleaves two
resources per block:

1. a metadata transaction on the fjall keyspace -- fjall is a single-writer
   database, so an open write transaction holds an exclusive, *synchronous*
   lock;
2. the block file write to disk.

The original code performed the disk write while the metadata transaction was
still open, and did so through an async filesystem interface. Awaiting disk
I/O while holding fjall's synchronous writer lock is the classic
sync-lock-across-await hazard: the task suspends without releasing the lock,
the executor schedules another upload task onto the same worker, that task
blocks the thread on the same writer lock, and with enough concurrent uploads
every worker thread ends up parked on a lock whose holder can no longer be
polled. The process stalls without crashing.

## The fix (two halves)

Both halves are visible in `write_path.rs`:

- **Commit before I/O.** The metadata transaction is committed immediately
  after `write_block` decides whether the block is new -- before any disk
  I/O. The lock is never held across the block file write at all. The inline
  comments ("we commit the meta database transaction BEFORE writing the block
  to disk to avoid holding the lock during slow I/O operations", "COMMIT
  IMMEDIATELY to release lock") are this half.

- **A synchronous write seam.** The disk write itself goes through the
  `AsyncFileSystem` trait (`cas/async_fs.rs`), whose methods are deliberately
  plain `fn`, backed by `std::fs`. With no await point inside the write, the
  remaining critical sections cannot suspend mid-flight, and the name of the
  hazard disappears from the type system rather than being policed by
  convention. The trait survives only as a mocking seam for
  `test_store_object_write_failure`.

## Consequences

- Crash ordering: metadata can name a block whose file write subsequently
  fails. The `BlockWriteGuard` accounts for this (`blocks_dropped` /
  `block_write_error` metrics); refcount reconciliation is described in
  `docs/refcount.md`.
- The write path blocks an executor thread for the duration of one block
  write (at most `BLOCK_SIZE` = 1 MiB). This is accepted: block writes are
  bounded and local, and the alternative reintroduces the hazard.
