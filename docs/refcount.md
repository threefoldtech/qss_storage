# Reference Counting (Refcount)

Refcount adds reference counting to data blocks.

The idea is that different objects might share the same data blocks.
When a data block is used by an object, its refcount is increased by one.
When an object stops using the data block, the refcount is decreased by one.
When the refcount reaches zero, the data block can be deleted.

`There can be some data leakage, but never data loss`. This means that, in
case of some failures, it is acceptable to not decrease the refcount.
However, it is crucial to never fail to increase the refcount, as this can
lead to data loss.

This contract holds unconditionally: there is a single metadata backend
(the transactional fjall store; ADR 0007 removed the non-transactional
one), and since ADR 0006 every block-record mutation -- insert, dedup
bump, decrement, removal -- happens as a transactional read-modify-write
under the block's stripe lock, with block files reaching their final path
durably before their record commits (file-first). Crash and cancellation
residue is therefore always over-counts or orphan files (leakage, ADR
0005's reconciliation feed), never under-counts.

## Counting rule

One reference per block OCCURRENCE in an object: every dedup hit bumps
the refcount, including a re-PUT of the same content under the same key
(the old same-key skip produced loss-shaped under-counts in the multipart
trace -- see `docs/arch/key-has-block-skip.md` -- and was dropped by ADR
0006). An overwrite therefore deliberately over-counts until reconciled.

## Test Plan

**Normal cases:**

- Creating a new object will set `refcount` to 1.
- Every reuse of the block increases `refcount` by one -- same key or
  new key alike.
- Deleting the object will reduce the `refcount` value.
- When the `refcount` value reaches zero, the block record is removed and
  its file unlinked (under one stripe hold).
- Double-DELETE of one key decrements once: the object record is taken
  atomically, so the second DELETE finds nothing to do.

**Failure cases:**
- when writing/reusing block, failed to write the metadata or increase the `refcount`

    -> it is not allowed to happen, the writing/reusing must be failed, data loss could happend

- when writing the block, the block file was written but the record commit failed

    -> it is OK, the file at its final path is orphan residue: a later
    write of the same block heals it in place, fsck can collect it

- when deleting a block, failed to reduce the `refcount` or delete the metadata

    -> it is OK, data leakage could happen

- when deleting a block, failed to delete from the underlying storage

    -> it is OK, data leakage happened

For the above failure cases, the tests only mandatory to be implemented for the cases that not allowed to happen
