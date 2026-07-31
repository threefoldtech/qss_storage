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

## Reconciliation

The other half of this contract is `qss-storage-fsck`, the offline tool ADR
0005 built (`docs/fsck.md`). It walks every reference holder -- object
records in each bucket tree, part records of in-flight uploads -- counts one
reference per block OCCURRENCE by the rule above, and compares that with
`_BLOCKS` and with what is actually on disk. The workflow is report, then
repair, then recount: the report is emitted before any repair runs,
`--repair` applies the safe subset (rc set to the walked truth in both
directions, orphan and off-depth files deleted, corrupt and foreign files
quarantined rather than deleted, adoption of a record whose bytes turn up at
another depth, half-deleted bucket teardowns resumed), and every pass then
runs again -- anything the tool claims to repair that still stands is
CRITICAL and the run exits nonzero. A recount to zero frees the block the
way a last decrement does: the record is removed and its file unlinked, so
rc=0 is never a state anything observes. No refcount is touched at all
unless the holder enumeration closed completely; a count over a partial
holder set would authorise freeing blocks that are still referenced, which
is loss by repair.

One class of leak now has a collector that runs *online*, beside fsck: the
stale-upload GC of ADR 0003 (`docs/multipart.md`). A multipart upload that
is never completed or aborted holds its blocks through its part records
forever, so s3cas sweeps on a TTL -- aborting aged uploads through the same
claim a client's `AbortMultipartUpload` takes, and removing part records
whose upload record is gone. It releases references exactly the way
`DeleteObject` does (per occurrence, striped RMW, record removed before its
blocks), so it introduces no new rc semantics: it is a caller of the delete
primitive, not a second one. Each reap is a take -- the record and the block
list released come out of one transaction -- so two reapers racing over one
part release it exactly once. fsck stays the backstop and the only thing
that ever *reconciles*: the GC collects what it can name, fsck counts what
is actually there.

The one residue this contract cannot express -- a record whose bytes are
gone (a `buffer`-durability power cut, or corruption) -- gets the `degraded`
flag on the block record rather than a removal. The record keeps accounting
for its surviving holders, so nothing under-counts, and the write path
treats a degraded record as absent for dedup: the next PUT of that content
writes the file, clears the flag and adds its own reference in the same
striped transaction. Damage stops propagating without the accounting ever
lying.

## Counting rule

One reference per block OCCURRENCE in an object: every dedup hit bumps
the refcount, including a re-PUT of the same content under the same key
(the old same-key skip produced loss-shaped under-counts in the multipart
trace -- see `docs/arch/key-has-block-skip.md` -- and was dropped by ADR
0006).

An overwrite pays that bump back. Writing an object record over an
existing one releases the replaced record's occurrences, through the same
striped primitive a DELETE of that object would have used (ADR 0008): the
new record commits first, the release follows. So per block shared by the
old object and the new one the net is zero (bumped by the write, dropped
by the release), per block only the old one held it is -1, per block only
the new one holds it is +1 -- exactly the truth, with no reconciliation
pending. This covers every object-record write: PUT, the inline path, and
`CompleteMultipartUpload`. An overwrite that crashes between the commit
and the release leaves an over-count, which is leakage and fsck's to
collect, in keeping with the contract above.

## Test Plan

**Normal cases:**

- Creating a new object will set `refcount` to 1.
- Every reuse of the block increases `refcount` by one -- same key or
  new key alike.
- Deleting the object will reduce the `refcount` value.
- Overwriting a key reduces it too, once per occurrence of the replaced
  object: a same-content re-PUT therefore leaves the count where it was,
  and N overwrites of one key leave exactly the final object's
  occurrences.
- When the `refcount` value reaches zero, the block record is removed and
  its file unlinked (under one stripe hold).
- Double-DELETE of one key decrements once: the object record is taken
  atomically, so the second DELETE finds nothing to do.
- Concurrent overwrites of one key decrement once each: the replace is
  the same kind of atomic pair, so every writer releases exactly the
  record it displaced and no record is released twice.

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
