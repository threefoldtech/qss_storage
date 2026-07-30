# Block Write Protocol: File-First, Per-Block Striped Locking

**Status**: Proposed
**Date**: 2026-07-30

---

## Context

The block write path has been redesigned once already: upstream held fjall's
single-writer lock (a `std::sync::MutexGuard`) across awaited disk writes and
deadlocked under concurrency; the shipped fix commits metadata before disk
I/O through a deliberately synchronous write seam. The full account is in
`docs/arch/deadlock-fix.md`. The fix is concurrency-correct but left four
defects on the table, all verified in the current code:

1. **Dangling records on ordinary failure**: the record is committed before
   the file write starts, so any failed write leaves metadata naming a file
   that never landed. GET of that object returns an I/O error. (The pre-fix
   design rolled back; the fix traded that away.)
2. **Partial-file visibility**: `std::fs::write` goes straight to the final
   path. A concurrent GET between the record commit and write completion can
   open a missing or half-written block file.
3. **A durability hole**: block files are never fsynced. The `Durability`
   config guards only fjall, so `durability = "fsync"` still allows a power
   cut to persist the record while the block bytes evaporate from the page
   cache. The knob promises more than the store delivers.
4. **Executor starvation**: the sync 1 MiB writes park tokio workers for the
   duration of each disk write -- caution left over from the deadlock fix,
   no longer required by anything (the lock is no longer held during I/O).

Related: ADR 0002 (BLAKE3 addressing -- what makes this ADR possible),
ADR 0005 (fsck -- what reconciles the failure residue), `docs/refcount.md`
(leakage-allowed / loss-never contract), `docs/arch/deadlock-fix.md`.

---

## Decision

Proposed, pending review: invert the write ordering and change the locking
model, exploiting the property the previous designs never used --

> **Block files are immutable and content-addressed. Two writers of the
> same block write identical bytes to the same path. File creation is
> idempotent and commutative.**

Therefore file writes need no metadata lock at all; they need only
per-block mutual exclusion against unlink, and all-or-nothing visibility.

### The protocol

**Stripe locks.** `N` async mutexes (`tokio::sync::Mutex<()>`, default
`N = 1024`), indexed by a prefix of the block hash. Async locks may be held
across I/O awaits -- the historical hazard was a *sync* guard held across
awaits, not locking as such.

**PUT of one block** (inside the existing per-block streaming closure):

```text
lock stripe(hash)
  if block record exists:                   # dedup hit
      short fjall tx: bump refcount; commit
  else:
      spawn_blocking:
          write blocks/.tmp/<hash>-<nonce>
          fsync file                         # per Durability config
          rename into final path             # atomic, same filesystem
          fsync parent dir                   # per Durability config
      short fjall tx: insert record rc=1 (or bump if raced); commit
unlock
```

**DELETE (per object)**:

```text
one fjall tx: decrement refcounts for the object's blocks,
              remove records that reach zero; commit; collect zeroed list
for each zeroed block:                       # after the tx, no global lock
    lock stripe(hash)
        if block record still absent:        # re-check under the stripe
            unlink file (spawn_blocking)
    unlock
```

**Invariants**:
- The fjall writer guard is only ever acquired *inside* a stripe lock and
  is never held across an await. Lock order stripe -> fjall is global and
  acyclic: the deadlock is structurally impossible.
- A block record is committed only after its file is durable at its final
  path. Metadata never asserts anything the disk has not already made true.
- Readers never lock: rename gives them complete files only; a GET that
  opened the fd before an unlink streams to completion (POSIX fd
  semantics); open-after-unlink fails exactly when the record is gone.

### Race analysis (the one that matters)

DELETE-of-last-ref racing a PUT that recreates the same block:

- Delete zeroes and removes the record in its tx, then wants the file gone.
- PUT takes the stripe, sees no record, renames a fresh (identical) file
  into place, inserts a new record -- all under the stripe.
- Delete's unlink runs under the same stripe and *re-checks the record*:
  if the PUT got there first, the record exists and the unlink is skipped
  (rc=1, file present -- consistent). If delete got there first, the file
  is gone before the PUT takes the stripe, and the PUT writes it fresh
  (record + file -- consistent). No interleaving produces a record without
  a file or an unlinked live block. This is the race today's meta-first
  ordering closes by accident and a naive file-first ordering would open;
  the stripe re-check closes it by construction.

---

## Architecture Overview

### Component Breakdown

1. **Stripe set** (`cas-storage/src/cas/`, new module)
   - `Stripes { locks: Vec<tokio::sync::Mutex<()>> }`, `fn for_hash(&BlockId) -> &Mutex<()>`.
     Lives on `SharedBlockStore` so all namespaces of one store share it
     (two namespaces writing the same block must serialize on one stripe).
2. **Atomic file writer** (replaces the body of the `AsyncFileSystem` seam)
   - temp-write / fsync / rename / dir-fsync, executed via `spawn_blocking`;
     fsyncs gated by the store's `Durability` (`Buffer` skips both).
     The mocking seam for `test_store_object_write_failure` is preserved.
3. **Write path** (`cas-storage/src/cas/write_path.rs`)
   - The per-block closure reorders to the protocol above; the
     `BlockWriteGuard` metrics state machine survives with the same states.
4. **Delete path** (`cas-storage/src/cas/delete_path.rs`)
   - Split into the decrement tx and the per-block striped unlink pass;
     also replaces the current `.expect("Could not delete file")` panic.
5. **Temp hygiene**
   - `blocks/.tmp/` is cleared on store open (crash residue is by
     definition garbage) and ignored by walkers; ADR 0005's orphan sweep
     covers renamed-but-uncommitted files.

---

## Alternatives Considered

### Compensating delete (keep meta-first, undo on failure)
- **The idea**: on disk-write failure, open a new short tx and remove the
  just-committed record. The notx backend's hand-rolled rollback is this
  pattern.
- **Optimizes for**: smallest possible patch to the shipped design.
- **Sharpest tradeoff**: fixes only defect 1 -- partial-file visibility and
  the durability hole remain, because metadata still commits before bytes.
- **Bets on**: defects 2 and 3 never mattering. The durability hole alone
  falsifies that for any deployment that chose `durability = "fsync"`.

### Two-phase records (pending -> finalize)
- **The idea**: commit a `pending` record, write the file, commit a
  `finalize`; failures leave a self-describing pending record.
- **Optimizes for**: an explicit intent log; fsck can distinguish states.
- **Sharpest tradeoff**: two transactions per block on the hot path, and
  readers must treat `pending` as absent -- a read-path change; and it
  still commits intent before bytes, so the durability ordering still
  needs the file fsync anyway.
- **Bets on**: the intent-log visibility being worth the tx traffic. With
  content-addressed idempotent files, the intent log records nothing the
  filesystem does not already express.

### Global write serialization (hold the lock over a sync write)
- **The idea**: pre-fix ordering with a sync write under the guard.
- **Optimizes for**: transactional purity with no new machinery.
- **Sharpest tradeoff**: one block write per disk latency, store-wide.
- **Bets on**: write throughput not mattering. Rejected, as it was in the
  original fix.

### Sharding fjall instead (per-stripe keyspaces)
- **The idea**: eliminate the single-writer bottleneck by sharding the
  metadata DB itself.
- **Optimizes for**: metadata write parallelism.
- **Sharpest tradeoff**: cross-shard consistency for object records that
  span blocks; an on-disk layout change with migration; solves a
  bottleneck nobody has measured as a problem.
- **Bets on**: metadata commit rate being the limiting factor. No evidence;
  the short-tx design keeps guard hold times in microseconds.

---

## Consequences

### Positive
- All four defects close at once; every failure mode degrades to an
  invisible orphan (leakage-shaped), never a client-visible dangling
  record, partial read, or phantom-durable record.
- Orphans self-heal: a retried upload of the same content renames over the
  orphan and inserts the record it lacked.
- `Durability` becomes true end-to-end; `Buffer` keeps today's performance
  for users who chose speed.
- Disk I/O leaves the executor threads; the deadlock remains structurally
  impossible (single acyclic lock order, no sync guard across awaits).
- Likely fixes latent refcount races on the lock-free notx backend as a
  side effect (concurrent read-modify-write of rc has no serialization
  there today -- to be verified in review).

### Negative
- Per-new-block fsync latency at `Fsync`/`Fdatasync` durability (real
  cost, correct cost -- it was being skipped, not saved).
- A stripe is held across a block's disk write: concurrent writers of the
  *same* block serialize (correct and required); hash-prefix collisions
  between different blocks serialize spuriously at 1/N probability.
- More moving parts in the write path: stripes, temp dir, two-step delete.

### Risks
- rename atomicity requires temp and final path on the same filesystem:
  `blocks/.tmp/` under the blocks root guarantees it; must be documented
  against anyone mounting `.tmp` elsewhere.
- `spawn_blocking` pool exhaustion under massive concurrent PUTs shifts
  the queue from tokio workers to the blocking pool -- bounded and
  backpressured, but worth a metric.
- Multi-stripe operations (bulk delete) must never hold two stripes at
  once; the per-block loop above never does. Any future batched variant
  must acquire in sorted-hash order or one at a time.

---

## What an Expert Would Ask

**Q: The stripe is an async mutex held across disk I/O -- have you just
rebuilt the original deadlock with nicer types?**
A: No. The original deadlock needed a *sync* lock (parks OS threads) plus
a bounded worker pool. An async mutex parks *tasks*: workers stay free,
wakers fire on release, and progress needs only that the holder eventually
completes I/O. The failure mode of a stuck disk becomes stuck requests on
that stripe, not a stuck runtime. The fjall guard -- the only sync lock --
is never held across an await, enforced by the acyclic stripe->fjall order.

**Q: What happens when the process crashes between rename and the record
insert?**
A: An orphan file at its final content address. Invisible to reads (no
record), reclaimed by ADR 0005's orphan sweep, or healed by any future
upload of the same content. Loss is impossible; leakage is bounded and
collectable. This is the designed failure residue, and it is strictly
smaller than today's (dangling records are client-visible; orphans are
not).

**Q: Doesn't fsync-per-block destroy write throughput?**
A: At `Fsync` durability it costs what durability costs -- one file fsync
plus an amortizable dir fsync per *new* block (dedup hits fsync nothing).
Today that cost is not saved, it is silently skipped while the config
claims otherwise. `Buffer` preserves today's behavior byte-for-byte for
deployments that want speed. If measured throughput at `Fsync` is
unacceptable, batching dir fsyncs is the tuning knob, not dropping the
file fsync.

**Q: Why is the dedup-hit refcount bump safe against a concurrent delete
zeroing the same block?**
A: Both run their fjall tx under the same stripe. Under the stripe, either
the bump commits first (delete's decrement then sees rc=2->1, no unlink)
or the removal commits first (the PUT then sees no record and takes the
insert path, writing the file fresh). The re-check-before-unlink covers
the tail where delete already left its tx. Every interleaving ends
consistent; this is the invariant test ADR 0005 wants, and it should be
written as a loom-style or stress test in this ADR's implementation.

**Q: Does anything still need `AsyncFileSystem` to be sync?**
A: No -- that constraint died when the lock stopped spanning I/O. The seam
survives for test injection only; its implementation moves to
`spawn_blocking` internally. The trait's name should finally match
reality (it has been neither async nor about async since the fix).

---

## Implementation Plan

### Decisions you will probably want to tweak
- **Stripe count (1024) and placement on `SharedBlockStore`**.
  Alternative: per-namespace stripes (wrong: two namespaces share blocks).
  Cost to change later: none (in-memory only) -- but the *placement* is
  correctness, only the count is tunable.
- **Durability gating of file fsyncs** (`Buffer` skips, others sync).
  Alternative: always fsync. Cost to change: config semantics, document
  either way before release.
- **Temp naming and location** (`blocks/.tmp/<hash>-<nonce>`).
  Alternative: `O_TMPFILE` + `linkat` (no temp names at all, Linux-only).
  Cost to change: low; decide portability stance now.

### Known unknowns and how the plan absorbs them
- notx backend refcount races today: verify before building; if confirmed,
  this ADR's stripes are the fix and the finding goes in the commit
  message. Signal to pivot: none expected -- stripes are needed regardless.
- Blocking-pool sizing under load: default pool, add a gauge; pivot to a
  dedicated bounded pool if the gauge shows starvation.
- Whether `complete_multipart_upload`'s block reuse path has the same
  bump-vs-delete race: audit during implementation; the stripe API makes
  the fix mechanical wherever it appears.

### The mechanical work
Stripe module + placement; atomic writer with durability-gated fsyncs;
write_path reorder (guard states unchanged); delete_path split + panic
removal; `.tmp` cleanup on open; rename `AsyncFileSystem`; tests: race
stress (PUT-vs-DELETE same block), crash-window fixtures for ADR 0005
(orphan, temp residue), fsync gating per durability level, and an
end-to-end concurrent-PUT benchmark to quantify the executor win.

Review asks:
1. Stripe placement on `SharedBlockStore` (shared across namespaces) --
   agreed?
2. Durability-gated file fsyncs, `Buffer` = today's behavior -- agreed, or
   always-fsync?
3. `O_TMPFILE` (Linux-only) or portable named temps?
4. Should this land before ADR 0005 (fsck then reconciles a smaller
   failure zoo) or after (fsck first cleans historic residue)?

---

## Open Questions

**Architecture-changers**
- [ ] Is there any deployment story where blocks/ and its `.tmp` could end
      up on different filesystems (breaking rename atomicity)? If yes, the
      writer needs a same-device check at open.

**Behavior definers**
- [ ] On rename-into-place when the file already exists (orphan heal):
      rename over it unconditionally, or compare-and-skip? (Rename-over is
      simpler and idempotent by content addressing; skip saves a write but
      trusts the orphan's integrity without verifying it.)
- [ ] Should the stripe also serialize verify-on-read's re-hash against
      concurrent writes, or is read-side locking still rejected?
