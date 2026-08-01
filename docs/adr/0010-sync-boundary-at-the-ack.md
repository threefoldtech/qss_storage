# The Sync Boundary Moves from the Block to the Ack

**Status**: Proposed
**Date**: 2026-08-01

---

## Context

The write path syncs per 1 MiB block, and the measurements from the ADR
0009 campaign night say that cadence -- not the disk, not the pipeline
-- is the store's ingest ceiling.

What one block costs today, at `fsync` durability:

1. `AtomicBlockWriter` writes the temp file and fsyncs it (ADR 0006
   file-first protocol), then renames it into place.
2. One blocks-DB transaction per block (insert or dedup rc-bump,
   `write_path.rs`), whose commit runs `persist(SyncAll)` -- a journal
   fsync -- serialized through fjall's single-writer transaction lock.

Two-plus fsyncs per MiB, with the journal half single-file. The numbers:

- Terabyte run (2026-08-01, /s3 xfs, campaign config): giants band
  348 -> 203 -> 177 -> 171 MB/s per 300 GiB object, mean 194 MiB/s,
  disk utilization pinned at 99% -- busy waiting on flushes, not
  moving data.
- Controlled A/B (same 16 GiB multipart ingest, same binary, home
  btrfs nvme, only `--durability` varied):

  | durability | MB/s | wall  |
  | ---------- | ---- | ----- |
  | fsync      |   62 | 265 s |
  | fdatasync  |   75 | 218 s |
  | buffer     | 1025 |  16 s |

  With sync off, the complete stack -- multipart client, chunking,
  BLAKE3, dedup lookups, per-block metadata, block file writes --
  sustains a gigabyte per second. The pipeline is not slow. The flush
  cadence is.

Meanwhile the durability CONTRACT binds nowhere near the block. What
ADR 0009's campaign verifies -- and what a client can observe -- is the
acknowledgement: every write the client saw succeed survives a crash.
Nothing is promised about a part that never acked. Syncing per block
buys durability nobody can see, at 16x the price.

Constraints this ADR inherits:

- ADR 0006 file-first: a block record must never be readable before its
  block file is durable at its final path. This ordering is kept.
- ADR 0008 rc exactness: dedup bumps and releases stay transactional.
- Hard rules 1 and 6: per-block stripe locks guard rc mutations; fjall
  is the leaf lock -- no stripe may be acquired while holding a fjall
  guard.
- The `--durability` levels keep their meanings. `buffer` is untouched
  by this ADR.

---

## Decision

Move the sync boundary from the block to the ack. One request -- a
part upload, a single-part PUT, a complete -- becomes one durability
unit: all of its block files are made durable as a group, then all of
its block records commit in one transaction with one journal persist,
and only then does the client see success.

Concretely, per request at `fsync`/`fdatasync` durability:

1. Block files stream to temp names as they arrive, exactly as today.
2. When the request's blocks are all written (or a batch cap is hit),
   their files are synced CONCURRENTLY -- fdatasync in flight for the
   whole batch, then the renames, then one fsync of the fanout
   directories touched (deduplicated: a 16 MiB part usually touches
   16 distinct fanout dirs, but each is synced once).
3. One blocks-DB transaction carries every insert and rc-bump of the
   batch, committed with a single `persist()` at the store durability.
4. The part ETag / PUT response goes out after step 3. The ack still
   never precedes the sync -- the contract is unchanged, the boundary
   is bigger.

A batch cap (default 64 blocks, config `max_blocks_per_commit`) bounds
transaction size and stripe hold time; a request larger than the cap
becomes several consecutive batches, the last one closing at the ack.
A single-block PUT is a one-block batch: today's behavior, unchanged
latency.

Stripe acquisition for a batch takes every needed stripe in sorted
block-hash order before the transaction begins, preventing ABBA between
concurrent batches while preserving hard rule 6 (stripes first, fjall
second, exactly as today -- just N of them).

Crash windows: a kill after step 2 but before step 3 leaves up to
`max_blocks_per_commit` orphan block files. That is residue class 1
(ADR 0005), already collected by fsck and already healed in place by a
later PUT of the same content. The batch changes the residue's SIZE,
not its class. No new residue class exists, because the ordering
(files durable before records) is preserved wholesale.

---

## Architecture Overview

### Component Breakdown

1. **Batch accumulator** (`cas-storage/src/cas/write_path.rs`)
   - Collects `(block_hash, bytes, dedup-vs-new)` decisions for the
     request instead of committing each; dedup lookups still happen
     per block, against committed state plus the accumulator itself
     (a block appearing twice in one request is one insert plus a
     bump, same as today across requests).
   - Flushes at the cap and at request end.

2. **Grouped disk writer** (`cas-storage/src/cas/block_disk.rs`)
   - New batch operation on `AtomicBlockWriter`: sync N temp files
     concurrently (spawn_blocking pool or io_uring later; the
     concurrency primitive is an implementation detail behind the
     batch API), rename N, fsync the union of touched directories.
   - The existing single-block path remains as the one-block batch.

3. **Multi-stripe guard** (`cas-storage/src/cas/stripes.rs`)
   - Acquire-many in sorted hash order, released as one RAII guard.

4. **One-tx batch commit** (`cas-storage/src/metastore/meta_store.rs`)
   - The existing transaction surface already holds arbitrarily many
     operations (ADR 0003's claim tx proves it); this only widens the
     write path's use of it.

### Data Flow

```
part bytes -> chunk -> hash -> [temp write ...] ----------------+
                                                                |
                 batch boundary (cap or request end)            v
   stripes(sorted) -> fdatasync xN (concurrent) -> rename xN -> dirsync
                 -> one tx: inserts + bumps -> persist(durability)
                 -> release stripes -> ack
```

---

## Alternatives Considered

### Bigger blocks (4-16 MiB)
- **The idea**: fewer blocks means fewer syncs; keep the per-block
  boundary.
- **Optimizes for**: no concurrency work, no batching code.
- **Sharpest tradeoff**: dedup granularity collapses -- a 1 MiB edit
  re-stores 16 MiB -- and the on-disk format (record v3, fanout,
  every deployed store) changes under a migration this codebase just
  demonstrated it refuses to do implicitly (ADR: blocks/.db).
- **Bets on**: dedup at 1 MiB not being load-bearing. It is the
  product.

### fdatasync as the default durability, no grouping
- **The idea**: cheaper syscall, same cadence.
- **Optimizes for**: a one-line change.
- **Sharpest tradeoff**: 62 -> 75 MB/s. The measurement says the
  cadence dominates, not the syscall flavor.
- **Bets on**: the journal fsync being the minor term. It is not: it
  is serialized behind a single-writer lock.

### Parallel per-block syncs, per-block commits kept
- **The idea**: io_uring / thread-pool the block-file fsyncs only.
- **Optimizes for**: not touching the metadata path.
- **Sharpest tradeoff**: the journal persist stays one-per-block and
  single-writer-serialized -- the other half of the ceiling remains.
- **Bets on**: the block-file fsync being the whole cost. The A/B
  numbers (fdatasync barely better than fsync) say otherwise.

### Ack from page cache, sync on a timer
- **The idea**: what `buffer` mode already is, made default.
- **Optimizes for**: the full 1 GB/s.
- **Sharpest tradeoff**: the ADR 0009 contract -- acked writes survive
  a crash -- becomes false for power loss. That is a contract change,
  not an optimization, and the campaign that would have to bless it
  currently has an open loss finding at buffer durability.
- **Bets on**: operators accepting a durability window. Not this
  ADR's call to make.

### Group commit at the ack boundary (chosen)
- **The idea**: sync once per request, everything the contract
  observes, nothing it does not.
- **Optimizes for**: durable throughput -- projected 400-800 MB/s at
  full fsync semantics (16-64x fewer journal fsyncs, block syncs
  overlapped).
- **Sharpest tradeoff**: wider crash residue (bounded, existing
  class), multi-stripe locking discipline, a real diff in the write
  path.
- **Bets on**: requests being the unit clients reason about. They
  are: it is what the S3 API acks.

---

## Consequences

### Positive
- Ingest at full durability projected into the hundreds of MB/s; the
  16 GiB A/B becomes the acceptance benchmark.
- Fewer transactions contending on the single-writer lock helps every
  concurrent writer, not just large objects.
- Small-object latency unchanged (one-block batch == today).

### Negative
- The write path gains a stateful accumulator where it had a loop.
- Crash residue per crash grows from "up to one block file" to "up to
  one batch per in-flight request" -- more orphans for fsck to sweep,
  same class.
- Multi-stripe acquisition is new deadlock surface, mitigated by
  sorted order and a single acquire point.

### Risks
- A subtle rc error inside the batched tx would be exactly the class
  ADR 0008 spent a campaign making exact. Mitigation: the batch tx is
  built from the SAME primitives (insert, bump), property-tested, and
  the campaign's phase 5 refcount recount is the backstop.
- Directory-fsync semantics differ across filesystems. Mitigation:
  the batch dirsync set is computed, not assumed; xfs and btrfs are
  both in the test matrix via the A/B rig.

---

## What an Expert Would Ask

**Q: A crash lands between the batch dirsync and the tx commit. What
does fsck see, and can a concurrent dedup hit have referenced a block
whose record died with the tx?**
A: fsck sees up to a batch of orphan block files -- residue class 1,
swept today. The dedup question is the sharp one: a concurrent request
must not bump a record that is not yet committed. The accumulator only
exposes its blocks for dedup WITHIN its own request; cross-request
dedup still reads committed state. So a parallel writer of the same
content writes its own temp file and, at commit, the second tx's
insert becomes a bump (the insert-vs-bump decision is made inside the
tx against current state, as it is today). No reference can precede
its record's commit.

**Q: Does the rename-before-record ordering survive batching on xfs,
where rename durability needs a directory sync?**
A: The batch syncs data (fdatasync), then renames, then fsyncs each
touched fanout directory, then commits records. The record therefore
never exists without a durably-renamed file, which is ADR 0006's
invariant verbatim. The current per-block code fsyncs the file and
relies on the same dirsync reasoning at open; the batch makes the
dirsync explicit and per-batch, which is stronger than today, not
weaker.

**Q: What does this do to ack latency at the tail -- a 64-block batch
holds stripes across up to 64 concurrent fdatasyncs?**
A: Stripes are taken AFTER the data syncs, immediately before the
renames and the tx (the syncs need no stripes: temp files are
per-attempt-unique, invisible to readers). Hold time is renames + one
tx + one persist -- comparable to today's per-block hold times summed,
but paid once. If measurements show tail inflation, the cap comes
down; it is a config, not a format.

**Q: Two concurrent batches both contain block X, both new. Who wins?**
A: The stripe for X serializes them at the tx step. The first commits
the insert; the second's in-tx decision sees the committed record and
becomes a bump; its own temp file for X is surplus and is removed on
release (the existing adopt-in-place logic from the crash-heal path
covers the file already being at its final name). This is today's
race, at batch width.

**Q: Where is the next wall after this one?**
A: The A/B's buffer row: ~1 GB/s, which on this hardware is roughly
where hashing plus the client meet. That is CPU, not policy -- an
acceptable place to stop.

---

## Implementation Plan

### Decisions you will probably want to tweak
- **Batch cap**: 64 blocks (64 MiB) default, `max_blocks_per_commit`
  in `[store]`. Alternative: cap by bytes or by stripe count. Cost to
  change later: none, it is config.
- **Sync primitive for block files**: fdatasync (data + size), with
  the per-batch dirsync carrying rename durability. Alternative: keep
  full fsync per file. Cost to change later: one function, but it is
  THE performance decision -- changing it back is changing the ADR's
  point.
- **Where the batch closes**: at the request ack and at the cap, never
  on a timer. Alternative: a small time window to merge concurrent
  small PUTs into one journal persist (true group commit). Cost to
  change later: additive -- a timer can be introduced behind the same
  batch API without touching callers. Deliberately out of scope now:
  it trades small-PUT latency for throughput and needs its own
  numbers.

### Known unknowns and how the plan absorbs them
- **Concurrent-fdatasync scaling on xfs**: assumed near-linear to
  batch width on NVMe. Signal to pivot: the A/B rig at batch width 64
  showing sublinear gains -> drop the cap to the knee.
- **fjall tx size limits**: assumed a 64-op tx is routine (ADR 0003's
  claim tx already batches arbitrarily many part deletions). Signal:
  memtable/journal pressure in the benchmark -> halve the cap.
- **The buffer-durability loss finding** (campaign, twice, RECORD
  ABSENT, unreproduced in 92 targeted kills): unrelated codepath in
  principle -- this ADR does not touch buffer mode -- but the batched
  persist reduces journal traffic, which narrows the suspected
  rotation window as a side effect. If the root cause lands in
  fjall's rotation, this ADR neither fixes nor worsens it.

### The mechanical work
Batch accumulator in `write_path.rs`; batch API on `AtomicBlockWriter`
(concurrent fdatasync + rename set + dirsync set); sorted multi-stripe
guard in `stripes.rs`; the one-tx commit is existing transaction
surface. Tests: crash fixtures for kill-between-dirsync-and-commit
(orphan batch), the concurrent-same-block property test at batch
width, the 16 GiB A/B as a regression benchmark with a floor, and the
campaign unchanged as the acceptance gate.

### Review asks
1. Sync boundary = the request ack (with a 64-block cap): yes/no?
2. fdatasync + per-batch dirsync as the block-file primitive, or keep
   full per-file fsync inside the batch?
3. Stripes taken after data syncs, sorted, released after commit --
   sign off on that locking order?
4. Is the timer-based small-PUT merge (true group commit) wanted in
   scope now, or later behind the same API as written?

---

## Open Questions

**Architecture-changers**
- [ ] Should `fdatasync` durability remain a distinct user-facing
      level once the batch exists, or collapse into `fsync` (the batch
      makes their cost nearly identical, and two near-identical levels
      invite misconfiguration)?

**Behavior definers**
- [ ] The cap-sized partial batch mid-request: on a crash, a client
      retrying the part re-uploads all its blocks; earlier batches'
      blocks dedup-hit and heal orphans in place. Confirm that is the
      intended retry story (it is today's, at batch width).
- [ ] Does `complete-multipart-upload` need its own batch (it writes
      no blocks, only records), or is one tx + one persist -- which it
      already is -- sufficient? (Proposed: already sufficient.)
