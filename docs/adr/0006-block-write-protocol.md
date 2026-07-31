# Block Write Protocol: File-First, Per-Block Striped Locking

**Status**: Proposed (revised 2026-07-30 after adversarial code review;
13-agent verification pass, all Context claims below carry file:line
evidence; all six adversarially re-derived claims survived skeptic
refutation; decided 2026-07-31: full-id adaptive-depth paths,
key_has_block skip dropped, fjall_notx scoped out of loss-never, lands
before ADR 0005 -- all review asks resolved)
**Date**: 2026-07-30

---

## Context

The block write path has been redesigned once already: upstream held fjall's
single-writer lock (a `std::sync::MutexGuard`) across awaited disk writes and
deadlocked under concurrency; the shipped fix commits metadata before disk
I/O through a deliberately synchronous write seam. The full account is in
`docs/arch/deadlock-fix.md`. The fix is concurrency-correct but the review
verified five defects in the current code -- three of them loss-shaped,
which the previous revision of this ADR understated:

1. **A racy, best-effort compensating delete.** The record commits before
   the file write starts (`write_path.rs:141-171`), and on write failure
   `cleanup_on_failure` (`write_path.rs:184-195`, in-tree since `e349d9d`)
   removes the just-committed record with an unconditional, out-of-tx
   `block_tree().remove`. Verified failure modes:
   - a crash or panic between commit and write leaves a dangling rc=1
     record -- and the `.unwrap()` on commit at `write_path.rs:157/167`
     turns a fjall `PersistError` into exactly that panic, with cleanup
     skipped;
   - cleanup failure is warn-and-continue (`write_path.rs:190-191`);
   - cleanup removes the `_BLOCKS` record but never the `_PATHS` entry
     inserted with it (`meta_store.rs:710-711`) -- permanent path-space
     leakage (a re-upload then claims a longer prefix);
   - **loss race**: a concurrent same-content PUT dedup-bumps rc 1->2 and
     skips its own disk write (`write_path.rs:151-163`); the failing
     writer's cleanup then removes the rc=2 record -- the other PUT's
     committed object references a block with no record and no complete
     file (the failed write may have left a partial). GET fails with
     `BlockNotFound` (`read_path.rs:29-31`).
   Any dangling record that survives also *poisons dedup*: a later PUT of
   the same content bumps it, skips the write, and commits a live object
   over a file that never landed. Loss, not leakage.
2. **Partial-file visibility**: `std::fs::write` goes straight to the final
   path (`async_fs.rs:24-26`). The client-visible route is the dedup-hit
   object: writer A commits the record and starts the slow write; writer B
   dedup-hits and commits its object; a GET of B's object opens a missing
   or half-written file (`block_stream.rs:348-351` opens lazily
   mid-stream).
3. **A durability hole**: block files and their directories are never
   fsynced anywhere in the workspace. `Durability` has exactly one sink:
   the fjall commit persist (`fjall.rs:33-41,183-187`). At
   `durability = "fsync"` (the default) the record reaches stable media
   via journal `sync_all` *before* the file bytes enter page cache; a
   power cut in the writeback window recovers a record whose file is
   missing, zero-length, or partial -- loss at the strongest configured
   durability, and client-visible with certainty via the dedup-poisoning
   chain of defect 1 (the next same-content PUT skips its write against
   the recovered record). On `fjall_notx` the knob is dropped entirely
   (`fjall_notx.rs:110-117`, commit is a no-op at `:150-152`): it fsyncs
   nothing there, not even metadata.
4. **Executor starvation**: the sync 1 MiB writes park tokio workers for
   the duration of each disk write (`write_path.rs:205`, zero
   `spawn_blocking` in the workspace) -- caution left over from the
   deadlock fix, no longer required by anything.
5. **Non-transactional delete on both backends.**
   `MetaStore::delete_object` (`meta_store.rs:343-415`) is a sequence of
   bare tree ops -- no transaction on *either* backend -- and the object
   record lives in the namespace DB while block records live in the shared
   DB, so no single fjall tx could ever span both. Its rc read-modify-write
   races a concurrent PUT's atomic bump: DELETE reads rc=1
   (`meta_store.rs:372`), PUT bumps to 2 in its tx and writes no file
   (dedup hit), DELETE removes the record (`:383`) and unlinks the file
   (`delete_path.rs:16-18`) -- the PUT's committed object dangles. Loss,
   today, on the default backend. On notx the PUT-vs-PUT bump is also an
   unlocked get-then-insert that loses updates (undercount -> premature
   free). Concurrent DELETEs of one key double-decrement for the same
   reason. Skeptic-verified with step-by-step interleavings.

Correction to the previous revision: it claimed today's meta-first ordering
closes the delete-vs-recreating-PUT race "by accident". It does not. The
exact recreate interleaving is defused only by `_PATHS` indirection (a
recreating PUT allocates a *longer* path while the old entry survives, so
the pending unlink hits a dead file -- `meta_store.rs:678-685`,
`delete_path.rs:16` before `:19`), and that protection does not cover the
dedup-bump variant in defect 5.

**A load-bearing premise correction.** Block disk paths today are NOT
hash-derived. They are allocated per-insert as the shortest free prefix in
the shared `_PATHS` tree and stored in the record
(`meta_store.rs:671-711`, `block.rs:216-227`). "Two writers of the same
block write identical bytes to the same path" is therefore false in
general in the current layout, and this ADR must decide the path scheme
explicitly (see Decision).

Related: ADR 0002 (BLAKE3 addressing), ADR 0004 (store ownership -- the
`.tmp` sweep depends on it), ADR 0005 (fsck -- reconciles the failure
residue), ADR 0007 (the fjall_notx backend's fate -- this ADR only
scopes it out), `docs/refcount.md` (leakage-allowed / loss-never),
`docs/arch/deadlock-fix.md`.

---

## Decision

Proposed, pending review: invert the write ordering and change the locking
model, exploiting the property content addressing makes available --

> **Block files are immutable and content-addressed. Two writers of the
> same block write identical bytes, and a file's name identifies its
> block (below), so a path can only ever hold one block's bytes: file
> creation is idempotent and commutative.**

File writes then need no metadata lock; they need per-block mutual
exclusion against unlink, and all-or-nothing visibility. One invariant
makes the whole protocol compose:

> **Every block-record mutation (insert, bump, decrement, remove) happens
> under `stripe(hash)`.** While a stripe is held, that block's record and
> file are frozen for everyone else.

### Path scheme: full-id filenames, adaptive fanout depth (DECIDED 2026-07-31)

A block file is named by its full id and placed under a fanout depth
chosen at write time:

```text
blocks/<hex b0>/.../<hex b(d-1)>/<full-hex id>        d in 1..=width(id)
```

Directory names are single hex bytes of the id (2 chars); the filename
is the full lowercase hex of the id (32 chars for 16-byte ids, 64 for
32-byte). Dir names and file names can never collide; the old `_xx`
underscore convention dies. The record stores `d` (one byte, replacing
the stored path bytes), so `disk_path(id, depth)` is pure and GET and
DELETE never probe.

The load-bearing property is that **a file's name identifies its
block**: two different hashes can never collide on a path, so the
depth choice is a *placement policy* with no correctness content -- any
depth is correct. That is what makes file creation idempotent and
commutative, keeps orphan heal sound (an insert probes the bounded
candidate depths first and, on finding a file named `<id>`, reuses that
depth and renames over it -- complete or corrupt alike), and makes the
freed-prefix-reuse loss race (a different hash claiming a dead record's
path) structurally impossible. The `_PATHS` tree and the
shortest-free-prefix allocator (`meta_store.rs:663-727`) are removed;
`delete_object`'s path-map maintenance (`delete_path.rs:19-25`)
disappears with them.

The adaptive depth preserves the current design's virtue -- small
stores stay shallow and directory sizes stay bounded for the VFS --
without its defect (names that only a `_PATHS` lookup could attribute
to a block). Placement default: shallowest depth whose target directory
is below an occupancy threshold; depth is bounded only by the id width,
so the fanout can deepen for as long as the store grows.

**No upgrade path is needed**: no deployed store carries data, so this
layout simply *is* the layout -- no format bump, no migration
machinery. Fsck (ADR 0005) and every tool assume it.

(Rejected alternatives -- `_PATHS` with added discipline, and the
fixed two-level draft -- are recorded in Alternatives Considered.)

### The protocol

**Stripe locks.** `N` async mutexes (`tokio::sync::Mutex<()>`, default
`N = 1024`, tunable), indexed by a prefix of the block hash. Async locks
may be held across I/O awaits -- the historical hazard was a *sync* guard
held across awaits, not locking as such.

**PUT of one block** (inside the existing per-block streaming closure):

```text
lock stripe(hash)
  one short fjall tx: re-read record INSIDE the tx
      if present: bump refcount; commit      # dedup hit -- no file I/O
      else: drop the tx, fall through
  if no record:
      spawn_blocking:
          write blocks/.tmp/<hash>-<nonce>   # O_CREAT|O_EXCL, nonce mandatory
          fsync file                         # per Durability config
          fsync newly created fanout dirs,   # per Durability config,
              deepest-up                     #   cached once durable
          rename into final path             # atomic; rename-over, always
          fsync parent dir                   # per Durability config
      short fjall tx: insert record rc=1; commit
unlock
```

The existence check and the rc mutation are one transactional
read-modify-write -- never a pre-tx read with a blind write. A stale-read
bump would resurrect a concurrently removed record with rc=2 while true
references are 1: a permanent leak. (Today's `write_block` already reads
inside the tx, `meta_store.rs:626-629`; the protocol must not regress
this.) On the insert path no re-check before the insert tx is needed:
only stripe-holders insert, and we hold the stripe.

**DELETE (per object)**:

```text
one namespace-DB tx: read AND remove the object record   # atomic pair
for each block in the removed object's list:
    lock stripe(hash)          # guard moves INTO the blocking closure
        short fjall tx: read rc inside tx; decrement;
                        if rc reaches 0: remove record; commit
        if removed: unlink file    # ENOENT tolerated; never abort loop
    unlock
```

Object-record removal is ordered *first* and is a separate operation in a
different fjall database (namespace DB vs shared blocks DB -- no single tx
can span them, `fs.rs:130-141` vs `shared_block_store.rs:55-74`). A crash
between the two steps leaves rc over-counts: leakage, never loss. The
atomic read+remove of the object record defeats double-DELETE of one key
on the transactional backend (second tx sees the key gone); notx cannot
provide even that, and is scoped out of the loss-never guarantee
(decided 2026-07-31 -- see Consequences).

The per-block decrement lives *inside* the stripe. This is the material
change from the previous revision, which ran one big decrement tx before
taking any stripe: that shape is sound on the transactional backend (the
single-writer tx serializes whole-tx interleavings) but collapses on notx,
where "tx" is a no-op and the unstriped decrement races the striped bump
-- and the stripe re-check then *authorizes* unlinking a just-recreated
live block, because it cannot distinguish "PUT never happened" from "my
own decrement used a stale read". Striping the decrement fixes both
backends identically and makes the safety argument uniform.

**Invariants**:
- All block-record mutations happen under `stripe(hash)`. The fjall
  writer guard is acquired only inside awaitless synchronous sections and
  never spans an await (this is a *memory-safety* requirement, not just
  hygiene: `FjallTransaction`'s manual `Send` is sound only because tx
  begin and commit happen in one uninterrupted synchronous stretch --
  `fjall.rs:222-252`). No task ever acquires a stripe while holding the
  fjall guard; DELETE's namespace tx takes fjall with no stripe held,
  which the order permits -- fjall is the innermost (leaf) lock. Lock
  order stripe -> fjall is acyclic: the deadlock is structurally
  impossible.
- A block record is committed only after its file is durable at its final
  path. At `Fsync`/`Fdatasync` this is crash-true. At `Buffer` it is an
  in-process ordering guarantee only: it holds across process crashes
  (both writes were handed to the kernel in order) but a power cut can
  persist the record's journal pages while the file's data pages are
  lost -- a dangling record. Choosing Buffer accepts that residue; ADR
  0005 keeps a dangling-record sweep for Buffer-configured stores.
- Readers never lock: rename gives them complete files only; a GET that
  opened the fd before an unlink streams to completion (POSIX fd
  semantics); open-after-unlink fails exactly when the record is gone.
- The unlink re-check may be a plain (non-tx) read: while the stripe is
  held, the only writers that could make an absent record present are
  PUTs of the same block, and they are blocked on this stripe. Removals
  by others cannot turn absent into present.

### Race analysis

DELETE-of-last-ref racing a PUT that recreates the same block, under the
amended protocol: every rc mutation is striped, so per block the world
serializes into stripe-holds. Either the PUT's bump lands first (DELETE's
striped decrement then sees rc=2->1, no unlink), or the DELETE's
decrement-and-remove lands first (the PUT then finds no record under its
own stripe hold and takes the insert path, writing the file fresh --
rename-over an orphan is idempotent). The unlink runs inside the same
stripe hold as the decrement that zeroed the record, so no unlink can
interleave with a recreate. No interleaving produces a record without a
file or an unlinked live block.

The dedup path's record-vanished tail: a PUT holds `stripe(h)`, its in-tx
re-read finds the record gone (a DELETE's striped decrement won the race
before we acquired the stripe -- once we hold it, nothing mutates). The
PUT falls through to the full insert path, unconditionally correct and
simple: the vanished record's file was unlinked under the same stripe
hold that removed the record, so the insert probes, finds nothing, and
writes fresh.

### Cancellation (new)

Request futures are dropped when clients disconnect (`main.rs:374-379`
spawns per-connection tasks; handlers run on them). Dropping a
`spawn_blocking` `JoinHandle` does NOT cancel the closure -- it runs
detached to completion, possibly after a long queue delay. The protocol
is designed against that:

- **Drop while awaiting the stripe**: zero residue -- nothing was written,
  `tokio::sync::Mutex::lock` is cancel-safe, no poisoning.
- **Drop while awaiting the PUT's blocking write**: the stripe releases;
  the detached closure completes its rename *outside any stripe*. Safe,
  because a rename only installs a complete, fsynced, byte-identical
  file, and the detached path never inserts a record. Worst residue: an
  orphan at the final content address (leakage). This safety rests on two
  hard requirements:
  1. **The temp nonce is load-bearing, not style.** Without it, a queued
     zombie writer from a cancelled attempt opens the *same* temp path
     with `O_TRUNC` and can truncate the live retry's already-fsynced
     temp inode between its write and its rename -- publishing a partial
     file behind a committed record. Identical content does not save
     this: `O_TRUNC` destroys completed bytes. Use `O_CREAT|O_EXCL` so a
     collision fails loudly.
  2. **DELETE's re-check + unlink must be uncancellable as a unit.** If
     the stripe guard lives in the request future while the unlink runs
     in `spawn_blocking`, a cancelled DELETE drops the guard and its
     detached unlink lands later -- possibly after a new PUT's
     rename+insert under that stripe: committed record, file gone. Loss.
     Fix: acquire the stripe via `Arc<Mutex>::lock_owned` and move the
     `OwnedMutexGuard` *into* the blocking closure that performs the
     re-check (sync metastore read) and the unlink. General rule: a
     stripe guard must never be held by a cancellable future across an
     await whose detached continuation performs a destructive operation.
     Renames (constructive, idempotent) are exempt; unlinks are not.
- **No await between the blocking-write join and the record-insert tx
  commit** (also required by the `Send`-soundness rule above).
  Recommended stronger form: move the short tx into the same
  `spawn_blocking` closure as the rename -- rename+insert become
  uncancellable as a unit and the fjall journal fsync leaves the executor
  too (it currently runs on the calling thread, `fjall.rs:33-41`, which
  otherwise keeps one sync fsync per commit on a worker).
- **Cancelled DELETE between object-record removal and the block loop**:
  remaining blocks keep over-counted rcs -- leakage.
- **Cancelled multi-block PUT between blocks**: earlier blocks hold
  committed rc=1 records *with* files, referenced by no object.

Residue classes and their collectors (this replaces the previous
revision's "every failure mode degrades to an invisible orphan", which
was imprecise):
1. temp files -> cleared at store open;
2. file without record (orphan) -> ADR 0005 orphan sweep, or healed by
   rename-over on re-upload;
3. record+file without a referencing object (rc over-count) -> ADR 0005
   refcount reconciliation (recount from live objects).
All three are invisible leakage. None is client-visible loss.

### key_has_block and same-key overwrite (DECIDED 2026-07-31: drop the skip)

Today `store_object` suppresses the dedup bump when the destination key's
*old* object already contains the block (`write_path.rs:123-127`,
`meta_store.rs:636-659`), based on a snapshot read taken outside all
locking (`write_path.rs:78-82` -- one stale read per multipart part, over
a long upload). Combined with delete's per-occurrence decrement this
under-counts rc and frees live blocks: e.g. old object at K has block B
(rc=2 with another key K2); a new multipart upload to K contains B in two
parts; both skip the bump; delete of K decrements twice, removes the
record and unlinks -- K2's object dangles. Loss-shaped, and stripes do
not fix it: the stale predicate, not the RMW, is the bug.

**Decision: drop the skip -- always bump under the stripe.** Same-key
overwrite already leaks the replaced object's refcounts today
(`create_object_meta` blind-upserts, `fs.rs:246-258`, decrementing
nothing), so dropping the skip converts a loss-class under-count into
more of the leak-class over-count that ADR 0005's recount already
reconciles. The proper pairing -- overwrite decrements the replaced
object's blocks through this ADR's striped delete primitive -- is
follow-up work recorded in ADR 0005/0003 scope, not smuggled in here.

---

## Architecture Overview

### Component Breakdown

1. **Stripe set** (`cas-storage/src/cas/`, new module)
   - `Stripes { locks: Vec<tokio::sync::Mutex<()>> }`,
     `fn for_hash(&BlockId) -> &Mutex<()>` (plus an owned-guard variant
     for the delete closure). Lives on `SharedBlockStore` so all
     namespaces of one store share it. Correctness preconditions the
     review surfaced:
     - at most one `SharedBlockStore` per on-disk store per process:
       `CasFS::single_namespace` mints a *private* one per call
       (`fs.rs:177-183`) -- a process must never open one store through
       it twice;
     - block *files* must be rooted per shared store, not per `CasFS`:
       today paths derive from each instance's `fs_root`
       (`write_path.rs:181`, `read_path.rs:33`) and nothing ties
       namespaces to one root -- divergent roots would break
       cross-namespace dedup into dangling reads. Move the blocks root
       (and `blocks/.tmp`) onto `SharedBlockStore`, or assert same-root
       in `CasFS::new`.
   - Sizing: spurious serialization per writer is `(K-1)/N` for K
     concurrent distinct-block writers (K=64: ~6%, K=256: ~22% at
     N=1024), tail-latency only. Make N a config knob; a sane rule is
     ~16x the max concurrent block writers (the blocking pool caps those
     at 512). 8192 mutexes cost well under 1 MB.
2. **Atomic file writer** (replaces the body of the `AsyncFileSystem` seam)
   - temp-write (`O_CREAT|O_EXCL`) / file fsync / fanout-dir fsyncs /
     rename / parent-dir fsync via `spawn_blocking`; fsyncs gated by
     `Durability` (`Buffer` skips all; `Fdatasync` still uses full fsync
     for directories). Directory durability is a chain: fsync of the
     final parent persists only its own new dentry, so every directory
     `create_dir_all` newly created must be fsynced deepest-up until an
     ancestor already known durable (keep an in-process known-durable
     cache, seeded empty at open). At store open: create and fsync
     `blocks/`, `blocks/.tmp`, and the parent of `blocks/` once.
   - Open-time same-device check: after creating `blocks/.tmp`, compare
     `st_dev` of `blocks/` and `blocks/.tmp` and refuse to start on
     mismatch (catches btrfs subvolumes; optionally verify with a probe
     rename). Runtime `EXDEV` is a store-level fault, logged loudly.
   - The mocking seam for `test_store_object_write_failure` is preserved;
     the trait finally gets an honest name.
3. **Write path** (`cas-storage/src/cas/write_path.rs`)
   - The per-block closure reorders to the protocol above; the
     `BlockWriteGuard` metrics state machine survives with the same
     states. `cleanup_on_failure` is deleted outright (no record precedes
     the file, so there is nothing to compensate). The commit
     `.unwrap()`s at `write_path.rs:157/167` are replaced with error
     mapping *as a prerequisite* -- the new protocol adds commit sites.
     The `key_has_block` skip is dropped per the decision above.
4. **Delete path** (`cas-storage/src/cas/delete_path.rs`)
   - Namespace-DB tx (read+remove object record), then the per-block
     striped decrement/unlink loop with the owned guard inside the
     blocking closure. Unlink tolerates ENOENT and never aborts the loop
     (replaces the `.expect("Could not delete file")` panic at
     `delete_path.rs:18`, which today kills the connection task *after*
     metadata removal, leaking the remaining blocks). `_PATHS`
     maintenance (today `delete_path.rs:19-25`) retires with the
     full-id layout.
     `bucket_delete` keeps its inherited ordering (bucket meta removed
     before object teardown, `delete_path.rs:33-34`); its mid-loop
     failure residue (invisible half-deleted bucket) goes to ADR 0005's
     fixture zoo.
5. **Temp hygiene**
   - `blocks/.tmp/` is cleared on store open and ignored by walkers.
     Open-time-only clearing is mandatory (a runtime sweeper would race
     live temps). Exclusivity caveat: fjall's LOCK file guards only the
     *meta* DB; nothing locks the blocks file tree, so two processes
     sharing `fs_root` with different meta roots could sweep each other's
     temps (under the new ordering that is an availability failure, not
     loss). Depend on ADR 0004's store-root flock, or take an interim
     flock on `blocks/` at open.

---

## Alternatives Considered

### Compensating delete (keep meta-first, undo on failure)
- **The idea**: on disk-write failure, remove the just-committed record in
  a follow-up operation.
- **Status: this is the shipped code**, not a hypothetical
  (`cleanup_on_failure`, `write_path.rs:184-195`) -- and it is itself the
  sharpest loss bug in Context defect 1: the unconditional remove races a
  concurrent dedup bump and deletes an rc=2 record another object
  references. A corrected version (decrement-or-remove-if-rc==1 in one
  tx) would fix only defect 1; partial-file visibility and the durability
  hole remain, because metadata still commits before bytes.
- **Bets on**: defects 2, 3, 5 never mattering. Falsified in review.

### Two-phase records (pending -> finalize)
- **The idea**: commit a `pending` record, write the file, commit a
  `finalize`; failures leave a self-describing pending record.
- **Sharpest tradeoff**: two transactions per block on the hot path, and
  readers must treat `pending` as absent -- a read-path change; and it
  still commits intent before bytes, so the durability ordering still
  needs the file fsync anyway.
- **Bets on**: the intent-log visibility being worth the tx traffic. With
  content-addressed idempotent files, the intent log records nothing the
  filesystem does not already express.

### Global write serialization (hold the lock over a sync write)
- **The idea**: pre-fix ordering with a sync write under the guard.
- **Sharpest tradeoff**: one block write per disk latency, store-wide.
- Rejected, as it was in the original fix.

### Sharding fjall instead (per-stripe keyspaces)
- **The idea**: eliminate the single-writer bottleneck by sharding the
  metadata DB itself.
- **Sharpest tradeoff**: cross-shard consistency for object records that
  span blocks; an on-disk layout change with migration; solves a
  bottleneck nobody has measured. The short-tx design keeps guard hold
  times in microseconds.

### Keeping the _PATHS prefix allocation (rejected with the path decision)
- **The idea**: retain the per-insert shortest-free-prefix paths and make
  the protocol safe around them with three added rules: a claim-tx
  allocating the path *before* the disk write (new crash residue: a
  claimed entry with no record and no file, a new ADR 0005 sweep class);
  path entries of zeroed blocks freed only inside the striped delete
  section (freed earlier, a different hash claims the prefix and the
  pending unlink destroys its live file); dedup re-inserts reusing the
  vanished record's path.
- **Why rejected**: every rule exists only to simulate what
  self-identifying file names give structurally, and it adds a
  crash-residue class. With no deployed data there is no migration cost
  to trade against, and the decided scheme keeps the adaptive fanout;
  shorter file names were the only remaining benefit.

### Fixed two-level fanout (first deterministic draft, superseded)
- **The idea**: `blocks/<b0>/<b1>/<full-hex id>`, depth always 2 -- the
  path fully derivable from the id alone, no stored field, no probing.
- **Why superseded**: the adaptive-depth scheme keeps small stores
  shallow and directory sizes adaptively bounded (the current design's
  VFS virtue) at the cost of a one-byte depth field in the record and a
  bounded probe on insert (at most id-width stats; in practice the
  depth of the existing dir chain). Both are sound; the owner chose
  adaptive.

### O_TMPFILE + linkat (moved here from the open decisions: rejected)
- **The idea**: anonymous temp inodes, no temp names, no `.tmp` sweep.
- **Why rejected**: `linkat` fails `EEXIST` on an existing target and
  Linux has no link-with-replace; orphan heal *requires* rename-over
  (below), so O_TMPFILE needs an unlink-then-linkat fallback whose
  no-file window is reader-visible (a GET can hit ENOENT that atomic
  rename-over structurally cannot produce). Plus Linux-only, per-fs
  support gaps forcing a named-temp fallback to exist anyway. Named temp
  + fsync + rename-over is one portable code path with strictly stronger
  visibility guarantees.

---

## Consequences

### Positive
- All five defects close at once; every failure mode degrades to
  invisible *leakage* (the three residue classes in Cancellation, each
  with a collector), never a client-visible dangling record, partial
  read, or phantom-durable record -- with the Buffer power-loss caveat
  stated in Invariants.
- Orphans self-heal: a retried upload of the same content renames over
  the orphan and inserts the record it lacked.
- `Durability` becomes true end-to-end at `Fsync`/`Fdatasync`. On
  `fjall_notx` the file fsyncs still follow the configured level even
  though the backend ignores it for metadata -- safe direction: a file
  more durable than its record can only yield orphans.
- Block-file I/O leaves the executor threads. (The fjall journal fsync
  stays on-thread at commit unless the tx moves into the blocking
  closure, as recommended in Cancellation.) The deadlock remains
  structurally impossible: single acyclic lock order, no sync guard
  across awaits.
- Fixes the verified (not "latent") notx refcount races for PUT-vs-PUT
  and, with the striped decrement, PUT-vs-DELETE -- both loss paths in
  today's code.

### Negative
- Per-new-block fsync latency at `Fsync`/`Fdatasync` (real cost, correct
  cost -- it was being skipped, not saved). Dir-fsyncs amortize via the
  known-durable cache; dedup hits fsync nothing.
- A stripe is held across a block's disk write: concurrent writers of
  the *same* block serialize (correct and required); distinct blocks
  collide spuriously at `(K-1)/N` per writer -- tail latency, tunable
  via N.
- Stripe hold time includes blocking-pool *queue* wait, not just I/O:
  tokio's blocking pool bounds threads (default 512) but its queue is
  unbounded; the effective in-flight bound is the number of concurrent
  block writes, and cancelled PUTs' detached tasks still occupy slots.
  A queue-depth gauge is required, not optional.
- `Buffer` keeps today's performance (zero added fsyncs) and today's
  *crash* residue class (power loss can dangle records); its
  ordinary-failure residue is strictly smaller than today's. It is not
  "byte-for-byte today's behavior" -- ordering, temp files, and residue
  shapes all change.
- notx scope limit (DECIDED 2026-07-31): `fjall_notx` is scoped out of
  the loss-never guarantee rather than growing a key-stripe. The striped
  protocol still closes its practical races (PUT-vs-PUT bump,
  PUT-vs-DELETE decrement), but concurrent DELETEs of one key remain
  unserializable there (no tx for the object-record read+remove), and
  its metadata is never durable (the Durability knob is ignored). The
  loss-never contract is guaranteed on the transactional backend only;
  the notx startup warning (`shared_block_store.rs:62-69`) must state
  that scope explicitly.
- More moving parts in the write path: stripes, temp dir, two-step
  delete, dir-fsync cache.

### Risks
- rename atomicity requires temp and final path on one filesystem:
  enforced mechanically by the open-time `st_dev` check (component 2),
  not by documentation alone.
- Multi-stripe operations must never hold two stripes at once; the
  per-block loop never does. Any future batched variant must acquire in
  sorted-hash order or one at a time. Likewise nothing may ever acquire
  a stripe while holding the fjall guard -- fjall stays the leaf lock.
- The `Send`-soundness of `FjallTransaction` (`fjall.rs:222-252`) makes
  the no-await-inside-tx rule a memory-safety requirement; implementation
  and review must treat any await between tx begin and commit as UB-class,
  not style.

---

## What an Expert Would Ask

**Q: The stripe is an async mutex held across disk I/O -- have you just
rebuilt the original deadlock with nicer types?**
A: No. The original deadlock needed a *sync* lock (parks OS threads) plus
a bounded worker pool, with the holder itself suspended at an await. An
async mutex parks *tasks*: workers stay free, wakers fire on release, and
the only sync lock -- the fjall guard -- is acquired strictly inside
awaitless sections and released before any await. DELETE's namespace tx
takes fjall with no stripe held, which keeps fjall a leaf lock; no task
holds fjall and waits on a stripe, so no cycle exists. The failure mode
of a stuck disk becomes stuck requests on that stripe, not a stuck
runtime.

**Q: What happens when the process crashes between rename and the record
insert?**
A: An orphan file at its final content address -- residue class 2:
invisible to reads, reclaimed by ADR 0005's sweep, or healed by any
future upload of the same content. A cancelled multi-block PUT leaves
class 3 (records+files with no object), reclaimed by refcount recount.
Loss is impossible in either; leakage is bounded and collectable.

**Q: Doesn't fsync-per-block destroy write throughput?**
A: At `Fsync` durability it costs what durability costs -- one file fsync
plus amortized dir fsyncs per *new* block (dedup hits fsync nothing).
Today that cost is not saved, it is silently skipped while the config
claims otherwise -- and the review confirmed the power-cut loss it
implies, at the default setting. `Buffer` keeps today's performance for
users who chose speed, with the power-loss caveat now stated instead of
implied away. If measured throughput at `Fsync` is unacceptable, batching
dir fsyncs is the tuning knob, not dropping the file fsync.

**Q: Why is the dedup-hit refcount bump safe against a concurrent delete
zeroing the same block?**
A: Because *every* rc mutation -- bump, insert, decrement, remove -- runs
under the same stripe, and the check-and-mutate is a single transactional
RMW. (The previous revision claimed this while its own DELETE pseudocode
ran the decrement outside the stripes; the review caught the
contradiction, and the striped decrement is the fix, not a better
argument for the old shape.) Under the stripe, either the bump commits
first (the delete sees rc=2->1, no unlink) or the removal commits first
(the PUT sees no record and takes the insert path). The unlink shares the
decrement's stripe hold, so no tail case remains. This is the invariant
the stress tests must pin.

**Q: Does anything still need `AsyncFileSystem` to be sync?**
A: No -- that constraint died when the lock stopped spanning I/O. The seam
survives for test injection only; its implementation moves to
`spawn_blocking` internally, and the trait gets renamed to match reality.

---

## Implementation Plan

### Decisions locked (previously open)
- **Path scheme**: full-id filenames at adaptive fanout depth, decided
  2026-07-31 (owner's scheme; supersedes the fixed two-level draft).
  `_PATHS` and the prefix allocator are removed; the record's path
  field becomes a one-byte depth; a file's name identifies its block.
  No migration -- no deployed store carries data.
- **key_has_block**: skip dropped, decided 2026-07-31 -- every dedup hit
  bumps rc under the stripe. Converts the silent loss-class under-count
  (multipart double-occurrence, stale-snapshot skip) into the existing
  leak-class over-count that ADR 0005's recount reconciles; the
  overwrite-decrements-replaced-blocks pairing stays follow-up work.
- **notx scope**: `fjall_notx` scoped out of the loss-never guarantee,
  decided 2026-07-31 -- no key-stripe. Stripes still fix its bump and
  decrement races; the double-DELETE-same-key window and the absent
  metadata durability are accepted and must be named in the startup
  warning and docs. The backend's overall fate is ADR 0007's question
  (removal proposed there).
- **Landing order**: ADR 0006 lands before ADR 0005, decided
  2026-07-31. Fsck cannot detect the rc undercounts defects 1 and 5
  produce (an undercount looks internally consistent), so
  reconciliation cannot substitute for this fix; 0005 then reconciles
  the strictly smaller post-0006 failure zoo.
- **Stripe placement on `SharedBlockStore`**: confirmed (review ask 1 of
  the previous revision), with the two new preconditions in component 1
  (no double-open via `single_namespace`; blocks root bound to the shared
  store).
- **Durability-gated fsyncs**: gated, `Buffer` skips all -- with the
  Buffer power-loss caveat documented in Invariants/Consequences, and
  gating defined for notx (files follow the configured level).
- **Temp mechanism**: portable named temps, `O_CREAT|O_EXCL`, mandatory
  nonce. O_TMPFILE rejected (see Alternatives).
- **Orphan heal**: rename-over unconditionally. A corrupt orphan
  (pre-ADR partial write at a final path, or bitrot -- both in the threat
  model; `verify_on_read` exists for the latter) *must* be replaced, and
  compare-and-skip would publish it as loss; verification costs a full
  read+hash and can never beat a sequential write of bytes already in
  memory. Readers holding the replaced inode's fd stream identical bytes.
- **Same-device**: `st_dev` check at open, fail loudly; runtime EXDEV is
  a store fault.

### Known unknowns, resolved
- notx refcount races: **confirmed with citations**, both PUT-vs-PUT
  (unlocked get-then-insert, `fjall_notx.rs:161-180`) and the decrement
  side (`meta_store.rs:343-415`, both backends). The striped protocol is
  the fix; the finding goes in the commit message.
- Multipart: `complete_multipart_upload` performs **no block writes and
  no rc operations** (`s3fs.rs:104-195`) -- everything block-shaped flows
  through `upload_part` -> `store_object`, so the stripes cover multipart
  for free. Its real hazard was the `key_has_block` stale-skip (decided
  above). ADR 0003's part-GC decrement must use this ADR's striped delete
  primitive.
- Blocking-pool sizing: default pool, add the queue-depth gauge; pivot to
  a dedicated bounded pool if the gauge shows starvation.

### The mechanical work
Prerequisites first: replace the commit `.unwrap()`s
(`write_path.rs:157/167`) with error mapping; fix the
`buffered_byte_stream.rs:58` comparison bug (compares `self.buffer.len()`
where `bytes.len()` is meant, letting >1 MiB blocks through -- the new
writer assumes blocks <= BLOCK_SIZE).

Then: stripe module + placement (with owned-guard API); atomic file
writer with the full durability chain and open-time checks; write_path
reorder (guard states unchanged, cleanup deleted, skip dropped);
delete_path split with guard-in-closure and panic removal; `.tmp` cleanup
on open; the path layout (remove `_PATHS` and the allocator; the path
field becomes a one-byte depth; full-id filenames with insert-time
probe); rename `AsyncFileSystem`.

Tests: race stress -- PUT-vs-DELETE same block, dedup-bump-vs-delete,
double-DELETE same key, PUT-vs-PUT same block on both backends;
cancellation fixtures -- detached-rename-after-drop (orphan only),
guarded unlink (no detached destructive op); crash-window fixtures for
ADR 0005 (orphan, temp residue, Buffer dangling record); fsync gating
per durability level; rc-exactness
tests must account for the pre-existing key-overwrite leak (or land the
overwrite-decrement follow-up first); an end-to-end concurrent-PUT
benchmark to quantify the executor win and measure `(K-1)/N` in practice.

Review asks: none -- all resolved (path scheme, key_has_block, notx
scope, landing order; see Decisions locked above).

---

## Open Questions

**Behavior definers**
- [ ] Should the stripe also serialize verify-on-read's re-hash against
      concurrent writes, or is read-side locking still rejected? (Review
      note: rename-over installs byte-identical content, so a heal race
      cannot fail verification; the remaining argument for read-side
      locking is thin.)
