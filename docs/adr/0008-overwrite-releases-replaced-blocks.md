# Overwrite Releases the Replaced Object's Blocks

**Status**: Proposed
**Date**: 2026-08-01

---

## Context

Overwriting a key leaks the replaced object's refcounts, by design and
routinely. `create_object_meta` blind-upserts the object record: the old
record's block references are simply forgotten, never decremented. ADR
0006 made this worse on purpose -- dropping the `key_has_block` skip
means every dedup hit bumps, so a same-content re-PUT of a key grows rc
with each write -- because the alternative was a loss-shaped
under-count, and leakage had a collector (fsck's recount) while loss had
nothing.

That trade was correct then and is obsolete now. The two pieces it was
waiting for both landed:

- **`release_blocks`** (ADR 0003 component 3): the striped, per-
  occurrence, never-aborting decrement loop, factored out of
  `delete_object` as a named primitive. Overwrite-decrement needed
  exactly this and it did not exist.
- **The atomic take pattern**: `take_object` (ADR 0006) and its
  descendants give the read-and-replace-in-one-tx shape the overwrite
  needs to know *what* it replaced.

What the leak costs today: an overwrite-heavy workload (any
keep-latest-under-a-fixed-key pattern -- logs, checkpoints, state
snapshots) grows `blocks/` without bound between fsck runs, and fsck is
offline. The recount reconciles the numbers but reclaiming the space
costs an availability window. The 0006-series rc-exactness tests carry
an explicit allowance for this leak ("account for the pre-existing
key-overwrite leak"); this ADR deletes the allowance.

Affected write paths (everything that writes an object record over a
possibly-existing key):

- `create_object_meta` -- called by `put_object`, by
  `complete_multipart_upload`, and by `copy_object`'s destination write;
- `store_inlined_object` -- an inline overwrite of a block-backed object
  must release the old blocks too (the new record holds no references
  at all);
- respd's `set` -- always inline over inline, no references on either
  side: covered by the same code path, no-op in practice.

Related: ADR 0003 (release_blocks, the claim patterns), ADR 0005 (the
recount that today collects this leak; its expectations tighten), ADR
0006 (the bump-always rule that makes overwrite rc-arithmetic exact
once release lands), `docs/refcount.md` (counting rule: one reference
per occurrence).

---

## Decision

Proposed, pending review: **every object-record write that replaces an
existing record releases the replaced record's blocks**, in this order:

```
one namespace-DB tx: read old record (if any); insert new record; commit
then, if an old record existed and held blocks:
    release_blocks(old.blocks())        # striped, per occurrence
```

The read-and-replace is one transaction (a `replace_object` sibling of
`take_object`): the tx returns the displaced record, so concurrent
overwrites of one key serialize under fjall's single-writer tx and each
writer releases exactly the record it displaced. No writer ever
releases a record another writer displaced; no displaced record is
released twice.

**The ordering is load-bearing, same argument as ADR 0003's abort**:
the new record commits before the old references are released. A crash
between the two leaves the old blocks over-counted -- leakage, INFO,
the recount collects it. The reverse order could release references
while the old record is still the visible one: a reader holding the old
block list races the unlink -- loss-shaped. New-commits-first keeps
every failure in the leak direction.

**Dedup arithmetic stays exact** when old and new share blocks: the new
write bumped every shared block (every dedup hit bumps, ADR 0006), the
release then decrements the old occurrences -- net effect per shared
block is unchanged rc, per dropped block is -1, per added block is +1.
Exactly the truth.

**What this does NOT change**: `delete_object` (already releases),
multipart part records (their references transfer at complete, ADR
0003), and the recount's role as backstop -- crash residue still leaks
and fsck still collects it. The change is that *successful* overwrites
stop leaking.

### Interaction with fsck and the tests

- The rc-exactness allowance for the overwrite leak is deleted from the
  0006-series tests; overwrite becomes rc-exact and the race tests can
  assert it (overwrite storms: N overwrites of one key leave exactly
  the final object's occurrences).
- ADR 0005's Context named the overwrite leak the recount's routine
  diet; after this ADR the recount's steady-state findings on a healthy
  store shrink toward zero. fsck text updates accordingly (one line).

---

## Alternatives Considered

### Keep the leak, rely on fsck (status quo)
- **The idea**: the recount already reconciles; run fsck on a schedule.
- **Optimizes for**: zero write-path change.
- **Sharpest tradeoff**: unbounded growth between offline windows for a
  routine workload shape; the collector costs availability. A storage
  system that needs scheduled downtime to not grow without bound on
  overwrites is mis-designed, and now needlessly so.
- **Bets on**: overwrite-light workloads. Not a bet worth carrying once
  the fix is one tx shape plus one existing primitive.

### Deferred release queue (write a tombstone, release in background)
- **The idea**: overwrite enqueues the displaced block list; a
  background task releases.
- **Optimizes for**: taking the release latency off the PUT path.
- **Sharpest tradeoff**: a new persistent queue with its own crash
  states and its own reconciliation story -- the exact complexity class
  this codebase keeps declining. The release is a handful of striped
  RMWs; the PUT already paid a disk write per new block.
- **Bets on**: release latency mattering. Measure first; the queue can
  wrap `release_blocks` later without changing the tx shape.

### Release inside the same transaction
- **The idea**: decrement the old blocks in the replace tx itself.
- **Sharpest tradeoff**: impossible as stated -- block records live in
  the shared DB, object records in the namespace DB, no tx spans them
  (ADR 0006's foundational constraint); and rc mutations must happen
  under stripes, which are never held inside a tx (lock order).
- Rejected structurally; recorded because it looks attractive for
  exactly one minute.

---

## Consequences

### Positive
- Overwrites stop leaking: steady-state `blocks/` size tracks live
  data. The last routine leak class with no online collector closes.
- rc becomes exact for the entire object lifecycle (write, overwrite,
  delete); the test allowance dies; fsck findings on a healthy store
  approach zero.

### Negative
- Overwrite PUTs gain the release cost: one striped RMW per displaced
  occurrence (and an unlink for blocks reaching zero). This is the same
  cost a DELETE of the old object would have paid; the overwrite was
  getting it for free by leaking.
- `replace_object` is new tx surface beside `take_object` -- small, and
  shaped identically.

### Risks
- Overwrite racing a reader of the old object: the reader resolved its
  block list from the old record; the release can zero and unlink a
  block mid-read. This is EXACTLY the delete-vs-reader race that
  already exists and is already accepted (POSIX fd semantics keep an
  opened stream alive; an open-after-unlink fails loudly). No new
  window -- but the equivalence must be stated in code comment and
  verified by a race test.
- copy_object with source == destination (self-copy overwrite): the
  displaced record's blocks are the source's blocks; the new record
  bumped them as dedup hits; net rc unchanged. Verify with a test, not
  an argument.

---

## What an Expert Would Ask

**Q: Two concurrent PUTs overwrite the same key -- who releases what?**
A: Each replace tx displaces exactly one record: the one visible when
its tx ran. fjall's single-writer serializes the txs; writer A
displaces the original and releases it, writer B displaces A's record
and releases that. Every record is released exactly once by exactly
the writer that displaced it. The final state is B's object at exact
rc. The storm test pins this.

**Q: PUT-overwrite racing DELETE of the same key?**
A: Serialized by the same namespace-DB txs. DELETE takes the record
(take_object) and releases; PUT's replace sees no old record (nothing
to release) and inserts fresh -- or the orders swap and PUT displaces
first, DELETE then takes PUT's record. Either interleaving releases
each record once. No new analysis: this is take/replace symmetry.

**Q: Why not batch the release for multipart completes (potentially
thousands of displaced occurrences)?**
A: `release_blocks` already iterates; the cost is proportional to the
displaced object's size, paid once, off the client's critical path only
if made async -- which is the deferred-queue alternative, rejected
until measured. A complete that displaces a huge object pays a delete's
cost; that is the honest price of the semantics.

**Q: Does this change what fsck's recount may assume?**
A: No assumption changes -- the recount walks truth regardless. What
changes is expectation: steady-state over-counts stop being routine.
The dangling/orphan machinery is untouched.

---

## Implementation Plan

### Decisions you will probably want to tweak
- **Release placement**: synchronous after the replace commit (chosen)
  vs deferred queue. Cost to change: the tx shape survives either; the
  queue is an additive wrapper later.
- **`replace_object` shape**: returns `Option<Object>` displaced
  (chosen, take_object symmetry). Alternative: insert-returning-old in
  the backend trait. Cost: trait surface.

### Known unknowns and how the plan absorbs them
- Whether any caller depends on overwrite NOT touching old blocks
  (nothing found in-tree; respd inline-only verified). The exhaustive
  caller audit of create_object_meta/store_inlined_object is step one
  of the work, before any behavior changes.

### The mechanical work
`Transaction::replace_object`; wire into create_object_meta and
store_inlined_object (audit all callers first); release after commit;
delete the rc-exactness allowances and tighten those tests; race
arms: overwrite storm same key, overwrite-vs-delete, overwrite-vs-read
(old-reader equivalence), self-copy; fsck doc line; refcount.md
counting-rule note (overwrite now releases).

Review asks:
1. Apply to both `create_object_meta` and `store_inlined_object`
   (inline-over-block-backed releases too) -- agree?
2. New-record-commits-first, release-after ordering -- agree?
3. Synchronous release (no deferred queue until measured) -- agree?

---

## Open Questions

**Behavior definers**
- [ ] Should `copy_object` onto self be short-circuited (no-op) instead
      of replace+release with net-zero rc? Semantically identical;
      short-circuit skips pointless work but adds a special case.
