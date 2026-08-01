# Strangers Share a Flush: Cross-Request Group Commit

**Status**: Proposed
**Date**: 2026-08-01

---

## Context

ADR 0010 moved the sync boundary from the block to the ack, and the same-day
measurements say what that bought and where the next wall stands:

- 16 GiB single-stream A/B (home btrfs nvme, same rig as 0010's motivating
  table): fsync 62 -> 238 MB/s, buffer 1025 -> 2360 MB/s.
- /s3 xfs, 6 x 16 GiB parallel multipart: fsync 2244 MiB/s (the 0010
  projection of 400-800 exceeded ~3x), buffer 3224 MiB/s.
- The fsync run's iostat: the disk pinned at 92-95 percent utilization,
  sustaining ~1250-1350 flushes per second. The device is no longer waiting
  on us; it is busy -- and a third of what it is busy WITH is flushing.

So 0010 did its job: the ceiling moved from flush cadence to flush
bandwidth. What remains is flush COUNT. Every request still pays its own
closing: one blocks-DB transaction with one journal persist, plus its own
wave of block fdatasyncs and directory syncs. Two shapes still lose to that
arithmetic:

- **Small objects.** A 4 KiB PUT pays the same journal persist as a 64 MiB
  part. A thousand concurrent small writers are a thousand persists per
  second, serialized behind fjall's single-writer lock -- per-request
  overhead IS the workload.
- **Single streams.** One stream at fsync runs 238 MB/s against buffer's
  2360 on the same disk: a 10x gap that says one request's worth of
  amortization is all a lone stream can buy itself. (At K=6 the gap is
  1.4x -- concurrency already amortizes across the journal lock. The gap
  this ADR attacks lives at low K and small sizes.)

ADR 0010 deferred exactly this: "the cross-request timer merge (true group
commit) gets its own ADR when wanted; the batch API here is shaped so that
ADR is additive and touches no callers." This is that ADR.

Constraints inherited:

- ADR 0006 file-first ordering, ADR 0008 rc exactness, hard rules 1 and 6
  (stripes before fjall, fjall is the leaf lock) -- all unchanged.
- ADR 0010's cap: `max_blocks_per_commit` bounds one transaction, one
  stripe hold, and one kill's orphan residue. This ADR keeps that bound
  authoritative -- see the Decision.
- The owner's ruling for this ADR: the feature is OPTIONAL, config-gated,
  default OFF. Grouping strangers couples their fates and widens what one
  commit carries; an operator opts into that, it is not sprung on them.

---

## Decision

Add a per-store **commit station**: a single committer that merges the
closing step of CONCURRENT requests' batches into one transaction with one
journal persist, gated by `[store] group_commit` (default `false`, in which
case nothing changes and the write path is ADR 0010's, byte for byte).

With `group_commit = true`, a request's batch runs stages 1-2 of ADR 0010
unchanged (stage temp files as bytes arrive; fdatasync the batch
concurrently, no stripes). Then, instead of closing itself, the batch is
handed to the station and the request parks. The committer drains whatever
is queued, up to the SAME `max_blocks_per_commit` cap:

1. Take the union of the group's stripes, sorted by stripe index, one
   acquisition (the multi-stripe guard from 0010, at group width).
2. Re-check dedup decisions under the stripes (the 0010 as-built overtake
   rule), now also ACROSS members: two requests both staging new block X
   become one insert and one bump inside the one transaction; the loser's
   staged file is discarded, not renamed over.
3. Rename every member's staged files, fsync the union of touched fanout
   directories, each exactly once.
4. ONE transaction carrying every member's inserts and bumps, ONE persist
   at the store durability.
5. Release the stripes, wake every member: each acks its own client.

**Group formation is natural batching, not a mandatory timer.** While one
group commits, arriving batches queue; when the commit returns, the
committer takes everything queued (up to the cap) as the next group. An
uncontended request finds the committer idle and commits IMMEDIATELY --
group commit adds zero latency at zero load, by construction. The optional
`group_commit_window` (default `0`) is an extra bounded wait after the
first member arrives, for operators who want bigger groups than commit
duration alone accumulates; `0` means the timer does not exist.

**The cap is the cap.** A group never carries more than
`max_blocks_per_commit` blocks, the same bound a single large request
already fills under 0010. Bigger transactions was the stated worry, and the
answer is that the transaction does not get bigger -- its BOUND is
unchanged; grouping only lets strangers fill a cap that one small request
would have wasted. Queued batches beyond the cap form the next group.

**Failure isolation: a stranger's error never fails your ack.** If the
group transaction errors, it rolls back, and the committer degrades: each
member's batch is retried as its own ADR 0010-style transaction. One bad
member fails one request; the rest commit individually. The degradation is
per-group, not sticky -- the next group merges again.

---

## Architecture Overview

### Component Breakdown

1. **Commit station** (`cas-storage/src/cas/group_commit.rs`, new)
   - One per `SharedBlockStore`, built only when the config asks for it.
   - A queue of sealed batches (each: entries + staged files, data already
     synced) and one committer loop on a blocking thread.
   - Members park on oneshot channels; the committer completes them with
     their individual outcome after the persist.

2. **Write path handoff** (`cas-storage/src/cas/write_path.rs`)
   - `flush_batch` gains one branch: station present -> seal and enqueue;
     absent -> exactly today's close. Stages 1-2 (accumulate, stage,
     sync_batch) are untouched -- 0010 promised the batch API makes this
     ADR additive, and this is that promise kept.

3. **Group closer** (inside the station)
   - The 0010 close (lock_batch -> overtake re-check -> land_batch -> one
     tx -> persist) generalized from one batch to a vector of batches. The
     cross-member same-block merge happens here, with the same primitives
     (`bump_block_rc`, `insert_new_block`) so ADR 0008's exactness stays a
     property of the primitives.

### Data Flow

```
request A: chunk -> stage -> fdatasync xN --+
request B: chunk -> stage -> fdatasync xN --+--> station queue
request C: chunk -> stage -> fdatasync xN --+        |
                                                     v
                       committer: stripes(union, sorted)
                         -> re-check dedup (incl. cross-member)
                         -> rename all -> dirsync (union, once each)
                         -> ONE tx: everyone's inserts+bumps
                         -> ONE persist -> release -> wake A, B, C
                                                     |
                             A acks   B acks   C acks
```

---

## Alternatives Considered

### Status quo (ADR 0010 per-request close)
- **The idea**: ship nothing; 2244 MiB/s at K=6 is already past the
  projection.
- **Optimizes for**: simplicity; no new concurrency surface.
- **Sharpest tradeoff**: small objects and lone streams keep paying a full
  persist per request; ~1300 flushes/s of the device's attention stays
  spent on closings.
- **Bets on**: workloads staying big-object and concurrent. The moment a
  respd-style small-value flood or a single fat pipe shows up, the bet is
  lost.

### Mandatory window: every ack waits the timer
- **The idea**: classic group commit -- hold every ack up to T ms, merge
  whatever gathers.
- **Optimizes for**: maximum group size, maximum flush amortization.
- **Sharpest tradeoff**: p50 latency at low load inflates by T for no
  benefit -- a lone PUT waits on nobody for nothing.
- **Bets on**: throughput mattering more than tail latency for every
  operator. That is a workload opinion a storage engine should not impose,
  which is also why the whole feature is opt-in.

### Natural batching + optional window, config-gated, default off (chosen)
- **The idea**: merge only what concurrency already delivered (groups form
  during the previous commit); a timer only if the operator asks; the
  feature only if the operator asks.
- **Optimizes for**: zero-cost when idle, zero-risk when off, full
  amortization under load.
- **Sharpest tradeoff**: two write-path modes to keep correct (the station
  and the direct close), and group size at high device speed is limited by
  commit duration unless the window is set.
- **Bets on**: commit duration (rename + tx + persist, ~ms) being long
  enough to accumulate meaningful groups under real concurrency. Today's
  numbers say it is: 72 in-flight part uploads at K=6 against ~1300
  commits/s leaves several batches queued per commit.

### Raise `max_blocks_per_commit` instead
- **The idea**: bigger per-request batches amortize more without new
  machinery.
- **Optimizes for**: a config change.
- **Sharpest tradeoff**: helps only requests large enough to fill the cap;
  a 4 KiB PUT still pays a full persist, and residue/stripe-hold bounds
  grow for everyone.
- **Bets on**: the workload being large objects -- exactly the case that
  needs no help.

---

## Consequences

### Positive
- Small-object fsync throughput scales with concurrency instead of
  persist rate: N concurrent small PUTs approach one persist per group
  instead of one each.
- Lone-stream fsync ingest closes toward the buffer ceiling as its
  pipelined parts merge (aws-style clients run many part uploads in
  flight; those are exactly the strangers the station merges).
- Journal lock contention drops for every writer -- the buffer rows
  (1025 -> 2360 from 0010's tx batching alone) show tx count matters even
  with no fsync in the picture.
- Off by default: a store that never sets `group_commit` runs code
  identical to 0010's.

### Negative
- A second closing path to test and reason about; the station is new
  concurrency surface (queue, parking, wakeups).
- Coupled fates within a group: a member waits on strangers' renames and
  one shared persist; the degrade path bounds the damage but exists.
- Retained dedup bytes (0010 as-built) now pool across a group -- same
  cap-derived bound, but held in one place.

### Risks
- A committer stall (device hiccup mid-persist) stalls every parked
  request instead of one. Mitigation: the station never holds more than
  one group's stripes; parked members are cancel-safe (their guards and
  staged files are group-owned, cleaned on the degrade path).
- Subtle rc drift in the cross-member merge would be an ADR 0008
  regression. Mitigation: same primitives, property test at group width,
  and the campaign's refcount recount as backstop.
- The window knob invites cargo-cult tuning. Mitigation: default 0,
  documented as "leave it unless you measured".

---

## What an Expert Would Ask

**Q: What is the ack latency of a lone small PUT with group commit on?**
A: The same as with it off. An idle committer takes the solo batch
immediately -- natural batching only merges what was ALREADY waiting behind
an in-flight commit. Only a non-zero `group_commit_window` trades lone-ack
latency for group size, and it defaults to zero.

**Q: A group member's insert fails the shared transaction. Do five
innocent requests return 500?**
A: No. The group tx rolls back (nothing renamed is un-renamed -- renames
precede the tx exactly as in 0010, so the residue is orphan files, class
1), and each member replays as its own individual transaction. The bad
member fails alone with its own error; the rest ack. The cost of the
degrade is one wasted group attempt, not correctness.

**Q: Two requests in one group both carry new block X. Who wins, and where
does the loser's file go?**
A: Same answer as 0010's concurrent-batch question, moved inside the tx:
the committer's re-check under X's stripe sees no committed record, the
first member's entry inserts, the second's becomes a bump, and the
second's staged temp file is discarded -- never renamed over the winner,
never left as off-depth residue. Pinned by a property test at group width.

**Q: A kill -9 lands between the group's dirsync and its persist. What is
the residue?**
A: Up to `max_blocks_per_commit` orphan block files -- the SAME bound and
the SAME class as 0010, because the group is capped by the same knob. The
difference is provenance (the orphans belonged to several requests), which
fsck does not care about: class 1, swept and healed identically. No new
residue class exists because the ordering is 0010's ordering.

**Q: Does this interact with the open buffer-loss finding (the acked
record dying in fjall's userspace journal buffer)?**
A: It reduces journal write count at buffer durability, which narrows that
window as a side effect, but it is NOT the fix and must not be sold as
one. The fix (flushing the journal writer to the OS before ack at buffer)
is orthogonal and stays its own work item, corpse in hand.

**Q: Why a station per store and not per namespace?**
A: The blocks DB is the store-wide single-writer bottleneck -- one journal,
one lock, every namespace funnels into it. That is where merging pays.
Namespace DBs (object records; one more persist per PUT at fsync) are
per-bucket and lighter, and the station pattern extends to them -- deferred,
see Open Questions.

---

## Implementation Plan

### Decisions you will probably want to tweak
- **Config surface**: `[store] group_commit = false` and
  `group_commit_window = "0ms"` (humantime string, 0 disables the timer).
  - Alternative: a single tri-state knob, or auto-on above a concurrency
    threshold. Cost to change later: none, config only -- but auto-on
    contradicts the owner's opt-in ruling, so it needs a new ruling first.
- **Group bound**: reuse `max_blocks_per_commit` as the group cap.
  - Alternative: a separate `max_blocks_per_group`. Cost to change later:
    none (config), but two caps invite the "bigger transactions" problem
    back in through the second knob; one cap keeps the 0010 invariant
    "one number bounds tx, stripes, and residue" true.
- **Window semantics**: deadline measured from the FIRST member entering
  an empty queue; the group closes at deadline or cap, whichever first.
  - Alternative: rolling deadline (reset per arrival) -- groups grow
    without bound under steady load. Cost to change later: small, but the
    rolling variant is a latency footgun; first-entrant is the default
    worth defending.

### Known unknowns and how the plan absorbs them
- **Group sizes under natural batching on fast nvme**: assumed several
  batches per commit at real concurrency. Signal: the group-size histogram
  (new metric) pinned at 1 under load -> the window knob exists, and its
  documentation gains a measured recommendation.
- **Committer as convoy**: assumed one blocking-thread committer keeps the
  device saturated (it overlaps nothing -- but persists were serialized by
  fjall's lock anyway). Signal: fsync throughput at high K REGRESSING vs
  0010 with the station on -> allow k committers with disjoint stripe
  unions, which the sorted-union guard already permits.
- **Fairness / starvation**: FIFO queue assumed sufficient. Signal: p99
  ack latency of small members climbing under giant-heavy load -> cap the
  blocks one member may contribute to a mixed group (spill the giant to
  its own group).

### The mechanical work
`group_commit.rs` (station, queue, committer loop, oneshot completion);
the `flush_batch` branch; the group closer generalizing the 0010 close
over a batch vector; config plumbing (`config.rs`, `store_options.rs`,
example toml, both CLIs' help text untouched -- file-only knob like the
cap); metrics (group size histogram, persists per second); tests: lone
request latency (station on == off), stranger-failure isolation (one
poisoned member, N-1 acks), cross-member same-block property test, crash
fixture at group width asserting class-1-only residue, and the 16 GiB A/B
plus a new small-object A/B (4 KiB x N clients) as the regression pair.

### Review asks
1. Natural batching default with `group_commit_window = 0`, timer strictly
   opt-in: yes/no?
2. Reuse `max_blocks_per_commit` as the one group bound (no second cap):
   yes/no?
3. Degrade-to-individual-replay on group tx failure (stranger isolation
   over group atomicity): yes/no?
4. Scope: blocks DB only in this ADR, namespace DBs deferred: yes/no?

---

## Open Questions

**Architecture-changers**
- [ ] Does the station pattern extend to the namespace keyspaces (object
      records pay the second persist per PUT at fsync -- dominant for
      small objects)? Proposed: yes, as a follow-up amendment once the
      blocks station has numbers; the mechanism is identical, the
      keyspaces are many.

**Behavior definers**
- [ ] Should group commit merge at `buffer` durability too (tx merge, no
      persist -- lock contention is the only win)? Proposed: yes, the
      station is durability-agnostic and the 1025 -> 2360 buffer delta
      says tx count alone is worth it.
- [ ] Is the degrade path's individual replay allowed to re-enter the
      station (and possibly merge with a NEW group), or strictly direct?
      Proposed: strictly direct -- degraded means degraded, no second
      coupling on the retry.

**Polish**
- Config names `group_commit` / `group_commit_window`: proposed as
  written; renaming later is free until the first release ships them.
