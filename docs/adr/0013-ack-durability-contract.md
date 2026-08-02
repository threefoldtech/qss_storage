# The ack durability contract: which 200s promise an fsync

**Status**: Accepted (2026-08-02, on the owner's instruction; implemented
same day, this document folded to as-built before the acceptance flip)
**Date**: 2026-08-02

---

## Context

The per-ack persist fix (a9b1587, the ADR 0011 rider) made every bare,
acknowledgement-carrying metadata write call `persist(<configured
durability>)` before returning: `CreateBucket`, `CreateMultipartUpload`,
`UploadPart`'s ETag, respd's `SET`/`DEL`. Its reason was real and stands:
fjall persists bare keyspace writes at `PersistMode::Buffer` internally --
kernel-visible, never fsynced -- so at `durability = fsync` those acks were
power-loss-vulnerable (docs/upstream/fjall-journal-ack-visibility.md,
Finding 1, regression-tested).

Two things changed since it shipped:

1. **Its price was measured.** The 2026-08-02 campaign re-baseline:
   K=6 parallel ingest fell from 2244 to 1009 MiB/s at fsync (-55%) and
   from 3224 to 928 at buffer (-71%), while single streams were flat.
   The cost is a convoy at fjall's single mutex-serialized journal
   writer: one persist per part-ack, dozens of concurrent requests, and
   throughput becomes the ticket line, not the disk. Group commit does
   not refund it at high K (769 MiB/s, worse).
2. **Its urgency evaporated.** The kill -9 "losses" that made every ack
   feel loss-shaped were traced to an aws-cli false acknowledgement
   (campaign report 2026-08-02, captured live as fsync-2/mp-80). Nothing
   the store acknowledged was ever lost across a kill, before or after
   the fix. What remains is exactly the power-loss gap -- no more, no
   less.

Which forces the question the fix answered wholesale: **which
acknowledgements actually promise durability at the configured level?**
S3's own semantics answer it per ack: some losses are LOUD (a later,
mandatory operation fails and the client recovers), some are SILENT
(nothing ever tells anyone). Paying the strongest guarantee for acks
whose loss is loud buys nothing the protocol does not already provide.

Related: ADR 0006 (loss-never for acknowledged objects), ADR 0010 (sync
boundary at the ack for the transactional paths), ADR 0011 (group
commit), ADR 0005 (fsck's leak-vs-loss vocabulary).

---

## Decision

Split acknowledged bare writes into two classes, by TREE, and persist
accordingly:

**Contract class (persist at configured durability, unchanged)** --
writes whose loss after an ack is silent or terminal:

| ack | write | why strict |
| --- | --- | --- |
| PUT 200 | object record (bucket tree) | loss is silent: nothing revisits it |
| CompleteMultipartUpload 200 | claim tx + object record | same, and it is THE multipart ack |
| CreateBucket 200 | bucket metadata | rare enough to be free; a vanished bucket orphans its config |
| respd SET / DEL reply | namespace tree | RESP has no later operation that would surface the loss |

**Recoverable class (no explicit persist; fjall's internal
`PersistMode::Buffer` write applies)** -- writes whose loss after an ack
is loud, caught by a mandatory later step of the same protocol:

| ack | write | what catches a power-loss |
| --- | --- | --- |
| UploadPart 200 (ETag) | part record, `_MULTIPART_PARTS` | the complete fails InvalidPart; client re-uploads the part |
| CreateMultipartUpload 200 | upload marker, `_UPLOADS` | the next upload-part fails NoSuchUpload; client restarts the upload |

Both classes remain kernel-visible before the ack (fjall's internal
Buffer-level persist, pinned by the ack-visibility regression tests), so
the kill -9 guarantee -- every daemon-acknowledged write survives process
death -- is identical for both. The classes differ only against power
loss, and only where the protocol already shouts.

The policy is keyed by tree name at `get_tree` time: `_MULTIPART_PARTS`
and `_UPLOADS` construct their `FjallTree` in the recoverable class,
every other tree in the contract class. Transactional paths
(`commit_persist`) are untouched: always configured durability.

---

## Architecture Overview

One component changes and one harness function rides along:

1. **`FjallStore::get_tree` / `FjallTree`**
   (`cas-storage/src/metastore/stores/fjall.rs`)
   - `FjallTree` gains an `AckPersist` field (`Contract` | `Recoverable`),
     chosen by tree name in `get_tree`/`get_tree_ext`.
   - `insert`/`remove` persist only in the `Contract` class. The
     `Recoverable` class relies on fjall's internal Buffer persist --
     already regression-pinned as kernel-visible.
   - Doc comments carry the two-class table; the durability tests pin
     BOTH classes (contract tree persists at configured mode; parts tree
     does not fsync but its bytes are journal-visible at return).

2. **`crash_multipart_worker`** (`tests/real/lib/crash.sh`, harness, not
   this ADR's subject but its acceptance depends on it): the mp ack is
   now evidence of the complete -- `s3api complete-multipart-upload`'s
   returned ETag -- never a client tool's exit code (the aws-cli false
   ack, campaign 2026-08-02). Without this the campaign cannot grade
   this ADR: the false ack manufactures "losses" no store can prevent.

Data flow is unchanged; only the persist call after two tree names'
bare writes disappears.

---

## Alternatives Considered

### Keep the fix wholesale (status quo)
- **The idea**: every ack-carrying write persists at configured durability.
- **Optimizes for**: the simplest possible statement -- "every 200 is fsynced".
- **Sharpest tradeoff**: 55-71% of parallel ingest, paid mostly for acks
  whose loss the protocol already surfaces.
- **Bets on**: throughput not mattering, or group commit refunding it
  (measured: it does not at high K).

### Revert a9b1587 entirely
- **The idea**: bare writes go back to fjall's internal Buffer level,
  everywhere.
- **Optimizes for**: maximum throughput, minimum code.
- **Sharpest tradeoff**: respd's SET reply becomes a silent power-loss
  liar at fsync durability, and the object-record... is transactional,
  so actually only respd and CreateBucket are exposed -- but exposed
  silently.
- **Bets on**: nobody running respd with a durability promise. That bet
  is wrong by construction: durability is a store-level setting respd
  advertises.

### Coalesced ack-persist (extend the ADR 0011 commit station to bare writes)
- **The idea**: bare persists join the group-commit station; many acks
  share one fsync.
- **Optimizes for**: keeping the wholesale contract AND the throughput.
- **Sharpest tradeoff**: couples every small ack's latency to strangers'
  fsyncs; the station itself showed a -24% regression and an unexplained
  latency debt at high K in the same campaign.
- **Bets on**: the recoverable class someday needing strictness. If that
  day comes, this is the alternative that revives.

### A config knob (per-level or per-tree operator override)
- **The idea**: ship both behaviors, let deployments choose.
- **Optimizes for**: not deciding.
- **Sharpest tradeoff**: the contract stops being a contract; every
  operator re-derives this ADR's table, some wrongly.
- **Bets on**: deployments that genuinely differ in which acks matter.
  None known; revive only when one shows up.

---

## Consequences

### Positive
- The K=6 convoy's per-part fsync disappears; parallel multipart ingest
  should return to the ADR 0010 baseline (acceptance measures it).
- The durability promise becomes an explicit, documented table instead
  of an implicit "everything" that was quietly costing half the disk.
- respd's contract is stated and kept, not accidental.

### Negative
- A power cut can now take acknowledged part records and upload markers
  (kill -9 still cannot). The loss is loud and recoverable by protocol,
  but it IS a weaker statement than a9b1587 made, and clients that
  treat an UploadPart 200 as archival-grade would be wrong -- S3's own
  semantics never promised them that.
- Two persist classes to keep straight in review; the tree-name keying
  concentrates it in one match.

### Risks
- If the -71% at buffer was NOT this persist call (it is nearly free at
  buffer level, so the attribution is partly inference), relaxing will
  not recover buffer throughput. The acceptance bench is the detector;
  if buffer K=6 stays near 928, the regression hunt reopens against
  0011/0012's shared code, and this ADR's perf rationale shrinks to the
  fsync case (which is directly measured and safe).
- Recount interactions: a part record lost to power loss leaves its
  blocks' refcounts over-counted (holders = bucket trees +
  `_MULTIPART_PARTS`); that is leak-class, fsck recount collects it.
  Verified against ADR 0005's closed-holder-set rule -- no repair path
  reads a part record it cannot see.

---

## What an Expert Would Ask

**Q: A power cut lands after CompleteMultipartUpload acked but the part
records it claimed were only kernel-visible. Is the completed object
whole?**
A: Yes. The complete's claim tx and object record persist at configured
durability (contract class), and the object's BLOCKS were made durable
by each part's transactional block-batch commit (ADR 0010, untouched).
Post-complete, part records are dead weight -- they were removed by the
claim; their earlier volatility is irrelevant to the object.

**Q: Power cut between a part's block commit (durable) and its part
record (volatile). What is left?**
A: Durable blocks whose refcounts count a part-holder that no longer
exists: an over-count. Leak class, exactly ADR 0005's recount case; the
client's complete fails loudly (InvalidPart) and the re-uploaded part
dedups onto the same blocks. Loss is not reachable this way.

**Q: Why is CreateBucket strict when its loss is arguably loud
(NoSuchBucket on the next PUT)?**
A: Frequency, not principle. Bucket creation is rare enough that the
persist is free, and a vanished bucket takes its metadata (auth,
config) with it, which is closer to silent than a part record. Strict
costs nothing; relaxing it buys nothing. If the table ever gets a
second look, this is the row most arguable either way.

**Q: The recoverable class leans on "fjall internally persists bare
writes at Buffer". That is undocumented upstream behavior -- what if
fjall 3.2 changes it?**
A: The ack-visibility regression tests pin exactly that boundary (ack a
write, read the journal file, assert the bytes). A fjall upgrade that
weakens it fails the suite before it ships; the recoverable class then
needs an explicit `persist(Buffer)` call, a one-line restoration.

**Q: How does the campaign grade this, given kill -9 cannot distinguish
the classes?**
A: It cannot and should not: both classes are kill-safe by the same
mechanism, and the crash matrix keeps proving that. The class split is
a power-loss distinction; grading it honestly needs the power-loss rig
(the owner's tiered-testing direction, ADR pending). Until that rig
exists, the contract table plus the regression tests ARE the grade.

---

## Implementation Plan

### Decisions you will probably want to tweak
- **Choice**: policy keyed by tree NAME (`_MULTIPART_PARTS`, `_UPLOADS`)
  inside `get_tree`. **Alternative**: policy as a `get_tree` parameter
  chosen by callers. **Cost to change later**: trivial (one match moves);
  name-keying was chosen because the two trees are already named
  constants in the same file and no caller has an opinion.
- **Choice**: recoverable class makes NO persist call. **Alternative**:
  explicit `persist(PersistMode::Buffer)` for symmetry. **Cost to
  change**: one line; skipped because fjall's internal persist is pinned
  by tests and an extra flush per part is precisely the convoy being
  removed (measured at buffer: the redundant call is not free).

### Known unknowns and how the plan absorbs them
- Buffer-level K=6 recovery is predicted, not guaranteed (see Risks).
  Default: attribute to the convoy. Pivot signal: post-implementation
  bench stays near 928 at buffer -> reopen the hunt against 0011/0012.

### The mechanical work
`AckPersist` enum + field on `FjallTree`; match in
`get_tree`/`get_tree_ext`; durability tests split into contract/
recoverable pins; doc comments updated to carry the table; harness
`crash_multipart_worker` acks on the complete's returned ETag (s3api),
keeping the QSSRT_CRASH_MP_LOG audit fields.

Review asks (all answered by the owner's standing instruction, recorded
here for the record): (1) the two-row recoverable set -- confirmed;
(2) respd strict -- confirmed; (3) acceptance = clean campaign + K=6
bench recovery at fsync -- confirmed.

---

## Open Questions

**Behavior definers**
- [ ] Should the store LOG the class split at startup (one line stating
  which trees are recoverable), so an operator reading "fsync" in the
  config learns the table exists? Default: yes, one INFO line.
