# fsck: Offline Reconciliation and Scrub for the Block Store

**Status**: Proposed
**Date**: 2026-07-30
**Updated**: 2026-07-31 (revised against the as-built ADR 0006 file-first
protocol and the ADR 0007 backend removal; the original draft predates
both and described a residue zoo that no longer exists. Same day, owner
sign-off on all four review asks: degraded flag as record v3;
under-counts raised and still reported CRITICAL; exit codes 0/1/2/3;
store-level binary, not an s3cas subcommand. Decision-complete;
Accepted on implementation start)

---

## Context

The integrity contract (`docs/refcount.md`) is asymmetric by design:
refcounts may over-count ("leakage") but must never under-count ("loss").
Since ADR 0006 that contract holds unconditionally at run time -- one
metadata backend (ADR 0007 removed the non-transactional one), every
block-record mutation a transactional RMW under the block's stripe, block
files durable at their final path before their record commits. What is
still missing is the reconciliation half: nothing ever recounts, reclaims,
or repairs, so every accepted-leakage path accumulates forever.

The original draft of this ADR enumerated the pre-0006 residue zoo. That
zoo is gone. What accumulates now:

- **The same-key overwrite leak -- routine, not crash-only.**
  `create_object_meta` blind-upserts the object record and decrements
  nothing for the replaced object, and ADR 0006 dropped the
  `key_has_block` skip, so every dedup hit bumps. Every overwrite
  therefore leaks the replaced object's refcounts *by design* until the
  overwrite-decrement follow-up lands. This changes fsck's job
  description: the recount is routine reclamation for a leak that grows
  with normal use, not just disaster repair.
- **Cancellation residue** (ADR 0006's enumerated classes): a cancelled
  multi-block PUT leaves committed rc=1 records with files and no
  referencing object; a cancelled DELETE between object-record removal
  and the block loop leaves over-counted rcs; a crash between rename and
  record insert leaves an orphan file at its final content address.
- **Off-depth duplicates**: a file named `<id>` at a fanout depth the
  live record does not name -- unreferenced residue of placement-policy
  drift or an interrupted insert probe.
- **Half-deleted buckets**: `bucket_delete` removes the bucket meta
  before object teardown; a crash mid-loop strands an invisible object
  tree whose refcounts still hold.
- **Buffer-mode dangling records -- the one loss-shaped residue.** At
  `durability = "buffer"` nothing is fsynced, so a power cut can persist
  a record whose file's pages were lost. Choosing Buffer accepts that
  residue (ADR 0006), but an unrepaired dangling record actively
  *poisons dedup*: every future same-content PUT bumps the fileless
  record, skips its write, and commits another damaged object. Left
  alone it propagates; fsck is the designated collector.
- **Silent corruption**: verify-on-read (ADR 0002) is opt-in, whole-object
  reads only -- cold data is never verified by anyone.

Equally important is what fsck no longer has to handle:

- "Metadata referencing a never-written block is a legal crash state" --
  inverted by 0006. At `Fsync`/`Fdatasync` a dangling record now
  indicates Buffer history, foreign interference, or a bug; it is never
  routine.
- The `_PATHS` tree and every path-map pass: gone with the full-id
  layout. **Every file in `blocks/` names its own block** (single
  hex-byte fanout dirs, full-hex filename), so the disk walker attributes
  every file without a DB lookup and the corruption scrub re-hashes
  against the filename alone.
- Per-backend branches: one backend (ADR 0007).
- Temp files: `blocks/.tmp` is purged wholesale at store open
  (`block_disk.rs` open duties); fsck opens the store, so the purge has
  already run. Not a pass.
- The pre-QSST migration story: pre-QSST stores refuse to open (ADR
  0002) and no deployed store carries data. The original draft's
  old-multipart-format reasoning is void.

Assets in-tree to build on: `crash_fixtures.rs` (residue constructors
with pinning tests -- orphan file, off-depth duplicate, tmp residue,
dangling record -- built by 0006 component 9 *for this ADR*); the pure
`block_disk_path(id, depth)` (a dangling check is one stat per record);
the typed block iterator (`BlockTree::iter_all`); the header-aware
`open_existing` discipline of `inspect.rs`/`check.rs` (refuse to create,
refuse a store this build cannot read).

Store layout the passes operate on (per `inspect.rs`): one `--meta-root`
holds the namespace DB (`<meta_root>/db`, bucket trees) and the shared
blocks DB (`<meta_root>/blocks/db`, trees `_BLOCKS` and
`_MULTIPART_PARTS`); block data files live under `<fs_root>/blocks/`.

Related: ADR 0002 (addressing, headers, verify-on-read), ADR 0003
(multipart GC -- still Proposed; see the multipart pass), ADR 0004 (store
ownership -- still Proposed; no longer a hard dependency, see Decision),
ADR 0006 (the protocol whose residue this reconciles), ADR 0007 (one
backend), `docs/refcount.md`.

---

## Decision

A store-level `fsck` binary (DECIDED 2026-07-31: its own binary target,
home in cas-storage, usable against any store root -- not an s3cas
subcommand; working name `qss-storage-fsck`, naming per the qss_storage
convention). It opens the store's fjall DBs through the same
header-aware path as the daemon and the existing tools (never creates,
refuses on header mismatch).

**Exclusivity** is inherited, not built: fjall's LOCK file makes fsck and
a running daemon mutually exclusive on every DB fsck opens. The residual
hole -- a second process using a *different* meta root over the same
`blocks/` tree -- is ADR 0004's scope; fsck documents it and no longer
waits for it. (The original draft blocked on 0004's exclusive lock; the
fjall LOCK already covers every non-misconfigured deployment.)

**The closed-holder-set rule.** A recount is only meaningful over the
complete set of reference holders. fsck derives that set from the store
layout -- every bucket tree in the namespace DB, plus `_MULTIPART_PARTS`
in the shared DB -- and **refuses to run the recount (and any repair) if
any part of it fails to open or parse**. A recount over a partial holder
set that then "repairs" would free live blocks: loss by repair, the one
failure mode this tool must structurally exclude.

### Passes

1. **Refcount recount** (metadata-only, fast). Walk all holders, counting
   block references *per occurrence* (the counting rule of
   `docs/refcount.md`):
   - object records in every bucket tree, `ObjectData` matched
     exhaustively so a new variant fails compilation (`Inline` holds no
     refs; respd namespaces are verified inline-only --
     `respd/src/namespace.rs:244` always constructs `ObjectData::Inline`
     -- and the same exhaustive match pins that);
   - multipart part records -- **unconditionally**. With ADR 0003
     unimplemented, part records are the *only* reference holders of
     in-flight uploads; a recount that skipped them would reconcile a
     live upload's blocks to zero and free them. Loss by repair.
   Compare with `_BLOCKS`. Over-count: INFO (the expected direction --
   overwrite leak, cancellation residue). Under-count: CRITICAL.
2. **Disk sweep**. Walk `blocks/` (skipping `.tmp`). Every entry must be
   a single-hex-byte directory or a file named by a full-hex id of the
   store's width. Findings: orphan file (INFO), off-depth duplicate --
   a file whose id has a record at a different depth (INFO), foreign
   file, i.e. unparseable name (WARN), file-size vs record-size mismatch
   (CRITICAL -- a corruption tell that costs one stat, no read).
3. **Dangling-record sweep**. One stat per record via the pure
   `disk_path`. Before classifying, cross-reference against pass 2's
   orphans: a dangling record whose id exists as an orphan at another
   depth is repaired by **adoption** -- re-hash the orphan (fsck does not
   have the writer's bytes, so unlike insert-time heal it must verify
   before trusting), then point the record's depth at it. The finding
   pair collapses to one repaired INFO. An un-adoptable dangling record
   is CRITICAL: the bytes are gone, every holder object is damaged, and
   the record poisons future same-content PUTs (see the damaged-block
   decision below).
4. **Corruption scrub** (`--scrub`, disk-bound). Re-hash every block file
   against its own filename; verify record size. Self-attributing: no DB
   lookup per file. The offline, whole-store complement to
   verify-on-read.
5. **Multipart report**. Count and age of `_MULTIPART_PARTS` records,
   grouped by upload. **Report-only until ADR 0003 lands**: reaping
   requires 0003's abort semantics (which decrements through the striped
   delete primitive), and guessing them here would smuggle 0003 in.
6. **Bucket integrity**. Object trees in the namespace DB with no bucket
   meta (the `bucket_delete` crash residue): WARN; under `--repair`,
   resume the teardown, then let the closing recount reconcile.

Report-only by default. `--repair` applies:

- **rc := recounted value, in both directions** (DECIDED 2026-07-31).
  Lowering to the walked truth is safe under exclusivity. Raising an
  under-count is the conservative direction and defuses a live
  premature-free landmine -- the CRITICAL finding, its evidence, and
  the nonzero exit all remain, so the bug it indicates is not hidden.
  (The original draft refused to touch under-counts; leaving a known
  under-count in place leaves loss armed.)
- delete orphan files and off-depth duplicates (nothing references
  them);
- quarantine, never delete: corrupt blocks and foreign files rename into
  `blocks/.quarantine/` (full-id names stay self-identifying; a
  filesystem quarantine is visible with `ls` and survives DB damage);
- adoption for dangling-record/orphan pairs (hash-verified, above);
- resume half-deleted bucket teardowns;
- after all actions: **re-run the recount; anything but a clean diff is
  CRITICAL** and exits nonzero.

### Damaged blocks: heal vs accounting

Dangling records and quarantined-corrupt blocks share a dilemma the
pre-0006 draft never faced. The data is already lost; the question is
what the record should say afterwards.

- *Remove the record* and dedup heals: a future same-content PUT takes
  the insert path and writes the file fresh. But the surviving holder
  objects still reference the id, so the healed block's rc=1 undercounts
  them -- when the new object is deleted, the block is freed while old
  holders reference it. An object that read fine yesterday (incidentally
  healed) breaks tomorrow with no operation on it. That converts
  documented damage into silent future breakage: worse than staying
  broken.
- *Keep the record* and accounting stays honest, but the record keeps
  poisoning every future same-content PUT into a new damaged object.
  The loss propagates.

**Chosen mechanism (DECIDED 2026-07-31): a `degraded` flag on the block
record** (record format v3 adds a flags byte). fsck's repair marks damaged records
degraded instead of removing them; the write path's bump RMW treats a
degraded record as absent-for-dedup -- it falls through to the insert
path, writes and fsyncs the file, then *clears the flag and bumps* in
the same striped tx. Holders heal permanently, rc never lies, poisoning
stops. Cost: a format bump (free today -- no deployed stores, the same
stance ADR 0006 took) and one branch in the bump path, inside the
existing tx and stripe. This is the only piece of this ADR that touches
the daemon.

The rejected fallback -- `--repair --evict-damaged`, exporting every
holder object's metadata into the report and then removing holders and
record -- was accounting-correct and heal-enabling without a format
change, but it destroys metadata naming what was lost, and a
multi-block object dies whole for one damaged block. Eviction can still
be added later as an operator policy on top of the flag; the reverse
order would have left early damaged stores poisoned until the flag
shipped.

---

## Architecture Overview

### Component Breakdown

1. **Walkers** (`cas-storage`, new `scrub` module)
   - Holder walker (the one place reference classes are enumerated;
     exhaustive `ObjectData` match), block-record walker
     (`BlockTree::iter_all`), disk walker (structure-validating).
     Library code: the daemons' stores must be checkable by the same
     code the CLI uses, and a future online mode wraps these.
2. **Pass engine**
   - `Finding { severity, class, ids, evidence }`; passes are functions
     over shared walker outputs -- one record walk powers both the
     recount diff and the dangling sweep; the disk walk powers orphan,
     off-depth, foreign, and size findings in one traversal.
   - Severities: INFO (expected leakage), WARN (inconsistency), CRITICAL
     (loss or loss-risk: under-count, un-adoptable dangling record,
     corruption, partial holder set, dirty post-repair recount).
3. **Repair actions**
   - One type per finding class with `apply(&store)`; every action
     idempotent (rc-set, ENOENT-tolerant deletes and renames,
     idempotent degraded-marking), so a crashed `--repair` is re-run,
     not recovered. The report file is written before repair begins.
4. **CLI** (store-level binary, DECIDED 2026-07-31)
   - Its own binary target homed in cas-storage (working name
     `qss-storage-fsck`), `--meta-root`/`--fs-root` plus the
     StoreOptions merge the other tools use; `[--scrub] [--repair]
     [--json]`. Usable against any store root regardless of which
     daemon owns it (respd stores verifiably hold no blocks today, but
     the tool does not care who wrote the store). Exit codes (DECIDED
     2026-07-31): 0 clean or INFO-only, 1 WARN, 2 CRITICAL, 3
     could-not-run.

### Data Flow

```
open store (header-aware, never creates; fjall LOCK excludes the daemon)
  -> enumerate holder set: bucket trees + _MULTIPART_PARTS
       (refuse recount/repair on partial enumeration)
  -> walk holders ------> expected rc per occurrence --+
  -> walk _BLOCKS ------> actual rc, depth ------------+-> diff -> findings
  -> stat disk_path per record -> dangling (cross-ref orphans -> adoption)
  -> walk blocks/ ------> orphans / off-depth / foreign / size mismatch
  -> rehash files vs their own names -> corruption findings     (--scrub)
findings -> report | --repair -> actions -> recount again, must be clean
```

---

## Alternatives Considered

### Online scrub inside the daemon (background, rate-limited)
- **The idea**: continuous low-priority scrubbing while serving traffic,
  like ZFS scrub or garage's repair workers.
- **Optimizes for**: zero-downtime hygiene on long-running stores.
- **Sharpest tradeoff**: every pass must be safe against concurrent
  mutation of the structures it reconciles. Post-0006 the stripes at
  least provide the per-block serialization primitive an online mode
  would hook, but the recount would still become incremental bookkeeping
  instead of a trivially correct offline walk.
- **Bets on**: stores too hot to ever take offline. Not today's
  deployments. The offline walkers are the necessary first step either
  way; online mode can wrap them later.

### Fold everything into verify-on-read plus ADR 0003's GC
- **The idea**: read-time verification catches corruption, upload GC
  stops the multipart leak; skip the tool.
- **Optimizes for**: no new tool.
- **Sharpest tradeoff**: cold data is never read, so never verified; and
  post-0006 the *overwrite* leak grows with every normal overwrite, not
  just with failures -- nothing in the read path or 0003 ever reclaims
  it, and nothing else stops a Buffer dangling record from poisoning
  dedup forever.
- **Bets on**: leaks staying small and corruption only mattering for hot
  data. The first half is now false by design, the second by definition
  for archival use.

### Repair-by-default (no report-only mode)
- **The idea**: fsck fixes what it finds, like journal replay.
- **Optimizes for**: one-command operation.
- **Sharpest tradeoff**: a bug in a repair action becomes a data-eating
  bug on first contact with a damaged store -- precisely when trust is
  lowest.
- **Bets on**: repair actions being correct on day one. Report-first is
  the storage-tool norm (fsck -n, zpool scrub then status) for a reason.

### Recount at every store open (daemon-side, no tool)
- **The idea**: the daemon runs the metadata passes at startup, the way
  it already purges `.tmp`.
- **Optimizes for**: zero operator involvement.
- **Sharpest tradeoff**: open becomes O(store metadata) on every
  restart, and repair policy (what to do with CRITICALs) gets hardcoded
  where no operator can see it.
- **Bets on**: stores staying small and repairs never needing judgment.
  The library split keeps this revivable: the daemon could later invoke
  cheap passes at open without a new design.

---

## Consequences

### Positive
- The loss invariant becomes checkable instead of aspirational; every
  accepted-leakage decision gains its missing other half.
- The overwrite leak -- which now grows with *normal use* -- gets its
  collector; without fsck, `blocks/` only ever grows.
- Buffer-mode dangling records stop propagating (dedup un-poisoning)
  instead of silently damaging every future same-content PUT.
- Operators get a disk-usage truth tool (how much of `blocks/` is live).

### Negative
- Offline: store unavailable for the duration (see expert Q on runtime).
- A repair tool is new surface that can itself destroy data; it demands
  the highest test rigor in the workspace. (Mitigation already landed:
  the `crash_fixtures.rs` constructors and their pinning tests.)
- The degraded flag touches the record format (v3) and adds one branch
  to the write path's bump RMW -- small, but it is daemon surface inside
  a tool ADR.

### Risks
- Quarantine or eviction of a shared (deduped) damaged block affects
  every object referencing it; the report must enumerate holders so the
  operator sees the blast radius before repairing.
- Walk cost on huge stores; mitigated by per-pass selection (the
  recount is metadata-only and fast; `--scrub` is the disk-bound one).
- The degraded-flag branch must not weaken the bump RMW: it stays
  inside the same tx and stripe, and the race stress tests must gain a
  degraded-record arm.

---

## What an Expert Would Ask

**Q: How long does a full scrub take on a real store, and what does that
do to availability?**
A: Metadata passes are bounded by fjall iteration (fast, millions of
records). The corruption scrub is disk-bound: ~storage size / sequential
read rate (a 4 TB store at 500 MB/s is ~2.5 hours offline). Acceptable
for now; the pivot signal is a store whose owner cannot take that window
-- which revives the online-scrub alternative, incremental, on the same
walkers, with the stripes as the serialization hook.

**Q: Can `--repair` itself violate the loss invariant?**
A: The dangerous action is lowering a refcount. The recount lowers only
to the value derived from walking *every* holder under exclusive access.
The two ways that walk can lie are a missed reference class -- excluded
by the single walker module with its exhaustive `ObjectData` match and
the closed-holder-set refusal -- and a stale view, excluded by the fjall
LOCK. Defense in depth: the post-repair recount must come back clean or
fsck exits CRITICAL.

**Q: Why does fsck count part records of possibly-abandoned uploads as
live references?**
A: Asymmetric costs. Counting a stale part's references preserves a leak
(INFO, reclaimed after ADR 0003's reaper runs). *Not* counting a live
part's references frees blocks a completing upload is about to
reference: loss. Staleness is a judgment ADR 0003 owns (its abort path
decrements through the striped delete primitive); until it lands, fsck
reports ages and touches nothing. Reap-then-recount is the only safe
order.

**Q: Why not just delete a dangling record and let dedup heal it?**
A: Because the surviving holders make the healed record's rc a lie, and
the lie breaks in the worst direction: the incidentally-healed object
reads fine until the new writer's delete frees the block, then breaks
again with no operation on it. Silent future breakage is strictly worse
than present, documented damage. The degraded flag exists precisely to
make heal-with-honest-accounting expressible.

**Q: What if fsck crashes mid-`--repair`?**
A: Every action is idempotent (set-rc, ENOENT-tolerant unlink and
rename, idempotent flag-set), the report is written before repair
starts, and the closing recount runs on the next invocation too: re-run
fsck, no recovery procedure. The fixture zoo must include a
kill-mid-repair test.

---

## Implementation Plan

### Decisions locked (owner sign-off 2026-07-31)
- **Damaged-block mechanism**: degraded flag, record format v3 (flags
  byte). The format bump is free until a deployed store exists (ADR
  0006's stance); eviction stays available later as policy on top.
- **Under-counts**: `--repair` raises rc to the recounted value; the
  CRITICAL finding and nonzero exit remain.
- **Exit-code contract**: 0 clean/INFO, 1 WARN, 2 CRITICAL, 3
  could-not-run. Scripting depends on it once shipped.
- **CLI home**: store-level binary in cas-storage (working name
  `qss-storage-fsck`), usable against any store root; no daemon
  subcommand wrappers to start with.

### Decisions you will probably want to tweak
- **Finding/severity model and `--json` schema**: the tool's scripting
  API; renaming fields later breaks automation. Decide the schema in
  implementation review, not in code.
- **Quarantine mechanics**: filesystem rename into `blocks/.quarantine/`
  (visible with `ls`, survives DB damage) vs a DB tree. Cost to change:
  low until documented.
- **Binary name**: `qss-storage-fsck` is a working name (qss_storage
  naming convention; never bare qss). Cost to change: operator-facing
  once documented.

### Known unknowns and how the plan absorbs them
- Real-store scale: per-pass selection is the first lever, per-bucket
  scoping the second, incrementalism last. Signal to pivot: metadata
  passes exceeding minutes on a real store.
- Whether the holder walk stays two classes (bucket trees, part
  records). The exhaustive match turns "a class was added" into a
  compile error, and the closed-holder-set refusal turns "a class failed
  to open" into a hard stop instead of a wrong repair.

### The mechanical work
`scrub` module in cas-storage (walkers, findings, passes, repair
actions); extend `crash_fixtures.rs` (inflated/deflated rc, bit-flipped
block, half-deleted bucket, stale part records, kill-mid-repair);
record format v3 with the degraded-flag branch in the bump RMW plus a
degraded arm in the race stress tests; the store-level binary target
and `--json`; docs page pairing fsck with verify-on-read and
`docs/refcount.md`.

Review asks: none -- all four resolved 2026-07-31 (see Decisions
locked).

---

## Open Questions

**Architecture-changers**
- [ ] Does online/incremental scrub have a real near-term customer, or
      is offline-only acceptable for the first year?

**Behavior definers**
- [ ] Ordering vs ADR 0003: if fsck ships first, the multipart pass is
      report-only and flips to reap-via-abort when 0003 lands --
      confirm that ordering is acceptable (the reverse order changes
      nothing here).
