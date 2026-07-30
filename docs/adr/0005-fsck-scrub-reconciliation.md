# fsck: Offline Reconciliation and Scrub for the Block Store

**Status**: Proposed
**Date**: 2026-07-30

---

## Refcount invariant (context)

The store's integrity story is asymmetric by design (`docs/refcount.md`):
refcounts may over-count ("leakage") but must never under-count ("loss").
Several code paths deliberately choose leakage on failure, and the
write path commits metadata before block files hit disk (the deadlock fix,
`docs/arch/deadlock-fix.md`), so metadata referencing a never-written block
is a legal crash state. Verify-on-read (ADR 0002) catches corruption at
read time, opt-in, whole-object reads only.

What is missing is the reconciliation half: nothing ever recounts,
reclaims, or repairs. Every accepted-leakage path accumulates forever:

- blocks whose file write failed after the metadata commit (dangling
  block records);
- block files on disk that no record references (orphan files);
- refcounts left elevated by failed or abandoned writes, including every
  abandoned multipart part (ADR 0003 stops the bleeding going forward;
  existing stores keep their accumulated leaks);
- silent corruption in block files that verify-on-read only finds if and
  when someone reads that object whole.

There is prior art in-tree to build on: the header-aware `inspect` tooling
already opens the same DBs the server writes, and `s3cas check` walks
objects. Neither mutates anything.

---

## Decision

Proposed, pending review: a `fsck` subcommand (working name; lives next to
`inspect`) that opens the store exclusively (the ADR 0004 lock) and runs
some or all of these passes:

1. **Refcount recount**: rebuild expected refcounts by walking all object
   records (and, with ADR 0003, part records of live uploads); compare to
   the block tree; report diffs. Elevated counts are the expected leak
   direction; a count *below* the walked references is a loss-invariant
   violation and reported as CRITICAL.
2. **Orphan file sweep**: walk `blocks/` on disk; report files with no
   block record.
3. **Dangling record sweep**: walk block records; report records whose
   file is missing (the crash-window state) or whose size mismatches.
4. **Corruption scrub**: re-hash block files against their address (the
   offline, whole-store complement to verify-on-read).
5. **Stale multipart report**: count/age of incomplete uploads (reaping
   itself belongs to ADR 0003's abort path, which fsck may invoke).

Report-only by default. `--repair` applies the safe subset: set refcounts
to recounted values, delete orphan files, delete dangling records (their
data is already gone), quarantine corrupt blocks (rename aside, never
silently delete -- the object referencing them is damaged either way and
must keep failing loudly).

---

## Architecture Overview

### Component Breakdown

1. **Walkers** (`cas-storage`, new `scrub` module)
   - Object walker (all namespaces), block-record walker, disk walker.
     Library code, not CLI code: respd stores must be checkable too.
2. **Pass engine**
   - Each pass = walker(s) + a diff + a `Finding`; findings carry severity
     (INFO leak / WARN inconsistency / CRITICAL loss-invariant breach).
3. **Repair actions**
   - One type per finding kind with an `apply(&store)`; `--repair` maps
     findings to actions; corruption maps to quarantine only.
4. **CLI** (`s3cas fsck` and/or a shared binary -- see open questions)
   - Text + `--json` output; nonzero exit on WARN+ for scripting.

### Data Flow

```
open store (exclusive lock, ADR 0004)
  -> walk objects ----> expected refcounts --+
  -> walk block tree -> actual refcounts  ---+-> diff -> findings
  -> walk blocks/ dir -> orphans / dangling / sizes
  -> rehash files ----> corruption findings          (--scrub)
findings -> report | --repair -> actions -> re-verify pass
```

---

## Alternatives Considered

### Online scrub inside the daemon (background, rate-limited)
- **The idea**: continuous low-priority scrubbing while serving traffic,
  like ZFS scrub or garage's repair workers.
- **Optimizes for**: zero-downtime hygiene on long-running stores.
- **Sharpest tradeoff**: every pass must now be safe against concurrent
  mutation of the very structures it reconciles; the refcount recount
  becomes incremental bookkeeping instead of a trivially correct offline
  walk. Large complexity multiplier on the part that must be exactly
  right.
- **Bets on**: stores too hot to ever take offline. Not today's
  deployments. The offline walkers are the necessary first step either
  way; online mode can wrap them later.

### Fold everything into verify-on-read plus ADR 0003's GC
- **The idea**: read-time verification catches corruption, upload GC stops
  the biggest leak; skip the tool.
- **Optimizes for**: no new tool.
- **Sharpest tradeoff**: cold data is never read, so never verified;
  historical leaks are never reclaimed; the loss invariant is never
  actually checked against reality.
- **Bets on**: leaks staying small and corruption only mattering for hot
  data. The second half is false by definition for archival use.

### Repair-by-default (no report-only mode)
- **The idea**: fsck fixes what it finds, like journal replay.
- **Optimizes for**: one-command operation.
- **Sharpest tradeoff**: a bug in a repair action becomes a data-eating
  bug on first contact with a damaged store -- precisely when trust is
  lowest.
- **Bets on**: repair actions being correct on day one. Report-first is
  the storage-tool norm (fsck -n, zpool scrub then status) for a reason.

---

## Consequences

### Positive
- The loss invariant becomes checkable instead of aspirational; every
  accepted-leakage decision in the codebase gains its missing other half.
- Operators get a disk-usage truth tool (how much of `blocks/` is live).

### Negative
- Offline: store unavailable for the duration (see expert Q on runtime).
- A repair tool is new surface that can itself destroy data; it demands
  the highest test rigor in the workspace (fault-injected stores as
  fixtures).

### Risks
- Quarantine of a shared (deduped) corrupt block affects every object
  referencing it; the report must enumerate affected objects so the
  operator sees the blast radius before repairing.
- Walk cost on huge stores; mitigated by per-pass selection (recount
  without scrub is metadata-only and fast).

---

## What an Expert Would Ask

**Q: How long does a full scrub take on a real store, and what does that
do to availability?**
A: Metadata passes are bounded by fjall iteration (fast, millions of
records). The corruption scrub is disk-bound: ~storage size / sequential
read rate (a 4 TB store at 500 MB/s is ~2.5 hours offline). Acceptable for
now; the pivot signal is a store whose owner cannot take that window --
which revives the online-scrub alternative, incremental, on the same
walkers.

**Q: Can `--repair` violate the loss invariant itself?**
A: The dangerous action is lowering a refcount. The recount lowers counts
only to the value derived from walking every reference holder while
holding exclusive access -- there is no concurrent writer to race. The
remaining risk is a walker missing a reference class (e.g. forgetting
part records); mitigated by the walkers being shared library code with the
write paths' tests pinning them, and by re-running the recount pass after
repair and requiring a clean diff.

**Q: What does fsck do with a CRITICAL finding (actual under-count)?**
A: Report and refuse to auto-repair. An under-count means a bug elsewhere;
"fixing" the number hides the bug. The finding carries enough context
(block, holders found) to file against the write path.

**Q: Old stores predating ADR 0003 have leaked multipart state in the old
ambiguous key format -- can fsck even parse it?**
A: It does not parse it: any record in the multipart tree not reachable
from a live upload record (which old-format records cannot be, the tree
predates upload records) is by definition stale and its blocks flow into
the recount as non-references. That is the migration story for historic
leaks.

---

## Implementation Plan

### Decisions you will probably want to tweak
- **Where the CLI lives**: `s3cas fsck` vs a store-level binary usable for
  respd stores too. Choice: store-level home in cas-storage with thin
  subcommand wrappers in both daemons. Alternative: s3cas-only first.
  Cost to change later: operator-facing command names.
- **Finding/severity model and --json schema**: this is the tool's API for
  scripting; renaming fields later breaks automation. Decide the schema in
  review, not in code.
- **Quarantine mechanics**: rename into `blocks/.quarantine/` preserving
  path, vs a quarantine tree in the DB. Choice: filesystem rename (visible
  with ls, survives DB damage). Cost to change: low until documented.

### Known unknowns and how the plan absorbs them
- Whether object walking across *all* respd namespaces via the shared
  library covers every reference class (respd inlines aggressively;
  inline objects hold no block refs -- verify, do not assume). Default:
  enumerate reference classes in one place with a unit test that fails
  when a new ObjectData variant appears.
- Store sizes in the wild: if metadata passes turn out slow at real scale,
  add per-bucket scoping before reaching for incrementalism.

### The mechanical work
Walkers + findings + passes in cas-storage; fault-injection fixtures
(orphan file, dangling record, inflated/deflated refcount, bit-flipped
block); CLI wiring + JSON output; exclusive-open integration (needs ADR
0004's lock first); docs page pairing it with verify-on-read.

Review asks:
1. Store-level tool (works for respd stores) rather than s3cas-only --
   agreed?
2. Report-only default with explicit `--repair` -- agreed?
3. Quarantine-never-delete for corrupt blocks -- agreed?
4. Is CRITICAL-refuses-auto-repair the right call, or should
   `--repair --force` exist day one?

---

## Open Questions

**Architecture-changers**
- [ ] Does online/incremental scrub have a real near-term customer, or is
      offline-only acceptable for the first year?

**Behavior definers**
- [ ] Exit-code contract: nonzero on WARN, or only on CRITICAL?
- [ ] Should fsck reap stale uploads itself (invoking ADR 0003's abort) or
      only report them?
