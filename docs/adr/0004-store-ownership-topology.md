# Store Ownership: One Process Owns a Store

**Status**: Proposed
**Date**: 2026-07-30

---

## Context

The workspace ships two daemons (`s3cas`, `respd`) over one library
(`cas-storage`). "Shared library" invites the assumption that the two
daemons can share one data directory. They cannot: fjall is an in-process,
single-writer embedded database. Two processes opening the same keyspace is
not a supported fjall topology; behavior ranges from open failure to
corruption depending on timing, and nothing in our config validation, docs,
or startup path says anything about it.

Current state:

- `s3cas` opens its store via `CasFS::single_namespace`; `respd` builds its
  own `Storage` over the same library. Nothing stops both from being
  pointed at one `data_dir`.
- `SharedBlockStore` already exists as the *in-process* sharing seam
  (multiple namespaces over one block store inside one process). The
  cross-protocol story was designed for a single process; it was never a
  cross-process design.
- The QSST store header (ADR 0002) refuses wrong-format opens but says
  nothing about concurrent opens.

The decision needed: is "both protocols against one store" a topology we
support (by building a single owning process) or refuse (by enforcing
exclusive ownership at open)?

---

## Decision

Proposed, pending review -- two parts, one immediate and one directional:

1. **Immediate: exclusive ownership, enforced at open.** Opening a store
   takes an advisory `flock` on a `LOCK` file in the store root, held for
   the process lifetime. A second open of any kind (second daemon, an
   inspect tool in write mode, a concurrent fsck) fails fast with a clear
   error naming the holder (pid recorded in the lock file). Read-only
   tooling that can operate safely on a live store documents itself as
   such and skips the lock only when it genuinely never writes.
2. **Directional: multi-frontend is one process, not two.** If serving S3
   and RESP from one store becomes a requirement, the answer is a single
   daemon hosting both frontends over one `SharedBlockStore` -- not a
   cross-process coordination layer. This ADR records that direction so
   nobody builds file-level coordination.

---

## Architecture Overview

### Component Breakdown

1. **Store lock** (`cas-storage`, at `MetaStore::open_or_create` /
   `SharedBlockStore::new` level)
   - `LOCK` file in the store root; `flock(LOCK_EX | LOCK_NB)`; pid +
     process name written for diagnostics; released by process exit
     (advisory locks die with the fd, so crashes cannot wedge the store).
2. **Error surface**
   - New `MetaError::StoreLocked { holder_pid }`; both daemons print an
     operator-grade message.
3. **Tool audit** (`inspect`, benches, tests)
   - Each existing tool declares itself read-only-safe or takes the lock.
     `inspect` currently opens the same DBs the server writes -- it must
     take the lock (or a shared/read lock, see open questions).

### Data Flow

```
s3cas  --> open store --> flock(LOCK) ok --> serve
respd  --> open same  --> flock fails   --> "store owned by pid N (s3cas)"
```

---

## Alternatives Considered

### Do nothing, document the constraint
- **The idea**: a README warning that one store belongs to one process.
- **Optimizes for**: zero code.
- **Sharpest tradeoff**: the failure mode it leaves in place is silent
  corruption on an ops mistake, the worst class of failure a storage
  product can have.
- **Bets on**: every future operator reading the README. Rejected.

### Rely on fjall's own locking
- **The idea**: if fjall already locks its directory, ours is redundant.
- **Optimizes for**: no new code, engine-level correctness.
- **Sharpest tradeoff**: unverified; and even if fjall locks its keyspace,
  the store is more than fjall (block files on disk, the blocks/ tree) --
  a second process could still mutate block files while the first serves
  them. Verification of fjall's behavior is a prerequisite task, but the
  store-level lock is warranted regardless.
- **Bets on**: the engine's lock covering the whole store's integrity. It
  cannot, by construction.

### Build the unified multi-frontend daemon now
- **The idea**: one binary, S3 + RESP listeners, one SharedBlockStore.
- **Optimizes for**: the end-state topology; deletes the problem.
- **Sharpest tradeoff**: a product decision (deployment, config, naming,
  packaging) taken under the guise of a safety fix; weeks not hours.
- **Bets on**: actually needing both protocols on one dataset. Unproven --
  today's known deployments run them on separate stores. Kept as the
  recorded direction, not built now.

---

## Consequences

### Positive
- The corruption class "two processes, one store" becomes a startup error.
- The scrub tool (ADR 0005) gets its exclusivity mechanism for free.

### Negative
- Legitimate concurrent read-only tooling on a live store needs an explicit
  story (shared locks or documented unsafety) instead of just working by
  luck.

### Risks
- Advisory locks on network filesystems (NFS) are historically unreliable.
  Mitigation: document local-disk assumption; it already holds for fjall.
- Stale-looking lock files after `kill -9` confuse operators even though
  flock itself is released -- mitigated by making the error message trust
  flock, not file existence.

---

## What an Expert Would Ask

**Q: Does fjall already refuse a second opener?**
A: Unverified at time of writing; verifying it is the first task in the
plan. Even a yes only covers the metadata DB, not block files, so the
store-level lock stands either way. If yes, our error message can improve
on fjall's.

**Q: flock or fcntl/OFD locks?**
A: `flock`: inherited-fd semantics are simpler, it dies with the process,
and we do not need byte ranges. OFD locks add nothing for a whole-file
guard here.

**Q: What about the read paths of `inspect` on a live store?**
A: fjall does not promise external-process readers a consistent view.
Proposed: `inspect` takes the same exclusive lock by default and gains an
explicit `--live` flag that skips it, documented as best-effort. That makes
the unsafe thing spellable but deliberate.

**Q: Containers -- two containers can share a volume and defeat pid-based
diagnostics.**
A: flock still works across containers sharing a bind mount (same
filesystem); only the pid in the message may be meaningless outside the
namespace. Acceptable: the *refusal* is what matters, the pid is garnish.

---

## Implementation Plan

### Decisions you will probably want to tweak
- **Lock placement**: store root (`data_dir/LOCK`) covering blocks + all
  DBs as one unit. Alternative: per-DB locks. Cost to change later: none
  technically, but operator docs and muscle memory form around the path.
- **`inspect --live` escape hatch**: ship it or not. Alternative: inspect
  always locks (safest, least useful). Cost to change: none; behavior
  flag.

### Known unknowns and how the plan absorbs them
- fjall's own second-open behavior: verified by a two-process test first.
  Whatever it does, the outer lock ships; the finding only tunes error
  messages.
- Whether any existing workflow (benches, CI) opens a store twice
  concurrently and would newly fail: the two-process test suite will say.

### The mechanical work
Lock module in cas-storage (open, write pid, error type); wire into
`SharedBlockStore::new` and `MetaStore::open_or_create`; error rendering in
both daemons; two-process integration test (fork + expect refusal); tool
audit; README topology section stating the one-owner rule and the recorded
multi-frontend direction.

Review asks:
1. Confirm the directional call: multi-frontend = single process, never
   cross-process coordination -- agreed?
2. `inspect --live` escape hatch: yes/no?
3. Lock file name: `LOCK` in store root -- fine, or namespaced
   (`qss_storage.lock`)?

---

## Open Questions

**Architecture-changers**
- [ ] Is there a real deployment that needs S3 and RESP over the same
      dataset? (If yes, the unified daemon moves from "direction" to
      "roadmap" and deserves its own ADR.)

**Behavior definers**
- [ ] Should read-only opens take a shared flock (allowing N readers, no
      writer) instead of the binary exclusive/none proposed here?
