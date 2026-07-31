# Multipart Upload Lifecycle Completion and Stale-Upload GC

**Status**: Proposed
**Date**: 2026-07-30
**Updated**: 2026-07-31 (revised against the as-built ADR 0005/0006/0007
world; the original draft predates the striped write/delete protocol,
the fsck machinery, and the notx removal, and its release_blocks and
race sections described machinery that now exists in different form)

---

## Context

The S3 multipart surface is half a lifecycle. `create_multipart_upload`,
`upload_part`, and `complete_multipart_upload` are implemented;
`abort_multipart_upload`, `list_parts`, and `list_multipart_uploads` are
absent from both `S3FS` and the `MetricFs` wrapper, so they fall through
to the s3s trait defaults (`NotImplemented`) and are invisible to
metrics end to end (`S3_API_METHODS` does not name them). There is no
garbage collection of any kind for uploads that are started and never
completed.

Consequences of the gap, as the code stands (verified at 48756fc):

- `create_multipart_upload` records nothing: the upload id is a UUID
  handed to the client (`s3fs.rs`), with no server-side record of the
  upload's existence or creation time. Nothing can be listed or aged.
- `upload_part` accepts ANY upload-id string and writes part records and
  block refcounts for it -- there is no existence check anywhere.
- `complete_multipart_upload` checks only per-part existence; a complete
  with an EMPTY parts list sails through and mints an empty object.
- Every part record of an abandoned upload stays in `_MULTIPART_PARTS`
  forever, and every block it references keeps its refcount forever.
  Post-ADR 0005 this leak is at least *visible* -- the fsck multipart
  pass reports per-upload part counts and bytes held, and the recount
  counts part records as holders -- but fsck deliberately reaps nothing:
  its pass doc says reaping belongs to this ADR's abort semantics.
- Standard clients (awscli, boto3, rclone) call `AbortMultipartUpload`
  on interrupted transfers as normal operation. Today that returns
  `NotImplemented` and the client gives up, leaving the leak in place.

Structural constraints, updated to the as-built code:

- Part records are keyed `{bucket}-{key}-{upload_id}-{part_number}`
  (`fs.rs part_key`) with `-` as separator. Bucket names and keys may
  contain `-`, so keys cannot be unambiguously parsed or prefix-scanned.
  Point reads work; enumeration does not. The record VALUE, however,
  carries bucket, key, upload_id, part_number, and the block list
  (`MultiPart` v1), so value-driven enumeration via `iter_all` works
  regardless of key format -- the scrub holder walk already does exactly
  that.
- What the original draft called `release_blocks` now exists in pieces:
  ADR 0006 landed the per-block striped decrement (`decrement_one_block`
  in `delete_path.rs`: stripe guard moved into the blocking closure, tx
  RMW, commit-then-unlink in one hold), but it is crate-private and the
  loop around it is inlined in `delete_object`. Abort needs that loop
  factored, not reinvented.
- fsck (ADR 0005) is live, with assets this ADR plugs into: the
  `plant_stale_part` crash fixture, the value-driven multipart pass with
  an explicit "age arrives with ADR 0003" deviation note, per-occurrence
  holder counting of part records, and a repair engine whose closing
  recount reconciles any refcount consequence of record removal.
- `CasFS` is not `Clone`; the S3 service consumes the only handle. A
  daemon GC task needs its own handle (all `CasFS` fields are already
  cheaply clonable; the single-`SharedBlockStore`-per-store rule is
  preserved because a clone shares the same `Arc`).

Related: ADR 0002 (addressing, ContentHash/ETag split), ADR 0005 (fsck
-- the multipart pass this ADR upgrades), ADR 0006 (the striped
protocol whose delete primitive abort reuses), `docs/refcount.md`
(leakage-vs-loss), `docs/fsck.md`.

---

## Decision

Proposed, pending review:

1. **An `_UPLOADS` tree** in the shared blocks DB (sibling of
   `_MULTIPART_PARTS`, opened in `SharedBlockStore::new`).
   `create_multipart_upload` writes an upload record; `complete` and
   `abort` claim it atomically; the GC ages it. Record v1 in the house
   codec style (`bucket_meta.rs` is the template, golden vectors and
   all): `created_at i64 | bucket_len u64 | bucket | key_len u64 | key
   | upload_id_len u64 | upload_id`. `created_at` is
   `chrono::Utc::now().timestamp()`, the `BucketMeta`/`Object` pattern.
2. **The upload record is the linearization point.** A new
   `Transaction::take_upload` (the `take_object` template: read AND
   remove in one tx) is the single claim primitive. `complete` and
   `abort` both start by claiming the record; exactly one wins, the
   loser fails `NoSuchUpload`. Double-complete, double-abort, and
   complete-vs-abort all collapse into this one rule; no per-upload
   lock exists or is needed.
3. **Existence enforcement.** `upload_part` and
   `complete_multipart_upload` verify the upload record exists (point
   read at entry; complete's claim is the atomic version) and fail
   `NoSuchUpload` otherwise. Complete additionally rejects an empty
   parts list. This is a client-visible behavior change from today's
   laissez-faire acceptance of any upload id; it is what makes abort
   semantics coherent.
4. **Re-keyed part records.** Part keys become length-prefixed:
   `bucket_len u64 | bucket | key_len u64 | key | upload_id_len u64 |
   upload_id | part_number u64 BE` -- unambiguous, and all parts of one
   upload share an exact byte prefix, so per-upload enumeration is a
   prefix scan (`iter_kv` from the prefix, stop at first non-match).
   Record values are unchanged (`MultiPart` v1).
   **No store-header bump and no migration**: keys are opaque to every
   reader (scrub decodes values; point reads reconstruct the key they
   wrote), an old-format record simply becomes unreachable by the new
   point reads -- and an unreachable part record with no upload record
   is exactly what the GC's orphan sweep reaps, value-driven. The first
   GC pass IS the migration.
5. **`release_blocks`**: factor `delete_object`'s inline striped
   decrement loop into a `pub(crate) release_blocks(...)` over an
   explicit block list (per-occurrence, ENOENT-tolerant, never aborts
   the loop); `delete_object` becomes its first caller, abort and GC
   its new ones. No new locking semantics -- it is the ADR 0006
   primitive with a name.
6. **`abort_multipart_upload`**: claim the upload record
   (`take_upload`; absent -> `NoSuchUpload`), prefix-scan the parts,
   and for each part **remove the part record first, then
   `release_blocks` its blocks**. That ordering is load-bearing for
   fsck: a crash between the two leaves an over-count (leakage, INFO,
   recount collects it). The reverse order would leave a part record
   whose references were already released -- fsck's recount would see
   holders exceeding rc, a false loss alarm on a store that lost
   nothing.
7. **`list_parts` and `list_multipart_uploads`**: read-only. Listings
   are bounded (parts per upload by S3's 10k cap; uploads by the TTL),
   so both scan, decode values, and sort in memory -- S3 ordering (key
   then upload_id; part_number) comes from the sort, not from on-disk
   key order, which buys the freedom to keep keys optimized for prefix
   scans instead of collation. Pagination markers and max limits are
   honored in-memory; `delimiter` grouping for uploads is deferred
   (known unknown below).
8. **Stale-upload GC**: a daemon task in s3cas (spawned before the
   accept loop, exiting on the existing graceful-shutdown signal). Each
   sweep: (a) scan `_UPLOADS` for records older than the TTL and abort
   them through the exact client abort path; (b) scan
   `_MULTIPART_PARTS` for orphan parts -- records whose upload record
   does not exist (the abort race window, and every pre-existing or
   old-key-format record) -- remove the record, then `release_blocks`.
   Config: `[multipart] stale_ttl_days` in qss_storage.toml (house
   pattern: optional section, Option leaves, DEFAULT_ constant, CLI
   flag wins), `0 = disabled`. Sweep interval: fixed `max(ttl/20, 1h)`,
   not configurable until someone needs it.
9. **fsck integration** (the promise ADR 0005 left): the multipart pass
   reads `_UPLOADS` and reports per-upload AGE alongside parts/bytes;
   a new `orphan_part` finding class (part record with no upload
   record, INFO); its repair action removes the part record -- the
   engine's closing recount then lowers the freed blocks' rcs and
   `SetRc`-to-zero reclaims them, entirely inside existing machinery.
   The daemon GC remains the primary reaper; fsck is the offline
   backstop.
10. **Observability**: the three new methods enter `S3_API_METHODS` and
    get `MetricFs` wrappers (today they bypass metrics entirely); the
    GC gains `uploads_reaped` / `orphan_parts_reaped` counters in the
    existing default-registry pattern.

---

## Architecture Overview

### Component Breakdown

1. **Uploads tree + record codec** (`cas-storage`: `multipart.rs` or a
   sibling `uploads.rs`; tree opened in `shared_block_store.rs`)
   - `UPLOADS_TREE` constant next to `MULTIPART_PARTS_TREE`; record v1
     with golden vectors; `take_upload` on `Transaction`.
2. **Part re-key + prefix scan** (`cas-storage/src/cas/fs.rs`,
   `multipart.rs`)
   - Binary part-key codec (encode + per-upload prefix); `MultiPartTree`
     gains prefix enumeration via the ext-tree handle.
3. **`release_blocks`** (`cas-storage/src/cas/delete_path.rs`)
   - The factored striped loop; `delete_object` calls it.
4. **Handlers** (`s3cas/src/s3fs.rs` + `metrics.rs`)
   - `abort_multipart_upload`, `list_parts`, `list_multipart_uploads`;
     existence checks in `upload_part`/`complete`; empty-parts
     rejection; `MetricFs` + `S3_API_METHODS` entries.
5. **GC task** (`s3cas/src/main.rs` + a `cas-storage` sweep function)
   - `CasFS` gains `Clone`; task spawned pre-accept-loop, selects on
     interval tick vs shutdown; sweep logic lives in cas-storage so
     fsck and tests share it.
6. **fsck upgrade** (`cas-storage/src/scrub/`)
   - Age in the multipart pass; `orphan_part` class + repair action;
     fixtures: `plant_upload_record`, orphaned-part variants.
7. **Config + docs** (`config.rs`, `qss_storage.toml.example`,
   `docs/fsck.md`, `docs/multipart.md` or a section in existing docs)

### Data Flow

```
create_multipart_upload -> _UPLOADS record (created_at)
upload_part   [upload exists?] -> blocks (striped bumps) -> part record
complete      [take_upload claim] -> object meta -> remove part records
abort         [take_upload claim] -> per part: remove record
                                       -> release_blocks (striped)
GC sweep      -> aged uploads -> abort path
              -> orphan parts (no upload record) -> remove -> release
fsck          -> age + orphan_part findings; repair removes records,
                 closing recount reclaims
```

---

## Alternatives Considered

### Offline-only GC (fsck repair, no daemon task)
- **The idea**: no background task; stale uploads are reaped only when
  the operator runs fsck (whose orphan_part repair this ADR adds
  anyway).
- **Optimizes for**: zero new runtime moving parts.
- **Sharpest tradeoff**: leaks grow until an operator remembers, and
  fsck is offline -- reclamation costs an availability window. The
  default deployment never reclaims anything.
- **Bets on**: operators running fsck on a schedule. Weak bet; rejected
  as the only mechanism, but the fsck backstop ships regardless.

### Reuse the object-delete machinery by materializing a tombstone object
- **The idea**: on abort, write the parts as a fake object then delete
  it through `delete_object`.
- **Optimizes for**: no new decrement entry point.
- **Sharpest tradeoff**: pollutes object listings and metrics with
  synthetic objects; the complete-vs-abort claim would need a second
  mechanism anyway.
- **Bets on**: the object path staying the only refcount authority.
  Moot post-0006: `release_blocks` is a rename-and-factor of code that
  already exists, not a new path.

### Keep the dash-separated part keys, add a separate index
- **The idea**: leave part keys alone; maintain a parallel index tree
  for enumeration.
- **Optimizes for**: not touching the part write path.
- **Sharpest tradeoff**: two structures to keep consistent for data
  that is transient anyway -- and the GC's orphan sweep would still
  need value-driven iteration, so the index buys only the prefix scan.
- **Bets on**: the ambiguity never biting. Rejected: re-keying is free
  (the first GC pass reaps old-format records as orphans; that IS the
  migration).

### Store-header bump to v4 for the key format change
- **The idea**: gate the part-key change the way v2/v3 gated record
  changes.
- **Optimizes for**: one uniform versioning story.
- **Sharpest tradeoff**: refuses stores that are fully compatible --
  keys are opaque to every value-driven reader, and the only readers
  that reconstruct keys are the point reads that wrote them. A v4 gate
  would force "drain your uploads before upgrade" ceremony for state
  the GC reaps automatically.
- **Bets on**: nothing new ever parsing old part KEYS. If a future
  feature must parse keys, it bumps then. Rejected for now; the
  version-bump precedent (0005/0006) applied to record VALUES, which
  are unchanged here.

---

## Consequences

### Positive
- Interrupted uploads stop leaking permanently; standard clients work;
  uploads become observable to clients, operators, and fsck alike.
- The refcount story closes its last routine-leak class that has no
  collector (fsck could see stale parts but not age or reap them).
- The abort/complete claim rule is one primitive (`take_upload`) with
  no locks, no new invariants beyond ADR 0006's.

### Negative
- A new tree, three new handlers, a background task, and a config
  section: more daemon surface.
- In-flight uploads do not survive the upgrade (their parts become
  orphans and are reaped by the first GC pass). Acceptable: transient
  state, no deployed stores, and today they leak forever instead.
- `upload_part`/`complete` existence enforcement is a client-visible
  tightening (review ask 2).

### Risks
- Abort racing an in-flight `upload_part` that already passed its
  existence check: the late part lands with refcounted blocks and no
  upload record -- an orphan part, reaped within one GC interval.
  Leakage bounded by the interval; loss impossible (every rc mutation
  is striped, ADR 0006). The race-window test must pin this.
- GC aborting an upload a slow client still intends to finish:
  mitigated by the conservative default TTL and per-store config; the
  client sees `NoSuchUpload` on its next part, the S3-idiomatic
  outcome.
- The GC's orphan sweep iterates the whole parts tree each sweep.
  Bounded: the tree holds only in-flight uploads once GC runs
  regularly. If a pathological store makes this slow, the sweep is
  already interval-amortized; scoping per-sweep is the tuning knob.

---

## What an Expert Would Ask

**Q: What exactly serializes complete against abort?**
A: The upload record. Both start with `take_upload` -- an atomic
read+remove inside one transaction on the shared DB (`take_object`'s
template, which already defeats double-DELETE). Exactly one caller gets
the record; the other sees `None` and returns `NoSuchUpload`. There is
no window where both proceed: everything either path does afterwards is
keyed to having won the claim. Part records and refcounts touched by a
loser that was mid-`upload_part` degrade to orphan parts, which the GC
reaps.

**Q: Why must abort remove the part record before releasing its
blocks, and not the reverse?**
A: Because fsck's recount treats part records as reference holders.
Record-then-release leaves, on a crash between them, a block whose rc
exceeds its walked holders: an over-count, INFO, collected by the next
recount. Release-then-record would leave a part record claiming
references that were already dropped: holders exceed rc, which fsck
must classify as CRITICAL loss-shaped under-count -- a false alarm
that, with `--repair`, would RAISE the rc back and permanently leak the
blocks. The ordering keeps the accounting monotone in the safe
direction. Same reasoning as ADR 0006's delete ordering, applied one
level up.

**Q: Does the GC racing a live client break anything?**
A: The GC IS a client: it calls the same abort path, so the claim rule
covers it. GC-aborts-vs-active-upload_part is the same bounded-leak
race as client abort. The one new interaction is GC-vs-complete on a
TTL-boundary upload: whichever claims first wins, the loser errors --
for complete that means the client retries and gets `NoSuchUpload`,
which is exactly what a TTL policy means.

**Q: Is `created_at` from the wall clock safe?**
A: TTLs of days make clock skew irrelevant; monotonicity is not
required. A store moved between machines with wildly wrong clocks can
prematurely expire uploads -- accepted, documented.

**Q: Old-format part records reference blocks; the GC deletes those
records value-driven. Can that free a block a completed object still
references?**
A: No. Releasing a part's blocks decrements per occurrence through the
striped RMW; a block shared with a live object holds that object's own
increments (every dedup hit bumps, ADR 0006), so the release lands at
the object's count, not zero. The only rc that reaches zero is one
whose every holder was the reaped parts themselves. This is the same
argument as object delete; `release_blocks` inherits it by being the
same code.

---

## Implementation Plan

### Decisions you will probably want to tweak
- **TTL default**: 7 days proposed. Alternative: 24h (aggressive,
  garage-like) or disabled-by-default. Cost to change: none technically
  (config), but the default ships into fleets and muscle memory.
  Review ask 1.
- **Existence enforcement on `upload_part`** (and empty-parts rejection
  on complete): client-visible semantics. Decide now, before any
  client depends on the laissez-faire behavior. Review ask 2.
- **Abort of an unknown upload id**: `NoSuchUpload` (S3-conformant;
  s3s maps it to 404) vs idempotent success. AWS returns the error;
  idempotent success is friendlier to blind cleanup scripts. Review
  ask 3.
- **GC placement and default**: daemon task default-on at 7 days, with
  the fsck backstop -- vs default-off (operator opts in), vs fsck-only.
  Review ask 4.
- **Upload record codec**: house fixed-width v1 style with golden
  vectors, settled by 0002/0005/0006 precedent -- not re-asked.

### Known unknowns and how the plan absorbs them
- s3s pagination semantics for `list_multipart_uploads` `delimiter` /
  `common_prefixes`: deferred; listings ship with prefix + markers +
  max honored in-memory. Pivot signal: a real client that groups by
  delimiter. (DTO fields exist; returning them empty is conformant for
  no-delimiter requests.)
- Whether the orphan sweep's full parts-tree iteration ever matters at
  real scale: bounded by TTL in steady state; per-sweep scoping is the
  lever if a pathological store appears.

### The mechanical work
Uploads tree + record codec with golden vectors; `take_upload`;
binary part-key codec + prefix scan; `release_blocks` factoring;
three s3s handlers + two existence checks + `MetricFs`/`S3_API_METHODS`
entries; `CasFS: Clone`; GC sweep function in cas-storage + daemon task
+ config section (three config-test fixtures + example file); fsck
multipart-pass age + `orphan_part` class + repair action + fixtures;
metrics counters; tests: claim races (complete-vs-abort, double-abort),
abort-vs-upload_part orphan window, GC reap + old-key reap, NoSuchUpload
cases, empty-parts rejection, listing order/pagination, fsck
orphan_part end-to-end.

Review asks:
1. TTL default: 7 days, 24 hours, or disabled-by-default?
2. Enforce upload-record existence on `upload_part` + reject empty
   `complete` -- yes/no?
3. Abort of unknown upload id: `NoSuchUpload` or idempotent success?
4. GC: daemon task default-on, default-off, or fsck-only?

---

## Open Questions

**Behavior definers**
- [ ] `list_multipart_uploads` delimiter grouping: deferred until a
      client needs it -- confirm.
