# Multipart Upload Lifecycle Completion and Stale-Upload GC

**Status**: Accepted and IMPLEMENTED (2026-08-01). Landed on
`development` as the commit series `cdb03a0..3a29395` (components 1-7 of
`docs/plans/adr-0003-implementation.md` plus the claim-window amendment
`cf64fc6`, one commit per component; as-built deviations are folded in
below, marked "(as built)").
**Date**: 2026-07-30
**Updated**: 2026-07-31 (revised against the as-built ADR 0005/0006/0007
world; the original draft predates the striped write/delete protocol,
the fsck machinery, and the notx removal, and its release_blocks and
race sections described machinery that now exists in different form.
Same day, owner sign-off on all four review asks: TTL default 7 days;
existence enforcement on upload_part AND empty-parts rejection on
complete; abort of unknown id returns NoSuchUpload; GC as a daemon
task, default-on, with the fsck backstop. Decision-complete; Accepted
on implementation start)

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
(leakage-vs-loss), `docs/fsck.md`, and `docs/multipart.md` (the
operator page this ADR produced).

---

## Decision

Accepted and implemented as follows:

1. **An `_UPLOADS` tree** in the shared blocks DB (sibling of
   `_MULTIPART_PARTS`, opened in `SharedBlockStore::new`).
   `create_multipart_upload` writes an upload record; `complete` and
   `abort` claim it atomically; the GC ages it. Record v1 in the house
   codec style (`bucket_meta.rs` is the template, golden vectors and
   all): `created_at i64 | bucket_len u64 | bucket | key_len u64 | key
   | upload_id_len u64 | upload_id`. `created_at` is
   `chrono::Utc::now().timestamp()`, the `BucketMeta`/`Object` pattern.
   **(as built)** The record type lives in the metastore layer
   (`metastore/upload_record.rs`, beside `bucket_meta.rs`), not in `cas`:
   `Transaction::take_upload` decodes it inside the transaction, and the
   metastore layer names no type from above it. A crate-internal
   `with_created_at` constructor exists solely so everything that ages an
   upload -- the TTL sweep, fsck's reported age -- can be tested against
   records that are already old. Sleeping through a TTL is not a test.
2. **The upload record is the linearization point.** A new
   `Transaction::take_upload` (the `take_object` template: read AND
   remove in one tx) is the single claim primitive. `complete` and
   `abort` both start by claiming the record; exactly one wins, the
   loser fails `NoSuchUpload`. Double-complete, double-abort, and
   complete-vs-abort all collapse into this one rule; no per-upload
   lock exists or is needed.
   **(as built -- THE AMENDMENT.** This supersedes the shape the rest of
   this revision was written against, and Decision 6 with it. The
   landed history keeps the original, which is why the change is
   recorded here rather than silently rewritten.**)**
   *Complete claims the upload record AND every part it names in ONE
   transaction* (`claim_upload_with_parts`), not the record alone.
   `_UPLOADS` and `_MULTIPART_PARTS` live in the same shared fjall
   database, so one transaction spans both -- which turns a window that
   could only have been narrowed into one that does not exist.
   The window: complete does not release its parts' blocks, it hands
   them to the object it mints. Between "the upload record is gone" and
   "the part records are gone" those blocks would have two claimants on
   paper and one in truth, and anything reaping orphan parts in that
   instant (the GC's phase 2, by construction) would release references
   the new object owns. That is loss, and no amount of narrowing makes
   it not loss. With the parts inside the claim, a named part record
   disappears at the same instant as its upload record: nothing can ever
   observe an inheritable part as an orphan.
   Consequences of the amendment, all landed:
   - the post-object-creation part-cleanup loop no longer exists --
     the claim already removed those records;
   - the object is built from the values the CLAIMING transaction read,
     not from whatever an earlier validation pass saw;
   - parts the client did NOT name survive the claim. Nothing inherited
     their references, so they are true orphans and the GC reaps them;
   - a claim that does not win removes nothing, so a rejected complete
     leaves the upload intact and retryable.
   **(as built)** Complete VALIDATES before it claims: a point read of
   the upload record at entry (so an upload that is simply gone answers
   `NoSuchUpload` rather than failing part validation), then the parts
   list, then the claim. A request the validation rejects has changed
   nothing, which is what real S3 does with `InvalidPart`. The residual
   window is stated rather than closed: an abort landing between the
   entry read and the validation makes the validation fail with
   `InvalidArgument` ("Part not uploaded") instead of `NoSuchUpload`.
   Both are 4xx, both leave the store consistent, and the claim remains
   the authoritative answer -- so the cost of closing it (claiming
   before validating, and thereby destroying the upload on every
   malformed request) is not worth paying.
3. **Existence enforcement.** `upload_part` and
   `complete_multipart_upload` verify the upload record exists (point
   read at entry; complete's claim is the atomic version) and fail
   `NoSuchUpload` otherwise. Complete additionally rejects an empty
   parts list. This is a client-visible behavior change from today's
   laissez-faire acceptance of any upload id; it is what makes abort
   semantics coherent.
   **(as built)** `upload_part`'s check is at entry, before a single
   block is stored, so an unknown id costs the client nothing. It is
   deliberately NOT atomic against a concurrent complete or abort: a
   part that passes it and lands after another caller claimed the record
   becomes an orphan part, which the GC reaps. That is the one accepted
   race of this design (Risks below), and it is bounded leakage.
   Complete's empty-parts rejection is `InvalidRequest`, "You must
   specify at least one part".
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
   **(as built)** The per-part removal is a TAKE, not a read followed by
   a remove: `Transaction::take_part` reads and removes the record in
   one transaction, and the blocks released are the ones that
   transaction read (`reap_part`). The ordering above is unchanged --
   the take commits before the release begins, and a take that finds
   nothing releases nothing -- but the fusion buys exclusion the split
   version did not have: two reapers over one part (a client's abort
   against a GC sweep, or two sweeps) cannot both release it. Split
   read-then-remove would let both read the same block list and both
   decrement it, which is a double release: loss, not leakage. `Ok(None)`
   -- somebody else took it -- is an ordinary outcome, not an error, for
   every caller.
   **(as built)** The reap takes the ITERATED storage key rather than
   rebuilding one from the record's fields, because the GC's orphan
   sweep reaps records whose key is not reconstructible at all (the
   legacy dash-joined ones). Rebuilding there would remove nothing and
   then release blocks the surviving record still claims: precisely the
   forbidden order.
   **(as built)** Abort's handler body is `CasFS::abort_upload`, called
   by both `AbortMultipartUpload` and the GC, so there is exactly one
   abort implementation. One unreadable or unreapable part record is
   logged and stepped over rather than stranding every other part of the
   upload; what is left behind is an orphan part for the next sweep.
7. **`list_parts` and `list_multipart_uploads`**: read-only. Listings
   are bounded (parts per upload by S3's 10k cap; uploads by the TTL),
   so both scan, decode values, and sort in memory -- S3 ordering (key
   then upload_id; part_number) comes from the sort, not from on-disk
   key order, which buys the freedom to keep keys optimized for prefix
   scans instead of collation. Pagination markers and max limits are
   honored in-memory; `delimiter` grouping for uploads is deferred
   (known unknown below).
   **(as built)** Three answers this decision did not spell out:
   `list_multipart_uploads` on a bucket that does not exist returns
   `NoSuchBucket` (a listing is bucket-scoped, and an empty list would
   claim the bucket is there and idle); `list_parts` on an upload that
   does not exist returns `NoSuchUpload`, the point read again; and
   `max_parts = 0` / `max_uploads = 0` return an empty page with
   `is_truncated: true` and no next marker -- the request asked for
   nothing and there is more, which is exactly what the flag means. An
   `upload_id_marker` without a `key_marker` is ignored, as S3 does: it
   only disambiguates within one key.
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
   **(as built)** Three operational details settled during the build:
   - a part record that does not DECODE is counted in the sweep's error
     tally and left exactly where it is. Its block list cannot be read,
     so removing it would strand the references it names forever; that
     is fsck's business, not a collector's. Nothing else stops a sweep
     -- every failure is logged, counted and stepped over, because there
     is no caller to return an error to and one bad record must not
     strand every later one;
   - the first tick lands one full period AFTER startup, never at boot.
     Boot is when a store is busiest, a sweep is disk work, and residue
     that has already sat for days can wait an hour;
   - shutdown signals the task where the accept loop breaks and waits at
     most 5 seconds for it. A sweep that overruns that budget is left to
     be abandoned: every step it takes is crash-safe on its own (record
     first, blocks second), so an abandoned sweep is exactly the crash
     case that ordering was chosen for. Blocking shutdown on a large
     store's sweep would be the worse failure.
9. **fsck integration** (the promise ADR 0005 left): the multipart pass
   reads `_UPLOADS` and reports per-upload AGE alongside parts/bytes;
   a new `orphan_part` finding class (part record with no upload
   record, INFO); its repair action removes the part record -- the
   engine's closing recount then lowers the freed blocks' rcs and
   `SetRc`-to-zero reclaims them, entirely inside existing machinery.
   The daemon GC remains the primary reaper; fsck is the offline
   backstop.
   **(as built)** Four refinements, three of them consequences of the
   amendment above:
   - the repair action does not merely remove the record: it reaps
     through the same take-style primitive the daemon uses
     (`reap_part`), so the release is the striped decrement and a lost
     take is a skipped action rather than an error. The closing recount
     then VALIDATES the result instead of being the thing that produces
     it;
   - the reaping is planned in repair ROUND ONE, beside the half-deleted
     bucket teardowns, not with the rest. Round one is the actions that
     remove HOLDERS: a reap planned beside the `SetRc`s would have the
     recount -- computed from a walk that still counted the part -- raise
     the rc straight back to include the reference just released;
   - it is double-gated: on `Pass::Recount` like every rc-consequential
     action, AND on `Pass::MultipartReport`, because that pass is what
     closes the live-versus-orphan classification. Which leads to the
     fourth: an UPLOAD record that will not decode refuses the multipart
     pass outright, the same refusal an undecodable part record makes.
     Without the full set of upload records a live upload's part cannot
     be told from an orphan, and reaping a live part is loss;
   - the `orphan_part` finding carries the raw storage key the record is
     filed under (the only address a legacy dash-keyed record has),
     rendered as text when it is printable and lowercase hex otherwise.
     Length-prefixed keys are frequently valid UTF-8 yet full of NULs,
     so decodability is the wrong test; printability is the right one.
   The upload age is reported as days/hours/minutes against the wall
   clock, the same subtraction the TTL makes. fsck is offline and holds
   the store's lock, so there is no concurrent writer to be monotonic
   against.
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
   - **(as built)** split in two by layering: the record type in
     `metastore/upload_record.rs` (the transaction decodes it), the
     lifecycle operations in `cas/uploads.rs` (`create_upload`,
     `get_upload`, `claim_upload`, `claim_upload_with_parts`,
     `reap_part`, `abort_upload`, `list_uploads`, `upload_parts`).
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
   - **(as built)** its own page, `docs/multipart.md`: the lifecycle in
     plain language, the claim rule, the client-visible answers, TTL
     semantics and the `[multipart]` table, orphan parts and their two
     reapers, and the upgrade story for a pre-0003 store.

### Data Flow

**(as built)** -- complete's claim takes the named parts with the record,
so there is no cleanup loop after the object is minted, and every reap is
a take:

```
create_multipart_upload -> _UPLOADS record (created_at)
upload_part   [upload exists?] -> blocks (striped bumps) -> part record
complete      [validate parts] -> claim upload AND named parts (one tx)
                              -> object meta inherits their blocks
abort         [take_upload claim] -> per part: take record
                                       -> release_blocks (striped)
GC sweep      -> aged uploads -> abort path
              -> orphan parts (no upload record) -> take -> release
fsck          -> age + orphan_part findings; repair reaps through the
                 same take, closing recount validates
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
  **(as built)** Pinned, and it is the ONLY window of this shape that
  survived: the complete-versus-orphan-sweep window that the original
  Decision 6 shape would have opened is closed by the amendment, not
  narrowed. This one stays open on purpose -- closing it would mean
  serializing every `upload_part` against the claim, which is a lock on
  the hot path to prevent bounded leakage.
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
**(as built)** Complete's claim additionally takes every part it names,
in the same transaction (Decision 2's amendment). That does not change
what serializes complete against abort -- the upload record still does --
but it removes the only state in which a third party (the GC) could act
on the difference between the two.

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
**(as built)** The removal is a take, so the record and the block list
that gets released come from one transaction. The ordering argument
above is untouched; what the take adds is that two reapers of one part
cannot both act on it, which the split read-then-remove shape would have
allowed (both reading the same list, both decrementing: a double
release, which is loss).

**Q: Can the GC's orphan sweep reap a part that a complete in flight is
about to inherit? (as built)**
A: No, and not because the window is small -- because it does not exist.
A completed object INHERITS its parts' block references rather than
taking new ones, so a part record outliving its upload record while a
complete was in flight would be indistinguishable from a crashed abort's
residue, and reaping it would release references the new object holds.
`_UPLOADS` and `_MULTIPART_PARTS` are trees of the same fjall database,
so complete takes the upload record and every part it names in ONE
transaction: a named part disappears at the same instant as its upload
record. Phase 2 of the sweep can therefore only ever see parts nothing
inherited -- an aborted upload's residue, an `upload_part` that landed
after the claim, a part the completing client never named, or a legacy
record. Every one of those holds its blocks alone, so releasing them is
right. The GC needs no knowledge of complete's progress, and complete
needs no coordination with the GC.

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

### Decisions locked (owner sign-off 2026-07-31)
- **TTL default**: 7 days, `[multipart] stale_ttl_days`, CLI flag wins,
  0 disables.
- **Existence enforcement**: yes, both -- `upload_part` fails
  `NoSuchUpload` for unknown ids; `complete` rejects an empty parts
  list.
- **Abort of an unknown upload id**: `NoSuchUpload` (S3-conformant,
  404 via s3s).
- **GC placement**: daemon task, default-on, sweep `max(ttl/20, 1h)`;
  fsck's `orphan_part` repair ships as the offline backstop.
- **Upload record codec**: house fixed-width v1 style with golden
  vectors, settled by 0002/0005/0006 precedent -- was never re-asked.

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

Review asks: none -- all four resolved 2026-07-31 (see Decisions
locked).

---

## Open Questions

**Behavior definers**
- [ ] `list_multipart_uploads` delimiter grouping: deferred until a
      client needs it -- confirm.
