# Multipart Upload Lifecycle Completion and Stale-Upload GC

**Status**: Proposed
**Date**: 2026-07-30

---

## Context

The S3 multipart surface is half a lifecycle. `create_multipart_upload`,
`upload_part`, and `complete_multipart_upload` are implemented;
`abort_multipart_upload`, `list_parts`, and `list_multipart_uploads` fall
through to the s3s `NotImplemented` default. There is no garbage collection
of any kind for uploads that are started and never completed.

Consequences of the gap, as the code stands:

- Every part record of an abandoned upload stays in the multipart tree
  forever.
- Every block written for an abandoned part keeps its refcount increment
  forever -- the disk space is unreclaimable ("leakage" per
  `docs/refcount.md`, but unbounded and routine rather than rare).
- Standard clients (awscli, boto3, rclone) call `AbortMultipartUpload` on
  interrupted transfers as normal operation. Today that returns
  `NotImplemented` and the client gives up, leaving the leak in place.

Structural constraints discovered while scoping:

- `create_multipart_upload` records nothing: the upload id is a UUID handed
  to the client, with no server-side record of the upload's existence or
  creation time. Listing or expiring uploads has nothing to scan.
- Part records are keyed `{bucket}-{key}-{upload_id}-{part_number}` with `-`
  as separator. Bucket names and keys may themselves contain `-`, so the
  key format cannot be unambiguously parsed or prefix-scanned. Writing and
  point-reading works; enumeration does not.
- Refcount decrement logic currently exists only inside object deletion
  (`namespace.delete_object` -> blocks-to-delete). Aborting an upload needs
  a decrement path for a bare block list with no owning object.

Related: ADR 0002 (block addressing), `docs/refcount.md` (leakage-vs-loss
contract), `docs/as-built/04-code-health.md` (the audit that surfaced this).

---

## Decision

Proposed, pending review:

1. **An uploads tree.** `create_multipart_upload` writes an upload record
   (bucket, key, upload id, created-at) to a new `_UPLOADS` tree in the
   shared store. `complete` and `abort` remove it. This makes uploads
   enumerable and ageable.
2. **Re-keyed part records.** Part keys become length-prefixed
   (`u16 len ++ bucket ++ u16 len ++ key ++ upload_id ++ u16 part`), making
   prefix scans per-upload unambiguous. The multipart tree holds only
   transient state, so the format change needs no migration: a store is
   drained of in-flight uploads by completing or aborting them before
   upgrade, and a leftover old-format record is exactly what the GC sweep
   (below) deletes.
3. **`abort_multipart_upload`**: removes the upload record and all part
   records, and decrements the refcount of every block referenced by those
   parts, deleting block files that reach zero. Built on a new
   `release_blocks(&[BlockId])` in cas-storage shared by object deletion.
4. **`list_parts` and `list_multipart_uploads`**: read-only scans of the
   two trees. Table stakes for client compatibility.
5. **Stale-upload GC**: a background task in the daemon (not a cron
   external) that aborts uploads older than a configurable TTL. Default
   proposed: 7 days, `[multipart] stale_ttl_days` in qss_storage.toml,
   0 = disabled.

---

## Architecture Overview

### Component Breakdown

1. **Uploads tree** (`cas-storage/src/cas/fs.rs`, new tree constant)
   - Record: `{bucket, key, upload_id, created_at_unix: u64}` in the
     format-v1 fixed-width style with golden-vector tests.
2. **`release_blocks`** (`cas-storage/src/cas/delete_path.rs`)
   - Decrement + delete-at-zero for an explicit block list; the existing
     object delete path becomes a caller of it.
3. **Abort / list handlers** (`s3cas/src/s3fs.rs`)
   - Thin S3 wrappers over the above.
4. **GC task** (`s3cas/src/main.rs` + `cas-storage`)
   - Interval scan of the uploads tree; calls the same abort path.

### Data Flow

```
create_multipart_upload --> uploads tree (record + timestamp)
upload_part             --> parts tree (new key format) + block refcounts
complete                --> object meta; remove upload + part records
abort  <-- client or GC --> release_blocks(parts' blocks); remove records
```

---

## Alternatives Considered

### Offline-only GC (fsck subcommand, no daemon task)
- **The idea**: no background task; stale uploads are reaped only when the
  operator runs the scrub tool (ADR 0005).
- **Optimizes for**: zero new runtime moving parts.
- **Sharpest tradeoff**: leaks grow until an operator remembers; the
  default deployment never reclaims anything.
- **Bets on**: operators running fsck on a schedule. Weak bet; rejected as
  the only mechanism, but the scrub tool should also be able to do it.

### Reuse the object-delete machinery by materializing a tombstone object
- **The idea**: on abort, write the parts as a fake object then delete it.
- **Optimizes for**: no new decrement path.
- **Sharpest tradeoff**: pollutes object listings and metrics with
  synthetic objects; complete-vs-abort races get harder to reason about.
- **Bets on**: the object path staying the only refcount authority. Not
  worth the contortion; `release_blocks` is small.

### Keep the dash-separated part keys, add a separate index
- **The idea**: leave part keys alone; maintain a parallel index tree for
  enumeration.
- **Optimizes for**: not touching the existing write path.
- **Sharpest tradeoff**: two structures to keep consistent for data that is
  transient anyway.
- **Bets on**: the ambiguity never biting point reads. It already cannot be
  scanned; carrying the ambiguity forward buys nothing since migration is
  free for transient state.

---

## Consequences

### Positive
- Interrupted uploads stop leaking disk permanently; standard clients work.
- Uploads become observable (`list_multipart_uploads` for operators too).

### Negative
- A new tree and a new background task in the daemon: more state, more
  config, one more thing running.
- The part-key format change means in-flight uploads do not survive the
  upgrade (acceptable: they are transient, and today they leak anyway).

### Risks
- Abort racing an in-flight `upload_part` for the same upload id: the part
  write can re-add records/refcounts after abort cleaned up. Needs an
  ordering rule (see expert questions).
- GC aborting an upload the client still intends to finish (slow uploads):
  mitigated by a conservative default TTL and per-store config.

---

## What an Expert Would Ask

**Q: What happens when `upload_part` races `abort` (client retry, or GC
firing mid-upload)?**
A: Proposed rule: abort deletes the upload record first; `upload_part` and
`complete` check the upload record exists (today they check nothing) and
fail with `NoSuchUpload` if not. A part write that already passed the check
and lands after cleanup recreates only part records, whose blocks the next
GC pass releases because the upload record is gone. Loss is impossible
(refcounts only over-count in the race); leakage is bounded by one GC
interval. This ordering must be written down in the code.

**Q: Does `release_blocks` hold the "leakage allowed, loss never" line under
concurrency with a deduping PUT?**
A: Same window as object delete has today: decrement-to-zero racing a PUT
that dedups onto the block. The invariant test for this exists nowhere and
is a prerequisite for this ADR (it hardens the shared path this ADR makes
more traveled). See ADR 0005's scrub for the backstop.

**Q: Is `created_at` from the wall clock safe?**
A: TTLs of days make clock skew irrelevant; monotonicity is not required.
A store moved between machines with wildly wrong clocks can prematurely
expire uploads -- accepted, documented.

**Q: Why a per-daemon task and not a shared job with respd?**
A: respd has no multipart concept; the uploads tree is S3-surface state.
If ADR 0004 lands a single-owner topology, the task runs wherever the
store's owning process is.

---

## Implementation Plan

### Decisions you will probably want to tweak
- **Upload record format**: fixed-width v1-style struct.
  Alternative: serde/postcard. Cost to change later: migration of a
  now-persistent tree -- decide before first release, cheap until then.
- **TTL default (7 days) and config key shape**.
  Alternative: 24h (garage-like aggressive). Cost to change: none
  (config), but the default ships in docs and muscle memory.
- **`NoSuchUpload` enforcement on `upload_part`/`complete`**: behavior
  change for clients that today can upload parts to a never-created id.
  Alternative: keep the laissez-faire behavior for parts. Cost to change:
  client-visible semantics; decide now.

### Known unknowns and how the plan absorbs them
- Whether s3s's dev-rev trait exposes everything `list_multipart_uploads`
  needs (pagination markers). Default: implement without pagination first
  (bounded by TTL anyway); pivot signal: a real listing exceeding one
  response.

### The mechanical work
Uploads tree + record codec with golden vectors; part-key codec;
`release_blocks` extraction; three s3s handlers; GC interval task; config
plumbing; tests: abort releases refcounts, GC reaps, complete-vs-abort
race, `NoSuchUpload` cases.

Review asks:
1. TTL default: 7 days, 24 hours, or disabled-by-default?
2. Enforce upload-record existence on `upload_part` -- yes/no?
3. Fixed-width codec for the uploads tree -- yes, or serde since the tree
   is new anyway?

---

## Open Questions

**Architecture-changers**
- [ ] Should GC live in the daemon or only in the fsck tool (ADR 0005), or
      both with the daemon task defaulting off?

**Behavior definers**
- [ ] Does abort of an unknown upload id return success (idempotent, what
      AWS does) or `NoSuchUpload`?
- [ ] Are complete and abort mutually exclusive via the upload record
      alone, or is a per-upload lock needed?
