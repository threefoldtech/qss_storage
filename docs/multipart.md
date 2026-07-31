# Multipart uploads: lifecycle, garbage collection, and what clients see

An S3 multipart upload is a three-step conversation -- start, send parts,
finish -- and any of the three can be abandoned halfway. This page is what
an operator needs to know about the state that leaves behind: where it
lives, what reclaims it, when, and what a client is told in every case
where the server says no.

Built by `docs/adr/0003-multipart-lifecycle-and-gc.md`. The refcount
contract it operates under is `docs/refcount.md`; the offline tool that
backstops it is `docs/fsck.md`.

## The lifecycle

```
CreateMultipartUpload  -> upload record in _UPLOADS (stamped created_at)
UploadPart             -> [upload exists?] -> blocks -> part record
CompleteMultipartUpload-> [validate] -> claim upload + named parts (one tx)
                          -> object record inherits the parts' blocks
AbortMultipartUpload   -> claim upload -> per part: take record, release blocks
GC sweep (daemon)      -> uploads past the TTL -> the abort path
                       -> part records no upload owns -> take, release
fsck                   -> reports upload age; --repair reaps orphan parts
```

**The upload record is the upload.** `CreateMultipartUpload` writes one row
in the store's `_UPLOADS` tree, carrying the bucket, the key, the upload id
and the creation time. While that row exists the upload exists; the moment
it is gone the upload is over. Everything below follows from that one
sentence.

**Parts hold their own blocks.** Each `UploadPart` writes its blocks
(refcounted like any other write, ADR 0006) and then a part record naming
them. Until the upload completes or aborts, that part record is the only
thing holding those blocks -- which is why fsck counts part records as
reference holders and why an abandoned upload occupies real disk.

**Complete hands the blocks over.** A completed upload does not release its
parts' blocks: the object it mints inherits those references. The part
records and the upload record disappear together, in one transaction, and
the object record takes their place as the holder.

**Abort gives the blocks back.** Each part record is removed and the
references it held are released, per occurrence, through the same striped
decrement `DeleteObject` uses. A block shared with a live object keeps that
object's references; only blocks nothing else holds are freed.

## The claim rule

`CompleteMultipartUpload` and `AbortMultipartUpload` both begin by
*claiming* the upload record: reading and removing it in one transaction.
Exactly one caller can win that, so:

- two aborts of one upload: the first succeeds, the second gets
  `NoSuchUpload`;
- two completes: the same;
- a complete racing an abort: whichever claims first wins, the other gets
  `NoSuchUpload`;
- the garbage collector racing either: the GC is just another client of the
  same claim, with no privileges.

There is no lock anywhere in this, and no other serialization to reason
about. Everything each path does after the claim is conditional on having
won it.

## What clients see

| Situation | Answer |
| --- | --- |
| `UploadPart` with an upload id that does not exist (never created, completed, aborted, or reaped by the GC) | `NoSuchUpload` (404), **before** any bytes are stored |
| `CompleteMultipartUpload` / `AbortMultipartUpload` of an upload that is gone | `NoSuchUpload` (404) |
| `CompleteMultipartUpload` with an empty parts list | `InvalidRequest`, "You must specify at least one part" |
| Complete naming a part that was never uploaded | `InvalidArgument`, "Part not uploaded" -- **the upload survives**, and a corrected complete can be sent |
| Complete whose part numbers are not 1, 2, 3, ... in order | `InvalidPartOrder` -- also non-destructive |
| `ListParts` for an upload that does not exist | `NoSuchUpload` |
| `ListMultipartUploads` for a bucket that does not exist | `NoSuchBucket` |
| `max_parts=0` or `max_uploads=0` | an empty page with `is_truncated: true` and no next marker: the request asked for nothing, and there is more |
| `ListMultipartUploads` with a `delimiter` | honored as a passthrough field; uploads are **not** grouped into `common_prefixes` (deferred until a client needs it) |

Two properties are worth stating on their own, because they decide how a
client should retry:

- **A rejected complete is retryable.** Validation runs against the upload
  before it is claimed, so a complete that names a missing part or gets the
  order wrong changes nothing: the upload record and every part record are
  exactly where they were. Retry with a corrected request. This matches
  what real S3 does.
- **A successful complete or abort is final.** The upload record is gone,
  so a retry answers `NoSuchUpload`. A client that treats `NoSuchUpload` on
  a *repeat* of a request it already got a success for as an error will
  report a spurious failure; it is the same shape as double-DELETE.

Uploads are visible while they are in flight: `ListMultipartUploads` shows
every unfinished upload in a bucket (sorted by key, then upload id, with
`key_marker` / `upload_id_marker` pagination), and `ListParts` shows one
upload's parts in part-number order.

## The stale-upload garbage collector

Clients abandon uploads: a laptop closes, a CI job is cancelled, a network
drops. Nothing in the S3 protocol tells the server about it, so the server
ages them out.

s3cas runs a sweeper in the background. Each sweep does two things:

1. **Aged uploads.** Every upload record older than the TTL is aborted
   through the same path a client's `AbortMultipartUpload` takes -- same
   claim, same per-part release. The GC is not a second implementation of
   abort; it is another caller of the first one.
2. **Orphan parts.** Every part record whose upload record no longer exists
   is removed and its blocks released (see the next section).

### Configuration

```toml
[multipart]
# Age at which an unfinished upload is aborted, in days. 0 disables the
# sweep entirely. Default: 7
stale_ttl_days = 7
```

The CLI flag `--multipart-stale-ttl-days` wins over the config file, which
wins over the default of 7. The full example is in
`qss_storage.toml.example`.

| Setting | Behavior |
| --- | --- |
| `stale_ttl_days = 7` (default) | uploads are reaped once they are more than 7 days old |
| `stale_ttl_days = 0` | the sweeper is never started; **nothing** reclaims an abandoned upload until an operator runs `qss-storage-fsck --repair` |
| sweep interval | fixed at `max(ttl / 20, 1 hour)`, not configurable |
| first sweep | one full period after startup, never at boot -- boot is when a store is busiest, and residue that has sat for days can wait an hour |
| shutdown | the sweeper stops on the same signal that stops the accept loop, with a 5-second budget; a sweep still running past that is left to be abandoned, which is safe (see below) |

Two counters are exported on the metrics endpoint, both cumulative:
`s3_multipart_uploads_reaped` (uploads aborted for outliving the TTL) and
`s3_multipart_orphan_parts_reaped` (part records removed with their
references). A sweep that did anything also logs one line at INFO.

### TTL semantics

The age is wall-clock time since the upload was created (the record's
`created_at`), compared with the TTL: strictly older is stale. At TTLs
measured in days, clock skew does not matter and monotonicity is not
required -- but a store carried to a machine whose clock is wildly wrong
can expire uploads early. That is accepted, not defended against.

A client whose upload is reaped mid-transfer sees `NoSuchUpload` on its
next `UploadPart` or on its complete. That is the S3-idiomatic outcome of
a TTL policy and what every client already handles.

Choosing a TTL is a trade between disk held by abandoned uploads and the
slowest upload you are willing to support. The default of 7 days is
generous on purpose; a store under space pressure with well-behaved clients
can go to 1 without ceremony.

**Abandoning a sweep is safe.** Every step it takes is crash-safe on its
own -- the part record is removed before its blocks are released, so an
interruption anywhere leaves an over-count (leakage), never a reference
dropped from under a live holder. A sweep killed by shutdown is exactly the
crash case that ordering was chosen for, and the next sweep picks up what
this one did not reach.

## Orphan parts, and who reaps them

An **orphan part** is a part record whose upload record does not exist.
Nothing can complete it and nothing can abort it, so it holds its blocks
until something reaps it. Four ways one appears:

- a crash (or a shutdown) between the claim and the last part of an abort;
- an `UploadPart` that passed its existence check and landed *after*
  another caller claimed the upload -- the one accepted race in this
  design, and it costs bounded leakage, never loss;
- a part a completing client never named in its parts list: nothing
  inherited its blocks, so it really is an orphan;
- a part record written before ADR 0003 re-keyed the tree (see the
  migration below).

**The daemon GC is the primary reaper**, on every sweep. **fsck is the
offline backstop**: its multipart pass reports each one as `orphan_part`
(INFO) and `--repair` reaps it with the same primitive the daemon uses. A
store whose daemon has never run -- or has run with `stale_ttl_days = 0` --
is reclaimed by the offline pass, not left to leak. See `docs/fsck.md`.

**An inheritable part can never look like an orphan.** This is the property
that makes reaping safe at all. Complete takes the upload record and every
part it names in ONE transaction (both trees live in the same database), so
there is no instant at which a part whose blocks a new object now owns is
visible without its upload record. A reaper can only ever see parts that
nothing inherited.

**Reaping is take-style.** The part record is read and removed in one
transaction and the blocks released are the ones that transaction read, so
two reapers over one part -- a client's abort against a GC sweep, or two
sweeps -- release it exactly once. A double release would take a block
below its true holder count, which is the one direction that means data
loss.

## Upgrading a store written before ADR 0003

Older stores keyed part records by joining bucket, key, upload id and part
number with dashes, which is ambiguous (all three can contain a dash) and
not scannable. ADR 0003 re-keyed them with length prefixes. There is **no
store-header bump and no migration step**: keys are opaque to every reader
that matters (records are found by value-driven scans, and point reads
rebuild the key they wrote), so an old-format record is simply unreachable
by the new point reads -- which makes it, by definition, an orphan part.

The first GC sweep reaps them. That IS the migration; there is nothing for
an operator to do but let the daemon run once (or run `fsck --repair`).

The cost is stated plainly: **in-flight uploads do not survive the
upgrade.** Their parts become orphans and are collected; clients see
`NoSuchUpload` and start over. This is transient state that used to leak
forever, so the trade is one restart's worth of re-uploads against a leak
with no collector.

## Operating notes

**What is in flight right now**

```sh
aws --endpoint-url http://localhost:8014 s3api list-multipart-uploads --bucket photos
```

**What in-flight uploads are costing you, offline**, with ages and the
blocks they hold (this needs the daemon stopped -- fsck takes the store's
lock):

```sh
qss-storage-fsck --meta-root /srv/qss --fs-root /srv/qss --json \
  | jq -r '.findings[] | select(.class == "multipart_upload") | .evidence'
```

**Orphan parts a daemon-less store is holding**, and reclaiming them:

```sh
qss-storage-fsck --meta-root /srv/qss --fs-root /srv/qss --json \
  | jq -r '.findings[] | select(.class == "orphan_part") | .evidence'
qss-storage-fsck --meta-root /srv/qss --fs-root /srv/qss --repair
```

**A sweep that reports errors** (`stale-upload sweep: ... N errors` in the
log) has stepped over records it could not read, decode or reap, and left
them for the next sweep. A record that will not decode is left alone
deliberately: its block list cannot be read, so removing it would strand
the blocks it names. Those are fsck's business.

**Nothing here is urgent.** Everything this page describes is leakage --
space held by records nothing will ever use again. The refcount contract
guarantees the other direction never happens: no reaping path can drop a
reference a live object still holds.

## See also

- `docs/adr/0003-multipart-lifecycle-and-gc.md` -- why the lifecycle looks
  like this: the claim rule, the atomic complete, the key format change,
  and the alternatives that were rejected.
- `docs/fsck.md` -- the offline reconciliation tool: the `multipart_upload`
  and `orphan_part` findings, and the `reap_orphan_part` repair.
- `docs/refcount.md` -- leakage allowed, loss never; one reference per
  block occurrence.
- `docs/adr/0006-block-write-protocol.md` -- the striped write and delete
  protocol every refcount mutation here goes through.
