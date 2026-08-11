# Durability: which acknowledgements promise what

The store's durability contract binds at the acknowledgement, not at the
block and not at the file. One request -- a PUT, a part upload, a complete
-- is one durability unit: its block files are made durable as a group, its
records commit in one transaction with one journal persist, and only then
does the client see success (ADR 0010). Nothing acknowledged is lost to a
process death, and at the default `fsync` level nothing acknowledged is
lost to a power cut either, with two documented exceptions this page
exists to name.

This is a single-node statement throughout. qss_storage has no
replication, no dispersal, and no second copy. Everything below is about
what survives a kill or a power cut on the one machine holding the store.

Companion pages: `docs/refcount.md` for the leak-versus-loss vocabulary,
`docs/fsck.md` for the offline collectors, and
`qss_quantum_storage/docs/05_durability.md` for the same treatment of the
QSFS stack.

## Two levels, and the strong one is the default

| level | what an ack means |
| --- | --- |
| `buffer` | Reached the kernel, nothing flushed. Survives the process being killed; lost only to a power cut or an OS crash. |
| `fsync` | Flushed to stable storage before the acknowledgement: block files (fdatasync plus the directory fsync that makes their renames durable), then the metadata journal (fjall `SyncAll`), once per batch. |

There is no third level. `fdatasync` was removed as a user-facing level
with no alias, and `Durability::from_str` rejects it with an error rather
than quietly mapping it to something else
(`cas-storage/src/metastore/traits.rs:222-252`).

`DEFAULT_DURABILITY` is `Fsync` (`config.rs:65`). The defaults that shape
the rest of the picture:

| constant | default | what it bounds |
| --- | --- | --- |
| `DEFAULT_MAX_BLOCKS_PER_COMMIT` | 64 | blocks per durability unit; transaction size, stripe hold time, and the residue one kill can leave |
| `DEFAULT_STRIPE_COUNT` | 1024 | per-block locks guarding refcount mutation |
| `DEFAULT_GROUP_COMMIT` | `false` | cross-request grouping is opt-in |
| `DEFAULT_VERIFY_ON_READ` | `false` | blocks are not re-hashed on read unless asked |
| `DEFAULT_HASH_WIDTH` | 32 | full BLAKE3-256 block addresses |

## The ordering underneath: file first, always

ADR 0006's rule is that a block record is never readable before its block
file is durable at its final path. Nothing is written in place; every block
reaches its address by write-temp, fsync, rename.

```text
lock stripe(hash)
  one short fjall tx: re-read record INSIDE the tx
      if present: bump refcount; commit        # dedup hit -- no file I/O
      else: drop the tx, fall through
  if no record:
      spawn_blocking:
          write blocks/.tmp/<hash>-<nonce>     # O_CREAT|O_EXCL, nonce mandatory
          fsync file                           # per Durability config
          fsync newly created fanout dirs      # per Durability config
          rename into final path               # atomic; rename-over, always
          fsync parent dir                     # per Durability config
      short fjall tx: insert record rc=1; commit
unlock
```

Two properties beyond the ordering matter for durability specifically.
The existence check and the refcount mutation are one transactional
read-modify-write, never a pre-transaction read followed by a blind write
-- a stale-read bump would resurrect a concurrently removed record with
rc=2 while true references are 1, which is a permanent leak. And delete is
ordered object-record-first, in a different database from the block
records, so no single transaction spans them: a crash between the two
steps leaves over-counts, which is leakage, never loss.

ADR 0010 widened the unit from one block to one request without moving any
of that. Files are still durable before records exist; there are simply
more of them per flush.

## Not every 200 promises the same thing

This is the part most easily misread, and it is deliberate. ADR 0013 splits
acknowledged non-transactional writes into two classes, keyed by tree name
at `get_tree` time.

**Contract class** -- persists at the configured durability. These are the
writes whose loss after an ack would be silent or terminal, with nothing in
the protocol to surface it.

| ack | write | why strict |
| --- | --- | --- |
| PUT 200 | object record (bucket tree) | loss is silent: nothing revisits it |
| CompleteMultipartUpload 200 | claim tx + object record | same, and it is THE multipart ack |
| CreateBucket 200 | bucket metadata | rare enough to be free; a vanished bucket orphans its config |
| respcas SET / DEL reply | namespace tree | RESP has no later operation that would surface the loss |

**Recoverable class** -- no explicit persist; fjall's internal
`PersistMode::Buffer` write applies. These are the writes whose loss after
an ack is loud, caught by a mandatory later step of the same protocol.

| ack | write | what catches a power loss |
| --- | --- | --- |
| UploadPart 200 (ETag) | part record, `_MULTIPART_PARTS` | the complete fails `InvalidPart`; client re-uploads the part |
| CreateMultipartUpload 200 | upload marker, `_UPLOADS` | the next upload-part fails `NoSuchUpload`; client restarts the upload |

Both classes are kernel-visible before the ack, so the `kill -9` guarantee
-- every daemon-acknowledged write survives process death -- is identical
for both. **The classes differ only against power loss, and only where the
protocol already shouts.** A client treating an `UploadPart 200` as
archival-grade would be wrong, and S3's own semantics never promised
otherwise.

Transactional paths (`commit_persist`) are untouched by the split: always
the configured durability.

## Why the split exists at all

fjall 3.1.8 persists single keyspace operations -- an `insert` or `remove`
outside an explicit batch or transaction -- with `PersistMode::Buffer`
unconditionally. Kernel-visible, never fsynced. An application that maps
"fsync durability" onto bare keyspace writes therefore has
power-loss-vulnerable acks and no way to notice.

The first fix was wholesale: persist on every ack-carrying path. It worked
and it cost 55% of parallel ingest at fsync and 71% at buffer, spent
mostly on acks whose loss the protocol already surfaces. ADR 0013 narrowed
it to the table above, and the regression tests pin the boundary by acking
a write, reading the journal file, and asserting the bytes.

Worth recording alongside it, because it changes how the crash evidence
should be read: the `kill -9` losses that originally motivated the
wholesale persist were never store losses. They were traced to a
client-side false acknowledgement in aws-cli, caught in the act by an
instrumented harness. Across 492 targeted kills at the ack, zero losses
were reproducible. See `docs/upstream/fjall-journal-ack-visibility.md`.

## Group commit changes throughput, not the contract

The ADR 0011 commit station merges the closing step of concurrent requests
-- and only the closing step -- into one transaction with one persist. It
is off by default; a store that never enables it runs the ADR 0010 write
path byte for byte.

What it does not move: files are still durable at their final paths before
any record exists, stripes are still taken before fjall and never after,
the transaction still runs in one blocking closure with no await inside it,
and the refcount arithmetic is unchanged. A group is a wider batch, not a
different protocol.

Two properties an operator should know. A stranger's error never fails your
ack: if the group transaction errors it rolls back and each member replays
as its own transaction under the stripes the group already holds, so one
bad member fails one request. And cancelling the wait does not cancel the
work -- a sealed batch belongs to the station, so a client that hangs up
mid-ack leaves a committed batch rather than a half-applied one.

## What a crash leaves behind

Three residue classes, all leakage, each with a collector.

| # | residue | collector |
| --- | --- | --- |
| 1 | temp files | cleared wholesale at store open |
| 2 | file without record (orphan) | fsck orphan sweep, or healed by rename-over on re-upload |
| 3 | record + file with no referencing object (rc over-count) | fsck refcount reconciliation, recounted from live objects |

None is client-visible loss. The governing asymmetry, stated in
`docs/refcount.md` and enforced by ADR 0005: refcounts may over-count
("leakage") but must never under-count ("loss"). An under-count is graded
CRITICAL, because a repair pass acting on one would free live blocks --
loss by repair, the failure mode the whole contract is shaped to avoid.

Two further leak-class residues are named in ADR 0005 and are not
crash-only. Half-deleted buckets: `bucket_delete` removes the bucket
metadata before object teardown, so a crash mid-loop strands an invisible
object tree whose refcounts still hold. Off-depth duplicates: a file named
`<id>` at a fanout depth the live record does not name, residue of
placement drift or an interrupted insert probe.

ADR 0008 closed the largest routine leak. Overwriting a key now releases
the replaced object's blocks rather than blind-upserting and forgetting
them, so an overwrite-heavy workload no longer grows `blocks/` without
bound between fsck runs.

## The one loss-shaped hole

At `durability = "buffer"` nothing is fsynced, so a power cut can persist a
record whose file's pages were lost. Choosing Buffer accepts that residue.
What makes it worse than an ordinary orphan is that an unrepaired dangling
record actively **poisons dedup**: every future same-content PUT bumps the
fileless record, skips its write, and commits another damaged object. Left
alone it propagates, and fsck is the designated collector.

This is the only residue in the store that is loss-shaped rather than
leak-shaped, and it exists only below the default durability level. At
`fsync`, a dangling record indicates Buffer history, foreign interference,
or a bug -- it is never a routine crash state.

One further carve-out: the `notx` backend (ADR 0007) is explicitly scoped
out of the loss-never guarantee. It cannot provide the atomic
read-and-remove that defeats a double-DELETE of one key.

## Verification is opt-in and partial

`verify_on_read` defaults to `false`. Enabled, it re-hashes each block
before serving it and fails the stream with a `BlockCorruption` error
rather than handing out bytes that no longer match their address.

Three limits, all in the code rather than merely implied:

- **Range requests are never verified.** A partial block cannot be checked
  against a whole-block address, so verification is a no-op for anything
  but a whole-object read. Callers may apply it unconditionally; a ranged
  read simply streams unverified
  (`cas-storage/src/cas/block_stream.rs:127-132`).
- **Verification changes the memory profile.** A verified block is read
  whole and hashed before any of it is yielded, instead of streaming out in
  small pieces.
- **Cold data is never verified by anyone** unless a scrub or `s3cas check`
  is run explicitly. Nothing walks the store in the background.

Separately, a store whose header is missing or names a hash the build does
not support refuses to open rather than guessing.

## What is asserted but not measured

ADR 0013 states its own limit and it should be repeated here rather than
buried: grading the contract-versus-recoverable split honestly needs a
power-loss rig, and that rig does not exist. Until it does, the contract
table plus the regression tests **are** the grade.

Concretely:

- `kill -9` behaviour is measured, extensively -- 492 targeted kills at the
  ack with zero reproducible losses, plus a standing crash matrix
  (`docs/realtest.md`).
- Power-loss behaviour is reasoned: from the fsync call sites, the fjall
  persist modes, and the ack-visibility regression tests that pin journal
  bytes at the ack.
- No test in the suite cuts power to a machine.

The distinction matters more than it might look, because `kill -9` cannot
tell the two ack classes apart. Both are kill-safe by the same mechanism.
The class split is a power-loss distinction, and power loss is exactly what
is untested.

## Failure matrix

At the default `fsync` durability, for a request the client saw succeed.

| event | object blocks | contract-class records | recoverable-class records |
| --- | --- | --- | --- |
| Process kill (`kill -9`) | survive | survive | survive |
| Power loss, `fsync` | survive | survive | may be lost, loudly |
| Power loss, `buffer` | may be lost | may be lost | may be lost |
| Disk loss | lost | lost | lost |
| Silent bit rot | detected only under `verify_on_read` or scrub | not checked | not checked |

"May be lost, loudly" means a mandatory later step of the same protocol
catches it: the multipart complete fails and the client re-uploads.

The last two rows are the honest ones. There is no replication and no
background scrub; the store is single-node and integrity checking is
opt-in.

## What an ack means, per operation

| operation | on 200, at `fsync`, the data is... |
| --- | --- |
| PUT object | blocks fsynced at final paths, object record journal-fsynced |
| UploadPart | blocks fsynced at final paths; part record kernel-visible only |
| CompleteMultipartUpload | claim transaction and object record journal-fsynced |
| CreateMultipartUpload | upload marker kernel-visible only |
| CreateBucket | bucket metadata journal-fsynced |
| respcas SET / DEL | namespace tree journal-fsynced |
| any of the above at `buffer` | kernel-visible, nothing flushed |

Every row is a single-node statement. None of them says anything about a
second copy, because there is no second copy.

## Where this sits relative to QSFS

Recorded because the two are routinely compared, and the comparison runs
in opposite directions depending on which property is being asked about.

| | qss_storage | QSFS (zdbfs / 0-db / zstor) |
| --- | --- | --- |
| Ack means durable locally | yes, by contract, at the `fsync` default | no; `fsync()` is a no-op and 0-db does not sync by default |
| Off-node redundancy | none | yes, erasure-coded across N backends |
| Overwrite behaviour | releases replaced blocks (ADR 0008) | append-only; the local DB grows with writes |
| Integrity checking | opt-in on read, plus offline scrub and fsck | zstor repair queue over dispersed objects |
| Power-loss testing | no rig | no rig |

qss_storage makes a precise, documented promise about a single node and
offers nothing beyond it. QSFS makes no meaningful local promise and offers
real redundancy once data crosses the dispersal boundary. Neither has been
tested against an actual power cut.

## Source references

- `cas-storage/src/metastore/traits.rs:222-252` -- Durability enum, rejected `fdatasync`
- `cas-storage/src/config.rs:61-106`, `:145` -- defaults
- `cas-storage/src/cas/write_path/batch.rs:11` -- batch cap 64
- `cas-storage/src/cas/stripes.rs:25` -- stripe count 1024
- `cas-storage/src/cas/block_stream.rs:127-132` -- ranges are not verified
- `cas-storage/src/cas/group_commit.rs` -- commit station, sealed batches
- ADR 0005 -- leak vs loss vocabulary, buffer-mode dangling records
- ADR 0006 -- file-first protocol, residue classes
- ADR 0007 -- notx scoped out of loss-never
- ADR 0008 -- overwrite releases replaced blocks
- ADR 0010 -- ack as durability unit, measured costs
- ADR 0011 -- group commit, isolation properties
- ADR 0013 -- the two-class table, and the missing power-loss rig
- `docs/upstream/fjall-journal-ack-visibility.md` -- fjall bare-write persist mode, aws-cli false ack
