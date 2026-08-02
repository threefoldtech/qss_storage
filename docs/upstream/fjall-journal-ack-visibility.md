# Journal write visibility vs the ack: an fjall 3.1.8 field report

**Audience**: this repo's record first. One paragraph still concerns
fjall-rs (Finding 1's discoverability note); nothing in here any longer
asks them a question.
**Status**: RESOLVED 2026-08-02. Finding 1 (bare writes persist at
`PersistMode::Buffer`) stands: confirmed, regression-tested, fixed on
our side. Finding 2 -- the kill -9 losses this document existed to ask
about -- is CLOSED: not fjall, and not our store either. A client-side
false acknowledgement in aws-cli, caught in the act by an instrumented
harness, compounded by a grep artifact in our own first corpse reading.
The old Finding 2 is replaced below with the resolution.
**Date**: 2026-08-01 (revised same day: the first draft's mechanism
hypothesis did not survive its own regression test); resolved 2026-08-02
(the second draft's open mechanism did not survive the corpse decoder --
this document keeps saying so rather than pretending otherwise)
**fjall**: 3.1.8 (crates.io; source cross-checked against fjall-rs/fjall at
tag 3.1.8, local clone at ~/prppl/fjall)
**Consumer**: qss_storage (content-addressed S3 store; fjall via
`SingleWriterTxDatabase`, one database for block records, one per-namespace
database for object records)

---

## One-paragraph summary

While crash-testing at our weakest durability level we lost three
client-acknowledged writes across `kill -9` (never at our fsync level, and
never reproducible by killing at the ack -- 492 targeted kills, zero
losses). Chasing it produced two distinct results. CONFIRMED: fjall
persists single keyspace operations (`insert`/`remove` outside an explicit
batch or tx) with `PersistMode::Buffer` unconditionally -- reaching the
kernel, but never fsyncing -- so an application that maps "fsync
durability" onto bare keyspace writes silently has power-loss-vulnerable
acks; we fixed that on our side by calling `persist(<configured mode>)` on
every ack-carrying path, and a one-line doc note upstream would spare the
next consumer the archaeology. CLOSED (2026-08-02): the kill -9 losses
themselves were never store losses. Decoding the corpses' journals at
the wire level shows every "lost" upload caught mid-flight -- create
persisted, some parts landed, CompleteMultipartUpload never executed,
journal clean to EOF -- and an instrumented rerun caught `aws s3 cp`
(2.36.14) exiting 0, stderr empty, for an upload whose complete the
killed daemon never answered. The client manufactured the
acknowledgement; fjall's journal did exactly what it claims. Details in
Finding 2.

## The mechanism, from source (fjall 3.1.8)

`src/journal/writer.rs`:

- The journal writer wraps its file in a `BufWriter` with
  `JOURNAL_BUFFER_BYTES = 8 * 1024` (writer.rs:21, :177, :194).
- `write_raw` / `write_batch` serialize entries INTO the BufWriter and mark
  it dirty (writer.rs:258-298, :326-379). No flush here.
- `Writer::persist(mode)` (writer.rs:203-234) FIRST drains the BufWriter to
  the kernel (`flush()`) whenever dirty, THEN applies the mode: `SyncAll`
  -> `sync_all()`, `SyncData` -> `sync_data()`, `Buffer` -> `Ok(())`.

So the three `PersistMode`s are write() / write()+fdatasync /
write()+fsync -- `Buffer` is not a no-op, it is the write() tier.

The part we missed on first read, found by writing a regression test
against our own hypothesis: single keyspace operations do not skip persist.
`SingleWriterTxKeyspace::insert`/`remove` route through an internal write
that fjall itself persists with `PersistMode::Buffer` (see the
`durability(Some(PersistMode::Buffer))` wiring in `src/db.rs`), so bare
writes ARE kernel-visible by the time they return. Our deterministic test
-- ack a write, then read the journal FILES through the filesystem and
assert the record bytes are present -- passes on unmodified fjall 3.1.8,
at every durability level. That test killed our first theory and now
pins the boundary permanently in our suite. (The resolution below
vindicates it a second time: fjall's journal did exactly what it
claims, in every corpse, both nights.)

## Finding 1 (confirmed, fixed on our side): bare writes cap out at Buffer

The flip side of that internal `PersistMode::Buffer`: it is applied
REGARDLESS of what durability the application wanted. Transactions let the
caller persist afterwards at any mode (we always did); bare keyspace
writes got Buffer, full stop, unless the application remembers to call
`persist()` itself -- and ours did not. Every ack-carrying
non-transactional write we make (bucket creation, multipart-upload
creation, `UploadPart`'s ETag, our RESP daemon's `SET`/`DEL`) was
therefore page-cache-only even with the store configured to fsync.
Invisible to kill-based testing (the page cache outlives the process);
real against power loss. Fixed on our side: `insert`/`remove` now call
`persist(<configured mode>)` before returning -- one added journal fsync
per such ack at our fsync level, free at our buffer level.

Upstream suggestion 1: a sentence on the keyspace write API -- "single
operations are persisted at `PersistMode::Buffer`; call `persist()` for
stronger guarantees" -- would have saved us the archaeology. The behavior
is defensible; its discoverability is the trap.

## Finding 2 (CLOSED 2026-08-02): the kill -9 "losses" were false client acks

The 2026-08-01 night campaign reproduced the loss shape four more times,
now at our FSYNC level (4 of 6 cycles, two configurations), which forced
the investigation that closed it. Two instruments settled what greps
could not:

1. **A journal decoder** (fjall 3.1.8 wire format: Start/Item/End
   batches, seqnos, full keys) replaced substring greps over the `.jnl`
   files. The greps had a trap this document walked into on 2026-08-01:
   the campaign's journal spans crash cycles, every cycle names its
   objects `mp-<n>`, so a bare grep for a lost key matches OTHER cycles'
   same-numbered keys and manufactures exactly the "created long ago,
   parts vanished" shape the old Finding 2 reported. Decoded, every
   corpse from both nights -- including this document's original
   buffer-cycle7/mp-68 -- tells one story: the flagged upload's
   CreateMultipartUpload record is the LAST (or nearly last) batch in
   the journal, some parts' blocks follow, the CompleteMultipartUpload
   never executed, and the journal parses clean to EOF with contiguous
   batch seqnos. No torn tail, no elision, no reordering. The store was
   photographed mid-upload; the daemon never acknowledged the complete.

2. **A per-cp audit** in the crash storm (`QSSRT_CRASH_MP_LOG`: exit
   code, wall window, stderr, per upload) caught the acknowledgement
   being minted. Run 20260802T100525, cycle 2, `fsync-2/mp-80`: all
   four part records persisted (the journal's final batches), kill -9
   inside the CompleteMultipartUpload window, complete never executed
   -- and `aws s3 cp` exited 0 with zero bytes of stderr, ~0.2s after
   the daemon died. The harness appended its "acknowledged" line on
   that exit code, as designed; the exit code was a lie.

So: aws-cli 2.36.14 (`aws s3 cp`, multipart path) can report success
for an upload whose CompleteMultipartUpload was never answered. At idle
this window is milliseconds and 60 random kills never hit it; under a
saturated fsync-level daemon the complete stretches to a fat fraction
of the transfer and the campaign hit it 4-in-6 cycles. That asymmetry
also explains the old "never at fsync, only at buffer" -> "suddenly at
fsync" flip-flop: it was never durability, only how long an upload
stays in flight at each level. The upstream report this deserves goes
to aws-cli, not fjall-rs, with the mp-80 capture as the reproduction.

What this closes: the corpse question this document asked upstream is
withdrawn -- fjall's journal contained, both nights, precisely what a
correct append-only mutex-serialized journal should contain. Our
store's fsync claim (every DAEMON-acknowledged write survives kill -9)
held in every decoded corpse. Finding 1 is untouched: bare writes still
persist at `PersistMode::Buffer` unless the application asks otherwise,
that is still a power-loss trap, and our per-ack persist fix (and its
regression tests) remain in place while its cost/scope is decided.

## Reproduction

All in the qss_storage repo (github.com/threefoldtech/qss_storage):

- The aws-cli false ack: `tests/real/run.sh --phase 6 --resume` with
  `QSSRT_CRASH_MP_LOG=<file>` and `QSSRT_CRASH_SNAPSHOT_DIR=<dir>`;
  grade the audit tsv against the FAIL lines. Captured first try-but-one
  on 2026-08-02 (runs 20260802T094650 clean, 20260802T100525 captured).
- `tests/real/tools/buffer-loss-repro.sh` -- the kill-at-ack rig; its
  hundreds of clean kills were evidence all along: killing AT the ack
  never loses, because daemon-acknowledged writes are really there.
- The corpses (pre-recovery snapshots) and the journal decoder:
  preserved outside the repo, available on request.
- The permanent regression tests for the write-visibility boundary:
  `cas-storage/src/metastore/stores/fjall.rs` (journal-bytes assertions)
  and `cas-storage/src/cas/ack_visibility_tests.rs`.
